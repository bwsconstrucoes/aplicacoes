# -*- coding: utf-8 -*-
"""
app.py — Análise de SPs (substituição da aba 'Relatório'/'Lote' do Google Sheets).

Fase 1 (offline): consulta, filtros, KPIs, aba Lote e edição OTIMISTA no cache local.
Fase 2 (com seus scripts): envio das edições ao Sheets e ações em lote reais.

Rodar:  streamlit run app.py
"""

import os
import re
import json
import zlib
import threading
import inspect
from datetime import datetime
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
import streamlit.components.v1 as components

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
    try:
        from st_aggrid import ColumnsAutoSizeMode
    except Exception:
        ColumnsAutoSizeMode = None
    _TEM_AGGRID = True
except Exception:
    _TEM_AGGRID = False

import cache
import dados
import gsheets
import relatorio
import auditoria
import pagamentos
import rateio
import beevale
import config
import agenda
import bradesco
from schema import DISPLAY_ORDER, COLS, labels_map, EDITAVEIS, KEPT_KEYS

# O Styler do Pandas só estiliza até 262.144 células por padrão. Como o Relatório
# pode ter dezenas de milhares de linhas, elevamos o limite. O custo é baixo: o Pandas
# guarda apenas as células que de fato recebem cor (as vazias não pesam).
pd.set_option("styler.render.max_elements", 100_000_000)

st.set_page_config(page_title="Análise de SPs — BWS", layout="wide",
                   initial_sidebar_state="expanded")

SEED_CSV = os.path.join(os.path.dirname(__file__), "seed_spsbd.csv")
LABELS = labels_map()

# Largura total compatível com qualquer versão do Streamlit:
#   - versões atuais/antigas usam use_container_width=True
#   - versões futuras (que removeram use_container_width) usam width='stretch'
def _ver_ge(maj: int, minr: int) -> bool:
    try:
        p = st.__version__.split(".")
        return (int(p[0]), int(p[1])) >= (maj, minr)
    except Exception:
        return False

# width='stretch' substituiu use_container_width a partir do Streamlit 1.43
_FULLW = ({"width": "stretch"} if _ver_ge(1, 43)
          else {"use_container_width": True})


# ----------------------------------------------------------------------------
# Inicialização / carga
# ----------------------------------------------------------------------------
# Schema garantido a CADA início (idempotente e barato): cria tabelas que faltam
# mesmo após reload (runOnSave não re-roda o @st.cache_resource abaixo). Assim,
# novas tabelas/colunas passam a existir sem precisar reiniciar o app do zero.
cache.init_db()


@st.cache_resource
def _init():
    if cache.contar() == 0:
        # Com credenciais.json -> carrega a planilha real. Sem -> usa o seed offline.
        if gsheets.disponivel():
            try:
                gsheets.bootstrap()
                return "online"
            except Exception as e:
                st.warning(f"Falha ao ler a planilha ({e}). Usando dados de exemplo (seed).")
        if os.path.exists(SEED_CSV):
            cache.seed_de_csv(SEED_CSV)
    return "ok"


_modo = _init()


# Reabertura do detalhe na MESMA célula: o AgGrid reenvia um evento idêntico ao
# clicar de novo na mesma célula, então deduplicamos por assinatura (_agev_*).
# Quando o modal de detalhe FECHA, liberamos as assinaturas para permitir reabrir.
# O modal seta _det_flag enquanto está visível (Streamlit re-renderiza o corpo do
# dialog a cada run enquanto ele está aberto).
_det_render_anterior = st.session_state.get("_det_flag", False)
if st.session_state.get("_det_estava_aberto", False) and not _det_render_anterior:
    for _k in [k for k in list(st.session_state.keys()) if k.startswith("_agev_")]:
        st.session_state.pop(_k, None)
st.session_state["_det_estava_aberto"] = _det_render_anterior
st.session_state["_det_flag"] = False


def recarregar():
    st.cache_data.clear()


# ---------------------------------------------------------------------------
# ESCRITA GARANTIDA — padrão para TODAS as alterações daqui pra frente.
# 1) aplica no cache local na hora (otimista, instantâneo);
# 2) enfileira a célula numa fila durável (SQLite);
# 3) tenta drenar a fila pro Sheets (carimba V p/ os outros usuários sincronizarem).
# Se a internet/API falhar, a alteração FICA na fila e é reenviada sozinha no
# ciclo de 90 s (e ao reabrir o app) — nada se perde.
# ---------------------------------------------------------------------------
def _drenar_fila() -> int:
    """Tenta enviar a fila ao Sheets. Retorna quantas alterações ainda restam."""
    pend = cache.fila_pendentes()
    if not pend or not gsheets.disponivel():
        return len(pend)
    try:
        escritos, nao_enc = gsheets.escrever_alteracoes(pend)
        if escritos:
            cache.fila_remover(escritos)
            cache.log_marcar_enviado(escritos)
        if nao_enc:
            cache.fila_erro(nao_enc, "ID não encontrado na planilha (tentará de novo)")
            cache.log_marcar_erro(nao_enc, "ID não encontrado na planilha")
    except Exception as e:
        pares = [(p["sp_id"], p["coluna"]) for p in pend]
        cache.fila_erro(pares, str(e))
        cache.log_marcar_erro(pares, str(e))
    return cache.fila_contar()


def aplicar_alteracao(ids, coluna_key: str, valor: str, acao: str = "Alterar Status") -> int:
    """Aplica 'coluna_key=valor' a vários IDs: local na hora + log + fila + tenta enviar."""
    ids = [str(i) for i in ids]
    n = 0
    for sp_id in ids:
        if cache.editar_local(sp_id, {coluna_key: valor}):
            n += 1
        cache.enfileirar(sp_id, coluna_key, valor)
        cache.log_registrar(sp_id, coluna_key, valor, acao, "pendente")
    recarregar()
    restam = _drenar_fila()
    if restam == 0:
        st.session_state["_flash"] = ("success", f"{n} SP(s) atualizada(s) e já sincronizada(s) online.")
    else:
        st.session_state["_flash"] = ("warning",
            f"{n} SP(s) atualizada(s) localmente. {restam} alteração(ões) em fila — "
            "serão enviadas automaticamente assim que a conexão permitir.")
    return n


def aplicar_no_modal(reg: dict, sp_id, coluna_key: str, valor: str,
                     acao: str = "Alterar Status", campo_reg: str | None = None) -> None:
    """Versão do aplicar_alteracao para uso DENTRO do modal de detalhes:
    - aplica no cache local na hora (a tela já reflete);
    - enfileira e DRENA A FILA EM SEGUNDO PLANO (thread) — não congela o modal;
    - NÃO chama st.rerun() — o detalhamento continua aberto;
    - mostra um toast de confirmação imediata (estilo Google Sheets).
    O envio ao Sheets segue em background; se a conexão oscilar, fica na fila e
    é reenviado no ciclo de 90 s."""
    sp_id = str(sp_id)
    cache.editar_local(sp_id, {coluna_key: valor})
    cache.enfileirar(sp_id, coluna_key, valor)
    cache.log_registrar(sp_id, coluna_key, valor, acao, "pendente")
    recarregar()
    threading.Thread(target=_drenar_fila, daemon=True).start()
    reg[campo_reg or coluna_key] = valor          # reflete já nesta renderização
    st.toast(f"{acao} — salvo. Enviando ao Google Sheets em segundo plano…", icon="✅")


def _aviso_fila_modal() -> None:
    """Mostra dentro do modal quantas alterações estão sendo enviadas ao Sheets."""
    try:
        n = cache.fila_contar()
    except Exception:
        n = 0
    if n:
        st.caption(f"⏳ {n} alteração(ões) sendo enviada(s) ao Google Sheets em segundo plano…")


def limpar_pagamento_modal(reg: dict, sp_id) -> None:
    """Botão 'Limpar Pagamento' do detalhe: Status Pgt (O)='Pagar', Agendado (AB)=''
    e Comprovante (AG)=''. Aplica local na hora, envia ao Sheets em 2º plano e NÃO
    fecha o modal (mesmo padrão de aplicar_no_modal)."""
    sp_id = str(sp_id)
    mudancas = {"status_pgt": "Pagar", "agendado": "", "comprovante": ""}
    cache.editar_local(sp_id, mudancas)
    for col, val in mudancas.items():
        cache.enfileirar(sp_id, col, val)
        cache.log_registrar(sp_id, col, val, "Detalhe: Limpar Pagamento", "pendente")
    recarregar()
    threading.Thread(target=_drenar_fila, daemon=True).start()
    reg.update(mudancas)
    st.toast("Pagamento limpo (Status→Pagar; Agendado e Comprovante vazios). "
             "Enviando ao Sheets…", icon="✅")


# E-mail de reenvio de comprovante (config via variáveis de ambiente — nada de
# senha no código). Defaults pensados para Gmail (STARTTLS).
_COMP_EMAIL_DEST = "bbhf6mf88hg3l7rxyrilis2e5hqb8i2v@hook.us1.make.com"


def _baixar_comprovante(url: str):
    """Baixa o arquivo do link do comprovante. Trata link do Dropbox (força
    download direto). Retorna (conteudo_bytes, nome_arquivo) ou levanta exceção."""
    import urllib.request
    from urllib.parse import urlparse, unquote
    u = (url or "").strip()
    if not u:
        raise ValueError("Registro sem link de comprovante (coluna AG vazia).")
    # Dropbox -> download direto
    if "dropbox.com" in u:
        u = u.replace("www.dropbox.com", "dl.dropboxusercontent.com")
        u = re.sub(r"[?&]dl=0", "", u)
        u = u + ("&dl=1" if "?" in u else "?dl=1")
    req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 (spsbd_app)"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        dados = resp.read()
        # nome do arquivo: Content-Disposition > caminho da URL > padrão
        nome = ""
        cd = resp.headers.get("Content-Disposition", "")
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)', cd, re.I)
        if m:
            nome = unquote(m.group(1)).strip()
        if not nome:
            base = os.path.basename(urlparse(u).path)
            nome = unquote(base) if base else ""
        if not nome:
            nome = "comprovante.pdf"
    if not dados:
        raise ValueError("Download retornou vazio.")
    return dados, nome


def _enviar_email_comprovante(conteudo: bytes, nome_arquivo: str) -> None:
    """Envia o comprovante como anexo para o mailhook do Make. As credenciais de
    e-mail vêm da planilha de Credenciais (aba Chave|Valor) via config.get_token:
    EMAIL_USUARIO e EMAIL_SENHA (senha de app). Host/porta opcionais:
    EMAIL_HOST (padrão smtp.gmail.com) e EMAIL_PORT (padrão 587). Variável de
    ambiente de mesmo nome tem prioridade. Levanta exceção em caso de falha."""
    import smtplib, mimetypes
    from email.message import EmailMessage
    user = config.get_token("EMAIL_USUARIO")
    senha = config.get_token("EMAIL_SENHA")
    host = config.get_token("EMAIL_HOST", "smtp.gmail.com")
    try:
        port = int(config.get_token("EMAIL_PORT", "587") or 587)
    except (TypeError, ValueError):
        port = 587
    remetente = config.get_token("EMAIL_FROM", user)
    if not user or not senha:
        raise RuntimeError(
            "E-mail não configurado. Cadastre EMAIL_USUARIO e EMAIL_SENHA (senha "
            "de app) na aba 'Credenciais' da planilha de tokens e clique em "
            "'Atualizar tokens' na aba Sincronização.")
    msg = EmailMessage()
    msg["Subject"] = "Comprovante Bradesco"
    msg["From"] = remetente
    msg["To"] = _COMP_EMAIL_DEST
    msg.set_content("Arquivo enviado via Streamlit")
    tipo, _ = mimetypes.guess_type(nome_arquivo)
    maintype, subtype = (tipo.split("/", 1) if tipo else ("application", "octet-stream"))
    msg.add_attachment(conteudo, maintype=maintype, subtype=subtype, filename=nome_arquivo)
    with smtplib.SMTP(host, port, timeout=60) as s:
        s.ehlo()
        try:
            s.starttls(); s.ehlo()
        except smtplib.SMTPException:
            pass                                  # servidor sem STARTTLS (ex.: porta 465 SSL)
        s.login(user, senha)
        s.send_message(msg)


def _abrir_cards(urls: list):
    """Abre vários cards do Pipefy em novas abas (precisa permitir pop-ups)."""
    urls = [u for u in dict.fromkeys(urls) if u and str(u).startswith("http")]
    if not urls:
        st.info("Nenhum card do Pipefy nas linhas selecionadas.")
        return
    arr = json.dumps(urls)
    links = "".join(
        f'<li><a href="{u}" target="_blank" rel="noopener">{u}</a></li>' for u in urls)
    components.html(f"""
      <div style="font-family:sans-serif">
        <button id="bcards" style="padding:8px 14px;font-size:14px;cursor:pointer;
                border:1px solid #888;border-radius:6px;background:#0a7d2c;color:#fff">
          Abrir {len(urls)} card(s) em novas abas
        </button>
        <span style="font-size:12px;color:#666"> &nbsp;(permita pop-ups para este site)</span>
        <script>
          const U = {arr};
          function abrir() {{ U.forEach(function(u){{ window.open(u, '_blank'); }}); }}
          document.getElementById('bcards').onclick = abrir;
          abrir();  // tenta abrir automaticamente; se o navegador bloquear, use o botão
        </script>
        <details style="margin-top:8px"><summary style="font-size:12px;color:#666;cursor:pointer">
          links (caso algum não abra)</summary>
          <ul style="font-size:12px">{links}</ul>
        </details>
      </div>
    """, height=140)


_LIMITE_CODIGOS = 40
_BARCODE_MAXW = 900  # largura do código de barras na tela (px). 44-48 dígitos
#                      precisam de ~850-930px p/ barras de ~2px (ler sem zoom).


@st.cache_data(show_spinner=False)
def _png_pix(chave: str, valor: float, nome: str, copia_cola: bool):
    return pagamentos.gerar_pix(chave, valor, nome, copia_cola=copia_cola)


@st.cache_data(show_spinner=False)
def _png_boleto(ai: str):
    return pagamentos.barcode_png_bytes(ai)


@st.cache_data(show_spinner=False)
def _svg_boleto(ai: str):
    return pagamentos.barcode_svg(ai)


def _bloco_codigo(i: int, reg: dict):
    """Bloco: ID · Valor · Credor + QR/Código (largo) + botão Agendado (c/ confirmação)."""
    sp_id = str(reg.get("id", ""))
    valor_num = reg.get("valor_num", 0)
    credor = reg.get("credor", "")
    forma = reg.get("forma_pagamento", "")
    ja_agendado = str(reg.get("agendado", "")).strip().lower() == "agendado"
    with st.container(border=True):
        c_inf, c_btn = st.columns([3, 1])
        c_inf.markdown(f"**Seleção {i} — ID {sp_id}**  \n"
                       f"**Valor:** {_fmt_moeda(valor_num)}  ·  **Credor:** {credor}  ·  "
                       f"**Forma:** {forma}")
        # Botão Agendado com confirmação (sempre clicável; só indica se já está)
        ck = f"cfm_ag_{sp_id}"
        if st.session_state.get(ck):
            c_btn.warning("Confirmar Agendado?")
            if c_btn.button("✅ Sim, agendar", key=f"sim_{sp_id}", type="primary"):
                st.session_state.pop(ck, None)
                aplicar_alteracao([sp_id], "agendado", "Agendado", acao="Agendamento (QR)")
                st.rerun()
            if c_btn.button("✖ Cancelar", key=f"nao_{sp_id}"):
                st.session_state.pop(ck, None)
                st.rerun()
        else:
            if c_btn.button("📅 Agendado", key=f"ag_{sp_id}", type="primary",
                            use_container_width=True):
                st.session_state[ck] = True
                st.rerun()
            if ja_agendado:
                c_btn.caption("✓ já consta agendado")

        info = pagamentos.classificar(forma, reg.get("info_pgt", ""))
        try:
            if info["tipo"] == "pix":
                if info["falta_chave"]:
                    st.warning("🟠 Chave Pix ausente — atualize o cadastro (coluna Y).")
                else:
                    png, payload = _png_pix(info["chave"], float(valor_num or 0),
                                            credor, info["subtipo"] == "copia_cola")
                    qc = st.columns([1, 2, 1])
                    qc[1].image(png, width=240, caption="QR Pix")
                    with st.expander("Pix copia e cola"):
                        st.code(payload, language=None)
            elif info["tipo"] == "boleto":
                svg, status = _svg_boleto(reg.get("codigo_barras", ""))
                if status == "ok":
                    html = (f'<div style="max-width:{_BARCODE_MAXW}px;margin:4px auto 2px">'
                            f'<style>svg{{width:100%;height:auto;display:block}}</style>'
                            f'{svg}</div>')
                    components.html(html, height=125)
                    st.caption("Código de barras (boleto)")
                    d, _ = pagamentos.codigo_boleto(reg.get("codigo_barras", ""))
                    with st.expander("Código (digitável)"):
                        st.code(d, language=None)
                elif status == "invalido":
                    st.warning("🟠 Código de barras INVALIDO — atualize o cadastro (col AI).")
                else:
                    st.warning("Código de barras fora do padrão — confira manualmente.")
            else:
                st.info(f"Forma '{forma}' não é Pix nem Boleto.")
        except Exception as e:
            st.error(f"Falha ao gerar: {e}")
    st.write("")
    st.write("")


def _render_codigos(ids: list, fonte: pd.DataFrame):
    ids = [str(i) for i in ids][:_LIMITE_CODIGOS]
    base = fonte.set_index(fonte["id"].astype(str))
    ch, cf = st.columns([4, 1])
    ch.markdown(f"### 🔳 QR / Código de barras — {len(ids)} seleção(ões)")
    if cf.button("Fechar", key="fechar_codigos"):
        st.session_state.pop("_codigos_ids", None)
        st.rerun()
    st.caption("Leia pelo celular para agendar no banco e clique em **Agendar** "
               "(grava 'Agendado' na planilha).")
    for n, sp_id in enumerate(ids, start=1):
        if sp_id in base.index:
            _bloco_codigo(n, base.loc[sp_id].to_dict())


def _acoes_selecao(sel_ids, d_sel, fonte, prefix, extras=None):
    st.session_state["_sel_count"] = len(sel_ids)   # sync usa p/ não atropelar seleção
    st.session_state["_sel_ids_atual"] = [str(i) for i in sel_ids]  # p/ extras (ex.: relatório)
    """Barra de ações FIXA, contínua com a barra de abas (CSS .st-key-acoes_fixas).
    Linha 2 (1ª da barra): botões de ação colados + extras da aba (ex.: Lote).
    Linha 3 (2ª da barra): rádios + Alterar Status + 'Seleção N SP(s) · Total'."""
    n_sel = len(sel_ids)
    _total_sel = pd.to_numeric(d_sel.get("valor_num", pd.Series(dtype=float)),
                               errors="coerce").sum() if n_sel else 0.0
    extras = list(extras or [])
    _cx_acoes = globals().get("_BARRA_ACOES")
    if _cx_acoes is None:                    # segurança (não deveria ocorrer)
        try:
            _cx_acoes = st.container(key="acoes_fixas")
        except TypeError:
            _cx_acoes = st.container()
    with _cx_acoes:
        _n2 = 7 + len(extras)
        try:
            cols2 = st.columns([1] * _n2, vertical_alignment="center")
        except TypeError:
            cols2 = st.columns([1] * _n2)
        try:
            c3 = st.columns([3.0, 1.2, 3.0], vertical_alignment="center")
        except TypeError:
            c3 = st.columns([3.0, 1.2, 3.0])
    cC, cD, cE, cF, cG, cH, cI = cols2[:7]

    # ---- Linha 3: rádios + Alterar Status + info da seleção ----
    novo = c3[0].radio("Status de Agendamento",
                       ["Agendar", "Agendado", "Desagendar", "Falha Agendar"],
                       horizontal=True, key=f"bulk_agend_{prefix}",
                       format_func=lambda v: "Falha" if v == "Falha Agendar" else v,
                       label_visibility="collapsed")
    pode_status = (n_sel > 0) and bool(
        d_sel["validacao"].astype(str).str.strip().str.lower().eq("sim").all())
    if c3[1].button("✅ Alterar Status", disabled=(n_sel == 0 or not pode_status),
                    type="primary", key=f"btn_status_{prefix}",
                    help="Habilita quando todos os selecionados têm Validação = 'Sim'"):
        aplicar_alteracao(sel_ids, "agendado", novo)
        st.rerun()
    c3[2].markdown(f"<div class='sel-info'>Seleção {n_sel} SP(s) · "
                   f"Total {_fmt_moeda(_total_sel)}</div>", unsafe_allow_html=True)

    # ---- Linha 2: botões de ação ----
    if cC.button("🔗 Abrir cards", disabled=(n_sel == 0), key=f"btn_cards_{prefix}"):
        _abrir_cards(list(d_sel["pipefy_url"]))
    if cD.button("🔳 QR/Código", disabled=(n_sel == 0), key=f"btn_qr_{prefix}"):
        st.session_state["_codigos_ids"] = list(sel_ids)
        st.session_state["_codigos_origem"] = prefix
        st.rerun()
    todos_bv = (n_sel > 0) and bool(
        d_sel["forma_pagamento"].astype(str).str.lower().str.contains("beevale").all())
    if cE.button("🐝 Gerar BeeVale", disabled=not todos_bv, key=f"btn_bv_{prefix}",
                 help="Habilita quando todos os selecionados são BeeVale"):
        st.session_state["bv_ger_ids"] = list(sel_ids)
        st.session_state.pop("bv_ger_res", None)
        _dialog_gerar_beevale()
    if cF.button("📇 Cadastro BeeVale", key=f"btn_cadbv_{prefix}"):
        _dialog_cadastro_beevale()
    if cG.button("✅ Validar", disabled=(n_sel == 0), key=f"btn_val_{prefix}",
                 help="Marca Validação = 'Sim' nas SPs selecionadas"):
        st.session_state["_val_ids"] = list(sel_ids)
        _dialog_validar()
    if cH.button("🚫 Cancelar SP", disabled=(n_sel == 0), key=f"btn_cancel_{prefix}",
                 help="Abre pedidos de cancelamento no Pipefy (um card por SP), "
                      "com confirmação antes de enviar."):
        st.session_state["_cancel_ids"] = list(sel_ids)
        st.session_state.pop("_cancel_res", None)
        st.session_state.pop("cancel_confirmo", None)
        _dialog_cancelar_sp()
    if cI.button("📌 Enviar Lote", disabled=(n_sel == 0), key=f"btn_envlote_{prefix}",
                 help="Adiciona os IDs selecionados no campo IDs da aba Lote, no topo, "
                      "num grupo numerado 'Novo Lote N' (o que já estava é mantido abaixo)."):
        atual = (st.session_state.get("lote_ids", "")
                 or cache.get_meta("lote_ids_txt", "") or "").strip("\n")
        # título numerado -> cada envio vira uma TABELA separada no Lote
        _n = len(re.findall(r"(?m)^\s*Novo Lote\b", atual)) + 1
        bloco = f"Novo Lote {_n}\n" + "\n".join(str(i) for i in sel_ids)
        novo_txt = bloco + (("\n\n" + atual) if atual.strip() else "")
        # chave PRÓPRIA (não é de widget): sobrevive à limpeza do Streamlit e o
        # Lote a consome ao abrir; o meta garante persistência entre sessões.
        st.session_state["_lote_ids_novo"] = novo_txt
        try:
            cache.set_meta("lote_ids_txt", novo_txt)
        except Exception:
            pass
        st.toast(f"📌 {n_sel} ID(s) enviados para a aba Lote (grupo 'Novo Lote').",
                 icon="📌")
    # extras da aba (ex.: Lote: Processar/Limpar/Remover/Atualizar).
    # on_click quando o handler mexe em estado de widget (precisa rodar ANTES do
    # rerun); 'fn' para corpo inline (pode usar st.rerun).
    for _col, _ex in zip(cols2[7:], extras):
        if _ex.get("on_click"):
            _col.button(_ex["label"], key=_ex["key"], help=_ex.get("help"),
                        type=_ex.get("type", "secondary"),
                        on_click=_ex["on_click"], args=_ex.get("args", ()))
        else:
            if _col.button(_ex["label"], key=_ex["key"], help=_ex.get("help"),
                           type=_ex.get("type", "secondary")):
                if _ex.get("fn"):
                    _ex["fn"]()
    if (st.session_state.get("_codigos_ids")
            and st.session_state.get("_codigos_origem") == prefix):
        st.divider()
        _render_codigos(st.session_state["_codigos_ids"], fonte)


def _sync_ao_iniciar():
    """Uma vez por sessão: drena a fila e busca atualizações (delta) em BACKGROUND,
    SEM bloquear a abertura. A tela abre na hora com o cache local; quando o sync
    termina com novidades, sinaliza p/ a tela recarregar no próximo ciclo do
    fragmento (até ~90s). Também marca _auto_ultimo_ts p/ o fragmento não fazer um
    2º sync bloqueante logo na abertura."""
    if st.session_state.get("_sync_sessao"):
        return
    st.session_state["_sync_sessao"] = True
    st.session_state["_auto_ultimo_ts"] = _time.time()
    if gsheets.disponivel() and cache.contar() > 0:
        def _worker():
            try:
                _drenar_fila()
                m = gsheets.sync_delta()
                if m and (m.get("diferentes", m.get("mudadas")) or m.get("removidas")):
                    cache.set_meta("_ui_recarregar", "1")
            except Exception:
                pass
        threading.Thread(target=_worker, daemon=True).start()


# Bump este valor sempre que carregar_df() mudar as colunas derivadas. Garante que o
# cache seja invalidado mesmo em recarga automática (runOnSave) sem reiniciar o app.
SCHEMA_VERSION = "v6-conta_coluna_u"


@st.cache_data(show_spinner=False)
def df_cache(versao: str = SCHEMA_VERSION):
    # Reconstruído só quando a versão muda ou recarregar() chama st.cache_data.clear()
    # (após sincronização ou edição). Evita rebuild a cada interação.
    return dados.carregar_df()


# ----------------------------------------------------------------------------
# Helpers de formatação
# ----------------------------------------------------------------------------
def brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def aplicar_filtros(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    d = df.copy()
    if f["busca"]:
        termos = [t.strip().lower() for t in f["busca"].split(",") if t.strip()]
        campos = ["id", "credor", "documento", "descricao", "tipo_despesa",
                  "centro_custo", "responsavel", "nf", "pedido", "analise_ia"]
        # monta o texto de busca UMA vez (vetorizado), depois aplica máscaras
        blob = d[campos[0]].astype(str)
        for c in campos[1:]:
            blob = blob.str.cat(d[c].astype(str), sep=" ")
        blob = blob.str.lower()
        mask = pd.Series(True, index=d.index)
        for t in termos:
            mask &= blob.str.contains(t, regex=False, na=False)
        d = d[mask]
    if f["status_pgt"]:
        d = d[d["status_pgt"].isin(f["status_pgt"])]
    if f["conta"]:
        d = d[d["conta_fmt"].isin(f["conta"])]
    if f["forma"]:
        d = d[d["forma_pagamento"].isin(f["forma"])]
    if f["status_agend"]:
        sel = list(f["status_agend"])
        alvos = [s for s in sel if s != "Sem Agendamento"]
        mask = d["status_agend"].isin(alvos)
        if "Sem Agendamento" in sel:        # sem informação na coluna (status_agend vazio)
            mask = mask | (d["status_agend"].astype(str).str.strip() == "")
        d = d[mask]
    if f["tipo_despesa"]:
        d = d[d["tipo_despesa"].isin(f["tipo_despesa"])]
    if f.get("projeto"):
        d = d[d["projeto"].isin(f["projeto"])]
    if f["responsavel"]:
        d = d[d["responsavel"].isin(f["responsavel"])]
    if f["centro_custo"]:
        # CC pode vir concatenado -> match por "contém"
        padrao = "|".join(map(lambda x: x.replace("|", ""), f["centro_custo"]))
        d = d[d["centro_custo"].str.contains(padrao, case=False, na=False, regex=True)]
    if f.get("situacoes"):
        if "Pendências" in f["situacoes"]:
            d = d[d["status_pgt"].astype(str).str.strip().str.lower() == "pagar"]
        if "Risco de Duplicidade" in f["situacoes"] and "risco" in d.columns:
            d = d[d["risco"]]
        if "Cadastro incompleto" in f["situacoes"] and "alerta_laranja" in d.columns:
            d = d[d["alerta_laranja"]]
        if "Boleto Inválido" in f["situacoes"] and "codigo_barras" in d.columns:
            _fb = d["forma_pagamento"].astype(str).str.strip().str.lower().eq("boleto")
            _pagar = d["status_pgt"].astype(str).str.strip().str.lower().eq("pagar")
            _cb = d["codigo_barras"].astype(str)
            _txt = _cb.str.upper().str.replace("Á", "A", regex=False)
            _tem_invalido = _txt.str.contains("INVALIDO", na=False)
            _dig = _cb.str.replace(r"\D", "", regex=True)            # só dígitos
            _vazio_ou_zero = _dig.eq("") | _dig.str.fullmatch("0+").fillna(False)
            d = d[_fb & _pagar & (_tem_invalido | _vazio_ou_zero)]
        if "Boleto Duplicado" in f["situacoes"] and "codigo_barras" in df.columns:
            # Duplicidade apurada no conjunto COMPLETO (df). Conta repetições entre
            # boletos com status 'Pagar' OU 'Pago' (universo de risco de pagar 2x);
            # 'Cancelado' e outros não contam. Exibe só os que estão a Pagar — assim,
            # "1 Pago + 1 Pagar" mostra o Pagar (que ainda pode ser pago em duplicidade).
            _fb0 = df["forma_pagamento"].astype(str).str.strip().str.lower().eq("boleto")
            _st0 = df["status_pgt"].astype(str).str.strip().str.lower()
            _dig0 = df["codigo_barras"].astype(str).str.replace(r"\D", "", regex=True)
            _univ = (_fb0 & _dig0.ne("") & ~_dig0.str.fullmatch("0+").fillna(False)
                     & _st0.isin(["pagar", "pago"]))
            _cont = _dig0[_univ].value_counts()
            _dups = set(_cont[_cont > 1].index)
            _digd = d["codigo_barras"].astype(str).str.replace(r"\D", "", regex=True)
            _fbd = d["forma_pagamento"].astype(str).str.strip().str.lower().eq("boleto")
            _pagard = d["status_pgt"].astype(str).str.strip().str.lower().eq("pagar")
            d = d[_fbd & _pagard & _digd.isin(_dups)]
    if f["periodo_ini"]:
        d = d[d["vencimento_dt"] >= pd.Timestamp(f["periodo_ini"])]
    if f["periodo_fim"]:
        d = d[d["vencimento_dt"] <= pd.Timestamp(f["periodo_fim"])]
    if f.get("pgt_ini"):
        d = d[d["data_pagamento_dt"] >= pd.Timestamp(f["pgt_ini"])]
    if f.get("pgt_fim"):
        d = d[d["data_pagamento_dt"] <= pd.Timestamp(f["pgt_fim"])]
    if f["valor_ini"] is not None:
        d = d[d["valor_num"] >= f["valor_ini"]]
    if f["valor_fim"] is not None:
        d = d[d["valor_num"] <= f["valor_fim"]]

    ordem = f["ordenar"]
    if ordem in ("Vencimento", "Vencimento A-Z"):
        d = d.sort_values("vencimento_dt", na_position="last")
    elif ordem == "Vencimento Z-A":
        d = d.sort_values("vencimento_dt", ascending=False, na_position="last")
    elif ordem == "Valor crescente":
        d = d.sort_values("valor_num")
    elif ordem == "Valor decrescente":
        d = d.sort_values("valor_num", ascending=False)
    elif ordem == "Credor":
        d = d.sort_values("credor")
    else:
        d = d.sort_values("id")
    return d


def fmt_contabil(v) -> str:
    """Formato contábil BR: 1.234,56 e negativos entre parênteses."""
    try:
        n = float(v or 0)
    except (TypeError, ValueError):
        n = 0.0
    s = f"{abs(n):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"({s})" if n < 0 else s


def _fmt_moeda(v) -> str:
    """Valor em reais: 'R$ 1.234,56' (negativos entre parênteses)."""
    return "R$ " + fmt_contabil(v)


def extrair_ids_texto(texto: str) -> list:
    """Extrai números de SP de texto livre (mensagens do WhatsApp etc.).
    SPs têm 10 dígitos; captura qualquer número de exatamente 10 dígitos isolado
    (cobre 'Nº da SP: 1426036778', 'SP: 14...', '1417685204 | credor | valor',
    listas soltas). Telefones (11 díg.), CNPJ/CPF (blocos menores), valores e
    datas não casam. Remove duplicados preservando a ordem."""
    achados = re.findall(r"\b(\d{10})\b", str(texto or ""))
    vistos, out = set(), []
    for sp in achados:
        if sp not in vistos:
            vistos.add(sp)
            out.append(sp)
    return out


def _data_hora_br(s) -> str:
    """Converte 'AAAA-MM-DD HH:MM:SS' (como é gravado) para 'DD/MM/AAAA HH:MM:SS'.
    Se não conseguir converter, devolve o texto original."""
    txt = str(s or "").strip()
    if not txt or txt == "—":
        return txt or "—"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt).strftime(
                "%d/%m/%Y %H:%M:%S" if " " in txt or "T" in txt else "%d/%m/%Y")
        except ValueError:
            continue
    return txt


# Colunas exibidas no grid (Relatório e Lote são IGUAIS). O detalhamento completo
# fica no modal ao clicar na linha.
GRID_COLS = [
    ("pipefy_url",      "Abrir"),
    ("id",              "ID"),
    ("solicitacao",     "Data"),
    ("vencimento",      "Vencimento"),
    ("credor",          "Credor"),
    ("documento",       "CPF/CNPJ"),
    ("tipo_despesa",    "Tipo de Despesa"),
    ("centro_custo",    "Centro de Custo"),
    ("valor",           "Valor"),
    ("status_pgt",      "Status Pgt"),
    ("status_agend",    "Status Agend"),
    ("sp_fiscal",       "SP Fiscal"),
    ("forma_pagamento", "Forma de Pgt"),
    ("conta_fmt",       "Conta Corrente"),
    ("validacao",       "Validação"),
    ("info_pgt",        "Informação p/ Pgt"),
    ("nf",              "Nº NF"),
    ("data_pagamento",  "Data Pgt"),
    ("comprovante",     "Comprovante"),
    ("responsavel",     "Responsável"),
]

# Colunas de data exibidas como DD/MM/YYYY
_DATE_SRC = {"solicitacao", "vencimento", "data_pagamento"}


# ============================================================================
# LARGURA DAS COLUNAS (em pixels) — EDITE AQUI à vontade.
# Cada número é a largura fixa daquela coluna no grid (Solicitações, Lote,
# Relatório). Para descobrir um bom número: abra a aba "Sincronização" e veja
# "🔧 Larguras sugeridas" — ele mostra, para os dados atuais, quantos pixels
# cabem o maior texto de cada coluna. Copie de lá e ajuste a gosto.
# Regra de bolso: largura ≈ (nº de caracteres do maior texto) × 8 + 30.
# Se apagar uma coluna daqui, ela volta a se ajustar sozinha ao conteúdo.
# ============================================================================
LARGURAS_COLUNAS = {
    "sel":               44,    # caixa de seleção (ícone)
    "Abrir":             55,    # ➔ (só ícone)
    "ID":               130,
    "Data":             110,    # dd/mm/aaaa
    "Vencimento":       120,
    "Credor":           280,
    "CPF/CNPJ":         170,
    "Tipo de Despesa":  240,
    "Centro de Custo":  220,
    "Valor":            120,
    "Status Pgt":       120,
    "Status Agend":     130,
    "SP Fiscal":        110,
    "Forma de Pgt":     150,
    "Conta Corrente":   140,
    "Validação":        110,
    "Informação p/ Pgt": 220,
    "Nº NF":            100,
    "Data Pgt":         120,
    "Comprovante":      100,    # ↓☁ (só ícone)
    "Responsável":      170,
}

# Colunas que QUEBRAM o texto em várias linhas (em vez de cortar com "…").
# A linha cresce de altura para caber o conteúdo. Use para textos longos.
# Para uma coluna NÃO quebrar, é só tirar daqui. O texto quebra dentro da
# largura definida em LARGURAS_COLUNAS — quanto mais estreita, mais linhas.
QUEBRAR_TEXTO = {
    "Credor",
    "Tipo de Despesa",
    "Centro de Custo",
    "Informação p/ Pgt",
    "Responsável",
}

# ============================================================================
# CONFIGURAÇÃO DAS TABELAS (editável pela aba ⚙️ Configurações e salva no cache).
# Os valores de LARGURAS_COLUNAS e QUEBRAR_TEXTO acima são apenas os PADRÕES de
# fábrica; o que você ajustar na aba sobrescreve e persiste.
# ============================================================================
def _config_tabela_padrao() -> dict:
    # 'largura' em pixels; 0 = AUTOMÁTICO (o app dimensiona sozinho).
    cfg = {}
    for _src, lab in GRID_COLS:
        cfg[lab] = {
            "visivel": True,
            "largura": int(LARGURAS_COLUNAS.get(lab, 0)),    # 0 = automático
            "quebra": lab in QUEBRAR_TEXTO,
        }
    return cfg


def _carregar_config_tabela() -> dict:
    cfg = _config_tabela_padrao()
    try:
        raw = cache.get_meta("tabela_config", "")
        if raw:
            salvo = json.loads(raw)
            for lab, vals in salvo.items():
                if lab not in cfg or not isinstance(vals, dict):
                    continue
                if "visivel" in vals:
                    cfg[lab]["visivel"] = bool(vals["visivel"])
                if "quebra" in vals:
                    cfg[lab]["quebra"] = bool(vals["quebra"])
                # Largura: formato NOVO usa só 'largura' (0=auto). Formato ANTIGO
                # tinha 'padrao' (auto) + 'largura' — convertemos aqui.
                if "padrao" in vals:
                    cfg[lab]["largura"] = 0 if vals.get("padrao") else int(vals.get("largura", 0) or 0)
                elif "largura" in vals:
                    cfg[lab]["largura"] = int(vals.get("largura", 0) or 0)
    except Exception:
        pass
    return cfg


def _salvar_config_tabela(cfg: dict):
    cache.set_meta("tabela_config", json.dumps(cfg, ensure_ascii=False))
    st.session_state["_cfg_tabela"] = _carregar_config_tabela()


def _cfg_tabela() -> dict:
    """Config vigente (cacheada na sessão; recarrega do cache ao salvar)."""
    cfg = st.session_state.get("_cfg_tabela")
    if cfg is None:
        cfg = _carregar_config_tabela()
        st.session_state["_cfg_tabela"] = cfg
    return cfg


# Garante no máximo UM modal aberto por execução (Relatório + Lote rodam no mesmo run).
DIALOG_GUARD = {"used": False}


def _conteudo_detalhes(reg: dict):
    st.session_state["_det_flag"] = True   # sinaliza que o modal está visível neste run
    sp_id = reg.get("id", "")
    st.markdown(f"### SP {sp_id}")
    tem_risco = (str(reg.get("risco", "")).strip().lower() in ("true", "1", "sim")
                 or "COM RISCO" in str(reg.get("analise_ia", "")).upper())
    if st.session_state.get(f"_semrisco_{sp_id}"):
        tem_risco = False
    anexo = str(reg.get("anexo_link", "") or "").strip()
    comp = str(reg.get("comprovante", "") or "").strip()
    forma_bv = "beevale" in str(reg.get("forma_pagamento", "")).lower()
    validado = str(reg.get("validacao", "")).strip().lower() == "sim"
    _HOOK = "https://hook.us1.make.com/ssvbu6pgx5nlkzr3sqkw3yk141fkebua"

    # ---- Links (abrem em nova aba) ----
    links = [("Abrir Card", f"https://app.pipefy.com/open-cards/{sp_id}")]
    if anexo.startswith("http"):
        links.append(("Anexo", anexo))
    if comp.startswith("http"):
        links.append(("Comprovante", comp))
    links += [
        ("🔎 Consulta", f"{_HOOK}?id={sp_id}&acao=consultastatusomie"),
        ("🔄 Atualizar", f"{_HOOK}?id={sp_id}&acao=atualizatitulo"),
        ("🚫 Cancelar", "https://app.pipefy.com/public/form/RCvCgX9c"
                        f"?n_da_solicita_o={sp_id}&selecione_o_procedimento=Cancelar%20SP"),
    ]
    lc = st.columns(len(links))
    for i, (rotulo, url) in enumerate(links):
        lc[i].link_button(rotulo, url, use_container_width=True)

    # ---- Ações no app ----
    extras = []
    if not validado:                       # Validar só quando ainda NÃO está 'Sim'
        extras.append(("validar", "✅ Validar"))
    if forma_bv:
        extras.append(("beevale", "🐝 Gerar BeeVale"))
    if tem_risco:
        extras.append(("risco", "🟢 Remover Risco"))
    if extras:
        ec = st.columns([1] * len(extras) + [max(1, 6 - len(extras))])
        for i, (tipo, rotulo) in enumerate(extras):
            if tipo == "validar":
                if ec[i].button(rotulo, key=f"det_validar_{sp_id}", use_container_width=True):
                    st.session_state["_val_ids"] = [str(sp_id)]
                    st.session_state["_val_open"] = True
                    st.rerun()
            elif tipo == "beevale":
                if ec[i].button(rotulo, key=f"det_bv_{sp_id}", use_container_width=True):
                    st.session_state["bv_ger_ids"] = [str(sp_id)]
                    st.session_state.pop("bv_ger_res", None)
                    st.session_state["_bv_open"] = True
                    st.rerun()
            elif tipo == "risco":
                if ec[i].button(rotulo, key=f"det_remrisco_{sp_id}", type="primary",
                                use_container_width=True):
                    txt = f"SEM RISCO (revisado em {datetime.now().strftime('%d/%m/%Y')})"
                    # Aplica local na hora + grava na planilha em SEGUNDO PLANO (não trava).
                    cache.editar_local(str(sp_id), {"analise_ia": txt})
                    cache.enfileirar(str(sp_id), "analise_ia", txt)
                    cache.log_registrar(str(sp_id), "analise_ia", txt,
                                        "Detalhe: SEM RISCO", "pendente")
                    recarregar()
                    threading.Thread(target=_drenar_fila, daemon=True).start()
                    # Atualiza o próprio registro para o modal já refletir (sem fechar).
                    reg["analise_ia"] = txt
                    reg["risco"] = False
                    st.session_state[f"_semrisco_{sp_id}"] = True
                    st.toast("Risco removido — enviando ao Google Sheets em segundo plano…",
                             icon="✅")

    # ---- Agendamento (precisa Validação='Sim') + ações de pagamento, na MESMA
    # linha pra economizar vertical. Os 4 primeiros dependem de 'validado'. ----
    st.markdown("**Status de Agendamento**  ·  **Pagamento**")
    if not validado:
        st.caption("🔒 Agendamento bloqueado: a coluna **Validação** precisa estar "
                   f"como **'Sim'** (atual: '{reg.get('validacao', '') or '—'}').")
    _comp_url = str(reg.get("comprovante", "") or "").strip()
    sc = st.columns(6)
    for i, opt in enumerate(["Agendar", "Agendado", "Desagendar", "Falha Agendar"]):
        if sc[i].button(opt, key=f"det_ag_{i}_{sp_id}", disabled=not validado,
                        use_container_width=True):
            # Aplica sem travar e SEM fechar o modal (envio ao Sheets em 2º plano).
            aplicar_no_modal(reg, sp_id, "agendado", opt, acao=f"Detalhe: {opt}")
    if sc[4].button("🧹 Limpar Pgto", key=f"det_limpar_{sp_id}",
                    use_container_width=True,
                    help="Status Pgt → 'Pagar'; Agendado e Comprovante ficam vazios."):
        limpar_pagamento_modal(reg, sp_id)
    if sc[5].button("📤 Reenviar", key=f"det_reenv_{sp_id}",
                    use_container_width=True, disabled=not _comp_url,
                    help=("Baixa o comprovante (coluna AG) e envia por e-mail ao Make."
                          if _comp_url else "Sem link de comprovante neste registro.")):
        try:
            with st.spinner("Baixando comprovante e enviando por e-mail…"):
                _dados, _nome = _baixar_comprovante(_comp_url)
                _enviar_email_comprovante(_dados, _nome)
            st.success(f"Comprovante '{_nome}' enviado para o Make.")
            st.toast("Comprovante reenviado por e-mail.", icon="📤")
        except Exception as e:
            st.error(f"Falha ao reenviar comprovante: {e}")
    _aviso_fila_modal()

    # Destaque das pendências (o que precisa ser atualizado neste registro).
    pend = pagamentos.pendencias(reg.get("forma_pagamento", ""), reg.get("info_pgt", ""),
                                 reg.get("centro_custo", ""), reg.get("codigo_integracao", ""),
                                 reg.get("status_pgt", ""))
    if pend:
        st.warning("⚠️ **Pendências:** " + "  ·  ".join(pend)
                   + "\n\nAtualize estes itens no cadastro do lançamento.")

    # Risco de duplicidade (IA): mostra a análise e LINKS para os lançamentos apontados.
    if tem_risco:
        ai_txt = str(reg.get("analise_ia", "")).strip()
        if ai_txt:
            st.error(f"⚠️ **Risco de duplicidade (IA):** {ai_txt}")
        # IDs do Pipefy têm ~10 dígitos; ignora o próprio ID do registro.
        risco_ids = list(dict.fromkeys(
            i for i in re.findall(r"\d{9,}", ai_txt) if i != str(sp_id)))
        if risco_ids:
            st.caption("Lançamento(s) apontado(s) — clique para abrir no Pipefy:")
            rc = st.columns(min(len(risco_ids), 4))
            for i, rid in enumerate(risco_ids):
                rc[i % len(rc)].link_button(f"🔗 SP {rid}",
                                            f"https://app.pipefy.com/open-cards/{rid}",
                                            use_container_width=True)

    # tabela com todos os campos
    ocultar = {"_dirty"}
    linhas = []
    for k in KEPT_KEYS:
        if k in ocultar:
            continue
        val = reg.get(k, "")
        if k == "valor":
            val = fmt_contabil(reg.get("valor_num", 0))
        linhas.append({"Campo": LABELS.get(k, k), "Valor": "" if val is None else str(val)})
    st.dataframe(pd.DataFrame(linhas), **_FULLW, hide_index=True, height=520)


def _criar_dialog():
    """Cria o decorator de modal conforme o que a versão do Streamlit suporta."""
    if not hasattr(st, "dialog"):
        return None
    try:
        return st.dialog("Detalhes da SP", width="large")
    except TypeError:
        return st.dialog("Detalhes da SP")


_DIALOG = _criar_dialog()
if _DIALOG is not None:
    _dialog_detalhes = _DIALOG(_conteudo_detalhes)
else:
    def _dialog_detalhes(reg: dict):
        # sem suporte a modal -> mostra inline num expander
        with st.expander(f"Detalhes da SP {reg.get('id', '')}", expanded=True):
            _conteudo_detalhes(reg)


def _strike(t) -> str:
    """Tacha o texto com o caractere combinante U+0336 (funciona em qualquer célula)."""
    return "".join(ch + "\u0336" for ch in str(t))


def _conteudo_cadastro_beevale():
    st.caption("Cole os **e-mails** (11díg@bwsconstrucoes.com.br) ou **CPFs**. A planilha de "
               "cadastro é gerada a partir da base 'Dados Documentos'.")
    txt = st.text_area("E-mails / CPFs", height=160, key="bv_cad_txt",
                       placeholder="12345678901@bwsconstrucoes.com.br\n"
                                   "98765432100@bwsconstrucoes.com.br")
    if st.button("📇 Gerar cadastro", type="primary", key="bv_cad_gerar"):
        cpfs = beevale.extrair_cpfs(txt)
        if not cpfs:
            st.session_state["bv_cad_res"] = {"erro": "Nenhum CPF identificado no texto."}
        else:
            try:
                encontrados, nao = beevale.buscar_registros_por_cpf(cpfs)
                if not encontrados:
                    st.session_state["bv_cad_res"] = {
                        "erro": "Nenhum dos CPFs foi encontrado na base.", "nao": nao}
                else:
                    st.session_state["bv_cad_res"] = {
                        "xlsx": beevale.cadastro_xlsx(encontrados),
                        "ident": len(cpfs), "gerados": len(encontrados), "nao": nao}
            except Exception as e:
                st.session_state["bv_cad_res"] = {"erro": f"Falha ao gerar: {e}"}

    res = st.session_state.get("bv_cad_res")
    if res:
        if res.get("erro"):
            st.error(res["erro"])
            if res.get("nao"):
                st.caption("Não encontrados: " + ", ".join(res["nao"]))
        else:
            st.success(f"CPFs identificados: **{res['ident']}** · Gerados: **{res['gerados']}** · "
                       f"Não encontrados: **{len(res.get('nao', []))}**")
            if res.get("nao"):
                st.caption("Não encontrados: " + ", ".join(res["nao"]))
            st.download_button(
                "⬇️ Baixar Cadastro BeeVale", res["xlsx"],
                file_name=f"Cadastro_BeeVale_{datetime.now().strftime('%d.%m.%Y_%H.%M.%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="bv_cad_dl")


def _criar_dialog_cad():
    if not hasattr(st, "dialog"):
        return None
    try:
        return st.dialog("Planilha Cadastro BeeVale", width="large")
    except TypeError:
        return st.dialog("Planilha Cadastro BeeVale")


_DIALOG_CAD = _criar_dialog_cad()
if _DIALOG_CAD is not None:
    _dialog_cadastro_beevale = _DIALOG_CAD(_conteudo_cadastro_beevale)
else:
    def _dialog_cadastro_beevale():
        with st.expander("Planilha Cadastro BeeVale", expanded=True):
            _conteudo_cadastro_beevale()


def _conteudo_gerar_beevale():
    ids = st.session_state.get("bv_ger_ids", [])
    st.caption(f"{len(ids)} card(s) BeeVale. Para cada card: busca CPF/valor no Pipefy e os "
               "dados do colaborador na database, gera **Pagamento + Cadastro**, sobe no "
               "Drive e **atualiza a descrição do card** com os links (+ Documentação "
               "Fiscal = BeeVale).")
    if not config.tem_token("PIPEFY_TOKEN"):
        st.error("Token do Pipefy não configurado. Defina a chave **PIPEFY_TOKEN** na planilha "
                 "de tokens (e use *Atualizar tokens*) ou a variável de ambiente PIPEFY_TOKEN.")
        return
    if st.button("🐝 Gerar, subir no Drive e atualizar cards", type="primary", key="bv_ger_run"):
        try:
            with st.spinner("Consultando Pipefy, gerando planilhas, subindo no Drive e "
                            "atualizando os cards..."):
                st.session_state["bv_ger_res"] = beevale.gerar_beevale(ids)
        except Exception as e:
            st.session_state["bv_ger_res"] = {"erro": str(e)}
    res = st.session_state.get("bv_ger_res")
    if res:
        if res.get("erro"):
            st.error(res["erro"])
            return
        ok = res.get("ok_ids", [])
        atualizados = res.get("atualizados", [])
        err = res.get("erros", [])
        st.success(f"Gerados: **{len(ok)}** · Cards atualizados: **{len(atualizados)}** · "
                   f"Erros: **{len(err)}**")
        if res.get("resultados"):
            st.markdown("**Links gerados por card** (já gravados na descrição):")
            st.dataframe(pd.DataFrame(res["resultados"]), hide_index=True, **_FULLW,
                         column_config={
                             "Pagamento": st.column_config.LinkColumn("Pagamento"),
                             "Cadastro": st.column_config.LinkColumn("Cadastro")})
        if err:
            st.markdown("**Erros:**")
            st.dataframe(pd.DataFrame(err), hide_index=True, **_FULLW)


def _criar_dialog_ger():
    if not hasattr(st, "dialog"):
        return None
    try:
        return st.dialog("Gerar BeeVale", width="large")
    except TypeError:
        return st.dialog("Gerar BeeVale")


_DIALOG_GER = _criar_dialog_ger()
if _DIALOG_GER is not None:
    _dialog_gerar_beevale = _DIALOG_GER(_conteudo_gerar_beevale)
else:
    def _dialog_gerar_beevale():
        with st.expander("Gerar BeeVale", expanded=True):
            _conteudo_gerar_beevale()


def _conteudo_validar():
    ids = [str(i) for i in st.session_state.get("_val_ids", [])]
    if not ids:
        st.info("Nenhuma SP selecionada.")
        return
    alvo = f"a SP **{ids[0]}**" if len(ids) == 1 else f"**{len(ids)} SP(s)** selecionada(s)"
    st.markdown(f"Validar {alvo} — marca a coluna **Validação = Sim**.")
    senha_ok = config.get_token("SENHA_VALIDACAO")
    if not senha_ok:
        st.error("Senha de validação não configurada. Inclua a chave **SENHA_VALIDACAO** na "
                 "planilha de Credenciais e use *Sincronização → Atualizar tokens*.")
        return
    senha = st.text_input("Senha de validação", type="password", key="val_senha")
    if st.button("✅ Enviar validação", type="primary", key="val_enviar"):
        if senha != senha_ok:
            st.error("Senha incorreta.")
        else:
            # 1) aplica na hora (local): a tela já mostra Validação = 'Sim'
            for sp_id in ids:
                cache.editar_local(sp_id, {"validacao": "Sim"})
                cache.enfileirar(sp_id, "validacao", "Sim")
                cache.log_registrar(sp_id, "validacao", "Sim", "Validar", "pendente")
            recarregar()
            # 2) grava na planilha em SEGUNDO PLANO (não trava o uso do app)
            threading.Thread(target=_drenar_fila, daemon=True).start()
            st.session_state["_flash"] = ("success",
                f"✅ {len(ids)} SP(s) validada(s). Enviando para a planilha em segundo plano "
                "(você já pode continuar usando).")
            st.rerun()


def _criar_dialog_val():
    if not hasattr(st, "dialog"):
        return None
    try:
        return st.dialog("Validar SP", width="small")
    except TypeError:
        return st.dialog("Validar SP")


_DIALOG_VAL = _criar_dialog_val()
if _DIALOG_VAL is not None:
    _dialog_validar = _DIALOG_VAL(_conteudo_validar)
else:
    def _dialog_validar():
        with st.expander("Validar SP", expanded=True):
            _conteudo_validar()


def _conteudo_cancelar_sp():
    ids = [str(i) for i in st.session_state.get("_cancel_ids", [])]
    if not ids:
        st.info("Nenhuma SP selecionada.")
        return
    st.warning(f"Isto vai abrir **{len(ids)} pedido(s) de cancelamento** no Pipefy "
               "(pipe Solicitações Administrativas), um card por SP, com o "
               "procedimento **'Cancelar SP'** e o motivo "
               f"_'{'Cancelamento em Lote - Análise de SPs Streamlit'}'_.")
    mostrar = ", ".join(ids[:30]) + (f" … (+{len(ids) - 30})" if len(ids) > 30 else "")
    st.caption(f"SPs: {mostrar}")
    if not config.tem_token("PIPEFY_TOKEN"):
        st.error("Token do Pipefy não configurado. Defina **PIPEFY_TOKEN** na planilha "
                 "de tokens e use *Sincronização → Atualizar tokens*.")
        return
    ok = st.checkbox(f"Confirmo a solicitação de cancelamento de {len(ids)} SP(s)",
                     key="cancel_confirmo")
    if st.button("🚫 Enviar pedidos de cancelamento", type="primary",
                 disabled=not ok, key="cancel_enviar"):
        try:
            import pipefy
            with st.spinner(f"Criando {len(ids)} card(s) no Pipefy…"):
                st.session_state["_cancel_res"] = pipefy.criar_cards_cancelamento(ids)
        except Exception as e:
            st.session_state["_cancel_res"] = [{"sp": "-", "ok": False, "erro": str(e)}]
    res = st.session_state.get("_cancel_res")
    if res:
        oks = [r for r in res if r.get("ok")]
        errs = [r for r in res if not r.get("ok")]
        if oks:
            st.success(f"✅ {len(oks)} pedido(s) criado(s).")
        if errs:
            st.error(f"❌ {len(errs)} falha(s).")
            st.dataframe(pd.DataFrame(errs)[["sp", "erro"]], hide_index=True, **_FULLW)
        if oks:
            st.dataframe(pd.DataFrame(oks)[["sp", "card_id"]], hide_index=True, **_FULLW)
        st.caption("Os cards seguem o fluxo normal do pipe — o cancelamento em si "
                   "acontece por lá; aqui nada foi alterado nas SPs.")


def _criar_dialog_cancel():
    if not hasattr(st, "dialog"):
        return None
    try:
        return st.dialog("Cancelar SP", width="large")
    except TypeError:
        return st.dialog("Cancelar SP")


_DIALOG_CANCEL = _criar_dialog_cancel()
if _DIALOG_CANCEL is not None:
    _dialog_cancelar_sp = _DIALOG_CANCEL(_conteudo_cancelar_sp)
else:
    def _dialog_cancelar_sp():
        with st.expander("Cancelar SP", expanded=True):
            _conteudo_cancelar_sp()


def _conteudo_relatorio_lote():
    ids = [str(i) for i in st.session_state.get("_lote_rel_ids", [])]
    if not ids:
        st.info("Marque as SPs nas tabelas do Lote e clique de novo em "
                "**📄 Relatório do Lote**.")
        return
    d = df[df["id"].astype(str).isin(ids)].copy()
    if d.empty:
        st.warning("As SPs selecionadas não estão no cache local.")
        return
    mapa_sel = st.session_state.get("_lote_sel_grupo_map", {})
    mapa_ger = st.session_state.get("_lote_grupos_map", {})
    d["grupo_lote"] = (d["id"].astype(str)
                       .map(lambda x: mapa_sel.get(x) or mapa_ger.get(x))
                       .fillna("(painéis por status)"))
    tot = float(pd.to_numeric(d["valor_num"], errors="coerce").fillna(0).sum())
    st.caption(f"{len(d)} SP(s) · Total {_fmt_moeda(tot)} · "
               f"{d['grupo_lote'].nunique()} grupo(s)")

    def _agg(dim):
        return (d.assign(_v=pd.to_numeric(d["valor_num"], errors="coerce").fillna(0),
                         _d=d[dim].astype(str).str.strip().replace("", "(vazio)"))
                  .groupby("_d", as_index=False).agg(Qtd=("_v", "size"), Total=("_v", "sum"))
                  .sort_values("Total", ascending=False))

    def _tab_pdf(g, rotulo):
        return ([rotulo, "Qtd", "Total"],
                [[r["_d"], str(int(r["Qtd"])), _fmt_moeda(r["Total"])]
                 for _, r in g.iterrows()])

    if st.button("📄 Gerar PDF e Excel", type="primary", key="lote_rel_gerar"):
        with st.spinner("Montando o relatório…"):
            g_grupo = _agg("grupo_lote")
            g_cc = _agg("centro_custo")
            g_tipo = _agg("tipo_despesa")
            g_credor = _agg("credor")
            tabelas = {
                "Por Grupo do Lote": _tab_pdf(g_grupo, "Grupo"),
                "Por Centro de Custo": _tab_pdf(g_cc, "Centro de Custo"),
                "Por Tipo de Despesa": _tab_pdf(g_tipo, "Tipo de Despesa"),
                "Por Credor": _tab_pdf(g_credor, "Credor"),
            }
            _fl = relatorio.fluxo_despesas(d)
            if _fl and not _fl["tabela"].empty:
                t = _fl["tabela"]
                tabelas[f"Fluxo de Despesas — {_fl['nivel']} (por vencimento)"] = (
                    ["Período", "Qtd", "Total"],
                    [[r["Período"], str(int(r["Qtd"])), _fmt_moeda(r["Total"])]
                     for _, r in t.iterrows()])
            kpis = [("SPs", str(len(d))), ("Total", _fmt_moeda(tot)),
                    ("Grupos do Lote", str(d["grupo_lote"].nunique()))]
            analitico = relatorio.analitico_despesas(
                d, agrupar_por="grupo_lote",
                titulo="Relatório Analítico das Despesas (por Grupo do Lote)")
            st.session_state["_lote_rel_pdf"] = relatorio.gerar_pdf(
                "Relatório do Lote",
                f"gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                kpis, tabelas, analitico=analitico)
            # ---- Excel: mesmas visões em abas + dados linha a linha ----
            def _aba(g, rotulo):
                return g.rename(columns={"_d": rotulo})
            abas = {"PorGrupoLote": _aba(g_grupo, "Grupo do Lote"),
                    "PorCentroCusto": _aba(g_cc, "Centro de Custo"),
                    "PorTipoDespesa": _aba(g_tipo, "Tipo de Despesa"),
                    "PorCredor": _aba(g_credor, "Credor")}
            if _fl and not _fl["tabela"].empty:
                abas["Fluxo"] = _fl["tabela"]
            _cols_dados = [("id", "ID"), ("grupo_lote", "Grupo do Lote"),
                           ("vencimento", "Vencimento"), ("credor", "Credor"),
                           ("cpf_cnpj", "CPF/CNPJ"), ("descricao", "Descrição"),
                           ("tipo_despesa", "Tipo de Despesa"),
                           ("centro_custo", "Centro de Custo"),
                           ("forma_pagamento", "Forma"), ("status_pgt", "Status Pgt"),
                           ("valor_num", "Valor")]
            dados = pd.DataFrame({rot: d[k] for k, rot in _cols_dados if k in d.columns})
            resumo = {"SPs": len(d), "Total": tot,
                      "Grupos do Lote": int(d["grupo_lote"].nunique())}
            st.session_state["_lote_rel_xlsx"] = relatorio.gerar_xlsx(
                "Relatório do Lote", resumo, abas, dados)
    _dl1, _dl2 = st.columns(2)
    if st.session_state.get("_lote_rel_pdf"):
        _dl1.download_button(
            "⬇️ Baixar PDF", st.session_state["_lote_rel_pdf"],
            file_name=f"Relatorio_Lote_{datetime.now().strftime('%d.%m.%Y_%H.%M')}.pdf",
            mime="application/pdf", key="lote_rel_dl", **_FULLW)
    if st.session_state.get("_lote_rel_xlsx"):
        _dl2.download_button(
            "⬇️ Baixar Excel", st.session_state["_lote_rel_xlsx"],
            file_name=f"Relatorio_Lote_{datetime.now().strftime('%d.%m.%Y_%H.%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="lote_rel_dl_x", **_FULLW)


def _criar_dialog_rel_lote():
    if not hasattr(st, "dialog"):
        return None
    try:
        return st.dialog("Relatório do Lote", width="large")
    except TypeError:
        return st.dialog("Relatório do Lote")


_DIALOG_REL_LOTE = _criar_dialog_rel_lote()
if _DIALOG_REL_LOTE is not None:
    _dialog_relatorio_lote = _DIALOG_REL_LOTE(_conteudo_relatorio_lote)
else:
    def _dialog_relatorio_lote():
        with st.expander("Relatório do Lote", expanded=True):
            _conteudo_relatorio_lote()


def _monta_visao(d: pd.DataFrame) -> pd.DataFrame:
    visao = pd.DataFrame(index=d.index)
    for src, lab in GRID_COLS:
        if src == "valor":
            visao[lab] = d["valor_num"].values                       # numérico (NumberColumn)
        elif src in _DATE_SRC:
            dt = d[src + "_dt"]
            visao[lab] = np.where(dt.notna(), dt.dt.strftime("%d/%m/%Y"),
                                  d[src].astype(str)).astype(str)
        elif src == "status_pgt":
            visao[lab] = [_strike(v) if str(v).strip().lower() == "cancelado" else v
                          for v in d["status_pgt"].astype(str).values]
        elif src in d.columns:
            visao[lab] = d[src].values
        else:
            visao[lab] = ""
    return visao


def _estiliza(visao: pd.DataFrame, d: pd.DataFrame):
    """Cores por célula (Vencimento, Status Pgt, Status Agend)."""
    hoje = pd.Timestamp(datetime.now().date())
    venc = d["vencimento_dt"].dt.normalize() if "vencimento_dt" in d.columns else pd.Series(pd.NaT, index=d.index)
    pgt = d["status_pgt"].astype(str).str.strip().str.lower()
    pendente = pgt.eq("pagar")

    venc_cor = pd.Series("", index=d.index)
    venc_cor[pendente & venc.notna() & (venc < hoje)] = "color:#ff0000"   # atrasada
    venc_cor[pendente & venc.notna() & (venc == hoje)] = "color:#fbbc04"  # vence hoje

    pgt_cor = pd.Series("", index=d.index)
    pgt_cor[pgt.eq("pagar")] = "color:#ff0000"
    pgt_cor[pgt.eq("pago")] = "color:#0000ff"

    ag = d["status_agend"].astype(str).str.strip()
    ag_cor = pd.Series("", index=d.index)
    ag_cor[ag.eq("Agendar")] = "color:#9900ff"
    ag_cor[ag.eq("Agendado")] = "color:#38761d"
    ag_cor[ag.eq("Falha Agendar")] = "color:#ff9900"

    def _col(serie):
        return lambda s: serie.reindex(s.index).values

    sty = visao.style
    if "Vencimento" in visao.columns:
        sty = sty.apply(_col(venc_cor), subset=["Vencimento"])
    if "Status Pgt" in visao.columns:
        sty = sty.apply(_col(pgt_cor), subset=["Status Pgt"])
    if "Status Agend" in visao.columns:
        sty = sty.apply(_col(ag_cor), subset=["Status Agend"])
    return sty


def _grid_dataframe(d: pd.DataFrame, key: str, selection_mode: str = "single-row"):
    """Fallback (sem st_aggrid): grid via st.dataframe; seleção única abre o modal."""
    visao = _monta_visao(d)
    sty = _estiliza(visao, d)

    colcfg = {
        "Abrir":       st.column_config.LinkColumn("", display_text="➔", width="small"),
        "Valor":       st.column_config.NumberColumn("Valor", format="localized"),
        "Comprovante": st.column_config.LinkColumn("Comprov.", display_text="↓☁", width="small"),
    }

    try:
        ev = st.dataframe(sty, **_FULLW, hide_index=True, height=520, column_config=colcfg,
                          on_select="rerun", selection_mode=selection_mode, key=key)
        sel_idx = list(ev.selection.rows) if ev and ev.selection else []
        sel_ids = d.iloc[sel_idx]["id"].tolist()
    except TypeError:
        st.dataframe(sty, **_FULLW, hide_index=True, height=520, column_config=colcfg)
        return []

    marca = f"_modal_visto_{key}"
    if len(sel_idx) == 1 and not DIALOG_GUARD["used"]:
        if st.session_state.get(marca) != sel_ids[0]:
            st.session_state[marca] = sel_ids[0]
            DIALOG_GUARD["used"] = True
            _dialog_detalhes(d.iloc[sel_idx[0]].to_dict())
    elif len(sel_idx) != 1:
        st.session_state.pop(marca, None)

    return sel_ids


# ---- AgGrid -----------------------------------------------------------------
if _TEM_AGGRID:
    _JS_LINK_ABRIR = JsCode(
        "class A{init(p){this.e=document.createElement('a');"
        "if(p.value){this.e.href=p.value;this.e.target='_blank';this.e.rel='noopener';"
        "this.e.style.textDecoration='none';this.e.style.fontSize='15px';"
        "this.e.textContent='\u279c';}}getGui(){return this.e;}}")
    _JS_LINK_COMP = JsCode(
        "class C{init(p){this.e=document.createElement('span');"
        "if(p.value){var a=document.createElement('a');a.href=p.value;a.target='_blank';"
        "a.rel='noopener';a.style.textDecoration='none';a.textContent='\u2193\u2601';"
        "this.e.appendChild(a);}}getGui(){return this.e;}}")
    _JS_FMT_VALOR = JsCode(
        "function(p){if(p.value==null||p.value==='')return '';"
        "return Number(p.value).toLocaleString('pt-BR',"
        "{minimumFractionDigits:2,maximumFractionDigits:2});}")
    _JS_STY_VENC = JsCode("function(p){return p.data._venc_cor?{color:p.data._venc_cor}:{};}")
    _JS_STY_PGT = JsCode(
        "function(p){var s={};if(p.data._pgt_cor)s['color']=p.data._pgt_cor;"
        "if(p.data._cancelado)s['text-decoration']='line-through';return s;}")
    _JS_STY_AG = JsCode("function(p){return p.data._ag_cor?{color:p.data._ag_cor}:{};}")
    _JS_STY_ID = JsCode(
        "function(p){return p.data._id_cor?"
        "{color:p.data._id_cor,'font-weight':'700'}:{};}")

# Teto de linhas enviadas ao AgGrid (JSON pesado). KPIs usam o total filtrado.
_LIMITE_AG = 2000


def _monta_visao_ag(d: pd.DataFrame) -> pd.DataFrame:
    hoje = pd.Timestamp(datetime.now().date())
    venc = (d["vencimento_dt"].dt.normalize() if "vencimento_dt" in d.columns
            else pd.Series(pd.NaT, index=d.index))
    pgt = d["status_pgt"].astype(str).str.strip().str.lower()
    pend = pgt.eq("pagar")
    venc_cor = pd.Series("", index=d.index)
    venc_cor[pend & venc.notna() & (venc < hoje)] = "#ff0000"
    venc_cor[pend & venc.notna() & (venc == hoje)] = "#fbbc04"
    pgt_cor = pd.Series("", index=d.index)
    pgt_cor[pgt.eq("pagar")] = "#ff0000"
    pgt_cor[pgt.eq("pago")] = "#0000ff"
    ag = d["status_agend"].astype(str).str.strip()
    ag_cor = pd.Series("", index=d.index)
    ag_cor[ag.eq("Agendar")] = "#9900ff"
    ag_cor[ag.eq("Agendado")] = "#38761d"
    ag_cor[ag.eq("Falha Agendar")] = "#ff9900"

    v = pd.DataFrame(index=d.index)
    v["sel"] = ""
    for src, lab in GRID_COLS:
        if src == "valor":
            v[lab] = d["valor_num"].values
        elif src in _DATE_SRC:
            dt = d[src + "_dt"]
            v[lab] = np.where(dt.notna(), dt.dt.strftime("%d/%m/%Y"),
                              d[src].astype(str)).astype(str)
        elif src in d.columns:
            v[lab] = d[src].values
        else:
            v[lab] = ""
    v["_id"] = d["id"].astype(str).values
    v["_venc_cor"] = venc_cor.values
    v["_pgt_cor"] = pgt_cor.values
    v["_ag_cor"] = ag_cor.values
    v["_cancelado"] = pgt.eq("cancelado").values
    # ID: vermelho quando risco (AL "COM RISCO"); laranja quando cadastro incompleto
    # (Pix sem chave / sem Centro de Custo). Vermelho tem prioridade.
    risco = d["risco"].values if "risco" in d.columns else np.full(len(d), False)
    alerta = d["alerta_laranja"].values if "alerta_laranja" in d.columns else np.full(len(d), False)
    v["_id_cor"] = np.where(risco, "#d50000", np.where(alerta, "#ff9900", ""))
    return v


def _w_conteudo(serie, header, px=10.0, pad=40, mn=70, mx=520):
    """Largura (px) que cabe o maior texto da coluna — calculada no servidor (rápido).
    Folga proposital (px por caractere alto) para nada cortar mesmo se a fonte variar
    entre PCs/versões do AgGrid. Um pouco de espaço sobrando é aceitável."""
    try:
        n = max([len(str(header))] + [len(str(x)) for x in serie.values])
    except Exception:
        n = len(str(header))
    return int(min(max(n * px + pad, mn), mx))


def _grid_aggrid(d: pd.DataFrame, key: str, altura: int = 520, copiar_coluna: bool = True):
    """Grid via AgGrid: DUPLO clique abre o detalhe; caixa de seleção é independente."""
    truncado = len(d) > _LIMITE_AG
    d_view = d.head(_LIMITE_AG) if truncado else d
    if truncado:
        st.caption(f"Mostrando as primeiras **{_LIMITE_AG}** de **{len(d)}** linhas — "
                   "refine os filtros para ver o restante (os KPIs consideram tudo).")

    v = _monta_visao_ag(d_view)
    gb = GridOptionsBuilder.from_dataframe(v)
    # suppressSizeToFit: impede o AgGrid de espremer as colunas pra caber na tela
    # (sizeColumnsToFit). Assim a largura que definimos é respeitada — sobra rolagem
    # horizontal quando o total passa da tela, mas nada fica cortado.
    gb.configure_default_column(sortable=True, filter=True, resizable=True,
                                suppressSizeToFit=True, minWidth=45)
    gb.configure_selection("multiple", use_checkbox=True, header_checkbox=True,
                           suppressRowClickSelection=True)

    # Configuração (aba ⚙️ Configurações): largura, quebra e visibilidade por coluna.
    _cfg = _cfg_tabela()
    _ICON_W = {"sel": 44, "Abrir": 55, "Comprovante": 95}
    # Assinatura da config: muda quando você altera largura/visibilidade/quebra.
    # Entra no 'key' do AgGrid para forçar o grid a RECARREGAR e aplicar o novo
    # layout (sem isso, o AgGrid mantém o estado antigo das colunas e nada muda).
    _sig_cfg = zlib.crc32(json.dumps(_cfg, sort_keys=True).encode("utf-8")) & 0xFFFFFFFF
    _grid_key = f"{key}_{_sig_cfg:x}"

    def _larg(lab: str) -> int:
        c = _cfg.get(lab, {})
        larg = int(c.get("largura", 0) or 0)              # 0 = automático
        if lab in _ICON_W:
            # Ícone: NUNCA medir conteúdo (a URL por trás é enorme).
            return larg if larg > 0 else _ICON_W[lab]
        if larg > 0:
            return larg
        return _w_conteudo(v[lab], lab) if lab in v.columns else 120

    def _wrap(lab: str) -> dict:
        if lab in _ICON_W:
            return {}
        return {"wrapText": True, "autoHeight": True} if _cfg.get(lab, {}).get("quebra") else {}

    def _vis(lab: str) -> bool:
        # 'sel' (caixa de seleção) sempre visível; o resto segue a config.
        return True if lab == "sel" else bool(_cfg.get(lab, {}).get("visivel", True))

    # Cada coluna é configurada UMA ÚNICA VEZ. 'width' é a largura INICIAL (do
    # dicionário); como o minWidth é baixo (45), você consegue ARRASTAR para
    # diminuir/aumentar à vontade. Os ícones (sel/Abrir/Comprovante) ficam travados.
    gb.configure_column("sel", headerName="", width=_larg("sel"), minWidth=_larg("sel"),
                        maxWidth=_larg("sel"), pinned="left", filter=False,
                        sortable=False, resizable=False)
    gb.configure_column("Abrir", headerName="", width=_larg("Abrir"),
                        cellRenderer=_JS_LINK_ABRIR, filter=False, sortable=False,
                        hide=not _vis("Abrir"))
    gb.configure_column("ID", cellStyle=_JS_STY_ID, width=_larg("ID"), hide=not _vis("ID"))
    _JS_CMP_DATA = JsCode(
        "function(a,b){function p(s){if(!s)return 0;var x=String(s).split('/');"
        "return x.length===3?(parseInt(x[2])*10000+parseInt(x[1])*100+parseInt(x[0])):0;}"
        "return p(a)-p(b);}")
    gb.configure_column("Vencimento", cellStyle=_JS_STY_VENC, width=_larg("Vencimento"),
                        comparator=_JS_CMP_DATA, hide=not _vis("Vencimento"))
    gb.configure_column("Status Pgt", cellStyle=_JS_STY_PGT, width=_larg("Status Pgt"),
                        hide=not _vis("Status Pgt"))
    gb.configure_column("Status Agend", cellStyle=_JS_STY_AG, width=_larg("Status Agend"),
                        hide=not _vis("Status Agend"))
    gb.configure_column("Valor", type=["numericColumn"], valueFormatter=_JS_FMT_VALOR,
                        width=_larg("Valor"), hide=not _vis("Valor"))
    gb.configure_column("Responsável", width=_larg("Responsável"),
                        hide=not _vis("Responsável"), **_wrap("Responsável"))
    gb.configure_column("Informação p/ Pgt", width=_larg("Informação p/ Pgt"),
                        hide=not _vis("Informação p/ Pgt"), **_wrap("Informação p/ Pgt"))
    gb.configure_column("Comprovante", headerName="Comprov.", width=_larg("Comprovante"),
                        cellRenderer=_JS_LINK_COMP, filter=False, sortable=False,
                        hide=not _vis("Comprovante"))
    # Demais colunas (texto): uma chamada cada.
    _JA_CONFIG = {"sel", "Abrir", "Comprovante", "ID", "Vencimento", "Status Pgt",
                  "Status Agend", "Valor", "Responsável", "Informação p/ Pgt"}
    for _src, _lab in GRID_COLS:
        if _lab in _JA_CONFIG or _lab not in v.columns:
            continue
        if _src in _DATE_SRC:                          # Data, Data Pgt: ordena como data
            gb.configure_column(_lab, width=_larg(_lab), hide=not _vis(_lab),
                                comparator=_JS_CMP_DATA, **_wrap(_lab))
        else:
            gb.configure_column(_lab, width=_larg(_lab), hide=not _vis(_lab), **_wrap(_lab))
    for c in ["_id", "_venc_cor", "_pgt_cor", "_ag_cor", "_cancelado", "_id_cor"]:
        gb.configure_column(c, hide=True)
    gb.configure_grid_options(enableCellTextSelection=True, ensureDomOrder=True)
    go = gb.build()

    resp = AgGrid(v, gridOptions=go, allow_unsafe_jscode=True,
                  fit_columns_on_grid_load=False,
                  update_on=["cellDoubleClicked", "selectionChanged"],
                  theme="streamlit", height=altura, key=_grid_key,
                  show_toolbar=True, show_search=False, show_download_button=True)

    # DUPLO clique numa célula abre o detalhe. eventData é o evento "cru" do AG Grid:
    # tentamos achar o ID por vários caminhos (data._id, data.ID, node.data, ou rowIndex).
    ev = getattr(resp, "event_data", None)
    if ev:
        sig = json.dumps(ev, sort_keys=True, default=str)
        marca = f"_agev_{key}"
        if st.session_state.get(marca) != sig:          # só um evento NOVO age
            st.session_state[marca] = sig
            col = ev.get("colId") or (ev.get("column") or {}).get("colId")
            data = ev.get("data") or (ev.get("node") or {}).get("data") or {}
            rid = data.get("_id") or data.get("ID")
            if not rid and ev.get("rowIndex") is not None:
                try:
                    rid = str(d_view.iloc[int(ev["rowIndex"])]["id"])
                except Exception:
                    rid = None
            # SÓ o duplo-clique abre o detalhe. O 'selectionChanged' (que dispara
            # junto, ao marcar checkbox ou clicar na linha) NÃO deve abrir/piscar
            # o modal. Se a lib não informar o tipo, exigimos colId (só cliques em
            # célula têm) como aproximação.
            _tipo_ev = str(ev.get("type") or ev.get("eventType") or "")
            _eh_dbl = (_tipo_ev == "cellDoubleClicked") if _tipo_ev else bool(col)
            if (rid and _eh_dbl and col not in ("sel", "Abrir", "Comprovante")
                    and not DIALOG_GUARD["used"]):
                sub = d[d["id"].astype(str) == str(rid)]
                if not sub.empty:
                    DIALOG_GUARD["used"] = True
                    _dialog_detalhes(sub.iloc[0].to_dict())

    # Copiar uma coluna inteira (Community não tem cópia retangular tipo Excel).
    if copiar_coluna:
        with st.expander("📋 Copiar uma coluna"):
            _ocultas = ("sel", "Abrir", "Comprovante", "_id",
                        "_venc_cor", "_pgt_cor", "_ag_cor", "_cancelado", "_id_cor")
            _cols = [c for c in v.columns if c not in _ocultas]
            if _cols:
                cc = st.selectbox("Coluna", _cols, key=f"copcol_{key}")
                _vals = v[cc].astype(str).tolist()
                st.caption(f"{len(_vals)} valores (das linhas exibidas) — clique no ícone de "
                           "copiar no canto do bloco e cole numa coluna da planilha.")
                st.code("\n".join(_vals) or " ", language=None)

    # Seleção reservada para o futuro (não faz nada por enquanto).
    try:
        sel = resp.selected_rows
        if sel is None:
            return []
        if hasattr(sel, "columns"):
            return [str(x) for x in sel["_id"].tolist()] if "_id" in sel.columns else []
        return [str(r.get("_id")) for r in sel if r.get("_id")]
    except Exception:
        return []


def grid(d: pd.DataFrame, key: str, selection_mode: str = "single-row", altura: int = 520,
         copiar_coluna: bool = True):
    """Usa AgGrid quando disponível; senão cai no st.dataframe."""
    if _TEM_AGGRID:
        return _grid_aggrid(d, key, altura=altura, copiar_coluna=copiar_coluna)
    return _grid_dataframe(d, key, selection_mode)


def painel_kpis(d_exib: pd.DataFrame, d_sel: pd.DataFrame):
    # KPIs NÃO contabilizam linhas com Status Pgt = 'Cancelado'
    def _sem_cancel(x):
        if x is None or x.empty:
            return x
        return x[x["status_pgt"].astype(str).str.strip().str.lower() != "cancelado"]
    d_exib = _sem_cancel(d_exib)
    d_sel = _sem_cancel(d_sel)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Σ Exibidos", brl(dados.total_geral(d_exib)), f"{len(d_exib)} SPs")
    c2.metric("Σ Selecionados", brl(dados.total_geral(d_sel)), f"{len(d_sel)} SPs")
    ag = dados.contagem_agendamento(d_exib)
    c3.metric("Agendado / Pago", f"{ag['Agendado']} / {ag['Pago']}")
    c4.metric("Agendar / Falha", f"{ag['Agendar']} / {ag['Falha Agendar']}")

    e1, e2 = st.columns(2)
    with e1:
        st.caption("**Σ por Conta — Exibidos**")
        sc = dados.soma_por_conta(d_exib)
        if sc:
            st.dataframe(pd.DataFrame(
                [{"Conta Corrente": k, "Σ Valor": brl(v)} for k, v in sc.items()]),
                hide_index=True, **_FULLW)
    with e2:
        st.caption("**Σ por Conta — Selecionados**")
        ss = dados.soma_por_conta(d_sel)
        if ss:
            st.dataframe(pd.DataFrame(
                [{"Conta Corrente": k, "Σ Valor": brl(v)} for k, v in ss.items()]),
                hide_index=True, **_FULLW)
        else:
            st.caption("_(nenhuma SP selecionada)_")

    st.caption("**Por Forma de Pagamento — Exibidos**")
    sf = dados.soma_por_forma(d_exib)
    if sf:
        cols = st.columns(max(len(sf), 1))
        for col, (forma, v) in zip(cols, sf.items()):
            col.metric(forma, brl(v["soma"]), f"{v['qtd']} SPs")


# ----------------------------------------------------------------------------
# Sidebar — filtros (espelha a 'Pesquisa' da planilha)
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# Auto-atualização opcional — roda em SEGUNDO PLANO (fragmento), sem esmaecer a tela
# ----------------------------------------------------------------------------
try:
    from streamlit_autorefresh import st_autorefresh
    _TEM_AUTOREFRESH = True
except ImportError:
    _TEM_AUTOREFRESH = False

# esconde o indicador "Running..." do canto superior
st.markdown(
    "<style>"
    "[data-testid='stStatusWidget']{display:none !important;}"
    # Não esmaecer a tela enquanto atualiza: o conteúdo da execução anterior fica
    # 100% visível até o novo render chegar (troca suave, sem o 'cinza' de recarregar).
    "[data-stale='true']{opacity:1 !important;}"
    ".element-container{opacity:1 !important;}"
    "[data-testid='stAppViewContainer'], .main{opacity:1 !important;transition:none !important;}"
    "</style>",
    unsafe_allow_html=True)

import time as _time

_auto = st.sidebar.checkbox("🔄 Auto-atualizar (90s)", value=True,
                            help="Busca atualizações em segundo plano, a cada 90s.")

# Marca a abertura. O 1º sync de rede pelo fragmento só ocorre ~90s depois
# (a abertura já é coberta por _sync_ao_iniciar). Sem isso, cada clique/filtro
# disparava uma ida à rede -> era a causa da lentidão.
if "_auto_ultimo_ts" not in st.session_state:
    st.session_state["_auto_ultimo_ts"] = _time.time()
if "_sessao_inicio_ts" not in st.session_state:
    st.session_state["_sessao_inicio_ts"] = _time.time()


def _bg_sync_corpo():
    """Sincroniza em background. Vai à REDE no máximo a cada ~90s; cliques e
    filtros dentro desse intervalo NÃO tocam a rede (mantém a UI rápida).

    Novidades são aplicadas EM SILÊNCIO (sem banner e sem refresh forçado): o
    cache é atualizado e os dados novos entram no próximo clique natural do
    usuário. Única exceção: se houver SPs MARCADAS no grid (seleção em andamento),
    segura a aplicação — com um indicador discreto — para não desmarcar tudo; ao
    limpar a seleção (ou clicar em Aplicar), entra sozinho."""
    _SECOES_GRID = ("📝 Solicitações", "📌 Lote", "📊 Relatório")

    def _seguro_aplicar() -> bool:
        em_grid = st.session_state.get("_secao_atual") in _SECOES_GRID
        tem_sel = st.session_state.get("_sel_count", 0) > 0
        return not (em_grid and tem_sel)

    def _tem_novidade_aplicar():
        if _time.time() - st.session_state.get("_sessao_inicio_ts", 0) < 20:
            recarregar()                     # início da sessão: aplica e mostra já
            st.rerun()
        if _seguro_aplicar():
            recarregar()                     # silencioso: entra no próximo clique
            st.session_state["_novidades_pend"] = False
        else:
            st.session_state["_novidades_pend"] = True

    # 1) Reflete o sync INICIAL feito em background.
    try:
        if cache.get_meta("_ui_recarregar", "") == "1":
            cache.set_meta("_ui_recarregar", "")
            _tem_novidade_aplicar()
    except Exception:
        pass

    # 2) Pendência antiga: aplica sozinha assim que a seleção for limpa.
    if st.session_state.get("_novidades_pend"):
        if _seguro_aplicar():
            recarregar()
            st.session_state["_novidades_pend"] = False
        else:
            cnv = st.columns([6, 1])
            cnv[0].caption("🔔 Atualizações da planilha pendentes — entram sozinhas "
                           "quando você limpar a seleção.")
            if cnv[1].button("Aplicar", key="_aplicar_novidades", use_container_width=True):
                st.session_state["_novidades_pend"] = False
                recarregar()
                st.rerun()

    agora = _time.time()
    if agora - st.session_state.get("_auto_ultimo_ts", 0) < 85:
        return
    st.session_state["_auto_ultimo_ts"] = agora
    try:
        _drenar_fila()                       # reenvia alterações em fila (ex.: voltou a internet)
        _m = gsheets.sync_delta()
        if _m.get("diferentes", _m.get("mudadas")) or _m.get("removidas"):
            _tem_novidade_aplicar()
    except Exception:
        pass


# Dispara a sincronização inicial EM BACKGROUND antes de montar o fragmento (assim o
# fragmento não faz um 2º sync bloqueando a abertura). A tela abre com o cache local.
_sync_ao_iniciar()

if _auto and not gsheets.disponivel():
    st.sidebar.caption("Sem credenciais.json — auto-atualização indisponível offline.")
elif _auto:
    # 1ª opção: fragmento (reexecuta só ele, não a página) -> sem esmaecer
    _frag = None
    if hasattr(st, "fragment"):
        _frag = st.fragment(run_every="90s")
    elif hasattr(st, "experimental_fragment"):
        _frag = st.experimental_fragment(run_every="90s")
    if _frag is not None:
        _bg_sync = _frag(_bg_sync_corpo)
        _bg_sync()
    elif _TEM_AUTOREFRESH:
        # fallback p/ versões sem fragmento (pode piscar um pouco)
        _cnt = st_autorefresh(interval=90_000, key="_auto")
        if _cnt != st.session_state.get("_auto_last", 0):
            st.session_state["_auto_last"] = _cnt
            _bg_sync_corpo()
    else:
        st.sidebar.caption("⚠️ Atualize o Streamlit (fragmentos) para o modo background.")

df = df_cache()


def opcoes(col):
    if col not in df.columns:
        return []
    vals = sorted([v for v in df[col].dropna().unique() if str(v).strip()])
    return vals


_CC_OPTS = sorted({cc.strip() for v in df["centro_custo"]
                   for cc in str(v).split(",") if cc.strip()})
_AGEND_OPTS = ["Agendar", "Agendado", "Falha Agendar", "Verificar", "Sem Agendamento"]
_ORDEM_OPTS = ["Vencimento A-Z", "Vencimento Z-A", "Valor crescente",
               "Valor decrescente", "Credor", "ID"]
_SITUACAO_OPTS = ["Pendências", "Risco de Duplicidade", "Cadastro incompleto",
                  "Boleto Inválido", "Boleto Duplicado"]


def _restaurar_filtros():
    """Restaura (uma vez por sessão) o último filtro usado, salvo no cache."""
    if st.session_state.get("_filtros_carregados"):
        return
    try:
        salvos = json.loads(cache.get_meta("ultimo_filtro", "") or "{}")
    except Exception:
        salvos = {}
    multis = {
        "f_status_pgt": opcoes("status_pgt"), "f_conta": opcoes("conta_fmt"),
        "f_status_agend": _AGEND_OPTS, "f_centro": _CC_OPTS,
        "f_tipo": opcoes("tipo_despesa"), "f_projeto": opcoes("projeto"),
        "f_resp": opcoes("responsavel"), "f_forma": opcoes("forma_pagamento"),
        "f_situacoes": _SITUACAO_OPTS,
    }
    for k, opts in multis.items():
        if isinstance(salvos.get(k), list):
            st.session_state[k] = [x for x in salvos[k] if x in opts]  # sanitiza
    if isinstance(salvos.get("f_busca"), str):
        st.session_state["f_busca"] = salvos["f_busca"]
    if salvos.get("f_ordenar") in _ORDEM_OPTS:
        st.session_state["f_ordenar"] = salvos["f_ordenar"]
    st.session_state["_filtros_carregados"] = True


_restaurar_filtros()

with st.sidebar:
    st.markdown("### 🔎 Pesquisa")
    busca = st.text_input("Busca livre (ID, credor, CPF/CNPJ, descrição, NF…)",
                          key="f_busca",
                          help="Vários termos separados por vírgula (E lógico).")
    situacoes = st.multiselect(
        "Situação", _SITUACAO_OPTS, key="f_situacoes",
        help=(
            "Filtra por situações específicas (cumulativo: marcar mais de uma "
            "mostra quem atende a todas):\n\n"
            "• **Pendências** — solicitações que ainda estão a pagar (pagamento "
            "não realizado).\n\n"
            "• **Risco de Duplicidade** — a análise automática apontou que o "
            "pagamento pode ser duplicado de outro.\n\n"
            "• **Cadastro incompleto** — falta alguma informação essencial para "
            "pagar ou lançar (ex.: dados de pagamento, centro de custo ou "
            "integração contábil).\n\n"
            "• **Boleto Inválido** — boletos **a pagar** cujo código de barras "
            "está vazio, zerado ou marcado como inválido.\n\n"
            "• **Boleto Duplicado** — boletos **a pagar** cujo código de barras se "
            "repete em outra solicitação (inclusive quando a outra já foi paga — "
            "ou seja, risco de pagar de novo)."))
    status_pgt = st.multiselect("Status Pgt", opcoes("status_pgt"), key="f_status_pgt")
    conta = st.multiselect("Conta Corrente", opcoes("conta_fmt"), key="f_conta")
    status_agend = st.multiselect("Status Agendamento", _AGEND_OPTS, key="f_status_agend")
    projeto = st.multiselect("Projeto", opcoes("projeto"), key="f_projeto")
    centro_custo = st.multiselect("Centro de Custo", _CC_OPTS, key="f_centro")
    tipo_despesa = st.multiselect("Tipo de Despesa", opcoes("tipo_despesa"), key="f_tipo")
    responsavel = st.multiselect("Responsável pela Despesa", opcoes("responsavel"), key="f_resp")
    forma = st.multiselect("Forma de Pagamento", opcoes("forma_pagamento"), key="f_forma")

    st.markdown("**Período (vencimento)**")
    cpa, cpb = st.columns(2)
    periodo_ini = cpa.date_input("Inicial", value=None, format="DD/MM/YYYY")
    periodo_fim = cpb.date_input("Final", value=None, format="DD/MM/YYYY")

    st.markdown("**Data do Pagamento**")
    cga, cgb = st.columns(2)
    pgt_ini = cga.date_input("Inicial", value=None, format="DD/MM/YYYY", key="f_pgt_ini")
    pgt_fim = cgb.date_input("Final", value=None, format="DD/MM/YYYY", key="f_pgt_fim")

    st.markdown("**Valor**")
    cva, cvb = st.columns(2)
    valor_ini = cva.number_input("Mínimo", value=None, step=100.0, format="%.2f")
    valor_fim = cvb.number_input("Máximo", value=None, step=100.0, format="%.2f")

    ordenar = st.selectbox("Ordenar por", _ORDEM_OPTS, key="f_ordenar")

    def _limpar_filtros():
        for k in ["f_status_pgt", "f_conta", "f_status_agend", "f_projeto",
                  "f_centro", "f_tipo", "f_resp", "f_forma", "f_situacoes",
                  "f_pgt_ini", "f_pgt_fim"]:
            st.session_state[k] = []
        st.session_state["f_busca"] = ""
        st.session_state["f_ordenar"] = _ORDEM_OPTS[0]
        try:
            cache.set_meta("ultimo_filtro", "{}")
        except Exception:
            pass

    st.button("🧹 Limpar filtros", on_click=_limpar_filtros)

# salva o filtro atual (persiste entre sessões)
try:
    cache.set_meta("ultimo_filtro", json.dumps({
        "f_busca": busca, "f_status_pgt": status_pgt, "f_conta": conta,
        "f_status_agend": status_agend, "f_projeto": projeto, "f_centro": centro_custo,
        "f_tipo": tipo_despesa, "f_resp": responsavel, "f_forma": forma,
        "f_situacoes": situacoes, "f_ordenar": ordenar}))
except Exception:
    pass

filtros = dict(busca=busca, status_pgt=status_pgt, conta=conta, status_agend=status_agend,
               centro_custo=centro_custo, tipo_despesa=tipo_despesa, projeto=projeto,
               responsavel=responsavel, forma=forma, situacoes=situacoes,
               periodo_ini=periodo_ini, periodo_fim=periodo_fim,
               pgt_ini=pgt_ini, pgt_fim=pgt_fim,
               valor_ini=valor_ini, valor_fim=valor_fim, ordenar=ordenar)


# ----------------------------------------------------------------------------
# Cabeçalho
# ----------------------------------------------------------------------------
hc1, hc2 = st.columns([3, 1])
hc1.title("Análise de SPs")
ultimo_sync = cache.get_meta("ultimo_sync", "—")
_fila_n = cache.fila_contar()
_fila_txt = f"  ·  📤 {_fila_n} em fila" if _fila_n else ""
hc2.caption(f"Cache: {cache.contar()} SPs  ·  último sync: {_data_hora_br(ultimo_sync)}{_fila_txt}")

_flash = st.session_state.pop("_flash", None)
if _flash:
    (st.success if _flash[0] == "success" else st.warning)(_flash[1])

# ---------------------------------------------------------------------------
# NAVEGAÇÃO — seletor de seção fixo no topo (substitui st.tabs).
# Guardado em session_state -> sobrevive a st.rerun() (não "volta pra primeira
# aba" depois de uma ação em lote/validar) e renderiza SÓ a seção ativa, o que
# também deixa o app mais leve (antes os corpos das 9 abas rodavam todo run).
# ---------------------------------------------------------------------------
SECOES = ["📝 Solicitações", "📌 Lote", "📊 Relatório", "🔍 Auditoria",
          "🧮 Ratear", "🏦 Bradesco", "📅 Agenda", "🔄 Sincronização",
          "⚙️ Configurações", "🧾 Log"]

st.markdown("""
<style>
[data-testid="stHeader"]{height:0 !important;}
/* ============ BARRAS STICKY (no FLUXO, não mais position:fixed) ============
   fixed quebrava quando a sidebar recolhia (a animação cria um containing block
   e as barras 'descolam'). sticky fica no fluxo: recolher a sidebar vira um
   reflow natural, sem contas de left nem regras de sidebar. */
.block-container{padding-top:1.0rem !important;}
/* STICKY só funciona se NENHUM ancestral (até o elemento que rola) tiver
   overflow diferente de visible. Libera a cadeia completa — sem tocar no
   scroller (section stMain), que precisa continuar rolando. */
[data-testid="stMainBlockContainer"],
.block-container,
.block-container [data-testid="stVerticalBlock"],
.block-container [data-testid="stVerticalBlockBorderWrapper"],
.block-container [data-testid="stElementContainer"],
.block-container [data-testid="element-container"]{
  overflow:visible !important;
}
.st-key-nav_secao{
  position:sticky; top:0; z-index:1000;
  background:var(--background-color,#ffffff);
  padding:.45rem 0 .15rem 0;
  margin-bottom:-1rem;                    /* anula o gap padrão p/ a barra de ações */
}
.st-key-acoes_fixas{
  position:sticky; top:2.7rem; z-index:999;
  background:var(--background-color,#ffffff);
  padding:.15rem 0 .45rem 0;
  box-shadow:0 4px 6px -2px rgba(0,0,0,.12);
}
/* Abas sem ações: o placeholder vazio some por completo (nada de faixa branca),
   e a sombra volta pra própria nav. */
.st-key-acoes_fixas:not(:has(.stButton)){display:none;}
.block-container:not(:has(.st-key-acoes_fixas .stButton)) .st-key-nav_secao{
  box-shadow:0 4px 6px -2px rgba(0,0,0,.12); margin-bottom:0;
}
/* Vão entre as linhas 2 e 3 = mesmo respiro da linha 1 p/ a 2 (.3rem no total). */
.st-key-acoes_fixas, .st-key-acoes_fixas [data-testid="stVerticalBlock"],
.st-key-acoes_fixas [data-testid="stVerticalBlockBorderWrapper"]{
  overflow:visible !important;
}
.st-key-acoes_fixas [data-testid="stVerticalBlock"]{gap:.3rem !important;}
.st-key-acoes_fixas [data-testid="stRadio"]{margin:0 !important;}
/* Linha 3: contador + somatório, um respiro à direita do Alterar Status */
.st-key-acoes_fixas .sel-info{
  font-size:.78rem; font-weight:600; color:rgba(49,51,63,.75);
  margin:.05rem 0 0 .9rem; white-space:nowrap;
}
[data-testid="stSidebarCollapsedControl"]{z-index:1002 !important;}
/* Colunas encolhem pro conteúdo e ficam lado a lado, SEM vão (gap 0). */
.st-key-acoes_fixas [data-testid="stHorizontalBlock"]{
  flex-wrap:nowrap; gap:0; align-items:center;
}
.st-key-acoes_fixas [data-testid="stColumn"],
.st-key-acoes_fixas [data-testid="column"]{
  width:auto !important; flex:0 0 auto !important; min-width:0 !important;
}
/* Rádios: junta as opções entre si e separa o grupo do botão vizinho. */
.st-key-acoes_fixas [data-testid="stColumn"]:has([role="radiogroup"]){
  margin-right:.7rem !important;
}
.st-key-acoes_fixas [role="radiogroup"]{gap:.1rem !important; flex-wrap:nowrap;}
.st-key-acoes_fixas [role="radiogroup"] label{
  white-space:nowrap; padding-right:.45rem !important; margin-right:0 !important;
  margin-bottom:0 !important;
}
.st-key-acoes_fixas [role="radiogroup"] label p{font-size:.8rem;}
/* Botões COLADOS estilo segmented (como a barra de abas): TODOS com a MESMA
   altura fixa e centro alinhado. Cantos arredondados nas PONTAS de cada linha. */
.st-key-acoes_fixas .stButton{margin:0;}
.st-key-acoes_fixas .stButton button{
  height:1.95rem !important; min-height:1.95rem !important;
  padding:0 .65rem !important; font-size:.8rem;
  display:inline-flex; align-items:center; justify-content:center;
  border:1px solid rgba(49,51,63,.25); border-radius:0; white-space:nowrap;
}
.st-key-acoes_fixas .stButton button p{font-size:.8rem; line-height:1; margin:0;}
.st-key-acoes_fixas [data-testid="stHorizontalBlock"] > div:nth-child(n+2){
  margin-left:-1px;                      /* colapsa a borda dupla entre botões */
}
.st-key-acoes_fixas [data-testid="stHorizontalBlock"] > div:first-child button{
  border-radius:.45rem 0 0 .45rem;
}
.st-key-acoes_fixas [data-testid="stHorizontalBlock"] > div:last-child button{
  border-radius:0 .45rem .45rem 0;
}
/* Alterar Status (primary) fica com cantos próprios (não gruda no rádio/texto). */
.st-key-acoes_fixas button[kind="primary"],
.st-key-acoes_fixas [data-testid="stBaseButton-primary"]{
  border-radius:.45rem !important;
}
</style>
""", unsafe_allow_html=True)

try:
    _nav = st.container(key="nav_secao")
except TypeError:                       # versões muito antigas sem 'key' no container
    _nav = st.container()
with _nav:
    # Semeia o estado ANTES de criar o widget: se a chave sumiu ou ficou None
    # (desmarcado/rerun completo), volta pra ÚLTIMA seção ativa — nunca pra 1ª.
    if st.session_state.get("_secao") not in SECOES:
        st.session_state["_secao"] = st.session_state.get("_secao_atual", SECOES[0])
    if hasattr(st, "segmented_control"):
        _sel = st.segmented_control("Seção", SECOES, key="_secao",
                                    label_visibility="collapsed")
    else:
        _sel = st.radio("Seção", SECOES, key="_secao", horizontal=True,
                        label_visibility="collapsed")
# segmented_control permite desmarcar (retorna None) -> mantém a última seção ativa.
_secao = _sel or st.session_state.get("_secao_atual", SECOES[0])
st.session_state["_secao_atual"] = _secao

# Placeholder da barra de ações: criado AQUI (logo abaixo da nav, no fluxo) para
# ficar sticky colado nela; as abas com ações preenchem via _acoes_selecao e nas
# demais ele fica vazio (o CSS o esconde por completo).
try:
    _BARRA_ACOES = st.container(key="acoes_fixas")
except TypeError:
    _BARRA_ACOES = st.container()

# Abre o dialog "Gerar BeeVale" quando solicitado pelo detalhe (não pode abrir
# modal dentro de modal, então o detalhe agenda e abrimos aqui após fechar).
if st.session_state.pop("_bv_open", False):
    _dialog_gerar_beevale()
if st.session_state.pop("_val_open", False):
    _dialog_validar()

# ---- Lembretes da Agenda: avisa (toast) o que vence hoje / na janela de alerta,
# uma vez por dia por sessão (não repete a cada clique). ----
def _toasts_agenda():
    from datetime import date as _date
    hoje = _date.today()
    # Carrega a agenda da planilha em BACKGROUND (uma vez por sessão) — NÃO bloqueia
    # a abertura. Se ainda não veio, mostramos os lembretes no próximo ciclo.
    if not st.session_state.get("_agenda_boot"):
        st.session_state["_agenda_boot"] = True
        def _wk():
            try:
                agenda.bootstrap()
            except Exception:
                pass
        threading.Thread(target=_wk, daemon=True).start()

    marca = f"_agenda_avisado_{hoje.isoformat()}"
    if st.session_state.get(marca):
        return
    try:
        lista = agenda.carregar()            # só cache local (sem rede)
        lem = agenda.lembretes(lista, hoje)
    except Exception:
        lista, lem = [], []
    if not lista:
        return                                # agenda ainda carregando; tenta no próximo ciclo
    st.session_state[marca] = True
    for it in lem[:6]:
        c = it["compromisso"]
        quando = ("hoje" if it["dias"] == 0
                  else "amanhã" if it["dias"] == 1
                  else f"em {it['dias']} dias")
        st.toast(f"📅 {c.get('titulo', '(sem título)')} — {quando} "
                 f"({agenda.fmt_br(it['data'])})", icon="⏰")
    if len(lem) > 6:
        st.toast(f"📅 +{len(lem) - 6} compromisso(s) na janela de alerta. "
                 f"Veja a aba Agenda.", icon="⏰")

_toasts_agenda()


# ----------------------------------------------------------------------------
# ABA RELATÓRIO
# ----------------------------------------------------------------------------
if _secao == "📝 Solicitações":
    d_exib = aplicar_filtros(df, filtros)
    sel_ids = grid(d_exib, key="grid_rel", altura=650)   # +25% (era 520)
    d_sel = d_exib[d_exib["id"].isin(sel_ids)] if sel_ids else d_exib.iloc[0:0]
    n_sel = len(sel_ids)

    st.divider()
    _acoes_selecao(sel_ids, d_sel, df, "rel")

    st.divider()
    painel_kpis(d_exib, d_sel)


# ----------------------------------------------------------------------------
# ABA RELATÓRIO (dashboard analítico + exportação XLSX/PDF)
# ----------------------------------------------------------------------------
def _grafico_barras(df_agg: pd.DataFrame, dim_col: str, titulo: str, topn: int = 15):
    """Barras horizontais? Não: verticais com rótulo da categoria em ângulo e
    valor no formato contábil (no topo da barra e no tooltip)."""
    if df_agg.empty:
        st.caption(f"{titulo}: sem dados.")
        return
    d = df_agg.head(topn).copy()
    d["rotulo"] = d["Total"].map(_fmt_moeda)
    base = alt.Chart(d).encode(
        x=alt.X(f"{dim_col}:N", sort="-y",
                axis=alt.Axis(labelAngle=-40, title=None, labelLimit=160)),
        y=alt.Y("Total:Q", axis=alt.Axis(labels=False, ticks=False, title=None)),
        tooltip=[alt.Tooltip(f"{dim_col}:N", title=titulo),
                 alt.Tooltip("Qtd:Q", title="Qtd"),
                 alt.Tooltip("rotulo:N", title="Total")],
    )
    barras = base.mark_bar(color="#0a7d2c", cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
    texto = base.mark_text(dy=-6, fontSize=9, color="#333").encode(text="rotulo:N")
    st.caption(titulo)
    st.altair_chart((barras + texto).properties(height=300), use_container_width=True)


_DIM_QUEBRA = {
    "Conta Corrente": "conta_fmt", "Centro de Custo": "centro_custo", "Projeto": "projeto",
    "Tipo de Despesa": "tipo_despesa", "Responsável": "responsavel",
}


if _secao == "📊 Relatório":
    usar_filtros_rel = st.checkbox("Aplicar os filtros da barra lateral", value=True,
                                   key="rel_usar_filtros")
    base = aplicar_filtros(df, filtros) if usar_filtros_rel else df.copy()
    # Ignora cancelados nas análises
    base = base[base["status_pgt"].astype(str).str.strip().str.lower() != "cancelado"]

    c1, c2, c3 = st.columns([1.4, 1.4, 1.2])
    tipo_rel = c1.radio("Relatório", ["Visão geral", "Contas a Pagar", "Contas Pagas"],
                        horizontal=True, key="rel_tipo")
    periodo_rel = c2.radio("Período", ["Tudo", "Esta semana", "Este mês"],
                           horizontal=True, key="rel_periodo")
    topn = c3.slider("Top N (gráficos)", 5, 30, 15, key="rel_topn")

    pgt = base["status_pgt"].astype(str).str.strip().str.lower()
    if tipo_rel == "Contas a Pagar":
        base_r = base[pgt == "pagar"]
        col_data, nome_data = "vencimento_dt", "vencimento"
    elif tipo_rel == "Contas Pagas":
        base_r = base[pgt == "pago"]
        col_data, nome_data = "data_pagamento_dt", "pagamento"
    else:
        base_r = base
        col_data, nome_data = "vencimento_dt", "vencimento"

    # Período rápido (sobre a data relevante)
    hoje = pd.Timestamp(datetime.now().date())
    if periodo_rel == "Esta semana":
        ini = hoje - pd.Timedelta(days=hoje.weekday())
        fim = ini + pd.Timedelta(days=6)
    elif periodo_rel == "Este mês":
        ini = hoje.replace(day=1)
        fim = (ini + pd.offsets.MonthEnd(1)).normalize()
    else:
        ini = fim = None
    if ini is not None and col_data in base_r.columns:
        dd = base_r[col_data]
        base_r = base_r[dd.notna() & (dd >= ini) & (dd <= fim)]

    periodo_txt = (f"{ini.strftime('%d/%m/%Y')}–{fim.strftime('%d/%m/%Y')} (por {nome_data})"
                   if ini is not None else "todo o período")
    st.caption(f"**{tipo_rel}** · {periodo_txt} · {len(base_r)} lançamento(s) "
               "(cancelados ignorados; respeita os filtros da barra lateral).")

    k = relatorio.kpis(base_r, hoje=hoje)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total", _fmt_moeda(k["total"]))
    m2.metric("Lançamentos", k["qtd"])
    m3.metric("Ticket médio", _fmt_moeda(k["ticket"]))
    if tipo_rel == "Contas Pagas":
        m4.metric("—", "")
    else:
        m4.metric("Vencidos", f"{k['venc_qtd']}", _fmt_moeda(k["venc_total"]), delta_color="off")

    if base_r.empty:
        st.info("Sem lançamentos para os filtros/período atuais.")
    else:
        # ---- Gráficos ----
        st.markdown("#### Gráficos")
        g1, g2 = st.columns(2)
        with g1:
            _grafico_barras(relatorio.agregar(base_r, "projeto"), "projeto", "Por Projeto", topn)
            _grafico_barras(relatorio.agregar(base_r, "centro_custo"), "centro_custo",
                            "Por Centro de Custo", topn)
        with g2:
            _grafico_barras(relatorio.agregar(base_r, "tipo_despesa"), "tipo_despesa",
                            "Por Tipo de Despesa", topn)
            _grafico_barras(relatorio.agregar(base, "status_pgt"), "status_pgt",
                            "Por Status de Pagamento (sem cancelados)", 20)

        # ---- Tabela com as solicitações exibidas (igual à aba Solicitações) ----
        st.markdown("#### Solicitações exibidas")
        grid(base_r, key="grid_relat")

        # ---- Top credores (agrupado por CPF/CNPJ) ----
        st.markdown("#### Top credores (agrupados por CPF/CNPJ)")
        tc = relatorio.top_credores(base_r, n=topn)
        tc_show = tc.copy()
        tc_show["Total"] = tc_show["Total"].map(_fmt_moeda)
        st.dataframe(tc_show, hide_index=True, **_FULLW)

        # ---- Aging de vencidos ----
        if tipo_rel != "Contas Pagas":
            ag_v = relatorio.aging_vencidos(base_r, hoje=hoje)
            if not ag_v.empty:
                st.markdown("#### Aging de vencidos (a pagar)")
                agv = ag_v.copy()
                agv["Total"] = agv["Total"].map(_fmt_moeda)
                st.dataframe(agv, hide_index=True, **_FULLW)

        # ---- Quebra (breakdown) configurável (inclui Credor por CPF/CNPJ) ----
        st.markdown("#### Quebra detalhada")
        opcoes_quebra = list(_DIM_QUEBRA.keys()) + ["Credor (CPF/CNPJ)"]
        dim_label = st.selectbox("Quebrar por", opcoes_quebra, key="rel_quebra")
        if dim_label == "Credor (CPF/CNPJ)":
            quebra = relatorio.top_credores(base_r, n=10**9)
        else:
            quebra = relatorio.agregar(base_r, _DIM_QUEBRA[dim_label])
            quebra = quebra.rename(columns={_DIM_QUEBRA[dim_label]: dim_label})
        quebra_show = quebra.copy()
        quebra_show["Total"] = quebra_show["Total"].map(_fmt_moeda)
        st.dataframe(quebra_show, hide_index=True, **_FULLW)

        # ---- Exportação (analítica completa, igual nos dois formatos) ----
        st.markdown("#### Exportar")
        resumo = {"Relatório": tipo_rel, "Período": periodo_txt,
                  "Total": _fmt_moeda(k["total"]), "Lançamentos": k["qtd"],
                  "Ticket médio": _fmt_moeda(k["ticket"]),
                  "Vencidos (qtd)": k["venc_qtd"], "Vencidos (R$)": _fmt_moeda(k["venc_total"])}
        cols_dados = ["id", "solicitacao", "vencimento", "credor", "documento",
                      "projeto", "centro_custo", "tipo_despesa", "conta_fmt",
                      "status_pgt", "responsavel", "valor_num"]
        dados_exp = base_r[[c for c in cols_dados if c in base_r.columns]].copy()

        q_projeto = relatorio.agregar(base_r, "projeto").rename(columns={"projeto": "Projeto"})
        q_cc = relatorio.agregar(base_r, "centro_custo").rename(
            columns={"centro_custo": "Centro de Custo"})
        q_tipo = relatorio.agregar(base_r, "tipo_despesa").rename(
            columns={"tipo_despesa": "Tipo de Despesa"})
        q_conta = relatorio.agregar(base_r, "conta_fmt").rename(
            columns={"conta_fmt": "Conta Corrente"})
        q_resp = relatorio.agregar(base_r, "responsavel").rename(
            columns={"responsavel": "Responsável"})
        q_credor = relatorio.top_credores(base_r, n=10**9)

        abas_xlsx = {
            "PorProjeto": q_projeto, "PorCentroCusto": q_cc, "PorTipoDespesa": q_tipo,
            "PorConta": q_conta, "PorResponsavel": q_resp, "PorCredor": q_credor,
            "TopCredores": tc,
        }
        ex1, ex2 = st.columns(2)
        try:
            xlsx_bytes = relatorio.gerar_xlsx(f"Relatório — {tipo_rel}", resumo,
                                              abas_xlsx, dados_exp)
            ex1.download_button("⬇️ Baixar XLSX", xlsx_bytes,
                                file_name=f"relatorio_{tipo_rel.lower().replace(' ', '_')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument."
                                     "spreadsheetml.sheet", use_container_width=True)
        except Exception as e:
            ex1.error(f"Falha ao gerar XLSX: {e}")

        def _tab(df_in, max_rows=40):
            cols = list(df_in.columns)
            linhas = []
            for _, r in df_in.head(max_rows).iterrows():
                linhas.append([_fmt_moeda(r[c]) if c == "Total" else str(r[c]) for c in cols])
            if len(df_in) > max_rows:
                linhas.append([f"… (+{len(df_in) - max_rows} linhas)"] + [""] * (len(cols) - 1))
            return cols, linhas

        try:
            kpi_linhas = [("Total", _fmt_moeda(k["total"])), ("Lançamentos", str(k["qtd"])),
                          ("Ticket médio", _fmt_moeda(k["ticket"])),
                          ("Vencidos", f"{k['venc_qtd']} · {_fmt_moeda(k['venc_total'])}")]
            tabelas_pdf = {
                "Por Projeto": _tab(q_projeto), "Por Centro de Custo": _tab(q_cc),
                "Por Tipo de Despesa": _tab(q_tipo), "Por Conta": _tab(q_conta),
                "Por Responsável": _tab(q_resp),
                "Por Credor (CPF/CNPJ)": _tab(q_credor),
            }
            _fl = relatorio.fluxo_despesas(base_r)
            if _fl and not _fl["tabela"].empty:
                tabelas_pdf[f"Fluxo de Despesas — {_fl['nivel']} (por vencimento)"] = (
                    _tab(_fl["tabela"], max_rows=62))
            pdf_bytes = relatorio.gerar_pdf(
                f"Relatório — {tipo_rel}",
                f"Período: {periodo_txt} · gerado em {hoje.strftime('%d/%m/%Y')}",
                kpi_linhas, tabelas_pdf,
                analitico=relatorio.analitico_despesas(base_r))
            ex2.download_button("⬇️ Baixar PDF", pdf_bytes,
                                file_name=f"relatorio_{tipo_rel.lower().replace(' ', '_')}.pdf",
                                mime="application/pdf", use_container_width=True)
        except Exception as e:
            ex2.error(f"Falha ao gerar PDF: {e}")

    # ===== Painéis diários (reproduzem os quadros da planilha) =====
    st.divider()
    st.markdown("### 📅 Quantidade de pagamentos diários")
    st.caption("Por **data de pagamento** (col X) e **forma** (col J) — lançamentos já pagos.")
    pdia = relatorio.pagamentos_diarios(base)
    tdia = pdia["tabela"]
    if tdia.empty:
        st.info("Sem pagamentos com data preenchida nos filtros atuais.")
    else:
        gdia = tdia[["Data Pgt", "Qtd"]].copy()
        gdia["_o"] = pd.to_datetime(gdia["Data Pgt"], format="%d/%m/%Y")
        gdia = gdia.sort_values("_o")
        ch = (alt.Chart(gdia).mark_bar(color="#1f6f3f")
              .encode(x=alt.X("Data Pgt:N", sort=list(gdia["Data Pgt"]),
                              title=None, axis=alt.Axis(labelAngle=-45)),
                      y=alt.Y("Qtd:Q", title=None),
                      tooltip=["Data Pgt", "Qtd"]).properties(height=200))
        st.altair_chart(ch, use_container_width=True)
        disp = tdia.copy()
        for c in pdia["formas"]:
            disp[c] = disp[c].apply(lambda x: "" if x == 0 else int(x))
        st.dataframe(disp, hide_index=True, **_FULLW)
        cont, med = pdia["contagem"], pdia["media"]
        rod = pd.DataFrame([
            {"Data Pgt": "Contagem",
             **{c: int(cont.get(c, 0)) for c in pdia["formas"] + ["Qtd"]}},
            {"Data Pgt": "Média",
             **{c: int(med.get(c, 0)) for c in pdia["formas"] + ["Qtd"]}},
        ])
        st.dataframe(rod, hide_index=True, **_FULLW)

    st.divider()
    st.markdown("### 💰 Necessidade de caixa diário por conta (D-15 a D+20 · por vencimento)")
    st.caption("Valores **a pagar** (não Pago/Cancelado) por **vencimento** × **conta** (col U). "
               "Linhas em vermelho = já vencidas.")
    nc = relatorio.necessidade_caixa(base, hoje)
    tnc = nc["tabela"]
    if tnc.empty:
        st.info("Sem valores a pagar na janela D-15/D+20 nos filtros atuais.")
    else:
        numcols = nc["contas"] + ["Soma"]
        vencido = nc["vencido"]
        gnc = tnc[["Vencimento", "Soma"]].copy()
        gnc["_o"] = pd.to_datetime(gnc["Vencimento"], format="%d/%m/%Y")
        gnc["Situação"] = ["Vencido" if v else "A vencer" for v in vencido]
        ch2 = (alt.Chart(gnc.sort_values("_o")).mark_bar()
               .encode(x=alt.X("Vencimento:N", sort=list(gnc.sort_values("_o")["Vencimento"]),
                               title=None, axis=alt.Axis(labelAngle=-45)),
                       y=alt.Y("Soma:Q", title=None),
                       color=alt.Color("Situação:N",
                                       scale=alt.Scale(domain=["Vencido", "A vencer"],
                                                       range=["#c00000", "#1f6f3f"]),
                                       legend=alt.Legend(title=None, orient="top")),
                       tooltip=["Vencimento", "Situação", "Soma"]).properties(height=220))
        st.altair_chart(ch2, use_container_width=True)

        def _cel(v):
            try:
                f = float(v)
            except (TypeError, ValueError):
                return ""
            return "" if f == 0 else fmt_contabil(f)

        def _cor(row):
            cor = "color:#c00000;font-weight:600" if vencido[row.name] else ""
            return [cor] * len(row)

        sty = (tnc.style.format(_cel, subset=numcols)
               .apply(_cor, axis=1).hide(axis="index"))
        st.dataframe(sty, **_FULLW)
        rod2 = (nc["rodape"].apply(lambda col: col.map(_cel))
                .reset_index().rename(columns={"index": ""}))
        st.dataframe(rod2, hide_index=True, **_FULLW)


# ----------------------------------------------------------------------------
# ABA AUDITORIA (pontualidade, risco IA, códigos de barras, extras)
# ----------------------------------------------------------------------------
def _money_cols(d: pd.DataFrame, cols) -> pd.DataFrame:
    d = d.copy()
    for c in cols:
        if c in d.columns:
            d[c] = d[c].map(_fmt_moeda)
    return d


def _url_card(series) -> pd.Series:
    """Transforma uma série de IDs em URLs de card do Pipefy (vazio se não houver ID)."""
    s = series.astype(str).str.strip()
    return s.where(s == "", "https://app.pipefy.com/open-cards/" + s)


def _link_id(label: str = "ID"):
    """LinkColumn que mostra só o número do ID (extraído da URL) e abre o card."""
    return st.column_config.LinkColumn(label, display_text=r"open-cards/(\d+)")


if _secao == "🔍 Auditoria":
    usar_filtros = st.checkbox("Aplicar os filtros da barra lateral", value=False,
                               key="aud_filtros",
                               help="Desligado = audita toda a base (recomendado).")
    base_a = aplicar_filtros(df, filtros) if usar_filtros else df.copy()
    base_a = base_a[base_a["status_pgt"].astype(str).str.strip().str.lower() != "cancelado"]
    st.caption(f"Auditando **{len(base_a)}** lançamentos (cancelados ignorados).")

    tipo_aud = st.radio("Análise", ["Pontualidade do registro", "Risco de duplicidade (IA)",
                                    "Códigos de barras", "Outras checagens"],
                        horizontal=True, key="aud_tipo")

    # ---------- 5.1 Pontualidade ----------
    if tipo_aud == "Pontualidade do registro":
        st.markdown("#### Pontualidade do registro (antecedência = vencimento − solicitação)")
        st.caption("Antecedência **negativa** = registrado **depois** do vencimento "
                   "(gera juros). Objetivo: cobrar quem lança em cima/atrasado.")
        min_l = st.slider("Considerar apenas responsáveis com ao menos N lançamentos",
                          1, 50, 5, key="aud_minl")
        pont = auditoria.pontualidade(base_a, min_lanc=min_l)
        if pont.empty:
            st.info("Sem dados suficientes (precisa de data de solicitação e vencimento).")
        else:
            tot_atras = int(pont["Atrasados"].sum())
            rs_atras = float(pont["R$ Atrasado"].sum())
            k1, k2, k3 = st.columns(3)
            k1.metric("Responsáveis", len(pont))
            k2.metric("Lançamentos atrasados", tot_atras)
            k3.metric("R$ exposto (atrasados)", _fmt_moeda(rs_atras))

            d = pd.concat([pont.head(20), pont.tail(20)]).drop_duplicates(subset="Responsável")
            d = d.assign(cor=d["Média dias"].map(lambda x: "#d50000" if x < 0 else "#2e7d32"))
            ch = alt.Chart(d).mark_bar().encode(
                x=alt.X("Média dias:Q", title="Antecedência média (dias)"),
                y=alt.Y("Responsável:N", sort="x", title=None,
                        axis=alt.Axis(labelLimit=220)),
                color=alt.Color("cor:N", scale=None, legend=None),
                tooltip=["Responsável", "Qtd", "Média dias", "Atrasados", "% Atrasados"])
            st.altair_chart(ch.properties(height=min(640, 26 * len(d) + 40)),
                            use_container_width=True)

            cpior, cmelhor = st.columns(2)
            with cpior:
                st.markdown("**🔴 Piores (mais atrasam)**")
                st.dataframe(_money_cols(pont.head(10), ["R$ Atrasado", "R$ Total"]),
                             hide_index=True, **_FULLW)
            with cmelhor:
                st.markdown("**🟢 Melhores (mais antecedência)**")
                st.dataframe(_money_cols(pont.tail(10).iloc[::-1], ["R$ Atrasado", "R$ Total"]),
                             hide_index=True, **_FULLW)

            st.markdown("**Ranking completo**")
            st.dataframe(_money_cols(pont, ["R$ Atrasado", "R$ Total"]),
                         hide_index=True, **_FULLW)

    # ---------- 5.3 Risco IA ----------
    elif tipo_aud == "Risco de duplicidade (IA)":
        st.markdown("#### Risco de duplicidade apontado pela IA")
        rk = auditoria.risco_ia(base_a)
        if rk.empty:
            st.success("Nenhum lançamento marcado como 'COM RISCO'. 🎉")
        else:
            st.caption(f"{len(rk)} lançamento(s) COM RISCO. **Marque as linhas** que forem "
                       "falso positivo e clique no botão — a análise é reescrita, some daqui "
                       "e o ID deixa de ficar vermelho em Solicitações.")
            rk = rk.reset_index(drop=True)
            refs_list = [[x.strip() for x in str(s).split(",") if x.strip()]
                         for s in rk["ref_ids"].fillna("")]
            maxr = min(4, max((len(r) for r in refs_list), default=0))
            data = {
                "ID": _url_card(rk["id"]).values,
                "Credor": rk["credor"].values,
                "CPF/CNPJ": rk["documento"].values,
                "Valor": rk["valor_num"].map(_fmt_moeda).values,
            }
            cfg = {"ID": _link_id()}
            for j in range(maxr):
                label = "ID referenciado" if maxr == 1 else f"ID ref. {j + 1}"
                data[label] = ["https://app.pipefy.com/open-cards/" + r[j] if j < len(r) else ""
                               for r in refs_list]
                cfg[label] = _link_id(label)
            data["Análise da IA"] = rk["analise_ia"].values
            cfg["Análise da IA"] = st.column_config.TextColumn(width="large")
            disp = pd.DataFrame(data)
            ev = st.dataframe(
                disp, hide_index=True, **_FULLW, on_select="rerun",
                selection_mode="multi-row", key="aud_risco_df", column_config=cfg)
            linhas_sel = ev.selection.rows if (ev and ev.selection) else []
            sel_ids = [str(rk.iloc[i]["id"]) for i in linhas_sel]
            st.caption("Cada **ID ref.** é um link próprio (abre o card). Para marcar como "
                       "falso positivo, selecione as linhas e use o botão abaixo.")
            cR1, cR2 = st.columns([1, 1])
            if cR1.button(f"✅ Marcar SEM RISCO ({len(sel_ids)})", disabled=not sel_ids,
                          type="primary", key="aud_btn_semrisco"):
                txt = f"SEM RISCO (revisado em {datetime.now().strftime('%d/%m/%Y')})"
                aplicar_alteracao(sel_ids, "analise_ia", txt, acao="Auditoria: SEM RISCO")
                st.rerun()
            if cR2.button(f"🔗 Abrir cards referenciados ({len(sel_ids)})", disabled=not sel_ids,
                          key="aud_btn_abrir_ref"):
                refs = []
                for i in linhas_sel:
                    for rid in str(rk.iloc[i]["ref_ids"]).split(","):
                        rid = rid.strip()
                        if rid:
                            refs.append("https://app.pipefy.com/open-cards/" + rid)
                _abrir_cards(refs)

    # ---------- 5.2 Códigos de barras ----------
    elif tipo_aud == "Códigos de barras":
        st.markdown("#### Códigos de barras duplicados (coluna AI · só Boleto)")
        st.caption("Mesmo código em lançamentos diferentes = risco de pagar 2×. "
                   "Cada linha é um lançamento; clique no ID para abrir o card.")
        dup = auditoria.barras_duplicadas(base_a)
        if dup.empty:
            st.success("Nenhum código de barras duplicado em boletos. 🎉")
        else:
            show = pd.DataFrame({
                "Código de Barras": dup["Código de Barras"],
                "ID": _url_card(dup["id"]), "Credor": dup["credor"],
                "Valor": dup["valor_num"].map(_fmt_moeda), "Qtd grupo": dup["Qtd grupo"]})
            st.dataframe(show, hide_index=True, **_FULLW, column_config={"ID": _link_id()})

        inval = auditoria.codigos_invalidos(base_a)
        st.markdown(f"#### Códigos inválidos ({len(inval)})")
        if inval.empty:
            st.success("Nenhum boleto com código INVALIDO.")
        else:
            iv = pd.DataFrame({
                "ID": _url_card(inval["id"]), "Credor": inval["credor"],
                "Valor": inval["valor_num"].map(_fmt_moeda),
                "Forma": inval["forma_pagamento"], "Código": inval["codigo_barras"]})
            st.dataframe(iv, hide_index=True, **_FULLW, column_config={"ID": _link_id()})

    # ---------- Extras ----------
    else:
        st.markdown("#### Possível duplicidade (mesmo CPF/CNPJ + valor, até 7 dias)")
        st.caption("Checagem determinística, complementar à IA. Clique no ID para abrir.")
        pdup = auditoria.possivel_duplicidade(base_a, dias=7)
        if pdup.empty:
            st.success("Nada encontrado.")
        else:
            show = pd.DataFrame({
                "CPF/CNPJ": pdup["CPF/CNPJ"], "Valor": pdup["Valor"].map(_fmt_moeda),
                "ID": _url_card(pdup["id"]), "Credor": pdup["credor"],
                "Janela (dias)": pdup["Janela (dias)"], "Qtd grupo": pdup["Qtd grupo"]})
            st.dataframe(show, hide_index=True, **_FULLW, column_config={"ID": _link_id()})

        st.markdown("#### Nº de NF repetido (mesmo CPF/CNPJ)")
        nfd = auditoria.nf_duplicada(base_a)
        if nfd.empty:
            st.success("Nenhuma NF repetida.")
        else:
            show = pd.DataFrame({
                "CPF/CNPJ": nfd["CPF/CNPJ"], "Nº NF": nfd["Nº NF"],
                "ID": _url_card(nfd["id"]), "Credor": nfd["credor"],
                "Valor": nfd["valor_num"].map(_fmt_moeda), "Qtd grupo": nfd["Qtd grupo"]})
            st.dataframe(show, hide_index=True, **_FULLW, column_config={"ID": _link_id()})

        st.markdown("#### Cadastro incompleto (sem Centro de Custo e/ou Projeto)")
        semc = auditoria.sem_classificacao(base_a)
        if semc.empty:
            st.success("Todos com Centro de Custo e Projeto.")
        else:
            show = pd.DataFrame({
                "ID": _url_card(semc["id"]), "Credor": semc["credor"],
                "Valor": semc["valor_num"].map(_fmt_moeda),
                "Centro de Custo": semc["centro_custo"], "Projeto": semc["projeto"],
                "Faltando": semc["Faltando"]})
            st.dataframe(show, hide_index=True, **_FULLW, column_config={"ID": _link_id()})

        st.markdown("#### Integração Omie pendente (sem código na col P)")
        st.caption("Títulos ativos (não Cancelado e não Pago) sem código de integração "
                   "Omie — precisam ser integrados. Estes IDs ficam laranja em Solicitações.")
        omie = auditoria.sem_integracao_omie(base_a)
        if omie.empty:
            st.success("Todos os títulos ativos estão integrados ao Omie.")
        else:
            show = pd.DataFrame({
                "ID": _url_card(omie["id"]), "Credor": omie["credor"],
                "Valor": omie["valor_num"].map(_fmt_moeda),
                "Status Pgt": omie["status_pgt"]})
            st.dataframe(show, hide_index=True, **_FULLW, column_config={"ID": _link_id()})
if _secao == "📌 Lote":
    st.markdown("Cole os **IDs** (um por linha, ou separados por vírgula/espaço). "
                "Linhas de **texto** (ex.: _Pagar amanhã_) viram **cabeçalhos de seção**: "
                "os IDs abaixo de cada texto aparecem agrupados sob ele. Ao lado, a "
                "**Extração de IDs** aceita mensagens do WhatsApp e cria um Novo Lote "
                "com as SPs encontradas.")

    # Restaura os IDs salvos (sobrevive a fechar/reabrir o app). Prioridade:
    # 1) transferência pendente do 'Enviar Lote' (chave própria, que NÃO é apagada
    #    pela limpeza de widgets do Streamlit); 2) estado da sessão; 3) meta salvo.
    _pend = st.session_state.pop("_lote_ids_novo", None)
    if _pend is not None:
        st.session_state["lote_ids"] = _pend
    elif ("lote_ids" not in st.session_state
          or not str(st.session_state.get("lote_ids", "")).strip()):
        try:
            _meta_ids = cache.get_meta("lote_ids_txt", "") or ""
        except Exception:
            _meta_ids = ""
        # só recarrega do meta se a sessão está vazia E o meta tem conteúdo —
        # evita um estado vazio 'sombrear' os IDs persistidos.
        if _meta_ids or "lote_ids" not in st.session_state:
            st.session_state["lote_ids"] = _meta_ids

    def _salvar_lote_ids():
        try:
            cache.set_meta("lote_ids_txt", st.session_state.get("lote_ids", ""))
        except Exception:
            pass

    def _extrair_ids_lote():
        """on_click do 'Extrair IDs': roda ANTES do rerun (pode mexer no estado dos
        campos). Extrai as SPs das mensagens coladas e cria um grupo 'Novo Lote N'
        no topo do campo IDs, preservando o que já estava."""
        bruto = st.session_state.get("lote_extracao_txt", "")
        ids_ext = extrair_ids_texto(bruto)
        if not ids_ext:
            st.session_state["_flash"] = ("warning",
                                          "Nenhum ID de SP (10 dígitos) encontrado no texto colado.")
            return
        atual = (st.session_state.get("lote_ids", "")
                 or cache.get_meta("lote_ids_txt", "") or "").strip("\n")
        _n = len(re.findall(r"(?m)^\s*Novo Lote\b", atual)) + 1
        novo = (f"Novo Lote {_n}\n" + "\n".join(ids_ext)
                + (("\n\n" + atual) if atual.strip() else ""))
        st.session_state["lote_ids"] = novo
        st.session_state["lote_extracao_txt"] = ""      # limpa a caixa de extração
        try:
            cache.set_meta("lote_ids_txt", novo)
        except Exception:
            pass
        st.session_state["_flash"] = ("success",
                                      f"🔎 {len(ids_ext)} SP(s) extraída(s) → grupo 'Novo Lote {_n}'.")

    cids, cext = st.columns(2)
    with cids:
        txt = st.text_area("IDs", height=120, key="lote_ids", on_change=_salvar_lote_ids,
                           placeholder="1384831053\n1384844943\n1384852359")
    with cext:
        st.text_area("Extração de IDs (cole as mensagens do WhatsApp)", height=120,
                     key="lote_extracao_txt",
                     placeholder="✅💵💵✅\nSolicitação de Pagamento Validada\n"
                                 "Nº da SP: 1426036778\n…")

    def _limpar_lote_ids():
        st.session_state["lote_ids"] = ""
        try:
            cache.set_meta("lote_ids_txt", "")
        except Exception:
            pass

    def _remover_por_status(status_alvos, rotulo):
        alvos = {s.lower() for s in status_alvos}
        smap = (df.set_index("id")["status_pgt"].astype(str)
                .str.strip().str.lower().to_dict())
        novas, removidos = [], 0
        for bruta in st.session_state.get("lote_ids", "").split("\n"):
            linha = bruta.strip()
            if not linha:
                continue
            toks = [t for t in re.split(r"[\s,;]+", linha) if t]
            if toks and all(re.fullmatch(r"\d+", t) for t in toks):
                mantidos = [t for t in toks if smap.get(t, "") not in alvos]
                removidos += len(toks) - len(mantidos)
                if mantidos:
                    novas.append(" ".join(mantidos))
            else:
                novas.append(linha)            # cabeçalho de seção
        st.session_state["lote_ids"] = "\n".join(novas).strip("\n")
        try:
            cache.set_meta("lote_ids_txt", st.session_state["lote_ids"])
        except Exception:
            pass
        st.toast(f"{removidos} ID(s) {rotulo} removido(s) do lote.")

    def _titulo_total(titulo, d_tab):
        """Linha com o título à esquerda e o somatório de Valor à direita (alinhados)."""
        total = pd.to_numeric(d_tab.get("valor_num", pd.Series(dtype=float)),
                              errors="coerce").sum()
        if titulo:
            st.markdown(f"#### {titulo}")
        st.markdown(
            "<div style='text-align:center;font-weight:700;padding:4px 0'>"
            f"Total: {_fmt_moeda(total)}</div>", unsafe_allow_html=True)

    # Botões do Lote agora vivem na BARRA FIXA (extras da _acoes_selecao).
    def _abrir_relatorio_lote():
        st.session_state["_lote_rel_ids"] = list(st.session_state.get("_sel_ids_atual", []))
        st.session_state.pop("_lote_rel_pdf", None)
        st.session_state.pop("_lote_rel_xlsx", None)
        _dialog_relatorio_lote()

    def _atualizar_tabelas_lote():
        try:
            with st.spinner("Atualizando…"):
                gsheets.sync_delta()
            recarregar()
            st.rerun()
        except Exception:
            st.warning("Não consegui atualizar agora (conexão oscilou). "
                       "Exibindo o que já está em cache.")

    _extras_lote = [
        {"label": "✅ Processar IDs", "key": "lote_processar", "type": "primary",
         "on_click": _salvar_lote_ids},
        {"label": "🔎 Extrair IDs", "key": "lote_extrair",
         "on_click": _extrair_ids_lote,
         "help": "Encontra os números de SP (10 dígitos) nas mensagens coladas no "
                 "campo 'Extração de IDs', remove duplicados e cria um grupo "
                 "'Novo Lote' no campo IDs."},
        {"label": "🧹 Limpar IDs", "key": "lote_limpar", "on_click": _limpar_lote_ids},
        {"label": "✔️ Remover Pagos", "key": "lote_rm_pagos",
         "on_click": _remover_por_status, "args": (["pago"], "pago(s)")},
        {"label": "🚫 Remover Cancelados", "key": "lote_rm_canc",
         "on_click": _remover_por_status, "args": (["cancelado"], "cancelado(s)")},
        {"label": "🔄 Atualizar tabelas", "key": "lote_atualizar",
         "fn": _atualizar_tabelas_lote},
        {"label": "📄 Relatório do Lote", "key": "lote_relatorio",
         "fn": _abrir_relatorio_lote,
         "help": "Gera PDF das SPs selecionadas com visão por Grupo do Lote "
                 "(cabeçalhos das tabelas), Centro de Custo, Tipo e Credor."},
    ]

    sel_total = []   # seleção acumulada de TODAS as tabelas (painel + lote colado)

    # ---- Lote colado: as tabelas a partir dos IDs colados (agora EM CIMA) ----
    st.divider()
    st.markdown("### 📋 Lote colado")

    # Parse: linha cujos tokens são TODOS dígitos = IDs; senão = CABEÇALHO de seção
    # (a linha inteira vira o título, então títulos com espaço funcionam).
    secoes = []                       # [{"titulo": str|None, "ids": [..]}]
    atual = {"titulo": None, "ids": []}
    for bruta in txt.split("\n"):
        linha = bruta.strip()
        if not linha:
            continue
        tokens = [t for t in re.split(r"[\s,;]+", linha) if t]
        if tokens and all(re.fullmatch(r"\d+", t) for t in tokens):
            atual["ids"].extend(tokens)
        else:
            if atual["titulo"] is not None or atual["ids"]:
                secoes.append(atual)
            atual = {"titulo": linha, "ids": []}
    secoes.append(atual)
    secoes = [s for s in secoes if s["titulo"] or s["ids"]]

    todos_ids = [i for s in secoes for i in s["ids"]]
    # mapa id -> título do grupo (usado pelo Relatório do Lote)
    st.session_state["_lote_grupos_map"] = {
        str(i): (s["titulo"] or "(sem título)") for s in secoes for i in s["ids"]}
    base = df.set_index("id")
    d_lote = base.iloc[0:0].reset_index()

    if not todos_ids and not any(s["titulo"] for s in secoes):
        st.info("Cole ao menos um ID acima e clique em **Processar IDs**.")
    else:
        _encontrados = sum(1 for i in todos_ids if i in base.index)
        st.caption(f"✅ {_encontrados} de {len(todos_ids)} ID(s) reconhecido(s) · "
                   f"{len([s for s in secoes if s['ids']])} seção(ões) com dados.")
        _sel_grupo_map = {}
        _keys_vistas = {}
        for i, s in enumerate(secoes):
            achados_ids = [x for x in s["ids"] if x in base.index]
            d_sec = (base.reindex(achados_ids).reset_index() if achados_ids
                     else base.iloc[0:0].reset_index())
            _titulo_total(s["titulo"], d_sec)
            if not achados_ids:
                if s["ids"]:
                    st.caption("_(nenhum ID desta seção encontrado no cache)_")
                continue
            # CHAVE POR TÍTULO (não por posição): quando um grupo novo entra no
            # topo, os índices deslocam — com chave posicional, a SELEÇÃO de uma
            # tabela 'vestia' a seguinte (linhas marcadas sozinhas). Com a chave
            # amarrada ao título, a seleção fica presa à sua própria tabela.
            _base_key = re.sub(r"\W+", "_", str(s["titulo"] or "sem_titulo"))[:40]
            _keys_vistas[_base_key] = _keys_vistas.get(_base_key, 0) + 1
            _gkey = f"grid_lote_{_base_key}_{_keys_vistas[_base_key]}"
            # altura acompanha a quantidade de linhas (cabeçalho+barra ~110px,
            # ~32px/linha) — SEM teto: exibe sempre todos os registros da seção.
            alt = 110 + len(d_sec) * 32
            _sel_sec = grid(d_sec, key=_gkey, selection_mode="multi-row",
                            altura=alt, copiar_coluna=False)
            sel_total.extend(_sel_sec)
            for _sid in _sel_sec:
                _sel_grupo_map[str(_sid)] = s["titulo"] or "(sem título)"
        st.session_state["_lote_sel_grupo_map"] = _sel_grupo_map

        d_lote = base.reindex([i for i in todos_ids if i in base.index]).reset_index()
        faltando = [i for i in todos_ids if i not in base.index]
        if faltando:
            st.warning(f"IDs não encontrados no cache: {', '.join(faltando)}")

    # ---- 4 tabelas-painel por status de agendamento (agora EMBAIXO; atualizam a
    # cada acesso, pois leem o df já sincronizado). 'Verificar' = status_agend. ----
    st.divider()
    st.markdown("### 📊 Painel por status")
    _ag_series = df["status_agend"].astype(str).str.strip()
    for _label in ["Agendar", "Agendado", "Falha Agendar", "Verificar"]:
        d_st = df[_ag_series.str.casefold() == _label.casefold()].reset_index(drop=True)
        _titulo_total(_label, d_st)
        if len(d_st):
            _alt = 110 + len(d_st) * 32          # sem teto: exibe todos
            sel_total.extend(grid(d_st, key=f"grid_status_{_label}",
                                  selection_mode="multi-row", altura=_alt,
                                  copiar_coluna=False))
        else:
            st.caption("_(nenhum)_")

    # Ações na seleção — agem sobre TUDO que foi marcado (painel + lote colado).
    d_sel = df[df["id"].isin(sel_total)] if sel_total else df.iloc[0:0]
    st.divider()
    _acoes_selecao(sel_total, d_sel, df, "lote", extras=_extras_lote)

    if len(d_lote):
        st.divider()
        painel_kpis(d_lote, d_lote[d_lote["id"].isin(sel_total)])


# ----------------------------------------------------------------------------
# ABA RATEAR — gera os JSONs de rateio (centro de custo + categoria) para o Omie
# ----------------------------------------------------------------------------
if _secao == "🧮 Ratear":
    st.markdown("### 🧮 Ratear (gera JSON para atualizar título no Omie)")
    st.caption("Monte o rateio por **Centro de Custo** (obrigatório) e, se quiser, por "
               "**Categoria de Despesa**. Os percentuais fecham 100% com arredondamento "
               "de menor erro; os valores da categoria fecham a base informada.")

    ref = {}
    try:
        ref = json.loads(cache.get_meta("ref_rateio", "") or "{}")
    except Exception:
        ref = {}
    obras_ref = ref.get("obras", [])
    cats_ref = ref.get("categorias", [])
    mapa_obra = {o["obra"]: o["codigo"] for o in obras_ref}
    mapa_cat = {c["categoria"]: c["codigo"] for c in cats_ref}

    cR1, cR2 = st.columns([1.2, 3])
    if cR1.button("🔄 Atualizar listas (Omie)", key="ratear_refresh"):
        try:
            novo = gsheets.carregar_referencias_rateio()
            cache.set_meta("ref_rateio", json.dumps(novo, ensure_ascii=False))
            st.success(f"Listas atualizadas: {len(novo['obras'])} obras · "
                       f"{len(novo['categorias'])} categorias.")
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao atualizar (precisa de internet/credenciais): {e}")
    cR2.caption(f"Carregadas: **{len(obras_ref)}** obras · **{len(cats_ref)}** categorias. "
                "Se estiver vazio ou desatualizado, clique em Atualizar.")

    if not obras_ref:
        st.info("Clique em **Atualizar listas (Omie)** para carregar as obras (aba "
                "'C. Diários') e categorias (aba 'Plano Financeiro'). Precisa de internet.")
    else:
        colA, colB = st.columns(2)
        with colA:
            st.markdown("#### Rateio Centro de Custo")
            ed_cc = st.data_editor(
                pd.DataFrame({"Obra": ["", "", ""], "Valor": ["", "", ""]}),
                key="ratear_cc", num_rows="dynamic", hide_index=True, **_FULLW,
                column_config={
                    "Obra": st.column_config.SelectboxColumn("Obra", options=list(mapa_obra)),
                    "Valor": st.column_config.TextColumn(
                        "Valor (R$)", help="Padrão contábil BR — ex.: 1.234,56")})
        with colB:
            st.markdown("#### Rateio Categoria de Despesa")
            ed_cat = st.data_editor(
                pd.DataFrame({"Categoria": ["", "", ""], "Valor": ["", "", ""]}),
                key="ratear_cat", num_rows="dynamic", hide_index=True, **_FULLW,
                column_config={
                    "Categoria": st.column_config.SelectboxColumn(
                        "Categoria de Despesa", options=list(mapa_cat)),
                    "Valor": st.column_config.TextColumn(
                        "Valor (R$)", help="Padrão contábil BR — ex.: 1.234,56")})
            base_cat_txt = st.text_input(
                "Valor a ratear na categoria (opcional; senão usa a soma acima)",
                key="ratear_base_cat_txt", placeholder="ex.: 10.000,00")
            base_cat = rateio._to_float(base_cat_txt)
            if base_cat_txt.strip() and base_cat <= 0:
                st.warning(f"Não entendi o valor '{base_cat_txt}'. Use o padrão "
                           "contábil, ex.: 10.000,00.")

        if st.button("⚙️ Gerar JSONs", type="primary", key="ratear_gerar"):
            linhas_cc = [{"obra": str(r["Obra"]).strip(),
                          "codigo": mapa_obra.get(str(r["Obra"]).strip(), ""),
                          "valor": r["Valor"]}
                         for _, r in ed_cc.iterrows()
                         if str(r.get("Obra", "")).strip()]
            linhas_cat = [{"categoria": str(r["Categoria"]).strip(),
                           "codigo": mapa_cat.get(str(r["Categoria"]).strip(), ""),
                           "valor": r["Valor"]}
                          for _, r in ed_cat.iterrows()
                          if str(r.get("Categoria", "")).strip()]
            res = rateio.gerar_jsons(linhas_cc, linhas_cat,
                                     base_cat=(base_cat or None))
            if res.get("erro"):
                st.warning(res["erro"])
            else:
                if res.get("distribuicao"):
                    st.markdown("**Rateio Centro de Custo**")
                    st.code(res["distribuicao"], language="json")
                if res.get("categorias"):
                    st.markdown("**Rateio Categoria de Despesa**")
                    st.code(res["categorias"], language="json")
                if not res.get("categorias"):
                    st.caption("Categoria não gerada (preencha as categorias à direita, "
                               "se desejar).")


# ----------------------------------------------------------------------------
# ABA SINCRONIZAÇÃO
# ----------------------------------------------------------------------------
if _secao == "🏦 Bradesco":
    st.markdown("#### Conferência de pagamentos do Bradesco × SPsBD")
    st.caption("Cole a tela **Detalhes das Operações** do Bradesco (pode misturar boleto, "
               "conta de consumo e Pix, de várias empresas). O app separa em duas tabelas: "
               "**Boletos/Consumo** (casa por nº da SP + código de barras — dupla validação) e "
               "**Pix** (casa por conta de débito + valor; BeeVale por valor exato ou +1,5%). "
               "Sinaliza já pago, conta debitada diferente do cadastro, valor diferente e "
               "duplicidade no lote.")
    txt_brad = st.text_area("Cole aqui o texto do Bradesco", height=240, key="brad_txt")
    foco_ag = st.checkbox(
        "Focar em SPs agendadas (Agendar / Agendado / Falha Agendar)",
        value=True, key="brad_foco",
        help="Restringe os candidatos às SPs na fila de pagamento — reduz muito o ruído "
             "e é onde está o risco real de pagar algo já pago. Desmarque para considerar todas.")
    if st.button("🔎 Cruzar com a SPsBD", type="primary", key="brad_run"):
        try:
            st.session_state["brad_res"] = bradesco.cruzar_tudo(txt_brad, df,
                                                                foco_agendados=foco_ag)
        except Exception as e:
            st.session_state["brad_res"] = {"erro": str(e)}

    _COR_CLASSE = {
        "ALTA": "#38761d", "ÚNICO": "#38761d", "OK (ID+barras)": "#38761d",
        "OK (barras)": "#38761d", "OK (só ID)": "#38761d", "NOME OK": "#38761d",
        "MÉDIA": "#bf9000", "BEEVALE?": "#bf9000", "AMBÍGUO": "#e69138",
        "ALERTA (barras)": "#e69138", "ALERTA (ID)": "#e69138",
        "NOME (conta difere)": "#e69138",
        "CONFLITO": "#cc0000", "SEM MATCH": "#cc0000", "NOME DIVERGE": "#cc0000"}

    def _render_brad(linhas, col_classe, cols):
        import html as _html
        dfb = pd.DataFrame(linhas)
        cols = [c for c in cols if c in dfb.columns]
        # 'Card' é a única coluna de ID que fica (a antiga 'SP' era a mesma coisa);
        # no cabeçalho ela aparece como 'SP'.
        _hdr = {"Card": "SP"}
        css = (
            "<style>"
            ".bradwrap{overflow-x:auto;max-width:100%;}"
            "table.brad{border-collapse:collapse;font-size:13px;width:100%;}"
            "table.brad th,table.brad td{border:1px solid #e3e6e3;padding:4px 8px;"
            "text-align:left;vertical-align:top;}"
            "table.brad th{background:#eef3ee;white-space:nowrap;position:sticky;top:0;}"
            "table.brad td.nw{white-space:nowrap;}"
            "table.brad td.alert{white-space:normal;word-break:break-word;"
            "min-width:240px;max-width:380px;color:#cc0000;font-weight:600;}"
            "</style>")
        ths = "".join(f"<th>{_html.escape(str(_hdr.get(c, c)))}</th>" for c in cols)
        trs = []
        for _, row in dfb.iterrows():
            tds = []
            for c in cols:
                val = row.get(c, "")
                s = "" if (val is None or (isinstance(val, float) and pd.isna(val))) else str(val)
                if c == "Alertas":
                    esc = _html.escape(s)
                    # transforma IDs de SP (9–12 dígitos) em links para o Pipefy
                    esc = re.sub(
                        r"(?<!\d)(\d{9,12})(?!\d)",
                        r"<a href='https://app.pipefy.com/open-cards/\1' "
                        r"target='_blank'>\1</a>", esc)
                    tds.append(f"<td class='alert'>{esc}</td>")
                elif c == "Card":
                    if s:
                        m = re.search(r"(\d+)", s)
                        num = m.group(1) if m else s
                        tds.append(f"<td class='nw'><a href='{_html.escape(s)}' "
                                   f"target='_blank'>{_html.escape(num)}</a></td>")
                    else:
                        tds.append("<td></td>")
                elif c == col_classe:
                    cor = _COR_CLASSE.get(s, "")
                    tds.append(f"<td class='nw' style='color:{cor};font-weight:600'>"
                               f"{_html.escape(s)}</td>")
                else:
                    tds.append(f"<td class='nw'>{_html.escape(s)}</td>")
            trs.append("<tr>" + "".join(tds) + "</tr>")
        st.markdown(
            css + "<div class='bradwrap'><table class='brad'><thead><tr>"
            + ths + "</tr></thead><tbody>" + "".join(trs) + "</tbody></table></div>",
            unsafe_allow_html=True)

    def _totais_brad(linhas, titulo):
        """Somatório geral e por conta debitada (coluna 'Valor (Bradesco)')."""
        if not linhas:
            return
        from collections import defaultdict
        total = 0.0
        por_conta = defaultdict(lambda: [0.0, 0])     # conta -> [soma, qtd]
        for r in linhas:
            v = bradesco.to_float(r.get("Valor (Bradesco)", "") or 0)
            total += v
            ck = str(r.get("Conta (Bradesco)", "") or "—")
            por_conta[ck][0] += v
            por_conta[ck][1] += 1
        linhas_md = [f"**{titulo} — Total: {_fmt_moeda(total)}**  ·  {len(linhas)} operação(ões)"]
        if len(por_conta) > 1:
            linhas_md.append("Por conta debitada:")
            for ck in sorted(por_conta):
                soma, qtd = por_conta[ck]
                linhas_md.append(f"- **{ck}**: {_fmt_moeda(soma)}  ·  {qtd}")
        st.markdown("\n".join(linhas_md))

    res_brad = st.session_state.get("brad_res")
    if isinstance(res_brad, dict) and res_brad.get("erro"):
        st.error(f"Falha ao processar: {res_brad['erro']}")
    elif isinstance(res_brad, dict):
        bol, pix = res_brad.get("boletos", []), res_brad.get("pix", [])
        if not bol and not pix:
            st.warning("Nenhuma operação reconhecida. Confira se colou o **texto** da tela "
                       "Detalhes das Operações (com as linhas 'Boleto de Cobrança - …', "
                       "'Conta de Consumo - …' e 'Pagamento Pix').")
        n_alert = sum(1 for r in (bol + pix) if r.get("Alertas"))
        st.markdown(f"**{len(bol)}** boleto(s)/consumo · **{len(pix)}** pix · "
                    f"⚠️ {n_alert} com alerta")
        if bol:
            st.markdown("##### 📄 Boletos e Contas de Consumo")
            _render_brad(bol, "Validação",
                         ["Alertas", "Tipo", "Empresa", "Conta (Bradesco)", "Valor (Bradesco)",
                          "SP (Bradesco)", "Cód. barras", "Card", "Validação",
                          "Confiança", "Credor (SP)", "Doc Fiscal", "Status Pgt",
                          "Status Agend", "Centro de Custo", "Vencimento", "Conta (SP)",
                          "Diferença"])
            _totais_brad(bol, "Boletos/Consumo")
        if pix:
            st.markdown("##### ⚡ Pix")
            _render_brad(pix, "Classificação",
                         ["Alertas", "Tipo", "Empresa", "Conta (Bradesco)", "Valor (Bradesco)",
                          "Nome (Bradesco)", "Card", "Classificação", "Confiança", "Credor (SP)",
                          "Doc Fiscal", "Status Pgt", "Status Agend", "Centro de Custo",
                          "Vencimento", "Conta (SP)", "Diferença"])
            _totais_brad(pix, "Pix")
        if bol or pix:
            st.caption("💡 A coluna **SP** abre o card no Pipefy. Revise as linhas com alerta "
                       "(vermelho), as CONFLITO/SEM MATCH e os Pix AMBÍGUO.")

if _secao == "🔄 Sincronização":
    st.markdown("#### Como funciona a atualização dos dados")
    with st.expander("Entenda em 30 segundos"):
        st.markdown(
            "- **Cache local:** os dados ficam salvos no seu computador (arquivo "
            "`spsbd_cache.db`). Por isso o app abre rápido e funciona offline.\n"
            "- **Recarregar tudo:** baixa a SPsBD inteira e regrava o cache. Use na 1ª vez "
            "ou quando quiser forçar uma atualização geral.\n"
            "- **Buscar atualizações:** traz só as **linhas que mudaram** desde a última vez "
            "(rápido). Acontece automaticamente ao abrir o app, uma vez por sessão.\n"
            "- Para 'Buscar atualizações' detectar edições feitas por outros (via automações), "
            "o reconciliador do Apps Script precisa estar ligado (carimba a coluna V). "
            "Enquanto não estiver, use **Recarregar tudo** para ter certeza do que está atual.\n"
            "- **Conta:** vem direto da coluna **U** da SPsBD."
        )

    c1, c2, c3 = st.columns(3)
    c1.metric("SPs no cache", cache.contar())
    c2.metric("Última atualização", _data_hora_br(cache.get_meta("ultimo_sync", "—")))
    c3.metric("📤 Alterações em fila", cache.fila_contar())

    _pend = cache.fila_pendentes()
    if _pend:
        st.warning(f"{len(_pend)} alteração(ões) aguardando envio ao Sheets "
                   "(serão reenviadas sozinhas a cada 90 s e ao reabrir o app).")
        with st.expander("Ver fila pendente"):
            st.dataframe(pd.DataFrame(_pend)[
                ["sp_id", "coluna", "valor", "tentativas", "ultimo_erro", "criado_em"]],
                hide_index=True, **_FULLW)
        if st.button("📤 Tentar enviar agora"):
            with st.spinner("Enviando alterações pendentes…"):
                restam = _drenar_fila()
            recarregar()
            if restam == 0:
                st.success("Tudo enviado.")
            else:
                st.warning(f"Ainda restam {restam} (verifique a conexão). Continuará tentando.")
            st.rerun()

    st.divider()
    online = gsheets.disponivel()
    st.markdown(f"**Conexão com o Google Sheets:** "
                f"{'🟢 ligada' if online else '⚪ desligada (sem credenciais.json)'}")

    if online:
        cba, cbb = st.columns(2)
        if cba.button("⬇️ Recarregar tudo"):
            try:
                with st.spinner("Lendo a SPsBD inteira…"):
                    n = gsheets.bootstrap()
                recarregar()
                st.success(f"{n} SPs recarregadas.")
                st.rerun()
            except Exception as e:
                st.error("Falha temporária de conexão com o Google Sheets. "
                         "Tente de novo em alguns segundos.")
                st.caption(f"Detalhe técnico: {type(e).__name__}")
        if cbb.button("🔄 Buscar atualizações"):
            try:
                with st.spinner("Trazendo só o que mudou…"):
                    m = gsheets.sync_delta()
                recarregar()
                st.success(f"{m['mudadas']} alteradas · {m['removidas']} removidas.")
                st.rerun()
            except Exception as e:
                st.error("Falha temporária de conexão com o Google Sheets. "
                         "Tente de novo em alguns segundos (a conexão oscilou).")
                st.caption(f"Detalhe técnico: {type(e).__name__}")

        st.divider()
        st.markdown("**🧾 SP Fiscal (Documentação Fiscal)**")
        st.caption(f"Fonte: planilha *Lançamentos* · SPs com Doc. Fiscal no cache: "
                   f"**{cache.contar_sp_fiscal()}**.")
        if st.button("🧾 Atualizar SP Fiscal"):
            try:
                with st.spinner("Lendo Documentação Fiscal…"):
                    n = gsheets.sync_sp_fiscal()
                recarregar()
                st.success(f"{n} SP(s) com Documentação Fiscal sincronizada(s).")
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao sincronizar SP Fiscal: {e}")
        st.divider()
        st.markdown("**🔐 Credenciais / Tokens**")
        _tok_ok = config.tem_token("PIPEFY_TOKEN")
        st.caption(f"Pipefy: {'🟢 carregado' if _tok_ok else '⚪ não carregado'} · "
                   f"Fonte: planilha `{config.ABA_CONFIG}` (coluna A = nome, B = valor).")
        if st.button("🔐 Atualizar tokens"):
            try:
                with st.spinner("Lendo credenciais…"):
                    d = config.atualizar_tokens()
                st.success(f"{len(d)} credencial(is) carregada(s): {', '.join(d.keys()) or '—'}.")
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao ler credenciais: {e}")
    else:
        st.info("Para ligar a conexão, coloque o JSON da Service Account em "
                "`credenciais.json` na pasta do app e compartilhe a SPsBD "
                "com o e-mail da Service Account. Para 'Buscar atualizações' "
                "pegar edições de outros, instale o reconciliador do Apps Script e rode "
                "`recar_configurarDoZero()` uma vez.")

    st.divider()
    if st.button("♻️ Recarregar do seed CSV (descarta cache)"):
        import sqlite3
        sqlite3.connect(cache.DB_PATH).execute("DELETE FROM sps")
        cache.seed_de_csv(SEED_CSV)
        recarregar()
        st.rerun()

    st.divider()
    with st.expander("🔧 Larguras das colunas (descobrir px sugeridos)"):
        st.caption("Mostra, para os dados de agora, quantos pixels cabem o maior texto "
                   "de cada coluna. Copie o bloco abaixo e cole por cima de "
                   "`LARGURAS_COLUNAS`, no topo do app.py — depois ajuste o que quiser.")
        if st.button("📏 Calcular larguras sugeridas", key="calc_larguras"):
            try:
                vv = _monta_visao_ag(df)
                linhas = ['    "sel":               44,    # caixa de seleção',
                          '    "Abrir":             55,    # ➔ (só ícone)']
                for _src, _lab in GRID_COLS:
                    if _lab in ("Abrir", "Comprovante") or _lab not in vv.columns:
                        continue
                    px = _w_conteudo(vv[_lab], _lab)
                    linhas.append(f'    "{_lab}":{" " * max(1, 18 - len(_lab))}{px},')
                linhas.append('    "Comprovante":      100,    # ↓☁ (só ícone)')
                bloco = "LARGURAS_COLUNAS = {\n" + "\n".join(linhas) + "\n}"
                st.code(bloco, language="python")
            except Exception as e:
                st.warning(f"Não consegui calcular agora: {type(e).__name__}")


# ----------------------------------------------------------------------------
# ABA CONFIGURAÇÕES — personalização das tabelas (e futuras opções)
# ----------------------------------------------------------------------------
if _secao == "⚙️ Configurações":
    st.markdown("### ⚙️ Configurações")
    st.caption("Ajustes do app. Por enquanto: **personalização das tabelas**. "
               "Outras opções virão aqui conforme a necessidade.")

    with st.expander("📋 Tabelas — colunas, largura e quebra de texto", expanded=True):
        st.caption(
            "Por coluna: **Exibir** (mostrar ou não), **Largura (px)** — digite **0** "
            "para o app dimensionar sozinho, ou um número para largura fixa — e **Quebrar "
            "texto** (texto longo passa para várias linhas). Vale para Solicitações, Lote "
            "e Relatório. Edite à vontade e clique em **Salvar** — só aí grava e aplica "
            "(enquanto edita, nada recarrega, fica rápido).")

        _cfg_atual = _cfg_tabela()
        _linhas_cfg = []
        for _src, _lab in GRID_COLS:
            c = _cfg_atual.get(_lab, {})
            _linhas_cfg.append({
                "Coluna": _lab,
                "Exibir": bool(c.get("visivel", True)),
                "Largura (px)": int(c.get("largura", 0) or 0),
                "Quebrar texto": bool(c.get("quebra", False)),
            })
        _df_cfg = pd.DataFrame(_linhas_cfg)

        # st.form: as edições só são processadas ao clicar em Salvar (sem rerun a
        # cada tecla -> rápido). Ao submeter, lê o estado final do editor de uma vez.
        with st.form("cfg_tab_form", clear_on_submit=False):
            _editado = st.data_editor(
                _df_cfg, hide_index=True, key="cfg_tab_editor", **_FULLW,
                disabled=["Coluna"], num_rows="fixed", height=560,
                column_config={
                    "Coluna": st.column_config.TextColumn("Coluna", width="medium"),
                    "Exibir": st.column_config.CheckboxColumn("Exibir", help="Mostrar a coluna"),
                    "Largura (px)": st.column_config.NumberColumn(
                        "Largura (px)", min_value=0, max_value=800, step=10, format="%d",
                        help="0 = automático (o app dimensiona). Ou digite os pixels."),
                    "Quebrar texto": st.column_config.CheckboxColumn(
                        "Quebrar texto", help="Texto longo quebra em várias linhas"),
                })
            _salvar = st.form_submit_button("💾 Salvar", type="primary")

        if _salvar:
            novo = {}
            for _, r in _editado.iterrows():
                lab = str(r["Coluna"])
                try:
                    larg = int(r["Largura (px)"])
                except (TypeError, ValueError):
                    larg = 0
                novo[lab] = {
                    "visivel": bool(r["Exibir"]),
                    "largura": max(0, min(800, larg)),       # 0 = automático
                    "quebra": bool(r["Quebrar texto"]),
                }
            _salvar_config_tabela(novo)
            st.session_state.pop("cfg_tab_editor", None)
            st.session_state["_flash"] = ("success", "Configuração das tabelas salva e aplicada.")
            st.rerun()

        if st.button("↩️ Restaurar padrão de fábrica", key="cfg_tab_reset"):
            try:
                cache.set_meta("tabela_config", "")
            except Exception:
                pass
            st.session_state.pop("_cfg_tabela", None)
            st.session_state.pop("cfg_tab_editor", None)
            st.session_state["_flash"] = ("success", "Tabelas restauradas ao padrão de fábrica.")
            st.rerun()


# ----------------------------------------------------------------------------
# ABA LOG (auditoria permanente — retenção 90 dias)
# ----------------------------------------------------------------------------
if _secao == "📅 Agenda":
    import calendar as _calmod
    from datetime import date as _date, timedelta as _td

    st.subheader("📅 Agenda do Financeiro")
    st.caption("Compromissos e tarefas recorrentes (contas, transferências, "
               "impostos…). Base compartilhada na planilha de Credenciais (aba "
               "Agenda). Os lembretes aparecem como aviso ao abrir o app.")

    # Identidade de quem cadastra (apontar 'criado por').
    _topo = st.columns([2, 1, 1])
    st.session_state.setdefault("agenda_usuario", "")
    st.session_state["agenda_usuario"] = _topo[0].text_input(
        "Seu nome (para registrar quem cadastrou)",
        value=st.session_state.get("agenda_usuario", ""), key="ag_usuario_in")
    if _topo[1].button("🔄 Sincronizar agenda", use_container_width=True):
        try:
            agenda.sincronizar()
            st.session_state["_flash"] = ("success", "Agenda sincronizada.")
        except Exception as e:
            st.session_state["_flash"] = ("error", f"Falha ao sincronizar: {e}")
        st.rerun()
    if _topo[2].button("➕ Novo compromisso", use_container_width=True, type="primary"):
        st.session_state["agenda_edit_id"] = "__novo__"
        st.rerun()

    # Carrega da planilha em background (sem travar). Se ainda vazio, avisa.
    if not st.session_state.get("_agenda_boot"):
        st.session_state["_agenda_boot"] = True
        def _wk_ag():
            try:
                agenda.bootstrap()
            except Exception:
                pass
        threading.Thread(target=_wk_ag, daemon=True).start()
    _lista = agenda.carregar()
    _hoje = _date.today()
    if not _lista:
        st.info("Carregando a agenda da planilha… se não aparecer em alguns segundos, "
                "clique em **🔄 Sincronizar agenda**.")

    # ---- Formulário (novo/editar) ----
    _edit_id = st.session_state.get("agenda_edit_id")
    if _edit_id:
        _novo = _edit_id == "__novo__"
        _reg = ({} if _novo else
                next((c for c in _lista if c.get("id") == _edit_id), {}))
        with st.container(border=True):
            st.markdown("#### " + ("Novo compromisso" if _novo else "Editar compromisso"))
            cA, cB = st.columns(2)
            _tit = cA.text_input("Título*", value=_reg.get("titulo", ""), key="ag_tit")
            _cat = cB.selectbox("Categoria", agenda.CATEGORIAS,
                                index=(agenda.CATEGORIAS.index(_reg["categoria"])
                                       if _reg.get("categoria") in agenda.CATEGORIAS else 0),
                                key="ag_cat")
            _desc = st.text_area("Descrição (opcional)", value=_reg.get("descricao", ""),
                                 key="ag_desc", height=70)
            cC, cD = st.columns(2)
            _data = cC.date_input("Data (1ª ocorrência)",
                                  value=(agenda.parse_date(_reg.get("data_base")) or _hoje),
                                  format="DD/MM/YYYY", key="ag_data")
            _rec = cD.selectbox("Recorrência", agenda.RECORRENCIAS,
                                index=(agenda.RECORRENCIAS.index(_reg["recorrencia"])
                                       if _reg.get("recorrencia") in agenda.RECORRENCIAS else 0),
                                key="ag_rec")
            # O dia da recorrência SAI da data escolhida (sem campo separado p/ não
            # conflitar). Mensal usa o dia da data; 31 = último dia do mês.
            _diam = _data.day
            _dows = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
            if _rec == "mensal":
                st.caption(f"🔁 Recorre **todo dia {_diam}** de cada mês (fim de mês ajusta: "
                           f"escolha uma data no dia 31 para 'último dia do mês').")
            elif _rec == "anual":
                st.caption(f"🔁 Recorre **todo {_data.strftime('%d/%m')}** de cada ano.")
            elif _rec == "semanal":
                st.caption(f"🔁 Recorre **toda {_dows[_data.weekday()]}**.")
            cF, cG = st.columns(2)
            _sug = agenda.ajuste_sugerido(_cat)
            _aj_atual = _reg.get("ajuste_dia_util") or _sug
            _aj = cF.selectbox(
                "Se cair em fim de semana/feriado", agenda.AJUSTES,
                index=agenda.AJUSTES.index(_aj_atual if _aj_atual in agenda.AJUSTES else _sug),
                key="ag_aj",
                help=f"Sugestão para '{_cat}': {_sug}. posterga = próximo dia útil; "
                     f"antecipa = dia útil anterior.")
            _alerta = cG.number_input("Alertar quantos dias antes", min_value=0, max_value=60,
                                      value=int(float(_reg.get("alerta_dias_antes") or 3)),
                                      step=1, key="ag_alerta")
            _resp = st.text_input("Responsável (opcional)", value=_reg.get("responsavel", ""),
                                  key="ag_resp")

            bb = st.columns([1, 1, 4])
            if bb[0].button("💾 Salvar", type="primary", key="ag_salvar"):
                if not _tit.strip():
                    st.warning("Informe o título.")
                else:
                    dados = {"titulo": _tit.strip(), "categoria": _cat,
                             "descricao": _desc.strip(), "data_base": _data.isoformat(),
                             "recorrencia": _rec, "dia_mes": int(_diam),
                             "ajuste_dia_util": _aj, "alerta_dias_antes": int(_alerta),
                             "responsavel": _resp.strip()}
                    try:
                        with st.spinner("Salvando na planilha…"):
                            if _novo:
                                agenda.adicionar(dados, criado_por=st.session_state.get("agenda_usuario", ""))
                            else:
                                agenda.atualizar(_edit_id, dados)
                        st.session_state.pop("agenda_edit_id", None)
                        st.session_state["_flash"] = ("success", "Compromisso salvo.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao salvar (online?): {e}")
            if bb[1].button("Cancelar", key="ag_cancelar"):
                st.session_state.pop("agenda_edit_id", None)
                st.rerun()

    # ---- Navegação do mês ----
    st.session_state.setdefault("agenda_ano_mes", (_hoje.year, _hoje.month))
    _ano, _mes = st.session_state["agenda_ano_mes"]
    nav = st.columns([1, 2, 1, 3])
    if nav[0].button("◀", key="ag_prev", use_container_width=True):
        _mes -= 1
        if _mes < 1:
            _mes = 12; _ano -= 1
        st.session_state["agenda_ano_mes"] = (_ano, _mes); st.rerun()
    nav[1].markdown(f"### {_ano}-{_mes:02d}  ·  "
                    f"{['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'][_mes-1]}")
    if nav[2].button("▶", key="ag_next", use_container_width=True):
        _mes += 1
        if _mes > 12:
            _mes = 1; _ano += 1
        st.session_state["agenda_ano_mes"] = (_ano, _mes); st.rerun()
    if nav[3].button("Hoje", key="ag_hoje"):
        st.session_state["agenda_ano_mes"] = (_hoje.year, _hoje.month)
        st.session_state["agenda_dia"] = _hoje.isoformat(); st.rerun()

    # ---- Ocorrências do mês visível (grid completo: inclui dias vizinhos) ----
    _semanas = _calmod.Calendar(firstweekday=6).monthdatescalendar(_ano, _mes)  # 6=Domingo
    _ini = _semanas[0][0]; _fim = _semanas[-1][-1]
    _fer = agenda._todos_feriados({_ini.year, _fim.year})
    _por_dia = {}
    for c in _lista:
        if str(c.get("status", "ativo")).strip().lower() not in ("", "ativo"):
            continue
        for d in agenda.ocorrencias(c, _ini, _fim, _fer):
            _por_dia.setdefault(d, []).append(c)

    # ---- Grade do calendário ----
    _dias_sem = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]
    hc = st.columns(7)
    for i, dn in enumerate(_dias_sem):
        hc[i].markdown(f"**{dn}**")
    for semana in _semanas:
        cols = st.columns(7)
        for i, d in enumerate(semana):
            n = len(_por_dia.get(d, []))
            do_mes = (d.month == _mes)
            rotulo = f"{d.day}"
            if d == _hoje:
                rotulo = f"•{d.day}"
            if n:
                rotulo += f"  ({n})"
            if not do_mes:
                rotulo = f"·{d.day}"
            if cols[i].button(rotulo, key=f"ag_dia_{d.isoformat()}",
                              use_container_width=True,
                              type=("primary" if n and do_mes else "secondary")):
                st.session_state["agenda_dia"] = d.isoformat()
                st.rerun()

    # ---- Detalhe do dia selecionado ----
    _sel = agenda.parse_date(st.session_state.get("agenda_dia"))
    if _sel:
        st.divider()
        st.markdown(f"#### {agenda.fmt_br(_sel)} — compromissos do dia")
        itens = _por_dia.get(_sel, [])
        if not itens:
            st.caption("_(nada neste dia)_")
        for c in itens:
            feitas = agenda._concluidas(c)
            ok = _sel in feitas
            with st.container(border=True):
                topo = st.columns([5, 1, 1, 1])
                marca = "✅ " if ok else ""
                rec = c.get("recorrencia", "nenhuma")
                topo[0].markdown(
                    f"{marca}**{c.get('titulo','')}**  ·  _{c.get('categoria','')}_"
                    + (f"  ·  🔁 {rec}" if rec not in ("", "nenhuma") else "")
                    + (f"\n\n{c.get('descricao')}" if c.get("descricao") else "")
                    + (f"\n\n👤 {c.get('responsavel')}" if c.get("responsavel") else "")
                    + (f"  ·  cadastrado por {c.get('criado_por')}" if c.get("criado_por") else ""))
                if not ok and topo[1].button("✓ Concluir", key=f"ag_ok_{c['id']}_{_sel}"):
                    try:
                        agenda.concluir(c["id"], _sel)
                        st.session_state["_flash"] = ("success", "Compromisso concluído.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha: {e}")
                if topo[2].button("✏️ Editar", key=f"ag_ed_{c['id']}"):
                    st.session_state["agenda_edit_id"] = c["id"]; st.rerun()
                if topo[3].button("🗑️ Excluir", key=f"ag_del_{c['id']}"):
                    try:
                        agenda.remover(c["id"])
                        st.session_state["_flash"] = ("success", "Compromisso excluído.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha: {e}")

    # ---- Próximos (lista rápida 30 dias) ----
    st.divider()
    st.markdown("#### Próximos 30 dias")
    prox = []
    for c in _lista:
        if str(c.get("status", "ativo")).strip().lower() not in ("", "ativo"):
            continue
        feitas = agenda._concluidas(c)
        for d in agenda.ocorrencias(c, _hoje, _hoje + _td(days=30), _fer):
            if d not in feitas:
                prox.append((d, c))
    prox.sort(key=lambda x: (x[0], x[1].get("titulo", "")))
    if not prox:
        st.caption("_(nada nos próximos 30 dias)_")
    for d, c in prox[:40]:
        dias = (d - _hoje).days
        quando = "hoje" if dias == 0 else ("amanhã" if dias == 1 else f"em {dias}d")
        st.markdown(f"- **{agenda.fmt_br(d)}** ({quando}) — {c.get('titulo','')} "
                    f"_{c.get('categoria','')}_")


if _secao == "🧾 Log":
    st.markdown("Histórico de **todas as alterações** feitas pelo app (aplicação local "
                "e envio ao Sheets). Mantém os **últimos 90 dias**.")

    cont = cache.log_contar()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total (90 dias)", sum(cont.values()))
    m2.metric("✅ Enviados", cont.get("enviado", 0))
    m3.metric("📤 Pendentes", cont.get("pendente", 0))
    m4.metric("⚠️ Erros", cont.get("erro", 0))

    f1, f2, f3 = st.columns([1, 1, 2])
    fstatus = f1.selectbox("Status", ["todos", "enviado", "pendente", "erro"], key="log_status")
    fdias = f2.selectbox("Período", [7, 30, 90], index=2, key="log_dias",
                         format_func=lambda d: f"últimos {d} dias")
    fbusca = f3.text_input("Buscar ID", key="log_busca", placeholder="parte do ID")

    linhas = cache.log_listar(dias=fdias, status=fstatus, busca=(fbusca or None))
    if not linhas:
        st.info("Nenhum registro para o filtro atual.")
    else:
        dlog = pd.DataFrame(linhas)
        dlog["coluna"] = dlog["coluna"].map(lambda k: LABELS.get(k, k))
        dlog = dlog.rename(columns={
            "criado_em": "Quando", "sp_id": "ID", "coluna": "Campo", "valor": "Novo valor",
            "acao": "Ação", "status": "Status", "enviado_em": "Enviado em",
            "tentativas": "Tent.", "ultimo_erro": "Último erro"})
        st.caption(f"{len(dlog)} registro(s) — clique no ícone de copiar de cada célula "
                   "ou baixe o CSV abaixo.")
        st.dataframe(dlog, hide_index=True, **_FULLW)
        st.download_button("⬇️ Baixar log (CSV)",
                           dlog.to_csv(index=False).encode("utf-8-sig"),
                           file_name="log_alteracoes.csv", mime="text/csv")