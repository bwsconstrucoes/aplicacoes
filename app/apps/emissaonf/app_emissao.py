# -*- coding: utf-8 -*-
"""
Espelho editável + emissão da NFS-e (Eusébio/CE) — Streamlit.

Abra com o ID do card na URL:   ...?card_id=1375191806
Fluxo na tela:
  1) carrega a nota do Pipefy (worker.preparar) e mostra o espelho fiel;
  2) permite ajustar a DISCRIMINAÇÃO (o corpo da nota) — a prévia reflete a edição;
  3) "Confirmar e Emitir" assina com o texto final, envia ao Eusébio, e ao receber
     número/código dispara o pós-emissão imediato (concluir.py).

Roda local hoje (streamlit run app_emissao.py) e sobe no Render depois — é a mesma app.

Segurança: trava anti-emissão-dupla por card (uma vez emitido na sessão, não reenvia).
Para mudar valores/alíquotas/tomador, ajuste no card do Pipefy e clique em "Recarregar"
— assim o cálculo tributário continua vindo da fonte da verdade (o card), sem duplicar
a lógica aqui.
"""
from __future__ import annotations
import io
import os
import tempfile
import contextlib

import streamlit as st
import streamlit.components.v1 as components

from worker import preparar
import montar_emissao
import el_nfse_envio as envio
import preview
import validacao
import concluir as mod_concluir

st.set_page_config(page_title="Emissão NFS-e — BWS", layout="wide")


# --------------------------------------------------------------------------- #
def _cert_temp(cert_pem: bytes, chave_pem: bytes):
    cf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); cf.write(cert_pem); cf.close()
    kf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); kf.write(chave_pem); kf.close()
    return cf.name, kf.name


def _carregar(card_id: str):
    """Roda o pipeline e guarda o contexto na sessão."""
    ctx = preparar(card_id)
    st.session_state["ctx"] = ctx
    st.session_state["ctx_card_id"] = card_id
    st.session_state["discr"] = getattr(ctx.get("dados_rps"), "discriminacao", "") or ""


def _emitir(card_id: str, ctx: dict, discriminacao: str) -> dict:
    """Aplica a discriminação editada, re-assina, envia ao Eusébio e devolve o
    resultado (número, código, data, xml oficial ou erros)."""
    dados = ctx["dados_rps"]
    dados.discriminacao = discriminacao
    xml = montar_emissao.gerar_xml_preview(dados, ctx["chave_pem"], ctx["cert_pem"])

    cp, kp = _cert_temp(ctx["cert_pem"], ctx["chave_pem"])
    try:
        resp = envio.enviar(xml, de_verdade=True, incluir_cabec=True, cert=(cp, kp))
    finally:
        for p in (cp, kp):
            try:
                os.unlink(p)
            except OSError:
                pass
    res = envio.parse_resposta(resp.text)
    res["http_status"] = resp.status_code
    res["resposta_bruta"] = resp.text
    return res


# --------------------------------------------------------------------------- #
st.title("Emissão de NFS-e · Eusébio/CE")

qp = st.query_params
card_id = st.text_input("ID do card (Pipefy)", value=qp.get("card_id", "")).strip()

if not card_id:
    st.info("Abra com **?card_id=NUMERO** na URL, ou informe o ID acima.")
    st.stop()

col_a, col_b = st.columns([1, 5])
recarregar = col_a.button("🔄 Recarregar do card")
precisa_carregar = ("ctx" not in st.session_state
                    or st.session_state.get("ctx_card_id") != card_id
                    or recarregar)

if precisa_carregar:
    with st.spinner("Carregando a nota do Pipefy..."):
        try:
            _carregar(card_id)
        except Exception as e:
            st.error(f"Não consegui carregar o card {card_id}: {type(e).__name__}: {e}")
            st.stop()

ctx = st.session_state.get("ctx")
if not ctx:
    st.stop()

card, obra, r = ctx["card"], ctx["obra"], ctx["r"]
prox = ctx["prox"]

# Cabeçalho resumido (escapando o $ para o Streamlit não interpretar como LaTeX)
st.markdown(
    f"**Obra:** {card.get('codigo_obra','')} &nbsp;|&nbsp; "
    f"**Medição:** {card.get('numero_medicao','')} &nbsp;|&nbsp; "
    f"**Nº esperado da NFS-e:** {prox} &nbsp;|&nbsp; "
    f"**Valor da nota:** R\\$ {preview.brl(r.valor_total)} &nbsp;|&nbsp; "
    f"**Líquido:** R\\$ {preview.brl(r.valor_liquido)}"
)

if not ctx.get("assinado"):
    st.error("⚠️ O XML NÃO foi assinado (certificado/senha ausente). "
             "Confira o certificado.p12 e a senha na aba Credenciais. Emissão bloqueada.")

avisos = ctx.get("avisos") or []
if avisos:
    with st.expander(f"⚠️ {len(avisos)} aviso(s) na montagem da nota", expanded=True):
        for a in avisos:
            st.write("• ", a)

# Já emitida nesta sessão? mostra o resultado e não reenvia.
res_key = f"emitido_{card_id}"
if res_key in st.session_state:
    res = st.session_state[res_key]
    st.success(f"✅ NFS-e **{res.get('numero')}** já emitida nesta sessão "
               f"(código {res.get('codigo_verificacao')}).")
    if st.session_state.get(f"log_{card_id}"):
        with st.expander("Log do pós-emissão (concluir)", expanded=True):
            st.code(st.session_state[f"log_{card_id}"], language="text")
    st.caption("Para emitir outra nota, troque o ID do card no topo.")
    st.stop()

# --------------------------------------------------------------------------- #
# Validação de teto de valor (não deixa emitir acima do Valor da Medição)
val = validacao.checar(card, r)

st.subheader("Validação de valor")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Valor da Medição (teto)", f"R$ {validacao.brl(val['cap'])}")
c2.metric("Já emitido (notas válidas)", f"R$ {validacao.brl(val['ja_valido'])}")
c3.metric("Esta nota", f"R$ {validacao.brl(val['atual'])}")
if val["restante"] is not None:
    c4.metric("Saldo após esta nota", f"R$ {validacao.brl(val['cap'] - val['total'])}")

if val["slots"]:
    with st.expander(f"Notas já lançadas no card ({len(val['slots'])})", expanded=False):
        for x in val["slots"]:
            marca = "✅" if x["valida"] else "⚠️"
            st.write(f"{marca} Slot {x['slot']} — nº {x['numero'] or '—'} — "
                     f"status **{x['status']}** — R$ {validacao.brl(x['valor'])}")

for b in val["bloqueios"]:
    st.error("🚫 " + b)
for a in val["avisos"]:
    st.warning("⚠️ " + a)

# --------------------------------------------------------------------------- #
# Corpo editável + espelho (espelho em largura cheia para não cortar)
st.subheader("Corpo da nota (editável)")
discr = st.text_area(
    "Discriminação dos serviços",
    value=st.session_state.get("discr", ""),
    height=200,
    help="Este é o corpo da nota. O espelho abaixo reflete a edição quando você clica "
         "fora do campo.",
)
st.session_state["discr"] = discr
st.caption("Valores, alíquotas e tomador vêm do card no Pipefy. Para mudá-los, ajuste no "
           "card e clique em **Recarregar do card**.")

st.subheader("Espelho")
try:
    html = preview.montar_preview_html(
        card, obra, r, prox, prox, ctx.get("ibge"),
        tomador_end=ctx.get("end_tom"), discriminacao_override=discr,
    )
    components.html(html, height=1050, scrolling=True)
except Exception as e:
    st.warning(f"Não consegui montar o espelho visual ({e}). A emissão não depende disso.")

# --------------------------------------------------------------------------- #
st.divider()
st.subheader("Emitir")

bloqueado = (not ctx.get("assinado")) or (not val["ok"])
if not val["ok"]:
    st.error("Emissão bloqueada pela validação de valor acima. "
             "Ajuste o Valor Parcial/Medição no card e clique em **Recarregar do card**.")
confirmar = st.checkbox("Confiro os dados acima e **autorizo a emissão** desta NFS-e",
                        disabled=bloqueado)
emitir_click = st.button("✅ Confirmar e Emitir", type="primary",
                         disabled=(bloqueado or not confirmar))

if emitir_click:
    # trava: marca cedo para evitar reenvio em reruns
    if st.session_state.get(f"emitindo_{card_id}"):
        st.warning("Emissão já em andamento.")
        st.stop()
    st.session_state[f"emitindo_{card_id}"] = True

    with st.status("Emitindo a NFS-e...", expanded=True) as status:
        try:
            st.write("Assinando e enviando ao Eusébio...")
            res = _emitir(card_id, ctx, st.session_state["discr"])
        except Exception as e:
            st.session_state[f"emitindo_{card_id}"] = False
            status.update(label="Falha no envio", state="error")
            st.error(f"Erro ao enviar: {type(e).__name__}: {e}")
            st.stop()

        if not res.get("numero"):
            st.session_state[f"emitindo_{card_id}"] = False
            status.update(label="A prefeitura não retornou número", state="error")
            st.error("❌ A prefeitura NÃO retornou número de NFS-e.")
            for er in (res.get("erros") or []):
                st.write("• ", er)
            with st.expander("Resposta bruta"):
                st.code((res.get("resposta_bruta") or "")[:3500], language="xml")
            st.stop()

        numero = res["numero"]
        codigo = res.get("codigo_verificacao", "")
        data_iso = (res.get("data_emissao") or "")[:10]
        st.write(f"✅ Emitida: número **{numero}**, código **{codigo}**, data {data_iso}.")
        if str(numero) != str(prox):
            st.warning(f"Número devolvido ({numero}) ≠ esperado ({prox}). Confira a numeração.")

        # salva a nota oficial em arquivo p/ o concluir
        nota_path = f"NFSe_{numero}.xml"
        try:
            if res.get("nota_xml"):
                with open(nota_path, "w", encoding="utf-8") as fh:
                    fh.write(res["nota_xml"])
        except Exception as e:
            st.warning(f"Não consegui salvar {nota_path}: {e}")

        # guarda o resultado ANTES do pós-emissão (a nota já existe na prefeitura)
        st.session_state[res_key] = res

        # pós-emissão imediato (concluir) — captura o log
        st.write("Rodando o pós-emissão (Notas BWS, Omie, Pipefy, Drive, WhatsApp)...")
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                mod_concluir.concluir(card_id, numero, codigo, data_iso, nota_path)
        except Exception as e:
            buf.write(f"\n>>> ERRO no concluir: {type(e).__name__}: {e}")
        log = buf.getvalue()
        st.session_state[f"log_{card_id}"] = log
        st.session_state[f"emitindo_{card_id}"] = False
        status.update(label=f"NFS-e {numero} emitida e processada", state="complete")

    st.rerun()
