# -*- coding: utf-8 -*-
"""
Blueprint Flask da emissão de NFS-e (Eusébio/CE) — substitui o app Streamlit.
Página HTML com espelho + discriminação editável + botão "Confirmar e Emitir".

Registrado em app/main.py com url_prefix="/emissao". Link no card do Pipefy:
    https://SEU-DOMINIO/emissao?card_id={{card_id}}&token=SEU_EMISSAO_NF_TOKEN

Os módulos do emissaonf são scripts planos (import worker, import validacao...),
então injetamos a pasta no sys.path para que os imports funcionem dentro do Flask,
sem depender da pasta de trabalho.
"""
from __future__ import annotations
import os
import sys
import io
import uuid
import html
import contextlib
import threading
import tempfile

# permite os imports planos dos módulos desta pasta (worker, validacao, etc.)
_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from flask import Blueprint, request, redirect, url_for, Response

import worker as _worker
import validacao as _val
import preview as _preview
import montar_emissao as _me
import el_nfse_envio as _envio
import concluir as _concluir
import substituicao as _sub
import omie
from decimal import Decimal
import completar_imediato as _compl

bp = Blueprint("emissao", __name__)

_LOCK = threading.Lock()
_EMITINDO: set[str] = set()
_RESULTADOS: dict[str, dict] = {}
_NAC_LOCK = threading.Lock()


def _token_ok() -> bool:
    esperado = os.getenv("EMISSAO_NF_TOKEN") or os.getenv("EMISSAO_TOKEN") or ""
    if not esperado:
        return True                       # sem token configurado (dev) -> livre
    return request.values.get("token", "") == esperado


def _cert_temp(cert_pem: bytes, chave_pem: bytes):
    cf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); cf.write(cert_pem); cf.close()
    kf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); kf.write(chave_pem); kf.close()
    return cf.name, kf.name


def _diag_cert() -> str:
    """Diz exatamente por que o certificado não carregou (env, base64, senha, .p12)."""
    import base64 as _b64
    b64 = os.getenv("EMISSAO_NF_CERTIFICADO_P12_BASE64") or os.getenv("CERTIFICADO_P12_BASE64") or ""
    senha = (os.getenv("EMISSAO_NF_CERTIFICADO_SENHA") or os.getenv("CERTIFICADO_SENHA")
             or os.getenv("SENHA_CERTIFICADO") or os.getenv("CERT_SENHA") or "")
    if not b64:
        return "a env EMISSAO_NF_CERTIFICADO_P12_BASE64 não está definida (ou veio vazia) no serviço."
    if not senha:
        return "a env EMISSAO_NF_CERTIFICADO_SENHA não está definida no serviço."
    try:
        raw = _b64.b64decode(b64)
    except Exception as e:
        return f"o base64 do certificado é inválido ({e}). Refaça a cópia com Set-Clipboard."
    if len(raw) < 200:
        return (f"o base64 decodifica para apenas {len(raw)} bytes — provavelmente foi truncado "
                f"na cópia. Refaça com Set-Clipboard e cole de novo.")
    try:
        from cryptography.hazmat.primitives.serialization import pkcs12
        pkcs12.load_key_and_certificates(raw, senha.encode("utf-8"))
        return ("certificado e senha carregam OK isoladamente — se a página ainda diz 'não assinado', "
                "o código deployado pode estar desatualizado (confirme o push de el_nfse_abrasf.py/credenciais.py).")
    except Exception as e:
        return (f"falha ao abrir o .p12 com a senha: {type(e).__name__}: {e}. "
                f"Quase sempre é senha incorreta ou .p12 truncado.")


# --------------------------------------------------------------------------- #
# Rotas
# --------------------------------------------------------------------------- #
def _so_num(v) -> str:
    """Só os dígitos do número da nota (robusto a '3.076'/'3076,00' do Pipefy)."""
    return "".join(c for c in str(v or "") if c.isdigit())


# Contas p/ Pagamento (vão na discriminação). Mesmas opções do campo do Pipefy;
# servem para suprir o campo numa substituição, em que o card não o traz mais.
CONTAS_PAGAMENTO = [
    "Bradesco - Agência: 624-6 - Conta Corrente: 7011-4",
    "Bradesco - Agência: 624-6 - Conta Corrente: 49211-6",
    "Bradesco - Agência: 624-6 - Conta Corrente: 22069-8",
    "Bradesco - Agência: 624-6 - Conta Corrente: 50302-9",
    "Bradesco - Agência: 624-6 - Conta Corrente: 50024-0",
    "Banco do Brasil - Agência: 3515-7 - Conta Corrente: 22970-9",
    "Banco do Nordeste - Agência: 313 - Conta Corrente: 168-5",
    "Caixa Econômica - Agência: 1977 - Conta Corrente: 2625-5 - Op. 003",
    "Banco Sicredi (748) - Agência: 2301 - Conta Corrente: 30008-0",
]


@bp.route("/", methods=["GET"])
def pagina():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado — link inválido ou sem token."),
                        status=403, mimetype="text/html")
    card_id = (request.args.get("card_id") or "").strip()
    token = request.args.get("token", "")
    nota_sub = _so_num(request.args.get("nota_substituida") or request.args.get("notasubstituida"))
    if not card_id:
        return Response(_pagina_pedir_card(token), mimetype="text/html")
    try:
        ctx = _worker.preparar(card_id)
    except Exception as e:
        return Response(_pagina_erro(f"Erro ao carregar o card {card_id}: "
                                     f"{type(e).__name__}: {e}"), mimetype="text/html")
    return Response(_render_pagina(ctx, card_id, token, nota_sub), mimetype="text/html")


@bp.route("/emitir", methods=["POST"])
def emitir():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    card_id = (request.form.get("card_id") or "").strip()
    token = request.form.get("token", "")
    discr = request.form.get("discriminacao", "")
    nota_sub = _so_num(request.form.get("nota_substituida"))
    if not card_id:
        return Response(_pagina_erro("card_id ausente."), status=400, mimetype="text/html")
    if request.form.get("confirmo") != "on":
        return Response(_pagina_erro("Você precisa marcar a confirmação antes de emitir."),
                        mimetype="text/html")

    with _LOCK:
        if card_id in _EMITINDO:
            return Response(_pagina_erro("Emissão já em andamento para este card."), mimetype="text/html")
        _EMITINDO.add(card_id)
    try:
        ctx = _worker.preparar(card_id)
        if nota_sub and not _sub.localizar_slot_por_numero(ctx["card"], nota_sub):
            return Response(_pagina_erro(
                f"Substituição: a NF {nota_sub} não foi encontrada nos slots A–E deste card. "
                f"Confira o número no parâmetro 'nota_substituida' do link."), mimetype="text/html")
        # regra do município: só permite substituir por valor IGUAL ou MAIOR. Barramos antes de tentar.
        if nota_sub:
            _slots_v = {_so_num(x["numero"]): x["valor"] for x in _val.slots_preenchidos(ctx["card"])}
            _v_old = _slots_v.get(nota_sub)
            try:
                _v_new = Decimal(str(getattr(ctx["r"], "valor_total", 0) or 0))
            except Exception:
                _v_new = None
            if _v_old is not None and _v_new is not None and _v_new < _v_old:
                return Response(_pagina_erro(
                    f"Substituição bloqueada: o valor da nova nota (R$ {_val.brl(_v_new)}) é MENOR que o da "
                    f"NF {nota_sub} (R$ {_val.brl(_v_old)}). O município só permite substituir por valor igual "
                    f"ou maior. Valor menor (ou troca de competência) é caso de cancelamento — passo à parte."),
                    mimetype="text/html")
        val = _val.checar(ctx["card"], ctx["r"], ignorar_numero=nota_sub or None)   # revalida no servidor
        if not val["ok"]:
            return Response(_pagina_erro("Bloqueado pela validação: " + " | ".join(val["bloqueios"])),
                            mimetype="text/html")
        if not ctx.get("assinado"):
            return Response(_pagina_erro("XML não assinado (certificado/senha ausente)."),
                            mimetype="text/html")

        dados = ctx["dados_rps"]
        dados.discriminacao = discr or getattr(dados, "discriminacao", "")
        # substituição na prefeitura: a nova nota carrega o RPS da antiga (RpsSubstituido)
        if nota_sub:
            dados.rps_substituido_numero = nota_sub
        xml = _me.gerar_xml_preview(dados, ctx["chave_pem"], ctx["cert_pem"])

        cp, kp = _cert_temp(ctx["cert_pem"], ctx["chave_pem"])
        try:
            resp = _envio.enviar(xml, de_verdade=True, incluir_cabec=True, cert=(cp, kp))
        finally:
            for p in (cp, kp):
                try:
                    os.unlink(p)
                except OSError:
                    pass

        res = _envio.parse_resposta(resp.text)
        if not res.get("numero"):
            erros = "; ".join(res.get("erros") or []) or "sem detalhes"
            corpo = resp.text or ""
            diag = (f"HTTP {resp.status_code}\n"
                    f"Content-Type: {resp.headers.get('Content-Type', '?')}\n"
                    f"Tamanho: {len(resp.content)} bytes\n"
                    f"URL final: {resp.url}\n"
                    f"------ início do corpo (até 1800 chars) ------\n"
                    f"{corpo[:1800] if corpo else '(corpo vazio)'}")
            return Response(_pagina_erro_diag(
                "A prefeitura NÃO retornou número de NFS-e. " + erros, diag),
                mimetype="text/html")

        numero = res["numero"]
        codigo = res.get("codigo_verificacao", "")
        data_iso = (res.get("data_emissao") or "")[:10]

        nota_path = os.path.join(_DIR, f"NFSe_{numero}.xml")
        if res.get("nota_xml"):
            try:
                with open(nota_path, "w", encoding="utf-8") as fh:
                    fh.write(res["nota_xml"])
            except Exception:
                pass

        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                _concluir.concluir(card_id, numero, codigo, data_iso, nota_path, ctx=ctx,
                                   nota_substituida=(nota_sub or None))
        except Exception as e:
            buf.write(f"\n>>> ERRO no concluir: {type(e).__name__}: {e}")

        # SUBSTITUIÇÃO: com a nova nota já concluída, cancela a antiga (card + planilha)
        sub_info = None
        if nota_sub:
            sub_info = {"numero_antigo": nota_sub, "card": "", "planilha": ""}
            try:
                tk = ctx["cred"]["PIPEFY_TOKEN"]
                rc = _sub.cancelar_no_card(ctx["card"], nota_sub, tk, novo_numero=numero)
                sub_info["card"] = rc["msg"]
                buf.write(f"\n>>> SUBSTITUIÇÃO (card): {rc['msg']}")
            except Exception as e:
                sub_info["card"] = f"ERRO ao cancelar no card: {type(e).__name__}: {e}"
                buf.write(f"\n>>> SUBSTITUIÇÃO (card) ERRO: {e}")
            try:
                ws_notas = _worker.abrir_aba(ctx["gc"].open_by_key(_worker.ID_PROC), _worker.ABA_NOTAS)
                rp = _sub.cancelar_na_planilha(ws_notas, nota_sub, novo_numero=numero)
                sub_info["planilha"] = rp["msg"]
                buf.write(f"\n>>> SUBSTITUIÇÃO (planilha): {rp['msg']}")
            except Exception as e:
                sub_info["planilha"] = f"ERRO ao marcar na planilha: {type(e).__name__}: {e}"
                buf.write(f"\n>>> SUBSTITUIÇÃO (planilha) ERRO: {e}")
            # Omie: remove o nº da nota cancelada do campo numero_documento_fiscal do título
            try:
                integracao = ctx["card"].get("omie_integracao", "")
                if integracao:
                    _, doc_omie = omie.remover_documento(ctx["cred"], integracao, nota_sub)
                    sub_info["omie"] = f"nº {nota_sub} removido do título (documento agora: {doc_omie or '(vazio)'})"
                    buf.write(f"\n>>> SUBSTITUIÇÃO (Omie): {sub_info['omie']}")
                else:
                    sub_info["omie"] = "card sem omie_integracao — nada a remover no Omie"
                    buf.write(f"\n>>> SUBSTITUIÇÃO (Omie): {sub_info['omie']}")
            except Exception as e:
                sub_info["omie"] = f"ERRO ao remover no Omie: {type(e).__name__}: {e}"
                buf.write(f"\n>>> SUBSTITUIÇÃO (Omie) ERRO: {e}")

        # dispara a busca nacional em background (não bloqueia a resposta ao usuário)
        try:
            threading.Thread(target=_auto_nacional_bg, daemon=True).start()
        except Exception:
            pass

        rid = uuid.uuid4().hex
        _RESULTADOS[rid] = {"numero": numero, "codigo": codigo, "data": data_iso,
                            "log": buf.getvalue(), "card_id": card_id, "prox": ctx.get("prox"),
                            "sub": sub_info}
        return redirect(url_for(".resultado", id=rid, token=token))
    except Exception as e:
        return Response(_pagina_erro(f"Erro ao emitir: {type(e).__name__}: {e}"), mimetype="text/html")
    finally:
        with _LOCK:
            _EMITINDO.discard(card_id)


@bp.route("/resultado", methods=["GET"])
def resultado():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    r = _RESULTADOS.get(request.args.get("id", ""))
    if not r:
        return Response(_pagina_erro("Resultado não encontrado (link expirou?)."), mimetype="text/html")
    return Response(_pagina_resultado(r), mimetype="text/html")


@bp.route("/diag", methods=["GET"])
def diag():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    b64 = os.getenv("EMISSAO_NF_CERTIFICADO_P12_BASE64") or os.getenv("CERTIFICADO_P12_BASE64") or ""
    linhas = [
        f"EMISSAO_NF_CERTIFICADO_P12_BASE64 definida: {'sim' if b64 else 'NÃO'}"
        + (f" (tamanho do texto: {len(b64)} chars)" if b64 else ""),
        f"EMISSAO_NF_CERTIFICADO_SENHA definida: "
        f"{'sim' if (os.getenv('EMISSAO_NF_CERTIFICADO_SENHA') or os.getenv('CERTIFICADO_SENHA')) else 'NÃO'}",
        "",
        "Resultado do teste de carregamento:",
        _diag_cert(),
    ]
    # --- Google Drive: qual service account + personificação ---
    import json as _json
    from base64 import b64decode as _b64d
    sa_email = sa_cid = "?"
    try:
        _info = _json.loads(_b64d(os.getenv("GOOGLE_CREDENTIALS_BASE64", "")).decode("utf-8"))
        sa_email = _info.get("client_email", "?")
        sa_cid = _info.get("client_id", "?")
    except Exception as e:
        sa_email = f"(erro ao ler GOOGLE_CREDENTIALS_BASE64: {e})"
    _imp = os.getenv("EMISSAO_NF_DRIVE_IMPERSONAR", "contato@bwsconstrucoes.com.br")
    linhas += [
        "",
        "----- Google Drive -----",
        f"Service account (client_email): {sa_email}",
        f"Client ID (este é o número a autorizar na DWD): {sa_cid}",
        f"Personificando: {_imp or '(vazio = sem personificação / modo Drive Compartilhado)'}",
    ]
    corpo = "<h1>Diagnóstico</h1><pre>" + html.escape("\n".join(linhas)) + "</pre>"
    return Response(_doc("Diagnóstico", corpo), mimetype="text/html")


def _ids_da_nota(xml_texto: str):
    """Extrai (numero, codigo_verificacao, data_iso) de InfNfse do XML ABRASF."""
    import xml.etree.ElementTree as _ET
    root = _ET.fromstring(xml_texto.encode("utf-8"))
    for el in root.iter():                       # remove namespace p/ os find funcionarem
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    inf = root.find(".//InfNfse")
    if inf is None:
        return "", "", ""
    def _txt(tag):
        e = inf.find(tag)
        return (e.text or "").strip() if e is not None else ""
    data = _txt("DataEmissao")
    return _txt("Numero"), _txt("CodigoVerificacao"), (data[:10] if data else "")


def _pagina_recuperar(token):
    t = html.escape(token)
    return _doc("Recuperar entrega", f"""
      <h1>Recuperar entrega de nota já emitida</h1>
      <div class='card'>
        <p class='sub'>Para notas emitidas cujos documentos não subiram ao Drive (ex.: falha de
        credencial). NÃO refaz Notas BWS / Omie / slots — só a entrega (Drive + links + Descrição).
        É idempotente, pode repetir.</p>
        <form method='post' action='{url_for('.recuperar')}'>
          <label class='lbl'>card_id (Pipefy):
            <input name='card_id' style='padding:8px;border:1px solid #c8d0da;border-radius:6px'></label>
          <label class='lbl'>XML da NFS-e (baixe no portal da prefeitura e cole aqui):</label>
          <textarea name='xml' rows='12' placeholder='<?xml ...><CompNfse>...'></textarea>
          <label class='lbl'><input type='checkbox' name='reenviar_whatsapp'> reenviar WhatsApp
            (deixe DESMARCADO — já foi enviado na emissão)</label>
          <label class='lbl'><input type='checkbox' name='completo'> rodar <b>concluir completo</b>
            (marque SÓ se a nota foi emitida mas NÃO rodou bookkeeping — ex.: erro no concluir.
            Faz Notas BWS + Omie + slots + entrega. Tem trava anti-duplicação.)</label>
          <input type='hidden' name='token' value='{t}'>
          <button type='submit'>Reprocessar entrega</button>
        </form>
      </div>""")


@bp.route("/recuperar", methods=["GET", "POST"])
def recuperar():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    token = request.values.get("token", "")
    if request.method == "GET":
        return Response(_pagina_recuperar(token), mimetype="text/html")

    card_id = (request.form.get("card_id") or "").strip()
    xml_texto = (request.form.get("xml") or "").strip()
    zap = request.form.get("reenviar_whatsapp") == "on"
    completo = request.form.get("completo") == "on"
    if not card_id or not xml_texto:
        return Response(_pagina_erro("Informe o card_id e cole o XML da nota."), mimetype="text/html")
    try:
        numero, codigo, data_iso = _ids_da_nota(xml_texto)
    except Exception as e:
        return Response(_pagina_erro(f"XML inválido: {e}"), mimetype="text/html")
    if not numero:
        return Response(_pagina_erro("Não encontrei o Número da NFS-e no XML (esperava InfNfse/Numero). "
                                     "Confira se colou o XML completo da nota."), mimetype="text/html")

    tmp = os.path.join(_DIR, f"recuperar_{numero}.xml")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(xml_texto)
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            if completo:
                # nota emitida sem bookkeeping: roda o concluir inteiro (tem trava ja_existe)
                _concluir.concluir(card_id, numero, codigo, data_iso, tmp)
            else:
                _compl.completar(card_id, numero, codigo, data_iso, tmp,
                                 enviar_whatsapp=zap, discriminacao="")
    except Exception as e:
        buf.write(f"\n>>> ERRO: {type(e).__name__}: {e}")
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

    modo = "concluir completo" if completo else "entrega"
    corpo = (f"<h1>Recuperação da NF {html.escape(numero)} ({modo})</h1>"
             f"<div class='ok'>Reprocessado — card {html.escape(card_id)}, "
             f"código {html.escape(codigo)}, emissão {html.escape(data_iso)}.</div>"
             f"<div class='card'><b>Log</b><pre>{html.escape(buf.getvalue())}</pre></div>"
             "<p class='sub'>Confira no log se cada passo concluiu sem ERRO.</p>")
    return Response(_doc("Recuperação", corpo), mimetype="text/html")


def _rodar_nacional() -> str:
    """Roda o job nacional uma vez (protegido por lock). Devolve o log."""
    if not _NAC_LOCK.acquire(blocking=False):
        return "(job nacional já em execução — pulei esta chamada)"
    buf = io.StringIO()
    try:
        import job_nacional
        with contextlib.redirect_stdout(buf):
            job_nacional.rodar()
    except Exception as e:
        buf.write(f"\n>>> ERRO job nacional: {type(e).__name__}: {e}")
    finally:
        _NAC_LOCK.release()
    return buf.getvalue()


def _rodar_sefin_bg() -> str:
    """Fecha pendentes pela SEFIN (DPS/chave) — caminho rápido, sem NSU."""
    if not _NAC_LOCK.acquire(blocking=False):
        return "(nacional já em execução — pulei)"
    buf = io.StringIO()
    try:
        import job_nacional
        with contextlib.redirect_stdout(buf):
            job_nacional.fechar_via_sefin()
    except Exception as e:
        buf.write(f"\n>>> ERRO SEFIN: {type(e).__name__}: {e}")
    finally:
        _NAC_LOCK.release()
    return buf.getvalue()


def _auto_nacional_bg():
    """Após emitir, fecha o nacional pela SEFIN assim que ele sobe (segundos).
    A SEFIN tem a nota pela chave/DPS bem antes do ADN distribuir por NSU, então
    tentamos em 60/180/300s — a maioria fecha já junto da emissão. O Cron de
    10 em 10 min fica só como rede de segurança."""
    import time
    for atraso in (60, 180, 300):
        time.sleep(atraso)
        _rodar_sefin_bg()


@bp.route("/nacional", methods=["GET"])
def nacional():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    log = _rodar_nacional()
    corpo = ("<h1>Busca nacional (manual)</h1>"
             "<p class='sub'>Roda o job que casa as notas pendentes com o ADN e fecha a parte nacional "
             "(DANFSe + chave + QR + 3º link na Descrição). Pode repetir — é idempotente.</p>"
             f"<div class='card'><b>Log</b><pre>{html.escape(log)}</pre></div>")
    return Response(_doc("Busca nacional", corpo), mimetype="text/html")


@bp.route("/nacional_chave", methods=["GET"])
def nacional_chave():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    chave = "".join(c for c in (request.values.get("chave") or "") if c.isdigit())
    if not chave:
        t = html.escape(request.values.get("token", ""))
        corpo = f"""
          <h1>Fechar nacional pela chave</h1>
          <div class='card'>
            <p class='sub'>Cole a <b>chave de acesso nacional</b> (50 dígitos) e ele busca o XML
            nacional direto na SEFIN com o certificado — sem captcha, sem esperar o NSU — gera a
            DANFSe, regenera a municipal com a chave e completa os links. A chave aparece no portal
            do município assim que a nota é transmitida.</p>
            <form method='get' action='{url_for('.nacional_chave')}'>
              <label class='lbl'>Chave de acesso nacional (50 dígitos):</label>
              <input type='text' name='chave' placeholder='2304285...' style='width:100%'>
              <input type='hidden' name='token' value='{t}'>
              <button type='submit'>Fechar nacional</button>
            </form>
          </div>"""
        return Response(_doc("Fechar nacional pela chave", corpo), mimetype="text/html")
    buf = io.StringIO()
    try:
        import job_nacional
        with contextlib.redirect_stdout(buf):
            job_nacional.fechar_por_chave(chave)
    except Exception as e:
        buf.write(f"\n>>> ERRO: {type(e).__name__}: {e}")
    corpo = ("<h1>Fechar nacional pela chave</h1>"
             f"<div class='card'><b>Chave:</b> {html.escape(chave)}<pre>{html.escape(buf.getvalue())}</pre></div>")
    return Response(_doc("Fechar nacional pela chave", corpo), mimetype="text/html")


@bp.route("/diag_nacional_chave", methods=["GET"])
def diag_nacional_chave():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    chave = "".join(c for c in (request.values.get("chave") or "") if c.isdigit())
    if not chave:
        t = html.escape(request.values.get("token", ""))
        corpo = f"""
          <h1>Diagnóstico nacional por chave</h1>
          <div class='card'>
            <p class='sub'>Testa, com o certificado da BWS, vários endpoints federais de busca
            POR CHAVE — pra descobrir se dá pra pegar a NFS-e/DANFSe nacional sem captcha e sem
            esperar a distribuição por NSU. Só faz GET (não altera nada).</p>
            <form method='get' action='{url_for('.diag_nacional_chave')}'>
              <label class='lbl'>Chave de acesso nacional (50 dígitos):</label>
              <input type='text' name='chave' placeholder='2304285...' style='width:100%'>
              <input type='hidden' name='token' value='{t}'>
              <button type='submit'>Rodar diagnóstico</button>
            </form>
          </div>"""
        return Response(_doc("Diagnóstico nacional por chave", corpo), mimetype="text/html")
    buf = io.StringIO()
    try:
        import job_nacional
        if request.values.get("modo") == "dps":
            buf.write(job_nacional.diag_dps_chave(chave))
        else:
            buf.write(job_nacional.diag_federal_chave(chave))
    except Exception as e:
        buf.write(f">>> ERRO: {type(e).__name__}: {e}")
    corpo = ("<h1>Diagnóstico nacional por chave</h1>"
             f"<div class='card'><b>Chave:</b> {html.escape(chave)}<pre>{html.escape(buf.getvalue())}</pre></div>")
    return Response(_doc("Diagnóstico nacional por chave", corpo), mimetype="text/html")


@bp.route("/nacional_xml", methods=["GET", "POST"])
def nacional_xml():
    if not _token_ok():
        return Response(_pagina_erro("Acesso não autorizado."), status=403, mimetype="text/html")
    token = request.values.get("token", "")
    if request.method == "GET":
        t = html.escape(token)
        corpo = f"""
          <h1>Fechar nacional pelo XML</h1>
          <div class='card'>
            <p class='sub'>Use quando a nota já existe no nacional (você baixou o XML no portal),
            mas ainda não saiu na distribuição por NSU. Cole o <b>XML nacional</b> e ele fecha a
            pendente na hora — gera a DANFSe, regenera a municipal com a chave e completa os links.</p>
            <form method='post' action='{url_for('.nacional_xml')}'>
              <label class='lbl'>XML nacional (baixado do portal):</label>
              <textarea name='xml' rows='14' placeholder='<?xml ...><NFSe ...>'></textarea>
              <input type='hidden' name='token' value='{t}'>
              <button type='submit'>Fechar nacional</button>
            </form>
          </div>"""
        return Response(_doc("Fechar nacional pelo XML", corpo), mimetype="text/html")

    xml_nac = (request.form.get("xml") or "").strip()
    if not xml_nac:
        return Response(_pagina_erro("Cole o XML nacional."), mimetype="text/html")
    buf = io.StringIO()
    try:
        import job_nacional
        with contextlib.redirect_stdout(buf):
            job_nacional.fechar_por_xml_nacional(xml_nac)
    except Exception as e:
        buf.write(f"\n>>> ERRO: {type(e).__name__}: {e}")
    corpo = ("<h1>Fechar nacional pelo XML</h1>"
             f"<div class='card'><b>Log</b><pre>{html.escape(buf.getvalue())}</pre></div>")
    return Response(_doc("Fechar nacional pelo XML", corpo), mimetype="text/html")


# --------------------------------------------------------------------------- #
# HTML
# --------------------------------------------------------------------------- #
_CSS = """
<style>
 body{font-family:Arial,Helvetica,sans-serif;background:#eef1f4;margin:0;padding:24px;color:#1a2230}
 .wrap{max-width:1000px;margin:0 auto}
 h1{font-size:20px;margin:0 0 4px} .sub{color:#555;margin:0 0 16px}
 .card{background:#fff;border:1px solid #d8dee6;border-radius:8px;padding:16px;margin:0 0 16px}
 .metrics{display:flex;gap:12px;flex-wrap:wrap}
 .metric{flex:1;min-width:160px;background:#f6f8fb;border:1px solid #e1e7ee;border-radius:6px;padding:10px}
 .metric .l{font-size:12px;color:#667} .metric .v{font-size:18px;font-weight:700}
 .err{background:#fdecea;border:1px solid #f5c6c2;color:#a32118;padding:10px;border-radius:6px;margin:6px 0}
 .warn{background:#fff7e6;border:1px solid #ffe1a8;color:#8a5a00;padding:10px;border-radius:6px;margin:6px 0}
 .ok{background:#e9f7ef;border:1px solid #b6e2c6;color:#1a7a3c;padding:10px;border-radius:6px;margin:6px 0}
 textarea{width:100%;box-sizing:border-box;font:13px monospace;padding:8px;border:1px solid #c8d0da;border-radius:6px}
 iframe{width:100%;height:1100px;border:1px solid #d8dee6;border-radius:6px;background:#fff}
 button{background:#1a7a3c;color:#fff;border:0;border-radius:6px;padding:12px 20px;font-size:15px;cursor:pointer}
 button:disabled{background:#9bb;cursor:not-allowed}
 .lbl{display:block;margin:8px 0;font-size:14px}
 pre{background:#0f1620;color:#d5e3f0;padding:12px;border-radius:6px;overflow:auto;font-size:12px}
 a.btn{display:inline-block;background:#1a7a3c;color:#fff;text-decoration:none;padding:10px 18px;border-radius:6px}
</style>
"""


def _doc(titulo, corpo):
    return (f"<!DOCTYPE html><html lang='pt-br'><head><meta charset='utf-8'>"
            f"<meta name='viewport' content='width=device-width, initial-scale=1'>"
            f"<title>{html.escape(titulo)}</title>{_CSS}</head>"
            f"<body><div class='wrap'>{corpo}</div></body></html>")


def _pagina_pedir_card(token):
    t = html.escape(token)
    return _doc("Emissão NFS-e", f"""
      <h1>Emissão de NFS-e · Eusébio/CE</h1>
      <div class='card'>
        <form method='get' action=''>
          <label class='lbl'>ID do card (Pipefy):
            <input name='card_id' style='padding:8px;border:1px solid #c8d0da;border-radius:6px'>
          </label>
          <input type='hidden' name='token' value='{t}'>
          <button type='submit'>Carregar</button>
        </form>
        <p class='sub'>Ou abra direto com <code>?card_id=NUMERO&amp;token=...</code></p>
      </div>""")


def _pagina_erro(msg):
    return _doc("Erro", f"<h1>Emissão de NFS-e</h1><div class='err'>{html.escape(msg)}</div>")


def _pagina_erro_diag(msg, diag):
    return _doc("Erro", f"<h1>Emissão de NFS-e</h1><div class='err'>{html.escape(msg)}</div>"
                f"<div class='card'><b>Resposta crua da prefeitura (para diagnóstico)</b>"
                f"<pre>{html.escape(diag)}</pre></div>")


def _render_pagina(ctx, card_id, token, nota_sub=""):
    card, obra, r = ctx["card"], ctx["obra"], ctx["r"]
    prox = ctx["prox"]
    val = _val.checar(card, r, ignorar_numero=nota_sub or None)
    discr_atual = getattr(ctx.get("dados_rps"), "discriminacao", "") or ""

    # substituição (opcional, via ?nota_substituida=NNN)
    slot_old = _sub.localizar_slot_por_numero(card, nota_sub) if nota_sub else None
    sub_ok = (not nota_sub) or bool(slot_old)
    sub_banner = sub_hidden = sub_bloqueio = ""
    if nota_sub and slot_old:
        _si = {_so_num(x["numero"]): x for x in _val.slots_preenchidos(card)}
        _vo = _si.get(nota_sub, {}).get("valor")
        _vt = f" (R$ {_val.brl(_vo)})" if _vo is not None else ""
        sub_banner = ("<div style='background:#fde7c2;border:2px solid #e08600;color:#7a4a00;"
                      "padding:14px;border-radius:8px;margin:0 0 16px;font-size:15px'>"
                      f"🔁 <b>SUBSTITUIÇÃO DE NOTA</b> — esta emissão substitui a "
                      f"<b>NF {html.escape(str(nota_sub))}</b>{_vt} (slot <b>{slot_old}</b>). "
                      f"Ao emitir, a NF {html.escape(str(nota_sub))} será marcada como "
                      "<b>Cancelada</b> no card e na planilha 'Notas BWS'. "
                      "O teto abaixo já desconsidera a nota substituída.</div>")
        sub_hidden = f"<input type='hidden' name='nota_substituida' value='{html.escape(str(nota_sub))}'>"
    elif nota_sub and not slot_old:
        sub_bloqueio = (f"<div class='err'>🚫 SUBSTITUIÇÃO: a NF {html.escape(str(nota_sub))} não foi "
                        "encontrada nos slots A–E deste card. Confira o número no parâmetro "
                        "'nota_substituida' do link.</div>")

    # espelho (HTML completo) embutido num iframe isolado
    try:
        espelho = _preview.montar_preview_html(card, obra, r, prox, prox, ctx.get("ibge"),
                                               tomador_end=ctx.get("end_tom"),
                                               discriminacao_override=discr_atual)
    except Exception as e:
        espelho = f"<p>Não foi possível montar o espelho: {html.escape(str(e))}</p>"
    iframe = f"<iframe srcdoc=\"{html.escape(espelho, quote=True)}\"></iframe>"

    # cabeçalho + métricas
    cab = (f"<h1>Emissão de NFS-e · Eusébio/CE</h1>"
           f"<p class='sub'>Obra <b>{html.escape(str(card.get('codigo_obra','')))}</b> · "
           f"Medição <b>{html.escape(str(card.get('numero_medicao','')))}</b> · "
           f"Nº esperado <b>{html.escape(str(prox))}</b></p>")
    metrics = (f"<div class='metrics'>"
               f"<div class='metric'><div class='l'>Valor da Medição (teto)</div>"
               f"<div class='v'>R$ {_val.brl(val['cap'])}</div></div>"
               f"<div class='metric'><div class='l'>Já emitido (válidas)</div>"
               f"<div class='v'>R$ {_val.brl(val['ja_valido'])}</div></div>"
               f"<div class='metric'><div class='l'>Esta nota</div>"
               f"<div class='v'>R$ {_val.brl(val['atual'])}</div></div>"
               f"<div class='metric'><div class='l'>Saldo após esta nota</div>"
               f"<div class='v'>R$ {_val.brl(val['cap'] - val['total'])}</div></div></div>")

    alertas = "".join(f"<div class='err'>🚫 {html.escape(b)}</div>" for b in val["bloqueios"])
    alertas += "".join(f"<div class='warn'>⚠️ {html.escape(a)}</div>" for a in val["avisos"])
    if not ctx.get("assinado"):
        alertas += ("<div class='err'>⚠️ XML não assinado — emissão bloqueada.<br><b>Motivo:</b> "
                    + html.escape(_diag_cert()) + "</div>")

    alertas += sub_bloqueio
    pode = val["ok"] and ctx.get("assinado") and sub_ok

    # seletor de Conta p/ Pagamento — reescreve a linha na discriminação (que é o corpo emitido).
    _opts_conta = "".join(f"<option value='{html.escape(c)}'>{html.escape(c)}</option>"
                          for c in CONTAS_PAGAMENTO)
    _conta_hint = ("  <span style='color:#b35900;font-weight:bold'>— substituição: o card não traz a "
                   "conta, escolha aqui (vai na discriminação)</span>" if nota_sub else "")
    # JS como string normal (NÃO f-string): troca a linha 'Conta p/ Pagamento:' no textarea.
    # Usa replace com regex (.* para no fim da linha) — sem precisar de \n.
    _script_conta = (
        "<script>function _aplicarConta(sel){"
        "if(!sel.value)return;"
        "var ta=document.querySelector(\"textarea[name='discriminacao']\");"
        "if(!ta)return;"
        "ta.value=ta.value.replace(/Conta p\\/ Pagamento:.*/, 'Conta p/ Pagamento: '+sel.value);"
        "}</script>"
    )

    # formulário de emissão
    if pode:
        form = f"""
        <div class='card'>
          <form method='post' action='{url_for('.emitir')}' onsubmit="this.querySelector('button').disabled=true;this.querySelector('button').textContent='Emitindo...';">
            <label class='lbl'><b>Discriminação dos serviços</b> (editável — é o corpo da nota):</label>
            <textarea name='discriminacao' rows='8'>{html.escape(discr_atual)}</textarea>
            <label class='lbl' style='margin-top:8px'><b>Conta p/ Pagamento</b>{_conta_hint}</label>
            <select onchange="_aplicarConta(this)"
                    style="width:100%;box-sizing:border-box;padding:8px;border:1px solid #c8d0da;border-radius:6px;margin-bottom:6px">
              <option value="">— manter a conta atual da discriminação —</option>
              {_opts_conta}
            </select>
            {_script_conta}
            <input type='hidden' name='card_id' value='{html.escape(card_id)}'>
            <input type='hidden' name='token' value='{html.escape(token)}'>
            {sub_hidden}
            <label class='lbl'><input type='checkbox' name='confirmo' onchange="document.getElementById('btn').disabled=!this.checked">
              Confiro os dados e <b>autorizo a emissão</b> desta NFS-e.</label>
            <button id='btn' type='submit' disabled>✅ Confirmar e Emitir</button>
          </form>
          <p class='sub'>Valores, alíquotas e tomador vêm do card. Para mudá-los, ajuste no Pipefy e recarregue a página.</p>
        </div>"""
    else:
        form = "<div class='card'><div class='warn'>Emissão bloqueada pela validação acima. " \
               "Ajuste o card no Pipefy e recarregue a página.</div></div>"

    return _doc("Emissão NFS-e", sub_banner + cab + f"<div class='card'>{metrics}{alertas}</div>"
                + form + f"<div class='card'><b>Espelho</b>{iframe}</div>")


def _pagina_resultado(r):
    log = html.escape(r.get("log", "") or "")
    aviso_num = ""
    if r.get("prox") and str(r["numero"]) != str(r["prox"]):
        aviso_num = (f"<div class='warn'>Número devolvido ({r['numero']}) ≠ esperado "
                     f"({r['prox']}). Confira a numeração.</div>")
    sub_box = ""
    sub = r.get("sub")
    if sub:
        sub_box = (f"<div class='card'><b>🔁 Substituição da NF {html.escape(str(sub['numero_antigo']))}</b>"
                   f"<div class='lbl'>No card: {html.escape(str(sub.get('card','') or '—'))}</div>"
                   f"<div class='lbl'>Na planilha: {html.escape(str(sub.get('planilha','') or '—'))}</div>"
                   "<p class='sub'>Obs.: o cancelamento fiscal junto à prefeitura (ABRASF), se necessário, "
                   "é um passo separado e não é feito automaticamente.</p></div>")
    return _doc("NFS-e emitida", f"""
      <h1>NFS-e emitida</h1>
      <div class='ok'>✅ Nota <b>{html.escape(str(r['numero']))}</b> emitida — código
        <b>{html.escape(str(r['codigo']))}</b> — emissão {html.escape(str(r['data']))}.</div>
      {aviso_num}
      {sub_box}
      <div class='card'><b>Log do pós-emissão</b><pre>{log}</pre></div>
      <p>Pode fechar esta aba. Os 4 documentos sobem no Drive e os links vão pra Descrição do card.</p>""")
