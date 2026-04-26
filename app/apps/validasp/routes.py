# -*- coding: utf-8 -*-
"""validasp/routes.py — POST /api/validasp/executar"""

import logging
import base64
from flask import Blueprint, request, jsonify
from .core import validar_payload, executar, CONFIG
from ..atualizaspbotao.utils import as_string

logger = logging.getLogger(__name__)
bp = Blueprint('validasp', __name__)


@bp.route('/executar', methods=['POST'])
def rota_executar():
    payload = request.get_json(silent=True) or {}

    try:
        validar_payload(payload)
    except ValueError as e:
        return jsonify({'ok': False, 'erro': str(e), 'response': _html_erro(str(e))}), 400

    try:
        resultado = executar(payload)
        resultado['ok']       = True
        resultado['response'] = _html_resultado(payload, resultado)
        return jsonify(resultado), 200

    except Exception as e:
        logger.exception('Erro ao executar validasp')
        msg = str(e)
        return jsonify({'ok': False, 'erro': msg, 'response': _html_erro(msg)}), 500


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _decodificar_base64(valor: str) -> str:
    """Tenta decodificar base64, retorna o valor original se não for base64."""
    try:
        decoded = base64.b64decode(valor).decode('utf-8')
        # Só aceita se o resultado for texto legível (sem caracteres de controle)
        if decoded.isprintable():
            return decoded
    except Exception:
        pass
    return valor


def _campo(payload: dict, campo: str) -> str:
    """Busca campo no payload ou no bloco anuencia."""
    return as_string(
        payload.get(campo) or
        (payload.get('anuencia') or {}).get(campo, '')
    )


# ---------------------------------------------------------------------------
# HTML DE RESPOSTA
# ---------------------------------------------------------------------------

def _html_resultado(payload: dict, resultado: dict) -> str:
    sp_id = _campo(payload, 'id')
    data  = _campo(payload, 'datadasolicitacao')
    valor = _campo(payload, 'valortotaldadespesa')

    secoes = resultado.get('secoes', {})

    # Pipefy
    ok_pipefy = secoes.get('pipefy', {}).get('ok', False)
    linha_pip = "✅ Campo de validação atualizado no Pipefy." if ok_pipefy else "⚠️ Não foi possível atualizar o campo de validação no Pipefy."

    # Z-API responsável
    ok_resp    = secoes.get('zapiResponsavel', {}).get('ok', False)
    linha_resp = "✅ Notificação enviada ao responsável via WhatsApp." if ok_resp else "⚠️ Falha no envio da notificação ao responsável."

    # Z-API anuente — só exibe se não ignorado
    zapi_anu     = secoes.get('zapiAnuente', {})
    ignorado_anu = zapi_anu.get('ignorado', False)
    if not ignorado_anu:
        ok_anu    = zapi_anu.get('ok', False)
        linha_anu = "✅ Notificação de anuência enviada via WhatsApp." if ok_anu else "⚠️ Falha no envio da notificação de anuência."
    else:
        linha_anu = ""

    # SPsBD
    ok_spsbd    = secoes.get('spsbd', {}).get('ok', False)
    linha_spsbd = "✅ Base de dados em atualização." if ok_spsbd else "⚠️ Não foi possível atualizar a base de dados."

    # Monta linhas de status
    linhas_status = ""
    for linha in [linha_pip, linha_resp, linha_anu, linha_spsbd]:
        if linha:
            linhas_status += f"""
                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {linha}
                    </span>
                </p>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Solicitação de Pagamento Validada</title>
    <link rel="icon" type="image/x-icon" href="https://dl.dropboxusercontent.com/s/xzsjhm9xudwqf8o/favicon.ico">
</head>
<body>
<table align="center" border="0" cellpadding="1" cellspacing="1" style="height:auto; width:420px">
    <tbody>
        <tr>
            <td>
                <a href="https://www.bwsconstrucoes.com.br">
                    <img alt="" src="https://www.dropbox.com/scl/fi/0z1uetm8zcujmaep9z9t1/Logo-BWS-M.jpg?rlkey=9nnqdkftijf5ls6kjsjt8ccjn&dl=1"
                        style="display:block; height:62px; margin-left:auto; margin-right:auto; width:168px" />
                </a>
            </td>
        </tr>
        <tr>
            <td>
                <h1 style="text-align:center">
                    <span style="font-family:Verdana,Geneva,sans-serif; font-size:22px; color:#2c3e50">
                        <strong>✅ Solicitação de Pagamento Validada</strong>
                    </span>
                </h1>

                <p style="text-align:center">
                    <span style="font-size:14px; font-family:Verdana,Geneva,sans-serif; color:#2c3e50">
                        <strong>SP Nº {sp_id}</strong>
                    </span>
                </p>

                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#555555">
                        📅 Data: {data} &nbsp;|&nbsp; 💰 Valor: <strong>{valor}</strong>
                    </span>
                </p>

                <hr style="border:none; border-top:1px solid #eeeeee; margin:10px 0">

                {linhas_status}

                <p style="text-align:center">&nbsp;</p>
                <p style="text-align:center">
                    <span style="font-size:16px">
                        <a href="http://portal.pipefy.com/bwsconstrucoes">
                            <span style="color:#3498db">Acessar Portal de Solicitações</span>
                        </a>
                    </span>
                </p>
                <p style="text-align:center">&nbsp;</p>
            </td>
        </tr>
        <tr>
            <td style="text-align:center">
                😉 <span style="font-size:11px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                    Obrigado! Qualquer dúvida entre em contato com o setor responsável.
                </span>
            </td>
        </tr>
    </tbody>
</table>
<p>&nbsp;</p>
</body>
</html>"""


def _html_erro(mensagem: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head><title>Erro</title></head>
<body>
<table align="center" border="0" cellpadding="1" cellspacing="1" style="width:420px">
    <tbody>
        <tr>
            <td>
                <a href="https://www.bwsconstrucoes.com.br">
                    <img alt="" src="https://www.dropbox.com/scl/fi/0z1uetm8zcujmaep9z9t1/Logo-BWS-M.jpg?rlkey=9nnqdkftijf5ls6kjsjt8ccjn&dl=1"
                        style="display:block; height:62px; margin-left:auto; margin-right:auto; width:168px" />
                </a>
            </td>
        </tr>
        <tr>
            <td>
                <h1 style="text-align:center">
                    <span style="font-family:Verdana,Geneva,sans-serif; font-size:24px; color:#e74c3c">
                        <strong>❌ Erro no Processamento</strong>
                    </span>
                </h1>
                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {mensagem}
                    </span>
                </p>
                <p style="text-align:center">
                    <span style="font-size:16px">
                        <a href="http://portal.pipefy.com/bwsconstrucoes">
                            <span style="color:#3498db">Acessar Portal de Solicitações</span>
                        </a>
                    </span>
                </p>
            </td>
        </tr>
        <tr>
            <td style="text-align:center">
                😉 <span style="font-size:11px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                    Qualquer dúvida entre em contato com o setor responsável.
                </span>
            </td>
        </tr>
    </tbody>
</table>
</body>
</html>"""