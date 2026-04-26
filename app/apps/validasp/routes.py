# -*- coding: utf-8 -*-
"""validasp/routes.py — POST /api/validasp/executar"""

import logging
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
# HTML DE RESPOSTA
# ---------------------------------------------------------------------------

def _html_resultado(payload: dict, resultado: dict) -> str:
    sp_id = as_string(payload.get('id'))
    data  = as_string(payload.get('datadasolicitacao', ''))
    valor = as_string(payload.get('valortotaldadespesa', ''))

    secoes = resultado.get('secoes', {})

    # Pipefy
    ok_pipefy  = secoes.get('pipefy', {}).get('ok', False)
    linha_pip  = "✅ Campo de validação atualizado no Pipefy." if ok_pipefy else "⚠️ Não foi possível atualizar o campo de validação no Pipefy."

    # Link anuência
    link_sec   = secoes.get('linkAnuencia', {})
    short_url  = as_string(link_sec.get('short_url', ''))
    ok_link    = link_sec.get('ok', False)
    linha_link = f'✅ Link de anuência gerado: <a href="{short_url}" style="color:#3498db">{short_url}</a>' if ok_link else "⚠️ Link de anuência gerado (URL longa)."

    # Z-API
    zapi_sec   = secoes.get('zapi', {})
    ok_zapi    = zapi_sec.get('ok', False)
    linha_zapi = "✅ Notificações enviadas via WhatsApp." if ok_zapi else "⚠️ Falha no envio de uma ou mais notificações WhatsApp."

    # SPsBD
    spsbd_sec  = secoes.get('spsbd', {})
    ok_spsbd   = spsbd_sec.get('ok', False)
    linha_spsbd = "✅ Base de dados atualizada (SPsBD)." if ok_spsbd else "⚠️ Não foi possível atualizar a base de dados. Registro de falha criado."

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Solicitação de Pagamento Validada</title>
    <link rel="icon" type="image/x-icon" href="https://dl.dropboxusercontent.com/s/xzsjhm9xudwqf8o/favicon.ico">
</head>
<body>
<table align="center" border="0" cellpadding="1" cellspacing="1" style="height:auto; width:400px">
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
                    <span style="font-family:Verdana,Geneva,sans-serif; font-size:24px; color:#2c3e50">
                        <strong>✅ Solicitação de Pagamento Validada</strong>
                    </span>
                </h1>

                <p style="text-align:center">
                    <span style="font-size:14px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        Número: {sp_id} — Data: {data} — Valor: {valor}
                    </span>
                </p>

                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {linha_pip}
                    </span>
                </p>
                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {linha_zapi}
                    </span>
                </p>
                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {linha_spsbd}
                    </span>
                </p>
                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {linha_link}
                    </span>
                </p>

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
<table align="center" border="0" cellpadding="1" cellspacing="1" style="width:400px">
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
