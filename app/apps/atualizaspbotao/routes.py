# -*- coding: utf-8 -*-
"""
atualizaspbotao/routes.py
POST /api/atualizaspbotao/executar
Body JSON: payload completo (mesmo formato enviado ao Apps Script atual)
Header ou body: campo "secret" para autenticação
"""

import logging
from flask import Blueprint, request, make_response
from .core import validar_payload, executar
from .utils import as_string

logger = logging.getLogger(__name__)
bp = Blueprint('atualizaspbotao', __name__)


@bp.route('/executar', methods=['POST'])
def rota_executar():
    payload = request.get_json(silent=True) or {}

    try:
        validar_payload(payload)
    except ValueError as e:
        return make_response(_html_erro(str(e)), 400)

    try:
        resultado = executar(payload)
        html = _html_resultado(payload, resultado)
        return make_response(html, 200)

    except Exception as e:
        logger.exception('Erro ao executar atualizaspbotao')
        return make_response(_html_erro(str(e)), 500)


# ---------------------------------------------------------------------------
#  GERAÇÃO DO HTML DE RESPOSTA
# ---------------------------------------------------------------------------

def _html_resultado(payload: dict, resultado: dict) -> str:
    sp_id  = as_string(payload.get('id'))
    secoes = resultado.get('secoes', {})

    # --- Título Omie ---
    omie      = secoes.get('omie', {})
    titulo    = omie.get('titulo', {})
    op_omie   = as_string(titulo.get('operacao', ''))
    ok_omie   = bool(titulo.get('ok'))
    cod_lanc  = as_string(titulo.get('codigo_lancamento_integracao', ''))
    msg_omie  = as_string(titulo.get('mensagem', ''))

    if ok_omie:
        if 'alterar' in op_omie:
            titulo_linha = f"O Título à Pagar referente à SP {sp_id} foi <strong>atualizado</strong> no Omie."
            icone_omie   = "✅"
        else:
            titulo_linha = f"O Título à Pagar referente à SP {sp_id} foi <strong>registrado</strong> no Omie."
            icone_omie   = "✅"
        detalhe_omie = f"Código de lançamento: <strong>{cod_lanc}</strong>"
    elif omie.get('ignorado'):
        titulo_linha = f"SP {sp_id} — integração com o Omie não executada."
        icone_omie   = "⚠️"
        detalhe_omie = as_string(omie.get('motivo', ''))
    else:
        titulo_linha = f"SP {sp_id} — ocorreu um problema ao registrar o título no Omie."
        icone_omie   = "❌"
        detalhe_omie = msg_omie or "Verifique os parâmetros e tente novamente."

    # --- Bases de dados ---
    spsbd_sec  = secoes.get('atualizaLogeSPsBD', {})
    ok_spsbd   = bool(spsbd_sec.get('ok'))
    modo_spsbd = as_string(spsbd_sec.get('spsbd', {}).get('modo', ''))
    modo_log   = as_string(spsbd_sec.get('log', {}).get('modo', ''))

    if ok_spsbd:
        acao_spsbd = "atualizado" if modo_spsbd == 'update' else "registrado"
        acao_log   = "atualizado" if modo_log == 'update' else "registrado"
        linha_bd   = f"✅ Banco de dados {acao_spsbd} e log {acao_log} com sucesso."
    else:
        linha_bd   = "⚠️ Houve um problema ao atualizar o banco de dados interno."

    # --- Boleto ---
    boleto_sec  = secoes.get('validacaoBoletoDDA', {})
    linha_boleto = ""
    if boleto_sec.get('executado'):
        if boleto_sec.get('valido'):
            linha_boleto = "✅ Código de boleto validado e registrado na fila DDA."
        else:
            linha_boleto = "⚠️ Código de boleto inválido ou não informado."

    # --- Heading da página ---
    if ok_omie:
        heading = "Inclusão de Título" if 'incluir' in op_omie else "Atualização de Título"
    else:
        heading = "Registro de Solicitação"

    # --- Monta o HTML ---
    linhas_extras = ""
    if linha_boleto:
        linhas_extras += f"""
            <p style="text-align:center">
                <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                    {linha_boleto}
                </span>
            </p>"""

    linhas_extras += f"""
            <p style="text-align:center">
                <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                    {linha_bd}
                </span>
            </p>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>{heading}</title>
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
                        <strong>{icone_omie} {heading}</strong>
                    </span>
                </h1>

                <p style="text-align:center">
                    <span style="font-size:14px; font-family:Verdana,Geneva,sans-serif; color:#555555">
                        {titulo_linha}
                    </span>
                </p>

                <p style="text-align:center">
                    <span style="font-size:13px; font-family:Verdana,Geneva,sans-serif; color:#999999">
                        {detalhe_omie}
                    </span>
                </p>

                {linhas_extras}

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
                <p style="text-align:center">&nbsp;</p>
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