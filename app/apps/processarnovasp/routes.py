# -*- coding: utf-8 -*-
"""
processarnovasp/routes.py
POST /api/processarnovasp/executar
Body JSON: payload do webhook Pipefy + parametros Omie/Pipefy
Header ou body: campo "secret" para autenticacao
"""

import logging
from flask import Blueprint, request, jsonify
from .core import validar_payload, executar
from .payload_adapter import adaptar
from .utils import as_string

logger = logging.getLogger(__name__)
bp = Blueprint('processarnovasp', __name__)


@bp.route('/executar', methods=['POST'])
def rota_executar():
    raw_payload = request.get_json(silent=True) or {}
    # Adapta payload nested do Pipefy/Make → estrutura plana interna.
    # (Se já vier plano, o adapter detecta e devolve como está.)
    payload = adaptar(raw_payload)

    try:
        validar_payload(payload)
    except ValueError as e:
        return jsonify({
            'ok':       False,
            'erro':     str(e),
            'response': _html_erro(str(e)),
        }), 400

    try:
        resultado = executar(payload)
        resultado['ok']       = True
        resultado['response'] = _html_resultado(payload, resultado)
        return jsonify(resultado), 200

    except Exception as e:
        logger.exception('Erro ao executar processarnovasp')
        msg = str(e)
        return jsonify({
            'ok':       False,
            'erro':     msg,
            'response': _html_erro(msg),
        }), 500


# ---------------------------------------------------------------------------
#  HTML DE RESPOSTA
# ---------------------------------------------------------------------------

def _html_resultado(payload: dict, resultado: dict) -> str:
    sp_id  = as_string(payload.get('id'))
    secoes = resultado.get('secoes', {})
    rota   = as_string(resultado.get('rota'))  # transferencia | pagamento_futuro | padrao

    # --- Omie ---
    omie     = secoes.get('omie', {})
    titulo   = omie.get('titulo', {}) if isinstance(omie, dict) else {}
    op_omie  = as_string(titulo.get('operacao', ''))
    ok_omie  = bool(titulo.get('ok'))
    cod_lanc = as_string(titulo.get('codigo_lancamento_integracao', ''))

    if rota == 'transferencia':
        heading      = 'Transferência de Recursos'
        icone        = '↔️'
        titulo_linha = f"SP {sp_id} registrada como transferência interna (não gera título Omie)."
        detalhe      = ''
    elif rota == 'pagamento_futuro':
        heading      = 'Pagamento Futuro / Antecipação'
        icone        = '⏳'
        titulo_linha = f"SP {sp_id} registrada como pagamento futuro de pedido (Omie pendente)."
        detalhe      = ''
    elif ok_omie:
        heading      = 'Inclusão de Título'
        icone        = '✅'
        titulo_linha = f"O Título à Pagar referente à SP {sp_id} foi <strong>registrado</strong> no Omie."
        detalhe      = f"Código de lançamento: <strong>{cod_lanc}</strong>"
    elif omie.get('duplicado'):
        heading      = 'Título já existente'
        icone        = '⚠️'
        titulo_linha = f"SP {sp_id} — título já cadastrado no Omie. Linha registrada em SPsBD."
        detalhe      = ''
    elif omie.get('falha'):
        heading      = 'Falha no Omie'
        icone        = '❌'
        titulo_linha = f"SP {sp_id} — falha ao registrar no Omie (enviado para FalhaProcessar)."
        detalhe      = as_string(titulo.get('mensagem', ''))
    else:
        heading      = 'Processamento'
        icone        = 'ℹ️'
        titulo_linha = f"SP {sp_id} processada."
        detalhe      = ''

    # SPsBD info
    spsbd_sec  = secoes.get('spsbd', {})
    if spsbd_sec.get('ok'):
        linha_bd = "✅ SP registrada no banco de dados interno."
    else:
        linha_bd = "⚠️ Houve um problema ao registrar no banco de dados interno."

    # Boleto info
    boleto_sec  = secoes.get('boleto', {})
    linha_boleto = ""
    if boleto_sec.get('executado'):
        if boleto_sec.get('duplicado'):
            linha_boleto = f"⚠️ Código de barras já lançado na SP {boleto_sec.get('sp_duplicada')} — card de cancelamento criado."
        elif boleto_sec.get('valido'):
            linha_boleto = "✅ Código de boleto validado e registrado na fila DDA."
        else:
            linha_boleto = "⚠️ Código de boleto inválido ou não informado."

    # Pedido info
    pedido_sec = secoes.get('pedido', {})
    linha_pedido = ""
    if pedido_sec.get('executado'):
        qtd = pedido_sec.get('pedidos_atualizados', 0)
        if qtd:
            linha_pedido = f"🔗 Vinculado a {qtd} pedido(s) de compra."

    extras = ""
    for l in (linha_boleto, linha_pedido, linha_bd):
        if l:
            extras += f"""
                <p style="text-align:center"><span style="font-size:13px;font-family:Verdana;color:#999">{l}</span></p>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>{heading}</title>
    <link rel="icon" type="image/x-icon" href="https://dl.dropboxusercontent.com/s/xzsjhm9xudwqf8o/favicon.ico">
</head>
<body>
<table align="center" border="0" cellpadding="1" cellspacing="1" style="width:420px">
    <tbody>
        <tr><td>
            <a href="https://www.bwsconstrucoes.com.br">
                <img alt="" src="https://www.dropbox.com/scl/fi/0z1uetm8zcujmaep9z9t1/Logo-BWS-M.jpg?rlkey=9nnqdkftijf5ls6kjsjt8ccjn&dl=1"
                    style="display:block;height:62px;margin:auto;width:168px" />
            </a>
        </td></tr>
        <tr><td>
            <h1 style="text-align:center">
                <span style="font-family:Verdana;font-size:22px;color:#2c3e50"><strong>{icone} {heading}</strong></span>
            </h1>
            <p style="text-align:center"><span style="font-size:14px;font-family:Verdana;color:#555">{titulo_linha}</span></p>
            {f'<p style="text-align:center"><span style="font-size:13px;font-family:Verdana;color:#999">{detalhe}</span></p>' if detalhe else ''}
            {extras}
            <p style="text-align:center">&nbsp;</p>
            <p style="text-align:center">
                <span style="font-size:16px">
                    <a href="http://portal.pipefy.com/bwsconstrucoes" style="color:#3498db">Acessar Portal de Solicitações</a>
                </span>
            </p>
        </td></tr>
        <tr><td style="text-align:center">
            😉 <span style="font-size:11px;font-family:Verdana;color:#999">
                Qualquer dúvida entre em contato com o setor responsável.
            </span>
        </td></tr>
    </tbody>
</table>
</body>
</html>"""


def _html_erro(mensagem: str) -> str:
    return f"""<!DOCTYPE html>
<html><head><title>Erro</title></head>
<body>
<table align="center" border="0" cellpadding="1" cellspacing="1" style="width:400px">
<tbody>
<tr><td>
    <a href="https://www.bwsconstrucoes.com.br">
        <img alt="" src="https://www.dropbox.com/scl/fi/0z1uetm8zcujmaep9z9t1/Logo-BWS-M.jpg?rlkey=9nnqdkftijf5ls6kjsjt8ccjn&dl=1"
            style="display:block;height:62px;margin:auto;width:168px" />
    </a>
</td></tr>
<tr><td>
    <h1 style="text-align:center"><span style="font-family:Verdana;font-size:22px;color:#e74c3c"><strong>❌ Erro no Processamento</strong></span></h1>
    <p style="text-align:center"><span style="font-size:13px;font-family:Verdana;color:#999">{mensagem}</span></p>
    <p style="text-align:center">&nbsp;</p>
    <p style="text-align:center">
        <span style="font-size:16px"><a href="http://portal.pipefy.com/bwsconstrucoes" style="color:#3498db">Acessar Portal de Solicitações</a></span>
    </p>
</td></tr>
</tbody></table>
</body></html>"""
