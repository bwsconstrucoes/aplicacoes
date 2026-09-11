"""A identidade de cada linha do extrato importado.

A falha encontrada na varredura de 11/09/2026: bancos que não mandam FITID (o
número que identifica a transação) faziam o ERP identificar a linha por
data + valor + histórico. DOIS pagamentos de verdade, iguais no mesmo dia para
o mesmo favorecido — dois PIX de R$ 1.500 para a mesma construtora — viravam
UM só. O segundo era descartado como "duplicado", o extrato passava a divergir
do banco, e a tela dizia "1 duplicada" com ar de tudo certo.

Pior: é exatamente o caso que a conciliação foi feita para resolver (dois
pagamentos iguais e dois lançamentos iguais, casados pelo par completo). Ele
nunca chegava lá, porque a segunda linha era perdida na importação.

Sem banco: o parser é função pura.
"""
from __future__ import annotations

from app.apps.erp.core.pagamentos.ofx import parsear_ofx

CABECALHO = b"""OFXHEADER:100
<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><BANKTRANLIST>"""
RODAPE = b"</BANKTRANLIST></STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>"


def _linha(data="20260910", valor="-1500.00", memo="PIX DES: CONSTRUTORA ALFA",
           fitid=None):
    id_ = f"<FITID>{fitid}".encode() if fitid else b""
    return (b"<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>" + data.encode()
            + b"<TRNAMT>" + valor.encode() + id_
            + b"<MEMO>" + memo.encode() + b"</STMTTRN>")


def _arquivo(*linhas):
    return CABECALHO + b"".join(linhas) + RODAPE


def test_dois_pagamentos_iguais_no_mesmo_dia_sao_duas_linhas():
    """Sem FITID, duas saídas idênticas continuam sendo duas."""
    lancs = parsear_ofx(_arquivo(_linha(), _linha()), conta_bancaria_id=1)
    assert len(lancs) == 2
    assert lancs[0].hash_linha != lancs[1].hash_linha


def test_reimportar_o_mesmo_arquivo_nao_duplica():
    """A identidade é estável: importar de novo reconhece as mesmas linhas."""
    arquivo = _arquivo(_linha(), _linha(), _linha(valor="-300.00", memo="TARIFA"))
    primeira = [l.hash_linha for l in parsear_ofx(arquivo, conta_bancaria_id=1)]
    segunda = [l.hash_linha for l in parsear_ofx(arquivo, conta_bancaria_id=1)]
    assert primeira == segunda
    assert len(set(primeira)) == 3


def test_extrato_que_cobre_periodo_maior_reconhece_o_que_ja_entrou():
    """Extrato de dois meses contendo o mês já importado: as linhas repetidas
    do período antigo têm de ser reconhecidas, não entrar de novo."""
    mes = _arquivo(_linha(), _linha())
    dois_meses = _arquivo(_linha(), _linha(),
                          _linha(data="20261015", valor="-800.00", memo="PIX BETA"))
    ja = {l.hash_linha for l in parsear_ofx(mes, conta_bancaria_id=1)}
    novos = [l for l in parsear_ofx(dois_meses, conta_bancaria_id=1)
             if l.hash_linha not in ja]
    assert len(novos) == 1
    assert novos[0].valor == __import__("decimal").Decimal("-800.00")


def test_a_mesma_linha_em_contas_diferentes_nao_se_confunde():
    """O número da conta entra na identidade — extratos de dois bancos com a
    mesma tarifa no mesmo dia são lançamentos diferentes."""
    arquivo = _arquivo(_linha(valor="-30.00", memo="TARIFA CESTA"))
    a = parsear_ofx(arquivo, conta_bancaria_id=1)[0].hash_linha
    b = parsear_ofx(arquivo, conta_bancaria_id=2)[0].hash_linha
    assert a != b


def test_fitid_repetido_continua_sendo_uma_linha_so():
    """Quando o banco MANDA o identificador, ele manda a verdade: FITID
    repetido é a mesma transação aparecendo duas vezes no arquivo."""
    arquivo = _arquivo(_linha(fitid="ABC123"), _linha(fitid="ABC123"))
    assert len(parsear_ofx(arquivo, conta_bancaria_id=1)) == 1
