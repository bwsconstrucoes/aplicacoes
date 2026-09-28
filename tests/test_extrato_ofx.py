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


def test_fitid_repetido_com_o_MESMO_conteudo_continua_sendo_uma_linha_so():
    """FITID repetido E conteúdo igual: é a mesma transação duas vezes no
    arquivo. Isso não mudou — e não pode mudar, senão extrato já importado
    voltaria a entrar em duplicidade."""
    arquivo = _arquivo(_linha(fitid="ABC123"), _linha(fitid="ABC123"))
    assert len(parsear_ofx(arquivo, conta_bancaria_id=1)) == 1


# ---------------------------------------------------------------------------
# 28/09/2026 — O BANCO QUE USA O FITID COMO CÓDIGO DO TIPO
#
# O dono importou um extrato do banco 520 (SOMABWS) com 211 transações e a tela
# disse: "Li 4 lançamento(s)". O arquivo tinha QUATRO FITIDs distintos para 211
# transações:
#
#     FITID 3121 = "Liberação de folha"  → 110 linhas, valores diferentes
#     FITID 3029 = "Recebimento Pix"     →  95 linhas, valores diferentes
#     FITID 7101 →  4 linhas    FITID 3074 → 2 linhas
#
# Ou seja: ali o FITID é o código do TIPO da transação, não o identificador
# dela. O parser descartava 207 linhas como "repetidas", em silêncio.
# ---------------------------------------------------------------------------
def test_fitid_que_e_codigo_de_tipo_NAO_engole_o_extrato():
    """⚠️ O caso do banco 520. Mesmo FITID, valores e datas diferentes: são
    transações diferentes, e todas têm de entrar."""
    arquivo = _arquivo(
        _linha(data="20260925", valor="-1249.20", memo="Liberação de folha",
               fitid="3121"),
        _linha(data="20260924", valor="-2412.61", memo="Liberação de folha",
               fitid="3121"),
        _linha(data="20260924", valor="-2429.48", memo="Liberação de folha",
               fitid="3121"),
        _linha(data="20260924", valor="2412.61", memo="Recebimento Pix",
               fitid="3029"))
    lancs = parsear_ofx(arquivo, conta_bancaria_id=1)

    assert len(lancs) == 4, "as quatro são transações diferentes"
    assert len({l.hash_linha for l in lancs}) == 4
    from decimal import Decimal
    assert sum(l.valor for l in lancs) == Decimal("-3678.68")


def test_o_banco_de_fitid_unico_NAO_sente_diferenca():
    """⚠️ A GARANTIA QUE PROTEGE O QUE JÁ ESTÁ IMPORTADO. Se a identidade das
    linhas de um banco bem-comportado mudasse, todo extrato já gravado voltaria
    a entrar em duplicidade na próxima importação."""
    arquivo = _arquivo(_linha(fitid="AAA1"), _linha(valor="-90.00", fitid="AAA2"))
    lancs = parsear_ofx(arquivo, conta_bancaria_id=1)

    assert [l.identidade for l in lancs] == ["AAA1", "AAA2"], (
        "com FITID que identifica, a identidade continua sendo o FITID puro")


def test_fitid_de_tipo_continua_estavel_ao_reimportar():
    """Importar o mesmo arquivo de novo tem de reconhecer as mesmas linhas —
    senão o conserto trocaria 207 linhas perdidas por 207 duplicadas."""
    arquivo = _arquivo(
        _linha(data="20260925", valor="-1249.20", memo="Folha", fitid="3121"),
        _linha(data="20260924", valor="-2412.61", memo="Folha", fitid="3121"),
        _linha(data="20260923", valor="-500.00", memo="Folha", fitid="3121"))
    a = [l.hash_linha for l in parsear_ofx(arquivo, conta_bancaria_id=1)]
    b = [l.hash_linha for l in parsear_ofx(arquivo, conta_bancaria_id=1)]
    assert a == b
    assert len(set(a)) == 3


def test_extrato_maior_do_banco_de_tipo_reconhece_o_que_ja_entrou():
    """O caso real: ele importa setembro e, depois, setembro+outubro. As linhas
    de setembro têm de ser reconhecidas, não entrar de novo."""
    setembro = _arquivo(
        _linha(data="20260925", valor="-1249.20", memo="Folha", fitid="3121"),
        _linha(data="20260924", valor="-2412.61", memo="Folha", fitid="3121"))
    dois_meses = _arquivo(
        _linha(data="20260925", valor="-1249.20", memo="Folha", fitid="3121"),
        _linha(data="20260924", valor="-2412.61", memo="Folha", fitid="3121"),
        _linha(data="20261005", valor="-777.00", memo="Folha", fitid="3121"))

    ja = {l.hash_linha for l in parsear_ofx(setembro, conta_bancaria_id=1)}
    novos = [l for l in parsear_ofx(dois_meses, conta_bancaria_id=1)
             if l.hash_linha not in ja]
    assert len(novos) == 1
    assert novos[0].data.isoformat() == "2026-10-05"


def test_duas_transacoes_IDENTICAS_com_fitid_de_tipo_sao_duas():
    """Mesmo motivo dos dois PIX de R$ 1.500: se o FITID não identifica, duas
    linhas iguais de verdade continuam sendo duas."""
    arquivo = _arquivo(
        _linha(data="20260925", valor="-100.00", memo="Tarifa", fitid="7101"),
        _linha(data="20260925", valor="-100.00", memo="Tarifa", fitid="7101"),
        _linha(data="20260926", valor="-250.00", memo="Tarifa", fitid="7101"))
    lancs = parsear_ofx(arquivo, conta_bancaria_id=1)
    assert len(lancs) == 3
    assert len({l.hash_linha for l in lancs}) == 3


def test_contar_transacoes_denuncia_linha_perdida():
    """⚠️ A REDE DE PROTEÇÃO. O que custou caro não foi só perder 207 linhas: foi
    a tela dizer "li 4" sem nada avisando. Agora dá para comparar."""
    from app.apps.erp.core.pagamentos.ofx import contar_transacoes

    arquivo = _arquivo(*[_linha(valor=f"-{n}.00", fitid="3121")
                         for n in range(1, 6)])
    assert contar_transacoes(arquivo) == 5
    assert len(parsear_ofx(arquivo, conta_bancaria_id=1)) == 5
    # E num arquivo que não é OFX, contar não pode estourar.
    assert contar_transacoes(b"nada disso") == 0
