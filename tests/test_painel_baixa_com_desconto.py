"""A baixa com desconto não pode puxar o recebimento inteiro para a data dela.

12/11/2026, o dono: *"uma determinada obra recebeu o recurso em várias
parcelas. Esses recebimentos foram em 2020 e 2021. E ficou faltando 5.000
reais. Eu apliquei um desconto à medição e fiz a baixa, em 6 de junho de 2026.
O painel considerou como se todo recebimento tivesse sido em junho de 2026 (…)
e isso implicou diretamente nos juros, porque o sistema entendeu que a obra
ficou esse período todo negativa."*

O que o código fazia, lido nele (o dado real do título eu não vi): o OMIE
guarda a baixa em duas pernas — a consolidada, com o DESCONTO dentro do valor
quitado, e os créditos bancários, só com o dinheiro que entrou. Comparadas
cruas, as somas deixavam de fechar pelo valor do desconto, e a regra caía na
consolidada: uma linha só, na data da última baixa.

Estes testes não sobem banco: as pernas vêm como o `fato` as lê do espelho.
"""
import datetime as dt

from app.apps.painel.sync import fato


def _perna(data, valor, *, liquidado="", liquido=0.0, desconto=0.0, conta="1"):
    return {"data": data, "valor": valor, "liquido": liquido, "desconto": desconto,
            "liquidado": liquidado, "conta": conta, "juros": 0.0, "multa": 0.0,
            "aberto": 0.0, "status": "", "grupo": ""}


# quatro créditos no banco (2020 e 2021): 95.000 que entraram de verdade
CREDITOS = [_perna("10/03/2020", 40000.0), _perna("15/07/2020", 30000.0),
            _perna("20/01/2021", 15000.0), _perna("05/05/2021", 10000.0)]


def test_a_consolidada_com_o_desconto_dentro_nao_derruba_as_parcelas():
    """A consolidada diz 100.000 quitados em 06/06/2026, com 5.000 de desconto.
    O dinheiro foram 95.000 — e são os quatro créditos, cada um na data dele."""
    consolidada = _perna("06/06/2026", 100000.0, liquidado="S", liquido=95000.0,
                         desconto=5000.0)
    movs, origem = fato._escolher_recebimentos(CREDITOS + [consolidada], 95000.0)
    assert origem == fato.ORIGEM_CREDITO
    assert [(m["data"], m["valor"]) for m in movs] == [
        ("10/03/2020", 40000.0), ("15/07/2020", 30000.0),
        ("20/01/2021", 15000.0), ("05/05/2021", 10000.0)]


def test_as_parcelas_do_fato_ficam_cada_uma_na_data_dela():
    consolidada = _perna("06/06/2026", 100000.0, liquidado="S", liquido=95000.0,
                         desconto=5000.0)
    junho = dt.date(2026, 6, 6)
    parcelas = fato._parcelas_da_baixa(
        CREDITOS + [consolidada], 95000.0, 0.0, 0.0, {}, None, "CONTA",
        junho, 2026, 6, junho)
    assert [(round(valor, 2), ano, mes) for valor, _j, _m, _d, ano, mes, _p, _c in parcelas] == [
        (40000.0, 2020, 3), (30000.0, 2020, 7), (15000.0, 2021, 1), (10000.0, 2021, 5)]
    assert round(sum(p[0] for p in parcelas), 2) == 95000.0       # a soma não muda
    assert not any(ano == 2026 for _v, _j, _m, _d, ano, _mes, _p, _c in parcelas)


def test_cada_baixa_numa_perna_consolidada_e_a_do_desconto_nao_e_recebimento():
    """O outro jeito de o OMIE guardar: uma perna liquidada por baixa, e a do
    desconto com o desconto no valor. Ela não é dinheiro: não vira parcela."""
    pernas = [_perna("10/03/2020", 40000.0, liquidado="S"),
              _perna("15/07/2020", 55000.0, liquidado="S"),
              _perna("06/06/2026", 5000.0, liquidado="S", desconto=5000.0)]
    movs, origem = fato._escolher_recebimentos(pernas, 95000.0)
    assert origem == fato.ORIGEM_MOV
    assert [(m["data"], m["valor"]) for m in movs] == [
        ("10/03/2020", 40000.0), ("15/07/2020", 55000.0)]


def test_desconto_dentro_de_uma_baixa_com_dinheiro_sai_do_valor_dela():
    pernas = [_perna("10/03/2020", 40000.0, liquidado="S"),
              _perna("06/06/2026", 60000.0, liquidado="S", desconto=5000.0)]
    movs, _origem = fato._escolher_recebimentos(pernas, 95000.0)
    assert [(m["data"], m["valor"]) for m in movs] == [
        ("10/03/2020", 40000.0), ("06/06/2026", 55000.0)]
    assert pernas[1]["valor"] == 60000.0          # a perna original não é alterada


def test_o_que_ja_funcionava_continua_igual():
    # créditos que fecham com a consolidada, sem desconto: os créditos
    consolidada = _perna("05/05/2021", 95000.0, liquidado="S", liquido=95000.0)
    movs, origem = fato._escolher_recebimentos(CREDITOS + [consolidada], 95000.0)
    assert origem == fato.ORIGEM_CREDITO and len(movs) == 4
    # créditos que NÃO fecham com nada: fica a consolidada (uma linha a menos
    # de detalhe é melhor que um total errado)
    movs, origem = fato._escolher_recebimentos(CREDITOS[:2] + [consolidada], 95000.0)
    assert origem == fato.ORIGEM_MOV and [m["valor"] for m in movs] == [95000.0]
    # título 100% descontado: nada de dinheiro, e a regra antiga vale
    tudo_desconto = _perna("06/06/2026", 5000.0, liquidado="S", desconto=5000.0)
    movs, origem = fato._escolher_recebimentos([tudo_desconto], 0.0)
    assert origem == fato.ORIGEM_MOV and movs == [tudo_desconto]
    # nenhuma perna paga
    assert fato._escolher_recebimentos([_perna("", 0.0)], 0.0) == ([], None)
