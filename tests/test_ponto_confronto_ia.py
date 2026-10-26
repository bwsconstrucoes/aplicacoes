# -*- coding: utf-8 -*-
"""Ponto — o que a IA leu no atestado × o que foi lançado (26/10/2026): o dono, "cruzar
as datas, a quantidade de dias, o nome da pessoa — ver o que bate e dar um confronto"."""
from app.apps.ponto.core.leitura_atestado import confronto, nomes_batem

PEDIDO = {"colaborador": "JOSÉ RIBAMAR SOUSA", "data_inicio": "2026-10-20", "data_fim": "2026-10-22",
          "cid": "J11", "medico": None, "crm": None}


def _por_campo(c):
    return {i["campo"]: i["situacao"] for i in c["itens"]}


def test_nomes():
    assert nomes_batem("José R. Sousa", "JOSÉ RIBAMAR SOUSA")
    assert nomes_batem("JOSE RIBAMAR", "José Ribamar Sousa")
    assert not nomes_batem("Maria Ribamar Sousa", "JOSÉ RIBAMAR SOUSA")
    assert not nomes_batem("", "JOSÉ") and not nomes_batem("José", None)


def test_tudo_bate():
    c = confronto({"tipo": "ATESTADO", "paciente": "José Ribamar Sousa", "data_inicio": "2026-10-20",
                   "dias_afastamento": 3, "cid": "j11", "legivel": True, "data_emissao": "2026-10-20"}, PEDIDO)
    assert c["nao_batem"] == 0 and c["avisos"] == [] and c["resumo"].startswith("tudo")
    assert _por_campo(c)["Último dia"] == "BATE" and _por_campo(c)["CID"] == "BATE"


def test_o_que_nao_bate_e_os_avisos():
    c = confronto({"tipo": "ATESTADO", "paciente": "Maria das Dores", "data_inicio": "2026-10-21",
                   "dias_afastamento": "2", "crm": "CRM-CE 999", "legivel": False, "data_emissao": "2026-10-27"}, PEDIDO)
    s = _por_campo(c)
    assert s["Nome da pessoa"] == s["Primeiro dia"] == s["Dias de afastamento"] == "NAO_BATE"
    assert s["CRM"] == "SO_NO_DOCUMENTO" and c["nao_batem"] >= 3
    assert any("legível" in a for a in c["avisos"]) and any("para trás" in a for a in c["avisos"])


def test_declaracao_de_comparecimento_avisa():
    c = confronto({"tipo": "DECLARACAO_DE_COMPARECIMENTO", "paciente": "José Sousa", "data_emissao": "2026-10-20",
                   "horario_inicio": "08:00", "horario_fim": "10:00"}, PEDIDO)
    assert any("COMPARECIMENTO" in a and "08:00" in a for a in c["avisos"])
    assert confronto({}, PEDIDO)["itens"] == []
