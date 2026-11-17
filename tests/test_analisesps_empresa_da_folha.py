"""A empresa de admissão (CNPJ da coluna BT) recorta as folhas (10/11/2026).

O dono: *"gerar a folha de diaristas, mas eu quero exibir somente os que estão
cadastrados para uma empresa e não para outra (…) antes de gerar qualquer
informação (…) alimentação e transporte, diaristas, despesa com colaboradores."*
"""
import json
from decimal import Decimal as D

import pytest

from app.apps.analisesps import colaboradores as col, empresa_da_folha as ef

A = "11222333000181"
B = "44555666000172"


def test_a_coluna_BT_entra_pelo_nome_e_pela_posicao():
    cab = ["CPF", "Nome Completo"] + [f"c{i}" for i in range(2, 71)] + ["CNPJ Admissão"]
    posicoes, _ = col._achar_colunas(cab)
    assert posicoes["pg_cnpj_admissao"] == 71
    cab2 = ["CPF", "Nome Completo"] + [f"c{i}" for i in range(2, 71)] + ["CNPJ da empresa"]
    posicoes, _ = col._achar_colunas(cab2)
    assert posicoes["pg_cnpj_admissao"] == 71, "sem o nome, a BT — conferindo a palavra"


def test_a_coluna_BT_como_a_planilha_escreve():
    """Dono, 10/11/2026: a BT vem "00.079.526/0001-09 - BWS"."""
    assert ef.ler_coluna("00.079.526/0001-09 - BWS") == ("00079526000109", "BWS")
    assert ef.ler_coluna("50.422.792/0001-09 - INFRADENDE") == ("50422792000109", "INFRADENDE")
    assert ef.ler_coluna(" 38.240.209/0001-03  -  LC ") == ("38240209000103", "LC")
    assert ef.ler_coluna("04895340000189") == ("04895340000189", "")
    assert ef.ler_coluna("BWS") == ("", "")
    assert ef.normalizar("00.079.526/0001-09 - BWS") == "00079526000109"


@pytest.mark.banco
def test_o_nome_do_filtro_e_o_escrito_na_coluna(banco_diaristas):
    ef._nomes_da_coluna.clear()
    _cnpj(DIARISTA, "00.079.526/0001-09 - BWS")
    opcoes = dict(fd.calcular(2026, 9, "quinzena")["empresas"])
    assert opcoes["00079526000109"] == "00.079.526/0001-09 - BWS · 1 pessoa"
    assert [p["cpf"] for p in fd.calcular(2026, 9, "quinzena", "00079526000109")["pessoas"]] == [DIARISTA]


def test_recortar_e_as_opcoes(monkeypatch):
    monkeypatch.setattr(ef, "nomes_das_empresas", lambda: {A: "BWS"})
    pessoas = [{"cpf": "1", "cnpj_admissao": A}, {"cpf": "2", "cnpj_admissao": B},
               {"cpf": "3", "cnpj_admissao": ""}]
    assert [p["cpf"] for p in ef.recortar(pessoas, "11.222.333/0001-81")] == ["1"]
    assert [p["cpf"] for p in ef.recortar(pessoas, ef.SEM_CNPJ)] == ["3"]
    assert len(ef.recortar(pessoas, "")) == 3, "sem empresa, todas"
    assert len(ef.recortar(pessoas, "123")) == 3, "CNPJ torto vale como 'todas'"
    opcoes = dict(ef.opcoes(pessoas))
    assert opcoes[A] == "11.222.333/0001-81 - BWS · 1 pessoa"
    assert opcoes[B] == "44.555.666/0001-72 · 1 pessoa"
    assert list(opcoes)[-1] == ef.SEM_CNPJ


# ---------------------------------------------------------------------------
# Com banco: os diaristas recortados, e a geração só da empresa escolhida
# ---------------------------------------------------------------------------
from tests.test_analisesps_folha_diaristas import (DIARISTA, _drive_falso,  # noqa: E402
                                                    banco_diaristas, fd)
from tests._feriados_do_ponto import _schema_ponto2, feriados_ponto  # noqa: E402,F401


def _cnpj(cpf, cnpj):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("UPDATE analisesps.colaborador SET documentos = ? WHERE cpf = ?",
                     (json.dumps({"cnpj_admissao": cnpj}), cpf))
        conn.commit()


@pytest.mark.banco
def test_os_diaristas_saem_so_da_empresa_escolhida(banco_diaristas):
    _cnpj(DIARISTA, "11.222.333/0001-81")
    todas = fd.calcular(2026, 9, "quinzena")
    assert todas["total"] == D("330.00")
    assert A in dict(todas["empresas"]) and ef.SEM_CNPJ in dict(todas["empresas"])
    da_a = fd.calcular(2026, 9, "quinzena", A)
    assert [p["cpf"] for p in da_a["pessoas"]] == [DIARISTA] and da_a["total"] == D("330.00")
    da_b = fd.calcular(2026, 9, "quinzena", B)
    assert da_b["pessoas"] == [] and da_b["total"] == D("0.00")
    assert dict(da_b["empresas"]) == dict(todas["empresas"]), "as opções são de todos"
    sem = fd.calcular(2026, 9, "quinzena", ef.SEM_CNPJ)
    assert DIARISTA not in [p["cpf"] for p in sem["pessoas"]]


@pytest.mark.banco
def test_gerar_POR_EMPRESA_tem_fechamento_proprio_e_nao_mistura_com_JUNTAS(banco_diaristas,
                                                                         monkeypatch):
    """Dono, 10/11/2026: *"devemos fechar por empresa"* e *"quero poder fazer junto
    ou por empresa"*."""
    from app.apps.analisesps import folha_pagamento as fp
    _drive_falso(monkeypatch)
    _cnpj(DIARISTA, A)
    with pytest.raises(fp.ErroDoPagamento):
        fp.gerar_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena",
                                   "empresa": B}, "somapay")      # ninguém da B
    fp.gerar_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena",
                               "empresa": A}, "somapay", quem="MARCELO")
    verba_a = f"diaria.{A}"
    assert {l["cpf"] for l in fp.linhas_para_pagar(2026, 9, "quinzena", [verba_a])} == {DIARISTA}
    assert fd.calcular(2026, 9, "quinzena", A)["fechamento"]["total_apropriado"] == D("330.00")
    assert fd.calcular(2026, 9, "quinzena")["fechamento"] is None, "o de todas é outro"
    # A mesma empresa de novo: a regra de sempre (recusa; "gerar mesmo assim" passa).
    with pytest.raises(fp.ErroDoPagamento) as erro:
        fp.gerar_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena",
                                   "empresa": A}, "somapay")
    assert "já foi gerado" in str(erro.value)
    # Todas juntas depois de uma empresa: nem o "gerar mesmo assim" passa.
    with pytest.raises(fp.ErroDoPagamento) as erro:
        fp.gerar_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena"},
                        "somapay", forcar=True)
    assert "não se misturam" in str(erro.value)


def test_junto_e_por_empresa_nao_se_misturam_mas_empresas_diferentes_sim():
    assert ef.conflita(f"diaria.{A}", f"diaria.{B}") == ""
    assert ef.conflita(f"diaria.{A}", f"diaria.{A}") == "mesma"
    assert ef.conflita("diaria", f"diaria.{A}") == "mistura"
    assert ef.conflita(f"diaria.{A}", "diaria") == "mistura"
    assert ef.conflita(f"diaria.{A}", "alimentacao") == ""
    assert ef.verba_da_empresa("diaria", "") == "diaria"
    assert ef.verba_da_empresa("alimentacao", "11.222.333/0001-81") == f"alimentacao.{A}"


def test_a_verba_da_empresa_tem_o_rotulo_a_natureza_e_o_grupo_da_base(monkeypatch):
    from app.apps.analisesps import folha_cards as fcd, folha_geracao as g
    monkeypatch.setattr(ef, "nomes_das_empresas", lambda: {A: "BWS"})
    assert g.rotulo_da_verba(f"diaria.{A}") == "Diárias — BWS"
    assert g.rotulo_da_verba(f"alimentacao.{ef.SEM_CNPJ}") == "Alimentação — sem CNPJ de admissão"
    assert g.natureza(f"transporte.{A}") == g.natureza("transporte")
    assert g.carteira_da_verba(f"diaria.{A}") == "Diárias"
    grupo = fcd.grupo_da_verba(f"diaria.{A}", "quinzena")
    assert grupo[0] == "Pagamento de Diárias - BWS"
    assert grupo[1:] == fcd.grupo_da_verba("diaria", "quinzena")[1:]
    assert fcd.DESCRICAO_DA_VERBA[fcd.verba_do_grupo(f"alimentacao.{B}")] == "Despesas com Alimentação"


from tests.test_analisesps_dc import ATIVO, OUTRO, banco_dc  # noqa: E402,F401


@pytest.mark.banco
def test_a_DC_sai_so_da_empresa_e_a_geracao_leva_so_ela(banco_dc):
    from app.apps.analisesps import dc
    _cnpj(ATIVO, A)
    _cnpj(OUTRO, B)
    da_a = dc.calcular(empresa=A)
    assert {p["cpf"] for p in da_a["pessoas"]} == {ATIVO}
    assert {l["cpf"] for l in dc.linhas_a_pagar(empresa=A)} <= {ATIVO}
    assert {p["cpf"] for p in dc.calcular(empresa=B)["pessoas"]} == {OUTRO}
    assert len(dc.calcular()["pessoas"]) > len(da_a["pessoas"])
