"""A complementar das diárias (11/11/2026): a diferença de uma quinzena já paga.

O dono: *"fizemos o pagamento (…) da última quinzena. Só que aí o pessoal precisou
corrigir ponto (…) a gente precisaria pagar a diferença (…) ver o que foi pago, ver
o que tem de diferente e poder gerar um segundo arquivo só da diferença do que a
gente selecionar."*
"""
import datetime as dt
from decimal import Decimal as D

import pytest

from app.apps.analisesps import diaristas_complementar as dcomp, folha_geracao as g
from tests.test_analisesps_folha_diaristas import (DIARISTA, _campos,  # noqa: F401
                                                    _drive_falso, banco_diaristas, fd)
from tests._feriados_do_ponto import _schema_ponto2, feriados_ponto  # noqa: F401
from tests.test_analisesps_telas import _como_mestre, app  # noqa: F401

QUINZENA = {"ano": 2026, "mes": 9, "periodo": "quinzena"}


def test_a_verba_da_complementar_das_diarias_e_da_familia_da_diaria():
    assert g.rotulo_da_verba("diaria_c1") == "Diárias complementar 1"
    assert g.natureza("diaria_c2") == g.natureza("diaria")
    assert g.carteira_da_verba("diaria_c1") == "Diárias"
    assert dcomp.e_da_diaria("diaria") and dcomp.e_da_diaria("diaria_c3.11222333000181")
    assert not dcomp.e_da_diaria("alimentacao")
    assert dcomp.proxima_verba([{"verba": "diaria"}, {"verba": "diaria_c1.1"}]) == "diaria_c2"
    from app.apps.analisesps import folha_cards as fcd
    assert fcd.grupo_da_verba("diaria_c1", "quinzena")[0] == "Pagamento de Diárias - Complementar 1"


def _mais_um_dia():
    """O ponto corrigido: um dia a mais de presença para o diarista."""
    from app.apps.analisesps.db import conexao, consultar
    carga = consultar("SELECT id FROM analisesps.ponto_carga WHERE ano=2026 AND mes=9")[0][0]
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.ponto_dia (carga_id, cpf, nome, data, campos) "
                     " VALUES (?,?,?,?,?)", (carga, DIARISTA, "X", dt.date(2026, 9, 10), _campos()))
        conn.commit()


@pytest.mark.banco
def test_sem_pagamento_nao_ha_complementar(banco_diaristas):
    assert dcomp.montar(2026, 9, "quinzena")["ja_paga"] is False


@pytest.mark.banco
def test_paga_a_quinzena_corrige_o_ponto_e_a_complementar_paga_SO_a_diferenca(
        banco_diaristas, monkeypatch, app):
    from app.apps.analisesps import folha_pagamento as fp
    _drive_falso(monkeypatch)
    fp.gerar_direto("diaria", dict(QUINZENA), "somapay", quem="MARCELO")
    antes = dcomp.montar(2026, 9, "quinzena")
    assert antes["ja_paga"] and antes["totais"]["quantos_a_receber"] == 0

    _mais_um_dia()
    montado = dcomp.montar(2026, 9, "quinzena")
    (l,) = montado["grupos"]["a_receber"]
    assert l["cpf"] == DIARISTA and l["antes"] == D("330.00")
    assert l["diferenca"] == D("100.00") and l["pagar"], "a receber vem marcado"
    assert montado["verba"] == "diaria_c1"
    assert [p["obra"] for p in l["obras_da_diferenca"]] == ["CREPEOLINDA"]

    # A tela abre, com a faixa e o arquivo só da diferença.
    html = _como_mestre(app).get(
        "/analisesps/folha/diaristas/complementar?ano=2026&mes=9&periodo=quinzena"
    ).get_data(as_text=True)
    assert "Diárias complementar 1" in html and "Esta quinzena já foi paga" in html

    fp.gerar_direto("diaria_complementar", dict(QUINZENA), "somapay", quem="MARCELO")
    linhas = fp.linhas_para_pagar(2026, 9, "quinzena", ["diaria_c1"])
    assert [(l_["cpf"], l_["valor"]) for l_ in linhas] == [(DIARISTA, D("100.00"))]
    depois = dcomp.montar(2026, 9, "quinzena")
    assert [x["cpf"] for x in depois["grupos"]["igual"]] == [DIARISTA], \
        "a segunda comparação soma a 1ª quinzena e a complementar"
    assert depois["verba"] == "diaria_c2"


@pytest.mark.banco
def test_quem_ficou_desmarcado_vem_em_NAO_PAGO_e_desmarcado(banco_diaristas, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    from app.apps.analisesps.db import conexao
    _drive_falso(monkeypatch)
    fp.gerar_direto("diaria", dict(QUINZENA), "somapay")
    # Alguém que não recebeu: o "sem valor" ganha valor da diária no cadastro.
    from tests.test_analisesps_folha_diaristas import SEM_VALOR
    with conexao() as conn:
        conn.execute("UPDATE analisesps.colaborador SET valor_diaria = 100 WHERE cpf = ?",
                     (SEM_VALOR,))
        conn.commit()
    montado = dcomp.montar(2026, 9, "quinzena")
    (l,) = montado["grupos"]["nao_pago"]
    assert l["cpf"] == SEM_VALOR and not l["pagar"] and l["pode_pagar"]
    dcomp.salvar_selecao(2026, 9, "quinzena", [{"cpf": SEM_VALOR, "pagar": True}])
    assert [x["cpf"] for x in dcomp.montar(2026, 9, "quinzena")["a_pagar"]] == [SEM_VALOR]


@pytest.mark.banco
def test_ponto_corrigido_para_MENOS_vai_para_a_descontar(banco_diaristas, monkeypatch):
    from app.apps.analisesps import folha_complementar as fcomp, folha_pagamento as fp
    from app.apps.analisesps.db import conexao
    _drive_falso(monkeypatch)
    fp.gerar_direto("diaria", dict(QUINZENA), "somapay")
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.ponto_dia WHERE cpf = ? AND data = ?",
                     (DIARISTA, dt.date(2026, 9, 5)))
        conn.commit()
    montado = dcomp.montar(2026, 9, "quinzena")
    (l,) = montado["grupos"]["pago_a_mais"]
    assert l["cpf"] == DIARISTA and l["diferenca"] < 0
    fcomp.guardar_a_descontar(montado)
    item = next(x for x in fcomp.a_descontar() if x["cpf"] == DIARISTA)
    assert item["competencia"].startswith("Diárias 09/2026")
    assert item["competencia_chave"] == "diaria-2026-09-quinzena"
