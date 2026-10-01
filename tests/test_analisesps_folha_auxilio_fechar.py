# -*- coding: utf-8 -*-
"""
Fechar o auxílio — 01/10/2026.

O botão "Conferir e gerar" da tela de alimentação e transporte levava a uma tela
que dizia "nada fechado": nenhuma tela fechava essas verbas, e o gerador só paga
verba fechada. Estes testes provam o caminho inteiro, com banco.
"""
import datetime as dt
from decimal import Decimal as D

import pytest

pytestmark = pytest.mark.banco

ATIVO, SAIU = "99713349334", "03513441363"


@pytest.fixture
def banco_auxilio(banco_analisesps):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                     " VALUES ('obra', 'CREPEOLINDA', '111')")
        conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                     " VALUES ('CREPEOLINDA', '50024')")
        for cpf, nome, fase, saida in [
                (ATIVO, "ATIVO UM", "Colaboradores Ativos", None),
                (SAIU, "SAIU UM", "Colaboradores Desligados", dt.date(2026, 8, 20))]:
            conn.execute(
                "INSERT INTO analisesps.colaborador "
                "  (cpf, nome, fase, data_saida, obra_codigo, valor_alimentacao, "
                "   modo_alimentacao) VALUES (?,?,?,?,?,?,?)",
                (cpf, nome, fase, saida, "CREPEOLINDA", D("300.00"), "Mês"))
        conn.commit()
    return banco_analisesps


def test_fechar_a_alimentacao_libera_o_arquivo(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx, folha_pagamento as fp
    feito = fx.fechar(fx.ALIMENTACAO, 2026, 9, "fim_de_mes", quem="MARCELO")
    assert feito["pessoas"] == 1 and feito["total"] == D("300.00")
    linhas = fp.linhas_para_pagar(2026, 9, "fim_de_mes", ["alimentacao"])
    assert [(l["cpf"], l["conta"], l["valor"]) for l in linhas] == [
        (ATIVO, "50024", D("300.00"))]
    assert fx.calcular(fx.ALIMENTACAO, 2026, 9)["fechamento"]["tipo"] == "fim_de_mes"


def test_quem_ja_SAIU_nao_entra_no_fechamento(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx
    calculado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    saiu = next(p for p in calculado["pessoas"] if p["cpf"] == SAIU)
    assert saiu["desligado"] and not saiu["pagar"]


def test_fechar_num_pagamento_TIRA_o_fechamento_do_outro(banco_auxilio):
    """Senão a mesma verba do mês sairia duas vezes."""
    from app.apps.analisesps import (folha_apropriacao_guardada as guardada,
                                     folha_auxilio as fx)
    fx.fechar(fx.ALIMENTACAO, 2026, 9, "quinzena")
    fx.fechar(fx.ALIMENTACAO, 2026, 9, "fim_de_mes")
    assert guardada.fechamento(2026, 9, "quinzena", "alimentacao") is None
    assert guardada.fechamento(2026, 9, "fim_de_mes", "alimentacao") is not None
