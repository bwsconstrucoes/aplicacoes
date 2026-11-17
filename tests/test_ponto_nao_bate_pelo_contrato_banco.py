# -*- coding: utf-8 -*-
"""Ponto — QUEM NÃO BATE PONTO SAI DO TIPO DE CONTRATO DO CADASTRO.

⚑ POR QUE ISTO EXISTE, e é a diferença entre uma regra e um buraco. Até
16/11/2026 a regra morava SÓ no caminho da planilha (`registro.nao_bate_sql`,
lendo a coluna "tipo" do Registro de Colaboradores). Usando o cadastro do ERP
como base, `nao_bate` era `NULL`: **ninguém ficava de fora**.

Isso não incomodava enquanto a base era a planilha — mas o dono decidiu, no
mesmo dia, cortar o vínculo com ela. No dia em que a chave virasse, PJ e
pró-labore passariam a bater ponto, e ponto de PJ é exatamente o registro que
não se quer ter, porque parece vínculo de emprego. O defeito só apareceria
DEPOIS da virada, numa manhã, com batida de quem não devia bater.

A regra, ditada por ele em 16/11/2026:

    *"Quando é prestador de serviço, quem é que bate ponto? É o autônomo RPA e
    o autônomo mensalista. Os demais não batem ponto (…) PJ não bate ponto,
    pró-labore não bate ponto, estagiário bate ponto, jovem aprendiz bate
    ponto."*

Postgres de verdade: isto vive inteiro num `WHERE`/`CASE`, que o dublê da
suíte não enxerga.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import (CPF_JOAO, _schema_ponto2, app,  # noqa: F401
                                           como, mundo)

pytestmark = pytest.mark.banco


@pytest.fixture
def base_erp(mundo, banco):
    """O ponto lendo o CADASTRO DO ERP, que é para onde ele vai."""
    from app.apps.ponto import db
    from app.apps.ponto.core import parametros, registro as reg
    with db.conexao() as conn:
        parametros.gravar(conn, reg.PARAMETRO_FONTE, reg.FONTE_ERP, "teste")
    reg.esquecer()
    yield
    reg.esquecer()


def _com_contrato(banco, colaborador_id: int, tipo):
    with banco.connect() as conn:
        conn.execute(text("UPDATE colaboradores SET tipo_contrato = :t WHERE id = :i"),
                     {"t": tipo, "i": colaborador_id})
        conn.commit()


# A lista inteira dos nove tipos do cadastro, do jeito que ele ditou.
BATEM = ("CLT_DETERMINADO", "CLT_INDETERMINADO", "TEMPORARIO",
         "AUTONOMO_RPA", "AUTONOMO_MENSALISTA", "JOVEM_APRENDIZ", "ESTAGIO")
NAO_BATEM = ("PJ", "PRO_LABORE")


@pytest.mark.parametrize("tipo", BATEM)
def test_estes_tipos_de_contrato_BATEM_ponto(app, mundo, banco, base_erp, tipo):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    _com_contrato(banco, mundo["joao"], tipo)
    with db.conexao() as conn:
        assert not cadastros.colaborador_por_cpf(conn, CPF_JOAO)["nao_bate_motivo"]


@pytest.mark.parametrize("tipo", NAO_BATEM)
def test_PJ_e_PRO_LABORE_nao_batem_e_o_motivo_vem_escrito(app, mundo, banco, base_erp, tipo):
    """O motivo vai junto porque quem for barrado no portão precisa saber
    POR QUÊ — "recusado" sem motivo vira ligação para o escritório."""
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    _com_contrato(banco, mundo["joao"], tipo)
    with db.conexao() as conn:
        motivo = cadastros.colaborador_por_cpf(conn, CPF_JOAO)["nao_bate_motivo"]
    assert motivo and "não batem ponto" in motivo
    assert tipo in motivo


def test_sem_tipo_de_contrato_a_pessoa_BATE(app, mundo, banco, base_erp):
    """Cadastro pela metade não pode barrar ninguém: o erro caro aqui é gente
    parada no portão, não uma batida a mais que depois se confere."""
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    _com_contrato(banco, mundo["joao"], None)
    with db.conexao() as conn:
        assert not cadastros.colaborador_por_cpf(conn, CPF_JOAO)["nao_bate_motivo"]


def test_a_excecao_do_DP_passa_por_cima_do_tipo_de_contrato(app, mundo, banco, base_erp):
    """Migração 025, e ele foi claro na época: *"são exatamente as regras da
    planilha que eu quero flexibilizar."* A regra automática não tira a
    decisão de ninguém."""
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros
    _com_contrato(banco, mundo["joao"], "PJ")
    with db.conexao() as conn:
        if not db.tem_coluna(conn, "colaborador_config", "excecao_planilha"):
            pytest.skip("banco sem a coluna da exceção")
        conn.execute(text("""
            INSERT INTO ponto.colaborador_config (colaborador_id, excecao_planilha)
            VALUES (:i, TRUE)
            ON CONFLICT (colaborador_id) DO UPDATE SET excecao_planilha = TRUE"""),
            {"i": mundo["joao"]})
        conn.commit()
    with db.conexao() as conn:
        pessoa = cadastros.colaborador_por_cpf(conn, CPF_JOAO)
    # Bate por exceção…
    assert not pessoa["nao_bate_motivo"]
    # …e a janela dele continua mostrando o que a regra dizia.
    assert pessoa["nao_bate_planilha"]
