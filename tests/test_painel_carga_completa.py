# -*- coding: utf-8 -*-
"""
A carga inicial INTEIRA, com um OMIE de mentira e um Postgres de verdade.

Este arquivo nasceu de um defeito bobo que chegou em produção depois de cinco
etapas já concluídas:

    name 'tot' is not defined

Ao acrescentar o andamento em `carregar_movimentos_full`, a linha nova citou
`tot`; a variável ali se chama `tot_mov`. Python só reclama de nome inexistente
quando a linha executa — e aquela linha só executa depois de baixar todos os
títulos, os catálogos e começar os movimentos. Horas de download para um erro
de digitação.

Os testes que havia exercitavam as funções de gravar uma a uma. **Nenhum rodava
`carga_inicial` de ponta a ponta**, que é onde moram os laços, os desvios de
retomada e todas as linhas de andamento.

Aqui um cliente de mentira devolve páginas no mesmo formato do OMIE, e a carga
roda inteira contra o Postgres de teste. Toda linha de `carga_inicial` executa —
inclusive as de progresso, que são justamente as que ninguém olha.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
# O OMIE de mentira
# ---------------------------------------------------------------------------
class OmieDeMentira:
    """Devolve páginas no mesmo formato do cliente de verdade:
    (página, total de páginas, total de registros, registros).

    Guarda o que foi pedido, para os testes de retomada poderem afirmar que
    uma etapa já concluída NÃO foi baixada de novo."""

    def __init__(self, paginas_de_titulos: int = 3, por_pagina: int = 2):
        self.paginas = paginas_de_titulos
        self.por_pagina = por_pagina
        self.pedidos: list[str] = []

    # -- títulos ------------------------------------------------------------
    def _titulos(self, natureza, inicio):
        total = self.paginas * self.por_pagina
        for pagina in range(1, self.paginas + 1):
            registros = []
            for i in range(self.por_pagina):
                codigo = inicio + (pagina - 1) * self.por_pagina + i
                registros.append({
                    "codigo_lancamento_omie": codigo,
                    "valor_documento": 100.0 * codigo,
                    "codigo_categoria": "1.01",
                    "codigo_cliente_fornecedor": 555,
                    "id_conta_corrente": 7,
                    "numero_documento": f"NF{codigo}",
                    "numero_parcela": "1/1",
                    "status_titulo": "RECEBIDO" if natureza == "R" else "PAGO",
                    "data_emissao": "01/03/2025",
                    "data_vencimento": "10/03/2025",
                    "valor_ir": 0, "valor_iss": 0, "valor_inss": 0,
                    "valor_pis": 0, "valor_cofins": 0, "valor_csll": 0,
                    "info": {"dInc": "01/03/2025", "dAlt": "05/03/2025",
                             "hAlt": "10:00:00", "cImpAPI": "N"},
                    "distribuicao": [{"cCodDep": "D1", "cDesDep": "Obra Um",
                                      "nPerDep": 100.0,
                                      "nValDep": 100.0 * codigo}],
                })
            yield pagina, self.paginas, total, registros

    def listar_contas_pagar(self):
        self.pedidos.append("contapagar")
        yield from self._titulos("P", 1000)

    def listar_contas_receber(self):
        self.pedidos.append("contareceber")
        yield from self._titulos("R", 2000)

    # -- catálogos ----------------------------------------------------------
    def listar_categorias(self):
        self.pedidos.append("categorias")
        yield 1, 1, 1, [{
            "codigo": "1.01", "descricao": "Receita de Obras",
            "categoria_superior": "1", "natureza": "R", "conta_inativa": "N",
            "codigo_dre": "3.01", "transferencia": "N", "totalizadora": "N",
            "dadosDRE": {"descricaoDRE": "Receita Bruta"},
        }]

    def listar_clientes(self):
        self.pedidos.append("clientes")
        yield 1, 1, 1, [{
            "codigo_cliente_omie": 555, "razao_social": "CLIENTE TAL LTDA",
            "nome_fantasia": "CLIENTE", "cnpj_cpf": "12.345.678/0001-90",
        }]

    def listar_contas_correntes(self):
        self.pedidos.append("contas_correntes")
        yield 1, 1, 1, [{
            "nCodCC": 7, "descricao": "Bradesco C/C 1234-5", "tipo_conta": "CC",
            "codigo_banco": "237", "agencia": "1234", "numero_conta": "5678",
            "inativo": "N",
        }]

    # -- movimentos ---------------------------------------------------------
    def listar_movimentos(self):
        self.pedidos.append("movimentos")
        # duas páginas: é o que faz a linha de andamento da última página rodar,
        # que é exatamente onde o defeito estava
        for pagina in (1, 2):
            registros = [{
                "detalhes": {
                    "nCodTitulo": 2000 + pagina, "cNatureza": "R",
                    "cGrupo": "CONTA_A_RECEBER", "cStatus": "RECEBIDO",
                    "cCodCateg": "1.01", "nCodCC": 7, "nCodCliente": 555,
                    "dDtPagamento": "15/03/2025", "dDtVenc": "10/03/2025",
                    "dDtEmissao": "01/03/2025", "dDtRegistro": "01/03/2025",
                    "nValorTitulo": 1000.0,
                },
                "resumo": {
                    "cLiquidado": "S", "nValPago": 1000.0, "nValLiquido": 1000.0,
                    "nValAberto": 0.0, "nJuros": 0.0, "nMulta": 0.0,
                    "nDesconto": 0.0,
                },
            }]
            yield pagina, 2, 2, registros


@pytest.fixture()
def carga_pronta(monkeypatch):
    """Banco de teste limpo e o OMIE trocado pelo de mentira."""
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    from app.apps.painel.sync import espelho

    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), f"migração falhou: {resultado}"

    tabelas = ("fato", "fato_recebimentos", "titulos", "rateio", "movimentos",
               "cat", "clientes", "contas_correntes", "depto_projeto",
               "sync_state", "execucoes")

    def _limpar():
        with painel_db.conexao() as conn:
            for tabela in tabelas:
                conn.execute(f"TRUNCATE TABLE {tabela}")
            conn.commit()

    _limpar()
    omie = OmieDeMentira()
    monkeypatch.setattr(espelho.OmieClient, "de_ambiente",
                        classmethod(lambda cls, *a, **k: omie))
    yield omie
    _limpar()
    painel_db._engine = None


# ---------------------------------------------------------------------------
# 1. A carga inteira roda
# ---------------------------------------------------------------------------
def test_a_carga_inicial_roda_de_ponta_a_ponta(carga_pronta):
    """O teste que faltava. Toda linha de `carga_inicial` executa — inclusive
    as de andamento, que foi onde o `name 'tot' is not defined` se escondeu por
    cinco etapas."""
    from app.apps.painel.db import consultar
    from app.apps.painel.sync import espelho

    espelho.carga_inicial()

    assert consultar("SELECT COUNT(*) FROM titulos")[0][0] == 12   # 6 pagar + 6 receber
    assert consultar("SELECT COUNT(*) FROM rateio")[0][0] == 12
    assert consultar("SELECT COUNT(*) FROM movimentos")[0][0] == 2
    assert consultar("SELECT COUNT(*) FROM cat")[0][0] == 1
    assert consultar("SELECT COUNT(*) FROM clientes")[0][0] == 1
    assert consultar("SELECT COUNT(*) FROM contas_correntes")[0][0] == 1

    # todas as etapas foram pedidas ao OMIE
    assert set(carga_pronta.pedidos) == {
        "contapagar", "contareceber", "categorias", "clientes",
        "contas_correntes", "movimentos"}


def test_o_andamento_e_avisado_durante_a_carga(carga_pronta):
    """As linhas de progresso são as que ninguém olha — e por isso as que
    guardam erro de digitação. Aqui elas são obrigadas a executar."""
    from app.apps.painel.sync import espelho

    avisos = []
    espelho.definir_progresso(lambda etapa, detalhe="": avisos.append((etapa, detalhe)))
    try:
        espelho.carga_inicial()
    finally:
        espelho.definir_progresso(None)

    etapas = " | ".join(e for e, _d in avisos)
    assert "contas a pagar" in etapas
    assert "contas a receber" in etapas
    assert "plano de contas" in etapas
    assert "clientes e fornecedores" in etapas
    assert "contas correntes" in etapas
    assert "pagamentos e recebimentos" in etapas

    # e o detalhe traz a posição, não só a etapa
    detalhes = " | ".join(d for _e, d in avisos)
    assert "etapa " in detalhes and " de 7" in detalhes
    assert "página" in detalhes
    assert "movimentos" in detalhes          # o trecho onde estava o defeito


def test_carga_concluida_apaga_as_marcas_de_etapa(carga_pronta):
    """Terminou inteira: as marcas não servem mais. Se ficassem, uma próxima
    "primeira carga" pularia tudo e não faria nada."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    espelho.carga_inicial()
    with conexao() as conn:
        assert espelho.etapas_concluidas(conn) == set()


# ---------------------------------------------------------------------------
# 2. A retomada não refaz o que já ficou pronto
# ---------------------------------------------------------------------------
def test_retomada_pula_as_etapas_ja_concluidas(carga_pronta):
    """A promessa que a tela faz ao dono: "5 de 7 etapas já estão prontas —
    elas não serão baixadas de novo". Aqui isso é verificado pedindo ao OMIE
    de mentira o que ele recebeu."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        for etapa in ("contapagar", "contareceber", "categorias",
                      "clientes", "contas_correntes"):
            espelho._marcar_etapa(conn, etapa)

    carga_pronta.pedidos.clear()
    espelho.carga_inicial()

    # só o que faltava foi baixado
    assert carga_pronta.pedidos == ["movimentos"]


def test_recomecar_do_zero_baixa_tudo_de_novo(carga_pronta):
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    espelho.carga_inicial()
    with conexao() as conn:
        espelho.limpar_etapas(conn)

    carga_pronta.pedidos.clear()
    espelho.carga_inicial()
    assert "contapagar" in carga_pronta.pedidos
    assert "movimentos" in carga_pronta.pedidos


# ---------------------------------------------------------------------------
# 3. Da carga até o número na tela
# ---------------------------------------------------------------------------
def test_depois_da_carga_as_telas_mostram_numero(carga_pronta):
    """O caminho completo: baixa do OMIE, monta o fato, e a consulta que a tela
    usa devolve valor. É o que o dono vê quando a carga termina."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho, fato

    espelho.carga_inicial()
    with conexao() as conn:
        linhas, _receb = fato.reconstruir(conn)

    assert linhas == 12
    assert consultas.base_vazia() is False

    resultado = consultas.resultado_dre(consultas.Filtros())
    # receber: 2001..2006 x 100 = 2.700.000 ; pagar: 1001..1006 x 100 = 1.200.900
    assert round(resultado["receita"], 2) > 0
    assert round(resultado["despesa"], 2) < 0
    assert consultas.opcoes_de_filtro()["obras"] == ["Obra Um"]
