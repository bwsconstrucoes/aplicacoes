"""O relatório somava o que a obra vai RECEBER com o que ela CUSTOU.

A FALHA, achada em 12/09/2026 ao revisar os relatórios a pedido do dono.

Títulos A RECEBER (medição de obra) moram na MESMA tabela dos títulos a pagar,
com rateio por obra igual — foi decisão de projeto, e é boa: parcelas,
conciliação e relatórios funcionam de graça. O que faltou foi o filtro: os
relatórios nunca olharam a espécie.

Resultado: "totais por obra" devolvia custo + receita num número positivo só.
Uma obra que gastou R$ 10.000 e vai receber R$ 50.000 aparecia com "R$ 60.000",
e quem lesse não teria como desconfiar — o número tem cara de certo.

O DRE sempre esteve certo, porque ele classifica pela conta do plano (grupo 1 é
receita) e não pela espécie. O erro estava no resumo e no analítico.

COM BANCO DE VERDADE porque a correção vive no `WHERE`.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core import relatorios
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    Categoria, Empresa, EscopoVisao, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    EspecieTitulo, FormaPagamento, Parcela, Rateio, StatusParcela, StatusTitulo,
    TipoTitulo, Titulo,
)

pytestmark = pytest.mark.banco

CUSTO = Decimal("10000")
RECEITA = Decimal("50000")


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    emp = Empresa(razao_social="BWS Construções Exemplo", cnpj="11222333000181",
                  nome_fantasia="BWS Exemplo")
    outra = Empresa(razao_social="Segunda Empresa Exemplo", cnpj="11222333000262",
                    nome_fantasia="Segunda")
    s.add_all([emp, outra])
    s.flush()
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio", empresa_id=emp.id)
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto", empresa_id=outra.id)
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    conta_custo = Categoria(codigo="3.1.01", descricao="Cimento",
                            grupo_codigo="3", grupo_nome="Custos de obra",
                            natureza="RESULTADO")
    conta_receita = Categoria(codigo="1.1.01", descricao="Receita de medição",
                              grupo_codigo="1", grupo_nome="Receitas",
                              natureza="RESULTADO")
    quem = Usuario(nome="Diretora", email="dir.rel@teste.bws.local",
                   senha_hash=gerar_hash("senha-de-teste-1234"),
                   perfil=P.DIRETOR_FINANCEIRO,
                   escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s.add_all([creche, escola, forn, conta_custo, conta_receita, quem])
    s.flush()

    def titulo(sp, obra, conta, valor, especie, vencimento):
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao="lançamento do teste",
                   valor_bruto=valor, valor_liquido=valor,
                   competencia=date(2026, 9, 1), categoria_id=conta.id,
                   forma_pagamento=FormaPagamento.PIX,
                   status=StatusTitulo.APROVADO, especie=especie,
                   solicitante_id=quem.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=valor,
                     percentual=Decimal("100.0000"), categoria_id=conta.id))
        s.add(Parcela(titulo_id=t.id, numero=1, valor=valor,
                      vencimento=vencimento, status=StatusParcela.ABERTA))
        s.flush()
        return t

    hoje = date.today()
    titulo("SP7001", creche, conta_custo, CUSTO, EspecieTitulo.PAGAR,
           hoje.replace(day=min(hoje.day, 28)))
    titulo("SP7002", creche, conta_receita, RECEITA, EspecieTitulo.RECEBER,
           hoje.replace(day=min(hoje.day, 28)))
    titulo("SP7003", escola, conta_custo, Decimal("2000"), EspecieTitulo.PAGAR,
           hoje.replace(day=min(hoje.day, 28)))
    s.commit()
    return {"usuario": quem, "creche": creche, "escola": escola,
            "empresa": emp, "outra": outra}


def _por_chave(r, pedaco):
    return next((l for l in r["linhas"] if pedaco in l["chave"]), None)


# ---------------------------------------------------------------------------
# A falha
# ---------------------------------------------------------------------------
def test_totais_por_obra_mostram_o_CUSTO_e_nao_custo_mais_receita(cenario, sessao_real):
    """O padrão é "a pagar": este é um relatório de custo. Antes, esta obra
    aparecia com 60.000 — a soma do que ela gastou com o que ela vai receber."""
    r = relatorios.resumo(sessao_real, "obra", {}, cenario["usuario"])
    linha = _por_chave(r, "CRECHE")
    assert linha is not None
    assert linha["total"] == float(CUSTO), (
        "o total da obra voltou a somar receita com custo — é o defeito que "
        "este arquivo existe para impedir")


def test_dá_para_pedir_o_que_a_obra_vai_receber(cenario, sessao_real):
    r = relatorios.resumo(sessao_real, "obra", {"especie": "receber"},
                          cenario["usuario"])
    assert _por_chave(r, "CRECHE")["total"] == float(RECEITA)


def test_os_dois_juntos_so_quando_pedido_e_separados_por_especie(cenario, sessao_real):
    """Pedir "os dois" é legítimo — o que não pode é acontecer sem pedir. E
    com a dimensão espécie eles aparecem em linhas diferentes."""
    r = relatorios.resumo(sessao_real, "especie", {"especie": "tudo"},
                          cenario["usuario"])
    assert _por_chave(r, "A pagar")["total"] == float(CUSTO) + 2000
    assert _por_chave(r, "A receber")["total"] == float(RECEITA)


def test_especie_invalida_e_recusada(cenario, sessao_real):
    with pytest.raises(ValueError):
        relatorios.resumo(sessao_real, "obra", {"especie": "qualquer"},
                          cenario["usuario"])


# ---------------------------------------------------------------------------
# Consolidado por empresa
# ---------------------------------------------------------------------------
def test_totais_por_empresa(cenario, sessao_real):
    """A BWS opera com mais de um CNPJ, e era a única visão que faltava."""
    r = relatorios.resumo(sessao_real, "empresa", {}, cenario["usuario"])
    assert _por_chave(r, "BWS Exemplo")["total"] == float(CUSTO)
    assert _por_chave(r, "Segunda")["total"] == 2000


def test_filtrar_por_empresa_deixa_so_o_que_e_dela(cenario, sessao_real):
    r = relatorios.resumo(sessao_real, "obra",
                          {"empresa_id": cenario["outra"].id}, cenario["usuario"])
    assert [l["chave"] for l in r["linhas"] if l["total"]] == ["ESCOLA · Escola do Planalto"]


# ---------------------------------------------------------------------------
# Curva ABC
# ---------------------------------------------------------------------------
def test_curva_abc_classifica_e_acumula(cenario, sessao_real):
    """O maior gasto responde por 10.000 de 12.000 — ele sozinho é classe A."""
    r = relatorios.curva_abc(sessao_real, "obra", {}, cenario["usuario"])
    assert [l["classe"] for l in r["linhas"]] == ["A", "B"]
    assert r["linhas"][0]["acumulado_pct"] == pytest.approx(83.33, abs=0.01)
    assert r["linhas"][-1]["acumulado_pct"] == pytest.approx(100.0, abs=0.01)
    assert {c["classe"] for c in r["classes"]} == {"A", "B"}


# ---------------------------------------------------------------------------
# Fluxo de caixa projetado
# ---------------------------------------------------------------------------
def test_fluxo_separa_o_que_entra_do_que_sai(cenario, sessao_real):
    """Entrada e saída em colunas diferentes — somadas, o relatório não
    serviria para decidir nada."""
    f = relatorios.fluxo_de_caixa(sessao_real, {}, cenario["usuario"])
    assert f["total_entra"] == float(RECEITA)
    assert f["total_sai"] == float(CUSTO) + 2000


def test_fluxo_acumula_a_partir_do_saldo_informado(cenario, sessao_real):
    """O sistema NÃO sabe o saldo do banco. Inventar um faria o acumulado
    parecer conta bancária sem ser."""
    sem = relatorios.fluxo_de_caixa(sessao_real, {}, cenario["usuario"])
    com = relatorios.fluxo_de_caixa(sessao_real, {}, cenario["usuario"],
                                    saldo_inicial=1000)
    assert com["saldo_final"] == pytest.approx(sem["saldo_final"] + 1000)


def test_fluxo_respeita_o_filtro_de_obra(cenario, sessao_real):
    f = relatorios.fluxo_de_caixa(sessao_real, {"obra_id": cenario["escola"].id},
                                  cenario["usuario"])
    assert f["total_sai"] == 2000
    assert f["total_entra"] == 0


def test_fluxo_recusa_passo_desconhecido(cenario, sessao_real):
    with pytest.raises(ValueError):
        relatorios.fluxo_de_caixa(sessao_real, {}, cenario["usuario"], passo="dia")
