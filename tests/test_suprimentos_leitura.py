"""Colar a lista de materiais e a IA montar as linhas.

Ao montar o cronograma a quantidade já está tabulada em algum lugar; redigitar
40 linhas é onde se perde tempo e entram erros. A regra que vale para toda
leitura por IA neste sistema: ela SUGERE e CRITICA, nunca decide sozinha.

O que não pode falhar:
  - material que não casa com o cadastro vem marcado como NÃO RECONHECIDO — a
    máquina não pode escolher um insumo parecido e seguir em frente;
  - a quantidade em português ("1.200,50") vira número;
  - a leitura NÃO grava nada;
  - o consumo de IA passa pelo mesmo ponto de registro das outras leituras,
    senão some do painel de custo.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import leitura
from app.apps.erp.db.models.cadastros import (
    Insumo, Obra, PerfilUsuario as P, UnidadeCompra,
)

from conftest import SessaoFalsa, novo_usuario

CADASTRO = (
    UnidadeCompra(codigo="UN", descricao="Unidade"),
    UnidadeCompra(codigo="M", descricao="Metro"),
    Insumo(id=10, codigo="INS-0010", descricao="Vergalhão CA50 12.5mm", unidade="M"),
    Insumo(id=11, codigo="INS-0011", descricao="Cola/Selante PU Sache 800ml", unidade="UN"),
    Obra(id=1, codigo="CREPETERRA", nome="Crepeterra"),
)

TEXTO = "Vergalhão CA50 12.5mm\t180\tM\nCola PU 800ml\t12\tUN"


@pytest.fixture
def ia(monkeypatch):
    """Substitui a chamada à OpenAI. O teste diz o que a IA 'respondeu'."""
    chamadas = []

    def falsa(*, texto="", imagens=None, dica=""):
        chamadas.append({"texto": texto, "dica": dica})
        return falsa.resposta
    falsa.resposta = {"itens": []}
    falsa.chamadas = chamadas

    import app.apps.erp.core.documentos.leitor as leitor
    monkeypatch.setattr(leitor, "_chamar_ia", falsa)
    return falsa


def test_o_que_casa_com_o_cadastro_vem_pronto(ia):
    ia.resposta = {"itens": [
        {"descricao": "Vergalhão CA50 12.5mm", "quantidade": "180",
         "unidade": "M", "obra": "CREPETERRA", "especificacao": "CA50"},
    ]}
    s = SessaoFalsa(*CADASTRO)

    r = leitura.ler_lista(s, TEXTO)

    linha = r["itens"][0]
    assert linha["insumo_id"] == 10
    assert linha["confianca"] == "ALTA"
    assert linha["quantidade"] == "180" and linha["unidade"] == "M"
    assert linha["obra_id"] == 1
    assert r["nao_reconhecidos"] == []


def test_o_que_nao_casa_vem_marcado_e_nao_e_chutado(ia):
    """Escolher um insumo parecido e seguir em frente é como o pedido sai com
    o material errado — e ninguém percebe até chegar na obra."""
    ia.resposta = {"itens": [
        {"descricao": "Parafuso sextavado inox 3/8", "quantidade": "50", "unidade": "UN"},
    ]}
    s = SessaoFalsa(*CADASTRO)

    r = leitura.ler_lista(s, TEXTO)

    linha = r["itens"][0]
    assert linha["insumo_id"] is None
    assert linha["confianca"] == "NAO_RECONHECIDO"
    assert linha["descricao_lida"] == "Parafuso sextavado inox 3/8"
    assert r["nao_reconhecidos"] == ["Parafuso sextavado inox 3/8"]
    assert "1 sem correspondência" in r["resumo"]


@pytest.mark.parametrize("lido,esperado", [
    ("1.200,50", "1200.50"), ("180", "180"), ("12 un", "12"),
    ("qtd: 7,5", "7.5"), ("", ""), ("sem número", ""),
])
def test_a_quantidade_em_portugues_vira_numero(ia, lido, esperado):
    ia.resposta = {"itens": [{"descricao": "Vergalhão CA50 12.5mm", "quantidade": lido}]}

    r = leitura.ler_lista(SessaoFalsa(*CADASTRO), TEXTO)

    assert r["itens"][0]["quantidade"] == esperado


def test_unidade_desconhecida_cai_para_a_do_insumo(ia):
    ia.resposta = {"itens": [{"descricao": "Vergalhão CA50 12.5mm",
                              "quantidade": "10", "unidade": "BARRA"}]}

    r = leitura.ler_lista(SessaoFalsa(*CADASTRO), TEXTO)

    assert r["itens"][0]["unidade"] == "M", "veio do cadastro do insumo"


def test_linha_sem_descricao_e_ignorada(ia):
    ia.resposta = {"itens": [{"descricao": "", "quantidade": "10"},
                             {"descricao": "Vergalhão CA50 12.5mm", "quantidade": "1"}]}

    r = leitura.ler_lista(SessaoFalsa(*CADASTRO), TEXTO)

    assert len(r["itens"]) == 1


def test_a_leitura_nao_grava_nada(ia):
    ia.resposta = {"itens": [{"descricao": "Vergalhão CA50 12.5mm", "quantidade": "1"}]}
    s = SessaoFalsa(*CADASTRO)

    leitura.ler_lista(s, TEXTO)

    assert s.adicionados == [] and s.eventos == []


def test_o_consumo_de_ia_e_registrado_com_a_operacao(ia, monkeypatch):
    """Se a leitura não passar pelo ponto de registro, ela some do painel de
    custo — e aí o teto mensal deixa de significar o gasto real."""
    vistos = []
    import app.apps.erp.core.comum.ia_custo as ia_custo
    original = ia_custo.contexto

    @contextlib.contextmanager
    def espiao(**kw):
        vistos.append(kw)
        with original(**kw):
            yield
    monkeypatch.setattr(ia_custo, "contexto", espiao)
    ia.resposta = {"itens": []}

    leitura.ler_lista(SessaoFalsa(*CADASTRO), TEXTO)

    assert vistos and vistos[0]["operacao"] == "lista_suprimentos"


@pytest.mark.parametrize("texto,erro", [
    ("", "Cole a lista"), ("curto", "Cole a lista"), ("x" * 20001, "grande demais")])
def test_texto_invalido_e_recusado_antes_de_gastar_token(texto, erro, ia):
    with pytest.raises(ErroValidacao, match=erro):
        leitura.ler_lista(SessaoFalsa(*CADASTRO), texto)
    assert ia.chamadas == [], "não se paga leitura de texto que já sabemos recusar"


def test_falha_da_ia_vira_recado_e_nao_erro_cru(monkeypatch):
    import app.apps.erp.core.documentos.leitor as leitor

    def explode(**kw):
        raise leitor.ErroLeitura("chave da OpenAI não configurada")
    monkeypatch.setattr(leitor, "_chamar_ia", explode)

    with pytest.raises(ErroValidacao, match="Não consegui ler a lista"):
        leitura.ler_lista(SessaoFalsa(*CADASTRO), TEXTO)


# ---------------------------------------------------------------------------
# A rota
# ---------------------------------------------------------------------------
def _cliente(sessao, monkeypatch, usuario_id):
    from app.apps.erp import routes
    app = Flask(__name__)
    app.secret_key = "teste"
    app.register_blueprint(routes.bp)
    monkeypatch.setattr(routes, "get_session",
                        lambda: contextlib.nullcontext(sessao))
    c = app.test_client()
    with c.session_transaction() as web:
        web["erp_usuario_id"] = usuario_id
    return c


def test_a_rota_devolve_as_linhas_para_conferir(ia, monkeypatch):
    ia.resposta = {"itens": [{"descricao": "Vergalhão CA50 12.5mm", "quantidade": "180"}]}
    s = SessaoFalsa(novo_usuario(5, P.ADMINISTRATIVO_OBRA), *CADASTRO)

    r = _cliente(s, monkeypatch, 5).post("/erp/api/suprimentos/ler-lista",
                                         json={"texto": TEXTO})

    assert r.status_code == 200
    assert r.get_json()["leitura"]["itens"][0]["insumo_id"] == 10


def test_quem_nao_pede_material_nao_usa_a_leitura(ia, monkeypatch):
    s = SessaoFalsa(novo_usuario(1, P.FINANCEIRO), *CADASTRO)

    r = _cliente(s, monkeypatch, 1).post("/erp/api/suprimentos/ler-lista",
                                         json={"texto": TEXTO})

    assert r.status_code == 403
