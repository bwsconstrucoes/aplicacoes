"""Zerar o movimento de Suprimentos depois de um teste.

Isto é um botão que apaga dados em produção — então os testes aqui são menos
sobre "funciona" e mais sobre **o que ele nunca pode fazer**:

  - nunca encostar no financeiro nem nos cadastros;
  - nunca apagar sem a frase de confirmação digitada;
  - nunca apagar quando uma previsão já virou título, porque aí deixou de ser
    teste e virou dinheiro;
  - nunca apagar sem deixar rastro de quem fez e de quanto saiu;
  - e a ordem dos passos tem de respeitar as chaves estrangeiras, senão o
    banco recusa no meio e sobra metade.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import limpeza
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Marcelo")

# Tabelas que este botão NUNCA pode tocar. A lista está escrita à mão de
# propósito: se alguém acrescentar uma tabela do financeiro aos passos, é aqui
# que o erro aparece — e não no dia em que alguém apertar o botão.
INTOCAVEIS = {
    # financeiro
    "titulos", "parcelas", "pagamentos", "conciliacoes", "rateios", "extratos",
    "extrato_linhas", "despesas_colaborador", "contrato_medicoes",
    "contratos_servico", "contratos_locacao", "locacao_parcelas", "retencoes",
    "analises", "documentos_fiscais",
    # cadastros que custam trabalho para refazer
    "insumos", "insumo_categorias", "fornecedores", "fornecedor_contatos",
    "fornecedor_categorias", "fornecedor_contas", "unidades_compra",
    "condicoes_pagamento", "obras", "usuarios", "categorias", "contas_bancarias",
    "usuario_obras", "usuario_permissoes", "parametros", "_migracoes",
}


def _sessao(**kw):
    return SessaoFalsa(ADMIN, **kw)


# ---------------------------------------------------------------------------
# O que a limpeza jamais pode alcançar
# ---------------------------------------------------------------------------
def test_nenhum_passo_toca_no_financeiro_nem_nos_cadastros():
    tocadas = {tabela for tabela, _, _ in limpeza.PASSOS}
    proibidas = tocadas & INTOCAVEIS
    assert not proibidas, (
        f"a limpeza apagaria {sorted(proibidas)} — trazer os cadastros das "
        f"planilhas dá trabalho, e o financeiro não é assunto deste botão")


def test_os_anexos_apagados_sao_so_os_das_propostas():
    """A tabela de anexos guarda nota fiscal, comprovante e contrato do
    financeiro. Só as propostas de cotação podem sair."""
    corpo = next(c for t, _, c in limpeza.PASSOS if t == "anexos")
    assert "entidade_tipo = 'cotacao_fornecedor'" in corpo


def test_a_ordem_respeita_as_chaves_estrangeiras():
    """Pai antes de filho e o banco recusa no meio, deixando metade apagada."""
    ordem = [tabela for tabela, _, _ in limpeza.PASSOS]
    depende_de = {
        "recebimento_itens": "recebimentos",
        "previsoes_pagamento": "pedidos_compra",
        "pedido_itens": "pedidos_compra",
        "pedido_item_reserva": "pedidos_compra",
        "cotacao_precos": "cotacao_itens",
        "cotacao_itens": "cotacoes",
        "cotacao_fornecedores": "cotacoes",
        "precos_historico": "cotacoes",
        "suprimento_itens": "suprimento_solicitacoes",
        "pedidos_compra": "cotacoes",
    }
    for filho, pai in depende_de.items():
        assert ordem.index(filho) < ordem.index(pai), (
            f"{filho} tem de sair antes de {pai}")


def test_a_lista_do_que_e_preservado_aparece_para_quem_confirma():
    texto = " ".join(limpeza.PRESERVADO).lower()
    for palavra in ("insumos", "fornecedores", "financeiro"):
        assert palavra in texto


# ---------------------------------------------------------------------------
# A confirmação
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("digitado", ["", "sim", "zerar", "ZERAR", "apagar tudo"])
def test_sem_a_frase_exata_nao_apaga_nada(digitado):
    s = _sessao()
    with pytest.raises(ErroValidacao, match="digite exatamente"):
        limpeza.zerar(s, digitado, ADMIN)
    assert s.executados == [], "nem uma consulta pode sair antes da confirmação"


def test_a_frase_certa_libera_e_aceita_minuscula():
    s = _sessao(linhas_sql=[[]])
    limpeza.zerar(s, "zerar suprimentos", ADMIN)
    assert any("DELETE" in x for x in s.executados)


# ---------------------------------------------------------------------------
# A recusa que mais importa
# ---------------------------------------------------------------------------
def test_recusa_quando_a_previsao_ja_virou_titulo():
    """Se virou título, isto deixou de ser teste e virou dinheiro."""
    s = _sessao(linhas_sql=[[("PC-0001", 555)]])

    with pytest.raises(ErroValidacao, match="já virou título"):
        limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, ADMIN)

    assert not any("DELETE" in x for x in s.executados), (
        "a recusa tem de vir ANTES de apagar a primeira linha")


def test_a_recusa_diz_qual_pedido_esta_preso():
    s = _sessao(linhas_sql=[[("PC-0042", 777)]])
    with pytest.raises(ErroValidacao, match="PC-0042"):
        limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, ADMIN)


# ---------------------------------------------------------------------------
# O rastro
# ---------------------------------------------------------------------------
def test_apagar_deixa_registro_de_quem_fez_e_de_quanto_saiu():
    s = _sessao(linhas_sql=[[]])

    limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, ADMIN)

    evento = next(e for e in s.eventos if e.get("ac") == "MOVIMENTO_ZERADO")
    assert evento["ui"] == 1
    assert "total" in evento["dt"]


def test_data_invalida_e_recusada():
    with pytest.raises(ErroValidacao, match="Data inválida"):
        limpeza.zerar(_sessao(), limpeza.FRASE_DE_CONFIRMACAO, ADMIN,
                      desde_bruto="ontem")


# ---------------------------------------------------------------------------
# A rota: só o ADMIN, e a prévia não apaga
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


@pytest.mark.parametrize("perfil", [P.FINANCEIRO, P.DIRETOR_FINANCEIRO,
                                    P.GESTOR_OBRA, P.ADMINISTRATIVO_OBRA])
def test_so_o_administrador_chega_no_botao(perfil, monkeypatch):
    s = SessaoFalsa(novo_usuario(9, perfil))
    c = _cliente(s, monkeypatch, 9)

    assert c.get("/erp/api/suprimentos/zerar").status_code == 403
    assert c.post("/erp/api/suprimentos/zerar", json={}).status_code == 403


def test_a_previa_conta_sem_apagar(monkeypatch):
    # uma contagem por passo e, por último, a consulta dos impedimentos (vazia)
    s = _sessao(linhas_sql=[[(3,)]] * len(limpeza.PASSOS) + [[]])
    c = _cliente(s, monkeypatch, 1)

    r = c.get("/erp/api/suprimentos/zerar")

    assert r.status_code == 200
    assert not any("DELETE" in x for x in s.executados)
    assert r.get_json()["resumo"]["frase"] == limpeza.FRASE_DE_CONFIRMACAO
