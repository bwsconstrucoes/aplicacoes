"""Zerar o movimento do ERP por área, para recomeçar depois de testar.

Botão que apaga dados em produção. Os testes aqui são menos sobre "funciona" e
mais sobre **o que ele nunca pode fazer**:

  - nunca apagar cadastro — obra, fornecedor, plano de contas, colaborador,
    operador. Refazer isso custa horas e não é lançamento;
  - nunca apagar em cascata: se algo de fora aponta para o que sairia, ele
    RECUSA e diz qual área falta marcar;
  - nunca apagar sem a frase digitada;
  - e a ordem tem de sair das chaves estrangeiras do próprio banco, senão o
    Postgres recusa no meio e sobra metade.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.manutencao import limpeza
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Marcelo")

# Cadastros que custaram uma importação, e as tabelas sem as quais ninguém
# entra no sistema. Escrita à mão de propósito: é aqui que o erro aparece se
# alguém acrescentar uma delas a uma área.
CADASTROS = {
    "usuarios", "usuario_permissoes", "alcadas", "parametros", "_migracoes",
    "obras", "fornecedores", "fornecedor_contas", "categorias",
    "contas_bancarias", "colaboradores", "funcoes", "insumos",
    "unidades_compra", "condicoes_pagamento",
}


# ---------------------------------------------------------------------------
# O que jamais sai
# ---------------------------------------------------------------------------
def test_nenhuma_area_inclui_cadastro():
    for chave, (_rotulo, _desc, tabelas) in limpeza.AREAS.items():
        invasoras = set(tabelas) & CADASTROS
        assert not invasoras, (
            f"a área {chave} apagaria {sorted(invasoras)} — isso é cadastro, "
            f"não lançamento")


def test_a_lista_do_que_jamais_sai_cobre_o_essencial():
    for tabela in CADASTROS:
        assert tabela in limpeza.JAMAIS, f"{tabela} tinha de estar protegida"


def test_pedir_para_apagar_um_cadastro_e_recusado(monkeypatch):
    """Mesmo que alguém edite a lista de áreas por engano, a segunda trava pega."""
    monkeypatch.setitem(limpeza.AREAS, "invadida",
                        ("Invadida", "teste", ("usuarios",)))
    with pytest.raises(ErroValidacao, match="cadastro e nunca sai"):
        limpeza._tabelas_de(["invadida"])


def test_area_inventada_e_recusada():
    with pytest.raises(ErroValidacao, match="Área desconhecida"):
        limpeza._tabelas_de(["planetas"])


# ---------------------------------------------------------------------------
# A ordem, derivada do banco
# ---------------------------------------------------------------------------
def test_o_filho_sai_antes_do_pai():
    dependencias = [("parcelas", "titulos", "titulo_id", True),
                    ("pagamentos", "parcelas", "parcela_id", True)]

    ordem = limpeza.ordenar(["titulos", "parcelas", "pagamentos"], dependencias)

    assert ordem.index("pagamentos") < ordem.index("parcelas")
    assert ordem.index("parcelas") < ordem.index("titulos")


def test_tabela_sem_dependencia_tambem_entra():
    ordem = limpeza.ordenar(["ia_uso", "eventos"], [])
    assert set(ordem) == {"ia_uso", "eventos"}


def test_a_autorreferencia_nao_trava_a_ordem():
    """Categoria aponta para categoria; isso não pode virar laço infinito."""
    ordem = limpeza.ordenar(["titulos"], [("titulos", "titulos", "pai_id", False)])
    assert ordem == ["titulos"]


def test_um_ciclo_entre_tabelas_nao_trava():
    ordem = limpeza.ordenar(["a", "b"], [("a", "b", "x", True), ("b", "a", "y", True)])
    assert set(ordem) == {"a", "b"}, "com ciclo, resolve — mas não fica preso"


# ---------------------------------------------------------------------------
# A recusa que evita a cascata
# ---------------------------------------------------------------------------
def test_recusa_quando_algo_de_fora_aponta_para_o_que_sairia(monkeypatch):
    """Apagar em cascata é como se perde, num clique, uma tabela que ninguém
    pretendia tocar."""
    monkeypatch.setattr(limpeza, "_existentes", lambda s, t: list(t))
    monkeypatch.setattr(limpeza, "_dependencias",
                        lambda s: [("previsoes_pagamento", "titulos",
                                    "titulo_id", False)])
    monkeypatch.setattr(limpeza, "bloqueios",
                        lambda s, t, d: [{"tabela": "previsoes_pagamento",
                                          "aponta_para": "titulos",
                                          "coluna": "titulo_id", "linhas": 3,
                                          "area": "suprimentos"}])
    s = SessaoFalsa(ADMIN)

    with pytest.raises(ErroValidacao, match="Marque também: suprimentos"):
        limpeza.zerar(s, ["financeiro"], limpeza.FRASE_DE_CONFIRMACAO, ADMIN)

    assert not any("DELETE" in x for x in s.executados), (
        "a recusa tem de vir antes de apagar a primeira linha")


def test_a_recusa_diz_quantas_linhas_prendem(monkeypatch):
    monkeypatch.setattr(limpeza, "_existentes", lambda s, t: list(t))
    monkeypatch.setattr(limpeza, "_dependencias", lambda s: [])
    monkeypatch.setattr(limpeza, "bloqueios",
                        lambda s, t, d: [{"tabela": "previsoes_pagamento",
                                          "aponta_para": "titulos",
                                          "coluna": "titulo_id", "linhas": 7,
                                          "area": "suprimentos"}])
    with pytest.raises(ErroValidacao, match=r"previsoes_pagamento \(7\)"):
        limpeza.zerar(SessaoFalsa(ADMIN), ["financeiro"],
                      limpeza.FRASE_DE_CONFIRMACAO, ADMIN)


# ---------------------------------------------------------------------------
# A confirmação e o rastro
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("digitado", ["", "sim", "zerar tudo", "apagar"])
def test_sem_a_frase_exata_nao_apaga(digitado):
    s = SessaoFalsa(ADMIN)
    with pytest.raises(ErroValidacao, match="digite exatamente"):
        limpeza.zerar(s, ["ia"], digitado, ADMIN)
    assert s.executados == []


def test_sem_area_escolhida_nao_apaga():
    with pytest.raises(ErroValidacao, match="ao menos uma área"):
        limpeza.zerar(SessaoFalsa(ADMIN), [], limpeza.FRASE_DE_CONFIRMACAO, ADMIN)


def test_apagar_deixa_registro(monkeypatch):
    monkeypatch.setattr(limpeza, "_existentes", lambda s, t: ["ia_uso"])
    monkeypatch.setattr(limpeza, "_dependencias", lambda s: [])
    monkeypatch.setattr(limpeza, "bloqueios", lambda s, t, d: [])
    s = SessaoFalsa(ADMIN, linhas_sql=[[(1,), (2,)]])

    limpeza.zerar(s, ["ia"], limpeza.FRASE_DE_CONFIRMACAO, ADMIN)

    evento = next(e for e in s.eventos if e.get("ac") == "MOVIMENTO_ZERADO")
    assert evento["ui"] == 1


def test_zerar_a_auditoria_nao_tenta_gravar_evento_no_que_acabou_de_apagar(monkeypatch):
    monkeypatch.setattr(limpeza, "_existentes", lambda s, t: ["eventos"])
    monkeypatch.setattr(limpeza, "_dependencias", lambda s: [])
    monkeypatch.setattr(limpeza, "bloqueios", lambda s, t, d: [])
    s = SessaoFalsa(ADMIN, linhas_sql=[[]])

    limpeza.zerar(s, ["auditoria"], limpeza.FRASE_DE_CONFIRMACAO, ADMIN)

    assert s.eventos == [], "gravar o evento logo após apagar a tabela é ilusão"


# ---------------------------------------------------------------------------
# O catálogo que a tela usa
# ---------------------------------------------------------------------------
def test_o_catalogo_descreve_cada_area_em_portugues():
    for area in limpeza.catalogo():
        assert area["rotulo"] and area["descricao"]
        assert area["tabelas"]


def test_as_areas_nao_repetem_tabela_entre_si():
    """Tabela em duas áreas faria a contagem mentir e a ordem ficar ambígua."""
    vistas: dict[str, str] = {}
    for chave, (_r, _d, tabelas) in limpeza.AREAS.items():
        for tabela in tabelas:
            assert tabela not in vistas, (
                f"{tabela} está em {vistas.get(tabela)} e em {chave}")
            vistas[tabela] = chave


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


@pytest.mark.parametrize("perfil", [P.FINANCEIRO, P.DIRETOR_FINANCEIRO,
                                    P.GESTOR_OBRA, P.ADMINISTRATIVO_OBRA])
def test_so_o_administrador_chega_no_botao(perfil, monkeypatch):
    s = SessaoFalsa(novo_usuario(9, perfil))
    c = _cliente(s, monkeypatch, 9)

    assert c.get("/erp/api/manutencao/limpeza").status_code == 403
    assert c.post("/erp/api/manutencao/limpeza", json={}).status_code == 403


def test_a_previa_nao_apaga(monkeypatch):
    monkeypatch.setattr(limpeza, "_existentes", lambda s, t: ["ia_uso"])
    monkeypatch.setattr(limpeza, "_dependencias", lambda s: [])
    monkeypatch.setattr(limpeza, "bloqueios", lambda s, t, d: [])
    s = SessaoFalsa(ADMIN, linhas_sql=[[(5,)]])
    c = _cliente(s, monkeypatch, 1)

    r = c.get("/erp/api/manutencao/limpeza?areas=ia")

    assert r.status_code == 200
    assert not any("DELETE" in x for x in s.executados)
    assert r.get_json()["resumo"]["frase"] == limpeza.FRASE_DE_CONFIRMACAO


def test_a_tela_recebe_o_catalogo_das_areas(monkeypatch):
    s = SessaoFalsa(ADMIN)
    r = _cliente(s, monkeypatch, 1).get("/erp/api/manutencao/limpeza/areas")

    assert r.status_code == 200
    chaves = {a["chave"] for a in r.get_json()["areas"]}
    assert {"suprimentos", "financeiro", "conciliacao"} <= chaves
