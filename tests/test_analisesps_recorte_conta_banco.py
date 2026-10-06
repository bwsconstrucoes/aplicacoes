# -*- coding: utf-8 -*-
"""
ACESSO PRESO A UMA CONTA BANCÁRIA — 06/10/2026, com banco de verdade.

O dono: *"quero criar um usuario que vai poder acessar somente Solicitações de
uma conta especifica"*.

O recorte vive num `WHERE` — e o dublê da suíte ignora `WHERE`. Por isso estes
testes só valem com Postgres: um erro aqui não estoura, ele mostra a conta dos
outros.
"""
import pytest

from tests.test_analisesps_banco import semear, sp
from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso, criar, entrar_como)

pytestmark = pytest.mark.banco

CONTA_A, CONTA_B = "BRADESCO 1234-5", "ITAU 9876-0"


@pytest.fixture
def duas_contas(app):
    semear([
        sp("900001", credor="CREDOR DA A", conta=CONTA_A, valor="100,00",
           vencimento="10/10/2026", status_pgt="Pagar"),
        sp("900002", credor="CREDOR DA A DOIS", conta=CONTA_A, valor="50,00",
           vencimento="11/10/2026", status_pgt="Pagar"),
        sp("900003", credor="CREDOR SEGREDO DA B", conta=CONTA_B,
           valor="9.999,00", vencimento="12/10/2026", status_pgt="Pagar"),
    ])
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()
    yield app
    consultas.esquecer_opcoes_de_filtro()


def criar_preso(contas=(CONTA_A,), telas=("solicitacoes",), pode_operar=True):
    from app.apps.analisesps import usuarios
    r = usuarios.criar("joana", "senha-da-joana", nome="JOANA", telas=telas,
                       pode_operar=pode_operar, contas=contas)
    assert r.get("ok"), r
    return r["id"]


def entrar_presa(cliente):
    entrar_como(cliente, login="joana", senha="senha-da-joana")


# ---------------------------------------------------------------------------
# O cadastro
# ---------------------------------------------------------------------------
def test_o_cadastro_guarda_as_contas_e_vazio_quer_dizer_TODAS(app):
    from app.apps.analisesps import usuarios
    uid = criar_preso(contas=[f"  {CONTA_A} ", CONTA_A])
    assert usuarios.buscar("joana")["contas"] == [CONTA_A]
    assert [p["contas"] for p in usuarios.listar()] == [[CONTA_A]]
    # Sem conta nenhuma marcada: solta — vê todas, como era antes da 051.
    assert usuarios.atualizar(uid, contas=[])["ok"]
    assert usuarios.buscar("joana")["contas"] == []
    # Quem não manda contas não mexe nelas (alterar só a senha, por exemplo).
    assert usuarios.atualizar(uid, contas=[CONTA_B])["ok"]
    assert usuarios.atualizar(uid, senha="outra-senha-boa")["ok"]
    assert usuarios.buscar("joana")["contas"] == [CONTA_B]


def test_MESTRE_preso_a_conta_e_recusado_em_vez_de_ignorado(app):
    """Gravar e deixar sem efeito faria a tela mostrar uma trava que não trava."""
    from app.apps.analisesps import usuarios
    r = usuarios.criar("chefe", "senha-do-chefe", telas=(), mestre=True,
                       contas=[CONTA_A])
    assert not r["ok"] and "mestre" in r["erro"].lower()


def test_apagar_a_pessoa_leva_as_contas_junto(app):
    from app.apps.analisesps import usuarios
    from app.apps.analisesps.db import consultar_um
    uid = criar_preso()
    assert usuarios.apagar(uid)["ok"]
    assert consultar_um("SELECT count(*) FROM analisesps.usuario_contas")[0] == 0


def test_a_tela_de_cadastro_grava_as_contas_marcadas(duas_contas):
    from app.apps.analisesps import usuarios
    with duas_contas.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        tela = cliente.get("/analisesps/configuracoes").get_data(as_text=True)
        assert 'name="conta_do_usuario"' in tela and CONTA_B in tela
        r = cliente.post("/analisesps/usuarios", data={
            "acao": "criar", "novo_usuario": "joana", "nova_senha": "senha-da-joana",
            "nome": "JOANA", "tela_do_usuario": ["solicitacoes"],
            "conta_do_usuario": [CONTA_A]})
        assert r.status_code == 302 and "erro_usuario" not in r.location
    assert usuarios.buscar("joana")["contas"] == [CONTA_A]


# ---------------------------------------------------------------------------
# O que ela vê
# ---------------------------------------------------------------------------
def test_a_lista_os_totais_e_os_filtros_so_tem_a_conta_DELA(duas_contas):
    criar_preso()
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        tela = cliente.get("/analisesps/solicitacoes?busca=",
                           follow_redirects=True).get_data(as_text=True)
    assert "900001" in tela and "900002" in tela
    assert "900003" not in tela and "SEGREDO" not in tela
    assert CONTA_B not in tela, "nem como opção do filtro de conta"
    assert "9.999" not in tela, "nem nos totais"
    assert f"na conta {CONTA_A}" in tela


def test_a_EXPORTACAO_so_leva_a_conta_dela(duas_contas):
    criar_preso()
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        csv = cliente.get("/analisesps/exportar").get_data(as_text=True)
    assert "900001" in csv and "900003" not in csv


def test_pedir_a_conta_dos_outros_pelo_FILTRO_nao_traz_nada(duas_contas):
    criar_preso()
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        tela = cliente.get(f"/analisesps/solicitacoes?conta={CONTA_B}",
                           follow_redirects=True).get_data(as_text=True)
    assert "900003" not in tela and "900001" not in tela


def test_a_FICHA_de_outra_conta_nao_existe_para_ela(duas_contas):
    criar_preso()
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        assert cliente.get("/analisesps/sp/900001?modal=1").status_code == 200
        fora = cliente.get("/analisesps/sp/900003")
        # igual à SP que não existe: 404, e nada do conteúdo
        assert fora.status_code == 404
        assert "SEGREDO" not in fora.get_data(as_text=True)
        assert cliente.get("/analisesps/sp/123456789").status_code == 404
        codigos = cliente.get("/analisesps/codigos?id=900003").get_data(as_text=True)
        assert "SEGREDO" not in codigos and "não encontrada" in codigos


def test_ALTERAR_ou_VALIDAR_SP_de_outra_conta_e_recusado_sem_gravar_nada(
        duas_contas, monkeypatch):
    from app.apps.analisesps import web
    gravadas = []
    monkeypatch.setattr(web, "_gravar_alteracao",
                        lambda ids, *a, **k: gravadas.append(ids) or {"ok": True})
    criar_preso()
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        # metade dentro, metade fora: recusa TUDO
        r = cliente.post("/analisesps/api/alterar", json={
            "ids": ["900001", "900003"], "coluna": "status_pgt", "valor": "Pago"})
        assert r.status_code == 404 and not r.get_json()["ok"]
        r = cliente.post("/analisesps/api/sem-risco", json={"ids": ["900003"]})
        assert r.status_code == 404
        r = cliente.post("/analisesps/api/enviar-ao-lote", json={"ids": ["900003"]})
        assert r.status_code == 404
        assert cliente.get("/analisesps/beevale/gerar?id=900003").status_code == 404
        assert gravadas == []
        # a SP dela passa
        r = cliente.post("/analisesps/api/alterar", json={
            "ids": ["900001"], "coluna": "status_pgt", "valor": "Pago"})
        assert r.status_code == 200, r.get_data(as_text=True)
    assert gravadas == [["900001"]]


# ---------------------------------------------------------------------------
# As telas
# ---------------------------------------------------------------------------
def test_tela_que_soma_todas_as_contas_fica_FECHADA_mesmo_marcada(duas_contas):
    """Relatório, Agenda, Lote… somam as contas juntas. Marcadas no cadastro de
    quem está presa, elas saem do menu e respondem "não encontrado"."""
    criar_preso(telas=("solicitacoes", "relatorio", "lote", "agenda"))
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        assert cliente.get("/analisesps/relatorio").status_code == 404
        assert cliente.get("/analisesps/lote").status_code == 404
        assert cliente.get("/analisesps/agenda").status_code == 404
        # o cadastro BeeVale procura gente em todas as SPs
        assert cliente.get("/analisesps/beevale/cadastro").status_code == 404
        tela = cliente.get("/analisesps/solicitacoes",
                           follow_redirects=True).get_data(as_text=True)
    assert "/analisesps/relatorio" not in tela, "nem aparece no menu"
    assert "/analisesps/beevale/cadastro" not in tela, "nem o botão"


def test_SOLTAR_a_conta_vale_na_hora(duas_contas):
    from app.apps.analisesps import usuarios
    uid = criar_preso()
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        assert cliente.get("/analisesps/sp/900003").status_code == 404
        usuarios.atualizar(uid, contas=[])
        assert cliente.get("/analisesps/sp/900003").status_code == 200


def test_quem_NAO_esta_preso_continua_vendo_todas(duas_contas):
    criar(telas=("solicitacoes",))
    with duas_contas.test_client() as cliente:
        entrar_como(cliente)
        tela = cliente.get("/analisesps/solicitacoes",
                           follow_redirects=True).get_data(as_text=True)
    assert "900001" in tela and "900003" in tela


def test_a_presa_nao_ENVENENA_os_filtros_guardados_dos_outros(duas_contas):
    """As listas de filtro ficam guardadas para todo mundo. Se a dela entrasse
    nesse guardado, o dono abriria a tela e veria só a conta dela."""
    criar_preso()
    criar(telas=("solicitacoes",))
    with duas_contas.test_client() as cliente:
        entrar_presa(cliente)
        cliente.get("/analisesps/solicitacoes", follow_redirects=True)
    with duas_contas.test_client() as cliente:
        entrar_como(cliente)
        tela = cliente.get("/analisesps/solicitacoes",
                           follow_redirects=True).get_data(as_text=True)
    assert CONTA_B in tela
