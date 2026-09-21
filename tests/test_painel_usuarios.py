# -*- coding: utf-8 -*-
"""
Acesso por pessoa, preso a obras e a telas.

Pedido do dono em 21/09/2026: dar acesso a um parceiro de obra que entre com
senha própria e veja SÓ as obras dele, e só as telas liberadas — começando por
DRE e Despesas Analítico.

O que se prova aqui é uma coisa só, dita de várias formas: **não vaza**. E a
regra que sustenta isso é sempre a mesma — falhar FECHADO. Cadastro pela metade
não vira acesso total; lista vazia quer dizer nenhuma, nunca todas.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.banco

SENHA_MESTRE = "senha-do-dono"


@pytest.fixture()
def base_com_duas_obras():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import consultas
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), resultado

    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM usuarios")
        for cod, obra, valor in ((701, "OBRA DELE", -1000),
                                 (702, "OBRA DE OUTRO", -9000)):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " situacao_vencimento, categoria, grupo, departamento, projeto,"
                " razao_social, data, ano, pago_recebido, a_pagar_receber,"
                " juros, multa)"
                " VALUES (?,'2. Contas a Pagar','DRE','PAGO','Quitado',"
                "         'Serviços','Custo',?,'ALFA','FORNECEDOR X',"
                "         '2025-03-10',2025,?,0,0,0)", (cod, obra, valor))
        conn.commit()
    consultas.esquecer_listas()
    yield
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM usuarios")
        conn.commit()


@pytest.fixture()
def parceiro(base_com_duas_obras):
    """Alguém com UMA obra e DUAS telas — o caso que o dono descreveu."""
    from app.apps.painel import usuarios
    r = usuarios.criar("parceiro", "senha-dele", nome="Parceiro da Obra",
                       obras=["OBRA DELE"], telas=["dre", "analitico"])
    assert r["ok"], r
    return r["id"]


def _cliente(monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", SENHA_MESTRE)
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    return app.test_client()


def _entrar(cliente, usuario="", senha=SENHA_MESTRE):
    return cliente.post("/painel/entrar",
                        data={"usuario": usuario, "senha": senha})


# ===========================================================================
# 1. A senha guardada
# ===========================================================================
def test_a_senha_nao_fica_legivel_no_banco(parceiro):
    """Este banco tem o financeiro inteiro da empresa. Senha legível aqui é
    vazamento esperando acontecer — nem o dono lê a senha de alguém, só troca."""
    from app.apps.painel.db import consultar
    (guardada,) = consultar("SELECT senha_hash FROM usuarios WHERE id = ?",
                            (parceiro,))[0]
    assert "senha-dele" not in guardada
    assert len(guardada) > 40, "não parece um hash"


def test_senha_errada_nunca_confere(parceiro):
    from app.apps.painel import usuarios
    pessoa = usuarios.buscar("parceiro")
    assert usuarios.senha_confere(pessoa, "senha-dele") is True
    assert usuarios.senha_confere(pessoa, "senha-dela") is False
    assert usuarios.senha_confere(None, "qualquer") is False


def test_o_login_nao_depende_de_maiuscula(parceiro):
    from app.apps.painel import usuarios
    assert usuarios.buscar("PARCEIRO") is not None
    assert usuarios.buscar("  Parceiro ") is not None


def test_nao_da_para_criar_dois_com_o_mesmo_login(parceiro):
    from app.apps.painel import usuarios
    r = usuarios.criar("PARCEIRO", "outra-senha", obras=["OBRA DELE"],
                       telas=["dre"])
    assert r["ok"] is False and "Já existe" in r["erro"]


def test_senha_curta_e_recusada(base_com_duas_obras):
    from app.apps.painel import usuarios
    r = usuarios.criar("fulano", "12345", obras=["OBRA DELE"], telas=["dre"])
    assert r["ok"] is False and "6 letras" in r["erro"]


# ===========================================================================
# 2. O escopo — que é o ponto de tudo
# ===========================================================================
def test_o_parceiro_so_ve_o_numero_da_obra_dele(parceiro, monkeypatch):
    """A prova principal: a despesa da outra obra não pode aparecer nem no
    total."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    html = cliente.get("/painel/dre").get_data(as_text=True)
    assert "1.000,00" in html, "a despesa da obra dele tem de aparecer"
    assert "9.000,00" not in html, "a despesa da outra obra VAZOU"
    assert "OBRA DE OUTRO" not in html, "até o nome da outra obra vazou"


def test_tirar_a_obra_do_endereco_nao_abre_a_empresa_inteira(parceiro, monkeypatch):
    """O ataque óbvio: apagar o filtro da barra de endereço. Se a lista vazia
    virasse "sem filtro", bastaria isso para ver tudo."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    html = cliente.get("/painel/dre?obra=").get_data(as_text=True)
    assert "9.000,00" not in html


def test_pedir_a_obra_de_outro_no_endereco_nao_funciona(parceiro, monkeypatch):
    """O segundo ataque óbvio: escrever a obra do vizinho na barra de endereço."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    html = cliente.get(
        "/painel/dre?obra=OBRA+DE+OUTRO").get_data(as_text=True)
    assert "9.000,00" not in html
    assert "1.000,00" in html, "e continua vendo a dele"


def test_a_barra_lateral_nao_mostra_as_obras_dos_outros(parceiro, monkeypatch):
    """Mostrar a lista inteira entregaria o nome de todas as obras da empresa a
    quem só pode ver uma — informação que ele não teria de outro jeito."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    html = cliente.get("/painel/dre").get_data(as_text=True)
    assert "OBRA DELE" in html
    assert "OBRA DE OUTRO" not in html


def test_o_download_sai_so_com_a_obra_dele(parceiro, monkeypatch):
    """O dono liberou o download. Ele tem de sair no mesmo recorte da tela —
    senão o arquivo é a brecha."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    r = cliente.get("/painel/baixar/analitico")
    assert r.status_code == 200
    corpo = r.get_data()
    assert b"OBRA DE OUTRO" not in corpo


# ===========================================================================
# 3. As telas
# ===========================================================================
def test_tela_nao_liberada_responde_nao_encontrado(parceiro, monkeypatch):
    """404 e não 403: dizer "sem permissão" confirma que a tela existe, e varrer
    os endereços mapearia o sistema sem abrir nada."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    assert cliente.get("/painel/dre").status_code == 200
    assert cliente.get("/painel/analitico").status_code == 200
    assert cliente.get("/painel/fluxo").status_code == 404
    assert cliente.get("/painel/receita").status_code == 404


def test_o_topo_so_mostra_as_abas_que_abrem(parceiro, monkeypatch):
    """Aba que responde "não encontrado" ao ser clicada é pior que aba nenhuma:
    a pessoa acha que o sistema está com defeito."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    html = cliente.get("/painel/dre").get_data(as_text=True)
    assert "Fluxo de Caixa" not in html
    assert "Despesas Analítico" in html


def test_configuracoes_e_explorador_sao_so_do_dono(parceiro, monkeypatch):
    """Os dois escrevem — um aplica migração e dispara carga, o outro altera e
    exclui título no OMIE. Quem é preso a obra não escreve."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    assert cliente.get("/painel/configuracoes").status_code == 404
    assert cliente.get("/painel/explorador").status_code == 404
    assert cliente.post("/painel/explorador/excluir",
                        data={"codigo": "701"}).status_code == 404
    assert cliente.post("/painel/usuarios",
                        data={"acao": "criar"}).status_code == 404


# ===========================================================================
# 4. Falhar fechado
# ===========================================================================
def test_sem_obra_marcada_nao_entra(base_com_duas_obras, monkeypatch):
    """Cadastro pela metade não pode virar acesso total."""
    from app.apps.painel import usuarios
    usuarios.criar("semobra", "senha-dele", obras=[], telas=["dre"])
    cliente = _cliente(monkeypatch)
    r = _entrar(cliente, usuario="semobra", senha="senha-dele")
    assert r.status_code == 403
    assert "não tem obra ou tela liberada" in r.get_data(as_text=True)


def test_sem_tela_marcada_nao_entra(base_com_duas_obras, monkeypatch):
    from app.apps.painel import usuarios
    usuarios.criar("semtela", "senha-dele", obras=["OBRA DELE"], telas=[])
    cliente = _cliente(monkeypatch)
    assert _entrar(cliente, usuario="semtela", senha="senha-dele").status_code == 403


def test_tirar_a_obra_vale_na_hora(parceiro, monkeypatch):
    """Sem isso, tirar o acesso de alguém só valeria quando ele fechasse o
    navegador — e ninguém controla isso."""
    from app.apps.painel import usuarios
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    assert cliente.get("/painel/dre").status_code == 200
    usuarios.atualizar(parceiro, obras=[])
    assert cliente.get("/painel/dre").status_code == 404


def test_desativar_derruba_a_sessao(parceiro, monkeypatch):
    from app.apps.painel import usuarios
    cliente = _cliente(monkeypatch)
    _entrar(cliente, usuario="parceiro", senha="senha-dele")
    usuarios.atualizar(parceiro, ativo=False)
    assert cliente.get("/painel/dre").status_code in (302, 404)


def test_a_senha_do_dono_digitada_no_campo_de_usuario_nao_vira_administrador(
        parceiro, monkeypatch):
    """Os dois caminhos são separados no servidor de propósito."""
    cliente = _cliente(monkeypatch)
    r = _entrar(cliente, usuario="parceiro", senha=SENHA_MESTRE)
    assert r.status_code == 401


# ===========================================================================
# 5. E o dono continua vendo tudo
# ===========================================================================
def test_o_dono_continua_vendo_tudo(base_com_duas_obras, monkeypatch):
    """O conserto não pode ter fechado a porta do próprio dono."""
    cliente = _cliente(monkeypatch)
    _entrar(cliente)
    html = cliente.get("/painel/dre").get_data(as_text=True)
    # O DRE agrupa por categoria: as duas despesas são "Serviços", então o que
    # aparece é a soma. Ver 10.000 é ver as DUAS obras — que é o ponto.
    assert "10.000,00" in html, "o dono deixou de ver a empresa inteira"
    assert cliente.get("/painel/configuracoes").status_code == 200
    assert cliente.get("/painel/fluxo").status_code == 200
