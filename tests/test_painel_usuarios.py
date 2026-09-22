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


def test_o_dono_entra_mesmo_com_o_campo_de_usuario_preenchido(parceiro, monkeypatch):
    """O conserto de 22/09/2026, e o motivo dele.

    Enquanto o caminho era escolhido pelo campo de usuário estar VAZIO, o
    gerenciador de senhas do navegador — que guardara a senha do dono de quando
    esta tela tinha um campo só — passou a preencher o campo novo sozinho. O
    dono digitava a senha certa e ouvia "usuário ou senha incorretos", sem ter
    como adivinhar o que estava acontecendo. Ficou trancado para fora do
    próprio painel.

    Agora quem decide é a SENHA. Isso não afrouxa nada: quem conhece a senha
    mestre já é o administrador, e a única brecha que isso abriria — uma pessoa
    presa a obra com a mesma senha do dono — está fechada no cadastro, no teste
    seguinte."""
    cliente = _cliente(monkeypatch)
    r = _entrar(cliente, usuario="parceiro", senha=SENHA_MESTRE)
    assert r.status_code in (200, 302), "o dono ficou trancado para fora"
    assert cliente.get("/painel/configuracoes").status_code == 200, \
        "entrou, mas não como administrador"


def test_ninguem_pode_ter_a_senha_do_dono(base_com_duas_obras, monkeypatch):
    """Se pudesse, essa pessoa entraria como administrador — porque é a senha
    que decide. Fecha-se onde custa nada: no cadastro."""
    monkeypatch.setenv("PAINEL_SENHA", SENHA_MESTRE)
    from app.apps.painel import usuarios
    r = usuarios.criar("outro", SENHA_MESTRE, obras=["OBRA DELE"], telas=["dre"])
    assert r["ok"] is False
    assert "senha do dono" in r["erro"]


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


# ===========================================================================
# 6. Trocar a senha e mudar as obras, pela tela
# ===========================================================================
# O dono, sobre o fluxo: "eu cadastro o usuário e a senha e dou para ela, e
# ponto final". É isso mesmo — e a consequência de guardar a senha embaralhada
# é que quem esquece não tem a senha consultada: ganha uma nova. Dois cliques.

@pytest.fixture()
def cliente_dono(parceiro, monkeypatch):
    cliente = _cliente(monkeypatch)
    _entrar(cliente)
    return cliente


def test_o_dono_troca_a_senha_de_quem_esqueceu(parceiro, cliente_dono, monkeypatch):
    from app.apps.painel import usuarios
    cliente_dono.post("/painel/usuarios", data={
        "acao": "salvar", "usuario_id": str(parceiro), "nome": "Parceiro",
        "nova_senha": "senha-nova-dele", "ativo": "1",
        "obra_do_usuario": "OBRA DELE", "tela_do_usuario": "dre"})

    pessoa = usuarios.buscar("parceiro")
    assert usuarios.senha_confere(pessoa, "senha-nova-dele") is True
    assert usuarios.senha_confere(pessoa, "senha-dele") is False, \
        "a senha antiga não pode continuar valendo"

    # e a pessoa entra com a nova
    outro = _cliente(monkeypatch)
    assert _entrar(outro, usuario="parceiro",
                   senha="senha-nova-dele").status_code == 302


def test_senha_em_branco_mantem_a_que_existe(parceiro, cliente_dono):
    """Salvar as obras sem querer trocar a senha é o caso comum. Se o campo
    vazio apagasse a senha, o dono trancaria a pessoa do lado de fora sem saber."""
    from app.apps.painel import usuarios
    cliente_dono.post("/painel/usuarios", data={
        "acao": "salvar", "usuario_id": str(parceiro), "nome": "Outro Nome",
        "nova_senha": "", "ativo": "1",
        "obra_do_usuario": "OBRA DELE", "tela_do_usuario": "dre"})
    assert usuarios.senha_confere(usuarios.buscar("parceiro"), "senha-dele")


def test_o_dono_muda_as_obras_pela_tela(parceiro, cliente_dono, monkeypatch):
    cliente_dono.post("/painel/usuarios", data={
        "acao": "salvar", "usuario_id": str(parceiro), "nome": "Parceiro",
        "nova_senha": "", "ativo": "1",
        "obra_do_usuario": "OBRA DE OUTRO", "tela_do_usuario": "dre"})

    dele = _cliente(monkeypatch)
    _entrar(dele, usuario="parceiro", senha="senha-dele")
    html = dele.get("/painel/dre").get_data(as_text=True)
    assert "9.000,00" in html, "passou a ver a obra nova"
    assert "1.000,00" not in html, "e deixou de ver a antiga"


def test_desmarcar_todas_as_obras_tira_o_acesso_sem_apagar(parceiro, cliente_dono,
                                                           monkeypatch):
    """Tirar o acesso sem perder o cadastro — útil quando a parceria pausa."""
    cliente_dono.post("/painel/usuarios", data={
        "acao": "salvar", "usuario_id": str(parceiro), "nome": "Parceiro",
        "nova_senha": "", "ativo": "1", "tela_do_usuario": "dre"})
    dele = _cliente(monkeypatch)
    assert _entrar(dele, usuario="parceiro",
                   senha="senha-dele").status_code == 403


def test_a_tela_oferece_alterar_cada_pessoa(parceiro, cliente_dono):
    """Sem isso, trocar a senha de quem esqueceu exigiria apagar e recadastrar."""
    html = cliente_dono.get("/painel/configuracoes").get_data(as_text=True)
    assert "Alterar parceiro" in html
    assert "em branco mantém a atual" in html
