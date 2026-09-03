"""Análise de SPs — o padrão de autorização é NEGAR.

O teste de inventário é a rede que torna a regra durável: quem acrescentar uma
rota sem declarar o que ela exige quebra a suíte, em vez de deixar uma brecha
descoberta em produção. Foi assim que a folha de estilo apareceu bloqueada
antes de qualquer publicação.

Rodam SEM banco. O guarda decide antes de o handler abrir conexão — e é
justamente essa ordem que estes testes fixam: se um dia alguém mover a checagem
para depois, o teste que exige 403 (e não 500) passa a falhar.
"""
from __future__ import annotations

import pytest
from flask import Flask

from app.apps.analisesps import auth, web


SENHA_OPERADOR = "operador-de-teste"
SENHA_CONSULTA = "consulta-de-teste"


@pytest.fixture
def app(monkeypatch):
    """App mínimo, só com este blueprint.

    Não usa `create_app()` de propósito: ele importa os outros 15 blueprints,
    que puxam gspread, dropbox e openai — dependências que este teste não
    precisa e que só o deixariam lento e frágil.
    """
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def entrar(cliente, senha):
    resposta = cliente.post("/analisesps/entrar", data={"senha": senha})
    assert resposta.status_code in (301, 302), "o login deveria ter funcionado"


# ---------------------------------------------------------------------------
# Inventário — nenhuma rota fica sem declarar
# ---------------------------------------------------------------------------
def test_toda_rota_declara_o_que_exige(app):
    """Sem declaração, o guarda fecha. Este teste garante que ninguém precise
    descobrir isso em produção."""
    sem_declaracao = []
    for regra in app.url_map.iter_rules():
        if not regra.endpoint.startswith("analisesps."):
            continue
        if regra.endpoint == auth.ENDPOINT_ESTILO:
            continue                      # o Flask cria; liberada com motivo
        funcao = app.view_functions[regra.endpoint]
        if not hasattr(funcao, "_analisesps_exigencia"):
            sem_declaracao.append(regra.endpoint)

    assert not sem_declaracao, (
        "Estas rotas não declararam exigência de acesso: "
        + ", ".join(sem_declaracao)
        + ". Ponha @exige_consulta, @exige_operador ou @publica('motivo').")


def test_toda_rota_publica_tem_motivo_escrito(app):
    """Ser pública é decisão, não descuido — e decisão tem justificativa."""
    for regra in app.url_map.iter_rules():
        if not regra.endpoint.startswith("analisesps."):
            continue
        funcao = app.view_functions.get(regra.endpoint)
        exigencia = getattr(funcao, "_analisesps_exigencia", None)
        if isinstance(exigencia, tuple):
            assert exigencia[1].strip(), (
                f"A rota {regra.endpoint} é pública sem motivo escrito.")


# ---------------------------------------------------------------------------
# Sem login não se alcança nada
# ---------------------------------------------------------------------------
TODAS_AS_TELAS = [
    ("GET", "/analisesps/"),
    ("GET", "/analisesps/solicitacoes"),
    ("GET", "/analisesps/sp/123"),
    ("GET", "/analisesps/lote"),
    ("POST", "/analisesps/lote"),
    ("GET", "/analisesps/relatorio"),
    ("GET", "/analisesps/auditoria"),
    ("GET", "/analisesps/ratear"),
    ("POST", "/analisesps/ratear"),
    ("GET", "/analisesps/bradesco"),
    ("POST", "/analisesps/bradesco"),
    ("GET", "/analisesps/agenda"),
    ("GET", "/analisesps/log"),
    ("GET", "/analisesps/codigos"),
    ("GET", "/analisesps/configuracoes"),
    ("GET", "/analisesps/exportar"),
    ("GET", "/analisesps/relatorio/exportar"),
    ("GET", "/analisesps/auditoria/exportar"),
    ("GET", "/analisesps/lote/exportar"),
    ("GET", "/analisesps/relatorio/pdf"),
    ("GET", "/analisesps/lote/pdf"),
    ("GET", "/analisesps/api/andamento"),
    ("POST", "/analisesps/api/alterar"),
    ("POST", "/analisesps/api/migrar"),
    ("GET", "/analisesps/sair"),
]


@pytest.mark.parametrize("metodo,url", TODAS_AS_TELAS)
def test_sem_login_manda_para_a_entrada(app, metodo, url):
    with app.test_client() as cliente:
        resposta = cliente.open(url, method=metodo, json={})
    assert resposta.status_code in (301, 302)
    assert "entrar" in resposta.headers.get("Location", "")


def test_a_lista_de_telas_cobre_todas_as_rotas(app):
    """Este teste é a rede do teste acima: acrescentar uma tela e esquecer de
    conferir que ela exige login passaria despercebido sem ele."""
    testadas = {url for _, url in TODAS_AS_TELAS}
    faltando = []
    for regra in app.url_map.iter_rules():
        if not regra.endpoint.startswith("analisesps."):
            continue
        funcao = app.view_functions.get(regra.endpoint)
        exigencia = getattr(funcao, "_analisesps_exigencia", None)
        if isinstance(exigencia, tuple) or regra.endpoint == auth.ENDPOINT_ESTILO:
            continue                          # pública, com motivo escrito
        caminho = str(regra).replace("<sp_id>", "123").replace(
            "<path:filename>", "x")
        if caminho not in testadas:
            faltando.append(caminho)
    assert not faltando, (
        "Estas rotas não estão na lista TODAS_AS_TELAS e portanto ninguém "
        "conferiu que exigem login: " + ", ".join(sorted(faltando)))


def test_sincronizar_sem_segredo_e_sem_sessao_e_recusada(app):
    """A rota da máquina é `@publica` no guarda porque um agendador não tem
    sessão. A autenticação dela acontece dentro — e este teste é o que impede
    que ela vire uma porta aberta se alguém mexer lá."""
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/api/sincronizar", json={})
    assert resposta.status_code == 403


def test_sincronizar_com_segredo_errado_e_recusada(app, monkeypatch):
    monkeypatch.setenv("ANALISESPS_SECRET", "o-segredo-certo")
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/api/sincronizar",
                                json={"secret": "chute"})
    assert resposta.status_code == 403


# ---------------------------------------------------------------------------
# Sem senha configurada, ninguém entra — nem com senha em branco
# ---------------------------------------------------------------------------
def test_sem_senha_no_ambiente_ninguem_entra(app, monkeypatch):
    """Falha FECHADO. Uma variável esquecida no Render não pode transformar os
    pagamentos da empresa numa página aberta a quem descobrir o endereço."""
    monkeypatch.delenv("ANALISESPS_SENHA_OPERADOR", raising=False)
    monkeypatch.delenv("ANALISESPS_SENHA_CONSULTA", raising=False)
    assert auth.perfis_configurados() == []
    for tentativa in ("", "qualquer", None):
        assert auth.identificar(tentativa) is None

    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar", data={"senha": ""})
    assert resposta.status_code == 200          # continua na tela de entrada


def test_senha_errada_nao_entra(app):
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar", data={"senha": "chute"})
    assert resposta.status_code == 200
    assert auth.identificar("chute") is None


def test_cada_senha_leva_ao_seu_perfil(app):
    assert auth.identificar(SENHA_OPERADOR) == auth.OPERADOR
    assert auth.identificar(SENHA_CONSULTA) == auth.CONSULTA


def test_senhas_iguais_dao_o_perfil_de_maior_poder(app, monkeypatch):
    """Se as duas variáveis forem preenchidas com o mesmo valor por descuido,
    quem entra recebe Operador — o perfil que o dono espera ao digitar o que
    ele considera a senha principal. O aviso sai no log."""
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_OPERADOR)
    assert auth.identificar(SENHA_OPERADOR) == auth.OPERADOR


# ---------------------------------------------------------------------------
# Consulta vê, mas não altera
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("metodo,url", [
    ("POST", "/analisesps/api/alterar"),
    ("POST", "/analisesps/api/migrar"),
])
def test_consulta_nao_alcanca_rota_de_operador(app, metodo, url):
    """403, e antes de encostar no banco.

    Se um dia a checagem for movida para depois da consulta, este teste passa a
    receber 500 em vez de 403 e falha — que é exatamente o alarme desejado."""
    with app.test_client() as cliente:
        entrar(cliente, SENHA_CONSULTA)
        resposta = cliente.open(url, method=metodo, json={})
    assert resposta.status_code == 403


def test_consulta_dispara_sincronizacao_e_e_recusada(app):
    with app.test_client() as cliente:
        entrar(cliente, SENHA_CONSULTA)
        resposta = cliente.post("/analisesps/api/sincronizar", json={})
    assert resposta.status_code == 403


def test_sair_apaga_a_sessao(app):
    with app.test_client() as cliente:
        entrar(cliente, SENHA_OPERADOR)
        cliente.get("/analisesps/sair")
        resposta = cliente.get("/analisesps/solicitacoes")
    assert resposta.status_code in (301, 302)
    assert "entrar" in resposta.headers.get("Location", "")


# ---------------------------------------------------------------------------
# O destino depois do login não pode apontar para fora
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("destino", [
    "https://exemplo-malicioso.com",
    "//exemplo-malicioso.com",
    "/erp/configuracoes",
])
def test_login_nao_redireciona_para_fora_do_modulo(app, destino):
    """Sem isto, o login da empresa viraria trampolim: bastaria mandar a alguém
    um endereço de entrada que jogasse noutro lugar depois da senha digitada."""
    with app.test_client() as cliente:
        resposta = cliente.post(f"/analisesps/entrar?proximo={destino}",
                                data={"senha": SENHA_OPERADOR})
    assert resposta.status_code in (301, 302)
    assert resposta.headers["Location"].endswith("/analisesps/solicitacoes")


def test_saude_responde_sem_login_e_nao_vaza_dado(app):
    """Serve para saber qual versão está no ar. Não pode contar mais que isso."""
    with app.test_client() as cliente:
        resposta = cliente.get("/analisesps/saude")
    assert resposta.status_code == 200
    corpo = resposta.get_json()
    assert corpo["modulo"] == "analisesps"
    assert set(corpo) == {"ok", "modulo", "versao", "senhas_configuradas"}
    assert SENHA_OPERADOR not in resposta.get_data(as_text=True)
