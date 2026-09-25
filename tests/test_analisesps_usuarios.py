"""Análise de SPs — cadastro de acesso: usuário, senha e telas.

Pedido do dono em 25/09/2026: *"vou poder cadastrar o operador, definir a senha,
definir as telas que ele tem acesso. Aí vai ter um usuário master, e os outros a
gente define as permissões."*

Estes testes RODAM SEM BANCO. Eles fixam as decisões que não dependem de dado
gravado: o mapa de qual tela é cada rota (que é o coração da permissão), quem é
só do mestre, e o que a tela de entrada oferece. O que precisa de banco está em
`test_analisesps_usuarios_banco.py`.
"""
from __future__ import annotations

import pytest
from flask import Flask

from app.apps.analisesps import auth, usuarios, web

SENHA_OPERADOR = "operador-de-teste"
SENHA_CONSULTA = "consulta-de-teste"


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


# ---------------------------------------------------------------------------
# O MAPA — é o coração da permissão, e por isso tem inventário
# ---------------------------------------------------------------------------
def test_toda_rota_esta_classificada(app):
    """⚠️ ESTE É O TESTE QUE TORNA A REGRA DURÁVEL.

    Quem acrescentar uma rota e esquecer de dizer de que tela ela é vai
    encontrar a suíte vermelha — não um 404 na cara de alguém que tinha
    permissão. Sem este inventário, "o padrão é negar" viraria armadilha para
    quem só quis criar uma tela nova."""
    faltando = []
    for regra in app.url_map.iter_rules():
        endpoint = regra.endpoint
        if not endpoint.startswith("analisesps."):
            continue
        funcao = app.view_functions.get(endpoint)
        exigencia = getattr(funcao, "_analisesps_exigencia", None)
        if isinstance(exigencia, tuple) or endpoint == auth.ENDPOINT_ESTILO:
            continue                        # pública, com motivo escrito
        if auth.e_so_do_mestre(endpoint) or endpoint in auth.APOIO:
            continue
        if auth.telas_da_rota(endpoint) is None:
            faltando.append(endpoint)
    assert not faltando, (
        "Estas rotas não estão em TELA_DA_ROTA, em APOIO nem na lista do "
        "mestre (auth.py): " + ", ".join(sorted(faltando)) + ". Enquanto "
        "isso elas ficam FECHADAS para quem tem cadastro próprio.")


def test_toda_rota_de_apoio_tem_motivo_escrito():
    """Ser exceção ao padrão de negar é decisão, e decisão tem justificativa."""
    for endpoint, motivo in auth.APOIO.items():
        assert motivo.strip(), f"A rota de apoio {endpoint} não tem motivo."


def test_a_tela_nao_aponta_para_permissao_que_nao_existe(app):
    """Uma tela escrita errado no mapa nunca seria marcável — e a rota ficaria
    fechada para todo mundo, sem ninguém entender."""
    validas = {chave for chave, _rotulo, _rota in web.TELAS}
    for endpoint, telas in auth.TELA_DA_ROTA.items():
        for tela in telas:
            assert tela in validas, (
                f"A rota {endpoint} aponta para a tela '{tela}', que não existe "
                "em web.TELAS.")
    for _prefixo, telas in auth.TELA_POR_PREFIXO:
        for tela in telas:
            assert tela in validas


def test_configuracoes_nunca_e_liberavel(app):
    """É de onde se aplica migração, se troca o certificado e se cadastra
    gente. Se um dia ela entrar na lista de marcáveis, é decisão consciente —
    não um descuido de quem mexeu na lista de telas."""
    with app.app_context():
        assert "configuracoes" not in usuarios.chaves_liberaveis()


def test_as_telas_liberaveis_saem_da_MESMA_lista_do_menu(app):
    """Se as duas listas fossem escritas à mão, uma tela nova apareceria no
    menu e não no cadastro — e ninguém descobriria até alguém reclamar."""
    with app.app_context():
        liberaveis = usuarios.telas_liberaveis()
    do_menu = [(c, r) for c, r, _ in web.TELAS
               if c not in auth.SO_DO_MESTRE_POR_TELA]
    assert liberaveis == do_menu


@pytest.mark.parametrize("endpoint", [
    "analisesps.configuracoes",
    "analisesps.migrar",
    "analisesps.subir_certificado",
    "analisesps.remover_certificado",
    "analisesps.gravar_pessoas",
    "analisesps.tela_aportes",
    "analisesps.aportes_gravar",
    "analisesps.usuarios_salvar",
    "analisesps.tela_credores",
])
def test_o_que_e_so_do_mestre(endpoint):
    """Configurar o módulo, mexer no certificado, lançar no OMIE e criar outro
    acesso não são de quem tem cadastro. O último é o mais importante: sem ele,
    uma pessoa presa a uma tela criaria outro acesso com todas."""
    assert auth.e_so_do_mestre(endpoint)


def test_conferir_nota_fiscal_nao_cai_na_lista_do_mestre():
    """Armadilha de prefixo: `conferir_certificado` e `conferir_drive` são do
    mestre, mas `conferir_nota_fiscal` é da tela Doc. Fiscal. Um prefixo
    "analisesps.conferir_" teria fechado a nota para todo mundo."""
    assert not auth.e_so_do_mestre("analisesps.conferir_nota_fiscal")
    assert auth.telas_da_rota("analisesps.conferir_nota_fiscal") == ("fiscal",)


def test_a_conciliacao_inteira_e_uma_tela_so():
    """São vinte rotas e vão ser mais. Todas pedem a mesma tela — por prefixo,
    para a próxima já nascer coberta."""
    for endpoint in ("analisesps.tela_conciliacao",
                     "analisesps.conciliacao_panorama",
                     "analisesps.conciliacao_marcar",
                     "analisesps.conciliacao_omie_lancar",
                     "analisesps.conciliacao_desfazer"):
        assert auth.telas_da_rota(endpoint) == ("conciliacao",)


def test_rota_inventada_nao_tem_tela():
    """O padrão é negar: rota que ninguém classificou não vale nada."""
    assert auth.telas_da_rota("analisesps.tela_que_alguem_criou_ontem") is None


# ---------------------------------------------------------------------------
# O menu mostra só o que a pessoa alcança
# ---------------------------------------------------------------------------
def test_o_mestre_ve_o_menu_inteiro(app):
    with app.test_request_context("/analisesps/"):
        from flask import session
        session[auth.CHAVE_SESSAO] = auth.OPERADOR
        assert auth.telas_permitidas() is None
        assert auth.telas_para_o_menu(web.TELAS) == list(web.TELAS)


def test_menu_de_quem_nao_esta_logado_nao_quebra(app):
    """A tela de erro e a de entrada também passam pelo processador de
    contexto. Se ele estourasse sem sessão, o login pararia de abrir."""
    with app.test_request_context("/analisesps/entrar"):
        assert auth.telas_permitidas() is None


# ---------------------------------------------------------------------------
# A tela de entrada
# ---------------------------------------------------------------------------
def test_a_entrada_e_SO_usuario_e_senha(app):
    """A lista de nomes acabou em 25/09/2026 — pedido do dono, com todas as
    letras. Quem entra, entra pelo cadastro."""
    with app.test_client() as cliente:
        html = cliente.get("/analisesps/entrar").get_data(as_text=True)
    assert 'name="usuario"' in html
    assert 'name="senha"' in html
    assert 'name="nome"' not in html, "o campo de nome voltou"
    assert "<select" not in html, "a lista de nomes voltou"


def test_a_entrada_explica_como_criar_o_PRIMEIRO_mestre(app):
    """Sem este recado, quem aplicasse a atualização do banco ficaria olhando
    uma tela de login sem nenhum cadastro criado, sem saber por onde começar.
    Ele só aparece enquanto não existe mestre nenhum."""
    with app.test_client() as cliente:
        html = cliente.get("/analisesps/entrar").get_data(as_text=True)
    assert "usuário em branco" in html
    assert "É mestre" in html or "mestre" in html


def test_a_senha_geral_continua_entrando_mesmo_com_usuario_preenchido(app):
    """⚠️ ISTO É O CONSERTO DE UM JEITO DE TRANCAR O DONO PARA FORA.

    No painel, em 22/09/2026, o gerenciador de senhas do navegador preencheu o
    campo novo sozinho: o pedido caía no caminho do cadastro e a resposta era
    "usuário ou senha incorretos" — com a senha certa digitada. A senha geral é
    conferida PRIMEIRO justamente para isso não se repetir aqui."""
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar", data={
            "senha": SENHA_OPERADOR, "nome": "MARCELO",
            "usuario": "o-navegador-preencheu-isto"})
    assert resposta.status_code in (301, 302), (
        "a senha geral tem de entrar mesmo com o campo de usuário preenchido")


def test_a_porta_de_emergencia_nao_pede_nome_nenhum(app):
    """Ela existe para destravar quem perdeu o acesso — pedir uma escolha a
    mais seria só mais uma coisa para dar errado na pior hora."""
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar",
                                data={"senha": SENHA_OPERADOR})
    assert resposta.status_code in (301, 302)


def test_senha_errada_sem_usuario_recusa_e_ensina_a_emergencia(app):
    """A recusa tem de dizer o que fazer. Quem cuida do sistema e perdeu o
    acesso precisa descobrir a porta de emergência AQUI — não num chat."""
    with app.test_client() as cliente:
        html = cliente.post("/analisesps/entrar",
                            data={"senha": "chute"}).get_data(as_text=True)
    assert "Digite o seu usuário e a sua senha" in html
    assert "usuário em branco" in html


# ---------------------------------------------------------------------------
# Regras da senha, que não dependem de banco
# ---------------------------------------------------------------------------
def test_a_senha_do_mestre_nao_pode_virar_senha_de_ninguem(app):
    """Se pudesse, a pessoa entraria como mestre e as telas marcadas para ela
    não valeriam nada — o cadastro pareceria funcionar e não funcionaria."""
    with app.app_context():
        assert usuarios._e_senha_do_mestre(SENHA_OPERADOR)
        assert usuarios._e_senha_do_mestre(SENHA_CONSULTA)
        assert not usuarios._e_senha_do_mestre("uma-senha-qualquer")


def test_senha_curta_e_recusada_com_frase_de_gente(app):
    with app.app_context():
        erro = usuarios._conferir_senha("12345")
    assert "pelo menos" in erro


def test_login_e_normalizado():
    """"Thiago", "THIAGO " e " thiago" são a mesma pessoa — senão o dono
    cadastraria dois acessos sem perceber."""
    assert usuarios.normalizar_login("  THIAGO ") == "thiago"
    assert usuarios.normalizar_login("Thiago") == "thiago"
    assert usuarios.normalizar_login(None) == ""


def test_login_gigante_e_cortado():
    assert len(usuarios.normalizar_login("x" * 500)) == usuarios.MAX_LOGIN


def test_senha_nunca_confere_para_quem_nao_existe():
    assert not usuarios.senha_confere(None, "qualquer")
    assert not usuarios.senha_confere({}, "qualquer")
    assert not usuarios.senha_confere({"senha_hash": ""}, "")


def test_hash_corrompido_e_senha_que_nao_confere():
    """Hash quebrado no banco não pode virar erro 500 na tela de entrada."""
    assert not usuarios.senha_confere({"usuario": "x", "senha_hash": "lixo"},
                                      "qualquer")
