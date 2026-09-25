# -*- coding: utf-8 -*-
"""
O CADASTRO DE ACESSO da Análise de SPs, com banco de verdade — 25/09/2026.

⚠️ ESTES TESTES SÃO OS QUE IMPORTAM AQUI. O dublê da suíte ignora `WHERE` e
índice único, e este cadastro é feito exatamente dessas duas coisas: achar a
pessoa pelo login (num `WHERE lower(usuario) = ?`) e impedir dois acessos com o
mesmo nome (num índice único). Um erro nisto não estoura: ele deixa alguém
entrar em tela que não é dele, o que é o pior tipo de defeito que este cadastro
pode ter.

E a pergunta mais importante deste arquivo não é "o cadastro grava?" — é **"o
que acontece quando alguém esquece de marcar uma tela?"**. A resposta tem de ser
sempre a mesma: não entra.
"""
import pytest
from flask import Flask

pytestmark = pytest.mark.banco

SENHA_MESTRE_OPERADOR = "mestre-operador-de-teste"
SENHA_MESTRE_CONSULTA = "mestre-consulta-de-teste"


@pytest.fixture
def banco_acesso(banco, monkeypatch):
    """Sobe o schema pelas migrações de verdade — as que o botão aplica."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_MESTRE_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_MESTRE_CONSULTA)
    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None

    pasta = __import__("pathlib").Path(db_analisesps.__file__).parent / "migracoes"
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        for caminho in sorted(pasta.glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.commit()
    db_analisesps._engine = None


@pytest.fixture
def app(banco_acesso):
    from app.apps.analisesps import web
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def criar(login="thiago", senha="senha-do-thiago", nome="THIAGO",
          telas=("solicitacoes",), pode_operar=False, mestre=False):
    from app.apps.analisesps import usuarios
    r = usuarios.criar(login, senha, nome=nome, telas=telas,
                       pode_operar=pode_operar, mestre=mestre)
    assert r.get("ok"), r
    return r["id"]


def criar_mestre(login="marcelo", senha="senha-do-dono", nome="MARCELO"):
    """O dono, cadastrado como mestre. Sem tela nenhuma marcada de propósito:
    mestre alcança todas, e é isso que estes testes provam."""
    return criar(login=login, senha=senha, nome=nome, telas=(), mestre=True)


# ---------------------------------------------------------------------------
# A migração e o cadastro
# ---------------------------------------------------------------------------
def test_a_migracao_023_roda_no_postgres(app):
    from app.apps.analisesps import usuarios
    assert usuarios._pronto() is True
    assert usuarios.listar() == []


def test_cadastra_e_encontra_pela_senha(app):
    from app.apps.analisesps import usuarios
    criar()
    pessoa = usuarios.buscar("thiago")
    assert pessoa["nome"] == "THIAGO"
    assert usuarios.senha_confere(pessoa, "senha-do-thiago")
    assert not usuarios.senha_confere(pessoa, "outra")


def test_a_senha_NAO_e_guardada_legivel(app):
    """⚠️ Nem o dono lê a senha de alguém depois — só troca. Este banco tem os
    pagamentos da empresa: senha legível aqui é vazamento esperando dia."""
    from app.apps.analisesps import db, usuarios
    criar(senha="segredo-do-thiago")
    guardado = db.consultar_um(
        "SELECT senha_hash FROM analisesps.usuarios WHERE usuario = ?",
        ("thiago",))[0]
    assert "segredo-do-thiago" not in guardado
    assert usuarios.senha_confere(usuarios.buscar("thiago"), "segredo-do-thiago")


def test_login_nao_repete_nem_com_maiuscula_diferente(app):
    """O índice único é `lower(usuario)`. Sem ele o dono cadastraria "Thiago" e
    "thiago" sem perceber, e um dos dois nunca entraria."""
    from app.apps.analisesps import usuarios
    criar(login="thiago")
    r = usuarios.criar("THIAGO", "outra-senha-boa", telas=("relatorio",))
    assert not r["ok"]
    assert "Já existe" in r["erro"]


def test_login_com_espaco_em_volta_e_a_mesma_pessoa(app):
    from app.apps.analisesps import usuarios
    criar(login="thiago")
    assert usuarios.buscar("  THIAGO  ")["usuario"] == "thiago"


def test_senha_do_mestre_e_recusada_no_cadastro(app):
    """Se passasse, a pessoa entraria como mestre e as telas marcadas para ela
    não valeriam nada — pareceria funcionar sem funcionar."""
    from app.apps.analisesps import usuarios
    r = usuarios.criar("karla", SENHA_MESTRE_OPERADOR, telas=("relatorio",))
    assert not r["ok"]
    assert "senhas gerais" in r["erro"]


def test_tela_inventada_nao_entra_no_cadastro(app):
    """Um pedido montado à mão não cria permissão que não existe."""
    from app.apps.analisesps import usuarios
    criar(telas=("solicitacoes", "tela-que-nao-existe", "configuracoes"))
    assert usuarios.buscar("thiago")["telas"] == ["solicitacoes"]


def test_configuracoes_nao_se_marca_para_ninguem(app):
    from app.apps.analisesps import usuarios
    criar(telas=("configuracoes",))
    assert usuarios.buscar("thiago")["telas"] == []


def test_desativado_desaparece_do_login(app):
    from app.apps.analisesps import usuarios
    uid = criar()
    usuarios.atualizar(uid, ativo=False)
    assert usuarios.buscar("thiago") is None
    # mas continua na lista da tela, marcado como desativado
    assert usuarios.listar()[0]["ativo"] is False


def test_trocar_a_senha_sem_mexer_no_resto(app):
    from app.apps.analisesps import usuarios
    uid = criar(telas=("relatorio", "calendario"), pode_operar=True)
    usuarios.atualizar(uid, senha="senha-nova-dele")
    pessoa = usuarios.buscar("thiago")
    assert usuarios.senha_confere(pessoa, "senha-nova-dele")
    assert not usuarios.senha_confere(pessoa, "senha-do-thiago")
    assert sorted(pessoa["telas"]) == ["calendario", "relatorio"]
    assert pessoa["pode_operar"] is True


def test_senha_em_branco_MANTEM_a_que_existe(app):
    """Salvar o cadastro para mudar uma tela não pode apagar a senha da pessoa
    — ela ficaria sem conseguir entrar, e ninguém ligaria uma coisa à outra."""
    from app.apps.analisesps import usuarios
    uid = criar()
    usuarios.atualizar(uid, senha="", telas=("relatorio",))
    assert usuarios.senha_confere(usuarios.buscar("thiago"), "senha-do-thiago")


def test_senha_curta_na_alteracao_nao_grava_nada(app):
    """Recusar depois de ter mudado as telas deixaria o cadastro pela metade."""
    from app.apps.analisesps import usuarios
    uid = criar(telas=("solicitacoes",))
    r = usuarios.atualizar(uid, senha="123", telas=("relatorio",))
    assert not r["ok"]
    assert usuarios.buscar("thiago")["telas"] == ["solicitacoes"]


def test_apagar_leva_as_telas_junto(app):
    from app.apps.analisesps import db, usuarios
    uid = criar(telas=("solicitacoes", "relatorio"))
    usuarios.apagar(uid)
    assert usuarios.buscar("thiago") is None
    assert db.consultar("SELECT 1 FROM analisesps.usuario_telas"
                        " WHERE usuario_id = ?", (uid,)) == []


def test_marcar_acesso_grava_a_hora(app):
    from app.apps.analisesps import usuarios
    uid = criar()
    assert usuarios.listar()[0]["ultimo_acesso"] is None
    usuarios.marcar_acesso(uid)
    assert usuarios.listar()[0]["ultimo_acesso"] is not None


# ---------------------------------------------------------------------------
# A ENTRADA
# ---------------------------------------------------------------------------
def test_entra_com_usuario_e_senha_proprios(app):
    criar(telas=("relatorio",))
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar",
                                data={"usuario": "thiago",
                                      "senha": "senha-do-thiago"})
    assert resposta.status_code in (301, 302)
    # vai para a PRIMEIRA TELA QUE ELE TEM, não para Solicitações: mandá-lo
    # para uma tela que ele não abre daria 404 logo depois de um login que
    # funcionou, e ele concluiria que o acesso não foi criado
    assert "/analisesps/relatorio" in resposta.headers["Location"]


def test_sem_nenhuma_tela_marcada_NAO_ENTRA(app):
    """⚠️ A pergunta mais importante deste arquivo. Lista vazia quer dizer
    NENHUMA, nunca "todas" — senão um cadastro esquecido pela metade viraria
    acesso total."""
    criar(telas=())
    with app.test_client() as cliente:
        html = cliente.post("/analisesps/entrar",
                            data={"usuario": "thiago",
                                  "senha": "senha-do-thiago"}
                            ).get_data(as_text=True)
    assert "nenhuma tela liberada" in html


def test_senha_errada_e_usuario_inexistente_dizem_A_MESMA_COISA(app):
    """Dizer qual dos dois falhou entrega metade da resposta a quem tenta."""
    criar()
    with app.test_client() as cliente:
        errada = cliente.post("/analisesps/entrar",
                              data={"usuario": "thiago", "senha": "chute"}
                              ).get_data(as_text=True)
        inexistente = cliente.post("/analisesps/entrar",
                                   data={"usuario": "ninguem",
                                         "senha": "chute"}).get_data(as_text=True)
    assert "Usuário ou senha incorretos" in errada
    assert "Usuário ou senha incorretos" in inexistente


def test_desativado_nao_entra(app):
    from app.apps.analisesps import usuarios
    uid = criar()
    usuarios.atualizar(uid, ativo=False)
    with app.test_client() as cliente:
        html = cliente.post("/analisesps/entrar",
                            data={"usuario": "thiago",
                                  "senha": "senha-do-thiago"}
                            ).get_data(as_text=True)
    assert "Usuário ou senha incorretos" in html


def test_o_mestre_entra_pela_senha_do_render_mesmo_com_cadastros(app):
    """⚠️ É o que impede o dono de se trancar para fora. Vale sempre."""
    criar()
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar",
                                data={"senha": SENHA_MESTRE_OPERADOR,
                                      "nome": "MARCELO"})
    assert resposta.status_code in (301, 302)


# ---------------------------------------------------------------------------
# AS TELAS — o que ele alcança e o que não
# ---------------------------------------------------------------------------
def entrar_como(cliente, login="thiago", senha="senha-do-thiago"):
    resposta = cliente.post("/analisesps/entrar",
                            data={"usuario": login, "senha": senha})
    assert resposta.status_code in (301, 302), resposta.get_data(as_text=True)


def test_abre_a_tela_que_e_dele_e_nao_abre_a_que_nao_e(app):
    criar(telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente)
        # a dele responde (200, ou o recado de base vazia — o que importa é
        # NÃO ser 404)
        assert cliente.get("/analisesps/relatorio").status_code != 404
        # a que não é dele responde "não encontrado"
        assert cliente.get("/analisesps/conciliacao").status_code == 404
        assert cliente.get("/analisesps/lote").status_code == 404


def test_fora_do_alcance_responde_NAO_ENCONTRADO_e_nao_sem_permissao(app):
    """Mesma regra do ERP e do painel: dizer "sem permissão" confirma que a
    tela existe, e varrer os endereços mapearia o sistema sem abrir nada."""
    criar(telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente)
        resposta = cliente.get("/analisesps/conciliacao")
    assert resposta.status_code == 404
    assert "não existe" in resposta.get_data(as_text=True)


def test_quem_tem_cadastro_nao_abre_CONFIGURACOES(app):
    """É de onde se aplica migração, se troca o certificado e se cadastra
    gente."""
    criar(telas=("relatorio",), pode_operar=True)
    with app.test_client() as cliente:
        entrar_como(cliente)
        assert cliente.get("/analisesps/configuracoes").status_code == 404


def test_quem_tem_cadastro_NAO_CRIA_OUTRO_ACESSO(app):
    """⚠️ O TESTE MAIS IMPORTANTE DESTE ARQUIVO.

    No painel isto passou batido na primeira versão: uma pessoa presa a uma
    obra conseguia criar outro acesso — inclusive um com todas as obras. Quem
    pode criar acesso pode dar a si mesmo tudo, então esta porta fechada é o
    que faz as outras valerem."""
    from app.apps.analisesps import usuarios
    criar(telas=("relatorio",), pode_operar=True)
    with app.test_client() as cliente:
        entrar_como(cliente)
        resposta = cliente.post("/analisesps/usuarios", data={
            "acao": "criar", "novo_usuario": "eu-mesmo-de-novo",
            "nova_senha": "senha-bem-boa", "tela_do_usuario": "conciliacao",
            "pode_operar": "1"})
    assert resposta.status_code == 404
    assert usuarios.buscar("eu-mesmo-de-novo") is None


def test_quem_tem_cadastro_nao_mexe_no_certificado_nem_lanca_aporte(app):
    criar(telas=("fiscal",), pode_operar=True)
    with app.test_client() as cliente:
        entrar_como(cliente)
        assert cliente.post("/analisesps/certificados/remover",
                            data={}).status_code == 404
        assert cliente.get("/analisesps/aportes").status_code == 404


def test_quem_nao_pode_operar_ve_mas_nao_altera(app):
    criar(telas=("conciliacao",), pode_operar=False)
    with app.test_client() as cliente:
        entrar_como(cliente)
        # a tela abre
        assert cliente.get("/analisesps/conciliacao").status_code != 404
        # a ação de alterar, não
        resposta = cliente.post("/analisesps/api/conciliacao/marcar",
                                json={"ids": [1], "conciliado": True})
        assert resposta.status_code == 403


def test_fora_do_alcance_responde_404_ANTES_de_falar_de_permissao(app):
    """⚠️ A ORDEM DAS DUAS CONFERÊNCIAS IMPORTA.

    Se a alçada fosse conferida primeiro, uma pessoa que só vê o Relatório
    receberia "sem permissão" ao tocar numa rota da Conciliação — e "sem
    permissão" confirma que a rota existe. Fora do alcance é "não encontrado";
    sem alçada, mas dentro do alcance, é "sem permissão", que é honesto."""
    criar(telas=("relatorio",), pode_operar=False)
    with app.test_client() as cliente:
        entrar_como(cliente)
        resposta = cliente.post("/analisesps/api/conciliacao/marcar",
                                json={"ids": [1], "conciliado": True})
    assert resposta.status_code == 404, (
        "quem não tem a tela não pode receber 403 — isso confirma que a rota "
        "existe")


def test_quem_pode_operar_passa_pela_trava_de_alteracao(app):
    """O 403 tem de sumir quando a marcação está lá. Sem este teste, um erro no
    sentido do `if` deixaria todo mundo sem alterar nada e pareceria seguro."""
    criar(telas=("conciliacao",), pode_operar=True)
    with app.test_client() as cliente:
        entrar_como(cliente)
        resposta = cliente.post("/analisesps/api/conciliacao/marcar",
                                json={"ids": [], "conciliado": True})
    assert resposta.status_code != 403


def test_TIRAR_uma_tela_vale_na_hora_e_nao_no_proximo_login(app):
    """As permissões são lidas do banco a cada pedido, de propósito. Guardá-las
    na sessão faria a pessoa continuar entrando na tela até fechar o navegador
    — o que é justamente o momento em que se tira o acesso de alguém."""
    from app.apps.analisesps import usuarios
    uid = criar(telas=("relatorio", "calendario"))
    with app.test_client() as cliente:
        entrar_como(cliente)
        assert cliente.get("/analisesps/calendario").status_code != 404
        usuarios.atualizar(uid, telas=("relatorio",))
        assert cliente.get("/analisesps/calendario").status_code == 404
        assert cliente.get("/analisesps/relatorio").status_code != 404


def test_APAGAR_a_pessoa_no_meio_da_sessao_a_derruba(app):
    from app.apps.analisesps import usuarios
    uid = criar(telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente)
        usuarios.apagar(uid)
        resposta = cliente.get("/analisesps/relatorio")
    assert resposta.status_code in (301, 302)
    assert "entrar" in resposta.headers["Location"]


def test_o_menu_dele_mostra_so_as_telas_dele(app):
    """Deixar no menu uma tela que responde 404 é pior do que não mostrá-la: a
    pessoa clica, não entende, e liga para o dono."""
    criar(telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente)
        html = cliente.get("/analisesps/relatorio").get_data(as_text=True)
    assert "/analisesps/relatorio" in html
    assert "/analisesps/conciliacao" not in html
    assert "/analisesps/configuracoes" not in html


def test_o_nome_do_cadastro_e_quem_assina_o_trabalho(app):
    """O nome é a chave do lote, dos filtros e do registro de quem alterou o
    quê. Quem entra por cadastro não escolhe nome na lista — o nome é o do
    cadastro, e é por isso que cadastrar com o mesmo nome de antes faz o lote
    da pessoa continuar sendo o dela."""
    from app.apps.analisesps import auth
    criar(nome="THIAGO", telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente)
        with cliente.session_transaction() as sessao:
            assert sessao[auth.CHAVE_NOME] == "THIAGO"


def test_sem_nome_no_cadastro_vale_o_login(app):
    """Melhor o login do que vazio: o registro de auditoria precisa dizer QUEM
    mexeu, e "—" não diz nada."""
    from app.apps.analisesps import auth
    criar(nome="", telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente)
        with cliente.session_transaction() as sessao:
            assert sessao[auth.CHAVE_NOME] == "thiago"


# ---------------------------------------------------------------------------
# A TELA DE CONFIGURAÇÕES, que é onde o dono cadastra
# ---------------------------------------------------------------------------
def test_o_mestre_cadastra_pela_tela(app):
    from app.apps.analisesps import usuarios
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR,
                                                 "nome": "MARCELO"})
        resposta = cliente.post("/analisesps/usuarios", data={
            "acao": "criar", "novo_usuario": "Karla", "nome": "KARLA",
            "nova_senha": "senha-da-karla", "pode_operar": "1",
            "tela_do_usuario": ["relatorio", "calendario"]})
    assert resposta.status_code in (301, 302)
    pessoa = usuarios.buscar("karla")
    assert pessoa["pode_operar"] is True
    assert sorted(pessoa["telas"]) == ["calendario", "relatorio"]


def test_o_erro_do_cadastro_volta_para_a_tela_em_portugues(app):
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR,
                                                 "nome": "MARCELO"})
        resposta = cliente.post("/analisesps/usuarios", data={
            "acao": "criar", "novo_usuario": "karla", "nova_senha": "123",
            "tela_do_usuario": "relatorio"})
        assert resposta.status_code in (301, 302)
        assert "erro_usuario" in resposta.headers["Location"]


def test_quem_so_consulta_nao_cadastra_ninguem(app):
    """A senha de consulta é do mestre também, mas ela não altera nada — e
    criar acesso é a alteração mais forte que existe neste módulo."""
    from app.apps.analisesps import usuarios
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_CONSULTA,
                                                 "nome": "MARCELO"})
        resposta = cliente.post("/analisesps/usuarios", data={
            "acao": "criar", "novo_usuario": "karla",
            "nova_senha": "senha-da-karla", "tela_do_usuario": "relatorio"})
    assert resposta.status_code == 403
    assert usuarios.buscar("karla") is None


def test_a_tela_de_configuracoes_lista_quem_tem_acesso(app):
    criar(nome="THIAGO", telas=("relatorio",))
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR,
                                                 "nome": "MARCELO"})
        html = cliente.get("/analisesps/configuracoes").get_data(as_text=True)
    assert "Quem tem acesso" in html
    assert "thiago" in html
    assert "Cadastrar uma pessoa" in html


# ---------------------------------------------------------------------------
# O MESTRE É UMA MARCAÇÃO NO CADASTRO — 25/09/2026
#
# *"Elimine do login o login via Nomes na lista da entrada. Vamos ficar somente
# com os cadastrados. Como ajustar o acesso master?"*
# ---------------------------------------------------------------------------
def test_o_mestre_cadastrado_abre_TUDO_sem_nenhuma_tela_marcada(app):
    """⚠️ Mestre alcança todas as telas por definição. Se dependesse das
    caixinhas, o dono cadastraria a si mesmo, esqueceria de marcar uma, e
    descobriria pelo 404 — provavelmente na tela que ele mais usa."""
    criar_mestre()
    with app.test_client() as cliente:
        entrar_como(cliente, "marcelo", "senha-do-dono")
        for caminho in ("/analisesps/relatorio", "/analisesps/conciliacao",
                        "/analisesps/lote", "/analisesps/configuracoes"):
            assert cliente.get(caminho).status_code != 404, caminho


def test_o_mestre_cadastrado_ALTERA_mesmo_sem_marcar_pode_operar(app):
    """As duas marcações não podem discordar: um mestre que não altera seria
    um administrador que não administra."""
    criar_mestre()
    with app.test_client() as cliente:
        entrar_como(cliente, "marcelo", "senha-do-dono")
        resposta = cliente.post("/analisesps/api/conciliacao/marcar",
                                json={"ids": [], "conciliado": True})
    assert resposta.status_code != 403


def test_o_mestre_cadastrado_CADASTRA_gente(app):
    """É o caminho normal a partir de agora — a porta de emergência deixa de
    ser necessária assim que existe um mestre."""
    from app.apps.analisesps import usuarios
    criar_mestre()
    with app.test_client() as cliente:
        entrar_como(cliente, "marcelo", "senha-do-dono")
        resposta = cliente.post("/analisesps/usuarios", data={
            "acao": "criar", "novo_usuario": "karla", "nome": "KARLA",
            "nova_senha": "senha-da-karla", "tela_do_usuario": "relatorio"})
    assert resposta.status_code in (301, 302)
    assert usuarios.buscar("karla") is not None


def test_quem_NAO_e_mestre_continua_sem_abrir_configuracoes(app):
    criar(telas=("relatorio",), pode_operar=True)
    with app.test_client() as cliente:
        entrar_como(cliente)
        assert cliente.get("/analisesps/configuracoes").status_code == 404


def test_o_ULTIMO_mestre_nao_pode_ser_apagado(app):
    """Apagar o único mestre deixa o sistema sem ninguém que cadastre ou
    configure, e o conserto passaria pela porta de emergência — que é
    justamente o que não se quer usar no dia a dia."""
    from app.apps.analisesps import usuarios
    uid = criar_mestre()
    r = usuarios.apagar(uid)
    assert not r["ok"]
    assert "único mestre" in r["erro"]
    assert usuarios.buscar("marcelo") is not None


def test_o_ULTIMO_mestre_nao_pode_se_desmarcar_nem_se_desativar(app):
    from app.apps.analisesps import usuarios
    uid = criar_mestre()
    assert not usuarios.atualizar(uid, mestre=False)["ok"]
    assert not usuarios.atualizar(uid, ativo=False)["ok"]
    assert usuarios.buscar("marcelo")["mestre"] is True


def test_com_DOIS_mestres_um_deles_pode_sair(app):
    """A trava é do ÚLTIMO, não de qualquer um — senão trocar de administrador
    viraria um problema."""
    from app.apps.analisesps import usuarios
    uid = criar_mestre()
    criar_mestre(login="thiago", senha="senha-do-thiago-2", nome="THIAGO")
    assert usuarios.apagar(uid)["ok"]
    assert usuarios.buscar("marcelo") is None
    assert usuarios.buscar("thiago")["mestre"] is True


def test_ha_mestre_responde_certo_nos_dois_estados(app):
    """É o que decide o recado da tela de entrada — sem ele, quem aplicasse a
    atualização do banco ficaria olhando um login sem saber por onde começar."""
    from app.apps.analisesps import usuarios
    assert usuarios.ha_mestre() is False
    criar(telas=("relatorio",))                  # gente comum não conta
    assert usuarios.ha_mestre() is False
    criar_mestre()
    assert usuarios.ha_mestre() is True


def test_a_tela_de_entrada_deixa_de_avisar_quando_ja_ha_mestre(app):
    with app.test_client() as cliente:
        antes = cliente.get("/analisesps/entrar").get_data(as_text=True)
    assert "Ainda não há ninguém cadastrado como mestre" in antes
    criar_mestre()
    with app.test_client() as cliente:
        depois = cliente.get("/analisesps/entrar").get_data(as_text=True)
    assert "Ainda não há ninguém cadastrado como mestre" not in depois


def test_a_porta_de_emergencia_continua_abrindo_com_mestre_cadastrado(app):
    """Ela não é desligada por haver mestre: o caso que ela resolve é
    justamente o mestre ter se perdido."""
    criar_mestre()
    with app.test_client() as cliente:
        resposta = cliente.post("/analisesps/entrar",
                                data={"senha": SENHA_MESTRE_OPERADOR})
        assert resposta.status_code in (301, 302)
        assert cliente.get("/analisesps/configuracoes").status_code == 200


def test_a_tela_mostra_quem_e_mestre(app):
    criar_mestre()
    criar(telas=("relatorio",))
    with app.test_client() as cliente:
        entrar_como(cliente, "marcelo", "senha-do-dono")
        html = cliente.get("/analisesps/configuracoes").get_data(as_text=True)
    assert "MESTRE" in html
    assert "É mestre" in html          # a caixinha de marcar
