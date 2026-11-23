"""UMA CONTA DE E-MAIL POR ÁREA DA EMPRESA — com banco de verdade.

Pergunta dele em 18/11/2026: *"A empresa tem áreas diferentes. O que eu vou
encaminhar a partir do DP é uma coisa, é um e-mail. O que eu vou encaminhar a
partir do suprimento é outro e-mail. E a parte de sistema (…) é outro e-mail. E
eu acho que a gente não está tendo essa diferenciação."*

Ele estava certo: os eixos eram EMPRESA (CNPJ) e SISTEMA, e a ÁREA não era eixo
nenhum — o pedido à contabilidade e a cotação ao fornecedor saíam do mesmo
endereço.

POR QUE com banco de verdade: as contas moram em `parametros`, e o que se quer
provar é que a conta de uma área **não vaza para a outra** — duas linhas da
mesma tabela que não podem se misturar. E que a conta antiga do sistema
continua valendo sem migração nenhuma.

Cobre:
  1. cada área guarda a sua conta, e uma não vê a da outra;
  2. a conta do SISTEMA continua nas chaves antigas (nada a migrar);
  3. a senha é guardada CIFRADA e não volta na leitura;
  4. o DP manda pela conta DELE quando ela existe;
  5. e pela da empresa quando ela não existe — a volta que impede o DP de
     parar quando isto for publicado;
  6. a frase do "o que falta" manda a pessoa para a tela CERTA de cada área;
  7. área inventada é recusada.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.comum import email_sistema as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao

pytestmark = pytest.mark.banco

CONTA = {"servidor": "smtp.exemplo.com", "porta": 587, "seguranca": "STARTTLS",
         "senha": "senha-que-nao-aparece"}


@pytest.fixture(autouse=True)
def _cofre(monkeypatch):
    """A senha de SMTP é guardada cifrada, e sem a chave o `cifrar()` recusa —
    de propósito (`core/comum/segredos.py`). Aqui a chave é descartável."""
    from cryptography.fernet import Fernet

    from app.apps.erp.core.comum import segredos
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", Fernet.generate_key().decode())
    segredos._cofre.cache_clear() if hasattr(segredos._cofre, "cache_clear") else None
    yield
    if hasattr(segredos._cofre, "cache_clear"):
        segredos._cofre.cache_clear()


def test_cada_area_guarda_a_sua_conta(sessao_real):
    svc.configurar(sessao_real, {**CONTA, "usuario": "sistema@bws.teste",
                                 "remetente": "ERP BWS"}, area="SISTEMA")
    svc.configurar(sessao_real, {**CONTA, "usuario": "dp@bws.teste",
                                 "remetente": "DP — BWS"}, area="DP")

    sistema = svc.configuracao(sessao_real, "SISTEMA")
    dp = svc.configuracao(sessao_real, "DP")

    assert sistema["usuario"] == "sistema@bws.teste"
    assert dp["usuario"] == "dp@bws.teste"
    assert sistema["usavel"] and dp["usavel"]
    # ⚠️ O QUE IMPORTA: uma não é a outra.
    assert svc.conta(sessao_real, "DP").email == "dp@bws.teste"
    assert svc.conta(sessao_real, "SISTEMA").email == "sistema@bws.teste"


def test_configurar_uma_area_nao_mexe_na_outra(sessao_real):
    svc.configurar(sessao_real, {**CONTA, "usuario": "sistema@bws.teste"},
                   area="SISTEMA")
    svc.configurar(sessao_real, {**CONTA, "usuario": "dp@bws.teste"}, area="DP")

    svc.configurar(sessao_real, {"usuario": "dp-novo@bws.teste"}, area="DP")

    assert svc.configuracao(sessao_real, "SISTEMA")["usuario"] == "sistema@bws.teste"
    assert svc.configuracao(sessao_real, "DP")["usuario"] == "dp-novo@bws.teste"


def test_a_conta_do_sistema_continua_nas_chaves_antigas(sessao_real):
    """Sem isto, publicar esta mudança faria o ERP ESQUECER a conta que já
    estava configurada em produção — e o convite de acesso pararia de sair sem
    ninguém relacionar uma coisa com a outra."""
    svc.configurar(sessao_real, {**CONTA, "usuario": "sistema@bws.teste"},
                   area="SISTEMA")

    guardado = sessao_real.execute(text(
        "SELECT valor FROM parametros WHERE chave = 'email_sistema_usuario'"
    )).scalar()

    assert guardado == "sistema@bws.teste"
    # … e a chamada SEM área continua respondendo pelo sistema, como antes
    assert svc.configuracao(sessao_real)["usuario"] == "sistema@bws.teste"
    assert svc.conta(sessao_real).email == "sistema@bws.teste"


def test_a_senha_e_cifrada_e_nunca_volta(sessao_real):
    svc.configurar(sessao_real, {**CONTA, "usuario": "dp@bws.teste"}, area="DP")

    c = svc.configuracao(sessao_real, "DP")
    assert c["tem_senha"] is True
    assert "senha" not in c, "a senha não pode voltar na leitura"

    guardado = sessao_real.execute(text(
        "SELECT valor FROM parametros WHERE chave = 'email_dp_senha'")).scalar()
    assert guardado and "senha-que-nao-aparece" not in guardado, (
        "a senha tem de estar cifrada no banco")


def test_o_que_falta_manda_para_a_tela_certa_de_cada_area(sessao_real):
    """Frase que não diz ONDE configurar faz a pessoa procurar — e o DP não tem
    de saber que a conta dele não fica em Configurações."""
    assert "Configurações" in svc.o_que_falta(sessao_real, "SISTEMA")
    assert "Pessoal" in svc.o_que_falta(sessao_real, "DP")

    svc.configurar(sessao_real, {**CONTA, "usuario": "dp@bws.teste"}, area="DP")
    assert svc.o_que_falta(sessao_real, "DP") == ""


def test_area_inventada_e_recusada(sessao_real):
    with pytest.raises(ErroValidacao) as e:
        svc.configuracao(sessao_real, "SUPRIMENTO")
    # ⚑ E A MENSAGEM DIZ QUAIS EXISTEM. "SUPRIMENTO" é justamente o erro que
    # alguém vai cometer: a conta dele é a da EMPRESA, por CNPJ — decisão dele,
    # *"suprimento é um e-mail por empresa"*.
    assert "SISTEMA" in str(e.value) and "DP" in str(e.value)


# ---------------------------------------------------------------------------
# O DP manda pela conta DELE — e, sem ela, pela da empresa
# ---------------------------------------------------------------------------
def test_o_pedido_ao_contador_sai_pela_conta_do_DP(sessao_real, monkeypatch):
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Colaborador, Empresa

    e = Empresa(razao_social="BWS Construções", cnpj="11222333000181",
                smtp_servidor="smtp.empresa.com", smtp_porta=587,
                smtp_usuario="compras@bws.teste", smtp_senha_cifrada="x",
                smtp_seguranca="STARTTLS", email="compras@bws.teste")
    c = _pessoa_completa(sessao_real, "PESSOA DO PEDIDO", "86288366757")
    sessao_real.add(e)
    sessao_real.flush()
    c.empresa_id = e.id
    sessao_real.flush()

    svc.configurar(sessao_real, {**CONTA, "usuario": "dp@bws.teste"}, area="DP")

    usadas = []
    monkeypatch.setattr(svc_email, "enviar",
                        lambda s, **k: usadas.append(k["empresa"]) or _registro())
    p = svc_ct.abrir(sessao_real, {"colaborador_id": c.id,
                                   "template": "ASSINATURA_CTPS"}, None)
    svc_ct.enviar(sessao_real, p["id"], None, para=["contabil@exemplo.com"])

    assert usadas, "nada foi enviado"
    assert getattr(usadas[0], "smtp_usuario", "") == "dp@bws.teste", (
        "o pedido do DP tem de sair pela conta do DP, não pela da empresa")


def test_sem_a_conta_do_DP_o_pedido_sai_pela_da_empresa(sessao_real, monkeypatch):
    """A volta que impede o DP de parar no dia em que isto for publicado:
    ninguém configurou a conta do DP ainda, e o pedido tem de continuar
    saindo."""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Colaborador, Empresa

    e = Empresa(razao_social="BWS Construções", cnpj="11222333000182",
                smtp_servidor="smtp.empresa.com", smtp_porta=587,
                smtp_usuario="compras@bws.teste", smtp_senha_cifrada="x",
                smtp_seguranca="STARTTLS", email="compras@bws.teste")
    c = _pessoa_completa(sessao_real, "OUTRA PESSOA", "19100000000")
    sessao_real.add(e)
    sessao_real.flush()
    c.empresa_id = e.id
    sessao_real.flush()

    assert svc.conta(sessao_real, "DP") is None, (
        "este teste precisa do DP SEM conta configurada")

    usadas = []
    monkeypatch.setattr(svc_email, "enviar",
                        lambda s, **k: usadas.append(k["empresa"]) or _registro())
    p = svc_ct.abrir(sessao_real, {"colaborador_id": c.id,
                                   "template": "ASSINATURA_CTPS"}, None)
    svc_ct.enviar(sessao_real, p["id"], None, para=["contabil@exemplo.com"])

    assert usadas and getattr(usadas[0], "smtp_usuario", "") == "compras@bws.teste"


def _registro():
    """Um registro de envio que basta para quem chamou seguir."""
    class R:
        id = 1
        situacao = "ENVIADO"
        erro = None
        message_id = "<teste@erp>"
    return R()


def _pessoa_completa(s, nome: str, cpf: str):
    """Uma pessoa com o que o template de CTPS exige.

    Preencher o que o pedido pede é mais honesto que escolher um template sem
    exigência: é esse o pedido que o DP manda todo dia, e é por ele que o
    e-mail do DP tem de sair.
    """
    import datetime as dt

    from app.apps.erp.db.models.cadastros import Cargo, Colaborador
    cargo = Cargo(nome=f"CARGO {cpf[-4:]}")
    s.add(cargo)
    s.flush()
    c = Colaborador(nome=nome, cpf=cpf, situacao="ATIVO", vinculo="CTPS",
                    data_nascimento=dt.date(1990, 5, 2), pis="12345678901",
                    cargo_id=cargo.id, salario=2500,
                    admissao=dt.date.today() - dt.timedelta(days=10))
    s.add(c)
    s.flush()
    return c


def test_o_email_se_corrige_ganha_anexo_e_sai_com_ele(sessao_real, monkeypatch):
    """23/11/2026, ele: *"é para eu ler o e-mail, ver se eu concordo, para
    poder enviar (…) permitir até anexar alguma coisa."*"""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.arquivo import service as svc_arq
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Empresa

    e = Empresa(razao_social="BWS Construções", cnpj="11222333000183",
                smtp_servidor="smtp.empresa.com", smtp_porta=587,
                smtp_usuario="compras@bws.teste", smtp_senha_cifrada="x",
                smtp_seguranca="STARTTLS", email="compras@bws.teste")
    c = _pessoa_completa(sessao_real, "PESSOA COM ANEXO", "71428793860")
    sessao_real.add(e)
    sessao_real.flush()
    c.empresa_id = e.id
    sessao_real.flush()
    p = svc_ct.abrir(sessao_real, {"colaborador_id": c.id,
                                   "template": "ASSINATURA_CTPS"}, None)
    lido = svc_ct.ler(sessao_real, p["id"])
    assert lido["situacao_rotulo"] == "Ainda não enviado"

    svc_ct.editar_rascunho(sessao_real, p["id"], assunto="Assunto corrigido",
                           corpo="Texto corrigido pelo DP", usuario=None)
    with pytest.raises(ErroValidacao):
        svc_ct.editar_rascunho(sessao_real, p["id"], assunto="", corpo="x", usuario=None)

    from app.apps.erp.core.documentos.armazenamento import salvar  # noqa: F401
    guardado = {}

    class Doc:
        id = None

    def arquivar(s_, conteudo, nome, **kw):
        from sqlalchemy import text as _t
        anexo = s_.execute(_t("""
            INSERT INTO anexos (entidade_tipo, entidade_id, nome_arquivo, hash_sha256,
                                tamanho_bytes, mime_type, conteudo)
            VALUES ('colaborador', 1, :n, md5(random()::text), 3, 'application/pdf',
                    :c) RETURNING id"""), {"n": nome, "c": conteudo}).scalar_one()
        d = Doc()
        d.id = s_.execute(_t("""
            INSERT INTO documentos (tipo_codigo, anexo_id, colaborador_id, referencia,
                                    nome_padronizado)
            VALUES (:t, :a, :c, 'x', 'extra.pdf') RETURNING id"""),
            {"t": kw["tipo_codigo"], "a": anexo, "c": kw.get("colaborador_id")}).scalar_one()
        guardado["tipo"] = kw["tipo_codigo"]
        return d
    monkeypatch.setattr(svc_arq, "arquivar", arquivar)
    svc_ct.anexar_ao_rascunho(sessao_real, p["id"], conteudo=b"%PD", nome_arquivo="extra.pdf",
                              usuario=None)
    assert "extra.pdf" in svc_ct.ler(sessao_real, p["id"])["anexos"]

    saiu = []
    monkeypatch.setattr(svc_email, "enviar", lambda s, **k: saiu.append(k) or _registro())
    svc_ct.enviar(sessao_real, p["id"], None, para=["contabil@exemplo.com"])
    assert saiu[0]["assunto"] == "Assunto corrigido"
    assert saiu[0]["corpo"] == "Texto corrigido pelo DP"
    assert [a["nome"] for a in saiu[0]["anexos"]] == ["extra.pdf"]
    with pytest.raises(ErroValidacao, match="já foi enviado"):
        svc_ct.editar_rascunho(sessao_real, p["id"], assunto="a", corpo="b", usuario=None)


def test_reenviar_manda_o_mesmo_email_sem_mudar_o_prazo(sessao_real, monkeypatch):
    """23/11/2026, ele: *"se de repente eu precisar reenviar o e-mail, eu não
    tenho essa opção."*"""
    from app.apps.erp.core import contabilidade as svc_ct
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.db.models.cadastros import Empresa
    e = Empresa(razao_social="BWS Construções", cnpj="11222333000184",
                smtp_servidor="smtp.empresa.com", smtp_porta=587,
                smtp_usuario="compras@bws.teste", smtp_senha_cifrada="x",
                smtp_seguranca="STARTTLS", email="compras@bws.teste")
    c = _pessoa_completa(sessao_real, "PESSOA REENVIO", "35379838855")
    sessao_real.add(e)
    sessao_real.flush()
    c.empresa_id = e.id
    sessao_real.flush()
    p = svc_ct.abrir(sessao_real, {"colaborador_id": c.id,
                                   "template": "ASSINATURA_CTPS"}, None)
    saiu = []
    monkeypatch.setattr(svc_email, "enviar", lambda s, **k: saiu.append(k) or _registro())
    with pytest.raises(ErroValidacao, match="use Enviar"):
        svc_ct.reenviar(sessao_real, p["id"], None)      # ainda não foi enviado
    svc_ct.enviar(sessao_real, p["id"], None, para=["contabil@exemplo.com"])
    prazo = svc_ct.ler(sessao_real, p["id"])["prazo"]
    r = svc_ct.reenviar(sessao_real, p["id"], None)
    assert r["enviado"] and len(saiu) == 2
    assert saiu[1]["assunto"] == saiu[0]["assunto"] and saiu[1]["para"] == ["contabil@exemplo.com"]
    lido = svc_ct.ler(sessao_real, p["id"])
    assert lido["prazo"] == prazo and lido["situacao"] == "ENVIADO"
    assert [x["tipo"] for x in lido["eventos"]][-1] == "REENVIADO"


def test_copia_em_enviados_acha_a_pasta_e_respeita_o_provedor(sessao_real, monkeypatch):
    """O ERP grava o que mandou na pasta de enviados da MESMA conta (IMAP)."""
    from email.message import EmailMessage
    from app.apps.erp.core.comum import email as svc_email
    from app.apps.erp.core.comum import email_entrada

    class Caixa:
        gravado = []

        def __init__(self, *a, **k): pass
        def login(self, u, s): pass
        def list(self):
            return "OK", [b'(\\HasNoChildren) "." "INBOX"',
                          b'(\\HasNoChildren \\Sent) "." "INBOX.Enviados"']
        def append(self, pasta, flags, data, msg):
            Caixa.gravado.append(pasta)
            return "OK", [b""]
        def logout(self): pass

    import imaplib
    monkeypatch.setattr(imaplib, "IMAP4_SSL", Caixa)
    monkeypatch.setattr(email_entrada, "configuracao", lambda s: {
        "usavel": True, "usuario": "dp@bws.teste", "servidor": "imap.x", "porta": 993})
    monkeypatch.setattr(email_entrada, "_ler", lambda s, k, p="": "")

    class Conta:
        smtp_servidor = "smtp.hostinger.com"
        smtp_usuario = "DP@bws.teste"
    msg = EmailMessage()
    msg["Subject"] = "x"
    msg.set_content("y")
    assert svc_email.guardar_copia_nos_enviados(sessao_real, Conta, msg) == "guardado em Enviados"
    assert Caixa.gravado == ["INBOX.Enviados"]

    class Gmail(Conta):
        smtp_servidor = "smtp.gmail.com"
    assert "provedor" in svc_email.guardar_copia_nos_enviados(sessao_real, Gmail, msg)

    class Outra(Conta):
        smtp_usuario = "compras@bws.teste"
    assert "não é a mesma conta" in svc_email.guardar_copia_nos_enviados(sessao_real, Outra, msg)
