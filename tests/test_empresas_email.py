"""A empresa que executa a obra, e o e-mail que sai por ela.

A BWS passa a operar com mais de um CNPJ. Isso muda três coisas concretas, e
os testes aqui guardam justamente essas três:

  1. a cotação sai do e-mail DA EMPRESA da obra — mandar do CNPJ errado faz o
     fornecedor responder para uma caixa que ninguém lê;
  2. a senha da conta de e-mail NUNCA fica em claro no banco;
  3. o comprador precisa saber que a mensagem saiu, e para quem — e precisa
     saber, com estas palavras, que "saiu" não é "foi lido".

E o que não pode falhar no envio: um fornecedor fora do ar não derruba os
outros, e nada some em silêncio — ou registra ENVIADO, ou registra FALHOU com
o motivo escrito.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.cadastros import empresas as svc
from app.apps.erp.core.comum import email as correio
from app.apps.erp.core.comum import segredos
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.db.models.cadastros import (
    Empresa, Obra, PerfilUsuario as P,
)

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Marcelo")

# CNPJ com dígito verificador correto — o validador do ERP é o mesmo para dado
# de teste e dado de verdade.
CNPJ_A = "71000001000184"
CNPJ_B = "71000002000129"


def empresa(id_=1, **extra):
    padrao = dict(razao_social="CONSTRUTORA A LTDA", cnpj=CNPJ_A, ativo=True,
                  padrao=False, smtp_seguranca="STARTTLS", smtp_servidor=None,
                  smtp_porta=None, smtp_usuario=None, smtp_senha_cifrada=None,
                  smtp_remetente=None, smtp_responder_para=None, email=None,
                  nome_fantasia=None, logo=None, logo_mime=None, logo_nome=None,
                  smtp_conferido_em=None, municipio=None, uf=None,
                  logradouro=None, numero=None, bairro=None, telefone=None,
                  cep=None, complemento=None, site=None,
                  inscricao_estadual=None, inscricao_municipal=None)
    padrao.update(extra)
    return Empresa(id=id_, **padrao)


def pronta(id_=1, **extra):
    """Uma empresa com a conta de e-mail completa."""
    campos = dict(smtp_servidor="smtp.exemplo.com", smtp_porta=587,
                  smtp_usuario="compras@exemplo.com",
                  smtp_senha_cifrada="cifrado")
    campos.update(extra)
    return empresa(id_, **campos)


@pytest.fixture
def com_chave(monkeypatch):
    """Com a chave de segredos configurada, como em produção."""
    from cryptography.fernet import Fernet
    monkeypatch.setenv(segredos.VARIAVEL, Fernet.generate_key().decode())


# ---------------------------------------------------------------------------
# A senha nunca fica em claro
# ---------------------------------------------------------------------------
def test_a_senha_vai_e_volta_cifrada(com_chave):
    guardado = segredos.cifrar("minha-senha-de-aplicativo")
    assert "minha-senha" not in guardado, "a senha não pode aparecer no texto guardado"
    assert segredos.decifrar(guardado) == "minha-senha-de-aplicativo"


def test_sem_a_chave_nao_se_guarda_senha(monkeypatch):
    monkeypatch.delenv(segredos.VARIAVEL, raising=False)
    with pytest.raises(ErroValidacao, match="ERP_CHAVE_SEGREDOS"):
        segredos.cifrar("qualquer")


def test_trocar_a_chave_avisa_em_portugues_em_vez_de_devolver_lixo(monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setenv(segredos.VARIAVEL, Fernet.generate_key().decode())
    guardado = segredos.cifrar("senha")
    monkeypatch.setenv(segredos.VARIAVEL, Fernet.generate_key().decode())
    with pytest.raises(ErroValidacao, match="chave atual"):
        segredos.decifrar(guardado)


def test_definir_conta_sem_a_chave_e_recusado_com_o_motivo(monkeypatch):
    monkeypatch.delenv(segredos.VARIAVEL, raising=False)
    e = empresa()
    s = SessaoFalsa(e, ADMIN)
    with pytest.raises(ErroValidacao, match="ERP_CHAVE_SEGREDOS"):
        svc.definir_conta_de_email(s, 1, {
            "smtp_servidor": "smtp.exemplo.com", "smtp_porta": 587,
            "smtp_usuario": "compras@exemplo.com", "smtp_senha": "abc"}, ADMIN)


def test_a_senha_guardada_no_banco_nao_e_a_senha(com_chave):
    e = empresa()
    s = SessaoFalsa(e, ADMIN)
    svc.definir_conta_de_email(s, 1, {
        "smtp_servidor": "smtp.exemplo.com", "smtp_porta": 587,
        "smtp_usuario": "compras@exemplo.com",
        "smtp_senha": "senha-secreta-123"}, ADMIN)
    assert "senha-secreta-123" not in (e.smtp_senha_cifrada or "")
    assert segredos.decifrar(e.smtp_senha_cifrada) == "senha-secreta-123"


def test_senha_em_branco_mantem_a_que_ja_estava(com_chave):
    """A tela manda o campo vazio quando não se quer mexer na senha. Sem esta
    regra, corrigir a porta apagaria a senha sem ninguém perceber."""
    e = pronta(smtp_senha_cifrada="cifrado-antigo")
    s = SessaoFalsa(e, ADMIN)
    svc.definir_conta_de_email(s, 1, {
        "smtp_servidor": "smtp.novo.com", "smtp_porta": 465,
        "smtp_usuario": "compras@exemplo.com", "smtp_senha": ""}, ADMIN)
    assert e.smtp_senha_cifrada == "cifrado-antigo"
    assert e.smtp_porta == 465


# ---------------------------------------------------------------------------
# Cadastro da empresa
# ---------------------------------------------------------------------------
def test_cnpj_invalido_e_recusado():
    with pytest.raises(ErroValidacao, match="CNPJ inválido"):
        svc.criar(SessaoFalsa(ADMIN),
                  {"razao_social": "TESTE LTDA", "cnpj": "11111111111111"}, ADMIN)


def test_cnpj_repetido_e_recusado():
    s = SessaoFalsa(empresa(), ADMIN)
    with pytest.raises(ErroValidacao, match="Já existe empresa"):
        svc.criar(s, {"razao_social": "OUTRA LTDA", "cnpj": CNPJ_A}, ADMIN)


def test_a_primeira_empresa_vira_padrao_sozinha():
    """Sem isso, o primeiro disparo falharia com 'nenhuma empresa escolhida' e
    ninguém entenderia por quê."""
    s = SessaoFalsa(ADMIN)
    e = svc.criar(s, {"razao_social": "PRIMEIRA LTDA", "cnpj": CNPJ_A}, ADMIN)
    assert e.padrao is True


def test_a_segunda_empresa_nao_rouba_o_padrao():
    s = SessaoFalsa(empresa(1, padrao=True), ADMIN)
    e = svc.criar(s, {"razao_social": "SEGUNDA LTDA", "cnpj": CNPJ_B}, ADMIN)
    assert e.padrao is False


def test_definir_padrao_tira_a_marca_da_anterior():
    antiga, nova = empresa(1, padrao=True), empresa(2, cnpj=CNPJ_B)
    s = SessaoFalsa(antiga, nova, ADMIN)
    svc.definir_padrao(s, 2, ADMIN)
    assert nova.padrao is True
    assert antiga.padrao is False, "duas empresas padrão fariam o envio sortear"


def test_e_mail_com_formato_invalido_e_recusado():
    with pytest.raises(ErroValidacao, match="formato inválido"):
        svc.criar(SessaoFalsa(ADMIN), {"razao_social": "TESTE LTDA",
                                       "cnpj": CNPJ_A, "email": "isso não é e-mail"},
                  ADMIN)


def test_empresa_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.obter(SessaoFalsa(ADMIN), 99)


# ---------------------------------------------------------------------------
# Logo
# ---------------------------------------------------------------------------
def test_logo_de_tipo_errado_e_recusada():
    s = SessaoFalsa(empresa(), ADMIN)
    with pytest.raises(ErroValidacao, match="PNG, JPG, WEBP ou SVG"):
        svc.definir_logo(s, 1, "logo.pdf", "application/pdf", b"x" * 10, ADMIN)


def test_logo_grande_demais_e_recusada():
    s = SessaoFalsa(empresa(), ADMIN)
    grande = b"x" * (svc.LIMITE_LOGO + 1)
    with pytest.raises(ErroValidacao, match="limite"):
        svc.definir_logo(s, 1, "logo.png", "image/png", grande, ADMIN)


def test_logo_e_guardada_com_o_tipo():
    e = empresa()
    s = SessaoFalsa(e, ADMIN)
    svc.definir_logo(s, 1, "marca.png", "image/png; charset=binary", b"png", ADMIN)
    assert e.logo == b"png"
    assert e.logo_mime == "image/png"


# ---------------------------------------------------------------------------
# Quem manda: a empresa da obra
# ---------------------------------------------------------------------------
def test_a_obra_manda_pela_empresa_dela():
    a, b = empresa(1), empresa(2, cnpj=CNPJ_B, razao_social="CONSTRUTORA B LTDA")
    obra = Obra(id=10, codigo="OBRA-B", nome="Obra da B", empresa_id=2)
    s = SessaoFalsa(a, b, obra, ADMIN)
    assert svc.empresa_da_obra(s, 10).id == 2


def test_obra_sem_empresa_cai_na_padrao():
    a = empresa(1, padrao=True)
    obra = Obra(id=10, codigo="ORFA", nome="Obra sem empresa", empresa_id=None)
    s = SessaoFalsa(a, obra, ADMIN)
    assert svc.empresa_da_obra(s, 10).id == 1


def test_sem_padrao_e_com_varias_empresas_nao_se_escolhe_por_conta_propria():
    """Sortear o CNPJ que manda o e-mail seria pior do que recusar."""
    s = SessaoFalsa(empresa(1), empresa(2, cnpj=CNPJ_B), ADMIN)
    assert svc.padrao(s) is None


# ---------------------------------------------------------------------------
# A conta está pronta para enviar?
# ---------------------------------------------------------------------------
def test_conta_incompleta_diz_o_que_falta():
    pode, falta = correio.conta_configurada(empresa())
    assert pode is False
    for pedaco in ("servidor", "porta", "usuário", "senha"):
        assert pedaco in falta


def test_conta_completa_esta_pronta():
    pode, falta = correio.conta_configurada(pronta())
    assert pode is True and falta == ""


def test_sem_empresa_nenhuma_a_mensagem_e_clara():
    pode, falta = correio.conta_configurada(None)
    assert pode is False and "Nenhuma empresa" in falta


def test_o_remetente_usa_o_nome_de_fantasia_quando_nao_ha_um_escrito():
    """O nome com acento sai codificado no cabeçalho (é o que manda a norma do
    e-mail); quem lê vê "BWS Construções" normalmente. O que importa conferir
    aqui é o ENDEREÇO — mandar do endereço errado é o estrago de verdade."""
    from email.utils import parseaddr
    e = pronta(nome_fantasia="BWS Construções")
    _nome, endereco = parseaddr(correio.remetente_de(e))
    assert endereco == "compras@exemplo.com"


def test_o_remetente_escrito_a_mao_vence():
    e = pronta(nome_fantasia="BWS", smtp_remetente="Compras BWS <x@y.com>")
    assert correio.remetente_de(e) == "Compras BWS <x@y.com>"


# ---------------------------------------------------------------------------
# O envio: nada some em silêncio
# ---------------------------------------------------------------------------
def test_envio_bem_sucedido_fica_registrado(monkeypatch, com_chave):
    monkeypatch.setattr(correio, "_entregar", lambda *a, **k: None)
    s = SessaoFalsa(ADMIN)
    r = correio.enviar(s, empresa=pronta(), para=["forn@exemplo.com"],
                       assunto="Cotação COT-0001", corpo="lista de itens",
                       entidade_tipo="cotacao", entidade_id=7, usuario=ADMIN)
    assert r.situacao == "ENVIADO"
    assert r.erro is None
    assert r.para == ["forn@exemplo.com"]
    assert r.corpo == "lista de itens", "o texto exato fica guardado"


def test_falha_de_envio_nao_levanta_mas_fica_escrita(monkeypatch, com_chave):
    def explodir(*a, **k):
        raise correio.ErroDeEnvio("O servidor recusou o usuário ou a senha.")
    monkeypatch.setattr(correio, "_entregar", explodir)
    s = SessaoFalsa(ADMIN)
    r = correio.enviar(s, empresa=pronta(), para=["forn@exemplo.com"],
                       assunto="Cotação COT-0001", corpo="lista",
                       entidade_tipo="cotacao", entidade_id=7, usuario=ADMIN)
    assert r.situacao == "FALHOU"
    assert "usuário ou a senha" in r.erro


def test_envio_sem_destinatario_e_recusado(com_chave):
    with pytest.raises(ErroValidacao, match="Sem endereço de destino"):
        correio.enviar(SessaoFalsa(ADMIN), empresa=pronta(), para=[],
                       assunto="x y z", corpo="c", entidade_tipo="cotacao",
                       entidade_id=1)


def test_endereco_torto_e_recusado_antes_de_tentar(com_chave):
    with pytest.raises(ErroValidacao, match="formato inválido"):
        correio.enviar(SessaoFalsa(ADMIN), empresa=pronta(),
                       para=["nao é e-mail"], assunto="x y z", corpo="c",
                       entidade_tipo="cotacao", entidade_id=1)


def test_envio_por_empresa_sem_conta_e_recusado_com_o_motivo():
    with pytest.raises(ErroValidacao, match="está incompleta"):
        correio.enviar(SessaoFalsa(ADMIN), empresa=empresa(),
                       para=["forn@exemplo.com"], assunto="x y z", corpo="c",
                       entidade_tipo="cotacao", entidade_id=1)


@pytest.mark.parametrize("endereco,vale", [
    ("compras@bws.com.br", True),
    ("BWS <compras@bws.com.br>", True),
    ("compras@bws", False),
    ("sem arroba", False),
    ("", False),
])
def test_o_que_conta_como_endereco(endereco, vale):
    assert correio.endereco_valido(endereco) is vale
