"""Uma conta de e-mail servindo VÁRIAS empresas.

Pedido do dono em 14/09/2026: *"o ideal seria que a gente continuasse
utilizando um e-mail principal para encaminhar de outras empresas, e de repente
usar um alias — mas eu acho que não dá, né?"*.

Dá: o e-mail SAI pela conta principal (servidor, usuário e senha dela) e
APARECE com o remetente da empresa que está mandando. O que o ERP não resolve
— e está dito na tela — é o provedor aceitar um remetente de outro domínio.

COM BANCO DE VERDADE porque a conta emprestada é resolvida por um `JOIN` da
empresa com ela mesma, e porque a senha passa pela cifragem.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.cadastros import empresas as svc
from app.apps.erp.core.comum import email as correio
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Empresa

pytestmark = pytest.mark.banco

CONTA = {
    "smtp_servidor": "smtp.locaweb.com.br", "smtp_porta": 587,
    "smtp_usuario": "contato@principal.com.br", "smtp_seguranca": "STARTTLS",
    "smtp_senha": "senha-de-teste-do-email",
    "smtp_remetente": "Construtora Principal <contato@principal.com.br>",
}


@pytest.fixture
def empresas(sessao_real, monkeypatch):
    """Duas empresas: uma com conta de verdade, outra sem nenhuma."""
    # A cifragem exige a chave do Render; nos testes ela é gerada na hora, no
    # mesmo formato que a de produção (Fernet).
    from cryptography.fernet import Fernet
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", Fernet.generate_key().decode())
    s = sessao_real
    principal = Empresa(razao_social="Construtora Principal",
                        cnpj="11222333000181")
    outra = Empresa(razao_social="Construtora Segunda", cnpj="11222333000262")
    s.add_all([principal, outra])
    s.flush()
    svc.definir_conta_de_email(s, principal.id, dict(CONTA), None)
    s.flush()
    return {"s": s, "principal": principal, "outra": outra}


def test_sem_conta_propria_a_empresa_nao_manda(empresas):
    d = empresas
    pode, falta = correio.conta_configurada(d["outra"], d["s"])
    assert pode is False
    assert "incompleta" in falta


def test_emprestando_a_conta_a_empresa_passa_a_mandar(empresas):
    """É o pedido em uma frase: sem cadastrar servidor, usuário nem senha na
    segunda empresa, ela passa a conseguir enviar."""
    d = empresas
    svc.definir_conta_de_email(d["s"], d["outra"].id, {
        "conta_email_de_id": d["principal"].id,
        "smtp_remetente": "Construtora Segunda <obras@segunda.com.br>",
        "smtp_responder_para": "obras@segunda.com.br"}, None)
    d["s"].flush()

    pode, falta = correio.conta_configurada(d["outra"], d["s"])
    assert pode is True, falta

    # De onde SAI é a principal; como APARECE é da segunda.
    dona = correio.conta_de_envio(d["s"], d["outra"])
    assert dona.id == d["principal"].id
    assert dona.smtp_usuario == "contato@principal.com.br"
    assert correio.remetente_de(d["outra"]) == \
        "Construtora Segunda <obras@segunda.com.br>"


def test_a_mensagem_sai_com_o_remetente_de_quem_manda(empresas):
    """O cabeçalho da mensagem é o que o fornecedor lê — tem de ser o da
    empresa da obra, não o da conta que entrou no servidor."""
    d = empresas
    svc.definir_conta_de_email(d["s"], d["outra"].id, {
        "conta_email_de_id": d["principal"].id,
        "smtp_remetente": "Construtora Segunda <obras@segunda.com.br>",
        "smtp_responder_para": "obras@segunda.com.br"}, None)
    d["s"].flush()

    msg = correio._montar(d["outra"], ["fornecedor@exemplo.com.br"],
                          "Cotação", "corpo", [], None, [])
    assert msg["From"] == "Construtora Segunda <obras@segunda.com.br>"
    assert msg["Reply-To"] == "obras@segunda.com.br"


def test_nao_se_empresta_de_quem_ja_esta_emprestando(empresas):
    """Corrente de empréstimo é o "parou de mandar e ninguém sabe por quê"."""
    d = empresas
    svc.definir_conta_de_email(d["s"], d["outra"].id,
                               {"conta_email_de_id": d["principal"].id}, None)
    d["s"].flush()
    terceira = Empresa(razao_social="Construtora Terceira", cnpj="11222333000343")
    d["s"].add(terceira)
    d["s"].flush()
    with pytest.raises(ErroValidacao) as erro:
        svc.definir_conta_de_email(d["s"], terceira.id,
                                   {"conta_email_de_id": d["outra"].id}, None)
    assert "conta de verdade" in str(erro.value)


def test_nao_se_empresta_de_conta_incompleta(empresas):
    """Pendurar numa conta que não manda só apareceria na hora da cotação."""
    d = empresas
    with pytest.raises(ErroValidacao) as erro:
        svc.definir_conta_de_email(d["s"], d["principal"].id,
                                   {"conta_email_de_id": d["outra"].id}, None)
    assert "incompleta" in str(erro.value)


def test_nao_se_empresta_de_si_mesma(empresas):
    d = empresas
    with pytest.raises(ErroValidacao):
        svc.definir_conta_de_email(d["s"], d["outra"].id,
                                   {"conta_email_de_id": d["outra"].id}, None)


def test_voltar_para_a_conta_propria(empresas):
    """Desfazer é um clique: escolher "conta própria" devolve tudo ao que era."""
    d = empresas
    svc.definir_conta_de_email(d["s"], d["outra"].id,
                               {"conta_email_de_id": d["principal"].id}, None)
    d["s"].flush()
    assert correio.conta_configurada(d["outra"], d["s"])[0] is True

    svc.definir_conta_de_email(d["s"], d["outra"].id,
                               {"conta_email_de_id": None}, None)
    d["s"].flush()
    assert d["outra"].conta_email_de_id is None
    assert correio.conta_configurada(d["outra"], d["s"])[0] is False


def test_a_tela_diz_de_quem_e_a_conta(empresas):
    d = empresas
    svc.definir_conta_de_email(d["s"], d["outra"].id,
                               {"conta_email_de_id": d["principal"].id}, None)
    d["s"].flush()
    painel = svc.gerenciar(d["s"])
    linha = next(e for e in painel["empresas"] if e["id"] == d["outra"].id)
    assert linha["conta_email_de_id"] == d["principal"].id
    assert linha["conta_email_de_nome"] == "Construtora Principal"
    assert linha["pode_enviar"] is True
