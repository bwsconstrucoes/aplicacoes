# -*- coding: utf-8 -*-
"""
O ACESSO AO SISTEMA DE QUEM TRABALHA (migração 096).

Três coisas que ele pediu em 09/11/2026, antes de pôr o DP em prática:

  1. não existe "pré-cadastro" — existe cadastro, e ele vem com documentação;
  2. desligar o colaborador tem de **bloquear o acesso dele**, sozinho;
  3. o primeiro acesso é por **convite**: a pessoa grava a própria senha.

**O ARQUIVO INTEIRO PROTEGE UMA COISA SÓ: que ninguém entre quando não deve.**
Por isso quase tudo aqui é teste de recusa — link usado, link vencido, link de
quem foi bloqueado, acesso que continuou de pé depois do desligamento.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth import convites
from app.apps.erp.core.cadastros import pessoas as svc
from app.apps.erp.core.comum import email_sistema
from app.apps.erp.core.comum.auditoria import ErroValidacao

banco_ = pytest.mark.banco


# ---------------------------------------------------------------------------
# SEM BANCO
# ---------------------------------------------------------------------------
def test_nao_existe_mais_pre_cadastro_na_fila_do_DP():
    """Ele, com todas as letras: *"não existe esse pré-cadastro, existe o
    cadastro, ponto final. O cadastro já tem que vir com documentação."*

    A diferença não é de palavra, é de quem faz o quê: no pré-cadastro, a obra
    mandava nome e CPF e o DP ia atrás do resto — que é o trabalho que esta
    fila existe para acabar.
    """
    from app.apps.erp.core import pessoal_solicitacoes as fila
    assert "PRE_CADASTRO" not in fila.TIPOS
    assert "CADASTRO" in fila.TIPOS
    assert "documento" in fila.TIPOS["CADASTRO"]["explicacao"].lower()


def test_o_token_do_convite_nunca_é_igual_ao_que_fica_guardado():
    """O que vai para o e-mail e o que fica no banco são coisas diferentes —
    é o que faz um backup vazado não abrir a conta de ninguém."""
    token = "um-token-qualquer"
    assert convites._resumo(token) != token
    assert len(convites._resumo(token)) == 64          # SHA-256 em hexa
    assert convites._resumo(token) == convites._resumo(token)


def test_a_conta_do_sistema_diz_o_que_falta_em_portugues():
    """Tela que só diz "não configurado" faz a pessoa adivinhar o quê."""
    class SemNada:
        def get(self, *a, **k):
            return None
    falta = email_sistema.o_que_falta(SemNada())
    assert "servidor" in falta and "senha" in falta
    assert "E-mail do sistema" in falta       # diz ONDE configurar


# ---------------------------------------------------------------------------
# COM BANCO
# ---------------------------------------------------------------------------
@pytest.fixture
def admin(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Dona do sistema", email="admin.acesso@teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMIN, telefone="5585900000020")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.fixture
def pessoa(sessao_real):
    """Um colaborador com acesso ao ERP ligado a ele."""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        Colaborador, PerfilUsuario, Usuario,
    )
    operador = Usuario(nome="João da Obra", email="joao.obra@teste", ativo=True,
                       senha_hash=gerar_hash("senha-de-teste-123"),
                       perfil=PerfilUsuario.ADMINISTRATIVO_OBRA,
                       telefone="5585900000021")
    sessao_real.add(operador)
    sessao_real.flush()
    c = Colaborador(nome="João da Obra", cpf="52998224725", situacao="ATIVO",
                    vinculo="CTPS", admissao=date(2026, 1, 5),
                    email="joao.obra@teste", usuario_id=operador.id)
    sessao_real.add(c)
    sessao_real.flush()
    return c


@banco_
def test_desligar_o_colaborador_bloqueia_o_acesso_dele(sessao_real, pessoa, admin):
    """⚑ O TESTE QUE MAIS IMPORTA AQUI.

    Ele: *"quando uma pessoa for desativada do cadastro, for demitido, sair da
    empresa, a gente precisaria automaticamente bloquear o acesso dela."*

    É o tipo de coisa cujo esquecimento NÃO aparece: o ex-colaborador continua
    entrando e vendo obra, pagamento e gente — e nada quebra para denunciar.
    """
    from app.apps.erp.db.models.cadastros import Usuario
    operador = sessao_real.get(Usuario, pessoa.usuario_id)
    assert operador.ativo is True

    svc.desligar(sessao_real, pessoa.id,
                 {"data_saida": "2026-06-30", "forma_desligamento": "AVISO_INDENIZADO"},
                 admin)
    sessao_real.flush()
    assert operador.ativo is False, "o acesso continuou de pé depois de sair"


@banco_
def test_desligar_quem_nao_tem_acesso_nao_quebra(sessao_real, admin):
    """A maioria de quem trabalha não entra no sistema. O desligamento dessa
    gente tem de seguir normal."""
    from app.apps.erp.db.models.cadastros import Colaborador
    c = Colaborador(nome="Sem Login", cpf="11144477735", situacao="ATIVO",
                    vinculo="CTPS", admissao=date(2026, 1, 5))
    sessao_real.add(c)
    sessao_real.flush()
    svc.desligar(sessao_real, c.id,
                 {"data_saida": "2026-06-30", "forma_desligamento": "PEDIDO_COLABORADOR"},
                 admin)
    assert c.situacao == "DESLIGADO"


@banco_
def test_um_login_e_de_uma_pessoa_so(sessao_real, pessoa, admin):
    """Dois colaboradores no mesmo login fariam o desligamento de um derrubar
    o acesso do outro."""
    from app.apps.erp.db.models.cadastros import Colaborador
    outro = Colaborador(nome="Outra Pessoa", cpf="11144477735", situacao="ATIVO",
                        vinculo="CTPS", admissao=date(2026, 2, 1))
    sessao_real.add(outro)
    sessao_real.flush()
    with pytest.raises(ErroValidacao, match="uma pessoa só"):
        svc.ligar_ao_operador(sessao_real, outro.id, pessoa.usuario_id, admin)


# ---------------------------------------------------------------------------
# O CONVITE
# ---------------------------------------------------------------------------
@banco_
def test_o_convite_grava_a_senha_que_a_PESSOA_escolheu(sessao_real, pessoa, admin):
    from app.apps.erp.core.auth.service import autenticar
    from app.apps.erp.db.models.cadastros import Usuario

    _id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    convites.usar(sessao_real, token, "minha-senha-nova-123")
    sessao_real.flush()

    operador = sessao_real.get(Usuario, pessoa.usuario_id)
    assert autenticar(sessao_real, operador.email, "minha-senha-nova-123")


@banco_
def test_o_link_serve_UMA_vez(sessao_real, pessoa, admin):
    """Link que continua valendo depois de usado é link que, reencaminhado por
    engano, troca a senha de alguém."""
    _id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    convites.usar(sessao_real, token, "primeira-senha-123")
    with pytest.raises(ErroValidacao, match="não vale mais"):
        convites.usar(sessao_real, token, "outra-senha-qualquer-456")


@banco_
def test_convite_novo_mata_o_anterior(sessao_real, pessoa, admin):
    """Reenviar porque "não chegou" tem de deixar UM link válido. Dois abertos
    é a situação em que a pessoa usa o velho e alguém jura que o sistema
    quebrou."""
    _id, velho = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    _id2, novo = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    with pytest.raises(ErroValidacao, match="não vale mais"):
        convites.conferir(sessao_real, velho)
    assert convites.conferir(sessao_real, novo)["usuario_id"] == pessoa.usuario_id


@banco_
def test_link_vencido_e_recusado(sessao_real, pessoa, admin):
    convite_id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    sessao_real.execute(text("UPDATE usuario_convites SET expira_em = :q "
                             "WHERE id = :i"),
                        {"q": datetime.now(timezone.utc) - timedelta(days=1),
                         "i": convite_id})
    with pytest.raises(ErroValidacao, match="venceu"):
        convites.conferir(sessao_real, token)


@banco_
def test_convite_NAO_ressuscita_acesso_bloqueado(sessao_real, pessoa, admin):
    """⚑ SEGURANÇA, e é sutil: se o convite religasse, bastaria a pessoa ter um
    link velho no e-mail para voltar a entrar depois de sair da empresa."""
    _id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    svc.desligar(sessao_real, pessoa.id,
                 {"data_saida": "2026-06-30", "forma_desligamento": "AVISO_INDENIZADO"},
                 admin)
    sessao_real.flush()
    with pytest.raises(ErroValidacao, match="bloqueado"):
        convites.conferir(sessao_real, token)


@banco_
def test_senha_curta_demais_e_recusada(sessao_real, pessoa, admin):
    _id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    with pytest.raises(ErroValidacao, match="8"):
        convites.usar(sessao_real, token, "123")


@banco_
def test_token_desconhecido_nao_diz_que_e_desconhecido(sessao_real):
    """O mesmo recado para inexistente, usado e vencido: dizer qual dos três
    ajudaria quem está tentando links no escuro."""
    with pytest.raises(ErroValidacao, match="não vale mais"):
        convites.conferir(sessao_real, "token-que-nunca-existiu")


@banco_
def test_sem_e_mail_nao_ha_para_onde_mandar(sessao_real, admin):
    from app.apps.erp.db.models.cadastros import Usuario
    sem = sessao_real.get(Usuario, admin.id)
    sem.email = ""
    sessao_real.flush()
    with pytest.raises(ErroValidacao, match="e-mail"):
        convites.criar(sessao_real, sem.id, autor=admin)


@banco_
def test_sem_conta_do_sistema_o_convite_nao_sai_mas_avisa(sessao_real, pessoa,
                                                          admin):
    """Cadastro não se perde porque a conta de e-mail não está configurada —
    mas quem cadastrou precisa SABER que o link não saiu."""
    r = convites.enviar(sessao_real, pessoa.usuario_id,
                        link_base="https://exemplo/erp/primeiro-acesso",
                        autor=admin)
    assert r["enviado"] is False
    assert "E-mail do sistema" in r["erro"]


# ---------------------------------------------------------------------------
# PELA TELA
# ---------------------------------------------------------------------------
@banco_
def test_a_tela_do_primeiro_acesso_é_publica_e_recusa_link_invalido(app_real):
    """Pública porque quem abre ainda NÃO TEM SENHA — não há login a exigir.
    O que protege é o token."""
    c = app_real.test_client()                     # sem sessão nenhuma
    r = c.get("/erp/primeiro-acesso/token-invalido")
    assert r.status_code == 200
    assert "não vale mais" in r.data.decode()


@banco_
def test_a_pessoa_define_a_senha_pela_tela(app_real, sessao_real, pessoa, admin):
    _id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    sessao_real.flush()
    c = app_real.test_client()
    assert "João da Obra" in c.get(f"/erp/primeiro-acesso/{token}").data.decode()
    r = c.post(f"/erp/primeiro-acesso/{token}",
               data={"senha": "senha-escolhida-123",
                     "repetida": "senha-escolhida-123"})
    assert "Senha gravada" in r.data.decode()


@banco_
def test_as_duas_senhas_tem_de_bater(app_real, sessao_real, pessoa, admin):
    _id, token = convites.criar(sessao_real, pessoa.usuario_id, autor=admin)
    sessao_real.flush()
    r = app_real.test_client().post(
        f"/erp/primeiro-acesso/{token}",
        data={"senha": "senha-escolhida-123", "repetida": "outra-coisa-123"})
    texto = r.data.decode()
    assert "não são iguais" in texto
    # E O LINK CONTINUA VALENDO: errar a repetição não pode queimar o convite.
    assert convites.conferir(sessao_real, token)


@banco_
def test_liberar_acesso_pela_ficha_cria_o_operador_e_liga(app_real, sessao_real,
                                                          admin):
    """A resposta à pergunta dele: *"estando tudo certo, o que eu vou precisar
    é só cadastrar as pessoas e liberar o acesso?"*"""
    from tests.conftest import como
    from app.apps.erp.db.models.cadastros import Colaborador
    c = Colaborador(nome="Maria da Obra", cpf="11144477735", situacao="ATIVO",
                    vinculo="CTPS", admissao=date(2026, 3, 1))
    sessao_real.add(c)
    sessao_real.flush()

    r = como(app_real, admin.id).post(f"/erp/api/colaboradores/{c.id}/acesso",
                                      json={"email": "maria.obra@teste"})
    assert r.status_code == 200, r.data[:300]
    sessao_real.flush()
    assert c.usuario_id is not None
    # O convite não saiu (não há conta do sistema no teste), e a resposta DIZ.
    assert r.get_json()["convite"]["enviado"] is False


@banco_
def test_nao_se_libera_acesso_para_quem_ja_saiu(app_real, sessao_real, admin):
    from tests.conftest import como
    from app.apps.erp.db.models.cadastros import Colaborador
    c = Colaborador(nome="Já Saiu", cpf="11144477735", situacao="DESLIGADO",
                    vinculo="CTPS", admissao=date(2025, 1, 1),
                    demissao=date(2026, 1, 1))
    sessao_real.add(c)
    sessao_real.flush()
    r = como(app_real, admin.id).post(f"/erp/api/colaboradores/{c.id}/acesso",
                                      json={"email": "jasaiu@teste"})
    assert r.status_code == 400
    assert "desligada" in r.get_json()["erro"]
