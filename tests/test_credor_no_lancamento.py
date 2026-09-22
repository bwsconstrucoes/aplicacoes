"""CADASTRAR O CREDOR SEM SAIR DO LANÇAMENTO — e a saída do beco.

22/09/2026, o dono usando a tela:

  *"Se o credor não tem cadastro, o ERP deve avisar e tem que ser permitido o
  cadastro já a partir da tela."*

  *"Veja: 'Dados bancários vivem no cadastro do credor, nunca no lançamento.'
  Isso não tem sentido se tivermos cadastrando na hora. E ainda tem que prever
  como sairemos da situação que cadastro o credor enquanto lanço e preciso
  voltar pra cadastrar a forma de pgt."*

A DECISÃO CENTRAL, e é ela que estes testes defendem:

  A homologação em duas pessoas existe contra o golpe da troca de conta — quem
  lança não pode ser quem libera o destino do dinheiro. Deixar o lançador criar
  conta JÁ HOMOLOGADA destruiria o controle. Não deixar cadastrar nada obriga a
  jogar fora o lançamento e recomeçar.

  PENDENTE resolve os dois: a conta nasce pendente, o lançamento é GRAVADO, o
  título nasce BLOQUEADO pela crítica C2 (que já existia), e quem tem alçada
  homologa e manda REANALISAR. O trabalho não se perde e o controle não cai.

O que NÃO pode acontecer, e tem teste para cada um: conta nascer homologada;
título bloqueado ser aprovado; e o título ficar preso para sempre depois de a
conta ser homologada — que era o beco, porque a análise só rodava na criação.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import service as svc_tit
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, FornecedorConta, FormaPagamento, Obra,
    PerfilUsuario as P, StatusConta, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import StatusTitulo

from conftest import como

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    obra = Obra(codigo="CREDLANC", nome="Obra do teste de credor", status="ATIVA")
    cat = Categoria(codigo="3.1.99", descricao="Material diverso", ativo=True,
                    grupo_codigo="3", grupo_nome="Custos")
    lancador = Usuario(nome="Quem lança", email="lanca.cred@teste.local", ativo=True,
                       senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN,
                       telefone="5585911110000")
    s.add_all([obra, cat, lancador])
    s.flush()
    return {"s": s, "obra": obra, "cat": cat, "lancador": lancador}


def _cadastrar(app, usuario_id, **corpo):
    return como(app, usuario_id).post("/erp/api/credores", json=corpo)


# ---------------------------------------------------------------------------
# O cadastro pela tela de lançamento
# ---------------------------------------------------------------------------
def test_credor_novo_nasce_da_tela_de_lancamento(cenario, app_real):
    c = cenario
    r = _cadastrar(app_real, c["lancador"].id,
                   cnpj_cpf="11.444.777/0001-61",
                   razao_social="MADEIREIRA DO TESTE LTDA")

    corpo = r.get_json()
    assert corpo["ok"] is True and corpo["ja_existia"] is False
    assert corpo["credor"]["documento"] == "11444777000161", "o documento entra só com dígitos"
    assert corpo["credor"]["contas"] == [], "sem forma que use conta, não inventa conta"


def test_a_conta_criada_no_lancamento_nasce_PENDENTE(cenario, app_real):
    """É O CONTROLE INTEIRO. Se um dia isto virar HOMOLOGADA, quem lança passa a
    escolher sozinho para onde o dinheiro vai."""
    c = cenario
    r = _cadastrar(app_real, c["lancador"].id,
                   cnpj_cpf="11444777000161", razao_social="MADEIREIRA DO TESTE LTDA",
                   conta_forma="PIX", pix_tipo="CNPJ", pix_chave="11444777000161")

    corpo = r.get_json()
    assert corpo["credor"]["contas"] == [], "conta pendente NÃO aparece como homologada"
    assert len(corpo["credor"]["contas_pendentes"]) == 1
    conta = c["s"].get(FornecedorConta, corpo["conta_pendente_id"])
    assert conta.status == StatusConta.PENDENTE
    assert conta.homologada_por is None and conta.homologada_em is None


def test_documento_ja_cadastrado_devolve_o_que_existe_em_vez_de_reclamar(cenario, app_real):
    """Para quem está lançando, "já existe" não é erro: é a resposta que ele
    procurava. Mandar procurar na lista seria devolver o problema."""
    c = cenario
    _cadastrar(app_real, c["lancador"].id, cnpj_cpf="11444777000161",
               razao_social="MADEIREIRA DO TESTE LTDA")

    r = _cadastrar(app_real, c["lancador"].id, cnpj_cpf="11444777000161",
                   razao_social="NOME DIFERENTE QUE A PESSOA DIGITOU")

    corpo = r.get_json()
    assert corpo["ok"] is True and corpo["ja_existia"] is True
    assert corpo["credor"]["nome"] == "MADEIREIRA DO TESTE LTDA", \
        "devolve o cadastro que existe, sem sobrescrever com o que foi digitado"


def test_pix_sem_chave_e_recusado(cenario, app_real):
    c = cenario
    r = _cadastrar(app_real, c["lancador"].id, cnpj_cpf="11444777000161",
                   razao_social="MADEIREIRA DO TESTE LTDA", conta_forma="PIX")
    assert r.status_code == 400
    assert "chave Pix" in r.get_json()["erro"]


def test_ted_sem_banco_agencia_e_conta_e_recusado(cenario, app_real):
    c = cenario
    r = _cadastrar(app_real, c["lancador"].id, cnpj_cpf="11444777000161",
                   razao_social="MADEIREIRA DO TESTE LTDA", conta_forma="TED",
                   banco_codigo="237")
    assert r.status_code == 400
    assert "agência" in r.get_json()["erro"]


# ---------------------------------------------------------------------------
# O lançamento com conta pendente — e a saída
# ---------------------------------------------------------------------------
def _titulo(s, cenario, conta_id):
    return svc_tit.criar_titulo(s, {
        "tipo": "T14_EXCECAO_SEM_NOTA",
        "fornecedor_id": cenario["forn"].id, "categoria_id": cenario["cat"].id,
        "descricao": "compra de material para a obra",
        "valor_bruto": "500.00", "forma_pagamento": "PIX",
        "fornecedor_conta_id": conta_id,
        "justificativa_excecao": "sem nota fiscal, teste",
        "parcelas": [{"vencimento": (date.today() + timedelta(days=30)).isoformat(),
                      "valor": "500.00"}],
        "rateios": [{"obra_id": cenario["obra"].id, "valor": "500.00"}],
    }, cenario["lancador"])


@pytest.fixture
def com_credor(cenario):
    s = cenario["s"]
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="MADEIREIRA DO TESTE LTDA", ativo=True)
    s.add(forn); s.flush()
    conta = FornecedorConta(fornecedor_id=forn.id, forma=FormaPagamento.PIX,
                            pix_tipo="CNPJ", pix_chave="11444777000161",
                            titular_nome=forn.razao_social, titular_doc=forn.cnpj_cpf,
                            status=StatusConta.PENDENTE)
    s.add(conta); s.flush()
    return {**cenario, "forn": forn, "conta": conta}


def test_o_lancamento_com_conta_pendente_E_GRAVADO_e_nasce_BLOQUEADO(com_credor):
    """Era o beco: até 22/09/2026 isto RECUSAVA, e o trabalho inteiro se perdia
    na hora de salvar."""
    c = com_credor
    t = _titulo(c["s"], c, c["conta"].id)

    assert t.id is not None, "o lançamento tem de existir"
    assert t.status == StatusTitulo.BLOQUEADO


def test_o_recado_do_bloqueio_diz_o_que_fazer(com_credor):
    from sqlalchemy import select
    from app.apps.erp.db.models.financeiro import Analise

    c = com_credor
    t = _titulo(c["s"], c, c["conta"].id)
    c["s"].flush()
    analise = c["s"].scalars(
        select(Analise).where(Analise.titulo_id == t.id)
        .order_by(Analise.id.desc())).first()
    c2 = [x for x in analise.criticas if x["codigo"] == "C2"][0]

    assert c2["severidade"] == "BLOQUEIA"
    assert "REANALISAR" in c2["msg"], "tem de dizer como sair, não só que travou"
    assert "não se perde" in c2["msg"]


def test_titulo_bloqueado_nao_pode_ser_aprovado(com_credor):
    """A porta dos fundos que NÃO pode existir: se bloqueado pudesse ser
    aprovado, a conta pendente viraria pagamento."""
    c = com_credor
    t = _titulo(c["s"], c, c["conta"].id)
    outro = Usuario(nome="Aprovador", email="aprova.cred@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN,
                    telefone="5585922220000")
    c["s"].add(outro); c["s"].flush()

    with pytest.raises(ErroValidacao):
        svc_tit.aprovar(c["s"], t.id, outro)


def test_homologada_a_conta_a_REANALISE_destrava(com_credor):
    """A saída do beco. Antes disto, a análise só rodava na criação: homologar a
    conta não adiantava nada, e o título ficava preso para sempre."""
    c = com_credor
    t = _titulo(c["s"], c, c["conta"].id)
    assert t.status == StatusTitulo.BLOQUEADO

    c["conta"].status = StatusConta.HOMOLOGADA
    c["s"].flush()
    svc_tit.reanalisar(c["s"], t.id, c["lancador"])

    assert t.status == StatusTitulo.AGUARDANDO_APROVACAO


def test_reanalisar_NAO_aprova(com_credor):
    """Destravar não é liberar: a aprovação continua sendo de outra pessoa, com
    alçada. Se reanalisar aprovasse, teria virado um atalho."""
    c = com_credor
    t = _titulo(c["s"], c, c["conta"].id)
    c["conta"].status = StatusConta.HOMOLOGADA
    c["s"].flush()
    svc_tit.reanalisar(c["s"], t.id, c["lancador"])

    assert t.status != StatusTitulo.APROVADO


def test_reanalisar_so_vale_para_BLOQUEADO(com_credor):
    """Reanalisar um título já aprovado poderia rebaixá-lo sem ninguém pedir."""
    c = com_credor
    c["conta"].status = StatusConta.HOMOLOGADA
    c["s"].flush()
    t = _titulo(c["s"], c, c["conta"].id)
    assert t.status != StatusTitulo.BLOQUEADO

    with pytest.raises(ErroValidacao):
        svc_tit.reanalisar(c["s"], t.id, c["lancador"])


def test_conta_de_OUTRO_credor_continua_recusada(com_credor):
    """O que é erro de verdade continua sendo erro: só o que era atrito de
    FLUXO foi afrouxado."""
    c = com_credor
    s = c["s"]
    outro = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11222333000181",
                       razao_social="OUTRO CREDOR LTDA", ativo=True)
    s.add(outro); s.flush()
    conta_alheia = FornecedorConta(fornecedor_id=outro.id, forma=FormaPagamento.PIX,
                                   pix_tipo="CNPJ", pix_chave="11222333000181",
                                   titular_nome=outro.razao_social,
                                   titular_doc=outro.cnpj_cpf,
                                   status=StatusConta.HOMOLOGADA)
    s.add(conta_alheia); s.flush()

    with pytest.raises(ErroValidacao) as e:
        _titulo(s, c, conta_alheia.id)
    assert "não pertence ao fornecedor" in str(e.value)
