"""O agente não pode virar spam — provado com banco de verdade.

A rotina roda TODO DIA. Se ela mandar de novo a cada execução, a obra passa a
ignorar as mensagens na primeira semana, e aí o agente não serve para nada.

A trava tem duas camadas, e as duas são provadas aqui:

  1. o código pergunta antes ("já mandei este degrau para esta pessoa?");
  2. o banco recusa a repetição, por chave única. Esta segunda existe porque a
     primeira sozinha não segura duas execuções ao mesmo tempo.

O dublê de sessão não prova nada disso: ele ignora `WHERE` e não tem chave
única. Só o Postgres executa a consulta e a restrição.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.apps.erp.core import agente as ag
from app.apps.erp.core import locacoes_conferencia as conf
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Obra, PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    AgenteMensagem, ContratoLocacao, LocacaoItem,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    """Uma conferência bem atrasada, e o agente sem mandar mensagem de verdade."""
    s = sessao_real
    ruan = Usuario(nome="Ruan do administrativo", email="ruan.ag@teste.local",
                   senha_hash=gerar_hash("senha-de-teste-123"),
                   perfil=P.ADMINISTRATIVO_OBRA, ativo=True,
                   telefone="5585999990000")
    dono = Usuario(nome="Marcelo", email="dono.ag@teste.local",
                   senha_hash=gerar_hash("senha-de-teste-123"),
                   perfil=P.ADMIN, ativo=True, telefone="5585988880000")
    obra = Obra(codigo="OBRAAGENTE", nome="Obra do agente")
    loc = Fornecedor(razao_social="Locadora do agente", cnpj_cpf="71000002000165",
                     tipo_pessoa=TipoPessoa.PJ,
                     regime_tributario=RegimeTributario.NAO_INFORMADO, ativo=True)
    s.add_all([ruan, dono, obra, loc]); s.flush()

    c = ContratoLocacao(numero="LOCAGENTE1", fornecedor_id=loc.id, obra_id=obra.id,
                        periodicidade="MENSAL", dia_vencimento=5,
                        data_inicio=date.today() - timedelta(days=120),
                        status="ATIVO", criado_por=ruan.id)
    s.add(c); s.flush()
    s.add(LocacaoItem(contrato_id=c.id, descricao="Betoneira do agente",
                      quantidade=Decimal("1"), quantidade_devolvida=Decimal("0"),
                      valor_unitario=Decimal("380.00"), obra_id=obra.id))
    s.flush()

    # competência de três meses atrás: bem depois do dia 15
    antiga = conf.competencia_de(date.today() - timedelta(days=90))
    conf.abrir_do_mes(s, competencia=antiga, usuario=dono)
    # o responsável é quem o contrato diz; força para o Ruan
    from app.apps.erp.db.models.financeiro import LocacaoConferencia
    for x in s.scalars(select(LocacaoConferencia)).all():
        if x.contrato_id == c.id:
            x.responsavel_id = ruan.id
    s.flush()

    # nada sai para o mundo durante o teste
    enviados = []
    monkeypatch.setattr(ag, "_enviar",
                        lambda texto, telefone: (enviados.append((telefone, texto)),
                                                 {"ok": True, "canais": {"teste": {"ok": True}}})[1])
    return {"s": s, "ruan": ruan, "dono": dono, "contrato": c, "enviados": enviados}


def test_a_conferencia_atrasada_vira_cobranca_de_verdade(cenario):
    s = cenario["s"]
    r = ag.varrer(s)
    assert r["enviadas"], "uma conferência de três meses atrás tem de ser cobrada"
    assert cenario["enviados"], "a mensagem tem de sair"
    telefone, texto = cenario["enviados"][0]
    assert "OBRAAGENTE" in texto and "LOCAGENTE1" in texto


def test_rodar_de_novo_no_mesmo_dia_nao_manda_nada(cenario):
    """É o teste que impede o agente de virar spam."""
    s = cenario["s"]
    primeira = ag.varrer(s)
    quantas = len(cenario["enviados"])
    assert quantas > 0

    segunda = ag.varrer(s)
    assert segunda["enviadas"] == [], "mandou de novo o mesmo degrau"
    assert len(cenario["enviados"]) == quantas, "saiu mensagem repetida"
    assert any("já enviado" in p["motivo"] for p in segunda["puladas"])


def test_o_banco_recusa_a_repeticao_mesmo_se_o_codigo_deixar_passar(cenario):
    """A segunda camada. Se duas execuções rodarem ao mesmo tempo, a pergunta
    do código responde 'ainda não mandei' nas duas — e é a chave única que
    segura."""
    from sqlalchemy.exc import IntegrityError
    s = cenario["s"]
    ag.varrer(s)
    gravada = s.scalars(select(AgenteMensagem)).all()[0]

    s.add(AgenteMensagem(
        assunto=gravada.assunto, referencia_id=gravada.referencia_id,
        destinatario_id=gravada.destinatario_id, degrau=gravada.degrau,
        texto="tentativa repetida", telefone=gravada.telefone))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_o_que_o_agente_falou_fica_registrado_com_nome_e_hora(cenario):
    """É a resposta para 'o Ruan foi cobrado?'."""
    s = cenario["s"]
    ag.varrer(s)
    linhas = ag.historico(s, assunto="locacao_conferencia")
    assert linhas, "nada ficou registrado"
    linha = linhas[0]
    assert linha["para"]
    assert linha["degrau"] in ("LEMBRETE", "COBRANCA", "ESCALADA")
    assert linha["entregue"] is True
    assert linha["quando"]
    assert "conferência" in linha["texto"].lower()
