"""A conferência do mês chega à LISTA e à FICHA do contrato — com banco real.

POR QUE COM BANCO. O dublê de sessão ignora `WHERE` e devolve todos os objetos
do tipo pedido: ele confirma a regra, não a consulta. Duas vezes seguidas o
defeito esteve exatamente na consulta — a listagem citava uma variável que não
existia ali (a tela de locações abria vazia) e a ficha simplesmente não trazia
o bloco (o aviso nunca aparecia para quem ia lançar a parcela). Nenhum dos dois
apareceria sem executar o SQL de verdade.

O que se prova aqui, e que só o Postgres prova:

  1. `listar` devolve o bloco da conferência em cada contrato, sem estourar.
  2. `detalhar` devolve o MESMO bloco — lista e ficha não podem divergir.
  3. Respondida a conferência, os dois passam a dizer "conferido".
  4. A previsão de devolução por item vai e volta do banco, e o item vencido
     aparece marcado como atrasado.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.apps.erp.core import locacoes as svc
from app.apps.erp.core import locacoes_conferencia as conf
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Obra, PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import ContratoLocacao, LocacaoItem

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Ruan do administrativo", email="ruan@teste.local",
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=P.ADMINISTRATIVO_OBRA, ativo=True)
    obra = Obra(codigo="OBRATESTE", nome="Obra de teste")
    loc = Fornecedor(razao_social="Locadora Teste Ltda", cnpj_cpf="71000001000184",
                     tipo_pessoa=TipoPessoa.PJ,
                     regime_tributario=RegimeTributario.SIMPLES, ativo=True)
    s.add_all([u, obra, loc])
    s.flush()

    c = ContratoLocacao(numero="LOCTESTE1", fornecedor_id=loc.id, obra_id=obra.id,
                        periodicidade="MENSAL", dia_vencimento=5,
                        data_inicio=date.today() - timedelta(days=40),
                        status="ATIVO", criado_por=u.id)
    s.add(c)
    s.flush()
    hoje = date.today()
    s.add_all([
        LocacaoItem(contrato_id=c.id, descricao="Betoneira 400L",
                    quantidade=Decimal("2"), quantidade_devolvida=Decimal("0"),
                    valor_unitario=Decimal("380.00"), obra_id=obra.id,
                    devolucao_prevista=hoje + timedelta(days=30),
                    devolucao_prevista_original=hoje + timedelta(days=30)),
        LocacaoItem(contrato_id=c.id, descricao="Gerador 15 kVA",
                    quantidade=Decimal("1"), quantidade_devolvida=Decimal("0"),
                    valor_unitario=Decimal("2400.00"), obra_id=obra.id,
                    devolucao_prevista=hoje - timedelta(days=7),
                    devolucao_prevista_original=hoje - timedelta(days=7)),
    ])
    s.flush()
    return {"s": s, "usuario": u, "obra": obra, "contrato": c}


def test_a_lista_de_contratos_traz_a_conferencia_do_mes(cenario):
    s, c, u = cenario["s"], cenario["contrato"], cenario["usuario"]
    conf.abrir_do_mes(s, usuario=u)

    linhas = [l for l in svc.listar(s) if l["numero"] == "LOCTESTE1"]
    assert len(linhas) == 1, "o contrato tem de aparecer na tela"
    bloco = linhas[0]["conferencia"]
    assert bloco["conferido"] is False
    assert bloco["conferencia_id"]
    assert "ainda não foi respondida" in bloco["aviso"]


def test_a_ficha_diz_a_mesma_coisa_que_a_lista(cenario):
    s, c, u = cenario["s"], cenario["contrato"], cenario["usuario"]
    conf.abrir_do_mes(s, usuario=u)

    da_lista = [l for l in svc.listar(s) if l["id"] == c.id][0]["conferencia"]
    da_ficha = svc.detalhar(s, c.id)["conferencia"]
    assert da_ficha is not None, "a ficha do contrato tem de trazer o bloco"
    assert da_ficha["conferido"] == da_lista["conferido"]
    assert da_ficha["conferencia_id"] == da_lista["conferencia_id"]


def test_respondida_a_conferencia_lista_e_ficha_param_de_cobrar(cenario):
    s, c, u = cenario["s"], cenario["contrato"], cenario["usuario"]
    conf.abrir_do_mes(s, usuario=u)
    aberta = s.scalars(select(conf.LocacaoConferencia)).all()[0]
    itens = s.scalars(select(LocacaoItem).where(
        LocacaoItem.contrato_id == c.id)).all()

    conf.responder(s, aberta.id, {"itens": [
        {"item_id": i.id, "presente": "SIM", "em_uso": True,
         "onde_esta": "Térreo, bloco A", "decisao": "MANTER"} for i in itens]},
        usuario=u)

    assert svc.detalhar(s, c.id)["conferencia"]["conferido"] is True
    da_lista = [l for l in svc.listar(s) if l["id"] == c.id][0]
    assert da_lista["conferencia"]["conferido"] is True


def test_a_previsao_de_devolucao_vai_e_volta_e_o_vencido_sai_marcado(cenario):
    s, c = cenario["s"], cenario["contrato"]
    itens = {i["descricao"]: i for i in svc.detalhar(s, c.id)["itens"]}

    assert itens["Betoneira 400L"]["devolucao_prevista"]
    assert itens["Betoneira 400L"]["atrasado"] is False
    assert itens["Gerador 15 kVA"]["atrasado"] is True, \
        "equipamento com devolução vencida tem de aparecer marcado"


def test_o_alerta_do_contrato_cita_o_equipamento_vencido(cenario):
    s, c = cenario["s"], cenario["contrato"]
    alertas = " ".join(a["msg"] for a in svc.detalhar(s, c.id)["alertas"])
    assert "Gerador" in alertas and "devol" in alertas.lower()
