"""A conferência mensal dos equipamentos locados.

O problema que ela resolve, nas palavras do dono: "muitas vezes eles são
locados e deixam de ser utilizados, não são devolvidos". O sistema já gritava
"10 meses locado, o aluguel já paga a compra" — o que faltava era ALGUÉM SER
OBRIGADO A RESPONDER.

O que estes testes seguram:

  - a conferência é UMA por contrato por mês, e abrir de novo não duplica;
  - "onde está e para quê" é obrigatório: "está aí" não é resposta, e é esse
    campo que faz a diferença entre prestar contas e carimbar;
  - adiar a devolução exige motivo, e a data ORIGINAL não se perde;
  - "devolver" e "remanejar" na conferência FAZEM acontecer — conferência que
    registra intenção e não faz nada é papel;
  - o financeiro é avisado quando a obra não conferiu (avisado, não bloqueado).
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core import locacoes_conferencia as svc
from app.apps.erp.db.models.cadastros import Obra, PerfilUsuario as P
from app.apps.erp.db.models.financeiro import (
    ContratoLocacao, LocacaoConferencia, LocacaoConferenciaItem, LocacaoItem,
)

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Ruan do administrativo")
HOJE = date.today()
COMP = date(HOJE.year, HOJE.month, 1)


def contrato(**extra):
    dados = dict(id=5, numero="LOC00005", fornecedor_id=9, obra_id=1,
                 status="ATIVO", data_inicio=HOJE - timedelta(days=60),
                 responsavel_id=ADMIN.id)
    dados.update(extra)
    return ContratoLocacao(**dados)


def item(**extra):
    dados = dict(id=50, contrato_id=5, descricao="Escora metálica 3,00m",
                 quantidade=Decimal("120"), quantidade_devolvida=Decimal("0"),
                 valor_unitario=Decimal("7.20"), obra_id=1)
    dados.update(extra)
    return LocacaoItem(**dados)


def conferencia(**extra):
    dados = dict(id=70, contrato_id=5, competencia=COMP, obra_id=1,
                 responsavel_id=ADMIN.id, situacao="ABERTA")
    dados.update(extra)
    return LocacaoConferencia(**dados)


OBRA = Obra(id=1, codigo="CREPETRIUNFO", nome="Creche Triunfo", status="ATIVA")
OUTRA = Obra(id=2, codigo="ESCPLANALTO", nome="Escola do Planalto", status="ATIVA")


def sessao(*objetos):
    return SessaoFalsa(OBRA, OUTRA, ADMIN, *objetos)


# ---------------------------------------------------------------------------
# Abrir
# ---------------------------------------------------------------------------
def test_abre_uma_conferencia_por_contrato_ativo():
    c = contrato()
    s = sessao(c, item())
    r = svc.abrir_do_mes(s)
    assert r["abertas"] == ["LOC00005"]
    assert len(s.adicionados) == 1


def test_abrir_de_novo_nao_duplica():
    """A rotina vai rodar todo dia. Duas conferências do mesmo mês fariam a
    obra responder a mesma coisa duas vezes e não saber qual vale."""
    s = sessao(contrato(), conferencia())
    assert svc.abrir_do_mes(s)["abertas"] == []
    assert s.adicionados == []


def test_contrato_encerrado_nao_gera_conferencia():
    s = sessao(contrato(status="ENCERRADO"))
    assert svc.abrir_do_mes(s)["abertas"] == []


def test_contrato_que_comecou_depois_do_mes_nao_gera():
    s = sessao(contrato(data_inicio=HOJE + timedelta(days=90)))
    assert svc.abrir_do_mes(s)["abertas"] == []


def test_a_conferencia_nasce_endereçada_a_alguem():
    """Pendência sem dono ninguém lê."""
    s = sessao(contrato())
    svc.abrir_do_mes(s)
    assert s.adicionados[0].responsavel_id == ADMIN.id


# ---------------------------------------------------------------------------
# Responder — o que é obrigatório
# ---------------------------------------------------------------------------
def responder(s, **campos):
    base = {"item_id": 50, "presente": "SIM", "em_uso": True,
            "onde_esta": "na laje do bloco B, escorando até a desforma",
            "decisao": "MANTER"}
    base.update(campos)
    return svc.responder(s, 70, {"itens": [base]}, ADMIN)


def test_onde_esta_e_obrigatorio_quando_o_equipamento_esta_na_obra():
    """"está aí" não é resposta. É esse campo que separa prestar contas de
    carimbar um formulário."""
    s = sessao(contrato(), item(), conferencia())
    with pytest.raises(ErroValidacao, match="onde está"):
        responder(s, onde_esta="")


def test_dizer_que_nao_esta_exige_explicacao():
    s = sessao(contrato(), item(), conferencia())
    with pytest.raises(ErroValidacao, match="o que aconteceu"):
        responder(s, presente="NAO", onde_esta="", motivo="")


def test_resposta_sem_dizer_se_esta_na_obra_e_recusada():
    s = sessao(contrato(), item(), conferencia())
    with pytest.raises(ErroValidacao, match="diga se está na obra"):
        responder(s, presente="")


def test_conferencia_ja_respondida_nao_e_respondida_de_novo():
    s = sessao(contrato(), item(), conferencia(situacao="RESPONDIDA"))
    with pytest.raises(ErroValidacao, match="já foi respondida"):
        responder(s)


def test_conferencia_sem_item_nenhum_e_recusada():
    s = sessao(contrato(), item(), conferencia())
    with pytest.raises(ErroValidacao, match="pelo menos um"):
        svc.responder(s, 70, {"itens": []}, ADMIN)


# ---------------------------------------------------------------------------
# Responder — o que fica gravado
# ---------------------------------------------------------------------------
def test_a_resposta_fica_gravada_com_quem_respondeu():
    conf = conferencia()
    s = sessao(contrato(), item(), conf)
    r = responder(s)
    assert conf.situacao == "RESPONDIDA"
    assert conf.respondida_por == ADMIN.id
    assert conf.respondida_em is not None
    assert "registrada" in r["resumo"]


def test_adiar_a_devolucao_exige_motivo():
    """Prorrogar é normal; prorrogar em silêncio é que não pode."""
    i = item(devolucao_prevista=HOJE)
    s = sessao(contrato(), i, conferencia())
    with pytest.raises(ErroValidacao, match="motivo"):
        responder(s, devolucao_prevista=(HOJE + timedelta(days=30)).isoformat(),
                  motivo="")


def test_adiar_guarda_a_data_original():
    """Depois de duas ou três prorrogações, a data original é o que denuncia."""
    i = item(devolucao_prevista=HOJE)
    s = sessao(contrato(), i, conferencia())
    nova = HOJE + timedelta(days=30)
    r = responder(s, devolucao_prevista=nova.isoformat(),
                  motivo="a desforma atrasou por causa da chuva")
    assert i.devolucao_prevista == nova
    assert i.devolucao_prevista_original == HOJE
    assert r["avisos"] and "adiada" in r["avisos"][0]


# ---------------------------------------------------------------------------
# A conferência FAZ acontecer
# ---------------------------------------------------------------------------
def test_devolver_na_conferencia_devolve_de_verdade(monkeypatch):
    """Conferência que registra intenção e não faz nada é papel — e papel não
    devolve equipamento."""
    chamadas = []
    from app.apps.erp.core import locacoes as svc_contrato
    monkeypatch.setattr(svc_contrato, "devolver",
                        lambda s, cid, d, u: chamadas.append(("devolver", cid, d)))
    monkeypatch.setattr(svc_contrato, "remanejar", lambda *a, **k: None)
    s = sessao(contrato(), item(), conferencia())
    r = responder(s, decisao="DEVOLVER", presente="SIM",
                  onde_esta="parado no canteiro desde a desforma")
    assert chamadas and chamadas[0][0] == "devolver"
    assert chamadas[0][2]["quantidade"] == "120"
    assert any("devolução" in a for a in r["acoes"])


def test_remanejar_na_conferencia_remaneja_de_verdade(monkeypatch):
    chamadas = []
    from app.apps.erp.core import locacoes as svc_contrato
    monkeypatch.setattr(svc_contrato, "remanejar",
                        lambda s, cid, d, u: chamadas.append(("remanejar", d)))
    monkeypatch.setattr(svc_contrato, "devolver", lambda *a, **k: None)
    s = sessao(contrato(), item(), conferencia())
    responder(s, decisao="REMANEJAR", obra_destino_id=2,
              onde_esta="parado, a laje já desformou")
    assert chamadas[0][1]["obra_destino_id"] == 2


def test_remanejar_sem_dizer_para_onde_e_recusado(monkeypatch):
    from app.apps.erp.core import locacoes as svc_contrato
    monkeypatch.setattr(svc_contrato, "remanejar", lambda *a, **k: None)
    monkeypatch.setattr(svc_contrato, "devolver", lambda *a, **k: None)
    s = sessao(contrato(), item(), conferencia())
    with pytest.raises(ErroValidacao, match="para qual obra"):
        responder(s, decisao="REMANEJAR", onde_esta="parado no canteiro")


def test_nao_encontrado_vira_alerta_e_nao_some(monkeypatch):
    """Sumir com o equipamento faria todo mundo esquecer dele — e é justamente
    a cobrança que faz alguém procurar."""
    from app.apps.erp.core import locacoes as svc_contrato
    monkeypatch.setattr(svc_contrato, "devolver", lambda *a, **k: None)
    monkeypatch.setattr(svc_contrato, "remanejar", lambda *a, **k: None)
    s = sessao(contrato(), item(), conferencia())
    r = responder(s, presente="NAO_ENCONTRADO", onde_esta="",
                  motivo="ninguém sabe dizer onde foi parar")
    assert any("NÃO ENCONTRADO" in a for a in r["acoes"])


# ---------------------------------------------------------------------------
# O aviso ao financeiro
# ---------------------------------------------------------------------------
def test_sem_conferencia_o_financeiro_e_avisado():
    """Avisado, não bloqueado: bloquear o pagamento trocaria equipamento
    esquecido por multa e briga com a locadora."""
    r = svc.houve_conferencia(sessao(contrato()), 5)
    assert r["conferido"] is False
    assert "não conferiu" in r["aviso"]


def test_conferencia_aberta_e_nao_respondida_tambem_avisa():
    r = svc.houve_conferencia(sessao(contrato(), conferencia()), 5)
    assert r["conferido"] is False
    assert "ainda" in r["aviso"] and "Ruan" in r["aviso"]


def test_conferencia_respondida_nao_avisa():
    from datetime import datetime, timezone
    conf = conferencia(situacao="RESPONDIDA",
                       respondida_em=datetime.now(timezone.utc))
    r = svc.houve_conferencia(sessao(contrato(), conf), 5)
    assert r["conferido"] is True


# ---------------------------------------------------------------------------
# A lista do que espera resposta
# ---------------------------------------------------------------------------
def test_a_pendencia_conta_os_dias_e_diz_quando_cobrar():
    antiga = conferencia(competencia=date(2026, 1, 1))
    lista = svc.pendentes(sessao(contrato(), antiga), todas=True)
    assert lista[0]["dias_de_atraso"] > 0
    assert lista[0]["cobrar"] is True
    assert lista[0]["responsavel"] == "Ruan do administrativo"


def test_sem_todas_a_lista_e_so_de_quem_pergunta():
    outra_pessoa = novo_usuario(2, P.LANCADOR, nome="Outra pessoa")
    s = SessaoFalsa(OBRA, ADMIN, outra_pessoa, contrato(), conferencia())
    assert svc.pendentes(s, usuario=outra_pessoa) == []
    assert len(svc.pendentes(s, usuario=ADMIN)) == 1


def test_conferencia_respondida_sai_da_lista():
    s = sessao(contrato(), conferencia(situacao="RESPONDIDA"))
    assert svc.pendentes(s, todas=True) == []


# ---------------------------------------------------------------------------
# A competência
# ---------------------------------------------------------------------------
def test_a_competencia_e_o_primeiro_dia_do_mes():
    assert svc.competencia_de(date(2026, 9, 23)) == date(2026, 9, 1)
