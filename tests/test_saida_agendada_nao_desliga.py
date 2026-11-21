# -*- coding: utf-8 -*-
"""SAÍDA MARCADA PARA O FUTURO NÃO DESLIGA HOJE.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026).

Ele, horas depois de ligar o cadastro do ERP como base do ponto:

> *"Pessoas que estavam com último dia trabalhado mesmo depois de hoje e
> estavam na fase Solicitações DP, quando ativei o cadastro do ERP foram
> considerados desligados."*

Estava certo, e o estrago é dos piores que este sistema pode fazer: **gente
cumprindo aviso prévio parou de conseguir bater ponto**. Quem cumpre aviso
prévio TRABALHA — e a jornada dela é registro legal do REP-P, que não pode ter
buraco. Pelo mesmo erro ela saía da FOLHA (`para_folha` descartava quem tinha
data de saída) e perdia o acesso ao sistema.

**A causa, em quatro lugares diferentes:** cada um escrevia "DESLIGADO" ao ver
que EXISTIA uma data de saída, **sem olhar se ela já tinha chegado**. Uma saída
marcada para a semana que vem valia como saída de ontem.

**Por que só apareceu quando a base do ponto virou o ERP:** com a planilha, o
ponto PERGUNTAVA a data (`r.data_saida < hoje`); com o ERP, ele acreditava na
caixinha. A caixinha estava errada desde a importação — a troca de base só
tornou isso visível.

**A régua é uma só agora** (`pessoas.ja_saiu`), e é a MESMA do ponto: `<` hoje,
porque **a pessoa trabalha o último dia**.
"""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.erp.core.cadastros import pessoas as svc

banco_ = pytest.mark.banco

CPF_A = "52998224725"
CPF_B = "11144477735"

HOJE = dt.date.today()
ONTEM = HOJE - dt.timedelta(days=1)
SEMANA_QUE_VEM = HOJE + dt.timedelta(days=7)


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP do aviso previo", email="dp@aviso.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000001")
    sessao_real.add(u)
    sessao_real.flush()
    return u


# ---------------------------------------------------------------------------
# A RÉGUA, sem banco
# ---------------------------------------------------------------------------
def test_sem_data_de_saida_a_pessoa_nao_saiu():
    assert svc.ja_saiu(None) is False
    assert svc.saida_agendada(None) is False


def test_saida_de_ontem_e_saida():
    assert svc.ja_saiu(ONTEM) is True
    assert svc.saida_agendada(ONTEM) is False


def test_saida_de_HOJE_ainda_nao_e_saida():
    """A pessoa TRABALHA o último dia — é a régua que o ponto já usava com a
    planilha (`r.data_saida < hoje`). Trocar para `<=` tiraria do ponto quem
    está trabalhando o último dia dela, e a batida desse dia é o registro que
    a rescisão usa."""
    assert svc.ja_saiu(HOJE) is False
    assert svc.saida_agendada(HOJE) is True


def test_saida_da_semana_que_vem_e_aviso_previo():
    assert svc.ja_saiu(SEMANA_QUE_VEM) is False
    assert svc.saida_agendada(SEMANA_QUE_VEM) is True


# ---------------------------------------------------------------------------
# O CADASTRO: gravar data futura não desliga
# ---------------------------------------------------------------------------
@banco_
def test_gravar_saida_no_futuro_mantem_a_pessoa_ativa(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Pessoa Em Aviso Previo",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Pessoa Em Aviso Previo",
                                 "demissao": SEMANA_QUE_VEM.isoformat()}, dono)
    sessao_real.flush()

    assert c.demissao == SEMANA_QUE_VEM        # a data entra
    assert (c.situacao or "").upper() == "ATIVO"   # e a pessoa continua trabalhando


@banco_
def test_gravar_saida_que_ja_passou_desliga(sessao_real, dono):
    """A outra metade da regra: data vencida e caixinha em ATIVO é cadastro que
    mente para quem monta a folha."""
    svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Pessoa Que Saiu Mesmo",
                             "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    c = svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Pessoa Que Saiu Mesmo",
                                 "demissao": ONTEM.isoformat()}, dono)
    sessao_real.flush()

    assert (c.situacao or "").upper() == "DESLIGADO"


# ---------------------------------------------------------------------------
# A FOLHA: quem cumpre aviso prévio continua recebendo
# ---------------------------------------------------------------------------
@banco_
def test_quem_esta_em_aviso_previo_continua_na_folha(sessao_real, dono):
    """⚠️ ESTE É O TESTE DO DINHEIRO. Quem cai fora de `para_folha(so_ativos)`
    não entra na folha do mês — e quem está em aviso prévio trabalhou."""
    svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Pessoa Em Aviso Previo",
                             "admissao": "2026-01-05", "salario": "2.000,00",
                             "demissao": SEMANA_QUE_VEM.isoformat()}, dono)
    sessao_real.flush()

    cpfs = [p["cpf"] for p in svc.para_folha(sessao_real, so_ativos=True)]
    assert CPF_A in cpfs


@banco_
def test_quem_ja_saiu_fica_fora_da_folha(sessao_real, dono):
    svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Pessoa Que Saiu Mesmo",
                             "admissao": "2026-01-05",
                             "demissao": ONTEM.isoformat()}, dono)
    sessao_real.flush()

    cpfs = [p["cpf"] for p in svc.para_folha(sessao_real, so_ativos=True)]
    assert CPF_B not in cpfs


# ---------------------------------------------------------------------------
# DESLIGAR PELA TELA com data futura: agenda, não executa
# ---------------------------------------------------------------------------
@banco_
def test_desligar_com_data_futura_agenda_sem_tirar_a_pessoa_da_ativa(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Pessoa Em Aviso Previo",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    etapa_antes = c.etapa

    svc.desligar(sessao_real, c.id, {
        "data_saida": SEMANA_QUE_VEM.isoformat(),
        "ultimo_dia": (SEMANA_QUE_VEM - dt.timedelta(days=2)).isoformat(),
        "aviso_previo": HOJE.isoformat(),
        "forma_desligamento": "AVISO_TRABALHADO"}, dono)
    sessao_real.flush()

    assert (c.situacao or "").upper() == "ATIVO"      # trabalha o aviso prévio
    assert c.demissao == SEMANA_QUE_VEM              # a data fica gravada
    assert c.ultimo_dia is not None and c.aviso_previo is not None
    # a etapa não vai para "Desligado": o cadastro dela é trabalho pendente do DP
    assert (c.etapa or "").upper() != "DESLIGADO"
    assert c.etapa == etapa_antes


@banco_
def test_desligar_com_data_que_ja_passou_desliga_de_verdade(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Pessoa Que Saiu Mesmo",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()

    svc.desligar(sessao_real, c.id, {"data_saida": ONTEM.isoformat(),
                                     "forma_desligamento": "AVISO_INDENIZADO"}, dono)
    sessao_real.flush()

    assert (c.situacao or "").upper() == "DESLIGADO"
    assert (c.etapa or "").upper() == "DESLIGADO"


# ---------------------------------------------------------------------------
# VOLTAR PARA A ATIVA não pode cair numa etapa que a tela não mostra
# ---------------------------------------------------------------------------
@banco_
def test_quem_volta_para_a_ativa_cai_numa_etapa_QUE_EXISTE(sessao_real, dono):
    """⚠️ Era "EM_CONFERENCIA", e em 17/11/2026 essa etapa saiu de circulação
    (migração 107, "Conferência" fundida na "Inicial"). Quem voltasse para a
    ativa caía numa etapa SEM COLUNA NO QUADRO — desaparecia da fila do DP."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Pessoa Que Voltou",
                                 "admissao": "2026-01-05",
                                 "demissao": ONTEM.isoformat()}, dono)
    sessao_real.flush()
    assert (c.etapa or "").upper() == "DESLIGADO"

    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Pessoa Que Voltou",
                                 "situacao": "ATIVO"}, dono)
    sessao_real.flush()

    assert c.etapa in svc.ETAPAS, (
        f"etapa {c.etapa!r} não está em ETAPAS — não tem coluna no quadro, "
        f"e a pessoa desaparece da fila do DP")


# ---------------------------------------------------------------------------
# A IMPORTAÇÃO DA PLANILHA DO DP — foi por aqui que a maioria passou
# ---------------------------------------------------------------------------
def test_a_importacao_nao_desliga_quem_tem_saida_no_futuro():
    from app.apps.erp.core.importadores import planilha_dp as imp

    assert imp._ja_passou(ONTEM.isoformat()) is True
    assert imp._ja_passou(HOJE.isoformat()) is False
    assert imp._ja_passou(SEMANA_QUE_VEM.isoformat()) is False
    # data ilegível NÃO vale como passada: na dúvida a pessoa continua ativa
    assert imp._ja_passou("31/02/2026") is False
    assert imp._ja_passou(None) is False


# ---------------------------------------------------------------------------
# A GUARDA: ninguém volta a comparar a data à mão
# ---------------------------------------------------------------------------
def test_nenhum_lugar_escreve_DESLIGADO_olhando_so_se_a_data_existe():
    """Os quatro lugares do estrago tinham a MESMA linha, escrita quatro vezes.
    Este teste recusa a quinta."""
    import re
    from pathlib import Path

    raiz = Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
    suspeitas = []
    for arq in raiz.rglob("*.py"):
        texto = arq.read_text(encoding="utf-8")
        for n, linha in enumerate(texto.splitlines(), 1):
            # `if c.demissao and ...` / `if c.demissao:` — a data só EXISTINDO
            if re.search(r"if\s+\w+\.demissao\s*(and|:)", linha) and "ja_saiu" not in linha:
                suspeitas.append(f"{arq.relative_to(raiz)}:{n}: {linha.strip()}")

    assert suspeitas == [], (
        "estes lugares decidem olhando só se a data de saída EXISTE:\n  "
        + "\n  ".join(suspeitas)
        + "\n  Use `cadastros.pessoas.ja_saiu(...)`: data no futuro é aviso "
        "prévio, e a pessoa trabalha, bate ponto e entra na folha.")
