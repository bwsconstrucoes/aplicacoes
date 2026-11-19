# -*- coding: utf-8 -*-
"""
OBRA CONCLUÍDA SAI DA FRENTE NO DIÁRIO — contra um Postgres de verdade.

Ele, em 19/11/2026: *"não tem sentido eu fazer diário de uma obra que está
concluída, uma obra que está concluída com dívida, que está só aguardando
recebimento provisório ou recebimento definitivo, ou que está apenas tratando
de acervo técnico, ou que foi destratado. (…) Eu não digo nem que proíba a
confecção do diário, mas que pelo menos a exibição não fique na tela inicial.
(…) Senão fica aquela lista imensa de obras que deveriam estar concluídas."*

⚑ POR QUE COM BANCO: a regra inteira mora no `WHERE` de duas consultas, e a
`SessaoFalsa` ignora `WHERE` — ela devolveria todas as obras e aprovaria um
código que não filtra nada. É exatamente o que se quer provar aqui.

⚠️ O DEFEITO QUE ISTO FECHA: até 19/11/2026 o filtro era só
`status <> 'ENCERRADA'` e a **fase era ignorada**. Obra marcada "Concluída" no
cadastro seguia no quadro, porque o status dela continuava ATIVA — ninguém
encerra obra no cadastro no mesmo dia em que ela acaba na prática.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core import diario as svc
from app.apps.erp.core.cadastros.obras import FASES_ENCERRADAS, FASES_OBRA

pytestmark = pytest.mark.banco


def _obra(s, codigo: str, fase: str) -> int:
    oid = s.execute(text("""
        INSERT INTO obras (codigo, nome, status, fase, data_inicio, data_termino)
        VALUES (:c, :n, 'ATIVA', :f, :ini, :fim)
        RETURNING id"""),
        {"c": codigo, "n": f"Obra {codigo}", "f": fase,
         "ini": date.today() - timedelta(days=200),
         "fim": date.today() + timedelta(days=200)}).scalar()
    s.flush()
    return oid


@pytest.fixture
def cenario(sessao_real):
    """Uma obra de cada lado da régua, mais uma sem fase nenhuma."""
    return {
        "andando": _obra(sessao_real, "AND-01", "EM_EXECUCAO"),
        "paralisada": _obra(sessao_real, "PAR-01", "PARALISADA"),
        "concluida": _obra(sessao_real, "CON-01", "CONCLUIDA"),
        "distratada": _obra(sessao_real, "DIS-01", "DISTRATADA"),
        # A coluna `fase` é NOT NULL com padrão CRIACAO, então "sem fase"
        # na prática é uma obra recém-cadastrada — e ela tem de aparecer.
        "recem_criada": _obra(sessao_real, "SEM-01", "CRIACAO"),
    }


# ---------------------------------------------------------------------------
# A RÉGUA EM SI
# ---------------------------------------------------------------------------
def test_as_quatro_fases_que_ele_chamou_de_iniciais_ficam_de_fora_da_regua():
    """*"criação/cadastro, aguardando a ordem de serviço, em execução, ou
    paralisada. Daí em diante, as demais fases são todas fases associadas à
    conclusão da obra."*

    Este teste existe para que acrescentar uma fase nova ao sistema obrigue
    alguém a decidir de que lado ela fica — em vez de ela entrar calada como
    "ainda andando" e voltar a poluir a tela.
    """
    andando = [k for k, _ in FASES_OBRA if k not in FASES_ENCERRADAS]
    assert andando == ["CRIACAO", "AGUARDANDO_OS", "EM_EXECUCAO", "PARALISADA"]


# ---------------------------------------------------------------------------
# O QUADRO DO MÊS — a tela inicial do diário
# ---------------------------------------------------------------------------
def test_o_quadro_do_mes_esconde_a_obra_concluida(sessao_real, cenario):
    hoje = date.today()
    q = svc.quadro_do_mes(sessao_real, hoje.year, hoje.month)
    codigos = {o["codigo"] for o in q["obras"]}
    assert "AND-01" in codigos and "PAR-01" in codigos
    assert "CON-01" not in codigos and "DIS-01" not in codigos


def test_obra_recem_cadastrada_continua_aparecendo(sessao_real, cenario):
    """A fase nasce em CRIACAO — e obra recém-cadastrada é justamente onde o
    diário começa. Sumir com ela seria esconder trabalho de verdade."""
    hoje = date.today()
    q = svc.quadro_do_mes(sessao_real, hoje.year, hoje.month)
    assert "SEM-01" in {o["codigo"] for o in q["obras"]}


def test_o_filtro_de_ver_todas_traz_a_obra_concluida_de_volta(sessao_real,
                                                              cenario):
    """ESCONDE, não proíbe: *"eu altero o filtro e visualizo todas, se eu
    quiser."* Quem tem diário atrasado de obra que já acabou precisa chegar
    nela."""
    hoje = date.today()
    q = svc.quadro_do_mes(sessao_real, hoje.year, hoje.month,
                          incluir_encerradas=True)
    codigos = {o["codigo"] for o in q["obras"]}
    assert {"AND-01", "PAR-01", "CON-01", "DIS-01", "SEM-01"} <= codigos


def test_todas_as_fases_de_conclusao_somem_juntas(sessao_real):
    """Uma por uma, porque esquecer UMA delas é o defeito provável — e ele
    nomeou as seis."""
    hoje = date.today()
    for i, fase in enumerate(FASES_ENCERRADAS):
        _obra(sessao_real, f"FIM-{i:02d}", fase)
    q = svc.quadro_do_mes(sessao_real, hoje.year, hoje.month)
    codigos = {o["codigo"] for o in q["obras"]}
    assert not [c for c in codigos if c.startswith("FIM-")], (
        "alguma fase de conclusão continuou aparecendo no quadro")


# ---------------------------------------------------------------------------
# O PAINEL DE "POR ONDE COMEÇAR"
# ---------------------------------------------------------------------------
def test_obra_concluida_nao_entra_na_conta_de_quem_deve_diario(sessao_real,
                                                               cenario):
    """Cobrar diário de obra que acabou é cobrança falsa — e some o número
    vermelho de quem está realmente atrasado no meio da lista."""
    linhas = svc.pendencias_por_obra(sessao_real)
    codigos = {l["codigo"] for l in linhas}
    assert "AND-01" in codigos
    assert "CON-01" not in codigos and "DIS-01" not in codigos


def test_as_pendencias_tambem_sabem_mostrar_todas(sessao_real, cenario):
    linhas = svc.pendencias_por_obra(sessao_real, incluir_encerradas=True)
    assert "CON-01" in {l["codigo"] for l in linhas}


# ---------------------------------------------------------------------------
# O QUE **NÃO** MUDOU, e é de propósito
# ---------------------------------------------------------------------------
def test_lancar_diario_em_obra_concluida_continua_permitido(sessao_real,
                                                            cenario):
    """*"Eu não digo nem que proíba a confecção do diário."* O filtro é de
    EXIBIÇÃO. Se isto um dia virar trava, é decisão dele, não efeito
    colateral."""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Quem lança", email="lanca@fases.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMINISTRATIVO_OBRA)
    sessao_real.add(u)
    sessao_real.flush()

    ontem = date.today() - timedelta(days=1)
    svc.salvar(sessao_real, cenario["concluida"], ontem,
               {"relato": "Retirada do canteiro, já com a obra concluída."}, u)
    sessao_real.flush()
    gravado = sessao_real.execute(text("""
        SELECT relato FROM diarios_obra WHERE obra_id = :o AND data = :d"""),
        {"o": cenario["concluida"], "d": ontem}).scalar()
    assert gravado and "canteiro" in gravado
