# -*- coding: utf-8 -*-
"""
MUDAR A FASE DE UMA OBRA — contra um Postgres de verdade.

A regra saiu do `routes.py` para `core/cadastros/obras.py` em 24/11/2026,
pela regra da casa (regra de negócio mora em `core/<dominio>/`). Ela tem
efeitos colaterais que ninguém vê na tela e que importam muito:

  · o histórico em `obra_fases` — que é documento, não rascunho;
  · a trilha de auditoria;
  · as datas de conclusão e de recebimento.

⚑ POR QUE COM BANCO: a `SessaoFalsa` não enxerga nada disso.

⚠️ E POR QUE ESTES TESTES VÃO IMPORTAR MAIS DEPOIS: nem o CSV de obras nem a
C. Diários trazem fase, então toda obra importada nasceu em CRIACAO e lá
ficou. Quando a fase vier de fora — do Pipefy, enquanto ele existir —, vai
entrar por esta mesma função, em cima de centenas de obras de uma vez. Os
dois casos abaixo são os que estragam dado em silêncio nessa hora: regravar
a fase que já está lá (enche o histórico de mudança que não houve) e
carimbar a data de hoje como conclusão de obra que acabou ano passado.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import obras as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao

pytestmark = pytest.mark.banco


def _obra(s, codigo: str, fase: str = "CRIACAO") -> int:
    oid = s.execute(text("""
        INSERT INTO obras (codigo, nome, status, fase)
        VALUES (:c, :n, 'ATIVA', :f) RETURNING id"""),
        {"c": codigo, "n": f"Obra {codigo}", "f": fase}).scalar()
    s.flush()
    return oid


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Quem acerta", email="acerta@fases.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMIN)
    sessao_real.add(u)
    sessao_real.flush()
    return u


def _historico(s, obra_id) -> list[str]:
    return [l[0] for l in s.execute(text(
        "SELECT fase FROM obra_fases WHERE obra_id = :o ORDER BY id"),
        {"o": obra_id}).all()]


# ---------------------------------------------------------------------------
# A FUNÇÃO QUE AS DUAS PORTAS USAM
# ---------------------------------------------------------------------------
def test_mudar_de_fase_guarda_o_historico_e_a_data(sessao_real, dono):
    o = _obra(sessao_real, "LOTE-01")
    r = svc.mudar_fase(sessao_real, o, "EM_EXECUCAO", dono)
    sessao_real.flush()
    assert r == {"mudou": True, "de": "CRIACAO", "para": "EM_EXECUCAO"}
    linha = sessao_real.execute(text(
        "SELECT fase, fase_desde FROM obras WHERE id = :o"), {"o": o}).first()
    assert linha[0] == "EM_EXECUCAO" and linha[1] == date.today()
    assert _historico(sessao_real, o) == ["EM_EXECUCAO"]


def test_a_mesma_fase_de_novo_nao_faz_nada(sessao_real, dono):
    """⚠️ É O QUE PROTEGE O ACERTO EM MASSA: o histórico de fase é documento.
    Regravar a fase que já está lá encheria o histórico de toda obra com uma
    mudança que não houve."""
    o = _obra(sessao_real, "LOTE-02", "EM_EXECUCAO")
    r = svc.mudar_fase(sessao_real, o, "EM_EXECUCAO", dono)
    sessao_real.flush()
    assert r["mudou"] is False
    assert _historico(sessao_real, o) == []


def test_a_data_de_conclusao_nao_e_reescrita(sessao_real, dono):
    """Obra que acabou ano passado não pode receber a data de hoje como data
    de conclusão só porque alguém passou pelo acerto em lote."""
    o = _obra(sessao_real, "LOTE-03", "EM_EXECUCAO")
    antiga = date.today() - timedelta(days=400)
    sessao_real.execute(text(
        "UPDATE obras SET data_conclusao = :d WHERE id = :o"),
        {"d": antiga, "o": o})
    svc.mudar_fase(sessao_real, o, "CONCLUIDA", dono)
    sessao_real.flush()
    assert sessao_real.execute(text(
        "SELECT data_conclusao FROM obras WHERE id = :o"),
        {"o": o}).scalar() == antiga


def test_a_data_de_conclusao_nasce_quando_estava_vazia(sessao_real, dono):
    o = _obra(sessao_real, "LOTE-04", "EM_EXECUCAO")
    svc.mudar_fase(sessao_real, o, "CONCLUIDA", dono)
    sessao_real.flush()
    assert sessao_real.execute(text(
        "SELECT data_conclusao FROM obras WHERE id = :o"),
        {"o": o}).scalar() == date.today()


def test_fase_inventada_e_recusada(sessao_real, dono):
    o = _obra(sessao_real, "LOTE-05")
    with pytest.raises(ErroValidacao):
        svc.mudar_fase(sessao_real, o, "QUASE_PRONTA", dono)
