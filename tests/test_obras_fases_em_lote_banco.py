# -*- coding: utf-8 -*-
"""
ACERTAR A FASE DE VÁRIAS OBRAS DE UMA VEZ — contra um Postgres de verdade.

Ele, em 24/11/2026: *"quando importei as obras pro sistema, o sistema não
copiou as informações de fase, e estão todas desatualizadas. Essa importação
deve ter sido da planilha C. Diários, como fazer pra atualizar?"*

E era isso mesmo: nem o CSV de obras (`codigo,nome,cno,municipio,uf,
codigo_omie_depto`) nem a C. Diários trazem fase. Toda obra nasceu no padrão
da coluna — CRIACAO — e lá ficou. Acertar uma a uma seria abrir a ficha de
cada obra.

⚑ POR QUE COM BANCO: o que importa aqui é o EFEITO COLATERAL de mudar de
fase — o histórico em `obra_fases`, a trilha de auditoria e as datas de
conclusão e recebimento. A `SessaoFalsa` não enxerga nada disso.

⚠️ O RISCO QUE ESTES TESTES GUARDAM: um acerto em massa que escreva no
histórico de fases de TODA obra, ou que carimbe a data de hoje como data de
conclusão de obra que acabou ano passado, estraga dado que ninguém vai
conferir linha a linha.
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


# ---------------------------------------------------------------------------
# A PORTA DO LOTE
# ---------------------------------------------------------------------------
def test_o_lote_muda_varias_e_conta_certo(app_real, sessao_real, dono):
    from tests.conftest import como                      # type: ignore
    a = _obra(sessao_real, "LOTE-10")
    b = _obra(sessao_real, "LOTE-11")
    c = _obra(sessao_real, "LOTE-12", "EM_EXECUCAO")

    r = como(app_real, dono.id).post("/erp/api/obras/fases", json={"fases": [
        {"obra_id": a, "fase": "EM_EXECUCAO"},
        {"obra_id": b, "fase": "CONCLUIDA"},
        # esta já está na fase pedida: entra na lista e não conta como mudança
        {"obra_id": c, "fase": "EM_EXECUCAO"},
    ], "observacao": "acerto das fases da importação"})
    assert r.status_code == 200, r.get_json()
    assert r.get_json()["mudadas"] == 2
    assert r.get_json()["sem_mudanca"] == 1

    sessao_real.expire_all()
    fases = dict(sessao_real.execute(text(
        "SELECT id, fase FROM obras WHERE id = ANY(:ids)"),
        {"ids": [a, b, c]}).all())
    assert fases[a] == "EM_EXECUCAO" and fases[b] == "CONCLUIDA"
    assert _historico(sessao_real, c) == [], "a que não mudou não vira registro"


def test_a_observacao_vai_para_o_historico_de_todas(app_real, sessao_real,
                                                    dono):
    from tests.conftest import como                      # type: ignore
    a = _obra(sessao_real, "LOTE-20")
    b = _obra(sessao_real, "LOTE-21")
    como(app_real, dono.id).post("/erp/api/obras/fases", json={"fases": [
        {"obra_id": a, "fase": "PARALISADA"},
        {"obra_id": b, "fase": "PARALISADA"}],
        "observacao": "acerto da importação"})
    sessao_real.expire_all()
    for o in (a, b):
        assert sessao_real.execute(text(
            "SELECT observacao FROM obra_fases WHERE obra_id = :o"),
            {"o": o}).scalar() == "acerto da importação"


def test_uma_fase_errada_no_meio_nao_grava_nenhuma(app_real, sessao_real,
                                                   dono):
    """⚑ TUDO OU NADA. Meio acerto aplicado, sem saber onde parou, é pior que
    nenhum — e quem acerta oitenta obras não tem como conferir uma a uma."""
    from tests.conftest import como                      # type: ignore
    a = _obra(sessao_real, "LOTE-30")
    b = _obra(sessao_real, "LOTE-31")
    r = como(app_real, dono.id).post("/erp/api/obras/fases", json={"fases": [
        {"obra_id": a, "fase": "EM_EXECUCAO"},
        {"obra_id": b, "fase": "NAO_EXISTE"}]})
    assert r.status_code == 400

    sessao_real.expire_all()
    assert sessao_real.execute(text("SELECT fase FROM obras WHERE id = :o"),
                               {"o": a}).scalar() == "CRIACAO"
    assert _historico(sessao_real, a) == []


def test_lista_vazia_e_recusada(app_real, dono):
    from tests.conftest import como                      # type: ignore
    assert como(app_real, dono.id).post(
        "/erp/api/obras/fases", json={"fases": []}).status_code == 400


def test_quem_nao_configura_nao_acerta_fase(app_real, sessao_real):
    """A rota declara `configurar`, e é o guard que recusa — não um `if`
    dentro dela."""
    from tests.conftest import como                      # type: ignore
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    o = _obra(sessao_real, "LOTE-40")
    lancador = Usuario(nome="Lançador", email="lancador@fases.teste",
                       ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                       perfil=PerfilUsuario.LANCADOR)
    sessao_real.add(lancador)
    sessao_real.flush()

    r = como(app_real, lancador.id).post("/erp/api/obras/fases", json={
        "fases": [{"obra_id": o, "fase": "CONCLUIDA"}]})
    assert r.status_code in (403, 404), r.status_code
    sessao_real.expire_all()
    assert sessao_real.execute(text("SELECT fase FROM obras WHERE id = :o"),
                               {"o": o}).scalar() == "CRIACAO"
