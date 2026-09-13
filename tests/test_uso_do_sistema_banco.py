"""O trabalho no sistema, com banco de verdade.

Tudo aqui depende de `WHERE`, de `GROUP BY` e — o mais traiçoeiro — de FUSO
HORÁRIO, e a sessão dublada não alcança nenhum dos três.

O que se prova:

  1. O dia é o dia de QUEM TRABALHA, não o do servidor. O ERP roda num servidor
     em UTC e a BWS trabalha no Ceará (UTC−3). Sem dizer o fuso, um lançamento
     das 22h cairia no dia seguinte e "começou às 8h" viraria "começou às 11h".
  2. Trabalho que a fila de segundo plano fez sozinha NÃO entra na conta de
     ninguém — sai numa linha à parte.
  3. A semana de uma pessoa traz só o que ELA fez.
  4. O passo a passo do dia sai em ordem e respeita o teto.
  5. E a fronteira que importa: ver a PRÓPRIA semana é de todo operador; ver a
     dos OUTROS exige a ação `ver_uso_da_equipe`.
"""
from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum import uso as svc
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, Usuario

from conftest import como

pytestmark = pytest.mark.banco

DIA = date(2026, 9, 9)


@pytest.fixture
def gente(sessao_real):
    """Um administrador (vê a equipe) e um administrativo de obra (não vê)."""
    chefe = Usuario(nome="Marcelo", email="chefe@bws.test",
                    senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMIN)
    operador = Usuario(nome="Rafael", email="rafa@bws.test",
                       senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMINISTRATIVO_OBRA)
    sessao_real.add_all([chefe, operador])
    sessao_real.flush()
    return chefe, operador


def _evento(s, *, usuario_id, entidade, acao, quando_utc: str) -> None:
    """Grava direto na trilha. `quando_utc` em UTC, como o banco guarda."""
    s.execute(text("""
        INSERT INTO eventos (entidade_tipo, entidade_id, usuario_id, acao, criado_em)
             VALUES (:e, 1, :u, :a, CAST(:q AS timestamptz))"""),
        {"e": entidade, "u": usuario_id, "a": acao, "q": quando_utc})
    s.flush()


# ---------------------------------------------------------------------------
# 1. O dia é o de quem trabalha
# ---------------------------------------------------------------------------
def test_o_dia_e_o_do_ceara_e_nao_o_do_servidor(sessao_real, gente):
    """23h no Ceará é 2h do dia seguinte em UTC. Se o relatório agrupasse pelo
    horário do servidor, o trabalho da noite apareceria no dia errado — e a
    pessoa que virou a noite fechando o mês pareceria ter trabalhado dois
    dias."""
    _, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-10T02:00:00+00")

    semana = svc.semana_da_pessoa(sessao_real, operador.id, de=DIA, ate=DIA)

    assert [d["dia"] for d in semana["dias"]] == ["2026-09-09"]
    assert semana["dias"][0]["primeira"] == "23:00", \
        "a hora tem de ser a que a pessoa viu no relógio dela"


def test_primeira_e_ultima_acao_do_dia(sessao_real, gente):
    _, operador = gente
    for hora in ("11:10", "14:35", "20:05"):     # UTC → 08:10, 11:35, 17:05
        _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
                acao="CRIADO", quando_utc=f"2026-09-09T{hora}:00+00")

    dia = svc.semana_da_pessoa(sessao_real, operador.id, de=DIA, ate=DIA)["dias"][0]

    assert dia["primeira"] == "08:10"
    assert dia["ultima"] == "17:05"
    assert dia["acoes"] == 3


# ---------------------------------------------------------------------------
# 2. O que o sistema faz sozinho não é produção de ninguém
# ---------------------------------------------------------------------------
def test_trabalho_automatico_sai_a_parte_e_nao_entra_na_conta_de_ninguem(
        sessao_real, gente):
    """A fila de segundo plano importa cards e emite nota sozinha. Se isso
    entrasse na linha de alguém, a pessoa apareceria produzindo à noite."""
    _, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")
    _evento(sessao_real, usuario_id=None, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:05:00+00")

    equipe = svc.semana_da_equipe(sessao_real, de=DIA, ate=DIA)

    linhas = {p["nome"]: p for p in equipe["pessoas"]}
    assert linhas["Rafael"]["acoes"] == 1, "o trabalho do robô virou produção da pessoa"
    assert equipe["do_sistema"] is not None
    assert equipe["do_sistema"]["acoes"] == 1
    assert None not in [p["usuario_id"] for p in equipe["pessoas"]]
    assert equipe["acoes"] == 1, "o total da equipe não conta o robô"


# ---------------------------------------------------------------------------
# 3. Cada semana é de uma pessoa só
# ---------------------------------------------------------------------------
def test_a_semana_de_uma_pessoa_nao_traz_o_trabalho_da_outra(sessao_real, gente):
    chefe, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")
    _evento(sessao_real, usuario_id=chefe.id, entidade="titulo",
            acao="APROVADO", quando_utc="2026-09-09T14:00:00+00")

    do_operador = svc.semana_da_pessoa(sessao_real, operador.id, de=DIA, ate=DIA)
    do_chefe = svc.semana_da_pessoa(sessao_real, chefe.id, de=DIA, ate=DIA)

    assert do_operador["acoes"] == 1
    assert do_chefe["acoes"] == 1
    assert [c["chave"] for c in do_operador["dias"][0]["categorias"]] == ["lancamento"]
    assert [c["chave"] for c in do_chefe["dias"][0]["categorias"]] == ["analise"]


def test_dia_sem_trabalho_simplesmente_nao_aparece(sessao_real, gente):
    _, operador = gente
    semana = svc.semana_da_pessoa(sessao_real, operador.id,
                                  de=date(2026, 9, 1), ate=date(2026, 9, 7))
    assert semana["dias"] == []
    assert semana["dias_com_trabalho"] == 0
    assert semana["acoes"] == 0


# ---------------------------------------------------------------------------
# 4. O passo a passo do dia
# ---------------------------------------------------------------------------
def test_o_passo_a_passo_sai_em_ordem_e_com_o_tipo_de_trabalho(sessao_real, gente):
    _, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="APROVADO", quando_utc="2026-09-09T14:00:00+00")
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")

    passos = svc.detalhe_do_dia(sessao_real, operador.id, DIA)

    assert [p["hora"] for p in passos] == ["10:00", "11:00"]
    assert [p["categoria"] for p in passos] == ["lancamento", "analise"]
    assert passos[0]["rotulo"] == "Lançou despesa"


def test_repeticao_seguida_vira_uma_linha_so(sessao_real, gente):
    """A carga da planilha de insumos gravou milhares de eventos iguais em
    segundos, e a primeira versão desta tela devolveu 76 linhas de "criou
    insumo" — o dia inteiro ficava ilegível por causa de um minuto."""
    _, operador = gente
    for minuto in range(5):
        _evento(sessao_real, usuario_id=operador.id, entidade="insumo",
                acao="CRIADO", quando_utc=f"2026-09-09T13:0{minuto}:00+00")
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:30:00+00")

    passos = svc.detalhe_do_dia(sessao_real, operador.id, DIA)

    assert len(passos) == 2, "as cinco iguais tinham de virar uma linha só"
    assert passos[0]["quantas"] == 5
    assert passos[0]["hora"] == "10:00" and passos[0]["ate"] == "10:04"
    assert passos[0]["o_que"] == "insumo criado", "o nome tem de ser em português"
    assert passos[1]["quantas"] == 1


def test_a_mesma_acao_separada_por_outra_nao_se_junta(sessao_real, gente):
    """Juntar tudo que é igual no dia esconderia a ordem do trabalho — só se
    junta o que aconteceu EM SEGUIDA."""
    _, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="insumo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:10:00+00")
    _evento(sessao_real, usuario_id=operador.id, entidade="insumo",
            acao="CRIADO", quando_utc="2026-09-09T13:20:00+00")

    passos = svc.detalhe_do_dia(sessao_real, operador.id, DIA)

    assert [p["entidade"] for p in passos] == ["insumo", "titulo", "insumo"]
    assert all(p["quantas"] == 1 for p in passos)


def test_o_passo_a_passo_tem_teto_de_leitura(sessao_real, gente):
    """O teto é sobre os EVENTOS lidos, e continua valendo depois do
    agrupamento — é o que impede um dia de carga de derrubar a instância."""
    _, operador = gente
    for minuto in range(5):
        _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
                acao="CRIADO", quando_utc=f"2026-09-09T13:0{minuto}:00+00")

    passos = svc.detalhe_do_dia(sessao_real, operador.id, DIA, limite=3)

    assert sum(p["quantas"] for p in passos) == 3


# ---------------------------------------------------------------------------
# 5. A fronteira: a minha semana é de todos; a dos outros, não
# ---------------------------------------------------------------------------
def test_qualquer_operador_ve_a_propria_semana(app_real, sessao_real, gente):
    _, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")

    r = como(app_real, operador.id).get(
        "/erp/api/uso/minha-semana?de=2026-09-09&ate=2026-09-09")

    assert r.status_code == 200
    assert r.get_json()["semana"]["acoes"] == 1
    assert r.get_json()["semana"]["nome"] == "Rafael"


def test_qualquer_operador_ve_o_passo_a_passo_do_proprio_dia(
        app_real, sessao_real, gente):
    """Quem não consegue abrir o próprio dia não tem como conferir o que a tela
    diz sobre ele — e aí o relatório vira acusação sem defesa."""
    _, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")

    r = como(app_real, operador.id).get("/erp/api/uso/minha-semana?dia=2026-09-09")

    assert r.status_code == 200
    assert [p["o_que"] for p in r.get_json()["detalhe"]] == ["título criado"]


def test_quem_nao_tem_a_acao_nao_ve_a_equipe(app_real, gente):
    _, operador = gente
    assert como(app_real, operador.id).get("/erp/api/uso/equipe").status_code == 403


def test_quem_nao_tem_a_acao_nao_ve_a_semana_de_outra_pessoa(app_real, gente):
    """A rota da própria semana nem aceita o número de outra pessoa; esta
    aceita, e por isso é ela que precisa da ação."""
    chefe, operador = gente
    r = como(app_real, operador.id).get(f"/erp/api/uso/pessoa/{chefe.id}")
    assert r.status_code == 403


def test_quem_tem_a_acao_ve_a_equipe_e_a_pessoa(app_real, sessao_real, gente):
    chefe, operador = gente
    _evento(sessao_real, usuario_id=operador.id, entidade="titulo",
            acao="CRIADO", quando_utc="2026-09-09T13:00:00+00")
    cliente = como(app_real, chefe.id)

    equipe = cliente.get("/erp/api/uso/equipe?de=2026-09-09&ate=2026-09-09")
    pessoa = cliente.get(
        f"/erp/api/uso/pessoa/{operador.id}?de=2026-09-09&ate=2026-09-09")

    assert equipe.status_code == 200
    assert any(p["nome"] == "Rafael" for p in equipe.get_json()["equipe"]["pessoas"])
    assert pessoa.status_code == 200
    assert pessoa.get_json()["semana"]["acoes"] == 1


def test_pessoa_que_nao_existe_responde_nao_encontrado(app_real, gente):
    """Fora do alcance responde 404, nunca 403 — dizer "sem permissão" para um
    número que existe já entrega que ele existe."""
    chefe, _ = gente
    assert como(app_real, chefe.id).get("/erp/api/uso/pessoa/999999").status_code == 404
