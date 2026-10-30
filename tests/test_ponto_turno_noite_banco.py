# -*- coding: utf-8 -*-
"""Ponto — o turno da noite (30/10/2026, core/escalas.py `dia_de_trabalho`, migração 021): a escala que
passa da meia-noite leva a batida da madrugada para o dia em que o turno começou, sem precisar marcar
"vigia noturno" na jornada da pessoa."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from tests.test_ponto_gestao_banco import _schema_ponto2, _segunda_passada, app, como, mundo  # noqa: F401


def test_quando_a_escala_e_do_turno_da_noite():
    from app.apps.ponto.core import apuracao, escalas
    seg = dt.date(2026, 10, 26)
    noite_12x36 = apuracao.Escala(tipo="CICLO_12X36", periodo_ciclo=(17 * 60, 30 * 60), data_base=seg)
    dia_12x36 = apuracao.Escala(tipo="CICLO_12X36", periodo_ciclo=(7 * 60, 19 * 60), data_base=seg)
    assert escalas.turno_da_noite(noite_12x36, seg)
    assert escalas.turno_da_noite(noite_12x36, seg + dt.timedelta(days=1))     # folga no ciclo: a noite fica inteira
    assert not escalas.turno_da_noite(dia_12x36, seg)
    # semanal com uma noite só (sexta): só a manhã de sábado volta
    mista = apuracao.Escala(tipo="SEMANAL", semana={0: [(420, 1020)], 4: [(1320, 1800)]})
    assert escalas.turno_da_noite(mista, seg + dt.timedelta(days=4))
    assert not escalas.turno_da_noite(mista, seg)
    # semanal só de noite: até a noite fora da escala fica no dia da entrada
    so_noite = apuracao.Escala(tipo="SEMANAL", semana={0: [(1080, 1800)], 2: [(1080, 1800)]})
    assert escalas.turno_da_noite(so_noite, seg + dt.timedelta(days=1))
    assert not escalas.turno_da_noite(None, seg)


def test_o_ciclo_12x36_sai_das_batidas():
    """30/10/2026, o dono: "essa questão do dia em que o ciclo começa (…) poderia ser considerado de forma
    automática." """
    from app.apps.ponto.core.apuracao import dia_do_ciclo
    d0 = dt.date(2026, 10, 1)
    dias = lambda *ns: tuple(d0 + dt.timedelta(days=n) for n in ns)
    noites = dias(0, 2, 4, 6, 8)
    assert dia_do_ciclo(noites, d0 + dt.timedelta(days=10)) is True        # a próxima do ciclo
    assert dia_do_ciclo(noites, d0 + dt.timedelta(days=9)) is False        # folga
    # a noite a mais, fora do ciclo, não vira o ciclo: continua folga (é extra)
    assert dia_do_ciclo(noites + dias(9), d0 + dt.timedelta(days=9)) is False
    assert dia_do_ciclo(noites + dias(9), d0 + dt.timedelta(days=10)) is True
    # a troca de turma: passado o tempo, a maioria muda e o ciclo acompanha
    trocado = noites + dias(*range(11, 40, 2))
    assert dia_do_ciclo(trocado, d0 + dt.timedelta(days=41)) is True
    # empate nas noites de perto (31/10/2026): o dia não trabalhado vai pela noite mais perto (a do dia 3)
    assert dia_do_ciclo(dias(0, 3), d0 + dt.timedelta(days=4)) is False
    assert dia_do_ciclo(dias(0, 3), d0 + dt.timedelta(days=5)) is True
    # … e o dia trabalhado, no empate, é dele (segunda e quarta, com a terça a mais: a quarta é do ciclo)
    assert dia_do_ciclo(dias(0, 1, 2), d0 + dt.timedelta(days=2)) is True
    assert dia_do_ciclo(dias(0, 1, 2), d0 + dt.timedelta(days=1)) is False
    # sem batida nos 30 dias: a paridade da última noite trabalhada
    assert dia_do_ciclo(dias(0), d0 + dt.timedelta(days=60)) is True
    assert dia_do_ciclo(dias(0), d0 + dt.timedelta(days=61)) is False
    # nunca trabalhou: o dia informado, se houver; senão nada previsto até a primeira batida
    assert dia_do_ciclo((), d0, data_base=d0 - dt.timedelta(days=2)) is True
    assert dia_do_ciclo((), d0) is None
    assert dia_do_ciclo(dias(0), d0) is True
    # o começo do ponto (30/10/2026, o vigia dos dias ímpares desde 17/10, com "16" informado ao dar a
    # escala): sem noite antes, votam as noites seguintes — e o dia informado não passa por cima delas
    impares = tuple(dt.date(2026, 10, n) for n in (17, 19, 21, 23, 25, 27, 29))
    base = dt.date(2026, 10, 16)
    assert dia_do_ciclo(impares, dt.date(2026, 10, 16), base) is False
    assert dia_do_ciclo(impares, dt.date(2026, 10, 17), base) is True
    assert dia_do_ciclo(impares, dt.date(2026, 10, 18), base) is False
    assert dia_do_ciclo(impares, dt.date(2026, 10, 31), base) is True
    # a troca de plantão (31/10/2026, os dois vigias da CREPEFLORESTA2): o Adriano, das noites ímpares, trabalhou o
    # 18 no lugar do colega e o 19 dele — o empate antes do 20 e do 21 desempata pelas noites seguintes
    adriano = tuple(dt.date(2026, 10, n) for n in (18, 19, 21, 23, 25))
    assert dia_do_ciclo(adriano, dt.date(2026, 10, 20)) is False          # folga, não falta
    assert dia_do_ciclo(adriano, dt.date(2026, 10, 21)) is True           # noite dele, não extra
    adelson = tuple(dt.date(2026, 10, n) for n in (16, 17, 20, 22, 24))
    assert dia_do_ciclo(adelson, dt.date(2026, 10, 18)) is True           # a noite que ele deixou: falta (ou a troca)
    assert dia_do_ciclo(adelson, dt.date(2026, 10, 19)) is False


@pytest.mark.banco
def test_a_saida_da_madrugada_fica_no_dia_da_entrada(app, mundo):
    """O dono: "o dia que ele inicia de tarde (…) é o horário de entrada dele. E no dia seguinte, é de saída." """
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    ter, qua, qui = (seg + dt.timedelta(days=n) for n in (1, 2, 3))

    def dias():
        with db.conexao() as conn:
            return [r["data_referencia"] for r in db.todos(
                conn, "SELECT data_referencia FROM ponto.marcacoes WHERE colaborador_id = :c "
                      "ORDER BY timestamp_servidor", c=mundo["joao"])]

    # na escala do dia, a saída das 6h cai no dia seguinte (era o defeito)
    bater_via_chave(app, CPF_JOAO, local(seg, 17))
    bater_via_chave(app, CPF_JOAO, local(ter, 6))
    assert dias() == [seg, ter]
    # a escala da noite, dada a partir da segunda: a batida já gravada volta para a segunda
    with db.conexao() as conn:
        noite = escalas.gravar(conn, {"nome": "Vigia noturno 12x36", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "06:00"})
        escalas.atribuir(conn, mundo["joao"], noite["id"], seg.isoformat(), por="teste")   # sem dia do ciclo
    assert dias() == [seg, seg]
    # a batida nova já nasce no dia certo
    bater_via_chave(app, CPF_JOAO, local(qua, 17))
    bater_via_chave(app, CPF_JOAO, local(qui, 5, 55))
    assert dias() == [seg, seg, qua, qua]
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    d = next(x for x in esp["dias"] if x["data"] == seg.isoformat())
    assert d["trabalhado"] == 13 * 60 and d["situacao"] == "OK", d
    assert "INTERVALO_CURTO" not in d["alertas"]
    # o ciclo veio das batidas: a terça é folga, a sexta (noite sim, noite não) é falta
    situacao = {x["data"]: x["situacao"] for x in esp["dias"]}
    sex = seg + dt.timedelta(days=4)
    assert situacao[ter.isoformat()] == "DESCANSO" and situacao[qua.isoformat()] == "OK"
    if sex.month == seg.month:
        assert situacao[sex.isoformat()] == "FALTA", situacao

    # a migração 021 acerta o que ficou gravado no dia errado antes do código novo
    with db.conexao() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET data_referencia = data_referencia + 1 "
                          "WHERE colaborador_id = :c AND EXTRACT(HOUR FROM timestamp_servidor "
                          "AT TIME ZONE 'America/Fortaleza') < 10"), {"c": mundo["joao"]})
    assert dias() == [seg, ter, qua, qui]
    sql = (Path(__file__).resolve().parents[1] / "app/apps/ponto/migracoes/021_turno_da_noite.sql").read_text()
    with db.conexao() as conn:
        conn.execute(text(sql))
    assert dias() == [seg, seg, qua, qua]
    # trocar de volta para a escala do dia desfaz
    with db.conexao() as conn:
        escalas.atribuir(conn, mundo["joao"], mundo["escala"], seg.isoformat(), por="teste")
    assert dias() == [seg, ter, qua, qui]

    # a noite trabalhada fora do ciclo: o sistema critica e o DP decide (troca ou extra)
    with db.conexao() as conn:
        escalas.atribuir(conn, mundo["joao"], noite["id"], seg.isoformat(), por="teste")
    bater_via_chave(app, CPF_JOAO, local(ter, 17))
    bater_via_chave(app, CPF_JOAO, local(qua, 6))
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    d = {x["data"]: x for x in esp["dias"]}
    assert "FORA_DO_CICLO" in d[ter.isoformat()]["alertas"] and d[ter.isoformat()]["extra"] == 13 * 60
    assert "FORA_DO_CICLO" not in d[qua.isoformat()]["alertas"] and d[qua.isoformat()]["situacao"] == "OK"


def test_a_batida_no_meio_do_turno_da_noite_nao_e_saida():
    """30/10/2026, o dono, num vigia com 17:05, 22:56, 04:48 e 07:07: "imagino que essas batidas tenham sido
    erro (…) não tem como o vigia sair e voltar." """
    from app.apps.ponto import horario
    from app.apps.ponto.core import apuracao
    sex = dt.date(2026, 10, 23)
    noite = apuracao.Escala(tipo="CICLO_12X36", periodo_ciclo=(17 * 60, 29 * 60), data_base=sex)

    def em(d, h, m):
        return dt.datetime(d.year, d.month, d.day, h, m, tzinfo=horario.FUSO)
    sab = sex + dt.timedelta(days=1)
    batidas = [em(sex, 17, 5), em(sex, 22, 56), em(sab, 4, 48), em(sab, 7, 7)]
    r = apuracao.apurar_dia(sex, noite, batidas)
    assert r.trabalhado == 14 * 60 + 2 and r.debito == 0 and r.situacao == "OK", r
    assert "BATIDA_NO_MEIO_DO_TURNO" in r.alertas
    # a noite de folga do ciclo, trabalhada: o mesmo
    r = apuracao.apurar_dia(sab, noite, [em(sab, 17, 0), em(sab, 23, 0), em(sab + dt.timedelta(days=1), 5, 0)])
    assert r.trabalhado == 12 * 60 and "BATIDA_NO_MEIO_DO_TURNO" in r.alertas
    # o turno do dia, com almoço, continua contando a saída e a volta
    dia = apuracao.Escala(tipo="SEMANAL", semana={4: [(420, 660), (720, 1020)]})
    r = apuracao.apurar_dia(sex, dia, [em(sex, 7, 0), em(sex, 11, 0), em(sex, 12, 0), em(sex, 17, 0)])
    assert r.trabalhado == 9 * 60 and "BATIDA_NO_MEIO_DO_TURNO" not in r.alertas


def test_o_dia_completo_nao_pede_batida_mesmo_longe_da_escala():
    """O vigia de 21/10: entrou 16:45 e saiu 07:01 numa escala até 05:00 — a saída estava batida, e o espelho
    oferecia "Saída 05:00" para incluir."""
    from app.apps.ponto.core import ajustes
    prev = ajustes.marcas_previstas([(17 * 60, 29 * 60)])
    assert ajustes.faltantes(prev, [16 * 60 + 45, 31 * 60 + 1]) == []
    assert [f["rotulo"] for f in ajustes.faltantes(prev, [16 * 60 + 45])] == ["Saída"]
    # a repetida (menos de 10 min) não completa o dia
    assert [f["rotulo"] for f in ajustes.faltantes(prev, [17 * 60, 17 * 60 + 3])] == ["Saída"]


@pytest.mark.banco
def test_o_primeiro_dia_da_escala_do_vigia(app, mundo):
    """31/10/2026, o dono, num vigia do 12x36 com a escala a partir de 16/10: o 07:01 do dia 16 (a saída do turno do
    dia 15) ficou no 16 — "descanso · batida faltando" — e, votando no ciclo, fez o 18 (folga) virar falta."""
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    d0 = _segunda_passada()
    d = [d0 + dt.timedelta(days=n) for n in range(5)]
    with db.conexao() as conn:
        # como o vigia de verdade: sem escala própria antes (vale a padrão, de dia); a da noite começa em d0
        conn.execute(text("DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = :c"), {"c": mundo["joao"]})
        noite = escalas.gravar(conn, {"nome": "Vigia 12x36 17h-5h", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "05:00"})
        escalas.atribuir(conn, mundo["joao"], noite["id"], d[0].isoformat(), por="teste")
    bater_via_chave(app, CPF_JOAO, local(d[0], 7, 1))                 # a saída do turno da véspera
    for entra, sai in ((d[1], d[2]), (d[3], d[4])):
        bater_via_chave(app, CPF_JOAO, local(entra, 16, 31))
        bater_via_chave(app, CPF_JOAO, local(sai, 7, 0))

    def dias():
        with db.conexao() as conn:
            return [r["data_referencia"] for r in db.todos(
                conn, "SELECT data_referencia FROM ponto.marcacoes WHERE colaborador_id = :c "
                      "ORDER BY timestamp_servidor", c=mundo["joao"])]
    assert dias() == [d0 - dt.timedelta(days=1), d[1], d[1], d[3], d[3]]

    def situacoes():
        esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={d0:%Y-%m}").get_json()
        return {x["data"]: x for x in esp["dias"]}
    s = situacoes()
    if all(x.month == d0.month for x in d):
        assert s[d[0].isoformat()]["situacao"] == "DESCANSO" and s[d[0].isoformat()]["alertas"] == []
        assert s[d[2].isoformat()]["situacao"] == "DESCANSO"                       # folga, não falta
        assert s[d[1].isoformat()]["situacao"] == "OK" and s[d[3].isoformat()]["situacao"] == "OK"

    # o que já estava gravado no dia errado: a 022 acerta; e a batida solta da manhã não vota no ciclo
    with db.conexao() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET data_referencia = data_referencia + 1 WHERE colaborador_id = :c "
                          "AND data_referencia = :v"), {"c": mundo["joao"], "v": d0 - dt.timedelta(days=1)})
        assert escalas.dias_trabalhados(conn, [mundo["joao"]])[mundo["joao"]] == (d[1], d[3])
    sql = (Path(__file__).resolve().parents[1] / "app/apps/ponto/migracoes/022_turno_da_noite_primeiro_dia.sql").read_text()
    with db.conexao() as conn:
        conn.execute(text(sql))
    assert dias()[0] == d0 - dt.timedelta(days=1)


@pytest.mark.banco
def test_a_troca_de_plantao_lancada_como_compensacao_vira_o_voto_do_ciclo(app, mundo):
    """31/10/2026: lançada a troca (compensação: a folga = a noite que ele deixou; o dia trabalhado = a do colega), a
    noite trabalhada no lugar do outro não vota no ciclo, e a deixada vota."""
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    d0 = _segunda_passada()
    d1, d2 = d0 + dt.timedelta(days=1), d0 + dt.timedelta(days=2)
    bater_via_chave(app, CPF_JOAO, local(d1, 17, 0))
    bater_via_chave(app, CPF_JOAO, local(d1, 23, 0))
    with db.conexao() as conn:
        assert escalas.dias_trabalhados(conn, [mundo["joao"]])[mundo["joao"]] == (d1,)
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "COMPENSACAO", "colaborador_id": mundo["joao"], "data_inicio": d2.isoformat(),
        "dia_trabalhado": d1.isoformat(), "descricao": "troca de plantão com o colega", "aprovar_ja": True})
    assert r.status_code == 201, r.get_json()
    with db.conexao() as conn:
        assert escalas.dias_trabalhados(conn, [mundo["joao"]])[mundo["joao"]] == (d2,)


def test_trocar_a_escala_na_janela_vale_dali_em_diante(app, mundo):
    """31/10/2026, o dono: "tem alguma possibilidade de eu trocar uma escala e, quando ver o espelho, não atualizar?"
    Tinha: a troca marcada DEPOIS da data escolhida continuava valendo. Agora a troca da janela substitui as
    seguintes (como a em lote), e o dia das batidas da noite é refeito com a escala certa."""
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    dp = como(app, mundo["dp"])
    seg = _segunda_passada()
    ter = seg + dt.timedelta(days=1)
    with db.conexao() as conn:
        db.executar(conn, "DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = :c", c=mundo["joao"])
        escalas.atribuir(conn, mundo["joao"], mundo["escala"], ter.isoformat(), por="teste")      # a do dia, desde terça
        noite = escalas.gravar(conn, {"nome": "Vigia noite troca janela", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "05:00"})
    bater_via_chave(app, CPF_JOAO, local(ter, 17))
    bater_via_chave(app, CPF_JOAO, local(ter + dt.timedelta(days=1), 5))
    # a noite, a partir de segunda, pela janela: a de terça (a do dia) sai e a saída das 5h volta para terça
    r = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/escala",
                json={"escala_id": noite["id"], "vigencia_inicio": seg.isoformat()})
    assert r.status_code == 200, r.get_json()
    with db.conexao() as conn:
        vig = [v["vigencia_inicio"] for v in escalas.historico_da_pessoa(conn, mundo["joao"])]
        dias = [m["data_referencia"] for m in db.todos(conn, "SELECT data_referencia FROM ponto.marcacoes "
                                                             "WHERE colaborador_id = :c ORDER BY timestamp_servidor",
                                                       c=mundo["joao"])]
    assert vig == [seg] and dias == [ter, ter], (vig, dias)


def test_a_saida_esticada_ate_o_meio_dia_fecha_o_turno_da_vespera(app, mundo):
    """31/10/2026, o dono, no vigia que entrou às 15:55 e saiu às 11:52 do dia seguinte: "nesse dia o rapaz precisou
    ficar mais tempo (…) podemos deixar até o meio-dia". A saída até o meio-dia fecha o turno da véspera ABERTO; o 11:59
    de domingo, sem turno aberto na véspera, é a entrada do dia. A migração 026 acerta o que já estava gravado."""
    from sqlalchemy import text
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    from tests.test_ponto_gestao_banco import CPF_JOAO, bater_via_chave, local
    seg = _segunda_passada()
    d = [seg + dt.timedelta(days=n) for n in range(5)]

    def dias():
        with db.conexao() as conn:
            return [r["data_referencia"] for r in db.todos(
                conn, "SELECT data_referencia FROM ponto.marcacoes WHERE colaborador_id = :c "
                      "ORDER BY timestamp_servidor", c=mundo["joao"])]
    with db.conexao() as conn:
        db.executar(conn, "DELETE FROM ponto.colaborador_escalas WHERE colaborador_id = :c", c=mundo["joao"])
        noite = escalas.gravar(conn, {"nome": "Vigia 12x36 esticado", "tipo": "CICLO_12X36",
                                      "ciclo_entrada": "17:00", "ciclo_saida": "05:00"})
        escalas.atribuir(conn, mundo["joao"], noite["id"], d[0].isoformat(), por="teste")
    bater_via_chave(app, CPF_JOAO, local(d[0], 15, 55))
    bater_via_chave(app, CPF_JOAO, local(d[1], 11, 52))        # a saída esticada: do turno de d0
    bater_via_chave(app, CPF_JOAO, local(d[2], 11, 59))        # a entrada do turno longo: d1 não ficou aberto
    bater_via_chave(app, CPF_JOAO, local(d[3], 5, 52))
    assert dias() == [d[0], d[0], d[2], d[2]]
    # o que já estava gravado no dia do relógio: a 026 leva a saída esticada para a véspera, e só ela
    with db.conexao() as conn:
        conn.execute(text("UPDATE ponto.marcacoes SET data_referencia = (timestamp_servidor AT TIME ZONE "
                          "'America/Fortaleza')::date WHERE colaborador_id = :c AND EXTRACT(HOUR FROM timestamp_servidor "
                          "AT TIME ZONE 'America/Fortaleza') BETWEEN 10 AND 11"), {"c": mundo["joao"]})
    assert dias() == [d[0], d[1], d[2], d[2]]
    sql = (Path(__file__).resolve().parents[1] / "app/apps/ponto/migracoes/026_turno_esticado.sql").read_text()
    with db.conexao() as conn:
        conn.execute(text(sql))
    assert dias() == [d[0], d[0], d[2], d[2]]
