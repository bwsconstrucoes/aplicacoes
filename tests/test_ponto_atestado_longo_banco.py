# -*- coding: utf-8 -*-
"""Ponto — O ATESTADO QUE PASSA DE 15 DIAS ABRE O AFASTAMENTO (15/11/2026).

Decisão do dono, escolhendo entre abrir sozinho, sempre manual e sempre
automático: *"Acima de 15 dias, o sistema abre sozinho."*

A RAZÃO DOS 15: os primeiros 15 dias de atestado são da EMPRESA — ela paga e
abona as faltas. Do 16º em diante o benefício é do INSS, e aí deixa de ser
abono de falta e passa a ser AFASTAMENTO (perícia, documento, outro tratamento
na folha).

E a mesma conta vale com atestados PICADOS: a lei soma os dias da mesma doença
dentro de 60 dias. Três atestados de cinco dias e um de dois já estouram o
limite — e é justamente aí que escapa, porque ninguém do DP vai somar à mão os
atestados de 575 pessoas.

⚑ O AFASTAMENTO NASCE AGUARDANDO O DP, NÃO APROVADO: afastamento aprovado muda
o que a folha paga, e a soma legal vale para a MESMA doença — coisa que só quem
lê o atestado sabe. Aberto como pedido, o pior caso é o DP negar. Contra um
Postgres de verdade.
"""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto import db
from app.apps.ponto.core import afastamento_longo as al
from tests.test_ponto_gestao_banco import (CPF_MARIA, _schema_ponto2, app,  # noqa: F401
                                           como, mundo)

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
# A conta, sem banco
# ---------------------------------------------------------------------------
def test_o_limite_e_quinze_dias_e_a_janela_e_sessenta():
    """Números da lei, não escolha de desenho — por isso ficam nomeados."""
    assert al.DIAS_DA_EMPRESA == 15
    assert al.JANELA_DIAS == 60


def test_contar_dias_inclui_o_primeiro_e_o_ultimo():
    """Atestado de 01 a 15 é QUINZE dias, não quatorze. Errar por um aqui é
    empurrar alguém para o INSS um dia antes (ou deixar passar um dia depois)."""
    assert al._dias(dt.date(2026, 3, 1), dt.date(2026, 3, 15)) == 15
    assert al._dias(dt.date(2026, 3, 1), dt.date(2026, 3, 1)) == 1


# ---------------------------------------------------------------------------
# Com banco: o caminho inteiro, pela tela do DP
# ---------------------------------------------------------------------------
def _atestado(dp, pessoa, de, ate, cid="J11"):
    r = dp.post("/erp/api/ponto/ocorrencias", json={
        "tipo": "ATESTADO", "colaborador_id": pessoa,
        "data_inicio": de.isoformat(), "data_fim": ate.isoformat(),
        "descricao": "atestado de teste", "cid": cid})
    assert r.status_code in (200, 201), r.get_json()
    corpo = r.get_json()
    return (corpo.get("ocorrencia") or corpo)["id"]


def _aprovar(dp, oid):
    r = dp.post(f"/erp/api/ponto/afastamentos/{oid}/etapa-dp", json={"aprovar": True})
    assert r.status_code in (200, 201), r.get_json()
    return r.get_json()


def _afastamentos(pessoa):
    with db.conexao() as conn:
        return db.todos(conn, """
            SELECT id, data_inicio, data_fim, status, descricao, solicitado_por
              FROM ponto.ocorrencias
             WHERE colaborador_id = :c AND tipo = 'AFASTAMENTO'
             ORDER BY id""", c=int(pessoa))


def _passado(dias_atras: int) -> dt.date:
    """Uma data no passado, para a competência da folha estar aberta."""
    return dt.date.today() - dt.timedelta(days=dias_atras)


def test_atestado_curto_nao_abre_nada(app, mundo):
    """O caso comum. Três dias de gripe não têm nada a ver com o INSS, e abrir
    pedido aqui encheria a fila do DP de repetição — o jeito mais rápido de
    uma fila deixar de ser olhada."""
    dp = como(app, mundo["dp"])
    _aprovar(dp, _atestado(dp, mundo["maria"], _passado(10), _passado(8)))
    assert _afastamentos(mundo["maria"]) == []


def test_atestado_de_vinte_dias_abre_o_afastamento_do_decimo_sexto_em_diante(app, mundo):
    dp = como(app, mundo["dp"])
    de, ate = _passado(30), _passado(11)          # 20 dias
    _aprovar(dp, _atestado(dp, mundo["maria"], de, ate))

    af = _afastamentos(mundo["maria"])
    assert len(af) == 1
    # Cinco dias excedentes: do 16º ao 20º.
    assert af[0]["data_fim"] == ate
    assert af[0]["data_inicio"] == ate - dt.timedelta(days=4)
    # ⚑ AGUARDANDO O DP, e não aprovado: quem decide continua sendo gente.
    assert af[0]["status"] == "AGUARDANDO_DP"
    assert al.MARCA in af[0]["descricao"]
    assert "INSS" in af[0]["descricao"]
    assert "sistema" in af[0]["solicitado_por"]


def test_atestados_picados_somam_e_estouram_o_limite_juntos(app, mundo):
    """⚑ O CASO QUE ESCAPA. Quatro atestados de cinco dias não chamam atenção
    nenhum; juntos passam dos 15 e o quinto dia do quarto já é do INSS."""
    dp = como(app, mundo["dp"])
    base = _passado(40)
    for n in range(4):                             # 4 × 5 dias = 20
        de = base + dt.timedelta(days=n * 7)
        _aprovar(dp, _atestado(dp, mundo["maria"], de, de + dt.timedelta(days=4)))
    af = _afastamentos(mundo["maria"])
    assert len(af) == 1, [dict(x) for x in af]
    assert "4 atestado(s)" in af[0]["descricao"]
    assert "20 dias" in af[0]["descricao"]


def test_atestado_novo_na_mesma_doenca_nao_abre_um_SEGUNDO_afastamento(app, mundo):
    """Sem esta trava, cada atestado novo da mesma pessoa abriria outro pedido,
    e a fila do DP viraria uma lista de repetições."""
    dp = como(app, mundo["dp"])
    de, ate = _passado(30), _passado(11)
    _aprovar(dp, _atestado(dp, mundo["maria"], de, ate))
    af = _afastamentos(mundo["maria"])
    assert len(af) == 1
    # Outro atestado, em dias NOVOS, que cai dentro do afastamento já aberto
    # (o ponto recusa atestado sobreposto a outro ATESTADO, mas o afastamento
    # aberto pelo sistema vai até o último dia do primeiro — e o atestado
    # seguinte continua estourando a soma).
    _aprovar(dp, _atestado(dp, mundo["maria"], _passado(10), _passado(6)))
    assert len(_afastamentos(mundo["maria"])) == 2, (
        "atestado em dias novos abre o afastamento DELE — o que não pode é "
        "abrir dois para o mesmo período")
    # E o terceiro, dentro do período que o segundo afastamento já cobre, não
    # abre um terceiro pedido.
    with db.conexao() as conn:
        antes = len(_afastamentos(mundo["maria"]))
        criado = al.conferir(conn, {
            "tipo": "ATESTADO", "colaborador_id": int(mundo["maria"]),
            "data_inicio": _passado(10), "data_fim": _passado(6), "id": 0,
            "obra_id": None})
    assert criado is None
    assert len(_afastamentos(mundo["maria"])) == antes


def test_a_declaracao_de_comparecimento_nao_entra_na_soma(app, mundo):
    """Ela é a consulta de algumas horas, não dia de doença. Somá-la empurraria
    gente para o INSS por ter ido ao dentista muitas vezes."""
    dp = como(app, mundo["dp"])
    de, ate = _passado(25), _passado(13)          # 13 dias: ainda não estoura
    _aprovar(dp, _atestado(dp, mundo["maria"], de, ate))
    assert _afastamentos(mundo["maria"]) == []
    with db.conexao() as conn:
        # Três declarações, que somariam três dias se contassem.
        for n in range(3):
            dia = _passado(9 - n)
            db.executar(conn, """
                INSERT INTO ponto.ocorrencias
                       (colaborador_id, tipo, subtipo, data_inicio, data_fim,
                        descricao, status, origem, solicitado_por)
                VALUES (:c, 'ATESTADO', 'DECLARACAO', :d, :d, 'consulta',
                        'APROVADA', 'GESTAO', 'teste')""",
                c=int(mundo["maria"]), d=dia)
        conta = al.somar_atestados(conn, int(mundo["maria"]), dt.date.today())
    assert conta["dias"] == 13 and conta["passou"] is False


def test_dias_sobrepostos_contam_UMA_vez(app, mundo):
    """Dois atestados que se cruzam somariam o mesmo dia duas vezes e
    estourariam o limite sem motivo — por isso a conta é de dias de calendário
    COBERTOS, e não a soma do tamanho de cada atestado.

    ⚑ Hoje a tela do ponto RECUSA atestado sobreposto a outro ("já existe um
    pedido para esses dias"), então este caso não chega pela tela — chega pela
    IMPORTAÇÃO e pela carga de histórico, que gravam direto. É por isso que o
    teste insere direto: é o caminho por onde o dado realmente aparece.
    """
    de = _passado(30)
    with db.conexao() as conn:
        for _ in range(2):
            db.executar(conn, """
                INSERT INTO ponto.ocorrencias
                       (colaborador_id, tipo, data_inicio, data_fim, descricao,
                        status, origem, solicitado_por)
                VALUES (:c, 'ATESTADO', :i, :f, 'carga de teste', 'APROVADA',
                        'GESTAO', 'teste')""",
                c=int(mundo["maria"]), i=de, f=de + dt.timedelta(days=9))
        conta = al.somar_atestados(conn, int(mundo["maria"]),
                                   de + dt.timedelta(days=9))
    # Vinte dias se contasse duas vezes; dez, contando dias cobertos.
    assert conta["atestados"] == 2 and conta["dias"] == 10
    assert conta["passou"] is False
