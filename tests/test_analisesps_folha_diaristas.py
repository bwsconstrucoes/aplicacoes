# -*- coding: utf-8 -*-
"""
O pagamento dos diaristas — 01/10/2026.

O dono: *"a prioridade agora é a de diaristas, que é o próximo que eu vou
gerar"*. A regra é a da aba "Diaristas" da planilha (`docs/FOLHA_DE_PAGAMENTO.md`
§7.10.4 e §7.14.12): só dia de DIÁRIA, vigia fora, meia diária em presença
parcial acima de 7h, +20 feriado, +10 sábado, +20 domingo. E o que a folha da
contabilidade já fazia: quem saiu não paga, quem está sem valor no cadastro
trava, e o fechamento é o que libera o arquivo.
"""
import datetime as dt
import json
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folha_diaristas as fd


# ---------------------------------------------------------------------------
# A CONTA DE UM DIA — sem banco
# ---------------------------------------------------------------------------
def _lido(data, presenca="PRESENÇA", horas="08:00", falta=""):
    return {"data": data, "presenca": presenca, "total_de_horas": horas,
            "falta": falta}


def test_presenca_cheia_vale_UMA_diaria():
    assert fd.quantidade_do_dia(_lido(dt.date(2026, 9, 8)), True)[0] == D("1")


def test_presenca_PARCIAL_acima_de_7h_vale_MEIA_e_ate_7h_nao_conta():
    acima = fd.quantidade_do_dia(_lido(dt.date(2026, 9, 8), "PRESENÇA PARCIAL",
                                       "07:30"), True)
    ate = fd.quantidade_do_dia(_lido(dt.date(2026, 9, 8), "PRESENÇA PARCIAL",
                                     "07:00"), True)
    assert acima[0] == D("0.5") and "meia" in acima[1]
    assert ate[0] == D("0")


def test_falta_justificada_no_fim_de_semana_acima_de_6h30_vale_meia():
    sabado = dt.date(2026, 9, 5)
    assert fd.quantidade_do_dia(
        _lido(sabado, "FALTA JUSTIFICADA", "06:45"), False)[0] == D("0.5")
    assert fd.quantidade_do_dia(
        _lido(sabado, "FALTA JUSTIFICADA", "06:00"), False)[0] == D("0")


def test_falta_nao_conta():
    assert fd.quantidade_do_dia(_lido(dt.date(2026, 9, 9), "", "", "FALTA"),
                                False)[0] == D("0")


def test_os_ADICIONAIS_sao_20_no_feriado_10_no_sabado_20_no_domingo():
    assert fd.adicional_do_dia(dt.date(2026, 9, 7), True)[0] == D("20.00")
    assert fd.adicional_do_dia(dt.date(2026, 9, 5), False) == (D("10.00"), "sábado")
    assert fd.adicional_do_dia(dt.date(2026, 9, 6), False) == (D("20.00"), "domingo")
    assert fd.adicional_do_dia(dt.date(2026, 9, 8), False)[0] == D("0.00")


def test_o_feriado_de_SABADO_paga_o_adicional_de_feriado_e_nao_o_de_sabado():
    assert fd.adicional_do_dia(dt.date(2026, 9, 5), True) == (D("20.00"), "feriado")


def test_vigia_e_reconhecido_pelo_cargo():
    assert fd.e_vigia({"cargo": "Vigia Noturno"})
    assert not fd.e_vigia({"cargo": "SERVENTE"})


# ---------------------------------------------------------------------------
# PONTA A PONTA, COM BANCO: ponto + cadastro → conta → seleção → fechar → gerar
# ---------------------------------------------------------------------------
DIARISTA, VIGIA, SAIU, SEM_VALOR = ("99713349334", "03513441363",
                                    "11144477735", "52998224725")


def _campos(obra="CREPEOLINDA", presenca="PRESENÇA", horas="08:00"):
    return json.dumps({"obra_entrada": obra, "obra_almoco": obra,
                       "obra_retorno": obra, "obra_saida": obra,
                       "hr_entrada": "07:00", "hr_almoco": "12:00",
                       "hr_retorno": "13:00", "hr_saida": "17:00",
                       "presenca_ausencia": presenca, "totalHrs": horas})


@pytest.fixture
def banco_diaristas(banco_analisesps):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                     " VALUES ('obra', 'CREPEOLINDA', '111')")
        conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                     " VALUES ('CREPEOLINDA', '50024')")
        for cpf, nome, cargo, fase, saida, valor in [
                (DIARISTA, "DIARISTA UM", "SERVENTE", "Colaboradores Ativos", None, "100"),
                (VIGIA, "VIGIA UM", "VIGIA NOTURNO", "Colaboradores Ativos", None, "100"),
                (SAIU, "SAIU UM", "SERVENTE", "Colaboradores Desligados",
                 dt.date(2026, 9, 10), "100"),
                (SEM_VALOR, "SEM VALOR", "SERVENTE", "Colaboradores Ativos", None, None)]:
            conn.execute(
                "INSERT INTO analisesps.colaborador "
                "  (cpf, nome, cargo, fase, tipo, tipo_contrato, data_inicio, "
                "   data_saida, obra_codigo, valor_diaria) "
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (cpf, nome, cargo, fase, "Prestador de Serviço", "Autônomo (RPA)",
                 dt.date(2026, 8, 1), saida, "CREPEOLINDA",
                 D(valor) if valor else None))
        cur = conn.execute(
            "INSERT INTO analisesps.ponto_carga (ano, mes, terminada_em) "
            " VALUES (2026, 9, now()) RETURNING id")
        carga = cur.fetchone()[0]
        cur.close()
        dias = [
            (DIARISTA, dt.date(2026, 9, 5), _campos()),                 # sábado
            (DIARISTA, dt.date(2026, 9, 7), _campos()),                 # feriado
            (DIARISTA, dt.date(2026, 9, 8), _campos(presenca="PRESENÇA PARCIAL",
                                                    horas="08:00")),    # meia
            (DIARISTA, dt.date(2026, 9, 9), json.dumps({"desc_falta": "FALTA"})),
            (VIGIA, dt.date(2026, 9, 8), _campos()),
            (SAIU, dt.date(2026, 9, 8), _campos()),
            (SEM_VALOR, dt.date(2026, 9, 8), _campos()),
        ]
        conn.executemany(
            "INSERT INTO analisesps.ponto_dia (carga_id, cpf, nome, data, campos) "
            " VALUES (?,?,?,?,?)", [(carga, c, "X", d, j) for c, d, j in dias])
        conn.execute("INSERT INTO analisesps.feriado (data, abrangencia, obra, descricao) "
                     " VALUES (?, 'nacional', '', 'Independência')",
                     (dt.date(2026, 9, 7),))
        conn.commit()
    return banco_analisesps


def _pessoa(calculado, cpf):
    return next(p for p in calculado["pessoas"] if p["cpf"] == cpf)


@pytest.mark.banco
def test_a_conta_do_diarista_dia_a_dia(banco_diaristas):
    calculado = fd.calcular(2026, 9, "mes")
    p = _pessoa(calculado, DIARISTA)
    # sábado 100+10, feriado 100+20, meia 50, falta 0.
    assert p["valor"] == D("280.00")
    assert p["quantidade"] == D("2.5")
    assert p["adicionais"] == D("30.00")
    assert p["pagar"] is True
    assert p["por_obra"] == [{"obra": "CREPEOLINDA", "dias": D("2.5"),
                              "valor": D("280.00")}]
    assert calculado["total"] == D("280.00")
    assert calculado["quantos_a_pagar"] == 1


@pytest.mark.banco
def test_vigia_desligado_e_sem_valor_NAO_pagam_e_cada_um_diz_por_que(banco_diaristas):
    calculado = fd.calcular(2026, 9, "mes")
    vigia, saiu, sem = (_pessoa(calculado, c) for c in (VIGIA, SAIU, SEM_VALOR))
    assert not vigia["pagar"] and vigia["vigia"]
    assert not saiu["pagar"] and saiu["desligado"]
    assert not sem["pagar"] and sem["impossivel"]
    assert any("valor da diária" in m for m in sem["motivos"])


@pytest.mark.banco
def test_desmarcar_e_salvar_tira_do_pagamento(banco_diaristas):
    fd.salvar_selecao(2026, 9, "mes", [{"cpf": DIARISTA, "pagar": False}], quem="T")
    p = _pessoa(fd.calcular(2026, 9, "mes"), DIARISTA)
    assert p["pagar"] is False
    assert any("desmarcado manualmente" in m for m in p["motivos"])
    from app.apps.analisesps import folha_apropriacao_guardada as guardada
    with pytest.raises(guardada.ErroDaApropriacao):
        fd.fechar(2026, 9, "mes")


@pytest.mark.banco
def test_FECHAR_libera_o_arquivo_de_pagamento_da_diaria(banco_diaristas, monkeypatch):
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)
    feito = fd.fechar(2026, 9, "mes", quem="MARCELO")
    assert feito["tipo"] == "fim_de_mes"
    linhas = fp.linhas_para_pagar(2026, 9, "fim_de_mes", ["diaria"])
    assert [(l["cpf"], l["obra"], l["conta"], l["valor"]) for l in linhas] == [
        (DIARISTA, "CREPEOLINDA", "50024", D("280.00"))]

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        {"id": "x", "link": "https://drive/x"})
    saida = fp.gerar(2026, 9, "fim_de_mes", ["diaria"], g.SOMAPAY, quem="MARCELO")
    pagamento = [a for a in saida["arquivos"] if a["destino"] == "somapay"]
    assert pagamento[0]["conta"] == "50024"
    assert pagamento[0]["total"] == D("280.00")
    assert fd.calcular(2026, 9, "mes")["fechamento"]["total_apropriado"] == D("280.00")


@pytest.mark.banco
def test_a_QUINZENA_so_conta_os_dias_de_1_a_15(banco_diaristas):
    """Todos os dias do teste caem na primeira quinzena; a segunda fica vazia."""
    assert fd.calcular(2026, 9, "quinzena")["total"] == D("280.00")
    assert fd.calcular(2026, 9, "fim_de_mes")["pessoas"] == []
