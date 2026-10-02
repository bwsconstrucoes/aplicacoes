# -*- coding: utf-8 -*-
"""
O pagamento dos diaristas — 01/10/2026.

O dono: *"a prioridade agora é a de diaristas, que é o próximo que eu vou
gerar"*. A regra é a da aba "Diaristas" da planilha (`docs/FOLHA_DE_PAGAMENTO.md`
§7.10.4 e §7.14.12): só dia de DIÁRIA, vigia fora, a quantidade do dia pela
coluna AJ (fórmula enviada em 02/10/2026), +20 feriado, +10 sábado, +20 domingo. E o que a folha da
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
RPA = {"tipo": "Prestador de Serviço", "contrato": "Autônomo (RPA)"}
CTPS_ANTES = {"tipo": "CTPS", "contrato": "CLT (tempo Indeterminado)"}
TERCA, SABADO = dt.date(2026, 9, 8), dt.date(2026, 9, 5)


def _lido(data, presenca="PRESENÇA", horas=("07:00", "12:00", "13:00", "17:00"),
          falta=""):
    return {"data": data, "presenca": presenca, "horas": list(horas),
            "falta": falta}


def _qtd(lido, cadastro=RPA, vinculo="DIÁRIA", aq=fd.PAGAR_DIARIA):
    return fd.quantidade_do_dia(lido, cadastro, vinculo, aq)[0]


def test_AG_e_saida_menos_entrada_e_cai_para_retorno_e_almoco():
    """`SE(K="";J-I;SE(L="";K-I;L-I))` — o almoço não é descontado."""
    assert fd.horas_trabalhadas(["07:00", "12:00", "13:00", "17:00"]) == D(36000) / 86400
    assert fd.horas_trabalhadas(["07:00", "12:00", "13:00", ""]) == D(21600) / 86400
    assert fd.horas_trabalhadas(["07:00", "12:00", "", ""]) == D(18000) / 86400
    assert fd.horas_trabalhadas(["07:00:00", "", "13:00:30", ""]) == D(21630) / 86400
    assert fd.horas_trabalhadas(["2026-09-08 07:00:00", "2026-09-08 12:00:00",
                                 "2026-09-08 13:00:00", "2026-09-08 17:00:00"]) \
        == D(36000) / 86400


def test_presenca_cheia_vale_UMA_diaria():
    assert _qtd(_lido(TERCA)) == D("1")
    assert _qtd(_lido(TERCA), CTPS_ANTES) == D("1")


def test_presenca_PARCIAL_de_7h_ou_mais_vale_UMA_e_abaixo_vale_MEIA():
    """A fórmula do dono (02/10/2026): AG > 0,2916 → 1; AG < 0,2916 → 0,5."""
    sete = _lido(TERCA, "PRESENÇA PARCIAL", ("07:00", "12:00", "13:00", "14:00"))
    seis = _lido(TERCA, "PRESENÇA PARCIAL", ("07:00", "12:00", "13:00", "13:59"))
    for cadastro in (RPA, CTPS_ANTES):
        assert _qtd(sete, cadastro) == D("1")
        assert _qtd(seis, cadastro) == D("0.5")
    assert "meia" in fd.quantidade_do_dia(seis, RPA, "DIÁRIA")[1]


def test_falta_NAO_justificada_com_presenca_vale_meia_para_o_prestador():
    dia = _lido(TERCA, falta="Falta não justificada")
    assert _qtd(dia) == D("0.5")
    assert _qtd(dia, CTPS_ANTES) == D("1"), "o termo da CTPS não olha a falta"


def test_sem_presenca_nao_conta():
    assert _qtd(_lido(TERCA, "", ("", "", "", ""), "Falta Justificada")) == D("0")
    assert _qtd(_lido(TERCA, "AUSÊNCIA", ("", "", "", ""))) == D("0")


def test_CTPS_em_dia_de_CTPS_nao_e_diaria():
    assert _qtd(_lido(TERCA), CTPS_ANTES, "CTPS") == D("0")


def test_os_termos_de_PAGAR_EXTRA_da_formula_estao_traduzidos():
    """Não servem aos diaristas (a aba filtra PAGAR DIÁRIA), mas são a mesma
    coluna — e a diária extra da CTPS vai usá-los."""
    fj = _lido(SABADO, falta="Falta Justificada",
               horas=("07:00", "12:00", "13:00", "13:45"))
    curto = _lido(SABADO, falta="Falta Justificada",
                  horas=("07:00", "12:00", "13:00", "13:00"))
    assert _qtd(fj, CTPS_ANTES, "CTPS", fd.PAGAR_EXTRA) == D("1")
    assert _qtd(curto, CTPS_ANTES, "CTPS", fd.PAGAR_EXTRA) == D("0.5")
    assert _qtd(_lido(TERCA), CTPS_ANTES, "CTPS", fd.PAGAR_EXTRA) == D("1")


def test_os_ADICIONAIS_sao_20_no_feriado_10_no_sabado_20_no_domingo():
    assert fd.adicional_do_dia(dt.date(2026, 9, 7), True)[0] == D("20.00")
    assert fd.adicional_do_dia(dt.date(2026, 9, 5), False) == (D("10.00"), "sábado")
    assert fd.adicional_do_dia(dt.date(2026, 9, 6), False) == (D("20.00"), "domingo")
    assert fd.adicional_do_dia(dt.date(2026, 9, 8), False)[0] == D("0.00")


def test_o_feriado_de_SABADO_paga_o_adicional_de_feriado_e_nao_o_de_sabado():
    assert fd.adicional_do_dia(dt.date(2026, 9, 5), True) == (D("20.00"), "feriado")


def test_vigia_e_a_funcao_EXATA_como_na_planilha():
    """A planilha filtra `W != 'VIGIA'` — igualdade, não "contém"."""
    assert fd.e_vigia({"cargo": "VIGIA"})
    assert fd.e_vigia({"cargo": " Vigia "})
    assert not fd.e_vigia({"cargo": "VIGIA NOTURNO"})
    assert not fd.e_vigia({"cargo": "SERVENTE"})


def test_diarista_e_pago_por_QUINZENA_e_a_tela_abre_na_que_esta_sendo_paga():
    assert fd.periodo_sugerido(dt.date(2026, 10, 2)) == (2026, 9, "fim_de_mes")
    assert fd.periodo_sugerido(dt.date(2026, 10, 11)) == (2026, 10, "quinzena")
    assert fd.periodo_sugerido(dt.date(2027, 1, 5)) == (2026, 12, "fim_de_mes")
    assert set(fd.PERIODOS) == {"quinzena", "fim_de_mes"}


def test_o_valor_da_diaria_vem_da_COLUNA_49_quando_o_nome_nao_casa():
    """Informado pelo dono: "Valor Diária está na coluna 49 em 'Dados
    Documentos'"."""
    from app.apps.analisesps import colaboradores as col
    cabecalho = ["CPF (Cadastro de Pessoa Física)", "Nome Completo"] + \
        [f"outra {i}" for i in range(3, 49)] + ["Diária (R$)"]
    posicoes, avisos = col._achar_colunas(cabecalho)
    assert posicoes["valor_diaria"] == 48
    assert not any("Valor da Diária" in a for a in avisos)


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
                (VIGIA, "VIGIA UM", "VIGIA", "Colaboradores Ativos", None, "100"),
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
                                                    horas="08:00")),    # parcial de 10h: 1
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
    calculado = fd.calcular(2026, 9, "quinzena")
    p = _pessoa(calculado, DIARISTA)
    # sábado 100+10, feriado 100+20, parcial de 10h 100, falta 0.
    assert p["valor"] == D("330.00")
    assert p["quantidade"] == D("3")
    assert p["adicionais"] == D("30.00")
    assert p["pagar"] is True
    assert p["por_obra"] == [{"obra": "CREPEOLINDA", "dias": D("3"),
                              "valor": D("330.00")}]
    assert calculado["total"] == D("330.00")
    assert calculado["quantos_a_pagar"] == 1


@pytest.mark.banco
def test_vigia_desligado_e_sem_valor_NAO_pagam_e_cada_um_diz_por_que(banco_diaristas):
    calculado = fd.calcular(2026, 9, "quinzena")
    vigia, saiu, sem = (_pessoa(calculado, c) for c in (VIGIA, SAIU, SEM_VALOR))
    assert not vigia["pagar"] and vigia["vigia"]
    assert not saiu["pagar"] and saiu["desligado"]
    assert not sem["pagar"] and sem["impossivel"]
    assert any("valor da diária" in m for m in sem["motivos"])


@pytest.mark.banco
def test_desmarcar_e_salvar_tira_do_pagamento(banco_diaristas):
    fd.salvar_selecao(2026, 9, "quinzena", [{"cpf": DIARISTA, "pagar": False}], quem="T")
    p = _pessoa(fd.calcular(2026, 9, "quinzena"), DIARISTA)
    assert p["pagar"] is False
    assert any("desmarcado manualmente" in m for m in p["motivos"])
    from app.apps.analisesps import folha_apropriacao_guardada as guardada
    with pytest.raises(guardada.ErroDaApropriacao):
        fd.fechar(2026, 9, "quinzena")


@pytest.mark.banco
def test_FECHAR_libera_o_arquivo_de_pagamento_da_diaria(banco_diaristas, monkeypatch):
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)
    feito = fd.fechar(2026, 9, "quinzena", quem="MARCELO")
    assert feito["tipo"] == "quinzena"
    linhas = fp.linhas_para_pagar(2026, 9, "quinzena", ["diaria"])
    assert [(l["cpf"], l["obra"], l["conta"], l["valor"]) for l in linhas] == [
        (DIARISTA, "CREPEOLINDA", "50024", D("330.00"))]

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        {"id": "x", "link": "https://drive/x"})
    saida = fp.gerar(2026, 9, "quinzena", ["diaria"], g.SOMAPAY, quem="MARCELO")
    pagamento = [a for a in saida["arquivos"] if a["destino"] == "somapay"]
    assert pagamento[0]["conta"] == "50024"
    assert pagamento[0]["total"] == D("330.00")
    assert fd.calcular(2026, 9, "quinzena")["fechamento"]["total_apropriado"] == D("330.00")


@pytest.mark.banco
def test_a_QUINZENA_so_conta_os_dias_de_1_a_15(banco_diaristas):
    """Todos os dias do teste caem na primeira quinzena; a segunda fica vazia."""
    assert fd.calcular(2026, 9, "quinzena")["total"] == D("330.00")
    assert fd.calcular(2026, 9, "fim_de_mes")["pessoas"] == []


# ---------------------------------------------------------------------------
# GERAR DE DENTRO DA FOLHA (02/10/2026) — resumo sem gravar; "ok" fecha e gera
# ---------------------------------------------------------------------------
def _drive_falso(monkeypatch):
    from app.apps.analisesps import beevale, drive
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append(nome) or {"id": f"d{len(subidos)}",
                                                 "link": f"https://drive/{len(subidos)}"})
    return subidos


@pytest.mark.banco
def test_o_RESUMO_da_geracao_direta_nao_grava_nada(banco_diaristas, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    _drive_falso(monkeypatch)
    plano = fp.resumo_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena"},
                             "somapay")
    assert plano["resumo"]["total"] == D("330.00")
    assert [l["conta"] for l in plano["lotes"]] == ["50024"]
    assert fd.calcular(2026, 9, "quinzena")["fechamento"] is None
    assert fp.log() == []


@pytest.mark.banco
def test_GERAR_DIRETO_fecha_e_gera_de_uma_vez(banco_diaristas, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fp
    subidos = _drive_falso(monkeypatch)
    saida = fp.gerar_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena"},
                            "somapay", quem="MARCELO")
    assert len(subidos) == 2, "o de pagamento e o de análise"
    assert fd.calcular(2026, 9, "quinzena")["fechamento"]["total_apropriado"] == D("330.00")
    rodada = fp.rodadas()[0]
    assert rodada["lancavel"] and not rodada["incompleta"]
    assert rodada["total"] == D("330.00")
    assert saida["competencia"] == "09/2026"


@pytest.mark.banco
def test_gerar_de_novo_refaz_o_fechamento_com_a_SITUACAO_ATUAL(banco_diaristas, monkeypatch):
    """O arquivo nunca sai de um fechamento antigo."""
    from app.apps.analisesps import folha_pagamento as fp
    _drive_falso(monkeypatch)
    fd.fechar(2026, 9, "quinzena")
    fd.salvar_selecao(2026, 9, "quinzena", [{"cpf": DIARISTA, "pagar": False}])
    with pytest.raises(fp.ErroDoPagamento):
        fp.gerar_direto("diaria", {"ano": 2026, "mes": 9, "periodo": "quinzena"},
                        "somapay")


# ---------------------------------------------------------------------------
# SEM DIÁRIA × CADASTRO INCOMPLETO (02/10/2026) — eram o mesmo "dados incompletos"
# ---------------------------------------------------------------------------
def _ficha(**m):
    base = {"cpf": "99713349334", "nome": "X", "cargo": "SERVENTE",
            "tipo": "Prestador de Serviço", "tipo_contrato": "Autônomo (RPA)",
            "data_inicio": dt.date(2026, 8, 1), "data_admissao": None,
            "situacao": "ativo"}
    base.update(m)
    return base


def _dia_lido(data, presenca="PRESENÇA", falta=""):
    return {"data": data, "presenca": presenca, "falta": falta,
            "horas": ["07:00", "12:00", "13:00", "17:00"],
            "marcacoes": ["OBRA1"] * 4 if presenca else ["", "", "", ""]}


def _calc(ficha, dias, valor="100"):
    return fd.calcular_pessoa(ficha, dias, dt.date(2026, 9, 1), dt.date(2026, 9, 15),
                              D(valor) if valor else None, [], {}, "OBRA1")


def test_dia_de_diaria_SEM_PRESENCA_e_sem_diaria_e_nao_cadastro_incompleto():
    p = _calc(_ficha(), [_dia_lido(TERCA, "", "Falta não justificada")])
    assert p["sem_diaria"] and not p["impossivel"] and not p["pagar"]
    from app.apps.analisesps import folha_lista
    assert folha_lista.situacoes_da_pessoa(p) == {"sem_diaria"}


def test_sem_VALOR_com_diaria_e_cadastro_incompleto():
    p = _calc(_ficha(), [_dia_lido(TERCA)], valor=None)
    assert p["impossivel"] and not p["sem_diaria"]
    assert any("valor da diária" in m for m in p["motivos"])


def test_sem_DATAS_no_cadastro_aparece_como_cadastro_incompleto():
    """O RPA é diária em todo dia (coluna AQ); o caso é de quem é CTPS."""
    p = _calc(_ficha(tipo="CTPS", tipo_contrato="CLT (tempo Indeterminado)",
                     data_inicio=None), [_dia_lido(TERCA)])
    assert p["impossivel"] and p["dias_sem_decidir"] == 1


def test_prestador_SEM_RPA_diz_por_que_nao_recebe_diaria():
    p = _calc(_ficha(tipo_contrato="PJ"), [_dia_lido(TERCA)])
    assert p["sem_diaria"]
    assert "Autônomo (RPA)" in p["motivos"][-1]
