# -*- coding: utf-8 -*-
"""
Fechar o auxílio — 01/10/2026.

O botão "Conferir e gerar" da tela de alimentação e transporte levava a uma tela
que dizia "nada fechado": nenhuma tela fechava essas verbas, e o gerador só paga
verba fechada. Estes testes provam o caminho inteiro, com banco.
"""
import datetime as dt
from decimal import Decimal as D

import pytest

pytestmark = pytest.mark.banco

ATIVO, SAIU = "99713349334", "03513441363"


@pytest.fixture
def banco_auxilio(banco_analisesps):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                     " VALUES ('obra', 'CREPEOLINDA', '111')")
        conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                     " VALUES ('CREPEOLINDA', '50024')")
        for cpf, nome, fase, saida in [
                (ATIVO, "ATIVO UM", "Colaboradores Ativos", None),
                (SAIU, "SAIU UM", "Colaboradores Desligados", dt.date(2026, 8, 20))]:
            conn.execute(
                "INSERT INTO analisesps.colaborador "
                "  (cpf, nome, fase, data_saida, obra_codigo, valor_alimentacao, "
                "   modo_alimentacao) VALUES (?,?,?,?,?,?,?)",
                (cpf, nome, fase, saida, "CREPEOLINDA", D("300.00"), "Mês"))
        conn.commit()
    # Sem ponto, a obra é escolhida por ele (03/10/2026) — a do cadastro não
    # entra sozinha.
    from app.apps.analisesps import folha_auxilio as fx
    fx.gravar_extras(fx.ALIMENTACAO, 2026, 9, ATIVO, obra="CREPEOLINDA")
    return banco_analisesps


def test_fechar_a_alimentacao_libera_o_arquivo(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx, folha_pagamento as fp
    feito = fx.fechar(fx.ALIMENTACAO, 2026, 9, "fim_de_mes", quem="MARCELO")
    assert feito["pessoas"] == 1 and feito["total"] == D("300.00")
    linhas = fp.linhas_para_pagar(2026, 9, "fim_de_mes", ["alimentacao"])
    assert [(l["cpf"], l["conta"], l["valor"]) for l in linhas] == [
        (ATIVO, "50024", D("300.00"))]
    assert fx.calcular(fx.ALIMENTACAO, 2026, 9)["fechamento"]["tipo"] == "fim_de_mes"


def test_quem_ja_SAIU_nao_entra_no_fechamento(banco_auxilio):
    from app.apps.analisesps import folha_auxilio as fx
    calculado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    saiu = next(p for p in calculado["pessoas"] if p["cpf"] == SAIU)
    assert saiu["desligado"] and not saiu["pagar"]


def test_fechar_num_pagamento_TIRA_o_fechamento_do_outro(banco_auxilio):
    """Senão a mesma verba do mês sairia duas vezes."""
    from app.apps.analisesps import (folha_apropriacao_guardada as guardada,
                                     folha_auxilio as fx)
    fx.fechar(fx.ALIMENTACAO, 2026, 9, "quinzena")
    fx.fechar(fx.ALIMENTACAO, 2026, 9, "fim_de_mes")
    assert guardada.fechamento(2026, 9, "quinzena", "alimentacao") is None
    assert guardada.fechamento(2026, 9, "fim_de_mes", "alimentacao") is not None


def test_gerar_o_auxilio_sobe_o_RELATORIO_em_PDF_da_conta(banco_auxilio, monkeypatch):
    """Dono, 03/10/2026: o relatório em PDF gerado junto, baixável em Arquivos
    gerados e com link no card."""
    from app.apps.analisesps import beevale, drive, folha_pagamento as fp
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append((nome, k.get("mime"), conteudo[:5]))
                        or {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"})
    fp.gerar_direto("alimentacao", {"ano": 2026, "mes": 9, "pagamento": "fim_de_mes"},
                    "beevale", quem="MARCELO")
    pdfs = [s for s in subidos if s[1] == fp.MIME_PDF]
    assert len(pdfs) == 1 and pdfs[0][2] == b"%PDF-" and "50024" in pdfs[0][0]
    rodada = fp.rodadas()[0]
    assert rodada["relatorios"]["50024"]["link"]
    assert rodada["total"] == D("300.00")


def test_a_REGRA_DE_RATEIO_divide_o_auxilio_entre_as_obras(banco_auxilio):
    """Dono, 03/10/2026: *"o rateio das obras serve sim para alimentação e
    transporte e diaristas"*. A regra manda sobre o ponto, como na folha."""
    from app.apps.analisesps import folha_auxilio as fx, folha_rateio as fr
    # A obra escolhida à mão manda sobre a regra; tirada, vale a regra.
    fx.gravar_extras(fx.ALIMENTACAO, 2026, 9, ATIVO, obra="")
    fr.gravar({"nome": "Supervisores", "obras": [
        {"obra": "CREPEOLINDA", "percentual": "60"},
        {"obra": "CREPEAREIAS", "percentual": "40"}],
        "pessoas": [{"cpf": ATIVO}]}, "MARCELO")
    calculado = fx.calcular(fx.ALIMENTACAO, 2026, 9)
    p = next(x for x in calculado["pessoas"] if x["cpf"] == ATIVO)
    assert p["obra_de_onde"] == "regra" and p["regra"] == "Supervisores"
    assert [(x["obra"], x["valor"]) for x in p["rateio"]] == [
        ("CREPEOLINDA", D("180.00")), ("CREPEAREIAS", D("120.00"))]
    por_obra = {o["obra"]: o["total"] for o in calculado["por_obra"]}
    assert por_obra == {"CREPEOLINDA": D("180.00"), "CREPEAREIAS": D("120.00")}

    fx.fechar(fx.ALIMENTACAO, 2026, 9, "fim_de_mes", quem="MARCELO")
    from app.apps.analisesps import folha_pagamento as fp
    pagas = fp.linhas_para_pagar(2026, 9, "fim_de_mes", ["alimentacao"])
    assert sorted((l["obra"], l["valor"]) for l in pagas) == [
        ("CREPEAREIAS", D("120.00")), ("CREPEOLINDA", D("180.00"))]


def test_A_TRAVA_E_O_ARQUIVO_gerar_de_novo_pede_confirmacao(banco_auxilio, monkeypatch):
    """O dono, 06/10/2026, ao tirar o botão "Fechar": *"a trava é o arquivo que a
    gente gerou"*. Nada impedia gerar o mesmo auxílio duas vezes (pagaria duas)."""
    from app.apps.analisesps import beevale, drive, folha_pagamento as fp
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append(nome)
                        or {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"})
    monkeypatch.setattr(drive, "mover_para_lixeira", lambda arquivo: None)
    pedido = {"ano": 2026, "mes": 9, "pagamento": "fim_de_mes"}
    assert fp.resumo_direto("alimentacao", pedido, "beevale")["resumo"]["ja_gerado"] == []
    primeira = fp.gerar_direto("alimentacao", pedido, "beevale", quem="MARCELO")

    resumo = fp.resumo_direto("alimentacao", pedido, "beevale")["resumo"]
    assert len(resumo["ja_gerado"]) == 1 and not resumo["pode_gerar"]
    # O auxílio é um por mês: na quinzena também conta.
    quinzena = dict(pedido, pagamento="quinzena")
    assert fp.resumo_direto("alimentacao", quinzena, "beevale")["resumo"]["ja_gerado"]
    with pytest.raises(fp.ErroDoPagamento) as e:
        fp.gerar_direto("alimentacao", pedido, "beevale", quem="MARCELO")
    assert "já foi gerado" in str(e.value) and "MARCELO" in str(e.value)
    antes = len(subidos)
    fp.gerar_direto("alimentacao", pedido, "beevale", quem="MARCELO", forcar=True)
    assert len(subidos) > antes, "com a confirmação, gera"

    # Excluir as gerações destrava.
    fp.excluir_arquivos([a["id"] for a in fp.log()], quem="MARCELO")
    assert fp.resumo_direto("alimentacao", pedido, "beevale")["resumo"]["ja_gerado"] == []
    assert primeira
