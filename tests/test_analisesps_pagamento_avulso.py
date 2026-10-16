# -*- coding: utf-8 -*-
"""
PAGAMENTO AVULSO — 15/10/2026.

O dono: "uma lista com CPF, valor e obra (…) gerar SomaPay ou BeeVale (…) a
dinâmica toda é igual (…) a diferença é a fonte dos dados". Estes testes provam
o caminho com banco: colar, enriquecer pelo cadastro e pelo ponto, alertar,
selecionar, trocar a obra, gerar, guardar o lote e lançar a SP no Pipefy.
"""
from decimal import Decimal as D

import pytest

from tests.test_analisesps_telas import _como_mestre, app  # noqa: F401 — a fixture

pytestmark = pytest.mark.banco

ATIVO, SAIU, FORA = "99713349334", "03513441363", "11122233396"


def test_ler_a_LINHA_colada_em_qualquer_ordem():
    from app.apps.analisesps.pagamento_avulso import ler_linha, ler_texto
    assert ler_linha("997.133.493-34\t1.500,00\tCREPEOLINDA")["obra"] == "CREPEOLINDA"
    l = ler_linha("R$ 250,50; 99713349334")
    assert (l["cpf"], l["valor"], l["obra"]) == (ATIVO, D("250.50"), "")
    l = ler_linha("3513441363\t99,90")                 # a planilha comeu o zero
    assert l["cpf"] == SAIU and l["cpf_completado"]
    boas, recusadas = ler_texto("CPF\tValor\tObra\n\n997.133.493-34 10,00\nsem cpf 5,00\n")
    assert len(boas) == 1 and recusadas == [
        {"linha": 4, "texto": "sem cpf 5,00", "erro": "não achei o CPF (11 dígitos)."}]


@pytest.fixture
def banco_avulso(banco_analisesps, monkeypatch):
    from app.apps.analisesps import folha_cards, pagamento_avulso as pa
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        for codigo, conta in (("CREPEOLINDA", "50024"), ("IFSPSAOJOSE", "7011")):
            conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                         " VALUES ('obra', ?, ?)", (codigo, codigo[:3]))
            conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                         " VALUES (?, ?)", (codigo, conta))
        conn.execute("INSERT INTO analisesps.colaborador (cpf, nome, fase, cargo, obra_codigo) "
                     " VALUES (?,?,?,?,?)", (ATIVO, "ATIVO UM", "Colaboradores Ativos",
                                             "PEDREIRO", "IFSPSAOJOSE"))
        conn.execute("INSERT INTO analisesps.colaborador (cpf, nome, fase, cargo, data_saida) "
                     " VALUES (?,?,?,?, DATE '2026-09-01')",
                     (SAIU, "SAIU DOIS", "Desligados", "SERVENTE"))
        conn.commit()
    monkeypatch.setattr(folha_cards, "_codigos_omie", lambda: {
        folha_cards._chave("CREPEOLINDA"): "111", folha_cards._chave("IFSPSAOJOSE"): "222"})
    monkeypatch.setattr(folha_cards, "_plano_financeiro", lambda: ({
        folha_cards._chave("Gratificações e Extras"): [
            {"nome": "Gratificações e Extras", "record_id": "R-GRA", "codigo_omie": "2.03.01"}],
    }, ""))
    # O ponto: ATIVO trabalhou na CREPEOLINDA; os outros não têm ponto.
    monkeypatch.setattr(pa, "_obras_do_ponto", lambda hoje: (
        {ATIVO: {"obra": "CREPEOLINDA", "dias": 9}}, {ATIVO: [{}]}))
    return banco_analisesps


COLADO = "\n".join(["997.133.493-34\t1.000,00",              # sem obra: vale a do ponto
                    "035.134.413-63\t200,00\tCREPEOLINDA",     # desligado
                    "111.222.333-96\t50,00\tCREPEOLINDA",      # fora do cadastro
                    "997.133.493-34\t300,00\tIFSPSAOJOSE"])    # mesmo CPF, obra colada


def _por(calculado):
    return {(p["cpf"], p["valor"]): p for p in calculado["pessoas"]}


def test_a_lista_COLADA_vira_linhas_com_cadastro_ponto_e_alertas(banco_avulso):
    from app.apps.analisesps import pagamento_avulso as pa
    pa.gravar_lista(COLADO, "prêmio de setembro", quem="MARCELO")
    c = _por(pa.calcular())
    sem_obra = c[(ATIVO, D("1000.00"))]
    assert sem_obra["obra"] == "CREPEOLINDA" and sem_obra["obra_de_onde"] == "ponto"
    assert sem_obra["obra_cadastro"] == "IFSPSAOJOSE" and sem_obra["conta"] == "50024"
    assert sem_obra["pagar"] and any("2 vezes" in m for m in sem_obra["motivos"])
    colada = c[(ATIVO, D("300.00"))]
    assert colada["obra"] == "IFSPSAOJOSE" and colada["obra_de_onde"] == "colada"
    assert any("o ponto diz CREPEOLINDA" in m for m in colada["motivos"])
    saiu = c[(SAIU, D("200.00"))]
    assert not saiu["pagar"], "quem já saiu vem desmarcado"
    assert any("desligado" in m for m in saiu["motivos"])
    assert any("sem ponto" in m for m in saiu["motivos"])
    fora = c[(FORA, D("50.00"))]
    assert fora["impossivel"] and not fora["pagar"]
    tudo = pa.calcular()
    assert tudo["total"] == D("1300.00") and tudo["quantos_a_pagar"] == 2


def test_SELECAO_e_OBRA_trocada_valem_ate_colar_outra_lista(banco_avulso):
    from app.apps.analisesps import pagamento_avulso as pa
    pa.gravar_lista(COLADO, quem="MARCELO")
    c = _por(pa.calcular())
    chave = c[(ATIVO, D("1000.00"))]["chave"]
    pa.escolher_obra(chave, "ifspsaojose")
    pa.salvar_selecao([{"chave": c[(SAIU, D("200.00"))]["chave"], "pagar": True}])
    c = _por(pa.calcular())
    assert c[(ATIVO, D("1000.00"))]["obra"] == "IFSPSAOJOSE"
    assert c[(ATIVO, D("1000.00"))]["rotulo_obra"] == "trocada por você"
    assert c[(SAIU, D("200.00"))]["pagar"], "marcar à mão paga mesmo assim"
    pa.gravar_lista(COLADO, quem="MARCELO")
    assert not _por(pa.calcular())[(SAIU, D("200.00"))]["pagar"], "lista nova, sem ajustes"


def _gerar(monkeypatch, destino="beevale"):
    from app.apps.analisesps import beevale, drive, folha_pagamento as fp
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append((nome, k.get("mime"), conteudo))
                        or {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"})
    return fp.gerar_direto("avulso", {}, destino, quem="MARCELO"), subidos


def test_GERAR_sobe_no_Drive_guarda_o_lote_e_TRAVA_a_lista(banco_avulso, monkeypatch):
    import io
    import openpyxl
    from app.apps.analisesps import folha_pagamento as fp, pagamento_avulso as pa
    pa.gravar_lista(COLADO, "prêmio", quem="MARCELO")
    saida, subidos = _gerar(monkeypatch)
    pagamento = sorted((a for a in saida["arquivos"] if a["destino"] == "beevale"),
                       key=lambda a: a["conta"])
    assert [(a["conta"], a["total"]) for a in pagamento] == [
        ("50024", D("1000.00")), ("7011", D("300.00"))]
    assert "Pagamento avulso" in pagamento[0]["nome"]
    conteudo = next(c for n, m, c in subidos if n == pagamento[0]["nome"])
    aba = openpyxl.load_workbook(io.BytesIO(conteudo)).active
    assert aba.cell(row=2, column=3).value == "Gratiticações e Extras", "a carteira do tipo"
    assert any(m == fp.MIME_PDF for _, m, _ in subidos), "o relatório de cada conta"
    assert any(n.startswith("Cadastro") for n, _, _ in subidos), "a planilha de cadastro"

    analise = next(a["id"] for a in saida["arquivos"] if a["destino"] == fp.ANALISE)
    lote = pa.lote_da_analise(analise)
    assert lote["descricao"] == "prêmio" and len(lote["linhas"]) == 2
    assert pa.calcular()["gerado"]["analise_id"] == analise
    with pytest.raises(fp.ErroDoPagamento):
        _gerar(monkeypatch)                                # a mesma lista não sai duas vezes

    # Excluir a geração devolve a lista para gerar de novo.
    fp.excluir_arquivos([a["id"] for a in saida["arquivos"]], quem="MARCELO")
    assert pa.lote_da_analise(analise) is None and not pa.calcular()["gerado"]


def test_a_SP_do_Pipefy_sai_do_LOTE_com_o_tipo_de_despesa_escolhido(banco_avulso, monkeypatch):
    from app.apps.analisesps import folha_cards as fcd, folha_pagamento as fp
    from app.apps.analisesps import pagamento_avulso as pa
    from tests.test_analisesps_folha_cards import PipefyFalso
    pipe = PipefyFalso(monkeypatch)
    pa.gravar_lista(COLADO, "prêmio de setembro", quem="MARCELO")
    saida, _ = _gerar(monkeypatch, destino="somapay")
    analise = next(a["id"] for a in saida["arquivos"] if a["destino"] == fp.ANALISE)
    vista = fcd.previa(analise)
    assert vista["bloqueios"] == [], vista["bloqueios"]
    grupo = vista["grupos"][0]
    assert grupo["verba"] == "avulso" and grupo["tipo_despesa"] == "Gratificações e Extras"
    assert grupo["tipo_sp"] == "R-GRA" and grupo["total"] == D("1300.00")
    sp = next(s for s in grupo["sps"] if s["conta"] == "50024")
    linhas = sp["descricao"].splitlines()
    assert linhas[0] == "Conta Origem: 50024" and linhas[1] == "Descrição: prêmio de setembro"
    fcd.lancar(analise, quem="MARCELO")
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 2, "uma SP por conta"


def test_a_TELA_cola_mostra_e_tem_os_botoes_de_gerar(banco_avulso, app):  # noqa: F811
    cliente = _como_mestre(app)
    vazia = cliente.get("/analisesps/folha/avulso").get_data(as_text=True)
    assert "Colar a lista" in vazia and 'id="btn-avulso-ler"' in vazia
    r = cliente.post("/analisesps/api/folha/avulso/lista",
                     json={"texto": COLADO + "\nlixo", "descricao": "prêmio"}).get_json()
    assert r["ok"] and r["linhas"] == 4 and len(r["recusadas"]) == 1
    tela = cliente.get("/analisesps/folha/avulso").get_data(as_text=True)
    assert "ATIVO UM" in tela and "usar a do cadastro" in tela
    assert "ficaram de fora" in tela and "Pagamento avulso" in tela
    assert 'data-origem="avulso"' in tela or "avulso" in tela
    assert cliente.get("/analisesps/folha/avulso/relatorio.pdf").data.startswith(b"%PDF")
    r = cliente.post("/analisesps/api/folha/avulso/lista", json={"limpar": True}).get_json()
    assert r["ok"] and "ATIVO UM" not in cliente.get("/analisesps/folha/avulso").get_data(as_text=True)
