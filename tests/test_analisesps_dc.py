# -*- coding: utf-8 -*-
"""
DESPESAS COM COLABORADORES (DC) — 03/10/2026.

A aba "Data" da planilha da DC vira uma tela no padrão das outras folhas, e o
arquivo do BeeVale/SomaPay sai daqui. Estes testes provam o caminho com banco:
ler a aba, enriquecer pelo cadastro, selecionar, gerar, esconder o gerado e
acusar a duplicidade.
"""
from decimal import Decimal as D

import pytest

from tests.test_analisesps_telas import _como_mestre, app  # noqa: F401 — a fixture

pytestmark = pytest.mark.banco

ATIVO, OUTRO, FORA = "99713349334", "03513441363", "11122233396"

CABECALHO = ["ID", "Data Solicitação", "Vencimento", "Centro de Custo", "Tipo",
             "Período", "Valor diária?", "Responsável", "Requerente", "CPF",
             "Qtd", "Valor", "Valor diária", "Descrição"]
DATA = [
    CABECALHO,
    ["900100", "01/10/2026", "05/10/2026", "CREPEOLINDA", "Despesas com Alimentação",
     "", "", "JOAO", "MARIA", "997.133.493-34", "", "150,00", "", "Lanche da equipe"],
    ["900100", "01/10/2026", "05/10/2026", "CREPEOLINDA", "Despesas com Alimentação",
     "", "", "JOAO", "MARIA", "035.134.413-63", "3", "", "", "Lanche da equipe"],
    ["900200", "02/10/2026", "06/10/2026", "CREPEOLINDA", "Diárias de Viagem",
     "", "", "JOAO", "MARIA", "111.222.333-96", "", "80,00", "", "Fora do cadastro"],
    ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
]


@pytest.fixture
def banco_dc(banco_analisesps, monkeypatch):
    from app.apps.analisesps import dc, folha_cards
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                     " VALUES ('obra', 'CREPEOLINDA', '111')")
        conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                     " VALUES ('CREPEOLINDA', '50024')")
        for cpf, nome, diaria in [(ATIVO, "ATIVO UM", None), (OUTRO, "OUTRO DOIS", D("40.00"))]:
            conn.execute(
                "INSERT INTO analisesps.colaborador (cpf, nome, fase, cargo, valor_diaria) "
                " VALUES (?,?,?,?,?)", (cpf, nome, "Colaboradores Ativos", "PEDREIRO", diaria))
        conn.commit()

    abas = {dc.ABA_DATA: DATA,
            "Data base BeeVale": [["Plano Financeiro", "Carteira BeeVale"],
                                  ["Despesas com Alimentação", "Auxílio Alimentação"]]}

    def ler_aba(nome):
        if nome not in abas:
            raise RuntimeError("aba não existe")
        return abas[nome]
    monkeypatch.setattr(dc, "_ler_aba", ler_aba)
    monkeypatch.setattr(folha_cards, "_plano_financeiro", lambda: ({
        folha_cards._chave("Despesas com Alimentação"): [
            {"nome": "Despesas com Alimentação", "record_id": "R-ALI", "codigo_omie": "2.01.01"}],
        folha_cards._chave("Diárias de Viagem"): [
            {"nome": "Diárias de Viagem", "record_id": "R-VIA", "codigo_omie": "2.02.01"}],
    }, ""))
    dc._cache.clear()
    yield banco_analisesps
    dc._cache.clear()


@pytest.fixture
def cliente_mestre(banco_dc, app):  # noqa: F811
    return _como_mestre(app)


def test_a_aba_Data_vira_LINHAS_enriquecidas_pelo_cadastro(banco_dc):
    from app.apps.analisesps import dc
    calculado = dc.calcular()
    por_cpf = {p["cpf"]: p for p in calculado["pessoas"]}
    assert set(por_cpf) == {ATIVO, OUTRO, FORA}       # cabeçalho e linha vazia fora

    ativo = por_cpf[ATIVO]
    assert ativo["nome"] == "ATIVO UM" and ativo["cargo"] == "PEDREIRO"
    assert ativo["valor"] == D("150.00")                # o valor informado
    assert ativo["conta"] == "50024" and ativo["omie"] == "111"
    assert ativo["categoria"] == "2.01.01" and ativo["record_id"] == "R-ALI"
    assert ativo["carteira"] == "Auxílio Alimentação"   # da aba Data base BeeVale
    assert ativo["pagar"] and ativo["link_card"].endswith("/900100")

    # Sem valor informado: quantidade × diária do cadastro.
    assert por_cpf[OUTRO]["valor"] == D("120.00")

    # Fora do cadastro: não entra (o banco recusa sem o nome).
    fora = por_cpf[FORA]
    assert fora["impossivel"] and not fora["pagar"]
    assert fora["carteira"] == dc.CARTEIRA_PADRAO       # tipo sem carteira = Produção

    assert calculado["total"] == D("270.00") and calculado["quantos_a_pagar"] == 2
    assert calculado["cards"] == 2


def test_a_SELECAO_guarda_so_a_excecao(banco_dc):
    from app.apps.analisesps import dc
    chave = next(p["chave"] for p in dc.calcular()["pessoas"] if p["cpf"] == OUTRO)
    dc.salvar_selecao([{"chave": chave, "pagar": False}], quem="MARCELO")
    calculado = dc.calcular()
    assert not next(p for p in calculado["pessoas"] if p["cpf"] == OUTRO)["pagar"]
    assert calculado["total"] == D("150.00")
    # Remarcar volta ao calculado e apaga a exceção.
    dc.salvar_selecao([{"chave": chave, "pagar": True}], quem="MARCELO")
    assert dc.ajustes() == {}


def test_trocar_a_OBRA_muda_a_conta(banco_dc):
    from app.apps.analisesps import dc
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                     " VALUES ('CREPEAREIAS', '60010')")
        conn.commit()
    chave = next(p["chave"] for p in dc.calcular()["pessoas"] if p["cpf"] == ATIVO)
    dc.escolher_obra(chave, "crepeareias", quem="MARCELO")
    p = next(p for p in dc.calcular()["pessoas"] if p["chave"] == chave)
    assert p["obra"] == "CREPEAREIAS" and p["obra_trocada"] and p["conta"] == "60010"
    dc.escolher_obra(chave, "", quem="MARCELO")
    assert dc.ajustes() == {}


def test_GERAR_sobe_o_arquivo_guarda_o_lote_e_TIRA_as_linhas_da_tela(banco_dc, monkeypatch):
    import io
    import openpyxl
    from app.apps.analisesps import beevale, dc, drive, folha_pagamento as fp
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append((nome, k.get("mime"), conteudo))
                        or {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"})
    saida = fp.gerar_direto("dc", {}, "beevale", quem="MARCELO")
    pagamento = [a for a in saida["arquivos"] if a["destino"] == "beevale"]
    assert len(pagamento) == 1 and pagamento[0]["total"] == D("270.00")
    assert "Despesas com colaboradores" in pagamento[0]["nome"]

    # A carteira de cada linha vai no arquivo do BeeVale.
    conteudo = next(c for n, m, c in subidos if n == pagamento[0]["nome"])
    aba = openpyxl.load_workbook(io.BytesIO(conteudo)).active
    carteiras = {aba.cell(row=r, column=1).value: aba.cell(row=r, column=3).value
                 for r in range(2, aba.max_row + 1)}
    assert carteiras == {"ATIVO UM": "Auxílio Alimentação", "OUTRO DOIS": "Auxílio Alimentação"}
    assert any(m == fp.MIME_PDF for _, m, _ in subidos)   # o relatório da conta

    # O lote guarda o que foi; a tela não mostra mais.
    analise = next(a["id"] for a in saida["arquivos"] if a["destino"] == fp.ANALISE)
    lote = dc.lote_da_analise(analise)
    assert sorted(l["cpf"] for l in lote["linhas"]) == sorted([ATIVO, OUTRO])
    assert all(l["destino"] == "beevale" for l in lote["linhas"])
    calculado = dc.calcular()
    assert {p["cpf"] for p in calculado["pessoas"]} == {FORA}
    assert calculado["geradas"] == 2
    assert {p["cpf"] for p in dc.calcular(mostrar_geradas=True)["pessoas"]} == {ATIVO, OUTRO, FORA}

    # Excluir a geração devolve as linhas para a tela.
    fp.excluir_arquivos([a["id"] for a in saida["arquivos"]], quem="MARCELO")
    assert dc.lote_da_analise(analise) is None
    assert {p["cpf"] for p in dc.calcular()["pessoas"]} == {ATIVO, OUTRO, FORA}


def test_a_DUPLICIDADE_acusa_o_mesmo_CPF_e_tipo_dos_ultimos_10_dias(banco_dc):
    from app.apps.analisesps import dc
    linhas = [l for l in dc.linhas_a_pagar() if l["cpf"] == ATIVO]
    dc.registrar_lote(None, [dict(linhas[0], chave="outra-chave", card_id="800000")],
                      {"50024": "beevale"}, quem="MARCELO")
    p = next(p for p in dc.calcular()["pessoas"] if p["cpf"] == ATIVO)
    assert p["duplicidade"] and any("possível duplicidade" in m for m in p["motivos"])
    assert "800000" in " ".join(p["motivos"])


def test_sem_a_aba_das_CARTEIRAS_vale_Producao_com_aviso(banco_dc, monkeypatch):
    from app.apps.analisesps import dc
    monkeypatch.setattr(dc, "_ler_aba", lambda nome: DATA if nome == dc.ABA_DATA
                        else (_ for _ in ()).throw(RuntimeError("sem aba")))
    dc._cache.clear()
    calculado = dc.calcular()
    assert all(p["carteira"] == "Produção" for p in calculado["pessoas"])
    assert any("Data base BeeVale" in a for a in calculado["avisos"])


def test_a_TELA_mostra_as_linhas_e_os_blocos_da_lateral(banco_dc, cliente_mestre):
    resposta = cliente_mestre.get("/analisesps/folha/dc")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Despesas com colaboradores" in html
    assert "ATIVO UM" in html and "card 900100" in html
    assert "Gerar arquivos" in html and "Planilha de cadastro" in html
    assert "Divisão por obra e conta" in html
    assert "Auxílio Alimentação" in html


def test_a_tela_filtra_por_TIPO_DE_DESPESA(banco_dc, cliente_mestre):
    html = cliente_mestre.get(
        "/analisesps/folha/dc?tipo_despesa=Di%C3%A1rias+de+Viagem").get_data(as_text=True)
    assert "card 900200" in html and "card 900100" not in html


def test_o_RELATORIO_da_DC_sai_em_pdf(banco_dc, cliente_mestre):
    resposta = cliente_mestre.get("/analisesps/folha/dc/relatorio.pdf")
    assert resposta.status_code == 200
    assert resposta.data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# NO PIPEFY: uma SP por conta, rateio por obra e por categoria, e os cards de
# origem marcados e movidos (o que o `geraspbeevale.gs` fazia).
# ---------------------------------------------------------------------------
def _gerar_dc(monkeypatch, destino="beevale"):
    from app.apps.analisesps import beevale, drive, folha_pagamento as fp
    subidos = []
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta, **k:
                        subidos.append(nome)
                        or {"id": f"d{len(subidos)}", "link": f"https://drive/{len(subidos)}"})
    saida = fp.gerar_direto("dc", {}, destino, quem="MARCELO")
    return next(a["id"] for a in saida["arquivos"] if a["destino"] == fp.ANALISE)


@pytest.fixture
def com_duas_categorias(banco_dc, monkeypatch):
    """Uma terceira linha, de outro tipo de despesa, na mesma conta."""
    from app.apps.analisesps import dc
    data = DATA[:-1] + [
        ["900300", "02/10/2026", "06/10/2026", "CREPEOLINDA", "Diárias de Viagem",
         "", "", "JOAO", "MARIA", "997.133.493-34", "", "50,00", "", "Viagem"]]
    original = dc._ler_aba
    monkeypatch.setattr(dc, "_ler_aba", lambda nome: data if nome == dc.ABA_DATA
                        else original(nome))
    dc._cache.clear()
    return banco_dc


def test_a_SP_da_DC_tem_RATEIO_POR_CATEGORIA_e_move_os_cards(com_duas_categorias,
                                                             monkeypatch):
    from app.apps.analisesps import dc, folha_cards as fcd, pipefy
    from tests.test_analisesps_folha_cards import PipefyFalso
    pipe = PipefyFalso(monkeypatch)
    movidos = []
    monkeypatch.setattr(pipefy, "mover_card", lambda card, fase, *a, **k:
                        movidos.append((str(card), int(fase))))
    analise = _gerar_dc(monkeypatch)

    vista = fcd.previa(analise)
    assert vista["bloqueios"] == []
    grupo = vista["grupos"][0]
    assert grupo["verba"] == "dc" and grupo["cards_de_origem"] == ["900100", "900300"]
    sp = grupo["sps"][0]
    assert sp["conta"] == "50024" and sp["valor"] == D("320.00")
    assert sp["tipo_sp"] == "R-ALI"                    # o da maior parte (270 × 50)
    assert "Mais de um tipo de despesa" in sp["observacao"]

    fcd.lancar(analise, quem="MARCELO")
    criada = pipe.do_pipe(fcd.PIPE_SP)
    assert len(criada) == 1
    valores = criada[0]["valores"]
    assert valores["valor"] == "320.00" and valores["tipo_de_despesa"] == "R-ALI"
    rateio = valores["rateio_m_ltiplo"]
    assert '"codigo_categoria":"2.01.01"' in rateio and '"codigo_categoria":"2.02.01"' in rateio
    assert '"cCodDep":"111"' in rateio
    descricao = valores["descri_o"]
    assert "IDs de origem: 900100, 900300" in descricao
    assert "Valor BeeVale (+1,50%): R$ 324,80" in descricao
    assert "Planilha de pagamento: https://drive/" in descricao
    assert "Relatório (PDF): https://drive/" in descricao
    assert pipe.atualizacoes_de(criada[0]["id"])["etiquetas"] == fcd.ETIQUETA_DA_DC

    # Os cards de origem: mover_card = Sim e a fase de processados.
    assert sorted(movidos) == [("900100", dc.FASE_PROCESSADO), ("900300", dc.FASE_PROCESSADO)]
    assert pipe.atualizacoes_de("900100") == {dc.CAMPO_MOVER: "Sim"}
    assert dc.lote_da_analise(analise)["cards_movidos"]

    with pytest.raises(fcd.ErroDosCards):
        fcd.lancar(analise)
    assert len(pipe.criados) == 1 and len(movidos) == 2


def test_se_o_Pipefy_recusa_MOVER_lancar_de_novo_so_termina_a_mudanca(banco_dc, monkeypatch):
    from app.apps.analisesps import dc, folha_cards as fcd, pipefy
    from tests.test_analisesps_folha_cards import PipefyFalso
    pipe = PipefyFalso(monkeypatch)
    movidos, recusar = [], {"vez": True}

    def mover(card, fase, *a, **k):
        if recusar["vez"]:
            raise pipefy.ErroDoPipefy("fase não permite")
        movidos.append(str(card))
    monkeypatch.setattr(pipefy, "mover_card", mover)
    analise = _gerar_dc(monkeypatch, destino="somapay")

    saida = fcd.lancar(analise, quem="MARCELO")
    assert saida["avisos"] and saida["cards_movidos"] == []
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 1
    assert "automa_o_2" not in pipe.do_pipe(fcd.PIPE_SP)[0]["valores"]   # só no BeeVale
    assert not dc.lote_da_analise(analise)["cards_movidos"]

    recusar["vez"] = False
    saida = fcd.lancar(analise, quem="MARCELO")
    assert saida["cards_movidos"] == ["900100"] and movidos == ["900100"]
    assert len(pipe.do_pipe(fcd.PIPE_SP)) == 1, "nenhuma SP a mais"
    assert dc.lote_da_analise(analise)["cards_movidos"]
