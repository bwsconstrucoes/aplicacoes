# -*- coding: utf-8 -*-
"""
Os cards do Pipefy da folha — 27/09/2026.

⚠️ O QUE ESTES TESTES PROTEGEM. Card criado no Pipefy **não se apaga** pelo
sistema, e o risco mais silencioso não é criar card errado: é criar card com valor
no campo do vizinho. O blueprint do Make faz isso hoje (o par 62 grava em
`valor_centro_de_custo_63` — `docs/FOLHA_DE_PAGAMENTO.md` §4), e um valor de centro
de custo trocado só aparece no fechamento da obra, meses depois.

Daí o desenho testado aqui: **os campos são lidos do pipe**, e campo que não foi
reconhecido fica **vazio e dito**, nunca preenchido por parecença.
"""
import pytest

from app.apps.analisesps import folha_cards as fcd


def _campos(*pares):
    return {"id": "301433085", "nome": "Despesa com Colaboradores",
            "campos": {cid: {"label": label, "tipo": "short_text", "opcoes": []}
                       for cid, label in pares},
            "fases": [{"id": "1", "nome": "Entrada"}]}


# ---------------------------------------------------------------------------
# CONFERIR O PIPE — o passo que não cria nada
# ---------------------------------------------------------------------------
def test_conferir_o_pipe_diz_o_que_reconheceu(monkeypatch):
    from app.apps.analisesps import pipefy

    monkeypatch.setattr(pipefy, "campos_do_pipe", lambda *a, **k: _campos(
        ("descri_o", "Descrição"), ("valor", "Valor"),
        ("tipo_de_despesa", "Tipo de Despesa"),
        ("planilha_de_pagamento", "Planilha de Pagamento"),
        ("planilha_de_an_lise", "Planilha de Análise"),
        ("centro_de_custo_1", "Centro de Custo 1"),
        ("valor_centro_de_custo_1", "Valor Centro de Custo 1")))

    saida = fcd.conferir_pipe()
    assert saida["nome"] == "Despesa com Colaboradores"
    assert saida["reconhecidos"]["descricao"]["id"] == "descri_o"
    assert saida["reconhecidos"]["link_pagamento"]["id"] == "planilha_de_pagamento"
    assert saida["reconhecidos"]["link_analise"]["id"] == "planilha_de_an_lise"
    assert saida["nao_encontrados"] == []
    assert saida["centros_de_custo"] == ["centro_de_custo_1",
                                         "valor_centro_de_custo_1"]


def test_campo_que_nao_existe_fica_DITO_e_nao_adivinhado(monkeypatch):
    """⚠️ Campo parecido é pior que campo vazio: vazio alguém vê e preenche;
    errado ninguém vê."""
    from app.apps.analisesps import pipefy

    monkeypatch.setattr(pipefy, "campos_do_pipe", lambda *a, **k: _campos(
        ("descri_o", "Descrição"), ("valor", "Valor")))

    saida = fcd.conferir_pipe()
    assert sorted(saida["nao_encontrados"]) == ["link_analise",
                                               "link_pagamento",
                                               "tipo_de_despesa"]


def test_conferir_o_pipe_devolve_a_frase_do_erro(monkeypatch):
    from app.apps.analisesps import pipefy

    def explode(*a, **k):
        raise pipefy.ErroDoPipefy("falta o PIPEFY_TOKEN.")

    monkeypatch.setattr(pipefy, "campos_do_pipe", explode)
    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.conferir_pipe()
    assert "PIPEFY_TOKEN" in str(erro.value)


# ---------------------------------------------------------------------------
# A DESCRIÇÃO — ela é a prova
# ---------------------------------------------------------------------------
def test_a_descricao_do_card_diz_TUDO_o_que_alguem_vai_querer_saber():
    """Quem abrir o card meses depois precisa saber de que competência é, que
    verbas entraram, de qual conta saiu e onde está o arquivo."""
    texto = fcd.descricao_do_card(
        "09/2026", "quinzena", ["alimentacao", "transporte"], "50024", 12,
        "4200.00", "https://drive/pag", "https://drive/ana")

    assert "Competência: 09/2026" in texto
    assert "Quinzena (dias 1 a 15)" in texto
    assert "Alimentação + Transporte" in texto
    assert "Conta de pagamento: 50024" in texto
    assert "Pessoas: 12" in texto
    assert "R$ 4.200,00" in texto, "o card é lido por gente, com vírgula"
    assert "https://drive/pag" in texto
    assert "https://drive/ana" in texto


def test_a_descricao_nao_deixa_linha_vazia_de_campo_que_nao_se_aplica():
    """"Conta de pagamento:" sozinha faria parecer que faltou preencher."""
    texto = fcd.descricao_do_card("09/2026", "fim_de_mes", ["folha"], "", 3,
                                  "10.00")
    assert "Conta de pagamento" not in texto
    assert "Planilha de pagamento" not in texto
    assert texto.endswith("Gerado pelo Análise de SPs.")


# ---------------------------------------------------------------------------
# LANÇAR — a chamada sem volta
# ---------------------------------------------------------------------------
def _log_com(arquivo):
    return [arquivo]


def _arquivo(**mudancas):
    from decimal import Decimal as D
    base = {"id": 7, "ano": 2026, "mes": 9, "tipo": "quinzena",
            "destino": "beevale", "verbas": "alimentacao", "conta": "50024",
            "nome": "BeeVale.xlsx", "pessoas": 12, "total": D("4200.00"),
            "link": "https://drive/pag", "card_pipefy": "", "link_card": "",
            "avisos": "", "criado_em": None, "criado_por": "MARCELO",
            "competencia": "09/2026"}
    base.update(mudancas)
    return base


def _preparar(monkeypatch, arquivos, campos=None):
    from app.apps.analisesps import folha_pagamento as fpg, pipefy
    criados = {}

    monkeypatch.setattr(fpg, "log", lambda *a, **k: list(arquivos))
    monkeypatch.setattr(fpg, "registrar_card",
                        lambda *a, **k: criados.setdefault("amarrou", a) or True)
    monkeypatch.setattr(pipefy, "campos_do_pipe", lambda *a, **k: campos
                        if campos is not None else _campos(
                            ("descri_o", "Descrição"), ("valor", "Valor"),
                            ("planilha_de_pagamento", "Planilha de Pagamento"),
                            ("planilha_de_an_lise", "Planilha de Análise")))
    monkeypatch.setattr(pipefy, "criar_card", lambda pipe, titulo, valores,
                        **k: criados.update({"titulo": titulo,
                                             "valores": valores}) or
                        {"id": "9999", "titulo": titulo,
                         "link": "https://app.pipefy.com/open-cards/9999"})
    return criados


def test_lancar_cria_o_card_e_amarra_ao_log(monkeypatch):
    criados = _preparar(monkeypatch, [
        _arquivo(),
        _arquivo(id=8, destino="analise", conta="",
                 link="https://drive/ana")])

    saida = fcd.lancar(7, quem="MARCELO")
    assert saida["card"] == "9999"
    assert saida["link"].endswith("/9999")
    assert criados["amarrou"][0] == 7, "amarrou o card ao arquivo do log"
    assert "conta 50024" in criados["titulo"]

    # ⚠️ O LINK DA ANÁLISE ENTRA NO CARD DO PAGAMENTO: são os dois arquivos que
    # ele pediu, e o card é onde a equipe os encontra.
    valores = {v["campo"]: v["valor"] for v in criados["valores"]}
    assert valores["planilha_de_pagamento"] == "https://drive/pag"
    assert valores["planilha_de_an_lise"] == "https://drive/ana"
    assert "09/2026" in valores["descri_o"]


def test_lancar_NAO_preenche_campo_que_nao_reconheceu(monkeypatch):
    criados = _preparar(monkeypatch, [_arquivo()],
                        campos=_campos(("descri_o", "Descrição")))

    saida = fcd.lancar(7, quem="MARCELO")
    assert [v["campo"] for v in criados["valores"]] == ["descri_o"]
    assert "link_pagamento" in saida["sem_mapeamento"]
    # E o que ficou de fora está na DESCRIÇÃO, que é o campo que sempre existe.
    assert "https://drive/pag" in saida["descricao"]


def test_sem_NENHUM_campo_reconhecido_nao_cria_card(monkeypatch):
    """É melhor não lançar do que lançar um card vazio: alguém confiaria nele."""
    criados = _preparar(monkeypatch, [_arquivo()],
                        campos=_campos(("outra_coisa", "Outra coisa")))

    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(7)
    assert "não criei card" in str(erro.value)
    assert "valores" not in criados


def test_o_arquivo_de_ANALISE_nao_tem_card_proprio(monkeypatch):
    """Ele vai como link DENTRO do card do pagamento."""
    _preparar(monkeypatch, [_arquivo(id=8, destino="analise")])
    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(8)
    assert "DENTRO do card" in str(erro.value)


def test_lancar_duas_vezes_o_MESMO_arquivo_e_recusado(monkeypatch):
    """Regerar é livre (decisão dele), mas lançar o mesmo arquivo duas vezes é
    distração — e o cancelamento do card é feito no Pipefy, por ele."""
    _preparar(monkeypatch, [_arquivo(card_pipefy="1234")])
    with pytest.raises(fcd.ErroDosCards) as erro:
        fcd.lancar(7)
    assert "1234" in str(erro.value)
    assert "Cancele o card no Pipefy" in str(erro.value)


def test_arquivo_que_nao_esta_no_log_nao_lanca(monkeypatch):
    _preparar(monkeypatch, [])
    with pytest.raises(fcd.ErroDosCards):
        fcd.lancar(7)


def test_ja_lancado_AVISA_em_vez_de_impedir(monkeypatch):
    """Decisão dele (D15): impedir seria eu decidindo no lugar dele."""
    from app.apps.analisesps import folha_pagamento as fpg

    monkeypatch.setattr(fpg, "log", lambda *a, **k: [
        _arquivo(card_pipefy="1234"), _arquivo(id=9, card_pipefy="")])
    assert [a["id"] for a in fcd.ja_lancado(2026, 9, "quinzena")] == [7]
    assert fcd.ja_lancado(2026, 9, "fim_de_mes") == []


# ---------------------------------------------------------------------------
# ⚠️ O CAMPO PARECIDO — o defeito que este bloco existe para impedir
# ---------------------------------------------------------------------------
def test_o_rotulo_EXATO_ganha_do_parecido(monkeypatch):
    """⚠️ O pipe tem um campo "Valor" e setenta e cinco "Valor Centro de Custo N".
    Procurando só por conter "valor", o TOTAL da despesa poderia ser escrito dentro
    do valor de um centro de custo — e valor de centro de custo trocado só aparece
    no fechamento da obra, meses depois. É exatamente o defeito que o blueprint do
    Make tem hoje."""
    from app.apps.analisesps import pipefy

    # De propósito com os campos de centro de custo ANTES, que é a ordem que fazia
    # a busca antiga errar.
    monkeypatch.setattr(pipefy, "campos_do_pipe", lambda *a, **k: _campos(
        ("valor_centro_de_custo_1", "Valor Centro de Custo 1"),
        ("valor_centro_de_custo_2", "Valor Centro de Custo 2"),
        ("valor", "Valor"),
        ("descri_o", "Descrição")))

    saida = fcd.conferir_pipe()
    assert saida["reconhecidos"]["valor"]["id"] == "valor"


def test_sem_o_campo_exato_a_busca_FOGE_do_centro_de_custo(monkeypatch):
    """Se o campo "Valor" não existir com esse nome, é melhor ficar sem do que cair
    num "Valor Centro de Custo"."""
    from app.apps.analisesps import pipefy

    monkeypatch.setattr(pipefy, "campos_do_pipe", lambda *a, **k: _campos(
        ("valor_centro_de_custo_1", "Valor Centro de Custo 1"),
        ("descri_o", "Descrição")))

    saida = fcd.conferir_pipe()
    assert "valor" in saida["nao_encontrados"]


def test_achar_campo_sem_pedaco_nenhum_devolve_vazio():
    """Chamada torta não pode virar "o primeiro campo que aparecer"."""
    from app.apps.analisesps import pipefy
    assert pipefy.achar_campo({"a": {"label": "Qualquer"}}) == ""
    assert pipefy.achar_campo({}, "valor") == ""
