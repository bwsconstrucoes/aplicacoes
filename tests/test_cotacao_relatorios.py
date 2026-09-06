"""Os quatro relatórios do mapa — os mesmos das abas R2 a R5 da planilha.

Cada um responde uma pergunta diferente, e o que estes testes seguram é que
eles NÃO SE CONTRADIZEM. Todos saem do mesmo mapa; se um dia dois deles
derem números diferentes para a mesma cotação, nenhum serve para decidir.

O que não pode falhar:
  - "por item" e "por fornecedor" mostram os MESMOS itens e o MESMO total —
    muda só a ordem;
  - o menor preço escolhido é mesmo o menor;
  - "comprando tudo de um" avisa quantos itens aquele fornecedor NÃO cotou;
  - o comparativo soma frete e desconto, que é o que decide a compra de
    verdade — comparar só o preço unitário engana.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import cotacao as svc

# Um mapa de mentira, no formato exato que `montar_mapa` devolve. Assim os
# relatórios são testados sozinhos, sem depender de banco.
MAPA = {
    "id": 1, "numero": "COT-0001", "titulo": "Rede de esgoto", "status": "ABERTA",
    "itens": [
        {"id": 10, "numero": 1, "insumo": "Tubo PVC 100mm", "especificacao": "barra 6m",
         "quantidade": "76", "unidade": "UN", "obra": "ESCPE18",
         "status_rotulo": "Cotação",
         "precos": {100: {"preco_unitario": "54.68", "total": "4155.68"},
                    200: {"preco_unitario": "58.00", "total": "4408.00"}},
         "menor_preco_de": 100},
        {"id": 11, "numero": 2, "insumo": "Joelho 45°", "especificacao": None,
         "quantidade": "56", "unidade": "UN", "obra": "ESCPE18",
         "status_rotulo": "Cotação",
         "precos": {100: {"preco_unitario": "1.30", "total": "72.80"},
                    200: {"preco_unitario": "1.10", "total": "61.60"}},
         "menor_preco_de": 200},
        {"id": 12, "numero": 3, "insumo": "Grelha inox", "especificacao": None,
         "quantidade": "12", "unidade": "UN", "obra": "CREPETERRA",
         "status_rotulo": "Cotação",
         "precos": {200: {"preco_unitario": "8.24", "total": "98.88"}},
         "menor_preco_de": 200},
    ],
    "fornecedores": [
        {"id": 100, "fornecedor_id": 1, "fornecedor": "KRONA TUBOS SA",
         "condicao": "28/56 dias", "entrega": "ENTREGA", "frete": "250.00",
         "desconto": "0", "acrescimo_percentual": "0", "itens_cotados": 2,
         "itens_no_mapa": 3, "soma_itens": "4228.48", "total": "4478.48"},
        {"id": 200, "fornecedor_id": 2, "fornecedor": "LOJAO DO FERRO LTDA",
         "condicao": "à vista", "entrega": "COLETA", "frete": "0",
         "desconto": "100.00", "acrescimo_percentual": "0", "itens_cotados": 3,
         "itens_no_mapa": 3, "soma_itens": "4568.48", "total": "4468.48"},
    ],
    "melhor_fornecedor_unico": 200,
    "total_pulverizado": "4316.16",
}


@pytest.fixture
def sem_banco(monkeypatch):
    monkeypatch.setattr(svc, "montar_mapa", lambda s, cid: MAPA)


def pegar(tipo, **extra):
    return svc.relatorio(None, 1, tipo, **extra)


# ---------------------------------------------------------------------------
# Os quatro existem e se identificam
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tipo", sorted(svc.TIPOS_DE_RELATORIO))
def test_todo_relatorio_diz_de_qual_cotacao_e_com_quais_filtros(tipo, sem_banco):
    r = pegar(tipo)
    assert r["cotacao"] == "COT-0001"
    assert "COT-0001" in r["subtitulo"]
    assert any("Obras" in f for f in r["filtros"]), \
        "sem os filtros no alto, o relatório não diz de onde veio"
    assert r["linhas"] and r["colunas"]


def test_relatorio_desconhecido_e_recusado(sem_banco):
    with pytest.raises(ErroValidacao, match="desconhecido"):
        pegar("QUALQUER_COISA")


# ---------------------------------------------------------------------------
# R2 e R3 — por item e por fornecedor
# ---------------------------------------------------------------------------
def test_por_item_escolhe_mesmo_o_menor_preco(sem_banco):
    r = pegar("POR_ITEM")
    por_descricao = {l[1]: l for l in r["linhas"]}
    assert por_descricao["Tubo PVC 100mm — barra 6m"][6] == "KRONA TUBOS SA"
    assert por_descricao["Tubo PVC 100mm — barra 6m"][7] == "54,68", \
        "o relatório é lido por gente: vírgula decimal, não ponto"
    assert por_descricao["Joelho 45°"][6] == "LOJAO DO FERRO LTDA", \
        "1,10 é menor que 1,30 — tem de ser o Lojão"


def test_por_item_e_por_fornecedor_somam_o_mesmo(sem_banco):
    """Mudam a ordem, não a conta. Se divergirem, nenhum dos dois serve."""
    a, b = pegar("POR_ITEM"), pegar("POR_FORNECEDOR")
    assert a["total"] == b["total"]
    assert len(a["linhas"]) == len(b["linhas"])
    assert sorted(l[1] for l in a["linhas"]) == sorted(l[1] for l in b["linhas"])


def test_por_fornecedor_agrupa_de_verdade(sem_banco):
    """É a lista de compra: "do A levo estes, do B estes". Intercalado, não
    serve para mandar o pedido."""
    nomes = [l[6] for l in pegar("POR_FORNECEDOR")["linhas"]]
    assert nomes == sorted(nomes), "as linhas do mesmo fornecedor têm de ficar juntas"


def test_o_total_do_menor_preco_e_a_soma_das_linhas(sem_banco):
    r = pegar("POR_ITEM")
    # 4155,68 (Krona) + 61,60 + 98,88 (Lojão)
    assert Decimal(r["total"]) == Decimal("4316.16")


def test_a_lista_de_compra_sai_pronta_por_fornecedor(sem_banco):
    r = pegar("POR_FORNECEDOR")
    por_nome = {x["fornecedor"]: x for x in r["resumo_por_fornecedor"]}
    assert por_nome["KRONA TUBOS SA"]["itens"] == 1
    assert por_nome["LOJAO DO FERRO LTDA"]["itens"] == 2


def test_o_relatorio_avisa_que_o_frete_nao_esta_somado(sem_banco):
    """Comprar cada item de quem tem o menor preço ignora que cada fornecedor
    cobra o SEU frete. Sem esse aviso, o número engana."""
    assert "frete" in pegar("POR_ITEM")["aviso"].lower()


def test_item_que_ninguem_cotou_aparece_e_e_contado(sem_banco):
    mapa = {**MAPA, "itens": MAPA["itens"] + [
        {"id": 13, "numero": 4, "insumo": "Caixa sifonada", "especificacao": None,
         "quantidade": "2", "unidade": "UN", "obra": "ESCPE18",
         "status_rotulo": "Cotação", "precos": {}, "menor_preco_de": None}]}
    import app.apps.erp.core.suprimentos.cotacao as mod
    guardado = mod.montar_mapa
    mod.montar_mapa = lambda s, cid: mapa
    try:
        r = svc.relatorio(None, 1, "POR_ITEM")
        assert r["sem_preco"] == 1
        assert any(l[6] == "não cotado" for l in r["linhas"]), \
            "sumir com o item faria o comprador esquecer de cotá-lo"
    finally:
        mod.montar_mapa = guardado


# ---------------------------------------------------------------------------
# R4 — comprando tudo de um
# ---------------------------------------------------------------------------
def test_sem_escolha_usa_quem_sai_melhor_cotando_tudo(sem_banco):
    r = pegar("UM_FORNECEDOR")
    assert r["fornecedor"] == "LOJAO DO FERRO LTDA"
    assert r["itens_sem_preco"] == 0
    assert "todos os itens" in r["aviso"]


def test_da_para_pedir_outro_fornecedor(sem_banco):
    r = pegar("UM_FORNECEDOR", cotacao_fornecedor_id=100)
    assert r["fornecedor"] == "KRONA TUBOS SA"
    assert r["itens_sem_preco"] == 1, "a Krona não cotou a grelha"
    assert "ficam de fora" in r["aviso"], \
        "comprar tudo de quem não cotou tudo deixa item de fora — tem de avisar"


def test_o_total_de_um_fornecedor_e_o_do_mapa_com_encargos(sem_banco):
    """O total tem de ser o MESMO que o mapa mostra no rodapé — senão a tela e
    o relatório discordam na frente do comprador."""
    r = pegar("UM_FORNECEDOR", cotacao_fornecedor_id=100)
    assert r["total"] == "4.478,48"


def test_a_troca_de_fornecedor_vem_pronta_na_resposta(sem_banco):
    r = pegar("UM_FORNECEDOR")
    assert {e["fornecedor"] for e in r["escolhas"]} == {
        "KRONA TUBOS SA", "LOJAO DO FERRO LTDA"}


# ---------------------------------------------------------------------------
# R5 — comparativo
# ---------------------------------------------------------------------------
def test_o_comparativo_ordena_do_mais_barato_para_o_mais_caro(sem_banco):
    linhas = pegar("COMPARATIVO")["linhas"]
    assert linhas[0][1] == "LOJAO DO FERRO LTDA", "4.468,48 é menor que 4.478,48"


def test_o_comparativo_mostra_frete_e_desconto(sem_banco):
    """É a conta que decide: o mais barato por item pode sair mais caro no
    total por causa do frete."""
    linhas = pegar("COMPARATIVO")["linhas"]
    por_nome = {l[1]: l for l in linhas}
    assert por_nome["KRONA TUBOS SA"][6] == "250,00"
    assert por_nome["LOJAO DO FERRO LTDA"][7] == "100,00"


def test_o_comparativo_diz_quantos_itens_cada_um_cotou(sem_banco):
    """Quem cotou menos aparece com total menor por isso, não por ser mais
    barato. Sem essa coluna, a comparação mente."""
    por_nome = {l[1]: l for l in pegar("COMPARATIVO")["linhas"]}
    assert por_nome["KRONA TUBOS SA"][4] == "2/3"
    assert "itens cotados" in pegar("COMPARATIVO")["aviso"]


def test_o_comparativo_traz_o_total_pulverizado_para_comparar(sem_banco):
    """"Comprando de cada um o mais barato daria X" é o número que o comprador
    põe ao lado do total de um fornecedor só."""
    assert pegar("COMPARATIVO")["total_pulverizado"] == "4316.16"


# ---------------------------------------------------------------------------
# Os números são lidos por gente
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto,esperado", [
    ("35.9000", "35,90"), ("4155.68", "4.155,68"), ("0.1250", "0,125"),
    ("0", "0,00"), ("1234567.5", "1.234.567,50"), ("", ""), (None, "")])
def test_dinheiro_sai_em_portugues(bruto, esperado):
    """"35.9000" e "4155.68" não se lê em português — e o relatório vai para o
    papel, para a mão do comprador."""
    assert svc._dinheiro_br(bruto) == esperado


@pytest.mark.parametrize("bruto,esperado", [
    ("14.000", "14"), ("1.000", "1"), ("37.500", "37,5"), ("", ""), (None, "")])
def test_quantidade_nao_finge_ser_milhar(bruto, esperado):
    """"14.000" no banco é catorze. Em português isso se lê como catorze mil —
    e aí a quantidade do relatório mente por mil vezes."""
    assert svc._quantidade_br(bruto) == esperado


def test_a_soma_por_fornecedor_nao_sai_do_texto_formatado(sem_banco):
    """Somar "4.155,68" como texto é como se erra por mil. A soma sai do mapa,
    não da linha já formatada."""
    r = pegar("POR_FORNECEDOR")
    por_nome = {x["fornecedor"]: x for x in r["resumo_por_fornecedor"]}
    assert por_nome["KRONA TUBOS SA"]["total"] == "4.155,68"
    assert por_nome["LOJAO DO FERRO LTDA"]["total"] == "160,48"
