# ============================================================================
# ERP — core/suprimentos/mapa_pdf.py
# O mapa de cotação em PDF — o documento que a gestão analisa depois.
#
# O dono, em 20/11/2026, mandou o relatório do mapa 9228 ("é meio que um print
# da tela da planilha que a gente trabalha") e pediu: "se eu quiser gerar um
# mapa de cotação para ser analisado a posteriori pela gestão, eu quero ver
# como é que fica."
#
# O QUE EXISTIA era a exportação genérica da tela: uma tabela com o preço
# unitário de cada fornecedor e mais nada — sem as condições, sem os totais,
# sem os resumos. Menos do que a planilha entregava.
#
# ESTE SEGUE O MODELO DELE, página a página:
#   1. o mapa: cabeçalho de cada fornecedor (pagamento, entrega, frete,
#      desconto, total geral), preço unitário e subtotal lado a lado, o menor
#      preço da linha destacado, e as quatro contas do rodapé (melhor
#      fornecedor e melhor compra, sem e com frete);
#   2. resumo por item — de quem comprar cada coisa;
#   3. resumo por fornecedor — a lista de compra de cada um;
#   4. melhor fornecedor único — e se fechar tudo com um só;
#   5. comparativo dos fornecedores.
#
# Tudo sai de `cotacao.montar_mapa` e `cotacao.relatorio`, as mesmas funções da
# tela. Um PDF com conta própria um dia mostraria número diferente do mapa — e
# é o PDF que vai para a mesa de quem autoriza.
# ============================================================================
from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from app.apps.erp.core.suprimentos.pedido_pdf import (
    AZUL, CINZA, FUNDO, _br, _latin, _qtd,
)

logger = logging.getLogger(__name__)

VERDE = (226, 243, 232)
ROSA = (251, 236, 234)


def gerar(s: Session, cotacao_id: int, *, empresa_nome: str = "",
          quem: str = "") -> bytes:
    from app.apps.erp.core.suprimentos import cotacao as svc

    mapa = svc.montar_mapa(s, cotacao_id)
    relatorios = []
    for tipo in ("POR_ITEM", "POR_FORNECEDOR", "UM_FORNECEDOR", "COMPARATIVO"):
        try:
            relatorios.append(svc.relatorio(s, cotacao_id, tipo))
        except Exception:        # um resumo que não fecha não derruba o mapa
            logger.exception("ERP/suprimentos: resumo %s do mapa não pôde ser gerado", tipo)
    return desenhar(mapa, relatorios, empresa_nome=empresa_nome, quem=quem)


def desenhar(mapa: dict[str, Any], relatorios: list[dict[str, Any]], *,
             empresa_nome: str = "", quem: str = "") -> bytes:
    from fpdf import FPDF

    colunas = mapa["fornecedores"]
    # Até seis fornecedores cabem numa folha A4 deitada; acima disso, A3 —
    # espremer oito fornecedores em A4 dá um mapa que ninguém lê.
    formato = "A4" if len(colunas) <= 6 else "A3"
    pdf = FPDF(orientation="L", unit="mm", format=formato)
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.set_margins(10, 10, 10)
    agora = datetime.now()

    def fonte(estilo="", tamanho=7, cor=(20, 35, 59)):
        pdf.set_font("Helvetica", estilo, tamanho)
        pdf.set_text_color(*cor)

    def cabecalho_da_pagina(titulo: str):
        pdf.add_page()
        fonte("B", 13, AZUL)
        pdf.cell(0, 7, _latin(titulo), new_x="LMARGIN", new_y="NEXT")
        fonte("", 8, CINZA)
        obras = sorted({i["obra"] for i in mapa["itens"] if i.get("obra")})
        pdf.cell(0, 4.5, _latin(
            f"{mapa['numero']} · {mapa['titulo']}   |   Obras: {', '.join(obras) or '-'}"
            f"   |   {empresa_nome}   |   gerado em {agora:%d/%m/%Y %H:%M}"
            + (f" por {quem}" if quem else "")), new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(*AZUL)
        pdf.set_line_width(0.4)
        pdf.line(pdf.l_margin, pdf.get_y() + 1, pdf.w - pdf.r_margin, pdf.get_y() + 1)
        pdf.ln(3)

    largura = pdf.w - pdf.l_margin - pdf.r_margin

    # ------------------------------------------------------------- 1. o mapa
    cabecalho_da_pagina("Mapa de Cotação")
    fixas = [("Nº SS", 17, "L"), ("#", 6, "R"), ("Descrição", 66, "L"),
             ("Obra", 24, "L"), ("Qtd", 13, "R"), ("Und", 9, "C")]
    w_fixas = sum(f[1] for f in fixas)
    n = max(len(colunas), 1)
    w_par = min((largura - w_fixas) / n, 46)
    w_unit, w_sub = w_par * 0.47, w_par * 0.53
    tam = 6.5 if n <= 4 else 6

    def linha_do_fornecedor(rotulo, valor, estilo="", fundo=FUNDO):
        pdf.set_fill_color(*fundo)
        fonte("B", 6.5, CINZA)
        pdf.cell(w_fixas, 4.6, _latin(rotulo.upper() + "  "), align="R", fill=True, border=1)
        fonte(estilo, tam)
        for c in colunas:
            pdf.cell(w_par, 4.6, _latin(valor(c))[:int(w_par / 1.35)], align="C",
                     fill=True, border=1)
        pdf.ln(4.6)

    def cab_itens():
        pdf.set_fill_color(221, 229, 241)
        fonte("B", 6.5)
        for nome, w, a in fixas:
            pdf.cell(w, 4.8, _latin(nome), align=a, fill=True, border=1)
        for _ in colunas:
            pdf.cell(w_unit, 4.8, _latin("Preço unit."), align="R", fill=True, border=1)
            pdf.cell(w_sub, 4.8, "Subtotal", align="R", fill=True, border=1)
        pdf.ln(4.8)

    def cab_fornecedores():
        linha_do_fornecedor("Fornecedor", lambda c: c["fornecedor"], "B")
        linha_do_fornecedor("Pagamento", lambda c: c.get("condicao") or "-")
        linha_do_fornecedor("Entrega", lambda c: (
            "-" if not c.get("entrega") else
            "Coleta (FOB)" if c["entrega"] == "COLETA" else "Frete CIF"))
        linha_do_fornecedor("Frete · Desconto", lambda c: (
            f"{_br(c['frete']) if Decimal(c['frete']) > 0 else '-'} · "
            f"{_br(c['desconto']) if Decimal(c['desconto']) > 0 else '-'}"))
        linha_do_fornecedor("Total geral", lambda c: (
            _br(c["total"]) + ("  (não cotou tudo)"
                               if c["itens_cotados"] < c["itens_no_mapa"] else "")),
            "B", (227, 234, 245))
        cab_itens()

    cab_fornecedores()
    for numero, item in enumerate(mapa["itens"], start=1):
        descricao = item["insumo"] + (f" - {item['especificacao']}"
                                      if item.get("especificacao") else "")
        linhas_desc = pdf.multi_cell(fixas[2][1], 3.6, _latin(descricao),
                                     dry_run=True, output="LINES")
        altura = max(4.6, 3.6 * len(linhas_desc) + 1)
        if pdf.get_y() + altura > pdf.h - 14:
            cabecalho_da_pagina("Mapa de Cotação (continuação)")
            cab_fornecedores()
        y, x = pdf.get_y(), pdf.l_margin
        fonte("", tam)
        valores = [item.get("solicitacao") or "", str(numero), None,
                   item.get("obra") or "", _qtd(item["quantidade"]), item["unidade"]]
        for (nome, w, a), valor in zip(fixas, valores):
            pdf.set_xy(x, y)
            if valor is None:
                pdf.rect(x, y, w, altura)
                pdf.set_xy(x, y + 0.5)
                pdf.multi_cell(w, 3.6, _latin(descricao))
            else:
                pdf.cell(w, altura, _latin(valor)[:int(w / 1.2)], align=a, border=1)
            x += w
        for c in colunas:
            celula = (item.get("precos") or {}).get(c["id"])
            melhor = item.get("menor_preco_de") == c["id"]
            pdf.set_fill_color(*(VERDE if melhor else ROSA))
            pintar = melhor or celula is None
            pdf.set_xy(x, y)
            fonte("B" if melhor else "", tam)
            pdf.cell(w_unit, altura, _br(celula["preco_unitario"]) if celula else "",
                     align="R", border=1, fill=pintar)
            pdf.cell(w_sub, altura, _br(celula["total"]) if celula else "",
                     align="R", border=1, fill=pintar)
            x += w_par
        pdf.set_y(y + altura)

    # as quatro contas do rodapé
    por_id = {c["id"]: c for c in colunas}
    vencedores = {i["menor_preco_de"] for i in mapa["itens"] if i.get("menor_preco_de")}
    encargos = sum((Decimal(por_id[v]["frete"]) - Decimal(por_id[v]["desconto"])
                    for v in vencedores), Decimal("0"))
    unico = por_id.get(mapa.get("melhor_fornecedor_unico"))
    pulverizado = Decimal(str(mapa.get("total_pulverizado") or 0))
    sem_preco = sum(1 for i in mapa["itens"] if not i.get("menor_preco_de"))
    if pdf.get_y() > pdf.h - 34:
        cabecalho_da_pagina("Mapa de Cotação (resumo)")
    pdf.ln(3)
    contas = [
        ("Melhor fornecedor (sem frete)", _br(unico["soma_itens"]) if unico else "-",
         unico["fornecedor"] if unico else "ninguém cotou todos os itens"),
        ("Melhor fornecedor (com frete)", _br(unico["total"]) if unico else "-",
         "comprando tudo de um só"),
        ("Melhor compra (sem frete)", _br(pulverizado),
         f"cada item de quem tem o menor preço - {len(vencedores)} fornecedor(es)"),
        ("Melhor compra (com frete)", _br(pulverizado + encargos),
         "somando o frete de cada fornecedor envolvido"),
    ]
    w = largura / 4
    y = pdf.get_y()
    for i, (rotulo, valor, detalhe) in enumerate(contas):
        x = pdf.l_margin + i * w
        pdf.set_fill_color(*FUNDO)
        pdf.rect(x + 0.5, y, w - 1, 15, style="F")
        pdf.set_xy(x + 2, y + 1.5)
        fonte("B", 6.5, CINZA)
        pdf.cell(w - 4, 3.5, _latin(rotulo.upper()))
        pdf.set_xy(x + 2, y + 5)
        fonte("B", 11)
        pdf.cell(w - 4, 5, _latin("R$ " + valor if valor != "-" else "-"))
        pdf.set_xy(x + 2, y + 10.5)
        fonte("", 6.5, CINZA)
        pdf.cell(w - 4, 3.5, _latin(detalhe)[:70])
    pdf.set_y(y + 18)
    fonte("", 7, CINZA)
    pdf.multi_cell(0, 3.8, _latin(
        "Verde: menor preço da linha. Rosa: o fornecedor não cotou o item."
        + (f" {sem_preco} item(ns) sem preço de ninguém ficaram fora das contas."
           if sem_preco else "")
        + "   Aprovação: ______________________________   Data: ____/____/______"),
        new_x="LMARGIN", new_y="NEXT")

    # ------------------------------------------------- 2 a 5. os resumos
    for rel in relatorios:
        cabecalho_da_pagina(rel.get("titulo") or "Resumo")
        if rel.get("fornecedor"):
            fonte("B", 9)
            pdf.cell(0, 5, _latin(f"Fornecedor: {rel['fornecedor']}"
                                  + (f"   ·   {rel['condicao']}" if rel.get("condicao") else "")),
                     new_x="LMARGIN", new_y="NEXT")
        cols = [c.get("rotulo", "") for c in rel.get("colunas") or []]
        linhas = rel.get("linhas") or []
        if not cols:
            continue
        # largura pela maior célula de cada coluna, com teto — texto livre
        # (descrição, fornecedor) fica com o que sobrar.
        fonte("", 7.5)
        naturais = []
        for i, nome in enumerate(cols):
            maior = max([pdf.get_string_width(_latin(nome)) + 4] + [
                pdf.get_string_width(_latin(l[i] if i < len(l) else "")) + 4
                for l in linhas])
            naturais.append(min(maior, 95))
        escala = min(1.0, largura / sum(naturais))
        ws = [x * escala for x in naturais]
        sobra = largura - sum(ws)
        if sobra > 0:
            ws[naturais.index(max(naturais))] += sobra
        numericas = {i for i, c in enumerate(rel.get("colunas") or [])
                     if c.get("tipo") in ("dinheiro", "inteiro", "quantidade")}
        if rel.get("tipo") != "COMPARATIVO":
            numericas |= {2, 7, 8}

        def cab():
            pdf.set_fill_color(221, 229, 241)
            fonte("B", 7.5)
            for i, nome in enumerate(cols):
                pdf.cell(ws[i], 5.2, _latin(nome), border=1, fill=True,
                         align="R" if i in numericas else "L")
            pdf.ln(5.2)

        cab()
        for k, l in enumerate(linhas):
            if pdf.get_y() > pdf.h - 16:
                cabecalho_da_pagina((rel.get("titulo") or "Resumo") + " (continuação)")
                cab()
            pdf.set_fill_color(250, 251, 253)
            fonte("", 7.5)
            for i in range(len(cols)):
                texto = _latin(l[i] if i < len(l) else "")
                while texto and pdf.get_string_width(texto) > ws[i] - 2:
                    texto = texto[:-2]
                pdf.cell(ws[i], 4.8, texto, border="B", fill=bool(k % 2),
                         align="R" if i in numericas else "L")
            pdf.ln(4.8)
        pdf.ln(2)
        fonte("B", 9.5)
        total = rel.get("total_br") or rel.get("total") or rel.get("total_pulverizado_br")
        if total:
            pdf.cell(0, 6, _latin(f"Total: R$ {total}"), align="R",
                     new_x="LMARGIN", new_y="NEXT")
        for agr in rel.get("resumo_por_fornecedor") or []:
            fonte("", 8)
            pdf.cell(0, 4.4, _latin(f"{agr['fornecedor']}: {agr['itens']} item(ns) · "
                                    f"R$ {agr['total']}"), align="R",
                     new_x="LMARGIN", new_y="NEXT")
        if rel.get("aviso"):
            fonte("I", 7.5, CINZA)
            pdf.multi_cell(0, 4, _latin(rel["aviso"]), new_x="LMARGIN", new_y="NEXT")
    return bytes(pdf.output())
