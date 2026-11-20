# ============================================================================
# ERP — core/suprimentos/pedido_pdf.py
# O pedido de compra em PDF — o documento que o fornecedor recebe.
#
# O dono, em 20/11/2026: "o pedido em PDF, sim, eu acho que é em PDF mesmo, a
# gente vai encaminhar por e-mail."
#
# O PONTO DE PARTIDA é o modelo que a equipe já manda ("Aquisição de
# Suprimentos", pedido 19457): logo, número, CNPJ de faturamento, tabela com o
# número da solicitação, obra e endereço, totais, fornecedor, pagamento,
# previsão, instruções da nota e a assinatura de quem comprou.
#
# O QUE ESTE TEM A MAIS, e por quê:
#   · UM BLOCO POR LOCAL DE ENTREGA, com subtotal — o modelo listava tudo
#     junto e o motorista separava de cabeça;
#   · o aviso quando o material NÃO desce na obra (sede, depósito);
#   · AS PARCELAS COM DATA E VALOR — o modelo dizia "30 dias"; aqui o
#     fornecedor vê quando recebe, e o financeiro dele não liga para perguntar;
#   · a especificação de cada item em linha própria, que é o que evita vir a
#     variação errada do material.
#
# Tudo sai dos MESMOS dados do texto do e-mail (`envio.montar_pedido`): o
# endereço vem de `entrega.py`, o total de `pedido.total`. Dois documentos com
# contas próprias divergiriam no primeiro pedido com desconto.
# ============================================================================
from __future__ import annotations

import io
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao

logger = logging.getLogger(__name__)

AZUL = (11, 44, 92)
CINZA = (90, 103, 129)
FUNDO = (238, 242, 248)
VERMELHO = (163, 32, 32)


def _latin(t: Any) -> str:
    """A fonte embutida do PDF é latin-1: o que não cabe vira o equivalente."""
    s = str(t if t is not None else "")
    for de, para in (("—", "-"), ("–", "-"), ("“", '"'), ("”", '"'), ("‘", "'"),
                     ("’", "'"), ("•", "-"), ("…", "..."), (" ", " ")):
        s = s.replace(de, para)
    return s.encode("latin-1", "replace").decode("latin-1")


def _br(valor: Any) -> str:
    d = Decimal(str(valor or 0)).quantize(Decimal("0.01"))
    inteiro, _, centavos = f"{d:,.2f}".partition(".")
    return inteiro.replace(",", ".") + "," + centavos


def _qtd(valor: Any) -> str:
    d = Decimal(str(valor or 0)).normalize()
    texto = f"{d:f}"
    inteiro, _, fracao = texto.partition(".")
    grupos = f"{int(inteiro):,}".replace(",", ".")
    return grupos + ("," + fracao if fracao else "")


def dados(s: Session, pedido_id: int, empresa, usuario=None) -> dict[str, Any]:
    """O pedido em estrutura — a mesma que o PDF e a tela de prévia leem."""
    from app.apps.erp.core.comum.formato import documento_por_extenso
    from app.apps.erp.core.suprimentos import pedido as svc_pedido
    from app.apps.erp.core.suprimentos.entrega import (
        desvios_do_pedido, endereco_da_empresa, local_de_entrega, rotulo_da_obra)
    from app.apps.erp.core.suprimentos.pagamento import descrever
    from app.apps.erp.db.models.cadastros import (
        CondicaoPagamento, Fornecedor, FornecedorContato, Insumo, ModoEntrega, Obra,
        PedidoCompra, PedidoItem, PrevisaoPagamento, SuprimentoItem,
        SuprimentoSolicitacao)

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    forn = s.get(Fornecedor, pedido.fornecedor_id)
    cond = (s.get(CondicaoPagamento, pedido.condicao_pagamento_id)
            if pedido.condicao_pagamento_id else None)
    contato = (s.get(FornecedorContato, pedido.contato_id)
               if pedido.contato_id else None)
    linhas = sorted([x for x in s.scalars(select(PedidoItem).where(
        PedidoItem.pedido_id == pedido_id)).all() if x.pedido_id == pedido_id],
        key=lambda x: x.numero or 0)
    if not linhas:
        raise ErroValidacao("Este pedido não tem item nenhum.")

    desvios = desvios_do_pedido(s, pedido_id)
    blocos: dict[str, dict[str, Any]] = {}
    numero = 0
    soma = Decimal("0")
    for linha in linhas:
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        obra = s.get(Obra, item.obra_id) if item is not None and item.obra_id else None
        sol = (s.get(SuprimentoSolicitacao, item.solicitacao_id)
               if item is not None else None)
        local = local_de_entrega(obra, desvios.get(getattr(item, "obra_id", None)))
        bloco = blocos.setdefault(local["endereco"], {
            "endereco": local["endereco"], "na_obra": local["na_obra"],
            "local": local["rotulo"], "mapa": local["mapa"], "obras": [],
            "itens": [], "subtotal": Decimal("0")})
        rotulo = rotulo_da_obra(obra)
        if rotulo and rotulo not in bloco["obras"]:
            bloco["obras"].append(rotulo)
        numero += 1
        total_linha = (Decimal(str(linha.quantidade or 0))
                       * Decimal(str(linha.preco_unitario or 0)))
        soma += total_linha
        bloco["subtotal"] += total_linha
        bloco["itens"].append({
            "numero": numero, "solicitacao": getattr(sol, "numero", "") or "",
            "insumo": getattr(insumo, "descricao", "") or "item",
            "especificacao": (getattr(item, "especificacao", "") or "").strip(),
            "unidade": getattr(item, "unidade", "") or "",
            "quantidade": linha.quantidade, "preco_unitario": linha.preco_unitario,
            "total": total_linha})

    previsoes = sorted([p for p in s.scalars(select(PrevisaoPagamento).where(
        PrevisaoPagamento.pedido_id == pedido_id)).all() if p.pedido_id == pedido_id],
        key=lambda p: (p.vencimento, p.numero or 0))
    em_palavras = descrever(cond.entrada_percentual, cond.dias) if cond else ""
    return {
        "numero": pedido.numero,
        "emitido_em": (pedido.autorizado_em or pedido.criado_em).date()
                      if (pedido.autorizado_em or pedido.criado_em) else date.today(),
        "empresa": {"razao_social": empresa.razao_social,
                    "fantasia": empresa.nome_fantasia or empresa.razao_social,
                    "cnpj": documento_por_extenso(empresa.cnpj) or empresa.cnpj,
                    "endereco": endereco_da_empresa(empresa),
                    "telefone": empresa.telefone or "", "email": empresa.email or "",
                    "logo": bytes(empresa.logo) if getattr(empresa, "logo", None) else None},
        "fornecedor": {"razao_social": getattr(forn, "razao_social", ""),
                       "documento": documento_por_extenso(
                           getattr(forn, "cnpj_cpf", "")) or "",
                       "contato": getattr(contato, "nome", "") or ""},
        "blocos": list(blocos.values()),
        "subtotal": soma, "frete": Decimal(str(pedido.frete or 0)),
        "desconto": Decimal(str(pedido.desconto or 0)),
        "total": svc_pedido.total(s, pedido),
        "condicao": getattr(cond, "nome", "") or "a combinar",
        "condicao_em_palavras": em_palavras,
        "antecipado": bool(pedido.antecipado),
        "parcelas": [{"numero": p.numero, "vencimento": p.vencimento,
                      "valor": p.valor, "entrada": bool(p.entrada)} for p in previsoes],
        "entrega": (None if pedido.entrega is None else
                    "Coleta por nossa conta (FOB)" if pedido.entrega is ModoEntrega.COLETA
                    else "Entrega por conta do fornecedor (CIF)"),
        "previsao_entrega": pedido.previsao_entrega,
        "observacoes": (pedido.observacoes or "").strip(),
        "comprador": {"nome": getattr(usuario, "nome", "") or "",
                      "email": getattr(usuario, "email", "") or "",
                      "telefone": getattr(usuario, "telefone", "") or ""},
    }


def gerar(s: Session, pedido_id: int, empresa, usuario=None) -> bytes:
    """O PDF do pedido, pronto para anexar no e-mail."""
    return desenhar(dados(s, pedido_id, empresa, usuario))


def desenhar(d: dict[str, Any]) -> bytes:
    from fpdf import FPDF

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=16)
    pdf.set_margins(14, 14, 14)
    pdf.add_page()
    largura = pdf.w - pdf.l_margin - pdf.r_margin

    def fonte(estilo="", tamanho=9, cor=(20, 35, 59)):
        pdf.set_font("Helvetica", estilo, tamanho)
        pdf.set_text_color(*cor)

    def linha(texto, altura=4.6, **kw):
        pdf.multi_cell(0, altura, _latin(texto), new_x="LMARGIN", new_y="NEXT", **kw)

    # ---- cabeçalho: logo (ou o nome), título e número
    topo = pdf.get_y()
    emp = d["empresa"]
    if emp["logo"]:
        try:
            pdf.image(io.BytesIO(emp["logo"]), x=pdf.l_margin, y=topo, h=16)
        except Exception:                       # logo em formato que o PDF não lê
            logger.warning("ERP/suprimentos: logo da empresa não coube no PDF do pedido")
            emp = {**emp, "logo": None}
    if not emp["logo"]:
        fonte("B", 15, AZUL)
        pdf.set_xy(pdf.l_margin, topo + 2)
        pdf.cell(80, 8, _latin(emp["fantasia"]))
    fonte("B", 15, AZUL)
    pdf.set_xy(pdf.l_margin + 70, topo)
    pdf.cell(largura - 70, 8, _latin("PEDIDO DE COMPRA"), align="R")
    fonte("B", 12)
    pdf.set_xy(pdf.l_margin + 70, topo + 8)
    pdf.cell(largura - 70, 6, _latin(f"Nº {d['numero']}"), align="R")
    fonte("", 8.5, CINZA)
    pdf.set_xy(pdf.l_margin + 70, topo + 14)
    pdf.cell(largura - 70, 4, _latin(f"emitido em {d['emitido_em']:%d/%m/%Y}"), align="R")
    pdf.set_y(topo + 21)
    pdf.set_draw_color(*AZUL)
    pdf.set_line_width(0.5)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(3)

    # ---- quem compra e de quem
    meio = largura / 2 - 2
    y = pdf.get_y()

    def caixa(x, titulo, linhas_):
        pdf.set_xy(x, y)
        fonte("B", 7.5, CINZA)
        pdf.cell(meio, 4, _latin(titulo.upper()))
        yy = y + 4.5
        for i, (estilo, texto) in enumerate(linhas_):
            if not texto:
                continue
            pdf.set_xy(x, yy)
            fonte(estilo, 9.5 if i == 0 else 8.5)
            pdf.multi_cell(meio, 4.2, _latin(texto), new_x="LEFT", new_y="NEXT")
            yy = pdf.get_y()
        return yy

    y1 = caixa(pdf.l_margin, "Faturar para", [
        ("B", emp["razao_social"]), ("", f"CNPJ {emp['cnpj']}"),
        ("", emp["endereco"] if emp["endereco"] != "Endereço não informado" else "")])
    forn = d["fornecedor"]
    y2 = caixa(pdf.l_margin + meio + 4, "Fornecedor", [
        ("B", forn["razao_social"]),
        ("", f"CNPJ/CPF {forn['documento']}" if forn["documento"] else ""),
        ("", f"A/C {forn['contato']}" if forn["contato"] else "")])
    pdf.set_y(max(y1, y2) + 3)

    # ---- os itens, um bloco por local de entrega
    colunas = [("Nº", 8, "R"), ("Solicitação", 22, "L"), ("Material", 0, "L"),
               ("Un.", 10, "C"), ("Qtd", 16, "R"), ("Unitário", 22, "R"),
               ("Total", 24, "R")]
    fixo = sum(c[1] for c in colunas)
    colunas = [(n, w or (largura - fixo), a) for n, w, a in colunas]
    varios = len(d["blocos"]) > 1

    for bloco in d["blocos"]:
        if pdf.get_y() > pdf.h - 60:
            pdf.add_page()
        pdf.set_fill_color(*FUNDO)
        fonte("B", 8.5, AZUL if bloco["na_obra"] else VERMELHO)
        titulo = ("ENTREGAR EM: " if bloco["na_obra"]
                  else f"ENTREGAR FORA DA OBRA - {bloco['local']}: ") + bloco["endereco"]
        pdf.multi_cell(largura, 5, _latin(titulo), fill=True,
                       new_x="LMARGIN", new_y="NEXT")
        fonte("", 8, CINZA)
        if bloco["obras"]:
            linha(("Material da(s) obra(s): " if not bloco["na_obra"] else "Obra: ")
                  + " · ".join(bloco["obras"]), 4)
        if bloco["mapa"]:
            fonte("", 8, (11, 92, 143))
            pdf.cell(0, 4, _latin(f"Localização no mapa: {bloco['mapa']}"),
                     link=bloco["mapa"], new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        fonte("B", 7.5, CINZA)
        for nome, w, alinhamento in colunas:
            pdf.cell(w, 5, _latin(nome.upper()), border="B", align=alinhamento)
        pdf.ln(5.5)
        for item in bloco["itens"]:
            if pdf.get_y() > pdf.h - 28:
                pdf.add_page()
            y0 = pdf.get_y()
            fonte("", 8.5)
            x = pdf.l_margin
            valores = [str(item["numero"]), item["solicitacao"], None, item["unidade"],
                       _qtd(item["quantidade"]), _br(item["preco_unitario"]),
                       _br(item["total"])]
            # o material primeiro, porque é ele que decide a altura da linha
            xm = pdf.l_margin + colunas[0][1] + colunas[1][1]
            pdf.set_xy(xm, y0)
            fonte("B", 8.5)
            pdf.multi_cell(colunas[2][1], 4.2, _latin(item["insumo"]),
                           new_x="LEFT", new_y="NEXT")
            if item["especificacao"]:
                fonte("", 8, CINZA)
                pdf.set_x(xm)
                pdf.multi_cell(colunas[2][1], 3.8, _latin(item["especificacao"]),
                               new_x="LEFT", new_y="NEXT")
            y1 = pdf.get_y()
            fonte("", 8.5)
            for (nome, w, alinhamento), valor in zip(colunas, valores):
                if valor is not None:
                    pdf.set_xy(x, y0)
                    pdf.cell(w, 4.2, _latin(valor), align=alinhamento)
                x += w
            pdf.set_y(y1 + 1)
            pdf.set_draw_color(220, 225, 234)
            pdf.set_line_width(0.1)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            pdf.ln(1)
        if varios:
            fonte("B", 8.5)
            pdf.cell(largura - 24, 5, _latin("Subtotal deste local"), align="R")
            pdf.cell(24, 5, _latin(_br(bloco["subtotal"])), align="R",
                     new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

    # ---- totais
    if pdf.get_y() > pdf.h - 70:
        pdf.add_page()

    def total(rotulo, valor, forte=False):
        fonte("B" if forte else "", 10 if forte else 9)
        if forte:
            pdf.set_fill_color(*FUNDO)
        pdf.set_x(pdf.l_margin + largura - 80)
        pdf.cell(50, 6 if forte else 5, _latin(rotulo), align="R", fill=forte)
        pdf.cell(30, 6 if forte else 5, _latin(valor), align="R", fill=forte,
                 new_x="LMARGIN", new_y="NEXT")

    total("Valor dos itens", _br(d["subtotal"]))
    if d["frete"] > 0:
        total("Frete", _br(d["frete"]))
    if d["desconto"] > 0:
        total("Desconto", "- " + _br(d["desconto"]))
    total("TOTAL DO PEDIDO  R$", _br(d["total"]), forte=True)
    pdf.ln(3)

    # ---- condições
    def par(rotulo, valor):
        if not valor:
            return
        fonte("B", 9)
        pdf.cell(42, 5, _latin(rotulo))
        fonte("", 9)
        pdf.multi_cell(0, 5, _latin(valor), new_x="LMARGIN", new_y="NEXT")

    condicao = d["condicao"]
    palavras = d["condicao_em_palavras"]
    if palavras and palavras.lower().replace(" ", "") != condicao.lower().replace(" ", ""):
        condicao += f" ({palavras})"
    par("Condição de pagamento:", condicao
        + (" - PAGAMENTO ANTECIPADO" if d["antecipado"] else ""))
    if d["parcelas"]:
        par("Parcelas:", "   ".join(
            f"{p['vencimento']:%d/%m/%Y}  R$ {_br(p['valor'])}"
            + (" (entrada)" if p["entrada"] else "") for p in d["parcelas"]))
    par("Entrega:", d["entrega"])
    if d["previsao_entrega"]:
        par("Material em obra até:", f"{d['previsao_entrega']:%d/%m/%Y}")
    par("Observações:", d["observacoes"])
    pdf.ln(2)

    # ---- as instruções da nota: é o que faz a nota casar com o pedido
    pdf.set_fill_color(255, 246, 229)
    fonte("B", 8.5, (138, 91, 0))
    pdf.multi_cell(largura, 5, _latin("PARA A NOTA FISCAL"), fill=True,
                   new_x="LMARGIN", new_y="NEXT")
    fonte("", 8.5)
    for texto in (
            f"1. Informar nas observações da nota: Pedido BWS {d['numero']}.",
            "2. Informar nas observações da nota o endereço de destino do material, "
            "conforme acima.",
            "3. Faturar apenas os itens listados neste pedido, contra o CNPJ "
            f"{d['empresa']['cnpj']}."):
        pdf.multi_cell(largura, 4.6, _latin(texto), fill=True,
                       new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # ---- quem comprou
    if pdf.get_y() > pdf.h - 34:
        pdf.add_page()
    comprador = d["comprador"]
    fonte("", 9)
    linha("Atenciosamente,")
    pdf.ln(2)
    if comprador["nome"]:
        fonte("B", 9.5)
        linha(comprador["nome"])
    fonte("", 8.5, CINZA)
    linha("Suprimentos - " + d["empresa"]["fantasia"])
    contatos = " · ".join(x for x in (comprador["email"] or d["empresa"]["email"],
                                      comprador["telefone"] or d["empresa"]["telefone"])
                          if x)
    if contatos:
        linha(contatos)
    return bytes(pdf.output())
