# ============================================================================
# ERP — core/suprimentos/cotacao_pdf.py
# O pedido de cotação em PDF — o que o fornecedor recebe para dar preço.
#
# O dono, em 21/11/2026: "o e-mail você colocou em TXT, mas ele tem que ser em
# PDF mesmo. Tem que ter um corpo de e-mail padrão, avisando o fornecedor. E no
# anexo, o relatório completo."
#
# O e-mail passa a ser curto e sempre igual; o conteúdo vai neste anexo. Ele
# traz o mesmo que o texto trazia — itens com especificação, quantidade e
# unidade, separados por endereço de entrega — e duas colunas em branco
# (preço unitário e prazo), porque muito fornecedor pequeno responde
# preenchendo o próprio pedido à mão e devolvendo a foto.
#
# SEM PREÇO DE NINGUÉM, como sempre: este documento vai para todos os
# fornecedores da cotação, e é o mesmo para todos.
# ============================================================================
from __future__ import annotations

import io
import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos.pedido_pdf import AZUL, CINZA, FUNDO, _latin, _qtd

logger = logging.getLogger(__name__)


def gerar(s: Session, cotacao_id: int, empresa, *, prazo: Optional[str] = None,
          observacao: str = "", usuario=None) -> bytes:
    from fpdf import FPDF
    from app.apps.erp.core.comum.formato import documento_por_extenso
    from app.apps.erp.core.suprimentos import envio
    from app.apps.erp.core.suprimentos.entrega import endereco_da_empresa
    from app.apps.erp.db.models.cadastros import Cotacao, CotacaoItem

    cot = s.get(Cotacao, cotacao_id)
    if cot is None:
        raise ErroNaoEncontrado("Cotação não encontrada.")
    linhas = sorted([x for x in s.scalars(select(CotacaoItem).where(
        CotacaoItem.cotacao_id == cotacao_id)).all() if x.cotacao_id == cotacao_id],
        key=lambda x: x.numero or 0)
    if not linhas:
        raise ErroValidacao("A cotação não tem item nenhum para cotar.")
    blocos = envio._por_endereco(s, linhas)

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=16)
    pdf.set_margins(14, 14, 14)
    pdf.add_page()
    largura = pdf.w - pdf.l_margin - pdf.r_margin

    def fonte(estilo="", tamanho=9, cor=(20, 35, 59)):
        pdf.set_font("Helvetica", estilo, tamanho)
        pdf.set_text_color(*cor)

    def linha(texto, altura=4.6):
        pdf.multi_cell(0, altura, _latin(texto), new_x="LMARGIN", new_y="NEXT")

    topo = pdf.get_y()
    fantasia = empresa.nome_fantasia or empresa.razao_social
    logo = bytes(empresa.logo) if getattr(empresa, "logo", None) else None
    if logo:
        try:
            pdf.image(io.BytesIO(logo), x=pdf.l_margin, y=topo, h=16)
        except Exception:
            logo = None
    if not logo:
        fonte("B", 15, AZUL)
        pdf.set_xy(pdf.l_margin, topo + 2)
        pdf.cell(80, 8, _latin(fantasia))
    fonte("B", 15, AZUL)
    pdf.set_xy(pdf.l_margin + 70, topo)
    pdf.cell(largura - 70, 8, _latin("SOLICITAÇÃO DE COTAÇÃO"), align="R")
    fonte("B", 12)
    pdf.set_xy(pdf.l_margin + 70, topo + 8)
    pdf.cell(largura - 70, 6, _latin(f"Nº {cot.numero}"), align="R")
    fonte("", 8.5, CINZA)
    pdf.set_xy(pdf.l_margin + 70, topo + 14)
    # ⚑ SEM O NOME INTERNO DA COTAÇÃO, COM A HORA (21/11/2026). O dono viu
    # "Utilitários 1" no cabeçalho e não soube o que era — é o apelido que o
    # comprador dá à cotação, e não diz nada ao fornecedor. A hora entra
    # porque o mesmo pedido pode sair de manhã e ser refeito à tarde.
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.apps.erp.core.comum.uso import FUSO
    agora = datetime.now(ZoneInfo(FUSO))
    pdf.cell(largura - 70, 4, _latin(f"Emitida em {agora:%d/%m/%Y} às {agora:%H:%M}"), align="R")
    pdf.set_y(topo + 21)
    pdf.set_draw_color(*AZUL)
    pdf.set_line_width(0.5)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(3)

    fonte("B", 7.5, CINZA)
    linha("EMPRESA", 4)
    fonte("B", 9.5)
    linha(empresa.razao_social, 4.4)
    fonte("", 8.5)
    cnpj = documento_por_extenso(empresa.cnpj) or empresa.cnpj
    linha(f"CNPJ {cnpj}", 4.2)
    sede = endereco_da_empresa(empresa)
    if sede != "Endereço não informado":
        linha(sede, 4.2)
    pdf.ln(2)
    fonte("", 9)
    # ⚑ Saiu "os preços podem ser informados nas colunas em branco deste
    # documento" (21/11/2026). Ele: "É um PDF que dá para lançar o valor?
    # Porque se não der, não adianta." Não dá — e as colunas em branco saíram
    # junto, para o papel não prometer o que não entrega.
    linha("Solicitamos proposta de preço para os itens abaixo.")
    pdf.ln(2)

    colunas = [("Nº", 8, "R"), ("Material", 0, "L"), ("Qtd", 18, "R"), ("Un.", 14, "C")]
    fixo = sum(c[1] for c in colunas)
    colunas = [(n, w or (largura - fixo), a) for n, w, a in colunas]
    numero = 0
    for bloco in blocos:
        if pdf.get_y() > pdf.h - 50:
            pdf.add_page()
        pdf.set_fill_color(*FUNDO)
        fonte("B", 8.5, AZUL)
        pdf.multi_cell(largura, 5, _latin(f"ENTREGAR EM: {bloco['endereco']}"), fill=True,
                       new_x="LMARGIN", new_y="NEXT")
        if bloco["obras"]:
            fonte("", 8, CINZA)
            linha("Obra(s): " + " · ".join(bloco["obras"]), 4)
        pdf.ln(1)
        fonte("B", 7.5, CINZA)
        for nome, w, a in colunas:
            pdf.cell(w, 5, _latin(nome.upper()), border="B", align=a)
        pdf.ln(5.5)
        for item, insumo in bloco["itens"]:
            numero += 1
            if pdf.get_y() > pdf.h - 26:
                pdf.add_page()
            y0 = pdf.get_y()
            x_mat = pdf.l_margin + colunas[0][1]
            pdf.set_xy(x_mat, y0 + 0.6)
            fonte("B", 8.5)
            pdf.multi_cell(colunas[1][1], 4.2,
                           _latin(getattr(insumo, "descricao", "") or "item"),
                           new_x="LEFT", new_y="NEXT")
            especificacao = (getattr(item, "especificacao", "") or "").strip()
            if especificacao:
                fonte("", 8, CINZA)
                pdf.set_x(x_mat)
                pdf.multi_cell(colunas[1][1], 3.8, _latin(especificacao),
                               new_x="LEFT", new_y="NEXT")
            altura = max(pdf.get_y() - y0 + 1.2, 7.5)
            fonte("", 8.5)
            x = pdf.l_margin
            valores = [str(numero), None, _qtd(getattr(item, "quantidade", 0)),
                       getattr(item, "unidade", "") or ""]
            pdf.set_draw_color(200, 207, 220)
            pdf.set_line_width(0.15)
            for (nome, w, a), valor in zip(colunas, valores):
                pdf.rect(x, y0, w, altura)
                if valor is not None:
                    pdf.set_xy(x, y0 + 0.6)
                    pdf.cell(w, 4.2, _latin(valor), align=a)
                x += w
            pdf.set_y(y0 + altura)
        pdf.ln(3)

    if pdf.get_y() > pdf.h - 60:
        pdf.add_page()
    pdf.set_fill_color(255, 246, 229)
    fonte("B", 8.5, (138, 91, 0))
    pdf.multi_cell(largura, 5, _latin("A PROPOSTA DEVE INFORMAR"), fill=True,
                   new_x="LMARGIN", new_y="NEXT")
    fonte("", 8.5)
    for texto in ("Preço unitário de cada item, na unidade pedida.",
                  "Prazo de entrega no endereço indicado.",
                  "Condição de pagamento (à vista, 28 dias, 30/60...).",
                  "Valor do frete, se não estiver incluso, e a validade da proposta."):
        pdf.multi_cell(largura, 4.6, _latin("- " + texto), fill=True,
                       new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    if prazo:
        fonte("B", 9.5)
        linha(f"Pedimos retorno até {prazo}.")
    if (observacao or "").strip():
        fonte("", 9)
        linha(observacao.strip())
    pdf.ln(2)
    fonte("", 9)
    linha(f"Referência desta cotação: {cot.numero}. Favor mantê-la ao responder.")
    pdf.ln(4)
    linha("Atenciosamente,")
    from app.apps.erp.core.suprimentos.pedido_pdf import _assinatura_de, colocar_assinatura
    colocar_assinatura(pdf, _assinatura_de(s, usuario))
    if getattr(usuario, "nome", ""):
        fonte("B", 9.5)
        linha(usuario.nome)
    fonte("", 8.5, CINZA)
    linha("Suprimentos - " + fantasia)
    contatos = " · ".join(x for x in (getattr(usuario, "email", "") or empresa.email,
                                      getattr(usuario, "telefone", "") or empresa.telefone)
                          if x)
    if contatos:
        linha(contatos)
    return bytes(pdf.output())
