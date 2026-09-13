# -*- coding: utf-8 -*-
"""
Relatórios em PDF.

Usa o `fpdf2`, que **já está no serviço** — nenhuma dependência nova. O
`reportlab` do Streamlit não está, e acrescentá-lo era o que faria este
relatório ficar de fora.

SOBRE O ACENTO, que é a armadilha desta biblioteca. Com as fontes embutidas
(Helvetica e companhia), o `fpdf2` só escreve o que couber em **latin-1** — e
se algo não couber, ele não avisa: ele ESTOURA no meio da geração.

Latin-1 cobre o português inteiro: ç, ã, õ, é, ê, á, ú. O que ele não cobre são
os sinais tipográficos que entram sem ninguém perceber — o travessão "—" (que
está em vários textos deste projeto), as aspas curvas e as reticências de um
caractere só. Por isso todo texto passa por `_texto()` antes de ir para a
página: os sinais viram o equivalente simples, e o resto que sobrar vira "?"
em vez de derrubar o relatório.

É o mesmo caminho que o `emissaonf` já usa para a DANFSe.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.pdf")

# Sinais tipográficos que não existem em latin-1, e o que pôr no lugar.
# Vira o equivalente legível, não "?" — um travessão virando interrogação no
# meio de uma frase é pior do que um hífen.
TROCAS = {
    "—": "-",    # travessão
    "–": "-",    # meia-risca
    "‘": "'", "’": "'",
    "“": '"', "”": '"',
    "…": "...",
    " ": " ",    # espaço que não quebra
    "→": "->",
    "﻿": "",
}

LARGURA_UTIL = 190      # A4 retrato, com as margens padrão de 10 mm


def _texto(valor) -> str:
    """Deixa o texto no que a fonte embutida sabe escrever."""
    s = "" if valor is None else str(valor)
    for de, para in TROCAS.items():
        s = s.replace(de, para)
    return s.encode("latin-1", "replace").decode("latin-1")


class Folha:
    """Uma folha A4 com cabeçalho e rodapé, e alguns blocos prontos.

    Não é uma classe genérica de PDF: é só o suficiente para os dois
    relatórios que existem. Uma camada a mais aqui seria peso sem uso."""

    def __init__(self, titulo: str, subtitulo: str = ""):
        from fpdf import FPDF

        self.titulo = _texto(titulo)
        self.subtitulo = _texto(subtitulo)
        self.pdf = FPDF(format="A4")
        self.pdf.set_auto_page_break(auto=True, margin=15)
        self.pdf.add_page()
        self._cabecalho()

    def _cabecalho(self) -> None:
        from .horario import agora

        self.pdf.set_font("Helvetica", "B", 15)
        self.pdf.cell(0, 9, self.titulo, new_x="LMARGIN", new_y="NEXT")
        if self.subtitulo:
            self.pdf.set_font("Helvetica", "", 10)
            self.pdf.set_text_color(90, 104, 131)
            self.pdf.cell(0, 6, self.subtitulo, new_x="LMARGIN", new_y="NEXT")
        self.pdf.set_font("Helvetica", "", 8)
        self.pdf.set_text_color(120, 128, 145)
        self.pdf.cell(0, 5, _texto(
            f"BWS Construções - gerado em {agora().strftime('%d/%m/%Y às %H:%M')} "
            "(hora de Brasília)"), new_x="LMARGIN", new_y="NEXT")
        self.pdf.set_text_color(0, 0, 0)
        self.pdf.ln(3)

    def titulo_secao(self, texto: str) -> None:
        self.pdf.ln(3)
        self.pdf.set_font("Helvetica", "B", 11)
        self.pdf.set_fill_color(238, 242, 250)
        self.pdf.cell(0, 7, " " + _texto(texto), new_x="LMARGIN", new_y="NEXT",
                      fill=True)
        self.pdf.ln(1)

    def observacao(self, texto: str) -> None:
        self.pdf.set_font("Helvetica", "I", 8)
        self.pdf.set_text_color(90, 104, 131)
        self.pdf.multi_cell(0, 4, _texto(texto), new_x="LMARGIN", new_y="NEXT")
        self.pdf.set_text_color(0, 0, 0)
        self.pdf.ln(1)

    def numeros(self, pares) -> None:
        """Os totais do topo, dois por linha."""
        self.pdf.set_font("Helvetica", "", 10)
        for rotulo, valor in pares:
            self.pdf.set_font("Helvetica", "", 10)
            self.pdf.cell(60, 6, _texto(rotulo))
            self.pdf.set_font("Helvetica", "B", 10)
            self.pdf.cell(0, 6, _texto(valor), new_x="LMARGIN", new_y="NEXT")

    def _quebrar(self, texto: str, largura: float, linhas_max: int) -> list:
        """O texto repartido nas linhas que cabem na coluna.

        QUEBRA POR PALAVRA, e só parte a palavra quando ela sozinha não cabe —
        um código de obra comprido não pode empurrar a descrição inteira para
        fora. Passando do teto de linhas, a última termina em "..." para ficar
        claro que sobrou texto; sem isso, a pessoa lê meia frase achando que é
        a frase inteira."""
        util = largura - 1.4
        saida: list = []
        for pedaco in _texto(texto).split():
            if not saida:
                saida.append(pedaco)
                continue
            tentativa = saida[-1] + " " + pedaco
            if self.pdf.get_string_width(tentativa) <= util:
                saida[-1] = tentativa
            else:
                saida.append(pedaco)
        # Palavra sozinha maior que a coluna: parte na marra, senão ela invade
        # a coluna vizinha e embaralha a linha toda.
        partidas: list = []
        for linha in saida:
            while self.pdf.get_string_width(linha) > util:
                corte = len(linha)
                while corte > 1 and self.pdf.get_string_width(linha[:corte]) > util:
                    corte -= 1
                partidas.append(linha[:corte])
                linha = linha[corte:]
            partidas.append(linha)
        if not partidas:
            return [""]
        if len(partidas) > linhas_max:
            partidas = partidas[:linhas_max]
            ultima = partidas[-1]
            while ultima and self.pdf.get_string_width(ultima + "...") > util:
                ultima = ultima[:-1]
            partidas[-1] = ultima + "..."
        return partidas

    def tabela(self, cabecalho, linhas, larguras=None, direita=(),
               fonte=8, linhas_max=1) -> None:
        """Uma tabela, com o cabeçalho repetido a cada página.

        `direita` são os índices das colunas de número, que alinham à direita —
        coluna de dinheiro alinhada à esquerda é ilegível.

        `linhas_max` maior que 1 deixa o texto QUEBRAR EM VÁRIAS LINHAS dentro
        da célula, em vez de ser cortado. Pedido do dono em 11/09/2026, para o
        relatório do lote caber a descrição: *"pode reduzir a fonte
        consideravelmente pra caber mais informação (…) e pode usar a quebra de
        linha"*. Com `linhas_max=1` o comportamento é o de antes."""
        if larguras is None:
            larguras = [LARGURA_UTIL / len(cabecalho)] * len(cabecalho)
        altura = fonte * 0.5          # entrelinha proporcional ao corpo

        def escrever_cabecalho():
            self.pdf.set_font("Helvetica", "B", fonte)
            self.pdf.set_fill_color(238, 242, 250)
            for i, titulo in enumerate(cabecalho):
                self.pdf.cell(larguras[i], altura + 1.5, _texto(titulo),
                              border="B", fill=True,
                              align="R" if i in direita else "L")
            self.pdf.ln()

        escrever_cabecalho()
        self.pdf.set_font("Helvetica", "", fonte)
        for linha in linhas:
            celulas = [self._quebrar(valor, larguras[i], linhas_max)
                       for i, valor in enumerate(linha)]
            alta = max(len(c) for c in celulas) * altura + 1

            # Quebrou a página? O cabeçalho precisa reaparecer, senão a segunda
            # página vira uma tabela de colunas sem nome.
            if self.pdf.will_page_break(alta):
                self.pdf.add_page()
                escrever_cabecalho()
                self.pdf.set_font("Helvetica", "", fonte)

            topo, esquerda = self.pdf.get_y(), self.pdf.get_x()
            x = esquerda
            for i, pedacos in enumerate(celulas):
                for n, pedaco in enumerate(pedacos):
                    self.pdf.set_xy(x, topo + n * altura)
                    self.pdf.cell(larguras[i], altura, pedaco,
                                  align="R" if i in direita else "L")
                x += larguras[i]
            # A régua vai embaixo da linha INTEIRA, depois de escrever tudo:
            # com alturas diferentes por célula, a borda de cada `cell` sairia
            # em alturas diferentes e a tabela ficaria serrilhada.
            self.pdf.line(esquerda, topo + alta, x, topo + alta)
            self.pdf.set_xy(esquerda, topo + alta)

    def bytes(self) -> bytes:
        return bytes(self.pdf.output())


# ---------------------------------------------------------------------------
# Os dois relatórios
# ---------------------------------------------------------------------------
def relatorio(filtros: dict, tipo: str, periodo: str) -> bytes:
    """O relatório da tela, em PDF: os números, as quebras e os credores."""
    from . import consultas
    from .formatos import moeda

    numeros = consultas.numeros_do_relatorio(filtros, tipo, periodo)
    folha = Folha(
        "Análise de SPs - Relatório",
        f"{consultas.TIPOS[tipo]} · {consultas.PERIODOS[periodo]}")

    folha.numeros([
        ("Lançamentos", f"{numeros['quantidade']:,}".replace(",", ".")),
        ("Valor total", "R$ " + moeda(numeros["total"])),
        ("Ticket médio", "R$ " + moeda(numeros["ticket"])),
        ("Vencidos", f"{numeros['vencidos_qtd']} - R$ "
                     + moeda(numeros["vencidos_total"])),
    ])
    folha.observacao(
        "SPs canceladas ficam de fora de todo o relatório. O período conta pela "
        + ("data do pagamento." if tipo == "pagas" else "data de vencimento."))

    for dimensao, rotulo in consultas.DIMENSOES.items():
        linhas = consultas.agregar(filtros, dimensao, tipo, periodo, 40)
        if not linhas:
            continue
        folha.titulo_secao(f"Por {rotulo.lower()}")
        folha.tabela(
            [rotulo, "Qtd", "Total"],
            [[l["rotulo"], l["quantidade"], "R$ " + moeda(l["total"])]
             for l in linhas],
            larguras=[110, 25, 55], direita={1, 2})

    credores = consultas.top_credores(filtros, tipo, periodo, 40)
    if credores:
        folha.titulo_secao("Maiores credores")
        folha.tabela(
            ["CPF/CNPJ", "Credor", "Qtd", "Total"],
            [[c["documento"], c["credor"], c["quantidade"],
              "R$ " + moeda(c["total"])] for c in credores],
            larguras=[45, 65, 25, 55], direita={2, 3})
        folha.observacao(
            "Agrupados pelo CPF/CNPJ, não pelo nome: o mesmo fornecedor aparece "
            "escrito de vários jeitos, e somar por nome o partiria em três.")

    aging = consultas.aging_vencidos(filtros, periodo)
    if aging:
        folha.titulo_secao("Há quanto tempo está atrasado")
        folha.tabela(
            ["Atraso", "Qtd", "Total"],
            [[f["faixa"], f["quantidade"], "R$ " + moeda(f["total"])]
             for f in aging],
            larguras=[110, 25, 55], direita={1, 2})
        folha.observacao("Uma SP que vence hoje não está atrasada.")

    return folha.bytes()


def relatorio_do_lote(montado: dict) -> bytes:
    """O lote, grupo a grupo, com o total de cada um.

    É o papel que acompanha a remessa: quem vai efetivar os pagamentos confere
    por aqui, na mesma organização que quem montou o lote escolheu."""
    from .formatos import data_br, moeda

    folha = Folha(
        "Análise de SPs - Relatório do Lote",
        f"{montado['quantidade']} SP(s) · total R$ {moeda(montado['total_geral'])}")

    for grupo in montado["grupos"]:
        if not grupo["linhas"] and not grupo["nao_encontrados"]:
            continue
        folha.titulo_secao(
            f"{grupo['titulo_exibido']}  -  {len(grupo['linhas'])} SP(s), "
            f"R$ {moeda(grupo['total'])}")
        if grupo["linhas"]:
            # AS COLUNAS E O TAMANHO DA FONTE SÃO PEDIDO DO DONO, 11/09/2026:
            # *"reduz a fonte consideravelmente pra caber mais informação.
            # Quero que tenha a descrição. Quero que tenha obra. E pode usar a
            # quebra de linha."*
            #
            # "Obra" é o CENTRO DE CUSTO da planilha — é a palavra que ele usa,
            # e é o mesmo nome que a coluna tem na tela.
            #
            # A DESCRIÇÃO GANHA A MAIOR FATIA e três linhas: é o texto livre, o
            # único que realmente precisa quebrar. As outras colunas cabem em
            # uma linha quase sempre, e duas são folga para o credor comprido.
            #
            # O "R$" saiu das células e foi para o título da coluna: repetido
            # em trinta linhas ele só gastava a largura que a descrição queria.
            folha.tabela(
                ["SP", "Vencim.", "Credor", "Descrição", "Obra", "Forma",
                 "Conta", "Valor R$"],
                [[l["id"], data_br(l["vencimento_d"]), l["credor"],
                  l.get("descricao") or "", l.get("centro_custo") or "",
                  l["forma_pagamento"], l["conta"], moeda(l["valor_num"])]
                 for l in grupo["linhas"]],
                larguras=[20, 17, 34, 44, 24, 16, 17, 18], direita={7},
                fonte=6.5, linhas_max=3)
        if grupo["nao_encontrados"]:
            folha.observacao(
                "Não encontradas na base: " + ", ".join(grupo["nao_encontrados"]))

    folha.titulo_secao(f"TOTAL GERAL:  R$ {moeda(montado['total_geral'])}")
    return folha.bytes()
