# -*- coding: utf-8 -*-
"""
O relatório do DRE em PDF — o último pedaço do painel antigo a ser convertido.

O gerador original usava `reportlab`, que **não está no serviço**. Aqui é
`fpdf2`, que já está: nenhuma dependência nova. Foi essa a razão de o PDF ter
ficado por último — e é essa a razão de ele finalmente caber.

MESMOS NÚMEROS QUE A PLANILHA, POR CONSTRUÇÃO
Este módulo recebe **exatamente** a estrutura que o `excel.montar` recebe:
uma lista de `(nome, colunas, linhas)`. Quem monta o relatório completo é a
rota de download, num lugar só. Assim o PDF e o Excel não podem divergir: se
divergissem, a pergunta "qual dos dois está certo?" não teria resposta, e um
relatório em que não se confia não serve para nada.

SOBRE O ACENTO, que é a armadilha desta biblioteca. Com as fontes embutidas, o
`fpdf2` só escreve o que couber em **latin-1** — e não avisa: estoura no meio da
geração. Latin-1 cobre o português (ç, ã, õ, é), mas não os sinais tipográficos
que entram sem ninguém perceber: o travessão, as aspas curvas, as reticências
de um caractere. Por isso todo texto passa por `_texto()`.

É o mesmo caminho que o `emissaonf` e o `analisesps` já usam. O código não é
compartilhado de propósito: cada área do repositório é mexida por uma sessão
que não conhece as outras, e um relatório do painel não pode quebrar porque
alguém ajustou o PDF de outro módulo.
"""
from __future__ import annotations

import datetime as dt
import io
import logging

logger = logging.getLogger("painel.pdf")

# Sinais tipográficos que não existem em latin-1, e o que pôr no lugar. Vira o
# equivalente legível, não "?" — travessão virando interrogação no meio de uma
# frase é pior do que hífen.
TROCAS = {
    "—": "-", "–": "-", "−": "-",
    "‘": "'", "’": "'", "“": '"', "”": '"',
    "…": "...", " ": " ", "→": "->", "×": "x", "Δ": "D", "﻿": "",
}

# A4 deitado: as tabelas deste relatório têm até oito colunas, e em pé elas
# ficariam espremidas a ponto de não dar para ler.
LARGURA_PAGINA = 297
ALTURA_PAGINA = 210
MARGEM = 10
LARGURA_UTIL = LARGURA_PAGINA - 2 * MARGEM

AZUL = (21, 101, 192)
CINZA_CLARO = (242, 242, 242)
CINZA_BORDA = (204, 204, 204)
TINTA_SUAVE = (85, 85, 85)
VERDE = (39, 174, 96)
VERMELHO = (192, 57, 43)

# Teto por seção. O relatório antigo cortava em 2.500 e dizia isso na página; o
# mesmo número, pelo mesmo motivo: um PDF de cem mil linhas ninguém abre, e
# quem precisa da lista inteira baixa a planilha.
TETO_DE_LINHAS = 2500

CHAVES_DE_IDENTIFICADOR = {"lancamento", "codigo", "codigo_lancamento"}


def _texto(valor) -> str:
    """Deixa o texto no que a fonte embutida sabe escrever."""
    s = "" if valor is None else str(valor)
    for de, para in TROCAS.items():
        s = s.replace(de, para)
    return s.encode("latin-1", "replace").decode("latin-1")


def brl(v) -> str:
    """Dinheiro em portugues. Publico: a rota tambem monta o resumo da capa."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v or "")
    texto = f"{abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-R$ " if v < 0 else "R$ ") + texto


def _celula(valor, chave: str, titulo: str) -> tuple[str, str]:
    """O texto da célula e o alinhamento. Dinheiro e data seguem a MESMA regra
    da planilha — a coluna se reconhece pelo nome —, senão as duas saídas do
    mesmo relatório formatariam o mesmo número de jeitos diferentes."""
    from . import excel

    if valor is None or valor == "":
        return "", "L"
    # Endereço vira "Pipefy", clicável (quem escreve a linha passa o link).
    # O endereço inteiro na célula tomaria a largura da página e não diria nada.
    if isinstance(valor, str) and valor.startswith(("http://", "https://")):
        return "Pipefy", "L"
    if isinstance(valor, (dt.date, dt.datetime)):
        return valor.strftime("%d/%m/%Y"), "L"
    if excel._e_dinheiro(titulo) and isinstance(valor, (int, float)):
        return brl(valor), "R"
    # número de título do OMIE é identificador, não quantidade: sem pontos de
    # milhar, senão quem vai procurá-lo no OMIE digita "11.204.772.585"
    if chave in CHAVES_DE_IDENTIFICADOR and isinstance(valor, int):
        return str(valor), "R"
    if isinstance(valor, (int, float)):
        formatado = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return (formatado.removesuffix(",00") if isinstance(valor, int)
                else formatado), "R"
    return _texto(valor), "L"


# O que sai do PDF primeiro quando as colunas não cabem na folha deitada — na
# ordem. São colunas de DETALHE: estão todas na planilha, que não tem limite de
# largura. Dinheiro, data e o que identifica o lançamento nunca saem.
#
# 01/10/2026, o dono: "na parte mais analítica fica muito imprensado e não
# aparece; em coluna de valor sai só o R$ e não sai o número". O Despesas
# Analítico tem 23 colunas; repartidas numa folha, a coluna de valor ficava
# com 11 mm e o corte deixava "-R$ 3." no lugar de "-R$ 3.000,00".
ORDEM_DE_SAIDA = (
    "cnpj", "observacao", "pedido", "medicao", "vencimento", "projeto",
    "situacao", "grupo", "data_vencimento", "data_pagamento", "atraso",
    "link", "lancamento", "codigo", "conta", "documento", "categoria",
)
LARGURA_MINIMA_DE_TEXTO = 21.0     # mm — abaixo disso um nome vira "SH FORM."
LARGURA_MAXIMA_DE_TEXTO = 70.0
FOLGA_DA_CELULA = 2.2              # mm — a borda e o respiro dos dois lados
FONTES_DA_TABELA = (6.5, 6.0, 5.5) # tenta a maior; desce só se não couber


def _e_rigida(chave, titulo, linhas) -> bool:
    """Coluna que NÃO pode ser cortada: dinheiro, data, número. Cortada, ela
    mente — "-R$ 3." parece um valor e não é."""
    from . import excel
    if excel._e_dinheiro(titulo):
        return True
    amostra = [l.get(chave) for l in linhas[:200]
               if l.get(chave) not in (None, "")]
    return bool(amostra) and all(
        isinstance(v, (int, float, dt.date, dt.datetime)) and not isinstance(v, bool)
        for v in amostra)


class Relatorio:
    """A folha A4 deitada do painel, com cabeçalho, rodapé e tabelas.

    Não é uma classe genérica de PDF: é só o que este relatório precisa."""

    def __init__(self, titulo: str, subtitulo: str = ""):
        from fpdf import FPDF

        self.titulo = _texto(titulo)
        self.subtitulo = _texto(subtitulo)
        self.pdf = FPDF(orientation="L", format="A4")
        self.pdf.set_auto_page_break(auto=True, margin=14)
        self.pdf.set_margins(MARGEM, MARGEM, MARGEM)
        self.pdf.set_title(self.titulo)
        # o rodapé com o número da página é o que faz um relatório de cem
        # páginas continuar sendo um documento, e não um monte de folhas
        self.pdf.footer = self._rodape                      # type: ignore[method-assign]

    def _rodape(self):
        self.pdf.set_y(-12)
        self.pdf.set_font("Helvetica", "", 7)
        self.pdf.set_text_color(*TINTA_SUAVE)
        self.pdf.cell(0, 5, _texto(f"{self.titulo}  |  pagina {self.pdf.page_no()}"),
                      align="C")
        self.pdf.set_text_color(0, 0, 0)

    # -- blocos -----------------------------------------------------------
    def capa(self, resumo: list[tuple[str, str]], observacao: str = ""):
        self.pdf.add_page()
        self.pdf.set_font("Helvetica", "B", 18)
        self.pdf.set_text_color(*AZUL)
        self.pdf.cell(0, 10, self.titulo, new_x="LMARGIN", new_y="NEXT")
        self.pdf.set_text_color(0, 0, 0)
        if self.subtitulo:
            self.pdf.set_font("Helvetica", "", 9)
            self.pdf.set_text_color(*TINTA_SUAVE)
            self.pdf.multi_cell(0, 5, self.subtitulo, new_x="LMARGIN", new_y="NEXT")
            self.pdf.set_text_color(0, 0, 0)
        self.pdf.ln(4)

        for rotulo, valor in resumo:
            self.pdf.set_font("Helvetica", "B" if not valor else "", 11)
            self.pdf.cell(110, 7, _texto(rotulo), border="B")
            self.pdf.set_font("Helvetica", "B", 11)
            self.pdf.cell(70, 7, _texto(valor), border="B", align="R",
                          new_x="LMARGIN", new_y="NEXT")
        if observacao:
            self.pdf.ln(3)
            self.pdf.set_font("Helvetica", "I", 8)
            self.pdf.set_text_color(*TINTA_SUAVE)
            self.pdf.multi_cell(0, 4, _texto(observacao), new_x="LMARGIN", new_y="NEXT")
            self.pdf.set_text_color(0, 0, 0)

    def titulo_de_secao(self, texto: str):
        self.pdf.add_page()
        self.pdf.set_font("Helvetica", "B", 13)
        self.pdf.set_text_color(*AZUL)
        self.pdf.cell(0, 8, _texto(texto), new_x="LMARGIN", new_y="NEXT")
        self.pdf.set_text_color(0, 0, 0)
        self.pdf.ln(1)

    def tabela(self, colunas, linhas):
        """Uma seção do relatório. Cabeçalho repetido a cada página — sem isso,
        da segunda folha em diante ninguém sabe o que é cada coluna."""
        if not linhas:
            self.pdf.set_font("Helvetica", "I", 9)
            self.pdf.set_text_color(*TINTA_SUAVE)
            self.pdf.cell(0, 6, _texto("Nenhum lançamento nos filtros escolhidos."),
                          new_x="LMARGIN", new_y="NEXT")
            self.pdf.set_text_color(0, 0, 0)
            return

        cortadas = 0
        if len(linhas) > TETO_DE_LINHAS:
            cortadas = len(linhas) - TETO_DE_LINHAS
            linhas = linhas[:TETO_DE_LINHAS]

        colunas, larguras, fonte, omitidas = self._plano(colunas, linhas)
        altura = 4.6
        if omitidas:
            self.pdf.set_font("Helvetica", "I", 7)
            self.pdf.set_text_color(*TINTA_SUAVE)
            self.pdf.multi_cell(0, 3.8, _texto(
                "Neste PDF ficaram de fora, por falta de espaço na folha: "
                + ", ".join(omitidas) + ". Estão na planilha (botão Baixar Excel)."),
                new_x="LMARGIN", new_y="NEXT")
            self.pdf.set_text_color(0, 0, 0)
            self.pdf.ln(1)

        cabecalhos = [self._quebra_em_linhas(_texto(t), l, fonte)
                      for (_c, t), l in zip(colunas, larguras)]
        linhas_do_cabecalho = max(len(c) for c in cabecalhos)
        altura_da_linha_do_cabecalho = 3.1

        def _cabecalho():
            self.pdf.set_font("Helvetica", "B", fonte)
            self.pdf.set_fill_color(*AZUL)
            self.pdf.set_text_color(255, 255, 255)
            self.pdf.set_draw_color(*CINZA_BORDA)
            x0, y0 = self.pdf.get_x(), self.pdf.get_y()
            alto = linhas_do_cabecalho * altura_da_linha_do_cabecalho + 1.2
            x = x0
            for texto, largura in zip(cabecalhos, larguras):
                self.pdf.rect(x, y0, largura, alto, style="DF")
                topo = y0 + (alto - len(texto) * altura_da_linha_do_cabecalho) / 2
                for k, pedaco in enumerate(texto):
                    self.pdf.set_xy(x, topo + k * altura_da_linha_do_cabecalho)
                    self.pdf.cell(largura, altura_da_linha_do_cabecalho, pedaco,
                                  align="C")
                x += largura
            self.pdf.set_xy(x0, y0 + alto)
            self.pdf.set_text_color(0, 0, 0)

        _cabecalho()
        self.pdf.set_font("Helvetica", "", fonte)
        for i, linha in enumerate(linhas):
            # a folha acabou: nova página e o cabeçalho de novo
            if self.pdf.will_page_break(altura):
                self.pdf.add_page()
                _cabecalho()
                self.pdf.set_font("Helvetica", "", fonte)
            listrado = i % 2 == 1
            self.pdf.set_fill_color(*(CINZA_CLARO if listrado else (255, 255, 255)))
            for (chave, titulo), largura in zip(colunas, larguras):
                valor = linha.get(chave)
                texto, alinhamento = _celula(valor, chave, titulo)
                endereco = (valor if isinstance(valor, str)
                            and valor.startswith(("http://", "https://")) else "")
                if endereco:
                    self.pdf.set_text_color(*AZUL)
                if alinhamento == "R" or self._rigida_na_tabela(chave):
                    # número e data NUNCA são cortados: se por algum motivo não
                    # couberem, a letra diminui só nesta célula
                    self._celula_inteira(largura, altura, texto, alinhamento, fonte)
                    continue
                self.pdf.cell(largura, altura, self._recorta(texto, largura),
                              border=1, align=alinhamento, fill=True,
                              link=endereco or None)
                if endereco:
                    self.pdf.set_text_color(0, 0, 0)
            self.pdf.ln()

        if cortadas:
            self.pdf.ln(2)
            self.pdf.set_font("Helvetica", "I", 7.5)
            self.pdf.set_text_color(*TINTA_SUAVE)
            self.pdf.multi_cell(0, 4, _texto(
                f"Mostrando as {TETO_DE_LINHAS:,} primeiras de "
                f"{TETO_DE_LINHAS + cortadas:,} linhas. A lista completa está na "
                f"planilha (botão Baixar Excel).").replace(",", "."),
                new_x="LMARGIN", new_y="NEXT")
            self.pdf.set_text_color(0, 0, 0)

    # -- medidas da tabela ---------------------------------------------------
    def _largura_do_texto(self, texto: str, fonte: float, negrito=False) -> float:
        self.pdf.set_font("Helvetica", "B" if negrito else "", fonte)
        return self.pdf.get_string_width(texto)

    def _plano(self, colunas, linhas):
        """Quais colunas entram, com que largura e em que tamanho de letra.

        Rígidas (dinheiro, data, número) ganham a largura do MAIOR valor delas,
        medida de verdade na fonte do PDF. As de texto dividem o que sobra, na
        proporção do que precisam, com um mínimo. Se nem assim couber, sai uma
        coluna de detalhe (ORDEM_DE_SAIDA) e tenta de novo; só no fim a letra
        diminui."""
        amostra = linhas[:400]
        rigidas = {c for c, t in colunas if _e_rigida(c, t, amostra)}
        self._rigidas = rigidas
        restantes = list(colunas)
        omitidas: list[str] = []
        while True:
            for fonte in FONTES_DA_TABELA:
                plano = self._larguras_para(restantes, amostra, rigidas, fonte)
                if plano is not None:
                    return restantes, plano, fonte, omitidas
            fora = next((c for c in ORDEM_DE_SAIDA
                         if any(k == c for k, _t in restantes)), None)
            if fora is None or len(restantes) <= 2:
                # não há mais o que tirar: reparte como der, na menor letra
                fonte = FONTES_DA_TABELA[-1]
                return (restantes,
                        self._larguras_para(restantes, amostra, rigidas, fonte,
                                            forcar=True),
                        fonte, omitidas)
            omitidas.append(next(t for k, t in restantes if k == fora))
            restantes = [(k, t) for k, t in restantes if k != fora]

    def _larguras_para(self, colunas, amostra, rigidas, fonte, forcar=False):
        fixas, naturais = {}, {}
        for chave, titulo in colunas:
            maior = 0.0
            for linha in amostra:
                texto, _ = _celula(linha.get(chave), chave, titulo)
                if texto:
                    maior = max(maior, self._largura_do_texto(texto, fonte))
            # o título quebra em até duas linhas: conta a maior palavra dele
            palavra = max((self._largura_do_texto(p, fonte, negrito=True)
                           for p in _texto(titulo).split()), default=0.0)
            if chave in rigidas:
                fixas[chave] = max(maior, palavra) + FOLGA_DA_CELULA
            else:
                naturais[chave] = min(max(maior, palavra) + FOLGA_DA_CELULA,
                                      LARGURA_MAXIMA_DE_TEXTO)
        sobra = LARGURA_UTIL - sum(fixas.values())
        minimos = {k: min(LARGURA_MINIMA_DE_TEXTO, v) for k, v in naturais.items()}
        if sobra < sum(minimos.values()) and not forcar:
            return None
        larguras = {}
        if naturais:
            if sum(naturais.values()) <= sobra:
                # cabe tudo: o que sobra se espalha por TODAS as colunas, na
                # proporção de cada uma — senão uma coluna de nome curto vira
                # uma faixa enorme e os números ficam espremidos ao lado
                extra = sobra - sum(naturais.values())
                todas = {**naturais, **fixas}
                total = sum(todas.values()) or 1
                larguras = {k: v + extra * v / total for k, v in naturais.items()}
                fixas = {k: v + extra * v / total for k, v in fixas.items()}
            else:
                base = max(sobra, 0.0) - sum(minimos.values())
                excesso = {k: naturais[k] - minimos[k] for k in naturais}
                total = sum(excesso.values()) or 1
                larguras = {k: minimos[k] + max(base, 0.0) * excesso[k] / total
                            for k in naturais}
        elif fixas:
            # só colunas rígidas: espalha a sobra entre elas
            extra = max(sobra, 0.0) / len(fixas)
            fixas = {k: v + extra for k, v in fixas.items()}
        larguras.update(fixas)
        resultado = [larguras[c] for c, _t in colunas]
        if forcar and sum(resultado) > LARGURA_UTIL:
            fator = LARGURA_UTIL / sum(resultado)
            resultado = [w * fator for w in resultado]
        return resultado

    def _rigida_na_tabela(self, chave) -> bool:
        return chave in getattr(self, "_rigidas", set())

    def _quebra_em_linhas(self, titulo: str, largura: float, fonte: float) -> list[str]:
        """O título da coluna em até duas linhas que caibam nela."""
        cabe = largura - 1.0
        linhas, atual = [], ""
        for palavra in titulo.split():
            tentativa = (atual + " " + palavra).strip()
            if self._largura_do_texto(tentativa, fonte, negrito=True) <= cabe or not atual:
                atual = tentativa
            else:
                linhas.append(atual)
                atual = palavra
        if atual:
            linhas.append(atual)
        if len(linhas) > 2:
            linhas = [linhas[0], self._recorta(" ".join(linhas[1:]), largura,
                                               fonte=fonte, negrito=True)]
        return [self._recorta(l, largura, fonte=fonte, negrito=True) for l in linhas] or [""]

    def _recorta(self, texto: str, largura_mm: float, fonte=None, negrito=False) -> str:
        """Corta o texto que não cabe, MEDINDO — não estimando por letra.

        Sem isto o fpdf2 escreve por cima da coluna vizinha. Só vale para
        texto: número e data nunca passam por aqui."""
        tamanho = fonte or self.pdf.font_size_pt
        cabe = largura_mm - 1.2
        if self._largura_do_texto(texto, tamanho, negrito) <= cabe:
            self.pdf.set_font("Helvetica", "B" if negrito else "", tamanho)
            return texto
        while texto and self._largura_do_texto(texto + ".", tamanho, negrito) > cabe:
            texto = texto[:-1]
        self.pdf.set_font("Helvetica", "B" if negrito else "", tamanho)
        return (texto + ".") if texto else ""

    def _celula_inteira(self, largura, altura, texto, alinhamento, fonte):
        """Escreve sem cortar; se não couber, diminui a letra só aqui."""
        tamanho = fonte
        while tamanho > 4.0 and self._largura_do_texto(texto, tamanho) > largura - 1.0:
            tamanho -= 0.3
        self.pdf.set_font("Helvetica", "", tamanho)
        self.pdf.cell(largura, altura, texto, border=1, align=alinhamento, fill=True)
        self.pdf.set_font("Helvetica", "", fonte)

    def grafico_de_barras(self, g, titulo: str):
        """Desenha, com as MESMAS coordenadas que a tela usa no SVG.

        A geometria vem de `graficos.py`, que já é a fonte de verdade do
        desenho na página. Recalcular aqui abriria a porta para o gráfico do PDF
        e o da tela contarem histórias diferentes."""
        if not g or g.get("vazio"):
            return
        self.pdf.set_font("Helvetica", "B", 10)
        self.pdf.cell(0, 6, _texto(titulo), new_x="LMARGIN", new_y="NEXT")

        # o desenho vem em coordenadas de tela (900x320 px); aqui vira mm
        escala = LARGURA_UTIL / g["largura"]
        topo = self.pdf.get_y() + 2
        cores = {"b-receita": VERDE, "b-despesa": VERMELHO, "b-caixa": AZUL}

        def _x(v):
            return MARGEM + v * escala

        def _y(v):
            return topo + v * escala

        self.pdf.set_draw_color(225, 225, 225)
        self.pdf.set_line_width(0.15)
        self.pdf.set_font("Helvetica", "", 6)
        self.pdf.set_text_color(*TINTA_SUAVE)
        for marca in g["marcas"]:
            self.pdf.line(_x(g["margem_esq"]), _y(marca["y"]),
                          _x(g["largura"] - g["margem_dir"]), _y(marca["y"]))
            self.pdf.set_xy(MARGEM, _y(marca["y"]) - 1.6)
            self.pdf.cell(_x(g["margem_esq"]) - MARGEM - 1.5, 3.2,
                          _texto(marca["rotulo"]), align="R")

        for barra in g["barras"]:
            self.pdf.set_fill_color(*cores.get(barra["classe"], AZUL))
            self.pdf.rect(_x(barra["x"]), _y(barra["y"]),
                          barra["largura"] * escala,
                          max(barra["altura"] * escala, 0.2), style="F")

        self.pdf.set_draw_color(120, 120, 120)
        self.pdf.set_line_width(0.25)
        self.pdf.line(_x(g["margem_esq"]), _y(g["y_zero"]),
                      _x(g["largura"] - g["margem_dir"]), _y(g["y_zero"]))

        if g.get("pontos") and len(g["pontos"]) > 1:
            self.pdf.set_draw_color(*AZUL)
            anterior = g["pontos"][0]
            for ponto in g["pontos"][1:]:
                self.pdf.line(_x(anterior["x"]), _y(anterior["y"]),
                              _x(ponto["x"]), _y(ponto["y"]))
                anterior = ponto

        for rotulo in g["rotulos_x"]:
            largura_rotulo = 16
            self.pdf.set_xy(_x(rotulo["x"]) - largura_rotulo / 2,
                            _y(g["altura"] - 12))
            self.pdf.cell(largura_rotulo, 3.2, _texto(rotulo["texto"]), align="C")

        self.pdf.set_text_color(0, 0, 0)
        self.pdf.set_line_width(0.2)
        self.pdf.set_y(topo + g["altura"] * escala + 3)

        if g.get("legenda"):
            self.pdf.set_font("Helvetica", "", 7)
            for item in g["legenda"]:
                self.pdf.set_fill_color(*cores.get(item["classe"], AZUL))
                self.pdf.cell(3, 3, "", fill=True)
                self.pdf.cell(30, 3, " " + _texto(item["nome"]))
            self.pdf.ln(6)

    def bytes(self) -> bytes:
        saida = self.pdf.output()
        return bytes(saida) if not isinstance(saida, bytes) else saida


def montar(abas, titulo: str, subtitulo: str = "", resumo=None,
           observacao: str = "", graficos_iniciais=()) -> bytes:
    """O relatório inteiro. `abas` é o MESMO `(nome, colunas, linhas)` que o
    `excel.montar` recebe — é isso que garante que os dois batam."""
    relatorio = Relatorio(titulo, subtitulo)
    relatorio.capa(resumo or [], observacao)
    for grafico, titulo_grafico in graficos_iniciais:
        relatorio.grafico_de_barras(grafico, titulo_grafico)
    for nome, colunas, linhas in abas:
        relatorio.titulo_de_secao(nome)
        relatorio.tabela(colunas, linhas)
    return relatorio.bytes()


def nome_do_arquivo(assunto: str) -> str:
    return f"painel-{assunto}-{dt.datetime.now():%Y-%m-%d}.pdf"
