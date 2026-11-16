# -*- coding: utf-8 -*-
"""
O RELATÓRIO COMPLETO DO LOTE — um PDF só, para mandar à parceria (15/11/2026).

O dono: *"a gente vai lá nas solicitações, filtra, visualiza, aí faz um print da
tela, cola no WhatsApp, depois (…) baixa todos os anexos relacionados àqueles
registros e depois cola também no WhatsApp (…) Eu gostaria de um relatório (…) de
um lote (…) uma apresentação bonita, organizada, com os agrupamentos (…) de forma
resumida, de forma analítica, detalhada (…) e embutir nesse relatório todos os
anexos (…) de forma organizada, segregada, explicada (…) a data, quem decide sou eu
na hora de gerar."*

É DIFERENTE do "Relatório do Lote (PDF)" que já existe (`pdf.relatorio_do_lote`),
que é a lista para quem efetiva os pagamentos. Este é para quem RECEBE a prestação
de contas — o parceiro da obra — e por isso vai tudo dentro de um arquivo só:

  1. a CAPA: o total, os números do lote e os agrupamentos (por grupo do lote, por
     obra, por tipo de despesa, por forma e conta de pagamento, maiores credores);
  2. a RELAÇÃO ANALÍTICA: todas as SPs, grupo a grupo, cada linha clicável;
  3. o DETALHAMENTO: uma página por SP, com tudo o que se sabe dela, seguida dos
     ANEXOS dela, embutidos — cada página do anexo ganha uma faixa no alto dizendo
     de que SP é ("SP 1384831053 · Fulano · anexo 1 de 2 · página 1 de 3").

O ANEXO DA FOLHA (o dono, no mesmo dia): *"os registros que são gerados a partir
(…) da folha do pagamento (…) não têm anexo. Só que, dentro da descrição deles,
tem lá o relatório PDF (…) esse seria o anexo a colocar."* Por isso, SP sem anexo
cujo texto traz "Relatório (PDF): <link>" usa esse link como anexo
(`anexos_da_sp`). Os OUTROS links da descrição (planilha de pagamento, cadastro de
colaboradores, planilha de análise) não vão para o parceiro: no texto impresso
eles viram "(link interno, não incluído)" — são planilhas com dado bancário e
pessoal dos colaboradores, e o link daria acesso a elas.

MEMÓRIA E TEMPO, que são os limites de verdade aqui: cada anexo é baixado com teto
de tamanho (`MAXIMO_ANEXO`), o relatório inteiro tem teto de anexos
(`MAXIMO_TOTAL`) e o que passar disso sai listado com o link, sem embutir. Baixar
dezenas de anexos leva tempo — por isso a geração roda FORA do pedido da tela
(`iniciar`/`estado`), numa linha própria, e a tela acompanha o progresso. Sem
isso, um lote grande seguraria uma das 4 linhas do serviço por um minuto.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re
import tempfile
import threading
import time
import uuid
from decimal import Decimal

from .pdf import _texto

logger = logging.getLogger("analisesps.lote_relatorio_completo")

# Os tetos valem para o arquivo como ele vem (ANTES de otimizar): é o que ocupa
# memória enquanto baixa. Subiram em 16/11/2026 junto com a otimização — o anexo
# grande agora entra, só que reduzido.
MAXIMO_ANEXO = 40 * 1024 * 1024
MAXIMO_TOTAL = 150 * 1024 * 1024         # a soma dos anexos de um relatório

# A OTIMIZAÇÃO DOS ANEXOS (dono, 16/11/2026): *"existe anexos muito grandes em
# tamanho. Preciso dar uma otimizada neles antes de gerar o relatório completo."*
# O que pesa num anexo é quase sempre FOTO ou DIGITALIZAÇÃO: uma nota fotografada
# pelo celular vem com 4000 px e 5 MB, para ser lida numa tela de telefone. Cada
# imagem é reduzida para a resolução de leitura (DPI_ALVO, numa folha A4) e salva
# como JPEG na QUALIDADE abaixo — o texto continua legível e o arquivo cai a uma
# fração. Texto e desenho de PDF gerado por sistema (NF-e, boleto) não são imagem
# e não mudam.
DPI_ALVO = 130
DPI_A_PARTIR_DE = 160                    # imagem já abaixo disso fica como está
QUALIDADE_JPEG = 70
LADO_MAXIMO_DA_FOTO = 2000               # px: A4 a ~170 dpi
MAXIMO_PAGINAS_POR_ANEXO = 60
SEGUNDOS_DE_ESPERA = 60

# As cores da tela (`analisesps.css`), em RGB.
AZUL_FUNDO = (10, 27, 46)
AZUL = (18, 56, 92)
AZUL_CLARO = (30, 95, 148)
AMARELO = (232, 163, 23)
PAPEL = (242, 244, 248)
BORDA = (217, 223, 234)
TINTA = (20, 33, 58)
TINTA_SUAVE = (90, 104, 131)
VERDE = (27, 127, 59)
VERMELHO = (179, 38, 30)
AMBAR = (166, 91, 0)
CIANO = (14, 110, 140)
BRANCO = (255, 255, 255)

# Os selos de status, como na tela: (fundo, texto).
SELOS = {
    "pagar": ((253, 240, 213), AMBAR),
    "pago": ((228, 242, 232), VERDE),
    "cancelado": ((240, 240, 243), (107, 118, 144)),
    "agendado": ((230, 240, 247), CIANO),
    "falha agendar": ((251, 231, 229), VERMELHO),
    "vencida": ((251, 231, 229), VERMELHO),
    "vence hoje": ((253, 240, 213), AMBAR),
}

DIAS_DA_SEMANA = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira",
                  "sexta-feira", "sábado", "domingo"]


# ---------------------------------------------------------------------------
# OS ANEXOS DE CADA SP
# ---------------------------------------------------------------------------
_RELATORIO_NA_DESCRICAO = re.compile(r"relat[óo]rio[^:\n]{0,40}:\s*(https?://\S+)", re.I)
_URL = re.compile(r"https?://[^\s,;\"'<>]+")
_DRIVE_ID = re.compile(r"drive\.google\.com/(?:file/d/([\w-]+)|open\?id=([\w-]+)|"
                       r"uc\?[^ ]*?id=([\w-]+))")


def anexos_da_sp(sp: dict) -> list:
    """`[{"url", "rotulo", "origem"}]` — o que vai embutido no relatório.

    O anexo da SP, quando há. Sem anexo, o "Relatório (PDF): <link>" escrito na
    descrição (as SPs da folha de pagamento — pedido do dono, 15/11/2026)."""
    from .formatos import links_da_celula
    links = links_da_celula(sp.get("anexo_link"))
    if links:
        return [{"url": u, "rotulo": f"Anexo {i}" if len(links) > 1 else "Anexo",
                 "origem": "anexo"} for i, u in enumerate(links, start=1)]
    saida = []
    for achado in _RELATORIO_NA_DESCRICAO.finditer(str(sp.get("descricao") or "")):
        url = achado.group(1).rstrip(".)]")
        if url not in {a["url"] for a in saida}:
            saida.append({"url": url, "rotulo": "Relatório da folha (PDF)",
                          "origem": "descricao"})
    return saida


def descricao_para_o_parceiro(descricao: str, anexados: set) -> str:
    """A descrição sem os links: o que foi embutido vira "(anexado neste
    relatório)"; o resto, "(link interno, não incluído)"."""
    def trocar(m):
        url = m.group(0).rstrip(".)]")
        return ("(anexado neste relatório)" if url in anexados
                else "(link interno, não incluído)")
    return _URL.sub(trocar, str(descricao or "")).strip()


def _tipo_do_arquivo(conteudo: bytes) -> str:
    cabeca = conteudo[:1024]
    if b"%PDF" in cabeca:
        return "pdf"
    if cabeca.startswith(b"\x89PNG"):
        return "png"
    if cabeca.startswith(b"\xff\xd8"):
        return "jpg"
    if cabeca[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if cabeca[:4] == b"RIFF" and cabeca[8:12] == b"WEBP":
        return "webp"
    if cabeca[:2] == b"PK":
        return "office"                 # xlsx, docx, zip
    if cabeca.lstrip()[:1] == b"<":
        return "html"                   # em geral, a página de login do serviço
    return ""


def baixar_anexo(url: str) -> bytes:
    """O conteúdo do anexo, com teto de tamanho e de tempo. Levanta `RuntimeError`
    com a explicação em português.

    Link do Google Drive vai pela conta de serviço (`drive.baixar_arquivo`), que
    lê também o arquivo de acesso restrito — os relatórios da folha estão lá."""
    from .formatos import link_de_download
    endereco = str(url or "").strip()
    achado = _DRIVE_ID.search(endereco)
    if achado:
        from . import drive
        try:
            conteudo = drive.baixar_arquivo(next(g for g in achado.groups() if g))
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(str(e)) from e
        if len(conteudo) > MAXIMO_ANEXO:
            raise RuntimeError(f"o arquivo passa de {MAXIMO_ANEXO // (1024 * 1024)} MB")
        return conteudo

    import requests
    resposta = requests.get(link_de_download(endereco), stream=True,
                            timeout=SEGUNDOS_DE_ESPERA)
    if resposta.status_code >= 300:
        raise RuntimeError(f"o servidor do anexo respondeu {resposta.status_code}")
    pedacos, total = [], 0
    for pedaco in resposta.iter_content(64 * 1024):
        total += len(pedaco)
        if total > MAXIMO_ANEXO:
            resposta.close()
            raise RuntimeError(f"o arquivo passa de {MAXIMO_ANEXO // (1024 * 1024)} MB")
        pedacos.append(pedaco)
    return b"".join(pedacos)


def _otimizar_foto(conteudo: bytes, tipo: str) -> tuple:
    """A foto reduzida a `LADO_MAXIMO_DA_FOTO` e em JPEG. Fica a original quando a
    reduzida não sai menor, ou quando a imagem não abre (o PyMuPDF tenta depois)."""
    import io

    try:
        from PIL import Image, ImageOps
        with Image.open(io.BytesIO(conteudo)) as foto:
            foto = ImageOps.exif_transpose(foto)       # foto de celular deitada
            if foto.mode not in ("RGB", "L"):
                fundo = Image.new("RGB", foto.size, "white")
                if foto.mode in ("RGBA", "LA", "P"):
                    foto = foto.convert("RGBA")
                    fundo.paste(foto, mask=foto.split()[-1])
                else:
                    fundo.paste(foto.convert("RGB"))
                foto = fundo
            foto.thumbnail((LADO_MAXIMO_DA_FOTO, LADO_MAXIMO_DA_FOTO))
            saida = io.BytesIO()
            foto.save(saida, "JPEG", quality=QUALIDADE_JPEG, optimize=True)
    except Exception:  # noqa: BLE001 — sem otimizar, mas embutida
        logger.info("Relatório completo do lote: não consegui reduzir uma foto")
        return conteudo, tipo
    reduzida = saida.getvalue()
    return (reduzida, "jpg") if len(reduzida) < len(conteudo) else (conteudo, tipo)


def _otimizar_pdf(doc) -> None:
    """Reduz as imagens de dentro do PDF (digitalização, foto colada) à resolução
    de leitura. Em versão do PyMuPDF sem `rewrite_images`, segue sem otimizar."""
    if not hasattr(doc, "rewrite_images"):
        return
    try:
        doc.rewrite_images(dpi_threshold=DPI_A_PARTIR_DE, dpi_target=DPI_ALVO,
                           quality=QUALIDADE_JPEG, lossy=True, lossless=True,
                           bitonal=False, color=True, gray=True)
    except Exception:  # noqa: BLE001 — sem otimizar, mas embutido
        logger.exception("Relatório completo do lote: não consegui reduzir as imagens de um PDF")


def _preparar_anexo(conteudo: bytes):
    """(documento fitz pronto para embutir, número de páginas) ou levanta
    `RuntimeError` dizendo por que não dá para embutir."""
    import fitz
    tipo = _tipo_do_arquivo(conteudo)
    if tipo == "pdf":
        doc = fitz.open(stream=conteudo, filetype="pdf")
        if doc.needs_pass:
            doc.close()
            raise RuntimeError("o PDF tem senha")
        _otimizar_pdf(doc)
        return doc
    if tipo in ("png", "jpg", "gif", "webp"):
        conteudo, tipo = _otimizar_foto(conteudo, tipo)
        imagem = fitz.open(stream=conteudo, filetype=tipo)
        doc = fitz.open("pdf", imagem.convert_to_pdf())
        imagem.close()
        return doc
    if tipo == "office":
        raise RuntimeError("é planilha ou documento do Office, que não dá para embutir")
    if tipo == "html":
        raise RuntimeError("o link abriu uma página da internet, e não o arquivo "
                           "(o anexo pode exigir login)")
    raise RuntimeError("formato de arquivo que não dá para embutir")


# ---------------------------------------------------------------------------
# OS DADOS
# ---------------------------------------------------------------------------
_EXTRAS = ["id", "solicitacao", "dt_autorizacao_d", "resp_autorizacao",
           "status_anuencia", "parcela"]


def _extras(ids: list) -> dict:
    """O que a lista do lote não traz e o detalhamento mostra, numa consulta só."""
    if not ids:
        return {}
    from .db import consultar
    marcadores = ",".join(["?"] * len(ids))
    linhas = consultar(f"SELECT {', '.join(_EXTRAS)} FROM analisesps.sps "
                       f" WHERE id IN ({marcadores})", tuple(ids))
    return {str(l[0]): dict(zip(_EXTRAS, l)) for l in linhas}


def _obra(sp: dict) -> str:
    return " ".join(str(sp.get("centro_custo") or "").split()) or "(sem obra)"


def obras_do_lote(montado: dict) -> list:
    """`[(obra, quantas SPs, total)]`, da maior para a menor — as opções do filtro
    "só estas obras" na janela de gerar."""
    soma: dict = {}
    for g in montado.get("grupos") or []:
        for l in g.get("linhas") or []:
            o = _obra(l)
            q, t = soma.get(o, (0, Decimal("0")))
            soma[o] = (q + 1, t + Decimal(str(l.get("valor_num") or 0)))
    return sorted(((o, q, t) for o, (q, t) in soma.items()), key=lambda x: -x[2])


def _agrupar(linhas: list, chave) -> list:
    """`[{"nome", "quantidade", "total", "percentual"}]`, do maior total ao menor."""
    soma: dict = {}
    for l in linhas:
        nome = chave(l) or "(sem informação)"
        q, t = soma.get(nome, (0, Decimal("0")))
        soma[nome] = (q + 1, t + Decimal(str(l.get("valor_num") or 0)))
    total = sum((t for _, t in soma.values()), Decimal("0")) or Decimal("1")
    return [{"nome": n, "quantidade": q, "total": t,
             "percentual": (t * 100 / total).quantize(Decimal("0.1"))}
            for n, (q, t) in sorted(soma.items(), key=lambda x: -x[1][1])]


def montar_dados(montado: dict, *, titulo: str = "", para: str = "",
                 data_pagamento=None, obras=None, quem: str = "",
                 extras: dict | None = None) -> dict:
    """O que o PDF precisa, a partir do lote montado (`lote.montar`). `obras`:
    só estas (vazio = todas) — para mandar a um parceiro só o que é da obra dele."""
    filtro = {" ".join(str(o).split()) for o in (obras or []) if str(o).strip()}
    # ⚠️ A RELAÇÃO É POR OBRA, NÃO PELOS GRUPOS DO LOTE (dono, 15/11/2026, vendo a
    # prévia): *"tem um pagar amanhã, aí tem outro (…) folha e locação. Eu não entendi
    # essa divisão, sendo que a gente tá relacionando os pagamentos de um determinado
    # evento."* Os títulos do lote ("Pagar amanhã", "WhatsApp1") organizam o trabalho
    # aqui dentro e não dizem nada a quem recebe; a obra é o que a parceria reconhece.
    # A mesma SP em dois grupos do lote entra uma vez só.
    vistas, todas = set(), []
    for g in montado.get("grupos") or []:
        for l in g.get("linhas") or []:
            if str(l["id"]) in vistas or (filtro and _obra(l) not in filtro):
                continue
            vistas.add(str(l["id"]))
            todas.append(dict(l))
    por_obra: dict = {}
    for l in todas:
        por_obra.setdefault(_obra(l), []).append(l)
    grupos = [{"titulo": obra, "linhas": linhas,
               "total": sum((Decimal(str(l.get("valor_num") or 0)) for l in linhas),
                            Decimal("0"))}
              for obra, linhas in por_obra.items()]
    grupos.sort(key=lambda g: -g["total"])
    todas = [l for g in grupos for l in g["linhas"]]
    if extras is None:
        extras = _extras([str(l["id"]) for l in todas])
    numero = 0
    for g in grupos:
        for l in g["linhas"]:
            numero += 1
            l.update({k: v for k, v in (extras.get(str(l["id"])) or {}).items() if k != "id"})
            l["_n"] = numero
            l["_grupo"] = g["titulo"]
            l["_anexos"] = anexos_da_sp(l)
    total = sum((g["total"] for g in grupos), Decimal("0"))
    hoje = dt.date.today()
    hoje_sp = [l for l in todas if l.get("vence_hoje")]
    return {
        "titulo": titulo.strip(), "para": para.strip(), "quem": quem,
        "data_pagamento": data_pagamento, "obras_filtro": sorted(filtro),
        "grupos": grupos, "linhas": todas, "total": total,
        "quantidade": len(todas),
        "credores": len({(l.get("documento") or l.get("credor") or "") for l in todas}),
        "obras": _agrupar(todas, _obra),
        "por_tipo": _agrupar(todas, lambda l: l.get("tipo_despesa")),
        "por_forma": _agrupar(todas, lambda l: l.get("forma_pagamento")),
        "por_conta": _agrupar(todas, lambda l: l.get("conta")),
        "por_credor": _agrupar(todas, lambda l: l.get("credor"))[:10],
        "vencidas": [l for l in todas if l.get("vencido")],
        "vence_hoje": hoje_sp,
        "sem_anexo": [l for l in todas if not l["_anexos"]],
        "gerado_em": hoje,
    }


# ---------------------------------------------------------------------------
# O PDF
# ---------------------------------------------------------------------------
def _moeda(valor) -> str:
    from .formatos import moeda
    return "R$ " + (moeda(valor) or "0,00")


def _data(valor) -> str:
    from .formatos import data_br
    return data_br(valor) or "-"


def _quando(data) -> str:
    """16/11/2026 (segunda-feira)."""
    if not data:
        return ""
    return f"{data.strftime('%d/%m/%Y')} ({DIAS_DA_SEMANA[data.weekday()]})"


class _Relatorio:
    """O corpo do relatório, desenhado com o `fpdf2` (que já está no serviço). Os
    anexos entram depois, com o PyMuPDF (`montar_pdf`)."""

    def __init__(self, dados: dict):
        from fpdf import FPDF
        self.d = dados
        self.pdf = FPDF(orientation="P", format="A4")
        self.pdf.set_auto_page_break(auto=True, margin=16)
        self.pdf.set_margins(12, 14, 12)
        self.largura = self.pdf.w - 24
        # Os links internos ficam ANOTADOS e entram no fim (`montar_pdf`), já com a
        # numeração final: o `fpdf2` não aceita link para página que ainda não
        # existe, e os anexos mudam a posição das páginas.
        self.links: list = []          # (página do corpo, x, y, w, h em mm, alvo)
        self.primeira_pagina_da_sp: dict = {}
        self.ultima_pagina_da_sp: dict = {}      # id -> índice (0) da última página
        self.paginas_da_relacao = (0, 0)

    # --- peças -----------------------------------------------------------
    def cor(self, rgb, fundo=False, linha=False):
        if fundo:
            self.pdf.set_fill_color(*rgb)
        if linha:
            self.pdf.set_draw_color(*rgb)
        if not fundo and not linha:
            self.pdf.set_text_color(*rgb)

    def fonte(self, tamanho, estilo=""):
        self.pdf.set_font("Helvetica", estilo, tamanho)

    def escrever(self, x, y, w, h, texto, tamanho=9, estilo="", cor=TINTA, align="L",
                 link=None):
        self.fonte(tamanho, estilo)
        self.cor(cor)
        self.pdf.set_xy(x, y)
        self.pdf.cell(w, h, _texto(texto), align=align)
        if link:
            self.marcar(x, y, w, h, link)

    def marcar(self, x, y, w, h, alvo):
        """Anota um link: `alvo` é "relacao" ou "sp:<número>"."""
        self.links.append((self.pdf.page_no() - 1, x, y, w, h, alvo))

    def cortar(self, texto, largura) -> str:
        texto = _texto(texto)
        if self.pdf.get_string_width(texto) <= largura:
            return texto
        while texto and self.pdf.get_string_width(texto + "...") > largura:
            texto = texto[:-1]
        return texto + "..."

    def caixa(self, x, y, w, h, fundo=BRANCO, borda=BORDA, raio=2.2):
        self.cor(fundo, fundo=True)
        self.cor(borda, linha=True)
        self.pdf.set_line_width(0.25)
        self.pdf.rect(x, y, w, h, style="DF", round_corners=True, corner_radius=raio)

    def selo(self, x, y, texto, chave=None) -> float:
        """Um selo arredondado; devolve a largura usada."""
        fundo, tinta = SELOS.get((chave or texto or "").lower(), ((237, 239, 243), TINTA_SUAVE))
        self.fonte(6.8, "B")
        w = self.pdf.get_string_width(_texto(texto)) + 5
        self.caixa(x, y, w, 4.6, fundo=fundo, borda=fundo, raio=2.2)
        self.escrever(x, y + 0.1, w, 4.4, texto, 6.8, "B", tinta, "C")
        return w + 1.5

    def secao(self, texto, detalhe=""):
        if self.pdf.get_y() > self.pdf.h - 45:
            self.pdf.add_page()
        y = self.pdf.get_y() + 3
        self.cor(AMARELO, fundo=True)
        self.pdf.rect(12, y + 0.6, 1.4, 5.2, style="F")
        self.escrever(15.5, y, 120, 6.4, texto, 11.5, "B", AZUL)
        if detalhe:
            self.escrever(12, y, self.largura, 6.4, detalhe, 8, "", TINTA_SUAVE, "R")
        self.pdf.set_y(y + 8.5)

    # --- capa ------------------------------------------------------------
    def capa(self):
        d = self.d
        pdf = self.pdf
        pdf.add_page()
        self.cor(AZUL_FUNDO, fundo=True)
        pdf.rect(0, 0, pdf.w, 50, style="F")
        self.cor(AMARELO, fundo=True)
        pdf.rect(0, 50, pdf.w, 1.4, style="F")

        self.escrever(12, 9, 120, 5, "BWS CONSTRUÇÕES  ·  RELATÓRIO DE PAGAMENTOS",
                      7.5, "B", AMARELO)
        titulo = d["titulo"] or (f"Pagamentos de {d['data_pagamento'].strftime('%d/%m/%Y')}"
                                 if d["data_pagamento"] else "Pagamentos do lote")
        self.fonte(19, "B")
        self.escrever(12, 15, 125, 10, self.cortar(titulo, 125), 19, "B", BRANCO)
        obras = ", ".join(o["nome"] for o in d["obras"])
        linhas = [x for x in [
            ("Pagamento previsto: " + _quando(d["data_pagamento"])) if d["data_pagamento"] else "",
            ("Para: " + d["para"]) if d["para"] else "",
            ("Obra: " if len(d["obras"]) == 1 else "Obras: ") + obras] if x]
        for i, texto in enumerate(linhas):
            self.fonte(9 if i == 0 else 8.5)
            self.escrever(12, 26.5 + i * 5, 125, 5, self.cortar(texto, 125),
                          9 if i == 0 else 8.5, "", (214, 224, 238))
        from .horario import agora
        self.escrever(12, 43, 125, 4.5,
                      f"Gerado em {agora().strftime('%d/%m/%Y às %H:%M')}"
                      + (f" por {d['quem']}" if d["quem"] else ""), 7, "", (150, 166, 190))

        # O total, à direita, em destaque.
        self.caixa(140, 12, 58, 30, fundo=(20, 45, 72), borda=(40, 72, 108), raio=3)
        self.escrever(140, 16, 58, 4, "TOTAL A PAGAR", 7, "B", (170, 186, 210), "C")
        self.fonte(15, "B")
        self.escrever(140, 22, 58, 9, _moeda(d["total"]), 15, "B", AMARELO, "C")
        self.escrever(140, 32, 58, 4, f"{d['quantidade']} pagamento(s)", 7.5, "",
                      (214, 224, 238), "C")

        # Os números do lote.
        y = 58
        cartoes = [("Pagamentos", str(d["quantidade"])),
                   ("Credores", str(d["credores"])),
                   ("Obras", str(len(d["obras"]))),
                   ("Com documento", f"{d['quantidade'] - len(d['sem_anexo'])} de {d['quantidade']}")]
        w = (self.largura - 3 * 4) / 4
        for i, (rotulo, valor) in enumerate(cartoes):
            x = 12 + i * (w + 4)
            self.caixa(x, y, w, 19)
            self.escrever(x + 4, y + 3, w - 8, 4, rotulo.upper(), 6.8, "B", TINTA_SUAVE)
            self.escrever(x + 4, y + 8.5, w - 8, 8, valor, 15, "B", TINTA)
        self.pdf.set_y(y + 23)

        self.atencao()
        self.quadro("Por obra", d["obras"])
        self.quadro("Por tipo de despesa", d["por_tipo"])
        # Forma e conta de pagamento lado a lado: são listas curtas.
        tem_conta = any(c["nome"] != "(sem informação)" for c in d["por_conta"])
        self.lado_a_lado(("Por forma de pagamento", d["por_forma"]),
                         ("Por conta de pagamento", d["por_conta"]) if tem_conta else None)
        # Com poucos credores, o quadro repetiria a relação da página seguinte.
        if d["credores"] > 5:
            self.quadro("Maiores credores", d["por_credor"],
                        detalhe="os 10 de maior valor" if d["credores"] > 10 else "")
        self.escrever(12, self.pdf.get_y() + 2, self.largura, 5,
                      "Relação de todos os pagamentos, e o detalhe de cada um com os "
                      "documentos, nas páginas seguintes.  Ir para a relação  >",
                      7.5, "I", AZUL_CLARO, "R", link="relacao")

    def atencao(self):
        d = self.d
        avisos = []
        if d["vencidas"]:
            avisos.append((VERMELHO, f"{len(d['vencidas'])} pagamento(s) com vencimento "
                           f"já passado ({_moeda(sum(Decimal(str(l.get('valor_num') or 0)) for l in d['vencidas']))})."))
        if d["vence_hoje"]:
            avisos.append((AMBAR, f"{len(d['vence_hoje'])} pagamento(s) vencem hoje."))
        if d["sem_anexo"]:
            avisos.append((AMBAR, f"{len(d['sem_anexo'])} pagamento(s) sem documento "
                           "anexado: " + ", ".join(str(l['id']) for l in d['sem_anexo'][:8])
                           + ("..." if len(d["sem_anexo"]) > 8 else "") + "."))
        if not avisos:
            return
        y = self.pdf.get_y()
        h = 6 + 4.6 * len(avisos)
        self.caixa(12, y, self.largura, h, fundo=(254, 247, 234), borda=(240, 214, 160))
        self.escrever(16, y + 1.6, 100, 4.5, "PONTOS DE ATENÇÃO", 7, "B", AMBAR)
        for i, (cor, texto) in enumerate(avisos):
            yy = y + 6 + i * 4.6
            self.cor(cor, fundo=True)
            self.pdf.ellipse(16.3, yy + 1.4, 1.6, 1.6, style="F")
            self.fonte(8)
            self.escrever(19.5, yy, self.largura - 12, 4.4,
                          self.cortar(texto, self.largura - 12), 8, "", TINTA)
        self.pdf.set_y(y + h + 2)

    def lado_a_lado(self, a, b):
        """Dois quadros curtos, meia largura cada: nome, quantos, valor e %."""
        if b is None:
            self.quadro(*a)
            return
        maior = max(len(a[1]), len(b[1]))
        if self.pdf.get_y() + 14 + 5.4 * min(maior, 4) > self.pdf.h - 16:
            self.pdf.add_page()
        y0 = self.pdf.get_y() + 3
        fim = y0
        w = (self.largura - 8) / 2
        for k, (titulo, itens) in enumerate((a, b)):
            x0 = 12 + k * (w + 8)
            self.cor(AMARELO, fundo=True)
            self.pdf.rect(x0, y0 + 0.6, 1.4, 5.2, style="F")
            self.escrever(x0 + 3.5, y0, w, 6.4, titulo, 11.5, "B", AZUL)
            y = y0 + 8.5
            self.escrever(x0 + w - 54, y, 10, 4, "QTD.", 6.5, "B", TINTA_SUAVE, "R")
            self.escrever(x0 + w - 41, y, 28, 4, "VALOR", 6.5, "B", TINTA_SUAVE, "R")
            self.escrever(x0 + w - 12, y, 12, 4, "%", 6.5, "B", TINTA_SUAVE, "R")
            y += 4.5
            for n, item in enumerate(itens):
                if n % 2 == 0:
                    self.cor(PAPEL, fundo=True)
                    self.pdf.rect(x0, y, w, 5.4, style="F")
                self.fonte(8)
                self.escrever(x0 + 2, y + 0.5, w - 58, 4.4,
                              self.cortar(item["nome"], w - 60), 8, "", TINTA)
                self.escrever(x0 + w - 54, y + 0.5, 10, 4.4, str(item["quantidade"]), 8, "",
                              TINTA_SUAVE, "R")
                self.escrever(x0 + w - 41, y + 0.5, 28, 4.4, _moeda(item["total"]), 8, "B",
                              TINTA, "R")
                self.escrever(x0 + w - 12, y + 0.5, 12, 4.4,
                              f"{item['percentual']}".replace(".", ",") + "%", 7, "",
                              TINTA_SUAVE, "R")
                y += 5.4
            fim = max(fim, y)
        self.pdf.set_y(fim + 2)

    def quadro(self, titulo, itens, detalhe=""):
        """Um agrupamento: nome, quantos, barra, valor e %."""
        if not itens:
            return
        altura = 5.2
        # Um quadro curto não se parte entre duas páginas.
        if self.pdf.get_y() + 14 + altura * min(len(itens), 12) > self.pdf.h - 16:
            self.pdf.add_page()
        self.secao(titulo, detalhe)
        maior = max((i["total"] for i in itens), default=Decimal("1")) or Decimal("1")
        x_nome, w_nome = 12, 70
        x_q, w_q = 82, 14
        x_barra, w_barra = 99, 52
        x_valor, w_valor = 152, 32
        x_pct, w_pct = 184, 14
        self.fonte(6.8, "B")
        y = self.pdf.get_y()
        for x, w, t, a in ((x_nome, w_nome, "", "L"), (x_q, w_q, "QTD.", "R"),
                           (x_valor, w_valor, "VALOR", "R"), (x_pct, w_pct, "%", "R")):
            self.escrever(x, y, w, 4, t, 6.5, "B", TINTA_SUAVE, a)
        y += 4.5
        for n, item in enumerate(itens):
            if y + altura > self.pdf.h - 16:
                self.pdf.add_page()
                y = self.pdf.get_y()
            if n % 2 == 0:
                self.cor(PAPEL, fundo=True)
                self.pdf.rect(12, y, self.largura, altura, style="F")
            self.fonte(8.2)
            self.escrever(x_nome + 2, y + 0.6, w_nome - 2, altura - 1,
                          self.cortar(item["nome"], w_nome - 4), 8.2, "", TINTA)
            self.escrever(x_q, y + 0.6, w_q, altura - 1, str(item["quantidade"]), 8.2, "",
                          TINTA_SUAVE, "R")
            proporcao = float(item["total"] / maior) if maior else 0
            self.cor((225, 231, 241), fundo=True)
            self.pdf.rect(x_barra, y + 1.9, w_barra, 1.8, style="F", round_corners=True,
                          corner_radius=0.9)
            if proporcao > 0:
                self.cor(AZUL_CLARO, fundo=True)
                self.pdf.rect(x_barra, y + 1.9, max(1.2, w_barra * proporcao), 1.8,
                              style="F", round_corners=True, corner_radius=0.9)
            self.escrever(x_valor, y + 0.6, w_valor, altura - 1, _moeda(item["total"]),
                          8.2, "B", TINTA, "R")
            self.escrever(x_pct, y + 0.6, w_pct, altura - 1,
                          f"{item['percentual']}".replace(".", ",") + "%", 7.5, "",
                          TINTA_SUAVE, "R")
            y += altura
        self.pdf.set_y(y + 2)

    # --- relação analítica ----------------------------------------------
    # Sem a coluna da obra: a relação já vem separada por obra.
    COLUNAS = [("#", 7, "R"), ("SP", 20, "L"), ("VENC.", 15, "L"),
               ("CREDOR / DESCRIÇÃO", 75, "L"),
               ("TIPO DE DESPESA", 30, "L"), ("FORMA", 17, "L"), ("VALOR", 22, "R")]

    def relacao(self):
        pdf = self.pdf
        pdf.add_page()
        inicio = pdf.page_no() - 1
        self.secao("Relação dos pagamentos",
                   f"{self.d['quantidade']} pagamento(s) · {_moeda(self.d['total'])}")
        for g in self.d["grupos"]:
            self.cabecalho_do_grupo(g)
            self.cabecalho_da_relacao()
            for l in g["linhas"]:
                self.linha_da_relacao(l)
            pdf.set_y(pdf.get_y() + 3)
        self.escrever(12, pdf.get_y(), self.largura, 4,
                      "Toque numa linha para ir ao detalhe e aos documentos daquele pagamento.",
                      7, "I", TINTA_SUAVE)
        self.paginas_da_relacao = (inicio, pdf.page_no() - 1)

    def cabecalho_do_grupo(self, g):
        if self.pdf.get_y() > self.pdf.h - 40:
            self.pdf.add_page()
        y = self.pdf.get_y()
        self.caixa(12, y, self.largura, 8, fundo=AZUL, borda=AZUL, raio=1.8)
        self.fonte(9.5, "B")
        self.escrever(15, y + 1.5, 120, 5, self.cortar(g["titulo"], 120), 9.5, "B", BRANCO)
        self.escrever(12, y + 1.5, self.largura - 3, 5,
                      f"{len(g['linhas'])} pagamento(s)   ·   {_moeda(g['total'])}",
                      8.5, "B", AMARELO, "R")
        self.pdf.set_y(y + 9.5)

    def cabecalho_da_relacao(self):
        y, x = self.pdf.get_y(), 12
        for titulo, w, a in self.COLUNAS:
            self.escrever(x + (0 if a == "R" else 0.8), y, w - 0.8, 4, titulo, 6.3, "B",
                          TINTA_SUAVE, a)
            x += w
        self.cor(BORDA, linha=True)
        self.pdf.line(12, y + 4.4, 12 + self.largura, y + 4.4)
        self.pdf.set_y(y + 5)

    def linha_da_relacao(self, l):
        altura = 9
        if self.pdf.get_y() + altura > self.pdf.h - 16:
            self.pdf.add_page()
            self.cabecalho_da_relacao()
        y = self.pdf.get_y()
        if l["_n"] % 2 == 0:
            self.cor(PAPEL, fundo=True)
            self.pdf.rect(12, y, self.largura, altura, style="F")
        x = 12
        valores = [str(l["_n"]), str(l["id"]), _data(l.get("vencimento_d"))[:5] + "/"
                   + _data(l.get("vencimento_d"))[8:10] if l.get("vencimento_d") else "-",
                   None, l.get("tipo_despesa") or "-",
                   l.get("forma_pagamento") or "-", _moeda(l.get("valor_num"))]
        for (titulo, w, a), v in zip(self.COLUNAS, valores):
            if titulo == "CREDOR / DESCRIÇÃO":
                self.fonte(7.8, "B")
                self.escrever(x + 0.8, y + 0.9, w - 1.6, 3.8,
                              self.cortar(l.get("credor") or "-", w - 2), 7.8, "B", TINTA)
                self.fonte(6.6)
                self.escrever(x + 0.8, y + 4.6, w - 1.6, 3.4,
                              self.cortar(" ".join(str(l.get("descricao") or "").split()), w - 2),
                              6.6, "", TINTA_SUAVE)
            elif titulo == "SP":
                self.escrever(x + 0.8, y + 0.9, w - 1, 3.8, v, 7.6, "", AZUL_CLARO)
                n = len(l["_anexos"])
                self.escrever(x + 0.8, y + 4.6, w - 1, 3.4,
                              f"{n} documento(s)" if n else "sem documento", 5.8, "",
                              TINTA_SUAVE if n else AMBAR)
            else:
                estilo = "B" if titulo == "VALOR" else ""
                tamanho = 7.8 if titulo == "VALOR" else 7.2
                self.fonte(tamanho, estilo)
                cor = VERMELHO if (titulo == "VENC." and l.get("vencido")) else TINTA
                self.escrever(x + (0 if a == "R" else 0.8), y + 2.6, w - 1.2, 3.8,
                              self.cortar(v, w - 1.6), tamanho, estilo, cor, a)
            x += w
        self.marcar(12, y, self.largura, altura, f"sp:{l['id']}")
        self.pdf.set_y(y + altura)

    # --- detalhamento ----------------------------------------------------
    def detalhe(self, l, anexos: list):
        """A página de uma SP. `anexos`: [{"rotulo", "paginas" ou "erro", "url"}]."""
        pdf = self.pdf
        pdf.add_page()
        self.primeira_pagina_da_sp[str(l["id"])] = pdf.page_no() - 1
        d = self.d
        self.escrever(12, 9, 140, 4, f"PAGAMENTO {l['_n']} DE {d['quantidade']}  ·  "
                      f"OBRA {l['_grupo']}".upper(), 6.8, "B", TINTA_SUAVE)
        self.escrever(12, 9, self.largura, 4, "< voltar à relação", 7, "", AZUL_CLARO,
                      "R", link="relacao")

        # O cartão do alto: credor, documento, valor e os selos.
        y = 15
        self.caixa(12, y, self.largura, 30, fundo=BRANCO, borda=BORDA, raio=3)
        self.cor(AZUL, fundo=True)
        self.pdf.rect(12, y + 3, 1.6, 24, style="F")
        self.fonte(14, "B")
        self.escrever(17, y + 4, 120, 7, self.cortar(l.get("credor") or "-", 122), 14, "B", TINTA)
        self.escrever(17, y + 11.5, 120, 4.5, ("CPF/CNPJ " + str(l.get("documento")))
                      if l.get("documento") else "CPF/CNPJ não informado", 8.5, "", TINTA_SUAVE)
        self.escrever(140, y + 4, 54, 4, "VALOR", 6.8, "B", TINTA_SUAVE, "R")
        self.escrever(140, y + 8.5, 54, 8, _moeda(l.get("valor_num")), 16, "B", AZUL, "R")
        x = 17
        status = str(l.get("status_pgt") or "").strip()
        if status:
            x += self.selo(x, y + 19.5, status.upper(), status)
        agend = str(l.get("status_agend") or "").strip()
        if agend and agend.lower() not in ("", "-"):
            x += self.selo(x, y + 19.5, agend.upper(), agend)
        if l.get("vencido"):
            x += self.selo(x, y + 19.5, "VENCIDA", "vencida")
        elif l.get("vence_hoje"):
            x += self.selo(x, y + 19.5, "VENCE HOJE", "vence hoje")
        self.escrever(140, y + 19.5, 54, 4.6, f"SP {l['id']}", 8, "B", TINTA_SUAVE, "R")
        pdf.set_y(y + 34)

        # Os campos, em duas colunas.
        autorizacao = " · ".join(x for x in [
            str(l.get("resp_autorizacao") or "").strip(),
            _data(l.get("dt_autorizacao_d")) if l.get("dt_autorizacao_d") else "",
            str(l.get("status_aut") or "").strip()] if x)
        anuencia = " · ".join(x for x in [str(l.get("anuente") or "").strip(),
                                          str(l.get("status_anuencia") or "").strip()] if x)
        esquerda = [("Vencimento", _data(l.get("vencimento_d"))),
                    ("Solicitação", _data(l.get("solicitacao_d"))),
                    ("Obra", _obra(l)),
                    ("Projeto", l.get("projeto")),
                    ("Tipo de despesa", l.get("tipo_despesa")),
                    ("Parcela", l.get("parcela")),
                    ("Nota fiscal", l.get("nf")),
                    ("Pedido", l.get("pedido"))]
        direita = [("Forma de pagamento", l.get("forma_pagamento")),
                   ("Conta de pagamento", l.get("conta")),
                   ("Solicitante", l.get("responsavel")),
                   ("Autorização", autorizacao),
                   ("Anuência", anuencia),
                   ("Validação", l.get("validacao")),
                   ("Pago em", _data(l.get("data_pagamento_d")) if l.get("data_pagamento_d") else "")]
        self.campos(esquerda, direita)

        info = str(l.get("info_pgt") or "").strip()
        barras = str(l.get("codigo_barras") or "").strip()
        if info or barras:
            self.bloco("Dados para pagamento", info, mono=barras)

        anexados = {a["url"] for a in anexos if a.get("paginas")}
        descricao = descricao_para_o_parceiro(l.get("descricao"), anexados)
        if descricao:
            self.bloco("Descrição", descricao)
        self.bloco_dos_anexos(anexos)
        self.ultima_pagina_da_sp[str(l["id"])] = pdf.page_no() - 1

    def campos(self, esquerda, direita):
        y0 = self.pdf.get_y()
        w = (self.largura - 6) / 2
        for coluna, itens in ((0, esquerda), (1, direita)):
            y = y0
            x = 12 + coluna * (w + 6)
            for rotulo, valor in itens:
                valor = str(valor or "").strip()
                if not valor or valor == "-":
                    continue
                self.escrever(x, y, w, 3.6, rotulo.upper(), 6.2, "B", TINTA_SUAVE)
                self.fonte(8.8)
                self.escrever(x, y + 3.6, w, 4.6, self.cortar(valor, w), 8.8, "", TINTA)
                self.cor(BORDA, linha=True)
                self.pdf.line(x, y + 9, x + w, y + 9)
                y += 10.5
            if coluna == 0:
                fim_esq = y
        self.pdf.set_y(max(fim_esq, y) + 2)

    def bloco(self, titulo, texto, mono=""):
        """Uma caixa com título e texto livre (quebra em várias linhas)."""
        pdf = self.pdf
        self.fonte(8.4)
        linhas_texto = pdf.multi_cell(self.largura - 8, 4.2, _texto(texto), dry_run=True,
                                      output="LINES") if texto else []
        altura = 7 + 4.2 * len(linhas_texto) + (9 if mono else 0) + 2
        if pdf.get_y() + min(altura, 60) > pdf.h - 16:
            pdf.add_page()
        y = pdf.get_y()
        if y + altura <= pdf.h - 16:
            self.caixa(12, y, self.largura, altura, fundo=PAPEL, borda=PAPEL, raio=2.2)
        self.escrever(16, y + 2, 120, 4, titulo.upper(), 6.8, "B", AZUL)
        pdf.set_xy(16, y + 7)
        if texto:
            self.fonte(8.4)
            self.cor(TINTA)
            pdf.multi_cell(self.largura - 8, 4.2, _texto(texto), new_x="LMARGIN",
                           new_y="NEXT")
        if mono:
            pdf.set_xy(16, pdf.get_y() + 1)
            self.escrever(16, pdf.get_y(), 60, 3.6, "CÓDIGO DE BARRAS / LINHA DIGITÁVEL",
                          6, "B", TINTA_SUAVE)
            pdf.set_font("Courier", "B", 8.5)
            self.cor(TINTA)
            pdf.set_xy(16, pdf.get_y() + 3.8)
            pdf.multi_cell(self.largura - 8, 4, _texto(mono), new_x="LMARGIN", new_y="NEXT")
        pdf.set_y(max(pdf.get_y(), y + altura) + 3)

    def bloco_dos_anexos(self, anexos):
        pdf = self.pdf
        if pdf.get_y() > pdf.h - 40:
            pdf.add_page()
        y = pdf.get_y()
        self.escrever(12, y, 120, 4.5, "DOCUMENTOS DESTE PAGAMENTO", 6.8, "B", AZUL)
        y += 5.5
        if not anexos:
            self.caixa(12, y, self.largura, 8, fundo=(254, 247, 234), borda=(240, 214, 160))
            self.escrever(16, y + 1.8, self.largura - 8, 4.5,
                          "Este pagamento não tem documento anexado.", 8.2, "", AMBAR)
            pdf.set_y(y + 11)
            return
        for a in anexos:
            if a.get("paginas"):
                cor, texto = VERDE, (f"{a['rotulo']} - {a['paginas']} página(s), "
                                     "a seguir neste relatório")
            else:
                cor, texto = VERMELHO, (f"{a['rotulo']} - não deu para embutir: "
                                        f"{a.get('erro') or 'motivo desconhecido'}")
            self.caixa(12, y, self.largura, 7.4, fundo=BRANCO, borda=BORDA, raio=1.8)
            self.cor(cor, fundo=True)
            pdf.ellipse(15.5, y + 2.9, 1.7, 1.7, style="F")
            self.fonte(8.2)
            self.escrever(19, y + 1.5, self.largura - 10, 4.5,
                          self.cortar(texto, self.largura - 10), 8.2, "", TINTA)
            y += 8.6
        pdf.set_y(y + 2)

    def bytes(self) -> bytes:
        return bytes(self.pdf.output())


def _faixa_do_anexo(pagina, l: dict, anexo: dict, i_anexo: int, n_anexos: int,
                    i_pag: int, n_pag: int):
    """A faixa no alto de cada página de anexo: de que pagamento ela é."""
    import fitz
    w = pagina.rect.width
    faixa = fitz.Rect(0, 0, w, 34)
    pagina.draw_rect(faixa, color=None, fill=tuple(c / 255 for c in AZUL_FUNDO))
    pagina.draw_rect(fitz.Rect(0, 34, w, 36), color=None, fill=tuple(c / 255 for c in AMARELO))
    credor = _texto(l.get("credor") or "-")[:60]
    pagina.insert_text((24, 15), _texto(f"PAGAMENTO {l['_n']}  ·  SP {l['id']}  ·  {credor}"),
                       fontsize=9, fontname="hebo", color=(1, 1, 1))
    rotulo = anexo["rotulo"] + (f" {i_anexo} de {n_anexos}" if n_anexos > 1
                                and not anexo["rotulo"][-1].isdigit() else "")
    pagina.insert_text((24, 27), _texto(f"{rotulo}  ·  página {i_pag} de {n_pag}  ·  "
                                        f"{_moeda(l.get('valor_num'))}"),
                       fontsize=7.5, fontname="helv", color=(0.84, 0.88, 0.93))
    pagina.insert_text((w - 112, 20), "< voltar à relação", fontsize=7.5, fontname="helv",
                       color=tuple(c / 255 for c in AMARELO))


def _paginas_do_anexo(destino, origem, l, anexo, i_anexo, n_anexos) -> int:
    """Põe as páginas de `origem` em `destino` (no fim), cada uma numa folha A4
    com a faixa no alto. Devolve quantas páginas entraram."""
    import fitz
    n = min(len(origem), MAXIMO_PAGINAS_POR_ANEXO)
    for i in range(n):
        fonte = origem[i].rect
        deitada = fonte.width > fonte.height
        largura, altura = (842, 595) if deitada else (595, 842)
        pagina = destino.new_page(width=largura, height=altura)
        area = fitz.Rect(18, 44, largura - 18, altura - 26)
        pagina.show_pdf_page(area, origem, i, keep_proportion=True)
        _faixa_do_anexo(pagina, l, anexo, i_anexo, n_anexos, i + 1, n)
    return n


def montar_pdf(dados: dict, baixar=None, progresso=None, com_anexos: bool = True) -> bytes:
    """O relatório inteiro. `baixar(url) -> bytes` (o padrão é `baixar_anexo`; o
    teste passa um falso). `progresso(feitos, total, texto)` acompanha os downloads."""
    import fitz
    baixar = baixar or baixar_anexo
    progresso = progresso or (lambda *a: None)

    # 1. Baixa e prepara os anexos ANTES de desenhar: o detalhe de cada SP diz
    #    quantas páginas vêm a seguir, ou por que um documento ficou de fora.
    preparados: dict = {}            # id -> [(anexo, doc fitz | None)]
    resumo: dict = {}                # id -> [{"rotulo", "url", "paginas"|"erro"}]
    total_baixado = 0
    pedidos = [(l, a) for l in dados["linhas"] for a in (l["_anexos"] if com_anexos else [])]
    for n, (l, a) in enumerate(pedidos, start=1):
        progresso(n - 1, len(pedidos), f"Baixando o documento da SP {l['id']}")
        item = {"rotulo": a["rotulo"], "url": a["url"]}
        doc = None
        try:
            if total_baixado > MAXIMO_TOTAL:
                raise RuntimeError("o relatório já passou do tamanho máximo de documentos")
            conteudo = baixar(a["url"])
            total_baixado += len(conteudo)
            doc = _preparar_anexo(conteudo)
            item["paginas"] = min(len(doc), MAXIMO_PAGINAS_POR_ANEXO)
            if len(doc) > MAXIMO_PAGINAS_POR_ANEXO:
                item["rotulo"] += f" (primeiras {MAXIMO_PAGINAS_POR_ANEXO} de {len(doc)} páginas)"
        except Exception as e:  # noqa: BLE001 — um anexo ruim não derruba o relatório
            logger.info("Relatório completo do lote: anexo da SP %s ficou de fora: %s",
                        l["id"], e)
            item["erro"] = str(e)[:160]
        preparados.setdefault(str(l["id"]), []).append((item, doc))
        resumo.setdefault(str(l["id"]), []).append(item)
    if not com_anexos:
        for l in dados["linhas"]:
            resumo[str(l["id"])] = [{"rotulo": a["rotulo"], "url": a["url"],
                                     "erro": "o relatório foi gerado sem os documentos"}
                                    for a in l["_anexos"]]
    progresso(len(pedidos), len(pedidos), "Montando o relatório")

    # 2. O corpo.
    corpo = _Relatorio(dados)
    corpo.capa()
    corpo.relacao()
    for l in dados["linhas"]:
        corpo.detalhe(l, resumo.get(str(l["id"])) or [])
    final = fitz.open(stream=corpo.bytes(), filetype="pdf")

    # 3. Os anexos, logo depois da página de cada SP. De trás para a frente, para
    #    a posição das SPs anteriores não mudar enquanto se insere.
    inseridas: dict = {}
    for l in reversed(dados["linhas"]):
        sp = str(l["id"])
        docs = [(i, d) for i, d in preparados.get(sp, []) if d is not None]
        if not docs:
            continue
        bloco = fitz.open()
        for k, (item, doc) in enumerate(docs, start=1):
            _paginas_do_anexo(bloco, doc, l, item, k, len(docs))
            doc.close()
        inseridas[sp] = len(bloco)
        final.insert_pdf(bloco, start_at=corpo.ultima_pagina_da_sp[sp] + 1)
        bloco.close()

    # 4. A numeração final: cada página do corpo anda para a frente o tanto de
    #    páginas de anexo inseridas antes dela.
    antes = sorted((corpo.ultima_pagina_da_sp[sp], n) for sp, n in inseridas.items())

    def final_do_corpo(i: int) -> int:
        return i + sum(n for ultima, n in antes if ultima < i)

    relacao = corpo.paginas_da_relacao[0]
    toc = [[1, "Resumo", 1], [1, "Relação dos pagamentos", relacao + 1]]
    paginas_de_anexo = set()
    for l in dados["linhas"]:
        sp = str(l["id"])
        toc.append([1, _texto(f"{l['_n']}. {l.get('credor') or '-'} - "
                              f"{_moeda(l.get('valor_num'))}"),
                    final_do_corpo(corpo.primeira_pagina_da_sp[sp]) + 1])
        n = inseridas.get(sp, 0)
        if n:
            comeco = final_do_corpo(corpo.ultima_pagina_da_sp[sp]) + 1
            toc.append([2, "Documentos", comeco + 1])
            paginas_de_anexo.update(range(comeco, comeco + n))
    try:
        final.set_toc(toc)
    except Exception:  # noqa: BLE001 — sumário é conforto; sem ele o PDF serve
        logger.exception("Relatório completo do lote: não consegui pôr o sumário")

    mm = 72 / 25.4
    for pagina_corpo, x, y, w, h, alvo in corpo.links:
        destino = (relacao if alvo == "relacao" else
                   final_do_corpo(corpo.primeira_pagina_da_sp.get(alvo[3:], relacao)))
        final[final_do_corpo(pagina_corpo)].insert_link(
            {"kind": fitz.LINK_GOTO, "page": destino,
             "from": fitz.Rect(x * mm, y * mm, (x + w) * mm, (y + h) * mm)})

    total_paginas = len(final)
    for i, pagina in enumerate(final):
        h, w = pagina.rect.height, pagina.rect.width
        if i in paginas_de_anexo:
            pagina.insert_link({"kind": fitz.LINK_GOTO, "page": relacao,
                                "from": fitz.Rect(w - 116, 10, w - 20, 26)})
        pagina.insert_text((34, h - 14), _texto(
            "BWS Construções  ·  Relatório de pagamentos" +
            (f"  ·  {dados['titulo']}" if dados["titulo"] else "")),
            fontsize=6.5, fontname="helv", color=(0.55, 0.6, 0.68))
        pagina.insert_text((w - 90, h - 14), f"página {i + 1} de {total_paginas}",
                           fontsize=6.5, fontname="helv", color=(0.55, 0.6, 0.68))
    saida = final.tobytes(garbage=4, deflate=True, deflate_images=True,
                          deflate_fonts=True, use_objstms=1)
    final.close()
    return saida


def nome_do_arquivo(dados: dict) -> str:
    quando = dados["data_pagamento"] or dados["gerado_em"]
    base = re.sub(r"[^\w\- ]+", "", _texto(dados["titulo"] or "Pagamentos"), flags=re.UNICODE)
    base = " ".join(base.split())[:50] or "Pagamentos"
    return f"{base} - {quando.strftime('%Y-%m-%d')}.pdf"


# ---------------------------------------------------------------------------
# A GERAÇÃO FORA DO PEDIDO DA TELA
# ---------------------------------------------------------------------------
# Baixar os anexos leva de segundos a minutos. Se isso rodasse dentro do pedido,
# segurava uma das 4 linhas do serviço (o gunicorn roda 1 processo × 4 linhas) e o
# sistema inteiro ficaria lento enquanto isso. Roda numa linha própria; a tela
# pergunta o andamento a cada 2 segundos e baixa quando fica pronto.
#
# Guardado na MEMÓRIA do processo (o serviço roda com 1 processo só — CLAUDE.md,
# "Gunicorn"). Uma renovação do serviço no meio perde o trabalho: a tela diz e a
# pessoa gera de novo. O arquivo pronto fica numa pasta temporária por 2 horas.
VALIDADE_S = 2 * 3600
_trabalhos: dict = {}
_trava = threading.Lock()
_uma_de_cada_vez = threading.Semaphore(2)


def _limpar_velhos() -> None:
    agora_ = time.time()
    with _trava:
        for chave in [k for k, t in _trabalhos.items() if agora_ - t["criado"] > VALIDADE_S]:
            caminho = _trabalhos.pop(chave).get("caminho")
            if caminho:
                try:
                    os.remove(caminho)
                except OSError:
                    pass


def iniciar(dados: dict, com_anexos: bool = True) -> str:
    """Começa a geração e devolve o número do trabalho."""
    _limpar_velhos()
    chave = uuid.uuid4().hex
    with _trava:
        _trabalhos[chave] = {"criado": time.time(), "feitos": 0, "total": 0,
                             "etapa": "Na fila", "pronto": False, "erro": "",
                             "caminho": "", "nome": nome_do_arquivo(dados),
                             "paginas": 0}

    def anotar(feitos, total, etapa):
        with _trava:
            _trabalhos[chave].update(feitos=feitos, total=total, etapa=etapa)

    def trabalhar():
        with _uma_de_cada_vez:
            try:
                conteudo = montar_pdf(dados, progresso=anotar, com_anexos=com_anexos)
                arquivo = tempfile.NamedTemporaryFile(prefix="relatorio-lote-",
                                                      suffix=".pdf", delete=False)
                with arquivo:
                    arquivo.write(conteudo)
                with _trava:
                    _trabalhos[chave].update(pronto=True, caminho=arquivo.name,
                                             etapa="Pronto", tamanho=len(conteudo))
            except Exception as e:  # noqa: BLE001
                logger.exception("Relatório completo do lote: falhou a geração")
                with _trava:
                    _trabalhos[chave].update(erro=str(e) or e.__class__.__name__,
                                             etapa="Falhou")

    threading.Thread(target=trabalhar, name=f"relatorio-lote-{chave[:8]}",
                     daemon=True).start()
    return chave


def estado(chave: str) -> dict | None:
    with _trava:
        t = _trabalhos.get(str(chave or ""))
        return dict(t) if t else None
