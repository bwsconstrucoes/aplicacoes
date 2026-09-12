# -*- coding: utf-8 -*-
"""
Os comprovantes arrastados para dentro da tela.

O QUE ESTE ARQUIVO **NÃO** FAZ, e é a decisão que poupou um módulo inteiro:
dar baixa. O robô que faz isso já existe e roda em produção há meses — o
`baixabradesco` recebe o PDF, descobre a qual SP pertence, dá baixa no Omie,
marca a SP como paga na SPsBD, move o card no Pipefy e guarda o comprovante.

Hoje quem chama esse robô é um cenário do Make.com, alimentado por e-mail. O
dono pediu o caminho curto: *"eu arrasto esses comprovantes pra dentro e
dispara a automação, sem nem precisar passar pelo Make."* Então aqui há só a
porta de entrada, a divisão em levas e a MEMÓRIA do que aconteceu.

AS LEVAS DE DEZ, e o número não é chute meu: é o que o script do dono já faz
hoje, descrito por ele em 11/09/2026 — *"ele divide o PDF em dez páginas. Se eu
mandar cinquenta páginas num único PDF, ele quebra em cinco e manda um por um."*
Manter o mesmo tamanho tem uma razão a mais do que a simetria: é o tamanho de
lote que o robô já recebe há meses em produção, então não se está estreando
carga nova nele.

E A CONTA É POR PÁGINA, NÃO POR ARQUIVO. O robô trata cada página como um
comprovante separado. Cortar por quantidade de arquivos deixaria um PDF de
cinquenta páginas passar inteiro numa chamada só — exatamente o caso que as
levas existem para evitar.
"""
from __future__ import annotations

import io
import logging
import os

logger = logging.getLogger("analisesps.comprovantes")

# Páginas por leva. Ver o porquê no cabeçalho.
POR_LEVA = 10

# Teto por arquivo. A instância tem 2 GB divididos com 17 módulos e já morreu
# de falta de memória em julho de 2026 — um PDF sem teto derruba o serviço
# inteiro, não só esta tela.
MAXIMO_POR_ARQUIVO = 25 * 1024 * 1024      # 25 MB
MAXIMO_DE_ARQUIVOS = 20

# Onde o PDF espera a vez. NÃO vai para o banco: ele tem 1 GB e já usa 430 MB,
# e o comprovante já é guardado pelo robô no fim do processo.
PASTA = os.getenv("ANALISESPS_PASTA_COMPROVANTES", "/tmp/analisesps_comprovantes")


class ErroDeComprovante(RuntimeError):
    """Falha com a mensagem já pronta para a tela."""


# ---------------------------------------------------------------------------
# Partir o PDF
# ---------------------------------------------------------------------------
def contar_paginas(conteudo: bytes) -> int:
    """Quantas páginas tem o PDF. Zero quando não dá para ler."""
    try:
        from pypdf import PdfReader
        return len(PdfReader(io.BytesIO(conteudo)).pages)
    except Exception as e:  # noqa: BLE001 — arquivo torto não pode derrubar a tela
        # `warning`, e não `exception`: soltar um arquivo que não é PDF é erro
        # de quem arrasta, não defeito do serviço. Despejar um traceback no log
        # do Render a cada foto solta faria o log de verdade sumir no meio.
        logger.warning("Análise de SPs: arquivo não parece um PDF (%s)", e)
        return 0


def quantas_levas(paginas: int, por_leva: int = POR_LEVA) -> int:
    if paginas <= 0:
        return 0
    return (paginas + por_leva - 1) // por_leva


def separar_em_levas(conteudo: bytes, por_leva: int = POR_LEVA):
    """Devolve (primeira_pagina, bytes) de cada leva, uma de cada vez.

    É GERADOR de propósito, e isso é memória: um PDF de cinquenta páginas
    montado inteiro em cinco pedaços na memória seria o arquivo duas vezes.
    Assim só existe uma leva por vez.

    A numeração devolvida é a da PÁGINA NO ARQUIVO ORIGINAL (começando em 1) —
    é o que a tela mostra, e é o que permite a pessoa achar o comprovante no
    PDF que ela mesma soltou."""
    from pypdf import PdfReader, PdfWriter

    leitor = PdfReader(io.BytesIO(conteudo))
    total = len(leitor.pages)
    for inicio in range(0, total, por_leva):
        escritor = PdfWriter()
        for n in range(inicio, min(inicio + por_leva, total)):
            escritor.add_page(leitor.pages[n])
        saco = io.BytesIO()
        escritor.write(saco)
        yield inicio + 1, saco.getvalue()


# ---------------------------------------------------------------------------
# Ler o que o robô respondeu
#
# ELE JÁ SEPARA TUDO O QUE O DONO PEDIU — "esse deu certo, esse deu errado,
# esse tem duplicidade, esse faltou aquilo". A resposta traz `resumo`,
# `duplicados`, `recusados` e um `plano` por página, com o motivo escrito.
# Isso hoje volta para o Make.com e morre lá; aqui vira linha no banco.
# ---------------------------------------------------------------------------
BAIXADO = "BAIXADO"
DUPLICADO = "DUPLICADO"
NAO_LOCALIZADO = "NAO_LOCALIZADO"
PENDENTE_VALIDACAO = "PENDENTE_VALIDACAO"
RECUSADO = "RECUSADO"
ERRO = "ERRO"

ROTULOS = {
    BAIXADO: "Baixado",
    DUPLICADO: "Já tinha sido baixado",
    NAO_LOCALIZADO: "Não achei a SP",
    PENDENTE_VALIDACAO: "Falta liberar antes de baixar",
    RECUSADO: "Pagamento não efetivado",
    ERRO: "Deu erro",
}

# A ordem em que a tela mostra: primeiro o que pede ação, por último o que
# deu certo. Quem abre a tela quer saber o que ficou de fora — o que baixou é
# o esperado.
ORDEM = [ERRO, NAO_LOCALIZADO, PENDENTE_VALIDACAO, RECUSADO, DUPLICADO, BAIXADO]


def _texto(v) -> str:
    return "" if v is None else str(v).strip()


def _situacao_do_plano(plano: dict) -> tuple[str, str]:
    """A situação de uma página e o motivo, em português.

    O robô responde em três níveis: o `match.status` diz se achou a SP, os
    `motivos_bloqueio` dizem por que não pôde executar, e as `responses`
    dizem o que cada sistema respondeu. A tela precisa de UMA frase."""
    casamento = plano.get("match") or {}
    status = _texto(casamento.get("status")).lower()
    motivos = [_texto(m) for m in (plano.get("motivos_bloqueio") or []) if _texto(m)]
    motivo = motivos[0] if motivos else _texto(casamento.get("motivo"))

    if status == "nao_localizado":
        return NAO_LOCALIZADO, motivo or "Não encontrei a SP deste comprovante."
    if status == "pendente_validacao":
        return PENDENTE_VALIDACAO, motivo or "A SP ainda não está liberada para baixa."
    if not plano.get("pode_executar"):
        return ERRO, motivo or "Não foi possível dar baixa."
    return BAIXADO, ""


def ler_resposta(resposta: dict, primeira_pagina: int = 1) -> list[dict]:
    """A resposta do robô virando linhas para o banco, uma por comprovante.

    `primeira_pagina` desloca a numeração: dentro da leva a página 1 é a
    primeira DAQUELA leva, e quem olha a tela precisa do número da página no
    arquivo que ele soltou."""
    resposta = resposta or {}
    linhas: list[dict] = []

    def acrescentar(situacao, bruto, motivo=""):
        recibo = (bruto or {}).get("receipt") or bruto or {}
        try:
            pagina = int(recibo.get("page") or 0)
        except (TypeError, ValueError):
            pagina = 0
        linhas.append({
            "pagina": (primeira_pagina + pagina - 1) if pagina else None,
            "situacao": situacao,
            "sp_id": _texto(recibo.get("id_pipefy")),
            "valor": _texto(recibo.get("valor_pago")),
            "recebedor": _texto(recibo.get("nome_recebedor"))[:120],
            "motivo": _texto(motivo)[:500],
        })

    for item in resposta.get("duplicados") or []:
        acrescentar(DUPLICADO, item,
                    _texto(item.get("motivo"))
                    or "Este comprovante já tinha sido baixado antes.")
    for item in resposta.get("recusados") or []:
        acrescentar(RECUSADO, item,
                    _texto(item.get("motivo"))
                    or "O pagamento não consta como efetivado.")
    for plano in resposta.get("planos") or []:
        situacao, motivo = _situacao_do_plano(plano)
        acrescentar(situacao, plano, motivo)

    return linhas


# ---------------------------------------------------------------------------
# Guardar o arquivo até a vez dele
# ---------------------------------------------------------------------------
def guardar(conteudo: bytes, nome: str, pessoa: str, quem: str) -> int:
    """Põe o arquivo em disco e abre o lote. Devolve o número do lote.

    O lote nasce ESPERANDO: quem faz o trabalho é o processo separado, porque
    a baixa fala com Omie, Pipefy, Sheets e Dropbox e leva minutos — dentro do
    worker ela seria morta pelo reinício do gunicorn, como já aconteceu três
    vezes com a carga da planilha (ver `executar_sync.py`)."""
    from .db import conexao

    if not conteudo:
        raise ErroDeComprovante("O arquivo chegou vazio.")
    if len(conteudo) > MAXIMO_POR_ARQUIVO:
        raise ErroDeComprovante(
            f"{nome}: são no máximo {MAXIMO_POR_ARQUIVO // (1024 * 1024)} MB "
            "por arquivo. Divida o PDF e mande em duas vezes.")

    paginas = contar_paginas(conteudo)
    if not paginas:
        raise ErroDeComprovante(
            f"{nome}: não consegui ler este PDF. Ele está protegido por senha "
            "ou veio corrompido?")

    os.makedirs(PASTA, exist_ok=True)
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.comprovantes_lote "
            "  (pessoa, quem, arquivo, caminho, paginas, levas, situacao) "
            "VALUES (?, ?, ?, '', ?, ?, 'ESPERANDO') RETURNING id",
            (pessoa or "", quem or "", nome[:200], paginas,
             quantas_levas(paginas)))
        lote_id = cur.fetchone()[0]
        cur.close()
        caminho = os.path.join(PASTA, f"lote-{lote_id}.pdf")
        with open(caminho, "wb") as arquivo:
            arquivo.write(conteudo)
        conn.execute(
            "UPDATE analisesps.comprovantes_lote SET caminho = ? WHERE id = ?",
            (caminho, lote_id))
        conn.commit()

    logger.info("Análise de SPs: comprovante %r recebido — lote %d, %d página(s), "
                "%d leva(s).", nome, lote_id, paginas, quantas_levas(paginas))
    return lote_id


def _apagar_arquivo(caminho: str) -> None:
    """O PDF sai do disco assim que o lote termina.

    O comprovante já foi guardado pelo robô no destino definitivo; o que fica
    aqui é cópia de passagem, e cópia de passagem que não é apagada vira disco
    cheio sem ninguém perceber."""
    try:
        if caminho and os.path.exists(caminho):
            os.remove(caminho)
    except OSError:
        logger.exception("Análise de SPs: não consegui apagar %r", caminho)


# ---------------------------------------------------------------------------
# O trabalho
# ---------------------------------------------------------------------------
def _mandar_ao_robo(pedaco: bytes, nome: str) -> dict:
    """Entrega uma leva ao `baixabradesco` e devolve a resposta dele.

    CHAMADA DIRETA, e não por HTTP. O processo separado poderia falar com a
    rota `/api/baixabradesco/executar` — é o que o Make faz —, mas isso
    ocuparia uma das QUATRO threads do gunicorn por vários minutos, e elas são
    dividas com os outros 17 módulos. O pedido montado aqui é o MESMO que
    aquela rota passa adiante, então o contrato é o documentado.

    O `secret` não vai: quem autenticou foi a tela do Análise de SPs, com o
    login dela. A senha do módulo existe para quem chega de fora."""
    import base64

    from app.apps.baixabradesco.core import processar_baixabradesco

    pedido = {
        "attachments": [{
            "filename": nome,
            "base64": base64.b64encode(pedaco).decode("ascii"),
        }],
    }
    return processar_baixabradesco(pedido) or {}


def _gravar_itens(conn, lote_id: int, linhas: list) -> None:
    for linha in linhas:
        conn.execute(
            "INSERT INTO analisesps.comprovantes_item "
            "  (lote_id, pagina, situacao, sp_id, valor, recebedor, motivo) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (lote_id, linha.get("pagina"), linha.get("situacao", ""),
             linha.get("sp_id", ""), linha.get("valor", ""),
             linha.get("recebedor", ""), linha.get("motivo", "")))
    conn.commit()


def processar_um(lote_id: int, anotar=None) -> dict:
    """Processa um lote inteiro, leva por leva, gravando o que sai de cada uma.

    GRAVA A CADA LEVA, e não no fim. Se o serviço reiniciar no meio de um PDF
    de cinquenta páginas, as quatro primeiras levas já estão no banco e a
    pessoa vê o que baixou. Guardar tudo para o fim perderia as cinco."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)

    with conexao() as conn:
        cur = conn.execute(
            "SELECT arquivo, caminho, paginas, levas FROM "
            " analisesps.comprovantes_lote WHERE id = ?", (lote_id,))
        linha = cur.fetchone()
        cur.close()
        if not linha:
            return {"ok": False, "erro": "Lote não encontrado."}
        conn.execute(
            "UPDATE analisesps.comprovantes_lote SET situacao = 'RODANDO' "
            " WHERE id = ?", (lote_id,))
        conn.commit()

    nome, caminho, paginas, levas = linha[0], linha[1], linha[2], linha[3]

    try:
        with open(caminho, "rb") as arquivo:
            conteudo = arquivo.read()
    except OSError as e:
        # O contêiner reinicia e leva o disco junto. Dizer isso é melhor do
        # que deixar o lote "rodando" para sempre.
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.comprovantes_lote SET situacao = 'FALHOU', "
                "  erro = ?, terminado_em = now() WHERE id = ?",
                ("O arquivo não está mais no servidor (o serviço reiniciou "
                 "antes de processar). Arraste o PDF de novo — o que já tiver "
                 "sido baixado não baixa duas vezes.", lote_id))
            conn.commit()
        logger.warning("Análise de SPs: lote %d sem arquivo (%s).", lote_id, e)
        return {"ok": False, "erro": "arquivo sumiu"}

    feitas, contagem = 0, {}
    try:
        for primeira, pedaco in separar_em_levas(conteudo):
            anotar("dando baixa nos comprovantes",
                   f"{nome}: leva {feitas + 1} de {levas}")
            resposta = _mandar_ao_robo(pedaco, nome)
            itens = ler_resposta(resposta, primeira)
            with conexao() as conn:
                _gravar_itens(conn, lote_id, itens)
                feitas += 1
                conn.execute(
                    "UPDATE analisesps.comprovantes_lote SET levas_feitas = ? "
                    " WHERE id = ?", (feitas, lote_id))
                conn.commit()
            for item in itens:
                contagem[item["situacao"]] = contagem.get(item["situacao"], 0) + 1
    except Exception as e:  # noqa: BLE001 — a falha tem de aparecer na tela
        logger.exception("Análise de SPs: falhou o lote %d de comprovantes", lote_id)
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.comprovantes_lote SET situacao = 'FALHOU', "
                "  erro = ?, terminado_em = now() WHERE id = ?",
                (str(e)[:1000], lote_id))
            conn.commit()
        _apagar_arquivo(caminho)
        return {"ok": False, "erro": str(e), "levas_feitas": feitas}

    with conexao() as conn:
        conn.execute(
            "UPDATE analisesps.comprovantes_lote SET situacao = 'PRONTO', "
            "  terminado_em = now() WHERE id = ?", (lote_id,))
        conn.commit()
    _apagar_arquivo(caminho)

    logger.info("Análise de SPs: lote %d pronto — %s.", lote_id,
                ", ".join(f"{ROTULOS.get(s, s)}: {q}"
                          for s, q in contagem.items()) or "nada")
    return {"ok": True, "paginas": paginas, "contagem": contagem}


def processar_pendentes(anotar=None) -> dict:
    """Drena a fila de lotes ESPERANDO. É o que o processo separado chama."""
    from .db import consultar

    esperando = consultar(
        "SELECT id FROM analisesps.comprovantes_lote "
        " WHERE situacao = 'ESPERANDO' ORDER BY id")
    feitos, falhas = 0, 0
    for (lote_id,) in esperando:
        resultado = processar_um(lote_id, anotar)
        if resultado.get("ok"):
            feitos += 1
        else:
            falhas += 1
    return {"lotes": feitos, "falhas": falhas}


# ---------------------------------------------------------------------------
# O que a tela mostra
# ---------------------------------------------------------------------------
def historico(pessoa: str = "", quantos: int = 15) -> list[dict]:
    """Os últimos lotes, com a contagem por situação de cada um.

    UMA CONSULTA PARA OS LOTES E UMA PARA OS ITENS, não uma por lote: com
    quinze lotes na tela seriam dezesseis idas ao banco, e o banco tem um
    décimo de um núcleo."""
    from .db import consultar

    lotes = consultar(
        "SELECT id, arquivo, quem, paginas, situacao, levas, levas_feitas, "
        "       erro, recebido_em, terminado_em "
        "  FROM analisesps.comprovantes_lote "
        " ORDER BY id DESC LIMIT ?", (max(1, int(quantos)),))
    if not lotes:
        return []

    ids = [l[0] for l in lotes]
    marcadores = ",".join(["?"] * len(ids))
    contagens = consultar(
        f"SELECT lote_id, situacao, count(*) FROM analisesps.comprovantes_item "
        f" WHERE lote_id IN ({marcadores}) GROUP BY lote_id, situacao",
        tuple(ids))
    por_lote: dict = {}
    for lote_id, situacao, quantos_ in contagens:
        por_lote.setdefault(lote_id, {})[situacao] = quantos_

    saida = []
    for l in lotes:
        contagem = por_lote.get(l[0], {})
        saida.append({
            "id": l[0], "arquivo": l[1], "quem": l[2], "paginas": l[3],
            "situacao": l[4], "levas": l[5], "levas_feitas": l[6],
            "erro": l[7], "recebido_em": l[8], "terminado_em": l[9],
            "contagem": [(s, ROTULOS.get(s, s), contagem[s])
                         for s in ORDEM if contagem.get(s)],
            "resolvidos": contagem.get(BAIXADO, 0) + contagem.get(DUPLICADO, 0),
            "pendencias": sum(q for s, q in contagem.items()
                              if s not in (BAIXADO, DUPLICADO)),
        })
    return saida


def itens_do_lote(lote_id: int) -> list[dict]:
    """As páginas de um lote, o que PEDE AÇÃO primeiro.

    A ordem não é a das páginas de propósito: quem abre isto quer saber o que
    ficou de fora, e o que baixou é o esperado."""
    from .db import consultar

    ordem = {s: n for n, s in enumerate(ORDEM)}
    linhas = consultar(
        "SELECT pagina, situacao, sp_id, valor, recebedor, motivo "
        "  FROM analisesps.comprovantes_item WHERE lote_id = ? "
        " ORDER BY pagina NULLS LAST", (lote_id,))
    itens = [{"pagina": l[0], "situacao": l[1], "rotulo": ROTULOS.get(l[1], l[1]),
              "sp_id": l[2], "valor": l[3], "recebedor": l[4], "motivo": l[5]}
             for l in linhas]
    itens.sort(key=lambda i: (ordem.get(i["situacao"], 99),
                              i["pagina"] if i["pagina"] is not None else 10 ** 6))
    return itens
