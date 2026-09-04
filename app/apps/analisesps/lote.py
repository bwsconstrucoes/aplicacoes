# -*- coding: utf-8 -*-
"""
O lote de trabalho: a remessa de pagamentos que está sendo tratada agora.

É um bloco de texto livre, e o formato é o do Streamlit, sem mudança nenhuma —
quem já usa não precisa reaprender:

    Pagar amanhã
    1384831053
    1384844943

    Aguardando anuência
    1384852359 1384860011

Linha só com números vira lista de SPs. Linha com qualquer outra coisa vira
TÍTULO DE GRUPO, e as SPs abaixo dela ficam agrupadas sob esse título. Uma
linha pode trazer vários números separados por espaço, vírgula ou ponto e
vírgula.

UMA DIFERENÇA CONSCIENTE em relação ao Streamlit, e ela precisa estar escrita
porque um dia vai surpreender alguém: o lote é ÚNICO E COMPARTILHADO. Lá ele
morava no computador de quem usava; aqui, duas pessoas que abrirem a tela veem
o mesmo lote, e a segunda a salvar sobrescreve a primeira.

É de propósito. A equipe trabalha sobre a mesma remessa, e dois lotes paralelos
seriam pior do que um só — mas a tela mostra quem salvou por último e quando,
para ninguém apagar o trabalho do outro sem perceber.
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger("analisesps.lote")

# Um número de SP tem exatamente 10 dígitos.
#
# É o que permite extrair SPs de mensagens coladas do WhatsApp sem pescar lixo:
# telefone tem 11, CNPJ e CPF vêm em blocos menores, e valores e datas trazem
# pontuação no meio. A borda `\b` impede casar um pedaço de número maior.
PADRAO_SP = re.compile(r"\b(\d{10})\b")

SEPARADORES = re.compile(r"[\s,;]+")
SO_DIGITOS = re.compile(r"\d+")

SEM_TITULO = "(sem título)"


def extrair_ids(texto: str) -> list[str]:
    """Pesca os números de SP de um texto livre, na ordem, sem repetir.

    Serve para colar as mensagens de validação que chegam pelo WhatsApp e tirar
    dali as SPs, em vez de digitar uma a uma."""
    vistos: set[str] = set()
    saida: list[str] = []
    for sp in PADRAO_SP.findall(str(texto or "")):
        if sp not in vistos:
            vistos.add(sp)
            saida.append(sp)
    return saida


def separar_grupos(texto: str) -> list[dict]:
    """Quebra o texto do lote em grupos: cada um com um título e seus IDs."""
    grupos: list[dict] = []
    atual: dict = {"titulo": None, "ids": []}

    for bruta in str(texto or "").split("\n"):
        linha = bruta.strip()
        if not linha:
            continue
        pedacos = [p for p in SEPARADORES.split(linha) if p]
        if pedacos and all(SO_DIGITOS.fullmatch(p) for p in pedacos):
            atual["ids"].extend(pedacos)
        else:
            if atual["titulo"] is not None or atual["ids"]:
                grupos.append(atual)
            atual = {"titulo": linha, "ids": []}

    grupos.append(atual)
    return [g for g in grupos if g["titulo"] or g["ids"]]


def acrescentar_grupo(texto_atual: str, ids: list[str]) -> tuple[str, str]:
    """Põe um grupo novo NO TOPO do lote, numerado, preservando o que havia.

    No topo, e não no fim, porque é o que acabou de chegar e é sobre o que se
    vai trabalhar agora. Devolve o texto novo e o nome do grupo criado."""
    atual = str(texto_atual or "").strip("\n")
    numero = len(re.findall(r"(?m)^\s*Novo Lote\b", atual)) + 1
    titulo = f"Novo Lote {numero}"
    novo = titulo + "\n" + "\n".join(ids)
    if atual.strip():
        novo += "\n\n" + atual
    return novo, titulo


def remover_por_status(texto: str, status_alvo: set[str],
                       status_por_id: dict) -> tuple[str, int]:
    """Tira do lote as SPs que já estão num determinado status.

    Serve para limpar o que já foi pago ou cancelado sem desmontar os grupos: os
    títulos ficam, mesmo que o grupo esvazie. Devolve o texto novo e quantas
    saíram."""
    alvos = {s.strip().lower() for s in status_alvo}
    linhas_novas: list[str] = []
    removidos = 0

    for bruta in str(texto or "").split("\n"):
        linha = bruta.strip()
        if not linha:
            continue
        pedacos = [p for p in SEPARADORES.split(linha) if p]
        if pedacos and all(SO_DIGITOS.fullmatch(p) for p in pedacos):
            mantidos = [p for p in pedacos
                        if str(status_por_id.get(p, "")).strip().lower() not in alvos]
            removidos += len(pedacos) - len(mantidos)
            if mantidos:
                linhas_novas.append(" ".join(mantidos))
        else:
            linhas_novas.append(linha)      # título de grupo: sempre fica

    return "\n".join(linhas_novas).strip("\n"), removidos


# ---------------------------------------------------------------------------
# Onde o lote fica guardado
# ---------------------------------------------------------------------------
# A chave do lote de antes de 04/09/2026, quando havia UM lote para todo mundo.
# Ninguém escreve nele: ele existe só para que o trabalho que estava salvo na
# véspera da mudança não desapareça, e para ser trazido por botão.
COMPARTILHADO = ""


def ler(pessoa: str = "") -> dict:
    """O lote DESTA pessoa, com quem salvou por último e quando.

    Até 04/09/2026 havia um lote só, de todo mundo: quem salvasse depois
    sobrescrevia o trabalho do outro sem aviso. Agora cada um tem o seu — foi
    decisão do dono, e é como era no Streamlit, que rodava numa máquina só."""
    from .db import consultar_um
    linha = consultar_um(
        "SELECT conteudo, salvo_por, salvo_em FROM analisesps.lote "
        " WHERE pessoa = ?", (str(pessoa or ""),))
    if not linha:
        return {"conteudo": "", "salvo_por": None, "salvo_em": None}
    return {"conteudo": linha[0] or "", "salvo_por": linha[1],
            "salvo_em": linha[2]}


def salvar(conteudo: str, quem: str = "", pessoa: str = "") -> None:
    """Guarda o lote da pessoa. `quem` é o nome que a tela mostra depois."""
    from .db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.lote (pessoa, conteudo, salvo_por, salvo_em) "
            "VALUES (?, ?, ?, now()) "
            "ON CONFLICT (pessoa) DO UPDATE SET conteudo = EXCLUDED.conteudo, "
            "  salvo_por = EXCLUDED.salvo_por, salvo_em = now()",
            (str(pessoa or ""), str(conteudo or ""), quem))
        conn.commit()


def lote_de_antes() -> dict:
    """O lote de quando ele era compartilhado — só para oferecer, uma vez.

    Copiar sozinho para as quatro pessoas faria quatro cópias do mesmo lote
    sem ninguém pedir, e a segunda pessoa a "terminar" apagaria SPs que ainda
    estavam na lista da primeira. Oferecer por botão deixa a escolha com quem
    sabe de quem era aquele trabalho."""
    return ler(COMPARTILHADO)


# ---------------------------------------------------------------------------
# O que a tela mostra
# ---------------------------------------------------------------------------
def montar(texto: str) -> dict:
    """Junta os grupos do texto com os dados de cada SP no banco.

    Uma consulta só para todas as SPs do lote, não uma por grupo: um lote com
    dez grupos não pode custar dez idas ao banco.

    SPs que o texto cita e não existem na base vêm listadas à parte. Ignorá-las
    em silêncio seria o pior comportamento possível — quem colou precisa saber
    que aquele número não foi reconhecido."""
    from . import consultas
    from .db import consultar

    grupos = separar_grupos(texto)
    todos = [i for g in grupos for i in g["ids"]]
    if not todos:
        return {"grupos": grupos, "linhas": {}, "nao_encontrados": [],
                "total_geral": 0, "quantidade": 0}

    unicos = list(dict.fromkeys(todos))
    campos = ", ".join(consultas.CAMPOS_LISTA)
    marcadores = ",".join(["?"] * len(unicos))
    linhas = consultar(
        f"SELECT {campos}, ({consultas.SQL_STATUS_AGEND}) AS status_agend, "
        f"       ({consultas.SQL_RISCO}) AS risco, "
        f"       {consultas.SQL_CADASTRO_INCOMPLETO} AS cadastro_incompleto, "
        f"       (vencimento_d IS NOT NULL AND vencimento_d < {consultas.SQL_HOJE} "
        "         AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vencido, "
        f"       (vencimento_d = {consultas.SQL_HOJE} "
        "         AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vence_hoje "
        f"  FROM analisesps.sps WHERE id IN ({marcadores})", tuple(unicos))

    nomes = consultas.CAMPOS_LISTA + ["status_agend", "risco",
                                      "cadastro_incompleto", "vencido",
                                      "vence_hoje"]
    por_id = {str(l[0]): dict(zip(nomes, l)) for l in linhas}

    total_geral = 0
    quantidade = 0
    for grupo in grupos:
        grupo["linhas"] = [por_id[i] for i in grupo["ids"] if i in por_id]
        grupo["nao_encontrados"] = [i for i in grupo["ids"] if i not in por_id]
        grupo["total"] = sum((l["valor_num"] or 0) for l in grupo["linhas"])
        grupo["titulo_exibido"] = grupo["titulo"] or SEM_TITULO
        total_geral += grupo["total"]
        quantidade += len(grupo["linhas"])

    return {
        "grupos": grupos,
        "linhas": por_id,
        "nao_encontrados": [i for i in unicos if i not in por_id],
        "total_geral": total_geral,
        "quantidade": quantidade,
    }
