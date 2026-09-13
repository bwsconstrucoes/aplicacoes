# -*- coding: utf-8 -*-
"""
O nome do credor: o mesmo CNPJ escrito de cinco jeitos.

O PROBLEMA, nas palavras do dono, em 11/09/2026: *"o Pipefy é frouxo no campo
credor. Um lança 'Aço Cearense Limitada', outro bota só 'Aço Cearense', o outro
escreve errado."* O pedido foi comparar credor e CPF/CNPJ e equalizar para o
melhor nome.

QUANTO DISSO EXISTE, MEDIDO E NÃO CHUTADO. Em 116 lançamentos da planilha de
verdade, com 41 CNPJs distintos, **8 CNPJs — um em cada cinco — aparecem com
mais de um nome**.

E FOI A MEDIÇÃO QUE DERRUBOU A REGRA ÓBVIA. "Fica o nome mais completo" não
serve sozinho, e a prova está nos dados dele:

    MAGNA LOCAÇÕES LTDA      3x
    MAGNA LOCAÇÃOES LTDA     1x   <- o MAIS LONGO é o digitado errado

Aplicar "o mais completo ganha" trocaria o certo pelo errado em todas as SPs
daquele fornecedor. E há casos em que nenhum dos dois é erro:

    CELPE CIA ENERGETICA  /  NEOENERGIA          a empresa mudou de nome
    MAFEMA MATERIAIS ELÉTRICOS / MAFEMA LIMITADA nome de fantasia / razão social

Esses não são divergência para corrigir: são decisão de gente.

A REGRA QUE ESTE ARQUIVO SEGUE, e ela é a mesma da conciliação fiscal: **juntar
errado é pior do que não juntar**. O sistema resolve sozinho SÓ quando é
literalmente o mesmo nome escrito diferente; tudo o mais vira uma escolha que o
dono faz UMA VEZ por CNPJ, e que fica gravada — da segunda vez em diante aquele
fornecedor é automático.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter

logger = logging.getLogger("analisesps.credores")


# ---------------------------------------------------------------------------
# A CHAVE DE COMPARAÇÃO
#
# Tira acento, pontuação, espaço e maiúscula. O ESPAÇO SAI JUNTO, e isso não é
# descuido: é o que faz "MBP ISOBLOCK" e "MBP ISO BLOCK" — que nos dados dele
# são o mesmo fornecedor escrito por duas pessoas — virarem a mesma chave.
# ---------------------------------------------------------------------------
def chave(nome) -> str:
    """O nome reduzido ao que ele tem de essencial, para comparar."""
    limpo = unicodedata.normalize("NFD", str(nome or ""))
    limpo = "".join(c for c in limpo if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z0-9]", "", limpo).upper()


def so_digitos(valor) -> str:
    return re.sub(r"\D", "", str(valor or ""))


def documento_valido(valor) -> bool:
    """CPF tem 11 dígitos, CNPJ tem 14. Qualquer outra coisa não agrupa nada.

    Campo pela metade juntaria fornecedores diferentes debaixo do mesmo
    "documento" — e aí o nome de um entraria nas SPs do outro."""
    return len(so_digitos(valor)) in (11, 14)


# ---------------------------------------------------------------------------
# ESCOLHER O NOME
# ---------------------------------------------------------------------------
def _melhor_escrita(variantes: list) -> str:
    """Entre grafias do MESMO nome, qual fica.

    A mais usada ganha — é o que a equipe já escreve, e trocar o nome que todo
    mundo reconhece pelo que um digitou uma vez seria piorar. Empate desempata
    pela mais longa (costuma ser a que não foi abreviada) e depois pela ordem
    alfabética, só para o resultado não mudar de uma rodada para a outra."""
    return sorted(variantes, key=lambda v: (-v[1], -len(v[0]), v[0]))[0][0]


# Quando o sistema resolve sozinho, e por quê.
IGUAL = "IGUAL"          # o mesmo nome, só escrito diferente
COMECO = "COMECO"        # um é o começo do outro ("TRI" → "TRIBUNAL DE…")
DECIDIR = "DECIDIR"      # nomes de verdade diferentes: é escolha de gente

MOTIVOS = {
    IGUAL: "é o mesmo nome, escrito de jeitos diferentes",
    COMECO: "um dos nomes é a abreviação do outro",
    DECIDIR: "são nomes diferentes — só você sabe qual vale",
}


def _e_comeco_de_todos(candidata: str, chaves: set) -> bool:
    return all(outra.startswith(candidata) for outra in chaves)


def escolher(nomes) -> dict:
    """O nome que deve valer para um CPF/CNPJ, e se dá para decidir sozinho.

    `nomes` é a lista dos nomes como foram escritos, um por SP (repetidos
    incluídos — a repetição é o que diz qual grafia a equipe usa).

    Devolve {nome, tipo, automatico, variantes}. `automatico` é o que separa o
    que muda sozinho do que espera a decisão do dono."""
    contagem = Counter(str(n or "").strip() for n in nomes if str(n or "").strip())
    if not contagem:
        return {"nome": "", "tipo": DECIDIR, "automatico": False, "variantes": []}

    # Agrupa as grafias do mesmo nome. "ESPERANÇA" e "ESPERANCA" caem aqui;
    # "ESPERAÇA", que é erro de digitação, NÃO — e é por isso que aquele caso
    # vai para a decisão do dono em vez de ser adivinhado.
    por_chave: dict = {}
    for nome, quantas in contagem.items():
        por_chave.setdefault(chave(nome), []).append((nome, quantas))

    variantes = []
    for k, grafias in por_chave.items():
        variantes.append({
            "chave": k,
            "nome": _melhor_escrita(grafias),
            "vezes": sum(q for _, q in grafias),
            "grafias": sorted(n for n, _ in grafias),
            # QUANTAS SPs TÊM CADA GRAFIA, e não só quais grafias existem.
            # A contagem por grupo não serve para dizer o que falta arrumar:
            # "SERVIÇOS" e "SERVICOS" são o mesmo grupo, e mesmo assim há uma
            # SP escrita diferente para reescrever.
            "vezes_por_grafia": {n: q for n, q in grafias},
        })
    variantes.sort(key=lambda v: (-v["vezes"], v["nome"]))

    # UM NOME SÓ (ou só grafias do mesmo): resolvido, e sem pedir nada.
    if len(variantes) == 1:
        return {"nome": variantes[0]["nome"], "tipo": IGUAL,
                "automatico": True, "variantes": variantes}

    # ABREVIAÇÃO: uma das chaves é o começo de TODAS as outras. "TRI" é começo
    # de "TRIBUNALDEJUSTICADOCEARA", então o completo ganha — e aqui o mais
    # longo é seguro justamente porque o curto está inteiro dentro dele.
    chaves = {v["chave"] for v in variantes}
    if any(_e_comeco_de_todos(k, chaves) for k in chaves):
        maior = max(variantes, key=lambda v: len(v["chave"]))
        return {"nome": maior["nome"], "tipo": COMECO,
                "automatico": True, "variantes": variantes}

    # NOMES DE VERDADE DIFERENTES. Aqui o sistema PARA. É o caso do
    # CELPE/NEOENERGIA e do MAGNA com erro de digitação: sugere o mais usado,
    # mas não escreve nada sem o dono dizer.
    return {"nome": variantes[0]["nome"], "tipo": DECIDIR,
            "automatico": False, "variantes": variantes}


def divergencias(linhas) -> list:
    """Os CPF/CNPJ que aparecem com mais de um nome, já com a proposta.

    `linhas` são pares (documento, nome) — uma por SP. Devolve o que precisa de
    decisão primeiro: é o que a tela mostra em cima."""
    por_documento: dict = {}
    for documento, nome in linhas:
        if not documento_valido(documento):
            continue
        por_documento.setdefault(so_digitos(documento), []).append(nome)

    saida = []
    for documento, nomes in por_documento.items():
        resultado = escolher(nomes)
        # DIVERGÊNCIA É MAIS DE UMA GRAFIA ESCRITA, e não mais de um nome
        # diferente. A distinção custou um defeito: contando grupos de nome,
        # "MASSA PRONTA … SERVIÇOS LTDA" e "… SERVICOS LTDA" viravam UM grupo
        # e a planilha ficava como estava — justamente o caso mais seguro de
        # arrumar, porque é a mesma palavra com e sem cedilha.
        escritas = sum(len(v["grafias"]) for v in resultado["variantes"])
        if escritas < 2:
            continue          # escrito sempre igual: não há o que equalizar
        saida.append({
            "documento": documento,
            "nome": resultado["nome"],
            "tipo": resultado["tipo"],
            "motivo": MOTIVOS[resultado["tipo"]],
            "automatico": resultado["automatico"],
            "variantes": resultado["variantes"],
            "sps": sum(v["vezes"] for v in resultado["variantes"]),
        })
    # O que espera decisão primeiro, e dentro disso o que afeta mais SPs.
    saida.sort(key=lambda d: (d["automatico"], -d["sps"]))
    return saida


def a_corrigir(documento_nome_por_sp, escolhido_por_documento) -> list:
    """As SPs cujo nome do credor difere do escolhido. Devolve (sp_id, nome).

    COMPARA PELA CHAVE, e não pelo texto: se a SP já tem o nome certo com outro
    acento, reescrever seria gravar na planilha por nada — e cada gravação é
    uma célula na fila e uma ida ao Google."""
    saida = []
    for sp_id, documento, nome in documento_nome_por_sp:
        certo = escolhido_por_documento.get(so_digitos(documento))
        if not certo:
            continue
        if str(nome or "").strip() != certo:
            saida.append((sp_id, certo))
    return saida


# ---------------------------------------------------------------------------
# O QUE VEM DO BANCO
# ---------------------------------------------------------------------------
def _agrupado_da_base() -> list:
    """(documento só com dígitos, nome, quantas SPs) — já agrupado pelo banco.

    AGRUPA NO SQL, e não em Python: trazer as 59 mil linhas para contar aqui
    seriam 59 mil textos na memória de uma instância que já morreu disso. O
    banco devolve alguns milhares de pares, que é o tamanho do cadastro de
    fornecedores, não o da base.

    E NORMALIZA O DOCUMENTO NO SQL pelo mesmo motivo de sempre: na planilha o
    mesmo CNPJ vem "29.066.773/0001-52" numa linha e "29066773000152" noutra.
    Agrupar pelo texto cru faria o fornecedor virar dois."""
    from .db import consultar

    return consultar(
        "SELECT regexp_replace(documento, '\\D', '', 'g') AS doc, "
        "       trim(credor) AS nome, count(*) "
        "  FROM analisesps.sps "
        " WHERE length(regexp_replace(coalesce(documento, ''), '\\D', '', 'g')) "
        "       IN (11, 14) "
        "   AND trim(coalesce(credor, '')) <> '' "
        " GROUP BY 1, 2")


def escolhas_guardadas() -> dict:
    """O que já foi decidido: documento -> {nome, tipo, automatico, ...}."""
    from .db import consultar
    try:
        linhas = consultar(
            "SELECT documento, nome, tipo, automatico, decidido_por, "
            "       decidido_em, sps_mudadas FROM analisesps.credor_nome")
    except Exception:  # noqa: BLE001 — migração 007 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler os nomes escolhidos")
        return {}
    return {l[0]: {"nome": l[1], "tipo": l[2], "automatico": l[3],
                   "decidido_por": l[4], "decidido_em": l[5],
                   "sps_mudadas": l[6]} for l in linhas}


def divergencias_da_base() -> list:
    """A lista que a tela mostra, já sabendo o que o dono decidiu antes.

    Uma divergência JÁ DECIDIDA não some da lista — ela muda de lugar: vai para
    "resolvido", com o nome escolhido. Sumir faria parecer que o problema
    desapareceu sozinho, e no dia em que alguém lançasse o nome velho de novo a
    pessoa não entenderia por que voltou."""
    agrupado = _agrupado_da_base()
    por_documento: dict = {}
    for documento, nome, quantas in agrupado:
        por_documento.setdefault(documento, []).extend([nome] * int(quantas or 1))

    decididas = escolhas_guardadas()
    saida = []
    for documento, nomes in por_documento.items():
        resultado = escolher(nomes)
        escritas = sum(len(v["grafias"]) for v in resultado["variantes"])
        ja = decididas.get(documento)
        if escritas < 2 and not ja:
            continue
        # A escolha guardada MANDA sobre a proposta. Se o dono disse que é
        # NEOENERGIA, a regra não pode voltar a propor CELPE na semana seguinte.
        nome = ja["nome"] if ja else resultado["nome"]
        saida.append({
            "documento": documento,
            "nome": nome,
            "proposto": resultado["nome"],
            "tipo": (ja or resultado)["tipo"],
            "motivo": MOTIVOS.get((ja or resultado)["tipo"], ""),
            "automatico": resultado["automatico"],
            "decidido": bool(ja),
            "decidido_por": (ja or {}).get("decidido_por", ""),
            "variantes": resultado["variantes"],
            "sps": sum(v["vezes"] for v in resultado["variantes"]),
            # QUANTAS SPs AINDA ESTÃO ESCRITAS DIFERENTE do nome que vale.
            # Conta grafia por grafia: contar por grupo dizia "0 a arrumar"
            # justamente no caso mais fácil — o mesmo nome com e sem cedilha,
            # que está no mesmo grupo e mesmo assim precisa ser reescrito.
            "fora": sum(quantas
                        for v in resultado["variantes"]
                        for grafia, quantas in v["vezes_por_grafia"].items()
                        if grafia != nome),
        })
    # Primeiro o que espera decisão, depois o que já pode ser aplicado, e
    # dentro de cada grupo o que afeta mais SPs.
    saida.sort(key=lambda d: (d["decidido"], d["automatico"], -d["sps"]))
    return saida


def guardar_escolha(documento: str, nome: str, tipo: str, automatico: bool,
                    quem: str, sps_mudadas: int = 0) -> None:
    """Grava a decisão. É ela que faz a pergunta não voltar na semana seguinte."""
    from .db import conexao

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.credor_nome "
            "  (documento, nome, tipo, automatico, decidido_por, decidido_em, "
            "   sps_mudadas) "
            "VALUES (?, ?, ?, ?, ?, now(), ?) "
            "ON CONFLICT (documento) DO UPDATE SET "
            "  nome = EXCLUDED.nome, tipo = EXCLUDED.tipo, "
            "  automatico = EXCLUDED.automatico, "
            "  decidido_por = EXCLUDED.decidido_por, decidido_em = now(), "
            "  sps_mudadas = analisesps.credor_nome.sps_mudadas "
            "                + EXCLUDED.sps_mudadas",
            (so_digitos(documento), nome, tipo, bool(automatico), quem or "",
             int(sps_mudadas)))
        conn.commit()


def sps_para_reescrever(documento: str, nome: str) -> list:
    """Os IDs das SPs daquele CPF/CNPJ cujo credor está escrito diferente.

    COMPARA O TEXTO EXATO, e não a chave: o objetivo aqui é deixar a planilha
    toda com a MESMA grafia, então "SERVICOS" tem de virar "SERVIÇOS" mesmo
    sendo a mesma palavra. É justo esse o pedido do dono."""
    from .db import consultar

    linhas = consultar(
        "SELECT id FROM analisesps.sps "
        " WHERE regexp_replace(coalesce(documento, ''), '\\D', '', 'g') = ? "
        "   AND trim(coalesce(credor, '')) <> ? "
        "   AND trim(coalesce(credor, '')) <> ''", (so_digitos(documento), nome))
    return [str(l[0]) for l in linhas]
