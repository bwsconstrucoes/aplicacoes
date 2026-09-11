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


def _limpar(texto: str, sai) -> tuple[str, int]:
    """A limpeza do lote, com a regra de quem sai vindo de fora.

    `sai(sp_id)` responde se aquela SP deve deixar o lote. É chamada NA ORDEM
    do texto, de cima para baixo, e pode guardar estado entre as chamadas —
    é assim que a remoção de duplicados sabe qual ocorrência é a primeira.

    O TÍTULO DE UM GRUPO QUE ESVAZIOU NESTA LIMPEZA VAI JUNTO. Antes ele
    ficava, e o lote terminava cheio de cabeçalhos sem nada embaixo — "Pagar
    amanhã" sem uma SP sequer. Pedido do dono em 11/09/2026.

    MAS SÓ QUEM ESVAZIOU AGORA. Um grupo que já estava vazio antes continua:
    alguém escreveu aquele título de propósito, para encher depois, e apagar o
    que a pessoa acabou de digitar seria pior do que o cabeçalho sobrando.

    Vive separado porque as três limpezas (pagas, canceladas, duplicadas) têm
    de tratar o cabeçalho órfão do MESMO jeito. Em três cópias, a terceira
    nasceria sem a regra — e ninguém notaria até o lote encher de título
    solto."""
    removidos = 0

    # Primeiro quebra em blocos: cada um é um título (ou nenhum, no começo) e
    # as linhas de SPs que vêm debaixo dele. Só assim dá para saber se um
    # título ficou órfão POR CAUSA desta limpeza.
    blocos: list = [{"titulo": None, "linhas": [], "tinha": 0}]
    for bruta in str(texto or "").split("\n"):
        linha = bruta.strip()
        if not linha:
            continue
        pedacos = [p for p in SEPARADORES.split(linha) if p]
        if pedacos and all(SO_DIGITOS.fullmatch(p) for p in pedacos):
            mantidos = [p for p in pedacos if not sai(p)]
            removidos += len(pedacos) - len(mantidos)
            blocos[-1]["tinha"] += len(pedacos)
            if mantidos:
                blocos[-1]["linhas"].append(" ".join(mantidos))
        else:
            blocos.append({"titulo": linha, "linhas": [], "tinha": 0})

    linhas_novas: list = []
    for bloco in blocos:
        esvaziou_agora = bloco["tinha"] > 0 and not bloco["linhas"]
        if bloco["titulo"] is not None and not esvaziou_agora:
            linhas_novas.append(bloco["titulo"])
        linhas_novas.extend(bloco["linhas"])

    return "\n".join(linhas_novas).strip("\n"), removidos


def remover_por_status(texto: str, status_alvo: set[str],
                       status_por_id: dict) -> tuple[str, int]:
    """Tira do lote as SPs que já estão num determinado status.

    Serve para limpar o que já foi pago ou cancelado. Devolve o texto novo e
    quantas saíram. O cabeçalho de grupo que esvaziou sai junto — ver
    `_limpar`."""
    alvos = {s.strip().lower() for s in status_alvo}
    return _limpar(
        texto,
        lambda sp: str(status_por_id.get(sp, "")).strip().lower() in alvos)


def remover_duplicados(texto: str) -> tuple[str, int]:
    """Tira do lote a SP repetida, guardando a PRIMEIRA aparição.

    Pedido do dono em 11/09/2026, e ele disse qual das cópias fica: *"mantém o
    registro mais superior, e os que estão mais para baixo no lote remove"*.

    A primeira, e não a última, porque o lote é lido de cima para baixo e o que
    está em cima é o grupo mais recente — `acrescentar_grupo` põe o novo no
    topo. Guardar a de baixo mudaria a SP de grupo sem ninguém ter pedido.

    POR QUE A REPETIÇÃO ATRAPALHA, e não é só feiúra: o mesmo número em dois
    grupos aparece duas vezes na tela, é somado duas vezes no total do lote, e
    convida a agir duas vezes sobre o mesmo pagamento. Era também o que fazia a
    marcação reposta pegar a linha errada.

    Devolve o texto novo e quantas cópias saíram — cópias, não SPs: três
    aparições do mesmo número contam duas."""
    vistos: set = set()

    def sai(sp: str) -> bool:
        if sp in vistos:
            return True
        vistos.add(sp)
        return False

    return _limpar(texto, sai)


def contar_duplicados(texto: str) -> int:
    """Quantas cópias sobrando existem no lote, sem mexer em nada.

    A tela usa isto para só oferecer o botão quando há o que remover — um botão
    que não faz nada quando apertado é pior do que botão nenhum."""
    vistos: set = set()
    sobrando = 0
    for grupo in separar_grupos(texto):
        for sp in grupo["ids"]:
            if sp in vistos:
                sobrando += 1
            else:
                vistos.add(sp)
    return sobrando


def remover_ids(texto: str, ids) -> tuple[str, int]:
    """Tira do lote as SPs pedidas, estejam em que grupo estiverem.

    É o "Remover" da barra do alto: marcar linhas em grupos diferentes e tirar
    todas de uma vez. Sem ele, tirar uma SP do lote era editar o texto na mão
    e achar o número no meio dos outros.

    Os TÍTULOS DOS GRUPOS FICAM, mesmo que o grupo esvazie — apagar o título
    junto faria a remessa perder a divisão que alguém montou, e reconstruir
    isso custa mais do que uma linha vazia incomoda. Mesma decisão do
    `remover_por_status` ao lado."""
    alvos = {str(i).strip() for i in (ids or []) if str(i).strip()}
    if not alvos:
        return str(texto or ""), 0

    linhas_novas: list[str] = []
    removidos = 0

    for bruta in str(texto or "").split("\n"):
        linha = bruta.strip()
        if not linha:
            continue
        pedacos = [p for p in SEPARADORES.split(linha) if p]
        if pedacos and all(SO_DIGITOS.fullmatch(p) for p in pedacos):
            mantidos = [p for p in pedacos if p not in alvos]
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


def por_pessoa() -> bool:
    """O banco já sabe separar o lote por pessoa?

    Entre a publicação do código e o apertar do botão "Aplicar atualizações do
    banco" existe uma janela em que o programa é novo e o banco é velho. Nessa
    janela o lote continua sendo um só — que é como funcionava na véspera, e
    portanto não surpreende ninguém. Melhor isso do que a tela do Lote
    estourar justamente enquanto se espera o botão."""
    from .db import tem_coluna
    return tem_coluna("lote", "pessoa")


# A chave do lote no armário de reserva — ver `preferencias.py`. Enquanto a
# coluna `pessoa` não existir, é aqui que o lote de cada um fica.
CHAVE_RESERVA = "lote"


def _reserva_ler(pessoa: str) -> dict:
    from . import preferencias
    return preferencias.ler(pessoa, CHAVE_RESERVA)


def ler(pessoa: str) -> dict:
    """O lote DESTA pessoa, com quem salvou por último e quando.

    A PESSOA NÃO TEM VALOR PADRÃO, e isso é de propósito. Ela tinha, e o padrão
    era `""` — que significa o LOTE ANTIGO, de quando ele era compartilhado.
    Duas rotas (a exportação e o PDF) ficaram chamando `ler()` sem argumento
    quando o lote passou a ser de cada um, e por meses entregaram um lote
    congelado sem reclamar de nada. Quem quiser mesmo o lote antigo chama
    `lote_de_antes()`, que diz isso no nome.

    Até 04/09/2026 havia um lote só, de todo mundo: quem salvasse depois
    sobrescrevia o trabalho do outro sem aviso. Agora cada um tem o seu — foi
    decisão do dono, e é como era no Streamlit, que rodava numa máquina só.

    ENQUANTO A COLUNA `pessoa` NÃO EXISTIR (migração 003 não aplicada), o lote
    de cada um vai para o armário de reserva, em vez de todo mundo voltar a
    dividir a mesma lista. Antes daqui a separação por pessoa só passava a
    valer depois do botão — e "depois do botão" durou dias."""
    from .db import consultar_um
    if not por_pessoa():
        guardado = _reserva_ler(pessoa)
        if guardado:
            return {"conteudo": guardado.get("conteudo", "") or "",
                    "salvo_por": guardado.get("salvo_por"),
                    "salvo_em": guardado.get("salvo_em"),
                    "compartilhado": False}
        # Nada guardado ainda: aproveita o lote antigo, o de quando era um só.
        # É trabalho de verdade que estava em andamento; começar do zero seria
        # o mesmo que apagá-lo.
        linha = consultar_um(
            "SELECT conteudo, salvo_por, salvo_em FROM analisesps.lote "
            " WHERE id = 1")
        if not linha:
            return {"conteudo": "", "salvo_por": None, "salvo_em": None,
                    "compartilhado": False}
        return {"conteudo": linha[0] or "", "salvo_por": linha[1],
                "salvo_em": linha[2], "compartilhado": False}

    linha = consultar_um(
        "SELECT conteudo, salvo_por, salvo_em FROM analisesps.lote "
        " WHERE pessoa = ?", (str(pessoa or ""),))
    if not linha:
        # A tabela boa existe mas esta pessoa não tem linha lá: o que ela
        # guardou antes do botão está no armário de reserva. Traz para cá.
        guardado = _reserva_ler(pessoa)
        if guardado and guardado.get("conteudo"):
            salvar(guardado["conteudo"], guardado.get("salvo_por") or "", pessoa)
            logger.info("Análise de SPs: lote de %r trazido do armário de "
                        "reserva.", pessoa)
            return {"conteudo": guardado["conteudo"],
                    "salvo_por": guardado.get("salvo_por"),
                    "salvo_em": guardado.get("salvo_em"),
                    "compartilhado": False}
        return {"conteudo": "", "salvo_por": None, "salvo_em": None,
                "compartilhado": False}
    return {"conteudo": linha[0] or "", "salvo_por": linha[1],
            "salvo_em": linha[2], "compartilhado": False}


def salvar(conteudo: str, quem: str, pessoa: str) -> None:
    """Guarda o lote da pessoa. `quem` é o nome que a tela mostra depois.

    Sem valor padrão pelo mesmo motivo de `ler`: salvar no lote errado é pior
    do que não salvar, porque ninguém percebe."""
    from .db import conexao
    if not por_pessoa():
        from . import preferencias
        from .horario import agora
        # A hora vai em formato de máquina: quem mostra na tela é o
        # `momento_br`, que sabe converter. Guardar já formatado faria a tela
        # mostrar a hora duas vezes escrita de jeitos diferentes.
        preferencias.gravar(pessoa, CHAVE_RESERVA, {
            "conteudo": str(conteudo or ""), "salvo_por": quem,
            "salvo_em": agora().isoformat()})
        return
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.lote (pessoa, conteudo, salvo_por, salvo_em) "
            "VALUES (?, ?, ?, now()) "
            "ON CONFLICT (pessoa) DO UPDATE SET conteudo = EXCLUDED.conteudo, "
            "  salvo_por = EXCLUDED.salvo_por, salvo_em = now()",
            (str(pessoa or ""), str(conteudo or ""), quem))
        conn.commit()


def lote_de_antes() -> dict:
    if not por_pessoa():
        return {"conteudo": "", "salvo_por": None, "salvo_em": None}
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
