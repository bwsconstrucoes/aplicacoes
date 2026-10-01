# -*- coding: utf-8 -*-
"""
OS CARDS DO PIPEFY PARA A FOLHA: um por conta, com o link do arquivo.

Decisões do dono que desenham este módulo:

**26/09/2026 — gerar arquivo NÃO cria card.** *"Eu vou poder gerar, por exemplo,
arquivo de pagamento e folha e relatórios, tudo sem necessariamente gerar os cards
do Pipefy. É melhor dessa forma."* Então lançar no Pipefy é um segundo botão, e o
arquivo já existe no Drive antes dele ser apertado. Se o relatório estiver errado,
nada foi criado lá fora.

**26/09/2026 — regerar é normal, e os dois lados não se interligam.** *"A gente faz
o cancelamento no Pipefy e gera de novo quando for necessário."* O sistema não tenta
consertar o Pipefy: só registra o que lançou, e AVISA quando a competência já foi
lançada, em vez de impedir.

**27/09/2026 — o card recebe o link do arquivo.** *"O importante é que tenha o
arquivo salvo, e que tenha link em card."*

⚠️ OS CAMPOS DO PIPE SÃO LIDOS DO PIPEFY, NÃO ESCRITOS AQUI. O blueprint do Make
tem defeito conhecido de campo trocado (o par 62 grava no campo do 63 —
`docs/FOLHA_DE_PAGAMENTO.md` §4), e copiar a lista de lá copiaria o defeito. Campo
que eu não reconheço fica **dito**, nunca preenchido no escuro: valor de centro de
custo caindo no vizinho só aparece no fechamento da obra, meses depois.
"""
from __future__ import annotations

import logging
from decimal import Decimal

from . import folha_geracao as geracao
from . import formatos, pipefy

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

# Os pipes, lidos do blueprint do Make (§4). São só os números do pipe — os campos
# vêm da API.
PIPE_DESPESA = "301433085"        # Despesa com Colaboradores


class ErroDosCards(RuntimeError):
    """Não deu para lançar. A frase vai inteira para a tela."""


def conferir_pipe(pipe_id: str = PIPE_DESPESA) -> dict:
    """Lê os campos do pipe e diz quais eu reconheço. NÃO CRIA NADA.

    É o passo honesto antes de deixar alguém lançar: mostra o que vai ser
    preenchido, o que vai ficar vazio e por quê."""
    try:
        pipe = pipefy.campos_do_pipe(pipe_id)
    except pipefy.ErroDoPipefy as e:
        raise ErroDosCards(str(e)) from e

    campos = pipe.get("campos") or {}
    achados = {
        "descricao": pipefy.achar_campo(campos, "descri"),
        # ⚠️ "VALOR" TEM DE FUGIR DOS SETENTA E CINCO "Valor Centro de Custo N".
        # O total da despesa caindo no valor de um centro de custo é o defeito que
        # o blueprint do Make já tem, e que só aparece no fechamento da obra.
        "valor": pipefy.achar_campo(campos, "valor", fora=("centro de custo",)),
        "tipo_de_despesa": pipefy.achar_campo(campos, "tipo", "despesa"),
        "link_pagamento": pipefy.achar_campo(campos, "planilha", "pagamento"),
        # ⚠️ "analis" E NÃO "an": "an" está dentro de "plan" (de "planilha"), e o
        # campo da planilha de PAGAMENTO era reconhecido como o da análise — o link
        # errado indo para o campo errado no card. Achado por teste.
        "link_analise": pipefy.achar_campo(campos, "planilha", "analis"),
    }
    return {
        "pipe": pipe.get("id"), "nome": pipe.get("nome"),
        "quantos_campos": len(campos),
        "reconhecidos": {k: {"id": v, "label": campos.get(v, {}).get("label", "")}
                         for k, v in achados.items() if v},
        "nao_encontrados": [k for k, v in achados.items() if not v],
        # Os pares de centro de custo, que é onde o blueprint do Make erra.
        "centros_de_custo": sorted(
            campo_id for campo_id, d in campos.items()
            if "centro de custo" in (d.get("label") or "").lower()),
        "fases": pipe.get("fases") or [],
    }


def _valores_do_card(reconhecidos: dict, descricao: str, total,
                     link_pagamento: str, link_analise: str) -> tuple:
    """Monta os pares campo/valor, e devolve também o que ficou de fora.

    ⚠️ SÓ PREENCHE O QUE FOI RECONHECIDO. Um campo parecido é pior que campo vazio:
    vazio alguém vê e preenche; errado ninguém vê."""
    mapa = {
        "descricao": descricao,
        "valor": f"{Decimal(str(total or 0)).quantize(CENTAVO)}",
        "link_pagamento": link_pagamento or "",
        "link_analise": link_analise or "",
    }
    valores, de_fora = [], []
    for chave, valor in mapa.items():
        campo = (reconhecidos.get(chave) or {}).get("id")
        if campo and valor:
            valores.append({"campo": campo, "valor": valor})
        elif valor:
            de_fora.append(chave)
    return valores, de_fora


def descricao_do_card(competencia: str, tipo: str, verbas, conta: str,
                      pessoas: int, total, link_pagamento: str = "",
                      link_analise: str = "") -> str:
    """O texto do card. ⚠️ ELE É A PROVA: quem abrir o card meses depois precisa
    saber de que competência é, que verbas entraram, de qual conta saiu e onde está
    o arquivo — sem depender de ninguém lembrar."""
    rotulo_tipo = {"quinzena": "Quinzena (dias 1 a 15)",
                   "fim_de_mes": "Fim de mês (16 ao último dia)"}.get(
                       str(tipo or ""), str(tipo or ""))
    linhas = [
        f"Competência: {competencia}",
        f"Pagamento: {rotulo_tipo}" if rotulo_tipo else "",
        "Verbas: " + " + ".join(geracao.rotulo_da_verba(v) for v in (verbas or [])),
        f"Conta de pagamento: {conta}" if conta else "",
        f"Pessoas: {pessoas}",
        # O valor com vírgula e ponto de milhar: o card é lido por gente, e
        # "4200.00" num card de despesa se confunde com quatro reais.
        f"Total: R$ {formatos.moeda(total)}",
    ]
    if link_pagamento:
        linhas.append(f"Planilha de pagamento: {link_pagamento}")
    if link_analise:
        linhas.append(f"Planilha de análise: {link_analise}")
    # As vazias são as que não se aplicam (conta em branco, por exemplo); elas
    # saem, e só depois entra a linha em branco de separação.
    corpo = [l for l in linhas if l]
    return "\n".join(corpo + ["", "Gerado pelo Análise de SPs."])


def ja_lancado(ano: int, mes: int, tipo: str) -> list:
    """Os arquivos desta competência que JÁ têm card.

    ⚠️ AVISA, NÃO IMPEDE — decisão dele (D15): *"a gente faz o cancelamento no
    Pipefy e gera de novo quando for necessário"*. Impedir seria eu decidindo no
    lugar dele; avisar respeita a decisão e evita o lançamento duplicado por
    distração."""
    from . import folha_pagamento as fpg
    return [a for a in fpg.log(teto=200, ano=ano, mes=mes)
            if a.get("card_pipefy") and (not tipo or a.get("tipo") == tipo)]


def lancar(arquivo_id: int, pipe_id: str = PIPE_DESPESA,
           quem: str = "") -> dict:
    """Cria UM card para um arquivo já gerado, e amarra o card ao log.

    ⚠️ CHAMADA SEM VOLTA. O card não se apaga por aqui."""
    from . import folha_pagamento as fpg

    registros = [a for a in fpg.log(teto=400) if a["id"] == int(arquivo_id)]
    if not registros:
        raise ErroDosCards("não achei este arquivo no log de gerados.")
    arquivo = registros[0]
    if arquivo.get("card_pipefy"):
        raise ErroDosCards(
            f"este arquivo já foi lançado no card {arquivo['card_pipefy']}. "
            "Cancele o card no Pipefy antes de lançar de novo.")
    if arquivo["destino"] == fpg.ANALISE:
        raise ErroDosCards(
            "o arquivo de análise vai como link DENTRO do card do pagamento — "
            "ele não tem card próprio.")

    conferencia = conferir_pipe(pipe_id)
    # O arquivo de análise da MESMA competência, para o link entrar no card.
    analise = next((a for a in fpg.log(teto=400, ano=arquivo["ano"],
                                       mes=arquivo["mes"])
                    if a["destino"] == fpg.ANALISE and a["tipo"] == arquivo["tipo"]),
                   None)
    verbas = [v for v in (arquivo["verbas"] or "").split("+") if v]
    descricao = descricao_do_card(
        arquivo["competencia"], arquivo["tipo"], verbas, arquivo["conta"],
        arquivo["pessoas"], arquivo["total"], arquivo["link"],
        (analise or {}).get("link", ""))

    valores, de_fora = _valores_do_card(
        conferencia["reconhecidos"], descricao, arquivo["total"],
        arquivo["link"], (analise or {}).get("link", ""))
    if not valores:
        raise ErroDosCards(
            "não reconheci nenhum campo deste pipe, então eu não criei card "
            "nenhum. Confira o pipe na tela: é melhor não lançar do que lançar "
            "com os campos vazios.")

    titulo = (f"{geracao.ROTULO_DO_DESTINO.get(arquivo['destino'], '')} "
              f"{arquivo['competencia']}"
              + (f" - conta {arquivo['conta']}" if arquivo["conta"] else "")
              ).strip()
    card = pipefy.criar_card(pipe_id, titulo, valores)
    fpg.registrar_card(arquivo["id"], card["id"], card["link"])
    logger.info(
        "Folha: card %s criado para o arquivo %s (%s) por %s. Campos sem "
        "mapeamento: %s.", card["id"], arquivo["id"], arquivo["nome"],
        quem or "(sem nome)", ", ".join(de_fora) or "nenhum")
    return {"ok": True, "card": card["id"], "link": card["link"],
            "titulo": titulo, "sem_mapeamento": de_fora,
            "descricao": descricao}
