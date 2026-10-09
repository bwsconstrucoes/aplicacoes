# -*- coding: utf-8 -*-
"""
A história das atualizações, contada para quem não é programador.

Pedido do dono em 06/10/2026: *"essa tela de atualizar os dados era para ter
mais informativo — uma sequência, a gente visualiza, entender o que é que
atualizou, até onde atualizou, onde é que interrompeu, foram quantas páginas,
foram quantas linhas, o que é que ficou pendente, qual foi a última tentativa
(…) e qual ação eu preciso fazer se der um erro. Está muito… ninguém entende
direito."*

Três respostas, uma por bloco da tela:

1. **Cada tipo de atualização** — quando foi a última que terminou bem e como
   foi a última tentativa. Responde "a releitura de pagamentos já rodou alguma
   vez inteira?".
2. **O histórico** — as últimas atualizações, cada uma com os PASSOS que deu
   (o que fez, até onde foi, onde parou) e o que fazer agora.
3. **O que fazer** — uma frase só, a partir do estado.

Os passos vêm da tabela `execucao_passos` (migração 020), gravada pelo mesmo
carimbo que mantém a atualização "viva". Sem a migração aplicada, a tela mostra
o que dá (a etapa em que parou) e diz que falta apertar o botão.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("painel.andamento")

# Os passos, com o nome que aparece na tela. São os MESMOS textos que a
# atualização grava ao andar — por isso moram aqui, num lugar só.
TITULOS_A_PAGAR = "lendo os títulos a pagar no OMIE"
TITULOS_A_RECEBER = "lendo os títulos a receber no OMIE"
PAGAMENTOS = "lendo os pagamentos no OMIE"
CADASTROS = "atualizando o plano de contas e os cadastros"
PLANILHA = "lendo a planilha de projetos"
EXCLUIDOS = "procurando títulos excluídos no OMIE"
RECALCULO = "recalculando os números do painel"
PAGAMENTOS_ANTIGOS = "relendo os pagamentos no OMIE, ano a ano"
PERIODO = "lendo os pagamentos do período no OMIE"
APROPRIACAO_CC = "lendo no OMIE as obras dos lançamentos de conta corrente"
TITULOS_TODOS = "relendo todos os títulos no OMIE"

_LEITURA = [TITULOS_A_PAGAR, TITULOS_A_RECEBER, PAGAMENTOS, CADASTROS, PLANILHA]
PASSOS_POR_MODO = {
    "rapida": _LEITURA + [APROPRIACAO_CC, RECALCULO],
    "pagamentos": _LEITURA + [PAGAMENTOS_ANTIGOS, APROPRIACAO_CC, RECALCULO],
    "completa": _LEITURA + [EXCLUIDOS, APROPRIACAO_CC, RECALCULO],
    "so_numeros": [RECALCULO],
    "titulos": [TITULOS_TODOS, PLANILHA, RECALCULO],
    "periodo": [TITULOS_A_PAGAR, TITULOS_A_RECEBER, PAGAMENTOS, CADASTROS, PERIODO,
                APROPRIACAO_CC, RECALCULO],
}

# O que cada situação quer dizer, e o que fazer — em português de gente.
RODANDO, PARADA, INTERROMPIDA, FALHOU, COM_AVISO, CONCLUIDA = (
    "rodando", "parada", "interrompida", "falhou", "com_aviso", "concluida")
ROTULO_DA_SITUACAO = {
    RODANDO: "Rodando agora",
    PARADA: "Parou de dar sinal",
    INTERROMPIDA: "Interrompida",
    FALHOU: "Falhou",
    COM_AVISO: "Concluída, com aviso",
    CONCLUIDA: "Concluída",
}
ROTULO_DO_DISPARO = {"agendado": "automática (madrugada)",
                     "retomada": "retomada automática",
                     "manual": "pelo botão"}


def situacao(e: dict) -> str:
    """Uma de RODANDO … CONCLUIDA, a partir da linha de `execucoes`."""
    if e.get("fim") is None:
        return RODANDO if e.get("viva") else PARADA
    if e.get("ok"):
        return COM_AVISO if "ATENÇÃO" in (e.get("mensagem") or "") else CONCLUIDA
    # o faxineiro de órfãs fecha deixando a etapa preenchida; o fim normal a zera
    return INTERROMPIDA if e.get("etapa") else FALHOU


def passos_da_execucao(e: dict, gravados: list[dict]) -> list[dict]:
    """Os passos esperados do modo, cada um com o que aconteceu nele.

    estado: "feito", "andando", "parou_aqui" ou "nao_chegou". Passos gravados
    que o modo não prevê (a primeira carga tem os dela) entram na ordem em que
    aconteceram."""
    sit = situacao(e)
    if not gravados:
        # Sem passos gravados (execução de antes da migração 020, ou com ela
        # pendente) não há como saber o que já foi feito — só onde está ou
        # parou. Marcar o resto como "feito" seria inventar (06/10/2026: a
        # tela mostrou seis passos "feitos" que ninguém viu acontecer).
        if not e.get("etapa"):
            return []
        estado = {RODANDO: "andando", CONCLUIDA: "feito", COM_AVISO: "feito"}.get(
            sit, "parou_aqui")
        return [{"etapa": e["etapa"], "estado": estado,
                 "detalhe": e.get("progresso") or "", "inicio": None,
                 "visto_em": None}]
    esperados = list(PASSOS_POR_MODO.get(e.get("tipo"), []))
    nomes = [p["etapa"] for p in gravados]
    for nome in nomes:
        if nome not in esperados:
            esperados.append(nome)
    por_nome = {p["etapa"]: p for p in gravados}
    # sem passos gravados (migração pendente, ou execução antiga): o que se
    # sabe é a etapa em que estava
    ultimo = nomes[-1] if nomes else (e.get("etapa") or "")
    if ultimo and ultimo not in esperados:
        esperados.append(ultimo)
    saida = []
    alcancou_ultimo = False
    for nome in esperados:
        p = por_nome.get(nome, {})
        if sit in (CONCLUIDA, COM_AVISO):
            estado = "feito" if (p or not gravados) else "nao_chegou"
        elif nome == ultimo:
            estado = "andando" if sit == RODANDO else "parou_aqui"
            alcancou_ultimo = True
        elif not alcancou_ultimo and (p or ultimo in esperados):
            estado = "feito"
        else:
            estado = "nao_chegou"
        saida.append({"etapa": nome, "estado": estado,
                      "detalhe": p.get("detalhe") or (
                          e.get("progresso") if nome == ultimo else ""),
                      "inicio": p.get("inicio"), "visto_em": p.get("visto_em")})
    return saida


def o_que_fazer(e: dict, rotulo: str, retomadas_seguidas: int = 0,
                teto_de_retomadas: int = 2) -> str:
    """A ação, numa frase — o que o dono pediu: "qual ação eu preciso fazer"."""
    sit = situacao(e)
    if sit == RODANDO:
        return "Nada: está rodando. Pode fechar a página; o trabalho continua no servidor."
    if sit == PARADA:
        if retomadas_seguidas >= teto_de_retomadas:
            return (f"Já foi retomada {retomadas_seguidas} vezes e parou de novo. "
                    f"Rode “{rotulo}” pelo botão num horário calmo; se parar outra "
                    "vez no mesmo passo, me mande esta tela.")
        return ("Nada, por enquanto: o painel a retoma sozinho em até 5 minutos. "
                "Se esta linha continuar assim depois disso, rode pelo botão.")
    if sit == INTERROMPIDA:
        return (f"O serviço reiniciou no meio (publicação de código, quase sempre). "
                f"Nada foi corrompido. Se ela não foi retomada logo abaixo, rode "
                f"“{rotulo}” de novo.")
    if sit == FALHOU:
        return (f"Rode “{rotulo}” de novo. Se falhar com a mesma mensagem, me "
                "mande a mensagem: é um erro, não um reinício.")
    if sit == COM_AVISO:
        return ("Os números foram refeitos, mas uma parte não deu certo (veja o "
                "aviso). Normalmente se resolve rodando de novo.")
    return ""


# ---------------------------------------------------------------------------
# Leitura do banco
# ---------------------------------------------------------------------------
def _passos_gravados(ids) -> tuple[dict, bool]:
    """{execucao_id: [passos em ordem]} e se a tabela existe."""
    from .db import consultar
    if not ids:
        return {}, True
    try:
        linhas = consultar(
            "SELECT execucao_id, etapa, ordem, inicio, visto_em, COALESCE(detalhe,'')"
            "  FROM execucao_passos WHERE execucao_id = ANY(?)"
            " ORDER BY execucao_id, ordem", [list(ids)])
    except Exception:  # noqa: BLE001 — migração 020 ainda não aplicada
        return {}, False
    from .horario import para_brasilia
    saida: dict = {}
    for eid, etapa, _ordem, inicio, visto, detalhe in linhas:
        saida.setdefault(eid, []).append({
            "etapa": etapa, "inicio": para_brasilia(inicio),
            "visto_em": para_brasilia(visto), "detalhe": detalhe})
    return saida, True


def historico(limite: int = 15) -> dict:
    """As últimas atualizações, cada uma com situação, passos e o que fazer."""
    from .consultas import MINUTOS_SEM_SINAL_ATE_MORTA
    from .db import consultar
    from .horario import para_brasilia
    from .tarefas import RETOMADAS_SEGUIDAS, ROTULOS

    linhas = consultar(
        "SELECT id, tipo, disparo, inicio, fim, ok, COALESCE(mensagem,''),"
        "       linhas_fato, etapa, COALESCE(progresso,''),"
        "       EXTRACT(EPOCH FROM (now() - COALESCE(visto_em, inicio))),"
        "       EXTRACT(EPOCH FROM (COALESCE(fim, now()) - inicio))"
        "  FROM execucoes ORDER BY inicio DESC LIMIT ?", [int(limite)])
    passos, tem_passos = _passos_gravados([l[0] for l in linhas])
    execucoes = []
    for (eid, tipo, disparo, inicio, fim, ok, mensagem, linhas_fato, etapa,
         progresso, silencio, duracao) in linhas:
        e = {"id": eid, "tipo": tipo, "disparo": disparo,
             "inicio": para_brasilia(inicio), "fim": para_brasilia(fim),
             "ok": ok, "mensagem": mensagem, "linhas": linhas_fato,
             "etapa": etapa, "progresso": progresso,
             "silencio_minutos": round(float(silencio or 0) / 60, 1),
             "duracao_minutos": round(float(duracao or 0) / 60, 1),
             "viva": float(silencio or 0) < MINUTOS_SEM_SINAL_ATE_MORTA * 60}
        e["rotulo"] = ROTULOS.get(tipo, tipo)
        e["disparo_rotulo"] = ROTULO_DO_DISPARO.get(disparo, disparo)
        e["situacao"] = situacao(e)
        e["situacao_rotulo"] = ROTULO_DA_SITUACAO[e["situacao"]]
        e["passos"] = passos_da_execucao(e, passos.get(eid, []))
        execucoes.append(e)

    # retomadas seguidas da mais recente — para a frase do "o que fazer"
    for i, e in enumerate(execucoes):
        seguidas = 0
        for anterior in execucoes[i:]:
            if anterior["tipo"] != e["tipo"]:
                continue
            if anterior["disparo"] != "retomada":
                break
            seguidas += 1
        e["o_que_fazer"] = o_que_fazer(e, e["rotulo"], seguidas, RETOMADAS_SEGUIDAS)

    return {"execucoes": execucoes, "tem_passos": tem_passos,
            "por_modo": por_modo(execucoes)}


def por_modo(execucoes: list[dict]) -> list[dict]:
    """Para cada tipo de atualização: a última que terminou bem e a última
    tentativa. Lê do histórico já carregado e, para o que não está nele, do
    banco (a última concluída pode ser antiga)."""
    from .db import consultar
    from .horario import para_brasilia
    from .tarefas import ROTULOS
    concluidas = {t: para_brasilia(f) for t, f in consultar(
        "SELECT tipo, MAX(fim) FROM execucoes WHERE ok GROUP BY tipo")}
    saida = []
    for tipo, rotulo in ROTULOS.items():
        ultima = next((e for e in execucoes if e["tipo"] == tipo), None)
        saida.append({"tipo": tipo, "rotulo": rotulo,
                      "ultima_concluida": concluidas.get(tipo),
                      "ultima_tentativa": ultima})
    return saida
