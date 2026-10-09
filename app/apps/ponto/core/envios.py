# -*- coding: utf-8 -*-
"""
A fila de mensagens do ponto por WhatsApp — com RITMO.

Pedido do dono, 03/10/2026: *"o disparo de QR Codes tem que ter uma certa
aleatoriedade, não pode ser tudo de uma vez (…) a gente usa a API do WhatsApp
não oficial, isso pode gerar bloqueio (…) em dias diferentes, em horários
diferentes, para pessoas diferentes."*

O RITMO, e o motivo de cada número (são a sugestão pedida por ele):

  · **QR troca a cada 7 a 14 dias, sorteado por pessoa.** Com 400 pessoas dá
    uns 38 envios por dia, espalhados — nunca um lote. Mais curto que 7 dias
    aumenta o volume sem ganho real (quem empresta o QR empresta o novo também;
    o que pega esse caso é a foto). Mais longo que 14 deixa uma cópia valendo
    tempo demais.
  · **Envio automático só de segunda a sábado, das 7h30 às 17h30**, em hora
    sorteada. Mensagem de madrugada é o padrão de robô que o WhatsApp bloqueia.
  · **Entre uma mensagem e outra, de 30 a 90 segundos, sorteados.**
  · **Teto de 40 por hora e 200 por dia** (150 para o que é automático; o
    resto fica para quem PEDIU o QR e para o aviso do mosaico).
  · **Quem pede o QR de novo** ("esqueci", "troquei de celular") recebe na hora,
    das 6h às 22h, qualquer dia — até 3 pedidos por dia por pessoa.
  · **O primeiro envio para todo mundo** se espalha por vários dias (no máximo
    120 por dia), em vez de 400 de uma vez.
  · **Nada sai sozinho até alguém ligar** o envio automático em Ponto ›
    Configuração. Mandar mensagem para 400 pessoas é escrever em sistema de
    terceiro; quem decide a hora é o dono.

QUEM ENVIA: uma linha separada do servidor, acordada pelas próprias batidas (o
serviço não tem relógio — mesmo motivo da rotina do dia). Ela manda UMA
mensagem, dorme o intervalo sorteado, manda a próxima, e termina quando não há
mais nada na hora. Se o serviço reiniciar no meio, a fila está no banco e
continua na próxima batida.

O CÓDIGO DO QR É GERADO NA HORA DO ENVIO e só o hash é guardado, depois que o
WhatsApp aceitou. Mensagem que falhou não deixa QR valendo.
"""
from __future__ import annotations

import base64
import datetime as dt
import logging
import math
import random
import re
import threading
import time
from typing import Callable, Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import cadastros, parametros, qr

logger = logging.getLogger("ponto.envios")

# --- o ritmo ---------------------------------------------------------------
TROCA_MIN_DIAS, TROCA_MAX_DIAS = 7, 14
ESPACO_MIN_S, ESPACO_MAX_S = 30, 90
POR_HORA = 40
POR_DIA = 200
AUTOMATICOS_POR_DIA = 150
INICIAIS_POR_DIA = 120
PEDIDOS_POR_DIA_POR_PESSOA = 3
JANELA_AUTO = (dt.time(7, 30), dt.time(17, 30))        # segunda a sábado
JANELA_ATENDIMENTO = (dt.time(6, 0), dt.time(22, 0))   # todos os dias
TENTATIVAS_MAX = 3
PRESO_MIN = 15          # ENVIANDO há mais que isso = o serviço caiu no meio

PRIORIDADE = {("QR", "PEDIDO"): 1, ("QR", "GESTAO"): 1, ("MOSAICO", ""): 2,
              ("AVISO_FOTO", ""): 5, ("QR", "ROTACAO"): 6, ("QR", "INICIAL"): 7}
AUTOMATICOS = {("QR", "ROTACAO"), ("QR", "INICIAL"), ("AVISO_FOTO", "")}

QR_AUTOMATICO = "qr.envio_automatico"
AVISO_SEM_FOTO = "avisos.sem_foto"
ENDERECO = "sistema.endereco_publico"
ENDERECO_PADRAO = "https://aplicacoes.bwsconstrucoes.com.br"


# ---------------------------------------------------------------------------
# Regras puras (o teste percorre estas sem banco)
# ---------------------------------------------------------------------------
def e_automatico(tipo: str, motivo: str) -> bool:
    return (tipo, motivo or "") in AUTOMATICOS


def dentro_da_janela(tipo: str, motivo: str, momento: dt.datetime) -> bool:
    local = horario.para_local(momento)
    if e_automatico(tipo, motivo):
        if local.weekday() == 6:      # domingo
            return False
        inicio, fim = JANELA_AUTO
    else:
        inicio, fim = JANELA_ATENDIMENTO
    return inicio <= local.time() < fim


def sortear_horario(rng: random.Random, dia: dt.date) -> dt.datetime:
    """Uma hora sorteada dentro da janela automática daquele dia (domingo vira
    segunda). Devolve com fuso."""
    while dia.weekday() == 6:
        dia += dt.timedelta(days=1)
    inicio = dt.datetime.combine(dia, JANELA_AUTO[0], tzinfo=horario.FUSO)
    duracao = (dt.datetime.combine(dia, JANELA_AUTO[1]) - dt.datetime.combine(dia, JANELA_AUTO[0]))
    return inicio + dt.timedelta(seconds=rng.randrange(int(duracao.total_seconds())))


def sortear_proxima_troca(rng: random.Random, momento: dt.datetime) -> dt.datetime:
    """A próxima troca do QR: 7 a 14 dias depois, em hora sorteada da janela."""
    dia = horario.para_local(momento).date() + dt.timedelta(days=rng.randint(TROCA_MIN_DIAS,
                                                                            TROCA_MAX_DIAS))
    return sortear_horario(rng, dia)


def espaco_entre_envios(rng: random.Random) -> float:
    return rng.uniform(ESPACO_MIN_S, ESPACO_MAX_S)


def telefone_valido(bruto) -> Optional[str]:
    n = re.sub(r"\D", "", str(bruto or ""))
    if len(n) < 10:
        return None
    return n if n.startswith("55") and len(n) >= 12 else "55" + n


# ---------------------------------------------------------------------------
# Enfileirar
# ---------------------------------------------------------------------------
def enfileirar(conn: Connection, *, tipo: str, motivo: str = "", referencia: str, telefone: str,
               texto: str = "", colaborador_id: Optional[int] = None,
               usuario_id: Optional[int] = None, agendado_para: Optional[dt.datetime] = None,
               pedido_por: str = "") -> bool:
    """Põe na fila. A mesma `referencia` duas vezes não duplica (devolve False)."""
    n = db.executar(conn, """
        INSERT INTO ponto.envios (tipo, motivo, prioridade, referencia, colaborador_id, usuario_id,
                                  telefone, texto, agendado_para, pedido_por)
        VALUES (:t, :m, :p, :r, :c, :u, :tel, :txt, COALESCE(:ag, now()), :por)
        ON CONFLICT (referencia) DO NOTHING
    """, t=tipo, m=motivo or "", p=PRIORIDADE.get((tipo, motivo or ""), 5), r=referencia[:200],
         c=colaborador_id, u=usuario_id, tel=telefone, txt=texto[:2000], ag=agendado_para,
         por=pedido_por[:120])
    return n > 0


def pedir_qr(conn: Connection, colaborador_id: int, *, motivo: str = "PEDIDO",
             por: str = "", agora: Optional[dt.datetime] = None) -> dict:
    """O QR na hora: a pessoa pediu ("esqueci") ou a gestão mandou. Se já havia
    um QR esperando na fila, ele passa para a frente — não sai um segundo."""
    momento = agora or horario.agora()
    p = cadastros.colaborador_por_id(conn, colaborador_id)
    if not p or p["situacao"] == "DESLIGADO" or not p["ativo_no_ponto"]:
        return {"enfileirado": False, "motivo": "pessoa inativa"}
    telefone = telefone_valido(p.get("telefone"))
    if not telefone:
        return {"enfileirado": False, "motivo": "sem telefone no cadastro"}
    if motivo == "PEDIDO":
        hoje = db.um(conn, """
            SELECT count(*) AS n FROM ponto.envios
             WHERE tipo = 'QR' AND colaborador_id = :c AND motivo = 'PEDIDO'
               AND criado_em > now() - interval '1 day'
        """, c=colaborador_id)["n"]
        if hoje >= PEDIDOS_POR_DIA_POR_PESSOA:
            logger.warning("Ponto: teto de pedidos de QR para a pessoa %s", colaborador_id)
            return {"enfileirado": False, "motivo": "teto de pedidos do dia"}
    adiantado = db.executar(conn, """
        UPDATE ponto.envios SET motivo = :m, prioridade = 1, agendado_para = :ag, pedido_por = :por
         WHERE tipo = 'QR' AND colaborador_id = :c AND status = 'PENDENTE'
    """, m=motivo, ag=momento, por=por[:120], c=colaborador_id)
    if adiantado:
        return {"enfileirado": True, "adiantado": True}
    enfileirar(conn, tipo="QR", motivo=motivo, telefone=telefone, colaborador_id=colaborador_id,
               referencia=f"QR:{colaborador_id}:{motivo}:{momento.timestamp():.0f}",
               agendado_para=momento, pedido_por=por)
    logger.info("Ponto: QR pedido para a pessoa %s (%s, por %s)", colaborador_id, motivo, por or "—")
    return {"enfileirado": True}


def planejar(conn: Connection, *, agora: Optional[dt.datetime] = None,
             rng: Optional[random.Random] = None) -> dict:
    """Agenda as trocas de QR que vencem — só com o envio automático LIGADO.
    Roda na rotina do dia. Quem nunca recebeu QR entra espalhado por dias."""
    if parametros.ler(conn, QR_AUTOMATICO, "") != "1":
        return {"ligado": False, "iniciais": 0, "trocas": 0}
    momento = agora or horario.agora()
    rng = rng or random.Random()
    hoje = horario.para_local(momento).date()
    # As pessoas pelo cadastro do ponto (Registro de Colaboradores ou ERP —
    # cadastros.py decide), e não direto da tabela do ERP: o celular que vale é
    # o da base em uso.
    ativos = [p for p in cadastros.listar_colaboradores(conn, so_ativos=True)
              if p["situacao"] in ("ATIVO", "AFASTADO", "FORA_DO_REGISTRO")]
    com_qr = {r["colaborador_id"] for r in db.todos(
        conn, "SELECT DISTINCT colaborador_id FROM ponto.qr_codigos WHERE revogado_em IS NULL")}
    na_fila = {r["colaborador_id"] for r in db.todos(
        conn, "SELECT DISTINCT colaborador_id FROM ponto.envios WHERE tipo = 'QR' "
              "AND status IN ('PENDENTE', 'ENVIANDO')")}
    trocas = {r["colaborador_id"]: r["qr_proxima_troca"] for r in db.todos(
        conn, "SELECT colaborador_id, qr_proxima_troca FROM ponto.colaborador_config")}
    pessoas = [{"id": p["id"], "telefone": p.get("telefone"), "qr_proxima_troca": trocas.get(p["id"]),
                "tem_qr": p["id"] in com_qr, "na_fila": p["id"] in na_fila} for p in ativos]
    sem_qr = [p for p in pessoas if not p["na_fila"] and not p["tem_qr"]
              and telefone_valido(p["telefone"])]
    vencendo = [p for p in pessoas if not p["na_fila"] and p["tem_qr"]
                and telefone_valido(p["telefone"]) and p["qr_proxima_troca"] is not None
                and p["qr_proxima_troca"] <= momento + dt.timedelta(days=1)]
    rng.shuffle(sem_qr)
    dias = max(1, math.ceil(len(sem_qr) / INICIAIS_POR_DIA))
    for i, p in enumerate(sem_qr):
        dia = hoje + dt.timedelta(days=i % dias)
        quando = max(sortear_horario(rng, dia), momento)
        enfileirar(conn, tipo="QR", motivo="INICIAL", telefone=telefone_valido(p["telefone"]),
                   colaborador_id=p["id"], referencia=f"QR:{p['id']}:INICIAL:{hoje.isoformat()}",
                   agendado_para=quando, pedido_por="rotina")
    for p in vencendo:
        quando = max(p["qr_proxima_troca"], sortear_horario(rng, hoje), momento)
        enfileirar(conn, tipo="QR", motivo="ROTACAO", telefone=telefone_valido(p["telefone"]),
                   colaborador_id=p["id"], referencia=f"QR:{p['id']}:ROTACAO:{hoje.isoformat()}",
                   agendado_para=quando, pedido_por="rotina")
    if sem_qr or vencendo:
        logger.info("Ponto: QR — %d primeiros envios (em %d dia(s)) e %d trocas agendadas",
                    len(sem_qr), dias, len(vencendo))
    return {"ligado": True, "iniciais": len(sem_qr), "dias": dias, "trocas": len(vencendo)}


# ---------------------------------------------------------------------------
# Tirar da fila e mandar
# ---------------------------------------------------------------------------
def _contagens(conn: Connection, momento: dt.datetime) -> dict:
    inicio_dia = dt.datetime.combine(horario.para_local(momento).date(), dt.time(0), tzinfo=horario.FUSO)
    linha = db.um(conn, """
        SELECT count(*) FILTER (WHERE enviado_em > :h) AS hora,
               count(*) FILTER (WHERE enviado_em >= :d) AS dia,
               count(*) FILTER (WHERE enviado_em >= :d AND (
                     (tipo = 'QR' AND motivo IN ('ROTACAO', 'INICIAL')) OR tipo = 'AVISO_FOTO')) AS auto
          FROM ponto.envios
         WHERE status IN ('ENVIADO', 'ENVIANDO') AND enviado_em >= LEAST(:h, :d)
    """, h=momento - dt.timedelta(hours=1), d=inicio_dia)
    return {k: int(v or 0) for k, v in linha.items()}


def pegar_proximo(conn: Connection, agora: Optional[dt.datetime] = None) -> Optional[dict]:
    """O próximo que pode sair AGORA (janela e tetos), já marcado ENVIANDO."""
    momento = agora or horario.agora()
    db.executar(conn, """
        UPDATE ponto.envios SET status = 'PENDENTE', pego_em = NULL
         WHERE status = 'ENVIANDO' AND pego_em < :limite
    """, limite=momento - dt.timedelta(minutes=PRESO_MIN))
    cont = _contagens(conn, momento)
    if cont["hora"] >= POR_HORA or cont["dia"] >= POR_DIA:
        return None
    candidatos = db.todos(conn, """
        SELECT * FROM ponto.envios
         WHERE status = 'PENDENTE' AND agendado_para <= :m
         ORDER BY prioridade, agendado_para, id
         LIMIT 25 FOR UPDATE SKIP LOCKED
    """, m=momento)
    # Desligar a chave do QR automático vale NA HORA, também para o que já
    # estava na fila: a troca agendada espera; o pedido da pessoa, não.
    qr_ligado = parametros.ler(conn, QR_AUTOMATICO, "") == "1"
    for c in candidatos:
        if not dentro_da_janela(c["tipo"], c["motivo"], momento):
            continue
        if c["tipo"] == "QR" and c["motivo"] in ("ROTACAO", "INICIAL") and not qr_ligado:
            continue
        if e_automatico(c["tipo"], c["motivo"]) and cont["auto"] >= AUTOMATICOS_POR_DIA:
            continue
        db.executar(conn, "UPDATE ponto.envios SET status = 'ENVIANDO', pego_em = :m, "
                          "tentativas = tentativas + 1 WHERE id = :id", m=momento, id=c["id"])
        c["tentativas"] = int(c["tentativas"]) + 1
        return c
    return None


def _texto_do_qr(nome: str, motivo: str) -> str:
    primeiro = (nome or "").split(" ")[0].title()
    abertura = {"PEDIDO": "aqui está o QR Code que você pediu",
                "INICIAL": "este é o seu QR Code para bater o ponto"}.get(motivo, "este é o seu NOVO QR Code para bater o ponto")
    return (f"BWS Ponto — {primeiro}, {abertura}.\n\n"
            "No tablet da obra, mostre esta imagem na câmera: ele reconhece você e tira a foto.\n"
            "O QR anterior para de valer assim que você usar este.\n\n"
            "É pessoal, como a sua assinatura: não passe para ninguém. "
            "Se perder o celular, avise a administração da obra.")


# O TIPO de cada mensagem na mensageria (tela Mensagens do ERP): é lá que o
# dono decide se sai por WhatsApp. Pedido dele em 05/10/2026: ponto liberado.
FINALIDADE = {"QR": "ponto.qr", "MOSAICO": "ponto.mosaico", "AVISO_FOTO": "ponto.aviso_foto"}


def _notificar_padrao(**kw) -> dict:
    from app.apps.notificador import notificar
    kw.setdefault("finalidade", "ponto.qr")
    return notificar(canais=("whatsapp",), **kw)


def preparar(conn: Connection, item: dict) -> tuple[Optional[dict], Optional[str], str]:
    """Monta a mensagem. Devolve (argumentos do envio, token do QR, problema).
    O QR é gerado AQUI, na hora do envio — e só vira hash no banco depois que
    o WhatsApp aceitou (`concluir`)."""
    if item["tipo"] == "QR":
        p = cadastros.colaborador_por_id(conn, int(item["colaborador_id"]))
        if not p or p["situacao"] == "DESLIGADO" or not p["ativo_no_ponto"]:
            return None, None, "pessoa desligada ou inativa — envio cancelado"
        token = qr.novo_token()
        jpeg = qr.imagem(token, formato="JPEG", legenda=p["nome"].split(" ")[0].upper())
        return ({"telefone": item["telefone"], "mensagem": _texto_do_qr(p["nome"], item["motivo"]),
                 "arquivo_base64": base64.b64encode(jpeg).decode("ascii"),
                 "nome_arquivo": "qr-ponto.jpg", "tipo": "imagem", "finalidade": "ponto.qr"}, token, "")
    return {"telefone": item["telefone"], "mensagem": item["texto"],
            "finalidade": FINALIDADE.get(item["tipo"], "ponto.qr")}, None, ""


def mandar(argumentos: dict, enviar: Optional[Callable] = None) -> tuple[bool, str]:
    """Fala com o WhatsApp. Fora de transação: pode demorar, e o banco não
    pode ficar preso esperando."""
    enviar = enviar or _notificar_padrao
    try:
        resultado = enviar(**argumentos)
        wa = (resultado or {}).get("whatsapp") or {}
        return bool(wa.get("ok")), str(wa.get("detalhe") or wa.get("erro") or resultado)[:500]
    except Exception as e:  # noqa: BLE001 — falha de envio nunca derruba nada
        logger.warning("Ponto: envio falhou: %s", str(e)[:200])
        return False, str(e)[:500]


def concluir(conn: Connection, item: dict, ok: bool, resultado: str, token: Optional[str],
             *, agora: Optional[dt.datetime] = None, rng: Optional[random.Random] = None) -> str:
    momento = agora or horario.agora()
    if ok:
        db.executar(conn, "UPDATE ponto.envios SET status = 'ENVIADO', enviado_em = :m, resultado = :r "
                          "WHERE id = :id", m=momento, r=resultado, id=item["id"])
        if item["tipo"] == "QR" and token:
            qr.gravar_enviado(conn, int(item["colaborador_id"]), token, item["motivo"] or "ROTACAO")
            db.executar(conn, """
                INSERT INTO ponto.colaborador_config (colaborador_id, qr_proxima_troca) VALUES (:c, :t)
                ON CONFLICT (colaborador_id) DO UPDATE SET qr_proxima_troca = :t
            """, c=item["colaborador_id"], t=sortear_proxima_troca(rng or random.Random(), momento))
        return "ENVIADO"
    if "cancelado" in resultado or int(item["tentativas"]) >= TENTATIVAS_MAX:
        situacao = "CANCELADO" if "cancelado" in resultado else "FALHOU"
        db.executar(conn, "UPDATE ponto.envios SET status = :s, resultado = :r WHERE id = :id",
                    s=situacao, r=resultado, id=item["id"])
        logger.warning("Ponto: envio %s %s — %s", item["id"], situacao.lower(), resultado[:200])
        return situacao
    db.executar(conn, """
        UPDATE ponto.envios SET status = 'PENDENTE', resultado = :r, pego_em = NULL,
               agendado_para = :ag WHERE id = :id
    """, r=resultado, ag=momento + dt.timedelta(minutes=10 * int(item["tentativas"])), id=item["id"])
    return "PENDENTE"


def processar_um(enviar: Optional[Callable] = None, agora: Optional[dt.datetime] = None) -> Optional[str]:
    """Um envio, em três passos e duas transações: pegar (marca ENVIANDO),
    mandar (fora de transação — o WhatsApp pode demorar), concluir."""
    with db.conexao() as conn:
        item = pegar_proximo(conn, agora)
        if not item:
            return None
        argumentos, token, problema = preparar(conn, item)
    if argumentos is None:
        ok, resultado = False, problema
    else:
        ok, resultado = mandar(argumentos, enviar)
    with db.conexao() as conn:
        return concluir(conn, item, ok, resultado, token, agora=agora)


# ---------------------------------------------------------------------------
# A linha que envia
# ---------------------------------------------------------------------------
VERIFICAR_A_CADA_S = 60
_trabalhando = threading.Lock()
_ultima_verificacao = 0.0


def whatsapp_pronto() -> bool:
    """Credenciais do Z-API presentes E o WhatsApp liberado para o QR do ponto.
    Quem decide é a mensageria (tela Mensagens do ERP: política do tipo
    "QR Code do ponto" e a chave geral do WhatsApp); sem ela no banco, as
    variáveis NOTIFICAR_WHATSAPP / NOTIFICAR_WHATSAPP_PONTO_QR. (Até
    05/10/2026 este teste lia um nome de token que não existia no Render e a
    fila nunca saía do lugar.)"""
    from app.apps.notificador import canal_ativo, whatsapp_configurado
    return whatsapp_configurado() and canal_ativo("whatsapp", "ponto.qr")


def _trabalhar() -> None:
    rng = random.Random()
    try:
        while True:
            situacao = processar_um()
            if situacao is None:
                return
            time.sleep(espaco_entre_envios(rng))
    except Exception:  # noqa: BLE001 — a fila nunca derruba nada
        logger.exception("Ponto: a fila de envios parou; volta na próxima batida")
    finally:
        _trabalhando.release()


def disparar_se_preciso() -> bool:
    """Chamada barata em toda requisição do ponto: no máximo uma olhada por
    minuto, e nenhuma se o WhatsApp não estiver configurado."""
    global _ultima_verificacao
    agora = time.time()
    if agora - _ultima_verificacao < VERIFICAR_A_CADA_S:
        return False
    _ultima_verificacao = agora
    if not whatsapp_pronto():
        return False
    if not _trabalhando.acquire(blocking=False):
        return False
    threading.Thread(target=_trabalhar, name="ponto-envios", daemon=True).start()
    return True


def _acordar() -> bool:
    if not _trabalhando.acquire(blocking=False):
        return False
    threading.Thread(target=_trabalhar, name="ponto-envios", daemon=True).start()
    return True


def enviar_agora() -> bool:
    """Chamada DEPOIS de um pedido entrar na fila, com a transação já fechada:
    acorda a linha que envia na hora, sem esperar o minuto nem a próxima batida.

    O defeito de 09/10/2026 ("precisei solicitar umas três vezes para ele
    chegar"): a fila só era acordada no COMEÇO de cada requisição do ponto,
    antes de o pedido ser gravado — olhava, não achava nada, e só voltava a olhar
    um minuto depois, SE viesse outra requisição. O pedido feito pela tela do
    ERP nem isso. Se a linha já está enviando, ela pega o pedido na volta (ele é
    o primeiro da fila); se estava terminando naquele instante, a nova olhada
    em 15 s garante."""
    global _ultima_verificacao
    try:
        if not whatsapp_pronto():
            return False
    except Exception:  # noqa: BLE001 — sem mensageria, o pedido fica na fila
        return False
    _ultima_verificacao = time.time()
    if _acordar():
        return True
    t = threading.Timer(15, _acordar)
    t.daemon = True
    t.start()
    return False


# ---------------------------------------------------------------------------
# Para a tela
# ---------------------------------------------------------------------------
def resumo(conn: Connection) -> dict:
    linhas = db.todos(conn, """
        SELECT tipo, status, count(*) AS n FROM ponto.envios
         WHERE criado_em > now() - interval '14 days' OR status IN ('PENDENTE', 'ENVIANDO')
         GROUP BY tipo, status
    """)
    cont = _contagens(conn, horario.agora())
    proximo = db.um(conn, "SELECT min(agendado_para) AS t FROM ponto.envios WHERE status = 'PENDENTE'")
    return {"por_tipo": linhas, "na_ultima_hora": cont["hora"], "hoje": cont["dia"],
            "proximo": horario.texto((proximo or {}).get("t")),
            "whatsapp_configurado": whatsapp_pronto(),
            "ritmo": {"troca_dias": [TROCA_MIN_DIAS, TROCA_MAX_DIAS],
                      "espaco_segundos": [ESPACO_MIN_S, ESPACO_MAX_S],
                      "por_hora": POR_HORA, "por_dia": POR_DIA,
                      "automaticos_por_dia": AUTOMATICOS_POR_DIA,
                      "janela_automatica": "segunda a sábado, 7h30 às 17h30",
                      "pedidos_por_dia_por_pessoa": PEDIDOS_POR_DIA_POR_PESSOA}}


def endereco_publico(conn: Connection) -> str:
    return (parametros.ler(conn, ENDERECO, "") or ENDERECO_PADRAO).rstrip("/")


def lembrar_endereco(conn: Connection, url_raiz: str) -> None:
    valor = (url_raiz or "").strip().rstrip("/")
    if not valor.startswith("https://") or "localhost" in valor:
        return
    if parametros.ler(conn, ENDERECO, "") != valor:
        parametros.gravar(conn, ENDERECO, valor, "o próprio sistema")


def validar_ligado(valor) -> str:
    if valor in (True, "1", 1, "true", "sim"):
        return "1"
    if valor in (False, "0", 0, "false", "nao", "não", "", None):
        return ""
    raise ErroDeValidacao("use ligado ou desligado", campo="valor")
