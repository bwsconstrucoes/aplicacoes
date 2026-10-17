# -*- coding: utf-8 -*-
"""
A RONDA SURPRESA DOS VIGIAS NOTURNOS (migração 011).

Pedido do dono, 13/10/2026: *"os vigias batem a entrada e a saída. Porém nada
garante que fiquem na obra o tempo todo (…) vamos fazer a ronda surpresa pelo
WhatsApp: ele bate a entrada, a saída, e faz quatro rondas surpresa. Só para os
vigias noturnos, vinculado ao cadastro dele."* E: *"o vigia inicia por volta das
17 às 18 horas e sai no outro dia de manhã; folga 36 horas. Para não ter perigo
de receber a ronda num dia que não esteja trabalhando: se ele não bater a
entrada — pode faltar —, também não tem ronda."*

COMO FUNCIONA
  · A ronda NÃO é batida de ponto: não tem NSR, não entra no cálculo da
    jornada (a Portaria 671 é da jornada). É um registro de presença à parte.
  · As rondas nascem na ENTRADA do vigia (`ao_bater`): a primeira batida dele à
    tarde/noite (15h às 23h), sem turno aberto. Sem entrada — folga, falta —,
    nada é agendado. Batida de manhã sem turno aberto não abre turno.
  · São QUANTAS (4) por turno, em horários sorteados: o turno (12 h, ou o da
    escala) é dividido em quatro partes, e cada ronda cai num ponto sorteado da
    sua parte — nunca na primeira nem na última hora, e com folga entre elas.
  · Na hora, sai um WhatsApp para o vigia com o link; ele tem PRAZO_MIN (10)
    minutos para abrir, tirar a foto e mandar a localização. Feita depois do
    prazo (até TOLERANCIA_ATRASO_MIN), fica ATRASADA; sem nada, PERDIDA — e sai
    um WhatsApp na hora para os telefones do resumo diário.
  · A SAÍDA (a próxima batida, passada a primeira meia hora) cancela as rondas
    que ainda não saíram. Turno esquecido aberto se fecha sozinho em 16 h.

O RELÓGIO: o serviço não tem agendador (rotina.py), e de madrugada ninguém usa o
sistema para acordá-lo. Então há uma linha separada (`garantir_relogio`) que, a
cada 30 s, faz uma rodada (`rodar`). Ela nasce quando o serviço sobe e é
conferida a cada requisição do ponto; se o serviço reiniciar, volta sozinha, e a
ronda que passou da hora com o serviço fora do ar vira NAO_ENVIADA (não se manda
ronda de meia hora atrás). Nada aqui derruba batida nem rota: tudo vira log.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import random
import secrets
import sys
import threading
import time
from typing import Callable, Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado

logger = logging.getLogger("ponto.rondas")

QUANTAS = 4                    # rondas por turno (decisão do dono, 13/10/2026)
PRAZO_MIN = 10                 # minutos para responder
TOLERANCIA_ATRASO_MIN = 30     # depois do prazo, ainda registra — como ATRASADA
TURNO_PADRAO_H = 12            # sem escala, o turno do vigia (12x36)
MARGEM_MIN = 60                # nem na primeira nem na última hora do turno
ENTRADA_DE, ENTRADA_ATE = 15, 23   # a entrada do vigia noturno cai nesta faixa (hora local)
REPETIDA_MIN = 30              # batida a menos disto da entrada não é a saída
TURNO_MAX_H = 16               # turno sem saída se fecha sozinho
ATRASO_MAX_ENVIO_MIN = 30      # ronda que passou disto sem sair (serviço fora) não sai mais
INTERVALO_RELOGIO_S = 30



def _vale(conn):
    """O ponto vale a partir de 16/10/2026 (vigencia.py): ronda de antes não aparece."""
    from . import vigencia
    return vigencia.inicio(conn)

def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "rondas", "id") and db.tem_coluna(conn, "colaborador_config", "faz_ronda")


def definir_vigia(conn: Connection, colaborador_id: int, faz_ronda: bool) -> None:
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 011) antes de marcar o vigia")
    db.executar(conn, """
        INSERT INTO ponto.colaborador_config (colaborador_id, faz_ronda) VALUES (:c, :f)
        ON CONFLICT (colaborador_id) DO UPDATE SET faz_ronda = :f, atualizado_em = now()
    """, c=int(colaborador_id), f=bool(faz_ronda))


def _hash(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# O sorteio (PURO)
# ---------------------------------------------------------------------------
def sortear_horarios(inicio: dt.datetime, duracao: dt.timedelta, rng: random.Random,
                     quantas: int = QUANTAS) -> list[dt.datetime]:
    """Os horários das rondas de um turno que começa em `inicio`. A janela útil
    (sem a primeira e a última hora) é dividida em `quantas` partes iguais, e
    cada ronda cai no miolo sorteado da sua parte: ninguém adivinha a hora, e
    duas rondas não saem coladas uma na outra."""
    ini = inicio + dt.timedelta(minutes=MARGEM_MIN)
    fim = inicio + duracao - dt.timedelta(minutes=MARGEM_MIN)
    if fim <= ini or quantas < 1:
        return []
    parte = (fim - ini) / quantas
    saida = []
    for i in range(quantas):
        comeco = ini + parte * i
        # o miolo da parte (do 15% ao 85%), para não colar na vizinha
        saida.append(comeco + parte * rng.uniform(0.15, 0.85))
    return [h.replace(microsecond=0) for h in saida]


def duracao_do_turno(conn: Connection, colaborador_id: int, entrada_local: dt.datetime) -> dt.timedelta:
    """A duração do turno pela escala (o período que começa perto da entrada); sem
    escala, 12 h."""
    try:
        from . import escalas
        esc, _ = escalas.EscalasDaPessoa(conn, colaborador_id).no_dia(entrada_local.date())
        if esc is not None:
            periodos = esc.periodos(entrada_local.date())
            if periodos:
                ini, fim = periodos[0][0], periodos[-1][1]
                if fim - ini >= 6 * 60:
                    return dt.timedelta(minutes=fim - ini)
    except Exception:  # noqa: BLE001 — sem escala legível, o padrão
        pass
    return dt.timedelta(hours=TURNO_PADRAO_H)


# ---------------------------------------------------------------------------
# A batida do vigia abre e fecha o turno
# ---------------------------------------------------------------------------
def _turno_aberto(conn: Connection, colaborador_id: int, agora: dt.datetime) -> Optional[dict]:
    return db.um(conn, """
        SELECT r.entrada_id, min(m.timestamp_servidor) AS entrada_em
          FROM ponto.rondas r JOIN ponto.marcacoes m ON m.id = r.entrada_id
         WHERE r.colaborador_id = :c AND m.timestamp_servidor > :desde
           AND r.status IN ('AGENDADA', 'ENVIADA')
         GROUP BY r.entrada_id ORDER BY entrada_em DESC LIMIT 1
    """, c=colaborador_id, desde=agora - dt.timedelta(hours=TURNO_MAX_H))


def ao_bater(conn: Connection, pessoa: dict, marcacao: dict, *, rng: Optional[random.Random] = None) -> str:
    """Chamada depois de cada batida aceita. Devolve o que fez ('', 'ABRIU',
    'FECHOU'). Nunca levanta erro: a batida já valeu."""
    try:
        if not pessoa.get("faz_ronda") or not disponivel(conn):
            return ""
        if marcacao.get("status") == "REJEITADA":
            return ""
        momento = marcacao["timestamp_servidor"]
        aberto = _turno_aberto(conn, int(pessoa["id"]), momento)
        if aberto:
            if momento - aberto["entrada_em"] < dt.timedelta(minutes=REPETIDA_MIN):
                return ""                 # a entrada batida de novo, não a saída
            n = db.executar(conn, """
                UPDATE ponto.rondas SET status = 'CANCELADA', motivo = 'saída batida antes da hora da ronda'
                 WHERE entrada_id = :e AND status = 'AGENDADA'""", e=aberto["entrada_id"])
            logger.info("Ponto: ronda — saída do vigia %s; %s ronda(s) cancelada(s)", pessoa["id"], n)
            return "FECHOU"
        local = horario.para_local(momento)
        if not (ENTRADA_DE <= local.hour < ENTRADA_ATE):
            return ""                     # de manhã, sem turno aberto: é saída de turno esquecido, não entrada
        rng = rng or random.Random()
        horarios = sortear_horarios(momento, duracao_do_turno(conn, int(pessoa["id"]), local), rng)
        for i, h in enumerate(horarios, start=1):
            db.executar(conn, """
                INSERT INTO ponto.rondas (colaborador_id, entrada_id, data_referencia, obra_id, numero, agendada_para)
                VALUES (:c, :e, :d, :o, :n, :h)""", c=int(pessoa["id"]), e=int(marcacao["id"]),
                d=marcacao.get("data_referencia") or local.date(), o=marcacao.get("obra_id"), n=i, h=h)
        logger.info("Ponto: ronda — entrada do vigia %s; %d ronda(s) sorteada(s)", pessoa["id"], len(horarios))
        return "ABRIU"
    except Exception as e:  # noqa: BLE001
        logger.warning("Ponto: ronda — falhou ao tratar a batida %s: %s", marcacao.get("id"), str(e)[:300])
        return ""


# ---------------------------------------------------------------------------
# A rodada do relógio: mandar o que chegou a hora, marcar o que passou do prazo
# ---------------------------------------------------------------------------
def _texto_da_ronda(nome: str, link: str) -> str:
    primeiro = (nome or "").split(" ")[0].title()
    return (f"🔦 *Ronda surpresa* — {primeiro}, registre a sua ronda AGORA: você tem {PRAZO_MIN} minutos.\n"
            f"Abra o link, tire a foto e deixe a localização ligada:\n{link}")


def _mandar(telefone: str, mensagem: str, finalidade: str, enviar: Optional[Callable]) -> tuple[bool, str]:
    from . import envios

    def padrao(**kw):
        from app.apps.notificador import notificar
        return notificar(canais=("whatsapp",), **kw)
    return envios.mandar({"telefone": telefone, "mensagem": mensagem, "finalidade": finalidade},
                         enviar=enviar or padrao)


def rodar(conn_fabrica: Callable, *, agora: Optional[dt.datetime] = None,
          enviar: Optional[Callable] = None) -> dict:
    """Uma rodada. `conn_fabrica()` abre uma transação (db.conexao): cada passo
    usa a sua, e o WhatsApp é chamado FORA delas (pode demorar)."""
    from . import cadastros, envios, rotina
    agora = agora or horario.agora()
    feito = {"enviadas": 0, "nao_enviadas": 0, "perdidas": 0, "fechadas": 0}
    with conn_fabrica() as conn:
        if not disponivel(conn):
            return feito
        # turno esquecido aberto: fecha
        feito["fechadas"] = db.executar(conn, """
            UPDATE ponto.rondas r SET status = 'CANCELADA', motivo = 'turno sem saída há mais de 16 h'
              FROM ponto.marcacoes m
             WHERE m.id = r.entrada_id AND r.status = 'AGENDADA' AND m.timestamp_servidor < :limite
        """, limite=agora - dt.timedelta(hours=TURNO_MAX_H)) or 0
        # passou da hora com o serviço fora do ar: não sai mais
        feito["nao_enviadas"] += db.executar(conn, """
            UPDATE ponto.rondas SET status = 'NAO_ENVIADA', motivo = 'o serviço estava fora do ar na hora da ronda'
             WHERE status = 'AGENDADA' AND agendada_para < :limite
        """, limite=agora - dt.timedelta(minutes=ATRASO_MAX_ENVIO_MIN)) or 0
        devidas = db.todos(conn, """
            SELECT id FROM ponto.rondas WHERE status = 'AGENDADA' AND agendada_para <= :agora
             ORDER BY agendada_para LIMIT 20""", agora=agora)
        endereco = envios.endereco_publico(conn)
    for linha in devidas:
        token = secrets.token_urlsafe(18)
        with conn_fabrica() as conn:
            r = db.um(conn, """SELECT r.*, c.nome FROM ponto.rondas r JOIN public.colaboradores c ON c.id = r.colaborador_id
                                WHERE r.id = :i AND r.status = 'AGENDADA' FOR UPDATE SKIP LOCKED""", i=linha["id"])
            if not r:
                continue
            pessoa = cadastros.colaborador_por_id(conn, int(r["colaborador_id"]))
            telefone = envios.telefone_valido(pessoa.get("telefone")) if pessoa else None
            if not telefone:
                db.executar(conn, "UPDATE ponto.rondas SET status = 'NAO_ENVIADA', motivo = :m WHERE id = :i",
                            m="o vigia não tem telefone válido no cadastro", i=r["id"])
                feito["nao_enviadas"] += 1
                continue
            # reserva antes de mandar: o link vale a partir de agora
            db.executar(conn, """UPDATE ponto.rondas SET status = 'ENVIADA', enviada_em = :a, prazo_ate = :p,
                                        token_hash = :h WHERE id = :i""",
                        a=agora, p=agora + dt.timedelta(minutes=PRAZO_MIN), h=_hash(token), i=r["id"])
        ok, detalhe = _mandar(telefone, _texto_da_ronda(r["nome"], f"{endereco}/ponto/app?ronda={token}"),
                              "ponto.ronda", enviar)
        if ok:
            feito["enviadas"] += 1
            logger.info("Ponto: ronda %s enviada ao vigia %s", r["id"], r["colaborador_id"])
        else:
            with conn_fabrica() as conn:
                db.executar(conn, """UPDATE ponto.rondas SET status = 'NAO_ENVIADA', token_hash = NULL,
                                            motivo = :m WHERE id = :i AND status = 'ENVIADA'""",
                            m=f"o WhatsApp não aceitou: {detalhe}"[:300], i=r["id"])
            feito["nao_enviadas"] += 1
            logger.warning("Ponto: ronda %s não saiu — %s", r["id"], detalhe[:200])
    # passou do prazo sem resposta: PERDIDA, e o aviso na hora
    with conn_fabrica() as conn:
        perdidas = db.todos(conn, """
            UPDATE ponto.rondas SET status = 'PERDIDA'
             WHERE status = 'ENVIADA' AND prazo_ate < :agora
            RETURNING id, colaborador_id, obra_id, agendada_para, enviada_em""", agora=agora)
        avisos = []
        if perdidas:
            telefones = rotina.telefones(conn)
            for p in perdidas:
                pessoa = cadastros.colaborador_por_id(conn, int(p["colaborador_id"]))
                obra = cadastros.obra_por_id(conn, int(p["obra_id"])) if p.get("obra_id") else None
                hora = horario.para_local(p["enviada_em"] or p["agendada_para"]).strftime("%H:%M")
                texto = (f"🚨 *Ronda perdida* — {pessoa['nome'] if pessoa else 'vigia'} não registrou a ronda "
                         f"das {hora}" + (f" na obra {obra['codigo']} — {obra['nome']}" if obra else "") +
                         f" (prazo de {PRAZO_MIN} min).")
                avisos += [(t, texto, p["id"]) for t in telefones]
        feito["perdidas"] = len(perdidas)
    for telefone, texto, rid in avisos:
        ok, _ = _mandar(telefone, texto, "ponto.ronda_perdida", enviar)
        if ok:
            with conn_fabrica() as conn:
                db.executar(conn, "UPDATE ponto.rondas SET avisado_em = :a WHERE id = :i", a=agora, i=rid)
    return feito


# ---------------------------------------------------------------------------
# O vigia registra a ronda (o link do WhatsApp)
# ---------------------------------------------------------------------------
def da_vez(conn: Connection, token: str, colaborador_id: int, *, agora: Optional[dt.datetime] = None) -> dict:
    """A ronda do link, se for desta pessoa. Fora do alcance responde 'não
    encontrada' (o mesmo para link errado e link de outro)."""
    r = db.um(conn, "SELECT * FROM ponto.rondas WHERE token_hash = :h", h=_hash(token)) if token else None
    if not r or int(r["colaborador_id"]) != int(colaborador_id):
        raise NaoEncontrado("ronda não encontrada — abra o link do WhatsApp mais recente")
    agora = agora or horario.agora()
    return {"id": r["id"], "status": r["status"], "numero": r["numero"],
            "enviada_em": horario.texto(r["enviada_em"]) if r.get("enviada_em") else None,
            "prazo_ate": r["prazo_ate"].isoformat() if r.get("prazo_ate") else None,
            "segundos_restantes": (max(0, int((r["prazo_ate"] - agora).total_seconds())) if r.get("prazo_ate") else 0),
            "feita_em": horario.texto(r["feita_em"]) if r.get("feita_em") else None}


def registrar(conn: Connection, token: str, colaborador_id: int, *, latitude=None, longitude=None,
              precisao=None, foto_base64: str = "", agora: Optional[dt.datetime] = None) -> dict:
    """Foto e localização são obrigatórias: é o que prova a presença."""
    from . import cadastros, fotos, geo
    agora = agora or horario.agora()
    r = db.um(conn, "SELECT * FROM ponto.rondas WHERE token_hash = :h FOR UPDATE", h=_hash(token)) if token else None
    if not r or int(r["colaborador_id"]) != int(colaborador_id):
        raise NaoEncontrado("ronda não encontrada — abra o link do WhatsApp mais recente")
    if r["status"] in ("FEITA", "ATRASADA"):
        raise ErroDeValidacao("esta ronda já foi registrada")
    if r["status"] not in ("ENVIADA", "PERDIDA"):
        raise ErroDeValidacao("esta ronda não está mais aberta")
    if agora > r["prazo_ate"] + dt.timedelta(minutes=TOLERANCIA_ATRASO_MIN):
        raise ErroDeValidacao("passou do prazo desta ronda — ela ficou como perdida")
    if not foto_base64:
        raise ErroDeValidacao("tire a foto para registrar a ronda", campo="foto_base64")
    if not geo.coordenada_valida(latitude, longitude):
        raise ErroDeValidacao("ligue a localização do celular para registrar a ronda", campo="latitude")
    foto = fotos.preparar(foto_base64)
    foto_id = fotos.guardar(conn, foto, nome=f"{horario.para_local(agora):%Y-%m-%d_%H%M%S}_colab{colaborador_id}_ronda{r['id']}.jpg",
                            momento=agora, subir=False)
    obra = cadastros.obra_por_id(conn, int(r["obra_id"])) if r.get("obra_id") else None
    dentro, distancia = None, None
    if obra:
        dentro, distancia, _ = geo.avaliar_cerca(latitude, longitude, obra.get("latitude"), obra.get("longitude"),
                                                 obra.get("raio_metros") or 200)
    status = "FEITA" if agora <= r["prazo_ate"] else "ATRASADA"
    db.executar(conn, """
        UPDATE ponto.rondas SET status = :s, feita_em = :a, latitude = :la, longitude = :lo, precisao_metros = :p,
               distancia_metros = :d, dentro_da_cerca = :dc, foto_id = :f
         WHERE id = :i""", s=status, a=agora, la=float(latitude), lo=float(longitude),
        p=(float(precisao) if precisao not in (None, "") else None), d=distancia, dc=dentro, f=foto_id, i=r["id"])
    logger.info("Ponto: ronda %s registrada (%s) pelo vigia %s — %s da obra", r["id"], status, colaborador_id,
                f"{distancia:.0f} m" if distancia is not None else "?")
    return {"status": status, "hora": horario.para_local(agora).strftime("%H:%M"), "dentro_da_cerca": dentro,
            "distancia_metros": distancia}


# ---------------------------------------------------------------------------
# Leitura: as rondas de uma pessoa (espelho) e as de uma noite (tela Rondas)
# ---------------------------------------------------------------------------
ROTULO = {"AGENDADA": "agendada", "ENVIADA": "esperando o vigia", "FEITA": "feita", "ATRASADA": "feita com atraso",
          "PERDIDA": "perdida", "CANCELADA": "cancelada", "NAO_ENVIADA": "não enviada"}


def para_json(r: dict, *, mostrar_agendada: bool = False) -> dict:
    hora = r.get("enviada_em") or (r["agendada_para"] if mostrar_agendada else None)
    return {"id": r["id"], "numero": r["numero"], "status": r["status"], "rotulo": ROTULO.get(r["status"], r["status"]),
            "hora": horario.para_local(hora).strftime("%H:%M") if hora else None,
            "feita": horario.para_local(r["feita_em"]).strftime("%H:%M") if r.get("feita_em") else None,
            "dentro_da_cerca": r.get("dentro_da_cerca"), "distancia_metros": r.get("distancia_metros"),
            "latitude": r.get("latitude"), "longitude": r.get("longitude"),
            "foto_id": r.get("foto_id"), "motivo": r.get("motivo")}


def da_pessoa(conn: Connection, colaborador_id: int, inicio: dt.date, fim: dt.date) -> dict[str, list[dict]]:
    """{data_referencia ISO: [rondas]} — a hora da ronda AGENDADA não aparece (é
    surpresa até sair: quem consulta o próprio espelho não descobre a próxima)."""
    if not disponivel(conn):
        return {}
    saida: dict[str, list[dict]] = {}
    for r in db.todos(conn, """SELECT * FROM ponto.rondas WHERE colaborador_id = :c
                                 AND data_referencia BETWEEN :i AND :f ORDER BY data_referencia, numero""",
                      c=colaborador_id, i=inicio, f=fim):
        saida.setdefault(r["data_referencia"].isoformat(), []).append(para_json(r))
    return saida


def da_noite(conn: Connection, dia: dt.date, obras: Optional[list[int]] = None) -> list[dict]:
    """Os turnos que COMEÇARAM em `dia` (a noite de `dia` para o dia seguinte),
    um por vigia, com a entrada, a saída e as rondas. `obras`: o recorte de quem
    consulta (None = todas)."""
    if not disponivel(conn):
        return []
    linhas = db.todos(conn, """
        SELECT r.*, c.nome, o.codigo AS obra_codigo, o.nome AS obra_nome, m.timestamp_servidor AS entrada_em
          FROM ponto.rondas r JOIN public.colaboradores c ON c.id = r.colaborador_id
          JOIN ponto.marcacoes m ON m.id = r.entrada_id
          LEFT JOIN public.obras o ON o.id = r.obra_id
         WHERE r.data_referencia = :d AND r.data_referencia >= :vale
         ORDER BY c.nome, r.entrada_id, r.numero""", d=dia, vale=_vale(conn))
    turnos: dict[int, dict] = {}
    for r in linhas:
        if obras is not None and r.get("obra_id") not in obras:
            continue
        t = turnos.setdefault(int(r["entrada_id"]), {
            "colaborador_id": r["colaborador_id"], "nome": r["nome"], "obra": r.get("obra_codigo"),
            "obra_nome": r.get("obra_nome"), "entrada": horario.para_local(r["entrada_em"]).strftime("%H:%M"),
            "saida": None, "rondas": []})
        t["rondas"].append(para_json(r, mostrar_agendada=True))
    for entrada_id, t in turnos.items():
        saida = db.um(conn, """SELECT m2.timestamp_servidor FROM ponto.marcacoes m1
                                 JOIN ponto.marcacoes m2 ON m2.colaborador_id = m1.colaborador_id
                                WHERE m1.id = :e AND m2.status <> 'REJEITADA'
                                  AND m2.timestamp_servidor > m1.timestamp_servidor + interval '30 minutes'
                                  AND m2.timestamp_servidor < m1.timestamp_servidor + interval '16 hours'
                                ORDER BY m2.timestamp_servidor LIMIT 1""", e=entrada_id)
        if saida:
            t["saida"] = horario.para_local(saida["timestamp_servidor"]).strftime("%H:%M")
        t["feitas"] = sum(1 for x in t["rondas"] if x["status"] in ("FEITA", "ATRASADA"))
        t["perdidas"] = sum(1 for x in t["rondas"] if x["status"] == "PERDIDA")
    return list(turnos.values())


# ---------------------------------------------------------------------------
# O relógio
# ---------------------------------------------------------------------------
_relogio: Optional[threading.Thread] = None
_relogio_trava = threading.Lock()


def _volta_do_relogio() -> None:
    time.sleep(5)
    while True:
        try:
            rodar(db.conexao)
        except Exception as e:  # noqa: BLE001 — o relógio nunca morre por erro de uma rodada
            logger.warning("Ponto: relógio das rondas — rodada falhou: %s", str(e)[:300])
        time.sleep(INTERVALO_RELOGIO_S)


def garantir_relogio() -> bool:
    """Liga o relógio das rondas se ele não estiver rodando neste processo. Não
    liga nos testes nem sem banco configurado."""
    global _relogio
    if "pytest" in sys.modules or os.getenv("PONTO_SEM_RELOGIO") or not os.getenv("DATABASE_URL"):
        return False
    if _relogio is not None and _relogio.is_alive():
        return True
    with _relogio_trava:
        if _relogio is not None and _relogio.is_alive():
            return True
        _relogio = threading.Thread(target=_volta_do_relogio, name="ponto-rondas", daemon=True)
        _relogio.start()
        logger.info("Ponto: relógio das rondas ligado")
    return True
