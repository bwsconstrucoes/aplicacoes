# -*- coding: utf-8 -*-
"""
A rotina do dia: gerar os alertas e mandar o resumo por WhatsApp — uma vez por
dia, sem ninguém apertar botão. Desde a migração 003, também abre a conferência
do mosaico de ontem nas obras obrigatórias e agenda as trocas de QR Code que
vencem (espalhadas na fila de envios — ver envios.py).

POR QUE ELA SE DISPARA SOZINHA NA PRIMEIRA REQUISIÇÃO DO DIA: o serviço não tem
relógio próprio (o Render roda o gunicorn, não um agendador), e o ERP só tem
fila de trabalho, não horário marcado. Mas o ponto tem uma garantia que nenhum
outro módulo tem: toda manhã centenas de pessoas batem ponto. A primeira
requisição do ponto depois das 6h dispara a rotina numa linha separada — quem
bateu não espera por ela.

DUAS TRAVAS contra rodar duas vezes: uma em memória (o mesmo processo) e a
trava do Postgres (`pg_try_advisory_lock`), que vale até se um dia houver mais
de um processo. A data da última rodada fica em `ponto.parametros`.

AVISAR NUNCA DERRUBA NADA: falha no WhatsApp vira linha de log.
"""
from __future__ import annotations

import logging
import threading

from .. import db, horario
from . import alertas, parametros

logger = logging.getLogger("ponto.rotina")

HORA_MINIMA = 6
_TRAVA_PG = 7_671_0001
_em_andamento = threading.Lock()
# Depois que a rotina rodou (ou se viu que já tinha rodado), o resto do dia não
# custa nem uma consulta ao banco por requisição.
_dia_resolvido: str | None = None


def telefones(conn) -> list[str]:
    import re
    bruto = parametros.ler(conn, parametros.TELEFONES_RESUMO, "")
    saida = []
    # Separados por vírgula, ponto e vírgula ou linha — NÃO por espaço, porque
    # "(85) 99999-1111" tem espaço dentro do próprio número.
    for t in re.split(r"[;,\n]+", bruto):
        n = re.sub(r"\D", "", t)
        if len(n) >= 10 and n not in saida:
            saida.append(n if n.startswith("55") else "55" + n)
    return saida


def enviar_resumo(conn, *, forcar: bool = False) -> dict:
    hoje = horario.hoje()
    texto = alertas.resumo_do_dia(conn, hoje)
    enviados = []
    for tel in telefones(conn):
        referencia = f"{hoje.isoformat()}:{tel}"
        if not forcar and db.um(conn, "SELECT 1 AS x FROM ponto.resumos_enviados WHERE referencia = :r",
                                r=referencia):
            continue
        try:
            from app.apps.notificador import notificar
            resultado = notificar(telefone=tel, mensagem=texto, canais=("whatsapp",),
                                  finalidade="ponto")
            ok = bool((resultado.get("whatsapp") or {}).get("ok"))
        except Exception as e:  # noqa: BLE001
            resultado, ok = {"erro": str(e)[:200]}, False
        db.executar(conn, """
            INSERT INTO ponto.resumos_enviados (referencia, destino, texto, resultado)
            VALUES (:r, :d, :t, :res) ON CONFLICT (referencia) DO UPDATE SET resultado = :res
        """, r=(referencia if not forcar else f"{referencia}:{horario.agora():%H%M%S}"), d=tel,
             t=texto, res=str(resultado)[:500])
        enviados.append({"telefone": tel[-4:], "ok": ok})
        logger.info("Ponto: resumo do dia para ***%s — %s", tel[-4:], "ok" if ok else "falhou")
    return {"texto": texto, "enviados": enviados}


def rodar(conn) -> dict:
    import datetime as dt
    extras = {}
    if db.tem_003(conn):
        # Antes dos alertas: a conferência de ontem nasce agora, e o alerta de
        # "mosaico sem conferência" só abre para a de anteontem para trás.
        from . import envios, mosaico
        extras["mosaicos"] = mosaico.preparar_do_dia(conn, horario.hoje() - dt.timedelta(days=1))
        extras["qr"] = envios.planejar(conn)
    resultado = alertas.gerar(conn)
    resultado.update(extras)
    resultado["resumo"] = enviar_resumo(conn)
    parametros.gravar(conn, parametros.ULTIMA_ROTINA, horario.hoje().isoformat(), "rotina")
    return resultado


def precisa_rodar(conn) -> bool:
    agora = horario.para_local(horario.agora())
    if agora.hour < HORA_MINIMA:
        return False
    return parametros.ler(conn, parametros.ULTIMA_ROTINA, "") != agora.date().isoformat()


def _trabalhar() -> None:
    try:
        with db.conexao() as conn:
            pegou = conn.exec_driver_sql("SELECT pg_try_advisory_xact_lock(%s)", (_TRAVA_PG,)).scalar()
            if not pegou or not precisa_rodar(conn):
                return
            rodar(conn)
        global _dia_resolvido
        _dia_resolvido = horario.hoje().isoformat()
    except Exception:  # noqa: BLE001 — rotina nunca derruba nada
        logger.exception("Ponto: a rotina do dia falhou; tenta de novo na próxima requisição")
    finally:
        _em_andamento.release()


def disparar_se_preciso() -> bool:
    """Chamada barata em toda requisição do ponto. Devolve True se disparou."""
    global _dia_resolvido
    agora = horario.para_local(horario.agora())
    if _dia_resolvido == agora.date().isoformat() or agora.hour < HORA_MINIMA:
        return False
    if not _em_andamento.acquire(blocking=False):
        return False
    try:
        with db.conexao() as conn:
            if not db.schema_existe(conn) or not _tabela(conn):
                _em_andamento.release()
                return False
            if not precisa_rodar(conn):
                _dia_resolvido = agora.date().isoformat()
                _em_andamento.release()
                return False
    except Exception:  # noqa: BLE001 — sem banco, sem rotina
        _em_andamento.release()
        return False
    threading.Thread(target=_trabalhar, name="ponto-rotina-diaria", daemon=True).start()
    return True


def _tabela(conn) -> bool:
    return db.um(conn, "SELECT 1 AS x FROM information_schema.tables "
                       "WHERE table_schema = 'ponto' AND table_name = 'parametros'") is not None
