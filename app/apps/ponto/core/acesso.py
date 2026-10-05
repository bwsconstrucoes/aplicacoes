# -*- coding: utf-8 -*-
"""
Como o colaborador entra no "Meu ponto" do celular: CPF + PIN de 6 dígitos.

O PIN é criado (ou trocado) com um CÓDIGO de 6 dígitos mandado por WhatsApp para
o telefone do cadastro do ERP — o mesmo gateway que já avisa pagamento. Sem
e-mail, sem senha complicada: é gente de obra que vai usar.

PROTEÇÕES, cada uma com o motivo:
  · PIN e código só em hash (PBKDF2 com sal), nunca em claro;
  · 5 PINs errados bloqueiam por 15 minutos — CPF é fácil de descobrir, e PIN
    de 6 dígitos sem bloqueio cai em força bruta;
  · o código vale 15 minutos, morre no 5º erro e só serve uma vez;
  · no máximo 3 códigos por hora por pessoa — o WhatsApp da empresa não pode
    virar canhão de mensagem nas mãos de quem digita CPFs alheios;
  · a resposta ao pedido de código é A MESMA exista o CPF ou não: dizer
    "CPF não cadastrado" ensinaria quem trabalha na empresa.
  · PIN óbvio (123456, 000000, a data de nascimento não temos) é recusado.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import logging
import os
import re
import secrets

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoAutenticado
from . import cadastros, recusas

logger = logging.getLogger("ponto.acesso")

TENTATIVAS_PIN = 5
BLOQUEIO_MIN = 15
CODIGO_VALIDADE_MIN = 15
CODIGO_TENTATIVAS = 5
CODIGOS_POR_HORA = 3
_ITERACOES = 120_000
RESPOSTA_DO_CODIGO = ("Se o CPF estiver cadastrado com telefone, o código chega por WhatsApp "
                      "em instantes. Ele vale 15 minutos.")


def _hash(segredo: str, sal: bytes | None = None) -> str:
    sal = sal or os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", segredo.encode("utf-8"), sal, _ITERACOES)
    return f"{sal.hex()}${h.hex()}"


def _confere(segredo: str, guardado: str | None) -> bool:
    if not guardado or "$" not in guardado:
        return False
    sal_hex, h_hex = guardado.split("$", 1)
    calc = hashlib.pbkdf2_hmac("sha256", segredo.encode("utf-8"), bytes.fromhex(sal_hex), _ITERACOES)
    return hmac.compare_digest(calc.hex(), h_hex)


def validar_pin(pin: str) -> str:
    valor = re.sub(r"\D", "", str(pin or ""))
    if len(valor) != 6:
        raise ErroDeValidacao("o PIN tem 6 números", campo="pin")
    if len(set(valor)) == 1 or valor in ("123456", "654321", "012345", "123123", "112233"):
        raise ErroDeValidacao("PIN fácil demais — escolha outro", campo="pin")
    return valor


def _config(conn: Connection, colaborador_id: int) -> dict:
    db.executar(conn, "INSERT INTO ponto.colaborador_config (colaborador_id) VALUES (:c) "
                      "ON CONFLICT DO NOTHING", c=colaborador_id)
    return db.um(conn, "SELECT * FROM ponto.colaborador_config WHERE colaborador_id = :c",
                 c=colaborador_id)


def pedir_codigo(conn: Connection, cpf, *, ip: str = "", enviar=None) -> dict:
    """Manda o código por WhatsApp. Responde sempre igual (ver o cabeçalho)."""
    try:
        cpf_ok = cadastros.normalizar_cpf(cpf)
    except ErroDeValidacao:
        raise
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    telefone = re.sub(r"\D", "", (pessoa or {}).get("telefone") or "")
    if not pessoa or pessoa["situacao"] == "DESLIGADO" or len(telefone) < 10:
        logger.info("Ponto: código pedido para cpf %s sem cadastro/telefone", recusas.mascarar_cpf(cpf_ok))
        return {"mensagem": RESPOSTA_DO_CODIGO}
    recentes = db.um(conn, "SELECT count(*) AS n FROM ponto.codigos_acesso WHERE colaborador_id = :c "
                           "AND criado_em > now() - interval '1 hour'", c=pessoa["id"])["n"]
    if recentes >= CODIGOS_POR_HORA:
        logger.warning("Ponto: teto de códigos por hora para cpf %s", recusas.mascarar_cpf(cpf_ok))
        return {"mensagem": RESPOSTA_DO_CODIGO}
    codigo = f"{secrets.randbelow(1_000_000):06d}"
    db.executar(conn, """
        INSERT INTO ponto.codigos_acesso (colaborador_id, codigo_hash, expira_em, ip)
        VALUES (:c, :h, now() + make_interval(mins => :v), :ip)
    """, c=pessoa["id"], h=_hash(codigo), v=CODIGO_VALIDADE_MIN, ip=(ip or "")[:64])
    texto = (f"BWS Ponto: seu código é {codigo}. Ele vale {CODIGO_VALIDADE_MIN} minutos. "
             "Não passe este código para ninguém.")
    try:
        if enviar is None:
            from app.apps.notificador import notificar
            enviar = lambda tel, msg: notificar(telefone=tel, mensagem=msg, canais=("whatsapp",),  # noqa: E731
                                                finalidade="ponto.codigo_acesso")
        enviar(telefone if telefone.startswith("55") else "55" + telefone, texto)
    except Exception:  # noqa: BLE001 — falha de envio não revela nada a quem pediu
        logger.exception("Ponto: não consegui mandar o código por WhatsApp")
    return {"mensagem": RESPOSTA_DO_CODIGO}


def criar_pin(conn: Connection, cpf, codigo: str, pin: str) -> dict:
    """Confere o código e grava o PIN. Devolve a pessoa (já entra)."""
    cpf_ok = cadastros.normalizar_cpf(cpf)
    pin_ok = validar_pin(pin)
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    if not pessoa:
        raise NaoAutenticado("código inválido ou vencido")
    cod = db.um(conn, """
        SELECT * FROM ponto.codigos_acesso
         WHERE colaborador_id = :c AND usado_em IS NULL AND expira_em > now()
           AND tentativas < :t ORDER BY id DESC LIMIT 1
    """, c=pessoa["id"], t=CODIGO_TENTATIVAS)
    if not cod or not _confere(re.sub(r"\D", "", str(codigo or "")), cod["codigo_hash"]):
        if cod:
            # Em transação PRÓPRIA: a recusa desfaz a transação de quem chamou,
            # e o erro precisa ficar contado — senão o teto de tentativas não existe.
            with db.conexao() as separada:
                db.executar(separada, "UPDATE ponto.codigos_acesso SET tentativas = tentativas + 1 "
                                      "WHERE id = :id", id=cod["id"])
        raise NaoAutenticado("código inválido ou vencido")
    db.executar(conn, "UPDATE ponto.codigos_acesso SET usado_em = now() WHERE id = :id", id=cod["id"])
    _config(conn, pessoa["id"])
    db.executar(conn, """
        UPDATE ponto.colaborador_config SET pin_hash = :h, pin_criado_em = now(), pin_falhas = 0,
               pin_bloqueado_ate = NULL, atualizado_em = now() WHERE colaborador_id = :c
    """, h=_hash(pin_ok), c=pessoa["id"])
    logger.info("Ponto: PIN criado para cpf %s", recusas.mascarar_cpf(cpf_ok))
    return pessoa


def entrar(conn: Connection, cpf, pin: str) -> dict:
    """CPF + PIN. Levanta NaoAutenticado com a MESMA frase para qualquer erro,
    exceto o bloqueio (que precisa ser dito, senão a pessoa tenta para sempre)."""
    cpf_ok = cadastros.normalizar_cpf(cpf)
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    generico = NaoAutenticado("CPF ou PIN incorreto")
    if not pessoa or pessoa["situacao"] == "DESLIGADO" or not pessoa["ativo_no_ponto"]:
        raise generico
    cfg = _config(conn, pessoa["id"])
    if cfg.get("pin_bloqueado_ate") and cfg["pin_bloqueado_ate"] > horario.agora():
        minutos = int((cfg["pin_bloqueado_ate"] - horario.agora()).total_seconds() // 60) + 1
        raise NaoAutenticado(f"muitas tentativas erradas; tente de novo em {minutos} min")
    if not cfg.get("pin_hash"):
        raise NaoAutenticado("você ainda não criou o seu PIN — toque em “primeiro acesso”")
    if not _confere(re.sub(r"\D", "", str(pin or "")), cfg["pin_hash"]):
        falhas = int(cfg.get("pin_falhas") or 0) + 1
        bloqueio = (horario.agora() + dt.timedelta(minutes=BLOQUEIO_MIN)) if falhas >= TENTATIVAS_PIN else None
        # Transação PRÓPRIA, pelo mesmo motivo do código: a recusa desfaz a de
        # quem chamou, e sem isto o bloqueio por tentativas nunca chegaria.
        with db.conexao() as separada:
            db.executar(separada, "UPDATE ponto.colaborador_config SET pin_falhas = :f, "
                                  "pin_bloqueado_ate = :b WHERE colaborador_id = :c",
                        f=(0 if bloqueio else falhas), b=bloqueio, c=pessoa["id"])
        logger.warning("Ponto: PIN errado para cpf %s (%d)", recusas.mascarar_cpf(cpf_ok), falhas)
        raise generico
    db.executar(conn, "UPDATE ponto.colaborador_config SET pin_falhas = 0, pin_bloqueado_ate = NULL "
                      "WHERE colaborador_id = :c", c=pessoa["id"])
    return pessoa
