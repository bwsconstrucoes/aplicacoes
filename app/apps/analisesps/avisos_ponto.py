# -*- coding: utf-8 -*-
"""
O aviso a quem cuida quando a carga do ponto PARA.

Pedido do dono em 29/09/2026: *"Precisa que caso a carga pare que possa ser
retomada de onde parou e que sejamos avisados."* A retomada mora em
`ponto.carregar`; o aviso mora aqui.

Só o que parou avisa. O que entrou é o esperado, e aviso demais faz a pessoa
parar de ler — a mesma regra do aviso de comprovante não baixado do
BaixaBradesco, de onde vêm também os destinatários: o financeiro (quem resolve)
e o dono (quem decide). `ANALISESPS_AVISO_TELEFONE` troca a lista inteira, com
vários números separados por vírgula ou ponto e vírgula.

⚠️ AVISAR NUNCA DERRUBA NADA: falha no WhatsApp vira linha de log, não erro.
O que parou já está registrado em Configurações e na tela do Ponto; o aviso é
para não depender de alguém abrir a tela.
"""
from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger("analisesps.avisos_ponto")


def telefones() -> list:
    """Para quem vai o aviso. Ver o cabeçalho."""
    configurado = (os.getenv("ANALISESPS_AVISO_TELEFONE") or "").strip()
    if configurado:
        brutos = re.split(r"[;,]", configurado)
    else:
        try:
            from app.apps.baixabradesco.avisos import TELEFONES_AVISO
            brutos = list(TELEFONES_AVISO)
        except Exception:  # noqa: BLE001 — outra área; sem ela, sem padrão
            brutos = []
    saida = []
    for bruto in brutos:
        numero = re.sub(r"\D", "", bruto or "")
        if numero and numero not in saida:
            saida.append(numero)
    return saida


def montar_aviso(motivo: str, paradas: list) -> str:
    """O texto: o que parou, onde, e o que acontece em seguida."""
    linhas = ["⚠️ Ponto do Mobponto: a carga PAROU."]
    if paradas:
        for c in paradas:
            lidas, total = int(c.get("paginas_lidas") or 0), int(c.get("paginas") or 0)
            onde = (f"na página {lidas + 1} de {total}" if total else "no começo")
            linhas.append(f"• {c.get('competencia')}: parou {onde}"
                          + (f" ({lidas} página(s) já guardada(s))" if lidas else ""))
    if motivo:
        linhas.append(f"Motivo: {str(motivo).strip()[:300]}")
    linhas.append("O que já entrou fica guardado. A próxima carga do mês — pelo "
                  "botão em Folha PGT › Ponto ou pelo automático do dia — "
                  "continua de onde parou. Até lá o mês vale a carga anterior.")
    return "\n".join(linhas)


def avisar_que_parou(motivo: str) -> dict:
    """Manda o aviso. Devolve `{telefone: resultado}`; nunca levanta erro."""
    try:
        from . import ponto
        paradas = ponto.cargas_paradas()
    except Exception:  # noqa: BLE001 — o aviso sai mesmo sem a lista
        logger.exception("Análise de SPs: não consegui listar as cargas paradas")
        paradas = []
    texto = montar_aviso(motivo, paradas)

    resultados = {}
    for numero in telefones():
        try:
            from app.apps.notificador import notificar
            resultados[numero] = notificar(telefone=numero, mensagem=texto,
                                           canais=("whatsapp", "telegram"),
                                           politica="fallback")
        except Exception as e:  # noqa: BLE001 — um número não impede o outro
            logger.exception("Análise de SPs: aviso do ponto não saiu para %s", numero)
            resultados[numero] = {"ok": False, "erro": str(e)[:200]}
    if not resultados:
        logger.warning("Análise de SPs: a carga do ponto parou e não há telefone "
                       "para avisar (ANALISESPS_AVISO_TELEFONE).")
    return resultados
