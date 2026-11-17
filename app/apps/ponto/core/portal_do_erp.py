# -*- coding: utf-8 -*-
"""
PEDIR ACESSO AO PORTAL, de dentro do "Meu ponto".

Desenho do dono, 17/11/2026: *"que tal a partir do ponto eu requerer a
liberação do acesso ao portal? (…) Ela já tem o cadastro no ponto, e lá pelo
ponto ela vai informar o e-mail dela, na hora que ela pediu acesso ao portal."*

POR QUE AQUI E NÃO NO ERP. São três logins diferentes e só um deles a gente
sabe que a pessoa tem: o ponto (CPF + PIN de 6 dígitos, código por WhatsApp).
O portal e o ERP pedem e-mail e senha — e o e-mail da pessoa de obra o sistema
não tem, porque ninguém pede e-mail para contratar. Resultado: o acesso não
nascia. Quem sabe o e-mail é a pessoa, e ela já está autenticada aqui.

A DECISÃO NÃO É DAQUI. Este módulo só coloca o pedido na fila do DP — a
liberação continua no ERP, por quem tem `gerir_usuarios`, pela mesma porta que
já existia ("Liberar acesso e convidar", na ficha da pessoa). O ponto não cria
usuário do ERP, não escolhe perfil e não manda convite.

⚠️ AMARRA ENTRE ÁREAS: a fila é `public.portal_pedidos_de_acesso`, criada pela
migração 108 do ERP, e a regra dela mora em
`app/apps/erp/core/auth/pedidos_de_portal.py`. Aqui não há SQL dessa tabela de
propósito: duas cópias da mesma regra divergem. Antes do botão da migração ser
apertado, a função do ERP responde "ainda não disponível" e a tela do ponto
esconde o botão — ninguém vê erro.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy.engine import Connection

logger = logging.getLogger("ponto.portal_do_erp")


def _regra():
    from app.apps.erp.core.auth import pedidos_de_portal
    return pedidos_de_portal


def meu_estado(conn: Connection, pessoa: dict) -> dict[str, Any]:
    """Em que pé está o acesso desta pessoa — para a tela saber o que mostrar.

    NUNCA LEVANTA. O "Meu ponto" existe para bater ponto; se esta consulta
    falhar, a pessoa continua batendo e só não vê o cartão do portal.
    """
    try:
        regra = _regra()
        if not regra.tabela_existe(conn):
            return {"disponivel": False}
        # JÁ TEM ACESSO? A ligação é `colaboradores.usuario_id` (migração 096).
        from .. import db
        ja = db.um(conn, """
            SELECT c.email AS email_no_cadastro, u.email, u.ativo
              FROM public.colaboradores c
              LEFT JOIN public.usuarios u ON u.id = c.usuario_id
             WHERE c.id = :c""", c=int(pessoa["id"]))
        if ja and ja.get("ativo"):
            return {"disponivel": True, "tem_acesso": True, "email": ja["email"]}
        pedido = regra.meu(conn, int(pessoa["id"]))
        return {"disponivel": True, "tem_acesso": False, "pedido": pedido,
                # O e-mail do cadastro, quando existe, vem preenchido: digitar
                # e-mail no celular é onde o erro de digitação nasce.
                "email_sugerido": (ja or {}).get("email_no_cadastro") or ""}
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: falha ao ver o estado do acesso ao portal")
        return {"disponivel": False}


def pedir(conn: Connection, pessoa: dict, email: str) -> dict[str, Any]:
    """Coloca o pedido na fila do DP, com a obra em que a pessoa bate ponto.

    A OBRA VEM DO PONTO, não de digitação: é onde ela mais bateu nos últimos
    60 dias (`cadastros.obra_pelo_ponto`), e na falta disso a do cadastro. Era
    a pergunta dele de 17/11 — *"mais onde é que a pessoa (…) de qual obra ela
    está cuidando"* — e o ponto é quem sabe responder, porque é o único lugar
    onde a presença fica registrada todo dia.
    """
    from . import cadastros
    obra = cadastros.obra_pelo_ponto(conn, int(pessoa["id"]))
    obra_id: Optional[int] = int(obra["id"]) if obra else (
        int(pessoa["obra_id"]) if pessoa.get("obra_id") else None)
    r = _regra().registrar(
        conn, colaborador_id=int(pessoa["id"]), cpf=pessoa.get("cpf") or "",
        nome=pessoa.get("nome") or "", email=email,
        telefone=pessoa.get("telefone") or "", obra_id=obra_id, origem="PONTO")
    if r.get("ok"):
        logger.info("Ponto: %s pediu acesso ao portal (obra %s)", pessoa.get("nome"), obra_id)
    return r
