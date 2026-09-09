# ============================================================================
# ERP — core/cadastros/vinculos.py
# A ligação entre OPERADOR e OBRA, escrita num lugar só.
#
# POR QUE ESTE MÓDULO EXISTE
#
# O dono pediu para marcar quem responde pela obra pelos DOIS lados: "tanto
# associar o operador à obra indo pelo cadastro do operador, ou então indo pelo
# cadastro da obra… porque facilita o manuseio do sistema".
#
# Duas telas mexendo na mesma ligação é onde as coisas divergem. E já havia
# uma armadilha pronta: a tela do operador APAGA todos os vínculos dele e
# recria — o que apagaria, sem avisar, a marca de responsável feita na tela da
# obra. Por isso as duas telas passam por aqui, e não escrevem na tabela
# diretamente. É o mesmo motivo de `aplicar_escopo` ser único: se a regra mora
# em dois lugares, um dia os dois discordam.
#
# ESTAR LIGADO À OBRA é ENXERGAR. `responsavel` é RESPONDER — é quem a
# conferência mensal cobra e para quem o agente manda mensagem. Quem responde
# tem de enxergar, e é por isso que a marca vive na mesma linha.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Obra, Usuario, UsuarioObra

logger = logging.getLogger(__name__)


def _numeros(valores: Optional[Iterable[Any]]) -> list[int]:
    saida = []
    for v in valores or []:
        try:
            saida.append(int(v))
        except (TypeError, ValueError):
            continue
    return saida


def obras_do_operador(s: Session, usuario_id: int) -> list[dict[str, Any]]:
    """As obras deste operador, dizendo por quais ele responde."""
    obras = {o.id: o for o in s.scalars(select(Obra)).all()}
    saida = []
    for v in s.scalars(select(UsuarioObra).where(
            UsuarioObra.usuario_id == usuario_id)).all():
        o = obras.get(v.obra_id)
        saida.append({"obra_id": v.obra_id,
                      "codigo": getattr(o, "codigo", ""),
                      "nome": getattr(o, "nome", ""),
                      "responsavel": bool(v.responsavel)})
    saida.sort(key=lambda x: x["codigo"])
    return saida


def operadores_da_obra(s: Session, obra_id: int) -> list[dict[str, Any]]:
    """Os operadores ligados a esta obra, dizendo quais respondem por ela."""
    pessoas = {u.id: u for u in s.scalars(select(Usuario)).all()}
    saida = []
    for v in s.scalars(select(UsuarioObra).where(
            UsuarioObra.obra_id == obra_id)).all():
        u = pessoas.get(v.usuario_id)
        if u is None:
            continue
        saida.append({"usuario_id": u.id, "nome": u.nome, "email": u.email,
                      "perfil": u.perfil.value if u.perfil else "",
                      "telefone": u.telefone or "",
                      "responsavel": bool(v.responsavel)})
    saida.sort(key=lambda x: (not x["responsavel"], x["nome"]))
    return saida


def definir_obras_do_operador(s: Session, usuario_id: int,
                              obras: Optional[Iterable[Any]],
                              responsavel_por: Optional[Iterable[Any]] = None
                              ) -> dict[str, Any]:
    """Regrava as obras de um operador SEM perder a marca de responsável.

    Quando a tela do operador manda só a lista de obras (sem dizer nada sobre
    responsabilidade), as marcas que já existiam são MANTIDAS para as obras que
    continuam na lista. Foi o defeito que este módulo veio evitar: a tela do
    operador apagava tudo e recriava, e a marca feita na tela da obra sumia sem
    ninguém perceber.
    """
    quero = set(_numeros(obras))
    responde = set(_numeros(responsavel_por)) if responsavel_por is not None else None

    atuais = {v.obra_id: v for v in s.scalars(select(UsuarioObra).where(
        UsuarioObra.usuario_id == usuario_id)).all()}

    for obra_id, vinculo in atuais.items():
        if obra_id not in quero:
            s.delete(vinculo)
    for obra_id in quero:
        vinculo = atuais.get(obra_id)
        if vinculo is None:
            vinculo = UsuarioObra(usuario_id=usuario_id, obra_id=obra_id,
                                  responsavel=False)
            s.add(vinculo)
        if responde is not None:
            vinculo.responsavel = obra_id in responde
    # responder por obra que não está na lista não existe: a marca some junto
    if responde is not None and (responde - quero):
        logger.info("ERP/vínculos: operador %s marcado como responsável por "
                    "obra(s) que não estão na lista dele — ignorado: %s",
                    usuario_id, sorted(responde - quero))
    s.flush()
    return {"obras": sorted(quero),
            "responsavel_por": sorted((responde or set()) & quero)}


def definir_responsaveis_da_obra(s: Session, obra_id: int,
                                 usuarios: Optional[Iterable[Any]]
                                 ) -> dict[str, Any]:
    """Marca quem responde por esta obra, pela tela da OBRA.

    Aceita mais de um — decisão do dono. Quem for marcado e ainda não estiver
    ligado à obra passa a estar: **responder exige enxergar**, e seria pior
    marcar alguém que abre a cobrança e não consegue abrir a tela.

    Desmarcar NÃO remove o vínculo: a pessoa continua enxergando a obra, só
    deixa de responder por ela. Remover visão é outra decisão, e não pode
    acontecer de raspão.
    """
    quero = set(_numeros(usuarios))
    atuais = {v.usuario_id: v for v in s.scalars(select(UsuarioObra).where(
        UsuarioObra.obra_id == obra_id)).all()}

    ligados_agora = []
    for usuario_id, vinculo in atuais.items():
        vinculo.responsavel = usuario_id in quero
    for usuario_id in quero - set(atuais):
        s.add(UsuarioObra(usuario_id=usuario_id, obra_id=obra_id,
                          responsavel=True))
        ligados_agora.append(usuario_id)
    s.flush()
    if ligados_agora:
        logger.info("ERP/vínculos: obra %s — %d pessoa(s) passaram a enxergar "
                    "a obra por terem sido marcadas como responsáveis",
                    obra_id, len(ligados_agora))
    return {"responsaveis": sorted(quero), "passaram_a_enxergar": ligados_agora}
