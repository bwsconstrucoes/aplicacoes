# -*- coding: utf-8 -*-
"""
A CENTRAL DE ASSINATURAS — o lado do DP e a ficha da pessoa (23/11/2026).

O pedido do dono: *"a gente ter essa gestão dessas pendências (…) o DP poder gerir, também dentro do ERP, para saber
o que é que tá, o que é que precisa ser feito, o que é que não precisa, o que é que já foi. (…) dentro do cadastro de
cada colaborador (…) visualizar esse histórico de assinaturas, de documentos, ter acesso a esses documentos que ele
tem assinado."*

Três fontes, cada uma com a sua regra no seu lugar — aqui só se junta:
- os documentos do DP (`dp_termos`, `app/apps/erp/core/termos.py`): aviso prévio, termo de compensação…;
- a folha de ponto do mês (`core/assinaturas.py`);
- o termo do ponto (`core/termo.py`).

O escopo de quem vê é o do ponto: o perfil preso a obras vê só as obras dele.

CUSTO: a folha de ponto é contada sem remontar o espelho de ninguém (quem bateu no mês × quem tem assinatura
valendo) — a conferência do lacre fica para quando a folha é aberta. Por isso, "assinada" aqui pode incluir uma folha
que mudou depois (o mês reaberto); o aplicativo de quem conduz é que refaz essa conta, pessoa por pessoa.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import competencias
from .ocorrencias import Quem


def _mes_da_folha(conn: Connection) -> dt.date:
    """O último mês fechado entre os dois anteriores (o que se está assinando agora); sem nenhum, o anterior."""
    hoje = competencias.primeiro_dia(horario.hoje())
    anterior = competencias.primeiro_dia(hoje - dt.timedelta(days=1))
    for comp in (anterior, competencias.primeiro_dia(anterior - dt.timedelta(days=1))):
        if competencias.esta_fechada(conn, comp):
            return comp
    return anterior


def folhas(conn: Connection, quem: Quem, competencia=None) -> dict:
    from . import assinaturas
    comp = competencias.primeiro_dia(competencia) if competencia else _mes_da_folha(conn)
    if not assinaturas.disponivel(conn):
        return {"competencia": comp.strftime("%Y-%m"), "disponivel": False, "faltam": [], "assinadas": 0,
                "recusadas": 0, "fechado": False}
    linhas = db.todos(conn, """
        SELECT DISTINCT ON (m.colaborador_id) m.colaborador_id, c.nome, o.id AS obra_id, o.codigo AS obra
          FROM ponto.marcacoes m
          JOIN public.colaboradores c ON c.id = m.colaborador_id
          JOIN public.obras o ON o.id = m.obra_id
         WHERE m.data_referencia BETWEEN :i AND :f AND m.status <> 'REJEITADA'
         ORDER BY m.colaborador_id, m.timestamp_servidor DESC""", i=comp, f=competencias.ultimo_dia(comp))
    gente = [l for l in linhas if quem.alcanca_obra(l["obra_id"])]
    valendo = assinaturas._valendo(conn, [int(l["colaborador_id"]) for l in gente], comp)
    faltam = sorted(({"id": int(l["colaborador_id"]), "nome": l["nome"], "obra": l["obra"]}
                     for l in gente if int(l["colaborador_id"]) not in valendo),
                    key=lambda x: (x["obra"] or "", x["nome"] or ""))
    return {"competencia": comp.strftime("%Y-%m"), "disponivel": True, "fechado": competencias.esta_fechada(conn, comp),
            "prazo": assinaturas.prazo(comp).isoformat(), "pessoas": len(gente), "faltam": faltam,
            "assinadas": sum(1 for v in valendo.values() if v["situacao"] == "ASSINADA"),
            "recusadas": sum(1 for v in valendo.values() if v["situacao"] == "RECUSADA")}


def termo_do_ponto(conn: Connection, quem: Quem) -> list[dict]:
    from . import cadastros, termo
    obras = {int(o["id"]): o["codigo"] for o in db.todos(conn, "SELECT id, codigo FROM public.obras")}
    return sorted(({"id": p["id"], "nome": p["nome"], "obra": obras.get(int(p["obra_id"])) if p.get("obra_id") else ""}
                   for p in termo.pendentes(conn) if quem.alcanca_obra(p.get("obra_id"))),
                  key=lambda x: (x["obra"] or "", x["nome"] or ""))


def documentos(quem: Quem) -> dict:
    """Os do DP (o ERP): abertos, esperando validação, feitos nos últimos 60 dias — no escopo de quem vê."""
    from app.apps.erp.core import termos
    from app.apps.erp.db.database import get_session
    with get_session() as s:
        r = termos.painel_dp(s)
        obra_de = {}
        if r.get("disponivel"):
            ids = [x["id"] for k in ("abertos", "validar", "feitos") for x in r[k]]
            if ids:
                from sqlalchemy import text
                obra_de = {int(l["id"]): l["obra_id"] for l in s.execute(text(
                    "SELECT id, obra_id FROM dp_termos WHERE id = ANY(:i)"), {"i": ids}).mappings()}
                if termos.tem_origem(s):
                    origens = {int(l["id"]): (l["origem"], list(l["origem_ids"] or [])) for l in s.execute(text(
                        "SELECT id, origem, origem_ids FROM dp_termos WHERE id = ANY(:i)"), {"i": ids}).mappings()}
                    for k in ("abertos", "validar", "feitos"):
                        for x in r[k]:
                            x["origem"], x["origem_ids"] = origens.get(int(x["id"]), (None, []))
    if quem.obras is not None:
        for k in ("abertos", "validar", "feitos"):
            r[k] = [x for x in r.get(k, []) if quem.alcanca_obra(obra_de.get(int(x["id"])))]
    return r


def painel(conn: Connection, quem: Quem, competencia=None) -> dict:
    return {"documentos": documentos(quem), "folhas": folhas(conn, quem, competencia),
            "termo_do_ponto": termo_do_ponto(conn, quem)}


def da_pessoa(conn: Connection, colaborador_id: int) -> dict:
    """A ficha (23/11/2026): o que a pessoa assinou e o que falta — com o documento de cada um."""
    from app.apps.erp.core import termos
    from app.apps.erp.db.database import get_session
    from . import assinaturas, termo
    folhas_ = []
    if assinaturas.disponivel(conn):
        folhas_ = [{"competencia": l["competencia"].strftime("%Y-%m"), "situacao": l["situacao"],
                    "em": horario.texto(l["criado_em"]), "por": l["conduzida_por_nome"] or "",
                    "documento_id": l["documento_id"], "motivo": l["motivo_recusa"] or ""}
                   for l in db.todos(conn, """
            SELECT competencia, situacao, criado_em, conduzida_por_nome, documento_id, motivo_recusa
              FROM ponto.espelho_assinaturas
             WHERE colaborador_id = :c AND situacao <> 'SUBSTITUIDA' ORDER BY competencia DESC""", c=colaborador_id)]
    termo_ = None
    if termo.disponivel(conn):
        l = db.um(conn, """SELECT situacao, criado_em, documento_id, versao FROM ponto.termo_aceites
                            WHERE colaborador_id = :c AND situacao <> 'SUBSTITUIDO' ORDER BY id DESC LIMIT 1""",
                  c=colaborador_id)
        termo_ = ({"situacao": l["situacao"], "em": horario.texto(l["criado_em"]), "documento_id": l["documento_id"]}
                  if l else {"situacao": "FALTA"})
    with get_session() as s:
        docs = termos.da_pessoa(s, colaborador_id)
    return {"documentos": docs, "folhas": folhas_, "termo_do_ponto": termo_}


from contextlib import contextmanager


@contextmanager
def _erros_do_erp():
    """O erro do ERP vira o do ponto, com a mesma frase (senão a tela do ponto responde "erro interno")."""
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
    from ..erros import ErroDeValidacao, NaoEncontrado
    try:
        yield
    except ErroValidacao as e:
        raise ErroDeValidacao(str(e)) from None
    except ErroNaoEncontrado as e:
        raise NaoEncontrado(str(e) or "documento não encontrado") from None


def validar(quem: Quem, termo_id: int, *, aceitar: bool, motivo: str = "") -> dict:
    """O DP olha o papel (ou a recusa) e aceita — ou devolve para a obra, dizendo o porquê."""
    from app.apps.erp.core import termos
    from app.apps.erp.db.database import get_session
    with _erros_do_erp(), get_session() as s:
        _no_escopo(s, quem, termo_id)
        from app.apps.erp.db.models.cadastros import Usuario
        usuario = s.get(Usuario, quem.usuario_id) if quem.usuario_id else None
        r = termos.validar(s, termo_id, usuario, aceitar=aceitar, motivo=motivo)
        if aceitar:
            from app.apps.erp.core import pessoal_solicitacoes
            try:
                pessoal_solicitacoes.termo_voltou(s, termo_id, usuario)
            except Exception:  # noqa: BLE001 — o documento já vale; o atendimento anda pelo DP
                pass
        s.commit()
    return r


def cancelar(quem: Quem, termo_id: int, motivo: str) -> None:
    from app.apps.erp.core import termos
    from app.apps.erp.db.database import get_session
    if len((motivo or "").strip()) < 5:
        from ..erros import ErroDeValidacao
        raise ErroDeValidacao("diga por que o documento foi cancelado", campo="motivo")
    with _erros_do_erp(), get_session() as s:
        _no_escopo(s, quem, termo_id)
        termos.cancelar(s, termo_id, motivo.strip(), None)
        s.commit()


def _no_escopo(s, quem: Quem, termo_id: int) -> dict:
    from app.apps.erp.core import termos
    from ..erros import NaoEncontrado
    t = termos.ler(s, termo_id)
    if not quem.alcanca_obra(t.get("obra_id")):
        raise NaoEncontrado("documento não encontrado")
    return t
