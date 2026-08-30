# ============================================================================
# ERP — core/titulos/aval.py
# Dupla confirmação antes de ir a pagamento.
#
# Regra: nenhum título chega ao caixa com a assinatura de uma pessoa só.
#   Quem lançou é a primeira. A segunda é o AVAL, e depende de quem lançou:
#     ADMINISTRATIVO_OBRA  → supervisor da obra do título, gestor de obras,
#                            diretor financeiro ou admin
#     FINANCEIRO (escritório) → diretor financeiro ou admin
#     SUPERVISOR / GESTOR  → diretor financeiro ou admin (quem responde acima)
#     DIRETOR / ADMIN      → não exige aval (já é a instância final)
#
# Ninguém avaliza o próprio lançamento — nem o diretor. Segregação vale para
# todos, senão a dupla confirmação vira formalidade.
#
# A confirmação é registrada como ASSINATURA: quem, quando, de onde (IP), com
# que dispositivo, e um HASH do resumo do título no momento do aval. Se algo
# for alterado depois, dá para provar o que foi efetivamente assinado.
# ============================================================================
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario, UsuarioObra
from app.apps.erp.db.models.financeiro import (
    Rateio, StatusTitulo, Titulo, TituloAval,
)

logger = logging.getLogger(__name__)
P = PerfilUsuario

# perfis cujo lançamento precisa de aval, e quem pode dá-lo
EXIGE_AVAL: dict[PerfilUsuario, set[PerfilUsuario]] = {
    P.ADMINISTRATIVO_OBRA: {P.SUPERVISOR_OBRA, P.GESTOR_OBRA, P.DIRETOR_FINANCEIRO, P.ADMIN},
    P.LANCADOR:            {P.SUPERVISOR_OBRA, P.GESTOR_OBRA, P.DIRETOR_FINANCEIRO, P.ADMIN},
    P.FINANCEIRO:          {P.DIRETOR_FINANCEIRO, P.ADMIN},
    P.SUPERVISOR_OBRA:     {P.GESTOR_OBRA, P.DIRETOR_FINANCEIRO, P.ADMIN},
    P.GESTOR_OBRA:         {P.DIRETOR_FINANCEIRO, P.ADMIN},
}

# quem não pode ver dados de pagamento (conta do credor, boleto, Pix)
SEM_DADOS_PAGAMENTO = {P.ADMINISTRATIVO_OBRA, P.LANCADOR, P.CONSULTA}


def exige_aval(solicitante: Usuario) -> bool:
    return solicitante.perfil in EXIGE_AVAL


def pode_ver_dados_pagamento(usuario: Usuario) -> bool:
    return usuario.perfil not in SEM_DADOS_PAGAMENTO


def _obras_do_titulo(s: Session, titulo_id: int) -> set[int]:
    return {r.obra_id for r in s.scalars(
        select(Rateio).where(Rateio.titulo_id == titulo_id)).all()}


def pode_avalizar(s: Session, titulo: Titulo, usuario: Usuario) -> tuple[bool, str]:
    """Diz se este usuário pode assinar este título, e por quê não."""
    if titulo.status != StatusTitulo.AGUARDANDO_AVAL:
        return False, f"O título está {titulo.status.value}, não aguardando aval."
    if usuario.id == titulo.solicitante_id:
        return False, "Quem lança não avaliza o próprio título."

    solicitante = s.get(Usuario, titulo.solicitante_id)
    autorizados = EXIGE_AVAL.get(solicitante.perfil, {P.DIRETOR_FINANCEIRO, P.ADMIN})
    if usuario.perfil not in autorizados:
        nomes = ", ".join(sorted(p.value.replace("_", " ").lower() for p in autorizados))
        return False, f"Este título precisa do aval de: {nomes}."

    # supervisor só avaliza as obras dele
    if usuario.perfil == P.SUPERVISOR_OBRA:
        minhas = {o.obra_id for o in s.scalars(
            select(UsuarioObra).where(UsuarioObra.usuario_id == usuario.id)).all()}
        if not (minhas & _obras_do_titulo(s, titulo.id)):
            return False, "Este título não é de uma obra sob sua supervisão."
    return True, ""


def _resumo(s: Session, t: Titulo) -> dict[str, Any]:
    """O que está sendo assinado — congelado no momento do aval."""
    obras = sorted({r.obra.codigo for r in s.scalars(
        select(Rateio).where(Rateio.titulo_id == t.id)
        .options(selectinload(Rateio.obra))).all() if r.obra})
    return {
        "numero_sp": t.numero_sp, "credor": t.fornecedor.razao_social,
        "documento_credor": t.fornecedor.cnpj_cpf, "descricao": t.descricao,
        "valor_bruto": str(t.valor_bruto), "valor_liquido": str(t.valor_liquido),
        "competencia": t.competencia.isoformat(), "obras": obras,
        "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
        "parcelas": [{"n": p.numero, "venc": p.vencimento.isoformat(),
                      "valor": str(p.valor)} for p in t.parcelas],
        "solicitante_id": t.solicitante_id,
    }


def _assinar(resumo: dict[str, Any], usuario: Usuario, quando: datetime) -> str:
    base = json.dumps(resumo, sort_keys=True, ensure_ascii=False)
    base += f"|{usuario.id}|{usuario.email}|{quando.isoformat()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def marcar_para_aval(s: Session, titulo: Titulo, solicitante: Usuario) -> bool:
    """Chamado logo após a análise: decide se o título trava aguardando aval."""
    if not exige_aval(solicitante):
        return False
    titulo.exige_aval = True
    if titulo.status in (StatusTitulo.AGUARDANDO_APROVACAO, StatusTitulo.EM_ANALISE):
        titulo.status = StatusTitulo.AGUARDANDO_AVAL
    # título bloqueado pela análise continua bloqueado: o aval vem depois da
    # revisão do financeiro, não por cima dela
    return True


def registrar(s: Session, titulo_id: int, usuario: Usuario, *, decisao: str = "CONFIRMADO",
              motivo: str = "", ip: str = "", dispositivo: str = "") -> dict[str, Any]:
    """Assina (ou recusa) o título. Confirmado, ele segue para liberação de
    pagamento; recusado, volta ao solicitante com o motivo."""
    t = s.get(Titulo, titulo_id, options=[
        selectinload(Titulo.parcelas), selectinload(Titulo.fornecedor),
        selectinload(Titulo.categoria)])
    if t is None:
        raise ErroValidacao("Título não encontrado.")
    decisao = (decisao or "").upper()
    if decisao not in ("CONFIRMADO", "RECUSADO"):
        raise ErroValidacao("Decisão inválida.")

    permitido, porque = pode_avalizar(s, t, usuario)
    if not permitido:
        raise ErroPermissao(porque)
    if decisao == "RECUSADO" and len((motivo or "").strip()) < 10:
        raise ErroValidacao("Recusar exige motivo (mínimo 10 caracteres).")

    quando = datetime.now(timezone.utc)
    resumo = _resumo(s, t)
    assinatura = _assinar(resumo, usuario, quando)

    s.add(TituloAval(
        titulo_id=t.id, usuario_id=usuario.id, papel=usuario.perfil.value,
        decisao=decisao, motivo=(motivo or "").strip() or None,
        assinatura=assinatura, resumo_assinado=resumo,
        ip=(ip or "")[:60], dispositivo=(dispositivo or "")[:200]))

    if decisao == "CONFIRMADO":
        t.avalizado_em = quando
        t.avalizado_por = usuario.id
        t.status = StatusTitulo.AGUARDANDO_APROVACAO
        acao, mensagem = "AVAL_CONFIRMADO", "segue para liberação de pagamento"
    else:
        t.status = StatusTitulo.DEVOLVIDO
        acao, mensagem = "AVAL_RECUSADO", "devolvido ao solicitante"

    s.flush()
    registrar_evento(s, "titulo", t.id, acao, {
        "numero_sp": t.numero_sp, "por": usuario.nome, "papel": usuario.perfil.value,
        "decisao": decisao, "motivo": motivo, "assinatura": assinatura[:16],
        "ip": ip, "valor_assinado": resumo["valor_liquido"]}, usuario.id)
    logger.info("ERP/aval: %s %s por %s (%s)", t.numero_sp, decisao, usuario.email,
                usuario.perfil.value)
    return {"numero_sp": t.numero_sp, "decisao": decisao, "status": t.status.value,
            "assinatura": assinatura[:16], "mensagem": f"{t.numero_sp} {mensagem}."}


def pendentes(s: Session, usuario: Usuario, limite: int = 200) -> list[dict[str, Any]]:
    """Títulos esperando a assinatura DESTE usuário."""
    stmt = (select(Titulo).where(Titulo.status == StatusTitulo.AGUARDANDO_AVAL)
            .options(selectinload(Titulo.parcelas), selectinload(Titulo.fornecedor),
                     selectinload(Titulo.categoria),
                     selectinload(Titulo.rateios).selectinload(Rateio.obra))
            .order_by(Titulo.id.desc()).limit(limite))
    saida = []
    for t in s.scalars(stmt).all():
        permitido, porque = pode_avalizar(s, t, usuario)
        if not permitido:
            continue
        solicitante = s.get(Usuario, t.solicitante_id)
        venc = min((p.vencimento for p in t.parcelas), default=None)
        saida.append({
            "id": t.id, "numero_sp": t.numero_sp,
            "credor": t.fornecedor.razao_social,
            "documento_credor": t.fornecedor.cnpj_cpf,
            "descricao": t.descricao,
            "categoria": f"{t.categoria.codigo} · {t.categoria.descricao}",
            "obra": " + ".join(sorted({r.obra.codigo for r in t.rateios if r.obra})),
            "valor": float(t.valor_liquido), "valor_bruto": float(t.valor_bruto),
            "competencia": t.competencia.strftime("%m/%Y"),
            "vencimento": venc.isoformat() if venc else None,
            "parcelas": len(t.parcelas),
            "risco": t.score_risco or 0,
            "solicitante": solicitante.nome if solicitante else "—",
            "solicitante_perfil": solicitante.perfil.value if solicitante else "",
            "lancado_em": t.criado_em.strftime("%d/%m/%Y %H:%M"),
        })
    return saida


def historico_avais(s: Session, titulo_id: int) -> list[dict[str, Any]]:
    linhas = s.scalars(select(TituloAval).where(TituloAval.titulo_id == titulo_id)
                       .order_by(TituloAval.criado_em)).all()
    saida = []
    for a in linhas:
        u = s.get(Usuario, a.usuario_id)
        saida.append({
            "quem": u.nome if u else "—", "papel": a.papel, "decisao": a.decisao,
            "motivo": a.motivo, "quando": a.criado_em.strftime("%d/%m/%Y %H:%M:%S"),
            "assinatura": a.assinatura[:16], "ip": a.ip,
            "valor_assinado": a.resumo_assinado.get("valor_liquido"),
        })
    return saida
