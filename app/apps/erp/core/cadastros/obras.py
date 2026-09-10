# ============================================================================
# BWS ERP — core/cadastros/obras.py
# Service simples de obras (centros de custo).
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Obra, Usuario


def listar(s: Session, *, apenas_ativas: bool = True, busca: str = "") -> list[Obra]:
    stmt = select(Obra).order_by(Obra.codigo)
    if apenas_ativas:
        stmt = stmt.where(Obra.status == "ATIVA")
    busca = (busca or "").strip()
    if busca:
        stmt = stmt.where(Obra.nome.ilike(f"%{busca}%") | Obra.codigo.ilike(f"%{busca}%"))
    return list(s.scalars(stmt).all())


def criar(s: Session, dados: dict[str, Any], usuario: Optional[Usuario]) -> Obra:
    codigo = (dados.get("codigo") or "").strip().upper()
    nome = (dados.get("nome") or "").strip()
    if not codigo or not nome:
        raise ErroValidacao("Código e nome da obra são obrigatórios.")
    if s.scalars(select(Obra).where(Obra.codigo == codigo)).first():
        raise ErroValidacao(f"Já existe obra com o código {codigo}.")
    obra = Obra(codigo=codigo, nome=nome,
                cno=(dados.get("cno") or "").strip() or None,
                municipio=(dados.get("municipio") or "").strip() or None,
                uf=(dados.get("uf") or "").strip().upper() or None,
                endereco=(dados.get("endereco") or "").strip() or None,
                codigo_omie_depto=(str(dados.get("codigo_omie_depto") or "").strip() or None),
                ref_sheets=(dados.get("ref_sheets") or "").strip() or None,
                objeto=(dados.get("objeto") or "").strip() or None,
                cliente=(dados.get("cliente") or "").strip() or None,
                cnpj_cliente=(dados.get("cnpj_cliente") or "").strip() or None,
                contrato=(dados.get("contrato") or "").strip() or None,
                valor_contrato=_num(dados.get("valor_contrato")),
                aliquota_iss=_iss(dados),
                aliquota_iss_pct=_iss(dados),
                tributacao=(dados.get("tributacao") or "").strip() or None,
                data_inicio=_dt(dados.get("data_inicio")),
                data_termino=_dt(dados.get("data_termino")),
                orgao_resumido=(dados.get("orgao_resumido") or "").strip() or None,
                ref_pipefy=(str(dados.get("ref_pipefy") or "").strip() or None))
    s.add(obra)
    s.flush()
    registrar_evento(s, "obra", obra.id, "CRIADA",
                     {"codigo": codigo, "nome": nome,
                      "origem": dados.get("origem", "SISTEMA")},
                     usuario.id if usuario else None)
    return obra


def encerrar(s: Session, obra_id: int, usuario: Usuario) -> Obra:
    obra = s.get(Obra, obra_id)
    if obra is None:
        raise ErroValidacao("Obra inexistente.")
    obra.status = "ENCERRADA"
    registrar_evento(s, "obra", obra.id, "ENCERRADA", {}, usuario.id)
    return obra


def _iss(dados: dict[str, Any]):
    """A alíquota de ISS, gravada nas DUAS colunas de propósito.

    A tabela tem `aliquota_iss` (antiga) e `aliquota_iss_pct` (a que a
    tributação, a tela e o cálculo da medição leem). Enquanto as duas
    existirem, obra nascida aqui tem de nascer com as duas iguais — obra com
    uma preenchida e outra vazia é a origem de "está no cadastro mas a nota
    diz que falta".
    """
    return _num(dados.get("aliquota_iss_pct") or dados.get("aliquota_iss"))


def _num(v):
    from decimal import Decimal, InvalidOperation
    v = str(v or "").strip().replace("R$", "").replace(".", "").replace(",", ".")
    if not v:
        return None
    try:
        return Decimal(v).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def _dt(v):
    from datetime import datetime
    v = str(v or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    return None
