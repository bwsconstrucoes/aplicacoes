# ============================================================================
# BWS ERP — core/cadastros/categorias.py
# Service simples do plano financeiro (categorias).
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Categoria, TipoTitulo, Usuario


def listar(s: Session, *, apenas_ativas: bool = True, busca: str = "") -> list[Categoria]:
    stmt = select(Categoria).order_by(Categoria.codigo)
    if apenas_ativas:
        stmt = stmt.where(Categoria.ativo.is_(True))
    busca = (busca or "").strip()
    if busca:
        stmt = stmt.where(Categoria.descricao.ilike(f"%{busca}%") | Categoria.codigo.ilike(f"%{busca}%"))
    return list(s.scalars(stmt).all())


def criar(s: Session, dados: dict[str, Any], usuario: Optional[Usuario]) -> Categoria:
    codigo = (dados.get("codigo") or "").strip()
    descricao = (dados.get("descricao") or "").strip()
    if not codigo or not descricao:
        raise ErroValidacao("Código e descrição da categoria são obrigatórios.")
    if s.scalars(select(Categoria).where(Categoria.codigo == codigo)).first():
        raise ErroValidacao(f"Já existe categoria com o código {codigo}.")
    tipos = []
    for t in (dados.get("tipos_permitidos") or []):
        try:
            tipos.append(TipoTitulo(t if isinstance(t, str) else t.value))
        except ValueError:
            raise ErroValidacao(f"Tipo de título inválido em tipos_permitidos: {t!r}")
    natureza = (dados.get("natureza") or "RESULTADO").strip().upper()
    if natureza not in ("RESULTADO", "FLUXO"):
        raise ErroValidacao("Natureza da categoria deve ser RESULTADO ou FLUXO.")
    cat = Categoria(codigo=codigo, descricao=descricao, natureza=natureza,
                    codigo_omie=(str(dados.get("codigo_omie") or "").strip() or None),
                    tipos_permitidos=tipos,
                    dedutivel_padrao=bool(dados.get("dedutivel_padrao", True)),
                    credito_pis_cofins=bool(dados.get("credito_pis_cofins", False)),
                    conta_contabil=(dados.get("conta_contabil") or "").strip() or None)
    s.add(cat)
    s.flush()
    registrar_evento(s, "categoria", cat.id, "CRIADA",
                     {"codigo": codigo, "descricao": descricao,
                      "origem": dados.get("origem", "SISTEMA")},
                     usuario.id if usuario else None)
    return cat
