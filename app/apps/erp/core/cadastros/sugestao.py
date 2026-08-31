# ============================================================================
# ERP — core/cadastros/sugestao.py
# Sugere a CONTA do plano e a OBRA a partir do documento lido.
#
# A sugestão nunca decide: vem marcada como sugestão, com o motivo, para a
# pessoa validar. Duas fontes, nesta ordem:
#   1. HISTÓRICO — o que este credor costuma receber. É o sinal mais forte e
#      não custa nada: se as últimas cinco notas da Gerdau foram para "aço e
#      armadura", a sexta provavelmente é também.
#   2. IA — lê descrição, itens e natureza da operação e escolhe entre as
#      contas que o usuário pode usar. Só entra quando o histórico não resolve.
#
# A obra é buscada pelo CNO e pelo endereço que aparecem na nota, cruzando com
# o cadastro — é a informação que já está no documento e ninguém aproveita.
# ============================================================================
from __future__ import annotations

import json
import logging
import re
import unicodedata
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Categoria, Fornecedor, Obra, Usuario
from app.apps.erp.db.models.financeiro import Titulo

logger = logging.getLogger(__name__)


def _norm(t: str) -> str:
    t = unicodedata.normalize("NFKD", (t or "").upper())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", t).strip()


def categorias_do_usuario(s: Session, usuario: Usuario) -> Optional[list[int]]:
    """Contas liberadas. None = todas (sem restrição cadastrada)."""
    from app.apps.erp.db.models.cadastros import UsuarioCategoria
    ids = [x.categoria_id for x in s.scalars(select(UsuarioCategoria).where(
        UsuarioCategoria.usuario_id == usuario.id)).all()]
    return ids or None


def por_historico(s: Session, fornecedor_id: int,
                  permitidas: Optional[list[int]] = None) -> Optional[dict[str, Any]]:
    """A conta que este credor mais recebeu."""
    stmt = (select(Titulo.categoria_id, func.count(Titulo.id))
            .where(Titulo.fornecedor_id == fornecedor_id,
                   Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))
            .group_by(Titulo.categoria_id)
            .order_by(func.count(Titulo.id).desc()).limit(1))
    linha = s.execute(stmt).first()
    if not linha or not linha[0]:
        return None
    if permitidas and linha[0] not in permitidas:
        return None
    cat = s.get(Categoria, linha[0])
    if cat is None or not cat.ativo:
        return None
    return {"categoria_id": cat.id, "codigo": cat.codigo, "descricao": cat.descricao,
            "confianca": "ALTA" if linha[1] >= 3 else "MEDIA",
            "motivo": f"este credor já teve {linha[1]} título(s) nesta conta"}


def por_ia(s: Session, documento: dict[str, Any],
           permitidas: Optional[list[int]] = None) -> Optional[dict[str, Any]]:
    """Escolhe a conta lendo o documento — só entre as contas permitidas."""
    import os
    if not os.getenv("OPENAI_API_KEY", "").strip():
        return None
    stmt = select(Categoria).where(Categoria.ativo.is_(True))
    if permitidas:
        stmt = stmt.where(Categoria.id.in_(permitidas))
    contas = s.scalars(stmt.order_by(Categoria.ordem)).all()
    if not contas:
        return None
    catalogo = "\n".join(
        f"{c.codigo}|{c.descricao}" + (f"|{c.descricao_uso[:90]}" if c.descricao_uso else "")
        for c in contas)
    texto = " · ".join(str(documento.get(k) or "") for k in (
        "descricao", "emitente_nome", "tipo_documento", "municipio_emissao",
        "observacoes")) or ""
    itens = documento.get("itens") or []
    if itens:
        texto += " · itens: " + "; ".join(str(i.get("descricao") or "") for i in itens[:8])
    if len(texto.strip(" ·")) < 8:
        return None
    try:
        from openai import OpenAI
        resp = OpenAI(api_key=os.environ["OPENAI_API_KEY"]).chat.completions.create(
            model=os.getenv("ERP_MODELO_IA", "gpt-4o-mini"), temperature=0, max_tokens=200,
            messages=[
                {"role": "system", "content":
                 "Você classifica despesas de uma construtora no plano de contas dela. "
                 "Responda SÓ com JSON: {\"codigo\":\"3.1.03\",\"confianca\":"
                 "\"ALTA|MEDIA|BAIXA\",\"motivo\":\"uma frase curta\"}. "
                 "Use exclusivamente um código da lista. Se nenhum servir, "
                 "devolva codigo vazio."},
                {"role": "user", "content":
                 f"Contas disponíveis (codigo|descrição|quando usar):\n{catalogo}\n\n"
                 f"Documento: {texto[:1200]}"}])
        bruto = (resp.choices[0].message.content or "").strip()
        bruto = re.sub(r"^```(?:json)?|```$", "", bruto, flags=re.MULTILINE).strip()
        d = json.loads(bruto)
    except Exception as e:
        logger.warning("ERP/sugestão: IA não classificou (%s)", e)
        return None
    codigo = (d.get("codigo") or "").strip()
    if not codigo:
        return None
    cat = next((c for c in contas if c.codigo == codigo), None)
    if cat is None:
        return None
    return {"categoria_id": cat.id, "codigo": cat.codigo, "descricao": cat.descricao,
            "confianca": (d.get("confianca") or "MEDIA").upper(),
            "motivo": (d.get("motivo") or "sugerido pela leitura do documento")[:160]}


def sugerir_categoria(s: Session, documento: dict[str, Any], usuario: Usuario,
                      fornecedor_id: Optional[int] = None) -> Optional[dict[str, Any]]:
    permitidas = categorias_do_usuario(s, usuario)
    if fornecedor_id:
        h = por_historico(s, fornecedor_id, permitidas)
        if h:
            h["origem"] = "HISTORICO"
            return h
    ia = por_ia(s, documento, permitidas)
    if ia:
        ia["origem"] = "IA"
    return ia


def sugerir_obra(s: Session, documento: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Acha a obra pelo CNO ou pelo endereço que aparecem na nota."""
    texto = _norm(" ".join(str(documento.get(k) or "") for k in (
        "descricao", "observacoes", "obra_mencionada", "municipio_emissao")))
    cno_doc = re.sub(r"\D", "", str(documento.get("cno") or ""))
    obras = s.scalars(select(Obra).where(Obra.status == "ATIVA")).all()

    for o in obras:
        if cno_doc and o.cno and re.sub(r"\D", "", o.cno) == cno_doc:
            return {"obra_id": o.id, "codigo": o.codigo, "confianca": "ALTA",
                    "motivo": f"CNO {o.cno} confere com o cadastro da obra"}
    for o in obras:
        if o.codigo and _norm(o.codigo) in texto:
            return {"obra_id": o.id, "codigo": o.codigo, "confianca": "ALTA",
                    "motivo": "código da obra citado no documento"}
    for o in obras:
        if o.endereco and len(o.endereco) > 10 and _norm(o.endereco)[:22] in texto:
            return {"obra_id": o.id, "codigo": o.codigo, "confianca": "MEDIA",
                    "motivo": "endereço de entrega bate com o da obra"}
    candidatas = [o for o in obras if o.municipio and _norm(o.municipio) in texto]
    if len(candidatas) == 1:
        o = candidatas[0]
        return {"obra_id": o.id, "codigo": o.codigo, "confianca": "BAIXA",
                "motivo": f"única obra em {o.municipio}"}
    return None
