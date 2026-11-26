"""O DIÁRIO DO AGENTE DE DP (26/11/2026) — tudo o que ele fez sozinho, para
uma pessoa revisar.

O dono: *"eu quero um agente que trabalhe como um humano (…) as ações que o
agente fez de forma autônoma ficasse todo o histórico (…) ele vai fazendo, aí
eu analiso (…) Aí chegar outra situação (…) opa, essa aqui deu errado. Aí eu
vou entrar aqui no chat e dizer, ó, essa tratativa aqui o agente fez errado."*
E sobre o destaque: *"tudo que for feito pelo agente (…) na própria listagem
tem alguma tag que indique isso, porque serve de uma espécie de alerta para a
gente abrir e verificar."*

O agente NÃO aprende sozinho com a revisão, de propósito: um "errado" vira
conversa no chat e ajuste de regra — agente que muda a própria regra sem
ninguém ver é o risco que se quer evitar. Tabela: `dp_agente_acoes` (129).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

ROTULOS = {
    "ENVIOU": "Enviou o e-mail à contabilidade",
    "CONFERIU": "Conferiu o documento que voltou",
    "CONCLUIU": "Concluiu o pedido",
    "DIVERGIU": "Achou diferença no documento — deixou para o DP",
    "NAO_ENVIOU": "Não enviou sozinho — deixou para o DP",
    "AVISOU": "Avisou quem pediu",
}

_TEM: dict[str, bool] = {}


def tem_tabela(s: Session) -> bool:
    if "ok" not in _TEM:
        _TEM["ok"] = s.execute(text("SELECT to_regclass('public.dp_agente_acoes')")).scalar() is not None
    return _TEM["ok"]


def registrar(s: Session, acao: str, texto_: str, *, solicitacao_id: Optional[int] = None,
              pedido_id: Optional[int] = None, colaborador_id: Optional[int] = None,
              detalhe: Optional[dict[str, Any]] = None) -> Optional[int]:
    """Anota uma ação do agente. Sem a migração 129, não anota (e não quebra)."""
    if not tem_tabela(s):
        return None
    if pedido_id and not solicitacao_id:
        solicitacao_id = s.execute(text("SELECT solicitacao_id FROM dp_pedidos WHERE id = :p"),
                                   {"p": pedido_id}).scalar()
    if pedido_id and not colaborador_id:
        colaborador_id = s.execute(text("SELECT colaborador_id FROM dp_pedidos WHERE id = :p"),
                                   {"p": pedido_id}).scalar()
    return s.execute(text("""
        INSERT INTO dp_agente_acoes (acao, texto, solicitacao_id, pedido_id, colaborador_id, detalhe)
        VALUES (:a, :t, :s, :p, :c, CAST(:d AS jsonb)) RETURNING id"""),
        {"a": acao, "t": texto_[:2000], "s": solicitacao_id, "p": pedido_id,
         "c": colaborador_id, "d": json.dumps(detalhe or {}, ensure_ascii=False, default=str)}).scalar()


def listar(s: Session, *, so_pendentes: bool = False, solicitacao_id: Optional[int] = None,
           limite: int = 200) -> list[dict[str, Any]]:
    if not tem_tabela(s):
        return []
    onde = ["TRUE"]
    params: dict[str, Any] = {"l": limite}
    if so_pendentes:
        onde.append("a.revisao IS NULL")
    if solicitacao_id:
        onde.append("a.solicitacao_id = :s")
        params["s"] = solicitacao_id
    linhas = s.execute(text(f"""
        SELECT a.*, sol.numero AS solicitacao_numero, c.nome AS pessoa,
               u.nome AS revisado_por_nome
          FROM dp_agente_acoes a
          LEFT JOIN dp_solicitacoes sol ON sol.id = a.solicitacao_id
          LEFT JOIN colaboradores c ON c.id = a.colaborador_id
          LEFT JOIN usuarios u ON u.id = a.revisado_por
         WHERE {" AND ".join(onde)}
         ORDER BY a.id DESC LIMIT :l"""), params).mappings().all()
    saida = []
    for l in linhas:
        d = dict(l)
        for k in ("criado_em", "revisado_em"):
            d[k] = d[k].isoformat() if d.get(k) else None
        d["acao_rotulo"] = ROTULOS.get(d["acao"], d["acao"])
        saida.append(d)
    return saida


def revisar(s: Session, acao_id: int, revisao: str, comentario: str,
            usuario: Optional[Usuario]) -> dict[str, Any]:
    r = (revisao or "").strip().upper()
    if r not in ("CERTO", "ERRADO"):
        raise ErroValidacao("A revisão é CERTO ou ERRADO.")
    if r == "ERRADO" and len((comentario or "").strip()) < 5:
        raise ErroValidacao("Diga o que estava errado e o que era para ter feito — "
                            "é isso que vira o ajuste do agente.")
    n = s.execute(text("""
        UPDATE dp_agente_acoes SET revisao = :r, comentario = NULLIF(:c, ''),
               revisado_por = :u, revisado_em = now() WHERE id = :i"""),
        {"r": r, "c": (comentario or "").strip()[:2000],
         "u": usuario.id if usuario else None, "i": acao_id}).rowcount
    if not n:
        raise ErroNaoEncontrado("Ação não encontrada.")
    return {"id": acao_id, "revisao": r}


def nao_revisadas_por_solicitacao(s: Session, ids: list[int]) -> dict[int, int]:
    """{solicitacao_id: quantas ações do agente ainda sem revisão} — a marca
    "🤖 agente" na lista de solicitações."""
    if not ids or not tem_tabela(s):
        return {}
    return {r[0]: r[1] for r in s.execute(text("""
        SELECT solicitacao_id, count(*) FROM dp_agente_acoes
         WHERE revisao IS NULL AND solicitacao_id = ANY(:i) GROUP BY solicitacao_id"""),
        {"i": list(ids)}).all()}
