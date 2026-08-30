# ============================================================================
# BWS ERP — core/comum/auditoria.py
# Registro na trilha append-only (tabela eventos). Uso por todos os services.
# ============================================================================
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


def registrar_evento(s: Session, entidade_tipo: str, entidade_id: int,
                     acao: str, detalhe: dict[str, Any],
                     usuario_id: Optional[int]) -> None:
    s.execute(
        text("INSERT INTO eventos (entidade_tipo, entidade_id, usuario_id, acao, detalhe) "
             "VALUES (:et, :ei, :ui, :ac, CAST(:dt AS jsonb))"),
        {"et": entidade_tipo, "ei": entidade_id, "ui": usuario_id,
         "ac": acao, "dt": json.dumps(detalhe, ensure_ascii=False, default=str)},
    )


class ErroValidacao(Exception):
    """Erro de regra de negócio — mensagem segura para exibir ao usuário."""


class ErroPermissao(Exception):
    """Usuário sem perfil para a operação."""
