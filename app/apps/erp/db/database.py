# ============================================================================
# BWS ERP — db/database.py
# Conexão com o PostgreSQL (Render). Camada única de acesso.
# Inicialização PREGUIÇOSA + carregamento automático de .env (dev local).
# ============================================================================
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


def _carregar_dotenv() -> None:
    """Carrega variáveis de um arquivo .env na raiz do projeto (dev local).
    Sem dependência externa; variáveis já definidas no ambiente têm prioridade
    (comportamento do Render preservado)."""
    # raiz do monorepo (…/aplicacoes) — no Render as variáveis vêm do ambiente
    raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    caminho = os.path.join(raiz, ".env")
    if not os.path.isfile(caminho):
        return
    try:
        with open(caminho, "r", encoding="utf-8-sig") as f:
            for linha in f:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, _, valor = linha.partition("=")
                chave = chave.strip()
                valor = valor.strip().strip('"').strip("'")
                if chave and chave not in os.environ:
                    os.environ[chave] = valor
    except OSError:
        pass


def _montar_url() -> str:
    _carregar_dotenv()
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL não definida. Configure a variável de ambiente com a "
            "connection string do PostgreSQL (painel do Render > Database > Connect)."
        )
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def obter_engine() -> Engine:
    global _engine, _SessionLocal
    if _engine is None:
        _engine = create_engine(
            _montar_url(), pool_size=5, max_overflow=5,
            pool_pre_ping=True, pool_recycle=300, future=True,
        )
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False,
                                     expire_on_commit=False, future=True)
    return _engine


def _fabrica() -> sessionmaker:
    obter_engine()
    assert _SessionLocal is not None
    return _SessionLocal


class Base(DeclarativeBase):
    """Base declarativa única para todos os models do sistema."""


@contextmanager
def get_session() -> Iterator[Session]:
    session = _fabrica()()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Iterator[Session]:
    """Dependência para o FastAPI (Depends(get_db))."""
    session = _fabrica()()
    try:
        yield session
    finally:
        session.close()
