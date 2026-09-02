# -*- coding: utf-8 -*-
"""
Camada de banco do Painel OMIE.

Mora no MESMO Postgres do ERP (a variavel DATABASE_URL), mas dentro de um
schema separado chamado `painel`. Isso e proposital: o ERP ja tem tabelas
`titulos`, `rateios` e `categorias`, e o espelho do OMIE tem tabelas com esses
mesmos nomes. Schemas separados fazem as duas conviverem sem renomear nada e
sem nenhum risco de uma escrita do painel encostar em dado do ERP.

A engine e PREGUICOSA, pela mesma razao do ERP (`erp/db/database.py`): nada de
banco acontece no import. Sem DATABASE_URL o painel falha sozinho, na primeira
tela aberta, e os outros blueprints do monorepo sobem normalmente.
"""
from __future__ import annotations

import os
import re
import logging
from contextlib import contextmanager
from typing import Iterator, Optional

logger = logging.getLogger("painel.db")

SCHEMA = "painel"

_engine = None


# --------------------------------------------------------------------------- 
# Conexao
# --------------------------------------------------------------------------- 
def _carregar_dotenv() -> None:
    """Le um .env da raiz do monorepo (so em desenvolvimento local).
    Variavel ja definida no ambiente tem prioridade — no Render nada muda."""
    raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
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
                if chave and chave not in os.environ:
                    os.environ[chave] = valor.strip().strip('"').strip("'")
    except OSError:
        pass


def _recusar_producao_em_teste(url: str) -> None:
    """Trava de seguranca: teste nunca encosta em banco que nao seja de teste.

    O `.env` da raiz tem a DATABASE_URL da PRODUCAO — e correto, e como o
    desenvolvimento local funciona. Mas o `_carregar_dotenv` abaixo le esse
    arquivo, e uma unica funcao de teste que esqueca de dublar a conexao acaba
    falando com o banco da empresa.

    Aconteceu comigo durante a conversao: um teste novo chamou a prestacao de
    contas sem dublar tudo, e o painel tentou abrir o Postgres do Render. So nao
    deu em nada porque o usuario do banco nao tinha permissao de login.

    Entao: com o pytest rodando, so passa URL local com "teste" no nome — a
    mesma regra do `tests/conftest.py` do ERP. Fora do pytest, nada muda."""
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        return
    from urllib.parse import urlparse
    partes = urlparse(url)
    host = (partes.hostname or "").lower()
    nome = partes.path.lstrip("/").lower()
    if host not in {"localhost", "127.0.0.1", "::1"} or "teste" not in nome:
        raise RuntimeError(
            "O painel tentou abrir um banco que NAO e de teste durante a suite "
            f"(host '{host or '?'}', banco '{nome or '?'}'). Ou o teste esqueceu "
            "de dublar a conexao, ou faltou a fixture de banco. A producao nao e "
            "alcancavel a partir do pytest, de proposito.")


def _montar_url() -> str:
    _carregar_dotenv()
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL nao definida. O painel guarda os dados no mesmo Postgres "
            "do ERP (painel do Render > Database > Connect > Internal Database URL)."
        )
    _recusar_producao_em_teste(url)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def obter_engine():
    """Engine SQLAlchemy so pelo pool de conexoes; as consultas sao SQL cru.
    Pool pequeno de proposito: a instancia tem 2 GB e divide com 14 modulos.

    O schema do painel e fixado como PARAMETRO DA CONEXAO (`options`), nao com
    um `SET search_path` depois de conectar. A diferenca importa: o `SET` roda
    dentro de uma transacao, e o pool do SQLAlchemy da rollback ao devolver a
    conexao — o que desfazia o `SET`. Na pratica a primeira tela abria e a
    segunda dizia que a tabela nao existia. Como parametro de conexao, o valor
    vale para a sessao inteira e nenhum rollback o alcanca."""
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(
            _montar_url(), pool_size=2, max_overflow=2,
            pool_pre_ping=True, pool_recycle=300, future=True,
            connect_args={"options": f"-c search_path={SCHEMA},public"},
        )
        logger.info("Painel: engine criada (schema %s).", SCHEMA)
    return _engine


# --------------------------------------------------------------------------- 
# Adaptador: da a uma conexao psycopg2 a mesma cara da conexao do sqlite3
# --------------------------------------------------------------------------- 
# O codigo que baixa o OMIE e monta o fato foi escrito contra o sqlite3, com
# `conn.execute(...)` devolvendo cursor iteravel e placeholder `?`. Reescrever
# 1.600 linhas de SQL a mao para o Postgres seria trocar codigo em producao ha
# meses por codigo novo e nao testado. Este adaptador de ~60 linhas faz a ponte:
# a regra de negocio segue identica, so o banco por baixo muda.

_ASPAS = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def traduzir_placeholders(sql: str) -> str:
    """Troca `?` por `%s` e protege `%` literal, sem tocar no que esta entre aspas."""
    saida, fim = [], 0
    for trecho in _ASPAS.finditer(sql):
        saida.append(sql[fim:trecho.start()].replace("%", "%%").replace("?", "%s"))
        saida.append(trecho.group(0).replace("%", "%%"))
        fim = trecho.end()
    saida.append(sql[fim:].replace("%", "%%").replace("?", "%s"))
    return "".join(saida)


class ConexaoCompat:
    """Conexao Postgres com a interface do sqlite3 usada pelo espelho."""

    def __init__(self, bruta):
        self._bruta = bruta

    def execute(self, sql, params=()):
        cur = self._bruta.cursor()
        cur.execute(traduzir_placeholders(sql), tuple(params))
        return cur

    def executemany(self, sql, seq_params):
        seq = list(seq_params)
        if not seq:
            return None
        cur = self._bruta.cursor()
        # execute_batch agrupa varios INSERT num round-trip so: numa carga de
        # 120 mil titulos a diferenca e de horas para minutos.
        from psycopg2.extras import execute_batch
        execute_batch(cur, traduzir_placeholders(sql), [tuple(p) for p in seq],
                      page_size=500)
        return cur

    def executar_em_stream(self, sql, params=(), por_vez=2000):
        """Cursor do lado do SERVIDOR, para varrer tabela grande sem trazer tudo.

        O cursor comum do psycopg2 carrega o resultado inteiro na memoria assim
        que o execute roda — 120 mil titulos de uma vez, exatamente o que este
        painel precisa evitar. Um cursor nomeado deixa as linhas no Postgres e
        traz de `por_vez` em `por_vez`.

        Enquanto ele estiver aberto, nao dar commit nesta conexao: o commit
        fecha o cursor. Use uma conexao so para ler e outra para gravar."""
        nome = "painel_%d" % (id(self) ^ hash(sql) & 0xFFFFFF)
        cur = self._bruta.cursor(name=nome)
        cur.itersize = por_vez
        cur.execute(traduzir_placeholders(sql), tuple(params))
        return cur

    def executescript(self, sql):
        cur = self._bruta.cursor()
        cur.execute(sql)
        self._bruta.commit()
        return cur

    def commit(self):
        self._bruta.commit()

    def rollback(self):
        self._bruta.rollback()

    def close(self):
        self._bruta.close()


@contextmanager
def conexao() -> Iterator[ConexaoCompat]:
    """Conexao crua com a cara do sqlite3. Faz rollback no erro e fecha sempre."""
    bruta = obter_engine().raw_connection()
    conn = ConexaoCompat(bruta)
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def consultar(sql: str, params=()) -> list[tuple]:
    """Atalho de leitura para as telas. Devolve as linhas ja materializadas."""
    with conexao() as conn:
        cur = conn.execute(sql, params)
        linhas = cur.fetchall()
        cur.close()
        return linhas


def consultar_um(sql: str, params=()) -> Optional[tuple]:
    linhas = consultar(sql, params)
    return linhas[0] if linhas else None
