# ============================================================================
# ERP — core/comum/migracoes.py
# Aplica as migrações pendentes do ERP a partir da própria interface, para o
# ADMIN não precisar do Shell do Render. Mesma lógica do scripts/migrar.py:
# tabela de controle _migracoes + arquivos .sql em scripts/migracoes/.
#
# Deliberadamente FORA do start do serviço: uma migração com problema não pode
# derrubar o monorepo inteiro (baixabradesco, emissaonf, telegram e gateway
# rodam no mesmo processo).
# ============================================================================
from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import text

from app.apps.erp.db.database import obter_engine

logger = logging.getLogger(__name__)

PASTA = os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts", "migracoes"))


def listar_estado() -> dict[str, Any]:
    """Diz quais migrações já foram aplicadas e quais faltam."""
    eng = obter_engine()
    with eng.connect() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS _migracoes ("
            " nome TEXT PRIMARY KEY, aplicada_em TIMESTAMPTZ NOT NULL DEFAULT now())"))
        conn.commit()
        aplicadas = {r[0]: r[1] for r in conn.execute(
            text("SELECT nome, aplicada_em FROM _migracoes ORDER BY nome"))}
    arquivos = sorted(f for f in os.listdir(PASTA) if f.endswith(".sql"))
    return {
        "aplicadas": [{"nome": n, "em": aplicadas[n].strftime("%d/%m/%Y %H:%M")}
                      for n in arquivos if n in aplicadas],
        "pendentes": [n for n in arquivos if n not in aplicadas],
    }


def aplicar_pendentes() -> dict[str, Any]:
    """Aplica, em ordem, as migrações que faltam. Cada uma em sua transação:
    se a terceira falhar, as duas primeiras permanecem aplicadas."""
    estado = listar_estado()
    aplicadas, erro = [], None
    eng = obter_engine()
    for nome in estado["pendentes"]:
        sql = open(os.path.join(PASTA, nome), encoding="utf-8").read()
        try:
            with eng.connect() as conn:
                conn.execute(text(sql))
                conn.execute(text("INSERT INTO _migracoes (nome) VALUES (:n)"), {"n": nome})
                conn.commit()
            aplicadas.append(nome)
            logger.info("ERP: migração %s aplicada", nome)
        except Exception as e:
            texto = str(e)
            dica = ""
            if "unsafe use of new value" in texto:
                dica = ("valor novo de enum usado na mesma migração em que foi criado — "
                        "separe em duas migrações")
            elif "already exists" in texto:
                dica = "objeto já existe; a migração pode ter sido aplicada parcialmente"
            elif "does not exist" in texto:
                dica = "depende de uma migração anterior que não foi aplicada"
            erro = {"migracao": nome, "erro": texto[:800], "dica": dica}
            logger.exception("ERP: falha na migração %s", nome)
            break
    return {"aplicadas": aplicadas, "erro": erro,
            "pendentes_restantes": [n for n in estado["pendentes"] if n not in aplicadas
                                    and (not erro or n != erro["migracao"])]}
