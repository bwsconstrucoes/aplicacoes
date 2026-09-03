# -*- coding: utf-8 -*-
"""
Aplica as migracoes da Análise de SPs a partir da propria tela de Configuracoes.

Mesma disciplina do ERP (`erp/core/comum/migracoes.py`): arquivos .sql numerados,
tabela de controle, cada arquivo na sua transacao. E, pelo mesmo motivo do ERP,
DELIBERADAMENTE fora do start do gunicorn — uma migracao com defeito no boot
derrubaria os 15 modulos do monorepo junto, nao so este.
"""
from __future__ import annotations

import os
import logging
from typing import Any

logger = logging.getLogger("analisesps.migracoes")

PASTA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migracoes")


def _com_controle(conn) -> None:
    """Garante schema e tabela de controle. Roda antes de qualquer migracao."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS analisesps")
    conn.execute("CREATE TABLE IF NOT EXISTS analisesps._migracoes ("
                 " nome TEXT PRIMARY KEY,"
                 " aplicada_em TIMESTAMPTZ NOT NULL DEFAULT now())")
    conn.commit()


def listar_estado() -> dict[str, Any]:
    """Diz quais migracoes ja foram aplicadas e quais faltam."""
    from .db import conexao
    from .horario import texto
    with conexao() as conn:
        _com_controle(conn)
        cur = conn.execute("SELECT nome, aplicada_em FROM analisesps._migracoes ORDER BY nome")
        aplicadas = {n: em for n, em in cur.fetchall()}
        cur.close()
    arquivos = sorted(f for f in os.listdir(PASTA) if f.endswith(".sql"))
    return {
        "aplicadas": [{"nome": n, "em": texto(aplicadas[n])}
                      for n in arquivos if n in aplicadas],
        "pendentes": [n for n in arquivos if n not in aplicadas],
    }


def aplicar_pendentes() -> dict[str, Any]:
    """Aplica, em ordem, o que falta. Cada migracao na sua transacao: se a
    terceira falhar, as duas primeiras continuam aplicadas."""
    from .db import conexao
    estado = listar_estado()
    aplicadas, erro = [], None
    for nome in estado["pendentes"]:
        with open(os.path.join(PASTA, nome), encoding="utf-8") as f:
            sql = f.read()
        try:
            with conexao() as conn:
                conn.executescript(sql)
                conn.execute("INSERT INTO analisesps._migracoes (nome) VALUES (?)", (nome,))
                conn.commit()
            aplicadas.append(nome)
            logger.info("Análise de SPs: migracao %s aplicada.", nome)
        except Exception as e:  # noqa: BLE001 — a mensagem vai para a tela
            texto = str(e)
            dica = ""
            if "already exists" in texto:
                dica = "o objeto ja existe; a migracao pode ter sido aplicada pela metade"
            elif "does not exist" in texto:
                dica = "depende de uma migracao anterior que nao foi aplicada"
            elif "permission denied" in texto:
                dica = ("o usuario do banco nao pode criar schema; peca ao Render "
                        "para conceder CREATE ao usuario da DATABASE_URL")
            erro = {"migracao": nome, "erro": texto[:800], "dica": dica}
            logger.exception("Análise de SPs: falha na migracao %s", nome)
            break
    return {"aplicadas": aplicadas, "erro": erro,
            "pendentes_restantes": [n for n in estado["pendentes"]
                                    if n not in aplicadas
                                    and (not erro or n != erro["migracao"])]}
