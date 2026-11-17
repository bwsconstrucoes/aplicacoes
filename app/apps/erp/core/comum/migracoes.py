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
    catalogo = _catalogo_de_documentos() if not erro else {}
    return {"aplicadas": aplicadas, "erro": erro,
            "tipos_de_documento": catalogo,
            "pendentes_restantes": [n for n in estado["pendentes"] if n not in aplicadas
                                    and (not erro or n != erro["migracao"])]}


# ---------------------------------------------------------------------------
# ⚑ O CATÁLOGO DE TIPOS DE DOCUMENTO ENTRA AQUI — conserto de 16/11/2026
#
# O dono tentou importar os cards do Pipefy e quatro arquivos de uma pessoa só
# ficaram de fora, todos com o mesmo recado: *"Tipo de documento desconhecido:
# FOTO-COLABORADOR"*, *"…: DOC-PESSOA-OUTRO"*.
#
# A CAUSA, e ela é a mesma família do CREA e do seguro-garantia: os 69 tipos de
# documento moram no CÓDIGO (`core/arquivo/catalogo.py`), e só chegavam ao
# banco quando alguém apertava um botão escondido na tela de Arquivo. Um tipo
# novo subia para o Render e ficava invisível para o sistema até esse clique —
# que ninguém sabia que precisava dar, porque nada avisava. Banco recém-migrado
# tem SETE tipos; o código tem SESSENTA E NOVE.
#
# Agora o catálogo é aplicado junto com as migrações, no mesmo botão que o
# dono já aperta a cada publicação. Ele só CRIA o que falta e nunca apaga nem
# desativa nada — tipo que alguém desligou à mão continua desligado.
#
# Falhar aqui NÃO derruba as migrações: elas já foram aplicadas e confirmadas,
# uma a uma. O catálogo é o acessório, e um acessório não pode fazer parecer
# que o principal falhou.
# ---------------------------------------------------------------------------
def _catalogo_de_documentos() -> dict[str, Any]:
    from app.apps.erp.db.database import get_session

    try:
        from app.apps.erp.core.arquivo import catalogo as svc_catalogo
        with get_session() as s:
            r = svc_catalogo.aplicar(s)
            s.commit()
        logger.info("ERP: catálogo de documentos — %s", r)
        return r
    except Exception as e:                  # noqa: BLE001 — acessório, não trava
        logger.exception("ERP: catálogo de documentos não pôde ser aplicado")
        return {"erro": str(e)[:300]}
