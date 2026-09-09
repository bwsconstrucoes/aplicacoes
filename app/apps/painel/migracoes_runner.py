# -*- coding: utf-8 -*-
"""
Aplica as migracoes do painel a partir da propria tela de Configuracoes.

Mesma disciplina do ERP (`erp/core/comum/migracoes.py`): arquivos .sql numerados,
tabela de controle, cada arquivo na sua transacao. E, pelo mesmo motivo do ERP,
DELIBERADAMENTE fora do start do gunicorn — uma migracao com defeito no boot
derrubaria os 15 modulos do monorepo junto, nao so o painel.
"""
from __future__ import annotations

import os
import logging
from typing import Any

logger = logging.getLogger("painel.migracoes")

PASTA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migracoes")


def _com_controle(conn) -> None:
    """Garante schema e tabela de controle. Roda antes de qualquer migracao."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS painel")
    conn.execute("CREATE TABLE IF NOT EXISTS painel._migracoes ("
                 " nome TEXT PRIMARY KEY,"
                 " aplicada_em TIMESTAMPTZ NOT NULL DEFAULT now())")
    conn.commit()


def listar_estado() -> dict[str, Any]:
    """Diz quais migracoes ja foram aplicadas e quais faltam."""
    from .db import conexao
    from .horario import texto
    with conexao() as conn:
        _com_controle(conn)
        cur = conn.execute("SELECT nome, aplicada_em FROM painel._migracoes ORDER BY nome")
        aplicadas = {n: em for n, em in cur.fetchall()}
        cur.close()
    arquivos = sorted(f for f in os.listdir(PASTA) if f.endswith(".sql"))
    return {
        "aplicadas": [{"nome": n, "em": texto(aplicadas[n])}
                      for n in arquivos if n in aplicadas],
        "pendentes": [n for n in arquivos if n not in aplicadas],
    }


# Marca que uma migracao poe no proprio arquivo para dizer: "a coluna que eu
# crio nasce VAZIA, e quem a preenche e a reconstrucao do fato".
#
# Existe por uma reclamacao justa do dono, em 04/09/2026. A migracao 006 criou
# Vencimento e Pagamento vazias e ele teve de descobrir sozinho que precisava
# apertar "So refazer os numeros" para as colunas encherem. Entrega que exige um
# clique do dono para valer e entrega pela metade.
#
# E uma MARCA no arquivo, e nao uma lista aqui dentro, porque quem sabe se a
# migracao precisa de reconstrucao e quem a escreveu — e a marca fica ao lado do
# SQL que a justifica, nao num lugar que alguem esquece de atualizar.
MARCA_REFAZER_O_FATO = "REFAZER-O-FATO"


def _pede_reconstrucao(sql: str) -> bool:
    return MARCA_REFAZER_O_FATO in sql


def aplicar_pendentes() -> dict[str, Any]:
    """Aplica, em ordem, o que falta. Cada migracao na sua transacao: se a
    terceira falhar, as duas primeiras continuam aplicadas.

    Se alguma das aplicadas pedir, dispara a reconstrucao do fato no fim — ver
    `MARCA_REFAZER_O_FATO`."""
    from .db import conexao
    estado = listar_estado()
    refazer = False
    aplicadas, erro = [], None
    for nome in estado["pendentes"]:
        with open(os.path.join(PASTA, nome), encoding="utf-8") as f:
            sql = f.read()
        try:
            with conexao() as conn:
                conn.executescript(sql)
                conn.execute("INSERT INTO painel._migracoes (nome) VALUES (?)", (nome,))
                conn.commit()
            aplicadas.append(nome)
            refazer = refazer or _pede_reconstrucao(sql)
            logger.info("Painel: migracao %s aplicada.", nome)
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
            logger.exception("Painel: falha na migracao %s", nome)
            break
    reconstrucao = _reconstruir_se_preciso(refazer)

    return {"aplicadas": aplicadas, "erro": erro,
            "reconstrucao": reconstrucao,
            "pendentes_restantes": [n for n in estado["pendentes"]
                                    if n not in aplicadas
                                    and (not erro or n != erro["migracao"])]}


def _reconstruir_se_preciso(pediram: bool) -> dict[str, Any] | None:
    """Dispara "so refazer os numeros" quando uma migracao aplicada pediu.

    Nao baixa nada do OMIE: refaz o fato a partir do espelho que ja esta no
    banco. Devolve o que dizer na tela, ou None quando nao havia o que fazer.

    Tres casos em que NAO dispara, cada um por um motivo:
      - ninguem pediu;
      - a base ainda esta vazia: nao houve primeira carga, entao nao ha o que
        recalcular — e a proxima carga preenche tudo de qualquer jeito;
      - ja existe uma atualizacao rodando: o `disparar` recusa a segunda, e
        aqui isso e o comportamento certo, nao um erro a mostrar em vermelho.

    Falhar aqui NAO derruba a migracao: as migracoes ja foram aplicadas e
    gravadas. O pior caso vira uma frase na tela pedindo o clique manual — que
    e exatamente o que acontecia antes desta funcao existir."""
    if not pediram:
        return None
    try:
        from .consultas import base_vazia
        if base_vazia():
            return {"disparada": False,
                    "mensagem": "As colunas novas serão preenchidas na primeira "
                                "carga da base."}
        from . import tarefas
        resultado = tarefas.disparar("so_numeros", "migracao")
        if resultado.get("ok"):
            logger.info("Painel: reconstrucao do fato disparada pela migracao.")
            return {"disparada": True,
                    "mensagem": "As colunas novas estão sendo preenchidas agora "
                                "— acompanhe em “Atualizar os dados do OMIE”."}
        return {"disparada": False, "mensagem": resultado.get("erro", "")}
    except Exception as e:  # noqa: BLE001 — a migracao ja passou; isto e extra
        logger.exception("Painel: nao consegui disparar a reconstrucao")
        return {"disparada": False,
                "mensagem": f"Não consegui iniciar o recálculo sozinho ({e}). "
                            "Rode “Só refazer os números” quando puder."}
