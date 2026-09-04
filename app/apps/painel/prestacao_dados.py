# -*- coding: utf-8 -*-
"""
A configuração da prestação de contas: sócios, participações, regras e ajustes.

Isto é o único dado do painel que **não dá para regenerar**. Todo o resto vem
do OMIE e se refaz sozinho; isto aqui alguém digitou. É a razão principal de o
painel ter deixado de guardar dados em arquivo: o disco do Render é apagado a
cada reinício, e isto sumiria junto.
"""
from __future__ import annotations

import json

from .db import conexao, consultar

TIPOS_AJUSTE = ["Valor Percebido (-)", "Dívida Assumida (+)", "Outro (+/-)"]
ESCOPOS = {"AMBAS": "Matriz e filial", "MATRIZ": "Só a matriz", "FILIAL": "Só a filial"}


# --------------------------------------------------------------------- leitura
def config() -> dict:
    return dict(consultar("SELECT chave, valor FROM config"))


def socios(apenas_ativos: bool = False) -> list[dict]:
    extra = " WHERE ativo = 1" if apenas_ativos else ""
    return [{"id": i, "nome": n, "tipo": t, "ativo": bool(a)}
            for i, n, t, a in consultar(
                f"SELECT id, nome, tipo, ativo FROM socios{extra} ORDER BY nome")]


def participacoes() -> list[dict]:
    """Quem participa de qual projeto e com quanto. Já vem com o nome do sócio:
    a tela nunca mostra número de cadastro para quem está lendo."""
    return [{"id": i, "projeto": p, "socio_id": s, "socio": nome,
             "tipo": tipo, "pct": float(pct)}
            for i, p, s, nome, tipo, pct in consultar(
                "SELECT p.id, p.projeto, p.socio_id, s.nome, s.tipo, p.pct "
                "  FROM participacoes p JOIN socios s ON s.id = p.socio_id "
                " WHERE s.ativo = 1 ORDER BY p.projeto, s.nome")]


def regras() -> list[dict]:
    """As regras de rateio. `grupos` e `categorias` continuam como texto JSON
    (é assim que o cálculo os espera), mas cada regra vem com um `resumo` legível
    já montado — a tela não deveria precisar interpretar JSON."""
    campos = ("id", "nome", "depto", "todas", "grupos", "categorias", "pct",
              "escopo", "mes_ini", "mes_fim", "ativo")
    saida = []
    for linha in consultar(
            "SELECT id, nome, depto, todas, grupos, categorias, pct, escopo, "
            "       mes_ini, mes_fim, ativo FROM regras ORDER BY id"):
        regra = dict(zip(campos, linha))
        if int(regra["todas"]):
            regra["resumo"] = "todas as despesas"
        else:
            itens = (json.loads(regra["grupos"] or "[]")
                     + json.loads(regra["categorias"] or "[]"))
            regra["resumo"] = ", ".join(itens) if itens else "nada selecionado"
        saida.append(regra)
    return saida


def ajustes() -> list[dict]:
    return [{"id": i, "socio_id": s, "socio": nome, "projeto": p, "data": d,
             "tipo": t, "valor": float(v), "descricao": desc}
            for i, s, nome, p, d, t, v, desc in consultar(
                "SELECT a.id, a.socio_id, s.nome, a.projeto, a.data, a.tipo, "
                "       a.valor, a.descricao "
                "  FROM ajustes a JOIN socios s ON s.id = a.socio_id "
                " ORDER BY a.data DESC, a.id DESC")]


# --------------------------------------------------------------------- escrita
def salvar_config(chave: str, valor: str) -> None:
    with conexao() as conn:
        conn.execute("INSERT INTO config (chave, valor) VALUES (?,?) "
                     "ON CONFLICT (chave) DO UPDATE SET valor = excluded.valor",
                     (chave, str(valor)))
        conn.commit()


def salvar_socio(nome: str, tipo: str = "Interno", socio_id=None) -> None:
    with conexao() as conn:
        if socio_id:
            conn.execute("UPDATE socios SET nome=?, tipo=? WHERE id=?",
                         (nome.strip(), tipo, int(socio_id)))
        else:
            conn.execute("INSERT INTO socios (nome, tipo) VALUES (?,?) "
                         "ON CONFLICT (nome) DO UPDATE SET tipo = excluded.tipo",
                         (nome.strip(), tipo))
        conn.commit()


def desativar_socio(socio_id: int) -> None:
    """Sócio sai de cena sem apagar histórico: as quotas já calculadas e os
    ajustes lançados continuam existindo."""
    with conexao() as conn:
        conn.execute("UPDATE socios SET ativo = 0 WHERE id = ?", (int(socio_id),))
        conn.commit()


def salvar_participacao(projeto: str, socio_id: int, pct: float) -> None:
    with conexao() as conn:
        conn.execute(
            "INSERT INTO participacoes (projeto, socio_id, pct) VALUES (?,?,?) "
            "ON CONFLICT (projeto, socio_id) DO UPDATE SET pct = excluded.pct",
            (projeto.strip(), int(socio_id), float(pct)))
        conn.commit()


def apagar_participacao(participacao_id: int) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM participacoes WHERE id = ?", (int(participacao_id),))
        conn.commit()


def salvar_regra(dados: dict, regra_id=None) -> None:
    valores = (
        dados["nome"].strip(), dados["depto"].strip(),
        1 if dados.get("todas") else 0,
        json.dumps(dados.get("grupos") or [], ensure_ascii=False),
        json.dumps(dados.get("categorias") or [], ensure_ascii=False),
        float(dados.get("pct") or 100), dados.get("escopo") or "AMBAS",
        (dados.get("mes_ini") or "").strip(), (dados.get("mes_fim") or "").strip(),
        1 if dados.get("ativo", True) else 0,
    )
    with conexao() as conn:
        if regra_id:
            conn.execute(
                "UPDATE regras SET nome=?, depto=?, todas=?, grupos=?, categorias=?,"
                " pct=?, escopo=?, mes_ini=?, mes_fim=?, ativo=? WHERE id=?",
                valores + (int(regra_id),))
        else:
            conn.execute(
                "INSERT INTO regras (nome, depto, todas, grupos, categorias, pct,"
                " escopo, mes_ini, mes_fim, ativo) VALUES (?,?,?,?,?,?,?,?,?,?)",
                valores)
        conn.commit()


def salvar_parametros_das_regras(regras) -> int:
    """Grava um cenário por cima das regras oficiais — só os PARÂMETROS.

    Mexe em `pct`, `escopo`, `mes_ini`, `mes_fim` e `ativo`, que é exatamente o
    que a tela de cenários deixa alterar. Grupos e categorias não são tocados:
    quem os escolhe é a tela de Regras, onde existe a lista para escolher.

    A tela antiga fazia isto com um DELETE de todas as regras seguido de um
    INSERT de todas — o que trocava os ids a cada gravação e perdia o vínculo de
    qualquer coisa que apontasse para uma regra. Aqui é UPDATE por id.

    Devolve quantas regras foram alteradas."""
    if not regras:
        return 0
    with conexao() as conn:
        for regra in regras:
            conn.execute(
                "UPDATE regras SET pct=?, escopo=?, mes_ini=?, mes_fim=?, ativo=?"
                " WHERE id=?",
                (float(regra.get("pct") or 0), regra.get("escopo") or "AMBAS",
                 (regra.get("mes_ini") or "").strip(),
                 (regra.get("mes_fim") or "").strip(),
                 1 if int(regra.get("ativo", 1)) else 0, int(regra["id"])))
        conn.commit()
    return len(regras)


def apagar_regra(regra_id: int) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM regras WHERE id = ?", (int(regra_id),))
        conn.commit()


def salvar_ajuste(socio_id: int, tipo: str, valor: float, data: str = "",
                  projeto: str = "", descricao: str = "") -> None:
    with conexao() as conn:
        conn.execute(
            "INSERT INTO ajustes (socio_id, projeto, data, tipo, valor, descricao) "
            "VALUES (?,?,?,?,?,?)",
            (int(socio_id), projeto.strip(), data.strip(), tipo,
             float(valor), descricao.strip()))
        conn.commit()


def apagar_ajuste(ajuste_id: int) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM ajustes WHERE id = ?", (int(ajuste_id),))
        conn.commit()


# --------------------------------------------------------------------- importação
def importar_do_arquivo_local(caminho_sqlite: str) -> dict:
    """Traz a configuração do `prestacao_contas.db` que rodava no computador.

    Existe para uma vez só: a configuração de sócios e percentuais é dado
    sensível e não foi versionada no Git — o caminho é o dono enviar o arquivo
    pela tela de Configurações.

    Não apaga nada do que já existe: sócio com o mesmo nome é reaproveitado,
    participação e regra com a mesma identidade são atualizadas.
    """
    import sqlite3

    origem = sqlite3.connect(f"file:{caminho_sqlite}?mode=ro", uri=True)
    try:
        contagem = {"socios": 0, "participacoes": 0, "regras": 0,
                    "ajustes": 0, "config": 0}
        de_para = {}    # id no arquivo -> id no banco

        with conexao() as conn:
            for chave, valor in origem.execute("SELECT chave, valor FROM config"):
                conn.execute(
                    "INSERT INTO config (chave, valor) VALUES (?,?) "
                    "ON CONFLICT (chave) DO UPDATE SET valor = excluded.valor",
                    (chave, valor))
                contagem["config"] += 1

            for id_antigo, nome, tipo, ativo in origem.execute(
                    "SELECT id, nome, tipo, ativo FROM socios"):
                cur = conn.execute(
                    "INSERT INTO socios (nome, tipo, ativo) VALUES (?,?,?) "
                    "ON CONFLICT (nome) DO UPDATE SET tipo = excluded.tipo, "
                    "  ativo = excluded.ativo RETURNING id",
                    (nome, tipo, ativo))
                de_para[id_antigo] = cur.fetchone()[0]
                cur.close()
                contagem["socios"] += 1

            for projeto, socio_antigo, pct in origem.execute(
                    "SELECT projeto, socio_id, pct FROM participacoes"):
                if socio_antigo not in de_para:
                    continue
                conn.execute(
                    "INSERT INTO participacoes (projeto, socio_id, pct) VALUES (?,?,?) "
                    "ON CONFLICT (projeto, socio_id) DO UPDATE SET pct = excluded.pct",
                    (projeto, de_para[socio_antigo], pct))
                contagem["participacoes"] += 1

            conn.execute("DELETE FROM regras")     # as regras vêm inteiras do arquivo
            for linha in origem.execute(
                    "SELECT nome, depto, todas, grupos, categorias, pct, escopo, "
                    "       mes_ini, mes_fim, ativo FROM regras"):
                conn.execute(
                    "INSERT INTO regras (nome, depto, todas, grupos, categorias, "
                    " pct, escopo, mes_ini, mes_fim, ativo) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    linha)
                contagem["regras"] += 1

            # Ajustes não têm identidade natural (dois lançamentos iguais no
            # mesmo dia são possíveis e legítimos). Importar por cima somaria
            # tudo de novo, então a importação substitui: o arquivo manda.
            conn.execute("DELETE FROM ajustes")
            for socio_antigo, projeto, data, tipo, valor, descricao in origem.execute(
                    "SELECT socio_id, projeto, data, tipo, valor, descricao FROM ajustes"):
                if socio_antigo not in de_para:
                    continue
                conn.execute(
                    "INSERT INTO ajustes (socio_id, projeto, data, tipo, valor, descricao) "
                    "VALUES (?,?,?,?,?,?)",
                    (de_para[socio_antigo], projeto, data, tipo, valor, descricao))
                contagem["ajustes"] += 1

            conn.commit()
        return contagem
    finally:
        origem.close()
