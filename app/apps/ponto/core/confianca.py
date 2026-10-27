# -*- coding: utf-8 -*-
"""
A CONFERÊNCIA DO ROSTO QUE SE ADAPTA À PESSOA.

Pedido do dono, 17/10/2026: *"quanto melhor a pessoa, menos se confere; quanto
pior a pessoa, mais se confere. (…) A gente começa conferindo tudo e, à medida
que for compreendido que determinadas pessoas não têm a necessidade de tanta
conferência, a gente vai reduzindo. Mas se de repente, nas conferências
aleatórias, aquela pessoa começar a aparecer de novo, a gente volta ela para
insistir na conferência."*

COMO FUNCIONA:
  · cada pessoa tem uma SEQUÊNCIA LIMPA: quantas conferências seguidas deram
    "mesma pessoa" desde a última falha. Falha = rosto que não confere ou foto
    sem rosto. Rosto que não conferiu, mas o DP olhou e aprovou a batida, é falso
    alarme: não conta como falha (nem como limpa);
  · a sequência dá o NÍVEL, e o nível dá a CHANCE de cada batida ser sorteada
    para conferir: Em observação 100 % → Boa 50 % → Ótima 25 % → Exemplar 10 %.
    Ninguém chega a zero — o sorteio continua, e é ele que pega quem relaxou;
  · falhou: a sequência volta a zero e a pessoa volta a ser conferida em tudo;
  · REINCIDENTE (2 falhas ou mais em 60 dias): precisa do DOBRO da sequência
    para descer de nível, e o alerta diz que é reincidência;
  · GARANTIA: quem não tem uma conferência de verdade há 7 dias é conferido na
    próxima batida, sorteado ou não.

O sorteio é pela própria batida (o número dela, embaralhado), não pela hora: a
mesma batida dá sempre o mesmo resultado, e ninguém consegue prever qual será.

Os limites moram em `rosto.config` › "dinamica" e se mudam em Configuração ›
Fotos. Só vale o que veio depois do início do ponto (vigencia.py).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao

NIVEIS = (("OBSERVACAO", "Em observação"), ("BOA", "Boa"), ("OTIMA", "Ótima"), ("EXEMPLAR", "Exemplar"))
PADRAO = {
    "ligada": True,
    # Por nível (na ordem de NIVEIS): a sequência limpa a partir da qual vale, e a % conferida.
    "limpas": [0, 20, 60, 150],
    "pct": [100, 50, 25, 10],
    "reincidente_falhas": 2,      # falhas no período abaixo → reincidente
    "reincidente_dias": 60,
    "garantia_dias": 7,           # no máximo isso sem uma conferência de verdade
}
PISO_PCT = 5                      # ninguém fica sem sorteio
FALHAS = ("OUTRA_PESSOA", "SEM_ROSTO")


def disponivel(conn: Connection) -> bool:
    """A migração 013 (o resultado DISPENSADA e o nível gravado)."""
    return db.tem_coluna(conn, "conferencias_rosto", "nivel")


def config(cfg_rosto: dict) -> dict:
    d = dict(PADRAO)
    d.update({k: v for k, v in (cfg_rosto.get("dinamica") or {}).items() if k in PADRAO})
    return d


def validar(novos: dict, atual: dict) -> dict:
    """A parte "dinamica" de rosto.config, conferida. Devolve o dict a gravar."""
    d = config({"dinamica": atual})
    if "ligada" in novos:
        d["ligada"] = bool(novos["ligada"])
    try:
        if "limpas" in novos:
            d["limpas"] = [int(x) for x in novos["limpas"]]
        if "pct" in novos:
            d["pct"] = [int(x) for x in novos["pct"]]
        for k in ("reincidente_falhas", "reincidente_dias", "garantia_dias"):
            if k in novos:
                d[k] = int(novos[k])
    except (TypeError, ValueError):
        raise ErroDeValidacao("os números da conferência que se adapta têm de ser inteiros", campo="dinamica")
    if len(d["limpas"]) != len(NIVEIS) or len(d["pct"]) != len(NIVEIS):
        raise ErroDeValidacao(f"são {len(NIVEIS)} níveis", campo="dinamica")
    d["limpas"][0], d["pct"][0] = 0, 100              # quem começa, ou falhou, confere tudo
    if any(b <= a for a, b in zip(d["limpas"], d["limpas"][1:])):
        raise ErroDeValidacao("cada nível pede mais conferências limpas que o anterior", campo="limpas")
    if any(b > a for a, b in zip(d["pct"], d["pct"][1:])) or min(d["pct"]) < PISO_PCT:
        raise ErroDeValidacao(f"a % conferida desce de nível em nível, e nunca abaixo de {PISO_PCT} %",
                              campo="pct")
    if d["limpas"][-1] > 2000:
        raise ErroDeValidacao("no máximo 2.000 conferências limpas no último nível", campo="limpas")
    if not 1 <= d["reincidente_falhas"] <= 20 or not 7 <= d["reincidente_dias"] <= 365:
        raise ErroDeValidacao("reincidente: de 1 a 20 falhas, em 7 a 365 dias", campo="reincidente_falhas")
    if not 1 <= d["garantia_dias"] <= 60:
        raise ErroDeValidacao("a garantia é de 1 a 60 dias", campo="garantia_dias")
    return d


# ---------------------------------------------------------------------------
# As contas (PURAS)
# ---------------------------------------------------------------------------
def nivel(limpas: int, falhas_recentes: int, d: dict) -> dict:
    """PURA. O nível de quem tem `limpas` conferências limpas seguidas e
    `falhas_recentes` falhas no período de reincidência."""
    reincidente = falhas_recentes >= int(d["reincidente_falhas"])
    fator = 2 if reincidente else 1
    i = max(k for k, minimo in enumerate(d["limpas"]) if limpas >= minimo * fator)
    codigo, rotulo = NIVEIS[i]
    saida = {"codigo": codigo, "rotulo": rotulo, "pct": int(d["pct"][i]), "limpas": limpas,
             "falhas_recentes": falhas_recentes, "reincidente": reincidente, "proximo": None}
    if i + 1 < len(NIVEIS):
        saida["proximo"] = {"rotulo": NIVEIS[i + 1][1], "pct": int(d["pct"][i + 1]),
                            "faltam": d["limpas"][i + 1] * fator - limpas}
    return saida


def sorteada(marcacao_id: int, pct: int) -> bool:
    """PURA. A batida cai no sorteio de `pct` %? Sempre o mesmo resultado para a
    mesma batida; o embaralhamento leva um segredo do servidor, para ninguém prever."""
    if pct >= 100:
        return True
    sal = os.environ.get("PONTO_API_KEY", "") or os.environ.get("SECRET_KEY", "")
    h = hashlib.sha256(f"bwsp-sorteio|{sal}|{int(marcacao_id)}".encode()).hexdigest()
    return int(h[:8], 16) % 100 < pct


def decidir(n: dict, marcacao_id: int, ultima_real: Optional[dt.datetime], agora: dt.datetime,
            d: dict) -> tuple[bool, str]:
    """PURA. (confere?, por quê) — para o registro da batida."""
    if not d["ligada"] or n["pct"] >= 100:
        return True, ""
    if ultima_real is None or agora - ultima_real >= dt.timedelta(days=int(d["garantia_dias"])):
        return True, f"nível {n['rotulo']}, mas sem conferência há {d['garantia_dias']} dias ou mais"
    if sorteada(marcacao_id, n["pct"]):
        return True, f"nível {n['rotulo']}: sorteada ({n['pct']} %)"
    return False, f"nível {n['rotulo']}: confere {n['pct']} % das batidas — não sorteada"


# ---------------------------------------------------------------------------
# O histórico de cada pessoa
# ---------------------------------------------------------------------------
def historico(conn: Connection, ids: Optional[list[int]], d: dict) -> dict[int, dict]:
    """Por pessoa: a sequência limpa, as falhas recentes e a última conferência de
    verdade. `ids` None = todo mundo que já foi conferido."""
    from . import vigencia
    filtro = "AND m.colaborador_id = ANY(:ids)" if ids is not None else ""
    linhas = db.todos(conn, f"""
        WITH c AS (
            SELECT m.colaborador_id AS cid, r.resultado, r.conferido_em,
                   -- a batida que o DP validou depois é falso alarme — também a "sem rosto"
                   -- (28/10/2026: a pessoa ficava "em observação" por fotos com o rosto à vista)
                   (r.resultado IN ('OUTRA_PESSOA', 'SEM_ROSTO') AND m.status <> 'VALIDA') AS falha
              FROM ponto.conferencias_rosto r JOIN ponto.marcacoes m ON m.id = r.marcacao_id
             WHERE m.data_referencia >= :vale {filtro}
               AND r.resultado IN ('MESMA_PESSOA', 'OUTRA_PESSOA', 'SEM_ROSTO')),
        u AS (SELECT cid, max(conferido_em) FILTER (WHERE falha) AS ultima_falha,
                     max(conferido_em) AS ultima_real,
                     count(*) FILTER (WHERE falha AND conferido_em > now() - make_interval(days => :dias)) AS falhas
                FROM c GROUP BY cid)
        SELECT u.cid, u.ultima_falha, u.ultima_real, u.falhas,
               (SELECT count(*) FROM c WHERE c.cid = u.cid AND c.resultado = 'MESMA_PESSOA'
                   AND c.conferido_em > coalesce(u.ultima_falha, '-infinity'::timestamptz)) AS limpas
          FROM u""", vale=vigencia.inicio(conn), ids=list(ids or []), dias=int(d["reincidente_dias"]))
    return {int(l["cid"]): {"limpas": int(l["limpas"]), "falhas_recentes": int(l["falhas"]),
                            "ultima_real": l["ultima_real"], "ultima_falha": l["ultima_falha"]} for l in linhas}


def da_pessoa(h: Optional[dict], d: dict) -> dict:
    h = h or {"limpas": 0, "falhas_recentes": 0, "ultima_real": None, "ultima_falha": None}
    n = nivel(h["limpas"], h["falhas_recentes"], d)
    n["ultima_real"] = h["ultima_real"].isoformat() if h.get("ultima_real") else None
    n["ultima_falha"] = h["ultima_falha"].isoformat() if h.get("ultima_falha") else None
    return n


def pessoas(conn: Connection, d: dict, dias: int = 30) -> list[dict]:
    """Quem bateu com foto nos últimos `dias`, com o nível de cada um — os que
    mais se confere primeiro (reincidentes, depois em observação)."""
    from . import vigencia
    gente = db.todos(conn, """
        SELECT c.id, c.nome, o.codigo AS obra, f.nome AS funcao, count(*) AS batidas
          FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
          LEFT JOIN public.obras o ON o.id = c.obra_id
          LEFT JOIN public.funcoes f ON f.id = c.funcao_id
         WHERE m.foto_id IS NOT NULL AND m.timestamp_servidor > now() - make_interval(days => :d)
           AND m.data_referencia >= :vale
         GROUP BY c.id, c.nome, o.codigo, f.nome""", d=dias, vale=vigencia.inicio(conn))
    hist = historico(conn, [int(g["id"]) for g in gente], d)
    saida = [{"id": int(g["id"]), "nome": g["nome"], "obra": g["obra"], "funcao": g["funcao"], "batidas": int(g["batidas"]),
              **da_pessoa(hist.get(int(g["id"])), d)} for g in gente]
    ordem = [c for c, _ in NIVEIS]
    saida.sort(key=lambda p: (not p["reincidente"], ordem.index(p["codigo"]), p["nome"] or ""))
    return saida


ROTULO_RESULTADO = {"MESMA_PESSOA": "o rosto confere", "OUTRA_PESSOA": "o rosto NÃO confere",
                    "SEM_ROSTO": "nenhum rosto na foto da batida", "SEM_CADASTRAL": "sem foto de cadastro para comparar",
                    "VIROU_CADASTRAL": "virou a foto de cadastro", "DISPENSADA": "não sorteada (não conferida)",
                    "ERRO": "não deu para conferir"}


def porque(n: dict, ultima: Optional[dict], d: dict) -> str:
    """PURA. O nível da pessoa, em uma frase (28/10/2026, o dono: "é uma pessoa que está em
    observação, reincidente. Eu clico em cima dela (…) e não consigo entender por que está em
    observação")."""
    falta = n.get("proximo")
    subir = (f" Faltam {falta['faltam']} conferência(s) limpa(s) seguidas para subir para {falta['rotulo']}"
             f" ({falta['pct']} % conferidas).") if falta else " É o nível mais alto."
    if n["reincidente"]:
        txt = (f"Reincidente: {n['falhas_recentes']} falha(s) nos últimos {d['reincidente_dias']} dias "
               f"(o limite é {d['reincidente_falhas']}) — por isso precisa do dobro das conferências limpas para subir.")
    elif n["codigo"] == "OBSERVACAO" and ultima:
        txt = (f"Em observação porque a conferência de {ultima['quando']} deu “{ultima['rotulo']}”"
               f" (batida das {ultima['hora']}). Falhou, volta a conferir todas as batidas.")
    elif n["codigo"] == "OBSERVACAO":
        txt = "Em observação porque todo mundo começa assim: as primeiras batidas são todas conferidas."
    else:
        txt = f"{n['limpas']} conferência(s) limpa(s) seguidas: confere {n['pct']} % das batidas, por sorteio."
    if ultima and n["codigo"] == "OBSERVACAO" and ultima.get("resultado") == "SEM_ROSTO":
        txt += (" Se a foto da batida mostra o rosto, o problema costuma ser a foto de CADASTRO: confira em"
                " Fotos de cadastro; e valide a batida — batida validada é falso alarme e não conta como falha.")
    return txt + (subir if n["codigo"] != "EXEMPLAR" else "")


def explicar(conn: Connection, colaborador_id: int, d: dict, limite: int = 15) -> dict:
    """Por que a pessoa está neste nível: o nível, a frase e as últimas conferências."""
    from .. import horario
    from . import vigencia
    nivel_atual = da_pessoa(historico(conn, [int(colaborador_id)], d).get(int(colaborador_id)), d)
    linhas = db.todos(conn, """
        SELECT r.marcacao_id, r.resultado, r.semelhanca, r.detalhe, r.conferido_em, r.nivel,
               m.timestamp_servidor, m.status, m.foto_id IS NOT NULL AS tem_foto, o.codigo AS obra
          FROM ponto.conferencias_rosto r JOIN ponto.marcacoes m ON m.id = r.marcacao_id
          JOIN public.obras o ON o.id = m.obra_id
         WHERE m.colaborador_id = :c AND m.data_referencia >= :vale
         ORDER BY r.conferido_em DESC LIMIT :lim""", c=int(colaborador_id), vale=vigencia.inicio(conn), lim=limite)
    conferencias = []
    for l in linhas:
        local = horario.para_local(l["timestamp_servidor"])
        falha = l["resultado"] in ("OUTRA_PESSOA", "SEM_ROSTO") and l["status"] != "VALIDA"
        conferencias.append({
            "marcacao_id": int(l["marcacao_id"]), "resultado": l["resultado"],
            "rotulo": ROTULO_RESULTADO.get(l["resultado"], l["resultado"]),
            "semelhanca": float(l["semelhanca"]) if l["semelhanca"] is not None else None,
            "detalhe": l["detalhe"], "quando": local.strftime("%d/%m"), "hora": local.strftime("%H:%M"),
            "obra": l["obra"], "status": l["status"], "tem_foto": bool(l["tem_foto"]), "falha": falha,
            "falso_alarme": l["resultado"] in ("OUTRA_PESSOA", "SEM_ROSTO") and l["status"] == "VALIDA"})
    ultima_falha = next((c for c in conferencias if c["falha"]), None)
    return {"nivel": nivel_atual, "porque": porque(nivel_atual, ultima_falha, d), "conferencias": conferencias,
            "dinamica": d}


def resumo(lista: list[dict]) -> dict:
    """PURA. Quantas pessoas em cada nível, e quantos reincidentes."""
    conta = {c: 0 for c, _ in NIVEIS}
    for p in lista:
        conta[p["codigo"]] += 1
    return {"por_nivel": [{"codigo": c, "rotulo": r, "pessoas": conta[c]} for c, r in NIVEIS],
            "reincidentes": sum(1 for p in lista if p["reincidente"]), "pessoas": len(lista)}
