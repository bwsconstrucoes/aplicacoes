# -*- coding: utf-8 -*-
"""
A APROPRIAÇÃO dos lançamentos de conta corrente, perguntada ao OMIE.

07/10/2026. O dono mandou o arquivo cru do movimento financeiro de 09/01/2026,
e ele respondeu a dúvida de vez: os 193 movimentos do dia vêm com "detalhes" e
"resumo" — e NENHUM com departamento. O lançamento da Sicredi (100.000,01,
metade CRECHESUAPE, metade ESCPE18 na tela do OMIE) chegava ao painel sem obra.

O título traz a apropriação na própria listagem (`distribuicao`). O lançamento
feito direto na conta corrente, não: ela só vem na consulta DELE, uma chamada
por lançamento. É o que este módulo faz:

- pergunta só pelo que ainda não foi respondido — a resposta fica guardada
  (migração 022) e não é apagada quando a janela de pagamentos é relida;
- não pergunta por TRANSFERÊNCIA entre contas da empresa, que não tem obra;
- pergunta primeiro pelos mais recentes, e no máximo `LIMITE_POR_RODADA` por
  atualização: o primeiro dia pode ter milhares atrasados, e a atualização
  da madrugada não pode virar uma tarde inteira;
- PARA SOZINHO se as primeiras perguntas falharem todas. O formato da consulta
  não foi conferido contra o OMIE real (a documentação não abre daqui): se o
  número do movimento não for o número do lançamento, ou o nome do método
  estiver errado, são milhares de chamadas inúteis. Cinco bastam para saber.

Falha aqui NÃO derruba a atualização: os lançamentos continuam entrando no
painel, como "(não apropriado)", e a tela diz o que faltou.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time

log = logging.getLogger("painel.apropriacao_cc")

LIMITE_POR_RODADA = 600
TENTATIVAS_POR_LANCAMENTO = 3
FALHAS_SEGUIDAS_NO_COMECO = 5
CONFIRMAR_A_CADA = 25

# Transferência entre contas da empresa: não tem obra, não vale a chamada.
ORIGENS_DE_TRANSFERENCIA = ("TRAP", "TRAR")


def tabela_existe(conn) -> bool:
    """A migração 022 já foi aplicada? Sem ela, nada é perguntado."""
    try:
        return bool(conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'painel'"
            "   AND table_name = 'apropriacao_lancamentos_cc'").fetchone())
    except Exception:  # noqa: BLE001 — conexão de teste, sem catálogo
        return False


_DATA = ("CASE WHEN m.ddtpagamento ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'"
         " THEN to_date(m.ddtpagamento, 'DD/MM/YYYY') END")
_CODIGO = ("COALESCE(m.bruto::json->'detalhes'->>'nCodMovCC',"
           " m.bruto::json->'detalhes'->>'nCodLanc')")


def pendentes(conn, de: dt.date | None = None, ate: dt.date | None = None,
              limite: int | None = LIMITE_POR_RODADA,
              retentar: bool = False) -> list[int]:
    """Os números dos lançamentos que ainda não têm a apropriação guardada:
    primeiro os nunca perguntados, do mais recente para o mais antigo; depois
    os que falharam.

    `retentar`: pergunta de novo também os que já falharam
    `TENTATIVAS_POR_LANCAMENTO` vezes — que as atualizações comuns deixam de
    lado para não gastar chamada à toa todo dia."""
    filtros, params = [], []
    if de:
        filtros.append(f"{_DATA} >= ?")
        params.append(de)
    if ate:
        filtros.append(f"{_DATA} <= ?")
        params.append(ate)
    onde = "".join(f" AND {f}" for f in filtros)
    desistiu = "" if retentar else " AND a.tentativas < ?"
    sql = (
        f"SELECT p.codigo FROM ("
        f"  SELECT {_CODIGO} AS codigo, MAX({_DATA}) AS dia"
        f"    FROM movimentos_sem_titulo m"
        f"   WHERE m.bruto IS NOT NULL"
        f"     AND COALESCE(m.cliquidado, '') <> 'N'"
        f"     AND COALESCE(m.bruto::json->'detalhes'->>'cOrigem', '')"
        f"         NOT IN ('TRAP', 'TRAR')"
        f"     AND COALESCE({_CODIGO}, '') ~ '^[0-9]+$'{onde}"
        f"   GROUP BY 1) p"
        f"  LEFT JOIN apropriacao_lancamentos_cc a ON a.ncodmovcc = p.codigo::bigint"
        f" WHERE a.ncodmovcc IS NULL"
        f"    OR (a.erro IS NOT NULL{desistiu})"
        f" ORDER BY (a.ncodmovcc IS NOT NULL), p.dia DESC NULLS LAST")
    if not retentar:
        params.append(TENTATIVAS_POR_LANCAMENTO)
    if limite:
        sql += f" LIMIT {int(limite)}"
    return [int(c) for (c,) in conn.execute(sql, params).fetchall()]


def _guardar(conn, codigo: int, bruto: str | None, erro: str | None) -> None:
    conn.execute(
        "INSERT INTO apropriacao_lancamentos_cc (ncodmovcc, bruto, erro, tentativas, lido_em)"
        " VALUES (?, ?, ?, 1, now())"
        " ON CONFLICT (ncodmovcc) DO UPDATE SET"
        "   bruto = COALESCE(EXCLUDED.bruto, apropriacao_lancamentos_cc.bruto),"
        "   erro = EXCLUDED.erro,"
        "   tentativas = apropriacao_lancamentos_cc.tentativas + 1,"
        "   lido_em = now()",
        (int(codigo), bruto, erro))


def buscar(conn, cli, de: dt.date | None = None, ate: dt.date | None = None,
           limite: int | None = LIMITE_POR_RODADA, progresso=None,
           retentar: bool = False) -> dict:
    """Pergunta ao OMIE a apropriação dos lançamentos pendentes e guarda a
    resposta inteira. `cli` pode ser o cliente ou uma função que o crie. Devolve {'lidos', 'falhas', 'pendentes', 'parou'}:
    `parou` traz o motivo quando as primeiras perguntas falharam todas."""
    if not tabela_existe(conn):
        return {"lidos": 0, "falhas": 0, "pendentes": 0,
                "parou": "falta apertar \"Aplicar atualizações do banco\" (migração 022)"}
    codigos = pendentes(conn, de, ate, limite, retentar=retentar)
    if codigos and callable(cli) and not hasattr(cli, "consultar_lancamento_cc"):
        cli = cli()            # o cliente só nasce se houver o que perguntar
    lidos = falhas = 0
    ultimo_erro = ""
    pausa = float(getattr(cli, "pausa", 0) or 0)
    for i, codigo in enumerate(codigos, 1):
        if pausa and i > 1:
            time.sleep(pausa)
        try:
            resposta = cli.consultar_lancamento_cc(codigo)
        except Exception as e:  # noqa: BLE001 — um lançamento não para os outros
            # o bloqueio do OMIE (consumo excessivo) para a rodada inteira
            if type(e).__name__ == "OmieBloqueada":
                conn.commit()
                raise
            falhas += 1
            ultimo_erro = str(e)[:500]
            _guardar(conn, codigo, None, ultimo_erro)
            if lidos == 0 and falhas >= FALHAS_SEGUIDAS_NO_COMECO:
                conn.commit()
                log.warning("Apropriação: %s perguntas, nenhuma respondida — parei. "
                            "Último erro: %s", falhas, ultimo_erro)
                return {"lidos": 0, "falhas": falhas, "pendentes": len(codigos),
                        "parou": f"o OMIE não respondeu as {falhas} primeiras "
                                 f"perguntas ({ultimo_erro[:200]})"}
            continue
        _guardar(conn, codigo, json.dumps(resposta, ensure_ascii=False, default=str), None)
        lidos += 1
        if i % CONFIRMAR_A_CADA == 0:
            conn.commit()
        if progresso:
            progresso(i, len(codigos))
    conn.commit()
    log.info("Apropriação: %s lançamentos lidos, %s falharam.", lidos, falhas)
    return {"lidos": lidos, "falhas": falhas, "pendentes": len(codigos), "parou": ""}


def guardadas(conn) -> dict[int, str]:
    """{número do lançamento: resposta do OMIE} — o que o fato usa para ratear."""
    if not tabela_existe(conn):
        return {}
    return {int(c): b for c, b in conn.execute(
        "SELECT ncodmovcc, bruto FROM apropriacao_lancamentos_cc"
        " WHERE bruto IS NOT NULL").fetchall()}


def _tem_obra(bruto) -> bool:
    from .fato import _departamentos_do_bruto
    return bool(_departamentos_do_bruto(bruto))


def situacao(conn) -> dict:
    """Quantos lançamentos de conta já têm a obra lida do OMIE, e quantos não.

    10/11/2026: o dono achou um lançamento de 28/12/2020 "(não apropriado)" no
    painel e apropriado no OMIE — *"situação gravíssima"*. O painel não dizia
    quantos estavam nessa situação, nem por quê. Agora diz:
    - `com_obra` / `sem_obra`: o OMIE respondeu, com ou sem obra;
    - `pendentes`: nunca perguntados (ou falharam menos de 3 vezes);
    - `desistidos`: falharam 3 vezes e as atualizações comuns pararam de
      perguntar;
    - `sem_numero`: guardados sem o movimento inteiro (antes da migração 021) —
      não dá para perguntar até o período ser relido."""
    vazio = {"existe": False}
    if not tabela_existe(conn):
        return vazio
    linhas = conn.execute(
        f"SELECT p.codigo, p.ano, a.bruto, a.erro, COALESCE(a.tentativas, 0) FROM ("
        f"  SELECT {_CODIGO} AS codigo, EXTRACT(YEAR FROM MAX({_DATA}))::int AS ano"
        f"    FROM movimentos_sem_titulo m"
        f"   WHERE m.bruto IS NOT NULL"
        f"     AND COALESCE(m.cliquidado, '') <> 'N'"
        f"     AND COALESCE(m.bruto::json->'detalhes'->>'cOrigem', '')"
        f"         NOT IN ('TRAP', 'TRAR')"
        f"     AND COALESCE({_CODIGO}, '') ~ '^[0-9]+$'"
        f"   GROUP BY 1) p"
        f"  LEFT JOIN apropriacao_lancamentos_cc a ON a.ncodmovcc = p.codigo::bigint"
    ).fetchall()
    r = {"existe": True, "total": len(linhas), "com_obra": 0, "sem_obra": 0,
         "pendentes": 0, "desistidos": 0, "pendentes_por_ano": {}}
    for _codigo, ano, bruto, erro, tentativas in linhas:
        if bruto is not None:
            r["com_obra" if _tem_obra(bruto) else "sem_obra"] += 1
            continue
        if erro is not None and tentativas >= TENTATIVAS_POR_LANCAMENTO:
            r["desistidos"] += 1
        else:
            r["pendentes"] += 1
        chave = str(ano or "sem data")
        r["pendentes_por_ano"][chave] = r["pendentes_por_ano"].get(chave, 0) + 1
    r["pendentes_por_ano"] = dict(sorted(r["pendentes_por_ano"].items(), reverse=True))
    (r["sem_numero"],) = conn.execute(
        "SELECT COUNT(*) FROM movimentos_sem_titulo"
        " WHERE bruto IS NULL AND COALESCE(cliquidado, '') <> 'N'").fetchone()
    return r


def diagnostico(conn, codigo: int) -> dict:
    """Tudo o que o painel tem de UM lançamento de conta: o movimento como veio
    do OMIE, a resposta guardada da consulta (ou o erro), e as linhas que
    viraram no painel. É o que responde "por que este está sem obra?" com
    dado, em vez de suposição."""
    texto = str(int(codigo))
    movimentos = [
        {"id": mid, "data": dpg, "valor": float(v or 0), "conta": ncc,
         "categoria": ccat, "liquidado": liq,
         "detalhes": (json.loads(b).get("detalhes") if b else None)}
        for mid, dpg, v, ncc, ccat, liq, b in conn.execute(
            "SELECT id, ddtpagamento, nvalpago, ncodcc, ccodcateg, cliquidado, bruto"
            "  FROM movimentos_sem_titulo m"
            " WHERE m.bruto IS NOT NULL"
            "   AND (m.bruto::json->'detalhes'->>'nCodMovCC' = ?"
            "        OR m.bruto::json->'detalhes'->>'nCodLanc' = ?)",
            (texto, texto)).fetchall()]
    guardada = None
    if tabela_existe(conn):
        # o número que se pergunta ao OMIE é o nCodMovCC do movimento; o que o
        # painel mostra é o nCodLanc. Hoje são iguais — mas se um dia não
        # forem, o diagnóstico tem de achar a resposta pelos dois
        candidatos = [int(codigo)] + [
            int(v) for m in movimentos for k in ("nCodMovCC", "nCodLanc")
            if str(v := (m["detalhes"] or {}).get(k) or "").isdigit()]
        linha = None
        for candidato in dict.fromkeys(candidatos):
            linha = conn.execute(
                "SELECT bruto, erro, tentativas, lido_em FROM apropriacao_lancamentos_cc"
                " WHERE ncodmovcc = ?", (candidato,)).fetchone()
            if linha:
                break
        if linha:
            bruto, erro, tentativas, lido_em = linha
            guardada = {"erro": erro, "tentativas": tentativas,
                        "lido_em": str(lido_em) if lido_em else None,
                        "tem_obra": bool(bruto) and _tem_obra(bruto),
                        "resposta_do_omie": json.loads(bruto) if bruto else None}
    no_painel = [
        {"obra": dep, "projeto": proj, "valor": float(v or 0), "data": str(d)}
        for dep, proj, v, d in conn.execute(
            "SELECT departamento, projeto, pago_recebido, data FROM fato"
            " WHERE codigo_lancamento = ?", (int(codigo),)).fetchall()]
    if not movimentos:
        motivo = ("o painel não tem este número entre os movimentos de conta "
                  "guardados — ou é título, ou o período ainda não foi relido")
    elif guardada is None:
        motivo = ("a obra deste lançamento ainda NÃO foi perguntada ao OMIE — "
                  "use \"Ler as obras dos lançamentos de conta\"")
    elif guardada["resposta_do_omie"] is None:
        motivo = (f"o OMIE recusou a consulta {guardada['tentativas']} vez(es): "
                  f"{guardada['erro']}")
    elif not guardada["tem_obra"]:
        motivo = ("o OMIE respondeu, e a resposta não traz obra que o painel "
                  "reconheça — veja \"resposta_do_omie\" abaixo")
    else:
        motivo = ("o OMIE respondeu com obra; se o painel ainda mostra sem obra, "
                  "falta refazer os números")
    return {"codigo": int(codigo), "diagnostico": motivo,
            "movimentos": movimentos, "apropriacao_guardada": guardada,
            "linhas_no_painel": no_painel}
