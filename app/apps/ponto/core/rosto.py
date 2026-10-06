# -*- coding: utf-8 -*-
"""
A CONFERÊNCIA DO ROSTO — a foto da batida contra a foto cadastral da pessoa, pelo
Amazon Rekognition (CompareFaces).

Pedido do dono, 06/10/2026: *"acho que devemos ativar a análise via AWS. Hoje
pago 2 mil reais pelo ponto que usamos. Vamos reduzir (…). Devemos permitir
colocar padrão para tudo, excluir alguma obra ou algum horário ou dia caso
entenda que não precise ser 100% das fotos."*

O QUE ELA FAZ, de pouco em pouco numa linha separada (acordada pelas próprias
requisições do ponto, como a base de pessoas):
  · pega as batidas com foto ainda não conferidas, que passam pelas regras
    (obras, dias e horários excluídos; "só a primeira batida do dia");
  · compara com a foto cadastral (a escolhida no mosaico). Quem não tem foto
    cadastral ganha a da primeira batida com foto boa — marcada "automática,
    confira no mosaico", e essa batida não é cobrada;
  · OUTRA PESSOA (semelhança abaixo do mínimo) ou SEM ROSTO → a batida vai para
    conferência ("rosto não confere") e abre o alerta urgente;
  · o TETO do mês: passou, para até o mês seguinte (e diz na tela).

CUSTO: CompareFaces cobra por imagem (US$ 0,001 no primeiro milhão do mês,
preço público de 06/10/2026; 1.000 por mês grátis no primeiro ano da conta). O
custo de cada conferência fica gravado, e a tela soma o mês.

O QUE PRECISA: as chaves AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY no Render (a
conta é do dono; as chaves nunca entram no chat), a biblioteca boto3 e a chave
"Conferir o rosto" ligada na Configuração. Sem qualquer um dos três, o ponto
segue igual — nada quebra.

⚠️ DADO BIOMÉTRICO (LGPD art. 11): o rosto é dado sensível, e a foto sai do
Brasil para a AWS (região configurável). A AWS pode usar o conteúdo para
melhorar os serviços de IA dela, a menos que a conta desligue isso (política de
"opt-out" dos serviços de IA da AWS Organizations) — fazer ao criar a conta.
Avisar os funcionários no termo do ponto é obrigação da empresa — HISTORICO.md.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import time
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import parametros

logger = logging.getLogger("ponto.rosto")

PARAMETRO = "rosto.config"
PRECO_POR_FOTO_USD = 0.001
PADRAO = {
    "ligado": False,              # nasce desligada: liga quem configura, depois das chaves
    "teto_mensal_usd": 10.0,      # ≈ R$ 55: o freio, não a meta
    "semelhanca_minima": 90,      # % — abaixo disso, "outra pessoa"
    "so_primeira_do_dia": False,  # 1 de cada 4 batidas: corta o custo em ~75 %
    "obras_excluidas": [],        # ids das obras que não conferem
    "dias_excluidos": [],         # 0 = segunda … 6 = domingo
    "horarios_excluidos": [],     # [{"de": "11:00", "ate": "13:30"}]
    "regiao": "us-east-1",
}
LOTE = 60                         # por rodada
MINUTOS_ENTRE_RODADAS = 10
DIAS_PARA_TRAS = 3                # não confere batida mais velha que isso


# ---------------------------------------------------------------------------
# A configuração
# ---------------------------------------------------------------------------
def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "conferencias_rosto", "marcacao_id")


def ler(conn: Connection) -> dict:
    cfg = dict(PADRAO)
    if not db.tem_coluna(conn, "parametros", "valor"):
        return cfg
    try:
        cfg.update(json.loads(parametros.ler(conn, PARAMETRO, "") or "{}"))
    except ValueError:
        logger.warning("Ponto: rosto.config ilegível; valendo o padrão")
    return cfg


def _hora(texto: str, campo: str) -> str:
    try:
        return dt.time.fromisoformat(str(texto).strip()[:5]).strftime("%H:%M")
    except ValueError:
        raise ErroDeValidacao("horário ilegível (HH:MM)", campo=campo)


def gravar(conn: Connection, novos: dict, por: str) -> dict:
    cfg = ler(conn)
    if "ligado" in novos:
        cfg["ligado"] = bool(novos["ligado"])
    if "teto_mensal_usd" in novos:
        try:
            teto = float(str(novos["teto_mensal_usd"]).replace(",", "."))
        except ValueError:
            raise ErroDeValidacao("teto em dólares, só números", campo="teto_mensal_usd")
        if not 0 <= teto <= 500:
            raise ErroDeValidacao("teto entre US$ 0 e US$ 500 por mês", campo="teto_mensal_usd")
        cfg["teto_mensal_usd"] = round(teto, 3)
    if "semelhanca_minima" in novos:
        n = int(novos["semelhanca_minima"])
        if not 70 <= n <= 99:
            raise ErroDeValidacao("semelhança mínima entre 70 % e 99 %", campo="semelhanca_minima")
        cfg["semelhanca_minima"] = n
    if "so_primeira_do_dia" in novos:
        cfg["so_primeira_do_dia"] = bool(novos["so_primeira_do_dia"])
    if "obras_excluidas" in novos:
        cfg["obras_excluidas"] = sorted({int(x) for x in novos["obras_excluidas"] or []})
    if "dias_excluidos" in novos:
        dias = sorted({int(x) for x in novos["dias_excluidos"] or []})
        if any(d not in range(7) for d in dias):
            raise ErroDeValidacao("dia da semana de 0 (segunda) a 6 (domingo)", campo="dias_excluidos")
        cfg["dias_excluidos"] = dias
    if "horarios_excluidos" in novos:
        faixas = []
        for f in novos["horarios_excluidos"] or []:
            de, ate = _hora(f.get("de"), "de"), _hora(f.get("ate"), "ate")
            faixas.append({"de": de, "ate": ate})
        cfg["horarios_excluidos"] = faixas
    if "regiao" in novos:
        cfg["regiao"] = "".join(ch for ch in str(novos["regiao"]) if ch.isalnum() or ch == "-")[:20] or PADRAO["regiao"]
    parametros.gravar(conn, PARAMETRO, json.dumps(cfg, ensure_ascii=False), por)
    logger.info("Ponto: conferência do rosto configurada por %s — %s", por, cfg)
    return cfg


def entra_na_regra(cfg: dict, *, obra_id: int, momento_local: dt.datetime, primeira_do_dia: bool) -> bool:
    """PURA. A batida passa pelas exclusões?"""
    if int(obra_id) in set(cfg.get("obras_excluidas") or []):
        return False
    if momento_local.weekday() in set(cfg.get("dias_excluidos") or []):
        return False
    hm = momento_local.strftime("%H:%M")
    for f in cfg.get("horarios_excluidos") or []:
        de, ate = f["de"], f["ate"]
        if (de <= hm < ate) if de <= ate else (hm >= de or hm < ate):   # faixa que vira a noite
            return False
    if cfg.get("so_primeira_do_dia") and not primeira_do_dia:
        return False
    return True


def credenciais_ok() -> bool:
    return bool(os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"))


def biblioteca_ok() -> bool:
    try:
        import boto3  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# O gasto
# ---------------------------------------------------------------------------
def gasto_do_mes(conn: Connection, hoje: Optional[dt.date] = None) -> dict:
    hoje = hoje or horario.hoje()
    inicio = hoje.replace(day=1)
    linha = db.um(conn, """
        SELECT count(*) FILTER (WHERE custo_usd > 0) AS cobradas, coalesce(sum(custo_usd), 0) AS custo,
               count(*) FILTER (WHERE resultado IN ('OUTRA_PESSOA', 'SEM_ROSTO')) AS suspeitas,
               count(*) AS conferidas
          FROM ponto.conferencias_rosto
         WHERE (conferido_em AT TIME ZONE 'America/Fortaleza')::date >= :i""", i=inicio)
    return {"conferidas": int(linha["conferidas"]), "cobradas": int(linha["cobradas"]),
            "custo_usd": float(linha["custo"]), "suspeitas": int(linha["suspeitas"])}


def estimativa(conn: Connection, cfg: dict) -> dict:
    """Quanto custaria o mês com a regra de hoje, pelas batidas dos últimos 30 dias."""
    linhas = db.todos(conn, """
        SELECT m.obra_id, m.timestamp_servidor, m.colaborador_id, m.data_referencia,
               row_number() OVER (PARTITION BY m.colaborador_id, m.data_referencia ORDER BY m.timestamp_servidor) AS ordem
          FROM ponto.marcacoes m
         WHERE m.foto_id IS NOT NULL AND m.timestamp_servidor > now() - interval '30 days'""")
    entram = sum(1 for l in linhas if entra_na_regra(
        cfg, obra_id=l["obra_id"], momento_local=horario.para_local(l["timestamp_servidor"]),
        primeira_do_dia=int(l["ordem"]) == 1))
    return {"fotos_30_dias": len(linhas), "entrariam": entram,
            "custo_estimado_usd": round(entram * PRECO_POR_FOTO_USD, 2)}


# ---------------------------------------------------------------------------
# A comparação
# ---------------------------------------------------------------------------
def _cliente(regiao: str):
    import boto3
    return boto3.client("rekognition", region_name=regiao)


def comparar(cliente, cadastral: bytes, batida: bytes, minima: int) -> tuple[str, Optional[float], str]:
    """(resultado, semelhança, detalhe). CompareFaces: a cadastral é a origem."""
    try:
        r = cliente.compare_faces(SourceImage={"Bytes": cadastral}, TargetImage={"Bytes": batida},
                                  SimilarityThreshold=0)
    except Exception as e:  # noqa: BLE001
        nome = type(e).__name__
        texto = str(e)
        if "InvalidParameterException" in nome or "InvalidParameter" in texto:
            # A AWS responde assim quando não acha rosto numa das duas fotos.
            return "SEM_ROSTO", None, "nenhum rosto encontrado na foto"
        raise
    achados = r.get("FaceMatches") or []
    melhor = max((float(f.get("Similarity") or 0) for f in achados), default=0.0)
    if not achados and not (r.get("UnmatchedFaces") or []):
        return "SEM_ROSTO", None, "nenhum rosto encontrado na foto"
    if melhor >= minima:
        return "MESMA_PESSOA", round(melhor, 2), ""
    return "OUTRA_PESSOA", round(melhor, 2), f"semelhança de {melhor:.0f} % (mínimo {minima} %)"


def _pendentes(conn: Connection, cfg: dict, limite: int) -> list[dict]:
    linhas = db.todos(conn, """
        SELECT m.id, m.colaborador_id, m.obra_id, m.foto_id, m.status, m.timestamp_servidor, m.motivo_analise,
               m.data_referencia, pc.foto_cadastral_id, fo.luminancia, fo.contraste,
               (SELECT count(*) FROM ponto.marcacoes m2 WHERE m2.colaborador_id = m.colaborador_id
                  AND m2.data_referencia = m.data_referencia AND m2.timestamp_servidor < m.timestamp_servidor) AS antes
          FROM ponto.marcacoes m
          LEFT JOIN ponto.colaborador_config pc ON pc.colaborador_id = m.colaborador_id
          LEFT JOIN ponto.fotos fo ON fo.id = m.foto_id
         WHERE m.foto_id IS NOT NULL AND m.status <> 'REJEITADA'
           AND m.timestamp_servidor > now() - make_interval(days => :d)
           AND NOT EXISTS (SELECT 1 FROM ponto.conferencias_rosto r WHERE r.marcacao_id = m.id)
         ORDER BY m.timestamp_servidor LIMIT :lim""", d=DIAS_PARA_TRAS, lim=limite * 5)
    saida = []
    for l in linhas:
        if entra_na_regra(cfg, obra_id=l["obra_id"], momento_local=horario.para_local(l["timestamp_servidor"]),
                          primeira_do_dia=int(l["antes"]) == 0):
            saida.append(l)
        if len(saida) >= limite:
            break
    return saida


def _gravar(conn: Connection, marcacao_id: int, resultado: str, semelhanca, custo: float, detalhe: str) -> None:
    db.executar(conn, """
        INSERT INTO ponto.conferencias_rosto (marcacao_id, resultado, semelhanca, custo_usd, detalhe)
        VALUES (:m, :r, :s, :c, :d) ON CONFLICT (marcacao_id) DO NOTHING""",
                m=marcacao_id, r=resultado, s=semelhanca, c=custo, d=(detalhe or "")[:300] or None)


def conferir(conn: Connection, *, cliente=None, limite: int = LOTE) -> dict:
    """Uma rodada. Devolve o que fez — e por que parou, se parou."""
    from . import fotos, mosaico
    cfg = ler(conn)
    feito = {"conferidas": 0, "suspeitas": 0, "viraram_cadastral": 0, "erros": 0, "parou": None}
    if not disponivel(conn):
        feito["parou"] = "aplique as atualizações do ponto (migração 007)"
        return feito
    if not cfg["ligado"]:
        feito["parou"] = "desligada"
        return feito
    if cliente is None:
        if not credenciais_ok():
            feito["parou"] = "faltam as chaves da AWS no Render"
            return feito
        if not biblioteca_ok():
            feito["parou"] = "falta a biblioteca da AWS (boto3)"
            return feito
        cliente = _cliente(cfg["regiao"])
    gasto = gasto_do_mes(conn)["custo_usd"]
    cadastrais: dict[int, int] = {}          # a cadastral definida NESTA rodada vale para as seguintes
    for m in _pendentes(conn, cfg, limite):
        cid = int(m["colaborador_id"])
        m["foto_cadastral_id"] = cadastrais.get(cid) or m["foto_cadastral_id"]
        if not m["foto_cadastral_id"]:
            # Sem foto cadastral, as fotos das batidas servem (pergunta do dono,
            # 06/10/2026: "podemos usar as fotos que vêm sendo batidas?"). Vira a
            # cadastral a PRIMEIRA FOTO BOA: batida aceita (não em conferência) e
            # foto que deixa ver (nem escura, nem lisa). As seguintes já são
            # comparadas com ela — se ela própria estiver errada, as outras não
            # conferem e a pessoa cai em conferência, onde o RH troca a foto.
            ruim = mosaico.sinais_da_foto(m.get("luminancia"), m.get("contraste"))
            if m["status"] != "VALIDA" or ruim:
                _gravar(conn, m["id"], "SEM_CADASTRAL", None, 0,
                        "sem foto cadastral; esta não serve para ser a cadastral"
                        + (f" (foto {', '.join(ruim)})" if ruim else " (batida em conferência)"))
                continue
            mosaico.definir_foto_cadastral(conn, cid, int(m["id"]))
            cadastrais[cid] = int(m["foto_id"])
            _gravar(conn, m["id"], "VIROU_CADASTRAL", None, 0, "foto cadastral automática — confira no mosaico")
            feito["viraram_cadastral"] += 1
            continue
        if m["foto_cadastral_id"] == m["foto_id"]:
            _gravar(conn, m["id"], "VIROU_CADASTRAL", None, 0, "é a própria foto cadastral")
            continue
        if gasto + PRECO_POR_FOTO_USD > float(cfg["teto_mensal_usd"]):
            feito["parou"] = f"teto do mês atingido (US$ {cfg['teto_mensal_usd']:.2f})"
            break
        try:
            cadastral, _ = fotos.baixar(conn, int(m["foto_cadastral_id"]))
            batida, _ = fotos.baixar(conn, int(m["foto_id"]))
        except LookupError as e:
            _gravar(conn, m["id"], "ERRO", None, 0, f"foto indisponível: {e}")
            feito["erros"] += 1
            continue
        try:
            resultado, semelhanca, detalhe = comparar(cliente, cadastral, batida, int(cfg["semelhanca_minima"]))
        except Exception as e:  # noqa: BLE001 — a AWS fora: para a rodada, tenta depois
            logger.warning("Ponto: conferência do rosto falhou: %s", e)
            feito["erros"] += 1
            feito["parou"] = f"a AWS respondeu com erro: {str(e)[:160]}"
            break
        _gravar(conn, m["id"], resultado, semelhanca, PRECO_POR_FOTO_USD, detalhe)
        gasto += PRECO_POR_FOTO_USD
        feito["conferidas"] += 1
        if resultado in ("OUTRA_PESSOA", "SEM_ROSTO"):
            feito["suspeitas"] += 1
            motivo = ("rosto não confere com a foto cadastral" if resultado == "OUTRA_PESSOA"
                      else "nenhum rosto na foto") + (f" ({detalhe})" if detalhe else "")
            db.executar(conn, """
                UPDATE ponto.marcacoes SET status = CASE WHEN status = 'VALIDA' THEN 'EM_ANALISE' ELSE status END,
                       motivo_analise = concat_ws('; ', NULLIF(motivo_analise, ''), :mot)
                 WHERE id = :id""", mot=motivo, id=m["id"])
    if feito["conferidas"] or feito["suspeitas"]:
        logger.info("Ponto: conferência do rosto — %s", feito)
    return feito


# ---------------------------------------------------------------------------
# De pouco em pouco, numa linha separada
# ---------------------------------------------------------------------------
_trabalhando = threading.Lock()
_ultima = 0.0


def _trabalhar() -> None:
    try:
        with db.conexao() as conn:
            conferir(conn)
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: a rodada da conferência do rosto falhou; tenta de novo depois")
    finally:
        _trabalhando.release()


def manter_em_dia() -> bool:
    global _ultima
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return False
    agora = time.time()
    if agora - _ultima < MINUTOS_ENTRE_RODADAS * 60:
        return False
    _ultima = agora
    if not credenciais_ok():
        return False
    if not _trabalhando.acquire(blocking=False):
        return False
    threading.Thread(target=_trabalhar, name="ponto-rosto", daemon=True).start()
    return True


def retrato(conn: Connection) -> dict:
    cfg = ler(conn)
    saida = {"disponivel": disponivel(conn), "config": cfg, "credenciais": credenciais_ok(),
             "biblioteca": biblioteca_ok(), "preco_por_foto_usd": PRECO_POR_FOTO_USD}
    if saida["disponivel"]:
        saida["mes"] = gasto_do_mes(conn)
        saida["estimativa"] = estimativa(conn, cfg)
        saida["sem_cadastral"] = int(db.um(conn, """
            SELECT count(DISTINCT m.colaborador_id) AS n FROM ponto.marcacoes m
              LEFT JOIN ponto.colaborador_config pc ON pc.colaborador_id = m.colaborador_id
             WHERE m.timestamp_servidor > now() - interval '30 days' AND pc.foto_cadastral_id IS NULL""")["n"])
    return saida
