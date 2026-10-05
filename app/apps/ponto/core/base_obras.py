# -*- coding: utf-8 -*-
"""
A BASE DE OBRAS DO PONTO: a aba "C. Diários" da planilha "Bases de Dados Pipefy".

Pedido do dono, 05/10/2026: *"Quero utilizar temporariamente as obras de
C. Diários. Depois vamos usar o cadastro do ERP. (…) Na coluna V de C. Diários
tem o status da obra. Não exiba obra que estão como 'Concluída', 'Concluída com
Dívida' ou 'Distratada'. (…) O cadastro das obras precisa estar sempre
atualizado."* E, minutos depois: *"Coluna AM, o cabeçalho é 'Coordenadas
Geográficas'."*

O DESENHO É O MESMO DA BASE DE PESSOAS (`registro.py`): a planilha é copiada
para uma tabela do ponto (`ponto.obras_planilha`) e o ponto lê a cópia por cima
do cadastro do ERP. A obra continua sendo uma linha de `public.obras` — é nela
que batidas, aparelhos e cercas se penduram. Obra que está ativa na planilha e
não existe no ERP é criada lá com o mínimo (código e nome), a mesma escrita que
o importador já fazia.

O QUE A PLANILHA MANDA, enquanto ela for a base:
  · QUAIS OBRAS APARECEM: só as que estão na planilha e cujo status (coluna V)
    não começa por "Conclu" nem por "Distrat" — cobre "Concluída", "Concluída
    com Dívida" e "Distratada", com ou sem acento. Obra do ERP que não está na
    planilha também some da lista de obras do ponto.
  · A COORDENADA: a da coluna "Coordenadas Geográficas" (AM) vence a do ERP; sem
    ela, vale a do ERP; sem nenhuma, a obra não tem cerca (não bloqueia, e a
    tela da cerca acusa).
  · O NOME, quando preenchido.
O raio e o que fazer fora da cerca continuam no ponto (Configuração › Cerca).

O FORMATO DA COORDENADA (`ler_coordenada`): latitude e longitude em graus
decimais, separadas por vírgula — exatamente o que o Google Maps copia quando se
clica com o botão direito no local: `-3.731862, -38.526670`. Também se aceita
vírgula decimal com ponto e vírgula entre os dois (`-3,731862; -38,526670`), o
link do Google Maps e graus-minutos-segundos (`3°43'54.7"S 38°31'36.0"W`).
Recusado, com o motivo escrito na tela: menos de 4 casas decimais (o erro passa
de 10 m e a cerca fica errada), fora do Brasil, longitude sem o sinal de menos.
Latitude e longitude trocadas são desvirada sozinhas, com aviso.

ATUALIZAÇÃO CONTÍNUA (`manter_em_dia`): de 2 em 2 horas, das 6h às 20h, a aba é
lida de novo (uma faixa de algumas centenas de linhas, segundos). Há o botão
"Ler a planilha agora" na Configuração e a chave para voltar ao ERP.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import unicodedata
from decimal import Decimal
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from . import parametros

logger = logging.getLogger("ponto.base_obras")

PARAMETRO_FONTE = "obras.fonte"
PARAMETRO_AUTOMATICO = "obras.automatico"
PARAMETRO_ULTIMA = "obras.ultima_leitura"
FONTE_PLANILHA, FONTE_ERP = "PLANILHA", "ERP"
FONTE_PADRAO = FONTE_PLANILHA

# A planilha "Bases de Dados Pipefy" — a mesma que o painel lê para o de-para de
# projetos (PAINEL_SHEET_PROJETOS) e a emissão de NFS-e lê para a tributação.
PLANILHA_PADRAO = "1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk"
ABAS = ("C. Diários", "C. Diarios", "C.Diários", "Centro de Custo")
FAIXA = "A1:AZ3000"

# Colunas. O status é pela POSIÇÃO (o dono disse "coluna V"); a coordenada, pelo
# nome que ele informou, com a posição AM de reserva.
COL_STATUS = 21                 # V
COL_COORDENADA = 38             # AM
NOMES_COORDENADA = ("Coordenadas Geográficas", "Coordenadas Geograficas", "Coordenadas",
                    "Coordenada", "Localização", "Localizacao")
NOMES_CODIGO = ("Código Primário", "Codigo Primario")
NOMES_NOME = ("Nome da Obra", "Obra", "Centro de Custo", "Objeto")
NOMES_MUNICIPIO = ("Município", "Municipio")
ENCERRADA_COMECA_COM = ("conclu", "distrat")

CASAS_MINIMAS = 4               # 4 casas ≈ 11 m; menos que isso a cerca erra feio
# O Brasil, com folga: latitude de +6 a −34, longitude de −28 a −74.
LAT_BR, LON_BR = (-34.5, 6.0), (-74.5, -28.0)

HORAS_ENTRE_LEITURAS = 2
JANELA = (6, 20)
CACHE_S = 30
_cache: dict = {"em": 0.0, "estado": None}


def _sem_acento(texto) -> str:
    t = unicodedata.normalize("NFKD", str(texto or ""))
    return " ".join("".join(c for c in t if not unicodedata.combining(c)).lower().split())


def encerrada(status) -> bool:
    """PURA. 'Concluída', 'Concluída com Dívida', 'Distratada' (e variações)."""
    return _sem_acento(status).startswith(ENCERRADA_COMECA_COM)


# ---------------------------------------------------------------------------
# A coordenada
# ---------------------------------------------------------------------------
_DMS = re.compile(r"(\d{1,3})\s*[°º]\s*(\d{1,2})\s*['’′]\s*(\d{1,2}(?:[.,]\d+)?)\s*(?:\"|''|”|″)?\s*([NSLOEW])",
                  re.IGNORECASE)
_NUMERO = re.compile(r"[-+]?\d{1,3}(?:[.,]\d+)?")


def _casas(numero: str) -> int:
    partes = re.split(r"[.,]", numero)
    return len(partes[1]) if len(partes) > 1 else 0


def ler_coordenada(texto) -> dict:
    """PURA. {'latitude', 'longitude', 'aviso', 'problema'}. Vazio → tudo None."""
    vazio = {"latitude": None, "longitude": None, "aviso": None, "problema": None}
    bruto = " ".join(str(texto or "").split())
    if not bruto:
        return vazio

    dms = _DMS.findall(bruto)
    if len(dms) == 2:
        valores = []
        for g, m, s, h in dms:
            v = int(g) + int(m) / 60 + float(s.replace(",", ".")) / 3600
            valores.append((-v if h.upper() in ("S", "O", "W") else v, h.upper()))
        (a, ha), (b, hb) = valores
        lat, lon = (a, b) if ha in ("N", "S") else (b, a)
        numeros_txt = None
    else:
        alvo = bruto
        achado = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", bruto) or \
            re.search(r"[?&](?:q|ll|query)=(-?\d+\.\d+)(?:,|%2C)\s*(-?\d+\.\d+)", bruto)
        if achado:
            alvo = f"{achado.group(1)}, {achado.group(2)}"
        numeros_txt = _NUMERO.findall(alvo)
        if len(numeros_txt) != 2:
            return {**vazio, "problema": "não reconheci dois números (latitude, longitude) — "
                                         "cole como o Google Maps copia: -3.731862, -38.526670"}
        if min(_casas(n) for n in numeros_txt) < CASAS_MINIMAS:
            return {**vazio, "problema": f"poucas casas decimais (use pelo menos {CASAS_MINIMAS}; "
                                         "o Google Maps dá 6) — com menos, a cerca erra dezenas de metros"}
        lat, lon = (float(n.replace(",", ".")) for n in numeros_txt)

    aviso = None
    no_br = lambda la, lo: LAT_BR[0] <= la <= LAT_BR[1] and LON_BR[0] <= lo <= LON_BR[1]  # noqa: E731
    if not no_br(lat, lon):
        if no_br(lon, lat):
            lat, lon = lon, lat
            aviso = "latitude e longitude estavam trocadas — desvirei"
        elif LON_BR[0] <= -lon <= LON_BR[1] and LAT_BR[0] <= lat <= LAT_BR[1]:
            return {**vazio, "problema": "a longitude está sem o sinal de menos (no Brasil ela é negativa)"}
        else:
            return {**vazio, "problema": "a coordenada cai fora do Brasil — confira os números"}
    return {"latitude": Decimal(f"{lat:.6f}"), "longitude": Decimal(f"{lon:.6f}"),
            "aviso": aviso, "problema": None}


def formatar(latitude, longitude) -> str:
    """O texto padrão para escrever na coluna AM."""
    return f"{float(latitude):.6f}, {float(longitude):.6f}"


# ---------------------------------------------------------------------------
# A aba, interpretada (PURA)
# ---------------------------------------------------------------------------
def _letra(i: int) -> str:
    n, s = i + 1, ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _achar(cabecalho_norm: list[str], nomes) -> Optional[int]:
    for nome in nomes:
        alvo = _sem_acento(nome)
        if alvo in cabecalho_norm:
            return cabecalho_norm.index(alvo)
    return None


def interpretar(valores: list[list]) -> dict:
    """Linhas da aba → {'obras': [...], 'problemas': [...], 'colunas': {...}}."""
    if not valores:
        raise ValueError('a aba "C. Diários" veio vazia')
    cabecalho = [str(c or "").strip() for c in valores[0]]
    norm = [_sem_acento(c) for c in cabecalho]
    i_codigo = _achar(norm, NOMES_CODIGO)
    i_nome = _achar(norm, NOMES_NOME)
    i_mun = _achar(norm, NOMES_MUNICIPIO)
    i_coord = _achar(norm, NOMES_COORDENADA)
    i_coord = COL_COORDENADA if i_coord is None else i_coord
    i_status = COL_STATUS

    def cel(linha, i):
        return str(linha[i]).strip() if i is not None and i < len(linha) and linha[i] is not None else ""

    colunas = {
        "status": {"letra": _letra(i_status), "titulo": cel(cabecalho, i_status)},
        "coordenada": {"letra": _letra(i_coord), "titulo": cel(cabecalho, i_coord)},
        "codigo": {"letra": _letra(i_codigo) if i_codigo is not None else "A",
                   "titulo": cel(cabecalho, i_codigo) if i_codigo is not None else cel(cabecalho, 0)},
        "nome": {"letra": _letra(i_nome) if i_nome is not None else None,
                 "titulo": cel(cabecalho, i_nome) if i_nome is not None else None},
    }
    obras, problemas, vistos = [], [], set()
    for n, linha in enumerate(valores[1:], start=2):
        primario = cel(linha, i_codigo)
        secundario = cel(linha, 0)
        codigo = primario or secundario
        if not codigo:
            continue
        chave = codigo.upper()
        if chave in vistos:
            problemas.append({"linha": n, "codigo": codigo, "problema": "código repetido — valeu a primeira linha"})
            continue
        vistos.add(chave)
        status = cel(linha, i_status)
        coord_txt = cel(linha, i_coord)
        c = ler_coordenada(coord_txt)
        obra = {
            "linha": n, "codigo": codigo,
            "codigo_secundario": secundario if secundario and secundario.upper() != chave else None,
            "nome": cel(linha, i_nome) or codigo, "municipio": cel(linha, i_mun) or None,
            "status_planilha": status or None, "encerrada": encerrada(status),
            "latitude": c["latitude"], "longitude": c["longitude"],
            "coordenada_texto": coord_txt or None, "coordenada_problema": c["problema"],
            "coordenada_aviso": c["aviso"],
        }
        obras.append(obra)
        if c["problema"] and not obra["encerrada"]:
            problemas.append({"linha": n, "codigo": codigo, "problema": f"coordenada: {c['problema']}"})
    return {"obras": obras, "problemas": problemas, "colunas": colunas}


# ---------------------------------------------------------------------------
# Ler a planilha
# ---------------------------------------------------------------------------
def id_da_planilha() -> str:
    return (os.getenv("PAINEL_SHEET_PROJETOS") or "").strip() or PLANILHA_PADRAO


def ler_planilha() -> list[list]:
    """A faixa A:AZ da aba "C. Diários". Levanta RuntimeError com o motivo."""
    import base64
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    if not b64:
        raise RuntimeError("GOOGLE_CREDENTIALS_BASE64 não configurado")
    info = json.loads(base64.b64decode(b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    servico = build("sheets", "v4", credentials=creds, cache_discovery=False)
    motivos = []
    for aba in ABAS:
        try:
            resp = servico.spreadsheets().values().get(
                spreadsheetId=id_da_planilha(), range=f"'{aba}'!{FAIXA}",
                valueRenderOption="FORMATTED_VALUE").execute()
            return resp.get("values", [])
        except Exception as e:  # noqa: BLE001 — tenta o próximo nome de aba
            motivos.append(f"{aba}: {str(e)[:160]}")
    raise RuntimeError('não consegui ler a aba "C. Diários" — ' + " / ".join(motivos))


# ---------------------------------------------------------------------------
# Gravar a cópia
# ---------------------------------------------------------------------------
def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "obras_planilha", "obra_id")


def gravar(conn: Connection, lido: dict, por: str) -> dict:
    """Refaz a cópia e liga cada obra à linha do ERP (criando a que falta)."""
    from . import cadastros
    erp = db.todos(conn, "SELECT id, upper(btrim(codigo)) AS codigo FROM public.obras")
    por_codigo = {o["codigo"]: int(o["id"]) for o in erp if o["codigo"]}
    usados: set[int] = set()
    criadas, problemas = [], list(lido["problemas"])
    for o in lido["obras"]:
        obra_id = por_codigo.get(o["codigo"].upper())
        if obra_id is None and o["codigo_secundario"]:
            obra_id = por_codigo.get(o["codigo_secundario"].upper())
        if obra_id is None and not o["encerrada"]:
            obra_id = cadastros.criar_obra_no_erp(conn, codigo=o["codigo"], nome=o["nome"])
            por_codigo[o["codigo"].upper()] = obra_id
            criadas.append(f"{o['codigo']} — {o['nome']}")
        if obra_id is not None and obra_id in usados:
            problemas.append({"linha": o["linha"], "codigo": o["codigo"],
                              "problema": "duas linhas da planilha caem na mesma obra do ERP — valeu a primeira"})
            obra_id = None
        if obra_id is not None:
            usados.add(obra_id)
        o["obra_id"] = obra_id
    db.executar(conn, "DELETE FROM ponto.obras_planilha")
    for o in lido["obras"]:
        db.executar(conn, """
            INSERT INTO ponto.obras_planilha (codigo, codigo_secundario, nome, municipio, status_planilha,
                   encerrada, latitude, longitude, coordenada_texto, coordenada_problema, coordenada_aviso,
                   obra_id, linha)
            VALUES (:codigo, :codigo_secundario, :nome, :municipio, :status_planilha, :encerrada,
                    :latitude, :longitude, :coordenada_texto, :coordenada_problema, :coordenada_aviso,
                    :obra_id, :linha)""", **{k: o[k] for k in (
            "codigo", "codigo_secundario", "nome", "municipio", "status_planilha", "encerrada", "latitude",
            "longitude", "coordenada_texto", "coordenada_problema", "coordenada_aviso", "obra_id", "linha")})
    from .. import horario
    resultado = {
        "em": horario.agora().isoformat(), "por": por, "ok": True,
        "lidas": len(lido["obras"]), "ativas": sum(1 for o in lido["obras"] if not o["encerrada"]),
        "encerradas": sum(1 for o in lido["obras"] if o["encerrada"]),
        "criadas_no_erp": criadas[:50], "quantidade_criadas": len(criadas),
        "problemas": problemas[:80], "quantidade_problemas": len(problemas), "colunas": lido["colunas"],
    }
    parametros.gravar(conn, PARAMETRO_ULTIMA, json.dumps(resultado, ensure_ascii=False, default=str), por)
    esquecer()
    logger.info("Ponto: C. Diários lida por %s — %d obras (%d ativas, %d criadas no ERP, %d problemas)",
                por, resultado["lidas"], resultado["ativas"], len(criadas), len(problemas))
    return resultado


def atualizar(por: str) -> dict:
    """Lê a planilha e grava. A falha fica anotada (a tela mostra), sem levantar."""
    from .. import horario
    try:
        lido = interpretar(ler_planilha())
    except Exception as e:  # noqa: BLE001
        logger.warning("Ponto: não consegui ler a C. Diários: %s", e)
        with db.conexao() as conn:
            anterior = ultima_leitura(conn) or {}
            parametros.gravar(conn, PARAMETRO_ULTIMA, json.dumps(
                {**anterior, "falha_em": horario.agora().isoformat(), "falha": str(e)[:400]},
                ensure_ascii=False, default=str), por)
        return {"ok": False, "erro": str(e)[:400]}
    with db.conexao() as conn:
        if not disponivel(conn):
            return {"ok": False, "erro": "aplique as atualizações do ponto (migração 004) antes"}
        return gravar(conn, lido, por)


def ultima_leitura(conn: Connection) -> Optional[dict]:
    if not db.tem_coluna(conn, "parametros", "valor"):
        return None
    try:
        return json.loads(parametros.ler(conn, PARAMETRO_ULTIMA, "") or "null")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Usar a cópia
# ---------------------------------------------------------------------------
def estado(conn: Connection, *, fresco: bool = False) -> dict:
    """{'disponivel', 'fonte', 'linhas', 'usar'}. Usa só com a tabela criada E
    com pelo menos uma obra lida: planilha nunca lida não esvazia a lista."""
    agora = time.time()
    if fresco or _cache["estado"] is None or agora - _cache["em"] > CACHE_S:
        tem = disponivel(conn)
        fonte = ((parametros.ler(conn, PARAMETRO_FONTE, FONTE_PADRAO) or FONTE_PADRAO)
                 if db.tem_coluna(conn, "parametros", "valor") else FONTE_PADRAO)
        linhas = int(db.um(conn, "SELECT count(*) AS n FROM ponto.obras_planilha")["n"]) if tem else 0
        _cache["estado"] = {"disponivel": tem, "fonte": fonte, "linhas": linhas,
                            "usar": tem and linhas > 0 and fonte == FONTE_PLANILHA}
        _cache["em"] = agora
    return dict(_cache["estado"])


def esquecer() -> None:
    _cache["estado"] = None


def usando(conn: Connection) -> bool:
    return estado(conn)["usar"]


def gravar_fonte(conn: Connection, fonte: str, por: str) -> None:
    from ..erros import ErroDeValidacao
    fonte = str(fonte or "").upper()
    if fonte not in (FONTE_PLANILHA, FONTE_ERP):
        raise ErroDeValidacao("use PLANILHA ou ERP", campo="fonte")
    parametros.gravar(conn, PARAMETRO_FONTE, fonte, por)
    esquecer()


def retrato(conn: Connection) -> dict:
    e = estado(conn, fresco=True)
    saida = {**e, "automatico": automatico(conn), "ultima": ultima_leitura(conn),
             "planilha": id_da_planilha()[:6] + "…",
             "ritmo": {"horas": HORAS_ENTRE_LEITURAS, "janela": list(JANELA)}}
    if not e["disponivel"]:
        return saida
    n = db.um(conn, """
        SELECT count(*) FILTER (WHERE NOT encerrada) AS ativas,
               count(*) FILTER (WHERE encerrada) AS encerradas,
               count(*) FILTER (WHERE NOT encerrada AND latitude IS NOT NULL) AS com_coordenada,
               count(*) FILTER (WHERE NOT encerrada AND latitude IS NULL AND coordenada_problema IS NULL) AS sem_coordenada,
               count(*) FILTER (WHERE NOT encerrada AND coordenada_problema IS NOT NULL) AS coordenada_ruim,
               count(*) FILTER (WHERE NOT encerrada AND status_planilha IS NULL) AS sem_status
          FROM ponto.obras_planilha""")
    fora = db.um(conn, """SELECT count(*) AS n FROM public.obras o WHERE o.status = 'ATIVA'
                            AND NOT EXISTS (SELECT 1 FROM ponto.obras_planilha p WHERE p.obra_id = o.id)""")
    saida.update({k: int(v or 0) for k, v in n.items()})
    saida["ativas_no_erp_fora_da_planilha"] = int(fora["n"])
    saida["sem_coordenada_lista"] = [l["codigo"] for l in db.todos(conn, """
        SELECT codigo FROM ponto.obras_planilha WHERE NOT encerrada AND latitude IS NULL
         AND coordenada_problema IS NULL ORDER BY codigo LIMIT 60""")]
    saida["coordenada_ruim_lista"] = db.todos(conn, """
        SELECT codigo, linha, coordenada_texto AS texto, coordenada_problema AS problema
          FROM ponto.obras_planilha WHERE NOT encerrada AND coordenada_problema IS NOT NULL
         ORDER BY linha LIMIT 60""")
    saida["status_vistos"] = db.todos(conn, """
        SELECT coalesce(status_planilha, '(vazio)') AS status, encerrada, count(*) AS n
          FROM ponto.obras_planilha GROUP BY 1, 2 ORDER BY 3 DESC""")
    return saida


def trechos_sql(conn: Connection) -> Optional[dict]:
    """Os pedaços do SELECT de obra que trocam o ERP pela planilha."""
    if not usando(conn):
        return None
    return {
        "join": "LEFT JOIN ponto.obras_planilha op ON op.obra_id = o.id",
        "nome": "COALESCE(NULLIF(btrim(op.nome), ''), o.nome)",
        "status": ("CASE WHEN op.obra_id IS NULL THEN 'FORA_DA_PLANILHA' "
                   "WHEN op.encerrada THEN 'ENCERRADA' ELSE 'ATIVA' END"),
        "latitude": "COALESCE(op.latitude, o.latitude)",
        "longitude": "COALESCE(op.longitude, o.longitude)",
        "origem_coordenada": ("CASE WHEN op.latitude IS NOT NULL THEN 'PLANILHA' "
                              "WHEN o.latitude IS NOT NULL THEN 'ERP' END"),
        "status_planilha": "op.status_planilha",
    }


def condicao_ativa(conn: Connection, alias: str = "o") -> str:
    """O filtro "obra ativa" para quem consulta `public.obras` direto."""
    if usando(conn):
        return (f"EXISTS (SELECT 1 FROM ponto.obras_planilha op_ WHERE op_.obra_id = {alias}.id "
                "AND NOT op_.encerrada)")
    return f"{alias}.status = 'ATIVA'"


def obra_pelo_codigo_sql(conn: Connection, coluna: str) -> Optional[str]:
    """Subconsulta: o código de outra base (o do Registro de Colaboradores) →
    a obra, pelos dois códigos da planilha. None quando a planilha não está em uso."""
    if not usando(conn):
        return None
    return (f"(SELECT op_.obra_id FROM ponto.obras_planilha op_ WHERE {coluna} <> '' AND op_.obra_id IS NOT NULL "
            f"AND (upper(btrim(op_.codigo)) = upper(btrim({coluna})) "
            f"OR upper(btrim(op_.codigo_secundario)) = upper(btrim({coluna}))) "
            f"ORDER BY (upper(btrim(op_.codigo)) = upper(btrim({coluna}))) DESC LIMIT 1)")


# ---------------------------------------------------------------------------
# Em dia sozinha
# ---------------------------------------------------------------------------
_trabalhando = threading.Lock()
_ultima_olhada = 0.0
MINUTOS_ENTRE_OLHADAS = 15


def automatico(conn: Connection) -> bool:
    if not db.tem_coluna(conn, "parametros", "valor"):
        return False
    return parametros.ler(conn, PARAMETRO_AUTOMATICO, "1") == "1"


def precisa_ler(ultima: Optional[dict], agora) -> bool:
    """PURA. Passou o bastante desde a última leitura (ou tentativa)?"""
    import datetime as dt
    from .. import horario
    local = horario.para_local(agora)
    if not (JANELA[0] <= local.hour < JANELA[1]):
        return False
    if not ultima:
        return True
    marcas = [ultima.get("em"), ultima.get("falha_em")]
    datas = []
    for m in marcas:
        if m:
            try:
                d = dt.datetime.fromisoformat(m)
                datas.append(d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc))
            except ValueError:
                pass
    if not datas:
        return True
    return (agora - max(datas)).total_seconds() >= HORAS_ENTRE_LEITURAS * 3600


def _trabalhar() -> None:
    from .. import horario
    try:
        with db.conexao() as conn:
            if not disponivel(conn) or not automatico(conn) or \
                    estado(conn, fresco=True)["fonte"] != FONTE_PLANILHA:
                return
            if not precisa_ler(ultima_leitura(conn), horario.agora()):
                return
        atualizar("o próprio ponto (base de obras em dia)")
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: manter a base de obras em dia falhou; tenta de novo depois")
    finally:
        _trabalhando.release()


def manter_em_dia() -> bool:
    """Barata em toda requisição: no máximo uma olhada a cada 15 min, numa linha separada."""
    global _ultima_olhada
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return False
    agora = time.time()
    if agora - _ultima_olhada < MINUTOS_ENTRE_OLHADAS * 60:
        return False
    _ultima_olhada = agora
    if not _trabalhando.acquire(blocking=False):
        return False
    threading.Thread(target=_trabalhar, name="ponto-base-obras", daemon=True).start()
    return True
