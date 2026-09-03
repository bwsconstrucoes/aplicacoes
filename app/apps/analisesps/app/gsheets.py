# -*- coding: utf-8 -*-
"""
gsheets.py — Conector da aba SPsBD (planilha 1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA).

Fase offline: estes métodos NÃO são chamados; o app roda só do cache (seed CSV).
Fase online : ligar uma Service Account e chamar bootstrap()/sync_delta().

------------------------------------------------------------------------------------
ESTRATÉGIA DE ATUALIZAÇÃO INCREMENTAL (a resposta à sua pergunta)
------------------------------------------------------------------------------------
Como a SPsBD é base de dados escrita SÓ por automações (Python/Make/Apps Script via API),
nenhum gatilho onEdit/onChange dispara. Então NÃO instrumentamos os escritores.

Em vez disso, um RECONCILIADOR agendado no Apps Script (gatilho time-driven, de minuto
em minuto — ver apps_script_reconciliador.gs) faz o trabalho, independente de quem
escreveu:

  1) lê a SPsBD e calcula a "assinatura" de cada linha;
  2) compara com a assinatura do ciclo anterior (aba oculta _SyncHash);
  3) nas linhas que mudaram (ou novas), grava o timestamp na coluna V ("Carimbo").

Daí o Python só precisa:
  - sync_delta(): ler APENAS 2 colunas leves — ID (A) e Carimbo (V) — descobrir o que
    mudou e baixar EM LOTE só essas linhas (A:AL). Payload mínimo.
  - bootstrap(): carga completa inicial (uma vez).

O operador ativo vê a própria mudança na hora (edição otimista no cache, cache.editar_local)
e as mudanças dos outros entram a cada ciclo (~1–2 min), conforme você considerou aceitável.
"""
from __future__ import annotations

import os
import json
import base64
import logging
from datetime import datetime

import cache
from schema import COLS, ALL_KEYS, CARIMBO_KEY

logger = logging.getLogger(__name__)

PLANILHA = "1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA"
ABA = "SPsBD"
PRIMEIRA_LINHA_DADOS = 2  # linha 1 é cabeçalho

# Planilha de "Documentação Fiscal" por SP (aba Lançamentos: A = ID SP, B = Doc. Fiscal).
SP_FISCAL_ID = "1xMu76lEiiJFlCgNNXldraW2enIuHdZL0D5QTuhZAc0w"
SP_FISCAL_ABA = "Lançamentos"

# Última coluna real da linha (analise_ia = AL). O carimbo (V) fica no meio, então
# os ranges precisam cobrir a linha inteira A:AL — não parar no carimbo.
FIM_LETRA = max(COLS.values(), key=lambda c: c.idx).letter

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]  # leitura+escrita (Fase 2);
# para a Fase 1 (só leitura) basta compartilhar a planilha com a SA como Leitor.


def _caminho_credenciais():
    """
    Procura o JSON da Service Account nesta ordem:
      1) variável de ambiente SPSBD_CREDENCIAIS (caminho)
      2) credenciais.json na pasta do app   <- padrão pedido
      3) service_account.json na pasta do app
    """
    env = os.environ.get("SPSBD_CREDENCIAIS")
    if env and os.path.exists(env):
        return env
    aqui = os.path.dirname(__file__)
    for nome in ("credenciais.json", "service_account.json"):
        p = os.path.join(aqui, nome)
        if os.path.exists(p):
            return p
    return None


def _credenciais():
    """Lê a Service Account de credenciais.json (preferido) ou de GOOGLE_CREDENTIALS_BASE64."""
    caminho = _caminho_credenciais()
    if caminho:
        with open(caminho, encoding="utf-8") as f:
            return json.load(f)
    b64 = os.environ.get("GOOGLE_CREDENTIALS_BASE64")
    if b64:
        return json.loads(base64.b64decode(b64).decode("utf-8"))
    return None


def disponivel() -> bool:
    if _credenciais() is None:
        return False
    try:
        import gspread  # noqa
        return True
    except ImportError:
        return False


def _reforcar_sessao(gc):
    """Monta retry no nível de TRANSPORTE da sessão HTTP do gspread: reconecta
    sozinho em ConnectionError/timeout e em 5xx, com backoff. Retries de status só
    em métodos idempotentes (GET); POST ganha apenas retry de conexão (seguro, pois
    a falha ocorre antes de a requisição chegar ao servidor)."""
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        sess = (getattr(gc, "session", None)
                or getattr(getattr(gc, "http_client", None), "session", None))
        if sess is None:
            return gc
        retry = Retry(total=5, connect=5, read=4, status=3, backoff_factor=1.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      raise_on_status=False)
        ad = HTTPAdapter(max_retries=retry)
        sess.mount("https://", ad)
        sess.mount("http://", ad)
    except Exception:
        pass
    return gc


def _abrir():
    import gspread
    from google.oauth2.service_account import Credentials
    info = _credenciais()
    if info is None:
        raise RuntimeError("Service Account ausente (GOOGLE_CREDENTIALS_BASE64 ou service_account.json).")
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = _reforcar_sessao(gspread.authorize(creds))
    return gc.open_by_key(PLANILHA).worksheet(ABA)


def _abrir_aba(nome_aba: str, planilha_id: str | None = None):
    import gspread
    from google.oauth2.service_account import Credentials
    info = _credenciais()
    if info is None:
        raise RuntimeError("Service Account ausente.")
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = _reforcar_sessao(gspread.authorize(creds))
    return gc.open_by_key(planilha_id or PLANILHA).worksheet(nome_aba)


def _norm_hdr(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.split()).strip().lower()


def _idx_hdr(headers, *nomes):
    norm = [_norm_hdr(h) for h in headers]
    for nome in nomes:
        n = _norm_hdr(nome)
        if n in norm:
            return norm.index(n)
    return None


def carregar_referencias_rateio() -> dict:
    """
    Lê 'C. Diários' (Obra -> Código Omie do centro de custo) e
    'Plano Financeiro' (Categoria -> Código Omie) da planilha principal.
    Retorna {'obras': [{obra,codigo,projeto,conta}], 'categorias': [{categoria,codigo}]}.
    """
    obras = []
    try:
        vals = _abrir_aba("C. Diários").get_all_values()
    except Exception:
        vals = []
    if vals:
        h = vals[0]
        i_obra = _idx_hdr(h, "Código Primário", "Obra")
        i_proj = _idx_hdr(h, "Projeto")
        i_conta = _idx_hdr(h, "Conta de Pagamento", "Conta")
        i_cod = _idx_hdr(h, "Código Omie", "Codigo Omie")
        for r in vals[1:]:
            def _g(i):
                return r[i].strip() if (i is not None and i < len(r)) else ""
            obra, cod = _g(i_obra), _g(i_cod)
            if not obra or not cod:
                continue
            obras.append({"obra": obra, "codigo": cod,
                          "projeto": _g(i_proj), "conta": _g(i_conta)})

    categorias = []
    try:
        vals2 = _abrir_aba("Plano Financeiro").get_all_values()
    except Exception:
        vals2 = []
    if vals2:
        h2 = vals2[0]
        i_cat = _idx_hdr(h2, "Plano Financeiro", "Categoria")
        i_cod2 = _idx_hdr(h2, "Código Omie", "Codigo Omie")
        for r in vals2[1:]:
            cat = r[i_cat].strip() if (i_cat is not None and i_cat < len(r)) else ""
            cod = r[i_cod2].strip() if (i_cod2 is not None and i_cod2 < len(r)) else ""
            if not cat or not cod:
                continue
            categorias.append({"categoria": cat, "codigo": cod})

    return {"obras": obras, "categorias": categorias}


def carregar_sp_fiscal() -> list:
    """Lê (ID SP, Documentação Fiscal) da aba Lançamentos. Assume cabeçalho na linha 1.
    Retorna [(sp_id, doc_fiscal), ...]."""
    vals = _abrir_aba(SP_FISCAL_ABA, planilha_id=SP_FISCAL_ID).get_all_values()
    if not vals:
        return []
    h = vals[0]
    i_id = _idx_hdr(h, "ID SP", "ID da SP", "ID", "SP")
    i_doc = _idx_hdr(h, "Documentação Fiscal", "Documentacao Fiscal",
                     "Doc Fiscal", "Documentação")
    if i_id is None:
        i_id = 0          # coluna A
    if i_doc is None:
        i_doc = 1         # coluna B
    out = []
    for r in vals[1:]:
        sid = r[i_id].strip() if i_id < len(r) else ""
        doc = r[i_doc].strip() if i_doc < len(r) else ""
        if sid:
            out.append((sid, doc))
    return out


def sync_sp_fiscal() -> int:
    """Lê a planilha de Lançamentos e grava o mapa ID SP -> Doc. Fiscal no cache."""
    return cache.set_sp_fiscal(carregar_sp_fiscal())


def _linha_para_dict(valores: list) -> dict:
    """Converte uma linha bruta (lista de células) num dict por chave do schema."""
    d = {}
    for k in ALL_KEYS:
        idx = COLS[k].idx
        d[k] = valores[idx].strip() if idx < len(valores) and valores[idx] is not None else ""
    return d


# ---------------------------------------------------------------------------
# ESCRITA GARANTIDA (Fase 2) — grava células + carimbo, com retry leve.
# ---------------------------------------------------------------------------
def _agora_fortaleza() -> str:
    """Carimbo no MESMO formato/timezone do reconciliador (comparável por string)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Fortaleza")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _com_retry(fn, tentativas: int = 5, espera: float = 1.5):
    """Tenta algumas vezes com espera crescente + jitter. Erros de CONEXÃO
    (ConnectionError/SSL/timeout/5xx) ganham espera maior, pois costumam durar alguns
    segundos. Como a sessão do gspread já tem retry de transporte, isto é uma 2ª
    camada. Se falhar tudo, levanta o erro — quem chama mantém na fila e tenta no
    próximo ciclo."""
    import time
    import random
    ult = None
    for i in range(max(1, tentativas)):
        try:
            return fn()
        except Exception as e:       # noqa
            ult = e
            if i >= tentativas - 1:
                break
            msg = str(e).lower()
            conexao = any(k in msg for k in (
                "connection", "timed out", "timeout", "ssl", "broken pipe",
                "reset", "temporarily", "remote end", "max retries",
                "502", "503", "500", "504"))
            base = espera * (i + 1) * (2.0 if conexao else 1.0)
            time.sleep(base + random.uniform(0, 0.6))
    raise ult


def escrever_alteracoes(pendentes: list[dict]):
    """
    Grava no Sheets uma lista de células pendentes [{sp_id, coluna, valor}, ...]
    numa única requisição (batch_update) e carimba a coluna V de cada linha tocada
    (para os outros usuários sincronizarem). Robusto a deslocamento de linhas:
    relê a coluna A (IDs) a cada execução e mapeia id -> nº da linha.

    Retorna (escritos, nao_encontrados):
      - escritos        : lista de (sp_id, coluna) gravados com sucesso
      - nao_encontrados : lista de (sp_id, coluna) cujo ID não existe na planilha
    Levanta exceção se a requisição de escrita falhar (sem internet/API) —
    nesse caso quem chama mantém tudo na fila.
    """
    if not pendentes:
        return [], []
    ws = _abrir()
    ids_col = _com_retry(lambda: ws.col_values(1))     # coluna A (com cabeçalho)
    mapa = {}
    for i, val in enumerate(ids_col):
        v = (val or "").strip()
        if i >= (PRIMEIRA_LINHA_DADOS - 1) and v and v not in mapa:
            mapa[v] = i + 1                              # nº de linha (1-based)

    carimbo = _agora_fortaleza()
    v_letter = COLS[CARIMBO_KEY].letter
    dados_batch, escritos, nao_enc, linhas = [], [], [], set()
    for p in pendentes:
        sp, col, valor = str(p["sp_id"]), p["coluna"], p.get("valor", "")
        rn = mapa.get(sp)
        if not rn or col not in COLS:
            nao_enc.append((sp, col))
            continue
        letra = COLS[col].letter
        # range RELATIVO à worksheet (sem 'SPsBD!'): o ws.batch_update do gspread
        # já prefixa o nome da aba sozinho. Prefixar aqui duplicava → erro de range.
        dados_batch.append({"range": f"{letra}{rn}", "values": [[valor]]})
        linhas.add(rn)
        escritos.append((sp, col))
    for rn in linhas:                                   # carimba cada linha tocada
        dados_batch.append({"range": f"{v_letter}{rn}", "values": [[carimbo]]})

    if dados_batch:
        _com_retry(lambda: ws.batch_update(dados_batch, value_input_option="USER_ENTERED"))
    return escritos, nao_enc


def bootstrap() -> int:
    """Carga COMPLETA inicial: lê toda a SPsBD e popula o cache. Use uma vez."""
    cache.init_db()
    ws = _abrir()
    valores = ws.get_all_values()  # inclui cabeçalho
    rows = [_linha_para_dict(v) for v in valores[1:] if v and v[0].strip()]
    n = cache.upsert_rows(rows)
    maior = _maior_carimbo(rows)
    cache.set_meta("ultimo_carimbo", maior)
    cache.set_meta("ultimo_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("bootstrap: %s linhas carregadas (carimbo=%s)", n, maior)
    return n


def _maior_carimbo(rows: list[dict]) -> str:
    carimbos = [r.get(CARIMBO_KEY, "") for r in rows if r.get(CARIMBO_KEY)]
    return max(carimbos) if carimbos else ""


def sync_delta() -> dict:
    """
    Sincronização incremental. Lê só ID + Carimbo, busca em lote as linhas mudadas,
    remove as excluídas. Retorna métricas do ciclo.
    """
    ws = _abrir()
    col_id = COLS["id"].letter

    # 2 colunas leves: A (ID) e V (Carimbo). Com retry: uma oscilação momentânea
    # do Google (500/503/429 passageiro) não pode abortar o ciclo inteiro — é o
    # mesmo blindamento que o caminho de escrita já usa.
    ids_col = _com_retry(lambda: ws.col_values(COLS["id"].idx + 1))
    carimbos_col = _com_retry(lambda: ws.col_values(COLS[CARIMBO_KEY].idx + 1))

    ultimo = cache.get_meta("ultimo_carimbo", "") or ""
    cache_ids = cache.sync_keys()

    linhas_mudadas = []      # nº de linha (1-based) na planilha
    ids_planilha = set()
    for i in range(PRIMEIRA_LINHA_DADOS, len(ids_col) + 1):
        sp_id = (ids_col[i - 1] if i - 1 < len(ids_col) else "").strip()
        if not sp_id:
            continue
        ids_planilha.add(sp_id)
        carimbo = (carimbos_col[i - 1] if i - 1 < len(carimbos_col) else "").strip()
        novo = sp_id not in cache_ids
        mudou = carimbo and carimbo > ultimo
        if novo or mudou:
            linhas_mudadas.append(i)

    # busca em lote só as linhas mudadas (intervalos completos A:AL).
    # Fatiado em blocos para não estourar payload/URL quando muitas linhas mudam
    # de uma vez (ex.: logo após uma rodada grande do Make), cada bloco com retry.
    rows_novas = []
    if linhas_mudadas:
        ranges = [f"{col_id}{ln}:{FIM_LETRA}{ln}" for ln in linhas_mudadas]
        TAM_BLOCO = 200
        for ini in range(0, len(ranges), TAM_BLOCO):
            fatia = ranges[ini:ini + TAM_BLOCO]
            blocos = _com_retry(lambda f=fatia: ws.batch_get(f))
            for bloco in blocos:
                if bloco and bloco[0]:
                    rows_novas.append(_linha_para_dict(bloco[0]))

    # Quantas dessas linhas têm conteúdo REALMENTE diferente do cache local?
    # (ignora o carimbo: as próprias gravações deste app voltam com carimbo novo,
    # mas o conteúdo já está na tela — não é novidade para o usuário.)
    diferentes = 0
    if rows_novas:
        antes = cache.rows_por_ids([r.get("id", "") for r in rows_novas])
        for r in rows_novas:
            a = antes.get(str(r.get("id", "")))
            if a is None:                      # linha nova de verdade
                diferentes += 1
                continue
            for k, v in r.items():
                if k in ("id", CARIMBO_KEY):
                    continue
                if str(v or "") != str(a.get(k, "") or ""):
                    diferentes += 1
                    break

    inseridas = cache.upsert_rows(rows_novas)

    # exclusões: estava no cache, sumiu da planilha
    excluidos = list(cache_ids - ids_planilha)
    removidas = cache.remover_ids(excluidos)

    if rows_novas:
        novo_carimbo = max(_maior_carimbo(rows_novas), ultimo)
        cache.set_meta("ultimo_carimbo", novo_carimbo)
    cache.set_meta("ultimo_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return {"mudadas": inseridas, "diferentes": diferentes, "removidas": removidas,
            "linhas_lidas_leves": max(0, len(ids_col) - 1)}