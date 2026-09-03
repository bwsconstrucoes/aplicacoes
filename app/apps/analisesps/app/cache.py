# -*- coding: utf-8 -*-
"""
cache.py — Cache local em SQLite. É a base de dados que o Streamlit lê.

Por que SQLite e não ler o Sheets a cada interação:
  - leitura instantânea para quem está operando (filtros, KPIs, Lote saem na hora);
  - a sincronização com o Sheets acontece em segundo plano, por DELTA (só as linhas
    que mudaram desde o último carimbo), evitando baixar tudo toda hora.

Tabela única `sps`: uma coluna por chave do schema + uma coluna de controle
`_dirty` (1 = editado localmente e ainda não enviado ao Sheets — fase 2).

Estratégia de atualização (resposta à pergunta do Marcelo):
  1) bootstrap()  -> carga inicial completa (uma vez).
  2) upsert_rows()-> aplica em lote as linhas vindas do delta do Sheets.
  3) editar_local()-> grava a alteração do próprio usuário NA HORA (otimista) e marca
                      _dirty=1; o push pro Sheets fica para a fase 2 (scripts do Marcelo).
  4) sync_keys()  -> conjunto de IDs presentes, para detectar exclusões.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timedelta

from schema import ALL_KEYS, CARIMBO_KEY

def _db_padrao() -> str:
    """Caminho padrão do banco de CACHE.

    O banco é LOCAL da máquina e NUNCA deve ser sincronizado (Dropbox, OneDrive
    etc.): SQLite não se funde entre máquinas — duas instâncias geram conflito e
    a 'última sincronização'/config divergem. Por isso o padrão fica numa pasta
    local do sistema operacional, fora de qualquer pasta sincronizada. Pode ser
    sobrescrito pela variável de ambiente SPSBD_DB."""
    base = (os.environ.get("LOCALAPPDATA")                       # Windows
            or os.environ.get("XDG_DATA_HOME")                  # Linux
            or os.path.join(os.path.expanduser("~"), ".local", "share"))
    pasta = os.path.join(base, "spsbd_app")
    try:
        os.makedirs(pasta, exist_ok=True)
        return os.path.join(pasta, "spsbd_cache.db")
    except Exception:
        # Fallback extremo: ao lado do código (evita quebrar se a pasta falhar).
        return os.path.join(os.path.dirname(__file__), "spsbd_cache.db")


DB_PATH = os.environ.get("SPSBD_DB", _db_padrao())
_LOCK = threading.Lock()


def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    cols_sql = ", ".join(f'"{k}" TEXT' for k in ALL_KEYS if k != "id")
    with _LOCK, _conn() as c:
        c.execute(f'CREATE TABLE IF NOT EXISTS sps ("id" TEXT PRIMARY KEY, {cols_sql}, "_dirty" INTEGER DEFAULT 0)')
        # garante colunas novas se o schema evoluir
        existentes = {r["name"] for r in c.execute('PRAGMA table_info(sps)')}
        for k in ALL_KEYS + ["_dirty"]:
            if k not in existentes:
                tipo = "INTEGER DEFAULT 0" if k == "_dirty" else "TEXT"
                c.execute(f'ALTER TABLE sps ADD COLUMN "{k}" {tipo}')
        c.execute('CREATE TABLE IF NOT EXISTS meta (chave TEXT PRIMARY KEY, valor TEXT)')
        # C. Diários: codigo primario (centro de custo) -> Conta de Pagamento (texto cru)
        c.execute('CREATE TABLE IF NOT EXISTS contas_diarios '
                  '(codigo TEXT PRIMARY KEY, conta_pagamento TEXT)')
        # SP Fiscal: ID da SP -> Documentação Fiscal (vindo da planilha Lançamentos)
        c.execute('CREATE TABLE IF NOT EXISTS sp_fiscal '
                  '(sp_id TEXT PRIMARY KEY, doc_fiscal TEXT)')
        # FILA DURÁVEL de escrita p/ o Sheets. Uma linha por célula a gravar
        # (sp_id + coluna). Sobrevive a reinício e a quedas de internet: só sai
        # daqui depois de confirmada a gravação online. Reescrever a mesma célula
        # substitui o valor pendente (o último vale) e zera as tentativas.
        c.execute('CREATE TABLE IF NOT EXISTS fila ('
                  'sp_id TEXT, coluna TEXT, valor TEXT, criado_em TEXT, '
                  'tentativas INTEGER DEFAULT 0, ultimo_erro TEXT, '
                  'PRIMARY KEY (sp_id, coluna))')
        # LOG DE AUDITORIA permanente (retenção configurável, padrão 90 dias).
        # Registra TODA alteração: o que mudou, quando aplicou local, quando
        # subiu pro Sheets, status e erros. Append-only (nunca sobrescreve linhas).
        c.execute('CREATE TABLE IF NOT EXISTS log_alteracoes ('
                  'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                  'sp_id TEXT, coluna TEXT, valor TEXT, acao TEXT, status TEXT, '
                  'aplicado_local TEXT, enviado_em TEXT, '
                  'tentativas INTEGER DEFAULT 0, ultimo_erro TEXT, criado_em TEXT)')
        c.execute('CREATE INDEX IF NOT EXISTS ix_log_criado ON log_alteracoes(criado_em)')


# ---------------------------------------------------------------------------
# FILA DURÁVEL (escrita garantida no Sheets)
# ---------------------------------------------------------------------------
def enfileirar(sp_id: str, coluna: str, valor: str):
    """Coloca (ou atualiza) uma célula na fila de envio. O último valor vale."""
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _LOCK, _conn() as c:
        c.execute('INSERT INTO fila (sp_id, coluna, valor, criado_em, tentativas, ultimo_erro) '
                  'VALUES (?,?,?,?,0,NULL) '
                  'ON CONFLICT(sp_id, coluna) DO UPDATE SET '
                  'valor=excluded.valor, criado_em=excluded.criado_em, '
                  'tentativas=0, ultimo_erro=NULL',
                  (str(sp_id), coluna, "" if valor is None else str(valor), agora))


def fila_pendentes() -> list[dict]:
    with _conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT sp_id, coluna, valor, criado_em, tentativas, ultimo_erro "
            "FROM fila ORDER BY criado_em")]


def fila_remover(pares: list) -> int:
    """Remove da fila os pares (sp_id, coluna) já gravados online."""
    if not pares:
        return 0
    with _LOCK, _conn() as c:
        cur = c.executemany("DELETE FROM fila WHERE sp_id=? AND coluna=?",
                            [(str(a), b) for a, b in pares])
        return cur.rowcount


def fila_erro(pares: list, msg: str):
    """Marca tentativa falha (incrementa contador) sem remover da fila."""
    if not pares:
        return
    with _LOCK, _conn() as c:
        c.executemany("UPDATE fila SET tentativas=tentativas+1, ultimo_erro=? "
                      "WHERE sp_id=? AND coluna=?",
                      [(str(msg)[:300], str(a), b) for a, b in pares])


def fila_contar() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) AS n FROM fila").fetchone()["n"]


# ---------------------------------------------------------------------------
# LOG DE AUDITORIA (permanente, retenção padrão 90 dias)
# ---------------------------------------------------------------------------
LOG_RETENCAO_DIAS = 90


def _agora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _limite_data(dias: int) -> str:
    return (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d %H:%M:%S")


def log_registrar(sp_id: str, coluna: str, valor: str, acao: str, status: str = "pendente"):
    """Registra uma alteração no log e poda registros além da retenção."""
    agora = _agora()
    with _LOCK, _conn() as c:
        c.execute("INSERT INTO log_alteracoes "
                  "(sp_id, coluna, valor, acao, status, aplicado_local, criado_em, tentativas) "
                  "VALUES (?,?,?,?,?,?,?,0)",
                  (str(sp_id), coluna, "" if valor is None else str(valor),
                   acao, status, agora, agora))
        c.execute("DELETE FROM log_alteracoes WHERE criado_em < ?",
                  (_limite_data(LOG_RETENCAO_DIAS),))


def log_marcar_enviado(pares: list):
    """Marca como 'enviado' os registros (sp_id, coluna) confirmados online."""
    if not pares:
        return
    agora = _agora()
    with _LOCK, _conn() as c:
        c.executemany("UPDATE log_alteracoes SET status='enviado', enviado_em=?, "
                      "ultimo_erro=NULL WHERE sp_id=? AND coluna=? AND status!='enviado'",
                      [(agora, str(a), b) for a, b in pares])


def log_marcar_erro(pares: list, msg: str):
    if not pares:
        return
    with _LOCK, _conn() as c:
        c.executemany("UPDATE log_alteracoes SET status='erro', tentativas=tentativas+1, "
                      "ultimo_erro=? WHERE sp_id=? AND coluna=? AND status='pendente'",
                      [(str(msg)[:300], str(a), b) for a, b in pares])


def log_listar(dias: int = LOG_RETENCAO_DIAS, status: str = None,
               busca: str = None, limite: int = 5000) -> list[dict]:
    cond, args = ["criado_em >= ?"], [_limite_data(dias)]
    if status and status != "todos":
        cond.append("status = ?"); args.append(status)
    if busca:
        cond.append("sp_id LIKE ?"); args.append(f"%{busca}%")
    q = ("SELECT criado_em, sp_id, coluna, valor, acao, status, enviado_em, "
         "tentativas, ultimo_erro FROM log_alteracoes WHERE " + " AND ".join(cond) +
         " ORDER BY criado_em DESC, id DESC LIMIT ?")
    args.append(limite)
    with _conn() as c:
        return [dict(r) for r in c.execute(q, args)]


def log_contar(dias: int = LOG_RETENCAO_DIAS) -> dict:
    with _conn() as c:
        rows = c.execute("SELECT status, COUNT(*) AS n FROM log_alteracoes "
                         "WHERE criado_em >= ? GROUP BY status",
                         (_limite_data(dias),)).fetchall()
    return {r["status"]: r["n"] for r in rows}


def set_contas_diarios(rows: list[tuple]) -> int:
    """rows = [(codigo, conta_pagamento), ...]. Substitui o mapa inteiro."""
    with _LOCK, _conn() as c:
        c.execute("DELETE FROM contas_diarios")
        c.executemany("INSERT OR REPLACE INTO contas_diarios (codigo, conta_pagamento) "
                      "VALUES (?, ?)", [(str(a).strip().upper(), str(b).strip()) for a, b in rows if str(a).strip()])
    return len(rows)


def get_mapa_contas() -> dict:
    """{CODIGO_PRIMARIO_UPPER: 'Conta de Pagamento (texto cru)'}."""
    with _conn() as c:
        return {r["codigo"]: r["conta_pagamento"]
                for r in c.execute("SELECT codigo, conta_pagamento FROM contas_diarios")}


def contar_contas() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) n FROM contas_diarios").fetchone()["n"]


def set_sp_fiscal(rows: list) -> int:
    """rows = [(sp_id, doc_fiscal), ...]. Substitui o mapa inteiro."""
    limpos = [(str(a).strip(), str(b).strip()) for a, b in rows if str(a).strip()]
    with _LOCK, _conn() as c:
        c.execute("DELETE FROM sp_fiscal")
        c.executemany("INSERT OR REPLACE INTO sp_fiscal (sp_id, doc_fiscal) VALUES (?, ?)",
                      limpos)
    return len(limpos)


def get_mapa_sp_fiscal() -> dict:
    """{ID_SP: 'Documentação Fiscal'}."""
    with _conn() as c:
        return {r["sp_id"]: r["doc_fiscal"]
                for r in c.execute("SELECT sp_id, doc_fiscal FROM sp_fiscal")}


def contar_sp_fiscal() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) n FROM sp_fiscal").fetchone()["n"]


def contar() -> int:
    with _conn() as c:
        return c.execute("SELECT COUNT(*) n FROM sps").fetchone()["n"]


def rows_por_ids(ids: list[str]) -> dict:
    """Linhas do cache para os IDs dados: {id: dict}. Usado pelo sync para saber
    se o que chegou da planilha é realmente DIFERENTE do que já temos."""
    out: dict = {}
    ids = [str(i) for i in ids if str(i).strip()]
    if not ids:
        return out
    with _LOCK, _conn() as c:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            q = ",".join("?" * len(chunk))
            for r in c.execute(f"SELECT * FROM sps WHERE id IN ({q})", chunk):
                d = dict(r)
                out[str(d.get("id", ""))] = d
    return out


def upsert_rows(rows: list[dict]) -> int:
    """Insere/atualiza linhas (cada dict tem ao menos 'id'). Não mexe em _dirty."""
    if not rows:
        return 0
    cols = ALL_KEYS
    col_list = ", ".join('"' + k + '"' for k in cols)
    placeholders = ", ".join("?" for _ in cols)
    update = ", ".join('"' + k + '"=excluded."' + k + '"' for k in cols if k != "id")
    sql = (f'INSERT INTO sps ({col_list}) '
           f'VALUES ({placeholders}) '
           f'ON CONFLICT(id) DO UPDATE SET {update}')
    data = [[str(r.get(k, "") or "") for k in cols] for r in rows]
    with _LOCK, _conn() as c:
        c.executemany(sql, data)
    return len(data)


def remover_ids(ids: list[str]) -> int:
    if not ids:
        return 0
    with _LOCK, _conn() as c:
        c.executemany("DELETE FROM sps WHERE id=?", [(str(i),) for i in ids])
    return len(ids)


def editar_local(sp_id: str, alteracoes: dict) -> bool:
    """
    Edição OTIMISTA: grava na hora no cache local e marca _dirty=1.
    alteracoes = {'status_pgt': 'Pago', 'agendado': 'Agendado', ...}
    O envio dessas mudanças ao Sheets é a fase 2 (usar scripts do Marcelo).
    """
    alteracoes = {k: v for k, v in alteracoes.items() if k in ALL_KEYS}
    if not alteracoes:
        return False
    alteracoes[CARIMBO_KEY] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sets = ", ".join(f'"{k}"=?' for k in alteracoes) + ', "_dirty"=1'
    vals = list(alteracoes.values()) + [str(sp_id)]
    with _LOCK, _conn() as c:
        cur = c.execute(f"UPDATE sps SET {sets} WHERE id=?", vals)
        return cur.rowcount > 0


def pendentes_envio() -> list[dict]:
    """Linhas editadas localmente ainda não enviadas (fase 2)."""
    with _conn() as c:
        return [dict(r) for r in c.execute("SELECT * FROM sps WHERE _dirty=1")]


def marcar_enviados(ids: list[str]):
    if not ids:
        return
    with _LOCK, _conn() as c:
        c.executemany("UPDATE sps SET _dirty=0 WHERE id=?", [(str(i),) for i in ids])


def ler_tudo() -> list[dict]:
    with _conn() as c:
        return [dict(r) for r in c.execute("SELECT * FROM sps")]


def ler_por_ids(ids: list[str]) -> list[dict]:
    if not ids:
        return []
    marks = ", ".join("?" for _ in ids)
    with _conn() as c:
        return [dict(r) for r in c.execute(f"SELECT * FROM sps WHERE id IN ({marks})",
                                           [str(i) for i in ids])]


def sync_keys() -> set[str]:
    with _conn() as c:
        return {r["id"] for r in c.execute("SELECT id FROM sps")}


def get_meta(chave: str, default=None):
    with _conn() as c:
        r = c.execute("SELECT valor FROM meta WHERE chave=?", (chave,)).fetchone()
        return r["valor"] if r else default


def set_meta(chave: str, valor: str):
    with _LOCK, _conn() as c:
        c.execute("INSERT INTO meta (chave, valor) VALUES (?, ?) "
                  "ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor", (chave, str(valor)))


def seed_contas_demo() -> int:
    """Mapa mínimo da C. Diários (extrato fornecido) para uso offline/demonstração."""
    demo = [
        ("CONS", "0624 | 0007011-4 | Conta-Corrente"),
        ("INFRADENDE", "0624 | 0007011-4 | Conta-Corrente"),
        ("INFRAPF", "0624 | 0007011-4 | Conta-Corrente"),
        ("CEIURU", "0624 | 0007011-4 | Conta-Corrente"),
        ("CEIFOR2", "0624 | 0007011-4 | Conta-Corrente"),
        ("CEIFOR3", "0624 | 0007011-4 | Conta-Corrente"),
    ]
    return set_contas_diarios(demo)


def seed_de_csv(caminho: str) -> int:
    """Popula o cache a partir de um CSV no schema canônico (fase offline)."""
    import csv
    init_db()
    with open(caminho, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return upsert_rows(rows)