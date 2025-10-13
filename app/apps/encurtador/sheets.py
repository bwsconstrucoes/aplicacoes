import os, csv, time, unicodedata, requests, re
import re
from datetime import datetime, timezone, date
import re
from urllib.parse import quote

SHEET_ID   = os.getenv("SHEET_ID", "1k-ydMq9JEhWGSt7P3D0ucYj2bWNMkhA9uk1kBJiOMb8")
SHEET_NAME = os.getenv("SHEET_NAME", "Links")

# -------- util --------
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def norm_col(col: str) -> str:
    return _strip_accents(col).strip().lower().replace(" ", "_")

def _parse_expira_em(v: str):
    """
    Regras simples e à prova de erro:
    - vazio ou 'nunca'  -> sem expiração (None)
    - qualquer string   -> usa só os 10 primeiros chars como 'YYYY-MM-DD'
      (ex.: '2025-10-20T11:56:37-03:00' -> '2025-10-20')
    - se não conseguir parsear -> sem expiração (None)
    """
    if not v:
        return None
    s = v.strip()
    if s.lower() == "nunca":
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None  # nunca explode; datas inválidas passam como "sem expiração"

# -------- leitura via API (preferida) --------
def _google_service_sheets():
    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    if not b64:
        return None
    import base64, json
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    info = json.loads(base64.b64decode(b64).decode("utf-8"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds)

def _rows_via_api():
    svc = _google_service_sheets()
    if not svc:
        return None
    rng = f"{SHEET_NAME}!A:C"
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=rng,
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute()
    values = res.get("values", [])
    if not values:
        return []
    header = [norm_col(h) for h in values[0]]
    idx = {h: i for i, h in enumerate(header)}
    out = []
    for row in values[1:]:
        def get(k):
            i = idx.get(k)
            return str(row[i]).strip() if i is not None and i < len(row) else ""
        out.append({
            "codigo": get("codigo"),
            "url": get("url"),
            "expira_em": get("expira_em"),
        })
    return out

# -------- fallback CSV público --------
def _csv_url():
    cb = int(time.time())
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={quote(SHEET_NAME)}&cachebust={cb}"

def _rows_via_csv():
    r = requests.get(_csv_url(), timeout=30)
    r.raise_for_status()
    lines = [ln for ln in r.content.decode("utf-8", errors="replace").splitlines() if ln.strip()]
    if not lines:
        return []
    first = lines[0]
    delim = "\t" if "\t" in first else (";" if first.count(";") > first.count(",") else ",")
    reader = csv.DictReader(lines, delimiter=delim)
    reader.fieldnames = [norm_col(h) for h in (reader.fieldnames or [])]
    out = []
    for row in reader:
        row = {norm_col(k): (v or "").strip() for k, v in row.items()}
        # tolerância a cabeçalhos “estranhos”
        def pick(key):
            if key in row:
                return row[key]
            for k in row:
                if key in k:
                    return row[k]
            return ""
        out.append({
            "codigo": pick("codigo"),
            "url": pick("url"),
            "expira_em": pick("expira_em"),
        })
    return out

def _carregar_linhas():
    rows = _rows_via_api()
    if rows is None:  # sem credencial, usa CSV
        rows = _rows_via_csv()
    return [r for r in rows if any(r.values())]

# -------- API pública usada pelo encurtador --------
def buscar_url_por_codigo(codigo: str):
    codigo = (codigo or "").strip()
    if not codigo:
        return None
    for r in _carregar_linhas():
        if r.get("codigo", "").strip().lower() == codigo.lower():
            exp = _parse_expira_em((r.get("expira_em") or "").strip())
            if isinstance(exp, date) and date.today() > exp:
                return None
            return r
    return None

# -------- escrita (se usar criar pelo servidor) --------
def adicionar_link(codigo: str, url: str, expira_em: str) -> bool:
    svc = _google_service_sheets()
    if not svc:
        return False
    body = {"values": [[codigo, url, expira_em]]}
    try:
        svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A:C",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
        return True
    except Exception:
        return False
