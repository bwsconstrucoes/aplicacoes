import csv
import io
import os
import unicodedata
from datetime import datetime, timezone
import re
from typing import Dict, List, Optional
from urllib.parse import quote

import requests

# ==== CONFIGURAÇÃO DA SUA PLANILHA ====
SHEET_ID = os.getenv("SHEET_ID", "1k-ydMq9JEhWGSt7P3D0ucYj2bWNMkhA9uk1kBJiOMb8")
SHEET_NAME = os.getenv("SHEET_NAME", "Links")  # aba


# ---------- utilidades ----------
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def normalizar_coluna(col: str) -> str:
    col = _strip_accents(col or "")
    return (
        col.strip().lower()
        .replace(" ", "_")  # <— fundamental para expira em -> expira_em
    )


def _detectar_delimitador(first_line: str) -> str:
    # Decide entre TAB, ';' ou ',' pela “maior evidência”
    if "\t" in first_line:
        return "\t"
    sc, cc = first_line.count(";"), first_line.count(",")
    return ";" if sc > cc else ","


def _csv_url(sheet_id: str, sheet_name: str) -> str:
    # CSV público da aba (compartilhamento "qualquer pessoa com o link: leitor")
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}"


def _baixar_csv_linhas() -> List[str]:
    url = _csv_url(SHEET_ID, SHEET_NAME)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    content = r.content.decode("utf-8", errors="replace").splitlines()
    # Remove linhas totalmente vazias
    return [ln for ln in content if ln.strip()]


def _normalizar_header(headers: List[str]) -> List[str]:
    return [normalizar_coluna(h) for h in headers]


def _mapear_campos(row: Dict[str, str]) -> Dict[str, str]:
    """
    Garante as chaves 'codigo', 'url', 'expira_em' mesmo que o CSV venha com nomes esquisitos.
    """
    norm = {normalizar_coluna(k): v for k, v in row.items()}

    def pick(key: str) -> Optional[str]:
        if key in norm:
            return norm[key]
        # fallback: primeira chave que contenha o token desejado
        for k in norm:
            if key in k:
                return norm[k]
        return None

    return {
        "codigo": (pick("codigo") or "").strip(),
        "url": (pick("url") or "").strip(),
        "expira_em": (pick("expira_em") or "").strip(),
    }


def _carregar_linhas_normalizadas() -> List[Dict[str, str]]:
    linhas = _baixar_csv_linhas()
    if not linhas:
        return []
    delim = _detectar_delimitador(linhas[0])

    reader = csv.DictReader(linhas, delimiter=delim)
    # padroniza cabeçalhos
    reader.fieldnames = _normalizar_header(reader.fieldnames or [])

    out: List[Dict[str, str]] = []
    for row in reader:
        m = _mapear_campos(row)
        if not (m["codigo"] or m["url"] or m["expira_em"]):
            continue
        out.append(m)
    return out


def _parse_expira_em(v: str):
    if not v:
        return None
    v = v.strip()
    if v.lower() == "nunca":
        return None

    # tolerar "YYYY-MM-DD HH:MM:SS..." => vira "YYYY-MM-DDTHH:MM:SS..."
    if " " in v and "T" not in v:
        v = v.replace(" ", "T", 1)

    # tolerar "Z" => "+00:00"
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"

    # normalizar offsets tipo -0300 ou -03:00
    m = re.match(r"^(.*?)([+-]\d{2}):?(\d{2})$", v)
    if m:
        v = f"{m.group(1)}{m.group(2)}:{m.group(3)}"

    try:
        return datetime.fromisoformat(v)
    except Exception:
        # fallback: formatos comuns
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(v, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
        # se realmente não deu:
        raise ValueError("Formato de expiração inválido")


# ---------- API usada pelo encurtador ----------
def buscar_url_por_codigo(codigo: str) -> Optional[Dict[str, str]]:
    """
    Retorna dict {'codigo','url','expira_em'} para o código informado,
    ou None se não achar / estiver expirado.
    """
    codigo = (codigo or "").strip()
    if not codigo:
        return None

    linhas = _carregar_linhas_normalizadas()
    if not linhas:
        return None

    # busca case-insensitive
    alvo = None
    for row in linhas:
        if row.get("codigo", "").strip().lower() == codigo.lower():
            alvo = row
            break

    if not alvo:
        return None

    # valida expiração
    exp = _parse_expira_em(alvo.get("expira_em", ""))
    if exp and datetime.now(exp.tzinfo or timezone.utc) >= exp:
        return None

    return alvo


# ---------- escrita opcional (se você usa adicionar_link no servidor) ----------
def _google_service_sheets():
    """
    Cria o client do Google Sheets se GOOGLE_CREDENTIALS_BASE64 estiver setado.
    """
    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    if not b64:
        return None

    import base64
    import json
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    info = json.loads(base64.b64decode(b64).decode("utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def adicionar_link(codigo: str, url: str, expira_em: str) -> bool:
    values = [[codigo, url, expira_em]]
    """
    Anexa uma linha na planilha (se as credenciais Google estiverem configuradas).
    Retorna True em caso de sucesso, False caso contrário.
    """
    svc = _google_service_sheets()
    if not svc:
        return False

    values = [[codigo, url, expira_em]]
    body = {"values": values}
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
