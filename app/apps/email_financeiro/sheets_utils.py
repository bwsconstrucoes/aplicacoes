# -*- coding: utf-8 -*-
"""
Integração com Google Sheets.
- Usa service account com scopes corretos (Sheets + Drive)
- Garante cabeçalhos mesmo se a aba já existir sem header
- Faz append via values.append (USER_ENTERED + INSERT_ROWS)
"""

import os, json
from base64 import b64decode
from datetime import datetime

import gspread
from gspread.exceptions import WorksheetNotFound, APIError
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "Emails": ["Conta", "Remetente", "Assunto", "Data", "Anexos Válidos"],
    "Relatório": [
        "Conta","Remetente","Assunto","Data","Nome do Arquivo","Fornecedor","CNPJ",
        "Nº NF","Valor (R$)","Vencimento","Código de Barras","Tipo","Status","Link"
    ],
    "Runs": ["Data/Hora","Conta","Total","Status","Mensagem","Valor Total Processado (R$)"],
}

def get_sheets_client():
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    creds_dict = json.loads(b64decode(creds_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    return sh

def _get_or_create_ws(sh, title: str):
    """Abre a worksheet; se não existir, cria. Depois garante cabeçalho na linha 1."""
    headers = HEADERS[title]
    try:
        ws = sh.worksheet(title)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=200, cols=max(len(headers), 20))
    # garantir header na linha 1
    try:
        first_row = ws.row_values(1)
    except Exception:
        first_row = []
    if first_row != headers:
        # escreve/atualiza cabeçalho
        sh.values_update(
            f"{title}!A1",
            params={"valueInputOption": "USER_ENTERED"},
            body={"values": [headers]},
        )
    return ws

def _append_row(spreadsheet, title: str, row: list):
    """
    Usa spreadsheets.values.append (mais robusto).
    Range apenas com o nome da aba evita erros de range.
    """
    spreadsheet.values_append(
        title,
        params={
            "valueInputOption": "USER_ENTERED",
            "insertDataOption": "INSERT_ROWS",
        },
        body={"values": [row]},
    )

# ---------- Emails ----------
def append_email_entry(entry):
    sh = get_sheets_client()
    _get_or_create_ws(sh, "Emails")
    _append_row(sh, "Emails", [
        entry.get("Conta", ""),
        entry.get("Remetente", ""),
        entry.get("Assunto", ""),
        entry.get("Data", ""),
        entry.get("Anexos Válidos", ""),
    ])

# ---------- Relatório ----------
def append_financial_entry(entry):
    sh = get_sheets_client()
    _get_or_create_ws(sh, "Relatório")
    _append_row(sh, "Relatório", [
        entry.get("Conta", ""),
        entry.get("Remetente", ""),
        entry.get("Assunto", ""),
        entry.get("Data", ""),
        entry.get("Nome do Arquivo", ""),
        entry.get("Fornecedor", ""),
        entry.get("CNPJ", ""),
        entry.get("Nº NF", ""),
        entry.get("Valor (R$)", ""),
        entry.get("Vencimento", ""),
        entry.get("Código de Barras", ""),
        entry.get("Tipo", ""),
        entry.get("Status", ""),
        entry.get("Link", ""),
    ])

# ---------- Runs ----------
def log_run_summary(results):
    sh = get_sheets_client()
    _get_or_create_ws(sh, "Runs")
    total_emails = sum(r.get("emails", 0) for r in results)
    total_anexos = sum(r.get("anexos", 0) for r in results)
    total_valor = sum(r.get("valor_total", 0.0) for r in results)
    _append_row(sh, "Runs", [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Auto",
        total_emails,
        "✅ OK",
        f"{len(results)} contas processadas, {total_anexos} anexos",
        f"R$ {total_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
    ])

# ---------- Painel ----------
def get_status_summary():
    try:
        sh = get_sheets_client()
        ws = _get_or_create_ws(sh, "Runs")
        data = ws.get_all_values()
        if len(data) < 2:
            return {"message": "Sem registros."}
        last = data[-1]
        return {
            "ultima_execucao": last[0],
            "emails": last[2],
            "status": last[3],
            "descricao": last[4],
            "valor_total": last[5],
        }
    except APIError as e:
        return {"erro": f"APIError Sheets: {e}"}
    except Exception as e:
        return {"erro": str(e)}
