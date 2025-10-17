# -*- coding: utf-8 -*-
"""
Integração com Google Sheets.
Cria abas se faltarem e faz append via values_append (USER_ENTERED).
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

SHEET_TITLES = {
    "Emails": ["Conta", "Remetente", "Assunto", "Data", "Anexos Válidos"],
    "Relatório": [
        "Conta","Remetente","Assunto","Data","Nome do Arquivo","Fornecedor","CNPJ",
        "Nº NF","Valor (R$)","Vencimento","Código de Barras","Tipo","Status","Link"
    ],
    "Runs": ["Data/Hora","Conta","Total","Status","Mensagem","Valor Total Processado (R$)"],
}

# ---------- Autenticação ----------
def get_sheets_client():
    creds_json_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    creds_dict = json.loads(b64decode(creds_json_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    return sh

def _get_or_create_ws(sh, title: str):
    """Abre a worksheet; se não existir, cria com cabeçalho."""
    headers = SHEET_TITLES[title]
    try:
        ws = sh.worksheet(title)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=200, cols=max(10, len(headers)))
        # escreve cabeçalho pela API de valores (mais robusto)
        sh.values_update(
            f"{title}!A1",
            params={"valueInputOption": "USER_ENTERED"},
            body={"values": [headers]},
        )
    return ws

def _append_row_via_api(sh, title: str, row_values: list):
    """
    Usa a endpoint values_append da Sheets API (evita 400 invalid argument).
    Inserção como linhas com USER_ENTERED.
    """
    sh.values_append(
        f"{title}!A1",
        params={
            "valueInputOption": "USER_ENTERED",
            "insertDataOption": "INSERT_ROWS",
        },
        body={"values": [row_values]},
    )

# ---------- Grava e-mails ----------
def append_email_entry(entry):
    try:
        sh = get_sheets_client()
        _get_or_create_ws(sh, "Emails")
        _append_row_via_api(sh, "Emails", [
            entry.get("Conta", ""),
            entry.get("Remetente", ""),
            entry.get("Assunto", ""),
            entry.get("Data", ""),
            entry.get("Anexos Válidos", ""),
        ])
    except APIError as e:
        print(f"[Erro append_email_entry API] {e}")
        raise
    except Exception as e:
        print(f"[Erro append_email_entry] {e}")
        raise

# ---------- Grava anexos financeiros ----------
def append_financial_entry(entry):
    try:
        sh = get_sheets_client()
        _get_or_create_ws(sh, "Relatório")
        _append_row_via_api(sh, "Relatório", [
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
    except APIError as e:
        print(f"[Erro append_financial_entry API] {e}")
        raise
    except Exception as e:
        print(f"[Erro append_financial_entry] {e}")
        raise

# ---------- Log das execuções ----------
def log_run_summary(results):
    try:
        sh = get_sheets_client()
        _get_or_create_ws(sh, "Runs")
        total_emails = sum(r.get("emails", 0) for r in results)
        total_anexos = sum(r.get("anexos", 0) for r in results)
        total_valor = sum(r.get("valor_total", 0.0) for r in results)
        _append_row_via_api(sh, "Runs", [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Auto",
            total_emails,
            "✅ OK",
            f"{len(results)} contas processadas, {total_anexos} anexos",
            f"R$ {total_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        ])
    except APIError as e:
        print(f"[Erro log_run_summary API] {e}")
        raise
    except Exception as e:
        print(f"[Erro log_run_summary] {e}")
        raise

# ---------- Resumo p/ painel ----------
def get_status_summary():
    try:
        sh = get_sheets_client()
        _get_or_create_ws(sh, "Runs")
        ws = sh.worksheet("Runs")
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
