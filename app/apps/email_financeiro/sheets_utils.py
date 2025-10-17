# -*- coding: utf-8 -*-
"""
Integração com Google Sheets.
Grava as abas Emails, Relatório e Runs.
"""

import os
import json
from base64 import b64decode
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# Scopes necessários (Sheets de leitura/escrita):
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheets_client():
    creds_json_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "")
    creds_dict = json.loads(b64decode(creds_json_b64).decode("utf-8"))
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    return sh

# (restante do arquivo permanece igual)


# ========== Autenticação Google ==========
def get_sheets_client():
    creds_json = b64decode(os.getenv("GOOGLE_CREDENTIALS_BASE64")).decode("utf-8")
    creds = Credentials.from_service_account_info(eval(creds_json))
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    return sh


# ========== Grava e-mails ==========
def append_email_entry(entry):
    try:
        sh = get_sheets_client()
        ws = sh.worksheet("Emails")
        ws.append_row([
            entry.get("Conta", ""),
            entry.get("Remetente", ""),
            entry.get("Assunto", ""),
            entry.get("Data", ""),
            entry.get("Anexos Válidos", ""),
        ])
    except Exception as e:
        print(f"[Erro append_email_entry] {e}")


# ========== Grava anexos financeiros ==========
def append_financial_entry(entry):
    try:
        sh = get_sheets_client()
        ws = sh.worksheet("Relatório")
        ws.append_row([
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
    except Exception as e:
        print(f"[Erro append_financial_entry] {e}")


# ========== Log das execuções ==========
def log_run_summary(results):
    try:
        sh = get_sheets_client()
        ws = sh.worksheet("Runs")
        total_emails = sum(r["emails"] for r in results)
        total_anexos = sum(r["anexos"] for r in results)
        total_valor = sum(r["valor_total"] for r in results)
        ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Auto",
            total_emails,
            "✅ OK",
            f"{len(results)} contas processadas, {total_anexos} anexos",
            f"R$ {total_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        ])
    except Exception as e:
        print(f"[Erro log_run_summary] {e}")


# ========== Resumo p/ painel ==========
def get_status_summary():
    try:
        sh = get_sheets_client()
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
    except Exception as e:
        return {"erro": str(e)}

