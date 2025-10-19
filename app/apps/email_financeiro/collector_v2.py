# -*- coding: utf-8 -*-
"""
Fase 1 (Rápida): Coleta em IMAP SOMENTE LEITURA + extração rule-based melhorada.
- NÃO MARCA E-MAILS COMO LIDOS (readonly=True + BODY.PEEK[])
- Filtra anexos relevantes (PDF/XML)
- Extrai dados com parser rule-based v2
- Sobe anexos no Drive e grava no Sheets (Emails / Relatório / Runs)
- Respeita STOP_FLAG (se existir em routes.py)
"""

import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta
import time
import traceback
import re
import os

# STOP opcional (se você já adicionou no routes.py)
try:
    from .state import STOP_FLAG  # type: ignore
except Exception:
    STOP_FLAG = {"active": False}

from .parser_financeiro_v2 import extract_financial_data_v2
from .sheets_utils import append_email_entry, append_financial_entry, log_run_summary
from .gdrive_utils import upload_to_drive
from base64 import b64decode
import gspread
from google.oauth2.service_account import Credentials
import json


# ----------------- helpers básicos -----------------

def _decode_mime(text):
    if not text:
        return ""
    parts = decode_header(text)
    decoded = ""
    for s, enc in parts:
        if isinstance(s, bytes):
            try:
                decoded += s.decode(enc or "utf-8", errors="ignore")
            except Exception:
                decoded += s.decode("latin-1", errors="ignore")
        else:
            decoded += s
    return decoded.strip()


def _parse_date_header(date_header):
    """Normaliza data YYYY-MM-DD sem tocar em flags IMAP."""
    if not date_header:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        base = date_header.split("(")[0].strip()
        return datetime.strptime(base[:25], "%a, %d %b %Y %H:%M:%S").strftime("%Y-%m-%d")
    except Exception:
        try:
            return datetime.fromtimestamp(email.utils.mktime_tz(email.utils.parsedate_tz(date_header))).strftime("%Y-%m-%d")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d")


def _is_relevant_attachment(filename: str) -> bool:
    if not filename:
        return False
    name = filename.lower()
    if not (name.endswith(".pdf") or name.endswith(".xml")):
        return False
    # foco financeiro
    if any(k in name for k in ["boleto", "nota", "nf", "nfe", "danfe", "duplicata", "fatura", "cobranca", "cobrança"]):
        # excluir ruídos comuns
        if any(w in name for w in ["assinatura", "signature", "comprovante", "relatorio", "relatório",
                                   "extrato", "planilha", "recibo", "manual", "foto", "imagem",
                                   "contrato", "proposta", "orcamento", "orçamento", "pedido", "curriculo"]):
            return False
        return True
    return False


# -------------- leitura de aba Configurações --------------

def _load_mailbox_configs_from_sheet():
    """
    Lê a aba "Configurações" e encontra a tabela de contas:
    ativo | label | imap_host | imap_user | imap_password | search_since_days | max_emails_per_box
    """
    print("[CFG] Lendo Configurações para contas IMAP...")
    creds_json = b64decode(os.getenv("GOOGLE_CREDENTIALS_BASE64", "")).decode("utf-8")
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    ws = sh.worksheet("Configurações")
    values = ws.get_all_values()

    header = ["ativo", "label", "imap_host", "imap_user", "imap_password", "search_since_days", "max_emails_per_box"]
    header_idx = None
    for i, row in enumerate(values):
        row_norm = [r.strip().lower() for r in row]
        if row_norm[:len(header)] == header:
            header_idx = i
            break
    if header_idx is None:
        print("[CFG] Cabeçalho de contas não encontrado. Verifique a planilha.")
        return []

    configs = []
    for row in values[header_idx + 1:]:
        if not any(cell.strip() for cell in row):
            break
        ativo = (row[0].strip().upper() == "TRUE")
        if not ativo:
            continue
        try:
            cfg = {
                "label": row[1].strip(),
                "host": row[2].strip(),
                "user": row[3].strip(),
                "password": row[4].strip(),
                "since_days": int(row[5].strip() or "90"),
                "max_emails": int(row[6].strip() or "1000"),
            }
            if cfg["host"] and cfg["user"] and cfg["password"]:
                configs.append(cfg)
        except Exception:
            continue

    print(f"[CFG] Contas ativas encontradas: {len(configs)}")
    for c in configs:
        print(f"     - {c['label']} ({c['user']} @ {c['host']}) / since_days={c['since_days']} / max={c['max_emails']}")
    return configs


# -------------- processamento --------------

def _fetch_message_peek(mail: imaplib.IMAP4_SSL, eid: bytes):
    r"""
    Faz FETCH com PEEK para não alterar \Seen.
    """
    typ, msg_data = mail.fetch(eid, "(BODY.PEEK[])")  # <- PEEK evita marcar como lido
    if typ != "OK":
        return None
    raw = msg_data[0][1]
    return email.message_from_bytes(raw)


def _process_single_mailbox(cfg):
    label = cfg["label"]; host = cfg["host"]; user = cfg["user"]; password = cfg["password"]
    since_days = cfg["since_days"]; max_emails = cfg["max_emails"]

    print(f"\n📥 [START] Caixa: {label}  <{user}>  host={host}")
    start = time.time()

    # respeita /stop
    if STOP_FLAG.get("active"):
        print(f"[STOP] Interrompido antes de conectar: {label}")
        return {"conta": label, "emails": 0, "anexos": 0, "valor_total": 0.0}

    mail = imaplib.IMAP4_SSL(host)
    mail.login(user, password)

    # SOMENTE LEITURA: não altera flags
    mail.select("INBOX", readonly=True)

    date_limit = (datetime.now() - timedelta(days=int(since_days))).strftime("%d-%b-%Y")
    typ, search_data = mail.search(None, f'(SINCE "{date_limit}")')
    email_ids = search_data[0].split() if typ == "OK" else []
    print(f"[INFO] {label}: {len(email_ids)} e-mails encontrados desde {date_limit} (limite {max_emails}).")

    total_emails = 0
    total_valid_atts = 0
    total_value = 0.0

    for eid in reversed(email_ids[-int(max_emails):]):
        if STOP_FLAG.get("active"):
            print(f"[STOP] Interrompido durante processamento da caixa: {label}")
            break

        try:
            msg = _fetch_message_peek(mail, eid)
            if msg is None:
                continue

            subject = _decode_mime(msg.get("Subject"))
            from_ = _decode_mime(msg.get("From"))
            date_fmt = _parse_date_header(msg.get("Date"))

            valid_attachments = 0

            for part in msg.walk():
                if STOP_FLAG.get("active"):
                    print(f"[STOP] Interrompido dentro do loop de anexos: {label}")
                    break
                if part.get_content_maintype() == "multipart":
                    continue
                filename = part.get_filename()
                if not filename:
                    continue
                filename = _decode_mime(filename)

                if not _is_relevant_attachment(filename):
                    continue

                file_bytes = part.get_payload(decode=True)

                # sobe Drive
                drive_link = upload_to_drive(filename, file_bytes)

                # extrai (v2)
                extracted = extract_financial_data_v2(filename, file_bytes)

                # completa metadados
                extracted["Conta"] = label
                extracted["Remetente"] = from_
                extracted["Assunto"] = subject
                extracted["Data"] = date_fmt
                extracted["Nome do Arquivo"] = filename
                extracted["Link"] = drive_link

                append_financial_entry(extracted)

                total_value += extracted.get("ValorNum", 0.0)
                valid_attachments += 1
                total_valid_atts += 1

            append_email_entry({
                "Conta": label,
                "Remetente": from_,
                "Assunto": subject,
                "Data": date_fmt,
                "Anexos Válidos": valid_attachments
            })
            total_emails += 1

        except Exception as e:
            print(f"[ERRO] {label}: falha no e-mail {eid}: {e}")
            traceback.print_exc()

    try:
        mail.logout()
    except Exception:
        pass

    elapsed = time.time() - start
    print(f"✅ [DONE] {label} | emails={total_emails} | anexos_validos={total_valid_atts} | valor_total=R$ {total_value:,.2f} | tempo={elapsed:,.1f}s".replace(",", "X").replace(".", ",").replace("X", "."))

    return {"conta": label, "emails": total_emails, "anexos": total_valid_atts, "valor_total": round(total_value, 2)}


def process_all_mailboxes_v2():
    print("\n================ INÍCIO DA EXECUÇÃO (Email Financeiro v2) ================\n")
    exec_start = time.time()
    resumo = []
    error_happened = False

    try:
        configs = _load_mailbox_configs_from_sheet()
        if not configs:
            print("[WARN] Nenhuma conta ativa.")
        for cfg in configs:
            if STOP_FLAG.get("active"):
                print("⏹️ Execução interrompida por /stop.")
                break
            try:
                r = _process_single_mailbox(cfg)
                resumo.append(r)
            except Exception as e:
                error_happened = True
                print(f"[ERRO] Caixa {cfg.get('label')}: {e}")
                traceback.print_exc()
    except Exception as e:
        error_happened = True
        print(f"[ERRO] Configurações: {e}")
        traceback.print_exc()

    # registra Runs (parcial ou completo)
    try:
        log_run_summary(resumo)
    except Exception as e:
        print(f"[ERRO] Runs: {e}")

    elapsed = time.time() - exec_start
    print("\n================ FIM DA EXECUÇÃO (v2) =================")
    print(f"Resumo: {resumo}")
    print("Tempo total: {:.1f}s".format(elapsed))
    print("Status:", "CONCLUÍDO COM ALERTAS" if error_happened else "CONCLUÍDO COM SUCESSO")
    if STOP_FLAG.get("active"):
        STOP_FLAG["active"] = False
    print("=================================================\n")
