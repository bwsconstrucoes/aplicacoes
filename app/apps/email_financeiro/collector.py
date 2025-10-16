# -*- coding: utf-8 -*-
"""
Coleta e-mails de múltiplas contas IMAP, identifica anexos financeiros
(PDFs, XMLs), envia pro Google Drive e grava resultados no Google Sheets.
"""

import imaplib
import email
import re
from datetime import datetime, timedelta
from email.header import decode_header
import io
import os
from base64 import b64decode

from .parser_financeiro import extract_financial_data_from_attachment
from .sheets_utils import (
    append_email_entry,
    append_financial_entry,
    log_run_summary
)
from .gdrive_utils import upload_to_drive


def clean_text(text):
    if not text:
        return ""
    text = str(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def decode_mime(text):
    if not text:
        return ""
    parts = decode_header(text)
    decoded = ""
    for s, enc in parts:
        if isinstance(s, bytes):
            decoded += s.decode(enc or "utf-8", errors="ignore")
        else:
            decoded += s
    return decoded


def is_relevant_attachment(filename):
    if not filename:
        return False
    name = filename.lower()
    if not (name.endswith(".pdf") or name.endswith(".xml")):
        return False
    ignore_words = [
        "assinatura", "signature", "comprovante", "relatorio",
        "extrato", "planilha", "recibo", "manual", "foto"
    ]
    if any(w in name for w in ignore_words):
        return False
    keywords = [
        "boleto", "nota", "nf", "nfe", "fatura", "duplicata", "danfe", "cobranca"
    ]
    return any(k in name for k in keywords)


def process_mailbox(label, host, user, password, since_days=90, max_emails=1000):
    print(f"\n📥 Processando caixa: {user}")
    mail = imaplib.IMAP4_SSL(host)
    mail.login(user, password)
    mail.select("INBOX")

    date_limit = (datetime.now() - timedelta(days=int(since_days))).strftime("%d-%b-%Y")
    _, search_data = mail.search(None, f'(SINCE "{date_limit}")')

    email_ids = search_data[0].split()
    print(f"  Total encontrado: {len(email_ids)} emails (limite {max_emails})")

    total_emails = 0
    total_attachments = 0
    total_value = 0.0

    for eid in reversed(email_ids[-int(max_emails):]):
        _, msg_data = mail.fetch(eid, "(RFC822)")
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        subject = decode_mime(msg.get("Subject"))
        from_ = decode_mime(msg.get("From"))
        date_ = msg.get("Date")
        date_fmt = ""
        try:
            date_fmt = datetime.strptime(date_[:25], "%a, %d %b %Y %H:%M:%S").strftime("%Y-%m-%d")
        except Exception:
            date_fmt = datetime.now().strftime("%Y-%m-%d")

        valid_attachments = 0
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            filename = part.get_filename()
            if filename:
                filename = decode_mime(filename)
                if is_relevant_attachment(filename):
                    file_bytes = part.get_payload(decode=True)
                    drive_link = upload_to_drive(filename, file_bytes)

                    extracted = extract_financial_data_from_attachment(filename, file_bytes)
                    extracted["Conta"] = label
                    extracted["Remetente"] = from_
                    extracted["Assunto"] = subject
                    extracted["Data"] = date_fmt
                    extracted["Nome do Arquivo"] = filename
                    extracted["Link"] = drive_link

                    append_financial_entry(extracted)
                    total_value += extracted.get("ValorNum", 0.0)
                    valid_attachments += 1
                    total_attachments += 1

        append_email_entry({
            "Conta": label,
            "Remetente": from_,
            "Assunto": subject,
            "Data": date_fmt,
            "Anexos Válidos": valid_attachments
        })
        total_emails += 1

    mail.logout()

    return {
        "conta": label,
        "emails": total_emails,
        "anexos": total_attachments,
        "valor_total": round(total_value, 2),
    }


def process_all_mailboxes():
    """
    Lê a aba Configurações e executa todas as caixas marcadas como TRUE.
    """
    import gspread
    from google.oauth2.service_account import Credentials

    creds_json = b64decode(os.getenv("GOOGLE_CREDENTIALS_BASE64")).decode("utf-8")
    creds = Credentials.from_service_account_info(eval(creds_json))
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    ws = sh.worksheet("Configurações")

    data = ws.get_all_records()
    configs = []
    current = None
    for row in data:
        if not row.get("ativo") and not row.get("label"):
            continue
        if row.get("ativo", "").strip().upper() == "TRUE":
            current = {
                "label": row.get("label", ""),
                "imap_host": row.get("imap_host", ""),
                "imap_user": row.get("imap_user", ""),
                "imap_password": row.get("imap_password", ""),
                "search_since_days": row.get("search_since_days", 90),
                "max_emails_per_box": row.get("max_emails_per_box", 1000)
            }
            configs.append(current)

    resumo = []
    for cfg in configs:
        result = process_mailbox(**cfg)
        resumo.append(result)

    log_run_summary(resumo)
    print("\n✅ Execução finalizada com sucesso.\n")
