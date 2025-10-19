# -*- coding: utf-8 -*-
"""
Coletor financeiro:
- Lê múltiplas contas IMAP a partir da aba "Configurações"
- Filtra anexos financeiros relevantes (PDF/XML)
- Envia anexos ao Google Drive (link público)
- Extrai dados (PDF/XML/OCR opcional) e grava no Google Sheets:
  - Emails (1 linha por e-mail processado, com contagem de anexos válidos)
  - Relatório (1 linha por anexo válido)
  - Runs (resumo da execução)
Logs detalhados via stdout (Render Logs).
"""

import imaplib
import email
import re
import time
from datetime import datetime, timedelta
from email.header import decode_header
import io
import os
import traceback
from base64 import b64decode

# STOP_FLAG (vem de routes.py)
from .state import STOP_FLAG

# Dependências internas existentes
from .parser_financeiro import extract_financial_data_from_attachment
from .sheets_utils import (
    append_email_entry,
    append_financial_entry,
    log_run_summary
)
from .gdrive_utils import upload_to_drive


# ------------------------ Utilidades ------------------------

def _clean_spaces(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


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
    return decoded


def _is_relevant_attachment(filename):
    """
    Regras simples: somente .pdf/.xml e com palavras-chave de financeiro,
    exclui termos que usualmente não são títulos a pagar.
    """
    if not filename:
        return False
    name = filename.lower()

    # extensões
    if not (name.endswith(".pdf") or name.endswith(".xml")):
        return False

    # descartar arquivos comuns não financeiros
    ignore_words = [
        "assinatura", "signature", "comprovante", "relatorio", "relatório",
        "extrato", "planilha", "recibo", "manual", "foto", "imagem",
        "contrato", "proposta", "orcamento", "orçamento", "pedido", "curriculo"
    ]
    if any(w in name for w in ignore_words):
        return False

    # focar em financeiro
    keywords = [
        "boleto", "nota", "nf", "nfe", "danfe", "duplicata", "fatura", "cobranca", "cobrança"
    ]
    return any(k in name for k in keywords)


def _parse_date_header(date_header):
    """
    Tenta normalizar a data do cabeçalho do e-mail em YYYY-MM-DD.
    """
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


# ------------------------ Leitura de Configurações ------------------------

def _load_mailbox_configs_from_sheet():
    """
    Lê a aba "Configurações" e encontra a tabela de contas:
    colunas esperadas:
    ativo | label | imap_host | imap_user | imap_password | search_since_days | max_emails_per_box
    Retorna lista de dicts somente com ativo=TRUE.
    """
    import gspread
    from google.oauth2.service_account import Credentials
    import json

    print("[CFG] Lendo configurações do Google Sheets...")
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
        print("[CFG] Cabeçalho de contas não encontrado na aba Configurações. Verifique a planilha.")
        return []

    configs = []
    for row in values[header_idx + 1:]:
        if not any(cell.strip() for cell in row):
            break
        try:
            ativo = (row[0].strip().upper() == "TRUE")
        except Exception:
            ativo = False
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
            if cfg["host"] and cfg["user"]:
                configs.append(cfg)
        except Exception:
            continue

    print(f"[CFG] Contas ativas encontradas: {len(configs)}")
    for c in configs:
        print(f"     - {c['label']} ({c['user']} @ {c['host']}) / since_days={c['since_days']} / max={c['max_emails']}")
    return configs


# ------------------------ Processamento de uma caixa ------------------------

def _process_single_mailbox(label, host, user, password, since_days=90, max_emails=1000):
    print("\n [START] Caixa:", label, f"<{user}>", "host=", host)
    start = time.time()

    # Respeita /stop ANTES de conectar
    if STOP_FLAG.get("active"):
        print(f"[STOP] Interrompido antes de conectar: {label}")
        return {"conta": label, "emails": 0, "anexos": 0, "valor_total": 0.0}

    mail = imaplib.IMAP4_SSL(host)
    mail.login(user, password)

    # Respeita /stop após login
    if STOP_FLAG.get("active"):
        print(f"[STOP] Interrompido após login: {label}")
        try:
            mail.logout()
        except Exception:
            pass
        return {"conta": label, "emails": 0, "anexos": 0, "valor_total": 0.0}

    # SOMENTE LEITURA: não altera flags
    mail.select("INBOX", readonly=True)

    date_limit = (datetime.now() - timedelta(days=int(since_days))).strftime("%d-%b-%Y")
    typ, search_data = mail.search(None, f'(SINCE "{date_limit}")')

    if typ != "OK":
        print(f"[WARN] Search falhou para {user}: {typ}")
        email_ids = []
    else:
        email_ids = search_data[0].split()

    print(f"[INFO] {label}: {len(email_ids)} e-mails encontrados desde {date_limit} (limite {max_emails}).")

    total_emails = 0
    total_valid_atts = 0
    total_value = 0.0

    # Percorre do mais recente para o mais antigo, respeitando o limite
    for eid in reversed(email_ids[-int(max_emails):]):
        # Respeita /stop durante o loop
        if STOP_FLAG.get("active"):
            print(f"[STOP] Interrompido durante processamento da caixa: {label}")
            break

        try:
            # FETCH usando BODY.PEEK[] para NÃO marcar como lido
            typ, msg_data = mail.fetch(eid, "(BODY.PEEK[])")
            if typ != "OK" or not msg_data or not msg_data[0]:
                continue
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject = _decode_mime(msg.get("Subject"))
            from_ = _decode_mime(msg.get("From"))
            date_fmt = _parse_date_header(msg.get("Date"))

            valid_attachments = 0

            for part in msg.walk():
                # Respeita /stop dentro dos anexos
                if STOP_FLAG.get("active"):
                    print(f"[STOP] Interrompido dentro do loop de anexos: {label}")
                    break

                if part.get_content_maintype() == "multipart":
                    continue
                filename = part.get_filename()
                if not filename:
                    continue
                filename = _decode_mime(filename)

                if _is_relevant_attachment(filename):
                    file_bytes = part.get_payload(decode=True)

                    # 1) sobe pro Drive
                    drive_link = upload_to_drive(filename, file_bytes)

                    # 2) extrai dados financeiros
                    extracted = extract_financial_data_from_attachment(filename, file_bytes)

                    # 3) completa metadados e envia ao Relatório
                    extracted["Conta"] = label
                    extracted["Remetente"] = from_
                    extracted["Assunto"] = subject
                    extracted["Data"] = date_fmt
                    extracted["Nome do Arquivo"] = filename
                    extracted["Link"] = drive_link

                    append_financial_entry(extracted)

                    # 4) soma valor numérico (se houver)
                    total_value += extracted.get("ValorNum", 0.0)
                    valid_attachments += 1
                    total_valid_atts += 1

            # Registro básico do e-mail na aba Emails
            append_email_entry({
                "Conta": label,
                "Remetente": from_,
                "Assunto": subject,
                "Data": date_fmt,
                "Anexos Válidos": valid_attachments
            })

            total_emails += 1

        except Exception as e:
            print(f"[ERRO] Falha ao processar e-mail {eid} em {label}: {e}")
            traceback.print_exc()

    try:
        mail.logout()
    except Exception:
        pass

    elapsed = time.time() - start
    print(f" [DONE] Caixa: {label} | emails={total_emails} | anexos_validos={total_valid_atts} | valor_total=R$ {total_value:,.2f} | tempo={elapsed:,.1f}s".replace(",", "X").replace(".", ",").replace("X", "."))

    return {
        "conta": label,
        "emails": total_emails,
        "anexos": total_valid_atts,
        "valor_total": round(total_value, 2),
    }


# ------------------------ Orquestração de todas as caixas ------------------------

def process_all_mailboxes():
    """
    Lê as contas na aba Configurações e executa todas com ativo=TRUE.
    Loga um resumo consolidado na aba Runs (via log_run_summary).
    (Mantida a lógica; apenas adicionadas checagens de parada.)
    """
    print("\n================ INÍCIO DA EXECUÇÃO (Email Financeiro) ================\n")
    exec_start = time.time()
    resumo = []
    error_happened = False

    try:
        configs = _load_mailbox_configs_from_sheet()
        if not configs:
            print("[WARN] Nenhuma conta ativa encontrada na aba Configurações.")

        for cfg in configs:
            # Respeita /stop entre as caixas
            if STOP_FLAG.get("active"):
                print(" Execução interrompida por /stop antes de processar a próxima caixa.")
                break

            try:
                r = _process_single_mailbox(
                    label=cfg["label"],
                    host=cfg["host"],
                    user=cfg["user"],
                    password=cfg["password"],
                    since_days=cfg["since_days"],
                    max_emails=cfg["max_emails"],
                )
                resumo.append(r)
            except Exception as e:
                error_happened = True
                print(f"[ERRO] Caixa {cfg['label']} falhou: {e}")
                traceback.print_exc()

    except Exception as e:
        error_happened = True
        print(f"[ERRO] Falha ao carregar configurações: {e}")
        traceback.print_exc()

    # Registro do resumo (mesmo parcial, em caso de parada)
    try:
        log_run_summary(resumo)
    except Exception as e:
        print(f"[ERRO] Falha ao registrar resumo (Runs): {e}")
        traceback.print_exc()

    elapsed = time.time() - exec_start
    print("\n================ FIM DA EXECUÇÃO =================")
    print(f"Resumo: {resumo}")
    print("Tempo total: {:.1f}s".format(elapsed))
    if error_happened:
        print("Status: CONCLUÍDO COM ALERTAS (ver logs acima)")
    else:
        print("Status: CONCLUÍDO COM SUCESSO")

    # Reseta a flag p/ próximas execuções
    if STOP_FLAG.get("active"):
        STOP_FLAG["active"] = False

    print("=================================================\n")
