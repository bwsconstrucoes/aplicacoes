# -*- coding: utf-8 -*-
"""
telegram_bot.py — Blueprint Flask: autocadastro de ID Telegram (BWS)
====================================================================
Versão 2 — aba centralizada "TelegramID"

Desenho:
  - As abas "Dados Documentos" e "Colaborador (Cartões)" são SOMENTE CONSULTA
    (validação de telefone/CPF + obtenção do nome). Nada é gravado nelas.
  - Todo cadastro confirmado é gravado na aba "TelegramID" (criada
    automaticamente se não existir), que é a base oficial de envio:
        A=Data cadastro | B=CPF | C=Telefone | D=ID Telegram
        E=Nome | F=Origem (TELEFONE/CPF) | G=Observação
  - Falhas e tentativas não localizadas vão para a aba "Pendências Telegram".

Fluxo do bot:
  1. /start -> boas-vindas + botão "📱 Compartilhar meu número"
  2. Contato compartilhado (número verificado pelo Telegram):
       - telefone encontrado nas bases -> grava na TelegramID e confirma
         chamando a pessoa pelo NOME da base
       - não encontrado -> pede o CPF (fallback)
  3. CPF digitado (11 dígitos):
       - encontrado -> grava na TelegramID (telefone real do Telegram,
         observação de divergência) e confirma pelo nome
       - não encontrado -> orienta procurar o RH + log em pendências
  4. chat_id já cadastrado -> "Você já está cadastrado" (atualiza telefone
     se tiver mudado)

Variáveis de ambiente (Render -> Environment):
  TELEGRAM_BOT_TOKEN        -> token do @BotFather
  TELEGRAM_SECRET_TOKEN     -> string aleatória sua (validação do webhook)
  GOOGLE_CREDENTIALS_BASE64 -> já existente no monorepo (Service Account)

Registro no app principal:
  from telegram_bot import telegram_bp
  app.register_blueprint(telegram_bp)

Vinculação do webhook (uma vez, após o deploy):
  https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://SEU-APP.onrender.com/telegram/webhook&secret_token=<SECRET>&allowed_updates=["message"]
"""

import base64
import json
import os
import re
import threading
import time
import unicodedata
from datetime import datetime

import requests
from flask import Blueprint, jsonify, request

import gspread
from google.oauth2.service_account import Credentials

telegram_bp = Blueprint("telegram_bot", __name__)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_SECRET = os.environ.get("TELEGRAM_SECRET_TOKEN", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# --- Bases de CONSULTA (somente leitura) ---

# Planilha 1 — Dados Documentos
PLAN1_ID = "1fqi4QUOVGUd1_4Gg4vK5qP_IMOSgFaw8DD9MDgmM3vo"
PLAN1_ABA = "Dados Documentos"
PLAN1_COL_CPF = 1        # A
PLAN1_COL_NOME = 5       # E
PLAN1_COL_TEL = 23       # W  (padrão: "81 98790-7522")

# Planilha 2 — Colaborador (Cartões)
PLAN2_ID = "1C7MWQmr5uFGWuJ18osUNDapiojVXzQ_GxMMDQqxPsBk"
PLAN2_ABA = "Colaborador (Cartões)"
PLAN2_COL_CPF = 4        # D
PLAN2_COL_NOME = 1       # A
PLAN2_COL_TEL = 2        # B  (padrão: "5583999213393")

# --- Base de GRAVAÇÃO (cadastros confirmados) ---
# Fica na Planilha 2 (a mesma da aba "Colaborador (Cartões)").
# Para mudar de planilha, altere apenas PLAN_TGID_ID.
PLAN_TGID_ID = PLAN2_ID
ABA_TGID = "TelegramID"
TGID_CABECALHO = ["Data cadastro", "CPF", "Telefone", "ID Telegram",
                  "Nome", "Origem", "Observação"]
# Índices (1-based) da aba TelegramID
TGID_COL_DATA = 1
TGID_COL_CPF = 2
TGID_COL_TEL = 3
TGID_COL_ID = 4
TGID_COL_NOME = 5
TGID_COL_ORIGEM = 6
TGID_COL_OBS = 7

# --- Aba de pendências/auditoria (mesma planilha da TelegramID) ---
ABA_PENDENCIAS = "Pendências Telegram"

# --- Mensagens ---
MSG_BOAS_VINDAS = (
    "👷 *BWS Construções — Cadastro de Notificações*\n\n"
    "Para receber os avisos da BWS por aqui (pagamentos, ponto, comunicados), "
    "toque no botão abaixo para confirmar seu número de telefone."
)
MSG_SUCESSO = (
    "✅ Cadastro confirmado, *{nome}*!\n\n"
    "A partir de agora você receberá os avisos da BWS por este chat."
)
MSG_JA_CADASTRADO = (
    "👍 Você já está cadastrado, *{nome}*.\n\n"
    "Os avisos da BWS chegam automaticamente por aqui.\n\n"
    "O que você precisa?\n\n"
    "*1* — 📄 Contracheque"
)
MSG_MENU = (
    "Olá, *{nome}*! O que você precisa?\n\n"
    "*1* — 📄 Contracheque\n\n"
    "_Os avisos da BWS chegam automaticamente por aqui._"
)
MSG_NAO_ENCONTRADO_TEL = (
    "⚠️ Não encontrei seu número na base da BWS.\n\n"
    "Se o seu Telegram usa um chip diferente do que está no seu cadastro, "
    "me envie o seu *CPF* (somente números) que eu tento localizar por ele."
)
MSG_CPF_NAO_ENCONTRADO = (
    "❌ CPF não localizado na base da BWS.\n\n"
    "Procure o RH/Financeiro para verificar seu cadastro. "
    "Sua tentativa foi registrada para análise."
)
MSG_INSTRUCAO = (
    "Para se cadastrar, toque no botão *📱 Compartilhar meu número* abaixo.\n"
    "Se o botão não aparecer, envie /start."
)
MSG_INDISPONIVEL = (
    "⚠️ Sistema temporariamente indisponível para consulta.\n"
    "Tente novamente em alguns minutos, por favor."
)

# ---------------------------------------------------------------------------
# Google Sheets (singleton + cache de worksheets)
# ---------------------------------------------------------------------------

_GC = None
_WS_CACHE = {}


def _gspread_client():
    global _GC
    if _GC is None:
        raw = base64.b64decode(os.environ["GOOGLE_CREDENTIALS_BASE64"])
        info = json.loads(raw)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        _GC = gspread.authorize(creds)
    return _GC


def _abrir_aba(sheet_id, nome_aba, criar_se_faltar=False, cabecalho=None,
               cols=10):
    chave = f"{sheet_id}::{nome_aba}"
    if chave not in _WS_CACHE:
        sh = _gspread_client().open_by_key(sheet_id)
        try:
            ws = sh.worksheet(nome_aba)
        except gspread.exceptions.WorksheetNotFound:
            if not criar_se_faltar:
                raise
            ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=cols)
            if cabecalho:
                ws.append_row(cabecalho, value_input_option="USER_ENTERED")
        _WS_CACHE[chave] = ws
    return _WS_CACHE[chave]


def _aba_telegram_id():
    return _abrir_aba(PLAN_TGID_ID, ABA_TGID, criar_se_faltar=True,
                      cabecalho=TGID_CABECALHO, cols=len(TGID_CABECALHO))


def _aba_pendencias():
    return _abrir_aba(
        PLAN_TGID_ID, ABA_PENDENCIAS, criar_se_faltar=True,
        cabecalho=["Data/Hora", "Evento", "Telefone Telegram",
                   "CPF informado", "chat_id", "Nome Telegram", "Observação"],
        cols=7,
    )


def _log_pendencia(evento, telefone="", cpf="", chat_id="", nome="", obs=""):
    try:
        _aba_pendencias().append_row(
            [datetime.now().strftime("%d/%m/%Y %H:%M:%S"), evento, telefone,
             cpf, str(chat_id), nome, obs],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        print(f"[telegram_bot] Falha ao logar pendência: {e}")


# ---------------------------------------------------------------------------
# Normalização de telefone/CPF
# ---------------------------------------------------------------------------

def _variantes_telefone(bruto):
    """
    Variantes canônicas de um telefone BR para matching robusto.
    Canônico = DDD + número (sem o 55), com e sem o nono dígito.
    Cobre: "+5585998322004", "5583999213393", "81 98790-7522".
    """
    d = re.sub(r"\D", "", bruto or "")
    if not d:
        return set()
    if d.startswith("55") and len(d) >= 12:
        d = d[2:]
    if len(d) < 10:
        return {d}
    variantes = {d}
    if len(d) == 11 and d[2] == "9":
        variantes.add(d[:2] + d[3:])          # sem o nono dígito
    elif len(d) == 10:
        variantes.add(d[:2] + "9" + d[2:])    # com o nono dígito
    return variantes


def _telefone_canonico(bruto):
    """Forma única para gravação: 55 + DDD + número (ex.: 5585998322004)."""
    d = re.sub(r"\D", "", bruto or "")
    if d.startswith("55") and len(d) >= 12:
        return d
    return "55" + d if d else ""


def _normalizar_cpf(bruto):
    d = re.sub(r"\D", "", bruto or "")
    return d.zfill(11) if len(d) in (10, 11) else d


# ---------------------------------------------------------------------------
# Consulta nas bases (somente leitura)
# ---------------------------------------------------------------------------

def _ler_valores_com_retry(sheet_id, nome_aba, tentativas=3):
    """
    Leitura de aba com retry + backoff progressivo. Cobre 429 (rate limit),
    5xx do Google e falhas de rede. Lança RuntimeError após esgotar.
    """
    ultimo_erro = None
    for t in range(tentativas):
        try:
            ws = _abrir_aba(sheet_id, nome_aba)
            return ws.get_all_values()
        except gspread.exceptions.APIError as e:
            ultimo_erro = e
            print(f"[telegram_bot] APIError lendo {nome_aba} "
                  f"({t + 1}/{tentativas}): {e}")
            time.sleep(2 * (t + 1))  # 2s, 4s, 6s
        except Exception as e:
            ultimo_erro = e
            print(f"[telegram_bot] Erro lendo {nome_aba} "
                  f"({t + 1}/{tentativas}): {e}")
            time.sleep(1 + t)
    raise RuntimeError(f"falha lendo {nome_aba} após {tentativas} "
                       f"tentativas: {ultimo_erro}")


_BASES_CONSULTA = [
    {"id": PLAN1_ID, "aba": PLAN1_ABA, "col_cpf": PLAN1_COL_CPF,
     "col_nome": PLAN1_COL_NOME, "col_tel": PLAN1_COL_TEL,
     "rotulo": "Dados Documentos"},
    {"id": PLAN2_ID, "aba": PLAN2_ABA, "col_cpf": PLAN2_COL_CPF,
     "col_nome": PLAN2_COL_NOME, "col_tel": PLAN2_COL_TEL,
     "rotulo": "Colaborador (Cartões)"},
]


def _consultar_bases(telefone=None, cpf=None):
    """
    Procura por telefone OU cpf nas bases de consulta (sem gravar nada).
    Retorna (pessoa, leitura_ok):
      pessoa     -> dict {"cpf", "nome", "telefone_base", "base"} ou None
      leitura_ok -> False se alguma base ficou inacessível (distingue
                    "não encontrado" de "sistema indisponível")
    """
    alvo_tel = _variantes_telefone(telefone) if telefone else set()
    alvo_cpf = _normalizar_cpf(cpf) if cpf else None
    leitura_ok = True

    for cfg in _BASES_CONSULTA:
        try:
            valores = _ler_valores_com_retry(cfg["id"], cfg["aba"])
        except Exception as e:
            print(f"[telegram_bot] Base {cfg['rotulo']} indisponível: {e}")
            leitura_ok = False
            continue

        for linha in valores[1:]:  # pula cabeçalho
            tel_cel = linha[cfg["col_tel"] - 1] if len(linha) >= cfg["col_tel"] else ""
            cpf_cel = linha[cfg["col_cpf"] - 1] if len(linha) >= cfg["col_cpf"] else ""
            nome_cel = linha[cfg["col_nome"] - 1] if len(linha) >= cfg["col_nome"] else ""

            bate = False
            if alvo_tel and (_variantes_telefone(tel_cel) & alvo_tel):
                bate = True
            if alvo_cpf and _normalizar_cpf(cpf_cel) == alvo_cpf:
                bate = True

            if bate:
                return {
                    "cpf": _normalizar_cpf(cpf_cel),
                    "nome": (nome_cel or "").strip(),
                    "telefone_base": tel_cel,
                    "base": cfg["rotulo"],
                }, True
    return None, leitura_ok


# ---------------------------------------------------------------------------
# Gravação na aba TelegramID
# ---------------------------------------------------------------------------

def _buscar_cadastro_por_chat_id(chat_id):
    """Retorna (linha_index, nome) se o chat_id já existe na TelegramID."""
    try:
        _aba_telegram_id()  # garante que a aba existe
        valores = _ler_valores_com_retry(PLAN_TGID_ID, ABA_TGID)
    except Exception as e:
        print(f"[telegram_bot] Erro lendo TelegramID: {e}")
        return None, None
    alvo = str(chat_id)
    for i, linha in enumerate(valores[1:], start=2):
        if len(linha) >= TGID_COL_ID and linha[TGID_COL_ID - 1].strip() == alvo:
            nome = linha[TGID_COL_NOME - 1] if len(linha) >= TGID_COL_NOME else ""
            return i, nome
    return None, None


def _gravar_cadastro(chat_id, cpf, telefone, nome, origem, obs=""):
    """
    Grava/atualiza o cadastro na aba TelegramID.
    Se o chat_id já existir, atualiza a linha; senão, insere nova.
    Retry com backoff para APIError (rate limit).
    """
    linha_existente, _ = _buscar_cadastro_por_chat_id(chat_id)
    dados = [
        datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        cpf,
        _telefone_canonico(telefone),
        str(chat_id),
        nome,
        origem,
        obs,
    ]
    ws = _aba_telegram_id()
    for tentativa in range(3):
        try:
            if linha_existente:
                ws.update(f"A{linha_existente}:G{linha_existente}", [dados],
                          value_input_option="USER_ENTERED")
            else:
                ws.append_row(dados, value_input_option="USER_ENTERED")
            return True
        except gspread.exceptions.APIError as e:
            print(f"[telegram_bot] APIError gravando TelegramID "
                  f"({tentativa + 1}/3): {e}")
            time.sleep(2 * (tentativa + 1))
        except Exception as e:
            print(f"[telegram_bot] Erro gravando TelegramID: {e}")
            time.sleep(1)
    return False


# ---------------------------------------------------------------------------
# Envio de mensagens ao Telegram (retry + checagem de status)
# ---------------------------------------------------------------------------

def _tg_enviar(chat_id, texto, teclado=None, remover_teclado=False):
    payload = {
        "chat_id": chat_id,
        "text": texto,
        "parse_mode": "Markdown",
        # CRÍTICO: sem preview de link. O crawler do Telegram faz um GET real
        # na URL para montar o preview — em links de ação de um clique (ex.:
        # validação/anuência de SP), esse GET DISPARAVA o webhook antes do
        # usuário clicar (incidente 2026-07-16).
        "disable_web_page_preview": True,
        "link_preview_options": {"is_disabled": True},
    }
    if teclado:
        payload["reply_markup"] = json.dumps(teclado)
    elif remover_teclado:
        payload["reply_markup"] = json.dumps({"remove_keyboard": True})

    for tentativa in range(3):
        try:
            r = requests.post(f"{TELEGRAM_API}/sendMessage", json=payload,
                              timeout=15)
            if r.status_code == 200:
                return True
            print(f"[telegram_bot] sendMessage HTTP {r.status_code}: "
                  f"{r.text[:300]}")
            if (r.status_code == 400 and payload.get("parse_mode")
                    and "parse" in r.text.lower()):
                # Markdown desbalanceado na mensagem (ex.: * ou [ sem par):
                # reenvia imediatamente como texto puro em vez de falhar
                payload.pop("parse_mode", None)
                continue
            if r.status_code == 429:
                espera = r.json().get("parameters", {}).get("retry_after", 3)
                time.sleep(espera)
            else:
                time.sleep(1 + tentativa)
        except requests.RequestException as e:
            print(f"[telegram_bot] Erro de rede sendMessage: {e}")
            time.sleep(1 + tentativa)
    return False


def _teclado_contato():
    return {
        "keyboard": [[{"text": "📱 Compartilhar meu número",
                       "request_contact": True}]],
        "resize_keyboard": True,
        "one_time_keyboard": True,
    }


def _primeiro_nome(nome_completo, fallback="colaborador"):
    nome = (nome_completo or "").strip()
    return nome.split()[0].title() if nome else fallback


# ---------------------------------------------------------------------------
# ASSISTENTE — Contracheque (porte do chatbot WhatsApp p/ o Telegram)
# Reaproveita os módulos agnósticos de canal do app.apps.chatbot:
#   session (estados/TTL), auth (níveis master/normal), sheets_cache
#   (colaboradores + desligados), paystub (recorte do PDF por CPF) e
#   dropbox_client (download da folha).
# Diferença p/ o WhatsApp: aqui NÃO se pede CPF — a identidade já foi
# verificada no cadastro da TelegramID (número validado pelo Telegram).
# ---------------------------------------------------------------------------

PALAVRAS_RESET = {"sair", "cancelar", "reset", "reiniciar", "inicio"}
PALAVRAS_CONTRACHEQUE = ("contracheque", "holerite", "salario", "folha")


def _norm_txt(texto):
    t = unicodedata.normalize("NFD", (texto or "").strip().lower())
    return "".join(ch for ch in t if unicodedata.category(ch) != "Mn")


def _registro_por_chat_id(chat_id):
    """Dados do cadastro na aba TelegramID: {cpf, telefone, nome} ou None."""
    alvo = str(chat_id)
    try:
        linhas = _linhas_telegram_id()
    except Exception as e:
        print(f"[telegram_bot] assistente: TelegramID indisponível: {e}")
        return None
    for linha in linhas[1:]:
        id_cel = linha[TGID_COL_ID - 1] if len(linha) >= TGID_COL_ID else ""
        if id_cel.strip() == alvo:
            return {
                "cpf": linha[TGID_COL_CPF - 1] if len(linha) >= TGID_COL_CPF else "",
                "telefone": linha[TGID_COL_TEL - 1] if len(linha) >= TGID_COL_TEL else "",
                "nome": linha[TGID_COL_NOME - 1] if len(linha) >= TGID_COL_NOME else "",
            }
    return None


def _assist_modulos():
    """Import lazy dos módulos do chatbot (não derruba o cadastro se falhar)."""
    from app.apps.chatbot import session as chat_session
    from app.apps.chatbot import auth as chat_auth
    from app.apps.chatbot import sheets_cache as chat_sheets
    from app.apps.chatbot import paystub as chat_paystub
    from app.apps.chatbot import dropbox_client as chat_dropbox
    return chat_session, chat_auth, chat_sheets, chat_paystub, chat_dropbox


def _assist_prompt_competencia(chat_id):
    agora = datetime.now()
    mes_atual = f"{agora.month:02d}/{agora.year}"
    mes_ant = (f"{agora.month - 1:02d}/{agora.year}" if agora.month > 1
               else f"12/{agora.year - 1}")
    _tg_enviar(chat_id,
               "📄 *Solicitação de Contracheque*\n\n"
               "Para qual competência?\n\n"
               f"• `{mes_atual}` — mês atual\n"
               f"• `{mes_ant}` — mês anterior\n\n"
               "Digite no formato *MM/AAAA*:")


def _assist_iniciar(chat_id, chave, nome_tg):
    """Pedido de contracheque: valida cadastro/desligamento e abre a sessão."""
    chat_session, chat_auth, chat_sheets, _, _ = _assist_modulos()

    registro = _registro_por_chat_id(chat_id)
    if not registro or not registro.get("cpf"):
        _tg_enviar(chat_id,
                   "Para solicitar o contracheque, primeiro faça seu "
                   "cadastro tocando no botão abaixo. 👇",
                   teclado=_teclado_contato())
        return

    colaborador = chat_sheets.buscar_por_cpf(registro["cpf"]) or {
        "cpf": _normalizar_cpf(registro["cpf"]),
        "nome": registro.get("nome", ""),
        "tel": registro.get("telefone", ""),
        "status": "",
    }
    if chat_sheets.esta_desligado(colaborador):
        _tg_enviar(chat_id,
                   "ℹ️ Você não faz mais parte do quadro de colaboradores.\n\n"
                   "Entre em contato com o *RH* para mais informações.")
        return

    # MASTER pode consultar o contracheque de qualquer CPF
    if chat_auth.is_master(registro.get("telefone", "")):
        chat_session.criar_session(chave, "AGUARDANDO_CPF_MASTER")
        chat_session.atualizar_session(
            chave, estado="AGUARDANDO_CPF_MASTER",
            dados_extra={"colaborador": colaborador,
                         "telefone_master": registro.get("telefone", "")})
        _tg_enviar(chat_id,
                   "🔑 *Acesso master*\n\n"
                   "Envie o *CPF* do colaborador (somente números)\n"
                   "ou `meu` para o seu próprio contracheque:")
        return

    chat_session.criar_session(chave, "AGUARDANDO_COMPETENCIA")
    chat_session.atualizar_session(chave, estado="AGUARDANDO_COMPETENCIA",
                                   dados_extra={"colaborador": colaborador})
    _assist_prompt_competencia(chat_id)


def _assist_cpf_master(chat_id, chave, texto, sess):
    chat_session, chat_auth, chat_sheets, _, _ = _assist_modulos()
    dados = sess.get("dados", {})
    texto_norm = _norm_txt(texto)

    if texto_norm == "meu":
        colaborador = dados.get("colaborador")
    else:
        cpf = re.sub(r"\D", "", texto)
        if len(cpf) != 11:
            _tg_enviar(chat_id,
                       "❌ CPF inválido. Informe os *11 dígitos*, "
                       "ou `meu` para o seu próprio.")
            return
        r = chat_auth.validar_acesso(cpf, dados.get("telefone_master", ""))
        if not r["ok"]:
            if r.get("motivo") == "desligado":
                _tg_enviar(chat_id,
                           "ℹ️ Esse CPF consta como *desligado*. "
                           "Envie outro CPF ou `sair`.")
            else:
                _tg_enviar(chat_id,
                           "❌ CPF não encontrado na base. "
                           "Envie outro CPF ou `sair`.")
            return
        colaborador = r["colaborador"]

    chat_session.atualizar_session(chave, estado="AGUARDANDO_COMPETENCIA",
                                   dados_extra={"colaborador": colaborador})
    _assist_prompt_competencia(chat_id)


def _assist_competencia(chat_id, chave, texto, sess):
    chat_session, _, _, chat_paystub, _ = _assist_modulos()
    colaborador = sess.get("dados", {}).get("colaborador")
    if not colaborador:
        chat_session.destruir_session(chave)
        _tg_enviar(chat_id, "Sessão perdida. Envie *contracheque* para recomeçar.")
        return

    resultado = chat_paystub.parsear_competencia(texto)
    if not resultado:
        _tg_enviar(chat_id,
                   "❌ Período não reconhecido.\n\n"
                   "Informe no formato *MM/AAAA*. Exemplo: `04/2025`")
        return

    ano, mes = resultado
    chat_session.destruir_session(chave)
    _tg_enviar(chat_id,
               f"⏳ Buscando contracheque de *{mes:02d}/{ano}*...\nAguarde. 🔍")
    threading.Thread(target=_assist_entregar,
                     args=(chat_id, dict(colaborador), ano, mes),
                     daemon=True).start()


def _assist_entregar(chat_id, colaborador, ano, mes):
    """Roda em background: baixa a folha, recorta o contracheque e envia."""
    try:
        _, _, _, chat_paystub, chat_dropbox = _assist_modulos()
        cpf = colaborador.get("cpf", "")
        nome = colaborador.get("nome", "Colaborador")

        pdf_bytes = chat_dropbox.baixar_pdf(ano, mes)
        if not pdf_bytes:
            _tg_enviar(chat_id,
                       f"❌ Contracheque de *{mes:02d}/{ano}* não encontrado.\n\n"
                       "Verifique o período ou entre em contato com o RH.")
            return

        recorte = chat_paystub.extrair_contracheque_por_cpf(pdf_bytes, cpf)
        if not recorte:
            _tg_enviar(chat_id,
                       f"❌ Seu contracheque de *{mes:02d}/{ano}* não foi "
                       "localizado no arquivo.\n\nEntre em contato com o RH.")
            return

        nome_arquivo = (f"Contracheque_{mes:02d}_{ano}_"
                        f"{nome[:15].replace(' ', '_')}.pdf")
        legenda = f"📄 Contracheque {mes:02d}/{ano} — {nome.title()}"
        b64 = base64.b64encode(recorte).decode("ascii")

        ok, detalhe = _tg_enviar_arquivo(chat_id, "documento",
                                         conteudo_b64=b64,
                                         nome_arquivo=nome_arquivo,
                                         legenda=legenda)
        if ok:
            _tg_enviar(chat_id,
                       "✅ Contracheque enviado!\n\n"
                       "Para outra solicitação, envie *1* ou `contracheque`.")
        else:
            print(f"[telegram_bot] contracheque: falha no envio: {detalhe}")
            _tg_enviar(chat_id, "❌ Erro ao enviar o arquivo. Tente novamente.")
    except Exception as e:
        print(f"[telegram_bot] contracheque: erro inesperado: {e}")
        _tg_enviar(chat_id, "❌ Erro ao processar. Tente novamente em instantes.")
    finally:
        # PDF da folha pode ser grande — solta a memória imediatamente
        try:
            del pdf_bytes, recorte, b64
        except Exception:
            pass


def _assistente_processar_texto(chat_id, texto, nome_tg):
    """Camada de conversa do assistente. True = mensagem consumida."""
    try:
        chat_session, _, _, _, _ = _assist_modulos()
    except Exception as e:
        print(f"[telegram_bot] assistente indisponível (import): {e}")
        return False

    chave = str(chat_id)
    texto_norm = _norm_txt(texto)

    # Palavras de reset — só consomem se houver sessão ativa
    if texto_norm in PALAVRAS_RESET:
        if chat_session.get_session(chave):
            chat_session.destruir_session(chave)
            _tg_enviar(chat_id, "👋 Sessão encerrada. Até logo!")
            return True
        return False

    sess = chat_session.get_session(chave)
    if sess:
        estado = sess.get("estado", "")
        if estado == "AGUARDANDO_CPF_MASTER":
            _assist_cpf_master(chat_id, chave, texto, sess)
            return True
        if estado == "AGUARDANDO_COMPETENCIA":
            _assist_competencia(chat_id, chave, texto, sess)
            return True
        chat_session.destruir_session(chave)  # estado desconhecido

    if texto_norm == "1" or any(p in texto_norm for p in PALAVRAS_CONTRACHEQUE):
        _assist_iniciar(chat_id, chave, nome_tg)
        return True

    return False


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

@telegram_bp.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    # Validação de origem: header enviado pelo Telegram em todo POST
    if TELEGRAM_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != TELEGRAM_SECRET:
            return jsonify({"ok": False, "erro": "não autorizado"}), 403

    update = request.get_json(silent=True) or {}
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return jsonify({"ok": True})  # ignora updates sem mensagem

    chat_id = msg.get("chat", {}).get("id")
    de = msg.get("from", {}) or {}
    nome_tg = " ".join(filter(None, [de.get("first_name"),
                                     de.get("last_name")])) or "colaborador"

    # ---- 1) Contato compartilhado (caminho principal) ----
    contato = msg.get("contact")
    if contato:
        # Segurança: só aceita o contato do PRÓPRIO usuário (não da agenda)
        if contato.get("user_id") != de.get("id"):
            _tg_enviar(chat_id,
                       "⚠️ Compartilhe o *seu próprio* contato usando o "
                       "botão, não um contato da agenda.",
                       teclado=_teclado_contato())
            return jsonify({"ok": True})

        telefone = contato.get("phone_number", "")

        # Já cadastrado? Atualiza telefone e confirma pelo nome.
        linha, nome_cad = _buscar_cadastro_por_chat_id(chat_id)
        if linha:
            _tg_enviar(chat_id,
                       MSG_JA_CADASTRADO.format(
                           nome=_primeiro_nome(nome_cad, nome_tg)),
                       remover_teclado=True)
            return jsonify({"ok": True})

        pessoa, leitura_ok = _consultar_bases(telefone=telefone)
        if pessoa:
            ok = _gravar_cadastro(
                chat_id, pessoa["cpf"], telefone, pessoa["nome"],
                origem="TELEFONE",
                obs=f"Validado por telefone na base {pessoa['base']}",
            )
            if ok:
                _tg_enviar(chat_id,
                           MSG_SUCESSO.format(
                               nome=_primeiro_nome(pessoa["nome"], nome_tg)),
                           remover_teclado=True)
            else:
                _tg_enviar(chat_id,
                           "⚠️ Encontrei seu cadastro, mas houve um erro ao "
                           "gravar. Tente novamente em instantes com /start.",
                           remover_teclado=True)
                _log_pendencia("ERRO_GRAVACAO", telefone=telefone,
                               cpf=pessoa["cpf"], chat_id=chat_id,
                               nome=nome_tg)
        elif not leitura_ok:
            _tg_enviar(chat_id, MSG_INDISPONIVEL, teclado=_teclado_contato())
            _log_pendencia("ERRO_LEITURA_BASES", telefone=telefone,
                           chat_id=chat_id, nome=nome_tg,
                           obs="Sheets indisponível durante cadastro por telefone")
        else:
            _tg_enviar(chat_id, MSG_NAO_ENCONTRADO_TEL, remover_teclado=True)
            _log_pendencia("TELEFONE_NAO_ENCONTRADO", telefone=telefone,
                           chat_id=chat_id, nome=nome_tg,
                           obs="Aguardando CPF do usuário")
        return jsonify({"ok": True})

    # ---- 2) Texto ----
    texto = (msg.get("text") or "").strip()

    if texto.startswith("/start"):
        linha, nome_cad = _buscar_cadastro_por_chat_id(chat_id)
        if linha:
            _tg_enviar(chat_id,
                       MSG_JA_CADASTRADO.format(
                           nome=_primeiro_nome(nome_cad, nome_tg)),
                       remover_teclado=True)
        else:
            _tg_enviar(chat_id, MSG_BOAS_VINDAS, teclado=_teclado_contato())
        return jsonify({"ok": True})

    # ---- 2.5) Assistente (contracheque e sessões ativas) ----
    if _assistente_processar_texto(chat_id, texto, nome_tg):
        return jsonify({"ok": True})

    # ---- 3) Fallback por CPF (11 dígitos) ----
    apenas_digitos = re.sub(r"\D", "", texto)
    if len(apenas_digitos) == 11 and not texto.startswith("/"):
        linha, nome_cad = _buscar_cadastro_por_chat_id(chat_id)
        if linha:
            _tg_enviar(chat_id,
                       MSG_JA_CADASTRADO.format(
                           nome=_primeiro_nome(nome_cad, nome_tg)),
                       remover_teclado=True)
            return jsonify({"ok": True})

        pessoa, leitura_ok = _consultar_bases(cpf=apenas_digitos)
        if pessoa:
            ok = _gravar_cadastro(
                chat_id, pessoa["cpf"], "", pessoa["nome"],
                origem="CPF",
                obs=(f"Validado por CPF na base {pessoa['base']}; telefone "
                     f"do Telegram diverge do cadastrado "
                     f"({pessoa['telefone_base']}) — RH conferir"),
            )
            if ok:
                _tg_enviar(chat_id,
                           MSG_SUCESSO.format(
                               nome=_primeiro_nome(pessoa["nome"], nome_tg)),
                           remover_teclado=True)
                _log_pendencia("CADASTRO_CPF_DIVERGENTE",
                               cpf=pessoa["cpf"], chat_id=chat_id,
                               nome=nome_tg,
                               obs=f"Telefone da base: {pessoa['telefone_base']}")
            else:
                _tg_enviar(chat_id,
                           "⚠️ Encontrei seu cadastro, mas houve um erro ao "
                           "gravar. Tente novamente em instantes.",
                           remover_teclado=True)
                _log_pendencia("ERRO_GRAVACAO", cpf=pessoa["cpf"],
                               chat_id=chat_id, nome=nome_tg)
        elif not leitura_ok:
            _tg_enviar(chat_id, MSG_INDISPONIVEL, remover_teclado=True)
            _log_pendencia("ERRO_LEITURA_BASES", cpf=apenas_digitos,
                           chat_id=chat_id, nome=nome_tg,
                           obs="Sheets indisponível durante cadastro por CPF")
        else:
            _tg_enviar(chat_id, MSG_CPF_NAO_ENCONTRADO, remover_teclado=True)
            _log_pendencia("CPF_NAO_ENCONTRADO", cpf=apenas_digitos,
                           chat_id=chat_id, nome=nome_tg)
        return jsonify({"ok": True})

    # ---- 4) Qualquer outra coisa ----
    registro = _registro_por_chat_id(chat_id)
    if registro:
        _tg_enviar(chat_id,
                   MSG_MENU.format(
                       nome=_primeiro_nome(registro.get("nome"), nome_tg)))
    else:
        _tg_enviar(chat_id, MSG_INSTRUCAO, teclado=_teclado_contato())
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# ENVIO DE MENSAGENS (consumido pelo Make e pelas aplicações Python)
# ---------------------------------------------------------------------------
#
# POST /telegram/enviar
# Header obrigatório:  X-Api-Key: <TELEGRAM_SECRET_TOKEN>
# Body JSON — identifique o destinatário por UM dos campos:
#   "chat_id":  123456789                (direto, sem lookup)
#   "telefone": "5585999999999"          (lookup na aba TelegramID)
#   "cpf":      "12345678901"            (lookup na aba TelegramID)
#
# Conteúdo:
#   "mensagem":      "texto"             (texto puro OU legenda do arquivo)
#   "arquivo_url":   "https://..."       (PDF/imagem público — o Telegram baixa)
#   "arquivo_base64": "<base64>"         (alternativa p/ arquivo não público)
#   "nome_arquivo":  "comprovante.pdf"   (obrigatório com arquivo_base64)
#   "tipo":          "documento"|"imagem" (opcional; inferido pela extensão)
#
# Respostas:
#   200 {"ok": true,  "chat_id": ..., "enviado": "texto|documento|imagem"}
#   404 {"ok": false, "erro": "nao_cadastrado"}   -> destinatário sem TelegramID
#   400/403/502 com "erro" descritivo
# ---------------------------------------------------------------------------

_EXT_IMAGEM = (".jpg", ".jpeg", ".png", ".webp", ".gif")
_CACHE_TGID = {"linhas": None, "ts": 0.0}
_CACHE_TGID_TTL = 120  # segundos — novo cadastro fica visível p/ envio em até 2 min


def _linhas_telegram_id():
    """
    Lê a aba TelegramID com cache em memória (TTL).
    Fallback: se o Sheets estiver indisponível e existir cache antigo,
    usa o cache antigo — o envio não para por instabilidade momentânea.
    """
    agora = time.time()
    if _CACHE_TGID["linhas"] is not None and (agora - _CACHE_TGID["ts"]) < _CACHE_TGID_TTL:
        return _CACHE_TGID["linhas"]
    try:
        _aba_telegram_id()  # garante que a aba existe
        linhas = _ler_valores_com_retry(PLAN_TGID_ID, ABA_TGID)
    except Exception as e:
        if _CACHE_TGID["linhas"] is not None:
            print(f"[telegram_bot] Sheets indisponível; usando cache antigo "
                  f"da TelegramID: {e}")
            return _CACHE_TGID["linhas"]
        raise
    _CACHE_TGID["linhas"] = linhas
    _CACHE_TGID["ts"] = agora
    return linhas


def _lookup_chat_id(telefone=None, cpf=None):
    """Procura o chat_id na aba TelegramID por telefone ou CPF."""
    alvo_tel = _variantes_telefone(telefone) if telefone else set()
    alvo_cpf = _normalizar_cpf(cpf) if cpf else None
    for linha in _linhas_telegram_id()[1:]:
        tel_cel = linha[TGID_COL_TEL - 1] if len(linha) >= TGID_COL_TEL else ""
        cpf_cel = linha[TGID_COL_CPF - 1] if len(linha) >= TGID_COL_CPF else ""
        id_cel = linha[TGID_COL_ID - 1] if len(linha) >= TGID_COL_ID else ""
        if not id_cel.strip():
            continue
        if alvo_tel and (_variantes_telefone(tel_cel) & alvo_tel):
            return id_cel.strip()
        if alvo_cpf and _normalizar_cpf(cpf_cel) == alvo_cpf:
            return id_cel.strip()
    return None


def _destravar_escapes(texto):
    """Converte sequências de escape LITERAIS (\\n, \\t, \\r) em caracteres
    reais. Necessário porque o corpo form-urlencoded do Make entrega o texto
    cru — o "\\n" que no JSON virava quebra de linha chega como barra + n."""
    if not texto or "\\" not in texto:
        return texto
    return (texto.replace("\\r\\n", "\n")
                 .replace("\\n", "\n")
                 .replace("\\t", "\t")
                 .replace("\\r", ""))


# --- Aviso de migração via WhatsApp p/ destinatário sem cadastro -----------
# Quando um envio chega p/ alguém sem ID Telegram, em vez de falhar (404 e
# cenário interrompido no Make), o servidor manda um alerta pelo WhatsApp
# (Z-API) cobrando o cadastro — e responde 200 p/ o fluxo seguir.
# Throttle de 6h por telefone p/ não bombardear a pessoa em lotes.

TELEGRAM_BOT_LINK = os.environ.get("TELEGRAM_BOT_LINK",
                                   "t.me/bwsconstrucoesbotbot")
_AVISOS_WA = {}           # telefone -> timestamp do último aviso
_AVISOS_WA_JANELA = 6 * 3600

MSG_AVISO_CADASTRO_WA = (
    "⚠️ *BWS Construções — Aviso importante*\n\n"
    "Você deveria ter recebido *agora* uma mensagem da BWS, mas ela "
    "*NÃO foi entregue* porque você ainda não fez o cadastro no *Telegram* "
    "— o novo canal oficial de avisos da empresa.\n\n"
    "Para não perder as próximas mensagens (pagamentos, comprovantes e "
    "comunicados), faça o cadastro agora. Leva 1 minuto:\n\n"
    "1️⃣ Abra: {link}\n"
    "2️⃣ Toque em *INICIAR*\n"
    "3️⃣ Toque em *📱 Compartilhar meu número*\n\n"
    "_Não tem o Telegram? Instale pela loja de aplicativos, crie sua conta "
    "e depois abra o link._\n\n"
    "Dúvidas? Fale com o RH/Financeiro."
)


def _wa_aviso_cadastro(telefone):
    """Envia o alerta de migração via Z-API. Retorna dict com o resultado."""
    tel = re.sub(r"\D", "", telefone or "")
    if not tel:
        return {"ok": False, "detalhe": "sem telefone para avisar"}

    agora = time.time()
    ultimo = _AVISOS_WA.get(tel, 0)
    if agora - ultimo < _AVISOS_WA_JANELA:
        return {"ok": None, "detalhe": "aviso já enviado nas últimas 6h"}

    instancia = os.environ.get("ZAPI_INSTANCE_ID", "")
    token = os.environ.get("ZAPI_INSTANCE_TOKEN", "")
    client_token = os.environ.get("ZAPI_CLIENT_TOKEN", "")
    if not instancia or not token:
        return {"ok": False,
                "detalhe": "ZAPI_INSTANCE_ID/ZAPI_INSTANCE_TOKEN não configurados"}

    url = (f"https://api.z-api.io/instances/{instancia}"
           f"/token/{token}/send-text")
    headers = {"Client-Token": client_token} if client_token else {}
    mensagem = MSG_AVISO_CADASTRO_WA.format(link=TELEGRAM_BOT_LINK)
    try:
        r = requests.post(url, json={"phone": tel, "message": mensagem},
                          headers=headers, timeout=20)
        if r.status_code == 200:
            _AVISOS_WA[tel] = agora
            return {"ok": True, "detalhe": "aviso enviado via WhatsApp"}
        return {"ok": False,
                "detalhe": f"Z-API HTTP {r.status_code}: {r.text[:150]}"}
    except requests.RequestException as e:
        return {"ok": False, "detalhe": f"erro de rede Z-API: {e}"}


def _inferir_tipo(nome_ou_url):
    base = (nome_ou_url or "").lower().split("?")[0]
    return "imagem" if base.endswith(_EXT_IMAGEM) else "documento"


def _baixar_arquivo_url(url):
    """
    Baixa o arquivo da URL PELO SERVIDOR, seguindo redirects — necessário
    para Google Drive/Dropbox, que o Telegram não consegue baixar sozinho.
    Retorna (bytes, nome_sugerido) em caso de sucesso ou (None, erro).
    """
    try:
        r = requests.get(url, timeout=60, allow_redirects=True, stream=True)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code} ao baixar a URL"
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype:
            return None, ("a URL retornou uma página HTML e não um arquivo — "
                          "verifique se o compartilhamento é público "
                          "('qualquer pessoa com o link') e se o link é de "
                          "download direto")
        limite = 48 * 1024 * 1024  # limite de upload p/ bots no Telegram: 50 MB
        partes = []
        total = 0
        for chunk in r.iter_content(chunk_size=256 * 1024):
            partes.append(chunk)
            total += len(chunk)
            if total > limite:
                return None, "arquivo excede 48 MB (limite do Telegram p/ bots)"
        conteudo = b"".join(partes)
        if not conteudo:
            return None, "a URL retornou conteúdo vazio"
        # Nome sugerido: Content-Disposition ou último trecho da URL
        nome = ""
        cd = r.headers.get("Content-Disposition", "")
        m = re.search(r'filename="?([^";]+)"?', cd)
        if m:
            nome = m.group(1).strip()
        if not nome:
            nome = url.split("?")[0].rstrip("/").split("/")[-1] or "arquivo"
        return conteudo, nome
    except requests.RequestException as e:
        return None, f"erro de rede ao baixar a URL: {e}"


def _tg_enviar_arquivo(chat_id, tipo, url=None, conteudo_b64=None,
                       conteudo_bytes=None, nome_arquivo=None, legenda=""):
    """
    Envia documento ou imagem. Três origens possíveis:
      url            -> baixada pelo servidor (compatível com Drive/Dropbox)
      conteudo_bytes -> arquivo binário recebido por upload multipart
      conteudo_b64   -> string base64
    Retorna (ok: bool, detalhe: str).
    """
    metodo = "sendPhoto" if tipo == "imagem" else "sendDocument"
    campo = "photo" if tipo == "imagem" else "document"
    legenda = (legenda or "")[:1024]  # limite de caption do Telegram

    if url:
        conteudo, info = _baixar_arquivo_url(url)
        if conteudo is None:
            return False, info
        if not nome_arquivo:
            nome_arquivo = info
    elif conteudo_bytes is not None:
        conteudo = conteudo_bytes
        if not conteudo:
            return False, "arquivo enviado por upload está vazio"
    else:
        try:
            conteudo = base64.b64decode(conteudo_b64)
        except Exception as e:
            return False, f"base64 inválido: {e}"
        if not conteudo:
            return False, "arquivo_base64 vazio"

    data = {"chat_id": chat_id}
    if legenda:
        data["caption"] = legenda
        data["parse_mode"] = "Markdown"

    for tentativa in range(3):
        try:
            r = requests.post(
                f"{TELEGRAM_API}/{metodo}", data=data,
                files={campo: (nome_arquivo or "arquivo", conteudo)},
                timeout=120,
            )
            if r.status_code == 200:
                return True, "ok"
            print(f"[telegram_bot] {metodo} HTTP {r.status_code}: "
                  f"{r.text[:300]}")
            if (r.status_code == 400 and data.get("parse_mode")
                    and "parse" in r.text.lower()):
                # Markdown inválido na legenda: reenvia como texto puro
                data.pop("parse_mode", None)
                continue
            if r.status_code == 429:
                espera = r.json().get("parameters", {}).get("retry_after", 3)
                time.sleep(espera)
            elif 400 <= r.status_code < 500:
                # erro definitivo (ex.: chat bloqueou o bot, formato inválido)
                return False, r.text[:300]
            else:
                time.sleep(1 + tentativa)
        except requests.RequestException as e:
            print(f"[telegram_bot] Erro de rede {metodo}: {e}")
            time.sleep(1 + tentativa)
    return False, "falha após 3 tentativas"


@telegram_bp.route("/telegram/enviar", methods=["POST"])
def telegram_enviar():
    # Autenticação: mesma chave secreta do webhook, no header X-Api-Key
    if not TELEGRAM_SECRET or request.headers.get("X-Api-Key") != TELEGRAM_SECRET:
        return jsonify({"ok": False, "erro": "não autorizado"}), 403

    dados = request.get_json(silent=True)
    if not dados:
        # Fallback: aceita application/x-www-form-urlencoded e multipart.
        # No Make, isso evita quebrar o corpo quando a mensagem contém
        # aspas duplas ou quebras de linha (que invalidam JSON montado em Raw).
        dados = request.form.to_dict() if request.form else {}
    chat_id = dados.get("chat_id")
    telefone = (dados.get("telefone") or "").strip()
    cpf = (dados.get("cpf") or "").strip()
    mensagem = _destravar_escapes((dados.get("mensagem") or "").strip())
    arquivo_url = (dados.get("arquivo_url") or "").strip()
    arquivo_b64 = (dados.get("arquivo_base64") or "").strip()
    nome_arquivo = (dados.get("nome_arquivo") or "").strip()
    tipo = (dados.get("tipo") or "").strip().lower()

    # Upload multipart (campo de arquivo "arquivo" — recomendado no Make)
    arquivo_upload = request.files.get("arquivo")
    bytes_upload = None
    if arquivo_upload:
        bytes_upload = arquivo_upload.read()
        if not nome_arquivo:
            nome_arquivo = arquivo_upload.filename or ""

    if not chat_id and not telefone and not cpf:
        return jsonify({"ok": False,
                        "erro": "informe chat_id, telefone ou cpf"}), 400
    if not mensagem and not arquivo_url and not arquivo_b64 \
            and bytes_upload is None:
        return jsonify({"ok": False,
                        "erro": "informe mensagem e/ou arquivo"}), 400
    if arquivo_b64 and not nome_arquivo:
        return jsonify({"ok": False,
                        "erro": "nome_arquivo é obrigatório com "
                                "arquivo_base64"}), 400

    # Lookup na TelegramID quando não veio chat_id direto
    if not chat_id:
        try:
            chat_id = _lookup_chat_id(telefone=telefone or None,
                                      cpf=cpf or None)
        except Exception as e:
            return jsonify({"ok": False, "erro": "base_indisponivel",
                            "detalhe": f"Google Sheets inacessível: "
                                       f"{str(e)[:200]}"}), 503
        if not chat_id:
            # Sem cadastro: avisa via WhatsApp e responde 200 p/ o cenário
            # do Make NÃO ser interrompido (migração forçada, sem quebra).
            tel_aviso = telefone
            if not tel_aviso and cpf:
                try:
                    pessoa, _ = _consultar_bases(cpf=cpf)
                    tel_aviso = (pessoa or {}).get("telefone_base", "")
                except Exception:
                    tel_aviso = ""
            aviso = _wa_aviso_cadastro(tel_aviso)
            _log_pendencia("ENVIO_SEM_CADASTRO", telefone=telefone, cpf=cpf,
                           obs=f"Mensagem não entregue; aviso WhatsApp: "
                               f"{aviso.get('detalhe', '')}")
            return jsonify({"ok": False, "erro": "nao_cadastrado",
                            "detalhe": "destinatário sem ID Telegram na aba "
                                       "TelegramID; aviso de cadastro "
                                       "disparado via WhatsApp",
                            "aviso_whatsapp": aviso}), 200

    # Envio
    if arquivo_url or arquivo_b64 or bytes_upload is not None:
        if not tipo:
            tipo = _inferir_tipo(nome_arquivo or arquivo_url)
        ok, detalhe = _tg_enviar_arquivo(
            chat_id, tipo, url=arquivo_url or None,
            conteudo_b64=arquivo_b64 or None,
            conteudo_bytes=bytes_upload,
            nome_arquivo=nome_arquivo or None, legenda=mensagem,
        )
        if ok:
            return jsonify({"ok": True, "chat_id": chat_id, "enviado": tipo})
        _log_pendencia("FALHA_ENVIO_ARQUIVO", telefone=telefone, cpf=cpf,
                       chat_id=chat_id, obs=detalhe[:200])
        return jsonify({"ok": False, "erro": "falha_envio_arquivo",
                        "detalhe": detalhe}), 502

    if _tg_enviar(chat_id, mensagem):
        return jsonify({"ok": True, "chat_id": chat_id, "enviado": "texto"})
    _log_pendencia("FALHA_ENVIO_TEXTO", telefone=telefone, cpf=cpf,
                   chat_id=chat_id, obs=mensagem[:200])
    return jsonify({"ok": False, "erro": "falha_envio_texto"}), 502


# Rota de saúde (testa se o blueprint subiu e se o token está configurado)
@telegram_bp.route("/telegram/health", methods=["GET"])
def telegram_health():
    return jsonify({"ok": True, "servico": "telegram_bot",
                    "token_configurado": bool(TELEGRAM_TOKEN)})