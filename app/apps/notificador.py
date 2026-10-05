# -*- coding: utf-8 -*-
"""
notificador.py — Envio unificado de notificações (Telegram + WhatsApp)
======================================================================

Módulo compartilhado do monorepo. Os demais blueprints importam e chamam:

    from app.apps.notificador import notificar

    resultado = notificar(
        telefone="5585999999999",
        mensagem="Seu pagamento foi agendado para 20/07.",
    )
    # resultado = {"telegram": {"ok": True, ...}, "whatsapp": {"ok": True, ...}}

Com arquivo:

    notificar(
        telefone="5585999999999",
        mensagem="Segue seu comprovante.",
        arquivo_url="https://.../comprovante.pdf",   # OU arquivo_base64=...
        nome_arquivo="comprovante.pdf",
    )

Escolhendo canais/política:

    notificar(..., canais=("telegram",))                     # só Telegram
    notificar(..., canais=("whatsapp",))                     # só WhatsApp
    notificar(..., canais=("telegram", "whatsapp"))          # ambos (padrão)
    notificar(..., politica="fallback")                      # Telegram primeiro;
                                                             # WhatsApp só se falhar

Canal Telegram: chamada interna direta às funções do blueprint telegram
(mesmo processo — sem overhead de HTTP). O destinatário precisa estar na
aba TelegramID; caso contrário retorna {"ok": False, "erro": "nao_cadastrado"}.

Liga/desliga por canal SEM mexer em código (env vars no Render):
  NOTIFICAR_TELEGRAM = "1" (padrão) | "0" desativa o canal Telegram
  NOTIFICAR_WHATSAPP = "1" (padrão) | "0" desativa o canal WhatsApp
Canal desativado retorna {"ok": None, "detalhe": "canal desativado"} —
não conta como falha, apenas não envia.

Liga/desliga por FINALIDADE (desde 05/10/2026): o chamador pode dizer para
que serve o aviso — notificar(..., finalidade="ponto") — e aí a variável
NOTIFICAR_<CANAL>_<FINALIDADE> (ex.: NOTIFICAR_WHATSAPP_PONTO) decide
sozinha para aquela finalidade. Se ela não existir, vale a geral. É o que
permite deixar o WhatsApp desligado para tudo (o Z-API bloqueia quando o
volume sobe) e ligado só para o ponto, sem publicar código.

Canal WhatsApp: HTTP direto para a API do Z-API (api.z-api.io).
Variáveis de ambiente:
  ZAPI_INSTANCE_ID   -> id da instância (o mesmo dos teus cenários)
  ZAPI_API_TOKEN     -> token da instância. ZAPI_INSTANCE_TOKEN é aceito
                        como apelido (foi o nome usado aqui até 05/10/2026;
                        o Render só tem ZAPI_API_TOKEN)
  ZAPI_CLIENT_TOKEN  -> Account Security Token do painel Z-API
                        (header Client-Token; deixe vazio se a conta não exigir)
"""

import os
import re
import time

import requests

# Funções internas do blueprint do Telegram (mesmo processo)
from app.apps.telegram.telegram_bot import (
    _inferir_tipo,
    _lookup_chat_id,
    _tg_enviar,
    _tg_enviar_arquivo,
)

# ---------------------------------------------------------------------------
# Configuração WhatsApp (Z-API direto)
# ---------------------------------------------------------------------------

WA_BASE = "https://api.z-api.io"


def _wa_credenciais():
    """Lê as credenciais do Z-API a cada chamada (trocar variável no Render
    passa a valer sem reiniciar). Aceita os dois nomes do token."""
    return {
        "instancia": os.environ.get("ZAPI_INSTANCE_ID", "").strip(),
        "token": (os.environ.get("ZAPI_API_TOKEN", "").strip()
                  or os.environ.get("ZAPI_INSTANCE_TOKEN", "").strip()),
        "client_token": os.environ.get("ZAPI_CLIENT_TOKEN", "").strip(),
    }


def _wa_url(sufixo):
    c = _wa_credenciais()
    return f"{WA_BASE}/instances/{c['instancia']}/token/{c['token']}/{sufixo}"


def _wa_configurado():
    c = _wa_credenciais()
    return bool(c["instancia"] and c["token"])


def whatsapp_configurado():
    """Há credenciais do Z-API no ambiente? (não olha o liga/desliga)"""
    return _wa_configurado()


def _wa_post(url, payload, timeout=60):
    """POST ao Z-API com retry e checagem de status."""
    client_token = _wa_credenciais()["client_token"]
    headers = {"Client-Token": client_token} if client_token else {}
    for tentativa in range(3):
        try:
            r = requests.post(url, json=payload, headers=headers,
                              timeout=timeout)
            if r.status_code == 200:
                return True, "ok"
            print(f"[notificador] WhatsApp HTTP {r.status_code}: "
                  f"{r.text[:300]}")
            if 400 <= r.status_code < 500 and r.status_code != 429:
                return False, f"HTTP {r.status_code}: {r.text[:200]}"
            time.sleep(2 * (tentativa + 1))
        except requests.RequestException as e:
            print(f"[notificador] Erro de rede WhatsApp: {e}")
            time.sleep(1 + tentativa)
    return False, "falha após 3 tentativas"


def _wa_enviar_texto(telefone, mensagem):
    if not _wa_configurado():
        return {"ok": False, "erro": "whatsapp_nao_configurado",
                "detalhe": "defina ZAPI_INSTANCE_ID/ZAPI_API_TOKEN"}
    ok, detalhe = _wa_post(_wa_url("send-text"),
                           {"phone": telefone, "message": mensagem})
    return {"ok": ok, "detalhe": detalhe}


def _wa_enviar_arquivo(telefone, tipo, arquivo_url=None, arquivo_base64=None,
                       nome_arquivo=None, legenda=""):
    if not _wa_configurado():
        return {"ok": False, "erro": "whatsapp_nao_configurado",
                "detalhe": "defina ZAPI_INSTANCE_ID/ZAPI_API_TOKEN"}

    if tipo == "imagem":
        payload = {"phone": telefone,
                   "image": arquivo_url or f"data:image/jpeg;base64,{arquivo_base64}"}
        if legenda:
            payload["caption"] = legenda
        ok, detalhe = _wa_post(_wa_url("send-image"), payload)
    else:
        ext = "pdf"
        if nome_arquivo and "." in nome_arquivo:
            ext = nome_arquivo.rsplit(".", 1)[-1].lower()
        payload = {"phone": telefone,
                   "document": (arquivo_url or
                                f"data:application/{ext};base64,{arquivo_base64}"),
                   "fileName": nome_arquivo or f"arquivo.{ext}"}
        if legenda:
            payload["caption"] = legenda
        ok, detalhe = _wa_post(_wa_url(f"send-document/{ext}"), payload)
    return {"ok": ok, "detalhe": detalhe}


# ---------------------------------------------------------------------------
# Canal Telegram (chamadas internas, sem HTTP)
# ---------------------------------------------------------------------------

def _tg_notificar(telefone=None, cpf=None, chat_id=None, mensagem="",
                  arquivo_url=None, arquivo_base64=None, nome_arquivo=None,
                  tipo=None):
    if not chat_id:
        try:
            chat_id = _lookup_chat_id(telefone=telefone or None,
                                      cpf=cpf or None)
        except Exception as e:
            return {"ok": False, "erro": "base_indisponivel",
                    "detalhe": str(e)[:200]}
        if not chat_id:
            return {"ok": False, "erro": "nao_cadastrado",
                    "detalhe": "destinatário sem ID Telegram na aba TelegramID"}

    if arquivo_url or arquivo_base64:
        if not tipo:
            tipo = _inferir_tipo(nome_arquivo or arquivo_url or "")
        ok, detalhe = _tg_enviar_arquivo(
            chat_id, tipo, url=arquivo_url or None,
            conteudo_b64=arquivo_base64 or None,
            nome_arquivo=nome_arquivo or None, legenda=mensagem,
        )
        return {"ok": ok, "chat_id": chat_id, "detalhe": detalhe}

    ok = _tg_enviar(chat_id, mensagem)
    return {"ok": ok, "chat_id": chat_id,
            "detalhe": "ok" if ok else "falha no envio"}


# ---------------------------------------------------------------------------
# Liga/desliga — DUAS camadas, e a ordem importa
#
#  1. A MENSAGERIA (app/apps/mensageria, tela "Mensagens" do ERP): quando o
#     chamador diz a FINALIDADE (o tipo da mensagem, ex. "ponto.qr") e as
#     tabelas dela existem, é a POLÍTICA gravada na tela que decide os canais,
#     com a chave geral "WhatsApp ligado" e o teto por hora/dia. Tudo é
#     registrado lá. É a gestão que o dono pediu em 05/10/2026.
#  2. As VARIÁVEIS DE AMBIENTE (NOTIFICAR_<CANAL> e NOTIFICAR_<CANAL>_<FINALIDADE>):
#     valem quando a mensageria não alcança — sem finalidade, sem banco, antes
#     de a migração 083 ser aplicada. Rede de segurança, não gestão.
# ---------------------------------------------------------------------------

_DESLIGADO = ("0", "false", "nao", "não", "off")


def _nome_finalidade(finalidade):
    """'ponto' -> 'PONTO'; 'ponto.qr' -> 'PONTO_QR'."""
    return re.sub(r"[^A-Z0-9]+", "_", str(finalidade or "").strip().upper()).strip("_")


def _canal_ativo(canal, finalidade=None):
    if canal not in ("telegram", "whatsapp"):
        return True
    geral = f"NOTIFICAR_{canal.upper()}"
    nome = _nome_finalidade(finalidade)
    if nome:
        especifico = os.environ.get(f"{geral}_{nome}")
        if especifico is not None and especifico.strip() != "":
            return especifico.strip().lower() not in _DESLIGADO
    return os.environ.get(geral, "1").strip().lower() not in _DESLIGADO


def _mensageria():
    """Os módulos da mensageria, se o banco já os alcança; senão None."""
    try:
        from app.apps.mensageria import core as m_core, db as m_db
    except Exception:  # noqa: BLE001
        return None
    try:
        if not m_db.disponivel():
            return None
    except Exception:  # noqa: BLE001
        return None
    return m_core, m_db


def _registrar(m, **campos):
    """Anota no registro da mensageria. Nunca derruba o envio."""
    if not m:
        return
    m_core, m_db = m
    try:
        with m_db.conexao() as conn:
            m_core.registrar(conn, **campos)
            m_core.limpar_se_preciso(conn)
    except Exception:  # noqa: BLE001
        print("[notificador] não consegui registrar o envio na mensageria")


def _avisar_admins_do_teto(m, motivo, tipo):
    """Os ADMIN do ERP recebem, por Telegram, que o WhatsApp bateu no teto —
    no máximo uma vez por hora (a mensageria guarda a hora do último aviso)."""
    m_core, m_db = m
    try:
        import datetime as _dt
        with m_db.conexao() as conn:
            if not m_core._deve_avisar_limite(conn, _dt.datetime.now(_dt.timezone.utc)):
                return
            quem_consome = m_core.tipo_que_mais_consome(conn, _dt.datetime.now(_dt.timezone.utc))
            admins = m_db.todos(conn, """SELECT telefone, cpf FROM public.usuarios
                                          WHERE perfil::text = 'ADMIN' AND ativo
                                            AND (coalesce(telefone, '') <> '' OR coalesce(cpf, '') <> '')""")
        texto = ("⚠️ *Mensagens — teto do WhatsApp*\n\n"
                 f"{motivo}. A mensagem de *{m_core.nome_do_tipo(tipo)}* ficou sem sair.\n"
                 + (f"Quem mais consumiu hoje: {quem_consome}.\n" if quem_consome else "")
                 + "O teto se ajusta em ERP › Mensagens.")
        # Direto pelo Telegram, sem passar pela política: o aviso de teto nunca
        # pode cair no próprio teto nem virar WhatsApp.
        for a in admins:
            r = _tg_notificar(telefone=a.get("telefone"), cpf=a.get("cpf"), mensagem=texto)
            _registrar(m, tipo="mensageria.limite", canal="telegram",
                       destinatario=str(r.get("chat_id") or a.get("telefone") or ""), cpf=a.get("cpf") or "",
                       texto=texto, nome_arquivo="", status=_status_tg(r),
                       detalhe=str(r.get("detalhe") or r.get("erro") or ""))
    except Exception:  # noqa: BLE001
        print("[notificador] não consegui avisar os ADMIN do teto do WhatsApp")


# ---------------------------------------------------------------------------
# Funções públicas auxiliares
# ---------------------------------------------------------------------------

def canal_ativo(canal, finalidade=None):
    """O canal está liberado para esta finalidade? Pela mensageria quando ela
    alcança; senão pelas variáveis de ambiente."""
    if finalidade:
        m = _mensageria()
        if m:
            m_core, m_db = m
            try:
                with m_db.conexao() as conn:
                    return canal in m_core.decidir(conn, finalidade).canais
            except Exception:  # noqa: BLE001
                pass
    return _canal_ativo(canal, finalidade)


def enviar_whatsapp(telefone, mensagem, finalidade=None):
    """Envio SÓ de texto pelo WhatsApp, respeitando a política/toggle."""
    if finalidade and _mensageria():
        r = notificar(telefone=telefone, mensagem=mensagem, canais=("whatsapp",),
                      finalidade=finalidade)
        return r.get("whatsapp") or r.get("mensageria") or {"ok": None, "detalhe": "sem canal"}
    if not _canal_ativo("whatsapp", finalidade):
        return {"ok": None, "detalhe": "canal desativado (env NOTIFICAR_*)"}
    tel = re.sub(r"\D", "", telefone or "")
    if not tel:
        return {"ok": False, "erro": "sem_telefone"}
    r = _wa_enviar_texto(tel, mensagem)
    _registrar(_mensageria(), tipo=finalidade or "sem_tipo", canal="whatsapp", destinatario=tel,
               cpf="", texto=mensagem, nome_arquivo="",
               status="ENVIADO" if r.get("ok") else "FALHOU", detalhe=str(r.get("detalhe") or r.get("erro") or ""))
    return r


def enviar_telegram(telefone=None, cpf=None, chat_id=None, mensagem="",
                    arquivo_url=None, arquivo_base64=None, nome_arquivo=None,
                    tipo=None, finalidade=None):
    """Envio pelo Telegram. Com finalidade e mensageria no ar, a política do
    tipo manda (pode virar WhatsApp para quem não tem Telegram); o resultado
    devolvido é o do canal que entregou."""
    if finalidade and _mensageria():
        r = notificar(telefone=telefone, cpf=cpf, chat_id=chat_id, mensagem=mensagem,
                      arquivo_url=arquivo_url, arquivo_base64=arquivo_base64,
                      nome_arquivo=nome_arquivo, tipo=tipo, canais=("telegram",),
                      finalidade=finalidade)
        for canal in ("telegram", "whatsapp", "mensageria"):
            if r.get(canal, {}).get("ok"):
                return r[canal]
        return r.get("telegram") or r.get("whatsapp") or r.get("mensageria") \
            or {"ok": None, "detalhe": "sem canal"}
    if not _canal_ativo("telegram", finalidade):
        return {"ok": None, "detalhe": "canal desativado (env NOTIFICAR_*)"}
    r = _tg_notificar(telefone=telefone, cpf=cpf, chat_id=chat_id,
                      mensagem=mensagem, arquivo_url=arquivo_url,
                      arquivo_base64=arquivo_base64,
                      nome_arquivo=nome_arquivo, tipo=tipo)
    _registrar(_mensageria(), tipo=finalidade or "sem_tipo", canal="telegram",
               destinatario=str(r.get("chat_id") or telefone or ""), cpf=cpf or "", texto=mensagem,
               nome_arquivo=nome_arquivo or "", status=_status_tg(r), detalhe=str(r.get("detalhe") or r.get("erro") or ""))
    return r


def _status_tg(r):
    if r.get("ok"):
        return "ENVIADO"
    if r.get("erro") == "nao_cadastrado":
        return "SEM_DESTINO"
    return "FALHOU"


# ---------------------------------------------------------------------------
# Função pública
# ---------------------------------------------------------------------------

def notificar(telefone=None, cpf=None, chat_id=None, mensagem="",
              arquivo_url=None, arquivo_base64=None, nome_arquivo=None,
              tipo=None, canais=("telegram", "whatsapp"),
              politica="ambos", finalidade=None):
    """
    Envia a notificação.

    finalidade: o TIPO da mensagem ("ponto.qr", "erp.titulo_pago"…). Com ele e
      a mensageria no ar, a política da tela "Mensagens" decide os canais e a
      ordem — `canais`/`politica` passados pelo chamador são ignorados. Sem
      mensageria, valem `canais`/`politica` e as variáveis NOTIFICAR_*.
    politica (sem mensageria):
      "ambos"    -> envia por todos os canais listados
      "fallback" -> tenta na ordem de `canais`; para no primeiro que der certo

    Retorna dict por canal, ex.:
      {"telegram": {"ok": True, "chat_id": "701..."},
       "whatsapp": {"ok": True, "detalhe": "ok"}}
    Canais não executados aparecem como {"ok": None, "detalhe": "não executado"};
    tipo desligado na tela vem como {"mensageria": {"ok": None, "detalhe": ...}}.
    """
    if not mensagem and not arquivo_url and not arquivo_base64:
        raise ValueError("informe mensagem e/ou arquivo")
    if not telefone and not cpf and not chat_id:
        raise ValueError("informe telefone, cpf ou chat_id")

    telefone_norm = re.sub(r"\D", "", telefone or "")
    m = _mensageria()          # para registrar, mesmo sem finalidade
    decisao = None
    if m and finalidade:
        m_core, m_db = m
        try:
            with m_db.conexao() as conn:
                decisao = m_core.decidir(conn, finalidade)
        except Exception:  # noqa: BLE001
            print("[notificador] mensageria não decidiu; seguindo pelas variáveis")
            decisao = None
    if decisao is not None:
        canais = decisao.canais
        politica = "fallback" if decisao.fallback else "ambos"
        if not canais:
            _registrar(m, tipo=finalidade, canal="", destinatario=telefone_norm or str(chat_id or ""),
                       cpf=cpf or "", texto=mensagem, nome_arquivo=nome_arquivo or "",
                       status="DESLIGADO", detalhe=decisao.motivo)
            return {"mensageria": {"ok": None, "detalhe": decisao.motivo}}

    def _ativo(canal):
        return True if decisao is not None else _canal_ativo(canal, finalidade)

    resultados = {}
    for canal in canais:
        if not _ativo(canal):
            resultados[canal] = {"ok": None,
                                 "detalhe": "canal desativado "
                                            "(env NOTIFICAR_*)"}
            continue
        if canal == "telegram":
            r = _tg_notificar(
                telefone=telefone_norm or None, cpf=cpf, chat_id=chat_id,
                mensagem=mensagem, arquivo_url=arquivo_url,
                arquivo_base64=arquivo_base64, nome_arquivo=nome_arquivo,
                tipo=tipo,
            )
            resultados["telegram"] = r
            _registrar(m, tipo=finalidade or "sem_tipo", canal="telegram",
                       destinatario=str(r.get("chat_id") or telefone_norm or ""), cpf=cpf or "",
                       texto=mensagem, nome_arquivo=nome_arquivo or "", status=_status_tg(r),
                       detalhe=str(r.get("detalhe") or r.get("erro") or ""))
        elif canal == "whatsapp":
            if not telefone_norm:
                r = {"ok": False, "erro": "sem_telefone", "detalhe": "WhatsApp exige telefone"}
            else:
                cabe, motivo = True, ""
                if m:
                    try:
                        with m[1].conexao() as conn:
                            cabe, motivo = m[0].cabe_no_teto(conn)
                    except Exception:  # noqa: BLE001
                        cabe, motivo = True, ""
                if not cabe:
                    r = {"ok": False, "erro": "limite", "detalhe": motivo}
                    _registrar(m, tipo=finalidade, canal="whatsapp", destinatario=telefone_norm,
                               cpf=cpf or "", texto=mensagem, nome_arquivo=nome_arquivo or "",
                               status="LIMITE", detalhe=motivo)
                    _avisar_admins_do_teto(m, motivo, finalidade)
                    resultados["whatsapp"] = r
                    continue
                if arquivo_url or arquivo_base64:
                    t = tipo or _inferir_tipo(nome_arquivo or arquivo_url or "")
                    r = _wa_enviar_arquivo(
                        telefone_norm, t, arquivo_url=arquivo_url,
                        arquivo_base64=arquivo_base64,
                        nome_arquivo=nome_arquivo, legenda=mensagem,
                    )
                else:
                    r = _wa_enviar_texto(telefone_norm, mensagem)
            resultados["whatsapp"] = r
            _registrar(m, tipo=finalidade or "sem_tipo", canal="whatsapp", destinatario=telefone_norm,
                       cpf=cpf or "", texto=mensagem, nome_arquivo=nome_arquivo or "",
                       status="ENVIADO" if r.get("ok") else "FALHOU",
                       detalhe=str(r.get("detalhe") or r.get("erro") or ""))
        else:
            resultados[canal] = {"ok": False, "erro": "canal_desconhecido"}

        if politica == "fallback" and resultados[canal].get("ok"):
            for restante in canais:
                if restante not in resultados:
                    resultados[restante] = {"ok": None,
                                            "detalhe": "não executado"}
            break

    return resultados
