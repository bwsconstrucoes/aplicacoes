"""Tradução do webhook da Evolution para o formato on-message-received do z-api.

A Evolution POSTa eventos (messages.upsert etc.) no gateway; aqui a gente
converte o payload dela no formato "ReceivedCallback" que o z-api entrega e que
seus cenários do make.com já esperam. Assim o make não precisa mudar o mapeamento
dos campos — só a origem da chamada.

Cobertura de mídia: a Evolution precisa estar configurada para entregar a mídia
(URL ou base64) no webhook — ver README (WEBHOOK_BASE64 / storage). Se vier
base64, repassamos em `*.base64`; se vier URL, em `*Url`.
"""

import logging

logger = logging.getLogger(__name__)


def _phone_from_jid(jid: str) -> str:
    """5585999998888@s.whatsapp.net → 5585999998888 (ou id de grupo sem sufixo)."""
    if not jid:
        return ""
    return jid.split("@", 1)[0].split(":", 1)[0]


def _is_group(jid: str) -> bool:
    return bool(jid) and jid.endswith("@g.us")


def _extract_text(message: dict) -> str:
    """Extrai texto de conversation / extendedTextMessage."""
    if not isinstance(message, dict):
        return ""
    if message.get("conversation"):
        return message["conversation"]
    ext = message.get("extendedTextMessage")
    if isinstance(ext, dict) and ext.get("text"):
        return ext["text"]
    return ""


def translate_evolution_to_zapi(event: dict) -> dict | None:
    """Converte um evento da Evolution no payload z-api. Retorna None se o evento
    não for uma mensagem que deva ser repassada (ex.: eventos de status/conexão)."""
    if not isinstance(event, dict):
        return None

    evt_name = event.get("event", "")
    # Só repassa mensagens novas. Outros eventos (connection.update, etc.) são ignorados.
    if evt_name not in ("messages.upsert", "messages.update"):
        return None

    data = event.get("data")
    if not isinstance(data, dict):
        return None

    key = data.get("key", {}) if isinstance(data.get("key"), dict) else {}
    remote_jid = key.get("remoteJid", "")
    message = data.get("message", {}) if isinstance(data.get("message"), dict) else {}

    timestamp = data.get("messageTimestamp") or 0
    try:
        momment = int(timestamp) * 1000  # z-api usa milissegundos
    except (TypeError, ValueError):
        momment = 0

    payload = {
        "isStatusReply": False,
        "instanceId": event.get("instance", ""),
        "messageId": key.get("id", ""),
        "phone": _phone_from_jid(remote_jid),
        "fromMe": bool(key.get("fromMe", False)),
        "momment": momment,
        "senderName": data.get("pushName", ""),
        "chatName": data.get("pushName", ""),
        "type": "ReceivedCallback",
        "isGroup": _is_group(remote_jid),
    }

    # ---- Texto ----
    text = _extract_text(message)
    if text:
        payload["text"] = {"message": text}
        return payload

    # ---- Imagem ----
    img = message.get("imageMessage")
    if isinstance(img, dict):
        payload["image"] = {
            "caption": img.get("caption", ""),
            "mimeType": img.get("mimetype", ""),
            "imageUrl": data.get("mediaUrl", "") or img.get("url", ""),
            "base64": data.get("base64", ""),
        }
        return payload

    # ---- Áudio ----
    audio = message.get("audioMessage")
    if isinstance(audio, dict):
        payload["audio"] = {
            "mimeType": audio.get("mimetype", ""),
            "audioUrl": data.get("mediaUrl", "") or audio.get("url", ""),
            "base64": data.get("base64", ""),
        }
        return payload

    # ---- Documento ----
    doc = message.get("documentMessage") or message.get("documentWithCaptionMessage")
    if isinstance(doc, dict):
        # documentWithCaptionMessage aninha o documento
        inner = doc.get("message", {}).get("documentMessage") if doc.get("message") else None
        d = inner if isinstance(inner, dict) else doc
        payload["document"] = {
            "caption": d.get("caption", ""),
            "fileName": d.get("fileName", ""),
            "mimeType": d.get("mimetype", ""),
            "documentUrl": data.get("mediaUrl", "") or d.get("url", ""),
            "base64": data.get("base64", ""),
        }
        return payload

    # Tipo não mapeado (localização, contato, etc.) — repassa só o esqueleto.
    logger.info("Webhook Evolution: tipo de mensagem não mapeado, repassando esqueleto.")
    payload["text"] = {"message": ""}
    return payload
