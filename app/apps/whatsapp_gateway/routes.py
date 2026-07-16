"""Rotas do gateway WhatsApp — espelham o dialeto do z-api e traduzem p/ Evolution.

Objetivo: nos cenários do make.com, trocar SOMENTE o domínio base da URL. Os
caminhos abaixo replicam exatamente os do z-api:

    z-api : https://api.z-api.io/instances/{id}/token/{tk}/send-text
    gateway: https://<seu-dominio>/instances/{id}/token/{tk}/send-text

Rotas z-api espelhadas (registradas SEM url_prefix — ver main.py):
- POST /instances/<instance>/token/<token>/send-text
- POST /instances/<instance>/token/<token>/send-image
- POST /instances/<instance>/token/<token>/send-document/<extension>
- POST /instances/<instance>/token/<token>/send-audio
- POST /instances/<instance>/token/<token>/send-link

Rotas internas (não expostas ao make):
- POST /api/whatsapp_gateway/webhook/<evolution_instance>  ← Evolution chama aqui
- GET  /api/whatsapp_gateway/health
"""

import logging

import requests
from flask import Blueprint, jsonify, request

from . import config
from .evolution import EvolutionClient, EvolutionError, extract_message_id
from .webhook import translate_evolution_to_zapi

logger = logging.getLogger(__name__)

bp = Blueprint("whatsapp_gateway", __name__)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _zapi_error(mensagem: str, status: int = 400):
    """Erro no formato que o z-api costuma devolver."""
    return jsonify({"error": mensagem}), status


def _zapi_success(evolution_response: dict):
    """Resposta de sucesso no formato do z-api (zaapId/messageId/id)."""
    msg_id = extract_message_id(evolution_response)
    return jsonify({"zaapId": msg_id, "messageId": msg_id, "id": msg_id}), 200


def _resolve(instance: str, token: str):
    """Valida instance/token (+ Client-Token opcional) e devolve (cfg, client).

    Levanta uma tupla (response, status) via exceção _Abort quando inválido.
    """
    # Client-Token opcional, espelhando o header de segurança do z-api.
    expected_client = config.client_token_expected()
    if expected_client:
        sent = request.headers.get("Client-Token", "")
        if sent != expected_client:
            raise _Abort(_zapi_error("Client-Token inválido.", 401))

    try:
        cfg = config.get_instance(instance, token)
    except ValueError:
        raise _Abort(_zapi_error("Instância ou token inválidos.", 401))
    except config.GatewayConfigError as exc:
        logger.error("Config do gateway inválida: %s", exc)
        raise _Abort(_zapi_error("Gateway mal configurado.", 500))

    client = EvolutionClient(
        base_url=cfg["base_url"],
        instance=cfg["evolution_instance"],
        apikey=cfg["evolution_apikey"],
    )
    return cfg, client


class _Abort(Exception):
    """Interrompe o handler devolvendo uma resposta Flask pronta."""

    def __init__(self, response):
        self.response = response


def _body() -> dict:
    return request.get_json(silent=True) or {}


# --------------------------------------------------------------------------- #
# Rotas espelhando o z-api
# --------------------------------------------------------------------------- #
@bp.post("/instances/<instance>/token/<token>/send-text")
def send_text(instance, token):
    try:
        _cfg, client = _resolve(instance, token)
    except _Abort as abort:
        return abort.response

    body = _body()
    phone = body.get("phone")
    message = body.get("message", "")
    if not phone or message == "":
        return _zapi_error("Campos 'phone' e 'message' são obrigatórios.")

    try:
        resp = client.send_text(phone, message)
    except EvolutionError as exc:
        return _zapi_error(exc.message, exc.status_code)
    return _zapi_success(resp)


@bp.post("/instances/<instance>/token/<token>/send-image")
def send_image(instance, token):
    try:
        _cfg, client = _resolve(instance, token)
    except _Abort as abort:
        return abort.response

    body = _body()
    phone = body.get("phone")
    image = body.get("image")  # URL http(s) ou base64 — igual ao z-api
    if not phone or not image:
        return _zapi_error("Campos 'phone' e 'image' são obrigatórios.")

    try:
        resp = client.send_media(
            number=phone,
            media=image,
            mediatype="image",
            caption=body.get("caption", ""),
        )
    except EvolutionError as exc:
        return _zapi_error(exc.message, exc.status_code)
    return _zapi_success(resp)


@bp.post("/instances/<instance>/token/<token>/send-document/<extension>")
def send_document(instance, token, extension):
    try:
        _cfg, client = _resolve(instance, token)
    except _Abort as abort:
        return abort.response

    body = _body()
    phone = body.get("phone")
    document = body.get("document")  # URL ou base64
    if not phone or not document:
        return _zapi_error("Campos 'phone' e 'document' são obrigatórios.")

    # z-api usa a extensão na URL; derivamos um fileName se não vier no body.
    filename = body.get("fileName") or f"arquivo.{extension}"

    try:
        resp = client.send_media(
            number=phone,
            media=document,
            mediatype="document",
            filename=filename,
            caption=body.get("caption", ""),
        )
    except EvolutionError as exc:
        return _zapi_error(exc.message, exc.status_code)
    return _zapi_success(resp)


@bp.post("/instances/<instance>/token/<token>/send-audio")
def send_audio(instance, token):
    try:
        _cfg, client = _resolve(instance, token)
    except _Abort as abort:
        return abort.response

    body = _body()
    phone = body.get("phone")
    audio = body.get("audio")  # URL ou base64
    if not phone or not audio:
        return _zapi_error("Campos 'phone' e 'audio' são obrigatórios.")

    try:
        resp = client.send_audio(phone, audio)
    except EvolutionError as exc:
        return _zapi_error(exc.message, exc.status_code)
    return _zapi_success(resp)


@bp.post("/instances/<instance>/token/<token>/send-link")
def send_link(instance, token):
    """z-api send-link → Evolution sendText com linkPreview ligado.

    A Evolution não tem endpoint dedicado de link; enviamos o texto com a URL e
    deixamos o WhatsApp gerar o preview. Thumbnail customizada (campo 'image' do
    z-api) não é replicada 1:1 — o preview usa o do próprio link.
    """
    try:
        _cfg, client = _resolve(instance, token)
    except _Abort as abort:
        return abort.response

    body = _body()
    phone = body.get("phone")
    message = body.get("message", "")
    link_url = body.get("linkUrl", "")
    if not phone or not link_url:
        return _zapi_error("Campos 'phone' e 'linkUrl' são obrigatórios.")

    # Monta o texto: mensagem + link (o preview vem do próprio WhatsApp).
    texto = f"{message}\n{link_url}".strip() if message else link_url

    try:
        resp = client.send_text(phone, texto, link_preview=True)
    except EvolutionError as exc:
        return _zapi_error(exc.message, exc.status_code)
    return _zapi_success(resp)


# --------------------------------------------------------------------------- #
# Webhook de entrada (Evolution → gateway → make)
# --------------------------------------------------------------------------- #
@bp.post("/api/whatsapp_gateway/webhook/<evolution_instance>")
def receber_webhook(evolution_instance):
    """Recebe eventos da Evolution, traduz p/ formato z-api e repassa ao make.

    Segurança: valida ?secret= contra WHATSAPP_GATEWAY_WEBHOOK_SECRET.
    """
    secret_esperado = config.webhook_secret_expected()
    if secret_esperado and request.args.get("secret") != secret_esperado:
        return jsonify({"ok": False, "erro": "Secret inválido."}), 401

    event = _body()

    try:
        cfg = config.get_instance_by_evolution_name(evolution_instance)
    except ValueError as exc:
        logger.warning("Webhook de instância não mapeada: %s", exc)
        # 200 pra Evolution não ficar reenviando; só não repassamos.
        return jsonify({"ok": True, "ignorado": True}), 200

    zapi_payload = translate_evolution_to_zapi(event)
    if zapi_payload is None:
        return jsonify({"ok": True, "ignorado": True}), 200

    # Garante o instanceId no dialeto z-api (o que o make espera).
    zapi_payload["instanceId"] = cfg["zapi_instance_id"]

    make_url = cfg.get("make_webhook_url")
    if not make_url:
        logger.info("Instância '%s' sem make_webhook_url; nada a repassar.", evolution_instance)
        return jsonify({"ok": True, "sem_destino": True}), 200

    try:
        requests.post(make_url, json=zapi_payload, timeout=20)
    except requests.RequestException as exc:
        logger.error("Falha ao repassar webhook pro make: %s", exc)
        return jsonify({"ok": False, "erro": "Falha ao repassar ao make."}), 502

    return jsonify({"ok": True}), 200


# --------------------------------------------------------------------------- #
# Health / diagnóstico
# --------------------------------------------------------------------------- #
@bp.get("/api/whatsapp_gateway/health")
def health():
    return jsonify({"ok": True, "instancias": config.list_instances_public()}), 200
