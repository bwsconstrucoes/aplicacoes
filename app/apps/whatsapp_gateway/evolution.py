"""Cliente HTTP para a Evolution API (v2).

Encapsula as chamadas que o gateway usa. Reusa `requests` (já no requirements).
Todas as respostas seguem o padrão: retorna dict (JSON parseado) em caso de
sucesso, ou levanta EvolutionError com detalhe pra ser traduzido em erro z-api.

Endpoints Evolution v2 usados:
- POST /message/sendText/{instance}
- POST /message/sendMedia/{instance}
- POST /message/sendWhatsAppAudio/{instance}

Auth: header `apikey: <apikey da instância>`.
Número: dígitos com DDI (ex.: 5585999998888) — igual ao `phone` do z-api.
"""

import logging

import requests

logger = logging.getLogger(__name__)

# Timeout generoso: envio de mídia por URL pode demorar a Evolution baixar o arquivo.
_TIMEOUT = 60


class EvolutionError(Exception):
    """Falha ao falar com a Evolution API."""

    def __init__(self, message: str, status_code: int = 502, payload: dict | None = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.payload = payload or {}


def _only_digits(phone: str) -> str:
    """Normaliza número: mantém só dígitos (Evolution aceita 5585... sem sufixo)."""
    return "".join(ch for ch in str(phone) if ch.isdigit())


class EvolutionClient:
    def __init__(self, base_url: str, instance: str, apikey: str):
        self.base_url = base_url.rstrip("/")
        self.instance = instance
        self.apikey = apikey

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self.base_url}{path}"
        headers = {"apikey": self.apikey, "Content-Type": "application/json"}
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=_TIMEOUT)
        except requests.RequestException as exc:
            logger.error("Falha de rede ao chamar Evolution %s: %s", url, exc)
            raise EvolutionError(f"Falha de rede com a Evolution: {exc}", 502)

        if resp.status_code >= 400:
            detail = _safe_json(resp)
            logger.error(
                "Evolution retornou %s em %s: %s", resp.status_code, path, detail
            )
            raise EvolutionError(
                f"Evolution respondeu {resp.status_code}.",
                status_code=resp.status_code,
                payload=detail,
            )
        return _safe_json(resp)

    def send_text(self, number: str, text: str, link_preview: bool = False) -> dict:
        body = {
            "number": _only_digits(number),
            "text": text,
            "linkPreview": link_preview,
        }
        return self._post(f"/message/sendText/{self.instance}", body)

    def send_media(
        self,
        number: str,
        media: str,
        mediatype: str,
        filename: str = "",
        caption: str = "",
        mimetype: str = "",
    ) -> dict:
        """Envia imagem/documento/vídeo. `media` = URL http(s) ou base64.

        mediatype: "image" | "document" | "video".
        """
        body = {
            "number": _only_digits(number),
            "mediatype": mediatype,
            "media": media,
        }
        if caption:
            body["caption"] = caption
        if filename:
            body["fileName"] = filename
        if mimetype:
            body["mimetype"] = mimetype
        return self._post(f"/message/sendMedia/{self.instance}", body)

    def send_audio(self, number: str, audio: str) -> dict:
        """Envia áudio de voz (PTT). `audio` = URL http(s) ou base64."""
        body = {"number": _only_digits(number), "audio": audio}
        return self._post(f"/message/sendWhatsAppAudio/{self.instance}", body)


def extract_message_id(evolution_response: dict) -> str:
    """Extrai o messageId da resposta da Evolution de forma robusta."""
    if not isinstance(evolution_response, dict):
        return ""
    key = evolution_response.get("key")
    if isinstance(key, dict) and key.get("id"):
        return str(key["id"])
    # fallbacks conforme versão
    for field in ("id", "messageId"):
        if evolution_response.get(field):
            return str(evolution_response[field])
    return ""


def _safe_json(resp: "requests.Response") -> dict:
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}
    except ValueError:
        return {"raw": resp.text[:500]}
