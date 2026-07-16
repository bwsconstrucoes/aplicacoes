"""Configuração e registry de instâncias do gateway WhatsApp (Evolution API).

Este módulo carrega o mapeamento entre o "dialeto z-api" (instance/token que o
make.com já usa nas URLs) e as instâncias reais da Evolution API. A ideia é que,
na migração, você mantenha os MESMOS instance/token que já estão nos cenários do
make — assim só o domínio base da URL muda.

Envvars usadas (reusa o padrão de secret por módulo do projeto):
- EVOLUTION_BASE_URL            → URL pública do serviço Evolution (ex.: https://evolution-bws.onrender.com)
- WHATSAPP_GATEWAY_INSTANCES    → JSON com o registry (ver formato abaixo)
- WHATSAPP_GATEWAY_CLIENT_TOKEN → (opcional) espelha o header Client-Token do z-api
- WHATSAPP_GATEWAY_WEBHOOK_SECRET → secret que a Evolution envia de volta no webhook

Formato de WHATSAPP_GATEWAY_INSTANCES (JSON, string única na envvar):

    {
      "<ZAPI_INSTANCE_ID_1>": {
        "token": "<ZAPI_TOKEN_1>",
        "evolution_instance": "bws-numero1",
        "evolution_apikey": "<APIKEY_DA_INSTANCIA_1>",
        "make_webhook_url": "https://hook.make.com/xxxxxxxx"
      },
      "<ZAPI_INSTANCE_ID_2>": {
        "token": "<ZAPI_TOKEN_2>",
        "evolution_instance": "bws-numero2",
        "evolution_apikey": "<APIKEY_DA_INSTANCIA_2>",
        "make_webhook_url": "https://hook.make.com/yyyyyyyy"
      }
    }

- <ZAPI_INSTANCE_ID> e "token": mantenha os mesmos que o make já usa (transição sem dor).
- "evolution_instance": nome da instância criada na Evolution.
- "evolution_apikey": apikey retornada ao criar a instância na Evolution.
- "make_webhook_url": para onde repassar as mensagens recebidas (formato z-api).
"""

import json
import logging
import os

logger = logging.getLogger(__name__)


class GatewayConfigError(Exception):
    """Erro de configuração do gateway (registry ausente/ inválido)."""


def _base_url() -> str:
    url = os.getenv("EVOLUTION_BASE_URL", "").strip().rstrip("/")
    if not url:
        raise GatewayConfigError("EVOLUTION_BASE_URL não configurado.")
    return url


def _load_registry() -> dict:
    """Lê o registry de instâncias da envvar. Sem cache: barato e evita
    precisar redeploy só pra trocar mapeamento (Render permite editar envvar)."""
    raw = os.getenv("WHATSAPP_GATEWAY_INSTANCES", "").strip()
    if not raw:
        raise GatewayConfigError("WHATSAPP_GATEWAY_INSTANCES não configurado.")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GatewayConfigError(f"WHATSAPP_GATEWAY_INSTANCES não é JSON válido: {exc}")
    if not isinstance(data, dict):
        raise GatewayConfigError("WHATSAPP_GATEWAY_INSTANCES deve ser um objeto JSON.")
    return data


def get_instance(instance_id: str, token: str) -> dict:
    """Resolve instance/token (dialeto z-api) para a config da instância Evolution.

    Levanta ValueError se o par instance/token não bater — o caller traduz isso
    para o erro 401 no formato do z-api.
    """
    registry = _load_registry()
    cfg = registry.get(instance_id)
    if not cfg:
        raise ValueError("Instância não encontrada.")
    if cfg.get("token") != token:
        raise ValueError("Token inválido para a instância.")

    evolution_instance = cfg.get("evolution_instance")
    evolution_apikey = cfg.get("evolution_apikey")
    if not evolution_instance or not evolution_apikey:
        raise GatewayConfigError(
            f"Instância '{instance_id}' sem evolution_instance/evolution_apikey."
        )

    return {
        "zapi_instance_id": instance_id,
        "evolution_instance": evolution_instance,
        "evolution_apikey": evolution_apikey,
        "make_webhook_url": cfg.get("make_webhook_url", ""),
        "base_url": _base_url(),
    }


def get_instance_by_evolution_name(name: str) -> dict:
    """Busca config pelo nome da instância Evolution (usado no webhook de entrada)."""
    registry = _load_registry()
    for zapi_id, cfg in registry.items():
        if cfg.get("evolution_instance") == name:
            return {
                "zapi_instance_id": zapi_id,
                "evolution_instance": name,
                "make_webhook_url": cfg.get("make_webhook_url", ""),
            }
    raise ValueError(f"Instância Evolution '{name}' não mapeada no registry.")


def client_token_expected() -> str:
    """Client-Token esperado (opcional). Vazio = validação desligada."""
    return os.getenv("WHATSAPP_GATEWAY_CLIENT_TOKEN", "").strip()


def webhook_secret_expected() -> str:
    """Secret esperado nas chamadas de webhook vindas da Evolution."""
    return os.getenv("WHATSAPP_GATEWAY_WEBHOOK_SECRET", "").strip()


def list_instances_public() -> list:
    """Lista instâncias configuradas sem expor tokens/apikeys (rota de health)."""
    try:
        registry = _load_registry()
    except GatewayConfigError:
        return []
    return [
        {
            "zapi_instance_id": zapi_id,
            "evolution_instance": cfg.get("evolution_instance"),
            "has_webhook": bool(cfg.get("make_webhook_url")),
        }
        for zapi_id, cfg in registry.items()
    ]
