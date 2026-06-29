# -*- coding: utf-8 -*-
"""
Instala retry global com backoff em 429/5xx para TODAS as chamadas gspread.

A service account é compartilhada entre os apps do monorepo; sob carga o Sheets
devolve 429 ('Read/Write requests per minute per user'). Importar este módulo já
aplica o patch (idempotente) — não é preciso mexer em cada chamada.
"""
from __future__ import annotations
import time
from gspread.http_client import HTTPClient
from gspread.exceptions import APIError

_RETRYABLE = (429, 500, 502, 503, 504)
_MAX_TENTATIVAS = 6


def _status(e: APIError):
    code = getattr(e, "code", None)
    if code in (None, -1):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
    return code


def instalar() -> None:
    if getattr(HTTPClient, "_bws_retry", False):
        return
    _orig = HTTPClient.request

    def request(self, *args, **kwargs):
        atraso = 1.0
        ultima = None
        for _ in range(_MAX_TENTATIVAS):
            try:
                return _orig(self, *args, **kwargs)
            except APIError as e:
                if _status(e) not in _RETRYABLE:
                    raise
                ultima = e
                try:
                    ra = e.response.headers.get("Retry-After")
                except Exception:
                    ra = None
                espera = float(ra) if (ra and str(ra).replace(".", "", 1).isdigit()) else atraso
                time.sleep(min(espera, 30))
                atraso = min(atraso * 2, 16)
        if ultima is not None:
            raise ultima

    HTTPClient.request = request
    HTTPClient._bws_retry = True


instalar()
