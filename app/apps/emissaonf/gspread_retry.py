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
_MAX_429 = 3  # 1 chamada + 2 re-tentativas (esperas de 30s e 65s)


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
        tent_429 = 0
        ultima = None
        for _ in range(_MAX_TENTATIVAS):
            try:
                return _orig(self, *args, **kwargs)
            except APIError as e:
                st = _status(e)
                if st not in _RETRYABLE:
                    raise
                ultima = e
                try:
                    ra = e.response.headers.get("Retry-After")
                except Exception:
                    ra = None
                ra_f = float(ra) if (ra and str(ra).replace(".", "", 1).isdigit()) else None

                if st == 429:
                    # Quota é POR MINUTO — retry de 1s é inútil. Espera a janela
                    # seguinte: 30s na 1ª re-tentativa, 65s na 2ª, depois desiste.
                    tent_429 += 1
                    if tent_429 >= _MAX_429:
                        raise
                    espera = max(ra_f or 0.0, 30.0 if tent_429 == 1 else 65.0)
                    time.sleep(min(espera, 65))
                else:
                    espera = ra_f if ra_f is not None else atraso
                    time.sleep(min(espera, 30))
                    atraso = min(atraso * 2, 16)
        if ultima is not None:
            raise ultima

    HTTPClient.request = request
    HTTPClient._bws_retry = True


instalar()