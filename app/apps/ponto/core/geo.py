# -*- coding: utf-8 -*-
"""A cerca da obra: distância em metros entre dois pontos e se está dentro do raio.

Funções puras. Haversine basta: a precisão é de metros, e a cerca é de centenas."""
from __future__ import annotations

import math
from decimal import Decimal
from typing import Optional

RAIO_DA_TERRA_METROS = 6_371_008.8


def coordenada_valida(latitude, longitude) -> bool:
    try:
        lat, lon = float(latitude), float(longitude)
    except (TypeError, ValueError):
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0 and not (lat == 0.0 and lon == 0.0)


def distancia_metros(lat1, lon1, lat2, lon2) -> float:
    """Distância pelo grande círculo, em metros."""
    f1, f2 = math.radians(float(lat1)), math.radians(float(lat2))
    dfi = f2 - f1
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dfi / 2) ** 2 + math.cos(f1) * math.cos(f2) * math.sin(dlambda / 2) ** 2
    return 2 * RAIO_DA_TERRA_METROS * math.asin(math.sqrt(a))


def avaliar_cerca(lat, lon, obra_lat, obra_lon, raio_metros: int
                  ) -> tuple[Optional[bool], Optional[float], Optional[str]]:
    """Devolve (dentro, distância, motivo). `dentro` é None quando não dá para
    saber — e o motivo diz por quê, para a batida ir para análise em vez de
    fingir que está certa."""
    if not coordenada_valida(obra_lat, obra_lon):
        return None, None, "obra sem coordenada cadastrada"
    if not coordenada_valida(lat, lon):
        return None, None, "batida sem localização"
    d = distancia_metros(lat, lon, obra_lat, obra_lon)
    dentro = d <= float(raio_metros or 0)
    return dentro, round(d, 1), (None if dentro else
                                 f"fora da cerca: {d:.0f} m da obra (raio {raio_metros} m)")


def decimal_ou_none(valor) -> Optional[Decimal]:
    if valor is None or str(valor).strip() == "":
        return None
    return Decimal(str(valor).strip().replace(",", "."))
