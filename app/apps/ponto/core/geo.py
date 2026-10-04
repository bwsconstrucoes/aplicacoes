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


# ---------------------------------------------------------------------------
# Em que obra a pessoa está (04/10/2026)
#
# Decisão do dono: "não queremos permitir que a pessoa bata ponto fora das
# áreas de obra. E quero ainda que a obra seja detectada automaticamente." A
# obra da batida passa a ser a da CERCA em que o celular está — não a que a
# pessoa escolheu numa lista.
#
# A PRECISÃO DO GPS entra na conta, porque dentro de prédio ou debaixo de laje o
# celular pode errar 50, 100 m. Quem está fora do raio, mas a menos da precisão
# informada (no máximo 150 m de folga), está "na borda": a batida é aceita e
# vai para conferência. Mais longe que isso, está fora.
# ---------------------------------------------------------------------------
FOLGA_MAXIMA_DA_PRECISAO_M = 150.0

DENTRO, BORDA, FORA, SEM_LOCAL = "DENTRO", "BORDA", "FORA", "SEM_LOCAL"


def localizar_obra(lat, lon, precisao, obras: list[dict]) -> tuple[str, Optional[dict], Optional[float]]:
    """PURA. Devolve (situação, obra, distância em metros).

    `obras` traz id, latitude, longitude e raio_metros. Só entram na conta as
    que têm coordenada. DENTRO/BORDA devolvem a obra da cerca (a de centro mais
    perto, se houver duas); FORA devolve a obra mais perto, para a mensagem
    dizer a quantos metros ela está; SEM_LOCAL, quando o celular não mandou
    localização válida."""
    if not coordenada_valida(lat, lon):
        return SEM_LOCAL, None, None
    try:
        folga = min(max(float(precisao or 0), 0.0), FOLGA_MAXIMA_DA_PRECISAO_M)
    except (TypeError, ValueError):
        folga = 0.0
    medidas = []
    for o in obras:
        if not coordenada_valida(o.get("latitude"), o.get("longitude")):
            continue
        d = distancia_metros(lat, lon, o["latitude"], o["longitude"])
        medidas.append((d, o))
    if not medidas:
        return FORA, None, None
    medidas.sort(key=lambda x: x[0])
    dentro = [(d, o) for d, o in medidas if d <= float(o.get("raio_metros") or 0)]
    if dentro:
        return DENTRO, dentro[0][1], round(dentro[0][0], 1)
    borda = [(d, o) for d, o in medidas if d - folga <= float(o.get("raio_metros") or 0)]
    if borda:
        return BORDA, borda[0][1], round(borda[0][0], 1)
    return FORA, medidas[0][1], round(medidas[0][0], 1)


def distancia_legivel(metros: Optional[float]) -> str:
    if metros is None:
        return "?"
    return f"{metros / 1000:.1f} km".replace(".", ",") if metros >= 1000 else f"{metros:.0f} m"
