# -*- coding: utf-8 -*-
"""
ERP — O CLIMA MEDIDO DO DIA DA OBRA (24/11/2026).

A DECISÃO É DE 08/11/2026, e ficou um ano-luz na gaveta. Ele, olhando a
maquete do diário: *"a chuva da estação meteorológica entra — eu acho que é
interessante sim colocar."* A coluna `clima_medido` nasceu na migração 092 e
**nunca foi preenchida por ninguém**: o histórico registra, com todas as
letras, *"falta ligar o serviço, que é dependência nova e ainda não entrou"*.

Ele cobrou em 24/11/2026: *"você disse que ia trazer baseado no registro do
tempo, né? (…) E eu não vi em canto nenhum no diário. Isso aqui ficou feito?"*
Não tinha ficado. Agora ficou.

⚑ PARA QUE ISTO SERVE, e não é enfeite: num pleito de prazo, o que vale não é
a obra dizer que choveu — é **prova independente**, com fonte e hora da
consulta. Por isso o que se guarda é a resposta do serviço inteira, com a
procedência, e não só "choveu: sim".

⚑ POR QUE O OPEN-METEO: é gratuito, não pede cadastro nem chave, e serve
dado histórico por coordenada. Chave de API seria mais um segredo para
guardar e renovar — e a regra da casa é não criar variável nova para
credencial. **Não é dependência nova de código**: usa o `requests`, que o
monorepo já tem.

⚠️ O QUE ELE NÃO FAZ, de propósito:

1. **Não substitui o tempo informado pela obra.** São duas colunas
   diferentes: `tempo_manha`/`tempo_tarde` é o que o encarregado viu;
   `clima_medido` é o que a estação registrou. Quando divergem, isso é
   informação — não erro a corrigir.
2. **Não mede o futuro.** Dia que ainda não aconteceu não tem medição, e
   previsão não é prova.
3. **Não mede obra sem coordenada.** Sem latitude e longitude não há o que
   perguntar — e chutar pelo município poria a medição a quilômetros do
   canteiro. A coordenada vem da C. Diários, na importação de obras.
4. **Nunca derruba a tela.** Serviço fora do ar, demora, resposta estranha:
   tudo vira "não mediu", e o diário abre igual.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

SERVICO = "open-meteo"
ENDERECO = "https://archive-api.open-meteo.com/v1/archive"
# Segundos. A tela do diário espera por isto: mais que uns poucos segundos e
# a pessoa acha que o sistema travou.
PACIENCIA = 6


def _ja_tem(linha) -> bool:
    return bool(linha and linha["clima_medido"])


def medir(s: Session, obra_id: int, dia: date, *,
          forcar: bool = False) -> Optional[dict[str, Any]]:
    """Busca o clima daquele dia naquela obra e guarda. Devolve o que achou.

    Uma consulta por obra e por dia, **uma vez na vida**: medido, fica
    guardado e não se pergunta de novo. `forcar=True` refaz.
    """
    linha = s.execute(text("""
        SELECT d.id, d.clima_medido, o.latitude, o.longitude
          FROM diarios_obra d JOIN obras o ON o.id = d.obra_id
         WHERE d.obra_id = :o AND d.data = :d"""),
        {"o": obra_id, "d": dia}).mappings().first()
    if linha is None:
        return None
    if _ja_tem(linha) and not forcar:
        return linha["clima_medido"]
    if linha["latitude"] is None or linha["longitude"] is None:
        return None
    if dia >= date.today():
        return None                 # previsão não é prova

    medido = consultar(float(linha["latitude"]), float(linha["longitude"]), dia)
    if medido is None:
        return None
    s.execute(text("""
        UPDATE diarios_obra SET clima_medido = CAST(:c AS JSONB),
               clima_medido_em = now()
         WHERE id = :i"""),
        {"c": _json(medido), "i": linha["id"]})
    return medido


def _json(valor: dict[str, Any]) -> str:
    import json
    return json.dumps(valor, ensure_ascii=False)


def consultar(latitude: float, longitude: float, dia: date
              ) -> Optional[dict[str, Any]]:
    """A ida ao serviço. Isolada para o teste poder trocá-la por um dublê."""
    import requests

    try:
        r = requests.get(ENDERECO, timeout=PACIENCIA, params={
            "latitude": round(latitude, 4), "longitude": round(longitude, 4),
            "start_date": dia.isoformat(), "end_date": dia.isoformat(),
            "daily": ("precipitation_sum,precipitation_hours,"
                      "temperature_2m_max,temperature_2m_min,"
                      "windspeed_10m_max"),
            "timezone": "America/Fortaleza"})
        r.raise_for_status()
        dados = r.json()
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/clima: não deu para medir %s (%s)", dia, e)
        return None
    return _traduzir(dados, latitude, longitude, dia)


def _primeiro(diario: dict[str, Any], chave: str):
    valores = (diario or {}).get(chave) or []
    return valores[0] if valores else None


def _traduzir(dados: dict[str, Any], latitude: float, longitude: float,
              dia: date) -> Optional[dict[str, Any]]:
    """A resposta do serviço vira o que o diário mostra e guarda.

    ⚑ GUARDA A PROCEDÊNCIA JUNTO (serviço, coordenada e hora da consulta).
    Prova sem procedência não é prova — é o comentário que está na migração
    092 desde que a coluna nasceu.
    """
    diario = (dados or {}).get("daily") or {}
    chuva = _primeiro(diario, "precipitation_sum")
    if chuva is None:
        return None
    horas = _primeiro(diario, "precipitation_hours")
    try:
        chuva = round(float(chuva), 1)
        horas = round(float(horas), 1) if horas is not None else None
    except (TypeError, ValueError):
        return None
    return {
        "chuva_mm": chuva,
        "chuva_horas": horas,
        "temp_max": _primeiro(diario, "temperature_2m_max"),
        "temp_min": _primeiro(diario, "temperature_2m_min"),
        "vento_max_kmh": _primeiro(diario, "windspeed_10m_max"),
        "resumo": resumo_em_portugues(chuva, horas),
        "fonte": SERVICO,
        "latitude": round(latitude, 4),
        "longitude": round(longitude, 4),
        "dia": dia.isoformat(),
        "consultado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def resumo_em_portugues(chuva_mm: float, horas: Optional[float]) -> str:
    """"12,4 mm de chuva em 3 h" — a frase que a tela mostra.

    Os cortes são os da prática de obra, não da meteorologia: o que importa
    para o diário é se deu para trabalhar. Abaixo de 1 mm ninguém para.
    """
    if chuva_mm < 1:
        return "sem chuva registrada"
    quanto = f"{chuva_mm:.1f}".replace(".", ",")
    if horas:
        h = f"{horas:.0f}"
        return f"{quanto} mm de chuva em {h} h"
    return f"{quanto} mm de chuva"
