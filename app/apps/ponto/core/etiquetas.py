# -*- coding: utf-8 -*-
"""
AS ETIQUETAS DA PESSOA — o que se vê de relance em qualquer lista do ponto.

Pedido do dono, 21/10/2026: *"quero dar destaque se aquela pessoa é vigia, se está
com a ronda ativada (…) ele está com celular liberado para bater ponto (…) o
correto é que os vigias estejam todos com a ronda ativa e com o celular
cadastrado; a gestão precisa verificar isso fácil"*, e *"opa, esse aqui tem um
cadastro de ponto da obra, esse aqui tem um celular de equipe"*.

Uma leitura para todas as pessoas da lista (sem uma consulta por pessoa). O tom
diz se é bom (verde), pendência (vermelho), espera (âmbar) ou só informação (azul).
"""
from __future__ import annotations

from sqlalchemy.engine import Connection

from .. import db

# código → (ícone, rótulo, tom). A ordem é a de exibição.
ETIQUETAS = {
    "VIGIA": ("🛡️", "Vigia", "azul"),
    "RONDA": ("🔦", "Ronda ativa", "verde"),
    "VIGIA_SEM_RONDA": ("⚠️", "Vigia sem ronda", "vermelho"),
    "CELULAR": ("📱", "Celular liberado", "verde"),
    "CELULAR_PENDENTE": ("📱", "Celular esperando aprovação", "ambar"),
    "VIGIA_SEM_CELULAR": ("⚠️", "Vigia sem celular liberado", "vermelho"),
    "PONTO_OBRA": ("🏗️", "Responde por um ponto da obra", "azul"),
    "EQUIPE": ("👥", "Ponto de equipe", "azul"),
    "ADMINISTRATIVO": ("🗂️", "Administrativo da obra", "azul"),
    "SEM_PIN": ("🔑", "Ainda sem PIN", "cinza"),
}
# Para o filtro "tipo de pessoa": as etiquetas e a pendência dos vigias juntas.
FILTRAVEIS = ("VIGIA", "VIGIA_PENDENTE", "RONDA", "CELULAR", "CELULAR_PENDENTE", "PONTO_OBRA", "EQUIPE",
              "ADMINISTRATIVO", "SEM_PIN")
ROTULO_FILTRO = {**{k: v[1] for k, v in ETIQUETAS.items()}, "VIGIA_PENDENTE": "Vigia com pendência (ronda ou celular)"}


def e_vigia(p: dict) -> bool:
    return str(p.get("tipo_jornada") or "").startswith("VIGIA") or "VIGIA" in str(p.get("funcao") or "").upper()


def calcular(p: dict, aparelhos: list[dict], tem_pin: bool) -> list[str]:
    """PURA. Os códigos das etiquetas de uma pessoa, na ordem de exibição."""
    aprovados = {a["perfil"] for a in aparelhos if a["status"] == "APROVADO"}
    pendente = any(a["perfil"] == "INDIVIDUAL" and a["status"] == "PENDENTE" for a in aparelhos)
    vigia = e_vigia(p)
    tem = set()
    if vigia:
        tem.add("VIGIA")
        if not p.get("faz_ronda"):
            tem.add("VIGIA_SEM_RONDA")
        if "INDIVIDUAL" not in aprovados:
            tem.add("VIGIA_SEM_CELULAR")
    if p.get("faz_ronda"):
        tem.add("RONDA")
    if "INDIVIDUAL" in aprovados:
        tem.add("CELULAR")
    elif pendente:
        tem.add("CELULAR_PENDENTE")
    if "COMPARTILHADO" in aprovados:
        tem.add("PONTO_OBRA")
    if "LISTA" in aprovados:
        tem.add("EQUIPE")
    if p.get("administrativo_obra"):
        tem.add("ADMINISTRATIVO")
    if not tem_pin:
        tem.add("SEM_PIN")
    return [c for c in ETIQUETAS if c in tem]


def para_json(codigos: list[str]) -> list[dict]:
    return [{"codigo": c, "icone": ETIQUETAS[c][0], "rotulo": ETIQUETAS[c][1], "tom": ETIQUETAS[c][2]}
            for c in codigos]


def das_pessoas(conn: Connection, pessoas: list[dict]) -> dict[int, list[str]]:
    """{id: [códigos]} para a lista inteira, numa leitura só."""
    ids = [int(p["id"]) for p in pessoas]
    if not ids:
        return {}
    arquivado = "AND arquivado_em IS NULL" if db.tem_coluna(conn, "dispositivos", "arquivado_em") else ""
    aparelhos: dict[int, list[dict]] = {}
    for a in db.todos(conn, f"""
        SELECT colaborador_id, perfil, status FROM ponto.dispositivos
         WHERE colaborador_id = ANY(:ids) AND status IN ('APROVADO', 'PENDENTE') {arquivado}""", ids=ids):
        aparelhos.setdefault(int(a["colaborador_id"]), []).append(a)
    pins = {int(l["colaborador_id"]) for l in db.todos(conn, """
        SELECT colaborador_id FROM ponto.colaborador_config
         WHERE colaborador_id = ANY(:ids) AND pin_hash IS NOT NULL""", ids=ids)}
    return {int(p["id"]): calcular(p, aparelhos.get(int(p["id"]), []), int(p["id"]) in pins) for p in pessoas}


def casa_com_filtro(codigos: list[str], quer: set[str]) -> bool:
    """PURA. A pessoa tem alguma das etiquetas pedidas? (VIGIA_PENDENTE = sem ronda ou sem celular)."""
    if not quer:
        return True
    tem = set(codigos)
    if "VIGIA_SEM_RONDA" in tem or "VIGIA_SEM_CELULAR" in tem:
        tem.add("VIGIA_PENDENTE")
    return bool(tem & quer)
