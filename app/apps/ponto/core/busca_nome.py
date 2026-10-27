# -*- coding: utf-8 -*-
"""
A BUSCA PELO NOME NO PONTO DA OBRA (27/10/2026).

O responsável de uma obra: *"criar uma lista com foto dos funcionários da obra (…) pra eu
achar rápido quem esqueceu o CPF"*. O dono aprovou com o cuidado da LGPD que propus: é uma
BUSCA, não uma lista — 3 letras ou mais, no máximo 6 pessoas —, com a foto pequena, o nome
e a função, e NUNCA o CPF (o CPF é o dado que serve a golpe; mostrar seria pior que a
foto). Só no aparelho da obra, com o responsável conectado, e a batida feita assim fica
marcada "identificada pelo nome" e sempre passa pela conferência do rosto (rosto.py).

QUEM APARECE: quem é da obra para o aparelho (papeis.pessoas_do_alcance: cadastrado na
obra, quem bateu nela no mês, a equipe fixa) — e, pela observação do dono ("se um
funcionário novo surgir, ele não vai estar naquela obra"), também quem foi ADMITIDO há
até 30 dias e quem está SEM OBRA no cadastro. Não achou? "Procurar em todas as obras".
"""
from __future__ import annotations

import datetime as dt
import re
import unicodedata

from sqlalchemy.engine import Connection

from .. import horario
from . import cadastros, papeis

MIN_LETRAS = 3
MAX_RESULTADOS = 6
NOVOS_DIAS = 30


def _palavras(texto) -> list[str]:
    t = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode().upper()
    return re.findall(r"[A-Z]+", t)


def casa(busca: str, nome: str) -> bool:
    """PURA. Cada palavra digitada é começo de alguma palavra do nome ("jos sous" acha
    José Ribamar Sousa)."""
    procuradas, do_nome = _palavras(busca), _palavras(nome)
    return bool(procuradas) and all(any(n.startswith(p) for n in do_nome) for p in procuradas)


def nome_curto(nome: str) -> str:
    """PURA. O primeiro nome e o último sobrenome — o bastante para reconhecer."""
    partes = str(nome or "").split()
    return " ".join(partes[:1] + partes[-1:]) if len(partes) > 1 else (partes[0] if partes else "")


def buscar(conn: Connection, alcance: papeis.Alcance, texto: str, *, todas: bool = False,
           hoje: dt.date | None = None) -> dict:
    texto = str(texto or "").strip()
    if len("".join(_palavras(texto))) < MIN_LETRAS:
        return {"pessoas": [], "mais": 0, "curta": True}
    hoje = hoje or horario.hoje()
    ativas = [p for p in cadastros.listar_colaboradores(conn, so_ativos=True)
              if p.get("situacao") not in ("DESLIGADO", "AFASTADO")]
    if not todas:
        da_obra = {int(p["id"]) for p in papeis.pessoas_do_alcance(conn, alcance)}
        novos = {int(p["id"]) for p in ativas if not p.get("obra_id")
                 or (p.get("admissao") and p["admissao"] >= hoje - dt.timedelta(days=NOVOS_DIAS))}
        ativas = [p for p in ativas if int(p["id"]) in da_obra | novos]
    achadas = sorted((p for p in ativas if casa(texto, p["nome"])), key=lambda p: p["nome"])
    return {"pessoas": achadas[:MAX_RESULTADOS], "mais": max(0, len(achadas) - MAX_RESULTADOS), "curta": False}
