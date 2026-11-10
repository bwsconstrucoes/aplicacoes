# -*- coding: utf-8 -*-
"""
O RÓTULO DE UMA OBRA — num lugar só, para as telas não divergirem.

⚑ POR QUE ISTO VIROU MÓDULO PRÓPRIO (10/11/2026): este defeito já tinha sido
consertado em 10/09/2026, e voltou duas vezes — no diário e no portal da obra
— porque cada tela montava "código — nome" à mão. A terceira vez não acontece
se só existir UM lugar que sabe a regra.

A versão de navegador é a `rotuloObra` do `erp_base.html`, e **as duas têm de
concordar sempre**: há teste comparando as regras.
"""
from __future__ import annotations

import unicodedata
from typing import Any


def rotulo_da_obra(codigo: Any, nome: Any) -> str:
    """"081 · Residencial Vale do Sol", mas sem repetir o que já foi dito.

    ⚑ ESTE DEFEITO JÁ TINHA SIDO CONSERTADO EM 10/09/2026, e eu o recriei no
    diário escrevendo `código — nome` à mão. A reclamação dele foi a mesma das
    duas vezes: *"está aparecendo duplicado o nome da obra."* Muita obra tem
    código e nome iguais ou quase ("AERO ARACATI" e "Aero Aracati"), e os dois
    lado a lado fazem a linha parecer um erro de digitação.

    A regra é a mesma do `rotuloObra` do `erp_base.html`: quando um já contém
    o outro, o mais completo basta; só quando dizem coisas diferentes é que os
    dois aparecem. **As duas versões têm de concordar sempre**, e há teste
    comparando as regras.
    """
    cod, nom = str(codigo or "").strip(), str(nome or "").strip()
    if not nom:
        return cod
    if not cod:
        return nom
    def _limpo(t: str) -> str:
        cru = unicodedata.normalize("NFD", t.lower())
        return "".join(c for c in cru
                       if unicodedata.category(c) != "Mn" and c.isalnum())
    c, n = _limpo(cod), _limpo(nom)
    if not c or not n:
        return nom or cod
    if c in n:
        return nom
    if n in c:
        return cod
    # Todas as palavras do código estão no nome: também é repetição.
    def _palavras(t: str) -> list[str]:
        cru = unicodedata.normalize("NFD", t.lower())
        limpo = "".join(c if c.isalnum() else " " for c in cru
                        if unicodedata.category(c) != "Mn")
        return [p for p in limpo.split() if len(p) >= 2]
    do_nome, do_cod = set(_palavras(nom)), _palavras(cod)
    if do_cod and all(p in do_nome for p in do_cod):
        return nom
    return f"{cod} · {nom}"
