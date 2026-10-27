# -*- coding: utf-8 -*-
"""
O FILTRO DE FUNCIONÁRIOS, UM SÓ PARA TODAS AS TELAS (27/10/2026).

O dono: *"precisa da filtragem mais ampla no sidebar. Precisa ser facilitado o filtro
dos funcionários de uma maneira geral. Esses filtros na parte superior não ficam
legal."* O Quadro já tinha o bloco "Quem"; ele passa a ser O filtro de pessoas, igual
no Quadro, nas Validações e nos Relatórios: pessoa (nome ou CPF), obra do cadastro,
obra em que bateu no período, funções (várias de uma vez), escala, situação no
cadastro e tipo de pessoa (vigia, ronda, celular, ponto da obra…). Uma regra só, aqui:
a tela que usa este filtro vê as MESMAS pessoas que o Quadro veria.
"""
from __future__ import annotations

import datetime as dt
from collections import Counter

from sqlalchemy.engine import Connection

from . import escalas, etiquetas as etiquetas_da_pessoa, filtros as filtros_do_ponto
from .ocorrencias import Quem

CAMPOS = ("busca", "obra_cadastro", "obra_batida", "funcoes", "escalas", "situacao_cadastro", "tipos_pessoa")


def ids(valor) -> set[int]:
    """PURA. As obras escolhidas — uma, várias (lista) ou "1,2,3" (28/10/2026, o dono: "para
    obras é mandatório poder selecionar mais de uma; dezenas de obras, escolher só uma ou
    todas não tem sentido"). Vazio = nenhuma escolhida."""
    if valor in (None, "", []):
        return set()
    itens = valor if isinstance(valor, (list, tuple, set)) else str(valor).split(",")
    return {int(x) for x in itens if str(x).strip().isdigit()}


def ativo(f: dict) -> bool:
    """PURA. Algum filtro de pessoa foi escolhido?"""
    return any(f.get(c) for c in CAMPOS)


def do_cadastro(conn: Connection, quem: Quem, pessoas: list[dict], f: dict, inicio: dt.date,
                fim: dt.date) -> list[dict]:
    """O recorte de quem pergunta e os filtros do cadastro (sem escala e sem tipo)."""
    if quem.obras is not None:
        alcance = set(quem.obras)
        pessoas = [p for p in pessoas if p.get("obra_id") in alcance
                   or any(o["id"] in alcance for o in p["obras_adicionais"])]
    busca = str(f.get("busca") or "").strip().lower()
    if busca:
        digitos = "".join(ch for ch in busca if ch.isdigit())
        pessoas = [p for p in pessoas if busca in (p["nome"] or "").lower()
                   or (digitos and digitos in (p.get("cpf") or ""))]
    obras = ids(f.get("obra_cadastro"))
    if obras:
        pessoas = [p for p in pessoas if p.get("obra_id") in obras or any(x["id"] in obras for x in p["obras_adicionais"])]
    obras = ids(f.get("obra_batida"))
    if obras:                                   # bateu em alguma dessas obras no período
        bateram = {int(x["id"]) for o in obras for x in filtros_do_ponto.pessoas_com_batida(conn, o, inicio, fim)}
        pessoas = [p for p in pessoas if int(p["id"]) in bateram]
    funcoes = {str(x).strip().lower() for x in (f.get("funcoes") or []) if str(x).strip()}
    if funcoes:
        pessoas = [p for p in pessoas if (p.get("funcao") or "").strip().lower() in funcoes]
    if f.get("situacao_cadastro"):
        pessoas = [p for p in pessoas if p.get("situacao") == f["situacao_cadastro"]]
    return pessoas


def por_tipo(conn: Connection, pessoas: list[dict], f: dict) -> tuple[list[dict], Counter, dict]:
    """O filtro "tipo de pessoa" (as etiquetas) — e quantos há de cada, antes do filtro."""
    marcas = etiquetas_da_pessoa.das_pessoas(conn, pessoas)
    contagem: Counter = Counter()
    for p in pessoas:
        cods = set(marcas.get(int(p["id"]), []))
        if cods & {"VIGIA_SEM_RONDA", "VIGIA_SEM_CELULAR"}:
            cods.add("VIGIA_PENDENTE")
        contagem.update(cods)
    tipos = {str(x) for x in (f.get("tipos_pessoa") or [])}
    if tipos:
        pessoas = [p for p in pessoas if etiquetas_da_pessoa.casa_com_filtro(marcas.get(int(p["id"]), []), tipos)]
    return pessoas, contagem, marcas


def por_escala(conn: Connection, pessoas: list[dict], f: dict, dia: dt.date) -> list[dict]:
    """A escala que vale no dia (a própria, ou a padrão da empresa para quem não tem)."""
    quer = {str(x).strip().lower() for x in (f.get("escalas") or []) if str(x).strip()}
    if not quer or not pessoas:
        return pessoas
    padrao = escalas.escala_padrao(conn)
    nome = {e["id"]: (e["escala"] or (padrao["nome"] if padrao else "")).strip().lower()
            for e in escalas.pessoas_e_escalas(conn, pessoas, dia)}
    return [p for p in pessoas if nome.get(int(p["id"]), "") in quer]


def aplicar(conn: Connection, quem: Quem, pessoas: list[dict], f: dict, inicio: dt.date,
            fim: dt.date) -> tuple[list[dict], Counter]:
    """Tudo junto: o cadastro, a escala (a de `fim`) e o tipo. Devolve as pessoas e a
    contagem dos tipos (para a barra mostrar quantos há)."""
    pessoas = do_cadastro(conn, quem, pessoas, f, inicio, fim)
    pessoas = por_escala(conn, pessoas, f, fim)
    pessoas, contagem, _ = por_tipo(conn, pessoas, f)
    return pessoas, contagem


def de_argumentos(args) -> dict:
    """PURA. O filtro vindo do endereço (GET): listas separadas por vírgula."""
    lista = lambda k: [x for x in str(args.get(k) or "").split(",") if x.strip()]  # noqa: E731
    return {"busca": args.get("pessoa") or "", "obra_cadastro": lista("obra_cadastro"),
            "obra_batida": lista("obra_batida"), "funcoes": lista("funcoes"),
            "escalas": lista("escalas"), "situacao_cadastro": args.get("situacao_cadastro") or None,
            "tipos_pessoa": lista("tipos_pessoa")}
