# -*- coding: utf-8 -*-
"""
NOME DE PERFIL ERRADO NUMA MIGRAÇÃO NÃO RECLAMA — ELE SÓ NÃO FAZ NADA.

Esta trava nasceu de um defeito de verdade, em 08/11/2026. A migração 092 dava
a seção do diário de obra ao perfil `'Parceiro'`. O perfil se chama **"Parceiro
da obra"**. O `INSERT ... SELECT ... JOIN perfis p ON v.perfil = p.nome`
simplesmente não casou aquela linha e inseriu as outras, **sem erro nenhum** —
e justamente o perfil por onde a FISCALIZAÇÃO entra no diário ficou sem acesso.

O mesmo arquivo trazia `'Financeiro'`, que também não existe: o cargo
FINANCEIRO virou o perfil "Administrativo financeiro" na migração 065. Outra
linha morta, outro silêncio.

É o pior feitio de defeito que existe aqui: a migração roda "com sucesso", o
botão de aplicar fica verde, e alguém descobre meses depois que um perfil
inteiro não enxerga uma tela.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRACOES = Path("app/apps/erp/scripts/migracoes")

# Os nomes que EXISTEM, lidos da migração que cria os perfis — não de uma lista
# escrita à mão aqui, que envelheceria sozinha.
#
# ⚑ SÓ O PRIMEIRO LITERAL DE CADA TUPLA. A primeira versão desta trava pegava
# todas as aspas do INSERT, então as DESCRIÇÕES dos perfis entravam na lista de
# nomes conhecidos — e aí qualquer texto passava. A trava passou verde com o
# defeito injetado de propósito, que é o pior resultado possível: uma rede que
# não pega nada e ainda dá a sensação de que pega.
def _perfis_conhecidos() -> set[str]:
    nomes: set[str] = set()
    for sql in sorted(MIGRACOES.glob("*.sql")):
        texto = sql.read_text(encoding="utf-8")
        for m in re.finditer(r"INSERT\s+INTO\s+perfis\b(.*?);", texto, re.S | re.I):
            # ('Nome do perfil', 'descrição', TRUE) → fica só o primeiro
            nomes |= set(re.findall(r"\(\s*'([^']+)'\s*,", m.group(1)))
    return nomes


def _nomes_usados_no_join() -> dict[str, set[str]]:
    """Os nomes de perfil que cada migração usa num JOIN por nome.

    ⚑ A primeira versão procurava a string "JOIN perfis" e não achava nada: o
    SQL do ERP escreve `FROM perfis p` e depois `JOIN (VALUES …)`. Foi o
    segundo motivo de a trava passar vazia.
    """
    usados: dict[str, set[str]] = {}
    for sql in sorted(MIGRACOES.glob("*.sql")):
        texto = sql.read_text(encoding="utf-8")
        if not re.search(r"\bperfis\s+p\b", texto):
            continue
        achados = set(re.findall(
            r"\(\s*'([^']+)'\s*,\s*'[a-z_]+'\s*,\s*'(?:LER|EDITAR|NADA)'\s*\)",
            texto))
        if achados:
            usados[sql.name] = achados
    return usados


def test_toda_migracao_usa_nome_de_perfil_que_existe():
    conhecidos = _perfis_conhecidos()
    assert conhecidos, (
        "não achei a migração que cria os perfis — esta trava perdeu a "
        "referência e precisa ser consertada, não ignorada")

    erradas: list[str] = []
    for arquivo, nomes in _nomes_usados_no_join().items():
        for nome in sorted(nomes):
            if nome not in conhecidos:
                erradas.append(f"{arquivo}: '{nome}'")
    assert not erradas, (
        "migração dando seção a perfil que NÃO EXISTE — o JOIN por nome não "
        "casa, nada é inserido, e nenhum erro aparece:\n  "
        + "\n  ".join(erradas)
        + f"\n\nOs perfis que existem: {sorted(conhecidos)}")
