# -*- coding: utf-8 -*-
"""
EXPORTAR O EXTRATO DA CONCILIAÇÃO EM PDF OU PLANILHA — 14/10/2026.

O dono: *"quero poder exportar o extrato conciliado em pdf ou em planilha"*.

Sai O QUE A TELA ESTÁ MOSTRANDO — a mesma conta e o mesmo filtro (situação,
período, busca, colunas) —, mas TODAS as páginas, em ordem de data, com o
saldo corrido da conta e se a linha está conciliada, por quem e a observação.
O que é anterior à data de início da conta fica de fora, como na tela.
"""
from __future__ import annotations

import io

TETO_DE_LINHAS = 20000

COLUNAS = ["Data", "Histórico", "Documento", "Entrada", "Saída", "Saldo",
           "Conciliado", "Conciliado por", "Observação", "No OMIE"]


def linhas(f: dict) -> list[dict]:
    """Todas as linhas do filtro, da mais antiga para a mais nova."""
    from . import conciliacao as conc
    saida, pagina = [], 1
    while len(saida) < TETO_DE_LINHAS:
        bloco = conc.listar(f, pagina)
        saida.extend(bloco)
        if len(bloco) < conc.POR_PAGINA:
            break
        pagina += 1
    saida.sort(key=lambda l: (l["data"], l["id"]))
    return saida[:TETO_DE_LINHAS]


def _no_omie(l: dict) -> str:
    if l.get("omie_codigo"):
        return f"lançado ({l['omie_codigo']})"
    return str(l.get("omie_situacao") or "")


def _valores(l: dict) -> list:
    from decimal import Decimal
    valor = Decimal(str(l.get("valor") or 0))
    return [l["data"], l.get("descricao") or "", l.get("documento") or "",
            valor if valor > 0 else None, -valor if valor < 0 else None,
            l.get("saldo"), "Sim" if l.get("conciliado") else "Não",
            l.get("conciliado_por") or "", l.get("observacao") or "", _no_omie(l)]


def _titulo(conta: dict, f: dict) -> tuple[str, str]:
    from .formatos import data_br
    partes = []
    if f.get("data_ini") or f.get("data_fim"):
        partes.append(f"de {data_br(f.get('data_ini')) or 'o início'} "
                      f"a {data_br(f.get('data_fim')) or 'hoje'}")
    situacao = {"pendentes": "só o que falta conciliar",
                "conciliados": "só o já conciliado",
                "com_observacao": "só o que tem observação"}.get(f.get("situacao") or "")
    if situacao:
        partes.append(situacao)
    if f.get("busca"):
        partes.append(f"busca: {f['busca']}")
    return (f"Extrato da conciliação - {conta.get('nome', '')}",
            " · ".join(partes) or "todo o extrato")


def xlsx(conta: dict, f: dict, lista: list, resumo: dict) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    titulo, subtitulo = _titulo(conta, f)
    planilha = Workbook()
    aba = planilha.active
    aba.title = "Extrato"
    aba.append([titulo])
    aba["A1"].font = Font(bold=True, size=13)
    aba.append([subtitulo])
    aba.append([])
    aba.append(COLUNAS)
    for celula in aba[4]:
        celula.font = Font(bold=True)
    for l in lista:
        aba.append(_valores(l))
    aba.append([])
    aba.append(["", "Total", "", resumo.get("entradas") or 0,
                abs(resumo.get("saidas") or 0), None,
                f"{(resumo.get('quantidade') or 0) - (resumo.get('pendentes') or 0)} "
                f"de {resumo.get('quantidade') or 0}"])
    larguras = [12, 48, 16, 14, 14, 16, 11, 18, 40, 18]
    for i, largura in enumerate(larguras, start=1):
        aba.column_dimensions[get_column_letter(i)].width = largura
    for linha in aba.iter_rows(min_row=5):
        linha[0].number_format = "DD/MM/YYYY"
        for c in (3, 4, 5):
            linha[c].number_format = '#,##0.00'
    aba.freeze_panes = "A5"
    memoria = io.BytesIO()
    planilha.save(memoria)
    return memoria.getvalue()


def pdf(conta: dict, f: dict, lista: list, resumo: dict) -> bytes:
    from .formatos import moeda
    from .pdf import Folha, _data_br

    titulo, subtitulo = _titulo(conta, f)
    folha = Folha(titulo, subtitulo)
    conciliadas = (resumo.get("quantidade") or 0) - (resumo.get("pendentes") or 0)
    folha.numeros([
        ("Lançamentos", str(resumo.get("quantidade") or 0)),
        ("Conciliados", f"{conciliadas} de {resumo.get('quantidade') or 0}"),
        ("Entrou", "R$ " + moeda(resumo.get("entradas") or 0)),
        ("Saiu", "R$ " + moeda(abs(resumo.get("saidas") or 0))),
    ])
    folha.pdf.ln(2)

    def dinheiro(v):
        return moeda(v) if v not in (None, "") else ""

    tabela = []
    for l in lista:
        v = _valores(l)
        historico = (v[1], v[8]) if v[8] else v[1]       # observação embaixo, pequena
        tabela.append([_data_br(v[0]), historico, v[2], dinheiro(v[3]), dinheiro(v[4]),
                       dinheiro(v[5]), "sim" if v[6] == "Sim" else "-"])
    folha.tabela(["Data", "Histórico", "Documento", "Entrada", "Saída", "Saldo", "Conc."],
                 tabela, larguras=[15, 76, 22, 20, 20, 25, 12], direita=(3, 4, 5),
                 fonte=7, linhas_max=2)
    return folha.bytes()
