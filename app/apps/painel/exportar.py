# -*- coding: utf-8 -*-
"""
Levar os números do painel para uma planilha.

**Por que CSV e não .xlsx.** Gerar Excel de verdade exigiria uma biblioteca
nova no serviço, e a regra da casa é não acrescentar dependência sem combinar.
O CSV abre no Excel com dois cliques e não custa nada — desde que seja escrito
do jeito certo, que é o que este módulo faz:

  - separador **ponto-e-vírgula**, que é o que o Excel em português espera;
  - decimal com **vírgula**, senão 1.234,56 vira texto;
  - **BOM** no começo do arquivo, senão os acentos viram caracteres estranhos.

Esses três detalhes são a diferença entre "abriu certinho" e "abriu tudo numa
coluna só, sem acento". Se um dia a formatação (negrito, cores, várias abas)
passar a fazer falta, aí sim vale conversar sobre a biblioteca.
"""
from __future__ import annotations

import csv
import io
import datetime as dt


def _numero(valor) -> str:
    """Número no formato que o Excel brasileiro entende: vírgula decimal."""
    if valor is None:
        return ""
    if isinstance(valor, (int,)) and not isinstance(valor, bool):
        return str(valor)
    if isinstance(valor, float):
        return f"{valor:.2f}".replace(".", ",")
    return str(valor)


def _celula(valor) -> str:
    if valor is None:
        return ""
    if isinstance(valor, (dt.date, dt.datetime)):
        return valor.strftime("%d/%m/%Y")
    if isinstance(valor, bool):
        return "sim" if valor else "não"
    if isinstance(valor, (int, float)):
        return _numero(valor)
    return str(valor)


def montar_csv(colunas, linhas) -> bytes:
    """`colunas` são pares (chave, título). `linhas` são dicionários."""
    buffer = io.StringIO(newline="")
    escritor = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_MINIMAL,
                          lineterminator="\r\n")
    escritor.writerow([titulo for _chave, titulo in colunas])
    for linha in linhas:
        escritor.writerow([_celula(linha.get(chave)) for chave, _titulo in colunas])
    # BOM primeiro: é o que faz o Excel entender que o arquivo é UTF-8
    return b"\xef\xbb\xbf" + buffer.getvalue().encode("utf-8")


def nome_do_arquivo(assunto: str) -> str:
    """Nome com data, para não sobrescrever o download da semana passada."""
    return f"{assunto}_{dt.date.today():%Y-%m-%d}.csv"


# ---------------------------------------------------------------------------
# O que cada tela exporta
# ---------------------------------------------------------------------------
COLUNAS = {
    "dre": [("linha", "Linha"), ("executado", "Executado"),
            ("aberto", "Em aberto"), ("comprometido", "Comprometido")],
    "despesas": [("nome", "Grupo ou categoria"), ("valor", "Valor")],
    "medicoes": [("medicao", "Medição"), ("cliente", "Cliente"),
                 ("obra", "Obra"), ("projeto", "Projeto"),
                 ("documento", "Documento"), ("data", "Data"),
                 ("bruto", "Bruto"), ("recebido", "Recebido"),
                 ("retido", "Retido na fonte"), ("a_receber", "A receber"),
                 ("situacao", "Situação")],
    "fluxo": [("rotulo", "Mês"), ("entradas", "Entradas"), ("saidas", "Saídas"),
              ("liquido", "No mês"), ("acumulado", "Acumulado")],
    "obras": [("nome", "Projeto ou obra"), ("receita", "Receita líquida"),
              ("despesa", "Despesas"), ("resultado", "Resultado")],
    "execucao": [("nome", "Projeto ou obra"), ("executado", "Executado"),
                 ("a_executar", "A executar"), ("comprometido", "Comprometido"),
                 ("pct", "% andado")],
    "credores": [("nome", "Fornecedor"), ("pago", "Já pago"),
                 ("aberto", "A pagar"), ("titulos", "Títulos")],
    "quotas": [("socio", "Sócio"), ("tipo", "Tipo"), ("projeto", "Projeto"),
               ("pct", "%"), ("base", "Base de cálculo"),
               ("credito_bws", "Crédito BWS"), ("quota", "Quota"),
               ("visao", "Como foi calculado")],
    "posicao": [("socio", "Sócio"), ("tipo", "Tipo"), ("projetos", "Projetos"),
                ("quota", "Quota"), ("ajustes", "Ajustes"), ("saldo", "Saldo")],
    "caixa": [("rotulo", "Mês"), ("conjunto_a", "Conjunto A"),
              ("resto", "Resto das obras"), ("empresa", "Empresa inteira"),
              ("emprestimo_liquido", "Empréstimo líquido"),
              ("caixa_reconstruido", "Caixa reconstruído")],
}


def linhas_do_dre(dre: dict) -> list[dict]:
    """O DRE é montado por partes na tela; aqui ele vira uma lista corrida."""
    saida = [dre["receita_bruta"], dre["retencoes"], dre["receita_liquida"]]
    saida.extend(dre["despesas"])
    saida.append(dre["total_despesas"])
    saida.append(dre["resultado"])
    return saida
