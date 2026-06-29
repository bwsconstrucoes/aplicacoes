# -*- coding: utf-8 -*-
"""
Carregador da C. Diários (aba "Centro de Custo" da planilha base).
Desacoplado da fonte: recebe as linhas (ex.: de gspread get_all_values) + o cabeçalho
e devolve as obras indexadas por Código Primário.

Em produção o worker faz:
    import gspread
    ws = gspread.authorize(cred).open_by_key(ID_BASE).worksheet("Centro de Custo")
    linhas = ws.get_all_values()
    obras = carregar_obras(linhas)
    obra = obras["CREPEEXU"]
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Obra:
    codigo_primario: str
    centro_custo: str
    municipio: str           # ex.: "Exu-PE"  (nome+UF; resolvido p/ IBGE depois)
    uf: str
    valor: str
    aliquota_iss: str        # col Alíquota ISS
    tributacao: str          # col Tributação (categoria 4 blocos)
    cno: str
    cliente: str
    cnpj_cliente: str
    endereco_cliente: str
    contrato: str
    objeto: str
    codigo_omie: str
    conta_pagamento: str
    num_centro_custo: str
    bruto: dict              # linha inteira, p/ qualquer campo extra


# nomes de coluna esperados na C. Diários (tolerante a espaços/acentos)
_MAPA = {
    "codigo_primario": "Código Primário",
    "centro_custo": "Centro de Custo",
    "municipio": "Município",
    "uf": "UF",
    "valor": "Valor",
    "aliquota_iss": "Alíquota ISS",
    "tributacao": "Tributação",
    "cno": "CNO",
    "cliente": "Cliente",
    "cnpj_cliente": "CNPJ Cliente",
    "endereco_cliente": "Endereço Cliente",
    "contrato": "Contrato",
    "objeto": "Objeto",
    "codigo_omie": "Código Omie",
    "conta_pagamento": "Conta de Pagamento",
    "num_centro_custo": "Nº Centro de Custo",
}


def _idx(headers):
    norm = {h.strip().lower(): i for i, h in enumerate(headers)}
    return {campo: norm.get(col.strip().lower()) for campo, col in _MAPA.items()}


def carregar_obras(linhas: list[list[str]]) -> dict[str, Obra]:
    if not linhas:
        return {}
    headers = linhas[0]
    idx = _idx(headers)
    obras = {}
    for row in linhas[1:]:
        def g(campo):
            i = idx.get(campo)
            return (row[i].strip() if i is not None and i < len(row) else "")
        cod = g("codigo_primario")
        if not cod:
            continue
        bruto = {headers[i].strip(): (row[i] if i < len(row) else "") for i in range(len(headers))}
        obras[cod] = Obra(
            codigo_primario=cod, centro_custo=g("centro_custo"), municipio=g("municipio"),
            uf=g("uf"), valor=g("valor"), aliquota_iss=g("aliquota_iss"),
            tributacao=g("tributacao"), cno=g("cno"), cliente=g("cliente"),
            cnpj_cliente=g("cnpj_cliente"), endereco_cliente=g("endereco_cliente"),
            contrato=g("contrato"), objeto=g("objeto"), codigo_omie=g("codigo_omie"),
            conta_pagamento=g("conta_pagamento"), num_centro_custo=g("num_centro_custo"),
            bruto=bruto,
        )
    return obras


def buscar_obra(codigo: str, obras: dict[str, Obra]) -> Obra:
    obra = obras.get(codigo.strip())
    if obra is None:
        raise KeyError(f"Obra '{codigo}' não encontrada na C. Diários.")
    return obra
