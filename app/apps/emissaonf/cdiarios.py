# -*- coding: utf-8 -*-
"""
Carregador da C. Diários (aba "Centro de Custo" da planilha base).
Desacoplado da fonte: recebe as linhas (ex.: de gspread get_all_values) + o cabeçalho
e devolve as obras indexadas pelo código da obra.

DOIS CÓDIGOS, NÃO UM. A planilha identifica a mesma obra por dois códigos: o
PRIMÁRIO, na coluna "Código Primário", e o SECUNDÁRIO, na primeira coluna (A).
O card do Pipefy pode trazer qualquer um dos dois — foi o que aconteceu com a
obra AREFORTAL09, que existia na planilha e mesmo assim a emissão recusou.
Por isso o índice tem os dois, e a busca tenta o primário primeiro; só se não
achar é que cai no secundário. O primário nunca é encoberto: se um secundário
repetir um código primário de outra linha, o primário é que vale.

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
    codigo_secundario: str   # coluna A da planilha — o outro código da mesma obra
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


# A primeira coluna da planilha (A) guarda o código secundário da obra. É por
# POSIÇÃO, não por nome: o cabeçalho dela varia, e o que não varia é ser a
# primeira coluna.
COL_CODIGO_SECUNDARIO = 0


def _chave(v) -> str:
    """Normaliza um código para comparação (sem espaços, tudo maiúsculo)."""
    return str(v or "").strip().upper()


def _idx(headers):
    norm = {h.strip().lower(): i for i, h in enumerate(headers)}
    return {campo: norm.get(col.strip().lower()) for campo, col in _MAPA.items()}


def carregar_obras(linhas: list[list[str]]) -> dict[str, Obra]:
    """Indexa as obras pelos DOIS códigos: o primário e o secundário (coluna A).

    Duas passadas, e a ordem importa: primeiro entram todos os códigos
    primários, depois os secundários que ainda não estiverem ocupados. Assim um
    código secundário nunca encobre o primário de outra linha.
    """
    if not linhas:
        return {}
    headers = linhas[0]
    idx = _idx(headers)
    obras: dict[str, Obra] = {}
    secundarios: list[tuple[str, Obra]] = []

    for row in linhas[1:]:
        def g(campo):
            i = idx.get(campo)
            return (row[i].strip() if i is not None and i < len(row) else "")
        cod = g("codigo_primario")
        cod2 = (row[COL_CODIGO_SECUNDARIO].strip()
                if len(row) > COL_CODIGO_SECUNDARIO else "")
        if not cod and not cod2:
            continue
        bruto = {headers[i].strip(): (row[i] if i < len(row) else "") for i in range(len(headers))}
        obra = Obra(
            codigo_primario=cod, codigo_secundario=cod2,
            centro_custo=g("centro_custo"), municipio=g("municipio"),
            uf=g("uf"), valor=g("valor"), aliquota_iss=g("aliquota_iss"),
            tributacao=g("tributacao"), cno=g("cno"), cliente=g("cliente"),
            cnpj_cliente=g("cnpj_cliente"), endereco_cliente=g("endereco_cliente"),
            contrato=g("contrato"), objeto=g("objeto"), codigo_omie=g("codigo_omie"),
            conta_pagamento=g("conta_pagamento"), num_centro_custo=g("num_centro_custo"),
            bruto=bruto,
        )
        if cod:
            obras[_chave(cod)] = obra
        if cod2 and _chave(cod2) != _chave(cod):
            secundarios.append((_chave(cod2), obra))

    for chave, obra in secundarios:
        obras.setdefault(chave, obra)      # o primário, já registrado, ganha
    return obras


def buscar_obra(codigo: str, obras: dict[str, Obra]) -> Obra:
    """Acha a obra pelo código primário ou, na falta dele, pelo secundário.

    Só levanta erro depois de tentar os dois — o card do Pipefy pode trazer
    qualquer um dos dois códigos da mesma obra.
    """
    obra = obras.get(_chave(codigo))
    if obra is None:
        raise KeyError(
            f"Obra '{codigo}' não encontrada na C. Diários — procurei tanto na "
            f"coluna 'Código Primário' quanto no código da primeira coluna (A). "
            f"Confira o código da obra no card e na planilha."
        )
    return obra
