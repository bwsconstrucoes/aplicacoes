# -*- coding: utf-8 -*-
"""
Monta a linha da aba 'Notas BWS' (colunas A–P). As colunas Q–BA são fórmulas
na planilha e NÃO são escritas (preenchem sozinhas).

Mapeamento (confirmado pelo cabeçalho + linha real CREPEEXU/3067):
 A Código Obra | B MM/YYYY | C MM | D Mês | E Ano | F Nº Nota | G Data Emissão
 H Valor da Nota | I Código Obra | J Nº Med. | K RF | L Observação
 M Data de Recebimento | N Valor Recebido em Conta
 O Valor a ser Recebido pelo Destaque (= líquido, valor - retenções)
 P Valor Líquido Tributado (= valor - todos os federais cheios: PIS 0,65 / COFINS 3 / IR 1,2 / CSLL 1,08 / INSS / ISS)
"""
from __future__ import annotations
from decimal import Decimal
from preview import brl

MESES = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
         "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]


def _num_medicao(card) -> str:
    """Nº da medição para a coluna J. Quando o 'Tipo de Documento' do card é de
    REAJUSTE (ex.: 'Solicitação de Pagamento de Medição de Reajuste'), o número
    recebe o sufixo 'R' (ex.: '7R')."""
    num = str(card.get("numero_medicao", "") or "").strip()
    tipo_doc = str(card.get("tipo_documento", "") or "").upper()
    if num and "REAJUSTE" in tipo_doc and not num.upper().endswith("R"):
        return f"{num}R"
    return num


def montar_linha(card, obra, r, numero, data_emissao_iso) -> list:
    a, m, d = data_emissao_iso.split("-")
    mes_nome = MESES[int(m)]
    valor = r.valor_total

    # P: Valor Líquido Tributado = valor - todos os federais a cheio, cada um
    # arredondado a 2 casas ANTES de somar (igual à planilha). CSLL aqui é 1,08%.
    csll_108 = (Decimal("0.0108") * valor).quantize(Decimal("0.01"))
    liquido_tributado = (valor - r.inss - r.iss - r.ir - r.pis - r.cofins - csll_108)

    return [
        card.get("codigo_obra", ""),                 # A Código Obra
        f"{mes_nome} / {a}",                          # B MM/YYYY
        str(int(m)),                                  # C MM
        mes_nome.upper(),                             # D Mês
        a,                                            # E Ano
        numero,                                       # F Nº Nota
        f"{int(d):02d}/{int(m):02d}/{a}",             # G Data Emissão
        brl(valor),                                   # H Valor da Nota
        card.get("codigo_obra", ""),                  # I Código Obra
        _num_medicao(card),                           # J Nº Med. ("7R" se Reajuste)
        "",                                           # K RF
        "",                                           # L Observação
        "",                                           # M Data de Recebimento
        "",                                           # N Valor Recebido em Conta
        brl(r.valor_liquido),                         # O Valor a ser Recebido pelo Destaque (líquido)
        brl(liquido_tributado),                       # P Valor Líquido Tributado
    ]


def ja_existe(ws, numero) -> bool:
    """True se o número já está na coluna F (Nº Nota) da Notas BWS."""
    col = ws.col_values(6)  # F
    alvo = str(numero).strip()
    return any(c.strip() == alvo for c in col)


def gravar_linha(ws, card, obra, r, numero, data_emissao_iso) -> bool:
    """Acrescenta a linha A–P na Notas BWS. Não grava se o número já existir."""
    if ja_existe(ws, numero):
        return False
    linha = montar_linha(card, obra, r, numero, data_emissao_iso)
    ws.append_row(linha, value_input_option="USER_ENTERED", table_range="A1")
    return True


def gravar_links(ws, numero, obra, ano, nome_base, link_municipal="", link_nacional="", link_recibo="") -> None:
    """Acrescenta a linha na Notas BWS Links.
    Colunas: A id | B numero | C ano | D obra | E nome_base |
             F link NFS-e (municipal) | G link NFS-e Nacional | H link Recibo
    """
    linha = [f"{numero} - {obra}", numero, ano, obra, nome_base,
             link_municipal, link_nacional, link_recibo]
    ws.append_row(linha, value_input_option="USER_ENTERED", table_range="A1")


def atualizar_links_nota(ws, numero, link_municipal=None, link_nacional=None, link_recibo=None) -> bool:
    """Preenche os links na linha já existente (casada pela coluna B):
    F=municipal, G=nacional, H=recibo. Atualiza só os passados (não-None)."""
    col = ws.col_values(2)   # coluna B = numero
    alvo = str(numero).strip()
    for i, v in enumerate(col, start=1):
        if str(v).strip() == alvo:
            if link_municipal is not None:
                ws.update(f"F{i}", [[link_municipal]], value_input_option="USER_ENTERED")
            if link_nacional is not None:
                ws.update(f"G{i}", [[link_nacional]], value_input_option="USER_ENTERED")
            if link_recibo is not None:
                ws.update(f"H{i}", [[link_recibo]], value_input_option="USER_ENTERED")
            return True
    return False
