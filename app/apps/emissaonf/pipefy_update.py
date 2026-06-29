# -*- coding: utf-8 -*-
"""
Atualização do card no Pipefy após a emissão (réplica do que o cenário Make fazia).

Regra de slot (notas A–E): preenche o PRIMEIRO slot cujo Status está vazio.
  A preenchida -> próxima vai para B; A,B -> C; e assim por diante.

Cada emissão atualiza, no slot escolhido: status="Válida", data de emissão,
nº da nota, valor e valor líquido; limpa os campos de entrada da emissão; e
ajusta a etiqueta.
"""
from __future__ import annotations
import re
from decimal import Decimal

# nome do campo de status no card (getCardInfo) por slot
STATUS_CARD = {s: f"Status Nota Fiscal {s}" for s in "ABCDE"}

# field_id (API) de cada slot — A é irregular (legado); B–E seguem o sufixo
CAMPOS_SLOT = {
    "A": {"status": "status_nota_fiscal_1", "data": "data_da_emiss_o",
          "numero": "n_da_nota_fiscal", "valor": "valor_do_recebimento_1",
          "liquido": "valor_do_recebimento_l_quido_a"},
    "B": {"status": "status_nota_fiscal_b", "data": "data_da_emiss_o_b",
          "numero": "n_da_nota_fiscal_b", "valor": "valor_da_nota_fiscal_b",
          "liquido": "valor_do_recebimento_l_quido_b"},
    "C": {"status": "status_nota_fiscal_c", "data": "data_da_emiss_o_c",
          "numero": "n_da_nota_fiscal_c", "valor": "valor_da_nota_fiscal_c",
          "liquido": "valor_do_recebimento_l_quido_c"},
    "D": {"status": "status_nota_fiscal_d", "data": "data_da_emiss_o_d",
          "numero": "n_da_nota_fiscal_d", "valor": "valor_da_nota_fiscal_d",
          "liquido": "valor_do_recebimento_l_quido_d"},
    "E": {"status": "status_nota_fiscal_e", "data": "data_da_emiss_o_e",
          "numero": "n_da_nota_fiscal_e", "valor": "valor_da_nota_fiscal_e",
          "liquido": "valor_do_recebimento_l_quido_e"},
}

# campos de entrada da emissão que são zerados após emitir (limpos de fato, sem hífen)
CAMPOS_LIMPAR = [
    "valor_parcial", "valor_bdi_diferenciado", "n_do_empenho", "observa_es",
    "informar_al_quota_e_ou_dedu_o", "dedu_es_servi_os_materiais",
    "al_quota_de_ir", "al_quota_de_inss", "al_quota_de_iss",
    "banco_para_recebimento", "tipo_de_medi_o", "emiss_o_de_nota_fiscal",
]
ETIQUETA_FIELD = "etiqueta_1"
ETIQUETA_VALOR = "305209507"


class TodasNotasPreenchidas(Exception):
    pass


def detectar_slot(campos_card: dict) -> str:
    """Primeiro slot (A–E) cujo 'Status Nota Fiscal X' está vazio."""
    for s in "ABCDE":
        v = (campos_card.get(STATUS_CARD[s]) or "").strip()
        if not v:
            return s
    raise TodasNotasPreenchidas("Todas as 5 notas (A–E) já estão preenchidas neste card.")


def _fmt_valor(v) -> str:
    """Pipefy guarda sem separador de milhar: 110.124,91 -> 110124,91."""
    s = f"{Decimal(str(v)):.2f}"          # 110124.91
    return s.replace(".", ",")            # 110124,91


def _campo(card_id, fid, valor):
    nv = "null" if valor is None else f"\"{valor}\""
    return (f"updateCardField(input: {{card_id: {card_id} field_id: \"{fid}\" "
            f"new_value: {nv}}}) {{clientMutationId}}")


def montar_mutation(card_id, slot, numero_nota, data_ddmmaaaa, valor, liquido) -> str:
    f = CAMPOS_SLOT[slot]
    partes = [
        _campo(card_id, f["status"], "Válida"),
        _campo(card_id, f["data"], data_ddmmaaaa),
        _campo(card_id, f["numero"], str(numero_nota)),
        _campo(card_id, f["valor"], _fmt_valor(valor)),
        _campo(card_id, f["liquido"], _fmt_valor(liquido)),
    ]
    for fid in CAMPOS_LIMPAR:
        partes.append(_campo(card_id, fid, None))
    partes.append(_campo(card_id, ETIQUETA_FIELD, ETIQUETA_VALOR))
    corpo = "\n".join(f"n{i+1}: {p}" for i, p in enumerate(partes))
    return "mutation {\n" + corpo + "\n}"


def executar(mutation: str, token: str) -> dict:
    """Executa a mutation no Pipefy (GraphQL). Levanta se houver erro."""
    import requests
    r = requests.post("https://api.pipefy.com/graphql", json={"query": mutation},
                      headers={"Authorization": f"Bearer {token}",
                               "Content-Type": "application/json"}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Pipefy HTTP {r.status_code}: {r.text[:300]}")
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(f"Pipefy GraphQL: {data['errors']}")
    return data


# ---------------------------------------------------------------------------
# Atualização do campo 'Descrição' (field_id "descri_o") com os links das NFS-e.
# Lê o valor atual pelo id e coloca os links no topo, preservando o texto antigo.
# ---------------------------------------------------------------------------
DESCRICAO_FIELD_ID = "descri_o"   # campo 'Descrição' (id real, confirmado)


def _esc(s: str) -> str:
    """Escapa uma string para literal GraphQL (aspas, barra e quebras de linha)."""
    return (s or "").replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "")


def obter_valor_campo(card_id, field_id: str, token: str) -> str:
    """Lê o valor atual de um campo do card pelo seu field_id (vazio se não houver)."""
    q = ("query { card(id: %s) { fields { value field { id } } } }" % card_id)
    data = executar(q, token)
    fields = (((data.get("data") or {}).get("card") or {}).get("fields")) or []
    for f in fields:
        if ((f.get("field") or {}).get("id") or "") == field_id:
            return f.get("value") or ""
    return ""


def atualizar_descricao_links(card_id, link_mun, link_nac, link_rec, token,
                              field_id: str = DESCRICAO_FIELD_ID) -> dict:
    """Coloca os links no TOPO do campo 'Descrição', preservando o conteúdo que já
    existe. Renderiza só os links presentes (municipal -> nacional -> recibo).
    Idempotente: um bloco de links anterior no topo é substituído (não duplica)."""
    atual = obter_valor_campo(card_id, field_id, token)
    linhas = atual.split("\n")
    # remove um bloco de links anterior (linhas iniciais que começam com 'Link NFS-e')
    while linhas and linhas[0].strip().startswith("Link NFS-e"):
        linhas.pop(0)
    if linhas and linhas[0].strip() == "":
        linhas.pop(0)
    antiga = "\n".join(linhas).strip()

    partes = []
    if link_mun:
        partes.append(f"Link NFS-e: {link_mun}")
    if link_nac:
        partes.append(f"Link NFS-e Nacional: {link_nac}")
    if link_rec:
        partes.append(f"Link NFS-e Recibo: {link_rec}")
    bloco = "\n".join(partes)
    novo = (bloco + ("\n\n" + antiga if antiga else "")) if bloco else antiga

    mut = ("mutation {\n"
           f"  updateCardField(input: {{card_id: {card_id} field_id: \"{field_id}\" "
           f"new_value: \"{_esc(novo)}\"}}) {{clientMutationId}}\n"
           "}")
    return executar(mut, token)