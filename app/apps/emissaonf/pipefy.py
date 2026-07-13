# -*- coding: utf-8 -*-
"""
Entrada via Pipefy: getCardInfo por ID e extração dos campos que a emissão precisa.
"""
from __future__ import annotations
import json
import re
import unicodedata
import requests

URL = "https://api.pipefy.com/graphql"


def get_card(card_id, token: str) -> dict:
    query = ("{ card(id: %s) { id title fields { name value report_value "
             "field { id label } } } }" % str(card_id).strip())
    r = requests.post(URL, json={"query": query},
                      headers={"Authorization": f"Bearer {token}",
                               "Content-Type": "application/json"}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Pipefy HTTP {r.status_code}: {r.text[:300]}")
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(f"Pipefy GraphQL: {data['errors']}")
    return data["data"]["card"]


def _v(val):
    """Normaliza valores: listas JSON (['X']) viram o 1º item."""
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            arr = json.loads(val)
            return arr[0] if arr else ""
        except Exception:
            return val
    return val if val is not None else ""


def _num(brl: str) -> str:
    """'121.954,49' -> '121954.49' (string decimal p/ o motor). Vazio vira '0'."""
    s = str(brl).strip()
    if not s:
        return "0"
    s = re.sub(r"[^\d,.-]", "", s).replace(".", "").replace(",", ".")
    return s or "0"


def _chave_label(s) -> str:
    """Normaliza o rótulo do campo p/ comparação: sem acento, sem caixa, sem
    espaços extras e sem pontuação de borda (':', '.'). Evita que o campo não
    seja encontrado por diferença boba de rótulo no Pipefy."""
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = " ".join(t.split()).strip(" :.\u00a0")
    return t.upper()


def extrair_card(card: dict) -> dict:
    campos = {f["name"]: _v(f.get("value")) for f in card.get("fields", [])}
    campos_por_id = {(f.get("field") or {}).get("id"): _v(f.get("value"))
                     for f in card.get("fields", []) if (f.get("field") or {}).get("id")}
    # índice normalizado: rótulo "limpo" -> valor
    campos_norm = {_chave_label(k): v for k, v in campos.items()}

    def pega(*nomes):
        for n in nomes:
            if campos.get(n) not in (None, "", []):     # match exato (rápido)
                return campos[n]
        for n in nomes:                                  # match tolerante
            v = campos_norm.get(_chave_label(n))
            if v not in (None, "", []):
                return v
        return ""

    return {
        "card_id": str(card.get("id", "")),
        "numero_medicao": pega("Número da Medição"),
        "periodo_ini": pega("Período de Início da Medição"),
        "periodo_fim": pega("Período de Término da Medição"),
        "valor_medicao": _num(pega("Valor da Medição")),
        "contrato": pega("Contrato"),
        "contratante": pega("Contratante"),
        "cnpj_contratante": pega("CNPJ Contratante"),
        "objeto": pega("Objeto"),
        "codigo_obra": pega("Código de Obra", "Código Primário", "Conexão Centros de Custo"),
        "bdi": _num(pega("Valor BDI Diferenciado", "Valor BDI", "valorbdidiferenciado")),
        "omie_titulo": pega("Código Lançamento Omie Título à Receber"),
        "omie_integracao": pega("Código Integração Omie Título à Receber"),
        # --- fase Medições: valor da nota e overrides ---
        "emissao_nf": pega("Emissão de Nota Fiscal"),
        "valor_parcial": _num(pega("Valor Parcial")),
        "tipo_medicao": pega("Tipo de Medição"),
        "tipo_documento": pega("Tipo de Documento", "Tipo do Documento", "Tipo Documento"),
        "banco": pega("Banco para Recebimento"),
        "empenho": pega("Nº do Empenho", "N do Empenho", "Número do Empenho"),
        "observacoes": pega("Observações", "Observacoes", "Observação"),
        "informar_aliq_ded": pega("Informar Alíquota e ou Dedução", "Informar Alíquota e ou Dedução:"),
        "deducoes_split": pega("Deduções (Serviços/Materiais)", "Deduções (Serviços / Materiais)"),
        "aliq_ir": _num(pega("Alíquota de IR")),
        "aliq_inss": _num(pega("Alíquota de INSS")),
        "aliq_iss": _num(pega("Alíquota de ISS")),
        "aliq_pis": _num(pega("Alíquota de PIS")),
        "aliq_cofins": _num(pega("Alíquota de COFINS")),
        "aliq_csll": _num(pega("Alíquota de CSLL")),
        "campos_raw": campos,
        "campos_por_id": campos_por_id,
    }
