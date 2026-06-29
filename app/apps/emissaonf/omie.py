# -*- coding: utf-8 -*-
"""
Atualização do título a receber no Omie após a emissão.
Consulta o título (por codigo_lancamento_integracao) e o ALTERA gravando as
retenções (IR, ISS, PIS, COFINS, CSLL, INSS) e o nº do documento fiscal.

Acúmulo de notas: se o título já tiver nota emitida, o novo número é acrescentado
ao campo, virando ex.: '3001/3072'. Em cancelamento, o número é removido do campo.

Credenciais (aba Credenciais): OMIE_KEY, OMIE_SECRET.
"""
from __future__ import annotations
import requests

URL = "https://app.omie.com.br/api/v1/financas/contareceber/"


def _post(call, param, creds, timeout=40):
    body = {"call": call, "param": [param],
            "app_key": creds["OMIE_KEY"], "app_secret": creds["OMIE_SECRET"]}
    r = requests.post(URL, json=body, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Omie {call} HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    if isinstance(data, dict) and data.get("faultstring"):
        raise RuntimeError(f"Omie {call}: {data['faultstring']}")
    return data


def consultar(creds, codigo_integracao):
    return _post("ConsultarContaReceber",
                 {"codigo_lancamento_integracao": codigo_integracao}, creds)


def _f(v):
    return float(v)


def _split_docs(s):
    """'3001/3072' -> ['3001','3072'] (ignora vazios/espaços)."""
    return [x for x in str(s or "").replace(" ", "").split("/") if x]


def _merge_doc(atual, novo):
    """Acrescenta 'novo' ao campo, mantendo os que já existem. Não duplica."""
    docs = _split_docs(atual)
    novo = str(novo).strip()
    if novo and novo not in docs:
        docs.append(novo)
    return "/".join(docs)


def _remove_doc(atual, alvo):
    """Remove 'alvo' do campo (usado quando uma nota é cancelada)."""
    alvo = str(alvo).strip()
    return "/".join(d for d in _split_docs(atual) if d != alvo)


def _ler_num_doc(consulta):
    """Acha 'numero_documento_fiscal' na resposta do ConsultarContaReceber."""
    if isinstance(consulta, dict):
        if "numero_documento_fiscal" in consulta:
            return consulta["numero_documento_fiscal"]
        for v in consulta.values():
            achou = _ler_num_doc(v)
            if achou:
                return achou
    elif isinstance(consulta, list):
        for v in consulta:
            achou = _ler_num_doc(v)
            if achou:
                return achou
    return ""


def montar_param_retencoes(codigo_integracao, r, doc_final) -> dict:
    """Monta o param do AlterarContaReceber (puro, sem rede). r = ResultadoCalculo.
    doc_final = string pronta do numero_documento_fiscal (ex.: '3001/3072')."""
    fed = r.federais_retidos
    return {
        "codigo_lancamento_integracao": codigo_integracao,
        "numero_documento_fiscal": str(doc_final),
        "retem_inss": "S", "valor_inss": _f(r.inss),
        "retem_iss": "S" if r.iss_retido else "N", "valor_iss": _f(r.iss) if r.iss_retido else 0.0,
        "retem_ir": "S" if "IR" in fed else "N", "valor_ir": _f(r.ir) if "IR" in fed else 0.0,
        "retem_pis": "S" if "PIS" in fed else "N", "valor_pis": _f(r.pis) if "PIS" in fed else 0.0,
        "retem_cofins": "S" if "COFINS" in fed else "N", "valor_cofins": _f(r.cofins) if "COFINS" in fed else 0.0,
        "retem_csll": "S" if "CSLL" in fed else "N", "valor_csll": _f(r.csll) if "CSLL" in fed else 0.0,
    }


def alterar_retencoes(creds, codigo_integracao, r, numero_nota):
    """Lê o título, acumula o nº da nota no documento fiscal (ex.: 3001/3072) e grava
    as retenções calculadas. Retorna (resposta, doc_final).
    Use na PRIMEIRA nota (com r da medição INTEGRAL)."""
    atual = ""
    try:
        atual = _ler_num_doc(consultar(creds, codigo_integracao))
    except Exception:
        atual = ""                      # se a consulta falhar, grava só o novo número
    doc = _merge_doc(atual, numero_nota)
    param = montar_param_retencoes(codigo_integracao, r, doc)
    return _post("AlterarContaReceber", param, creds), doc


def adicionar_numero(creds, codigo_integracao, numero_nota):
    """Apenas ACUMULA o nº da nota no documento fiscal, SEM tocar nas retenções
    (use da 2ª nota parcial em diante — as retenções já são as da medição integral).
    Envia só a chave + o documento, então o Omie preserva os demais campos."""
    atual = ""
    try:
        atual = _ler_num_doc(consultar(creds, codigo_integracao))
    except Exception:
        atual = ""
    doc = _merge_doc(atual, numero_nota)
    param = {
        "codigo_lancamento_integracao": codigo_integracao,
        "numero_documento_fiscal": str(doc),
    }
    return _post("AlterarContaReceber", param, creds), doc


def remover_documento(creds, codigo_integracao, numero_cancelado):
    """Remove o nº de uma nota cancelada do campo numero_documento_fiscal do título.
    Retorna (resposta, doc_final)."""
    atual = _ler_num_doc(consultar(creds, codigo_integracao))
    novo = _remove_doc(atual, numero_cancelado)
    param = {"codigo_lancamento_integracao": codigo_integracao,
             "numero_documento_fiscal": novo}
    return _post("AlterarContaReceber", param, creds), novo
