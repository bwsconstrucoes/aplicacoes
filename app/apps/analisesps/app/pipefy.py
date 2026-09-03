# -*- coding: utf-8 -*-
"""
pipefy.py — cliente GraphQL do Pipefy (token vem do config.py).

Portado de `gerarPlanilhasBeeVale`: lê cards em lote, busca a database BeeVale por
CPF e (opcional) marca a Documentação Fiscal como 'BeeVale'.
NOTA: as chamadas de rede só funcionam ao vivo (api.pipefy.com).
"""
from __future__ import annotations

import re
import json

import config

API = "https://api.pipefy.com/graphql"
DATABASE_ID = "307056545"
FIELD_DESCRICAO = "descri_o"
FIELD_CADASTRO = "cadastro_bee_vale"
FIELD_VALOR = "valor"
FIELD_DOC_FISCAL = "documenta_o_fiscal"
BATCH = 20


def _token() -> str:
    t = config.get_token("PIPEFY_TOKEN")
    if not t:
        raise RuntimeError("Token do Pipefy não configurado (PIPEFY_TOKEN).")
    return t


def graphql(query: str, token: str | None = None) -> dict:
    import requests
    token = token or _token()
    resp = requests.post(API, json={"query": query},
                         headers={"Authorization": "Bearer " + token,
                                  "Content-Type": "application/json"}, timeout=60)
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Resposta inválida do Pipefy. HTTP {resp.status_code}: "
                           f"{resp.text[:300]}")
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(f"Erro HTTP Pipefy {resp.status_code}: {resp.text[:300]}")
    if data.get("errors"):
        raise RuntimeError("Erro GraphQL Pipefy: " + json.dumps(data["errors"])[:400])
    return data.get("data", {}) or {}


def _chunk(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def _gql_str(v) -> str:
    return json.dumps(str("" if v is None else v))


def extract_cpf(connector_raw) -> str:
    """Extrai o CPF do valor do conector 'Cadastro BeeVale' (JSON array ou regex)."""
    if not connector_raw:
        return ""
    try:
        parsed = json.loads(connector_raw)
        if isinstance(parsed, list) and parsed:
            return str(parsed[0] or "").strip()
    except Exception:
        pass
    m = re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}", str(connector_raw))
    return m.group(0) if m else ""


def fetch_cards(ids, token=None) -> dict:
    token = token or _token()
    out = {}
    for chunk in _chunk([str(i) for i in ids], BATCH):
        parts = [f'c{i}: card(id: {int(cid)}) {{ id fields {{ field {{ id label }} value }} }}'
                 for i, cid in enumerate(chunk)]
        data = graphql("query { " + "\n".join(parts) + " }", token)
        for card in (data or {}).values():
            if not card:
                continue
            fbi = {}
            for f in (card.get("fields") or []):
                fid = (f.get("field") or {}).get("id")
                if fid:
                    fbi[fid] = "" if f.get("value") is None else str(f.get("value"))
            out[str(card["id"])] = {"id": str(card["id"]), "fieldsById": fbi}
    return out


def fetch_beevale_records(cpfs, token=None) -> dict:
    token = token or _token()
    cpf_to_rid = {}
    for chunk in _chunk(list(cpfs), BATCH):
        parts = [f'r{i}: findRecords(tableId: "{DATABASE_ID}", '
                 f'search: {{ fieldId: "cpf", fieldValue: {_gql_str(cpf)} }}) '
                 f'{{ edges {{ node {{ id title }} }} }}'
                 for i, cpf in enumerate(chunk)]
        data = graphql("query { " + "\n".join(parts) + " }", token)
        for wrapper in (data or {}).values():
            edges = (wrapper or {}).get("edges") or []
            if not edges:
                continue
            node = edges[0].get("node") or {}
            cpf = str(node.get("title", "")).strip()
            rid = str(node.get("id", "")).strip()
            if cpf and rid:
                cpf_to_rid[cpf] = rid

    out = {}
    for chunk in _chunk(list(cpf_to_rid.items()), BATCH):
        parts = [f't{i}: table_record(id: {int(rid)}) '
                 f'{{ id title record_fields {{ indexName name value }} }}'
                 for i, (cpf, rid) in enumerate(chunk)]
        data = graphql("query { " + "\n".join(parts) + " }", token)
        for node in (data or {}).values():
            if not node:
                continue
            byname = {}
            for f in (node.get("record_fields") or []):
                nm = str(f.get("name", "")).strip().lower()
                ix = str(f.get("indexName", "")).strip().lower()
                val = "" if f.get("value") is None else str(f.get("value")).strip()
                if nm:
                    byname[nm] = val
                if ix:
                    byname[ix] = val
            nome = byname.get("nome completo") or byname.get("nome_completo") or ""
            nasc = byname.get("data de nascimento") or byname.get("data_de_nascimento") or ""
            tel = byname.get("telefone celular") or byname.get("telefone_celular") or ""
            cpfv = byname.get("cpf") or str(node.get("title", "")).strip()
            out[cpfv] = {"id": str(node.get("id", "")), "nome_completo": nome,
                         "data_de_nascimento": nasc, "telefone_celular": tel, "cpf": cpfv}
    return out


PIPE_SOLICITACOES_ADM = 301426645          # pipe onde nascem os pedidos de cancelamento
_CANCEL_MOTIVO = "Cancelamento em Lote - Análise de SPs Streamlit"
_CANCEL_SOLICITANTE = "620439304"


def _card_cancel_input(sp_id: str) -> str:
    campos = [
        ("selecione_o_procedimento", "Cancelar SP"),
        ("n_da_solicita_o", str(sp_id)),
        ("motivo", _CANCEL_MOTIVO),
        ("colaborador_solicitante", _CANCEL_SOLICITANTE),
    ]
    fa = ", ".join(f'{{field_id: "{fid}", field_value: {_gql_str(val)}}}'
                   for fid, val in campos)
    return (f'createCard(input: {{ pipe_id: {PIPE_SOLICITACOES_ADM}, '
            f'fields_attributes: [{fa}] }}) {{ card {{ id }} }}')


def criar_cards_cancelamento(sp_ids, token=None) -> list:
    """Cria um card de 'Cancelar SP' no pipe {PIPE_SOLICITACOES_ADM} para cada SP,
    em LOTE (mutations com alias na mesma requisição). Se um lote falhar, tenta
    os cards daquele lote um a um (salva os bons e aponta os ruins).
    Retorna [{'sp': id, 'ok': bool, 'card_id'|'erro': ...}, ...]."""
    token = token or _token()
    out = []
    ids = [str(i) for i in sp_ids if str(i).strip()]
    for chunk in _chunk(ids, BATCH):
        parts = [f"m{i}: {_card_cancel_input(sp)}" for i, sp in enumerate(chunk)]
        try:
            data = graphql("mutation { " + "\n".join(parts) + " }", token)
            for i, sp in enumerate(chunk):
                res = (data or {}).get(f"m{i}") or {}
                cid = ((res.get("card") or {}).get("id"))
                if cid:
                    out.append({"sp": sp, "ok": True, "card_id": str(cid)})
                else:
                    out.append({"sp": sp, "ok": False,
                                "erro": "Pipefy não retornou o card."})
        except Exception as e_lote:
            # lote falhou -> tenta um a um para não perder os bons
            for sp in chunk:
                try:
                    d1 = graphql("mutation { " + _card_cancel_input(sp) + " }", token)
                    cid = (((d1 or {}).get("createCard") or {}).get("card") or {}).get("id")
                    if cid:
                        out.append({"sp": sp, "ok": True, "card_id": str(cid)})
                    else:
                        out.append({"sp": sp, "ok": False,
                                    "erro": "Pipefy não retornou o card."})
                except Exception as e1:
                    out.append({"sp": sp, "ok": False, "erro": str(e1)[:300]})
    return out


def marcar_doc_fiscal_beevale(ids, token=None) -> bool:
    token = token or _token()
    for chunk in _chunk([str(i) for i in ids], BATCH):
        parts = [f'm{i}: updateFieldsValues(input: {{ nodeId: {int(cid)}, '
                 f'values: [{{ fieldId: "{FIELD_DOC_FISCAL}", value: "BeeVale" }}] }}) '
                 f'{{ success }}'
                 for i, cid in enumerate(chunk)]
        data = graphql("mutation { " + "\n".join(parts) + " }", token)
        for alias, res in (data or {}).items():
            if not res or res.get("success") is not True:
                raise RuntimeError(f"Falha ao atualizar card ({alias}).")
    return True


def atualizar_descricao_e_doc_fiscal(updates, token=None) -> list:
    """Atualiza, em lote, a DESCRIÇÃO (descri_o) e a Documentação Fiscal ('BeeVale')
    dos cards. updates: [{'cardId': str, 'descricao': str}, ...].
    Retorna a lista de cardIds que FALHARAM (vazia = tudo certo). Mesmo formato do
    updateDescriptionsBatch_ do Apps Script."""
    token = token or _token()
    falhas = []
    for chunk in _chunk(list(updates), BATCH):
        parts = []
        for i, u in enumerate(chunk):
            parts.append(
                f'm{i}: updateFieldsValues(input: {{ nodeId: {int(u["cardId"])}, values: ['
                f'{{ fieldId: "{FIELD_DESCRICAO}", value: {_gql_str(u["descricao"])} }}, '
                f'{{ fieldId: "{FIELD_DOC_FISCAL}", value: "BeeVale" }} ] }}) {{ success }}')
        data = graphql("mutation { " + "\n".join(parts) + " }", token)
        for i, u in enumerate(chunk):
            res = (data or {}).get(f"m{i}")
            if not res or res.get("success") is not True:
                falhas.append(str(u["cardId"]))
    return falhas
