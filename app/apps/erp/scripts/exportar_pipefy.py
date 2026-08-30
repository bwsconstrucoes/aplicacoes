# ============================================================================
# ERP BWS — scripts/exportar_pipefy.py
# Exporta pipes do Pipefy via GraphQL para JSON — usado no replanejamento do
# formulário de lançamento e na futura integração do pipe Centro de Custo.
#
# COMO USAR (uma vez):
#   1. Token: Pipefy > avatar (canto sup. direito) > Personal access tokens >
#      Generate new token. Coloque no .env:  PIPEFY_TOKEN=seu_token
#   2. Listar os pipes e descobrir os IDs:
#         python scripts/exportar_pipefy.py listar
#   3. Exportar a ESTRUTURA (fases + campos do formulário) de um pipe:
#         python scripts/exportar_pipefy.py estrutura <pipe_id>
#   4. (Opcional) Exportar os CARDS de um pipe (paginado, todos):
#         python scripts/exportar_pipefy.py cards <pipe_id>
#
# Saída: pasta export_pipefy/ com JSONs legíveis — é isso que o Marcelo envia
# ao Claude no chat (estrutura basta para o replanejamento; cards só se quiser
# analisar conteúdo real).
#
# Robustez: verificação de HTTP 200, tratamento de errors[] do GraphQL,
# retry com backoff, paginação por cursor.
# ============================================================================
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")))

from app.apps.erp.db.database import _carregar_dotenv  # reaproveita o carregador de .env

_URL = "https://api.pipefy.com/graphql"
_PASTA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "export_pipefy")

Q_LISTAR = """
{ me { name organizations { id name pipes { id name cards_count } } } }
"""

Q_ESTRUTURA = """
query ($id: ID!) {
  pipe(id: $id) {
    id name
    start_form_fields {
      id label type required description options help
    }
    phases {
      id name done
      fields { id label type required description options help }
    }
    labels { id name }
  }
}
"""

Q_CARDS = """
query ($id: ID!, $after: String) {
  allCards(pipeId: $id, first: 50, after: $after) {
    pageInfo { hasNextPage endCursor }
    edges { node {
      id title current_phase { name } createdAt
      fields { name value }
      labels { name }
    } }
  }
}
"""


def _token() -> str:
    _carregar_dotenv()
    tk = os.environ.get("PIPEFY_TOKEN", "").strip()
    if not tk:
        print("ERRO: defina PIPEFY_TOKEN no .env (Pipefy > Personal access tokens).")
        sys.exit(1)
    return tk


def gql(query: str, variaveis: dict | None = None) -> dict:
    corpo = json.dumps({"query": query, "variables": variaveis or {}}).encode("utf-8")
    for tentativa in range(1, 4):
        try:
            req = urllib.request.Request(_URL, data=corpo, headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_token()}"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                dados = json.loads(resp.read().decode("utf-8"))
            if dados.get("errors"):
                raise RuntimeError("; ".join(e.get("message", "?") for e in dados["errors"]))
            return dados["data"]
        except Exception as e:
            if tentativa == 3:
                print(f"ERRO na chamada GraphQL: {e}")
                sys.exit(1)
            time.sleep(2 ** tentativa)
    raise AssertionError


def _salvar(nome: str, obj) -> str:
    os.makedirs(_PASTA, exist_ok=True)
    caminho = os.path.join(_PASTA, nome)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    return caminho


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in ("listar", "estrutura", "cards"):
        print(__doc__)
        return
    acao = sys.argv[1]

    if acao == "listar":
        d = gql(Q_LISTAR)
        print(f"Usuário: {d['me']['name']}")
        for org in d["me"]["organizations"]:
            print(f"\nOrganização: {org['name']} (id {org['id']})")
            for p in org["pipes"]:
                print(f"  pipe {p['id']:>12}  {p['name']}  ({p['cards_count']} cards)")
        caminho = _salvar("pipes.json", d)
        print(f"\nSalvo em {caminho}")
        return

    if len(sys.argv) < 3:
        print("Informe o pipe_id (veja com: python scripts/exportar_pipefy.py listar)")
        return
    pipe_id = sys.argv[2]

    if acao == "estrutura":
        d = gql(Q_ESTRUTURA, {"id": pipe_id})
        pipe = d["pipe"]
        caminho = _salvar(f"estrutura_{pipe_id}.json", pipe)
        n_start = len(pipe.get("start_form_fields") or [])
        n_fases = len(pipe.get("phases") or [])
        print(f"Pipe: {pipe['name']} — {n_start} campos no formulário inicial, {n_fases} fases.")
        print(f"Salvo em {caminho}  ← envie este arquivo ao Claude.")
        return

    if acao == "cards":
        todos, cursor, pagina = [], None, 0
        while True:
            d = gql(Q_CARDS, {"id": pipe_id, "after": cursor})
            bloco = d["allCards"]
            todos.extend(e["node"] for e in bloco["edges"])
            pagina += 1
            print(f"  página {pagina}: {len(todos)} cards acumulados")
            if not bloco["pageInfo"]["hasNextPage"]:
                break
            cursor = bloco["pageInfo"]["endCursor"]
            time.sleep(0.4)
        caminho = _salvar(f"cards_{pipe_id}.json", todos)
        print(f"{len(todos)} cards exportados. Salvo em {caminho}")


if __name__ == "__main__":
    main()
