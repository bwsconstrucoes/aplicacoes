# -*- coding: utf-8 -*-
# Consulta CRUA do status da DPS - mostra a resposta exata da prefeitura,
# sem nenhum filtro, pra vermos a estrutura real do retorno/erro.
# So precisa do TOKEN (consulta nao usa certificado).

import os

import requests

# O token autentica o canal com a prefeitura e NÃO fica no código: leia da
# Environment do Render (EL_NFSE_TOKEN). Sem ele o script para e explica.
TOKEN = os.getenv("EL_NFSE_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit(
        "Defina EL_NFSE_TOKEN no ambiente antes de rodar esta consulta.")

ID_DPS = "DPS230428520007952600010900001000000000003066"
URLBASE = "https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40"

caminhos = [
    f"api/nacional/homologacao/nfseDps/{ID_DPS}",   # traz a NFS-e (ou erro)
    f"api/nacional/homologacao/dps/{ID_DPS}",        # traz a chave de acesso
]

for path in caminhos:
    url = f"{URLBASE}/{path}"
    try:
        r = requests.get(url, params={"token": TOKEN}, timeout=60)
        print("=" * 64)
        print("GET", path)
        print("HTTP", r.status_code)
        print("RESPOSTA:")
        print(r.text[:3000])
        print()
    except Exception as e:
        print("ERRO de conexao em", path, ":", e)
