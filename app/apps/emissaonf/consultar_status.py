# -*- coding: utf-8 -*-
# Consulta CRUA do status da DPS - mostra a resposta exata da prefeitura,
# sem nenhum filtro, pra vermos a estrutura real do retorno/erro.
# So precisa do TOKEN (consulta nao usa certificado).

import requests

# ===== preencha o token (o mesmo do outro script) =====
TOKEN = "f7e9ab2e-7fb2-492c-9bdf-30c582ef4d0b"
# ======================================================

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
