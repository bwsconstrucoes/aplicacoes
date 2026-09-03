# -*- coding: utf-8 -*-
"""
updater.py — atualiza SOMENTE o código (pasta app\\) a partir da pasta compartilhada
do Dropbox. Offline-first: se não houver internet, ignora e segue com o código local.

NUNCA toca em dados (spsbd_cache.db) nem em segredos (credenciais.json): protegidos
por extensão (só baixa código) e por nome.
"""
from __future__ import annotations

import os
import io
import sys
import zipfile
import hashlib

# Link da PASTA do Dropbox com dl=1 (devolve um .zip da pasta). Sem o 'st' (token
# temporário) para a URL ficar estável. Mantém o rlkey. NÃO regere este link.
DROPBOX_ZIP = ("https://www.dropbox.com/scl/fo/ppxr18uf5imghn7ny4sn7/"
               "ADSlO-0ycsvv7ctscRx5CJs?rlkey=scwv683zxzands0f0cjf4gnoa&dl=1")

APP_DIR = os.path.dirname(os.path.abspath(__file__))   # a própria pasta app\

# Só estes tipos são atualizados (código). Tudo o mais é ignorado.
EXT_OK = {".py", ".txt", ".md", ".toml", ".cfg", ".csv"}
# Reforço por nome: jamais sobrescrever.
PROTEGIDOS = {"credenciais.json", "service_account.json", "spsbd_cache.db",
              "Atualizar_e_Abrir.bat"}


def _baixar_zip(url: str) -> bytes:
    import requests
    r = requests.get(url, timeout=120, allow_redirects=True)
    r.raise_for_status()
    return r.content


def _raiz_comum(nomes) -> str:
    """Se todos os arquivos estiverem sob uma única pasta de 1º nível, devolve essa raiz."""
    tops = set(n.split("/", 1)[0] for n in nomes if "/" in n)
    arquivo_na_raiz = any("/" not in n for n in nomes)
    if len(tops) == 1 and not arquivo_na_raiz:
        return list(tops)[0] + "/"
    return ""


def _seguro(rel: str) -> bool:
    # bloqueia path traversal e caminhos absolutos
    if not rel or rel.startswith("/") or ".." in rel.replace("\\", "/").split("/"):
        return False
    return True


def aplicar_zip(conteudo: bytes) -> int:
    zf = zipfile.ZipFile(io.BytesIO(conteudo))
    nomes = [i.filename for i in zf.infolist() if not i.is_dir()]
    raiz = _raiz_comum(nomes)
    n = 0
    for info in zf.infolist():
        if info.is_dir():
            continue
        nome = info.filename
        rel = nome[len(raiz):] if raiz and nome.startswith(raiz) else nome
        if not _seguro(rel):
            continue
        base = os.path.basename(rel)
        if base in PROTEGIDOS:
            continue
        if os.path.splitext(base)[1].lower() not in EXT_OK:
            continue
        destino = os.path.join(APP_DIR, rel.replace("/", os.sep))
        os.makedirs(os.path.dirname(destino) or APP_DIR, exist_ok=True)
        with zf.open(info) as src, open(destino, "wb") as out:
            out.write(src.read())
        n += 1
    return n


def atualizar() -> None:
    # Trava de segurança: no PC de desenvolvimento (onde o app/ É a pasta do Dropbox),
    # crie um arquivo vazio chamado 'NAO_ATUALIZAR' nesta pasta para nunca sobrescrever.
    if os.path.exists(os.path.join(APP_DIR, "NAO_ATUALIZAR")):
        print("[updater] 'NAO_ATUALIZAR' presente — pulando atualização (PC de origem).")
        return
    try:
        conteudo = _baixar_zip(DROPBOX_ZIP)
    except Exception as e:
        print(f"[updater] Sem atualização agora ({e}). Abrindo com o código local.")
        return
    h = hashlib.sha256(conteudo).hexdigest()
    marca = os.path.join(APP_DIR, ".update_hash")
    try:
        if os.path.exists(marca) and open(marca, encoding="utf-8").read().strip() == h:
            print("[updater] Já está na última versão.")
            return
    except Exception:
        pass
    try:
        n = aplicar_zip(conteudo)
    except zipfile.BadZipFile:
        print("[updater] O link não devolveu um .zip (verifique se está como dl=1 e se a "
              "pasta tem só código). Abrindo com o código local.")
        return
    try:
        open(marca, "w", encoding="utf-8").write(h)
    except Exception:
        pass
    print(f"[updater] Atualizado: {n} arquivo(s) de código.")


if __name__ == "__main__":
    atualizar()
    sys.exit(0)