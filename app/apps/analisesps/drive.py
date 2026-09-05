# -*- coding: utf-8 -*-
"""
Subida de arquivo para o Google Drive, com link de leitura por link.

Usa a MESMA service account de todo o resto (`GOOGLE_CREDENTIALS_BASE64`) —
nenhuma credencial nova, como manda a regra da casa.

FALA REST DIRETO, sem `google-api-python-client`. O `emissaonf` usa a
biblioteca; aqui não, e é de propósito: o pacote arrasta um monte de
dependência para três chamadas HTTP, e este serviço já morreu de falta de
memória uma vez (§9 do CONTEXTO.md). A autenticação vem do `google-auth`, que
já está instalado por causa do gspread.

⚠️ A ARMADILHA DA COTA, e é a razão de este módulo ter ficado de fora até
agora. A service account **não tem espaço de armazenamento próprio**. Ela
consegue criar arquivo dentro de uma pasta de **Drive Compartilhado** (Shared
Drive) onde seja membro com permissão de gravar. Numa pasta de "Meu Drive"
comum — mesmo compartilhada com ela como Editor — o Google recusa com um erro
de cota, e a mensagem que ele devolve não diz isso com todas as letras.

Por isso o erro de cota é traduzido aqui: quem ler a tela precisa saber que o
conserto é MOVER A PASTA para um Drive Compartilhado, não pedir mais espaço.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("analisesps.drive")

ESCOPOS = ["https://www.googleapis.com/auth/drive"]
MIME_XLSX = ("application/vnd.openxmlformats-officedocument"
             ".spreadsheetml.sheet")

# O que o Google devolve quando a service account tenta gravar onde não tem
# cota. A frase muda com o tempo; o código de motivo, não.
_MARCAS_DE_COTA = ("storageQuotaExceeded", "quotaExceeded",
                   "Service Accounts do not have storage quota")

DICA_DA_COTA = (
    "A pasta do Drive precisa ser de um DRIVE COMPARTILHADO (Shared Drive), "
    "com a conta de serviço como membro que pode gravar. Numa pasta comum do "
    "\"Meu Drive\" o Google recusa, porque a conta de serviço não tem espaço "
    "próprio — e compartilhar a pasta com ela não resolve. Mover a pasta para "
    "um Drive Compartilhado resolve.")


class ErroDoDrive(RuntimeError):
    """Falha na subida. A mensagem já vem pronta para aparecer na tela."""


def _sessao():
    """Sessão HTTP autenticada como a service account."""
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.service_account import Credentials

    from . import credenciais

    info = credenciais.credencial_bruta()
    if info is None:
        raise ErroDoDrive(
            "A credencial do Google não está no ambiente "
            "(GOOGLE_CREDENTIALS_BASE64). É a mesma que os outros módulos "
            "usam — confira se não foi apagada no Render.")
    return AuthorizedSession(Credentials.from_service_account_info(
        info, scopes=ESCOPOS))


def _explicar(resposta) -> str:
    """A mensagem do Google, traduzida para quem vai consertar."""
    texto = (resposta.text or "")[:400]
    if any(m in texto for m in _MARCAS_DE_COTA):
        return f"{DICA_DA_COTA} (o Google respondeu: {texto[:160]})"
    if resposta.status_code == 404:
        return ("A pasta do Drive não foi encontrada, ou a conta de serviço "
                "não enxerga ela. Confira o identificador da pasta e se ela "
                f"foi compartilhada com a conta de serviço. ({texto[:160]})")
    return f"HTTP {resposta.status_code}: {texto[:240]}"


def subir_xlsx(conteudo: bytes, nome: str, pasta_id: str) -> dict:
    """Cria o arquivo na pasta, libera por link e devolve {'id', 'link'}.

    São três chamadas, e as três podem falhar por motivos diferentes — por
    isso cada uma tem a sua mensagem. Um arquivo que sobe mas não fica público
    é pior do que um que não sobe: o link vai para o card do Pipefy e quem
    clica recebe "sem permissão", sem saber por quê."""
    if not str(pasta_id or "").strip():
        raise ErroDoDrive(
            "A pasta do Drive não está configurada. Defina DRIVE_FOLDER_ID "
            "(no Render ou na aba Credenciais) com o identificador da pasta.")

    sessao = _sessao()
    todos_os_drives = {"supportsAllDrives": "true"}

    resposta = sessao.post(
        "https://www.googleapis.com/drive/v3/files", params=todos_os_drives,
        json={"name": nome, "parents": [str(pasta_id).strip()],
              "mimeType": MIME_XLSX}, timeout=60)
    if resposta.status_code >= 300:
        raise ErroDoDrive("Não consegui criar o arquivo no Drive. "
                          + _explicar(resposta))
    arquivo_id = (resposta.json() or {}).get("id")
    if not arquivo_id:
        raise ErroDoDrive("O Drive aceitou a criação mas não devolveu o "
                          f"identificador do arquivo: {resposta.text[:200]}")

    resposta = sessao.patch(
        f"https://www.googleapis.com/upload/drive/v3/files/{arquivo_id}",
        params={"uploadType": "media", **todos_os_drives},
        headers={"Content-Type": MIME_XLSX}, data=conteudo, timeout=180)
    if resposta.status_code >= 300:
        raise ErroDoDrive("O arquivo foi criado, mas o conteúdo não subiu. "
                          + _explicar(resposta))

    resposta = sessao.post(
        f"https://www.googleapis.com/drive/v3/files/{arquivo_id}/permissions",
        params=todos_os_drives, json={"role": "reader", "type": "anyone"},
        timeout=60)
    if resposta.status_code >= 300:
        raise ErroDoDrive(
            "O arquivo subiu, mas não consegui liberar o acesso por link — "
            "quem clicar vai receber \"sem permissão\". " + _explicar(resposta))

    logger.info("Análise de SPs: '%s' subiu no Drive (%s).", nome, arquivo_id)
    return {"id": arquivo_id,
            "link": f"https://drive.google.com/uc?export=download&id={arquivo_id}"}


def conferir_pasta(pasta_id: str) -> dict:
    """Olha a pasta SEM escrever nada: existe? é de Drive Compartilhado?

    Serve à tela de Configurações, para o dono conferir o identificador que
    acabou de colar sem precisar gerar um BeeVale de verdade para descobrir
    que estava errado."""
    pasta_id = str(pasta_id or "").strip()
    if not pasta_id:
        return {"ok": False, "erro": "Nenhuma pasta configurada."}
    try:
        sessao = _sessao()
    except ErroDoDrive as e:
        return {"ok": False, "erro": str(e)}
    try:
        resposta = sessao.get(
            f"https://www.googleapis.com/drive/v3/files/{pasta_id}",
            params={"supportsAllDrives": "true",
                    "fields": "id,name,mimeType,driveId"}, timeout=30)
    except Exception as e:  # noqa: BLE001 — rede caiu; a tela tem de dizer
        return {"ok": False, "erro": f"Não consegui falar com o Drive: {e}"}
    if resposta.status_code >= 300:
        return {"ok": False, "erro": _explicar(resposta)}

    dados = resposta.json() or {}
    compartilhado = bool(dados.get("driveId"))
    return {
        "ok": True,
        "nome": dados.get("name", ""),
        "compartilhado": compartilhado,
        # O aviso vale mesmo com a leitura funcionando: enxergar a pasta e
        # poder gravar nela são coisas diferentes, e é na gravação que a cota
        # morde. Melhor avisar agora do que no meio de uma geração.
        "aviso": None if compartilhado else DICA_DA_COTA,
    }
