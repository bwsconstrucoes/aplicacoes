# ============================================================================
# ERP — core/documentos/drive.py
# A pasta do Google Drive onde os anexos passam a morar.
#
# TRÊS COISAS QUE NÃO PODEM SER ESQUECIDAS AQUI:
#
# 1. NADA É PÚBLICO. Ao contrário do `emissaonf`, que marca os PDFs da nota
#    como "qualquer pessoa com o link pode ver", aqui o arquivo nasce fechado e
#    continua fechado. O link do Drive nunca sai do servidor: quem entrega o
#    documento é o endereço do ERP, depois de conferir permissão e escopo.
#    Anexo do ERP é holerite, comprovante bancário e contrato.
#
# 2. A CONTA DE SERVIÇO NÃO TEM ESPAÇO PRÓPRIO no Google. Ela só consegue
#    guardar arquivo emprestando espaço de alguém — de um Drive compartilhado
#    (o caminho escolhido, porque ali o dono é a empresa) ou personificando um
#    usuário do domínio. Por isso `supportsAllDrives=True` em toda chamada: sem
#    isso o Drive compartilhado simplesmente não é enxergado.
#
# 3. A CREDENCIAL É A QUE JÁ EXISTE (`GOOGLE_CREDENTIALS_BASE64`). Não se cria
#    variável nova para credencial neste repositório — é regra do `CLAUDE.md`.
#    O que é configuração (qual pasta) mora na tela de Configurações, porque
#    quem decide isso é o dono, não quem faz deploy.
# ============================================================================
from __future__ import annotations

import json
import logging
import os
from base64 import b64decode
from typing import Any, Optional

logger = logging.getLogger(__name__)

CHAVE_PASTA = "anexos.drive.pasta"          # id da pasta, na tabela parametros
CHAVE_LIGADO = "anexos.drive.ligado"        # "1" liga o Drive para anexo NOVO
CHAVE_IMPERSONAR = "anexos.drive.impersonar"  # vazio = Drive compartilhado

ESCOPO = ["https://www.googleapis.com/auth/drive"]


class ErroDrive(Exception):
    """Falha ao falar com o Drive. Quem chama decide se cai para o banco."""


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
def _parametro(s, chave: str, padrao: str = "") -> str:
    from app.apps.erp.db.models.cadastros import Parametro
    linha = s.get(Parametro, chave)
    return (linha.valor if linha is not None else padrao) or padrao


def configuracao(s) -> dict[str, Any]:
    """O que está configurado hoje, do jeito que a tela precisa mostrar."""
    pasta = _parametro(s, CHAVE_PASTA).strip()
    ligado = _parametro(s, CHAVE_LIGADO).strip() == "1"
    return {
        "pasta": pasta,
        "impersonar": _parametro(s, CHAVE_IMPERSONAR).strip(),
        "ligado": ligado,
        "tem_credencial": bool(os.getenv("GOOGLE_CREDENTIALS_BASE64", "").strip()),
        # "usável" é diferente de "ligado": sem pasta e sem credencial, ligar
        # não adianta — e a tela precisa dizer isso em vez de deixar o dono
        # apertar um botão que não faz nada.
        "usavel": bool(pasta) and bool(os.getenv("GOOGLE_CREDENTIALS_BASE64", "").strip()),
    }


def ativo(s) -> bool:
    """O anexo NOVO deve ir para o Drive?"""
    c = configuracao(s)
    return bool(c["ligado"] and c["usavel"])


def id_da_pasta(texto: str) -> str:
    """Aceita o id ou o endereço inteiro colado da barra do navegador.

    O dono vai colar o que estiver na barra — pedir que ele extraia o pedaço
    certo de uma URL é pedir erro de digitação em algo que a máquina sabe
    fazer.
    """
    texto = (texto or "").strip()
    if not texto:
        return ""
    for marca in ("/folders/", "/drive/u/0/folders/", "id="):
        if marca in texto:
            texto = texto.split(marca, 1)[1]
    return texto.split("?", 1)[0].split("/", 1)[0].strip()


# ---------------------------------------------------------------------------
# Conversa com o Google
# ---------------------------------------------------------------------------
def _servico(impersonar: str = ""):
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError as e:                              # pragma: no cover
        raise ErroDrive(f"Bibliotecas do Google ausentes: {e}")

    bruto = os.getenv("GOOGLE_CREDENTIALS_BASE64", "").strip()
    if not bruto:
        raise ErroDrive("Falta a credencial do Google (GOOGLE_CREDENTIALS_BASE64).")
    try:
        info = json.loads(b64decode(bruto).decode("utf-8"))
        cred = Credentials.from_service_account_info(info, scopes=ESCOPO)
    except Exception as e:
        raise ErroDrive(f"Credencial do Google ilegível: {e}")
    if impersonar:
        cred = cred.with_subject(impersonar)
    return build("drive", "v3", credentials=cred, cache_discovery=False)


def enviar(conteudo: bytes, nome: str, mime: str, *, pasta: str,
           impersonar: str = "") -> str:
    """Sobe o arquivo FECHADO e devolve o id dele. Nunca torna público."""
    from googleapiclient.http import MediaInMemoryUpload
    if not pasta:
        raise ErroDrive("Pasta do Drive não configurada.")
    svc = _servico(impersonar)
    midia = MediaInMemoryUpload(conteudo, mimetype=mime or "application/octet-stream",
                                resumable=False)
    try:
        arquivo = svc.files().create(
            body={"name": nome, "parents": [pasta]}, media_body=midia,
            fields="id", supportsAllDrives=True).execute()
    except Exception as e:
        raise ErroDrive(f"Falha ao enviar para o Drive: {e}")
    return arquivo["id"]


def baixar(file_id: str, *, impersonar: str = "") -> bytes:
    """Traz os bytes de volta. É isto que a rota de download do ERP usa."""
    import io
    from googleapiclient.http import MediaIoBaseDownload
    svc = _servico(impersonar)
    try:
        pedido = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        baixador = MediaIoBaseDownload(buffer, pedido)
        pronto = False
        while not pronto:
            _, pronto = baixador.next_chunk()
        return buffer.getvalue()
    except Exception as e:
        raise ErroDrive(f"Falha ao ler do Drive: {e}")


def apagar(file_id: str, *, impersonar: str = "") -> None:
    svc = _servico(impersonar)
    try:
        svc.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except Exception as e:
        logger.warning("ERP/drive: não consegui apagar %s — %s", file_id, e)


def testar(s) -> dict[str, Any]:
    """Prova de vida, para o botão da tela: escreve, lê de volta e apaga.

    Vale mais que "configuração salva": o erro que interessa (pasta que a conta
    de serviço não enxerga, credencial sem escopo, cota) só aparece ao tentar
    de verdade.
    """
    c = configuracao(s)
    if not c["pasta"]:
        return {"ok": False, "erro": "Cole o endereço da pasta do Drive primeiro."}
    if not c["tem_credencial"]:
        return {"ok": False, "erro": "Falta a credencial do Google no servidor."}
    marca = b"teste do ERP - pode apagar"
    try:
        fid = enviar(marca, "teste-do-erp.txt", "text/plain",
                     pasta=c["pasta"], impersonar=c["impersonar"])
    except ErroDrive as e:
        return {"ok": False, "erro": str(e)}
    try:
        volta = baixar(fid, impersonar=c["impersonar"])
    except ErroDrive as e:
        return {"ok": False, "erro": f"Gravou mas não consegui ler de volta: {e}"}
    finally:
        apagar(fid, impersonar=c["impersonar"])
    if volta != marca:
        return {"ok": False, "erro": "O que voltou do Drive não é o que foi gravado."}
    return {"ok": True, "mensagem": "Escrevi, li de volta e apaguei. A pasta está boa."}
