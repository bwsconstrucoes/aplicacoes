# -*- coding: utf-8 -*-
"""
Acesso ao Google e leitura dos segredos da aba "Credenciais".

MESMO PADRÃO DO `emissaonf` — de propósito, e não por preguiça. A credencial da
service account vem de `GOOGLE_CREDENTIALS_BASE64`, que já existe no Render e
já serve os outros módulos. Nenhuma variável nova, nenhum arquivo
`credenciais.json` no servidor, nenhum "Secret File".

O Streamlit original procurava o arquivo no disco. Aqui não há disco: o
contêiner do Render é apagado a cada reinício, e um arquivo de credencial ali
seria ou perdido ou versionado por engano — os dois ruins.

Ordem de busca de cada segredo: variável de ambiente primeiro, aba
"Credenciais" depois. Assim dá para trocar um token no Render sem mexer na
planilha, e a planilha continua sendo o lugar onde o dono enxerga tudo junto.
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
from base64 import b64decode

logger = logging.getLogger("analisesps.credenciais")

# A planilha só de tokens. É a mesma do `emissaonf` — um lugar só para os
# segredos da empresa, compartilhado apenas com a service account e o dono.
SHEET_CREDENCIAIS = os.getenv(
    "ANALISESPS_SHEET_CREDENCIAIS",
    "1D4aVC7wVHL_t-5QpI6v7vtLJMjJpA7DpDnByTFB9i-U")
ABA_CREDENCIAIS = "Credenciais"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_cache_cliente = None
_cache_segredos: dict | None = None


def cliente():
    """Cliente gspread autenticado. Criado uma vez por processo.

    Levanta erro claro quando a credencial não está no ambiente — melhor falhar
    dizendo o nome da variável que falta do que estourar lá dentro do gspread
    com uma mensagem que ninguém entende."""
    global _cache_cliente
    if _cache_cliente is not None:
        return _cache_cliente

    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "").strip()
    if not b64:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_BASE64 não está definida. É a credencial da "
            "service account do Google, em base64, e já existe no Render para "
            "os outros módulos — confira se não foi apagada.")

    import gspread
    from google.oauth2.service_account import Credentials

    try:
        info = json.loads(b64decode(b64).decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_BASE64 existe mas não é um JSON válido em "
            f"base64 ({e}). Refaça a codificação do arquivo da service account."
        ) from e

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _cache_cliente = gspread.authorize(creds)
    logger.info("Análise de SPs: Google autenticado como %s",
                info.get("client_email", "?"))
    return _cache_cliente


def credencial_bruta() -> dict | None:
    """O JSON da service account, já decodificado — ou None se não houver.

    Existe porque o Drive não passa pelo gspread: ele fala REST direto, e
    precisa montar a própria sessão autenticada. Decodificar em dois lugares
    daria duas mensagens de erro diferentes para a mesma variável faltando."""
    b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64", "").strip()
    if not b64:
        return None
    try:
        return json.loads(b64decode(b64).decode("utf-8"))
    except Exception:  # noqa: BLE001 — quem chama decide o que dizer
        logger.exception("Análise de SPs: GOOGLE_CREDENTIALS_BASE64 ilegível")
        return None


def com_retry(fn, tentativas: int = 5, espera: float = 1.5):
    """Repete a chamada quando o Google oscila.

    O Google devolve 429 (cota) e 500/503 (instabilidade momentânea) com alguma
    frequência quando se lê uma planilha de 59 mil linhas. Sem isto, uma
    oscilação de dois segundos aborta uma carga inteira.

    A espera cresce a cada tentativa e leva um tempero aleatório: se dois
    processos baterem na cota juntos, eles não voltam no mesmo instante."""
    ultimo = None
    for tentativa in range(tentativas):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — a API tem erro de vários tipos
            texto = str(e)
            passageiro = any(c in texto for c in
                             ("429", "500", "502", "503", "504",
                              "Connection", "timed out", "timeout"))
            if not passageiro or tentativa == tentativas - 1:
                raise
            ultimo = e
            pausa = espera * (2 ** tentativa) + random.uniform(0, 0.5)
            logger.warning("Google oscilou (%s). Tentando de novo em %.1fs "
                           "(%d de %d).", texto[:120], pausa,
                           tentativa + 2, tentativas)
            time.sleep(pausa)
    raise ultimo  # pragma: no cover — inalcançável, o laço acima sempre decide


def segredos(recarregar: bool = False) -> dict:
    """Lê a aba "Credenciais" (coluna A = chave, coluna B = valor).

    Guardado em memória depois da primeira leitura: são poucos valores, e ir à
    planilha a cada uso gastaria cota à toa."""
    global _cache_segredos
    if _cache_segredos is not None and not recarregar:
        return _cache_segredos

    aba = com_retry(lambda: cliente()
                    .open_by_key(SHEET_CREDENCIAIS)
                    .worksheet(ABA_CREDENCIAIS))
    valores = com_retry(aba.get_all_values)
    _cache_segredos = {
        str(linha[0]).strip(): str(linha[1]).strip()
        for linha in valores
        if len(linha) >= 2 and str(linha[0]).strip()
    }
    logger.info("Análise de SPs: %d segredos lidos da planilha.",
                len(_cache_segredos))
    return _cache_segredos


# Apelidos aceitos por segredo, na ordem de preferência. Existem porque o mesmo
# token aparece com nomes diferentes na planilha e no Render, e trocar um dos
# dois quebraria outro módulo que já usa o nome antigo.
APELIDOS = {
    "PIPEFY_TOKEN": ["PIPEFY_TOKEN", "PIPEFY_API_TOKEN"],
    "SMTP_HOST": ["SMTP_HOST", "EMAIL_HOST"],
    "SMTP_PORT": ["SMTP_PORT", "EMAIL_PORT"],
    "SMTP_USER": ["SMTP_USER", "EMAIL_USER", "EMAIL_REMETENTE"],
    "SMTP_SENHA": ["SMTP_SENHA", "EMAIL_SENHA", "EMAIL_PASSWORD"],
    "DRIVE_FOLDER_ID": ["DRIVE_FOLDER_ID", "BEEVALE_DRIVE_FOLDER_ID"],
}


def token(nome: str, padrao: str = "") -> str:
    """Um segredo pelo nome. Ambiente ganha da planilha.

    Se a planilha estiver fora de alcance (sem internet, cota estourada), o que
    estiver no ambiente ainda responde — o módulo degrada em vez de parar."""
    for apelido in APELIDOS.get(nome, [nome]):
        valor = os.getenv(apelido) or os.getenv(apelido.upper())
        if valor:
            return valor.strip()
    try:
        d = segredos()
    except Exception:  # noqa: BLE001 — planilha inacessível não pode derrubar a tela
        logger.exception("Análise de SPs: não consegui ler a aba Credenciais")
        return padrao
    for apelido in APELIDOS.get(nome, [nome]):
        if d.get(apelido):
            return str(d[apelido]).strip()
    return padrao
