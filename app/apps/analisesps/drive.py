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

⚠️ A ARMADILHA DA COTA — e a correção de 22/09/2026, que é o que importa aqui.

A service account **não tem espaço de armazenamento próprio**. Entrando como
ela mesma, o Google recusa a criação de arquivo em pasta do "Meu Drive" com um
erro de cota, mesmo que a pasta esteja compartilhada com ela como Editor.

Havia DUAS saídas para isso, e este módulo conhecia só uma. A que ele mandava
na tela era "mova a pasta para um Drive Compartilhado". A outra — a que o
`emissaonf` usa desde sempre, sem ninguém ter reparado — é **personificar uma
pessoa de verdade** (delegação em todo o domínio, no Admin do Google): a conta
de serviço age COMO `contato@bwsconstrucoes.com.br`, que tem cota, e o arquivo
nasce dono dele numa pasta comum.

O dono cobrou justamente a diferença: *"eu tenho várias automações que gravam
em pastas compartilhadas, por que essa não pode?"* — podiam porque
personificavam. Agora esta também personifica, e a pasta pode ficar onde está.

Os dois caminhos continuam valendo: com `ANALISESPS_DRIVE_IMPERSONAR=""`
(vazio) volta a entrar como a própria conta de serviço, que é o certo quando a
pasta for mesmo de um Drive Compartilhado — ali a delegação é dispensável.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger("analisesps.drive")

ESCOPOS = ["https://www.googleapis.com/auth/drive"]
MIME_XLSX = ("application/vnd.openxmlformats-officedocument"
             ".spreadsheetml.sheet")

# Quem a conta de serviço personifica. Mesmo e-mail que o `emissaonf` usa há
# tempos (`EMISSAO_NF_DRIVE_IMPERSONAR`), mas com variável PRÓPRIA: são áreas
# diferentes, e desligar a personificação de uma não pode desligar a da outra.
EMAIL_IMPERSONAR = "contato@bwsconstrucoes.com.br"

# O que o Google devolve quando a service account tenta gravar onde não tem
# cota. A frase muda com o tempo; o código de motivo, não.
_MARCAS_DE_COTA = ("storageQuotaExceeded", "quotaExceeded",
                   "Service Accounts do not have storage quota")

DICA_DA_COTA = (
    "O Google recusou por falta de espaço da conta de serviço — ela não tem "
    "armazenamento próprio, e compartilhar a pasta com ela não resolve. Há "
    "dois consertos: manter a personificação ligada (a conta de serviço grava "
    "em nome de uma pessoa da empresa, que tem espaço — é assim que a emissão "
    "de NFS-e grava em pasta comum), ou mover a pasta para um DRIVE "
    "COMPARTILHADO (Shared Drive), onde a cota não é cobrada de ninguém.")


def quem_personificar() -> str:
    """O e-mail que a conta de serviço vai vestir, ou "" para entrar como ela.

    Vazio é decisão legítima, não falta de configuração: numa pasta de Drive
    Compartilhado a personificação é dispensável.
    """
    return os.getenv("ANALISESPS_DRIVE_IMPERSONAR", EMAIL_IMPERSONAR).strip()


def _dica_da_delegacao(quem: str) -> str:
    """Quando o Google recusa a personificação. Diz onde se conserta."""
    return (
        f"O Google não deixou a conta de serviço gravar em nome de {quem}. "
        "Isso se libera no Admin do Google, em Segurança › Controle de dados "
        "e acesso › Controles de API › Delegação em todo o domínio: a conta "
        "de serviço precisa estar lá com o escopo "
        "https://www.googleapis.com/auth/drive. A emissão de NFS-e usa esse "
        "mesmo caminho, então se ela está funcionando a liberação existe e o "
        "problema é outro — pode ser o e-mail personificado, que hoje é "
        f"{quem} (muda em ANALISESPS_DRIVE_IMPERSONAR).")


class ErroDoDrive(RuntimeError):
    """Falha na subida. A mensagem já vem pronta para aparecer na tela."""


def _sessao():
    """Sessão HTTP autenticada — personificando, se houver quem.

    O token é pedido AQUI, de propósito. A recusa da delegação não chega como
    resposta HTTP da chamada que a gente fez: ela estoura no meio do pedido do
    token, e viraria um erro cru no meio da tela. Pedindo agora, ela vira uma
    frase que diz onde se conserta. O custo é zero — a sessão pediria o token
    na primeira chamada de qualquer jeito.
    """
    from google.auth.transport.requests import AuthorizedSession, Request
    from google.oauth2.service_account import Credentials

    from . import credenciais

    info = credenciais.credencial_bruta()
    if info is None:
        raise ErroDoDrive(
            "A credencial do Google não está no ambiente "
            "(GOOGLE_CREDENTIALS_BASE64). É a mesma que os outros módulos "
            "usam — confira se não foi apagada no Render.")
    cred = Credentials.from_service_account_info(info, scopes=ESCOPOS)

    quem = quem_personificar()
    if quem:
        cred = cred.with_subject(quem)
        try:
            cred.refresh(Request())
        except Exception as e:  # noqa: BLE001 — qualquer recusa vira a dica
            raise ErroDoDrive(
                f"{_dica_da_delegacao(quem)} (o Google respondeu: "
                f"{str(e)[:200]})") from e
    return AuthorizedSession(cred)


def _explicar(resposta) -> str:
    """A mensagem do Google, traduzida para quem vai consertar."""
    texto = (resposta.text or "")[:400]
    if any(m in texto for m in _MARCAS_DE_COTA):
        return f"{DICA_DA_COTA} (o Google respondeu: {texto[:160]})"
    if resposta.status_code == 404:
        # ⚠️ COM PERSONIFICAÇÃO, QUEM PRECISA ENXERGAR A PASTA É A PESSOA, não
        # a conta de serviço. Mandar compartilhar com a conta de serviço aqui
        # faria o dono compartilhar com quem não está pedindo.
        quem = quem_personificar()
        com_quem = (f"com {quem}, que é quem a conta de serviço está "
                    "personificando") if quem else "com a conta de serviço"
        return ("A pasta do Drive não foi encontrada, ou não está visível "
                "para quem está gravando. Confira o identificador da pasta e "
                f"se ela foi compartilhada {com_quem}. ({texto[:160]})")
    return f"HTTP {resposta.status_code}: {texto[:240]}"


def subir_xlsx(conteudo: bytes, nome: str, pasta_id: str) -> dict:
    """Sobe uma planilha. Ver `subir_arquivo` — é o mesmo caminho."""
    return subir_arquivo(conteudo, nome, pasta_id, MIME_XLSX)


def subir_arquivo(conteudo: bytes, nome: str, pasta_id: str,
                  mime: str = MIME_XLSX) -> dict:
    """Cria o arquivo na pasta, libera por link e devolve {'id', 'link'}.

    São três chamadas, e as três podem falhar por motivos diferentes — por
    isso cada uma tem a sua mensagem. Um arquivo que sobe mas não fica público
    é pior do que um que não sobe: o link vai para o card do Pipefy e quem
    clica recebe "sem permissão", sem saber por quê.

    ⚠️ O TIPO DO ARQUIVO É PARÂMETRO desde 17/09/2026: além das planilhas,
    agora sobe o **XML da nota fiscal** baixado da Receita. Era `subir_xlsx`
    com o tipo fixo, e um XML subindo como planilha abriria quebrado no Drive.
    """
    if not str(pasta_id or "").strip():
        raise ErroDoDrive(
            "A pasta do Drive não está configurada. Defina DRIVE_FOLDER_ID "
            "(no Render ou na aba Credenciais) com o identificador da pasta.")

    sessao = _sessao()
    todos_os_drives = {"supportsAllDrives": "true"}

    resposta = sessao.post(
        "https://www.googleapis.com/drive/v3/files", params=todos_os_drives,
        json={"name": nome, "parents": [str(pasta_id).strip()],
              "mimeType": mime}, timeout=60)
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
        headers={"Content-Type": mime}, data=conteudo, timeout=180)
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


def baixar_arquivo(arquivo_id: str) -> bytes:
    """O conteúdo de um arquivo que este módulo subiu. Levanta `ErroDoDrive`.

    Pela conta de serviço, e não pelo link público: o link existe para quem
    abre no navegador; aqui quem lê é o servidor, que já tem credencial. Assim
    a leitura continua funcionando no dia em que a pasta deixar de ser
    aberta por link — e um arquivo com acesso restrito não vira tela quebrada.
    """
    arquivo_id = str(arquivo_id or "").strip()
    if not arquivo_id:
        raise ErroDoDrive("Nenhum arquivo informado.")
    sessao = _sessao()
    try:
        resposta = sessao.get(
            f"https://www.googleapis.com/drive/v3/files/{arquivo_id}",
            params={"alt": "media", "supportsAllDrives": "true"}, timeout=60)
    except Exception as e:  # noqa: BLE001 — rede caiu; a tela tem de dizer
        raise ErroDoDrive(f"Não consegui falar com o Drive: {e}") from e
    if resposta.status_code >= 300:
        raise ErroDoDrive("Não consegui baixar o arquivo do Drive. "
                          + _explicar(resposta))
    return resposta.content


def conferir_pasta(pasta_id: str) -> dict:
    """Olha a pasta SEM escrever nada: existe? dá para gravar nela?

    Serve à tela de Configurações, para o dono conferir o identificador que
    acabou de colar sem precisar gerar um BeeVale de verdade para descobrir
    que estava errado.

    ⚠️ ATÉ 22/09/2026 ISTO ACUSAVA PASTA COMUM COMO PROBLEMA, e era conselho
    errado: com a personificação ligada, pasta comum é exatamente onde deve
    ficar. O aviso agora só sai quando NÃO há personificação E a pasta não é
    de Drive Compartilhado — que é o único caso em que a gravação vai falhar.
    """
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
    quem = quem_personificar()
    return {
        "ok": True,
        "nome": dados.get("name", ""),
        "compartilhado": compartilhado,
        "personificando": quem,
        # O aviso vale mesmo com a leitura funcionando: enxergar a pasta e
        # poder gravar nela são coisas diferentes, e é na gravação que a cota
        # morde. Melhor avisar agora do que no meio de uma geração.
        "aviso": None if (compartilhado or quem) else DICA_DA_COTA,
    }
