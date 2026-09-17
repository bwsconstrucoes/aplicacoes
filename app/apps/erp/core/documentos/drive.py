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
import re
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


# ---------------------------------------------------------------------------
# A ÁRVORE DE PASTAS (migração 070)
#
# PEDIDO DO DONO, 17/09/2026: *"a minha ideia é que tivesse tudo no Google
# Drive, separado numa pasta de obra (…) tem a pasta Obras, aí tem as
# subpastas. E aqueles outros documentos (…) salvar em outra pasta, tipo
# Arquivo (…) é importante, senão fica bagunçado."*
#
#   <pasta configurada>
#     ├── Obras
#     │     ├── ESCPE18 - Escola Planalto
#     │     └── CRECHEEUS26 - Creche do Eusébio
#     └── Arquivo
#           ├── Empresas · Pessoas · Fornecedores · Financeiro
#
# DUAS REGRAS QUE VALEM A PENA SABER:
#
#   1. **Pasta com o nome certo que já exista é REUSADA, não duplicada.** Se o
#      dono criar "Obras" à mão, o sistema entra nela. Duas pastas com o mesmo
#      nome é o começo de documento sumido.
#   2. **Nada aqui apaga coisa alguma.** Reorganizar move (troca o pai); o
#      arquivo continua o mesmo, com o mesmo id e o mesmo histórico. Ele pediu
#      isso com todas as letras: *"só tem que ter cuidado para não excluir"*.
# ---------------------------------------------------------------------------
PASTA_OBRAS = "Obras"
PASTA_ARQUIVO = "Arquivo"

# entidade do anexo → onde ela mora dentro de "Arquivo"
GAVETAS = {
    "empresa": "Empresas",
    "colaborador": "Pessoas",
    "fornecedor": "Fornecedores",
    "titulo": "Financeiro",
    "pagamento": "Financeiro",
    "movimentacao": "Financeiro",
    "lote": "Financeiro",
}


def _limpo(nome: str) -> str:
    """Nome de pasta sem o que atrapalha em pasta: barra, quebra e excesso."""
    limpo = re.sub(r"[\\/\r\n\t]+", " ", (nome or "").strip())
    limpo = re.sub(r"\s{2,}", " ", limpo)
    return limpo[:120] or "Sem nome"


def _pasta_guardada(s, chave: str) -> str:
    from app.apps.erp.db.models.cadastros import DrivePasta
    linha = s.get(DrivePasta, chave)
    return linha.file_id if linha is not None else ""


def _guardar_pasta(s, chave: str, file_id: str, nome: str) -> None:
    from app.apps.erp.db.models.cadastros import DrivePasta
    linha = s.get(DrivePasta, chave)
    if linha is None:
        s.add(DrivePasta(chave=chave, file_id=file_id, nome=nome))
    else:
        linha.file_id, linha.nome = file_id, nome
    s.flush()


def _procurar_pasta(svc, nome: str, pai: str) -> str:
    """O id da pasta com este nome dentro do pai, se já existir."""
    seguro = nome.replace("\\", "\\\\").replace("'", "\\'")
    consulta = ("mimeType='application/vnd.google-apps.folder' and trashed=false "
                f"and name='{seguro}' and '{pai}' in parents")
    try:
        r = svc.files().list(q=consulta, fields="files(id,name)", pageSize=5,
                             supportsAllDrives=True,
                             includeItemsFromAllDrives=True).execute()
    except Exception as e:
        raise ErroDrive(f"Falha ao procurar a pasta “{nome}” no Drive: {e}")
    achados = r.get("files") or []
    return achados[0]["id"] if achados else ""


def garantir_pasta(s, *, chave: str, nome: str, pai: str,
                   impersonar: str = "") -> str:
    """O id da pasta `nome` dentro de `pai` — achando, criando ou lembrando."""
    lembrada = _pasta_guardada(s, chave)
    if lembrada:
        return lembrada
    svc = _servico(impersonar)
    achada = _procurar_pasta(svc, nome, pai)
    if not achada:
        try:
            achada = svc.files().create(
                body={"name": nome, "parents": [pai],
                      "mimeType": "application/vnd.google-apps.folder"},
                fields="id", supportsAllDrives=True).execute()["id"]
        except Exception as e:
            raise ErroDrive(f"Falha ao criar a pasta “{nome}” no Drive: {e}")
        logger.info("ERP/drive: pasta “%s” criada", nome)
    _guardar_pasta(s, chave, achada, nome)
    return achada


def pasta_de(s, entidade_tipo: str, entidade_id: int) -> str:
    """A pasta onde ESTE anexo deve ficar. Cria o caminho, se faltar.

    Se qualquer passo falhar, devolve a pasta raiz configurada: guardar no
    lugar menos bonito é melhor do que não guardar.
    """
    cfg = configuracao(s)
    raiz = cfg["pasta"]
    if not raiz:
        return ""
    try:
        if entidade_tipo == "obra" and entidade_id:
            from app.apps.erp.db.models.cadastros import Obra
            obra = s.get(Obra, entidade_id)
            if obra is None:
                return raiz
            pai = garantir_pasta(s, chave=f"raiz:{PASTA_OBRAS}", nome=PASTA_OBRAS,
                                 pai=raiz, impersonar=cfg["impersonar"])
            nome = _limpo(f"{obra.codigo} - {obra.nome}" if obra.nome else obra.codigo)
            return garantir_pasta(s, chave=f"obra:{obra.id}", nome=nome, pai=pai,
                                  impersonar=cfg["impersonar"])

        gaveta = GAVETAS.get((entidade_tipo or "").lower(), "Diversos")
        pai = garantir_pasta(s, chave=f"raiz:{PASTA_ARQUIVO}", nome=PASTA_ARQUIVO,
                             pai=raiz, impersonar=cfg["impersonar"])
        return garantir_pasta(s, chave=f"arquivo:{gaveta}", nome=gaveta, pai=pai,
                              impersonar=cfg["impersonar"])
    except ErroDrive as e:
        logger.warning("ERP/drive: não deu para montar a pasta de %s %s (%s) — "
                       "usando a pasta raiz", entidade_tipo, entidade_id, e)
        return raiz


def mudar_de_pasta(file_id: str, *, nova: str, impersonar: str = "") -> bool:
    """Move o arquivo para outra pasta. NÃO copia e NÃO apaga nada.

    Devolve True quando mudou de lugar, False quando já estava certo.
    """
    svc = _servico(impersonar)
    try:
        atual = svc.files().get(fileId=file_id, fields="parents",
                                supportsAllDrives=True).execute()
        pais = atual.get("parents") or []
        if pais == [nova]:
            return False
        svc.files().update(fileId=file_id, addParents=nova,
                           removeParents=",".join(pais), fields="id",
                           supportsAllDrives=True).execute()
    except Exception as e:
        raise ErroDrive(f"Falha ao mover o arquivo no Drive: {e}")
    return True


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
