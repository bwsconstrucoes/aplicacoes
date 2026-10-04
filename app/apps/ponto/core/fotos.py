# -*- coding: utf-8 -*-
"""
A foto da batida: do base64 do celular ao Google Drive.

ONDE ELA FICA: no Drive, pela MESMA rotina que o ERP usa para os anexos
(`erp/core/documentos/drive.py` — importada, não copiada, como o precedente de
24/09/2026). Decisão do dono, 03/10/2026: *"as fotos a gente pode armazenar no
Google Drive, lá o espaço é virtualmente infinito; na base de dados não"*. O
arquivo sobe FECHADO (nunca público por link), numa subpasta por mês
(`AAAA-MM`) dentro da pasta `PONTO_DRIVE_PASTA`.

O QUE FICA NO BANCO é só a ficha: hash SHA-256, tamanho, medidas e o id do
arquivo no Drive. A ficha é o que a marcação aponta e o que prova, amanhã, que
a foto existiu e não foi trocada.

A FILA DE REENVIO, e por que ela existe: a batida não pode falhar porque o Drive
demorou. Se a subida falhar na hora (rede, cota, pasta não configurada), os
bytes ficam TEMPORARIAMENTE em `ponto.fotos.conteudo` e a rota
`POST /ponto/api/admin/fotos/enviar-pendentes` (ou o script) tenta de novo;
quando sobe, os bytes são apagados do banco. Não é armazenamento — é a sala de
espera. O `health` mostra quantas estão esperando.

Por que redimensionar: um celular manda 3 a 8 MB; a 800 px e JPEG 80 a foto
fica em ~60 a 120 KB e o rosto continua reconhecível — que é para o que serve.

A BATIDA NÃO ESPERA O DRIVE (03/10/2026, junto com o tablet de câmera sempre
ligada): a foto entra na sala de espera e uma linha separada leva para o Drive
logo depois da resposta. Subir no Drive leva de 1 a 3 segundos — numa fila de
40 pessoas às 7h, seriam 2 minutos de espera somados por nada.

OS SINAIS DA FOTO (migração 003), medidos aqui, sem IA e sem custo: brilho
médio (câmera tampada fica perto de zero), contraste (parede lisa, dedo na
lente) e uma impressão da imagem (`dhash`, 256 bits) — duas fotos com
impressão quase igual são a MESMA foto mandada de novo. Os alertas usam os três.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import io
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao

logger = logging.getLogger("ponto.fotos")

VARIAVEL_PASTA = "PONTO_DRIVE_PASTA"
VARIAVEL_IMPERSONAR = "PONTO_DRIVE_IMPERSONAR"
# Mesmo e-mail que a emissão de NFS-e e a Análise de SPs personificam: a conta
# de serviço não tem cota própria no Drive, e grava em nome de uma pessoa.
IMPERSONAR_PADRAO = "contato@bwsconstrucoes.com.br"

MAX_RECEBIDO_BYTES = 8 * 1024 * 1024
MAX_GUARDADO_BYTES = 300 * 1024
LADO_MAIOR_PX = 800
QUALIDADE_JPEG = 80
TENTATIVAS_NA_HORA = 3

_PREFIXO_DATA_URL = re.compile(r"^data:image/[a-zA-Z0-9.+-]+;base64,", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
def pasta_configurada() -> str:
    return (os.getenv(VARIAVEL_PASTA) or "").strip()


def quem_personificar() -> str:
    return os.getenv(VARIAVEL_IMPERSONAR, IMPERSONAR_PADRAO).strip()


def drive_configurado() -> bool:
    return bool(pasta_configurada()) and bool((os.getenv("GOOGLE_CREDENTIALS_BASE64") or "").strip())


# ---------------------------------------------------------------------------
# Preparo (puro: sem banco, sem rede)
# ---------------------------------------------------------------------------
def decodificar_base64(texto: str) -> bytes:
    """Aceita com ou sem o prefixo `data:image/...;base64,`."""
    limpo = _PREFIXO_DATA_URL.sub("", (texto or "").strip())
    limpo = re.sub(r"\s+", "", limpo)
    if not limpo:
        raise ErroDeValidacao("foto vazia", campo="foto_base64")
    if len(limpo) * 3 // 4 > MAX_RECEBIDO_BYTES:
        raise ErroDeValidacao("foto maior que o limite de 8 MB", campo="foto_base64")
    try:
        return base64.b64decode(limpo, validate=True)
    except Exception as e:  # noqa: BLE001
        raise ErroDeValidacao("foto em base64 ilegível", campo="foto_base64") from e


def sinais(imagem) -> tuple[int, int, str]:
    """(brilho médio 0–255, contraste, dhash de 256 bits em hexadecimal)."""
    from PIL import ImageStat
    cinza = imagem.convert("L")
    est = ImageStat.Stat(cinza)
    luminancia, contraste = int(round(est.mean[0])), int(round(est.stddev[0]))
    pequena = cinza.resize((17, 16))
    px = pequena.tobytes()
    bits = 0
    for linha in range(16):
        for coluna in range(16):
            esquerda, direita = px[linha * 17 + coluna], px[linha * 17 + coluna + 1]
            bits = (bits << 1) | (1 if esquerda > direita else 0)
    return luminancia, contraste, f"{bits:064x}"


def distancia_dhash(a: Optional[str], b: Optional[str]) -> Optional[int]:
    """Quantos dos 256 bits diferem. 0–3 = a mesma foto; foto nova de verdade,
    mesmo da mesma pessoa no mesmo lugar, fica bem acima disso."""
    if not a or not b:
        return None
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def reduzir(conteudo: bytes, com_sinais: bool = False):
    """Abre, corrige a orientação, reduz e regrava como JPEG. Devolve
    (bytes, largura, altura) — e os sinais, com `com_sinais`. Levanta
    ErroDeValidacao se não for imagem."""
    try:
        from PIL import Image, ImageOps
        imagem = Image.open(io.BytesIO(conteudo))
        imagem.load()
    except Exception as e:  # noqa: BLE001
        raise ErroDeValidacao("a foto não é uma imagem legível", campo="foto_base64") from e
    imagem = ImageOps.exif_transpose(imagem)
    if imagem.mode not in ("RGB", "L"):
        imagem = imagem.convert("RGB")
    imagem.thumbnail((LADO_MAIOR_PX, LADO_MAIOR_PX))
    qualidade = QUALIDADE_JPEG
    while True:
        saida = io.BytesIO()
        imagem.save(saida, format="JPEG", quality=qualidade, optimize=True)
        dados = saida.getvalue()
        if len(dados) <= MAX_GUARDADO_BYTES or qualidade <= 40:
            break
        qualidade -= 10
    if com_sinais:
        return dados, imagem.width, imagem.height, sinais(imagem)
    return dados, imagem.width, imagem.height


def sha256(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


class FotoPronta:
    """A foto já reduzida e com hash, antes de ir para o Drive."""
    __slots__ = ("dados", "largura", "altura", "hash", "luminancia", "contraste", "dhash")

    def __init__(self, dados: bytes, largura: int, altura: int,
                 sinais_da_foto: tuple | None = None):
        self.dados, self.largura, self.altura = dados, largura, altura
        self.hash = sha256(dados)
        self.luminancia, self.contraste, self.dhash = sinais_da_foto or (None, None, None)


def preparar(foto_base64: str) -> FotoPronta:
    """Valida e reduz. É chamada ANTES de gravar a marcação, para uma foto
    ilegível virar 400 sem deixar marcação pela metade."""
    return FotoPronta(*reduzir(decodificar_base64(foto_base64), com_sinais=True))


def nome_do_arquivo(momento: dt.datetime, colaborador_id: int, nsr: int) -> str:
    """Sem CPF no nome: é dado pessoal, e o nome do arquivo fica visível para
    quem abre a pasta. O NSR já identifica a batida."""
    local = horario.para_local(momento)
    return f"{local:%Y-%m-%d_%H%M%S}_colab{colaborador_id}_nsr{nsr}.jpg"


# ---------------------------------------------------------------------------
# Drive
# ---------------------------------------------------------------------------
def _pasta_do_mes(conn: Connection, svc, momento: dt.datetime) -> str:
    """A subpasta `AAAA-MM` dentro da raiz — lembrada em `ponto.drive_pastas`
    para não perguntar ao Google a cada batida. Pasta com o nome certo que já
    exista é reusada, nunca duplicada (regra do ERP)."""
    from app.apps.erp.core.documentos import drive as drive_erp
    raiz = pasta_configurada()
    nome = f"{horario.para_local(momento):%Y-%m}"
    lembrada = db.um(conn, "SELECT file_id FROM ponto.drive_pastas WHERE nome = :n", n=nome)
    if lembrada:
        return lembrada["file_id"]
    achada = drive_erp._procurar_pasta(svc, nome, raiz)
    if not achada:
        achada = svc.files().create(
            body={"name": nome, "parents": [raiz],
                  "mimeType": "application/vnd.google-apps.folder"},
            fields="id", supportsAllDrives=True).execute()["id"]
        logger.info("Ponto: pasta %s criada no Drive", nome)
    db.executar(conn, "INSERT INTO ponto.drive_pastas (nome, file_id) VALUES (:n, :f) "
                      "ON CONFLICT (nome) DO UPDATE SET file_id = :f", n=nome, f=achada)
    return achada


def _subir_no_drive(conn: Connection, dados: bytes, nome: str, momento: dt.datetime,
                    mime: str = "image/jpeg") -> str:
    """Sobe e devolve o id do arquivo. Levanta RuntimeError se não der.
    Tenta até TENTATIVAS_NA_HORA vezes com pausa curta: falha passageira de
    rede não pode mandar a foto para a fila à toa."""
    if not drive_configurado():
        raise RuntimeError(f"Drive do ponto não configurado ({VARIAVEL_PASTA} ou "
                           "GOOGLE_CREDENTIALS_BASE64 ausentes)")
    from app.apps.erp.core.documentos import drive as drive_erp
    ultimo: Exception | None = None
    for tentativa in range(1, TENTATIVAS_NA_HORA + 1):
        try:
            svc = drive_erp._servico(quem_personificar())
            pasta = _pasta_do_mes(conn, svc, momento)
            return drive_erp.enviar(dados, nome, mime, pasta=pasta,
                                    impersonar=quem_personificar())
        except Exception as e:  # noqa: BLE001 — qualquer falha conta como tentativa
            ultimo = e
            logger.warning("Ponto: subida da foto %s falhou (tentativa %d/%d): %s",
                           nome, tentativa, TENTATIVAS_NA_HORA, str(e)[:200])
            if tentativa < TENTATIVAS_NA_HORA:
                time.sleep(0.5 * tentativa)
    raise RuntimeError(str(ultimo)[:500] if ultimo else "falha desconhecida")


# ---------------------------------------------------------------------------
# Gravação
# ---------------------------------------------------------------------------
def guardar(conn: Connection, foto: FotoPronta, *, nome: str, momento: dt.datetime,
            subir: bool = True) -> int:
    """Grava a ficha e (com `subir`) sobe para o Drive na hora. Sem `subir`, ou
    se o Drive falhar, os bytes ficam na sala de espera — a batida chama
    `disparar_envio()` depois de confirmada, e a linha separada leva. Devolve o
    id da ficha. NUNCA levanta erro: a batida já foi aceita, e a foto não pode
    desfazê-la."""
    file_id, erro = None, None
    if subir:
        try:
            file_id = _subir_no_drive(conn, foto.dados, nome, momento)
        except Exception as e:  # noqa: BLE001
            erro = str(e)[:500]
    sinais_ok = db.tem_coluna(conn, "fotos", "dhash")
    linha = db.um(conn, f"""
        INSERT INTO ponto.fotos (sha256, conteudo, tamanho, mime, largura, altura, nome_arquivo,
                                 drive_file_id, enviada_em, tentativas, ultimo_erro
                                 {', luminancia, contraste, dhash' if sinais_ok else ''})
        VALUES (:h, :c, :t, 'image/jpeg', :w, :a, :nome, :fid,
                CASE WHEN :fid IS NULL THEN NULL ELSE now() END, :tent, :erro
                {', :lum, :con, :dh' if sinais_ok else ''})
        RETURNING id
    """, h=foto.hash, c=(None if file_id else foto.dados), t=len(foto.dados),
         w=foto.largura, a=foto.altura, nome=nome, fid=file_id, erro=erro,
         tent=(1 if subir else 0), lum=foto.luminancia, con=foto.contraste, dh=foto.dhash)
    if file_id:
        logger.info("Ponto: foto %s no Drive (%s, %d bytes)", nome, file_id, len(foto.dados))
    elif subir:
        logger.warning("Ponto: foto %s ficou na fila de reenvio — %s", nome, erro)
    return int(linha["id"])


# ---------------------------------------------------------------------------
# A subida em segundo plano
# ---------------------------------------------------------------------------
_subindo = threading.Lock()


def _subir_em_segundo_plano() -> None:
    try:
        for _ in range(20):                  # no máximo 20 rodadas de 10 por vez
            with db.conexao() as conn:
                r = enviar_pendentes(conn, limite=10)
            if not r["enviadas"] or not r["restantes"]:
                return
    except Exception:  # noqa: BLE001 — a fila fica para a próxima batida
        logger.warning("Ponto: subida das fotos em segundo plano parou", exc_info=True)
    finally:
        _subindo.release()


def disparar_envio() -> bool:
    """Leva a sala de espera para o Drive numa linha separada. Chamada pela
    rota DEPOIS de a batida estar confirmada no banco."""
    if not drive_configurado():
        return False
    if not _subindo.acquire(blocking=False):
        return False
    threading.Thread(target=_subir_em_segundo_plano, name="ponto-fotos", daemon=True).start()
    return True


# ---------------------------------------------------------------------------
# Miniaturas, para o mosaico: guardadas na MEMÓRIA do serviço (não no banco),
# até 32 MB. Mosaico de 60 pessoas × 4 batidas = 240 fotos; do Drive, uma a
# uma, demora — na segunda abertura, sai daqui.
# ---------------------------------------------------------------------------
LADO_MINIATURA = 240
_MINIATURAS_MAX_BYTES = 32 * 1024 * 1024
_miniaturas: "OrderedDict[int, bytes]" = OrderedDict()
_miniaturas_bytes = 0
_miniaturas_trava = threading.Lock()


def _reduzir_miniatura(dados: bytes) -> bytes:
    from PIL import Image
    imagem = Image.open(io.BytesIO(dados))
    imagem.load()
    if imagem.mode not in ("RGB", "L"):
        imagem = imagem.convert("RGB")
    imagem.thumbnail((LADO_MINIATURA, LADO_MINIATURA))
    saida = io.BytesIO()
    imagem.save(saida, format="JPEG", quality=72)
    return saida.getvalue()


def miniatura(conn: Connection, foto_id: int) -> bytes:
    """A miniatura (240 px). Levanta LookupError como `baixar`."""
    global _miniaturas_bytes
    with _miniaturas_trava:
        if foto_id in _miniaturas:
            _miniaturas.move_to_end(foto_id)
            return _miniaturas[foto_id]
    dados, _mime = baixar(conn, foto_id)
    pequena = _reduzir_miniatura(dados)
    with _miniaturas_trava:
        _miniaturas[foto_id] = pequena
        _miniaturas_bytes += len(pequena)
        while _miniaturas_bytes > _MINIATURAS_MAX_BYTES and _miniaturas:
            _, saiu = _miniaturas.popitem(last=False)
            _miniaturas_bytes -= len(saiu)
    return pequena


def baixar(conn: Connection, foto_id: int) -> tuple[bytes, str]:
    """Os bytes da foto (do Drive, ou da sala de espera). Levanta LookupError."""
    f = db.um(conn, "SELECT drive_file_id, conteudo, mime FROM ponto.fotos WHERE id = :id",
              id=foto_id)
    if not f:
        raise LookupError("foto não encontrada")
    if f["conteudo"] is not None:
        return bytes(f["conteudo"]), f["mime"]
    if not f["drive_file_id"]:
        raise LookupError("foto expurgada")
    from app.apps.erp.core.documentos import drive as drive_erp
    return drive_erp.baixar(f["drive_file_id"], impersonar=quem_personificar()), f["mime"]


def pendentes(conn: Connection) -> int:
    return int(db.um(conn, "SELECT count(*) AS n FROM ponto.fotos "
                           "WHERE drive_file_id IS NULL AND conteudo IS NOT NULL")["n"])


def enviar_pendentes(conn: Connection, *, limite: int = 50) -> dict:
    """Leva até `limite` fotos da sala de espera para o Drive. Confere o hash do
    que subiu antes de apagar os bytes do banco."""
    fila = db.todos(conn, """
        SELECT f.id, f.conteudo, f.sha256, f.nome_arquivo, f.criado_em
          FROM ponto.fotos f
         WHERE f.drive_file_id IS NULL AND f.conteudo IS NOT NULL
         ORDER BY f.id LIMIT :n
    """, n=max(1, min(int(limite), 500)))
    enviadas, falhas = 0, []
    for f in fila:
        dados = bytes(f["conteudo"])
        if sha256(dados) != f["sha256"]:
            falhas.append({"id": f["id"], "erro": "conteúdo no banco não confere com o hash"})
            continue
        try:
            file_id = _subir_no_drive(conn, dados, f["nome_arquivo"] or f"foto_{f['id']}.jpg",
                                      f["criado_em"])
        except Exception as e:  # noqa: BLE001
            db.executar(conn, "UPDATE ponto.fotos SET tentativas = tentativas + 1, "
                              "ultimo_erro = :e WHERE id = :id", e=str(e)[:500], id=f["id"])
            falhas.append({"id": f["id"], "erro": str(e)[:200]})
            continue
        db.executar(conn, """
            UPDATE ponto.fotos SET drive_file_id = :fid, enviada_em = now(), conteudo = NULL,
                   ultimo_erro = NULL WHERE id = :id
        """, fid=file_id, id=f["id"])
        enviadas += 1
    if enviadas:
        logger.info("Ponto: %d foto(s) da fila subiram para o Drive", enviadas)
    return {"enviadas": enviadas, "falhas": falhas, "restantes": pendentes(conn)}

