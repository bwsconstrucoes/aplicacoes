# -*- coding: utf-8 -*-
"""
O QR Code pessoal e o "bilhete" do tablet.

Decisão do dono em 03/10/2026: no tablet da obra a pessoa se identifica por CPF
ou por QR Code — e o QR NÃO é crachá impresso. Crachá se empresta: vinte
crachás na mão de uma pessoa só batem o ponto de vinte. O QR mora no celular da
pessoa e MUDA de tempos em tempos.

DOIS QR, os dois aceitos pelo tablet:

  · **o do WhatsApp** (`BWSP1.<código>`) — uma imagem que chega no WhatsApp da
    pessoa e vale até a próxima troca (7 a 14 dias, sorteado por pessoa — ver
    `envios.py`). O banco guarda só o HASH: quem lê a tabela não gera o QR de
    ninguém. Na troca, o antigo continua valendo até o novo ser usado pela
    primeira vez, ou por 3 dias.

  · **o do "Meu ponto"** (`BWSP2.<pessoa>.<janela>.<assinatura>`) — muda a cada
    30 segundos, na tela do celular de quem entrou com CPF + PIN. Print de tela
    não serve no dia seguinte, nem dali a dois minutos.

O BILHETE: quando o tablet identifica alguém (QR ou CPF), o servidor devolve um
bilhete assinado, que vale 2 minutos e só naquele aparelho. A batida que vem
depois, com a foto, apresenta o bilhete — e a batida guarda COMO a pessoa foi
identificada.

A assinatura usa a chave de sessão do serviço (`ERP_SECRET_KEY`), derivada para
este uso: não é segredo novo para cuidar.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import io
import logging
import secrets
import time
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao

logger = logging.getLogger("ponto.qr")

PREFIXO_WHATSAPP = "BWSP1."
PREFIXO_APP = "BWSP2."
JANELA_APP_S = 30
JANELAS_ACEITAS = 3            # a atual e as duas anteriores: até 90 s de folga
BILHETE_VALIDADE_S = 120
CONVIVENCIA_DIAS = 3           # o QR antigo ainda vale por até 3 dias depois da troca

IDENT_WHATSAPP = "QR_WHATSAPP"
IDENT_APP = "QR_APP"
IDENT_CPF = "CPF"
_CODIGO_IDENT = {IDENT_WHATSAPP: "W", IDENT_APP: "A", IDENT_CPF: "C"}
_IDENT_DO_CODIGO = {v: k for k, v in _CODIGO_IDENT.items()}


# ---------------------------------------------------------------------------
# Chave de assinatura
# ---------------------------------------------------------------------------
def _chave() -> bytes:
    import os
    try:
        from flask import current_app
        base = current_app.secret_key or ""
    except RuntimeError:          # fora de uma requisição (rotina, teste)
        base = ""
    base = base or os.getenv("ERP_SECRET_KEY") or os.getenv("SECRET_KEY") or "bws-erp-dev"
    return hashlib.sha256(f"ponto-qr|{base}".encode("utf-8")).digest()


def _assinar(texto: str) -> str:
    sig = hmac.new(_chave(), texto.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig[:15]).decode("ascii")


def hash_token(token: str) -> str:
    return hashlib.sha256(token.strip().encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# O QR do WhatsApp
# ---------------------------------------------------------------------------
def novo_token() -> str:
    """O conteúdo do QR do WhatsApp. 18 bytes aleatórios: impossível de chutar."""
    return PREFIXO_WHATSAPP + secrets.token_urlsafe(18)


def gravar_enviado(conn: Connection, colaborador_id: int, token: str, motivo: str) -> int:
    """Depois que o WhatsApp ACEITOU a mensagem: guarda o hash do novo QR e põe
    prazo nos antigos (valem até o novo ser usado, ou 3 dias)."""
    db.executar(conn, """
        UPDATE ponto.qr_codigos
           SET substituido_em = COALESCE(substituido_em, now()),
               valido_ate = LEAST(COALESCE(valido_ate, 'infinity'::timestamptz),
                                  now() + make_interval(days => :d))
         WHERE colaborador_id = :c AND revogado_em IS NULL
    """, c=colaborador_id, d=CONVIVENCIA_DIAS)
    linha = db.um(conn, """
        INSERT INTO ponto.qr_codigos (colaborador_id, token_hash, motivo)
        VALUES (:c, :h, :m) RETURNING id
    """, c=colaborador_id, h=hash_token(token), m=motivo)
    return int(linha["id"])


def situacao_da_pessoa(conn: Connection, colaborador_id: int) -> dict:
    """O que a gestão vê: tem QR valendo? quando foi enviado? quando troca?"""
    atual = db.um(conn, """
        SELECT id, motivo, criado_em, primeiro_uso_em, ultimo_uso_em, usos
          FROM ponto.qr_codigos
         WHERE colaborador_id = :c AND revogado_em IS NULL AND substituido_em IS NULL
         ORDER BY id DESC LIMIT 1
    """, c=colaborador_id)
    troca = db.um(conn, "SELECT qr_proxima_troca FROM ponto.colaborador_config "
                        "WHERE colaborador_id = :c", c=colaborador_id)
    na_fila = db.um(conn, """
        SELECT id, motivo, agendado_para, status FROM ponto.envios
         WHERE tipo = 'QR' AND colaborador_id = :c AND status IN ('PENDENTE', 'ENVIANDO')
         ORDER BY id DESC LIMIT 1
    """, c=colaborador_id)
    return {
        "tem_qr": atual is not None,
        "enviado_em": horario.texto(atual["criado_em"]) if atual else None,
        "primeiro_uso_em": horario.texto(atual["primeiro_uso_em"]) if atual else None,
        "ultimo_uso_em": horario.texto(atual["ultimo_uso_em"]) if atual else None,
        "usos": int(atual["usos"]) if atual else 0,
        "proxima_troca": horario.texto((troca or {}).get("qr_proxima_troca")),
        "na_fila": ({"motivo": na_fila["motivo"], "agendado_para": horario.texto(na_fila["agendado_para"]),
                     "status": na_fila["status"]} if na_fila else None),
    }


def revogar_todos(conn: Connection, colaborador_id: int, por: str) -> int:
    """Celular perdido ou roubado: nenhum QR da pessoa vale mais."""
    return db.executar(conn, """
        UPDATE ponto.qr_codigos SET revogado_em = now(), revogado_por = :p
         WHERE colaborador_id = :c AND revogado_em IS NULL
    """, c=colaborador_id, p=por[:120])


# ---------------------------------------------------------------------------
# O QR do "Meu ponto" (muda a cada 30 s)
# ---------------------------------------------------------------------------
def _janela(agora: Optional[float] = None) -> int:
    return int((agora if agora is not None else time.time()) // JANELA_APP_S)


def conteudo_do_app(colaborador_id: int, agora: Optional[float] = None) -> str:
    j = _janela(agora)
    corpo = f"{int(colaborador_id)}.{j}"
    return f"{PREFIXO_APP}{corpo}.{_assinar('app|' + corpo)}"


def segundos_ate_trocar(agora: Optional[float] = None) -> int:
    agora = agora if agora is not None else time.time()
    return int(JANELA_APP_S - (agora % JANELA_APP_S)) or JANELA_APP_S


# ---------------------------------------------------------------------------
# Ler o que o tablet viu
# ---------------------------------------------------------------------------
class Lido:
    """O resultado de ler um QR. `problema` preenchido = não identifica."""
    __slots__ = ("colaborador_id", "identificacao", "problema", "qr_id", "antigo")

    def __init__(self, colaborador_id=None, identificacao=None, problema=None, qr_id=None,
                 antigo=False):
        self.colaborador_id, self.identificacao = colaborador_id, identificacao
        self.problema, self.qr_id, self.antigo = problema, qr_id, antigo


def ler(conn: Connection, conteudo: str, agora: Optional[dt.datetime] = None) -> Lido:
    """Identifica a pessoa pelo conteúdo do QR. Não grava nada — quem grava o
    uso é `registrar_uso`, depois que o tablet conferiu que a pessoa pode bater
    naquele aparelho e naquela obra."""
    texto = (conteudo or "").strip()
    momento = agora or horario.agora()
    if texto.startswith(PREFIXO_APP):
        partes = texto[len(PREFIXO_APP):].split(".")
        if len(partes) != 3 or not partes[0].isdigit() or not partes[1].lstrip("-").isdigit():
            return Lido(problema="QR Code não reconhecido")
        corpo = f"{partes[0]}.{partes[1]}"
        if not hmac.compare_digest(_assinar("app|" + corpo), partes[2]):
            return Lido(problema="QR Code não reconhecido")
        atraso = _janela(momento.timestamp()) - int(partes[1])
        if atraso < 0 or atraso >= JANELAS_ACEITAS:
            return Lido(colaborador_id=int(partes[0]), identificacao=IDENT_APP, antigo=True,
                        problema="este QR já trocou — abra o “Meu ponto” de novo")
        return Lido(colaborador_id=int(partes[0]), identificacao=IDENT_APP)
    if texto.startswith(PREFIXO_WHATSAPP):
        q = db.um(conn, "SELECT * FROM ponto.qr_codigos WHERE token_hash = :h",
                  h=hash_token(texto))
        if not q:
            return Lido(problema="QR Code não reconhecido")
        if q["revogado_em"] is not None or (q["valido_ate"] is not None and q["valido_ate"] <= momento):
            return Lido(colaborador_id=int(q["colaborador_id"]), identificacao=IDENT_WHATSAPP,
                        qr_id=int(q["id"]), antigo=True,
                        problema="este QR Code foi trocado — use o mais novo que chegou no "
                                 "seu WhatsApp, ou digite o CPF")
        return Lido(colaborador_id=int(q["colaborador_id"]), identificacao=IDENT_WHATSAPP,
                    qr_id=int(q["id"]))
    return Lido(problema="QR Code não reconhecido")


def registrar_uso(conn: Connection, qr_id: int, agora: Optional[dt.datetime] = None) -> None:
    """Conta o uso. O PRIMEIRO uso do QR novo encerra os antigos da pessoa — a
    partir daí, só o novo vale."""
    momento = agora or horario.agora()
    q = db.um(conn, "SELECT colaborador_id, primeiro_uso_em, substituido_em FROM ponto.qr_codigos "
                    "WHERE id = :id", id=qr_id)
    if not q:
        return
    db.executar(conn, """
        UPDATE ponto.qr_codigos SET usos = usos + 1, ultimo_uso_em = :m,
               primeiro_uso_em = COALESCE(primeiro_uso_em, :m) WHERE id = :id
    """, m=momento, id=qr_id)
    if q["primeiro_uso_em"] is None and q["substituido_em"] is None:
        db.executar(conn, """
            UPDATE ponto.qr_codigos SET valido_ate = LEAST(COALESCE(valido_ate, :m), :m)
             WHERE colaborador_id = :c AND id < :id AND revogado_em IS NULL
        """, c=q["colaborador_id"], id=qr_id, m=momento)


# ---------------------------------------------------------------------------
# O bilhete do tablet
# ---------------------------------------------------------------------------
# O bilhete do PEDIDO no aparelho da obra (atestado, licença, ajuste) dura mais:
# fotografar o documento e preencher as datas leva minutos. Ele não serve para
# bater (`bilhete_de_pedido`), para a batida continuar sendo de quem está ali.
BILHETE_PEDIDO_S = 600


def emitir_bilhete(colaborador_id: int, dispositivo_id: int, identificacao: str,
                   agora: Optional[float] = None, *, para_pedido: bool = False) -> str:
    validade = int((agora if agora is not None else time.time())
                   + (BILHETE_PEDIDO_S if para_pedido else BILHETE_VALIDADE_S))
    corpo = f"{int(colaborador_id)}.{int(dispositivo_id)}.{validade}.{_CODIGO_IDENT[identificacao]}"
    return f"{corpo}.{_assinar('bilhete|' + corpo)}"


def bilhete_de_pedido(bilhete: str, agora: Optional[float] = None) -> bool:
    """O bilhete dura mais que o da batida? (então é de pedido, e não bate)"""
    partes = str(bilhete or "").strip().split(".")
    if len(partes) != 5 or not partes[2].isdigit():
        return False
    return int(partes[2]) - (agora if agora is not None else time.time()) > BILHETE_VALIDADE_S + 5


def conferir_bilhete(bilhete: str, dispositivo_id: int,
                     agora: Optional[float] = None) -> tuple[int, str]:
    """Devolve (colaborador_id, identificação). Levanta ErroDeValidacao."""
    partes = str(bilhete or "").strip().split(".")
    if len(partes) != 5 or not all(p.isdigit() for p in partes[:3]) or partes[3] not in _IDENT_DO_CODIGO:
        raise ErroDeValidacao("identificação ilegível — identifique-se de novo", campo="bilhete")
    corpo = ".".join(partes[:4])
    if not hmac.compare_digest(_assinar("bilhete|" + corpo), partes[4]):
        raise ErroDeValidacao("identificação ilegível — identifique-se de novo", campo="bilhete")
    if int(partes[1]) != int(dispositivo_id):
        raise ErroDeValidacao("identificação de outro aparelho", campo="bilhete")
    if int(partes[2]) < (agora if agora is not None else time.time()):
        raise ErroDeValidacao("a identificação venceu — mostre o QR ou digite o CPF de novo",
                              campo="bilhete")
    return int(partes[0]), _IDENT_DO_CODIGO[partes[3]]


# ---------------------------------------------------------------------------
# A imagem
# ---------------------------------------------------------------------------
def imagem(conteudo: str, *, formato: str = "PNG", legenda: str = "") -> bytes:
    """O QR em imagem. JPEG para o WhatsApp (o envio de imagem da casa manda
    como JPEG), PNG para a tela. Correção de erro M: aguenta tela riscada."""
    import qrcode
    from PIL import Image, ImageDraw
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=12, border=3)
    qr.add_data(conteudo)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    if legenda:
        largura, altura = img.size
        tela = Image.new("RGB", (largura, altura + 56), "white")
        tela.paste(img, (0, 0))
        desenho = ImageDraw.Draw(tela)
        desenho.text((largura // 2, altura + 22), legenda[:60], fill="black", anchor="mm")
        img = tela
    saida = io.BytesIO()
    if formato.upper() == "JPEG":
        img.save(saida, format="JPEG", quality=92)
    else:
        img.save(saida, format="PNG", optimize=True)
    return saida.getvalue()
