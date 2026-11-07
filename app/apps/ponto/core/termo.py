# -*- coding: utf-8 -*-
"""
O TERMO DO PONTO ELETRÔNICO, ASSINADO NO CELULAR (07/11/2026, migração 028).

O dono: *"para fechar a formalização do aplicativo em relação aos funcionários (…) coletar a assinatura deles (…) das
pessoas que já estão admitidas e das que ainda vão ser admitidas. Toda vida que entrar tem que gerar alguma pendência
(…) o próprio DP também tem que visualizar (…) um termo jurídico simples, porque o nível de instrução das pessoas é
baixo — o mais curto e simples possível (…) o ideal é que eles assinem no próprio celular deles, mas quem não conseguir
também poder ser disponibilizado pelo pessoal da própria obra."*

DUAS FORMAS, O MESMO TERMO:
  · PELA_PESSOA — ela entrou no aplicativo com CPF e PIN, lê e assina; digita o PIN de novo na hora (é o "assino").
  · CONDUZIDA — o operador do ponto da obra (ou o administrativo) mostra o termo no aparelho dele. A pessoa digita o
    PIN dela; quem não tem PIN (ou esqueceu) tira uma FOTO na hora, que fica junto. Sem um dos dois, não assina.

QUEM PRECISA ASSINAR: quem bate ponto (o mesmo conjunto das listas — fora o desligado, o afastado e quem não bate).
Quem entra na empresa já nasce com a pendência: não há nada a fazer além de cadastrar.

O TEXTO TEM LACRE (SHA-256): mudou uma vírgula, todos voltam a ter a pendência, e a assinatura antiga fica guardada
como SUBSTITUÍDA. Por isso o texto mora AQUI, num lugar só — o aplicativo e o PDF leem daqui.

Por que é "termo de ciência" e não "consentimento" (dito ao dono): para o ponto, a base da LGPD é a obrigação legal
de registrar a jornada (art. 7º, II) e a prevenção à fraude na biometria (art. 11, II, "g"). Consentimento pode ser
retirado a qualquer hora — e aí a empresa ficaria sem poder registrar o ponto de quem retirou. A pessoa é INFORMADA e
assina que entendeu; recusar fica registrado, e o ponto continua.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import io
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado

logger = logging.getLogger("ponto.termo")

VERSAO = "1 (novembro de 2026)"
TITULO = "Termo do ponto eletrônico"
ITENS = (
    "O meu ponto é marcado pelo aplicativo da BWS: na chegada, no intervalo e na saída.",
    "Quando eu bato o ponto, o aplicativo tira uma foto do meu rosto e guarda o lugar onde estou (GPS). "
    "É só para mostrar que fui eu e que eu estava no trabalho.",
    "O meu PIN e o meu QR Code são só meus. Não passo para ninguém e não bato o ponto de outra pessoa. "
    "Bater o ponto de outro é falta grave.",
    "Posso ver o meu ponto no aplicativo ou com o pessoal da obra. Se estiver errado, peço a correção.",
    "Todo mês eu confiro e assino a minha folha de ponto.",
    "A BWS guarda as fotos e as marcações pelo tempo que a lei manda e não usa para outra coisa. "
    "Posso perguntar ao DP o que está guardado sobre mim.",
)
DECLARACAO = "Li, ou leram para mim, e entendi. Assino."
MAX_FOTO = 900 * 1024


def lacre() -> str:
    """PURA. O SHA-256 do texto do termo, como ele é mostrado."""
    corpo = "\n".join((TITULO, VERSAO, *ITENS, DECLARACAO))
    return hashlib.sha256(corpo.encode("utf-8")).hexdigest()


def texto() -> dict:
    return {"titulo": TITULO, "versao": VERSAO, "itens": list(ITENS), "declaracao": DECLARACAO, "lacre": lacre()}


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "termo_aceites", "lacre")


def tem_pin(conn: Connection, colaborador_id: int) -> bool:
    return bool(db.um(conn, "SELECT 1 FROM ponto.colaborador_config WHERE colaborador_id = :c AND pin_hash IS NOT NULL",
                      c=int(colaborador_id)))


# ---------------------------------------------------------------------------
# A situação
# ---------------------------------------------------------------------------
def _valendo(conn: Connection, ids: list[int]) -> dict[int, dict]:
    if not ids or not disponivel(conn):
        return {}
    return {int(l["colaborador_id"]): dict(l) for l in db.todos(conn, """
        SELECT id, colaborador_id, situacao, forma, com_pin, lacre, versao, criado_em, conduzida_por_nome, motivo_recusa,
               documento_id
          FROM ponto.termo_aceites WHERE colaborador_id = ANY(:ids) AND situacao <> 'SUBSTITUIDO'""",
        ids=[int(i) for i in ids])}


def _para_json(a: Optional[dict]) -> dict:
    """ASSINADO, RECUSADO ou FALTA (também quem assinou um texto que já mudou — `versao_antiga`)."""
    if not a:
        return {"situacao": "FALTA"}
    antiga = a["situacao"] == "ASSINADO" and a["lacre"] != lacre()
    return {"situacao": "FALTA" if antiga else a["situacao"], "versao_antiga": antiga,
            "em": horario.texto(a["criado_em"]), "forma": a["forma"], "com_pin": bool(a["com_pin"]),
            "por": a["conduzida_por_nome"], "motivo": a["motivo_recusa"], "documento_id": a["documento_id"],
            "id": int(a["id"])}


def situacao_de(conn: Connection, ids: list[int]) -> dict[int, dict]:
    """{pessoa: situação do termo} — para as listas (a do DP e a do operador)."""
    valendo = _valendo(conn, ids)
    return {int(i): _para_json(valendo.get(int(i))) for i in ids}


def da_pessoa(conn: Connection, colaborador_id: int) -> dict:
    """A situação de agora e o histórico (também as substituídas) — para a ficha no ERP."""
    atual = situacao_de(conn, [int(colaborador_id)])[int(colaborador_id)] if disponivel(conn) else {"situacao": "FALTA"}
    historico = [] if not disponivel(conn) else [
        {"id": int(l["id"]), "situacao": l["situacao"], "versao": l["versao"], "forma": l["forma"],
         "em": horario.texto(l["criado_em"]), "por": l["conduzida_por_nome"], "motivo": l["motivo_recusa"],
         "documento_id": l["documento_id"], "substituido": l["substituido_motivo"]}
        for l in db.todos(conn, "SELECT * FROM ponto.termo_aceites WHERE colaborador_id = :c ORDER BY id DESC",
                          c=int(colaborador_id))]
    return {**atual, "disponivel": disponivel(conn), "historico": historico}


def pendentes(conn: Connection) -> list[dict]:
    """Quem bate ponto e ainda não tem o termo de agora assinado nem recusado: [{id, nome, obra_id}]."""
    from . import cadastros
    if not disponivel(conn):
        return []
    pessoas = cadastros.listar_colaboradores(conn)          # quem bate ponto (o mesmo conjunto das listas)
    sit = situacao_de(conn, [int(p["id"]) for p in pessoas])
    return [{"id": int(p["id"]), "nome": p["nome"], "obra_id": p.get("obra_id")}
            for p in pessoas if sit[int(p["id"])]["situacao"] == "FALTA"]


# ---------------------------------------------------------------------------
# Assinar e recusar
# ---------------------------------------------------------------------------
def _foto(foto) -> Optional[bytes]:
    if not foto:
        return None
    t = str(foto)
    if "," in t[:100]:
        t = t.split(",", 1)[1]
    try:
        dados = base64.b64decode(t, validate=True)
    except Exception as e:  # noqa: BLE001
        raise ErroDeValidacao("a foto não chegou — tire de novo", campo="foto") from e
    if not dados.startswith(b"\xff\xd8") or len(dados) < 1000:
        raise ErroDeValidacao("a foto não chegou — tire de novo", campo="foto")
    if len(dados) > MAX_FOTO:
        raise ErroDeValidacao("foto grande demais — tire de novo", campo="foto")
    return dados


def _substituir(conn: Connection, aceite_id: int, motivo: str) -> None:
    db.executar(conn, """UPDATE ponto.termo_aceites SET situacao = 'SUBSTITUIDO', substituido_em = now(),
                                substituido_motivo = :m WHERE id = :i AND situacao <> 'SUBSTITUIDO'""",
                i=int(aceite_id), m=motivo)


def assinar(conn: Connection, *, colaborador_id: int, assinatura, forma: str, conduzida_por: dict,
            pin: Optional[str] = None, foto=None, dispositivo_id: Optional[int] = None, latitude=None, longitude=None,
            ip: str = "") -> dict:
    """A pessoa assina o termo. PELA_PESSOA: o PIN dela, sempre. CONDUZIDA: o PIN dela ou, na falta, a foto na hora."""
    from . import acesso, cadastros, documentos
    from .assinaturas import _png
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 028) antes")
    if forma not in ("PELA_PESSOA", "CONDUZIDA"):
        raise ErroDeValidacao("forma de assinatura desconhecida")
    pessoa = cadastros.colaborador_por_id(conn, int(colaborador_id))
    if not pessoa:
        raise NaoEncontrado("pessoa não encontrada")
    if forma == "CONDUZIDA" and int(conduzida_por.get("id") or 0) == int(colaborador_id):
        raise ErroDeValidacao("o seu próprio termo você assina em “Assinar o termo do ponto”, no começo da tela")
    pin = "".join(ch for ch in str(pin or "") if ch.isdigit())
    foto_ = _foto(foto)
    if forma == "PELA_PESSOA" and not pin:
        raise ErroDeValidacao("digite o seu PIN para assinar", campo="pin")
    if not pin and not foto_:
        raise ErroDeValidacao("a pessoa digita o PIN dela — ou, se não tiver PIN, tire uma foto dela agora", campo="pin")
    png = _png(assinatura)
    if pin:
        acesso.entrar(conn, pessoa["cpf"], pin)               # o PIN da própria pessoa (a frase de erro de sempre)
    ja = _valendo(conn, [int(colaborador_id)]).get(int(colaborador_id))
    if ja and ja["situacao"] == "ASSINADO" and ja["lacre"] == lacre():
        raise ErroDeValidacao(f"{pessoa['nome'].split(' ')[0]} já assinou o termo do ponto")
    if ja:
        _substituir(conn, ja["id"], "a pessoa assinou depois da recusa" if ja["situacao"] == "RECUSADO"
                    else "o texto do termo mudou — assinado de novo")
    agora = horario.agora()
    selo = lacre()
    pdf = termo_pdf(pessoa, assinatura=png, foto=foto_, quando=agora, forma=forma, com_pin=bool(pin),
                    conduzida_por=conduzida_por.get("nome") or "", latitude=latitude, longitude=longitude,
                    dispositivo_id=dispositivo_id)
    doc = documentos.guardar(conn, "data:application/pdf;base64," + base64.b64encode(pdf).decode(),
                             colaborador_id=int(colaborador_id), tipo_ocorrencia="TERMO_PONTO",
                             nome_original=f"termo_do_ponto_{pessoa['cpf']}.pdf")
    linha = db.um(conn, """
        INSERT INTO ponto.termo_aceites (colaborador_id, versao, lacre, situacao, forma, com_pin, assinatura_png,
               foto_jpeg, documento_id, conduzida_por_id, conduzida_por_nome, dispositivo_id, latitude, longitude, ip)
        VALUES (:c, :v, :l, 'ASSINADO', :f, :cp, :png, :foto, :doc, :cid, :cnome, :disp, :lat, :lon, :ip) RETURNING id""",
        c=int(colaborador_id), v=VERSAO, l=selo, f=forma, cp=bool(pin), png=png, foto=foto_, doc=doc,
        cid=conduzida_por.get("id"), cnome=str(conduzida_por.get("nome") or "")[:120], disp=dispositivo_id,
        lat=latitude, lon=longitude, ip=(ip or "")[:60])
    logger.info("Ponto: termo de %s ASSINADO (%s, %s)", colaborador_id, forma, "PIN" if pin else "foto")
    return {"id": int(linha["id"]), "documento_id": doc, "lacre": selo}


def recusar(conn: Connection, *, colaborador_id: int, motivo: str, conduzida_por: dict,
            dispositivo_id: Optional[int] = None, latitude=None, longitude=None, ip: str = "") -> dict:
    """A pessoa não quis assinar: fica registrado com o que ela disse e quem conduziu. O ponto continua valendo."""
    from . import cadastros
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 028) antes")
    if not cadastros.colaborador_por_id(conn, int(colaborador_id)):
        raise NaoEncontrado("pessoa não encontrada")
    if int(conduzida_por.get("id") or 0) == int(colaborador_id):
        raise ErroDeValidacao("a recusa é registrada por quem conduz, não pela própria pessoa")
    motivo = " ".join(str(motivo or "").split())
    if len(motivo) < 5:
        raise ErroDeValidacao("diga o que a pessoa disse ao recusar (ao menos 5 letras)", campo="motivo")
    ja = _valendo(conn, [int(colaborador_id)]).get(int(colaborador_id))
    if ja and (ja["situacao"] == "RECUSADO" or ja["lacre"] == lacre()):
        raise ErroDeValidacao("o termo desta pessoa já foi assinado ou recusado")
    if ja:                                                    # assinou o texto antigo e recusa o novo
        _substituir(conn, ja["id"], "o texto do termo mudou — a pessoa recusou o novo")
    linha = db.um(conn, """
        INSERT INTO ponto.termo_aceites (colaborador_id, versao, lacre, situacao, forma, motivo_recusa,
               conduzida_por_id, conduzida_por_nome, dispositivo_id, latitude, longitude, ip)
        VALUES (:c, :v, :l, 'RECUSADO', 'CONDUZIDA', :m, :cid, :cnome, :disp, :lat, :lon, :ip) RETURNING id""",
        c=int(colaborador_id), v=VERSAO, l=lacre(), m=motivo[:300], cid=conduzida_por.get("id"),
        cnome=str(conduzida_por.get("nome") or "")[:120], disp=dispositivo_id, lat=latitude, lon=longitude,
        ip=(ip or "")[:60])
    logger.info("Ponto: termo de %s RECUSADO (conduzido por %s)", colaborador_id, conduzida_por.get("nome"))
    return {"id": int(linha["id"])}


# ---------------------------------------------------------------------------
# O PDF: o termo e o comprovante
# ---------------------------------------------------------------------------
def termo_pdf(pessoa: dict, *, assinatura: bytes, foto: Optional[bytes], quando: dt.datetime, forma: str,
              com_pin: bool, conduzida_por: str, latitude=None, longitude=None, dispositivo_id=None) -> bytes:
    """PURA (sem banco). O termo, a assinatura, a foto (quando houver) e o comprovante."""
    from fpdf import FPDF
    cpf = str(pessoa.get("cpf") or "")
    cpf_f = f"{cpf[0:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}" if len(cpf) == 11 else cpf
    pdf = FPDF(orientation="P", format="A4")
    pdf.set_auto_page_break(True, margin=12)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 15)
    pdf.cell(0, 9, f"{TITULO} - BWS", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, f"{pessoa.get('nome') or ''}  -  CPF {cpf_f}  -  versão {VERSAO}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 11)
    for i, item in enumerate(ITENS, 1):
        pdf.multi_cell(0, 6, f"{i}. {item}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
    pdf.set_font("Helvetica", "B", 11)
    pdf.multi_cell(0, 6, DECLARACAO, new_x="LMARGIN", new_y="NEXT")
    y = pdf.get_y() + 3
    try:
        pdf.image(io.BytesIO(assinatura), x=pdf.l_margin, y=y, w=70)
    except Exception:  # noqa: BLE001 — imagem ilegível não derruba o comprovante
        pass
    if foto:
        try:
            pdf.image(io.BytesIO(foto), x=pdf.l_margin + 100, y=y, h=36)
        except Exception:  # noqa: BLE001
            pass
    pdf.set_y(y + 40)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Comprovante de assinatura eletrônica", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8.5)
    local = horario.para_local(quando)
    como = ("no aplicativo, entrando com CPF e PIN" if forma == "PELA_PESSOA"
            else f"no aparelho de {conduzida_por or '-'}, que conduziu")
    prova = "o PIN pessoal digitado por ela" if com_pin else "a foto dela, tirada na hora (ao lado da assinatura)"
    for t in (f"Assinado por: {pessoa.get('nome') or ''} - CPF {cpf_f}",
              f"Em: {local:%d/%m/%Y %H:%M} (horário de Fortaleza), {como}",
              f"Quem assinou foi confirmado por: {prova}",
              (f"Aparelho nº {dispositivo_id}" if dispositivo_id else "Aparelho: não informado"),
              (f"Local: {latitude}, {longitude}" if latitude is not None and longitude is not None
               else "Local: não informado"),
              f"Lacre do texto (SHA-256): {lacre()}"):
        pdf.multi_cell(0, 4.4, t, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 7)
    pdf.multi_cell(0, 3.6, "Assinatura eletrônica (Lei 14.063/2020; MP 2.200-2/2001, art. 10 §2º). Tratamento de dados "
                           "pela obrigação legal de registrar a jornada (CLT art. 74; LGPD art. 7º, II) e para prevenir "
                           "fraude no registro (LGPD art. 11, II, g).", new_x="LMARGIN", new_y="NEXT")
    return bytes(pdf.output())
