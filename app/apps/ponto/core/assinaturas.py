# -*- coding: utf-8 -*-
"""
A ASSINATURA DA FOLHA DE PONTO NA TELA (06/11/2026, migração 027).

O dono: *"o operador do ponto, com o celular, vai mostrar para a pessoa a folha de ponto, e a pessoa vai assinar
(…) essa assinatura vai ser arquivada (…) para a gente imprimir posteriormente"*. Decidido com ele: prazo até o
DIA 20 do mês seguinte; quem conduz é o operador do ponto da obra ou o administrativo; cópia por WhatsApp, ainda não.

Como fica de pé juridicamente (dito ao dono; a confirmar com o advogado): a assinatura eletrônica vale quando prova
QUEM assinou e que o documento NÃO MUDOU (Lei 14.063/2020; MP 2.200-2/2001, art. 10 §2º). Por isso, guardado junto:
  · a pessoa digita o PIN DELA no momento de assinar (não basta o operador estar logado);
  · a hora do servidor, o aparelho, o local e quem conduziu;
  · o LACRE — o SHA-256 da folha como foi mostrada (os dias, as batidas, os totais). Se o mês for reaberto e a folha
    mudar, a assinatura antiga vira SUBSTITUÍDA (fica guardada) e a folha volta a pedir assinatura;
  · o PDF arquivado: a folha e a página do comprovante de assinatura.
Só se assina mês FECHADO (a folha não muda mais). A recusa é registrada, com o motivo e quem conduziu.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import io
import json
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import competencias

logger = logging.getLogger("ponto.assinaturas")

DIA_DO_PRAZO = 20
MAX_ASSINATURA = 400 * 1024          # o traço do dedo, em PNG
TERMO = ("Declaro que conferi as marcações de ponto deste período, mostradas nesta folha, e que elas correspondem à "
         "minha jornada. Assino com o meu PIN pessoal.")


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "espelho_assinaturas", "lacre")


def prazo(competencia: dt.date) -> dt.date:
    """PURA. O dia 20 do mês seguinte ao da folha."""
    c = competencias.primeiro_dia(competencia)
    return (c.replace(day=28) + dt.timedelta(days=4)).replace(day=DIA_DO_PRAZO)


def lacre(esp: dict) -> str:
    """PURA. O SHA-256 da folha como ela é mostrada: a pessoa, o mês, cada dia (as batidas, a situação, as horas, o
    que justifica) e os totais. O que não aparece na folha (as notas internas) não entra."""
    p = esp.get("pessoa") or {}
    dias = [{"d": d.get("data"), "b": [b.get("hora") for b in d.get("batidas") or [] if b.get("status") != "REJEITADA"],
             "s": d.get("situacao"), "p": d.get("previsto"), "t": d.get("trabalhado"), "e": d.get("extra"),
             "db": d.get("debito"), "o": (d.get("ocorrencia") or {}).get("rotulo")} for d in esp.get("dias") or []]
    r = esp.get("resumo") or {}
    corpo = {"cpf": p.get("cpf"), "nome": p.get("nome"), "competencia": esp.get("competencia"), "dias": dias,
             "resumo": {k: r.get(k) for k in ("previsto", "trabalhado", "extra", "debito", "faltas", "abonados")}}
    return hashlib.sha256(json.dumps(corpo, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _png(assinatura) -> bytes:
    texto = str(assinatura or "")
    if "," in texto[:100]:
        texto = texto.split(",", 1)[1]
    try:
        dados = base64.b64decode(texto, validate=True)
    except Exception as e:  # noqa: BLE001
        raise ErroDeValidacao("a assinatura não chegou — assine de novo no quadro", campo="assinatura") from e
    if not dados.startswith(b"\x89PNG") or len(dados) < 200:
        raise ErroDeValidacao("a assinatura não chegou — assine de novo no quadro", campo="assinatura")
    if len(dados) > MAX_ASSINATURA:
        raise ErroDeValidacao("assinatura grande demais — limpe e assine de novo", campo="assinatura")
    return dados


# ---------------------------------------------------------------------------
# A situação do mês
# ---------------------------------------------------------------------------
def _valendo(conn: Connection, ids: list[int], competencia: dt.date) -> dict[int, dict]:
    if not ids:
        return {}
    return {int(l["colaborador_id"]): dict(l) for l in db.todos(conn, """
        SELECT id, colaborador_id, situacao, lacre, criado_em, conduzida_por_nome, motivo_recusa, documento_id
          FROM ponto.espelho_assinaturas
         WHERE colaborador_id = ANY(:ids) AND competencia = :c AND situacao <> 'SUBSTITUIDA'""",
        ids=[int(i) for i in ids], c=competencia)}


def conferir_se_mudou(conn: Connection, assinatura: dict, esp: dict) -> bool:
    """A folha mudou depois de assinada (o mês foi reaberto)? Então a assinatura vira SUBSTITUÍDA. Devolve se mudou."""
    if assinatura["situacao"] != "ASSINADA" or assinatura["lacre"] == lacre(esp):
        return False
    db.executar(conn, """UPDATE ponto.espelho_assinaturas SET situacao = 'SUBSTITUIDA', substituida_em = now(),
                                substituida_motivo = 'a folha mudou depois de assinada (o mês foi reaberto)'
                          WHERE id = :i AND situacao = 'ASSINADA'""", i=int(assinatura["id"]))
    logger.info("Ponto: assinatura %s substituída — a folha mudou", assinatura["id"])
    return True


def situacao(conn: Connection, pessoas: list[dict], competencia, *, hoje: Optional[dt.date] = None) -> dict:
    """A lista do mês para quem conduz: cada pessoa ASSINADA / RECUSADA / FALTA, o prazo e se o mês está fechado."""
    from . import espelho
    comp = competencias.primeiro_dia(competencia)
    hoje = hoje or horario.hoje()
    fechado = competencias.esta_fechada(conn, comp)
    ids = [int(p["id"]) for p in pessoas]
    valendo = _valendo(conn, ids, comp) if disponivel(conn) else {}
    linhas = []
    if valendo:
        lote = espelho.Lote(conn, pessoas, comp, competencias.ultimo_dia(comp))
        for p in pessoas:
            a = valendo.get(int(p["id"]))
            if a and a["situacao"] == "ASSINADA":
                esp = espelho.montar(conn, int(p["id"]), comp, competencias.ultimo_dia(comp), lote=lote)
                esp["competencia"] = comp.strftime("%Y-%m")
                if conferir_se_mudou(conn, a, esp):
                    valendo.pop(int(p["id"]))
    for p in pessoas:
        a = valendo.get(int(p["id"]))
        linhas.append({"id": int(p["id"]), "nome": p["nome"], "funcao": p.get("funcao") or "",
                       "situacao": a["situacao"] if a else "FALTA",
                       "em": horario.texto(a["criado_em"]) if a else None,
                       "por": a["conduzida_por_nome"] if a else None,
                       "motivo": a.get("motivo_recusa") if a else None})
    linhas.sort(key=lambda x: (x["situacao"] != "FALTA", (x["nome"] or "").upper()))
    p_ = prazo(comp)
    return {"competencia": comp.strftime("%Y-%m"), "fechado": fechado, "prazo": p_.isoformat(),
            "vencido": hoje > p_, "pessoas": linhas, "disponivel": disponivel(conn),
            "faltam": sum(1 for x in linhas if x["situacao"] == "FALTA")}


# ---------------------------------------------------------------------------
# Assinar e recusar
# ---------------------------------------------------------------------------
def _folha_do_mes(conn: Connection, colaborador_id: int, comp: dt.date) -> dict:
    from . import espelho
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 027) antes")
    if not competencias.esta_fechada(conn, comp):
        raise ErroDeValidacao(f"o mês {comp:%m/%Y} ainda não foi fechado — a folha só é assinada depois do fechamento")
    return espelho.do_mes(conn, colaborador_id, comp)


def assinar(conn: Connection, *, colaborador_id: int, competencia, pin: str, assinatura, conduzida_por: dict,
            dispositivo_id: Optional[int] = None, latitude=None, longitude=None, ip: str = "") -> dict:
    """A pessoa assina a folha do mês: o PIN DELA confere, a folha é lacrada e o PDF arquivado."""
    from . import acesso, cadastros, documentos
    comp = competencias.primeiro_dia(competencia)
    pessoa = cadastros.colaborador_por_id(conn, int(colaborador_id))
    if not pessoa:
        raise NaoEncontrado("pessoa não encontrada")
    if int(conduzida_por.get("id") or 0) == int(colaborador_id):
        raise ErroDeValidacao("quem conduz não assina a própria folha por aqui — outra pessoa conduz a sua")
    esp = _folha_do_mes(conn, int(colaborador_id), comp)
    acesso.entrar(conn, pessoa["cpf"], pin)                    # o PIN da própria pessoa (a frase de erro de sempre)
    png = _png(assinatura)
    ja = _valendo(conn, [int(colaborador_id)], comp).get(int(colaborador_id))
    if ja and ja["situacao"] == "ASSINADA" and not conferir_se_mudou(conn, ja, esp):
        raise ErroDeValidacao(f"{pessoa['nome'].split(' ')[0]} já assinou a folha de {comp:%m/%Y}")
    if ja and ja["situacao"] == "RECUSADA":          # mudou de ideia: a recusa fica guardada como substituída
        db.executar(conn, """UPDATE ponto.espelho_assinaturas SET situacao = 'SUBSTITUIDA', substituida_em = now(),
                                    substituida_motivo = 'a pessoa assinou depois da recusa' WHERE id = :i""", i=ja["id"])
    agora = horario.agora()
    selo = lacre(esp)
    pdf = folha_assinada_pdf(esp, assinatura=png, quando=agora, conduzida_por=conduzida_por.get("nome") or "",
                             lacre_=selo, latitude=latitude, longitude=longitude, dispositivo_id=dispositivo_id)
    doc = documentos.guardar(conn, "data:application/pdf;base64," + base64.b64encode(pdf).decode(),
                             colaborador_id=int(colaborador_id), tipo_ocorrencia="ESPELHO_ASSINADO",
                             nome_original=f"folha_{comp:%Y-%m}_{pessoa['cpf']}.pdf")
    linha = db.um(conn, """
        INSERT INTO ponto.espelho_assinaturas (colaborador_id, competencia, situacao, lacre, assinatura_png, documento_id,
               conduzida_por_id, conduzida_por_nome, dispositivo_id, latitude, longitude, ip)
        VALUES (:c, :comp, 'ASSINADA', :l, :png, :doc, :cid, :cnome, :disp, :lat, :lon, :ip) RETURNING id""",
        c=int(colaborador_id), comp=comp, l=selo, png=png, doc=doc, cid=conduzida_por.get("id"),
        cnome=str(conduzida_por.get("nome") or "")[:120], disp=dispositivo_id, lat=latitude, lon=longitude, ip=ip[:60])
    logger.info("Ponto: folha %s de %s ASSINADA (conduzida por %s, lacre %s…)", comp, colaborador_id,
                conduzida_por.get("nome"), selo[:12])
    return {"id": int(linha["id"]), "documento_id": doc, "lacre": selo}


def recusar(conn: Connection, *, colaborador_id: int, competencia, motivo: str, conduzida_por: dict,
            dispositivo_id: Optional[int] = None, latitude=None, longitude=None, ip: str = "") -> dict:
    from . import cadastros
    comp = competencias.primeiro_dia(competencia)
    if not cadastros.colaborador_por_id(conn, int(colaborador_id)):
        raise NaoEncontrado("pessoa não encontrada")
    motivo = " ".join(str(motivo or "").split())
    if len(motivo) < 5:
        raise ErroDeValidacao("diga o que a pessoa disse ao recusar (ao menos 5 letras)", campo="motivo")
    esp = _folha_do_mes(conn, int(colaborador_id), comp)
    if _valendo(conn, [int(colaborador_id)], comp).get(int(colaborador_id)):
        raise ErroDeValidacao("esta folha já foi assinada ou recusada neste mês")
    linha = db.um(conn, """
        INSERT INTO ponto.espelho_assinaturas (colaborador_id, competencia, situacao, lacre, motivo_recusa,
               conduzida_por_id, conduzida_por_nome, dispositivo_id, latitude, longitude, ip)
        VALUES (:c, :comp, 'RECUSADA', :l, :m, :cid, :cnome, :disp, :lat, :lon, :ip) RETURNING id""",
        c=int(colaborador_id), comp=comp, l=lacre(esp), m=motivo[:300], cid=conduzida_por.get("id"),
        cnome=str(conduzida_por.get("nome") or "")[:120], disp=dispositivo_id, lat=latitude, lon=longitude, ip=ip[:60])
    logger.info("Ponto: folha %s de %s RECUSADA (conduzida por %s)", comp, colaborador_id, conduzida_por.get("nome"))
    return {"id": int(linha["id"])}


def da_pessoa(conn: Connection, colaborador_id: int) -> list[dict]:
    """As assinaturas da pessoa (todas, também as substituídas), da mais nova para a mais antiga — para o ERP."""
    if not disponivel(conn):
        return []
    return [{"id": int(l["id"]), "competencia": l["competencia"].strftime("%Y-%m"), "situacao": l["situacao"],
             "em": horario.texto(l["criado_em"]), "por": l["conduzida_por_nome"], "motivo": l["motivo_recusa"],
             "documento_id": l["documento_id"], "substituida": l["substituida_motivo"]}
            for l in db.todos(conn, """SELECT * FROM ponto.espelho_assinaturas WHERE colaborador_id = :c
                                        ORDER BY competencia DESC, id DESC""", c=int(colaborador_id))]


# ---------------------------------------------------------------------------
# O PDF: a folha e o comprovante da assinatura
# ---------------------------------------------------------------------------
def _hm(m) -> str:
    if m is None or m == "":
        return "-"
    n = int(m)
    return f"{'-' if n < 0 else ''}{abs(n) // 60}h{abs(n) % 60:02d}"


SITUACAO = {"OK": "Normal", "FALTA": "Falta", "ABONADO": "Abonado", "INCOMPLETO": "Batida faltando",
            "FALTA_PARCIAL": "Falta parcial", "DESCANSO": "Descanso", "SEM_ESCALA": "Sem escala",
            "COMPENSADO": "Compensado", "FUTURO": "", "EM_ANDAMENTO": "", "FORA_DO_CONTRATO": "Fora do contrato",
            "ANTES_DO_PONTO": "Antes do ponto"}


def folha_assinada_pdf(esp: dict, *, assinatura: bytes, quando: dt.datetime, conduzida_por: str, lacre_: str,
                       latitude=None, longitude=None, dispositivo_id=None) -> bytes:
    """PURA (sem banco). A folha do mês e, no fim, o comprovante da assinatura eletrônica."""
    from fpdf import FPDF
    p, r = esp.get("pessoa") or {}, esp.get("resumo") or {}
    cpf = str(p.get("cpf") or "")
    cpf_f = f"{cpf[0:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}" if len(cpf) == 11 else cpf
    comp = str(esp.get("competencia") or "")
    pdf = FPDF(orientation="P", format="A4")
    pdf.set_auto_page_break(True, margin=12)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, f"Espelho de ponto - {comp[5:7]}/{comp[0:4]}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, f"{p.get('nome') or ''}  -  CPF {cpf_f}  -  Obra {p.get('obra') or '-'}", new_x="LMARGIN", new_y="NEXT")
    br = lambda iso: f"{iso[8:10]}/{iso[5:7]}/{iso[0:4]}" if iso and len(str(iso)) >= 10 else ""  # noqa: E731
    pdf.cell(0, 5, f"Período {br(str(esp.get('inicio', '')))} a {br(str(esp.get('fim', '')))}  -  BWS - Ponto eletrônico (REP-P)",
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    larg = [20, 14, 58, 18, 20, 16, 16, 28]
    cab = ["Data", "Dia", "Batidas", "Previsto", "Trabalhado", "Extra", "Débito", "Situação"]
    pdf.set_font("Helvetica", "B", 7.5)
    for w, t in zip(larg, cab):
        pdf.cell(w, 5, t, border=1)
    pdf.ln()
    pdf.set_font("Helvetica", "", 7.5)
    for d in esp.get("dias") or []:
        bat = " ".join(b.get("hora", "") + ("(aj)" if b.get("status") == "AJUSTADA" else "")
                       for b in d.get("batidas") or [] if b.get("status") != "REJEITADA")
        sit = (d.get("ocorrencia") or {}).get("rotulo") or SITUACAO.get(d.get("situacao"), d.get("situacao") or "")
        data = str(d.get("data") or "")
        valores = [f"{data[8:10]}/{data[5:7]}", (d.get("dia_semana") or "")[:3], bat[:48], _hm(d.get("previsto")),
                   _hm(d.get("trabalhado")), _hm(d.get("extra")) if d.get("extra") else "",
                   _hm(d.get("debito")) if d.get("debito") else "", str(sit)[:20]]
        for w, t in zip(larg, valores):
            pdf.cell(w, 4.6, t, border=1)
        pdf.ln()
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(0, 6, f"Totais: previsto {_hm(r.get('previsto'))} - trabalhado {_hm(r.get('trabalhado'))} - extra "
                   f"{_hm(r.get('extra'))} - débito {_hm(r.get('debito'))} - faltas {r.get('faltas') or 0}",
             new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 7)
    pdf.multi_cell(0, 3.6, "(aj) batida incluída por ajuste aprovado - a original, quando existe, continua guardada. "
                           "Batidas originais preservadas (Portaria MTP 671/2021).", new_x="LMARGIN", new_y="NEXT")
    # o comprovante
    pdf.ln(3)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, "Comprovante de assinatura eletrônica", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8.5)
    pdf.multi_cell(0, 4.4, TERMO, new_x="LMARGIN", new_y="NEXT")
    y = pdf.get_y() + 2
    try:
        pdf.image(io.BytesIO(assinatura), x=pdf.l_margin, y=y, w=70)
    except Exception:  # noqa: BLE001 — imagem ilegível não derruba o comprovante
        pass
    pdf.set_y(y + 30)
    local = horario.para_local(quando)
    linhas = [f"Assinado por: {p.get('nome') or ''} - CPF {cpf_f}",
              f"Em: {local:%d/%m/%Y %H:%M} (horário de Fortaleza), com o PIN pessoal da pessoa",
              f"Conduzido por: {conduzida_por or '-'}" + (f" - aparelho nº {dispositivo_id}" if dispositivo_id else ""),
              f"Local: {latitude}, {longitude}" if latitude is not None and longitude is not None else "Local: não informado",
              f"Lacre da folha (SHA-256): {lacre_}"]
    for t in linhas:
        pdf.multi_cell(0, 4.4, t, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 7)
    pdf.multi_cell(0, 3.6, "Assinatura eletrônica (Lei 14.063/2020; MP 2.200-2/2001, art. 10 §2º). O lacre é calculado a "
                           "partir dos números desta folha: qualquer mudança neles produz outro lacre.", new_x="LMARGIN", new_y="NEXT")
    return bytes(pdf.output())
