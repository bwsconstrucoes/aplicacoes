# -*- coding: utf-8 -*-
"""
O MOSAICO: as fotos do dia de uma obra, lado a lado, por pessoa.

Pedido do dono, 03/10/2026: *"o mosaico é interessante (…) alguém analisar e
confirmar (…) não é obrigatório (…) se tiver como obrigatoriedade a gente
determinar alguma obra que seja obrigatória e vincular uma pessoa (…) ela vai
receber e vai ter que fazer essa confirmação, e ter esse alerta no ponto
dizendo que está faltando validação."*

POR QUE ELE FUNCIONA: comparar rostos é a coisa que gente faz melhor que
máquina barata. Numa linha por pessoa — foto de cadastro, depois as quatro
batidas do dia —, o rosto que não é o mesmo salta aos olhos em segundos. Numa
lista de batidas, não salta nunca.

DOIS MODOS POR OBRA (`ponto.obra_config`):
  · OPCIONAL (padrão) — o mosaico está sempre na tela; quem quiser confere.
  · OBRIGATÓRIO — na manhã seguinte o dia vira uma conferência PENDENTE e o
    responsável (usuário do ERP) recebe o aviso por WhatsApp, com o link. Se
    ninguém conferir até o fim do dia seguinte, abre o alerta "mosaico sem
    conferência". Conferir resolve o alerta.

CONFERIR PODE MARCAR FOTO SUSPEITA: a batida volta para ANÁLISE, com o motivo,
e entra na fila de quem trata o ponto. A batida não muda de hora nem some — a
decisão fica em `marcacao_decisoes`, como qualquer tratamento.

Quem confere: quem trata o ponto da obra (`tratar_ponto`, no recorte de obras
dele). O responsável escolhido precisa ter essa permissão — a tela avisa se
não tiver.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import competencias, envios, fotos

logger = logging.getLogger("ponto.mosaico")

LIMIAR_ESCURA = 30          # brilho médio abaixo disto: câmera tampada ou no escuro
LIMIAR_LISA = 10            # contraste abaixo disto: imagem lisa (parede, dedo)
LIMIAR_REPETIDA = 3         # bits de diferença na impressão: a mesma foto de novo


# ---------------------------------------------------------------------------
# Regras puras
# ---------------------------------------------------------------------------
def sinais_da_foto(luminancia: Optional[int], contraste: Optional[int]) -> list[str]:
    s = []
    if luminancia is not None and luminancia < LIMIAR_ESCURA:
        s.append("escura")
    if contraste is not None and contraste < LIMIAR_LISA:
        s.append("sem rosto visível")
    return s


# ---------------------------------------------------------------------------
# Configuração da obra
# ---------------------------------------------------------------------------
def configuracao_das_obras(conn: Connection, obras: Optional[list[int]] = None) -> list[dict]:
    sql = """
        SELECT o.id, o.codigo, o.nome, COALESCE(oc.mosaico_obrigatorio, FALSE) AS obrigatorio,
               oc.mosaico_responsavel_id AS responsavel_id, u.nome AS responsavel,
               u.telefone AS responsavel_telefone
          FROM public.obras o
          LEFT JOIN ponto.obra_config oc ON oc.obra_id = o.id
          LEFT JOIN public.usuarios u ON u.id = oc.mosaico_responsavel_id
         WHERE o.status = 'ATIVA'"""
    params: dict = {}
    if obras is not None:
        sql += " AND o.id = ANY(:obras)"
        params["obras"] = list(obras) or [0]
    linhas = db.todos(conn, sql + " ORDER BY o.codigo", **params)
    for l in linhas:
        l["responsavel_tem_telefone"] = bool(envios.telefone_valido(l.pop("responsavel_telefone")))
    return linhas


def configurar_obra(conn: Connection, obra_id: int, *, obrigatorio: bool,
                    responsavel_id: Optional[int]) -> dict:
    if obrigatorio and not responsavel_id:
        raise ErroDeValidacao("mosaico obrigatório precisa de um responsável", campo="responsavel_id")
    if responsavel_id:
        u = db.um(conn, "SELECT id, ativo FROM public.usuarios WHERE id = :id", id=responsavel_id)
        if not u or u.get("ativo") is False:
            raise ErroDeValidacao("usuário do ERP não encontrado ou inativo", campo="responsavel_id")
    db.executar(conn, """
        INSERT INTO ponto.obra_config (obra_id, mosaico_obrigatorio, mosaico_responsavel_id)
        VALUES (:o, :ob, :r)
        ON CONFLICT (obra_id) DO UPDATE SET mosaico_obrigatorio = :ob, mosaico_responsavel_id = :r,
               atualizado_em = now()
    """, o=obra_id, ob=bool(obrigatorio), r=responsavel_id)
    return next(c for c in configuracao_das_obras(conn, [obra_id]) if c["id"] == obra_id)


# ---------------------------------------------------------------------------
# Montar
# ---------------------------------------------------------------------------
def montar(conn: Connection, obra_id: int, data: dt.date) -> dict:
    """O mosaico de uma obra num dia: uma linha por pessoa que bateu lá."""
    batidas = db.todos(conn, """
        SELECT m.id, m.nsr, m.colaborador_id, m.timestamp_servidor, m.status, m.motivo_analise,
               m.foto_id, m.origem, m.identificacao, m.dispositivo_id,
               c.nome, f.nome AS funcao, pc.foto_cadastral_id,
               fo.luminancia, fo.contraste, fo.dhash
          FROM ponto.marcacoes m
          JOIN public.colaboradores c ON c.id = m.colaborador_id
          LEFT JOIN public.funcoes f ON f.id = c.funcao_id
          LEFT JOIN ponto.colaborador_config pc ON pc.colaborador_id = c.id
          LEFT JOIN ponto.fotos fo ON fo.id = m.foto_id
         WHERE m.obra_id = :o AND m.data_referencia = :d AND m.status <> 'REJEITADA'
           AND m.origem <> 'MANUAL'
         ORDER BY c.nome, m.colaborador_id, m.timestamp_servidor
    """, o=obra_id, d=data)
    pessoas: dict[int, dict] = {}
    for b in batidas:
        p = pessoas.setdefault(b["colaborador_id"], {
            "colaborador_id": b["colaborador_id"], "nome": b["nome"], "funcao": b["funcao"],
            "tem_foto_cadastral": b["foto_cadastral_id"] is not None, "batidas": []})
        p["batidas"].append({
            "id": b["id"], "nsr": b["nsr"],
            "hora": horario.para_local(b["timestamp_servidor"]).strftime("%H:%M"),
            "status": b["status"], "motivo_analise": b["motivo_analise"],
            "tem_foto": b["foto_id"] is not None, "identificacao": b["identificacao"],
            "sinais": sinais_da_foto(b["luminancia"], b["contraste"]),
        })
    linha = db.um(conn, "SELECT * FROM ponto.mosaicos WHERE obra_id = :o AND data = :d",
                  o=obra_id, d=data)
    obra = db.um(conn, """SELECT o.id, o.codigo, o.nome, COALESCE(oc.mosaico_obrigatorio, FALSE) AS obrigatorio
                            FROM public.obras o LEFT JOIN ponto.obra_config oc ON oc.obra_id = o.id
                           WHERE o.id = :o""", o=obra_id)
    lista = list(pessoas.values())
    sem_foto = sum(1 for p in lista for b in p["batidas"] if not b["tem_foto"])
    return {
        "obra": obra, "data": data.isoformat(), "pessoas": lista,
        "totais": {"pessoas": len(lista), "batidas": len(batidas), "sem_foto": sem_foto,
                   "com_sinal": sum(1 for p in lista for b in p["batidas"] if b["sinais"])},
        "conferencia": (None if not linha else {
            "situacao": linha["situacao"], "obrigatorio": linha["obrigatorio"],
            "conferido_por": linha["conferido_por"], "conferido_em": horario.texto(linha["conferido_em"]),
            "suspeitas": linha["suspeitas"], "nota": linha["nota"],
            "avisado_em": horario.texto(linha["avisado_em"])}),
    }


def pendentes(conn: Connection, obras: Optional[list[int]] = None) -> list[dict]:
    sql = """SELECT mo.obra_id, mo.data, mo.batidas, mo.avisado_em, o.codigo, o.nome
               FROM ponto.mosaicos mo JOIN public.obras o ON o.id = mo.obra_id
              WHERE mo.situacao = 'PENDENTE'"""
    params: dict = {}
    if obras is not None:
        sql += " AND mo.obra_id = ANY(:obras)"
        params["obras"] = list(obras) or [0]
    return [{**l, "data": l["data"].isoformat(), "avisado_em": horario.texto(l["avisado_em"])}
            for l in db.todos(conn, sql + " ORDER BY mo.data, o.codigo LIMIT 200", **params)]


# ---------------------------------------------------------------------------
# Conferir
# ---------------------------------------------------------------------------
def conferir(conn: Connection, obra_id: int, data: dt.date, *, usuario_id: Optional[int],
             usuario_nome: str, nota: str = "", suspeitas: Optional[list[dict]] = None) -> dict:
    """Confere o dia. Cada suspeita ({marcacao_id, motivo}) manda a batida para
    análise. Mês fechado não aceita — a mesma regra de qualquer tratamento."""
    competencias.exigir_aberta(conn, data)
    marcadas = 0
    for s in suspeitas or []:
        try:
            mid = int(s.get("marcacao_id"))
        except (TypeError, ValueError):
            raise ErroDeValidacao("batida suspeita sem número", campo="suspeitas")
        motivo = str(s.get("motivo") or "").strip()
        if len(motivo) < 5:
            raise ErroDeValidacao("diga o que a foto tem de estranho (ao menos 5 letras)",
                                  campo="suspeitas")
        m = db.um(conn, "SELECT id, nsr, status, obra_id, data_referencia FROM ponto.marcacoes "
                        "WHERE id = :id", id=mid)
        if not m or m["obra_id"] != obra_id or m["data_referencia"] != data:
            raise NaoEncontrado("batida não encontrada neste mosaico")
        if m["status"] != "VALIDA":
            continue          # já em análise, ajustada ou rejeitada: nada a mudar
        texto = f"foto suspeita no mosaico: {motivo}"[:500]
        db.executar(conn, """
            UPDATE ponto.marcacoes SET status = 'EM_ANALISE',
                   motivo_analise = CASE WHEN motivo_analise IS NULL OR motivo_analise = '' THEN :t
                                         ELSE motivo_analise || '; ' || :t END
             WHERE id = :id AND status = 'VALIDA'
        """, t=texto, id=mid)
        db.executar(conn, """
            INSERT INTO ponto.marcacao_decisoes (marcacao_id, de_status, para_status, motivo,
                                                 usuario_id, usuario_nome)
            VALUES (:m, 'VALIDA', 'EM_ANALISE', :mot, :u, :n)
        """, m=mid, mot=texto, u=usuario_id, n=usuario_nome[:120])
        marcadas += 1
        logger.info("Ponto: NSR %s voltou para análise pelo mosaico (%s)", m["nsr"], usuario_nome)
    n_batidas = db.um(conn, "SELECT count(*) AS n FROM ponto.marcacoes WHERE obra_id = :o "
                            "AND data_referencia = :d AND status <> 'REJEITADA'", o=obra_id, d=data)["n"]
    db.executar(conn, """
        INSERT INTO ponto.mosaicos (obra_id, data, situacao, batidas, suspeitas, conferido_por,
                                    conferido_usuario_id, conferido_em, nota)
        VALUES (:o, :d, 'CONFERIDO', :b, :s, :por, :u, now(), :nota)
        ON CONFLICT (obra_id, data) DO UPDATE SET situacao = 'CONFERIDO', batidas = :b,
               suspeitas = ponto.mosaicos.suspeitas + :s, conferido_por = :por,
               conferido_usuario_id = :u, conferido_em = now(), nota = :nota
    """, o=obra_id, d=data, b=n_batidas, s=marcadas, por=usuario_nome[:120], u=usuario_id,
         nota=(nota or "").strip()[:500] or None)
    db.executar(conn, """
        UPDATE ponto.alertas SET situacao = 'RESOLVIDO', tratado_por = :p, tratado_em = now(),
               nota = 'mosaico conferido'
         WHERE chave = :k AND situacao = 'ABERTO'
    """, p=usuario_nome[:120], k=chave_do_alerta(obra_id, data))
    logger.info("Ponto: mosaico da obra %s em %s conferido por %s (%d suspeita(s))",
                obra_id, data, usuario_nome, marcadas)
    return montar(conn, obra_id, data)


def chave_do_alerta(obra_id: int, data: dt.date) -> str:
    return f"MOSAICO_PENDENTE:{obra_id}:{data.isoformat()}"


# ---------------------------------------------------------------------------
# A rotina da manhã: abrir as conferências obrigatórias de ontem e avisar
# ---------------------------------------------------------------------------
def preparar_do_dia(conn: Connection, data: dt.date) -> dict:
    """Para cada obra OBRIGATÓRIA que teve batida em `data`: abre a conferência
    PENDENTE e põe na fila o aviso ao responsável."""
    obras = db.todos(conn, """
        SELECT o.id, o.codigo, o.nome, oc.mosaico_responsavel_id, u.nome AS responsavel,
               u.telefone, (SELECT count(*) FROM ponto.marcacoes m
                             WHERE m.obra_id = o.id AND m.data_referencia = :d
                               AND m.status <> 'REJEITADA' AND m.origem <> 'MANUAL') AS batidas
          FROM ponto.obra_config oc JOIN public.obras o ON o.id = oc.obra_id
          LEFT JOIN public.usuarios u ON u.id = oc.mosaico_responsavel_id
         WHERE oc.mosaico_obrigatorio AND o.status = 'ATIVA'
    """, d=data)
    abertas, avisos = 0, 0
    base = envios.endereco_publico(conn)
    for o in obras:
        if not o["batidas"]:
            continue
        nova = db.um(conn, """
            INSERT INTO ponto.mosaicos (obra_id, data, obrigatorio, responsavel_id, batidas)
            VALUES (:o, :d, TRUE, :r, :b)
            ON CONFLICT (obra_id, data) DO NOTHING RETURNING id
        """, o=o["id"], d=data, r=o["mosaico_responsavel_id"], b=o["batidas"])
        if not nova:
            continue
        abertas += 1
        telefone = envios.telefone_valido(o["telefone"])
        if not telefone:
            continue
        link = f"{base}/erp/ponto/mosaico?obra={o['id']}&data={data.isoformat()}"
        primeiro = (o["responsavel"] or "").split(" ")[0]
        texto = (f"BWS Ponto — {primeiro}, o mosaico de fotos da obra {o['codigo']} de "
                 f"{data:%d/%m} está esperando a sua conferência ({o['batidas']} batidas).\n\n"
                 f"Confira os rostos e confirme: {link}")
        if envios.enfileirar(conn, tipo="MOSAICO", referencia=f"MOSAICO:{o['id']}:{data.isoformat()}",
                             telefone=telefone, texto=texto, usuario_id=o["mosaico_responsavel_id"],
                             pedido_por="rotina"):
            db.executar(conn, "UPDATE ponto.mosaicos SET avisado_em = now() WHERE id = :id",
                        id=nova["id"])
            avisos += 1
    return {"abertas": abertas, "avisos": avisos}


def definir_foto_cadastral(conn: Connection, colaborador_id: int, marcacao_id: int) -> None:
    """A foto de referência da pessoa (a 1ª coluna do mosaico) passa a ser a de
    uma batida DELA — escolhida por quem conferiu que o rosto é o certo. Sem
    upload à parte: a foto já está no Drive."""
    m = db.um(conn, "SELECT colaborador_id, foto_id FROM ponto.marcacoes WHERE id = :id", id=marcacao_id)
    if not m or m["colaborador_id"] != colaborador_id:
        raise NaoEncontrado("batida não encontrada")
    if not m["foto_id"]:
        raise ErroDeValidacao("esta batida não tem foto", campo="marcacao_id")
    db.executar(conn, """
        INSERT INTO ponto.colaborador_config (colaborador_id, foto_cadastral_id) VALUES (:c, :f)
        ON CONFLICT (colaborador_id) DO UPDATE SET foto_cadastral_id = :f, atualizado_em = now()
    """, c=colaborador_id, f=m["foto_id"])


def foto_cadastral(conn: Connection, colaborador_id: int) -> bytes:
    p = db.um(conn, "SELECT foto_cadastral_id FROM ponto.colaborador_config WHERE colaborador_id = :c",
              c=colaborador_id)
    if not p or not p["foto_cadastral_id"]:
        raise LookupError("sem foto de cadastro")
    return fotos.miniatura(conn, int(p["foto_cadastral_id"]))
