# -*- coding: utf-8 -*-
"""
Ocorrências: atestado, licença, férias, afastamento, abono, ajuste de batida,
compensação e folga do banco de horas — e o caminho de aprovação de cada uma.

QUEM APROVA O QUÊ — desde 04/10/2026 CONFIGURÁVEL pela tela (core/validacao.py:
encarregado, DP, ou os dois), com o DP como padrão, a pedido do dono. A tabela
abaixo era a proposta de 03/10/2026, e hoje é só uma das combinações possíveis;
atestado e afastamento continuam só no DP (saúde). O PODER de cada etapa segue
configurável pessoa a pessoa no cadastro de perfis do ERP — seções "Ponto":

  tipo                          etapas                    efeito ao aprovar
  ----------------------------  ------------------------  --------------------------------
  ATESTADO, LICENCA, FERIAS,    DP                        abona os dias
  AFASTAMENTO, ABONO
  AJUSTE_BATIDA                 SUPERVISOR                inclui a batida como AJUSTADA
  COMPENSACAO                   SUPERVISOR → DP           folga abonada; o dia trabalhado
                                                          deixa de ser extra
  FOLGA_BANCO                   SUPERVISOR → DP           abona e debita o banco

"SUPERVISOR" = quem tem "tratar ponto" e enxerga a obra da pessoa. "DP" = quem
tem "aprovar afastamento". A mesma pessoa pode ter as duas, e aí aprova as duas
etapas — mas cada uma fica registrada.

SAÚDE É SIGILOSA (LGPD, art. 11; decisão do dono: "só o DP vê atestado"): CID,
médico, CRM e o documento do atestado só saem para quem aprova afastamento. O
supervisor vê "atestado" e os dias — nunca o papel.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import ajustes, banco, cadastros, competencias, documentos, escalas, marcacoes, validacao

logger = logging.getLogger("ponto.ocorrencias")

SUPERVISOR, DP = "SUPERVISOR", "DP"
# Os tipos, com as etapas do PADRÃO (tudo no DP). A regra em vigor é a da tela:
# `validacao.etapas_do_tipo`.
ETAPAS = {
    "ATESTADO": (DP,), "LICENCA": (DP,), "FERIAS": (DP,), "AFASTAMENTO": (DP,), "ABONO": (DP,),
    "AJUSTE_BATIDA": (DP,), "COMPENSACAO": (DP,), "FOLGA_BANCO": (DP,),
}
STATUS_DA_ETAPA = {SUPERVISOR: "AGUARDANDO_SUPERVISOR", DP: "AGUARDANDO_DP"}
SAUDE = ("ATESTADO", "AFASTAMENTO")
# O que o próprio colaborador pode pedir pelo celular. Férias e abono nascem no DP.
PEDIDOS_DO_APP = ("ATESTADO", "AJUSTE_BATIDA", "COMPENSACAO", "FOLGA_BANCO", "LICENCA")
MAX_DIAS = 120


@dataclass
class Quem:
    """Quem está agindo, com o que ele pode — montado pela rota, a partir do ERP."""
    nome: str
    usuario_id: Optional[int] = None
    supervisor: bool = False          # tem "tratar_ponto"
    dp: bool = False                  # tem "aprovar_afastamento"
    obras: Optional[list[int]] = None  # None = todas
    colaborador_id: Optional[int] = None  # quando é o próprio colaborador (app)
    extras: dict = field(default_factory=dict)

    def alcanca_obra(self, obra_id: Optional[int]) -> bool:
        return self.obras is None or (obra_id is not None and int(obra_id) in self.obras)


def _data(valor, campo: str) -> dt.date:
    try:
        return valor if isinstance(valor, dt.date) else dt.date.fromisoformat(str(valor))
    except ValueError as e:
        raise ErroDeValidacao(f"{campo} ilegível (AAAA-MM-DD)", campo=campo) from e


def pessoa_no_alcance(conn: Connection, quem: Quem, colaborador_id: int) -> bool:
    if quem.colaborador_id is not None:
        return quem.colaborador_id == colaborador_id
    if quem.obras is None:
        return True
    obras = cadastros.obras_da_pessoa(conn, colaborador_id)
    return bool(obras & set(quem.obras))


def exigir_pessoa_no_alcance(conn: Connection, quem: Quem, colaborador_id: int) -> dict:
    pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
    if not pessoa or not pessoa_no_alcance(conn, quem, colaborador_id):
        raise NaoEncontrado("pessoa não encontrada")
    return pessoa


# ---------------------------------------------------------------------------
# Criar
# ---------------------------------------------------------------------------
def _conferir_ajuste(conn: Connection, colaborador_id: int, dia: dt.date, momento: dt.datetime) -> None:
    """As travas do pedido de ajuste (ver ajustes.py): o dia como o espelho o
    vê, as batidas que já existem e os pedidos que esperam decisão."""
    from . import espelho
    d = espelho.montar(conn, colaborador_id, dia, dia)["dias"][0]
    existentes = [ajustes.minutos_do_dia(horario.ler_iso(b["data_hora"]), dia) for b in d["batidas"]]
    pendentes = [ajustes.minutos_do_dia(horario.para_local(o["horario"]), dia) for o in db.todos(conn, """
        SELECT horario FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND tipo = 'AJUSTE_BATIDA' AND data_inicio = :d
           AND status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP') AND horario IS NOT NULL
    """, c=colaborador_id, d=dia)]
    previstas = [{"minuto": int(p["hora"][:2]) * 60 + int(p["hora"][3:]), **p} for p in d["previstas"]]
    # turno da noite: o previsto que "volta" no relógio é da madrugada seguinte
    for i in range(1, len(previstas)):
        while previstas[i]["minuto"] < previstas[i - 1]["minuto"]:
            previstas[i]["minuto"] += 1440
    hoje = horario.hoje()
    agora_min = (ajustes.minutos_do_dia(horario.para_local(horario.agora()), dia)
                 if dia >= hoje - dt.timedelta(days=1) else None)
    ajustes.conferir_pedido(dia=d, data=dia, novos_min=[ajustes.minutos_do_dia(horario.para_local(momento), dia)],
                            existentes_min=existentes, pendentes_min=pendentes, previstas=previstas,
                            agora_min=agora_min)


def criar_ajuste_do_dia(conn: Connection, quem: Quem, dados: dict, *, origem: str = "APP") -> list[dict]:
    """Os horários que faltaram num dia, de uma vez ("o celular quebrou, fiquei
    o dia sem bater"). Um pedido por horário — cada um é decidido e vira uma
    batida —, todos na mesma transação: ou entram todos, ou nenhum."""
    horarios = dados.get("horarios") or []
    if not isinstance(horarios, list) or not horarios:
        raise ErroDeValidacao("marque ao menos um horário que faltou", campo="horarios")
    if len(horarios) > ajustes.MAX_SEM_ESCALA:
        raise ErroDeValidacao("horários demais num pedido só", campo="horarios")
    criados = []
    for h in sorted(str(x) for x in horarios):
        criados.append(criar(conn, quem, {**dados, "tipo": "AJUSTE_BATIDA", "horario": h,
                                          "data_inicio": str(h)[:10]}, origem=origem))
    return criados


def criar(conn: Connection, quem: Quem, dados: dict, *, origem: str = "GESTAO") -> dict:
    tipo = str(dados.get("tipo") or "").strip().upper()
    if tipo not in ETAPAS:
        raise ErroDeValidacao("tipo de pedido desconhecido", campo="tipo")
    if origem == "APP" and tipo not in PEDIDOS_DO_APP:
        raise ErroDeValidacao("este pedido é feito pelo DP, não pelo celular", campo="tipo")
    colaborador_id = int(dados.get("colaborador_id") or quem.colaborador_id or 0)
    pessoa = exigir_pessoa_no_alcance(conn, quem, colaborador_id)
    if origem == "GESTAO" and not (quem.supervisor or quem.dp):
        raise ErroDeValidacao("você não pode registrar pedidos de ponto", campo="tipo")
    if origem == "GESTAO" and tipo in ("ATESTADO", "LICENCA", "FERIAS", "AFASTAMENTO", "ABONO") \
            and not quem.dp:
        raise ErroDeValidacao("este tipo é registrado pelo DP", campo="tipo")

    inicio = _data(dados.get("data_inicio") or dados.get("data"), "data_inicio")
    fim = _data(dados.get("data_fim") or inicio, "data_fim")
    if fim < inicio:
        raise ErroDeValidacao("a data final é antes da inicial", campo="data_fim")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"no máximo {MAX_DIAS} dias por pedido", campo="data_fim")
    descricao = str(dados.get("descricao") or "").strip()
    horario_ajuste, obra_id, dia_trabalhado, minutos = None, None, None, None

    if tipo == "AJUSTE_BATIDA":
        fim = inicio
        try:
            horario_ajuste = horario.ler_iso(dados.get("horario"))
        except ValueError as e:
            raise ErroDeValidacao("horário da batida ilegível", campo="horario") from e
        if horario_ajuste is None:
            raise ErroDeValidacao("diga o horário da batida que faltou", campo="horario")
        if horario_ajuste > horario.agora():
            raise ErroDeValidacao("não dá para pedir ajuste de batida no futuro", campo="horario")
        obra = cadastros.resolver_obra(conn, dados.get("obra") or dados.get("obra_id")
                                       or pessoa.get("obra_id"))
        if not obra:
            raise ErroDeValidacao("diga a obra da batida", campo="obra")
        if obra["id"] not in cadastros.obras_da_pessoa(conn, colaborador_id):
            raise ErroDeValidacao("essa obra não é da pessoa", campo="obra")
        obra_id = int(obra["id"])
        motivo = str(dados.get("motivo") or "").strip().upper()
        if motivo:
            if motivo not in ajustes.MOTIVOS:
                raise ErroDeValidacao("motivo desconhecido", campo="motivo")
            if motivo == "OUTRO" and len(descricao) < 10:
                raise ErroDeValidacao("conte o que aconteceu (ao menos 10 letras)", campo="descricao")
            descricao = f"[{ajustes.MOTIVOS[motivo]}] {descricao}".strip()
        if len(descricao) < 10:
            raise ErroDeValidacao("explique o que aconteceu (ao menos 10 letras)", campo="descricao")
        inicio = fim = horario.data_referencia(horario_ajuste, pessoa["tipo_jornada"])
        _conferir_ajuste(conn, colaborador_id, inicio, horario_ajuste)
    elif tipo == "COMPENSACAO":
        dia_trabalhado = _data(dados.get("dia_trabalhado"), "dia_trabalhado")
        if dia_trabalhado == inicio:
            raise ErroDeValidacao("o dia trabalhado e o dia de folga são o mesmo", campo="dia_trabalhado")
        if len(descricao) < 5:
            raise ErroDeValidacao("descreva a compensação", campo="descricao")
    elif tipo == "FOLGA_BANCO":
        if pessoa.get("regime_banco") in (None, "SEM_BANCO"):
            raise ErroDeValidacao("esta pessoa não tem banco de horas", campo="tipo")
    elif tipo == "ATESTADO" and origem == "APP" and not dados.get("documento_base64"):
        raise ErroDeValidacao("anexe a foto ou o PDF do atestado", campo="documento")

    competencias.exigir_aberta(conn, inicio, fim, dia_trabalhado)
    sobreposta = db.um(conn, """
        SELECT id, tipo FROM ponto.ocorrencias
         WHERE colaborador_id = :c AND status NOT IN ('NEGADA', 'CANCELADA')
           AND tipo <> 'AJUSTE_BATIDA' AND :tipo <> 'AJUSTE_BATIDA'
           AND data_fim >= :i AND data_inicio <= :f LIMIT 1
    """, c=colaborador_id, tipo=tipo, i=inicio, f=fim)
    if sobreposta:
        raise ErroDeValidacao(f"já existe um pedido para esses dias (nº {sobreposta['id']})",
                              campo="data_inicio")

    documento_id = None
    if dados.get("documento_base64"):
        documento_id = documentos.guardar(
            conn, dados["documento_base64"], colaborador_id=colaborador_id, tipo_ocorrencia=tipo,
            nome_original=str(dados.get("documento_nome") or ""), sigiloso=tipo in SAUDE)

    etapas = validacao.etapas_do_tipo(conn, tipo)
    linha = db.um(conn, """
        INSERT INTO ponto.ocorrencias (colaborador_id, tipo, data_inicio, data_fim, horario,
            obra_id, dia_trabalhado, minutos, descricao, cid, medico, crm, documento_id, status,
            origem, solicitado_por, solicitado_usuario_id)
        VALUES (:c, :t, :i, :f, :h, :o, :dt, :m, :d, :cid, :med, :crm, :doc, :st, :orig, :por, :uid)
        RETURNING id
    """, c=colaborador_id, t=tipo, i=inicio, f=fim, h=horario_ajuste, o=obra_id,
         dt=dia_trabalhado, m=minutos, d=descricao[:1000],
         cid=(str(dados.get("cid") or "").strip()[:20] or None) if quem.dp or origem == "APP" else None,
         med=(str(dados.get("medico") or "").strip()[:120] or None),
         crm=(str(dados.get("crm") or "").strip()[:30] or None),
         doc=documento_id, st=STATUS_DA_ETAPA[etapas[0]], orig=origem,
         por=quem.nome[:120], uid=quem.usuario_id)
    ocorrencia_id = int(linha["id"])
    logger.info("Ponto: pedido %d (%s) aberto para colaborador %d por %s", ocorrencia_id, tipo,
                colaborador_id, quem.nome)

    # Quem registra e já pode aprovar (o DP lançando férias) não precisa de um
    # segundo clique — mas a aprovação continua sendo um passo registrado.
    if dados.get("aprovar_ja") and origem == "GESTAO":
        while True:
            atual = obter(conn, ocorrencia_id, quem)
            etapa = _etapa_atual(atual)
            if etapa is None or not _pode_na_etapa(conn, quem, atual, etapa):
                break
            decidir(conn, ocorrencia_id, quem, aprovar=True, motivo="")
    return obter(conn, ocorrencia_id, quem)


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
_SQL = """
    SELECT oc.*, c.nome AS colaborador_nome, c.cpf, c.obra_id AS obra_principal_id,
           o.codigo AS obra_codigo, op.codigo AS obra_principal_codigo
      FROM ponto.ocorrencias oc
      JOIN public.colaboradores c ON c.id = oc.colaborador_id
      LEFT JOIN public.obras o ON o.id = oc.obra_id
      LEFT JOIN public.obras op ON op.id = c.obra_id
"""


def _para_json(o: dict, quem: Quem, etapas: tuple | None = None) -> dict:
    sigilo = o["tipo"] in SAUDE and not quem.dp and quem.colaborador_id != o["colaborador_id"]
    from .espelho import ROTULO_TIPO
    return {
        "id": o["id"], "tipo": o["tipo"], "rotulo": ROTULO_TIPO[o["tipo"]],
        "colaborador_id": o["colaborador_id"], "colaborador": o["colaborador_nome"],
        "cpf": o["cpf"], "obra": o.get("obra_codigo") or o.get("obra_principal_codigo"),
        "data_inicio": o["data_inicio"].isoformat(), "data_fim": o["data_fim"].isoformat(),
        "dias": (o["data_fim"] - o["data_inicio"]).days + 1,
        "horario": horario.texto(o.get("horario")),
        "dia_trabalhado": o["dia_trabalhado"].isoformat() if o.get("dia_trabalhado") else None,
        "descricao": o["descricao"],
        "cid": None if sigilo else o.get("cid"),
        "medico": None if sigilo else o.get("medico"),
        "crm": None if sigilo else o.get("crm"),
        "tem_documento": bool(o.get("documento_id")),
        "documento_id": None if sigilo else o.get("documento_id"),
        "sigiloso": o["tipo"] in SAUDE,
        "leitura_ia": None if sigilo else o.get("leitura_ia"),
        "status": o["status"], "origem": o["origem"], "solicitado_por": o["solicitado_por"],
        "supervisor": o.get("supervisor_nome"), "supervisor_em": horario.texto(o.get("supervisor_em")),
        "dp": o.get("dp_nome"), "dp_em": horario.texto(o.get("dp_em")),
        "motivo_negativa": o.get("motivo_negativa"),
        "etapas": list(etapas or ETAPAS[o["tipo"]]), "etapa_atual": _etapa_atual(o),
        "criado_em": horario.texto(o["criado_em"]),
    }


def _bruta(conn: Connection, ocorrencia_id: int) -> dict:
    o = db.um(conn, _SQL + " WHERE oc.id = :id", id=ocorrencia_id)
    if not o:
        raise NaoEncontrado("pedido não encontrado")
    return o


def obter(conn: Connection, ocorrencia_id: int, quem: Quem) -> dict:
    o = _bruta(conn, ocorrencia_id)
    if not pessoa_no_alcance(conn, quem, o["colaborador_id"]):
        raise NaoEncontrado("pedido não encontrado")
    j = _para_json(o, quem, validacao.etapas_do_tipo(conn, o["tipo"]))
    j["pode_decidir"] = (j["etapa_atual"] is not None
                         and _pode_na_etapa(conn, quem, o, j["etapa_atual"]))
    return j


def listar(conn: Connection, quem: Quem, *, status: str | None = None,
           colaborador_id: int | None = None, limite: int = 300) -> list[dict]:
    sql, params = _SQL + " WHERE 1 = 1", {"lim": max(1, min(int(limite), 1000))}
    if status == "PENDENTES":
        sql += " AND oc.status IN ('AGUARDANDO_SUPERVISOR', 'AGUARDANDO_DP')"
    elif status:
        sql += " AND oc.status = :st"
        params["st"] = status
    if colaborador_id:
        sql += " AND oc.colaborador_id = :c"
        params["c"] = colaborador_id
    if quem.colaborador_id is not None:
        sql += " AND oc.colaborador_id = :eu"
        params["eu"] = quem.colaborador_id
    elif quem.obras is not None:
        sql += """ AND (c.obra_id = ANY(:obras) OR EXISTS (
                   SELECT 1 FROM ponto.colaborador_obras co
                    WHERE co.colaborador_id = c.id AND co.obra_id = ANY(:obras)))"""
        params["obras"] = list(quem.obras) or [0]
    sql += " ORDER BY oc.id DESC LIMIT :lim"
    saida = []
    regra = {t: validacao.etapas_do_tipo(conn, t) for t in ETAPAS}
    for o in db.todos(conn, sql, **params):
        j = _para_json(o, quem, regra[o["tipo"]])
        j["pode_decidir"] = (j["etapa_atual"] is not None
                             and _pode_na_etapa(conn, quem, o, j["etapa_atual"]))
        saida.append(j)
    return saida


# ---------------------------------------------------------------------------
# Decidir
# ---------------------------------------------------------------------------
def _etapa_atual(o: dict) -> Optional[str]:
    return {"AGUARDANDO_SUPERVISOR": SUPERVISOR, "AGUARDANDO_DP": DP}.get(o["status"])


def _pode_na_etapa(conn: Connection, quem: Quem, o: dict, etapa: str) -> bool:
    if quem.colaborador_id is not None:
        return False
    if etapa == DP:
        return quem.dp
    if not quem.supervisor:
        return False
    obra = o.get("obra_id") or o.get("obra_principal_id")
    if quem.alcanca_obra(obra):
        return True
    return pessoa_no_alcance(conn, quem, o["colaborador_id"])


def decidir(conn: Connection, ocorrencia_id: int, quem: Quem, *, aprovar: bool,
            motivo: str = "") -> dict:
    o = _bruta(conn, ocorrencia_id)
    if not pessoa_no_alcance(conn, quem, o["colaborador_id"]):
        raise NaoEncontrado("pedido não encontrado")
    etapa = _etapa_atual(o)
    if etapa is None:
        raise ErroDeValidacao(f"este pedido já está {o['status'].lower()}", campo="status")
    if not _pode_na_etapa(conn, quem, o, etapa):
        raise ErroDeValidacao("esta etapa é de " + ("quem aprova afastamento (DP)" if etapa == DP
                              else "quem trata o ponto da obra"), campo="etapa")
    competencias.exigir_aberta(conn, o["data_inicio"], o["data_fim"], o.get("dia_trabalhado"))
    campos = ("supervisor_usuario_id", "supervisor_nome", "supervisor_em") if etapa == SUPERVISOR \
        else ("dp_usuario_id", "dp_nome", "dp_em")
    if not aprovar:
        if len((motivo or "").strip()) < 5:
            raise ErroDeValidacao("diga por que o pedido foi negado", campo="motivo")
        db.executar(conn, f"""
            UPDATE ponto.ocorrencias SET status = 'NEGADA', motivo_negativa = :m,
                   {campos[0]} = :u, {campos[1]} = :n, {campos[2]} = now(), atualizado_em = now()
             WHERE id = :id
        """, m=motivo.strip()[:500], u=quem.usuario_id, n=quem.nome[:120], id=ocorrencia_id)
        logger.info("Ponto: pedido %d NEGADO por %s", ocorrencia_id, quem.nome)
        return obter(conn, ocorrencia_id, quem)

    etapas = validacao.etapas_do_tipo(conn, o["tipo"])
    if etapa in etapas:
        seguinte = etapas[etapas.index(etapa) + 1] if etapas.index(etapa) + 1 < len(etapas) else None
    else:   # a regra mudou com o pedido na fila: depois do encarregado, só o DP, se houver
        seguinte = DP if etapa == SUPERVISOR and DP in etapas else None
    novo_status = STATUS_DA_ETAPA[seguinte] if seguinte else "APROVADA"
    db.executar(conn, f"""
        UPDATE ponto.ocorrencias SET status = :s, {campos[0]} = :u, {campos[1]} = :n,
               {campos[2]} = now(), atualizado_em = now() WHERE id = :id
    """, s=novo_status, u=quem.usuario_id, n=quem.nome[:120], id=ocorrencia_id)
    if novo_status == "APROVADA":
        _aplicar(conn, _bruta(conn, ocorrencia_id), quem)
    logger.info("Ponto: pedido %d etapa %s aprovada por %s → %s", ocorrencia_id, etapa,
                quem.nome, novo_status)
    return obter(conn, ocorrencia_id, quem)


def _aplicar(conn: Connection, o: dict, quem: Quem) -> None:
    """O que muda no mundo quando o pedido termina aprovado."""
    if o["tipo"] == "AJUSTE_BATIDA":
        m = marcacoes.registrar_ajustada(conn, colaborador_id=o["colaborador_id"],
                                         obra_id=o["obra_id"], momento=o["horario"],
                                         ocorrencia_id=o["id"], aprovado_por=quem.nome)
        db.executar(conn, "UPDATE ponto.ocorrencias SET marcacao_gerada_id = :m WHERE id = :id",
                    m=m["id"], id=o["id"])
    elif o["tipo"] == "FOLGA_BANCO":
        vig = escalas.EscalasDaPessoa(conn, o["colaborador_id"])
        previsto = 0
        dia = o["data_inicio"]
        while dia <= o["data_fim"]:
            esc, _ = vig.no_dia(dia)
            if esc is not None:
                previsto += sum(s - e for e, s in esc.periodos(dia))
            dia += dt.timedelta(days=1)
        if previsto:
            banco.lancar(conn, o["colaborador_id"], data=o["data_inicio"], minutos=-previsto,
                         tipo="FOLGA", descricao=f"folga aprovada (pedido nº {o['id']})",
                         usuario_id=quem.usuario_id, usuario_nome=quem.nome,
                         ocorrencia_id=o["id"])


def cancelar(conn: Connection, ocorrencia_id: int, quem: Quem) -> dict:
    """Quem pediu desiste — enquanto ninguém decidiu a primeira etapa."""
    o = _bruta(conn, ocorrencia_id)
    if not pessoa_no_alcance(conn, quem, o["colaborador_id"]):
        raise NaoEncontrado("pedido não encontrado")
    if o["status"] not in ("AGUARDANDO_SUPERVISOR", "AGUARDANDO_DP") or \
            o.get("supervisor_em") or o.get("dp_em"):
        raise ErroDeValidacao("este pedido já foi decidido e não pode ser cancelado", campo="status")
    db.executar(conn, "UPDATE ponto.ocorrencias SET status = 'CANCELADA', atualizado_em = now() "
                      "WHERE id = :id", id=ocorrencia_id)
    return obter(conn, ocorrencia_id, quem)


def corrigir_leitura(conn: Connection, ocorrencia_id: int, quem: Quem, dados: dict) -> dict:
    """O DP confere o que a leitura do atestado preencheu (CID, médico, CRM, dias)."""
    if not quem.dp:
        raise NaoEncontrado("pedido não encontrado")
    o = _bruta(conn, ocorrencia_id)
    if o["status"] != "AGUARDANDO_DP":
        raise ErroDeValidacao("só dá para corrigir enquanto o pedido espera o DP", campo="status")
    inicio = _data(dados.get("data_inicio") or o["data_inicio"], "data_inicio")
    fim = _data(dados.get("data_fim") or o["data_fim"], "data_fim")
    if fim < inicio:
        raise ErroDeValidacao("a data final é antes da inicial", campo="data_fim")
    competencias.exigir_aberta(conn, inicio, fim)
    db.executar(conn, """
        UPDATE ponto.ocorrencias SET data_inicio = :i, data_fim = :f, cid = :cid, medico = :m,
               crm = :crm, atualizado_em = now() WHERE id = :id
    """, i=inicio, f=fim, cid=(str(dados.get("cid") or "").strip()[:20] or None),
         m=(str(dados.get("medico") or "").strip()[:120] or None),
         crm=(str(dados.get("crm") or "").strip()[:30] or None), id=ocorrencia_id)
    return obter(conn, ocorrencia_id, quem)
