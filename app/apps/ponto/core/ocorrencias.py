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
import re
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import ajustes, banco, cadastros, competencias, documentos, escalas, licencas, marcacoes, validacao

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
# A DECLARAÇÃO DE COMPARECIMENTO (27/10/2026, o dono: "a pessoa vai ao hospital, faz uma
# consulta rápida, não é dado atestado, entrega a declaração e falta o resto do dia (…) o
# DP precisa analisar e decidir se vai abonar a totalidade das horas, só o período, ou
# não vai abonar nada"). É um ATESTADO com subtipo: DECLARACAO enquanto espera; na
# aprovação final, quem aprova escolhe — e o subtipo guarda a escolha. O horário de
# início vai em `horario` e a duração do período declarado, em `minutos`.
DECLARACAO = "DECLARACAO"
DECLARACAO_ABONO = {"TOTAL": "DECL_TOTAL", "PERIODO": "DECL_PERIODO", "NADA": "DECL_NADA"}
NOME_DECLARACAO = {"DECLARACAO": "Declaração de comparecimento — falta decidir o abono",
                   "DECL_TOTAL": "Declaração de comparecimento — abonado o dia todo",
                   "DECL_PERIODO": "Declaração de comparecimento — abonado só o período",
                   "DECL_NADA": "Declaração de comparecimento — não abonada"}


def e_declaracao(o: dict) -> bool:
    """PURA. O pedido é uma declaração de comparecimento (decidida ou não)?"""
    return o.get("tipo") == "ATESTADO" and str(o.get("subtipo") or "") in NOME_DECLARACAO


def _periodo_declarado(dados: dict, dia: dt.date) -> tuple[dt.datetime, int]:
    """O início e a duração (minutos) do período escrito na declaração."""
    def hm(chave, rotulo):
        try:
            h, m = (int(x) for x in str(dados.get(chave) or "").strip().split(":")[:2])
            assert 0 <= h < 24 and 0 <= m < 60
            return h * 60 + m
        except (ValueError, AssertionError):
            raise ErroDeValidacao(f"diga {rotulo} da declaração (HH:MM)", campo=chave)
    ini, fim = hm("hora_inicio", "a hora de início"), hm("hora_fim", "a hora de fim")
    if fim <= ini:
        raise ErroDeValidacao("a hora de fim da declaração é antes da de início", campo="hora_fim")
    inicio = dt.datetime.combine(dia, dt.time(ini // 60, ini % 60), tzinfo=horario.FUSO)
    return inicio, fim - ini
# O que se pede pelo aplicativo (o próprio celular, o ponto da obra, o responsável). Férias e afastamento
# nascem no DP; o abono, desde 29/10/2026, é pedido como os outros (o dono: "não é pra ser só o DP. É pra ser
# pros mesmos que podem fazer requisição") — e vai para a validação.
PEDIDOS_DO_APP = ("ATESTADO", "AJUSTE_BATIDA", "COMPENSACAO", "FOLGA_BANCO", "LICENCA", "ABONO")
MAX_DIAS_DE_AJUSTE = 31    # dias diferentes num pedido de ajuste só
RECUSA_PEDIDO_NO_CELULAR = ("atestado, licença, ajuste e compensação são entregues no ponto da obra "
                            "ou com o administrativo da obra")
# Onde o pedido nasce fora do ERP: o próprio celular (APP), o ponto da obra com a
# pessoa identificada (APARELHO), e o responsável ou administrativo da obra
# pedindo POR OUTRA PESSOA no aplicativo (RESPONSAVEL, migração 008).
ORIGENS_DO_APLICATIVO = ("APP", "APARELHO", "RESPONSAVEL")
# O ajuste que a gestão inclui em lote, no quadro do ponto (quadro.py).
SUBTIPO_LOTE = "LOTE_GESTAO"
MOTIVO_LOTE = "incluída em lote pela gestão"
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
    pessoas: Optional[set] = None      # alcance fechado numa lista (o administrativo no app)
    extras: dict = field(default_factory=dict)

    def alcanca_obra(self, obra_id: Optional[int]) -> bool:
        return self.obras is None or (obra_id is not None and int(obra_id) in self.obras)


def _data(valor, campo: str) -> dt.date:
    try:
        return valor if isinstance(valor, dt.date) else dt.date.fromisoformat(str(valor))
    except ValueError as e:
        raise ErroDeValidacao(f"{campo} ilegível (AAAA-MM-DD)", campo=campo) from e


def pessoa_no_alcance(conn: Connection, quem: Quem, colaborador_id: int) -> bool:
    if quem.pessoas is not None:
        return int(colaborador_id) in quem.pessoas
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
    # Vários dias de uma vez (12/10/2026: "corrigir o ponto do dia 5 ao dia 9"): o
    # limite é por DIA de trabalho, e o pedido inteiro cabe num mês.
    por_dia: dict[str, int] = {}
    for h in horarios:
        chave = str(h)[:10]
        por_dia[chave] = por_dia.get(chave, 0) + 1
    if len(por_dia) > MAX_DIAS_DE_AJUSTE:
        raise ErroDeValidacao(f"no máximo {MAX_DIAS_DE_AJUSTE} dias num pedido só", campo="horarios")
    if max(por_dia.values()) > ajustes.MAX_SEM_ESCALA:
        raise ErroDeValidacao("horários demais num dia só", campo="horarios")
    varios_dias = len(por_dia) > 1
    criados = []
    for h in sorted(str(x) for x in horarios):
        try:
            criados.append(criar(conn, quem, {**dados, "tipo": "AJUSTE_BATIDA", "horario": h,
                                              "data_inicio": str(h)[:10]}, origem=origem))
        except ErroDeValidacao as e:
            if not varios_dias:
                raise
            # Em vários dias, a frase diz DE QUAL dia — e nada entra (mesma transação).
            dia = f"{h[8:10]}/{h[5:7]}"
            raise ErroDeValidacao(f"{dia}: {e.mensagem}", **e.detalhes) from e
    return criados


def criar(conn: Connection, quem: Quem, dados: dict, *, origem: str = "GESTAO") -> dict:
    tipo = str(dados.get("tipo") or "").strip().upper()
    if tipo == DECLARACAO:                  # a declaração de comparecimento é um atestado com subtipo
        dados = {**dados, "tipo": "ATESTADO", "subtipo": DECLARACAO}
        tipo = "ATESTADO"
    if tipo not in ETAPAS:
        raise ErroDeValidacao("tipo de pedido desconhecido", campo="tipo")
    if origem in ORIGENS_DO_APLICATIVO and tipo not in PEDIDOS_DO_APP:
        raise ErroDeValidacao("este pedido é feito pelo DP, não pelo celular", campo="tipo")
    colaborador_id = int(dados.get("colaborador_id") or quem.colaborador_id or 0)
    pessoa = exigir_pessoa_no_alcance(conn, quem, colaborador_id)
    # QUEM FAZ PEDIDO PELO PRÓPRIO CELULAR (decisão do dono, 08/10/2026): só quem
    # tem a marcação "faz pedidos pelo celular", ou é administrativo de obra
    # (papeis.py). Os outros entregam no ponto da obra ou com o administrativo.
    if origem == "APP":
        from . import papeis
        if not papeis.pede_pelo_proprio_celular(conn, pessoa):
            raise ErroDeValidacao(RECUSA_PEDIDO_NO_CELULAR, campo="tipo")
    if origem == "GESTAO" and not (quem.supervisor or quem.dp):
        raise ErroDeValidacao("você não pode registrar pedidos de ponto", campo="tipo")
    # O responsável da obra registra atestado, licença e abono e quem valida decide; férias e
    # afastamento continuam só do DP.
    if origem == "GESTAO" and tipo in ("FERIAS", "AFASTAMENTO") and not quem.dp:
        raise ErroDeValidacao("este tipo é registrado pelo DP", campo="tipo")
    if origem == "GESTAO" and tipo == "ATESTADO" and not quem.dp and not dados.get("documento_base64"):
        raise ErroDeValidacao("anexe a foto ou o PDF do atestado", campo="documento")

    inicio = _data(dados.get("data_inicio") or dados.get("data"), "data_inicio")
    fim = _data(dados.get("data_fim") or inicio, "data_fim")
    if fim < inicio:
        raise ErroDeValidacao("a data final é antes da inicial", campo="data_fim")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"no máximo {MAX_DIAS} dias por pedido", campo="data_fim")
    # O ponto vale a partir de 16/10/2026 (vigencia.py): pedido inteiro antes disso não entra.
    from . import vigencia
    vale = vigencia.inicio(conn)
    if fim < vale:
        raise ErroDeValidacao(f"o ponto vale a partir de {vale:%d/%m/%Y} — antes disso, nada se lança",
                              campo="data_inicio")
    descricao = str(dados.get("descricao") or "").strip()
    horario_ajuste, obra_id, dia_trabalhado, minutos = None, None, None, None
    # O abono pedido (não lançado pelo DP) diz o porquê: quem valida decide por ele.
    if tipo == "ABONO" and not (origem == "GESTAO" and quem.dp) and len(descricao) < 10:
        raise ErroDeValidacao("conte o motivo do abono (ao menos 10 letras)", campo="descricao")

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
        # QUALQUER obra cadastrada (decisão do dono, 16/10/2026: "não é para ser a obra
        # do cadastro, é para aparecer a relação de obras cadastradas"): a pessoa pode
        # ter trabalhado em outra obra naquele dia, e quem valida o pedido confere.
        # Antes, só as obras do cadastro dela (ou a do alcance de quem pedia).
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
    elif tipo == "ATESTADO" and origem in ORIGENS_DO_APLICATIVO and not dados.get("documento_base64"):
        raise ErroDeValidacao("anexe a foto ou o PDF do atestado", campo="documento")
    if tipo == "ATESTADO" and str(dados.get("subtipo") or "").upper() == DECLARACAO:
        fim = inicio                         # a declaração é de um dia, com o horário
        horario_ajuste, minutos = _periodo_declarado(dados, inicio)
    from .espelho import SUBTIPOS_ABONO
    subtipo = DECLARACAO if tipo == "ATESTADO" and str(dados.get("subtipo") or "").upper() == DECLARACAO else None
    if tipo == "LICENCA" and licencas.disponivel(conn):
        # A licença sai da lista da lei (licencas.py): dias, limite e documento conferidos.
        t = licencas.conferir(conn, colaborador_id=colaborador_id, subtipo=str(dados.get("subtipo") or ""),
                              inicio=inicio, fim=fim, tem_documento=bool(dados.get("documento_base64")))
        subtipo = t["codigo"]
        if not descricao:
            descricao = t["nome"]
    elif tipo == "ABONO" and str(dados.get("subtipo") or "").strip():
        # o abono com nome próprio: viagem a serviço, dispensa… — os da Configuração (tipos_ausencia.py)
        from . import tipos_ausencia
        tipos_ausencia.recarregar(conn)
        subtipo = str(dados["subtipo"]).strip().upper()
        if not tipos_ausencia.abono_valido(conn, subtipo):
            raise ErroDeValidacao("escolha o abono na lista", campo="subtipo")
        if not descricao:
            descricao = SUBTIPOS_ABONO.get(subtipo, subtipo)
    elif tipo == "AJUSTE_BATIDA" and dados.get("lote") and origem == "GESTAO":
        # Incluída pela gestão no quadro do ponto (21/10/2026) — falha do sistema, não
        # da pessoa: não conta contra ela na qualidade (qualidade.py).
        subtipo = SUBTIPO_LOTE

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
         cid=(str(dados.get("cid") or "").strip()[:20] or None) if quem.dp or origem in ORIGENS_DO_APLICATIVO else None,
         med=(str(dados.get("medico") or "").strip()[:120] or None),
         crm=(str(dados.get("crm") or "").strip()[:30] or None),
         doc=documento_id, st=STATUS_DA_ETAPA[etapas[0]], orig=origem,
         por=quem.nome[:120], uid=quem.usuario_id)
    ocorrencia_id = int(linha["id"])
    if subtipo:
        db.executar(conn, "UPDATE ponto.ocorrencias SET subtipo = :s WHERE id = :id", s=subtipo, id=ocorrencia_id)
    # QUEM PEDIU, como pessoa (migração 014): a própria pessoa (celular ou aparelho da obra) ou
    # quem pediu por ela no aplicativo — é a quem a negativa também é avisada.
    solicitante = quem.colaborador_id or quem.extras.get("solicitante_id")
    if solicitante and db.tem_coluna(conn, "ocorrencias", "solicitado_colaborador_id"):
        db.executar(conn, "UPDATE ponto.ocorrencias SET solicitado_colaborador_id = :s WHERE id = :id",
                    s=int(solicitante), id=ocorrencia_id)
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
            decidir(conn, ocorrencia_id, quem, aprovar=True, motivo="", abono=dados.get("abono"))
    return obter(conn, ocorrencia_id, quem)


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
_SQL = """
    SELECT oc.*, c.nome AS colaborador_nome, c.cpf, c.obra_id AS obra_principal_id,
           o.codigo AS obra_codigo, o.nome AS obra_nome, op.codigo AS obra_principal_codigo
      FROM ponto.ocorrencias oc
      JOIN public.colaboradores c ON c.id = oc.colaborador_id
      LEFT JOIN public.obras o ON o.id = oc.obra_id
      LEFT JOIN public.obras op ON op.id = c.obra_id
"""


# Os nomes das licenças da lei, para o rótulo do pedido (lidos uma vez).
_NOME_SUBTIPO: dict = {}


def _nomes_das_licencas(conn: Connection) -> None:
    if not _NOME_SUBTIPO and licencas.disponivel(conn):
        _NOME_SUBTIPO.update({t["codigo"]: t["nome"] for t in licencas.listar(conn, so_ativos=False)})


def _para_json(o: dict, quem: Quem, etapas: tuple | None = None) -> dict:
    sigilo = o["tipo"] in SAUDE and not quem.dp and quem.colaborador_id != o["colaborador_id"]
    from .espelho import ROTULO_TIPO, SUBTIPOS_ABONO
    return {
        "id": o["id"], "tipo": o["tipo"],
        "rotulo": SUBTIPOS_ABONO[o["subtipo"]] if o["tipo"] == "ABONO" and o.get("subtipo") in SUBTIPOS_ABONO else
                  NOME_DECLARACAO[o["subtipo"]] if e_declaracao(o) else
                  ROTULO_TIPO[o["tipo"]] + (f" — {MOTIVO_LOTE}" if o.get("subtipo") == SUBTIPO_LOTE else
                                            f" — {_NOME_SUBTIPO.get(o['subtipo'], o['subtipo']).split(' (')[0].lower()}"
                                             if o.get("subtipo") else ""),
        "subtipo": o.get("subtipo"),
        "colaborador_id": o["colaborador_id"], "colaborador": o["colaborador_nome"],
        "cpf": o["cpf"], "obra": o.get("obra_codigo") or o.get("obra_principal_codigo"),
        # A obra escolhida no pedido, à parte da do cadastro (28/10/2026, o dono: "um ajuste de batida
        # esperando validação não aparece a obra que foi selecionada").
        "obra_pedida": ({"codigo": o.get("obra_codigo"), "nome": o.get("obra_nome")} if o.get("obra_codigo") else None),
        "obra_cadastro": o.get("obra_principal_codigo"),
        "data_inicio": o["data_inicio"].isoformat(), "data_fim": o["data_fim"].isoformat(),
        "dias": (o["data_fim"] - o["data_inicio"]).days + 1,
        "horario": horario.texto(o.get("horario")),
        "dia_trabalhado": o["dia_trabalhado"].isoformat() if o.get("dia_trabalhado") else None,
        "minutos": o.get("minutos"), "declaracao": e_declaracao(o),
        "declaracao_periodo": (f"{horario.para_local(o['horario']):%H:%M} às "
                               f"{horario.para_local(o['horario']) + dt.timedelta(minutes=int(o['minutos'] or 0)):%H:%M}"
                               if e_declaracao(o) and o.get("horario") and o.get("minutos") else None),
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
    _nomes_das_licencas(conn)
    j = _para_json(o, quem, validacao.etapas_do_tipo(conn, o["tipo"]))
    j["pode_decidir"] = (j["etapa_atual"] is not None
                         and _pode_na_etapa(conn, quem, o, j["etapa_atual"]))
    j["alteracoes"] = alteracoes(conn, ocorrencia_id)
    # o DP corrige a obra do ajuste que ainda espera decisão (29/10/2026, migração 016)
    j["pode_trocar_obra"] = bool(quem.dp and o["tipo"] == "AJUSTE_BATIDA" and j["etapa_atual"] is not None
                                 and db.tem_coluna(conn, "ocorrencia_alteracoes", "campo"))
    return j


def alteracoes(conn: Connection, ocorrencia_id: int) -> list[dict]:
    """O que o DP mudou no pedido antes de decidir (migração 016), do mais antigo ao mais novo."""
    if not db.tem_coluna(conn, "ocorrencia_alteracoes", "campo"):
        return []
    return [{"campo": a["campo"], "de": a["de"], "para": a["para"], "motivo": a["motivo"],
             "por": a["usuario_nome"], "em": horario.texto(a["criado_em"])}
            for a in db.todos(conn, "SELECT * FROM ponto.ocorrencia_alteracoes WHERE ocorrencia_id = :id "
                                    "ORDER BY id", id=ocorrencia_id)]


MOTIVO_MINIMO_TROCA = 10


def trocar_obra(conn: Connection, ocorrencia_id: int, quem: Quem, *, obra: str, motivo: str) -> dict:
    """A PESSOA ESCOLHEU A OBRA ERRADA NO AJUSTE (29/10/2026). O DP corrige ANTES de decidir: o pedido
    ainda não é batida, e a Portaria 671 protege as marcações, não o pedido. O que era, o que ficou,
    quem, quando e por quê ficam em `ocorrencia_alteracoes`, para sempre. Depois de aprovado, a batida
    já existe: vale "Corrigir a obra ou a hora" da batida (marcacoes.corrigir)."""
    if not db.tem_coluna(conn, "ocorrencia_alteracoes", "campo"):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 016) para corrigir a obra do pedido")
    if not quem.dp:
        raise ErroDeValidacao("só o DP corrige a obra de um pedido", campo="obra")
    o = _bruta(conn, ocorrencia_id)
    if not pessoa_no_alcance(conn, quem, o["colaborador_id"]):
        raise NaoEncontrado("pedido não encontrado")
    if o["tipo"] != "AJUSTE_BATIDA":
        raise ErroDeValidacao("só o ajuste de batida tem obra para corrigir", campo="obra")
    if _etapa_atual(o) is None:
        raise ErroDeValidacao(f"este pedido já está {o['status'].lower()} — corrija a batida, não o pedido",
                              campo="status")
    motivo = " ".join(str(motivo or "").split())
    if len(motivo) < MOTIVO_MINIMO_TROCA:
        raise ErroDeValidacao("diga por que a obra está sendo corrigida (ao menos 10 letras)", campo="motivo")
    nova = cadastros.obra_por_codigo(conn, str(obra or ""))
    if not nova or nova["status"] != "ATIVA":
        raise ErroDeValidacao("obra não encontrada ou inativa no ponto", campo="obra")
    if not quem.alcanca_obra(int(nova["id"])):
        raise NaoEncontrado("obra não encontrada")
    if o.get("obra_id") and int(o["obra_id"]) == int(nova["id"]):
        raise ErroDeValidacao("o pedido já está nesta obra", campo="obra")
    competencias.exigir_aberta(conn, o["data_inicio"], o["data_fim"], o.get("dia_trabalhado"))
    de = o.get("obra_codigo") or "(sem obra)"
    db.executar(conn, "UPDATE ponto.ocorrencias SET obra_id = :o, atualizado_em = now() WHERE id = :id",
                o=int(nova["id"]), id=ocorrencia_id)
    db.executar(conn, """
        INSERT INTO ponto.ocorrencia_alteracoes (ocorrencia_id, campo, de, para, motivo, usuario_id, usuario_nome)
        VALUES (:id, 'obra', :de, :para, :m, :u, :n)""", id=ocorrencia_id, de=de, para=nova["codigo"],
                 m=motivo[:500], u=quem.usuario_id, n=quem.nome[:120])
    logger.info("Ponto: pedido %d — obra corrigida de %s para %s por %s", ocorrencia_id, de, nova["codigo"], quem.nome)
    return obter(conn, ocorrencia_id, quem)


def listar(conn: Connection, quem: Quem, *, status: str | None = None,
           colaborador_id: int | None = None, limite: int = 300) -> list[dict]:
    _nomes_das_licencas(conn)
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
def _no_aparelho(descricao: str, quem: str) -> str:
    """PURA. "no aparelho …", sem repetir o nome (28/10/2026, o dono: "tá em duplicidade esse texto:
    pedido por no aparelho Pedido por JORDAO (…) — lançado por JORDAO"). O aparelho que ninguém
    renomeou guarda o nome do pedido de aprovação, "Pedido por Fulano (CPF final 123)"."""
    from .dispositivos import PREFIXO_PEDIDO
    d = (descricao or "").strip()
    if d.startswith(PREFIXO_PEDIDO):
        dono = re.sub(r"\s*\(CPF final \d+\)\s*$", "", d[len(PREFIXO_PEDIDO):]).strip()
        if dono and dono.lower() == (quem or "").strip().lower():
            return "no próprio aparelho"
        return f"no aparelho de {dono}" if dono else "no aparelho"
    return f"no aparelho “{d}”" if d else "no aparelho"


def pedido_por(origem: str, solicitado_por: str, colaborador: str = "") -> dict:
    """PURA. Quem fez o pedido, para a tela (21/10/2026: "na tela de validações não
    aparece a pessoa que solicitou o ajuste ou encaminhou um atestado"). O texto
    gravado já diz por onde ("(pelo celular)", "no aparelho X — lançado por Y",
    "(responsável, pelo aplicativo)"); o do ERP ganha "pelo ERP". `propria` diz se
    foi a própria pessoa do pedido."""
    texto = (solicitado_por or "").strip()
    nome, como = texto, ""
    if origem == "APP" and texto.endswith(" (pelo celular)"):
        nome, como = texto[: -len(" (pelo celular)")], "pelo celular"
    elif origem == "APARELHO" and " — no aparelho " in texto:
        nome, _, ap = texto.partition(" — no aparelho ")
        como = _no_aparelho(ap, nome)
    elif origem == "APARELHO" and texto.startswith("no aparelho "):
        # Por outra pessoa, no ponto da obra (app_obra.py): "no aparelho X — lançado por Y".
        lugar, _, quem_lancou = texto.partition(" — lançado por ")
        nome = quem_lancou or "ponto da obra"
        como = _no_aparelho(lugar[len("no aparelho "):], nome)
    elif origem == "GESTAO":
        como = "pelo ERP"
    elif origem == "API":
        como = "por integração"
    propria = bool(colaborador) and nome.strip().lower() == colaborador.strip().lower()
    return {"nome": nome or "—", "como": como, "propria": propria, "texto": texto}


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
            motivo: str = "", abono: Optional[str] = None, avisar: bool = True) -> dict:
    """Aprova a etapa ou nega o pedido. Negado, avisa a pessoa e quem pediu por ela
    (`core/negativas.py`): no aplicativo sempre; pelo WhatsApp se `avisar` (o padrão)."""
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
        from . import negativas
        try:
            with conn.begin_nested():      # o aviso nunca desfaz a negativa
                aviso = negativas.registrar(conn, o, avisar_whatsapp=avisar, quem_negou=quem.nome)
        except Exception:  # noqa: BLE001
            logger.exception("Ponto: o aviso da negativa do pedido %d falhou", ocorrencia_id)
            aviso = {"whatsapp": [], "sem_telefone": [], "falhou": True}
        return {**obter(conn, ocorrencia_id, quem), "aviso_da_negativa": aviso}

    etapas = validacao.etapas_do_tipo(conn, o["tipo"])
    if etapa in etapas:
        seguinte = etapas[etapas.index(etapa) + 1] if etapas.index(etapa) + 1 < len(etapas) else None
    else:   # a regra mudou com o pedido na fila: depois do encarregado, só o DP, se houver
        seguinte = DP if etapa == SUPERVISOR and DP in etapas else None
    novo_status = STATUS_DA_ETAPA[seguinte] if seguinte else "APROVADA"
    if novo_status == "APROVADA" and e_declaracao(o):
        # Na aprovação final da declaração, quem aprova escolhe o abono.
        escolha = str(abono or "").strip().upper()
        if escolha not in DECLARACAO_ABONO:
            raise ErroDeValidacao("declaração de comparecimento: diga se abona o dia todo, só o período "
                                  "declarado ou nada", campo="abono")
        db.executar(conn, "UPDATE ponto.ocorrencias SET subtipo = :s WHERE id = :id",
                    s=DECLARACAO_ABONO[escolha], id=ocorrencia_id)
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
                                         ocorrencia_id=o["id"], aprovado_por=quem.nome,
                                         motivo=(f"{MOTIVO_LOTE} (pedido nº {o['id']}): {(o.get('descricao') or '')[:120]}"
                                                 if o.get("subtipo") == SUBTIPO_LOTE else None))
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
