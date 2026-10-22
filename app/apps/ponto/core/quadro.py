# -*- coding: utf-8 -*-
"""
O QUADRO DO PONTO — todas as pessoas, dia a dia, com as batidas e os buracos.

Pedido do dono, 21/10/2026, depois do incidente da saída (o aplicativo não
carregou e muita gente ficou sem a 4ª batida): *"para evitar uma enxurrada de
pedidos, é melhor que a gestão faça isso em lote (…) uma tela que liste as
pessoas, que eu consiga visualizar um dia ou dias, a pessoa em linha: pessoa,
dia e os pontos dela (…) ou só o dia (…) o que já é falta e o que não é falta,
quem tem presença e quem não tem (…) só tem uma batida, tem duas, só tem a
primeira (…) quem está com ponto batido 100 %, onde estão os buracos"*.

O CÁLCULO É O DO ESPELHO (`espelho.montar`, com a leitura em bloco do `Lote`):
o quadro e o espelho de cada pessoa nunca dizem coisas diferentes. Uma conta por
vez (a lição do incidente: conta pesada não roda junto com outra), e nenhuma
conta roda sozinha ao abrir a tela — só quando alguém pede.

AS VISÕES:
  · pessoa × dia — uma linha por pessoa e dia, com as batidas e o que falta;
  · resumo por dia — quantas pessoas, com batida, sem, completas, com buraco, faltas;
  · por pessoa — o período de cada um somado.
"""
from __future__ import annotations

import datetime as dt
import threading
from collections import Counter
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import cadastros, escalas, espelho, etiquetas as etiquetas_da_pessoa, filtros as filtros_do_ponto, vigencia
from .ocorrencias import Quem

MAX_DIAS = 31
MAX_LINHAS = 6000          # pessoa × dia que voltam para a tela (os números contam tudo)

# O que o dia é, para o filtro "situação do dia". A ordem é a da tela.
SITUACOES = (
    ("SEM_BATIDA", "Sem nenhuma batida"),
    ("COM_BATIDA", "Com alguma batida"),
    ("COMPLETO", "Completo (todas as batidas da escala)"),
    ("COM_BURACO", "Com batida faltando"),
    ("FALTA", "Falta (sem justificativa)"),
    ("JUSTIFICADO", "Justificado (atestado, férias, folga…)"),
    ("FORA_DO_PREVISTO", "Com batida fora do horário da escala"),
    ("NAO_TRABALHA", "Não trabalha (descanso, feriado)"),
    ("EM_ANALISE", "Com batida em conferência"),
    ("COM_PEDIDO", "Com pedido esperando decisão"),
)
ROTULO_SITUACAO = dict(SITUACOES)
SITUACOES_DO_DIA = {"OK": "Completo", "FALTA": "Falta", "INCOMPLETO": "Incompleto", "DESCANSO": "Descanso",
                    "FERIADO": "Feriado", "ABONADO": "Justificado", "COMPENSADO": "Compensado",
                    "SEM_ESCALA": "Sem escala", "EM_ANDAMENTO": "Em andamento", "FUTURO": "Ainda não chegou",
                    "FORA_DO_CONTRATO": "Fora do contrato", vigencia.SITUACAO_ANTES: "Antes do ponto"}
NAO_JULGA = ("FUTURO", "FORA_DO_CONTRATO", vigencia.SITUACAO_ANTES)

_uma_por_vez = threading.Semaphore(1)


class Ocupado(Exception):
    """Outro quadro está sendo montado agora."""


def _data(valor, campo: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(valor or "").strip()[:10])
    except ValueError as e:
        raise ErroDeValidacao(f"{campo}: use AAAA-MM-DD", campo=campo) from e


def periodo(de, ate) -> tuple[dt.date, dt.date]:
    inicio = _data(de, "de")
    fim = _data(ate, "ate") if ate else inicio
    if fim < inicio:
        raise ErroDeValidacao("o fim é antes do começo", campo="ate")
    if (fim - inicio).days + 1 > MAX_DIAS:
        raise ErroDeValidacao(f"no máximo {MAX_DIAS} dias de uma vez", campo="ate")
    if fim > horario.hoje():
        fim = horario.hoje()
        if fim < inicio:
            raise ErroDeValidacao("esse período ainda não chegou", campo="de")
    return inicio, fim


# ---------------------------------------------------------------------------
# As contas do dia (PURAS)
# ---------------------------------------------------------------------------
def _minutos_em_sequencia(horas: list[str]) -> list[int]:
    """PURA. "HH:MM" em minutos do dia de trabalho; o que "volta" no relógio é da
    madrugada seguinte (turno da noite)."""
    saida: list[int] = []
    for h in horas:
        m = int(h[:2]) * 60 + int(h[3:5])
        while saida and m < saida[-1]:
            m += 1440
        saida.append(m)
    return saida


def fora_do_previsto(previstas: list[str], batidas: list[str]) -> list[int]:
    """PURA. As posições das batidas que não casam com nenhum horário da escala
    (22/10/2026, o dono: "o cara bateu 5 horas da manhã e depois bateu 7 (…) isso
    aqui é errado"). O casamento é o mesmo que diz o que falta (ajustes.faltantes):
    cada horário previsto fica com a batida mais perto, até 2 h; a que sobra, sobra."""
    from . import ajustes
    if not previstas or not batidas:
        return []
    prev, bat = _minutos_em_sequencia(previstas), _minutos_em_sequencia(batidas)
    livres = list(range(len(bat)))
    for p in prev:
        perto = min(livres, key=lambda i: abs(bat[i] - p), default=None)
        if perto is not None and abs(bat[perto] - p) <= ajustes.CASAR_MIN:
            livres.remove(perto)
    return livres


def etiquetas_do_dia(d: dict) -> set[str]:
    """PURA. Em quais filtros de "situação do dia" este dia entra (vários)."""
    e = set()
    batidas, faltando, situacao = d.get("batidas") or [], d.get("faltando") or [], d.get("situacao")
    e.add("COM_BATIDA" if batidas else "SEM_BATIDA")
    if situacao == "FALTA":
        e.add("FALTA")
    elif situacao in ("ABONADO", "COMPENSADO") or d.get("ocorrencia"):
        e.add("JUSTIFICADO")
    elif situacao in ("DESCANSO", "FERIADO") or d.get("feriado"):
        e.add("NAO_TRABALHA")
    if batidas and (faltando or situacao == "INCOMPLETO"):   # bateu parte do dia
        e.add("COM_BURACO")
    elif batidas and situacao in ("OK", "EM_ANDAMENTO") and d.get("previstas"):
        e.add("COMPLETO")
    if d.get("em_analise"):
        e.add("EM_ANALISE")
    if d.get("pendentes"):
        e.add("COM_PEDIDO")
    if fora_do_previsto([p["hora"] for p in d.get("previstas") or [] if p.get("hora")],
                        [b["hora"] for b in batidas if b.get("hora")]):
        e.add("FORA_DO_PREVISTO")
    return e


def passa_no_filtro(d: dict, etiquetas: set[str], f: dict) -> bool:
    """PURA. O dia passa pelos filtros de dia? (situação, nº de batidas, o que falta)."""
    quer = set(f.get("situacoes") or [])
    if quer and not (quer & etiquetas):
        return False
    n = len(d.get("batidas") or [])
    quantas = {str(x) for x in (f.get("batidas") or [])}
    if quantas and not (str(min(n, 4)) in quantas):
        return False
    falta = {str(x).lower() for x in (f.get("faltando") or [])}
    # "Falta a batida X" é de quem bateu parte do dia; quem não bateu nada está em "sem batida".
    if falta and (n == 0 or not (falta & {x["rotulo"].lower() for x in d.get("faltando") or []})):
        return False
    return True


def _linha(p: dict, d: dict, etiquetas: set[str], marcas: list[dict]) -> dict:
    sobrando = set(fora_do_previsto([x["hora"] for x in d.get("previstas") or []],
                                    [b["hora"] for b in d["batidas"]]))
    return {
        "colaborador_id": int(p["id"]), "nome": p["nome"], "cpf": p.get("cpf"), "funcao": p.get("funcao"),
        "marcas": marcas,
        "obra_cadastro": p.get("obra_codigo"), "data": d["data"], "dia_semana": d["dia_semana"],
        "escala": d.get("escala"), "feriado": d.get("feriado"),
        "situacao": d["situacao"], "situacao_rotulo": SITUACOES_DO_DIA.get(d["situacao"], d["situacao"]),
        "falta": bool(d.get("falta")), "etiquetas": sorted(etiquetas),
        "batidas": [{"id": b["id"], "hora": b["hora"], "status": b["status"], "obra": b["obra"],
                     "origem": b["origem"], "sem_internet": b.get("sem_internet"), "nsr": b.get("nsr"),
                     "fora_da_cerca": b.get("fora_da_cerca"), "distancia_metros": b.get("distancia_metros"),
                     "motivo_analise": b.get("motivo_analise"), "registrado_por": b.get("registrado_por"),
                     "recebida_em": b.get("recebida_em"), "fora_do_previsto": i in sobrando}
                    for i, b in enumerate(d["batidas"])],
        "previstas": d.get("previstas") or [], "faltando": d.get("faltando") or [],
        "ocorrencia": (d.get("ocorrencia") or {}).get("rotulo"),
        "pendentes": [{"id": x["id"], "tipo": x["tipo"], "status": x["status"], "rotulo": x["rotulo"],
                       "hora": x.get("hora")} for x in d.get("pendentes") or []],
        "em_analise": d.get("em_analise") or 0,
        "obra_do_dia": next((b["obra"] for b in d["batidas"] if b.get("obra")), None) or p.get("obra_codigo"),
    }


# ---------------------------------------------------------------------------
# O quadro
# ---------------------------------------------------------------------------
def _pessoas(conn: Connection, quem: Quem, f: dict, inicio: dt.date, fim: dt.date) -> list[dict]:
    pessoas = cadastros.listar_colaboradores(conn, so_ativos=True)
    if quem.obras is not None:                  # o mesmo recorte da tela Pessoas
        alcance = set(quem.obras)
        pessoas = [p for p in pessoas if p.get("obra_id") in alcance
                   or any(o["id"] in alcance for o in p["obras_adicionais"])]
    busca = str(f.get("busca") or "").strip().lower()
    if busca:
        digitos = "".join(ch for ch in busca if ch.isdigit())
        pessoas = [p for p in pessoas if busca in (p["nome"] or "").lower()
                   or (digitos and digitos in (p.get("cpf") or ""))]
    if f.get("obra_cadastro"):
        o = int(f["obra_cadastro"])
        pessoas = [p for p in pessoas if p.get("obra_id") == o or any(x["id"] == o for x in p["obras_adicionais"])]
    if f.get("obra_batida"):                    # bateu nessa obra no período
        ids = {int(x["id"]) for x in filtros_do_ponto.pessoas_com_batida(conn, int(f["obra_batida"]), inicio, fim)}
        pessoas = [p for p in pessoas if int(p["id"]) in ids]
    funcoes = {str(x).strip().lower() for x in (f.get("funcoes") or []) if str(x).strip()}
    if funcoes:
        pessoas = [p for p in pessoas if (p.get("funcao") or "").strip().lower() in funcoes]
    if f.get("situacao_cadastro"):
        pessoas = [p for p in pessoas if p.get("situacao") == f["situacao_cadastro"]]
    return pessoas


def montar(conn: Connection, quem: Quem, de, ate, f: Optional[dict] = None, *, esperar: float = 0) -> dict:
    """O quadro do período, com os filtros `f`. Uma montagem por vez: com outra
    rodando, levanta Ocupado (a tela diz "tente em instantes")."""
    f = f or {}
    inicio, fim = periodo(de, ate)
    ok = _uma_por_vez.acquire(timeout=esperar) if esperar > 0 else _uma_por_vez.acquire(blocking=False)
    if not ok:
        raise Ocupado("o quadro está sendo montado para outra pessoa — tente de novo em instantes")
    try:
        pessoas = _pessoas(conn, quem, f, inicio, fim)
        # As etiquetas (vigia, ronda, celular, ponto da obra…) — e o filtro "tipo de pessoa".
        marcas = etiquetas_da_pessoa.das_pessoas(conn, pessoas)
        contagem_tipos: Counter = Counter()           # antes do filtro: a barra mostra quantos há
        for p in pessoas:
            cods = set(marcas.get(int(p["id"]), []))
            if cods & {"VIGIA_SEM_RONDA", "VIGIA_SEM_CELULAR"}:
                cods.add("VIGIA_PENDENTE")
            contagem_tipos.update(cods)
        tipos = {str(x) for x in (f.get("tipos_pessoa") or [])}
        if tipos:
            pessoas = [p for p in pessoas if etiquetas_da_pessoa.casa_com_filtro(marcas.get(int(p["id"]), []), tipos)]
        lote = espelho.Lote(conn, pessoas, inicio, fim) if pessoas else None
        hoje, agora = horario.hoje(), horario.agora()
        escalas_quer = {str(x).strip().lower() for x in (f.get("escalas") or []) if str(x).strip()}
        linhas, por_dia, por_pessoa, por_obra = [], {}, {}, {}
        obra_quer = str(f.get("obra_do_dia") or "").strip().upper()
        contagem: Counter = Counter()
        funcoes_vistas, escalas_vistas = Counter(), Counter()
        total_linhas = 0
        for p in pessoas:
            esp = espelho.montar(conn, int(p["id"]), inicio, fim, hoje=hoje, agora=agora, lote=lote)
            if p.get("funcao"):
                funcoes_vistas[p["funcao"].strip()] += 1
            for d in esp["dias"]:
                if d["situacao"] in NAO_JULGA:
                    continue
                if d.get("escala"):
                    escalas_vistas[d["escala"]] += 1
                if escalas_quer and (d.get("escala") or "").strip().lower() not in escalas_quer:
                    continue
                # A obra do dia: a da primeira batida; sem batida, a do cadastro (como em Hoje).
                obra_dia = next((b["obra"] for b in d["batidas"] if b.get("obra")), None) or p.get("obra_codigo") or "—"
                if obra_quer and obra_dia.upper() != obra_quer:
                    continue
                etq = etiquetas_do_dia(d)
                ob = por_obra.setdefault(obra_dia, {"obra": obra_dia, "pessoas": set(), "contagem": Counter(),
                                                    "bateram": set(), "por_dia": Counter()})
                ob["pessoas"].add(int(p["id"]))
                if d["batidas"]:                      # o mapa conta quem BATEU (21/10/2026)
                    ob["bateram"].add(int(p["id"]))
                    ob["por_dia"][d["data"]] += 1
                ob["contagem"].update(etq)
                ob["contagem"]["DIAS"] += 1
                contagem.update(etq)
                contagem["DIAS"] += 1
                if not passa_no_filtro(d, etq, f):
                    continue
                total_linhas += 1
                linha = _linha(p, d, etq, etiquetas_da_pessoa.para_json(marcas.get(int(p["id"]), [])))
                r = por_dia.setdefault(d["data"], Counter())
                r["pessoas"] += 1
                r.update(etq)
                pp = por_pessoa.setdefault(int(p["id"]), {
                    "colaborador_id": int(p["id"]), "nome": p["nome"], "funcao": p.get("funcao"),
                    "obra_cadastro": p.get("obra_codigo"), "dias": 0, "batidas": 0, "contagem": Counter(),
                    "marcas": linha["marcas"]})
                pp["dias"] += 1
                pp["batidas"] += len(linha["batidas"])
                pp["contagem"].update(etq)
                if len(linhas) < MAX_LINHAS:
                    linhas.append(linha)
    finally:
        _uma_por_vez.release()
    return {
        "de": inicio.isoformat(), "ate": fim.isoformat(), "pessoas_no_recorte": len(pessoas),
        "linhas": linhas, "total_linhas": total_linhas, "cortado": total_linhas > len(linhas),
        "contagem": dict(contagem),
        "por_dia": [{"data": k, "dia_semana": espelho.escalas.DIAS[dt.date.fromisoformat(k).weekday()], **dict(v)}
                    for k, v in sorted(por_dia.items())],
        "por_pessoa": sorted(({**v, "contagem": dict(v["contagem"])} for v in por_pessoa.values()),
                             key=lambda x: (-(x["contagem"].get("COM_BURACO", 0) + x["contagem"].get("FALTA", 0)),
                                            x["nome"] or "")),
        "contagem_tipos": dict(contagem_tipos),
        "por_obra": sorted(({"obra": v["obra"], "pessoas": len(v["pessoas"]), "contagem": dict(v["contagem"]),
                             "bateram": len(v["bateram"]), "por_dia": dict(v["por_dia"])}
                            for v in por_obra.values()), key=lambda x: x["obra"]),
        "_ids": [int(p["id"]) for p in pessoas],
        # sem escala o dia não é julgado (nem falta, nem batida faltando): a tela avisa
        "escala_padrao": (lambda e: e["nome"] if e else None)(escalas.escala_padrao(conn)),
        "opcoes": {"situacoes": [{"codigo": c, "rotulo": r} for c, r in SITUACOES],
                   "tipos_pessoa": [{"codigo": c, "rotulo": etiquetas_da_pessoa.ROTULO_FILTRO[c]}
                                    for c in etiquetas_da_pessoa.FILTRAVEIS],
                   "funcoes": sorted(funcoes_vistas), "escalas": sorted(escalas_vistas)},
    }


# ---------------------------------------------------------------------------
# A inclusão em lote
# ---------------------------------------------------------------------------
MAX_POR_CHAMADA = 40       # a tela manda de 40 em 40 e mostra o andamento


ACOES_DO_LOTE = ("batida", "dia")


def _motivo_do_lote(motivo) -> str:
    motivo = str(motivo or "").strip()
    if len(motivo) < 10:
        raise ErroDeValidacao("diga o motivo (vai no registro de cada batida)", campo="motivo")
    return motivo


def _conferir_itens(itens) -> None:
    if not itens:
        raise ErroDeValidacao("marque ao menos uma linha", campo="itens")
    if len(itens) > MAX_POR_CHAMADA:
        raise ErroDeValidacao(f"no máximo {MAX_POR_CHAMADA} por vez", campo="itens")


def _minuto_de(hora: str) -> int:
    hh, mm = (int(x) for x in str(hora).strip().split(":")[:2])
    return hh * 60 + mm


def _cada_item(quem: Quem, itens: list[dict], fazer) -> list[dict]:
    """Roda `fazer(conn, pessoa, dia_do_espelho, data)` item a item, cada um na sua
    transação: a trava do NSR nunca fica presa pelo lote inteiro, e uma recusada
    não desfaz as outras. `fazer` devolve o que vai no resultado da linha."""
    from . import ocorrencias
    saida = []
    for it in itens:
        r = {"colaborador_id": it.get("colaborador_id"), "data": it.get("data")}
        try:
            cid, dia = int(it["colaborador_id"]), _data(it.get("data"), "data")
            with db.conexao() as conn:
                pessoa = ocorrencias.exigir_pessoa_no_alcance(conn, quem, cid)
                r["nome"] = pessoa["nome"]
                d = espelho.montar(conn, cid, dia, dia)["dias"][0]
                r.update(ok=True, **fazer(conn, pessoa, d, dia))
        except (ErroDeValidacao, NaoEncontrado) as e:
            r.update(ok=False, erro=str(e))
        except (KeyError, TypeError, ValueError):
            r.update(ok=False, erro="linha ilegível")
        saida.append(r)
    return saida


def _ajuste_em_lote(conn: Connection, quem: Quem, pessoa: dict, d: dict, dia: dt.date, minuto: int,
                    motivo: str) -> dict:
    from . import ocorrencias
    momento = dt.datetime.combine(dia, dt.time(0)) + dt.timedelta(minutes=minuto)
    obra = next((b["obra"] for b in d["batidas"] if b.get("obra")), None) or pessoa.get("obra_codigo")
    if not obra:
        raise ErroDeValidacao("a pessoa não tem obra no dia nem no cadastro")
    o = ocorrencias.criar(conn, quem, {
        "tipo": "AJUSTE_BATIDA", "colaborador_id": int(pessoa["id"]), "data_inicio": dia.isoformat(),
        "horario": momento.isoformat(timespec="minutes"), "obra": obra,
        "descricao": motivo, "aprovar_ja": True, "lote": True}, origem="GESTAO")
    return {"hora": momento.strftime("%H:%M"), "status": o["status"], "aprovado": o["status"] == "APROVADA",
            "pedido_id": o["id"]}


def incluir_em_lote(quem: Quem, itens: list[dict], *, rotulo: str = "", hora: Optional[str] = None,
                    motivo: str, acao: str = "batida") -> list[dict]:
    """Inclui batidas para as linhas marcadas no quadro. Cada inclusão é um pedido
    de ajuste da gestão, aprovado na hora por quem pode (o caminho de sempre:
    competência fechada, batida que já existe e pedido repetido continuam
    barrados), marcado "em lote" para não contar contra a pessoa na qualidade.

    `acao`:
      · "batida" — UMA batida (`rotulo`: "Saída", "Entrada"…) no dia que já tem
        alguma. A hora é a da escala de cada um — ou `hora` (HH:MM), se vier; com
        a hora digitada, entra mesmo quando a escala não diz que ela falta (pessoa
        sem escala, horário fora do previsto);
      · "dia" — o DIA INTEIRO pela escala, no dia sem nenhuma batida (22/10/2026,
        o dono: "várias pessoas que eu poderia muito bem selecionar"): todas as
        batidas que a escala previa, juntas — ou entram todas, ou nenhuma."""
    acao = str(acao or "batida").strip().lower()
    if acao not in ACOES_DO_LOTE:
        raise ErroDeValidacao("ação de lote desconhecida", campo="acao")
    motivo = _motivo_do_lote(motivo)
    rotulo = str(rotulo or "").strip()
    if acao == "batida" and not rotulo:
        raise ErroDeValidacao("escolha qual batida incluir", campo="rotulo")
    if hora:
        try:
            hh, mm = (int(x) for x in str(hora).strip().split(":")[:2])
            assert 0 <= hh < 24 and 0 <= mm < 60
        except (ValueError, AssertionError):
            raise ErroDeValidacao("hora no formato HH:MM", campo="hora")
    _conferir_itens(itens)

    def uma_batida(conn, pessoa, d, dia):
        if not d["batidas"]:
            # Sem batida nenhuma: uma batida só inventaria meio dia. O dia inteiro
            # entra pela ação "dia" (pela escala) ou pelo espelho.
            raise ErroDeValidacao("neste dia não há nenhuma batida — use “lançar o dia inteiro” ou abone a falta")
        alvo = next((x for x in d["faltando"] if x["rotulo"].lower() == rotulo.lower()), None)
        if alvo is None and not hora:
            raise ErroDeValidacao(f"pela escala, neste dia não falta a batida “{rotulo}” — para incluir assim "
                                  "mesmo, digite a hora")
        minuto = _minuto_de(hora) if hora else int(alvo["minuto"])
        if hora and alvo is not None and alvo["minuto"] >= 1440 and minuto < 720:
            minuto += 1440                  # a saída do vigia é na madrugada do dia seguinte
        return _ajuste_em_lote(conn, quem, pessoa, d, dia, minuto, motivo)

    def o_dia_inteiro(conn, pessoa, d, dia):
        if d["batidas"]:
            raise ErroDeValidacao("neste dia já há batida — use “incluir uma batida” para a que falta")
        if not d["faltando"]:
            raise ErroDeValidacao("a escala não prevê batida neste dia (ou o horário ainda não chegou) — "
                                  "lance pelo espelho, com os horários")
        feitas = [_ajuste_em_lote(conn, quem, pessoa, d, dia, int(f["minuto"]), motivo) for f in d["faltando"]]
        return {"hora": " · ".join(x["hora"] for x in feitas), "batidas": len(feitas),
                "aprovado": all(x["aprovado"] for x in feitas), "status": feitas[-1]["status"]}

    return _cada_item(quem, itens, uma_batida if acao == "batida" else o_dia_inteiro)


def abonar_em_lote(quem: Quem, itens: list[dict], *, motivo: str) -> list[dict]:
    """Abona a FALTA das linhas marcadas (dia de chuva, obra parada, equipe
    liberada): um ABONO do DP por pessoa e dia, aprovado na hora por quem pode,
    com o motivo. Só no dia que o espelho chama de falta — dia com batida,
    descanso, feriado ou já justificado fica de fora, com o porquê."""
    from . import ocorrencias
    if not quem.dp:
        raise ErroDeValidacao("o abono é lançado pelo DP", campo="tipo")
    motivo = _motivo_do_lote(motivo)
    _conferir_itens(itens)

    def abonar(conn, pessoa, d, dia):
        if d["situacao"] != "FALTA":
            raise ErroDeValidacao(f"neste dia não há falta para abonar "
                                  f"({SITUACOES_DO_DIA.get(d['situacao'], d['situacao']).lower()})")
        o = ocorrencias.criar(conn, quem, {
            "tipo": "ABONO", "colaborador_id": int(pessoa["id"]), "data_inicio": dia.isoformat(),
            "data_fim": dia.isoformat(), "descricao": motivo, "aprovar_ja": True}, origem="GESTAO")
        return {"status": o["status"], "aprovado": o["status"] == "APROVADA", "pedido_id": o["id"]}

    return _cada_item(quem, itens, abonar)


# ---------------------------------------------------------------------------
# O mapa
# ---------------------------------------------------------------------------
MAX_PONTOS_NO_MAPA = 4000


def mapa(conn: Connection, quem: Quem, de, ate, f: Optional[dict] = None) -> dict:
    """O quadro no mapa (21/10/2026, o dono: "visualizar geograficamente a quantidade
    de funcionários batendo ponto num período ou dia, para entender onde tem mais
    gente"). Uma bolha por obra (quem bateu, completos, buracos, faltas, dia a dia)
    e, se pedido, o ponto de cada batida (fora da cerca em destaque). Os filtros são
    os do Quadro — a mesma conta, sem divergir."""
    r = montar(conn, quem, de, ate, f)
    obras = {o["codigo"]: cadastros.obra_para_json(o) for o in cadastros.listar_obras(conn, so_ativas=False)}
    com_lugar, sem_lugar = [], []
    for o in r["por_obra"]:
        cad = obras.get(o["obra"]) or {}
        item = {"codigo": o["obra"], "nome": cad.get("nome") or "", "municipio": cad.get("municipio"),
                "latitude": cad.get("latitude"), "longitude": cad.get("longitude"),
                "raio_metros": cad.get("raio_metros"), "bateram": o["bateram"], "pessoas": o["pessoas"],
                "contagem": o["contagem"], "por_dia": o["por_dia"]}
        (com_lugar if item["latitude"] is not None and item["longitude"] is not None else sem_lugar).append(item)
    ids = r["_ids"]
    pontos = db.todos(conn, f"""
        SELECT m.latitude, m.longitude, m.dentro_da_cerca, o.codigo AS obra, m.data_referencia
          FROM ponto.marcacoes m JOIN public.obras o ON o.id = m.obra_id
         WHERE m.data_referencia BETWEEN :i AND :f AND m.status <> 'REJEITADA'
           AND m.latitude IS NOT NULL AND m.colaborador_id = ANY(:ids)
           {"AND m.obra_id = ANY(:obras)" if quem.obras is not None else ""}
         ORDER BY m.id DESC LIMIT :lim""", i=r["de"], f=r["ate"], ids=ids or [0],
                    obras=list(quem.obras or []), lim=MAX_PONTOS_NO_MAPA) if ids else []
    return {
        "de": r["de"], "ate": r["ate"], "pessoas_no_recorte": r["pessoas_no_recorte"], "contagem": r["contagem"],
        "obras": sorted(com_lugar, key=lambda x: -x["bateram"]), "sem_lugar": sorted(sem_lugar, key=lambda x: -x["bateram"]),
        "dias": [d["data"] for d in r["por_dia"]],
        "pontos": [{"lat": float(p["latitude"]), "lon": float(p["longitude"]), "fora": p["dentro_da_cerca"] is False,
                    "obra": p["obra"], "data": p["data_referencia"].isoformat()} for p in pontos],
        "pontos_cortados": len(pontos) >= MAX_PONTOS_NO_MAPA,
    }
