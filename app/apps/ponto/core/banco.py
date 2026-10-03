# -*- coding: utf-8 -*-
"""
Banco de horas — só para quem tem (decisão do dono: "não é todo mundo que pode
usar banco de horas"). O regime é marcado POR PESSOA:

  SEM_BANCO           extra é paga no mês e débito é descontado; nada acumula
  COMPENSACAO_MENSAL  o que sobra num dia compensa outro DENTRO do mesmo mês
                      (CLT art. 59, §6º); o saldo zera a cada mês
  BANCO_6_MESES       acordo individual escrito (art. 59, §5º); crédito vence em 6 meses
  BANCO_12_MESES      acordo/convenção coletiva (art. 59, §2º); vence em 12 meses

O SALDO é a soma do que o cálculo do dia dá (extra − atraso/saída antecipada)
desde o início do banco, MAIS os lançamentos à mão (saldo inicial, pagamento,
desconto, folga aprovada).

⚠️ FALTA DE DIA INTEIRO NÃO ENTRA NO BANCO: falta sem justificativa é desconto
em folha, não hora devida. É a leitura mais comum da CLT; se a convenção da
construção disser outra coisa, muda aqui, num lugar só.

VENCIMENTO: o crédito mais antigo é o primeiro a ser usado (primeiro que entra,
primeiro que sai). O que sobra de cada mês vence no fim do prazo do regime.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import competencias, espelho

REGIMES = ("SEM_BANCO", "COMPENSACAO_MENSAL", "BANCO_6_MESES", "BANCO_12_MESES")
ROTULOS = {"SEM_BANCO": "Sem banco de horas", "COMPENSACAO_MENSAL": "Compensação no mês",
           "BANCO_6_MESES": "Banco de 6 meses (acordo individual)",
           "BANCO_12_MESES": "Banco de 12 meses (acordo coletivo)"}
PRAZO_MESES = {"BANCO_6_MESES": 6, "BANCO_12_MESES": 12}
TIPOS_LANCAMENTO = ("SALDO_INICIAL", "PAGAMENTO", "DESCONTO", "FOLGA", "CORRECAO")


def _somar_meses(d: dt.date, meses: int) -> dt.date:
    ano, mes = divmod(d.month - 1 + meses, 12)
    return dt.date(d.year + ano, mes + 1, 1)


def definir_regime(conn: Connection, colaborador_id: int, regime: str, *,
                   inicio=None, acordo_documento_id: int | None = None) -> None:
    regime = str(regime or "").upper()
    if regime not in REGIMES:
        raise ErroDeValidacao("regime de banco de horas desconhecido", campo="regime_banco")
    data = None
    if regime in PRAZO_MESES:
        try:
            data = dt.date.fromisoformat(str(inicio))
        except ValueError as e:
            raise ErroDeValidacao("diga a data de início do banco (AAAA-MM-DD)",
                                  campo="banco_inicio") from e
        tem_acordo = acordo_documento_id or db.um(
            conn, "SELECT acordo_documento_id FROM ponto.colaborador_config WHERE colaborador_id = :c",
            c=colaborador_id)
        if not acordo_documento_id and not (tem_acordo and tem_acordo.get("acordo_documento_id")):
            raise ErroDeValidacao("banco de horas exige o acordo assinado anexado",
                                  campo="acordo")
    db.executar(conn, """
        INSERT INTO ponto.colaborador_config (colaborador_id, regime_banco, banco_inicio,
                                              acordo_documento_id)
        VALUES (:c, :r, :i, :d)
        ON CONFLICT (colaborador_id) DO UPDATE SET regime_banco = :r, banco_inicio = :i,
               acordo_documento_id = COALESCE(:d, ponto.colaborador_config.acordo_documento_id),
               atualizado_em = now()
    """, c=colaborador_id, r=regime, i=data, d=acordo_documento_id)


def lancar(conn: Connection, colaborador_id: int, *, data, minutos: int, tipo: str,
           descricao: str, usuario_id: int | None, usuario_nome: str,
           ocorrencia_id: int | None = None) -> dict:
    tipo = str(tipo or "").upper()
    if tipo not in TIPOS_LANCAMENTO:
        raise ErroDeValidacao("tipo de lançamento desconhecido", campo="tipo")
    try:
        dia = data if isinstance(data, dt.date) else dt.date.fromisoformat(str(data))
    except ValueError as e:
        raise ErroDeValidacao("data ilegível", campo="data") from e
    try:
        minutos = int(minutos)
    except (TypeError, ValueError) as e:
        raise ErroDeValidacao("minutos inválidos", campo="minutos") from e
    if minutos == 0:
        raise ErroDeValidacao("lançamento de zero minuto não muda nada", campo="minutos")
    if len((descricao or "").strip()) < 5 and tipo != "FOLGA":
        raise ErroDeValidacao("descreva o lançamento", campo="descricao")
    competencias.exigir_aberta(conn, dia)
    linha = db.um(conn, """
        INSERT INTO ponto.banco_lancamentos (colaborador_id, data, minutos, tipo, descricao,
                                             ocorrencia_id, usuario_id, usuario_nome)
        VALUES (:c, :d, :m, :t, :desc, :o, :u, :n) RETURNING *
    """, c=colaborador_id, d=dia, m=minutos, t=tipo, desc=(descricao or "").strip()[:300],
         o=ocorrencia_id, u=usuario_id, n=usuario_nome[:120])
    return dict(linha)


def _inicio_do_banco(regime: str, banco_inicio: Optional[dt.date], ate: dt.date) -> Optional[dt.date]:
    if regime == "COMPENSACAO_MENSAL":
        return ate.replace(day=1)
    if regime in PRAZO_MESES:
        return banco_inicio
    return None


def saldo(conn: Connection, colaborador_id: int, *, ate: Optional[dt.date] = None) -> dict:
    """Saldo, vencimentos e o extrato por mês. `ate` padrão = ontem (hoje não fechou)."""
    cfg = db.um(conn, "SELECT regime_banco, banco_inicio FROM ponto.colaborador_config "
                      "WHERE colaborador_id = :c", c=colaborador_id) or {}
    regime = cfg.get("regime_banco") or "SEM_BANCO"
    # O cálculo do dia vai até ONTEM (hoje ainda não fechou); o lançamento à mão
    # vale no dia em que foi feito — quem acabou de lançar quer ver o saldo mudar.
    lancamentos_ate = ate or horario.hoje()
    ate = ate or (horario.hoje() - dt.timedelta(days=1))
    base = {"regime": regime, "regime_rotulo": ROTULOS[regime], "saldo": 0, "meses": [],
            "lancamentos": [], "vencimentos": [], "inicio": None}
    if regime == "SEM_BANCO":
        return base
    inicio = _inicio_do_banco(regime, cfg.get("banco_inicio"), ate)
    if inicio is None or inicio > ate:
        base["aviso"] = "o banco de horas desta pessoa ainda não começou"
        return base
    esp = espelho.montar(conn, colaborador_id, inicio, ate, hoje=ate + dt.timedelta(days=1))
    por_mes: dict[str, int] = {}
    for d in esp["dias"]:
        if d["falta"]:
            continue
        chave = d["data"][:7]
        por_mes[chave] = por_mes.get(chave, 0) + d["extra"] - d["debito"]
    lancamentos = db.todos(conn, """
        SELECT * FROM ponto.banco_lancamentos WHERE colaborador_id = :c AND data BETWEEN :i AND :f
         ORDER BY data, id
    """, c=colaborador_id, i=inicio, f=max(ate, lancamentos_ate))
    for l in lancamentos:
        chave = l["data"].strftime("%Y-%m")
        por_mes[chave] = por_mes.get(chave, 0) + int(l["minutos"])
    meses = [{"mes": m, "liquido": v} for m, v in sorted(por_mes.items())]
    total = sum(v for v in por_mes.values())

    vencimentos = []
    if regime in PRAZO_MESES:
        # Primeiro que entra, primeiro que sai: o débito de um mês consome o
        # crédito mais antigo que ainda sobra.
        creditos: list[list] = []
        for m in meses:
            valor = m["liquido"]
            if valor > 0:
                creditos.append([m["mes"], valor])
            elif valor < 0:
                falta = -valor
                for c in creditos:
                    usado = min(c[1], falta)
                    c[1] -= usado
                    falta -= usado
                    if not falta:
                        break
        for mes, resto in creditos:
            if resto > 0:
                ano, mm = map(int, mes.split("-"))
                vence = _somar_meses(dt.date(ano, mm, 1), PRAZO_MESES[regime] + 1) - dt.timedelta(days=1)
                vencimentos.append({"mes": mes, "minutos": resto, "vence_em": vence.isoformat()})
    base.update(saldo=total, meses=meses, inicio=inicio.isoformat(),
                lancamentos=[{**l, "data": l["data"].isoformat(),
                              "criado_em": horario.texto(l["criado_em"])} for l in lancamentos],
                vencimentos=vencimentos)
    return base
