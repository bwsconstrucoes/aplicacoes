# -*- coding: utf-8 -*-
"""
A PROGRAMAÇÃO DE FÉRIAS (migração 116, 20/11/2026).

⚑ PEDIDO DELE: *"quando houver a confirmação do DP e essas férias estiverem
se aproximando, tanto o DP quanto a pessoa que bate o ponto desse colaborador
tem que estar ciente (…) o sistema precisa ter a programação dessas férias (…)
a gente pede à contabilidade, confirma o que foi registrado. O sistema
registra. Lembrando que tem que pensar na possibilidade de alteração e
cancelamento. E aí tem que estar DP no meio, contabilidade no meio, o agente
de IA no meio acompanhando."*

O CAMINHO, e quem está no meio de cada passo:

| Passo | Quem | O que o sistema grava |
|---|---|---|
| a obra (ou o DP) pede férias | solicitação FERIAS | nada ainda |
| o DP atende | DP | programação PEDIDA (esperando a contabilidade) |
| o DP envia o pedido à contabilidade | DP → contabilidade | o pedido ligado |
| a contabilidade devolve | contabilidade | programação CONFIRMADA |
| alguém pede alteração ou cancelamento | solicitação ALTERACAO_FERIAS / CANCELAMENTO_FERIAS | nada ainda |
| o DP atende | DP | `mudanca_pendente` — as datas confirmadas continuam valendo |
| a contabilidade devolve | contabilidade | a mudança vale; a anterior vai para o `historico` |

⚑ A DATA SÓ MUDA COM O RETORNO DA CONTABILIDADE, e é de propósito: enquanto
ela não confirma, o que está registrado no eSocial é a data antiga. Mostrar a
nova como se valesse seria o sistema dizendo uma coisa e a folha outra.

Os avisos de férias chegando (para o DP e para quem cuida do ponto na obra)
saem daqui: `chegando`.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

SITUACAO_ROTULOS = {
    "PEDIDA": "Aguardando a contabilidade",
    "CONFIRMADA": "Confirmada pela contabilidade",
    "CANCELADA": "Cancelada",
}
MUDANCA_ROTULOS = {
    "ALTERACAO": "alteração pedida, aguardando a contabilidade",
    "CANCELAMENTO": "cancelamento pedido, aguardando a contabilidade",
}
# Quantos dias antes do início o aviso de "férias chegando" aparece para a obra
# e para o DP. Duas semanas: dá tempo de a obra se organizar sem a pessoa.
AVISAR_COM_DIAS = 15


_tabela: dict[str, bool] = {"existe": False}


def tem_tabela(s: Session) -> bool:
    """A migração 116 pode não ter sido aplicada ainda: o código sobe para o
    Render antes de alguém apertar o botão. Sem a tabela, a lista vem vazia e
    o atendimento recusa com recado — nenhuma tela cai. Depois que a tabela
    existe, a pergunta não se repete no processo."""
    if not _tabela["existe"]:
        _tabela["existe"] = s.execute(text(
            "SELECT 1 FROM information_schema.tables "
            " WHERE table_schema = 'public' "
            "   AND table_name = 'ferias_programadas'")).first() is not None
    return _tabela["existe"]


def _exigir_tabela(s: Session) -> None:
    if not tem_tabela(s):
        raise ErroValidacao(
            "A programação de férias ainda não está no banco: um administrador "
            "precisa apertar \"Aplicar atualizações do banco\" em Configurações.")


def _data(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v or "")[:10])
    except ValueError:
        return None


def _dias(v: Any) -> Optional[int]:
    try:
        n = int(str(v or "").strip())
    except ValueError:
        return None
    return n if n > 0 else None


def _sim(v: Any) -> bool:
    return str(v or "").strip().lower() in ("1", "sim", "true", "s")


def periodo(inicio: date, dias: int) -> date:
    """O último dia de férias: o início conta como o primeiro."""
    return inicio + timedelta(days=dias - 1)


def _ler_periodo(dados: dict[str, Any]) -> tuple[date, int]:
    inicio = _data(dados.get("ferias_inicio"))
    dias = _dias(dados.get("ferias_dias"))
    if inicio is None:
        raise ErroValidacao("Informe o início das férias.")
    if dias is None or dias > 30:
        raise ErroValidacao("Informe quantos dias de férias (de 1 a 30).")
    return inicio, dias


def vigente(s: Session, colaborador_id: int) -> Optional[dict[str, Any]]:
    """A programação desta pessoa que ainda vale: a próxima ou a em curso.

    É a que um pedido de alteração ou cancelamento mexe quando não diz qual.
    """
    if not tem_tabela(s):
        return None
    linha = s.execute(text("""
        SELECT * FROM ferias_programadas
         WHERE colaborador_id = :c AND situacao <> 'CANCELADA'
           AND fim >= CURRENT_DATE
         ORDER BY inicio LIMIT 1"""), {"c": colaborador_id}).mappings().first()
    return dict(linha) if linha else None


def _da_solicitacao(s: Session, sol: dict[str, Any]) -> Optional[dict[str, Any]]:
    dados = sol.get("dados") or {}
    fid = str(dados.get("ferias_id") or "")
    if fid.isdigit():
        linha = s.execute(text("""SELECT * FROM ferias_programadas
                                   WHERE id = :i AND colaborador_id = :c"""),
                          {"i": int(fid), "c": sol.get("colaborador_id")}).mappings().first()
        if linha:
            return dict(linha)
    return vigente(s, int(sol["colaborador_id"])) if sol.get("colaborador_id") else None


# ---------------------------------------------------------------------------
# O ATENDIMENTO DO DP
# ---------------------------------------------------------------------------
def programar(s: Session, sol: dict[str, Any], dados: dict[str, Any],
              usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP atendeu o pedido de férias: a programação nasce PEDIDA."""
    _exigir_tabela(s)
    inicio, dias = _ler_periodo(dados)
    colaborador_id = int(sol["colaborador_id"])
    choque = s.execute(text("""
        SELECT inicio, fim FROM ferias_programadas
         WHERE colaborador_id = :c AND situacao <> 'CANCELADA'
           AND inicio <= :f AND fim >= :i LIMIT 1"""),
        {"c": colaborador_id, "i": inicio, "f": periodo(inicio, dias)}).first()
    if choque is not None:
        raise ErroValidacao(
            f"Já há férias programadas de {choque[0]:%d/%m/%Y} a "
            f"{choque[1]:%d/%m/%Y} para esta pessoa. Para mudar a data, o "
            "pedido é de alteração de férias.")
    obra = s.execute(text("SELECT obra_id FROM colaboradores WHERE id = :c"),
                     {"c": colaborador_id}).scalar()
    novo = s.execute(text("""
        INSERT INTO ferias_programadas (colaborador_id, obra_id, inicio, dias, fim,
                                        abono_pecuniario, adiantamento_13,
                                        situacao, solicitacao_id, observacao)
        VALUES (:c, :o, :i, :d, :f, :ab, :ad, 'PEDIDA', :s, :obs)
        RETURNING id"""),
        {"c": colaborador_id, "o": obra or sol.get("obra_id"), "i": inicio,
         "d": dias, "f": periodo(inicio, dias),
         "ab": _sim(dados.get("abono_pecuniario")),
         "ad": _sim(dados.get("adiantamento_13")), "s": sol["id"],
         "obs": (str(dados.get("observacao") or "").strip() or None)}).scalar()
    return {"ferias_id": int(novo), "inicio": str(inicio), "dias": dias}


def pedir_mudanca(s: Session, sol: dict[str, Any], dados: dict[str, Any],
                  tipo: str, usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP atendeu a alteração ou o cancelamento: a mudança fica PENDENTE até
    a contabilidade devolver. As datas confirmadas continuam valendo."""
    _exigir_tabela(s)
    atual = _da_solicitacao(s, sol)
    mudanca: dict[str, Any] = {"tipo": tipo, "solicitacao_id": sol["id"],
                               "numero": sol.get("numero"),
                               "pedido_em": date.today().isoformat()}
    if tipo == "ALTERACAO":
        inicio, dias = _ler_periodo(dados)
        mudanca.update({"inicio": inicio.isoformat(), "dias": dias,
                        "abono_pecuniario": _sim(dados.get("abono_pecuniario")),
                        "adiantamento_13": _sim(dados.get("adiantamento_13"))})
    if atual is None:
        # ⚠️ FÉRIAS MARCADAS ANTES DO SISTEMA (no Pipefy, na planilha) não têm
        # linha aqui. Recusar seria travar o DP por uma lacuna nossa: a
        # alteração segue para a contabilidade, e no retorno a programação
        # nasce com as datas novas. O cancelamento só passa a constar na
        # trilha do pedido.
        return {"sem_programacao": True, "mudanca": mudanca}
    if atual["situacao"] == "CANCELADA":
        raise ErroValidacao("Estas férias já estão canceladas.")
    if atual.get("mudanca_pendente"):
        ja = atual["mudanca_pendente"]
        raise ErroValidacao(
            f"Já há {MUDANCA_ROTULOS.get(ja.get('tipo'), 'uma mudança')} nestas "
            f"férias (pedido {ja.get('numero') or '?'}). Espere o retorno ou "
            "cancele aquele pedido antes.")
    s.execute(text("""UPDATE ferias_programadas
                         SET mudanca_pendente = CAST(:m AS jsonb), atualizado_em = now()
                       WHERE id = :i"""),
              {"m": json.dumps(mudanca), "i": atual["id"]})
    return {"ferias_id": int(atual["id"]), "mudanca": mudanca}


# ---------------------------------------------------------------------------
# O RETORNO DA CONTABILIDADE
# ---------------------------------------------------------------------------
def _registro(linha: dict[str, Any], motivo: str) -> dict[str, Any]:
    return {"inicio": str(linha["inicio"]), "dias": linha["dias"],
            "fim": str(linha["fim"]), "situacao": linha["situacao"],
            "motivo": motivo, "em": date.today().isoformat()}


def contabilidade_devolveu(s: Session, sol: dict[str, Any],
                           pedido: dict[str, Any]) -> Optional[str]:
    """A contabilidade concluiu o pedido desta solicitação: o que estava
    pedido passa a valer. Devolve o que aconteceu, para a trilha."""
    tipo = sol["tipo"]
    if tipo == "FERIAS":
        mexeu = s.execute(text("""
            UPDATE ferias_programadas
               SET situacao = 'CONFIRMADA', confirmada_em = now(),
                   pedido_id = :p, atualizado_em = now()
             WHERE solicitacao_id = :s AND situacao = 'PEDIDA'
         RETURNING inicio, fim"""), {"s": sol["id"], "p": pedido.get("id")}).first()
        return (f"férias confirmadas: {mexeu[0]:%d/%m/%Y} a {mexeu[1]:%d/%m/%Y}"
                if mexeu else None)
    linha = s.execute(text("""
        SELECT * FROM ferias_programadas
         WHERE mudanca_pendente ->> 'solicitacao_id' = :s"""),
        {"s": str(sol["id"])}).mappings().first()
    if tipo == "CANCELAMENTO_FERIAS":
        if linha is None:
            return "cancelamento confirmado (as férias não estavam programadas no sistema)"
        hist = list(linha["historico"] or []) + [_registro(dict(linha), "cancelada")]
        s.execute(text("""
            UPDATE ferias_programadas
               SET situacao = 'CANCELADA', cancelada_em = now(),
                   mudanca_pendente = NULL, historico = CAST(:h AS jsonb),
                   atualizado_em = now()
             WHERE id = :i"""), {"i": linha["id"], "h": json.dumps(hist)})
        return f"férias de {linha['inicio']:%d/%m/%Y} canceladas"
    if tipo == "ALTERACAO_FERIAS":
        if linha is None:
            # Férias de antes do sistema: a programação nasce com a data nova.
            d = sol.get("dados") or {}
            inicio, dias = _ler_periodo(d)
            s.execute(text("""
                INSERT INTO ferias_programadas
                    (colaborador_id, obra_id, inicio, dias, fim, abono_pecuniario,
                     adiantamento_13, situacao, solicitacao_id, pedido_id,
                     confirmada_em)
                SELECT c.id, c.obra_id, :i, :d, :f, :ab, :ad, 'CONFIRMADA', :s,
                       :p, now()
                  FROM colaboradores c WHERE c.id = :c"""),
                {"c": sol["colaborador_id"], "i": inicio, "d": dias,
                 "f": periodo(inicio, dias), "ab": _sim(d.get("abono_pecuniario")),
                 "ad": _sim(d.get("adiantamento_13")), "s": sol["id"],
                 "p": pedido.get("id")})
            return f"férias registradas: {inicio:%d/%m/%Y}, {dias} dias"
        m = linha["mudanca_pendente"] or {}
        inicio, dias = _data(m.get("inicio")), int(m.get("dias") or linha["dias"])
        hist = list(linha["historico"] or []) + [_registro(dict(linha), "alterada")]
        s.execute(text("""
            UPDATE ferias_programadas
               SET inicio = :i, dias = :d, fim = :f, abono_pecuniario = :ab,
                   adiantamento_13 = :ad, situacao = 'CONFIRMADA',
                   confirmada_em = now(), pedido_id = :p,
                   mudanca_pendente = NULL, historico = CAST(:h AS jsonb),
                   atualizado_em = now()
             WHERE id = :id"""),
            {"i": inicio, "d": dias, "f": periodo(inicio, dias),
             "ab": bool(m.get("abono_pecuniario")),
             "ad": bool(m.get("adiantamento_13")), "p": pedido.get("id"),
             "h": json.dumps(hist), "id": linha["id"]})
        return f"férias alteradas para {inicio:%d/%m/%Y}, {dias} dias"
    return None


# ---------------------------------------------------------------------------
# A LISTA E OS AVISOS
# ---------------------------------------------------------------------------
def _linha_para_tela(l: dict[str, Any], hoje: date) -> dict[str, Any]:
    mud = l.get("mudanca_pendente") or None
    faltam = (l["inicio"] - hoje).days
    if l["situacao"] == "CANCELADA":
        momento = "cancelada"
    elif l["inicio"] <= hoje <= l["fim"]:
        momento = "em_ferias"
    elif hoje > l["fim"]:
        momento = "passou"
    else:
        momento = "chegando" if faltam <= AVISAR_COM_DIAS else "programada"
    return {
        "id": l["id"], "colaborador_id": l["colaborador_id"],
        "pessoa": l.get("pessoa") or "", "cargo": l.get("cargo") or "",
        "obra": l.get("obra") or "",
        "inicio": str(l["inicio"]), "fim": str(l["fim"]), "dias": l["dias"],
        "retorno": str(l["fim"] + timedelta(days=1)),
        "abono_pecuniario": bool(l["abono_pecuniario"]),
        "adiantamento_13": bool(l["adiantamento_13"]),
        "situacao": l["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(l["situacao"], l["situacao"]),
        "mudanca": ({**mud, "rotulo": MUDANCA_ROTULOS.get(mud.get("tipo"), "")}
                    if mud else None),
        "faltam_dias": faltam, "momento": momento,
        "historico": l.get("historico") or [],
        "solicitacao": l.get("numero") or "",
    }


def listar(s: Session, *, obras: Optional[list[int]] = None,
           desde: Optional[date] = None, ate: Optional[date] = None,
           com_canceladas: bool = False, colaborador_id: Optional[int] = None,
           limite: int = 500) -> list[dict[str, Any]]:
    """A programação. `obras=None` é a base inteira; lista vazia, nenhuma."""
    if (obras is not None and not obras) or not tem_tabela(s):
        return []
    hoje = date.today()
    linhas = s.execute(text("""
        SELECT f.*, c.nome AS pessoa, g.nome AS cargo, o.codigo AS obra,
               d.numero
          FROM ferias_programadas f
          JOIN colaboradores c ON c.id = f.colaborador_id
          LEFT JOIN cargos g ON g.id = c.cargo_id
          LEFT JOIN obras o ON o.id = COALESCE(c.obra_id, f.obra_id)
          LEFT JOIN dp_solicitacoes d ON d.id = f.solicitacao_id
         WHERE (:todas OR COALESCE(c.obra_id, f.obra_id) = ANY(:obras))
           AND f.fim >= :desde AND f.inicio <= :ate
           AND (:canc OR f.situacao <> 'CANCELADA')
           AND (CAST(:pessoa AS INTEGER) IS NULL OR f.colaborador_id = :pessoa)
         ORDER BY f.inicio, c.nome LIMIT :l"""),
        {"todas": obras is None, "obras": obras or [],
         "desde": desde or (hoje - timedelta(days=60)),
         "ate": ate or (hoje + timedelta(days=400)), "canc": com_canceladas,
         "pessoa": colaborador_id, "l": limite}).mappings().all()
    return [_linha_para_tela(dict(l), hoje) for l in linhas]


def chegando(s: Session, *, obras: Optional[list[int]] = None,
             dias: int = AVISAR_COM_DIAS) -> list[dict[str, Any]]:
    """Férias que começam nos próximos `dias` ou que estão em curso — o aviso
    para quem cuida do ponto na obra e para o DP."""
    hoje = date.today()
    return [f for f in listar(s, obras=obras, desde=hoje,
                              ate=hoje + timedelta(days=dias))
            if f["momento"] in ("chegando", "em_ferias")]
