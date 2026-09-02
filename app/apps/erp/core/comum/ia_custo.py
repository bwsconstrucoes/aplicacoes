# ============================================================================
# ERP — core/comum/ia_custo.py
# Registro do consumo de IA, teto mensal e aviso de gasto.
#
# A resposta da OpenAI traz quantos tokens foram usados; guardando isso com o
# preço do modelo, sabe-se quanto custou cada leitura. Sem esse número, a
# decisão de usar mais ou menos IA vira palpite.
#
# Há UM ponto por onde toda chamada passa (leitor._chamar_ia) e é lá que o
# registro acontece — não em cada tela. Quem chama só diz, pelo `contexto`,
# QUAL operação está fazendo. Assim uma tela nova que use o leitor já nasce
# contabilizada; esquecer não deixa buraco.
#
# O registro roda em sessão própria e nunca derruba a operação principal:
# perder uma linha do painel é aceitável, perder um lançamento não.
#
# Os preços são por milhão de tokens, em dólar, e ficam aqui para poderem ser
# atualizados sem tocar no resto do código.
# ============================================================================
from __future__ import annotations

import contextlib
import contextvars
import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# US$ por milhão de tokens (entrada, saída) — conferido em 2026-09-02.
# Modelo fora desta tabela é cobrado pelo PRECO_PADRAO e aparece no painel
# marcado como "preço estimado", para ninguém confiar num número que não é.
PRECOS = {
    "gpt-4o-mini":  (Decimal("0.15"), Decimal("0.60")),
    "gpt-4o":       (Decimal("2.50"), Decimal("10.00")),
    "gpt-4.1-nano": (Decimal("0.10"), Decimal("0.40")),
    "gpt-4.1-mini": (Decimal("0.40"), Decimal("1.60")),
    "gpt-4.1":      (Decimal("2.00"), Decimal("8.00")),
    "gpt-5-nano":   (Decimal("0.05"), Decimal("0.40")),
    "gpt-5-mini":   (Decimal("0.25"), Decimal("2.00")),
    "gpt-5":        (Decimal("1.25"), Decimal("10.00")),
}
PRECO_PADRAO = (Decimal("0.50"), Decimal("2.00"))
_MILHAO = Decimal("1000000")

# Operação usada quando ninguém disse qual é. Existe para o registro nunca
# ficar sem rótulo — mas toda tela que usa IA deve declarar a sua.
OPERACAO_PADRAO = "leitura_documento"

# Chaves na tabela `parametros`
CHAVE_TETO = "ia_teto_mensal_usd"
CHAVE_ALERTA = "ia_alerta_enviado"        # valor: "AAAA-MM:80" ou "AAAA-MM:100"

LIMIAR_AVISO = 80                          # % do teto em que o aviso sai


def preco_conhecido(modelo: str) -> bool:
    return (modelo or "").lower() in PRECOS


def custo(modelo: str, entrada: int, saida: int) -> Decimal:
    p_in, p_out = PRECOS.get((modelo or "").lower(), PRECO_PADRAO)
    return ((Decimal(entrada) * p_in + Decimal(saida) * p_out) / _MILHAO).quantize(
        Decimal("0.000001"))


# ---------------------------------------------------------------------------
# Contexto: quem está pedindo, e para quê
#
# A rota sabe quem é o usuário; a função do core sabe qual operação está
# fazendo; o leitor sabe quantos tokens gastou. Ninguém sabe as três coisas.
# O contexto junta: cada camada preenche o que conhece, e o registro lê tudo.
# ---------------------------------------------------------------------------
_CONTEXTO: contextvars.ContextVar[Optional[dict[str, Any]]] = contextvars.ContextVar(
    "erp_ia_contexto", default=None)


def contexto_atual() -> dict[str, Any]:
    return dict(_CONTEXTO.get() or {})


@contextlib.contextmanager
def contexto(*, operacao: Optional[str] = None, usuario_id: Optional[int] = None,
             referencia: Optional[str] = None) -> Iterator[None]:
    """Declara a operação (e, se souber, quem pede) para as chamadas de IA
    feitas dentro do bloco. O que não for informado herda do bloco de fora."""
    atual = contexto_atual()
    if operacao is not None:
        atual["operacao"] = operacao
    if usuario_id is not None:
        atual["usuario_id"] = usuario_id
    if referencia is not None:
        atual["referencia"] = referencia
    token = _CONTEXTO.set(atual)
    try:
        yield
    finally:
        _CONTEXTO.reset(token)


def iniciar_contexto_requisicao(usuario_id: Optional[int]) -> contextvars.Token:
    """Chamado no início de cada requisição do ERP. As threads do gunicorn são
    reaproveitadas entre requisições, então o contexto tem de ser zerado a
    cada uma — senão o usuário anterior "assina" a chamada do próximo."""
    return _CONTEXTO.set({"usuario_id": usuario_id} if usuario_id else {})


def encerrar_contexto_requisicao(token: Optional[contextvars.Token]) -> None:
    if token is None:
        return
    try:
        _CONTEXTO.reset(token)
    except ValueError:                       # token de outro contexto — ignora
        pass


# ---------------------------------------------------------------------------
# Registro
# ---------------------------------------------------------------------------
def _tokens(resposta: Any) -> tuple[int, int]:
    uso = getattr(resposta, "usage", None) if resposta is not None else None
    if uso is None:
        return 0, 0
    entrada = int(getattr(uso, "prompt_tokens", 0) or getattr(uso, "input_tokens", 0) or 0)
    saida = int(getattr(uso, "completion_tokens", 0) or getattr(uso, "output_tokens", 0) or 0)
    return entrada, saida


def registrar(s: Session, *, modelo: str, operacao: str, resposta: Any = None,
              duracao_ms: Optional[int] = None, usuario_id: Optional[int] = None,
              referencia: str = "", sucesso: bool = True, erro: str = "") -> None:
    """Grava o consumo de uma chamada na sessão dada. Nunca derruba a operação
    principal."""
    from app.apps.erp.db.models.financeiro import IaUso
    try:
        entrada, saida = _tokens(resposta)
        s.add(IaUso(modelo=modelo or "?", operacao=operacao or OPERACAO_PADRAO,
                    tokens_entrada=entrada, tokens_saida=saida,
                    custo_usd=custo(modelo, entrada, saida),
                    duracao_ms=duracao_ms, sucesso=sucesso,
                    erro=(erro or "")[:400] or None,
                    usuario_id=usuario_id, referencia=(referencia or "")[:120] or None))
        s.flush()
    except Exception as e:                      # registro não pode quebrar nada
        logger.warning("ERP/IA: não foi possível registrar o consumo (%s)", e)


def registrar_autonomo(*, modelo: str, resposta: Any = None,
                       duracao_ms: Optional[int] = None, operacao: Optional[str] = None,
                       usuario_id: Optional[int] = None, referencia: Optional[str] = None,
                       sucesso: bool = True, erro: str = "") -> None:
    """Grava o consumo em sessão PRÓPRIA, com commit, lendo do contexto o que
    não vier por parâmetro. É o que o leitor chama — ele não tem sessão.

    Depois de gravar, confere o teto do mês e avisa se passou do limiar. Tudo
    dentro de um try: falha aqui vira log, nunca erro para quem lançou."""
    ctx = contexto_atual()
    operacao = operacao or ctx.get("operacao") or OPERACAO_PADRAO
    usuario_id = usuario_id if usuario_id is not None else ctx.get("usuario_id")
    referencia = referencia if referencia is not None else (ctx.get("referencia") or "")
    try:
        from app.apps.erp.db.database import get_session
        with get_session() as s:
            registrar(s, modelo=modelo, operacao=operacao, resposta=resposta,
                      duracao_ms=duracao_ms, usuario_id=usuario_id,
                      referencia=referencia, sucesso=sucesso, erro=erro)
            s.commit()
            _avisar_se_passou_do_teto(s)
    except Exception as e:
        logger.warning("ERP/IA: consumo de %s (%s) não registrado: %s",
                       operacao, modelo, e)


# ---------------------------------------------------------------------------
# Teto mensal
# ---------------------------------------------------------------------------
def _buscar_parametro(s: Session, chave: str):
    """A conferência da chave em Python é redundante com o WHERE no banco de
    verdade e necessária na sessão dublada dos testes, que ignora o WHERE —
    sem ela, gravar a marca de aviso sobrescreveria o teto."""
    from app.apps.erp.db.models.cadastros import Parametro
    for p in s.scalars(select(Parametro).where(Parametro.chave == chave)).all():
        if p.chave == chave:
            return p
    return None


def _parametro(s: Session, chave: str) -> Optional[str]:
    p = _buscar_parametro(s, chave)
    return p.valor if p is not None else None


def _gravar_parametro(s: Session, chave: str, valor: str,
                      usuario_id: Optional[int] = None) -> None:
    from app.apps.erp.db.models.cadastros import Parametro
    p = _buscar_parametro(s, chave)
    if p is None:
        p = Parametro(chave=chave, valor=valor)
        s.add(p)
    p.valor = valor
    p.atualizado_em = datetime.now(timezone.utc)
    p.atualizado_por = usuario_id
    s.flush()


def teto_mensal(s: Session) -> Optional[Decimal]:
    """Teto configurado em US$; None = sem teto."""
    bruto = _parametro(s, CHAVE_TETO)
    if not bruto:
        return None
    try:
        v = Decimal(str(bruto).replace(",", "."))
    except InvalidOperation:
        return None
    return v if v > 0 else None


def definir_teto_mensal(s: Session, valor: Any, usuario_id: Optional[int] = None) -> Optional[Decimal]:
    """Grava o teto. Vazio ou zero desliga o aviso."""
    texto = str(valor if valor is not None else "").strip().replace("US$", "").replace(" ", "")
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")
    if not texto:
        _gravar_parametro(s, CHAVE_TETO, "", usuario_id)
        return None
    try:
        v = Decimal(texto).quantize(Decimal("0.01"))
    except InvalidOperation:
        raise ValueError("Teto inválido — informe um valor em dólar, como 25 ou 25,50.")
    if v < 0:
        raise ValueError("O teto não pode ser negativo.")
    _gravar_parametro(s, CHAVE_TETO, str(v) if v > 0 else "", usuario_id)
    return v if v > 0 else None


def _inicio_do_mes(hoje: Optional[date] = None) -> datetime:
    hoje = hoje or date.today()
    return datetime(hoje.year, hoje.month, 1, tzinfo=timezone.utc)


def gasto_do_mes(s: Session, hoje: Optional[date] = None) -> Decimal:
    from app.apps.erp.db.models.financeiro import IaUso
    v = s.scalar(select(func.coalesce(func.sum(IaUso.custo_usd), 0))
                 .where(IaUso.criado_em >= _inicio_do_mes(hoje)))
    return Decimal(str(v or 0))


def situacao_teto(s: Session, hoje: Optional[date] = None) -> dict[str, Any]:
    """Quanto do teto já foi. `alerta` é None, "AVISO" (>= 80%) ou
    "ESTOUROU" (>= 100%). Sem teto, nunca há alerta — só o número."""
    teto = teto_mensal(s)
    gasto = gasto_do_mes(s, hoje)
    if teto is None:
        return {"teto": None, "gasto": float(gasto), "percentual": None, "alerta": None}
    pct = int((gasto / teto * 100).quantize(Decimal("1"))) if teto else 0
    alerta = "ESTOUROU" if pct >= 100 else ("AVISO" if pct >= LIMIAR_AVISO else None)
    return {"teto": float(teto), "gasto": float(gasto), "percentual": pct, "alerta": alerta}


def _nivel_ja_avisado(s: Session, mes: str) -> int:
    """0 = nada avisado neste mês; 80 ou 100 = já avisado até esse nível."""
    bruto = _parametro(s, CHAVE_ALERTA) or ""
    if not bruto.startswith(mes + ":"):
        return 0
    try:
        return int(bruto.split(":", 1)[1])
    except ValueError:
        return 0


def _avisar_se_passou_do_teto(s: Session, hoje: Optional[date] = None) -> Optional[str]:
    """Manda o aviso UMA vez por nível por mês (80% e depois 100%). Devolve o
    nível avisado, ou None. Não bloqueia nada — só avisa."""
    hoje = hoje or date.today()
    sit = situacao_teto(s, hoje)
    if not sit["alerta"]:
        return None
    nivel = 100 if sit["alerta"] == "ESTOUROU" else LIMIAR_AVISO
    mes = hoje.strftime("%Y-%m")
    if _nivel_ja_avisado(s, mes) >= nivel:
        return None
    texto = _texto_aviso(sit, nivel, hoje)
    enviados = _enviar_aos_administradores(s, texto)
    # marca como avisado mesmo sem destinatário: o log já tem o aviso, e
    # repetir a cada leitura só faria barulho
    _gravar_parametro(s, CHAVE_ALERTA, f"{mes}:{nivel}")
    s.commit()
    logger.warning("ERP/IA: %s — %s enviado(s)", texto.splitlines()[0], enviados)
    return sit["alerta"]


def _texto_aviso(sit: dict[str, Any], nivel: int, hoje: date) -> str:
    if nivel >= 100:
        cabeca = "⚠️ ERP: o gasto com IA ESTOUROU o teto do mês"
    else:
        cabeca = f"⚠️ ERP: gasto com IA passou de {nivel}% do teto do mês"
    return (f"{cabeca}\n"
            f"Gasto até {hoje.strftime('%d/%m')}: US$ {sit['gasto']:.2f} "
            f"de US$ {sit['teto']:.2f} ({sit['percentual']}%).\n"
            f"Nada foi bloqueado. Veja o painel em Configurações › Consumo de IA.")


def _enviar_aos_administradores(s: Session, texto: str) -> int:
    """Telegram para cada ADMIN com telefone ou CPF no cadastro."""
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    try:
        from app.apps.notificador import enviar_telegram
    except Exception as e:                        # módulo ausente fora do monorepo
        logger.warning("ERP/IA: notificador indisponível (%s)", e)
        return 0
    enviados = 0
    admins = s.scalars(select(Usuario).where(
        Usuario.perfil == PerfilUsuario.ADMIN, Usuario.ativo.is_(True))).all()
    for u in admins:
        if not (u.telefone or u.cpf):
            continue
        try:
            r = enviar_telegram(telefone=u.telefone, cpf=u.cpf, mensagem=texto)
            if r and r.get("ok"):
                enviados += 1
        except Exception as e:
            logger.warning("ERP/IA: aviso de teto não chegou a %s (%s)", u.nome, e)
    return enviados


# ---------------------------------------------------------------------------
# Painel
# ---------------------------------------------------------------------------
def painel(s: Session, dias: int = 90) -> dict[str, Any]:
    """Quanto se gastou, em que, por quem e com qual modelo."""
    from app.apps.erp.db.models.cadastros import Usuario
    from app.apps.erp.db.models.financeiro import IaUso

    desde = datetime.now(timezone.utc) - timedelta(days=dias)

    total = s.execute(select(
        func.count(IaUso.id), func.coalesce(func.sum(IaUso.custo_usd), 0),
        func.coalesce(func.sum(IaUso.tokens_entrada + IaUso.tokens_saida), 0),
        func.coalesce(func.avg(IaUso.duracao_ms), 0),
        func.count(IaUso.id).filter(IaUso.sucesso.is_(False)),
    ).where(IaUso.criado_em >= desde)).first()

    por_operacao = [{
        "operacao": op, "chamadas": n, "custo": float(c or 0),
        "tokens": int(t or 0),
        "custo_medio": float((c or 0) / n) if n else 0.0,
    } for op, n, c, t in s.execute(
        select(IaUso.operacao, func.count(IaUso.id),
               func.sum(IaUso.custo_usd),
               func.sum(IaUso.tokens_entrada + IaUso.tokens_saida))
        .where(IaUso.criado_em >= desde)
        .group_by(IaUso.operacao).order_by(func.sum(IaUso.custo_usd).desc())).all()]

    por_mes = [{
        "mes": m.strftime("%m/%Y") if m else "—", "chamadas": n,
        "custo": float(c or 0),
    } for m, n, c in s.execute(
        select(func.date_trunc("month", IaUso.criado_em).label("m"),
               func.count(IaUso.id), func.sum(IaUso.custo_usd))
        .where(IaUso.criado_em >= desde)
        .group_by("m").order_by("m")).all()]

    por_modelo = [{
        "modelo": mod, "chamadas": n, "custo": float(c or 0),
        "preco_conhecido": preco_conhecido(mod),
    } for mod, n, c in s.execute(
        select(IaUso.modelo, func.count(IaUso.id), func.sum(IaUso.custo_usd))
        .where(IaUso.criado_em >= desde)
        .group_by(IaUso.modelo).order_by(func.sum(IaUso.custo_usd).desc())).all()]

    por_pessoa = [{
        "pessoa": nome or "—", "chamadas": n, "custo": float(c or 0),
    } for nome, n, c in s.execute(
        select(Usuario.nome, func.count(IaUso.id), func.sum(IaUso.custo_usd))
        .join(Usuario, Usuario.id == IaUso.usuario_id, isouter=True)
        .where(IaUso.criado_em >= desde)
        .group_by(Usuario.nome).order_by(func.sum(IaUso.custo_usd).desc()).limit(15)).all()]

    hoje = date.today()
    sit = situacao_teto(s, hoje)
    dias_corridos = max(1, hoje.day)
    projecao = sit["gasto"] / dias_corridos * 30

    return {
        "periodo_dias": dias,
        "chamadas": total[0] or 0, "custo_total": float(total[1] or 0),
        "tokens": int(total[2] or 0), "duracao_media_ms": int(total[3] or 0),
        "falhas": total[4] or 0,
        "custo_mes_atual": sit["gasto"],
        "projecao_mes": round(projecao, 2),
        "teto_mensal": sit["teto"], "percentual_teto": sit["percentual"],
        "alerta_teto": sit["alerta"], "limiar_aviso": LIMIAR_AVISO,
        "por_operacao": por_operacao, "por_mes": por_mes,
        "por_modelo": por_modelo, "por_pessoa": por_pessoa,
        "precos": {m: [float(a), float(b)] for m, (a, b) in PRECOS.items()},
        "preco_padrao": [float(PRECO_PADRAO[0]), float(PRECO_PADRAO[1])],
    }
