# ============================================================================
# ERP — core/perguntas/respostas.py
# As perguntas do catálogo respondidas por CÓDIGO.
#
# POR QUE ISTO EXISTE, E POR QUE NÃO É A IA QUE FAZ A CONTA.
# O dono quer perguntar qualquer coisa ao sistema. O caminho fácil seria mandar
# a IA escrever a consulta na hora — e ele acerta quase sempre, e erra EM
# SILÊNCIO no resto. Número errado com cara de certo é pior que resposta
# nenhuma, e quem lê não tem como conferir.
#
# Então a pergunta PREVISTA — a que se sabe que vai ser feita — é respondida
# aqui: escrita, testada, exata, instantânea e sem custo de IA. À IA sobra
# escolher QUAL função chamar. A conta é sempre do sistema.
#
# TRÊS REGRAS QUE VALEM PARA TODA RESPOSTA DESTE ARQUIVO:
#
#   1. PASSA PELO MESMO ESCOPO DAS TELAS. Nenhuma consulta aqui é escrita à
#      mão: todas partem de `consulta_de_titulos(..., usuario=...)`, que aplica
#      `aplicar_escopo`. É o que garante que o administrativo de uma obra não
#      veja título de outra pelo assistente — e que, se a regra de escopo
#      mudar, mude num lugar só.
#   2. TODA RESPOSTA DIZ DE ONDE VEIO. Cada uma devolve `de_onde_veio`, com a
#      tela e o filtro que reproduzem o número. Sem isso o dono não tem como
#      auditar, e na primeira resposta errada para de confiar em todas.
#   3. QUANDO A PERGUNTA TEM MAIS DE UMA LEITURA, MOSTRA AS DUAS. Não se
#      escolhe por ele em silêncio.
# ============================================================================
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.titulos.service import consulta_de_titulos
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import (
    Anexo, Parcela, StatusParcela, StatusTitulo, Titulo,
)

# Situações em que o título ainda representa dinheiro que vai sair. Cancelado e
# pago total ficam de fora — e "bloqueado" fica DENTRO de propósito: ele é uma
# obrigação que existe e está travada, não uma que sumiu.
EM_ABERTO = (StatusTitulo.EM_ANALISE, StatusTitulo.AGUARDANDO_AVAL,
             StatusTitulo.AGUARDANDO_APROVACAO, StatusTitulo.APROVADO,
             StatusTitulo.BLOQUEADO, StatusTitulo.PAGO_PARCIAL)

# Onde a fila pode parar, e de quem é a vez em cada caso. É o que transforma
# "23 títulos parados" em "23 parados, e 15 esperando VOCÊ".
DE_QUEM_E_A_VEZ = {
    StatusTitulo.EM_ANALISE: "de quem lançou — falta completar ou corrigir",
    StatusTitulo.AGUARDANDO_AVAL: "do supervisor ou gestor da obra (1º aval)",
    StatusTitulo.AGUARDANDO_APROVACAO: "de quem aprova",
    StatusTitulo.BLOQUEADO: "de quem bloqueou — precisa destravar ou cancelar",
}


def _dec(v: Any) -> Decimal:
    return Decimal(str(v or 0))


def _reais(v: Any) -> str:
    """R$ 1.234,50 — e SÓ o número.

    Nasceu de um defeito que a tela mostrou: a troca de ponto por vírgula
    estava sendo aplicada à FRASE INTEIRA, e não ao número, então "a pagar de
    01/09 a 31/12, somando" virava "31/12. somando". Formatar dinheiro num
    lugar só resolve para sempre.
    """
    return f"R$ {_dec(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# Como cada situação se chama para uma pessoa. Sem isto a frase sai
# "aguardando aprovacao", que é o banco falando (sem acento, com sublinhado).
SITUACAO_EM_PORTUGUES = {
    StatusTitulo.EM_ANALISE: "em análise",
    StatusTitulo.AGUARDANDO_AVAL: "esperando o 1º aval",
    StatusTitulo.AGUARDANDO_APROVACAO: "esperando aprovação",
    StatusTitulo.APROVADO: "aprovado",
    StatusTitulo.BLOQUEADO: "bloqueado",
    StatusTitulo.PAGO_PARCIAL: "pago em parte",
    StatusTitulo.PAGO: "pago",
}


def situacao_em_portugues(status: Any) -> str:
    return SITUACAO_EM_PORTUGUES.get(status, getattr(status, "value", "") or "")


def _obras_do_titulo(t: Titulo) -> str:
    codigos = []
    for r in (t.rateios or []):
        obra = getattr(r, "obra", None)
        codigo = getattr(obra, "codigo", None)
        if codigo and codigo not in codigos:
            codigos.append(codigo)
    return ", ".join(codigos)


def _linha(t: Titulo, **extra: Any) -> dict[str, Any]:
    fornecedor = getattr(t, "fornecedor", None)
    linha = {
        "titulo_id": t.id,
        "numero_sp": t.numero_sp or "",
        "credor": getattr(fornecedor, "razao_social", "") or "",
        "descricao": (t.descricao or "")[:120],
        "obra": _obras_do_titulo(t),
        "situacao": situacao_em_portugues(t.status),
    }
    linha.update(extra)
    return linha


def _resposta(*, titulo: str, frase: str, linhas: list[dict[str, Any]],
              colunas: list[tuple[str, str]], de_onde_veio: dict[str, Any],
              total: Optional[Decimal] = None,
              observacao: str = "") -> dict[str, Any]:
    return {
        "titulo": titulo,
        "frase": frase,
        "quantas": len(linhas),
        "total": float(total) if total is not None else None,
        "colunas": [{"chave": c, "rotulo": r} for c, r in colunas],
        "linhas": linhas,
        "de_onde_veio": de_onde_veio,
        "observacao": observacao,
    }


def _abertas_do_titulo(t: Titulo) -> list[Parcela]:
    return [p for p in (t.parcelas or [])
            if p.status in (StatusParcela.ABERTA, StatusParcela.AGENDADA)]


# ---------------------------------------------------------------------------
# "O que tem a pagar hoje?" / "…esta semana?" / "…na obra tal?"
# ---------------------------------------------------------------------------
def a_pagar_no_periodo(s: Session, usuario: Usuario, *,
                       de: Optional[date] = None, ate: Optional[date] = None,
                       obra: str = "") -> dict[str, Any]:
    """O que vence no período e ainda não foi pago.

    POR VENCIMENTO, e a resposta diz isso na cara. "A pagar" também pode
    significar "por competência" — outro número, igualmente legítimo —, e
    trocar um pelo outro sem avisar é o tipo de coisa que faz dois relatórios
    discordarem sem ninguém descobrir por quê.
    """
    hoje = date.today()
    inicio = de or hoje
    fim = ate or inicio
    if inicio > fim:
        inicio, fim = fim, inicio

    stmt = consulta_de_titulos(s, status=list(EM_ABERTO), usuario=usuario)
    linhas, total = [], Decimal(0)
    for t in s.scalars(stmt).all():
        obras = _obras_do_titulo(t)
        if obra and obra.lower() not in obras.lower():
            continue
        for p in _abertas_do_titulo(t):
            if p.vencimento is None or not (inicio <= p.vencimento <= fim):
                continue
            total += _dec(p.valor)
            linhas.append(_linha(
                t, vencimento=p.vencimento.isoformat(),
                parcela=f"{p.numero}/{len(t.parcelas or [])}",
                valor=float(_dec(p.valor))))
    linhas.sort(key=lambda l: (l["vencimento"], l["credor"]))

    quando = ("hoje" if inicio == fim == hoje
              else f"de {inicio.strftime('%d/%m')} a {fim.strftime('%d/%m')}")
    onde = f" na obra {obra}" if obra else ""
    frase = (f"Nada a pagar {quando}{onde}." if not linhas else
             f"{len(linhas)} parcela(s) a pagar {quando}{onde}, "
             f"somando {_reais(total)}.")
    return _resposta(
        titulo=f"A pagar {quando}{onde}", frase=frase, linhas=linhas, total=total,
        colunas=[("vencimento", "Vence"), ("numero_sp", "SP"), ("credor", "Credor"),
                 ("descricao", "Descrição"), ("obra", "Obra"),
                 ("parcela", "Parcela"), ("situacao", "Situação"),
                 ("valor", "Valor")],
        de_onde_veio={"tela": "/erp/pagamentos",
                      "explicacao": "Agenda de pagamentos, no mesmo período."},
        observacao=("Conta pelo VENCIMENTO da parcela e inclui o que está "
                    "bloqueado — que é obrigação travada, não obrigação que "
                    "sumiu. Título cancelado e parcela já paga ficam de fora."))


# ---------------------------------------------------------------------------
# "O que está vencido e não foi pago?"
# ---------------------------------------------------------------------------
def vencidos_sem_pagar(s: Session, usuario: Usuario, *,
                       obra: str = "") -> dict[str, Any]:
    """O que já passou do vencimento e continua aberto, com o atraso em dias."""
    hoje = date.today()
    stmt = consulta_de_titulos(s, status=list(EM_ABERTO), usuario=usuario)
    linhas, total = [], Decimal(0)
    for t in s.scalars(stmt).all():
        obras = _obras_do_titulo(t)
        if obra and obra.lower() not in obras.lower():
            continue
        for p in _abertas_do_titulo(t):
            if p.vencimento is None or p.vencimento >= hoje:
                continue
            total += _dec(p.valor)
            linhas.append(_linha(
                t, vencimento=p.vencimento.isoformat(),
                atraso=(hoje - p.vencimento).days,
                valor=float(_dec(p.valor))))
    linhas.sort(key=lambda l: -l["atraso"])

    onde = f" na obra {obra}" if obra else ""
    frase = (f"Nada vencido em aberto{onde}." if not linhas else
             f"{len(linhas)} parcela(s) vencida(s){onde}, "
             f"somando {_reais(total)} — a mais antiga há "
             f"{linhas[0]['atraso']} dia(s).")
    return _resposta(
        titulo=f"Vencido e não pago{onde}", frase=frase, linhas=linhas, total=total,
        colunas=[("atraso", "Dias de atraso"), ("vencimento", "Venceu em"),
                 ("numero_sp", "SP"), ("credor", "Credor"),
                 ("descricao", "Descrição"), ("obra", "Obra"),
                 ("situacao", "Situação"), ("valor", "Valor")],
        de_onde_veio={"tela": "/erp/pagamentos",
                      "explicacao": "Agenda de pagamentos, filtrando o vencido."},
        observacao=("Parcela paga em parte continua aparecendo pelo valor "
                    "cheio da parcela — o que falta pagar está na ficha do "
                    "título."))


# ---------------------------------------------------------------------------
# "Onde a fila está parada?"  ← o pedido que ficou de fora do relatório de uso
# ---------------------------------------------------------------------------
def esperando_decisao(s: Session, usuario: Usuario) -> dict[str, Any]:
    """O que está parado esperando alguém decidir, e de quem é a vez.

    Era o pedaço que faltava do relatório de trabalho: ele mostra o que as
    pessoas FIZERAM; este mostra o que está parado esperando que façam.
    """
    parados = [StatusTitulo.EM_ANALISE, StatusTitulo.AGUARDANDO_AVAL,
               StatusTitulo.AGUARDANDO_APROVACAO, StatusTitulo.BLOQUEADO]
    stmt = consulta_de_titulos(s, status=parados, usuario=usuario)
    hoje = date.today()
    linhas, total = [], Decimal(0)
    por_situacao: dict[str, int] = {}
    for t in s.scalars(stmt).all():
        valor = _dec(t.valor_liquido or t.valor_bruto)
        total += valor
        situacao = situacao_em_portugues(t.status)
        por_situacao[situacao] = por_situacao.get(situacao, 0) + 1
        parado_desde = t.criado_em.date() if t.criado_em else None
        linhas.append(_linha(
            t, de_quem="—" if t.status is None else DE_QUEM_E_A_VEZ.get(t.status, "—"),
            parado_ha=(hoje - parado_desde).days if parado_desde else None,
            valor=float(valor)))
    linhas.sort(key=lambda l: -(l["parado_ha"] or 0))

    if not linhas:
        frase = "Nada parado esperando decisão."
    else:
        resumo = ", ".join(f"{q} {sit}" for sit, q in sorted(por_situacao.items()))
        frase = (f"{len(linhas)} título(s) parado(s) esperando decisão "
                 f"({resumo}), somando {_reais(total)}. O mais antigo está "
                 f"parado há {linhas[0]['parado_ha']} dia(s).")
    return _resposta(
        titulo="Parado esperando decisão", frase=frase, linhas=linhas, total=total,
        colunas=[("parado_ha", "Parado há (dias)"), ("situacao", "Situação"),
                 ("de_quem", "De quem é a vez"), ("numero_sp", "SP"),
                 ("credor", "Credor"), ("descricao", "Descrição"),
                 ("obra", "Obra"), ("valor", "Valor")],
        de_onde_veio={"tela": "/erp/titulos",
                      "explicacao": "Títulos, filtrando pelas situações em espera."},
        observacao=("O tempo parado é contado desde que o título foi LANÇADO, "
                    "não desde a última mexida — é o que interessa para saber "
                    "há quanto tempo o fornecedor espera."))


# ---------------------------------------------------------------------------
# "Quais títulos estão sem documento anexado?"
# ---------------------------------------------------------------------------
def sem_documento(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Títulos em aberto que não têm nenhum arquivo anexado.

    Título sem documento é o que trava a conferência do contador e o que
    aparece na auditoria meses depois, quando ninguém lembra do que era.
    """
    stmt = consulta_de_titulos(s, status=list(EM_ABERTO), usuario=usuario)
    titulos = list(s.scalars(stmt).all())
    if not titulos:
        return _resposta(
            titulo="Sem documento anexado",
            frase="Nenhum título em aberto.", linhas=[], colunas=[],
            de_onde_veio={"tela": "/erp/titulos", "explicacao": "Títulos em aberto."})

    com_anexo = set(s.scalars(
        select(Anexo.entidade_id).where(
            Anexo.entidade_tipo == "titulo",
            Anexo.entidade_id.in_([t.id for t in titulos]))).all())

    linhas, total = [], Decimal(0)
    for t in titulos:
        if t.id in com_anexo:
            continue
        valor = _dec(t.valor_liquido or t.valor_bruto)
        total += valor
        linhas.append(_linha(
            t, competencia=t.competencia.strftime("%m/%Y") if t.competencia else "",
            valor=float(valor)))
    linhas.sort(key=lambda l: l["numero_sp"])

    frase = ("Todo título em aberto tem documento anexado." if not linhas else
             f"{len(linhas)} título(s) em aberto sem documento anexado, "
             f"somando {_reais(total)}.")
    return _resposta(
        titulo="Sem documento anexado", frase=frase, linhas=linhas, total=total,
        colunas=[("numero_sp", "SP"), ("credor", "Credor"),
                 ("descricao", "Descrição"), ("obra", "Obra"),
                 ("competencia", "Competência"), ("situacao", "Situação"),
                 ("valor", "Valor")],
        de_onde_veio={"tela": "/erp/titulos",
                      "explicacao": "Títulos — abra a ficha e veja a aba de anexos."},
        observacao=("Conta só o que está em aberto. Título já pago sem "
                    "documento é outro problema, e outra pergunta."))


# ---------------------------------------------------------------------------
# "O que venceu, o que vence hoje e o que vence esta semana?"
# ---------------------------------------------------------------------------
def panorama_de_vencimentos(s: Session, usuario: Usuario) -> dict[str, Any]:
    """As três janelas de uma vez — vencido, hoje, e os próximos sete dias.

    Vem junto de propósito: um número solto de "a pagar" não diz se a situação
    está sob controle. Vencido subindo com a semana leve é um problema
    diferente de semana pesada com nada vencido.
    """
    hoje = date.today()
    stmt = consulta_de_titulos(s, status=list(EM_ABERTO), usuario=usuario)
    faixas = {"vencido": Decimal(0), "hoje": Decimal(0), "semana": Decimal(0)}
    contas = {"vencido": 0, "hoje": 0, "semana": 0}
    for t in s.scalars(stmt).all():
        for p in _abertas_do_titulo(t):
            if p.vencimento is None:
                continue
            if p.vencimento < hoje:
                faixa = "vencido"
            elif p.vencimento == hoje:
                faixa = "hoje"
            elif p.vencimento <= hoje + timedelta(days=7):
                faixa = "semana"
            else:
                continue
            faixas[faixa] += _dec(p.valor)
            contas[faixa] += 1

    linhas = [
        {"faixa": "Já venceu e está em aberto", "parcelas": contas["vencido"],
         "valor": float(faixas["vencido"])},
        {"faixa": "Vence hoje", "parcelas": contas["hoje"],
         "valor": float(faixas["hoje"])},
        {"faixa": "Vence nos próximos 7 dias", "parcelas": contas["semana"],
         "valor": float(faixas["semana"])},
    ]
    total = sum(faixas.values())
    frase = (f"Vencido: {_reais(faixas['vencido'])} · Hoje: "
             f"{_reais(faixas['hoje'])} · Próximos 7 dias: "
             f"{_reais(faixas['semana'])}.")
    return _resposta(
        titulo="Panorama de vencimentos", frase=frase, linhas=linhas, total=total,
        colunas=[("faixa", "Quando"), ("parcelas", "Parcelas"), ("valor", "Valor")],
        de_onde_veio={"tela": "/erp/pagamentos",
                      "explicacao": "Agenda de pagamentos."},
        observacao=("As três faixas não se sobrepõem, e o total é a soma "
                    "delas. Inclui o que está bloqueado."))
