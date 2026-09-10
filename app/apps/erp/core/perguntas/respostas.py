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


def _casa(procurado: str, texto: Any) -> bool:
    """Procura um pedaço de texto dentro de outro, SEM ligar para acento nem
    para maiúscula.

    Existe porque ninguém digita acento numa busca: quem procura a categoria
    "Hidráulico" escreve "hidra". A primeira versão comparava direto e não
    achava nada — e "nenhum insumo nessa categoria" sobre uma categoria cheia
    é o pior tipo de resposta errada, porque parece certa.
    """
    import unicodedata

    def limpo(v: Any) -> str:
        bruto = unicodedata.normalize("NFKD", str(v or "").strip().lower())
        return "".join(c for c in bruto if not unicodedata.combining(c))

    alvo = limpo(procurado)
    return bool(alvo) and alvo in limpo(texto)


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


# Quantas linhas a resposta mostra. A conta é sempre feita sobre TUDO — o que
# o teto corta é só o que viaja para a tela. A base de insumos tem 3.285 itens:
# devolver todos travaria o navegador, e ninguém lê 3.285 linhas de qualquer
# jeito. A resposta sempre diz quantas existem de verdade.
TETO_DE_LINHAS = 300


def _resposta(*, titulo: str, frase: str, linhas: list[dict[str, Any]],
              colunas: list[tuple[str, str]], de_onde_veio: dict[str, Any],
              total: Optional[Decimal] = None,
              observacao: str = "") -> dict[str, Any]:
    quantas = len(linhas)
    mostradas = linhas[:TETO_DE_LINHAS]
    if quantas > TETO_DE_LINHAS:
        corte = (f"Mostrando as primeiras {TETO_DE_LINHAS} de {quantas} linhas. "
                 f"O número acima é sobre TODAS — o corte é só do que aparece "
                 f"na tela. Use a tela de origem para ver a lista inteira.")
        observacao = f"{observacao} {corte}".strip()
    return {
        "titulo": titulo,
        "frase": frase,
        "quantas": quantas,
        "mostradas": len(mostradas),
        "total": float(total) if total is not None else None,
        "colunas": [{"chave": c, "rotulo": r} for c, r in colunas],
        "linhas": mostradas,
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
        if obra and not _casa(obra, obras):
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
        if obra and not _casa(obra, obras):
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


# ===========================================================================
# CONTRATOS — "quanto falta receber", nas quatro leituras que o dono definiu
#
# Estas respostas vivem sob a ação `ver_contratos`, e por isso ficam numa ROTA
# SEPARADA da do financeiro. A ação é deliberadamente estreita: o quadro mostra
# o contrato de ponta a ponta, e não há como recortá-lo por obra designada sem
# mentir no total — quem é preso a obra ou a autoria fica de fora.
# ===========================================================================
def _reguas_dos_contratos(s: Session, obra: str = "") -> list[dict[str, Any]]:
    """A régua de cada contrato, usando a MESMA aritmética do quadro da tela.

    Reusar `quadro()` em vez de somar de novo é o que impede o número da
    pergunta e o da tela divergirem — já aconteceu antes neste arquivo do
    contrato, com o reajuste, e quem vê dois números diferentes sobre a mesma
    coisa perde a confiança nos dois.
    """
    from app.apps.erp.core.titulos import quadro as svc_quadro

    reguas = []
    for resumo in svc_quadro.listar_contratos(s):
        if obra and not _casa(obra, resumo.get("obra")):
            continue
        q = svc_quadro.quadro(s, resumo["id"])
        t = q["totais"]
        reguas.append({
            "contrato_id": resumo["id"],
            "obra": q["contrato"]["obra"] or "—",
            "objeto": (q["contrato"]["objeto"] or "")[:80],
            "vigente": t["vigente"],
            "medido": t["medido"],
            "faturado": t["faturado"],
            "recebido": t["recebido"],
            "falta_receber_do_contrato": t["falta_receber_do_contrato"],
            "falta_receber_do_medido": t["falta_receber_do_medido"],
            "falta_faturar_do_medido": t["falta_faturar_do_medido"],
            "a_receber": t["a_receber"],
            "medido_sem_nota": len(q["pendencias"]["medido_sem_nota"]),
            "faturado_sem_receber": len(q["pendencias"]["faturado_sem_receber"]),
        })
    return reguas


def falta_receber(s: Session, usuario: Usuario, *, obra: str = "") -> dict[str, Any]:
    """A RÉGUA INTEIRA, e não um número solto.

    O dono desfez esta pergunta ele mesmo, em 10/09/2026, e mostrou que ela tem
    quatro leituras — todas legítimas, e todas etapas de uma mesma esteira:

        CONTRATO (+aditivos) → MEDIDO → FATURADO (nota emitida) → RECEBIDO

    Em vez de escolher uma e responder um número (que estaria certo para uma
    leitura e errado para as outras três), a resposta mostra as quatro juntas.
    Assim a leitura que ele queria já está na tela, e ele não precisou ter
    acertado a pergunta.
    """
    reguas = _reguas_dos_contratos(s, obra)
    if not reguas:
        onde = f" na obra {obra}" if obra else ""
        return _resposta(
            titulo=f"Quanto falta receber{onde}",
            frase=f"Nenhum contrato encontrado{onde}.", linhas=[], colunas=[],
            de_onde_veio={"tela": "/erp/contratos",
                          "explicacao": "Quadro financeiro dos contratos."})

    somas = {c: sum(r[c] for r in reguas) for c in
             ("vigente", "medido", "faturado", "recebido",
              "falta_receber_do_contrato", "falta_receber_do_medido",
              "falta_faturar_do_medido", "a_receber")}
    onde = f" na obra {obra}" if obra else f" ({len(reguas)} contrato(s))"
    frase = (
        f"Falta receber{onde}: {_reais(somas['falta_receber_do_contrato'])} "
        f"olhando o CONTRATO inteiro · "
        f"{_reais(somas['falta_receber_do_medido'])} olhando só o que já foi "
        f"MEDIDO · {_reais(somas['a_receber'])} olhando só o que já tem NOTA.")
    return _resposta(
        titulo=f"Quanto falta receber{onde}", frase=frase, linhas=reguas,
        total=somas["falta_receber_do_contrato"],
        colunas=[("obra", "Obra"), ("objeto", "Objeto"),
                 ("vigente", "Contrato vigente"), ("medido", "Medido"),
                 ("faturado", "Faturado"), ("recebido", "Recebido"),
                 ("falta_receber_do_contrato", "Falta receber (do contrato)"),
                 ("falta_receber_do_medido", "Falta receber (do medido)"),
                 ("a_receber", "Falta receber (do faturado)")],
        de_onde_veio={"tela": "/erp/contratos",
                      "explicacao": "Quadro financeiro do contrato, contrato a contrato."},
        observacao=(
            "São TRÊS leituras da mesma esteira — contrato → medido → faturado "
            "→ recebido — e as três estão certas: elas respondem a perguntas "
            "diferentes. A do CONTRATO inclui o que ainda nem foi executado; a "
            "do MEDIDO, só o que já foi feito; a do FATURADO, só o que já tem "
            "nota emitida e virou cobrança."))


def medido_sem_nota(s: Session, usuario: Usuario, *,
                    obra: str = "") -> dict[str, Any]:
    """O que já foi medido e ainda não virou nota — dinheiro parado na porta.

    É a etapa da esteira que mais custa caro quando fica esquecida: o serviço
    foi feito, o custo já saiu, e a cobrança nem começou.
    """
    reguas = [r for r in _reguas_dos_contratos(s, obra)
              if r["falta_faturar_do_medido"] > 0.01 or r["medido_sem_nota"]]
    total = sum(r["falta_faturar_do_medido"] for r in reguas)
    onde = f" na obra {obra}" if obra else ""
    frase = (f"Tudo que foi medido{onde} já virou nota." if not reguas else
             f"{_reais(total)} medido{onde} e ainda sem nota emitida, em "
             f"{len(reguas)} contrato(s).")
    return _resposta(
        titulo=f"Medido e ainda sem nota{onde}", frase=frase, linhas=reguas,
        total=total,
        colunas=[("obra", "Obra"), ("objeto", "Objeto"), ("medido", "Medido"),
                 ("faturado", "Faturado"),
                 ("falta_faturar_do_medido", "Falta faturar"),
                 ("medido_sem_nota", "Medições sem nota")],
        de_onde_veio={"tela": "/erp/contratos",
                      "explicacao": "Quadro do contrato, quadro 'pendências'."},
        observacao=("O serviço já foi executado e o custo já saiu; o que falta "
                    "é emitir a nota para poder cobrar."))


def faturado_sem_receber(s: Session, usuario: Usuario, *,
                         obra: str = "") -> dict[str, Any]:
    """O que já tem nota emitida e ainda não entrou na conta."""
    reguas = [r for r in _reguas_dos_contratos(s, obra) if r["a_receber"] > 0.01]
    total = sum(r["a_receber"] for r in reguas)
    onde = f" na obra {obra}" if obra else ""
    frase = (f"Toda nota emitida{onde} já foi recebida." if not reguas else
             f"{_reais(total)} faturado{onde} e ainda não recebido, em "
             f"{len(reguas)} contrato(s).")
    return _resposta(
        titulo=f"Faturado e ainda não recebido{onde}", frase=frase,
        linhas=reguas, total=total,
        colunas=[("obra", "Obra"), ("objeto", "Objeto"),
                 ("faturado", "Faturado"), ("recebido", "Recebido"),
                 ("a_receber", "Falta receber"),
                 ("faturado_sem_receber", "Medições em aberto")],
        de_onde_veio={"tela": "/erp/contratos",
                      "explicacao": "Quadro do contrato, quadro 'pendências'."},
        observacao=("Dinheiro recebido A MAIS do que foi faturado não aparece "
                    "aqui como negativo: é outra coisa (entrada sem nota) e a "
                    "tela do contrato mostra em linha própria."))


# ===========================================================================
# SUPRIMENTOS — o catálogo de insumos e a fila de pedidos de material
#
# Grupo próprio, sob a ação `ver_suprimentos`. Duas naturezas convivem aqui, e
# vale saber a diferença: o CATÁLOGO (insumos, categorias, preços) é cadastro
# da empresa e não se recorta por obra — quem pode ver Suprimentos vê o
# catálogo inteiro. Já a FILA DE PEDIDOS é da obra, e passa pelo mesmo filtro
# por pessoa que a tela de Solicitações usa (`solicitacao.listar_itens`).
# ===========================================================================
def insumos_da_categoria(s: Session, usuario: Usuario, *,
                         categoria: str = "") -> dict[str, Any]:
    """A lista de insumos de uma categoria de suprimento.

    Foi pedida com estas palavras: *"me manda uma lista dos insumos cadastrados
    na categoria tal"*. Sem categoria dita, responde o catálogo inteiro com a
    contagem por categoria — que é o que serve para escolher qual pedir.
    """
    from app.apps.erp.core.suprimentos import cadastro as svc_cadastro

    dados = svc_cadastro.gerenciar_insumos(s)
    ativos = [i for i in dados["insumos"] if i["ativo"]]
    alvo = (categoria or "").strip()
    if alvo:
        escolhidos = [i for i in ativos if _casa(alvo, i["categoria_insumo"])]
    else:
        escolhidos = ativos

    linhas = [{
        "codigo": i["codigo"], "descricao": i["descricao"],
        "categoria_insumo": i["categoria_insumo"] or "—",
        "unidade": i["unidade"] or "—",
        "conta": i["conta"] or "—",
        "locavel": "sim" if i["locavel"] else "",
        "ultimo_preco": i["ultimo_preco"],
    } for i in escolhidos]

    if alvo and not linhas:
        nomes = sorted({i["categoria_insumo"] for i in ativos if i["categoria_insumo"]})
        parecidas = [n for n in nomes if _casa(alvo[:4], n)][:6]
        dica = (f" Categorias parecidas: {', '.join(parecidas)}." if parecidas
                else f" São {len(nomes)} categorias cadastradas.")
        frase = f"Nenhum insumo na categoria '{alvo}'.{dica}"
    elif alvo:
        frase = f"{len(linhas)} insumo(s) na categoria '{alvo}'."
    else:
        quantas_cat = len({i["categoria_insumo"] for i in ativos if i["categoria_insumo"]})
        frase = (f"{len(linhas)} insumo(s) ativo(s) no catálogo, em "
                 f"{quantas_cat} categoria(s).")
    return _resposta(
        titulo=(f"Insumos da categoria {alvo}" if alvo else "Catálogo de insumos"),
        frase=frase, linhas=linhas,
        colunas=[("codigo", "Código"), ("descricao", "Insumo"),
                 ("categoria_insumo", "Categoria"), ("unidade", "Unidade"),
                 ("conta", "Conta do plano"), ("locavel", "Locável"),
                 ("ultimo_preco", "Último preço")],
        de_onde_veio={"tela": "/erp/suprimentos/insumos",
                      "explicacao": "Suprimentos › Cadastros › Insumos."},
        observacao=("O catálogo é cadastro da empresa e não se divide por obra: "
                    "quem enxerga Suprimentos enxerga o catálogo inteiro."))


def insumos_sem_conta_do_plano(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Insumos que não apontam para uma conta do plano financeiro.

    Importa porque é a conta do plano que faz o pedido de compra virar previsão
    de pagamento já apropriada. Sem ela, a compra chega no financeiro sem saber
    em que custo entra.
    """
    from app.apps.erp.core.suprimentos import cadastro as svc_cadastro

    dados = svc_cadastro.gerenciar_insumos(s)
    linhas = [{
        "codigo": i["codigo"], "descricao": i["descricao"],
        "categoria_insumo": i["categoria_insumo"] or "—",
        "unidade": i["unidade"] or "—",
    } for i in dados["insumos"] if i["ativo"] and not i["categoria_id"]]

    frase = ("Todo insumo ativo tem conta do plano." if not linhas else
             f"{len(linhas)} insumo(s) ativo(s) sem conta do plano financeiro.")
    return _resposta(
        titulo="Insumos sem conta do plano", frase=frase, linhas=linhas,
        colunas=[("codigo", "Código"), ("descricao", "Insumo"),
                 ("categoria_insumo", "Categoria"), ("unidade", "Unidade")],
        de_onde_veio={"tela": "/erp/suprimentos/insumos",
                      "explicacao": "Suprimentos › Insumos, filtro 'sem conta do plano'."},
        observacao=("É a conta do plano que faz o pedido virar previsão de "
                    "pagamento já apropriada. Sem ela, a compra chega no "
                    "financeiro sem saber em que custo entra."))


def pedidos_de_material_pendentes(s: Session, usuario: Usuario, *,
                                  obra: str = "") -> dict[str, Any]:
    """O que a obra pediu e ainda não foi resolvido.

    Passa pelo `listar_itens` da tela de Solicitações, que já filtra por
    pessoa: quem é preso a uma obra vê os pedidos daquela obra, e mais nada.
    """
    from app.apps.erp.core.suprimentos import solicitacao as svc_sol

    itens = svc_sol.listar_itens(s, usuario)
    abertos = [i for i in itens
               if (i.get("status") or "").upper() not in
               ("ATENDIDO", "CANCELADO", "RECUSADO", "COMPRADO", "RECEBIDO")]
    alvo = (obra or "").strip()
    if alvo:
        abertos = [i for i in abertos
                   if _casa(alvo, i.get("obra_codigo") or i.get("obra"))]

    linhas = [{
        "solicitacao": i.get("solicitacao") or "",
        "insumo": i.get("insumo") or "",
        "especificacao": (i.get("especificacao") or "")[:80],
        "quantidade": i.get("quantidade"),
        "obra": i.get("obra_codigo") or i.get("obra") or "—",
        "prioridade": i.get("prioridade") or "",
        "situacao": (i.get("status") or "").replace("_", " ").lower(),
        "previsao": i.get("previsao_entrega") or "",
    } for i in abertos]
    linhas.sort(key=lambda l: (l["obra"], l["solicitacao"]))

    onde = f" na obra {alvo}" if alvo else ""
    frase = (f"Nenhum pedido de material em aberto{onde}." if not linhas else
             f"{len(linhas)} item(ns) de material pedido(s) e ainda em "
             f"aberto{onde}.")
    return _resposta(
        titulo=f"Pedidos de material em aberto{onde}", frase=frase, linhas=linhas,
        colunas=[("solicitacao", "Solicitação"), ("insumo", "Insumo"),
                 ("especificacao", "Especificação"), ("quantidade", "Quantidade"),
                 ("obra", "Obra"), ("prioridade", "Prioridade"),
                 ("situacao", "Situação"), ("previsao", "Previsão")],
        de_onde_veio={"tela": "/erp/suprimentos",
                      "explicacao": "Suprimentos › Solicitações."},
        observacao=("Mostra o que ainda não foi atendido, comprado, recebido, "
                    "recusado nem cancelado. A lista respeita o alcance de quem "
                    "perguntou: quem é preso a uma obra vê só a dela."))


def preco_do_insumo(s: Session, usuario: Usuario, *,
                    insumo: str = "") -> dict[str, Any]:
    """O que já se pagou por um insumo, e quando.

    A pergunta prática é sempre a mesma: *"este preço que estão me cobrando
    está caro?"* — e ela só se responde olhando o que a própria empresa já
    pagou.
    """
    from app.apps.erp.core.suprimentos import cadastro as svc_cadastro

    alvo = (insumo or "").strip()
    if not alvo:
        return _resposta(
            titulo="Preço de um insumo",
            frase="Diga o nome do insumo (ou parte dele) para eu procurar.",
            linhas=[], colunas=[],
            de_onde_veio={"tela": "/erp/suprimentos/precos",
                          "explicacao": "Suprimentos › Banco de preços."})

    dados = svc_cadastro.gerenciar_insumos(s)
    achados = [i for i in dados["insumos"]
               if i["ativo"] and _casa(alvo, i["descricao"])]
    com_preco = [i for i in achados if i["ultimo_preco"] is not None]
    linhas = [{
        "codigo": i["codigo"], "descricao": i["descricao"],
        "unidade": i["unidade"] or "—",
        "ultimo_preco": i["ultimo_preco"],
        "ultimo_preco_em": i["ultimo_preco_em"] or "",
        "origem": i["ultimo_preco_origem"] or "",
        "categoria_insumo": i["categoria_insumo"] or "—",
    } for i in sorted(achados, key=lambda x: (x["ultimo_preco"] is None,
                                              x["descricao"]))]

    if not achados:
        frase = f"Nenhum insumo com '{alvo}' no nome."
    elif not com_preco:
        frase = (f"{len(achados)} insumo(s) com '{alvo}' no nome, mas nenhum "
                 f"tem preço registrado ainda.")
    else:
        frase = (f"{len(achados)} insumo(s) com '{alvo}' no nome; "
                 f"{len(com_preco)} com preço já registrado.")
    return _resposta(
        titulo=f"Preço de '{alvo}'", frase=frase, linhas=linhas,
        colunas=[("codigo", "Código"), ("descricao", "Insumo"),
                 ("unidade", "Unidade"), ("ultimo_preco", "Último preço"),
                 ("ultimo_preco_em", "Quando"), ("origem", "De onde veio"),
                 ("categoria_insumo", "Categoria")],
        de_onde_veio={"tela": "/erp/suprimentos/precos",
                      "explicacao": "Suprimentos › Banco de preços."},
        observacao=("É o ÚLTIMO preço registrado de cada insumo, não a média "
                    "nem o menor. Insumo sem preço nunca foi comprado pelo "
                    "sistema — ou a compra não passou pelo banco de preços."))
