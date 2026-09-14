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


# ---------------------------------------------------------------------------
# O QUE CADA COLUNA É — dito pelo SERVIDOR, não adivinhado pela tela.
#
# Na primeira versão a tela tinha a lista das colunas de dinheiro escrita
# dentro dela. Deu no que tinha de dar: "Já pago" e "Último preço" saíam como
# 4500.0, e a data de vencimento do seguro saía 2026-08-30 — formato de banco,
# não de gente. E o pior: pergunta NOVA nascia com o defeito calado, porque
# ninguém lembrava de ir na tela acrescentar o nome da coluna.
#
# Agora a resposta já diz o tipo de cada coluna, e a tela só obedece. Coluna
# nova sem tipo declarado é recusada por uma varredura da suíte — não dá para
# esquecer em silêncio.
#
# "numero" é contagem (dias, meses, parcelas): alinha à direita, sem R$.
# ---------------------------------------------------------------------------
TIPO_DA_COLUNA: dict[str, str] = {
    # dinheiro
    "a_receber": "dinheiro", "falta_faturar_do_medido": "dinheiro",
    "falta_receber_do_contrato": "dinheiro",
    "falta_receber_do_medido": "dinheiro", "faturado": "dinheiro",
    "faturado_sem_receber": "dinheiro", "medido": "dinheiro",
    "medido_sem_nota": "dinheiro", "pago_ate_agora": "dinheiro",
    "recebido": "dinheiro", "ultimo_preco": "dinheiro", "valor": "dinheiro",
    "valor_periodo": "dinheiro", "vigente": "dinheiro",
    # Custo da obra, nas duas visões decididas pelo dono em 12/09/2026.
    "comprometido": "dinheiro", "executado": "dinheiro",
    "a_executar": "dinheiro",
    # contagem
    "alertas": "numero", "atraso": "numero", "dias": "numero",
    "itens": "numero", "meses": "numero", "parado_ha": "numero",
    "parcelas": "numero", "quantidade": "numero", "quantos": "numero",
    # data
    "competencia": "data", "previsao": "data", "ultimo_preco_em": "data",
    "vence_em": "data", "venceu_em": "data", "vencimento": "data",
    # texto (declarado de propósito: o silêncio é que esconde defeito)
    "_": "texto", "apolice": "texto", "aviso": "texto",
    "documento": "texto", "de_quem": "texto", "trecho": "texto",
    "tipo": "texto",
    "categoria_insumo": "texto", "cliente": "texto", "codigo": "texto",
    "conta": "texto", "contrato": "texto", "credor": "texto",
    "de_quem": "texto", "descricao": "texto", "especificacao": "texto",
    "faixa": "texto", "falta": "texto", "fase": "texto",
    "gravidade": "texto", "insumo": "texto", "locadora": "texto",
    "locavel": "texto", "nome": "texto", "numero_sp": "texto",
    "objeto": "texto", "obra": "texto", "origem": "texto",
    "parcela": "texto", "periodicidade": "texto", "prioridade": "texto",
    "situacao": "texto", "solicitacao": "texto", "unidade": "texto",
}


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
        "colunas": [{"chave": c, "rotulo": r,
                     "tipo": TIPO_DA_COLUNA.get(c, "texto")}
                    for c, r in colunas],
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


# ===========================================================================
# LOCAÇÕES — o que está em obra, quanto custa por mês, e o que já passou da hora
#
# Vive no grupo de suprimentos (a tela de Locações mora lá), e reusa
# `locacoes.listar(s, usuario, ...)`, que já filtra por obra designada. Todo o
# cálculo pesado — quanto se pagou, há quantos meses está locado, quais
# alertas — já existe ali e NÃO é refeito aqui: refazer seria inventar um
# segundo número sobre a mesma coisa.
# ===========================================================================
def equipamentos_locados(s: Session, usuario: Usuario, *,
                         obra: str = "") -> dict[str, Any]:
    """O que está locado agora, em qual obra e quanto custa por período."""
    from app.apps.erp.core import locacoes as svc_loc

    contratos = svc_loc.listar(s, usuario, apenas_ativos=True)
    if obra:
        contratos = [c for c in contratos if _casa(obra, c.get("obra"))]

    linhas = [{
        "contrato": c.get("numero") or "",
        "locadora": c.get("locadora") or "",
        "obra": c.get("obra") or "—",
        "itens": c.get("itens") or 0,
        "valor_periodo": c.get("valor_periodo") or 0,
        "periodicidade": c.get("periodicidade") or "",
        "meses": c.get("meses") or 0,
        "pago_ate_agora": c.get("pago_ate_agora") or 0,
        "alertas": len(c.get("alertas") or []),
    } for c in contratos]
    linhas.sort(key=lambda l: (l["obra"], l["contrato"]))
    total = sum(Decimal(str(l["valor_periodo"])) for l in linhas)
    itens = sum(l["itens"] for l in linhas)

    onde = f" na obra {obra}" if obra else ""
    frase = (f"Nenhum equipamento locado{onde}." if not linhas else
             f"{itens} equipamento(s) em {len(linhas)} contrato(s) ativo(s)"
             f"{onde}, custando {_reais(total)} por período.")
    return _resposta(
        titulo=f"Equipamentos locados{onde}", frase=frase, linhas=linhas,
        total=total,
        colunas=[("obra", "Obra"), ("contrato", "Contrato"),
                 ("locadora", "Locadora"), ("itens", "Equipamentos"),
                 ("valor_periodo", "Por período"),
                 ("periodicidade", "Periodicidade"),
                 ("meses", "Meses locado"), ("pago_ate_agora", "Já pago"),
                 ("alertas", "Alertas")],
        de_onde_veio={"tela": "/erp/suprimentos/locacoes",
                      "explicacao": "Suprimentos › Locações."},
        observacao=("Só contratos ATIVOS. O valor é por PERÍODO do contrato — "
                    "quase sempre mensal, mas a coluna de periodicidade diz. "
                    "A lista respeita as obras que você alcança."))


def locacao_que_ja_pagou_a_compra(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Locação cujo aluguel acumulado já passou do preço de comprar.

    É a pergunta que economiza dinheiro de verdade: equipamento esquecido em
    obra continua faturando todo mês, e ninguém percebe porque cada parcela,
    sozinha, é pequena.
    """
    from app.apps.erp.core import locacoes as svc_loc

    contratos = [c for c in svc_loc.listar(s, usuario, apenas_ativos=True)
                 if c.get("alertas")]
    linhas = []
    for c in contratos:
        for alerta in c.get("alertas") or []:
            linhas.append({
                "obra": c.get("obra") or "—",
                "contrato": c.get("numero") or "",
                "locadora": c.get("locadora") or "",
                "gravidade": (alerta.get("gravidade") or "").capitalize(),
                "aviso": alerta.get("msg") or "",
                "valor_periodo": c.get("valor_periodo") or 0,
            })
    criticas = [l for l in linhas if l["gravidade"].upper().startswith("CRITIC")]
    linhas.sort(key=lambda l: (0 if l in criticas else 1, l["obra"]))

    if not linhas:
        frase = "Nenhuma locação pedindo decisão."
    elif criticas:
        frase = (f"{len(linhas)} aviso(s) em {len(contratos)} contrato(s), "
                 f"{len(criticas)} deles CRÍTICOS — aluguel que já pagou a "
                 f"compra, devolução vencida ou prazo estourado.")
    else:
        frase = (f"{len(linhas)} aviso(s) em {len(contratos)} contrato(s), "
                 f"nenhum crítico ainda.")
    return _resposta(
        titulo="Locações que pedem decisão", frase=frase, linhas=linhas,
        colunas=[("gravidade", "Gravidade"), ("obra", "Obra"),
                 ("contrato", "Contrato"), ("locadora", "Locadora"),
                 ("aviso", "O que houve"), ("valor_periodo", "Por período")],
        de_onde_veio={"tela": "/erp/suprimentos/locacoes",
                      "explicacao": "Suprimentos › Locações, coluna de pendências."},
        observacao=("Cada parcela de aluguel, sozinha, é pequena — por isso "
                    "equipamento esquecido em obra passa despercebido. É aqui "
                    "que ele aparece."))


def parcelas_de_locacao_sem_lancar(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Aluguel que já venceu e ainda não virou título a pagar.

    Enquanto não é lançado, ele não aparece em nenhuma previsão de caixa — e
    chega como surpresa quando a locadora cobra.
    """
    from app.apps.erp.core import locacoes as svc_loc

    contratos = [c for c in svc_loc.listar(s, usuario)
                 if (c.get("parcelas_vencidas_sem_lancar") or 0) > 0]
    linhas = [{
        "obra": c.get("obra") or "—",
        "contrato": c.get("numero") or "",
        "locadora": c.get("locadora") or "",
        "parcelas": c.get("parcelas_vencidas_sem_lancar") or 0,
        "valor_periodo": c.get("valor_periodo") or 0,
        "situacao": c.get("status") or "",
    } for c in contratos]
    linhas.sort(key=lambda l: -l["parcelas"])
    quantas = sum(l["parcelas"] for l in linhas)

    frase = ("Nenhuma parcela de locação vencida sem lançar." if not linhas else
             f"{quantas} parcela(s) de aluguel já venceram e ainda não viraram "
             f"título, em {len(linhas)} contrato(s).")
    return _resposta(
        titulo="Aluguel vencido e ainda não lançado", frase=frase, linhas=linhas,
        colunas=[("parcelas", "Parcelas"), ("obra", "Obra"),
                 ("contrato", "Contrato"), ("locadora", "Locadora"),
                 ("valor_periodo", "Por período"), ("situacao", "Situação")],
        de_onde_veio={"tela": "/erp/suprimentos/locacoes",
                      "explicacao": "Suprimentos › Locações, coluna 'vencidas sem lançar'."},
        observacao=("Enquanto não é lançada, a parcela não entra em previsão "
                    "de caixa nenhuma — e chega como surpresa quando a "
                    "locadora cobra."))


# ===========================================================================
# OBRAS — o cadastro que trava (ou destrava) o resto do sistema
#
# O GRUPO NASCEU PEQUENO E CRESCEU EM 12/09/2026, quando a palavra foi
# decidida. Até então "quanto custou a obra tal" não estava aqui, porque
# "custo da obra" tinha mais de uma leitura possível e cada uma dá um número
# diferente, todos com cara de certo.
#
# A DEFINIÇÃO, nas palavras do dono: *"o custo normalmente está associado só
# às despesas de DRE, nada de fluxo. E é o custo executado e o custo
# comprometido — são essas duas visões que a gente tem"*.
#
# Ou seja, três regras, e elas mandam em `custo_da_obra`:
#
#   1. **Só conta de DRE** (natureza RESULTADO). Conta de FLUXO — transferência
#      entre contas, aporte, principal de empréstimo — não é custo: é dinheiro
#      mudando de lugar. Somá-la inflaria o custo da obra sem que nada tenha
#      sido consumido.
#   2. **COMPROMETIDO** é o que já foi lançado e ainda vale: a obrigação
#      existe, tendo o dinheiro saído ou não. Rascunho, cancelado, estornado e
#      devolvido ficam de fora — não comprometem nada.
#   3. **EXECUTADO** é o que saiu do caixa de verdade: a soma dos pagamentos.
#
# E continua valendo a decisão de 11/09/2026: custo é a despesa DIRETA da obra,
# o que foi rateado nela. Rateio da administração da empresa não vira custo de
# obra.
#
# AS DUAS VISÕES APARECEM JUNTAS, sempre. Escolher uma e mostrar só ela seria
# escolher pelo dono em silêncio — e a diferença entre as duas é justamente o
# que ele ainda tem para pagar.
#
# O resto do grupo é conferência de cadastro: as perguntas que evitam descobrir
# que falta um campo no dia em que a nota precisa sair.
#
# ESCOPO: obra é registro SEM AUTOR, igual a contrato de locação. Por isso
# passa por `obras_de_registro_sem_autor`, e não por `obras_do_usuario` — a
# diferença entre as duas foi o que abriu a brecha das Locações em 11/09/2026.
# ===========================================================================

# Os campos que, faltando, IMPEDEM alguma coisa de acontecer — e o que cada um
# impede. A lista não é opinião: é a mesma conferência que
# `notas_emitidas/automatica.py` faz na hora de emitir. Se ela mudar lá, muda
# aqui, senão a resposta promete uma emissão que vai falhar.
TRAVAS_DO_CADASTRO = [
    ("cno", "CNO", "a nota de obra exige a matrícula"),
    ("codigo_ibge", "código IBGE", "diz à prefeitura ONDE o serviço foi prestado"),
    ("aliquota_iss_pct", "alíquota de ISS", "sem ela a nota não calcula o imposto"),
    ("empresa_id", "empresa", "é o CNPJ que emite a nota e dispara a cotação"),
]


def _obras_que_alcanco(s: Session, usuario: Usuario) -> list[Any]:
    """As obras que esta pessoa enxerga, já ordenadas por código."""
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    from app.apps.erp.db.models.cadastros import Obra

    permitidas = obras_de_registro_sem_autor(s, usuario)
    stmt = select(Obra).order_by(Obra.codigo)
    if permitidas is not None:
        stmt = stmt.where(Obra.id.in_(permitidas or [0]))
    return list(s.scalars(stmt).all())


def _iss_da_obra(obra: Any) -> Any:
    """A alíquota que vale. São dois campos por herança do cadastro antigo."""
    return getattr(obra, "aliquota_iss_pct", None) or getattr(obra, "aliquota_iss", None)


def cadastro_incompleto(s: Session, usuario: Usuario, *,
                        obra: str = "") -> dict[str, Any]:
    """Quais obras não emitem nota hoje por falta de cadastro."""
    faltantes = []
    for o in _obras_que_alcanco(s, usuario):
        if obra and not (_casa(obra, o.codigo) or _casa(obra, o.nome)):
            continue
        faltando = []
        for campo, rotulo, _ in TRAVAS_DO_CADASTRO:
            valor = _iss_da_obra(o) if campo == "aliquota_iss_pct" else getattr(o, campo, None)
            if not valor:
                faltando.append(rotulo)
        if faltando:
            faltantes.append({
                "obra": o.codigo, "nome": (o.nome or "")[:80],
                "cliente": (o.cliente or "")[:60],
                "falta": ", ".join(faltando),
                "quantos": len(faltando),
            })

    onde = f" (procurando por “{obra}”)" if obra else ""
    frase = (f"Todas as obras que você alcança estão com o cadastro completo "
             f"para emitir nota{onde}." if not faltantes else
             f"{len(faltantes)} obra(s) não emitem nota hoje por falta de "
             f"cadastro{onde}.")
    return _resposta(
        titulo="Obras com cadastro incompleto", frase=frase, linhas=faltantes,
        colunas=[("obra", "Obra"), ("nome", "Nome"), ("cliente", "Cliente"),
                 ("falta", "O que falta"), ("quantos", "Campos faltando")],
        de_onde_veio={"tela": "/erp/obras",
                      "explicacao": "Obras › abrir a obra › aba Fiscal."},
        observacao=("Confere os mesmos quatro campos que a emissão da nota "
                    "exige: " + "; ".join(f"{r} ({p})"
                                          for _, r, p in TRAVAS_DO_CADASTRO) +
                    ". Só a obra; o cliente e o certificado da empresa são "
                    "conferidos na hora de emitir."))


def garantia_vencendo(s: Session, usuario: Usuario, *,
                      dias: Any = 60) -> dict[str, Any]:
    """Seguro garantia vencido ou perto de vencer."""
    try:
        prazo = int(dias)
    except (TypeError, ValueError):
        prazo = 60
    hoje = date.today()
    limite = hoje + timedelta(days=prazo)

    linhas = []
    for o in _obras_que_alcanco(s, usuario):
        fim = getattr(o, "seguro_vigencia_fim", None)
        if fim is None or fim > limite:
            continue
        restam = (fim - hoje).days
        linhas.append({
            "obra": o.codigo, "nome": (o.nome or "")[:80],
            "apolice": getattr(o, "seguro_garantia", None) or "—",
            "vence_em": fim.isoformat(),
            "dias": restam,
            "situacao": "VENCIDO" if restam < 0 else "a vencer",
        })
    linhas.sort(key=lambda l: l["dias"])
    vencidos = [l for l in linhas if l["dias"] < 0]

    frase = (f"Nenhum seguro garantia vence nos próximos {prazo} dias."
             if not linhas else
             f"{len(linhas)} obra(s) com seguro garantia vencido ou vencendo "
             f"em até {prazo} dias" +
             (f" — {len(vencidos)} já vencido(s)." if vencidos else "."))
    return _resposta(
        titulo=f"Seguro garantia — vencido ou vencendo em {prazo} dias",
        frase=frase, linhas=linhas,
        colunas=[("obra", "Obra"), ("nome", "Nome"), ("apolice", "Apólice"),
                 ("vence_em", "Vence em"), ("dias", "Dias"),
                 ("situacao", "Situação")],
        de_onde_veio={"tela": "/erp/obras",
                      "explicacao": "Obras › abrir a obra › Seguro garantia."},
        observacao=("Dias negativos são obras que JÁ venceram. Obra sem data "
                    "de vigência preenchida não aparece aqui — e não aparecer "
                    "não quer dizer que esteja em dia: quer dizer que o "
                    "sistema não sabe."))


def vigencia_vencida(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Obras ainda abertas cuja vigência de contrato já passou."""
    from app.apps.erp.core.cadastros.obras import (
        FASES_ENCERRADAS, fase_em_portugues,
    )

    hoje = date.today()
    linhas = []
    for o in _obras_que_alcanco(s, usuario):
        fim = getattr(o, "vigencia_fim", None)
        if fim is None or fim >= hoje:
            continue
        if (getattr(o, "fase", "") or "").upper() in FASES_ENCERRADAS:
            continue
        linhas.append({
            "obra": o.codigo, "nome": (o.nome or "")[:80],
            "contrato": getattr(o, "contrato", None) or "—",
            # A fase vem do banco como EM_EXECUCAO. O dono não lê o sistema
            # dele em maiúscula e sem acento — o rótulo é o mesmo que a tela
            # de Obras usa, e vem de lá para não haver duas listas.
            "fase": fase_em_portugues(getattr(o, "fase", "") or ""),
            "venceu_em": fim.isoformat(),
            "dias": (hoje - fim).days,
        })
    linhas.sort(key=lambda l: -l["dias"])

    frase = ("Nenhuma obra aberta está com a vigência do contrato vencida."
             if not linhas else
             f"{len(linhas)} obra(s) ainda abertas com a vigência do contrato "
             f"vencida — precisam de aditivo de prazo ou de encerramento.")
    return _resposta(
        titulo="Vigência de contrato vencida", frase=frase, linhas=linhas,
        colunas=[("obra", "Obra"), ("nome", "Nome"), ("contrato", "Contrato"),
                 ("fase", "Fase"), ("venceu_em", "Venceu em"),
                 ("dias", "Dias atrás")],
        de_onde_veio={"tela": "/erp/obras",
                      "explicacao": "Obras › coluna Vigência."},
        observacao=("“Aberta” aqui é a FASE gravada no cadastro: fica de fora "
                    "o que está concluído, recebido, em acervo técnico ou "
                    "distratado. Não é uma opinião do sistema sobre a obra "
                    "estar ou não tocando — é o que alguém marcou na tela."))


# ---------------------------------------------------------------------------
# O CUSTO DA OBRA — nas duas visões que o dono nomeou
# ---------------------------------------------------------------------------
# Reusa `core/relatorios.py` inteiro: o mesmo recorte por obra, a mesma conta
# de rateio, a mesma correção da conta redutora, o mesmo filtro de espécie.
# Uma consulta própria aqui divergiria da tela de Relatórios no dia em que
# alguém corrigisse uma das duas — e aí o assistente e o relatório dariam
# números diferentes sobre a mesma obra, que é o pior desfecho possível.
# ---------------------------------------------------------------------------
def custo_da_obra(s: Session, usuario: Usuario, *, obra: str = "",
                  projeto: str = "",
                  competencia_de: str = "",
                  competencia_ate: str = "") -> dict[str, Any]:
    """Quanto a obra comprometeu e quanto ela já executou de custo.

    Dizendo um PROJETO, a resposta vem SOMADA por projeto — pedido do dono em
    13/09/2026: *"se a obra estiver dentro de algum projeto (…) eu poder
    visualizar o projeto, ou seja, o somatório daquelas obras"*.
    """
    from app.apps.erp.core import relatorios

    filtros: dict[str, Any] = {"natureza": "RESULTADO", "especie": "pagar"}
    if competencia_de:
        filtros["competencia_de"] = competencia_de
    if competencia_ate:
        filtros["competencia_ate"] = competencia_ate

    # O PROJETO some as obras dele numa linha só — e o agrupamento muda junto,
    # senão a resposta viria obra a obra e não seria a soma que ele pediu.
    dimensao, rotulo_coluna, chave_coluna = "obra", "Obra", "obra"
    escolhido_projeto = None
    if projeto:
        from app.apps.erp.core.cadastros import projetos as svc_projetos
        cadastrados = svc_projetos.listar(s)
        escolhido_projeto = next(
            (p for p in cadastrados
             if _casa(projeto, p["codigo"]) or _casa(projeto, p["nome"])), None)
        if escolhido_projeto is None:
            return _resposta(
                titulo="Custo do projeto",
                frase=f"Não achei o projeto “{projeto}”. "
                      f"Projetos cadastrados: "
                      f"{', '.join(p['codigo'] for p in cadastrados) or 'nenhum'}.",
                linhas=[], colunas=[],
                de_onde_veio={"tela": "/erp/relatorios",
                              "explicacao": "Relatórios › agrupar por projeto."})
        filtros["projeto_id"] = escolhido_projeto["id"]
        dimensao, rotulo_coluna, chave_coluna = "projeto", "Projeto", "obra"

    obras = _obras_que_alcanco(s, usuario)
    escolhida = None
    if obra:
        escolhida = next((o for o in obras
                          if _casa(obra, o.codigo) or _casa(obra, o.nome)), None)
        if escolhida is None:
            return _resposta(
                titulo="Custo da obra",
                frase=f"Não achei a obra “{obra}” entre as que você alcança.",
                linhas=[], colunas=[],
                de_onde_veio={"tela": "/erp/relatorios",
                              "explicacao": "Relatórios › totais por obra."})
        filtros["obra_id"] = escolhida.id

    r = relatorios.resumo(s, dimensao, filtros, usuario)
    linhas = [{"obra": l["chave"],
               "comprometido": l["total"],
               "executado": l["pago"],
               "a_executar": l["aberto"]} for l in r["linhas"] if l["total"]]
    linhas.sort(key=lambda l: -l["comprometido"])

    periodo = ""
    if competencia_de or competencia_ate:
        periodo = f" entre {competencia_de or '…'} e {competencia_ate or '…'}"

    if not linhas:
        frase = f"Nenhum custo lançado{periodo} nas obras que você alcança."
    elif escolhido_projeto is not None:
        l = linhas[0]
        frase = (f"Projeto {escolhido_projeto['codigo']}{periodo} "
                 f"({escolhido_projeto['quantas_obras']} obra(s)): "
                 f"**{_reais(l['comprometido'])} comprometido** e "
                 f"**{_reais(l['executado'])} executado**. "
                 f"Faltam {_reais(l['a_executar'])} para sair do caixa.")
    elif escolhida is not None:
        l = linhas[0]
        frase = (f"{escolhida.codigo}{periodo}: **{_reais(l['comprometido'])} "
                 f"comprometido** e **{_reais(l['executado'])} executado**. "
                 f"Faltam {_reais(l['a_executar'])} para sair do caixa.")
    else:
        frase = (f"{len(linhas)} obra(s){periodo}: "
                 f"{_reais(r['total'])} comprometido e "
                 f"{_reais(r['total_pago'])} executado.")

    return _resposta(
        titulo=(f"Custo do projeto{periodo}" if escolhido_projeto
                else f"Custo da obra{periodo}"), frase=frase, linhas=linhas,
        colunas=[(chave_coluna, rotulo_coluna), ("comprometido", "Comprometido"),
                 ("executado", "Executado"), ("a_executar", "Falta executar")],
        total=Decimal(str(r["total"])),
        de_onde_veio={"tela": "/erp/relatorios",
                      "explicacao": "Relatórios › totais por obra."},
        observacao=(
            "**Comprometido** é o que já foi lançado e ainda vale — a "
            "obrigação existe, tendo o dinheiro saído ou não. **Executado** é "
            "o que já saiu do caixa. Entram só contas de RESULTADO (DRE): "
            "transferência entre contas e aporte não são custo, são dinheiro "
            "mudando de lugar. É a despesa DIRETA da obra — rateio da "
            "administração da empresa não vira custo de obra. Definição do "
            "dono, em 12/09/2026."))


# ===========================================================================
# DOCUMENTOS — o que está ESCRITO, não o que está somado
#
# Esta é a única família de respostas deste arquivo que não faz conta nenhuma.
# Ela devolve pedaços de texto que já estavam num contrato, num edital, numa
# norma — e o que o sistema garante não é o número, é a PROCEDÊNCIA: de qual
# documento saiu e em que trecho.
#
# Por isso o trecho vem sempre junto, e é ele a resposta. A frase de cima,
# quando existe, é a IA juntando os trechos — nunca a IA lembrando de alguma
# coisa. Ver o porquê inteiro em `core/perguntas/documentos.py`.
# ===========================================================================
def o_que_os_documentos_dizem(s: Session, usuario: Usuario, *,
                              assunto: str = "",
                              obra: str = "") -> dict[str, Any]:
    """Procura o assunto nos documentos que esta pessoa alcança."""
    from app.apps.erp.core.perguntas import documentos as svc_doc

    assunto = (assunto or "").strip()
    if not assunto:
        return _resposta(
            titulo="Procurar nos documentos",
            frase=("Diga o que você quer procurar — por exemplo “reajuste”, "
                   "“prazo de garantia” ou “multa por atraso”."),
            linhas=[], colunas=[],
            de_onde_veio={"tela": "/erp/arquivo",
                          "explicacao": "Arquivo, o acervo de documentos."})

    achados = svc_doc.procurar(s, usuario, pergunta=assunto, obra=obra)
    onde = f" (na obra {obra})" if obra else ""

    if not achados:
        return _resposta(
            titulo=f"“{assunto}” nos documentos", frase=(
                f"Não achei “{assunto}” em nenhum documento que você alcança"
                f"{onde}."),
            linhas=[], colunas=[],
            de_onde_veio={"tela": "/erp/arquivo",
                          "explicacao": "Arquivo, o acervo de documentos."},
            observacao=(
                "Duas razões possíveis, e elas pedem coisas diferentes: o "
                "documento não está no Arquivo, ou ele está mas usa outras "
                "palavras. A busca de hoje acha pela palavra escrita, não pelo "
                "sentido — “reajuste” acha “reajustar”, mas não acha "
                "“correção monetária”."))

    linhas = [{"documento": a["nome"], "tipo": a["tipo"],
               "de_quem": a["de_quem"], "trecho": a["trecho"]}
              for a in achados]
    frase = (f"Achei em {len(achados)} documento(s){onde}. "
             f"O que está escrito, na íntegra, está abaixo.")

    # A frase da IA é um ACRÉSCIMO, e some sem quebrar nada. Os trechos são a
    # resposta; ela é só a leitura em voz alta.
    resumo = svc_doc.resumir(achados, assunto,
                             usuario_id=getattr(usuario, "id", None))
    return _resposta(
        titulo=f"“{assunto}” nos documentos", frase=(resumo or frase),
        linhas=linhas,
        colunas=[("documento", "Documento"), ("tipo", "Tipo"),
                 ("de_quem", "De quem é"), ("trecho", "O que está escrito")],
        de_onde_veio={"tela": "/erp/arquivo",
                      "explicacao": "Arquivo, o acervo de documentos."},
        observacao=(
            ("A frase acima foi escrita pela IA LENDO SÓ os trechos abaixo — "
             "ela não consulta o banco nem lembra de nada por fora. Confira "
             "nos trechos, que são o documento falando. " if resumo else "") +
            "As « » marcam onde as suas palavras aparecem. Abra o documento no "
            "Arquivo para ler o resto."))
