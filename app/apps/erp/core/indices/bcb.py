# ============================================================================
# ERP — core/indices/bcb.py
# A tabela do INCC, mantida pelo próprio sistema.
#
# PEDIDO DO DONO, 09/09/2026: *"já coloque aí dentro da programação do sistema
# ele fazer essa busca, atualizar a tabela e permitir todos esses cálculos."*
#
# DE ONDE VEM O NÚMERO, E POR QUE NÃO DA FGV
#
# O INCC é calculado pela FGV, e o serviço de dados dela (FGVDados) é
# licenciado — depender dele significaria contrato, chave e uma conta a pagar.
# O **Banco Central republica a série no SGS**, em API pública, sem cadastro e
# sem chave. O INCC-DI é a série **192**, e foi ela que o dono confirmou como a
# usada nos contratos da BWS.
#
# ⚠️ O INCC tem três versões — DI, M e 10 —, com períodos de apuração
# diferentes. Usar a errada dá valor errado COM CARA DE CERTO, que é o pior
# tipo de erro num número que ninguém confere. Por isso a série vem escrita no
# catálogo abaixo, com o nome por extenso.
#
# O QUE ACONTECE QUANDO A BUSCA FALHA
#
# Nada de catastrófico, de propósito: a coleta é pelo BOTÃO (nunca no start do
# gunicorn, mesma regra das migrações), devolve o que conseguiu, e diz em
# português o que houve. A tabela continua com o que já tinha, e o cálculo
# continua funcionando com os meses que existem — avisando quais faltam.
#
# E existe o lançamento MANUAL: o INCC-DI do mês só sai por volta do dia 25, e
# num fechamento apertado alguém precisa poder digitar o número do boletim da
# FGV. A linha guarda a fonte, então o relatório sempre diz qual é qual.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import IndiceAncora, IndiceEconomico

logger = logging.getLogger(__name__)

URL_SGS = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
TEMPO_LIMITE = 25          # segundos; o SGS às vezes demora, mas não a vida toda

# código interno → (série do SGS, nome por extenso)
#
# Fica curto de propósito: índice que ninguém usa é índice que ninguém confere.
CATALOGO: dict[str, tuple[int, str]] = {
    "INCC-DI": (192, "INCC-DI (Índice Nacional de Custo da Construção — "
                     "Disponibilidade Interna, FGV)"),
    "IPCA": (433, "IPCA (IBGE)"),
    "IGP-M": (189, "IGP-M (FGV)"),
    "INPC": (188, "INPC (IBGE)"),
}

PADRAO = "INCC-DI"


def _mes(d: date) -> date:
    return d.replace(day=1)


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v).strip().replace(",", "."))
    except (InvalidOperation, AttributeError, TypeError):
        raise ErroValidacao(f"Valor de índice ilegível: {v!r}")


def listar_indices() -> list[dict[str, str]]:
    return [{"codigo": c, "nome": nome, "serie": str(serie)}
            for c, (serie, nome) in CATALOGO.items()]


# ---------------------------------------------------------------------------
# A busca
# ---------------------------------------------------------------------------
def _buscar(serie: int, desde: Optional[date]) -> list[dict[str, str]]:
    """Chama o SGS. Isolada para o teste poder trocar por um dublê — não há
    como bater no Banco Central dentro da suíte, e nem seria desejável."""
    import requests

    params = {"formato": "json"}
    if desde:
        params["dataInicial"] = desde.strftime("%d/%m/%Y")
    r = requests.get(URL_SGS.format(serie=serie), params=params, timeout=TEMPO_LIMITE)
    r.raise_for_status()
    dados = r.json()
    if not isinstance(dados, list):
        raise ErroValidacao("O Banco Central respondeu num formato inesperado.")
    return dados


def atualizar(s: Session, codigo: str = PADRAO, *, desde: Optional[date] = None,
              usuario=None) -> dict[str, Any]:
    """Traz do Banco Central o que falta e grava.

    Nunca apaga e nunca sobrescreve linha MANUAL: quem digitou o número do
    boletim da FGV tinha um motivo, e a coleta automática passando por cima
    apagaria a correção sem avisar ninguém.
    """
    codigo = (codigo or PADRAO).strip().upper()
    if codigo not in CATALOGO:
        raise ErroValidacao(f"Índice desconhecido: {codigo}. "
                            f"Conhecidos: {', '.join(CATALOGO)}.")
    serie, nome = CATALOGO[codigo]

    if desde is None:
        ultimo = s.scalars(select(IndiceEconomico.competencia).where(
            IndiceEconomico.codigo == codigo)
            .order_by(IndiceEconomico.competencia.desc()).limit(1)).first()
        # Sem nada guardado, começa em 2010: cobre com folga qualquer contrato
        # vivo da BWS e não puxa quarenta anos de série à toa.
        desde = ultimo or date(2010, 1, 1)

    try:
        linhas = _buscar(serie, desde)
    except ErroValidacao:
        raise
    except Exception as e:
        # Rede fora, serviço fora, certificado, o que for: a tabela continua
        # como está e o sistema diz o que houve, em português.
        # O detalhe técnico vai para o log, não para a tela: um traçado de
        # exceção de dez linhas no meio da tela não ajuda quem está tentando
        # fechar uma medição, e esconde a única frase que importa.
        logger.warning("ERP/índices: falha ao buscar %s no Banco Central: %s",
                       codigo, e, exc_info=True)
        raise ErroValidacao(
            "Não consegui falar com o Banco Central agora. A tabela continua "
            "com o que já tinha — nada foi perdido. Tente de novo mais tarde, "
            "ou lance o mês à mão pelo boletim da FGV.")

    manuais = {i.competencia for i in s.scalars(select(IndiceEconomico).where(
        IndiceEconomico.codigo == codigo, IndiceEconomico.fonte == "MANUAL")).all()}
    existentes = {i.competencia: i for i in s.scalars(select(IndiceEconomico).where(
        IndiceEconomico.codigo == codigo)).all()}

    novos, atualizados, ignorados = 0, 0, 0
    for linha in linhas:
        try:
            quando = _mes(datetime.strptime(str(linha["data"])[:10], "%d/%m/%Y").date())
            valor = _dec(linha["valor"])
        except (KeyError, ValueError, ErroValidacao):
            ignorados += 1
            continue
        if quando in manuais:
            ignorados += 1
            continue
        atual = existentes.get(quando)
        if atual is None:
            s.add(IndiceEconomico(codigo=codigo, competencia=quando,
                                  variacao_pct=valor, fonte="BCB-SGS"))
            novos += 1
        elif atual.variacao_pct != valor:
            # O Banco Central revisa série. Quando revisa, vale o novo — e o
            # evento de auditoria guarda o valor que havia antes.
            registrar_evento(s, "indice", 0, "INDICE_REVISADO", {
                "codigo": codigo, "competencia": quando.isoformat(),
                "de": str(atual.variacao_pct), "para": str(valor)},
                usuario.id if usuario else None)
            atual.variacao_pct = valor
            atual.fonte = "BCB-SGS"
            atualizados += 1
    s.flush()
    recalcular_numeros(s, codigo)

    resumo = {"codigo": codigo, "nome": nome, "serie": serie,
              "novos": novos, "atualizados": atualizados,
              "preservados_manuais": ignorados,
              "desde": desde.isoformat(), "recebidos": len(linhas)}
    logger.info("ERP/índices: %s — %s novos, %s revisados", codigo, novos, atualizados)
    registrar_evento(s, "indice", 0, "INDICES_ATUALIZADOS", resumo,
                     usuario.id if usuario else None)
    return resumo


# ---------------------------------------------------------------------------
# Lançamento à mão, e leitura
# ---------------------------------------------------------------------------
def lancar_manual(s: Session, *, codigo: str, competencia: date, variacao_pct: Any,
                  usuario=None) -> IndiceEconomico:
    """O número do boletim da FGV, digitado.

    Existe porque o INCC-DI do mês só sai por volta do dia 25 — e num
    fechamento apertado esperar o Banco Central republicar não é opção.
    """
    codigo = (codigo or PADRAO).strip().upper()
    if codigo not in CATALOGO:
        raise ErroValidacao(f"Índice desconhecido: {codigo}.")
    quando = _mes(competencia)
    if quando > _mes(date.today()):
        raise ErroValidacao("Não dá para lançar índice de um mês que ainda não "
                            "terminou.")
    valor = _dec(variacao_pct)
    if abs(valor) > 50:
        # Variação mensal de índice de construção acima de 50% não existe; se
        # aparecer, é dígito trocado — e um dígito trocado aqui contamina todo
        # reajuste dali para a frente.
        raise ErroValidacao(f"Variação de {valor}% num mês só não é plausível. "
                            f"Confira o número do boletim.")

    linha = s.get(IndiceEconomico, (codigo, quando))
    if linha is None:
        linha = IndiceEconomico(codigo=codigo, competencia=quando,
                                variacao_pct=valor, fonte="MANUAL")
        s.add(linha)
    else:
        linha.variacao_pct = valor
        linha.fonte = "MANUAL"
    s.flush()
    # O mês lançado desloca todos os seguintes: a série inteira é refeita.
    recalcular_numeros(s, codigo)
    registrar_evento(s, "indice", 0, "INDICE_LANCADO_A_MAO", {
        "codigo": codigo, "competencia": quando.isoformat(), "valor": str(valor)},
        usuario.id if usuario else None)
    return linha


# ---------------------------------------------------------------------------
# O NÚMERO-ÍNDICE (migração 068)
# ---------------------------------------------------------------------------
BASE_DO_NUMERO_INDICE = Decimal("100")


def ancora_de(s: Session, codigo: str = PADRAO) -> Optional[IndiceAncora]:
    """O ponto de referência do número-índice, se alguém já informou um."""
    return s.get(IndiceAncora, (codigo or PADRAO).strip().upper())


def definir_ancora(s: Session, *, codigo: str = PADRAO, competencia: date,
                   numero_indice: Any, observacao: str = "",
                   usuario=None) -> IndiceAncora:
    """Pendura a série inteira num número oficial.

    O DONO, em 14/09/2026, vendo a coluna nova: *"apareceram os índices, mas os
    números estão diferentes do que eu costumo ver — veja o de 08/2026,
    305,943822"*.

    O número não estava errado: estava noutra base (100 em 01/2010, o mês mais
    antigo guardado). Aqui ele informa o número que o boletim publica para um
    mês, e a régua inteira se desloca para bater com o papel que ele tem na mão.
    As variações não mudam, então **nenhum reajuste já calculado muda de valor**
    — o fator é razão entre dois meses, e razão não sente mudança de base.
    """
    codigo = (codigo or PADRAO).strip().upper()
    if codigo not in CATALOGO:
        raise ErroValidacao(f"Índice desconhecido: {codigo}.")
    quando = _mes(competencia)
    valor = _dec(numero_indice)
    if valor <= 0:
        raise ErroValidacao("O número-índice tem de ser maior que zero.")

    # A âncora precisa cair num mês que a tabela tem; senão não há por onde
    # propagar, e o número informado ficaria pendurado no vazio.
    tem_o_mes = s.get(IndiceEconomico, (codigo, quando))
    if tem_o_mes is None:
        raise ErroValidacao(
            f"Não tenho o mês {quando.strftime('%m/%Y')} na tabela do {codigo}. "
            f"Atualize a tabela (ou lance esse mês à mão) antes de usá-lo como "
            f"referência.")

    linha = s.get(IndiceAncora, codigo)
    if linha is None:
        linha = IndiceAncora(codigo=codigo, competencia=quando,
                             numero_indice=valor,
                             observacao=(observacao or "").strip() or None,
                             definido_por=usuario.id if usuario else None)
        s.add(linha)
    else:
        linha.competencia = quando
        linha.numero_indice = valor
        linha.observacao = (observacao or "").strip() or None
        linha.definido_por = usuario.id if usuario else None
    s.flush()
    recalcular_numeros(s, codigo)
    registrar_evento(s, "indice", 0, "INDICE_ANCORADO", {
        "codigo": codigo, "competencia": quando.isoformat(),
        "numero_indice": str(valor), "observacao": observacao},
        usuario.id if usuario else None)
    return linha


def limpar_ancora(s: Session, *, codigo: str = PADRAO, usuario=None) -> bool:
    """Tira o ponto de referência: a série volta a 100 no mês mais antigo."""
    codigo = (codigo or PADRAO).strip().upper()
    linha = s.get(IndiceAncora, codigo)
    if linha is None:
        return False
    s.delete(linha)
    s.flush()
    recalcular_numeros(s, codigo)
    registrar_evento(s, "indice", 0, "INDICE_ANCORA_REMOVIDA", {"codigo": codigo},
                     usuario.id if usuario else None)
    return True


def recalcular_numeros(s: Session, codigo: str = PADRAO) -> int:
    """Refaz o número-índice da série inteira, do mês mais velho para o novo.

    Pedido do dono em 14/09/2026: *"a gente precisa do índice mesmo, não só
    variação (…) caso a gente queira saber qual índice inicial, qual índice
    final"*.

        índice do mês = índice do mês anterior × (1 + variação/100)

    **Onde a régua começa** depende de haver ou não uma âncora (migração 069):

    - com âncora, o mês informado recebe exatamente o número oficial que a
      pessoa digitou, e os outros saem dele — para a frente multiplicando pelas
      variações, para trás dividindo. É o que faz o número bater com o boletim;
    - sem âncora, **base 100 no mês mais antigo guardado**, como era antes.

    ⚠️ Sem âncora, o número absoluto NÃO é o do boletim da FGV — a base é outra.
    O que é idêntico nos dois casos, e é o que vale, é a RAZÃO entre dois meses:
    dividir o índice final pelo inicial dá o mesmo fator de reajuste da
    planilha. Trocar a base não mexe em reajuste nenhum já calculado.

    **Refaz a série toda, e não só o mês novo**, de propósito: o Banco Central
    revisa variação passada, e um mês revisado desloca todos os seguintes. Meia
    série atualizada seria pior do que série nenhuma, porque a razão entre dois
    meses de lados diferentes do remendo daria um fator errado — com cara de
    certo.
    """
    codigo = (codigo or PADRAO).strip().upper()
    linhas = list(s.scalars(select(IndiceEconomico).where(
        IndiceEconomico.codigo == codigo)
        .order_by(IndiceEconomico.competencia.asc())).all())
    if not linhas:
        return 0

    ancora = s.get(IndiceAncora, codigo)
    partida = 0
    corrente = BASE_DO_NUMERO_INDICE
    if ancora is not None:
        for i, linha in enumerate(linhas):
            if linha.competencia == ancora.competencia:
                partida, corrente = i, _dec(ancora.numero_indice)
                break
        else:
            # A âncora aponta para um mês que sumiu da tabela. Não dá para
            # inventar onde ela cairia: volta para a base 100 e diz no log, em
            # vez de espalhar um número deslocado pela série inteira.
            logger.warning("ERP/índices: a âncora de %s é de %s, mês que não "
                           "está na tabela — usando base 100.", codigo,
                           ancora.competencia)
            partida, corrente = 0, BASE_DO_NUMERO_INDICE

    # Do mês da âncora para a frente. Acumula em precisão cheia e guarda
    # arredondado: arredondar a cada passo empurraria o erro para a frente.
    valor = corrente
    for i in range(partida, len(linhas)):
        if i > partida:
            valor *= (Decimal(1) + _dec(linhas[i].variacao_pct) / 100)
        linhas[i].numero_indice = valor.quantize(Decimal("0.000001"))

    # E do mês da âncora para trás, desfazendo a variação de cada mês: o índice
    # do mês anterior é o deste dividido pela variação DESTE mês.
    valor = corrente
    for i in range(partida - 1, -1, -1):
        divisor = Decimal(1) + _dec(linhas[i + 1].variacao_pct) / 100
        if divisor == 0:      # queda de 100% num mês não existe; guarda a casa
            raise ErroValidacao(
                f"Variação de -100% em {linhas[i + 1].competencia.strftime('%m/%Y')} "
                f"impede refazer a série para trás.")
        valor /= divisor
        linhas[i].numero_indice = valor.quantize(Decimal("0.000001"))

    s.flush()
    return len(linhas)


def listar(s: Session, codigo: str = PADRAO, *, limite: int = 60) -> dict[str, Any]:
    """Os últimos meses guardados, do mais novo para o mais velho."""
    codigo = (codigo or PADRAO).strip().upper()
    linhas = list(s.scalars(select(IndiceEconomico).where(
        IndiceEconomico.codigo == codigo)
        .order_by(IndiceEconomico.competencia.desc()).limit(limite)).all())
    mais_velho = s.scalars(select(IndiceEconomico.competencia).where(
        IndiceEconomico.codigo == codigo)
        .order_by(IndiceEconomico.competencia.asc()).limit(1)).first()
    ancora = s.get(IndiceAncora, codigo)
    return {
        "codigo": codigo,
        "nome": CATALOGO.get(codigo, (0, codigo))[1],
        # Sem dizer a base, o número-índice vira um número solto: quem comparar
        # com o boletim da FGV vai achar que está errado.
        "base_do_numero": (
            (f"{_num_br(ancora.numero_indice)} em "
             f"{ancora.competencia.strftime('%m/%Y')}, informado por você")
            if ancora is not None else
            (f"100,000000 em {mais_velho.strftime('%m/%Y')} — a régua do "
             f"sistema, não a do boletim" if mais_velho else "")),
        # A âncora aberta, para a tela poder mostrar e deixar trocar.
        "ancora": ({"competencia": ancora.competencia.isoformat(),
                    "numero_indice": float(ancora.numero_indice),
                    "observacao": ancora.observacao or "",
                    "definido_em": (ancora.definido_em.isoformat()
                                    if ancora.definido_em else None)}
                   if ancora is not None else None),
        "serie": CATALOGO.get(codigo, (0, ""))[0],
        "meses": [{"competencia": i.competencia.isoformat(),
                   "variacao_pct": float(i.variacao_pct),
                   # O número-índice: é dele que saem "índice inicial" e
                   # "índice final" do reajuste (migração 068).
                   "numero_indice": (float(i.numero_indice)
                                     if i.numero_indice is not None else None),
                   "fonte": i.fonte,
                   "coletado_em": i.coletado_em.isoformat() if i.coletado_em else None}
                  for i in linhas],
        "ultimo": linhas[0].competencia.isoformat() if linhas else None,
        # A pergunta prática: a tabela está em dia? O mês corrente ainda não
        # existe, então "em dia" é ter o mês ANTERIOR.
        "em_dia": bool(linhas and linhas[0].competencia >= _mes_anterior(date.today())),
    }


def _num_br(v: Any) -> str:
    """Número com seis casas e vírgula, do jeito que se lê no boletim."""
    return f"{Decimal(str(v)):,.6f}".replace(",", "@").replace(".", ",").replace("@", ".")


def _mes_anterior(d: date) -> date:
    primeiro = d.replace(day=1)
    return (primeiro.replace(year=primeiro.year - 1, month=12) if primeiro.month == 1
            else primeiro.replace(month=primeiro.month - 1))
