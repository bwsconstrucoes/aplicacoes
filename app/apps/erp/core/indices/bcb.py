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


# ---------------------------------------------------------------------------
# COLAR O BOLETIM INTEIRO
#
# O dono mandou, em 18/09/2026, o formato em que os índices chegam para ele:
#
#     Mês/Ano      Índice     Variação No mês  Variação No ano  Variação 12 meses
#     julho/2025   1210,471   0,91             4,39             7,41
#     agosto/2025  1216,706   0,52             4,93             7,22
#
# Antes disso, alimentar a tabela com o boletim dele exigia DOIS trabalhos
# separados e desconexos: digitar a variação mês a mês (lancar_manual) e depois
# informar o número oficial de um mês para a régua bater (definir_ancora). Duas
# telas para uma coisa só que ele já tem inteira na mão, copiada da fonte.
#
# Colar resolve os dois de uma vez: cada linha traz a VARIAÇÃO (que é o que o
# cálculo usa) e o NÚMERO-ÍNDICE (que é o que ele confere com o papel). O mês
# mais recente da colagem vira a âncora.
#
# A TRAVA QUE IMPORTA: o boletim tem de ser consistente consigo mesmo. Se
# número[n] ÷ número[n-1] não bate com a variação declarada daquele mês, ou a
# colagem misturou duas séries (INCC-DI com INCC-M, por exemplo), ou veio
# coluna trocada. Nos dois casos o número entra com cara de certo e contamina
# todo reajuste dali para a frente — então é aviso na cara, não silêncio.
# ---------------------------------------------------------------------------
MESES_POR_EXTENSO = {
    "janeiro": 1, "jan": 1, "fevereiro": 2, "fev": 2, "marco": 3, "mar": 3,
    "abril": 4, "abr": 4, "maio": 5, "mai": 5, "junho": 6, "jun": 6,
    "julho": 7, "jul": 7, "agosto": 8, "ago": 8, "setembro": 9, "set": 9,
    "outubro": 10, "out": 10, "novembro": 11, "nov": 11,
    "dezembro": 12, "dez": 12,
}

# Quanto os dois números podem discordar antes de virar aviso. O boletim publica
# a variação com 2 casas, então a conta feita a partir dos números-índice sempre
# difere um pouco por arredondamento — 0,05 ponto percentual é folga para o
# arredondamento e aperto para série trocada.
FOLGA_DE_ARREDONDAMENTO = Decimal("0.05")


def _competencia_do_boletim(texto: str) -> Optional[date]:
    """"julho/2025", "07/2025", "jul/25", "2025-07" — todos viram 01/07/2025."""
    import re
    import unicodedata
    bruto = unicodedata.normalize("NFKD", (texto or "").strip().lower())
    limpo = "".join(c for c in bruto if not unicodedata.combining(c))
    limpo = limpo.replace(" de ", "/").strip()
    partes = [p for p in re.split(r"[/\-\s]+", limpo) if p]
    if len(partes) != 2:
        return None
    a, b = partes
    if a.isdigit() and len(a) == 4:          # 2025-07
        a, b = b, a
    mes = MESES_POR_EXTENSO.get(a) or (int(a) if a.isdigit() else None)
    if not mes or not 1 <= mes <= 12 or not b.isdigit():
        return None
    ano = int(b)
    if ano < 100:
        ano += 2000
    return date(ano, mes, 1)


def _numero_br(texto: str) -> Optional[Decimal]:
    """1.210,471 e 0,91 viram Decimal. Vazio e traço viram None."""
    t = (texto or "").strip().replace("%", "")
    if not t or t in {"-", "—", "–"}:
        return None
    t = t.replace(".", "").replace(",", ".") if "," in t else t
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


def ler_boletim(texto: str) -> dict[str, Any]:
    """Transforma o texto colado em linhas (competência, número, variação).

    Não toca no banco: existe separada para a prévia poder mostrar o que
    entendeu ANTES de gravar, e para ser testável sem sessão.
    """
    linhas: list[dict[str, Any]] = []
    ignoradas: list[str] = []
    for bruta in (texto or "").splitlines():
        if not bruta.strip():
            continue
        # A colagem vem do navegador ou do Excel: separador é TAB, mas ponto e
        # vírgula e duas casas de espaço também aparecem. A vírgula NÃO separa
        # colunas aqui — ela é a vírgula decimal.
        import re
        campos = [c.strip() for c in re.split(r"\t|;|\s{2,}", bruta.strip()) if c.strip()]
        if len(campos) < 2:
            ignoradas.append(bruta.strip())
            continue
        quando = _competencia_do_boletim(campos[0])
        if quando is None:
            ignoradas.append(bruta.strip())       # o cabeçalho cai aqui
            continue
        numero = _numero_br(campos[1])
        variacao = _numero_br(campos[2]) if len(campos) > 2 else None
        if numero is None and variacao is None:
            ignoradas.append(bruta.strip())
            continue
        linhas.append({"competencia": quando, "numero_indice": numero,
                       "variacao_pct": variacao})
    linhas.sort(key=lambda x: x["competencia"])
    return {"linhas": linhas, "ignoradas": ignoradas}


def _curto(v: Any, casas: int = 2) -> str:
    """Número com as casas que a pessoa lê, não as seis do banco.

    "0,270000%" num aviso obriga quem lê a contar zeros para ver se é 0,27.
    """
    q = Decimal(str(v)).quantize(Decimal("1." + "0" * casas))
    return f"{q:,.{casas}f}".replace(",", "@").replace(".", ",").replace("@", ".")


def _conferir_consistencia(linhas: list[dict[str, Any]]) -> list[str]:
    """O boletim bate consigo mesmo? Número ÷ número anterior = variação?"""
    avisos: list[str] = []
    for anterior, atual in zip(linhas, linhas[1:]):
        na, nb = anterior["numero_indice"], atual["numero_indice"]
        v = atual["variacao_pct"]
        if na is None or nb is None or v is None or na == 0:
            continue
        # meses precisam ser consecutivos para a conta fazer sentido
        if _mes_anterior(atual["competencia"]) != anterior["competencia"]:
            continue
        calculada = (nb / na - 1) * 100
        if abs(calculada - v) > FOLGA_DE_ARREDONDAMENTO:
            avisos.append(
                f"{atual['competencia'].strftime('%m/%Y')}: o boletim diz "
                f"{_curto(v)}%, mas de {_curto(na, 3)} para {_curto(nb, 3)} a "
                f"variação é {_curto(calculada)}%. "
                f"Confira se a colagem não misturou duas versões do índice "
                f"(DI, M e 10 são diferentes) ou trocou de coluna.")
    return avisos


def importar_boletim(s: Session, *, codigo: str = PADRAO, texto: str,
                     ancorar: bool = True, usuario=None,
                     simular: bool = False) -> dict[str, Any]:
    """A tabela do boletim, colada de uma vez: variações E número-índice.

    A variação é o que o cálculo do reajuste usa; o número-índice é o que o
    dono confere com o papel. O mês mais recente com número vira a âncora, e é
    isso que faz a coluna da tela bater com o boletim dele.
    """
    codigo = (codigo or PADRAO).strip().upper()
    if codigo not in CATALOGO:
        raise ErroValidacao(f"Índice desconhecido: {codigo}.")

    lido = ler_boletim(texto)
    linhas = lido["linhas"]
    if not linhas:
        raise ErroValidacao(
            "Não reconheci nenhuma linha. Cole a tabela do boletim com o mês na "
            "primeira coluna (por exemplo: julho/2025), o número-índice na "
            "segunda e a variação do mês na terceira.")

    hoje = _mes(date.today())
    avisos = _conferir_consistencia(linhas)
    resultado: list[dict[str, Any]] = []
    gravados = 0
    for ln in linhas:
        quando, variacao = ln["competencia"], ln["variacao_pct"]
        if quando > hoje:
            avisos.append(f"{quando.strftime('%m/%Y')} ainda não terminou — "
                          f"essa linha foi deixada de fora.")
            continue
        atual = s.get(IndiceEconomico, (codigo, quando))
        antes = atual.variacao_pct if atual is not None else None
        if variacao is None:
            situacao = "sem variação na colagem"
        elif atual is None:
            situacao = "novo"
        elif _dec(antes) == variacao:
            situacao = "igual ao que já havia"
        else:
            situacao = "mudou"
        resultado.append({
            "competencia": quando.strftime("%m/%Y"),
            "numero_indice": _curto(ln["numero_indice"], 3) if ln["numero_indice"] else "",
            "variacao_pct": _curto(variacao) if variacao is not None else "",
            "antes": _curto(antes) if antes is not None else "",
            "situacao": situacao})
        if variacao is None or simular or situacao == "igual ao que já havia":
            continue
        if abs(variacao) > 50:
            raise ErroValidacao(
                f"{quando.strftime('%m/%Y')} veio com {_curto(variacao)}% num "
                f"mês só — isso não existe em índice de construção. Confira a "
                f"coluna que você colou.")
        if atual is None:
            s.add(IndiceEconomico(codigo=codigo, competencia=quando,
                                  variacao_pct=variacao, fonte="BOLETIM"))
        else:
            atual.variacao_pct = variacao
            atual.fonte = "BOLETIM"
        gravados += 1

    com_numero = [l for l in linhas
                  if l["numero_indice"] is not None and l["competencia"] <= hoje]
    ancora = None
    if com_numero:
        referencia = com_numero[-1]           # o mês mais recente do boletim
        ancora = {"competencia": referencia["competencia"].strftime("%m/%Y"),
                  "numero_indice": _curto(referencia["numero_indice"], 3)}
    if not simular:
        s.flush()
        recalcular_numeros(s, codigo)
        if ancorar and com_numero:
            referencia = com_numero[-1]
            definir_ancora(s, codigo=codigo,
                           competencia=referencia["competencia"],
                           numero_indice=referencia["numero_indice"],
                           observacao="do boletim colado", usuario=usuario)
        registrar_evento(s, "indice", 0, "INDICE_BOLETIM_COLADO", {
            "codigo": codigo, "meses": len(resultado), "gravados": gravados,
            "ancorado": bool(ancorar and com_numero)},
            usuario.id if usuario else None)
        avisos += _conferir_contra_o_papel(s, codigo, com_numero)

    return {"codigo": codigo, "meses": resultado, "gravados": gravados,
            "ancora": ancora, "avisos": avisos,
            "ignoradas": lido["ignoradas"], "simulacao": simular}


# Diferença entre o número do papel e o que o sistema recompõe, acima da qual
# vale avisar. 0,02% sobre um contrato de R$ 1,5 milhão dá cerca de R$ 300 —
# abaixo disso é ruído de arredondamento e avisar só assusta à toa.
FOLGA_CONTRA_O_PAPEL = Decimal("0.02")


def _conferir_contra_o_papel(s: Session, codigo: str,
                             com_numero: list[dict[str, Any]]) -> list[str]:
    """Depois de gravar: o que está na tela bate com o que está no papel?

    Bate no mês ancorado por construção, e se afasta um pouco nos outros — o
    boletim publica a VARIAÇÃO arredondada em duas casas, e o sistema recompõe
    os demais meses multiplicando e dividindo por elas. O erro é de centésimos,
    mas é melhor ele estar escrito na tela do que ser descoberto pelo dono
    comparando com o papel e concluindo que o sistema errou de novo.
    """
    pior = None
    for ln in com_numero:
        linha = s.get(IndiceEconomico, (codigo, ln["competencia"]))
        if linha is None or linha.numero_indice is None or not ln["numero_indice"]:
            continue
        do_papel = Decimal(str(ln["numero_indice"]))
        if do_papel == 0:
            continue
        diferenca = abs(Decimal(str(linha.numero_indice)) - do_papel) / do_papel * 100
        if pior is None or diferenca > pior[1]:
            pior = (ln["competencia"], diferenca, do_papel,
                    Decimal(str(linha.numero_indice)))
    if pior is None or pior[1] <= FOLGA_CONTRA_O_PAPEL:
        return []
    quando, dif, papel, sistema = pior
    return [f"A régua foi alinhada pelo mês mais recente, então os meses "
            f"anteriores podem sair uns centésimos do papel — a maior diferença "
            f"é em {quando.strftime('%m/%Y')}: o boletim diz {_curto(papel, 3)} "
            f"e o sistema mostra {_curto(sistema, 3)} ({_curto(dif, 3)}%). "
            f"Isso vem de o boletim publicar a variação com duas casas; o "
            f"reajuste usa a razão entre dois meses, e nessa razão a diferença "
            f"é menor ainda."]
