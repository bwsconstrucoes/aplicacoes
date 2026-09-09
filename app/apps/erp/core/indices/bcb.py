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
from app.apps.erp.db.models.cadastros import IndiceEconomico

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
    registrar_evento(s, "indice", 0, "INDICE_LANCADO_A_MAO", {
        "codigo": codigo, "competencia": quando.isoformat(), "valor": str(valor)},
        usuario.id if usuario else None)
    return linha


def listar(s: Session, codigo: str = PADRAO, *, limite: int = 60) -> dict[str, Any]:
    """Os últimos meses guardados, do mais novo para o mais velho."""
    codigo = (codigo or PADRAO).strip().upper()
    linhas = list(s.scalars(select(IndiceEconomico).where(
        IndiceEconomico.codigo == codigo)
        .order_by(IndiceEconomico.competencia.desc()).limit(limite)).all())
    return {
        "codigo": codigo,
        "nome": CATALOGO.get(codigo, (0, codigo))[1],
        "serie": CATALOGO.get(codigo, (0, ""))[0],
        "meses": [{"competencia": i.competencia.isoformat(),
                   "variacao_pct": float(i.variacao_pct),
                   "fonte": i.fonte,
                   "coletado_em": i.coletado_em.isoformat() if i.coletado_em else None}
                  for i in linhas],
        "ultimo": linhas[0].competencia.isoformat() if linhas else None,
        # A pergunta prática: a tabela está em dia? O mês corrente ainda não
        # existe, então "em dia" é ter o mês ANTERIOR.
        "em_dia": bool(linhas and linhas[0].competencia >= _mes_anterior(date.today())),
    }


def _mes_anterior(d: date) -> date:
    primeiro = d.replace(day=1)
    return (primeiro.replace(year=primeiro.year - 1, month=12) if primeiro.month == 1
            else primeiro.replace(month=primeiro.month - 1))
