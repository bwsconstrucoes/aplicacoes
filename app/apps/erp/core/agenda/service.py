# ============================================================================
# ERP — core/agenda/service.py
# A agenda: sincronizar, listar, resolver.
#
# A SINCRONIZAÇÃO É IDEMPOTENTE E LIMPA ATRÁS DE SI
#
# Rodar duas vezes no mesmo dia não duplica nada (a chave é única), e o evento
# gerado que deixou de valer é APAGADO — certidão renovada, contrato encerrado,
# conferência respondida. Sem essa limpeza a agenda vira depósito de aviso
# velho, e aí a pessoa para de abrir; uma agenda em que não se confia é pior
# que agenda nenhuma, porque dá a sensação de que alguém está olhando.
#
# TRÊS COISAS NUNCA SÃO APAGADAS, e cada uma por um motivo diferente:
#
#   RESOLVIDO    é histórico: quem tratou, quando, e o que escreveu.
#   DISPENSADO   é decisão: alguém olhou e disse "não vale para nós". Apagar
#                faria o aviso voltar amanhã, e a pessoa dispensaria de novo,
#                para sempre.
#   MANUAL       ninguém deduziu, então ninguém pode deduzir que sumiu.
#
# QUANDO A SINCRONIZAÇÃO ACONTECE: ao ABRIR a tela da agenda, e no botão. Não
# há tarefa de fundo, e é de propósito — o ERP divide um processo com treze
# módulos, e trabalho pesado no meio de uma requisição é o que já derrubou o
# serviço antes. Os geradores são consultas curtas; se um dia pesarem, aí sim
# vira tarefa separada (está no roteiro).
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.agenda import geradores
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Empresa, Obra, Usuario
from app.apps.erp.db.models.financeiro import AgendaEvento

logger = logging.getLogger(__name__)

ROTULO_ORIGEM = {
    "REAJUSTE": "Reajuste",
    "CERTIDAO": "Documento vencendo",
    "LOCACAO": "Equipamento locado",
    "CONTRATO": "Contrato",
    "CERTIFICADO": "Certificado digital",
    "MANUAL": "Anotação",
}


def sincronizar(s: Session, *, hoje: Optional[date] = None,
                usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Recalcula os eventos deduzidos: cria o que falta, apaga o que caducou."""
    hoje = hoje or date.today()
    calculados: dict[str, dict[str, Any]] = {}
    falhas: list[str] = []
    for gerar in geradores.TODOS:
        try:
            for e in gerar(s, hoje):
                calculados[e["chave"]] = e
        except Exception as erro:
            # Um gerador com defeito não pode derrubar a agenda inteira: o
            # resto dos avisos continua valendo, e a falha fica dita.
            logger.exception("ERP/agenda: gerador %s falhou", gerar.__name__)
            falhas.append(f"{gerar.__name__}: {erro}")

    existentes = {e.chave: e for e in s.scalars(select(AgendaEvento)).all()}

    criados, atualizados = 0, 0
    for chave, dados in calculados.items():
        atual = existentes.get(chave)
        if atual is None:
            s.add(AgendaEvento(situacao="ABERTO", **dados))
            criados += 1
            continue
        if atual.situacao != "ABERTO":
            continue                       # resolvido/dispensado não volta
        # O texto pode mudar (o valor previsto do reajuste, por exemplo).
        for campo in ("titulo", "detalhe", "quando", "avisar_em", "link",
                      "obra_id", "empresa_id"):
            if getattr(atual, campo) != dados[campo]:
                setattr(atual, campo, dados[campo])
                atualizados += 1

    apagados = 0
    for chave, atual in existentes.items():
        if chave in calculados or atual.origem == "MANUAL":
            continue
        if atual.situacao != "ABERTO":
            continue                       # histórico não se apaga
        s.delete(atual)
        apagados += 1

    s.flush()
    resumo = {"criados": criados, "atualizados": atualizados,
              "apagados": apagados, "em_aberto": len(calculados),
              "falhas": falhas}
    if criados or apagados:
        logger.info("ERP/agenda: %s criados, %s apagados", criados, apagados)
    return resumo


def listar(s: Session, *, situacao: str = "ABERTO", origem: str = "",
           obra_id: Optional[int] = None, ate: Optional[date] = None,
           incluir_futuros: bool = False, hoje: Optional[date] = None,
           limite: int = 500) -> dict[str, Any]:
    """O que está na agenda, do mais urgente para o menos.

    Por padrão mostra só o que JÁ chegou a hora de avisar. O que vence daqui a
    três meses existe na tabela, mas ocupar a tela com ele hoje é o caminho
    para a pessoa parar de olhar a tela.
    """
    hoje = hoje or date.today()
    stmt = select(AgendaEvento)
    if situacao:
        stmt = stmt.where(AgendaEvento.situacao == situacao)
    if origem:
        stmt = stmt.where(AgendaEvento.origem == origem)
    if obra_id:
        stmt = stmt.where(AgendaEvento.obra_id == obra_id)
    if ate:
        stmt = stmt.where(AgendaEvento.quando <= ate)
    if not incluir_futuros:
        stmt = stmt.where(AgendaEvento.avisar_em <= hoje)
    itens = list(s.scalars(stmt.order_by(AgendaEvento.quando).limit(limite)).all())

    linhas = [_ler(s, e, hoje) for e in itens]
    return {
        "eventos": linhas,
        "resumo": {
            "total": len(linhas),
            "vencidos": sum(1 for l in linhas if l["dias"] < 0),
            "hoje": sum(1 for l in linhas if l["dias"] == 0),
            "esta_semana": sum(1 for l in linhas if 0 <= l["dias"] <= 7),
            "por_origem": {o: sum(1 for l in linhas if l["origem"] == o)
                           for o in ROTULO_ORIGEM},
        },
    }


def _ler(s: Session, e: AgendaEvento, hoje: date) -> dict[str, Any]:
    obra = s.get(Obra, e.obra_id) if e.obra_id else None
    empresa = s.get(Empresa, e.empresa_id) if e.empresa_id else None
    quem = s.get(Usuario, e.resolvido_por) if e.resolvido_por else None
    dias = (e.quando - hoje).days
    return {
        "id": e.id, "chave": e.chave, "origem": e.origem,
        "origem_nome": ROTULO_ORIGEM.get(e.origem, e.origem),
        "titulo": e.titulo, "detalhe": e.detalhe or "",
        "quando": e.quando.isoformat(),
        "dias": dias,
        # A frase que a tela mostra. Calculada aqui para as duas telas (agenda
        # e início) dizerem exatamente a mesma coisa.
        "prazo": ("vencido há {} dia(s)".format(-dias) if dias < 0
                  else "é hoje" if dias == 0
                  else "em {} dia(s)".format(dias)),
        "obra": getattr(obra, "codigo", "") or "",
        "empresa": (getattr(empresa, "nome_fantasia", "")
                    or getattr(empresa, "razao_social", "") or ""),
        "link": e.link or "",
        "situacao": e.situacao,
        "observacao": e.observacao or "",
        "resolvido_por": getattr(quem, "nome", "") or "",
        "resolvido_em": e.resolvido_em.isoformat() if e.resolvido_em else None,
        "manual": e.origem == "MANUAL",
    }


# ---------------------------------------------------------------------------
# O que a pessoa faz com o aviso
# ---------------------------------------------------------------------------
def resolver(s: Session, evento_id: int, *, observacao: str = "",
             dispensar: bool = False, usuario: Optional[Usuario] = None) -> AgendaEvento:
    """Marca o aviso como tratado (ou dispensado, com motivo).

    Dispensar EXIGE motivo: "não se aplica" sem explicação, três meses depois,
    é indistinguível de esquecimento — e é justamente o que alguém vai querer
    entender quando o problema aparecer.
    """
    e = s.get(AgendaEvento, evento_id)
    if e is None:
        raise ErroValidacao("Aviso não encontrado.")
    if e.situacao != "ABERTO":
        raise ErroValidacao(f"Este aviso já está {e.situacao.lower()}.")
    observacao = (observacao or "").strip()
    if dispensar and len(observacao) < 3:
        raise ErroValidacao("Para dispensar, escreva o motivo — sem ele ninguém "
                            "distingue 'não se aplica' de 'esqueceram'.")

    e.situacao = "DISPENSADO" if dispensar else "RESOLVIDO"
    e.observacao = observacao or None
    e.resolvido_por = usuario.id if usuario else None
    e.resolvido_em = datetime.now()
    s.flush()
    registrar_evento(s, "agenda", e.id,
                     "DISPENSADO" if dispensar else "RESOLVIDO",
                     {"chave": e.chave, "observacao": observacao},
                     usuario.id if usuario else None)
    return e


def reabrir(s: Session, evento_id: int, *, usuario: Optional[Usuario] = None) -> AgendaEvento:
    """Errou o clique, ou o assunto voltou. Reabrir é barato; perder o aviso
    porque alguém clicou errado, não."""
    e = s.get(AgendaEvento, evento_id)
    if e is None:
        raise ErroValidacao("Aviso não encontrado.")
    e.situacao = "ABERTO"
    e.resolvido_por, e.resolvido_em = None, None
    s.flush()
    registrar_evento(s, "agenda", e.id, "REABERTO", {"chave": e.chave},
                     usuario.id if usuario else None)
    return e


def criar_manual(s: Session, *, titulo: str, quando: date, detalhe: str = "",
                 avisar_dias: int = 7, obra_id: Optional[int] = None,
                 usuario: Optional[Usuario] = None) -> AgendaEvento:
    """A anotação que ninguém deduz: "entregar a declaração no dia 20"."""
    from datetime import timedelta

    titulo = (titulo or "").strip()
    if len(titulo) < 3:
        raise ErroValidacao("Escreva do que se trata.")
    if avisar_dias < 0:
        raise ErroValidacao("A antecedência do aviso não pode ser negativa.")

    # A chave carrega o instante de criação: duas anotações iguais no mesmo dia
    # são duas anotações, não uma repetida.
    e = AgendaEvento(
        chave=f"MANUAL:{datetime.now().timestamp():.6f}",
        origem="MANUAL", titulo=titulo, detalhe=(detalhe or "").strip() or None,
        quando=quando, avisar_em=quando - timedelta(days=avisar_dias),
        obra_id=obra_id, situacao="ABERTO",
        criado_por=usuario.id if usuario else None)
    s.add(e)
    s.flush()
    registrar_evento(s, "agenda", e.id, "ANOTADO", {"titulo": titulo,
                     "quando": quando.isoformat()}, usuario.id if usuario else None)
    return e


def apagar_manual(s: Session, evento_id: int, *,
                  usuario: Optional[Usuario] = None) -> None:
    """Só anotação se apaga. Aviso deduzido pelo sistema se resolve ou se
    dispensa — apagar faria ele voltar na próxima sincronização, e a pessoa
    apagaria de novo, para sempre."""
    e = s.get(AgendaEvento, evento_id)
    if e is None:
        raise ErroValidacao("Aviso não encontrado.")
    if e.origem != "MANUAL":
        raise ErroValidacao(
            "Este aviso foi deduzido pelo sistema — apagar não adianta, ele "
            "volta na próxima conferência. Marque como resolvido ou dispense "
            "com o motivo.")
    registrar_evento(s, "agenda", e.id, "ANOTACAO_APAGADA", {"titulo": e.titulo},
                     usuario.id if usuario else None)
    s.delete(e)
    s.flush()


def contagem(s: Session, *, hoje: Optional[date] = None) -> dict[str, int]:
    """O número que a tela de início mostra. Consulta curta de propósito: ela
    roda em toda visita à porta de entrada."""
    hoje = hoje or date.today()
    abertos = list(s.scalars(select(AgendaEvento).where(
        AgendaEvento.situacao == "ABERTO",
        AgendaEvento.avisar_em <= hoje)).all())
    return {"abertos": len(abertos),
            "vencidos": sum(1 for e in abertos if e.quando < hoje)}
