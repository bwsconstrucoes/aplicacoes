# ============================================================================
# ERP — core/locacoes_conferencia.py
# A prestação de contas mensal dos equipamentos locados.
#
# O PROBLEMA, nas palavras do dono: "muitas vezes eles são locados e deixam de
# ser utilizados, não são devolvidos". O aluguel corre, ninguém devolve, e
# meses depois já se pagou mais do que custaria comprar.
#
# O ERP JÁ SABIA GRITAR ISSO — `_alertas`, em core/locacoes.py, avisa "10 meses
# locado, o aluguel já paga a compra" desde sempre. O que faltava era ALGUÉM
# SER OBRIGADO A RESPONDER. É só isso que este módulo acrescenta, e é por isso
# que ele é simples: uma pergunta por mês, com nome de quem responde.
#
# QUATRO DECISÕES, todas do dono, ditas aqui para ninguém desfazer sem saber:
#
# 1. MENSAL, e quem responde é o ADMINISTRATIVO DA OBRA.
# 2. SEM FOTO. Sem etiqueta no equipamento a foto prova pouco — metadado se
#    falsifica, e o WhatsApp apaga o que existe — e daria trabalho a todo mundo
#    todo mês.
# 3. A CONFERÊNCIA NÃO BLOQUEIA O PAGAMENTO do aluguel. Bloquear trocaria
#    equipamento esquecido por multa e briga com a locadora. Ela vira pendência
#    e AVISA quem vai lançar a parcela.
# 4. "DEVOLVER" E "REMANEJAR" NA CONFERÊNCIA FAZEM A COISA ACONTECER, chamando
#    as mesmas funções do contrato. Conferência que registra intenção e não faz
#    nada é papel — e papel não devolve equipamento.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Obra, Usuario
from app.apps.erp.db.models.financeiro import (
    ContratoLocacao, LocacaoConferencia, LocacaoConferenciaItem, LocacaoItem,
)

logger = logging.getLogger(__name__)

PRESENTES = {"SIM", "NAO", "NAO_ENCONTRADO"}
DECISOES = {"MANTER", "DEVOLVER", "REMANEJAR"}

ROTULO_PRESENTE = {
    "SIM": "Está na obra",
    "NAO": "Não está mais aqui",
    "NAO_ENCONTRADO": "Não encontrei",
}
ROTULO_DECISAO = {
    "MANTER": "Manter na obra",
    "DEVOLVER": "Devolver à locadora",
    "REMANEJAR": "Remanejar para outra obra",
}

# Quantos dias depois do fim do mês a conferência passa a ser cobrança, e não
# lembrete. Dez dias é o que a obra leva para fechar o mês dela.
DIAS_PARA_COBRAR = 10


def competencia_de(quando: Optional[date] = None) -> date:
    """O primeiro dia do mês. É a chave da conferência."""
    d = quando or date.today()
    return date(d.year, d.month, 1)


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


# ---------------------------------------------------------------------------
# Abrir a conferência do mês
# ---------------------------------------------------------------------------
def abrir_do_mes(s: Session, *, competencia: Optional[date] = None,
                 usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Abre uma conferência por contrato ATIVO, para a competência.

    É seguro chamar quantas vezes for: a restrição do banco garante uma por
    contrato por mês, e aqui as já existentes são puladas. Isso importa porque
    a rotina vai rodar todo dia — e abrir duas faria a obra responder a mesma
    coisa duas vezes sem saber qual vale.
    """
    comp = competencia or competencia_de()
    ja_tem = {c.contrato_id for c in s.scalars(
        select(LocacaoConferencia).where(
            LocacaoConferencia.competencia == comp)).all()}

    abertas = []
    for contrato in s.scalars(select(ContratoLocacao)).all():
        if contrato.status != "ATIVO" or contrato.id in ja_tem:
            continue
        # contrato que começou depois do mês conferido não tem o que conferir
        if contrato.data_inicio and contrato.data_inicio > _fim_do_mes(comp):
            continue
        conf = LocacaoConferencia(
            contrato_id=contrato.id, competencia=comp,
            obra_id=contrato.obra_id,
            responsavel_id=_quem_responde(s, contrato),
            situacao="ABERTA")
        s.add(conf)
        abertas.append(contrato.numero)
    s.flush()

    if abertas:
        registrar_evento(s, "locacao_conferencia", 0, "ABERTAS", {
            "competencia": comp.isoformat(), "contratos": abertas},
            usuario.id if usuario else None)
        logger.info("ERP/locação: %d conferência(s) abertas para %s",
                    len(abertas), comp.strftime("%m/%Y"))
    return {"competencia": comp.isoformat(), "abertas": abertas}


def _fim_do_mes(comp: date) -> date:
    return (date(comp.year + 1, 1, 1) if comp.month == 12
            else date(comp.year, comp.month + 1, 1))


def responsaveis_da_obra(s: Session, obra_id: Optional[int]) -> list[int]:
    """Quem foi MARCADO no cadastro para responder por esta obra.

    Decisão do dono (07/09/2026): deixa de ser adivinhação e passa a ser
    escrito. Pode haver mais de um — "se por acaso tiverem dois, a gente
    cadastrar dois, permitir também, os dois recebem".

    Até 07/09 isto era um palpite que NUNCA funcionou: a função procurava
    campos (`administrativo_id`, `encarregado_id`) que não existem na obra, e
    portanto caía sempre no responsável do contrato. Fica registrado porque é o
    tipo de defeito que não dá erro — só endereça a cobrança para a pessoa
    errada, para sempre.
    """
    from app.apps.erp.db.models.cadastros import UsuarioObra
    if not obra_id:
        return []
    return [v.usuario_id for v in s.scalars(select(UsuarioObra).where(
        UsuarioObra.obra_id == obra_id, UsuarioObra.responsavel.is_(True))).all()]


def _quem_responde(s: Session, contrato: ContratoLocacao) -> Optional[int]:
    """O primeiro responsável marcado na obra.

    Sem ninguém marcado, cai no responsável do contrato: melhor endereçar a
    alguém do que abrir pendência sem dono, que ninguém lê. Quando há mais de
    um, todos recebem a cobrança — quem cuida disso é o agente, olhando
    `responsaveis` na pendência.
    """
    marcados = responsaveis_da_obra(s, contrato.obra_id)
    return marcados[0] if marcados else contrato.responsavel_id


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
def obter(s: Session, conferencia_id: int) -> dict[str, Any]:
    """A conferência com a lista do que o sistema acha que está na obra."""
    conf = s.get(LocacaoConferencia, conferencia_id)
    if conf is None:
        raise ErroNaoEncontrado("Conferência não encontrada.")
    contrato = s.get(ContratoLocacao, conf.contrato_id)
    obra = s.get(Obra, conf.obra_id) if conf.obra_id else None
    respostas = {r.item_id: r for r in s.scalars(
        select(LocacaoConferenciaItem).where(
            LocacaoConferenciaItem.conferencia_id == conf.id)).all()}
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}

    itens = []
    for item in s.scalars(select(LocacaoItem)).all():
        if item.contrato_id != conf.contrato_id:
            continue
        em_obra = Decimal(item.quantidade or 0) - Decimal(item.quantidade_devolvida or 0)
        if em_obra <= 0:
            continue                      # já voltou: não há o que conferir
        r = respostas.get(item.id)
        itens.append({
            "item_id": item.id,
            "descricao": item.descricao,
            "em_obra": float(em_obra),
            "valor_periodo": float(em_obra * Decimal(item.valor_unitario or 0)),
            "obra": obras.get(item.obra_id) or (obra.codigo if obra else ""),
            "devolucao_prevista": (item.devolucao_prevista.isoformat()
                                   if item.devolucao_prevista else None),
            "devolucao_original": (item.devolucao_prevista_original.isoformat()
                                   if item.devolucao_prevista_original else None),
            "atrasado": bool(item.devolucao_prevista
                             and item.devolucao_prevista < date.today()),
            "dias_de_atraso": ((date.today() - item.devolucao_prevista).days
                               if item.devolucao_prevista
                               and item.devolucao_prevista < date.today() else 0),
            # o que já foi respondido, para a tela abrir preenchida
            "presente": getattr(r, "presente", None),
            "em_uso": getattr(r, "em_uso", None),
            "onde_esta": getattr(r, "onde_esta", None),
            "decisao": getattr(r, "decisao", None),
            "motivo": getattr(r, "motivo", None),
        })

    quem = s.get(Usuario, conf.responsavel_id) if conf.responsavel_id else None
    respondeu = s.get(Usuario, conf.respondida_por) if conf.respondida_por else None
    return {
        "id": conf.id,
        "contrato_id": conf.contrato_id,
        "contrato": getattr(contrato, "numero", ""),
        "locadora_id": getattr(contrato, "fornecedor_id", None),
        "obra": getattr(obra, "codigo", ""),
        "competencia": conf.competencia.strftime("%m/%Y"),
        "competencia_iso": conf.competencia.isoformat(),
        "situacao": conf.situacao,
        "responsavel": getattr(quem, "nome", "—"),
        "respondida_por": getattr(respondeu, "nome", None),
        "respondida_em": (conf.respondida_em.isoformat()
                          if conf.respondida_em else None),
        "observacao": conf.observacao,
        "itens": itens,
        "custo_do_periodo": round(sum(i["valor_periodo"] for i in itens), 2),
        "atrasados": sum(1 for i in itens if i["atrasado"]),
    }


def pendentes(s: Session, *, usuario: Optional[Usuario] = None,
              todas: bool = False) -> list[dict[str, Any]]:
    """As conferências que ainda esperam resposta.

    Sem `todas`, devolve só as do usuário — é a lista "o que espera por mim".
    Com `todas`, é a visão de quem cobra.
    """
    hoje = date.today()
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}
    nomes = {u.id: u.nome for u in s.scalars(select(Usuario)).all()}
    saida = []
    for conf in s.scalars(select(LocacaoConferencia)).all():
        if conf.situacao != "ABERTA":
            continue
        if not todas and usuario is not None and conf.responsavel_id != usuario.id:
            continue
        contrato = s.get(ContratoLocacao, conf.contrato_id)
        vencimento = _fim_do_mes(conf.competencia)
        dias = (hoje - vencimento).days
        saida.append({
            "id": conf.id, "contrato_id": conf.contrato_id,
            "contrato": getattr(contrato, "numero", ""),
            "locadora_id": getattr(contrato, "fornecedor_id", None),
            "obra": obras.get(conf.obra_id, ""),
            "competencia": conf.competencia.strftime("%m/%Y"),
            "responsavel": nomes.get(conf.responsavel_id, "— sem responsável —"),
            "responsavel_id": conf.responsavel_id,
            # todos os marcados na obra: é para todos eles que o agente manda
            "responsaveis": (responsaveis_da_obra(s, conf.obra_id)
                             or ([conf.responsavel_id] if conf.responsavel_id else [])),
            "dias_de_atraso": max(0, dias),
            "cobrar": dias >= DIAS_PARA_COBRAR,
        })
    saida.sort(key=lambda x: (-x["dias_de_atraso"], x["contrato"]))
    return saida


def houve_conferencia(s: Session, contrato_id: int,
                      competencia: Optional[date] = None) -> dict[str, Any]:
    """A obra conferiu este contrato no mês? É o que o financeiro pergunta
    antes de lançar a parcela do aluguel."""
    comp = competencia or competencia_de()
    conf = next((c for c in s.scalars(select(LocacaoConferencia)).all()
                 if c.contrato_id == contrato_id and c.competencia == comp), None)
    if conf is None:
        return {"conferido": False, "situacao": "NAO_ABERTA",
                "aviso": (f"A obra não conferiu estes equipamentos em "
                          f"{comp.strftime('%m/%Y')}. Pode estar pagando "
                          f"aluguel de coisa que não está mais lá.")}
    if conf.situacao != "RESPONDIDA":
        quem = s.get(Usuario, conf.responsavel_id) if conf.responsavel_id else None
        return {"conferido": False, "situacao": conf.situacao,
                "conferencia_id": conf.id,
                "responsavel": getattr(quem, "nome", "—"),
                "aviso": (f"A conferência de {comp.strftime('%m/%Y')} está "
                          f"aberta com {getattr(quem, 'nome', 'a obra')} e ainda "
                          f"não foi respondida.")}
    return {"conferido": True, "situacao": "RESPONDIDA",
            "conferencia_id": conf.id,
            "respondida_em": (conf.respondida_em.isoformat()
                              if conf.respondida_em else None)}


# ---------------------------------------------------------------------------
# Responder
# ---------------------------------------------------------------------------
def responder(s: Session, conferencia_id: int, dados: dict[str, Any],
              usuario: Usuario) -> dict[str, Any]:
    """Grava a conferência e FAZ o que ela decidiu.

    Devolver e remanejar chamam as mesmas funções do contrato — as mesmas que
    o botão da ficha usa. Conferência que só registra intenção é papel.
    """
    from app.apps.erp.core import locacoes as svc_contrato

    conf = s.get(LocacaoConferencia, conferencia_id, with_for_update=True,
                 populate_existing=True)
    if conf is None:
        raise ErroNaoEncontrado("Conferência não encontrada.")
    if conf.situacao == "RESPONDIDA":
        raise ErroValidacao(
            f"A conferência de {conf.competencia:%m/%Y} já foi respondida. "
            f"Para corrigir, abra um movimento no contrato.")

    respostas = dados.get("itens") or []
    if not respostas:
        raise ErroValidacao("Responda pelo menos um equipamento.")

    itens = {i.id: i for i in s.scalars(select(LocacaoItem)).all()
             if i.contrato_id == conf.contrato_id}
    ja_respondido = {r.item_id: r for r in s.scalars(
        select(LocacaoConferenciaItem).where(
            LocacaoConferenciaItem.conferencia_id == conf.id)).all()}

    feitos, avisos = [], []
    for bruto in respostas:
        item = itens.get(int(bruto.get("item_id") or 0))
        if item is None:
            raise ErroValidacao("Equipamento não é deste contrato.")

        presente = (bruto.get("presente") or "").upper()
        if presente not in PRESENTES:
            raise ErroValidacao(
                f"{item.descricao}: diga se está na obra, se não está mais, "
                f"ou se não encontrou.")
        decisao = (bruto.get("decisao") or "MANTER").upper()
        if decisao not in DECISOES:
            raise ErroValidacao(f"{item.descricao}: decisão inválida.")

        onde = _texto(bruto.get("onde_esta"))
        # "onde está e para quê" é o campo que mais vale: "na laje do bloco B,
        # escorando até desforma" é uma resposta; "está aí" é outra.
        if presente == "SIM" and len(onde) < 3:
            raise ErroValidacao(
                f"{item.descricao}: diga onde está e para que está sendo usado.")
        if presente != "SIM" and len(_texto(bruto.get("motivo"))) < 3:
            raise ErroValidacao(
                f"{item.descricao}: escreva o que aconteceu com ele.")

        prevista = _data(bruto.get("devolucao_prevista"))
        if prevista and item.devolucao_prevista and prevista != item.devolucao_prevista:
            if len(_texto(bruto.get("motivo"))) < 3:
                raise ErroValidacao(
                    f"{item.descricao}: empurrar a devolução exige o motivo.")
            avisos.append(
                f"{item.descricao}: devolução adiada de "
                f"{item.devolucao_prevista:%d/%m/%Y} para {prevista:%d/%m/%Y}.")

        resposta = ja_respondido.get(item.id) or LocacaoConferenciaItem(
            conferencia_id=conf.id, item_id=item.id)
        resposta.presente = presente
        resposta.em_uso = bool(bruto.get("em_uso"))
        resposta.onde_esta = onde or None
        resposta.decisao = decisao
        resposta.motivo = _texto(bruto.get("motivo")) or None
        resposta.devolucao_prevista = prevista
        resposta.obra_destino_id = (int(bruto["obra_destino_id"])
                                    if bruto.get("obra_destino_id") else None)
        if bruto.get("quantidade_conferida") not in (None, ""):
            resposta.quantidade_conferida = Decimal(str(bruto["quantidade_conferida"]))
        if resposta.id is None:
            s.add(resposta)

        if prevista:
            if item.devolucao_prevista_original is None:
                item.devolucao_prevista_original = item.devolucao_prevista or prevista
            item.devolucao_prevista = prevista

        feitos.append(_executar(s, svc_contrato, conf, item, resposta, usuario))

    s.flush()
    conf.situacao = "RESPONDIDA"
    conf.respondida_por = usuario.id if usuario else None
    conf.respondida_em = datetime.now(timezone.utc)
    conf.observacao = _texto(dados.get("observacao")) or None
    s.flush()

    registrar_evento(s, "locacao_conferencia", conf.id, "RESPONDIDA", {
        "contrato_id": conf.contrato_id,
        "competencia": conf.competencia.isoformat(),
        "itens": len(respostas),
        "acoes": [f for f in feitos if f]}, usuario.id if usuario else None)

    return {"conferencia_id": conf.id,
            "acoes": [f for f in feitos if f],
            "avisos": avisos,
            "resumo": _resumo(feitos, avisos)}


def _executar(s: Session, svc_contrato, conf: LocacaoConferencia,
              item: LocacaoItem, resposta: LocacaoConferenciaItem,
              usuario: Usuario) -> Optional[str]:
    """Faz acontecer o que a conferência decidiu."""
    em_obra = Decimal(item.quantidade or 0) - Decimal(item.quantidade_devolvida or 0)
    if resposta.decisao == "DEVOLVER":
        svc_contrato.devolver(s, conf.contrato_id, {
            "item_id": item.id, "quantidade": str(em_obra),
            "data_movimento": date.today().isoformat(),
            "observacao": f"Conferência de {conf.competencia:%m/%Y}: "
                          f"{resposta.motivo or resposta.onde_esta or 'devolver'}",
        }, usuario)
        return f"{item.descricao}: devolução de {em_obra:g} registrada."
    if resposta.decisao == "REMANEJAR":
        if not resposta.obra_destino_id:
            raise ErroValidacao(
                f"{item.descricao}: para remanejar, diga para qual obra vai.")
        svc_contrato.remanejar(s, conf.contrato_id, {
            "item_id": item.id, "obra_destino_id": resposta.obra_destino_id,
            "observacao": f"Conferência de {conf.competencia:%m/%Y}: "
                          f"{resposta.motivo or ''}".strip(),
        }, usuario)
        obra = s.get(Obra, resposta.obra_destino_id)
        return f"{item.descricao}: remanejado para {getattr(obra, 'codigo', '?')}."
    if resposta.presente == "NAO_ENCONTRADO":
        # não some com o equipamento: vira alerta e continua sendo cobrado,
        # que é justamente o que faz alguém procurar
        return (f"{item.descricao}: NÃO ENCONTRADO na obra — "
                f"{resposta.motivo or 'sem explicação'}.")
    return None


def _resumo(feitos: list, avisos: list) -> str:
    acoes = [f for f in feitos if f]
    partes = ["Conferência registrada."]
    if acoes:
        partes.append(f"{len(acoes)} ação(ões) executada(s).")
    if avisos:
        partes.append(f"{len(avisos)} devolução(ões) adiada(s).")
    return " ".join(partes)


def _data(valor: Any) -> Optional[date]:
    if not valor:
        return None
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError:
        raise ErroValidacao(f"Data inválida: {valor}")
