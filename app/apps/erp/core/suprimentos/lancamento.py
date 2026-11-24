# ============================================================================
# ERP — core/suprimentos/lancamento.py
# A conta a pagar nasce do recebimento.
#
# O dono, em 20/11/2026: "eu quero gerar o financeiro a partir do recebimento
# (…) que atualmente é assim que funciona. A pessoa vai receber o pedido e
# lança nota, lança boleto, lança tudo e gera a obrigação financeira. Ou
# confirma uma previsão que já existe."
#
# COMO, E POR QUE ASSIM. Este módulo NÃO cria título. Ele faz duas coisas:
#
#   1. `preparar` — entrega à tela de Lançar tudo o que o pedido já sabe:
#      credor, valor do que chegou, rateio por obra com a conta do plano de
#      cada insumo, e as parcelas da previsão de pagamento. A pessoa joga a
#      nota e o boleto, confere e envia.
#   2. `vincular` — depois que o título foi criado PELO CAMINHO DE SEMPRE,
#      grava que ele é deste pedido e baixa a previsão correspondente.
#
# O título continua nascendo em `titulos.service.criar_titulo`, com as
# críticas fiscais, de duplicidade e de alçada que já valem para qualquer
# lançamento. Um segundo caminho para o dinheiro sair, por dentro do
# suprimento, é justamente o que o ERP evita (`SUPRIMENTOS.md` §8.2).
#
# "CONFIRMA UMA PREVISÃO QUE JÁ EXISTE": as parcelas vêm da previsão gerada na
# autorização do pedido. Lançar o título marca essas previsões como
# cumpridas — e elas saem da Projeção de pagamentos, que é só do que AINDA
# não virou conta.
# ============================================================================
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.core.suprimentos import acompanhamento as acomp
from app.apps.erp.db.models.cadastros import (
    Insumo, PedidoCompra, PedidoItem, PedidoTitulo, PrevisaoPagamento,
    Recebimento, RecebimentoItem, StatusPedidoCompra, SuprimentoItem, Usuario,
)

logger = logging.getLogger(__name__)

_CENT = Decimal("0.01")
_ZERO = Decimal("0")


def _d(v: Any) -> Decimal:
    return Decimal(str(v if v is not None else 0))


def _pedido_autorizado(s: Session, usuario: Usuario, pedido_id: int) -> PedidoCompra:
    from app.apps.erp.core.suprimentos.recebimento import exigir_pedido_no_escopo
    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    exigir_pedido_no_escopo(s, usuario, pedido_id)
    if pedido.status is not StatusPedidoCompra.AUTORIZADO:
        raise ErroValidacao(
            f"O pedido {pedido.numero} não está autorizado — não há o que lançar.")
    return pedido


def preparar(s: Session, usuario: Usuario, pedido_id: int, *,
             recebimento_id: Optional[int] = None,
             hoje: Optional[date] = None) -> dict[str, Any]:
    """O lançamento já preenchido com o que o pedido sabe.

    O VALOR é o do que CHEGOU e ainda não virou conta — não o do pedido
    inteiro: dez carradas viram dez notas. Com o pedido entregue por inteiro,
    o frete e o desconto do pedido entram na conta final.
    """
    hoje = hoje or date.today()
    pedido = _pedido_autorizado(s, usuario, pedido_id)
    base = acomp.carregar(s, [pedido])
    itens = base["itens"].get(pedido.id, [])
    fin = acomp.financeiro_do_pedido(pedido, base)
    total = acomp.total_do_pedido(pedido, itens)
    entregue = bool(itens) and all(i["saldo"] <= 0 for i in itens)

    recebimento = None
    if recebimento_id:
        recebimento = s.get(Recebimento, int(recebimento_id))
        if recebimento is None or recebimento.pedido_id != pedido.id:
            raise ErroNaoEncontrado("Recebimento não encontrado.")

    contas = {i.id: i.categoria_id for i in s.scalars(select(Insumo).where(
        Insumo.id.in_({x for x in s.scalars(
            select(SuprimentoItem.insumo_id)
            .join(PedidoItem, PedidoItem.suprimento_item_id == SuprimentoItem.id)
            .where(PedidoItem.pedido_id == pedido.id)).all()} or {0}))).all()}
    insumo_de = dict(s.execute(
        select(PedidoItem.id, SuprimentoItem.insumo_id)
        .join(SuprimentoItem, SuprimentoItem.id == PedidoItem.suprimento_item_id)
        .where(PedidoItem.pedido_id == pedido.id)).all())

    # O que chegou, por (obra, conta do plano): é o rateio do título.
    pesos: dict[tuple[int, Optional[int]], Decimal] = defaultdict(lambda: _ZERO)
    for i in itens:
        if i["valor_recebido"] > 0:
            conta = contas.get(insumo_de.get(i["pedido_item_id"]))
            pesos[(i["obra_id"], conta)] += i["valor_recebido"]
    recebido = sum(pesos.values(), _ZERO)

    if entregue:
        valor = total - fin["em_titulo"]
    else:
        valor = recebido - fin["em_titulo"]
    valor = max(valor, _ZERO).quantize(_CENT)

    avisos = []
    if recebido <= 0:
        avisos.append("Ainda não há recebimento registrado para este pedido. "
                      "Registre o que chegou antes de lançar a nota.")
    elif valor <= 0:
        avisos.append("Tudo o que chegou deste pedido já tem conta a pagar lançada.")
    if not entregue and recebido > 0:
        avisos.append("O pedido ainda não chegou por inteiro: o valor sugerido é "
                      "o do que já chegou. Confira com o valor da nota.")
    sem_conta = [k for k in pesos if k[1] is None]
    if sem_conta:
        avisos.append("Há insumo deste pedido sem conta do plano financeiro no "
                      "cadastro — escolha a conta no rateio.")

    from app.apps.erp.core.suprimentos.projecao import repartir
    rateios = [{"obra_id": obra_id, "categoria_id": conta, "valor": str(parte)}
               for (obra_id, conta), parte in
               (repartir(valor, dict(pesos)).items() if valor > 0 else [])
               if parte > 0]

    # AS PARCELAS VÊM DA PREVISÃO — é a "previsão que já existe" sendo
    # confirmada. O que já venceu vai para hoje: boleto não vence no passado.
    abertas = [p for p in base["previsoes"].get(pedido.id, []) if not p.titulo_id]
    parcelas = []
    if valor > 0 and abertas:
        soma = sum((_d(p.valor) for p in abertas), _ZERO)
        partes = repartir(valor, {p.id: _d(p.valor) for p in abertas}) if soma > 0 else {}
        for p in abertas:
            if partes.get(p.id, _ZERO) > 0:
                parcelas.append({"vencimento": max(p.vencimento, hoje).isoformat(),
                                 "valor": str(partes[p.id])})
    elif valor > 0:
        parcelas.append({"vencimento": hoje.isoformat(), "valor": str(valor)})

    nomes = []
    for i in itens:
        if i["valor_recebido"] > 0 and i["insumo"] not in nomes:
            nomes.append(i["insumo"])
    descricao = f"Pedido {pedido.numero} — " + ", ".join(nomes[:3]) + (
        f" e mais {len(nomes) - 3}" if len(nomes) > 3 else "")

    contas_validas = {r["categoria_id"] for r in rateios if r["categoria_id"]}
    return {
        "pedido_id": pedido.id, "numero": pedido.numero,
        "recebimento_id": recebimento.id if recebimento else None,
        "fornecedor_id": pedido.fornecedor_id,
        "fornecedor": base["fornecedores"].get(pedido.fornecedor_id, ""),
        "descricao": descricao[:200],
        "valor": str(valor),
        "documento_numero": (recebimento.nota_numero if recebimento else None)
                            or next((r.nota_numero for r in reversed(
                                base["chegadas"].get(pedido.id, []))
                                if r.nota_numero), None),
        # Uma conta só no pedido inteiro vira a conta do título; mais de uma
        # fica só no rateio, e a pessoa escolhe a principal.
        "categoria_id": next(iter(contas_validas)) if len(contas_validas) == 1 else None,
        "rateios": rateios, "parcelas": parcelas,
        "total_do_pedido": str(total), "ja_lancado": str(fin["em_titulo"]),
        "entregue": entregue, "avisos": avisos,
        # A nota e o boleto anexados na hora do recebimento: a tela de Lançar
        # os traz prontos para ler e anexar ao título.
        "documentos": _documentos(s, pedido.id, recebimento),
    }


def _documentos(s: Session, pedido_id: int, recebimento) -> list[dict[str, Any]]:
    from app.apps.erp.core.suprimentos.recebimento import documentos_do_pedido
    if recebimento is not None:
        return documentos_do_pedido(s, pedido_id, recebimento_id=recebimento.id)
    # Sem recebimento indicado: os dos recebimentos que ainda não viraram conta.
    lancados = {v.recebimento_id for v in s.scalars(select(PedidoTitulo).where(
        PedidoTitulo.pedido_id == pedido_id)).all() if v.recebimento_id}
    return [d for d in documentos_do_pedido(s, pedido_id)
            if d["recebimento_id"] not in lancados]


def pedidos_do_fornecedor_a_lancar(s: Session, usuario: Usuario,
                                   fornecedor_id: int) -> list[dict[str, Any]]:
    """Pedidos de compra deste fornecedor com material recebido e ainda sem
    conta lançada — os candidatos quando alguém lança uma nota dele.

    É a metade "identificar" do pedido do dono (21/11/2026): "quando fosse
    anexar uma nota e identificar que não identificou um pedido…". A tela de
    Lançar pergunta "é deste pedido?" antes de deixar a nota entrar solta.
    """
    pedidos = [p for p in acomp.pedidos_no_escopo(
        s, usuario, (StatusPedidoCompra.AUTORIZADO,))
        if p.fornecedor_id == int(fornecedor_id)]
    base = acomp.carregar(s, pedidos)
    saida = []
    for pedido in pedidos:
        itens = base["itens"].get(pedido.id, [])
        fin = acomp.financeiro_do_pedido(pedido, base)
        total = acomp.total_do_pedido(pedido, itens)
        falta = total - fin["em_titulo"]
        if falta <= acomp.tolerancia(total):
            continue
        recebido = sum((i["valor_recebido"] for i in itens), _ZERO)
        obras = []
        for i in itens:
            if i["obra"] not in obras:
                obras.append(i["obra"])
        saida.append({"pedido_id": pedido.id, "numero": pedido.numero,
                      "total": str(total), "a_lancar": str(falta.quantize(_CENT)),
                      "recebido": str(recebido.quantize(_CENT)), "obras": obras,
                      "chegou": recebido > 0})
    saida.sort(key=lambda p: (not p["chegou"], p["numero"]))
    return saida


def vincular(s: Session, titulo, pedido_id: int, usuario: Usuario, *,
             recebimento_id: Optional[int] = None) -> PedidoTitulo:
    """Grava que o título recém-criado é deste pedido e baixa a previsão."""
    pedido = _pedido_autorizado(s, usuario, int(pedido_id))
    if titulo.fornecedor_id != pedido.fornecedor_id:
        raise ErroValidacao(
            f"O credor do lançamento não é o fornecedor do pedido {pedido.numero}. "
            "Confira o credor ou tire o vínculo com o pedido.")
    if recebimento_id:
        rec = s.get(Recebimento, int(recebimento_id))
        if rec is None or rec.pedido_id != pedido.id:
            recebimento_id = None

    vinculo = PedidoTitulo(titulo_id=titulo.id, pedido_id=pedido.id,
                           recebimento_id=int(recebimento_id) if recebimento_id else None,
                           criado_por=usuario.id)
    s.add(vinculo)

    # Baixa as previsões mais antigas até cobrir o valor lançado.
    falta = _d(titulo.valor_liquido)
    baixadas = []
    for prev in s.scalars(select(PrevisaoPagamento).where(
            PrevisaoPagamento.pedido_id == pedido.id,
            PrevisaoPagamento.titulo_id.is_(None))
            .order_by(PrevisaoPagamento.vencimento, PrevisaoPagamento.numero)).all():
        if falta <= acomp.tolerancia(_d(prev.valor)) and falta < _d(prev.valor) / 2:
            break
        prev.titulo_id = titulo.id
        baixadas.append(prev.numero)
        falta -= _d(prev.valor)
    s.flush()
    registrar_evento(s, "pedido_compra", pedido.id, "TITULO_LANCADO",
                     {"titulo_id": titulo.id, "numero_sp": titulo.numero_sp,
                      "valor": str(titulo.valor_liquido),
                      "recebimento_id": vinculo.recebimento_id,
                      "previsoes_baixadas": baixadas}, usuario.id)
    return vinculo


def itens_do_recebimento(s: Session, recebimento_id: int) -> list[dict[str, Any]]:
    """O que veio numa remessa — para a tela mostrar ao lado do lançamento."""
    return [{"pedido_item_id": r.pedido_item_id, "quantidade": str(r.quantidade)}
            for r in s.scalars(select(RecebimentoItem).where(
                RecebimentoItem.recebimento_id == recebimento_id)).all()]


# ---------------------------------------------------------------------------
# RECEBER E LANÇAR NUM PROCEDIMENTO SÓ (21/11/2026)
#
# O dono: "fazer isso tudo no mesmo procedimento, receber e lançar notas e
# boletos (…) inclusive eu travo isso atualmente, a pessoa recebe e lança ao
# mesmo tempo, porque senão as pessoas podem ignorar, podem fazer só o
# lançamento financeiro, esquecer de fazer o recebimento e vice-versa."
#
# E sobre o preenchimento: "esse lançamento tem que preencher corretamente o
# título financeiro. Se a IA detectar, preenche tudo. Se não detectar, a pessoa
# tem que concluir o básico (…) número de nota, fornecedor, CNPJ, vencimento,
# código de barras, PIX."
#
# UMA TRANSAÇÃO: o recebimento, o título e o vínculo nascem juntos ou não
# nasce nenhum. O título continua saindo de `criar_titulo`, com a crítica de
# duplicidade na frente — a mesma ordem da tela de Lançar. O que vem do PEDIDO
# (credor, rateio por obra, conta do plano) não é digitado; o que vem da NOTA
# (número, valor, vencimento, linha digitável) é lido pela IA ou digitado.
#
# A SAÍDA "a nota não veio": existe, exige o motivo por escrito e fica à vista
# do coordenador e do financeiro. Sem ela, no dia em que o caminhão chegasse
# sem o boleto, a obra não registraria nem o recebimento — e aí o sistema
# perderia as duas informações em vez de uma.
# ---------------------------------------------------------------------------
FORMAS_DO_PORTAL = (("BOLETO", "Boleto"), ("PIX", "Pix"), ("TED", "Transferência (TED)"))


def dados_para_receber(s: Session, pedido: PedidoCompra, base: dict[str, Any],
                       obras: Optional[set[int]]) -> dict[str, Any]:
    """O que a tela de recebimento precisa para montar o lançamento sozinha:
    o preço de cada item, as parcelas previstas e as contas do fornecedor."""
    from app.apps.erp.db.models.cadastros import FornecedorConta
    itens = [i for i in base["itens"].get(pedido.id, [])
             if obras is None or i["obra_id"] in obras]
    todos = base["itens"].get(pedido.id, [])
    fin = acomp.financeiro_do_pedido(pedido, base)
    abertas = [p for p in base["previsoes"].get(pedido.id, []) if not p.titulo_id]
    soma = sum((_d(p.valor) for p in abertas), _ZERO)
    contas = [{"id": c.id, "forma": c.forma.value,
               "rotulo": (f"Pix {c.pix_chave}" if c.pix_chave else
                          f"Banco {c.banco_codigo or '?'} ag. {c.agencia or '?'} "
                          f"conta {c.conta or '?'}"),
               "homologada": getattr(c.status, "value", "") == "HOMOLOGADA"}
              for c in s.scalars(select(FornecedorConta).where(
                  FornecedorConta.fornecedor_id == pedido.fornecedor_id)).all()]
    return {
        "precos": {str(i["pedido_item_id"]): str(i["preco_unitario"]) for i in itens},
        # Frete e desconto entram no valor quando ESTE recebimento fecha o pedido.
        "frete": str(_d(pedido.frete)), "desconto": str(_d(pedido.desconto)),
        "outros_itens_em_aberto": any(i["saldo"] > 0 for i in todos if i not in itens),
        "ja_lancado": str(fin["em_titulo"]),
        "recebido_sem_conta": str(max(
            sum((i["valor_recebido"] for i in todos), _ZERO) - fin["em_titulo"], _ZERO)),
        "parcelas_previstas": [
            {"vencimento": p.vencimento.isoformat(),
             "parte": float(_d(p.valor) / soma) if soma > 0 else 0} for p in abertas],
        "contas": contas,
        "formas": [{"valor": v, "rotulo": r} for v, r in FORMAS_DO_PORTAL],
        "antecipado": bool(pedido.antecipado),
    }


def receber_e_lancar(s: Session, usuario: Usuario, pedido_id: int,
                     dados: dict[str, Any]) -> dict[str, Any]:
    """Registra o que chegou E lança a conta a pagar — juntos, ou nenhum."""
    from app.apps.erp.core.suprimentos import recebimento as svc_rec
    from app.apps.erp.core.suprimentos.projecao import repartir
    from app.apps.erp.core.titulos import service as svc_titulos
    from app.apps.erp.core.titulos.derivacao import derivar_por_contexto
    from app.apps.erp.core.titulos.duplicidade import checar
    from app.apps.erp.db.models.cadastros import Categoria

    pedido = _pedido_autorizado(s, usuario, int(pedido_id))
    fin = dados.get("financeiro") or {}
    rec = svc_rec.registrar(s, pedido.id, dados, usuario)
    s.flush()

    if fin.get("sem_lancamento"):
        motivo = " ".join(str(fin.get("motivo") or "").split())
        if len(motivo) < 10:
            raise ErroValidacao(
                "Explique por que a conta não está sendo lançada junto com o "
                "recebimento — o financeiro e o suprimento vão ler isto.")
        rec.observacoes = ((rec.observacoes + " · ") if rec.observacoes else "") + (
            f"SEM LANÇAMENTO NO RECEBIMENTO: {motivo}")
        registrar_evento(s, "pedido_compra", pedido.id, "RECEBIDO_SEM_LANCAMENTO",
                         {"recebimento_id": rec.id, "motivo": motivo}, usuario.id)
        return {"recebimento_id": rec.id, "titulo": None}

    sugerido = preparar(s, usuario, pedido.id, recebimento_id=rec.id)
    faltando = []
    numero_nota = " ".join(str(fin.get("documento_numero") or "").split())
    if not numero_nota:
        faltando.append("o número da nota fiscal")
    try:
        valor = _d(str(fin.get("valor") or "").replace(".", "").replace(",", ".")
                   if "," in str(fin.get("valor") or "") else fin.get("valor"))
    except Exception:
        valor = _ZERO
    if valor <= 0:
        faltando.append("o valor da nota")
    forma = str(fin.get("forma_pagamento") or "").strip().upper()
    if forma not in {v for v, _ in FORMAS_DO_PORTAL}:
        faltando.append("a forma de pagamento")
    parcelas = [p for p in (fin.get("parcelas") or []) if p.get("vencimento")]
    if not parcelas:
        faltando.append("o vencimento")
    if forma == "BOLETO" and any(not str(p.get("linha_digitavel") or "").strip()
                                 for p in parcelas):
        faltando.append("a linha digitável (código de barras) de cada boleto")
    if faltando:
        raise ErroValidacao("Para lançar a conta falta " + ", ".join(faltando) + ".")

    pesos = {(r["obra_id"], r["categoria_id"]): _d(r["valor"])
             for r in sugerido["rateios"]}
    if not pesos:
        raise ErroValidacao("Não há material recebido para ratear a conta.")
    conta_do_plano = (fin.get("categoria_id") or sugerido["categoria_id"]
                      or next((c for (_o, c) in pesos if c), None))
    cat = s.get(Categoria, int(conta_do_plano)) if conta_do_plano else None
    if cat is None:
        raise ErroValidacao(
            "O material deste pedido está sem conta do plano financeiro no "
            "cadastro de insumos. Peça ao suprimento para acertar o cadastro — "
            "sem a conta, o lançamento não tem onde cair.")

    d = {
        "fornecedor_id": pedido.fornecedor_id, "categoria_id": cat.id,
        "descricao": (str(fin.get("descricao") or "").strip() or sugerido["descricao"]),
        "valor_bruto": str(valor), "documento_numero": numero_nota,
        "data_emissao_doc": fin.get("data_emissao") or None,
        "forma_pagamento": forma,
        "fornecedor_conta_id": fin.get("fornecedor_conta_id") or None,
        "parcelas": [{"vencimento": p["vencimento"], "valor": str(p.get("valor") or valor),
                      "linha_digitavel": str(p.get("linha_digitavel") or "").strip()}
                     for p in parcelas],
        "rateios": [{"obra_id": obra_id, "valor": str(parte),
                     "categoria_id": conta or cat.id}
                    for (obra_id, conta), parte in repartir(valor, pesos).items()
                    if parte > 0],
        "retencoes": [], "interessados": [],
        "pedido_compra_id": pedido.id, "recebimento_id": rec.id,
    }
    nota = gravar_a_nota(s, pedido, numero=numero_nota, valor=valor,
                         emissao=fin.get("data_emissao"), chave=fin.get("chave_acesso"))
    d["documento_fiscal_id"] = nota.id
    critica = checar(s, {"fornecedor_id": d["fornecedor_id"], "valor": d["valor_bruto"],
                         "parcelas": d["parcelas"], "descricao": d["descricao"],
                         "categoria_id": d["categoria_id"],
                         "documento_numero": d["documento_numero"]})
    if critica["bloqueios"] and not fin.get("forcar"):
        raise ErroValidacao("Esta nota parece já ter sido lançada: " + " ".join(
            str(b.get("msg") or "") for b in critica["bloqueios"]))
    d["tipo"] = derivar_por_contexto(cat, d).value
    if d["tipo"] == "T14_EXCECAO_SEM_NOTA":
        d["justificativa_excecao"] = (f"Lançado no recebimento do pedido {pedido.numero} "
                                      f"por {usuario.email}.")
    titulo = svc_titulos.criar_titulo(s, d, usuario)
    if fin.get("chave_acesso"):
        titulo.chave_acesso_nfe = str(fin["chave_acesso"])[:44] or None
    vincular(s, titulo, pedido.id, usuario, recebimento_id=rec.id)
    if critica["alertas"]:
        registrar_evento(s, "titulo", titulo.id, "ALERTAS_DUPLICIDADE_ACEITOS",
                         {"alertas": critica["alertas"],
                          "confirmado_por": usuario.email}, usuario.id)
    return {"recebimento_id": rec.id,
            "titulo": {"id": titulo.id, "numero_sp": titulo.numero_sp,
                       "status": titulo.status.value,
                       "alertas": [a.get("msg") for a in critica["alertas"]]}}


# ---------------------------------------------------------------------------
# O RECEBIMENTO GRAVA A NOTA (24/11/2026)
#
# O dono, ao saber que a conta nascida do recebimento chegava ao financeiro com
# o aviso "exige documento fiscal vinculado", mesmo com o número da nota e o
# arquivo anexados: "o recebimento precisa anexar a nota e o sistema gravar a
# nota. Não tem sentido eu fazer isso novamente no financeiro se já poderia
# fazer no recebimento."
#
# Então a nota passa a existir como NOTA (em Financeiro › Notas), ligada ao
# pedido de compra e ao título, no mesmo ato do recebimento. Ninguém cadastra
# duas vezes, e a crítica A10 deixa de aparecer porque deixou de ser verdade.
#
# Nota que JÁ EXISTE (veio por XML, ou vai vir pelo monitoramento) é
# reaproveitada — pela chave de acesso, ou por emitente + número. E se ela já
# pertence a outro título, o lançamento é recusado: é a mesma nota duas vezes.
# ---------------------------------------------------------------------------
def gravar_a_nota(s: Session, pedido: PedidoCompra, *, numero: str, valor: Any,
                  emissao: Any = None, chave: Any = None):
    import re
    from app.apps.erp.db.models.cadastros import Fornecedor
    from app.apps.erp.db.models.financeiro import (
        DocumentoFiscal, SituacaoNota, TipoDocFiscal, Titulo)

    forn = s.get(Fornecedor, pedido.fornecedor_id)
    emitente = re.sub(r"\D", "", getattr(forn, "cnpj_cpf", "") or "")
    numero = " ".join(str(numero or "").split())
    chave = re.sub(r"\D", "", str(chave or ""))[:44] or None
    nota = None
    if chave:
        nota = s.scalars(select(DocumentoFiscal).where(
            DocumentoFiscal.chave_acesso == chave)).first()
    if nota is None and emitente and numero:
        nota = next((n for n in s.scalars(select(DocumentoFiscal).where(
            DocumentoFiscal.emitente_doc == emitente,
            DocumentoFiscal.numero == numero)).all()
            if n.emitente_doc == emitente and n.numero == numero), None)
    if nota is not None:
        dono = s.scalars(select(Titulo).where(
            Titulo.documento_fiscal_id == nota.id)).first()
        if dono is not None and getattr(dono.status, "value", "") not in ("CANCELADO", "ESTORNADO"):
            raise ErroValidacao(
                f"A nota {numero} deste fornecedor já está lançada no título "
                f"{dono.numero_sp}. Lançar de novo seria pagar duas vezes.")
        if nota.pedido_compra_id is None:
            nota.pedido_compra_id = pedido.id
        return nota

    data_emissao = None
    if emissao:
        try:
            data_emissao = date.fromisoformat(str(emissao)[:10])
        except ValueError:
            data_emissao = None
    empresa_id = None
    try:
        from app.apps.erp.core.suprimentos.envio import empresa_do_pedido
        empresa_id = empresa_do_pedido(s, pedido.id).get("sugerida")
    except Exception:
        empresa_id = None
    nota = DocumentoFiscal(
        tipo=TipoDocFiscal.NFE, chave_acesso=chave, numero=numero,
        emitente_doc=emitente or "0", emitente_nome=getattr(forn, "razao_social", None),
        valor_total=_d(valor), data_emissao=data_emissao,
        situacao=SituacaoNota.DESCONHECIDA, origem="RECEBIMENTO",
        empresa_id=int(empresa_id) if empresa_id else None, pedido_compra_id=pedido.id)
    s.add(nota)
    s.flush()
    return nota
