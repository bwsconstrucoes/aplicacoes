# ============================================================================
# ERP — core/locacoes.py
# Gestão de locação de equipamentos.
#
# O problema que ela resolve: hoje alguém loca, o equipamento entra na obra e
# ninguém acompanha. A cobrança chega por e-mail para compras ou para o
# financeiro, que não sabem do que se trata. O equipamento migra de obra e se
# perde. Ninguém percebe que já se pagou em aluguel o preço do equipamento.
#
# Aqui o contrato tem itens, cada um ligado a um insumo e a uma obra, e gera a
# PREVISÃO das parcelas conforme a periodicidade. Quando o boleto chega, o
# financeiro encontra a previsão e sabe exatamente o que é. Devolução parcial
# reduz as próximas parcelas; remanejo troca a obra sem perder o histórico.
#
# O alerta que muda decisão: locação acumulada que já passou (ou está perto
# de) o preço de compra do equipamento. Dez a doze meses de aluguel pagam a
# compra — passar disso é dinheiro jogado fora.
# ============================================================================
from __future__ import annotations

import calendar
import logging
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
import re

from app.apps.erp.db.models.cadastros import Categoria, Fornecedor, Insumo, Obra, Usuario
from app.apps.erp.db.models.financeiro import (
    ContratoLocacao, LocacaoItem, LocacaoMovimento, LocacaoParcela,
)

logger = logging.getLogger(__name__)
_CENT = Decimal("0.01")

PERIODICIDADES = {
    "DIARIA": ("Diária", 1), "SEMANAL": ("Semanal", 7),
    "QUINZENAL": ("Quinzenal", 15), "MENSAL": ("Mensal", 30),
}
MESES_ALERTA_COMPRA = 10       # a partir daqui, comprar costuma valer mais


def _dec(v: Any, campo: str = "valor") -> Decimal:
    try:
        s = str(v or "0").strip().replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return Decimal(s)
    except (InvalidOperation, TypeError):
        raise ErroValidacao(f"Valor inválido em {campo}: {v!r}")


def _data(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10]) if v else None
    except ValueError:
        return None


def _somar_mes(d: date, meses: int) -> date:
    ano, mes = divmod(d.month - 1 + meses, 12)
    ano, mes = d.year + ano, mes + 1
    return d.replace(year=ano, month=mes, day=min(d.day, calendar.monthrange(ano, mes)[1]))


# ---------------------------------------------------------------------------
# Contrato
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> ContratoLocacao:
    forn = s.get(Fornecedor, int(dados.get("fornecedor_id") or 0))
    obra = s.get(Obra, int(dados.get("obra_id") or 0))
    if forn is None or obra is None:
        raise ErroValidacao("Informe a locadora e a obra.")
    itens = dados.get("itens") or []
    if not itens:
        raise ErroValidacao("Adicione ao menos um equipamento.")
    period = (dados.get("periodicidade") or "MENSAL").upper()
    if period not in PERIODICIDADES:
        raise ErroValidacao(f"Periodicidade inválida: {period}")
    inicio = _data(dados.get("data_inicio")) or date.today()

    n = s.scalar(select(func.count()).select_from(ContratoLocacao)) or 0
    categoria_id = dados.get("categoria_id")
    if not categoria_id:
        cat = s.scalars(select(Categoria).where(Categoria.codigo == "3.3.01")).first()
        categoria_id = cat.id if cat else None

    c = ContratoLocacao(
        numero=f"LOC{n + 1:05d}", fornecedor_id=forn.id, obra_id=obra.id,
        categoria_id=categoria_id,
        numero_externo=(dados.get("numero_externo") or "").strip() or None,
        periodicidade=period,
        dia_vencimento=int(dados["dia_vencimento"]) if dados.get("dia_vencimento") else None,
        data_inicio=inicio, data_fim_prevista=_data(dados.get("data_fim_prevista")),
        responsavel_id=dados.get("responsavel_id") or usuario.id,
        observacoes=(dados.get("observacoes") or "").strip() or None,
        criado_por=usuario.id)
    s.add(c)
    s.flush()

    for i in itens:
        qtd = _dec(i.get("quantidade"), "quantidade")
        if qtd <= 0:
            raise ErroValidacao("Quantidade deve ser maior que zero.")
        s.add(LocacaoItem(
            contrato_id=c.id, insumo_id=i.get("insumo_id") or None,
            descricao=(i.get("descricao") or "").strip()[:200] or "(equipamento)",
            quantidade=qtd, valor_unitario=_dec(i.get("valor_unitario"), "valor unitário"),
            obra_id=obra.id))
    s.flush()
    registrar_evento(s, "contrato_locacao", c.id, "CRIADO", {
        "numero": c.numero, "locadora": forn.razao_social, "obra": obra.codigo,
        "periodicidade": period, "itens": len(itens),
        "valor_periodo": str(valor_periodo(s, c.id))}, usuario.id)
    gerar_previsao(s, c.id, meses=int(dados.get("meses_previsao") or 6), usuario=usuario)
    return c


def valor_periodo(s: Session, contrato_id: int) -> Decimal:
    """Quanto o contrato custa por período, já descontado o que foi devolvido."""
    itens = s.scalars(select(LocacaoItem).where(LocacaoItem.contrato_id == contrato_id)).all()
    total = sum(((Decimal(i.quantidade) - Decimal(i.quantidade_devolvida))
                 * Decimal(i.valor_unitario) for i in itens), Decimal("0"))
    return total.quantize(_CENT)


def gerar_previsao(s: Session, contrato_id: int, meses: int = 6,
                   usuario: Optional[Usuario] = None) -> int:
    """Cria as parcelas previstas — é o que faz o boleto ser reconhecido."""
    c = s.get(ContratoLocacao, contrato_id)
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    valor = valor_periodo(s, contrato_id)
    if valor <= 0:
        return 0
    passo = PERIODICIDADES[c.periodicidade][1]
    existentes = {p.competencia for p in s.scalars(select(LocacaoParcela).where(
        LocacaoParcela.contrato_id == contrato_id)).all()}
    criadas = 0
    referencia = max(c.data_inicio, date.today().replace(day=1))
    for i in range(meses):
        if c.periodicidade == "MENSAL":
            competencia = _somar_mes(referencia.replace(day=1), i)
            dia = c.dia_vencimento or 10
            venc = competencia.replace(
                day=min(dia, calendar.monthrange(competencia.year, competencia.month)[1]))
        else:
            competencia = referencia + timedelta(days=passo * i)
            venc = competencia + timedelta(days=passo)
        if c.data_fim_prevista and competencia > c.data_fim_prevista:
            break
        if competencia in existentes:
            continue
        s.add(LocacaoParcela(contrato_id=c.id, competencia=competencia,
                             vencimento=venc, valor_previsto=valor))
        criadas += 1
    s.flush()
    if criadas and usuario:
        registrar_evento(s, "contrato_locacao", c.id, "PREVISAO_GERADA",
                         {"parcelas": criadas, "valor": str(valor)}, usuario.id)
    return criadas


def devolver(s: Session, contrato_id: int, dados: dict[str, Any],
             usuario: Usuario) -> dict[str, Any]:
    """Devolução total ou parcial: reduz as próximas parcelas."""
    item = s.get(LocacaoItem, int(dados.get("item_id") or 0))
    if item is None or item.contrato_id != contrato_id:
        raise ErroValidacao("Equipamento não encontrado neste contrato.")
    qtd = _dec(dados.get("quantidade"), "quantidade devolvida")
    disponivel = Decimal(item.quantidade) - Decimal(item.quantidade_devolvida)
    if qtd <= 0 or qtd > disponivel:
        raise ErroValidacao(f"Devolução inválida: há {disponivel} em obra.")
    item.quantidade_devolvida = Decimal(item.quantidade_devolvida) + qtd
    data_mov = _data(dados.get("data_movimento")) or date.today()
    s.add(LocacaoMovimento(
        contrato_id=contrato_id, item_id=item.id, tipo="DEVOLUCAO", quantidade=qtd,
        obra_origem_id=item.obra_id, data_movimento=data_mov,
        documento=(dados.get("documento") or "").strip() or None,
        observacao=(dados.get("observacao") or "").strip() or None, usuario_id=usuario.id))
    s.flush()

    novo_valor = valor_periodo(s, contrato_id)
    futuras = s.scalars(select(LocacaoParcela).where(
        LocacaoParcela.contrato_id == contrato_id,
        LocacaoParcela.status == "PREVISTA",
        LocacaoParcela.vencimento >= data_mov)).all()
    for p in futuras:
        p.valor_previsto = novo_valor

    c = s.get(ContratoLocacao, contrato_id)
    encerrado = False
    if novo_valor <= 0:
        c.status = "ENCERRADO"
        c.data_encerramento = data_mov
        for p in futuras:
            p.status = "CANCELADA"
        encerrado = True
    registrar_evento(s, "contrato_locacao", contrato_id, "DEVOLUCAO", {
        "item": item.descricao, "quantidade": str(qtd), "data": data_mov.isoformat(),
        "novo_valor_periodo": str(novo_valor), "encerrado": encerrado}, usuario.id)
    return {"devolvido": float(qtd), "restante_em_obra": float(disponivel - qtd),
            "novo_valor_periodo": float(novo_valor),
            "parcelas_ajustadas": len(futuras), "contrato_encerrado": encerrado}


def remanejar(s: Session, contrato_id: int, dados: dict[str, Any],
              usuario: Usuario) -> dict[str, Any]:
    """Equipamento que muda de obra — sem isso ele se perde entre canteiros."""
    item = s.get(LocacaoItem, int(dados.get("item_id") or 0))
    if item is None or item.contrato_id != contrato_id:
        raise ErroValidacao("Equipamento não encontrado neste contrato.")
    destino = s.get(Obra, int(dados.get("obra_destino_id") or 0))
    if destino is None:
        raise ErroValidacao("Informe a obra de destino.")
    if destino.id == item.obra_id:
        raise ErroValidacao("O equipamento já está nesta obra.")
    origem_id = item.obra_id
    item.obra_id = destino.id
    data_mov = _data(dados.get("data_movimento")) or date.today()
    s.add(LocacaoMovimento(
        contrato_id=contrato_id, item_id=item.id, tipo="REMANEJO",
        quantidade=Decimal(item.quantidade) - Decimal(item.quantidade_devolvida),
        obra_origem_id=origem_id, obra_destino_id=destino.id, data_movimento=data_mov,
        observacao=(dados.get("observacao") or "").strip() or None, usuario_id=usuario.id))
    origem = s.get(Obra, origem_id)
    registrar_evento(s, "contrato_locacao", contrato_id, "REMANEJO", {
        "item": item.descricao, "de": origem.codigo if origem else "?",
        "para": destino.codigo, "data": data_mov.isoformat()}, usuario.id)
    return {"item": item.descricao, "de": origem.codigo if origem else None,
            "para": destino.codigo,
            "aviso": "O custo das próximas parcelas passa a ser apropriado na obra de destino."}


def lancar_parcela(s: Session, parcela_id: int, dados: dict[str, Any],
                   usuario: Usuario) -> dict[str, Any]:
    """Transforma a previsão no título a pagar — a cobrança deixa de ser órfã."""
    from app.apps.erp.core.titulos.service import criar_titulo

    p = s.get(LocacaoParcela, parcela_id)
    if p is None:
        raise ErroValidacao("Parcela não encontrada.")
    if p.status == "LANCADA":
        raise ErroValidacao(f"Esta competência já foi lançada no título {p.titulo_id}.")
    c = s.get(ContratoLocacao, p.contrato_id, options=[
        selectinload(ContratoLocacao.itens), selectinload(ContratoLocacao.fornecedor)])

    valor = _dec(dados.get("valor"), "valor") if dados.get("valor") else Decimal(p.valor_previsto)
    diferenca = (valor - Decimal(p.valor_previsto)).quantize(_CENT)

    # rateio segue a obra de cada equipamento (remanejo apropria certo)
    por_obra: dict[int, Decimal] = {}
    base = valor_periodo(s, c.id)
    for i in c.itens:
        ativo = (Decimal(i.quantidade) - Decimal(i.quantidade_devolvida)) * Decimal(i.valor_unitario)
        if ativo > 0:
            oid = i.obra_id or c.obra_id
            por_obra[oid] = por_obra.get(oid, Decimal("0")) + ativo
    if base > 0 and por_obra:
        rateios = [{"obra_id": oid, "valor": str((v / base * valor).quantize(_CENT))}
                   for oid, v in por_obra.items()]
        soma = sum(Decimal(r["valor"]) for r in rateios)
        if soma != valor:      # ajusta centavo na maior linha
            maior = max(rateios, key=lambda r: Decimal(r["valor"]))
            maior["valor"] = str(Decimal(maior["valor"]) + (valor - soma))
    else:
        rateios = [{"obra_id": c.obra_id, "valor": str(valor)}]

    titulo = criar_titulo(s, {
        "tipo": "T4_LOCACAO", "fornecedor_id": c.fornecedor_id,
        "categoria_id": c.categoria_id,
        "descricao": f"{c.numero} locação {p.competencia:%m/%Y} — "
                     f"{', '.join(i.descricao for i in c.itens[:3])[:120]}",
        "valor_bruto": str(valor), "competencia": p.competencia.strftime("%Y-%m"),
        # a cobrança da locadora quase sempre vem em boleto; sem a linha
        # digitável informada, o título nasce como Pix para não travar
        "forma_pagamento": (dados.get("forma_pagamento")
                            or ("BOLETO" if dados.get("linha_digitavel") else "PIX")),
        "fornecedor_conta_id": dados.get("fornecedor_conta_id"),
        "contrato_id": dados.get("contrato_id"),
        "locacao_contrato_id": c.id,
        "parcelas": [{"vencimento": (dados.get("vencimento") or p.vencimento.isoformat()),
                      "valor": str(valor),
                      "linha_digitavel": dados.get("linha_digitavel") or ""}],
        "rateios": rateios,
        "justificativa_excecao": f"Locação {c.numero}, competência {p.competencia:%m/%Y}.",
    }, usuario)
    titulo.locacao_parcela_id = p.id
    p.titulo_id = titulo.id
    p.status = "LANCADA"
    s.flush()
    registrar_evento(s, "contrato_locacao", c.id, "PARCELA_LANCADA", {
        "competencia": p.competencia.isoformat(), "titulo": titulo.numero_sp,
        "valor": str(valor), "diferenca_da_previsao": str(diferenca)}, usuario.id)
    return {"titulo": titulo.numero_sp, "valor": float(valor),
            "diferenca": float(diferenca),
            "aviso": (f"Cobrado R$ {valor} contra previsão de R$ {p.valor_previsto} "
                      f"— confira se houve devolução não registrada."
                      if abs(diferenca) > Decimal("0.01") else None)}


# ---------------------------------------------------------------------------
# Leitura e alertas
# ---------------------------------------------------------------------------
def _alertas(s: Session, c: ContratoLocacao, pago: Decimal,
             itens: list[LocacaoItem]) -> list[dict[str, str]]:
    alertas = []
    meses = max(1, ((date.today() - c.data_inicio).days) // 30)
    if c.status == "ATIVO" and meses >= MESES_ALERTA_COMPRA:
        alertas.append({"gravidade": "CRITICA",
                        "msg": f"{meses} meses locado. Nessa altura o aluguel já paga a "
                               f"compra do equipamento — avalie adquirir ou devolver."})
    elif c.status == "ATIVO" and meses >= 5:
        alertas.append({"gravidade": "ALERTA",
                        "msg": f"{meses} meses locado. A partir de ~10 meses comprar "
                               f"costuma sair mais barato."})
    for i in itens:
        insumo = s.get(Insumo, i.insumo_id) if i.insumo_id else None
        if insumo and insumo.valor_referencia_compra:
            gasto_item = (Decimal(i.quantidade) * Decimal(i.valor_unitario) * meses)
            preco = Decimal(insumo.valor_referencia_compra) * Decimal(i.quantidade)
            if gasto_item >= preco:
                alertas.append({
                    "gravidade": "CRITICA",
                    "msg": f"{i.descricao}: já se pagou R$ {gasto_item:.2f} de aluguel, "
                           f"acima do preço de compra (R$ {preco:.2f})."})
    if c.status == "ATIVO" and c.data_fim_prevista and c.data_fim_prevista < date.today():
        alertas.append({"gravidade": "CRITICA",
                        "msg": f"Prazo previsto venceu em {c.data_fim_prevista:%d/%m/%Y} "
                               f"e o contrato segue ativo."})
    obras = {i.obra_id for i in itens if i.obra_id}
    if len(obras) > 1:
        alertas.append({"gravidade": "ALERTA",
                        "msg": "Equipamentos deste contrato estão em obras diferentes."})
    return alertas


def listar(s: Session, usuario: Optional[Usuario] = None,
           apenas_ativos: bool = False) -> list[dict[str, Any]]:
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    stmt = (select(ContratoLocacao)
            .options(selectinload(ContratoLocacao.fornecedor),
                     selectinload(ContratoLocacao.obra),
                     selectinload(ContratoLocacao.itens))
            .order_by(ContratoLocacao.id.desc()).limit(300))
    if apenas_ativos:
        stmt = stmt.where(ContratoLocacao.status == "ATIVO")
    if usuario is not None:
        permitidas = obras_do_usuario(s, usuario)
        if permitidas is not None:
            stmt = stmt.where(ContratoLocacao.obra_id.in_(permitidas or [0]))
    saida = []
    for c in s.scalars(stmt).all():
        pago = Decimal(s.scalar(select(func.coalesce(func.sum(LocacaoParcela.valor_previsto), 0))
                                .where(LocacaoParcela.contrato_id == c.id,
                                       LocacaoParcela.status == "LANCADA")) or 0)
        atrasadas = s.scalar(select(func.count()).select_from(LocacaoParcela).where(
            LocacaoParcela.contrato_id == c.id, LocacaoParcela.status == "PREVISTA",
            LocacaoParcela.vencimento < date.today())) or 0
        meses = max(1, (date.today() - c.data_inicio).days // 30)
        saida.append({
            "id": c.id, "numero": c.numero, "numero_externo": c.numero_externo,
            "locadora": c.fornecedor.razao_social, "obra": c.obra.codigo,
            "periodicidade": PERIODICIDADES[c.periodicidade][0],
            "status": c.status, "inicio": c.data_inicio.isoformat(),
            "meses": meses,
            "itens": len([i for i in c.itens
                          if Decimal(i.quantidade) > Decimal(i.quantidade_devolvida)]),
            "valor_periodo": float(valor_periodo(s, c.id)),
            "pago_ate_agora": float(pago),
            "parcelas_vencidas_sem_lancar": atrasadas,
            "alertas": _alertas(s, c, pago, list(c.itens)),
        })
    return saida


def detalhar(s: Session, contrato_id: int) -> dict[str, Any]:
    c = s.get(ContratoLocacao, contrato_id, options=[
        selectinload(ContratoLocacao.fornecedor), selectinload(ContratoLocacao.obra),
        selectinload(ContratoLocacao.itens)])
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}
    parcelas = s.scalars(select(LocacaoParcela).where(
        LocacaoParcela.contrato_id == contrato_id)
        .order_by(LocacaoParcela.competencia)).all()
    movimentos = s.scalars(select(LocacaoMovimento).where(
        LocacaoMovimento.contrato_id == contrato_id)
        .order_by(LocacaoMovimento.data_movimento.desc())).all()
    pago = Decimal(s.scalar(select(func.coalesce(func.sum(LocacaoParcela.valor_previsto), 0))
                            .where(LocacaoParcela.contrato_id == contrato_id,
                                   LocacaoParcela.status == "LANCADA")) or 0)
    return {
        "id": c.id, "numero": c.numero, "numero_externo": c.numero_externo,
        "locadora": c.fornecedor.razao_social, "fornecedor_id": c.fornecedor_id,
        "obra": c.obra.codigo, "obra_id": c.obra_id, "status": c.status,
        "periodicidade": c.periodicidade,
        "periodicidade_rotulo": PERIODICIDADES[c.periodicidade][0],
        "dia_vencimento": c.dia_vencimento,
        "inicio": c.data_inicio.isoformat(),
        "fim_previsto": c.data_fim_prevista.isoformat() if c.data_fim_prevista else None,
        "meses": max(1, (date.today() - c.data_inicio).days // 30),
        "valor_periodo": float(valor_periodo(s, contrato_id)),
        "pago_ate_agora": float(pago),
        "alertas": _alertas(s, c, pago, list(c.itens)),
        "itens": [{
            "id": i.id, "descricao": i.descricao,
            "quantidade": float(i.quantidade),
            "devolvida": float(i.quantidade_devolvida),
            "em_obra": float(Decimal(i.quantidade) - Decimal(i.quantidade_devolvida)),
            "valor_unitario": float(i.valor_unitario),
            "valor_periodo": float(((Decimal(i.quantidade) - Decimal(i.quantidade_devolvida))
                                    * Decimal(i.valor_unitario)).quantize(_CENT)),
            "obra": obras.get(i.obra_id, c.obra.codigo),
        } for i in c.itens],
        "parcelas": [{
            "id": p.id, "competencia": p.competencia.strftime("%m/%Y"),
            "vencimento": p.vencimento.isoformat(),
            "valor": float(p.valor_previsto), "status": p.status,
            "titulo_id": p.titulo_id,
            "atrasada": p.status == "PREVISTA" and p.vencimento < date.today(),
        } for p in parcelas],
        "movimentos": [{
            "tipo": m.tipo, "quantidade": float(m.quantidade) if m.quantidade else None,
            "de": obras.get(m.obra_origem_id), "para": obras.get(m.obra_destino_id),
            "data": m.data_movimento.isoformat(), "observacao": m.observacao,
            "por": (s.get(Usuario, m.usuario_id).nome if m.usuario_id else "—"),
        } for m in movimentos],
    }


def ler_contrato(s: Session, conteudo: bytes, nome_arquivo: str) -> dict[str, Any]:
    """Lê o contrato de locação e monta o rascunho do cadastro.

    O contrato da locadora vem em texto corrido, com a lista de equipamentos,
    quantidades e valores. A nomenclatura dela raramente bate com a nossa —
    "ANDAIME FACHADEIRO 1,50M" contra "Andaime fachadeiro 1,5m" —, então cada
    item lido é aproximado contra o cadastro de insumos locáveis, e o mesmo
    vale para a locadora contra o cadastro de fornecedores.
    """
    from difflib import SequenceMatcher

    from app.apps.erp.core.comum.ia_custo import contexto
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento

    try:
        with contexto(operacao="contrato_locacao"):
            d = ler_documento(conteudo, nome_arquivo,
                              dica_usuario="É um CONTRATO DE LOCAÇÃO DE EQUIPAMENTOS. "
                                           "Extraia em 'itens' cada equipamento locado com "
                                           "descricao, quantidade e valor (unitário do "
                                           "período). Em observacoes diga a periodicidade "
                                           "(diária, semanal, quinzenal ou mensal), o dia de "
                                           "vencimento e o número do contrato da locadora.")
    except ErroLeitura as e:
        raise ErroValidacao(f"Não consegui ler o contrato: {e}")

    texto = " ".join(str(d.get(k) or "") for k in ("observacoes", "descricao")).upper()
    periodicidade = "MENSAL"
    for chave, termos in (("DIARIA", ("DIARI", "POR DIA", "/DIA")),
                          ("SEMANAL", ("SEMANA",)),
                          ("QUINZENAL", ("QUINZEN",)),
                          ("MENSAL", ("MENSAL", "MES", "MÊS"))):
        if any(t in texto for t in termos):
            periodicidade = chave
            break
    dia = None
    achado = re.search(r"(?:VENCIMENTO|VENCE)[^0-9]{0,20}(\d{1,2})", texto)
    if achado:
        try:
            valor = int(achado.group(1))
            dia = valor if 1 <= valor <= 31 else None
        except ValueError:
            dia = None

    # locadora: casa pelo CNPJ e, na falta, por semelhança de nome
    forn_id, forn_nome = d.get("fornecedor_id"), d.get("emitente_nome")
    if not forn_id and forn_nome:
        alvo = str(forn_nome).upper()
        melhor, escore = None, 0.0
        for f in s.scalars(select(Fornecedor).where(Fornecedor.ativo.is_(True))).all():
            r = SequenceMatcher(None, alvo, (f.razao_social or "").upper()).ratio()
            if f.nome_fantasia:
                r = max(r, SequenceMatcher(None, alvo, f.nome_fantasia.upper()).ratio())
            if r > escore:
                melhor, escore = f, r
        if melhor is not None and escore >= 0.72:
            forn_id, forn_nome = melhor.id, melhor.razao_social

    # equipamentos: aproxima cada linha contra os insumos locáveis
    locaveis = s.scalars(select(Insumo).where(
        Insumo.locavel.is_(True), Insumo.ativo.is_(True))).all()
    itens = []
    for linha in (d.get("itens") or []):
        desc = (linha.get("descricao") or "").strip()
        melhor, escore = None, 0.0
        for i in locaveis:
            r = SequenceMatcher(None, desc.upper(), i.descricao.upper()).ratio()
            if r > escore:
                melhor, escore = i, r
        itens.append({
            "descricao": desc,
            "quantidade": linha.get("quantidade") or "",
            "valor_unitario": linha.get("valor") or linha.get("valor_unitario") or "",
            "insumo_id": melhor.id if (melhor and escore >= 0.6) else None,
            "insumo_sugerido": (f"{melhor.codigo} · {melhor.descricao}"
                                if melhor and escore >= 0.6 else None),
            "confianca_insumo": ("ALTA" if escore >= 0.85 else
                                 "MEDIA" if escore >= 0.6 else "BAIXA"),
        })
    return {
        "fornecedor_id": forn_id, "locadora": forn_nome,
        "numero_externo": d.get("numero_documento") or "",
        "periodicidade": periodicidade, "dia_vencimento": dia,
        "data_inicio": d.get("data_emissao") or "",
        "itens": itens, "confianca": d.get("confianca") or "MEDIA",
        "observacoes": d.get("observacoes") or "",
        "sem_cadastro": [i["descricao"] for i in itens if not i["insumo_id"]],
    }


def mapa(s: Session) -> dict[str, Any]:
    """Onde estão as obras, os equipamentos e o dinheiro."""
    from app.apps.erp.db.models.financeiro import EspecieTitulo, Rateio, Titulo

    obras = s.scalars(select(Obra).where(Obra.status == "ATIVA")).all()
    locado: dict[int, float] = {}
    itens_por_obra: dict[int, int] = {}
    for obra_id, valor, qtd in s.execute(
            select(LocacaoItem.obra_id,
                   func.sum((LocacaoItem.quantidade - LocacaoItem.quantidade_devolvida)
                            * LocacaoItem.valor_unitario),
                   func.count(LocacaoItem.id))
            .join(ContratoLocacao, ContratoLocacao.id == LocacaoItem.contrato_id)
            .where(ContratoLocacao.status == "ATIVO",
                   LocacaoItem.quantidade > LocacaoItem.quantidade_devolvida)
            .group_by(LocacaoItem.obra_id)).all():
        locado[obra_id] = float(valor or 0)
        itens_por_obra[obra_id] = qtd

    gasto: dict[int, float] = {}
    for obra_id, total in s.execute(
            select(Rateio.obra_id, func.sum(Rateio.valor))
            .join(Titulo, Titulo.id == Rateio.titulo_id)
            .where(Titulo.especie != EspecieTitulo.RECEBER,
                   Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))
            .group_by(Rateio.obra_id)).all():
        gasto[obra_id] = float(total or 0)

    pontos, por_municipio = [], {}
    for o in obras:
        item = {
            "obra_id": o.id, "codigo": o.codigo, "nome": o.nome,
            "municipio": o.municipio, "uf": o.uf,
            "latitude": float(o.latitude) if o.latitude else None,
            "longitude": float(o.longitude) if o.longitude else None,
            "valor_contrato": float(o.valor_contrato or 0),
            "gasto": gasto.get(o.id, 0.0),
            "locado_periodo": locado.get(o.id, 0.0),
            "equipamentos": itens_por_obra.get(o.id, 0),
            "fase": o.fase,
        }
        pontos.append(item)
        chave = f"{o.municipio or 'Sem município'}/{o.uf or '--'}"
        agr = por_municipio.setdefault(chave, {
            "municipio": o.municipio or "Sem município", "uf": o.uf,
            "obras": 0, "gasto": 0.0, "locado_periodo": 0.0, "equipamentos": 0,
            "valor_contrato": 0.0})
        agr["obras"] += 1
        agr["gasto"] += item["gasto"]
        agr["locado_periodo"] += item["locado_periodo"]
        agr["equipamentos"] += item["equipamentos"]
        agr["valor_contrato"] += item["valor_contrato"]

    regioes = sorted(por_municipio.values(), key=lambda x: x["gasto"], reverse=True)
    return {"pontos": pontos, "regioes": regioes,
            "com_coordenada": sum(1 for p in pontos if p["latitude"]),
            "total_obras": len(pontos)}


def identificar_contrato(s: Session, documento: dict[str, Any],
                         fornecedor_id: Optional[int] = None) -> Optional[dict[str, Any]]:
    """Dada a nota de débito da locadora, acha o contrato e a parcela.

    É o caso mais comum: todo mês a locadora manda a nota de débito e o boleto.
    Em vez de a pessoa procurar o contrato, o sistema encontra — casando o
    credor, o número do contrato citado no documento, o valor do período e a
    competência. Se não achar, é porque a locação não está cadastrada, e aí
    cobra-se o cadastro em vez de deixar passar.
    """
    forn_id = fornecedor_id or documento.get("fornecedor_id")
    texto = " ".join(str(documento.get(k) or "") for k in (
        "descricao", "numero_documento", "observacoes", "emitente_nome")).upper()

    stmt = select(ContratoLocacao).where(ContratoLocacao.status == "ATIVO").options(
        selectinload(ContratoLocacao.itens), selectinload(ContratoLocacao.fornecedor))
    if forn_id:
        stmt = stmt.where(ContratoLocacao.fornecedor_id == int(forn_id))
    contratos = list(s.scalars(stmt).all())
    if not contratos:
        return None

    valor_doc = None
    try:
        bruto = str(documento.get("valor_total") or documento.get("valor_bruto") or "").strip()
        if bruto:
            valor_doc = Decimal(bruto.replace(".", "").replace(",", ".")
                                if "," in bruto else bruto).quantize(_CENT)
    except Exception:
        valor_doc = None

    competencia = _data(documento.get("competencia_servico")) or \
        _data(documento.get("data_emissao")) or date.today()

    melhor, pontos_melhor, motivos_melhor = None, 0, []
    for c in contratos:
        pontos, motivos = 0, []
        if c.numero.upper() in texto:
            pontos += 5
            motivos.append(f"contrato {c.numero} citado no documento")
        if c.numero_externo and c.numero_externo.upper() in texto:
            pontos += 5
            motivos.append(f"nº da locadora ({c.numero_externo}) no documento")
        if forn_id and c.fornecedor_id == int(forn_id):
            pontos += 3
            motivos.append("credor confere")
        periodo = valor_periodo(s, c.id)
        if valor_doc is not None and periodo > 0:
            dif = abs(valor_doc - periodo)
            if dif <= _CENT:
                pontos += 4
                motivos.append(f"valor bate com a parcela (R$ {periodo})")
            elif dif <= periodo * Decimal("0.1"):
                pontos += 2
                motivos.append(f"valor próximo da parcela (R$ {periodo})")
        equipamentos = [i.descricao.upper()[:14] for i in c.itens]
        if any(e and e in texto for e in equipamentos):
            pontos += 2
            motivos.append("equipamento do contrato citado")
        if pontos > pontos_melhor:
            melhor, pontos_melhor, motivos_melhor = c, pontos, motivos

    if melhor is None or pontos_melhor < 3:
        return None

    parcela = s.scalars(select(LocacaoParcela).where(
        LocacaoParcela.contrato_id == melhor.id,
        LocacaoParcela.status == "PREVISTA",
        LocacaoParcela.competencia <= competencia.replace(day=28))
        .order_by(LocacaoParcela.competencia.desc())).first()
    if parcela is None:
        parcela = s.scalars(select(LocacaoParcela).where(
            LocacaoParcela.contrato_id == melhor.id,
            LocacaoParcela.status == "PREVISTA")
            .order_by(LocacaoParcela.competencia)).first()

    return {
        "contrato_id": melhor.id, "numero": melhor.numero,
        "numero_externo": melhor.numero_externo,
        "locadora": melhor.fornecedor.razao_social,
        "obra": melhor.obra_id,
        "valor_periodo": float(valor_periodo(s, melhor.id)),
        "confianca": "ALTA" if pontos_melhor >= 7 else "MEDIA",
        "motivos": motivos_melhor,
        "parcela": ({"id": parcela.id,
                     "competencia": parcela.competencia.strftime("%m/%Y"),
                     "vencimento": parcela.vencimento.isoformat(),
                     "valor_previsto": float(parcela.valor_previsto)}
                    if parcela else None),
        "aviso": (None if parcela else
                  "Contrato encontrado, mas sem parcela prevista em aberto — "
                  "gere a previsão antes de lançar."),
    }


def painel_por_obra(s: Session) -> list[dict[str, Any]]:
    """Quanto cada obra tem locado por período — a visão macro que falta hoje."""
    linhas = s.execute(
        select(LocacaoItem.obra_id,
               func.sum((LocacaoItem.quantidade - LocacaoItem.quantidade_devolvida)
                        * LocacaoItem.valor_unitario),
               func.count(LocacaoItem.id))
        .join(ContratoLocacao, ContratoLocacao.id == LocacaoItem.contrato_id)
        .where(ContratoLocacao.status == "ATIVO",
               LocacaoItem.quantidade > LocacaoItem.quantidade_devolvida)
        .group_by(LocacaoItem.obra_id)).all()
    obras = {o.id: o for o in s.scalars(select(Obra)).all()}
    saida = []
    for obra_id, valor, qtd in linhas:
        o = obras.get(obra_id)
        saida.append({"obra_id": obra_id, "obra": o.codigo if o else "—",
                      "nome": o.nome if o else "", "municipio": o.municipio if o else None,
                      "valor_periodo": float(valor or 0), "itens": qtd})
    saida.sort(key=lambda x: x["valor_periodo"], reverse=True)
    return saida
