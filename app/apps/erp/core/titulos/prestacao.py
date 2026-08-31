# ============================================================================
# ERP — core/titulos/prestacao.py
# Prestação de contas de FUNDO FIXO e importação de FATURA DE CARTÃO.
#
# Por que não é um título comum: são muitas despesas pequenas dentro de um
# lançamento só. O financeiro recebe 20 comprovantes e, na prática, ninguém
# confere os 20 — é aí que passa o erro e a fraude. Então:
#
#   1. Os comprovantes são lidos um a um (mesmo leitor do lançamento) e viram
#      LINHAS com data, descrição, estabelecimento, valor e obra. O que a
#      leitura não conseguir, a pessoa completa à mão.
#   2. Cada linha carrega o seu comprovante — dá para abrir e conferir sem
#      procurar em pasta nenhuma.
#   3. O sistema CRITICA cada linha e o conjunto: duplicidade entre os itens,
#      repetição de comprovante já usado em prestações anteriores, valores
#      redondos demais, despesa em fim de semana, item acima do teto, soma que
#      não fecha, data fora do período, gasto fora do padrão histórico da
#      pessoa. Nenhum indício some da tela sem alguém assumir que analisou.
#
# Fundo fixo tem duas naturezas: ADIANTAMENTO (a empresa adiantou e a pessoa
# presta contas do que gastou) e REEMBOLSO (gastou do próprio bolso e pede de
# volta). A categoria do título é a conta de fundo fixo; o rateio é por OBRA.
# No cartão, cada linha tem também a sua categoria, porque varia item a item.
# ============================================================================
from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Categoria, Obra, Usuario
from app.apps.erp.db.models.financeiro import (
    Anexo, StatusTitulo, Titulo, TituloItem,
)

logger = logging.getLogger(__name__)

TETO_ITEM_PADRAO = Decimal("500.00")     # acima disso, fundo fixo merece explicação
_CENT = Decimal("0.01")


def _dec(v: Any, campo: str = "valor") -> Decimal:
    try:
        s = str(v or "").strip().replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return Decimal(s).quantize(_CENT)
    except (InvalidOperation, TypeError):
        raise ErroValidacao(f"Valor inválido em {campo}: {v!r}")


def _data(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    s = str(v or "").strip()[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _normalizar(t: str) -> str:
    t = unicodedata.normalize("NFKD", (t or "").upper())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", t).strip()


# ---------------------------------------------------------------------------
# Leitura dos comprovantes
# ---------------------------------------------------------------------------
def ler_comprovante_item(conteudo: bytes, nome_arquivo: str) -> dict[str, Any]:
    """Lê UM comprovante e devolve a linha correspondente."""
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
    try:
        d = ler_documento(conteudo, nome_arquivo,
                          dica_usuario="É um comprovante de despesa miúda (cupom fiscal, "
                                       "recibo, nota de compra). Extraia data, valor total, "
                                       "estabelecimento e o que foi comprado.")
    except ErroLeitura as e:
        return {"descricao": "", "valor": "", "data_despesa": "",
                "origem_leitura": "FALHA", "confianca": "BAIXA",
                "observacao": f"não foi possível ler: {e}"}
    return {
        "data_despesa": d.get("data_emissao") or "",
        "descricao": (d.get("descricao") or "")[:200],
        "estabelecimento": (d.get("emitente_nome") or "")[:120],
        "documento": d.get("numero_documento") or "",
        "valor": d.get("valor_total") or "",
        "origem_leitura": d.get("origem_leitura") or "IA",
        "confianca": d.get("confianca") or "MEDIA",
        "observacao": d.get("observacoes") or "",
        "campos_ilegiveis": d.get("campos_ilegiveis") or [],
    }


def ler_fatura_cartao(conteudo: bytes, nome_arquivo: str) -> dict[str, Any]:
    """Lê a fatura inteira e devolve todas as compras como linhas."""
    from app.apps.erp.core.documentos.leitor import ErroLeitura, ler_documento
    try:
        d = ler_documento(conteudo, nome_arquivo,
                          dica_usuario="É uma FATURA DE CARTÃO DE CRÉDITO. Liste TODAS as "
                                       "compras em 'itens', cada uma com data, "
                                       "estabelecimento na descrição e valor. O total da "
                                       "fatura vai em valor_total.")
    except ErroLeitura as e:
        raise ErroValidacao(f"Não consegui ler a fatura: {e}")
    itens = []
    for i, item in enumerate(d.get("itens") or [], start=1):
        itens.append({
            "ordem": i, "data_despesa": item.get("data") or d.get("data_emissao") or "",
            "descricao": (item.get("descricao") or "")[:200],
            "estabelecimento": (item.get("descricao") or "")[:120],
            "documento": item.get("documento") or "",
            "valor": item.get("valor") or "",
            "origem_leitura": "FATURA", "confianca": d.get("confianca") or "MEDIA",
        })
    return {"itens": itens, "total_fatura": d.get("valor_total") or "",
            "vencimento": (d.get("parcelas") or [{}])[0].get("vencimento") or "",
            "emitente": d.get("emitente_nome") or "",
            "confianca": d.get("confianca"), "observacoes": d.get("observacoes") or ""}


# ---------------------------------------------------------------------------
# Críticas
# ---------------------------------------------------------------------------
def _historico_do_solicitante(s: Session, usuario_id: int,
                              modalidade: str) -> dict[str, Any]:
    """Padrão de gasto da pessoa, para comparar com o que está sendo lançado."""
    titulos = s.scalars(select(Titulo).where(
        Titulo.solicitante_id == usuario_id, Titulo.modalidade == modalidade,
        Titulo.status.not_in([StatusTitulo.CANCELADO, StatusTitulo.ESTORNADO])
    ).order_by(Titulo.id.desc()).limit(24)).all()
    if not titulos:
        return {"prestacoes": 0, "media": None, "maior": None, "itens_media": None,
                "documentos_usados": set(), "descricoes": []}
    ids = [t.id for t in titulos]
    itens = s.scalars(select(TituloItem).where(TituloItem.titulo_id.in_(ids))).all()
    valores = [float(t.valor_liquido) for t in titulos]
    return {
        "prestacoes": len(titulos),
        "media": round(sum(valores) / len(valores), 2),
        "maior": round(max(valores), 2),
        "itens_media": round(len(itens) / len(titulos), 1) if titulos else None,
        "documentos_usados": {(_normalizar(i.estabelecimento or ""), str(i.documento or ""),
                               str(i.valor)) for i in itens if i.documento},
        "descricoes": [(_normalizar(i.descricao), float(i.valor),
                        i.data_despesa.isoformat() if i.data_despesa else "")
                       for i in itens],
        "ultimas": [{"numero_sp": t.numero_sp, "valor": float(t.valor_liquido),
                     "competencia": t.competencia.strftime("%m/%Y"),
                     "status": t.status.value} for t in titulos[:6]],
    }


def criticar(s: Session, itens: list[dict[str, Any]], *, solicitante_id: int,
             modalidade: str = "FUNDO_FIXO", total_declarado: Optional[Any] = None,
             periodo_inicio: Optional[date] = None,
             periodo_fim: Optional[date] = None,
             teto_item: Decimal = TETO_ITEM_PADRAO) -> dict[str, Any]:
    """Roda as críticas sobre as linhas e sobre o conjunto."""
    hist = _historico_do_solicitante(s, solicitante_id, modalidade)
    hoje = date.today()
    por_item: dict[int, list[dict[str, str]]] = {}
    gerais: list[dict[str, str]] = []

    def marcar(idx: int, codigo: str, msg: str, gravidade: str = "ALERTA") -> None:
        por_item.setdefault(idx, []).append(
            {"codigo": codigo, "msg": msg, "gravidade": gravidade})

    valores, chaves, soma = [], [], Decimal("0.00")
    for i, item in enumerate(itens):
        try:
            valor = _dec(item.get("valor"), f"item {i + 1}")
        except ErroValidacao:
            marcar(i, "F0", "Valor não informado ou ilegível.", "BLOQUEIA")
            continue
        soma += valor
        valores.append(valor)
        d = _data(item.get("data_despesa"))
        desc = _normalizar(item.get("descricao") or "")
        estab = _normalizar(item.get("estabelecimento") or "")

        if valor > teto_item:
            marcar(i, "F1", f"Acima do teto de fundo fixo (R$ {teto_item}) — "
                            f"despesa desse porte deveria seguir o fluxo normal.", "CRITICA")
        if valor % Decimal("50") == 0 and valor >= Decimal("100"):
            marcar(i, "F2", "Valor exatamente redondo — confira o comprovante.")
        if d is None:
            marcar(i, "F3", "Sem data — preencha para conferência.", "CRITICA")
        else:
            if d > hoje:
                marcar(i, "F4", "Data no futuro.", "BLOQUEIA")
            if (hoje - d).days > 90:
                marcar(i, "F5", f"Despesa de {(hoje - d).days} dias atrás — fora do prazo usual.")
            if periodo_inicio and d < periodo_inicio:
                marcar(i, "F6", f"Anterior ao período da prestação ({periodo_inicio:%d/%m/%Y}).",
                       "CRITICA")
            if periodo_fim and d > periodo_fim:
                marcar(i, "F6", f"Posterior ao período da prestação ({periodo_fim:%d/%m/%Y}).",
                       "CRITICA")
            if d.weekday() >= 5:
                marcar(i, "F7", "Despesa em fim de semana.")
        if not desc or len(desc) < 4:
            marcar(i, "F8", "Descrição vaga — diga o que foi comprado.", "CRITICA")
        if not item.get("obra_id"):
            marcar(i, "F9", "Sem obra — todo gasto precisa de centro de custo.", "BLOQUEIA")
        if modalidade == "CARTAO" and not item.get("categoria_id"):
            marcar(i, "F10", "Sem categoria — na fatura de cartão cada compra tem a sua.",
                   "BLOQUEIA")
        if (item.get("origem_leitura") or "") == "FALHA":
            marcar(i, "F11", "Comprovante ilegível — confira manualmente.", "CRITICA")
        if modalidade == "FUNDO_FIXO" and not item.get("anexo_id") \
                and not item.get("_sem_anexo_ok"):
            marcar(i, "F12", "Sem comprovante anexado.", "CRITICA")

        chave = (estab, str(item.get("documento") or ""), str(valor))
        chaves.append(chave)
        if item.get("documento") and chave in hist["documentos_usados"]:
            marcar(i, "F13", "Este comprovante já foi usado em prestação anterior.", "BLOQUEIA")
        for desc_h, valor_h, data_h in hist["descricoes"]:
            if (abs(valor_h - float(valor)) < 0.01 and desc
                    and SequenceMatcher(None, desc, desc_h).ratio() >= 0.9
                    and d and data_h == d.isoformat()):
                marcar(i, "F14", "Idêntica a uma despesa já prestada antes "
                                 "(mesma data, valor e descrição).", "BLOQUEIA")
                break

    # duplicidade dentro da própria prestação
    for chave, quantas in Counter(c for c in chaves if c[1] or c[0]).items():
        if quantas > 1:
            for i, c in enumerate(chaves):
                if c == chave:
                    marcar(i, "F15", f"Aparece {quantas}× nesta prestação "
                                     f"(mesmo estabelecimento, documento e valor).", "BLOQUEIA")

    # conjunto
    if total_declarado is not None:
        declarado = _dec(total_declarado, "total")
        if abs(declarado - soma) > _CENT:
            gerais.append({"codigo": "F16", "gravidade": "BLOQUEIA",
                           "msg": f"A soma dos itens (R$ {soma}) não fecha com o total "
                                  f"declarado (R$ {declarado})."})
    if hist["media"] and float(soma) > hist["media"] * 2 and hist["prestacoes"] >= 3:
        gerais.append({"codigo": "F17", "gravidade": "CRITICA",
                       "msg": f"Total de R$ {soma} é mais que o dobro da média desta pessoa "
                              f"(R$ {hist['media']:.2f} em {hist['prestacoes']} prestações)."})
    if hist["maior"] and float(soma) > hist["maior"]:
        gerais.append({"codigo": "F18", "gravidade": "ALERTA",
                       "msg": f"Maior prestação já feita por esta pessoa "
                              f"(anterior: R$ {hist['maior']:.2f})."})
    if len(itens) >= 15:
        gerais.append({"codigo": "F19", "gravidade": "ALERTA",
                       "msg": f"{len(itens)} itens — confira com atenção antes de aprovar."})

    bloqueios = sum(1 for lista in por_item.values() for c in lista
                    if c["gravidade"] == "BLOQUEIA")
    bloqueios += sum(1 for c in gerais if c["gravidade"] == "BLOQUEIA")
    criticas_n = sum(1 for lista in por_item.values() for c in lista
                     if c["gravidade"] == "CRITICA")
    criticas_n += sum(1 for c in gerais if c["gravidade"] == "CRITICA")

    return {
        "por_item": {str(k): v for k, v in por_item.items()},
        "gerais": gerais, "soma": float(soma),
        "bloqueios": bloqueios, "criticas": criticas_n,
        "exige_atencao": bool(bloqueios or criticas_n),
        "historico": {k: v for k, v in hist.items()
                      if k not in ("documentos_usados", "descricoes")},
    }


# ---------------------------------------------------------------------------
# Gravação
# ---------------------------------------------------------------------------
def criar_prestacao(s: Session, dados: dict[str, Any], usuario: Usuario) -> Titulo:
    """Cria o título de fundo fixo ou de fatura de cartão, com os itens."""
    modalidade = (dados.get("modalidade") or "FUNDO_FIXO").upper()
    if modalidade not in ("FUNDO_FIXO", "CARTAO"):
        raise ErroValidacao(f"Modalidade inválida: {modalidade}")
    itens = dados.get("itens") or []
    if not itens:
        raise ErroValidacao("Adicione ao menos uma despesa.")

    periodo_inicio = _data(dados.get("periodo_inicio"))
    periodo_fim = _data(dados.get("periodo_fim"))
    critica = criticar(s, itens, solicitante_id=usuario.id, modalidade=modalidade,
                       periodo_inicio=periodo_inicio, periodo_fim=periodo_fim)
    if critica["bloqueios"] and not dados.get("forcar"):
        raise ErroValidacao(
            f"{critica['bloqueios']} bloqueio(s) na conferência — corrija antes de enviar.")

    total = _dec(critica["soma"], "total")
    categoria_id = dados.get("categoria_id")
    if not categoria_id:
        codigo = "3.4.08" if modalidade == "FUNDO_FIXO" else "5.3.99"
        cat = s.scalars(select(Categoria).where(Categoria.codigo == codigo)).first()
        if cat is None:
            raise ErroValidacao(f"Conta {codigo} não encontrada — instale o plano financeiro.")
        categoria_id = cat.id

    # rateio por obra vem da soma dos itens
    por_obra: dict[int, Decimal] = {}
    for item in itens:
        obra_id = item.get("obra_id")
        if not obra_id:
            raise ErroValidacao("Todas as despesas precisam de obra.")
        por_obra[int(obra_id)] = por_obra.get(int(obra_id), Decimal("0")) + _dec(item["valor"])

    fundo_tipo = (dados.get("fundo_fixo_tipo") or "REEMBOLSO").upper()
    if modalidade == "FUNDO_FIXO" and fundo_tipo not in ("ADIANTAMENTO", "REEMBOLSO"):
        raise ErroValidacao("Informe se é prestação de adiantamento ou pedido de reembolso.")

    rotulo = ("Prestação de contas — fundo fixo" if modalidade == "FUNDO_FIXO"
              else "Fatura de cartão")
    periodo_txt = (f" ({periodo_inicio:%d/%m} a {periodo_fim:%d/%m/%Y})"
                   if periodo_inicio and periodo_fim else "")

    from app.apps.erp.core.titulos.service import criar_titulo
    titulo = criar_titulo(s, {
        "tipo": "T10_FUNDO_FIXO" if modalidade == "FUNDO_FIXO" else "T14_EXCECAO_SEM_NOTA",
        "fornecedor_id": dados.get("fornecedor_id"),
        "categoria_id": categoria_id,
        "descricao": (dados.get("descricao")
                      or f"{rotulo} — {usuario.nome}{periodo_txt}")[:200],
        "valor_bruto": str(total),
        "competencia": dados.get("competencia") or (periodo_fim or date.today()).strftime("%Y-%m"),
        "forma_pagamento": dados.get("forma_pagamento") or "PIX",
        "fornecedor_conta_id": dados.get("fornecedor_conta_id"),
        "parcelas": dados.get("parcelas") or [
            {"vencimento": (dados.get("vencimento")
                            or (date.today() + timedelta(days=7)).isoformat()),
             "valor": str(total)}],
        "rateios": [{"obra_id": oid, "valor": str(v)} for oid, v in por_obra.items()],
        "justificativa_excecao": dados.get("justificativa")
            or f"{rotulo}: despesas miúdas com comprovantes anexados item a item.",
        "interessados": dados.get("interessados") or [],
    }, usuario)

    titulo.modalidade = modalidade
    titulo.fundo_fixo_tipo = fundo_tipo if modalidade == "FUNDO_FIXO" else None
    titulo.periodo_prestacao_inicio = periodo_inicio
    titulo.periodo_prestacao_fim = periodo_fim
    if dados.get("adiantamento_titulo_id"):
        titulo.adiantamento_titulo_id = int(dados["adiantamento_titulo_id"])
    s.flush()

    for i, item in enumerate(itens, start=1):
        s.add(TituloItem(
            titulo_id=titulo.id, ordem=i,
            data_despesa=_data(item.get("data_despesa")),
            descricao=(item.get("descricao") or "").strip()[:200] or "(sem descrição)",
            estabelecimento=(item.get("estabelecimento") or "").strip()[:120] or None,
            documento=(item.get("documento") or "").strip() or None,
            valor=_dec(item["valor"]), obra_id=int(item["obra_id"]),
            categoria_id=int(item["categoria_id"]) if item.get("categoria_id") else None,
            anexo_id=int(item["anexo_id"]) if item.get("anexo_id") else None,
            origem_leitura=item.get("origem_leitura"), confianca=item.get("confianca"),
            criticas=critica["por_item"].get(str(i - 1), []),
            observacao=(item.get("observacao") or "").strip() or None))

    registrar_evento(s, "titulo", titulo.id, "PRESTACAO_CRIADA", {
        "numero_sp": titulo.numero_sp, "modalidade": modalidade, "tipo": fundo_tipo,
        "itens": len(itens), "total": str(total),
        "bloqueios": critica["bloqueios"], "criticas": critica["criticas"],
        "forcado": bool(dados.get("forcar"))}, usuario.id)
    logger.info("ERP/prestação: %s com %d item(ns), %d crítica(s)",
                titulo.numero_sp, len(itens), critica["criticas"])
    return titulo


def detalhar(s: Session, titulo_id: int) -> dict[str, Any]:
    """Abre a prestação: as linhas, os comprovantes e as críticas."""
    t = s.get(Titulo, titulo_id, options=[selectinload(Titulo.fornecedor)])
    if t is None:
        raise ErroValidacao("Título não encontrado.")
    itens = s.scalars(select(TituloItem).where(TituloItem.titulo_id == titulo_id)
                      .order_by(TituloItem.ordem)).all()
    obras = {o.id: o.codigo for o in s.scalars(select(Obra)).all()}
    cats = {c.id: f"{c.codigo} · {c.descricao}" for c in s.scalars(select(Categoria)).all()}
    solicitante = s.get(Usuario, t.solicitante_id)
    return {
        "id": t.id, "numero_sp": t.numero_sp, "modalidade": t.modalidade,
        "fundo_fixo_tipo": t.fundo_fixo_tipo, "status": t.status.value,
        "solicitante": solicitante.nome if solicitante else "—",
        "descricao": t.descricao, "total": float(t.valor_liquido),
        "periodo": (f"{t.periodo_prestacao_inicio:%d/%m/%Y} a "
                    f"{t.periodo_prestacao_fim:%d/%m/%Y}"
                    if t.periodo_prestacao_inicio and t.periodo_prestacao_fim else ""),
        "alertas_confirmados": t.alertas_confirmados or [],
        "itens": [{
            "id": i.id, "ordem": i.ordem,
            "data": i.data_despesa.isoformat() if i.data_despesa else None,
            "descricao": i.descricao, "estabelecimento": i.estabelecimento,
            "documento": i.documento, "valor": float(i.valor),
            "obra": obras.get(i.obra_id, "—"),
            "categoria": cats.get(i.categoria_id) if i.categoria_id else None,
            "anexo_id": i.anexo_id, "origem_leitura": i.origem_leitura,
            "confianca": i.confianca, "criticas": i.criticas or [],
            "conferido": bool(i.conferido_em),
            "observacao": i.observacao,
        } for i in itens],
    }


def confirmar_analise(s: Session, titulo_id: int, usuario: Usuario, *,
                      item_id: Optional[int] = None, observacao: str = "") -> dict[str, Any]:
    """Registra que alguém olhou o indício e assumiu a responsabilidade."""
    agora = datetime.now(timezone.utc)
    if item_id:
        item = s.get(TituloItem, item_id)
        if item is None or item.titulo_id != titulo_id:
            raise ErroValidacao("Item não encontrado nesta prestação.")
        item.conferido_por = usuario.id
        item.conferido_em = agora
        if observacao:
            item.observacao = observacao.strip()[:500]
        registrar_evento(s, "titulo", titulo_id, "ITEM_CONFERIDO", {
            "item": item.ordem, "descricao": item.descricao, "valor": str(item.valor),
            "por": usuario.nome, "observacao": observacao}, usuario.id)
        return {"item": item.ordem, "conferido_por": usuario.nome}

    t = s.get(Titulo, titulo_id)
    if t is None:
        raise ErroValidacao("Título não encontrado.")
    if len((observacao or "").strip()) < 10:
        raise ErroValidacao("Descreva o que foi analisado (mínimo 10 caracteres).")
    confirmados = list(t.alertas_confirmados or [])
    confirmados.append({"por": usuario.nome, "perfil": usuario.perfil.value,
                        "quando": agora.isoformat(), "observacao": observacao.strip()})
    t.alertas_confirmados = confirmados
    registrar_evento(s, "titulo", titulo_id, "ALERTAS_ANALISADOS",
                     {"por": usuario.nome, "observacao": observacao}, usuario.id)
    return {"confirmacoes": len(confirmados)}


def historico_do_solicitante(s: Session, usuario_id: int,
                             modalidade: str = "FUNDO_FIXO") -> dict[str, Any]:
    """Histórico visível na hora de lançar e na hora de aprovar."""
    h = _historico_do_solicitante(s, usuario_id, modalidade)
    return {k: v for k, v in h.items() if k not in ("documentos_usados", "descricoes")}
