# ============================================================================
# ERP — core/titulos/empreita.py
# Contratos de empreita e subcontratação, com medição.
#
# Substitui a planilha da obra. O problema que ela cria: a obra anota "fulano
# fez tantos metros" e pede o pagamento; se lançar duas vezes a mesma medição,
# o financeiro não tem como perceber no meio de centenas de títulos.
#
# Aqui o contrato tem SALDO. Cada medição consome o saldo, e o sistema recusa
# medir mais do que foi contratado, aponta período que se sobrepõe a uma
# medição anterior e abate automaticamente o adiantamento concedido. O título
# a pagar só nasce quando a medição é autorizada — e nasce amarrado a ela,
# então dá para ir do pagamento até a foto do serviço medido.
#
# Dois modos: MEDICAO (quantidade × preço unitário, com percentual de avanço)
# e PARCELAS (serviço fechado, pago em parcelas previstas).
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Obra, PerfilUsuario, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    ContratoMedicao, ContratoServico, StatusTitulo, Titulo,
)

logger = logging.getLogger(__name__)
_CENT = Decimal("0.01")


def _dec(v: Any, campo: str) -> Decimal:
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


def proximo_numero(s: Session) -> str:
    n = s.scalar(select(func.count()).select_from(ContratoServico)) or 0
    return f"CT{n + 1:05d}"


# ---------------------------------------------------------------------------
# Contrato
# ---------------------------------------------------------------------------
def criar_contrato(s: Session, dados: dict[str, Any], usuario: Usuario) -> ContratoServico:
    obra = s.get(Obra, int(dados.get("obra_id") or 0))
    if obra is None:
        raise ErroValidacao("Informe a obra do contrato.")
    forn = s.get(Fornecedor, int(dados.get("fornecedor_id") or 0))
    if forn is None:
        raise ErroValidacao("Informe o prestador (empreiteiro ou empresa).")
    objeto = (dados.get("objeto") or "").strip()
    if len(objeto) < 8:
        raise ErroValidacao("Descreva o serviço contratado (mínimo 8 caracteres).")

    modo = (dados.get("modo") or "MEDICAO").upper()
    if modo not in ("MEDICAO", "PARCELAS"):
        raise ErroValidacao("Modo deve ser MEDICAO ou PARCELAS.")

    quantidade = _dec(dados.get("quantidade"), "quantidade") if dados.get("quantidade") else None
    preco = _dec(dados.get("preco_unitario"), "preço unitário") if dados.get("preco_unitario") else None
    if modo == "MEDICAO" and quantidade and preco:
        valor = (quantidade * preco).quantize(_CENT)
    else:
        valor = _dec(dados.get("valor_total"), "valor total").quantize(_CENT)
    if valor <= 0:
        raise ErroValidacao("Valor do contrato deve ser maior que zero.")

    categoria_id = dados.get("categoria_id")
    if not categoria_id:
        cat = s.scalars(select(Categoria).where(Categoria.codigo == "3.2.01")).first()
        categoria_id = cat.id if cat else None

    contrato = ContratoServico(
        numero=proximo_numero(s), obra_id=obra.id, fornecedor_id=forn.id,
        categoria_id=categoria_id, objeto=objeto, modo=modo,
        unidade=(dados.get("unidade") or "").strip() or None,
        quantidade=quantidade, preco_unitario=preco, valor_total=valor,
        parcelas_previstas=int(dados.get("parcelas_previstas") or 0) or None,
        data_inicio=_data(dados.get("data_inicio")), data_fim=_data(dados.get("data_fim")),
        exige_foto=bool(dados.get("exige_foto", True)),
        observacoes=(dados.get("observacoes") or "").strip() or None,
        status="AGUARDANDO_AVAL", criado_por=usuario.id)
    s.add(contrato)
    s.flush()
    registrar_evento(s, "contrato_servico", contrato.id, "CRIADO", {
        "numero": contrato.numero, "obra": obra.codigo, "prestador": forn.razao_social,
        "objeto": objeto, "modo": modo, "valor": str(valor),
        "quantidade": str(quantidade) if quantidade else None,
        "unidade": contrato.unidade}, usuario.id)
    return contrato


def aprovar_contrato(s: Session, contrato_id: int, usuario: Usuario) -> ContratoServico:
    """Aval do contrato — mesma lógica do título: quem cria não aprova."""
    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    if c.status != "AGUARDANDO_AVAL":
        raise ErroValidacao(f"Contrato está {c.status}.")
    if c.criado_por == usuario.id:
        raise ErroPermissao("Quem cadastra o contrato não é quem o aprova.")
    if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO,
                              PerfilUsuario.GESTOR_OBRA, PerfilUsuario.SUPERVISOR_OBRA,
                              PerfilUsuario.FINANCEIRO):
        raise ErroPermissao("Perfil sem alçada para aprovar contrato de empreita.")
    c.status = "VIGENTE"
    c.aprovado_por = usuario.id
    c.aprovado_em = datetime.now(timezone.utc)
    registrar_evento(s, "contrato_servico", c.id, "APROVADO",
                     {"numero": c.numero, "por": usuario.nome}, usuario.id)
    return c


def aditivar(s: Session, contrato_id: int, valor: Any, motivo: str,
             usuario: Usuario, quantidade: Any = None) -> dict[str, Any]:
    """Ajusta o contratado: mil metros viraram mil e duzentos."""
    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    if len((motivo or "").strip()) < 10:
        raise ErroValidacao("Explique o aditivo (mínimo 10 caracteres).")
    delta = _dec(valor, "valor do aditivo").quantize(_CENT)
    c.valor_aditivos = (Decimal(c.valor_aditivos) + delta).quantize(_CENT)
    if quantidade and c.quantidade is not None:
        c.quantidade = Decimal(c.quantidade) + _dec(quantidade, "quantidade")
    if _valor_vigente(c) <= 0:
        raise ErroValidacao("O aditivo zeraria ou negativaria o contrato.")
    registrar_evento(s, "contrato_servico", c.id, "ADITIVADO", {
        "numero": c.numero, "delta": str(delta), "motivo": motivo,
        "valor_vigente": str(_valor_vigente(c))}, usuario.id)
    return {"numero": c.numero, "valor_vigente": float(_valor_vigente(c))}


def _valor_vigente(c: ContratoServico) -> Decimal:
    return (Decimal(c.valor_total) + Decimal(c.valor_aditivos)).quantize(_CENT)


def _medido(s: Session, contrato_id: int) -> Decimal:
    total = s.scalar(select(func.coalesce(func.sum(ContratoMedicao.valor_medido), 0))
                     .where(ContratoMedicao.contrato_id == contrato_id,
                            ContratoMedicao.status != "CANCELADA")) or 0
    return Decimal(total).quantize(_CENT)


def _adiantado(s: Session, contrato_id: int) -> Decimal:
    """Adiantamentos pagos no contrato, ainda não abatidos em medição."""
    concedido = s.scalar(select(func.coalesce(func.sum(Titulo.valor_liquido), 0))
                         .where(Titulo.contrato_servico_id == contrato_id,
                                Titulo.adiantamento_contrato.is_(True),
                                Titulo.status.not_in(["CANCELADO", "ESTORNADO"]))) or 0
    abatido = s.scalar(select(func.coalesce(
        func.sum(ContratoMedicao.valor_adiantamento_abatido), 0))
        .where(ContratoMedicao.contrato_id == contrato_id,
               ContratoMedicao.status != "CANCELADA")) or 0
    return (Decimal(concedido) - Decimal(abatido)).quantize(_CENT)


def saldo(s: Session, contrato_id: int) -> dict[str, Any]:
    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    vigente = _valor_vigente(c)
    medido = _medido(s, contrato_id)
    return {
        "valor_original": float(c.valor_total), "aditivos": float(c.valor_aditivos),
        "valor_vigente": float(vigente), "medido": float(medido),
        "saldo": float((vigente - medido).quantize(_CENT)),
        "percentual_medido": float((medido / vigente * 100).quantize(Decimal("0.01")))
                             if vigente else 0.0,
        "adiantamento_em_aberto": float(_adiantado(s, contrato_id)),
    }


# ---------------------------------------------------------------------------
# Medição
# ---------------------------------------------------------------------------
def criticar_medicao(s: Session, contrato_id: int, dados: dict[str, Any]) -> list[dict[str, str]]:
    """As checagens que a planilha não faz."""
    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    criticas: list[dict[str, str]] = []
    valor = _dec(dados.get("valor_medido"), "valor medido").quantize(_CENT)
    inicio, fim = _data(dados.get("periodo_inicio")), _data(dados.get("periodo_fim"))
    est = saldo(s, contrato_id)

    if valor <= 0:
        criticas.append({"codigo": "M1", "gravidade": "BLOQUEIA",
                         "msg": "Valor medido deve ser maior que zero."})
    if valor > Decimal(str(est["saldo"])) + _CENT:
        criticas.append({"codigo": "M2", "gravidade": "BLOQUEIA",
                         "msg": f"Medição de R$ {valor} excede o saldo do contrato "
                                f"(R$ {est['saldo']:.2f}). Se o serviço aumentou, "
                                f"registre um aditivo antes."})
    anteriores = s.scalars(select(ContratoMedicao).where(
        ContratoMedicao.contrato_id == contrato_id,
        ContratoMedicao.status != "CANCELADA")).all()
    for m in anteriores:
        if inicio and fim and m.periodo_inicio and m.periodo_fim:
            if inicio <= m.periodo_fim and fim >= m.periodo_inicio:
                criticas.append({"codigo": "M3", "gravidade": "BLOQUEIA",
                                 "msg": f"O período se sobrepõe à medição {m.numero} "
                                        f"({m.periodo_inicio:%d/%m} a {m.periodo_fim:%d/%m/%Y}). "
                                        f"É o caso clássico de medir duas vezes o mesmo serviço."})
                break
        if abs(Decimal(m.valor_medido) - valor) <= _CENT:
            criticas.append({"codigo": "M4", "gravidade": "CRITICA",
                             "msg": f"Mesmo valor da medição {m.numero} "
                                    f"(R$ {m.valor_medido}). Confira se não é repetida."})
    if c.modo == "MEDICAO" and dados.get("quantidade") and c.preco_unitario:
        qtd = _dec(dados["quantidade"], "quantidade")
        esperado = (qtd * Decimal(c.preco_unitario)).quantize(_CENT)
        if abs(esperado - valor) > _CENT:
            criticas.append({"codigo": "M5", "gravidade": "CRITICA",
                             "msg": f"{qtd} {c.unidade or 'un'} × R$ {c.preco_unitario} = "
                                    f"R$ {esperado}, mas o valor medido é R$ {valor}."})
    if est["adiantamento_em_aberto"] > 0:
        criticas.append({"codigo": "M6", "gravidade": "ALERTA",
                         "msg": f"Há adiantamento em aberto de "
                                f"R$ {est['adiantamento_em_aberto']:.2f} — será abatido "
                                f"desta medição."})
    if c.status != "VIGENTE":
        criticas.append({"codigo": "M7", "gravidade": "BLOQUEIA",
                         "msg": f"Contrato está {c.status} — só se mede contrato vigente."})
    return criticas


def registrar_medicao(s: Session, contrato_id: int, dados: dict[str, Any],
                      usuario: Usuario) -> ContratoMedicao:
    """A obra registra o que foi executado. Ainda não é pagamento."""
    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    criticas = criticar_medicao(s, contrato_id, dados)
    bloqueios = [x for x in criticas if x["gravidade"] == "BLOQUEIA"]
    if bloqueios:
        raise ErroValidacao(bloqueios[0]["msg"])

    valor = _dec(dados.get("valor_medido"), "valor medido").quantize(_CENT)
    disponivel = _adiantado(s, contrato_id)
    abate = min(disponivel, valor) if disponivel > 0 else Decimal("0.00")
    liquido = (valor - abate).quantize(_CENT)

    ultimo = s.scalar(select(func.coalesce(func.max(ContratoMedicao.numero), 0))
                      .where(ContratoMedicao.contrato_id == contrato_id)) or 0
    vigente = _valor_vigente(c)
    m = ContratoMedicao(
        contrato_id=c.id, numero=ultimo + 1,
        periodo_inicio=_data(dados.get("periodo_inicio")),
        periodo_fim=_data(dados.get("periodo_fim")),
        quantidade=_dec(dados["quantidade"], "quantidade") if dados.get("quantidade") else None,
        percentual=((valor / vigente * 100).quantize(Decimal("0.0001")) if vigente else None),
        valor_medido=valor, valor_adiantamento_abatido=abate, valor_liquido=liquido,
        observacao=(dados.get("observacao") or "").strip() or None,
        status="MEDIDA", medido_por=usuario.id)
    s.add(m)
    s.flush()
    registrar_evento(s, "contrato_servico", c.id, "MEDICAO_REGISTRADA", {
        "contrato": c.numero, "medicao": m.numero, "valor": str(valor),
        "abatido": str(abate), "liquido": str(liquido),
        "periodo": f"{m.periodo_inicio} a {m.periodo_fim}",
        "criticas": [x["codigo"] for x in criticas]}, usuario.id)
    return m


def autorizar_medicao(s: Session, medicao_id: int, usuario: Usuario,
                      dados_pagamento: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """O supervisor confere e autoriza: aí sim vira título a pagar."""
    m = s.get(ContratoMedicao, medicao_id, options=[
        selectinload(ContratoMedicao.contrato).selectinload(ContratoServico.fornecedor)])
    if m is None:
        raise ErroValidacao("Medição não encontrada.")
    if m.status != "MEDIDA":
        raise ErroValidacao(f"Medição está {m.status}.")
    c = m.contrato
    if m.medido_por == usuario.id and usuario.perfil not in (
            PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO):
        raise ErroPermissao("Quem mediu não autoriza a própria medição.")

    if c.exige_foto:
        from app.apps.erp.db.models.financeiro import Anexo
        tem = s.scalars(select(Anexo).where(
            Anexo.entidade_tipo == "medicao", Anexo.entidade_id == m.id)).first()
        if tem is None:
            raise ErroValidacao(
                "Este contrato exige foto da execução. Anexe ao menos uma imagem "
                "à medição antes de autorizar.")

    from app.apps.erp.core.titulos.service import criar_titulo
    d = dados_pagamento or {}
    titulo = criar_titulo(s, {
        "tipo": "T5_EMPREITEIRO",
        "fornecedor_id": c.fornecedor_id, "categoria_id": c.categoria_id,
        "descricao": f"{c.numero} medição {m.numero} — {c.objeto[:120]}",
        "valor_bruto": str(m.valor_liquido),
        "competencia": d.get("competencia") or (m.periodo_fim or date.today()).strftime("%Y-%m"),
        "forma_pagamento": d.get("forma_pagamento") or "PIX",
        "fornecedor_conta_id": d.get("fornecedor_conta_id"),
        "contrato_id": d.get("contrato_id"),
        "contrato_servico_id": c.id,
        "parcelas": d.get("parcelas") or [{
            "vencimento": d.get("vencimento")
                          or (date.today() + timedelta(days=15)).isoformat(),
            "valor": str(m.valor_liquido)}],
        "rateios": [{"obra_id": c.obra_id, "valor": str(m.valor_liquido)}],
        "justificativa_excecao": f"Medição {m.numero} do contrato {c.numero}.",
    }, usuario)
    titulo.contrato_servico_id = c.id
    titulo.medicao_id = m.id
    m.status = "FATURADA"
    m.titulo_id = titulo.id
    m.autorizado_por = usuario.id
    m.autorizado_em = datetime.now(timezone.utc)
    s.flush()

    est = saldo(s, c.id)
    if est["saldo"] <= 0.01 and c.status == "VIGENTE":
        c.status = "CONCLUIDO"
    registrar_evento(s, "contrato_servico", c.id, "MEDICAO_AUTORIZADA", {
        "contrato": c.numero, "medicao": m.numero, "titulo": titulo.numero_sp,
        "valor": str(m.valor_liquido), "por": usuario.nome,
        "saldo_restante": est["saldo"]}, usuario.id)
    return {"medicao": m.numero, "titulo": titulo.numero_sp,
            "valor": float(m.valor_liquido), "saldo_restante": est["saldo"],
            "contrato_status": c.status}


def detalhar(s: Session, contrato_id: int) -> dict[str, Any]:
    c = s.get(ContratoServico, contrato_id, options=[
        selectinload(ContratoServico.obra), selectinload(ContratoServico.fornecedor),
        selectinload(ContratoServico.medicoes)])
    if c is None:
        raise ErroValidacao("Contrato não encontrado.")
    est = saldo(s, contrato_id)
    return {
        "id": c.id, "numero": c.numero, "objeto": c.objeto, "modo": c.modo,
        "obra": f"{c.obra.codigo} · {c.obra.nome}", "obra_id": c.obra_id,
        "prestador": c.fornecedor.razao_social, "fornecedor_id": c.fornecedor_id,
        "unidade": c.unidade, "quantidade": float(c.quantidade) if c.quantidade else None,
        "preco_unitario": float(c.preco_unitario) if c.preco_unitario else None,
        "status": c.status, "exige_foto": c.exige_foto,
        "parcelas_previstas": c.parcelas_previstas,
        "periodo": (f"{c.data_inicio:%d/%m/%Y} a {c.data_fim:%d/%m/%Y}"
                    if c.data_inicio and c.data_fim else ""),
        **est,
        "medicoes": [{
            "id": m.id, "numero": m.numero,
            "periodo": (f"{m.periodo_inicio:%d/%m} a {m.periodo_fim:%d/%m/%Y}"
                        if m.periodo_inicio and m.periodo_fim else ""),
            "quantidade": float(m.quantidade) if m.quantidade else None,
            "valor_medido": float(m.valor_medido),
            "abatido": float(m.valor_adiantamento_abatido),
            "valor_liquido": float(m.valor_liquido),
            "percentual": float(m.percentual) if m.percentual else None,
            "status": m.status, "titulo_id": m.titulo_id,
            "observacao": m.observacao,
            "medido_por": (s.get(Usuario, m.medido_por).nome if m.medido_por else "—"),
            "quando": m.criado_em.strftime("%d/%m/%Y"),
        } for m in c.medicoes],
    }


def listar(s: Session, usuario: Optional[Usuario] = None) -> list[dict[str, Any]]:
    from app.apps.erp.core.auth.permissoes import obras_do_usuario
    stmt = (select(ContratoServico)
            .options(selectinload(ContratoServico.obra),
                     selectinload(ContratoServico.fornecedor))
            .order_by(ContratoServico.id.desc()).limit(300))
    if usuario is not None:
        permitidas = obras_do_usuario(s, usuario)
        if permitidas is not None:
            stmt = stmt.where(ContratoServico.obra_id.in_(permitidas or [0]))
    saida = []
    for c in s.scalars(stmt).all():
        est = saldo(s, c.id)
        saida.append({
            "id": c.id, "numero": c.numero, "objeto": c.objeto[:110],
            "obra": c.obra.codigo, "prestador": c.fornecedor.razao_social,
            "modo": c.modo, "status": c.status,
            "unidade": c.unidade,
            "quantidade": float(c.quantidade) if c.quantidade else None,
            **est,
            "medicoes": len(c.medicoes),
        })
    return saida


# ---------------------------------------------------------------------------
# Bloqueio de período
# ---------------------------------------------------------------------------
def periodo_bloqueado_ate(s: Session) -> Optional[date]:
    linha = s.scalars(select(PeriodoBloqueado).order_by(
        PeriodoBloqueado.id.desc())).first() if _tem_tabela(s) else None
    if linha is None:
        return None
    agora = datetime.now(timezone.utc)
    if linha.liberado_expira and linha.liberado_expira > agora and linha.liberado_ate:
        # janela aberta pelo diretor: tudo até `liberado_ate` volta a ser editável.
        # Se a liberação cobre todo o período fechado, não sobra bloqueio nenhum.
        if linha.liberado_ate >= linha.ate_data:
            return None
        return linha.liberado_ate
    return linha.ate_data


def _tem_tabela(s: Session) -> bool:
    from app.apps.erp.db.models.financeiro import PeriodoBloqueado as _P
    try:
        s.scalar(select(func.count()).select_from(_P))
        return True
    except Exception:
        s.rollback()
        return False


def exigir_periodo_aberto(s: Session, data_alvo: date, usuario: Usuario) -> None:
    """Recusa alteração em período já fechado."""
    limite = periodo_bloqueado_ate(s)
    if limite and data_alvo <= limite:
        if usuario.perfil in (PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO):
            return                       # diretor e admin passam sempre
        raise ErroPermissao(
            f"Período fechado até {limite:%d/%m/%Y} — já conciliado. Peça ao diretor "
            f"financeiro para destravar a data antes de alterar.")


def definir_bloqueio(s: Session, ate: Any, usuario: Usuario) -> dict[str, Any]:
    from app.apps.erp.db.models.financeiro import PeriodoBloqueado as _P
    if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO):
        raise ErroPermissao("Só o diretor financeiro ou o administrador fecha período.")
    data_limite = _data(ate)
    if data_limite is None:
        raise ErroValidacao("Informe a data até a qual o período fica fechado.")
    s.add(_P(ate_data=data_limite))
    registrar_evento(s, "periodo", 0, "FECHADO",
                     {"ate": data_limite.isoformat(), "por": usuario.nome}, usuario.id)
    return {"bloqueado_ate": data_limite.isoformat()}


def destravar(s: Session, ate: Any, motivo: str, usuario: Usuario,
              horas: int = 24) -> dict[str, Any]:
    """Abre uma janela temporária para corrigir algo no passado."""
    from app.apps.erp.db.models.financeiro import PeriodoBloqueado as _P
    if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO):
        raise ErroPermissao("Só o diretor financeiro ou o administrador destrava período.")
    if len((motivo or "").strip()) < 10:
        raise ErroValidacao("Explique por que o período precisa ser reaberto.")
    linha = s.scalars(select(_P).order_by(_P.id.desc())).first()
    if linha is None:
        raise ErroValidacao("Nenhum período fechado.")
    linha.liberado_ate = _data(ate)
    linha.liberado_por = usuario.id
    linha.liberado_em = datetime.now(timezone.utc)
    linha.liberado_motivo = motivo.strip()
    linha.liberado_expira = datetime.now(timezone.utc) + timedelta(hours=horas)
    registrar_evento(s, "periodo", linha.id, "DESTRAVADO", {
        "ate": str(linha.liberado_ate), "motivo": motivo, "horas": horas,
        "por": usuario.nome}, usuario.id)
    return {"liberado_ate": str(linha.liberado_ate),
            "expira_em": linha.liberado_expira.strftime("%d/%m/%Y %H:%M")}


from app.apps.erp.db.models.financeiro import PeriodoBloqueado  # noqa: E402
