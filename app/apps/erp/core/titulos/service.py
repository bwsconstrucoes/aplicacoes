# ============================================================================
# BWS ERP — core/titulos/service.py
# Motor de títulos: lançamento dirigido (T1–T14), parcelas, rateios,
# transições de status, aprovação por alçada, devolução, cancelamento e
# estorno. Regra pura — não conhece Streamlit nem FastAPI.
#
# Contrato de entrada de criar_titulo (dict `dados`):
#   tipo (str T1..T14) · fornecedor_id · descricao · valor_bruto (Decimal/str)
#   competencia (date | 'AAAA-MM' | 'AAAA-MM-DD') · categoria_id
#   forma_pagamento · fornecedor_conta_id? · pedido_id? · contrato_id?
#   documento_fiscal_id? · data_emissao_doc? · justificativa_excecao?
#   retencoes?: [{tipo, base_calculo, aliquota, valor, cno_obra?}]
#   parcelas:  [{vencimento, valor, linha_digitavel?}]   (>= 1)
#   rateios:   [{obra_id, valor}]                        (>= 1)
# ============================================================================
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.core.pagamentos.boleto import validar_linha_digitavel
from app.apps.erp.core.titulos.regras import modo_transicao, regras_de
from app.apps.erp.db.models.cadastros import (
    Alcada, Categoria, FormaPagamento, Fornecedor, FornecedorConta, Obra,
    PerfilUsuario, StatusConta, TipoTitulo, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    Parcela, Rateio, Retencao, StatusParcela, StatusTitulo, TipoRetencao, Titulo,
)

_CENT = Decimal("0.01")

# Ranking de perfis para alçadas (maior = mais poder)
_RANK = {
    PerfilUsuario.CONSULTA: 0, PerfilUsuario.LANCADOR: 1,
    PerfilUsuario.APROVADOR: 2, PerfilUsuario.FINANCEIRO: 3,
    PerfilUsuario.ADMIN: 4,
}


# ---------------------------------------------------------------------------
# Helpers de conversão/validação
# ---------------------------------------------------------------------------
def _dec(valor: Any, campo: str) -> Decimal:
    try:
        d = Decimal(str(valor).replace(",", "."))
    except (InvalidOperation, TypeError):
        raise ErroValidacao(f"Valor inválido em {campo}: {valor!r}")
    return d.quantize(_CENT)


def _data(valor: Any, campo: str) -> date:
    if isinstance(valor, date):
        return valor
    v = str(valor or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    raise ErroValidacao(f"Data inválida em {campo}: {valor!r} (use AAAA-MM-DD ou DD/MM/AAAA).")


def _competencia(valor: Any) -> date:
    if isinstance(valor, date):
        return valor.replace(day=1)
    v = str(valor or "").strip()
    for fmt in ("%Y-%m", "%m/%Y"):
        try:
            return datetime.strptime(v, fmt).date().replace(day=1)
        except ValueError:
            continue
    return _data(v, "competencia").replace(day=1)


def proximo_numero_sp(s: Session) -> str:
    """Número SP sequencial via sequence dedicada (migração 001)."""
    n = s.execute(text("SELECT nextval('seq_numero_sp')")).scalar_one()
    return f"SP{n:06d}"


# ---------------------------------------------------------------------------
# Criação (lançamento dirigido)
# ---------------------------------------------------------------------------
def criar_titulo(s: Session, dados: dict[str, Any], usuario: Usuario) -> Titulo:
    # ---- tipo e regras
    try:
        tipo = TipoTitulo(str(dados.get("tipo") or "").strip())
    except ValueError:
        raise ErroValidacao(f"Tipo de título inválido: {dados.get('tipo')!r}")
    regras = regras_de(tipo)
    criticas_transicao: list[str] = []

    # ---- fornecedor
    forn = s.get(Fornecedor, int(dados.get("fornecedor_id") or 0))
    if forn is None or not forn.ativo:
        raise ErroValidacao("Fornecedor inexistente ou inativo.")

    # ---- categoria + matriz de documento hábil (A10)
    cat = s.get(Categoria, int(dados.get("categoria_id") or 0))
    if cat is None or not cat.ativo:
        raise ErroValidacao("Categoria inexistente ou inativa.")
    permitidos = [t.value if hasattr(t, "value") else str(t) for t in (cat.tipos_permitidos or [])]
    if permitidos and tipo.value not in permitidos:
        raise ErroValidacao(
            f"A categoria {cat.codigo} não aceita títulos do tipo {regras.rotulo} "
            f"(matriz de documento hábil).")

    # ---- descrição e valores
    descricao = (dados.get("descricao") or "").strip()
    if len(descricao) < 5:
        raise ErroValidacao("Descrição obrigatória (mínimo 5 caracteres).")
    valor_bruto = _dec(dados.get("valor_bruto"), "valor_bruto")
    if valor_bruto <= 0:
        raise ErroValidacao("Valor bruto deve ser maior que zero.")

    # ---- retenções (manuais nesta fase; motor automático vem na fase fiscal)
    retencoes_in = dados.get("retencoes") or []
    total_ret = Decimal("0.00")
    retencoes_obj: list[Retencao] = []
    for i, r in enumerate(retencoes_in, start=1):
        try:
            tipo_ret = TipoRetencao(str(r.get("tipo") or "").strip().upper())
        except ValueError:
            raise ErroValidacao(f"Retenção {i}: tipo inválido {r.get('tipo')!r} (INSS/ISS/IRRF/PCC).")
        base = _dec(r.get("base_calculo", valor_bruto), f"retenções[{i}].base")
        aliq = _dec(r.get("aliquota"), f"retenções[{i}].aliquota")
        val = _dec(r.get("valor"), f"retenções[{i}].valor")
        esperado = (base * aliq / 100).quantize(_CENT)
        if abs(val - esperado) > Decimal("0.02"):
            raise ErroValidacao(
                f"Retenção {i} ({tipo_ret.value}): valor {val} não confere com "
                f"base {base} × {aliq}% = {esperado}.")
        total_ret += val
        retencoes_obj.append(Retencao(tipo=tipo_ret, base_calculo=base, aliquota=aliq,
                                      valor=val, cno_obra=(r.get("cno_obra") or None)))
    valor_liquido = (valor_bruto - total_ret).quantize(_CENT)
    if valor_liquido <= 0:
        raise ErroValidacao("Valor líquido (bruto − retenções) deve ser maior que zero.")

    # ---- vínculos exigidos pela matriz do tipo
    pedido_id = dados.get("pedido_id") or None
    contrato_id = dados.get("contrato_id") or None
    doc_fiscal_id = dados.get("documento_fiscal_id") or None

    if regras.exige_pedido and not pedido_id:
        if modo_transicao():
            criticas_transicao.append("B1: título do tipo exige pedido vinculado (pendência de transição).")
        else:
            raise ErroValidacao(f"{regras.rotulo}: vínculo com pedido é obrigatório.")
    if regras.exige_contrato and not contrato_id:
        # o contrato de empreita do próprio ERP satisfaz a exigência
        if not dados.get("contrato_servico_id"):
            raise ErroValidacao(
                f"{regras.rotulo}: vínculo com contrato é obrigatório. Cadastre o "
                f"contrato de empreita e gere o título pela medição.")
    if regras.exige_doc_fiscal and not doc_fiscal_id:
        if modo_transicao():
            criticas_transicao.append("A10: tipo exige documento fiscal vinculado (pendência de transição).")
        else:
            raise ErroValidacao(f"{regras.rotulo}: vínculo com documento fiscal é obrigatório.")

    justificativa = (dados.get("justificativa_excecao") or "").strip() or None
    if regras.exige_justificativa and not justificativa:
        raise ErroValidacao(f"{regras.rotulo}: justificativa é obrigatória.")

    # ---- forma de pagamento e conta homologada (princípio 3 / C2)
    try:
        forma = FormaPagamento(str(dados.get("forma_pagamento") or "").strip().upper())
    except ValueError:
        raise ErroValidacao(f"Forma de pagamento inválida: {dados.get('forma_pagamento')!r}")

    conta_id = dados.get("fornecedor_conta_id") or None
    if forma in (FormaPagamento.PIX, FormaPagamento.TED):
        if regras.exige_conta_fornecedor:
            if not conta_id:
                raise ErroValidacao(
                    "Pagamento por PIX/TED exige seleção de conta HOMOLOGADA do fornecedor "
                    "(dados bancários vivem no cadastro, nunca no lançamento).")
            conta = s.get(FornecedorConta, int(conta_id))
            if conta is None or conta.fornecedor_id != forn.id:
                raise ErroValidacao("Conta selecionada não pertence ao fornecedor do título.")
            if conta.status != StatusConta.HOMOLOGADA:
                raise ErroValidacao(f"Conta selecionada não está HOMOLOGADA (status: {conta.status.value}).")
            if conta.forma != forma:
                raise ErroValidacao(f"Conta selecionada é {conta.forma.value}, não {forma.value}.")

    # ---- parcelas
    parcelas_in = dados.get("parcelas") or []
    if not parcelas_in:
        raise ErroValidacao("Informe ao menos 1 parcela (vencimento + valor).")
    parcelas_obj: list[Parcela] = []
    soma_parc = Decimal("0.00")
    for i, p in enumerate(parcelas_in, start=1):
        venc = _data(p.get("vencimento"), f"parcelas[{i}].vencimento")
        val = _dec(p.get("valor"), f"parcelas[{i}].valor")
        if val <= 0:
            raise ErroValidacao(f"Parcela {i}: valor deve ser maior que zero.")
        linha = (p.get("linha_digitavel") or "").strip() or None
        codigo_barras = None
        if forma == FormaPagamento.BOLETO:
            if not linha:
                raise ErroValidacao(f"Parcela {i}: forma BOLETO exige a linha digitável.")
            res = validar_linha_digitavel(linha, referencia=venc)
            if not res.valido:
                raise ErroValidacao(f"Parcela {i}: boleto inválido — {res.mensagem}")
            if res.valor is not None and abs(res.valor - val) > Decimal("0.01"):
                raise ErroValidacao(
                    f"Parcela {i}: valor do boleto (R$ {res.valor}) difere do valor "
                    f"informado (R$ {val}) — divergência B9.")
            if res.vencimento is not None and res.vencimento != venc:
                raise ErroValidacao(
                    f"Parcela {i}: vencimento do boleto ({res.vencimento:%d/%m/%Y}) difere "
                    f"do informado ({venc:%d/%m/%Y}) — divergência B8.")
            linha = "".join(ch for ch in linha if ch.isdigit())
            codigo_barras = res.codigo_barras
            # C7(b): duplicidade de linha digitável em parcela ativa
            dup = s.scalars(select(Parcela).where(
                Parcela.linha_digitavel == linha,
                Parcela.status != StatusParcela.CANCELADA)).first()
            if dup is not None:
                raise ErroValidacao(
                    f"Parcela {i}: esta linha digitável já está lançada no título "
                    f"id {dup.titulo_id} — duplicidade bloqueada (C7).")
        soma_parc += val
        parcelas_obj.append(Parcela(numero=i, vencimento=venc, valor=val,
                                    linha_digitavel=linha, codigo_barras=codigo_barras))
    if abs(soma_parc - valor_liquido) > Decimal("0.01"):
        raise ErroValidacao(
            f"Soma das parcelas (R$ {soma_parc}) ≠ valor líquido (R$ {valor_liquido}).")

    # ---- rateios por obra
    rateios_in = dados.get("rateios") or []
    if not rateios_in:
        raise ErroValidacao(
            "Informe o rateio por obra — todo gasto precisa de centro de custo.")
    if not rateios_in:
        raise ErroValidacao("Informe ao menos 1 rateio (obra + valor).")
    rateios_obj: list[Rateio] = []
    soma_rat = Decimal("0.00")
    for i, r in enumerate(rateios_in, start=1):
        obra = s.get(Obra, int(r.get("obra_id") or 0))
        if obra is None or obra.status != "ATIVA":
            raise ErroValidacao(f"Rateio {i}: obra inexistente ou não ATIVA.")
        val = _dec(r.get("valor"), f"rateios[{i}].valor")
        if val <= 0:
            raise ErroValidacao(f"Rateio {i}: valor deve ser maior que zero.")
        soma_rat += val
        pct = (val / valor_liquido * 100).quantize(Decimal("0.0001"))
        # conta própria da linha: uma nota pode ter material e serviço juntos
        cat_linha = None
        if r.get("categoria_id"):
            cat_linha = s.get(Categoria, int(r["categoria_id"]))
            if cat_linha is None or not cat_linha.ativo:
                raise ErroValidacao(f"Rateio {i}: conta do plano inexistente ou aposentada.")
        rateios_obj.append(Rateio(obra_id=obra.id, valor=val, percentual=pct,
                                  categoria_id=cat_linha.id if cat_linha else None,
                                  descricao=(r.get("descricao") or "").strip() or None))
    if abs(soma_rat - valor_liquido) > Decimal("0.01"):
        raise ErroValidacao(
            f"Soma dos rateios (R$ {soma_rat}) ≠ valor líquido (R$ {valor_liquido}).")

    # ---- C7(d): duplicidade credor + valor + 1º vencimento em janela de 30 dias
    venc1 = parcelas_obj[0].vencimento
    dup_tit = s.execute(text(
        "SELECT t.numero_sp FROM titulos t JOIN parcelas p ON p.titulo_id = t.id AND p.numero = 1 "
        "WHERE t.fornecedor_id = :f AND t.valor_liquido = :v "
        "AND t.status NOT IN ('CANCELADO','ESTORNADO','DEVOLVIDO') "
        "AND abs(p.vencimento - CAST(:d AS date)) <= 30 LIMIT 1"),
        {"f": forn.id, "v": valor_liquido, "d": venc1.isoformat()}).first()
    possivel_dup = dup_tit[0] if dup_tit else None

    # ---- montagem
    titulo = Titulo(
        numero_sp=proximo_numero_sp(s),
        tipo=tipo, fornecedor_id=forn.id, descricao=descricao,
        valor_bruto=valor_bruto, valor_retencoes=total_ret, valor_liquido=valor_liquido,
        competencia=_competencia(dados.get("competencia")),
        data_emissao_doc=_data(dados["data_emissao_doc"], "data_emissao_doc")
            if dados.get("data_emissao_doc") else None,
        categoria_id=cat.id, pedido_id=pedido_id, contrato_id=contrato_id,
        documento_fiscal_id=doc_fiscal_id, forma_pagamento=forma,
        fornecedor_conta_id=conta_id,
        dedutivel=bool(dados.get("dedutivel", regras.dedutivel_padrao and cat.dedutivel_padrao)),
        justificativa_excecao=justificativa,
        solicitante_id=usuario.id,
        origem=str(dados.get("origem") or "SISTEMA"),
    )
    titulo.parcelas = parcelas_obj
    titulo.rateios = rateios_obj
    titulo.retencoes = retencoes_obj
    s.add(titulo)
    s.flush()

    # interessados adicionais: recebem os avisos junto com quem lançou
    from app.apps.erp.db.models.financeiro import TituloInteressado
    interessados = []
    for uid in (dados.get("interessados") or []):
        try:
            uid = int(uid)
        except (TypeError, ValueError):
            continue
        if uid == usuario.id:
            continue                       # o solicitante já é destinatário
        pessoa = s.get(Usuario, uid)
        if pessoa is None or not pessoa.ativo:
            continue
        s.add(TituloInteressado(titulo_id=titulo.id, usuario_id=uid,
                                adicionado_por=usuario.id))
        interessados.append(pessoa.nome)
    if interessados:
        s.flush()

    registrar_evento(s, "titulo", titulo.id, "CRIADO", {
        "interessados": interessados,
        "numero_sp": titulo.numero_sp, "tipo": tipo.value,
        "fornecedor": forn.razao_social, "valor_liquido": str(valor_liquido),
        "parcelas": len(parcelas_obj), "criticas_transicao": criticas_transicao,
        "possivel_duplicidade_de": possivel_dup,
    }, usuario.id)

    # análise automática imediata
    from app.apps.erp.core.titulos.analise import analisar_titulo
    analisar_titulo(s, titulo, criticas_extra=criticas_transicao,
                    possivel_duplicidade=possivel_dup)

    # dupla confirmação: lançamento de quem não é instância final trava
    # aguardando o aval de um supervisor/gestor/diretor
    from app.apps.erp.core.titulos.aval import marcar_para_aval
    if marcar_para_aval(s, titulo, usuario):
        registrar_evento(s, "titulo", titulo.id, "AGUARDANDO_AVAL", {
            "numero_sp": titulo.numero_sp,
            "motivo": f"lançado por {usuario.nome} ({usuario.perfil.value}) — "
                      f"exige confirmação de segunda pessoa"}, usuario.id)
    return titulo


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------
def obter(s: Session, titulo_id: int) -> Titulo:
    t = s.get(Titulo, titulo_id, options=[
        selectinload(Titulo.parcelas), selectinload(Titulo.rateios),
        selectinload(Titulo.retencoes), selectinload(Titulo.fornecedor),
        selectinload(Titulo.categoria)])
    if t is None:
        raise ErroValidacao(f"Título {titulo_id} não encontrado.")
    return t


def listar(s: Session, *, status: Optional[str] = None, fornecedor_id: Optional[int] = None,
           competencia: Optional[date] = None, busca: str = "",
           limite: int = 500, usuario: Optional[Usuario] = None) -> list[Titulo]:
    stmt = (select(Titulo)
            .options(selectinload(Titulo.parcelas), selectinload(Titulo.fornecedor),
                     selectinload(Titulo.categoria),
                     selectinload(Titulo.rateios).selectinload(Rateio.obra))
            .order_by(Titulo.id.desc()).limit(limite))
    if status:
        stmt = stmt.where(Titulo.status == StatusTitulo(status))
    if fornecedor_id:
        stmt = stmt.where(Titulo.fornecedor_id == fornecedor_id)
    if competencia:
        stmt = stmt.where(Titulo.competencia == competencia.replace(day=1))
    busca = (busca or "").strip()
    if busca:
        stmt = stmt.where(Titulo.descricao.ilike(f"%{busca}%") | Titulo.numero_sp.ilike(f"%{busca}%"))
    if usuario is not None:
        from app.apps.erp.core.auth.permissoes import aplicar_escopo
        stmt = aplicar_escopo(stmt, s, usuario)
    return list(s.scalars(stmt).all())


# ---------------------------------------------------------------------------
# Transições de status
# ---------------------------------------------------------------------------
def _exigir_status(t: Titulo, *permitidos: StatusTitulo) -> None:
    if t.status not in permitidos:
        nomes = "/".join(p.value for p in permitidos)
        raise ErroValidacao(f"Operação exige status {nomes}; título está {t.status.value}.")


def aprovar(s: Session, titulo_id: int, usuario: Usuario) -> Titulo:
    """Aprova título respeitando alçadas (E1) e segregação de funções (F2)."""
    t = obter(s, titulo_id)
    if t.status == StatusTitulo.AGUARDANDO_AVAL:
        raise ErroValidacao(
            "Este título ainda aguarda o aval da segunda pessoa (supervisor, gestor "
            "ou diretor financeiro). Só depois vai para liberação de pagamento.")
    if t.exige_aval and t.avalizado_em is None:
        raise ErroValidacao("Título sem aval registrado — não pode ser liberado.")
    _exigir_status(t, StatusTitulo.AGUARDANDO_APROVACAO, StatusTitulo.EM_ANALISE)
    if usuario.id == t.solicitante_id:
        raise ErroPermissao("Segregação de funções: quem lança não aprova o próprio título (F2).")

    linhas = s.scalars(select(Alcada).where(Alcada.ativo.is_(True))).all()
    aplicaveis = [a for a in linhas
                  if (a.categoria_id in (None, t.categoria_id))
                  and (a.obra_id is None or a.obra_id in {r.obra_id for r in t.rateios})
                  and Decimal(a.valor_max) >= Decimal(t.valor_liquido)]
    if aplicaveis:
        exigido = min(_RANK[a.perfil_minimo] for a in aplicaveis)
        if _RANK[usuario.perfil] < exigido:
            raise ErroPermissao(
                f"Alçada insuficiente: valor R$ {t.valor_liquido} exige perfil "
                f"{[p.value for p, r in _RANK.items() if r == exigido][0]} ou superior.")
    else:
        # Sem matriz cadastrada: só FINANCEIRO/ADMIN aprovam (padrão conservador)
        if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
            raise ErroPermissao("Sem matriz de alçadas cadastrada: aprovação restrita a FINANCEIRO/ADMIN.")

    t.status = StatusTitulo.APROVADO
    t.aprovador_id = usuario.id
    t.aprovado_em = datetime.now(timezone.utc)
    registrar_evento(s, "titulo", t.id, "APROVADO",
                     {"numero_sp": t.numero_sp, "por": usuario.email}, usuario.id)
    return t


def devolver(s: Session, titulo_id: int, motivo: str, usuario: Usuario) -> Titulo:
    t = obter(s, titulo_id)
    _exigir_status(t, StatusTitulo.EM_ANALISE, StatusTitulo.AGUARDANDO_APROVACAO,
                   StatusTitulo.BLOQUEADO)
    motivo = (motivo or "").strip()
    if len(motivo) < 10:
        raise ErroValidacao("Devolução exige motivo objetivo (mínimo 10 caracteres) — F4.")
    t.status = StatusTitulo.DEVOLVIDO
    registrar_evento(s, "titulo", t.id, "DEVOLVIDO",
                     {"numero_sp": t.numero_sp, "motivo": motivo}, usuario.id)
    return t


def cancelar(s: Session, titulo_id: int, motivo: str, usuario: Usuario) -> Titulo:
    t = obter(s, titulo_id)
    if any(p.status == StatusParcela.PAGA for p in t.parcelas):
        raise ErroValidacao("Título com parcela paga não se cancela — use estorno.")
    _exigir_status(t, StatusTitulo.RASCUNHO, StatusTitulo.EM_ANALISE,
                   StatusTitulo.DEVOLVIDO, StatusTitulo.AGUARDANDO_APROVACAO,
                   StatusTitulo.APROVADO, StatusTitulo.BLOQUEADO)
    motivo = (motivo or "").strip()
    if len(motivo) < 5:
        raise ErroValidacao("Informe o motivo do cancelamento.")
    t.status = StatusTitulo.CANCELADO
    for p in t.parcelas:
        p.status = StatusParcela.CANCELADA
    registrar_evento(s, "titulo", t.id, "CANCELADO",
                     {"numero_sp": t.numero_sp, "motivo": motivo}, usuario.id)
    return t


def estornar(s: Session, titulo_id: int, motivo: str, usuario: Usuario) -> Titulo:
    """Imutabilidade contábil: título pago não se edita — estorna-se.
    Marca o original ESTORNADO e o vincula; o lançamento corrigido é criado
    à parte pelo usuário."""
    t = obter(s, titulo_id)
    _exigir_status(t, StatusTitulo.PAGO, StatusTitulo.PAGO_PARCIAL, StatusTitulo.APROVADO)
    if usuario.perfil not in (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO):
        raise ErroPermissao("Estorno restrito a FINANCEIRO/ADMIN.")
    motivo = (motivo or "").strip()
    if len(motivo) < 10:
        raise ErroValidacao("Estorno exige motivo detalhado (mínimo 10 caracteres).")
    t.status = StatusTitulo.ESTORNADO
    registrar_evento(s, "titulo", t.id, "ESTORNADO",
                     {"numero_sp": t.numero_sp, "motivo": motivo}, usuario.id)
    return t
