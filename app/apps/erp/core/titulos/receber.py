# ============================================================================
# ERP — core/titulos/receber.py
# Títulos A RECEBER (medições de obra) e MOVIMENTAÇÕES entre contas próprias.
#
# Decisões de projeto:
#   - Receber reaproveita a mesma tabela de títulos, com espécie RECEBER. Assim
#     parcelas, rateio por obra, conciliação e relatórios funcionam de graça;
#     um universo paralelo duplicaria regra e manutenção.
#   - No LANÇAMENTO da medição não se exige nota fiscal (a nota é emitida
#     depois, quando o cliente aprova). As notas entram na BAIXA — e são
#     VÁRIAS, porque um recebimento costuma quitar mais de uma nota.
#   - Movimentação entre contas próprias é lançamento simples: valor, data e as
#     duas contas. Nada de credor, categoria obrigatória ou rateio — não é
#     despesa, é dinheiro mudando de lugar.
# ============================================================================
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Categoria, ContaBancaria, Contrato, Fornecedor, Obra, TipoTitulo, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    EspecieTitulo, Movimentacao, Parcela, Rateio, StatusParcela, StatusTitulo, Titulo,
)

_CENT = Decimal("0.01")


def _dec(v: Any, campo: str) -> Decimal:
    try:
        s = str(v or "").strip().replace("R$", "").strip()
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return Decimal(s).quantize(_CENT)
    except (InvalidOperation, TypeError):
        raise ErroValidacao(f"Valor inválido em {campo}: {v!r}")


def _data(v: Any, campo: str) -> date:
    if isinstance(v, date):
        return v
    s = str(v or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    raise ErroValidacao(f"Data inválida em {campo}: {v!r}")


# ---------------------------------------------------------------------------
# Medição / título a receber
# ---------------------------------------------------------------------------
def criar_medicao(s: Session, dados: dict[str, Any], usuario: Usuario) -> Titulo:
    """Lança uma medição a receber. Exige o mínimo que identifica a medição:
    obra (ou contrato), número, período e valor."""
    obra_id = dados.get("obra_id")
    contrato_id = dados.get("contrato_id")
    obra: Optional[Obra] = s.get(Obra, int(obra_id)) if obra_id else None
    contrato: Optional[Contrato] = s.get(Contrato, int(contrato_id)) if contrato_id else None
    if contrato is not None and obra is None and contrato.obra_id:
        obra = s.get(Obra, contrato.obra_id)          # contrato traz a obra junto
    if obra is None:
        raise ErroValidacao("Informe a obra da medição (ou um contrato vinculado a uma obra).")

    cliente_id = dados.get("cliente_id")
    cliente = s.get(Fornecedor, int(cliente_id)) if cliente_id else None
    if cliente is None:
        raise ErroValidacao("Informe o cliente/órgão que vai pagar a medição.")
    if not cliente.e_cliente:
        cliente.e_cliente = True                      # promove a cliente no cadastro

    numero_medicao = (dados.get("numero_medicao") or "").strip()
    if not numero_medicao:
        raise ErroValidacao("Informe o número da medição.")
    periodo_inicio = _data(dados["periodo_inicio"], "período inicial") if dados.get("periodo_inicio") else None
    periodo_fim = _data(dados["periodo_fim"], "período final") if dados.get("periodo_fim") else None
    if periodo_inicio and periodo_fim and periodo_fim < periodo_inicio:
        raise ErroValidacao("O período final é anterior ao inicial.")

    # duplicidade: mesma medição da mesma obra
    dup = s.scalars(select(Titulo).where(
        Titulo.especie == EspecieTitulo.RECEBER,
        Titulo.numero_medicao == numero_medicao,
        Titulo.status.not_in([StatusTitulo.CANCELADO, StatusTitulo.ESTORNADO])
    ).join(Rateio, Rateio.titulo_id == Titulo.id).where(Rateio.obra_id == obra.id)).first()
    if dup is not None:
        raise ErroValidacao(
            f"A medição {numero_medicao} da obra {obra.codigo} já está lançada em {dup.numero_sp}.")

    valor = _dec(dados.get("valor_bruto"), "valor da medição")
    if valor <= 0:
        raise ErroValidacao("Valor da medição deve ser maior que zero.")

    # retenções do contrato (ISS, INSS, caução) reduzem o líquido a receber
    retencoes_in = dados.get("retencoes") or []
    total_ret = Decimal("0.00")
    from app.apps.erp.db.models.financeiro import Retencao, TipoRetencao
    retencoes = []
    for i, r in enumerate(retencoes_in, start=1):
        try:
            tipo = TipoRetencao((r.get("tipo") or "").strip().upper())
        except ValueError:
            raise ErroValidacao(f"Retenção {i}: tipo inválido ({r.get('tipo')!r}).")
        v = _dec(r.get("valor"), f"retenção {i}")
        base = _dec(r.get("base_calculo") or valor, f"base da retenção {i}")
        aliq = _dec(r.get("aliquota") or 0, f"alíquota da retenção {i}")
        total_ret += v
        retencoes.append(Retencao(tipo=tipo, base_calculo=base, aliquota=aliq, valor=v))
    liquido = (valor - total_ret).quantize(_CENT)
    if liquido <= 0:
        raise ErroValidacao("Líquido a receber ficou zero ou negativo.")

    parcelas_in = dados.get("parcelas") or []
    if not parcelas_in:
        venc = _data(dados.get("vencimento") or date.today().isoformat(), "vencimento")
        parcelas_in = [{"vencimento": venc, "valor": str(liquido)}]
    parcelas, soma = [], Decimal("0.00")
    for i, p in enumerate(parcelas_in, start=1):
        v = _dec(p.get("valor"), f"parcela {i}")
        parcelas.append(Parcela(numero=i, vencimento=_data(p.get("vencimento"), f"vencimento {i}"),
                                valor=v))
        soma += v
    if abs(soma - liquido) > Decimal("0.01"):
        raise ErroValidacao(f"Soma das parcelas (R$ {soma}) ≠ líquido a receber (R$ {liquido}).")

    categoria = None
    if dados.get("categoria_id"):
        categoria = s.get(Categoria, int(dados["categoria_id"]))
    if categoria is None:
        categoria = s.scalars(select(Categoria).where(Categoria.codigo == "1.1.01")).first()
    if categoria is None:
        raise ErroValidacao("Instale o plano financeiro (conta 1.1.01 — receita de obras).")

    from app.apps.erp.core.titulos.service import proximo_numero_sp
    competencia = (_data(dados["competencia"] + "-01", "competência")
                   if dados.get("competencia") and len(str(dados["competencia"])) == 7
                   else (periodo_fim or date.today()).replace(day=1))

    titulo = Titulo(
        numero_sp=proximo_numero_sp(s), especie=EspecieTitulo.RECEBER,
        tipo=TipoTitulo.T2_SERVICO_NFSE,
        fornecedor_id=cliente.id, cliente_id=cliente.id,
        descricao=(dados.get("descricao") or
                   f"Medição {numero_medicao} — {obra.codigo}").strip(),
        valor_bruto=valor, valor_retencoes=total_ret, valor_liquido=liquido,
        competencia=competencia, categoria_id=categoria.id,
        contrato_id=contrato.id if contrato else None,
        numero_medicao=numero_medicao,
        periodo_inicio=periodo_inicio, periodo_fim=periodo_fim,
        notas_fiscais=[n.strip() for n in (dados.get("notas_fiscais") or []) if str(n).strip()],
        forma_pagamento=__import__("app.apps.erp.db.models.cadastros",
                                   fromlist=["FormaPagamento"]).FormaPagamento.TED,
        status=StatusTitulo.APROVADO,      # medição lançada já fica aguardando o recebimento
        solicitante_id=usuario.id, origem=str(dados.get("origem") or "SISTEMA"),
    )
    titulo.parcelas = parcelas
    titulo.rateios = [Rateio(obra_id=obra.id, valor=liquido, percentual=Decimal("100.0000"))]
    titulo.retencoes = retencoes
    s.add(titulo)
    s.flush()
    registrar_evento(s, "titulo", titulo.id, "MEDICAO_LANCADA", {
        "numero_sp": titulo.numero_sp, "medicao": numero_medicao, "obra": obra.codigo,
        "cliente": cliente.razao_social, "valor": str(valor), "liquido": str(liquido),
        "periodo": f"{periodo_inicio or '?'} a {periodo_fim or '?'}"}, usuario.id)
    return titulo


def registrar_notas(s: Session, titulo_id: int, notas: list[str], usuario: Usuario) -> Titulo:
    """Anexa as notas fiscais emitidas contra a medição. São várias porque um
    recebimento costuma quitar mais de uma nota."""
    t = s.get(Titulo, titulo_id)
    if t is None or t.especie != EspecieTitulo.RECEBER:
        raise ErroValidacao("Título a receber não encontrado.")
    limpas = [str(n).strip() for n in notas if str(n).strip()]
    if not limpas:
        raise ErroValidacao("Informe ao menos um número de nota.")
    t.notas_fiscais = sorted(set(list(t.notas_fiscais or []) + limpas))
    registrar_evento(s, "titulo", t.id, "NOTAS_REGISTRADAS",
                     {"notas": t.notas_fiscais}, usuario.id)
    return t


def registrar_recebimento(s: Session, *, parcela_id: int, conta_bancaria_id: int,
                          data_recebimento: date, valor: Any = None,
                          notas_fiscais: Optional[list[str]] = None,
                          usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Baixa da medição. Aceita as notas fiscais no ato — é aqui que elas
    existem, não no lançamento."""
    from app.apps.erp.db.models.financeiro import Pagamento
    parcela = s.get(Parcela, parcela_id, options=[selectinload(Parcela.titulo)])
    if parcela is None:
        raise ErroValidacao("Parcela não encontrada.")
    t = parcela.titulo
    if t.especie != EspecieTitulo.RECEBER:
        raise ErroValidacao("Esta parcela é de um título a pagar — use a baixa de pagamentos.")
    if parcela.status == StatusParcela.PAGA:
        raise ErroValidacao(f"A parcela {parcela.numero} de {t.numero_sp} já foi recebida.")
    conta = s.get(ContaBancaria, conta_bancaria_id)
    if conta is None or not conta.ativo:
        raise ErroValidacao("Conta bancária inexistente ou inativa.")

    v = Decimal(parcela.valor) if valor in (None, "") else _dec(valor, "valor recebido")
    diferenca = (v - Decimal(parcela.valor)).quantize(_CENT)

    pg = Pagamento(parcela_id=parcela.id, conta_bancaria_id=conta.id,
                   data_pagamento=data_recebimento, valor_pago=v,
                   meio=t.forma_pagamento,
                   executado_por=(usuario.id if usuario else None))
    s.add(pg)
    parcela.status = StatusParcela.PAGA
    if notas_fiscais:
        t.notas_fiscais = sorted(set(list(t.notas_fiscais or []) +
                                     [str(n).strip() for n in notas_fiscais if str(n).strip()]))
    abertas = [p for p in t.parcelas if p.id != parcela.id
               and p.status in (StatusParcela.ABERTA, StatusParcela.AGENDADA)]
    t.status = StatusTitulo.PAGO_PARCIAL if abertas else StatusTitulo.PAGO
    s.flush()
    registrar_evento(s, "titulo", t.id, "RECEBIMENTO", {
        "parcela": parcela.numero, "valor": str(v), "data": data_recebimento.isoformat(),
        "diferenca": str(diferenca), "notas": t.notas_fiscais},
        usuario.id if usuario else None)
    return {"pagamento_id": pg.id, "numero_sp": t.numero_sp, "valor": float(v),
            "diferenca": float(diferenca), "status_titulo": t.status.value,
            "notas_fiscais": list(t.notas_fiscais or [])}


def listar_receber(s: Session, filtros: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    f = filtros or {}
    stmt = (select(Titulo).where(Titulo.especie == EspecieTitulo.RECEBER)
            .options(selectinload(Titulo.parcelas), selectinload(Titulo.fornecedor),
                     selectinload(Titulo.rateios).selectinload(Rateio.obra))
            .order_by(Titulo.id.desc()).limit(int(f.get("limite") or 500)))
    if f.get("obra_id"):
        stmt = stmt.join(Rateio, Rateio.titulo_id == Titulo.id).where(
            Rateio.obra_id == int(f["obra_id"]))
    if f.get("status"):
        stmt = stmt.where(Titulo.status == StatusTitulo(f["status"]))
    hoje = date.today()
    saida = []
    for t in s.scalars(stmt).all():
        venc = min((p.vencimento for p in t.parcelas), default=None)
        recebido = sum(float(p.valor) for p in t.parcelas if p.status == StatusParcela.PAGA)
        saida.append({
            "id": t.id, "numero_sp": t.numero_sp, "cliente": t.fornecedor.razao_social,
            "obra": " + ".join(sorted({r.obra.codigo for r in t.rateios if r.obra})),
            "medicao": t.numero_medicao,
            "periodo": (f"{t.periodo_inicio:%d/%m} a {t.periodo_fim:%d/%m/%Y}"
                        if t.periodo_inicio and t.periodo_fim else ""),
            "descricao": t.descricao, "valor_bruto": float(t.valor_bruto),
            "retencoes": float(t.valor_retencoes), "valor": float(t.valor_liquido),
            "recebido": round(recebido, 2),
            "vencimento": venc.isoformat() if venc else None,
            "atrasado": bool(venc and venc < hoje and t.status != StatusTitulo.PAGO),
            "notas_fiscais": list(t.notas_fiscais or []),
            "status": t.status.value,
            "parcelas": [{"parcela_id": p.id, "numero": p.numero,
                          "vencimento": p.vencimento.isoformat(), "valor": float(p.valor),
                          "status": p.status.value} for p in t.parcelas],
        })
    return saida


# ---------------------------------------------------------------------------
# Movimentação entre contas próprias — simples de propósito
# ---------------------------------------------------------------------------
TIPOS_MOVIMENTO = {
    "TRANSFERENCIA": ("Transferência entre contas", "9.1.01", True, True),
    "APLICACAO": ("Aplicação financeira", "9.2.01", True, False),
    "RESGATE": ("Resgate de aplicação", "9.2.02", False, True),
    "APORTE_RECEBIDO": ("Aporte recebido", "9.3.01", False, True),
    "APORTE_CONCEDIDO": ("Aporte concedido", "9.3.03", True, False),
    "EMPRESTIMO_CAPTADO": ("Empréstimo captado", "9.4.01", False, True),
    "TARIFA": ("Tarifa bancária", "6.2.01", True, False),
    "RENDIMENTO": ("Rendimento de aplicação", "1.3.01", False, True),
}


def criar_movimentacao(s: Session, dados: dict[str, Any], usuario: Usuario) -> Movimentacao:
    """Lançamento enxuto: tipo, valor, data e as contas. Sem credor, sem
    rateio obrigatório — não é despesa, é dinheiro mudando de lugar."""
    tipo = (dados.get("tipo") or "TRANSFERENCIA").strip().upper()
    if tipo not in TIPOS_MOVIMENTO:
        raise ErroValidacao(f"Tipo de movimentação inválido: {tipo}")
    rotulo, codigo_conta, exige_origem, exige_destino = TIPOS_MOVIMENTO[tipo]

    valor = _dec(dados.get("valor"), "valor")
    if valor <= 0:
        raise ErroValidacao("Valor deve ser maior que zero.")
    data_mov = _data(dados.get("data_movimento") or date.today().isoformat(), "data")

    origem_id = dados.get("conta_origem_id") or None
    destino_id = dados.get("conta_destino_id") or None
    if exige_origem and not origem_id:
        raise ErroValidacao(f"{rotulo}: informe a conta de saída.")
    if exige_destino and not destino_id:
        raise ErroValidacao(f"{rotulo}: informe a conta de entrada.")
    if origem_id and destino_id and int(origem_id) == int(destino_id):
        raise ErroValidacao("Conta de origem e destino são a mesma.")
    for cid in (origem_id, destino_id):
        if cid and s.get(ContaBancaria, int(cid)) is None:
            raise ErroValidacao("Conta bancária inexistente.")

    categoria = None
    if dados.get("categoria_id"):
        categoria = s.get(Categoria, int(dados["categoria_id"]))
    if categoria is None:
        categoria = s.scalars(select(Categoria).where(Categoria.codigo == codigo_conta)).first()

    mov = Movimentacao(
        tipo=tipo, conta_origem_id=origem_id, conta_destino_id=destino_id,
        valor=valor, data_movimento=data_mov,
        descricao=(dados.get("descricao") or rotulo).strip(),
        categoria_id=categoria.id if categoria else None,
        obra_id=dados.get("obra_id") or None,
        extrato_saida_id=dados.get("extrato_saida_id") or None,
        extrato_entrada_id=dados.get("extrato_entrada_id") or None,
        criado_por=usuario.id)
    s.add(mov)
    s.flush()
    registrar_evento(s, "movimentacao", mov.id, "CRIADA", {
        "tipo": tipo, "valor": str(valor), "data": data_mov.isoformat(),
        "origem": origem_id, "destino": destino_id,
        "conta_plano": categoria.codigo if categoria else None}, usuario.id)
    return mov


def listar_movimentacoes(s: Session, limite: int = 300) -> list[dict[str, Any]]:
    linhas = s.scalars(select(Movimentacao)
                       .order_by(Movimentacao.data_movimento.desc(),
                                 Movimentacao.id.desc()).limit(limite)).all()
    contas = {c.id: c.descricao for c in s.scalars(select(ContaBancaria)).all()}
    categorias = {c.id: f"{c.codigo} · {c.descricao}"
                  for c in s.scalars(select(Categoria)).all()}
    return [{
        "id": m.id, "tipo": m.tipo, "rotulo": TIPOS_MOVIMENTO.get(m.tipo, (m.tipo,))[0],
        "valor": float(m.valor), "data": m.data_movimento.isoformat(),
        "origem": contas.get(m.conta_origem_id), "destino": contas.get(m.conta_destino_id),
        "descricao": m.descricao, "conta_plano": categorias.get(m.categoria_id),
        "conciliada": bool(m.extrato_saida_id or m.extrato_entrada_id),
    } for m in linhas]
