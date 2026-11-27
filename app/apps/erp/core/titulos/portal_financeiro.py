# ============================================================================
# ERP — core/titulos/portal_financeiro.py
# O FINANCEIRO NO PORTAL DA OBRA — o que a tela enxuta lê.
#
# ⚑ O PEDIDO (27/11/2026): *"Pedir Pagamento, Fundo Fixo, Medição de Empreita e
# o Cadastro dela, Cancelar um Pedido e uma forma de listar e visualizar todos
# esses lançamentos, devem estar disponíveis no portal. O Administrativo de Obra
# fará esses lançamentos."* — e, no dia anterior: *"a gente não vai deixar ele
# ativo ainda, vamos deixar ele ativo somente para o administrador."*
#
# ⚑ O QUE ESTE ARQUIVO NÃO FAZ: gravar. Pedir pagamento, prestar contas, cadastrar
# contrato e medir passam pelas MESMAS funções e rotas da tela cheia do ERP
# (`_gravar_lancamento`, `criar_prestacao`, `criar_contrato`,
# `registrar_medicao`), com a mesma conferência de campos (`pleitos.py`). Aqui
# mora só o que a tela do portal precisa LER, recortado para quem está nela.
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario

# ⚑ EM TESTE: SÓ O ADMINISTRADOR (26/11/2026). Para abrir à obra: False — e a
# área do portal passa a aparecer para quem tem a ação `lancar`.
EM_TESTE_SO_ADMIN = True


def liberado(usuario: Optional[Usuario]) -> bool:
    """Esta pessoa já pode usar o financeiro do portal?"""
    if usuario is None:
        return False
    from app.apps.erp.core.auth.permissoes import pode
    if EM_TESTE_SO_ADMIN:
        return usuario.perfil == PerfilUsuario.ADMIN
    return pode(usuario, "lancar")


# Os rótulos que a OBRA lê. A mesma palavra que o financeiro vê na tela dele
# seria "EM_ANALISE"; aqui é o que acontece com o pedido, em português.
SITUACAO = {
    "RASCUNHO": ("Rascunho", "cinza"),
    "EM_ANALISE": ("Em análise no financeiro", "ambar"),
    "AGUARDANDO_AVAL": ("Esperando a confirmação", "ambar"),
    "DEVOLVIDO": ("Devolvido para corrigir", "vermelha"),
    "AGUARDANDO_APROVACAO": ("Esperando a aprovação", "ambar"),
    "APROVADO": ("Aprovado — vai ser pago", "verde"),
    "BLOQUEADO": ("Parado — o financeiro precisa resolver", "vermelha"),
    "PAGO_PARCIAL": ("Pago em parte", "verde"),
    "PAGO": ("Pago", "verde"),
    "CANCELADO": ("Cancelado", "cinza"),
    "ESTORNADO": ("Estornado", "cinza"),
}
CANCELAVEIS = {"RASCUNHO", "EM_ANALISE", "DEVOLVIDO", "AGUARDANDO_AVAL",
               "AGUARDANDO_APROVACAO", "APROVADO", "BLOQUEADO"}
SITUACAO_CONTRATO = {"AGUARDANDO_AVAL": "Esperando o aval", "VIGENTE": "Vigente",
                     "CONCLUIDO": "Concluído", "CANCELADO": "Cancelado",
                     "SUSPENSO": "Suspenso"}
SITUACAO_MEDICAO = {"MEDIDA": "Esperando a autorização", "FATURADA": "Autorizada — virou pagamento",
                    "CANCELADA": "Cancelada", "RECUSADA": "Recusada"}


def _rotulo_do_tipo(tipo: str, modalidade: Optional[str]) -> str:
    from app.apps.erp.core.titulos.pleitos import SUBTIPOS_PAGAMENTO
    if (modalidade or "").upper() == "FUNDO_FIXO":
        return "Fundo fixo"
    if tipo == "T5_EMPREITEIRO":
        return "Medição de empreita"
    for v in SUBTIPOS_PAGAMENTO.values():
        if v["tipo"] == tipo:
            return v["rotulo"]
    return "Pagamento"


def obras(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """As obras ATIVAS que a pessoa alcança — o título só aceita obra ativa."""
    from app.apps.erp.core.auth.permissoes import (
        obras_de_registro_sem_autor, obras_do_usuario, so_portal,
    )
    from app.apps.erp.core.comum.rotulos import rotulo_da_obra
    ids = (obras_de_registro_sem_autor(s, usuario) if so_portal(usuario.perfil)
           else obras_do_usuario(s, usuario))
    if ids is not None and not ids:
        return []
    linhas = s.execute(text("""
        SELECT id, codigo, nome FROM obras
         WHERE status = 'ATIVA' AND (:todas OR id = ANY(:ids))
         ORDER BY codigo"""), {"todas": ids is None, "ids": ids or []}).mappings().all()
    return [{"id": l["id"], "codigo": l["codigo"],
             "rotulo": rotulo_da_obra(l["codigo"], l["nome"])} for l in linhas]


def categorias(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """As contas do plano que a pessoa usa, com os tipos que cada uma aceita —
    é por eles que a tela mostra só as que servem ao subtipo escolhido."""
    from app.apps.erp.core.cadastros.sugestao import categorias_do_usuario
    from app.apps.erp.db.models.cadastros import Categoria
    permitidas = categorias_do_usuario(s, usuario)
    stmt = select(Categoria).where(Categoria.ativo.is_(True))
    if permitidas:
        stmt = stmt.where(Categoria.id.in_(permitidas))
    return [{"id": c.id, "codigo": c.codigo, "descricao": c.descricao,
             "grupo": c.grupo_nome or "",
             "tipos": [(t.value if hasattr(t, "value") else str(t))
                       for t in (c.tipos_permitidos or [])]}
            for c in s.scalars(stmt.order_by(Categoria.ordem, Categoria.codigo)).all()]


def contratos_para_medir(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """Os contratos VIGENTES das obras da pessoa, com o saldo de cada serviço."""
    from app.apps.erp.core.titulos import empreita
    saida = []
    for c in empreita.listar(s, usuario):
        if c["status"] != "VIGENTE":
            continue
        itens = list(empreita._saldo_por_item(s, c["id"]).values())
        contrato = s.get(empreita.ContratoServico, c["id"])
        saida.append({
            "id": c["id"], "numero": c["numero"], "objeto": c["objeto"],
            "obra": c["obra"], "prestador": c["prestador"], "modo": c["modo"],
            "saldo": c["saldo"], "valor_vigente": c["valor_vigente"],
            "exige_foto": bool(contrato.exige_foto),
            "jeito": empreita.jeito_de_medir(s, c["id"]),
            "itens": [{"id": i["id"], "descricao": i["descricao"],
                       "unidade": i["unidade"] or "", "preco_unitario": i["preco_unitario"],
                       "saldo": i["saldo"]} for i in itens],
        })
    return saida


def meus_lancamentos(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """Tudo que ESTA PESSOA pediu no financeiro: pagamentos, fundo fixo,
    contratos cadastrados e medições — uma lista só, do mais novo ao mais velho.
    A pessoa quer saber o que está parado, não caçar tela por tela."""
    from app.apps.erp.db.models.financeiro import (
        ContratoMedicao, ContratoServico, Titulo,
    )
    itens: list[dict[str, Any]] = []
    titulos = s.scalars(
        select(Titulo).where(Titulo.solicitante_id == usuario.id)
        .options(selectinload(Titulo.fornecedor), selectinload(Titulo.parcelas),
                 selectinload(Titulo.rateios))
        .order_by(Titulo.id.desc()).limit(150)).all()
    for t in titulos:
        status = t.status.value
        rot, cor = SITUACAO.get(status, (status, "cinza"))
        venc = min((p.vencimento for p in t.parcelas), default=None)
        itens.append({
            "especie": "titulo", "id": t.id, "numero": t.numero_sp,
            "tipo": _rotulo_do_tipo(t.tipo.value, t.modalidade),
            "quem": t.fornecedor.razao_social if t.fornecedor else "",
            "valor": float(t.valor_liquido), "situacao": rot, "cor": cor,
            "vencimento": venc.isoformat() if venc else None,
            "quando": t.criado_em.isoformat() if getattr(t, "criado_em", None) else None,
            "pode_cancelar": status in CANCELAVEIS,
        })
    contratos = s.scalars(
        select(ContratoServico).where(ContratoServico.criado_por == usuario.id)
        .options(selectinload(ContratoServico.fornecedor), selectinload(ContratoServico.obra))
        .order_by(ContratoServico.id.desc()).limit(50)).all()
    for c in contratos:
        itens.append({
            "especie": "contrato", "id": c.id, "numero": c.numero,
            "tipo": "Contrato de empreita", "quem": c.fornecedor.razao_social,
            "valor": float(c.valor_total),
            "situacao": SITUACAO_CONTRATO.get(c.status, c.status),
            "cor": "verde" if c.status == "VIGENTE" else "ambar"
                   if c.status == "AGUARDANDO_AVAL" else "cinza",
            "vencimento": None, "obra": c.obra.codigo if c.obra else "",
            "quando": c.criado_em.isoformat() if getattr(c, "criado_em", None) else None,
            "pode_cancelar": False,
        })
    medicoes = s.scalars(
        select(ContratoMedicao).where(ContratoMedicao.medido_por == usuario.id)
        .options(selectinload(ContratoMedicao.contrato).selectinload(ContratoServico.fornecedor))
        .order_by(ContratoMedicao.id.desc()).limit(50)).all()
    for m in medicoes:
        itens.append({
            "especie": "medicao", "id": m.id,
            "numero": f"{m.contrato.numero} · medição {m.numero}",
            "tipo": "Medição de empreita", "quem": m.contrato.fornecedor.razao_social,
            "valor": float(m.valor_medido),
            "situacao": SITUACAO_MEDICAO.get(m.status, m.status),
            "cor": "verde" if m.status == "FATURADA" else "ambar"
                   if m.status == "MEDIDA" else "cinza",
            "vencimento": None,
            "quando": m.criado_em.isoformat() if getattr(m, "criado_em", None) else None,
            "pode_cancelar": False,
        })
    itens.sort(key=lambda x: x.get("quando") or "", reverse=True)
    return itens


def detalhe(s: Session, usuario: Usuario, titulo_id: int) -> dict[str, Any]:
    """Um pedido de pagamento por dentro: valores, parcelas, documentos e o
    que aconteceu com ele. Só o que é da pessoa — o resto responde "não
    encontrado", nunca "sem permissão"."""
    from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
    from app.apps.erp.core.documentos.armazenamento import listar as listar_anexos
    from app.apps.erp.db.models.financeiro import Titulo
    exigir_titulo_no_escopo(s, usuario, titulo_id)
    t = s.get(Titulo, titulo_id, options=[
        selectinload(Titulo.fornecedor), selectinload(Titulo.parcelas),
        selectinload(Titulo.rateios)])
    if t is None or t.solicitante_id != usuario.id:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    status = t.status.value
    rot, cor = SITUACAO.get(status, (status, "cinza"))
    eventos = s.execute(text("""
        SELECT e.acao, e.detalhe, e.criado_em, u.nome
          FROM eventos e LEFT JOIN usuarios u ON u.id = e.usuario_id
         WHERE e.entidade_tipo = 'titulo' AND e.entidade_id = :i
         ORDER BY e.id"""), {"i": t.id}).mappings().all()
    acontecimentos = {
        "CRIADO": "Pedido enviado", "AGUARDANDO_AVAL": "Esperando a confirmação",
        "AVAL_CONFIRMADO": "Confirmado", "AVAL_RECUSADO": "Confirmação recusada",
        "APROVADO": "Aprovado para pagamento",
        "DEVOLVIDO": "Devolvido para corrigir", "CANCELADO": "Cancelado",
        "BAIXA_CONFIRMADA_COM_COMPROVANTE": "Pago", "ESTORNADO": "Estornado",
        "REANALISADO": "Analisado de novo", "PRESTACAO_CRIADA": "Prestação enviada",
    }
    historia = [{"quando": str(e["criado_em"]), "quem": e["nome"] or "",
                 "o_que": acontecimentos[e["acao"]],
                 "motivo": (e["detalhe"] or {}).get("motivo", "")
                 if isinstance(e["detalhe"], dict) else ""}
                for e in eventos if e["acao"] in acontecimentos]
    # POR QUE ESTÁ PARADO, em português: o que a última análise bloqueou. Sem
    # isto a obra lê "parado" e não sabe se é com ela (conta do credor ainda
    # sem homologar é o caso mais comum, e não é com ela).
    ultima = s.execute(text("""
        SELECT criticas FROM analises WHERE titulo_id = :i
         ORDER BY id DESC LIMIT 1"""), {"i": t.id}).first()
    motivos = [c.get("mensagem") or c.get("msg") or ""
               for c in ((ultima[0] if ultima else None) or [])
               if isinstance(c, dict)
               and (c.get("severidade") or c.get("gravidade")) == "BLOQUEIA"]
    from app.apps.erp.core.titulos.pleitos import FORMAS
    return {
        "por_que_parado": [m for m in motivos if m] if status == "BLOQUEADO" else [],
        "id": t.id, "numero": t.numero_sp,
        "tipo": _rotulo_do_tipo(t.tipo.value, t.modalidade),
        "credor": t.fornecedor.razao_social if t.fornecedor else "",
        "descricao": t.descricao, "situacao": rot, "cor": cor,
        "forma": FORMAS.get(t.forma_pagamento.value, {}).get("rotulo", t.forma_pagamento.value),
        "valor_bruto": float(t.valor_bruto), "retencoes": float(t.valor_retencoes or 0),
        "valor_liquido": float(t.valor_liquido),
        "parcelas": [{"numero": p.numero, "vencimento": p.vencimento.isoformat(),
                      "valor": float(p.valor), "situacao": p.status.value}
                     for p in sorted(t.parcelas, key=lambda p: p.numero)],
        "documentos": listar_anexos(s, "titulo", t.id),
        "historia": historia,
        "pode_cancelar": status in CANCELAVEIS,
    }


# ---------------------------------------------------------------------------
# ⚑ OS CONTRATOS DA OBRA (27/11/2026). Ele: *"Quem está fazendo o lançamento do
# acompanhamento, ele vê todos os contratos. Da obra. É aquela mesma lógica do
# ponto (…) vai estar vendo as obras que ele está tocando."* Não só os que ELE
# cadastrou — todos os das obras que ele alcança (`empreita.listar`, que para
# quem é da obra usa as obras dele).
# ---------------------------------------------------------------------------
SITUACAO_CONTRATO["AGUARDANDO_ASSINATURA"] = "Esperando as assinaturas"


def contratos_da_obra(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    from app.apps.erp.core.titulos import contrato_assinatura, empreita
    saida = []
    for c in empreita.listar(s, usuario):
        ass = contrato_assinatura.situacao(s, c["id"])
        saida.append({
            "id": c["id"], "numero": c["numero"], "objeto": c["objeto"], "obra": c["obra"],
            "prestador": c["prestador"], "modo": c["modo"], "status": c["status"],
            "situacao": SITUACAO_CONTRATO.get(c["status"], c["status"]),
            "valor_vigente": c["valor_vigente"], "medido": c["medido"], "saldo": c["saldo"],
            "percentual_medido": c["percentual_medido"], "medicoes": c["medicoes"],
            "assinatura": ass["rotulo"] if ass else None,
        })
    return saida


def contrato_por_dentro(s: Session, usuario: Usuario, contrato_id: int) -> dict[str, Any]:
    """O contrato aberto: o saldo de cada serviço, as medições com o que foi
    executado, as observações e as FOTOS (ele: *"fotos dos serviços, para poder
    demonstrar que aquele serviço está sendo executado"*), e a assinatura."""
    from app.apps.erp.core.auth.permissoes import exigir_empreita_no_escopo
    from app.apps.erp.core.documentos.armazenamento import listar as anexos
    from app.apps.erp.core.titulos import contrato_assinatura, empreita
    exigir_empreita_no_escopo(s, usuario, contrato_id)
    d = empreita.detalhar(s, contrato_id)
    itens_por_medicao: dict[int, list[dict[str, Any]]] = {}
    for l in s.execute(text("""
            SELECT mi.medicao_id, ci.descricao, ci.unidade, mi.quantidade, mi.valor, mi.observacao
              FROM medicao_itens mi JOIN contrato_servico_itens ci ON ci.id = mi.contrato_item_id
             WHERE mi.medicao_id = ANY(:ids) ORDER BY ci.ordem"""),
            {"ids": [m["id"] for m in d["medicoes"]] or [0]}).mappings().all():
        itens_por_medicao.setdefault(l["medicao_id"], []).append({
            "servico": l["descricao"], "unidade": l["unidade"] or "",
            "quantidade": float(l["quantidade"]), "valor": float(l["valor"]),
            "observacao": l["observacao"] or ""})
    for m in d["medicoes"]:
        m["itens"] = itens_por_medicao.get(m["id"], [])
        m["fotos"] = anexos(s, "medicao", m["id"])
        m["situacao"] = SITUACAO_MEDICAO.get(m["status"], m["status"])
    d["situacao"] = SITUACAO_CONTRATO.get(d["status"], d["status"])
    d["documentos"] = anexos(s, "contrato_servico", contrato_id)
    d["assinatura"] = contrato_assinatura.situacao(s, contrato_id)
    d.pop("alcada", None)
    return d
