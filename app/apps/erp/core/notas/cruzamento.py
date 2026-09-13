# ============================================================================
# ERP — core/notas/cruzamento.py
# Nota fiscal × pedido de compra × título financeiro × prestação de fundo fixo.
#
# O PROBLEMA, nas palavras do dono (07/09/2026):
#
#   "Comprei dez carradas de brita, e o fornecedor emite a nota por carrada.
#    Cada caminhão que sai é uma nota emitida. Aquele pedido não se fecha
#    instantaneamente. (…) Aquela previsão de dez carradas vai se transformar
#    provavelmente em dez notas e dez boletos, ou seja, dez contas a pagar."
#
# É por isso que NADA aqui assume um-para-um. Um pedido tem N notas; cada nota
# vira (ou não) um título. Desenho que assumisse "um pedido = uma nota = um
# título" nasceria errado e teria de ser refeito.
#
# O MODELO É O DA CONCILIAÇÃO BANCÁRIA, que já funciona no ERP e que ele
# próprio citou: o sistema cruza o que consegue e EXPÕE PARA UMA PESSOA
# DECIDIR o que ficou duvidoso. O sistema propõe; quem confirma é gente.
#
# O ALERTA QUE INTERESSA não é a nota casada — é a que não cruza com nada.
# Pode ser compra legítima que alguém esqueceu de lançar, ou nota emitida em
# nome da empresa sem autorização. As duas precisam de olho humano.
# ============================================================================
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento)
from app.apps.erp.core.comum.formato import _dinheiro_br
from app.apps.erp.db.models.cadastros import (Empresa, Fornecedor, Obra,
                                              PedidoCompra, Usuario)
from app.apps.erp.db.models.financeiro import (DocumentoFiscal, Titulo,
                                               TituloItem)

logger = logging.getLogger(__name__)

CONFERENCIAS = ("PENDENTE", "CASADA", "SEM_PAR", "IGNORADA")

# Janela para casar nota com pedido e com título. Nota costuma sair dias depois
# do pedido e o título nasce perto da nota; 90 dias cobre a prática da obra sem
# transformar tudo em candidato.
JANELA_PEDIDO_DIAS = 90
JANELA_TITULO_DIAS = 30
TOLERANCIA = Decimal("0.02")


def _digitos(v: Optional[str]) -> str:
    return re.sub(r"\D", "", v or "")


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal(0)


# ---------------------------------------------------------------------------
# De quem é esta nota — e contra qual CNPJ nosso
# ---------------------------------------------------------------------------
def empresa_da_nota(s: Session, nota: DocumentoFiscal) -> Optional[Empresa]:
    """A empresa da BWS contra quem a nota foi emitida, pelo CNPJ do destinatário."""
    alvo = _digitos(nota.destinatario_doc)
    if not alvo:
        return None
    for e in s.scalars(select(Empresa)).all():
        if _digitos(e.cnpj) == alvo:
            return e
    return None


def fornecedor_da_nota(s: Session, nota: DocumentoFiscal) -> Optional[Fornecedor]:
    alvo = _digitos(nota.emitente_doc)
    if not alvo:
        return None
    for f in s.scalars(select(Fornecedor)).all():
        if _digitos(f.cnpj_cpf) == alvo:
            return f
    return None


# ---------------------------------------------------------------------------
# Os candidatos de cada ponta
# ---------------------------------------------------------------------------
def _candidatos_titulo(s: Session, nota: DocumentoFiscal) -> list[dict[str, Any]]:
    """Títulos que podem ser desta nota, do mais certo para o menos.

    A chave de acesso é prova; o resto é indício e precisa de gente.
    """
    saida: list[dict[str, Any]] = []
    vistos: set[int] = set()

    def _juntar(t: Titulo, confianca: float, motivo: str) -> None:
        if t.id in vistos:
            return
        vistos.add(t.id)
        saida.append({
            "id": t.id, "numero_sp": t.numero_sp,
            "credor": t.fornecedor.razao_social if t.fornecedor else "",
            "descricao": t.descricao,
            "valor": float(t.valor_liquido or 0),
            "status": t.status.value if hasattr(t.status, "value") else str(t.status),
            "confianca": confianca, "motivo": motivo,
        })

    # 1. já ligado, ou com a chave de acesso guardada: é prova, não palpite
    if nota.chave_acesso:
        for t in s.scalars(select(Titulo).where(or_(
                Titulo.documento_fiscal_id == nota.id,
                Titulo.chave_acesso_nfe == nota.chave_acesso))).all():
            _juntar(t, 1.0, "chave de acesso da nota")

    # 2. mesmo credor, mesmo valor, data próxima
    forn = fornecedor_da_nota(s, nota)
    if forn is not None and nota.valor_total is not None:
        for t in s.scalars(select(Titulo).where(
                Titulo.fornecedor_id == forn.id)).all():
            if abs(_dec(t.valor_liquido) - _dec(nota.valor_total)) > TOLERANCIA:
                continue
            if nota.data_emissao and t.competencia:
                if abs((t.competencia - nota.data_emissao).days) > JANELA_TITULO_DIAS + 31:
                    continue
            _juntar(t, 0.7, "mesmo credor e mesmo valor")

    saida.sort(key=lambda c: c["confianca"], reverse=True)
    return saida


def _candidatos_pedido(s: Session, nota: DocumentoFiscal) -> list[dict[str, Any]]:
    """Pedidos que esta nota pode estar cumprindo.

    Nunca por valor exato: a nota costuma ser UM PEDAÇO do pedido. O que conta é
    o fornecedor e a janela de tempo — e o quanto do pedido ainda falta vir.
    """
    forn = fornecedor_da_nota(s, nota)
    if forn is None:
        return []
    saida = []
    for p in s.scalars(select(PedidoCompra).where(
            PedidoCompra.fornecedor_id == forn.id)).all():
        criado = p.criado_em.date() if isinstance(p.criado_em, datetime) else p.criado_em
        if nota.data_emissao and criado:
            dias = (nota.data_emissao - criado).days
            if dias < -3 or dias > JANELA_PEDIDO_DIAS:
                continue      # nota anterior ao pedido, ou velha demais
        andamento = andamento_do_pedido(s, p)
        saida.append({
            "id": p.id, "numero": p.numero,
            "fornecedor": forn.razao_social,
            "status": p.status.value if hasattr(p.status, "value") else str(p.status),
            "total": andamento["total"], "em_notas": andamento["em_notas"],
            "falta": andamento["falta"], "notas": andamento["notas"],
            "confianca": 0.6 if andamento["falta"] > 0 else 0.3,
            "motivo": ("mesmo fornecedor, e o pedido ainda tem saldo"
                       if andamento["falta"] > 0 else "mesmo fornecedor"),
        })
    saida.sort(key=lambda c: c["confianca"], reverse=True)
    return saida


def _candidatos_fundo_fixo(s: Session, nota: DocumentoFiscal) -> list[dict[str, Any]]:
    """Linhas de prestação de fundo fixo que podem ser esta nota.

    O caso que o dono fez questão de citar: *"uma notazinha pequena que foi
    emitida vai aparecer lá na prestação do fundo fixo — não foi o comprador,
    foi alguém da administração da obra que precisou fazer uma compra e mandou
    emitir a nota no CNPJ da empresa, cem reais que seja."*
    """
    if nota.valor_total is None:
        return []
    saida = []
    for it in s.scalars(select(TituloItem)).all():
        if abs(_dec(it.valor) - _dec(nota.valor_total)) > TOLERANCIA:
            continue
        if nota.data_emissao and it.data_despesa:
            if abs((it.data_despesa - nota.data_emissao).days) > 7:
                continue
        t = s.get(Titulo, it.titulo_id)
        saida.append({
            "id": it.id, "titulo_id": it.titulo_id,
            "numero_sp": getattr(t, "numero_sp", ""),
            "descricao": it.descricao, "estabelecimento": it.estabelecimento,
            "documento": it.documento,
            "valor": float(it.valor or 0),
            "data": it.data_despesa.isoformat() if it.data_despesa else None,
            "confianca": 0.75, "motivo": "mesmo valor e mesma data na prestação",
        })
    return saida


def andamento_do_pedido(s: Session, pedido: PedidoCompra) -> dict[str, Any]:
    """Quanto do pedido já veio em nota, e quanto falta.

    É a pergunta do outro lado do cruzamento, e é ela que responde "esse pedido
    já fechou?" quando um pedido vira dez notas.
    """
    from app.apps.erp.db.models.cadastros import PedidoItem
    itens = s.scalars(select(PedidoItem).where(
        PedidoItem.pedido_id == pedido.id)).all()
    total = sum((_dec(i.quantidade) * _dec(i.preco_unitario) for i in itens), Decimal(0))
    total += _dec(pedido.frete) - _dec(pedido.desconto)
    notas = s.scalars(select(DocumentoFiscal).where(
        DocumentoFiscal.pedido_compra_id == pedido.id)).all()
    em_notas = sum((_dec(n.valor_total) for n in notas), Decimal(0))
    falta = total - em_notas
    return {
        "total": float(total), "em_notas": float(em_notas),
        "falta": float(falta if falta > 0 else 0),
        "notas": len(notas),
        "fechado": bool(total > 0 and falta <= TOLERANCIA),
    }


# ---------------------------------------------------------------------------
# A leitura de uma nota, do jeito que a tela precisa
# ---------------------------------------------------------------------------
def _porta_de_entrada(s: Session, nota: DocumentoFiscal,
                      titulo: Optional[Titulo]) -> str:
    """POR ONDE esta despesa entrou na contabilidade.

    O ponto mais perigoso do cruzamento inteiro, e o dono nomeou: *"vai
    acontecer de aparecer uma nota fiscal que é de um fundo fixo. Então ele
    está sendo dedutível de duas formas, pela nota ou por ser fundo fixo — mas
    também não pode entrar duplicado na contabilidade."*

    O erro aqui não aparece na tela: aparece na contabilidade, meses depois.
    """
    if nota.titulo_item_id and titulo is not None:
        return "DUAS_PORTAS"        # é isto que a tela precisa gritar
    if nota.titulo_item_id:
        return "FUNDO_FIXO"
    if titulo is not None:
        return "TITULO"
    return "NENHUMA"


def ler(s: Session, nota: DocumentoFiscal, *, com_candidatos: bool = False) -> dict[str, Any]:
    empresa = s.get(Empresa, nota.empresa_id) if nota.empresa_id else None
    pedido = s.get(PedidoCompra, nota.pedido_compra_id) if nota.pedido_compra_id else None
    titulo = s.scalars(select(Titulo).where(
        Titulo.documento_fiscal_id == nota.id)).first()
    item = s.get(TituloItem, nota.titulo_item_id) if nota.titulo_item_id else None
    porta = _porta_de_entrada(s, nota, titulo)

    linha = {
        "id": nota.id,
        "tipo": nota.tipo.value if hasattr(nota.tipo, "value") else str(nota.tipo),
        "chave": nota.chave_acesso,
        "numero": nota.numero, "serie": nota.serie,
        "emitente": nota.emitente_nome, "emitente_doc": nota.emitente_doc,
        "destinatario_doc": nota.destinatario_doc,
        "empresa": getattr(empresa, "nome_fantasia", None) or getattr(empresa, "razao_social", None),
        "empresa_id": nota.empresa_id,
        "valor": float(nota.valor_total or 0),
        "valor_br": _dinheiro_br(nota.valor_total or 0),
        "emissao": nota.data_emissao.isoformat() if nota.data_emissao else None,
        "origem": nota.origem,
        "conferencia": nota.conferencia,
        "motivo": nota.conferencia_motivo,
        "pedido": ({"id": pedido.id, "numero": pedido.numero} if pedido else None),
        "titulo": ({"id": titulo.id, "numero_sp": titulo.numero_sp,
                    "status": titulo.status.value if hasattr(titulo.status, "value")
                    else str(titulo.status),
                    "dedutibilidade": (titulo.dedutibilidade.value
                                       if hasattr(titulo.dedutibilidade, "value")
                                       else str(titulo.dedutibilidade))}
                   if titulo else None),
        "fundo_fixo": ({"item_id": item.id, "titulo_id": item.titulo_id,
                        "descricao": item.descricao} if item else None),
        "porta": porta,
        # DEDUTÍVEL É POR ONDE ENTROU. O dono foi taxativo em 07/09/2026:
        # "fundo fixo é dedutível, ponto final. Não é só nota que é dedutiva."
        "dedutivel": porta in ("TITULO", "FUNDO_FIXO", "DUAS_PORTAS"),
        "alerta_duplo": porta == "DUAS_PORTAS",
        "sem_par": porta == "NENHUMA" and not nota.pedido_compra_id,
    }
    if com_candidatos:
        linha["candidatos"] = {
            "titulos": _candidatos_titulo(s, nota),
            "pedidos": _candidatos_pedido(s, nota),
            "fundo_fixo": _candidatos_fundo_fixo(s, nota),
        }
    return linha


def aplicar_escopo(stmt, s: Session, usuario: Optional[Usuario]):
    """O recorte da tela de notas recebidas — em UM lugar só.

    Duas travas, e valem juntas:

      · quem NÃO é do financeiro vê apenas a nota **já associada** a um
        pedido, a um título ou a uma linha de prestação;
      · quem é preso a obra vê, dessas, só as que alcançam as obras dele.

    A nota associada é o que sobra: o que ela liga já é coisa da obra, e
    quem responde pela obra tem por que consultá-la.
    """
    if usuario is None:
        return stmt

    from app.apps.erp.core.auth.permissoes import (
        aplicar_escopo as escopo_de_titulo, obras_do_usuario, pode_com_banco)

    # QUEM CRUZA VÊ A NOTA SOLTA. Decisão do dono em 12/09/2026: *"esse negócio
    # de ver as notas acho que deve ficar restrito ao pessoal do financeiro.
    # Demais verão notas que já estão associadas"*.
    #
    # A trava é a AÇÃO de cruzar, não o cargo — de propósito. Hoje ela é do
    # financeiro por cargo, mas o ERP permite marcá-la numa pessoa (migração
    # 032), e é o caso do comprador: é ele quem sabe de que pedido cada nota
    # é. Amarrar ao cargo faria a regra mentir no dia em que o dono marcasse a
    # caixinha para alguém.
    #
    # E o motivo de fechar: a nota solta ou é compra legítima que ninguém
    # lançou, ou é nota emitida contra a empresa sem autorização. Nos dois
    # casos, quem não pode cruzar não pode fazer nada com ela — seria ruído.
    if pode_com_banco(s, usuario, "cruzar_notas"):
        return stmt

    # 1) só o que já está associado a alguma coisa
    stmt = stmt.where(or_(DocumentoFiscal.pedido_compra_id.is_not(None),
                          DocumentoFiscal.titulo_item_id.is_not(None)))

    obras = obras_do_usuario(s, usuario)
    if obras is None:
        return stmt                       # enxerga todas as obras

    # 2) e, dessas, só as que chegam nas obras da pessoa. O caminho até a obra
    #    é um dos dois: pela linha da prestação (nota → item → título) ou pelo
    #    pedido de compra (nota → pedido → item do suprimento → obra).
    from app.apps.erp.db.models.cadastros import PedidoItem, SuprimentoItem

    titulos_no_escopo = escopo_de_titulo(select(Titulo.id), s, usuario)
    por_titulo = select(TituloItem.id).where(
        TituloItem.titulo_id.in_(titulos_no_escopo))
    por_pedido = (select(PedidoItem.pedido_id)
                  .join(SuprimentoItem,
                        SuprimentoItem.id == PedidoItem.suprimento_item_id)
                  .where(SuprimentoItem.obra_id.in_(obras or [-1])))
    return stmt.where(or_(DocumentoFiscal.titulo_item_id.in_(por_titulo),
                          DocumentoFiscal.pedido_compra_id.in_(por_pedido)))


def pode_ver_nota(s: Session, usuario: Optional[Usuario], nota_id: int) -> bool:
    """A nota existe E está dentro do recorte desta pessoa?

    Passa pelo MESMO `aplicar_escopo` da listagem — tela e detalhe não têm
    como divergir sem que alguém altere os dois.
    """
    stmt = select(DocumentoFiscal.id).where(DocumentoFiscal.id == nota_id)
    return s.scalar(aplicar_escopo(stmt, s, usuario)) is not None


def exigir_nota_no_escopo(s: Session, usuario: Optional[Usuario],
                          nota_id: int) -> None:
    """Fora do recorte responde igual a inexistente."""
    if not pode_ver_nota(s, usuario, nota_id):
        raise ErroNaoEncontrado("Nota não encontrada.")


def listar(s: Session, *, conferencia: str = "", empresa_id: Optional[int] = None,
           desde: Optional[date] = None, ate: Optional[date] = None,
           busca: str = "", limite: int = 500,
           usuario: Optional[Usuario] = None) -> dict[str, Any]:
    stmt = aplicar_escopo(select(DocumentoFiscal), s, usuario).order_by(
        DocumentoFiscal.data_emissao.desc().nullslast(),
        DocumentoFiscal.id.desc()).limit(limite)
    if conferencia:
        stmt = stmt.where(DocumentoFiscal.conferencia == conferencia)
    if empresa_id:
        stmt = stmt.where(DocumentoFiscal.empresa_id == empresa_id)
    if desde:
        stmt = stmt.where(DocumentoFiscal.data_emissao >= desde)
    if ate:
        stmt = stmt.where(DocumentoFiscal.data_emissao <= ate)
    busca = (busca or "").strip()
    if busca:
        alvo = f"%{busca}%"
        stmt = stmt.where(or_(DocumentoFiscal.emitente_nome.ilike(alvo),
                              DocumentoFiscal.numero.ilike(alvo),
                              DocumentoFiscal.chave_acesso.ilike(alvo)))
    linhas = [ler(s, n) for n in s.scalars(stmt).all()]

    def _soma(f):
        return round(sum(l["valor"] for l in linhas if f(l)), 2)
    resumo = {
        "quantidade": len(linhas), "total": _soma(lambda l: True),
        "sem_par": sum(1 for l in linhas if l["sem_par"]),
        "valor_sem_par": _soma(lambda l: l["sem_par"]),
        "pendentes": sum(1 for l in linhas if l["conferencia"] == "PENDENTE"),
        "casadas": sum(1 for l in linhas if l["conferencia"] == "CASADA"),
        "duplo": sum(1 for l in linhas if l["alerta_duplo"]),
        "dedutivel": _soma(lambda l: l["dedutivel"]),
        "sem_empresa": sum(1 for l in linhas if not l["empresa_id"]),
    }
    return {"notas": linhas, "resumo": resumo,
            "limite_atingido": len(linhas) >= limite}


# ---------------------------------------------------------------------------
# O que a PESSOA decide
# ---------------------------------------------------------------------------
def _exigir(nota: Optional[DocumentoFiscal]) -> DocumentoFiscal:
    if nota is None:
        raise ErroValidacao("Nota não encontrada.")
    return nota


def ligar(s: Session, nota_id: int, *, titulo_id: Optional[int] = None,
          pedido_id: Optional[int] = None, item_id: Optional[int] = None,
          usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Amarra a nota ao que a pessoa confirmou. Uma ponta por chamada, ou várias.

    A TRAVA CONTRA CONTAR DUAS VEZES vive aqui: a mesma nota não pode ter
    título próprio E estar dentro de uma prestação de fundo fixo. Seria a mesma
    despesa deduzida duas vezes, e o erro só apareceria na contabilidade.
    """
    nota = _exigir(s.get(DocumentoFiscal, nota_id))
    titulo_atual = s.scalars(select(Titulo).where(
        Titulo.documento_fiscal_id == nota.id)).first()

    if titulo_id is not None:
        t = s.get(Titulo, titulo_id)
        if t is None:
            raise ErroValidacao("Título não encontrado.")
        if nota.titulo_item_id:
            raise ErroValidacao(
                "Esta nota já está dentro de uma prestação de fundo fixo. "
                "Ligar um título a ela faria a mesma despesa ser deduzida duas "
                "vezes. Desfaça a ligação com o fundo fixo primeiro.")
        outro = s.scalars(select(Titulo).where(
            Titulo.documento_fiscal_id == nota.id, Titulo.id != t.id)).first()
        if outro is not None:
            raise ErroValidacao(f"A nota já pertence ao título {outro.numero_sp}.")
        t.documento_fiscal_id = nota.id
        if nota.chave_acesso and not t.chave_acesso_nfe:
            t.chave_acesso_nfe = nota.chave_acesso
        titulo_atual = t

    if item_id is not None:
        it = s.get(TituloItem, item_id)
        if it is None:
            raise ErroValidacao("Linha da prestação não encontrada.")
        if titulo_atual is not None:
            raise ErroValidacao(
                f"Esta nota já é do título {titulo_atual.numero_sp}. Colocá-la "
                "também numa prestação de fundo fixo faria a mesma despesa ser "
                "deduzida duas vezes.")
        nota.titulo_item_id = it.id

    if pedido_id is not None:
        p = s.get(PedidoCompra, pedido_id)
        if p is None:
            raise ErroValidacao("Pedido não encontrado.")
        nota.pedido_compra_id = p.id

    if nota.empresa_id is None:
        e = empresa_da_nota(s, nota)
        if e is not None:
            nota.empresa_id = e.id

    nota.conferencia = "CASADA"
    nota.conferido_por = usuario.id if usuario else None
    nota.conferido_em = datetime.now()
    s.flush()
    registrar_evento(s, "documento_fiscal", nota.id, "CRUZAMENTO_CONFIRMADO", {
        "titulo_id": titulo_id, "pedido_id": pedido_id, "item_id": item_id},
        usuario.id if usuario else None)
    return ler(s, nota, com_candidatos=True)


def desligar(s: Session, nota_id: int, *, o_que: str,
             usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Desfaz uma ligação. `o_que` é 'titulo', 'pedido' ou 'fundo_fixo'."""
    nota = _exigir(s.get(DocumentoFiscal, nota_id))
    if o_que == "titulo":
        for t in s.scalars(select(Titulo).where(
                Titulo.documento_fiscal_id == nota.id)).all():
            t.documento_fiscal_id = None
    elif o_que == "pedido":
        nota.pedido_compra_id = None
    elif o_que == "fundo_fixo":
        nota.titulo_item_id = None
    else:
        raise ErroValidacao("Só dá para desfazer título, pedido ou fundo fixo.")
    nota.conferencia = "PENDENTE"
    s.flush()
    registrar_evento(s, "documento_fiscal", nota.id, "CRUZAMENTO_DESFEITO",
                     {"o_que": o_que}, usuario.id if usuario else None)
    return ler(s, nota, com_candidatos=True)


def marcar(s: Session, nota_id: int, *, conferencia: str, motivo: str = "",
           usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Decisão humana sem ligação: 'não achei par' ou 'ignore esta'."""
    if conferencia not in CONFERENCIAS:
        raise ErroValidacao(f"Situação inválida: {conferencia}.")
    nota = _exigir(s.get(DocumentoFiscal, nota_id))
    motivo = (motivo or "").strip()
    if conferencia == "IGNORADA" and not motivo:
        raise ErroValidacao(
            "Escreva o motivo de ignorar esta nota. Seis meses depois ninguém "
            "lembra por que ela foi posta de lado — e é isso que o fisco pergunta.")
    nota.conferencia = conferencia
    nota.conferencia_motivo = motivo or None
    nota.conferido_por = usuario.id if usuario else None
    nota.conferido_em = datetime.now()
    s.flush()
    registrar_evento(s, "documento_fiscal", nota.id, "CRUZAMENTO_MARCADO",
                     {"conferencia": conferencia, "motivo": motivo},
                     usuario.id if usuario else None)
    return ler(s, nota)


# ---------------------------------------------------------------------------
# O que o sistema propõe sozinho
# ---------------------------------------------------------------------------
def sugerir(s: Session, *, limite: int = 200) -> dict[str, Any]:
    """Casa sozinho só o que é PROVA (a chave de acesso) e deixa o resto para
    a pessoa. Casar por indício sem alguém olhar é como a conferência manual
    erra hoje — só que mais rápido e em silêncio."""
    casadas, propostas = 0, 0
    for nota in s.scalars(select(DocumentoFiscal).where(
            DocumentoFiscal.conferencia == "PENDENTE").limit(limite)).all():
        if nota.empresa_id is None:
            e = empresa_da_nota(s, nota)
            if e is not None:
                nota.empresa_id = e.id
        cands = _candidatos_titulo(s, nota)
        certo = [c for c in cands if c["confianca"] >= 1.0]
        if len(certo) == 1 and not nota.titulo_item_id:
            t = s.get(Titulo, certo[0]["id"])
            if t is not None and t.documento_fiscal_id is None:
                t.documento_fiscal_id = nota.id
                nota.conferencia = "CASADA"
                nota.conferido_em = datetime.now()
                casadas += 1
                continue
        if cands or _candidatos_pedido(s, nota) or _candidatos_fundo_fixo(s, nota):
            propostas += 1
    s.flush()
    logger.info("ERP/notas: %d casada(s) pela chave, %d com proposta", casadas, propostas)
    return {"casadas": casadas, "com_proposta": propostas}


# ---------------------------------------------------------------------------
# O outro lado: os pedidos e o quanto deles já veio em nota
# ---------------------------------------------------------------------------
def por_pedido(s: Session, *, limite: int = 300) -> list[dict[str, Any]]:
    saida = []
    for p in s.scalars(select(PedidoCompra).order_by(
            PedidoCompra.id.desc()).limit(limite)).all():
        forn = s.get(Fornecedor, p.fornecedor_id)
        a = andamento_do_pedido(s, p)
        saida.append({
            "id": p.id, "numero": p.numero,
            "fornecedor": getattr(forn, "razao_social", ""),
            "status": p.status.value if hasattr(p.status, "value") else str(p.status),
            "criado": (p.criado_em.date().isoformat()
                       if isinstance(p.criado_em, datetime) else None),
            **a,
        })
    return saida
