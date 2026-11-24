# ============================================================================
# ERP — core/suprimentos/insumos.py
# Quem precisa PEDE o cadastro do insumo; quem responde por suprimentos decide.
#
# Por que não deixar todo mundo cadastrar: em um mês a base tem "Cimento CP-II",
# "cimento cp 2" e "CIMENTO CPII 50KG" como três insumos diferentes, cada um com
# uma conta do plano, e os relatórios param de significar coisa alguma. O
# procedimento que já existe no papel vira tela aqui.
# ============================================================================
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Insumo, InsumoSolicitacao, StatusSolicitacaoInsumo, Usuario,
)

logger = logging.getLogger(__name__)


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _normalizar(texto: str) -> str:
    return " ".join((texto or "").split())


def solicitar(s: Session, dados: dict[str, Any], usuario: Usuario) -> InsumoSolicitacao:
    """Pedido de cadastro. Quem pede descreve; quem decide dá o nome final."""
    descricao = _normalizar(dados.get("descricao") or "")
    if len(descricao) < 3:
        raise ErroValidacao("Descreva o insumo (mínimo 3 letras).")

    ja_existe = _insumo_parecido(s, descricao)
    if ja_existe is not None:
        raise ErroValidacao(
            f"Já existe o insumo {ja_existe.codigo} · {ja_existe.descricao}. "
            f"Use esse, ou peça com outro nome se for realmente outro material.")

    pendente = [p for p in s.scalars(select(InsumoSolicitacao).where(
        InsumoSolicitacao.status == StatusSolicitacaoInsumo.PENDENTE)).all()
        if p.status is StatusSolicitacaoInsumo.PENDENTE
        and _normalizar(p.descricao).lower() == descricao.lower()]
    if pendente:
        raise ErroValidacao("Esse insumo já foi pedido e está aguardando decisão.")

    pedido = InsumoSolicitacao(
        descricao=descricao,
        justificativa=_normalizar(dados.get("justificativa") or "") or None,
        unidade=(dados.get("unidade") or "").strip().upper() or None,
        obra_id=int(dados["obra_id"]) if dados.get("obra_id") else None,
        solicitante_id=usuario.id,
        status=StatusSolicitacaoInsumo.PENDENTE)
    s.add(pedido)
    s.flush()
    registrar_evento(s, "insumo_solicitacao", pedido.id, "SOLICITADO",
                     {"descricao": descricao}, usuario.id)
    return pedido


def _insumo_parecido(s: Session, descricao: str) -> Optional[Insumo]:
    """Casa ignorando caixa e espaço repetido — o suficiente para pegar o
    pedido de algo que já está cadastrado com outra digitação."""
    alvo = _normalizar(descricao).lower()
    for i in s.scalars(select(Insumo)).all():
        if _normalizar(getattr(i, "descricao", "")).lower() == alvo:
            return i
    return None


def _obter_pendente(s: Session, solicitacao_id: int) -> InsumoSolicitacao:
    pedido = s.get(InsumoSolicitacao, solicitacao_id, with_for_update=True,
                   populate_existing=True)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido de cadastro não encontrado.")
    if pedido.status is not StatusSolicitacaoInsumo.PENDENTE:
        raise ErroValidacao(
            f"Este pedido já foi resolvido ({pedido.status.value.lower()}).")
    return pedido


def cadastrar(s: Session, solicitacao_id: int, dados: dict[str, Any],
              usuario: Usuario) -> Insumo:
    """Efetiva o cadastro: o nome final, a categoria e a conta do plano são de
    quem decide, não de quem pediu. É isso que mantém a base limpa."""
    pedido = _obter_pendente(s, solicitacao_id)

    descricao = _normalizar(dados.get("descricao") or pedido.descricao)
    if len(descricao) < 3:
        raise ErroValidacao("Dê um nome ao insumo (mínimo 3 letras).")
    existente = _insumo_parecido(s, descricao)
    if existente is not None:
        raise ErroValidacao(
            f"Já existe o insumo {existente.codigo} · {existente.descricao}.")
    if not dados.get("categoria_insumo_id"):
        raise ErroValidacao("Escolha a categoria de insumo.")
    if not dados.get("categoria_id"):
        raise ErroValidacao(
            "Escolha a conta do plano financeiro. Sem ela, o pedido de compra "
            "não vira previsão de pagamento apropriada.")

    insumo = Insumo(
        codigo=proximo_codigo(s),
        descricao=descricao,
        categoria_insumo_id=int(dados["categoria_insumo_id"]),
        categoria_id=int(dados["categoria_id"]),
        unidade=(dados.get("unidade") or pedido.unidade or "").strip().upper() or None)
    s.add(insumo)
    s.flush()

    pedido.status = StatusSolicitacaoInsumo.CADASTRADO
    pedido.insumo_id = insumo.id
    pedido.decidido_por = usuario.id
    pedido.decidido_em = _agora()
    liberados = soltar_pedidos_que_esperavam(s, pedido.id, insumo, usuario)
    registrar_evento(s, "insumo_solicitacao", pedido.id, "CADASTRADO",
                     {"insumo_id": insumo.id, "codigo": insumo.codigo,
                      "descricao": descricao,
                      "itens_de_pedido_liberados": liberados}, usuario.id)
    avisar_solicitante(s, pedido)
    return insumo


def recusar(s: Session, solicitacao_id: int, motivo: str,
            usuario: Usuario) -> InsumoSolicitacao:
    """Recusa COM motivo. Recusa sem motivo faz a pessoa pedir de novo amanhã."""
    pedido = _obter_pendente(s, solicitacao_id)
    motivo = _normalizar(motivo)
    if len(motivo) < 5:
        raise ErroValidacao("Escreva o motivo da recusa — quem pediu precisa saber.")
    pedido.status = StatusSolicitacaoInsumo.RECUSADO
    pedido.motivo = motivo
    pedido.decidido_por = usuario.id
    pedido.decidido_em = _agora()
    if _tem_fila_de_espera(s):
        from sqlalchemy import text
        s.execute(text(
            "UPDATE suprimento_itens_esperando_insumo SET recusado_motivo = :m "
            "WHERE insumo_solicitacao_id = :p AND item_id IS NULL"),
            {"m": motivo, "p": pedido.id})
    registrar_evento(s, "insumo_solicitacao", pedido.id, "RECUSADO",
                     {"motivo": motivo}, usuario.id)
    avisar_solicitante(s, pedido)
    return pedido


def proximo_codigo(s: Session) -> str:
    """Continua a numeração INS-0001 de onde parou."""
    numeros = []
    for insumo in s.scalars(select(Insumo)).all():
        codigo = getattr(insumo, "codigo", "") or ""
        sufixo = codigo.split("-", 1)[1] if codigo.startswith("INS-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"INS-{(max(numeros) + 1) if numeros else 1:04d}"


def avisar_solicitante(s: Session, pedido: InsumoSolicitacao) -> bool:
    """Telegram para quem pediu. O aviso é o que fecha o ciclo: sem ele, a
    pessoa fica perguntando no corredor se já saiu.

    Falha de envio NÃO derruba a decisão — o cadastro já está feito, e o
    `avisado_em` em branco mostra quem ficou sem aviso.
    """
    quem = s.get(Usuario, pedido.solicitante_id)
    if quem is None or not (getattr(quem, "telefone", None) or getattr(quem, "cpf", None)):
        return False
    if pedido.status is StatusSolicitacaoInsumo.CADASTRADO:
        texto = (f"✅ O insumo *{pedido.descricao}* que você pediu foi cadastrado. "
                 f"Já pode usar na solicitação de suprimentos.")
    else:
        texto = (f"❌ O cadastro do insumo *{pedido.descricao}* não foi feito.\n"
                 f"Motivo: {pedido.motivo}")
    try:
        from app.apps.notificador import enviar_telegram
        r = enviar_telegram(telefone=quem.telefone, cpf=quem.cpf, mensagem=texto,
                            finalidade="erp.insumos")
    except Exception as e:
        logger.warning("ERP/suprimentos: aviso de cadastro não saiu (%s)", e)
        return False
    if r and r.get("ok"):
        pedido.avisado_em = _agora()
        return True
    return False


def listar(s: Session, usuario: Usuario, apenas_pendentes: bool = False) -> list[dict[str, Any]]:
    """Os pedidos, com quem pediu e o que foi decidido."""
    pedidos = s.scalars(select(InsumoSolicitacao)).all()
    if apenas_pendentes:
        pedidos = [p for p in pedidos if p.status is StatusSolicitacaoInsumo.PENDENTE]
    # Quantos pedidos de material estão parados esperando cada cadastro — é o
    # que diz a quem cadastra o que é urgente.
    esperando: dict[int, list[str]] = {}
    if _tem_fila_de_espera(s):
        from sqlalchemy import text
        for l in s.execute(text("""
                SELECT e.insumo_solicitacao_id AS c, e.quantidade, e.unidade,
                       o.codigo AS obra, ss.numero
                  FROM suprimento_itens_esperando_insumo e
                  JOIN obras o ON o.id = e.obra_id
                  JOIN suprimento_solicitacoes ss ON ss.id = e.solicitacao_id
                 WHERE e.item_id IS NULL AND e.recusado_motivo IS NULL""")).mappings().all():
            esperando.setdefault(l["c"], []).append(
                f"{l['numero']}: {float(l['quantidade']):g} {l['unidade']} para {l['obra']}")
    saida = []
    for p in sorted(pedidos, key=lambda x: (x.id or 0), reverse=True):
        quem = s.get(Usuario, p.solicitante_id)
        saida.append({
            "pedidos_esperando": esperando.get(p.id, []),
            "id": p.id, "descricao": p.descricao, "justificativa": p.justificativa,
            "unidade": p.unidade, "status": p.status.value,
            "solicitante": getattr(quem, "nome", ""), "obra_id": p.obra_id,
            "insumo_id": p.insumo_id, "motivo": p.motivo,
            "avisado": p.avisado_em is not None,
            "criado_em": p.criado_em.isoformat() if p.criado_em else None,
        })
    return saida


# ---------------------------------------------------------------------------
# O PEDIDO QUE NÃO ESPERA O CADASTRO DO INSUMO (23/11/2026, migração 125)
#
# O dono: "quando não tem um insumo, quem vai lançar o pedido fica meio
# travado (...) O que a gente poderia adicionar? Um botão: não localizou o
# insumo, solicite o cadastramento. Aí ele faz o pedido já incluindo aquele
# cadastro de insumo (...) aquele pedido fica pendente, aguardando a
# confirmação do cadastro. Na hora que a pessoa responsável liberar, o pedido
# já fica pronto, e liberado para o setor de suprimentos iniciar os trabalhos."
#
# A fila é uma tabela lida por SQL direto, e não um modelo: assim, se o código
# subir antes de a migração 125 ser aplicada, o cadastro de insumo continua
# funcionando — só o recurso novo é que espera o botão.
# ---------------------------------------------------------------------------
def _tem_fila_de_espera(s: Session) -> bool:
    """A tabela da migração 125 existe? Falso também em sessão dublada."""
    from sqlalchemy import text
    try:
        with s.begin_nested():
            return s.execute(text(
                "SELECT to_regclass('suprimento_itens_esperando_insumo')")).scalar() is not None
    except Exception:
        return False


def pedir_junto_com_o_pedido(s: Session, sol, novo: dict[str, Any],
                             usuario: Usuario) -> int:
    """Um material que ainda não está no cadastro, pedido dentro de uma
    solicitação. Abre (ou reaproveita) o pedido de cadastro do insumo e deixa a
    linha esperando por ele."""
    from decimal import Decimal, InvalidOperation
    from sqlalchemy import text
    from app.apps.erp.db.models.cadastros import Obra, UnidadeCompra

    if not _tem_fila_de_espera(s):
        raise ErroValidacao(
            "O banco está desatualizado para pedir material fora do cadastro. "
            "Peça ao administrador para aplicar as atualizações do banco.")
    descricao = _normalizar(novo.get("descricao") or "")
    explicacao = _normalizar(novo.get("explicacao") or "")
    if len(descricao) < 3:
        raise ErroValidacao("Diga o nome do material que não está no cadastro.")
    if len(explicacao) < 15:
        raise ErroValidacao(
            f"Explique o que é \"{descricao}\" e para que serve — quem vai cadastrar "
            "precisa entender o material sem ter de perguntar.")
    bruto = str(novo.get("quantidade") or "").strip()
    if "," in bruto:
        bruto = bruto.replace(".", "").replace(",", ".")
    try:
        quantidade = Decimal(bruto)
    except (InvalidOperation, ValueError):
        raise ErroValidacao(f"{descricao}: quantidade inválida.")
    if quantidade <= 0:
        raise ErroValidacao(f"{descricao}: a quantidade tem de ser maior que zero.")
    unidade = (novo.get("unidade") or "").strip().upper()
    if not unidade or s.get(UnidadeCompra, unidade) is None:
        raise ErroValidacao(f"{descricao}: escolha a unidade.")
    obra = s.get(Obra, int(novo["obra_id"])) if novo.get("obra_id") else None
    if obra is None:
        raise ErroValidacao(f"{descricao}: escolha a obra que vai receber.")

    # Mesmo material já pedido e ainda sem decisão: entra na mesma fila, em vez
    # de recusar o pedido da obra por causa de um pedido de cadastro repetido.
    cadastro = next((p for p in s.scalars(select(InsumoSolicitacao).where(
        InsumoSolicitacao.status == StatusSolicitacaoInsumo.PENDENTE)).all()
        if p.status is StatusSolicitacaoInsumo.PENDENTE
        and _normalizar(p.descricao).lower() == descricao.lower()), None)
    if cadastro is None:
        cadastro = solicitar(s, {"descricao": descricao, "justificativa": explicacao,
                                 "unidade": unidade, "obra_id": obra.id}, usuario)
    s.execute(text(
        "INSERT INTO suprimento_itens_esperando_insumo "
        "(insumo_solicitacao_id, solicitacao_id, obra_id, quantidade, unidade, "
        " especificacao, criado_por) VALUES (:c, :s, :o, :q, :u, :e, :p)"),
        {"c": cadastro.id, "s": sol.id, "o": obra.id, "q": quantidade, "u": unidade,
         "e": _normalizar(novo.get("especificacao") or "") or None, "p": usuario.id})
    return cadastro.id


def soltar_pedidos_que_esperavam(s: Session, cadastro_id: int, insumo: Insumo,
                                 usuario: Usuario) -> int:
    """O insumo foi cadastrado: cada linha que esperava vira item de verdade na
    solicitação dela. O ok da engenharia vale como em qualquer pedido — quem
    pediu sem alçada continua precisando dele."""
    if not _tem_fila_de_espera(s):
        return 0
    from sqlalchemy import func, text
    from app.apps.erp.core.auth.permissoes import pode
    from app.apps.erp.db.models.cadastros import StatusItemSuprimento, SuprimentoItem
    linhas = s.execute(text(
        "SELECT id, solicitacao_id, obra_id, quantidade, unidade, especificacao, criado_por "
        "FROM suprimento_itens_esperando_insumo "
        "WHERE insumo_solicitacao_id = :c AND item_id IS NULL AND recusado_motivo IS NULL "
        "ORDER BY id"), {"c": cadastro_id}).all()
    for linha in linhas:
        numero = (s.execute(select(func.max(SuprimentoItem.numero)).where(
            SuprimentoItem.solicitacao_id == linha.solicitacao_id)).scalar() or 0) + 1
        quem_pediu = s.get(Usuario, linha.criado_por)
        libera = quem_pediu is not None and pode(quem_pediu, "liberar_solicitacao")
        item = SuprimentoItem(
            solicitacao_id=linha.solicitacao_id, numero=numero, insumo_id=insumo.id,
            especificacao=linha.especificacao, quantidade=linha.quantidade,
            unidade=linha.unidade, obra_id=linha.obra_id,
            status=StatusItemSuprimento.SOLICITACAO,
            liberacao="LIBERADO" if libera else "PENDENTE",
            liberado_por=quem_pediu.id if libera else None,
            liberado_em=_agora() if libera else None)
        s.add(item)
        s.flush()
        s.execute(text("UPDATE suprimento_itens_esperando_insumo SET item_id = :i WHERE id = :l"),
                  {"i": item.id, "l": linha.id})
        registrar_evento(s, "suprimento_item", item.id, "CRIADO_PELO_CADASTRO_DO_INSUMO",
                         {"insumo_solicitacao_id": cadastro_id, "codigo": insumo.codigo},
                         usuario.id)
    return len(linhas)


def esperando_cadastro(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """O que a obra pediu e ainda espera o insumo entrar no cadastro — e o que
    foi recusado, com o motivo. No recorte de obra de quem olha, mais o que a
    própria pessoa pediu."""
    if not _tem_fila_de_espera(s):
        return []
    from sqlalchemy import text
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    permitidas = obras_de_registro_sem_autor(s, usuario)
    linhas = s.execute(text("""
        SELECT e.id, e.quantidade, e.unidade, e.especificacao, e.obra_id, e.criado_por,
               e.recusado_motivo, c.descricao, c.justificativa, c.id AS cadastro_id,
               o.codigo AS obra, ss.numero AS solicitacao, ss.titulo
          FROM suprimento_itens_esperando_insumo e
          JOIN insumo_solicitacoes c ON c.id = e.insumo_solicitacao_id
          JOIN obras o ON o.id = e.obra_id
          JOIN suprimento_solicitacoes ss ON ss.id = e.solicitacao_id
         WHERE e.item_id IS NULL
         ORDER BY e.id DESC LIMIT 300""")).mappings().all()
    return [{"id": l["id"], "material": l["descricao"], "explicacao": l["justificativa"],
             "quantidade": float(l["quantidade"]), "unidade": l["unidade"],
             "especificacao": l["especificacao"] or "", "obra": l["obra"],
             "solicitacao": l["solicitacao"], "titulo": l["titulo"],
             "cadastro_id": l["cadastro_id"], "recusado_motivo": l["recusado_motivo"]}
            for l in linhas
            if permitidas is None or l["obra_id"] in permitidas
            or l["criado_por"] == usuario.id]
