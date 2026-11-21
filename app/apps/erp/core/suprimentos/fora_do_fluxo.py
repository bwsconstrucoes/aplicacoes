# ============================================================================
# ERP — core/suprimentos/fora_do_fluxo.py
# Material comprado sem passar pelo suprimento: ciência e autorização.
#
# O dono, em 21/11/2026: "a gente tem que tentar inibir a possibilidade de uma
# compra sem ter seguido o processo do suprimento. Imagina um engenheiro da
# obra que foi lá e comprou um material e esse material não passou pelo
# suprimento (…) alguém autorizar esse lançamento para o financeiro."
#
# COMO FUNCIONA. Quando a conta de MATERIAL é lançada sem estar ligada a um
# pedido de compra:
#   1. quem lança é obrigado a escrever POR QUE não passou pelo suprimento;
#   2. o título nasce BLOQUEADO — não pode ser aprovado nem pago;
#   3. ele aparece no Painel de suprimentos, para ciência do coordenador;
#   4. alguém com a alçada de autorizar (e que não seja quem lançou) autoriza,
#      e só então o título segue para a aprovação normal do financeiro.
#
# ⚑ NASCE DESLIGADA, E ISSO É O PONTO. Hoje nenhuma compra da empresa passa
# pelo ERP — ligar a regra antes da hora marcaria TODO lançamento de material
# como compra por fora e pararia o financeiro. A chave fica no Painel; ligada,
# vale para o que for lançado dali em diante.
#
# O que conta como "material": o título do tipo material com nota
# (T1_MATERIAL_NFE). Serviço, locação, empreita, folha e tributo têm fluxo
# próprio e não passam pelo suprimento.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Fornecedor, Obra, Parametro, PedidoTitulo, TipoTitulo, Usuario,
)

logger = logging.getLogger(__name__)

CHAVE = "suprimentos_exigir_pedido_desde"
TIPOS_DE_MATERIAL = (TipoTitulo.T1_MATERIAL_NFE,)


class PrecisaDoMotivo(ErroValidacao):
    """O lançamento é de material sem pedido e ainda não veio com o motivo."""


# ⚑ DOIS MODOS (21/11/2026, noite). Ele: "o que não seguir o processo poderemos
# por enquanto avisar, sem travar. Depois a gente trava, quando tiver tudo
# rodando já."
#   AVISAR  o lançamento entra normalmente; fica registrado e aparece no Painel
#           para o coordenador tomar ciência. Não pede motivo, não bloqueia.
#   TRAVAR  pede o motivo e bloqueia até alguém com alçada autorizar.
MODOS = ("AVISAR", "TRAVAR")


def estado(s: Session) -> tuple[Optional[str], Optional[date]]:
    """(modo, desde quando) — ou (None, None) com a regra desligada."""
    linha = s.get(Parametro, CHAVE)
    valor = (linha.valor or "").strip() if linha else ""
    if not valor:
        return None, None
    modo, _, quando = valor.partition(":") if ":" in valor else ("TRAVAR", "", valor)
    try:
        return (modo if modo in MODOS else "TRAVAR"), date.fromisoformat(quando)
    except ValueError:
        return None, None


def ligado_desde(s: Session) -> Optional[date]:
    return estado(s)[1]


def ligar(s: Session, usuario: Usuario, ligar: Any) -> Optional[date]:
    """`ligar` é o modo ("AVISAR" ou "TRAVAR"), ou falso para desligar.
    `True` continua valendo TRAVAR, como era."""
    modo = ("TRAVAR" if ligar is True else str(ligar or "").strip().upper())
    modo = modo if modo in MODOS else ""
    # Confere que a tabela existe ANTES de ligar: com a chave ligada e o banco
    # desatualizado, todo lançamento de material quebraria.
    if modo:
        try:
            s.execute(text("SELECT 1 FROM compras_fora_do_fluxo LIMIT 1"))
        except Exception:
            s.rollback()
            raise ErroValidacao(
                "O banco está desatualizado. Aperte \"Aplicar atualizações do banco\" "
                "em Configurações e ligue de novo.")
    valor = f"{modo}:{date.today().isoformat()}" if modo else ""
    linha = s.get(Parametro, CHAVE)
    if linha is None:
        s.add(Parametro(chave=CHAVE, valor=valor, atualizado_por=usuario.id))
    else:
        linha.valor = valor
        linha.atualizado_por = usuario.id
    s.flush()
    registrar_evento(s, "parametro", 0, "EXIGIR_PEDIDO_DE_COMPRA",
                     {"modo": modo or "DESLIGADO"}, usuario.id)
    return ligado_desde(s)


def conferir(s: Session, titulo, dados: dict[str, Any], usuario: Usuario) -> bool:
    """Chamado logo depois de o título nascer. Devolve True se ele ficou
    retido como compra fora do fluxo."""
    if ligado_desde(s) is None or titulo.tipo not in TIPOS_DE_MATERIAL:
        return False
    tem_pedido = s.scalars(select(PedidoTitulo).where(
        PedidoTitulo.titulo_id == titulo.id)).first()
    if tem_pedido is not None:
        return False
    motivo = " ".join(str(dados.get("motivo_sem_pedido") or "").split())
    if estado(s)[0] == "AVISAR":
        # Só avisa: o lançamento segue o caminho normal do financeiro, e fica
        # registrado para o coordenador de suprimentos tomar ciência no Painel.
        s.execute(text(
            "INSERT INTO compras_fora_do_fluxo (titulo_id, motivo, lancado_por) "
            "VALUES (:t, :m, :u)"),
            {"t": titulo.id, "m": motivo or "(motivo não informado)", "u": usuario.id})
        registrar_evento(s, "titulo", titulo.id, "COMPRA_FORA_DO_FLUXO",
                         {"motivo": motivo or None, "modo": "AVISAR"}, usuario.id)
        return True
    if len(motivo) < 10:
        raise PrecisaDoMotivo(
            "Este material não está ligado a nenhum pedido de compra. Explique por "
            "que a compra não passou pelo suprimento — o lançamento vai para "
            "autorização antes de seguir para o financeiro.")
    from app.apps.erp.db.models.financeiro import StatusTitulo
    s.execute(text(
        "INSERT INTO compras_fora_do_fluxo (titulo_id, motivo, lancado_por) "
        "VALUES (:t, :m, :u)"), {"t": titulo.id, "m": motivo, "u": usuario.id})
    titulo.status = StatusTitulo.BLOQUEADO
    registrar_evento(s, "titulo", titulo.id, "COMPRA_FORA_DO_FLUXO",
                     {"motivo": motivo}, usuario.id)
    logger.info("ERP/suprimentos: título %s retido — material sem pedido de compra",
                titulo.numero_sp)
    return True


def pendente(s: Session, titulo_id: int) -> Optional[str]:
    """O motivo, se este título está esperando autorização por ter sido
    lançado sem pedido de compra. É o que o motor de análise pergunta."""
    if estado(s)[0] != "TRAVAR":
        return None          # desligada, ou só avisando: não bloqueia ninguém
    linha = s.execute(text(
        "SELECT motivo FROM compras_fora_do_fluxo "
        "WHERE titulo_id = :t AND autorizado_em IS NULL"), {"t": titulo_id}).first()
    return linha[0] if linha else None


def listar(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """O que está esperando autorização, no recorte de obra de quem olha."""
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    from app.apps.erp.db.models.financeiro import Rateio, Titulo
    linhas = s.execute(text(
        "SELECT titulo_id, motivo, lancado_por, criado_em FROM compras_fora_do_fluxo "
        "WHERE autorizado_em IS NULL ORDER BY criado_em")).all()
    if not linhas:
        return []
    ids = [l[0] for l in linhas]
    titulos = {t.id: t for t in s.scalars(select(Titulo).where(Titulo.id.in_(ids))).all()}
    obras_de: dict[int, set[int]] = {}
    for r in s.scalars(select(Rateio).where(Rateio.titulo_id.in_(ids))).all():
        obras_de.setdefault(r.titulo_id, set()).add(r.obra_id)
    nomes_obra = {o.id: o.codigo for o in s.scalars(select(Obra).where(
        Obra.id.in_({x for v in obras_de.values() for x in v} or {0}))).all()}
    fornecedores = {f.id: f.razao_social for f in s.scalars(select(Fornecedor).where(
        Fornecedor.id.in_({t.fornecedor_id for t in titulos.values()} or {0}))).all()}
    quem = {u.id: u.nome for u in s.scalars(select(Usuario).where(
        Usuario.id.in_({l[2] for l in linhas if l[2]} or {0}))).all()}
    permitidas = obras_de_registro_sem_autor(s, usuario)
    saida = []
    for titulo_id, motivo, lancado_por, criado_em in linhas:
        t = titulos.get(titulo_id)
        if t is None or getattr(t.status, "value", "") in ("CANCELADO", "ESTORNADO"):
            continue
        obras = obras_de.get(titulo_id, set())
        if permitidas is not None and not (obras & set(permitidas)):
            continue
        saida.append({
            "titulo_id": t.id, "numero_sp": t.numero_sp, "descricao": t.descricao,
            "fornecedor": fornecedores.get(t.fornecedor_id, ""),
            "valor": float(t.valor_liquido),
            "obras": sorted(nomes_obra.get(o, "") for o in obras),
            "motivo": motivo, "lancado_por": quem.get(lancado_por, ""),
            "lancado_por_id": lancado_por,
            "dia": criado_em.date().isoformat() if criado_em else None})
    return saida


def autorizar(s: Session, titulo_id: int, usuario: Usuario, observacao: str = "") -> None:
    """Dá ciência e libera o lançamento para seguir ao financeiro."""
    from app.apps.erp.core.titulos import service as svc_titulos
    from app.apps.erp.db.models.financeiro import StatusTitulo
    linha = s.execute(text(
        "SELECT lancado_por FROM compras_fora_do_fluxo "
        "WHERE titulo_id = :t AND autorizado_em IS NULL"), {"t": titulo_id}).first()
    if linha is None:
        raise ErroNaoEncontrado("Não há lançamento esperando autorização com este número.")
    if linha[0] == usuario.id:
        raise ErroValidacao(
            "Quem lançou não autoriza o próprio lançamento. Peça a outra pessoa "
            "com alçada.")
    s.execute(text(
        "UPDATE compras_fora_do_fluxo SET autorizado_por = :u, autorizado_em = :q, "
        "observacao = :o WHERE titulo_id = :t"),
        {"u": usuario.id, "q": datetime.now(timezone.utc),
         "o": " ".join((observacao or "").split()) or None, "t": titulo_id})
    registrar_evento(s, "titulo", titulo_id, "COMPRA_FORA_DO_FLUXO_AUTORIZADA",
                     {"observacao": observacao or None}, usuario.id)
    titulo = svc_titulos.obter(s, titulo_id)
    if titulo.status is StatusTitulo.BLOQUEADO:
        # volta para a análise: se não houver OUTRO motivo de bloqueio (conta
        # do credor sem conferência, por exemplo), segue para a aprovação.
        svc_titulos.reanalisar(s, titulo_id, usuario)
