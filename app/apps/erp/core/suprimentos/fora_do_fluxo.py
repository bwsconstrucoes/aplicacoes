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
    # ⚑ DUAS PENDÊNCIAS, E A LINHA SÓ SAI QUANDO AS DUAS FECHAM (21/11/2026):
    # alguém com alçada deu ciência, E alguém registrou o que foi comprado (a
    # solicitação tardia). Uma não espera a outra — o pagamento não fica refém
    # de digitação, e o registro do material não some por já ter sido pago.
    linhas = s.execute(text(
        "SELECT titulo_id, motivo, lancado_por, criado_em, autorizado_em, pedido_id, itens "
        "FROM compras_fora_do_fluxo "
        "WHERE autorizado_em IS NULL OR (pedido_id IS NULL AND itens IS NULL) "
        "ORDER BY criado_em")).all()
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
    for titulo_id, motivo, lancado_por, criado_em, autorizado_em, pedido_id, itens in linhas:
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
            "ciente": autorizado_em is not None,
            "regularizado": pedido_id is not None or bool(itens),
            # o que quem vai dar a ciência precisa ler antes de dar
            "material_informado": [
                {"insumo": i.get("insumo"), "quantidade": i.get("quantidade"),
                 "unidade": i.get("unidade"), "preco_unitario": i.get("preco_unitario")}
                for i in (itens or [])],
            "fornecedor_id": t.fornecedor_id,
            "obra_ids": sorted(obras),
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
    # A ciência é o que faz o pedido tardio nascer, no nome de quem a deu.
    informado = s.execute(text(
        "SELECT itens, pedido_id FROM compras_fora_do_fluxo WHERE titulo_id = :t"),
        {"t": titulo_id}).first()
    if informado and informado[0] and informado[1] is None:
        _concretizar(s, titulo_id, list(informado[0]), usuario, usuario)
    registrar_evento(s, "titulo", titulo_id, "COMPRA_FORA_DO_FLUXO_AUTORIZADA",
                     {"observacao": observacao or None}, usuario.id)
    titulo = svc_titulos.obter(s, titulo_id)
    if titulo.status is StatusTitulo.BLOQUEADO:
        # volta para a análise: se não houver OUTRO motivo de bloqueio (conta
        # do credor sem conferência, por exemplo), segue para a aprovação.
        svc_titulos.reanalisar(s, titulo_id, usuario)


# ---------------------------------------------------------------------------
# A SOLICITAÇÃO TARDIA (21/11/2026, noite)
#
# O dono: "Como é que a gente tem feito aqui? É exigido o lançamento da
# solicitação, mesmo que tardia. Porque pode acontecer: surgiu uma urgência,
# foi comprado e não seguiu o procedimento normal. O engenheiro precisou
# comprar. Aí, até para a gente ter o registro do que está sendo comprado, o
# quantitativo de material que está sendo utilizado em obra, a gente precisa
# lançar aquela solicitação de suprimento. Mesmo que a posterior, para poder
# cobrir esse buraco."
#
# UM REGISTRO SÓ, E O SISTEMA FAZ O RESTO. A pessoa diz o que foi comprado
# (insumo, quantidade, preço); daí nascem, na mesma transação:
#   · a solicitação de suprimento, marcada como regularização;
#   · o pedido de compra, já autorizado — a compra JÁ ACONTECEU, autorizar de
#     novo seria teatro; a ciência de quem tem alçada é a do Painel;
#   · o recebimento integral — o material já está na obra;
#   · o vínculo com o lançamento que já existe.
# Com isso a compra por fora entra no histórico de preços, no consumo da obra
# e nos relatórios, igual às outras. NÃO cria conta a pagar: ela já existe.
# ---------------------------------------------------------------------------
def _numero(valor: Any, campo: str):
    from decimal import Decimal, InvalidOperation
    bruto = str(valor or "").strip()
    if "," in bruto:
        bruto = bruto.replace(".", "").replace(",", ".")
    try:
        return Decimal(bruto)
    except (InvalidOperation, ValueError):
        raise ErroValidacao(f"{campo}: número inválido.")


def regularizar(s: Session, titulo_id: int, dados: dict[str, Any],
                usuario: Usuario) -> dict[str, Any]:
    """Registra o que foi comprado por fora.

    ⚑ QUEM REGISTRA NÃO É QUEM AUTORIZA (22/11/2026). O dono: "a pessoa não
    pode simplesmente lançar e o financeiro pagar (...) tem que ir para
    autorização também. Mesmo que tardio. Porque a pessoa que autoriza
    participa, tem ciência de algo que não seguiu o procedimento."

    Por isso são dois tempos:
      1. quem lançou INFORMA os materiais — ficam guardados, à vista de quem
         vai dar a ciência;
      2. quando alguém com alçada dá a ciência, aí sim nascem a solicitação, o
         pedido (autorizado NO NOME DELE), o recebimento e o vínculo.
    Se a ciência já tinha sido dada antes, tudo nasce na hora, no nome de quem
    a deu. O banco não aceita pedido autorizado sem o nome de quem autorizou —
    e é essa regra que impede o registro de se autorizar sozinho.
    """
    import json
    from app.apps.erp.db.models.cadastros import Insumo
    from app.apps.erp.db.models.financeiro import Rateio

    linha = s.execute(text(
        "SELECT pedido_id, itens, autorizado_por FROM compras_fora_do_fluxo "
        "WHERE titulo_id = :t"), {"t": titulo_id}).first()
    if linha is None:
        raise ErroNaoEncontrado("Este lançamento não está marcado como compra sem pedido.")
    if linha[0] is not None or linha[1] is not None:
        raise ErroValidacao("O que foi comprado neste lançamento já está registrado.")

    rateios = [r for r in s.scalars(select(Rateio).where(
        Rateio.titulo_id == titulo_id)).all() if r.titulo_id == titulo_id]
    obra_padrao = max(rateios, key=lambda r: r.valor).obra_id if rateios else None
    brutos = dados.get("itens") or []
    if not brutos:
        raise ErroValidacao("Diga ao menos um material que foi comprado.")
    if obra_padrao is None and any(not b.get("obra_id") for b in brutos):
        raise ErroValidacao(
            "O lançamento não está rateado em nenhuma obra, então não dá para saber "
            "para onde foi o material. Acerte o rateio do lançamento e registre de novo.")
    itens = []
    for n, b in enumerate(brutos, start=1):
        insumo = s.get(Insumo, int(b.get("insumo_id") or 0))
        if insumo is None:
            raise ErroValidacao(f"Item {n}: escolha um material do cadastro.")
        preco = _numero(b.get("preco_unitario"), f"Item {n}: preço")
        quantidade = _numero(b.get("quantidade"), f"Item {n}: quantidade")
        if preco <= 0 or quantidade <= 0:
            raise ErroValidacao(f"Item {n}: quantidade e preço têm de ser maiores que zero.")
        itens.append({"insumo_id": insumo.id, "insumo": insumo.descricao,
                      "obra_id": int(b.get("obra_id") or obra_padrao),
                      "quantidade": str(quantidade), "preco_unitario": str(preco),
                      "unidade": (b.get("unidade") or insumo.unidade or "").strip().upper(),
                      "especificacao": (b.get("especificacao") or "").strip() or None})

    s.execute(text(
        "UPDATE compras_fora_do_fluxo SET itens = CAST(:i AS JSONB), "
        "regularizado_por = :u, regularizado_em = :q WHERE titulo_id = :t"),
        {"i": json.dumps(itens), "u": usuario.id,
         "q": datetime.now(timezone.utc), "t": titulo_id})
    registrar_evento(s, "titulo", titulo_id, "COMPRA_FORA_DO_FLUXO_MATERIAL_INFORMADO",
                     {"itens": len(itens)}, usuario.id)
    if linha[2] is None:
        return {"aguardando_ciencia": True, "pedido": None, "solicitacao": None}
    quem_autorizou = s.get(Usuario, linha[2])
    return {"aguardando_ciencia": False,
            **_concretizar(s, titulo_id, itens, usuario, quem_autorizou)}


def _concretizar(s: Session, titulo_id: int, itens: list[dict[str, Any]],
                 usuario: Usuario, quem_autorizou: Usuario) -> dict[str, Any]:
    """Com os materiais informados E a ciência dada: cria a solicitação, o
    pedido (autorizado por quem deu a ciência), o recebimento e o vínculo."""
    from app.apps.erp.core.suprimentos import lancamento, pedido as svc_pedido
    from app.apps.erp.core.suprimentos import recebimento, solicitacao
    from app.apps.erp.core.titulos import service as svc_titulos
    from app.apps.erp.db.models.cadastros import PedidoItem, SuprimentoItem

    titulo = svc_titulos.obter(s, titulo_id)
    if not titulo.fornecedor_id:
        raise ErroValidacao("O lançamento não tem fornecedor — não há de quem registrar a compra.")
    motivo = s.execute(text("SELECT motivo FROM compras_fora_do_fluxo WHERE titulo_id = :t"),
                       {"t": titulo_id}).scalar()
    marca = f"REGULARIZAÇÃO da compra lançada em {titulo.numero_sp}"
    sol = solicitacao.criar(s, {
        "titulo": f"Regularização — {titulo.numero_sp}",
        "observacoes": f"{marca}. Motivo informado: {motivo}",
        "itens": [{"insumo_id": i["insumo_id"], "obra_id": i["obra_id"],
                   "quantidade": i["quantidade"], "unidade": i["unidade"],
                   "especificacao": i.get("especificacao"),
                   "observacoes": "Compra feita fora do processo — registro tardio."}
                  for i in itens]}, usuario, dispensa_liberacao=True)
    do_pedido = sorted([i for i in s.scalars(select(SuprimentoItem).where(
        SuprimentoItem.solicitacao_id == sol.id)).all() if i.solicitacao_id == sol.id],
        key=lambda i: i.numero)
    pedido = svc_pedido.fechar_direto(s, {
        "fornecedor_id": titulo.fornecedor_id, "observacoes": marca + ".",
        "itens": [{"suprimento_item_id": i.id, "preco_unitario": bruto["preco_unitario"]}
                  for i, bruto in zip(do_pedido, itens)]}, usuario)
    svc_pedido.autorizar(s, pedido.id, quem_autorizou)
    linhas = [l for l in s.scalars(select(PedidoItem).where(
        PedidoItem.pedido_id == pedido.id)).all() if l.pedido_id == pedido.id]
    chegou = titulo.data_emissao_doc or date.today()
    rec = recebimento.registrar(s, pedido.id, {
        "data": min(chegou, date.today()).isoformat(),
        "nota_numero": getattr(titulo, "documento_numero", None) or "",
        "observacoes": marca + " — material já entregue quando foi registrado.",
        "itens": [{"pedido_item_id": l.id, "quantidade": str(l.quantidade)}
                  for l in linhas]}, usuario)
    s.flush()
    lancamento.vincular(s, titulo, pedido.id, usuario, recebimento_id=rec.id)
    s.execute(text("UPDATE compras_fora_do_fluxo SET pedido_id = :p WHERE titulo_id = :t"),
              {"p": pedido.id, "t": titulo_id})
    total = svc_pedido.total(s, pedido)
    registrar_evento(s, "titulo", titulo_id, "COMPRA_FORA_DO_FLUXO_REGULARIZADA",
                     {"solicitacao": sol.numero, "pedido": pedido.numero,
                      "total_dos_itens": str(total),
                      "valor_do_lancamento": str(titulo.valor_liquido),
                      "autorizado_por": quem_autorizou.id}, usuario.id)
    logger.info("ERP/suprimentos: compra sem pedido %s regularizada — %s / %s",
                titulo.numero_sp, sol.numero, pedido.numero)
    return {"solicitacao": sol.numero, "pedido": pedido.numero, "pedido_id": pedido.id,
            "total_dos_itens": float(total), "valor_do_lancamento": float(titulo.valor_liquido)}
