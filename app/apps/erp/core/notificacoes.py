# ============================================================================
# ERP — core/notificacoes.py
# Avisa quem lançou quando o título é pago — com o comprovante junto, como o
# baixabradesco faz hoje. Reusa o `app/apps/notificador.py` do monorepo, que
# já resolve o destinatário por telefone ou CPF e respeita os toggles
# NOTIFICAR_TELEGRAM / NOTIFICAR_WHATSAPP.
#
# O cuidado que o Marcelo pediu — não mandar duas vezes:
#   Cada envio tem uma REFERÊNCIA idempotente. Para a baixa, a referência é o
#   par título + parcela + valor + data do pagamento. Se a baixa for desfeita e
#   refeita igual (correção de digitação em outro campo, reprocessamento do
#   mesmo comprovante), a referência é a mesma e o aviso NÃO sai de novo.
#   Se a correção mudou o que interessa ao solicitante (valor ou data), a
#   referência muda e o aviso sai — agora marcado como CORREÇÃO, para a pessoa
#   entender que substitui o anterior.
# ============================================================================
from __future__ import annotations

import base64
import hashlib
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import (
    Anexo, Pagamento, Parcela, Rateio, Titulo,
)

logger = logging.getLogger(__name__)

MAX_ANEXO_ENVIO = 8 * 1024 * 1024      # acima disso manda só o texto


def _moeda(v: Any) -> str:
    return f"R$ {Decimal(str(v)):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _referencia_baixa(titulo_id: int, parcela_numero: int, valor: Any, data: Any) -> str:
    """Muda quando muda o que importa ao solicitante: valor ou data."""
    bruto = f"baixa|{titulo_id}|{parcela_numero}|{Decimal(str(valor)):.2f}|{data}"
    return hashlib.sha256(bruto.encode()).hexdigest()[:32]


def _ja_enviado(s: Session, evento: str, referencia: str) -> Optional[dict[str, Any]]:
    linha = s.execute(text(
        "SELECT id, situacao, enviado_em FROM notificacoes "
        "WHERE evento = :e AND referencia = :r"), {"e": evento, "r": referencia}).first()
    if linha is None:
        return None
    return {"id": linha[0], "situacao": linha[1], "enviado_em": linha[2]}


def _registrar(s: Session, *, evento: str, referencia: str, titulo_id: Optional[int],
               pagamento_id: Optional[int], destinatario_id: Optional[int],
               destino: str, situacao: str, mensagem: str, erro: str = "",
               com_anexo: bool = False) -> None:
    s.execute(text(
        "INSERT INTO notificacoes (evento, referencia, titulo_id, pagamento_id, "
        " destinatario_id, destino, canal, situacao, mensagem, erro, com_anexo, "
        " tentativas, enviado_em) "
        "VALUES (:e, :r, :t, :p, :u, :d, 'TELEGRAM', :s, :m, :err, :anexo, 1, :em) "
        "ON CONFLICT (evento, referencia) DO UPDATE SET "
        " situacao = EXCLUDED.situacao, erro = EXCLUDED.erro, "
        " tentativas = notificacoes.tentativas + 1, enviado_em = EXCLUDED.enviado_em"),
        {"e": evento, "r": referencia, "t": titulo_id, "p": pagamento_id,
         "u": destinatario_id, "d": destino, "s": situacao, "m": mensagem[:4000],
         "err": (erro or "")[:1000], "anexo": com_anexo,
         "em": datetime.now(timezone.utc) if situacao == "ENVIADO" else None})


def _texto_baixa(t: Titulo, p: Parcela, pg: Pagamento, obras: str,
                 correcao: bool) -> str:
    cabecalho = "🔁 *Pagamento corrigido*" if correcao else "✅ *Pagamento realizado*"
    linhas = [
        cabecalho,
        "",
        f"*{t.numero_sp}* — {t.fornecedor.razao_social}",
        f"{t.descricao[:120]}",
        "",
        f"💰 Valor: *{_moeda(pg.valor_pago)}*",
        f"📅 Pago em: {pg.data_pagamento:%d/%m/%Y}",
    ]
    if len(t.parcelas) > 1:
        linhas.append(f"📄 Parcela {p.numero} de {len(t.parcelas)}")
    if obras:
        linhas.append(f"🏗️ Obra: {obras}")
    if correcao:
        linhas.append("")
        linhas.append("_Esta baixa substitui o aviso anterior deste título._")
    linhas.append("")
    linhas.append("_Acompanhe pelo ERP: aplicacoes.bwsconstrucoes.com.br/erp_")
    return "\n".join(linhas)


def avisar_baixa(s: Session, pagamento_id: int, *, forcar: bool = False,
                 enviar_comprovante: bool = True) -> dict[str, Any]:
    """Avisa quem lançou o título de que ele foi pago, com o comprovante."""
    pg = s.get(Pagamento, pagamento_id, options=[
        selectinload(Pagamento.parcela).selectinload(Parcela.titulo)
        .selectinload(Titulo.fornecedor)])
    if pg is None:
        return {"ok": False, "motivo": "pagamento não encontrado"}
    p = pg.parcela
    t = p.titulo

    referencia = _referencia_baixa(t.id, p.numero, pg.valor_pago, pg.data_pagamento)
    anterior = _ja_enviado(s, "BAIXA", referencia)
    if anterior and anterior["situacao"] == "ENVIADO" and not forcar:
        logger.info("ERP/aviso: %s já avisado (mesma baixa) — nada enviado", t.numero_sp)
        return {"ok": True, "situacao": "JA_ENVIADO",
                "motivo": "esta mesma baixa já foi avisada",
                "enviado_em": anterior["enviado_em"].strftime("%d/%m/%Y %H:%M")
                              if anterior["enviado_em"] else None}

    # houve aviso anterior deste título com outra referência? então é correção
    houve_outro = s.execute(text(
        "SELECT 1 FROM notificacoes WHERE evento = 'BAIXA' AND titulo_id = :t "
        "AND referencia <> :r AND situacao = 'ENVIADO' LIMIT 1"),
        {"t": t.id, "r": referencia}).first() is not None

    solicitante = s.get(Usuario, t.solicitante_id)
    if solicitante is None or not (solicitante.telefone or solicitante.cpf):
        _registrar(s, evento="BAIXA", referencia=referencia, titulo_id=t.id,
                   pagamento_id=pg.id,
                   destinatario_id=(solicitante.id if solicitante else None),
                   destino="", situacao="IGNORADO", mensagem="",
                   erro="solicitante sem telefone/CPF cadastrado")
        return {"ok": False, "situacao": "SEM_DESTINO",
                "motivo": f"{solicitante.nome if solicitante else 'solicitante'} não tem "
                          f"telefone nem CPF no cadastro — aviso não enviado"}

    obras = " + ".join(sorted({r.obra.codigo for r in
                              s.scalars(select(Rateio).where(Rateio.titulo_id == t.id)
                                        .options(selectinload(Rateio.obra))).all()
                              if r.obra}))
    mensagem = _texto_baixa(t, p, pg, obras, correcao=houve_outro)

    # comprovante anexado à baixa, quando existir e couber
    arquivo_b64 = nome_arquivo = tipo = None
    if enviar_comprovante and pg.comprovante_anexo_id:
        anexo = s.get(Anexo, pg.comprovante_anexo_id)
        if anexo is not None and anexo.conteudo and len(anexo.conteudo) <= MAX_ANEXO_ENVIO:
            arquivo_b64 = base64.b64encode(bytes(anexo.conteudo)).decode()
            nome_arquivo = anexo.nome_arquivo
            tipo = "image" if (anexo.mime_type or "").startswith("image/") else "document"

    try:
        from app.apps.notificador import enviar_telegram
        resultado = enviar_telegram(
            telefone=solicitante.telefone, cpf=solicitante.cpf,
            mensagem=mensagem, arquivo_base64=arquivo_b64,
            nome_arquivo=nome_arquivo, tipo=tipo)
        ok = bool(resultado and resultado.get("ok"))
        detalhe = str(resultado.get("detalhe") or resultado.get("erro") or "") if resultado else ""
    except Exception as e:                      # falha de aviso não derruba a baixa
        logger.exception("ERP/aviso: falha ao notificar %s", t.numero_sp)
        ok, detalhe = False, str(e)

    _registrar(s, evento="BAIXA", referencia=referencia, titulo_id=t.id,
               pagamento_id=pg.id, destinatario_id=solicitante.id,
               destino=solicitante.telefone or solicitante.cpf or "",
               situacao="ENVIADO" if ok else "FALHA", mensagem=mensagem,
               erro="" if ok else detalhe, com_anexo=bool(arquivo_b64))
    logger.info("ERP/aviso: %s → %s (%s)%s", t.numero_sp, solicitante.nome,
                "enviado" if ok else f"falhou: {detalhe}",
                " com comprovante" if arquivo_b64 else "")
    return {"ok": ok, "situacao": "ENVIADO" if ok else "FALHA",
            "destinatario": solicitante.nome, "correcao": houve_outro,
            "com_comprovante": bool(arquivo_b64), "detalhe": detalhe}


def historico(s: Session, titulo_id: int) -> list[dict[str, Any]]:
    linhas = s.execute(text(
        "SELECT n.evento, n.situacao, n.criado_em, n.enviado_em, n.com_anexo, "
        "       n.erro, u.nome "
        "  FROM notificacoes n LEFT JOIN usuarios u ON u.id = n.destinatario_id "
        " WHERE n.titulo_id = :t ORDER BY n.criado_em DESC"), {"t": titulo_id}).all()
    return [{"evento": r[0], "situacao": r[1],
             "quando": (r[3] or r[2]).strftime("%d/%m/%Y %H:%M"),
             "com_anexo": r[4], "erro": r[5], "destinatario": r[6]} for r in linhas]
