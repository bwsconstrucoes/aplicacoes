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
    Anexo, ObraInteressado, Pagamento, Parcela, Rateio, Titulo, TituloInteressado,
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


def destinatarios(s: Session, titulo: Titulo) -> list[Usuario]:
    """Quem recebe os avisos deste título: quem lançou, os interessados
    adicionados no lançamento e os interessados fixos das obras do rateio.
    Sem repetição e sem quem estiver inativo."""
    ids: list[int] = [titulo.solicitante_id]
    ids += [i.usuario_id for i in s.scalars(select(TituloInteressado).where(
        TituloInteressado.titulo_id == titulo.id)).all()]
    obras = [r.obra_id for r in s.scalars(select(Rateio).where(
        Rateio.titulo_id == titulo.id)).all()]
    if obras:
        ids += [i.usuario_id for i in s.scalars(select(ObraInteressado).where(
            ObraInteressado.obra_id.in_(obras))).all()]
    vistos, saida = set(), []
    for uid in ids:
        if uid in vistos:
            continue
        vistos.add(uid)
        u = s.get(Usuario, uid)
        if u is not None and u.ativo:
            saida.append(u)
    return saida


def avisar_baixa(s: Session, pagamento_id: int, *, forcar: bool = False,
                 enviar_comprovante: bool = True) -> dict[str, Any]:
    """Avisa quem lançou e os demais interessados de que o título foi pago,
    com o comprovante junto."""
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

    pessoas = destinatarios(s, t)
    if not pessoas:
        return {"ok": False, "situacao": "SEM_DESTINO",
                "motivo": "nenhum destinatário ativo para este título"}

    obras = " + ".join(sorted({r.obra.codigo for r in
                              s.scalars(select(Rateio).where(Rateio.titulo_id == t.id)
                                        .options(selectinload(Rateio.obra))).all()
                              if r.obra}))
    mensagem = _texto_baixa(t, p, pg, obras, correcao=houve_outro)

    # comprovante anexado à baixa, quando existir e couber
    arquivo_b64 = nome_arquivo = tipo = None
    if enviar_comprovante and pg.comprovante_anexo_id:
        anexo = s.get(Anexo, pg.comprovante_anexo_id)
        # O comprovante pode estar no banco ou no Drive: `conteudo_de` sabe de
        # onde tirar. Se o Drive não responder, a mensagem vai sem o anexo —
        # avisar é mais importante que anexar.
        if anexo is not None and (anexo.tamanho_bytes or 0) <= MAX_ANEXO_ENVIO:
            from app.apps.erp.core.documentos.armazenamento import conteudo_de
            try:
                dados = conteudo_de(s, anexo)
            except Exception as e:
                logger.warning("ERP: comprovante não pôde ser lido para a mensagem — %s", e)
                dados = b""
            if dados:
                arquivo_b64 = base64.b64encode(dados).decode()
                nome_arquivo = anexo.nome_arquivo
                tipo = "image" if (anexo.mime_type or "").startswith("image/") else "document"

    enviados, falhas, sem_destino = [], [], []
    for pessoa in pessoas:
        # referência por PESSOA: cada uma recebe uma vez, e o reenvio de um
        # não dispara de novo para os outros
        ref_pessoa = f"{referencia}:{pessoa.id}"
        anterior_p = _ja_enviado(s, "BAIXA", ref_pessoa)
        if anterior_p and anterior_p["situacao"] == "ENVIADO" and not forcar:
            continue
        if not (pessoa.telefone or pessoa.cpf):
            sem_destino.append(pessoa.nome)
            _registrar(s, evento="BAIXA", referencia=ref_pessoa, titulo_id=t.id,
                       pagamento_id=pg.id, destinatario_id=pessoa.id, destino="",
                       situacao="IGNORADO", mensagem="",
                       erro="sem telefone/CPF no cadastro")
            continue
        try:
            from app.apps.notificador import enviar_telegram
            resultado = enviar_telegram(
                telefone=pessoa.telefone, cpf=pessoa.cpf, mensagem=mensagem,
                arquivo_base64=arquivo_b64, nome_arquivo=nome_arquivo, tipo=tipo)
            ok = bool(resultado and resultado.get("ok"))
            detalhe = str(resultado.get("detalhe") or resultado.get("erro") or "") if resultado else ""
        except Exception as e:               # falha de aviso não derruba a baixa
            logger.exception("ERP/aviso: falha ao notificar %s para %s",
                             t.numero_sp, pessoa.nome)
            ok, detalhe = False, str(e)
        _registrar(s, evento="BAIXA", referencia=ref_pessoa, titulo_id=t.id,
                   pagamento_id=pg.id, destinatario_id=pessoa.id,
                   destino=pessoa.telefone or pessoa.cpf or "",
                   situacao="ENVIADO" if ok else "FALHA", mensagem=mensagem,
                   erro="" if ok else detalhe, com_anexo=bool(arquivo_b64))
        (enviados if ok else falhas).append(
            pessoa.nome if ok else f"{pessoa.nome}: {detalhe}")

    if not enviados and not falhas and not sem_destino:
        return {"ok": True, "situacao": "JA_ENVIADO",
                "motivo": "todos os interessados já foram avisados desta baixa"}

    logger.info("ERP/aviso: %s → %d enviado(s), %d falha(s), %d sem destino",
                t.numero_sp, len(enviados), len(falhas), len(sem_destino))
    return {"ok": bool(enviados), "situacao": "ENVIADO" if enviados else "FALHA",
            "enviados": enviados, "falhas": falhas, "sem_destino": sem_destino,
            "correcao": houve_outro, "com_comprovante": bool(arquivo_b64),
            "destinatarios": len(pessoas)}


# ---------------------------------------------------------------------------
# AVISO DE CANCELAMENTO
#
# O PEDIDO, do dono, em 12/09/2026: *"a pessoa que lançou vai receber aquela
# informação de que o título foi cancelado e qual o motivo"*.
#
# O BURACO QUE ISTO TAPA. Cancelar título já existia, com motivo obrigatório e
# registro de quem cancelou. O que não existia era o aviso: quem tinha lançado
# descobria por acaso, abrindo a tela — ou não descobria, e ficava esperando um
# pagamento que nunca ia sair. Um lançamento cancelado em silêncio é pior que
# um lançamento errado: o errado alguém corrige, o silencioso ninguém vê.
#
# QUEM RECEBE: quem lançou e os demais interessados do título — os mesmos do
# aviso de pagamento, pela mesma função. Duas listas de interessados divergem
# no dia em que alguém corrige uma.
#
# QUEM NÃO RECEBE: quem cancelou. Ele acabou de fazer isso, na tela, e não
# precisa que o celular lhe conte.
# ---------------------------------------------------------------------------
def _referencia_cancelamento(titulo_id: int) -> str:
    """Um cancelamento por título. Cancelar é definitivo: não há o que repetir."""
    return hashlib.sha256(f"cancelamento|{titulo_id}".encode()).hexdigest()[:32]


def _texto_cancelamento(t: Titulo, motivo: str, quem: str, obras: str) -> str:
    linhas = [
        "🚫 *Lançamento cancelado*",
        "",
        f"*{t.numero_sp}* — {t.fornecedor.razao_social}",
        f"{t.descricao[:120]}",
        "",
        f"💰 Valor: *{_moeda(t.valor_liquido)}*",
    ]
    if obras:
        linhas.append(f"🏗️ Obra: {obras}")
    linhas += [
        f"👤 Cancelado por: {quem}",
        "",
        # O MOTIVO VEM INTEIRO, e é a razão de a mensagem existir. "Foi
        # cancelado" sem o porquê obriga a pessoa a ligar para perguntar — e
        # aí o aviso não economizou trabalho nenhum.
        f"📝 Motivo: {motivo}",
        "",
        "_Acompanhe pelo ERP: aplicacoes.bwsconstrucoes.com.br/erp_",
    ]
    return "\n".join(linhas)


def avisar_cancelamento(s: Session, titulo_id: int, *, motivo: str,
                        cancelado_por: Optional[Usuario] = None,
                        forcar: bool = False) -> dict[str, Any]:
    """Avisa quem lançou (e os interessados) de que o título foi cancelado."""
    t = s.get(Titulo, titulo_id, options=[selectinload(Titulo.fornecedor),
                                          selectinload(Titulo.parcelas)])
    if t is None:
        return {"ok": False, "motivo": "título não encontrado"}

    referencia = _referencia_cancelamento(t.id)
    quem_id = getattr(cancelado_por, "id", None)
    pessoas = [p for p in destinatarios(s, t) if p.id != quem_id]
    if not pessoas:
        return {"ok": True, "situacao": "SEM_DESTINO",
                "motivo": "não há mais ninguém para avisar deste cancelamento"}

    obras = " + ".join(sorted({r.obra.codigo for r in
                              s.scalars(select(Rateio).where(Rateio.titulo_id == t.id)
                                        .options(selectinload(Rateio.obra))).all()
                              if r.obra}))
    mensagem = _texto_cancelamento(
        t, (motivo or "").strip() or "não informado",
        getattr(cancelado_por, "nome", "") or "o financeiro", obras)

    enviados, falhas, sem_destino = [], [], []
    for pessoa in pessoas:
        ref_pessoa = f"{referencia}:{pessoa.id}"
        anterior = _ja_enviado(s, "CANCELAMENTO", ref_pessoa)
        if anterior and anterior["situacao"] == "ENVIADO" and not forcar:
            continue
        if not (pessoa.telefone or pessoa.cpf):
            sem_destino.append(pessoa.nome)
            _registrar(s, evento="CANCELAMENTO", referencia=ref_pessoa,
                       titulo_id=t.id, pagamento_id=None, destinatario_id=pessoa.id,
                       destino="", situacao="IGNORADO", mensagem="",
                       erro="sem telefone/CPF no cadastro")
            continue
        try:
            from app.apps.notificador import enviar_telegram
            resultado = enviar_telegram(telefone=pessoa.telefone, cpf=pessoa.cpf,
                                        mensagem=mensagem)
            ok = bool(resultado and resultado.get("ok"))
            detalhe = str(resultado.get("detalhe") or resultado.get("erro") or "") if resultado else ""
        except Exception as e:
            # Falha de aviso NUNCA derruba o cancelamento: o cancelamento já
            # aconteceu e está registrado; o aviso que não saiu fica gravado
            # como FALHA e dá para reenviar.
            logger.exception("ERP/aviso: falha ao avisar cancelamento de %s para %s",
                             t.numero_sp, pessoa.nome)
            ok, detalhe = False, str(e)
        _registrar(s, evento="CANCELAMENTO", referencia=ref_pessoa, titulo_id=t.id,
                   pagamento_id=None, destinatario_id=pessoa.id,
                   destino=pessoa.telefone or pessoa.cpf or "",
                   situacao="ENVIADO" if ok else "FALHA", mensagem=mensagem,
                   erro="" if ok else detalhe)
        (enviados if ok else falhas).append(
            pessoa.nome if ok else f"{pessoa.nome}: {detalhe}")

    if not enviados and not falhas and not sem_destino:
        return {"ok": True, "situacao": "JA_ENVIADO",
                "motivo": "todos já foram avisados deste cancelamento"}
    logger.info("ERP/aviso: cancelamento de %s → %d enviado(s), %d falha(s)",
                t.numero_sp, len(enviados), len(falhas))
    return {"ok": bool(enviados), "situacao": "ENVIADO" if enviados else "FALHA",
            "enviados": enviados, "falhas": falhas, "sem_destino": sem_destino,
            "destinatarios": len(pessoas)}


def historico(s: Session, titulo_id: int) -> list[dict[str, Any]]:
    linhas = s.execute(text(
        "SELECT n.evento, n.situacao, n.criado_em, n.enviado_em, n.com_anexo, "
        "       n.erro, u.nome "
        "  FROM notificacoes n LEFT JOIN usuarios u ON u.id = n.destinatario_id "
        " WHERE n.titulo_id = :t ORDER BY n.criado_em DESC"), {"t": titulo_id}).all()
    return [{"evento": r[0], "situacao": r[1],
             "quando": (r[3] or r[2]).strftime("%d/%m/%Y %H:%M"),
             "com_anexo": r[4], "erro": r[5], "destinatario": r[6]} for r in linhas]
