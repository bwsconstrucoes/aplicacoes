# ============================================================================
# ERP — core/encaminhar.py
# Mandar uma informação do sistema para alguém, por WhatsApp e Telegram.
#
# O PEDIDO, do dono, em 12/09/2026: *"às vezes a gente quer encaminhar alguma
# informação pra alguém (…) referente a um título financeiro. O pessoal pede
# informação, você quer encaminhar pra um operador, pra um número que a gente
# adicionar lá"*.
#
# E o motivo dele, que explica o desenho: *"os lançamentos vão ser no sistema
# agora, e o pessoal está muito habituado a esse recebimento de mensagens (…)
# o WhatsApp vai meio que reduzir e acabar, vai ficar em sistema mesmo, mas
# eventualmente pode ser que alguém precise encaminhar aqueles dados"*.
#
# Ou seja: isto é uma PONTE de transição, não um canal novo de trabalho. Por
# isso ela é simples de propósito — não há conversa, não há resposta de volta,
# não há fila. Sai a informação, fica o registro.
#
# ─────────────────────────────────────────────────────────────────────────────
# AS TRÊS TRAVAS, e por que cada uma existe
#
#   1. **SÓ SE ENCAMINHA O QUE SE PODE VER.** Todo envio passa pelo mesmo
#      `exigir_*_no_escopo` da tela. Sem isso, encaminhar viraria a porta dos
#      fundos do controle de acesso: bastaria mandar o número de um título de
#      outra obra para receber o conteúdo dele no próprio celular.
#
#   2. **FICA REGISTRADO QUEM MANDOU O QUÊ PARA QUEM.** A mensagem sai do
#      sistema e vai para um aparelho — de lá, quem recebeu reencaminha para
#      quem quiser, e o ERP não tem como impedir. O que ele pode fazer é dar
#      NOME ao que saiu. Sem o registro, um vazamento não teria nem começo de
#      investigação. Usa a mesma tabela `notificacoes` dos avisos automáticos.
#
#   3. **NÚMERO AVULSO PASSA PELA CONFIRMAÇÃO DA TELA.** Um dígito trocado
#      manda dado financeiro para um estranho, e não há como desfazer. Aqui o
#      servidor só normaliza e recusa o que não parece telefone brasileiro;
#      quem CONFIRMA o destino é a pessoa, na tela, vendo para quem vai.
#
# ─────────────────────────────────────────────────────────────────────────────
# O QUE NÃO VAI, NUNCA: senha, chave de acesso, dado bancário completo da
# empresa e valor de contrato. A regra já existia para o agente de cobrança
# (`core/agente/__init__.py`) e vale aqui pelo mesmo motivo — mensagem de
# WhatsApp é o lugar mais fácil de vazar que existe na empresa.
# ============================================================================
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import (
    Anexo, Documento, Parcela, Rateio, StatusParcela, Titulo,
)

logger = logging.getLogger(__name__)

EVENTO = "ENCAMINHADO"
MAX_ANEXO_ENVIO = 8 * 1024 * 1024
MAX_DESTINOS = 10

# O que dá para encaminhar hoje. Acrescentar um tipo é escrever o texto dele e
# dizer qual escopo confere — o resto é comum.
TIPOS = ("titulo", "documento")


def _moeda(v: Any) -> str:
    return f"R$ {Decimal(str(v or 0)):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _data(d: Any) -> str:
    return d.strftime("%d/%m/%Y") if d else "—"


# ---------------------------------------------------------------------------
# Telefone
# ---------------------------------------------------------------------------
def normalizar_telefone(bruto: str) -> str:
    """Devolve só os dígitos, com o 55 do Brasil na frente.

    Recusa o que claramente não é telefone. Não é firula: número curto demais
    costuma ser um ramal ou um erro de digitação, e o envio iria para qualquer
    lugar — ou para lugar nenhum, o que é pior, porque a pessoa acha que
    mandou.
    """
    so = re.sub(r"\D", "", bruto or "")
    if not so:
        raise ErroValidacao("Informe o número de WhatsApp com DDD.")
    if so.startswith("55"):
        so = so[2:]
    if len(so) not in (10, 11):
        raise ErroValidacao(
            f"“{bruto}” não parece um telefone com DDD. Use DDD + número, "
            f"como 85 99999-0000.")
    return "55" + so


# ---------------------------------------------------------------------------
# O TEXTO DE CADA COISA
#
# Escrito para ser LIDO no celular, por alguém que pode não abrir o ERP: sem
# sigla do sistema, sem número interno, com o valor por extenso em reais. É o
# mesmo cuidado do bloco de dados bancários que o ERP já monta para colar num
# WhatsApp (`core/cadastros/contas.py`).
# ---------------------------------------------------------------------------
def texto_do_titulo(s: Session, titulo_id: int) -> tuple[str, str]:
    """(assunto, mensagem) do lançamento — os campos que o pessoal pedia."""
    t = s.get(Titulo, titulo_id, options=[selectinload(Titulo.fornecedor),
                                          selectinload(Titulo.parcelas),
                                          selectinload(Titulo.categoria)])
    if t is None:
        raise ErroValidacao("Lançamento não encontrado.")

    obras = " + ".join(sorted({r.obra.codigo for r in s.scalars(
        select(Rateio).where(Rateio.titulo_id == t.id)
        .options(selectinload(Rateio.obra))).all() if r.obra}))

    linhas = [
        "📄 *Lançamento*",
        "",
        f"*{t.numero_sp}* — {t.fornecedor.razao_social if t.fornecedor else '—'}",
        f"{(t.descricao or '')[:200]}",
        "",
        f"💰 Valor: *{_moeda(t.valor_liquido)}*",
        f"💳 Forma de pagamento: {getattr(t.forma_pagamento, 'value', t.forma_pagamento) or '—'}",
    ]
    if t.categoria is not None:
        linhas.append(f"🗂️ Conta: {t.categoria.codigo} · {t.categoria.descricao}")
    if obras:
        linhas.append(f"🏗️ Obra: {obras}")

    # As parcelas, porque "quando vence" é metade do que perguntam.
    abertas = [p for p in t.parcelas if p.status != StatusParcela.CANCELADA]
    if len(abertas) == 1:
        linhas.append(f"📅 Vencimento: {_data(abertas[0].vencimento)}")
    elif abertas:
        linhas.append(f"📅 {len(abertas)} parcelas:")
        for p in sorted(abertas, key=lambda x: x.numero)[:6]:
            marca = " (paga)" if p.status == StatusParcela.PAGA else ""
            linhas.append(f"   {p.numero}. {_data(p.vencimento)} — "
                          f"{_moeda(p.valor)}{marca}")
        if len(abertas) > 6:
            linhas.append(f"   … e mais {len(abertas) - 6}")

    linhas += [
        "",
        f"📌 Situação: {getattr(t.status, 'value', t.status)}",
        "",
        "_Enviado pelo ERP da BWS._",
    ]
    return f"Lançamento {t.numero_sp}", "\n".join(linhas)


def texto_do_documento(s: Session, documento_id: int) -> tuple[str, str]:
    """(assunto, mensagem) do documento do acervo."""
    d = s.get(Documento, documento_id, options=[selectinload(Documento.tipo)])
    if d is None:
        raise ErroValidacao("Documento não encontrado.")
    linhas = [
        "📎 *Documento*",
        "",
        f"*{d.nome_padronizado}*",
        f"🗂️ Tipo: {d.tipo.nome if d.tipo else '—'}",
    ]
    if d.emissao:
        linhas.append(f"📅 Emitido em: {_data(d.emissao)}")
    if d.validade:
        linhas.append(f"⏳ Válido até: {_data(d.validade)}")
    linhas += ["", "_Enviado pelo ERP da BWS._"]
    return d.nome_padronizado, "\n".join(linhas)


# ---------------------------------------------------------------------------
# O ANEXO, quando a pessoa pedir
# ---------------------------------------------------------------------------
def _anexo_do_titulo(s: Session, titulo_id: int) -> Optional[Anexo]:
    """O primeiro documento pendurado no lançamento — nota, boleto, recibo."""
    d = s.scalars(select(Documento).where(
        Documento.lancamento_tipo == "titulo",
        Documento.lancamento_id == titulo_id).limit(1)).first()
    return s.get(Anexo, d.anexo_id) if d is not None else None


def _bytes_do_anexo(s: Session, anexo: Optional[Anexo]):
    """(base64, nome, tipo) do anexo, ou (None, None, None).

    Falhar aqui NÃO impede o envio: mandar a informação sem o arquivo é melhor
    que não mandar nada, e quem recebeu pede o arquivo se precisar.
    """
    import base64 as _b64

    if anexo is None or (anexo.tamanho_bytes or 0) > MAX_ANEXO_ENVIO:
        return None, None, None
    from app.apps.erp.core.documentos.armazenamento import conteudo_de
    try:
        dados = conteudo_de(s, anexo)
    except Exception as e:
        logger.warning("ERP/encaminhar: anexo %s não pôde ser lido (%s)",
                       anexo.id, e)
        return None, None, None
    if not dados:
        return None, None, None
    tipo = "image" if (anexo.mime_type or "").startswith("image/") else "document"
    return _b64.b64encode(dados).decode(), anexo.nome_arquivo, tipo


# ---------------------------------------------------------------------------
# Enviar
# ---------------------------------------------------------------------------
def _registrar(s: Session, *, referencia: str, titulo_id: Optional[int],
               destinatario_id: Optional[int], destino: str, situacao: str,
               mensagem: str, erro: str = "", com_anexo: bool = False) -> None:
    s.execute(text(
        "INSERT INTO notificacoes (evento, referencia, titulo_id, pagamento_id, "
        " destinatario_id, destino, canal, situacao, mensagem, erro, com_anexo, "
        " tentativas, enviado_em) "
        "VALUES (:e, :r, :t, NULL, :u, :d, 'WHATSAPP', :s, :m, :err, :anexo, 1, :em) "
        "ON CONFLICT (evento, referencia) DO UPDATE SET "
        " situacao = EXCLUDED.situacao, erro = EXCLUDED.erro, "
        " tentativas = notificacoes.tentativas + 1, enviado_em = EXCLUDED.enviado_em"),
        {"e": EVENTO, "r": referencia, "t": titulo_id, "u": destinatario_id,
         "d": destino, "s": situacao, "m": mensagem[:4000],
         "err": (erro or "")[:1000], "anexo": com_anexo,
         "em": datetime.now(timezone.utc) if situacao == "ENVIADO" else None})


def _referencia(tipo: str, registro_id: int, destino: str, quando: datetime) -> str:
    """Cada encaminhamento é um evento novo, e isso é de propósito.

    O aviso automático é idempotente — a pessoa não pode receber duas vezes o
    mesmo comprovante. Aqui é o contrário: reenviar é EXATAMENTE o que se
    espera de um encaminhamento ("manda de novo que eu apaguei"). Por isso o
    instante entra na chave, e cada envio vira uma linha própria no registro.
    """
    bruto = f"{tipo}|{registro_id}|{destino}|{quando.isoformat()}"
    return hashlib.sha256(bruto.encode()).hexdigest()[:32]


def montar(s: Session, usuario: Usuario, *, tipo: str,
           registro_id: int) -> dict[str, Any]:
    """O texto que SERÁ enviado, para a tela mostrar antes de disparar.

    A pré-visualização não é enfeite: é a última chance de alguém perceber que
    está mandando o lançamento errado, e o envio não se desfaz.
    """
    exigir_registro_no_escopo(s, usuario, tipo, registro_id)
    assunto, mensagem = _texto(s, tipo, registro_id)
    return {"tipo": tipo, "registro_id": registro_id,
            "assunto": assunto, "mensagem": mensagem,
            "tem_anexo": (tipo == "titulo"
                          and _anexo_do_titulo(s, registro_id) is not None)
                         or tipo == "documento"}


def _texto(s: Session, tipo: str, registro_id: int) -> tuple[str, str]:
    if tipo == "titulo":
        return texto_do_titulo(s, registro_id)
    if tipo == "documento":
        return texto_do_documento(s, registro_id)
    raise ErroValidacao(f"Não sei encaminhar “{tipo}”.")


def exigir_registro_no_escopo(s: Session, usuario: Usuario, tipo: str,
                              registro_id: int) -> None:
    """O MESMO recorte da tela. Fora dele responde "não encontrado"."""
    if tipo == "titulo":
        from app.apps.erp.core.auth.permissoes import exigir_titulo_no_escopo
        exigir_titulo_no_escopo(s, usuario, registro_id)
    elif tipo == "documento":
        from app.apps.erp.core.arquivo.service import exigir_documento_no_escopo
        exigir_documento_no_escopo(s, usuario, registro_id)
    else:
        raise ErroValidacao(f"Não sei encaminhar “{tipo}”.")


def destinos_possiveis(s: Session) -> list[dict[str, Any]]:
    """Os operadores com telefone no cadastro — a lista que a tela oferece.

    Escolher da lista é o caminho seguro; digitar é o caminho que erra. Por
    isso a lista vem primeiro, e quem não tem telefone aparece marcado, para a
    pessoa saber que precisa cadastrar em vez de achar que sumiu.
    """
    saida = []
    for u in s.scalars(select(Usuario).where(Usuario.ativo.is_(True))
                       .order_by(Usuario.nome)).all():
        saida.append({"id": u.id, "nome": u.nome,
                      "perfil": getattr(u.perfil, "value", u.perfil),
                      "tem_telefone": bool(u.telefone or u.cpf)})
    return saida


def enviar(s: Session, usuario: Usuario, *, tipo: str, registro_id: int,
           usuarios: Optional[list[int]] = None,
           numeros: Optional[list[str]] = None,
           com_anexo: bool = False,
           observacao: str = "") -> dict[str, Any]:
    """Encaminha a informação. Devolve o que saiu e o que não saiu."""
    from app.apps.notificador import notificar

    if tipo not in TIPOS:
        raise ErroValidacao(f"Não sei encaminhar “{tipo}”.")
    exigir_registro_no_escopo(s, usuario, tipo, registro_id)

    alvos: list[dict[str, Any]] = []
    for uid in (usuarios or []):
        u = s.get(Usuario, int(uid))
        if u is None or not u.ativo:
            continue
        if not (u.telefone or u.cpf):
            alvos.append({"nome": u.nome, "usuario_id": u.id, "telefone": "",
                          "sem_destino": True})
            continue
        alvos.append({"nome": u.nome, "usuario_id": u.id,
                      "telefone": u.telefone or "", "cpf": u.cpf or ""})
    for bruto in (numeros or []):
        alvos.append({"nome": bruto, "usuario_id": None,
                      "telefone": normalizar_telefone(bruto)})

    if not alvos:
        raise ErroValidacao("Escolha ao menos uma pessoa ou informe um número.")
    if len(alvos) > MAX_DESTINOS:
        raise ErroValidacao(
            f"São {len(alvos)} destinos de uma vez — o teto é {MAX_DESTINOS}. "
            f"Encaminhar para muita gente de uma vez é como criar um grupo "
            f"sem querer.")

    assunto, mensagem = _texto(s, tipo, registro_id)
    if observacao.strip():
        # O recado de quem encaminha vai ANTES da informação: é ele que
        # explica por que aquilo chegou.
        mensagem = f"💬 {observacao.strip()[:300]}\n\n{mensagem}"

    b64 = nome_arquivo = tipo_arquivo = None
    if com_anexo:
        anexo = (_anexo_do_titulo(s, registro_id) if tipo == "titulo"
                 else s.get(Anexo, s.get(Documento, registro_id).anexo_id))
        b64, nome_arquivo, tipo_arquivo = _bytes_do_anexo(s, anexo)

    agora = datetime.now(timezone.utc)
    titulo_id = registro_id if tipo == "titulo" else None
    enviados, falhas, sem_destino = [], [], []

    for alvo in alvos:
        if alvo.get("sem_destino"):
            sem_destino.append(alvo["nome"])
            _registrar(s, referencia=_referencia(tipo, registro_id,
                                                 f"u{alvo['usuario_id']}", agora),
                       titulo_id=titulo_id, destinatario_id=alvo["usuario_id"],
                       destino="", situacao="IGNORADO", mensagem="",
                       erro="sem telefone/CPF no cadastro")
            continue
        destino = alvo.get("telefone") or alvo.get("cpf") or ""
        try:
            resultado = notificar(
                telefone=alvo.get("telefone") or None, cpf=alvo.get("cpf") or None,
                mensagem=mensagem, arquivo_base64=b64,
                nome_arquivo=nome_arquivo, tipo=tipo_arquivo)
            # Um canal basta: o dono pediu WhatsApp, e quem tem Telegram
            # cadastrado recebe pelos dois — o que não é problema nenhum.
            ok = any((resultado or {}).get(c, {}).get("ok")
                     for c in ("whatsapp", "telegram"))
            detalhe = "" if ok else str(resultado)[:400]
        except Exception as e:
            logger.exception("ERP/encaminhar: falha ao enviar %s para %s",
                             assunto, alvo["nome"])
            ok, detalhe = False, str(e)
        _registrar(s, referencia=_referencia(tipo, registro_id, destino, agora),
                   titulo_id=titulo_id, destinatario_id=alvo.get("usuario_id"),
                   destino=destino, situacao="ENVIADO" if ok else "FALHA",
                   mensagem=mensagem, erro="" if ok else detalhe,
                   com_anexo=bool(b64))
        (enviados if ok else falhas).append(
            alvo["nome"] if ok else f"{alvo['nome']}: {detalhe[:120]}")

    logger.info("ERP/encaminhar: %s → %d enviado(s), %d falha(s) — por %s",
                assunto, len(enviados), len(falhas), usuario.nome)
    return {"ok": bool(enviados), "assunto": assunto,
            "enviados": enviados, "falhas": falhas, "sem_destino": sem_destino,
            "com_anexo": bool(b64)}
