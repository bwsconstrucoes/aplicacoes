# -*- coding: utf-8 -*-
"""
A leitura de um arquivo OFX de extrato bancário, para a Conciliação.

⚠️ O PARSER DAS TRANSAÇÕES NÃO É ESCRITO AQUI. Ele já existe no ERP
(`app/apps/erp/core/pagamentos/ofx.py`), roda em produção há meses e resolve
duas armadilhas que custaram caro para descobrir: o FITID como identidade da
linha, e a ORDEM da repetição quando o banco não manda FITID — sem ela, dois
PIX iguais de R$ 1.500 no mesmo dia viravam UM, e o extrato passava a divergir
do banco em silêncio (achado em 11/09/2026, registrado lá).

**Reescrever aquilo aqui seria recomeçar pelos mesmos erros.** Então este
módulo IMPORTA o parser do ERP e acrescenta só o que a Conciliação precisa e o
ERP não extrai: de qual CONTA o arquivo é, que PERÍODO ele cobre e qual SALDO
o banco declarou.

⚠️ E ISSO CRIA UMA AMARRA ENTRE DUAS ÁREAS, que este comentário existe para
tornar visível: se o chat do ERP mover ou mudar aquele arquivo, esta tela para.
Há um teste (`tests/test_analisesps_conciliacao.py`) que importa o parser e
confere o contrato — é ele que faz a quebra aparecer na suíte, e não na tela do
dono. A alternativa era copiar as 148 linhas, e cópia diverge calada.
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

logger = logging.getLogger("analisesps.conciliacao")


class ErroDoExtrato(RuntimeError):
    """Arquivo ilegível ou que não é extrato. A mensagem vai para a tela."""


@dataclass
class ExtratoLido:
    """O que um arquivo OFX diz, antes de qualquer decisão sobre gravar."""
    bankid: str
    acctid: str
    periodo_ini: date | None
    periodo_fim: date | None
    saldo: Decimal | None
    saldo_em: date | None
    lancamentos: list          # LancamentoOFX, do parser do ERP
    impressao: str             # a digital do arquivo inteiro


def _campo(texto: str, tag: str) -> str:
    m = re.search(rf"<{tag}>([^<\r\n]*)", texto, re.IGNORECASE)
    return (m.group(1) or "").strip() if m else ""


def _data(valor: str):
    dig = re.sub(r"\D", "", (valor or "")[:14])
    if len(dig) < 8:
        return None
    from datetime import datetime
    try:
        return datetime.strptime(dig[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _decimal(valor: str):
    v = (valor or "").strip().replace(",", ".")
    if not v:
        return None
    try:
        return Decimal(v).quantize(Decimal("0.01"))
    except Exception:  # noqa: BLE001 — saldo ilegível não derruba a importação
        return None


def ler(conteudo: bytes) -> ExtratoLido:
    """Lê o arquivo INTEIRO sem gravar nada. Levanta `ErroDoExtrato`.

    Ler e gravar são passos separados de propósito: é o que permite mostrar ao
    dono o que vai acontecer ANTES de acontecer — quantas linhas são novas,
    quantas já estavam, de que conta é o arquivo.
    """
    if not conteudo:
        raise ErroDoExtrato("O arquivo chegou vazio.")
    if len(conteudo) > 12 * 1024 * 1024:
        # Um extrato de um ano tem alguns megas. Doze é folga larga, e o teto
        # existe porque este serviço já morreu de falta de memória uma vez.
        raise ErroDoExtrato(
            "O arquivo tem mais de 12 MB. Extrato desse tamanho costuma ser "
            "outra coisa — confira se é mesmo um OFX de extrato.")

    from app.apps.erp.core.pagamentos.ofx import ErroOFX, _decodificar, parsear_ofx

    texto = _decodificar(conteudo)
    try:
        # A conta ainda não é conhecida: o identificador da linha é refeito na
        # gravação, quando ela já for. Aqui o `0` só dá forma ao que foi lido.
        lancamentos = parsear_ofx(conteudo, 0)
    except ErroOFX as e:
        raise ErroDoExtrato(str(e)) from e

    ini = _data(_campo(texto, "DTSTART"))
    fim = _data(_campo(texto, "DTEND"))
    datas = [x.data for x in lancamentos]
    return ExtratoLido(
        bankid=re.sub(r"\D", "", _campo(texto, "BANKID")),
        acctid=_campo(texto, "ACCTID").strip(),
        periodo_ini=ini or (min(datas) if datas else None),
        periodo_fim=fim or (max(datas) if datas else None),
        saldo=_decimal(_campo(texto, "BALAMT")),
        saldo_em=_data(_campo(texto, "DTASOF")),
        lancamentos=lancamentos,
        impressao=hashlib.sha256(conteudo).hexdigest(),
    )


def impressao_da_linha(conta_id: int, lanc) -> str:
    """A identidade de uma linha DENTRO de uma conta.

    ⚠️ É a mesma receita do parser do ERP, refeita aqui com a conta certa: lá
    ela é calculada com a conta que for passada, e na leitura ainda não se
    sabe qual é. Refazer é mais seguro do que adivinhar na hora da gravação.
    """
    base = lanc.fitid or (
        f"{lanc.data.isoformat()}|{lanc.valor}|{lanc.memo}|{lanc.documento or ''}")
    # Sem FITID, o parser já acrescentou "|#2" à segunda linha igual do
    # arquivo — e essa marca vem dentro do `hash_linha`, não do memo. Por isso
    # a digital sem FITID usa o hash que ele calculou, que já carrega a ordem.
    if not lanc.fitid:
        base = lanc.hash_linha
    return hashlib.sha256(f"conta{conta_id}|{base}".encode("utf-8")).hexdigest()
