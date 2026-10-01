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

⚠️ A IDENTIDADE DA LINHA, porém, É DAQUI — desde 29/09/2026 ela ignora o FITID
(ver `identidade_da_linha`, abaixo, e o motivo). O parser do ERP continua
usando o FITID na identidade DELE, e o ERP tem o mesmo ponto fraco com o
Bradesco; está anotado no `CONTEXTO.md` §9 para o chat do ERP decidir.

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
    # Quantas transações o arquivo TEM, antes de qualquer decisão. Comparado com
    # `len(lancamentos)`, é o que denuncia linha perdida na leitura — ver
    # `contar_transacoes` no parser e o incidente de 28/09/2026.
    transacoes_no_arquivo: int = 0


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

    from app.apps.erp.core.pagamentos.ofx import (ErroOFX, _decodificar,
                                                 contar_transacoes, parsear_ofx)

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

    # ⚠️ O PERÍODO DECLARADO PELO BANCO NÃO MANDA SOZINHO — ele mente.
    #
    # Achado em 25/09/2026, num extrato do Bradesco que o dono trouxe: o
    # cabeçalho dizia DTSTART = DTEND = 25/09, e dentro vinham 1.006
    # lançamentos, o mais antigo de 01/09. O banco escreveu ali a data do
    # DOWNLOAD, não o intervalo do extrato.
    #
    # E isso não é cosmético: o período é a janela em que a conferência procura
    # "o que está aqui e NÃO vem neste extrato" — a lista que acusa linha
    # digitada errada e lançamento estornado. Com a janela de um dia só, ela
    # olhava 25/09, não achava nada, e a tela passava a impressão de que estava
    # tudo conferido.
    #
    # Agora o período é a UNIÃO do que o banco declarou com o que o arquivo de
    # fato traz. Esticar é seguro (a janela cobre tudo); encolher nunca.
    if datas:
        ini = min([ini] + datas) if ini else min(datas)
        fim = max([fim] + datas) if fim else max(datas)

    return ExtratoLido(
        bankid=re.sub(r"\D", "", _campo(texto, "BANKID")),
        acctid=_campo(texto, "ACCTID").strip(),
        periodo_ini=ini,
        periodo_fim=fim,
        saldo=_decimal(_campo(texto, "BALAMT")),
        saldo_em=_data(_campo(texto, "DTASOF")),
        lancamentos=lancamentos,
        impressao=hashlib.sha256(conteudo).hexdigest(),
        transacoes_no_arquivo=contar_transacoes(conteudo),
    )


# ---------------------------------------------------------------------------
# A IDENTIDADE DE UMA LINHA — e por que o FITID NÃO entra nela
#
# ⚠️ ACHADO EM 29/09/2026, NO EXTRATO DO DONO, DEPOIS DE TRÊS TENTATIVAS ERRADAS.
#
# O Bradesco escreve no FITID um CONTADOR DO ARQUIVO, não o número da transação:
# N10127, N1013B, N10151, N10165… — cresce de 22 em 22 (em hexadecimal) a cada
# lançamento, e RECOMEÇA no mesmo ponto a cada download. Dois extratos baixados
# em dias diferentes têm os mesmos FITIDs para transações diferentes.
#
# Com o FITID como identidade, isso dava dois estragos ao mesmo tempo, e o dono
# viu os dois na mesma tela:
#
#   1. A transferência de R$ 56.284,17 "não importava": o FITID dela (N1013B)
#      já existia no banco, vindo de OUTRA linha de um extrato anterior. A
#      conferência dizia "já estava aqui" — e nunca esteve. Mais 35 linhas do
#      mesmo arquivo sumiram do mesmo jeito.
#   2. A MESMA transação, trazida por dois downloads, entrava DUAS vezes, porque
#      cada download dava um FITID diferente a ela (os PIX de 146,00 e de
#      4.616,22 de 28/09 apareceram em dobro).
#
# E o banco 520 (28/09) já tinha mostrado o outro jeito de o FITID mentir: como
# código do TIPO da transação. Dos dois bancos que o dono usa, nenhum manda um
# FITID que identifique alguma coisa.
#
# A REGRA AGORA: a identidade é o CONTEÚDO da linha — data, valor, histórico e
# documento — mais a ORDEM da repetição dentro do arquivo (a 1ª e a 2ª tarifa
# iguais de R$ 0,35 são #1 e #2). O FITID continua GUARDADO na linha, para quem
# for investigar, mas não decide nada.
#
# O preço, dito por inteiro: se o banco reescrever o histórico de uma linha
# entre dois downloads (acontece com lançamento provisório que vira definitivo),
# ela entra de novo — e aparece na lista "está aqui e não vem neste extrato",
# onde alguém apaga uma. Linha a mais se vê; linha a menos, não.
#
# ⚠️ A RECEITA VIVE AQUI, E SÓ AQUI. `conciliacao.refazer_identidades` recalcula
# as identidades antigas do banco com ESTAS funções, a partir das colunas
# gravadas — por isso `descricao_da_linha` e `documento_da_linha` são o que vai
# para o banco E o que entra na identidade: se um dia divergirem, a reimportação
# passa a duplicar tudo. O parser do ERP continua dono da LEITURA; a identidade
# dele (`lanc.identidade`) não é mais usada aqui.
# ---------------------------------------------------------------------------
def descricao_da_linha(lanc) -> str:
    """O histórico como ele é gravado — e como entra na identidade."""
    descricao = (getattr(lanc, "memo", "") or "").strip()
    nome = getattr(lanc, "nome", None)
    if nome and nome not in descricao:
        descricao = f"{nome} — {descricao}".strip(" —")
    return descricao[:500]


def documento_da_linha(lanc) -> str:
    """O documento como ele é gravado — e como entra na identidade."""
    return (getattr(lanc, "documento", "") or "")[:60]


def identidade_da_linha(data, valor, descricao: str, documento: str,
                        ordem: int = 1) -> str:
    """A identidade de uma linha ANTES de virar hash, a partir do conteúdo.

    `valor` sai sempre com duas casas: o parser entrega Decimal já
    arredondado, e o banco devolve NUMERIC(14,2) — os dois têm de dar o mesmo
    texto, senão a identidade refeita a partir do banco não bate com a do
    arquivo.
    """
    base = (f"{data.isoformat()}|{Decimal(valor):.2f}|"
            f"{(descricao or '').strip()}|{(documento or '').strip()}")
    return base if ordem <= 1 else f"{base}|#{ordem}"


def impressao_da_linha(conta_id: int, lanc, ordem: int = 1) -> str:
    """A identidade de uma linha DENTRO de uma conta, já como hash.

    Sozinha ela não sabe se a linha é a 1ª ou a 2ª repetição no arquivo —
    quem sabe é `marcas_do_arquivo`, que numera. Use esta só para UMA linha
    cuja ordem você conhece.
    """
    base = identidade_da_linha(lanc.data, lanc.valor, descricao_da_linha(lanc),
                               documento_da_linha(lanc), ordem)
    return impressao_de(conta_id, base)


def impressao_de(conta_id: int, base: str) -> str:
    """Hash da identidade dentro da conta — a coluna `impressao`."""
    return hashlib.sha256(f"conta{conta_id}|{base}".encode("utf-8")).hexdigest()


def marcas_do_arquivo(conta_id: int, lancamentos: list) -> list:
    """`[(impressao, lancamento), …]`, na ordem do arquivo, numerando repetições.

    ⚠️ DUAS LINHAS NUNCA SAEM COM A MESMA MARCA. É isto que impede o que
    aconteceu em 29/09/2026 na conferência: duas linhas com a mesma chave
    viravam uma no dicionário, e a diferença era contada como "já estava aqui".
    """
    vezes: dict = {}
    saida = []
    for lanc in lancamentos:
        chave = identidade_da_linha(lanc.data, lanc.valor,
                                    descricao_da_linha(lanc),
                                    documento_da_linha(lanc))
        vezes[chave] = vezes.get(chave, 0) + 1
        saida.append((impressao_da_linha(conta_id, lanc, vezes[chave]), lanc))
    return saida
