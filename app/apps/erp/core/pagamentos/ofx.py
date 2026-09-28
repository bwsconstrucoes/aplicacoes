# ============================================================================
# BWS ERP — core/pagamentos/ofx.py
# Parser de extrato OFX (Open Financial Exchange) sem dependências externas.
# Suporta o formato SGML clássico dos bancos brasileiros (Bradesco, BB, Itaú,
# Caixa, Santander) e o XML do OFX 2.x. Funções puras.
#
# Saída: lista de LancamentoOFX com hash determinístico por transação —
# o hash usa FITID quando presente (identificador único do banco), o que torna
# a reimportação do mesmo arquivo idempotente (constraint UNIQUE em extratos).
#
# ⚠️ ESTE ARQUIVO TEM UM SEGUNDO CONSUMIDOR, FORA DO ERP (24/09/2026).
# A tela de Conciliação Bancária do Análise de SPs importa `parsear_ofx`,
# `ErroOFX` e `_decodificar` daqui — ver `app/apps/analisesps/conciliacao_ofx.py`
# e o registro em `CONTEXTO.md` §9. Mudar a assinatura ou os campos de
# `LancamentoOFX` quebra aquela tela; há teste na suíte guardando o contrato.
# ============================================================================
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional


@dataclass
class LancamentoOFX:
    data: date
    valor: Decimal                 # negativo = débito (saída)
    tipo: str                      # DEBIT/CREDIT/PAYMENT/...
    memo: str
    nome: Optional[str]
    documento: Optional[str]       # CHECKNUM/REFNUM
    fitid: Optional[str]
    hash_linha: str
    # A identidade da linha ANTES de virar hash. Existe porque o Análise de SPs
    # precisa refazer o hash com o número da conta certo (na leitura ele ainda
    # não é conhecido) — e refazer a RECEITA lá era uma segunda cópia dela, que
    # divergiu e custou 207 lançamentos perdidos em 28/09/2026. Agora a receita
    # vive num lugar só, aqui.
    identidade: str = ""


logger = logging.getLogger("erp.ofx")


def _avisar(mensagem: str) -> None:
    """Anota no log o que mudou a identidade das linhas deste arquivo.

    Fica no log e não numa exceção porque NÃO é erro: é o parser se adaptando a
    um banco que usa o FITID de outro jeito. Mas quem for investigar uma
    diferença de saldo precisa achar isto escrito."""
    logger.info("%s", mensagem)


class ErroOFX(Exception):
    """Arquivo OFX ilegível ou sem transações."""


_RE_TRN = re.compile(r"<STMTTRN>(.*?)(?:</STMTTRN>|(?=<STMTTRN>)|\Z)",
                     re.DOTALL | re.IGNORECASE)


def _campo(bloco: str, tag: str) -> Optional[str]:
    m = re.search(rf"<{tag}>([^<\r\n]*)", bloco, re.IGNORECASE)
    if not m:
        return None
    v = m.group(1).strip()
    return v or None


def _data_ofx(valor: str) -> date:
    dig = re.sub(r"\D", "", (valor or "")[:14])
    if len(dig) < 8:
        raise ErroOFX(f"Data OFX inválida: {valor!r}")
    return datetime.strptime(dig[:8], "%Y%m%d").date()


def _valor_ofx(valor: str) -> Decimal:
    v = (valor or "").strip().replace(",", ".")
    try:
        return Decimal(v).quantize(Decimal("0.01"))
    except InvalidOperation:
        raise ErroOFX(f"Valor OFX inválido: {valor!r}")


def _decodificar(conteudo: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return conteudo.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return conteudo.decode("latin-1", errors="replace")


def contar_transacoes(conteudo: bytes) -> int:
    """Quantos blocos <STMTTRN> o arquivo TEM, antes de qualquer decisão.

    ⚠️ EXISTE PARA QUE PERDA DE LINHA NUNCA MAIS SEJA SILENCIOSA. Em 28/09/2026
    um extrato com 211 transações foi importado com 4, e a tela disse "li 4
    lançamentos" com ar de tudo certo. Comparar este número com o que o parser
    devolveu é o que permite a tela dizer "o arquivo tem 211 e eu reconheci 4" —
    que é a frase que teria poupado a investigação inteira."""
    try:
        texto = _decodificar(conteudo)
    except Exception:  # noqa: BLE001 — contagem é apoio, não pode derrubar nada
        return 0
    return len(_RE_TRN.findall(texto))


def parsear_ofx(conteudo: bytes, conta_bancaria_id: int) -> list[LancamentoOFX]:
    """Extrai as transações de um arquivo OFX. Levanta ErroOFX se nada for
    reconhecido — arquivo errado nunca passa em silêncio."""
    texto = _decodificar(conteudo)
    if "<OFX" not in texto.upper():
        raise ErroOFX("Arquivo não parece ser OFX (tag <OFX> ausente).")

    # ---------------------------------------------------------------------
    # PRIMEIRA PASSADA: o FITID deste banco é IDENTIFICADOR ou é CÓDIGO DE TIPO?
    #
    # ⚠️ ISTO CUSTOU 207 LANÇAMENTOS, em 28/09/2026. Um extrato do banco 520
    # (SOMABWS) com 211 transações entrou com QUATRO — porque o banco usa o
    # FITID como CÓDIGO DO TIPO da transação, não como identificador:
    #
    #     FITID 3121 = "Liberação de folha"  → 110 linhas, valores diferentes
    #     FITID 3029 = "Recebimento Pix"     →  95 linhas, valores diferentes
    #
    # O parser tratava FITID repetido como "a mesma transação aparecendo duas
    # vezes" e descartava — em silêncio, com a tela dizendo "li 4 lançamentos"
    # com ar de tudo certo. Extrato que perde 98% das linhas e não grita é o
    # pior defeito possível numa conciliação: o saldo não bate e ninguém sabe
    # por quê.
    #
    # A REGRA, e ela é conservadora de propósito: o FITID continua mandando
    # enquanto se comportar como identificador. Só quando o MESMO FITID aparece
    # com CONTEÚDO DIFERENTE (outra data, outro valor, outro histórico) é que
    # ele deixa de ser identidade — porque aí, por definição, ele não identifica
    # transação nenhuma. Banco que manda FITID único não sente diferença
    # alguma: a identidade das linhas dele continua byte a byte a mesma, e
    # extrato já importado não volta a entrar.
    # ---------------------------------------------------------------------
    def _conteudo(data, valor, memo, doc) -> str:
        return f"{data.isoformat()}|{valor}|{memo}|{doc or ''}"

    brutos = []
    conteudos_por_fitid: dict[str, set] = {}
    for m in _RE_TRN.finditer(texto):
        bloco = m.group(1)
        dt_raw = _campo(bloco, "DTPOSTED")
        val_raw = _campo(bloco, "TRNAMT")
        if not dt_raw or not val_raw:
            continue
        item = {
            "data": _data_ofx(dt_raw),
            "valor": _valor_ofx(val_raw),
            "fitid": _campo(bloco, "FITID"),
            "memo": _campo(bloco, "MEMO") or "",
            "nome": _campo(bloco, "NAME") or _campo(bloco, "PAYEE"),
            "doc": _campo(bloco, "CHECKNUM") or _campo(bloco, "REFNUM"),
            "tipo": (_campo(bloco, "TRNTYPE") or "").upper(),
        }
        brutos.append(item)
        if item["fitid"]:
            conteudos_por_fitid.setdefault(item["fitid"], set()).add(
                _conteudo(item["data"], item["valor"], item["memo"], item["doc"]))

    # Os FITIDs que carregam mais de um conteúdo são código de tipo, não
    # identificador. Para eles vale o mesmo caminho de quem não manda FITID.
    fitid_nao_identifica = {f for f, c in conteudos_por_fitid.items() if len(c) > 1}
    if fitid_nao_identifica:
        logger_aviso = (
            f"OFX: {len(fitid_nao_identifica)} FITID(s) se repetem com conteúdo "
            "diferente — este banco usa o FITID como código de tipo. A "
            "identidade das linhas passou a ser data+valor+histórico.")
        _avisar(logger_aviso)

    lancamentos: list[LancamentoOFX] = []
    vistos: set[str] = set()
    # Quantas vezes a MESMA combinação (data, valor, histórico, documento) já
    # apareceu neste arquivo. Usada quando o banco não manda FITID — ou quando
    # o FITID dele não identifica nada (ver acima).
    ocorrencias: dict[str, int] = {}
    for item in brutos:
        data, valor = item["data"], item["valor"]
        fitid, memo, doc = item["fitid"], item["memo"], item["doc"]
        nome = item["nome"]
        tipo = item["tipo"] or ("DEBIT" if valor < 0 else "CREDIT")

        if fitid and fitid not in fitid_nao_identifica:
            base = fitid
        else:
            # Sem FITID, a identidade da linha era só data+valor+histórico — e
            # aí DOIS pagamentos de verdade, iguais no mesmo dia para o mesmo
            # favorecido (dois PIX de R$ 1.500), viravam UM só: o segundo era
            # descartado como "duplicado" e o extrato passava a divergir do
            # banco em silêncio. Achado em 11/09/2026.
            #
            # Agora entra também a ORDEM da repetição dentro do arquivo: a 1ª e
            # a 2ª linha iguais recebem identidades diferentes. Continua
            # idempotente — reimportar o mesmo período reproduz a 1ª e a 2ª nas
            # mesmas posições, então nada duplica.
            chave = _conteudo(data, valor, memo, doc)
            ocorrencias[chave] = ocorrencias.get(chave, 0) + 1
            n = ocorrencias[chave]
            base = chave if n == 1 else f"{chave}|#{n}"
        h = hashlib.sha256(f"cta{conta_bancaria_id}|{base}".encode("utf-8")).hexdigest()
        # Mesma identidade duas vezes no arquivo: é a MESMA transação repetida.
        # Só chega aqui quem tem FITID que identifica de verdade (o caminho de
        # cima já numera as repetições de conteúdo).
        if h in vistos:
            continue
        vistos.add(h)
        lancamentos.append(LancamentoOFX(
            data=data, valor=valor, tipo=tipo, memo=memo.strip(),
            nome=(nome or "").strip() or None, documento=doc, fitid=fitid,
            hash_linha=h, identidade=base))

    if not lancamentos:
        raise ErroOFX("Nenhuma transação (<STMTTRN>) encontrada no arquivo.")
    return lancamentos


_RE_NOME_MEMO = re.compile(
    r"(?:PIX(?:\s+(?:DES|ENV|QRS|TRANSF))?|TED|DOC|TRANSF(?:ERENCIA)?)"
    r"[\s:_-]*(?:PARA|P/|FAVORECIDO)?[\s:_-]*([A-ZÀ-Ú][A-ZÀ-Ú0-9 .&-]{4,})",
    re.IGNORECASE)


def extrair_nome_contraparte(lanc: LancamentoOFX) -> Optional[str]:
    """Melhor esforço para obter o nome do favorecido: campo NAME quando o
    banco preenche; senão, heurística sobre o MEMO (padrão dos extratos
    Bradesco: 'PIX DES: FULANO DE TAL')."""
    if lanc.nome and len(lanc.nome) >= 5:
        return lanc.nome.upper().strip()
    m = _RE_NOME_MEMO.search(lanc.memo or "")
    if m:
        cand = m.group(1).strip().upper()
        cand = re.sub(r"\s{2,}", " ", cand)
        if len(cand) >= 5:
            return cand
    return None
