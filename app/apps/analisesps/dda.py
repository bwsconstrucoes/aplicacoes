# -*- coding: utf-8 -*-
"""
O DDA DO BRADESCO — colar a tela, ler os boletos e cruzar com as SPs (24/11/2026).

O dono: *"No sistema do Bradesco, se a gente entrar lá no DDA, a gente consegue
copiar uma série de informações (…) o vencimento, o beneficiário, o pagador (…) o
CNPJ do pagador, se é o da matriz, da filial, o valor, e o código de barras (…) eu
vou ficar colando diariamente (…) e a gente tentar cruzar essas informações com os
registros que nós temos lançado. Com detalhe, o beneficiário, em algumas vezes, não
vai bater 100%, porque às vezes a empresa vende o título (…) mas tem o CNPJ (…) Se
tem boletos que estão lançados no sistema DDA, mas não estão lançados no nosso
registro financeiro."*

TRÊS PARTES:

1. `ler(texto)` — o texto copiado da tela do DDA vira uma lista de boletos. Cada
   boleto termina na linha "Multa"; dentro dele, as datas, o pagador e o beneficiário
   (nome e CNPJ), o banco, o valor, a linha digitável, o beneficiário final, o número
   do documento e os acréscimos. O "Total (N boletos) R$ X" do fim é a PROVA de que
   nada se perdeu na leitura — a tela diz se confere.
2. `guardar(...)` — um boleto é uma linha, pelo código de barras: colar de novo no dia
   seguinte não duplica, só marca a colagem em que ele apareceu por último.
3. `cruzar(boletos)` — UMA consulta às SPs (a base sincronizada da SPsBD) e a nota de
   cada par boleto × SP, pela ordem do que mais prova:
     - o CÓDIGO DE BARRAS igual (a SP de boleto guarda a linha digitável);
     - o CNPJ do credor (inteiro, ou a raiz — matriz e filial) + o valor + o
       vencimento, ou + o nº do documento (que costuma ser a nota fiscal);
     - o CNPJ + o valor, com vencimento diferente;
     - o valor + o vencimento, com CNPJ diferente — é o caso do título VENDIDO (o
       beneficiário é o banco ou o fundo, não o fornecedor).
   Cada SP casa com UM boleto só, e cada boleto com UMA SP: os pares melhores primeiro
   (o Tintas Iquine manda cinco boletos de mesmo valor e vencimento — o nº do
   documento desempata). O vínculo é calculado a cada abertura da tela, porque a SP
   pode ser lançada depois do boleto aparecer no DDA; só o vínculo feito À MÃO fica
   gravado.

O QUE A TELA RESPONDE: o que está no DDA e não tem lançamento — a pergunta dele. E,
de brinde, o boleto cuja SP já consta PAGA (pagar de novo seria pagar duas vezes).
"""
from __future__ import annotations

import datetime as dt
import logging
import re
from decimal import Decimal, InvalidOperation

logger = logging.getLogger("analisesps.dda")

NENHUMA = "NENHUMA"            # vínculo manual: "este boleto não tem SP, de propósito"

# As situações do cruzamento, na ordem da tela: o que pede ação primeiro.
SEM_LANCAMENTO = "sem_lancamento"
SP_JA_PAGA = "sp_ja_paga"
PROVAVEL = "provavel"
LANCADO = "lancado"
SEM_SP_CONFIRMADO = "sem_sp_confirmado"
PAGO = "pago"
ROTULOS = {
    SEM_LANCAMENTO: "Sem lançamento",
    SP_JA_PAGA: "SP já paga — ainda no DDA",
    PROVAVEL: "Provável — conferir",
    LANCADO: "Lançado",
    PAGO: "Pago",
    SEM_SP_CONFIRMADO: "Sem SP (confirmado)",
}
ORDEM = [SEM_LANCAMENTO, SP_JA_PAGA, PROVAVEL, LANCADO, PAGO, SEM_SP_CONFIRMADO]

NOTA_LANCADO = 70        # daqui para cima, o vínculo é dado como certo
NOTA_PROVAVEL = 55       # entre as duas, aparece para conferir


class ErroDoDDA(RuntimeError):
    """Falha com a mensagem pronta para a tela."""


# ---------------------------------------------------------------------------
# 1. LER O TEXTO COLADO
# ---------------------------------------------------------------------------
_DATA = re.compile(r"^\d{2}/\d{2}/\d{4}$")
# CNPJ como o DDA escreve: com um zero a mais na frente ("000.079.526/0003-70"), ou
# o normal; e CPF ("012.345.678-90").
_DOC = re.compile(r"^\d{2,3}\.\d{3}\.\d{3}(/\d{4})?-\d{2}$")
_MOEDA = re.compile(r"R\$\s*([\d.]+,\d{2})")
_FIM_DO_BOLETO = re.compile(r"Multa:\s*R\$\s*[\d.,]*", re.I)
_TOTAL = re.compile(r"Total\s*\((\d+)\s*boletos?\)\s*R\$\s*([\d.]+,\d{2})", re.I)


def _digitos(valor) -> str:
    return "".join(c for c in str(valor or "") if c.isdigit())


def _documento(texto) -> str:
    """CNPJ com 14 dígitos (o DDA põe um zero a mais na frente) ou CPF com 11."""
    d = _digitos(texto)
    if len(d) == 15:
        return d[1:]
    if len(d) == 12:
        return d[1:]
    return d


def _valor(texto):
    achado = _MOEDA.search(str(texto or ""))
    if not achado:
        return None
    try:
        return Decimal(achado.group(1).replace(".", "").replace(",", "."))
    except InvalidOperation:
        return None


def _data(texto):
    try:
        return dt.datetime.strptime(str(texto).strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def _linhas_depois(bloco: str, rotulo: str, quantas: int = 2) -> list:
    i = bloco.find(rotulo)
    if i < 0:
        return []
    resto = bloco[i + len(rotulo):].lstrip(":").splitlines()
    linhas = [" ".join(l.split()) for l in resto]
    return [l for l in linhas if l][:quantas]


def _um_boleto(bloco: str) -> dict | None:
    if "Código de barras" not in bloco and "Codigo de barras" not in bloco:
        return None
    cabeca, _, cauda = re.split(r"(C[óo]digo de barras:)", bloco, maxsplit=1)
    linha = _digitos(cauda.splitlines()[0] if cauda else "")
    # Os pedaços do cabeçalho: datas, pagador, beneficiário, banco e valor, cada um
    # numa "célula" (a tela separa por tabulação e quebra de linha).
    # Dois ou mais espaços também separam: há navegador que copia a tabulação assim.
    pedacos = [" ".join(p.split()) for p in re.split(r"[\t\n]+| {2,}", cabeca)]
    pedacos = [p for p in pedacos if p and p not in ("/", "&nbsp;")]
    datas = [p for p in pedacos if _DATA.match(p)]
    docs = [i for i, p in enumerate(pedacos) if _DOC.match(p)]
    if len(docs) < 2 or not datas:
        return None
    i_pag, i_ben = docs[0], docs[1]
    valor_cab = next((p for p in pedacos[i_ben + 1:] if p.startswith("R$")), "")
    banco = next((p for p in pedacos[i_ben + 1:] if not p.startswith("R$")), "")
    # O beneficiário final: "Não Informado", ou o nome e o documento.
    finais = _linhas_depois(cauda, "Beneficiário Final e CPF/CNPJ", 2)
    final_nome, final_doc = "", ""
    if finais and finais[0].lower() != "não informado":
        final_nome = finais[0]
        if len(finais) > 1 and _DOC.match(finais[1]):
            final_doc = _documento(finais[1])
    numero = (_linhas_depois(cauda, "Número de documento", 1) or [""])[0]
    if set(numero) <= {"*"}:
        numero = ""
    valor_doc = _valor(" ".join(_linhas_depois(cauda, "Valor do Documento", 1)))
    valor = _valor(valor_cab)
    if valor is None:
        valor = valor_doc
    from .pagamentos import codigo_boleto
    barras, _situacao = codigo_boleto(linha)
    return {
        "vencimento": _data(datas[0]),
        "limite": _data(datas[1]) if len(datas) > 1 else None,
        "pagador_nome": pedacos[i_pag - 1] if i_pag else "",
        "pagador_cnpj": _documento(pedacos[i_pag]),
        "beneficiario_nome": pedacos[i_pag + 1] if i_pag + 1 < i_ben else "",
        "beneficiario_doc": _documento(pedacos[i_ben]),
        "beneficiario_final_nome": final_nome,
        "beneficiario_final_doc": final_doc,
        "banco": banco,
        "valor": valor if valor is not None else Decimal("0"),
        "valor_documento": valor_doc,
        "descontos": _valor(" ".join(_linhas_depois(cauda, "Descontos", 1))),
        "juros": _valor(" ".join(_linhas_depois(cauda, "Juros", 1))),
        "multa": _valor(" ".join(_linhas_depois(cauda, "Multa", 1))),
        "numero_documento": numero[:60],
        "linha_digitavel": linha,
        "barras": barras or linha,
    }


def ler(texto: str) -> dict:
    """`{"boletos": [...], "total": Decimal, "do_banco": {"boletos", "total"} | None,
    "confere": bool, "nao_lidos": n}`."""
    texto = str(texto or "").replace("\r\n", "\n").replace(" ", " ")
    # Cada boleto termina no valor da Multa; o que vem depois é o próximo.
    partes, inicio = [], 0
    for achado in _FIM_DO_BOLETO.finditer(texto):
        partes.append(texto[inicio:achado.end()])
        inicio = achado.end()
    resto = texto[inicio:]
    boletos, nao_lidos = [], 0
    for parte in partes:
        b = _um_boleto(parte)
        if b and b["barras"]:
            boletos.append(b)
        elif "barras" in parte.lower():
            nao_lidos += 1
    total = sum((b["valor"] for b in boletos), Decimal("0"))
    banco = _TOTAL.search(resto) or _TOTAL.search(texto)
    do_banco = None
    if banco:
        do_banco = {"boletos": int(banco.group(1)),
                    "total": Decimal(banco.group(2).replace(".", "").replace(",", "."))}
    return {"boletos": boletos, "total": total, "do_banco": do_banco,
            "nao_lidos": nao_lidos,
            "confere": bool(do_banco and do_banco["boletos"] == len(boletos)
                            and do_banco["total"] == total)}


# ---------------------------------------------------------------------------
# 2. GUARDAR
# ---------------------------------------------------------------------------
_COLUNAS = ["vencimento", "limite", "pagador_nome", "pagador_cnpj", "beneficiario_nome",
            "beneficiario_doc", "beneficiario_final_nome", "beneficiario_final_doc", "banco",
            "valor", "valor_documento", "descontos", "juros", "multa", "numero_documento",
            "linha_digitavel"]


def guardar(lido: dict, quem: str = "") -> dict:
    """Grava a colagem e os boletos (um por código de barras). Devolve
    `{"colagem", "novos", "ja_conhecidos"}`."""
    from .db import conexao
    if not lido.get("boletos"):
        raise ErroDoDDA("Não encontrei nenhum boleto no texto colado. Copie a lista do "
                        "DDA inteira, com o \"Código de barras\" de cada boleto.")
    do_banco = lido.get("do_banco") or {}
    novos = 0
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.dda_colagem (colado_por, boletos, total, "
            "  total_do_banco, boletos_do_banco) VALUES (?, ?, ?, ?, ?) RETURNING id",
            (str(quem or "")[:80], len(lido["boletos"]), lido["total"],
             do_banco.get("total"), do_banco.get("boletos")))
        colagem = cur.fetchone()[0]
        cur.close()
        for b in lido["boletos"]:
            valores = [b.get(c) for c in _COLUNAS]
            cur = conn.execute(
                "INSERT INTO analisesps.dda_boleto (barras, " + ", ".join(_COLUNAS)
                + ", primeira_colagem, ultima_colagem, visto_em) VALUES (?, "
                + ", ".join("?" for _ in _COLUNAS) + ", ?, ?, now()) "
                "ON CONFLICT (barras) DO UPDATE SET "
                + ", ".join(f"{c} = EXCLUDED.{c}" for c in _COLUNAS)
                + ", ultima_colagem = EXCLUDED.ultima_colagem, visto_em = now() "
                "RETURNING (xmax = 0)",
                (b["barras"], *valores, colagem, colagem))
            if cur.fetchone()[0]:
                novos += 1
            cur.close()
        conn.commit()
    esquecer()
    logger.info("DDA: colagem %s — %d boleto(s), %d novo(s), por %s.", colagem,
                len(lido["boletos"]), novos, quem or "?")
    return {"colagem": colagem, "novos": novos,
            "ja_conhecidos": len(lido["boletos"]) - novos}


def vincular(boleto_id: int, sp: str, quem: str = "") -> None:
    """O vínculo À MÃO: o número da SP, `NENHUMA` (não tem SP, de propósito) ou vazio
    (volta ao automático)."""
    from .db import conexao
    sp = str(sp or "").strip().upper()
    if sp and sp != NENHUMA and not sp.isdigit():
        raise ErroDoDDA("Informe o número da SP (só números).")
    with conexao() as conn:
        conn.execute("UPDATE analisesps.dda_boleto SET sp_manual = ?, sp_manual_por = ?, "
                     "  sp_manual_em = CASE WHEN ? = '' THEN NULL ELSE now() END "
                     " WHERE id = ?", (sp, str(quem or "")[:80], sp, int(boleto_id)))
        conn.commit()
    esquecer()


# ---------------------------------------------------------------------------
# 3. CRUZAR COM AS SPs
# ---------------------------------------------------------------------------
def _barras_da_sp(texto) -> str:
    from .pagamentos import codigo_boleto
    d, _ = codigo_boleto(texto)
    return d or ""


def _sem_zeros(texto) -> str:
    return _digitos(texto).lstrip("0")


def nota(boleto: dict, sp: dict) -> tuple:
    """`(nota 0–100, o porquê)` de um par boleto × SP."""
    if boleto["barras"] and sp["barras"] and boleto["barras"] == sp["barras"]:
        return 100, "mesmo código de barras"
    doc_b, doc_s = boleto["beneficiario_doc"], sp["doc"]
    docs_b = {doc_b, boleto.get("beneficiario_final_doc") or ""} - {""}
    cnpj = bool(doc_s and doc_s in docs_b)
    raiz = bool(not cnpj and len(doc_s) == 14
                and any(len(d) == 14 and d[:8] == doc_s[:8] for d in docs_b))
    mesmo_credor = cnpj or raiz
    valor = sp["valor"] is not None and abs(sp["valor"] - boleto["valor"]) <= Decimal("0.01")
    dias = (abs((sp["vencimento"] - boleto["vencimento"]).days)
            if sp["vencimento"] and boleto["vencimento"] else None)
    venc = dias == 0
    perto = dias is not None and dias <= 5
    numero = _sem_zeros(boleto["numero_documento"])
    nf = _sem_zeros(sp["nf"])
    documento = bool(numero and nf and (numero == nf or (len(nf) >= 3 and nf in numero)))
    quem = "CNPJ" if cnpj else "raiz do CNPJ (matriz/filial)"
    if mesmo_credor and valor and venc:
        return 92, f"{quem}, valor e vencimento"
    if mesmo_credor and valor and documento:
        return 90, f"{quem}, valor e nº do documento (NF)"
    if mesmo_credor and valor and perto:
        return 82, f"{quem} e valor; vencimento a {dias} dia(s)"
    if mesmo_credor and valor:
        return 72, f"{quem} e valor; vencimento diferente"
    if valor and venc and documento:
        return 70, "valor, vencimento e nº do documento — credor diferente (título vendido?)"
    if valor and venc:
        return 60, "valor e vencimento — credor diferente (título vendido?)"
    if mesmo_credor and documento and (venc or perto):
        return 58, f"{quem} e nº do documento; valor diferente"
    return 0, ""


def _campos_sp() -> str:
    from .consultas import SQL_STATUS_AGEND
    return ("id, credor, documento, valor_num, vencimento_d, codigo_barras, nf, "
            "trim(coalesce(status_pgt,'')), trim(coalesce(forma_pagamento,'')), "
            "(" + SQL_STATUS_AGEND + ")")


def _sp_da_linha(l) -> dict:
    return {"id": str(l[0]), "credor": l[1] or "", "doc": _digitos(l[2]),
            "valor": Decimal(str(l[3])) if l[3] is not None else None,
            "vencimento": l[4], "barras": _barras_da_sp(l[5]), "nf": l[6] or "",
            "status_pgt": l[7] or "", "forma": l[8] or "", "status_agend": l[9] or ""}


# ⚠️ O MOTOR, REESCRITO EM 26/11/2026. O dono: *"está muito lento (…) eu vou tentar
# fazer uma filtragem, aí ele demora demais (…) o motor (…) está pesado."* Estava:
# (1) a consulta das SPs trazia as de valor PARECIDO (±10%) de CADA boleto — com
# centenas de boletos, as faixas cobriam quase a base inteira — e lia a tabela toda
# (a expressão do CNPJ não era a do índice); (2) cada boleto era comparado com CADA
# SP trazida — milhões de comparações por clique; (3) tudo isso refeito a cada filtro.
#
# Agora: a consulta só traz o que PODE casar (mesma raiz de CNPJ/CPF ou o mesmo
# valor, as duas pelos índices); cada boleto é comparado só com as SPs do mesmo
# código de barras, CNPJ, raiz ou valor (toda regra de `nota` exige uma dessas); as
# parecidas (sugestão) saem só para as linhas da PÁGINA; e o cruzamento da base
# fica guardado na memória até mudar algo (colagem, vínculo, sincronização das SPs)
# ou por 3 minutos — filtrar é só filtrar.
def _sps_candidatas(boletos: list) -> list:
    """UMA consulta, pelos índices: as SPs (não canceladas) da mesma raiz de
    CNPJ/CPF de algum boleto (beneficiário ou beneficiário final) ou do mesmo valor."""
    from .db import consultar
    if not boletos:
        return []
    raizes = sorted({d[:8] for b in boletos
                     for d in (b["beneficiario_doc"], b.get("beneficiario_final_doc") or "")
                     if len(d) in (11, 14)})
    valores = sorted({b["valor"] for b in boletos if b["valor"]})
    condicoes, params = [], []
    if raizes:
        # A MESMA expressão do índice `ix_sps_raiz_documento` (migração 010) — com
        # outra escrita o banco varre a tabela inteira.
        condicoes.append("left(regexp_replace(coalesce(documento, ''), '\\D', '', 'g'), 8) IN ("
                         + ",".join("?" for _ in raizes) + ")")
        params += raizes
    if valores:
        condicoes.append("valor_num IN (" + ",".join("?" for _ in valores) + ")")
        params += valores
    linhas = consultar(
        "SELECT " + _campos_sp() + " "
        "  FROM analisesps.sps "
        " WHERE lower(trim(coalesce(status_pgt,''))) <> 'cancelado' AND ("
        + " OR ".join(condicoes) + ")", tuple(params))
    return [_sp_da_linha(l) for l in linhas]


def _centavos(v) -> int | None:
    return None if v is None else int((Decimal(v) * 100).to_integral_value())


class _Indice:
    """As SPs por código de barras, documento, raiz e valor — para cada boleto
    olhar só as que podem casar com ele."""

    def __init__(self, sps: list):
        self.por_id = {s["id"]: s for s in sps}
        self.barras, self.doc, self.raiz, self.valor = {}, {}, {}, {}
        for s in sps:
            if s["barras"]:
                self.barras.setdefault(s["barras"], []).append(s)
            if s["doc"]:
                self.doc.setdefault(s["doc"], []).append(s)
                if len(s["doc"]) == 14:
                    self.raiz.setdefault(s["doc"][:8], []).append(s)
            c = _centavos(s["valor"])
            if c is not None:
                self.valor.setdefault(c, []).append(s)

    def do_credor(self, b: dict) -> list:
        saida = {}
        for d in (b["beneficiario_doc"], b.get("beneficiario_final_doc") or ""):
            for s in self.doc.get(d, []) + (self.raiz.get(d[:8], []) if len(d) == 14 else []):
                saida[s["id"]] = s
        return list(saida.values())

    def para(self, b: dict) -> list:
        """As SPs que `nota` pode pontuar: mesmo código de barras, mesmo credor
        (CNPJ ou raiz) ou mesmo valor (±1 centavo)."""
        saida = {s["id"]: s for s in self.do_credor(b)}
        for s in self.barras.get(b["barras"], []) if b["barras"] else []:
            saida[s["id"]] = s
        c = _centavos(b["valor"])
        if c is not None:
            for cc in (c - 1, c, c + 1):
                for s in self.valor.get(cc, []):
                    saida[s["id"]] = s
        return list(saida.values())


def cruzar(boletos: list, sps: list | None = None) -> dict:
    """Põe em cada boleto `situacao`, `sp` (a SP casada, com o porquê) e as outras
    candidatas. Cada SP casa com um boleto só; os pares melhores escolhem primeiro.
    Devolve `{"indice", "usadas"}` — o que as sugestões da página precisam."""
    indice = _Indice(sps if sps is not None
                     else _sps_candidatas([b for b in boletos if not b.get("sp_manual")]))
    por_id = indice.por_id
    pares = []
    for i, b in enumerate(boletos):
        if b.get("sp_manual"):
            continue
        for s in indice.para(b):
            n, porque = nota(b, s)
            if n >= NOTA_PROVAVEL:
                dias = (abs((s["vencimento"] - b["vencimento"]).days)
                        if s["vencimento"] and b["vencimento"] else 999)
                pares.append((-n, dias, i, s["id"], porque))
    pares.sort()
    # A SP vinculada à mão também está "usada": não casa com outro boleto.
    usadas = {b["sp_manual"] for b in boletos if b.get("sp_manual") not in ("", None, NENHUMA)}
    casados, candidatas = {}, {}
    for neg, _dias, i, sp_id, porque in pares:
        candidatas.setdefault(i, []).append((sp_id, -neg, porque))
        if i in casados or sp_id in usadas:
            continue
        casados[i] = (sp_id, -neg, porque)
        usadas.add(sp_id)
    manuais = [b["sp_manual"] for b in boletos if b.get("sp_manual") not in ("", None, NENHUMA)]
    if manuais:
        for s in _sps_por_id([m for m in manuais if m not in por_id]):
            por_id[s["id"]] = s
    for i, b in enumerate(boletos):
        manual = b.get("sp_manual") or ""
        b["sugestoes"] = []
        if manual == NENHUMA:
            b.update(situacao=SEM_SP_CONFIRMADO, sp=None, outras=[], pagamento="sem_sp")
            continue
        if manual:
            s = por_id.get(manual)
            b.update(sp=dict(s or {"id": manual, "credor": "", "status_pgt": ""},
                             nota=100, porque=f"vinculado à mão por {b.get('sp_manual_por') or '—'}"),
                     outras=[])
        elif i in casados:
            sp_id, n, porque = casados[i]
            b.update(sp=dict(por_id[sp_id], nota=n, porque=porque),
                     outras=[{"id": x, "nota": nn, "porque": p}
                             for x, nn, p in candidatas.get(i, []) if x != sp_id][:3])
        else:
            b.update(sp=None, outras=[])
        sp = b.get("sp")
        if not sp:
            b["situacao"] = SEM_LANCAMENTO
        elif sp.get("status_pgt", "").lower().startswith("pago"):
            # A base CRESCE (24/11/2026): boleto de semanas atrás com a SP paga é o
            # normal — "Pago". O alarme de pagar duas vezes é só para o que o banco
            # AINDA lista em aberto: o que veio na última colagem.
            b["situacao"] = SP_JA_PAGA if b.get("na_ultima", True) else PAGO
        elif sp["nota"] >= NOTA_LANCADO:
            b["situacao"] = LANCADO
        else:
            b["situacao"] = PROVAVEL
        b["pagamento"] = pagamento(b)
    return {"indice": indice, "usadas": usadas}


def _parecidas_pelo_valor(boletos: list, usadas: set) -> list:
    """As SPs de valor PARECIDO (±10%) e vencimento perto (até 15 dias) dos boletos
    da página — a parte da sugestão que não sai do índice. Uma consulta, pelos
    índices de valor e vencimento; só para quem está na tela."""
    from .db import consultar
    faixas = [b for b in boletos if b["valor"] and b["vencimento"]]
    if not faixas:
        return []
    condicoes, params = [], []
    for b in faixas:
        condicoes.append("(valor_num BETWEEN ? AND ? AND vencimento_d BETWEEN ? AND ?)")
        params += [(b["valor"] * Decimal("0.9")).quantize(Decimal("0.01")),
                   (b["valor"] * Decimal("1.1")).quantize(Decimal("0.01")),
                   b["vencimento"] - dt.timedelta(days=15),
                   b["vencimento"] + dt.timedelta(days=15)]
    linhas = consultar(
        "SELECT " + _campos_sp() + " FROM analisesps.sps "
        " WHERE lower(trim(coalesce(status_pgt,''))) <> 'cancelado' AND ("
        + " OR ".join(condicoes) + ") LIMIT 2000", tuple(params))
    return [s for s in (_sp_da_linha(l) for l in linhas) if s["id"] not in usadas]


def sugerir(boletos: list, cruzado: dict) -> None:
    """As sugestões (`b["sugestoes"]`) dos boletos SEM lançamento desta página."""
    sem = [b for b in boletos if b.get("situacao") == SEM_LANCAMENTO]
    if not sem:
        return
    usadas = cruzado["usadas"]
    perto = _parecidas_pelo_valor(sem, usadas)
    for b in sem:
        do_credor = [s for s in cruzado["indice"].do_credor(b) if s["id"] not in usadas]
        vistos = {s["id"] for s in do_credor}
        b["sugestoes"] = sugestoes(b, do_credor + [s for s in perto if s["id"] not in vistos])


def sugestoes(boleto: dict, sps: list, quantas: int = 3) -> list:
    """As SPs mais PARECIDAS com um boleto que não casou: o mesmo credor (CNPJ ou
    raiz), o valor perto, o vencimento perto. `[{"id", "credor", "porque", ...}]`,
    da mais parecida para a menos; só as que somam pelo menos 30 pontos."""
    from .formatos import moeda
    docs_b = {boleto["beneficiario_doc"], boleto.get("beneficiario_final_doc") or ""} - {""}
    saida = []
    for s in sps:
        pontos, motivos = 0, []
        if s["doc"] and s["doc"] in docs_b:
            pontos += 50
            motivos.append("mesmo CNPJ")
        elif len(s["doc"]) == 14 and any(len(d) == 14 and d[:8] == s["doc"][:8] for d in docs_b):
            pontos += 45
            motivos.append("mesma raiz de CNPJ")
        if s["valor"] is not None and boleto["valor"]:
            dif = abs(s["valor"] - boleto["valor"])
            pct = dif / boleto["valor"]
            if dif <= Decimal("0.01"):
                pontos += 30
                motivos.append("mesmo valor")
            elif pct <= Decimal("0.10"):
                pontos += 20 if pct <= Decimal("0.05") else 10
                motivos.append(f"valor R$ {moeda(s['valor'])} (dif. R$ {moeda(dif)})")
        if s["vencimento"] and boleto["vencimento"]:
            dias = abs((s["vencimento"] - boleto["vencimento"]).days)
            if dias <= 3:
                pontos += 15
            elif dias <= 15:
                pontos += 8
            elif dias <= 45:
                pontos += 3
            if dias:
                motivos.append(f"venc. {s['vencimento'].strftime('%d/%m/%Y')}")
            else:
                motivos.append("mesmo vencimento")
        if s["status_pgt"].lower() == "pagar":
            pontos += 5
        if pontos >= 30:
            saida.append((pontos, s, motivos))
    saida.sort(key=lambda x: -x[0])
    return [dict(s, pontos=p, porque=", ".join(m)) for p, s, m in saida[:quantas]]


def _sps_por_id(ids) -> list:
    from .db import consultar
    ids = sorted({str(i) for i in ids if str(i).isdigit()})
    if not ids:
        return []
    linhas = consultar(
        "SELECT " + _campos_sp() + " "
        "  FROM analisesps.sps WHERE id IN (" + ",".join("?" for _ in ids) + ")", tuple(ids))
    return [_sp_da_linha(l) for l in linhas]


# ---------------------------------------------------------------------------
# 4. O QUE A TELA MOSTRA
# ---------------------------------------------------------------------------
# A ETIQUETA DE PAGAMENTO (dono, 24/11/2026: *"uma etiqueta similar à parte do lote de
# solicitações, para saber (…) se o boleto está pago ou se não está pago"*). O DDA não
# diz se o boleto foi pago: quem diz é a SP casada com ele — o mesmo selo do Lote.
PAGAMENTOS = {"pagar": "A pagar", "agendado": "Agendado", "parcial": "Pago parcial",
              "pago": "Pago", "sem_sp": "Sem SP"}
PRAZOS = {"vencido": "Vencidos", "hoje": "Vencem hoje",
          "semana": "Próximos 7 dias", "depois": "Mais adiante"}


def pagamento(b: dict) -> str:
    sp = b.get("sp")
    if not sp or not sp.get("status_pgt"):
        return "sem_sp"
    st = sp["status_pgt"].strip().lower()
    if st == "pago":
        return "pago"
    if st == "pago parcial":
        return "parcial"
    if (sp.get("status_agend") or "").strip().lower() == "agendado":
        return "agendado"
    return "pagar"


def prazo(b: dict, hoje: dt.date | None = None) -> str:
    """Só o que NÃO está pago tem prazo: boleto pago é passado, não alarme."""
    hoje = hoje or dt.date.today()
    v = b.get("vencimento")
    if b.get("pagamento") == "pago":
        return ""
    if not v:
        return "depois"
    if v < hoje:
        return "vencido"
    if v == hoje:
        return "hoje"
    return "semana" if (v - hoje).days <= 7 else "depois"


def filtros_da_tela(args) -> dict:
    def data(v):
        try:
            return dt.date.fromisoformat(str(v or "")[:10])
        except ValueError:
            return None
    def lista(nome, validos=None):
        vals = args.getlist(nome) if hasattr(args, "getlist") else []
        return [v for v in vals if v and (validos is None or v in validos)]

    def valor(v):
        try:
            v = str(v or "").replace("R$", "").strip()
            if "," in v:                    # 1.234,50 — o jeito brasileiro
                v = v.replace(".", "").replace(",", ".")
            return Decimal(v) if v else None
        except InvalidOperation:
            return None
    situacoes = lista("situacao", ROTULOS)
    colagem = args.get("colagem") or ("ultima" if args.get("so_ultima") else "") \
        or ("novos" if args.get("recorte") == "novos" else "")
    return {"situacoes": situacoes,
            "busca": " ".join(str(args.get("busca") or "").split())[:80],
            "de": data(args.get("de")), "ate": data(args.get("ate")),
            # A BASE INTEIRA é o padrão (dono, 24/11/2026: *"essa base de boletos vai só
            # crescendo (…) eu quero ir sempre adicionando e atualizando o cruzamento"*).
            # Ele cola a lista em pedaços: o que não veio na última colagem NÃO saiu do
            # DDA, só não estava naquele pedaço. "só a última" é recorte opcional.
            "so_ultima": colagem == "ultima", "aplicado": bool(args.get("f")),
            # "novos": os que apareceram pela primeira vez na última colagem.
            "recorte": "novos" if colagem == "novos" else "",
            "colagem": colagem if colagem in ("ultima", "novos") else "",
            "pagamentos": lista("pagamento", PAGAMENTOS), "prazos": lista("prazo", PRAZOS),
            "pagadoras": [_digitos(v) for v in lista("pagadora")][:20],
            "valor_de": valor(args.get("valor_de")), "valor_ate": valor(args.get("valor_ate")),
            "pagina": max(1, int(args.get("p")) if str(args.get("p") or "").isdigit() else 1)}


_CAMPOS_DA_LISTA = ["id", "barras"] + _COLUNAS + ["sp_manual", "sp_manual_por",
                                                    "ultima_colagem", "visto_em",
                                                    "primeira_colagem"]


def ultima_colagem() -> dict | None:
    from .db import consultar_um
    linha = consultar_um("SELECT id, colado_em, colado_por, boletos, total, total_do_banco, "
                         "       boletos_do_banco FROM analisesps.dda_colagem "
                         " ORDER BY id DESC LIMIT 1")
    if not linha:
        return None
    return dict(zip(["id", "colado_em", "colado_por", "boletos", "total", "total_do_banco",
                     "boletos_do_banco"], linha))


# A base cresce a cada colagem; o teto guarda os de vencimento MAIS RECENTE (o que
# interessa), e a tela avisa quando ele corta.
TETO_DA_LISTA = 3000
# A PÁGINA (26/11/2026): *"à medida que eu vou colando, a página vai só encompridando
# e a gente não consegue visualizar."* A lista mostra 50 por vez.
POR_PAGINA = 50

# O CRUZAMENTO GUARDADO (ver o aviso acima de `_sps_candidatas`): um por processo
# (o gunicorn roda com 1 worker), trocado quando a versão muda ou depois de 3 min.
_GUARDADO: dict = {}
_VALIDADE = 180


def esquecer() -> None:
    """Colagem e vínculo mudam a base: a próxima abertura cruza de novo."""
    _GUARDADO.clear()


def _versao() -> tuple:
    """O que muda quando a base precisa ser cruzada de novo: os boletos (quantos, o
    último visto, o último vínculo), a última colagem e a última sincronização das
    SPs. Uma consulta leve."""
    from .db import consultar_um
    linha = consultar_um(
        "SELECT count(*), max(visto_em), max(sp_manual_em), "
        "       (SELECT max(id) FROM analisesps.dda_colagem), "
        "       (SELECT valor FROM analisesps.meta WHERE chave = 'ultima_sincronizacao') "
        "  FROM analisesps.dda_boleto")
    return tuple(str(x) for x in (linha or ()))


def _base() -> dict:
    """A base inteira, cruzada — da memória quando nada mudou."""
    import time
    from .db import consultar, consultar_um
    versao = _versao()
    guardado = _GUARDADO.get("base")
    if guardado and guardado["versao"] == versao and time.monotonic() - guardado["em"] < _VALIDADE:
        return guardado
    ultima = ultima_colagem()
    anterior = None
    if ultima:
        linha = consultar_um("SELECT max(id) FROM analisesps.dda_colagem WHERE id < ?",
                             (ultima["id"],))
        anterior = linha[0] if linha else None
    linhas = consultar("SELECT " + ", ".join(_CAMPOS_DA_LISTA) + " FROM analisesps.dda_boleto"
                       " ORDER BY vencimento DESC NULLS FIRST, valor DESC LIMIT ?",
                       (TETO_DA_LISTA,))
    boletos = [dict(zip(_CAMPOS_DA_LISTA, l)) for l in linhas]
    for b in boletos:
        for c in ("valor", "valor_documento", "descontos", "juros", "multa"):
            if b[c] is not None:
                b[c] = Decimal(str(b[c]))
        b["na_ultima"] = bool(ultima and b["ultima_colagem"] == ultima["id"])
        b["novo"] = bool(ultima and anterior and b["primeira_colagem"] == ultima["id"])
        # O texto da busca, montado uma vez: beneficiário, final, CNPJ, nº, linha, valor.
        b["_busca"] = " ".join(str(x or "").lower() for x in (
            b["beneficiario_nome"], b["beneficiario_final_nome"], b["beneficiario_doc"],
            b["beneficiario_final_doc"], b["numero_documento"], b["linha_digitavel"],
            b["valor"], str(b["valor"] or "").replace(".", ",")))
    cruzado = cruzar(boletos)
    pagadoras: dict = {}
    for b in boletos:
        if b.get("pagador_cnpj"):
            pagadoras[b["pagador_cnpj"]] = pagadoras.get(b["pagador_cnpj"], 0) + 1
    guardado = {"versao": versao, "em": time.monotonic(), "boletos": boletos,
                "cruzado": cruzado, "ultima": ultima, "anterior": anterior,
                "novos": (sum(1 for b in boletos if b["novo"]) if anterior else 0),
                "pagadoras": [(c, filial(c), n) for c, n in sorted(pagadoras.items())],
                "cortado": len(boletos) >= TETO_DA_LISTA}
    _GUARDADO["base"] = guardado
    return guardado


def _passa(b: dict, f: dict, ultima) -> bool:
    """Os filtros que RECORTAM a base (antes da contagem por situação)."""
    if f.get("recorte") == "novos" and not b["novo"]:
        return False
    if f.get("so_ultima") and ultima and not b["na_ultima"]:
        return False
    v = b["vencimento"]
    if f.get("de") and (not v or v < f["de"]):
        return False
    if f.get("ate") and (not v or v > f["ate"]):
        return False
    if f.get("valor_de") is not None and b["valor"] < f["valor_de"]:
        return False
    if f.get("valor_ate") is not None and b["valor"] > f["valor_ate"]:
        return False
    if f.get("pagadoras") and b["pagador_cnpj"] not in f["pagadoras"]:
        return False
    if f.get("busca"):
        termo = f["busca"].lower()
        digitos = _digitos(termo)
        if termo not in b["_busca"] and not (digitos and len(digitos) >= 3 and digitos in b["_busca"]):
            return False
    return True


def listar(f: dict) -> dict:
    """Os boletos da base (ou só da última colagem), cruzados com as SPs, com a
    contagem por situação e a PÁGINA pedida. O cruzamento vem da memória quando nada
    mudou (`_base`); filtrar não vai ao banco além da conferência da versão."""
    base = _base()
    ultima = base["ultima"]
    hoje = dt.date.today()
    recorte = [b for b in base["boletos"] if _passa(b, f, ultima)]
    contagem = {s: 0 for s in ORDEM}
    totais = {s: Decimal("0") for s in ORDEM}
    por_pagamento = {p: 0 for p in PAGAMENTOS}
    por_prazo = {p: 0 for p in PRAZOS}
    prazos = {}
    for b in recorte:
        prazos[b["id"]] = pz = prazo(b, hoje)
        contagem[b["situacao"]] += 1
        totais[b["situacao"]] += b["valor"]
        por_pagamento[b["pagamento"]] += 1
        if pz:
            por_prazo[pz] += 1
    mostrar = [b for b in recorte
               if (not f.get("situacoes") or b["situacao"] in f["situacoes"])
               and (not f.get("pagamentos") or b["pagamento"] in f["pagamentos"])
               and (not f.get("prazos") or prazos[b["id"]] in f["prazos"])]
    ordem = {s: n for n, s in enumerate(ORDEM)}
    mostrar.sort(key=lambda b: (ordem[b["situacao"]], b["vencimento"] or dt.date.max,
                                -b["valor"]))
    paginas = max(1, -(-len(mostrar) // POR_PAGINA))
    pagina = min(int(f.get("pagina") or 1), paginas)
    # A PÁGINA EM CÓPIA: a base guardada é de todos os pedidos (4 threads); o que é
    # só desta tela (prazo de hoje, sugestões) não pode ir para ela.
    da_pagina = [dict(b, prazo=prazos[b["id"]])
                 for b in mostrar[(pagina - 1) * POR_PAGINA: pagina * POR_PAGINA]]
    sugerir(da_pagina, base["cruzado"])
    return {"boletos": da_pagina, "ultima": ultima, "novos": base["novos"],
            "na_base": len(base["boletos"]),
            "por_pagamento": [(k, PAGAMENTOS[k], por_pagamento[k]) for k in PAGAMENTOS],
            "por_prazo": [(k, PRAZOS[k], por_prazo[k]) for k in PRAZOS],
            "pagadoras": base["pagadoras"], "mostrados": len(mostrar),
            "pagina": pagina, "paginas": paginas, "por_pagina": POR_PAGINA,
            "total_mostrado": sum((b["valor"] for b in mostrar), Decimal("0")),
            "cortado": base["cortado"],
            "tem_anterior": bool(base["anterior"]),
            "contagem": [(s, ROTULOS[s], contagem[s], totais[s]) for s in ORDEM],
            "total": sum((b["valor"] for b in recorte), Decimal("0")),
            "quantos": len(recorte)}


def filial(cnpj: str) -> str:
    """"matriz" (0001) ou "filial 0003" — o pedaço do CNPJ do pagador que diz qual."""
    d = _digitos(cnpj)
    if len(d) != 14:
        return ""
    return "matriz" if d[8:12] == "0001" else f"filial {d[8:12]}"


def documento_bonito(d: str) -> str:
    d = _digitos(d)
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    if len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    return d
