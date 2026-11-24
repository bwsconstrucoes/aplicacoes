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
ROTULOS = {
    SEM_LANCAMENTO: "Sem lançamento",
    SP_JA_PAGA: "SP já paga",
    PROVAVEL: "Provável — conferir",
    LANCADO: "Lançado",
    SEM_SP_CONFIRMADO: "Sem SP (confirmado)",
}
ORDEM = [SEM_LANCAMENTO, SP_JA_PAGA, PROVAVEL, LANCADO, SEM_SP_CONFIRMADO]

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


def _sps_candidatas(boletos: list) -> list:
    """UMA consulta: as SPs (não canceladas) que podem casar com algum boleto — pelo
    código de barras, pela raiz do CNPJ ou pelo valor."""
    from .db import consultar
    if not boletos:
        return []
    raizes = sorted({d[:8] for b in boletos
                     for d in (b["beneficiario_doc"], b.get("beneficiario_final_doc") or "")
                     if len(d) == 14} | {d for b in boletos
                                         for d in (b["beneficiario_doc"],) if len(d) == 11})
    # O valor numa FAIXA de ±10%, e não exato: além do cruzamento, a consulta traz as
    # SPs de valor PARECIDO, que viram sugestão para o boleto que não casou (dono,
    # 24/11/2026: "quando o sistema não conseguir cruzar automático, é interessante
    # ele sugerir as SPs que são próximas").
    valores = sorted({b["valor"] for b in boletos if b["valor"]})
    barras = sorted({b["barras"] for b in boletos if b["barras"]}
                    | {b["linha_digitavel"] for b in boletos if b["linha_digitavel"]})
    condicoes, params = [], []
    if raizes:
        condicoes.append("left(regexp_replace(coalesce(documento,''), '[^0-9]', '', 'g'), 8) IN ("
                         + ",".join("?" for _ in raizes) + ")"
                         " OR regexp_replace(coalesce(documento,''), '[^0-9]', '', 'g') IN ("
                         + ",".join("?" for _ in raizes) + ")")
        params += [r[:8] for r in raizes] + raizes
    if valores:
        condicoes.append("(" + " OR ".join("valor_num BETWEEN ? AND ?" for _ in valores) + ")")
        for v in valores:
            params += [(v * Decimal("0.9")).quantize(Decimal("0.01")),
                       (v * Decimal("1.1")).quantize(Decimal("0.01"))]
    if barras:
        condicoes.append("regexp_replace(coalesce(codigo_barras,''), '[^0-9]', '', 'g') IN ("
                         + ",".join("?" for _ in barras) + ")")
        params += barras
    linhas = consultar(
        "SELECT id, credor, documento, valor_num, vencimento_d, codigo_barras, nf, "
        "       trim(coalesce(status_pgt,'')), trim(coalesce(forma_pagamento,'')) "
        "  FROM analisesps.sps "
        " WHERE lower(trim(coalesce(status_pgt,''))) <> 'cancelado' AND ("
        + " OR ".join(condicoes) + ")", tuple(params))
    return [{"id": str(l[0]), "credor": l[1] or "", "doc": _digitos(l[2]),
             "valor": Decimal(str(l[3])) if l[3] is not None else None,
             "vencimento": l[4], "barras": _barras_da_sp(l[5]), "nf": l[6] or "",
             "status_pgt": l[7] or "", "forma": l[8] or ""} for l in linhas]


def cruzar(boletos: list) -> None:
    """Põe em cada boleto `situacao`, `sp` (a SP casada, com o porquê) e as outras
    candidatas. Cada SP casa com um boleto só; os pares melhores escolhem primeiro."""
    sps = _sps_candidatas([b for b in boletos if not b.get("sp_manual")])
    por_id = {s["id"]: s for s in sps}
    pares = []
    for i, b in enumerate(boletos):
        if b.get("sp_manual"):
            continue
        for s in sps:
            n, porque = nota(b, s)
            if n >= NOTA_PROVAVEL:
                dias = (abs((s["vencimento"] - b["vencimento"]).days)
                        if s["vencimento"] and b["vencimento"] else 999)
                pares.append((-n, dias, i, s["id"], porque))
    pares.sort()
    usadas, casados = set(), {}
    # A SP vinculada à mão também está "usada": não casa com outro boleto.
    usadas |= {b["sp_manual"] for b in boletos if b.get("sp_manual") not in ("", NENHUMA)}
    candidatas: dict = {}
    for neg, _dias, i, sp_id, porque in pares:
        candidatas.setdefault(i, []).append((sp_id, -neg, porque))
        if i in casados or sp_id in usadas:
            continue
        casados[i] = (sp_id, -neg, porque)
        usadas.add(sp_id)
    sobras = [s for s in sps if s["id"] not in usadas]
    manuais = [b["sp_manual"] for b in boletos if b.get("sp_manual") not in ("", NENHUMA)]
    if manuais:
        for s in _sps_por_id(manuais):
            por_id.setdefault(s["id"], s)
    for i, b in enumerate(boletos):
        manual = b.get("sp_manual") or ""
        if manual == NENHUMA:
            b.update(situacao=SEM_SP_CONFIRMADO, sp=None, outras=[], sugestoes=[])
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
        b["sugestoes"] = [] if sp or manual else sugestoes(b, sobras)
        if not sp:
            b["situacao"] = SEM_LANCAMENTO
        elif sp.get("status_pgt", "").lower().startswith("pago"):
            b["situacao"] = SP_JA_PAGA
        elif sp["nota"] >= NOTA_LANCADO:
            b["situacao"] = LANCADO
        else:
            b["situacao"] = PROVAVEL


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
        "SELECT id, credor, documento, valor_num, vencimento_d, codigo_barras, nf, "
        "       trim(coalesce(status_pgt,'')), trim(coalesce(forma_pagamento,'')) "
        "  FROM analisesps.sps WHERE id IN (" + ",".join("?" for _ in ids) + ")", tuple(ids))
    return [{"id": str(l[0]), "credor": l[1] or "", "doc": _digitos(l[2]),
             "valor": Decimal(str(l[3])) if l[3] is not None else None,
             "vencimento": l[4], "barras": _barras_da_sp(l[5]), "nf": l[6] or "",
             "status_pgt": l[7] or "", "forma": l[8] or ""} for l in linhas]


# ---------------------------------------------------------------------------
# 4. O QUE A TELA MOSTRA
# ---------------------------------------------------------------------------
def filtros_da_tela(args) -> dict:
    def data(v):
        try:
            return dt.date.fromisoformat(str(v or "")[:10])
        except ValueError:
            return None
    situacoes = [s for s in (args.getlist("situacao") if hasattr(args, "getlist") else [])
                 if s in ROTULOS]
    return {"situacoes": situacoes,
            "busca": " ".join(str(args.get("busca") or "").split())[:80],
            "de": data(args.get("de")), "ate": data(args.get("ate")),
            "todas": bool(args.get("todas")), "aplicado": bool(args.get("f")),
            # "novos": os que apareceram pela primeira vez na última colagem;
            # "sairam": os que estavam na anterior e não estão na última (em geral, pagos).
            "recorte": args.get("recorte") if args.get("recorte") in ("novos", "sairam") else ""}


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


def listar(f: dict) -> dict:
    """Os boletos (da última colagem, ou de todas), cruzados com as SPs, com a
    contagem por situação. Duas consultas, mais a das SPs candidatas."""
    from .db import consultar, consultar_um
    ultima = ultima_colagem()
    # O QUE MUDOU DESDE A COLAGEM ANTERIOR (dono, 24/11/2026: *"o que já foi importado,
    # ignora e apenas adiciona as coisas novas. E é interessante até dar um que foi
    # adicionado novo"*): os novos desta colagem, e os que saíram do DDA.
    anterior, novos, sairam = None, 0, 0
    if ultima:
        linha = consultar_um("SELECT max(id) FROM analisesps.dda_colagem WHERE id < ?",
                             (ultima["id"],))
        anterior = linha[0] if linha else None
        linha = consultar_um(
            "SELECT count(*) FILTER (WHERE primeira_colagem = ?), "
            "       count(*) FILTER (WHERE ultima_colagem = ?) FROM analisesps.dda_boleto",
            (ultima["id"], anterior or -1))
        novos, sairam = (int(linha[0] or 0), int(linha[1] or 0)) if linha else (0, 0)
        if not anterior:
            novos = 0          # a primeira colagem: tudo é "novo", e dizer isso não ajuda
    partes, params = [], []
    if f.get("recorte") == "novos" and ultima:
        partes.append("primeira_colagem = ?")
        params.append(ultima["id"])
    elif f.get("recorte") == "sairam" and ultima:
        partes.append("ultima_colagem = ?")
        params.append(anterior or -1)
    elif not f.get("todas") and ultima:
        partes.append("ultima_colagem = ?")
        params.append(ultima["id"])
    if f.get("de"):
        partes.append("vencimento >= ?")
        params.append(f["de"].isoformat())
    if f.get("ate"):
        partes.append("vencimento <= ?")
        params.append(f["ate"].isoformat())
    if f.get("busca"):
        termo = "%" + f["busca"].lower() + "%"
        partes.append("(lower(beneficiario_nome) LIKE ? OR lower(beneficiario_final_nome) LIKE ? "
                      " OR beneficiario_doc LIKE ? OR lower(numero_documento) LIKE ? "
                      " OR linha_digitavel LIKE ? OR CAST(valor AS TEXT) LIKE ?)")
        so_digitos = "%" + (_digitos(f["busca"]) or f["busca"].lower()) + "%"
        params += [termo, termo, so_digitos, termo, so_digitos,
                   "%" + f["busca"].replace(".", "").replace(",", ".") + "%"]
    onde = (" WHERE " + " AND ".join(partes)) if partes else ""
    linhas = consultar("SELECT " + ", ".join(_CAMPOS_DA_LISTA) + " FROM analisesps.dda_boleto"
                       + onde + " ORDER BY vencimento NULLS LAST, valor DESC LIMIT 1000",
                       tuple(params))
    boletos = [dict(zip(_CAMPOS_DA_LISTA, l)) for l in linhas]
    for b in boletos:
        for c in ("valor", "valor_documento", "descontos", "juros", "multa"):
            if b[c] is not None:
                b[c] = Decimal(str(b[c]))
        b["na_ultima"] = bool(ultima and b["ultima_colagem"] == ultima["id"])
        b["novo"] = bool(ultima and anterior and b["primeira_colagem"] == ultima["id"])
    cruzar(boletos)
    contagem = {s: 0 for s in ORDEM}
    totais = {s: Decimal("0") for s in ORDEM}
    for b in boletos:
        contagem[b["situacao"]] += 1
        totais[b["situacao"]] += b["valor"]
    if f.get("situacoes"):
        mostrar = [b for b in boletos if b["situacao"] in f["situacoes"]]
    else:
        mostrar = boletos
    ordem = {s: n for n, s in enumerate(ORDEM)}
    mostrar.sort(key=lambda b: (ordem[b["situacao"]], b["vencimento"] or dt.date.max,
                                -b["valor"]))
    return {"boletos": mostrar, "ultima": ultima, "novos": novos, "sairam": sairam,
            "tem_anterior": bool(anterior),
            "contagem": [(s, ROTULOS[s], contagem[s], totais[s]) for s in ORDEM],
            "total": sum((b["valor"] for b in boletos), Decimal("0")),
            "quantos": len(boletos)}


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
