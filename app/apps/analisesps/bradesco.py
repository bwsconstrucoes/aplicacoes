# -*- coding: utf-8 -*-
"""
Conferência do extrato do Bradesco contra as SPs.

REAPROVEITADO do Streamlit quase inteiro. Toda a parte difícil — ler a tela
colada de "Detalhes das Operações", separar boleto de Pix, casar por código de
barras, por conta mais valor, e por semelhança de nome — é Python puro e
continua idêntica. Ela custou tempo para acertar e está testada no uso diário;
reescrever seria trocar código que funciona por código novo.

A ÚNICA mudança: onde o original varria um DataFrame do pandas (`iterrows`),
agora se varre uma LISTA DE DICIONÁRIOS, que é o que o banco devolve. Foram
cinco linhas. O acesso a cada campo já era por `r.get(...)`, então o resto
serviu sem tocar.
"""
from __future__ import annotations

import re
import unicodedata

# ---------------------------------------------------------------------------
# Normalizações (portadas 1:1)
# ---------------------------------------------------------------------------

def conta_key(s) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", "", str(s))
    m = re.match(r"^(\d+)-([0-9Xx])$", s)
    if not m:
        m = re.search(r"(\d+)-([0-9Xx])", s)
        if not m:
            return ""
    num = m.group(1).lstrip("0") or "0"
    return f"{num}-{m.group(2).upper()}"


def money_cents(valor_display) -> str:
    if valor_display in (None, ""):
        return ""
    s = re.sub(r"[^\d.,-]", "", str(valor_display))
    s = s.replace(".", "").replace(",", ".")
    try:
        n = float(s)
    except ValueError:
        return ""
    return str(round(n * 100))


def cents_from_float(v) -> str:
    try:
        return str(round(float(v) * 100))
    except (TypeError, ValueError):
        return ""


def to_float(s) -> float:
    s = re.sub(r"[^\d.,-]", "", str(s or "")).replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def fmt_money_br(n) -> str:
    try:
        fixed = f"{float(n):.2f}"
    except (TypeError, ValueError):
        return ""
    intp, dec = fixed.split(".")
    neg = intp.startswith("-")
    intp = intp.lstrip("-")
    intp = re.sub(r"(?<=\d)(?=(\d{3})+$)", ".", intp)
    return ("-" if neg else "") + f"{intp},{dec}"


_STOP = {"LTDA", "ME", "EPP", "EIRELI", "SA", "S/A", "DA", "DE", "DO", "DOS", "DAS",
         "E", "COMERCIO", "SERVICOS"}


def norm_name(name) -> str:
    if not name:
        return ""
    s = str(name).upper().strip()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split(" ") if t and t not in _STOP]
    return " ".join(toks)


def _lev(s, t) -> int:
    if s == t:
        return 0
    m, n = len(s), len(t)
    if not m:
        return n
    if not n:
        return m
    v0 = list(range(n + 1))
    v1 = [0] * (n + 1)
    for i in range(m):
        v1[0] = i + 1
        for j in range(n):
            cost = 0 if s[i] == t[j] else 1
            v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
        v0, v1 = v1, v0
    return v0[n]


def name_similarity(a, b) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    ta = set(x for x in a.split(" ") if x)
    tb = set(x for x in b.split(" ") if x)
    inter = len(ta & tb)
    union = len(ta) + len(tb) - inter
    jacc = inter / union if union else 0.0
    lev = _lev(a, b)
    maxlen = max(len(a), len(b)) or 1
    levnorm = 1 - lev / maxlen
    return 0.65 * jacc + 0.35 * levnorm


def _digits(s) -> str:
    return re.sub(r"\D+", "", str(s or ""))


# ---------------------------------------------------------------------------
# Parser da tela "Detalhes das Operações" (separada por TAB, bloco por empresa)
# ---------------------------------------------------------------------------

_RE_DATA = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_RE_AGCONTA = re.compile(r"\d{3,4}\s*\|\s*\d{4,10}-[0-9Xx]")
_RE_MONEY_FULL = re.compile(r"^\d{1,3}(?:\.\d{3})*,\d{2}$")


# COMO A LINHA DA OPERAÇÃO É SEPARADA EM COLUNAS
#
# O original só aceitava TABULAÇÃO, e isso funcionava porque copiar a tabela do
# Bradesco costuma trazer tabulação. Costuma — não sempre: dependendo do
# navegador e de como a seleção é feita, as colunas chegam separadas por
# ESPAÇOS, e aí o texto inteiro era ignorado em silêncio.
#
# Foi o que o dono viu em 11/09/2026: colou, clicou em Conferir, e a tela ficou
# em branco. Reproduzido — o mesmo texto com tabulação dá duas operações; com
# espaços, nenhuma.
#
# ACEITAR DOIS OU MAIS ESPAÇOS é seguro porque uma linha só vira operação se
# tiver, AO MESMO TEMPO, uma data, uma agência|conta e um valor. Nome de pessoa
# tem espaço simples e continua inteiro ("JOSE THIAGO DA SILVA").
_RE_COLUNAS = re.compile(r"\t+| {2,}")


def _colunas(line: str) -> list:
    return [f.strip() for f in _RE_COLUNAS.split(line) if f.strip()]


def parse_autorizacao(raw: str) -> list:
    """
    Lê a tela 'Detalhes das Operações' do Bradesco. Cada operação:
    {empresa, conta_debito, valor_display, valor_num, tipo(boleto|consumo|pix),
     sp, codigo, vencimento}.
    """
    lines = str(raw or "").replace("\r", "\n").split("\n")
    ops = []
    cur = None
    empresa = ""
    prev_simple = ""

    def _fecha_pix():
        nonlocal cur
        if cur and cur.get("tipo") == "pix":
            ops.append(cur)
        cur = None

    for line in lines:
        s = line.strip()
        if not s:
            continue
        if re.match(r"CNPJ\s*:", s, re.I):
            empresa = prev_simple
            continue

        fields = _colunas(line)
        conta = next((f for f in fields if _RE_AGCONTA.search(f)), "")
        has_date = any(_RE_DATA.match(f) for f in fields)
        moneys = [f for f in fields if _RE_MONEY_FULL.match(f)]
        if conta and has_date and moneys:
            _fecha_pix()
            mc = _RE_AGCONTA.search(conta)
            cur = {"empresa": empresa, "conta_debito": mc.group(0) if mc else conta,
                   "valor_display": moneys[-1], "valor_num": to_float(moneys[-1]),
                   "tipo": "", "sp": "", "codigo": "", "vencimento": "", "nome": ""}
            continue

        md = re.search(r"Opera[cç][aã]o e Descri[cç][aã]o\s*:\s*(.+)", s, re.I)
        if md and cur:
            desc = md.group(1).strip().lower()
            if "pix" in desc:
                cur["tipo"] = "pix"
            elif "boleto" in desc:
                cur["tipo"] = "boleto"
            elif any(k in desc for k in ("consumo", "luz", "água", "agua", "telefone", "gás", "gas")):
                cur["tipo"] = "consumo"
            else:
                cur["tipo"] = cur["tipo"] or "outro"
            continue

        # Nome do destinatário do PIX (rótulos variados: "Nome:", "Favorecido:",
        # "Recebedor:", "Beneficiário:", "Nome do Favorecido:" etc.) — usado p/
        # casar e desempatar contra o credor da SP.
        mn = re.match(
            r"(?:Nome(?:\s+(?:do|da|de)\s+[\wçãáéíóúâêô]+)?|Favorecid[oa]|Recebedor|"
            r"Benefici[áa]ri[oa]|Destinat[áa]ri[oa]|Para)\s*:\s*(.+)", s, re.I)
        if mn and cur:
            cur["nome"] = mn.group(1).strip()
            continue

        if re.search(r"Boleto\s+de\s+Cobran", s, re.I) and cur:
            partes = re.split(r"\s-\s", s)
            cur["tipo"] = "boleto"
            if len(partes) >= 4:
                cur["codigo"] = _digits(partes[1])
                cur["sp"] = _digits(partes[2])
                cur["vencimento"] = partes[3].strip()
            else:
                blocos = re.findall(r"\d+", s)
                if blocos:
                    cur["codigo"] = max(blocos, key=len)
                    cur["sp"] = next((d for d in blocos if d != cur["codigo"]
                                      and 8 <= len(d) <= 12), "")
            ops.append(cur)
            cur = None
            continue

        if re.search(r"Conta\s+de\s+Consumo", s, re.I) and re.search(r"Identifica", s, re.I) and cur:
            m2 = re.search(r"Identifica[cç][aã]o\s*:\s*([0-9.\s]+?)\s*-\s*(?:Descri|Concession|Data)",
                           s, re.I)
            conv = _digits(m2.group(1)) if m2 else ""
            if not conv:
                m3 = re.search(r"Identifica[cç][aã]o\s*:\s*([0-9.\s]+)", s, re.I)
                conv = _digits(m3.group(1)) if m3 else ""
            cur["tipo"] = "consumo"
            cur["codigo"] = conv
            ops.append(cur)
            cur = None
            continue

        # linha simples sem tab e que não é cabeçalho -> candidata a nome de empresa
        if "\t" not in line and not re.search(
                r"Total de|Valor Total|Data de Cria|Empresa|Detalhes das|"
                r"TRANSFER|^Nome\s*:", s, re.I):
            prev_simple = s

    _fecha_pix()
    return ops



# ---------------------------------------------------------------------------
# Cruzamento com a SPsBD (df) — dois grupos: boletos/consumo e pix
# ---------------------------------------------------------------------------

CARD_URL = "https://app.pipefy.com/open-cards/"


def _registro(r) -> dict:
    return {
        "id": str(r.get("id", "")).strip(),
        "credor": str(r.get("credor", "")),
        "valor_num": float(r.get("valor_num", 0) or 0),
        "conta": str(r.get("conta", "")),
        "forma": str(r.get("forma_pagamento", "")),
        "status_pgt": str(r.get("status_pgt", "")),
        "status_agend": str(r.get("status_agend", "")),
        "codigo_barras": str(r.get("codigo_barras", "")),
        "doc_fiscal": str(r.get("sp_fiscal", "")),
        "centro_custo": str(r.get("centro_custo", "")),
        "vencimento": str(r.get("vencimento", "")),
    }


def _norm_barcode(s) -> str:
    """Normaliza p/ código de barras (linha digitável 47 -> 44; mantém 44/48).

    O import é RELATIVO. No Streamlit ele era achatado (`import pagamentos`) e
    funcionava porque a pasta estava no caminho; dentro de um pacote, não. E
    como ele mora num `try/except`, a falha não apareceria: o código cairia em
    `_digits(s)` e a conferência do Bradesco deixaria de casar qualquer boleto
    digitado na forma de 47 dígitos — em silêncio, sem erro nenhum na tela."""
    try:
        from . import pagamentos
        d, _ = pagamentos.codigo_boleto(s)
        if d:
            return d
    except Exception:  # noqa: BLE001 — código estranho cai no caminho simples
        pass
    return _digits(s)


def _build_index(df):
    idx = {}
    for r in df:
        ck = conta_key(r.get("conta", ""))
        vc = cents_from_float(r.get("valor_num", 0))
        if not ck or not vc:
            continue
        idx.setdefault(f"{ck}|{vc}", []).append(_registro(r))
    return idx


def _diff(valor_b_num, reg) -> str:
    if reg and valor_b_num:
        d = round(valor_b_num - float(reg["valor_num"]), 2)
        if abs(d) >= 0.01:
            return fmt_money_br(d)
    return ""


def _alerta_conta(conta_b, reg) -> str:
    cb, cs = conta_key(conta_b), conta_key(reg.get("conta", ""))
    if cb and cs and cb != cs:
        return f"⚠️ Conta debitada ≠ cadastro (Bradesco {cb} × SP {cs})"
    return ""


def _alertas_status(reg, conta_b, diff_str) -> list:
    al = []
    stp = str(reg.get("status_pgt", "")).strip().lower()
    if stp == "pago":
        al.append("⚠️ JÁ PAGO (risco de duplicidade)")
    elif stp == "pago parcial":
        # A fase "Pago Parcial" do Pipefy (dono, 05/10/2026): parte já saiu —
        # pagar de novo pode ser o saldo ou pode ser duplicidade.
        al.append("⚠️ PAGO PARCIAL (confira o saldo)")
    elif stp == "cancelado":
        al.append("⚠️ SP CANCELADA")
    ac = _alerta_conta(conta_b, reg)
    if ac:
        al.append(ac)
    if diff_str:
        al.append(f"⚠️ Valor difere em R$ {diff_str}")
    return al


# ---------- PIX: casa por conta+valor e usa o NOME do extrato p/ desempatar ----------

_NOME_OK = 0.55          # a partir daqui consideramos que o nome bate
_NOME_GAP = 0.12         # folga mínima p/ desempatar entre candidatas
_NOME_RUIM = 0.30        # abaixo disto, nome é "outro" (divergência -> risco)


def _score_nome(op, reg):
    """Similaridade (0..1) entre o Nome do PIX (extrato) e o credor da SP, ou
    None quando falta um dos dois nomes."""
    nb = norm_name(op.get("nome", ""))
    nc = norm_name(reg.get("credor", ""))
    if not nb or not nc:
        return None
    return name_similarity(nb, nc)


def _match_nome_valor(op, df) -> list:
    """Candidatas por VALOR (centavos) + bom match de NOME, ignorando a conta.
    Usado quando conta+valor não achou nada — antes de cair no BeeVale.
    Retorna [(score, reg), ...] em ordem decrescente de similaridade."""
    nb = norm_name(op.get("nome", ""))
    if not nb:
        return []
    vc = cents_from_float(op.get("valor_num", 0))
    out = []
    for r in df:
        if cents_from_float(r.get("valor_num", 0)) != vc:
            continue
        reg = _registro(r)
        s = name_similarity(nb, norm_name(reg["credor"]))
        if s >= _NOME_OK:
            out.append((s, reg))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def _match_pix(op, idx, df) -> dict:
    nome_b = op.get("nome", "")
    cands = idx.get(f"{conta_key(op['conta_debito'])}|{cents_from_float(op['valor_num'])}", [])

    # ---------- 1) Candidatas por conta+valor ----------
    if cands:
        scored = [(_score_nome(op, c), c) for c in cands]
        tem_nome = bool(nome_b) and any(s is not None for s, _ in scored)

        if tem_nome:
            scored.sort(key=lambda x: (x[0] if x[0] is not None else -1.0), reverse=True)
            best_s, best = scored[0]

            if len(cands) == 1:
                reg = best
                diff = _diff(op["valor_num"], reg)
                alertas = _alertas_status(reg, op["conta_debito"], diff)
                if best_s is not None and best_s >= _NOME_OK:
                    classe, conf = "NOME OK", 96            # nome confere -> sem alerta
                elif best_s is not None and best_s < _NOME_RUIM:
                    # conta+valor batem, mas o nome é OUTRO -> risco (identidade fraca)
                    classe, conf = "NOME DIVERGE", 45
                    alertas.insert(0, f"⚠️ Conta/valor batem, mas o NOME é outro: "
                                      f"'{nome_b}' × credor '{reg['credor']}' ({best_s:.0%}) "
                                      f"— confirme se é a SP certa")
                else:
                    classe, conf = "ÚNICO", 85               # parcial -> nota curta
                    if best_s is not None:
                        alertas.insert(0, f"ℹ️ Nome confere só em parte ({best_s:.0%}): "
                                          f"'{nome_b}' × '{reg['credor']}'")
                return _linha_pix(op, reg, classe, conf, alertas)

            # várias candidatas: desempata pelo nome quando há folga clara
            second_s = scored[1][0] if scored[1][0] is not None else 0.0
            if best_s is not None and best_s >= _NOME_OK and (best_s - second_s) >= _NOME_GAP:
                reg = best                                   # casou pelo nome -> sem alerta
                diff = _diff(op["valor_num"], reg)
                alertas = _alertas_status(reg, op["conta_debito"], diff)
                return _linha_pix(op, reg, "NOME OK", 95, alertas)

            # nome existe mas não resolveu -> ambíguo, mostrando os scores
            reg = best
            diff = _diff(op["valor_num"], reg)
            alertas = _alertas_status(reg, op["conta_debito"], diff)
            detalhe = "; ".join(f"{c['id']} '{c['credor']}' ({(s or 0):.0%})" for s, c in scored if c.get("id"))
            alertas.append(f"{len(cands)} SPs mesma conta/valor — nome '{nome_b}' não desempatou: {detalhe}")
            return _linha_pix(op, reg, "AMBÍGUO", 80, alertas)

        # sem nome no extrato -> comportamento clássico (conta+valor)
        reg = cands[0]
        classe, conf = ("ÚNICO", 92) if len(cands) == 1 else ("AMBÍGUO", 80)
        diff = _diff(op["valor_num"], reg)
        alertas = _alertas_status(reg, op["conta_debito"], diff)
        if len(cands) > 1:
            ids = ", ".join(c["id"] for c in cands if c.get("id"))
            alertas.append(f"{len(cands)} SPs com mesma conta/valor (sem nome p/ desempate) — "
                           f"candidatas: {ids}")
        return _linha_pix(op, reg, classe, conf, alertas)

    # ---------- 2) Sem conta+valor: NOME+VALOR (qualquer conta) antes do BeeVale ----------
    nv = _match_nome_valor(op, df)
    if nv:
        best_s, reg = nv[0]
        diff = _diff(op["valor_num"], reg)
        alertas = _alertas_status(reg, op["conta_debito"], diff)  # já avisa conta ≠ cadastro
        if len(nv) > 1:
            outras = ", ".join(c["id"] for _, c in nv[1:] if c.get("id"))
            if outras:
                alertas.append(f"Outras SPs com mesmo valor+nome: {outras}")
        return _linha_pix(op, reg, "NOME (conta difere)", 88, alertas)

    # ---------- 3) BeeVale por último ----------
    bee = _beevale_match(op["valor_num"], df)
    if bee:
        reg = bee[0]
        diff = _diff(op["valor_num"], reg)
        alertas = _alertas_status(reg, op["conta_debito"], diff)
        alertas.append("Casou por valor BeeVale (exato ou +1,5%)")
        return _linha_pix(op, reg, "BEEVALE?", 90, alertas)

    return _linha_pix(op, None, "SEM MATCH", 0,
                      ["Nenhuma SP por conta/valor, por nome+valor, nem BeeVale ±1,5%."])


def _beevale_match(b_valor_num, df, tol=0.02, taxa=0.015) -> list:
    out = []
    for r in df:
        if "beevale" not in str(r.get("forma_pagamento", "")).lower():
            continue
        sv = float(r.get("valor_num", 0) or 0)
        if sv <= 0:
            continue
        if abs(sv - b_valor_num) <= tol or abs(sv * (1 + taxa) - b_valor_num) <= tol:
            out.append(_registro(r))
    return out


def _linha_pix(op, reg, classe, conf, alertas) -> dict:
    reg = reg or {}
    sp_id = reg.get("id", "")
    return {
        "Tipo": "Pix", "Empresa": op.get("empresa", ""),
        "Conta (Bradesco)": op.get("conta_debito", ""),
        "Valor (Bradesco)": op.get("valor_display", ""),
        "Nome (Bradesco)": op.get("nome", ""),
        "SP": sp_id, "Card": (CARD_URL + sp_id) if sp_id else "",
        "Classificação": classe, "Confiança": conf, "Credor (SP)": reg.get("credor", ""),
        "Doc Fiscal": reg.get("doc_fiscal", ""), "Status Pgt": reg.get("status_pgt", ""),
        "Status Agend": reg.get("status_agend", ""), "Centro de Custo": reg.get("centro_custo", ""),
        "Vencimento": reg.get("vencimento", ""), "Conta (SP)": reg.get("conta", ""),
        "Diferença": _diff(op.get("valor_num", 0), reg) if reg else "",
        "Alertas": " · ".join(a for a in alertas if a),
        # Para a seleção e os totais da tela (11/11/2026) — não são colunas.
        "_valor_banco": float(op.get("valor_num", 0) or 0),
        "_valor_sp": float(reg.get("valor_num", 0) or 0) if reg else 0.0,
    }


# ---------------------------------------------------------------------------
# ⚠️ AS COLUNAS DA TELA MORAM AQUI, ao lado de quem monta a linha — 15/09/2026
#
# Relato do dono: a tela do Bradesco, depois de "Conferir", mostrava
# "47 operação(ões)" e **47 linhas inteiramente em branco**.
#
# A CAUSA: a linha é um dicionário com chaves em português ("Valor
# (Bradesco)", "Credor (SP)"), e o template pedia chaves técnicas
# ("valor", "credor"). Nenhuma batia, então toda célula saía vazia. O
# contador vinha do tamanho da lista, e por isso continuava certo — o que
# fazia a tela parecer funcionando.
#
# POR QUE NÃO FOI PEGO ANTES: o teste da tela dublava `cruzar_tudo` e
# devolvia um dicionário com as chaves QUE O TEMPLATE QUERIA. Ele provava
# que o template desenha o que recebe, e não que recebe o que o código
# produz — o dublê escondia exatamente o defeito que existia.
#
# A LIÇÃO, e é por isso que estas listas ficam aqui e não no template:
# nome de campo que aparece em dois arquivos diferentes vira dois nomes
# diferentes no dia em que um dos dois mudar. Há teste percorrendo cada
# coluna daqui e exigindo que a chave exista na linha de verdade.
# ---------------------------------------------------------------------------
COLUNAS_BOLETO = [
    ("Alertas", "Alertas"), ("Empresa", "Empresa"),
    ("Conta", "Conta (Bradesco)"), ("Valor", "Valor (Bradesco)"),
    ("SP no banco", "SP (Bradesco)"), ("SP", "SP"),
    ("Agendamento", "Status Agend"), ("Status Pgt", "Status Pgt"),
    ("Credor", "Credor (SP)"), ("Validação", "Validação"),
    ("Cód. barras", "Cód. barras"), ("Vencimento", "Vencimento"),
    ("Diferença", "Diferença"),
]

COLUNAS_PIX = [
    ("Alertas", "Alertas"), ("Empresa", "Empresa"),
    ("Conta", "Conta (Bradesco)"), ("Valor", "Valor (Bradesco)"),
    ("Nome no banco", "Nome (Bradesco)"), ("SP", "SP"),
    ("Agendamento", "Status Agend"), ("Status Pgt", "Status Pgt"),
    ("Credor", "Credor (SP)"), ("Classificação", "Classificação"),
    ("Confiança", "Confiança"), ("Diferença", "Diferença"),
]


# ---------- BOLETO / CONTA DE CONSUMO (nº da SP + código de barras) ----------

def _linha_doc(op, reg, classe, conf, alertas) -> dict:
    reg = reg or {}
    sp_id = reg.get("id", "")
    return {
        "Tipo": "Boleto" if op.get("tipo") == "boleto" else "Conta de Consumo",
        "Empresa": op.get("empresa", ""),
        "Conta (Bradesco)": op.get("conta_debito", ""),
        "Valor (Bradesco)": op.get("valor_display", ""),
        "SP (Bradesco)": op.get("sp", ""),
        "Cód. barras": _norm_barcode(op.get("codigo", "")),
        "SP": sp_id, "Card": (CARD_URL + sp_id) if sp_id else "",
        "Validação": classe, "Confiança": conf, "Credor (SP)": reg.get("credor", ""),
        "Doc Fiscal": reg.get("doc_fiscal", ""), "Status Pgt": reg.get("status_pgt", ""),
        "Status Agend": reg.get("status_agend", ""), "Centro de Custo": reg.get("centro_custo", ""),
        "Vencimento": reg.get("vencimento", ""), "Conta (SP)": reg.get("conta", ""),
        "Diferença": _diff(op.get("valor_num", 0), reg) if reg else "",
        "Alertas": " · ".join(a for a in alertas if a),
        # Para a seleção e os totais da tela (11/11/2026) — não são colunas.
        "_valor_banco": float(op.get("valor_num", 0) or 0),
        "_valor_sp": float(reg.get("valor_num", 0) or 0) if reg else 0.0,
    }


def _acha_por_barras(cod, barras):
    if not cod:
        return None
    lst = barras.get(cod)
    if not lst:
        for nb, regs in barras.items():
            if nb and (cod in nb or nb in cod):
                lst = regs
                break
    return lst[0] if lst else None


def _match_doc(op, by_id, barras) -> dict:
    sp = op.get("sp", "")
    cod = _norm_barcode(op.get("codigo", ""))
    reg_id = by_id.get(sp) if sp else None
    reg_bar = _acha_por_barras(cod, barras)

    if op.get("tipo") == "consumo":
        if reg_bar:
            reg, classe, conf, base = reg_bar, "OK (barras)", 96, "Convênio localizado pelo código de identificação."
        else:
            return _linha_doc(op, None, "SEM MATCH", 0, ["Convênio não encontrado em nenhuma SP."])
    else:  # boleto: dupla validação ID + código de barras
        if reg_id and reg_bar and reg_id["id"] == reg_bar["id"]:
            reg, classe, conf, base = reg_id, "OK (ID+barras)", 100, "Nº da SP e código de barras conferem."
        elif reg_id and reg_bar and reg_id["id"] != reg_bar["id"]:
            reg, classe, conf = reg_id, "CONFLITO", 60
            base = f"Nº da SP aponta {reg_id['id']}, mas o código de barras casa com a SP {reg_bar['id']}."
        elif reg_id and not reg_bar:
            if _digits(reg_id["codigo_barras"]):
                reg, classe, conf, base = reg_id, "ALERTA (barras)", 70, "Nº da SP confere, mas o código de barras diverge."
            else:
                reg, classe, conf, base = reg_id, "OK (só ID)", 88, "Nº da SP confere; SP sem código de barras cadastrado."
        elif reg_bar and not reg_id:
            reg, classe, conf = reg_bar, "ALERTA (ID)", 75
            base = f"Código de barras casa com a SP {reg_bar['id']}, mas o nº da SP não bateu."
        else:
            return _linha_doc(op, None, "SEM MATCH", 0,
                              ["Nenhuma SP pelo nº nem pelo código de barras."])

    diff = _diff(op["valor_num"], reg)
    alertas = [base] + _alertas_status(reg, op["conta_debito"], diff)
    return _linha_doc(op, reg, classe, conf, alertas)


def _marca_dup(rows, credor_key="Credor (SP)"):
    from collections import Counter
    sp_cont = Counter(r["SP"] for r in rows if r.get("SP"))
    for r in rows:
        if r.get("SP") and sp_cont[r["SP"]] > 1:
            r["Alertas"] = " · ".join(x for x in [r.get("Alertas", ""),
                           f"⚠️ SP repetida no lote ({sp_cont[r['SP']]}×)"] if x)
    return rows


def _no_foco(r) -> bool:
    """True se a SP está na fila de pagamento: status_agend em Agendar/Agendado/
    Falha Agendar (e variações). Exclui Desagendar e vazio."""
    s = str(r.get("status_agend", "")).strip().lower()
    if not s or s.startswith("desagend"):
        return False
    return s.startswith("agend") or ("falha" in s and "agend" in s)


def cruzar_tudo(raw: str, df, foco_agendados: bool = True) -> dict:
    """Retorna {'boletos': [...], 'pix': [...]} a partir da tela colada.
    Se foco_agendados, restringe os candidatos às SPs na fila (Agendar/Agendado/
    Falha Agendar) — derruba o ruído e foca no risco real de duplicidade."""
    df_base = [r for r in df if _no_foco(r)] if foco_agendados else list(df)
    idx = _build_index(df_base)
    by_id, barras = {}, {}
    for r in df_base:
        reg = _registro(r)
        if reg["id"]:
            by_id[reg["id"]] = reg
        nb = _norm_barcode(reg["codigo_barras"])
        if nb:
            barras.setdefault(nb, []).append(reg)

    boletos, pix = [], []
    for op in parse_autorizacao(raw):
        if op["tipo"] in ("boleto", "consumo"):
            boletos.append(_match_doc(op, by_id, barras))
        else:  # pix e 'outro' -> casa por conta+valor
            pix.append(_match_pix(op, idx, df_base))
    return {"boletos": _marca_dup(boletos), "pix": _marca_dup(pix)}


def cruzar(raw: str, tipo: str, df):   # compat. antiga
    r = cruzar_tudo(raw, df)
    return r["boletos"] + r["pix"]

# ---------------------------------------------------------------------------
# OS NÚMEROS DA CONFERÊNCIA (11/11/2026). O dono: *"não tem KPIs, totalizados,
# somatórios, por conta também. Tudo isso facilita a aprovação."* Somados das
# operações coladas (o valor do BANCO): quantas, quanto, quantas acharam a SP, quais
# pedem olho, e a divisão por status de agendamento e por conta de débito.
# ---------------------------------------------------------------------------
def pede_conferencia(linha: dict) -> bool:
    """A operação que pede olho: sem SP, com a validação/classificação fora de "OK",
    ou com um alerta de status (⚠️ já pago, pago parcial…). A frase que só explica
    o acerto ("Nº da SP e código de barras conferem") não conta."""
    if not linha.get("SP"):
        return True
    classe = str(linha.get("Validação") or linha.get("Classificação") or "").upper()
    if classe and not (classe.startswith("OK") or classe.startswith("NOME OK")):
        return True
    return "⚠" in str(linha.get("Alertas") or "")


ORDEM_DO_AGENDAMENTO = ["Agendar", "Agendado", "Falha Agendar", "Verificar", "Pago", ""]


def resumo(resultado: dict) -> dict:
    """PURA. Os KPIs, o status de agendamento e as contas das operações coladas."""
    linhas = list((resultado or {}).get("boletos") or []) + list((resultado or {}).get("pix") or [])

    def soma(itens):
        return round(sum(float(i.get("_valor_banco") or 0) for i in itens), 2)

    com_sp = [i for i in linhas if i.get("SP")]
    sem_sp = [i for i in linhas if not i.get("SP")]
    com_alerta = [i for i in linhas if pede_conferencia(i)]
    agend: dict = {}
    for i in com_sp:
        # O status de agendamento só existe para SP em "Pagar"; a já paga conta como
        # "Pago" (é o que o extrato confirma).
        chave = str(i.get("Status Agend") or "") or (
            "Pago" if str(i.get("Status Pgt") or "").strip().lower().startswith("pago") else "")
        agend.setdefault(chave, []).append(i)
    por_agend = [{"status": k or "Sem agendamento", "quantos": len(agend[k]),
                  "total": soma(agend[k])}
                 for k in ORDEM_DO_AGENDAMENTO + sorted(set(agend) - set(ORDEM_DO_AGENDAMENTO))
                 if k in agend]
    contas: dict = {}
    for i in linhas:
        chave = str(i.get("Conta (Bradesco)") or "").strip() or "(sem conta)"
        c = contas.setdefault(chave, {"conta": chave, "empresa": i.get("Empresa") or "",
                                      "quantos": 0, "total": 0.0, "com_sp": 0, "sem_sp": 0,
                                      "com_alerta": 0})
        c["quantos"] += 1
        c["total"] = round(c["total"] + float(i.get("_valor_banco") or 0), 2)
        c["com_sp" if i.get("SP") else "sem_sp"] += 1
        c["com_alerta"] += 1 if pede_conferencia(i) else 0
    return {
        "quantos": len(linhas), "total": soma(linhas),
        "boletos": len((resultado or {}).get("boletos") or []),
        "pix": len((resultado or {}).get("pix") or []),
        "com_sp": len(com_sp), "total_com_sp": soma(com_sp),
        "sem_sp": len(sem_sp), "total_sem_sp": soma(sem_sp),
        "com_alerta": len(com_alerta), "total_com_alerta": soma(com_alerta),
        "por_agendamento": por_agend,
        "por_conta": sorted(contas.values(), key=lambda c: -c["total"]),
    }
