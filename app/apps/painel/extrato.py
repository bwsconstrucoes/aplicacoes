# -*- coding: utf-8 -*-
"""
O EXTRATO CONFERÍVEL — uma conta, dia a dia, com saldo.

Pedido do dono em 24/11/2026:

    "Quando a gente abre um extrato bancário, você visualiza os pagamentos,
    mas não consegue associar aquele lançamento a nada (…) A gente pegar o
    extrato da conta corrente, inclusive eu poder comparar com o extrato aberto,
    o extrato de um lado e a conta do outro. E conseguir visualizar o saldo do
    dia. E naquele extrato do painel, eu poder enxergar cada título e a que se
    refere, a que categoria, a qual centro de custo. (…) Vai ter lançamento que
    é de mais de uma obra: a gente poder expandir. (…) Isso traz mais
    credibilidade ao que está lançado."

TRÊS COISAS QUE ESTA VISÃO FAZ E A LISTA ANTIGA DO EXTRATO NÃO FAZIA:

1. **Saldo.** O painel sabe o que andou na conta, não quanto havia nela; o
   saldo de UM dia é informado (tabela `extrato_saldo_informado`) e o dos
   outros sai dele. Sem saldo informado a tela mostra só a variação.
2. **Uma linha por movimento do banco.** Um pagamento rateado em três obras é
   UM débito na conta e são três linhas no `fato`: aqui ele é uma linha, com o
   valor que saiu, e abre nas obras.
3. **A régua é a da conferência de saldo** (`conferencia_saldo.movimento_no_painel`):
   o que foi pago ou recebido, com juros e multa, SEM o imposto retido pelo
   cliente (que nunca passou pela conta), e sem corte de DRE — transferência,
   aporte, tarifa e rendimento aconteceram na conta e aparecem.

O QUE ELA NÃO CONSEGUE, dito na cara: se o banco debita VÁRIOS títulos num
lançamento só (um lote), o banco mostra uma linha e o painel mostra uma por
título — o OMIE não guarda o vínculo. E a ordem dentro do dia é a do número do
lançamento, não a do banco: o que se confere é o saldo no FIM do dia.

A parte de montar (agrupar, somar o saldo) é função pura; as consultas ficam
separadas embaixo.
"""
from __future__ import annotations

import datetime as dt

from .consultas import ENCARGO, OBRA_OU_SEM, PAG, PAGO, REC, RETIDO

# o valor que ANDOU na conta numa linha: o principal mais os encargos pagos
VALOR = f"(pago_recebido + {ENCARGO})"
BASE = f"{PAGO} AND NOT ({RETIDO}) AND data IS NOT NULL AND ABS({VALOR}) > 0.005"

TETO_DE_LINHAS = 6000          # linhas do `fato` num período; acima disso, encurtar

TIPOS = {
    "receita": "Receita", "despesa": "Despesa", "transferencia": "Transferência",
    "fora": "Fora do resultado",
}


def tipo_da_linha(tipo: str, analise: str) -> str:
    """Receita, despesa, transferência ou "fora do resultado" (aporte,
    empréstimo, dividendo, aplicação — o que mexe no caixa sem ser resultado)."""
    analise = (analise or "").strip()
    if analise == "TRF":
        return "transferencia"
    if analise == "DRE":
        return "receita" if tipo == REC else "despesa"
    return "fora"


# ---------------------------------------------------------------------------
# 1. Montar — função pura
# ---------------------------------------------------------------------------
def montar(linhas, saldo_antes=None, conferidos=()) -> dict:
    """O extrato de um período, dia a dia.

    `linhas` são as do `fato` daquela conta, em ordem de data; `saldo_antes` é
    o saldo da conta logo ANTES do primeiro dia do período (None = ninguém
    informou saldo: a coluna vira a variação desde o começo do período)."""
    conferidos = {d.isoformat() if hasattr(d, "isoformat") else str(d) for d in conferidos}
    tem_saldo = saldo_antes is not None
    saldo = float(saldo_antes or 0.0)
    inicial = saldo

    dias: list[dict] = []
    por_chave: dict[tuple, dict] = {}
    for n, l in enumerate(linhas):
        dia = l["data"].isoformat() if hasattr(l["data"], "isoformat") else str(l["data"])
        if not dias or dias[-1]["dia"] != dia:
            dias.append({"dia": dia, "rotulo": f"{dia[8:10]}/{dia[5:7]}/{dia[:4]}",
                         "movimentos": [], "entrou": 0.0, "saiu": 0.0,
                         "conferido": dia in conferidos})
        # o mesmo título no mesmo dia é UM movimento do banco; lançamento sem
        # número não se junta com nada
        chave = (dia, l["codigo"]) if l.get("codigo") is not None else (dia, "linha", n)
        mov = por_chave.get(chave)
        if mov is None:
            mov = por_chave[chave] = {
                "codigo": l.get("codigo"), "tipo": tipo_da_linha(l.get("tipo"), l.get("analise")),
                "quem": (l.get("quem") or "").strip(), "cnpj": (l.get("cnpj") or "").strip(),
                "documento": (l.get("documento") or "").strip(),
                "observacao": (l.get("observacao") or "").strip(), "link": l.get("link") or "",
                "valor": 0.0, "partes": [],
            }
            dias[-1]["movimentos"].append(mov)
        valor = float(l["valor"] or 0)
        mov["valor"] += valor
        mov["partes"].append({"obra": l.get("obra") or "", "projeto": l.get("projeto") or "",
                              "categoria": l.get("categoria") or "", "grupo": l.get("grupo") or "",
                              "valor": valor})

    entrou = saiu = 0.0
    quantos = 0
    for d in dias:
        for mov in d["movimentos"]:
            mov["tipo_rotulo"] = TIPOS[mov["tipo"]]
            obras = sorted({p["obra"] for p in mov["partes"]})
            categorias = sorted({p["categoria"] for p in mov["partes"] if p["categoria"]})
            grupos = sorted({p["grupo"] for p in mov["partes"] if p["grupo"]})
            mov["obra"] = obras[0] if len(obras) == 1 else ""
            mov["n_obras"] = len(obras)
            # rateado é o que tem mais de uma parte para mostrar (obras ou categorias)
            mov["rateado"] = len(mov["partes"]) > 1 and (len(obras) > 1 or len(categorias) > 1)
            mov["categoria"] = (categorias[0] if len(categorias) == 1
                                else f"{len(categorias)} categorias" if categorias else "")
            mov["grupo"] = grupos[0] if len(grupos) == 1 else ""
            mov["valor"] = round(mov["valor"], 2)
            saldo += mov["valor"]
            mov["saldo"] = round(saldo, 2)
            if mov["valor"] >= 0:
                d["entrou"] += mov["valor"]
            else:
                d["saiu"] -= mov["valor"]
        d["saldo_final"] = round(saldo, 2)
        d["quantos"] = len(d["movimentos"])
        quantos += d["quantos"]
        entrou += d["entrou"]
        saiu += d["saiu"]

    return {
        "dias": dias, "quantos": quantos, "tem_saldo": tem_saldo,
        "saldo_inicial": round(inicial, 2), "saldo_final": round(saldo, 2),
        "entrou": round(entrou, 2), "saiu": round(saiu, 2),
        "dias_conferidos": sum(1 for d in dias if d["conferido"]),
    }


def ler_saldo(texto) -> float:
    """O saldo digitado por gente, COM o sinal: conta no cheque especial tem
    saldo negativo, e "-1.250,50", "−1.250,50" e "(1.250,50)" são todos menos
    mil duzentos e cinquenta. Em branco não é zero: é erro — gravar saldo zero
    por engano desalinharia o extrato inteiro sem ninguém ver."""
    from .cenarios import dinheiro
    bruto = str(texto if texto is not None else "").strip()
    if not bruto:
        raise ValueError("o saldo veio em branco")
    negativo = bruto.startswith(("-", "−")) or (bruto.startswith("(") and bruto.endswith(")"))
    valor = dinheiro(bruto.strip("()").lstrip("-−"))
    return -valor if negativo else valor


def periodo_padrao(de: str, ate: str, hoje: dt.date | None = None) -> tuple[str, str]:
    """Sem período pedido, o mês corrente até hoje. Com um lado só, completa o
    outro com o mês dele — extrato sem começo ou sem fim seria a conta inteira."""
    hoje = hoje or dt.date.today()
    if not de and not ate:
        return hoje.replace(day=1).isoformat(), hoje.isoformat()
    if de and not ate:
        inicio = dt.date.fromisoformat(de)
        fim = (inicio.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
        return de, fim.isoformat()
    if ate and not de:
        return dt.date.fromisoformat(ate).replace(day=1).isoformat(), ate
    return de, ate


# ---------------------------------------------------------------------------
# 2. As consultas
# ---------------------------------------------------------------------------
def linhas_da_conta(conta: str, de: str, ate: str) -> list[dict]:
    """O que andou na conta no período, linha a linha do `fato`, em ordem."""
    from .db import consultar
    campos = ("data", "codigo", "tipo", "analise", "quem", "cnpj", "grupo", "categoria",
              "obra", "projeto", "documento", "observacao", "link", "valor")
    return [dict(zip(campos, linha)) for linha in consultar(
        f"""SELECT data, codigo_lancamento, tipo, analise, razao_social, cnpj_cpf,
                   COALESCE(NULLIF(TRIM(grupo),''), ''),
                   COALESCE(NULLIF(TRIM(categoria),''), '(sem categoria)'),
                   {OBRA_OU_SEM}, COALESCE(projeto, ''), numero_documento,
                   observacao, link, {VALOR}
              FROM fato
             WHERE {BASE} AND COALESCE(conta_corrente, '') = ?
               AND data BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
             ORDER BY data, codigo_lancamento NULLS LAST, {OBRA_OU_SEM}
             LIMIT {TETO_DE_LINHAS + 1}""", [conta, de, ate])]


def tabelas_prontas() -> bool:
    """A migração 029 já foi aplicada? Sem ela a tela funciona, só sem saldo
    informado e sem o "conferi" — e diz isso, em vez de dar erro."""
    from .db import consultar
    try:
        linha = consultar("SELECT to_regclass('painel.extrato_saldo_informado') IS NOT NULL")
        return bool(linha and linha[0][0])
    except Exception:  # noqa: BLE001
        return False


def saldo_informado(conta: str) -> dict | None:
    from .db import consultar
    linhas = consultar("SELECT dia, saldo, informado_por, informado_em"
                       "  FROM extrato_saldo_informado WHERE conta = ?", [conta])
    if not linhas:
        return None
    dia, saldo, por, em = linhas[0]
    return {"dia": dia, "saldo": float(saldo), "por": por or "", "em": em}


def saldo_antes_de(conta: str, de: str, informado: dict | None):
    """O saldo da conta logo ANTES do dia `de`, a partir do saldo informado
    (que é o do FIM do dia dele): soma o que andou depois dele até a véspera
    de `de`, ou tira o que andou de `de` até ele."""
    if not informado:
        return None
    from .db import consultar
    dia = informado["dia"].isoformat()
    linha = consultar(
        f"""SELECT COALESCE(SUM(CASE WHEN data > CAST(? AS DATE) AND data < CAST(? AS DATE)
                                      THEN {VALOR}
                                      WHEN data >= CAST(? AS DATE) AND data <= CAST(? AS DATE)
                                      THEN -{VALOR} ELSE 0 END), 0)
              FROM fato
             WHERE {BASE} AND COALESCE(conta_corrente, '') = ?""",
        [dia, de, de, dia, conta])
    return float(informado["saldo"]) + float(linha[0][0] or 0)


def gravar_saldo(conta: str, dia: str, saldo: float, por: str = "") -> None:
    from .db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO extrato_saldo_informado (conta, dia, saldo, informado_por, informado_em)"
            " VALUES (?, CAST(? AS DATE), ?, ?, now())"
            " ON CONFLICT (conta) DO UPDATE SET dia = EXCLUDED.dia, saldo = EXCLUDED.saldo,"
            "   informado_por = EXCLUDED.informado_por, informado_em = now()",
            (conta, dia, float(saldo), por))
        conn.commit()


def dias_conferidos(conta: str, de: str, ate: str) -> set:
    from .db import consultar
    return {d.isoformat() for (d,) in consultar(
        "SELECT dia FROM extrato_dia_conferido WHERE conta = ?"
        "   AND dia BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)", [conta, de, ate])}


def marcar_dia(conta: str, dia: str, conferido: bool, por: str = "") -> None:
    from .db import conexao
    with conexao() as conn:
        if conferido:
            conn.execute(
                "INSERT INTO extrato_dia_conferido (conta, dia, conferido_por)"
                " VALUES (?, CAST(? AS DATE), ?) ON CONFLICT (conta, dia) DO NOTHING",
                (conta, dia, por))
        else:
            conn.execute("DELETE FROM extrato_dia_conferido"
                         " WHERE conta = ? AND dia = CAST(? AS DATE)", (conta, dia))
        conn.commit()


def do_pedido(conta: str, de: str, ate: str) -> dict:
    """Tudo o que a tela e o arquivo precisam: lê, monta e diz o que faltou."""
    pronto = tabelas_prontas()
    informado = saldo_informado(conta) if pronto else None
    linhas = linhas_da_conta(conta, de, ate)
    cortado = len(linhas) > TETO_DE_LINHAS
    if cortado:
        # corta em dia INTEIRO: metade de um dia daria um saldo do dia errado
        ultimo = linhas[TETO_DE_LINHAS]["data"]
        linhas = [l for l in linhas[:TETO_DE_LINHAS] if l["data"] < ultimo]
    dados = montar(linhas, saldo_antes_de(conta, de, informado),
                   dias_conferidos(conta, de, ate) if pronto else ())
    dados.update({"conta": conta, "de": de, "ate": ate, "informado": informado,
                  "tabelas_prontas": pronto, "cortado": cortado})
    return dados


# ---------------------------------------------------------------------------
# 3. O arquivo: o mesmo extrato, para a planilha e para o PDF
# ---------------------------------------------------------------------------
def abas(dados: dict) -> list[tuple]:
    saldo_titulo = "Saldo" if dados["tem_saldo"] else "Saldo (variação no período)"
    colunas = [("data", "Data"), ("tipo", "Tipo"), ("quem", "Cliente ou fornecedor"),
               ("cnpj", "CNPJ/CPF"), ("categoria", "Categoria"), ("obra", "Obra / centro de custo"),
               ("documento", "Documento"), ("codigo", "Nº no OMIE"),
               ("observacao", "Observação"), ("entrou", "Valor que entrou"),
               ("saiu", "Valor que saiu"), ("parte", "Valor da parte (rateio)"),
               ("saldo", saldo_titulo)]
    linhas = []
    if dados["tem_saldo"]:
        linhas.append({"data": "", "tipo": "SALDO ANTERIOR", "quem": "", "cnpj": "", "categoria": "", "obra": "",
                       "documento": "", "codigo": "", "observacao": "", "entrou": "", "saiu": "",
                       "parte": "", "saldo": dados["saldo_inicial"]})
    for d in dados["dias"]:
        for m in d["movimentos"]:
            linhas.append({
                "data": d["rotulo"], "tipo": m["tipo_rotulo"], "quem": m["quem"],
                "cnpj": m["cnpj"], "categoria": m["categoria"],
                "obra": m["obra"] or f"rateado em {m['n_obras']} obras",
                "documento": m["documento"], "codigo": m["codigo"] or "",
                "observacao": m["observacao"],
                "entrou": m["valor"] if m["valor"] >= 0 else "",
                "saiu": -m["valor"] if m["valor"] < 0 else "",
                "parte": "", "saldo": m["saldo"]})
            if m["rateado"]:
                for p in m["partes"]:
                    linhas.append({"data": "", "tipo": "   parte", "quem": "", "cnpj": "", "categoria": p["categoria"],
                                   "obra": p["obra"], "documento": "", "codigo": "", "observacao": "",
                                   "entrou": "", "saiu": "", "parte": p["valor"], "saldo": ""})
    por_dia = [{"dia": d["rotulo"], "quantos": d["quantos"], "entrou": d["entrou"],
                "saiu": d["saiu"], "saldo": d["saldo_final"],
                "conferido": "sim" if d["conferido"] else ""} for d in dados["dias"]]
    return [
        ("Extrato dia a dia", colunas, linhas),
        ("Saldo por dia", [("dia", "Dia"), ("quantos", "Movimentos"), ("entrou", "Valor que entrou"),
                           ("saiu", "Valor que saiu"), ("saldo", saldo_titulo + " no fim do dia"),
                           ("conferido", "Conferido com o banco")], por_dia),
    ]
