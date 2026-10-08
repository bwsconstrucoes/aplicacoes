# -*- coding: utf-8 -*-
"""
Conferir o saldo de cada conta, painel × OMIE, no mês ou no ano inteiro.

07/10/2026, o dono: *"toda vida que eu vou analisar uma obra, eu descubro alguma
coisa que não está sendo vista"* — e, comparando com o BI da Controladoria:
*"o que a gente precisa fazer para ficar em caráter definitivo?"*

É a rede que pega o que ninguém previu. Para cada conta, no mês: quanto o
dinheiro andou segundo o OMIE (saldo final menos saldo anterior, do extrato) e
quanto o painel conta (a soma do que ele dá como pago ou recebido naquela conta,
com juros e multa, sem o imposto retido — que nunca passou pela conta). Se não
bate, falta (ou sobra) alguma coisa no painel naquela conta e mês.

⚠️ O FORMATO DA RESPOSTA DO EXTRATO NÃO FOI CONFIRMADO contra o OMIE real (a
documentação não é alcançável daqui). Os saldos são procurados pelo NOME do
campo; quando não acha, a linha diz isso e a resposta crua pode ser baixada
para ajustar com o dado verdadeiro.
"""
from __future__ import annotations

import datetime as dt
import logging
import time

logger = logging.getLogger("painel.conferencia_saldo")

VALIDADE = 150.0                 # o OMIE recusa a mesma chamada repetida em seguida
_LIDOS: dict = {}


def _periodo(mes: str, hoje: dt.date | None = None) -> tuple[dt.date, dt.date]:
    """"AAAA-MM" é o mês; "AAAA" é o ano inteiro, até hoje se for o corrente
    (dono, 07/10/2026: "são dados apenas deste ano que preciso hoje")."""
    if len(mes) == 4:
        ano = int(mes)
        hoje = hoje or dt.date.today()
        return dt.date(ano, 1, 1), min(dt.date(ano, 12, 31), hoje)
    ano, m = (int(x) for x in mes.split("-"))
    ini = dt.date(ano, m, 1)
    fim = (dt.date(ano + (m == 12), m % 12 + 1, 1) - dt.timedelta(days=1))
    return ini, fim


def _cliente():
    from .sync.omie_client import OmieClient, TETO_DE_ESPERA_NA_TELA
    return OmieClient.de_ambiente(teto_de_espera=TETO_DE_ESPERA_NA_TELA)


def extrato_cru(codigo_conta: int, mes: str, cliente=None) -> dict:
    """A resposta do OMIE, guardada por 150 s (conferir e baixar não gastam
    duas chamadas iguais)."""
    chave = (int(codigo_conta), mes)
    agora = time.monotonic()
    if chave in _LIDOS and agora - _LIDOS[chave][0] < VALIDADE:
        return _LIDOS[chave][1]
    ini, fim = _periodo(mes)
    resposta = (cliente or _cliente()).extrato(int(codigo_conta), f"{ini:%d/%m/%Y}",
                                               f"{fim:%d/%m/%Y}")
    _LIDOS[chave] = (agora, resposta)
    return resposta


def _numero(v):
    try:
        if isinstance(v, str):
            v = v.replace(".", "").replace(",", ".") if "," in v else v
        return float(v)
    except (TypeError, ValueError):
        return None


def saldos_da_resposta(resposta) -> dict:
    """{anterior, final} achados pelo nome do campo, no primeiro nível ou num
    nível abaixo. None no que não achou."""
    achados = {}

    def olhar(d):
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            nome = str(k).lower()
            if "saldo" not in nome:
                continue
            n = _numero(v)
            if n is None:
                continue
            if "anterior" in nome or "inicial" in nome:
                achados.setdefault("anterior", n)
            elif any(p in nome for p in ("atual", "final")) and "dispon" not in nome:
                achados.setdefault("final", n)

    if isinstance(resposta, dict):
        olhar(resposta)
        for v in resposta.values():
            if isinstance(v, dict):
                olhar(v)
    return {"anterior": achados.get("anterior"), "final": achados.get("final")}


def movimento_no_painel(mes: str) -> dict:
    """{nome da conta: quanto o painel conta que andou nela no mês}."""
    from .consultas import ENCARGO, PAGO, RETIDO
    from .db import consultar
    ini, fim = _periodo(mes)
    return {conta: float(v or 0) for conta, v in consultar(
        f"""SELECT COALESCE(conta_corrente, ''), SUM(pago_recebido + {ENCARGO})
              FROM fato
             WHERE {PAGO} AND NOT ({RETIDO})
               AND data BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
             GROUP BY 1""", [ini.isoformat(), fim.isoformat()])}


def contas_ativas() -> list[tuple[int, str]]:
    from .db import consultar
    return [(int(c), (d or "").strip() or str(c)) for c, d in consultar(
        "SELECT codigo, descricao FROM contas_correntes"
        " WHERE COALESCE(inativa, '') <> 'S' ORDER BY descricao")]


def conferir_mes(mes: str, cliente=None) -> dict:
    """Conta por conta: OMIE, painel, diferença — e o que não deu para ler."""
    painel = movimento_no_painel(mes)
    cliente = cliente or _cliente()
    linhas = []
    for codigo, nome in contas_ativas():
        linha = {"codigo": codigo, "conta": nome,
                 "painel": round(painel.get(nome, 0.0), 2),
                 "omie": None, "diferenca": None, "problema": ""}
        try:
            saldos = saldos_da_resposta(extrato_cru(codigo, mes, cliente))
        except Exception as e:  # noqa: BLE001 — uma conta não derruba as outras
            logger.warning("Conferência de saldo: %s %s falhou (%s)", nome, mes, e)
            linha["problema"] = f"o OMIE não respondeu: {e}"
            linhas.append(linha)
            continue
        if saldos["anterior"] is None or saldos["final"] is None:
            linha["problema"] = ("não achei os saldos na resposta do OMIE — baixe a "
                                 "resposta crua e me mande")
        else:
            linha["omie"] = round(saldos["final"] - saldos["anterior"], 2)
            linha["diferenca"] = round(linha["painel"] - linha["omie"], 2)
        linhas.append(linha)
    com_diferenca = [l for l in linhas if l["diferenca"] and abs(l["diferenca"]) > 0.05]
    ini, fim = _periodo(mes)
    return {"mes": mes, "de": f"{ini:%d/%m/%Y}", "ate": f"{fim:%d/%m/%Y}",
            "linhas": linhas, "com_diferenca": len(com_diferenca),
            "sem_leitura": sum(1 for l in linhas if l["problema"])}
