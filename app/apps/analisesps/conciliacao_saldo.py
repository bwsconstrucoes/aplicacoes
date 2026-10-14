# -*- coding: utf-8 -*-
"""
ONDE O SALDO DEIXA DE BATER — 14/10/2026.

O dono: *"uma determinada conta (…) não está batendo saldo, não é só questão
de ajustar o saldo inicial (…) normalmente a gente vem do passado para o
futuro tentando fazer bater; e se a gente fizer o inverso, do presente para o
passado, (…) ir buscar onde é que não está batendo, em que dia aconteceu um
lançamento a mais, um a menos?"*

O OFX traz UM saldo só — o do dia em que foi baixado (o Bradesco nem diz o dia:
manda "00000000"). Um arquivo sozinho só diz "no fim bate" ou "não bate". Mas
cada arquivo importado guarda o seu saldo, e a pessoa pode digitar o saldo de
qualquer dia, tirado do extrato do banco. Cada um é um PONTO DE CONFERÊNCIA:

    em 30/09 o banco dizia X e aqui dava X − 300
    em 15/09 o banco dizia Y e aqui dava Y          → o erro está entre 15 e 30/09

Do presente para o passado: o ponto mais novo primeiro; onde a diferença MUDA
de um ponto para o anterior está o desencontro. Dentro daquela janela a tela
aponta os suspeitos (linha do mesmo valor da diferença, linha repetida, duas
que somam a diferença, linha que não veio de extrato) e sugere o dia do meio
para o próximo saldo — cada saldo digitado corta a janela pela metade.

Os saldos digitados ficam em `analisesps.meta` (sem migração): poucos por
conta, e apagáveis.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from decimal import Decimal

logger = logging.getLogger("analisesps.conciliacao")

MAX_PONTOS_DIGITADOS = 200
MAX_LINHAS_NA_JANELA = 400      # acima disso, não procura pares
MAX_SUSPEITOS = 15


class ErroDoSaldo(Exception):
    """A frase vai para a tela."""


def _chave(conta_id: int) -> str:
    return f"conciliacao_saldos_{int(conta_id)}"


# ---------------------------------------------------------------------------
# Os saldos digitados
# ---------------------------------------------------------------------------
def digitados(conta_id: int) -> list[dict]:
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (_chave(conta_id),))
        dados = json.loads(linha[0]) if linha and linha[0] else []
    except Exception:  # noqa: BLE001 — lista ruim não derruba a tela
        logger.exception("Conciliação: não consegui ler os saldos digitados")
        dados = []
    return [d for d in dados if isinstance(d, dict) and d.get("data")]


def _gravar(conta_id: int, pontos: list[dict]) -> None:
    from .db import conexao
    from .sincronizacao import _meta_gravar
    with conexao() as conn:
        _meta_gravar(conn, _chave(conta_id), json.dumps(pontos, ensure_ascii=False))


def gravar_ponto(conta_id: int, data_txt, saldo_txt, quem: str = "") -> None:
    """O saldo do banco no FIM de um dia. Um por dia: o novo substitui."""
    from .formatos import para_data, para_numero
    dia = para_data(str(data_txt or "").strip())
    if not dia:
        raise ErroDoSaldo("Informe o dia.")
    saldo = para_numero(str(saldo_txt or "").strip())
    if saldo is None:
        raise ErroDoSaldo("Informe o saldo do banco no fim daquele dia (ex.: 2.607,21).")
    pontos = [p for p in digitados(conta_id) if p.get("data") != dia.isoformat()]
    if len(pontos) >= MAX_PONTOS_DIGITADOS:
        raise ErroDoSaldo(f"Já há {MAX_PONTOS_DIGITADOS} saldos digitados nesta conta — apague algum.")
    pontos.append({"data": dia.isoformat(), "saldo": str(Decimal(str(saldo)).quantize(Decimal("0.01"))),
                   "por": str(quem or "")[:80]})
    _gravar(conta_id, sorted(pontos, key=lambda p: p["data"]))
    logger.info("Conciliação: %s informou o saldo de %s na conta %s: %s",
                quem or "?", dia, conta_id, saldo)


def apagar_ponto(conta_id: int, data_txt) -> None:
    from .formatos import para_data
    dia = para_data(str(data_txt or "").strip())
    if not dia:
        raise ErroDoSaldo("Informe o dia.")
    _gravar(conta_id, [p for p in digitados(conta_id) if p.get("data") != dia.isoformat()])


# ---------------------------------------------------------------------------
# Os pontos e a análise
# ---------------------------------------------------------------------------
def _dos_arquivos(conta_id: int) -> list[dict]:
    """Cada OFX importado com saldo. Sem a data do saldo (Bradesco antigo), vale
    o fim do período gravado — que, no Bradesco, é o dia do download."""
    from .db import consultar
    linhas = consultar(
        "SELECT nome_arquivo, saldo_ofx, coalesce(saldo_ofx_em, periodo_fim), importado_em "
        "  FROM analisesps.conciliacao_arquivo "
        " WHERE conta_id = ? AND saldo_ofx IS NOT NULL "
        "   AND coalesce(saldo_ofx_em, periodo_fim) IS NOT NULL", (int(conta_id),))
    return [{"data": l[2], "saldo": Decimal(str(l[1])), "origem": "arquivo",
             "rotulo": f"OFX {l[0] or ''}".strip()} for l in linhas]


def pontos(conta_id: int) -> list[dict]:
    """Todos os pontos, do mais NOVO para o mais antigo, com o saldo daqui e a
    diferença."""
    from . import conciliacao as conc
    conta = next((c for c in conc.contas(so_ativas=False) if c["id"] == int(conta_id)), None) or {}
    inicial_em = conta.get("saldo_inicial_em")

    brutos = _dos_arquivos(conta_id) + [
        {"data": date.fromisoformat(p["data"]), "saldo": Decimal(str(p["saldo"])),
         "origem": "digitado", "rotulo": "digitado" + (f" por {p['por']}" if p.get("por") else "")}
        for p in digitados(conta_id)]
    saida = []
    for p in brutos:
        if inicial_em and p["data"] < inicial_em:
            continue          # antes do ponto de partida não há conta a fazer
        aqui = conc.saldo_da_conta(conta_id, p["data"])
        saida.append(dict(p, aqui=aqui, diferenca=p["saldo"] - aqui))
    if inicial_em:
        inicial = Decimal(str(conta.get("saldo_inicial") or 0))
        saida.append({"data": inicial_em, "saldo": inicial, "origem": "inicial",
                      "rotulo": "início da conciliação", "aqui": inicial,
                      "diferenca": Decimal("0")})
    # Do presente para o passado; no mesmo dia, o digitado antes do arquivo
    # (é o saldo do FIM do dia; o do arquivo é o da hora do download).
    ordem = {"digitado": 0, "arquivo": 1, "inicial": 2}
    saida.sort(key=lambda p: (-p["data"].toordinal(), ordem[p["origem"]]))
    return saida


def _linhas(conta_id: int, depois_de: date, ate: date) -> list[dict]:
    from .db import consultar
    linhas = consultar(
        "SELECT id, data, descricao, valor, origem FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND data > ? AND data <= ? ORDER BY data, id",
        (int(conta_id), depois_de, ate))
    return [dict(zip(("id", "data", "descricao", "valor", "origem"), l)) for l in linhas]


def suspeitos(linhas: list[dict], mudanca: Decimal) -> dict:
    """O que, dentro da janela, explica a diferença que apareceu nela.

    `mudanca` = quanto a diferença (banco − aqui) cresceu na janela. Positiva:
    aqui falta entrada (ou sobra saída) desse tamanho; negativa: o contrário."""
    alvo = abs(mudanca)
    mesmo_valor = [l for l in linhas if abs(Decimal(str(l["valor"]))) == alvo]

    # Repetidas: mesmo dia e mesmo valor, ou mesmo valor e mesmo texto em até 3
    # dias (a mesma transação entrando com duas datas).
    repetidas, vistos = [], set()
    for i, a in enumerate(linhas):
        for b in linhas[i + 1:]:
            if (b["data"] - a["data"]).days > 3:
                break
            if Decimal(str(a["valor"])) != Decimal(str(b["valor"])):
                continue
            mesmo_texto = (a["descricao"] or "").strip().upper() == (b["descricao"] or "").strip().upper()
            if a["data"] == b["data"] or mesmo_texto:
                for x in (a, b):
                    if x["id"] not in vistos:
                        vistos.add(x["id"])
                        repetidas.append(x)

    pares = []
    if alvo and len(linhas) <= MAX_LINHAS_NA_JANELA:
        for i, a in enumerate(linhas):
            for b in linhas[i + 1:]:
                if abs(Decimal(str(a["valor"])) + Decimal(str(b["valor"]))) == alvo:
                    pares.append([a, b])
                    if len(pares) >= 5:
                        break
            if len(pares) >= 5:
                break

    sem_extrato = [l for l in linhas if l.get("origem") in ("mao", "planilha")]
    return {"mesmo_valor": mesmo_valor[:MAX_SUSPEITOS],
            "repetidas": repetidas[:MAX_SUSPEITOS],
            "pares": pares,
            "sem_extrato": sem_extrato[:MAX_SUSPEITOS],
            "sem_extrato_total": len(sem_extrato)}


def diagnostico(conta_id: int) -> dict:
    """Os pontos e as janelas onde a diferença muda, do presente para o passado."""
    lista = pontos(conta_id)
    janelas = []
    for novo, velho in zip(lista, lista[1:]):
        mudanca = novo["diferenca"] - velho["diferenca"]
        if mudanca == 0 or novo["data"] == velho["data"]:
            continue
        linhas = _linhas(conta_id, velho["data"], novo["data"])
        meio = None
        dias = (novo["data"] - velho["data"]).days
        if dias > 1:
            meio = velho["data"] + timedelta(days=dias // 2)
        janelas.append({"de": velho["data"], "ate": novo["data"], "mudanca": mudanca,
                        "linhas": len(linhas), "dia_do_meio": meio,
                        **suspeitos(linhas, mudanca)})
    # A mesma data com dois saldos diferentes (arquivo × digitado) também é
    # informação: costuma ser lançamento do dia que o download ainda não tinha.
    return {"pontos": lista, "janelas": janelas,
            "bate_hoje": bool(lista) and lista[0]["diferenca"] == 0}


def para_a_tela(d: dict) -> dict:
    def br(x):
        return x.isoformat() if x else ""

    def linha(l):
        return {"id": l["id"], "data": br(l["data"]), "descricao": (l["descricao"] or "")[:120],
                "valor": str(l["valor"]), "origem": l.get("origem") or ""}
    return {
        "bate_hoje": d["bate_hoje"],
        "pontos": [{"data": br(p["data"]), "saldo": str(p["saldo"]), "aqui": str(p["aqui"]),
                    "diferenca": str(p["diferenca"]), "origem": p["origem"],
                    "rotulo": p["rotulo"]} for p in d["pontos"]],
        "janelas": [{"de": br(j["de"]), "ate": br(j["ate"]), "mudanca": str(j["mudanca"]),
                     "linhas": j["linhas"], "dia_do_meio": br(j["dia_do_meio"]),
                     "mesmo_valor": [linha(l) for l in j["mesmo_valor"]],
                     "repetidas": [linha(l) for l in j["repetidas"]],
                     "pares": [[linha(a), linha(b)] for a, b in j["pares"]],
                     "sem_extrato": [linha(l) for l in j["sem_extrato"]],
                     "sem_extrato_total": j["sem_extrato_total"]} for j in d["janelas"]],
    }
