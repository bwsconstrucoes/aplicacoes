# -*- coding: utf-8 -*-
"""
Conferir um dia do painel com o OMIE, na hora.

Pedido do dono em 23/09/2026, depois do SP1343985444 (pago na 22069 segundo o
OMIE, e na 7011 segundo o painel): *"era interessante uma forma de extrair a
informação completa da base e a do OMIE — consultando o OMIE e podendo
confrontar os dados pra entender onde há diferença."*

Como funciona: para UM dia, lê no OMIE todos os pagamentos e recebimentos com
aquela data de pagamento (a mesma consulta que a carga usa, `ListarMovimentos`
por `dDtPagtoDe/Ate`) e compara, perna a perna, com o que o espelho do painel
guardou para o mesmo dia. O que estiver só de um lado, ou com conta ou valor
diferente, aparece.

Por que por dia, e não a base inteira: a base tem centenas de milhares de
movimentos, e ler tudo do OMIE leva horas — é a carga inicial. Um dia são uma
ou duas páginas, cabem numa tela. Para corrigir em massa existe a atualização,
que desde 23/09 relê 30 dias (180 na completa).

Não escreve no OMIE. "Trazer do OMIE" substitui o dia no ESPELHO do painel
(apaga e regrava os movimentos daquele dia) — o mesmo que a atualização faria
se o dia estivesse na janela dela.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from collections import Counter

logger = logging.getLogger(__name__)

# A OMIE recusa a MESMA chamada repetida em menos de um minuto ("consumo
# redundante"). Conferir e baixar a planilha do mesmo dia seriam duas chamadas
# iguais em segundos — por isso a resposta fica guardada um pouco.
VALIDADE_DA_LEITURA = 150.0
_LIDOS: dict[str, tuple[float, list]] = {}

# Um dia raramente passa de uma página (500 registros). O teto evita que um
# dia anômalo prenda a tela.
MAX_PAGINAS_POR_DIA = 10


def _texto_do_dia(dia) -> str:
    if isinstance(dia, str):
        dia = dt.date.fromisoformat(dia)
    return dia.strftime("%d/%m/%Y")


def _cliente():
    from .sync.omie_client import OmieClient, TETO_DE_ESPERA_NA_TELA
    return OmieClient.de_ambiente(teto_de_espera=TETO_DE_ESPERA_NA_TELA)


def registros_do_omie(dia, cliente=None, *, usar_guardado=True) -> list:
    """Os movimentos crus que o OMIE tem com esta data de pagamento."""
    chave = _texto_do_dia(dia)
    agora = time.monotonic()
    guardado = _LIDOS.get(chave)
    if usar_guardado and guardado and agora - guardado[0] < VALIDADE_DA_LEITURA:
        return guardado[1]
    cliente = cliente or _cliente()
    registros = []
    for _p, _t, _r, lote in cliente.listar_movimentos(
            param_extra={"dDtPagtoDe": chave, "dDtPagtoAte": chave},
            max_paginas=MAX_PAGINAS_POR_DIA):
        registros.extend(lote or [])
    _LIDOS[chave] = (agora, registros)
    return registros


def _nomes_das_contas() -> dict:
    from .db import consultar
    return {c: (d or "").strip() for c, d in consultar(
        "SELECT codigo, descricao FROM contas_correntes")}


def _nome(contas, codigo) -> str:
    if codigo in (None, ""):
        return "(sem conta)"
    try:
        return contas.get(int(codigo)) or str(codigo)
    except (TypeError, ValueError):
        return str(codigo)


def _perna(codigo, natureza, conta, valor, liquidado) -> dict:
    return {"codigo": int(codigo) if codigo not in (None, "") else None,
            "natureza": (natureza or "").strip(),
            "conta": conta, "valor": round(float(valor or 0), 2),
            "liquidado": (liquidado or "").strip()}


def _chave(p) -> tuple:
    return (p["codigo"], str(p["conta"] or ""), p["valor"], p["liquidado"])


def pernas_do_omie(registros) -> list[dict]:
    """Os movimentos do OMIE lidos pela MESMA função que a carga usa para
    gravá-los — o que se compara é exatamente o que o espelho teria."""
    from .sync.espelho import _linha_movimento
    saida = []
    for mv in registros:
        linha = _linha_movimento(mv)
        # (ncodtitulo, cnatureza, cgrupo, cstatus, ccodcateg, ncodcc, ...,
        #  ddtpagamento[7], ..., cliquidado[11], nvalortitulo[12], nvalpago[13])
        saida.append(_perna(linha[0], linha[1], linha[5], linha[13], linha[11]))
    return saida


def pernas_do_espelho(dia) -> list[dict]:
    from .db import consultar
    saida = [_perna(cod, nat, cc, vpg, liq) for cod, nat, cc, vpg, liq in consultar(
        "SELECT ncodtitulo, cnatureza, ncodcc, nvalpago::float8, cliquidado"
        "  FROM movimentos WHERE ddtpagamento = ?", [_texto_do_dia(dia)])]
    saida += [_perna(None, nat, cc, vpg, liq) for nat, cc, vpg, liq in consultar(
        "SELECT cnatureza, ncodcc, nvalpago::float8, cliquidado"
        "  FROM movimentos_sem_titulo WHERE ddtpagamento = ?", [_texto_do_dia(dia)])]
    return saida


def _quem_e(codigos) -> dict:
    """Fornecedor/cliente, documento e obra de cada título, pelo painel."""
    from .db import consultar
    codigos = [c for c in codigos if c is not None]
    if not codigos:
        return {}
    return {cod: {"quem": quem or "", "documento": doc or "", "obra": obra or "",
                  "link": link or ""}
            for cod, quem, doc, obra, link in consultar(
                "SELECT codigo_lancamento, MIN(razao_social), MIN(numero_documento),"
                "       MIN(departamento), MIN(link)"
                "  FROM fato WHERE codigo_lancamento = ANY(?) GROUP BY 1", [codigos])}


def comparar(no_painel: list[dict], no_omie: list[dict]) -> dict:
    """Perna a perna. Chave: título, conta, valor e se é a baixa consolidada.

    Devolve os títulos com diferença (cada um com as pernas dos dois lados) e
    os totais. Uma perna igual dos dois lados não aparece."""
    lado_p, lado_o = Counter(map(_chave, no_painel)), Counter(map(_chave, no_omie))
    so_painel = lado_p - lado_o
    so_omie = lado_o - lado_p
    com_diferenca = sorted({k[0] for k in so_painel} | {k[0] for k in so_omie},
                           key=lambda c: (c is None, c or 0))
    titulos = []
    for cod in com_diferenca:
        titulos.append({
            "codigo": cod,
            "no_painel": [p for p in no_painel if p["codigo"] == cod],
            "no_omie": [p for p in no_omie if p["codigo"] == cod],
            "so_no_painel": sum(n for k, n in so_painel.items() if k[0] == cod),
            "so_no_omie": sum(n for k, n in so_omie.items() if k[0] == cod),
        })
    return {"titulos": titulos,
            "pernas_no_painel": len(no_painel), "pernas_no_omie": len(no_omie),
            "iguais": sum((lado_p & lado_o).values()),
            "bate": not titulos}


def conferir_dia(dia, cliente=None) -> dict:
    """A conferência inteira de um dia, pronta para a tela."""
    no_omie = pernas_do_omie(registros_do_omie(dia, cliente))
    no_painel = pernas_do_espelho(dia)
    resultado = comparar(no_painel, no_omie)
    contas = _nomes_das_contas()
    quem = _quem_e([t["codigo"] for t in resultado["titulos"]])
    for t in resultado["titulos"]:
        t.update(quem.get(t["codigo"], {"quem": "", "documento": "", "obra": "", "link": ""}))
        if t["codigo"] is None:
            t["quem"] = "(movimento sem título)"
        for lado in ("no_painel", "no_omie"):
            for p in t[lado]:
                p["conta_nome"] = _nome(contas, p["conta"])
                p["perna"] = ("baixa consolidada" if p["liquidado"] == "S"
                              else "previsão" if p["liquidado"] == "N"
                              else "baixa bancária")
    resultado["dia"] = _texto_do_dia(dia)
    return resultado


def linhas_para_planilha(resultado: dict) -> dict:
    """As três abas da planilha: as diferenças, e os dois lados inteiros."""
    difs = []
    for t in resultado["titulos"]:
        for lado, rotulo in (("no_painel", "Painel"), ("no_omie", "OMIE")):
            for p in t[lado]:
                difs.append({"codigo": t["codigo"], "quem": t["quem"],
                             "documento": t["documento"], "obra": t["obra"],
                             "lado": rotulo, "perna": p["perna"],
                             "conta": p["conta_nome"], "valor": p["valor"]})
    return {"diferencas": difs}


def trazer_dia_do_omie(dia, cliente=None) -> dict:
    """Substitui o dia no ESPELHO pelo que o OMIE tem. Não mexe no OMIE.

    Mesmo apaga-e-regrava da atualização do dia, restrito a um dia. As telas só
    mudam depois de refazer os números — quem chama dispara isso."""
    from .db import conexao
    from .sync.espelho import _apagar_movimentos_janela, gravar_movimentos
    registros = registros_do_omie(dia, cliente, usar_guardado=True)
    d = dt.date.fromisoformat(dia) if isinstance(dia, str) else dia
    with conexao() as conn:
        apagados = _apagar_movimentos_janela(conn, d, d)
        gravados, sem_titulo = gravar_movimentos(conn, registros)
    logger.info("Painel: dia %s trazido do OMIE — apagou %d, gravou %d (+%d sem título).",
                d, apagados, gravados, sem_titulo)
    return {"apagados": apagados, "gravados": gravados, "sem_titulo": sem_titulo}
