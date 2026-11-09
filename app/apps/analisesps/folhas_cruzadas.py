# -*- coding: utf-8 -*-
"""
AS OUTRAS FOLHAS DA PESSOA — o cruzamento das telas da folha (05/11/2026).

O dono: *"eu preciso fazer conversar as folhas de pagamento (…) enxergar dentro de
despesa colaboradores uma informaçãozinha (…) se aquela pessoa recebeu algo na folha
da contabilidade, quanto. Quanto com alimentação, quanto com transporte. Quanto de
gratificados (…) é uma informação relevante na tomada de decisão e para entender se
tem alguma coisa errada. Isso vale para todos (…) o que vai definir se a informação
vai ser exibida ou não é exatamente o que está ou não selecionado."*

Visto o exemplo em tela (só desenho), ele aprovou: *"acho que pode ser dessa forma"* —
com as duas sugestões que acompanhavam o exemplo: o alerta amarelo para a combinação
que pede conferência, e a contabilidade separada por quinzena e fim do mês.

O DESENHO:
  · `resumo(ano, mes)` — `{cpf: [{"folha", "valor", "obs"}]}` com o que está
    SELECIONADO em cada tela da folha naquela competência. Cada fonte é lida pela
    MESMA função que monta a tela dela (nada de recalcular a regra aqui), e uma fonte
    que falha não derruba as outras: vira aviso.
  · Guardado em memória por competência: montar o resumo passa por todas as telas,
    e isso não pode pesar em cada visita. Sai da memória quando QUALQUER tela salva
    seleção (`esquecer`), e sozinho depois de `VALIDADE_S`.
  · A tela pede as etiquetas DEPOIS de abrir (rota `folha_outras_folhas`), e o
    script `_outras_folhas.html` as põe embaixo do nome. A tela nunca espera o
    cruzamento para aparecer.
  · `etiquetas_da_pessoa` tira a folha da própria tela ("a tela em que você está não
    se repete") e marca em amarelo as combinações de `ALERTAS`.
"""
from __future__ import annotations

import logging
import threading
import time
from decimal import Decimal

logger = logging.getLogger("analisesps.folhas_cruzadas")

# A ordem das etiquetas é a ordem das telas na faixa de abas.
ORDEM = ("contab_quinzena", "contab_mes", "contab", "alimentacao", "transporte",
         "diarista", "dc", "avulso", "gratificados", "viasp")

ROTULOS = {
    "contab_quinzena": "Contab. quinz.",
    "contab_mes": "Contab. fim do mês",
    "contab": "Contab.",
    "alimentacao": "Alim.",
    "transporte": "Transp.",
    "diarista": "Diarista",
    "dc": "DC",
    "avulso": "Avulso",
    "gratificados": "Grat.",
    "viasp": "Via SP",
}

# Qual folha é a "própria" de cada tela (sai das etiquetas daquela tela). Na tela de
# auxílios a régua é o tipo aberto: na alimentação, o transporte continua aparecendo.
PROPRIAS = {
    "importar": {"contab_quinzena", "contab_mes", "contab"},
    "alimentacao": {"alimentacao"},
    "transporte": {"transporte"},
    "diaristas": {"diarista"},
    "dc": {"dc"},
    "avulso": {"avulso"},
    "gratificados": {"gratificados"},
    "viasp": {"viasp"},
}

# AS COMBINAÇÕES QUE PEDEM CONFERÊNCIA (as duas sugeridas e aceitas em 05/11/2026).
# Cada uma: (folhas que, juntas, acendem o alerta; a frase do porquê). A DC conta
# pelo TIPO da despesa (`dc_alimentacao`, `dc_transporte`), não como DC inteira.
ALERTAS = (
    ({"diarista", "contab"}, "recebe como diarista E está na folha da contabilidade"),
    ({"alimentacao", "dc_alimentacao"}, "alimentação no auxílio E numa despesa com colaboradores"),
    ({"transporte", "dc_transporte"}, "transporte no auxílio E numa despesa com colaboradores"),
)

VALIDADE_S = 10 * 60
_guardados: dict = {}
_montando: dict = {}
_trava = threading.Lock()


def _familia(folha: str) -> set:
    """As chaves com que uma folha participa dos alertas."""
    if folha.startswith("contab"):
        return {"contab"}
    return {folha}


def chaves_de_alerta(itens: list) -> set:
    """PURA. As chaves de alerta presentes nos itens de uma pessoa (a DC pelo tipo)."""
    saida = set()
    for i in itens:
        saida |= _familia(i["folha"])
        if i["folha"] == "dc":
            tipo = str(i.get("tipo") or "").lower()
            if "aliment" in tipo:
                saida.add("dc_alimentacao")
            if "transport" in tipo:
                saida.add("dc_transporte")
    return saida


def etiquetas_da_pessoa(itens: list, tela: str) -> list:
    """PURA. As etiquetas de UMA pessoa para a tela `tela`: sem a folha da própria
    tela, somadas por folha, na ordem das telas, com o alerta onde couber.

    O alerta olha TODAS as folhas da pessoa (inclusive a da tela aberta): na tela de
    diaristas, quem também está na contabilidade vê a etiqueta da contabilidade em
    amarelo."""
    if not itens:
        return []
    presentes = chaves_de_alerta(itens)
    em_alerta: dict = {}
    for juntas, porque in ALERTAS:
        if juntas <= presentes:
            for chave in juntas:
                em_alerta.setdefault(chave, porque)
    proprias = PROPRIAS.get(tela, set())
    por_folha: dict = {}
    for i in itens:
        if i["folha"] in proprias:
            continue
        atual = por_folha.setdefault(i["folha"], {"valor": Decimal("0"), "obs": [], "tipos": set()})
        atual["valor"] += Decimal(str(i.get("valor") or 0))
        if i.get("obs") and i["obs"] not in atual["obs"]:
            atual["obs"].append(i["obs"])
        if i.get("tipo"):
            atual["tipos"].add(str(i["tipo"]))
    saida = []
    for folha in sorted(por_folha, key=lambda f: ORDEM.index(f) if f in ORDEM else 99):
        dado = por_folha[folha]
        chaves = _familia(folha)
        if folha == "dc":
            chaves |= {k for k in ("dc_alimentacao", "dc_transporte")
                       if any(k.split("_")[1][:6] in t.lower() for t in dado["tipos"])}
        porque = next((em_alerta[c] for c in chaves if c in em_alerta), "")
        saida.append({"folha": folha, "rotulo": ROTULOS.get(folha, folha),
                      "valor": dado["valor"], "obs": ", ".join(dado["obs"])[:60],
                      "alerta": bool(porque),
                      "titulo": ("⚠ Conferir: " + porque) if porque else ""})
    return saida


# ---------------------------------------------------------------------------
# O resumo da competência, guardado
# ---------------------------------------------------------------------------
def esquecer(ano: int | None = None, mes: int | None = None) -> None:
    """Joga fora o resumo guardado (de uma competência, ou de todas). Chamado por
    toda rota que salva seleção numa tela da folha."""
    with _trava:
        if ano and mes:
            _guardados.pop((int(ano), int(mes)), None)
        else:
            _guardados.clear()


def etiquetas_da_tela(ano: int, mes: int, tela: str) -> dict:
    """`{"pessoas": {cpf: [etiquetas]}, "avisos": [...]}` — o que a rota entrega."""
    dado = resumo(ano, mes)
    saida = {}
    for cpf, itens in dado["pessoas"].items():
        etiquetas = etiquetas_da_pessoa(itens, tela)
        if etiquetas:
            saida[cpf] = etiquetas
    return {"pessoas": saida, "avisos": dado["avisos"]}


def resumo(ano: int, mes: int, recarregar: bool = False) -> dict:
    """`{"pessoas": {cpf: [itens]}, "avisos": [...], "montado_em": t}` da competência."""
    chave = (int(ano), int(mes))
    agora = time.monotonic()
    with _trava:
        guardado = _guardados.get(chave)
        if guardado and not recarregar and agora - guardado["em"] < VALIDADE_S:
            return guardado["dado"]
    # Uma montagem por competência de cada vez: duas telas abertas juntas não
    # montam o mesmo resumo duas vezes — a segunda espera e aproveita.
    with _trava:
        trava_da_chave = _montando.setdefault(chave, threading.Lock())
    with trava_da_chave:
        with _trava:
            guardado = _guardados.get(chave)
            if guardado and not recarregar and guardado["em"] >= agora \
                    and time.monotonic() - guardado["em"] < VALIDADE_S:
                return guardado["dado"]
        dado = _montar(int(ano), int(mes))
        with _trava:
            if len(_guardados) > 12:
                _guardados.clear()
            _guardados[chave] = {"em": time.monotonic(), "dado": dado}
    return dado


def _montar(ano: int, mes: int) -> dict:
    pessoas: dict = {}
    avisos: list = []
    for nome, fonte in FONTES:
        try:
            for cpf, item in fonte(ano, mes):
                cpf = "".join(c for c in str(cpf or "") if c.isdigit())
                if len(cpf) == 11 and Decimal(str(item.get("valor") or 0)) != 0:
                    pessoas.setdefault(cpf, []).append(item)
        except Exception as e:  # noqa: BLE001 — uma fonte não derruba as outras
            logger.exception("Outras folhas: não consegui ler %s", nome)
            avisos.append(f"{nome}: não foi possível ler ({e})")
    return {"pessoas": pessoas, "avisos": avisos, "montado_em": time.time()}


# ---------------------------------------------------------------------------
# AS FONTES — uma por tela, cada uma pela MESMA função que monta a tela dela.
# Cada uma devolve pares (cpf, item). "Selecionado" é a regra de cada tela.
# ---------------------------------------------------------------------------
METADES = (("quinzena", "1ª quinz."), ("fim_de_mes", "fim do mês"))


def _da_contabilidade(ano: int, mes: int):
    """A folha da contabilidade da quinzena e a do fim do mês: o líquido de cada
    pessoa, menos quem foi tirado do pagamento ("fora", na folha aberta)."""
    from . import colaboradores, folha_apropriacao_guardada as guardada, folha_arquivo as fa
    folhas = {f["tipo"]: f for f in fa.listar(teto=60)
              if int(f["ano"]) == ano and int(f["mes"]) == mes}
    de_para = None
    for tipo, chave in (("quinzena", "contab_quinzena"), ("fim_de_mes", "contab_mes")):
        folha = folhas.get(tipo)
        if not folha:
            continue
        aberta = fa.abrir(folha["id"]) or {}
        fora = {c for c, a in guardada.ajustes_do_pagamento(ano, mes, tipo).items() if a.get("fora")}
        for linha in aberta.get("linhas") or []:
            cpf = "".join(c for c in str(linha.get("cpf") or "") if c.isdigit())
            if len(cpf) != 11:
                if de_para is None:
                    de_para = colaboradores.de_para_do_fortes()
                codigo = str(linha.get("id_fortes") or "").strip()
                achado = de_para.get(codigo) or de_para.get(codigo.zfill(6)) or {}
                cpf = "".join(c for c in str(achado.get("cpf") or "") if c.isdigit())
            if len(cpf) == 11 and cpf not in fora:
                yield cpf, {"folha": chave, "valor": linha.get("valor") or 0}


def _dos_auxilios(ano: int, mes: int):
    from . import folha_auxilio as fx
    for tipo in (fx.ALIMENTACAO, fx.TRANSPORTE):
        for p in fx.calcular(tipo, ano, mes).get("pessoas") or []:
            if p.get("pagar") and (p.get("valor") or 0) > 0:
                yield p["cpf"], {"folha": "alimentacao" if tipo == fx.ALIMENTACAO else "transporte",
                                 "valor": p["valor"]}


def _dos_diaristas(ano: int, mes: int):
    from . import folha_diaristas as fd
    for periodo, rotulo in METADES:
        for p in fd.calcular(ano, mes, periodo).get("pessoas") or []:
            if p.get("pagar") and (p.get("valor") or 0) > 0:
                yield p["cpf"], {"folha": "diarista", "valor": p["valor"], "obs": rotulo}


def _da_dc(ano: int, mes: int):
    """A DC não tem competência: é o que está pendente e selecionado AGORA."""
    from . import dc
    for p in dc.calcular().get("pessoas") or []:
        if p.get("pagar") and (p.get("valor") or 0) > 0 and not p.get("gerada"):
            yield p["cpf"], {"folha": "dc", "valor": p["valor"],
                             "tipo": p.get("tipo_despesa") or "",
                             "obs": _curto(p.get("tipo_despesa"))}


def _do_avulso(ano: int, mes: int):
    """O avulso também não tem competência: a lista colada, selecionada, não gerada."""
    from . import pagamento_avulso as pa
    for p in pa.calcular_pessoas_a_pagar(pa.calcular()):
        yield p["cpf"], {"folha": "avulso", "valor": p["valor"]}


def _dos_gratificados(ano: int, mes: int):
    from . import gratificados as gm
    for via, folha in ((gm.GM, "gratificados"), (gm.SP, "viasp")):
        for periodo, rotulo in METADES:
            for p in gm.pessoas_a_pagar(gm.calcular(ano, mes, periodo, via)):
                yield p["cpf"], {"folha": folha, "valor": p["valor"], "obs": rotulo}


def _curto(tipo) -> str:
    """"Despesas com Alimentação" → "alimentação": a etiqueta é pequena."""
    texto = " ".join(str(tipo or "").split())
    for prefixo in ("Despesas com ", "Despesa com "):
        if texto.startswith(prefixo):
            texto = texto[len(prefixo):]
    return texto[:1].lower() + texto[1:] if texto else ""


FONTES: list = [
    ("Folha da contabilidade", _da_contabilidade),
    ("Alimentação e transporte", _dos_auxilios),
    ("Diaristas", _dos_diaristas),
    ("Despesas com colaboradores", _da_dc),
    ("Pagamento avulso", _do_avulso),
    ("Gratificados e via SP", _dos_gratificados),
]
