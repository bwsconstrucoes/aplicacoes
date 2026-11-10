# -*- coding: utf-8 -*-
"""
A FOLHA COMPLEMENTAR — pagar só a DIFERENÇA quando a folha de um mês já pago é
importada de novo (10/11/2026).

O dono: *"encerrada a folha, feitos os pagamentos, depois foram detectados (…)
ajustes de ponto (…) e precisou fazer correções. Eu preciso importar a nova folha
para, a partir dela, gerar um arquivo de pagamento com a diferença (…) mostrando
quem foi a mais, quem foi a menos — o que foi a menos vai ser só ilustrativo (…)
quem não foi pago, exibir desmarcado (…) ter a visão do que foi pago na anterior e
do que vai ser pago na nova."* Viu o exemplo em tela e aprovou: *"pode construir"*,
com as sugestões aceitas: o pago a mais vai para a lista "a descontar"; a obra da
diferença segue a folha nova; o arquivo sai pelo caminho normal.

O QUE FOI PAGO É O QUE FOI GERADO: as verbas da competência que têm geração no
log (`folha_pagamento.log`) — a `folha` e as complementares já feitas
(`complementar1`, `complementar2`…) — somadas por CPF a partir do FECHAMENTO de
cada uma (`apropriacao_linha`), que é de onde o arquivo saiu. Uma segunda
correção compara com a soma de tudo; nada se paga duas vezes.

A FOLHA NOVA é a folha importada agora, apropriada com a MESMA conta da tela
(`folha_gestao.apropriar_a_folha`) — mas sem o "fora": quem foi tirado do 1º
pagamento precisa de obra para a diferença, se for pago agora. O motivo de ter
ficado fora vem junto, para a tela dizer.

OS GRUPOS (por pessoa):
  · `a_receber`   — já recebeu e a folha nova é MAIOR: vem MARCADA; paga a diferença.
  · `nao_pago`    — não recebeu nada e está na folha nova: vem DESMARCADA.
  · `pago_a_mais` — a folha nova é MENOR (ou a pessoa saiu dela): só informa, e
                    vai para a lista "a descontar na próxima folha".
  · `igual`       — nada a fazer.

GERAR é o mesmo caminho das outras folhas (`folha_pagamento.gerar_direto`, origem
"complementar"): grava um FECHAMENTO com verba própria (`complementarN`) — o que
entra na conta da próxima comparação — e gera o arquivo dele.
"""
from __future__ import annotations

import json
import logging
from decimal import ROUND_HALF_UP, Decimal

logger = logging.getLogger("analisesps.folha_complementar")

CENTAVO = Decimal("0.01")
PREFIXO = "complementar"
A_RECEBER, NAO_PAGO, PAGO_A_MAIS, IGUAL = "a_receber", "nao_pago", "pago_a_mais", "igual"
ORDEM_DOS_GRUPOS = (A_RECEBER, NAO_PAGO, PAGO_A_MAIS, IGUAL)
ROTULO_DO_GRUPO = {
    A_RECEBER: "A receber",
    NAO_PAGO: "Não foi pago antes",
    PAGO_A_MAIS: "Pago a mais",
    IGUAL: "Sem mudança",
}
CHAVE_DA_SELECAO = "folha.complementar.{ano}-{mes:02d}-{tipo}"
CHAVE_A_DESCONTAR = "folha.a_descontar"


class ErroDaComplementar(RuntimeError):
    """Não dá para montar ou gerar a complementar. A frase vai para a tela."""


def _d(valor) -> Decimal:
    return Decimal(str(valor or 0)).quantize(CENTAVO)


def e_complementar(verba) -> bool:
    return str(verba or "").lower().startswith(PREFIXO)


def numero(verba) -> int:
    try:
        return int(str(verba)[len(PREFIXO):] or 0)
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# O que já foi pago
# ---------------------------------------------------------------------------
def pagamentos_feitos(ano: int, mes: int, tipo: str) -> list:
    """As gerações da competência que contam como pagamento da folha: a `folha` e as
    complementares. `[{verba, criado_em, criado_por, total, pessoas}]`, mais antiga
    primeiro. Uma verba gerada duas vezes conta UMA (o fechamento é um só)."""
    from . import folha_pagamento as fp
    vistas, saida = set(), []
    for g in reversed(fp.log(teto=400, ano=int(ano), mes=int(mes))):
        if g["destino"] != fp.ANALISE or g["tipo"] != tipo:
            continue
        for verba in (g["verbas"] or "").split("+"):
            verba = verba.strip().lower()
            if (verba == "folha" or e_complementar(verba)) and verba not in vistas:
                vistas.add(verba)
                saida.append({"verba": verba, "criado_em": g["criado_em"],
                              "criado_por": g.get("criado_por") or "",
                              "total": g["total"], "pessoas": g.get("pessoas") or 0})
    return saida


def ja_paga(ano: int, mes: int, tipo: str) -> bool:
    """A folha da competência já teve arquivo gerado (e, então, não se refecha)."""
    return any(p["verba"] == "folha" for p in pagamentos_feitos(ano, mes, tipo))


def pago_por_cpf(ano: int, mes: int, tipo: str, pagamentos=None) -> dict:
    """`{cpf: {"valor", "nome"}}` — a soma do que saiu em cada pagamento feito,
    pelo FECHAMENTO de cada verba (de onde o arquivo foi gerado)."""
    from . import folha_apropriacao_guardada as guardada
    from .db import consultar
    saida: dict = {}
    for p in pagamentos if pagamentos is not None else pagamentos_feitos(ano, mes, tipo):
        fechamento = guardada.fechamento(ano, mes, tipo, p["verba"])
        if not fechamento:
            raise ErroDaComplementar(
                f"o pagamento \"{p['verba']}\" de {int(mes):02d}/{ano} foi gerado, mas o "
                "fechamento dele não está guardado — sem ele não dá para saber quanto cada "
                "pessoa recebeu.")
        for cpf, nome, valor in consultar(
                "SELECT cpf, nome, valor FROM analisesps.apropriacao_linha "
                " WHERE apropriacao_id = ?", (fechamento["id"],)):
            dado = saida.setdefault(str(cpf or ""), {"valor": Decimal("0.00"), "nome": nome or ""})
            dado["valor"] += _d(valor)
    return saida


def proxima_verba(pagamentos: list) -> str:
    n = max([numero(p["verba"]) for p in pagamentos if e_complementar(p["verba"])] or [0])
    return f"{PREFIXO}{n + 1}"


# ---------------------------------------------------------------------------
# A comparação (PURA) e a montagem
# ---------------------------------------------------------------------------
def dividir(diferenca: Decimal, por_obra: list) -> list:
    """PURA. A diferença repartida nas obras na proporção da folha nova; o centavo
    que sobra do arredondamento vai para a última obra (a soma bate sempre)."""
    partes = [p for p in por_obra or [] if _d(p.get("valor")) > 0]
    if not partes or diferenca <= 0:
        return []
    total = sum((_d(p["valor"]) for p in partes), Decimal("0.00"))
    saida, usado = [], Decimal("0.00")
    for i, p in enumerate(partes):
        if i == len(partes) - 1:
            valor = diferenca - usado
        else:
            valor = (diferenca * _d(p["valor"]) / total).quantize(CENTAVO, ROUND_HALF_UP)
            usado += valor
        saida.append({"obra": p.get("obra") or "", "dias": int(p.get("dias") or 0),
                      "valor": valor, "origem": p.get("origem") or ""})
    return saida


def comparar(pessoas_novas: list, pagos: dict, ajustes: dict) -> list:
    """PURA. Uma linha por pessoa: pago antes × folha nova × diferença, e o grupo.

    `pessoas_novas`: a apropriação da folha nova (sem o "fora"); `pagos`:
    `pago_por_cpf`; `ajustes`: os do 1º pagamento (para dizer por que ficou fora)."""
    from .folha_rateio import cpf_bonito
    linhas, vistos = [], set()
    for p in pessoas_novas or []:
        cpf = str(p.get("cpf") or "")
        novo = sum((_d(x.get("valor")) for x in p.get("por_obra") or []), Decimal("0.00")) \
            or _d(p.get("valor"))
        antes = _d((pagos.get(cpf) or {}).get("valor")) if cpf else Decimal("0.00")
        vistos.add(cpf)
        dif = novo - antes
        if antes == 0 and novo > 0:
            grupo = NAO_PAGO
        elif dif > 0:
            grupo = A_RECEBER
        elif dif < 0:
            grupo = PAGO_A_MAIS
        else:
            grupo = IGUAL
        aj = (ajustes or {}).get(cpf) or {}
        motivos = []
        if grupo == NAO_PAGO and aj.get("fora"):
            motivos.append("no pagamento anterior estava desmarcado"
                           + (f': "{aj["motivo"]}"' if aj.get("motivo") else "")
                           + (f" ({aj['alterado_por']})" if aj.get("alterado_por") else ""))
        if not cpf:
            motivos.append("sem CPF casado com o cadastro — não dá para pagar")
        linhas.append({
            "cpf": cpf, "cpf_bonito": cpf_bonito(cpf) if cpf else "",
            "id_fortes": p.get("id_fortes") or "",
            "nome": p.get("nome_cadastro") or p.get("nome") or "",
            "antes": antes, "novo": novo, "diferenca": dif, "grupo": grupo,
            "por_obra": p.get("por_obra") or [],
            "obras_da_diferenca": dividir(dif, p.get("por_obra") or []) if dif > 0 else [],
            "motivos": motivos, "impossivel": not cpf,
            "saiu": False})
    for cpf, dado in (pagos or {}).items():
        if cpf and cpf not in vistos and _d(dado.get("valor")) > 0:
            linhas.append({
                "cpf": cpf, "cpf_bonito": cpf_bonito(cpf), "id_fortes": "",
                "nome": dado.get("nome") or "",
                "antes": _d(dado["valor"]), "novo": Decimal("0.00"),
                "diferenca": -_d(dado["valor"]), "grupo": PAGO_A_MAIS,
                "por_obra": [], "obras_da_diferenca": [],
                "motivos": ["saiu da folha nova"], "impossivel": False, "saiu": True})
    return linhas


def _ler_meta(chave: str):
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?", (chave,))
        return json.loads(linha[0]) if linha and linha[0] else None
    except Exception:  # noqa: BLE001 — guardado ilegível não derruba a tela
        logger.exception("Complementar: não consegui ler %s", chave)
        return None


def _gravar_meta(chave: str, valor) -> None:
    from .db import conexao
    from .sincronizacao import _meta_gravar
    with conexao() as conn:
        _meta_gravar(conn, chave, json.dumps(valor, ensure_ascii=False, default=str))
        conn.commit()


def _chave_da_selecao(ano, mes, tipo) -> str:
    return CHAVE_DA_SELECAO.format(ano=int(ano), mes=int(mes), tipo=tipo)


def selecao(ano, mes, tipo) -> dict:
    dado = _ler_meta(_chave_da_selecao(ano, mes, tipo))
    return dado if isinstance(dado, dict) else {}


def salvar_selecao(ano, mes, tipo, decisoes, quem: str = "") -> dict:
    atual = selecao(ano, mes, tipo)
    for d in decisoes or []:
        cpf = "".join(c for c in str((d or {}).get("cpf") or "") if c.isdigit())
        if len(cpf) == 11:
            atual[cpf] = bool(d.get("pagar"))
    _gravar_meta(_chave_da_selecao(ano, mes, tipo), atual)
    return {"salvas": len(decisoes or [])}


def _apropriado_sem_fora(folha) -> tuple:
    """(apropriado, ajustes do pagamento) da folha nova — com a mesma conta da tela,
    mas ninguém "fora": a diferença de quem ficou fora também precisa de obra."""
    from . import colaboradores, folha_apropriacao, folha_rateio
    from . import folha_apropriacao_guardada as guardada
    from . import ponto as mod_ponto
    ano, mes, tipo = folha["ano"], folha["mes"], folha["tipo"]
    regras_por_cpf = {}
    for regra in folha_rateio.listar(so_ativas=True):
        for pessoa in regra.get("pessoas") or []:
            regras_por_cpf[pessoa["cpf"]] = regra
    ajustes = guardada.ajustes_do_pagamento(ano, mes, tipo)
    sem_fora = {c: dict(a, fora=False) for c, a in ajustes.items()}
    apropriado = folha_apropriacao.apropriar(
        folha["linhas"], dias_por_cpf=mod_ponto.dias_por_cpf(ano, mes),
        regras_por_cpf=regras_por_cpf, ajustes_por_cpf=sem_fora,
        cadastro_por_id=colaboradores.de_para_do_fortes(),
        periodo=folha_apropriacao.periodo_do_pagamento(ano, mes, tipo))
    return apropriado, ajustes


def montar(folha_id: int) -> dict:
    """Tudo o que a tela, o relatório e o arquivo precisam."""
    from . import folha_arquivo
    folha = folha_arquivo.abrir(folha_id)
    if folha is None:
        raise ErroDaComplementar("folha não encontrada.")
    ano, mes, tipo = int(folha["ano"]), int(folha["mes"]), folha["tipo"]
    pagamentos = pagamentos_feitos(ano, mes, tipo)
    base = {"folha": folha, "ano": ano, "mes": mes, "tipo": tipo,
            "pagamentos": pagamentos, "ja_paga": any(p["verba"] == "folha" for p in pagamentos),
            "verba": proxima_verba(pagamentos), "pessoas": [], "grupos": {},
            "totais": {}, "a_pagar": [], "total_a_pagar": Decimal("0.00")}
    if not base["ja_paga"]:
        return base
    pagos = pago_por_cpf(ano, mes, tipo, pagamentos)
    apropriado, ajustes = _apropriado_sem_fora(folha)
    linhas = comparar(apropriado.get("pessoas") or [], pagos, ajustes)
    escolhas = selecao(ano, mes, tipo)
    for l in linhas:
        padrao = l["grupo"] == A_RECEBER
        pode = l["grupo"] in (A_RECEBER, NAO_PAGO) and not l["impossivel"] \
            and bool(l["obras_da_diferenca"])
        if l["grupo"] in (A_RECEBER, NAO_PAGO) and not l["obras_da_diferenca"] and not l["impossivel"]:
            l["motivos"].append("sem obra apropriada na folha nova — confira o ponto da pessoa")
        l["pode_pagar"] = pode
        l["pagar"] = pode and bool(escolhas.get(l["cpf"], padrao))
    linhas.sort(key=lambda l: (ORDEM_DOS_GRUPOS.index(l["grupo"]), l["nome"].lower()))
    grupos = {g: [l for l in linhas if l["grupo"] == g] for g in ORDEM_DOS_GRUPOS}
    a_pagar = [l for l in linhas if l["pagar"]]
    base.update({
        "pessoas": linhas, "grupos": grupos, "a_pagar": a_pagar,
        "total_a_pagar": sum((l["diferenca"] for l in a_pagar), Decimal("0.00")),
        "totais": {
            "pago_antes": sum((l["antes"] for l in linhas), Decimal("0.00")),
            "folha_nova": sum((l["novo"] for l in linhas), Decimal("0.00")),
            **{g: sum((l["diferenca"] for l in grupos[g]), Decimal("0.00")) for g in ORDEM_DOS_GRUPOS},
            **{f"quantos_{g}": len(grupos[g]) for g in ORDEM_DOS_GRUPOS},
        },
    })
    return base


# ---------------------------------------------------------------------------
# O arquivo: as linhas, o fechamento e a lista "a descontar"
# ---------------------------------------------------------------------------
def linhas_a_pagar(montado: dict) -> list:
    """No formato de `folha_pagamento.linhas_para_pagar`: uma linha por obra da
    diferença de cada pessoa marcada."""
    from . import folha_pagamento as fp
    contas = fp.conta_por_obra()
    saida = []
    for l in montado["a_pagar"]:
        for parte in l["obras_da_diferenca"]:
            obra = str(parte["obra"] or "").strip().upper()
            saida.append({"cpf": l["cpf"], "nome": l["nome"], "obra": obra,
                          "conta": contas.get(obra, ""), "verba": montado["verba"],
                          "dias": parte["dias"], "origem": parte["origem"],
                          "valor": parte["valor"]})
    return saida


def fechar(montado: dict, quem: str = "") -> int:
    """Congela a complementar com verba própria — é o que a próxima comparação soma."""
    from . import folha_apropriacao_guardada as guardada
    if not montado["a_pagar"]:
        raise ErroDaComplementar("nenhuma diferença marcada para pagar.")
    pessoas = [{"cpf": l["cpf"], "nome": l["nome"], "fora": False,
                "por_obra": l["obras_da_diferenca"]} for l in montado["a_pagar"]]
    total = montado["total_a_pagar"]
    return guardada.fechar(montado["ano"], montado["mes"], montado["tipo"],
                           {"pessoas": pessoas, "total_da_folha": total,
                            "total_apropriado": total, "fecha": True},
                           verba=montado["verba"], quem=quem)


def depois_de_gerar(montado: dict, quem: str = "") -> None:
    """Depois do arquivo: o pago a mais vai para "a descontar", e a seleção zera."""
    guardar_a_descontar(montado, quem=quem)
    _gravar_meta(_chave_da_selecao(montado["ano"], montado["mes"], montado["tipo"]), {})


def a_descontar(so_pendentes: bool = True) -> list:
    dado = _ler_meta(CHAVE_A_DESCONTAR)
    lista = dado if isinstance(dado, list) else []
    return [x for x in lista if not (so_pendentes and x.get("descontado_em"))]


def guardar_a_descontar(montado: dict, quem: str = "") -> int:
    """Grava (ou atualiza) o pago a mais desta competência na lista "a descontar
    na próxima folha". Uma entrada por pessoa e competência."""
    from .horario import agora
    rotulo = f"{montado['mes']:02d}/{montado['ano']} " + (
        "quinzena" if montado["tipo"] == "quinzena" else "fim do mês")
    lista = _ler_meta(CHAVE_A_DESCONTAR)
    lista = lista if isinstance(lista, list) else []
    chave = f"{montado['ano']}-{montado['mes']:02d}-{montado['tipo']}"
    por_chave = {(x.get("cpf"), x.get("competencia_chave")): x for x in lista}
    novos = 0
    for l in montado["grupos"].get(PAGO_A_MAIS) or []:
        item = por_chave.get((l["cpf"], chave))
        if item and item.get("descontado_em"):
            continue
        dado = {"cpf": l["cpf"], "cpf_bonito": l.get("cpf_bonito") or l["cpf"],
                "nome": l["nome"], "valor": str(-l["diferenca"]),
                "competencia": rotulo, "competencia_chave": chave,
                "motivo": "; ".join(l["motivos"]) or "folha nova menor que o pago",
                "registrado_em": agora().isoformat(timespec="seconds"),
                "registrado_por": str(quem or "")[:80], "descontado_em": ""}
        if item:
            item.update(dado)
        else:
            lista.append(dado)
            novos += 1
    _gravar_meta(CHAVE_A_DESCONTAR, lista)
    return novos


def marcar_descontado(cpf: str, competencia_chave: str, quem: str = "") -> bool:
    from .horario import agora
    lista = _ler_meta(CHAVE_A_DESCONTAR)
    lista = lista if isinstance(lista, list) else []
    achou = False
    for x in lista:
        if x.get("cpf") == cpf and x.get("competencia_chave") == competencia_chave:
            x["descontado_em"] = agora().isoformat(timespec="seconds")
            x["descontado_por"] = str(quem or "")[:80]
            achou = True
    if achou:
        _gravar_meta(CHAVE_A_DESCONTAR, lista)
    return achou


# ---------------------------------------------------------------------------
# O relatório (o compartilhado das folhas, com o detalhamento próprio)
# ---------------------------------------------------------------------------
DETALHE_DO_RELATORIO = {
    "titulo": "Pessoa por pessoa", "aba": "Complementar",
    "excel": {
        "cabecalho": ["Situação", "Colaborador", "CPF", "Código Fortes", "Obra(s) da diferença",
                      "Pago antes", "Folha nova", "Diferença", "Paga agora", "Observação"],
        "moeda": (5, 6, 7, 8),
        "larguras": [18, 38, 15, 12, 40, 14, 14, 14, 14, 50],
    },
    "pdf": {
        "cabecalho": ["Situação", "Colaborador", "Obra(s) da diferença", "Pago antes",
                      "Folha nova", "Diferença", "Paga agora", "Observação"],
        "larguras": [24, 58, 52, 22, 22, 22, 22, 55],
        "direita": (3, 4, 5, 6),
        "linhas_max": 3,
    },
}


def _celulas(l: dict) -> tuple:
    from . import formatos
    from .folha_rateio import cpf_bonito
    obras = ", ".join(f"{p['obra']} {formatos.moeda(p['valor'])}" for p in l["obras_da_diferenca"])
    paga = l["diferenca"] if l.get("pagar") else None
    rotulo = ROTULO_DO_GRUPO[l["grupo"]]
    obs = "; ".join(l["motivos"])
    excel = [rotulo, l["nome"], cpf_bonito(l["cpf"]) if l["cpf"] else "", l["id_fortes"], obras,
             float(l["antes"]), float(l["novo"]), float(l["diferenca"]),
             float(paga) if paga is not None else None, obs]
    pdf = [rotulo, (l["nome"], cpf_bonito(l["cpf"]) if l["cpf"] else ""), obras or "-",
           formatos.moeda(l["antes"]), formatos.moeda(l["novo"]),
           ("+" if l["diferenca"] > 0 else "") + formatos.moeda(l["diferenca"]),
           formatos.moeda(paga) if paga is not None else "-", obs or "-"]
    return excel, pdf


def montado_do_relatorio(montado: dict, so_a_pagar: bool = False) -> dict:
    """O montado de `folha_relatorio`: quem recebe agora entra nos totais e nos
    agrupamentos; quem não recebe aparece no detalhamento (pago a mais, não pago)."""
    pessoas = []
    for l in montado["pessoas"]:
        if l["grupo"] == IGUAL or (so_a_pagar and not l.get("pagar")):
            continue
        excel, pdf = _celulas(l)
        entra = bool(l.get("pagar"))
        pessoas.append({
            "entra": entra, "cpf": l["cpf"], "nome_na_tela": l["nome"],
            "por_obra": l["obras_da_diferenca"] if entra else [],
            "valor": l["diferenca"] if entra else Decimal("0.00"),
            "obras_resumo": ", ".join(p["obra"] for p in l["obras_da_diferenca"]) or "-",
            "contas": [], "dias_no_ponto": 0, "por_dia": [],
            "detalhe_excel": excel, "detalhe_pdf": pdf})
    competencia = f"{montado['mes']:02d}/{montado['ano']}"
    rotulo = f"Folha complementar {numero(montado['verba'])} · " + (
        "quinzena" if montado["tipo"] == "quinzena" else "fim do mês")
    return {"titulo": f"Folha complementar {competencia}",
            "prefixo_arquivo": "Folha complementar",
            "folha": {"competencia": competencia, "rotulo_do_tipo": rotulo},
            "pessoas": pessoas, "filtros_texto": [], "filtros": {},
            "totais": {"pessoas": len(pessoas)}, "fechamento": None,
            "paisagem": True, "detalhe": DETALHE_DO_RELATORIO, "detalhe_todos": True}
