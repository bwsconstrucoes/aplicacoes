# -*- coding: utf-8 -*-
"""
A LISTA DAS OUTRAS FOLHAS: situação, filtros e quem fica escondido.

O dono, 01/10/2026: as outras folhas (diaristas, alimentação e transporte)
devem herdar o que a da contabilidade ganhou — e a primeira coisa que ele viu
errada foi esta:

    "está aparecendo colaboradores desligados. Esse colaborador desligado, já é,
     o nome está dizendo, ele já saiu. Ou seja, é uma fase que não se utiliza. E
     esse critério está na planilha."

⚠️ QUEM JÁ SAIU NÃO APARECE NA LISTA — mas não some. Sem filtro de situação
marcado, a lista esconde os desligados (é o que a planilha faz, tirando da base
quem está em "Colaboradores Desligados"); a contagem deles fica na lateral, e o
filtro "já saiu" mostra quem é. Esconder sem dizer faria um diarista que saiu no
dia 20 — e trabalhou do 1 ao 19 — deixar de receber sem ninguém ver.

Os filtros são de caixinha, no padrão das Solicitações e da folha da
contabilidade: vários valores por bloco, e marcar já aplica.
"""
from __future__ import annotations

from . import colaboradores

SITUACOES = [
    ("vai", "a pagar"),
    ("nao_vai", "fora do pagamento"),
    ("falta_dado", "cadastro incompleto"),
    ("sem_diaria", "sem diária no período"),
    ("saindo", "em desligamento"),
    ("afastado", "afastado"),
    ("vigia", "vigia"),
    ("saiu", "desligado"),
    # Transporte: ausências no ponto com desconto ainda não aplicado (03/10/2026).
    ("ausencia", "ausência a descontar"),
    # Auxílio a receber sem obra do ponto: escolher a obra (03/10/2026).
    ("sem_obra", "sem obra do ponto"),
]
ESCONDIDAS_SEM_FILTRO = {"saiu"}

# ⚠️ NOS DIARISTAS A LISTA ABRE SÓ COM QUEM TEM VALOR A PAGAR (dono, 02/10/2026):
# *"na tela, a princípio aparecer somente quem tem a pagar"*. É o que a aba
# Diaristas da planilha faz — a QUERY pula vigia (`W <> 'VIGIA'`), quem está sem
# valor (`AP <> 'CORRIGIR VALOR DIÁRIA'`) e quem não tem diária (`AJ > 0`). Nada
# disso some: cada grupo é uma opção do filtro Situação, com a contagem, e o
# cadastro incompleto e os desligados ficam ditos em Pendências.
# Desligado NÃO fica de fora nos diaristas (02/10/2026): quem saiu do contrato e
# continuou trabalhando tem direito à diária — entra na lista, com o alerta.
ESCONDIDAS_NOS_DIARISTAS = {"vigia", "sem_diaria", "falta_dado"}

# ⚠️ NA ALIMENTAÇÃO E NO TRANSPORTE, O MESMO (dono, 03/10/2026): *"deve ser
# pessoas que não têm dado de alimentação e transporte (…) Se não tem, não
# precisa ser exibido, o que precisaria era um filtro que exiba eles."* O
# "cadastro incompleto" do auxílio é quem não tem valor ou modalidade DESTE
# auxílio na ficha — na maioria, quem simplesmente não recebe. Fica fora da
# lista, contado na lateral e no filtro Situação, como os desligados.
ESCONDIDAS_NOS_AUXILIOS = {"saiu", "falta_dado"}


def situacoes_da_pessoa(p: dict) -> set:
    """Em quais situações a pessoa entra. Pode ser mais de uma.

    "Fora do pagamento" é quem tem valor e foi desmarcado; desligado, vigia,
    cadastro incompleto e sem diária têm situação própria e não contam ali."""
    saida = set()
    situacao = p.get("situacao") or ""
    saiu = situacao == colaboradores.SITUACAO_SAIU or bool(p.get("desligado"))
    if saiu:
        saida.add("saiu")
    if situacao == colaboradores.SITUACAO_SAINDO:
        saida.add("saindo")
    if situacao == colaboradores.SITUACAO_AFASTADO:
        saida.add("afastado")
    if p.get("vigia"):
        saida.add("vigia")
    if p.get("ausencias") and not p.get("desconto_aplicado"):
        saida.add("ausencia")
    if p.get("sem_obra"):
        saida.add("sem_obra")
    if p.get("sem_diaria"):
        saida.add("sem_diaria")
    elif p.get("impossivel"):
        saida.add("falta_dado")
    elif p.get("pagar"):
        saida.add("vai")
    # Desmarcado à mão = "fora do pagamento". O desligado que a POLÍTICA tira
    # (auxílio) não é desmarcado à mão; o diarista desligado, que entra pela
    # regra (`pagar_calculado`), é — e quando desmarcado, conta aqui.
    elif not (p.get("vigia") or (saiu and not p.get("pagar_calculado"))):
        saida.add("nao_vai")
    return saida


def marcados(args, chave: str) -> list:
    """Os valores marcados num bloco do filtro (`?obra=A&obra=B`)."""
    return [" ".join(v.split()) for v in args.getlist(chave) if v and v.strip()]


def filtrar(pessoas: list, args, campo_da_obra: str = "obras",
            escondidas=None) -> dict:
    """A lista filtrada e as contagens para a lateral.

    `campo_da_obra`: a chave da pessoa com as obras dela — uma lista
    (diaristas, que podem ter dias em várias) ou um texto (auxílio).
    `escondidas`: as situações que ficam fora da lista sem filtro marcado."""
    from .folha_rateio import so_digitos

    busca = " ".join((args.get("q") or "").split())
    situacao = marcados(args, "situacao")
    # Os links antigos do auxílio (`?so=pagar`, `?so=problema`) continuam valendo.
    antigo = (args.get("so") or "").strip()
    if not situacao and antigo == "pagar":
        situacao = ["vai"]
    elif not situacao and antigo == "problema":
        situacao = ["nao_vai", "falta_dado"]
    obra = marcados(args, "obra")
    obra_cadastro = marcados(args, "obra_cadastro")
    fase = marcados(args, "fase")
    escondidas = set(ESCONDIDAS_SEM_FILTRO if escondidas is None else escondidas)

    contagem: dict = {}
    for p in pessoas:
        for s in situacoes_da_pessoa(p):
            contagem[s] = contagem.get(s, 0) + 1

    def obras_de(p) -> list:
        valor = p.get(campo_da_obra)
        if isinstance(valor, (list, tuple)):
            return [o for o in valor if o]
        return [valor] if valor else []

    # ⚠️ QUEM VAI SER PAGO NUNCA FICA ESCONDIDO (06/10/2026). A lista escondia,
    # por padrão, desligados e cadastro incompleto — inclusive quem estava
    # MARCADO para receber (o desligado marcado à mão, por exemplo). Esses saíam
    # no arquivo sem aparecer na lista: o dono viu "inclui 77 marcados fora do
    # filtro" sem ter filtrado nada. Escondido, só quem não vai receber.
    def escondido(p, dele) -> set:
        return set() if _vai(p) else (dele & escondidas)

    lista = []
    digitos = so_digitos(busca)
    alvo = busca.lower()
    for p in pessoas:
        dele = situacoes_da_pessoa(p)
        if situacao:
            if not dele & set(situacao):
                continue
            # ⚠️ O ESCONDIDO CONTINUA ESCONDIDO quando se filtra por OUTRA
            # situação (dono, 05/10/2026: filtrando "sem obra" para tratar quem
            # vai receber, apareciam os desligados). Ele aparece só quando a
            # própria situação dele é marcada.
            if escondido(p, dele) - set(situacao):
                continue
        elif escondido(p, dele):
            continue
        if obra and not set(obras_de(p)) & set(obra):
            continue
        if obra_cadastro and (p.get("obra_cadastro") or "") not in obra_cadastro:
            continue
        if fase and (p.get("fase") or "") not in fase:
            continue
        if busca and not (alvo in (p.get("nome") or "").lower()
                          or (digitos and digitos in (p.get("cpf") or ""))):
            continue
        lista.append(p)

    todas_as_obras = sorted({o for p in pessoas for o in obras_de(p)})
    escondidos_por: dict = {}
    if not situacao:
        for p in pessoas:
            for s_ in escondido(p, situacoes_da_pessoa(p)):
                escondidos_por[s_] = escondidos_por.get(s_, 0) + 1
    return {
        "pessoas": lista,
        "filtros": {"q": busca, "situacao": situacao, "obra": obra,
                    "obra_cadastro": obra_cadastro, "fase": fase},
        "filtrando": bool(busca or situacao or obra or obra_cadastro or fase),
        "contagem": contagem,
        "escondidos": sum(1 for p in pessoas
                          if not situacao and escondido(p, situacoes_da_pessoa(p))),
        "escondidos_por": [(k, r, escondidos_por[k]) for k, r in SITUACOES
                           if escondidos_por.get(k)],
        "opcoes_situacao": [(k, f"{r} ({contagem.get(k, 0)})")
                            for k, r in SITUACOES if contagem.get(k)],
        "opcoes_obra": [(o, o) for o in todas_as_obras],
        "opcoes_obra_cadastro": [(o, o) for o in sorted(
            {p.get("obra_cadastro") for p in pessoas if p.get("obra_cadastro")})],
        "opcoes_fase": [(f, f) for f in sorted({p.get("fase") for p in pessoas
                                                 if p.get("fase")})],
    }


# ---------------------------------------------------------------------------
# A LISTA AGRUPADA — a mesma nas folhas que a têm (DC, alimentação e
# transporte). O dono, 03/10/2026, na DC: *"visualizar de forma mais agrupada o
# que está para ser pago. Agrupar por obra etc."*; e 05/10/2026: *"tanto em
# alimentação como em transporte, possa ser realizado o agrupamento e
# desagrupamento das informações, por conta, por obra, etc."*
# ---------------------------------------------------------------------------
VAZIO_DO_GRUPO = {"obra": "(sem obra)", "conta": "(sem conta)",
                  "modo": "(sem categoria)", "fase": "(sem fase)",
                  "tipo_despesa": "(sem tipo)"}


def _vai(p: dict) -> bool:
    from decimal import Decimal
    return bool(p.get("pagar") and Decimal(str(p.get("valor") or 0)) > 0
                and not p.get("gerada"))


def rotulo_do_grupo(p: dict, campo: str) -> str:
    return str(p.get(campo) or "").strip() or VAZIO_DO_GRUPO.get(campo, "(vazio)")


def agrupar(pessoas, campo: str, rotulo=None, pendente=None) -> list:
    """A lista em grupos: `[{rotulo, pessoas, linhas, a_pagar, total, pendencias}]`.
    Sem campo, um grupo só (sem cabeçalho). Dentro do grupo, a ordem da lista
    (pendências primeiro); os grupos, do maior valor a pagar para o menor."""
    from decimal import Decimal
    if not campo:
        return [{"rotulo": "", "pessoas": list(pessoas)}]
    rotulo = rotulo or rotulo_do_grupo
    pendente = pendente or (lambda p: bool(p.get("impossivel")))
    grupos: dict = {}
    for p in pessoas:
        grupos.setdefault(rotulo(p, campo), []).append(p)
    saida = []
    for nome, membros in grupos.items():
        vao = [p for p in membros if _vai(p)]
        saida.append({"rotulo": nome, "pessoas": membros, "linhas": len(membros),
                      "a_pagar": len(vao),
                      "total": sum((Decimal(str(p["valor"])) for p in vao), Decimal("0.00")),
                      "pendencias": sum(1 for p in membros if pendente(p))})
    return sorted(saida, key=lambda g: (-g["total"], g["rotulo"]))

