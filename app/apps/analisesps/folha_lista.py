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
    ("nao_vai", "sem pagamento"),
    ("falta_dado", "cadastro incompleto"),
    ("saindo", "em desligamento"),
    ("afastado", "afastado"),
    ("vigia", "vigia (sem direito a diária)"),
    ("saiu", "desligado"),
]
ESCONDIDAS_SEM_FILTRO = {"saiu"}


def situacoes_da_pessoa(p: dict) -> set:
    """Em quais situações a pessoa entra. Pode ser mais de uma."""
    saida = set()
    situacao = p.get("situacao") or ""
    if situacao == colaboradores.SITUACAO_SAIU or p.get("desligado"):
        saida.add("saiu")
    if situacao == colaboradores.SITUACAO_SAINDO:
        saida.add("saindo")
    if situacao == colaboradores.SITUACAO_AFASTADO:
        saida.add("afastado")
    if p.get("vigia"):
        saida.add("vigia")
    if p.get("impossivel"):
        saida.add("falta_dado")
    elif p.get("pagar"):
        saida.add("vai")
    else:
        saida.add("nao_vai")
    return saida


def marcados(args, chave: str) -> list:
    """Os valores marcados num bloco do filtro (`?obra=A&obra=B`)."""
    return [" ".join(v.split()) for v in args.getlist(chave) if v and v.strip()]


def filtrar(pessoas: list, args, campo_da_obra: str = "obras") -> dict:
    """A lista filtrada e as contagens para a lateral.

    `campo_da_obra`: a chave da pessoa com as obras dela — uma lista
    (diaristas, que podem ter dias em várias) ou um texto (auxílio)."""
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
    fase = marcados(args, "fase")

    contagem: dict = {}
    for p in pessoas:
        for s in situacoes_da_pessoa(p):
            contagem[s] = contagem.get(s, 0) + 1

    def obras_de(p) -> list:
        valor = p.get(campo_da_obra)
        if isinstance(valor, (list, tuple)):
            return [o for o in valor if o]
        return [valor] if valor else []

    lista = []
    digitos = so_digitos(busca)
    alvo = busca.lower()
    for p in pessoas:
        dele = situacoes_da_pessoa(p)
        if situacao:
            if not dele & set(situacao):
                continue
        elif dele & ESCONDIDAS_SEM_FILTRO:
            continue
        if obra and not set(obras_de(p)) & set(obra):
            continue
        if fase and (p.get("fase") or "") not in fase:
            continue
        if busca and not (alvo in (p.get("nome") or "").lower()
                          or (digitos and digitos in (p.get("cpf") or ""))):
            continue
        lista.append(p)

    todas_as_obras = sorted({o for p in pessoas for o in obras_de(p)})
    return {
        "pessoas": lista,
        "filtros": {"q": busca, "situacao": situacao, "obra": obra, "fase": fase},
        "filtrando": bool(busca or situacao or obra or fase),
        "contagem": contagem,
        "escondidos": sum(1 for p in pessoas
                          if not situacao and situacoes_da_pessoa(p)
                          & ESCONDIDAS_SEM_FILTRO),
        "opcoes_situacao": [(k, f"{r} ({contagem.get(k, 0)})")
                            for k, r in SITUACOES if contagem.get(k)],
        "opcoes_obra": [(o, o) for o in todas_as_obras],
        "opcoes_fase": [(f, f) for f in sorted({p.get("fase") for p in pessoas
                                                 if p.get("fase")})],
    }
