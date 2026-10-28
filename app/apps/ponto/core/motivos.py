# -*- coding: utf-8 -*-
"""
OS MOTIVOS, EM CATEGORIAS, PARA FILTRAR AS VALIDAÇÕES (28/10/2026).

O dono: *"quando eu boto batida em conferência, a gente tem subinformações (…) o rosto que não é
localizado, que é de internet, que é porque está fora da área, que é porque obra sem coordenada, ou
então ficou na borda da obra, ou sem foto no aparelho (…) Eu queria ver uma forma de também fazer essa
filtragem (…) e nos ajustes (…) dentro desses outros já tem essas outras derivações."*

A batida em conferência guarda o motivo como TEXTO (`marcacoes.motivo_analise`, vários separados por
"; "), escrito em lugares diferentes (a cerca, a foto, a conferência do rosto, o mosaico, a batida sem
internet). Aqui cada pedaço vira uma categoria, pelo começo do texto — o que não casar com nenhuma vira
"Outro motivo", nunca some. O ajuste de batida já vem com os motivos de `credibilidade.py`, que agora
trazem o código de cada um.
"""
from __future__ import annotations

# (código, rótulo, começos do texto — sem acento e em minúsculas)
CONFERENCIA: list[tuple[str, str, tuple[str, ...]]] = [
    ("ROSTO_OUTRA", "Rosto não confere com o cadastro", ("rosto nao confere",)),
    ("SEM_ROSTO", "Rosto não encontrado na foto", ("nenhum rosto na foto", "foto sem rosto visivel")),
    ("FOTO_ESCURA", "Foto escura", ("foto escura",)),
    ("MOSAICO", "Foto suspeita no mosaico", ("foto suspeita no mosaico",)),
    ("SEM_FOTO", "Sem foto no aparelho da obra", ("sem foto no aparelho",)),
    ("SEM_INTERNET", "Batida sem internet", ("sem internet",)),
    ("FORA_DA_AREA", "Fora da área da obra", ("fora da area", "fora da cerca")),
    ("BORDA", "Na borda da cerca", ("na borda da cerca",)),
    ("SEM_COORDENADA", "Obra sem coordenada", ("obra sem coordenada",)),
    ("SEM_LOCALIZACAO", "Batida sem localização", ("batida sem localizacao",)),
    ("RELOGIO", "Relógio do aparelho diferente", ("relogio do aparelho",)),
    ("FILA_RAPIDA", "Fila rápida no aparelho", ("fila rapida",)),
    ("CADASTRO", "Situação no cadastro (afastado, fora do registro)", ("pessoa ",)),
    ("EXPLICOU", "A pessoa explicou o motivo", ("justificativa da pessoa",)),
]
OUTRO = ("OUTRO", "Outro motivo")

# Os motivos do AJUSTE (credibilidade.py): os de atenção, e os dois fortes a favor.
AJUSTE: list[tuple[str, str]] = [
    ("AJ_DIA_SEM_BATIDA", "Dia sem nenhuma batida"),
    ("AJ_FOTO", "Foto do dia com problema"),
    ("AJ_EM_CONFERENCIA", "Batida do dia em conferência"),
    ("AJ_FORA_DA_AREA", "Tentou bater fora da área"),
    ("AJ_OBRA_DIFERENTE", "Obra diferente da das batidas"),
    ("AJ_LONGE_DA_ESCALA", "Longe do horário da escala"),
    ("AJ_DESCANSO", "Dia de descanso ou feriado"),
    ("AJ_TARDIO", "Pedido muitos dias depois"),
    ("AJ_MUITOS", "Muitos ajustes no mês"),
    ("AJ_NEGADO", "Pedido negado há pouco"),
    ("AJ_POR_OUTRO", "Pedido por outra pessoa (responsável, administrativo)"),
    ("AJ_SO_UMA", "Só uma batida faltando no dia"),
]

GRUPO_CONFERENCIA = "Batida em conferência"
GRUPO_AJUSTE = "Ajuste de batida"


def _sem_acento(texto: str) -> str:
    import unicodedata
    return unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode().lower().strip()


def da_conferencia(texto: str | None) -> list[str]:
    """PURA. Os códigos dos motivos de uma batida em conferência, sem repetir, na ordem do texto."""
    codigos: list[str] = []
    for pedaco in (texto or "").split(";"):
        p = _sem_acento(pedaco)
        if not p:
            continue
        codigo = next((c for c, _, comecos in CONFERENCIA if p.startswith(comecos)), None)
        if codigo is None:
            # o próprio motivo pode ter ";" dentro ("raio 150 m; GPS com precisão de 30 m"): o pedaço
            # que não casa com nada é a continuação do anterior — só vira "Outro" se for o primeiro
            if codigos:
                continue
            codigo = OUTRO[0]
        if codigo not in codigos:
            codigos.append(codigo)
    return codigos or [OUTRO[0]]


def rotulos() -> dict[str, dict]:
    """Para a tela: {código: {rotulo, grupo, ordem}}. A `ordem` é a do filtro (o JSON da resposta sai
    com as chaves em ordem alfabética, e o filtro misturaria os grupos)."""
    lista = ([(c, r, GRUPO_CONFERENCIA) for c, r, _ in CONFERENCIA] + [(OUTRO[0], OUTRO[1], GRUPO_CONFERENCIA)]
             + [(c, r, GRUPO_AJUSTE) for c, r in AJUSTE])
    return {c: {"rotulo": r, "grupo": g, "ordem": n} for n, (c, r, g) in enumerate(lista)}
