# ============================================================================
# ERP — o bloco da tela tem de existir na página base
#
# POR QUE ESTE ARQUIVO EXISTE
#
# A tela de Compras nasceu com `{% block script %}` em vez de `{% block
# scripts %}` — uma letra. A base não conhece `script`, então o conteúdo foi
# jogado fora ao montar a página: nenhum erro, nenhum aviso, e a tela abriu
# bonita, com a tabela vazia para sempre. É o mesmo defeito de sempre, de outra
# forma: silêncio.
#
# Aqui a suíte lê cada tela, vê de qual página ela herda, e cobra que todo
# bloco declarado exista lá. Um nome errado passa a quebrar a suíte em vez de
# quebrar a tela em produção.
# ============================================================================
from __future__ import annotations

import re
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
TEMPLATES = RAIZ / "app/apps/erp/templates"

EXTENDS = re.compile(r"""{%-?\s*extends\s+["']([^"']+)["']""")
BLOCO = re.compile(r"""{%-?\s*block\s+([A-Za-z_][A-Za-z0-9_]*)""")


def _telas() -> list[Path]:
    return sorted(p for p in TEMPLATES.glob("*.html")
                  if EXTENDS.search(p.read_text(encoding="utf-8")))


@pytest.mark.parametrize("tela", _telas(), ids=lambda p: p.name)
def test_todo_bloco_da_tela_existe_na_pagina_base(tela: Path) -> None:
    """Bloco que a base não declara é conteúdo jogado fora, sem aviso.

    Falhou? Confira a grafia contra os blocos da base — quase sempre é
    singular/plural (`script` no lugar de `scripts`) ou uma letra trocada.
    """
    texto = tela.read_text(encoding="utf-8")
    pai_nome = EXTENDS.search(texto).group(1)
    pai = TEMPLATES / Path(pai_nome).name
    assert pai.exists(), f"{tela.name} herda de {pai_nome}, que não existe"

    # Os blocos da base, e os que ela mesma herda, se herdar de alguém.
    disponiveis: set[str] = set()
    atual, visitados = pai, set()
    while atual and atual.exists() and atual not in visitados:
        visitados.add(atual)
        conteudo = atual.read_text(encoding="utf-8")
        disponiveis |= set(BLOCO.findall(conteudo))
        acima = EXTENDS.search(conteudo)
        atual = TEMPLATES / Path(acima.group(1)).name if acima else None

    declarados = set(BLOCO.findall(texto))
    # blocos que a própria tela cria para uso interno dela são legítimos:
    # só se cobra o que ela declara no PRIMEIRO nível (o que a base recebe).
    orfaos = sorted(declarados - disponiveis)
    assert not orfaos, (
        f"{tela.name} declara bloco(s) que {pai.name} não tem: "
        f"{', '.join(orfaos)}. O conteúdo deles é jogado fora sem aviso. "
        f"A base oferece: {', '.join(sorted(disponiveis))}")
