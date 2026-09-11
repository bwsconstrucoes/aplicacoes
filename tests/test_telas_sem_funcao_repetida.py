"""Nenhuma tela pode declarar a MESMA função duas vezes.

NASCEU DE UM DEFEITO REAL, achado em 09/09/2026 na tela de Pagamentos.

O que aconteceu: em 01/09 uma melhoria dos lotes foi ACRESCENTADA no topo do
bloco de JavaScript, e a versão antiga das mesmas funções ficou embaixo, sem
ser removida. Em JavaScript, quando duas funções com o mesmo nome são
declaradas no mesmo lugar, **a de baixo vence** — silenciosamente, sem erro,
sem aviso no navegador.

O resultado: a melhoria inteira ficou no arquivo, passou por revisão, foi para
produção, e **nunca apareceu para ninguém**. O botão "Incluir SPs", o botão
"Excluir lote" e os quadrinhos novos existiam no código e não existiam na tela.

É a mesma família dos outros defeitos que este projeto já pagou caro:
`{% block script %}` em vez de `scripts`, nome indefinido, rota que não existe.
Todos silenciosos, todos descobertos tarde. A defesa é sempre a mesma — uma
varredura que roda junto com a suíte.

Este arquivo não usa navegador nem banco: lê o texto das telas.
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

TELAS = Path("app/apps/erp/templates")

# Só declarações no PRIMEIRO nível do script. Função dentro de função pode
# repetir nome à vontade — cada uma vive no seu próprio escopo, e aí não há
# atropelo nenhum.
DECLARACAO = re.compile(r"^(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(", re.M)

# O mesmo vale para `const`/`let` no primeiro nível: repetir um `const` no
# mesmo escopo não é atropelo silencioso, é erro de sintaxe que MATA a tela
# inteira — pior ainda. Já aconteceu neste projeto com `moeda` e `numero`.
CONSTANTE = re.compile(r"^(?:const|let)\s+([A-Za-z_$][\w$]*)\s*=", re.M)


def _arquivos():
    return sorted(TELAS.glob("*.html"))


def _script(texto: str) -> str:
    """Só o que está dentro de <script> sem src, que é onde a tela declara."""
    partes = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", texto, re.S)
    return "\n".join(partes)


@pytest.mark.parametrize("caminho", _arquivos(), ids=lambda p: p.name)
def test_a_tela_nao_declara_a_mesma_funcao_duas_vezes(caminho: Path):
    corpo = _script(caminho.read_text(encoding="utf-8"))
    repetidas = [nome for nome, n in Counter(DECLARACAO.findall(corpo)).items() if n > 1]
    assert not repetidas, (
        f"{caminho.name} declara {repetidas} mais de uma vez no mesmo escopo. "
        f"Em JavaScript a declaração DE BAIXO vence, calada: a de cima vira "
        f"código morto que ninguém percebe. Foi exatamente assim que a melhoria "
        f"dos lotes ficou um mês invisível. Apague a versão velha ou junte as duas."
    )


@pytest.mark.parametrize("caminho", _arquivos(), ids=lambda p: p.name)
def test_a_tela_nao_declara_a_mesma_constante_duas_vezes(caminho: Path):
    corpo = _script(caminho.read_text(encoding="utf-8"))
    repetidas = [nome for nome, n in Counter(CONSTANTE.findall(corpo)).items() if n > 1]
    assert not repetidas, (
        f"{caminho.name} declara {repetidas} mais de uma vez no primeiro nível. "
        f"`const` repetido no mesmo escopo é erro de sintaxe e MATA a tela "
        f"inteira — nem a primeira linha desenha."
    )
