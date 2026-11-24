"""A ARMADILHA DA CONSTANTE QUE LÊ DADO QUE AINDA NÃO CHEGOU.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026).

Na tela de Colaboradores havia esta linha, no topo do script:

    const COLUNAS_DO_QUADRO = ETAPAS.map(e => e.valor);

`ETAPAS` é uma lista que só é preenchida quando o servidor responde (dentro de
`carregar`). No momento em que a linha roda, ela é `[]` — e a constante
congelava esse vazio **para sempre**. O efeito, que o dono viu na tela:

> *"Eu clico em inicial, o contador está tudo zerado. Do inicial, do assinar,
> concluir, está tudo zerado. Eu clico em quadro, também não está trazendo
> nada."*

E o pior desta classe de defeito: **nada quebra**. Não há erro no navegador,
o servidor responde 200, o HTML está correto e a suíte inteira passa. A tela
simplesmente mostra zero em tudo, e só quem olha percebe.

O que este arquivo confere: nenhuma tela declara, no topo do script, uma
constante cujo valor é CALCULADO a partir de uma variável de estado que a tela
preenche depois. Quem precisa do valor calculado usa FUNÇÃO — que lê na hora.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

TELAS = sorted((Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
                / "templates").glob("erp_*.html"))

# As variáveis de ESTADO de uma tela: declaradas vazias no topo e preenchidas
# quando a resposta do servidor chega.
VAZIO = re.compile(
    r"^(?:let|var)\s+(.+?);\s*$", re.MULTILINE)
# `const X = ALGUMA_COISA.map(…)` / `.filter(…)` / `.length` — o valor nasce
# calculado, e é isso que o congela.
#
# ⚑ SÓ NA COLUNA ZERO, e isto é a diferença entre o teste servir e o teste
# atrapalhar: `const` com recuo está DENTRO de uma função, e aí roda na hora da
# chamada — calcular sobre o estado ali é o certo. O defeito é a constante de
# TOPO, que roda uma vez quando o arquivo carrega.
CALCULADA = re.compile(
    r"^const\s+([A-Za-z_$][\w$]*)\s*=\s*([A-Za-z_$][\w$]*)\s*\.\s*"
    r"(?:map|filter|slice|concat|reduce|flatMap|length|join)\b",
    re.MULTILINE)


def _estado_vazio(texto: str) -> set[str]:
    """Os nomes declarados com `let`/`var` e valor vazio (`[]`, `{}`, `null`)."""
    nomes: set[str] = set()
    for linha in VAZIO.findall(texto):
        # "COL = [], OBRAS = [], ETAPAS = []" → três nomes
        for parte in linha.split(","):
            if "=" not in parte:
                continue
            nome, _, valor = parte.partition("=")
            nome, valor = nome.strip(), valor.strip()
            if not re.fullmatch(r"[A-Za-z_$][\w$]*", nome):
                continue
            if valor in ("[]", "{}", "null", "undefined", '""', "''"):
                nomes.add(nome)
    return nomes


@pytest.mark.parametrize("tela", TELAS, ids=lambda p: p.name)
def test_constante_do_topo_nao_calcula_sobre_estado_que_chega_depois(tela):
    texto = tela.read_text(encoding="utf-8")
    estado = _estado_vazio(texto)
    if not estado:
        return

    culpadas = [(c, o) for c, o in CALCULADA.findall(texto) if o in estado]

    assert culpadas == [], (
        f"{tela.name}: "
        + "; ".join(f"`const {c}` é calculada a partir de `{o}`, que a tela "
                    f"preenche DEPOIS — vai congelar vazia" for c, o in culpadas)
        + ". Troque por uma função, que lê o valor na hora de usar."
    )
