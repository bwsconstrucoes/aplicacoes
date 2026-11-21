"""AS OPÇÕES DO `fetch` TÊM NOME EM INGLÊS — e errá-las não quebra nada.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026).

A tela de Colaboradores tinha três chamadas escritas assim:

    api(`/erp/api/colaboradores/${id}/etapa`,
        {metodo: "POST", corpo: {etapa: destino}})

`metodo` e `corpo` **não existem** no `fetch`. Os nomes certos são `method` e
`body`. O navegador não reclama de opção que não conhece: ele ignorou as duas,
mandou um **GET sem corpo**, a rota só aceita POST, e a tela mostrou "Falha na
operação" sem dizer mais nada.

As três chamadas eram as DUAS formas de mudar a etapa de uma pessoa (arrastar
no quadro e o diálogo da ficha) e o "concluir a conferência". Ou seja: a
funcionalidade que o dono mais tinha pedido — *"a movimentação das pessoas de
etapa tem que ser simples"* — estava inteiramente morta, e ele a encontrou
antes de qualquer teste: *"no quadro, não consigo movimentar os cadastros para
outras etapas."*

⚠️ É A MESMA CLASSE do defeito da constante congelada: escrever em português
uma coisa que a plataforma lê em inglês não dá erro nenhum — some em silêncio.
Por isso vira teste, e não só conserto.

Vale também para `cabecalhos` (headers), `sinal` (signal) e `credenciais`
(credentials), que são os outros nomes que a gente tenderia a traduzir.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1] / "app" / "apps"
TELAS = sorted(p for p in RAIZ.glob("*/templates/**/*.html"))

# O nome em português → o nome que o `fetch` entende.
TRADUZIDAS = {
    "metodo": "method",
    "método": "method",
    "corpo": "body",
    "cabecalhos": "headers",
    "cabeçalhos": "headers",
    "credenciais": "credentials",
    "sinal": "signal",
}

# `{metodo: ...}` ou `, metodo: ...` dentro de um objeto passado a api()/fetch().
# Procura o nome seguido de dois-pontos, precedido de `{` ou `,`.
PADRAO = re.compile(
    r"[{,]\s*(" + "|".join(TRADUZIDAS) + r")\s*:", re.IGNORECASE)


def _sem_comentarios(texto: str) -> str:
    """Tira os comentários Jinja e as linhas de comentário JS: o conserto de
    17/11 EXPLICA o defeito citando `{metodo: "POST"}`, e o teste não pode
    acusar a própria explicação."""
    texto = re.sub(r"\{#.*?#\}", "", texto, flags=re.DOTALL)
    return "\n".join(l for l in texto.splitlines()
                     if not l.lstrip().startswith("//"))


@pytest.mark.parametrize("tela", TELAS, ids=lambda p: p.name)
def test_nenhuma_chamada_usa_opcao_de_fetch_em_portugues(tela):
    achados = []
    for linha, texto in enumerate(_sem_comentarios(tela.read_text(encoding="utf-8")).splitlines(), 1):
        for m in PADRAO.finditer(texto):
            nome = m.group(1).lower()
            achados.append(f"linha {linha}: `{nome}:` — o fetch lê "
                           f"`{TRADUZIDAS[nome]}:`")

    assert achados == [], (
        f"{tela.name}: opção de fetch com nome em português. O navegador IGNORA "
        f"a opção que não conhece — a chamada vira um GET sem corpo e a tela diz "
        f"“Falha na operação” sem motivo.\n  " + "\n  ".join(achados))
