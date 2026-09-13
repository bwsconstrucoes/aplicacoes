"""Toda coluna de resposta tem de dizer O QUE ELA É.

POR QUE ESTA VARREDURA EXISTE.
A tela de Perguntar já teve, dentro dela, a lista escrita à mão das colunas
que eram dinheiro. O resultado apareceu no navegador em 11/09/2026: "Já pago"
saía 4500, "Último preço" saía 33.9, e a data de vencimento do seguro garantia
saía 2026-08-30 — formato de banco, não de gente.

E o defeito era do tipo pior: SILENCIOSO e REINCIDENTE. Pergunta nova trazia
coluna nova, ninguém lembrava de ir na tela acrescentar o nome dela na lista, e
a tabela ficava bonita mostrando número americano. Ninguém percebe, porque não
dá erro.

Agora quem diz o tipo é a resposta, e esta varredura recusa coluna sem tipo
declarado — inclusive as de texto, que são declaradas de propósito: é o
silêncio que esconde defeito.
"""
from __future__ import annotations

import re
from pathlib import Path

from app.apps.erp.core.perguntas.respostas import TIPO_DA_COLUNA

FONTE = Path("app/apps/erp/core/perguntas/respostas.py").read_text(encoding="utf-8")
TELA = Path("app/apps/erp/templates/erp_perguntar.html").read_text(encoding="utf-8")

TIPOS_VALIDOS = {"texto", "dinheiro", "numero", "data"}


def _colunas_usadas() -> set[str]:
    """As chaves de coluna que as respostas realmente montam.

    Elas aparecem sempre como o par ("chave", "Rótulo") dentro de `colunas=`.
    """
    return {chave for chave in re.findall(r'\("([a-z_0-9]+)", "[^"]+"\)', FONTE)}


def test_toda_coluna_usada_tem_tipo_declarado():
    faltando = sorted(_colunas_usadas() - set(TIPO_DA_COLUNA))
    assert not faltando, (
        "Estas colunas aparecem numa resposta mas não dizem o que são: "
        f"{faltando}. Declare cada uma em TIPO_DA_COLUNA — inclusive as de "
        "texto. Sem isso a tela adivinha, e adivinha errado em silêncio.")


def test_todo_tipo_declarado_e_um_tipo_que_a_tela_entende():
    estranhos = {c: t for c, t in TIPO_DA_COLUNA.items() if t not in TIPOS_VALIDOS}
    assert not estranhos, f"Tipo que a tela não sabe desenhar: {estranhos}"


def test_a_tela_nao_tem_mais_lista_de_coluna_escrita_a_mao():
    """Se a lista voltar para a tela, o defeito volta junto."""
    for proibido in ("COLUNAS_DE_DINHEIRO", "COLUNAS_DE_NUMERO"):
        assert proibido not in TELA, (
            f"{proibido} voltou para a tela. O tipo da coluna é dito pelo "
            f"servidor — duas listas divergem no dia em que alguém edita uma.")


def test_a_tela_usa_o_tipo_que_o_servidor_manda():
    assert 'coluna.tipo === "dinheiro"' in TELA
    assert 'coluna.tipo === "data"' in TELA


def test_as_respostas_carimbam_o_tipo_em_cada_coluna():
    assert 'TIPO_DA_COLUNA.get(c, "texto")' in FONTE
