"""A trilha de auditoria lida como TRABALHO — o mapa e as fronteiras.

O dono pediu isto pensando em equipe trabalhando de casa: *"eu preciso que o
sistema entenda se aquele pessoal está trabalhando ou não em determinado dia.
(…) o cara começou a tal hora, fez isso, fez aquilo, fez conciliação, lançou
título."*

Aqui se prova a parte que NÃO depende de banco: a tradução de "entidade + ação"
para um tipo de trabalho que uma pessoa entende. Ela decide o que a tela mostra,
e errar nela faz um aprovador parecer que passa o dia lançando.

O que se prova:

  1. O título atravessa a vida inteira do lançamento (nasce, é analisado, é
     pago) — e cada momento conta como um tipo de trabalho DIFERENTE.
  2. Nenhuma ação registrada no ERP cai em "Outros" por esquecimento.
  3. Toda categoria usada tem nome em português.
  4. O aviso de que isto não é jornada de trabalho viaja com o dado, não só
     na tela.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.apps.erp.core.comum.uso import (
    CATEGORIAS, POR_ACAO, POR_ENTIDADE, ROTULO_DA_CATEGORIA, categoria_de,
)

RAIZ = Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"


def _acoes_registradas() -> set[tuple[str, str]]:
    """Todo par (entidade, ação) que o ERP realmente grava na trilha.

    Lido do CÓDIGO, não de uma lista escrita à mão: assim uma ação nova nasce
    coberta, e o teste avisa quando alguém acrescenta uma sem classificar.
    """
    padrao = re.compile(
        r'registrar_evento\(\s*\w+\s*,\s*"([a-z_]+)"\s*,[^,]+,\s*"([A-Z_]+)"')
    achadas: set[tuple[str, str]] = set()
    for arquivo in RAIZ.rglob("*.py"):
        if "__pycache__" in str(arquivo):
            continue
        for entidade, acao in padrao.findall(arquivo.read_text(encoding="utf-8")):
            achadas.add((entidade, acao))
    return achadas


# ---------------------------------------------------------------------------
# 1. O título é várias coisas ao longo da vida
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("acao, esperado", [
    ("CRIADO", "lancamento"),
    ("PARCELAS_ALTERADAS", "lancamento"),
    ("RECLASSIFICADO", "lancamento"),
    ("ANALISADO", "analise"),
    ("APROVADO", "analise"),
    ("DEVOLVIDO", "analise"),
    ("BAIXA_POR_COMPROVANTE", "pagamento"),
    ("ESTORNADO", "pagamento"),
    ("MEDICAO_LANCADA", "medicao"),
    ("PROTOCOLADA", "medicao"),
])
def test_cada_momento_do_titulo_e_um_trabalho_diferente(acao, esperado):
    """Contar tudo como "lançou" faria quem só aprova parecer que lança o dia
    inteiro — e é exatamente a leitura que o dono quer evitar."""
    assert categoria_de("titulo", acao) == esperado


def test_quem_aprova_nao_aparece_como_quem_lanca():
    assert categoria_de("titulo", "APROVADO") != categoria_de("titulo", "CRIADO")


# ---------------------------------------------------------------------------
# 2 e 3. Nada cai em "Outros" por esquecimento
# ---------------------------------------------------------------------------
def test_toda_acao_que_o_erp_grava_esta_classificada():
    """Ação nova sem classificação apareceria como "Outros" na tela — e
    "Outros: 47" não diz nada a quem está lendo o relatório."""
    registradas = _acoes_registradas()
    assert registradas, "não achei ação nenhuma no código — o padrão de busca quebrou"
    sem_classe = sorted(par for par in registradas
                        if categoria_de(*par) == "outro")
    assert sem_classe == [], (
        "estas ações não têm tipo de trabalho definido em core/comum/uso.py: "
        f"{sem_classe}")


def test_toda_categoria_usada_tem_nome_em_portugues():
    usadas = set(POR_ENTIDADE.values()) | set(POR_ACAO.values()) | {"outro"}
    sem_nome = sorted(c for c in usadas if c not in ROTULO_DA_CATEGORIA)
    assert sem_nome == []


def test_a_ordem_das_categorias_nao_tem_repetida():
    chaves = [c for c, _ in CATEGORIAS]
    assert len(chaves) == len(set(chaves))


def test_entidade_desconhecida_vira_outros_sem_estourar():
    """A trilha é append-only e guarda o passado: entidade que não existe mais
    no código continua lá, e não pode derrubar a tela."""
    assert categoria_de("coisa_que_nao_existe", "SEI_LA") == "outro"
    assert categoria_de(None, None) == "outro"


# ---------------------------------------------------------------------------
# 4. O limite viaja junto com o dado
# ---------------------------------------------------------------------------
def test_o_modulo_diz_que_isto_nao_e_jornada_de_trabalho():
    """Se o aviso vive só no HTML, o primeiro relatório exportado para fora do
    sistema o perde — e é aí que ele vira controle de ponto sem querer."""
    fonte = (RAIZ / "core" / "comum" / "uso.py").read_text(encoding="utf-8")
    assert "não é controle de jornada" in fonte.lower() \
        or "nao e controle de jornada" in fonte.lower()


def test_a_tela_avisa_que_trabalho_fora_do_sistema_nao_aparece():
    tela = (RAIZ / "templates" / "erp_uso.html").read_text(encoding="utf-8")
    assert "não é controle de ponto" in tela
    assert "não aparecem aqui" in tela
