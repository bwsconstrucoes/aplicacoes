# -*- coding: utf-8 -*-
"""
Fora da análise, em Parâmetros — vale para a prestação inteira.

Pedido do dono em 22/09/2026: *"na parte de configurações da prestação de
conta, pra eu poder eliminar projetos e/ou obras dessa análise"* — e não só
dentro de um cenário. A lista vive na configuração da prestação e é aplicada
na base, antes de qualquer conta; a lista de cada cenário soma-se a ela.
"""
from __future__ import annotations

from app.apps.painel import prestacao


def test_a_lista_gravada_vira_itens_sem_repetir_nem_vazios():
    texto = "obra:CASA; projeto:ALFA;;obra:CASA\nobra:LOJA;  "
    assert prestacao.itens_fora_da_analise(texto) == [
        "obra:CASA", "projeto:ALFA", "obra:LOJA"]
    assert prestacao.itens_fora_da_analise("") == []
    assert prestacao.itens_fora_da_analise(None) == []


def test_tirar_da_base_alcanca_apuracao_pessoal_e_caixa():
    """As três leituras que dependem de obra perdem a obra tirada; o resto da
    base (a despesa administrativa) passa intacto."""
    base = {
        "apuracao": [{"obra": "CASA", "mes": "2025-01"},
                     {"obra": "LOJA ", "mes": "2025-01"}],
        "pessoal": [("2025-01", "CASA", 100.0), ("2025-01", "LOJA", 50.0)],
        "caixa": [("2025-01", "CASA", -10.0), ("2025-01", "LOJA", 5.0)],
        "admin": [{"grupo": "Administrativas", "valor": -900.0}],
    }
    saida = prestacao.sem_as_obras(base, {"LOJA"})
    assert [l["obra"] for l in saida["apuracao"]] == ["CASA"]
    assert saida["pessoal"] == [("2025-01", "CASA", 100.0)]
    assert saida["caixa"] == [("2025-01", "CASA", -10.0)]
    assert saida["admin"] == base["admin"]
    # sem nada para tirar, a base volta como veio
    assert prestacao.sem_as_obras(base, set()) is base


def test_um_projeto_tirado_leva_as_obras_dele_e_a_estrutura_nunca_sai():
    mapa = {"CASA": "ALFA", "LOJA": "ALFA", "PONTE": "BETA", "MATRIZ": "ALFA"}
    obras = {"CASA", "LOJA", "PONTE"}  # a matriz já não está entre as obras
    fora = prestacao.obras_fora_da_analise(
        ["projeto:ALFA", "obra:PONTE"], mapa, obras)
    assert fora == {"CASA", "LOJA", "PONTE"}
