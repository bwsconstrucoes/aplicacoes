# -*- coding: utf-8 -*-
"""A mesma página de comprovante não pode virar baixa duas vezes.

Cada página de PDF ganha uma impressão digital (o conteúdo do arquivo + o número
da página). Quando a baixa dá certo, essa impressão é gravada na aba
`LogBaixaBradesco`. O que faltava era o outro lado: **conferir a lista antes de
executar**. A função de conferência existia, era importada pelo `core.py` e
nunca era chamada — quem segurava o pagamento repetido era o Omie, respondendo
"título já pago". Proteção de terceiro, não nossa.

Corrigido em 04/09/2026. Estes testes existem para que o fio não se solte de
novo, e para que ninguém troque a leitura única do lote por uma consulta por
página — foi esse padrão que derrubou a instância em julho de 2026.
"""
import inspect

from app.apps.baixabradesco import core
from app.apps.baixabradesco.sheets import load_fingerprints_processados


# ── Dublês da planilha: nenhum teste encosta no Google ────────────────────────

class _AbaFalsa:
    def __init__(self, coluna, leituras=None):
        self._coluna = coluna
        self.leituras = leituras if leituras is not None else []

    def col_values(self, n):
        self.leituras.append(n)
        return self._coluna


class _PlanilhaFalsa:
    def __init__(self, aba):
        self._aba = aba

    def worksheet(self, nome):
        return self._aba


class _GoogleFalso:
    def __init__(self, aba):
        self._planilha = _PlanilhaFalsa(aba)

    def open_by_key(self, chave):
        return self._planilha


class _GoogleQuebrado:
    def open_by_key(self, chave):
        raise RuntimeError('429 Quota exceeded')


# ── A lista de comprovantes já baixados ───────────────────────────────────────

def test_le_a_lista_ignorando_o_cabecalho():
    aba = _AbaFalsa(['fingerprint', 'aaa111:1', 'bbb222:3'])
    assert load_fingerprints_processados(_GoogleFalso(aba)) == {'aaa111:1', 'bbb222:3'}


def test_celulas_vazias_nao_entram_na_lista():
    """Uma célula vazia viraria '' — e '' bateria com comprovante sem impressão."""
    aba = _AbaFalsa(['fingerprint', 'aaa111:1', '', '   '])
    assert load_fingerprints_processados(_GoogleFalso(aba)) == {'aaa111:1'}


def test_uma_unica_leitura_por_lote():
    """Uma consulta por página traria de volta o problema de memória e de cota."""
    aba = _AbaFalsa(['fingerprint', 'aaa111:1'])
    load_fingerprints_processados(_GoogleFalso(aba))
    assert aba.leituras == [1]


def test_falha_do_google_nao_derruba_o_lote():
    assert load_fingerprints_processados(_GoogleQuebrado()) == set()


# ── A trava está de fato ligada no fluxo ──────────────────────────────────────
#
# O defeito corrigido não era a função: era ela nunca ser chamada. Um teste de
# comportamento aqui exigiria PDF, planilha e Omie de verdade, então o que se
# confere é a ligação — e a ordem em que ela acontece.

FONTE = inspect.getsource(core.processar_baixabradesco)


def test_a_lista_e_carregada_uma_vez_no_lote():
    assert 'load_fingerprints_processados(gc)' in FONTE


def test_a_conferencia_acontece_antes_de_procurar_a_sp():
    posicao_conferencia = FONTE.find('in fingerprints_processados')
    posicao_match = FONTE.find('match_receipt(')
    assert posicao_conferencia != -1, 'a conferência de duplicidade sumiu do fluxo'
    assert posicao_match != -1
    assert posicao_conferencia < posicao_match, (
        'a duplicidade precisa ser barrada ANTES de procurar a SP — depois já '
        'houve leitura de planilha e risco de baixa repetida'
    )


def test_a_pagina_processada_entra_na_lista_do_proprio_lote():
    """O mesmo PDF enviado duas vezes no mesmo pedido também é duplicidade."""
    assert 'fingerprints_processados.add(' in FONTE


def test_o_que_foi_barrado_aparece_na_resposta():
    """Barrar em silêncio esconde do financeiro que chegou comprovante repetido."""
    assert "'duplicados_ja_baixados'" in FONTE
    assert "'duplicados': duplicados" in FONTE


def test_a_impressao_digital_carrega_o_numero_da_pagina():
    """Sem o número da página, um PDF de 5 comprovantes viraria uma baixa só."""
    assert "fingerprint=f'{fp_file}:{page_num}'" in FONTE
