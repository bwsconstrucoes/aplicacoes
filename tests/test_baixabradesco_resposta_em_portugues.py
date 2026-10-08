# -*- coding: utf-8 -*-
"""As respostas de diagnóstico trazem uma frase em português.

Quem lê estes números é o dono da empresa, pelo celular, e não é programador. A
resposta crua é chave-e-número — útil para quem sabe o que procurar, ilegível
para quem está na rua querendo saber se a fila andou. A frase vem JUNTO com os
números, não em lugar deles: quem precisa do detalhe continua tendo o detalhe.

O caso que deu origem a isto é real: em 08/10/2026 o dono colou no chat a
resposta inteira de `fila-resumo`, campo por campo, para perguntar o que ela
queria dizer.
"""
import pytest

from app.apps.baixabradesco import conferencia, fila as mod


# =====================================================================
# a frase do resumo da fila
# =====================================================================

def test_a_frase_descreve_a_fila_real_de_08_10_2026():
    """Os números que o dono colou, virados em uma frase."""
    frase = mod._frase_do_resumo(
        por_etapa={'zapi': 1943, 'omie': 238, 'pipefy': 88},
        vencidos=2269, agendados=0, falhados=0, concluidos=0,
        mais_antigo='18/06/2026')

    assert '2.269 pendência(s)' in frase
    assert '1.943 de aviso de pagamento' in frase
    assert '238 de baixa no Omie' in frase
    assert '88 de cartão do Pipefy' in frase
    assert '18/06/2026' in frase


def test_a_etapa_aparece_com_nome_de_gente_nao_com_nome_tecnico():
    frase = mod._frase_do_resumo({'sheets': 3}, 3, 0, 0, 0, '')

    assert 'atualização da planilha' in frase
    assert 'sheets' not in frase


def test_a_maior_quantidade_vem_primeiro():
    frase = mod._frase_do_resumo({'omie': 5, 'zapi': 900}, 905, 0, 0, 0, '')

    assert frase.index('900') < frase.index('5 de baixa')


def test_fila_vazia_diz_isso_sem_rodeio():
    assert mod._frase_do_resumo({}, 0, 0, 0, 0, '') == 'Nenhuma pendência na fila.'
    assert 'Nenhuma pendência na fila; 2.000 já concluídas.' == \
        mod._frase_do_resumo({}, 0, 0, 0, 2000, '')


def test_o_que_desistiu_e_o_que_esta_agendado_sao_explicados():
    frase = mod._frase_do_resumo({'omie': 10}, 4, 3, 3, 0, '')

    assert 'cinco vezes' in frase
    assert 'agendadas para mais tarde' in frase


def test_o_milhar_usa_ponto_como_no_brasil():
    assert mod._em_milhar(1943) == '1.943'
    assert mod._em_milhar(52000) == '52.000'
    assert mod._em_milhar(88) == '88'


# =====================================================================
# a frase do conferidor
# =====================================================================

def test_a_frase_do_conferidor_separa_dinheiro_de_cadastro_errado():
    frase = conferencia._frase_da_conferencia(
        divergentes=2, nao_encontradas=1, erros=0, confirmadas=47, restam=0,
        dias=60)

    assert 'baixa pela metade' in frase
    assert 'o dinheiro saiu' in frase
    assert 'não é dinheiro' in frase          # o cadastro errado, dito como tal
    assert '47 estão certas' in frase


def test_sem_divergencia_a_frase_tranquiliza_sem_exagerar():
    frase = conferencia._frase_da_conferencia(0, 0, 0, 50, 0, 60)

    assert 'Nenhuma divergência' in frase
    assert '50' in frase
    assert '60 dias' in frase                 # diz a janela: não é "tudo certo"


def test_quando_sobra_para_conferir_a_frase_diz_como_continuar():
    frase = conferencia._frase_da_conferencia(0, 0, 0, 50, 180, 60)

    assert 'Faltam 180' in frase
    assert 'chame de novo' in frase
