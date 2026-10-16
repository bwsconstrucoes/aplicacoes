# -*- coding: utf-8 -*-
"""A Somapay não tem mais tratamento próprio — removida em 16/10/2026.

Decisão do dono, com todas as letras:

> *"O comprovante faz a baixa dentro da regra normal dos outros comprovantes
> Bradesco. Vamos remover essa regra especial."*
> *"O comprovante emitido pelo próprio Soma não vai ser mais usado, remova a
> regra também."*

Eram **dois** caminhos, e os dois saíram:

1. **Bradesco → Somapay.** O robô reconhecia "Instituição destino: Somapay",
   achava a conta que recebeu pela chave PIX, **lançava a transferência no
   Omie** e só então baixava, na conta Somapay. Agora é comprovante comum do
   Bradesco: baixa na conta que o comprovante debitou, sem transferência
   nenhuma.
2. **Comprovante emitido pela própria Somapay.** Reconhecido por "Somapay
   Sociedade de Crédito Direto", casado por nome do beneficiário + valor, e
   baixado na conta Somapay achada pelo nome do depositante. A empresa parou de
   usar esse comprovante.

Este arquivo existe para a remoção não voltar por engano — é fácil alguém
reintroduzir um atalho "se for Somapay, faz diferente" sem saber que ele foi
tirado de propósito. Os dois comprovantes de exemplo continuam no repositório e
são lidos aqui: é a prova de que passam pelo caminho comum.
"""
import os

import pytest

from app.apps.baixabradesco import core, matcher, omie, sheets
from app.apps.baixabradesco.models import ExtractedReceipt
from app.apps.baixabradesco.parser_bradesco import parse_bradesco_text

EXEMPLOS = os.path.join(os.path.dirname(__file__), 'exemplos_baixabradesco')


def _exemplo(nome):
    with open(os.path.join(EXEMPLOS, nome), encoding='utf-8') as f:
        return f.read()


# =====================================================================
# 1. o leitor não cria mais tipo próprio para Somapay
# =====================================================================

def test_transferencia_para_somapay_vira_comprovante_comum():
    rec = parse_bradesco_text('transferencia.pdf', 1,
                              _exemplo('bradesco_transferencia_somapay.txt'))

    assert rec.tipo_comprovante not in ('somapay', 'somapay_deposito')
    # cai na classificação comum do Bradesco
    assert rec.tipo_comprovante in ('pix', 'transferencia', 'bradesco')


def test_comprovante_emitido_pela_somapay_tambem_vira_comum():
    rec = parse_bradesco_text('deposito.pdf', 1,
                              _exemplo('somapay_deposito_rescisorio.txt'))

    assert rec.tipo_comprovante not in ('somapay', 'somapay_deposito')


def test_o_valor_e_a_data_continuam_sendo_lidos():
    """Tirar a regra não pode cegar o leitor: valor e data são o mínimo."""
    for arquivo in ('bradesco_transferencia_somapay.txt',
                    'somapay_deposito_rescisorio.txt'):
        rec = parse_bradesco_text(arquivo, 1, _exemplo(arquivo))
        assert rec.valor_pago, arquivo
        assert rec.data_pagamento, arquivo


# =====================================================================
# 2. nada de Somapay sobrou no código
# =====================================================================

def test_o_casador_nao_tem_mais_estrategia_somapay():
    assert not hasattr(matcher, 'match_somapay')
    assert not hasattr(matcher, 'SOMAPAY_TIPOS_DESPESA')
    assert not hasattr(matcher, 'match_nome_valor')


def test_o_plano_do_omie_nao_tem_mais_transferencia():
    """A transferência automática Bradesco → Somapay saiu inteira."""
    for nome in ('build_somapay_plan', 'build_transferencia_somapay',
                 'build_alterar_conta_pagar_somapay',
                 'build_lancar_pagamento_somapay', 'resolver_conta_somapay',
                 'build_incluir_lanc_cc', 'execute_omie_lanccc'):
        assert not hasattr(omie, nome), nome


def test_a_busca_de_conta_somapay_saiu_da_planilha():
    for nome in ('find_somapay_account', 'apelido_somapay',
                 'find_account_by_pix_key'):
        assert not hasattr(sheets, nome), nome


def test_o_plano_nao_tem_mais_conta_de_destino():
    from app.apps.baixabradesco.models import ExecutionPlan
    assert 'banco_destino' not in ExecutionPlan.__dataclass_fields__


def test_nenhum_modulo_da_area_fala_em_somapay_como_regra():
    """Menção em comentário histórico pode ficar; regra viva, não."""
    import inspect
    for mod in (core, matcher, omie, sheets):
        fonte = inspect.getsource(mod)
        assert "'somapay'" not in fonte, mod.__name__
        assert "'somapay_deposito'" not in fonte, mod.__name__


# =====================================================================
# 3. o caminho comum continua inteiro
# =====================================================================

def test_o_plano_do_omie_continua_com_os_tres_passos_de_sempre():
    """Consultar, alterar se preciso, baixar — sem passo de transferência."""
    import inspect
    fonte = inspect.getsource(omie.build_omie_plan)
    assert 'consultar' in fonte
    assert 'baixar' in fonte
    assert 'lanccc' not in fonte


def test_o_executor_nao_tem_mais_desvio_de_transferencia():
    import inspect
    fonte = inspect.getsource(core)
    assert 'lanccc' not in fonte
    assert 'abort_apos_transferencia' not in fonte


def test_comprovante_sem_conta_reconhecivel_fica_pendente_nao_baixa():
    """O comprovante emitido pela Somapay não traz a conta da empresa.

    Sem a regra que a buscava pelo nome do depositante, ele não tem conta para
    baixar — e o certo é ficar PENDENTE, nunca baixar numa conta chutada.
    """
    from app.apps.baixabradesco.models import ExecutionPlan, MatchResult, SpRecord

    plano = ExecutionPlan(
        receipt=ExtractedReceipt(filename='x.pdf', page=1, valor_pago='1.000,00',
                                 data_pagamento='07/10/2026'),
        match=MatchResult(status='localizado', metodo='teste', id='1234567',
                          sp=SpRecord(row_number=2, id='1234567')),
        banco=None,
    )
    core._decidir_execucao(plano, True, False, False, False)

    assert plano.acao == 'pendente_validacao'
    assert plano.pode_executar is False
    assert any('codigo_conta_omie' in m for m in plano.motivos_bloqueio)
