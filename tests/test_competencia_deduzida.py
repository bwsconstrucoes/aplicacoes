"""A COMPETÊNCIA SAIU DA TELA — e continua certa.

22/09/2026, o dono: *"no lançamento do título não queria precisar lançar
competência, nem usamos isso."*

Ela saiu do FORMULÁRIO, não do sistema — e essa distinção é o ponto. A coluna
continua preenchida porque três coisas dependem dela, e nenhuma aparece na tela
de lançamento:

  * o relatório analítico filtra e agrupa por competência;
  * a crítica de duplicidade compara "mesmo valor no mesmo mês" (D4) e
    "aluguel já lançado neste mês" (D5) por ela;
  * a NFS-e emitida leva a competência dentro do XML.

A ORDEM DA DEDUÇÃO é o que estes testes defendem, e ela NÃO começa em "hoje":
uma nota de agosto lançada em outubro cairia no custo de outubro, e a obra
fecharia o mês com despesa que não é dela. Por isso vale primeiro a emissão do
documento, depois o primeiro vencimento, e "hoje" só quando não há nem uma nem
outra.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.titulos.service import _competencia_deduzida
from app.apps.erp.db.models.financeiro import Parcela


def _parcelas(*dias):
    return [Parcela(numero=i + 1, valor=Decimal("100"), vencimento=d)
            for i, d in enumerate(dias)]


def test_a_emissao_do_documento_manda():
    """É a resposta contabilmente certa, e é campo que a pessoa já preenche —
    ou que a leitura da nota preenche sozinha."""
    comp = _competencia_deduzida({"data_emissao_doc": "2026-08-14"},
                                 _parcelas(date(2026, 10, 5)))
    assert comp == date(2026, 8, 1), "o dia sempre vira 1: competência é MÊS"


def test_sem_emissao_vale_o_PRIMEIRO_vencimento():
    comp = _competencia_deduzida({}, _parcelas(date(2026, 11, 20),
                                               date(2026, 9, 10),
                                               date(2026, 10, 15)))
    assert comp == date(2026, 9, 1), "o menor vencimento, não o primeiro da lista"


def test_sem_emissao_e_sem_parcela_cai_em_hoje():
    comp = _competencia_deduzida({}, [])
    assert comp == date.today().replace(day=1)


def test_o_que_foi_informado_continua_valendo():
    """Nada aqui tira a competência de quem a mandar — o importador do Pipefy
    manda, e um dia a tela pode voltar a oferecer o campo."""
    assert _competencia_deduzida({"competencia": "2026-03"}, _parcelas(date(2026, 12, 1))) \
        == date(2026, 3, 1)


@pytest.mark.parametrize("vazio", ["", "   ", None])
def test_competencia_em_branco_nao_conta_como_informada(vazio):
    """A tela deixou de mandar o campo. Se "vazio" fosse lido como valor, o
    lançamento quebraria em vez de deduzir."""
    comp = _competencia_deduzida({"competencia": vazio, "data_emissao_doc": "2026-07-03"},
                                 _parcelas(date(2026, 9, 1)))
    assert comp == date(2026, 7, 1)


def test_emissao_em_branco_nao_atropela_o_vencimento():
    comp = _competencia_deduzida({"data_emissao_doc": ""}, _parcelas(date(2026, 5, 9)))
    assert comp == date(2026, 5, 1)


# ---------------------------------------------------------------------------
# A CRÍTICA DE DUPLICIDADE TEM DE DEDUZIR IGUAL
#
# Com o campo fora da tela, D4 e D5 parariam de rodar CALADAS — a falha mais
# cara que existe, porque ninguém nota que a rede de proteção saiu do ar.
# ---------------------------------------------------------------------------
def _titulo_existente(competencia, valor="500.00", categoria_id=3):
    from app.apps.erp.db.models.cadastros import FormaPagamento
    from app.apps.erp.db.models.financeiro import StatusTitulo, Titulo
    t = Titulo(id=1, numero_sp="000001", descricao="Aluguel do galpão",
               fornecedor_id=9, categoria_id=categoria_id,
               valor_bruto=Decimal(valor), valor_retencoes=Decimal("0.00"),
               valor_liquido=Decimal(valor), competencia=competencia,
               forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO)
    t.parcelas = [Parcela(id=1, titulo_id=1, numero=1, valor=Decimal(valor),
                          vencimento=date(2026, 9, 25))]
    return t


def test_a_critica_D4_continua_rodando_sem_o_campo_na_tela():
    """SEM a dedução aqui, a tela mandaria competência vazia e a D4 pararia de
    rodar calada — a falha mais cara que existe, porque ninguém nota que a rede
    de proteção saiu do ar.

    O vencimento novo fica LONGE do que já existe de propósito: perto, quem
    avisa é a D3 (vencimento próximo), que por desenho silencia a D4 para não
    dar dois avisos do mesmo título — e o teste passaria sem provar nada."""
    from app.apps.erp.core.titulos import duplicidade
    from conftest import SessaoFalsa

    existente = _titulo_existente(date(2026, 8, 1))
    s = SessaoFalsa(existente, *existente.parcelas)

    r = duplicidade.checar(s, {
        "fornecedor_id": 9, "valor": "500,00",
        "data_emissao_doc": "2026-08-14",          # mesma competência do que existe
        "parcelas": [{"vencimento": "2026-12-20"}]})   # longe do vencimento existente

    assert any(a["codigo"] == "D4" for a in r["alertas"]), \
        "mesmo credor, mesmo valor, mesma competência — tinha de avisar"


def test_a_critica_D4_nao_avisa_quando_a_competencia_e_outra():
    """O outro lado: avisar de tudo é o mesmo que não avisar de nada."""
    from app.apps.erp.core.titulos import duplicidade
    from conftest import SessaoFalsa

    existente = _titulo_existente(date(2026, 8, 1))
    s = SessaoFalsa(existente, *existente.parcelas)

    r = duplicidade.checar(s, {
        "fornecedor_id": 9, "valor": "500,00",
        "data_emissao_doc": "2026-09-14",          # mês seguinte
        "parcelas": [{"vencimento": "2026-12-20"}]})

    assert not any(a["codigo"] == "D4" for a in r["alertas"])


def test_sem_emissao_a_critica_usa_o_MENOR_vencimento():
    """A dedução da crítica tem de ser a MESMA da gravação — senão a tela
    avisa de uma coisa e o banco grava outra."""
    from app.apps.erp.core.titulos import duplicidade
    from conftest import SessaoFalsa

    existente = _titulo_existente(date(2026, 9, 1))
    s = SessaoFalsa(existente, *existente.parcelas)

    r = duplicidade.checar(s, {
        "fornecedor_id": 9, "valor": "500,00",
        "parcelas": [{"vencimento": "2026-12-20"}, {"vencimento": "2026-09-01"}]})

    assert any(a["codigo"] == "D4" for a in r["alertas"]), \
        "o menor vencimento é setembro, igual à competência do que já existe"
