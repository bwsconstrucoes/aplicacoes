# -*- coding: utf-8 -*-
"""
LANÇAR NO OMIE a partir do extrato — 24/09/2026.

Pedido do dono: *"tarifas bancárias, rentabilidade de investimento (…) tem
muita tarifa de PIX. Esse aí a gente poder lançar direto no OMIE."*

⚠️ ISTO ESCREVE NO OMIE. Estes testes existem para as quatro proteções não
saírem sem que alguém perceba: nada sem tipo reconhecido, nada duas vezes, a
conta corrente vindo da CONTA BANCÁRIA, e o ensaio antes do envio.
"""
import datetime as dt
from decimal import Decimal

import pytest

from app.apps.analisesps import conciliacao_omie as co


TIPOS = [
    {"id": 1, "nome": "Tarifa bancária", "palavras": "TARIFA;TAR ",
     "codigo_categoria": "2.01.05", "codigo_cliente": 111,
     "cod_departamento": "", "ativo": True, "ordem": 0},
    {"id": 2, "nome": "Tarifa PIX", "palavras": "TARIFA BANCARIA",
     "codigo_categoria": "2.01.06", "codigo_cliente": 111,
     "cod_departamento": "", "ativo": True, "ordem": 1},
    {"id": 3, "nome": "Rentabilidade", "palavras": "RENTAB;RENDE FACIL",
     "codigo_categoria": "1.05.01", "codigo_cliente": 222,
     "cod_departamento": "", "ativo": True, "ordem": 2},
]

CONTA = {"id": 1, "nome": "BD 7011", "omie_conta_corrente": 9999}


def linha(id_=10, descricao="TARIFA BANCARIA", valor="-9.00", **extra):
    base = {"id": id_, "data": dt.date(2026, 9, 10), "descricao": descricao,
            "documento": "41024", "valor": Decimal(valor),
            "omie_codigo": None}
    base.update(extra)
    return base


# ---------------------------------------------------------------------------
# Reconhecer o tipo pelo histórico
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("descricao,esperado", [
    ("TARIFA BANCARIA\nTRANSF PGTO PIX", "Tarifa PIX"),
    ("Tarifa Pacote de Serviços", "Tarifa bancária"),
    ("RENTAB.INVEST FACILCRED*", "Rentabilidade"),
    ("BB Rende Fácil — Rende Facil", "Rentabilidade"),
])
def test_o_tipo_sai_do_historico_sem_ligar_para_acento_nem_caixa(descricao,
                                                                 esperado):
    """O extrato escreve a mesma coisa de vários jeitos: "TARIFA BANCARIA",
    "Tarifa Pacote de Serviços", "Rende Fácil"."""
    achado = co.reconhecer(descricao, TIPOS)
    assert achado and achado["nome"] == esperado


def test_quando_dois_tipos_casam_vale_o_MAIS_ESPECIFICO():
    """⚠️ "TARIFA BANCARIA" casa com "TARIFA" e com "TARIFA BANCARIA". Quem
    cadastrou os dois quis o específico — senão não teria cadastrado."""
    achado = co.reconhecer("TARIFA BANCARIA TRANSF PGTO PIX", TIPOS)
    assert achado["nome"] == "Tarifa PIX"


def test_historico_desconhecido_NAO_e_chutado():
    """⚠️ Chutar a categoria é pior do que não lançar: no OMIE vira relatório
    errado que ninguém desconfia."""
    assert co.reconhecer("PAGTO ELETRON COBRANCA VOTORANTIM", TIPOS) is None
    assert co.reconhecer("", TIPOS) is None


# ---------------------------------------------------------------------------
# O ensaio — o que SERIA mandado
# ---------------------------------------------------------------------------
def test_o_sentido_vem_do_SINAL_do_valor():
    """⚠️ Não é configurado: negativo é conta a pagar, positivo é conta a
    receber. Um estorno de tarifa entra sozinho do lado certo, e não há campo
    a mais para alguém marcar errado."""
    plano = co.planejar(
        [linha(10, "TARIFA BANCARIA", "-9.00"),
         linha(11, "RENTAB.INVEST FACILCRED*", "3.87")], CONTA, TIPOS)

    por_linha = {x["linha_id"]: x for x in plano["vai"]}
    assert por_linha[10]["sentido"] == "pagar"
    assert por_linha[10]["valor"] == Decimal("9.00")
    assert por_linha[11]["sentido"] == "receber"
    assert por_linha[11]["valor"] == Decimal("3.87")


def test_a_conta_corrente_vem_da_CONTA_BANCARIA_e_nao_do_tipo():
    """⚠️ Lançar uma tarifa do Bradesco dentro da conta do Santander é o erro
    mais caro possível aqui. O único jeito de não cometê-lo é não ter onde
    errar: a conta corrente do OMIE mora na conta bancária, e só lá."""
    plano = co.planejar([linha()], CONTA, TIPOS)
    assert plano["vai"][0]["id_conta_corrente"] == 9999
    assert "id_conta_corrente" not in TIPOS[0]


def test_linha_ja_lancada_nao_entra_de_novo():
    plano = co.planejar([linha(omie_codigo=555)], CONTA, TIPOS)
    assert plano["vai"] == []
    assert "já foi lançada" in plano["nao_vai"][0]["motivo"]


def test_sem_conta_corrente_do_OMIE_a_linha_e_recusada_com_o_motivo():
    plano = co.planejar([linha()], {"id": 1, "nome": "X",
                                    "omie_conta_corrente": None}, TIPOS)
    assert plano["vai"] == []
    assert "conta corrente do OMIE" in plano["nao_vai"][0]["motivo"]


def test_tipo_sem_categoria_e_recusado_com_o_nome_do_tipo():
    """A lista do que NÃO vai é tão importante quanto a do que vai: é ela que
    diz o que falta configurar, em vez de o lote inteiro falhar sem explicar."""
    capengas = [dict(TIPOS[0], codigo_categoria="")]
    plano = co.planejar([linha()], CONTA, capengas)
    assert "Tarifa bancária" in plano["nao_vai"][0]["motivo"]
    assert "categoria" in plano["nao_vai"][0]["motivo"]


def test_tipo_sem_fornecedor_e_recusado():
    capengas = [dict(TIPOS[0], codigo_cliente=None)]
    plano = co.planejar([linha()], CONTA, capengas)
    assert "fornecedor" in plano["nao_vai"][0]["motivo"]


def test_historico_desconhecido_diz_o_que_fazer():
    plano = co.planejar([linha(descricao="COISA QUE NINGUEM CADASTROU")],
                        CONTA, TIPOS)
    assert "Cadastre um tipo" in plano["nao_vai"][0]["motivo"]


# ---------------------------------------------------------------------------
# O que vai para o OMIE
# ---------------------------------------------------------------------------
def test_o_numero_do_documento_respeita_o_teto_de_20_do_OMIE():
    """⚠️ Passar de 20 caracteres fez o OMIE recusar OITO vezes seguidas em
    21/09/2026, nos aportes. A lição custou caro uma vez."""
    item = co.planejar([linha(documento="1" * 40)], CONTA, TIPOS)["vai"][0]
    param = co.montar_inclusao(item)
    assert len(param["numero_documento"]) <= 20


def test_o_historico_com_quebra_de_linha_vira_uma_linha_so():
    """O Bradesco manda "TARIFA BANCARIA\\nTRANSF PGTO PIX". Mandar a quebra
    para o OMIE é pedir para o campo chegar torto do outro lado."""
    item = co.planejar([linha(descricao="TARIFA BANCARIA\nTRANSF PGTO PIX")],
                       CONTA, TIPOS)["vai"][0]
    assert "\n" not in co.montar_inclusao(item)["observacao"]


def test_o_codigo_de_integracao_sai_da_LINHA_e_nao_do_relogio():
    """⚠️ É o que faz reenviar ser seguro: o OMIE recusa o código repetido. Um
    código com a hora dentro criaria um título novo a cada tentativa."""
    item = co.planejar([linha(id_=42)], CONTA, TIPOS)["vai"][0]
    assert item["codigo_integracao"] == "CONC42"
    de_novo = co.planejar([linha(id_=42)], CONTA, TIPOS)["vai"][0]
    assert de_novo["codigo_integracao"] == item["codigo_integracao"]


def test_a_baixa_usa_a_conta_corrente_e_a_data_do_extrato():
    """⚠️ A baixa não é opcional aqui, e é diferente do aporte: a linha veio do
    EXTRATO, o dinheiro já saiu. Título em aberto deixaria saldo falso no
    OMIE."""
    item = co.planejar([linha()], CONTA, TIPOS)["vai"][0]
    baixa = co.montar_baixa(item, 777)
    assert baixa["codigo_lancamento"] == 777
    assert baixa["codigo_conta_corrente"] == 9999
    assert baixa["data"] == "10/09/2026"
    assert baixa["valor"] == 9.0


# ---------------------------------------------------------------------------
# TRANSFERÊNCIA ENTRE CONTAS — 24/09/2026
#
# *"Transferência no OMIE: você seleciona a conta origem e a conta destino, e
# já interfere nas duas pontas."*
#
# ⚠️ COMO O OMIE REPRESENTA ISSO, e foi o espelho do painel que respondeu: um
# PAR DE TÍTULOS com a categoria marcada como transferência. É essa marca que
# a tira do DRE. Não há rota especial a inventar — é o mesmo caminho dos
# aportes, que roda e que ele já validou.
# ---------------------------------------------------------------------------
TIPO_TRANSF = {"id": 9, "nome": "Transferência", "palavras": "TRANSF CC",
               "codigo_categoria": "9.99.99", "codigo_cliente": 333,
               "cod_departamento": "", "ativo": True, "ordem": 9,
               "natureza": "transferencia"}

DESTINO = {"id": 2, "nome": "BB 1234", "omie_conta_corrente": 8888}


class ClienteQueAnota:
    def __init__(self, falhar_em=None):
        self.chamadas = []
        self.falhar_em = falhar_em or []
        self.proximo = 100

    def _call(self, url, acao, param):
        self.chamadas.append((acao, param))
        if acao in self.falhar_em:
            raise RuntimeError("o OMIE recusou")
        self.proximo += 1
        return {"codigo_lancamento_omie": self.proximo}


def test_transferencia_SEM_destino_escolhido_e_recusada():
    """⚠️ Adivinhar o destino poria o dinheiro numa conta que ninguém pediu."""
    plano = co.planejar([linha(descricao="TRANSF CC PARA CC PJ")], CONTA,
                        [TIPO_TRANSF])
    assert plano["vai"] == []
    assert "escolha a conta de destino" in plano["nao_vai"][0]["motivo"]
    assert plano["nao_vai"][0]["pede_destino"] is True


def test_destino_sem_conta_corrente_do_OMIE_e_recusado():
    plano = co.planejar([linha(descricao="TRANSF CC PARA CC PJ")], CONTA,
                        [TIPO_TRANSF],
                        destinos={10: {"id": 2, "nome": "BB 1234",
                                       "omie_conta_corrente": None}})
    assert plano["vai"] == []
    assert "não tem a conta corrente do OMIE" in plano["nao_vai"][0]["motivo"]


def test_a_transferencia_cria_AS_DUAS_PONTAS(monkeypatch):
    """A saída na conta de origem e a entrada na de destino — é o que faz o
    dinheiro aparecer nas duas, como ele descreveu."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha(descricao="TRANSF CC PARA CC PJ",
                               valor="-30000.00")],
                        CONTA, [TIPO_TRANSF], destinos={10: DESTINO})
    assert plano["vai"][0]["transferencia"] is True

    cli = ClienteQueAnota()
    feito = co.lancar(plano["vai"], "MARCELO", cli)

    acoes = [a for a, _ in cli.chamadas]
    assert acoes == ["IncluirContaPagar", "LancarPagamento",
                     "IncluirContaReceber", "LancarRecebimento"]

    saida = cli.chamadas[0][1]
    entrada = cli.chamadas[2][1]
    assert saida["id_conta_corrente"] == 9999      # origem
    assert entrada["id_conta_corrente"] == 8888    # destino
    assert saida["valor_documento"] == entrada["valor_documento"] == 30000.0
    assert feito["feitos"][0]["codigo_par"]


def test_a_segunda_ponta_tem_codigo_de_integracao_PROPRIO(monkeypatch):
    """⚠️ Com o mesmo código, o OMIE recusaria a segunda ponta como repetição
    da primeira — e a transferência ficaria pela metade toda vez, sem ninguém
    entender por quê."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha(id_=42, descricao="TRANSF CC PARA CC PJ")],
                        CONTA, [TIPO_TRANSF], destinos={42: DESTINO})
    cli = ClienteQueAnota()
    co.lancar(plano["vai"], "T", cli)

    saida = cli.chamadas[0][1]["codigo_lancamento_integracao"]
    entrada = cli.chamadas[2][1]["codigo_lancamento_integracao"]
    assert saida == "CONC42"
    assert entrada == "CONC42D"
    assert saida != entrada


def test_meia_transferencia_GRITA(monkeypatch):
    """⚠️ Dinheiro que saiu de uma conta e não entrou em nenhuma: o saldo das
    DUAS fica errado. É o pior estado possível, e a tela tem de dizer isso com
    todas as letras em vez de contar como sucesso parcial."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha(descricao="TRANSF CC PARA CC PJ")], CONTA,
                        [TIPO_TRANSF], destinos={10: DESTINO})
    cli = ClienteQueAnota(falhar_em=["IncluirContaReceber"])

    feito = co.lancar(plano["vai"], "T", cli)

    assert len(feito["falhas"]) == 1
    erro = feito["falhas"][0]["erro"]
    assert "METADE DA TRANSFERÊNCIA" in erro
    assert "BB 1234" in erro
    assert "saldo das duas contas está errado" in erro


def test_o_tipo_normal_NAO_cria_segunda_ponta(monkeypatch):
    """Só a transferência tem duas pontas. Uma tarifa com segunda ponta seria
    dinheiro inventado."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha()], CONTA, TIPOS)
    cli = ClienteQueAnota()
    co.lancar(plano["vai"], "T", cli)

    assert [a for a, _ in cli.chamadas] == ["IncluirContaPagar",
                                            "LancarPagamento"]
