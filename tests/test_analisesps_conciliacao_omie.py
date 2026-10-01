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


# ---------------------------------------------------------------------------
# O FORNECEDOR DO OMIE MORA NA CONTA — 25/09/2026
#
# *"0 de 1 linha(s) podem ser lançadas na conta BD 50024 (…) o tipo 'Tarifa
# Bancária' está sem o fornecedor/cliente do OMIE. O código fornecedor tem que
# estar atrelado à conta bancária."*
#
# Ele tem razão: quem cobra a tarifa é o banco DAQUELA conta. No tipo, seria
# preciso um "Tarifa Bradesco", um "Tarifa Sicredi" e um "Tarifa BB", todos
# repetindo as mesmas palavras do histórico e brigando entre si na hora de
# reconhecer o lançamento.
# ---------------------------------------------------------------------------
def test_o_fornecedor_vem_da_CONTA_e_nao_do_tipo():
    conta = dict(CONTA, omie_fornecedor=7777)
    sem_forn = [dict(TIPOS[0], codigo_cliente=None)]
    plano = co.planejar([linha()], conta, sem_forn)
    assert plano["nao_vai"] == [], plano["nao_vai"]
    assert plano["vai"][0]["codigo_cliente"] == 7777


def test_o_fornecedor_da_conta_GANHA_do_que_estiver_no_tipo():
    """Senão o dono configuraria a conta, veria o tipo antigo continuar
    mandando, e concluiria que o campo novo não faz nada."""
    conta = dict(CONTA, omie_fornecedor=7777)
    plano = co.planejar([linha()], conta, TIPOS)     # o tipo tem 111
    assert plano["vai"][0]["codigo_cliente"] == 7777


def test_sem_fornecedor_na_conta_vale_o_do_TIPO():
    """A reserva existe para nada que já estava configurado parar de funcionar
    quando isto mudou — e para um cobrador que não seja o banco."""
    plano = co.planejar([linha()], CONTA, TIPOS)     # a conta não tem
    assert plano["vai"][0]["codigo_cliente"] == 111


def test_sem_fornecedor_em_LUGAR_NENHUM_a_recusa_manda_para_a_conta():
    """⚠️ ESTA FRASE É O CONSERTO DO QUE ELE VIU. A antiga mandava cadastrar no
    TIPO, que é o lugar errado — e ele teria de criar um tipo por banco."""
    sem_forn = [dict(TIPOS[0], codigo_cliente=None)]
    plano = co.planejar([linha()], CONTA, sem_forn)
    motivo = plano["nao_vai"][0]["motivo"]
    assert "conta" in motivo and "BD 7011" in motivo
    assert "Contas" in motivo, "a frase tem de dizer ONDE resolver"
    assert "tipo" not in motivo.lower(), (
        "a recusa não pode mais mandar cadastrar o fornecedor no tipo")


def test_zero_nao_conta_como_fornecedor():
    """Campo vazio no banco vira 0 ou None conforme o caminho; nenhum dos dois
    é um cadastro do OMIE, e lançar com 0 daria erro lá, não aqui."""
    conta = dict(CONTA, omie_fornecedor=0)
    plano = co.planejar([linha()], conta, TIPOS)
    assert plano["vai"][0]["codigo_cliente"] == 111


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
    """⚠️ `falhar_na_chamada` existe desde 29/09/2026: as duas pontas da
    transferência passaram a usar a MESMA ação (`IncluirLancCC`), então falhar
    "pela ação" deixou de conseguir distinguir a segunda da primeira."""

    def __init__(self, falhar_em=None, falhar_na_chamada=None):
        self.chamadas = []
        self.falhar_em = falhar_em or []
        self.falhar_na_chamada = falhar_na_chamada
        self.proximo = 100

    def _call(self, url, acao, param):
        self.chamadas.append((acao, param))
        if acao in self.falhar_em or len(self.chamadas) == self.falhar_na_chamada:
            raise RuntimeError("o OMIE recusou")
        self.proximo += 1
        return {"nCodLanc": self.proximo}


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

    # ⚠️ DUAS CHAMADAS, NÃO QUATRO — mudou em 29/09/2026. Antes cada ponta era um
    # título mais uma baixa; agora cada ponta é UM lançamento de conta corrente.
    # A baixa sumiu porque não havia título em aberto para consumir — e era ela
    # que respondia 404. Ver `montar_lancamento_cc`.
    acoes = [a for a, _ in cli.chamadas]
    assert acoes == ["IncluirLancCC", "IncluirLancCC"]

    saida = cli.chamadas[0][1]
    entrada = cli.chamadas[1][1]
    assert saida["cabecalho"]["nCodCC"] == 9999      # origem
    assert entrada["cabecalho"]["nCodCC"] == 8888    # destino
    assert (saida["cabecalho"]["nValorLanc"]
            == entrada["cabecalho"]["nValorLanc"] == 30000.0)
    # As DUAS pontas marcadas como transferência: é assim que o OMIE as tira do
    # resultado em vez de contar como despesa e receita.
    assert saida["detalhes"]["cTipo"] == entrada["detalhes"]["cTipo"] == "TRA"
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

    saida = cli.chamadas[0][1]["cCodIntLanc"]
    entrada = cli.chamadas[1][1]["cCodIntLanc"]
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
    # A segunda ponta é a SEGUNDA chamada de `IncluirLancCC` — as duas usam a
    # mesma ação agora, então o dublê falha pela ordem.
    cli = ClienteQueAnota(falhar_na_chamada=2)

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

    # UMA chamada só: o lançamento de conta corrente não tem baixa.
    assert [a for a, _ in cli.chamadas] == ["IncluirLancCC"]
    assert cli.chamadas[0][1]["detalhes"]["cTipo"] == "DEB", "saída é débito"


# ---------------------------------------------------------------------------
# O TAMANHO DO CÓDIGO DE INTEGRAÇÃO — 25/09/2026
#
# *"O código de integração tem limite de caracteres, salvo engano. Cheque."*
#
# ⚠️ O QUE EU NÃO SEI: o teto documentado desse campo. A documentação do OMIE
# não é alcançável do ambiente onde isto foi escrito. O que se SABE, porque
# custou oito tentativas repetidas em 21/09, é o teto de 20 do
# `numero_documento` — e é por ele que os dois campos são tratados aqui.
# ---------------------------------------------------------------------------
def test_o_codigo_de_integracao_cabe_folgado_no_teto_conhecido():
    from app.apps.analisesps.conciliacao_omie import MAX_CODIGO
    # o maior número de linha que este módulo verá em muitos anos
    assert len(co._codigo_de_integracao(999_999)) <= MAX_CODIGO
    assert len(co._codigo_de_integracao(999_999)) == 10


def test_a_SEGUNDA_PONTA_da_transferencia_tambem_cabe():
    """Ela é o código mais um "D". Se estourasse só nela, a transferência
    ficaria pela metade — o pior estado possível aqui."""
    from app.apps.analisesps.conciliacao_omie import MAX_CODIGO
    assert len(co._codigo_de_integracao(999_999) + "D") <= MAX_CODIGO


def test_linha_com_numero_absurdo_e_recusada_com_motivo_em_vez_de_estourar():
    """Nunca deve acontecer — o número teria de passar de quinze dígitos. Mas
    se acontecer, é uma linha recusada com frase, e não um erro do OMIE no meio
    do lote (nem meia transferência)."""
    plano = co.planejar([linha(id_=10 ** 18)], CONTA, TIPOS)
    assert plano["vai"] == []
    assert "grande demais" in plano["nao_vai"][0]["motivo"]


def test_o_numero_do_documento_continua_cortado_em_20():
    """O teto que se conhece de verdade, e o que custou as oito tentativas."""
    from app.apps.analisesps.conciliacao_omie import MAX_CODIGO
    item = co.planejar([linha(documento="1" * 60)], CONTA, TIPOS)["vai"][0]
    assert len(co.montar_inclusao(item)["numero_documento"]) == MAX_CODIGO


# ---------------------------------------------------------------------------
# O LANÇAMENTO DE CONTA CORRENTE — 29/09/2026
#
# O dono olhou o resultado de uma tarifa e achou o erro de conceito:
#
#   *"Acho que você criou uma conta a pagar para a tarifa, e não um lançamento de
#   conta corrente."*
#   *"Esse lançamento acho que não precisa de baixa, e ainda assim, acho que isso
#   não existe: https://app.omie.com.br/api/v1/financas/contapagarbaixa/"*
#
# Ele estava certo nas duas. Uma linha do extrato é dinheiro que JÁ se moveu; o
# título é compromisso a vencer. E a baixa respondia 404, deixando título criado,
# baixa falhando e um recado mandando ele terminar o serviço na mão.
# ---------------------------------------------------------------------------
def test_a_tarifa_vira_LANCAMENTO_DE_CONTA_CORRENTE_e_nao_titulo(monkeypatch):
    """A forma é a do blueprint do Make dele: cabeçalho, detalhes e
    departamentos."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha()], CONTA, TIPOS)
    cli = ClienteQueAnota()
    co.lancar(plano["vai"], "T", cli)

    acao, param = cli.chamadas[0]
    assert acao == "IncluirLancCC"
    assert set(param) >= {"cCodIntLanc", "cabecalho", "detalhes"}
    assert set(param["cabecalho"]) == {"nCodCC", "dDtLanc", "nValorLanc"}
    assert set(param["detalhes"]) == {"cCodCateg", "cTipo", "nCodCliente",
                                      "cObs"}
    # Nada de campo de título aqui: campo sobrando faz a chamada inteira falhar.
    assert "valor_documento" not in param
    assert "data_vencimento" not in param


def test_NAO_existe_mais_chamada_de_BAIXA(monkeypatch):
    """⚠️ A baixa era a chamada que respondia 404. Sem título em aberto, não há o
    que baixar — e o dono para de receber "dê a baixa por lá"."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha()], CONTA, TIPOS)
    cli = ClienteQueAnota()
    feito = co.lancar(plano["vai"], "T", cli)

    acoes = [a for a, _ in cli.chamadas]
    assert "LancarPagamento" not in acoes
    assert "LancarRecebimento" not in acoes
    assert feito["falhas"] == []


def test_o_valor_vai_POSITIVO_e_quem_diz_a_direcao_e_o_cTipo(monkeypatch):
    """⚠️ Regra do OMIE e do Make dele (o blueprint tira o sinal com `replace`).
    Mandar negativo com `cTipo` DEB debitaria o sinal duas vezes."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)

    saida = co.planejar([linha(valor="-35.50")], CONTA, TIPOS)["vai"][0]
    param = co.montar_lancamento_cc(saida)
    assert param["cabecalho"]["nValorLanc"] == 35.5
    assert param["detalhes"]["cTipo"] == "DEB"

    entrada = co.planejar([linha(valor="35.50")], CONTA, TIPOS)["vai"][0]
    param = co.montar_lancamento_cc(entrada)
    assert param["cabecalho"]["nValorLanc"] == 35.5
    assert param["detalhes"]["cTipo"] == "CRE"


def test_o_departamento_vai_em_VALOR_e_nao_em_percentual():
    """⚠️ É a diferença para o título (`distribuicao`/`nPerDep`), e está no
    blueprint dele: `nValDep` com o valor do lançamento."""
    import datetime as dt

    param = co.montar_lancamento_cc({
        "codigo_integracao": "CONC1", "id_conta_corrente": 9999,
        "data": dt.date(2026, 9, 28), "valor": 0.35,
        "codigo_categoria": "1.01.02", "codigo_cliente": 77,
        "descricao": "TARIFA  BANCARIA", "cod_departamento": "DEP1",
        "sentido": "pagar", "transferencia": False})
    assert param["departamentos"] == [{"cCodDep": "DEP1", "nValDep": 0.35}]
    assert "distribuicao" not in param


def test_sem_departamento_o_campo_nao_vai(monkeypatch):
    """Campo vazio sobrando faz a chamada inteira falhar, e o OMIE não diz qual
    foi o culpado."""
    import datetime as dt

    param = co.montar_lancamento_cc({
        "codigo_integracao": "CONC1", "id_conta_corrente": 9999,
        "data": dt.date(2026, 9, 28), "valor": 0.35,
        "codigo_categoria": "1.01.02", "codigo_cliente": 77,
        "descricao": "TARIFA", "cod_departamento": "",
        "sentido": "pagar", "transferencia": False})
    assert "departamentos" not in param


def test_o_numero_do_lancamento_e_lido_de_nCodLanc(monkeypatch):
    """⚠️ Sem ler o número, a linha ficaria marcada como "aceitou mas não sei o
    número" — e ninguém saberia se pode mandar de novo."""
    monkeypatch.setattr(co, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(co, "_pronto", lambda: True)
    plano = co.planejar([linha()], CONTA, TIPOS)

    class SoNCodLanc:
        chamadas = []

        def _call(self, url, acao, param):
            return {"nCodLanc": 7788}

    feito = co.lancar(plano["vai"], "T", SoNCodLanc())
    assert feito["feitos"][0]["codigo"] == 7788
