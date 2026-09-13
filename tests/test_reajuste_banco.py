"""O reajuste da medição e a tabela dos índices — com banco de verdade.

Pedido do dono em 09/09/2026: *"dentro do cadastro do contrato a gente precisa
fazer alguma configuração que permita prever o recebimento de reajustes."* E,
sobre a tabela: *"já coloque aí dentro da programação do sistema ele fazer essa
busca, atualizar a tabela e permitir todos esses cálculos."*

O que se prova:

  1. A data-base é CAMPO, não regra: orçamento ou proposta, muda por contrato.
     E o contrato herda da obra quando ele mesmo não diz.
  2. O direito nasce doze meses depois da data-base — antes disso o sistema
     recusa e diz quantos meses faltam.
  3. O acumulado começa no mês SEGUINTE à data-base: incluir o mês dela
     cobraria um mês a mais.
  4. Mês faltando na tabela NÃO vira número menor: vira recusa dizendo quais
     meses faltam. Um valor calculado com metade da série passaria despercebido.
  5. A previsão vira título com valor EDITÁVEL, correlacionado à medição de
     origem, e não pode ser gerada duas vezes.
  6. A coleta do Banco Central não sobrescreve o que foi lançado à mão.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.indices import bcb
from app.apps.erp.core.indices import reajuste as svc
from app.apps.erp.core.titulos import medicao as svc_med
from app.apps.erp.core.titulos import quadro as svc_quadro
from app.apps.erp.db.models.cadastros import (Categoria, Contrato, Fornecedor,
                                              IndiceEconomico, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               Rateio, StatusTitulo,
                                               TipoTitulo, Titulo)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    svc_med.aplicar_tipos(s)
    u = Usuario(nome="Financeiro", email="reajuste@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    cliente = Fornecedor(razao_social="Prefeitura Exemplo", cnpj_cpf="11111111000191",
                         tipo_pessoa=TipoPessoa.PJ, ativo=True, e_cliente=True,
                         regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    cat = Categoria(codigo="1.1.01", descricao="Receita de obras")
    s.add_all([u, cliente, obra, cat])
    s.flush()
    contrato = Contrato(fornecedor_id=cliente.id, obra_id=obra.id, tipo="OBRA",
                        objeto="Construção da Escola Planalto",
                        valor_total=Decimal("1000000.00"),
                        vigencia_inicio=date(2025, 1, 1), status="VIGENTE",
                        indice_reajuste="INCC-DI",
                        data_base=date(2025, 1, 15),
                        data_base_origem="PROPOSTA")
    s.add(contrato)
    s.flush()
    return {"s": s, "usuario": u, "cliente": cliente, "obra": obra,
            "categoria": cat, "contrato": contrato}


def _indices(cenario, *, de: date, ate: date, pct: str = "1.00",
             codigo: str = "INCC-DI", fonte: str = "BCB-SGS"):
    """Preenche a tabela com uma variação igual em todo mês do intervalo."""
    s = cenario["s"]
    atual = de.replace(day=1)
    while atual <= ate.replace(day=1):
        s.add(IndiceEconomico(codigo=codigo, competencia=atual,
                              variacao_pct=Decimal(pct), fonte=fonte))
        atual = (atual.replace(year=atual.year + 1, month=1) if atual.month == 12
                 else atual.replace(month=atual.month + 1))
    s.flush()


def _medicao(cenario, numero="1", valor="100000.00", fim=date(2026, 3, 31)):
    s = cenario["s"]
    t = Titulo(numero_sp=f"REC-{numero}", tipo=TipoTitulo.T2_SERVICO_NFSE,
               especie=EspecieTitulo.RECEBER, fornecedor_id=cenario["cliente"].id,
               cliente_id=cenario["cliente"].id,
               descricao=f"Medição {numero}", valor_bruto=Decimal(valor),
               valor_liquido=Decimal(valor), competencia=fim.replace(day=1),
               categoria_id=cenario["categoria"].id,
               forma_pagamento=FormaPagamento.TED, status=StatusTitulo.APROVADO,
               solicitante_id=cenario["usuario"].id,
               contrato_id=cenario["contrato"].id, numero_medicao=numero,
               periodo_inicio=fim.replace(day=1), periodo_fim=fim)
    s.add(t)
    s.flush()
    s.add(Rateio(titulo_id=t.id, obra_id=cenario["obra"].id,
                 valor=Decimal(valor), percentual=Decimal("100")))
    s.flush()
    return t


# ---------------------------------------------------------------------------
# 1. A data-base é campo, e herda da obra
# ---------------------------------------------------------------------------
def test_a_data_base_vem_do_contrato_com_a_origem_escrita(cenario):
    """Quando o órgão contestar, a primeira pergunta é "essa data é do
    orçamento ou da proposta?"."""
    cfg = svc.configuracao(cenario["s"], cenario["contrato"])
    assert cfg["data_base"] == date(2025, 1, 15)
    assert cfg["data_base_origem"] == "PROPOSTA"
    assert cfg["veio_de"] == "contrato"
    assert cfg["meses"] == 12


def test_o_contrato_sem_data_base_herda_a_da_obra(cenario):
    """Sem herança, todo contrato antigo apareceria como não configurado mesmo
    com o dado no sistema."""
    s = cenario["s"]
    cenario["contrato"].data_base = None
    cenario["obra"].data_base_orcamento = date(2024, 6, 1)
    cenario["obra"].indice_reajuste = "INCC"
    s.flush()
    cfg = svc.configuracao(s, cenario["contrato"])
    assert cfg["data_base"] == date(2024, 6, 1)
    assert cfg["veio_de"] == "obra"


def test_incc_escrito_solto_vira_a_serie_certa(cenario):
    """"INCC" é o que se escreve no contrato; a série é a do INCC-DI. Sem
    traduzir, o cadastro não bateria com nenhuma linha da tabela e o reajuste
    sumiria sem explicação."""
    s = cenario["s"]
    cenario["contrato"].indice_reajuste = "INCC"
    s.flush()
    assert svc.configuracao(s, cenario["contrato"])["indice"] == "INCC-DI"


def test_contrato_sem_data_base_recusa_com_o_conserto(cenario):
    s = cenario["s"]
    cenario["contrato"].data_base = None
    s.flush()
    p = svc.prever(s, _medicao(cenario).id)
    assert p["pode"] is False
    assert "data-base" in p["motivo"]
    assert "orçamento ou a da proposta" in p["motivo"]


# ---------------------------------------------------------------------------
# 2. Doze meses
# ---------------------------------------------------------------------------
def test_antes_do_aniversario_o_sistema_recusa_e_diz_quantos_faltam(cenario):
    """"Doze meses depois você tem direito ao reajuste", palavras dele."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2025, 12, 1))
    p = svc.prever(s, _medicao(cenario, fim=date(2025, 8, 31)).id)
    assert p["pode"] is False
    assert "aniversário" in p["motivo"]
    assert "faltam 5 mês" in p["motivo"]


def test_no_aniversario_o_direito_nasce(cenario):
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1))
    p = svc.prever(s, _medicao(cenario, fim=date(2026, 1, 31)).id)
    assert p["pode"] is True
    assert p["meses_desde_data_base"] == 12


def test_a_periodicidade_e_configuravel(cenario):
    s = cenario["s"]
    cenario["contrato"].reajuste_meses = 6
    s.flush()
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1))
    p = svc.prever(s, _medicao(cenario, fim=date(2025, 8, 31)).id)
    assert p["pode"] is True, "com 6 meses de periodicidade, agosto já tem direito"


# ---------------------------------------------------------------------------
# 3 e 4. O acumulado
# ---------------------------------------------------------------------------
def test_o_acumulado_comeca_no_mes_seguinte_a_data_base(cenario):
    """A data-base é o ponto zero: o mês dela já está no preço contratado.
    Incluí-lo cobraria um mês a mais."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 1, 1), pct="1.00")
    acc = svc.acumulado(s, indice="INCC-DI", de=date(2025, 1, 15), ate=date(2026, 1, 31))
    assert acc["meses"] == 12, "fevereiro a janeiro, não janeiro a janeiro"
    assert acc["de"] == "2025-02-01"


def test_o_fator_e_o_produto_das_variacoes(cenario):
    """1% ao mês por 12 meses dá 12,68%, não 12% — juros compostos."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 1, 1), pct="1.00")
    acc = svc.acumulado(s, indice="INCC-DI", de=date(2025, 1, 15), ate=date(2026, 1, 31))
    assert round(acc["variacao_pct"], 2) == 12.68


def test_mes_faltando_recusa_em_vez_de_calcular_menor(cenario):
    """Um valor calculado com metade da série passaria despercebido — e é
    justamente o erro que ninguém confere."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 2, 1), ate=date(2025, 10, 1))   # faltam nov/dez/jan
    p = svc.prever(s, _medicao(cenario, fim=date(2026, 1, 31)).id)
    assert p["pode"] is False
    assert "Faltam 3 mês" in p["motivo"]
    assert "2025-11-01" in p["motivo"]
    assert "boletim da FGV" in p["motivo"]


def test_a_conta_sai_escrita_por_extenso_e_em_portugues(cenario):
    """É o que se manda para o órgão quando ele pergunta de onde saiu o
    número — então sai como uma pessoa lê, não em formato americano."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    p = svc.prever(s, _medicao(cenario, valor="100000.00", fim=date(2026, 1, 31)).id)
    assert "INCC-DI acumulado de 02/2025 a 01/2026 (12 meses)" in p["explicacao"]
    assert "12,6825%" in p["explicacao"]
    assert "100.000,00" in p["explicacao"] and "12.682,50" in p["explicacao"]
    assert "100000.00" not in p["explicacao"]
    assert p["valor"] == 12682.50, "100.000 × 12,6825%"


# ---------------------------------------------------------------------------
# 5. A previsão virando título
# ---------------------------------------------------------------------------
def test_a_previsao_vira_titulo_correlacionado(cenario):
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    origem = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    novo = svc.gerar_titulo(s, origem.id, usuario=cenario["usuario"])

    assert novo.numero_medicao == "1R"
    assert novo.medicao_de_id == origem.id
    assert novo.medicao_tipo == "REAJUSTE"
    assert novo.valor_liquido == Decimal("12682.50")
    assert novo.reajuste_indice == "INCC-DI"
    assert novo.reajuste_data_base == date(2025, 1, 15)


def test_o_valor_e_editavel_e_a_diferenca_fica_visivel(cenario):
    """"Pode ser que o órgão tenha algum entendimento e mude algum centavo."
    O sistema estima; quem fecha é o órgão — e a diferença tem de aparecer."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    origem = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    novo = svc.gerar_titulo(s, origem.id, valor="12500.00", usuario=cenario["usuario"])

    assert novo.valor_liquido == Decimal("12500.00"), "vale o que o órgão aprovou"
    assert novo.reajuste_previsto == Decimal("12682.50"), "e o previsto continua guardado"


def test_o_reajuste_nao_pode_ser_gerado_duas_vezes(cenario):
    """Dois cliques gerariam dois títulos, e o contrato cobraria em dobro."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    origem = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    svc.gerar_titulo(s, origem.id, usuario=cenario["usuario"])
    with pytest.raises(ErroValidacao) as e:
        svc.gerar_titulo(s, origem.id, usuario=cenario["usuario"])
    assert "já foi gerado" in str(e.value)


def test_o_numero_da_medicao_do_reajuste_e_livre(cenario):
    """Órgão que numera em sequência chama o reajuste de "medição 3"."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    origem = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    novo = svc.gerar_titulo(s, origem.id, numero_medicao="3",
                            usuario=cenario["usuario"])
    assert novo.numero_medicao == "3"
    assert novo.medicao_de_id == origem.id


def test_o_reajuste_gerado_nao_consome_saldo_do_contrato(cenario):
    """Fecha o círculo com o quadro: reajuste é acréscimo por índice, não obra
    executada a mais."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    origem = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    svc.gerar_titulo(s, origem.id, usuario=cenario["usuario"])

    tot = svc_quadro.quadro(s, cenario["contrato"].id)["totais"]
    assert tot["medido"] == 112682.50
    assert tot["reajuste"] == 12682.50
    assert tot["saldo"] == 900000.00, "o saldo desconta só a medição de obra"


def test_o_reajuste_nao_se_reajusta(cenario):
    """Seria juros sobre juros, e o órgão não paga isso."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    origem = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    svc.gerar_titulo(s, origem.id, usuario=cenario["usuario"])

    p = svc.previsao_do_contrato(s, cenario["contrato"].id)
    assert [l["numero_sp"] for l in p["medicoes"]] == ["REC-1"]


def test_a_previsao_do_contrato_soma_so_o_que_falta_gerar(cenario):
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2026, 3, 1), pct="1.00")
    um = _medicao(cenario, "1", "100000.00", fim=date(2026, 1, 31))
    _medicao(cenario, "2", "100000.00", fim=date(2026, 2, 28))

    antes = svc.previsao_do_contrato(s, cenario["contrato"].id)
    assert antes["medicoes_a_gerar"] == ["1", "2"]

    svc.gerar_titulo(s, um.id, usuario=cenario["usuario"])
    depois = svc.previsao_do_contrato(s, cenario["contrato"].id)
    assert depois["medicoes_a_gerar"] == ["2"], "o que já virou título não conta de novo"
    assert depois["a_gerar"] < antes["a_gerar"]


# ---------------------------------------------------------------------------
# 6. A tabela dos índices
# ---------------------------------------------------------------------------
def test_a_coleta_grava_o_que_veio_do_banco_central(cenario, monkeypatch):
    """Não há como bater no Banco Central dentro da suíte — nem seria
    desejável. A busca é isolada justamente para o dublê entrar aqui."""
    s = cenario["s"]
    monkeypatch.setattr(bcb, "_buscar", lambda serie, desde: [
        {"data": "01/01/2026", "valor": "0.34"},
        {"data": "01/02/2026", "valor": "0.51"}])
    r = bcb.atualizar(s, "INCC-DI", desde=date(2026, 1, 1))
    assert r["novos"] == 2
    assert r["serie"] == 192, "INCC-DI é a série 192 do SGS"
    guardado = s.get(IndiceEconomico, ("INCC-DI", date(2026, 2, 1)))
    assert guardado.variacao_pct == Decimal("0.510000")
    assert guardado.fonte == "BCB-SGS"


def test_a_coleta_nao_sobrescreve_o_que_foi_lancado_a_mao(cenario, monkeypatch):
    """Quem digitou o número do boletim tinha um motivo; a coleta passando por
    cima apagaria a correção sem avisar ninguém."""
    s = cenario["s"]
    bcb.lancar_manual(s, codigo="INCC-DI", competencia=date(2026, 1, 1),
                      variacao_pct="0.40", usuario=cenario["usuario"])
    monkeypatch.setattr(bcb, "_buscar", lambda serie, desde: [
        {"data": "01/01/2026", "valor": "0.34"}])
    r = bcb.atualizar(s, "INCC-DI", desde=date(2026, 1, 1))
    assert r["preservados_manuais"] == 1
    assert s.get(IndiceEconomico, ("INCC-DI", date(2026, 1, 1))).variacao_pct \
        == Decimal("0.400000")


def test_a_revisao_do_banco_central_vale_e_fica_registrada(cenario, monkeypatch):
    s = cenario["s"]
    monkeypatch.setattr(bcb, "_buscar", lambda serie, desde: [
        {"data": "01/01/2026", "valor": "0.34"}])
    bcb.atualizar(s, "INCC-DI", desde=date(2026, 1, 1))
    monkeypatch.setattr(bcb, "_buscar", lambda serie, desde: [
        {"data": "01/01/2026", "valor": "0.39"}])
    r = bcb.atualizar(s, "INCC-DI", desde=date(2026, 1, 1))
    assert r["atualizados"] == 1
    assert s.get(IndiceEconomico, ("INCC-DI", date(2026, 1, 1))).variacao_pct \
        == Decimal("0.390000")


def test_o_banco_central_fora_do_ar_nao_derruba_nada(cenario, monkeypatch):
    """A tabela continua como está e o sistema diz o que houve, em português."""
    s = cenario["s"]
    _indices(cenario, de=date(2025, 1, 1), ate=date(2025, 3, 1))
    def explode(serie, desde):
        raise ConnectionError("timeout")
    monkeypatch.setattr(bcb, "_buscar", explode)
    with pytest.raises(ErroValidacao) as e:
        bcb.atualizar(s, "INCC-DI")
    assert "continua com o que já tinha" in str(e.value)
    assert s.get(IndiceEconomico, ("INCC-DI", date(2025, 2, 1))) is not None


def test_variacao_absurda_lancada_a_mao_e_recusada(cenario):
    """Dígito trocado aqui contamina todo reajuste dali para a frente."""
    with pytest.raises(ErroValidacao):
        bcb.lancar_manual(cenario["s"], codigo="INCC-DI",
                          competencia=date(2026, 1, 1), variacao_pct="150")


def test_indice_de_mes_que_nao_terminou_e_recusado(cenario):
    with pytest.raises(ErroValidacao):
        bcb.lancar_manual(cenario["s"], codigo="INCC-DI",
                          competencia=date(2099, 1, 1), variacao_pct="0.5")


def test_indice_desconhecido_e_recusado(cenario):
    with pytest.raises(ErroValidacao) as e:
        bcb.atualizar(cenario["s"], "INCC-XPTO")
    assert "INCC-DI" in str(e.value), "o erro diz quais existem"
