# -*- coding: utf-8 -*-
"""
A GESTÃO DA FOLHA DA CONTABILIDADE: o que a tela mostra e o que ela recorta.

⚠️ POR QUE ESTES TESTES EXISTEM. Em 29/09/2026 o dono abriu a folha importada e
não tinha gestão nenhuma sobre ela: *"Cadê os dados deles, cadê uma tabela
mostrando as informações, cadê a possibilidade de seleção de quem entra e quem não
entra, cadê onde gera o arquivo de pagamento?"*

A conta existia inteira e testada (`test_analisesps_folha_apropriacao.py`); o que
faltava era a tela. Estes casos travam as decisões dela: qual situação vale quando
duas se aplicam, como a obra aparece numa célula, e o que cada filtro recorta.

⚠️ E TRAVAM OS NOMES DOS CAMPOS DO PONTO. Eles foram lidos do programa que o dono
mandou, não supostos — e um rename silencioso neles esvaziaria a apropriação de
~500 pessoas sem nenhum erro na tela. Um teste que os afirma faz isso quebrar aqui.
"""
from decimal import Decimal

from app.apps.analisesps import folha_gestao as fg
from app.apps.analisesps import ponto


# ---------------------------------------------------------------------------
# OS NOMES DOS CAMPOS DO PONTO
# ---------------------------------------------------------------------------
def test_os_campos_do_dia_sao_os_do_programa_que_ja_roda():
    """⚠️ Lidos do `analysis_engine.py` que o dono mandou em 29/09/2026, não
    supostos. Se mudarem, é decisão — e decisão passa por aqui."""
    assert ponto.CAMPOS_DAS_HORAS == ("hr_entrada", "hr_almoco", "hr_retorno",
                                      "hr_saida")
    assert ponto.CAMPOS_DAS_OBRAS == ("obra_entrada", "obra_almoco",
                                      "obra_retorno", "obra_saida")
    assert ponto.CAMPO_PRESENCA == "presenca_ausencia"
    assert ponto.CAMPO_FALTA == "desc_falta"


def test_a_ordem_das_quatro_obras_e_a_ordem_do_DIA():
    """⚠️ A ORDEM É REGRA DE NEGÓCIO. No empate 2x2 vale a obra em que o dia
    COMEÇOU (decisão do dono); ordenar a lista faria o desempate virar sorteio
    alfabético, e ninguém notaria."""
    campos = {"obra_entrada": "ZZZ", "obra_almoco": "AAA",
              "obra_retorno": "AAA", "obra_saida": "ZZZ"}
    assert ponto.obras_do_dia(campos) == ["ZZZ", "AAA", "AAA", "ZZZ"]

    from app.apps.analisesps.folha_apropriacao import obra_do_dia
    decidido = obra_do_dia(ponto.obras_do_dia(campos), "Presença", "")
    assert decidido["obra"] == "ZZZ", "no empate vale a obra da primeira marcação"
    assert decidido["empate"] is True


def test_dia_sem_marcacao_devolve_QUATRO_posicoes_vazias():
    """`obra_do_dia` conta as posições preenchidas: uma lista curta mudaria a
    contagem sem avisar."""
    assert ponto.obras_do_dia({}) == ["", "", "", ""]
    assert ponto.obras_do_dia(None) == ["", "", "", ""]
    assert len(ponto.horas_do_dia({"hr_entrada": "07:00"})) == 4


def test_a_obra_sai_em_MAIUSCULA_e_sem_espaco_sobrando():
    """A obra é chave: " crepeolinda " e "CREPEOLINDA" têm de ser a mesma obra,
    senão o total por obra sai partido em duas linhas parecidas."""
    assert ponto.obras_do_dia(
        {"obra_entrada": "  crepeolinda  "})[0] == "CREPEOLINDA"


# ---------------------------------------------------------------------------
# A OBRA NUMA CÉLULA
# ---------------------------------------------------------------------------
def test_o_resumo_das_obras_mostra_DIAS_e_nao_percentual():
    """⚠️ Ele trabalha por dia: "10 dias na obra tal" se confere contra o ponto;
    "83,33%" não se confere contra nada."""
    resumo = fg.resumo_das_obras([
        {"obra": "AAA", "dias": 10, "valor": Decimal("1000.00")},
        {"obra": "BBB", "dias": 2, "valor": Decimal("200.00")}])
    assert resumo == "AAA · 10d + BBB · 2d"
    assert "%" not in resumo


def test_sem_dias_o_resumo_mostra_so_a_obra():
    """Quem tem regra de rateio não tem dia nenhum — e "AAA · 0d" diria que ela
    não trabalhou, o que é falso."""
    assert fg.resumo_das_obras([{"obra": "AAA", "dias": 0,
                                 "valor": Decimal("10")}]) == "AAA"


def test_a_obra_principal_e_a_de_MAIS_DIAS():
    assert fg.obra_principal([
        {"obra": "AAA", "dias": 2, "valor": Decimal("200")},
        {"obra": "BBB", "dias": 9, "valor": Decimal("900")}]) == "BBB"


def test_empatados_os_dias_desempata_o_VALOR():
    assert fg.obra_principal([
        {"obra": "AAA", "dias": 5, "valor": Decimal("100")},
        {"obra": "BBB", "dias": 5, "valor": Decimal("900")}]) == "BBB"


def test_sem_apropriacao_nao_ha_obra_principal():
    assert fg.obra_principal([]) == ""


# ---------------------------------------------------------------------------
# O TOTAL POR OBRA
# ---------------------------------------------------------------------------
def test_o_percentual_por_obra_fecha_CEM_e_sai_do_mesmo_lugar_do_card():
    """⚠️ O percentual vem de `folha_geracao.percentuais_por_obra` — o MESMO que
    vai para o card do Pipefy e para o arquivo de análise. Duas contas de
    percentual divergiriam no centavo, e a tela diria uma coisa e o card outra."""
    saida = fg.totais_por_obra([
        {"obra": "AAA", "valor": Decimal("1000.00"), "pessoas": 3,
         "origens": ["ponto"]},
        {"obra": "BBB", "valor": Decimal("500.00"), "pessoas": 1,
         "origens": ["regra"]}])
    assert sum(o["percentual"] for o in saida) == Decimal("100")
    assert [o["obra"] for o in saida] == ["AAA", "BBB"], "da maior para a menor"
    assert saida[0]["pessoas"] == 3
    assert saida[0]["origens"] == ["ponto"]


def test_obra_sem_valor_nao_inventa_percentual():
    """Obra com zero não entra no rateio: 0% num relatório de rateio é linha que
    alguém vai procurar para entender, e não há nada para entender."""
    saida = fg.totais_por_obra([{"obra": "AAA", "valor": Decimal("0.00"),
                                 "pessoas": 0, "origens": []}])
    assert saida[0]["percentual"] is None


# ---------------------------------------------------------------------------
# A SITUAÇÃO DE UMA LINHA: uma só, e a ordem importa
# ---------------------------------------------------------------------------
def _pessoa(**mudancas):
    base = {"cpf": "99713349334", "pendente_cadastro": False, "fora": False,
            "por_obra": [{"obra": "AAA", "dias": 10,
                          "valor": Decimal("1000.00")}],
            "por_dia": [{"data": None, "obra": "AAA", "empate": False}]}
    base.update(mudancas)
    return base


def test_quem_nao_tem_cadastro_vem_ANTES_de_tudo():
    """Sem CPF não há como pagar nem como guardar decisão: é o mais grave, e tem
    de ser a situação mostrada mesmo que a pessoa também esteja sem obra."""
    assert fg.situacao_da_pessoa(
        _pessoa(cpf="", pendente_cadastro=True, por_obra=[]),
        {}) == fg.SEM_CADASTRO


def test_tirada_do_pagamento_vence_a_falta_de_obra():
    """⚠️ Quem ele TIROU não precisa de obra — cobrar obra de quem não vai receber
    faria a folha nunca fechar por causa de gente que não está nela."""
    assert fg.situacao_da_pessoa(
        _pessoa(fora=True, por_obra=[]), {}) == fg.FORA


def test_sem_obra_vence_quem_esta_saindo():
    """Sem obra o pagamento não sai; "está saindo" é conferência. A situação
    mostrada é a que impede."""
    from app.apps.analisesps import colaboradores
    assert fg.situacao_da_pessoa(
        _pessoa(por_obra=[]),
        {"situacao": colaboradores.SITUACAO_SAINDO}) == fg.SEM_OBRA


def test_quem_ja_saiu_aparece_como_saiu_mesmo_com_tudo_apropriado():
    """*"Não podemos pagar salário ou diárias pra quem saiu."* Estar apropriado
    não torna a pessoa pagável."""
    from app.apps.analisesps import colaboradores
    assert fg.situacao_da_pessoa(
        _pessoa(), {"situacao": colaboradores.SITUACAO_SAIU}) == fg.SAIU


def test_dia_empatado_acende_quando_nada_mais_acende():
    """Pessoa em duas obras no mesmo dia é, na maioria dos casos, erro de batida —
    mas não impede pagar, então é a última situação a aparecer."""
    assert fg.situacao_da_pessoa(
        _pessoa(por_dia=[{"data": None, "obra": "AAA", "empate": True}]),
        {}) == fg.EMPATE


def test_o_normal_e_VAI_RECEBER_e_o_selo_e_verde():
    """⚠️ O selo `pagar` é VERMELHO no padrão do módulo (pendente/urgente). Já usei
    errado uma vez: quem vai receber é `aprovado`, verde."""
    assert fg.situacao_da_pessoa(_pessoa(), {}) == fg.PAGA
    assert fg.SELO_DA_SITUACAO[fg.PAGA] == "aprovado"
    assert fg.SELO_DA_SITUACAO[fg.PAGA] != "pagar"


def test_toda_situacao_tem_rotulo_em_portugues_e_um_selo():
    """Situação sem rótulo apareceria na tela com o nome técnico; sem selo,
    apareceria sem cor — e a coluna é lida pela cor."""
    for chave in fg.ORDEM_DAS_SITUACOES:
        assert fg.ROTULO_DA_SITUACAO.get(chave)
        assert fg.SELO_DA_SITUACAO.get(chave)


# ---------------------------------------------------------------------------
# OS FILTROS
# ---------------------------------------------------------------------------
def _linha(**mudancas):
    base = {
        "nome_na_tela": "GERLANIO GOMES LIMA", "nome_contabilidade": "GERLANIO",
        "cpf": "99713349334", "id_fortes": "000013", "fase": "Colaboradores ativos",
        "situacao": fg.PAGA, "origem": "ponto", "valor": Decimal("1000.00"),
        "por_obra": [{"obra": "AAA", "dias": 10, "valor": Decimal("1000.00")}],
    }
    base.update(mudancas)
    return base


def test_filtro_vazio_nao_recorta_nada():
    pessoas = [_linha(), _linha(cpf="11122233396")]
    assert len(fg._filtrar(pessoas, {})) == 2


def test_a_busca_acha_por_nome_por_CPF_e_pelo_CODIGO_do_fortes():
    """Os três jeitos que ele tem de identificar alguém. O código é o que a
    contabilidade usa, e é o único que aparece no arquivo dela."""
    pessoas = [_linha()]
    assert fg._filtrar(pessoas, {"busca": "gerlanio"})
    assert fg._filtrar(pessoas, {"busca": "997.133.493-34"})
    assert fg._filtrar(pessoas, {"busca": "000013"})
    assert not fg._filtrar(pessoas, {"busca": "luelia"})


def test_o_filtro_de_OBRA_acha_quem_tem_POUCOS_dias_nela():
    """⚠️ Casa em qualquer parte da divisão, não só na obra principal: quem tem 2
    dias numa obra também é custo dela, e quem filtra por obra quer o custo
    inteiro dela — senão o total da tela não bate com o total por obra."""
    dividida = _linha(por_obra=[
        {"obra": "AAA", "dias": 10, "valor": Decimal("900.00")},
        {"obra": "BBB", "dias": 2, "valor": Decimal("100.00")}])
    assert fg._filtrar([dividida], {"obra": "BBB"})
    assert fg._filtrar([dividida], {"obra": "bbb"}), "não é sensível a caixa"
    assert not fg._filtrar([dividida], {"obra": "CCC"})


def test_o_filtro_de_SITUACAO_recorta_de_verdade():
    """⚠️ Recorta no SERVIDOR. Esconder linha no navegador faria o subtotal do
    filtro mentir, porque ele é somado aqui."""
    pessoas = [_linha(), _linha(cpf="11122233396", situacao=fg.FORA)]
    so_fora = fg._filtrar(pessoas, {"situacao": fg.FORA})
    assert len(so_fora) == 1 and so_fora[0]["situacao"] == fg.FORA


def test_o_filtro_de_FASE_ATUAL_existe_e_e_exato():
    """Ele pediu a Fase Atual duas vezes. É exata, não por pedaço: "Colaboradores
    ativos" e "Colaboradores afastados" não podem cair no mesmo filtro."""
    pessoas = [_linha(), _linha(cpf="11122233396",
                               fase="Colaboradores afastados")]
    achados = fg._filtrar(pessoas, {"fase": "Colaboradores afastados"})
    assert len(achados) == 1
    assert achados[0]["fase"] == "Colaboradores afastados"


def test_o_filtro_de_ORIGEM_separa_o_que_veio_do_PONTO_do_que_veio_da_MAO():
    """*"Saber até de onde é que foi que veio aquela informação, se foi do ponto,
    se foi colocada de forma manual."*"""
    pessoas = [_linha(), _linha(cpf="11122233396", origem="mao")]
    assert len(fg._filtrar(pessoas, {"origem": "mao"})) == 1
    assert len(fg._filtrar(pessoas, {"origem": "ponto"})) == 1


def test_os_filtros_se_SOMAM():
    """Dois filtros juntos recortam os dois — um filtro que ignorasse o outro
    mostraria linha que ele achou que tinha excluído."""
    pessoas = [_linha(),
               _linha(cpf="11122233396", nome_na_tela="LUELIA", origem="mao")]
    assert not fg._filtrar(pessoas, {"busca": "luelia", "origem": "ponto"})
    assert fg._filtrar(pessoas, {"busca": "luelia", "origem": "mao"})


# ---------------------------------------------------------------------------
# O QUE A RELEITURA DE 29/09/2026 DEVOLVEU
#
# ⚠️ Ele mandou reler o que já havia proposto — *"não é melhor você fazer uma
# releitura do que eu já propus?"* — e a releitura de `docs/FOLHA_DE_PAGAMENTO.md`
# devolveu três colunas e uma regra que já estavam escritas e que eu não tinha
# feito. Cada teste abaixo trava uma delas.
# ---------------------------------------------------------------------------
def test_o_valor_por_dia_e_o_liquido_dividido_pelos_dias():
    """Coluna I das abas Quinzena e Fim de Mês: `F/H`. É o número com o qual o valor
    é rateado, e o que ele confere de cabeça."""
    assert fg._por_dia(Decimal("1000.00"), 20) == Decimal("50.00")
    assert fg._por_dia(Decimal("1074.64"), 11) == Decimal("97.69")


def test_sem_dia_o_valor_por_dia_e_TRACO_e_nao_zero():
    """⚠️ Zero dias é o "PT" da planilha — o caso que precisa da mão dele. "R$ 0,00
    por dia" pareceria um valor calculado; traço é pergunta aberta."""
    assert fg._por_dia(Decimal("1000.00"), 0) is None
    assert fg._por_dia(Decimal("1000.00"), None) is None


def test_a_divisao_por_dias_calcula_o_valor_e_nao_aceita_dinheiro_digitado():
    """⚠️ Nível 3 do ajuste fino: *"bota um dia numa obra, um dia em outra obra."*
    Entra DIA; o valor sai do valor por dia. Valor digitado deixaria a mesma pessoa
    com dois valores por dia diferentes no mesmo período."""
    partes = fg.dividir_por_dias(Decimal("900.00"), [
        {"obra": "AAA", "dias": 2}, {"obra": "BBB", "dias": 1}])
    assert [p["valor"] for p in partes] == [Decimal("600.00"), Decimal("300.00")]
    assert sum(p["valor"] for p in partes) == Decimal("900.00")


def test_a_sobra_do_centavo_da_divisao_fecha_o_valor_da_pessoa():
    """⚠️ Mesma regra de centavo do ponto. Duas regras diferentes fariam a tela e o
    arquivo divergirem em um real a cada quinhentas pessoas."""
    partes = fg.dividir_por_dias(Decimal("1000.00"), [
        {"obra": "AAA", "dias": 1}, {"obra": "BBB", "dias": 1},
        {"obra": "CCC", "dias": 1}])
    assert sum(p["valor"] for p in partes) == Decimal("1000.00")


def test_divisao_sem_dia_ou_com_obra_repetida_e_RECUSADA():
    """Dividir por zero dias não é divisão, é apagar o valor da pessoa. E obra
    repetida é erro de digitação — somar as duas caladamente esconderia o erro
    dentro de um total que parece certo."""
    import pytest

    with pytest.raises(fg.ErroDaGestao):
        fg.dividir_por_dias(Decimal("100.00"), [])
    with pytest.raises(fg.ErroDaGestao):
        fg.dividir_por_dias(Decimal("100.00"), [{"obra": "AAA", "dias": 0}])
    with pytest.raises(fg.ErroDaGestao):
        fg.dividir_por_dias(Decimal("100.00"), [{"obra": "AAA", "dias": 1},
                                                {"obra": "aaa", "dias": 2}])


def test_o_total_por_CONTA_CORRENTE_junta_as_obras_da_mesma_conta(monkeypatch):
    """⚠️ Estava na planilha (bloco da direita), na fórmula (coluna AF) e no desenho
    que ele aprovou (§5.2: a prévia mostra "por obra, POR CONTA CORRENTE") — e não
    estava na minha tela. Cada conta vira um arquivo e uma SP de transferência."""
    from app.apps.analisesps import folha_pagamento

    monkeypatch.setattr(folha_pagamento, "conta_por_obra",
                        lambda: {"AAA": "7011-4", "BBB": "7011-4",
                                 "CCC": "22069-8"})
    contas = fg.totais_por_conta([
        {"obra": "AAA", "valor": Decimal("100.00"), "pessoas": 1},
        {"obra": "BBB", "valor": Decimal("200.00"), "pessoas": 2},
        {"obra": "CCC", "valor": Decimal("50.00"), "pessoas": 1}])

    por_conta = {c["conta"]: c for c in contas}
    assert por_conta["7011-4"]["valor"] == Decimal("300.00")
    assert por_conta["7011-4"]["pessoas"] == 3
    assert sorted(por_conta["7011-4"]["obras"]) == ["AAA", "BBB"]
    assert por_conta["22069-8"]["valor"] == Decimal("50.00")


def test_obra_SEM_CONTA_vira_linha_propria_e_vem_PRIMEIRO(monkeypatch):
    """⚠️ NUNCA um padrão silencioso. A planilha da aba CTPS cai numa conta padrão
    quando o nome da obra não casa com nada (§7.14.4) — obra nova paga pela conta
    errada sem avisar. Aqui é crítica, e vem na frente porque é o que impede gerar."""
    from app.apps.analisesps import folha_pagamento

    monkeypatch.setattr(folha_pagamento, "conta_por_obra",
                        lambda: {"AAA": "7011-4"})
    contas = fg.totais_por_conta([
        {"obra": "AAA", "valor": Decimal("1000.00"), "pessoas": 9},
        {"obra": "NOVA", "valor": Decimal("10.00"), "pessoas": 1}])

    assert contas[0]["sem_conta"] is True, "a sem conta vem antes da maior"
    assert contas[0]["obras"] == ["NOVA"]
    assert contas[1]["conta"] == "7011-4"


def test_com_filtro_de_OBRA_o_subtotal_soma_so_os_dias_daquela_obra():
    """Quem trabalhou 2 dias na obra e 8 em outra aparece, mas só os 2 dias dela
    contam no subtotal da obra — senão a obra parece mais cara do que é."""
    from decimal import Decimal as D
    from app.apps.analisesps import folha_gestao as fg
    pessoas = [{"nome_na_tela": "A", "nome_contabilidade": "A", "cpf": "1",
                "valor": D("1000.00"),
                "por_obra": [{"obra": "CRE1", "valor": D("200.00")},
                             {"obra": "XYZ9", "valor": D("800.00")}]},
               {"nome_na_tela": "B", "nome_contabilidade": "B", "cpf": "2",
                "valor": D("500.00"),
                "por_obra": [{"obra": "XYZ9", "valor": D("500.00")}]}]
    mostradas = fg._filtrar(pessoas, {"obra": "CRE1"})
    assert [p["nome_na_tela"] for p in mostradas] == ["A"]
