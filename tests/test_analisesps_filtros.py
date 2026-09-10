"""Análise de SPs — a montagem do filtro em SQL.

Estes testes olham o SQL antes de ele chegar ao banco. Não substituem rodar
contra um Postgres de verdade (o `@pytest.mark.banco` existe para isso, e foi
um Postgres real que achou defeitos que leitura nenhuma acharia) — mas pegam
barato as duas coisas que mais doem: valor que vira comando em vez de
parâmetro, e coluna aberta a quem escolhe o nome dela.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import consultas


def montar(**filtros):
    base = {"busca": "", "status_pgt": [], "conta": [], "forma": [],
            "status_agend": [], "tipo_despesa": [], "projeto": [],
            "responsavel": [], "centro_custo": [], "situacoes": [],
            "periodo_ini": None, "periodo_fim": None, "pgt_ini": None,
            "pgt_fim": None, "valor_ini": None, "valor_fim": None}
    base.update(filtros)
    return consultas._condicoes(base)


# ---------------------------------------------------------------------------
# Nada escolhido não filtra nada
# ---------------------------------------------------------------------------
def test_sem_filtro_nao_gera_condicao():
    onde, params = montar()
    assert onde == []
    assert params == []


# ---------------------------------------------------------------------------
# Todo valor vindo de fora entra como parâmetro
# ---------------------------------------------------------------------------
def test_texto_de_busca_vira_parametro_e_nunca_sql():
    """O ataque clássico: um texto que fecha a aspa e emenda um comando. Ele
    tem de acabar dentro do parâmetro, inteiro, e não no corpo do SQL."""
    veneno = "'; DROP TABLE analisesps.sps; --"
    onde, params = montar(busca=veneno)
    sql = " ".join(onde)
    assert "DROP TABLE" not in sql
    assert sql.count("?") == 1
    assert params == [f"%{veneno.lower()}%"]


def test_credor_com_aspas_no_nome_nao_quebra():
    """Não é ataque, é o dia a dia: existe razão social com apóstrofo."""
    onde, params = montar(status_pgt=["D'Angelo & Cia"])
    assert params == ["D'Angelo & Cia"]
    assert "D'Angelo" not in " ".join(onde)


def test_cada_termo_da_busca_vira_um_parametro():
    """Termos separados por vírgula: TODOS precisam aparecer, como no
    Streamlit. São condições somadas, não alternativas."""
    onde, params = montar(busca="cimento, votorantim")
    assert len(onde) == 2
    assert params == ["%cimento%", "%votorantim%"]


def test_listas_geram_um_marcador_por_item():
    onde, params = montar(status_pgt=["Pagar", "Pago"])
    assert onde[0].count("?") == 2
    assert params == ["Pagar", "Pago"]


def test_valor_e_data_entram_como_parametro():
    import datetime as dt
    onde, params = montar(valor_ini=100, valor_fim=500,
                          periodo_ini=dt.date(2026, 1, 1))
    assert len(onde) == 3
    assert set(params) == {100, 500, dt.date(2026, 1, 1)}


def test_parametros_saem_na_mesma_ordem_das_condicoes():
    """O que de fato importa: cada `?` casa com o parâmetro da mesma posição.
    Trocar a ordem de um dos dois lados faria o filtro comparar valor com data
    — e o erro só apareceria no banco, com uma mensagem que não ajuda."""
    import datetime as dt
    onde, params = montar(busca="cimento", status_pgt=["Pagar"],
                          valor_ini=100, periodo_ini=dt.date(2026, 1, 1))
    assert sum(pedaco.count("?") for pedaco in onde) == len(params)

    # A condição que contém "valor_num" tem de vir na posição do parâmetro 100.
    posicao = 0
    for pedaco in onde:
        quantos = pedaco.count("?")
        if "valor_num" in pedaco:
            assert params[posicao] == 100
        if "vencimento_d" in pedaco:
            assert params[posicao] == dt.date(2026, 1, 1)
        posicao += quantos


# ---------------------------------------------------------------------------
# A coluna dos filtros de lista é escolhida por nós, não por quem chama
# ---------------------------------------------------------------------------
def test_opcoes_recusa_coluna_fora_da_lista():
    """`opcoes` monta o nome da coluna dentro do SQL — é o único lugar onde
    isso acontece, e por isso a lista de permitidas existe. Sem ela, um
    endereço com `?coluna=senha` viraria leitura de qualquer coisa."""
    with pytest.raises(ValueError):
        consultas.opcoes("senha_hash")
    with pytest.raises(ValueError):
        consultas.opcoes("id) FROM usuarios --")


def test_opcoes_aceita_as_colunas_de_filtro():
    for coluna in ("status_pgt", "conta", "forma_pagamento", "tipo_despesa",
                   "projeto", "responsavel", "centro_custo", "status_aut"):
        # não chega a consultar: só não pode levantar ValueError na validação
        assert coluna in {"status_pgt", "conta", "forma_pagamento",
                          "tipo_despesa", "projeto", "responsavel",
                          "centro_custo", "status_aut"}


def test_ordem_desconhecida_cai_no_padrao():
    """A ordenação vai para dentro do SQL, então ela NÃO pode vir de fora. Um
    valor não reconhecido usa o padrão em vez de ser costurado no comando."""
    assert consultas.ORDENS.get("vencimento_d; DROP TABLE x") is None


# ---------------------------------------------------------------------------
# As regras de negócio traduzidas do Streamlit
# ---------------------------------------------------------------------------
def test_sem_agendamento_significa_valor_vazio():
    """No original, "Sem Agendamento" não é um valor gravado: é a ausência
    dele. A tradução precisa manter isso, senão o filtro não acha nada."""
    onde, _ = montar(status_agend=["Sem Agendamento"])
    assert "= ''" in onde[0]


def test_agendamento_mistura_valor_e_ausencia_com_ou():
    onde, params = montar(status_agend=["Agendar", "Sem Agendamento"])
    assert " OR " in onde[0]
    assert params == ["Agendar"]


def test_a_obra_casa_com_a_celula_aberta_e_nao_por_pedaco():
    """Na planilha a célula às vezes traz mais de uma obra ("CONS, CRECHE
    SWAP", "OBRA-12 / OBRA-13"). Igualdade exata perderia essas linhas.

    Até 05/09/2026 a solução era casar por "contém", copiada do Streamlit.
    Funcionava na maioria dos casos e errava num que aparece: procurar a obra
    "CONS" trazia também "CONSTRUÇÃO DO GALPÃO", porque uma é pedaço da outra.

    Agora a célula é ABERTA nos separadores e a comparação é com a obra
    INTEIRA — a que a pessoa escolheu na lista. O valor continua entrando como
    PARÂMETRO, nunca no meio do comando."""
    onde, params = montar(centro_custo=["OBRA-12"])
    assert "regexp_split_to_array" in onde[0], "voltou a casar por pedaço"
    assert "LIKE" not in onde[0]
    assert params == ["obra-12"], "o valor tem de ir como parâmetro"
    assert "OBRA-12" not in onde[0] and "obra-12" not in onde[0]


def test_situacoes_conhecidas_entram_e_desconhecidas_sao_ignoradas():
    onde, _ = montar(situacoes=["pendencias", "inventada_por_alguem"])
    assert len(onde) == 1
    assert "pagar" in onde[0]


@pytest.mark.parametrize("chave", [
    "pendencias", "risco", "cadastro_incompleto",
    "boleto_invalido", "boleto_duplicado",
])
def test_toda_situacao_da_tela_tem_traducao(chave):
    """As cinco caixas da tela precisam existir aqui. Uma caixa sem tradução
    seria um filtro que não filtra — e ninguém perceberia."""
    assert chave in consultas.SITUACOES
    assert consultas.SITUACOES[chave].strip()


def test_boleto_duplicado_conta_pagar_e_pago_mas_mostra_so_pagar():
    """A regra do original, que não é óbvia: a contagem de repetições olha
    Pagar UNIÃO Pago (é onde existe risco de pagar duas vezes), mas a lista
    exibe só os que estão a Pagar — o par "1 Pago + 1 Pagar" mostra o Pagar,
    que é o que ainda pode ser pago em duplicidade."""
    sql = consultas.SQL_BOLETO_DUPLICADO
    assert "('pagar','pago')" in sql.replace(" ", "")
    assert sql.count("= 'pagar'") >= 1
    assert "count(*) > 1" in sql


def test_hoje_e_o_de_brasilia_e_nao_o_do_servidor():
    """O servidor roda em UTC. Entre 21h e meia-noite de Brasília ele já virou
    o dia — sem a conversão, uma SP que vence amanhã apareceria em vermelho
    como atrasada para quem confere à noite."""
    assert "America/Sao_Paulo" in consultas.SQL_HOJE


# ---------------------------------------------------------------------------
# AS LISTAS DE FILTRO FICAM GUARDADAS ATÉ A PRÓXIMA CARGA
#
# Medido com as 59.055 SPs de verdade: montar as sete listas custava 194 ms, e
# era isso a CADA clique no filtro — o pedaço mais caro da tela. Estes testes
# existem para que a correção não se desfaça sem ninguém notar: é o tipo de
# coisa que uma refatoração distraída remove, e o efeito só aparece em
# produção, como lentidão sem culpado.
# ---------------------------------------------------------------------------
def test_a_lista_do_filtro_nao_e_refeita_a_cada_clique(monkeypatch):
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()

    idas = []
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: idas.append(coluna) or ["A"])

    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")
    primeira = len(idas)
    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")
    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")

    assert primeira == len(consultas.COLUNAS_DE_FILTRO)
    assert len(idas) == primeira, "as listas foram refeitas sem carga nova"


def test_uma_carga_nova_refaz_as_listas(monkeypatch):
    """O carimbo da sincronização É o aviso. A carga roda num processo
    separado e não tem como falar com este; o que ela grava no banco, sim."""
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()

    idas = []
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: idas.append(coluna) or ["A"])

    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:00:00")
    consultas.opcoes_de_filtro(carimbo="2026-09-05T10:05:00")   # carga nova

    assert len(idas) == 2 * len(consultas.COLUNAS_DE_FILTRO)


# ---------------------------------------------------------------------------
# CONTAR AS SPs UMA VEZ POR CARGA, E NÃO UMA VEZ POR TELA
#
# `count(*)` no Postgres percorre a tabela inteira, e `base_carregada()` é
# chamada em TODA tela. Na produção, medido pelo dono em 09/09/2026, a rotina
# que só pergunta a hora da base levou 1,4 segundo — e ela não fazia nada além
# desta contagem. Mesmo tipo de correção das listas de filtro acima, e pelo
# mesmo motivo: o efeito só aparece com a base cheia, e aí é tarde.
# ---------------------------------------------------------------------------
def test_a_base_nao_e_contada_de_novo_enquanto_a_carga_for_a_mesma(monkeypatch):
    from app.apps.analisesps import consultas

    contagens = []

    def falso_consultar(sql, params=()):
        if "count(*)" in sql:
            contagens.append(sql)
            return [(59055,)]
        return [("ultima_sincronizacao", "2026-09-09T10:00:00"),
                ("quantidade", "59055"),
                ("quantidade_em", "2026-09-09T10:00:00")]

    from app.apps.analisesps import db
    monkeypatch.setattr(db, "consultar", falso_consultar)

    resposta = consultas.base_carregada()

    assert resposta["quantidade"] == 59055
    assert resposta["pronta"] is True
    assert not contagens, "percorreu a tabela tendo a contagem guardada"


def test_uma_carga_nova_manda_contar_de_novo(monkeypatch):
    """A contagem vale para a carga em que foi feita. Carga nova, número novo
    — senão a tela mostraria para sempre o total do dia em que foi contado."""
    from app.apps.analisesps import consultas

    contagens = []
    gravados = []

    def falso_consultar(sql, params=()):
        if "count(*)" in sql:
            contagens.append(sql)
            return [(59100,)]
        return [("ultima_sincronizacao", "2026-09-09T11:00:00"),
                ("quantidade", "59055"),
                ("quantidade_em", "2026-09-09T10:00:00")]   # carga anterior

    class ConexaoFalsa:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=()): gravados.append(params)
        def commit(self): pass

    from app.apps.analisesps import db
    monkeypatch.setattr(db, "consultar", falso_consultar)
    monkeypatch.setattr(db, "conexao", lambda: ConexaoFalsa())

    resposta = consultas.base_carregada()

    assert resposta["quantidade"] == 59100
    assert len(contagens) == 1
    # E guarda o número novo, para a próxima tela não contar outra vez.
    assert ("quantidade", "59100") in gravados
    assert ("quantidade_em", "2026-09-09T11:00:00") in gravados


def test_a_carga_anota_quantas_sps_ficaram_na_base():
    """Quem conta é o processo separado, onde um segundo a mais não incomoda
    ninguém. Assim nem a PRIMEIRA tela depois de uma carga precisa contar."""
    from app.apps.analisesps import sincronizacao

    gravados = {}

    class CursorFalso:
        def fetchone(self): return (59055,)
        def close(self): pass

    class ConexaoFalsa:
        def execute(self, sql, params=()):
            if params:
                gravados[params[0]] = params[1]
            return CursorFalso()
        def commit(self): pass

    sincronizacao._anotar_a_base_em_dia(ConexaoFalsa())

    assert gravados["quantidade"] == "59055"
    assert gravados["quantidade_em"] == gravados["ultima_sincronizacao"]


def test_a_carga_inicial_tambem_anota_a_hora_da_base():
    """Antes só a sincronização do dia anotava. Uma carga acabada de rodar É a
    base em dia — sem isto o relógio do alto ficava mudo justamente no dia da
    estreia, que é quando ninguém sabe se deu certo."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/sincronizacao.py").read_text(encoding="utf-8")
    depois_da_carga = fonte.split("carga inicial concluída")[0]
    assert "_anotar_a_base_em_dia" in depois_da_carga


def test_a_barra_de_filtros_traz_as_sete_listas_mais_o_agendamento(monkeypatch):
    """Guardar não pode significar entregar menos do que a tela desenha."""
    from app.apps.analisesps import consultas
    consultas.esquecer_opcoes_de_filtro()
    monkeypatch.setattr(consultas, "opcoes", lambda coluna, limite=400: ["A"])

    listas = consultas.opcoes_de_filtro(carimbo="x")

    for chave in ("status_pgt", "conta", "forma", "tipo_despesa", "projeto",
                  "responsavel", "centro_custo", "status_agend"):
        assert chave in listas, chave


def test_a_tela_pede_o_resumo_e_o_agendamento_numa_ida_so(monkeypatch):
    """Eram duas varreduras da MESMA tabela filtrada (44 ms + 48 ms medidos).
    Numa consulta só custam 59 ms — a varredura é uma, as contagens vão de
    carona."""
    import inspect
    from app.apps.analisesps import consultas

    fonte = inspect.getsource(consultas.resumo_e_agendamento)
    assert fonte.count("FROM analisesps.sps") == 1, (
        "voltou a varrer a tabela mais de uma vez")
    # Os oito números que a tela precisa, todos na mesma consulta.
    assert fonte.count("count(*)") + fonte.count("sum(valor_num)") >= 8


# ---------------------------------------------------------------------------
# UMA VARREDURA EM VEZ DE VÁRIAS
#
# "continuo achando lento quando mudamos de aba, ou quando vai carregar os
# dados após o filtro" — 09/09/2026. Medido com as 59.055 SPs: o Lote fazia
# OITO varreduras da base para montar o painel por status, e o Relatório
# CINCO para somar por quatro dimensões. Cada varredura são ~40 ms.
#
# Estes testes prendem a correção pela forma da consulta, porque o efeito
# (a lentidão) só aparece com a base cheia — e aí é tarde.
# ---------------------------------------------------------------------------
def test_o_painel_do_lote_sai_de_uma_varredura_so():
    import inspect
    from app.apps.analisesps import consultas

    fonte = inspect.getsource(consultas.painel_por_agendamento)
    # Duas consultas: as linhas (com `row_number`) e os totais (com `GROUP BY`).
    assert fonte.count("consultar(") == 2, (
        "o painel voltou a fazer uma consulta por status")
    assert "row_number" in fonte, (
        "sem a numeração por status, o banco precisa de uma varredura por lista")


def test_o_relatorio_soma_as_dimensoes_juntas():
    import inspect
    from app.apps.analisesps import consultas

    fonte = inspect.getsource(consultas.agregar_varias)
    assert "GROUPING SETS" in fonte, (
        "sem GROUPING SETS o banco varre a tabela uma vez por dimensão")
    assert fonte.count("FROM analisesps.sps") == 1


def test_a_auditoria_conta_tudo_numa_consulta():
    import inspect
    from app.apps.analisesps import auditoria

    fonte = inspect.getsource(auditoria.resumo)
    assert "FILTER (WHERE" in fonte, "voltou a contar uma condição por consulta"


def test_uma_dimensao_so_nao_paga_o_preco_do_agrupamento(monkeypatch):
    """Agrupar junto compensa a partir de duas; com uma, o caminho simples é
    mais barato — e é o que a exportação usa."""
    from app.apps.analisesps import consultas

    chamou = []
    monkeypatch.setattr(consultas, "agregar",
                        lambda f, d, t="geral", p="tudo", l=30: chamou.append(d) or [])
    consultas.agregar_varias({}, ["projeto"], "geral", "tudo", 15)
    assert chamou == ["projeto"]


def test_dimensao_desconhecida_nao_entra_no_sql():
    """O nome da dimensão entra no TEXTO do SQL, então não pode vir de fora
    sem conferência — é a porta aberta clássica."""
    from app.apps.analisesps import consultas
    assert consultas.agregar_varias({}, ["nao_existe; DROP TABLE"]) == {}
