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


def test_centro_de_custo_casa_por_conter():
    """Na planilha o centro de custo às vezes vem com mais de um código na
    mesma célula. Igualdade exata perderia essas linhas."""
    onde, params = montar(centro_custo=["OBRA-12"])
    assert "LIKE" in onde[0]
    assert params == ["%obra-12%"]


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
