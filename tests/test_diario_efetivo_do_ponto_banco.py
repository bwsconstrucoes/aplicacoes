# -*- coding: utf-8 -*-
"""
O EFETIVO QUE O DIÁRIO PEGA DO PONTO — contra um Postgres de verdade.

⚑ POR QUE ESTE ARQUIVO EXISTE: em 13/11/2026 ele olhou o diário e disse:

    *"Eu acho que ele está dobrando o efetivo. Eu acho que ele está
    contabilizando de forma errada. Talvez pela quantidade de pontos. Aí está
    dando um valor acima do que é o normal, do que é o real. E não é nem
    dobrando, não. E outra coisa, aí tá aparecendo uns horários esquisitos
    também. Tem erro aí nessa importação do ponto."*

Tinha três defeitos, e nenhum deles era alcançável pelo dublê de sessão do
`conftest.py`: todos moram no WHERE, no GROUP BY e no fuso da consulta. Por
isso aqui se sobe o schema `ponto` de verdade e se batem batidas de verdade.

Se um destes testes voltar a falhar, o número do efetivo no RDO está errado —
e RDO com efetivo errado é documento errado assinado pela fiscalização.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.banco

HOJE = dt.date(2026, 11, 10)


@pytest.fixture(scope="session")
def _schema_ponto(banco):
    """O schema `ponto` pela migração de verdade.

    NÃO derruba no fim, de propósito: outros arquivos da suíte também sobem
    este schema, e com `-n auto` o mesmo trabalhador pode rodar os dois — uma
    derrubada no teardown levaria o schema de quem ainda está usando."""
    from app.apps.ponto import migracoes_runner
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado["erro"], resultado
    return True


@pytest.fixture
def cenario(_schema_ponto, sessao_real):
    """Uma obra, três pessoas e nenhuma batida. Cada teste bate as suas."""
    s = sessao_real
    obra = s.execute(text("""
        INSERT INTO obras (codigo, nome, status)
        VALUES ('EFPONTO1', 'Obra do efetivo', 'ATIVA') RETURNING id""")).scalar()
    gente = {}
    for nome, cpf, cargo in [("Ana Pedreira", "52998224725", "Pedreira"),
                             ("Bruno Servente", "11144477735", "Servente"),
                             ("Caio Armador", "12345678909", "Armador")]:
        cid = s.execute(text("""
            INSERT INTO colaboradores (nome, cpf, situacao, obra_id)
            VALUES (:n, :c, 'ATIVO', :o) RETURNING id"""),
            {"n": nome, "c": cpf, "o": obra}).scalar()
        gente[nome] = {"id": cid, "cpf": cpf, "cargo": cargo}
    s.flush()
    return {"obra": obra, "gente": gente, "s": s}


def _bater(cenario, pessoa, hora_utc, *, status="VALIDA", dia=HOJE):
    """Uma batida crua, com a hora EM UTC — que é como o banco a guarda."""
    s = cenario["s"]
    nsr = s.execute(text(
        "SELECT COALESCE(MAX(nsr), 0) + 1 FROM ponto.marcacoes")).scalar()
    s.execute(text("""
        INSERT INTO ponto.marcacoes
            (nsr, colaborador_id, obra_id, timestamp_servidor, data_referencia,
             origem, status, hash_encadeado)
        VALUES (:n, :c, :o, :t, :d, 'PWA', :s, 'hash-de-teste-' || :n)"""),
        {"n": nsr, "c": cenario["gente"][pessoa]["id"], "o": cenario["obra"],
         "t": dt.datetime.combine(dia, hora_utc, tzinfo=dt.timezone.utc),
         "d": dia, "s": status})
    s.flush()


# ---------------------------------------------------------------------------
# 1. A HORA: o banco guarda UTC, o papel mostra Fortaleza
# ---------------------------------------------------------------------------
def test_a_hora_sai_no_fuso_da_empresa_e_nao_em_utc(cenario):
    """⚑ *"aí tá aparecendo uns horários esquisitos também."*

    Esquisitos porque vinham TRÊS HORAS adiantados. Quem bate às 7 da manhã em
    Fortaleza fica gravado como 10:00 UTC; o diário imprimia o número cru."""
    from app.apps.erp.core import diario as svc
    _bater(cenario, "Ana Pedreira", dt.time(10, 0))     # 07:00 em Fortaleza
    _bater(cenario, "Ana Pedreira", dt.time(20, 0))     # 17:00 em Fortaleza
    e = svc.efetivo_do_ponto(cenario["s"], cenario["obra"], HOJE)
    assert e["indisponivel"] is None
    assert [p["nome"] for p in e["pessoas"]] == ["Ana Pedreira"]
    assert e["pessoas"][0]["entrada"] == "07:00"
    assert e["pessoas"][0]["saida"] == "17:00"


# ---------------------------------------------------------------------------
# 2. BATIDA REJEITADA NÃO É GENTE EM OBRA
# ---------------------------------------------------------------------------
def test_batida_rejeitada_nao_entra_no_efetivo(cenario):
    """⚑ O filtro antigo era "não CANCELADA" — e CANCELADA não é um status do
    ponto (os quatro são VALIDA, EM_ANALISE, AJUSTADA e REJEITADA). Ou seja: o
    filtro não tirava nada, e quem teve a batida REJEITADA contava como
    presente no documento."""
    from app.apps.erp.core import diario as svc
    _bater(cenario, "Ana Pedreira", dt.time(10, 0))
    _bater(cenario, "Bruno Servente", dt.time(10, 5), status="REJEITADA")
    e = svc.efetivo_do_ponto(cenario["s"], cenario["obra"], HOJE)
    assert e["total"] == 1
    assert [p["nome"] for p in e["pessoas"]] == ["Ana Pedreira"]


def test_batida_ajustada_e_em_analise_continuam_contando(cenario):
    """AJUSTADA é a batida que a pessoa ESQUECEU e o supervisor incluiu — não
    é correção que substitui outra. Tirá-la faria sumir gente que trabalhou.
    EM_ANALISE também conta, como no espelho do ponto."""
    from app.apps.erp.core import diario as svc
    _bater(cenario, "Ana Pedreira", dt.time(10, 0), status="AJUSTADA")
    _bater(cenario, "Bruno Servente", dt.time(10, 5), status="EM_ANALISE")
    e = svc.efetivo_do_ponto(cenario["s"], cenario["obra"], HOJE)
    assert e["total"] == 2


# ---------------------------------------------------------------------------
# 3. UMA PESSOA É UMA PESSOA, por mais que ela bata
# ---------------------------------------------------------------------------
def test_quatro_batidas_da_mesma_pessoa_contam_uma_pessoa(cenario):
    """*"Talvez pela quantidade de pontos."* Era a desconfiança dele, e é a
    que o teste fecha: jornada normal tem quatro batidas (entrada, almoço,
    volta, saída) e continua sendo uma pessoa."""
    from app.apps.erp.core import diario as svc
    for h in (dt.time(10, 0), dt.time(14, 0), dt.time(16, 0), dt.time(20, 0)):
        _bater(cenario, "Ana Pedreira", h)
    e = svc.efetivo_do_ponto(cenario["s"], cenario["obra"], HOJE)
    assert e["total"] == 1
    assert e["por_funcao"] == [{"funcao": "—", "quantidade": 1}]
    assert e["pessoas"][0]["entrada"] == "07:00"
    assert e["pessoas"][0]["saida"] == "17:00"


def test_uma_batida_so_nao_inventa_horario_de_saida(cenario):
    """Quem só bateu a entrada não tem saída. Mostrar a mesma hora nos dois
    lados seria o diário afirmando uma jornada de zero minuto."""
    from app.apps.erp.core import diario as svc
    _bater(cenario, "Ana Pedreira", dt.time(10, 0))
    e = svc.efetivo_do_ponto(cenario["s"], cenario["obra"], HOJE)
    assert e["pessoas"][0]["entrada"] == "07:00"
    assert e["pessoas"][0]["saida"] is None


# ---------------------------------------------------------------------------
# 4. O EFETIVO DA PLANILHA NÃO SOMA COM O DO PONTO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("com_ponto,esperado", [(False, 17), (True, 1)])
def test_a_lista_da_planilha_so_conta_quando_o_ponto_esta_vazio(
        cenario, com_ponto, esperado):
    """⚑ O DEFEITO QUE ELE VIU. Os 1.427 diários da planilha antiga trazem o
    efetivo do dia; quando o MESMO dia também tem batida, o diário somava os
    dois registros da mesma gente (17 da planilha + 1 do ponto = 18 numa obra
    que teve 1). Agora, havendo ponto, vale o ponto — que tem nome e hora."""
    from app.apps.erp.core import diario as svc
    s = cenario["s"]
    if com_ponto:
        _bater(cenario, "Ana Pedreira", dt.time(10, 0))
    efetivo = svc.efetivo_do_ponto(s, cenario["obra"], HOJE)
    da_planilha = [{"funcao": "SERVENTE", "quantidade": 17}]

    assert svc.importado_conta(efetivo, da_planilha) is (not com_ponto)
    papel = svc._efetivo_para_o_papel(efetivo, [], da_planilha)
    assert papel["total_proprio"] == esperado
    # e o que ficou de fora é DITO, nunca apagado: é o que a obra informou na
    # época, e some do papel seria esconder registro.
    assert bool(papel["da_planilha_fora_da_soma"]) is com_ponto


def test_sem_ponto_nenhum_o_papel_antigo_continua_declarando_o_efetivo(cenario):
    """A trava do outro lado: o papel dos 1.380 dias antigos não pode voltar a
    sair declarando zero pessoa em obra."""
    from app.apps.erp.core import diario as svc
    efetivo = svc.efetivo_do_ponto(cenario["s"], cenario["obra"], HOJE)
    papel = svc._efetivo_para_o_papel(
        efetivo, [], [{"funcao": "SERVENTE", "quantidade": 17},
                      {"funcao": "PEDREIRO", "quantidade": 6}])
    assert papel["total_proprio"] == 23
    assert papel["da_planilha_fora_da_soma"] == []


def test_a_tela_diz_se_a_lista_da_planilha_entra_na_conta(cenario):
    """Quem decide é a regra, não a tela — o papel usa a mesma. A tela só
    precisa receber a resposta pronta."""
    from app.apps.erp.core import diario as svc
    _bater(cenario, "Ana Pedreira", dt.time(10, 0))
    dia = svc.abrir_dia(cenario["s"], cenario["obra"], HOJE)
    assert dia["efetivo_importado_conta"] is False
    assert dia["efetivo"]["total"] == 1


# ---------------------------------------------------------------------------
# 5. A FUNÇÃO VEM DO REGISTRO SEM MULTIPLICAR A PESSOA
# ---------------------------------------------------------------------------
def test_o_mesmo_cpf_em_duas_grafias_no_registro_nao_dobra_a_pessoa(
        banco_analisesps, cenario):
    """⚑ ERA UM JOIN, E JOIN MULTIPLICA.

    A função da maioria mora no Registro de Colaboradores
    (`analisesps.colaborador`), onde o CPF é a chave COMO TEXTO — e o
    casamento aqui é por dígitos. Bastava o mesmo CPF estar lá nas duas
    grafias ("529.982.247-25" e "52998224725") para a MESMA PESSOA virar duas
    linhas, e duas pessoas na conta do efetivo. Hoje a função vem por
    subconsulta com LIMIT 1: a contagem de gente em obra não depende de a
    tabela vizinha estar limpa.
    """
    from app.apps.erp.core import diario as svc
    s = cenario["s"]
    for grafia, cargo in [("529.982.247-25", "Pedreiro"),
                          ("52998224725", "Servente")]:
        s.execute(text("""
            INSERT INTO analisesps.colaborador (cpf, nome, cargo)
            VALUES (:c, 'Ana Pedreira', :g)
            ON CONFLICT (cpf) DO UPDATE SET cargo = EXCLUDED.cargo"""),
            {"c": grafia, "g": cargo})
    s.flush()
    _bater(cenario, "Ana Pedreira", dt.time(10, 0))

    e = svc.efetivo_do_ponto(s, cenario["obra"], HOJE)
    assert e["total"] == 1, f"a pessoa foi contada mais de uma vez: {e['pessoas']}"
    assert sum(f["quantidade"] for f in e["por_funcao"]) == 1
