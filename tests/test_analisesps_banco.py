"""Análise de SPs — o SQL contra um Postgres DE VERDADE.

Por que este arquivo existe. A sessão dublada da suíte ignora WHERE, e a
montagem de filtro testada em `test_analisesps_filtros.py` só olha o texto do
SQL antes de ele sair daqui. Nenhum dos dois responde à pergunta que importa:
o banco aceita e entende este comando?

Cinco coisas deste módulo vivem inteiramente dentro do SQL e não existem fora
dele — o `regexp_replace` do código de barras, o `translate` do acento em
"INVÁLIDO", a subconsulta de duplicidade, o `AT TIME ZONE` de Brasília e o
`ON CONFLICT` da gravação. Um erro em qualquer uma passaria por toda a suíte e
apareceria na tela do operador.

Rodam contra o Postgres descartável do `docker-compose.teste.yml` (no PC) ou o
que o GitHub Actions sobe a cada envio. Sem `ERP_TEST_DATABASE_URL`, são
pulados e a suíte segue.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_analisesps(banco, monkeypatch):
    """Cria o schema `analisesps` pela migração de verdade, e o derruba no fim.

    Usa o MESMO arquivo `.sql` que o botão da tela aplica em produção: se a
    migração tiver erro de sintaxe, é aqui que ele aparece — não no Render."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None          # a engine é preguiçosa; força recriar

    pasta = __import__("pathlib").Path(db_analisesps.__file__).parent / "migracoes"
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        # TODAS as migrações, em ordem, pelos mesmos arquivos que o botão da
        # tela aplica em produção: erro de sintaxe aparece aqui, não no Render.
        for caminho in sorted(pasta.glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.commit()
    db_analisesps._engine = None


def semear(registros):
    """Grava SPs pelo MESMO caminho que a sincronização usa."""
    from app.apps.analisesps import sincronizacao
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        sincronizacao.gravar_registros(conn, registros)


def sp(id_, **campos):
    """Uma SP com os campos vazios preenchidos — a planilha sempre manda todos."""
    from app.apps.analisesps import colunas
    base = {c: "" for c in colunas.CHAVES}
    base["id"] = id_
    base.update(campos)
    return base


# ---------------------------------------------------------------------------
# A migração e a gravação
# ---------------------------------------------------------------------------
def test_a_migracao_roda_no_postgres(banco_analisesps):
    """Se a migração não fosse válida, nada abaixo funcionaria — e em produção
    o botão pararia no meio."""
    from app.apps.analisesps.db import consultar_um
    assert consultar_um("SELECT count(*) FROM analisesps.sps")[0] == 0


def test_gravar_converte_valor_e_data(banco_analisesps):
    semear([sp("1", valor="6.750,00", vencimento="31/12/2026", credor="ACME")])
    from app.apps.analisesps.db import consultar_um
    linha = consultar_um(
        "SELECT valor_num, vencimento_d, credor FROM analisesps.sps WHERE id = '1'")
    assert linha[0] == Decimal("6750.00")
    assert linha[1] == dt.date(2026, 12, 31)
    assert linha[2] == "ACME"


def test_gravar_a_mesma_sp_duas_vezes_atualiza_em_vez_de_duplicar(banco_analisesps):
    """`ON CONFLICT`, e não "apaga tudo e insere de novo": a tabela nunca fica
    vazia no meio do caminho, então uma carga interrompida deixa a base velha
    íntegra em vez de deixar buraco."""
    semear([sp("1", credor="ANTES", valor="100,00")])
    semear([sp("1", credor="DEPOIS", valor="200,00")])
    from app.apps.analisesps.db import consultar_um
    assert consultar_um("SELECT count(*) FROM analisesps.sps")[0] == 1
    linha = consultar_um("SELECT credor, valor_num FROM analisesps.sps WHERE id='1'")
    assert linha == ("DEPOIS", Decimal("200.00"))


# ---------------------------------------------------------------------------
# O RELATÓRIO COM FILTRO — o defeito que chegou à produção em 09/09/2026
#
# "Vê quando clico em relatório: Deu erro." A tela estourava SEMPRE que havia
# filtro com valor, e abria normalmente quando não havia. A causa: as somas
# das quatro dimensões saem de uma varredura só (GROUPING SETS), e os
# parâmetros estavam sendo passados FORA DA ORDEM DO TEXTO do SQL — o WHERE
# antes dos CASE repetidos dentro do GROUPING.
#
# Sem filtro, as duas ordens coincidiam e tudo passava. Bastava filtrar por
# qualquer coisa para os CASE do GROUPING receberem o valor do filtro,
# deixarem de ser idênticos aos do SELECT, e o banco recusar a consulta.
#
# POR QUE PASSOU PELA SUÍTE: a sessão dublada ignora WHERE, então lá o filtro
# nunca vira parâmetro de verdade; e os testes de banco que existiam somavam
# sem filtro nenhum. É exatamente o buraco que este arquivo existe para tapar,
# e ele estava aberto.
# ---------------------------------------------------------------------------
FILTROS_DO_RELATORIO = [
    ({}, "sem filtro nenhum — era o único caso que passava"),
    ({"status_pgt": ["Pagar"]}, "uma lista suspensa"),
    ({"busca": "acme"}, "a busca livre"),
    ({"centro_custo": ["OBRA-1"]}, "a obra, que abre a célula antes de comparar"),
    ({"valor_ini": "100", "valor_fim": "5000"}, "faixa de valor"),
    ({"situacoes": ["risco"]}, "situação, que não vira parâmetro"),
    ({"status_pgt": ["Pagar"], "busca": "acme", "conta": ["ITAU"]},
     "três ao mesmo tempo"),
]


@pytest.mark.parametrize("filtro,porque", FILTROS_DO_RELATORIO,
                         ids=[p for _, p in FILTROS_DO_RELATORIO])
def test_o_relatorio_soma_com_filtro_sem_o_banco_recusar(banco_analisesps,
                                                         filtro, porque):
    """O banco tem de ACEITAR a consulta com filtro. Antes da correção, cinco
    destes sete estouravam."""
    from app.apps.analisesps import consultas
    semear([sp("1", credor="ACME", valor="1.000,00", status_pgt="Pagar",
               centro_custo="OBRA-1", conta="ITAU", projeto="P1",
               tipo_despesa="Material"),
            sp("2", credor="OUTRO", valor="2.000,00", status_pgt="Pago",
               centro_custo="OBRA-2", conta="BB", projeto="P2",
               tipo_despesa="Serviço")])

    somas = consultas.agregar_varias(
        filtro, ["projeto", "centro_custo", "tipo_despesa", "conta"],
        "geral", "tudo", 100)

    assert set(somas) == {"projeto", "centro_custo", "tipo_despesa", "conta"}


@pytest.mark.parametrize("filtro,porque", FILTROS_DO_RELATORIO,
                         ids=[p for _, p in FILTROS_DO_RELATORIO])
def test_a_soma_junta_bate_com_a_soma_separada(banco_analisesps, filtro, porque):
    """Não basta o banco aceitar: o número tem de ser o MESMO da soma feita
    uma dimensão por vez, que é a conta que o Streamlit fazia. Uma ordem de
    parâmetros errada pode não estourar e ainda assim somar a coisa errada —
    e aí ninguém percebe."""
    from app.apps.analisesps import consultas
    semear([sp("1", credor="ACME", valor="1.000,00", status_pgt="Pagar",
               centro_custo="OBRA-1", conta="ITAU", projeto="P1",
               tipo_despesa="Material"),
            sp("2", credor="OUTRO", valor="2.000,00", status_pgt="Pagar",
               centro_custo="OBRA-2", conta="BB", projeto="P2",
               tipo_despesa="Serviço"),
            sp("3", credor="ACME", valor="500,00", status_pgt="Pago",
               centro_custo="", conta="ITAU", projeto="P1",
               tipo_despesa="Material")])

    dimensoes = ["projeto", "centro_custo", "tipo_despesa", "conta"]
    juntas = consultas.agregar_varias(filtro, dimensoes, "geral", "tudo", 100)

    for d in dimensoes:
        sozinha = consultas.agregar(filtro, d, "geral", "tudo", 100)
        arruma = lambda linhas: sorted(  # noqa: E731
            (r["rotulo"], r["quantidade"], r["total"]) for r in linhas)
        assert arruma(juntas[d]) == arruma(sozinha), d


def test_o_relatorio_aceita_a_mesma_dimensao_repetida(banco_analisesps):
    """A tela pede as quatro dimensões fixas MAIS a que a pessoa escolheu — e
    a escolhida quase sempre é uma das quatro. Repetida, ela não pode virar
    dois agrupamentos iguais: o banco recusaria."""
    from app.apps.analisesps import consultas
    semear([sp("1", centro_custo="OBRA-1", valor="10,00", status_pgt="Pagar")])

    somas = consultas.agregar_varias(
        {"status_pgt": ["Pagar"]},
        ["projeto", "centro_custo", "tipo_despesa", "conta", "centro_custo"],
        "geral", "tudo", 100)

    assert [r["rotulo"] for r in somas["centro_custo"]] == ["OBRA-1"]


# ---------------------------------------------------------------------------
# O que só o banco sabe responder
# ---------------------------------------------------------------------------
def test_soma_e_contagem_saem_do_banco(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([
        sp("1", valor="100,00", status_pgt="Pagar"),
        sp("2", valor="250,50", status_pgt="Pagar"),
        sp("3", valor="1.000,00", status_pgt="Pago"),
    ])
    resumo = consultas.resumo({})
    assert resumo["quantidade"] == 3
    assert resumo["total"] == Decimal("1350.50")
    assert resumo["quantidade_pagar"] == 2
    assert resumo["total_pagar"] == Decimal("350.50")


def test_busca_livre_exige_todos_os_termos(banco_analisesps):
    """Termos separados por vírgula: TODOS precisam aparecer, e podem estar em
    campos diferentes da mesma SP."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", credor="VOTORANTIM S.A.", descricao="entrega de areia"),
        sp("2", credor="VOTORANTIM S.A.", descricao="frete"),
        sp("3", credor="OUTRA EMPRESA", descricao="entrega de areia"),
    ])
    achados = [l["id"] for l in consultas.listar({"busca": "votorantim, areia"})]
    assert achados == ["1"]


def test_a_busca_casa_por_trecho_e_nao_por_palavra_inteira(banco_analisesps):
    """Procurar "cimento" acha "CIMENTOS" — a busca é por TRECHO, como no
    Streamlit, que usava `contains`.

    Isto está aqui porque me enganou: escrevi um teste esperando que
    "cimento" não casasse com o credor "VOTORANTIM CIMENTOS", e ele casa. O
    comportamento é o certo e é o que o operador espera de uma busca; o que
    faltava era estar escrito em algum lugar."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", credor="VOTORANTIM CIMENTOS", descricao="frete"),
        sp("2", credor="AREIA E BRITA", descricao="cimento a granel"),
    ])
    achados = {l["id"] for l in consultas.listar({"busca": "cimento"})}
    assert achados == {"1", "2"}


def test_busca_nao_diferencia_maiuscula(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([sp("1", credor="Votorantim")])
    assert len(consultas.listar({"busca": "VOTORANTIM"})) == 1


def test_busca_trata_por_cento_como_texto_e_nao_como_curinga(banco_analisesps):
    """A prova no banco de verdade do que o escape faz: procurar "100%" acha a
    SP que tem "100%" escrito, e NÃO a que tem "1000"."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", descricao="desconto de 100% na entrega"),
        sp("2", descricao="1000 sacos de cimento"),
    ])
    assert [l["id"] for l in consultas.listar({"busca": "100%"})] == ["1"]


def test_busca_trata_sublinhado_como_texto(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([sp("1", nf="nota_1"), sp("2", nf="notaX1")])
    assert [l["id"] for l in consultas.listar({"busca": "nota_1"})] == ["1"]


def test_centro_de_custo_casa_dentro_da_celula(banco_analisesps):
    """Na planilha o centro de custo às vezes vem com mais de uma obra na
    mesma célula. Igualdade exata perderia essas linhas.

    Este teste, escrito na conversão a partir do dado real, é a razão de a
    separação aceitar BARRA além da vírgula: o dono citou "CONS, CRECHE SWAP",
    e a base também tem "OBRA-12 / OBRA-13"."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", centro_custo="OBRA-12"),
        sp("2", centro_custo="OBRA-12 / OBRA-13"),
        sp("3", centro_custo="OBRA-99"),
        sp("4", centro_custo="OBRA-12, OBRA-14"),
    ])
    achados = {l["id"] for l in consultas.listar({"centro_custo": ["OBRA-12"]})}
    assert achados == {"1", "2", "4"}


# ---------------------------------------------------------------------------
# As cinco situações — as regras traduzidas do Streamlit
# ---------------------------------------------------------------------------
def test_status_de_agendamento_so_vale_para_quem_esta_a_pagar(banco_analisesps):
    """Regra do original que é fácil perder na tradução: uma SP já paga não
    mostra agendamento nenhum, mesmo que a coluna esteja preenchida."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", status_pgt="Pagar", agendado="Agendado"),
        sp("2", status_pgt="Pago", agendado="Agendado"),
        sp("3", status_pgt="Pagar", agendado="falhaagendar"),
        sp("4", status_pgt="Pagar", agendado="Desagendar"),
    ])
    por_id = {l["id"]: l["status_agend"] for l in consultas.listar({})}
    assert por_id["1"] == "Agendado"
    assert por_id["2"] == ""                    # está paga: não mostra
    assert por_id["3"] == "Falha Agendar"       # "contém falha", em qualquer forma
    assert por_id["4"] == ""                    # Desagendar não é um estado


def test_sem_agendamento_acha_os_de_coluna_vazia(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([
        sp("1", status_pgt="Pagar", agendado="Agendado"),
        sp("2", status_pgt="Pagar", agendado=""),
    ])
    achados = [l["id"] for l in
               consultas.listar({"status_agend": ["Sem Agendamento"]})]
    assert achados == ["2"]


def test_boleto_invalido_pega_as_tres_formas(banco_analisesps):
    """Inválido é: a palavra escrita (com ou sem acento), o campo vazio, ou só
    zeros. O `translate` do acento é a parte que só o Postgres sabe se aceita."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="INVÁLIDO"),
        sp("2", forma_pagamento="Boleto", status_pgt="Pagar", codigo_barras=""),
        sp("3", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="00000000000"),
        sp("4", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="34191790010104351004791020150008291070026000"),
        sp("5", forma_pagamento="Boleto", status_pgt="Pago", codigo_barras=""),
    ])
    achados = {l["id"] for l in
               consultas.listar({"situacoes": ["boleto_invalido"]})}
    assert achados == {"1", "2", "3"}           # 4 é válido; 5 já foi pago


def test_boleto_duplicado_conta_pagar_e_pago_mas_lista_so_pagar(banco_analisesps):
    """A regra menos óbvia do original, e a que mais custaria errar: o par
    "1 Pago + 1 Pagar" precisa aparecer, mostrando o Pagar — porque é ele que
    ainda pode ser pago em duplicidade."""
    from app.apps.analisesps import consultas
    codigo = "34191790010104351004791020150008291070026000"
    semear([
        sp("1", forma_pagamento="Boleto", status_pgt="Pagar", codigo_barras=codigo),
        sp("2", forma_pagamento="Boleto", status_pgt="Pago", codigo_barras=codigo),
        sp("3", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="11111111111111111111111111111111111111111111"),
        sp("4", forma_pagamento="Boleto", status_pgt="Cancelado",
           codigo_barras="22222222222222222222222222222222222222222222"),
        sp("5", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="22222222222222222222222222222222222222222222"),
    ])
    achados = {l["id"] for l in
               consultas.listar({"situacoes": ["boleto_duplicado"]})}
    # 1 repete com a 2 (Pago conta) -> aparece. 3 é única. 5 repete só com uma
    # CANCELADA, que não conta -> não aparece.
    assert achados == {"1"}


def test_boleto_duplicado_ignora_formatacao_diferente(banco_analisesps):
    """O mesmo boleto digitado com e sem pontuação é o mesmo boleto. Se a
    comparação fosse por texto cru, a duplicidade passaria batida."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="34191.79001 01043.510047 91020.150008 2 91070026000"),
        sp("2", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="34191790010104351004791020150008291070026000"),
    ])
    achados = {l["id"] for l in
               consultas.listar({"situacoes": ["boleto_duplicado"]})}
    assert achados == {"1", "2"}


def test_cadastro_incompleto_pega_as_tres_causas(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([
        # (a) Pix sem a chave — a coluna traz só o rótulo
        sp("1", forma_pagamento="Pix", info_pgt="Chave Pix:",
           centro_custo="OBRA-1", codigo_integracao="123", status_pgt="Pagar"),
        # (b) sem centro de custo
        sp("2", forma_pagamento="Boleto", centro_custo="",
           codigo_integracao="123", status_pgt="Pagar"),
        # (c) sem integração Omie, e ainda ativa
        sp("3", forma_pagamento="Boleto", centro_custo="OBRA-1",
           codigo_integracao="", status_pgt="Pagar"),
        # sem integração, mas já paga: não é pendência
        sp("4", forma_pagamento="Boleto", centro_custo="OBRA-1",
           codigo_integracao="", status_pgt="Pago"),
        # completa
        sp("5", forma_pagamento="Pix", info_pgt="Chave Pix: 11999998888",
           centro_custo="OBRA-1", codigo_integracao="123", status_pgt="Pagar"),
    ])
    achados = {l["id"] for l in
               consultas.listar({"situacoes": ["cadastro_incompleto"]})}
    assert achados == {"1", "2", "3"}


def test_risco_vem_da_analise_da_ia(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([
        sp("1", analise_ia="Pagamento COM RISCO de duplicidade"),
        sp("2", analise_ia="Sem risco aparente"),
    ])
    achados = {l["id"] for l in consultas.listar({"situacoes": ["risco"]})}
    assert achados == {"1"}


# ---------------------------------------------------------------------------
# Fuso horário
# ---------------------------------------------------------------------------
def test_vencido_usa_o_dia_de_brasilia(banco_analisesps):
    """Entre 21h e meia-noite de Brasília o servidor em UTC já virou o dia. Uma
    SP que vence amanhã não pode aparecer como atrasada para quem confere à
    noite."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.horario import agora

    hoje = agora().date()
    semear([
        sp("1", vencimento=(hoje - dt.timedelta(days=1)).strftime("%d/%m/%Y"),
           status_pgt="Pagar"),
        sp("2", vencimento=hoje.strftime("%d/%m/%Y"), status_pgt="Pagar"),
        sp("3", vencimento=(hoje + dt.timedelta(days=1)).strftime("%d/%m/%Y"),
           status_pgt="Pagar"),
    ])
    por_id = {l["id"]: l for l in consultas.listar({})}
    assert por_id["1"]["vencido"] and not por_id["1"]["vence_hoje"]
    assert por_id["2"]["vence_hoje"] and not por_id["2"]["vencido"]
    assert not por_id["3"]["vencido"] and not por_id["3"]["vence_hoje"]


def test_sp_paga_nao_aparece_como_vencida(banco_analisesps):
    """Vencimento no passado com a conta já paga não é atraso — é histórico. A
    tela inteira ficaria vermelha sem esta distinção."""
    from app.apps.analisesps import consultas
    semear([sp("1", vencimento="01/01/2020", status_pgt="Pago")])
    assert consultas.listar({})[0]["vencido"] is False


# ---------------------------------------------------------------------------
# Ordenação e páginas
# ---------------------------------------------------------------------------
def test_ordena_por_data_e_nao_por_texto(banco_analisesps):
    """Como texto, "10/01" viria antes de "09/02". É o motivo de a coluna
    convertida existir."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", vencimento="10/01/2026"),
        sp("2", vencimento="09/02/2026"),
        sp("3", vencimento="05/01/2026"),
    ])
    ordem = [l["id"] for l in consultas.listar({}, ordem="vencimento")]
    assert ordem == ["3", "1", "2"]


def test_sem_vencimento_vai_para_o_fim(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([sp("1", vencimento=""), sp("2", vencimento="10/01/2026")])
    assert [l["id"] for l in consultas.listar({}, ordem="vencimento")] == ["2", "1"]


def test_paginas_nao_repetem_nem_pulam_linha(banco_analisesps, monkeypatch):
    """Duas páginas seguidas precisam cobrir todo mundo, uma vez cada. Sem o
    desempate por id, linhas com o mesmo vencimento poderiam trocar de lugar
    entre uma página e outra — e uma SP sumiria da lista sem ninguém notar."""
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "POR_PAGINA", 3)
    semear([sp(str(i), vencimento="10/01/2026", valor="10,00")
            for i in range(1, 8)])
    primeira = [l["id"] for l in consultas.listar({}, pagina=1)]
    segunda = [l["id"] for l in consultas.listar({}, pagina=2)]
    terceira = [l["id"] for l in consultas.listar({}, pagina=3)]
    assert len(primeira) == 3 and len(segunda) == 3 and len(terceira) == 1
    assert sorted(primeira + segunda + terceira) == sorted(
        str(i) for i in range(1, 8))


# ---------------------------------------------------------------------------
# A fila de escrita
# ---------------------------------------------------------------------------
def test_alterar_grava_enfileira_e_registra(banco_analisesps, monkeypatch):
    """O caminho inteiro de uma alteração, que é o que garante que nada se
    perca: grava na hora, põe na fila da planilha, e registra no log."""
    from flask import Flask

    from app.apps.analisesps import auth, tarefas, web
    from app.apps.analisesps.db import consultar, consultar_um

    semear([sp("1", status_pgt="Pagar"), sp("2", status_pgt="Pagar")])
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    # O envio de verdade sai em processo separado; aqui só interessa que a
    # alteração fique gravada e enfileirada.
    monkeypatch.setattr(tarefas, "disparar", lambda *a, **k: {"ok": True})

    aplicativo = Flask(__name__)
    aplicativo.secret_key = "teste"
    aplicativo.register_blueprint(web.bp)
    aplicativo.config["TESTING"] = True

    with aplicativo.test_client() as cliente:
        cliente.post("/analisesps/entrar",
                     data={"senha": "op", "nome": "Marcelo"})
        resposta = cliente.post("/analisesps/api/alterar", json={
            "ids": ["1", "2"], "coluna": "status_pgt",
            "valor": "Pago", "acao": "Marcar Pago"})

    assert resposta.status_code == 200
    assert resposta.get_json()["alteradas"] == 2

    assert consultar_um(
        "SELECT count(*) FROM analisesps.sps WHERE status_pgt = 'Pago'")[0] == 2
    assert consultar_um("SELECT count(*) FROM analisesps.fila")[0] == 2
    registros = consultar(
        "SELECT sp_id, valor_anterior, perfil, status "
        "  FROM analisesps.log_alteracoes ORDER BY sp_id")
    assert [r[1] for r in registros] == ["Pagar", "Pagar"]
    assert {r[2] for r in registros} == {auth.OPERADOR}
    assert {r[3] for r in registros} == {"pendente"}


def test_alterar_a_mesma_celula_duas_vezes_deixa_so_a_ultima(banco_analisesps,
                                                             monkeypatch):
    """Sem isto, duas trocas de status seguidas virariam duas gravações na
    planilha, e a ordem entre elas não seria garantida — a antiga poderia
    chegar depois da nova e desfazê-la."""
    from flask import Flask

    from app.apps.analisesps import tarefas, web
    from app.apps.analisesps.db import consultar_um

    semear([sp("1", status_pgt="Pagar")])
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    monkeypatch.setattr(tarefas, "disparar", lambda *a, **k: {"ok": True})

    aplicativo = Flask(__name__)
    aplicativo.secret_key = "teste"
    aplicativo.register_blueprint(web.bp)
    aplicativo.config["TESTING"] = True

    with aplicativo.test_client() as cliente:
        cliente.post("/analisesps/entrar",
                     data={"senha": "op", "nome": "Marcelo"})
        for valor in ("Pago", "Pagar", "Cancelado"):
            cliente.post("/analisesps/api/alterar", json={
                "ids": ["1"], "coluna": "status_pgt", "valor": valor})

    assert consultar_um("SELECT count(*) FROM analisesps.fila")[0] == 1
    assert consultar_um(
        "SELECT valor FROM analisesps.fila")[0] == "Cancelado"
    # O log, ao contrário da fila, guarda as três — é o histórico.
    assert consultar_um("SELECT count(*) FROM analisesps.log_alteracoes")[0] == 3


def test_coluna_nao_editavel_e_recusada(banco_analisesps, monkeypatch):
    """Só as duas colunas que o operador mexe no dia a dia. A planilha é a dona
    do resto — deixar alterar o valor ou o credor por aqui abriria caminho para
    a base e a planilha divergirem sem ninguém saber."""
    from flask import Flask

    from app.apps.analisesps import web
    from app.apps.analisesps.db import consultar_um

    semear([sp("1", credor="ACME")])
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")

    aplicativo = Flask(__name__)
    aplicativo.secret_key = "teste"
    aplicativo.register_blueprint(web.bp)
    aplicativo.config["TESTING"] = True

    with aplicativo.test_client() as cliente:
        cliente.post("/analisesps/entrar",
                     data={"senha": "op", "nome": "Marcelo"})
        resposta = cliente.post("/analisesps/api/alterar", json={
            "ids": ["1"], "coluna": "credor", "valor": "OUTRO"})

    assert resposta.status_code == 400
    assert consultar_um("SELECT credor FROM analisesps.sps WHERE id='1'")[0] == "ACME"
    assert consultar_um("SELECT count(*) FROM analisesps.fila")[0] == 0


def test_alterar_sp_inexistente_nao_grava_nada(banco_analisesps, monkeypatch):
    """Falha inteira ou não falha: se um id da lista não existe, nenhuma das
    outras é alterada. Meia alteração seria pior do que nenhuma."""
    from flask import Flask

    from app.apps.analisesps import web
    from app.apps.analisesps.db import consultar_um

    semear([sp("1", status_pgt="Pagar")])
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")

    aplicativo = Flask(__name__)
    aplicativo.secret_key = "teste"
    aplicativo.register_blueprint(web.bp)
    aplicativo.config["TESTING"] = True

    with aplicativo.test_client() as cliente:
        cliente.post("/analisesps/entrar",
                     data={"senha": "op", "nome": "Marcelo"})
        resposta = cliente.post("/analisesps/api/alterar", json={
            "ids": ["1", "999"], "coluna": "status_pgt", "valor": "Pago"})

    assert resposta.status_code == 404
    assert consultar_um(
        "SELECT status_pgt FROM analisesps.sps WHERE id='1'")[0] == "Pagar"
    assert consultar_um("SELECT count(*) FROM analisesps.fila")[0] == 0


# ---------------------------------------------------------------------------
# RELATÓRIO — as somas que substituem o pandas
# ---------------------------------------------------------------------------
def test_o_relatorio_ignora_canceladas(banco_analisesps):
    """Uma SP cancelada não é despesa. Somá-la inflaria todo total, e o erro
    passaria despercebido porque o número continuaria parecendo plausível."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", valor="100,00", status_pgt="Pagar"),
        sp("2", valor="900,00", status_pgt="Cancelado"),
    ])
    numeros = consultas.numeros_do_relatorio({})
    assert numeros["quantidade"] == 1
    assert numeros["total"] == Decimal("100.00")


def test_o_ticket_medio_nao_divide_por_zero(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([sp("1", valor="100,00", status_pgt="Cancelado")])
    assert consultas.numeros_do_relatorio({})["ticket"] == 0


def test_contas_a_pagar_e_contas_pagas_sao_universos_diferentes(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([
        sp("1", valor="100,00", status_pgt="Pagar"),
        sp("2", valor="200,00", status_pgt="Pago"),
    ])
    assert consultas.numeros_do_relatorio({}, "pagar")["total"] == Decimal("100.00")
    assert consultas.numeros_do_relatorio({}, "pagas")["total"] == Decimal("200.00")
    assert consultas.numeros_do_relatorio({}, "geral")["total"] == Decimal("300.00")


def test_o_periodo_das_pagas_conta_pela_data_do_pagamento(banco_analisesps):
    """A regra que mais engana: contas a pagar se olham pelo VENCIMENTO; contas
    pagas, pela DATA DO PAGAMENTO. Trocar as duas dá um total que não fecha com
    nada, e ninguém descobre por quê."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.horario import agora

    hoje = agora().date()
    # Vencida no mês passado, mas paga hoje.
    semear([sp("1", valor="500,00", status_pgt="Pago",
               vencimento=(hoje - dt.timedelta(days=45)).strftime("%d/%m/%Y"),
               data_pagamento=hoje.strftime("%d/%m/%Y"))])
    assert consultas.numeros_do_relatorio({}, "pagas", "mes")["quantidade"] == 1


def test_a_soma_por_dimensao_bate_com_o_total(banco_analisesps):
    """A soma das partes tem de dar o todo. É por isso que o valor em branco
    vira "(vazio)" em vez de sumir da lista."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", valor="100,00", centro_custo="OBRA-1", status_pgt="Pagar"),
        sp("2", valor="200,00", centro_custo="OBRA-2", status_pgt="Pagar"),
        sp("3", valor="300,00", centro_custo="", status_pgt="Pagar"),
    ])
    linhas = consultas.agregar({}, "centro_custo")
    assert sum(l["total"] for l in linhas) == Decimal("600.00")
    assert consultas.VAZIO in [l["rotulo"] for l in linhas]


def test_a_quebra_vem_da_maior_para_a_menor(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([
        sp("1", valor="100,00", projeto="Pequeno", status_pgt="Pagar"),
        sp("2", valor="900,00", projeto="Grande", status_pgt="Pagar"),
    ])
    assert [l["rotulo"] for l in consultas.agregar({}, "projeto")] == \
        ["Grande", "Pequeno"]


def test_dimensao_fora_da_lista_e_recusada(banco_analisesps):
    """O nome da coluna entra no texto do SQL. Ele não pode vir de fora."""
    from app.apps.analisesps import consultas
    with pytest.raises(ValueError):
        consultas.agregar({}, "codigo_barras")


def test_credores_agrupam_por_documento_e_nao_por_nome(banco_analisesps):
    """O mesmo fornecedor aparece escrito de vários jeitos. Somar por nome o
    partiria em três, e nenhuma das partes apareceria entre os maiores."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", valor="100,00", documento="123", credor="ACME LTDA",
           status_pgt="Pagar"),
        sp("2", valor="200,00", documento="123", credor="Acme Ltda ME",
           status_pgt="Pagar"),
        sp("3", valor="150,00", documento="456", credor="OUTRA", status_pgt="Pagar"),
    ])
    credores = consultas.top_credores({})
    assert credores[0]["documento"] == "123"
    assert credores[0]["total"] == Decimal("300.00")
    assert credores[0]["quantidade"] == 2


def test_credor_sem_documento_nao_some(banco_analisesps):
    from app.apps.analisesps import consultas
    semear([sp("1", valor="100,00", documento="", credor="SEM DOC",
               status_pgt="Pagar")])
    assert consultas.top_credores({})[0]["documento"] == "(sem CPF/CNPJ)"


def test_o_aging_poe_cada_atraso_na_sua_faixa(banco_analisesps):
    from app.apps.analisesps import consultas
    from app.apps.analisesps.horario import agora

    hoje = agora().date()
    def vencida(dias):
        return (hoje - dt.timedelta(days=dias)).strftime("%d/%m/%Y")

    semear([
        sp("1", valor="10,00", status_pgt="Pagar", vencimento=vencida(3)),
        sp("2", valor="20,00", status_pgt="Pagar", vencimento=vencida(10)),
        sp("3", valor="30,00", status_pgt="Pagar", vencimento=vencida(200)),
        sp("4", valor="40,00", status_pgt="Pagar", vencimento=vencida(0)),
    ])
    faixas = {f["faixa"]: f for f in consultas.aging_vencidos({})}
    assert faixas["1 a 7 dias"]["total"] == Decimal("10.00")
    assert faixas["8 a 15 dias"]["total"] == Decimal("20.00")
    assert faixas["mais de 90 dias"]["total"] == Decimal("30.00")
    # A que vence HOJE não está atrasada e não entra em faixa nenhuma.
    assert sum(f["quantidade"] for f in faixas.values()) == 3


def test_o_aging_sai_na_ordem_das_faixas(banco_analisesps):
    """Da mais recente para a mais antiga. Fora de ordem, a leitura inverte o
    sentido de urgência."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.horario import agora
    hoje = agora().date()
    semear([sp(str(i), valor="10,00", status_pgt="Pagar",
               vencimento=(hoje - dt.timedelta(days=d)).strftime("%d/%m/%Y"))
            for i, d in enumerate([200, 3, 40], start=1)])
    assert [f["faixa"] for f in consultas.aging_vencidos({})] == \
        ["1 a 7 dias", "31 a 60 dias", "mais de 90 dias"]


# ---------------------------------------------------------------------------
# AUDITORIA
# ---------------------------------------------------------------------------
def test_pontualidade_calcula_a_antecedencia_por_responsavel(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        # Ana registra com antecedência: +10 e +20 dias.
        sp("1", responsavel="Ana", valor="100,00",
           solicitacao="01/03/2026", vencimento="11/03/2026"),
        sp("2", responsavel="Ana", valor="100,00",
           solicitacao="01/03/2026", vencimento="21/03/2026"),
        # João registra DEPOIS de vencer: -5 dias.
        sp("3", responsavel="João", valor="500,00",
           solicitacao="10/03/2026", vencimento="05/03/2026"),
        sp("4", responsavel="João", valor="300,00",
           solicitacao="10/03/2026", vencimento="05/03/2026"),
    ])
    por_pessoa = {l["responsavel"]: l for l in
                  auditoria.pontualidade({}, minimo_lancamentos=1)}
    assert por_pessoa["Ana"]["media_dias"] == 15
    assert por_pessoa["Ana"]["atrasados"] == 0
    assert por_pessoa["João"]["media_dias"] == -5
    assert por_pessoa["João"]["atrasados"] == 2
    assert por_pessoa["João"]["valor_atrasado"] == Decimal("800.00")
    assert por_pessoa["João"]["percentual_atrasados"] == 100.0


def test_pontualidade_vem_do_pior_para_o_melhor(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        sp("1", responsavel="Ana", solicitacao="01/03/2026", vencimento="21/03/2026"),
        sp("2", responsavel="João", solicitacao="10/03/2026", vencimento="05/03/2026"),
    ])
    nomes = [l["responsavel"] for l in
             auditoria.pontualidade({}, minimo_lancamentos=1)]
    assert nomes[0] == "João"


def test_pontualidade_ignora_quem_tem_poucos_lancamentos(banco_analisesps):
    """Quem tem uma SP só não deve encabeçar o ranking dos piores por causa
    dela — seria injusto e tiraria a atenção de quem realmente atrasa."""
    from app.apps.analisesps import auditoria
    semear([
        sp("1", responsavel="Ocasional", solicitacao="10/03/2026",
           vencimento="05/03/2026"),
        sp("2", responsavel="Frequente", solicitacao="01/03/2026",
           vencimento="11/03/2026"),
        sp("3", responsavel="Frequente", solicitacao="01/03/2026",
           vencimento="11/03/2026"),
    ])
    nomes = [l["responsavel"] for l in
             auditoria.pontualidade({}, minimo_lancamentos=2)]
    assert nomes == ["Frequente"]


def test_pontualidade_so_conta_quem_tem_as_duas_datas(banco_analisesps):
    """Sem uma das datas não há antecedência para calcular. Chutar zero puxaria
    a média de todo mundo para baixo."""
    from app.apps.analisesps import auditoria
    semear([
        sp("1", responsavel="Ana", solicitacao="", vencimento="11/03/2026"),
        sp("2", responsavel="Ana", solicitacao="01/03/2026", vencimento=""),
        sp("3", responsavel="Ana", solicitacao="01/03/2026", vencimento="11/03/2026"),
    ])
    linha = auditoria.pontualidade({}, minimo_lancamentos=1)[0]
    assert linha["quantidade"] == 1


def test_nf_duplicada_acha_a_mesma_nota_no_mesmo_documento(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        sp("1", documento="123", nf="555", status_pgt="Pagar"),
        sp("2", documento="123", nf="555", status_pgt="Pagar"),
        sp("3", documento="999", nf="555", status_pgt="Pagar"),   # outro credor
        sp("4", documento="123", nf="", status_pgt="Pagar"),      # sem nota
    ])
    achados = {l["id"] for l in auditoria.nf_duplicada({})}
    assert achados == {"1", "2"}


def test_possivel_duplicidade_respeita_a_janela(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        # Mesmo credor, mesmo valor, 3 dias de diferença: suspeito.
        sp("1", documento="123", valor="500,00", solicitacao="01/03/2026"),
        sp("2", documento="123", valor="500,00", solicitacao="04/03/2026"),
        # Mesmo credor e valor, mas 40 dias depois: é o aluguel, não duplicidade.
        sp("3", documento="456", valor="900,00", solicitacao="01/03/2026"),
        sp("4", documento="456", valor="900,00", solicitacao="10/04/2026"),
    ])
    achados = {l["id"] for l in auditoria.possivel_duplicidade({}, dias=7)}
    assert achados == {"1", "2"}


def test_possivel_duplicidade_ignora_valor_zero(banco_analisesps):
    """Várias SPs sem valor preenchido cairiam todas no mesmo grupo e a tela
    apontaria uma duplicidade que não existe."""
    from app.apps.analisesps import auditoria
    semear([
        sp("1", documento="123", valor="", solicitacao="01/03/2026"),
        sp("2", documento="123", valor="", solicitacao="02/03/2026"),
    ])
    assert auditoria.possivel_duplicidade({}) == []


def test_sem_classificacao_diz_o_que_falta(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        sp("1", centro_custo="", projeto="Aurora", status_pgt="Pagar"),
        sp("2", centro_custo="OBRA-1", projeto="", status_pgt="Pagar"),
        sp("3", centro_custo="", projeto="", status_pgt="Pagar"),
        sp("4", centro_custo="OBRA-1", projeto="Aurora", status_pgt="Pagar"),
    ])
    faltas = {l["id"]: l["faltando"] for l in auditoria.sem_classificacao({})}
    assert faltas == {"1": "Centro de Custo", "2": "Projeto",
                      "3": "Centro de Custo + Projeto"}


def test_sem_integracao_so_aponta_as_ativas(banco_analisesps):
    """Uma SP já paga sem código é problema de histórico; uma ativa é trabalho
    a fazer. Cobrar as duas juntas afogaria o que importa."""
    from app.apps.analisesps import auditoria
    semear([
        sp("1", codigo_integracao="", status_pgt="Pagar"),
        sp("2", codigo_integracao="", status_pgt="Pago"),
        sp("3", codigo_integracao="", status_pgt="Cancelado"),
        sp("4", codigo_integracao="OMIE-9", status_pgt="Pagar"),
    ])
    assert [l["id"] for l in auditoria.sem_integracao_omie({})] == ["1"]


def test_a_auditoria_ignora_canceladas_em_todas_as_checagens(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        sp("1", status_pgt="Cancelado", centro_custo="", projeto="",
           documento="123", nf="555", analise_ia="COM RISCO"),
        sp("2", status_pgt="Cancelado", centro_custo="", projeto="",
           documento="123", nf="555", analise_ia="COM RISCO"),
    ])
    assert auditoria.sem_classificacao({}) == []
    assert auditoria.risco_ia({}) == []
    assert auditoria.nf_duplicada({}) == []


def test_o_resumo_conta_todas_as_checagens(banco_analisesps):
    from app.apps.analisesps import auditoria
    semear([
        sp("1", status_pgt="Pagar", centro_custo="", projeto="",
           analise_ia="COM RISCO", codigo_integracao=""),
    ])
    contagens = auditoria.resumo({})
    assert set(contagens) == set(auditoria.CHECAGENS)
    assert contagens["risco_ia"] == 1
    assert contagens["sem_classificacao"] == 1
    assert contagens["sem_integracao"] == 1


def test_a_checagem_de_barras_usa_o_mesmo_sql_da_tela_de_solicitacoes(banco_analisesps):
    """Se as duas telas divergirem, uma delas está mentindo — e ninguém sabe
    qual. Por isso a auditoria reusa o SQL do filtro, em vez de repetir a regra."""
    from app.apps.analisesps import auditoria, consultas
    semear([
        sp("1", forma_pagamento="Boleto", status_pgt="Pagar", codigo_barras=""),
        sp("2", forma_pagamento="Boleto", status_pgt="Pagar",
           codigo_barras="34191790010104351004791020150008291070026000"),
    ])
    pela_auditoria = {l["id"] for l in
                      auditoria.codigos_de_barras({})["invalidos"]}
    pela_tela = {l["id"] for l in
                 consultas.listar({"situacoes": ["boleto_invalido"]})}
    assert pela_auditoria == pela_tela == {"1"}


# ---------------------------------------------------------------------------
# LOTE
# ---------------------------------------------------------------------------
def test_o_lote_junta_os_grupos_com_os_dados_do_banco(banco_analisesps):
    from app.apps.analisesps import lote
    semear([
        sp("111", credor="ACME", valor="100,00", status_pgt="Pagar"),
        sp("222", credor="OUTRA", valor="250,00", status_pgt="Pagar"),
    ])
    montado = lote.montar("Pagar amanhã\n111\n\nDepois\n222")
    assert [g["titulo"] for g in montado["grupos"]] == ["Pagar amanhã", "Depois"]
    assert montado["grupos"][0]["total"] == Decimal("100.00")
    assert montado["grupos"][1]["total"] == Decimal("250.00")
    assert montado["total_geral"] == Decimal("350.00")
    assert montado["quantidade"] == 2


def test_o_lote_aponta_o_que_nao_existe_em_vez_de_ignorar(banco_analisesps):
    """Ignorar em silêncio seria o pior comportamento: quem colou precisa saber
    que aquele número não foi reconhecido."""
    from app.apps.analisesps import lote
    semear([sp("111", valor="100,00")])
    montado = lote.montar("111\n999")
    assert montado["nao_encontrados"] == ["999"]
    assert montado["grupos"][0]["nao_encontrados"] == ["999"]


def test_o_lote_faz_uma_consulta_so_para_todos_os_grupos(banco_analisesps):
    """Um lote com dez grupos não pode custar dez idas ao banco."""
    from app.apps.analisesps import db as banco
    from app.apps.analisesps import lote

    semear([sp(str(i), valor="10,00") for i in range(1, 11)])
    idas = []
    original = banco.consultar
    try:
        banco.consultar = lambda sql, params=(): idas.append(sql) or original(sql, params)
        texto = "\n\n".join(f"Grupo {i}\n{i}" for i in range(1, 11))
        montado = lote.montar(texto)
    finally:
        banco.consultar = original
    assert montado["quantidade"] == 10
    assert len(idas) == 1


def test_lote_vazio_nao_consulta_o_banco(banco_analisesps):
    from app.apps.analisesps import lote
    montado = lote.montar("")
    assert montado["quantidade"] == 0
    assert montado["grupos"] == []


def test_o_lote_e_guardado_e_relido(banco_analisesps):
    from app.apps.analisesps import lote
    lote.salvar("Pagar amanhã\n111", "operador", "marcelo")
    guardado = lote.ler("marcelo")
    assert guardado["conteudo"] == "Pagar amanhã\n111"
    assert guardado["salvo_por"] == "operador"
    assert guardado["salvo_em"] is not None


def test_salvar_o_lote_de_novo_substitui_e_nao_acumula(banco_analisesps):
    """Uma linha POR PESSOA. Salvar de novo substitui a dela, e não empilha."""
    from app.apps.analisesps import lote
    from app.apps.analisesps.db import consultar_um
    lote.salvar("primeiro", "a", "marcelo")
    lote.salvar("segundo", "b", "marcelo")
    assert consultar_um(
        "SELECT count(*) FROM analisesps.lote WHERE pessoa = 'marcelo'")[0] == 1
    assert lote.ler("marcelo")["conteudo"] == "segundo"


def test_o_lote_de_uma_pessoa_nao_encosta_no_da_outra(banco_analisesps):
    """Era um lote só até a migração 003, e a segunda pessoa a salvar
    sobrescrevia o trabalho da primeira — sem aviso nenhum.

    ESTE TESTE TAMBÉM COBRE O DEFEITO DE 11/09/2026: a exportação e o PDF
    liam o lote de `pessoa = ""`, que é o antigo compartilhado, e entregavam
    um lote congelado por mais que a pessoa salvasse o dela."""
    from app.apps.analisesps import lote
    lote.salvar("o do marcelo", "MARCELO", "marcelo")
    lote.salvar("o da karla", "KARLA", "karla")

    assert lote.ler("marcelo")["conteudo"] == "o do marcelo"
    assert lote.ler("karla")["conteudo"] == "o da karla"
    # E o lote antigo, o de pessoa vazia, continua sendo outra coisa.
    assert lote.ler("")["conteudo"] != "o do marcelo"


# ---------------------------------------------------------------------------
# AGENDA — a tabela e a leitura
# ---------------------------------------------------------------------------
def test_a_agenda_grava_e_le(banco_analisesps):
    from app.apps.analisesps import agenda
    from app.apps.analisesps.db import conexao
    registro = {c: "" for c in agenda.COLUNAS}
    registro.update({"id": "1", "titulo": "FGTS", "categoria": "FGTS",
                     "data_base": "07/01/2026", "recorrencia": "mensal",
                     "dia_mes": "7", "ajuste_dia_util": "antecipa",
                     "alerta_dias_antes": "5", "status": "ativo"})
    with conexao() as conn:
        assert agenda.gravar(conn, [registro]) == 1
    lidos = agenda.listar()
    assert len(lidos) == 1
    assert lidos[0]["titulo"] == "FGTS"
    assert agenda.um("1")["categoria"] == "FGTS"


def test_os_feriados_locais_sao_substituidos_e_nao_acumulados(banco_analisesps):
    """Um feriado tirado da planilha tem de sumir daqui, senão o ajuste de dia
    útil continuaria desviando de uma data que já não é feriado."""
    from app.apps.analisesps import agenda
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        agenda.gravar_feriados(conn, [(dt.date(2026, 3, 19), "São José"),
                                      (dt.date(2026, 8, 15), "Padroeira")])
    assert len(agenda.feriados_extra()) == 2
    with conexao() as conn:
        agenda.gravar_feriados(conn, [(dt.date(2026, 3, 19), "São José")])
    assert agenda.feriados_extra() == {dt.date(2026, 3, 19)}


def test_o_feriado_local_entra_no_calculo_do_ano(banco_analisesps):
    from app.apps.analisesps import agenda
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        agenda.gravar_feriados(conn, [(dt.date(2026, 3, 19), "São José")])
    do_ano = agenda.feriados_do_ano(2026)
    assert dt.date(2026, 3, 19) in do_ano       # o local
    assert dt.date(2026, 12, 25) in do_ano      # e os nacionais continuam


# ---------------------------------------------------------------------------
# QUEM É QUEM: o lote e os filtros de cada pessoa
#
# Estes só provam alguma coisa contra um Postgres de verdade: o dublê da suíte
# ignora WHERE, e é exatamente o WHERE (`pessoa = ?`) que separa o trabalho de
# um do trabalho do outro. Num dublê, o lote da Joana "voltaria" para o
# Marcelo e o teste passaria mesmo com o defeito.
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_cada_pessoa_tem_o_seu_lote(banco_analisesps):
    """Era um lote só: quem salvasse por último apagava o do outro, sem aviso."""
    from app.apps.analisesps import lote

    lote.salvar("Pagar amanhã\n1111111111", "Marcelo", "marcelo")
    lote.salvar("Semana que vem\n2222222222", "Joana", "joana")

    assert "1111111111" in lote.ler("marcelo")["conteudo"]
    assert "2222222222" not in lote.ler("marcelo")["conteudo"]
    assert "2222222222" in lote.ler("joana")["conteudo"]
    assert lote.ler("joana")["salvo_por"] == "Joana"

    # Salvar de novo troca só o da própria pessoa.
    lote.salvar("Outra coisa\n3333333333", "Marcelo", "marcelo")
    assert "2222222222" in lote.ler("joana")["conteudo"], (
        "salvar o lote de um apagou o do outro")


@pytest.mark.banco
def test_o_lote_de_antes_nao_se_perde(banco_analisesps):
    """Havia um lote compartilhado no dia da mudança. Ele não é apagado nem
    copiado para todo mundo — fica guardado, e a tela oferece trazer."""
    from app.apps.analisesps import lote
    lote.salvar("Lote da equipe\n9999999999", "operador", lote.COMPARTILHADO)

    assert lote.ler("marcelo")["conteudo"] == "", "o lote novo já nasceu cheio"
    assert "9999999999" in lote.lote_de_antes()["conteudo"]


@pytest.mark.banco
def test_o_filtro_guardado_e_de_cada_um(banco_analisesps):
    """O filtro volta sozinho na próxima vez, e o de um não vaza para o outro."""
    from app.apps.analisesps import preferencias

    preferencias.gravar("marcelo", preferencias.FILTRO,
                        {"status_pgt": ["Pagar"], "busca": ["cimento"]})
    preferencias.gravar("joana", preferencias.FILTRO, {"status_pgt": ["Pago"]})

    assert preferencias.ler("marcelo", preferencias.FILTRO)["busca"] == ["cimento"]
    assert preferencias.ler("joana", preferencias.FILTRO)["status_pgt"] == ["Pago"]
    assert "busca" not in preferencias.ler("joana", preferencias.FILTRO)

    # Guardar de novo substitui, não acumula.
    preferencias.gravar("marcelo", preferencias.FILTRO, {})
    assert preferencias.ler("marcelo", preferencias.FILTRO) == {}
    assert preferencias.ler("joana", preferencias.FILTRO) != {}, (
        "limpar o filtro de um limpou o do outro")


@pytest.mark.banco
def test_o_registro_de_alteracoes_guarda_quem_foi(banco_analisesps):
    """Antes o log sabia só que perfil mexeu. A pergunta que aparece quando um
    pagamento sai errado é "quem fez isso?", e ela não tinha resposta."""
    from app.apps.analisesps.db import conexao, consultar_um
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.log_alteracoes "
            "  (sp_id, coluna, valor, acao, perfil, pessoa, status) "
            "VALUES ('1', 'agendado', 'Agendado', 'Agendar', 'operador', "
            "        'Marcelo', 'pendente')")
        conn.commit()
    linha = consultar_um(
        "SELECT pessoa, perfil FROM analisesps.log_alteracoes WHERE sp_id = '1'")
    assert linha[0] == "Marcelo"
    assert linha[1] == "operador"


# ---------------------------------------------------------------------------
# NOTA REPETIDA x PARCELAMENTO
#
# Só com Postgres de verdade: a regra vive inteira no SQL (janela, agrupamento
# e as expressões que pescam o número da parcela na descrição). O dublê da
# suíte ignora WHERE e GROUP BY — aqui ele provaria nada.
# ---------------------------------------------------------------------------
def _sp_nf(conn, sp_id, doc, nf, parcela="", descricao="", venc="2026-09-10",
           solicitacao="2026-08-01"):
    conn.execute(
        "INSERT INTO analisesps.sps (id, credor, valor_num, vencimento_d, "
        "  solicitacao_d, status_pgt, documento, nf, parcela, descricao) "
        "VALUES (?, 'FORNECEDOR', 1000, ?, ?, 'Pagar', ?, ?, ?, ?)",
        (sp_id, venc, solicitacao, doc, nf, parcela, descricao))


@pytest.mark.banco
def test_parcelamento_nao_e_apontado_como_nota_repetida(banco_analisesps):
    """Uma nota parcelada em três gera três SPs com o mesmo número. Apontar as
    três como duplicidade todo mês é o jeito mais rápido de fazer alguém
    parar de olhar a auditoria. Pedido do dono em 04/09/2026."""
    from app.apps.analisesps import auditoria
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        # Parcelamento pela coluna Parcela: não é duplicidade.
        _sp_nf(conn, "1000000001", "11.111/0001", "NF-100", parcela="001/003")
        _sp_nf(conn, "1000000002", "11.111/0001", "NF-100", parcela="002/003")
        _sp_nf(conn, "1000000003", "11.111/0001", "NF-100", parcela="003/003")
        # Parcelamento escrito só na descrição: também não é.
        _sp_nf(conn, "3000000001", "33.333/0001", "NF-300",
               descricao="parcela 1 de 2")
        _sp_nf(conn, "3000000002", "33.333/0001", "NF-300",
               descricao="parcela 2 de 2")
        conn.commit()

    apontadas = {a["id"] for a in auditoria.nf_duplicada({})}
    assert not apontadas, f"parcelamento apontado como duplicidade: {apontadas}"


@pytest.mark.banco
def test_a_nota_repetida_de_verdade_continua_sendo_apontada(banco_analisesps):
    """A regra afrouxou de propósito, mas não pode cegar: sem marca de parcela
    nenhuma, ou com a MESMA parcela repetida, "é parcelamento" não explica — e
    o certo é alguém olhar. Na dúvida, aponta: conferir à toa custa pouco
    perto de pagar duas vezes."""
    from app.apps.analisesps import auditoria
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        # Sem parcela nenhuma: é duplicidade até prova em contrário.
        _sp_nf(conn, "2000000001", "22.222/0001", "NF-200")
        _sp_nf(conn, "2000000002", "22.222/0001", "NF-200")
        # Duas SPs dividindo a MESMA parcela: alguém lançou duas vezes.
        _sp_nf(conn, "4000000001", "44.444/0001", "NF-400", parcela="001/002")
        _sp_nf(conn, "4000000002", "44.444/0001", "NF-400", parcela="001/002")
        # Parcelamento pela metade: uma tem marca, a outra não.
        _sp_nf(conn, "5000000001", "55.555/0001", "NF-500", parcela="001/002")
        _sp_nf(conn, "5000000002", "55.555/0001", "NF-500")
        conn.commit()

    apontadas = {a["id"] for a in auditoria.nf_duplicada({})}
    assert {"2000000001", "2000000002"} <= apontadas, "sem parcela, tem de apontar"
    assert {"4000000001", "4000000002"} <= apontadas, "mesma parcela, tem de apontar"
    assert {"5000000001", "5000000002"} <= apontadas, (
        "parcelamento que não se explica inteiro tem de apontar")


@pytest.mark.banco
def test_o_periodo_da_auditoria_recorta_de_verdade(banco_analisesps):
    """O recorte vive no WHERE, que o dublê da suíte ignora."""
    import datetime as dt
    from app.apps.analisesps import auditoria
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        _sp_nf(conn, "6000000001", "66.666/0001", "NF-600",
               venc="2026-09-15", solicitacao="2026-08-01")
        _sp_nf(conn, "6000000002", "66.666/0001", "NF-600",
               venc="2026-09-15", solicitacao="2026-08-01")
        _sp_nf(conn, "7000000001", "77.777/0001", "NF-700",
               venc="2026-12-15", solicitacao="2026-11-01")
        _sp_nf(conn, "7000000002", "77.777/0001", "NF-700",
               venc="2026-12-15", solicitacao="2026-11-01")
        conn.commit()

    setembro = {a["id"] for a in auditoria.nf_duplicada(
        {}, periodo={"campo": "vencimento",
                     "de": dt.date(2026, 9, 1), "ate": dt.date(2026, 9, 30)})}
    assert setembro == {"6000000001", "6000000002"}

    por_solicitacao = {a["id"] for a in auditoria.nf_duplicada(
        {}, periodo={"campo": "solicitacao",
                     "de": dt.date(2026, 11, 1), "ate": dt.date(2026, 11, 30)})}
    assert por_solicitacao == {"7000000001", "7000000002"}


# ---------------------------------------------------------------------------
# AS OBRAS (centro de custo) — uma célula pode ter mais de uma
#
# Só com Postgres de verdade: a separação vive no SQL (`string_to_array` +
# `unnest`), e o dublê da suíte ignora GROUP BY e WHERE.
# ---------------------------------------------------------------------------
def _sp_obra(conn, sp_id, centro):
    conn.execute(
        "INSERT INTO analisesps.sps (id, credor, valor_num, vencimento_d, "
        "  status_pgt, centro_custo) "
        "VALUES (?, 'FORNECEDOR', 100, '2026-09-10', 'Pagar', ?)",
        (sp_id, centro))


@pytest.mark.banco
def test_a_lista_de_obras_vem_separada(banco_analisesps):
    """A planilha aceita "CONS, CRECHE SWAP" na mesma célula quando a despesa
    é rateada entre duas obras. A lista do filtro oferecia a combinação
    inteira como se fosse uma obra — e a obra sozinha, que é o que se procura,
    não aparecia em lugar nenhum."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        _sp_obra(conn, "0000000001", "CONS")
        _sp_obra(conn, "0000000002", "CRECHE SWAP")
        _sp_obra(conn, "0000000003", "CONS, CRECHE SWAP")
        _sp_obra(conn, "0000000004", "  CONS ,  ESCOLA SÃO JOSÉ  ")
        conn.commit()

    obras = consultas.opcoes("centro_custo", limite=200)
    assert not [o for o in obras if "," in o], f"combinação na lista: {obras}"
    assert "CONS" in obras
    assert "CRECHE SWAP" in obras
    # O espaço em volta da vírgula é comum na planilha e não pode virar uma
    # obra diferente.
    assert "ESCOLA SÃO JOSÉ" in obras


@pytest.mark.banco
def test_filtrar_por_obra_alcanca_a_celula_com_varias(banco_analisesps):
    from app.apps.analisesps import consultas
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        _sp_obra(conn, "0000000001", "CONS")
        _sp_obra(conn, "0000000002", "CRECHE SWAP")
        _sp_obra(conn, "0000000003", "CONS, CRECHE SWAP")
        conn.commit()

    achadas = {l["id"] for l in consultas.listar({"centro_custo": ["CONS"]})}
    assert achadas == {"0000000001", "0000000003"}

    das_duas = {l["id"] for l in consultas.listar(
        {"centro_custo": ["CONS", "CRECHE SWAP"]})}
    assert das_duas == {"0000000001", "0000000002", "0000000003"}, (
        "marcar duas obras tem de somar as duas")


@pytest.mark.banco
def test_uma_obra_nao_arrasta_a_de_nome_parecido(banco_analisesps):
    """Era o defeito do casamento por "contém", herdado do Streamlit:
    procurar a obra "CONS" trazia também "CONSTRUÇÃO DO GALPÃO", porque uma é
    pedaço da outra. Agora a célula é aberta na vírgula e a comparação é com a
    obra INTEIRA — a que a pessoa escolheu na lista."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        _sp_obra(conn, "0000000001", "CONS")
        _sp_obra(conn, "0000000002", "CONSTRUÇÃO DO GALPÃO")
        conn.commit()

    achadas = {l["id"] for l in consultas.listar({"centro_custo": ["CONS"]})}
    assert achadas == {"0000000001"}, "arrastou a obra de nome parecido"


@pytest.mark.banco
def test_a_obra_casa_sem_ligar_para_maiuscula(banco_analisesps):
    """A pessoa escolhe da lista, mas o filtro guardado pode voltar com outra
    caixa depois de alguém editar a planilha."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        _sp_obra(conn, "0000000001", "CONS")
        conn.commit()

    assert consultas.listar({"centro_custo": ["cons"]})
    assert consultas.listar({"centro_custo": ["CoNs"]})


# ---------------------------------------------------------------------------
# A SINCRONIZAÇÃO QUE NÃO ENXERGAVA A LINHA SEM CARIMBO NOVO
#
# O dono, em 11/09/2026, olhando a SP 1443253428: "na planilha esse registro
# está pago, e ele está aparecendo no lote como PAGAR. A base está atualizada,
# o relógio está batendo."
#
# A causa: o carimbo da coluna V é escrito pelo gatilho `onEdit` da planilha, e
# esse gatilho NÃO DISPARA quando quem escreve é um script — e quem alimenta a
# SPsBD são scripts. A célula muda, o carimbo não, e a linha nunca era relida.
# Conferido na planilha de verdade: entre as 63 primeiras linhas visíveis, 5
# estão com o carimbo VAZIO.
# ---------------------------------------------------------------------------
class AbaFalsa:
    """Uma aba de planilha de mentira, com as duas chamadas que a sincronização
    faz: ler uma coluna inteira e buscar faixas de linhas."""

    def __init__(self, linhas):
        # `linhas` já inclui o cabeçalho na posição 0, como na planilha.
        self.linhas = linhas
        self.faixas_pedidas = []

    def col_values(self, numero):
        return [(l[numero - 1] if numero - 1 < len(l) else "")
                for l in self.linhas]

    def batch_get(self, faixas):
        import re
        self.faixas_pedidas.extend(faixas)
        saida = []
        for faixa in faixas:
            n = int(re.search(r"A(\d+):", faixa).group(1))
            saida.append([self.linhas[n - 1]])
        return saida


def _linha_da_planilha(sp_id, status, carimbo):
    """Uma linha crua da SPsBD, nas posições de verdade das colunas."""
    from app.apps.analisesps import colunas
    linha = [""] * len(colunas._DEFS)
    linha[colunas.COLS["id"].idx] = sp_id
    linha[colunas.COLS["status_pgt"].idx] = status
    linha[colunas.COLS["carimbo"].idx] = carimbo
    return linha


def _sincronizar(monkeypatch, linhas):
    from app.apps.analisesps import sincronizacao
    aba = AbaFalsa([["cabeçalho"]] + linhas)
    monkeypatch.setattr(sincronizacao, "_aba_sps", lambda: aba)
    return sincronizacao.sincronizar_delta(), aba


def _marca_dagua_em(valor):
    """Põe a marca d'água onde o teste precisa.

    Sem isto a base nasce com marca d'água VAZIA, e aí todo carimbo é "maior
    que" ela — a sincronização traz a planilha inteira, que é o certo numa base
    virgem e atrapalha quem quer medir o caminho do dia a dia."""
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.meta (chave, valor) "
                     "VALUES ('ultimo_carimbo', ?) "
                     "ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor",
                     (valor,))
        conn.commit()


def _status_no_banco(sp_id):
    from app.apps.analisesps.db import consultar_um
    linha = consultar_um(
        "SELECT status_pgt FROM analisesps.sps WHERE id = ?", (sp_id,))
    return linha[0] if linha else None


@pytest.mark.banco
def test_linha_com_carimbo_VAZIO_e_alcancada_pela_conferencia(
        banco_analisesps, monkeypatch):
    """O DEFEITO QUE O DONO ACHOU. Sem carimbo, a linha nunca era relida — e
    não era atraso, era permanente: ela ficaria "Pagar" para sempre, por mais
    que se apertasse Atualizar."""
    semear([sp("1443253428", status_pgt="Pagar", carimbo="")])
    assert _status_no_banco("1443253428") == "Pagar"

    resultado, _ = _sincronizar(monkeypatch, [
        _linha_da_planilha("1443253428", "Pago", "")])

    assert _status_no_banco("1443253428") == "Pago", (
        "a linha sem carimbo continuou velha — é o defeito de 11/09/2026")
    assert resultado["sem_carimbo"] == 1


@pytest.mark.banco
def test_linha_com_carimbo_VELHO_tambem_e_alcancada(
        banco_analisesps, monkeypatch):
    """Pior que o carimbo vazio, e mais comum: o script muda a célula e o
    carimbo fica com a data antiga. Pelo carimbo, nada mudou."""
    semear([sp("1", status_pgt="Pagar", carimbo="2026-09-04 16:05:23")])
    _marca_dagua_em("2026-09-10 10:00:00")

    _sincronizar(monkeypatch, [
        _linha_da_planilha("1", "Pago", "2026-09-04 16:05:23")])

    assert _status_no_banco("1") == "Pago"


@pytest.mark.banco
def test_a_conferencia_nao_relê_a_planilha_inteira(banco_analisesps, monkeypatch):
    """A conferência acha a linha mudada SEM trazer as outras. Se ela puxasse
    tudo, a sincronização de 5 em 5 minutos viraria a carga inicial — e o banco
    tem um décimo de um núcleo."""
    semear([sp(str(n), status_pgt="Pagar", carimbo="2026-09-04 16:05:23")
            for n in range(1, 21)])
    _marca_dagua_em("2026-09-04 16:05:23")

    _, aba = _sincronizar(monkeypatch, [
        _linha_da_planilha(str(n), "Pago" if n == 7 else "Pagar",
                           "2026-09-04 16:05:23")
        for n in range(1, 21)])

    assert len(aba.faixas_pedidas) == 1, (
        f"trouxe {len(aba.faixas_pedidas)} linhas; só uma mudou")
    assert _status_no_banco("7") == "Pago"
    assert _status_no_banco("8") == "Pagar"


@pytest.mark.banco
def test_carimbo_novo_continua_bastando_sem_conferir_conteudo(
        banco_analisesps, monkeypatch):
    """O caminho barato não pode ter sido quebrado: carimbo novo traz a linha
    mesmo quando a coluna conferida está igual."""
    semear([sp("1", status_pgt="Pagar", carimbo="2026-09-04 16:05:23",
               nf="")])
    _marca_dagua_em("2026-09-04 16:05:23")
    _, aba = _sincronizar(monkeypatch, [
        _linha_da_planilha("1", "Pagar", "2026-09-30 09:00:00")])
    assert len(aba.faixas_pedidas) == 1


@pytest.mark.banco
def test_a_marca_dagua_fica_um_segundo_atras(banco_analisesps, monkeypatch):
    """AS LINHAS QUE EMPATAM NO SEGUNDO. Um script que grava 800 linhas de uma
    vez carimba TODAS com o mesmo segundo — visto na planilha de verdade: 58
    linhas com "2026-09-04 16:05:23".

    Se a varredura pegar metade delas, a marca d'água sobe para aquele segundo
    e a outra metade, carimbada igual, nunca mais satisfaz "maior que". Some
    para sempre. Guardar um segundo atrás faz a borda ser reexaminada."""
    from app.apps.analisesps.db import consultar_um

    semear([sp("1", status_pgt="Pagar", carimbo="")])
    _sincronizar(monkeypatch, [
        _linha_da_planilha("1", "Pagar", "2026-09-11 14:32:10")])

    guardado = consultar_um(
        "SELECT valor FROM analisesps.meta WHERE chave = 'ultimo_carimbo'")
    assert guardado[0] == "2026-09-11 14:32:09", (
        "a marca d'água ficou no próprio segundo — quem empatar nele se perde")


@pytest.mark.banco
def test_a_sincronizacao_registra_QUANTAS_linhas_desceram(
        banco_analisesps, monkeypatch):
    """O RELÓGIO DA TELA NÃO PROVA NADA, e foi isso que despistou o dono: a
    hora da sincronização é gravada no fim de TODA rodada, tenha vindo linha
    ou não. Então "o relógio está batendo" só diz que ela rodou."""
    from app.apps.analisesps.db import consultar_um

    semear([sp("1", status_pgt="Pagar", carimbo="2026-09-04 16:05:23")])
    _marca_dagua_em("2026-09-04 16:05:23")
    _sincronizar(monkeypatch, [
        _linha_da_planilha("1", "Pagar", "2026-09-04 16:05:23")])

    assert consultar_um("SELECT valor FROM analisesps.meta "
                        "WHERE chave = 'ultima_sincronizacao_alteradas'")[0] == "0"
    assert consultar_um("SELECT valor FROM analisesps.meta "
                        "WHERE chave = 'ultima_sincronizacao'")[0], (
        "a hora foi gravada mesmo sem nada ter mudado — é isso que despista")


# ---------------------------------------------------------------------------
# OS COMPROVANTES ARRASTADOS — a memória que o dono pediu
#
# "Se eu sair da tela e voltar, a informação vai ser me dada ainda ou eu vou
# perder se eu mudar de tela?" — o dono, em 11/09/2026.
#
# Vai ser dada. É por isso que o resultado mora no banco e não na tela, e é
# isso que estes testes travam.
# ---------------------------------------------------------------------------
def _pdf(paginas: int) -> bytes:
    import io as _io
    from pypdf import PdfWriter
    escritor = PdfWriter()
    for _ in range(paginas):
        escritor.add_blank_page(width=200, height=200)
    saco = _io.BytesIO()
    escritor.write(saco)
    return saco.getvalue()


def _robo_falso(por_leva):
    """Um `baixabradesco` de mentira: devolve o que o de verdade devolveria.

    O robô verdadeiro fala com Omie, Pipefy, Sheets e Dropbox — nenhum teste
    encosta neles."""
    def responder(pedaco, nome):
        return {"planos": [
            {"match": {"status": "localizado"}, "pode_executar": True,
             "receipt": {"page": n + 1, "id_pipefy": f"sp{n}",
                         "valor_pago": "10,00", "nome_recebedor": "FORNECEDOR"}}
            for n in range(por_leva)]}
    return responder


@pytest.mark.banco
def test_o_resultado_do_comprovante_fica_GUARDADO(banco_analisesps, monkeypatch,
                                                  tmp_path):
    """O teste que responde à pergunta do dono: sair da tela e voltar não
    perde nada, porque nada mora na tela."""
    from app.apps.analisesps import comprovantes

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", _robo_falso(10))

    lote_id = comprovantes.guardar(_pdf(25), "comprovantes.pdf", "marcelo", "Marcelo")
    assert comprovantes.processar_um(lote_id)["ok"]

    # Aqui é a "volta à tela": tudo relido do banco, sem nenhum estado em pé.
    itens = comprovantes.itens_do_lote(lote_id)
    assert len(itens) == 30, "cada leva devolveu 10; três levas são 30 linhas"
    assert all(i["situacao"] == comprovantes.BAIXADO for i in itens)
    assert comprovantes.historico()[0]["situacao"] == "PRONTO"


@pytest.mark.banco
def test_cada_leva_e_gravada_NA_HORA_e_nao_no_fim(banco_analisesps, monkeypatch,
                                                  tmp_path):
    """Se o serviço reiniciar no meio de um PDF de cinquenta páginas, as levas
    já processadas têm de estar no banco. Guardar tudo para o fim perderia
    todas — e o gunicorn recicla o processo a cada ~150 requisições."""
    from app.apps.analisesps import comprovantes
    from app.apps.analisesps.db import consultar_um

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    vistos = []

    def responder(pedaco, nome):
        # No meio da segunda leva, olha o banco: a primeira já tem de estar lá.
        vistos.append(consultar_um(
            "SELECT count(*) FROM analisesps.comprovantes_item "
            " WHERE lote_id = (SELECT max(id) FROM analisesps.comprovantes_lote)")[0])
        return {"planos": [{"match": {"status": "localizado"},
                            "pode_executar": True, "receipt": {"page": 1}}]}

    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", responder)
    lote_id = comprovantes.guardar(_pdf(25), "x.pdf", "p", "P")
    comprovantes.processar_um(lote_id)

    assert vistos == [0, 1, 2], (
        f"as levas não foram gravadas uma a uma: {vistos}")


@pytest.mark.banco
def test_o_PDF_sai_do_disco_quando_o_lote_termina(banco_analisesps, monkeypatch,
                                                  tmp_path):
    """O comprovante já é guardado pelo robô no destino definitivo. O que fica
    aqui é cópia de passagem — e cópia de passagem que não é apagada vira
    disco cheio sem ninguém perceber."""
    import os

    from app.apps.analisesps import comprovantes
    from app.apps.analisesps.db import consultar_um

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", _robo_falso(1))
    lote_id = comprovantes.guardar(_pdf(2), "x.pdf", "p", "P")
    caminho = consultar_um(
        "SELECT caminho FROM analisesps.comprovantes_lote WHERE id = ?",
        (lote_id,))[0]
    assert os.path.exists(caminho)

    comprovantes.processar_um(lote_id)
    assert not os.path.exists(caminho), "o PDF ficou no disco depois de pronto"


@pytest.mark.banco
def test_falha_no_meio_marca_o_lote_e_guarda_o_que_ja_tinha_saido(
        banco_analisesps, monkeypatch, tmp_path):
    """Metade processada é melhor do que nada, DESDE QUE a tela diga que
    falhou. O pior seria ficar "processando" para sempre."""
    from app.apps.analisesps import comprovantes

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    chamadas = [0]

    def responder(pedaco, nome):
        chamadas[0] += 1
        if chamadas[0] == 2:
            raise RuntimeError("o Omie não respondeu")
        return {"planos": [{"match": {"status": "localizado"},
                            "pode_executar": True, "receipt": {"page": 1}}]}

    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", responder)
    lote_id = comprovantes.guardar(_pdf(25), "x.pdf", "p", "P")
    assert comprovantes.processar_um(lote_id)["ok"] is False

    lote = comprovantes.historico()[0]
    assert lote["situacao"] == "FALHOU"
    assert "Omie" in lote["erro"]
    assert len(comprovantes.itens_do_lote(lote_id)) == 1, (
        "a leva que deu certo antes da falha se perdeu")


@pytest.mark.banco
def test_arquivo_que_sumiu_do_disco_vira_recado_e_nao_lote_eterno(
        banco_analisesps, monkeypatch, tmp_path):
    """O contêiner reinicia e leva o disco junto. Dizer isso é melhor do que
    deixar o lote "na fila" para sempre — e o recado precisa dizer que mandar
    de novo é seguro, porque a trava do robô barra a baixa repetida."""
    import os

    from app.apps.analisesps import comprovantes

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    lote_id = comprovantes.guardar(_pdf(2), "x.pdf", "p", "P")
    os.remove(os.path.join(str(tmp_path), f"lote-{lote_id}.pdf"))

    comprovantes.processar_um(lote_id)
    lote = comprovantes.historico()[0]
    assert lote["situacao"] == "FALHOU"
    assert "de novo" in lote["erro"] and "duas vezes" in lote["erro"]


@pytest.mark.banco
def test_o_historico_conta_em_UMA_consulta_por_situacao(banco_analisesps,
                                                        monkeypatch, tmp_path):
    """Com quinze lotes na tela, uma consulta por lote seriam dezesseis idas
    ao banco — e o banco tem um décimo de um núcleo."""
    from app.apps.analisesps import comprovantes

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", _robo_falso(1))
    for n in range(3):
        comprovantes.processar_um(
            comprovantes.guardar(_pdf(1), f"{n}.pdf", "p", "P"))

    historico = comprovantes.historico()
    assert len(historico) == 3
    assert all(l["contagem"] for l in historico)
    assert historico[0]["resolvidos"] == 1 and historico[0]["pendencias"] == 0


@pytest.mark.banco
def test_arquivo_grande_demais_e_recusado_ANTES_de_ir_para_o_disco(
        banco_analisesps, monkeypatch, tmp_path):
    """A instância tem 2 GB divididos com 17 módulos e já morreu de falta de
    memória em julho de 2026."""
    from app.apps.analisesps import comprovantes
    from app.apps.analisesps.db import consultar_um

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    monkeypatch.setattr(comprovantes, "MAXIMO_POR_ARQUIVO", 100)
    with pytest.raises(comprovantes.ErroDeComprovante) as erro:
        comprovantes.guardar(_pdf(5), "gigante.pdf", "p", "P")
    assert "MB" in str(erro.value)
    assert consultar_um(
        "SELECT count(*) FROM analisesps.comprovantes_lote")[0] == 0


@pytest.mark.banco
def test_arquivo_que_nao_e_pdf_e_recusado_com_recado_de_gente(
        banco_analisesps, monkeypatch, tmp_path):
    from app.apps.analisesps import comprovantes
    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    with pytest.raises(comprovantes.ErroDeComprovante) as erro:
        comprovantes.guardar(b"isto nao e um pdf", "foto.pdf", "p", "P")
    assert "senha" in str(erro.value) or "corrompido" in str(erro.value)


@pytest.mark.banco
def test_a_fila_pega_todos_os_lotes_esperando(banco_analisesps, monkeypatch,
                                              tmp_path):
    """Duas levas soltas antes de a primeira terminar não podem ficar para
    trás."""
    from app.apps.analisesps import comprovantes

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", _robo_falso(1))
    comprovantes.guardar(_pdf(1), "a.pdf", "p", "P")
    comprovantes.guardar(_pdf(1), "b.pdf", "p", "P")

    assert comprovantes.processar_pendentes() == {"lotes": 2, "falhas": 0}
    assert all(l["situacao"] == "PRONTO" for l in comprovantes.historico())


@pytest.mark.banco
def test_o_que_pede_acao_aparece_no_topo_da_lista(banco_analisesps, monkeypatch,
                                                  tmp_path):
    """A ordem NÃO é a das páginas de propósito: quem abre isto quer saber o
    que ficou de fora."""
    from app.apps.analisesps import comprovantes

    monkeypatch.setattr(comprovantes, "PASTA", str(tmp_path))
    monkeypatch.setattr(comprovantes, "_mandar_ao_robo", lambda p, n: {
        "planos": [
            {"match": {"status": "localizado"}, "pode_executar": True,
             "receipt": {"page": 1}},
            {"match": {"status": "nao_localizado", "motivo": "sem par"},
             "pode_executar": False, "receipt": {"page": 2}}]})
    lote_id = comprovantes.guardar(_pdf(2), "x.pdf", "p", "P")
    comprovantes.processar_um(lote_id)

    itens = comprovantes.itens_do_lote(lote_id)
    assert itens[0]["situacao"] == comprovantes.NAO_LOCALIZADO, (
        "o que baixou veio na frente do que precisa de atenção")


# ---------------------------------------------------------------------------
# O NOME DO CREDOR — o mesmo CNPJ escrito de cinco jeitos
#
# A regra pura está em `test_analisesps_credores.py`, montada com os oito casos
# reais da planilha. Aqui é o que só o banco alcança: o agrupamento por CNPJ
# quando ele vem formatado de dois jeitos, a memória da decisão, e a reescrita
# chegando à fila da planilha.
# ---------------------------------------------------------------------------
def _sp_credor(sp_id, documento, credor):
    return sp(sp_id, documento=documento, credor=credor)


@pytest.mark.banco
def test_o_mesmo_CNPJ_formatado_de_dois_jeitos_e_UM_fornecedor(banco_analisesps):
    """Na planilha o mesmo CNPJ vem "09.444.530/0001-01" numa linha e
    "09444530000101" noutra. Agrupar pelo texto cru faria o fornecedor virar
    dois — e aí nenhuma das duas metades pareceria divergente."""
    from app.apps.analisesps import credores

    semear([_sp_credor("1", "09.444.530/0001-01", "TRIBUNAL DE JUSTIÇA DO CEARÁ"),
            _sp_credor("2", "09444530000101", "TRIBUNAL DE JUSTIÇA DO CEARÁ"),
            _sp_credor("3", "09.444.530/0001-01", "TRI")])

    achadas = credores.divergencias_da_base()
    assert len(achadas) == 1
    assert achadas[0]["documento"] == "09444530000101"
    assert achadas[0]["sps"] == 3
    assert achadas[0]["automatico"] is True


@pytest.mark.banco
def test_fornecedor_escrito_sempre_igual_nao_entra_na_lista(banco_analisesps):
    """Com 59 mil SPs, a lista só é útil se trouxer o que está errado."""
    from app.apps.analisesps import credores
    semear([_sp_credor(str(n), "29.066.773/0001-52", "SERTAO CASA E CONSTRUCAO")
            for n in range(1, 21)])
    assert credores.divergencias_da_base() == []


@pytest.mark.banco
def test_quantas_SPs_faltam_arrumar_conta_GRAFIA_e_nao_grupo(banco_analisesps):
    """O DEFEITO QUE APARECEU RODANDO CONTRA O DADO REAL. "SERVIÇOS" e
    "SERVICOS" são o mesmo grupo de nome — e mesmo assim há uma SP escrita
    diferente para reescrever. Contando por grupo, a tela dizia "0 a arrumar"
    justamente no caso mais fácil de todos."""
    from app.apps.analisesps import credores
    semear([_sp_credor("1", "04100718000100", "MASSA PRONTA SERVIÇOS LTDA"),
            _sp_credor("2", "04100718000100", "MASSA PRONTA SERVIÇOS LTDA"),
            _sp_credor("3", "04100718000100", "MASSA PRONTA SERVICOS LTDA")])
    achada = credores.divergencias_da_base()[0]
    assert achada["sps"] == 3
    assert achada["fora"] == 1, "disse que não havia nada para arrumar"


@pytest.mark.banco
def test_a_decisao_do_dono_MANDA_sobre_a_proposta(banco_analisesps):
    """CELPE virou NEOENERGIA. Se a regra pudesse voltar a propor CELPE na
    semana seguinte, o dono decidiria a mesma coisa para sempre — o contrário
    de "minimizar a interação do humano"."""
    from app.apps.analisesps import credores

    semear([_sp_credor("1", "10835932000108", "CELPE CIA ENERGETICA"),
            _sp_credor("2", "10835932000108", "NEOENERGIA")])
    antes = credores.divergencias_da_base()[0]
    assert antes["decidido"] is False

    credores.guardar_escolha("10.835.932/0001-08", "NEOENERGIA",
                             credores.DECIDIR, False, "Marcelo", 1)
    depois = credores.divergencias_da_base()[0]
    assert depois["nome"] == "NEOENERGIA"
    assert depois["decidido"] is True and depois["decidido_por"] == "Marcelo"


@pytest.mark.banco
def test_so_as_SPs_fora_do_padrao_sao_reescritas(banco_analisesps):
    """Cada reescrita é uma célula na fila e uma ida ao Google. Regravar o que
    já está certo seria pagar o preço sem mudar nada."""
    from app.apps.analisesps import credores
    semear([_sp_credor("1", "09444530000101", "TRI"),
            _sp_credor("2", "09444530000101", "TRIBUNAL DE JUSTIÇA DO CEARÁ"),
            _sp_credor("3", "09444530000101", "TRIBUNAL")])
    assert sorted(credores.sps_para_reescrever(
        "09444530000101", "TRIBUNAL DE JUSTIÇA DO CEARÁ")) == ["1", "3"]


@pytest.mark.banco
def test_aplicar_o_nome_passa_pela_FILA_e_pelo_LOG(banco_analisesps, monkeypatch):
    """O caminho tem de ser o de sempre — banco, fila, log, planilha. É ele que
    garante que a mudança apareça no Log com quem mexeu, e que chegue à planilha
    mesmo se a internet cair no meio.

    Também trava a porta: a coluna do credor NÃO está em `EDITAVEIS`, então
    ninguém reescreve nome de fornecedor pela tela comum."""
    from app.apps.analisesps import colunas, credores
    from app.apps.analisesps.db import consultar

    assert "credor" not in colunas.EDITAVEIS, (
        "o credor virou coluna do dia a dia — qualquer operador reescreve "
        "nome de fornecedor pela tela comum")

    semear([_sp_credor("1", "09444530000101", "TRI"),
            _sp_credor("2", "09444530000101", "TRIBUNAL DE JUSTIÇA DO CEARÁ")])

    import app.main as main
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    cliente = main.app.test_client()
    with cliente.session_transaction() as sessao:
        sessao["analisesps_perfil"] = "operador"
        sessao["analisesps_nome"] = "Marcelo"
    resposta = cliente.post("/analisesps/credores/aplicar", follow_redirects=True,
                            data={"documento": "09444530000101",
                                  "nome": "TRIBUNAL DE JUSTIÇA DO CEARÁ",
                                  "tipo-09444530000101": "COMECO"})
    assert resposta.status_code == 200

    assert consultar("SELECT count(*) FROM analisesps.sps "
                     " WHERE credor = 'TRI'")[0][0] == 0
    assert consultar("SELECT sp_id, coluna FROM analisesps.fila") == [
        ("1", "credor")], "não entrou na fila da planilha"
    assert consultar("SELECT count(*) FROM analisesps.log_alteracoes "
                     " WHERE coluna = 'credor'")[0][0] == 1


@pytest.mark.banco
def test_aplicar_duas_vezes_nao_reescreve_de_novo(banco_analisesps, monkeypatch):
    """Apertar o botão duas vezes é comum. A segunda não pode encher a fila com
    gravações que não mudam nada."""
    from app.apps.analisesps.db import conexao, consultar

    semear([_sp_credor("1", "09444530000101", "TRI")])
    import app.main as main
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    cliente = main.app.test_client()
    with cliente.session_transaction() as sessao:
        sessao["analisesps_perfil"] = "operador"
        sessao["analisesps_nome"] = "Marcelo"
    dados = {"documento": "09444530000101",
             "nome": "TRIBUNAL DE JUSTIÇA DO CEARÁ",
             "tipo-09444530000101": "COMECO"}
    cliente.post("/analisesps/credores/aplicar", data=dados)
    with conexao() as conn:      # a fila é esvaziada pelo processo separado
        conn.execute("DELETE FROM analisesps.fila")
        conn.commit()
    resposta = cliente.post("/analisesps/credores/aplicar", data=dados,
                            follow_redirects=True)

    assert consultar("SELECT count(*) FROM analisesps.fila")[0][0] == 0
    assert "já estava" in resposta.get_data(as_text=True)
