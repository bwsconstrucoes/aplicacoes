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
    encosta neles.

    A RESPOSTA INCLUI A CONFIRMAÇÃO DO OMIE, e isso passou a importar depois do
    defeito de 13/09/2026: "pode executar" não é "executou", e a tela só chama
    de baixa o que o Omie confirmou. Um dublê que devolvesse só o plano estaria
    imitando o ENSAIO, não a produção — e um teste que imita o defeito não
    protege de nada."""
    def responder(pedaco, nome):
        return {"modo_teste": False, "planos": [
            {"match": {"status": "localizado"}, "pode_executar": True,
             "acao": "baixar_omie_atualizar_pipefy_sheets",
             "responses": {"omie": [{"step": "baixar", "response": {"ok": True}}]},
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
        "modo_teste": False,
        "planos": [
            {"match": {"status": "localizado"}, "pode_executar": True,
             "acao": "baixar_omie_atualizar_pipefy_sheets",
             "responses": {"omie": [{"step": "baixar", "response": {"ok": True}}]},
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


# ---------------------------------------------------------------------------
# A CONCILIAÇÃO FISCAL ligada ao banco
#
# As regras puras estão em `test_analisesps_fiscal.py`. Aqui é o que só o banco
# alcança: a busca das notas candidatas (que é o que faz a tela abrir em vez de
# comparar tudo contra tudo) e o diário das decisões.
# ---------------------------------------------------------------------------
def _chave(cnpj, resto="550010000123456789012345"):
    return ("26" + "2609" + cnpj + resto + "0" * 44)[:44]


def _guardar_nota(chave, numero, valor, emitente_doc, status="Autorizada",
                  emissao="2026-06-18", destinatario="10656452007869"):
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.notas_fiscais "
            "(chave, emissao, numero, valor, status, emitente_doc, emitente, "
            " destinatario_doc) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (chave, emissao, numero, valor, status, emitente_doc,
             "FORNECEDOR", destinatario))
        conn.commit()


CREDOR_CNPJ = "29066773000152"


@pytest.mark.banco
def test_a_busca_de_notas_traz_SO_as_do_credor_daquela_pagina(banco_analisesps):
    """O QUE FAZ A TELA ABRIR. São 59 mil SPs e milhares de notas, e o banco
    tem um décimo de um núcleo: pontuar tudo contra tudo seriam centenas de
    milhões de comparações.

    A nota de um lançamento quase sempre foi emitida PELO CREDOR dele — então
    só se buscam as notas daqueles CNPJs."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181"), "77", 88.00, "11222333000181")

    lancamento = {"documento": "29.066.773/0001-52", "nf": ""}
    # Pelo caminho de verdade: o índice guarda a nota sob mais de uma chave de
    # busca (o CNPJ e o número), e quem tira a repetição é `_para_esta_sp`.
    achadas = fiscal._para_esta_sp(
        lancamento, fiscal.notas_candidatas([lancamento]))
    assert len(achadas) == 1
    assert achadas[0]["numero"] == "1430", "trouxe a nota de outro fornecedor"


@pytest.mark.banco
def test_a_mesma_nota_nao_aparece_duas_vezes_como_candidata(banco_analisesps):
    """Ela é indexada pelo CNPJ de quem emitiu E pelo número — as duas portas
    de busca. Quando as duas apontam para a mesma nota, ela é UMA candidata:
    contá-la duas vezes não mudaria a pontuação, mas apareceria como empate e
    a proposta seria segurada sem motivo."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    lancamento = {"documento": "29.066.773/0001-52", "nf": "1430"}
    achadas = fiscal._para_esta_sp(
        lancamento, fiscal.notas_candidatas([lancamento]))
    assert len(achadas) == 1


@pytest.mark.banco
def test_a_nota_da_FILIAL_e_encontrada_pelo_CNPJ_da_matriz(banco_analisesps):
    """A nota sai da filial que entregou, e o cadastro do credor quase sempre
    tem a matriz. Exigir os catorze dígitos perderia o caso comum."""
    from app.apps.analisesps import fiscal
    filial = "29066773000899"
    _guardar_nota(_chave(filial), "50", 100.00, filial)
    por_raiz = fiscal.notas_candidatas([
        {"documento": "29.066.773/0001-52", "nf": ""}])
    assert any(n["numero"] == "50" for notas in por_raiz.values() for n in notas)


@pytest.mark.banco
def test_a_nota_e_achada_pelo_NUMERO_quando_o_CNPJ_do_cadastro_esta_errado(
        banco_analisesps):
    """Acontece: quem lançou digitou o número da nota, mas o CPF/CNPJ do credor
    está errado no cadastro. Sem esta segunda porta, a nota certa nunca seria
    nem considerada."""
    from app.apps.analisesps import fiscal
    _guardar_nota(_chave("11222333000181"), "1430", 269.00, "11222333000181")
    por_raiz = fiscal.notas_candidatas([
        {"documento": "99.999.999/9999-99", "nf": "001430"}])
    assert any(n["numero"] == "1430" for notas in por_raiz.values() for n in notas)


@pytest.mark.banco
def test_a_conciliacao_propoe_a_correcao_quando_o_card_diz_que_nao_ha_nota(
        banco_analisesps):
    """O caso que o dono descreveu: "colocado algo não dedutível de uma coisa
    que não foi localizada naquele momento, mas que depois ela surge"."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    linha = fiscal.conciliar([{
        "id": "1409289353", "credor": "SERTAO CASA E CONSTRUCAO",
        "documento": "29.066.773/0001-52", "valor": "269,00", "nf": "1430",
        "vencimento": "10/07/2026", "tipo_despesa": "Ferramentas"}])[0]

    assert linha["grupo"] == fiscal.CORRECAO
    assert linha["propoe"] is True, "a proposta não veio marcada"
    assert linha["documentacao"] == "NF-e (Mercadoria)"
    assert linha["confianca"] >= fiscal.CONFIANCA_PARA_PROPOR


@pytest.mark.banco
def test_o_que_NUNCA_tem_nota_eletronica_nao_e_conciliado(banco_analisesps):
    """Herdado do script da planilha, que já acertava nisto: procurar par para
    uma apólice ou um contrato só produziria ruído e faria a pessoa desconfiar
    do resto da tela."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise "
            "(sp_id, situacao, documentacao) VALUES ('1', 'ESCRITA', 'Seguros')")
        conn.commit()

    linha = fiscal.conciliar([{
        "id": "1", "credor": "SEGURADORA", "documento": "29.066.773/0001-52",
        "valor": "269,00", "nf": "1430", "tipo_despesa": "Seguros"}])[0]
    assert linha["nota"] is None, "procurou nota para uma apólice"


@pytest.mark.banco
def test_a_decisao_fica_no_DIARIO_e_ainda_NAO_no_card(banco_analisesps):
    """Duas coisas diferentes, e a diferença é o que permite tentar de novo
    quando o Pipefy recusa: `decidida_em` é quando alguém escolheu,
    `escrita_em` é quando o card aceitou."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import consultar_um

    fiscal.guardar_decisao("1409289353", "NF-e (Mercadoria)",
                           _chave(CREDOR_CNPJ), "achei a nota", 90, "Marcelo")

    linha = consultar_um(
        "SELECT situacao, documentacao, dedutivel, decidida_por, "
        "       decidida_em IS NOT NULL, escrita_em IS NULL "
        "  FROM analisesps.sp_fiscal_analise WHERE sp_id = '1409289353'")
    assert linha[0] == fiscal.CONFIRMADA
    assert linha[1] == "NF-e (Mercadoria)"
    assert linha[2] is True, "NF-e é dedutível"
    assert linha[3] == "Marcelo"
    assert linha[4] is True and linha[5] is True, (
        "decidida sim, escrita no card ainda não")
    assert [d["sp_id"] for d in fiscal.a_escrever_no_card()] == ["1409289353"]


@pytest.mark.banco
def test_decidir_de_novo_devolve_a_SP_para_a_fila_de_escrita(banco_analisesps):
    """Quem muda de ideia depois de o card já ter sido escrito precisa que a
    correção vá para o card também. Sem isto, a segunda decisão ficaria só
    aqui dentro e o Pipefy continuaria com a primeira."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import conexao

    fiscal.guardar_decisao("1", "NF-e (Mercadoria)", "", "", 90, "Marcelo")
    with conexao() as conn:       # finge que o card aceitou
        conn.execute("UPDATE analisesps.sp_fiscal_analise "
                     "SET escrita_em = now() WHERE sp_id = '1'")
        conn.commit()
    assert fiscal.a_escrever_no_card() == []

    fiscal.guardar_decisao("1", "Não Dedutível", "", "mudei de ideia", 0, "Marcelo")
    assert [d["documentacao"] for d in fiscal.a_escrever_no_card()] == [
        "Não Dedutível"]


@pytest.mark.banco
def test_a_tela_fiscal_abre_e_separa_as_duas_pilhas(banco_analisesps, monkeypatch):
    """O que o dono vê: os números do alto por grupo, a proposta já marcada e
    a dúvida desmarcada."""
    import re

    from app.apps.analisesps import colunas, sincronizacao

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    registros = []
    for sp_id, valor, nf in (("1409289353", "269,00", "1430"),
                             ("1409289354", "500,00", "")):
        r = {c: "" for c in colunas.CHAVES}
        r.update({"id": sp_id, "credor": "SERTAO CASA E CONSTRUCAO",
                  "documento": "29.066.773/0001-52", "valor": valor, "nf": nf,
                  "tipo_despesa": "Ferramentas"})
        registros.append(r)
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        sincronizacao.gravar_registros(conn, registros)
        sincronizacao._anotar_a_base_em_dia(conn)

    import app.main as main
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    cliente = main.app.test_client()
    with cliente.session_transaction() as sessao:
        sessao["analisesps_perfil"] = "operador"
        sessao["analisesps_nome"] = "Marcelo"
    html = cliente.get("/analisesps/fiscal").get_data(as_text=True)

    assert "Proposta de correção" in html
    assert "NF-e (Mercadoria)" in html
    # UMA marcada (a proposta), e a outra não: são as duas pilhas.
    marcadas = len(re.findall(r'class="fiscal-marca"[^>]*checked', html))
    assert marcadas == 1, f"vieram {marcadas} marcadas; deviam ser 1"


@pytest.mark.banco
def test_categoria_fora_da_lista_do_Pipefy_e_recusada(banco_analisesps, monkeypatch):
    """O Pipefy RECUSA O CARD INTEIRO quando o texto não é uma das 22 opções.
    Um valor inventado aqui não erraria uma SP: derrubaria a gravação do lote."""
    import app.main as main
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    cliente = main.app.test_client()
    with cliente.session_transaction() as sessao:
        sessao["analisesps_perfil"] = "operador"
        sessao["analisesps_nome"] = "Marcelo"
    resposta = cliente.post("/analisesps/api/fiscal/confirmar", json={
        "itens": [{"sp": "1", "documentacao": "CATEGORIA INVENTADA"}]})
    dados = resposta.get_json()
    assert dados["gravadas"] == 0
    assert "categoria desconhecida" in " ".join(dados["recusadas"])


@pytest.mark.banco
def test_o_card_que_ACEITOU_sai_da_fila_de_escrita(banco_analisesps, monkeypatch):
    """`escrita_em` é o que separa "decidido" de "gravado"."""
    from app.apps.analisesps import fiscal, pipefy

    fiscal.guardar_decisao("1", "NF-e (Mercadoria)", _chave(CREDOR_CNPJ),
                           "achei", 90, "Marcelo")
    monkeypatch.setattr(pipefy, "atualizar_documentacao_fiscal",
                        lambda itens, token=None: {"ok": ["1"], "falhas": {}})

    assert fiscal.escrever_nos_cards() == {"escritas": 1, "falhas": 0,
                                           "pendentes": 0}
    assert fiscal.a_escrever_no_card() == []
    from app.apps.analisesps.db import consultar_um
    assert consultar_um("SELECT situacao FROM analisesps.sp_fiscal_analise "
                        " WHERE sp_id = '1'")[0] == fiscal.ESCRITA


@pytest.mark.banco
def test_o_card_que_RECUSOU_continua_na_fila_com_o_motivo(banco_analisesps,
                                                          monkeypatch):
    """A decisão não pode se perder porque a API deu erro — ela volta na
    próxima leva. E o motivo fica gravado: "o Pipefy não confirmou" é
    informação; "sumiu" não é."""
    from app.apps.analisesps import fiscal, pipefy

    fiscal.guardar_decisao("1", "Seguros", "", "", 0, "Marcelo")
    monkeypatch.setattr(
        pipefy, "atualizar_documentacao_fiscal",
        lambda itens, token=None: {"ok": [], "falhas": {"1": "a rede caiu"}})

    resultado = fiscal.escrever_nos_cards()
    assert resultado["escritas"] == 0 and resultado["falhas"] == 1
    pendente = fiscal.a_escrever_no_card()[0]
    assert pendente["sp_id"] == "1"
    assert pendente["erro_anterior"] == "a rede caiu", (
        "a SP voltou para a fila sem dizer por que falhou")


@pytest.mark.banco
def test_sem_nada_na_fila_a_gravacao_nao_fala_com_o_Pipefy(banco_analisesps,
                                                           monkeypatch):
    """Uma rodada vazia não pode gastar uma chamada de API — a rotina roda
    junto com a atualização do dia."""
    from app.apps.analisesps import fiscal, pipefy

    def nunca(*a, **k):
        raise AssertionError("falou com o Pipefy sem ter o que gravar")

    monkeypatch.setattr(pipefy, "atualizar_documentacao_fiscal", nunca)
    assert fiscal.escrever_nos_cards() == {"escritas": 0, "falhas": 0,
                                           "pendentes": 0}


# ---------------------------------------------------------------------------
# A SEGUNDA VISÃO — nota → lançamento
#
# É ela que fecha com a contabilidade. Nas palavras do dono: *"se tem uma nota
# emitida, tem uma despesa para estar associada"*. Nota órfã é problema fiscal,
# e hoje ninguém a enxerga.
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_a_nota_sem_lancamento_aparece_e_a_conciliada_nao(banco_analisesps):
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181"), "77", 88.00, "11222333000181")
    fiscal.guardar_decisao("1", "NF-e (Mercadoria)", _chave(CREDOR_CNPJ),
                           "", 90, "Marcelo")

    notas, total = fiscal.notas_orfas()
    assert total == 1
    assert [n["numero"] for n in notas] == ["77"]


@pytest.mark.banco
def test_a_chave_gravada_com_pontuacao_ainda_TIRA_a_nota_da_lista(banco_analisesps):
    """A chave chega de mil jeitos — com espaço, com ponto. Se a comparação
    fosse literal, a mesma nota voltaria a aparecer como órfã depois de já
    conciliada, e ninguém entenderia por quê."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import conexao

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise (sp_id, situacao, chave) "
            "VALUES ('1', 'ESCRITA', ?)", (chave[:22] + " " + chave[22:],))
        conn.commit()

    assert fiscal.notas_orfas()[1] == 0


@pytest.mark.banco
def test_nota_CANCELADA_sem_lancamento_nao_e_achado(banco_analisesps):
    """Nota cancelada sem despesa é o esperado, não um problema. Listá-la faria
    esta visão nascer cheia de ruído."""
    from app.apps.analisesps import fiscal
    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ,
                  status="Cancelada")
    assert fiscal.notas_orfas()[1] == 0


@pytest.mark.banco
def test_a_nota_orfa_diz_POR_ONDE_COMECAR(banco_analisesps):
    """Não basta dizer "esta nota está órfã": quem vai resolver precisa das
    SPs candidatas. E a de MESMO VALOR vem na frente — é a que quase sempre
    é a certa."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1430", 269.00, CREDOR_CNPJ)
    semear([sp("1", documento="29.066.773/0001-52", credor="SERTAO",
               valor="999,00", valor_num=999.00),
            sp("2", documento="29066773000899", credor="SERTAO FILIAL",
               valor="269,00", valor_num=269.00)])

    nota = fiscal.notas_orfas()[0][0]
    candidatas = fiscal.sps_possiveis_da_nota(nota)
    assert [c["id"] for c in candidatas] == ["2", "1"], (
        "a SP de mesmo valor tinha de vir na frente")


@pytest.mark.banco
def test_a_categoria_da_nota_orfa_sai_de_dentro_da_chave(banco_analisesps):
    """Saber que a órfã é um CT-e já diz onde procurar a despesa — e a
    categoria é CERTEZA, porque vem dos dígitos 21-22 da própria chave."""
    from app.apps.analisesps import fiscal
    _guardar_nota(_chave("11222333000181", "570010000999888777666555"),
                  "77", 88.00, "11222333000181")
    assert fiscal.notas_orfas()[0][0]["categoria"] == "CT-e (Frete)"


# ---------------------------------------------------------------------------
# A FILA DA IA — ela não roda sozinha, e não atropela decisão de gente
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_a_fila_da_IA_mora_no_BANCO_e_nao_em_memoria(banco_analisesps):
    """O processo separado pode ser reiniciado no meio. Quem escolheu trinta
    SPs não pode perder a escolha por causa disso."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import consultar

    assert fiscal.por_na_fila_da_ia(["1", "2", "3"], "Marcelo") == 3
    na_fila = consultar(
        "SELECT sp_id FROM analisesps.sp_fiscal_analise "
        " WHERE situacao = ? ORDER BY sp_id", (fiscal.NA_FILA_IA,))
    assert [l[0] for l in na_fila] == ["1", "2", "3"]


@pytest.mark.banco
def test_a_IA_nao_atropela_o_que_uma_PESSOA_ja_decidiu(banco_analisesps):
    """Mandar a IA reescrever por cima do que alguém decidiu é o contrário de
    "a IA propõe, nunca decide". Quem quiser refazer desfaz primeiro."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import consultar_um

    fiscal.guardar_decisao("1", "Seguros", "", "eu decidi", 0, "Marcelo")
    assert fiscal.por_na_fila_da_ia(["1", "2"], "Marcelo") == 1, (
        "a SP já decidida entrou na fila da IA")
    assert consultar_um("SELECT situacao, documentacao "
                        "  FROM analisesps.sp_fiscal_analise WHERE sp_id = '1'"
                        ) == (fiscal.CONFIRMADA, "Seguros")


@pytest.mark.banco
def test_a_IA_grava_como_PROPOSTA_e_nunca_como_confirmada(banco_analisesps,
                                                          monkeypatch):
    """A IA propõe; quem confirma é gente. Se ela gravasse como confirmada, a
    leitura de um PDF torto iria direto para o card."""
    from app.apps.analisesps import colunas, fiscal, fiscal_ia, sincronizacao
    from app.apps.analisesps.db import conexao, consultar_um

    registro = {c: "" for c in colunas.CHAVES}
    registro.update({"id": "1", "credor": "SERTAO", "valor": "269,00",
                     "documento": "29.066.773/0001-52",
                     "anexo_link": "https://exemplo/anexo.pdf"})
    with conexao() as conn:
        sincronizacao.gravar_registros(conn, [registro])

    monkeypatch.setattr(fiscal_ia, "ler_anexo", lambda sp: {
        "tipo_documento": "NFE", "confianca": "ALTA",
        "chave_acesso": _chave(CREDOR_CNPJ), "emitente_documento": CREDOR_CNPJ})

    assert fiscal_ia.analisar(["1"])["lidas"] == 1
    linha = consultar_um(
        "SELECT situacao, documentacao, origem, dedutivel "
        "  FROM analisesps.sp_fiscal_analise WHERE sp_id = '1'")
    assert linha[0] == fiscal.PROPOSTA, "a IA gravou como decisão tomada"
    assert linha[1] == "NF-e (Mercadoria)"
    assert linha[2] == "IA", "não ficou registrado que a origem foi a IA"
    assert linha[3] is True
    assert fiscal.a_escrever_no_card() == [], (
        "a leitura da IA foi direto para a fila do card sem ninguém confirmar")


@pytest.mark.banco
def test_uma_leitura_que_falha_nao_derruba_as_outras(banco_analisesps, monkeypatch):
    """Trinta anexos, um corrompido. As vinte e nove boas têm de ficar."""
    from app.apps.analisesps import colunas, fiscal_ia, sincronizacao
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        for sp_id in ("1", "2"):
            registro = {c: "" for c in colunas.CHAVES}
            registro.update({"id": sp_id, "credor": "SERTAO",
                             "documento": "29.066.773/0001-52",
                             "anexo_link": "https://exemplo/anexo.pdf"})
            sincronizacao.gravar_registros(conn, [registro])

    def instavel(sp):
        if sp["id"] == "1":
            raise RuntimeError("o PDF veio corrompido")
        return {"tipo_documento": "GUIA", "confianca": "ALTA",
                "chave_acesso": "", "emitente_documento": ""}

    monkeypatch.setattr(fiscal_ia, "ler_anexo", instavel)
    resultado = fiscal_ia.analisar(["1", "2"])
    assert resultado["lidas"] == 1
    assert "corrompido" in resultado["falhas"]["1"]


# ---------------------------------------------------------------------------
# A IMPORTAÇÃO DO RELATÓRIO DE NOTAS — três números, e um deles é notícia
# ---------------------------------------------------------------------------
def _importar(monkeypatch, linhas):
    """Roda a importação do relatório do FSist com a aba dublada."""
    from app.apps.analisesps import sincronizacao

    class AbaFalsa:
        def get_all_values(self):
            return linhas

    monkeypatch.setattr(sincronizacao, "_aba",
                        lambda planilha, nome: AbaFalsa())
    return sincronizacao.sincronizar_notas_fiscais()


CABECALHO_NOTAS = ["Chave de Acesso", "Emissão", "Número", "Valor", "Status",
                   "CNPJ Emitente", "Emitente"]


def _linha_nota(chave, numero="1430", valor="269,00", status="Autorizada"):
    return [chave, "18/06/2026", numero, valor, status,
            "29.066.773/0001-52", "SERTAO CASA E CONSTRUCAO"]


@pytest.mark.banco
def test_a_importacao_separa_NOVAS_MUDARAM_e_JA_TINHA(banco_analisesps,
                                                      monkeypatch):
    """O dono pediu exatamente estes três: *"vai dizer quantos importou, que
    conseguiu, que já tinha, que não tinha"*."""
    chave = _chave(CREDOR_CNPJ)

    primeira = _importar(monkeypatch, [CABECALHO_NOTAS, _linha_nota(chave)])
    assert (primeira["novas"], primeira["mudaram"], primeira["ja_tinha"]) == (1, 0, 0)

    igual = _importar(monkeypatch, [CABECALHO_NOTAS, _linha_nota(chave)])
    assert (igual["novas"], igual["mudaram"], igual["ja_tinha"]) == (0, 0, 1), (
        "reimportar a mesma nota contou como movimento")


@pytest.mark.banco
def test_a_nota_que_volta_CANCELADA_aparece_como_MUDOU(banco_analisesps,
                                                       monkeypatch):
    """É O NÚMERO QUE INTERESSA, e ele não existia antes: uma nota que volta no
    relatório cancelada pode ser despesa já paga contra documento que não
    existe mais. Antes ela se escondia no meio das "atualizadas", que contavam
    as inalteradas junto."""
    chave = _chave(CREDOR_CNPJ)
    _importar(monkeypatch, [CABECALHO_NOTAS, _linha_nota(chave)])

    depois = _importar(monkeypatch, [
        CABECALHO_NOTAS, _linha_nota(chave, status="Cancelada")])
    assert depois["mudaram"] == 1 and depois["novas"] == 0
    assert depois["ja_tinha"] == 0

    from app.apps.analisesps.db import consultar_um
    assert consultar_um("SELECT status FROM analisesps.notas_fiscais "
                        " WHERE chave = ?", (chave,))[0] == "Cancelada"


@pytest.mark.banco
def test_a_mesma_nota_importada_duas_vezes_nao_vira_duas_linhas(banco_analisesps,
                                                                monkeypatch):
    """"Se aquela informação de nota já estiver dentro, você vai ignorar."""
    chave = _chave(CREDOR_CNPJ)
    for _ in range(3):
        _importar(monkeypatch, [CABECALHO_NOTAS, _linha_nota(chave)])
    from app.apps.analisesps.db import consultar_um
    assert consultar_um("SELECT count(*) FROM analisesps.notas_fiscais")[0] == 1


@pytest.mark.banco
def test_linha_sem_chave_de_44_digitos_e_contada_como_ignorada(banco_analisesps,
                                                               monkeypatch):
    """Total de rodapé, linha em branco, chave truncada. Contar quantas foram
    ignoradas é o que permite desconfiar de um relatório torto."""
    resultado = _importar(monkeypatch, [
        CABECALHO_NOTAS, _linha_nota(_chave(CREDOR_CNPJ)),
        _linha_nota("123"), ["", "", "", "", "", "", ""]])
    assert resultado["novas"] == 1
    assert resultado["ignoradas"] == 2


def test_a_importacao_das_notas_e_CHAMADA_pela_atualizacao():
    """O DEFEITO QUE ESTE TESTE GUARDA: a importação existia, estava testada, e
    NINGUÉM A CHAMAVA. A tabela de notas ficaria vazia para sempre e a
    conciliação fiscal não teria contra o que casar — uma tela inteira
    funcionando sobre nada. Achado em 12/09/2026 procurando quem importava o
    relatório."""
    from pathlib import Path
    fonte = Path("app/apps/analisesps/tarefas.py").read_text(encoding="utf-8")
    assert "sincronizar_notas_fiscais" in fonte, (
        "ninguém importa o relatório do FSist — a conciliação fica sem notas")


# ---------------------------------------------------------------------------
# O ARQUIVO PERMANENTE DE NOTAS
#
# É A BASE DO PLANO DO DONO, dito por ele em 12/09/2026: *"o passado é o que eu
# tenho, que eu já baixei de relatório lá. O relatório mais antigo que eu tenho
# a gente vai importar pra dentro do Análise de SPs, e deixar lá dentro; e a
# partir de então você vai começar a fazer o download."*
#
# Ou seja: a aba do FSist é uma JANELA que ele troca, e a tabela aqui é o
# ARQUIVO que só cresce. Se a importação apagasse o que não está no relatório
# do dia, o histórico dele se perderia na primeira colagem — e é histórico que
# não dá para recuperar, porque nem o FSist nem a Receita guardam o passado.
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_relatorio_novo_NAO_apaga_as_notas_do_relatorio_anterior(banco_analisesps,
                                                                 monkeypatch):
    """O teste que sustenta o plano inteiro. Ele cola o relatório de janeiro,
    depois o de fevereiro na MESMA aba — e as de janeiro têm de continuar
    aqui."""
    antiga = _chave(CREDOR_CNPJ, "550010000111111111111111")
    nova = _chave(CREDOR_CNPJ, "550010000222222222222222")

    _importar(monkeypatch, [CABECALHO_NOTAS, _linha_nota(antiga, numero="100")])
    # A aba é trocada inteira pelo relatório seguinte: a nota antiga some DELA.
    resultado = _importar(monkeypatch, [CABECALHO_NOTAS,
                                        _linha_nota(nova, numero="200")])

    from app.apps.analisesps.db import consultar
    guardadas = sorted(l[0] for l in consultar(
        "SELECT numero FROM analisesps.notas_fiscais"))
    assert guardadas == ["100", "200"], (
        "a importação apagou o histórico que não está no relatório do dia")
    assert resultado["novas"] == 1


@pytest.mark.banco
def test_nenhum_caminho_do_modulo_apaga_nota_fiscal(banco_analisesps):
    """A trava, escrita onde não depende de alguém lembrar: uma nota só sai
    daqui se alguém escrever um DELETE de propósito. O histórico do dono não
    pode depender de ninguém lembrar disso."""
    from pathlib import Path

    pasta = Path("app/apps/analisesps")
    arquivos = list(pasta.glob("*.py")) + list(pasta.glob("migracoes/*.sql"))
    for caminho in arquivos:
        texto = caminho.read_text(encoding="utf-8").lower()
        assert "delete from analisesps.notas_fiscais" not in texto, caminho
        assert "truncate" not in texto, caminho


# ---------------------------------------------------------------------------
# A BUSCA NA RECEITA, com banco de verdade
#
# A conversa com a Receita é dublada — nenhum teste liga para ela. O que se
# exercita é o laço: onde parou, quando parar, e a nota chegando na tabela.
# ---------------------------------------------------------------------------
def _resposta_sefaz(documentos=(), codigo="138", ultimo="10", maior="10"):
    import base64 as _b64
    import gzip as _gzip
    import io as _io

    def zipar(xml):
        saco = _io.BytesIO()
        with _gzip.GzipFile(fileobj=saco, mode="wb") as z:
            z.write(xml.encode("utf-8"))
        return _b64.b64encode(saco.getvalue()).decode("ascii")

    zips = "".join(f"<docZip>{zipar(d)}</docZip>" for d in documentos)
    return (f"<retDistDFeInt><cStat>{codigo}</cStat>"
            f"<xMotivo>ok</xMotivo><ultNSU>{ultimo}</ultNSU>"
            f"<maxNSU>{maior}</maxNSU>{zips}</retDistDFeInt>")


def _resumo(chave, valor="269.00", situacao="1"):
    return (f"<resNFe><chNFe>{chave}</chNFe><CNPJ>{CREDOR_CNPJ}</CNPJ>"
            f"<xNome>SERTAO</xNome><dhEmi>2026-06-18T10:00:00-03:00</dhEmi>"
            f"<vNF>{valor}</vNF><cSitNFe>{situacao}</cSitNFe></resNFe>")


@pytest.mark.banco
def test_a_nota_buscada_na_Receita_chega_na_tabela(banco_analisesps, monkeypatch):
    from app.apps.analisesps import sefaz
    from app.apps.analisesps.db import consultar_um

    chave = _chave(CREDOR_CNPJ)
    monkeypatch.setattr(sefaz, "_consultar_nfe", lambda cnpj, nsu:
                        sefaz._ler_resposta(_resposta_sefaz(
                            [_resumo(chave)], ultimo="10", maior="10")))

    resultado = sefaz.buscar_um("10.656.452/0078-69", sefaz.NFE)
    assert resultado["trazidas"] == 1
    assert consultar_um("SELECT emitente_doc, status FROM "
                        " analisesps.notas_fiscais WHERE chave = ?",
                        (chave,)) == (CREDOR_CNPJ, "Autorizada")


@pytest.mark.banco
def test_o_ponteiro_avanca_e_a_proxima_rodada_comeca_dali(banco_analisesps,
                                                          monkeypatch):
    """É o que impede reler tudo a cada rodada — e reler é o caminho curto
    para a Receita bloquear por consulta demais."""
    from app.apps.analisesps import sefaz

    pedidos = []

    def falso(cnpj, nsu):
        pedidos.append(nsu)
        return sefaz._ler_resposta(_resposta_sefaz(
            [_resumo(_chave(CREDOR_CNPJ))], ultimo="25", maior="25"))

    monkeypatch.setattr(sefaz, "_consultar_nfe", falso)
    sefaz.buscar_um("10656452007869", sefaz.NFE)
    assert sefaz.ponteiro("10656452007869", sefaz.NFE)["ultimo_nsu"] == \
        "000000000000025"

    sefaz.buscar_um("10656452007869", sefaz.NFE)
    assert pedidos[-1] == "000000000000025", (
        "a segunda rodada recomeçou do zero")


@pytest.mark.banco
def test_o_ponteiro_NUNCA_recua(banco_analisesps, monkeypatch):
    """Uma resposta vazia traz NSU zero. Se ela fizesse o ponteiro voltar, a
    rodada seguinte releria meses de documentos."""
    from app.apps.analisesps import sefaz

    sefaz.gravar_ponteiro("10656452007869", sefaz.NFE, "500", "500")
    sefaz.gravar_ponteiro("10656452007869", sefaz.NFE, "0", "0", "vazio")
    assert sefaz.ponteiro("10656452007869", sefaz.NFE)["ultimo_nsu"] == \
        "000000000000500"


@pytest.mark.banco
def test_a_busca_para_quando_a_Receita_diz_que_nao_ha_mais(banco_analisesps,
                                                           monkeypatch):
    """Insistir depois do "não há nada novo" é o caminho curto para o bloqueio
    por consulta demais. O teto de lotes é rede, não critério."""
    from app.apps.analisesps import sefaz

    chamadas = [0]

    def falso(cnpj, nsu):
        chamadas[0] += 1
        return sefaz._ler_resposta(_resposta_sefaz(codigo="137", ultimo="0"))

    monkeypatch.setattr(sefaz, "_consultar_nfe", falso)
    resultado = sefaz.buscar_um("10656452007869", sefaz.NFE)
    assert chamadas[0] == 1, f"insistiu {chamadas[0]} vezes depois do 'nada novo'"
    assert resultado["trazidas"] == 0
    assert "Nenhuma nota nova" in sefaz.ponteiro(
        "10656452007869", sefaz.NFE)["ultimo_recado"]


@pytest.mark.banco
def test_a_busca_para_quando_o_ponteiro_nao_anda(banco_analisesps, monkeypatch):
    """A terceira forma de saber que acabou, e a que protege de laço infinito:
    a Receita responde "há mais" mas devolve o mesmo NSU."""
    from app.apps.analisesps import sefaz

    chamadas = [0]

    def travado(cnpj, nsu):
        chamadas[0] += 1
        return sefaz._ler_resposta(_resposta_sefaz(codigo="138", ultimo="0",
                                                   maior="999"))

    monkeypatch.setattr(sefaz, "_consultar_nfe", travado)
    sefaz.buscar_um("10656452007869", sefaz.NFE)
    assert chamadas[0] == 1, f"ficou em laço: {chamadas[0]} consultas"


@pytest.mark.banco
def test_falha_de_rede_vira_RECADO_e_nao_queda(banco_analisesps, monkeypatch):
    """"Consumo indevido" e "certificado vencido" chegam os dois como falha, e
    pedem coisas completamente diferentes. O motivo fica gravado para a tela
    poder dizer qual é."""
    from app.apps.analisesps import sefaz

    def cai(cnpj, nsu):
        raise RuntimeError("certificado vencido")

    monkeypatch.setattr(sefaz, "_consultar_nfe", cai)
    resultado = sefaz.buscar_um("10656452007869", sefaz.NFE)
    assert resultado["trazidas"] == 0 and "vencido" in resultado["erro"]
    assert "vencido" in sefaz.ponteiro("10656452007869",
                                       sefaz.NFE)["ultimo_recado"]


@pytest.mark.banco
def test_NFe_e_CTe_tem_ponteiros_SEPARADOS(banco_analisesps):
    """São dois serviços da Receita, cada um com a sua contagem. Um ponteiro só
    faria um sobrescrever o outro e perder notas em silêncio."""
    from app.apps.analisesps import sefaz

    sefaz.gravar_ponteiro("10656452007869", sefaz.NFE, "100", "100")
    sefaz.gravar_ponteiro("10656452007869", sefaz.CTE, "7", "7")
    assert sefaz.ponteiro("10656452007869", sefaz.NFE)["ultimo_nsu"] == \
        "000000000000100"
    assert sefaz.ponteiro("10656452007869", sefaz.CTE)["ultimo_nsu"] == \
        "000000000000007"


@pytest.mark.banco
def test_a_nota_da_Receita_e_a_do_FSist_nao_viram_duas_linhas(banco_analisesps,
                                                              monkeypatch):
    """A MESMA nota chega pelas duas portas. Se virasse duas linhas, a
    conciliação veria duas candidatas idênticas, chamaria de empate e seguraria
    a proposta — em toda nota."""
    from app.apps.analisesps import sefaz
    from app.apps.analisesps.db import consultar_um

    chave = _chave(CREDOR_CNPJ)
    _importar(monkeypatch, [CABECALHO_NOTAS, _linha_nota(chave)])
    monkeypatch.setattr(sefaz, "_consultar_nfe", lambda cnpj, nsu:
                        sefaz._ler_resposta(_resposta_sefaz([_resumo(chave)])))
    sefaz.buscar_um("10656452007869", sefaz.NFE)

    assert consultar_um("SELECT count(*) FROM analisesps.notas_fiscais")[0] == 1


@pytest.mark.banco
def test_a_falha_do_CTe_nao_leva_a_busca_de_NFe_junto(banco_analisesps,
                                                      monkeypatch):
    """O caminho de CT-e nunca foi exercitado contra o serviço de verdade. Ele
    não pode derrubar a busca de NF-e, que é a maior parte do volume."""
    from app.apps.analisesps import sefaz

    from app.apps.analisesps import certificados
    monkeypatch.setattr(certificados, "cofre_configurado", lambda: True)
    monkeypatch.setattr(certificados, "cnpjs_ativos", lambda: ["10656452007869"])
    monkeypatch.setattr(sefaz, "_consultar_nfe", lambda cnpj, nsu:
                        sefaz._ler_resposta(_resposta_sefaz(
                            [_resumo(_chave(CREDOR_CNPJ))])))

    def cte_quebrado(cnpj, nsu):
        raise RuntimeError("o CT-e recusou")

    monkeypatch.setattr(sefaz, "_consultar_cte", cte_quebrado)
    resultado = sefaz.buscar_tudo()
    assert resultado["trazidas"] == 1, "a NF-e se perdeu junto com o CT-e"
    assert any("recusou" in (p.get("erro") or "") for p in resultado["por_cnpj"])


# ---------------------------------------------------------------------------
# O COFRE DOS CERTIFICADOS, com banco de verdade
# ---------------------------------------------------------------------------
def _pfx(cnpj="10656452007869", senha="senha-de-teste", vence=None):
    import datetime as _dt

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    vence = vence or _dt.datetime(2027, 1, 1)
    # O "vale a partir de" tem de ser ANTES do "vale até" — inclusive no
    # certificado já vencido que um dos testes usa, que é o caso real de quem
    # esqueceu de renovar.
    comeca = min(_dt.datetime(2026, 1, 1), vence - _dt.timedelta(days=365))
    chave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    titular = x509.Name([x509.NameAttribute(
        NameOID.COMMON_NAME, f"BWS CONSTRUCOES LTDA:{cnpj}")])
    cert = (x509.CertificateBuilder().subject_name(titular).issuer_name(titular)
            .public_key(chave.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(comeca)
            .not_valid_after(vence).sign(chave, hashes.SHA256()))
    return pkcs12.serialize_key_and_certificates(
        b"t", chave, cert, None,
        serialization.BestAvailableEncryption(senha.encode()))


@pytest.mark.banco
def test_o_certificado_vai_e_volta_inteiro_pelo_cofre(banco_analisesps,
                                                      monkeypatch):
    """Ele tem de sair do banco byte a byte igual ao que entrou — um
    certificado corrompido é recusado pela Receita sem dizer por quê."""
    from app.apps.analisesps import certificados

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    original = _pfx()
    certificados.guardar(original, "senha-de-teste", "BWS", "Marcelo")

    conteudo, senha = certificados.abrir_para_uso("10.656.452/0078-69")
    assert conteudo == original
    assert senha == "senha-de-teste"


@pytest.mark.banco
def test_o_banco_guarda_CIFRADO_e_nao_o_arquivo(banco_analisesps, monkeypatch):
    """A prova de que o vazamento do banco não entrega o certificado."""
    from app.apps.analisesps import certificados
    from app.apps.analisesps.db import consultar_um

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    original = _pfx()
    certificados.guardar(original, "senha-de-teste", "BWS", "Marcelo")

    guardado = consultar_um(
        "SELECT arquivo, senha FROM analisesps.certificados")
    assert bytes(guardado[0]) != original, "o arquivo está aberto no banco"
    assert b"senha-de-teste" not in bytes(guardado[1])


@pytest.mark.banco
def test_subir_de_novo_TROCA_o_certificado_daquele_CNPJ(banco_analisesps,
                                                        monkeypatch):
    """É como se faz a troca anual: sobe o novo por cima. Duas linhas para o
    mesmo CNPJ fariam a busca usar o vencido metade das vezes."""
    import datetime as _dt

    from app.apps.analisesps import certificados
    from app.apps.analisesps.db import consultar_um

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    certificados.guardar(_pfx(vence=_dt.datetime(2027, 1, 1)),
                         "senha-de-teste", "antigo", "Marcelo")
    certificados.guardar(_pfx(vence=_dt.datetime(2028, 6, 30)),
                         "senha-de-teste", "novo", "Marcelo")

    assert consultar_um("SELECT count(*) FROM analisesps.certificados")[0] == 1
    assert certificados.listar()[0]["valido_ate"] == _dt.date(2028, 6, 30)


@pytest.mark.banco
def test_o_CNPJ_com_certificado_VENCIDO_nao_e_consultado(banco_analisesps,
                                                         monkeypatch):
    """Consultar a Receita com certificado vencido só produz recusa — e a
    recusa GASTA A COTA de consultas, que é limitada."""
    import datetime as _dt

    from app.apps.analisesps import certificados

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    certificados.guardar(_pfx(vence=_dt.datetime(2020, 1, 1)),
                         "senha-de-teste", "vencido", "Marcelo")

    assert certificados.cnpjs_ativos() == []
    assert certificados.listar()[0]["vencido"] is True


@pytest.mark.banco
def test_a_lista_de_CNPJs_da_busca_SAI_do_cofre(banco_analisesps, monkeypatch):
    """Duas listas — uma de CNPJs e outra de certificados — divergiriam no dia
    em que alguém subisse um certificado e esquecesse de acrescentar o CNPJ, e
    a busca ficaria sem rodar para aquela empresa sem ninguém entender."""
    from app.apps.analisesps import certificados, sefaz

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    assert sefaz.cnpjs_vigiados() == []
    assert sefaz.configurado() is False

    certificados.guardar(_pfx(), "senha-de-teste", "BWS", "Marcelo")
    assert sefaz.cnpjs_vigiados() == ["10656452007869"]
    assert sefaz.configurado() is True


@pytest.mark.banco
def test_remover_apaga_de_verdade(banco_analisesps, monkeypatch):
    """Guardar credencial "desativada" é guardar credencial — e o motivo de
    tirar costuma ser justamente que ela não deveria mais existir."""
    from app.apps.analisesps import certificados
    from app.apps.analisesps.db import consultar_um

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    certificados.guardar(_pfx(), "senha-de-teste", "BWS", "Marcelo")
    assert certificados.remover("10.656.452/0078-69", "Marcelo") is True
    assert consultar_um("SELECT count(*) FROM analisesps.certificados")[0] == 0
    assert certificados.remover("10656452007869", "Marcelo") is False


@pytest.mark.banco
def test_a_tela_de_configuracoes_mostra_o_certificado_sem_o_conteudo(
        banco_analisesps, monkeypatch):
    from app.apps.analisesps import certificados

    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", "op")
    certificados.guardar(_pfx(), "senha-de-teste", "BWS Nordeste", "Marcelo")

    import app.main as main
    cliente = main.app.test_client()
    with cliente.session_transaction() as sessao:
        sessao["analisesps_perfil"] = "operador"
        sessao["analisesps_nome"] = "Marcelo"
    html = cliente.get("/analisesps/configuracoes").get_data(as_text=True)

    assert "BWS Nordeste" in html
    assert "10656452007869" in html
    assert "01/01/2027" in html
    assert "senha-de-teste" not in html, "a senha vazou para a tela"


# ---------------------------------------------------------------------------
# O RECORTE E OS TOTALIZADORES DA DOCUMENTAÇÃO FISCAL — 13/09/2026
#
# Correção do dono depois de usar a tela: *"você replicou os filtros de
# solicitações, mas não é o que a gente trabalha aqui (…) aqui era pra ter
# filtro do tipo: o que já tem feito, o que está marcado, o que não está
# marcado, o que está provavelmente errado"*.
#
# POR QUE ESTES TESTES PRECISAM DE BANCO. Todo o recorte é subconsulta
# correlacionada (`sp_fiscal`, `sp_fiscal_analise`, `notas_fiscais`), e o
# "provavelmente errado" lê o CNPJ de DENTRO da chave com `substring`. A sessão
# dublada ignora WHERE — lá isto passaria mesmo escrito errado, e o erro
# apareceria como tela em branco na mão de quem trabalha.
# ---------------------------------------------------------------------------
def _marcar_no_card(sp_id, doc_fiscal):
    """O que a planilha de apoio traz do card — a outra porta da documentação."""
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.sp_fiscal (sp_id, doc_fiscal) "
                     "VALUES (?, ?) ON CONFLICT (sp_id) DO UPDATE "
                     "   SET doc_fiscal = EXCLUDED.doc_fiscal",
                     (str(sp_id), doc_fiscal))
        conn.commit()


def _diario(sp_id, **campos):
    from app.apps.analisesps.db import conexao
    colunas_ = ["sp_id"] + list(campos)
    valores = [str(sp_id)] + list(campos.values())
    marcas = ",".join(["?"] * len(colunas_))
    with conexao() as conn:
        conn.execute(
            f"INSERT INTO analisesps.sp_fiscal_analise ({','.join(colunas_)}) "
            f"VALUES ({marcas})", tuple(valores))
        conn.commit()


def _quantas(fiscais):
    from app.apps.analisesps import consultas
    return consultas.resumo({"fiscais": fiscais})["quantidade"]


@pytest.mark.banco
def test_todo_recorte_fiscal_e_SQL_QUE_O_BANCO_ACEITA(banco_analisesps):
    """Um por um. Se qualquer um estiver mal escrito, é aqui que aparece — e
    não como "Deu erro" na tela de quem foi trabalhar."""
    from app.apps.analisesps import consultas
    semear([sp("1", credor="ACME", documento="11.222.333/0001-81",
               valor="269,00")])
    for chave in consultas.SITUACOES_FISCAIS:
        assert _quantas([chave]) >= 0, chave


@pytest.mark.banco
def test_sem_documentacao_e_ja_categorizado_SE_COMPLETAM(banco_analisesps):
    """Um recorte que deixasse SP de fora dos dois, ou pusesse nos dois, faria
    os totalizadores não fecharem — e o dono confere somando."""
    semear([sp("1", credor="A"), sp("2", credor="B"), sp("3", credor="C")])
    _marcar_no_card("2", "NF-e (Mercadoria)")
    _diario("3", documentacao="Ausente")

    assert _quantas(["sem_marcacao"]) == 1
    assert _quantas(["ja_marcado"]) == 2
    assert _quantas(["sem_marcacao"]) + _quantas(["ja_marcado"]) == 3


@pytest.mark.banco
def test_o_DIARIO_manda_sobre_o_que_veio_do_card(banco_analisesps):
    """A decisão tomada aqui é mais nova que a planilha de apoio. Se o card
    mandasse, a SP recém-categorizada voltaria para a fila do "sem
    documentação" na primeira carga."""
    semear([sp("1", credor="A")])
    _marcar_no_card("1", "")
    _diario("1", documentacao="NF-e (Mercadoria)")
    assert _quantas(["ja_marcado"]) == 1
    assert _quantas(["sem_marcacao"]) == 0


@pytest.mark.banco
def test_provavelmente_errado_pega_a_nota_CANCELADA_do_card(banco_analisesps):
    """O mais grave da lista: despesa contra documento que não existe mais."""
    chave = _chave(CREDOR_CNPJ)
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52")])
    _diario("1", documentacao="NF-e (Mercadoria)", chave=chave)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ, status="Cancelada")
    assert _quantas(["provavel_erro"]) == 1


@pytest.mark.banco
def test_provavelmente_errado_pega_quem_AFIRMA_nota_e_nao_tem_chave(banco_analisesps):
    """Classificado sem documento — ou a nota nunca foi anexada."""
    semear([sp("1", credor="ACME")])
    _diario("1", documentacao="NFS-e (Serviço)", chave="")
    assert _quantas(["provavel_erro"]) == 1


@pytest.mark.banco
def test_provavelmente_errado_pega_a_chave_de_OUTRO_CNPJ(banco_analisesps):
    """O sintoma clássico do anexo trocado entre dois lançamentos — e este é
    detectado SEM depender de achar a nota, porque o CNPJ de quem emitiu está
    dentro da própria chave."""
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52"),
            sp("2", credor="OUTRA", documento="11.222.333/0001-81")])
    _diario("1", documentacao="NF-e (Mercadoria)",
            chave=_chave("11222333000181"))      # chave da OUTRA empresa
    _diario("2", documentacao="NF-e (Mercadoria)",
            chave=_chave("11222333000181"))      # esta é a dona da chave
    assert _quantas(["provavel_erro"]) == 1


@pytest.mark.banco
def test_uma_SP_em_dia_NAO_entra_no_provavelmente_errado(banco_analisesps):
    """O recorte que aponta erro onde não há é pior que recorte nenhum: em uma
    semana ninguém olha mais para ele."""
    chave = _chave(CREDOR_CNPJ)
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52")])
    _diario("1", documentacao="NF-e (Mercadoria)", chave=chave)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    assert _quantas(["provavel_erro"]) == 0


@pytest.mark.banco
def test_CPF_de_credor_nao_vira_falso_erro(banco_analisesps):
    """Pessoa física tem 11 dígitos e nunca bate com os 14 de dentro da chave.
    Sem a conferência de tamanho, TODA SP de credor pessoa física apareceria
    como provavelmente errada."""
    semear([sp("1", credor="JOSE", documento="123.456.789-09")])
    _diario("1", documentacao="NF-e (Mercadoria)", chave=_chave(CREDOR_CNPJ))
    assert _quantas(["provavel_erro"]) == 0


@pytest.mark.banco
def test_marcar_DOIS_recortes_exige_os_dois(banco_analisesps):
    """"Sem documentação" + "com anexo" é a fila mais útil da tela: o que dá
    para resolver lendo o anexo."""
    semear([sp("1", credor="A", anexo_link="http://x"),
            sp("2", credor="B", anexo_link="")])
    assert _quantas(["sem_marcacao"]) == 2
    assert _quantas(["sem_marcacao", "com_anexo"]) == 1


@pytest.mark.banco
def test_os_totalizadores_sao_da_BASE_INTEIRA_e_nao_da_pagina(banco_analisesps):
    """O ponto que torna a tela gerenciável: *"onde é que eu tenho que focar"*.
    Contar as 200 linhas carregadas responderia "o que falta NESTA PÁGINA" — e
    pareceria certo."""
    from app.apps.analisesps import consultas

    semear([sp(str(i), credor="A", valor="100,00") for i in range(1, 6)])
    _marcar_no_card("1", "NF-e (Mercadoria)")

    painel = consultas.painel_fiscal({})
    assert painel["total"] == 5
    assert painel["sem_marcacao"] == 4
    assert painel["ja_marcado"] == 1
    assert painel["valor_sem_marcacao"] == Decimal("400.00")


@pytest.mark.banco
def test_o_painel_RESPEITA_o_filtro_que_esta_na_tela(banco_analisesps):
    """Os números são do recorte em que a pessoa está. Um painel que ignorasse
    o filtro diria "faltam 1.200" enquanto a lista mostra três."""
    from app.apps.analisesps import consultas
    semear([sp("1", credor="ACME", tipo_despesa="Material"),
            sp("2", credor="OUTRA", tipo_despesa="Serviço")])
    painel = consultas.painel_fiscal({"tipo_despesa": ["Material"]})
    assert painel["total"] == 1


@pytest.mark.banco
def test_o_painel_e_o_filtro_CONCORDAM_sempre(banco_analisesps):
    """Saem dos mesmos pedaços de SQL. Este teste é o que garante que clicar
    num total leva exatamente às linhas que ele contou."""
    from app.apps.analisesps import consultas
    semear([sp(str(i), credor="A") for i in range(1, 8)])
    _marcar_no_card("1", "NF-e (Mercadoria)")
    _marcar_no_card("2", "Ausente")

    painel = consultas.painel_fiscal({})
    for chave in ("sem_marcacao", "ja_marcado", "provavel_erro", "na_fila_ia",
                  "confirmada", "escrita", "sem_anexo"):
        assert painel[chave] == _quantas([chave]), chave


@pytest.mark.banco
def test_a_fila_da_IA_e_o_a_gravar_sao_recortes_diferentes(banco_analisesps):
    """Um é o que espera a máquina; o outro é o que espera o Pipefy. Confundir
    os dois faria a pessoa procurar trabalho no lugar errado."""
    semear([sp("1", credor="A"), sp("2", credor="B"), sp("3", credor="C")])
    _diario("1", situacao="NA_FILA_IA")
    _diario("2", situacao="CONFIRMADA", documentacao="NF-e (Mercadoria)")
    _diario("3", situacao="ESCRITA", documentacao="NF-e (Mercadoria)")

    assert _quantas(["na_fila_ia"]) == 1
    assert _quantas(["confirmada"]) == 1
    assert _quantas(["escrita"]) == 1


@pytest.mark.banco
def test_o_que_JA_FOI_GRAVADO_no_card_sai_da_fila_de_gravar(banco_analisesps):
    """`escrita_em` é o que separa "decidido" de "gravado". Sem ele, o mesmo
    trabalho apareceria como pendente para sempre."""
    from app.apps.analisesps.db import conexao
    semear([sp("1", credor="A")])
    _diario("1", situacao="CONFIRMADA", documentacao="NF-e (Mercadoria)")
    assert _quantas(["confirmada"]) == 1
    with conexao() as conn:
        conn.execute("UPDATE analisesps.sp_fiscal_analise "
                     "   SET escrita_em = now() WHERE sp_id = '1'")
        conn.commit()
    assert _quantas(["confirmada"]) == 0


@pytest.mark.banco
def test_os_numeros_do_lado_da_NOTA(banco_analisesps):
    """A outra metade da gestão: quanta nota existe sem despesa nenhuma."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1", 10.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181"), "2", 20.00, "11222333000181",
                  status="Cancelada")
    semear([sp("1", credor="A")])
    _diario("1", chave=_chave(CREDOR_CNPJ))

    painel = fiscal.painel_notas()
    assert painel["total"] == 2
    assert painel["canceladas"] == 1
    # A associada saiu; a cancelada nunca entrou.
    assert painel["sem_lancamento"] == 0


@pytest.mark.banco
def test_o_CTe_e_contado_pela_CHAVE_e_nao_pela_coluna_tipo(banco_analisesps):
    """DEFEITO ACHADO exercitando a tela contra banco de verdade em 13/09/2026:
    o contador dizia ZERO com CT-e na base.

    A coluna `tipo` vem preenchida de jeitos diferentes conforme a porta de
    entrada — a Receita grava "CT-e", e o relatório do FSist grava o que
    estiver na coluna "Tipo" da planilha, que ninguém controla. As posições 21
    e 22 da chave são o modelo do documento por definição da Receita, e valem
    para toda nota tenha ela vindo por onde tiver vindo."""
    from app.apps.analisesps import fiscal

    # Um CT-e (modelo 57) com a coluna `tipo` VAZIA, que é como o relatório do
    # FSist às vezes chega.
    _guardar_nota(_chave(CREDOR_CNPJ, "570010000123456789012345"),
                  "88", 90.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181"), "77", 20.00, "11222333000181")

    assert fiscal.painel_notas()["ctes"] == 1


@pytest.mark.banco
def test_gravar_a_mao_TIRA_a_nota_da_lista_de_orfas(banco_analisesps):
    """O caminho inteiro, da ponta à ponta: a nota órfã aparece, alguém
    associa, e ela some — que é o que faz a lista encolher em vez de só
    acusar."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00")])

    orfas, total = fiscal.notas_orfas()
    assert total == 1

    fiscal.decidir_a_mao("1", "", chave, "MARCELO",
                         {"documento": "29.066.773/0001-52"})

    orfas, total = fiscal.notas_orfas()
    assert total == 0
    assert _quantas(["ja_marcado"]) == 1


@pytest.mark.banco
def test_a_LISTA_e_os_TOTAIS_leem_a_MESMA_documentacao(banco_analisesps):
    """DEFEITO ACHADO abrindo a tela contra banco de verdade em 13/09/2026.

    Os totalizadores contavam as duas portas da documentação (o diário deste
    módulo e o espelho do card que a planilha de apoio traz) e a LISTA só olhava
    o diário. Na tela isso virava o painel dizer "3 já categorizados" e a linha
    mostrar "—" na coluna "Está como" — duas afirmações contrárias, e nenhuma
    delas com jeito de errada.

    Este teste é o que impede a volta: a mesma SP, marcada só no card, tem de
    aparecer categorizada nos dois lugares."""
    from app.apps.analisesps import fiscal

    semear([sp("1", credor="ALUGUEL LTDA", valor="3.000,00")])
    _marcar_no_card("1", "Contrato")

    assert _quantas(["ja_marcado"]) == 1
    assert fiscal.analises_guardadas(["1"])["1"]["documentacao"] == "Contrato"


@pytest.mark.banco
def test_o_DIARIO_manda_tambem_NA_LISTA(banco_analisesps):
    """Mesma ordem de precedência do SQL dos totais: a decisão tomada aqui é
    mais nova que a planilha de apoio."""
    from app.apps.analisesps import fiscal

    semear([sp("1", credor="A")])
    _marcar_no_card("1", "Ausente")
    _diario("1", documentacao="NF-e (Mercadoria)")
    assert fiscal.analises_guardadas(["1"])["1"]["documentacao"] \
        == "NF-e (Mercadoria)"


@pytest.mark.banco
def test_SP_sem_nenhuma_das_duas_portas_nao_aparece_no_diario(banco_analisesps):
    """A busca do diário não pode inventar linha: uma SP sem documentação em
    lugar nenhum tem de vir vazia, e não com um dicionário de campos nulos que
    a tela leria como "já tem alguma coisa"."""
    from app.apps.analisesps import fiscal

    semear([sp("1", credor="A")])
    assert fiscal.analises_guardadas(["1"]) == {}


@pytest.mark.banco
def test_QUEM_marcou_separa_pessoa_sistema_e_o_que_veio_do_card(banco_analisesps):
    """Pergunta do dono em 13/09/2026: *"qual o filtro pra aparecer somente as
    que o sistema marcou?"*

    São três origens diferentes e elas não podem se misturar: o que uma pessoa
    decidiu, o que o sistema decidiu (proposta aprovada ou leitura por IA), e o
    que já veio preenchido do card antes desta tela existir. Essa última não é
    decisão de ninguém aqui — contá-la como "o sistema marcou" faria o número
    parecer trabalho feito pela ferramenta quando não foi."""
    semear([sp(str(i), credor="A") for i in range(1, 5)])
    _diario("1", documentacao="NF-e (Mercadoria)", origem="PESSOA")
    _diario("2", documentacao="NF-e (Mercadoria)", origem="CONCILIACAO")
    _diario("3", documentacao="NF-e (Mercadoria)", origem="IA")
    _diario("4", documentacao="Contrato", origem="PIPEFY")

    assert _quantas(["decidida_por_pessoa"]) == 1
    assert _quantas(["decidida_pelo_sistema"]) == 2      # a conciliação e a IA
    assert _quantas(["veio_do_card"]) == 1


@pytest.mark.banco
def test_o_painel_e_o_filtro_concordam_tambem_COM_JUNCAO(banco_analisesps):
    """O painel passou a montar as regras por JUNÇÃO (rápido) enquanto o filtro
    segue com subconsulta correlacionada (índice resolve linha a linha). São
    duas escritas da MESMA regra, e o risco disso é óbvio.

    Medido em 13/09/2026 com 59.000 SPs: 1,18s com subconsulta contra 0,14s com
    junção, nesta máquina — e o banco do Render tem um décimo de um núcleo. A
    otimização vale; este teste é o preço dela.

    O caso é montado com as três portas da documentação ao mesmo tempo, que é
    onde as duas escritas teriam mais chance de discordar."""
    from app.apps.analisesps import consultas

    semear([sp("1", credor="A", documento="29.066.773/0001-52", valor="10,00"),
            sp("2", credor="B", documento="11.222.333/0001-81", valor="20,00",
               anexo_link="http://x"),
            sp("3", credor="C", documento="123.456.789-09", valor="30,00"),
            sp("4", credor="D", valor="40,00")])
    _marcar_no_card("1", "Contrato")                       # só no card
    _diario("2", documentacao="NF-e (Mercadoria)",         # chave de OUTRO CNPJ
            chave=_chave(CREDOR_CNPJ), origem="PESSOA")
    _diario("3", documentacao="NFS-e (Serviço)", chave="") # afirma nota sem ter
    _diario("4", situacao="NA_FILA_IA")

    painel = consultas.painel_fiscal({})
    assert painel["total"] == 4
    for chave in ("sem_marcacao", "ja_marcado", "provavel_erro", "com_chave",
                  "na_fila_ia", "confirmada", "escrita", "sem_anexo"):
        assert painel[chave] == _quantas([chave]), chave


@pytest.mark.banco
def test_a_busca_de_SPs_por_nota_traz_o_MESMO_em_lote_e_uma_a_uma(banco_analisesps):
    """A busca virou duas consultas para a página inteira, em vez de uma por
    nota — 28 segundos medidos com 59.000 SPs, e em produção a tela não abria.

    Trocar o caminho não pode trocar o RESULTADO: este teste compara os dois."""
    from app.apps.analisesps import fiscal

    semear([sp("1", credor="ACME", documento="29.066.773/0001-52", valor="269,00"),
            sp("2", credor="ACME", documento="29.066.773/0001-52", valor="500,00"),
            sp("3", credor="OUTRA", documento="11.222.333/0001-81", valor="269,00")])
    notas = [
        {"chave": _chave(CREDOR_CNPJ), "valor": 269.00,
         "emitente_doc": CREDOR_CNPJ},
        {"chave": _chave("11222333000181"), "valor": 269.00,
         "emitente_doc": "11222333000181"},
    ]

    em_lote = fiscal.sps_possiveis_das_notas(notas)
    for nota in notas:
        uma_a_uma = fiscal.sps_possiveis_da_nota(nota)
        chave = fiscal.so_digitos(nota["chave"])
        assert [s["id"] for s in em_lote[chave]] == [s["id"] for s in uma_a_uma]

    # E a de valor IGUAL vem na frente — é a que fecha o par.
    assert em_lote[fiscal.so_digitos(notas[0]["chave"])][0]["id"] == "1"


@pytest.mark.banco
def test_a_LISTA_de_notas_recorta_pelo_que_a_tela_oferece(banco_analisesps):
    """*"Não consigo visualizar numa tela o que temos de notas e o que não
    temos."* A lista era só das órfãs; agora é de tudo, e "sem lançamento" é um
    recorte dela."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1", 10.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181", "570010000123456789012345"),
                  "2", 20.00, "11222333000181")
    _guardar_nota(_chave("99888777000166"), "3", 30.00, "99888777000166",
                  status="Cancelada")
    semear([sp("1", credor="A")])
    _diario("1", chave=_chave(CREDOR_CNPJ))

    todas, resumo = fiscal.listar_notas({})
    assert resumo["quantidade"] == 3
    assert resumo["sem_lancamento"] == 2

    orfas, _ = fiscal.listar_notas({"recortes": ["sem_lancamento"]})
    assert len(orfas) == 2
    ctes, _ = fiscal.listar_notas({"recortes": ["cte"]})
    assert len(ctes) == 1
    canceladas, _ = fiscal.listar_notas({"recortes": ["cancelada"]})
    assert len(canceladas) == 1
    # Dois recortes somam, como na tela de lançamentos.
    nada, _ = fiscal.listar_notas({"recortes": ["cte", "cancelada"]})
    assert nada == []


@pytest.mark.banco
def test_a_busca_da_lista_de_notas_acha_por_numero_CNPJ_e_chave(banco_analisesps):
    """Os quatro jeitos de procurar uma nota que alguém tem na mão."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 10.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181"), "77", 20.00, "11222333000181")

    for termo in ("1430", CREDOR_CNPJ, chave, "fornecedor"):
        achadas, _ = fiscal.listar_notas({"busca": termo})
        assert achadas, f"a busca por {termo!r} não achou nada"


@pytest.mark.banco
def test_o_recorte_de_EMISSAO_responde_as_notas_do_dia(banco_analisesps):
    """*"Ver as notas do dia ou consultar as notas."*"""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1", 10.00, CREDOR_CNPJ,
                  emissao="2026-09-10")
    _guardar_nota(_chave("11222333000181"), "2", 20.00, "11222333000181",
                  emissao="2026-09-12")

    do_dia, _ = fiscal.listar_notas({"emissao_ini": "2026-09-10",
                                     "emissao_fim": "2026-09-10"})
    assert len(do_dia) == 1 and do_dia[0]["numero"] == "1"


# ---------------------------------------------------------------------------
# O RELATÓRIO DO FSIST SUBIDO COMO ARQUIVO — 13/09/2026
#
# *"Cadê a opção de incluir o arquivo? De onde vai tirar essa informação, se eu
# não estou nem colocando?"* Aqui é o caminho inteiro, com banco de verdade:
# arquivo entra, nota fica gravada, e reimportar não duplica.
# ---------------------------------------------------------------------------
def _relatorio_csv(linhas, separador=";"):
    return "\r\n".join(separador.join(c for c in linha)
                       for linha in linhas).encode("utf-8")


_CAB_FSIST = ["Emissão", "Chave", "Número", "Série", "Valor", "Situação",
              "Emitente CNPJ", "Emitente", "Emitente UF", "Destinatário CNPJ"]


@pytest.mark.banco
def test_o_arquivo_do_FSist_vira_nota_no_banco(banco_analisesps):
    from app.apps.analisesps import sincronizacao
    from app.apps.analisesps.db import consultar_um

    chave = _chave(CREDOR_CNPJ)
    arquivo = _relatorio_csv([
        ["Relatório de Notas de Compras"],          # o título, na linha 1
        _CAB_FSIST,
        ["01/09/2026", chave, "1430", "1", "269,00", "Autorizada",
         "29.066.773/0001-52", "ACME MATERIAIS", "BA", "10.656.452/0078-69"]])

    saida = sincronizacao.importar_notas_de_arquivo(arquivo, "fsist.csv")
    assert saida["lidas"] == 1 and saida["novas"] == 1

    linha = consultar_um(
        "SELECT numero, valor, status, emitente_doc, emissao "
        "  FROM analisesps.notas_fiscais WHERE chave = ?", (chave,))
    assert linha[0] == "1430"
    assert linha[1] == Decimal("269.00"), "o valor em português virou número"
    assert linha[2] == "Autorizada"
    assert linha[3] == CREDOR_CNPJ, "o CNPJ perdeu a pontuação"
    assert linha[4] == dt.date(2026, 9, 1), "a data em DD/MM/AAAA virou data"


@pytest.mark.banco
def test_subir_o_MESMO_relatorio_duas_vezes_nao_duplica(banco_analisesps):
    """A chave é a identidade. Reimportar é o que ele vai fazer sem pensar —
    e tem de ser inofensivo."""
    from app.apps.analisesps import sincronizacao
    from app.apps.analisesps.db import consultar_um

    arquivo = _relatorio_csv([
        _CAB_FSIST,
        ["01/09/2026", _chave(CREDOR_CNPJ), "1430", "1", "269,00",
         "Autorizada", "29.066.773/0001-52", "ACME", "BA",
         "10.656.452/0078-69"]])

    sincronizacao.importar_notas_de_arquivo(arquivo, "fsist.csv")
    segunda = sincronizacao.importar_notas_de_arquivo(arquivo, "fsist.csv")

    assert consultar_um("SELECT count(*) FROM analisesps.notas_fiscais")[0] == 1
    assert segunda["novas"] == 0


@pytest.mark.banco
def test_a_nota_que_voltou_CANCELADA_e_atualizada(banco_analisesps):
    """É o achado que mais importa num reenvio do relatório: pagar contra nota
    cancelada é problema fiscal."""
    from app.apps.analisesps import sincronizacao
    from app.apps.analisesps.db import consultar_um

    chave = _chave(CREDOR_CNPJ)

    def arquivo(status):
        return _relatorio_csv([
            _CAB_FSIST,
            ["01/09/2026", chave, "1430", "1", "269,00", status,
             "29.066.773/0001-52", "ACME", "BA", "10.656.452/0078-69"]])

    sincronizacao.importar_notas_de_arquivo(arquivo("Autorizada"), "a.csv")
    saida = sincronizacao.importar_notas_de_arquivo(arquivo("Cancelada"), "b.csv")

    assert saida["novas"] == 0 and saida["atualizadas"] == 1
    assert consultar_um("SELECT status FROM analisesps.notas_fiscais "
                        " WHERE chave = ?", (chave,))[0] == "Cancelada"


@pytest.mark.banco
def test_linhas_de_rodape_e_totalizador_sao_IGNORADAS(banco_analisesps):
    """O relatório traz total no fim e linhas em branco no meio. Elas não são
    erro — são o formato — e não podem virar recado de falha."""
    from app.apps.analisesps import sincronizacao

    arquivo = _relatorio_csv([
        _CAB_FSIST,
        ["01/09/2026", _chave(CREDOR_CNPJ), "1430", "1", "269,00",
         "Autorizada", "29.066.773/0001-52", "ACME", "BA", "10.656.452/0078-69"],
        ["", "", "", "", "", "", "", "", "", ""],
        ["TOTAL", "", "", "", "269,00", "", "", "", "", ""]])

    saida = sincronizacao.importar_notas_de_arquivo(arquivo, "fsist.csv")
    assert saida["lidas"] == 1
    assert saida["ignoradas"] == 2


@pytest.mark.banco
def test_a_nota_do_arquivo_APARECE_na_tela_de_notas(banco_analisesps):
    """O caminho da ponta à ponta: subiu o arquivo, a nota aparece na lista e
    conta nos totais."""
    from app.apps.analisesps import fiscal, sincronizacao

    sincronizacao.importar_notas_de_arquivo(_relatorio_csv([
        _CAB_FSIST,
        ["01/09/2026", _chave(CREDOR_CNPJ), "1430", "1", "269,00",
         "Autorizada", "29.066.773/0001-52", "ACME", "BA",
         "10.656.452/0078-69"]]), "fsist.csv")

    notas, resumo = fiscal.listar_notas({})
    assert resumo["quantidade"] == 1
    assert notas[0]["numero"] == "1430"
    assert notas[0]["orfa"] is True
    assert fiscal.painel_notas()["total"] == 1


# ---------------------------------------------------------------------------
# O PAR COM A SP, E O ALARME DA NOTA CANCELADA — 13/09/2026
#
# *"Nessas que estão aqui já verdinha pra associar (…) tem outra sim, o valor é
# diferente. Tem que ter um tratamento aí."* E, mais grave: *"imagina, o
# fornecedor emitiu e cancelou a nota. E a gente associou, pagou, e a nota virou
# cancelada. A gente tem que ter um local de visualização disso."*
# ---------------------------------------------------------------------------
@pytest.mark.banco
def test_o_recorte_separa_a_que_FECHA_da_que_tem_valor_diferente(banco_analisesps):
    """Três estados, e eles pedem coisas diferentes: a que fecha sem pensar, a
    que tem SP do credor mas de outro valor (precisa de olho), e a que não tem
    SP nenhuma daquele CNPJ (a despesa pode nem ter sido lançada)."""
    from app.apps.analisesps import fiscal

    _guardar_nota(_chave(CREDOR_CNPJ), "1", 269.00, CREDOR_CNPJ)
    _guardar_nota(_chave("11222333000181"), "2", 500.00, "11222333000181")
    _guardar_nota(_chave("99888777000166"), "3", 800.00, "99888777000166")
    semear([
        sp("1", credor="ACME", documento="29.066.773/0001-52", valor="269,00"),
        sp("2", credor="BETA", documento="11.222.333/0001-81", valor="123,00"),
    ])

    fecha, _ = fiscal.listar_notas({"recortes": ["casa_valor"]})
    assert [n["numero"] for n in fecha] == ["1"]

    olho, _ = fiscal.listar_notas({"recortes": ["sem_valor_igual"]})
    assert [n["numero"] for n in olho] == ["2"]

    sem_sp, _ = fiscal.listar_notas({"recortes": ["sem_sp_do_credor"]})
    assert [n["numero"] for n in sem_sp] == ["3"]


@pytest.mark.banco
def test_os_tres_recortes_do_par_se_COMPLETAM(banco_analisesps):
    """Uma nota que ficasse de fora dos três, ou em dois, faria os números não
    fecharem — e o dono confere somando."""
    from app.apps.analisesps import fiscal

    for i, cnpj in enumerate([CREDOR_CNPJ, "11222333000181", "99888777000166"]):
        _guardar_nota(_chave(cnpj), str(i), 100.00 * (i + 1), cnpj)
    semear([sp("1", credor="A", documento="29.066.773/0001-52", valor="100,00"),
            sp("2", credor="B", documento="11.222.333/0001-81", valor="999,00")])

    somados = sum(len(fiscal.listar_notas({"recortes": [r]})[0])
                  for r in ("casa_valor", "sem_valor_igual", "sem_sp_do_credor"))
    assert somados == 3


@pytest.mark.banco
def test_a_nota_CANCELADA_JA_ASSOCIADA_e_o_alarme(banco_analisesps):
    """O caso mais grave desta tela, e o que ficava INVISÍVEL justamente por
    estar "resolvido": a nota cancelada não é órfã — ela está associada —, então
    não aparecia na lista das que precisam de despesa."""
    from app.apps.analisesps import fiscal

    boa = _chave(CREDOR_CNPJ)
    ruim = _chave("11222333000181")
    _guardar_nota(boa, "1", 100.00, CREDOR_CNPJ)
    _guardar_nota(ruim, "2", 200.00, "11222333000181", status="Cancelada")
    # Uma cancelada e SOLTA: não é alarme — cancelada sem despesa é o esperado.
    _guardar_nota(_chave("99888777000166"), "3", 300.00, "99888777000166",
                  status="Cancelada")
    semear([sp("1", credor="A"), sp("2", credor="B")])
    _diario("1", chave=boa)
    _diario("2", chave=ruim, documentacao="NF-e (Mercadoria)")

    assert fiscal.painel_notas()["canceladas_em_uso"] == 1
    alarme, _ = fiscal.listar_notas({"recortes": ["cancelada_em_uso"]})
    assert [n["numero"] for n in alarme] == ["2"]
    # E a cancelada solta continua contando como cancelada, sem ser alarme.
    assert fiscal.painel_notas()["canceladas"] == 2


@pytest.mark.banco
def test_cada_candidata_vem_com_o_PORQUE_escrito(banco_analisesps):
    """*"Você bota 'mesmo valor'. Mas você está comparando o mesmo valor de
    quê? Como é que chegou a essa informação?"* "Mesmo valor" sozinho dá ar de
    conferência a uma coincidência."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ, emissao="2026-09-01")
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00", vencimento="10/09/2026", nf="1430")])

    candidatas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ, "emissao": dt.date(2026, 9, 1)}])
    sp1 = candidatas[chave][0]

    rotulos = {r["rotulo"]: r["bate"] for r in sp1["razoes"]}
    assert rotulos["CNPJ do credor é o de quem emitiu"] is True
    assert rotulos["Valor igual"] is True
    assert rotulos["Nº da nota no card"] is True
    assert rotulos["Data compatível"] is True
    assert sp1["confere"] == 4 and sp1["de"] == 4


@pytest.mark.banco
def test_a_candidata_de_valor_DIFERENTE_diz_os_dois_numeros(banco_analisesps):
    """Ver "a SP é R$ 500,00 e a nota R$ 269,00" é o que impede associar por
    engano — e é a informação que faltava."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52",
               valor="500,00")])

    candidatas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ}])
    razao = next(r for r in candidatas[chave][0]["razoes"]
                 if "Valor" in r["rotulo"])
    assert razao["bate"] is False
    assert "500,00" in razao["detalhe"] and "269,00" in razao["detalhe"]


@pytest.mark.banco
def test_a_nota_associada_DIZ_em_qual_SP_esta(banco_analisesps):
    """Dizer só "já está num lançamento" é meio caminho: na nota cancelada,
    saber QUAL SP é o que permite agir — ver se foi paga e ligar para o
    fornecedor. Sem o número, ele teria de procurar a chave na outra tela."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "555", 269.00, CREDOR_CNPJ, status="Cancelada")
    semear([sp("1443253428", credor="ACME", status_pgt="Pago")])
    _diario("1443253428", chave=chave, documentacao="NF-e (Mercadoria)")

    notas, _ = fiscal.listar_notas({"recortes": ["cancelada_em_uso"]})
    assert notas[0]["sp_id"] == "1443253428"
    assert notas[0]["sp_status"] == "Pago", (
        "sem saber que foi PAGA, não dá para medir a urgência")


@pytest.mark.banco
def test_as_ULTIMAS_rodadas_saem_UMA_POR_TIPO(banco_analisesps):
    """A tela mostra o resultado da última vez que CADA botão rodou. Uma
    consulta por tipo seriam cinco varreduras da mesma tabela num banco que tem
    um décimo de um núcleo — por isso é uma só, com DISTINCT ON."""
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        for tipo, quando, ok, msg in [
                ("fiscal", "2026-09-13 10:00", True, "3 card(s) gravado(s)"),
                ("fiscal", "2026-09-13 14:00", True, "12 card(s) gravado(s)"),
                ("notas_receita", "2026-09-13 13:00", False, "certificado vencido"),
                ("carga_inicial", "2026-09-13 09:00", True, "não é fiscal")]:
            conn.execute(
                "INSERT INTO analisesps.execucoes "
                "  (tipo, disparo, inicio, fim, ok, mensagem) "
                "VALUES (?, 'manual', ?, ?, ?, ?)",
                (tipo, quando, quando, ok, msg))
        conn.commit()

    ultimas = tarefas.ultimas_por_tipo(tarefas.MODOS_FISCAIS)
    assert ultimas["fiscal"]["mensagem"] == "12 card(s) gravado(s)", (
        "veio a rodada antiga, não a última")
    assert ultimas["notas_receita"]["ok"] is False
    assert "carga_inicial" not in ultimas, "trouxe um tipo que não foi pedido"


@pytest.mark.banco
def test_uma_rodada_que_NAO_TERMINOU_nao_conta_como_resultado(banco_analisesps):
    """O que está rodando agora aparece no aviso de andamento; aqui é o que
    TERMINOU. Misturar os dois faria a tela dizer "gravou" no meio da gravação."""
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.execucoes (tipo, disparo, inicio, fim) "
            "VALUES ('fiscal', 'manual', now(), NULL)")
        conn.commit()
    assert tarefas.ultimas_por_tipo(["fiscal"]) == {}


# ===========================================================================
# A SP QUE JÁ TEM NOTA NÃO É SUGESTÃO — cobrança do dono em 13/09/2026
#
# *"Tem registro que está aparecendo aqui que ele já tem nota fiscal, já é um
# registro que tem uma nota fiscal associada anteriormente, e inclusive já tem
# o número da nota, já está associado lá na planilha de documentação fiscal,
# ou seja, está tudo identificado — e ele está colocando aqui como sugestão de
# uma nota pra associar. Qual é o sentido disso?"*
#
# Nenhum. E o risco não era só ocupar vaga: era ficar a um clique de gravar uma
# SEGUNDA nota numa SP já conferida.
# ===========================================================================
@pytest.mark.banco
def test_a_SP_que_ja_aponta_para_outra_nota_NAO_disputa_vaga(banco_analisesps):
    """Trabalho já feito sai da fila de sugestões."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import conexao

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    # Duas SPs do mesmo CNPJ e do mesmo valor. A "2" já foi conferida antes,
    # contra OUTRA nota.
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00"),
            sp("2", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00")])
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise (sp_id, situacao, chave) "
            "VALUES ('2', 'ESCRITA', ?)", (_chave("11222333000144"),))
        conn.commit()

    escolhidas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ}])[chave]

    sugeridas = [c for c in escolhidas if not c["ja_tem_nota"]]
    assert [c["id"] for c in sugeridas] == ["1"]


@pytest.mark.banco
def test_a_SP_que_ja_tem_nota_continua_VISIVEL_so_que_marcada(banco_analisesps):
    """Sumir sem dizer seria esconder. Ela fica, contada e clicável, para dar
    onde conferir que o corte não comeu nada por engano."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import conexao

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    semear([sp("2", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00")])
    outra = _chave("11222333000144")
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise (sp_id, situacao, chave) "
            "VALUES ('2', 'ESCRITA', ?)", (outra,))
        conn.commit()

    escolhidas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ}])[chave]

    assert [c["id"] for c in escolhidas] == ["2"]
    assert escolhidas[0]["ja_tem_nota"] is True
    assert fiscal.so_digitos(escolhidas[0]["nota_que_ja_tem"]) == outra


@pytest.mark.banco
def test_a_SP_com_o_numero_da_NF_no_card_CONTINUA_sendo_sugerida(
        banco_analisesps):
    """⚠️ O CORTE É PELA CHAVE, NUNCA PELO Nº DA NF DIGITADO NO CARD.

    A SP que tem o número escrito mas nunca foi associada é a MELHOR candidata
    que existe — o número confere. Cortar por ele esconderia justamente o par
    mais fácil da base, que é o contrário do que foi pedido."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00", nf="1430")])

    escolhidas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ}])[chave]

    assert [c["id"] for c in escolhidas] == ["1"]
    assert escolhidas[0]["ja_tem_nota"] is False


@pytest.mark.banco
def test_a_SP_que_ja_aponta_para_ESTA_nota_some_de_vez(banco_analisesps):
    """Se a SP já é desta nota, não é sugestão nem "já tem nota de outra": é o
    que já existe. Mostrá-la seria oferecer o que já está feito."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import conexao

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    semear([sp("1", credor="ACME", documento="29.066.773/0001-52",
               valor="269,00")])
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise (sp_id, situacao, chave) "
            "VALUES ('1', 'ESCRITA', ?)", (chave[:22] + " " + chave[22:],))
        conn.commit()

    escolhidas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ}])[chave]

    assert escolhidas == []


# ===========================================================================
# AS SPs POR TRÁS DE CADA NOME — cobrança do dono em 13/09/2026
#
# *"Eu estou diante de um determinado CNPJ, aí aparecem várias opções (…) ele
# marca aqui uma, duas, três, quatro SPs que é de uma outra locadora que não
# tem nada a ver, ou seja, aqui foi claramente um erro. Só que a partir daqui
# eu não consigo ir a essas SPs que estão erradas. Só pra poder confirmar se eu
# posso realmente aplicar ou não, eu precisaria ver essas SPs e entender onde
# foi o erro."*
# ===========================================================================
@pytest.mark.banco
def test_ver_as_SPs_de_um_nome_traz_so_as_daquele_nome(banco_analisesps):
    """A conferência que fundamenta a decisão: quais SPs usam CADA nome."""
    from app.apps.analisesps import credores

    semear([
        sp("1", credor="LOCADORA DO VALE LTDA",
           documento="29.066.773/0001-52", valor="100,00"),
        sp("2", credor="LOCADORA DO VALE LTDA",
           documento="29066773000152", valor="200,00"),
        sp("3", credor="LOCADORA SERRA NEGRA",
           documento="29.066.773/0001-52", valor="300,00"),
    ])

    achadas = credores.sps_do_nome("29.066.773/0001-52",
                                   ["LOCADORA SERRA NEGRA"])
    assert [s_["id"] for s_ in achadas] == ["3"]


@pytest.mark.banco
def test_ver_as_SPs_junta_as_GRAFIAS_do_mesmo_nome(banco_analisesps):
    """A opção da tela é um GRUPO de grafias ("SERVIÇOS" e "SERVICOS"). Se a
    busca pegasse só a grafia escolhida, a tela diria "3 SPs" e a lista traria
    2 — e uma conta que não fecha derruba a confiança na tela inteira."""
    from app.apps.analisesps import credores

    semear([
        sp("1", credor="MASSA PRONTA SERVIÇOS", documento="29066773000152"),
        sp("2", credor="MASSA PRONTA SERVICOS", documento="29066773000152"),
        sp("3", credor="OUTRA EMPRESA", documento="29066773000152"),
    ])

    achadas = credores.sps_do_nome(
        "29066773000152", ["MASSA PRONTA SERVIÇOS", "MASSA PRONTA SERVICOS"])
    assert sorted(s_["id"] for s_ in achadas) == ["1", "2"]


@pytest.mark.banco
def test_ver_as_SPs_casa_o_CNPJ_escrito_de_qualquer_jeito(banco_analisesps):
    """Na planilha o mesmo CNPJ vem pontuado numa linha e cru noutra. Casar
    pelo texto faria o fornecedor virar dois — e a lista viria pela metade."""
    from app.apps.analisesps import credores

    semear([
        sp("1", credor="ACME", documento="29.066.773/0001-52"),
        sp("2", credor="ACME", documento="29066773000152"),
    ])

    achadas = credores.sps_do_nome("29066773000152", ["ACME"])
    assert sorted(s_["id"] for s_ in achadas) == ["1", "2"]


@pytest.mark.banco
def test_ver_as_SPs_sem_documento_ou_sem_nome_nao_varre_a_base(banco_analisesps):
    """Pedido vazio devolve vazio. Sem esta guarda, um clique com o campo em
    branco traria a base inteira — 59 mil linhas na memória da instância que já
    morreu disso em julho."""
    from app.apps.analisesps import credores

    semear([sp("1", credor="ACME", documento="29066773000152")])

    assert credores.sps_do_nome("", ["ACME"]) == []
    assert credores.sps_do_nome("29066773000152", []) == []
    assert credores.sps_do_nome("29066773000152", ["  "]) == []


@pytest.mark.banco
def test_ver_as_SPs_tem_TETO(banco_analisesps):
    """Ninguém confere novecentas linhas, e mandá-las para o navegador trava a
    tela. O teto é dito na tela, não escondido."""
    from app.apps.analisesps import credores

    semear([sp(str(i), credor="ACME", documento="29066773000152",
               valor="10,00") for i in range(1, credores.SPS_POR_NOME + 21)])

    assert len(credores.sps_do_nome("29066773000152", ["ACME"])) == \
        credores.SPS_POR_NOME


# ===========================================================================
# O ESCOPO DA DOCUMENTAÇÃO FISCAL — três cortes pedidos em 13/09/2026
#
# *"Os registros da documentação fiscal não devem retornar apenas os dados do
# que venceu em 2026 ou do que foi pago em 2026, o restante ignorar."*
# *"A princípio tudo que está com o Status Pgt = Cancelado não deveria ser
# exibido, somente se colocássemos para exibir."*
# *"Não deve ser exibido registros que contenham no Tipo de Despesa a
# informação '(TRF)'."*
#
# ⚠️ CADA CORTE É COBRADO NA LISTA **E** NO PAINEL. Painel e lista lendo
# universos diferentes é o defeito de 13/09 ("3 já categorizados" com a linha
# mostrando "—"), e é o que mais destrói a confiança na tela.
# ===========================================================================
FISCAL = {"escopo_fiscal": True}


@pytest.mark.banco
def test_o_escopo_fiscal_deixa_de_fora_o_que_e_ANTERIOR_a_2026(banco_analisesps):
    from app.apps.analisesps import consultas

    semear([
        sp("1", credor="ACME", vencimento="10/09/2026", valor="100,00"),
        sp("2", credor="ACME", vencimento="10/09/2025", valor="100,00"),
    ])

    ids = [l["id"] for l in consultas.listar(dict(FISCAL), pagina=1)]
    assert ids == ["1"]
    assert consultas.resumo(dict(FISCAL))["quantidade"] == 1


@pytest.mark.banco
def test_vencida_em_2025_mas_PAGA_em_2026_continua_aparecendo(banco_analisesps):
    """O ano vale pelo vencimento OU pelo pagamento. A SP que venceu em
    dezembro e foi paga em janeiro é trabalho fiscal de 2026."""
    from app.apps.analisesps import consultas

    semear([sp("1", credor="ACME", vencimento="28/12/2025",
               data_pagamento="05/01/2026", valor="100,00")])

    assert [l["id"] for l in consultas.listar(dict(FISCAL), pagina=1)] == ["1"]


@pytest.mark.banco
def test_a_SP_SEM_DATA_NENHUMA_nao_some(banco_analisesps):
    """⚠️ Sem data não é "velha", é *sem data*. Sumir com ela seria tirar da
    conta um trabalho que ninguém mais veria — e some em silêncio é o defeito
    que esta tela já teve duas vezes."""
    from app.apps.analisesps import consultas

    semear([sp("1", credor="ACME", valor="100,00")])

    assert [l["id"] for l in consultas.listar(dict(FISCAL), pagina=1)] == ["1"]


@pytest.mark.banco
def test_o_CANCELADO_fica_de_fora_por_padrao(banco_analisesps):
    from app.apps.analisesps import consultas

    semear([
        sp("1", credor="ACME", vencimento="10/09/2026", status_pgt="Pagar"),
        sp("2", credor="ACME", vencimento="10/09/2026", status_pgt="Cancelado"),
    ])

    assert [l["id"] for l in consultas.listar(dict(FISCAL), pagina=1)] == ["1"]
    assert consultas.resumo(dict(FISCAL))["quantidade"] == 1


@pytest.mark.banco
def test_o_CANCELADO_volta_quando_se_pede(banco_analisesps):
    """*"somente se colocássemos para exibir"* — a volta é o que separa
    "escondido" de "apagado"."""
    from app.apps.analisesps import consultas

    semear([
        sp("1", credor="ACME", vencimento="10/09/2026", status_pgt="Pagar"),
        sp("2", credor="ACME", vencimento="10/09/2026", status_pgt="Cancelado"),
    ])

    f = {"escopo_fiscal": True, "mostrar_canceladas": True}
    assert sorted(l["id"] for l in consultas.listar(f, pagina=1)) == ["1", "2"]
    assert consultas.resumo(f)["quantidade"] == 2


@pytest.mark.banco
def test_o_TRF_no_tipo_de_despesa_NUNCA_aparece(banco_analisesps):
    """Transferência entre contas da empresa não gera documento fiscal. Este
    corte não tem caixa para desligar, porque não foi pedido com volta."""
    from app.apps.analisesps import consultas

    semear([
        sp("1", credor="ACME", vencimento="10/09/2026",
           tipo_despesa="Material p/ Climatização"),
        sp("2", credor="ACME", vencimento="10/09/2026",
           tipo_despesa="Transferência entre contas (TRF)"),
        sp("3", credor="ACME", vencimento="10/09/2026",
           tipo_despesa="conta (trf) minúsculo"),
    ])

    assert [l["id"] for l in consultas.listar(dict(FISCAL), pagina=1)] == ["1"]
    f = {"escopo_fiscal": True, "mostrar_canceladas": True}
    assert [l["id"] for l in consultas.listar(f, pagina=1)] == ["1"]


@pytest.mark.banco
def test_o_PAINEL_conta_o_MESMO_universo_que_a_lista(banco_analisesps):
    """⚠️ O defeito que não pode voltar: painel e lista lendo universos
    diferentes. Aqui só a "1" sobrevive aos três cortes."""
    from app.apps.analisesps import consultas

    semear([
        sp("1", credor="ACME", vencimento="10/09/2026", status_pgt="Pagar"),
        sp("2", credor="ACME", vencimento="10/09/2025", status_pgt="Pagar"),
        sp("3", credor="ACME", vencimento="10/09/2026", status_pgt="Cancelado"),
        sp("4", credor="ACME", vencimento="10/09/2026", status_pgt="Pagar",
           tipo_despesa="ajuste (TRF)"),
    ])

    painel = consultas.painel_fiscal(dict(FISCAL))
    ids = [l["id"] for l in consultas.listar(dict(FISCAL), pagina=1)]
    assert ids == ["1"]
    assert painel["sem_marcacao"] == 1


@pytest.mark.banco
def test_Solicitacoes_NAO_herda_o_escopo_da_fiscal(banco_analisesps):
    """Os cortes são da tela fiscal. Em Solicitações ele veria menos SPs do que
    a planilha tem, e a conta dele deixaria de fechar com a SPsBD."""
    from app.apps.analisesps import consultas

    semear([
        sp("1", credor="ACME", vencimento="10/09/2026", status_pgt="Pagar"),
        sp("2", credor="ACME", vencimento="10/09/2020", status_pgt="Cancelado",
           tipo_despesa="ajuste (TRF)"),
    ])

    assert consultas.resumo({})["quantidade"] == 2


@pytest.mark.banco
def test_a_SP_CANCELADA_nao_e_oferecida_para_associar_a_nota(banco_analisesps):
    """Do lado da nota vale o mesmo escopo: oferecer para associar justamente
    o que não deve ser associado seria as duas metades da tela discordando."""
    from app.apps.analisesps import fiscal

    chave = _chave(CREDOR_CNPJ)
    _guardar_nota(chave, "1430", 269.00, CREDOR_CNPJ)
    semear([
        sp("1", credor="ACME", documento="29.066.773/0001-52",
           valor="269,00", vencimento="10/09/2026", status_pgt="Pagar"),
        sp("2", credor="ACME", documento="29.066.773/0001-52",
           valor="269,00", vencimento="10/09/2026", status_pgt="Cancelado"),
        sp("3", credor="ACME", documento="29.066.773/0001-52",
           valor="269,00", vencimento="10/09/2026", tipo_despesa="x (TRF)"),
    ])

    escolhidas = fiscal.sps_possiveis_das_notas([
        {"chave": chave, "valor": 269.00, "numero": "1430",
         "emitente_doc": CREDOR_CNPJ}])[chave]
    assert [c["id"] for c in escolhidas] == ["1"]


# ===========================================================================
# AS DEMAIS PARCELAS RECEBEM A MESMA NOTA — *"o registro deveria ser associado
# às demais parcelas"* (dono, 13/09/2026)
#
# Uma nota paga em três vezes gera TRÊS SPs. Associar a uma só deixa as outras
# duas eternamente "sem documentação", e o painel cobra trabalho já feito.
# ===========================================================================
FRIGELAR_DOC = "92660406000623"


def _tres_parcelas(nf="1002924", documento="92.660.406/0006-23"):
    return [sp(f"900{i}", credor="FRIGELAR COMERCIO LTDA",
               documento=documento, valor="696,34", parcela=f"{i}/3",
               nf=nf, vencimento=f"3{i}/0{7 + i}/2026")
            for i in (1, 2, 3)]


@pytest.mark.banco
def test_associar_uma_parcela_grava_a_nota_nas_TRES(banco_analisesps):
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import consultar

    chave = _chave(FRIGELAR_DOC)
    _guardar_nota(chave, "1002924", 2089.02, FRIGELAR_DOC)
    semear(_tres_parcelas())

    escolhida = {"id": "9003", "documento": "92.660.406/0006-23",
                 "nf": "1002924", "parcela": "3/3"}
    gravado = fiscal.decidir_a_mao("9003", "", chave, "MARCELO", escolhida)

    assert sorted(gravado["parcelas_irmas"]) == ["9001", "9002"]
    com_chave = {l[0] for l in consultar(
        "SELECT sp_id FROM analisesps.sp_fiscal_analise "
        " WHERE btrim(chave) <> ''")}
    assert com_chave == {"9001", "9002", "9003"}


@pytest.mark.banco
def test_a_irma_que_JA_TEM_outra_nota_nao_e_sobrescrita(banco_analisesps):
    """Quem já decidiu diferente mandou. Sobrescrever seria apagar a decisão de
    uma pessoa sem ela saber — e o desempate é dela, não do sistema."""
    from app.apps.analisesps import fiscal
    from app.apps.analisesps.db import conexao, consultar_um

    chave = _chave(FRIGELAR_DOC)
    outra = _chave("11222333000144")
    _guardar_nota(chave, "1002924", 2089.02, FRIGELAR_DOC)
    semear(_tres_parcelas())
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise (sp_id, situacao, chave) "
            "VALUES ('9001', 'ESCRITA', ?)", (outra,))
        conn.commit()

    gravado = fiscal.decidir_a_mao(
        "9003", "", chave, "MARCELO",
        {"id": "9003", "documento": "92.660.406/0006-23", "nf": "1002924",
         "parcela": "3/3"})

    assert gravado["parcelas_irmas"] == ["9002"]
    assert consultar_um("SELECT chave FROM analisesps.sp_fiscal_analise "
                        " WHERE sp_id = '9001'")[0] == outra


@pytest.mark.banco
def test_parcela_de_OUTRO_numero_de_nota_nao_e_irma(banco_analisesps):
    """⚠️ O laço forte é o nº da nota. Sem ele, "parcela 2/3 de 696,34"
    casaria com qualquer outro parcelamento de mesmo valor do mesmo credor —
    que num fornecedor de aluguel mensal é o caso comum, não a exceção."""
    from app.apps.analisesps import fiscal

    chave = _chave(FRIGELAR_DOC)
    _guardar_nota(chave, "1002924", 2089.02, FRIGELAR_DOC)
    semear(_tres_parcelas() + [
        sp("9101", credor="FRIGELAR COMERCIO LTDA",
           documento="92.660.406/0006-23", valor="696,34", parcela="2/3",
           nf="7777777", vencimento="30/09/2026")])

    gravado = fiscal.decidir_a_mao(
        "9003", "", chave, "MARCELO",
        {"id": "9003", "documento": "92.660.406/0006-23", "nf": "1002924",
         "parcela": "3/3"})
    assert "9101" not in gravado["parcelas_irmas"]


@pytest.mark.banco
def test_parcela_de_OUTRO_denominador_nao_e_irma(banco_analisesps):
    """2/3 é irmã de 3/3; 2/12 é outro parcelamento."""
    from app.apps.analisesps import fiscal

    chave = _chave(FRIGELAR_DOC)
    _guardar_nota(chave, "1002924", 2089.02, FRIGELAR_DOC)
    semear(_tres_parcelas() + [
        sp("9102", credor="FRIGELAR COMERCIO LTDA",
           documento="92.660.406/0006-23", valor="696,34", parcela="2/12",
           nf="1002924", vencimento="30/09/2026")])

    gravado = fiscal.decidir_a_mao(
        "9003", "", chave, "MARCELO",
        {"id": "9003", "documento": "92.660.406/0006-23", "nf": "1002924",
         "parcela": "3/3"})
    assert "9102" not in gravado["parcelas_irmas"]


@pytest.mark.banco
def test_sem_o_numero_da_nota_no_card_NAO_ha_irma(banco_analisesps):
    """Adivinhar aqui custaria caro; conferir custa um clique."""
    from app.apps.analisesps import fiscal

    chave = _chave(FRIGELAR_DOC)
    _guardar_nota(chave, "1002924", 2089.02, FRIGELAR_DOC)
    semear(_tres_parcelas(nf=""))

    gravado = fiscal.decidir_a_mao(
        "9003", "", chave, "MARCELO",
        {"id": "9003", "documento": "92.660.406/0006-23", "nf": "",
         "parcela": "3/3"})
    assert gravado["parcelas_irmas"] == []


@pytest.mark.banco
def test_sem_CHAVE_nao_se_espalha_categoria_para_as_irmas(banco_analisesps):
    """Categoria sem nota apontada é decisão sobre AQUELA SP. Copiá-la para as
    outras seria decidir por elas sem prova nenhuma."""
    from app.apps.analisesps import fiscal

    semear(_tres_parcelas())
    gravado = fiscal.decidir_a_mao(
        "9003", "Taxas Diversas", "", "MARCELO",
        {"id": "9003", "documento": "92.660.406/0006-23", "nf": "1002924",
         "parcela": "3/3"})
    assert gravado["parcelas_irmas"] == []


# ===========================================================================
# QUEM É "NÓS" — o defeito que o dono pegou com a tela NO AR, em 13/09/2026
#
# *"Na página de nomes está aparecendo um CNPJ errado e dizendo que é da BWS,
# sendo que não tem nada a ver o CNPJ. Tem algum acusamento errado aí."*
#
# A causa estava escrita no comentário antigo: *"a BWS é sempre o
# destinatário"*. Não é — a busca baixa também as notas que a BWS EMITE, e
# nessas o destinatário é o CLIENTE. Cada cliente virava "um CNPJ nosso".
# ===========================================================================
BWS_DOC = "10656452007869"


@pytest.mark.banco
def test_o_CLIENTE_da_BWS_nao_vira_CNPJ_NOSSO(banco_analisesps):
    """A nota que a BWS emite tem o cliente como destinatário. Ele recebe de um
    emitente só; a BWS recebe de centenas — e é isso que os separa."""
    from app.apps.analisesps import credores

    # A BWS recebendo de 5 fornecedores diferentes.
    for i in range(5):
        emitente = "%014d" % (11111111000100 + i)
        _guardar_nota(_chave(emitente), str(100 + i), 10.0, emitente,
                      destinatario=BWS_DOC)
    # E a BWS emitindo UMA nota para um cliente.
    cliente = "99888777000166"
    _guardar_nota(_chave(BWS_DOC), "900", 50.0, BWS_DOC, destinatario=cliente)

    nossos = credores.cnpjs_da_empresa()
    assert nossos.get(BWS_DOC) == "notas"
    assert cliente not in nossos


@pytest.mark.banco
def test_o_CERTIFICADO_manda_e_da_CERTEZA(banco_analisesps):
    """Certeza só vem do certificado: ali não há heurística nenhuma."""
    from app.apps.analisesps import credores
    from app.apps.analisesps.db import conexao

    for i in range(5):
        emitente = "%014d" % (11111111000100 + i)
        _guardar_nota(_chave(emitente), str(100 + i), 10.0, emitente,
                      destinatario=BWS_DOC)
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.certificados "
            "(cnpj, apelido, arquivo, senha, valido_ate, ativo) "
            "VALUES (?, 'BWS', 'x'::bytea, 'y'::bytea, '2027-01-01', true)",
            (BWS_DOC,))
        conn.commit()

    assert credores.cnpjs_da_empresa().get(BWS_DOC) == "certificado"
    achado = credores.suspeita_de_cnpj_errado(
        BWS_DOC, ["FORNECEDOR QUALQUER"],
        nossos=credores.cnpjs_da_empresa())
    assert achado["grau"] == "certeza"


@pytest.mark.banco
def test_destinatario_de_POUCOS_emitentes_nao_e_nosso(banco_analisesps):
    """Um destinatário que recebe de dois fornecedores é um cliente, não a
    empresa. O corte existe para o alarme não gritar à toa."""
    from app.apps.analisesps import credores

    outro = "55444333000122"
    for i in range(2):
        emitente = "%014d" % (11111111000100 + i)
        _guardar_nota(_chave(emitente), str(300 + i), 10.0, emitente,
                      destinatario=outro)

    assert outro not in credores.cnpjs_da_empresa()


# ===========================================================================
# A BUSCA QUE FALHA TEM DE DEIXAR RASTRO — 13/09/2026
#
# *"O certificado continua uma incógnita. Eu coloco o certificado, boto a
# senha, ele aceita, eu acho que a senha está certa, o certificado está
# válido. Mas simplesmente nada é feito, nada é executado, e eu não sei o que
# está acontecendo."*
#
# A tela lia só o ponteiro, e o ponteiro só era escrito quando a busca DAVA
# CERTO. O caso em que ele mais precisa saber o que houve era o único que não
# contava nada.
# ===========================================================================
@pytest.mark.banco
def test_a_busca_que_FALHA_grava_a_hora_e_o_motivo(banco_analisesps):
    from app.apps.analisesps import sefaz

    sefaz.registrar_falha("10656452007869", "NFE",
                          "certificado recusado pela Receita")

    estado = [e for e in sefaz.estado_das_buscas() if e["tipo"] == "NFE"][0]
    assert estado["falhou"] is True
    assert estado["motivo_da_falha"] == "certificado recusado pela Receita"
    assert estado["consultado_em"] is not None
    assert estado["em_dia"] is False


@pytest.mark.banco
def test_a_falha_NAO_MEXE_no_ponteiro_do_que_ja_foi_lido(banco_analisesps):
    """⚠️ O ponteiro é só do que entrou de verdade. Se a falha o zerasse, a
    próxima rodada releria tudo e bateria no limite da Receita — que é como se
    perde o acesso ao serviço por consumo indevido."""
    from app.apps.analisesps import sefaz

    sefaz.gravar_ponteiro("10656452007869", "NFE", "120", "500",
                          recado="", documentos=42)
    sefaz.registrar_falha("10656452007869", "NFE", "rede fora do ar")

    p = sefaz.ponteiro("10656452007869", "NFE")
    assert p["ultimo_nsu"].lstrip("0") == "120"
    assert p["maior_nsu"].lstrip("0") == "500"
    assert p["documentos"] == 42


@pytest.mark.banco
def test_uma_busca_BEM_SUCEDIDA_apaga_a_marca_de_falha(banco_analisesps):
    """A falha de ontem não pode continuar assustando depois de a busca voltar
    a funcionar."""
    from app.apps.analisesps import sefaz

    sefaz.registrar_falha("10656452007869", "NFE", "rede fora do ar")
    sefaz.gravar_ponteiro("10656452007869", "NFE", "10", "10",
                          recado="", documentos=3)

    estado = [e for e in sefaz.estado_das_buscas() if e["tipo"] == "NFE"][0]
    assert estado["falhou"] is False
    assert estado["em_dia"] is True
