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
    """Na planilha o centro de custo às vezes vem com mais de um código na
    mesma célula. Igualdade exata perderia essas linhas."""
    from app.apps.analisesps import consultas
    semear([
        sp("1", centro_custo="OBRA-12"),
        sp("2", centro_custo="OBRA-12 / OBRA-13"),
        sp("3", centro_custo="OBRA-99"),
    ])
    achados = {l["id"] for l in consultas.listar({"centro_custo": ["OBRA-12"]})}
    assert achados == {"1", "2"}


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
    lote.salvar("Pagar amanhã\n111", "operador")
    guardado = lote.ler()
    assert guardado["conteudo"] == "Pagar amanhã\n111"
    assert guardado["salvo_por"] == "operador"
    assert guardado["salvo_em"] is not None


def test_salvar_o_lote_de_novo_substitui_e_nao_acumula(banco_analisesps):
    """A tabela tem uma linha só, e é de propósito — o lote é um só."""
    from app.apps.analisesps import lote
    from app.apps.analisesps.db import consultar_um
    lote.salvar("primeiro", "a")
    lote.salvar("segundo", "b")
    assert consultar_um("SELECT count(*) FROM analisesps.lote")[0] == 1
    assert lote.ler()["conteudo"] == "segundo"


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
