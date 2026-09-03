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

    caminho = __import__("pathlib").Path(
        db_analisesps.__file__).parent / "migracoes" / "001_estrutura.sql"
    sql = caminho.read_text(encoding="utf-8")

    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.execute(text(sql))
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
    from app.apps.analisesps import consultas
    semear([
        sp("1", credor="VOTORANTIM CIMENTOS", descricao="entrega de cimento"),
        sp("2", credor="VOTORANTIM CIMENTOS", descricao="frete"),
        sp("3", credor="OUTRA EMPRESA", descricao="cimento"),
    ])
    achados = [l["id"] for l in consultas.listar({"busca": "votorantim, cimento"})]
    assert achados == ["1"]


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
        cliente.post("/analisesps/entrar", data={"senha": "op"})
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
        cliente.post("/analisesps/entrar", data={"senha": "op"})
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
        cliente.post("/analisesps/entrar", data={"senha": "op"})
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
        cliente.post("/analisesps/entrar", data={"senha": "op"})
        resposta = cliente.post("/analisesps/api/alterar", json={
            "ids": ["1", "999"], "coluna": "status_pgt", "valor": "Pago"})

    assert resposta.status_code == 404
    assert consultar_um(
        "SELECT status_pgt FROM analisesps.sps WHERE id='1'")[0] == "Pagar"
    assert consultar_um("SELECT count(*) FROM analisesps.fila")[0] == 0
