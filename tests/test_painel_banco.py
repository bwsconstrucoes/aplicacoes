"""Painel Financeiro — o SQL provado com banco de verdade.

Todos os outros testes do painel dublam a conexão, e por isso provam a REGRA
mas não o SQL. E o painel é quase todo SQL: as telas não abrem mais a base, elas
pedem a soma ao Postgres. Um erro no `WHERE`, no `GROUP BY` ou no tipo de uma
coluna não quebraria nada — devolveria o número errado, calado. É o pior defeito
possível num painel financeiro.

Aqui o Postgres é real e descartável (mesmo banco do `conftest`, ver a trava de
segurança lá): as migrações do painel são aplicadas de verdade, um punhado de
lançamentos é inserido, e cada consulta é conferida contra o valor calculado à
mão no próprio teste.

O cenário é pequeno de propósito — poucos números, todos verificáveis de cabeça:

    2025, obra CASA (projeto ALFA)
      R1  receita  1.000  RECEBIDA em 06/2025  (+ 100 retidos pelo cliente)
      D1  despesa    400  PAGA em 06/2025
      D2  despesa    250  A PAGAR
    2025, obra PONTE (projeto BETA)
      R2  receita  2.000  A RECEBER, vence 09/2025
      D3  despesa    900  PAGA em 07/2025
    2024, obra CASA (projeto ALFA)
      D4  despesa    100  PAGA em 03/2024
    obra CASA, SEM DATA (backlog, não pertence a ano nenhum)
      R3  receita    300  A RECEBER
    2025, obra CASA — TRANSFERÊNCIA entre contas (nem resultado nem caixa)
      T1  entrada    500  RECEBIDA
"""
from __future__ import annotations

import datetime as dt
import os

import pytest

pytestmark = pytest.mark.banco

REC = "1. Contas a Receber"
PAG = "2. Contas a Pagar"


# ---------------------------------------------------------------------------
# Preparação
# ---------------------------------------------------------------------------
def _linha(**campos):
    """Uma linha do fato, com o resto preenchido por padrão."""
    base = {
        "codigo_lancamento": 1, "tipo": PAG, "analise": "DRE",
        "situacao": "A Pagar", "situacao_vencimento": "A vencer",
        "categoria": "Materiais", "grupo": "Materiais Aplicados",
        "projeto": "ALFA", "departamento": "CASA",
        "razao_social": "FORNECEDOR X", "cnpj_cpf": "", "numero_documento": "",
        "pedido_compra": "", "conta_corrente": "", "observacao": "", "link": "",
        "data": dt.date(2025, 6, 10), "ano": 2025, "mes": 6,
        "pago_recebido": 0, "a_pagar_receber": 0, "juros": 0, "multa": 0,
    }
    base.update(campos)
    return base


CENARIO = [
    # --- 2025, obra CASA (projeto ALFA) ---
    _linha(codigo_lancamento=1, tipo=REC, situacao="Recebido",
           situacao_vencimento="Quitado", categoria="Receita de Obras",
           grupo="Receita Bruta", razao_social="CLIENTE A", pago_recebido=1000),
    # o imposto que o cliente reteve: entra no bruto, não no líquido nem no caixa
    _linha(codigo_lancamento=1, tipo=REC, situacao="Recebido",
           situacao_vencimento="Quitado", categoria="Impostos Retidos na Fonte",
           grupo="Retenções", razao_social="CLIENTE A", pago_recebido=100),
    _linha(codigo_lancamento=2, situacao="Pago", situacao_vencimento="Quitado",
           pago_recebido=-400),
    _linha(codigo_lancamento=3, a_pagar_receber=-250),

    # --- 2025, obra PONTE (projeto BETA) ---
    _linha(codigo_lancamento=4, tipo=REC, situacao="A Receber",
           categoria="Receita de Obras", grupo="Receita Bruta",
           projeto="BETA", departamento="PONTE", razao_social="CLIENTE B",
           data=dt.date(2025, 9, 30), mes=9, a_pagar_receber=2000),
    _linha(codigo_lancamento=5, situacao="Pago", situacao_vencimento="Quitado",
           projeto="BETA", departamento="PONTE", grupo="Despesas com Pessoal",
           categoria="Salários", pago_recebido=-900,
           data=dt.date(2025, 7, 15), mes=7),

    # --- 2024, obra CASA ---
    _linha(codigo_lancamento=6, situacao="Pago", situacao_vencimento="Quitado",
           data=dt.date(2024, 3, 5), ano=2024, mes=3, pago_recebido=-100),

    # --- a receber SEM data: backlog de hoje, sem ano de realização ---
    _linha(codigo_lancamento=8, tipo=REC, situacao="A Receber",
           categoria="Receita de Obras", grupo="Receita Bruta",
           razao_social="CLIENTE C", data=None, ano=None, mes=None,
           a_pagar_receber=300),

    # --- transferência entre contas: não é resultado nem caixa da empresa ---
    _linha(codigo_lancamento=7, tipo=REC, analise="TRF", situacao="Recebido",
           situacao_vencimento="Quitado", categoria="Transferência entre Contas",
           grupo="Transferências", pago_recebido=500),
]


@pytest.fixture(scope="module")
def painel_no_banco(request):
    """Aplica as migrações do painel no banco de teste e carrega o cenário.

    Reusa a MESMA trava do conftest: sem ERP_TEST_DATABASE_URL local e com
    'teste' no nome, nada roda. A produção não é alcançável daqui."""
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    url = url_de_teste_segura(bruto)
    os.environ["DATABASE_URL"] = url

    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner

    painel_db._engine = None                  # engine limpa, apontando para o teste
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), f"migração do painel falhou: {resultado}"

    colunas = list(CENARIO[0].keys())
    marcas = ",".join(["?"] * len(colunas))
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.executemany(
            f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
            [tuple(linha[c] for c in colunas) for linha in CENARIO])
        conn.commit()
    yield
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.commit()
    painel_db._engine = None


@pytest.fixture()
def consultas(painel_no_banco):
    from app.apps.painel import consultas as modulo
    return modulo


def reais(v):
    """Arredonda para centavos, para comparar sem sofrer com float."""
    return round(float(v), 2)


# ---------------------------------------------------------------------------
# 1. As migrações realmente rodam
# ---------------------------------------------------------------------------
def test_as_migracoes_do_painel_aplicam_num_postgres_de_verdade(painel_no_banco):
    """Este é o teste que faltava. Todo o esquema do painel foi escrito sem
    nunca ter sido executado; aqui ele é criado do zero num Postgres real."""
    from app.apps.painel import migracoes_runner
    estado = migracoes_runner.listar_estado()
    assert estado["pendentes"] == []
    assert len(estado["aplicadas"]) >= 3


def test_o_dicionario_de_categorias_foi_carregado(painel_no_banco):
    """A migração 003 traz as 110 correspondências que vinham do arquivo de
    14 MB. Se ela não rodar, toda categoria antiga cai na regra automática."""
    from app.apps.painel.db import consultar
    (quantas,) = consultar("SELECT COUNT(*) FROM categoria_de_para")[0]
    assert quantas >= 100
    (analise,) = consultar(
        "SELECT analise FROM categoria_de_para WHERE categoria = ?",
        ("Aportes Parceiros",))[0]
    assert analise == "Fluxo de Caixa"


# ---------------------------------------------------------------------------
# 2. Visão Geral — os números de cabeça
# ---------------------------------------------------------------------------
def test_resultado_sem_filtro(consultas):
    """Receita líquida 1.000 (o retido de 100 NÃO entra) + 2.000 a receber.
    Despesas 400 + 250 + 900 + 100. A transferência fica de fora."""
    r = consultas.resultado_dre(consultas.Filtros())
    assert reais(r["receita"]) == 3300.00
    assert reais(r["despesa"]) == -1650.00
    assert reais(r["resultado"]) == 1650.00
    # executado = só o que já circulou: recebeu 1.000, pagou 400+900+100
    assert reais(r["receita_exec"]) == 1000.00
    assert reais(r["despesa_exec"]) == -1400.00


def test_transferencia_entra_quando_pedida(consultas):
    """Com "incluir transferências" ligado, os 500 aparecem no caixa — e é
    justamente por isso que o padrão é excluí-las."""
    sem = consultas.caixa(consultas.Filtros(excluir_trf=True))
    com = consultas.caixa(consultas.Filtros(excluir_trf=False))
    assert reais(com["entradas"] - sem["entradas"]) == 500.00


def test_caixa_ignora_o_imposto_retido(consultas):
    """Entrou 1.000 na conta, não 1.100: os 100 o cliente reteve."""
    c = consultas.caixa(consultas.Filtros())
    assert reais(c["entradas"]) == 1000.00
    assert reais(c["saidas"]) == -1400.00
    assert reais(c["geracao"]) == -400.00


def test_filtro_de_ano_nao_descarta_o_que_esta_em_aberto(consultas):
    """O título a receber de 2.000 não tem data de realização. Filtrar por 2025
    não pode escondê-lo — senão "a receber" some da tela.

    Este é o caso que motivou a regra `ano = ANY(...) OR data IS NULL`, e só o
    SQL executado prova que ela funciona."""
    de_2025 = consultas.resultado_dre(consultas.Filtros(anos=[2025]))
    assert reais(de_2025["receita"]) == 3300.00     # 1.000 + 2.000 + os 300 sem data
    assert reais(de_2025["despesa"]) == -1550.00    # a despesa de 2024 fica de fora


def test_filtro_de_ano_sozinho(consultas):
    """Em 2024 só houve a despesa de 100 — mas os 300 sem data aparecem
    aqui também, porque backlog não pertence a ano nenhum."""
    r = consultas.resultado_dre(consultas.Filtros(anos=[2024]))
    assert reais(r["despesa"]) == -100.00
    assert reais(r["receita"]) == 300.00


def test_dre_por_ano_separa_os_exercicios(consultas):
    """No gráfico por ano, o que NÃO tem ano fica de fora — não há barra onde
    colocá-lo. É o oposto da regra do filtro, e de propósito."""
    anos = {linha["ano"]: linha for linha in consultas.dre_por_ano(consultas.Filtros())}
    assert set(anos) == {2024, 2025}                  # nada de barra "sem ano"
    assert reais(anos[2024]["despesa"]) == -100.00
    assert reais(anos[2025]["receita"]) == 3000.00    # os 300 sem ano não entram
    assert reais(anos[2025]["despesa"]) == -1550.00


# ---------------------------------------------------------------------------
# 3. Filtros de projeto e obra
# ---------------------------------------------------------------------------
def test_filtro_de_projeto(consultas):
    r = consultas.resultado_dre(consultas.Filtros(projetos=["BETA"]))
    assert reais(r["receita"]) == 2000.00
    assert reais(r["despesa"]) == -900.00


def test_filtro_de_obra(consultas):
    r = consultas.resultado_dre(consultas.Filtros(departamentos=["CASA"]))
    assert reais(r["receita"]) == 1300.00      # 1.000 recebidos + 300 sem data
    assert reais(r["despesa"]) == -750.00      # 400 + 250 + 100


def test_filtros_combinados_se_somam(consultas):
    """Projeto ALFA E ano 2025: fica de fora a despesa de 2024."""
    r = consultas.resultado_dre(consultas.Filtros(projetos=["ALFA"], anos=[2025]))
    assert reais(r["despesa"]) == -650.00       # 400 + 250
    assert reais(r["receita"]) == 1300.00       # 1.000 + os 300 sem data


def test_opcoes_de_filtro_saem_da_base(consultas):
    o = consultas.opcoes_de_filtro()
    assert o["anos"] == [2025, 2024]
    assert o["projetos"] == ["ALFA", "BETA"]
    assert o["obras"] == ["CASA", "PONTE"]


# ---------------------------------------------------------------------------
# 4. DRE
# ---------------------------------------------------------------------------
def test_dre_fecha_de_cima_a_baixo(consultas):
    """Bruta − retenções = líquida; líquida + despesas = resultado. Se alguma
    linha não fechar, a tela mente com aparência de planilha."""
    d = consultas.dre_linhas(consultas.Filtros())
    assert reais(d["receita_bruta"]["comprometido"]) == 3400.00
    assert reais(d["retencoes"]["comprometido"]) == 100.00
    assert reais(d["receita_liquida"]["comprometido"]) == 3300.00
    assert reais(d["total_despesas"]["comprometido"]) == -1650.00
    assert reais(d["resultado"]["comprometido"]) == 1650.00
    soma_grupos = sum(g["comprometido"] for g in d["despesas"])
    assert reais(soma_grupos) == reais(d["total_despesas"]["comprometido"])


def test_despesas_por_grupo_e_por_categoria(consultas):
    grupos = {d["nome"]: d["valor"] for d in
              consultas.despesas_por(consultas.Filtros(), quebra="grupo")}
    assert reais(grupos["Materiais Aplicados"]) == -750.00
    assert reais(grupos["Despesas com Pessoal"]) == -900.00

    categorias = {d["nome"]: d["valor"] for d in
                  consultas.despesas_por(consultas.Filtros(), quebra="categoria")}
    assert reais(categorias["Salários"]) == -900.00


def test_visao_de_despesa_muda_o_numero(consultas):
    """"Só o que foi pago" e "só o que falta pagar" têm de somar o comprometido."""
    f = consultas.Filtros(departamentos=["CASA"])
    pago = sum(d["valor"] for d in consultas.despesas_por(f, visao="executado"))
    falta = sum(d["valor"] for d in consultas.despesas_por(f, visao="aberto"))
    tudo = sum(d["valor"] for d in consultas.despesas_por(f, visao="comprometido"))
    assert reais(pago) == -500.00              # 400 + 100
    assert reais(falta) == -250.00
    assert reais(pago + falta) == reais(tudo)


def test_receita_por_obra_abre_o_bruto(consultas):
    obras = {r["obra"]: r for r in consultas.receita_por_obra(consultas.Filtros())}
    casa = obras["CASA"]
    assert reais(casa["recebido"]) == 1000.00
    assert reais(casa["retido"]) == 100.00
    assert reais(casa["a_receber"]) == 300.00
    assert reais(casa["bruto"]) == 1400.00
    assert reais(obras["PONTE"]["a_receber"]) == 2000.00


def test_maiores_credores_vem_do_maior_para_o_menor(consultas):
    credores = consultas.top_credores(consultas.Filtros())
    assert credores[0]["nome"] == "FORNECEDOR X"
    assert reais(credores[0]["pago"]) == -1400.00
    assert reais(credores[0]["aberto"]) == -250.00


# ---------------------------------------------------------------------------
# 5. Fluxo de caixa
# ---------------------------------------------------------------------------
def test_fluxo_mensal_acumula_na_ordem(consultas):
    meses = consultas.caixa_por_mes(consultas.Filtros())
    rotulos = [m["rotulo"] for m in meses]
    assert rotulos == ["03/2024", "06/2025", "07/2025"]
    assert reais(meses[0]["liquido"]) == -100.00
    assert reais(meses[1]["liquido"]) == 600.00     # +1.000 −400
    assert reais(meses[2]["liquido"]) == -900.00
    assert reais(meses[-1]["acumulado"]) == -400.00
    # o acumulado do último mês é a geração de caixa total
    assert reais(meses[-1]["acumulado"]) == reais(consultas.caixa(consultas.Filtros())["geracao"])


# ---------------------------------------------------------------------------
# 6. Resultado por obra e execução
# ---------------------------------------------------------------------------
def test_resultado_por_projeto_ordena_do_melhor_para_o_pior(consultas):
    """Também prova a correção do `ORDER BY`: com `2 + 3` a ordem seria
    arbitrária, e o pior projeto poderia aparecer no topo."""
    itens = consultas.resultado_por(consultas.Filtros(), nivel="projeto")
    assert [i["nome"] for i in itens] == ["BETA", "ALFA"]
    assert reais(itens[0]["resultado"]) == 1100.00    # BETA: 2.000 − 900
    assert reais(itens[1]["resultado"]) == 550.00     # ALFA: 1.300 − 750


def test_resultado_por_obra_usa_o_departamento(consultas):
    itens = {i["nome"]: i for i in
             consultas.resultado_por(consultas.Filtros(), nivel="obra")}
    assert set(itens) == {"CASA", "PONTE"}
    assert reais(itens["CASA"]["despesa"]) == -750.00


def test_comprometido_vs_executado_calcula_a_fracao(consultas):
    """CASA deve 750 no total e já pagou 500 -> 66,7% andado."""
    itens = {i["nome"]: i for i in
             consultas.comprometido_vs_executado(consultas.Filtros(),
                                                 nivel="obra", tipo="pagar")}
    casa = itens["CASA"]
    assert reais(casa["executado"]) == -500.00
    assert reais(casa["a_executar"]) == -250.00
    assert round(casa["pct"], 1) == 66.7


def test_lado_a_receber_olha_a_medicao(consultas):
    itens = {i["nome"]: i for i in
             consultas.comprometido_vs_executado(consultas.Filtros(),
                                                 nivel="obra", tipo="receber")}
    assert reais(itens["PONTE"]["executado"]) == 0.00
    assert reais(itens["PONTE"]["a_executar"]) == 2000.00
    assert round(itens["PONTE"]["pct"], 1) == 0.0
    # CASA recebeu 1.100 (líquido + retido) de 1.400 comprometidos
    assert reais(itens["CASA"]["executado"]) == 1100.00
    assert reais(itens["CASA"]["a_executar"]) == 300.00


# ---------------------------------------------------------------------------
# 7. Estado da base
# ---------------------------------------------------------------------------
def test_base_vazia_responde_certo_nos_dois_casos(painel_no_banco):
    from app.apps.painel import consultas as c
    from app.apps.painel.db import conexao
    assert c.base_vazia() is False
    with conexao() as conn:
        conn.execute("DELETE FROM fato")
        conn.commit()
    try:
        assert c.base_vazia() is True
    finally:
        colunas = list(CENARIO[0].keys())
        marcas = ",".join(["?"] * len(colunas))
        with conexao() as conn:
            conn.executemany(
                f"INSERT INTO fato ({', '.join(colunas)}) VALUES ({marcas})",
                [tuple(linha[col] for col in colunas) for linha in CENARIO])
            conn.commit()


# ---------------------------------------------------------------------------
# 8. A conexão sobrevive a ser reusada
# ---------------------------------------------------------------------------
def test_o_schema_continua_valendo_na_segunda_consulta(painel_no_banco):
    """Trava um defeito que só apareceu rodando: a primeira tela abria e a
    segunda dizia que a tabela não existia.

    O schema do painel era fixado com `SET search_path` depois de conectar. Mas
    o `SET` roda dentro de uma transação, e o pool do SQLAlchemy dá rollback ao
    devolver a conexão — desfazendo o ajuste. Agora o schema vai como parâmetro
    da conexão, que nenhum rollback alcança.

    Uma consulta só nunca pegaria isso: é preciso soltar a conexão e pedir de
    novo, que é exatamente o que acontece entre duas telas."""
    from app.apps.painel.db import conexao, consultar

    for _ in range(4):
        assert consultar("SELECT COUNT(*) FROM fato")[0][0] > 0

    # e o caminho pelo `with`, que é o usado pela sincronização
    for _ in range(3):
        with conexao() as conn:
            cur = conn.execute("SELECT current_setting('search_path')")
            assert cur.fetchone()[0].startswith("painel")
            cur.close()
