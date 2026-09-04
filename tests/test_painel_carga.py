# -*- coding: utf-8 -*-
"""
A CARGA: o caminho que traz os dados do OMIE e grava no banco.

Este arquivo existe por causa de um defeito que chegou em produção. A primeira
carga morreu com:

    'ConexaoCompat' object has no attribute 'cursor'

Parte do código do espelho pede `conn.cursor()` e chama `executemany` nele. A
camada de compatibilidade traduzia os marcadores `?` no caminho que passa pela
conexão, mas não existia caminho pelo cursor. Nenhum teste pegou porque todos
os testes de banco exercitavam a LEITURA (as telas) e a MONTAGEM do fato —
nunca a gravação vinda do OMIE.

Então o que se prova aqui é a escrita de verdade, contra um Postgres de verdade:
títulos, rateio, movimentos e catálogos entrando no banco, com a mesma forma de
registro que a API do OMIE devolve. Sem rede: os registros são montados no
próprio teste.
"""
from __future__ import annotations

import os
import datetime as dt

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture()
def espelho_limpo():
    """Banco de teste com as tabelas do painel e o espelho vazio."""
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner

    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), f"migração falhou: {resultado}"

    tabelas = ("fato", "titulos", "rateio", "movimentos", "cat", "clientes",
               "contas_correntes", "depto_projeto", "sync_state")

    def _limpar():
        with painel_db.conexao() as conn:
            for tabela in tabelas:
                conn.execute(f"TRUNCATE TABLE {tabela}")
            conn.commit()

    _limpar()
    yield
    _limpar()
    painel_db._engine = None


def _titulo_do_omie(codigo, valor=1000.0, natureza="R", **extra):
    """Um registro no formato que a API do OMIE devolve na listagem."""
    base = {
        "codigo_lancamento_omie": codigo,
        "valor_documento": valor,
        "codigo_categoria": "1.01",
        "codigo_cliente_fornecedor": 555,
        "id_conta_corrente": 7,
        "numero_documento": f"NF{codigo}",
        "numero_documento_fiscal": "",
        "numero_pedido": "",
        "numero_parcela": "1/1",
        "codigo_tipo_documento": "NFS",
        "status_titulo": "RECEBIDO",
        "data_emissao": "01/03/2025",
        "data_vencimento": "10/03/2025",
        "data_previsao": "10/03/2025",
        "valor_ir": 0, "valor_iss": 0, "valor_inss": 0,
        "valor_pis": 0, "valor_cofins": 0, "valor_csll": 0,
        "info": {"dInc": "01/03/2025", "dAlt": "05/03/2025",
                 "hAlt": "10:00:00", "cImpAPI": "N"},
        "distribuicao": [
            {"cCodDep": "D1", "cDesDep": "Obra Um", "nPerDep": 100.0,
             "nValDep": valor},
        ],
    }
    base.update(extra)
    return base


def _movimento_do_omie(codigo_titulo, pago=1000.0):
    """Um movimento como o OMIE devolve: dois blocos, `detalhes` e `resumo`.

    Errar essa forma foi o que fez o primeiro rascunho deste teste passar por
    cima do produto — o movimento era ignorado por não achar o título."""
    return {
        "detalhes": {
            "nCodTitulo": codigo_titulo, "cNatureza": "R",
            "cGrupo": "CONTA_A_RECEBER", "cStatus": "RECEBIDO",
            "cCodCateg": "1.01", "nCodCC": 7, "nCodCliente": 555,
            "dDtPagamento": "15/03/2025", "dDtVenc": "10/03/2025",
            "dDtEmissao": "01/03/2025", "dDtRegistro": "01/03/2025",
            "nValorTitulo": pago,
        },
        "resumo": {
            "cLiquidado": "S", "nValPago": pago, "nValLiquido": pago,
            "nValAberto": 0.0, "nJuros": 0.0, "nMulta": 0.0, "nDesconto": 0.0,
        },
    }


def _categoria_do_omie():
    """Categoria como o OMIE devolve. `codigo_dre` preenchido é o que faz a
    categoria entrar no DRE em vez do fluxo de caixa — e a descrição do DRE vem
    aninhada em `dadosDRE`."""
    return {
        "codigo": "1.01", "descricao": "Receita de Obras",
        "categoria_superior": "1", "natureza": "R", "conta_inativa": "N",
        "codigo_dre": "3.01", "transferencia": "N", "totalizadora": "N",
        "dadosDRE": {"descricaoDRE": "Receita Bruta"},
    }


# ---------------------------------------------------------------------------
# 1. O defeito que chegou em produção
# ---------------------------------------------------------------------------
def test_gravar_titulos_funciona_pelo_cursor(espelho_limpo):
    """Este é O teste. `gravar_titulos` pede `conn.cursor()` e grava por ele —
    é exatamente o caminho que quebrava a primeira carga."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    registros = [_titulo_do_omie(1), _titulo_do_omie(2, valor=2500.0)]
    with conexao() as conn:
        quantos, rateios, problemas = espelho.gravar_titulos(conn, registros, "R")

    assert quantos == 2
    assert rateios == 2
    assert problemas == []
    assert consultar("SELECT COUNT(*) FROM titulos")[0][0] == 2
    assert consultar("SELECT COUNT(*) FROM rateio")[0][0] == 2
    (valor,) = consultar(
        "SELECT valor_documento FROM titulos WHERE codigo_lancamento_omie = ?", (2,))[0]
    assert float(valor) == pytest.approx(2500.0)


def test_gravar_o_mesmo_titulo_duas_vezes_nao_duplica(espelho_limpo):
    """A sincronização roda todo dia sobre títulos que já existem. Se cada
    passada criasse linha nova, a base dobraria de tamanho por semana."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho.gravar_titulos(conn, [_titulo_do_omie(1, valor=1000.0)], "R")
        espelho.gravar_titulos(conn, [_titulo_do_omie(1, valor=1800.0)], "R")

    assert consultar("SELECT COUNT(*) FROM titulos")[0][0] == 1
    assert consultar("SELECT COUNT(*) FROM rateio")[0][0] == 1
    (valor,) = consultar("SELECT valor_documento FROM titulos")[0]
    assert float(valor) == pytest.approx(1800.0)     # ficou o valor novo


def test_a_observacao_do_backfill_nao_e_apagada(espelho_limpo):
    """A observação NÃO vem na listagem do OMIE, só na consulta título a
    título. Se a sincronização diária a sobrescrevesse com vazio, jogaria fora
    horas de backfill toda madrugada — e é dela que sai a chave da medição."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho.gravar_titulos(conn, [_titulo_do_omie(1)], "R")
        conn.execute("UPDATE titulos SET observacao = ? "
                     " WHERE codigo_lancamento_omie = 1",
                     ("OBRA1|Medição No: 3",))
        conn.commit()
        espelho.gravar_titulos(conn, [_titulo_do_omie(1)], "R")   # de novo

    (observacao,) = consultar("SELECT observacao FROM titulos")[0]
    assert observacao == "OBRA1|Medição No: 3"


def test_rateio_que_nao_fecha_com_o_documento_e_denunciado(espelho_limpo):
    """Rateio que não soma o valor do título é inconsistência de origem. O
    título é gravado assim mesmo — mas o problema volta na lista, não some."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    torto = _titulo_do_omie(9, valor=1000.0)
    torto["distribuicao"] = [{"cCodDep": "D1", "cDesDep": "Obra Um",
                              "nPerDep": 60.0, "nValDep": 600.0}]
    with conexao() as conn:
        _q, _r, problemas = espelho.gravar_titulos(conn, [torto], "P")
    assert problemas and problemas[0][0] == 9
    assert "rateio" in problemas[0][1]


# ---------------------------------------------------------------------------
# 2. O resto da gravação
# ---------------------------------------------------------------------------
def test_gravar_movimentos(espelho_limpo):
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        gravados, ignorados = espelho.gravar_movimentos(conn, [_movimento_do_omie(1)])
    assert (gravados, ignorados) == (1, 0)
    assert consultar("SELECT COUNT(*) FROM movimentos")[0][0] == 1
    (pago, liquidado) = consultar(
        "SELECT nvalpago, cliquidado FROM movimentos WHERE ncodtitulo = 1")[0]
    assert float(pago) == pytest.approx(1000.0)
    assert liquidado == "S"


def test_movimento_sem_titulo_e_ignorado_e_contado(espelho_limpo):
    """Movimento que não aponta para título nenhum não tem onde entrar. É
    descartado — mas a contagem volta, para o número não sumir calado."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    orfao = _movimento_do_omie(1)
    orfao["detalhes"].pop("nCodTitulo")
    with conexao() as conn:
        gravados, ignorados = espelho.gravar_movimentos(conn, [orfao])
    assert (gravados, ignorados) == (0, 1)
    assert consultar("SELECT COUNT(*) FROM movimentos")[0][0] == 0


def test_gravar_catalogos(espelho_limpo):
    """Categorias, clientes e contas correntes — os nomes que a tela mostra."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho.gravar_categorias(conn, [_categoria_do_omie()])
        espelho.gravar_clientes(conn, [
            {"codigo_cliente_omie": 555, "razao_social": "CLIENTE TAL LTDA",
             "nome_fantasia": "CLIENTE", "cnpj_cpf": "12.345.678/0001-90"}])
        espelho.gravar_contas_correntes(conn, [
            {"nCodCC": 7, "descricao": "Bradesco C/C 1234-5", "tipo_conta": "CC",
             "codigo_banco": "237", "agencia": "1234", "numero_conta": "5678",
             "inativo": "N"}])

    assert consultar("SELECT descricao FROM cat WHERE codigo = '1.01'")[0][0] == \
        "Receita de Obras"
    assert consultar("SELECT razao_social FROM clientes")[0][0] == "CLIENTE TAL LTDA"
    (nome,) = consultar("SELECT descricao FROM contas_correntes WHERE codigo = 7")[0]
    assert nome == "Bradesco C/C 1234-5"


def test_o_de_para_de_obra_e_projeto_grava(espelho_limpo):
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import projetos

    with conexao() as conn:
        assert projetos.gravar(conn, {"D1": "ALFA", "D2": "BETA"}) == 2
    assert dict(consultar("SELECT ccoddep, projeto FROM depto_projeto")) == \
        {"D1": "ALFA", "D2": "BETA"}


# ---------------------------------------------------------------------------
# 3. Da carga até a tela, sem atalho
# ---------------------------------------------------------------------------
def test_da_gravacao_do_omie_ate_o_numero_na_tela(espelho_limpo):
    """O caminho inteiro numa passada só: grava como o OMIE manda, reconstrói o
    fato e pergunta à consulta que a tela usa. É o que a primeira carga faz."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho, fato, projetos

    receber = _titulo_do_omie(1, valor=1000.0, natureza="R")
    pagar = _titulo_do_omie(2, valor=400.0, status_titulo="PAGO")
    pagar["distribuicao"] = [{"cCodDep": "D1", "cDesDep": "Obra Um",
                              "nPerDep": 100.0, "nValDep": 400.0}]

    with conexao() as conn:
        espelho.gravar_titulos(conn, [receber], "R")
        espelho.gravar_titulos(conn, [pagar], "P")
        espelho.gravar_categorias(conn, [_categoria_do_omie()])
        espelho.gravar_clientes(conn, [
            {"codigo_cliente_omie": 555, "razao_social": "CLIENTE TAL LTDA",
             "nome_fantasia": "", "cnpj_cpf": ""}])
        projetos.gravar(conn, {"D1": "ALFA"})
        linhas = fato.reconstruir_fato(conn)

    assert linhas == 2
    resultado = consultas.resultado_dre(consultas.Filtros())
    assert round(resultado["receita"], 2) == 1000.00
    assert round(resultado["despesa"], 2) == -400.00
    assert round(resultado["resultado"], 2) == 600.00

    obras = consultas.opcoes_de_filtro()["obras"]
    assert obras == ["Obra Um"]
    assert consultas.opcoes_de_filtro()["projetos"] == ["ALFA"]


# ---------------------------------------------------------------------------
# 4. A janela do incremental
# ---------------------------------------------------------------------------
def test_apagar_movimentos_da_janela(espelho_limpo):
    """A atualização diária apaga os movimentos do período que vai rebaixar,
    para não duplicar. A consulta que faz isso é específica do Postgres
    (`to_date` sobre texto) e nunca tinha sido executada — o mesmo tipo de
    ponto cego que deixou passar o defeito do cursor.

    O `~` testa o formato ANTES de converter: uma data mal preenchida no OMIE
    derrubaria a atualização inteira se fosse direto para o `to_date`."""
    import datetime as dt

    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    def _movimento(codigo, data_pagamento):
        mov = _movimento_do_omie(codigo)
        mov["detalhes"]["dDtPagamento"] = data_pagamento
        return mov

    with conexao() as conn:
        espelho.gravar_movimentos(conn, [
            _movimento(1, "05/03/2025"),      # antes da janela
            _movimento(2, "15/03/2025"),      # dentro
            _movimento(3, "20/03/2025"),      # dentro
            _movimento(4, "05/04/2025"),      # depois
            _movimento(5, ""),                # sem data
            _movimento(6, "data ruim"),       # lixo: não pode derrubar a query
        ])
        apagados = espelho._apagar_movimentos_janela(
            conn, dt.date(2025, 3, 10), dt.date(2025, 3, 31))

    assert apagados == 2
    restantes = {c for (c,) in consultar("SELECT ncodtitulo FROM movimentos")}
    assert restantes == {1, 4, 5, 6}


def test_as_duas_datas_chegam_do_omie_ate_o_fato(espelho_limpo):
    """O caminho inteiro para as colunas novas: o título traz o vencimento, o
    movimento traz o pagamento, e o fato tem de guardar cada um no seu lugar.

    Sem este teste, `data_vencimento` e `data_pagamento` poderiam ficar vazias
    na produção sem nada acusar — a tela mostraria traço e pareceria que o OMIE
    não tem a informação.
    """
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato, projetos

    # vence 10/03, pago 15/03: cinco dias de atraso
    titulo = _titulo_do_omie(1, valor=1000.0, natureza="R")
    em_aberto = _titulo_do_omie(2, valor=500.0, natureza="R",
                                status_titulo="A RECEBER")

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo, em_aberto], "R")
        espelho.gravar_movimentos(conn, [_movimento_do_omie(1)])
        espelho.gravar_categorias(conn, [_categoria_do_omie()])
        espelho.gravar_clientes(conn, [
            {"codigo_cliente_omie": 555, "razao_social": "CLIENTE TAL LTDA",
             "nome_fantasia": "", "cnpj_cpf": ""}])
        projetos.gravar(conn, {"D1": "ALFA"})
        fato.reconstruir_fato(conn)

    linhas = {c: (dv, dp) for c, dv, dp in consultar(
        "SELECT codigo_lancamento, data_vencimento, data_pagamento FROM fato "
        " ORDER BY codigo_lancamento")}

    venc, pago = linhas[1]
    assert venc == dt.date(2025, 3, 10)
    assert pago == dt.date(2025, 3, 15)

    # o que não foi recebido não pode ganhar data de pagamento: seria dizer que
    # entrou dinheiro que não entrou
    venc2, pago2 = linhas[2]
    assert venc2 == dt.date(2025, 3, 10)
    assert pago2 is None
