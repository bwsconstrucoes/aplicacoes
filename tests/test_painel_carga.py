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

    tabelas = ("fato", "titulos", "rateio", "movimentos",
               "movimentos_sem_titulo", "cat", "clientes",
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
    from app.apps.painel.db import conexao, consultar
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
    """Movimento que não aponta para título nenhum não vira linha do painel —
    não tem onde entrar. Mas desde a migração 012 ele também não é jogado fora:
    fica numa tabela própria, fora de todo número de tela, porque o dono
    perguntou quanto era e não havia como responder."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    orfao = _movimento_do_omie(1)
    orfao["detalhes"].pop("nCodTitulo")
    with conexao() as conn:
        gravados, ignorados = espelho.gravar_movimentos(conn, [orfao])
    assert (gravados, ignorados) == (0, 1)
    assert consultar("SELECT COUNT(*) FROM movimentos_sem_titulo")[0][0] == 1
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
    from app.apps.painel.db import conexao, consultar
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


def test_o_codigo_da_categoria_chega_no_fato(espelho_limpo):
    """A `categoria` do fato é a DESCRIÇÃO, que a tela mostra. Para ALTERAR a
    categoria de um título no OMIE é preciso o CÓDIGO, que existia no espelho e
    parava ali — a tela de saneamento teria de voltar ao espelho a cada linha
    mostrada, na tela que existe justamente para varrer milhares delas.

    A linha de imposto RETIDO não tem código: ela é sintética, não existe como
    categoria no OMIE, e portanto não pode ser alterada por lá."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato, projetos

    posicao = fato.COLUNAS_FATO.index("codigo_categoria")
    assert fato.COLUNAS_FATO.index("categoria") == posicao - 1, (
        "o código anda colado na descrição: quem mexer numa vê a outra")

    with conexao() as conn:
        espelho.gravar_titulos(conn, [_titulo_do_omie(1, valor=1000.0,
                                                     natureza="R")], "R")
        espelho.gravar_movimentos(conn, [_movimento_do_omie(1)])
        espelho.gravar_categorias(conn, [_categoria_do_omie()])
        espelho.gravar_clientes(conn, [
            {"codigo_cliente_omie": 555, "razao_social": "CLIENTE TAL LTDA",
             "nome_fantasia": "", "cnpj_cpf": ""}])
        projetos.gravar(conn, {"D1": "ALFA"})
        fato.reconstruir_fato(conn)

    linhas = consultar("SELECT categoria, codigo_categoria FROM fato")
    assert linhas, "o cenário do espelho tem de produzir linhas"
    for categoria, codigo in linhas:
        if categoria == fato.CATEGORIA_RETIDO:
            assert codigo is None, "o retido é sintético: não tem código no OMIE"
        else:
            assert codigo, f"{categoria} veio sem código de categoria"


def test_titulo_de_fornecedor_sem_cadastro_nao_fica_sem_nome(espelho_limpo):
    """13/09/2026: o dono buscou as devoluções de aporte de uma empresa pelo
    nome e achou menos do que existia. A linha cujo fornecedor não está no
    catálogo do painel saía com o nome VAZIO — e linha sem nome é invisível para
    quem procura por nome, que é como as pessoas procuram.

    A conta corrente, no mesmo arquivo, já fazia certo: sem cadastro, cai para o
    código cru. O fornecedor não fazia."""
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(9, valor=320000.0, status_titulo="PAGO")
    titulo["codigo_cliente_fornecedor"] = 4242   # este NAO vai para o catalogo

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "P")
        espelho.gravar_categorias(conn, [_categoria_do_omie()])
        fato.reconstruir_fato(conn)

    nomes = [n for (n,) in consultar("SELECT razao_social FROM fato")]
    assert nomes, "o título tem de existir no painel"
    assert all((n or "").strip() for n in nomes), \
        "nenhuma linha pode ficar sem nome: sem nome ela some de qualquer busca"
    assert "4242" in nomes[0], \
        "e o nome tem de carregar o código, senão não dá para saber de quem é"


# ===========================================================================
# A limpeza do checkpoint nunca mais derruba a carga
# ===========================================================================
# 20/09/2026. A carga completa da madrugada terminou com:
#
#     remove: path should be string, bytes or os.PathLike, not NoneType
#
# Um dos caminhos de checkpoint vem None, e os.remove(None) levanta TypeError —
# que NÃO é OSError, então passava direto pelo `except` e derrubava a varredura
# inteira. E o pior nem foi isso: a explosão impedia a ETAPA SEGUINTE, que é a
# que refaz os números das telas. A base atualizava e as telas continuavam
# mostrando número velho, sem ninguém perceber.

def test_apagar_checkpoint_com_caminho_vazio_nao_estoura():
    """O erro exato da madrugada de 20/09."""
    from app.apps.painel.sync.espelho import _ckpt_remover
    _ckpt_remover(None)                      # era aqui que estourava
    _ckpt_remover(None, "", "/nao/existe/arquivo.json")


def test_apagar_checkpoint_apaga_de_verdade_o_que_existe(tmp_path):
    """Tolerar caminho vazio não pode virar tolerar tudo: o arquivo que existe
    continua sendo apagado."""
    from app.apps.painel.sync.espelho import _ckpt_remover
    arquivo = tmp_path / "ckpt.json"
    arquivo.write_text("{}", encoding="utf-8")
    _ckpt_remover(str(arquivo), None)
    assert not arquivo.exists()


def test_varredura_de_excluidos_que_falha_nao_impede_o_recalculo(monkeypatch):
    """A lição que custou uma madrugada: achar título apagado é um extra
    semanal; refazer os números é o que faz a tela valer. O extra não pode
    custar o essencial."""
    from app.apps.painel import tarefas
    from app.apps.painel.sync import espelho, fato

    monkeypatch.setattr(espelho, "definir_progresso", lambda *a, **k: None)
    monkeypatch.setattr(espelho, "sync_incremental", lambda *a, **k: None)
    monkeypatch.setattr(espelho, "reconcile",
                        lambda *a, **k: (_ for _ in ()).throw(
                            TypeError("remove: path should be string, "
                                      "bytes or os.PathLike, not NoneType")))
    refez = []
    monkeypatch.setattr(fato, "reconstruir",
                        lambda conn: refez.append(1) or (100, 20))
    monkeypatch.setattr(tarefas, "_carimbar", lambda *a, **k: None)

    fechou = {}
    monkeypatch.setattr(tarefas, "_fechar_execucao",
                        lambda conn, eid, ok, msg, dur=None: fechou.update(
                            ok=ok, mensagem=msg))
    # `tarefas` importa a conexão DENTRO da função (`from .db import conexao`),
    # então quem tem de ser trocado é o módulo de origem, não o de destino.
    import contextlib

    from app.apps.painel import db as painel_db
    monkeypatch.setattr(painel_db, "conexao",
                        lambda: contextlib.nullcontext(object()))

    assert tarefas.executar_trabalho("completa", 1) is True
    assert refez, "os números TÊM de ser refeitos mesmo com a varredura falhando"
    assert fechou["ok"] is True
    assert "ATENÇÃO" in fechou["mensagem"], \
        "e a tela tem de dizer o que não foi feito — senão 'concluída' mente"
    assert "títulos excluídos" in fechou["mensagem"]


# ===========================================================================
# As observações dos títulos — 20/09/2026
# ===========================================================================
# A conferência mostrou: 0 de 120.772 títulos tinham a observação do OMIE. O
# dono desconfiou disso em 17/09 e estava certo. O trabalho que busca a
# observação existia, mas só dava para rodar pela linha de comando — ou seja,
# na prática nunca rodava. Virou modo de atualização, com botão.

def test_o_modo_de_observacoes_busca_as_duas_naturezas(monkeypatch):
    """A receber vai inteiro (são as medições, poucos); a pagar vai por bloco."""
    from app.apps.painel import tarefas
    from app.apps.painel.sync import espelho, fato

    chamadas = []

    def _falso(natureza=None, limite=None, **k):
        chamadas.append((natureza, limite))
        return 7

    monkeypatch.setattr(espelho, "definir_progresso", lambda *a, **k: None)
    monkeypatch.setattr(espelho, "backfill_observacoes", _falso)
    refez = []
    monkeypatch.setattr(fato, "reconstruir",
                        lambda conn: refez.append(1) or (100, 20))
    monkeypatch.setattr(tarefas, "_carimbar", lambda *a, **k: None)
    fechou = {}
    monkeypatch.setattr(tarefas, "_fechar_execucao",
                        lambda conn, eid, ok, msg, dur=None: fechou.update(
                            ok=ok, mensagem=msg))
    import contextlib

    from app.apps.painel import db as painel_db
    monkeypatch.setattr(painel_db, "conexao",
                        lambda: contextlib.nullcontext(object()))

    assert tarefas.executar_trabalho("observacoes", 1) is True
    assert chamadas == [("R", None),
                        ("P", tarefas.TETO_DE_OBSERVACOES_POR_RODADA)], \
        "a receber vai inteiro; a pagar vai limitado, senão a rodada não termina"
    assert refez, "e os números TÊM de ser refeitos — a observação vai para a linha"
    assert fechou["ok"] is True
    assert "14 observações" in fechou["mensagem"], \
        "a tela tem de dizer quantas vieram, senão não dá para saber se anda"


def test_a_busca_de_observacoes_nao_escreve_no_omie(monkeypatch):
    """Ela só CONSULTA. Se um dia alguém puser escrita aqui, este teste cai —
    e é para cair: escrever no OMIE exige a senha de execução e conferência."""
    import inspect

    from app.apps.painel.sync import espelho
    fonte = inspect.getsource(espelho.backfill_observacoes)
    for proibido in ("Alterar", "Incluir", "Excluir"):
        assert proibido not in fonte, \
            f"a busca de observações passou a chamar {proibido} no OMIE"


# ===========================================================================
# A retomada por página, exercitada de verdade — 20/09/2026
# ===========================================================================

class _OmieFalsoDeMovimentos:
    """Um OMIE de mentira com N páginas de movimentos, que anota o que pediram."""

    def __init__(self, paginas, por_pagina=2, primeiro_codigo=1):
        self.paginas = paginas
        self.por_pagina = por_pagina
        self.primeiro = primeiro_codigo
        self.pedidas = []

    def listar_movimentos(self, *, param_extra=None, max_paginas=None,
                          pagina_inicial=1):
        total = self.paginas * self.por_pagina
        for pagina in range(int(pagina_inicial), self.paginas + 1):
            self.pedidas.append(pagina)
            base = self.primeiro + (pagina - 1) * self.por_pagina
            registros = [_movimento_do_omie(base + i)
                         for i in range(self.por_pagina)]
            yield pagina, self.paginas, total, registros


def test_movimentos_retomam_na_pagina_seguinte_sem_apagar_o_que_ja_veio(espelho_limpo):
    """O ponto da retomada: o que já desceu não é baixado outra vez."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    cli = _OmieFalsoDeMovimentos(paginas=5)
    with conexao() as conn:
        # simula a carga tendo morrido depois da página 3
        espelho.gravar_movimentos(conn, [_movimento_do_omie(c) for c in range(1, 7)])
        espelho._salvar_pagina(conn, "movimentos", 3)
        espelho.carregar_movimentos_full(conn, cli)

    assert cli.pedidas == [4, 5], f"baixou páginas demais: {cli.pedidas}"
    assert consultar("SELECT COUNT(*) FROM movimentos")[0][0] == 10, \
        "as 6 linhas que já estavam mais as 4 que faltavam"


def test_comecando_do_zero_os_movimentos_sao_zerados_antes(espelho_limpo):
    """Sem isso, uma carga inteira somaria em cima da anterior — dinheiro
    dobrado, que é justamente o que não pode acontecer com movimento."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho.gravar_movimentos(conn, [_movimento_do_omie(c) for c in range(1, 7)])
        espelho.carregar_movimentos_full(conn, _OmieFalsoDeMovimentos(paginas=5))

    assert consultar("SELECT COUNT(*) FROM movimentos")[0][0] == 10, \
        "as 6 antigas tinham de ser apagadas antes das 10 novas"


def test_se_a_retomada_nao_fechar_a_conta_a_etapa_e_refeita_do_zero(espelho_limpo):
    """A defesa contra a retomada ter pulado alguma coisa não é confiar: é
    contar no fim contra o total que o próprio OMIE informou."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho

    cli = _OmieFalsoDeMovimentos(paginas=5)
    with conexao() as conn:
        # a marca diz página 3, mas a base está vazia: a conta não vai fechar
        espelho._salvar_pagina(conn, "movimentos", 3)
        queixa = espelho.carregar_movimentos_full(conn, cli)

    assert 1 in cli.pedidas, "tinha de refazer do zero ao ver que faltava"
    assert consultar("SELECT COUNT(*) FROM movimentos")[0][0] == 10
    assert queixa is None, "refez e fechou: não há do que reclamar"


def test_quando_nem_refazendo_fecha_a_tela_fica_sabendo(espelho_limpo):
    """Carga que termina com menos do que o OMIE diz existir não pode se
    anunciar como concluída e pronto."""
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    class _Mentiroso(_OmieFalsoDeMovimentos):
        def listar_movimentos(self, **k):
            # diz que são 999 e entrega 10
            for pagina, _tp, _tr, registros in super().listar_movimentos(**k):
                yield pagina, self.paginas, 999, registros

    with conexao() as conn:
        queixa = espelho.carregar_movimentos_full(conn, _Mentiroso(paginas=5))

    assert queixa and "999" in queixa and "OMIE" in queixa


def test_a_queixa_da_carga_inicial_chega_na_mensagem_da_tela(monkeypatch):
    """Carga que terminou com título faltando não pode se anunciar como
    "concluída" e mais nada — ninguém teria como desconfiar."""
    from app.apps.painel import tarefas
    from app.apps.painel.sync import espelho, fato

    monkeypatch.setattr(espelho, "definir_progresso", lambda *a, **k: None)
    monkeypatch.setattr(
        espelho, "carga_inicial",
        lambda *a, **k: ["contas a pagar: a base ficou com 118.000 títulos e o "
                         "OMIE diz que são 118.635"])
    monkeypatch.setattr(fato, "reconstruir", lambda conn: (100, 20))
    monkeypatch.setattr(tarefas, "_carimbar", lambda *a, **k: None)
    fechou = {}
    monkeypatch.setattr(tarefas, "_fechar_execucao",
                        lambda conn, eid, ok, msg, dur=None: fechou.update(
                            ok=ok, mensagem=msg))
    import contextlib

    from app.apps.painel import db as painel_db
    monkeypatch.setattr(painel_db, "conexao",
                        lambda: contextlib.nullcontext(object()))

    assert tarefas.executar_trabalho("carga_inicial", 1) is True
    assert "ATENÇÃO" in fechou["mensagem"]
    assert "118.635" in fechou["mensagem"]


# ===========================================================================
# A conta do relatório é a da BAIXA, não a da previsão — 21/09/2026
# ===========================================================================
# O dono: "No OMIE existe a conta de previsão de pagamento e existe a conta onde
# efetivamente foi realizado o pagamento. A informação que está sendo colocada
# nesse relatório analítico é exatamente a primeira. E a primeira é errada."
#
# Ele está certo, e o erro era silencioso: quem previu pagar pelo Bradesco e
# pagou pelo Itaú aparecia no Bradesco, e nenhuma análise por conta avisava.

def _conta_do_omie(codigo, descricao):
    return {"nCodCC": codigo, "descricao": descricao, "tipo_conta": "CORRENTE",
            "codigo_banco": "000", "agencia": "1", "conta_corrente": "1",
            "inativo": "N"}


def test_a_conta_do_fato_e_a_da_baixa_e_nao_a_da_previsao(espelho_limpo):
    """Título previsto na conta 7 e pago na conta 9: o relatório tem de dizer 9."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(1, valor=1000.0, natureza="R")  # id_conta_corrente = 7
    movimento = _movimento_do_omie(1, pago=1000.0)
    movimento["detalhes"]["nCodCC"] = 9                      # pago por OUTRA conta

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "R")
        espelho.gravar_movimentos(conn, [movimento])
        espelho.gravar_contas_correntes(conn, [
            _conta_do_omie(7, "Bradesco (previsão)"),
            _conta_do_omie(9, "Itaú (onde pagou)")])
        fato.reconstruir_fato(conn)

    (conta,) = consultar(
        "SELECT conta_corrente FROM fato WHERE codigo_lancamento = 1")[0]
    assert conta == "Itaú (onde pagou)", \
        f"o relatório mostrou '{conta}' — voltou a usar a conta da previsão"


def test_titulo_em_aberto_continua_mostrando_a_conta_prevista(espelho_limpo):
    """Sem baixa não há conta de baixa. A previsão é a única informação que
    existe, e esconder isso seria pior que mostrá-la."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(2, valor=500.0, natureza="R",
                             status_titulo="A RECEBER")
    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "R")
        espelho.gravar_contas_correntes(conn, [
            _conta_do_omie(7, "Bradesco (previsão)")])
        fato.reconstruir_fato(conn)

    (conta,) = consultar(
        "SELECT conta_corrente FROM fato WHERE codigo_lancamento = 2")[0]
    assert conta == "Bradesco (previsão)"


def test_pago_em_duas_contas_da_uma_linha_para_cada_conta(espelho_limpo):
    """Melhor ainda que escolher uma: desde que o título pago em parcelas vira
    uma linha por baixa, cada parcela mostra a conta DELA. A escolha "a do maior
    valor" ficou só para quando as baixas não dão para separar."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(3, valor=1000.0, natureza="R")
    pequeno = _movimento_do_omie(3, pago=300.0)
    pequeno["detalhes"]["nCodCC"] = 7
    pequeno["detalhes"]["dDtPagamento"] = "10/03/2025"
    grande = _movimento_do_omie(3, pago=700.0)
    grande["detalhes"]["nCodCC"] = 9
    grande["detalhes"]["dDtPagamento"] = "20/03/2025"

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "R")
        espelho.gravar_movimentos(conn, [pequeno, grande])
        espelho.gravar_contas_correntes(conn, [
            _conta_do_omie(7, "Bradesco"), _conta_do_omie(9, "Itaú")])
        fato.reconstruir_fato(conn)

    linhas = consultar("SELECT conta_corrente, pago_recebido FROM fato"
                       " WHERE codigo_lancamento = 3 ORDER BY data")
    assert [c for c, _v in linhas] == ["Bradesco", "Itaú"]
    assert [round(float(v), 2) for _c, v in linhas] == [300.0, 700.0]


def _perna_bancaria(codigo_titulo, pago, conta, data="15/03/2025"):
    """A OUTRA perna da mesma baixa, como o OMIE devolve: cLiquidado vazio,
    nValLiquido zero, e a conta por onde o dinheiro de fato passou."""
    m = _movimento_do_omie(codigo_titulo, pago=pago)
    m["detalhes"]["nCodCC"] = conta
    m["detalhes"]["dDtPagamento"] = data
    m["resumo"]["cLiquidado"] = ""
    m["resumo"]["nValLiquido"] = 0.0
    return m


def test_a_conta_vem_da_perna_bancaria_e_nao_do_resumo_do_titulo(espelho_limpo):
    """22/09/2026, o dono: "refiz os números do painel, mas o problema das
    contas permaneceu".

    O conserto do dia anterior trocou a FONTE (do título para o movimento) mas
    lia a perna CONSOLIDADA — que é o resumo do título e repete a conta dele.
    A conta real está na perna bancária. Previsto na 7, resumo na 7, dinheiro
    saiu da 9: o relatório tem de dizer 9."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(4, valor=1000.0, natureza="R")   # prevista: 7
    consolidada = _movimento_do_omie(4, pago=1000.0)          # resumo: 7
    bancaria = _perna_bancaria(4, 1000.0, conta=9)            # saiu da 9

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "R")
        espelho.gravar_movimentos(conn, [consolidada, bancaria])
        espelho.gravar_contas_correntes(conn, [
            _conta_do_omie(7, "Bradesco (previsão)"),
            _conta_do_omie(9, "Itaú (onde pagou)")])
        fato.reconstruir_fato(conn)

    linhas = consultar("SELECT conta_corrente, pago_recebido FROM fato"
                       " WHERE codigo_lancamento = 4")
    assert [c for c, _v in linhas] == ["Itaú (onde pagou)"], \
        f"o relatório mostrou {[c for c, _v in linhas]} — leu o resumo do título"
    # e a perna bancária NÃO dobrou o valor
    assert [round(float(v), 2) for _c, v in linhas] == [1000.0]


def test_a_conferencia_mede_quantas_contas_o_relatorio_antigo_errava(espelho_limpo):
    from app.apps.painel import consultas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import espelho

    with conexao() as conn:
        espelho.gravar_titulos(conn, [_titulo_do_omie(5, valor=1000.0, natureza="R"),
                                      _titulo_do_omie(6, valor=200.0, natureza="R")], "R")
        espelho.gravar_movimentos(conn, [
            _movimento_do_omie(5, pago=1000.0), _perna_bancaria(5, 1000.0, conta=9),
            _movimento_do_omie(6, pago=200.0), _perna_bancaria(6, 200.0, conta=7)])
        espelho.gravar_contas_correntes(conn, [
            _conta_do_omie(7, "Bradesco"), _conta_do_omie(9, "Itaú")])

    conf = consultas.conferencia_das_contas()
    assert conf["tem_perna_bancaria"] is True
    assert conf["bancaria"] == {"pernas": 2, "diferentes": 1, "sem_conta": 0}
    assert conf["consolidada"]["diferentes"] == 0
    assert [e["titulo"] for e in conf["exemplos"]] == [5]
    assert conf["exemplos"][0]["bancaria"] == "Itaú"


# ===========================================================================
# Título pago em parcelas vira UMA LINHA POR BAIXA — 21/09/2026
# ===========================================================================
# O dono: "ele foi pago em duas parcelas, em 2 dias diferentes e valores
# diferentes. Só que no relatório de despesa analítica aparece um único
# lançamento (…) do total do título. Se você for olhar no extrato, dá uma coisa.
# Aí você olha no relatório analítico, dá outro valor. Isso confunde."

def test_titulo_pago_em_duas_parcelas_vira_duas_linhas(espelho_limpo):
    """Cada linha com a SUA data e o SEU valor — é o que bate com o extrato."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(1, valor=1000.0, natureza="P", status_titulo="PAGO")
    primeira = _movimento_do_omie(1, pago=300.0)
    primeira["detalhes"]["dDtPagamento"] = "10/03/2025"
    segunda = _movimento_do_omie(1, pago=700.0)
    segunda["detalhes"]["dDtPagamento"] = "05/04/2025"

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "P")
        espelho.gravar_movimentos(conn, [primeira, segunda])
        fato.reconstruir_fato(conn)

    linhas = consultar("SELECT data, pago_recebido FROM fato"
                       " WHERE codigo_lancamento = 1 ORDER BY data")
    assert len(linhas) == 2, f"deviam ser duas linhas, vieram {len(linhas)}"
    assert [d.strftime("%d/%m/%Y") for d, _v in linhas] == \
        ["10/03/2025", "05/04/2025"], "cada parcela na SUA data"
    assert [round(float(v), 2) for _d, v in linhas] == [-300.0, -700.0]


def test_a_soma_do_titulo_nao_muda_ao_abrir_em_parcelas(espelho_limpo):
    """A divisão não pode criar nem sumir com dinheiro — só reparti-lo pelas
    datas certas. Se o total mudasse, o DRE mudaria de valor, e isso seria um
    defeito novo em vez de um conserto."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(2, valor=1000.0, natureza="P", status_titulo="PAGO")
    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "P")
        espelho.gravar_movimentos(conn, [
            _movimento_do_omie(2, pago=250.0), _movimento_do_omie(2, pago=750.0)])
        fato.reconstruir_fato(conn)

    (total,) = consultar("SELECT SUM(pago_recebido) FROM fato"
                         " WHERE codigo_lancamento = 2")[0]
    assert round(float(total), 2) == -1000.00


def test_o_saldo_em_aberto_nao_se_multiplica_pelas_parcelas(espelho_limpo):
    """O saldo é do TÍTULO, não de cada baixa. Repeti-lo multiplicaria o
    "a pagar" pelo número de parcelas — um erro que cresceria com o uso."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(3, valor=1000.0, natureza="P",
                             status_titulo="A PAGAR")
    parcial_1 = _movimento_do_omie(3, pago=200.0)
    parcial_1["resumo"]["nValAberto"] = 400.0
    parcial_2 = _movimento_do_omie(3, pago=400.0)
    parcial_2["detalhes"]["dDtPagamento"] = "20/03/2025"
    parcial_2["resumo"]["nValAberto"] = 400.0

    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "P")
        espelho.gravar_movimentos(conn, [parcial_1, parcial_2])
        fato.reconstruir_fato(conn)

    abertos = [round(float(v), 2) for (v,) in consultar(
        "SELECT a_pagar_receber FROM fato WHERE codigo_lancamento = 3")]
    assert sum(abertos) == round(sum(abertos), 2)
    assert len([v for v in abertos if v != 0]) <= 1, \
        f"o saldo apareceu em mais de uma linha: {abertos}"


def test_pago_de_uma_vez_continua_sendo_uma_linha_so(espelho_limpo):
    """O conserto não pode mexer no que já estava certo — e é a esmagadora
    maioria dos títulos."""
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    titulo = _titulo_do_omie(4, valor=800.0, natureza="P", status_titulo="PAGO")
    with conexao() as conn:
        espelho.gravar_titulos(conn, [titulo], "P")
        espelho.gravar_movimentos(conn, [_movimento_do_omie(4, pago=800.0)])
        fato.reconstruir_fato(conn)

    linhas = consultar("SELECT pago_recebido FROM fato WHERE codigo_lancamento = 4")
    assert len(linhas) == 1 and round(float(linhas[0][0]), 2) == -800.00


def test_titulo_sem_medicao_diz_isso_em_portugues():
    """21/09/2026: o dono viu "COD:11255312361" na Receita de Obra e perguntou o
    que significava. Não significava nada para quem lê — e o número ali é
    justamente o que serve para achar o lançamento no OMIE."""
    from app.apps.painel.sync.fato import rotulo_medicao
    assert rotulo_medicao("COD:11255312361") == \
        "Sem número de medição (título 11255312361)"
    # e o que JÁ era legível continua igual
    assert rotulo_medicao("MED:CEIFOR5|3") == "CEIFOR5 | Medição 3"
    assert rotulo_medicao("DOC:NF 900") == "NF 900"


# ===========================================================================
# O tipo de aporte é decidido na montagem do fato, não em cada consulta
# ===========================================================================
# 22/09/2026, o dono: "Tela montada em 126753 ms — 15 consultas ao banco". Cada
# consulta refazia a classificação (acento, dez expressões regulares, "bws" na
# contraparte) em 185 mil linhas. Agora é uma coluna (migração 017).

def test_o_fato_ja_diz_se_a_linha_e_aporte(espelho_limpo):
    from app.apps.painel.db import conexao, consultar
    from app.apps.painel.sync import espelho, fato

    aporte = _titulo_do_omie(11, valor=1000.0, natureza="R")
    aporte["codigo_categoria"] = "1.02.02"            # Aporte de Parceiro, pelo código
    comum = _titulo_do_omie(12, valor=500.0, natureza="P")

    with conexao() as conn:
        espelho.gravar_titulos(conn, [aporte], "R")
        espelho.gravar_titulos(conn, [comum], "P")
        espelho.gravar_movimentos(conn, [_movimento_do_omie(11, pago=1000.0),
                                         _movimento_do_omie(12, pago=500.0)])
        fato.reconstruir_fato(conn)

    tipos = dict(consultar("SELECT codigo_lancamento, tipo_aporte FROM fato"
                           " WHERE codigo_lancamento IN (11, 12)"))
    assert tipos[11] == "Aporte de Parceiro"
    assert tipos[12] == "", "linha que não é aporte leva '' — nunca NULL"
