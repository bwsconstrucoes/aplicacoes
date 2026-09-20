# -*- coding: utf-8 -*-
"""
Aportes no OMIE — contra um Postgres DE VERDADE.

O briefing pede estes testes por escrito: *"teste automatizado com banco de
verdade para a regra de qual categoria vai de cada lado, e para a crítica de
transferência."*

O motivo de eles não caberem na suíte sem banco é concreto. Duas coisas aqui
vivem inteiramente dentro do SQL e não existem fora dele:

  · a DESCOBERTA do código da categoria pela descrição, que lê `painel.cat` —
    o de-para inteiro depende dela, e é ele que impede código chumbado;
  · a CRÍTICA de transferência, que é uma consulta com JOIN, filtro por data e
    tolerância de centavo em `painel.movimentos`.

A sessão dublada da suíte ignora WHERE e JOIN: os dois passariam verdes com o
filtro errado. Sem `ERP_TEST_DATABASE_URL` estes testes são pulados e a suíte
segue; o GitHub Actions sobe o banco sozinho a cada envio.
"""
from __future__ import annotations

import pathlib

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_aportes(banco, monkeypatch):
    """Sobe os dois schemas: o `analisesps` (pelas migrações de verdade) e o
    `painel`, que é de onde sai todo o de-para.

    As migrações são os MESMOS arquivos que o botão da tela aplica em
    produção — erro de sintaxe na 018 aparece aqui, não no Render."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None

    raiz = pathlib.Path(db_analisesps.__file__).parent.parent
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS painel CASCADE"))
        for caminho in sorted((raiz / "analisesps" / "migracoes").glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        # ⚠️ AS MIGRAÇÕES DO PAINEL VÃO TODAS, e isso não é excesso de zelo.
        #
        # A 010 (20/09/2026) troca as colunas de dinheiro de REAL para
        # NUMERIC — inclusive `movimentos.nvalpago`, que é justamente a coluna
        # que a crítica de transferência compara com tolerância de centavo.
        # Testando só contra a 001, a crítica seria exercitada sobre um tipo
        # que a produção não tem mais, e uma incompatibilidade de tipo
        # apareceria na tela do dono em vez de aqui.
        for caminho in sorted((raiz / "painel" / "migracoes").glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield banco
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS painel CASCADE"))
        conn.commit()
    db_analisesps._engine = None


def semear_espelho(banco, categorias=(), contas=(), movimentos=(), rateios=()):
    """Enche o espelho do OMIE com o mínimo que o teste precisa."""
    from sqlalchemy import text
    with banco.connect() as conn:
        for codigo, descricao, transferencia in categorias:
            conn.execute(text(
                "INSERT INTO painel.cat (codigo, descricao, transferencia) "
                "VALUES (:c, :d, :t)"),
                {"c": codigo, "d": descricao, "t": transferencia})
        for codigo, descricao, numero in contas:
            conn.execute(text(
                "INSERT INTO painel.contas_correntes "
                "  (codigo, descricao, numero_conta) VALUES (:c, :d, :n)"),
                {"c": codigo, "d": descricao, "n": numero})
        for ncodcc, valor, dia, categoria in movimentos:
            conn.execute(text(
                "INSERT INTO painel.movimentos "
                "  (ncodtitulo, ncodcc, nvalpago, ddtpagamento, ccodcateg) "
                "VALUES (:t, :cc, :v, :d, :cat)"),
                {"t": 1, "cc": ncodcc, "v": valor, "d": dia, "cat": categoria})
        for i, (cod, nome) in enumerate(rateios):
            conn.execute(text(
                "INSERT INTO painel.rateio "
                "  (codigo_lancamento_omie, seq, ccoddep, cdesdep, nperdep) "
                "VALUES (:l, :s, :c, :n, 100)"),
                {"l": 900 + i, "s": 1, "c": cod, "n": nome})
        conn.commit()


AS_QUATRO = [
    ("9.01.01", "Aportes BWS", "N"),
    ("9.01.02", "Aportes Parceiros", "N"),
    ("9.02.01", "Devolução de Aportes", "N"),
    ("9.02.02", "Devolução de Aportes BWS", "N"),
]
AS_CONTAS = [(7011, "BWS MATRIZ", "12345-6"), (22069, "PARCERIA OBRA X", "99999-9")]


# ---------------------------------------------------------------------------
# A DESCOBERTA DO CÓDIGO PELA DESCRIÇÃO
# ---------------------------------------------------------------------------
def test_acha_as_quatro_categorias_pela_descricao(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_QUATRO + [
        ("1.01.01", "Receita de Obra", "N"),
        ("4.05.00", "Transferência entre Contas", "S"),
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws"]["situacao"] == "achou"
    assert achados["aportes_bws"]["codigo"] == "9.01.01"
    assert achados["devolucao_aportes_bws"]["codigo"] == "9.02.02"
    assert achados["devolucao_aportes"]["codigo"] == "9.02.01"
    assert achados["aportes_parceiros"]["codigo"] == "9.01.02"


def test_acento_e_maiuscula_nao_atrapalham(banco_aportes):
    """O plano financeiro é digitado por gente: "DEVOLUCAO DE APORTES" e
    "Devolução de Aportes" são a mesma coisa."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[
        ("9.01.01", "APORTES BWS", "N"),
        ("9.02.01", "DEVOLUCAO DE APORTES", "N"),
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws"]["codigo"] == "9.01.01"
    assert achados["devolucao_aportes"]["codigo"] == "9.02.01"


def test_descricao_repetida_para_e_pede_a_decisao(banco_aportes):
    """*"Se aparecer mais de uma categoria com nome parecido, a tela tem de
    parar e me dizer, em vez de escolher uma."* Escolher a primeira seria
    exatamente o lançamento errado silencioso que o de-para existe para
    evitar."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[
        ("9.01.01", "Aportes BWS", "N"),
        ("9.09.09", "Aportes BWS", "N"),      # a mesma descrição, outro código
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws"]["situacao"] == "ambigua"
    assert achados["aportes_bws"]["codigo"] == "", "escolheu uma sozinha"
    assert len(achados["aportes_bws"]["candidatos"]) == 2


def test_descricao_que_nao_existe_e_dita_em_vez_de_inventada(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[("1.01.01", "Receita", "N")])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws"]["situacao"] == "nao_achou"
    assert achados["aportes_bws"]["codigo"] == ""


def test_aportes_bws_nao_e_confundida_com_devolucao_de_aportes_bws(banco_aportes):
    """"Aportes BWS" é pedaço de "Devolução de Aportes BWS". Casando por
    pedaço, as duas viriam como candidatas uma da outra — e o dono escolheria
    a errada metade das vezes."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_QUATRO)
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws"]["codigo"] == "9.01.01"
    assert achados["devolucao_aportes_bws"]["codigo"] == "9.02.02"
    assert achados["devolucao_aportes"]["codigo"] == "9.02.01"


def test_a_marca_de_transferencia_vem_junto(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[("9.01.01", "Aportes BWS", "S")])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws"]["transferencia"] == "S"


# ---------------------------------------------------------------------------
# O DE-PARA GRAVADO — e o SQL de gravação
# ---------------------------------------------------------------------------
def test_guardar_e_reler_o_de_para(banco_aportes):
    from app.apps.analisesps import aportes, aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_QUATRO, contas=AS_CONTAS)
    aportes_de_para.guardar_conta(aportes.MATRIZ, 7011, "Marcelo")
    aportes_de_para.guardar_conta(aportes.PARCERIA, 22069, "Marcelo")
    aportes_de_para.guardar_categoria("aportes_bws", "9.01.01", "Marcelo")

    contas = aportes_de_para.contas_configuradas()
    assert contas[aportes.MATRIZ]["codigo"] == 7011
    assert contas[aportes.MATRIZ]["descricao"] == "BWS MATRIZ"
    assert aportes_de_para.categorias_configuradas()["aportes_bws"]["codigo"] \
        == "9.01.01"


def test_apontar_de_novo_troca_em_vez_de_duplicar(banco_aportes):
    """Exercita o ON CONFLICT: o dono troca a conta da parceria quando a obra
    muda, e isso não pode virar duas linhas brigando."""
    from app.apps.analisesps import aportes, aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS)
    aportes_de_para.guardar_conta(aportes.PARCERIA, 22069, "Marcelo")
    aportes_de_para.guardar_conta(aportes.PARCERIA, 7011, "Marcelo")
    contas = aportes_de_para.contas_configuradas()
    assert contas[aportes.PARCERIA]["codigo"] == 7011
    assert contas[aportes.PARCERIA]["descricao"] == "BWS MATRIZ"


def test_conta_fora_do_espelho_e_recusada(banco_aportes):
    from app.apps.analisesps import aportes, aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS)
    with pytest.raises(aportes_de_para.SemEspelho):
        aportes_de_para.guardar_conta(aportes.MATRIZ, 123456, "Marcelo")


def test_a_categoria_sem_duvida_se_resolve_sozinha(banco_aportes):
    """⚠️ MUDANÇA DE 20/09/2026: *"eu não entendi esse gravar o de-para. Eu
    acho que não precisaria."*

    Ele tem razão. Descrição que aparece UMA vez só no plano financeiro não
    tem decisão a tomar — pedir um clique de confirmação é cerimônia, e
    cerimônia que se repete vira clique automático, que é pior do que não ter
    conferência nenhuma. O de-para continua impedindo código chumbado; só
    deixou de ser um passo."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_QUATRO, contas=AS_CONTAS)
    # NADA foi confirmado à mão, de propósito.
    assert aportes_de_para.categorias_configuradas() == {}

    resolvidas = aportes_de_para.categorias_resolvidas()
    assert resolvidas["aportes_bws"]["codigo"] == "9.01.01"
    assert resolvidas["devolucao_aportes_bws"]["codigo"] == "9.02.02"
    assert all(v.get("descoberta") for v in resolvidas.values())


def test_a_categoria_com_duvida_continua_parando_a_tela(banco_aportes):
    """O que se resolve sozinho é o caso SEM dúvida. Descrição repetida
    continua sendo decisão dele — escolher a primeira seria o lançamento
    errado silencioso que o de-para existe para evitar."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS, categorias=[
        ("9.01.01", "Aportes BWS", "N"),
        ("9.09.09", "Aportes BWS", "N"),
        ("9.01.02", "Aportes Parceiros", "N"),
        ("9.02.01", "Devolução de Aportes", "N"),
        ("9.02.02", "Devolução de Aportes BWS", "N"),
    ])
    resolvidas = aportes_de_para.categorias_resolvidas()
    assert "aportes_bws" not in resolvidas, "escolheu uma sozinha"
    assert resolvidas["aportes_parceiros"]["codigo"] == "9.01.02"
    faltas = aportes_de_para.falta_configurar()
    assert any("Aportes BWS" in f for f in faltas)
    assert not any("Aportes Parceiros" in f for f in faltas)


def test_o_que_ele_confirmou_a_mao_vale_por_cima_do_descoberto(banco_aportes):
    """É assim que ele conserta um caso que o sistema leria errado."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS, categorias=AS_QUATRO + [
        ("9.99.99", "Aportes BWS (antiga)", "N")])
    aportes_de_para.guardar_categoria("aportes_bws", "9.99.99", "Marcelo")
    resolvidas = aportes_de_para.categorias_resolvidas()
    assert resolvidas["aportes_bws"]["codigo"] == "9.99.99"


def test_so_as_contas_sobram_para_ele_apontar(banco_aportes):
    """Com o plano financeiro limpo, a ÚNICA coisa que ele precisa dizer são
    as duas contas — e uma vez só."""
    from app.apps.analisesps import aportes, aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_QUATRO, contas=AS_CONTAS)
    faltas = aportes_de_para.falta_configurar()
    assert len(faltas) == 2, faltas
    assert all("conta" in f for f in faltas)

    aportes_de_para.guardar_conta(aportes.MATRIZ, 7011, "Marcelo")
    aportes_de_para.guardar_conta(aportes.PARCERIA, 22069, "Marcelo")
    assert aportes_de_para.falta_configurar() == []


def test_o_que_falta_e_dito_em_portugues(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    # Plano financeiro VAZIO: aí as quatro categorias também faltam, porque
    # não há o que descobrir.
    semear_espelho(banco_aportes, contas=AS_CONTAS)
    faltas = aportes_de_para.falta_configurar()
    assert len(faltas) == 6, "duas contas e quatro categorias"
    aportes_de_para.guardar_conta("matriz", 7011, "Marcelo")
    assert len(aportes_de_para.falta_configurar()) == 5


# ---------------------------------------------------------------------------
# ⚠️ A REGRA DE QUAL CATEGORIA VAI DE CADA LADO — com o de-para REAL
# ---------------------------------------------------------------------------
# Os testes sem banco já percorrem a tabela inteira, mas com um de-para de
# mentira. Este fecha a volta: os códigos saem do plano financeiro de verdade,
# pelo caminho de verdade.
# ---------------------------------------------------------------------------
def test_a_tabela_inteira_com_os_codigos_vindos_do_plano_financeiro(banco_aportes):
    from app.apps.analisesps import aportes, aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_QUATRO, contas=AS_CONTAS,
                   rateios=[("OBRA-1", "Obra Um")])
    aportes_de_para.guardar_conta(aportes.MATRIZ, 7011, "Marcelo")
    aportes_de_para.guardar_conta(aportes.PARCERIA, 22069, "Marcelo")
    # Nenhuma categoria confirmada à mão: elas se resolvem sozinhas, que é o
    # caminho que o dono vai usar de verdade.
    contas = aportes_de_para.contas_configuradas()
    categorias = aportes_de_para.categorias_resolvidas()

    esperado = {
        "aporte_bws": [(7011, "9.01.01", "P"), (22069, "9.01.01", "R")],
        "devolucao_bws": [(22069, "9.02.01", "P"), (7011, "9.02.02", "R")],
        "aporte_parceiro": [(22069, "9.01.02", "R")],
        "devolucao_parceiro": [(22069, "9.02.01", "P")],
    }
    contas_da_operacao = {
        "aporte_bws": (7011, 22069), "devolucao_bws": (22069, 7011),
        "aporte_parceiro": (None, 22069), "devolucao_parceiro": (22069, None),
    }
    for operacao, linhas in esperado.items():
        origem, destino = contas_da_operacao[operacao]
        plano = aportes.planejar(
            operacao=operacao, conta_origem=origem, conta_destino=destino,
            valor="12.500,00", data="2026-09-20", fornecedor=99,
            obra="OBRA-1", quem="Marcelo", contas=contas, categorias=categorias)
        saiu = [(t["id_conta_corrente"], t["codigo_categoria"], t["natureza"])
                for t in plano["titulos"]]
        assert saiu == linhas, operacao


# ---------------------------------------------------------------------------
# A CRÍTICA DE TRANSFERÊNCIA — a consulta, não o texto
# ---------------------------------------------------------------------------
def test_acha_a_movimentacao_do_mesmo_dia_e_do_mesmo_valor(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS,
                   categorias=[("4.05.00", "Transferência entre Contas", "S")],
                   movimentos=[(7011, 12500.0, "20/09/2026", "4.05.00"),
                               (22069, 12500.0, "20/09/2026", "4.05.00")])
    achados = aportes_de_para.consultar_semelhantes(
        12500.0, "20/09/2026", [7011, 22069])
    assert len(achados) == 1
    assert achados[0]["e_transferencia"] is True
    assert {achados[0]["conta_a"], achados[0]["conta_b"]} == \
        {"BWS MATRIZ", "PARCERIA OBRA X"}


def test_uma_perna_so_nao_e_transferencia(banco_aportes):
    """Um pagamento comum do mesmo valor no mesmo dia não tem cara de
    transferência — avisar aí seria ensinar a ignorar o aviso."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS,
                   movimentos=[(7011, 12500.0, "20/09/2026", "")])
    assert aportes_de_para.consultar_semelhantes(
        12500.0, "20/09/2026", [7011, 22069]) == []


def test_outro_dia_ou_outro_valor_nao_dispara(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS,
                   movimentos=[(7011, 12500.0, "19/09/2026", ""),
                               (22069, 12500.0, "19/09/2026", "")])
    assert aportes_de_para.consultar_semelhantes(
        12500.0, "20/09/2026", [7011, 22069]) == []
    assert aportes_de_para.consultar_semelhantes(
        999.0, "19/09/2026", [7011, 22069]) == []


def test_um_centavo_de_diferenca_ainda_dispara(banco_aportes):
    """Exigir igualdade perfeita faria a crítica calar justamente quando ela
    importa: o dono digita "12.500,00" e o movimento veio com arredondamento
    de outro caminho."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS,
                   movimentos=[(7011, 12500.01, "20/09/2026", ""),
                               (22069, 12499.99, "20/09/2026", "")])
    assert len(aportes_de_para.consultar_semelhantes(
        12500.0, "20/09/2026", [7011, 22069])) == 1


def test_movimentacao_em_conta_de_fora_nao_conta(banco_aportes):
    """A pergunta é "isto é transferência entre DUAS CONTAS SUAS?" — dinheiro
    andando em conta que não está nesta operação não responde nada."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS + [(555, "OUTRA", "1-1")],
                   movimentos=[(7011, 12500.0, "20/09/2026", ""),
                               (555, 12500.0, "20/09/2026", "")])
    assert aportes_de_para.consultar_semelhantes(
        12500.0, "20/09/2026", [7011, 22069]) == []


# ---------------------------------------------------------------------------
# O REGISTRO DO QUE FOI GRAVADO
# ---------------------------------------------------------------------------
def test_o_que_foi_gravado_fica_registrado_e_o_orfao_aparece(banco_aportes):
    """*"Tudo que for gravado fica registrado (o que, quando, por quem, e o
    número do título que o OMIE devolveu), para dar para desfazer e para
    auditar."*"""
    from app.apps.analisesps import aportes, aportes_de_para, aportes_omie

    semear_espelho(banco_aportes, categorias=AS_QUATRO, contas=AS_CONTAS)
    aportes_de_para.guardar_conta(aportes.MATRIZ, 7011, "Marcelo")
    aportes_de_para.guardar_conta(aportes.PARCERIA, 22069, "Marcelo")

    plano = aportes.planejar(
        operacao="aporte_bws",
        valor="12.500,00", data="2026-09-20", fornecedor=99, obra="OBRA-1",
        quem="Marcelo", contas=aportes_de_para.contas_configuradas(),
        categorias=aportes_de_para.categorias_resolvidas())

    class OmieQueFalhaNoSegundo:
        def __init__(self):
            self.n = 0

        def _call(self, url, call, param):
            if call.startswith("Incluir"):
                self.n += 1
                if self.n == 2:
                    raise RuntimeError("ERROR: categoria inválida")
                return {"codigo_lancamento_omie": 4242}
            if call.startswith("Excluir"):
                raise RuntimeError("ERROR: não foi possível excluir")
            return {}

    resultado = aportes_omie.gravar(plano, "Marcelo",
                                    cliente=OmieQueFalhaNoSegundo())
    assert resultado["ok"] is False

    orfaos = aportes_omie.orfaos()
    assert len(orfaos) == 1
    assert orfaos[0]["codigo_lancamento_omie"] == 4242
    assert orfaos[0]["criado_por"] == "Marcelo"
    assert orfaos[0]["grupo"] == plano["grupo"]

    # E o histórico traz os dois lados: o que entrou e o que falhou.
    historico = aportes_omie.historico(10)
    assert len(historico) == 2
    assert {h["situacao"] for h in historico} == {"orfao", "falhou"}


def test_gravar_duas_vezes_o_mesmo_lancamento_nao_duplica_o_registro(banco_aportes):
    """O código de integração é único por título: clique duplo, navegador que
    repete o envio, nada disso pode virar dois registros do mesmo aporte."""
    from app.apps.analisesps import aportes, aportes_de_para, aportes_omie

    semear_espelho(banco_aportes, categorias=AS_QUATRO, contas=AS_CONTAS)
    aportes_de_para.guardar_conta(aportes.MATRIZ, 7011, "Marcelo")
    aportes_de_para.guardar_conta(aportes.PARCERIA, 22069, "Marcelo")

    plano = aportes.planejar(
        operacao="aporte_parceiro", valor="100,00",
        data="2026-09-20", fornecedor=99, obra="OBRA-1", quem="Marcelo",
        contas=aportes_de_para.contas_configuradas(),
        categorias=aportes_de_para.categorias_resolvidas())

    class OmieOk:
        def _call(self, url, call, param):
            return {"codigo_lancamento_omie": 777}

    aportes_omie.gravar(plano, "Marcelo", cliente=OmieOk())
    aportes_omie.gravar(plano, "Marcelo", cliente=OmieOk())
    assert len(aportes_omie.historico(10)) == 1
