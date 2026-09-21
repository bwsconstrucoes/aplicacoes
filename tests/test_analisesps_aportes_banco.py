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


# ⚠️ O PLANO FINANCEIRO DE VERDADE, como ele mandou em 21/09/2026. Repare nas
# duas primeiras: nomes quase iguais, códigos que não têm nada a ver.
AS_CINCO = [
    ("2.08.97", "Aportes BWS", "N"),               # sai da provedora
    ("1.02.94", "Aporte BWS", "N"),                # entra na parceria
    ("1.02.02", "Aporte Parceiros", "N"),
    ("2.08.02", "Devolução de Aportes", "N"),
    ("1.02.95", "Devolução de Aportes BWS", "N"),
]
AS_CONTAS = [(7011, "BWS PROVEDORA", "12345-6"),
             (22069, "PARCERIA OBRA X", "99999-9")]


# ---------------------------------------------------------------------------
# A DESCOBERTA DO CÓDIGO PELA DESCRIÇÃO
# ---------------------------------------------------------------------------
def test_acha_as_quatro_categorias_pela_descricao(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_CINCO + [
        ("1.01.01", "Receita de Obra", "N"),
        ("4.05.00", "Transferência entre Contas", "S"),
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["situacao"] == "achou"
    assert achados["aportes_bws_saida"]["codigo"] == "2.08.97"
    assert achados["aporte_bws_entrada"]["codigo"] == "1.02.94"
    assert achados["devolucao_aportes_bws"]["codigo"] == "1.02.95"
    assert achados["devolucao_aportes"]["codigo"] == "2.08.02"
    assert achados["aporte_parceiros"]["codigo"] == "1.02.02"


def test_acento_e_maiuscula_nao_atrapalham(banco_aportes):
    """O plano financeiro é digitado por gente: "DEVOLUCAO DE APORTES" e
    "Devolução de Aportes" são a mesma coisa."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[
        ("2.08.97", "APORTES BWS", "N"),
        ("2.08.02", "DEVOLUCAO DE APORTES", "N"),
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["codigo"] == "2.08.97"
    assert achados["devolucao_aportes"]["codigo"] == "2.08.02"


def test_descricao_repetida_para_e_pede_a_decisao(banco_aportes):
    """*"Se aparecer mais de uma categoria com nome parecido, a tela tem de
    parar e me dizer, em vez de escolher uma."* Escolher a primeira seria
    exatamente o lançamento errado silencioso que o de-para existe para
    evitar."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[
        ("2.08.97", "Aportes BWS", "N"),
        ("9.09.09", "Aportes BWS", "N"),      # a mesma descrição, outro código
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["situacao"] == "ambigua"
    assert achados["aportes_bws_saida"]["codigo"] == "", "escolheu uma sozinha"
    assert len(achados["aportes_bws_saida"]["candidatos"]) == 2


def test_descricao_que_nao_existe_e_dita_em_vez_de_inventada(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[("1.01.01", "Receita", "N")])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["situacao"] == "nao_achou"
    assert achados["aportes_bws_saida"]["codigo"] == ""


def test_as_quatro_de_nome_parecido_nao_se_confundem(banco_aportes):
    """⚠️ O CAMPO MINADO DESTE PLANO FINANCEIRO, e o motivo de este teste
    existir:

      · "Aportes BWS" é pedaço de "Devolução de Aportes BWS";
      · "Aporte BWS" é o singular de "Aportes BWS" — e são LADOS OPOSTOS do
        mesmo dinheiro, com códigos diferentes;
      · "Devolução de Aportes" é pedaço de "Devolução de Aportes BWS".

    Casar por pedaço ou dobrar o plural faria o sistema escolher a errada em
    silêncio. Casamento EXATO resolve as quatro."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_CINCO)
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["codigo"] == "2.08.97"
    assert achados["aporte_bws_entrada"]["codigo"] == "1.02.94"
    assert achados["devolucao_aportes_bws"]["codigo"] == "1.02.95"
    assert achados["devolucao_aportes"]["codigo"] == "2.08.02"
    # E todas resolvidas sozinhas: nenhuma ficou ambígua por causa do nome.
    assert aportes_de_para.falta_configurar() == []


def test_o_singular_so_vira_candidato_nunca_certeza(banco_aportes):
    """Faltando "Aporte BWS" no plano, o sistema NÃO pode usar "Aportes BWS"
    no lugar: são lados opostos. Ele oferece como candidato e para."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[
        ("2.08.97", "Aportes BWS", "N"),
        ("1.02.02", "Aporte Parceiros", "N"),
        ("2.08.02", "Devolução de Aportes", "N"),
        ("1.02.95", "Devolução de Aportes BWS", "N"),
    ])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["codigo"] == "2.08.97"
    entrada = achados["aporte_bws_entrada"]
    assert entrada["codigo"] == "", "usou a categoria do outro lado"
    assert any(c["codigo"] == "2.08.97" for c in entrada["candidatos"]), \
        "nem sequer ofereceu a parecida para ele olhar"
    assert any("Aporte BWS" in f for f in aportes_de_para.falta_configurar())


def test_a_marca_de_transferencia_vem_junto(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=[("2.08.97", "Aportes BWS", "S")])
    achados = aportes_de_para.descobrir_categorias()
    assert achados["aportes_bws_saida"]["transferencia"] == "S"


# ---------------------------------------------------------------------------
# O DE-PARA GRAVADO — e o SQL de gravação
# ---------------------------------------------------------------------------
def test_guardar_e_reler_a_categoria(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_CINCO, contas=AS_CONTAS)
    aportes_de_para.guardar_categoria("aportes_bws_saida", "2.08.97", "Marcelo")
    assert aportes_de_para.categorias_configuradas()[
        "aportes_bws_saida"]["codigo"] == "2.08.97"


def test_a_conta_e_lembrada_por_operacao_e_papel_nao_cadastrada(banco_aportes):
    """⚠️ *"Não quero travar a conta Matriz e a da Parceria, tem mais de uma
    situação."*

    O que ficou não é cadastro: é memória, por (operação, papel), só para vir
    pré-escolhida. Duas parcerias diferentes em duas operações diferentes
    convivem — e nenhuma delas decide categoria nenhuma."""
    from app.apps.analisesps import aportes, aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS)
    aportes_de_para.lembrar_conta("aporte_bws", aportes.PARCERIA, 22069, "M")
    aportes_de_para.lembrar_conta("aporte_parceiro", aportes.PARCERIA, 7011, "M")
    lembradas = aportes_de_para.contas_lembradas()
    assert lembradas["aporte_bws:parceria"] == 22069
    assert lembradas["aporte_parceiro:parceria"] == 7011

    # Lembrar de novo troca, não duplica (exercita o ON CONFLICT).
    aportes_de_para.lembrar_conta("aporte_bws", aportes.PARCERIA, 7011, "M")
    assert aportes_de_para.contas_lembradas()["aporte_bws:parceria"] == 7011


def test_lembrar_a_conta_nunca_derruba_nada(banco_aportes):
    """Um aporte que já entrou no OMIE não pode falhar porque a memória de
    conforto não gravou."""
    from app.apps.analisesps import aportes_de_para

    aportes_de_para.lembrar_conta("aporte_bws", "papel_que_nao_existe", 1, "M")
    aportes_de_para.lembrar_conta("aporte_bws", "provedora", None, "M")


def test_a_categoria_sem_duvida_se_resolve_sozinha(banco_aportes):
    """⚠️ MUDANÇA DE 20/09/2026: *"eu não entendi esse gravar o de-para. Eu
    acho que não precisaria."*

    Ele tem razão. Descrição que aparece UMA vez só no plano financeiro não
    tem decisão a tomar — pedir um clique de confirmação é cerimônia, e
    cerimônia que se repete vira clique automático, que é pior do que não ter
    conferência nenhuma. O de-para continua impedindo código chumbado; só
    deixou de ser um passo."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_CINCO, contas=AS_CONTAS)
    # NADA foi confirmado à mão, de propósito.
    assert aportes_de_para.categorias_configuradas() == {}

    resolvidas = aportes_de_para.categorias_resolvidas()
    assert resolvidas["aportes_bws_saida"]["codigo"] == "2.08.97"
    assert resolvidas["aporte_bws_entrada"]["codigo"] == "1.02.94"
    assert resolvidas["devolucao_aportes_bws"]["codigo"] == "1.02.95"
    assert all(v.get("descoberta") for v in resolvidas.values())


def test_a_categoria_com_duvida_continua_parando_a_tela(banco_aportes):
    """O que se resolve sozinho é o caso SEM dúvida. Descrição repetida
    continua sendo decisão dele — escolher a primeira seria o lançamento
    errado silencioso que o de-para existe para evitar."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS,
                   categorias=AS_CINCO + [("9.09.09", "Aportes BWS", "N")])
    resolvidas = aportes_de_para.categorias_resolvidas()
    assert "aportes_bws_saida" not in resolvidas, "escolheu uma sozinha"
    assert resolvidas["aporte_parceiros"]["codigo"] == "1.02.02"
    faltas = aportes_de_para.falta_configurar()
    assert any("Aportes BWS" in f for f in faltas)
    assert not any("Aporte Parceiros" in f for f in faltas)


def test_o_que_ele_confirmou_a_mao_vale_por_cima_do_descoberto(banco_aportes):
    """É assim que ele conserta um caso que o sistema leria errado."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, contas=AS_CONTAS, categorias=AS_CINCO + [
        ("9.99.99", "Aportes BWS (antiga)", "N")])
    aportes_de_para.guardar_categoria("aportes_bws_saida", "9.99.99", "Marcelo")
    resolvidas = aportes_de_para.categorias_resolvidas()
    assert resolvidas["aportes_bws_saida"]["codigo"] == "9.99.99"


def test_com_o_plano_financeiro_em_ordem_nao_falta_nada(banco_aportes):
    """Nada a configurar: as categorias se resolvem sozinhas e a conta é
    escolhida na hora do lançamento."""
    from app.apps.analisesps import aportes_de_para

    semear_espelho(banco_aportes, categorias=AS_CINCO, contas=AS_CONTAS)
    assert aportes_de_para.falta_configurar() == []


def test_o_que_falta_e_dito_em_portugues(banco_aportes):
    from app.apps.analisesps import aportes_de_para

    # Plano financeiro VAZIO: as quatro categorias faltam, porque não há o que
    # descobrir. Conta NÃO entra nesta conta — ela é escolhida a cada
    # lançamento, não cadastrada.
    semear_espelho(banco_aportes, contas=AS_CONTAS)
    faltas = aportes_de_para.falta_configurar()
    assert len(faltas) == 5, faltas
    assert not any("conta" in f.lower() for f in faltas)
    aportes_de_para.guardar_categoria("aportes_bws_saida", "", "Marcelo")
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

    semear_espelho(banco_aportes, categorias=AS_CINCO, contas=AS_CONTAS,
                   rateios=[("OBRA-1", "Obra Um")])
    # Nenhuma categoria confirmada à mão: elas se resolvem sozinhas, que é o
    # caminho que o dono vai usar de verdade.
    descricoes = aportes_de_para.descricoes_das_contas()
    categorias = aportes_de_para.categorias_resolvidas()

    # ⚠️ REPARE NO aporte_bws: dois códigos DIFERENTES, um de cada lado.
    esperado = {
        "aporte_bws": [(7011, "2.08.97", "P"), (22069, "1.02.94", "R")],
        "devolucao_bws": [(22069, "2.08.02", "P"), (7011, "1.02.95", "R")],
        "aporte_parceiro": [(22069, "1.02.02", "R")],
        "devolucao_parceiro": [(22069, "2.08.02", "P")],
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
            obra="OBRA-1", quem="Marcelo", descricoes=descricoes,
            categorias=categorias)
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
        {"BWS PROVEDORA", "PARCERIA OBRA X"}


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

    semear_espelho(banco_aportes, categorias=AS_CINCO, contas=AS_CONTAS)
    plano = aportes.planejar(
        operacao="aporte_bws", conta_origem=7011, conta_destino=22069,
        valor="12.500,00", data="2026-09-20", fornecedor=99, obra="OBRA-1",
        quem="Marcelo", descricoes=aportes_de_para.descricoes_das_contas(),
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

    semear_espelho(banco_aportes, categorias=AS_CINCO, contas=AS_CONTAS)
    plano = aportes.planejar(
        operacao="aporte_parceiro", conta_destino=22069, valor="100,00",
        data="2026-09-20", fornecedor=99, obra="OBRA-1", quem="Marcelo",
        descricoes=aportes_de_para.descricoes_das_contas(),
        categorias=aportes_de_para.categorias_resolvidas())

    class OmieOk:
        def _call(self, url, call, param):
            return {"codigo_lancamento_omie": 777}

    aportes_omie.gravar(plano, "Marcelo", cliente=OmieOk())
    aportes_omie.gravar(plano, "Marcelo", cliente=OmieOk())
    assert len(aportes_omie.historico(10)) == 1
