# -*- coding: utf-8 -*-
"""
As regras de rateio da folha, com banco de verdade — 26/09/2026.

⚠️ O DUBLÊ DA SUÍTE NÃO ALCANÇA O QUE IMPORTA AQUI: a trava de "uma pessoa só
pode estar em uma regra valendo", o índice único de obra repetida e o cascade que
leva pessoas e obras junto quando a regra é apagada. Tudo isso é `WHERE` e
constraint — e um erro neles não estoura: ele ratearia o salário de alguém para a
obra errada, toda quinzena, em silêncio.
"""
from decimal import Decimal

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_folha(banco, monkeypatch):
    """Sobe o schema pelas migrações de verdade — as mesmas que o botão aplica."""
    from sqlalchemy import text

    from app.apps.analisesps import db as db_analisesps

    url = str(banco.url.render_as_string(hide_password=False))
    monkeypatch.setenv("DATABASE_URL", url)
    db_analisesps._engine = None

    pasta = __import__("pathlib").Path(db_analisesps.__file__).parent / "migracoes"
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        for caminho in sorted(pasta.glob("*.sql")):
            conn.execute(text(caminho.read_text(encoding="utf-8")))
        conn.commit()
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS analisesps CASCADE"))
        conn.commit()
    db_analisesps._engine = None


GERLANIO = {"cpf": "997.133.493-34", "nome": "GERLANIO GOMES LIMA"}
LUELIA = {"cpf": "035.134.413-63", "nome": "LUELIA MADIDA GOMES TOMAS"}


def regra(**extra):
    dados = {"nome": "Supervisores de Pernambuco",
             "pessoas": [GERLANIO],
             "obras": [{"obra": "CREPEAREIAS", "percentual": "50"},
                       {"obra": "CREPEOLINDA", "resto": True},
                       {"obra": "CREPETRIUNFO", "resto": True}]}
    dados.update(extra)
    return dados


def test_a_migracao_027_roda_no_postgres(banco_folha):
    from app.apps.analisesps import folha_rateio as fr
    assert fr._pronto() is True
    assert fr.listar() == []


def test_gravar_e_ler_de_volta_com_pessoas_e_obras(banco_folha):
    from app.apps.analisesps import folha_rateio as fr

    regra_id = fr.gravar(regra(), "MARCELO")
    lidas = fr.listar()

    assert len(lidas) == 1
    r = lidas[0]
    assert r["id"] == regra_id
    assert r["nome"] == "Supervisores de Pernambuco"
    assert r["criado_por"] == "MARCELO"
    # O CPF é guardado em dígitos e mostrado formatado.
    assert [p["cpf"] for p in r["pessoas"]] == ["99713349334"]
    assert r["pessoas"][0]["cpf_bonito"] == "997.133.493-34"
    # A ordem das obras é a que foi digitada.
    assert [o["obra"] for o in r["obras"]] == [
        "CREPEAREIAS", "CREPEOLINDA", "CREPETRIUNFO"]
    assert r["obras"][1]["resto"] is True
    assert r["obras"][1]["percentual"] is None


def test_alterar_uma_regra_TROCA_as_obras_e_as_pessoas_sem_deixar_resto(banco_folha):
    """⚠️ Uma regra gravada pela metade — obras novas e pessoas antigas — ratearia
    dinheiro de gente para obra errada, e ninguém saberia de onde veio."""
    from app.apps.analisesps import folha_rateio as fr

    regra_id = fr.gravar(regra(), "MARCELO")
    fr.gravar(regra(id=regra_id, nome="Só a Luélia", pessoas=[LUELIA],
                    obras=[{"obra": "ESCVARZEA", "percentual": "100"}]), "ANA")

    r = fr.buscar(regra_id)
    assert r["nome"] == "Só a Luélia"
    assert [p["cpf"] for p in r["pessoas"]] == ["03513441363"]
    assert [o["obra"] for o in r["obras"]] == ["ESCVARZEA"]
    assert r["alterado_por"] == "ANA"
    assert len(fr.listar()) == 1, "a alteração criou uma segunda regra"


def test_a_MESMA_pessoa_em_duas_regras_ATIVAS_e_recusada(banco_folha):
    """⚠️ A TRAVA MAIS IMPORTANTE DESTE ARQUIVO. Duas regras valendo para a mesma
    pessoa não têm resposta certa — o rateio dela dependeria de qual regra o
    sistema achasse primeiro.

    O Postgres não consegue fazer essa trava (índice parcial não aceita
    subconsulta, e `ativa` mora na outra tabela — ver a migração 027), então ela é
    no código. Este teste é a única coisa que a sustenta."""
    from app.apps.analisesps import folha_rateio as fr

    fr.gravar(regra(), "MARCELO")
    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.gravar(regra(nome="Outra regra"), "MARCELO")

    assert "GERLANIO" in str(erro.value)
    assert "Supervisores de Pernambuco" in str(erro.value)
    assert len(fr.listar()) == 1


def test_a_pessoa_PODE_voltar_numa_regra_desativada(banco_folha):
    """Regra desativada é histórico: ela explica como a folha de março foi
    rateada. Proibir a pessoa de aparecer ali apagaria a explicação."""
    from app.apps.analisesps import folha_rateio as fr

    fr.gravar(regra(nome="A antiga", ativa=False), "MARCELO")
    fr.gravar(regra(nome="A que vale"), "MARCELO")

    assert len(fr.listar()) == 2
    valendo = fr.regra_da_pessoa("99713349334")
    assert valendo["nome"] == "A que vale"


def test_alterar_a_propria_regra_nao_acusa_ela_mesma(banco_folha):
    """O erro bobo que faria a tela ficar impossível de usar: editar a regra e
    receber "essa pessoa já está nesta regra"."""
    from app.apps.analisesps import folha_rateio as fr

    regra_id = fr.gravar(regra(), "MARCELO")
    fr.gravar(regra(id=regra_id, nome="Supervisores PE"), "MARCELO")
    assert fr.buscar(regra_id)["nome"] == "Supervisores PE"


def test_a_regra_da_pessoa_responde_None_para_quem_nao_tem(banco_folha):
    """Quem não tem regra continua sendo apropriado pelo ponto, como sempre."""
    from app.apps.analisesps import folha_rateio as fr

    fr.gravar(regra(), "MARCELO")
    assert fr.regra_da_pessoa("035.134.413-63") is None
    assert fr.regra_da_pessoa("99713349334")["nome"] == "Supervisores de Pernambuco"


def test_apagar_leva_pessoas_e_obras_junto(banco_folha):
    from app.apps.analisesps import folha_rateio as fr
    from app.apps.analisesps.db import consultar_um

    regra_id = fr.gravar(regra(), "MARCELO")
    assert fr.apagar(regra_id, "MARCELO") is True

    assert fr.listar() == []
    for tabela in ("folha_regra_pessoa", "folha_regra_obra"):
        assert consultar_um(
            f"SELECT count(*) FROM analisesps.{tabela}")[0] == 0, tabela
    # E apagar de novo responde que não existe mais, sem estourar.
    assert fr.apagar(regra_id, "MARCELO") is False


def test_nome_de_regra_repetido_e_recusado_pelo_banco(banco_folha):
    """Duas regras com o mesmo nome fazem quem for editar escolher no escuro."""
    from app.apps.analisesps import folha_rateio as fr

    fr.gravar(regra(), "MARCELO")
    with pytest.raises(Exception):
        fr.gravar(regra(nome=" supervisores de PERNAMBUCO ", pessoas=[LUELIA]),
                  "MARCELO")


def test_regra_sem_pessoa_sem_obra_ou_sem_nome_nao_grava(banco_folha):
    from app.apps.analisesps import folha_rateio as fr

    for dados, pedaco in (
            (regra(nome=""), "nome"),
            (regra(pessoas=[]), "ao menos uma pessoa"),
            (regra(obras=[]), "ao menos uma obra"),
            (regra(pessoas=[{"cpf": "997.133.493-35"}]), "não é válido")):
        with pytest.raises(fr.ErroDoRateio) as erro:
            fr.gravar(dados, "MARCELO")
        assert pedaco in str(erro.value), f"{pedaco} → {erro.value}"
    assert fr.listar() == [], "gravou uma regra que devia ter sido recusada"


# ---------------------------------------------------------------------------
# APLICAR A TABELA COLADA — 27/09/2026, com banco de verdade
#
# ⚠️ É a operação mais destrutiva desta tela: ela desativa TODO o rateio que
# está valendo e cria o novo. O dublê da suíte não alcança nada disso — a trava
# de "uma pessoa em uma regra ativa só" é `WHERE ativa`, e o desativar em massa
# é UPDATE. Errado aqui, ou gente fica sem rateio, ou o rateio do mês passado
# desaparece e a folha antiga perde a explicação.
# ---------------------------------------------------------------------------
TABELA = ("997.133.493-34 ; GERLANIO GOMES LIMA ; CREPEAREIAS, CREPEOLINDA, CREPEOLINDA\n"
          "035.134.413-63 ; LUELIA MADIDA GOMES TOMAS ; CREPEAREIAS, CREPEOLINDA, CREPEOLINDA\n"
          "111.444.777-35 ; TERCEIRA PESSOA ; CREPETRIUNFO\n")


def test_a_tabela_colada_grava_as_regras_agrupadas(banco_folha):
    from app.apps.analisesps import folha_rateio as fr

    resultado = fr.aplicar_tabela(TABELA, quem="MARCELO")

    assert resultado["regras"] == 2, "duas distribuições diferentes"
    assert resultado["pessoas"] == 3
    assert resultado["desativadas"] == 0

    regras = fr.listar()
    assert len(regras) == 2
    # A de duas pessoas tem os pesos da repetição.
    grande = [r for r in regras if len(r["pessoas"]) == 2][0]
    por_obra = {o["obra"]: o["percentual"] for o in grande["obras"]}
    assert por_obra["CREPEOLINDA"] == Decimal("66.6667")
    assert por_obra["CREPEAREIAS"] == Decimal("33.3333")


def test_colar_de_novo_DESATIVA_o_que_valia_e_nao_apaga(banco_folha):
    """⚠️ É o que preserva a explicação da folha passada. Apagar deixaria um
    total de obra sem resposta."""
    from app.apps.analisesps import folha_rateio as fr

    fr.aplicar_tabela(TABELA, quem="MARCELO")
    antes = {r["id"] for r in fr.listar()}

    resultado = fr.aplicar_tabela(
        "997.133.493-34 ; GERLANIO ; OUTRAOBRA\n", quem="MARCELO")

    assert resultado["desativadas"] == 2
    todas = fr.listar()
    # As velhas continuam existindo, desativadas.
    velhas = [r for r in todas if r["id"] in antes]
    assert len(velhas) == 2
    assert all(r["ativa"] is False for r in velhas)
    # E só a nova está valendo.
    ativas = [r for r in todas if r["ativa"]]
    assert len(ativas) == 1
    assert ativas[0]["obras"][0]["obra"] == "OUTRAOBRA"


def test_colar_SEM_substituir_convive_com_as_regras_de_antes(banco_folha):
    from app.apps.analisesps import folha_rateio as fr

    fr.aplicar_tabela("111.444.777-35 ; TERCEIRA ; CREPETRIUNFO\n", quem="EU")
    resultado = fr.aplicar_tabela(
        "997.133.493-34 ; GERLANIO ; OUTRAOBRA\n", quem="EU", substituir=False)

    assert resultado["desativadas"] == 0
    assert len([r for r in fr.listar() if r["ativa"]]) == 2


def test_a_pessoa_colada_passa_a_ter_o_rateio_NOVO(banco_folha):
    """O caso do dia a dia: o mês mudou, a pessoa vai para outras obras."""
    from app.apps.analisesps import folha_rateio as fr

    fr.aplicar_tabela("997.133.493-34 ; GERLANIO ; OBRAVELHA\n", quem="EU")
    fr.aplicar_tabela("997.133.493-34 ; GERLANIO ; OBRANOVA, OBRANOVA, OUTRA\n",
                      quem="EU")

    regra = fr.regra_da_pessoa("99713349334")
    nomes = {o["obra"] for o in regra["obras"]}
    assert nomes == {"OBRANOVA", "OUTRA"}
    por_obra = {o["obra"]: o["percentual"] for o in regra["obras"]}
    assert por_obra["OBRANOVA"] == Decimal("66.6667")


def test_tabela_com_erro_NAO_mexe_em_nada(banco_folha):
    """⚠️ Confere tudo antes de escrever qualquer coisa. Se a linha 3 estiver
    errada, o rateio que estava valendo continua valendo — uma colagem pela
    metade deixaria gente sem rateio nenhum, e o valor cairia na obra do ponto
    sem ninguém pedir."""
    from app.apps.analisesps import folha_rateio as fr

    fr.aplicar_tabela("997.133.493-34 ; GERLANIO ; OBRAVELHA\n", quem="EU")
    antes = fr.listar()

    with pytest.raises(fr.ErroDoRateio) as erro:
        fr.aplicar_tabela(
            "035.134.413-63 ; LUELIA ; A\n"
            "111.444.777-35 ; TERCEIRA ; B\n"
            "000.000.000-00 ; QUARTA ; C\n", quem="EU")
    assert "Linha 3" in str(erro.value)

    depois = fr.listar()
    assert len(depois) == len(antes) == 1
    assert depois[0]["ativa"] is True, "a regra que valia continua valendo"
    assert depois[0]["obras"][0]["obra"] == "OBRAVELHA"


def test_o_nome_da_regra_colada_sai_das_OBRAS(banco_folha):
    """É como ele vai reconhecer a regra na tela. Pedir um nome a cada colagem
    seria uma pergunta por grupo — em trinta linhas, trinta perguntas."""
    from app.apps.analisesps import folha_rateio as fr

    fr.aplicar_tabela("997.133.493-34 ; GERLANIO ; ALFA, BETA, BETA\n", quem="EU")
    nome = fr.listar()[0]["nome"]
    assert "BETA" in nome and "ALFA" in nome


def test_regra_colada_com_MUITAS_obras_ganha_nome_curto(banco_folha):
    """Nome de setenta e tantos caracteres não cabe em cartão nenhum."""
    from app.apps.analisesps import folha_rateio as fr

    obras = ", ".join(f"OBRACOMNOMELONGO{n}" for n in range(1, 9))
    fr.aplicar_tabela(f"997.133.493-34 ; GERLANIO ; {obras}\n", quem="EU")
    nome = fr.listar()[0]["nome"]
    assert len(nome) <= 70
    assert "obra(s)" in nome


def test_dez_pessoas_em_dez_obras_numa_colagem(banco_folha):
    """O caso que ele descreveu como absurdo de preencher: antes eram cem
    campos."""
    from app.apps.analisesps import folha_rateio as fr

    cpfs = ["99713349334", "03513441363", "11144477735", "52998224725",
            "11122233396"]
    obras = ", ".join(f"OBRA{n}" for n in range(1, 11))
    tabela = "\n".join(f"{c} ; PESSOA {i} ; {obras}"
                       for i, c in enumerate(cpfs))

    resultado = fr.aplicar_tabela(tabela, quem="EU")
    assert resultado["regras"] == 1
    assert resultado["pessoas"] == 5

    regra = fr.listar()[0]
    assert len(regra["obras"]) == 10
    soma = sum((o["percentual"] for o in regra["obras"]), Decimal("0"))
    assert soma == Decimal("100.0000")
