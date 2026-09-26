# -*- coding: utf-8 -*-
"""
As regras de rateio da folha, com banco de verdade — 26/09/2026.

⚠️ O DUBLÊ DA SUÍTE NÃO ALCANÇA O QUE IMPORTA AQUI: a trava de "uma pessoa só
pode estar em uma regra valendo", o índice único de obra repetida e o cascade que
leva pessoas e obras junto quando a regra é apagada. Tudo isso é `WHERE` e
constraint — e um erro neles não estoura: ele ratearia o salário de alguém para a
obra errada, toda quinzena, em silêncio.
"""
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
