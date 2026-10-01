# -*- coding: utf-8 -*-
"""
A apropriação guardada — 27/09/2026, com banco de verdade.

⚠️ POR QUE COM BANCO: este módulo é quase só SQL, e o que ele promete são
justamente as coisas que o dublê da suíte não vê:

  - o índice único por (ano, mês, tipo, CPF) é o que impede dois ajustes da mesma
    pessoa no mesmo pagamento — dois deixariam a apropriação sem saber qual vale,
    e o dinheiro iria para a obra errada sem nada na tela;
  - gravar de novo tem de APAGAR as linhas filhas da divisão antiga. Um UPDATE as
    deixaria para trás, e a pessoa apareceria dividida entre obras que ele já
    havia tirado;
  - o `WHERE` da competência é o que separa a quinzena do fim do mês. O dublê
    ignora `WHERE` e devolveria tudo, então um ajuste vazando de um pagamento
    para o outro passaria batido aqui — e em produção pagaria em obra errada;
  - o `ON DELETE CASCADE` é o que faz reabrir um fechamento levar as linhas dele.
"""
import datetime as dt
import pathlib
from decimal import Decimal as D

import pytest

pytestmark = pytest.mark.banco


@pytest.fixture
def banco_apropriacao(banco_analisesps):
    """O schema `analisesps` limpo para este teste.

    ⚠️ ERA UM REFAZ-TUDO: `DROP SCHEMA` mais os 36 arquivos de migração, A CADA
    TESTE. Com 727 testes de banco na suíte, isso dava ~26 mil execuções de
    arquivo SQL por rodada para construir sempre a mesma coisa — e era a maior
    conta do tempo que o dono cobrou em 29/09/2026.

    Agora o schema nasce uma vez por sessão e as tabelas são esvaziadas entre os
    testes. O isolamento é o mesmo: tabelas vazias e contadores de `id` zerados.
    Ver `banco_analisesps` no `conftest.py`."""

GERLANIO = "99713349334"
LUELIA = "03513441363"


def test_a_migracao_034_roda_no_postgres(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag
    assert ag._pronto() is True
    assert ag.ajustes_do_pagamento(2026, 9, "quinzena") == {}
    assert ag.fechamento(2026, 9, "quinzena") is None
    assert ag.fechamentos() == []


# ---------------------------------------------------------------------------
# O AJUSTE — o que ele mexeu à mão
# ---------------------------------------------------------------------------
def test_jogar_tudo_numa_obra_so(banco_apropriacao):
    """O caso comum: *"o ponto dele está errado, joga tudo na CREPEOLINDA"*."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, nome="GERLANIO GOMES LIMA",
                     obra_unica="crepeolinda", quem="MARCELO")

    ajustes = ag.ajustes_do_pagamento(2026, 9, "quinzena")
    assert set(ajustes) == {GERLANIO}
    assert ajustes[GERLANIO]["obra_unica"] == "CREPEOLINDA", (
        "a obra é normalizada em maiúsculas: duas grafias virariam duas linhas "
        "no total por obra, que é o número com que ele decide o rateio")
    assert ajustes[GERLANIO]["por_obra"] == []
    assert ajustes[GERLANIO]["fora"] is False
    assert ajustes[GERLANIO]["alterado_por"] == "MARCELO"


def test_o_ajuste_encaixa_NO_FORMATO_da_conta_pura(banco_apropriacao):
    """⚠️ É O ENCAIXE ENTRE O BANCO E A CONTA. Se as chaves não casarem,
    `apropriar_pessoa` ignora o ajuste em silêncio — e o valor vai para a obra do
    ponto como se ele nunca tivesse mexido."""
    from app.apps.analisesps import (folha_apropriacao as fa,
                                     folha_apropriacao_guardada as ag)

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, nome="GERLANIO",
                     por_obra=[{"obra": "CREPEOLINDA", "dias": 8,
                                "valor": "1000.00"},
                               {"obra": "CREPEAREIAS", "dias": 3,
                                "valor": "500.00"}],
                     quem="MARCELO")
    ajuste = ag.ajustes_do_pagamento(2026, 9, "quinzena")[GERLANIO]

    feito = fa.apropriar_pessoa({"cpf": GERLANIO, "nome": "GERLANIO",
                                 "valor": "1500.00"}, [], None, ajuste)
    assert feito["origem"] == fa.DA_MAO
    assert {p["obra"]: p["valor"] for p in feito["por_obra"]} == {
        "CREPEAREIAS": D("500.00"), "CREPEOLINDA": D("1000.00")}
    assert feito["criticas"] == [], "o ajuste soma o valor da pessoa"


def test_regravar_APAGA_a_divisao_antiga(banco_apropriacao):
    """⚠️ Um UPDATE deixaria as linhas filhas antigas para trás, e a pessoa
    apareceria dividida entre obras que ele já havia tirado."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO,
                     por_obra=[{"obra": "OBRA A", "valor": "100.00"},
                               {"obra": "OBRA B", "valor": "200.00"}])
    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO,
                     por_obra=[{"obra": "OBRA C", "valor": "300.00"}])

    ajuste = ag.ajustes_do_pagamento(2026, 9, "quinzena")[GERLANIO]
    assert [p["obra"] for p in ajuste["por_obra"]] == ["OBRA C"]


def test_a_divisao_MANDA_e_a_obra_unica_sai_de_cena(banco_apropriacao):
    """Os dois preenchidos mostrariam dois números diferentes para a mesma
    pessoa: a conta usaria a divisão e a tela, a obra única."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag
    from app.apps.analisesps.db import conexao

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, obra_unica="OBRA A")
    # Forçando a situação que a função recusa, para provar que a LEITURA também
    # se protege — banco velho pode ter a linha torta.
    with conexao() as conn:
        linha = conn.execute(
            "SELECT id FROM analisesps.apropriacao_ajuste "
            " WHERE cpf = ?", (GERLANIO,)).fetchone()
        conn.execute(
            "INSERT INTO analisesps.apropriacao_ajuste_obra "
            "  (ajuste_id, obra, dias, valor) VALUES (?,?,?,?)",
            (linha[0], "OBRA B", 5, D("10.00")))
        conn.commit()

    ajuste = ag.ajustes_do_pagamento(2026, 9, "quinzena")[GERLANIO]
    assert ajuste["obra_unica"] == ""
    assert [p["obra"] for p in ajuste["por_obra"]] == ["OBRA B"]


def test_a_quinzena_e_o_fim_do_mes_nao_se_misturam(banco_apropriacao):
    """⚠️ É O `WHERE` QUE O DUBLÊ IGNORA. Um ajuste vazando de um pagamento para o
    outro pagaria na obra errada, e a tela não teria como mostrar isso."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, obra_unica="OBRA A")
    ag.gravar_ajuste(2026, 9, "fim_de_mes", GERLANIO, obra_unica="OBRA B")
    ag.gravar_ajuste(2026, 10, "quinzena", GERLANIO, obra_unica="OBRA C")

    assert (ag.ajustes_do_pagamento(2026, 9, "quinzena")[GERLANIO]["obra_unica"]
            == "OBRA A")
    assert (ag.ajustes_do_pagamento(2026, 9, "fim_de_mes")[GERLANIO]["obra_unica"]
            == "OBRA B")
    assert (ag.ajustes_do_pagamento(2026, 10, "quinzena")[GERLANIO]["obra_unica"]
            == "OBRA C")


def test_tirar_alguem_do_pagamento_NAO_exige_motivo(banco_apropriacao):
    """⚠️ CORREÇÃO DE 29/09/2026, contra uma regra que eu inventei. Eu exigia o
    motivo; ele respondeu: *"Quem não entra de onde do arquivo de pagamento? Não
    pago e ponto final. A gestão do pagamento é minha, eu decido."*

    O que ele faz hoje na planilha é marcar `Pagar QZ` / `Não Pagar` numa célula —
    um tique, sem justificativa. Exigir motivo virava formulário em 400 linhas.

    O campo segue existindo, opcional, e quando escrito tem de aparecer inteiro no
    relatório: é a única parte da regra que era minha e continua valendo."""
    from app.apps.analisesps import folha_apropriacao_guardada as guardada

    guardada.gravar_ajuste(2026, 9, "quinzena", "99713349334", nome="GERLANIO",
                           fora=True, quem="MARCELO")
    ajuste = guardada.ajustes_do_pagamento(2026, 9, "quinzena")["99713349334"]
    assert ajuste["fora"] is True
    assert ajuste["motivo"] == ""

    guardada.gravar_ajuste(2026, 9, "quinzena", "99713349334", nome="GERLANIO",
                           fora=True, quem="MARCELO",
                           motivo="pediu demissão e recebe na rescisão")
    ajuste = guardada.ajustes_do_pagamento(2026, 9, "quinzena")["99713349334"]
    assert "rescisão" in ajuste["motivo"]


def test_os_dois_caminhos_juntos_sao_RECUSADOS(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    with pytest.raises(ag.ErroDaApropriacao) as erro:
        ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, obra_unica="OBRA A",
                         por_obra=[{"obra": "OBRA B", "valor": "10.00"}])
    assert "ambíguo" in str(erro.value)


def test_ajuste_que_nao_diz_nada_e_recusado(banco_apropriacao):
    """Um ajuste vazio ficaria guardado sem efeito, e ele acharia que mexeu."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    with pytest.raises(ag.ErroDaApropriacao) as erro:
        ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO)
    assert "tirar do pagamento" in str(erro.value)


def test_obra_repetida_na_divisao_e_recusada(banco_apropriacao):
    """Somar as duas caladamente esconderia o erro de digitação dentro de um
    total que parece certo."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    with pytest.raises(ag.ErroDaApropriacao) as erro:
        ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, por_obra=[
            {"obra": "OBRA A", "valor": "10.00"},
            {"obra": "obra a", "valor": "20.00"}])
    assert "mais de uma vez" in str(erro.value)


def test_cpf_com_digito_errado_e_recusado(banco_apropriacao):
    """O CPF é a chave de tudo: um número trocado joga o salário de outra pessoa
    na obra escolhida."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    with pytest.raises(ag.ErroDaApropriacao) as erro:
        ag.gravar_ajuste(2026, 9, "quinzena", "11111111111",
                         obra_unica="OBRA A")
    assert "dígito verificador" in str(erro.value)


def test_competencia_invalida_nao_chega_ao_banco(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    for ano, mes, tipo in [(2026, 13, "quinzena"), (1999, 9, "quinzena"),
                           (2026, 9, "inventado")]:
        with pytest.raises(ag.ErroDaApropriacao):
            ag.gravar_ajuste(ano, mes, tipo, GERLANIO, obra_unica="OBRA A")


def test_limpar_o_ajuste_devolve_a_pessoa_ao_ponto(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, obra_unica="OBRA A",
                     por_obra=None)
    assert ag.limpar_ajuste(2026, 9, "quinzena", GERLANIO) is True
    assert ag.ajustes_do_pagamento(2026, 9, "quinzena") == {}
    # Limpar o que não existe não é erro: ele pode apertar duas vezes.
    assert ag.limpar_ajuste(2026, 9, "quinzena", GERLANIO) is False


def test_listar_ajustes_ordena_por_nome(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.gravar_ajuste(2026, 9, "quinzena", GERLANIO, nome="ZEZINHO",
                     obra_unica="OBRA A")
    ag.gravar_ajuste(2026, 9, "quinzena", LUELIA, nome="ANA",
                     obra_unica="OBRA B")
    assert [a["nome"] for a in ag.listar_ajustes(2026, 9, "quinzena")] == [
        "ANA", "ZEZINHO"]


# ---------------------------------------------------------------------------
# O FECHAMENTO — o que já foi pago
# ---------------------------------------------------------------------------
def _apropriado(fecha=True):
    """Um resultado de `folha_apropriacao.apropriar`, do tamanho mínimo."""
    return {
        "pessoas": [
            {"cpf": GERLANIO, "nome": "GERLANIO", "nome_cadastro": "GERLANIO G L",
             "valor": D("1500.00"), "fora": False,
             "por_obra": [{"obra": "CREPEOLINDA", "dias": 8,
                           "valor": D("1000.00"), "origem": "ponto"},
                          {"obra": "CREPEAREIAS", "dias": 4,
                           "valor": D("500.00"), "origem": "ponto"}]},
            {"cpf": LUELIA, "nome": "LUELIA", "nome_cadastro": "LUELIA S",
             "valor": D("900.00"), "fora": False,
             "por_obra": [{"obra": "CREPEOLINDA", "dias": 12,
                           "valor": D("900.00"), "origem": "regra"}]},
            # Quem saiu do pagamento NÃO entra no congelado: não foi pago.
            {"cpf": "12345678909", "nome": "FORA", "valor": D("100.00"),
             "fora": True, "por_obra": []},
        ],
        "total_da_folha": D("2500.00"),
        "total_apropriado": D("2400.00"),
        "fecha": fecha,
    }


def test_fechar_congela_o_que_foi_pago(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")

    guardado = ag.fechamento(2026, 9, "quinzena")
    assert guardado["total_apropriado"] == D("2400.00")
    assert guardado["pessoas"] == 2, "quem saiu do pagamento não conta"
    assert guardado["fecha"] is True
    assert guardado["fechado_por"] == "MARCELO"
    assert ag.esta_fechado(2026, 9, "quinzena") is True
    assert ag.esta_fechado(2026, 10, "quinzena") is False


def test_o_total_por_obra_sai_do_CONGELADO(banco_apropriacao):
    """⚠️ É O NÚMERO COM QUE ELE DECIDE O RATEIO DO MÊS SEGUINTE. Vem do que foi
    pago, não do que a conta diria hoje."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    totais = ag.totais_por_obra(2026, 9, "quinzena")

    assert [(t["obra"], t["total"], t["pessoas"]) for t in totais] == [
        ("CREPEOLINDA", D("1900.00"), 2), ("CREPEAREIAS", D("500.00"), 1)]
    assert sum(t["total"] for t in totais) == D("2400.00")


def test_a_ORIGEM_de_cada_valor_fica_guardada(banco_apropriacao):
    """Pedido do dono: *"saber até de onde é que foi que veio aquela informação,
    se foi do ponto, se foi colocada de forma manual"*."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    linhas = ag.por_pessoa(LUELIA)
    assert len(linhas) == 1
    assert linhas[0]["origem"] == "regra"
    assert linhas[0]["obra"] == "CREPEOLINDA"
    assert linhas[0]["valor"] == D("900.00")


def test_o_nome_do_CADASTRO_e_o_que_fica_guardado(banco_apropriacao):
    """É o nome que o banco confere contra o CPF no arquivo de pagamento."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    assert "GERLANIO G L" in {l["nome"] for l in _linhas_do_fechamento()}


def _linhas_do_fechamento():
    from app.apps.analisesps.db import consultar
    return [{"cpf": l[0], "nome": l[1], "obra": l[2], "valor": l[3]}
            for l in consultar(
                "SELECT cpf, nome, obra, valor "
                "  FROM analisesps.apropriacao_linha")]


def test_refechar_SUBSTITUI_em_vez_de_duplicar(banco_apropriacao):
    """Duas linhas da mesma competência dobrariam o total por obra."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")

    assert len(ag.fechamentos()) == 1
    assert sum(t["total"] for t in ag.totais_por_obra(2026, 9, "quinzena")) == (
        D("2400.00")), "o CASCADE levou as linhas do fechamento antigo"


def test_um_fechamento_que_NAO_batia_continua_dizendo_que_nao_batia(
        banco_apropriacao):
    """Apagar essa informação é apagar a explicação de uma diferença que alguém
    vai procurar depois."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(fecha=False), quem="MARCELO")
    assert ag.fechamento(2026, 9, "quinzena")["fecha"] is False


def test_as_verbas_se_fecham_separadas(banco_apropriacao):
    """A folha, a alimentação e o transporte são pagamentos diferentes no mesmo
    mês — e cada um tem o seu total por obra."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    ag.fechar(2026, 9, "quinzena", _apropriado(), verba="alimentacao",
              quem="MARCELO")

    assert len(ag.fechamentos()) == 2
    assert ag.esta_fechado(2026, 9, "quinzena", "alimentacao") is True
    assert ag.esta_fechado(2026, 9, "quinzena", "transporte") is False


def test_reabrir_leva_as_linhas_junto(banco_apropriacao):
    """⚠️ É DESTRUTIVO: apaga história de pagamento. O `ON DELETE CASCADE` é o que
    impede linhas órfãs contando no total por obra de ninguém."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    assert ag.reabrir(2026, 9, "quinzena", quem="MARCELO") is True
    assert ag.fechamento(2026, 9, "quinzena") is None
    assert _linhas_do_fechamento() == []
    assert ag.reabrir(2026, 9, "quinzena", quem="MARCELO") is False


def test_fechar_sem_apropriacao_nao_grava_nada(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    with pytest.raises(ag.ErroDaApropriacao):
        ag.fechar(2026, 9, "quinzena", {}, quem="MARCELO")
    assert ag.fechamentos() == []


def test_o_teto_de_linhas_protege_de_laco_torto(banco_apropriacao, monkeypatch):
    """20 mil linhas não é pagamento — é defeito. Melhor recusar do que escrever
    milhões de linhas em silêncio."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    monkeypatch.setattr(ag, "MAXIMO_DE_LINHAS", 2)
    with pytest.raises(ag.ErroDaApropriacao) as erro:
        ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    assert "teto" in str(erro.value)
    assert ag.fechamentos() == []


def test_fechamentos_vem_do_mais_novo_para_o_mais_antigo(banco_apropriacao):
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 8, "quinzena", _apropriado(), quem="MARCELO")
    ag.fechar(2026, 9, "fim_de_mes", _apropriado(), quem="MARCELO")
    assert [(f["ano"], f["mes"]) for f in ag.fechamentos()] == [
        (2026, 9), (2026, 8)]


def test_por_pessoa_com_cpf_torto_devolve_vazio(banco_apropriacao):
    """A tela passa o que o usuário digitou; isto não pode estourar."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag
    assert ag.por_pessoa("") == []
    assert ag.por_pessoa("123") == []


def test_o_fechado_em_e_uma_data(banco_apropriacao):
    """A tela mostra "fechado em"; sem data ela mostraria vazio e ninguém saberia
    quando o pagamento foi congelado."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag

    ag.fechar(2026, 9, "quinzena", _apropriado(), quem="MARCELO")
    quando = ag.fechamento(2026, 9, "quinzena")["fechado_em"]
    assert isinstance(quando, dt.datetime)
