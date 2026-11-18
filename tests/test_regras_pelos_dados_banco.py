# -*- coding: utf-8 -*-
"""
AS CONDICIONAIS SAEM DOS DADOS — contra um Postgres de verdade.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026). Ele:

> *"O pipe não exporta condicionais, por isso minha pergunta sobre mais ajuda
> na construção e na ideia."*

A exportação do Pipefy trouxe campos e fases; **as condicionais não saem de lá.**
Perguntá-las uma por uma é o desgaste que ele descreveu ("minúcia a minúcia").

A ideia: **a regra deixa rastro no dado.** Se o PIS era obrigatório para quem
tem carteira, entre os CTPS ele está preenchido em quase 100%; se o campo nem
aparecia para prestador, entre os prestadores está vazio em 100%. A regra se
MEDE nos cadastros que a importação trouxe, e ele confere em vez de ditar.

Estes testes defendem as três coisas que fazem a medição ser confiável:

  1. ela ACHA a regra que está no dado;
  2. ela NÃO PROPÕE regra sem base (poucos casos é coincidência, não regra);
  3. ela mostra a EXCEÇÃO NOMEADA — porque é olhando os discordantes que ele
     descobre se são erro de cadastro ou se a regra é outra.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import regras_pelos_dados as svc

pytestmark = pytest.mark.banco


def _dv(base: str) -> str:
    """CPF válido a partir de 9 dígitos."""
    def d(b):
        s = sum(int(x) * (len(b) + 1 - i) for i, x in enumerate(b))
        r = 11 - s % 11
        return str(0 if r >= 10 else r)
    b = base + d(base)
    return b + d(b)


def _pis(base10: str) -> str:
    """PIS válido a partir de 10 dígitos. ⚠️ Cada pessoa precisa do SEU: a
    coluna tem índice único (um PIS é de uma pessoa só)."""
    pesos = (3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    r = sum(int(base10[i]) * pesos[i] for i in range(10)) % 11
    return base10 + str(0 if r < 2 else 11 - r)


@pytest.fixture
def cena(sessao_real):
    """30 CTPS, 30 prestadores e 5 jovens aprendizes.

    Os números são escolhidos para exercitar as três faixas:
      · PIS nos CTPS: 34 de 35 → regra CLARA (acima de 97%);
      · PIS nos prestadores: 0 de 30 → campo que NÃO SE APLICA;
      · "valor do mês" nos CTPS: metade → FAIXA DO MEIO, nada é proposto;
      · jovem aprendiz: 5 casos → ABAIXO DO MÍNIMO, nada é proposto.
    """
    s = sessao_real
    n = 100000000
    def pessoa(i, **kw):
        nonlocal n
        n += 1
        campos = {"nome": f"Pessoa {i}", "cpf": _dv(str(n)[:9]), **kw}
        cols = ", ".join(campos)
        vals = ", ".join(f":{k}" for k in campos)
        s.execute(text(f"INSERT INTO colaboradores ({cols}, situacao, etapa) "
                       f"VALUES ({vals}, 'ATIVO', 'CONCLUIDO')"), campos)

    for i in range(30):
        # ⚠️ dois SEM PIS de propósito: são as exceções que ele vai olhar
        pessoa(f"CTPS {i}", vinculo="CTPS", tipo_contrato="CLT_INDETERMINADO",
               pis=(_pis(f"12056{41000 + i}") if i >= 1 else None),
               salario="2000", ctps_numero=f"123{4000 + i}",
               # metade com "valor do mês": a faixa do meio, de propósito
               valor_mes=("3000" if i % 2 == 0 else None))
    for i in range(30):
        pessoa(f"Prestador {i}", vinculo="PRESTADOR", tipo_contrato="AUTONOMO_RPA",
               valor_diaria="150", pis=None, ctps_numero=None)
    for i in range(5):
        pessoa(f"Aprendiz {i}", vinculo="CTPS", tipo_contrato="JOVEM_APRENDIZ",
               pis=_pis(f"12056{42000 + i}"))
    s.flush()
    return True


def _achar(r, chave, valor, campo):
    return next((p for p in r["propostas"] if p["chave"] == chave
                 and p["valor"] == valor and p["campo"] == campo), None)


def test_acha_que_CTPS_pede_PIS(sessao_real, cena):
    """A regra que está no dado: 28 de 30 CTPS têm PIS."""
    r = svc.medir(sessao_real)
    p = _achar(r, "vinculo", "CTPS", "pis")
    assert p and p["proposta"] == "OBRIGATORIO"
    assert p["casos"] == 35 and p["preenchidos"] == 34


def test_acha_que_PRESTADOR_nao_usa_PIS(sessao_real, cena):
    """O outro lado: campo vazio em 100% dos prestadores é campo que não se
    aplica, e a tela nem deveria mostrá-lo."""
    r = svc.medir(sessao_real)
    p = _achar(r, "vinculo", "PRESTADOR", "pis")
    assert p and p["proposta"] == "NAO_SE_APLICA"
    assert p["preenchidos"] == 0


def test_acha_que_PRESTADOR_usa_DIARIA_e_CTPS_nao(sessao_real, cena):
    r = svc.medir(sessao_real)
    assert _achar(r, "vinculo", "PRESTADOR", "valor_diaria")["proposta"] == "OBRIGATORIO"
    assert _achar(r, "vinculo", "CTPS", "valor_diaria")["proposta"] == "NAO_SE_APLICA"


def test_MOSTRA_AS_EXCECOES_COM_NOME(sessao_real, cena):
    """⚠️ É a parte que faz isto servir. Sem nome, ele não tem como saber se os
    dois CTPS sem PIS são erro de cadastro ou se a regra tem exceção."""
    r = svc.medir(sessao_real)
    p = _achar(r, "vinculo", "CTPS", "pis")
    assert len(p["excecoes"]) == 1
    for e in p["excecoes"]:
        assert e["nome"] and e["cpf"] and e["id"]
    assert "incompleto" in p["o_que_sao_as_excecoes"]


def test_NAO_PROPOE_REGRA_COM_POUCOS_CASOS(sessao_real, cena):
    """⚠️ A trava mais importante. Com 5 jovens aprendizes, "100% com PIS" é
    coincidência, não regra — e regra proposta por engano gasta a confiança
    dele, que já disse estar cansado de corrigir."""
    r = svc.medir(sessao_real)
    assert _achar(r, "tipo_contrato", "JOVEM_APRENDIZ", "pis") is None
    assert r["sem_base"] > 0

    # com o mínimo baixado na mão, ela aparece — ou seja, o corte é o mínimo e
    # não um defeito de leitura
    r2 = svc.medir(sessao_real, minimo=3)
    assert _achar(r2, "tipo_contrato", "JOVEM_APRENDIZ", "pis") is not None


def test_A_FAIXA_DO_MEIO_NAO_PROPOE_NADA(sessao_real, cena):
    """⚠️ A trava que impede chute, e é a mais importante depois do mínimo.

    "Valor do mês" está preenchido em metade dos CTPS. Isso pode ser regra com
    exceção, campo mal preenchido por anos, ou duas populações diferentes
    dentro do mesmo vínculo (mensalista × diarista) — e **não há como saber
    pelo número**. Propor qualquer coisa aqui seria adivinhar, e adivinhação
    com cara de medição é pior que silêncio: ele não teria como desconfiar.

    Entre 3% e 97%, o módulo não diz nada. É isso que faz as propostas que ele
    VAI ler merecerem confiança.
    """
    r = svc.medir(sessao_real)
    assert _achar(r, "vinculo", "CTPS", "valor_mes") is None


def test_a_frase_e_legivel_por_quem_nao_programa(sessao_real, cena):
    """A proposta é lida por ele, não por mim: tem de ter o número dentro."""
    r = svc.medir(sessao_real)
    p = _achar(r, "vinculo", "CTPS", "pis")
    assert "Nº do PIS" in p["frase"] and "OBRIGATÓRIO" in p["frase"]
    assert "%" in p["frase"] and "de 35" in p["frase"]
    assert "CTPS" in p["frase"]


def test_separa_o_que_o_formulario_JA_FAZ_do_que_falta(sessao_real, cena):
    """⚠️ Sem esta separação a medição devolve dezenas de regras que já valem, e
    a parte útil (o que falta) se perde no meio — que é o oposto do pedido
    dele."""
    r = svc.comparar_com_o_formulario(sessao_real)
    assert "ja_no_formulario" in r and "a_decidir" in r
    assert len(r["ja_no_formulario"]) + len(r["a_decidir"]) == len(r["propostas"])
    # o PIS para CTPS já é obrigatório no formulário desde 17/11 — então ele
    # NÃO pode aparecer na lista de decidir
    nomes = [(p["valor"], p["campo"]) for p in r["a_decidir"]]
    assert ("CTPS", "pis") not in nomes


def test_cadastro_cancelado_nao_entra_na_conta(sessao_real, cena):
    """Ficha abandonada mediria o preenchimento de quem nunca virou gente na
    empresa, e puxaria toda proporção para baixo."""
    antes = svc.medir(sessao_real)["cadastros_medidos"]
    sessao_real.execute(text(
        "UPDATE colaboradores SET etapa = 'CANCELADO' WHERE nome = 'Pessoa CTPS 5'"))
    sessao_real.flush()
    assert svc.medir(sessao_real)["cadastros_medidos"] == antes - 1


# ---------------------------------------------------------------------------
# ⚑ O CONTRASTE — achado dirigindo a tela, e é o que impede o módulo de dizer
# bobagem com cara de medição.
#
# Na primeira medição real a tela propôs "não exibir Data de admissão para
# CLT", porque na amostra o campo estava vazio nos 40. É absurdo, e o número
# não tinha como saber: "vazio em 100%" tem dois significados OPOSTOS —
# o campo não se aplica (condicional), ou ninguém preenche esse campo em caso
# nenhum (dado faltando em todo mundo).
#
# O que separa os dois é olhar o RESTO dos cadastros.
# ---------------------------------------------------------------------------
def test_campo_vazio_em_TODO_MUNDO_nao_vira_condicional(sessao_real, cena):
    """"Contrato de experiência" está vazio em CTPS e em prestador. Esconder
    para um dos dois seria encobrir falta de dado, não aplicar regra."""
    r = svc.medir(sessao_real)

    assert _achar(r, "vinculo", "CTPS", "contrato_experiencia") is None
    assert _achar(r, "vinculo", "PRESTADOR", "contrato_experiencia") is None
    # e o achado aparece com o nome certo, UMA vez por campo
    campos = [v["campo"] for v in r["campos_que_ninguem_preenche"]]
    assert "contrato_experiencia" in campos
    assert len(campos) == len(set(campos)), "o mesmo campo contado duas vezes"
    frase = next(v["frase"] for v in r["campos_que_ninguem_preenche"]
                 if v["campo"] == "contrato_experiencia")
    assert "ninguém preenche" in frase and "encobriria" in frase


def test_campo_preenchido_em_TODO_MUNDO_nao_vira_condicional(sessao_real, cena):
    """"Salário" está preenchido nos CTPS; se estivesse em todos, dizer
    "obrigatório QUANDO CTPS" ensinaria uma regra errada — o certo é
    "obrigatório sempre"."""
    s = sessao_real
    # põe salário em quem não tinha (prestadores e aprendizes): agora o campo
    # vale para TODO MUNDO, e aí ele deixa de ser condicional
    s.execute(text("UPDATE colaboradores SET salario = '1800' "
                   "WHERE salario IS NULL"))
    s.flush()

    r = svc.medir(s)
    p = _achar(r, "vinculo", "CTPS", "salario")
    assert p and p["proposta"] == "OBRIGATORIO_SEMPRE"
    assert "PARA TODOS" in p["frase"] and "não condicional" in p["frase"]


def test_a_condicional_DE_VERDADE_continua_sendo_achada(sessao_real, cena):
    """O contraste não pode matar o achado certo: a diária é dos prestadores e
    o PIS é dos CTPS, e esses TÊM contraste."""
    r = svc.medir(sessao_real)

    diaria = _achar(r, "vinculo", "PRESTADOR", "valor_diaria")
    assert diaria and diaria["proposta"] == "OBRIGATORIO"
    assert diaria["preenchidos_fora"] == 0, "o contraste tem de estar medido"

    pis = _achar(r, "vinculo", "CTPS", "pis")
    assert pis and pis["proposta"] == "OBRIGATORIO"


def test_a_conta_de_discordantes_da_tela_bate_com_a_lista(sessao_real, cena):
    """⚠️ A TELA DIZIA 409 E LISTAVA 4 (18/11/2026).

    Ele mandou a medição real e lá estava: *"409 discordante(s) — estes TÊM o
    campo preenchido"* seguido de quatro nomes. A tela contava sempre os
    VAZIOS; numa proposta de "não exibir" o discordante é quem TEM o campo.

    Número errado numa tela que existe para ele CONFIAR em números é o pior
    lugar para um descuido. Este teste prende a conta na FONTE: para cada
    proposta, quantos discordantes existem é derivável dos números que a API
    manda, e a tela só pode usar esse.
    """
    r = svc.medir(sessao_real)
    for p in r["propostas"]:
        quantos = (p["preenchidos"] if p["proposta"] == "NAO_SE_APLICA"
                   else p["casos"] - p["preenchidos"])
        # a lista vem com teto, então ela é no máximo o número — nunca MAIOR,
        # que é como o erro aparecia
        assert len(p["excecoes"]) <= quantos, (
            f"{p['frase']}: {len(p['excecoes'])} nomes para {quantos} "
            f"discordante(s) — a conta e a lista discordam")
        if quantos <= svc.TETO_DE_EXCECOES:
            assert len(p["excecoes"]) == quantos, (
                f"{p['frase']}: {quantos} discordante(s) e "
                f"{len(p['excecoes'])} nome(s) na lista")


# ---------------------------------------------------------------------------
# ⚑ O QUE ELE JÁ DECIDIU NÃO VOLTA A SER PERGUNTADO (18/11/2026)
#
# Ele revisou a medição dos 660 cadastros e recusou seis propostas, com motivo.
# Sem registrar isso, a próxima medição ofereceria as mesmas seis — e perguntar
# de novo o que ele já respondeu é a irritação que o CLAUDE.md descreve.
# ---------------------------------------------------------------------------
def test_regra_recusada_por_ele_sai_da_lista_de_decidir(sessao_real, cena):
    s = sessao_real
    # reproduz o caso real dele: "valor do mês" vazio nos CTPS e preenchido
    # nos prestadores (é assim que o PJ recebe) — tem CONTRASTE, então a
    # medição propõe "não exibir para CTPS", que é o que ele recusou
    s.execute(text("UPDATE colaboradores SET valor_mes = NULL "
                   "WHERE vinculo = 'CTPS'"))
    s.execute(text("UPDATE colaboradores SET valor_mes = '9000' "
                   "WHERE vinculo = 'PRESTADOR'"))
    s.flush()

    r = svc.comparar_com_o_formulario(s)
    chaves = [(p["chave"], p["valor"], p["campo"]) for p in r["a_decidir"]]
    assert ("vinculo", "CTPS", "valor_mes") not in chaves

    recusadas = [(p["chave"], p["valor"], p["campo"]) for p in r["recusadas_por_ele"]]
    assert ("vinculo", "CTPS", "valor_mes") in recusadas


def test_a_recusa_carrega_o_MOTIVO_dele(sessao_real, cena):
    """⚠️ O motivo importa mais que o "não": nos dois casos o número estava
    certo e a conclusão errada, pela mesma razão — o campo é RARO, não
    inaplicável. Sem o motivo escrito, alguém reabre o assunto achando que foi
    capricho."""
    s = sessao_real
    s.execute(text("UPDATE colaboradores SET valor_mes = NULL "
                   "WHERE vinculo = 'CTPS'"))
    s.execute(text("UPDATE colaboradores SET valor_mes = '9000' "
                   "WHERE vinculo = 'PRESTADOR'"))
    s.flush()

    r = svc.comparar_com_o_formulario(s)
    p = next(x for x in r["recusadas_por_ele"]
             if (x["chave"], x["valor"], x["campo"]) == ("vinculo", "CTPS", "valor_mes"))
    assert "mensalista" in p["porque_recusada"]
    assert "raro" in p["porque_recusada"]


def test_toda_recusa_tem_motivo_escrito():
    """Lista de recusas sem motivo é lista que ninguém sabe se ainda vale."""
    for chave, motivo in svc.RECUSADAS.items():
        assert motivo and len(motivo) > 25, f"{chave}: recusa sem motivo"
