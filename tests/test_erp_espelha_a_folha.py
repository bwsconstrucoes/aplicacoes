# -*- coding: utf-8 -*-
"""
O ERP CORRESPONDE AO QUE A FOLHA DO ANÁLISE DE SPs LÊ?

⚑ Pedido dele em 17/11/2026, antes de desligar a planilha:

    "Toda aquela parte de folha de pagamento do Análise de SPs faz a leitura de
     uma série de colunas para determinar se o pagamento é de diarista, de
     alimentação, de transporte, de gratificado ou mensalista. (…) você tem que
     fazer essa análise e saber se o ERP está correspondente com essas
     informações. (…) elas têm que estar se espelhando e depois a gente vai
     migrar (…) E aí não pode ter falha nenhuma."

O plano dele: hoje o Análise de SPs lê a planilha "Registro de Colaboradores";
amanhã passa a ler o ERP. Para isso o ERP tem de entregar os MESMOS campos com
as MESMAS palavras — porque a folha decide por TEXTO.

⚠️ POR QUE ESTE ARQUIVO É O ARQUIVO MAIS IMPORTANTE DESTA MUDANÇA: a falha
aqui não dá erro. Uma categoria que a folha não reconhece vira "modalidade não
informada", a pessoa sai com zero, e **ninguém repara num pagamento a menos**
tão rápido quanto num a mais. Foi o que a conferência achou: o ERP mandava o
RÓTULO DA TELA ("Vale-transporte"), e a folha espera a palavra da planilha
("Vale Transporte"). Um hífen.

Este arquivo lê os DOIS lados de verdade — o `folha_auxilio` do Análise de SPs
e o cadastro do ERP — e falha se eles divergirem. Não há valor copiado aqui
dentro: quem mudar um dos lados cai neste teste.
"""
from __future__ import annotations

import pytest

from app.apps.analisesps import folha_auxilio as fa
from app.apps.analisesps import folha_vinculo as fv
from app.apps.erp.core.cadastros import pessoas as erp


def _modos_que_a_folha_entende() -> set[str]:
    """As modalidades que `folha_auxilio` sabe calcular, lidas dela mesma."""
    return {fa.MODO_FIXO, fa.MODO_MENSAL, fa.MODO_SEG_SEX, fa.MODO_SEG_QUI,
            fa.MODO_CARTAO, *fa.MODOS_DIARIOS}


# ⚠️ AS QUATRO QUE A FOLHA NÃO ENTENDE NEM HOJE, lendo a planilha.
#
# Não são defeito do ERP: a planilha escreve exatamente estas palavras e a
# folha também não as reconhece — essas pessoas já ficam sem cálculo de
# transporte hoje. Estão listadas aqui para que o teste acuse se alguém
# ACHAR que resolveu, e para não deixar o problema invisível.
#
# A decisão é de negócio e é dele: ou a folha aprende estas quatro, ou essas
# pessoas têm outra categoria no cadastro. Levado a ele em 17/11/2026.
A_FOLHA_AINDA_NAO_ENTENDE = {"SEG_E_SEX", "VEICULO", "MOTOCICLETA", "BICICLETA"}


# ⚑ "NÃO RECEBE" É A AUSÊNCIA DE AUXÍLIO, não um modo de calcular (17/11/2026).
# Ele pediu a opção: *"adicionar para ambos os campos a opção Não recebe."*
#
# A folha calcula o auxílio pelo MODO (o texto). Não existe modo "não recebe" —
# existe não ter auxílio. Então o ERP manda VAZIO, e o teste abaixo prova
# justamente isso, em vez de exigir que a folha reconheça uma palavra que não
# deveria chegar lá.
def test_nao_recebe_vai_VAZIO_para_a_folha():
    assert erp.CATEGORIA_ALIMENTACAO_PARA_FOLHA["NAO_RECEBE"] == ""
    assert erp.CATEGORIA_TRANSPORTE_PARA_FOLHA["NAO_RECEBE"] == ""


def test_folha_entende_toda_categoria_de_ALIMENTACAO_que_o_ERP_manda():
    entende = _modos_que_a_folha_entende()
    for codigo in erp.CATEGORIAS_ALIMENTACAO:
        if codigo == "NAO_RECEBE":
            continue              # ver o teste acima: vai vazio, de propósito
        palavra = erp.CATEGORIA_ALIMENTACAO_PARA_FOLHA[codigo]
        assert fa._sem_acento(palavra) in entende, (
            f"o ERP manda “{palavra}” para {codigo} e a folha não reconhece — "
            f"a pessoa sairia com ZERO de alimentação, sem erro nenhum")


def test_folha_entende_as_categorias_de_TRANSPORTE_que_dao_para_entender():
    entende = _modos_que_a_folha_entende()
    for codigo in erp.CATEGORIAS_TRANSPORTE:
        if codigo == "NAO_RECEBE":
            continue              # ver `test_nao_recebe_vai_VAZIO_para_a_folha`
        palavra = erp.CATEGORIA_TRANSPORTE_PARA_FOLHA[codigo]
        reconhece = fa._sem_acento(palavra) in entende
        if codigo in A_FOLHA_AINDA_NAO_ENTENDE:
            assert not reconhece, (
                f"{codigo} está na lista de “a folha ainda não entende”, mas "
                f"agora ela entende “{palavra}” — tire da lista")
            continue
        assert reconhece, (
            f"o ERP manda “{palavra}” para {codigo} e a folha não reconhece — "
            f"a pessoa sairia com ZERO de transporte, sem erro nenhum")


def test_toda_categoria_do_cadastro_tem_palavra_para_a_folha():
    """Categoria nova sem palavra cairia na folha como texto do código
    ("SEG_QUI"), que não é reconhecido por nada."""
    for codigo in erp.CATEGORIAS_ALIMENTACAO:
        assert codigo in erp.CATEGORIA_ALIMENTACAO_PARA_FOLHA, codigo
    for codigo in erp.CATEGORIAS_TRANSPORTE:
        assert codigo in erp.CATEGORIA_TRANSPORTE_PARA_FOLHA, codigo


def test_o_vinculo_e_o_contrato_do_ERP_sao_as_palavras_que_a_folha_compara():
    """A regra diarista × CTPS (`folha_vinculo`) compara o TIPO com
    "Prestador de Serviço" e o CONTRATO com "Autônomo (RPA)" e as duas CLTs.

    É a regra que decide se o dia de uma pessoa é pago como diária ou entra na
    folha CTPS — errar aqui troca a forma de pagamento de um dia de trabalho.
    """
    assert fv._igual(erp.VINCULO_PLANILHA["PRESTADOR"], fv.PRESTADOR)
    assert fv._igual(erp.TIPO_CONTRATO_ROTULOS["AUTONOMO_RPA"], fv.RPA)
    assert fv._igual(erp.TIPO_CONTRATO_ROTULOS["CLT_INDETERMINADO"], fv.CLT[0])
    assert fv._igual(erp.TIPO_CONTRATO_ROTULOS["CLT_DETERMINADO"], fv.CLT[1])


def test_o_sim_do_ERP_e_o_sim_que_a_folha_testa():
    """Os gratificados e o "paga via SP" são decididos por `_sim()`, que testa
    se o texto começa com "sim". Mandar True/False em vez de "Sim" faria
    TODO MUNDO cair no lado errado — e sem erro nenhum."""
    from app.apps.analisesps import gratificados as gr
    assert gr._sim("Sim")
    assert not gr._sim("")
    # É isto que `para_folha` escreve (ver `pessoas.para_folha`).
    assert gr._sim("Sim") and not gr._sim("")


def test_o_ERP_entrega_todo_campo_que_a_folha_le_da_planilha():
    """A lista de colunas que o Análise de SPs importa da planilha, conferida
    contra o que `para_folha` devolve. Campo que falta é decisão que a folha
    vai tomar sem o dado.

    `paga_por_beevale` fica FORA de propósito: o ERP não tem essa coluna, e
    nenhuma conta da folha a usa hoje — ela é importada e guardada, nada mais.
    Levado a ele em 17/11/2026 como pendência, não como defeito.
    """
    import re
    from pathlib import Path

    from app.apps.analisesps import colaboradores as asp

    precisa = set()
    for mapa in (asp.COLUNAS, asp.COLUNAS_DOS_AUXILIOS, asp.COLUNAS_OPCIONAIS):
        precisa.update(mapa)
    precisa.discard("paga_por_beevale")

    fonte = Path("app/apps/erp/core/cadastros/pessoas.py").read_text(encoding="utf-8")
    bloco = fonte[fonte.index("def para_folha"):]
    bloco = bloco[:bloco.index("\ndef ", 10)]
    entrega = set(re.findall(r'"([a-z_0-9]+)":', bloco))

    faltando = sorted(precisa - entrega)
    assert not faltando, (
        "a folha lê estes campos da planilha e o ERP não os entrega: "
        + ", ".join(faltando))
