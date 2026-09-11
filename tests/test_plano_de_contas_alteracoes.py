"""As alterações do plano de contas pedidas pelo dono em 10/09/2026.

O documento que originou isto (`PLANO_CONTAS_alteracoes.md`) trata de oito
assuntos, e cada um deles vira uma exigência aqui. Não é zelo: o plano de
contas é a única parte do ERP em que um erro NÃO aparece na tela — aparece
meses depois, na contabilidade, e aí já foi lançado errado centenas de vezes.

O que se prova:

  1. Devolução e estorno saíram das RECEITAS e viraram redutoras de custo.
  2. A retenção conjunta CSRF/PCC deixou de existir como conta.
  3. Um tributo, uma conta: a CSLL não está em dois lugares.
  4. O parcelamento tributário deixou de ser fluxo.
  5. O grupo 8 (aquisição de bens) entra no resultado.
  6. Os nomes que o dono mandou trocar foram trocados.
  7. Toda conta com risco de confusão diz com o que não confundir, e o par
     ferramenta × equipamento durável tem CRITÉRIO DE VALOR escrito nos dois.
  8. Nada aponta para conta que não existe — nem as descrições, nem o de-para
     do Omie, nem a tabela de aposentadorias.

E o mais importante, que é o que o próprio documento manda: conta com
lançamento NÃO some. A instalação do plano padrão desativa só o que nunca foi
usado; o que tem movimento é relatado e continua funcionando.
"""
from __future__ import annotations

import re

import pytest

from app.apps.erp.core.cadastros.depara import MAPA_CODIGO_OMIE, MAPA_PADRAO
from app.apps.erp.core.cadastros.plano_padrao import (
    APELIDOS, APOSENTADORIAS, LIMITE_FERRAMENTA, PLANO,
)


# ---------------------------------------------------------------------------
# Leitura do plano — o plano é uma tabela, e estes ajudantes a consultam
# ---------------------------------------------------------------------------
def _contas() -> dict[str, dict]:
    saida = {}
    for grupo, grupo_nome, sub, sub_nome, categorias in PLANO:
        for linha in categorias:
            cod, desc, natureza, ded, tipos, uso = linha[:6]
            saida[cod] = {"codigo": cod, "descricao": desc, "natureza": natureza,
                          "dedutivel": ded, "tipos": tipos, "uso": uso or "",
                          "grupo": grupo, "subgrupo": sub,
                          "subgrupo_nome": sub_nome,
                          "redutora": bool(linha[6]) if len(linha) > 6 else False}
    return saida


CONTAS = _contas()


# ---------------------------------------------------------------------------
# 1. Devolução e estorno não são receita
# ---------------------------------------------------------------------------
def test_devolucao_estorno_e_reembolso_sairam_das_receitas():
    """No grupo 1 elas inflavam a receita e deixavam o custo intacto — a margem
    saía errada dos dois lados."""
    for antigo in ("1.2.01", "1.2.02", "1.2.03"):
        assert antigo not in CONTAS, f"{antigo} ainda está no plano, no grupo de receitas"


def test_as_tres_viraram_conta_redutora_no_grupo_de_custo():
    for cod in ("3.5.01", "3.5.02", "3.5.03"):
        conta = CONTAS[cod]
        assert conta["grupo"] == "3", f"{cod} devia estar no grupo de custos"
        assert conta["redutora"] is True, f"{cod} devia abater o custo, não somar"


def test_so_o_subgrupo_de_reducoes_e_redutor():
    """Marcar redutora a conta errada inverteria o sinal de um custo de verdade."""
    redutoras = {c["codigo"] for c in CONTAS.values() if c["redutora"]}
    assert redutoras == {"3.5.01", "3.5.02", "3.5.03"}


def test_multa_recebida_continua_sendo_receita():
    """Aqui não houve gasto nosso: é dinheiro que entra por descumprimento de
    terceiro. É a única das quatro que continua em 1.2."""
    conta = CONTAS["1.2.04"]
    assert conta["grupo"] == "1"
    assert conta["redutora"] is False
    assert "3.5.01" in conta["uso"], "precisa mandar a devolução para o lugar certo"


# ---------------------------------------------------------------------------
# 2 e 3. Retenção conjunta desfeita, e um tributo numa conta só
# ---------------------------------------------------------------------------
def test_a_retencao_conjunta_deixou_de_ser_uma_conta():
    """A 2.1.06 juntava PIS, COFINS e CSLL — alíquotas distintas, bases
    distintas — e por isso não dava para saber o custo real de nenhum deles."""
    assert "2.1.06" not in CONTAS
    assert APOSENTADORIAS["2.1.06"][0] is None, \
        "não pode ter destino único: a guia é rateada entre três contas"


def test_a_guia_conjunta_e_rateada_entre_as_tres_contas_de_tributo():
    """A guia é uma só (DARF 5952), então as três contas precisam dizer isso —
    senão quem lança não descobre que tem de ratear."""
    for cod in ("2.1.04", "2.1.05", "2.2.02"):
        assert "5952" in CONTAS[cod]["uso"], f"{cod} não explica a guia conjunta"


def test_csll_e_irpj_existem_uma_vez_so():
    """Retida é antecipação da devida: é o MESMO tributo em dois momentos. Com
    duas contas não se via o custo total da obra sem somar dois grupos."""
    por_nome = [c for c in CONTAS.values() if "CSLL" in c["descricao"]]
    assert [c["codigo"] for c in por_nome] == ["2.2.02"]
    irpj = [c for c in CONTAS.values() if c["descricao"].startswith("IRPJ")]
    assert [c["codigo"] for c in irpj] == ["2.2.01"]


# ---------------------------------------------------------------------------
# 4. Parcelamento tributário deixou de ser fluxo
# ---------------------------------------------------------------------------
def test_parcelamento_tributario_nao_e_mais_movimentacao_financeira():
    assert "9.4.03" not in CONTAS
    juros = CONTAS["2.3.02"]["uso"]
    assert "PRINCIPAL" in juros and "grupo 2" in juros, \
        "a conta de juros precisa dizer para onde vai o principal"


# ---------------------------------------------------------------------------
# 5. Grupo 8 entra no resultado
# ---------------------------------------------------------------------------
def test_aquisicao_de_bem_aparece_no_custo_da_obra():
    """Uma betoneira comprada para obra em parceria precisa aparecer no custo
    dela — senão não há como cobrar a parte do parceiro."""
    grupo8 = [c for c in CONTAS.values() if c["grupo"] == "8"]
    assert grupo8
    assert all(c["natureza"] == "RESULTADO" for c in grupo8), \
        "conta de fluxo fica fora da DRE e some do custo da obra"


def test_o_relatorio_soma_o_grupo_8_nas_despesas():
    """Se o grupo 8 virasse RESULTADO sem entrar na conta de despesas, ele
    apareceria na lista de grupos e sumiria do resultado do período."""
    from pathlib import Path
    fonte = Path("app/apps/erp/core/relatorios.py").read_text(encoding="utf-8")
    assert '"4", "5", "6", "7", "8"' in fonte


# ---------------------------------------------------------------------------
# 6. Nomenclatura
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("codigo, deve_ter, nao_pode_ter", [
    ("5.1.03", "sistemas", "software"),
    ("5.1.05", "Limpeza da sede", "copa"),
])
def test_os_nomes_que_o_dono_mandou_trocar(codigo, deve_ter, nao_pode_ter):
    descricao = CONTAS[codigo]["descricao"]
    assert deve_ter.lower() in descricao.lower()
    assert nao_pode_ter.lower() not in descricao.lower()


def test_casa_de_apoio_a_obra_nao_e_aluguel_da_sede():
    assert "3.3.04" in CONTAS["5.1.01"]["uso"]
    assert "CASA DE APOIO" in CONTAS["3.3.04"]["uso"].upper()


def test_anuidade_do_crea_e_administrativa_e_a_art_e_da_obra():
    assert "ANUIDADE" in CONTAS["5.2.04"]["uso"].upper()
    assert "3.4.06" in CONTAS["5.2.04"]["uso"]
    assert "ART/RRT" in CONTAS["3.4.06"]["uso"]
    assert "5.2.04" in CONTAS["3.4.06"]["uso"]


# ---------------------------------------------------------------------------
# 7. Descrições — o que impede o plano de apodrecer
# ---------------------------------------------------------------------------
def test_ferramenta_e_equipamento_duravel_tem_criterio_de_valor_escrito():
    """Sem um número escrito nas DUAS descrições a escolha vira loteria — foi
    exatamente o que o dono disse."""
    for cod in ("3.1.19", "8.1.04"):
        assert LIMITE_FERRAMENTA in CONTAS[cod]["uso"], \
            f"{cod} não diz o valor que separa ferramenta de equipamento"
    assert "8.1.04" in CONTAS["3.1.19"]["uso"]
    assert "3.1.19" in CONTAS["8.1.04"]["uso"]


@pytest.mark.parametrize("uma, outra", [
    ("2.1.02", "4.2.01"),          # INSS da obra × INSS patronal da folha
    ("2.1.03", "4.2.03"),          # IRRF do serviço × IRRF do empregado
    ("5.1.04", "8.2.02"),          # manutenção × benfeitoria que valoriza
    ("5.1.03", "8.3.01"),          # assinatura mensal × licença comprada
    ("3.2.01", "3.2.02"),          # empreiteiro com medição × serviço técnico
    ("3.1.18", "3.1.26"),          # parafusos e ferragens × serralheria
    ("3.1.04", "3.1.05"),          # vedação × pré-moldado
    ("3.1.21", "3.1.17"),          # argamassa de saco × aditivo e cola
])
def test_par_que_se_confunde_aponta_um_para_o_outro(uma, outra):
    """Descrição que não cita a conta parecida não resolve a dúvida de quem
    está lançando — e a dúvida no lançamento é o que suja o plano."""
    assert outra in CONTAS[uma]["uso"], f"{uma} não manda para {outra}"
    assert uma in CONTAS[outra]["uso"], f"{outra} não manda para {uma}"


def test_rpa_avisa_dos_tributos_que_ele_gera():
    uso = CONTAS["3.2.03"]["uso"]
    assert "RPA" in uso and "2.1.02" in uso and "2.1.03" in uso


def test_locacao_generica_diz_quando_usar_a_especifica():
    uso = CONTAS["3.3.01"]["uso"]
    for especifica in ("3.3.02", "3.3.03", "3.3.04", "3.3.06"):
        assert especifica in uso


def test_juros_de_emprestimo_manda_o_principal_para_o_fluxo():
    assert "9.4.02" in CONTAS["6.1.01"]["uso"]


def test_valores_de_terceiros_diz_que_a_mesma_conta_tem_as_duas_pontas():
    for cod in ("9.1.02", "9.1.03"):
        uso = CONTAS[cod]["uso"]
        assert "mesma conta" in uso.lower()
        assert "saldo" in uso.lower() and "pend" in uso.lower(), \
            "precisa dizer o que um saldo diferente de zero significa"


def test_todo_material_aplicado_tem_descricao():
    """O grupo 3.1 é onde mais se lança, e era o que estava mais vazio."""
    sem = [c["codigo"] for c in CONTAS.values()
           if c["subgrupo"] == "3.1" and not c["uso"].strip()]
    assert sem == [], f"contas de material sem descrição: {sem}"


@pytest.mark.parametrize("codigo", ["3.1.99", "5.3.99"])
def test_conta_de_ultimo_recurso_diz_que_e_ultimo_recurso(codigo):
    uso = CONTAS[codigo]["uso"].upper()
    assert "ÚLTIMO RECURSO" in uso
    assert "REVIS" in uso, "precisa dizer que tem de ser revisada periodicamente"


def test_aporte_diz_a_direcao_do_dinheiro():
    """A direção é o que confunde no grupo 9.3: recebido × concedido."""
    for cod in ("9.3.01", "9.3.02", "9.3.05"):
        assert "ENTRA dinheiro" in CONTAS[cod]["uso"]
    for cod in ("9.3.03", "9.3.04", "9.3.06"):
        assert "SAI dinheiro" in CONTAS[cod]["uso"]


def test_venda_de_ativo_registra_o_resultado_nao_o_dinheiro():
    for cod in ("1.4.01", "1.4.02", "1.4.03"):
        uso = CONTAS[cod]["uso"].lower()
        assert "resultado" in uso and "fluxo" in uso


# ---------------------------------------------------------------------------
# 8. Nada aponta para o vazio
# ---------------------------------------------------------------------------
def test_nenhuma_descricao_cita_conta_que_nao_existe():
    """Descrição que manda para uma conta extinta é pior que descrição
    nenhuma: manda a pessoa procurar o que não está lá."""
    quebradas = []
    for conta in CONTAS.values():
        for ref in re.findall(r"\b\d\.\d\.\d\d\b", conta["uso"]):
            if ref not in CONTAS:
                quebradas.append((conta["codigo"], ref))
    assert quebradas == [], f"referências quebradas: {quebradas}"


def test_o_de_para_do_omie_so_aponta_para_conta_que_existe():
    """O de-para traduz o plano velho do Omie, que chega nos cards do Pipefy.
    Uma conta que mudou de grupo derruba a tradução em silêncio."""
    ruins = {n: c for n, c in MAPA_PADRAO.items() if c not in CONTAS}
    ruins.update({n: c for n, c in MAPA_CODIGO_OMIE.items() if c not in CONTAS})
    assert ruins == {}, f"de-para apontando para conta extinta: {ruins}"


def test_apelido_e_destino_de_aposentadoria_apontam_para_conta_que_existe():
    assert {n: c for n, c in APELIDOS.items() if c not in CONTAS} == {}
    faltando = {cod: destino for cod, (destino, _) in APOSENTADORIAS.items()
                if destino and destino not in CONTAS}
    assert faltando == {}


def test_conta_aposentada_nao_pode_continuar_no_plano():
    """As duas listas se contradizendo, a instalação criaria e desativaria a
    mesma conta em sequência."""
    assert [c for c in APOSENTADORIAS if c in CONTAS] == []


def test_nenhum_codigo_repetido():
    todos = [linha[0] for _, _, _, _, cats in PLANO for linha in cats]
    assert len(todos) == len(set(todos))


# ---------------------------------------------------------------------------
# A regra que o documento faz questão — histórico não se apaga — é provada com
# BANCO DE VERDADE, em `test_plano_aposentadoria_banco.py`: ela depende de
# achar a conta pelo código e de contar lançamentos, e a sessão dublada ignora
# `WHERE`. Aqui fica só a coerência das tabelas, acima.
