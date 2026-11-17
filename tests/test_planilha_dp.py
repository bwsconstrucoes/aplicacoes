# -*- coding: utf-8 -*-
"""
O DE-PARA DA PLANILHA "Dados Documentos" → CADASTRO DO ERP.

⚑ POR QUE ESTE ARQUIVO EXISTE, e é a lição mais cara de 16/11/2026.

Eu escrevi o de-para três vezes de cabeça: lia os NOMES das colunas no código
(verificáveis) e INVENTAVA os VALORES que elas guardam. Valor inventado que não
bate não dá erro — o campo fica vazio e cala. O dono gastou três rodadas, e
terminou com:

    "Meu Deus do céu, socorro! É só casar informação da planilha vs campo do
     ERP. Tu não tá conseguindo (…) o que tu precisa de mim?"

    "Por que você não consulta a planilha? Você tem acesso."

Ele estava certo nas duas. Os valores abaixo foram LIDOS da planilha de
verdade, com a contagem de quantas pessoas usam cada um (3.667 linhas,
16/11/2026). Este arquivo é o que impede a quarta vez: se alguém mexer no
de-para sem olhar a planilha, ele cai aqui.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.importadores import planilha_dp as pl
from app.apps.erp.core.cadastros import pessoas as svc


def _linha(**campos):
    cab = pl.cabecalho_de(list(campos))
    return pl.ler_linha(cab, list(campos.values()))


# ---------------------------------------------------------------------------
# OS VALORES DE VERDADE, com a contagem de quem usa cada um
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("escrito,esperado", [
    ("CTPS", "CTPS"),                          # 2.667 pessoas
    ("Prestador de Serviço", "PRESTADOR"),     # 1.000 pessoas
])
def test_vinculo_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Tipo de Cadastro Radio horizontal": escrito})["vinculo"] == esperado


@pytest.mark.parametrize("escrito,esperado", [
    ("CLT (tempo determinado)", "CLT_DETERMINADO"),      # 1.859
    ("CLT (tempo Indeterminado)", "CLT_INDETERMINADO"),  # 817 — o "I" é maiúsculo lá
    ("Autônomo (RPA)", "AUTONOMO_RPA"),                  # 922
    ("Autônomo Mensalista", "AUTONOMO_MENSALISTA"),      # 17
    ("PJ", "PJ"),                                        # 31
    ("Jovem aprendiz", "JOVEM_APRENDIZ"),                # 12
    ("Estágio", "ESTAGIO"),                              # 5
    ("Pró-labore", "PRO_LABORE"),                        # 4
])
def test_tipo_de_contrato_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Tipo de Contrato": escrito})["tipo_contrato"] == esperado


@pytest.mark.parametrize("escrito,esperado", [
    ("Mês", "MES"), ("Mensal", "MENSAL"),
    # ⚠️ COM CRASE na planilha. Foi um dos valores que eu havia inventado sem
    # o acento e sem o "à" — e o campo ficava vazio, calado.
    ("Segunda à Sexta", "SEG_SEX"), ("Segunda à Quinta", "SEG_QUI"),
])
def test_categoria_de_alimentacao_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Categoria Auxílio Alimentação": escrito})["categoria_alimentacao"] == esperado


@pytest.mark.parametrize("escrito,esperado", [
    ("Mensal", "MENSAL"), ("Diário", "DIARIO"), ("Motocicleta", "MOTOCICLETA"),
    ("Segunda e Sexta", "SEG_E_SEX"), ("Vale Transporte", "VALE_TRANSPORTE"),
    ("Diário e Vale Transporte", "DIARIO_E_VT"), ("Cartão", "CARTAO"),
])
def test_categoria_de_transporte_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Categoria Auxílio Transporte": escrito})["categoria_transporte"] == esperado


@pytest.mark.parametrize("escrito,esperado", [
    ("Solteiro (a)", "SOLTEIRO"), ("Casado (a)", "CASADO"),
    ("União Estável", "UNIAO_ESTAVEL"), ("Divorciado (a)", "DIVORCIADO"),
    ("Viúvo (a)", "VIUVO"),
])
def test_estado_civil_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Estado Civil": escrito})["estado_civil"] == esperado


@pytest.mark.parametrize("escrito,esperado", [
    ("Ensino Fundamental", "FUNDAMENTAL"), ("Ensino Médio", "MEDIO"),
    ("Educação Infantil", "INFANTIL"), ("Ensino Superior", "SUPERIOR"),
    ("Não Alfabetizado", "NAO_ALFABETIZADO"), ("Pós-graduação", "POS"),
])
def test_escolaridade_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Escolaridade": escrito})["escolaridade"] == esperado


@pytest.mark.parametrize("escrito,esperado", [
    ("Nenhum", "NENHUM"), ("Transporte Coletivo", "COLETIVO"),
    ("Moto", "MOTO"), ("Bicicleta", "BICICLETA"),
])
def test_meio_de_transporte_como_a_planilha_escreve(escrito, esperado):
    assert _linha(**{"Meio de Transporte para chegar à Obra?": escrito})["meio_transporte"] == esperado


def test_TODA_opcao_traduzida_existe_no_cadastro():
    """A trava que fecha o círculo: traduzir para um valor que o cadastro não
    aceita seria trocar um campo vazio por uma recusa na gravação."""
    for campo, (_, tabela) in pl.LISTAS.items():
        validos = svc.DA_LISTA.get(campo)
        if not validos:
            continue
        for escrito, virou in tabela.items():
            assert virou in validos, f"{campo}: {escrito} → {virou} não é opção"


# ---------------------------------------------------------------------------
# OS FORMATOS, como a planilha escreve
# ---------------------------------------------------------------------------
def test_numero_com_pontuacao_entra_so_com_digitos():
    d = _linha(**{"CPF (Cadastro de Pessoa Física)": "008.611.323-27",
                  "CEP": "61.760-970", "Telefone": "88 99491-5597",
                  "Nº PIS": "160.83716.18-8"})
    assert d["cpf"] == "00861132327" and d["cep"] == "61760970"
    assert d["telefone"] == "88994915597" and d["pis"] == "16083716188"


def test_data_vem_em_dia_mes_ano_e_entra_como_data():
    assert _linha(**{"Data de Nascimento": "07/08/1983"})["data_nascimento"] == "1983-08-07"


def test_dinheiro_brasileiro_e_o_zero_que_quer_dizer_nada():
    d = _linha(**{"Salário": "1.995,20", "Valor Auxílio Alimentação": "0,00"})
    assert d["salario"] == "1995.20"
    # ⚠️ A planilha usa 0,00 para "não recebe". Gravar zero criaria auxílio de
    # zero real onde não há auxílio nenhum.
    assert "aux_alimentacao" not in d


def test_sim_e_nao_viram_marcacao():
    d = _linha(**{"Solicitar Salário Família?": "Não", "Recebe via SP": "Sim"})
    assert d["solicitar_salario_familia"] is False
    assert d["recebe_via_sp"] is True


def test_o_CNPJ_de_admissao_vem_com_o_nome_da_empresa_colado():
    """Na planilha é "00.079.526/0001-09 - BWS"."""
    d = _linha(**{"CNPJ Admissão": "00.079.526/0001-09 - BWS"})
    assert d["_nomes"]["empresa"] == "00079526000109"


def test_o_centro_de_custo_vem_com_a_descricao_colada():
    """Na planilha é "164.01 - OBRA ESCVARZEA …/SETOR PRODU"."""
    d = _linha(**{"Centro de Custo Contabilidade":
                  "164.01 - OBRA ESCVARZEA CONST EEM COM 12 SALAS/SETOR PRODU"})
    assert d["_nomes"]["centro_custo_contabil"] == "164.01"


def test_pagamento_sai_do_valor_da_diaria():
    """A planilha não tem coluna dizendo "é mensalista". Fica escrito porque é
    dedução, não dado: quem tem diária é diarista."""
    com = _linha(**{"Tipo": "CTPS", "Valor Diária": "130,00"})
    sem = _linha(**{"Tipo": "CTPS", "Valor Diária": ""})
    assert com["mensalista"] is False and sem["mensalista"] is True


def test_valor_que_eu_NAO_conheco_e_RELATADO_e_nao_chutado():
    d = _linha(**{"Tipo de Contrato": "Alguma Coisa Nova Que Inventaram"})
    assert "tipo_contrato" not in d
    assert any("Alguma Coisa Nova" in x["valor"] for x in d["_nao_entendi"])


# ---------------------------------------------------------------------------
# IMPORTAR DIRETO DA PLANILHA — sem card (17/11/2026)
#
# Ele: *"mas por que rodar com o card dela se quero que puxe da planilha?"*
# ---------------------------------------------------------------------------
@pytest.fixture
def mundo(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (Cargo, Convencao, Obra,
                                                  PerfilUsuario as P, Usuario)
    dp = Usuario(nome="dp-planilha", email="dp@planilha.teste", ativo=True,
                 senha_hash=gerar_hash("senha-de-teste-123"),
                 perfil=P.DEPARTAMENTO_PESSOAL, telefone="5585900000011")
    obra = Obra(codigo="GOIANAFLEX", nome="Obra da planilha", status="ATIVA")
    sessao_real.add_all([dp, obra, Cargo(nome="ENGENHEIRO CIVIL", ativo=True),
                         Convencao(nome="Sem Convenção")])
    sessao_real.flush()
    pessoa = svc.salvar(sessao_real, {
        "cpf": "09676749486", "nome": "Amanda Marques da Silva",
        "vinculo": "CTPS"}, dp)
    sessao_real.flush()
    return {"s": sessao_real, "dp": dp, "obra": obra, "pessoa": pessoa}


def _aba(**campos):
    """Uma planilha de mentira com as COLUNAS E VALORES de verdade."""
    base = {"CPF (Cadastro de Pessoa Física)": "096.767.494-86",
            "Nome Completo": "AMANDA MARQUES DA SILVA",
            "Tipo de Cadastro Radio horizontal": "Prestador de Serviço",
            "Tipo de Contrato": "PJ",
            "Cargo [ ]": "ENGENHEIRO CIVIL",
            "Convenção": "Sem Convenção",
            "Código da Obra": "GOIANAFLEX",
            "Salário": "4.000,00", "Valor Diária": "100,00",
            "Município": "RECIFE", "UF (Endereço)": "PE"}
    base.update(campos)
    cab = pl.cabecalho_de(list(base))
    lido = pl.ler_linha(cab, list(base.values()))
    return lambda: {"pessoas": {pl._digitos(base["CPF (Cadastro de Pessoa Física)"]): lido},
                    "erro": "", "planilha": "(teste)", "aba": "Dados Documentos",
                    "linhas": 1, "colunas": len(base)}


@pytest.mark.banco
def test_a_planilha_enche_o_cadastro_SEM_card_nenhum(mundo):
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True, ler=_aba())
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].vinculo == "PRESTADOR"
    assert mundo["pessoa"].tipo_contrato == "PJ"
    assert str(mundo["pessoa"].salario) == "4000.00"
    assert mundo["pessoa"].obra_id == mundo["obra"].id
    assert mundo["pessoa"].cargo_id is not None
    assert mundo["pessoa"].convencao_id is not None
    assert r["atualizadas"] and not r["nao_entendi"]


@pytest.mark.banco
def test_conferir_sem_gravar_nao_toca_no_cadastro(mundo):
    antes = mundo["pessoa"].vinculo
    r = pl.importar(mundo["s"], mundo["dp"], gravar=False, ler=_aba())
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].vinculo == antes
    # …mas o relatório já diz o que mudaria.
    assert any("vinculo" in p["campos"] for p in r["atualizadas"])


@pytest.mark.banco
def test_a_importacao_NUNCA_cria_gente(mundo):
    """Criar cadastro em massa a partir de planilha é o tipo de engano que se
    descobre depois de 3.000 linhas."""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"CPF (Cadastro de Pessoa Física)": "111.444.777-35",
                                "Nome Completo": "NINGUEM CONHECIDO"}))
    assert [x["cpf"] for x in r["sem_cadastro"]] == ["11144477735"]
    assert not r["atualizadas"]


@pytest.mark.banco
def test_cargo_e_obra_que_nao_existem_aqui_sao_RELATADOS(mundo):
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"Cargo [ ]": "CARGO QUE NAO EXISTE",
                                "Código da Obra": "OBRANENHUMA"}))
    escritos = " | ".join(x["valor"] for x in r["nao_entendi"])
    assert "CARGO QUE NAO EXISTE" in escritos and "OBRANENHUMA" in escritos


@pytest.mark.banco
def test_so_os_CPFs_pedidos_entram(mundo):
    """É como se começa: duas ou três pessoas, não 3.600."""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=False,
                    cpfs=["000.000.000-00"], ler=_aba())
    assert not r["atualizadas"]
    assert any(x["motivo"] == "não está na planilha" for x in r["sem_cadastro"])


@pytest.mark.banco
def test_falha_ao_ler_a_planilha_VOLTA_ESCRITA(mundo):
    """Silêncio não é resultado: o erro do Google vai para a tela."""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=lambda: {"pessoas": {}, "erro": "credencial faltando",
                                 "planilha": "x", "aba": "Dados Documentos",
                                 "linhas": 0, "colunas": 0})
    assert r["planilha"]["erro"] == "credencial faltando"
    assert not r["atualizadas"]
