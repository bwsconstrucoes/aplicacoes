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


def test_quem_tem_so_diaria_e_DIARISTA():
    """3.166 das 3.667 pessoas da planilha."""
    d = _linha(**{"Tipo": "CTPS", "Valor Diária": "130,00"})
    assert d["mensalista"] is False and d["valor_diaria"] == "130.00"


def test_quem_tem_VALOR_DO_MES_e_MENSALISTA_e_a_diaria_antiga_NAO_entra():
    """⚑ O caso da Amanda, corrigido com ele em 17/11/2026: *"no caso da
    Amanda, vai se sobrepor a informação Valor do Mês (…) ela não tem diária
    mais, é dado antigo."*

    Minha regra anterior ("quem tem diária é diarista") errava exatamente
    aqui. São 53 pessoas na planilha com as duas coisas: a diária ficou de
    quando a pessoa era diarista e ninguém apagou. Trazê-la poria um valor
    morto na ficha de quem é pago por mês — e é a diária da ficha que a folha
    usa para pagar dia de fim de semana.
    """
    d = _linha(**{"Tipo": "Prestador de Serviço", "Valor do Mês": "5.209,50",
                  "Valor Diária": "100,00"})
    assert d["mensalista"] is True
    assert d["valor_mes"] == "5209.50"
    assert "valor_diaria" not in d
    # E não some calado: o relatório diz que ignorou e por quê.
    assert any("mensalista" in x["motivo"] for x in d["_nao_entendi"])


def test_quem_nao_tem_nem_um_nem_outro_e_MENSALISTA():
    """390 pessoas: não há diária para pagar."""
    assert _linha(**{"Tipo": "CTPS"})["mensalista"] is True


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
                         Convencao(nome="CE-Fortaleza")])
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
            "Convenção": "CE-Fortaleza",
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
def test_quem_esta_ATIVO_na_planilha_e_CADASTRADO(mundo):
    """Ele, em 17/11/2026, vendo uma pessoa listada como "não existe no ERP":
    *"Pq não cadastrou a partir da planilha?"*"""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"CPF (Cadastro de Pessoa Física)": "111.444.777-35",
                                "Nome Completo": "NINGUEM CONHECIDO"}))
    assert [x["cpf"] for x in r["criadas"]] == ["11144477735"]
    from sqlalchemy import select
    from app.apps.erp.db.models.cadastros import Colaborador
    nova = mundo["s"].scalars(
        select(Colaborador).where(Colaborador.cpf == "11144477735")).first()
    assert nova is not None and nova.nome == "NINGUEM CONHECIDO"
    # E o cadastro dela já nasce com o que a planilha sabe.
    assert nova.vinculo == "PRESTADOR" and nova.cargo_id is not None


@pytest.mark.banco
def test_quem_esta_DESLIGADO_na_planilha_NAO_e_cadastrado(mundo):
    """São ~2.960 das 3.667 linhas. Trazer essa base morta encheria lista de
    obra, folha e relatório — e a decisão dele, de 04/11/2026, é só os ativos."""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"CPF (Cadastro de Pessoa Física)": "111.444.777-35",
                                "Nome Completo": "JA SAIU DA EMPRESA",
                                "Data de Saída": "10/03/2025"}))
    assert not r["criadas"]
    assert any("desligado" in x["motivo"] for x in r["sem_cadastro"])


@pytest.mark.banco
def test_cargo_e_convencao_que_faltam_SAO_CRIADOS_e_a_obra_NAO(mundo):
    """A diferença não é capricho: cargo e convenção são, no ERP, um NOME — o
    resto se preenche na tela deles. Obra é cadastro pesado (contrato, CNO,
    cliente), e obra fantasma é pior que código não casado."""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"Cargo [ ]": "CARGO QUE NAO EXISTE",
                                "Convenção": "PE-Petrolina",
                                "Código da Obra": "OBRANENHUMA"}))
    nasceram = {(x["tipo"], x["nome"]) for x in r["cadastros_criados"]}
    assert ("Cargo", "CARGO QUE NAO EXISTE") in nasceram
    assert ("Convenção", "PE-Petrolina") in nasceram
    # A obra, não: o código que não casa continua sendo relatado.
    escritos = " | ".join(x["valor"] for x in r["nao_entendi"])
    assert "OBRANENHUMA" in escritos
    assert "CARGO QUE NAO EXISTE" not in escritos


@pytest.mark.banco
def test_SEM_CONVENCAO_na_planilha_quer_dizer_sem_convencao(mundo):
    """E não uma convenção CHAMADA "Sem Convenção" — criá-la seria inventar um
    cadastro para representar a ausência dele."""
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"Convenção": "Sem Convenção"}))
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].convencao_id is None
    assert not [x for x in r["cadastros_criados"] if x["tipo"] == "Convenção"]


@pytest.mark.banco
def test_a_EMPRESA_DE_REGISTRO_casa_pelo_CNPJ_e_nao_pelo_nome(mundo):
    """Ele mostrou o motivo em 17/11/2026: a planilha escreve
    "00.079.526/0001-09 - BWS" e o cadastro tem "BWS CONSTRUÇÕES -
    00.079.526/0001-09". Mesma empresa, textos diferentes — comparar nome
    nunca ia bater, e era por isso que o campo ficava vazio em todo mundo.
    """
    from sqlalchemy import text
    mundo["s"].execute(text(
        "INSERT INTO empresas (razao_social, cnpj)"
        " VALUES ('BWS CONSTRUÇÕES - 00.079.526/0001-09',"
        "         '00.079.526/0001-09')"))
    mundo["s"].flush()
    pl.importar(mundo["s"], mundo["dp"], gravar=True,
                ler=_aba(**{"CNPJ Admissão": "00.079.526/0001-09 - BWS"}))
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert mundo["pessoa"].empresa_id is not None


@pytest.mark.banco
def test_PIS_que_nao_fecha_NAO_derruba_a_pessoa_e_vira_pendencia(mundo):
    """Ele, vendo 38 recusadas: *"Recusou só pq o dado não bateu? Importa sem
    o PIS e deixa como pendência."*

    O estrago era grande: 50 campos ficavam de fora por causa de um número de
    onze dígitos.
    """
    r = pl.importar(mundo["s"], mundo["dp"], gravar=True,
                    ler=_aba(**{"Nº PIS": "111.11111.11-1"}))
    mundo["s"].flush()
    mundo["s"].refresh(mundo["pessoa"])
    assert not r["recusadas"], "a pessoa não devia ser recusada pelo PIS"
    assert r["atualizadas"], "o resto do cadastro tinha de entrar"
    assert mundo["pessoa"].vinculo == "PRESTADOR"
    assert not (mundo["pessoa"].pis or "")
    assert any("PIS" in x["campo"] for x in r["nao_entendi"])


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


# ---------------------------------------------------------------------------
# AS INCOERÊNCIAS DA PLANILHA — varridas nas 3.667 linhas e decididas por ele
# em 17/11/2026, depois do pedido: *"você devia analisar outras situações
# discrepantes e me questionar"*.
# ---------------------------------------------------------------------------
def test_data_de_saida_manda_a_pessoa_esta_DESLIGADA():
    """Ele: *"quem está em Solicitações DP pode estar com data de saída — é
    gente que já saiu e o cadastro não foi finalizado, aguardando
    documentação."* Então a data manda, qualquer que seja a fase. Sem isto,
    37 pessoas que já saíram entrariam ATIVAS — e continuariam batendo ponto
    e aparecendo na folha."""
    d = _linha(**{"Tipo": "CTPS", "Data de Saída": "07/10/2026",
                  "Fase Atual": "Solicitações DP"})
    assert d["situacao"] == "DESLIGADO" and d["demissao"] == "2026-10-07"
    # Esse caso é legítimo: não vira aviso.
    assert not d["_nao_entendi"]


def test_cadastro_CONCLUIDO_com_data_de_saida_IGNORA_A_DATA_e_mantem_ativo():
    """Ele decidiu em 17/11/2026: *"Cadastro Concluído com data de saída é
    apagar a data de saída e manter ativo."*

    São 6 pessoas. A data NÃO entra — ela desligaria alguém que está
    trabalhando, e desligado não bate ponto nem entra na folha. O caso vai
    para a lista, para ser arrumado na planilha.
    """
    d = _linha(**{"Tipo": "CTPS", "Data de Saída": "07/10/2026",
                  "Fase Atual": "Cadastro Concluído"})
    assert "demissao" not in d
    # Desde 17/11/2026 a fase também diz a situação, e "Cadastro Concluído" é
    # ATIVO — antes o campo simplesmente não vinha, e a pessoa ficava com o
    # que já estava no cadastro, qualquer que fosse.
    assert d["situacao"] == "ATIVO"
    assert d["etapa"] == "CONCLUIDO"
    assert any("está errada e precisa ser apagada" in x["motivo"]
               for x in d["_nao_entendi"])


def test_JOVEM_APRENDIZ_com_CTPS_esta_CERTO_e_nao_vira_aviso():
    """⚑ Eu não tinha previsto. Ele, olhando os 9 ativos que eu havia acusado
    de erro: *"CTPS e jovem aprendiz na verdade estão corretos, esse é o
    certo. Não tinha previsto esse tipo de contrato formal. Não é CLT mas é
    Jovem aprendiz."*

    O contrato de aprendizagem é formal, com carteira assinada. Dos 9 que eu
    acusei, 5 eram isto — acusação minha, não erro dele.
    """
    d = _linha(**{"Tipo": "CTPS", "Tipo de Contrato": "Jovem aprendiz"})
    assert d["vinculo"] == "CTPS" and d["tipo_contrato"] == "JOVEM_APRENDIZ"
    assert not d["_nao_entendi"]


def test_tipo_e_contrato_que_se_contradizem_sao_RELATADOS():
    """57 linhas na planilha. Ele conferiu: *"todos que são prestador de
    serviço, que não estão desligados, estão com o contrato correto"* — e a
    conta bate (48 das 57 são de desligados, dado velho). O "Tipo" manda,
    porque é a coluna que a folha usa; a contradição é relatada."""
    d = _linha(**{"Tipo": "CTPS", "Tipo de Contrato": "Autônomo (RPA)"})
    assert d["vinculo"] == "CTPS"          # o Tipo mandou
    assert d["tipo_contrato"] == "AUTONOMO_RPA"
    assert any("se contradizem" in x["motivo"] for x in d["_nao_entendi"])


def test_vinculo_e_contrato_coerentes_NAO_viram_aviso():
    for tipo, contrato in (("CTPS", "CLT (tempo determinado)"),
                           ("Prestador de Serviço", "Autônomo (RPA)"),
                           ("Prestador de Serviço", "PJ"),
                           ("CTPS", "Jovem aprendiz"),
                           ("Prestador de Serviço", "PJ")):
        d = _linha(**{"Tipo": tipo, "Tipo de Contrato": contrato})
        assert not d["_nao_entendi"], (tipo, contrato)


def test_o_PIS_do_prestador_ENTRA():
    """173 prestadores têm PIS de um emprego CLT anterior. Ele: levar — o PIS
    é da pessoa, não do contrato."""
    d = _linha(**{"Tipo": "Prestador de Serviço", "Nº PIS": "160.83716.18-8"})
    assert d["pis"] == "16083716188"


# ---------------------------------------------------------------------------
# AS CORRELAÇÕES QUE ELE DITOU EM 17/11/2026
#
# Ele mandou a lista de campo do ERP × cabeçalho da planilha, depois de ver
# campos vazios na ficha de quem tinha acabado de ser importado. Cada caso
# aqui trava uma dessas correlações — porque de-para quebrado some em silêncio:
# a importação diz "atualizada" e o campo fica vazio.
# ---------------------------------------------------------------------------
def test_razao_social_do_pj_vem_de_nome_credor():
    """Ele: *"Razão social (PJ) = Nome Credor."* O nome da coluna não diz o que
    ela guarda — por isso a correlação fica travada por teste."""
    lido = _linha(**{"Tipo de Contrato": "PJ",
                     "Nome Credor": "MARCOS SERVICOS LTDA"})
    assert lido["razao_social_pj"] == "MARCOS SERVICOS LTDA"


def test_cnpj_do_pj_vem_de_pix_cnpj_e_so_os_digitos():
    """Ele: *"CNPJ (PJ) = Pix CNPJ."*"""
    lido = _linha(**{"Tipo de Contrato": "PJ",
                     "Pix CNPJ": "12.345.678/0001-95"})
    assert lido["cnpj_pj"] == "12345678000195"


@pytest.mark.parametrize("escrito,esperado", [
    ("45 dias, prorrogável por mais 45 dias", "45+45 dias"),
    ("30 dias, prorrogável por mais 60 dias", "30+60 dias"),
    ("Sem Contrato de Experiência", "Sem contrato de experiência"),
])
def test_contrato_de_experiencia_vira_a_opcao_que_o_ERP_tem(escrito, esperado):
    """Ele, em 17/11/2026: *"alguns campos as opções de seleção sequer existem
    no ERP."*

    A planilha escreve a frase inteira; a caixinha do cadastro só tem a
    abreviação. Sem esta tradução o texto entrava cru, não casava com opção
    nenhuma, e a ficha mostrava o campo VAZIO com o dado gravado por baixo —
    que é pior que não importar, porque parece que não veio.
    """
    assert _linha(**{"Contrato de Experiência": escrito})["contrato_experiencia"] == esperado
    # E o traduzido TEM de existir na caixinha, senão a ficha segue vazia.
    assert esperado in svc.CONTRATOS_EXPERIENCIA


def test_a_convencao_sai_como_nome_para_quem_chama_casar():
    """As sete opções que ele ditou: PE-Pernambuco, CE-Fortaleza,
    BA-Sobradinho, CE-Interior, PE-Petrolina, MA-Imperatriz, Sem Convenção."""
    assert _linha(**{"Convenção": "CE-Fortaleza"})["_nomes"]["convencao"] == "CE-Fortaleza"


def test_o_PIS_que_nao_fecha_vira_PENDENCIA_e_a_pessoa_entra():
    """Ele, vendo 38 pessoas recusadas por isso: *"Recusou só pq o dado não
    bateu? Importa sem o PIS e deixa como pendência."*

    Aqui se confere o que `ler_linha` entrega — o PIS sai como está escrito, e
    quem decide descartá-lo é o `importar`, num lugar só.
    """
    lido = _linha(**{"Nº PIS": "111.11111.11-1", "Nome Completo": "JOAO DA SILVA"})
    assert lido["pis"] == "11111111111"
    assert not svc.pis_valido(lido["pis"])


# ---------------------------------------------------------------------------
# A FASE DA PLANILHA → A ETAPA DO ERP (17/11/2026)
#
# Ele, pela segunda vez: *"Veja as coisas que cansam muito. Eu já havia falado
# sobre isso: não foi feito correlação entre fase e etapa."*
#
# A correlação existia e estava certa — ligada só na importação pelo Pipefy. A
# da planilha, que virou o caminho principal, lia "Fase Atual" apenas para a
# regra da data de saída. O dado estava lá, a tradução estava escrita, e nada
# as ligava. Estes casos existem para que isso não possa acontecer de novo.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fase,etapa,situacao", [
    ("Colaboradores Desligados", "DESLIGADO",        "DESLIGADO"),
    ("Cadastro Concluído",       "CONCLUIDO",        "ATIVO"),
    ("Colaboradores Afastados",  "AFASTADO",         "AFASTADO"),
    ("Cadastros Adicionais",     "EM_CONFERENCIA",   "ATIVO"),
    ("Cadastro Inicial",         "CADASTRO_INICIAL", "ATIVO"),
    ("Cadastro Cancelado",       "CANCELADO",        "DESLIGADO"),
    # Esta é a que não tinha par, e ele apontou: *"Não existe = Solicitações
    # DP."* Vai para Inicial, que é o que a etapa quer dizer — "o DP ainda não
    # olhou".
    ("Solicitações DP",          "CADASTRO_INICIAL", "ATIVO"),
])
def test_a_fase_da_planilha_vira_a_etapa_do_ERP(fase, etapa, situacao):
    lido = _linha(**{"Fase Atual": fase})
    assert lido["etapa"] == etapa
    assert lido["situacao"] == situacao


def test_TODA_etapa_traduzida_existe_no_cadastro():
    """Etapa inventada é recusada pelo `salvar` e derruba a pessoa inteira."""
    for _pedaco, etapa, situacao in svc.FASE_PIPEFY_PARA_ETAPA:
        assert etapa in svc.ETAPAS, etapa
        assert situacao in ("ATIVO", "AFASTADO", "DESLIGADO"), situacao


def test_fase_que_eu_nao_conheco_e_RELATADA_e_a_etapa_fica_como_estava():
    """Chutar a etapa é pior que não preenchê-la: ela é a fila de trabalho do
    DP, e quem cai em "concluído" por engano simplesmente some dela."""
    lido = _linha(**{"Fase Atual": "Alguma Fase Nova Que Criaram"})
    assert "etapa" not in lido
    assert any(x["campo"] == "Etapa" for x in lido["_nao_entendi"])


def test_a_data_de_saida_manda_mais_que_a_fase():
    """Quem está em "Solicitações DP" COM data de saída é desligado pela data.
    Ele: *"seriam duas situações: pessoas que já saíram e que ainda não foram
    o cadastro finalizado."*"""
    lido = _linha(**{"Fase Atual": "Solicitações DP",
                     "Data de Saída": "10/03/2025"})
    assert lido["etapa"] == "CADASTRO_INICIAL"
    assert lido["situacao"] == "DESLIGADO"


def test_razao_social_e_cnpj_do_PJ_nao_vao_para_quem_nao_e_PJ():
    """As colunas são "Nome Credor" e "Pix CNPJ" — e "credor" é quem recebe,
    o que vale para qualquer pessoa. Gravar isso em "Razão social (PJ)" daria
    empresa a quem não tem empresa."""
    clt = _linha(**{"Tipo de Contrato": "CLT (Tempo Indeterminado)",
                    "Nome Credor": "JOAO DA SILVA",
                    "Pix CNPJ": "12.345.678/0001-95"})
    assert "razao_social_pj" not in clt and "cnpj_pj" not in clt

    pj = _linha(**{"Tipo de Contrato": "PJ",
                   "Nome Credor": "SILVA SERVICOS LTDA",
                   "Pix CNPJ": "12.345.678/0001-95"})
    assert pj["razao_social_pj"] == "SILVA SERVICOS LTDA"
    assert pj["cnpj_pj"] == "12345678000195"


# ---------------------------------------------------------------------------
# "Nº" E "N°" SÃO A MESMA COISA — o defeito mais invisível de todos
#
# Ele, em 17/11/2026, pela quarta vez na mesma conversa: *"Outros campos que
# estão ficando vazios: Nº da CTPS, Série da CTPS, UF da CTPS, Emissão da
# CTPS."*
#
# As quatro colunas estavam mapeadas e corretas. O que não batia era o
# CABEÇALHO: "º" (ordinal masculino) e "°" (sinal de grau) são
# indistinguíveis na tela, e o código os lia como letras diferentes. Cabeçalho
# redigitado por alguém trocava um pelo outro, e a coluna inteira desaparecia
# sem erro nenhum.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cabeca", ["Nº CTPS", "N° CTPS", "Nº da CTPS",
                                    "Número da CTPS", "CTPS Nº"])
def test_o_numero_da_CTPS_entra_com_qualquer_forma_do_cabecalho(cabeca):
    assert _linha(**{cabeca: "1234567"})["ctps_numero"] == "1234567"


def test_os_quatro_campos_da_CTPS_entram():
    lido = _linha(**{"Nº CTPS": "1234567", "Série (CTPS)": "0010",
                     "UF (CTPS)": "PE", "Data de Emissão (CTPS)": "15/03/2019"})
    assert lido["ctps_numero"] == "1234567"
    assert lido["ctps_serie"] == "0010"
    assert lido["ctps_uf"] == "PE"
    assert lido["ctps_emissao"] == "2019-03-15"


def test_o_ordinal_e_o_grau_viram_o_mesmo_texto():
    """A causa, isolada: sem isto, uma das duas formas some sem erro."""
    assert pl._cru("Nº PIS") == pl._cru("N° PIS")


def test_a_lista_de_colunas_que_eu_leio_cobre_as_que_ele_ditou():
    """Toda coluna que ele mandou correlacionar tem de estar na lista do que
    eu leio — senão ela apareceria para ele como "coluna que eu não uso", que
    é a pergunta que este trabalho inteiro existe para não repetir."""
    uso = pl.colunas_que_eu_uso()
    for coluna in ("Nome Credor", "Pix CNPJ", "CNPJ Admissão", "Cargo [ ]",
                   "Convenção", "Contrato de Experiência", "Fase Atual",
                   "Nº CTPS", "Série (CTPS)", "UF (CTPS)",
                   "Data de Emissão (CTPS)"):
        assert pl._cru(coluna) in uso, coluna
