# -*- coding: utf-8 -*-
"""
A base consolidada das notas — a fonte da futura tela de Faturamento.

Por que ela existe (pedido do dono em 09/10/2026): a informação de UMA nota está
espalhada por cinco planilhas, e a "Notas BWS" tem 65+ colunas, a maioria de
cruzamento. Ele quer uma base só, e fazer a gestão das notas numa tela em vez de
na planilha — *"planilha é frágil, é uma bagunça"*.

O que estes testes vigiam, e é o que pode estragar silenciosamente: o MAPA de
colunas. A "Notas BWS" guarda os tributos do Omie em BB:BM, em pares valor/retém,
e na ordem PIS, COFINS, **CSLL, IR**, ISS, INSS — que NÃO é a ordem do nosso
cabeçalho. Trocar uma pela outra põe o ISS no lugar do IR, e ninguém percebe:
são dois números plausíveis na mesma linha.
"""
import os
import sys

import pytest
from decimal import Decimal

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

import base_faturamento as bf      # noqa: E402
import cdiarios                     # noqa: E402


def _linha_notas_bws():
    """Uma linha da "Notas BWS" no formato REAL, com o cabeçalho lido da planilha
    em 09/10/2026. Os índices importam mais que os valores."""
    linha = [""] * 65
    linha[0] = "CREPEMIRANDIBA"            # A Código Obra
    linha[4] = "2026"                      # E Ano
    linha[5] = "3271"                      # F Nº Nota
    linha[6] = "28/09/2026"                # G Data Emissão
    linha[7] = "169.228,34"                # H Valor da Nota
    linha[8] = "CREPEMIRANDIBA"            # I Código Obra
    linha[9] = "12"                        # J Nº Med.
    linha[11] = ""                         # L Observação
    linha[12] = "15/10/2026"               # M Data de Recebimento
    linha[13] = "144.808,69"               # N Valor Recebido em Conta
    linha[14] = "144.808,69"               # O Valor a ser Recebido pelo Destaque
    # P:BA — LIXO. Três repetições do mesmo conjunto de tributos, de uma
    # metodologia abandonada. Valores absurdos aqui de propósito: se algum deles
    # aparecer na base, o teste denuncia.
    linha[17] = "999.999,99"               # R Valor em Serviços
    linha[18] = "888.888,88"               # S Valor em Materiais
    linha[19] = "777.777,77"               # T PIS (bloco antigo)
    linha[24] = "666.666,66"               # Y ISS (bloco antigo)
    # BB:BM — os tributos do OMIE, em pares valor/retém, NA ORDEM DO SCRIPT
    linha[53], linha[54] = "1.099,98", "S"   # BB/BC PIS
    linha[55], linha[56] = "5.076,85", "S"   # BD/BE COFINS
    linha[57], linha[58] = "1.827,66", "S"   # BF/BG CSLL
    linha[59], linha[60] = "2.030,74", "S"   # BH/BI IR
    linha[61], linha[62] = "2.538,43", "S"   # BJ/BK ISS
    linha[63], linha[64] = "0,00", "N"       # BL/BM INSS (não retido)
    return linha


# --------------------------------------------------------------------------- #
# O cabeçalho
# --------------------------------------------------------------------------- #
def test_o_cabecalho_nao_tem_nome_repetido():
    """Nome repetido faria o índice apontar para a coluna errada — e é
    exatamente o defeito da planilha antiga, que tem "PIS (0,65%)" três vezes."""
    assert len(bf.CAB) == len(set(bf.CAB))


def test_os_campos_que_nunca_foram_gravados_existem_na_base():
    """A parte do pedido que não dava para resolver com cruzamento: nada disso
    está em planilha nenhuma hoje."""
    for campo in ("medicao_periodo_ini", "medicao_periodo_fim", "discriminacao",
                  "ibs", "cbs"):
        assert campo in bf.IDX, campo


@pytest.mark.parametrize("campo", ["empresa", "empresa_cnpj", "scp", "scp_cnpj",
                                   "contrato", "tributacao", "centro_custo",
                                   "municipio_obra", "obra_codigo_primario"])
def test_o_que_vem_da_c_diarios_NAO_entra_na_base(campo):
    """Correção do dono em 09/10/2026: *"informação que vem da C. Diários não
    precisa entrar na base, a gente vai cruzar"*. Guardar atributo de obra aqui
    seria duplicar o que já tem dono — e a base ficaria desatualizada sozinha."""
    assert campo not in bf.IDX


def test_a_chave_do_cruzamento_com_a_c_diarios_fica():
    """Sem o código da obra não há como cruzar nada."""
    assert "obra_codigo" in bf.IDX


def test_campo_inventado_e_recusado_em_vez_de_virar_coluna_errada():
    with pytest.raises(KeyError, match="não existe"):
        bf.montar_linha({"valor_da_nota": "100"})


def test_a_linha_sai_sempre_com_o_tamanho_do_cabecalho():
    linha = bf.montar_linha({"nota_numero": "3271"})
    assert len(linha) == len(bf.CAB)
    assert linha[bf.IDX["nota_numero"]] == "3271"
    assert linha[bf.IDX["atualizado_em"]], "a linha registra quando foi escrita"


# --------------------------------------------------------------------------- #
# O mapa da "Notas BWS" — onde um índice errado troca um imposto por outro
# --------------------------------------------------------------------------- #
def test_o_bloco_de_formula_da_planilha_e_IGNORADO():
    """⚠️ Correção de 09/10/2026. A primeira versão lia T:Y como se fossem os
    tributos da nota. O dono: *"não existe aquilo dali, aquilo são repetições, é
    outra metodologia que eu utilizava, dali é lixo"*. Ler dali encheria a base
    de números plausíveis e errados — o pior resultado possível, porque ninguém
    desconfia de um número com cara de certo."""
    d = bf.de_notas_bws(_linha_notas_bws())
    inteiro = " ".join(str(v) for v in d.values())
    for lixo in ("999.999,99", "888.888,88", "777.777,77", "666.666,66"):
        assert lixo not in inteiro, f"entrou lixo da planilha: {lixo}"


def test_o_bloco_BB_BM_entra_como_o_tributo_da_NOTA():
    """Esclarecimento do dono em 09/10/2026: *"a parte de tributos Omie, aquilo
    dali eu criei exatamente para equalizar. Já está tudo equalizado ali."*

    Então BB:BM não é "o que o Omie tem por acaso" — é o valor ACORDADO entre a
    nota e o título, e para as notas antigas é o único registro que existe dos
    tributos delas. Vai para o lado da NOTA."""
    d = bf.de_notas_bws(_linha_notas_bws())
    assert d["pis"] == "1.099,98"
    assert d["cofins"] == "5.076,85"
    assert d["csll"] == "1.827,66"
    assert d["ir"] == "2.030,74"
    assert d["iss"] == "2.538,43"
    assert d["retem_pis"] == "S"


def test_o_lado_OMIE_fica_vazio_ate_alguem_consultar():
    """As colunas `omie_*` são o que a consulta devolver AGORA. É comparando as
    duas que se vê se o título saiu do lugar depois de equalizado."""
    d = bf.de_notas_bws(_linha_notas_bws())
    for t in ("pis", "cofins", "ir", "csll", "inss", "iss"):
        assert not d.get(f"omie_{t}"), t
    assert bf.conferir_tributos(d) == "", "sem consulta, não há o que divergir"


def test_imposto_nao_retido_guarda_valor_E_a_marca():
    """O valor fica (é informação), e o retém diz que não é descontado. Só o que
    foi retido entra na soma que vai para o Omie."""
    d = bf.de_notas_bws(_linha_notas_bws())
    assert d["inss"] == "0,00" and d["retem_inss"] == "N"


def test_os_valores_da_nota_saem_das_colunas_certas():
    d = bf.de_notas_bws(_linha_notas_bws())
    assert d["nota_numero"] == "3271"
    assert d["data_emissao"] == "28/09/2026"
    assert d["valor_total"] == "169.228,34"
    assert d["obra_codigo"] == "CREPEMIRANDIBA"
    assert d["medicao_numero"] == "12"
    assert d["valor_recebido"] == "144.808,69"
    assert d["valor_liquido_previsto"] == "144.808,69"


def test_os_tributos_nao_trocam_de_lugar():
    """BB:BM está na ordem PIS, COFINS, **CSLL, IR**, ISS, INSS — e o nosso
    cabeçalho está em outra. Este teste é a única coisa que impede o ISS de
    virar IR: são dois números plausíveis na mesma linha."""
    d = bf.de_notas_bws(_linha_notas_bws())
    assert (d["csll"], d["ir"]) == ("1.827,66", "2.030,74")


def test_imposto_da_nota_e_do_omie_sao_campos_SEPARADOS():
    """É a razão de a base existir: poder ver que os dois divergem. Guardar um
    só esconderia justamente o que o dono confere à mão hoje."""
    for t in ("pis", "cofins", "ir", "csll", "inss", "iss"):
        assert bf.IDX[t] != bf.IDX[f"omie_{t}"]


def test_nota_cancelada_e_lida_da_observacao():
    """Os scripts do Apps Script já ignoram linha com "CANCELADA" na coluna L. A
    base passa a ter isso num campo, em vez de escondido em texto livre."""
    linha = _linha_notas_bws()
    linha[11] = "NF CANCELADA - substituída pela 3285"
    assert bf.de_notas_bws(linha)["status"] == bf.STATUS_CANCELADA


# --------------------------------------------------------------------------- #
# As divergências — a conferência que hoje é feita à mão
# --------------------------------------------------------------------------- #
def test_recebimento_que_bate_e_recebimento_que_nao_bate():
    d = bf.de_notas_bws(_linha_notas_bws())
    assert bf.conferir_recebimento(d) == "N"      # 144.808,69 = 144.808,69
    d["valor_recebido"] = "140.000,00"
    assert bf.conferir_recebimento(d) == "S"


def test_sem_recebimento_a_divergencia_fica_VAZIA_e_nao_N():
    """Dizer "não divergente" numa nota que ninguém recebeu ainda seria mentira —
    e a tela mostraria como conferida uma coisa que não foi."""
    d = bf.de_notas_bws(_linha_notas_bws())
    d["valor_recebido"] = ""
    assert bf.conferir_recebimento(d) == ""


def test_sem_consulta_ao_omie_a_divergencia_de_tributos_fica_VAZIA():
    linha = _linha_notas_bws()
    for i in (53, 55, 57, 59, 61, 63):
        linha[i] = ""
    assert bf.conferir_tributos(bf.de_notas_bws(linha)) == ""


def test_titulo_que_saiu_do_lugar_depois_de_equalizado_e_acusado():
    """O caso que a tela do Omie serve para pegar: a nota tem o tributo
    equalizado, a consulta volta com outro valor — alguém mexeu no título."""
    d = bf.de_notas_bws(_linha_notas_bws())
    for t in ("pis", "cofins", "csll", "ir", "iss", "inss"):
        d[f"omie_{t}"] = d[t]
    assert bf.conferir_tributos(d) == "N", "equalizado: tem de bater"
    d["omie_ir"] = "1.000,00"
    assert bf.conferir_tributos(d) == "S"


def test_valor_em_pt_br_com_milhar_e_lido_certo():
    """A planilha é pt-BR: 1.234,56. Ler como inglês daria 1,23456 — e a
    divergência apareceria em toda nota."""
    from decimal import Decimal
    assert bf._decimal("169.228,34") == Decimal("169228.34")
    assert bf._decimal("R$ 1.099,98") == Decimal("1099.98")
    assert bf._decimal("") == Decimal("0")


# --------------------------------------------------------------------------- #
# O cruzamento com as outras planilhas
# --------------------------------------------------------------------------- #
class _ObraFalsa:
    codigo_primario = "CREPEMIRANDIBA"
    centro_custo = "CRECHE MIRANDIBA"
    contrato = "268/2025"
    municipio = "Mirandiba-PE"
    tributacao = "ONERADA - 50/50 - 80/20 - IR"
    aliquota_iss = "3"
    cliente = "PREFEITURA DE MIRANDIBA"
    cnpj_cliente = "10572071000112"
    empresa = "BWS CONSTRUCOES LTDA"
    empresa_cnpj = "00079526000109"
    scp = "SCP MIRANDIBA I"
    scp_cnpj = "11222333000144"


def test_a_c_diarios_entrega_empresa_e_SCP_para_a_TELA_cruzar():
    """A base não guarda esses campos (ver acima), mas o carregador passou a
    lê-los: é a tela que cruza pelo código da obra. Nem toda obra é faturada no
    CNPJ da BWS, e algumas são SCP com CNPJ próprio."""
    o = _ObraFalsa()
    assert o.empresa and o.scp and o.scp_cnpj


def test_o_card_do_pipefy_vem_da_aba_protocolos_e_virou_link():
    d = bf.de_notas_bws(_linha_notas_bws())
    proto = ["CREPEMIRANDIBA-12", "", "1447316614", "PLG-ABC123"]
    bf.cruzar(d, proto=proto)
    assert d["card_id"] == "1447316614"
    assert d["link_card"].endswith("1447316614")
    assert d["omie_codigo_integracao"] == "PLG-ABC123"


def test_a_chave_da_aba_protocolos_e_obra_mais_medicao():
    assert bf.chave_obra_medicao("crepemirandiba", "12") == "CREPEMIRANDIBA-12"


def test_os_links_vem_da_aba_de_links():
    d = bf.de_notas_bws(_linha_notas_bws())
    link = ["3271 - CREPEMIRANDIBA", "3271", "2026", "CREPEMIRANDIBA", "NOTA",
            "https://drive/mun", "https://drive/nac", "https://drive/rec"]
    bf.cruzar(d, link=link)
    assert d["link_nfse_municipal"] == "https://drive/mun"
    assert d["link_nfse_nacional"] == "https://drive/nac"
    assert d["link_recibo"] == "https://drive/rec"


def test_a_chave_de_acesso_vem_do_controle_nacional_e_define_o_modelo():
    d = bf.de_notas_bws(_linha_notas_bws())
    ctrl = [""] * 15
    ctrl[0] = "3271"
    ctrl[13] = "2" * 50
    bf.cruzar(d, ctrl=ctrl)
    assert d["chave_acesso"] == "2" * 50
    assert d["modelo"] == "nacional"


def test_sem_chave_o_modelo_e_o_antigo():
    d = bf.de_notas_bws(_linha_notas_bws())
    bf.cruzar(d)
    assert d["modelo"] == "abrasf"


def test_o_sequencial_e_a_competencia_saem_sozinhos():
    d = bf.de_notas_bws(_linha_notas_bws())
    bf.cruzar(d)
    assert d["nota_sequencial"] == "3271"
    assert d["competencia"] == "2026-09"


def test_nota_nacional_tem_o_sequencial_extraido_do_numero_longo():
    d = {"nota_numero": "2600000003283", "data_emissao": "2026-10-08"}
    bf.cruzar(d)
    assert d["nota_sequencial"] == "3283"
    assert d["competencia"] == "2026-10"


def test_o_cruzamento_nao_sobrescreve_o_que_a_notas_bws_trouxe():
    """A "Notas BWS" é a fonte dos valores da nota; as outras abas só
    acrescentam. Sobrescrever faria o valor mudar conforme a ordem das leituras."""
    d = bf.de_notas_bws(_linha_notas_bws())
    d["tomador_cnpj"] = "11111111111111"
    ctrl = [""] * 15
    ctrl[0], ctrl[6] = "3271", "22222222222222"
    bf.cruzar(d, ctrl=ctrl)
    assert d["tomador_cnpj"] == "11111111111111"


def test_duplicata_nas_abas_de_apoio_nao_muda_o_resultado_entre_rodadas():
    """A "Notas BWS Links" tem ~20 mil linhas com duplicatas conhecidas. A
    primeira ocorrência ganha, sempre — senão a base mudaria de valor entre duas
    consolidações sem nada ter mudado na origem."""
    linhas = [["a", "3271", "", "", "", "PRIMEIRA"],
              ["b", "3271", "", "", "", "SEGUNDA"]]
    idx = bf.indexar(linhas, bf.LK_NUMERO)
    assert idx["3271"][5] == "PRIMEIRA"


# --------------------------------------------------------------------------- #
# A C. Diários passou a ler empresa e SCP
# --------------------------------------------------------------------------- #
def test_a_c_diarios_le_empresa_e_scp_pelo_nome_do_cabecalho():
    linhas = [
        ["Secundário", "Código Primário", "Centro de Custo", "Empresa", "SCP",
         "CNPJ SCP"],
        ["AREFORTAL09", "CREPEMIRANDIBA", "CRECHE", "OUTRA EMPRESA LTDA",
         "SCP X", "11222333000144"],
    ]
    obra = cdiarios.carregar_obras(linhas)["CREPEMIRANDIBA"]
    assert obra.empresa == "OUTRA EMPRESA LTDA"
    assert obra.scp == "SCP X"
    assert obra.scp_cnpj == "11222333000144"


def test_coluna_de_empresa_ausente_deixa_o_campo_vazio_e_nao_inventa_bws():
    """A coluna de SCP ainda vai ser criada pelo dono. Até lá o campo fica
    vazio — e a base registra isso, em vez de afirmar "BWS"."""
    linhas = [["Secundário", "Código Primário"], ["X", "CREPEMIRANDIBA"]]
    obra = cdiarios.carregar_obras(linhas)["CREPEMIRANDIBA"]
    assert obra.empresa == "" and obra.scp == ""


# --------------------------------------------------------------------------- #
# A tela
# --------------------------------------------------------------------------- #
TOKEN = "TOKEN-DE-TESTE"


@pytest.fixture
def cliente(monkeypatch):
    from app.main import app
    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    return app.test_client()


def test_sem_token_a_tela_de_faturamento_nao_abre(cliente):
    assert cliente.get("/emissao/faturamento").status_code == 403


def test_a_tela_explica_que_roda_em_lotes_e_nao_apaga_nada(cliente):
    corpo = cliente.get(f"/emissao/faturamento?token={TOKEN}").get_data(as_text=True)
    assert "lotes" in corpo
    assert "Não apaga" in corpo or "não apaga" in corpo
    assert "Base Faturamento" in corpo


def test_a_tela_de_emissao_tem_link_para_a_base(cliente):
    corpo = cliente.get(f"/emissao/?token={TOKEN}",
                        follow_redirects=True).get_data(as_text=True)
    assert f"/emissao/faturamento?token={TOKEN}" in corpo


# --------------------------------------------------------------------------- #
# Coluna que nada preenche é lixo — o defeito que a planilha antiga tem
# --------------------------------------------------------------------------- #
def test_a_nota_nova_nasce_com_o_sequencial_preenchido():
    """O emissor monta a linha com os dados que ele SABE, e o `cruzar` fecha o
    que é derivado. Sem essa chamada a nota nova nascia sem `nota_sequencial` —
    justamente o campo pelo qual o dono procura a nota (3283, e não
    2600000003283)."""
    d = {"nota_numero": "2600000003284", "data_emissao": "2026-10-08",
         "chave_acesso": "2" * 50, "origem": "emissor"}
    bf.cruzar(d)
    assert d["nota_sequencial"] == "3284"
    assert d["modelo"] == "nacional"
    assert d["competencia"] == "2026-10"


def test_o_emissor_preenche_tudo_menos_o_que_depende_do_omie():
    """Coluna que NADA preenche é exatamente o lixo de que o dono reclama na
    planilha antiga — e foi conferindo isto que três colunas órfãs apareceram
    (`link_xml`, `id_dps`, `tomador_municipio`), hoje preenchidas.

    O teste exige que as vazias sejam SÓ três grupos, cada um com motivo: o do
    Omie (depende de uma decisão dele, ver FATURAMENTO.md §5), o do recebimento
    (acontece depois da emissão) e a observação (texto escrito à mão). Coluna
    nova sem ninguém para preenchê-la quebra aqui."""
    import re
    fonte = open(os.path.join(_EMISSAONF, "concluir.py"), encoding="utf-8").read()
    bloco = fonte.split("dados_base = {")[1].split("\n        # O `cruzar`")[0]
    gravados = set(re.findall(r'"([a-z_]+)":', bloco))
    derivados = {"nota_sequencial", "divergencia_tributos", "divergencia_recebimento",
                 "atualizado_em"}          # o `cruzar` e o `montar_linha` fecham
    vazios = [c for c in bf.CAB if c not in gravados and c not in derivados]
    esperado = [
        # o grupo do Omie: preenchido pela tela /emissao/omie, não pela emissão
        "omie_pis", "omie_cofins", "omie_ir", "omie_csll", "omie_inss", "omie_iss",
        "omie_codigo_lancamento", "omie_numero_documento", "omie_valor_titulo",
        "omie_conferido_em",
        # IBS/CBS só existem no modelo nacional, e o `if nacional` os preenche
        # fora do dicionário — por isso não aparecem na varredura textual
        "ibs", "cbs",
        # o recebimento acontece DEPOIS da emissão — vazio aqui é o certo
        "data_recebimento", "valor_recebido",
        # observação é texto que alguém escreve à mão (ex.: "CANCELADA")
        "observacao",
    ]
    assert sorted(vazios) == sorted(esperado), (
        "coluna sem ninguém para preencher é lixo: " + str(sorted(vazios)))


# --------------------------------------------------------------------------- #
# A nota NOVA grava conforme o EMITIDO — e os três estados do campo
#
# Pedido do dono em 09/10/2026: *"as novas notas já têm a informação dos tributos
# emitidos, então vamos gravar conforme. Se necessário, a posteriori eu
# equalizo."*
#
# A diferença não é detalhe: o motor fiscal calcula os cinco federais SEMPRE (era
# assim que a coluna P da planilha antiga era feita), mas a nota só DECLARA o que
# foi retido — imposto não retido nem aparece no XML, que é a regra do E0699.
# Gravar o valor calculado de um imposto não retido afirmaria uma retenção que
# não houve.
# --------------------------------------------------------------------------- #
def _bloco_de_tributos_do_emissor():
    """O trecho do `concluir.py` que monta os tributos, lido do código."""
    fonte = open(os.path.join(_EMISSAONF, "concluir.py"), encoding="utf-8").read()
    return fonte.split("dados_base = {")[1].split("\n        # IBS e CBS")[0]


@pytest.mark.parametrize("imposto,sigla", [
    ("pis", "PIS"), ("cofins", "COFINS"), ("ir", "IR"), ("csll", "CSLL")])
def test_federal_nao_retido_vai_como_zero_e_nao_com_o_valor_calculado(imposto, sigla):
    bloco = _bloco_de_tributos_do_emissor()
    linha = [l for l in bloco.splitlines() if f'"{imposto}":' in l]
    assert linha, imposto
    assert f'"{sigla}" in fed' in linha[0], (
        f"{imposto} tem de ser gravado só quando retido")
    assert '"0.00"' in linha[0], f"{imposto} não retido tem de ir como 0,00"


def test_o_ISS_e_gravado_mesmo_sem_retencao():
    """É o único que a nota declara de qualquer jeito: a prefeitura o calcula e
    ele sai na nota; o que muda é quem recolhe."""
    bloco = _bloco_de_tributos_do_emissor()
    linha = [l for l in bloco.splitlines() if '"iss":' in l]
    assert linha and "in fed" not in linha[0]


def test_os_tres_estados_do_campo_de_tributo_sao_distinguiveis():
    """Vazio, 0,00+N e valor+S querem dizer coisas diferentes — e é o vazio que
    impede a tela do Omie de equalizar um título às cegas."""
    import omie_conferencia as oc
    nao_sei = [{"nota_numero": "1", "status": "valida", "valor_total": "100,00"}]
    nao_reteve = [{"nota_numero": "2", "status": "valida", "valor_total": "100,00",
                   "pis": "0,00", "retem_pis": "N"}]
    reteve = [{"nota_numero": "3", "status": "valida", "valor_total": "100,00",
               "pis": "0,65", "retem_pis": "S"}]
    assert oc.tem_tributos_declarados(nao_sei) is False
    assert oc.tem_tributos_declarados(nao_reteve) is True
    assert oc.tem_tributos_declarados(reteve) is True
    # e a soma só conta o retido
    assert oc.somar_tributos(nao_reteve)["pis"] == Decimal("0")
    assert oc.somar_tributos(reteve)["pis"] == Decimal("0.65")
