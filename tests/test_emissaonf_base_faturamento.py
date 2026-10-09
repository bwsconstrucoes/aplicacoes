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
    linha[17] = "84.614,17"                # R Valor em Serviços
    linha[18] = "84.614,17"                # S Valor em Materiais
    linha[19] = "1.099,98"                 # T PIS
    linha[20] = "5.076,85"                 # U COFINS
    linha[21] = "2.030,74"                 # V IR
    linha[22] = "1.827,66"                 # W CSLL
    linha[23] = "0,00"                     # X INSS
    linha[24] = "2.538,43"                 # Y ISS
    # BB:BM — os tributos do Omie, em pares valor/retém, NA ORDEM DO SCRIPT
    linha[53] = "1.099,98"                 # BB valor_pis
    linha[55] = "5.076,85"                 # BD valor_cofins
    linha[57] = "1.827,66"                 # BF valor_csll
    linha[59] = "2.030,74"                 # BH valor_ir
    linha[61] = "2.538,43"                 # BJ valor_iss
    linha[63] = "0,00"                     # BL valor_inss
    return linha


# --------------------------------------------------------------------------- #
# O cabeçalho
# --------------------------------------------------------------------------- #
def test_o_cabecalho_nao_tem_nome_repetido():
    """Nome repetido faria o índice apontar para a coluna errada — e é
    exatamente o defeito da planilha antiga, que tem "PIS (0,65%)" três vezes."""
    assert len(bf.CAB) == len(set(bf.CAB))


def test_os_tres_campos_que_nunca_foram_gravados_existem_na_base():
    """Era a parte do pedido que não dava para resolver com cruzamento: estes
    três não estão em planilha nenhuma hoje."""
    for campo in ("medicao_periodo_ini", "medicao_periodo_fim", "discriminacao",
                  "empresa", "scp"):
        assert campo in bf.IDX, campo


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
def test_os_valores_da_nota_saem_das_colunas_certas():
    d = bf.de_notas_bws(_linha_notas_bws())
    assert d["nota_numero"] == "3271"
    assert d["data_emissao"] == "28/09/2026"
    assert d["valor_total"] == "169.228,34"
    assert d["obra_codigo"] == "CREPEMIRANDIBA"
    assert d["medicao_numero"] == "12"
    assert d["valor_recebido"] == "144.808,69"
    assert d["valor_liquido_previsto"] == "144.808,69"


def test_os_tributos_do_omie_nao_trocam_de_lugar():
    """BB:BM está na ordem PIS, COFINS, CSLL, IR, ISS, INSS — e o nosso cabeçalho
    está em outra. Este teste é a única coisa que impede o ISS de virar IR."""
    d = bf.de_notas_bws(_linha_notas_bws())
    assert d["omie_pis"] == "1.099,98"
    assert d["omie_cofins"] == "5.076,85"
    assert d["omie_csll"] == "1.827,66"
    assert d["omie_ir"] == "2.030,74"
    assert d["omie_iss"] == "2.538,43"
    assert d["omie_inss"] == "0,00"


def test_imposto_da_nota_e_do_omie_sao_campos_SEPARADOS():
    """É a razão de a base existir: poder ver que os dois divergem. Guardar um
    só esconderia justamente o que o dono confere à mão hoje."""
    d = bf.de_notas_bws(_linha_notas_bws())
    assert d["ir"] == "2.030,74" and d["omie_ir"] == "2.030,74"
    assert bf.IDX["ir"] != bf.IDX["omie_ir"]


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


def test_tributo_que_o_omie_tem_diferente_da_nota_e_acusado():
    linha = _linha_notas_bws()
    linha[59] = "1.000,00"           # BH: IR no Omie diferente do da nota
    assert bf.conferir_tributos(bf.de_notas_bws(linha)) == "S"


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


def test_a_empresa_e_a_SCP_entram_pela_c_diarios():
    """O pedido foi explícito: nem toda obra é faturada no CNPJ da BWS, e
    algumas são SCP com CNPJ próprio. Nada disso era lido antes."""
    d = bf.de_notas_bws(_linha_notas_bws())
    bf.cruzar(d, obra=_ObraFalsa())
    assert d["empresa"] == "BWS CONSTRUCOES LTDA"
    assert d["scp"] == "SCP MIRANDIBA I"
    assert d["scp_cnpj"] == "11222333000144"
    assert d["contrato"] == "268/2025"
    assert d["tributacao"].startswith("ONERADA")


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
    """A "Notas BWS" é a fonte dos valores da nota; as outras só acrescentam.
    Sobrescrever faria o valor da nota mudar conforme a ordem das leituras."""
    d = bf.de_notas_bws(_linha_notas_bws())
    d["tomador_nome"] = "QUEM ESTÁ NA NOTA"
    bf.cruzar(d, obra=_ObraFalsa())
    assert d["tomador_nome"] == "QUEM ESTÁ NA NOTA"


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
