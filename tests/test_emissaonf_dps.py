# -*- coding: utf-8 -*-
"""
A declaração que vai para a prefeitura, conferida contra o schema OFICIAL.

Por que este arquivo existe: emitir nota é a única coisa deste repositório que
não se desfaz, e a prefeitura não tem como ser consultada "de mentira". O
schema oficial (`app/apps/emissaonf/xsd_nacional/`) é a única regra que dá para
aplicar aqui dentro — então é aqui que um campo fora de ordem, um valor fora do
domínio ou uma casa decimal sobrando é pego, antes de sair.

Os três campos que os testes vigiam com mais cuidado são os que, errados, fazem
a nota sair errada SEM NINGUÉM PERCEBER:

  - o tipo de retenção do ISS (o significado é invertido na intuição);
  - a dedução de material (sem ela a prefeitura cobra ISS sobre o valor cheio —
    foi o incidente de setembro/2026);
  - o código que diz quais federais foram retidos.
"""
import os
import sys
from decimal import Decimal

import pytest
from lxml import etree

# Os módulos do emissaonf se importam de forma plana (`import el_nfse_nacional`),
# porque nasceram como scripts de linha de comando — o web.py faz o mesmo.
_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

import el_nfse_nacional as nac          # noqa: E402
import montar_dps                        # noqa: E402
from el_nfse_abrasf import DadosRps      # noqa: E402
from tributacao import calcular, parse_categoria  # noqa: E402

XSD_DPS = os.path.join(_EMISSAONF, "xsd_nacional", "DPS_v1.01.xsd")


@pytest.fixture(scope="module")
def schema():
    return etree.XMLSchema(etree.parse(XSD_DPS))


class ObraFalsa:
    """Só o que o montador de DPS lê de uma obra."""
    cno = "90.025.25410/76"
    tributacao = "ONERADA - 50/50 - 80/20 - IR"
    aliquota_iss = "5"
    municipio = "Belem do Sao Francisco-PE"


def _dados_rps(discriminacao="PAGAMENTO DA 10a MEDICAO DA OBRA, CONTRATO 268/2025."):
    return DadosRps(
        numero_rps=3084, serie_rps="1", data_emissao="2026-10-07",
        toma_doc="10572071000112", toma_razao="SECRETARIA DE EDUCACAO",
        toma_logradouro="AVENIDA AFONSO OLINDENSE", toma_numero="1513",
        toma_bairro="VARZEA", toma_cmun="2611606", toma_uf="PE", toma_cep="50810900",
        discriminacao=discriminacao,
        codigo_servico_nacional="070202", codigo_tributacao_municipio="702",
    )


def _calculo(categoria="ONERADA - 50/50 - 80/20 - IR", valor="98720.04", aliquota_iss="5"):
    return calcular(valor, parse_categoria(categoria), aliquota_iss=aliquota_iss,
                    bdi_diferenciado="0", iss_retido=True)


def _montar(**kw):
    r = kw.pop("r", None) or _calculo()
    return montar_dps.montar(
        card={}, obra=ObraFalsa(), r=r, dados_rps=kw.pop("dados_rps", None) or _dados_rps(),
        numero_nota=kw.pop("numero_nota", 3084), ibge_obra=kw.pop("ibge_obra", 2601607),
        data_emissao=kw.pop("data_emissao", "2026-10-07"), producao=kw.pop("producao", False),
    )


def _xml(d):
    return nac.montar_dps_xml(d)


def _txt(root, caminho):
    """Texto de um elemento pelo caminho de tags, sem precisar escrever o namespace."""
    el = root
    for tag in caminho.split("/"):
        el = el.find("{%s}%s" % (nac.NS_NFSE, tag))
        if el is None:
            return None
    return (el.text or "").strip()


# --------------------------------------------------------------------------- #
# O schema oficial
# --------------------------------------------------------------------------- #
def test_a_declaracao_de_uma_nota_real_passa_no_schema_oficial(schema):
    doc = etree.fromstring(etree.tostring(_xml(_montar())))
    assert schema.validate(doc), schema.error_log


@pytest.mark.parametrize("categoria", [
    "ONERADA - 50/50 - 80/20 - IR",
    "ONERADA - 100/0 - 100/0 - IR,PIS,COFINS,CSLL",
    "ONERADA - SD - SD - SEM RETENÇÃO",
    "ONERADA - 60/40 - 60/40 - PIS,COFINS",
])
def test_passa_no_schema_em_todas_as_tributacoes_que_a_bws_usa(schema, categoria):
    doc = etree.fromstring(etree.tostring(_xml(_montar(r=_calculo(categoria)))))
    assert schema.validate(doc), f"{categoria}: {schema.error_log}"


# --------------------------------------------------------------------------- #
# O ISS retido — o campo com significado invertido
# --------------------------------------------------------------------------- #
def test_iss_retido_na_fonte_vai_como_retido_pelo_tomador():
    """No layout nacional 1 é NÃO retido e 2 é retido pelo tomador. As notas da
    BWS são retidas: mandar 1 declararia que quem deve o ISS é a BWS."""
    root = _xml(_montar())
    assert _txt(root, "infDPS/valores/trib/tribMun/tpRetISSQN") == "2"


def test_iss_nao_retido_vai_como_nao_retido():
    r = calcular("1000.00", parse_categoria("ONERADA - 100/0 - 100/0 - IR"),
                 aliquota_iss="5", iss_retido=False)
    root = _xml(_montar(r=r))
    assert _txt(root, "infDPS/valores/trib/tribMun/tpRetISSQN") == "1"


# --------------------------------------------------------------------------- #
# A dedução de material — o incidente de setembro/2026
# --------------------------------------------------------------------------- #
def test_a_deducao_de_material_vai_na_declaracao():
    """Numa nota 80/20 a base do ISS é 80% do valor: os 20% restantes são a
    dedução, e é ela que impede a prefeitura de cobrar sobre o valor cheio."""
    r = _calculo("ONERADA - 50/50 - 80/20 - IR", valor="100000.00")
    root = _xml(_montar(r=r))
    assert _txt(root, "infDPS/valores/vDedRed/vDR") == "20000.00"


def test_sem_deducao_o_grupo_nao_e_enviado():
    """Em 100/0 não há dedução nenhuma — mandar zero seria declarar um grupo vazio."""
    root = _xml(_montar(r=_calculo("ONERADA - 100/0 - 100/0 - IR")))
    assert root.find(".//{%s}vDedRed" % nac.NS_NFSE) is None


def test_a_deducao_fecha_com_o_valor_do_servico(schema):
    r = _calculo("ONERADA - 50/50 - 60/40 - IR", valor="283340.50")
    root = _xml(_montar(r=r))
    v_serv = Decimal(_txt(root, "infDPS/valores/vServPrest/vServ"))
    v_ded = Decimal(_txt(root, "infDPS/valores/vDedRed/vDR"))
    assert v_serv - v_ded == r.base_iss


# --------------------------------------------------------------------------- #
# Quais federais foram retidos
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("pis,cofins,csll,esperado", [
    (True, True, True, 3),
    (True, True, False, 4),
    (True, False, False, 5),
    (False, True, False, 6),
    (False, True, True, 7),
    (False, False, True, 8),
    (True, False, True, 9),
    (False, False, False, 0),
])
def test_o_codigo_de_retencao_federal_cobre_as_oito_combinacoes(pis, cofins, csll, esperado):
    assert nac.tipo_retencao_pis_cofins(pis, cofins, csll) == esperado


def test_so_entra_o_grupo_de_pis_cofins_quando_algum_dos_dois_e_retido():
    sem = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - IR")))
    assert sem.find(".//{%s}piscofins" % nac.NS_NFSE) is None

    com = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - IR,PIS,COFINS")))
    assert com.find(".//{%s}piscofins" % nac.NS_NFSE) is not None


def test_imposto_nao_retido_vai_zerado_e_nao_omitido():
    """A categoria 'IR' retém só o IR: CSLL tem de ir zerado, não ausente —
    ausência e zero são lidas igual aqui, mas zero é explícito."""
    root = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - IR")))
    assert _txt(root, "infDPS/valores/trib/tribFed/vRetCSLL") == "0.00"
    assert Decimal(_txt(root, "infDPS/valores/trib/tribFed/vRetIRRF")) > 0


# --------------------------------------------------------------------------- #
# A identificação da declaração — o que faz a nota ser reencontrada depois
# --------------------------------------------------------------------------- #
def test_o_numero_da_declaracao_mantem_a_convencao_de_sempre():
    """nDPS = ano em 2 dígitos + número da nota em 13. É por essa identificação
    que a busca na SEFIN reencontra a nota; mudar o formato perderia as antigas."""
    assert montar_dps.numero_dps(3084, 2026) == int("26" + "0000000003084")


def test_a_identificacao_bate_com_a_que_o_job_nacional_ja_montava():
    """O job que fecha a parte nacional monta a mesma identificação por outro
    caminho. Se os dois divergirem, a nota emitida hoje não é achada amanhã."""
    import job_nacional
    d = _montar()
    pela_emissao = nac.gerar_id_dps(d.c_loc_emi, d.prest_cnpj, d.serie, d.n_dps)
    pelo_job = job_nacional._id_dps(3084, "26")
    assert pela_emissao == pelo_job


def test_a_hora_vai_no_fuso_de_brasilia_e_nao_em_utc():
    """O servidor roda em UTC. Mandar Z faria a prefeitura ler 3 horas a mais —
    e perto da virada do dia isso muda a DATA da nota."""
    assert _montar().dh_emi.endswith("-03:00")


# --------------------------------------------------------------------------- #
# O que a emissão BARRA antes de tentar
# --------------------------------------------------------------------------- #
def test_aliquota_de_iss_que_nao_cabe_no_layout_barra_a_emissao():
    r = _calculo(valor="1000.00", aliquota_iss="10")
    with pytest.raises(montar_dps.DadoIncompativel) as erro:
        _montar(r=r)
    assert "Alíquota de ISS" in str(erro.value)


def test_discriminacao_vazia_barra_a_emissao():
    with pytest.raises(montar_dps.DadoIncompativel):
        _montar(dados_rps=_dados_rps(discriminacao="   "))


def test_discriminacao_longa_demais_barra_a_emissao():
    with pytest.raises(montar_dps.DadoIncompativel) as erro:
        _montar(dados_rps=_dados_rps(discriminacao="X" * 2001))
    assert "2.000" in str(erro.value)


def test_obra_sem_municipio_resolvido_barra_a_emissao():
    with pytest.raises(montar_dps.DadoIncompativel) as erro:
        _montar(ibge_obra=None)
    assert "município da obra" in str(erro.value)


# --------------------------------------------------------------------------- #
# Ambiente
# --------------------------------------------------------------------------- #
def test_homologacao_e_producao_sao_declaradas_no_proprio_documento():
    assert _txt(_xml(_montar(producao=False)), "infDPS/tpAmb") == "2"
    assert _txt(_xml(_montar(producao=True)), "infDPS/tpAmb") == "1"


def test_a_producao_nao_leva_o_ambiente_no_endereco():
    """O portal da prefeitura publica a produção sem o segmento de ambiente; o
    manual em PDF diz o contrário, e quem está no ar é o portal."""
    cliente = nac.ELNfseNacional(token="x", chave_pem=b"", cert_pem=b"", ambiente="producao")
    assert cliente._url("nfse").endswith("/api/nacional/nfse")

    hom = nac.ELNfseNacional(token="x", chave_pem=b"", cert_pem=b"", ambiente="homologacao")
    assert hom._url("nfse").endswith("/api/nacional/homologacao/nfse")
