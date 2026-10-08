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


def _dados_rps_com_servico(codigo):
    """O mesmo tomador, trocando só o código de tributação nacional do serviço —
    é ele que decide se o grupo de obra é obrigatório."""
    d = _dados_rps()
    d.codigo_servico_nacional = codigo
    return d


def _montar(**kw):
    r = kw.pop("r", None) or _calculo()
    return montar_dps.montar(
        card={}, obra=kw.pop("obra", None) or ObraFalsa(), r=r,
        dados_rps=kw.pop("dados_rps", None) or _dados_rps(),
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


def test_imposto_nao_retido_e_OMITIDO_e_nao_vai_zerado():
    """⚠️ Este teste afirmava o CONTRÁRIO até 08/10/2026, e estava errado.

    Ele dizia que "ausência e zero são lidas igual aqui, mas zero é explícito".
    A plataforma nacional não as lê igual: ela recusa valor zero, com o erro
    **E0699** — "o valor do tributo CP deve ser maior que zero e menor que o
    valor do serviço informado na DPS". Custou uma emissão.

    Zero declara uma retenção DE valor zero, que é diferente de não haver
    retenção. É a mesma regra que o grupo piscofins (teste acima) já seguia, e
    que o modelo antigo seguia — o HISTORICO da área tem a decisão escrita desde
    21/09/2026: "imposto sem retenção não aparece na nota". A migração a perdeu
    para estes três campos, e só para eles.
    """
    root = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - IR")))
    assert _txt(root, "infDPS/valores/trib/tribFed/vRetCSLL") is None
    assert Decimal(_txt(root, "infDPS/valores/trib/tribFed/vRetIRRF")) > 0


def test_nota_sem_retencao_federal_nenhuma_nao_leva_o_grupo(schema):
    """Grupo vazio é válido no schema e não diz nada. Foi este o caso que deu
    E0699: obra sem retenção de INSS mandava vRetCP igual a 0,00."""
    d = _montar(r=_calculo("ONERADA - SD - SD - SEM RETENÇÃO"))
    root = _xml(d)
    assert root.find(".//{%s}tribFed" % nac.NS_NFSE) is None
    doc = etree.fromstring(etree.tostring(root))
    assert schema.validate(doc), schema.error_log


def test_retencao_maior_que_o_servico_derruba_a_declaracao():
    """A outra metade da regra do E0699. Retenção maior que o serviço é erro de
    dado — e falhar aqui é mais barato que descobrir num dia depois."""
    d = _montar()
    d.v_serv = "1000.00"
    d.v_ret_inss = "1000.00"
    with pytest.raises(ValueError, match="maior ou igual ao valor do serviço"):
        _xml(d)


def test_o_total_aproximado_de_tributos_e_declarado_como_NAO_INFORMADO(schema):
    """O layout dá uma escolha de quatro, e uma delas existe exatamente para
    quem não informa valor estimado: `indTotTrib=0`. Mandávamos a outra
    (`vTotTrib`) com os três valores em 0,00 — o que DECLARA que o total
    aproximado dos tributos é zero, e é falso. Mesmo defeito do vRetCP, só que
    este ainda não tinha dado erro."""
    doc = etree.fromstring(etree.tostring(_xml(_montar())))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/valores/trib/totTrib/indTotTrib") == "0"
    assert doc.find(".//{%s}vTotTrib" % nac.NS_NFSE) is None


def test_se_um_dia_quiserem_informar_o_total_a_outra_opcao_continua_valendo(schema):
    d = _montar()
    d.v_tot_trib = ("10.00", "0.00", "5.00")
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/valores/trib/totTrib/vTotTrib/vTotTribFed") == "10.00"
    assert doc.find(".//{%s}indTotTrib" % nac.NS_NFSE) is None


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


# --------------------------------------------------------------------------- #
# A CSLL retida sozinha — o campo que viajava sem ninguém declarar
# --------------------------------------------------------------------------- #
def test_csll_retida_sozinha_e_declarada_como_retencao(schema):
    """O código tpRetPisCofins é o ÚNICO lugar que diz quais dos três foram
    retidos. Sem o grupo, o valor da CSLL ia sozinho e nada dizia que houve
    retenção."""
    root = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - IR,CSLL")))
    pisc = root.find(".//{%s}piscofins" % nac.NS_NFSE)
    assert pisc is not None, "o grupo tem de ir quando só a CSLL é retida"
    assert _txt(pisc, "tpRetPisCofins") == "8"      # PIS/COFINS não retidos, CSLL retido
    assert Decimal(_txt(root, "infDPS/valores/trib/tribFed/vRetCSLL")) > 0
    doc = etree.fromstring(etree.tostring(root))
    assert schema.validate(doc), schema.error_log


def test_imposto_nao_retido_nao_vai_com_valor_zero_no_grupo():
    """Mandar "0,00" num imposto não retido não é o mesmo que não mandar: o
    primeiro declara uma retenção de valor zero."""
    root = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - IR,CSLL")))
    pisc = root.find(".//{%s}piscofins" % nac.NS_NFSE)
    assert pisc.find("{%s}vPis" % nac.NS_NFSE) is None
    assert pisc.find("{%s}vCofins" % nac.NS_NFSE) is None


def test_sem_retencao_nenhuma_o_grupo_nao_vai():
    """Mesmo comportamento do modelo antigo, que só mandava imposto retido."""
    root = _xml(_montar(r=_calculo("ONERADA - SD - SD - SEM RETENÇÃO")))
    assert root.find(".//{%s}piscofins" % nac.NS_NFSE) is None


def test_pis_e_cofins_retidos_levam_aliquota_e_valor(schema):
    root = _xml(_montar(r=_calculo("ONERADA - 50/50 - 80/20 - PIS,COFINS,CSLL")))
    pisc = root.find(".//{%s}piscofins" % nac.NS_NFSE)
    assert _txt(pisc, "pAliqPis") == "0.65"
    assert _txt(pisc, "pAliqCofins") == "3.00"
    assert _txt(pisc, "tpRetPisCofins") == "3"      # os três retidos
    doc = etree.fromstring(etree.tostring(root))
    assert schema.validate(doc), schema.error_log


# --------------------------------------------------------------------------- #
# O endereço de produção: duas fontes oficiais que se contradizem
# --------------------------------------------------------------------------- #
class _RespostaFalsa:
    def __init__(self, status):
        self.status_code = status
        self.text = "{}"

    def json(self):
        return {"idDPS": "DPS-DE-TESTE"}


class _SessaoFalsa:
    """Registra os endereços chamados e responde o que o teste mandar."""

    def __init__(self, *status):
        self.status = list(status)
        self.chamadas = []

    def request(self, metodo, url, **kw):
        self.chamadas.append(url)
        return _RespostaFalsa(self.status.pop(0) if self.status else 200)

    def mount(self, *a, **kw):
        pass


def _cliente(ambiente, *status):
    c = nac.ELNfseNacional(token="x", chave_pem=b"", cert_pem=b"", ambiente=ambiente)
    c.session = _SessaoFalsa(*status)
    return c


def test_quando_o_endereco_do_portal_responde_o_do_manual_nao_e_tentado():
    c = _cliente("producao", 200)
    c.consultar_dps("DPS1")
    assert len(c.session.chamadas) == 1
    assert "/api/nacional/dps/DPS1" in c.session.chamadas[0]


def test_endereco_que_nao_existe_libera_tentar_o_do_manual():
    """404 e 405 provam que o endereço não existe — a prefeitura não recebeu
    nada, então repetir não arrisca uma segunda nota."""
    c = _cliente("producao", 404, 200)
    c.consultar_dps("DPS1")
    assert len(c.session.chamadas) == 2
    assert "/api/nacional/producao/dps/DPS1" in c.session.chamadas[1]


@pytest.mark.parametrize("status", [200, 201, 400, 401, 422, 500, 503])
def test_qualquer_outra_resposta_nao_e_repetida(status):
    """Esta é a trava que importa: 400, 500 ou timeout podem ter chegado à
    prefeitura. Repetir o envio nesses casos arriscaria a segunda nota do mesmo
    serviço — o pior desfecho possível nesta área."""
    c = _cliente("producao", status, 200)
    try:
        c.consultar_dps("DPS1")
    except Exception:
        pass
    assert len(c.session.chamadas) == 1


def test_se_os_dois_enderecos_falharem_o_erro_e_o_do_caminho_preferido():
    c = _cliente("producao", 404, 404)
    try:
        c.consultar_dps("DPS1")
    except Exception:
        pass
    assert len(c.session.chamadas) == 2


# --------------------------------------------------------------------------- #
# O grupo de OBRA — o que faltava e derrubou a nota 3281
#
# Em 08/10/2026 a TI da prefeitura mostrou o erro que a plataforma nacional
# tinha guardado: E0370, "o grupo de informações de obra é obrigatório quando o
# código de tributação nacional pertencer a um dos subitens 07.02.01, 07.02.02
# (…)". A BWS emite SEMPRE em 07.02.02. O município aceitava a declaração e o
# nacional recusava, por isso a nota ficava "em processamento" para sempre.
#
# É o tipo de defeito que só um teste de schema não pega: a declaração sem o
# grupo de obra é VÁLIDA no XSD (o grupo é minOccurs="0"). A obrigatoriedade é
# regra de negócio da plataforma, não do arquivo. Por isso os testes abaixo
# vigiam a REGRA, e não só a forma.
# --------------------------------------------------------------------------- #
class _ObraSemCNO(ObraFalsa):
    cno = ""


def test_a_obra_vai_identificada_na_declaracao_pelo_CNO(schema):
    doc = etree.fromstring(etree.tostring(_xml(_montar())))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/serv/obra/cObra") == "900252541076"


def test_o_CNO_vai_sem_pontuacao_como_todo_documento_deste_layout():
    """O CNPJ, o CPF e o CEP já são enviados só com dígitos pelo construtor; o
    CNO segue a mesma regra. Na C. Diários ele está escrito com pontos e barra."""
    assert ObraFalsa.cno == "90.025.25410/76"      # como está na planilha
    d = _montar()
    assert d.obra.c_obra == "900252541076"


def test_o_grupo_de_obra_vem_depois_do_grupo_do_servico():
    """A ordem é exigida pelo XSD e é o tipo de erro que a prefeitura devolve
    como mensagem obscura. O schema já garante, mas isto deixa escrito."""
    root = _xml(_montar())
    serv = root.find("{%s}infDPS/{%s}serv" % (nac.NS_NFSE, nac.NS_NFSE))
    tags = [etree.QName(e).localname for e in serv]
    assert tags == ["locPrest", "cServ", "obra"]


def test_obra_sem_CNO_barra_a_emissao_antes_de_enviar():
    """Barrar aqui custa um aviso na tela. Deixar passar custa um número de nota
    queimado e uma declaração presa na fila — foi o que aconteceu com a 3281."""
    with pytest.raises(montar_dps.DadoIncompativel) as e:
        _montar(obra=_ObraSemCNO())
    msg = str(e.value)
    assert "CNO" in msg
    assert "C. Diários" in msg          # onde a pessoa resolve
    assert "E0370" in msg               # para casar com o erro que ela viu


@pytest.mark.parametrize("subitem", sorted(montar_dps.SUBITENS_QUE_EXIGEM_OBRA))
def test_todos_os_subitens_da_lista_do_erro_exigem_a_obra(subitem):
    d = _montar(dados_rps=_dados_rps_com_servico(subitem))
    assert d.obra is not None, f"{subitem} deveria exigir o grupo de obra"
    assert d.obra.c_obra == "900252541076"


def test_servico_fora_da_lista_nao_leva_grupo_de_obra(schema):
    """Mandar o grupo onde ele não é previsto é tão errado quanto omiti-lo onde
    é. A lista do erro E0370 é a regra, e nada além dela."""
    d = _montar(dados_rps=_dados_rps_com_servico("010101"))
    assert d.obra is None
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/serv/obra/cObra") is None


def test_servico_fora_da_lista_nao_exige_CNO():
    """Obra sem CNO só barra onde o nacional de fato exige."""
    d = _montar(obra=_ObraSemCNO(), dados_rps=_dados_rps_com_servico("010101"))
    assert d.obra is None


def test_CNO_com_tamanho_estranho_avisa_mas_nao_barra(capsys):
    """A plataforma é que valida o número contra a base da Receita. Barrar por
    palpite impediria uma obra legítima de faturar — mas o aviso sai no log,
    porque CNO truncado é a explicação mais provável de uma recusa com o grupo
    presente."""
    class _ObraCurta(ObraFalsa):
        cno = "90.025.254"
    d = _montar(obra=_ObraCurta())
    assert d.obra.c_obra == "90025254"
    assert "ATENÇÃO" in capsys.readouterr().out


# --- as outras duas identificações que o layout aceita ---------------------- #
def test_a_identificacao_pode_ser_o_CIB(schema):
    """Não é o caminho da BWS, mas é uma das três do layout. Fica provado para
    quando aparecer obra sem CNO e com CIB."""
    d = _montar()
    d.obra = nac.GrupoObra(c_cib="12345678")
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/serv/obra/cCIB") == "12345678"


def test_a_identificacao_pode_ser_o_ENDERECO_DA_OBRA(schema):
    """A terceira alternativa. A C. Diários não guarda o endereço da OBRA (o que
    ela tem é o do cliente, que é outra coisa), então hoje este caminho não é
    usado — mas ele é o que destrava uma obra sem CNO, e por isso tem de estar
    provado antes de ser preciso."""
    d = _montar()
    d.obra = nac.GrupoObra(end={"CEP": "61.760-000", "xLgr": "Rua Luis Moreira Gomes",
                                "nro": "100", "xBairro": "Centro"})
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/serv/obra/end/CEP") == "61760000"   # sem pontuação
    assert _txt(doc, "infDPS/serv/obra/end/xBairro") == "Centro"


def test_grupo_de_obra_sem_nenhuma_identificacao_e_recusado():
    """O layout exige UMA das três. Um grupo vazio passaria batido num
    `if obra:` e sairia na declaração como `<obra/>`."""
    d = _montar()
    d.obra = nac.GrupoObra()
    with pytest.raises(ValueError, match="UMA"):
        _xml(d)


def test_a_inscricao_imobiliaria_quando_houver_vem_antes_da_identificacao(schema):
    d = _montar()
    d.obra = nac.GrupoObra(c_obra="900252541076", insc_imob_fisc="1234567")
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log
    obra = doc.find("{%s}infDPS/{%s}serv/{%s}obra"
                    % (nac.NS_NFSE, nac.NS_NFSE, nac.NS_NFSE))
    assert [etree.QName(e).localname for e in obra] == ["inscImobFisc", "cObra"]


# --------------------------------------------------------------------------- #
# O CST do IBS/CBS — o erro E0959
#
# "cClassTrib não pertence ao grupo CST indicado." Mandávamos CST 000 com a
# classificação 200046, e eles não casam: **o CST são os três primeiros dígitos
# da classificação**. Está nos dados do Anexo VIII oficial — 000001 ("Situações
# tributadas integralmente") é do grupo 000; 200046 ("Operações com bens
# imóveis"), que é o nosso, é do grupo 200.
#
# Como o E0370, este também passa pelo schema: os dois campos são válidos
# sozinhos, e quem confere a combinação é a plataforma. Por isso a regra virou
# código — o CST é derivado, não digitado — e o construtor recusa um par que não
# casa, para o erro aparecer aqui e não num dia depois.
# --------------------------------------------------------------------------- #
def test_o_CST_sai_do_cClassTrib_e_nao_de_um_valor_digitado():
    g = nac.GrupoIBSCBS()
    assert g.c_class_trib == "200046"      # Anexo VIII, item 07.02
    assert g.cst == "200"                  # os três primeiros dígitos dela


def test_a_declaracao_de_obra_leva_CST_200(schema):
    doc = etree.fromstring(etree.tostring(_xml(_montar())))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/IBSCBS/valores/trib/gIBSCBS/CST") == "200"
    assert _txt(doc, "infDPS/IBSCBS/valores/trib/gIBSCBS/cClassTrib") == "200046"


@pytest.mark.parametrize("classificacao,cst", [
    ("000001", "000"),   # situações tributadas integralmente
    ("200046", "200"),   # operações com bens imóveis  <- o nosso
    ("200045", "200"),   # reabilitação urbana
    ("400001", "400"),   # transporte público coletivo
])
def test_o_CST_derivado_segue_a_tabela_oficial(classificacao, cst):
    assert nac.GrupoIBSCBS(c_class_trib=classificacao).cst == cst


def test_CST_digitado_que_nao_casa_derruba_a_declaracao():
    """O par 000 + 200046 era exatamente o que ia, e a plataforma recusou um dia
    depois. Agora falha na montagem, com o motivo e o valor certo na mensagem."""
    d = _montar()
    d.ibscbs = nac.GrupoIBSCBS(c_class_trib="200046", cst="000")
    with pytest.raises(ValueError) as e:
        _xml(d)
    assert "200046" in str(e.value)
    assert "o CST é 200" in str(e.value)


def test_CST_digitado_que_casa_e_aceito(schema):
    d = _montar()
    d.ibscbs = nac.GrupoIBSCBS(c_class_trib="200046", cst="200")
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log


def test_o_codigo_indicador_da_operacao_tem_os_seis_digitos_do_schema():
    """No Anexo VIII ele aparece como 20201, porque o Excel come o zero da
    frente. O schema exige SEIS dígitos — 020201."""
    assert nac.GrupoIBSCBS().c_ind_op == "020201"
    assert len(nac.GrupoIBSCBS().c_ind_op) == 6


# --- os dois campos opcionais que são os próximos suspeitos ---------------- #
def test_por_padrao_tpOper_e_tpEnteGov_nao_sao_enviados(schema):
    """Mandar valor fiscal por palpite é pior que omitir campo opcional: o tipo
    de ente governamental não dá para deduzir do CNPJ do tomador."""
    doc = etree.fromstring(etree.tostring(_xml(_montar())))
    assert schema.validate(doc), schema.error_log
    assert _txt(doc, "infDPS/IBSCBS/tpOper") is None
    assert _txt(doc, "infDPS/IBSCBS/tpEnteGov") is None


def test_quando_preenchidos_tpOper_e_tpEnteGov_saem_na_ordem_do_schema(schema):
    """Deixados prontos: se a próxima recusa pedir um deles, é uma linha. A ordem
    dentro do grupo é exigida pelo XSD."""
    d = _montar()
    d.ibscbs = nac.GrupoIBSCBS(tp_oper="1", tp_ente_gov="4")
    doc = etree.fromstring(etree.tostring(_xml(d)))
    assert schema.validate(doc), schema.error_log
    grupo = doc.find("{%s}infDPS/{%s}IBSCBS" % (nac.NS_NFSE, nac.NS_NFSE))
    assert [etree.QName(e).localname for e in grupo] == [
        "finNFSe", "indFinal", "cIndOp", "tpOper", "tpEnteGov", "indDest", "valores"]


# --------------------------------------------------------------------------- #
# A varredura: NENHUM campo opcional vai com zero
#
# Este teste existe porque o mesmo defeito apareceu três vezes em formas
# diferentes — PIS/COFINS (visto na migração), vRetCP/vRetIRRF/vRetCSLL (erro
# E0699, 08/10/2026) e o total aproximado de tributos (que ainda não tinha dado
# erro). A plataforma trata "zero" e "ausente" como coisas diferentes, e o
# schema não ajuda: campo opcional com zero é um arquivo válido.
#
# Em vez de confiar em lembrar da regra campo por campo, aqui a declaração é
# varrida inteira contra o XSD: todo elemento que o layout permite OMITIR e que
# está indo com valor zero é um defeito, salvo os indicadores — neles o zero é
# um SIGNIFICADO ("não é consumidor final"), não um valor.
# --------------------------------------------------------------------------- #
INDICADORES_EM_QUE_ZERO_E_SIGNIFICADO = {
    "indFinal",      # 0 = tomador não é consumidor final
    "indDest",       # 0 = o destinatário é o próprio tomador
    "indTotTrib",    # 0 = não se informa valor estimado de tributos
    "finNFSe",       # 0 = finalidade normal
    "regEspTrib",    # 0 = nenhum regime especial
}


def _opcionais_do_layout():
    nomes = set()
    for arquivo in os.listdir(os.path.join(_EMISSAONF, "xsd_nacional")):
        if not arquivo.endswith(".xsd"):
            continue
        doc = etree.parse(os.path.join(_EMISSAONF, "xsd_nacional", arquivo))
        for el in doc.iter("{http://www.w3.org/2001/XMLSchema}element"):
            if el.get("minOccurs") == "0" and el.get("name"):
                nomes.add(el.get("name"))
    return nomes


@pytest.mark.parametrize("categoria", [
    "ONERADA - 50/50 - 80/20 - IR",
    "ONERADA - 100/0 - 100/0 - IR,PIS,COFINS,CSLL",
    "ONERADA - SD - SD - SEM RETENÇÃO",
    "ONERADA - 60/40 - 60/40 - PIS,COFINS",
])
def test_nenhum_campo_opcional_do_layout_vai_com_valor_zero(categoria):
    opcionais = _opcionais_do_layout()
    root = _xml(_montar(r=_calculo(categoria)))
    culpados = []
    for el in root.iter():
        tag = etree.QName(el).localname
        texto = (el.text or "").strip()
        if not texto or len(el) or tag in INDICADORES_EM_QUE_ZERO_E_SIGNIFICADO:
            continue
        try:
            zero = Decimal(texto) == 0
        except Exception:
            continue
        if zero and tag in opcionais:
            culpados.append(f"{tag}={texto}")
    assert not culpados, (
        f"{categoria}: campo opcional indo com zero — a plataforma recusa "
        f"(E0699): {culpados}")
