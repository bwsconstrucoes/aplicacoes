"""ATÉ ONDE O FORNECEDOR VENDE — e como isso cruza com o município da obra.

O dono achou o buraco em 21/09/2026:

    "Na cotação automática, eu acho que você não se atentou à importância
    disso. Os fornecedores, a gente não pode colocar para disparar uma cotação
    com qualquer fornecedor, tem que ter uma lógica. O fornecedor tem a região
    que atende, e existe o local da obra. Se não é um fornecedor que atenda a
    nível nacional, eu tenho que buscar na região da obra. Da forma que está, a
    gente simplesmente escreve de qualquer jeito, sem padronização. Como vamos
    cruzar obra x fornecedor?"

E O DADO REAL LHE DÁ RAZÃO. A coluna "Região de Atuação" da planilha de 1.772
fornecedores tem 27 grafias, misturando quatro níveis que não se comparam:

    país          BR (395 vezes)
    macrorregião  NE (256)
    estado        PE (263), CE (142), SP (87), PB, MT
    metropolitana RMF (204)
    microrregião  CARIRI (17)
    cidade        SÃO PAULO (396), TAUÁ CE, BARBALHA - CE, SOBRAL, BARRO…

Estes testes seguram as duas metades: a TRADUÇÃO desse texto e a PERGUNTA que o
disparo automático faz ("este fornecedor atende esta obra?").
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.suprimentos import regioes as svc


class Forn:
    """Um fornecedor de mentira, só com o que `atende` lê."""

    def __init__(self, abrangencia, ufs=(), municipios=()):
        self.abrangencia = abrangencia
        self.ufs_atendidas = list(ufs)
        self.municipios_atendidos = list(municipios)


# ---------------------------------------------------------------------------
# 1. Traduzir o que está escrito na planilha
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("texto", ["BR", "BRASIL", "NACIONAL", "br"])
def test_o_pais_vira_NACIONAL(texto):
    r = svc.traduzir([texto])
    assert r["abrangencia"] == svc.NACIONAL
    assert r["ufs"] == [] and r["municipios"] == [], \
        "nacional não precisa de lista: a lista seria o país inteiro"


def test_NE_vira_as_nove_ufs_do_nordeste():
    """256 fornecedores da planilha escreveram "NE". Sem traduzir, nenhum deles
    seria escolhido para uma obra no Ceará."""
    r = svc.traduzir(["NE"])
    assert r["abrangencia"] == svc.ESTADUAL
    assert r["ufs"] == ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]


def test_sigla_de_estado_vira_ESTADUAL():
    r = svc.traduzir(["CE", "PE"])
    assert r["abrangencia"] == svc.ESTADUAL
    assert r["ufs"] == ["CE", "PE"]


def test_RMF_vira_os_municipios_da_regiao_metropolitana():
    r = svc.traduzir(["RMF"])
    assert r["abrangencia"] == svc.REGIONAL
    assert "FORTALEZA" in r["municipios"]
    assert "EUSEBIO" in r["municipios"], "a obra do Eusébio tem de casar"
    assert "MARACANAU" in r["municipios"]


def test_CARIRI_vira_os_municipios_do_cariri():
    r = svc.traduzir(["CARIRI"])
    assert r["abrangencia"] == svc.REGIONAL
    assert {"JUAZEIRO DO NORTE", "CRATO", "BARBALHA"} <= set(r["municipios"])


@pytest.mark.parametrize("texto,esperado", [
    ("BARBALHA - CE", "BARBALHA"),
    ("TAUÁ CE", "TAUA"),
    ("JUAZEIRO DO NORTE - CE", "JUAZEIRO DO NORTE"),
    ("PACOTI CE", "PACOTI"),
])
def test_cidade_com_a_UF_colada_perde_a_UF_e_o_acento(texto, esperado):
    """A planilha escreve a cidade com a UF colada de três jeitos. Sem isto,
    "TAUÁ CE" nunca casaria com o município "TAUA" da obra."""
    r = svc.traduzir([texto])
    assert r["municipios"] == [esperado]
    assert r["abrangencia"] == svc.LOCAL


def test_cidade_sozinha_tambem_conta():
    r = svc.traduzir(["SOBRAL"])
    assert r["abrangencia"] == svc.LOCAL
    assert r["municipios"] == ["SOBRAL"]


def test_varios_termos_separados_por_virgula():
    r = svc.traduzir(["CE, RMF, FORTALEZA"])
    assert r["abrangencia"] == svc.ESTADUAL, "vale o MAIOR alcance encontrado"
    assert r["ufs"] == ["CE"]
    assert "FORTALEZA" in r["municipios"]


def test_o_maior_alcance_vence():
    """Quem escreveu "BR, CE" atende o Brasil. Restringir ao Ceará por causa da
    segunda palavra o tiraria de cotações que ele atende — e ninguém perceberia
    que faltou um preço."""
    assert svc.traduzir(["BR, CE"])["abrangencia"] == svc.NACIONAL


def test_texto_em_branco_fica_NAO_INFORMADA():
    r = svc.traduzir([])
    assert r["abrangencia"] == svc.NAO_INFORMADA


def test_termo_que_nao_e_lugar_e_RELATADO_e_nao_vira_municipio():
    """Aceitar qualquer coisa como município encheria o cadastro de "MATRIZ" e
    "A COMBINAR" — e o cruzamento passaria a mentir."""
    r = svc.traduzir(["A COMBINAR 123", "R$ 50"])
    assert r["abrangencia"] == svc.NAO_INFORMADA
    assert len(r["desconhecidos"]) == 2


# ---------------------------------------------------------------------------
# 2. A pergunta do disparo automático
# ---------------------------------------------------------------------------
def test_nacional_atende_qualquer_obra():
    pode, porque = svc.atende(Forn(svc.NACIONAL), "JUAZEIRO DO NORTE", "CE")
    assert pode and "todo o Brasil" in porque


def test_estadual_atende_a_obra_do_estado_dele():
    pode, porque = svc.atende(Forn(svc.ESTADUAL, ufs=["CE"]), "SOBRAL", "CE")
    assert pode and "atende o estado" in porque


def test_estadual_NAO_atende_obra_de_outro_estado():
    pode, porque = svc.atende(Forn(svc.ESTADUAL, ufs=["SP"]), "SOBRAL", "CE")
    assert not pode and "não atende CE" in porque


def test_regional_atende_so_os_municipios_da_lista():
    forn = Forn(svc.REGIONAL, municipios=["FORTALEZA", "CAUCAIA", "EUSEBIO"])
    assert svc.atende(forn, "EUSEBIO", "CE")[0] is True
    assert svc.atende(forn, "JUAZEIRO DO NORTE", "CE")[0] is False


def test_o_acento_da_obra_nao_atrapalha():
    """A obra escreve "Tauá" e o cadastro guarda "TAUA". Comparar sem
    normalizar faria o cruzamento falhar sem dar erro nenhum."""
    forn = Forn(svc.LOCAL, municipios=["TAUA"])
    assert svc.atende(forn, "Tauá", "CE")[0] is True


def test_sem_regiao_definida_fica_de_fora_e_o_motivo_diz_isso():
    pode, porque = svc.atende(Forn(svc.NAO_INFORMADA), "FORTALEZA", "CE")
    assert not pode
    assert "sem região definida" in porque


def test_estadual_sem_uf_nenhuma_nao_atende_ninguem():
    """Cadastro que diz atender e não atende lugar algum. O banco recusa (a
    migração 079 tem CHECK), e aqui a regra também fecha."""
    assert svc.atende(Forn(svc.ESTADUAL, ufs=[]), "FORTALEZA", "CE")[0] is False
