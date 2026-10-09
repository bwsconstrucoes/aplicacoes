# -*- coding: utf-8 -*-
"""
A tela "Só a linha da planilha" — para a nota que saiu certa e não entrou na
"Notas BWS".

Por que isto existe: em 07/10/2026 o dono tinha uma nota emitida corretamente em
tudo (Omie, card, arquivos, cliente) cuja linha da planilha não havia entrado.
As duas telas de conserto que existiam — "Recuperar entrega" e "Nota emitida no
portal" — rodam a conclusão INTEIRA: preencheriam um segundo slot no card,
mexeriam no Omie de novo e mandariam o aviso ao cliente outra vez. Resolver a
planilha criaria três problemas.

O que estes testes garantem, e é o essencial: esta tela grava a linha e **não
chama nada mais**, e os valores da linha saem do **XML**, não do card — porque a
conclusão limpa doze campos de entrada do card ao terminar, então recalcular a
nota depois daria números diferentes dos emitidos.
"""
import os
import sys
from decimal import Decimal

import pytest

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

from app.main import app as app_real          # noqa: E402
import notas_bws                              # noqa: E402

TOKEN = "TOKEN-DE-TESTE"
CARD = "1447316614"


# --------------------------------------------------------------------------- #
# As duas notas de amostra, na forma em que cada modelo realmente chega
# --------------------------------------------------------------------------- #
def xml_nacional(numero="3084", total="98720.04", liquido="86513.86",
                 iss="4936.00", inss="7245.00", irrf="1184.64",
                 pis="641.68", cofins="2961.60", com_piscofins=True):
    """NFS-e nacional como a prefeitura devolve: os totais em `infNFSe/valores`
    e a declaração inteira embutida — os federais retidos moram só lá dentro."""
    pc = (f"<piscofins><vPis>{pis}</vPis><vCofins>{cofins}</vCofins></piscofins>"
          if com_piscofins else "")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<NFSe xmlns="http://www.sped.fazenda.gov.br/nfse" versao="1.01">
  <infNFSe Id="NFS{'2' * 50}">
    <nNFSe>{numero}</nNFSe>
    <dhProc>2026-10-06T14:22:31-03:00</dhProc>
    <valores>
      <vBC>54844.47</vBC>
      <vISSQN>{iss}</vISSQN>
      <vLiq>{liquido}</vLiq>
    </valores>
    <DPS versao="1.01">
      <infDPS>
        <valores>
          <vServPrest><vServ>{total}</vServ></vServPrest>
          <trib>
            <tribFed>
              {pc}
              <vRetCP>{inss}</vRetCP>
              <vRetIRRF>{irrf}</vRetIRRF>
              <vRetCSLL>1066.18</vRetCSLL>
            </tribFed>
          </trib>
        </valores>
      </infDPS>
    </DPS>
  </infNFSe>
</NFSe>"""


def xml_abrasf(numero="3067", total="50000.00", liquido="43000.00"):
    """O modelo antigo. Continua aceito porque as notas no Drive estão nele."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<CompNfse xmlns="http://www.abrasf.org.br/nfse.xsd">
  <Nfse><InfNfse>
    <Numero>{numero}</Numero>
    <CodigoVerificacao>ABC123</CodigoVerificacao>
    <DataEmissao>2026-09-12T09:10:00</DataEmissao>
    <ValoresNfse>
      <ValorServicos>{total}</ValorServicos>
      <ValorLiquidoNfse>{liquido}</ValorLiquidoNfse>
      <ValorInss>3000.00</ValorInss>
      <ValorIss>2000.00</ValorIss>
      <ValorIr>600.00</ValorIr>
      <ValorPis>325.00</ValorPis>
      <ValorCofins>1500.00</ValorCofins>
    </ValoresNfse>
  </InfNfse></Nfse>
</CompNfse>"""


class AbaFalsa:
    """A 'Notas BWS' sem rede: só a coluna F (Nº Nota) e as linhas acrescentadas."""

    def __init__(self, numeros_ja_existentes=()):
        self._f = ["Nº Nota"] + [str(n) for n in numeros_ja_existentes]
        self.linhas = []

    def col_values(self, n):
        assert n == 6, "a trava de duplicidade lê a coluna F"
        return self._f

    def append_row(self, linha, **kw):
        self.linhas.append(linha)


def _card(valor_medicao="1.00"):
    """O card DEPOIS da conclusão: os doze campos de entrada já foram limpos.

    O valor aqui é absurdo de propósito — se algum número da linha vier do card
    em vez do XML, o teste quebra em cima dele.
    """
    return {"card_id": CARD, "codigo_obra": "CREPEEXU", "numero_medicao": "10",
            "valor_medicao": valor_medicao, "bdi": "", "tipo_medicao": "",
            "tipo_documento": "Solicitação de Pagamento de Medição",
            "campos_raw": [], "campos_por_id": {}, "omie_integracao": ""}


@pytest.fixture
def cenario(monkeypatch):
    """A tela ligada num mundo sem Google, sem Pipefy e sem pós-emissão."""
    import web as _flat                         # noqa: F401  (garante o sys.path)
    from app.apps.emissaonf import web          # é ESTE módulo que atende

    aba = AbaFalsa()
    chamadas = {"concluir": [], "omie": [], "pipefy_escrita": []}

    class GoogleFalso:
        def open_by_key(self, _k):
            return object()

    monkeypatch.setattr(web._worker, "cliente_gspread", lambda: GoogleFalso())
    monkeypatch.setattr(web._worker, "ler_credenciais", lambda gc: {"PIPEFY_TOKEN": "x"})
    monkeypatch.setattr(web._worker, "abrir_aba", lambda planilha, candidatos: aba)
    monkeypatch.setattr(web._pipefy, "get_card", lambda card_id, token: {"id": card_id})
    monkeypatch.setattr(web._pipefy, "extrair_card", lambda bruto: _card())

    # as redes que esta tela NÃO pode encostar
    monkeypatch.setattr(web._concluir, "concluir",
                        lambda *a, **k: chamadas["concluir"].append(a))
    monkeypatch.setattr(web.omie, "alterar_retencoes",
                        lambda *a, **k: chamadas["omie"].append(a))

    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    return app_real.test_client(), aba, chamadas


def _gravar(cliente, xml, card_id=CARD):
    return cliente.post("/emissao/planilha",
                        data={"card_id": card_id, "token": TOKEN, "xml": xml},
                        follow_redirects=True)


def _texto(r):
    return r.get_data(as_text=True)


# --------------------------------------------------------------------------- #
# A porta e o caminho até ela
# --------------------------------------------------------------------------- #
def test_sem_o_token_do_link_a_tela_nao_abre(cenario):
    cliente, _aba, _ch = cenario
    assert cliente.get("/emissao/planilha").status_code == 403


def test_a_tela_de_emissao_tem_link_para_ela_com_o_token_dentro(cenario):
    cliente, _aba, _ch = cenario
    corpo = _texto(cliente.get(f"/emissao/?token={TOKEN}", follow_redirects=True))
    assert f"/emissao/planilha?token={TOKEN}" in corpo


def test_a_tela_avisa_que_nao_mexe_em_mais_nada(cenario):
    """O risco desta tela é a pessoa usá-la achando que faz o serviço completo —
    ou usar a completa achando que só mexe na planilha. A tela diz as duas."""
    cliente, _aba, _ch = cenario
    corpo = _texto(cliente.get(f"/emissao/planilha?token={TOKEN}"))
    assert "não mexe em mais nada" in corpo
    assert "Recuperar entrega" in corpo         # a tela certa quando faltou mais
    assert "Nota emitida no portal" in corpo


# --------------------------------------------------------------------------- #
# O que ela faz: a linha, e só
# --------------------------------------------------------------------------- #
def test_grava_a_linha_com_os_valores_do_xml_e_nao_do_card(cenario):
    cliente, aba, _ch = cenario
    _gravar(cliente, xml_nacional())
    assert len(aba.linhas) == 1
    linha = aba.linhas[0]
    assert linha[0] == "CREPEEXU"               # A Código Obra
    assert linha[5] == "3084"                   # F Nº Nota
    assert linha[6] == "06/10/2026"             # G Data Emissão (do dhProc)
    assert linha[7] == "98.720,04"              # H Valor da Nota (vServ)
    assert linha[9] == "10"                     # J Nº Med. (sobrevive à limpeza)
    assert linha[14] == "80.684,94"             # O líquido, CALCULADO (ver abaixo)
    assert "1,00" not in linha[7]               # o valor do card NÃO entrou


def test_nao_chama_o_pos_emissao_nem_o_omie(cenario):
    """É a razão de a tela existir: a conclusão inteira preencheria um segundo
    slot no card, mexeria no Omie e mandaria o WhatsApp de novo."""
    cliente, _aba, chamadas = cenario
    _gravar(cliente, xml_nacional())
    assert chamadas["concluir"] == []
    assert chamadas["omie"] == []


def test_a_pagina_de_resultado_nao_diz_que_emitiu_nota(cenario):
    cliente, _aba, _ch = cenario
    corpo = _texto(_gravar(cliente, xml_nacional()))
    assert "NFS-e emitida" not in corpo
    assert "Nada além da planilha foi tocado" in corpo
    assert "Linha da nota 3084 gravada" in corpo


def test_numero_que_ja_esta_na_planilha_nao_duplica(cenario, monkeypatch):
    cliente, _aba, _ch = cenario
    from app.apps.emissaonf import web
    cheia = AbaFalsa(numeros_ja_existentes=["3084"])
    monkeypatch.setattr(web._worker, "abrir_aba", lambda planilha, cand: cheia)
    corpo = _texto(_gravar(cliente, xml_nacional()))
    assert cheia.linhas == []
    assert "JÁ estava na" in corpo and "não dupliquei nada" in corpo


def test_o_modelo_antigo_tambem_e_aceito(cenario):
    """Quem precisa consertar uma linha não tem como saber em que modelo a nota
    saiu — e há notas dos dois no Drive."""
    cliente, aba, _ch = cenario
    _gravar(cliente, xml_abrasf())
    assert len(aba.linhas) == 1
    assert aba.linhas[0][5] == "3067"
    assert aba.linhas[0][6] == "12/09/2026"
    assert aba.linhas[0][7] == "50.000,00"


# --------------------------------------------------------------------------- #
# O que ela recusa, e dizendo por quê
# --------------------------------------------------------------------------- #
def test_sem_o_xml_ela_pede_o_xml_e_explica(cenario):
    cliente, aba, _ch = cenario
    corpo = _texto(_gravar(cliente, ""))
    assert aba.linhas == []
    assert "Os valores saem do XML" in corpo


def test_sem_o_card_ela_pede_o_card(cenario):
    cliente, aba, _ch = cenario
    corpo = _texto(_gravar(cliente, xml_nacional(), card_id=""))
    assert aba.linhas == []
    assert "número do card" in corpo


def test_xml_quebrado_nao_estoura_a_tela(cenario):
    cliente, aba, _ch = cenario
    corpo = _texto(_gravar(cliente, "<isso nao é xml"))
    assert aba.linhas == []
    assert "não pôde ser lido" in corpo


def test_xml_da_declaracao_em_vez_da_nota_e_recusado(cenario):
    """O XML da DPS não tem número de nota. Gravar linha sem número encheria a
    planilha de lixo silencioso."""
    cliente, aba, _ch = cenario
    dps = ('<DPS xmlns="http://www.sped.fazenda.gov.br/nfse"><infDPS>'
           '<valores><vServPrest><vServ>100.00</vServ></vServPrest></valores>'
           '</infDPS></DPS>')
    corpo = _texto(_gravar(cliente, dps))
    assert aba.linhas == []
    assert "XML da NOTA" in corpo


# --------------------------------------------------------------------------- #
# A leitura dos valores, direto
# --------------------------------------------------------------------------- #
def test_os_valores_do_nacional_saem_dos_lugares_certos():
    v = notas_bws.valores_do_xml(xml_nacional())
    assert v.valor_total == Decimal("98720.04")   # vServ, dentro da DPS embutida
    assert v.iss == Decimal("4936.00")             # vISSQN
    assert v.inss == Decimal("7245.00")            # vRetCP
    assert v.ir == Decimal("1184.64")              # vRetIRRF
    assert v.pis == Decimal("641.68")              # vPis, dentro de piscofins
    assert v.cofins == Decimal("2961.60")          # vCofins


def test_federal_sem_retencao_entra_pela_aliquota_cheia():
    """A coluna P desconta os federais cheios, retidos ou não — é como a planilha
    sempre foi. O XML só traz os retidos, então o resto vem da alíquota padrão."""
    v = notas_bws.valores_do_xml(xml_nacional(com_piscofins=False))
    assert v.pis == Decimal("641.68")      # 0,65% de 98.720,04
    assert v.cofins == Decimal("2961.60")  # 3% de 98.720,04


def test_os_valores_do_modelo_antigo_saem_dos_lugares_certos():
    v = notas_bws.valores_do_xml(xml_abrasf())
    assert v.valor_total == Decimal("50000.00")
    assert v.valor_liquido == Decimal("42575.00")   # calculado, não o ValorLiquidoNfse
    assert v.iss == Decimal("2000.00")
    assert v.inss == Decimal("3000.00")
    assert v.ir == Decimal("600.00")
    assert v.pis == Decimal("325.00")
    assert v.cofins == Decimal("1500.00")


def test_a_linha_montada_do_xml_tem_as_mesmas_dezesseis_colunas():
    """A planilha tem fórmulas de Q em diante; linha com tamanho diferente
    desalinharia tudo."""
    v = notas_bws.valores_do_xml(xml_nacional())
    linha = notas_bws.montar_linha(_card(), None, v, "3084", "2026-10-06")
    assert len(linha) == 16


# --------------------------------------------------------------------------- #
# O líquido é CALCULADO, e não lido do `vLiq` — a nota 3283 provou por quê
#
# A primeira nota do padrão nacional (08/10/2026, obra IFSPSAOJOSE, medição 11)
# voltou com `vLiq = 24.222,04`. O que a BWS recebe de fato é **23.303,97**: o
# `vLiq` do modelo nacional NÃO desconta PIS nem COFINS, e nessa nota os dois
# foram retidos (163,49 e 754,58).
#
# A coluna O da planilha é "valor a ser recebido", ou seja valor menos TODAS as
# retenções. Ler o `vLiq` colocaria R$ 918,07 a mais nela — num campo que o dono
# usa para conferir recebimento.
# --------------------------------------------------------------------------- #
XML_NOTA_3283 = """<?xml version="1.0" encoding="UTF-8"?>
<NFSe xmlns="http://www.sped.fazenda.gov.br/nfse" versao="1.01">
  <infNFSe Id="NFS23042851200079526000109260000000328326100010793653">
    <nNFSe>2600000003283</nNFSe>
    <dhProc>2026-10-08T12:22:26-03:00</dhProc>
    <nDFSe>2600000003283</nDFSe>
    <valores>
      <vCalcDR>12576.34</vCalcDR><vBC>12576.35</vBC>
      <pAliqAplic>3.00</pAliqAplic>
      <vISSQN>377.29</vISSQN>
      <vTotalRet>930.65</vTotalRet>
      <vLiq>24222.04</vLiq>
    </valores>
    <DPS versao="1.01"><infDPS>
      <valores>
        <vServPrest><vServ>25152.69</vServ></vServPrest>
        <vDedRed><vDR>12576.34</vDR></vDedRed>
        <trib>
          <tribMun><tribISSQN>1</tribISSQN><tpRetISSQN>2</tpRetISSQN><pAliq>3.00</pAliq></tribMun>
          <tribFed>
            <piscofins><CST>01</CST><vBCPisCofins>25152.69</vBCPisCofins>
              <pAliqPis>0.65</pAliqPis><pAliqCofins>3.00</pAliqCofins>
              <vPis>163.49</vPis><vCofins>754.58</vCofins>
              <tpRetPisCofins>3</tpRetPisCofins></piscofins>
            <vRetIRRF>301.83</vRetIRRF>
            <vRetCSLL>251.53</vRetCSLL>
          </tribFed>
          <totTrib><indTotTrib>0</indTotTrib></totTrib>
        </trib>
      </valores>
    </infDPS></DPS>
  </infNFSe>
</NFSe>"""


def test_o_liquido_da_nota_3283_e_o_que_a_bws_recebe_e_nao_o_vLiq():
    v = notas_bws.valores_do_xml(XML_NOTA_3283)
    assert v.valor_total == Decimal("25152.69")
    assert v.valor_liquido == Decimal("23303.97"), (
        "o vLiq da nota diz 24.222,04 porque não desconta PIS nem COFINS")
    assert v.iss == Decimal("377.29")
    assert v.inss == Decimal("0.00")      # esta obra não retém INSS (foi o E0699)
    assert v.ir == Decimal("301.83")
    assert v.pis == Decimal("163.49")
    assert v.cofins == Decimal("754.58")


def test_a_nota_3283_tambem_confirma_que_o_INSS_nao_vai_quando_nao_ha():
    """O XML oficial da nota não tem vRetCP nenhum — é a prova de que o conserto
    do E0699 passou pela plataforma."""
    assert "vRetCP" not in XML_NOTA_3283
