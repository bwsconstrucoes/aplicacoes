# -*- coding: utf-8 -*-
"""
O que acontece DEPOIS de emitir, conferido sem emitir.

A prefeitura passou a devolver a nota no formato nacional (07/10/2026). Tudo o
que vem depois — o PDF da nota no layout da prefeitura, a DANFSe, o valor que
vai para o recibo — é desenhado a partir desse XML. Antes, era desenhado a
partir do XML do modelo antigo.

Então o risco desta migração não está só em emitir: está em emitir e os
documentos saírem errados, com a nota já criada e sem volta. Estes testes
montam a resposta que a prefeitura daria (`nfse_exemplo.py`), conferem que ela
é válida pelo schema oficial, e passam por todos os geradores.
"""
import os
import sys

import pytest
from lxml import etree

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

import el_nfse_nacional as nac          # noqa: E402
import nfse_exemplo                      # noqa: E402
import nota_municipal                    # noqa: E402
import emitir_dps                        # noqa: E402

from test_emissaonf_dps import _montar    # noqa: E402  (reusa o montador de nota realista)

XSD_NFSE = os.path.join(_EMISSAONF, "xsd_nacional", "NFSe_v1.01.xsd")


@pytest.fixture(scope="module")
def nota_xml():
    return nfse_exemplo.como_texto(nac.montar_dps_xml(_montar()), numero_nfse="3084")


def test_a_resposta_de_exemplo_e_uma_nfse_valida_pelo_schema_oficial(nota_xml):
    """Se a amostra não fosse válida, todos os testes abaixo estariam provando
    que o sistema lida bem com um XML que a prefeitura nunca mandaria."""
    schema = etree.XMLSchema(etree.parse(XSD_NFSE))
    doc = etree.fromstring(nota_xml.encode("utf-8"))
    assert schema.validate(doc), schema.error_log


# --------------------------------------------------------------------------- #
# O que a emissão tira da resposta
# --------------------------------------------------------------------------- #
def test_a_emissao_tira_numero_chave_e_data_da_resposta(nota_xml):
    d = emitir_dps.dados_da_nota(nota_xml)
    assert d["numero"] == "3084"
    assert d["data_iso"] == "2026-10-07"
    assert len(d["chave"]) == 50 and not d["chave"].startswith("NFS")


def test_resposta_sem_numero_nao_passa_por_nota_emitida(nota_xml):
    """Nota sem número não dá para registrar em lugar nenhum — melhor falhar
    alto do que gravar uma linha vazia na planilha."""
    sem_numero = nota_xml.replace("<nNFSe>3084</nNFSe>", "<nNFSe></nNFSe>")
    with pytest.raises(emitir_dps.NotaNaoSaiu):
        emitir_dps.dados_da_nota(sem_numero)


def test_resposta_que_nao_e_nfse_e_recusada():
    with pytest.raises(emitir_dps.NotaNaoSaiu):
        emitir_dps.dados_da_nota("<outraCoisa><a>1</a></outraCoisa>")


# --------------------------------------------------------------------------- #
# O PDF da nota no layout da prefeitura
# --------------------------------------------------------------------------- #
def test_o_pdf_municipal_sai_do_xml_nacional(nota_xml, tmp_path):
    """O cliente recebe este PDF. Ele era desenhado a partir do XML antigo, que
    não existe mais — então ou ele sai do nacional, ou o cliente deixa de
    receber o documento a que está acostumado."""
    saida = str(tmp_path / "municipal.pdf")
    nota_municipal.gerar_nota_municipal_pdf(None, saida, xml_nacional=nota_xml)
    assert os.path.getsize(saida) > 10000


def test_a_leitura_do_nacional_traz_os_campos_que_o_pdf_mostra(nota_xml):
    d = nota_municipal.parse_do_nacional(nota_xml)
    assert d["numero"] == "3084"
    assert d["emit_cnpj"].replace(".", "").replace("/", "").replace("-", "") == "00079526000109"
    assert d["toma_razao"] == "SECRETARIA DE EDUCACAO"
    assert d["iss_retido"] == "Retido na Fonte"
    assert d["discriminacao"].startswith("PAGAMENTO DA 10a MEDICAO")
    assert d["vServ"] == "98.720,04"


def test_a_deducao_de_material_aparece_no_pdf(nota_xml):
    """Numa nota 80/20 o PDF tem de mostrar a dedução — é ela que explica por
    que o ISS não foi calculado sobre o valor cheio."""
    d = nota_municipal.parse_do_nacional(nota_xml)
    assert d["vDed"] not in ("", "0,00")


def test_o_codigo_de_verificacao_vai_vazio_e_nao_inventado(nota_xml):
    """O modelo nacional não tem código de verificação: quem identifica a nota é
    a chave de acesso. Preencher aquele campo com qualquer coisa seria pior."""
    d = nota_municipal.parse_do_nacional(nota_xml)
    assert d["cod_verif"] == ""


def test_a_nota_sem_declaracao_dentro_e_recusada_com_motivo(nota_xml):
    import re
    sem_dps = re.sub(r"<DPS[^>]*>.*</DPS>", "", nota_xml, flags=re.S)
    with pytest.raises(ValueError) as erro:
        nota_municipal.parse_do_nacional(sem_dps)
    assert "declaração" in str(erro.value)


# --------------------------------------------------------------------------- #
# A DANFSe nacional
# --------------------------------------------------------------------------- #
def test_a_danfse_nacional_sai_da_mesma_resposta(nota_xml, tmp_path):
    """Ela já saía do XML nacional antes — mas antes chegava minutos depois, por
    um job. Agora sai na hora, da resposta da emissão. Vale provar que a
    resposta serve para ela também."""
    import danfse
    saida = str(tmp_path / "danfse.pdf")
    danfse.gerar_danfse_pdf(nota_xml, saida)
    assert os.path.getsize(saida) > 10000


# --------------------------------------------------------------------------- #
# O valor que vai para o recibo
# --------------------------------------------------------------------------- #
def test_o_valor_bruto_do_recibo_vem_do_xml_e_nao_do_card(nota_xml):
    """O card já foi limpo quando o recibo é gerado; o valor tem de sair do XML."""
    assert nota_municipal.valor_bruto_nf_nacional(nota_xml) == 98720.04


# --------------------------------------------------------------------------- #
# A tela de recuperação — a ferramenta de quando algo deu errado
# --------------------------------------------------------------------------- #
def test_a_recuperacao_reconhece_o_xml_nacional(nota_xml):
    """Esta tela é usada justamente quando a emissão falhou no meio. Ela não pode
    exigir que a pessoa saiba em que modelo a nota foi emitida."""
    import web
    numero, codigo, data, nacional, chave = web._ids_da_nota(nota_xml)
    assert (numero, nacional) == ("3084", True)
    assert data == "2026-10-07"
    assert len(chave) == 50
    assert codigo == ""          # não existe no nacional


def test_a_recuperacao_continua_reconhecendo_o_xml_antigo():
    """As notas de antes de 07/10/2026 estão no Drive no modelo antigo, e ainda
    precisam poder ser recuperadas."""
    import web
    antigo = """<?xml version="1.0"?><CompNfse><Nfse><InfNfse>
        <Numero>3070</Numero><CodigoVerificacao>ABC123</CodigoVerificacao>
        <DataEmissao>2026-09-15T09:00:00</DataEmissao></InfNfse></Nfse></CompNfse>"""
    numero, codigo, data, nacional, chave = web._ids_da_nota(antigo)
    assert (numero, codigo, data) == ("3070", "ABC123", "2026-09-15")
    assert nacional is False and chave == ""


def test_xml_que_nao_e_nota_nenhuma_devolve_vazio():
    import web
    assert web._ids_da_nota("<raiz><x>1</x></raiz>")[0] == ""


# --------------------------------------------------------------------------- #
# O PDF em branco que apagaria o documento bom
# --------------------------------------------------------------------------- #
# Os arquivos do Drive sobem com o MESMO nome, de propósito: mesmo link, nada
# duplicado. O outro lado disso é que um PDF em branco não fica ao lado do bom —
# ele toma o lugar dele. Quem regera PDF de nota antiga lê o XML do Drive sem
# saber de qual modelo ele é, e o leitor do modelo antigo, recebendo um XML
# nacional, não dá erro: dá um PDF vazio.

def test_o_xml_nacional_passado_no_lugar_do_antigo_e_reconhecido(nota_xml, tmp_path):
    """É o caso de regerar o PDF de uma nota emitida depois de 07/10/2026: o que
    está arquivado no Drive é o nacional, mas quem chama pede como se fosse o
    antigo."""
    saida = str(tmp_path / "municipal.pdf")
    nota_municipal.gerar_nota_municipal_pdf(nota_xml, saida)     # como se fosse ABRASF
    assert os.path.getsize(saida) > 10000
    d = nota_municipal.parse_do_nacional(nota_xml)
    assert d["numero"] == "3084"


def test_reconhece_os_dois_modelos_pelo_conteudo(nota_xml):
    assert nota_municipal.eh_xml_nacional(nota_xml) is True
    assert nota_municipal.eh_xml_nacional("<CompNfse><Nfse><InfNfse/></Nfse></CompNfse>") is False
    assert nota_municipal.eh_xml_nacional("") is False
    assert nota_municipal.eh_xml_nacional(None) is False


def test_xml_que_nao_da_para_ler_nao_gera_pdf_nenhum(tmp_path):
    """A rede de segurança: sem número, a leitura não entendeu o XML. Gerar o PDF
    aqui substituiria no Drive o documento que o cliente recebeu por um em
    branco — melhor falhar alto."""
    saida = str(tmp_path / "municipal.pdf")
    quase = """<?xml version="1.0"?><CompNfse><Nfse><InfNfse>
        <Numero></Numero></InfNfse></Nfse></CompNfse>"""
    with pytest.raises(ValueError) as erro:
        nota_municipal.gerar_nota_municipal_pdf(quase, saida)
    assert "sobrescreveria" in str(erro.value)
    assert not os.path.exists(saida)


def test_o_valor_do_recibo_decide_pelo_conteudo_do_xml(nota_xml):
    """Mesmo risco do PDF em branco, em outro lugar: recebendo o XML nacional, a
    busca pelo campo do modelo antigo não dava erro — devolvia nada, e o recibo
    sairia sem valor."""
    antigo = """<?xml version="1.0"?><CompNfse><Nfse><InfNfse><Numero>3070</Numero>
        <DeclaracaoPrestacaoServico><InfDeclaracaoPrestacaoServico><Servico><Valores>
        <ValorServicos>12345.67</ValorServicos></Valores></Servico>
        </InfDeclaracaoPrestacaoServico></DeclaracaoPrestacaoServico></InfNfse></Nfse></CompNfse>"""
    assert nota_municipal.valor_bruto_nf(nota_xml) == 98720.04      # nacional
    assert nota_municipal.valor_bruto_nf(antigo) == 12345.67        # modelo antigo
    # o nome antigo continua valendo para quem já chamava assim
    assert nota_municipal.valor_bruto_nf_nacional(nota_xml) == 98720.04
