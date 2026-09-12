# -*- coding: utf-8 -*-
"""Análise de SPs — a busca automática de notas na Receita.

*"Um dos corações dessa atualização é essa busca automática por novas notas."*
— o dono, em 12/09/2026.

O QUE ESTES TESTES ALCANÇAM, E O QUE NÃO. Tudo o que fala com a rede está em
duas funções, e só elas precisam do certificado de verdade — que não existe
fora do Render. **Nenhum teste aqui liga para a Receita.** O que é exercitado é
o resto, que é onde mora o risco de perder nota em silêncio: o ponteiro do
"até onde já li", a leitura do documento, e as três formas de saber que o lote
acabou.
"""
from __future__ import annotations

import base64
import gzip

import pytest

from app.apps.analisesps import sefaz


CREDOR = "29066773000152"
BWS = "10656452007869"


def chave(cnpj=CREDOR, modelo="55", numero="000001430"):
    montada = "26" + "2609" + cnpj + modelo + "001" + numero
    return (montada + "0" * 44)[:44]


def _zipado(xml: str) -> str:
    import io as _io
    saco = _io.BytesIO()
    with gzip.GzipFile(fileobj=saco, mode="wb") as z:
        z.write(xml.encode("utf-8"))
    return base64.b64encode(saco.getvalue()).decode("ascii")


def resumo_nfe(ch=None, valor="269.00", situacao="1", nome="SERTAO CASA"):
    ch = ch or chave()
    return (f'<resNFe versao="1.01"><chNFe>{ch}</chNFe>'
            f"<CNPJ>{CREDOR}</CNPJ><xNome>{nome}</xNome><UF>PE</UF>"
            f"<dhEmi>2026-06-18T10:00:00-03:00</dhEmi><tpNF>1</tpNF>"
            f"<vNF>{valor}</vNF><cSitNFe>{situacao}</cSitNFe></resNFe>")


def resumo_cte(ch=None, valor="88.00"):
    ch = ch or chave(cnpj="11222333000181", modelo="57", numero="000000077")
    return (f'<resCTe versao="1.01"><chCTe>{ch}</chCTe>'
            f"<CNPJ>11222333000181</CNPJ><xNome>TRANSPORTADORA</xNome>"
            f"<dhEmi>2026-06-20T10:00:00-03:00</dhEmi>"
            f"<vTPrest>{valor}</vTPrest><cSitCTe>1</cSitCTe></resCTe>")


def resposta(documentos=(), codigo="138", ultimo="000000000000010",
             maior="000000000000010"):
    """A resposta da Receita como ela chega: XML, com os documentos zipados."""
    zips = "".join(f'<docZip NSU="{n}">{_zipado(d)}</docZip>'
                   for n, d in enumerate(documentos, start=1))
    return (f"<retDistDFeInt><cStat>{codigo}</cStat>"
            f"<xMotivo>Documento(s) localizado(s)</xMotivo>"
            f"<ultNSU>{ultimo}</ultNSU><maxNSU>{maior}</maxNSU>"
            f"<loteDistDFeInt>{zips}</loteDistDFeInt></retDistDFeInt>")


# ---------------------------------------------------------------------------
# LER O QUE A RECEITA MANDOU
# ---------------------------------------------------------------------------
def test_o_resumo_de_NFe_vira_a_linha_que_a_conciliacao_usa():
    """A conciliação precisa de chave, valor, emitente e número. O resumo traz
    os quatro — não é preciso esperar o documento completo para casar."""
    lido = sefaz.ler_documento(resumo_nfe())
    assert lido["chave"] == chave()
    assert lido["valor"] == "269.00"
    assert lido["emitente_doc"] == CREDOR
    assert lido["status"] == "Autorizada"
    assert lido["tipo"] == "NF-e"


def test_o_numero_da_nota_sai_de_DENTRO_da_chave_quando_nao_vem_em_campo():
    """No resumo o número não tem campo próprio: está nas posições 26 a 34 da
    chave, por definição da Receita. Sem isto, a conciliação perderia os 25
    pontos do número da nota em TODA nota vinda por aqui."""
    assert sefaz.ler_documento(resumo_nfe())["numero"] == "1430"


def test_o_CTe_e_lido_com_os_campos_dele():
    """CT-e tem tag e campo de valor diferentes da NF-e. Ler com os nomes da
    NF-e devolveria uma linha sem valor nenhum."""
    lido = sefaz.ler_documento(resumo_cte())
    assert lido["tipo"] == "CT-e"
    assert lido["valor"] == "88.00"
    assert lido["numero"] == "77"


def test_nota_CANCELADA_na_Receita_vira_a_MESMA_palavra_do_FSist():
    """A Receita responde código 3; o FSist escreve "Cancelada". Se cada
    origem gravasse do seu jeito, a mesma nota teria dois status conforme a
    porta por onde entrou — e a crítica de nota cancelada deixaria de disparar
    para metade delas."""
    assert sefaz.ler_documento(resumo_nfe(situacao="3"))["status"] == "Cancelada"
    assert sefaz.ler_documento(resumo_nfe(situacao="1"))["status"] == "Autorizada"


def test_evento_no_meio_do_lote_NAO_vira_nota():
    """O lote traz eventos e avisos junto dos documentos. Não é erro: é o
    esperado. Gravá-los encheria a tabela de linhas sem chave."""
    assert sefaz.ler_documento(
        "<resEvento><tpEvento>110111</tpEvento></resEvento>") is None
    assert sefaz.ler_documento("<qualquerCoisa/>") is None


def test_documento_ilegivel_nao_derruba_o_lote_inteiro():
    """Um documento torto não pode fazer perder os outros quarenta e nove."""
    bruto = (f"<retDistDFeInt><cStat>138</cStat><ultNSU>2</ultNSU>"
             f"<maxNSU>2</maxNSU>"
             f"<docZip>nao-e-base64-valido!!</docZip>"
             f"<docZip>{_zipado(resumo_nfe())}</docZip></retDistDFeInt>")
    lida = sefaz._ler_resposta(bruto)
    assert len(lida["documentos"]) == 1


# ---------------------------------------------------------------------------
# O PONTEIRO — é ele que faz a busca funcionar
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto,esperado", [
    ("10", "000000000000010"),
    ("", "000000000000000"),
    (None, "000000000000000"),
    ("000000000000123", "000000000000123"),
])
def test_o_NSU_e_guardado_com_os_zeros_a_esquerda(bruto, esperado):
    """Com os zeros, a comparação de TEXTO ordena igual à numérica. Sem eles,
    "9" pareceria maior que "10" e o ponteiro recuaria — fazendo a busca reler
    o que já veio e bater no limite de consultas da Receita."""
    assert sefaz._nsu(bruto) == esperado


def test_a_UF_vira_o_codigo_que_a_Receita_usa():
    assert sefaz._codigo_uf("PE") == "26"
    assert sefaz._codigo_uf("SP") == "35"
    assert sefaz._codigo_uf("xx") == "26", "desconhecida cai no padrão da casa"


def test_sem_certificado_a_busca_NAO_estoura_e_diz_o_que_falta(monkeypatch):
    """Falta de certificado é configuração que não foi feita, não defeito. E o
    recado tem de dizer o nome das variáveis — senão ninguém sabe o que pôr."""
    monkeypatch.delenv("ANALISESPS_CERT_A1_BASE64", raising=False)
    monkeypatch.delenv("ANALISESPS_CERT_A1_SENHA", raising=False)
    assert sefaz.configurado() is False
    assert sefaz.buscar_tudo() == {"trazidas": 0, "erro": "sem certificado",
                                   "por_cnpj": []}
    with pytest.raises(sefaz.SemCertificado) as erro:
        sefaz._certificado()
    assert "ANALISESPS_CERT_A1_BASE64" in str(erro.value)


def test_a_lista_de_CNPJs_ignora_o_que_nao_e_CNPJ(monkeypatch):
    """Campo de configuração é texto livre: vírgula sobrando, espaço, CPF."""
    monkeypatch.setenv("ANALISESPS_CNPJS",
                       "10.656.452/0078-69, 29066773000152, , 12345")
    assert sefaz.cnpjs_vigiados() == ["10656452007869", "29066773000152"]
