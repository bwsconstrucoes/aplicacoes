# -*- coding: utf-8 -*-
"""
Emissor NFS-e ABRASF 2.04 (SOAP) para o municipio de Eusebio/CE - provedor E&L.
Canal de PRODUCAO ativo: GerarNfse no NfseWSService.

Fluxo:
    1) monta o XML do RPS (InfDeclaracaoPrestacaoServico) no padrao ABRASF
    2) assina o InfDeclaracaoPrestacaoServico (XMLDSig RSA-SHA1)
    3) embrulha no envelope SOAP (nfseCabecMsg + nfseDadosMsg)
    4) (opcional) envia ao webservice e le o retorno

ATENCAO: este canal e PRODUCAO. Cada GerarNfse gera nota fiscal REAL.
Por isso o modo padrao e DRY-RUN: monta, assina e mostra o XML, SEM enviar.
So envie depois de conferir os campos contra uma nota que voce sabe que esta certa.

Dependencias:  pip install requests signxml cryptography lxml
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import requests
from lxml import etree
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from signxml import XMLSigner, methods

# --------------------------------------------------------------------------- #
WSDL_EUSEBIO = "https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40/NfseWSService"
NS_ABRASF = "http://www.abrasf.org.br/nfse.xsd"
NS_DSIG = "http://www.w3.org/2000/09/xmldsig#"
COD_IBGE_EUSEBIO = "2304285"


class _Assinador(XMLSigner):
    def check_deprecated_methods(self):  # libera RSA-SHA1 exigido pelo ABRASF
        pass


def _carregar_p12_bytes(raw: bytes, senha: str) -> tuple[bytes, bytes]:
    chave, cert, _ = pkcs12.load_key_and_certificates(raw, senha.encode("utf-8"))
    if chave is None or cert is None:
        raise ValueError("Nao foi possivel extrair chave/certificado do .p12 (senha incorreta?).")
    chave_pem = chave.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return chave_pem, cert.public_bytes(serialization.Encoding.PEM)


def carregar_certificado_a1(caminho: str, senha: str) -> tuple[bytes, bytes]:
    with open(caminho, "rb") as fh:
        return _carregar_p12_bytes(fh.read(), senha)


def carregar_certificado_auto(senha: str, caminho: str = "certificado.p12"):
    """Resolve o A1 de UMA destas fontes (nesta ordem) e devolve (chave_pem, cert_pem):
      1) env CERTIFICADO_P12_BASE64 (.p12 em base64) — usado no Render;
      2) arquivo local (caminho) — usado localmente.
    Se nenhuma existir, devolve (None, None)."""
    import os
    from base64 import b64decode
    senha = senha or (os.getenv("EMISSAO_NF_CERTIFICADO_SENHA") or os.getenv("CERTIFICADO_SENHA")
                      or os.getenv("SENHA_CERTIFICADO") or os.getenv("CERT_SENHA") or "")
    b64 = (os.getenv("EMISSAO_NF_CERTIFICADO_P12_BASE64")
           or os.getenv("CERTIFICADO_P12_BASE64") or "")
    if b64:
        return _carregar_p12_bytes(b64decode(b64), senha)
    if os.path.exists(caminho):
        return carregar_certificado_a1(caminho, senha)
    return None, None


# --------------------------------------------------------------------------- #
@dataclass
class DadosRps:
    # identificacao do RPS
    numero_rps: int
    serie_rps: str = "1"
    tipo_rps: int = 1                 # 1 = RPS
    data_emissao: str = ""            # "AAAA-MM-DD"
    competencia: str = ""             # "AAAA-MM-DD"
    status: int = 1                   # 1 = Normal

    # substituição (opcional): identifica o RPS da nota antiga que esta nota substitui.
    # Quando preenchido, o município marca a nota antiga como substituída na hora da emissão.
    rps_substituido_numero: int | str | None = None
    rps_substituido_serie: str = ""   # vazio => usa a mesma série desta nota
    rps_substituido_tipo: int = 0     # 0 => usa o mesmo tipo desta nota

    # valores
    valor_servicos: str = "0.00"
    valor_deducoes: str = "0.00"
    valor_pis: str = "0.00"
    valor_cofins: str = "0.00"
    valor_inss: str = "0.00"
    valor_ir: str = "0.00"
    valor_csll: str = "0.00"
    outras_retencoes: str = "0.00"
    valor_iss: str = "0.00"
    aliquota: str = "0.00"
    desconto_incondicionado: str = "0.00"
    desconto_condicionado: str = "0.00"

    # Calibrado pelos XMLs reais autorizados (notas 3062-3065):
    iss_retido: int = 1               # 1 = Sim (retido) - sempre, no seu caso
    responsavel_retencao: Optional[int] = None  # Eusebio NAO envia este campo
    exigibilidade_iss: int = 1        # 1 = Exigivel (confirmado nos 4 XMLs reais)

    item_lista_servico: str = ""                # ABRASF LC116 (ex.: 07.02) — exigido pelo XSD da E&L
    codigo_tributacao_municipio: str = "702"    # codigo de servico municipal de Eusebio
    codigo_servico_nacional: str = ""           # E&L: codigo da lista nacional (ex.: 070202)
    codigo_cnae: str = ""
    discriminacao: str = ""
    codigo_municipio_servico: str = ""          # IBGE do local da obra
    municipio_incidencia: str = ""              # IBGE (normalmente = local da obra)

    # prestador (BWS) - ja preenchido
    prest_cnpj: str = "00079526000109"
    prest_im: str = "101084492"

    # tomador
    toma_doc: str = ""
    toma_razao: str = ""
    toma_logradouro: str = ""
    toma_numero: str = ""
    toma_bairro: str = ""
    toma_cmun: str = ""               # IBGE do municipio do tomador
    toma_uf: str = ""
    toma_cep: str = ""
    toma_email: str = ""

    optante_simples: int = 2          # 1 = Sim, 2 = Nao (BWS Lucro Real = 2)
    incentivo_fiscal: int = 2         # 1 = Sim, 2 = Nao
    regime_especial: Optional[int] = None   # normalmente vazio p/ Tributacao Normal
    informacoes_complementares: str = ""


def _e(parent, tag, text=None):
    el = etree.SubElement(parent, "{%s}%s" % (NS_ABRASF, tag))
    if text is not None and text != "":
        el.text = str(text)
    return el


def montar_rps(d: DadosRps) -> etree._Element:
    """Monta o elemento <Rps> (tcDeclaracaoPrestacaoServico) com o InfDeclaracao dentro."""
    rps = etree.Element("{%s}Rps" % NS_ABRASF, nsmap={None: NS_ABRASF})
    inf_id = f"rps{d.numero_rps}"
    inf = _e(rps, "InfDeclaracaoPrestacaoServico")
    inf.set("Id", inf_id)

    ident_rps = _e(inf, "Rps")
    idr = _e(ident_rps, "IdentificacaoRps")
    _e(idr, "Numero", d.numero_rps)
    _e(idr, "Serie", d.serie_rps)
    _e(idr, "Tipo", d.tipo_rps)
    _e(ident_rps, "DataEmissao", d.data_emissao)
    _e(ident_rps, "Status", d.status)
    # RpsSubstituido (opcional): vai logo após o Status, conforme o XSD ABRASF 2.04.
    # Faz esta nota substituir a nota antiga no município (IdentificacaoRps do RPS antigo).
    if getattr(d, "rps_substituido_numero", None):
        rsub = _e(ident_rps, "RpsSubstituido")
        _e(rsub, "Numero", d.rps_substituido_numero)
        _e(rsub, "Serie", d.rps_substituido_serie or d.serie_rps)
        _e(rsub, "Tipo", d.rps_substituido_tipo or d.tipo_rps)

    _e(inf, "Competencia", d.competencia)

    serv = _e(inf, "Servico")
    val = _e(serv, "Valores")
    _e(val, "ValorServicos", d.valor_servicos)
    _e(val, "ValorDeducoes", d.valor_deducoes)
    _e(val, "ValorPis", d.valor_pis)
    _e(val, "ValorCofins", d.valor_cofins)
    _e(val, "ValorInss", d.valor_inss)
    _e(val, "ValorIr", d.valor_ir)
    _e(val, "ValorCsll", d.valor_csll)
    _e(val, "OutrasRetencoes", d.outras_retencoes)
    _e(val, "ValorIss", d.valor_iss)
    _e(val, "Aliquota", d.aliquota)
    _e(val, "DescontoIncondicionado", d.desconto_incondicionado)
    _e(val, "DescontoCondicionado", d.desconto_condicionado)
    _e(serv, "IssRetido", d.iss_retido)
    if d.responsavel_retencao is not None:
        _e(serv, "ResponsavelRetencao", d.responsavel_retencao)
    if d.item_lista_servico:
        _e(serv, "ItemListaServico", d.item_lista_servico)
    if d.codigo_cnae:
        _e(serv, "CodigoCnae", d.codigo_cnae)
    _e(serv, "CodigoTributacaoMunicipio", d.codigo_tributacao_municipio)
    if d.codigo_servico_nacional:
        _e(serv, "CodigoServicoNacional", d.codigo_servico_nacional)
    _e(serv, "Discriminacao", d.discriminacao)
    _e(serv, "CodigoMunicipio", d.codigo_municipio_servico)
    _e(serv, "ExigibilidadeISS", d.exigibilidade_iss)
    _e(serv, "MunicipioIncidencia", d.municipio_incidencia or d.codigo_municipio_servico)

    prest = _e(inf, "Prestador")
    cpfcnpj = _e(prest, "CpfCnpj")
    _e(cpfcnpj, "Cnpj", "".join(filter(str.isdigit, d.prest_cnpj)))
    _e(prest, "InscricaoMunicipal", d.prest_im)

    toma = _e(inf, "TomadorServico")
    idt = _e(toma, "IdentificacaoTomador")
    cc = _e(idt, "CpfCnpj")
    doc = "".join(filter(str.isdigit, d.toma_doc))
    _e(cc, "Cnpj" if len(doc) == 14 else "Cpf", doc)
    _e(toma, "RazaoSocial", d.toma_razao)
    end = _e(toma, "Endereco")
    _e(end, "Endereco", d.toma_logradouro)
    _e(end, "Numero", d.toma_numero)
    _e(end, "Bairro", d.toma_bairro)
    _e(end, "CodigoMunicipio", d.toma_cmun)
    _e(end, "Uf", d.toma_uf)
    _e(end, "Cep", "".join(filter(str.isdigit, d.toma_cep)))
    if d.toma_email:
        cont = _e(toma, "Contato")
        _e(cont, "Email", d.toma_email)

    if d.regime_especial:
        _e(inf, "RegimeEspecialTributacao", d.regime_especial)
    _e(inf, "OptanteSimplesNacional", d.optante_simples)
    _e(inf, "IncentivoFiscal", d.incentivo_fiscal)
    if d.informacoes_complementares:
        _e(inf, "InformacoesComplementares", d.informacoes_complementares)

    return rps, inf_id


def montar_gerarnfse_assinado(d: DadosRps, chave_pem: bytes, cert_pem: bytes) -> bytes:
    """Monta o GerarNfseEnvio com o RPS assinado. Retorna os bytes do XML."""
    rps, inf_id = montar_rps(d)
    signer = _Assinador(
        method=methods.enveloped,
        signature_algorithm="rsa-sha1",
        digest_algorithm="sha1",
        c14n_algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    )
    # assina o InfDeclaracaoPrestacaoServico; a Signature entra dentro do <Rps>
    rps_assinado = signer.sign(rps, key=chave_pem, cert=cert_pem, reference_uri=inf_id)

    envio = etree.Element("{%s}GerarNfseEnvio" % NS_ABRASF,
                          nsmap={None: NS_ABRASF, "ds": NS_DSIG})
    envio.append(rps_assinado)
    return etree.tostring(envio, xml_declaration=True, encoding="UTF-8", standalone=False)


def montar_soap(envio_xml: bytes) -> bytes:
    """Embrulha o GerarNfseEnvio assinado no envelope SOAP do webservice."""
    cabec = ('<cabecalho xmlns="http://www.abrasf.org.br/nfse.xsd">'
             '<versaoDados>2.04</versaoDados></cabecalho>')
    dados = envio_xml.decode("utf-8")
    soap = (
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        'xmlns:nfse="http://nfse.abrasf.org.br">'
        '<soapenv:Header/><soapenv:Body>'
        '<nfse:GerarNfse><nfse:GerarNfseRequest>'
        f'<nfseCabecMsg><![CDATA[{cabec}]]></nfseCabecMsg>'
        f'<nfseDadosMsg><![CDATA[{dados}]]></nfseDadosMsg>'
        '</nfse:GerarNfseRequest></nfse:GerarNfse>'
        '</soapenv:Body></soapenv:Envelope>'
    )
    return soap.encode("utf-8")


def enviar(soap_bytes: bytes, url: str = WSDL_EUSEBIO, soap_action: str = "", timeout: int = 90):
    """Envia o SOAP ao webservice. So chame com DRY-RUN desligado."""
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": soap_action}
    r = requests.post(url, data=soap_bytes, headers=headers, timeout=timeout)
    return r


def extrair_retorno(resposta_texto: str) -> str:
    """Tira o outputXML de dentro do envelope SOAP de resposta (para leitura)."""
    try:
        ini = resposta_texto.index("<outputXML>") + len("<outputXML>")
        fim = resposta_texto.index("</outputXML>")
        conteudo = resposta_texto[ini:fim]
        return conteudo.replace("<![CDATA[", "").replace("]]>", "")
    except ValueError:
        return resposta_texto


# --------------------------------------------------------------------------- #
# Exemplo: RPS da BWS (baseado na sua nota 3066). MODO DRY-RUN.
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    chave_pem, cert_pem = carregar_certificado_a1("certificado.p12", "SUA_SENHA")

    d = DadosRps(
        numero_rps=999001,                      # use um numero de RPS NOVO
        serie_rps="1",
        data_emissao="2026-06-26",
        competencia="2026-06-01",
        valor_servicos="98720.04",
        valor_iss="4936.00",
        aliquota="5.00",
        valor_inss="5429.60",
        valor_ir="1184.64",
        iss_retido=1,                           # retido na fonte (confirmado nos XMLs reais)
        exigibilidade_iss=1,                    # 1 = Exigivel (confirmado nos 4 XMLs reais)
        codigo_tributacao_municipio="702",
        discriminacao="Execucao de obras, contrato 268/2025. CNO 90.025.25410/76.",
        codigo_municipio_servico="2601607",     # obra: Belem do Sao Francisco/PE
        municipio_incidencia="2601607",
        toma_doc="10572071000112",
        toma_razao="SECRETARIA DE EDUCACAO",
        toma_logradouro="AVENIDA AFONSO OLINDENSE",
        toma_numero="1513",
        toma_bairro="VARZEA",
        toma_cmun="2611606",                    # Recife/PE
        toma_uf="PE",
        toma_cep="50810900",
        optante_simples=2,
        incentivo_fiscal=2,
    )

    envio = montar_gerarnfse_assinado(d, chave_pem, cert_pem)
    soap = montar_soap(envio)

    DRY_RUN = True   # <<< deixe True para SO VER o XML; troque para False para EMITIR DE VERDADE

    print("===== XML QUE SERIA ENVIADO (confira campo a campo) =====")
    print(etree.tostring(etree.fromstring(envio), pretty_print=True, encoding="unicode"))

    if not DRY_RUN:
        print("\n>>> ENVIANDO EM PRODUCAO (nota real)...")
        r = enviar(soap)
        print("HTTP", r.status_code)
        print(extrair_retorno(r.text))
    else:
        print("\n[DRY-RUN ligado] Nada foi enviado. Revise o XML acima.")
