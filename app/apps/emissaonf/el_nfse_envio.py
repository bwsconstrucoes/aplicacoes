# -*- coding: utf-8 -*-
"""
Envio real da NFS-e ao webservice ABRASF do Eusébio (E&L) — operação GerarNfse.
SOAP 1.1 | SOAPAction: http://nfse.abrasf.org.br/GerarNfse

Uso seguro:
  envelope = montar_envelope(xml_gerarnfseenvio_assinado)     # só monta
  enviar(xml_assinado, de_verdade=False)  -> PREVIEW (mostra o envelope, NÃO envia)
  enviar(xml_assinado, de_verdade=True)   -> ENVIA e devolve a resposta
"""
from __future__ import annotations
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape
import requests

ENDPOINT = "https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40/NfseWSService"
SOAPACTION = "http://nfse.abrasf.org.br/GerarNfse"
NS_WRAP = "http://nfse.abrasf.org.br"
ELEMENTO = "GerarNfse"   # elemento de despacho da operação
VERSAO = "2.04"
CABECALHO = (f'<cabecalho xmlns="http://www.abrasf.org.br/nfse.xsd" versao="{VERSAO}">'
             f'<versaoDados>{VERSAO}</versaoDados></cabecalho>')


def montar_envelope(xml_gerarnfseenvio: str, dados_string: bool = True,
                    incluir_cabec: bool = True) -> str:
    """Embrulha o GerarNfseEnvio (já assinado) no envelope SOAP 1.1.
    dados_string=True  -> nfseCabecMsg/nfseDadosMsg como STRING XML escapada (padrão E&L/ABRASF)
    dados_string=False -> como elemento-filho (modo alternativo)
    """
    corpo = xml_gerarnfseenvio.strip()
    if corpo.startswith("<?xml"):
        corpo = corpo[corpo.find("?>") + 2:].strip()
    if dados_string:
        cabec = f"<nfseCabecMsg>{escape(CABECALHO)}</nfseCabecMsg>" if incluir_cabec else ""
        dados = f"<nfseDadosMsg>{escape(corpo)}</nfseDadosMsg>"
    else:
        cabec = f"<nfseCabecMsg>{CABECALHO}</nfseCabecMsg>" if incluir_cabec else ""
        dados = f"<nfseDadosMsg>{corpo}</nfseDadosMsg>"
    # estrutura real do XSD: GerarNfse > GerarNfseRequest(qualified) > {nfseCabecMsg, nfseDadosMsg}
    return (
        '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" '
        f'xmlns:nfse="{NS_WRAP}">'
        '<soap:Body>'
        f'<nfse:{ELEMENTO}>'
        f'<nfse:GerarNfseRequest>{cabec}{dados}</nfse:GerarNfseRequest>'
        f'</nfse:{ELEMENTO}>'
        '</soap:Body>'
        '</soap:Envelope>'
    )


def enviar(xml_gerarnfseenvio: str, de_verdade: bool = False, incluir_cabec: bool = True,
           dados_string: bool = True, cert=None, timeout: int = 90):
    """de_verdade=False => preview (não envia). cert = caminho do .pem cliente (opcional)."""
    envelope = montar_envelope(xml_gerarnfseenvio, dados_string=dados_string, incluir_cabec=incluir_cabec)
    if not de_verdade:
        print("===== PREVIEW DO ENVELOPE SOAP (NÃO ENVIADO) =====")
        print(envelope)
        print("===== fim do preview =====")
        return None
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": SOAPACTION,
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BWS-NFSe/1.0",
               "Accept": "text/xml, application/soap+xml, */*",
               "Connection": "close"}
    body = envelope.encode("utf-8")
    # 408 = o servidor NÃO recebeu a requisição completa -> nenhuma nota criada -> seguro retentar.
    # (Retentamos SÓ no 408, justamente para nunca arriscar nota duplicada.)
    import time
    ultima = None
    for tentativa in range(1, 4):
        ultima = requests.post(ENDPOINT, data=body, headers=headers, cert=cert, timeout=timeout)
        if ultima.status_code != 408:
            return ultima
        print(f"[envio] HTTP 408 (corpo não chegou completo) — tentativa {tentativa}/3, "
              f"retentando em {3 * tentativa}s...")
        time.sleep(3 * tentativa)
    return ultima


def parse_resposta(texto: str) -> dict:
    """Extrai número/código da NFS-e ou as mensagens de erro do retorno GerarNfse.
    A nota oficial vem dentro de <outputXML> (XML que o ET já desescapa no .text)."""
    out = {"http_ok": True, "numero": None, "codigo_verificacao": None,
           "data_emissao": None, "erros": [], "bruto": texto, "nota_xml": None}

    def local(t): return t.split("}")[-1]

    try:
        root = ET.fromstring(texto.encode("utf-8"))
    except Exception as e:
        out["erros"].append(f"resposta não é XML válido: {e}")
        return out

    # desempacota o conteúdo de outputXML (a resposta ABRASF de verdade)
    inner = None
    for el in root.iter():
        if local(el.tag) == "outputXML" and (el.text or "").strip():
            inner = el.text
            break
    alvo = root
    if inner:
        out["nota_xml"] = inner
        try:
            alvo = ET.fromstring(inner.encode("utf-8"))
        except Exception:
            alvo = root

    for el in alvo.iter():
        tag = local(el.tag)
        if tag == "Numero" and out["numero"] is None:
            out["numero"] = (el.text or "").strip()
        elif tag == "CodigoVerificacao" and out["codigo_verificacao"] is None:
            out["codigo_verificacao"] = (el.text or "").strip()
        elif tag == "DataEmissao" and out["data_emissao"] is None:
            out["data_emissao"] = (el.text or "").strip()
        elif tag == "MensagemRetorno":
            cod = msg = cor = ""
            for c in el:
                lt = local(c.tag)
                if lt == "Codigo": cod = (c.text or "").strip()
                elif lt == "Mensagem": msg = (c.text or "").strip()
                elif lt == "Correcao": cor = (c.text or "").strip()
            out["erros"].append(f"[{cod}] {msg}" + (f" — {cor}" if cor else ""))
        elif tag == "faultstring":
            out["erros"].append(f"SOAP Fault: {(el.text or '').strip()}")
    return out