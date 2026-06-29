# -*- coding: utf-8 -*-
"""
Cliente da API NFS-e Nacional da E&L (provedor do município de Eusébio/CE).
Calibrado para o cenário da BWS Construções (Lucro Real, obras de construção civil).

Fluxo (layout "Layout_EL_DPS_Nacional"):
    1) monta o XML da DPS (padrão nacional)
    2) assina (XMLDSig, RSA-SHA1 enveloped) com o certificado A1 da BWS
    3) compacta em GZip e codifica em Base64
    4) POST  .../api/nacional/{ambiente}/nfse?token={token}
    5) consulta o processamento assíncrono e recupera NFS-e / erros

ATENÇÃO: o token autentica o canal; a DPS PRECISA ser assinada com o A1.
Os dois são necessários.

Dependências:  pip install requests signxml cryptography lxml
"""

from __future__ import annotations

import base64
import gzip
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from signxml import XMLSigner, methods

# --------------------------------------------------------------------------- #
# Constantes (Eusébio / E&L)
# --------------------------------------------------------------------------- #
URLBASE_EUSEBIO = "https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40"
COD_IBGE_EUSEBIO = 2304285

NS_NFSE = "http://www.sped.fazenda.gov.br/nfse"
NS_DSIG = "http://www.w3.org/2000/09/xmldsig#"


# --------------------------------------------------------------------------- #
# Assinatura (RSA-SHA1, exigido pelo padrão nacional; signxml 5.x bloqueia por
# padrão — liberamos só o que o layout exige)
# --------------------------------------------------------------------------- #
class _AssinadorNacional(XMLSigner):
    def check_deprecated_methods(self):  # libera RSA-SHA1
        pass


def carregar_certificado_a1(caminho_pfx: str, senha: str) -> tuple[bytes, bytes]:
    with open(caminho_pfx, "rb") as fh:
        chave, cert, _ = pkcs12.load_key_and_certificates(fh.read(), senha.encode("utf-8"))
    if chave is None or cert is None:
        raise ValueError("Não foi possível extrair chave/certificado do .pfx (senha incorreta?).")
    chave_pem = chave.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return chave_pem, cert.public_bytes(serialization.Encoding.PEM)


def assinar_dps(root: etree._Element, chave_pem: bytes, cert_pem: bytes) -> etree._Element:
    inf = root.find("{%s}infDPS" % NS_NFSE)
    if inf is None or not inf.get("Id"):
        raise ValueError("infDPS sem atributo Id.")
    signer = _AssinadorNacional(
        method=methods.enveloped,
        signature_algorithm="rsa-sha1",
        digest_algorithm="sha1",
        c14n_algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    )
    return signer.sign(root, key=chave_pem, cert=cert_pem, reference_uri=inf.get("Id"))


def gerar_id_dps(c_loc_emi: int, cnpj_cpf: str, serie: int, n_dps: int) -> str:
    """DPS + cLocEmi(7) + tpInsc(1) + inscricao(14) + serie(5) + nDPS(15) = 45 chars."""
    doc = "".join(filter(str.isdigit, cnpj_cpf))
    tp_insc = 2 if len(doc) == 14 else 1
    return ("DPS" + f"{int(c_loc_emi):07d}" + str(tp_insc) + doc.zfill(14)
            + f"{int(serie):05d}" + f"{int(n_dps):015d}")


# --------------------------------------------------------------------------- #
# Grupo IBS/CBS (reforma) - obrigatório p/ Lucro Real em 2026.
# Valores ficam zerados na transição; CST + cClassTrib identificam a operação.
# Para construção civil (item 7.02): cIndOp=020201, cClassTrib=200046, CST=000.
# --------------------------------------------------------------------------- #
@dataclass
class GrupoIBSCBS:
    c_ind_op: str = "020201"        # 7.02 - obras de construção civil
    cst: str = "000"
    c_class_trib: str = "200046"    # Operações com bens imóveis
    fin_nfse: int = 0
    ind_final: int = 0              # 0 = tomador não é consumidor final (B2B/órgão)
    ind_dest: int = 0


@dataclass
class DadosDPS:
    # identificação
    serie: int
    n_dps: int
    dh_emi: str                     # ISO 8601 c/ fuso: "2026-06-19T11:36:06-03:00"
    d_compet: str                   # "AAAA-MM-DD"
    tp_amb: int = 2                 # 1=Produção, 2=Homologação

    # prestador (BWS) - já preenchido com os dados reais
    prest_cnpj: str = "00079526000109"
    prest_im: str = "101084492"
    prest_fone: str = ""
    op_simp_nac: int = 1            # 1 = Não optante (BWS é Lucro Real)
    reg_esp_trib: int = 0           # 0 = nenhum regime especial

    # tomador
    toma_doc: str = ""
    toma_nome: str = ""
    toma_cmun: int = 0
    toma_cep: str = ""
    toma_lgr: str = ""
    toma_nro: str = ""
    toma_bairro: str = ""

    # serviço
    c_loc_prestacao: int = 0        # *** local da OBRA (IBGE) - NÃO assumir Eusébio ***
    c_trib_nac: str = "070202"      # 7.02.2 empreitada/subempreitada (070201 = por administração)
    c_nbs: str = "101011100"        # construção de edificações (1.0101.11.00)
    c_int_contrib: str = "702"      # código de serviço MUNICIPAL de Eusébio
    x_desc_serv: str = ""

    # valores
    v_serv: str = "0.00"

    # ISS (município)
    trib_issqn: int = 1             # 1 = tributável
    tp_ret_issqn: int = 1           # 1 = retido na fonte, 2 = não retido
    p_aliq: str = "0.00"

    # retenções federais (valores retidos)
    v_ret_inss: str = "0.00"        # vRetCP (INSS/previdência)
    v_ret_irrf: str = "0.00"
    v_ret_csll: str = "0.00"
    # PIS/COFINS (opcional; só preencher se houver)
    pis_cofins: Optional[dict] = field(default=None)

    c_loc_emi: int = COD_IBGE_EUSEBIO
    tp_emit: int = 1                # 1 = prestador

    # grupo reforma (default: construção civil). Definir None p/ omitir.
    ibscbs: Optional[GrupoIBSCBS] = field(default_factory=GrupoIBSCBS)


def _sub(parent, tag, text=None):
    el = etree.SubElement(parent, "{%s}%s" % (NS_NFSE, tag))
    if text is not None:
        el.text = str(text)
    return el


def montar_dps_xml(d: DadosDPS) -> etree._Element:
    if not d.c_loc_prestacao:
        raise ValueError("c_loc_prestacao (IBGE do local da obra) é obrigatório.")

    root = etree.Element("{%s}DPS" % NS_NFSE,
                         nsmap={None: NS_NFSE, "ns2": NS_DSIG}, versao="1.01")
    inf = _sub(root, "infDPS")
    inf.set("Id", gerar_id_dps(d.c_loc_emi, d.prest_cnpj, d.serie, d.n_dps))

    _sub(inf, "tpAmb", d.tp_amb)
    _sub(inf, "dhEmi", d.dh_emi)
    _sub(inf, "verAplic", "1.0")
    _sub(inf, "serie", d.serie)
    _sub(inf, "nDPS", d.n_dps)
    _sub(inf, "dCompet", d.d_compet)
    _sub(inf, "tpEmit", d.tp_emit)
    _sub(inf, "cLocEmi", d.c_loc_emi)

    prest = _sub(inf, "prest")
    _sub(prest, "CNPJ", "".join(filter(str.isdigit, d.prest_cnpj)))
    _sub(prest, "IM", d.prest_im)
    if d.prest_fone:
        _sub(prest, "fone", d.prest_fone)
    reg = _sub(prest, "regTrib")
    _sub(reg, "opSimpNac", d.op_simp_nac)
    _sub(reg, "regEspTrib", d.reg_esp_trib)

    toma = _sub(inf, "toma")
    doc = "".join(filter(str.isdigit, d.toma_doc))
    _sub(toma, "CNPJ" if len(doc) == 14 else "CPF", doc)
    _sub(toma, "xNome", d.toma_nome)
    end = _sub(toma, "end")
    endnac = _sub(end, "endNac")
    _sub(endnac, "cMun", d.toma_cmun)
    _sub(endnac, "CEP", "".join(filter(str.isdigit, d.toma_cep)))
    _sub(end, "xLgr", d.toma_lgr)
    _sub(end, "nro", d.toma_nro)
    _sub(end, "xBairro", d.toma_bairro)

    serv = _sub(inf, "serv")
    locprest = _sub(serv, "locPrest")
    _sub(locprest, "cLocPrestacao", d.c_loc_prestacao)
    cserv = _sub(serv, "cServ")
    _sub(cserv, "cTribNac", d.c_trib_nac)
    _sub(cserv, "xDescServ", d.x_desc_serv)
    if d.c_nbs:
        _sub(cserv, "cNBS", d.c_nbs)
    _sub(cserv, "cIntContrib", d.c_int_contrib)

    valores = _sub(inf, "valores")
    vsp = _sub(valores, "vServPrest")
    _sub(vsp, "vServ", d.v_serv)
    trib = _sub(valores, "trib")
    tribmun = _sub(trib, "tribMun")
    _sub(tribmun, "tribISSQN", d.trib_issqn)
    _sub(tribmun, "tpRetISSQN", d.tp_ret_issqn)
    _sub(tribmun, "pAliq", d.p_aliq)

    tribfed = _sub(trib, "tribFed")
    if d.pis_cofins:
        pc = _sub(tribfed, "piscofins")
        for k in ("CST", "vBCPisCofins", "pAliqPis", "pAliqCofins",
                  "vPis", "vCofins", "tpRetPisCofins"):
            if k in d.pis_cofins:
                _sub(pc, k, d.pis_cofins[k])
    _sub(tribfed, "vRetCP", d.v_ret_inss)
    _sub(tribfed, "vRetIRRF", d.v_ret_irrf)
    _sub(tribfed, "vRetCSLL", d.v_ret_csll)

    tottrib = _sub(trib, "totTrib")
    vtt = _sub(tottrib, "vTotTrib")
    _sub(vtt, "vTotTribFed", "0.00")
    _sub(vtt, "vTotTribEst", "0.00")
    _sub(vtt, "vTotTribMun", "0.00")

    # grupo IBS/CBS (reforma) - filho de infDPS, depois de <valores>
    if d.ibscbs:
        g = d.ibscbs
        ib = _sub(inf, "IBSCBS")
        _sub(ib, "finNFSe", g.fin_nfse)
        _sub(ib, "indFinal", g.ind_final)
        _sub(ib, "cIndOp", g.c_ind_op)
        _sub(ib, "indDest", g.ind_dest)
        ibval = _sub(ib, "valores")
        ibtrib = _sub(ibval, "trib")
        gibscbs = _sub(ibtrib, "gIBSCBS")
        _sub(gibscbs, "CST", g.cst)
        _sub(gibscbs, "cClassTrib", g.c_class_trib)

    return root


# --------------------------------------------------------------------------- #
# Cliente HTTP
# --------------------------------------------------------------------------- #
class ELNfseNacional:
    def __init__(self, token, chave_pem, cert_pem, ambiente="homologacao",
                 urlbase=URLBASE_EUSEBIO, timeout=60):
        if ambiente not in ("homologacao", "producao"):
            raise ValueError("ambiente deve ser 'homologacao' ou 'producao'.")
        self.token, self.chave_pem, self.cert_pem = token, chave_pem, cert_pem
        self.ambiente, self.urlbase, self.timeout = ambiente, urlbase.rstrip("/"), timeout
        self.session = requests.Session()
        retry = Retry(total=4, backoff_factor=1.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=("GET", "POST"))
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _url(self, path):
        return f"{self.urlbase}/api/nacional/{self.ambiente}/{path}"

    @staticmethod
    def _gzip_b64(xml_bytes):
        return base64.b64encode(gzip.compress(xml_bytes)).decode("ascii")

    @staticmethod
    def descompactar(b64_str):
        if not b64_str:
            return b64_str
        try:
            return gzip.decompress(base64.b64decode(b64_str)).decode("utf-8")
        except Exception:
            return b64_str  # texto indicativo de "em processamento"

    def _checar(self, resp):
        try:
            data = resp.json()
        except ValueError:
            resp.raise_for_status()
            raise RuntimeError(f"Resposta não-JSON (HTTP {resp.status_code}): {resp.text[:300]}")
        if isinstance(data, dict) and data.get("erros"):
            partes = []
            for e in data["erros"]:
                cod = e.get("codigo") or e.get("Codigo") or ""
                desc = e.get("descricao") or e.get("Descricao") or ""
                comp = e.get("complemento") or e.get("Complemento") or ""
                partes.append(" - ".join(p for p in (cod, desc, comp) if p))
            raise RuntimeError(f"Rejeitada (HTTP {resp.status_code}): " + " | ".join(partes))
        if resp.status_code not in (200, 201):
            resp.raise_for_status()
        return data

    def enviar_dps(self, dados: DadosDPS) -> dict:
        root = montar_dps_xml(dados)
        assinado = assinar_dps(root, self.chave_pem, self.cert_pem)
        xml_bytes = etree.tostring(assinado, xml_declaration=True, encoding="UTF-8", standalone=False)
        resp = self.session.post(self._url("nfse"), params={"token": self.token},
                                 json={"dpsXmlGZipB64": self._gzip_b64(xml_bytes)},
                                 timeout=self.timeout)
        return self._checar(resp)

    def consultar_processamento_dps(self, id_dps) -> dict:
        resp = self.session.get(self._url(f"nfseDps/{id_dps}"),
                                params={"token": self.token}, timeout=self.timeout)
        return self._checar(resp)

    def consultar_dps(self, id_dps) -> dict:
        resp = self.session.get(self._url(f"dps/{id_dps}"),
                                params={"token": self.token}, timeout=self.timeout)
        return self._checar(resp)

    def consultar_nfse(self, chave_acesso) -> dict:
        resp = self.session.get(self._url(f"nfse/{chave_acesso}"),
                                params={"token": self.token}, timeout=self.timeout)
        return self._checar(resp)

    def registrar_evento(self, chave_acesso, evento_xml_bytes) -> dict:
        resp = self.session.post(self._url(f"nfse/{chave_acesso}/eventos"),
                                 params={"token": self.token},
                                 json={"pedidoRegistroEventoXmlGZipB64": self._gzip_b64(evento_xml_bytes)},
                                 timeout=self.timeout)
        return self._checar(resp)

    def emitir_e_aguardar(self, dados: DadosDPS, timeout_s=120, intervalo_s=5) -> dict:
        envio = self.enviar_dps(dados)
        id_dps = envio.get("idDPS")
        if not id_dps:
            raise RuntimeError(f"Envio sem idDPS: {envio}")
        limite = time.time() + timeout_s
        while time.time() < limite:
            proc = self.consultar_processamento_dps(id_dps)
            chave = proc.get("chaveAcesso")
            nfse_xml = self.descompactar(proc.get("nfseXmlGZipB64", ""))
            if chave and nfse_xml and "processamento" not in nfse_xml.lower():
                return {"idDPS": id_dps, "chaveAcesso": chave, "nfse_xml": nfse_xml}
            time.sleep(intervalo_s)
        raise TimeoutError(f"DPS {id_dps} ainda em processamento após {timeout_s}s.")


# --------------------------------------------------------------------------- #
# Exemplo replicando a nota real da BWS (RPS 3066) em HOMOLOGAÇÃO
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    chave_pem, cert_pem = carregar_certificado_a1("certificado_bws.pfx", "SENHA_DO_PFX")
    cliente = ELNfseNacional(token="COLE_AQUI_O_TOKEN", chave_pem=chave_pem,
                             cert_pem=cert_pem, ambiente="homologacao")

    dados = DadosDPS(
        serie=1,
        n_dps=3066,
        dh_emi="2026-06-19T11:36:06-03:00",
        d_compet="2026-06-01",
        # prestador BWS já vem por default (CNPJ/IM/Eusébio)
        toma_doc="10572071000112",                  # Secretaria de Educação - PE
        toma_nome="SECRETARIA DE EDUCACAO",
        toma_cmun=2611606,                           # Recife/PE
        toma_cep="50810900",
        toma_lgr="AVENIDA AFONSO OLINDENSE",
        toma_nro="1513",
        toma_bairro="VARZEA",
        c_loc_prestacao=2601607,                     # *** obra: Belém do São Francisco/PE ***
        c_trib_nac="070202",                         # empreitada (confirmar adm x empreitada)
        c_int_contrib="702",
        x_desc_serv=("PAGAMENTO DA 10a MEDICAO DA EXECUCAO DE OBRAS PARA CONSTRUCAO "
                     "DE CRECHES - BLOCO 02 LOTE 02, CONTRATO 268/2025. CNO 90.025.25410/76."),
        v_serv="98720.04",
        p_aliq="5.00",
        tp_ret_issqn=1,                              # ISS retido na fonte
        v_ret_inss="5429.60",
        v_ret_irrf="1184.64",
        v_ret_csll="0.00",
    )

    resultado = cliente.emitir_e_aguardar(dados)
    print("Chave de acesso:", resultado["chaveAcesso"])