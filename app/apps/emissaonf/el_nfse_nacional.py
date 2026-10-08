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
from decimal import Decimal
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

# Tipo de retenção do ISSQN (XSD: TSTipoRetISSQN). Os nomes existem para que
# ninguém precise lembrar qual número é qual — já foi trocado uma vez.
RET_ISS_NAO_RETIDO = 1
RET_ISS_TOMADOR = 2
RET_ISS_INTERMEDIARIO = 3

# Tipo de retenção de PIS/COFINS/CSLL (XSD: TSTipoRetPISCofins). A tabela
# combina os três, então a escolha depende de QUAIS foram retidos.
_RET_PIS_COFINS = {
    (True, True, True): 3,     # PIS/COFINS/CSLL retidos
    (True, True, False): 4,    # PIS/COFINS retidos, CSLL não
    (True, False, False): 5,   # só PIS
    (False, True, False): 6,   # só COFINS
    (False, True, True): 7,    # PIS não, COFINS/CSLL retidos
    (False, False, True): 8,   # PIS/COFINS não, CSLL retido
    (True, False, True): 9,    # COFINS não, PIS/CSLL retidos
    (False, False, False): 0,  # nenhum
}


def tipo_retencao_pis_cofins(pis: bool, cofins: bool, csll: bool) -> int:
    """Traduz "quais foram retidos" no código que o layout nacional espera."""
    return _RET_PIS_COFINS[(bool(pis), bool(cofins), bool(csll))]


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


# --------------------------------------------------------------------------- #
# Grupo de obra - OBRIGATORIO para servico de construcao civil.
#
# Descoberto em 08/10/2026, pela TI da prefeitura: a declaracao 3281 foi
# ACEITA pelo municipio e RECUSADA na plataforma nacional, com o erro
#
#   E0370 - O grupo de informacoes de obra e obrigatorio quando o codigo de
#   tributacao nacional pertencer a um dos subitens 07.02.01, 07.02.02,
#   07.04.01, 07.05.01, 07.05.02, 07.06.01, 07.06.02, 07.07.01, 07.08.01,
#   07.17.01, 07.19.01, 14.14.03 e 14.14.04 da lista de servicos.
#
# A BWS emite sempre em 07.02.02 (empreitada), entao para ela o grupo e
# obrigatorio em TODA nota. No modelo antigo (ABRASF) nao existia campo para
# isto: o CNO ia solto no texto da discriminacao, e por isso o defeito nao
# apareceu na migracao.
#
# O layout (TCInfoObra) exige UM de tres, e so um:
#   cObra  - numero do CNO ou do CEI da obra  <- o que a BWS tem
#   cCIB   - codigo do Cadastro Imobiliario Brasileiro (8 digitos)
#   end    - o endereco da obra (CEP + logradouro + numero + bairro)
# A inscricao imobiliaria fiscal (inscImobFisc) e opcional e vem antes.
# --------------------------------------------------------------------------- #
@dataclass
class GrupoObra:
    """Identificação da obra. Preencher UM dos três: CNO/CEI, CIB ou endereço."""
    c_obra: str = ""                # CNO ou CEI (o que a C. Diários guarda)
    c_cib: str = ""                 # Cadastro Imobiliário Brasileiro (8 dígitos)
    end: Optional[dict] = None      # {"CEP","xLgr","nro","xCpl","xBairro"}
    insc_imob_fisc: str = ""        # opcional: inscrição imobiliária / IPTU

    def identificacao(self) -> str:
        """Qual das três alternativas está preenchida. Vazio = nenhuma."""
        if self.c_obra:
            return "cObra"
        if self.c_cib:
            return "cCIB"
        if self.end:
            return "end"
        return ""


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
    trib_issqn: int = 1             # 1=tributável 2=imunidade 3=exportação 4=não incidência

    # ATENÇÃO — este campo já esteve com o significado INVERTIDO aqui (ver o
    # HISTORICO.md da área, 07/10/2026). O domínio oficial, no XSD
    # (TSTipoRetISSQN), é:
    #     1 = NÃO retido        2 = retido pelo TOMADOR        3 = retido pelo intermediário
    # As notas da BWS têm ISS retido na fonte pelo tomador, então o default é 2.
    # Mandar 1 numa nota retida declara à prefeitura que a BWS é que deve o ISS.
    tp_ret_issqn: int = RET_ISS_TOMADOR
    p_aliq: str = "0.00"

    # Dedução de material (equivale ao ValorDeducoes do modelo antigo): a
    # prefeitura calcula a base do ISS como vServ - vDR. Sem isso, ela aplica a
    # alíquota sobre o valor CHEIO — foi exatamente o defeito de setembro/2026.
    v_ded_red: str = ""            # vazio = sem dedução (não envia o grupo)

    # retenções federais (valores retidos)
    v_ret_inss: str = "0.00"        # vRetCP (INSS/previdência)
    v_ret_irrf: str = "0.00"
    v_ret_csll: str = "0.00"
    # PIS/COFINS (opcional; só preencher se houver)
    pis_cofins: Optional[dict] = field(default=None)

    c_loc_emi: int = COD_IBGE_EUSEBIO
    tp_emit: int = 1                # 1 = prestador

    # grupo de OBRA. Obrigatório nos subitens de construção civil (E0370) — e a
    # BWS emite sempre em 07.02.02, então na prática é obrigatório sempre.
    obra: Optional[GrupoObra] = field(default=None)

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

    # Grupo de OBRA. A ordem é exigida pelo XSD: dentro de <serv>, depois de
    # <cServ> (o grupo <comExt>, que não usamos, ficaria entre os dois).
    if d.obra is not None:
        qual = d.obra.identificacao()
        if not qual:
            raise ValueError(
                "O grupo de obra foi pedido sem nenhuma das três identificações. "
                "O layout exige UMA: o CNO/CEI (cObra), o CIB (cCIB) ou o "
                "endereço da obra (end)."
            )
        g = _sub(serv, "obra")
        if d.obra.insc_imob_fisc:
            _sub(g, "inscImobFisc", d.obra.insc_imob_fisc)
        if qual == "cObra":
            _sub(g, "cObra", d.obra.c_obra)
        elif qual == "cCIB":
            _sub(g, "cCIB", d.obra.c_cib)
        else:
            e = _sub(g, "end")
            _sub(e, "CEP", "".join(filter(str.isdigit, str(d.obra.end.get("CEP", "")))))
            _sub(e, "xLgr", d.obra.end.get("xLgr", ""))
            _sub(e, "nro", d.obra.end.get("nro", ""))
            if d.obra.end.get("xCpl"):
                _sub(e, "xCpl", d.obra.end["xCpl"])
            _sub(e, "xBairro", d.obra.end.get("xBairro", ""))

    valores = _sub(inf, "valores")
    vsp = _sub(valores, "vServPrest")
    _sub(vsp, "vServ", d.v_serv)
    # Dedução de material. A ordem é exigida pelo XSD: vem depois do valor do
    # serviço e antes dos tributos.
    if d.v_ded_red and Decimal(str(d.v_ded_red)) > 0:
        ded = _sub(valores, "vDedRed")
        _sub(ded, "vDR", d.v_ded_red)
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
        """Monta o endereço da operação (o caminho PREFERIDO).

        A produção NÃO tem o segmento de ambiente no caminho — o portal da
        prefeitura publica `/api/nacional/nfse`, enquanto a homologação é
        `/api/nacional/homologacao/nfse`. O manual em PDF diz que o ambiente é
        sempre um segmento do caminho, e nisso ele contradiz o portal; o portal
        ganha, porque é o que está no ar.
        """
        meio = "" if self.ambiente == "producao" else f"{self.ambiente}/"
        return f"{self.urlbase}/api/nacional/{meio}{path}"

    def _url_alternativa(self, path):
        """O outro jeito de escrever o mesmo endereço — o do manual em PDF.

        Existe porque a divergência acima é entre duas fontes oficiais, e
        descobrir qual está certa custaria a primeira emissão de verdade dar
        erro de endereço. Em vez de adivinhar, tenta-se o segundo.

        **Isto só é seguro por um motivo, e ele é o que importa:** 404 e 405
        significam que o endereço não existe — ou seja, a prefeitura não recebeu
        declaração nenhuma e NADA foi criado. Repetir nesse caso não arrisca uma
        segunda nota. Em qualquer outra resposta (inclusive erro de rede, que
        pode ter chegado) não se repete nada.
        """
        if self.ambiente == "producao":
            return f"{self.urlbase}/api/nacional/producao/{path}"
        return f"{self.urlbase}/api/nacional/{path}"

    # Respostas que provam que o endereço não existe — e só elas liberam a
    # segunda tentativa no caminho alternativo.
    _ENDERECO_NAO_EXISTE = (404, 405)

    def _chamar(self, metodo, path, **kw):
        """Faz a chamada no caminho preferido e, só se o endereço não existir,
        no alternativo. Devolve a resposta e lembra qual caminho funcionou."""
        resp = self.session.request(metodo, self._url(path), timeout=self.timeout, **kw)
        if resp.status_code not in self._ENDERECO_NAO_EXISTE:
            return resp
        alternativa = self._url_alternativa(path)
        print(f"[nacional] {self._url(path)} respondeu HTTP {resp.status_code} "
              f"(endereço não existe, nada foi criado) — tentando {alternativa}")
        segunda = self.session.request(metodo, alternativa, timeout=self.timeout, **kw)
        if segunda.status_code not in self._ENDERECO_NAO_EXISTE:
            self._caminho_que_funcionou = alternativa
            print(f"[nacional] o caminho que funciona neste ambiente é o do MANUAL "
                  f"({alternativa}) — vale corrigir o padrão no código.")
            return segunda
        return resp      # os dois falharam: devolve o erro do caminho preferido

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

    @staticmethod
    def erros_da_resposta(data) -> list[str]:
        """Lê a lista de erros que a plataforma devolve, em texto legível.

        O manual é explícito: quando a propriedade `erros` vem na resposta, a
        solicitação NÃO foi processada e alguma correção é necessária antes de
        tentar de novo. Ou seja: **não existe nota**. Saber disso é o que separa
        "espere" de "corrija e reenvie".
        """
        if not isinstance(data, dict):
            return []
        partes = []
        for e in (data.get("erros") or []):
            cod = e.get("codigo") or e.get("Codigo") or ""
            desc = e.get("descricao") or e.get("Descricao") or ""
            comp = e.get("complemento") or e.get("Complemento") or ""
            partes.append(" - ".join(x for x in (cod, desc, comp) if x))
        return partes

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
        resp = self._chamar("POST", "nfse", params={"token": self.token},
                            json={"dpsXmlGZipB64": self._gzip_b64(xml_bytes)})
        return self._checar(resp)

    def consultar_processamento_dps(self, id_dps, bruto: bool = False) -> dict:
        """Situação da declaração. Com `bruto=True`, devolve a resposta inteira
        SEM levantar erro quando ela trouxer a lista `erros`.

        Isso existe porque, numa consulta, `erros` não é falha da consulta: é a
        resposta — a declaração foi recusada. Tratar as duas coisas igual
        transformava "a prefeitura recusou, e aqui está o motivo" em "não
        consegui consultar", que manda a pessoa para o lugar errado.
        """
        resp = self._chamar("GET", f"nfseDps/{id_dps}", params={"token": self.token})
        if bruto:
            try:
                return resp.json()
            except ValueError:
                return {"_corpo": (resp.text or "")[:500], "_http": resp.status_code}
        return self._checar(resp)

    def consultar_dps(self, id_dps) -> dict:
        resp = self._chamar("GET", f"dps/{id_dps}", params={"token": self.token})
        return self._checar(resp)

    def consultar_nfse(self, chave_acesso) -> dict:
        resp = self._chamar("GET", f"nfse/{chave_acesso}", params={"token": self.token})
        return self._checar(resp)

    def registrar_evento(self, chave_acesso, evento_xml_bytes) -> dict:
        resp = self._chamar("POST", f"nfse/{chave_acesso}/eventos",
                            params={"token": self.token},
                            json={"pedidoRegistroEventoXmlGZipB64": self._gzip_b64(evento_xml_bytes)})
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
    import os

    # Nada de credencial colada no código: quem roda o exemplo define as três
    # variáveis no ambiente. Assim um token real nunca chega ao repositório.
    _token = os.getenv("EL_NFSE_TOKEN", "").strip()
    _senha_pfx = os.getenv("EMISSAO_NF_CERTIFICADO_SENHA", "").strip()
    if not _token or not _senha_pfx:
        raise SystemExit(
            "Defina EL_NFSE_TOKEN e EMISSAO_NF_CERTIFICADO_SENHA no ambiente.")

    chave_pem, cert_pem = carregar_certificado_a1(
        os.getenv("EMISSAO_NF_CERTIFICADO", "certificado_bws.pfx"), _senha_pfx)
    cliente = ELNfseNacional(token=_token, chave_pem=chave_pem,
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