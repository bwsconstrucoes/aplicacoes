# -*- coding: utf-8 -*-
"""
Monta uma NFS-e nacional de MENTIRA a partir de uma declaração (DPS) nossa.

Para que serve: a prefeitura não tem como ser consultada de brincadeira — toda
emissão de verdade é uma nota fiscal que não se apaga. Mas tudo o que acontece
DEPOIS de emitir (o PDF da nota, a DANFSe, a linha da planilha, o recibo) é
desenhado a partir do XML que ela devolve. Sem uma amostra desse XML, esse
pedaço do sistema só seria testado emitindo.

Então aqui se monta a resposta que a prefeitura daria: a declaração que nós
geramos, embrulhada no formato da nota, com número, chave e os valores
calculados. É conferida contra o schema oficial no
`tests/test_emissaonf_resposta_nacional.py`.

**Isto não é usado em produção** e não emite nada. É material de teste, e vive
no módulo (em vez de dentro de `tests/`) porque os módulos daqui se importam de
forma plana e para que dê para gerar uma amostra à mão quando for preciso
conferir um layout de PDF.
"""
from __future__ import annotations

from decimal import Decimal

from lxml import etree

import el_nfse_nacional as nac

NS = nac.NS_NFSE


def _sub(pai, tag, texto=None):
    el = etree.SubElement(pai, "{%s}%s" % (NS, tag))
    if texto is not None:
        el.text = str(texto)
    return el


def _v(x) -> str:
    return f"{Decimal(str(x or 0)):.2f}"


def montar(dps_root, numero_nfse="3084", chave=None, dh_proc="2026-10-07T10:00:12-03:00",
           x_loc_emi="Eusébio", x_loc_prestacao="Belém do São Francisco",
           x_trib_nac="Execução, por empreitada ou subempreitada, de obras de construção civil"):
    """Embrulha uma DPS na NFS-e que a prefeitura devolveria.

    `dps_root` é o elemento DPS montado por `el_nfse_nacional.montar_dps_xml`.
    Os valores calculados (base do ISS, ISS, líquido) são derivados da própria
    declaração — é o que a prefeitura faz.
    """
    inf_dps = dps_root.find("{%s}infDPS" % NS)
    if inf_dps is None:
        raise ValueError("DPS sem infDPS.")

    def t(caminho):
        el = inf_dps
        for tag in caminho.split("/"):
            el = el.find("{%s}%s" % (NS, tag))
            if el is None:
                return ""
        return (el.text or "").strip()

    v_serv = Decimal(t("valores/vServPrest/vServ") or 0)
    v_ded = Decimal(t("valores/vDedRed/vDR") or 0)
    p_aliq = Decimal(t("valores/trib/tribMun/pAliq") or 0)
    base = v_serv - v_ded
    v_iss = (base * p_aliq / 100).quantize(Decimal("0.01"))
    retidos = sum(Decimal(t(f"valores/trib/tribFed/{k}") or 0)
                  for k in ("vRetCP", "vRetIRRF", "vRetCSLL"))
    chave = chave or ("2304285" + "1" * 43)[:50]

    root = etree.Element("{%s}NFSe" % NS, nsmap={None: NS}, versao="1.01")
    inf = _sub(root, "infNFSe")
    inf.set("Id", "NFS" + chave)
    _sub(inf, "xLocEmi", x_loc_emi)
    _sub(inf, "xLocPrestacao", x_loc_prestacao)
    _sub(inf, "nNFSe", numero_nfse)
    _sub(inf, "xTribNac", x_trib_nac)
    _sub(inf, "verAplic", "1.0")
    _sub(inf, "ambGer", "1")
    _sub(inf, "tpEmis", "1")
    _sub(inf, "cStat", "100")
    _sub(inf, "dhProc", dh_proc)
    _sub(inf, "nDFSe", numero_nfse)

    emit = _sub(inf, "emit")
    _sub(emit, "CNPJ", t("prest/CNPJ"))
    _sub(emit, "IM", t("prest/IM"))
    _sub(emit, "xNome", "BWS CONSTRUCOES E SERVICOS LTDA")
    end = _sub(emit, "enderNac")
    _sub(end, "xLgr", "Rua Luís Moreira Gomes")
    _sub(end, "nro", "100")
    _sub(end, "xBairro", "Centro")
    _sub(end, "cMun", str(nac.COD_IBGE_EUSEBIO))
    _sub(end, "UF", "CE")
    _sub(end, "CEP", "61760000")

    val = _sub(inf, "valores")
    _sub(val, "vBC", _v(base))
    _sub(val, "pAliqAplic", _v(p_aliq))
    _sub(val, "vISSQN", _v(v_iss))
    _sub(val, "vTotalRet", _v(retidos + v_iss))
    _sub(val, "vLiq", _v(v_serv - retidos - v_iss))

    dps = _sub(inf, "DPS")
    dps.set("versao", "1.01")
    # a declaração entra inteira dentro da nota, como a prefeitura devolve
    import copy
    dps.append(copy.deepcopy(inf_dps))

    # A nota de verdade vem assinada pela prefeitura. A assinatura aqui é só a
    # ESTRUTURA, com valores de enfeite: o que se quer provar é que a amostra
    # tem a forma que o schema exige, não que a criptografia fecha.
    _assinatura_de_enfeite(root, inf.get("Id"))
    return root


def _assinatura_de_enfeite(root, referencia):
    ds = "http://www.w3.org/2000/09/xmldsig#"

    def s(pai, tag, texto=None, **attrs):
        el = etree.SubElement(pai, "{%s}%s" % (ds, tag), nsmap={None: ds})
        for k, v in attrs.items():
            el.set(k, v)
        if texto is not None:
            el.text = texto
        return el

    sig = s(root, "Signature")
    si = s(sig, "SignedInfo")
    s(si, "CanonicalizationMethod", Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315")
    s(si, "SignatureMethod", Algorithm=ds + "rsa-sha1")
    ref = s(si, "Reference", URI="#" + (referencia or ""))
    tr = s(ref, "Transforms")
    s(tr, "Transform", Algorithm=ds + "enveloped-signature")
    s(tr, "Transform", Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315")
    s(ref, "DigestMethod", Algorithm=ds + "sha1")
    s(ref, "DigestValue", "ZW5mZWl0ZQ==")
    s(sig, "SignatureValue", "ZW5mZWl0ZQ==")
    ki = s(sig, "KeyInfo")
    x5 = s(ki, "X509Data")
    s(x5, "X509Certificate", "ZW5mZWl0ZQ==")
    return sig


def como_texto(*args, **kwargs) -> str:
    return etree.tostring(montar(*args, **kwargs), pretty_print=True,
                          xml_declaration=True, encoding="UTF-8").decode("utf-8")
