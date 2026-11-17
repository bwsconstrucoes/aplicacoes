# -*- coding: utf-8 -*-
"""
Cancelamento de NFS-e na PREFEITURA, pelo padrão nacional.

⚠️ **Esta é a única parte do sistema que apaga um documento fiscal, e ela não se
desfaz.** Nota cancelada não volta. Por isso tudo aqui é explícito: quem chama
precisa passar a chave de acesso da nota, o motivo pelo código oficial e o motivo
escrito — nada tem valor-padrão que possa cancelar a nota errada por descuido.

**Por que existe — 17/11/2026.** O cancelamento era a única parte do caminho que
sobrava para a mão do dono, e ele cobrou, com razão:

    "Não é tudo automático, não? A automação cancela e emite nova nota? (…) E por
     que tu não faz tudo?"

Não havia motivo bom: o endereço da API já estava no `el_nfse_nacional`
(`registrar_evento`), o schema oficial do pedido está versionado em
`xsd_nacional/`, e a assinatura é a mesma da declaração. Só faltava montar o XML.

**Como o padrão nacional cancela:** não existe "apagar a nota". Registra-se um
EVENTO sobre ela — aqui o `e101101`, "Cancelamento de NFS-e". O pedido é um
`pedRegEvento` assinado com o certificado A1 da empresa, mandado GZip+Base64 para
`POST nfse/{chave}/eventos`.

O identificador do pedido tem forma fixa, e é o que o schema manda (`TSIdPedRegEvt`,
`PRE[0-9]{56}`): **"PRE" + chave de acesso (50) + tipo do evento (6)**.
"""
from __future__ import annotations
import datetime

from lxml import etree

from el_nfse_nacional import NS_NFSE, NS_DSIG, _AssinadorNacional
from signxml import methods

# O evento de cancelamento comum. Os outros do schema (cancelamento por
# substituição, por ofício, análise fiscal) NÃO entram aqui: cada um tem regra
# própria e nenhum deles é o que a BWS usa.
TIPO_EVENTO_CANCELAMENTO = "101101"
DESCRICAO_EVENTO = "Cancelamento de NFS-e"

# Códigos oficiais de justificativa (TSCodJustCanc). Só estes três existem.
MOTIVOS = {
    "1": "Erro na emissão",
    "2": "Serviço não prestado",
    "9": "Outros",
}

# O schema exige motivo escrito com 15 a 255 caracteres (TSMotivo) — texto curto
# demais é recusado, e por isso a crítica acontece aqui e não na prefeitura.
MOTIVO_MIN, MOTIVO_MAX = 15, 255

VERSAO_LAYOUT = "1.01"
VERSAO_APLICATIVO = "BWS-ERP 1.0"


class DadoInvalido(Exception):
    """Pedido que o schema oficial recusaria. Barrado antes de sair daqui."""


def _so_digitos(v) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def gerar_id_pedido(chave_acesso: str, tipo_evento: str = TIPO_EVENTO_CANCELAMENTO) -> str:
    """"PRE" + chave (50 dígitos) + tipo do evento (6 dígitos).

    O schema aceita `PRE` seguido de **56** dígitos, e 50 + 6 fecham os 56 — o
    número do pedido (`nPedRegEvento`), que a documentação menciona, é atribuído
    pela plataforma no identificador do EVENTO que ela devolve, não no pedido.
    """
    chave = _so_digitos(chave_acesso)
    if len(chave) != 50:
        raise DadoInvalido(
            f"A chave de acesso da nota tem de ter 50 dígitos, e esta tem {len(chave)}. "
            f"É a chave do padrão nacional, a mesma que aparece na DANFSe.")
    return "PRE" + chave + tipo_evento


def montar_pedido(chave_acesso: str, cnpj_autor: str, motivo_codigo: str,
                  motivo_texto: str, producao: bool,
                  quando: datetime.datetime | None = None) -> etree._Element:
    """Monta o `pedRegEvento` de cancelamento (ainda SEM assinatura).

    `producao=False` manda o pedido para o ambiente de homologação (`tpAmb=2`),
    que não tem validade fiscal.
    """
    chave = _so_digitos(chave_acesso)
    doc = _so_digitos(cnpj_autor)
    if len(doc) != 14:
        raise DadoInvalido(
            f"O CNPJ de quem cancela tem de ter 14 dígitos, e este tem {len(doc)}.")
    if str(motivo_codigo) not in MOTIVOS:
        raise DadoInvalido(
            f"Motivo '{motivo_codigo}' não existe. Os códigos oficiais são: "
            + ", ".join(f"{k} ({v})" for k, v in MOTIVOS.items()))
    texto = " ".join(str(motivo_texto or "").split())
    if not (MOTIVO_MIN <= len(texto) <= MOTIVO_MAX):
        raise DadoInvalido(
            f"O motivo escrito tem de ter de {MOTIVO_MIN} a {MOTIVO_MAX} caracteres, "
            f"e este tem {len(texto)}. A prefeitura recusa o pedido sem isso — "
            f"escreva a razão do cancelamento numa frase.")

    quando = quando or datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=-3)))

    root = etree.Element("{%s}pedRegEvento" % NS_NFSE,
                         nsmap={None: NS_NFSE, "ns2": NS_DSIG}, versao=VERSAO_LAYOUT)
    inf = etree.SubElement(root, "{%s}infPedReg" % NS_NFSE)
    inf.set("Id", gerar_id_pedido(chave))

    def _campo(nome, valor):
        el = etree.SubElement(inf, "{%s}%s" % (NS_NFSE, nome))
        el.text = str(valor)
        return el

    # A ordem é a do schema (TCInfPedReg) e não é negociável: campo fora de ordem
    # é recusado pela validação, não aceito "mais ou menos".
    _campo("tpAmb", "1" if producao else "2")
    _campo("verAplic", VERSAO_APLICATIVO)
    _campo("dhEvento", quando.replace(microsecond=0).isoformat())
    _campo("CNPJAutor", doc)
    _campo("chNFSe", chave)

    evt = etree.SubElement(inf, "{%s}e101101" % NS_NFSE)
    for nome, valor in (("xDesc", DESCRICAO_EVENTO),
                        ("cMotivo", str(motivo_codigo)),
                        ("xMotivo", texto)):
        el = etree.SubElement(evt, "{%s}%s" % (NS_NFSE, nome))
        el.text = valor
    return root


def assinar(root: etree._Element, chave_pem: bytes, cert_pem: bytes) -> etree._Element:
    """Assina o pedido com o certificado A1 — igual à declaração, mesma receita."""
    inf = root.find("{%s}infPedReg" % NS_NFSE)
    if inf is None or not inf.get("Id"):
        raise DadoInvalido("infPedReg sem atributo Id.")
    signer = _AssinadorNacional(
        method=methods.enveloped,
        signature_algorithm="rsa-sha1",
        digest_algorithm="sha1",
        c14n_algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    )
    return signer.sign(root, key=chave_pem, cert=cert_pem, reference_uri=inf.get("Id"))


def montar_xml(chave_acesso, cnpj_autor, motivo_codigo, motivo_texto, producao,
               chave_pem=None, cert_pem=None, quando=None) -> bytes:
    """O pedido pronto para enviar: montado, criticado e assinado.

    Sem certificado ele volta sem assinatura — serve para conferir o conteúdo numa
    tela de diagnóstico, nunca para enviar: a prefeitura recusa pedido sem
    assinatura.
    """
    root = montar_pedido(chave_acesso, cnpj_autor, motivo_codigo, motivo_texto,
                         producao, quando=quando)
    if chave_pem and cert_pem:
        root = assinar(root, chave_pem, cert_pem)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")
