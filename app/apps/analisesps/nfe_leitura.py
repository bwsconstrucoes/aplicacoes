# -*- coding: utf-8 -*-
"""O XML da nota virando um documento que uma pessoa lê.

Pedido do dono em 17/09/2026: *"não tem problema abrir o XML em tela. Abre num
modal? E desse modal poderia exportar em PDF?"*

⚠️ ISTO NÃO É UM DANFE OFICIAL, e a tela diz isso. O DANFE tem forma definida
pela Receita (código de barras da chave, faixas, posição de cada campo) e vale
como documento auxiliar de circulação de mercadoria. O que sai daqui é uma
LEITURA do XML: os mesmos dados, organizados para conferir e imprimir. Serve
para juntar ao processo, mandar para o cliente, arquivar — não serve para
acompanhar carga na estrada.

O que vale como documento continua sendo o XML, que está guardado no Drive.

SEM BIBLIOTECA NOVA, e é o ponto do desenho: a leitura é do `xml.etree` que já
vem no Python, e o PDF é o **próprio navegador** imprimindo a página. Gerar PDF
no servidor exigiria dependência nova; imprimir não exige nada, e ainda sai
melhor — quem imprime escolhe margem, tamanho e se quer papel ou arquivo.

⚠️ NAMESPACE. Todo XML de NF-e vem no namespace
`http://www.portalfiscal.inf.br/nfe`, e o `xml.etree` obriga a repetir o
namespace em CADA busca. Como aqui só se lê NF-e, ele é removido uma vez na
entrada e todo o resto do arquivo fica legível.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET

logger = logging.getLogger("analisesps.nfe_leitura")

# O que cada modalidade de frete quer dizer, em português de gente. O XML
# guarda o número; quem lê a nota quer a palavra.
MODALIDADES_DE_FRETE = {
    "0": "Por conta do emitente",
    "1": "Por conta do destinatário",
    "2": "Por conta de terceiros",
    "3": "Transporte próprio, por conta do emitente",
    "4": "Transporte próprio, por conta do destinatário",
    "9": "Sem frete",
}


class XmlIlegivel(ValueError):
    """O arquivo não é uma NF-e que dê para ler. A mensagem já vai para a tela."""


def _sem_namespace(elemento) -> None:
    """Tira o `{http://…}` da frente de cada etiqueta, recursivamente."""
    for no in elemento.iter():
        if isinstance(no.tag, str) and "}" in no.tag:
            no.tag = no.tag.split("}", 1)[1]


def _texto(no, caminho: str, padrao: str = "") -> str:
    """O texto de um campo, ou o padrão. Nunca levanta."""
    if no is None:
        return padrao
    achado = no.find(caminho)
    if achado is None or achado.text is None:
        return padrao
    return achado.text.strip()


def _numero(no, caminho: str):
    """O campo como número, ou None. Campo vazio e campo ausente dão o mesmo.

    ⚠️ Devolve None, não zero: "a nota não informou frete" e "o frete é zero"
    são coisas diferentes, e a tela esconde a primeira em vez de escrever
    R$ 0,00 — que quem lê tomaria por informação."""
    bruto = _texto(no, caminho)
    if not bruto:
        return None
    try:
        from decimal import Decimal
        return Decimal(bruto)
    except Exception:  # noqa: BLE001 — campo torto não derruba a leitura
        return None


def _endereco(no) -> dict:
    """O endereço de emitente ou destinatário, já montado numa linha."""
    if no is None:
        return {}
    logradouro = _texto(no, "xLgr")
    numero = _texto(no, "nro")
    complemento = _texto(no, "xCpl")
    bairro = _texto(no, "xBairro")
    pedacos = [p for p in (f"{logradouro}, {numero}".strip(", "), complemento,
                           bairro) if p]
    return {
        "linha": " · ".join(pedacos),
        "municipio": _texto(no, "xMun"),
        "uf": _texto(no, "UF"),
        "cep": _formatar_cep(_texto(no, "CEP")),
        "fone": _texto(no, "fone"),
    }


def _formatar_cep(bruto: str) -> str:
    so_numeros = re.sub(r"\D", "", bruto or "")
    if len(so_numeros) != 8:
        return bruto or ""
    return f"{so_numeros[:5]}-{so_numeros[5:]}"


def formatar_documento(bruto: str) -> str:
    """CNPJ ou CPF com a pontuação que todo mundo reconhece."""
    so_numeros = re.sub(r"\D", "", bruto or "")
    if len(so_numeros) == 14:
        return (f"{so_numeros[:2]}.{so_numeros[2:5]}.{so_numeros[5:8]}"
                f"/{so_numeros[8:12]}-{so_numeros[12:]}")
    if len(so_numeros) == 11:
        return (f"{so_numeros[:3]}.{so_numeros[3:6]}.{so_numeros[6:9]}"
                f"-{so_numeros[9:]}")
    return bruto or ""


def formatar_chave(bruto: str) -> str:
    """A chave em blocos de quatro — é assim que se confere dígito a dígito."""
    so_numeros = re.sub(r"\D", "", bruto or "")
    return " ".join(so_numeros[i:i + 4] for i in range(0, len(so_numeros), 4))


def _pessoa(no) -> dict:
    """Emitente ou destinatário. Os dois têm a mesma forma no XML."""
    if no is None:
        return {}
    documento = _texto(no, "CNPJ") or _texto(no, "CPF")
    return {
        "nome": _texto(no, "xNome"),
        "fantasia": _texto(no, "xFant"),
        "doc": formatar_documento(documento),
        "doc_cru": re.sub(r"\D", "", documento),
        "ie": _texto(no, "IE"),
        "endereco": _endereco(no.find("enderEmit") if no.find("enderEmit")
                              is not None else no.find("enderDest")),
    }


def _itens(inf) -> list:
    """As mercadorias da nota, na ordem em que a nota as traz."""
    saida = []
    for det in inf.findall("det"):
        prod = det.find("prod")
        if prod is None:
            continue
        saida.append({
            "numero": det.get("nItem", ""),
            "codigo": _texto(prod, "cProd"),
            "descricao": _texto(prod, "xProd"),
            "ncm": _texto(prod, "NCM"),
            "cfop": _texto(prod, "CFOP"),
            "unidade": _texto(prod, "uCom"),
            "quantidade": _numero(prod, "qCom"),
            "valor_unitario": _numero(prod, "vUnCom"),
            "valor_total": _numero(prod, "vProd"),
        })
    return saida


def _totais(inf) -> dict:
    """Os totais da nota. `None` onde a nota não informou — ver `_numero`."""
    icms = inf.find("total/ICMSTot")
    if icms is None:
        return {}
    return {
        "produtos": _numero(icms, "vProd"),
        "frete": _numero(icms, "vFrete"),
        "seguro": _numero(icms, "vSeg"),
        "desconto": _numero(icms, "vDesc"),
        "outras": _numero(icms, "vOutro"),
        "icms_base": _numero(icms, "vBC"),
        "icms": _numero(icms, "vICMS"),
        "ipi": _numero(icms, "vIPI"),
        "pis": _numero(icms, "vPIS"),
        "cofins": _numero(icms, "vCOFINS"),
        "nota": _numero(icms, "vNF"),
    }


def _transporte(inf) -> dict:
    transp = inf.find("transp")
    if transp is None:
        return {}
    transportador = transp.find("transporta")
    veiculo = transp.find("veicTransp")
    modalidade = _texto(transp, "modFrete")
    return {
        "modalidade": MODALIDADES_DE_FRETE.get(modalidade, ""),
        "transportador": _texto(transportador, "xNome"),
        "transportador_doc": formatar_documento(
            _texto(transportador, "CNPJ") or _texto(transportador, "CPF")),
        "placa": _texto(veiculo, "placa"),
    }


def ler(xml: str) -> dict:
    """O XML inteiro virando o dicionário que a tela desenha.

    ⚠️ ACEITA SÓ O DOCUMENTO INTEIRO. O resumo (`resNFe`) tem oito campos e
    nenhuma mercadoria — desenhar uma nota a partir dele seria mostrar uma
    folha quase vazia com cara de nota fiscal, que é pior do que dizer "ainda
    não tenho o documento". Por isso a recusa é explícita."""
    if not str(xml or "").strip():
        raise XmlIlegivel("O arquivo da nota veio vazio.")
    try:
        raiz = ET.fromstring(xml.encode("utf-8") if isinstance(xml, str) else xml)
    except ET.ParseError as e:
        raise XmlIlegivel(f"O arquivo não é um XML válido: {e}") from e

    _sem_namespace(raiz)

    inf = raiz.find(".//infNFe")
    if inf is None:
        raise XmlIlegivel(
            "Este arquivo não traz a nota inteira — é só o resumo que a "
            "Receita manda antes da ciência. O documento chega na próxima "
            "busca na Receita.")

    ide = inf.find("ide")
    protocolo = raiz.find(".//infProt")
    chave = re.sub(r"\D", "", (inf.get("Id") or ""))

    return {
        "chave": chave,
        "chave_bonita": formatar_chave(chave),
        "numero": _texto(ide, "nNF"),
        "serie": _texto(ide, "serie"),
        "emissao": _texto(ide, "dhEmi") or _texto(ide, "dEmi"),
        "saida": _texto(ide, "dhSaiEnt") or _texto(ide, "dSaiEnt"),
        "natureza": _texto(ide, "natOp"),
        "protocolo": _texto(protocolo, "nProt"),
        "protocolo_em": _texto(protocolo, "dhRecbto"),
        "situacao": _texto(protocolo, "xMotivo"),
        "emitente": _pessoa(inf.find("emit")),
        "destinatario": _pessoa(inf.find("dest")),
        "itens": _itens(inf),
        "totais": _totais(inf),
        "transporte": _transporte(inf),
        "observacoes": _texto(inf, "infAdic/infCpl"),
        "observacoes_fisco": _texto(inf, "infAdic/infAdFisco"),
    }
