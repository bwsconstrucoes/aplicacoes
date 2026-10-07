# -*- coding: utf-8 -*-
"""
Envio da nota pelo modelo que a prefeitura passou a exigir (DPS nacional).

Substitui o `el_nfse_envio.py`, que falava o modelo antigo (ABRASF) e, desde
07/10/2026, recebe de volta sempre o mesmo erro: *"Com a Obrigatoriedade do IBS
CBS o modelo Abrasf foi desativado e deve ser migrado para o modelo de DPS."*

O que muda e o que NÃO muda:

  - **não muda** a conta das retenções, o corpo da nota, o teto de valor, nem
    nada do que acontece depois de emitir;
  - **muda** o formato do documento e o jeito de enviar: em vez de um envelope
    SOAP que devolve a nota na mesma resposta, manda-se a declaração e a
    prefeitura responde com um protocolo. A nota sai alguns segundos depois, e
    é preciso perguntar por ela.

É esse "perguntar por ela" que exige cuidado, e é o motivo deste módulo existir
separado: **entre o envio e a resposta, a nota pode já ter sido criada.** Então
um erro de rede depois do envio NÃO autoriza reenviar — autoriza consultar.
"""
from __future__ import annotations

import time

import el_nfse_nacional as nac


# Quanto tempo esperar a prefeitura converter a declaração em nota. O manual diz
# que o processamento é assíncrono e vai para uma fila; na prática leva segundos.
# O teto existe para a tela não ficar pendurada — passado ele, a nota pode ter
# saído, e quem decide é a consulta, não um reenvio.
ESPERA_TOTAL_S = 150
ESPERA_ENTRE_CONSULTAS_S = 5


class NotaNaoSaiu(RuntimeError):
    """A prefeitura recusou a declaração. Nenhuma nota foi criada."""


class NotaTalvezTenhaSaido(RuntimeError):
    """A declaração foi aceita mas a nota não apareceu no tempo esperado.

    Esta é a situação delicada: a nota PODE existir. Quem receber isto não deve
    reenviar — deve consultar pela identificação da declaração (idDPS), que vem
    na mensagem.
    """

    def __init__(self, mensagem, id_dps=""):
        super().__init__(mensagem)
        self.id_dps = id_dps


def _texto(el):
    return (el.text or "").strip() if el is not None else ""


def dados_da_nota(xml_nacional: str) -> dict:
    """Tira do XML nacional o que o resto do sistema precisa: número, chave e data."""
    import xml.etree.ElementTree as ET
    ns = "{%s}" % nac.NS_NFSE
    root = ET.fromstring(xml_nacional.encode("utf-8") if isinstance(xml_nacional, str)
                         else xml_nacional)
    inf = root.find(ns + "infNFSe")
    if inf is None:
        raise NotaNaoSaiu("A prefeitura devolveu algo que não é uma NFS-e nacional.")
    chave = (inf.get("Id") or "").replace("NFS", "")
    numero = _texto(inf.find(ns + "nNFSe"))
    data = _texto(inf.find(ns + "dhProc"))[:10]
    if not numero:
        raise NotaNaoSaiu("A NFS-e voltou sem número (nNFSe) — não dá para registrar.")
    return {"numero": numero, "chave": chave, "data_iso": data, "xml_nacional": xml_nacional}


def _mensagem_de_erro(e: Exception) -> str:
    """Deixa o erro da prefeitura legível para quem está olhando a tela."""
    texto = str(e)
    if "Abrasf foi desativado" in texto:
        return (texto + "\n\n>>> Este erro é do modelo ANTIGO. Se ele aparecer aqui, "
                "a emissão não está usando o caminho novo — avise, porque é defeito nosso.")
    return texto


def emitir(ctx: dict, dados_dps: nac.DadosDPS, token: str, producao: bool,
           espera_total_s: int = ESPERA_TOTAL_S) -> dict:
    """Declara a nota à prefeitura e devolve número, chave, data e o XML.

    `ctx` é o contexto do `worker.preparar` (de onde saem o certificado e a
    chave já em memória). `token` é o token de integração da prefeitura.
    """
    chave_pem, cert_pem = ctx.get("chave_pem"), ctx.get("cert_pem")
    if not (chave_pem and cert_pem):
        raise NotaNaoSaiu("Certificado A1 não carregado — a declaração tem de ser assinada.")
    if not token:
        raise NotaNaoSaiu(
            "Token de integração da prefeitura ausente. Ele é o que autentica o canal: "
            "defina EL_NFSE_TOKEN no serviço (ou a chave EL_NFSE_TOKEN na aba Credenciais)."
        )

    cliente = nac.ELNfseNacional(
        token=token, chave_pem=chave_pem, cert_pem=cert_pem,
        ambiente="producao" if producao else "homologacao",
    )

    try:
        envio = cliente.enviar_dps(dados_dps)
    except Exception as e:
        # Recusa na entrada: a prefeitura não aceitou a declaração, então não
        # existe nota. É seguro corrigir e tentar de novo.
        raise NotaNaoSaiu(_mensagem_de_erro(e)) from e

    id_dps = envio.get("idDPS") or ""
    if not id_dps:
        raise NotaNaoSaiu(f"A prefeitura aceitou mas não devolveu a identificação da "
                          f"declaração (idDPS). Resposta: {envio}")

    # Daqui para baixo a declaração JÁ ESTÁ com a prefeitura. Só consultamos.
    limite = time.time() + espera_total_s
    ultimo = ""
    while True:
        try:
            proc = cliente.consultar_processamento_dps(id_dps)
        except Exception as e:
            # A consulta falhou, não a emissão. Insistir é seguro.
            ultimo = f"consulta falhou: {type(e).__name__}: {e}"
            proc = {}
        xml_nac = nac.ELNfseNacional.descompactar(proc.get("nfseXmlGZipB64", "") or "")
        if proc.get("chaveAcesso") and xml_nac and "processamento" not in xml_nac.lower():
            return dados_da_nota(xml_nac)
        if xml_nac and "processamento" not in xml_nac.lower() and "<" not in xml_nac:
            ultimo = xml_nac
        if time.time() >= limite:
            break
        time.sleep(ESPERA_ENTRE_CONSULTAS_S)

    raise NotaTalvezTenhaSaido(
        f"A prefeitura aceitou a declaração {id_dps} mas a nota não ficou pronta em "
        f"{espera_total_s}s. {('Último retorno: ' + ultimo) if ultimo else ''}\n\n"
        f">>> NÃO emita de novo: a nota pode ter saído. Use a tela "
        f"'Fechar nacional pela chave' ou consulte esta identificação de declaração "
        f"no portal antes de qualquer nova tentativa.",
        id_dps=id_dps,
    )
