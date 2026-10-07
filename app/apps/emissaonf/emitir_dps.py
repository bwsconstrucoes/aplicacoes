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


class AindaProcessando(RuntimeError):
    """A prefeitura recebeu a declaração e ainda não terminou. Nota não existe ainda."""


class DeclaracaoRecusada(RuntimeError):
    """A plataforma nacional RECUSOU a declaração. Nenhuma nota foi criada.

    É o terceiro desfecho, e o mais tranquilo dos três: o manual diz que quando a
    resposta traz a lista de erros, a solicitação não foi processada e alguma
    correção é necessária antes de nova tentativa — e que **a mesma declaração
    pode ser reenviada com a correção, mantendo a mesma identificação**.

    Ou seja: corrigir e emitir de novo, com o MESMO número, é o caminho previsto.
    Não é risco de nota duplicada — é o que a prefeitura manda fazer.
    """

    def __init__(self, motivos, id_dps=""):
        self.motivos = list(motivos or [])
        self.id_dps = id_dps
        super().__init__("; ".join(self.motivos) or "sem detalhes")


def numero_da_declaracao(id_dps: str) -> tuple[str, str]:
    """Tira o número da nota e o ano de dentro da identificação da declaração.

    A identificação termina em: série(5) + ano(2) + número(13). Serve para
    reencontrar o card e dizer à pessoa de que nota se está falando — ler um
    identificador de 45 dígitos à mão é pedir erro.
    """
    digitos = "".join(c for c in str(id_dps or "") if c.isdigit())
    if len(digitos) < 15:
        return "", ""
    ndps = digitos[-15:]
    return str(int(ndps[2:])), "20" + ndps[:2]


def consultar(ctx: dict, id_dps: str, token: str, producao: bool) -> dict:
    """Pergunta à prefeitura se aquela declaração já virou nota.

    É o caminho de saída do único aperto desta área: a declaração foi aceita, a
    nota pode existir, e reenviar seria criar a segunda nota do mesmo serviço.
    Em vez de reenviar, pergunta-se.

    Devolve os dados da nota, ou levanta `AindaProcessando` se a prefeitura
    ainda não terminou — e aí é só esperar e perguntar de novo.
    """
    chave_pem, cert_pem = ctx.get("chave_pem"), ctx.get("cert_pem")
    if not (chave_pem and cert_pem):
        raise NotaNaoSaiu("Certificado A1 não carregado — sem ele não dá para consultar.")
    if not token:
        raise NotaNaoSaiu("Token de integração da prefeitura ausente — ver a tela de Diagnóstico.")

    cliente = nac.ELNfseNacional(
        token=token, chave_pem=chave_pem, cert_pem=cert_pem,
        ambiente="producao" if producao else "homologacao",
    )
    proc = cliente.consultar_processamento_dps(id_dps, bruto=True)

    # Recusada: não existe nota, e o caminho é corrigir e reenviar com o MESMO
    # número. É o desfecho mais tranquilo dos três, e era o que vinha disfarçado
    # de "não consegui consultar".
    motivos = nac.ELNfseNacional.erros_da_resposta(proc)
    if motivos:
        raise DeclaracaoRecusada(motivos, id_dps=id_dps)

    xml_nac = nac.ELNfseNacional.descompactar(proc.get("nfseXmlGZipB64", "") or "")
    pronta = (proc.get("chaveAcesso") and xml_nac
              and "processamento" not in xml_nac.lower() and "<" in xml_nac)
    if not pronta:
        bruto = (xml_nac or "").strip()[:300]
        raise AindaProcessando(
            f"A prefeitura confirma que recebeu a declaração, mas a nota ainda não "
            f"ficou pronta.\n\nO que ela respondeu agora: "
            f"{bruto or '(sem conteúdo — só o protocolo)'}\n\n"
            f">>> Isto NÃO é erro, e NÃO autoriza emitir de novo. Espere alguns "
            f"minutos e consulte esta mesma identificação outra vez."
        )
    return dados_da_nota(xml_nac)


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
            "Falta o TOKEN DE INTEGRAÇÃO DA PREFEITURA.\n\n"
            "Ele NÃO é o token do link desta tela (EMISSAO_NF_TOKEN) — são duas coisas "
            "diferentes:\n"
            "  • EMISSAO_NF_TOKEN protege o endereço desta página, e é nosso;\n"
            "  • o token de integração autentica o canal com a prefeitura, e é dela.\n\n"
            "Por que ele nunca foi necessário antes: o modelo antigo (ABRASF) autenticava "
            "pelo CERTIFICADO digital, no próprio aperto de mão da conexão. O modelo "
            "nacional (DPS) exige certificado E token.\n\n"
            "Onde conseguir: no portal da prefeitura, na mesma tela de onde saiu o pacote "
            "da documentação — Configurações › APIs de Integração. Depois é só gravar como "
            "EL_NFSE_TOKEN no Render (ou na aba Credenciais da planilha).\n\n"
            "A tela de Diagnóstico mostra se ele chegou, e com que nome."
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
            proc = cliente.consultar_processamento_dps(id_dps, bruto=True)
        except Exception as e:
            # A consulta falhou, não a emissão. Insistir é seguro.
            ultimo = f"consulta falhou: {type(e).__name__}: {e}"
            proc = {}
        # Recusa é resposta definitiva: não há nota e não há o que esperar.
        # Sair daqui na hora poupa a espera inteira e diz o motivo de verdade.
        motivos = nac.ELNfseNacional.erros_da_resposta(proc)
        if motivos:
            raise DeclaracaoRecusada(motivos, id_dps=id_dps)
        xml_nac = nac.ELNfseNacional.descompactar(proc.get("nfseXmlGZipB64", "") or "")
        if proc.get("chaveAcesso") and xml_nac and "processamento" not in xml_nac.lower():
            return dados_da_nota(xml_nac)
        if xml_nac and "processamento" not in xml_nac.lower() and "<" not in xml_nac:
            ultimo = xml_nac
        if time.time() >= limite:
            break
        time.sleep(ESPERA_ENTRE_CONSULTAS_S)

    numero, _ano = numero_da_declaracao(id_dps)
    raise NotaTalvezTenhaSaido(
        f"A prefeitura ACEITOU a declaração da nota {numero or '?'} e ainda estava "
        f"processando quando a espera de {espera_total_s}s acabou.\n\n"
        f"Isto não é erro: o processamento dela é uma fila do lado da prefeitura, e "
        f"às vezes demora mais que a nossa espera.\n"
        f"{('O último retorno dela foi: ' + ultimo) if ultimo else 'Até o fim ela respondeu apenas que estava processando.'}\n\n"
        f">>> NÃO EMITA DE NOVO ANTES DE CONFERIR. Pode ser que a nota "
        f"{numero or ''} já exista — e aí emitir outra criaria a segunda nota do "
        f"mesmo serviço, que é o que não se desfaz. Pode também ser que a "
        f"plataforma tenha recusado a declaração, e nesse caso não existe nota "
        f"nenhuma e é seguro corrigir e reenviar. São coisas diferentes, e só a "
        f"consulta diz qual é.\n\n"
        f">>> O QUE FAZER: abra a tela \"Conferir declaração\" (link no pé da tela de "
        f"emissão), cole a identificação abaixo e clique em consultar. Ela pergunta à "
        f"prefeitura se a nota saiu e, se saiu, termina o serviço — planilha, Omie, "
        f"card, Drive e avisos — sem emitir nada de novo.\n\n"
        f"Identificação da declaração:\n{id_dps}",
        id_dps=id_dps,
    )
