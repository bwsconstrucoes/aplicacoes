# -*- coding: utf-8 -*-
"""
A busca automática de notas na Receita — NF-e e CT-e, pela chave do certificado.

Pedido do dono, com todas as letras: *"um dos corações dessa atualização é essa
busca automática por novas notas"*. Até aqui as notas só entravam pelo relatório
do FSist, colado à mão numa aba.

COMO O SERVIÇO FUNCIONA, e isto explica o desenho inteiro. Ele não responde "me
dá tudo de setembro". Responde **"me dá o que veio depois do número N"** — um
contador sequencial por CNPJ, chamado NSU. Cada resposta traz um lote e diz
qual foi o último número entregue; a consulta seguinte começa dali.

Por isso o ponteiro mora no banco (migração 008). Se ele se perdesse, a busca
recomeçaria do zero toda rodada — e **a Receita limita consultas**: quem rebobina
toda hora bate no limite e para de receber. Guardar onde parou não é
otimização; é o que faz a busca funcionar.

O QUE ELE TRAZ, E O QUE NÃO TRAZ:

  - **Traz** tudo que foi emitido CONTRA o CNPJ, sem ninguém pedir nota a
    ninguém. É a mesma fonte que o FSist consulta e revende.
  - **Não traz o passado.** A janela é curta — o histórico continua sendo os
    relatórios do FSist que o dono já guardou, importados uma vez e mantidos
    (ver `CONCILIACAO_FISCAL.md`).
  - **Não traz nota de serviço.** NFS-e é municipal, não tem serviço nacional.
    Decisão do dono em 12/09/2026: fora do escopo por enquanto.

UMA PARTE VEM PRONTA E OUTRA NÃO, e isso está dito onde importa: a consulta de
**NF-e** usa a biblioteca `erpbrasil.edoc`, que é código de terceiro usado por
muita gente há anos — inclusive a assinatura digital, que é a parte onde errar
é fácil e o erro chega como "recusado" sem dizer por quê. A de **CT-e** aquela
biblioteca não cobre, então é montada aqui, reusando o transporte e o
certificado dela.
"""
from __future__ import annotations

import base64
import gzip
import io
import logging
import os
import re

logger = logging.getLogger("analisesps.sefaz")

NFE, CTE = "NFE", "CTE"

# Ambiente da Receita: "1" é produção, "2" é homologação (o de testes, que não
# devolve nota de verdade). O padrão é PRODUÇÃO porque é para valer que isto
# existe — e homologação se liga pela variável, para quem quiser experimentar.
AMBIENTE = os.getenv("ANALISESPS_SEFAZ_AMBIENTE", "1")

# A UF de quem consulta. Só entra na montagem do pedido; o serviço de
# distribuição é nacional.
UF = os.getenv("ANALISESPS_SEFAZ_UF", "PE")

# Quantos lotes por rodada, por CNPJ e por tipo. Cada lote traz até cerca de
# cinquenta documentos. O teto existe porque a Receita limita consultas
# seguidas: varrer tudo de uma vez é o jeito mais rápido de ser bloqueado.
LOTES_POR_RODADA = 20

# O endereço nacional da distribuição de CT-e. O da NF-e a biblioteca resolve
# sozinha; este fica aqui porque ela não cobre CT-e.
URL_CTE_DISTRIBUICAO = (
    "https://www1.cte.fazenda.gov.br/CTeDistribuicaoDFe/CTeDistribuicaoDFe.asmx")


class SemCertificado(RuntimeError):
    """Falta o certificado A1 — não é falha, é configuração que não foi feita."""


class ErroDaReceita(RuntimeError):
    """A Receita recusou, e a mensagem já vem pronta para a tela."""


# ---------------------------------------------------------------------------
# O certificado
# ---------------------------------------------------------------------------
def _certificado(cnpj: str):
    """Abre o certificado A1 DAQUELE CNPJ, vindo do cofre.

    ELE VEM DA TELA, e não de variável de ambiente. Pedido do dono em
    12/09/2026, e o ganho é maior que a conveniência: o A1 vence todo ano, e
    com ele em variável cada troca é mexer no Render e reiniciar o serviço —
    cada empresa nova, uma variável nova.

    ⚠️ O A1 VENCE EM UM ANO, e no dia seguinte a busca para. Por isso a
    validade é lida de dentro do arquivo quando ele sobe, a tela avisa com
    trinta dias, e o CNPJ vencido nem é consultado: consultar com certificado
    vencido só produz recusa, e a recusa gasta a cota."""
    from erpbrasil.assinatura.certificado import Certificado

    from . import certificados

    try:
        conteudo, senha = certificados.abrir_para_uso(cnpj)
    except certificados.SemCofre as e:
        raise SemCertificado(str(e)) from e
    except certificados.ErroDeCertificado as e:
        raise SemCertificado(str(e)) from e

    try:
        return Certificado(conteudo, senha)
    except Exception as e:  # noqa: BLE001 — a mensagem tem de dizer o que fazer
        raise SemCertificado(
            f"Não consegui abrir o certificado de {cnpj}: {e}. Ele pode ter "
            "vencido — o A1 vale um ano. Suba o novo em Configurações.") from e


def configurado() -> bool:
    """Dá para buscar na Receita? A tela pergunta isto antes de oferecer."""
    from . import certificados
    return bool(certificados.cofre_configurado() and certificados.cnpjs_ativos())


def cnpjs_vigiados() -> list:
    """Os CNPJs consultados: os que TÊM certificado válido guardado.

    A LISTA SAI DO COFRE, e não de configuração à parte. Duas listas — uma de
    CNPJs e outra de certificados — divergiriam no dia em que alguém subisse um
    certificado e esquecesse de acrescentar o CNPJ, e a busca ficaria sem
    rodar para aquela empresa sem ninguém entender por quê."""
    from . import certificados
    return certificados.cnpjs_ativos()


# ---------------------------------------------------------------------------
# O ponteiro: até onde já se leu
# ---------------------------------------------------------------------------
def ponteiro(cnpj: str, tipo: str) -> dict:
    from .db import consultar_um

    linha = consultar_um(
        "SELECT ultimo_nsu, maior_nsu, consultado_em, ultimo_recado, documentos "
        "  FROM analisesps.sefaz_ponteiro WHERE cnpj = ? AND tipo = ?",
        (re.sub(r"\D", "", cnpj), tipo))
    if not linha:
        return {"ultimo_nsu": "0", "maior_nsu": "0", "consultado_em": None,
                "ultimo_recado": "", "documentos": 0}
    return {"ultimo_nsu": linha[0], "maior_nsu": linha[1],
            "consultado_em": linha[2], "ultimo_recado": linha[3],
            "documentos": linha[4]}


def gravar_ponteiro(cnpj: str, tipo: str, ultimo_nsu: str, maior_nsu: str,
                    recado: str = "", documentos: int = 0) -> None:
    """Anota onde parou. SÓ AVANÇA, nunca recua.

    Recuar faria a busca reler o que já veio e bater no limite da Receita —
    e uma resposta vazia (que traz NSU zero) chegaria como "volte ao começo"."""
    from .db import conexao

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sefaz_ponteiro "
            "  (cnpj, tipo, ultimo_nsu, maior_nsu, consultado_em, "
            "   ultimo_recado, documentos) "
            "VALUES (?, ?, ?, ?, now(), ?, ?) "
            "ON CONFLICT (cnpj, tipo) DO UPDATE SET "
            "  ultimo_nsu = GREATEST(analisesps.sefaz_ponteiro.ultimo_nsu, "
            "                        EXCLUDED.ultimo_nsu), "
            "  maior_nsu = GREATEST(analisesps.sefaz_ponteiro.maior_nsu, "
            "                       EXCLUDED.maior_nsu), "
            "  consultado_em = now(), ultimo_recado = EXCLUDED.ultimo_recado, "
            "  documentos = analisesps.sefaz_ponteiro.documentos "
            "              + EXCLUDED.documentos",
            (re.sub(r"\D", "", cnpj), tipo, _nsu(ultimo_nsu), _nsu(maior_nsu),
             str(recado or "")[:500], int(documentos)))
        conn.commit()


def _nsu(valor) -> str:
    """O NSU no formato da Receita: quinze dígitos, com os zeros à esquerda.

    Guardado com os zeros de propósito: assim a comparação de texto do banco
    (`GREATEST`) ordena igual à numérica, e o ponteiro nunca recua por causa de
    "9" parecer maior que "10"."""
    digitos = re.sub(r"\D", "", str(valor or "0")) or "0"
    return digitos.zfill(15)[-15:]


# ---------------------------------------------------------------------------
# Ler o que veio
# ---------------------------------------------------------------------------
def _descompactar(texto_base64: str) -> str:
    """O documento vem comprimido e em base64 dentro da resposta."""
    cru = base64.b64decode(texto_base64)
    try:
        return gzip.GzipFile(fileobj=io.BytesIO(cru)).read().decode("utf-8")
    except OSError:
        # Nem todo docZip vem comprimido; alguns chegam em texto puro.
        return cru.decode("utf-8", "replace")


def _tag(xml: str, nome: str) -> str:
    """O conteúdo da primeira tag com aquele nome, ignorando o namespace.

    Leitura por expressão em vez de parser de XML de propósito: são quatro
    campos de uma estrutura fixa, e o que chega já foi validado pela Receita.
    Um parser aqui seria mais código para o mesmo resultado."""
    achado = re.search(rf"<{nome}[^>]*>([^<]*)</{nome}>", xml)
    return achado.group(1).strip() if achado else ""


def ler_documento(xml: str) -> dict | None:
    """Um documento da Receita virando a linha que vai para a tabela de notas.

    ACEITA O RESUMO E O DOCUMENTO INTEIRO. A Receita entrega os dois formatos:
    o resumo (`resNFe`/`resCTe`), que já traz chave, emitente, valor e status, e
    o documento completo, quando ele está disponível. Os dois servem para a
    conciliação — ela precisa de chave, valor, emitente e número.

    Devolve None para o que não é documento: eventos, cancelamentos e o resto
    do que vem no mesmo lote."""
    from . import fiscal

    chave = fiscal.so_digitos(_tag(xml, "chNFe") or _tag(xml, "chCTe"))
    if len(chave) != 44:
        # O lote traz eventos e avisos junto. Não é erro; é o esperado.
        return None

    emissao = (_tag(xml, "dhEmi") or _tag(xml, "dEmi")
               or _tag(xml, "dhRecbto"))[:10]
    valor = (_tag(xml, "vNF") or _tag(xml, "vTPrest") or _tag(xml, "vRec"))
    numero = _tag(xml, "nNF") or _tag(xml, "nCT")
    if not numero:
        # No resumo o número não vem em campo próprio: está DENTRO da chave,
        # nas posições 26 a 34, por definição da Receita.
        numero = chave[25:34].lstrip("0")

    emitente_doc = (fiscal.so_digitos(_tag(xml, "CNPJ"))
                    or fiscal.emitente_da_chave(chave))
    return {
        "chave": chave,
        "emissao": emissao,
        "numero": numero,
        "serie": _tag(xml, "serie"),
        "tipo": "CT-e" if _tag(xml, "chCTe") else "NF-e",
        "valor": valor,
        # `cSitNFe` é a situação na Receita: 1 é autorizada, 3 é cancelada.
        # Traduzido aqui para a mesma palavra que o relatório do FSist usa,
        # senão a mesma nota teria dois status conforme a origem.
        "status": _situacao(_tag(xml, "cSitNFe") or _tag(xml, "cSitCTe")),
        "emitente_doc": emitente_doc,
        "emitente": _tag(xml, "xNome"),
        "emitente_uf": _tag(xml, "UF"),
        "destinatario_doc": "",
        "destinatario": "",
        "chaves_nfe": "",
    }


def _situacao(codigo: str) -> str:
    return {"1": "Autorizada", "2": "Denegada", "3": "Cancelada"}.get(
        str(codigo or "").strip(), "Autorizada")


# ---------------------------------------------------------------------------
# A CONVERSA COM A RECEITA
#
# TUDO O QUE FALA COM A REDE ESTÁ AQUI DENTRO, em duas funções, e isso é
# desenho e não acaso: é o único pedaço que não dá para exercitar sem o
# certificado de verdade. Todo o resto — o ponteiro, a leitura do documento, a
# gravação, o laço — é testado com a resposta dublada.
# ---------------------------------------------------------------------------
def _consultar_nfe(cnpj: str, desde_nsu: str) -> dict:
    """Um lote de NF-e, a partir do NSU informado.

    Usa a `erpbrasil.edoc` — código de terceiro, usado há anos por muita
    empresa. É ela que assina o pedido com o certificado, e assinatura digital
    é justamente onde escrever do zero custa caro: o erro volta como "recusado"
    sem dizer por quê."""
    from erpbrasil.edoc.nfe import NFe
    from erpbrasil.transmissao import TransmissaoSOAP

    with TransmissaoSOAP(_certificado(cnpj)) as transmissao:
        servico = NFe(transmissao, UF, ambiente=AMBIENTE)
        retorno = servico.consultar_distribuicao(
            cnpj_cpf=re.sub(r"\D", "", cnpj), ultimo_nsu=_nsu(desde_nsu))
    return _ler_resposta(getattr(retorno, "retorno", retorno))


def _consultar_cte(cnpj: str, desde_nsu: str) -> dict:
    """Um lote de CT-e.

    ⚠️ A BIBLIOTECA NÃO COBRE ESTE, e por isso o pedido é montado aqui. O que
    ela continua fazendo é o mais difícil: abrir o certificado e falar HTTPS
    autenticado com a Receita. O que se monta à mão é só o envelope.

    ⚠️ **Este caminho NUNCA foi exercitado contra o serviço de verdade** — não
    há certificado fora do Render. Ele é chamado separado do de NF-e de
    propósito: se recusar, as NF-e do dia continuam entrando."""
    import requests
    from erpbrasil.transmissao import TransmissaoSOAP

    pedido = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">'
        "<soap12:Body>"
        '<cteDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/cte/wsdl/'
        'CTeDistribuicaoDFe">'
        "<cteDadosMsg>"
        '<distDFeInt xmlns="http://www.portalfiscal.inf.br/cte" versao="1.00">'
        f"<tpAmb>{AMBIENTE}</tpAmb>"
        f"<cUFAutor>{_codigo_uf(UF)}</cUFAutor>"
        f"<CNPJ>{re.sub(r'[^0-9]', '', cnpj)}</CNPJ>"
        f"<distNSU><ultNSU>{_nsu(desde_nsu)}</ultNSU></distNSU>"
        "</distDFeInt>"
        "</cteDadosMsg></cteDistDFeInteresse></soap12:Body></soap12:Envelope>")

    transmissao = TransmissaoSOAP(_certificado(cnpj))
    sessao = getattr(transmissao, "session", None) or requests.Session()
    resposta = sessao.post(
        URL_CTE_DISTRIBUICAO, data=pedido.encode("utf-8"), timeout=60,
        headers={"Content-Type": "application/soap+xml; charset=utf-8"})
    resposta.raise_for_status()
    return _ler_resposta(resposta.text)


def _codigo_uf(sigla: str) -> str:
    """O código que a Receita usa para o estado. PE é 26."""
    return {"AC": "12", "AL": "27", "AM": "13", "AP": "16", "BA": "29",
            "CE": "23", "DF": "53", "ES": "32", "GO": "52", "MA": "21",
            "MG": "31", "MS": "50", "MT": "51", "PA": "15", "PB": "25",
            "PE": "26", "PI": "22", "PR": "41", "RJ": "33", "RN": "24",
            "RO": "11", "RR": "14", "RS": "43", "SC": "42", "SE": "28",
            "SP": "35", "TO": "17"}.get(str(sigla or "").upper(), "26")


def _ler_resposta(bruto) -> dict:
    """A resposta da Receita virando {motivo, ultimo_nsu, maior_nsu, documentos}.

    Aceita texto ou objeto: a biblioteca devolve um objeto para NF-e, e o CT-e
    volta como XML cru. As duas formas viram texto aqui, e daí em diante o
    caminho é um só — o que permite testar os dois com a mesma dublagem."""
    texto = bruto if isinstance(bruto, str) else _como_texto(bruto)
    motivo = _tag(texto, "xMotivo") or _tag(texto, "cStat")
    documentos = []
    for compactado in re.findall(r"<docZip[^>]*>([^<]+)</docZip>", texto):
        try:
            lido = ler_documento(_descompactar(compactado))
        except Exception:  # noqa: BLE001 — um documento torto não derruba o lote
            logger.exception("Análise de SPs: documento ilegível no lote")
            continue
        if lido:
            documentos.append(lido)
    return {
        "codigo": _tag(texto, "cStat"),
        "motivo": motivo,
        "ultimo_nsu": _tag(texto, "ultNSU"),
        "maior_nsu": _tag(texto, "maxNSU"),
        "documentos": documentos,
    }


def _como_texto(objeto) -> str:
    for atributo in ("retorno", "resposta", "text", "content"):
        valor = getattr(objeto, atributo, None)
        if isinstance(valor, bytes):
            return valor.decode("utf-8", "replace")
        if isinstance(valor, str):
            return valor
    return str(objeto)


# ---------------------------------------------------------------------------
# O LAÇO — é aqui que a busca acontece
# ---------------------------------------------------------------------------
# Os códigos que a Receita devolve e que este módulo precisa distinguir. Cada
# um pede uma coisa diferente de quem cuida do sistema, e todos chegam como
# "não veio nota":
CODIGO_LOTE_OK = "138"        # há documentos no lote
CODIGO_SEM_NOVIDADE = "137"   # não há nada novo — o esperado na maioria dos dias
CODIGO_CONSUMO_INDEVIDO = "656"   # consultou demais: esperar

RECADOS = {
    CODIGO_SEM_NOVIDADE: "Nenhuma nota nova desde a última consulta.",
    CODIGO_CONSUMO_INDEVIDO: (
        "A Receita pediu para esperar — foram consultas demais em pouco tempo. "
        "A próxima rodada tenta de novo; não há nada a fazer."),
}


def buscar_um(cnpj: str, tipo: str, anotar=None) -> dict:
    """Traz os lotes pendentes de UM CNPJ e UM tipo, e guarda as notas.

    PARA QUANDO A RECEITA DIZ QUE ACABOU, e não quando um teto é atingido: o
    teto de lotes é rede de segurança, não o critério. Insistir depois do
    "não há nada novo" é o caminho curto para o bloqueio por consulta demais.
    """
    from .db import conexao
    from .sincronizacao import _gravar_notas

    anotar = anotar or (lambda *a, **k: None)
    consultar = _consultar_nfe if tipo == NFE else _consultar_cte
    onde = ponteiro(cnpj, tipo)
    nsu = onde["ultimo_nsu"]
    trazidas, lotes = 0, 0

    for _ in range(LOTES_POR_RODADA):
        anotar("buscando notas na Receita",
               f"{cnpj[:8]}… {tipo}, lote {lotes + 1}")
        try:
            resposta = consultar(cnpj, nsu)
        except SemCertificado:
            raise
        except Exception as e:  # noqa: BLE001 — a falha vira recado, não queda
            logger.exception("Análise de SPs: falhou consultar %s de %s",
                             tipo, cnpj)
            gravar_ponteiro(cnpj, tipo, nsu, onde["maior_nsu"], str(e)[:400])
            return {"trazidas": trazidas, "lotes": lotes, "erro": str(e)}

        lotes += 1
        documentos = resposta["documentos"]
        if documentos:
            with conexao() as conn:
                _gravar_notas(conn, documentos)
            trazidas += len(documentos)

        codigo = resposta.get("codigo") or ""
        recado = RECADOS.get(codigo) or resposta.get("motivo") or ""
        novo_nsu = resposta.get("ultimo_nsu") or nsu
        gravar_ponteiro(cnpj, tipo, novo_nsu,
                        resposta.get("maior_nsu") or onde["maior_nsu"],
                        recado, len(documentos))

        # Acabou? Três jeitos de saber, e todos precisam ser respeitados:
        if codigo != CODIGO_LOTE_OK:
            break                      # a Receita disse que não há mais
        if _nsu(novo_nsu) <= _nsu(nsu):
            break                      # o ponteiro não andou: não há mais
        nsu = novo_nsu
        if _nsu(nsu) >= _nsu(resposta.get("maior_nsu") or "0"):
            break                      # chegou no fim da fila dela

    logger.info("Análise de SPs: Receita — %s de %s: %d nota(s) em %d lote(s).",
                tipo, cnpj, trazidas, lotes)
    return {"trazidas": trazidas, "lotes": lotes, "erro": ""}


def buscar_tudo(anotar=None) -> dict:
    """Percorre todos os CNPJs vigiados, NF-e e CT-e. É o que a rotina chama.

    NF-E E CT-E SÃO PEDIDOS SEPARADOS, e a falha de um não leva o outro: o
    caminho de CT-e nunca foi exercitado contra o serviço de verdade, e não
    pode derrubar a busca de NF-e, que é a maior parte do volume."""
    anotar = anotar or (lambda *a, **k: None)
    if not configurado():
        return {"trazidas": 0, "erro": "sem certificado", "por_cnpj": []}

    total, por_cnpj = 0, []
    for cnpj in cnpjs_vigiados():
        for tipo in (NFE, CTE):
            try:
                resultado = buscar_um(cnpj, tipo, anotar)
            except SemCertificado as e:
                return {"trazidas": total, "erro": str(e), "por_cnpj": por_cnpj}
            except Exception as e:  # noqa: BLE001
                logger.exception("Análise de SPs: falhou a busca %s/%s",
                                 cnpj, tipo)
                resultado = {"trazidas": 0, "erro": str(e)}
            total += resultado.get("trazidas", 0)
            por_cnpj.append({"cnpj": cnpj, "tipo": tipo, **resultado})
    return {"trazidas": total, "erro": "", "por_cnpj": por_cnpj}
