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

DE QUEM É CADA PEDAÇO — e isto mudou em 16/09/2026, depois de cinco defeitos
seguidos:

  - **Abrir o certificado A1** é da `erpbrasil.assinatura`. É a parte difícil e
    perigosa (chave privada, formatos, senha), e ela faz isso há anos para
    muita gente. Continua com ela.
  - **Os dois pedidos de distribuição — NF-e e CT-e — são montados aqui.** São
    dez linhas de envelope cada um, e o pedido de distribuição NÃO é assinado:
    quem autentica é o certificado da conexão.

A NF-e usava a `erpbrasil.edoc` e o preço apareceu: cinco defeitos em série
neste único caminho, um escondendo o outro, o último deles dentro da própria
biblioteca (`name 'distDFeInt' is not defined` — ela engole o erro de importação
dos módulos de XML e só falha lá na frente, dizendo outra coisa). O caminho de
CT-e, montado à mão, funcionava em produção o tempo todo. Então o de NF-e passou
a ser igual.
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

# Os dois endereços nacionais da distribuição de documentos. Os DOIS ficam
# aqui, e isso mudou em 16/09/2026: o de NF-e era resolvido pela biblioteca, e
# foi ela que quebrou em produção (ver `_consultar_nfe`).
URL_CTE_DISTRIBUICAO = (
    "https://www1.cte.fazenda.gov.br/CTeDistribuicaoDFe/CTeDistribuicaoDFe.asmx")
URL_NFE_DISTRIBUICAO = (
    "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx"
    if AMBIENTE == "1" else
    "https://hom.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx")


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

    # ⚠️ EM BASE64, E NÃO CRU — e este foi o defeito que manteve a busca na
    # Receita SEM FUNCIONAR desde que ela existe.
    #
    # O dono relatou em 13/09/2026, com a mensagem da tela na mão: *"o
    # certificado continua em falha: tentou e NÃO conseguiu — Certificado ou
    # senha inválida. Ele pode ter vencido."* E a desconfiança dele estava
    # certa desde o começo: **a senha estava certa e o certificado estava
    # válido**.
    #
    # A CAUSA, lida no código da `erpbrasil` e confirmada aqui: o construtor
    # dela, ao receber `bytes`, chama `base64.b64decode` em cima — ele assume
    # que bytes significa "conteúdo em base64". Mandando o `.pfx` cru, a
    # biblioteca decodificava lixo, o `load_key_and_certificates` levantava
    # `ValueError`, e ela traduzia isso para "Certificado ou senha
    # inválida!!!".
    #
    # Ou seja: a mensagem acusava a senha, e o erro era de quem chamava. Pior
    # tipo de erro — mandou o dono procurar no lugar errado por dias.
    #
    # MEDIDO com um .pfx de verdade e a senha certa:
    #     Certificado(bruto, senha) .................. CertificadoSenhaInvalida
    #     Certificado(base64encode(bruto), senha) .... abriu
    try:
        return Certificado(base64.b64encode(conteudo), senha)
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


# ⚠️ A TENTATIVA QUE FALHOU TAMBÉM DEIXA RASTRO — 13/09/2026
#
# Relato do dono, e é o retrato exato do buraco: *"o certificado continua uma
# incógnita. Eu coloco o certificado, boto a senha, ele aceita, eu acho que a
# senha está certa, o certificado está válido. Mas simplesmente nada é feito,
# nada é executado, e eu não sei o que está acontecendo."*
#
# A tela lia SÓ o ponteiro, e o ponteiro só era escrito quando a busca DAVA
# CERTO. Uma busca que falhasse — certificado recusado, rede bloqueada, a
# Receita fora do ar, "consumo indevido" — não escrevia nada em lugar nenhum
# que ele pudesse ver, e a tela continuava dizendo "a busca nunca rodou".
#
# Ou seja: o caso em que ele MAIS precisa saber o que houve era justamente o
# único que não contava nada. Agora a falha grava a hora e o motivo, sem mexer
# no NSU — o ponteiro continua sendo só do que foi lido de verdade.
def registrar_falha(cnpj: str, tipo: str, motivo: str) -> None:
    """Anota que houve tentativa e por que ela não deu certo."""
    from .db import conexao

    try:
        with conexao() as conn:
            conn.execute(
                "INSERT INTO analisesps.sefaz_ponteiro "
                "  (cnpj, tipo, ultimo_nsu, maior_nsu, consultado_em, "
                "   ultimo_recado, documentos) "
                "VALUES (?, ?, '0', '0', now(), ?, 0) "
                "ON CONFLICT (cnpj, tipo) DO UPDATE SET "
                "  consultado_em = now(), ultimo_recado = EXCLUDED.ultimo_recado",
                (re.sub(r"\D", "", cnpj), tipo,
                 ("FALHOU: " + str(motivo or "sem detalhe"))[:500]))
            conn.commit()
    except Exception:  # noqa: BLE001 — anotar a falha não pode virar outra falha
        logger.exception("Análise de SPs: não consegui anotar a falha da busca")


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

    # ⚠️ EVENTO TEM CHAVE E NÃO É NOTA. Foi este o defeito que a produção
    # acusou em 15/09/2026, depois que a busca finalmente andou:
    #
    #     invalid input syntax for type date: ""
    #     invalid input syntax for type numeric: ""
    #
    # O cancelamento, a carta de correção e o "ciência da operação" vêm no
    # MESMO lote das notas e carregam o `chNFe` da nota a que se referem. Como
    # o teste aqui era só o tamanho da chave, o evento passava por nota — e
    # chegava à gravação sem data de emissão e sem valor, que são as duas
    # colunas com tipo no banco. O lote inteiro morria aí, e o ponteiro não
    # andava: a busca ficava presa no mesmo lote para sempre.
    #
    # O evento NÃO é lixo — o de cancelamento é a notícia mais importante que
    # esta busca traz. Ele é lido por `ler_evento` e vira correção de status.
    if e_evento(xml):
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
# OS EVENTOS — o que não é nota, mas fala sobre uma nota
#
# A Receita entrega, no mesmo lote das notas, os eventos ligados a elas:
# cancelamento, carta de correção, ciência da operação, manifestação do
# destinatário. Todos carregam o `chNFe`/`chCTe` da nota a que se referem, e
# nenhum tem data de emissão ou valor.
#
# O CANCELAMENTO É A NOTÍCIA MAIS IMPORTANTE QUE ESTA BUSCA TRAZ: significa
# despesa paga contra documento que não existe mais. Por isso ele não é
# descartado com o resto — vira correção do status da nota que já está aqui.
# ---------------------------------------------------------------------------
# 110111 é o cancelamento da NF-e e do CT-e; 110112, o cancelamento por
# substituição do CT-e. A carta de correção (110110) NÃO cancela nada.
EVENTOS_DE_CANCELAMENTO = {"110111", "110112"}


def e_evento(xml: str) -> bool:
    """O documento é um evento, e não uma nota.

    Olha o que só existe em evento (`tpEvento`, `descEvento`, `nSeqEvento`), e
    não a ausência de data ou de valor: nota emitida com campo em branco é
    coisa de quem emitiu, e continuaria sendo nota."""
    return bool(_tag(xml, "tpEvento") or _tag(xml, "descEvento")
                or _tag(xml, "nSeqEvento"))


def ler_evento(xml: str) -> dict | None:
    """Um evento da Receita virando {chave, cancela, tipo, descricao}.

    Devolve None para o que não for evento com chave legível."""
    from . import fiscal

    if not e_evento(xml):
        return None
    chave = fiscal.so_digitos(_tag(xml, "chNFe") or _tag(xml, "chCTe"))
    if len(chave) != 44:
        return None
    tipo = re.sub(r"\D", "", _tag(xml, "tpEvento"))
    descricao = _tag(xml, "descEvento") or _tag(xml, "xEvento")
    # O código manda; a descrição é rede de segurança para o dia em que vier
    # sem ele — e é comparada sem acento, que aparece dos dois jeitos.
    sem_acento = (descricao.lower()
                  .replace("ç", "c").replace("ã", "a").replace("á", "a"))
    return {"chave": chave, "tipo": tipo, "descricao": descricao,
            "cancela": tipo in EVENTOS_DE_CANCELAMENTO
                       or "cancelamento" in sem_acento}


def aplicar_cancelamentos(conn, eventos) -> int:
    """Marca como Cancelada a nota que um evento cancelou. Devolve quantas.

    SÓ MEXE NO QUE JÁ ESTÁ AQUI: o evento não traz emitente, valor nem data,
    então inserir uma linha a partir dele criaria uma nota fantasma — pior do
    que não ter a notícia. Se a nota chegar depois, ela chega com o status
    certo da própria Receita.

    E só grava quando MUDA de verdade (`IS DISTINCT FROM`), pelo motivo de
    sempre: regravar o mesmo valor deixa lixo que engorda a tabela."""
    chaves = [e["chave"] for e in (eventos or []) if e and e.get("cancela")]
    if not chaves:
        return 0
    mudadas = 0
    for chave in chaves:
        cur = conn.execute(
            "UPDATE analisesps.notas_fiscais SET status = 'Cancelada', "
            "       importada_em = now() "
            " WHERE chave = ? AND status IS DISTINCT FROM 'Cancelada'",
            (chave,))
        mudadas += max(0, cur.rowcount or 0)
        cur.close()
    conn.commit()
    if mudadas:
        logger.info("Análise de SPs: Receita — %d nota(s) marcada(s) como "
                    "cancelada(s) por evento.", mudadas)
    return mudadas


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

    ⚠️ MONTADO À MÃO, COMO O DE CT-E — mudou em 16/09/2026, e o motivo é a
    produção. A biblioteca vinha falhando em série neste caminho, cada vez por
    um motivo diferente, e o último foi dela mesma:

        name 'distDFeInt' is not defined

    Isso é um defeito DENTRO da `erpbrasil`: ela importa os módulos do XML
    dentro de um `with suppress(ImportError)` e, se qualquer um dos onze falhar
    no ambiente, os nomes simplesmente não existem — e o erro só aparece lá na
    frente, na hora de usar, como se fosse outra coisa. Não há o que consertar
    daqui, e não dá para saber, de fora, qual dos onze falha no Render.

    **O caminho de CT-e, montado à mão, funciona em produção há dias** — foi
    ele que trouxe os 112 documentos. O envelope da NF-e é o mesmo, com outro
    namespace e outro endereço. Então este passa a ser montado do mesmo jeito.

    O que a biblioteca continua fazendo é o que importa e é difícil: abrir o
    certificado A1 e apresentar a identidade na conexão. O pedido de
    distribuição NÃO é assinado — quem autentica é o certificado da conexão.

    A conta a favor: sai da frente uma dependência que já produziu CINCO
    defeitos seguidos neste único caminho, e entra um envelope de dez linhas
    que dá para ler inteiro."""
    import requests
    from erpbrasil.assinatura.certificado import ArquivoCertificado

    pedido = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">'
        "<soap12:Body>"
        '<nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/'
        'NFeDistribuicaoDFe">'
        "<nfeDadosMsg>"
        '<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">'
        f"<tpAmb>{AMBIENTE}</tpAmb>"
        f"<cUFAutor>{_codigo_uf(UF)}</cUFAutor>"
        f"<CNPJ>{re.sub(r'[^0-9]', '', cnpj)}</CNPJ>"
        f"<distNSU><ultNSU>{_nsu(desde_nsu)}</ultNSU></distNSU>"
        "</distDFeInt>"
        "</nfeDadosMsg></nfeDistDFeInteresse></soap12:Body></soap12:Envelope>")

    # O certificado vai NA CONEXÃO — é assim que a Receita sabe quem pergunta.
    # `ArquivoCertificado` grava chave e certificado em arquivos temporários e
    # os apaga ao sair do bloco; é o mesmo caminho que a biblioteca usa por
    # dentro, e o mesmo que o CT-e usa aqui do lado.
    with ArquivoCertificado(_certificado(cnpj), "w") as (chave, certificado):
        sessao = requests.Session()
        sessao.cert = (chave, certificado)
        resposta = sessao.post(
            URL_NFE_DISTRIBUICAO, data=pedido.encode("utf-8"), timeout=60,
            headers={"Content-Type": "application/soap+xml; charset=utf-8"})
    resposta.raise_for_status()
    return _ler_resposta(resposta.text)


def _consultar_cte(cnpj: str, desde_nsu: str) -> dict:
    """Um lote de CT-e.

    ⚠️ A BIBLIOTECA NÃO COBRE ESTE, e por isso o pedido é montado aqui. O que
    ela continua fazendo é o mais difícil: abrir o certificado e falar HTTPS
    autenticado com a Receita. O que se monta à mão é só o envelope.

    ⚠️ **Este caminho NUNCA foi exercitado contra o serviço de verdade** — não
    há certificado fora do Render. Ele é chamado separado do de NF-e de
    propósito: se recusar, as NF-e do dia continuam entrando."""
    import requests
    from erpbrasil.assinatura.certificado import ArquivoCertificado

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

    # ⚠️ O CERTIFICADO PRECISA IR NA CONEXÃO, e não ir era o defeito: a
    # produção respondeu *"403 Client Error: Forbidden"* em 14/09/2026.
    #
    # A `session` que a `TransmissaoSOAP` guarda é uma sessão COMUM — o
    # certificado só é preso a ela dentro do método `cliente()`, que grava a
    # chave e o certificado em arquivos temporários. Postando pela sessão
    # crua, a Receita via um visitante sem identidade e recusava. É a mesma
    # família do defeito do base64: usar a biblioteca de um jeito que ela não
    # suporta, e a mensagem de erro apontando para outro lugar.
    #
    # `ArquivoCertificado` é o mesmo caminho que a biblioteca usa por dentro, e
    # apaga os arquivos ao sair do bloco.
    with ArquivoCertificado(_certificado(cnpj), "w") as (chave, certificado):
        sessao = requests.Session()
        sessao.cert = (chave, certificado)
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
    documentos, eventos = [], []
    ilegiveis, nem_nota_nem_evento = 0, 0
    for compactado in re.findall(r"<docZip[^>]*>([^<]+)</docZip>", texto):
        try:
            xml = _descompactar(compactado)
            # DUAS COISAS VÊM NO MESMO LOTE, e a diferença importa: a nota é
            # gravada, o evento corrige o status de uma nota que já está aqui.
            lido = ler_documento(xml)
            evento = ler_evento(xml) if lido is None else None
        except Exception:  # noqa: BLE001 — um documento torto não derruba o lote
            logger.exception("Análise de SPs: documento ilegível no lote")
            ilegiveis += 1
            continue
        if lido:
            documentos.append(lido)
        elif evento:
            eventos.append(evento)
        else:
            # ⚠️ O QUE NÃO É NOTA NEM EVENTO PRECISA SER CONTADO. Se um dia a
            # Receita mandar um formato que esta leitura não reconhece, sem
            # este número ele sumiria em silêncio — e a tela diria "recebi
            # tudo" tendo jogado fora metade.
            nem_nota_nem_evento += 1
    return {
        "codigo": _tag(texto, "cStat"),
        "motivo": motivo,
        "ultimo_nsu": _tag(texto, "ultNSU"),
        "maior_nsu": _tag(texto, "maxNSU"),
        "documentos": documentos,
        "eventos": eventos,
        "ilegiveis": ilegiveis,
        "nao_reconhecidos": nem_nota_nem_evento,
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


def _quantas_notas() -> int:
    """Quantas notas existem na tabela AGORA. É com isto que se sabe quantas
    das que a Receita mandou eram realmente novas."""
    from .db import consultar_um

    try:
        linha = consultar_um("SELECT count(*) FROM analisesps.notas_fiscais")
        return int(linha[0] or 0) if linha else 0
    except Exception:  # noqa: BLE001 — a contagem é acessório; a busca segue
        logger.exception("Análise de SPs: não consegui contar as notas")
        return 0


def _resumo_da_rodada(recebidos: int, novas: int, tipos: dict, datas: list,
                      eventos: int, perdidos: int = 0) -> str:
    """A frase que a tela mostra depois da busca.

    Os TRÊS números que faltavam: quantos documentos a Receita entregou,
    quantos viraram nota NOVA aqui, e de que tipo e período eles são. Sem o
    segundo, "112 documentos" se lê como "112 notas novas" — e foi exatamente
    assim que o dono leu, com razão."""
    partes = [f"{recebidos} documento(s) recebido(s) da Receita",
              f"{novas} nota(s) nova(s) aqui"]
    ja_tinha = max(0, recebidos - novas)
    if ja_tinha:
        partes.append(f"{ja_tinha} já estava(m) na base")
    if tipos:
        partes.append(" e ".join(f"{q} {nome}"
                                 for nome, q in sorted(tipos.items())))
    if datas:
        de, ate = min(datas), max(datas)
        def br(d):
            pedacos = d.split("-")
            return "/".join(reversed(pedacos)) if len(pedacos) == 3 else d
        partes.append(f"emissão de {br(de)} a {br(ate)}"
                      if de != ate else f"emissão em {br(de)}")
    if eventos:
        partes.append(f"{eventos} evento(s), que não são notas")
    if perdidos:
        partes.append(f"⚠️ {perdidos} documento(s) que não consegui ler — "
                      "me avise, é formato novo")
    return " · ".join(partes)


def buscar_um(cnpj: str, tipo: str, anotar=None) -> dict:
    """Traz os lotes pendentes de UM CNPJ e UM tipo, e guarda as notas.

    PARA QUANDO A RECEITA DIZ QUE ACABOU, e não quando um teto é atingido: o
    teto de lotes é rede de segurança, não o critério. Insistir depois do
    "não há nada novo" é o caminho curto para o bloqueio por consulta demais.
    """
    from .db import conexao
    from .sincronizacao import ORIGEM_RECEITA, _gravar_notas

    anotar = anotar or (lambda *a, **k: None)
    consultar = _consultar_nfe if tipo == NFE else _consultar_cte
    onde = ponteiro(cnpj, tipo)
    nsu = onde["ultimo_nsu"]
    trazidas, lotes, eventos_vistos = 0, 0, 0
    # ⚠️ "112 DOCUMENTOS" NÃO É "112 NOTAS NOVAS", e a tela deixava entender
    # que sim. Reclamação do dono em 15/09/2026: *"em Configurações diz 112
    # documentos; na planilha das notas, busca na Receita, só tem nove — e é
    # tudo frete."*
    #
    # Ele está certo em achar estranho, e a resposta é que os dois números
    # medem coisas diferentes: a Receita REENTREGA o histórico inteiro, e a
    # maior parte do que ela manda já estava aqui pelo relatório do FSist. O
    # que faltava era a tela dizer isso — quantas eram novas, de que tipo, e de
    # que período. Sem esses três números não dá para saber se a busca está
    # trazendo pouco ou se é a Receita que tem pouco para dar.
    antes_de_tudo = _quantas_notas()
    resumo_tipos: dict = {}
    datas: list = []
    perdidos = 0

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
            # ⚠️ FALHA TEM DE SE CHAMAR FALHA. Este caminho gravava o erro como
            # recado comum, e a tela mostrava "ainda há lote para buscar" com a
            # mensagem técnica do Python pendurada ao lado — como se fosse
            # informação, não defeito. Foi assim que o erro da UF ficou três
            # dias à vista sem ninguém (eu inclusive) tratar como falha.
            registrar_falha(cnpj, tipo, str(e))
            return {"trazidas": trazidas, "lotes": lotes, "erro": str(e)}

        lotes += 1
        documentos = resposta["documentos"]
        eventos = resposta.get("eventos") or []
        recusadas, canceladas = 0, 0
        if documentos or eventos:
            # ⚠️ A GRAVAÇÃO NÃO PODE DERRUBAR A BUSCA. Em 15/09/2026 um evento
            # que passou por nota estourou o lote inteiro no banco, e como o
            # ponteiro só anda DEPOIS da gravação, a busca ficou presa no mesmo
            # lote — repetindo o mesmo erro a cada rodada, para sempre.
            #
            # Agora a falha vira número e recado: o que dá para gravar é
            # gravado, o que não dá é contado, e o ponteiro anda.
            try:
                with conexao() as conn:
                    gravadas = _gravar_notas(conn, documentos,
                                             origem=ORIGEM_RECEITA)
                    canceladas = aplicar_cancelamentos(conn, eventos)
                recusadas = max(0, len(documentos) - gravadas)
                trazidas += gravadas
            except Exception as e:  # noqa: BLE001
                logger.exception("Análise de SPs: falhou gravar o lote de %s",
                                 cnpj)
                recusadas = len(documentos)
                resposta["motivo"] = (f"lote recebido, mas não consegui gravar: "
                                      f"{str(e)[:200]}")

        eventos_vistos += len(eventos)
        perdidos += (resposta.get("ilegiveis") or 0) + (
            resposta.get("nao_reconhecidos") or 0)
        for doc in documentos:
            rotulo = doc.get("tipo") or "?"
            resumo_tipos[rotulo] = resumo_tipos.get(rotulo, 0) + 1
            if doc.get("emissao"):
                datas.append(str(doc["emissao"])[:10])
        codigo = resposta.get("codigo") or ""
        recado = RECADOS.get(codigo) or resposta.get("motivo") or ""
        # O QUE ACONTECEU COM O LOTE VAI PARA A TELA, e não só para o log: é o
        # que ele lê em Configurações para saber se a busca está funcionando.
        #
        # O EVENTO CONTA, mesmo não virando nota. Sem isto, uma rodada inteira
        # de cancelamentos e cartas de correção aparece como "0 documento(s)" —
        # que se lê como "não veio nada", quando na verdade veio bastante coisa
        # e nada dela era nota.
        if eventos_vistos:
            recado = (f"{recado} · {eventos_vistos} evento(s) da Receita "
                      "(cancelamento, carta de correção) — não são notas"
                      ).strip(" ·")
        if canceladas:
            recado = (f"{recado} · {canceladas} nota(s) marcada(s) como "
                      "cancelada(s) por evento da Receita").strip(" ·")
        if recusadas:
            recado = (f"{recado} · {recusadas} documento(s) do lote não "
                      "entraram (formato inesperado)").strip(" ·")
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

    # O RESUMO DA RODADA, escrito onde ele lê. Vai num registro só no fim,
    # com `documentos=0` para não contar duas vezes o que os lotes já contaram.
    novas = max(0, _quantas_notas() - antes_de_tudo)
    if trazidas or eventos_vistos:
        gravar_ponteiro(cnpj, tipo, nsu, onde["maior_nsu"],
                        _resumo_da_rodada(trazidas, novas, resumo_tipos, datas,
                                          eventos_vistos, perdidos), 0)
    logger.info("Análise de SPs: Receita — %s de %s: %d documento(s), %d nota(s) "
                "nova(s), em %d lote(s).", tipo, cnpj, trazidas, novas, lotes)
    return {"trazidas": trazidas, "novas": novas, "lotes": lotes, "erro": ""}


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
                registrar_falha(cnpj, tipo, str(e))
                return {"trazidas": total, "erro": str(e), "por_cnpj": por_cnpj}
            except Exception as e:  # noqa: BLE001
                logger.exception("Análise de SPs: falhou a busca %s/%s",
                                 cnpj, tipo)
                # A FALHA VAI PARA O BANCO, não só para o log: o log ele não
                # lê, e é justamente aqui que ele fica sem saber o que houve.
                registrar_falha(cnpj, tipo, str(e))
                resultado = {"trazidas": 0, "erro": str(e)}
            total += resultado.get("trazidas", 0)
            por_cnpj.append({"cnpj": cnpj, "tipo": tipo, **resultado})
    return {"trazidas": total, "erro": "", "por_cnpj": por_cnpj}


def estado_das_buscas() -> list:
    """Em que pé está a busca na Receita, por CNPJ e por tipo de documento.

    Pergunta do dono em 13/09/2026: *"eu coloquei pra baixar notas mas não
    tenho nem ideia de que se baixou, se não baixou."*

    Ele está certo: o ponteiro sempre guardou tudo — até onde leu, quando, o
    recado da Receita e quantos documentos vieram — e NADA disso aparecia em
    tela nenhuma. Informação guardada e não mostrada é informação que não
    existe para quem usa."""
    from .db import consultar

    try:
        linhas = consultar(
            "SELECT cnpj, tipo, ultimo_nsu, maior_nsu, consultado_em, "
            "       ultimo_recado, documentos "
            "  FROM analisesps.sefaz_ponteiro ORDER BY cnpj, tipo")
    except Exception:      # noqa: BLE001 — migração 008 ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler o ponteiro da busca")
        return []

    nomes = ["cnpj", "tipo", "ultimo_nsu", "maior_nsu", "consultado_em",
             "ultimo_recado", "documentos"]
    saida = []
    for linha in linhas:
        estado = dict(zip(nomes, linha))
        # FALTA MUITO? É a diferença entre os dois contadores. Zero quer dizer
        # "está em dia"; um número grande quer dizer que a busca foi
        # interrompida no meio e vale rodar de novo.
        try:
            estado["faltam"] = max(0, int(estado["maior_nsu"] or 0)
                                   - int(estado["ultimo_nsu"] or 0))
        except (TypeError, ValueError):
            estado["faltam"] = 0
        # ⚠️ "EM DIA" É SOBRE A FILA DA RECEITA, e não sobre haver recado.
        # Antes, qualquer recado — inclusive "Nenhuma nota nova desde a última
        # consulta", que é a resposta BOA — fazia a tela dizer "ainda há lote
        # para buscar" com "Falta buscar: —" na coluna do lado. Duas células da
        # mesma linha se contradizendo é o tipo de coisa que faz quem lê
        # desconfiar da tela inteira, e com razão.
        estado["em_dia"] = estado["faltam"] == 0
        # A TENTATIVA QUE FALHOU FICA MARCADA COMO FALHA, e não como "em dia
        # com um recado estranho". É a diferença entre a tela dizer "rodou" e
        # dizer "tentei e não consegui, por isto aqui".
        estado["falhou"] = str(estado["ultimo_recado"] or "").startswith(
            "FALHOU: ")
        estado["motivo_da_falha"] = (
            str(estado["ultimo_recado"])[len("FALHOU: "):]
            if estado["falhou"] else "")
        if estado["falhou"]:
            estado["em_dia"] = False
        # ⚠️ RECADO NÃO É ERRO. A tela pintava de vermelho QUALQUER recado — e
        # o mais comum deles é "Nenhuma nota nova desde a última consulta",
        # que é a resposta boa. Alarme que toca no dia normal é alarme que se
        # aprende a ignorar, e aí o dia ruim passa despercebido.
        texto = str(estado["ultimo_recado"] or "").lower()
        estado["alerta"] = bool(estado["falhou"]) or any(
            marca in texto for marca in ("não entraram", "nao entraram",
                                         "não consegui", "nao consegui",
                                         "esperar", "falhou"))
        estado["rotulo_tipo"] = {"NFE": "Notas (NF-e)",
                                 "CTE": "Fretes (CT-e)"}.get(estado["tipo"],
                                                             estado["tipo"])
        saida.append(estado)
    return saida
