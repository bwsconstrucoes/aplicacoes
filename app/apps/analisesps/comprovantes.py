# -*- coding: utf-8 -*-
"""
Os comprovantes arrastados para dentro da tela.

O QUE ESTE ARQUIVO **NÃO** FAZ, e é a decisão que poupou um módulo inteiro:
dar baixa. O robô que faz isso já existe e roda em produção há meses — o
`baixabradesco` recebe o PDF, descobre a qual SP pertence, dá baixa no Omie,
marca a SP como paga na SPsBD, move o card no Pipefy e guarda o comprovante.

Hoje quem chama esse robô é um cenário do Make.com, alimentado por e-mail. O
dono pediu o caminho curto: *"eu arrasto esses comprovantes pra dentro e
dispara a automação, sem nem precisar passar pelo Make."* Então aqui há só a
porta de entrada, a divisão em levas e a MEMÓRIA do que aconteceu.

AS LEVAS DE DEZ, e o número não é chute meu: é o que o script do dono já faz
hoje, descrito por ele em 11/09/2026 — *"ele divide o PDF em dez páginas. Se eu
mandar cinquenta páginas num único PDF, ele quebra em cinco e manda um por um."*
Manter o mesmo tamanho tem uma razão a mais do que a simetria: é o tamanho de
lote que o robô já recebe há meses em produção, então não se está estreando
carga nova nele.

E A CONTA É POR PÁGINA, NÃO POR ARQUIVO. O robô trata cada página como um
comprovante separado. Cortar por quantidade de arquivos deixaria um PDF de
cinquenta páginas passar inteiro numa chamada só — exatamente o caso que as
levas existem para evitar.
"""
from __future__ import annotations

import io
import logging
import os

logger = logging.getLogger("analisesps.comprovantes")

# Páginas por leva. Ver o porquê no cabeçalho.
POR_LEVA = 10

# Teto por arquivo. A instância tem 2 GB divididos com 17 módulos e já morreu
# de falta de memória em julho de 2026 — um PDF sem teto derruba o serviço
# inteiro, não só esta tela.
MAXIMO_POR_ARQUIVO = 25 * 1024 * 1024      # 25 MB
MAXIMO_DE_ARQUIVOS = 20

# Onde o PDF espera a vez. NÃO vai para o banco: ele tem 1 GB e já usa 430 MB,
# e o comprovante já é guardado pelo robô no fim do processo.
PASTA = os.getenv("ANALISESPS_PASTA_COMPROVANTES", "/tmp/analisesps_comprovantes")


class ErroDeComprovante(RuntimeError):
    """Falha com a mensagem já pronta para a tela."""


# ---------------------------------------------------------------------------
# Partir o PDF
# ---------------------------------------------------------------------------
def contar_paginas(conteudo: bytes) -> int:
    """Quantas páginas tem o PDF. Zero quando não dá para ler."""
    try:
        from pypdf import PdfReader
        return len(PdfReader(io.BytesIO(conteudo)).pages)
    except Exception as e:  # noqa: BLE001 — arquivo torto não pode derrubar a tela
        # `warning`, e não `exception`: soltar um arquivo que não é PDF é erro
        # de quem arrasta, não defeito do serviço. Despejar um traceback no log
        # do Render a cada foto solta faria o log de verdade sumir no meio.
        logger.warning("Análise de SPs: arquivo não parece um PDF (%s)", e)
        return 0


def quantas_levas(paginas: int, por_leva: int = POR_LEVA) -> int:
    if paginas <= 0:
        return 0
    return (paginas + por_leva - 1) // por_leva


def separar_em_levas(conteudo: bytes, por_leva: int = POR_LEVA):
    """Devolve (primeira_pagina, bytes) de cada leva, uma de cada vez.

    É GERADOR de propósito, e isso é memória: um PDF de cinquenta páginas
    montado inteiro em cinco pedaços na memória seria o arquivo duas vezes.
    Assim só existe uma leva por vez.

    A numeração devolvida é a da PÁGINA NO ARQUIVO ORIGINAL (começando em 1) —
    é o que a tela mostra, e é o que permite a pessoa achar o comprovante no
    PDF que ela mesma soltou."""
    from pypdf import PdfReader, PdfWriter

    leitor = PdfReader(io.BytesIO(conteudo))
    total = len(leitor.pages)
    for inicio in range(0, total, por_leva):
        escritor = PdfWriter()
        for n in range(inicio, min(inicio + por_leva, total)):
            escritor.add_page(leitor.pages[n])
        saco = io.BytesIO()
        escritor.write(saco)
        yield inicio + 1, saco.getvalue()


# ---------------------------------------------------------------------------
# Ler o que o robô respondeu
#
# ELE JÁ SEPARA TUDO O QUE O DONO PEDIU — "esse deu certo, esse deu errado,
# esse tem duplicidade, esse faltou aquilo". A resposta traz `resumo`,
# `duplicados`, `recusados` e um `plano` por página, com o motivo escrito.
# Isso hoje volta para o Make.com e morre lá; aqui vira linha no banco.
# ---------------------------------------------------------------------------
BAIXADO = "BAIXADO"
DUPLICADO = "DUPLICADO"
NAO_LOCALIZADO = "NAO_LOCALIZADO"
PENDENTE_VALIDACAO = "PENDENTE_VALIDACAO"
RECUSADO = "RECUSADO"
ERRO = "ERRO"

ROTULOS = {
    BAIXADO: "Baixado",
    DUPLICADO: "Já tinha sido baixado",
    NAO_LOCALIZADO: "Não achei a SP",
    PENDENTE_VALIDACAO: "Falta liberar antes de baixar",
    RECUSADO: "Pagamento não efetivado",
    ERRO: "Deu erro",
}

# A ordem em que a tela mostra: primeiro o que pede ação, por último o que
# deu certo. Quem abre a tela quer saber o que ficou de fora — o que baixou é
# o esperado.
ORDEM = [ERRO, NAO_LOCALIZADO, PENDENTE_VALIDACAO, RECUSADO, DUPLICADO, BAIXADO]


def _texto(v) -> str:
    return "" if v is None else str(v).strip()


# AÇÕES QUE NÃO MEXEM EM SP NENHUMA. O robô as executa direto no Omie, sem
# card e sem planilha — é transferência entre contas e movimentação avulsa. A
# baixa é real, mas dizer só "Baixado" faria quem lê procurar a SP na planilha e
# não achar. Ver a linha de FERNANDO CARVALHO em 13/09/2026, que apareceu com
# "Baixado" e SP "—".
ACOES_SEM_SP = {
    "lancar_movimentacao_omie": "lançado no Omie como movimentação avulsa",
    "lancar_movimentacao_omie_sem_sp": "lançado no Omie como transferência",
}


# ⚠️ O ROBÔ SEMPRE DISSE POR QUE NÃO BAIXOU — ERA ESTA TELA QUE JOGAVA FORA
#
# Relato do dono em 14/09/2026, com quatro páginas na mão: *"baixam na planilha
# mas não baixam no Omie. Não tem sentido. (…) Por que que não está baixando no
# sistema Omie?"* — e as quatro linhas diziam a mesma frase vazia: *"Não recebi
# confirmação do Omie para esta baixa."*
#
# A FRASE ERA NOSSA, e escondia a resposta. O robô interrompe a sequência do
# Omie em vários pontos, e em CADA UM ele escreve o motivo:
#
#     skip ......................... "Título já consta PAGO no Omie."
#     abort ........................ "Título não encontrado no Omie. Inclua o
#                                     título primeiro."
#     abort_sem_credencial ......... "As credenciais do Omie não chegaram ao
#                                     robô: falta OMIE_BWS_APP_KEY no servidor."
#     abort_apos_alterar ........... "Falha ao alterar título. Baixa cancelada."
#     abort_apos_transferencia ..... "Falha na transferência. Baixa cancelada."
#     erro_baixa ................... "Falha ao lançar pagamento no Omie."
#
# Em TODOS eles não existe um passo `baixar` com `ok`, e a leitura antiga
# devolvia "não sei" — a tela então imprimia a frase genérica e a explicação de
# verdade morria dentro do JSON. Ele ficou dois dias procurando no lugar errado
# porque a tela não contava o que já sabia.
#
# E os dois primeiros casos são OPOSTOS: "já estava pago" não é erro nenhum;
# "título não encontrado" é trabalho para ele fazer no Omie. Tratar os dois com
# a mesma frase é pior do que não dizer nada.
def _leitura_do_omie(plano: dict) -> dict:
    """O que o Omie respondeu, com o motivo que o próprio robô escreveu.

    Devolve {'estado': …, 'motivo': …}. Os estados:
      'confirmou'  — houve passo `baixar` e ele voltou ok;
      'recusou'    — houve passo `baixar` e ele falhou;
      'ja_pago'    — o título já constava PAGO (não é erro);
      'parou'      — a sequência foi interrompida antes de baixar;
      'nao_chamou' — não houve chamada nenhuma ao Omie."""
    passos = (plano.get("responses") or {}).get("omie")
    if not passos:
        return {"estado": "nao_chamou", "motivo": ""}

    def passo_de(nome):
        for p in passos:
            if _texto((p or {}).get("step")).lower() == nome:
                return p or {}
        return None

    # ⚠️ A CAMADA DE BAIXO: A FRASE DO PRÓPRIO OMIE.
    #
    # Cobrança do dono em 14/09/2026, e ela reenquadrou o problema: *"marcar a
    # planilha só depois do Omie confirmar NÃO é resolver a causa raiz. A causa
    # raiz é saber POR QUE não está baixando no Omie, porque se eu estou
    # mandando pra baixar é pra baixar."*
    #
    # Ele está certo — e a resposta sempre esteve na mão. O Omie devolve o
    # motivo em `faultstring` ("título já baixado", "conta corrente não
    # encontrada", "valor maior que o saldo do título"…), e essa frase chega
    # inteira até aqui dentro de `response.body`. O robô a resume num genérico
    # "Falha ao lançar pagamento no Omie", e a tela mostrava o resumo.
    #
    # Resumo de erro é erro perdido. Quem vai agir precisa da frase do Omie.
    def frase_do_omie(passo):
        corpo = ((passo or {}).get("response") or {}).get("body") or {}
        return (_texto(corpo.get("faultstring"))
                or _texto(corpo.get("faultcode"))
                or _texto(((passo or {}).get("response") or {}).get("raw"))[:200])

    baixas = [p for p in passos
              if _texto((p or {}).get("step")).lower() == "baixar"]
    if baixas:
        if any(((p or {}).get("response") or {}).get("ok") for p in baixas):
            return {"estado": "confirmou", "motivo": ""}
        # A FRASE DO OMIE PRIMEIRO, o resumo do robô depois. É a do Omie que
        # diz o que fazer.
        do_omie = frase_do_omie(baixas[-1])
        resumo = _texto((passo_de("erro_baixa") or {}).get("motivo"))
        return {"estado": "recusou",
                "motivo": (f"O Omie respondeu: {do_omie}" if do_omie
                           else resumo)}

    if passo_de("skip"):
        return {"estado": "ja_pago",
                "motivo": _texto((passo_de("skip") or {}).get("motivo"))}

    # Qualquer interrupção anterior à baixa. Aqui também vale a frase do Omie:
    # o passo que falhou (consultar, alterar, transferir) traz a explicação
    # dele, e o resumo do robô só diz que parou.
    for nome, anterior in (("abort_sem_credencial", ""),
                           ("abort", "consultar"),
                           ("abort_apos_alterar", "alterar_se_necessario"),
                           ("abort_apos_transferencia", "")):
        parada = passo_de(nome)
        if parada:
            do_omie = frase_do_omie(passo_de(anterior)) if anterior else ""
            resumo = _texto(parada.get("motivo"))
            return {"estado": "parou",
                    "motivo": (f"{resumo} O Omie respondeu: {do_omie}"
                               if do_omie and resumo
                               else (do_omie or resumo))}

    # Último caso: a sequência acabou sem baixar e sem um passo de parada —
    # o motivo, se houver, está no último passo que falhou.
    falhos = [p for p in passos
              if not ((p or {}).get("response") or {}).get("ok")
              and (p or {}).get("response")]
    if falhos:
        do_omie = frase_do_omie(falhos[-1])
        if do_omie:
            return {"estado": "parou",
                    "motivo": (f"O Omie respondeu, no passo "
                               f"\"{_texto(falhos[-1].get('step'))}\": "
                               f"{do_omie}")}

    return {"estado": "parou", "motivo": ""}


def _situacao_do_plano(plano: dict, ensaio: bool = False) -> tuple[str, str]:
    """A situação de uma página e o motivo, em português.

    O robô responde em três níveis: o `match.status` diz se achou a SP, os
    `motivos_bloqueio` dizem por que não pôde executar, e as `responses` dizem
    o que cada sistema respondeu. A tela precisa de UMA frase.

    ⚠️ "PODE EXECUTAR" NÃO É "EXECUTOU", e confundir os dois foi o defeito de
    13/09/2026: a tela lia o plano e anunciava baixa. Agora "Baixado" só sai
    quando houve execução de verdade — e quando o Omie recusou, o que aparece é
    o erro dele, não um "Baixado" por cima."""
    casamento = plano.get("match") or {}
    status = _texto(casamento.get("status")).lower()
    motivos = [_texto(m) for m in (plano.get("motivos_bloqueio") or []) if _texto(m)]
    motivo = motivos[0] if motivos else _texto(casamento.get("motivo"))
    acao = _texto(plano.get("acao"))

    if status == "nao_localizado" and acao not in ACOES_SEM_SP:
        return NAO_LOCALIZADO, motivo or "Não encontrei a SP deste comprovante."
    if status == "pendente_validacao":
        return PENDENTE_VALIDACAO, motivo or "A SP ainda não está liberada para baixa."
    if not plano.get("pode_executar"):
        return ERRO, motivo or "Não foi possível dar baixa."

    # O PLANO ESTAVA BOM. Agora: ele chegou a ser executado?
    if ensaio:
        return ERRO, ("O robô rodou em modo de ENSAIO e não gravou nada. "
                      "Nada foi baixado — reenvie este comprovante.")

    omie = _leitura_do_omie(plano)

    # ⚠️ O QUE ACONTECEU COM A PLANILHA E COM O CARD, dito pelo próprio robô.
    #
    # Antes esta frase dizia "PODEM ter sido marcados", porque a planilha era
    # atualizada sem esperar o Omie — e ninguém sabia. Desde 16/09/2026 o robô
    # NÃO marca quando o Omie não confirma, e avisa que não marcou. Quando esse
    # aviso vem, a frase deixa de ser "pode ser" e passa a ser certeza.
    nao_marcou = _texto((plano.get("responses") or {}).get("nao_marcou"))
    aviso_planilha = (
        " " + nao_marcou if nao_marcou else
        " ⚠️ A planilha e o card PODEM ter sido marcados como pagos mesmo "
        "assim. Confira o título no Omie antes de reenviar.")

    if omie["estado"] == "recusou":
        return ERRO, (omie["motivo"] or "O Omie recusou a baixa.") + aviso_planilha

    if acao in ACOES_SEM_SP:
        # Baixa real, mas sem SP: dizer isso evita que ele vá procurar a SP na
        # planilha e conclua que o sistema mentiu.
        return BAIXADO, (ACOES_SEM_SP[acao].capitalize()
                         + ". Não há SP para marcar como paga na planilha.")

    # ⚠️ "JÁ ESTAVA PAGO" NÃO É ERRO, e chamar de erro faz ele reenviar um
    # comprovante que não precisa — e reenviar é o caminho para pagar duas
    # vezes. É a mesma família do "não juntar errado" da conciliação fiscal.
    if omie["estado"] == "ja_pago":
        return BAIXADO, (omie["motivo"]
                         or "O título já constava PAGO no Omie.")

    if omie["estado"] == "parou":
        return ERRO, (omie["motivo"]
                      or "A baixa parou antes de chegar ao Omie.") + aviso_planilha

    if omie["estado"] == "nao_chamou":
        return ERRO, ("O robô NÃO chegou a chamar o Omie para esta página"
                      + (f" (ação: {acao})" if acao else "")
                      + ". Nada foi baixado lá. Confira a planilha antes de "
                      "reenviar.")

    # A PLANILHA É ATUALIZADA EM SEGUNDO PLANO pelo robô, então a resposta dele
    # não diz se ela já mudou. Não se promete o que não se sabe.
    return BAIXADO, "Baixa confirmada no Omie. A planilha é atualizada logo em seguida."


# Quanto da conversa com o Omie fica guardado. Duas mil letras pegam os três
# passos com folga; o que passar disso é repetição de cabeçalho.
TETO_DA_CONVERSA = 4000


def _conversa_com_o_omie(plano: dict) -> str:
    """A sequência de passos do Omie em texto, para ler depois.

    Guarda o passo, se deu certo, e a frase que o Omie respondeu. Não guarda o
    pedido — ele tem o valor e a conta, e o que falta responder é o "por quê",
    que vem na resposta."""
    passos = ((plano or {}).get("responses") or {}).get("omie") or []
    if not passos:
        return ""

    linhas = []
    for p in passos:
        p = p or {}
        resposta = p.get("response") or {}
        corpo = resposta.get("body") or {}
        frase = (_texto(corpo.get("faultstring")) or _texto(corpo.get("faultcode"))
                 or _texto(p.get("motivo")))
        if "response" in p:
            estado = "ok" if resposta.get("ok") else "FALHOU"
            http = resposta.get("status")
            linhas.append(f"{_texto(p.get('step'))}: {estado}"
                          + (f" (HTTP {http})" if http else "")
                          + (f" — {frase}" if frase else ""))
        else:
            linhas.append(f"{_texto(p.get('step'))}: {frase}")
    return "\n".join(linhas)[:TETO_DA_CONVERSA]


def ler_resposta(resposta: dict, primeira_pagina: int = 1) -> list[dict]:
    """A resposta do robô virando linhas para o banco, uma por comprovante.

    `primeira_pagina` desloca a numeração: dentro da leva a página 1 é a
    primeira DAQUELA leva, e quem olha a tela precisa do número da página no
    arquivo que ele soltou."""
    resposta = resposta or {}
    linhas: list[dict] = []

    def acrescentar(situacao, bruto, motivo=""):
        recibo = (bruto or {}).get("receipt") or bruto or {}
        try:
            pagina = int(recibo.get("page") or 0)
        except (TypeError, ValueError):
            pagina = 0
        # ⚠️ A SP VEM DE DOIS LUGARES, e a tela lia só um. Relato do dono em
        # 16/09/2026: a linha do erro mostrava "SP: —" e mandava "confira o
        # título no Omie" — sem dizer QUAL título.
        #
        # O número que o comprovante traz impresso (`id_pipefy`) só existe em
        # alguns bancos. O número que a conciliação ENCONTROU fica em
        # `match.id`, e é justamente o que interessa quando algo deu errado:
        # é por ele que se acha o título no Omie e o card no Pipefy.
        achado = (bruto or {}).get("match") or {}
        linhas.append({
            "pagina": (primeira_pagina + pagina - 1) if pagina else None,
            "situacao": situacao,
            "sp_id": (_texto(recibo.get("id_pipefy"))
                      or _texto(achado.get("id"))),
            "valor": _texto(recibo.get("valor_pago")),
            "recebedor": _texto(recibo.get("nome_recebedor"))[:120],
            "motivo": _texto(motivo)[:500],
            # ⚠️ A CONVERSA COM O OMIE, GUARDADA. Ver a migração 013 e o
            # `_conversa_com_o_omie`: enquanto a falha não deixava rastro, ela
            # era invisível — foi assim com o certificado digital, dois dias
            # antes, e a lição custou dois dias dele.
            "conversa_omie": _conversa_com_o_omie(bruto),
        })

    for item in resposta.get("duplicados") or []:
        acrescentar(DUPLICADO, item,
                    _texto(item.get("motivo"))
                    or "Este comprovante já tinha sido baixado antes.")
    for item in resposta.get("recusados") or []:
        acrescentar(RECUSADO, item,
                    _texto(item.get("motivo"))
                    or "O pagamento não consta como efetivado.")
    # ⚠️ A TRAVA QUE TERIA PEGADO O DEFEITO DE 13/09/2026 NO PRIMEIRO DIA.
    # O robô devolve no corpo da resposta se rodou em ensaio. Se rodou, NADA
    # foi gravado — e nenhuma página pode ser anunciada como baixada, por mais
    # que o plano dela estivesse perfeito.
    ensaio = bool(resposta.get("modo_teste"))
    if ensaio:
        logger.error("Análise de SPs: o robô respondeu em MODO DE ENSAIO — "
                     "nenhuma baixa foi gravada. Confira o pedido enviado.")

    for plano in resposta.get("planos") or []:
        situacao, motivo = _situacao_do_plano(plano, ensaio=ensaio)
        acrescentar(situacao, plano, motivo)

    return linhas


# ---------------------------------------------------------------------------
# Guardar o arquivo até a vez dele
# ---------------------------------------------------------------------------
def guardar(conteudo: bytes, nome: str, pessoa: str, quem: str) -> int:
    """Põe o arquivo em disco e abre o lote. Devolve o número do lote.

    O lote nasce ESPERANDO: quem faz o trabalho é o processo separado, porque
    a baixa fala com Omie, Pipefy, Sheets e Dropbox e leva minutos — dentro do
    worker ela seria morta pelo reinício do gunicorn, como já aconteceu três
    vezes com a carga da planilha (ver `executar_sync.py`)."""
    from .db import conexao

    if not conteudo:
        raise ErroDeComprovante("O arquivo chegou vazio.")
    if len(conteudo) > MAXIMO_POR_ARQUIVO:
        raise ErroDeComprovante(
            f"{nome}: são no máximo {MAXIMO_POR_ARQUIVO // (1024 * 1024)} MB "
            "por arquivo. Divida o PDF e mande em duas vezes.")

    paginas = contar_paginas(conteudo)
    if not paginas:
        raise ErroDeComprovante(
            f"{nome}: não consegui ler este PDF. Ele está protegido por senha "
            "ou veio corrompido?")

    os.makedirs(PASTA, exist_ok=True)
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.comprovantes_lote "
            "  (pessoa, quem, arquivo, caminho, paginas, levas, situacao) "
            "VALUES (?, ?, ?, '', ?, ?, 'ESPERANDO') RETURNING id",
            (pessoa or "", quem or "", nome[:200], paginas,
             quantas_levas(paginas)))
        lote_id = cur.fetchone()[0]
        cur.close()
        caminho = os.path.join(PASTA, f"lote-{lote_id}.pdf")
        with open(caminho, "wb") as arquivo:
            arquivo.write(conteudo)
        conn.execute(
            "UPDATE analisesps.comprovantes_lote SET caminho = ? WHERE id = ?",
            (caminho, lote_id))
        conn.commit()

    logger.info("Análise de SPs: comprovante %r recebido — lote %d, %d página(s), "
                "%d leva(s).", nome, lote_id, paginas, quantas_levas(paginas))
    return lote_id


def _apagar_arquivo(caminho: str) -> None:
    """O PDF sai do disco assim que o lote termina.

    O comprovante já foi guardado pelo robô no destino definitivo; o que fica
    aqui é cópia de passagem, e cópia de passagem que não é apagada vira disco
    cheio sem ninguém perceber."""
    try:
        if caminho and os.path.exists(caminho):
            os.remove(caminho)
    except OSError:
        logger.exception("Análise de SPs: não consegui apagar %r", caminho)


# ---------------------------------------------------------------------------
# O trabalho
# ---------------------------------------------------------------------------
def _mandar_ao_robo(pedaco: bytes, nome: str) -> dict:
    """Entrega uma leva ao `baixabradesco` e devolve a resposta dele.

    CHAMADA DIRETA, e não por HTTP. O processo separado poderia falar com a
    rota `/api/baixabradesco/executar` — é o que o Make faz —, mas isso
    ocuparia uma das QUATRO threads do gunicorn por vários minutos, e elas são
    dividas com os outros 17 módulos. O pedido montado aqui é o MESMO que
    aquela rota passa adiante, então o contrato é o documentado.

    O `secret` não vai: quem autenticou foi a tela do Análise de SPs, com o
    login dela. A senha do módulo existe para quem chega de fora."""
    import base64

    from app.apps.baixabradesco.core import processar_baixabradesco

    # ⚠️ `modo_teste` VAI EXPLÍCITO, E ISSO NÃO É ZELO — É O DEFEITO DE
    # 13/09/2026.
    #
    # O robô assume **ensaio** quando o pedido não diz nada
    # (`payload.get('modo_teste', True)`). Como este pedido só mandava o
    # arquivo, TODA baixa feita por esta tela desde a estreia foi simulação: o
    # robô localizava a SP, montava o plano, respondia "dá para executar" — e
    # não escrevia em lugar nenhum. Nem Omie, nem SPsBD, nem Pipefy, nem o
    # comprovante guardado.
    #
    # A tela então dizia "Baixado", porque lia o "dá para executar" como "foi
    # feito". O dono descobriu do único jeito que dava: *"teve dois
    # comprovantes que eu acabei de encaminhar, e a resposta foi que estava
    # baixado, mas na planilha eles não ficaram como pagos."*
    #
    # AS OPÇÕES TAMBÉM VÃO ESCRITAS, pelo mesmo motivo: um padrão que muda do
    # outro lado muda o que esta tela faz, sem ninguém aqui saber. WhatsApp
    # fica DESLIGADO de propósito — mandar mensagem para fornecedor é efeito
    # para fora da empresa, e ninguém pediu isso a partir daqui.
    pedido = {
        "attachments": [{
            "filename": nome,
            "base64": base64.b64encode(pedaco).decode("ascii"),
        }],
        "modo_teste": False,
        "opcoes": {
            "executar_omie": True,
            "atualizar_spsbd": True,
            "atualizar_pipefy": True,
            "salvar_comprovante": True,
            "enviar_whatsapp": False,
        },
    }
    return processar_baixabradesco(pedido) or {}


def _gravar_itens(conn, lote_id: int, linhas: list) -> None:
    """Grava as linhas de uma leva.

    ⚠️ SEM A COLUNA NOVA, GRAVA SEM ELA — e isto é a correção de um defeito meu,
    relatado pelo dono em 16/09/2026 com o lote inteiro recusado:

        column "conversa_omie" of relation "comprovantes_item" does not exist

    A coluna nasce na migração 013, e **o código sobe para o Render antes de
    alguém apertar "Aplicar atualizações do banco"**. Nessa janela, todo
    comprovante arrastado falhava por inteiro — não é que ficasse sem a conversa
    do Omie: a baixa não acontecia.

    A LEITURA já tinha essa proteção; a GRAVAÇÃO não. É a regra do `CLAUDE.md`
    que eu mesmo quebrei, e ela vale para os dois lados: enquanto a migração não
    for aplicada, o sistema tem de funcionar **sem** a coluna, perdendo só o que
    ela guarda."""
    from .db import tem_coluna

    tem_conversa = tem_coluna("comprovantes_item", "conversa_omie")
    for linha in linhas:
        if tem_conversa:
            conn.execute(
                "INSERT INTO analisesps.comprovantes_item "
                "  (lote_id, pagina, situacao, sp_id, valor, recebedor, motivo, "
                "   conversa_omie) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (lote_id, linha.get("pagina"), linha.get("situacao", ""),
                 linha.get("sp_id", ""), linha.get("valor", ""),
                 linha.get("recebedor", ""), linha.get("motivo", ""),
                 linha.get("conversa_omie", "")))
        else:
            conn.execute(
                "INSERT INTO analisesps.comprovantes_item "
                "  (lote_id, pagina, situacao, sp_id, valor, recebedor, motivo) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (lote_id, linha.get("pagina"), linha.get("situacao", ""),
                 linha.get("sp_id", ""), linha.get("valor", ""),
                 linha.get("recebedor", ""), linha.get("motivo", "")))
    conn.commit()


def processar_um(lote_id: int, anotar=None) -> dict:
    """Processa um lote inteiro, leva por leva, gravando o que sai de cada uma.

    GRAVA A CADA LEVA, e não no fim. Se o serviço reiniciar no meio de um PDF
    de cinquenta páginas, as quatro primeiras levas já estão no banco e a
    pessoa vê o que baixou. Guardar tudo para o fim perderia as cinco."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)

    with conexao() as conn:
        cur = conn.execute(
            "SELECT arquivo, caminho, paginas, levas FROM "
            " analisesps.comprovantes_lote WHERE id = ?", (lote_id,))
        linha = cur.fetchone()
        cur.close()
        if not linha:
            return {"ok": False, "erro": "Lote não encontrado."}
        conn.execute(
            "UPDATE analisesps.comprovantes_lote SET situacao = 'RODANDO' "
            " WHERE id = ?", (lote_id,))
        conn.commit()

    nome, caminho, paginas, levas = linha[0], linha[1], linha[2], linha[3]

    try:
        with open(caminho, "rb") as arquivo:
            conteudo = arquivo.read()
    except OSError as e:
        # O contêiner reinicia e leva o disco junto. Dizer isso é melhor do
        # que deixar o lote "rodando" para sempre.
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.comprovantes_lote SET situacao = 'FALHOU', "
                "  erro = ?, terminado_em = now() WHERE id = ?",
                ("O arquivo não está mais no servidor (o serviço reiniciou "
                 "antes de processar). Arraste o PDF de novo — o que já tiver "
                 "sido baixado não baixa duas vezes.", lote_id))
            conn.commit()
        logger.warning("Análise de SPs: lote %d sem arquivo (%s).", lote_id, e)
        return {"ok": False, "erro": "arquivo sumiu"}

    feitas, contagem = 0, {}
    try:
        for primeira, pedaco in separar_em_levas(conteudo):
            anotar("dando baixa nos comprovantes",
                   f"{nome}: leva {feitas + 1} de {levas}")
            resposta = _mandar_ao_robo(pedaco, nome)
            itens = ler_resposta(resposta, primeira)
            with conexao() as conn:
                _gravar_itens(conn, lote_id, itens)
                feitas += 1
                conn.execute(
                    "UPDATE analisesps.comprovantes_lote SET levas_feitas = ? "
                    " WHERE id = ?", (feitas, lote_id))
                conn.commit()
            for item in itens:
                contagem[item["situacao"]] = contagem.get(item["situacao"], 0) + 1
    except Exception as e:  # noqa: BLE001 — a falha tem de aparecer na tela
        logger.exception("Análise de SPs: falhou o lote %d de comprovantes", lote_id)
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.comprovantes_lote SET situacao = 'FALHOU', "
                "  erro = ?, terminado_em = now() WHERE id = ?",
                (str(e)[:1000], lote_id))
            conn.commit()
        _apagar_arquivo(caminho)
        return {"ok": False, "erro": str(e), "levas_feitas": feitas}

    with conexao() as conn:
        conn.execute(
            "UPDATE analisesps.comprovantes_lote SET situacao = 'PRONTO', "
            "  terminado_em = now() WHERE id = ?", (lote_id,))
        conn.commit()
    _apagar_arquivo(caminho)

    logger.info("Análise de SPs: lote %d pronto — %s.", lote_id,
                ", ".join(f"{ROTULOS.get(s, s)}: {q}"
                          for s, q in contagem.items()) or "nada")
    return {"ok": True, "paginas": paginas, "contagem": contagem}


# Quanto tempo sem terminar até um lote ser considerado abandonado. Um PDF de
# cinquenta páginas leva poucos minutos; quinze é folgado.
MINUTOS_PARA_ABANDONADO = 15


def destravar_parados(minutos: int = MINUTOS_PARA_ABANDONADO) -> dict:
    """Devolve à fila os lotes que começaram e pararam no meio.

    ⚠️ ISTO É O QUE FALTAVA NO BOTÃO "Retomar a fila agora". Relato do dono em
    16/09/2026: *"clico nele e nada acontece"*.

    E não acontecia mesmo: retomar só pegava lote em ESPERANDO, e o lote dele
    estava em RODANDO — o processo tinha começado e morrido no meio (o serviço
    do Render reinicia de tempos em tempos). Um lote em RODANDO ficava assim
    PARA SEMPRE: nenhum processo o retomava, e nenhum botão o alcançava.

    Dois destinos, conforme o arquivo ainda exista:

      - **arquivo no disco** → volta para ESPERANDO e é reprocessado. O que já
        baixou no Omie é reconhecido como duplicado e não baixa duas vezes.
      - **arquivo sumiu** (o contêiner reiniciou e levou o disco) → é marcado
        como FALHOU, dizendo que o PDF precisa ser arrastado de novo. Deixar em
        RODANDO seria mentir que ainda está trabalhando."""
    import os

    from .db import conexao, consultar

    # ⚠️ ESPERANDO ENTRA JUNTO DESDE 17/09/2026, e é o aviso que não saía.
    #
    # Relato do dono: *"uma coisa que está aparecendo lá e não sai, é um
    # retomar fila. Não sei por que está com aquela pendência e está assim."*
    #
    # O aviso da tela acende para lote parado em ESPERANDO **ou** em RODANDO
    # (ver `parece_parado`), mas o destravamento só alcançava RODANDO. Um lote
    # que ficou em ESPERANDO sem o PDF no disco — o contêiner reiniciou entre o
    # envio e o processamento — era escolhido pela fila a cada rodada, falhava
    # ao abrir o arquivo, e **o aviso acendia de novo na rodada seguinte, para
    # sempre**. O botão fazia o que devia; era a lista que nunca esvaziava.
    #
    # As duas situações levam ao mesmo lugar: com arquivo, volta para a fila;
    # sem arquivo, é encerrado dizendo isso. O que não pode é ficar preso.
    try:
        parados = consultar(
            "SELECT id, caminho, arquivo FROM analisesps.comprovantes_lote "
            " WHERE situacao IN ('RODANDO', 'ESPERANDO') "
            "   AND recebido_em < now() - (? || ' minutes')::interval "
            " ORDER BY id", (str(int(minutos)),))
    except Exception:  # noqa: BLE001 — banco fora do ar
        logger.exception("Análise de SPs: não consegui procurar lotes parados")
        return {"devolvidos": 0, "sem_arquivo": 0}

    devolvidos, sem_arquivo = 0, 0
    for lote_id, caminho, nome in parados:
        existe = bool(caminho) and os.path.exists(caminho)
        with conexao() as conn:
            if existe:
                conn.execute(
                    "UPDATE analisesps.comprovantes_lote "
                    "   SET situacao = 'ESPERANDO' WHERE id = ?", (lote_id,))
                devolvidos += 1
            else:
                conn.execute(
                    "UPDATE analisesps.comprovantes_lote SET situacao = 'FALHOU', "
                    "  erro = ?, terminado_em = now() WHERE id = ?",
                    ("O processamento parou no meio e o arquivo não está mais "
                     "no servidor (o serviço reiniciou). Arraste o PDF de novo "
                     "— o que já tiver sido baixado não baixa duas vezes.",
                     lote_id))
                sem_arquivo += 1
            conn.commit()
        logger.warning("Análise de SPs: lote %s (%s) estava parado — %s.",
                       lote_id, nome,
                       "devolvido à fila" if existe else "sem arquivo, falhou")
    return {"devolvidos": devolvidos, "sem_arquivo": sem_arquivo}


def reprocessar_lote(lote_id: int) -> dict:
    """Devolve UM lote à fila, para ser processado de novo do começo.

    Pedido do dono em 17/09/2026: *"às vezes os comprovantes não baixam por
    algum motivo. Eu queria, a partir da tela, poder reenviar um comprovante.
    (…) permitir o reenvio de algo que a gente não baixou. Opa, esqueci algum
    detalhe — o título não está no [Omie]."*

    É o caso de todo dia: o comprovante não baixa porque o título ainda não
    existe no Omie, ou está faltando um dado no card. A pessoa conserta **lá** e
    quer tentar de novo — sem ter de achar o PDF e arrastar outra vez.

    ⚠️ OS ITENS ANTIGOS SÃO APAGADOS, e isto não é detalhe: eles são gravados
    com INSERT simples, sem chave única (`comprovantes_item`). Reprocessar sem
    limpar mostraria **cada página duas vezes** na tela — e quem olhasse
    concluiria que o comprovante foi baixado em dobro. O que foi baixado no
    Omie continua baixado: ele é reconhecido como duplicado e não baixa de
    novo. É a mesma promessa que o botão "Retomar a fila" já faz.

    ⚠️ SEM O PDF NÃO HÁ O QUE REPROCESSAR. O contêiner do Render reinicia e
    leva o disco junto. Nesse caso o lote é encerrado dizendo isso, em vez de
    voltar para uma fila onde ele falharia de novo a cada rodada."""
    import os

    from .db import conexao, consultar_um

    try:
        linha = consultar_um(
            "SELECT arquivo, caminho, situacao FROM analisesps.comprovantes_lote "
            " WHERE id = ?", (int(lote_id),))
    except Exception:  # noqa: BLE001 — banco fora do ar
        logger.exception("Análise de SPs: não consegui ler o lote %s", lote_id)
        return {"ok": False, "erro": "Não consegui falar com o banco agora."}
    if not linha:
        return {"ok": False, "erro": "Este lote não existe mais."}

    nome, caminho, situacao = linha[0], linha[1], linha[2]
    if not (caminho and os.path.exists(caminho)):
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.comprovantes_lote SET situacao = 'FALHOU', "
                "  erro = ?, terminado_em = now() WHERE id = ?",
                ("O PDF não está mais no servidor — o serviço reiniciou e o "
                 "disco foi junto. Arraste o arquivo de novo; o que já baixou "
                 "no Omie não baixa duas vezes.", int(lote_id)))
            conn.commit()
        return {"ok": False, "arquivo": nome, "sem_arquivo": True,
                "erro": ("O PDF de %s não está mais no servidor. Arraste o "
                         "arquivo de novo — o que já baixou não baixa duas "
                         "vezes." % (nome or "este lote"))}

    with conexao() as conn:
        # ⚠️ Primeiro os itens, depois o lote: o inverso deixaria a tela um
        # instante com o lote "esperando" e as linhas velhas ainda ali.
        conn.execute("DELETE FROM analisesps.comprovantes_item "
                     " WHERE lote_id = ?", (int(lote_id),))
        conn.execute(
            "UPDATE analisesps.comprovantes_lote "
            "   SET situacao = 'ESPERANDO', levas_feitas = 0, erro = '', "
            "       terminado_em = NULL "
            " WHERE id = ?", (int(lote_id),))
        conn.commit()
    logger.info("Análise de SPs: lote %s (%r) voltou para a fila — estava %s.",
                lote_id, nome, situacao)
    return {"ok": True, "arquivo": nome, "situacao_anterior": situacao}


def processar_pendentes(anotar=None) -> dict:
    """Drena a fila de lotes ESPERANDO. É o que o processo separado chama.

    ⚠️ ANTES DE DRENAR, DESTRAVA O QUE FICOU PELO CAMINHO. Sem isto, um lote
    que morreu no meio nunca mais era tocado por ninguém."""
    from .db import consultar

    destravados = destravar_parados()

    esperando = consultar(
        "SELECT id FROM analisesps.comprovantes_lote "
        " WHERE situacao = 'ESPERANDO' ORDER BY id")
    feitos, falhas = 0, 0
    for (lote_id,) in esperando:
        resultado = processar_um(lote_id, anotar)
        if resultado.get("ok"):
            feitos += 1
        else:
            falhas += 1
    return {"lotes": feitos, "falhas": falhas,
            "destravados": destravados["devolvidos"],
            "sem_arquivo": destravados["sem_arquivo"]}


# ---------------------------------------------------------------------------
# O que a tela mostra
# ---------------------------------------------------------------------------
def historico(pessoa: str = "", quantos: int = 15) -> list[dict]:
    """Os últimos lotes, com a contagem por situação de cada um.

    UMA CONSULTA PARA OS LOTES E UMA PARA OS ITENS, não uma por lote: com
    quinze lotes na tela seriam dezesseis idas ao banco, e o banco tem um
    décimo de um núcleo."""
    from .db import consultar

    lotes = consultar(
        "SELECT id, arquivo, quem, paginas, situacao, levas, levas_feitas, "
        "       erro, recebido_em, terminado_em "
        "  FROM analisesps.comprovantes_lote "
        " ORDER BY id DESC LIMIT ?", (max(1, int(quantos)),))
    if not lotes:
        return []

    ids = [l[0] for l in lotes]
    marcadores = ",".join(["?"] * len(ids))
    contagens = consultar(
        f"SELECT lote_id, situacao, count(*) FROM analisesps.comprovantes_item "
        f" WHERE lote_id IN ({marcadores}) GROUP BY lote_id, situacao",
        tuple(ids))
    por_lote: dict = {}
    for lote_id, situacao, quantos_ in contagens:
        por_lote.setdefault(lote_id, {})[situacao] = quantos_

    # ⚠️ HÁ QUANTO TEMPO ESTÁ ASSIM. A tela dizia "Ainda processando — as linhas
    # vão aparecendo" para um lote parado desde as 7h47 da manhã. Isso não é
    # informação, é engano: quem lê fica esperando uma coisa que não vem.
    #
    # O lote pode ficar parado por dois motivos, e os dois acontecem: ninguém
    # começou a processar (ESPERANDO), ou o processo morreu no meio (RODANDO e
    # o serviço reiniciou). Em nenhum dos dois alguém vai contar sozinho.
    from .horario import agora

    agora_ = agora()

    def _minutos(quando):
        if not quando:
            return None
        try:
            return int((agora_ - quando).total_seconds() // 60)
        except Exception:  # noqa: BLE001 — carimbo estranho não derruba a tela
            return None

    saida = []
    for l in lotes:
        contagem = por_lote.get(l[0], {})
        parado_ha = _minutos(l[8]) if l[4] in ("ESPERANDO", "RODANDO") else None
        saida.append({
            "id": l[0], "arquivo": l[1], "quem": l[2], "paginas": l[3],
            "situacao": l[4], "levas": l[5], "levas_feitas": l[6],
            "erro": l[7], "recebido_em": l[8], "terminado_em": l[9],
            "parado_ha": parado_ha,
            # Quinze minutos é folgado: um PDF de cinquenta páginas leva
            # poucos minutos. Passou disso sem terminar, alguma coisa houve.
            "parece_parado": bool(parado_ha is not None and parado_ha >= 15),
            "contagem": [(s, ROTULOS.get(s, s), contagem[s])
                         for s in ORDEM if contagem.get(s)],
            "resolvidos": contagem.get(BAIXADO, 0) + contagem.get(DUPLICADO, 0),
            "pendencias": sum(q for s, q in contagem.items()
                              if s not in (BAIXADO, DUPLICADO)),
        })
    return saida


def itens_do_lote(lote_id: int) -> list[dict]:
    """As páginas de um lote, o que PEDE AÇÃO primeiro.

    A ordem não é a das páginas de propósito: quem abre isto quer saber o que
    ficou de fora, e o que baixou é o esperado."""
    from .db import consultar

    ordem = {s: n for n, s in enumerate(ORDEM)}
    # A conversa com o Omie vem junto: é ela que responde "por que não baixou",
    # e a tela a mostra escondida atrás de um clique — não é leitura do dia a
    # dia, mas quando faz falta não pode estar noutro lugar.
    try:
        linhas = consultar(
            "SELECT pagina, situacao, sp_id, valor, recebedor, motivo, "
            "       conversa_omie "
            "  FROM analisesps.comprovantes_item WHERE lote_id = ? "
            " ORDER BY pagina NULLS LAST", (lote_id,))
    except Exception:  # noqa: BLE001 — migração 013 ainda não aplicada
        logger.exception("Análise de SPs: lendo os itens sem a conversa do Omie")
        linhas = [tuple(l) + ("",) for l in consultar(
            "SELECT pagina, situacao, sp_id, valor, recebedor, motivo "
            "  FROM analisesps.comprovantes_item WHERE lote_id = ? "
            " ORDER BY pagina NULLS LAST", (lote_id,))]
    itens = [{"pagina": l[0], "situacao": l[1], "rotulo": ROTULOS.get(l[1], l[1]),
              "sp_id": l[2], "valor": l[3], "recebedor": l[4], "motivo": l[5],
              "conversa_omie": l[6] if len(l) > 6 else ""}
             for l in linhas]
    itens.sort(key=lambda i: (ordem.get(i["situacao"], 99),
                              i["pagina"] if i["pagina"] is not None else 10 ** 6))
    return itens
