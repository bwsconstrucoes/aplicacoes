# -*- coding: utf-8 -*-
"""
O ponto do Mobponto, dia por dia.

⚠️ É O GARGALO DE TUDO NA FOLHA: sem o ponto não há total por obra, não há
diária, não há apropriação — e sem apropriação não há arquivo de pagamento.

O CONTRATO DA API FOI LIDO DOS APPS SCRIPT do dono, não suposto. Dois scripts,
três endpoints:

    FOLHA_BWS_EXCEL   o ponto por dia, paginado por mês/ano  ← este módulo
    REL_PRESENCA_BWS  o resumo de presença por mês, com o local
    FUNCIONARIOS      o cadastro do Mobponto

    GET https://www.mobponto.com.br/ponto/api/endpoint.php
        ?type_data=FOLHA_BWS_EXCEL&status=false&mes=<M>&ano=<AAAA>&pagina=<N>

    {"result": {"total_paginas": N,
                "funcionarios": [{"cpf": ..., "nome": ...,
                                  "relatorio": [{"dia": ..., "matricula": ...,
                                                 …campos dinâmicos…}]}]}}

⚠️ OS CAMPOS DE CADA DIA SÃO DINÂMICOS, e por isso este módulo **guarda o dia
inteiro como veio**, em `campos`. Quem interpreta é a leitura, não a gravação: se
o nome de um campo mudar, conserta-se num lugar e o histórico já gravado continua
valendo. Gravar interpretado obrigaria a recarregar meses inteiros de ponto para
corrigir um nome de coluna.

OS NOMES DOS CAMPOS SÃO CONHECIDOS DESDE 29/09/2026, e não foram supostos: o dono
mandou o programa que já roda em cima deste mesmo relatório, e é dele que eles
saem (`analysis_engine.py`, funções `normalize_folha` e `merge_folha_group`).
Antes disso este módulo dizia, com todas as letras, que não os conhecia — e era
essa falta que travava o total por obra da folha.

    cpf, nome, data          quem e quando
    hr_entrada, hr_almoco,   as QUATRO marcações do dia, nesta ordem
    hr_retorno, hr_saida
    obra_entrada,            a obra DE CADA marcação — é daqui que sai a
    obra_almoco,             apropriação: o dia pertence à obra que mais
    obra_retorno,            aparece nas quatro, e no empate 2x2 vale a
    obra_saida               obra em que o dia começou (decisão do dono)
    presenca_ausencia        presença, falta, férias, atestado…
    desc_falta               a descrição da falta, quando há
    totalHrs, dia_semana     conferência

⚠️ A ORDEM DAS QUATRO OBRAS É A ORDEM DO DIA, e não pode ser alterada: o
desempate do `folha_apropriacao.obra_do_dia` é justamente "vale a primeira".
Ordenar a lista antes de contar faria o desempate virar sorteio alfabético.

⚠️ NÃO COPIEI A REGRA DO PROGRAMA DELE PARA ESCOLHER A OBRA DO DIA. Lá a obra do
dia é a **primeira preenchida** das quatro (`bfill`); aqui é a **mais frequente**,
com o empate resolvido pela primeira. A diferença é decisão do dono, de
26/09/2026, e ela muda dinheiro: quem entra numa obra e passa o resto do dia em
outra tem o dia contado na segunda, não na primeira.

AS CREDENCIAIS vêm de `MOBPONTO_AUTHORIZATION` e `MOBPONTO_API_KEY` no Render.
Elas estão hoje escritas dentro dos Apps Script, em claro — ao trazer para cá,
**trocar na origem**, pelo mesmo motivo do `EL_NFSE_TOKEN` (CONTEXTO.md §9):
chave que já circulou é chave a trocar.
"""
from __future__ import annotations

import json
import logging
import os
import time

from . import formatos

logger = logging.getLogger("analisesps.ponto")

URL = os.getenv("MOBPONTO_URL",
                "https://www.mobponto.com.br/ponto/api/endpoint.php")
VERSAO_DA_API = os.getenv("MOBPONTO_API_VERSION", "1.0.0")

TIPO_FOLHA = "FOLHA_BWS_EXCEL"

# ---------------------------------------------------------------------------
# OS NOMES DOS CAMPOS DE UM DIA
#
# Lidos do programa do dono em 29/09/2026 (ver o topo do arquivo). Ficam aqui, em
# constante, por dois motivos: a leitura toda passa por eles — então um nome que
# mude se conserta num lugar — e um teste pode afirmar quais são, o que faz um
# rename silencioso quebrar a suíte em vez de esvaziar a apropriação em produção.
# ---------------------------------------------------------------------------
# As quatro marcações, NA ORDEM DO DIA. A ordem é regra de negócio, não estética.
CAMPOS_DAS_HORAS = ("hr_entrada", "hr_almoco", "hr_retorno", "hr_saida")

# A obra de cada marcação, na MESMA ordem das horas.
CAMPOS_DAS_OBRAS = ("obra_entrada", "obra_almoco", "obra_retorno", "obra_saida")

CAMPO_PRESENCA = "presenca_ausencia"
CAMPO_FALTA = "desc_falta"
CAMPO_TOTAL_DE_HORAS = "totalHrs"
CAMPO_DIA_DA_SEMANA = "dia_semana"

# ⚠️ QUANTO ESPERAR, E QUANTAS VEZES TENTAR — refeito em 30/09/2026.
#
# O dono: *"ponto não conclui, não sai disso"* — "Read timed out (read
# timeout=60)" na página 7. O Mobponto monta cada página na hora, e algumas
# levam mais de um minuto. Esperávamos 60 s, tentávamos três vezes em sete
# segundos e desistíamos do mês inteiro.
#
# O script dele que funciona (o "Relatório Geral Mensal", no Apps Script) faz o
# oposto nas duas coisas: pede UMA página por minuto, e quando uma falha NÃO
# desiste — tenta de novo no minuto seguinte, indefinidamente. Aqui:
#
#   - espera até 3 minutos pela resposta de uma página (conexão: 20 s);
#   - quando falha, tenta de novo com espera crescente: 15 s, 30 s, 1, 2 e 4
#     minutos — seis tentativas, uns 8 minutos de paciência por página;
#   - entre uma página e outra, uma pausa curta, para não emendar pedidos num
#     servidor que já está lento (o script dele espera um minuto inteiro).
#
# Durante as esperas o processo continua "dando sinal de vida" (ver
# `_mantendo_vivo`): sem isso, uma espera de 3 minutos faria a tarefa ser dada
# por morta (`tarefas.SEGUNDOS_ATE_DAR_POR_MORTA`).
SEGUNDOS_PARA_CONECTAR = 20
SEGUNDOS_DE_ESPERA = 180
ESPERAS_ENTRE_TENTATIVAS = (15, 30, 60, 120, 240)
TENTATIVAS = len(ESPERAS_ENTRE_TENTATIVAS) + 1
PAUSA_ENTRE_PAGINAS = 3

# ⚠️ O RITMO DO SCRIPT DELE, quando o Mobponto mostra que está lento — 30/09/2026.
# O script que "funciona perfeito" pede UMA página por minuto (`batchPagesPerRun:
# 1`, gatilho de minuto em minuto). Um minuto por página em todo mês seria lento
# à toa quando o servidor está bem; então o ritmo começa rápido e, na primeira
# página que demorar (ou precisar de nova tentativa), passa a ser o dele até o
# fim da carga.
SEGUNDOS_PARA_SER_LENTA = 30
PAUSA_SE_LENTO = 60

# Teto de páginas por carga. A API diz quantas há; o teto existe para o caso de
# ela dizer um número absurdo — ler mil páginas travaria o processo por horas.
MAXIMO_DE_PAGINAS = 400

# Quantos dias se gravam por vez. 60 mil linhas num mês é normal (500 pessoas ×
# 31 dias × 4 marcações, dependendo do formato), e gravar tudo de uma vez faria o
# pico de memória subir numa instância de 2 GB dividida com 17 módulos.
DIAS_POR_BLOCO = 2000


class ErroDoPonto(RuntimeError):
    """Não deu para trazer o ponto. A frase vai inteira para a tela."""


def _pronto() -> bool:
    """A migração 031 já rodou? Enquanto não, a tela avisa em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.ponto_carga LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


def _substituicao_segura() -> bool:
    """A migração 037 já rodou? Ela é o que permite a carga nova nascer AO LADO
    da antiga e a que caiu RETOMAR de onde parou.

    Sem ela, `carregar` faz o de antes: apaga a antiga e recomeça da página 1,
    com um aviso na carga. Diante de qualquer falha responde False: o caminho
    antigo funciona nos dois bancos."""
    from .db import tem_coluna
    return tem_coluna("ponto_carga", "terminada_em")


# Uma carga que caiu há mais tempo do que isto não é retomada: o Mobponto já
# mudou o suficiente para as páginas não casarem mais com as da tentativa.
HORAS_PARA_RETOMAR = 24


def configurado() -> bool:
    """As duas credenciais existem? A tela pergunta antes de oferecer o botão."""
    return bool((os.getenv("MOBPONTO_AUTHORIZATION") or "").strip()
                and (os.getenv("MOBPONTO_API_KEY") or "").strip())


def _cabecalhos() -> dict:
    """Os três cabeçalhos que a API exige.

    ⚠️ O VALOR DO `Authorization` VEM INTEIRO da variável de ambiente, com o
    "Basic " na frente. É como ele está no script do dono, e montar a
    codificação aqui obrigaria a guardar usuário e senha separados — mais peças
    para dar errado, e nenhuma vantagem."""
    auth = (os.getenv("MOBPONTO_AUTHORIZATION") or "").strip()
    chave = (os.getenv("MOBPONTO_API_KEY") or "").strip()
    if not auth or not chave:
        raise ErroDoPonto(
            "faltam as credenciais do Mobponto. Crie MOBPONTO_AUTHORIZATION e "
            "MOBPONTO_API_KEY no Render — os valores estão nos Apps Script das "
            "planilhas do ponto. Copie de lá direto para o Render e troque a "
            "chave na origem depois.")
    return {"Authorization": auth, "api-key": chave,
            "api-version": VERSAO_DA_API}


def _confianca_tls():
    """Qual pacote de certificados usar para falar com o Mobponto.

    ⚠️ ISTO EXISTE POR CAUSA DE UMA FALHA REAL, em 29/09/2026. A carga do ponto
    morreu com:

        SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED]
        certificate verify failed: unable to get local issuer certificate'))

    "unable to get local issuer certificate" NÃO é credencial recusada e NÃO é
    instabilidade de rede — é o Python não conseguindo montar a cadeia de confiança
    até uma raiz que ele conheça. As duas causas possíveis:

      1. o servidor manda a cadeia INCOMPLETA (falta o certificado intermediário).
         Navegador e Google Apps Script disfarçam isso indo buscar o intermediário;
         o `requests` não — e é por isso que o script antigo do dono funcionava e
         este caminho não;
      2. o pacote de raízes do container está velho, ausente, ou uma variável de
         ambiente (`REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE`) aponta para um arquivo
         que não tem a raiz certa.

    A causa 2 se resolve aqui, apontando explicitamente para o pacote do `certifi`,
    que vem com a biblioteca e é atualizado com ela — em vez de depender do que
    estiver no sistema ou do que uma variável de ambiente disser.

    ⚠️ A CAUSA 1 TEM CONSERTO SEM BAIXAR A SEGURANÇA, e ele é a saída preferida:
    **`MOBPONTO_CA_EXTRA`**. É o certificado que está faltando, colado ali em
    texto (o bloco `-----BEGIN CERTIFICATE-----`). Com ele, a verificação
    CONTINUA LIGADA — o que faltava era só a peça do meio da corrente, e agora ela
    está no bolso. É o que o navegador faz sozinho, e o `requests` não faz.

    Acrescentado em 29/09/2026, depois de a primeira versão deste arquivo oferecer
    só o desligar. Oferecer apenas a saída insegura empurra para ela.

    ⚠️ E `MOBPONTO_TLS_INSEGURO=1` continua existindo, como ÚLTIMO recurso e
    decisão DELE, nunca por padrão. O risco, dito por escrito: sem verificar o
    certificado, alguém no caminho da rede poderia se passar pelo Mobponto e
    receber a credencial que vai no cabeçalho. Em rede de servidor o risco é baixo,
    mas não é zero — e quem decide correr esse risco é ele."""
    import os
    if (os.getenv("MOBPONTO_TLS_INSEGURO") or "").strip() in ("1", "true", "sim"):
        logger.warning(
            "Análise de SPs: ponto sendo lido SEM verificar o certificado do "
            "Mobponto (MOBPONTO_TLS_INSEGURO ligado). A credencial vai no "
            "cabeçalho desta chamada.")
        return False

    try:
        import certifi
        pacote = certifi.where()
    except Exception:  # noqa: BLE001 — sem certifi, vale o padrão do requests
        pacote = None

    extra = (os.getenv("MOBPONTO_CA_EXTRA") or "").strip()
    if extra:
        juntado = _pacote_com_o_extra(pacote, extra)
        if juntado:
            return juntado
    return pacote or True


def _intermediario_do_servidor(url: str) -> str:
    """Baixa sozinho o certificado que falta na cadeia. `""` quando não dá.

    ⚠️ É O QUE O NAVEGADOR FAZ, e é por isso que o site "funciona no navegador e
    não aqui". Quando o servidor manda a cadeia incompleta, o certificado dele
    carrega dentro de si o ENDEREÇO de quem o assinou (o campo `caIssuers`, da
    extensão AIA). O navegador vai lá, baixa a peça que falta e completa a
    corrente. O `requests` não faz isso — e era essa a diferença.

    ⚠️ BAIXAR O INTERMEDIÁRIO SEM VERIFICAR NÃO ABRE BURACO, e vale explicar por
    quê, porque parece que abre: o certificado baixado **não passa a ser
    confiável**. Ele só entra no pacote como candidato; a verificação de verdade
    continua acontecendo depois, e só passa se a corrente inteira terminar numa
    RAIZ que já era confiável. Um intermediário falso não chega a raiz nenhuma e
    a conexão falha do mesmo jeito. O que se ganha é a peça do meio; o que decide
    continua sendo a raiz.

    Devolve o PEM do intermediário, ou "" quando não houver endereço, o download
    falhar, ou o arquivo não for um certificado."""
    import ssl
    from urllib.parse import urlparse

    import requests

    try:
        from cryptography import x509
        from cryptography.hazmat.primitives.serialization import Encoding
    except Exception:  # noqa: BLE001 — sem a biblioteca, não há o que fazer
        logger.warning("Análise de SPs: sem `cryptography` para ler a cadeia.")
        return ""

    alvo = urlparse(url)
    host, porta = alvo.hostname, alvo.port or 443
    if not host:
        return ""

    try:
        # ⚠️ SEM VERIFICAR, e SÓ PARA LER. Ver o aviso acima: o que sai daqui é
        # candidato, não confiança.
        bruto = ssl.get_server_certificate((host, porta),
                                           timeout=SEGUNDOS_DE_ESPERA)
        folha = x509.load_pem_x509_certificate(bruto.encode())
        aia = folha.extensions.get_extension_for_class(
            x509.AuthorityInformationAccess).value
        enderecos = [d.access_location.value for d in aia
                     if d.access_method == x509.oid.AuthorityInformationAccessOID
                     .CA_ISSUERS]
    except Exception:  # noqa: BLE001 — sem AIA, ou site fora do ar
        logger.info("Análise de SPs: o certificado do %s não diz onde está o "
                    "intermediário (sem AIA) — não dá para completar sozinho.",
                    host)
        return ""

    for endereco in enderecos:
        try:
            resposta = requests.get(endereco, timeout=SEGUNDOS_DE_ESPERA)
            resposta.raise_for_status()
            corpo = resposta.content
            # O arquivo vem em DER quase sempre (`.crt`/`.cer`); em PEM às vezes.
            if b"BEGIN CERTIFICATE" in corpo:
                pem = corpo.decode("utf-8", "replace")
            else:
                pem = x509.load_der_x509_certificate(corpo).public_bytes(
                    Encoding.PEM).decode()
            logger.info("Análise de SPs: intermediário do %s baixado de %s — a "
                        "cadeia pode ser completada sem baixar a segurança.",
                        host, endereco)
            return pem
        except Exception:  # noqa: BLE001 — tenta o próximo endereço
            logger.info("Análise de SPs: não consegui baixar o intermediário de "
                        "%s", endereco)
    return ""


def _pacote_com_o_extra(pacote, extra: str):
    """Junta o certificado que falta ao pacote de raízes. Devolve o caminho.

    ⚠️ O ARQUIVO É ESCRITO UMA VEZ E REAPROVEITADO. Escrever a cada chamada faria
    a carga do ponto criar centenas de arquivos temporários num serviço que divide
    2 GB com outros treze módulos — e o conteúdo é sempre o mesmo.

    ⚠️ ACEITA O CERTIFICADO EM TEXTO OU EM BASE64. O painel do Render engole
    quebra de linha em variável de ambiente com facilidade, e um PEM sem as
    quebras certas não vale nada. Com base64 não há como estragar no caminho.

    Devolve `None` quando não deu para montar — e aí vale o pacote normal, com a
    falha original aparecendo por inteiro. Silenciar aqui trocaria um erro claro
    por um erro confuso."""
    import base64
    import hashlib
    import os
    import tempfile

    texto = extra
    if "BEGIN CERTIFICATE" not in texto:
        try:
            texto = base64.b64decode(extra, validate=True).decode("utf-8")
        except Exception:  # noqa: BLE001
            logger.warning(
                "Análise de SPs: MOBPONTO_CA_EXTRA não parece um certificado "
                "(nem PEM, nem base64 de PEM) — ignorando e usando o pacote "
                "normal.")
            return None
    if "BEGIN CERTIFICATE" not in texto:
        logger.warning("Análise de SPs: MOBPONTO_CA_EXTRA sem bloco CERTIFICATE.")
        return None

    marca = hashlib.sha256(texto.encode("utf-8")).hexdigest()[:16]
    destino = os.path.join(tempfile.gettempdir(), f"mobponto-ca-{marca}.pem")
    if not os.path.exists(destino):
        try:
            base = ""
            if pacote and os.path.exists(pacote):
                with open(pacote, encoding="utf-8") as f:
                    base = f.read()
            with open(destino, "w", encoding="utf-8") as f:
                f.write(base.rstrip() + "\n" + texto.strip() + "\n")
        except Exception:  # noqa: BLE001 — disco cheio, permissão…
            logger.exception(
                "Análise de SPs: não consegui montar o pacote de certificados "
                "com o MOBPONTO_CA_EXTRA")
            return None
    logger.info("Análise de SPs: ponto usando o pacote de certificados com o "
                "intermediário do MOBPONTO_CA_EXTRA.")
    return destino


def _mantendo_vivo(anotar, etapa: str, progresso: str):
    """Enquanto o bloco roda, anota sinal de vida a cada 30 s.

    Um pedido ao Mobponto pode levar 3 minutos, e as esperas entre tentativas
    chegam a 4. Sem isto, a tarefa ficaria calada tempo bastante para ser dada
    por morta e encerrada — com a carga ainda andando."""
    import contextlib
    import threading

    @contextlib.contextmanager
    def _bloco():
        parar = threading.Event()

        def bater():
            while not parar.wait(30):
                try:
                    anotar(etapa, progresso)
                except Exception:  # noqa: BLE001 — sinal de vida nunca derruba
                    logger.exception("Análise de SPs: sinal de vida do ponto falhou")

        fio = threading.Thread(target=bater, daemon=True)
        fio.start()
        try:
            yield
        finally:
            parar.set()
            fio.join(timeout=5)

    return _bloco()


def _pedir_pagina(ano: int, mes: int, pagina: int) -> dict:
    """Uma página do relatório, com paciência. Ver `ESPERAS_ENTRE_TENTATIVAS`."""
    import requests

    parametros = {"type_data": TIPO_FOLHA, "status": "false",
                  "mes": str(int(mes)), "ano": str(int(ano)),
                  "pagina": str(int(pagina))}
    cabecalhos = _cabecalhos()
    confianca = _confianca_tls()
    ultimo = "falha desconhecida"

    for tentativa in range(1, TENTATIVAS + 1):
        try:
            resposta = requests.get(URL, params=parametros,
                                    headers=cabecalhos, verify=confianca,
                                    timeout=(SEGUNDOS_PARA_CONECTAR,
                                             SEGUNDOS_DE_ESPERA))
        except Exception as e:  # noqa: BLE001 — rede oscila
            # ⚠️ ERRO DE CERTIFICADO NÃO SE REPETE: ele não melhora na terceira
            # tentativa, e o recado precisa dizer o que é, porque "falha de
            # conexão" mandaria tentar de novo para sempre.
            if "CERTIFICATE_VERIFY_FAILED" in str(e) or "SSLError" in type(e).__name__:
                # ⚠️ UMA TENTATIVA DE COMPLETAR A CADEIA SOZINHO, antes de
                # devolver o erro. É o que o navegador faz: o certificado do site
                # diz onde está a peça que falta, e ela se baixa. Ver
                # `_intermediario_do_servidor` — inclusive por que isso NÃO abre
                # buraco de segurança.
                #
                # ⚠️ SÓ NA PRIMEIRA TENTATIVA e só quando a verificação está
                # LIGADA: se ele já desligou, não há cadeia a completar; e tentar
                # a cada volta faria a tela esperar três downloads para dar o
                # mesmo recado.
                if tentativa == 1 and confianca is not False:
                    remendo = _intermediario_do_servidor(URL)
                    if remendo:
                        novo_pacote = _pacote_com_o_extra(confianca if
                                                          isinstance(confianca, str)
                                                          else None, remendo)
                        if novo_pacote:
                            confianca = novo_pacote
                            ultimo = str(e)
                            continue
                raise ErroDoPonto(
                    "o certificado do site do Mobponto não pôde ser verificado "
                    "(CERTIFICATE_VERIFY_FAILED). Isto NÃO é credencial errada nem "
                    "instabilidade: falta uma peça do meio da corrente de "
                    "certificados. EU JÁ TENTEI BAIXAR ESSA PEÇA SOZINHO, do jeito "
                    "que o navegador faz, e não consegui — ou o certificado do "
                    "Mobponto não diz onde ela está, ou o servidor dela não "
                    "respondeu daqui. "
                    "HÁ DOIS JEITOS DE RESOLVER, e o primeiro é o certo: "
                    "(1) abra https://www.mobponto.com.br no navegador, clique no "
                    "cadeado, exporte o certificado do MEIO da cadeia (o que não é "
                    "o do site nem a raiz) e cole o conteúdo dele na variável "
                    "MOBPONTO_CA_EXTRA, no Render — a verificação continua ligada e "
                    "nada de segurança é perdido; "
                    "(2) se não der, MOBPONTO_TLS_INSEGURO=1 dispensa a "
                    "verificação — funciona na hora, mas alguém no caminho da rede "
                    "poderia se passar pelo Mobponto e pegar a credencial. "
                    "O conserto de vez é o suporte do Mobponto instalar o "
                    "certificado intermediário no servidor deles.") from e
            ultimo = str(e)
        else:
            if 200 <= resposta.status_code < 300:
                try:
                    return resposta.json() or {}
                except Exception as e:  # noqa: BLE001
                    ultimo = (f"a resposta não é JSON: {e}. Começo dela: "
                              f"{resposta.text[:200]}")
            else:
                # ⚠️ 401 e 403 NÃO SÃO PARA REPETIR: credencial errada não
                # melhora na terceira tentativa, e insistir só demora.
                if resposta.status_code in (401, 403):
                    raise ErroDoPonto(
                        f"o Mobponto recusou a credencial (HTTP "
                        f"{resposta.status_code}). Confira "
                        "MOBPONTO_AUTHORIZATION e MOBPONTO_API_KEY no Render.")
                ultimo = f"HTTP {resposta.status_code}"
        if tentativa < TENTATIVAS:
            espera = ESPERAS_ENTRE_TENTATIVAS[tentativa - 1]
            logger.warning("Análise de SPs: página %d do ponto %02d/%d falhou "
                           "(%s) — tentativa %d de %d, de novo em %d s.",
                           pagina, mes, ano, ultimo, tentativa, TENTATIVAS, espera)
            time.sleep(espera)

    raise ErroDoPonto(
        f"não consegui ler a página {pagina} do ponto de {mes:02d}/{ano} depois "
        f"de {TENTATIVAS} tentativas em uns {sum(ESPERAS_ENTRE_TENTATIVAS) // 60} "
        f"minutos: {ultimo}. O que já entrou ficou guardado — a próxima carga "
        f"continua da página {pagina}.")


def _dias_do_funcionario(bruto) -> tuple:
    """`(cpf, nome, [dias])` de um funcionário da resposta."""
    cpf = str((bruto or {}).get("cpf") or "").strip()
    nome = " ".join(str((bruto or {}).get("nome") or "").split())
    dias = (bruto or {}).get("relatorio") or []
    return cpf, nome, dias if isinstance(dias, list) else []


def _linha_do_dia(carga_id: int, cpf: str, nome: str, dia) -> tuple | None:
    """Uma linha da tabela, a partir de um dia da resposta.

    ⚠️ GUARDA O DIA INTEIRO em `campos`, e resolve só a data. Os nomes dos campos
    de marcação e de obra ainda não são conhecidos (ver o topo do arquivo), e
    coluna preenchida por palpite é resposta errada com cara de certa."""
    if not isinstance(dia, dict):
        return None
    from .folha_rateio import so_digitos
    return (carga_id, so_digitos(cpf), nome,
            formatos.para_data(dia.get("dia")),
            str(dia.get("matricula") or "").strip(),
            json.dumps(dia, ensure_ascii=False, default=str)[:8000])


def carregar(ano: int, mes: int, anotar=None, quem: str = "") -> dict:
    """Traz o ponto do mês e guarda. É o que o botão e o automático chamam.

    Página por página, cada uma gravada por inteiro na sua transação, com o
    andamento anotado na carga: é isso que permite RETOMAR de onde parou.

    ⚠️ A CARGA ANTERIOR DO MÊS FICA DE PÉ ATÉ ESTA TERMINAR — 29/09/2026.

    Até aqui a carga antiga era apagada ANTES de a nova começar. Se a nova
    caísse no meio (rede, o serviço reiniciando numa publicação, o Mobponto
    fora do ar), o mês ficava com um pedaço: a folha usava aquele pedaço como
    se fosse o ponto inteiro, sem aviso — gente com "menos dias", obra errada,
    e um mês que ANTES estava certo passava a estar errado. O dono perguntou
    exatamente isso: *"O que acontece se o ponto der problema pra baixar no
    meio do caminho?"* — e pediu: *"que possa ser retomada de onde parou e que
    sejamos avisados"*.

    Agora a carga nova nasce "em andamento" (`terminada_em` vazio) ao lado da
    antiga, e só no fim, na mesma transação em que é dada por terminada, a
    antiga é apagada. Quem lê o mês (`carga_do_mes`) só enxerga carga
    terminada. Uma carga que caiu fica na lista como "parou na página N de M"
    e a próxima chamada para o mesmo mês (botão ou automático) CONTINUA dela,
    se for recente (`HORAS_PARA_RETOMAR`). O aviso a quem cuida sai em
    `tarefas`, que é quem sabe que a execução morreu.

    O preço da retomada, dito por inteiro: as páginas antigas são da tentativa
    anterior, e quem mudou de página no Mobponto entre as duas pode faltar (a
    página é gravada por pessoa, então não duplica). Fica escrito nos avisos
    da carga, e a próxima carga completa do mês refaz tudo.
    """
    from .db import conexao, consultar_um

    anotar = anotar or (lambda *a, **k: None)
    if not _pronto():
        raise ErroDoPonto(
            "a tabela do ponto ainda não existe. Aperte "
            '"Aplicar atualizações do banco" em Configurações e tente de novo.')
    if not configurado():
        raise ErroDoPonto(
            "faltam as credenciais do Mobponto. Crie MOBPONTO_AUTHORIZATION e "
            "MOBPONTO_API_KEY no Render.")

    avisos = []
    segura = _substituicao_segura()
    if not segura:
        avisos.append(
            'a atualização 037 do banco ainda não foi aplicada ("Aplicar '
            'atualizações do banco", em Configurações): a carga anterior deste '
            "mês foi apagada antes de esta começar, e uma carga que caia no "
            "meio não retoma. Aplique a atualização.")

    retomada = carga_em_andamento(ano, mes) if segura else None
    if retomada and not (retomada["paginas_lidas"] > 0 and _recente(retomada)):
        retomada = None

    if retomada:
        carga_id = int(retomada["id"])
        total_paginas = int(retomada["paginas"] or 0)
        pagina = int(retomada["paginas_lidas"]) + 1
        primeira = None
        avisos.append(
            f"retomada da página {pagina} de {total_paginas}: as páginas "
            f"anteriores são da tentativa de "
            f"{retomada['carregado_em']:%d/%m %H:%M}. Quem mudou de página no "
            "Mobponto entre as duas tentativas pode faltar — a próxima carga "
            "completa do mês refaz tudo.")
        anotar("retomando o ponto", f"da página {pagina} de {total_paginas}")
        with conexao() as conn:
            # Outras tentativas que também caíram não servem mais.
            conn.execute("DELETE FROM analisesps.ponto_carga "
                         " WHERE ano = ? AND mes = ? AND terminada_em IS NULL "
                         "   AND id <> ?", (int(ano), int(mes), carga_id))
            conn.commit()
        logger.info("Análise de SPs: ponto %02d/%d retomado da página %d de %d "
                    "(carga %d).", mes, ano, pagina, total_paginas, carga_id)
    else:
        anotar("pedindo a primeira página do ponto")
        with _mantendo_vivo(anotar, "pedindo a primeira página do ponto",
                            "esperando o Mobponto responder"):
            primeira = _pedir_pagina(ano, mes, 1)
        resultado = (primeira or {}).get("result") or {}
        funcionarios = resultado.get("funcionarios") or []
        if not funcionarios:
            raise ErroDoPonto(
                f"o Mobponto não devolveu ninguém para {mes:02d}/{ano}. Confira "
                "se o mês está certo e se há ponto lançado nele.")
        try:
            total_paginas = int(resultado.get("total_paginas") or 0)
        except (TypeError, ValueError):
            total_paginas = 0
        # ⚠️ SEM `total_paginas`, LÊ ATÉ A PÁGINA VAZIA — como o script dele
        # ("999999, infinito prático"). Antes, total ausente virava "1 página":
        # o mês inteiro entrava com a primeira página só, e dizia que estava
        # completo. `0` fica gravado como "não sei"; o teto continua valendo.
        if total_paginas <= 0:
            total_paginas = 0
        if total_paginas > MAXIMO_DE_PAGINAS:
            avisos.append(
                f"a API disse que há {total_paginas} páginas e o teto é "
                f"{MAXIMO_DE_PAGINAS} — li só até lá. Se o mês tiver mais, o "
                "ponto está incompleto.")
            total_paginas = MAXIMO_DE_PAGINAS

        with conexao() as conn:
            # Com a 037: só as tentativas que não terminaram saem do caminho;
            # a carga que vale fica até esta terminar. Sem ela: como antes.
            conn.execute("DELETE FROM analisesps.ponto_carga "
                         " WHERE ano = ? AND mes = ?"
                         + (" AND terminada_em IS NULL" if segura else ""),
                         (int(ano), int(mes)))
            cur = conn.execute(
                "INSERT INTO analisesps.ponto_carga "
                "  (ano, mes, paginas, carregado_por) VALUES (?,?,?,?) "
                "RETURNING id",
                (int(ano), int(mes), total_paginas, str(quem or "")[:120]))
            carga_id = int(cur.fetchone()[0])
            cur.close()
            conn.commit()
        pagina = 1

    paginas_gravadas = pagina - 1
    limite = total_paginas or MAXIMO_DE_PAGINAS
    pausa = PAUSA_ENTRE_PAGINAS
    while pagina <= limite:
        if pagina == 1 and primeira is not None:
            dados = primeira
        else:
            if pausa and paginas_gravadas:
                time.sleep(pausa)
            etapa = (f"página {pagina} de {total_paginas}" if total_paginas
                     else f"página {pagina}")
            anotar("trazendo o ponto", etapa)
            comeco = time.monotonic()
            with _mantendo_vivo(anotar, "trazendo o ponto",
                                f"{etapa} — esperando o Mobponto responder"):
                dados = _pedir_pagina(ano, mes, pagina)
            if (pausa < PAUSA_SE_LENTO
                    and time.monotonic() - comeco > SEGUNDOS_PARA_SER_LENTA):
                pausa = PAUSA_SE_LENTO
                logger.warning("Análise de SPs: o Mobponto está lento (página %d "
                               "de %02d/%d) — passo a pedir uma página por "
                               "minuto, como o script da planilha.", pagina, mes, ano)
                avisos.append("o Mobponto estava lento: a carga passou a pedir "
                              "uma página por minuto, como o script da planilha.")
        resultado = (dados or {}).get("result") or {}
        funcionarios = resultado.get("funcionarios") or []

        if not funcionarios:
            # Página vazia no meio é o sinal de fim que a API dá quando
            # `total_paginas` vem otimista. Para em vez de insistir.
            break

        from .folha_rateio import so_digitos
        linhas, cpfs, campos = [], set(), set()
        for bruto in funcionarios:
            cpf, nome, dias = _dias_do_funcionario(bruto)
            if cpf:
                # Como vai para o banco (só dígitos): é por ele que a página
                # apaga o que já tinha da pessoa antes de gravar de novo.
                cpfs.add(so_digitos(cpf))
            for dia in dias:
                if isinstance(dia, dict):
                    campos.update(str(c) for c in dia)
                linha = _linha_do_dia(carga_id, cpf, nome, dia)
                if linha is not None:
                    linhas.append(linha)
        _gravar_pagina(carga_id, pagina, cpfs, linhas, campos)
        paginas_gravadas = pagina
        feito = consultar_um(
            "SELECT count(*), count(DISTINCT cpf) FROM analisesps.ponto_dia "
            " WHERE carga_id = ?", (carga_id,)) or (0, 0)
        anotar("trazendo o ponto",
               f"{feito[0]} dia(s) de {feito[1]} pessoa(s) — página {pagina} "
               f"de {total_paginas}")
        pagina += 1

    totais = consultar_um(
        "SELECT count(*), count(DISTINCT cpf), "
        "       count(*) FILTER (WHERE data IS NULL) "
        "  FROM analisesps.ponto_dia WHERE carga_id = ?", (carga_id,)) or (0, 0, 0)
    dias_gravados, pessoas, sem_data = int(totais[0]), int(totais[1]), int(totais[2])
    if sem_data:
        avisos.append(
            f"{sem_data} dia(s) vieram sem data que eu consiga ler. Eles ficaram "
            "guardados, com o conteúdo original, para conferência.")

    with conexao() as conn:
        # A anterior apagada E esta dada por terminada, numa transação só: ou
        # o mês troca de carga inteiro, ou não troca. Nesta ordem, porque o
        # índice único só admite UMA terminada por mês.
        conn.execute("DELETE FROM analisesps.ponto_carga "
                     " WHERE ano = ? AND mes = ? AND id <> ?",
                     (int(ano), int(mes), carga_id))
        conn.execute(
            "UPDATE analisesps.ponto_carga "
            "   SET paginas_lidas = ?, pessoas = ?, dias = ?, avisos = ?"
            # Total que a API não disse: é o que foi lido até a página vazia.
            + (", paginas = ?" if not total_paginas else "")
            + (", terminada_em = now()" if segura else "")
            + " WHERE id = ?",
            tuple([max(1, paginas_gravadas), pessoas, dias_gravados,
                   " | ".join(avisos)]
                  + ([max(1, paginas_gravadas)] if not total_paginas else [])
                  + [carga_id]))
        conn.commit()

    campos_vistos = (consultar_um(
        "SELECT campos_vistos FROM analisesps.ponto_carga WHERE id = ?",
        (carga_id,)) or [""])[0] or ""
    campos = [c for c in campos_vistos.split("|") if c]
    logger.info("Análise de SPs: ponto %02d/%d — %d dia(s) de %d pessoa(s) em "
                "%d página(s). Campos: %s", mes, ano, dias_gravados,
                pessoas, paginas_gravadas, ", ".join(campos))
    return {"id": carga_id, "ano": int(ano), "mes": int(mes),
            "pessoas": pessoas, "dias": dias_gravados,
            "paginas": total_paginas or max(1, paginas_gravadas),
            "paginas_lidas": paginas_gravadas,
            "campos": campos, "avisos": avisos,
            "retomada": bool(retomada)}


def _gravar_pagina(carga_id: int, pagina: int, cpfs: set, linhas: list,
                   campos: set) -> None:
    """Uma página inteira, numa transação: os dias, o andamento e os campos.

    ⚠️ POR PESSOA, E POR ISSO NÃO DUPLICA: antes de gravar, os dias que esta
    carga já tem das pessoas desta página são apagados. É o que torna a
    retomada segura mesmo se a mesma pessoa vier de novo em outra página.
    """
    from .db import conexao
    with conexao() as conn:
        if cpfs:
            conn.execute("DELETE FROM analisesps.ponto_dia "
                         " WHERE carga_id = ? AND cpf = ANY(?)",
                         (int(carga_id), sorted(cpfs)))
        if linhas:
            conn.executemany(
                "INSERT INTO analisesps.ponto_dia "
                "  (carga_id, cpf, nome, data, matricula, campos) "
                " VALUES (?,?,?,?,?,?)", linhas)
        cur = conn.execute("SELECT campos_vistos FROM analisesps.ponto_carga "
                           " WHERE id = ? FOR UPDATE", (int(carga_id),))
        atuais = ((cur.fetchone() or [""])[0] or "").split("|")
        cur.close()
        todos = sorted({c for c in atuais if c} | set(campos))
        conn.execute("UPDATE analisesps.ponto_carga "
                     "   SET paginas_lidas = ?, campos_vistos = ? WHERE id = ?",
                     (int(pagina), "|".join(todos), int(carga_id)))
        conn.commit()


def _recente(carga: dict) -> bool:
    """A tentativa é de menos de `HORAS_PARA_RETOMAR` horas?"""
    import datetime as _dt
    quando = carga.get("carregado_em")
    if not quando:
        return False
    if quando.tzinfo is None:
        quando = quando.replace(tzinfo=_dt.timezone.utc)
    return (_dt.datetime.now(_dt.timezone.utc) - quando
            ) < _dt.timedelta(hours=HORAS_PARA_RETOMAR)


# ---------------------------------------------------------------------------
# Ler de volta
# ---------------------------------------------------------------------------
CAMPOS_DA_CARGA = ("id", "ano", "mes", "paginas", "paginas_lidas", "pessoas",
                   "dias", "campos_vistos", "avisos", "carregado_em",
                   "carregado_por")


def _campos_da_carga() -> tuple:
    """Os campos a ler — com `terminada_em` só depois da migração 037."""
    if _substituicao_segura():
        return CAMPOS_DA_CARGA + ("terminada_em",)
    return CAMPOS_DA_CARGA


def _carga(linha) -> dict:
    campos = _campos_da_carga()
    carga = {c: linha[i] for i, c in enumerate(campos)}
    carga.setdefault("terminada_em", carga.get("carregado_em"))
    carga["competencia"] = f"{carga['mes']:02d}/{carga['ano']}"
    carga["campos"] = [c for c in (carga["campos_vistos"] or "").split("|") if c]
    carga["lista_de_avisos"] = [a for a in (carga["avisos"] or "").split(" | ")
                                if a]
    carga["completa"] = carga["paginas_lidas"] >= carga["paginas"]
    # Nunca chegou ao fim: o processo caiu no meio. O mês continua valendo a
    # carga anterior (ver `carregar`), a tela diz isso, e a próxima chamada do
    # mês continua da página em que parou.
    carga["interrompida"] = carga["terminada_em"] is None
    return carga


def cargas(teto: int = 36) -> list:
    """As cargas do ponto, da mais recente para a mais antiga."""
    from .db import consultar
    if not _pronto():
        return []
    return [_carga(l) for l in consultar(
        "SELECT " + ", ".join(_campos_da_carga()) + " FROM analisesps.ponto_carga "
        " ORDER BY ano DESC, mes DESC, id DESC LIMIT ?", (int(teto),))]


def carga_do_mes(ano: int, mes: int) -> dict | None:
    """A carga TERMINADA de uma competência, ou None.

    Uma carga em andamento ou que caiu (`terminada_em` vazio) não é o ponto
    do mês — é um pedaço dele. Ver `carregar`."""
    from .db import consultar_um
    if not _pronto():
        return None
    linha = consultar_um(
        "SELECT " + ", ".join(_campos_da_carga()) + " FROM analisesps.ponto_carga "
        " WHERE ano = ? AND mes = ?"
        + (" AND terminada_em IS NOT NULL" if _substituicao_segura() else "")
        + " ORDER BY id DESC LIMIT 1", (int(ano), int(mes)))
    return _carga(linha) if linha else None


def carga_em_andamento(ano: int, mes: int) -> dict | None:
    """A tentativa mais recente do mês que NÃO terminou, ou None."""
    from .db import consultar_um
    if not _pronto() or not _substituicao_segura():
        return None
    linha = consultar_um(
        "SELECT " + ", ".join(_campos_da_carga()) + " FROM analisesps.ponto_carga "
        " WHERE ano = ? AND mes = ? AND terminada_em IS NULL "
        " ORDER BY id DESC LIMIT 1", (int(ano), int(mes)))
    return _carga(linha) if linha else None


def cargas_paradas() -> list:
    """Todas as tentativas que não terminaram, da mais recente para trás.

    É o que o aviso a quem cuida lista: "09/2026 parou na página 12 de 40"."""
    return [c for c in cargas() if c.get("interrompida")]


def o_que_fazer_no_automatico(ano: int, mes: int, hoje=None) -> str:
    """"retomar", "trazer" ou "pular" — a decisão do automático para um mês.

    ⚠️ É O "NÃO MATA O JOB" DO SCRIPT DELE, com um relógio mais folgado. Lá, a
    página que falha é tentada de novo no minuto seguinte, sem fim. Aqui o
    agendador pode chamar DE HORA EM HORA, e cada chamada:

      - RETOMA a carga do mês que parou no meio (da página em que parou);
      - TRAZ o mês se ele ainda não foi trazido HOJE;
      - PULA o mês que já entrou inteiro hoje — sem isso, chamar de hora em
        hora refaria o mês inteiro 24 vezes por dia, e é justamente carga em
        cima do Mobponto que o deixa lento.
    """
    from .horario import agora, para_brasilia
    hoje = hoje or agora().date()
    parada = carga_em_andamento(ano, mes)
    if parada and parada.get("paginas_lidas", 0) > 0 and _recente(parada):
        return "retomar"
    feita = carga_do_mes(ano, mes)
    quando = para_brasilia((feita or {}).get("terminada_em"))
    if quando and quando.date() >= hoje and (feita or {}).get("completa"):
        return "pular"
    return "trazer"


def meses_do_ponto_diario(hoje=None) -> list:
    """Quais meses a carga automática do dia traz: `[(ano, mes), …]`.

    O mês corrente sempre; até o dia 10, TAMBÉM o anterior — é a mesma régua
    de `folha_apropriacao.competencia_sugerida`: nos primeiros dias do mês o
    trabalho é o fechamento do mês que acabou, e o ponto dele ainda recebe
    abono e acerto. O anterior vem primeiro, porque é o que está na mesa.
    """
    import datetime as _dt
    hoje = hoje or _dt.date.today()
    meses = [(hoje.year, hoje.month)]
    if hoje.day <= 10:
        anterior = (hoje.replace(day=1) - _dt.timedelta(days=1))
        meses.insert(0, (anterior.year, anterior.month))
    return meses


def amostra_de_dias(carga_id: int, quantos: int = 5) -> list:
    """Alguns dias como vieram, para a tela mostrar o formato de verdade.

    ⚠️ É O QUE TRANSFORMA "não sei os campos" EM "olha os campos". Sem ver um dia
    de verdade, o mapeamento da obra e das marcações continuaria sendo palpite."""
    from .db import consultar
    if not _pronto():
        return []
    linhas = consultar(
        "SELECT cpf, nome, data, matricula, campos FROM analisesps.ponto_dia "
        " WHERE carga_id = ? ORDER BY id LIMIT ?", (int(carga_id), int(quantos)))
    saida = []
    for cpf, nome, data, matricula, campos in linhas:
        try:
            conteudo = json.loads(campos or "{}")
        except Exception:  # noqa: BLE001 — JSON torto continua sendo mostrado
            conteudo = {"(não deu para ler)": campos}
        saida.append({"cpf": cpf, "nome": nome, "data": data,
                      "matricula": matricula, "campos": conteudo})
    return saida


def apagar(carga_id: int, quem: str = "") -> bool:
    """Apaga uma carga do ponto e os dias dela.

    Seguro: o que está guardado é cópia do que o Mobponto tem. Recarregar não
    perde decisão nenhuma — a apropriação e o ajuste fino moram em outro lugar."""
    from .db import conexao
    if not _pronto():
        return False
    with conexao() as conn:
        cur = conn.execute("DELETE FROM analisesps.ponto_carga WHERE id = ?",
                           (int(carga_id),))
        apagou = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()
        conn.commit()
    if apagou:
        logger.info("Análise de SPs: carga do ponto %s apagada por %s.",
                    carga_id, quem or "(sem nome)")
    return apagou


def dias_da_pessoa(cpf: str, ano: int, mes: int) -> dict:
    """O ponto de UMA pessoa no mês: o que veio, dia por dia.

    Pedido do dono em 28/09/2026: *"eu quero também poder visualizar o ponto do
    mês daquela pessoa. Eu clicar e visualizar o ponto da pessoa no modal."*

    ⚠️ DEVOLVE OS CAMPOS COMO VIERAM, sem interpretar. Enquanto o nome dos campos
    de cada dia não for conhecido (é o que trava o total por obra), inventar
    significado para eles poria o salário na obra errada — e a tela mostrando o
    dado cru é justamente o que permite descobrir o nome certo."""
    import json

    from .db import consultar
    from .folha_rateio import so_digitos

    digitos = so_digitos(cpf)
    carga = carga_do_mes(ano, mes)
    if not _pronto() or not carga or len(digitos) != 11:
        return {"tem_carga": bool(carga), "dias": [], "campos": [],
                "carga": carga}

    linhas = consultar(
        "SELECT data, matricula, campos, obra, presenca, falta "
        "  FROM analisesps.ponto_dia "
        " WHERE carga_id = ? AND cpf = ? ORDER BY data NULLS LAST",
        (carga["id"], digitos))

    dias, nomes = [], []
    for data, matricula, bruto, obra, presenca, falta in linhas:
        try:
            campos = json.loads(bruto) if bruto else {}
        except Exception:  # noqa: BLE001 — JSON torto não pode derrubar o modal
            campos = {}
        if isinstance(campos, dict):
            for chave in campos:
                if chave not in nomes:
                    nomes.append(chave)
        else:
            campos = {}
        # ⚠️ A OBRA DO DIA SAI DAS QUATRO MARCAÇÕES, não da coluna `obra` da
        # tabela: essa coluna nasceu vazia, porque na carga os nomes dos campos
        # ainda não eram conhecidos. Resolver na LEITURA faz o ponto que já está
        # guardado passar a mostrar obra sem ninguém recarregar mês nenhum.
        lido = _dia_lido(data, campos)
        resolvido = _obra_do_dia(lido)
        dias.append({"data": data, "matricula": matricula or "",
                     "obra": resolvido["obra"] or obra or "",
                     "empate": resolvido["empate"],
                     "marcacoes": lido["marcacoes"], "horas": lido["horas"],
                     "presenca": lido["presenca"] or presenca or "",
                     "falta": lido["falta"] or falta or "",
                     "total_de_horas": lido["total_de_horas"],
                     "campos": campos})
    return {"tem_carga": True, "carga": carga, "dias": dias, "campos": nomes}


# ---------------------------------------------------------------------------
# O DIA LIDO: das marcações cruas para a obra do dia
#
# ⚠️ ESTE É O PEDAÇO QUE DESTRAVA O TOTAL POR OBRA DA FOLHA. Até 29/09/2026 a
# apropriação existia inteira (`folha_apropriacao.py`) e não tinha de onde tirar
# a obra de cada dia — o cálculo estava escrito, testado, e sem dado.
# ---------------------------------------------------------------------------
def obras_do_dia(campos) -> list:
    """As quatro obras do dia, na ordem do dia. Vazio onde não houve marcação.

    Devolve sempre quatro posições: `folha_apropriacao.obra_do_dia` conta as
    preenchidas, e uma lista curta mudaria a contagem sem avisar."""
    if not isinstance(campos, dict):
        return ["", "", "", ""]
    return [" ".join(str(campos.get(c) or "").split()).upper()
            for c in CAMPOS_DAS_OBRAS]


def horas_do_dia(campos) -> list:
    """As quatro marcações de hora, na ordem do dia. Só para a tela conferir."""
    if not isinstance(campos, dict):
        return ["", "", "", ""]
    return [str(campos.get(c) or "").strip() for c in CAMPOS_DAS_HORAS]


def _dia_lido(data, campos) -> dict:
    """Um dia no formato que `folha_apropriacao` espera.

    As chaves são `data`, `marcacoes`, `presenca` e `falta` — o contrato de
    `folha_apropriacao._dias_uteis_do_ponto`. `marcacoes` são as OBRAS, não as
    horas: é a obra que se conta para decidir de quem é o dia."""
    campos = campos if isinstance(campos, dict) else {}
    return {
        "data": data,
        "marcacoes": obras_do_dia(campos),
        "horas": horas_do_dia(campos),
        "presenca": " ".join(str(campos.get(CAMPO_PRESENCA) or "").split()),
        "falta": " ".join(str(campos.get(CAMPO_FALTA) or "").split()),
        "total_de_horas": str(campos.get(CAMPO_TOTAL_DE_HORAS) or "").strip(),
        "dia_da_semana": str(campos.get(CAMPO_DIA_DA_SEMANA) or "").strip(),
    }


def _obra_do_dia(lido) -> dict:
    """A obra de um dia lido. O import fica DENTRO porque `folha_apropriacao`
    puxa o rateio, que puxa o banco — e este módulo é chamado na carga, antes de
    qualquer tela."""
    from .folha_apropriacao import obra_do_dia
    return obra_do_dia(lido["marcacoes"], lido["presenca"], lido["falta"])


def dias_por_cpf(ano: int, mes: int) -> dict:
    """`{cpf: [dias]}` do mês inteiro, pronto para a apropriação.

    ⚠️ UMA CONSULTA PARA O MÊS TODO, não uma por pessoa. São ~500 pessoas × 31
    dias: perguntar dentro do laço faria 500 idas ao banco para montar uma tela,
    e é o jeito mais fácil de deixar a folha lenta sem ninguém entender por quê.

    ⚠️ DEVOLVE `{}` QUANDO NÃO HÁ CARGA DO MÊS, e quem chama tem de DIZER isso na
    tela em vez de mostrar zero: folha sem ponto e folha com ponto vazio são
    coisas diferentes, e a segunda é um erro a consertar."""
    import json as _json

    from .db import consultar

    carga = carga_do_mes(ano, mes)
    if not _pronto() or not carga:
        return {}

    saida: dict = {}
    for cpf, data, bruto in consultar(
            "SELECT cpf, data, campos FROM analisesps.ponto_dia "
            " WHERE carga_id = ? AND data IS NOT NULL ORDER BY cpf, data",
            (carga["id"],)):
        if not cpf:
            continue
        try:
            campos = _json.loads(bruto) if bruto else {}
        except Exception:  # noqa: BLE001 — JSON torto de um dia não pode
            campos = {}    # derrubar o mês inteiro; o dia fica sem obra e cai
        saida.setdefault(cpf, []).append(_dia_lido(data, campos))
    return saida
