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

# Quanto esperar por página, e quantas vezes tentar. O script do dono usa
# backoff de 1s/2s/4s; aqui é o mesmo, porque a razão é a mesma: a API cai de vez
# em quando e uma página perdida deixa buraco no mês.
SEGUNDOS_DE_ESPERA = 60
TENTATIVAS = 3

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


def _pedir_pagina(ano: int, mes: int, pagina: int) -> dict:
    """Uma página do relatório. Tenta até três vezes, com espera crescente."""
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
                                    timeout=SEGUNDOS_DE_ESPERA)
        except Exception as e:  # noqa: BLE001 — rede oscila
            # ⚠️ ERRO DE CERTIFICADO NÃO SE REPETE: ele não melhora na terceira
            # tentativa, e o recado precisa dizer o que é, porque "falha de
            # conexão" mandaria tentar de novo para sempre.
            if "CERTIFICATE_VERIFY_FAILED" in str(e) or "SSLError" in type(e).__name__:
                raise ErroDoPonto(
                    "o certificado do site do Mobponto não pôde ser verificado "
                    "(CERTIFICATE_VERIFY_FAILED). Isto NÃO é credencial errada nem "
                    "instabilidade: falta uma peça do meio da corrente de "
                    "certificados — o site manda a cadeia incompleta e o navegador "
                    "disfarça, mas este caminho não. "
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
            time.sleep(min(2 ** tentativa, 8))

    raise ErroDoPonto(
        f"não consegui ler a página {pagina} do ponto de {mes:02d}/{ano}: "
        f"{ultimo}.")


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
    """Traz o ponto do mês e guarda. É o que o botão chama.

    Página por página, gravando cada bloco antes de pedir o seguinte: o pico de
    memória fica em poucos MB, não importa o tamanho do mês.
    """
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    if not _pronto():
        raise ErroDoPonto(
            "a tabela do ponto ainda não existe. Aperte "
            '"Aplicar atualizações do banco" em Configurações e tente de novo.')
    if not configurado():
        raise ErroDoPonto(
            "faltam as credenciais do Mobponto. Crie MOBPONTO_AUTHORIZATION e "
            "MOBPONTO_API_KEY no Render.")

    anotar("pedindo a primeira página do ponto")
    primeira = _pedir_pagina(ano, mes, 1)
    resultado = (primeira or {}).get("result") or {}
    funcionarios = resultado.get("funcionarios") or []
    if not funcionarios:
        raise ErroDoPonto(
            f"o Mobponto não devolveu ninguém para {mes:02d}/{ano}. Confira se "
            "o mês está certo e se há ponto lançado nele.")

    total_paginas = 0
    try:
        total_paginas = int(resultado.get("total_paginas") or 0)
    except (TypeError, ValueError):
        total_paginas = 0
    if total_paginas <= 0:
        total_paginas = 1
    avisos = []
    if total_paginas > MAXIMO_DE_PAGINAS:
        avisos.append(
            f"a API disse que há {total_paginas} páginas e o teto é "
            f"{MAXIMO_DE_PAGINAS} — li só até lá. Se o mês tiver mais, o ponto "
            "está incompleto.")
        total_paginas = MAXIMO_DE_PAGINAS

    # SUBSTITUI a carga daquele mês. O CASCADE leva os dias junto, e é numa
    # transação: uma carga sem dias (a antiga apagada, a nova não gravada)
    # mostraria "nenhum dia" como se fosse verdade.
    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.ponto_carga WHERE ano = ? AND mes = ?",
                     (int(ano), int(mes)))
        cur = conn.execute(
            "INSERT INTO analisesps.ponto_carga "
            "  (ano, mes, paginas, carregado_por) VALUES (?,?,?,?) RETURNING id",
            (int(ano), int(mes), total_paginas, str(quem or "")[:120]))
        carga_id = cur.fetchone()[0]
        cur.close()
        conn.commit()

    campos_vistos: dict = {}
    pessoas = set()
    dias_gravados = 0
    paginas_lidas = 0
    pagina = 1
    pendentes: list = []

    def descarregar():
        nonlocal dias_gravados, pendentes
        if not pendentes:
            return
        with conexao() as conn:
            conn.executemany(
                "INSERT INTO analisesps.ponto_dia "
                "  (carga_id, cpf, nome, data, matricula, campos) "
                " VALUES (?,?,?,?,?,?)", pendentes)
            conn.commit()
        dias_gravados += len(pendentes)
        pendentes = []

    while pagina <= total_paginas:
        if pagina == 1:
            dados = primeira
        else:
            anotar("trazendo o ponto", f"página {pagina} de {total_paginas}")
            dados = _pedir_pagina(ano, mes, pagina)
        resultado = (dados or {}).get("result") or {}
        funcionarios = resultado.get("funcionarios") or []
        paginas_lidas += 1

        if not funcionarios:
            # Página vazia no meio é o sinal de fim que a API dá quando
            # `total_paginas` vem otimista. Para em vez de insistir.
            break

        for bruto in funcionarios:
            cpf, nome, dias = _dias_do_funcionario(bruto)
            if cpf:
                pessoas.add(cpf)
            for dia in dias:
                if isinstance(dia, dict):
                    for campo in dia:
                        campos_vistos[str(campo)] = True
                linha = _linha_do_dia(carga_id, cpf, nome, dia)
                if linha is not None:
                    pendentes.append(linha)
            if len(pendentes) >= DIAS_POR_BLOCO:
                descarregar()
        descarregar()
        anotar("trazendo o ponto",
               f"{dias_gravados} dia(s) de {len(pessoas)} pessoa(s)")
        pagina += 1

    sem_data = 0
    from .db import consultar_um
    achado = consultar_um(
        "SELECT count(*) FROM analisesps.ponto_dia "
        " WHERE carga_id = ? AND data IS NULL", (carga_id,))
    sem_data = int((achado or [0])[0] or 0)
    if sem_data:
        avisos.append(
            f"{sem_data} dia(s) vieram sem data que eu consiga ler. Eles ficaram "
            "guardados, com o conteúdo original, para conferência.")

    # ⚠️ OS NOMES DOS CAMPOS SÃO A DESCOBERTA QUE DESTRAVA A APROPRIAÇÃO. Ficam
    # guardados e aparecem na tela: é com eles que se mapeia a obra e as
    # marcações, sem palpite.
    campos = sorted(campos_vistos)
    with conexao() as conn:
        conn.execute(
            "UPDATE analisesps.ponto_carga "
            "   SET paginas_lidas = ?, pessoas = ?, dias = ?, "
            "       campos_vistos = ?, avisos = ? "
            " WHERE id = ?",
            (paginas_lidas, len(pessoas), dias_gravados, "|".join(campos),
             " | ".join(avisos), carga_id))
        conn.commit()

    logger.info("Análise de SPs: ponto %02d/%d — %d dia(s) de %d pessoa(s) em "
                "%d página(s). Campos: %s", mes, ano, dias_gravados,
                len(pessoas), paginas_lidas, ", ".join(campos))
    return {"id": carga_id, "ano": int(ano), "mes": int(mes),
            "pessoas": len(pessoas), "dias": dias_gravados,
            "paginas": total_paginas, "paginas_lidas": paginas_lidas,
            "campos": campos, "avisos": avisos}


# ---------------------------------------------------------------------------
# Ler de volta
# ---------------------------------------------------------------------------
CAMPOS_DA_CARGA = ("id", "ano", "mes", "paginas", "paginas_lidas", "pessoas",
                   "dias", "campos_vistos", "avisos", "carregado_em",
                   "carregado_por")


def _carga(linha) -> dict:
    carga = {c: linha[i] for i, c in enumerate(CAMPOS_DA_CARGA)}
    carga["competencia"] = f"{carga['mes']:02d}/{carga['ano']}"
    carga["campos"] = [c for c in (carga["campos_vistos"] or "").split("|") if c]
    carga["lista_de_avisos"] = [a for a in (carga["avisos"] or "").split(" | ")
                                if a]
    carga["completa"] = carga["paginas_lidas"] >= carga["paginas"]
    return carga


def cargas(teto: int = 36) -> list:
    """As cargas do ponto, da mais recente para a mais antiga."""
    from .db import consultar
    if not _pronto():
        return []
    return [_carga(l) for l in consultar(
        "SELECT " + ", ".join(CAMPOS_DA_CARGA) + " FROM analisesps.ponto_carga "
        " ORDER BY ano DESC, mes DESC LIMIT ?", (int(teto),))]


def carga_do_mes(ano: int, mes: int) -> dict | None:
    """A carga de uma competência, ou None."""
    from .db import consultar_um
    if not _pronto():
        return None
    linha = consultar_um(
        "SELECT " + ", ".join(CAMPOS_DA_CARGA) + " FROM analisesps.ponto_carga "
        " WHERE ano = ? AND mes = ?", (int(ano), int(mes)))
    return _carga(linha) if linha else None


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
