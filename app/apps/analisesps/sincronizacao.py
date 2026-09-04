# -*- coding: utf-8 -*-
"""
A ponte entre a planilha SPsBD e o Postgres.

TRÊS TRABALHOS, todos rodando no processo separado (ver `executar_sync.py`):

  1. CARGA INICIAL   — traz as 59 mil SPs pela primeira vez. Em blocos, e
                       retomável: se o serviço reiniciar no meio, recomeça do
                       bloco seguinte, não do zero.
  2. SINCRONIZAÇÃO   — o dia a dia. Lê só as colunas A (ID) e V (carimbo),
                       descobre quem mudou desde a última vez e busca apenas
                       essas linhas. É o que faz a atualização custar segundos
                       em vez de minutos.
  3. FILA            — devolve para a planilha o que foi alterado nas telas.

MEMÓRIA. Nunca se abre a planilha inteira de uma vez. `get_all_values()` numa
aba de 59 mil linhas por 38 colunas devolve mais de dois milhões de textos —
mais de 100 MB numa instância de 2 GB dividida com 15 módulos. Por isso a carga
lê BLOCOS e grava cada um antes de pedir o próximo: o pico fica em poucos MB,
não importa o tamanho da base.
"""
from __future__ import annotations

import logging
import os

from . import colunas, formatos
from .credenciais import cliente, com_retry

logger = logging.getLogger("analisesps.sincronizacao")

PLANILHA_SPS = os.getenv(
    "ANALISESPS_SHEET_SPS", "1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA")
ABA_SPS = "SPsBD"

# Planilha de apoio: SP -> documentação fiscal.
PLANILHA_FISCAL = os.getenv(
    "ANALISESPS_SHEET_FISCAL", "1xMu76lEiiJFlCgNNXldraW2enIuHdZL0D5QTuhZAc0w")
ABA_FISCAL = "Lançamentos"

# Quantas linhas por ida à planilha na carga inicial.
#
# Cinco mil é o meio-termo medido: blocos menores multiplicam as idas ao Google
# (e a chance de esbarrar na cota); maiores voltam a inchar a memória, que é
# justamente o que se quer evitar. Cada bloco custa poucos MB e é gravado antes
# do próximo ser pedido.
LINHAS_POR_BLOCO = 5000

# Quantas linhas mudadas se pedem por vez na sincronização do dia.
FAIXAS_POR_LOTE = 200


def _aba(planilha_id: str, nome: str):
    return com_retry(lambda: cliente().open_by_key(planilha_id).worksheet(nome))


def _aba_sps():
    return _aba(PLANILHA_SPS, ABA_SPS)


# ---------------------------------------------------------------------------
# Gravação no banco
# ---------------------------------------------------------------------------
def _valores_da_linha(registro: dict) -> tuple:
    """Monta a tupla na ordem das colunas, já com as derivadas convertidas."""
    valores = [registro.get(c, "") for c in colunas.CHAVES]
    valores.append(formatos.para_numero(registro.get("valor")))
    for origem in ("solicitacao", "vencimento", "data_pagamento", "dt_autorizacao"):
        valores.append(formatos.para_data(registro.get(origem)))
    return tuple(valores)


def _sql_upsert() -> str:
    """INSERT que vira UPDATE quando a SP já existe.

    `ON CONFLICT` em vez de "apaga tudo e insere de novo": a tabela nunca fica
    vazia no meio do caminho, então uma carga interrompida deixa a base velha
    íntegra em vez de deixar buraco."""
    campos = list(colunas.CHAVES) + [
        "valor_num", "solicitacao_d", "vencimento_d",
        "data_pagamento_d", "dt_autorizacao_d"]
    marcadores = ", ".join(["?"] * len(campos))
    nomes = ", ".join(f'"{c}"' for c in campos)
    atualiza = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in campos if c != "id")
    return (f"INSERT INTO analisesps.sps ({nomes}) VALUES ({marcadores}) "
            f"ON CONFLICT (id) DO UPDATE SET {atualiza}, atualizado_em = now()")


def gravar_registros(conn, registros: list[dict]) -> int:
    """Grava um bloco. Devolve quantas linhas entraram."""
    registros = [r for r in registros if str(r.get("id", "")).strip()]
    if not registros:
        return 0
    conn.executemany(_sql_upsert(), [_valores_da_linha(r) for r in registros])
    conn.commit()
    return len(registros)


def _meta_ler(conn, chave: str, padrao: str = "") -> str:
    cur = conn.execute("SELECT valor FROM analisesps.meta WHERE chave = ?", (chave,))
    linha = cur.fetchone()
    cur.close()
    return (linha[0] if linha and linha[0] is not None else padrao)


def _meta_gravar(conn, chave: str, valor: str) -> None:
    conn.execute(
        "INSERT INTO analisesps.meta (chave, valor) VALUES (?, ?) "
        "ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor",
        (chave, str(valor)))
    conn.commit()


def _maior_carimbo(registros: list[dict]) -> str:
    marcas = [str(r.get(colunas.CHAVE_CARIMBO) or "") for r in registros]
    marcas = [m for m in marcas if m]
    return max(marcas) if marcas else ""


# ---------------------------------------------------------------------------
# 1. Carga inicial — em blocos, retomável
# ---------------------------------------------------------------------------
def carga_inicial(anotar=None, retomar_de: int = 0) -> int:
    """Traz a planilha inteira, um bloco de cada vez.

    `retomar_de` é a primeira linha ainda não carregada. Quem chama guarda esse
    número no banco a cada bloco, então uma carga interrompida na linha 40 mil
    recomeça na 40 mil — não na primeira.
    """
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    aba = _aba_sps()
    total_linhas = com_retry(lambda: aba.row_count)
    logger.info("Análise de SPs: carga inicial — a aba tem %d linhas.", total_linhas)

    primeira = max(retomar_de or colunas.PRIMEIRA_LINHA_DADOS,
                   colunas.PRIMEIRA_LINHA_DADOS)
    gravadas = 0
    maior_carimbo = ""

    with conexao() as conn:
        maior_carimbo = _meta_ler(conn, "ultimo_carimbo", "")

    linha = primeira
    while linha <= total_linhas:
        fim = min(linha + LINHAS_POR_BLOCO - 1, total_linhas)
        faixa = f"A{linha}:{colunas.ULTIMA_LETRA}{fim}"
        bruto = com_retry(lambda f=faixa: aba.get(f))

        registros = [colunas.linha_para_dicionario(v)
                     for v in bruto if v and str(v[0]).strip()]
        if registros:
            with conexao() as conn:
                gravadas += gravar_registros(conn, registros)
                carimbo = _maior_carimbo(registros)
                if carimbo > maior_carimbo:
                    maior_carimbo = carimbo
                    _meta_gravar(conn, "ultimo_carimbo", maior_carimbo)
                # A retomada aponta para a PRÓXIMA linha ainda não lida.
                _meta_gravar(conn, "carga_ate_linha", str(fim + 1))

        anotar("trazendo as SPs da planilha",
               f"{gravadas} de aproximadamente {total_linhas - 1}")
        # `bruto` e `registros` saem de escopo aqui: o bloco seguinte não soma
        # memória com este. É o que mantém o pico em poucos MB.
        linha = fim + 1

    with conexao() as conn:
        _meta_gravar(conn, "carga_ate_linha", "")      # terminou: nada a retomar
    logger.info("Análise de SPs: carga inicial concluída — %d SPs.", gravadas)
    return gravadas


# ---------------------------------------------------------------------------
# 2. Sincronização do dia — só o que mudou
# ---------------------------------------------------------------------------
def sincronizar_delta(anotar=None) -> dict:
    """Lê ID e carimbo, busca só as linhas que mudaram, remove as excluídas."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    aba = _aba_sps()

    anotar("conferindo o que mudou na planilha")
    coluna_ids = com_retry(lambda: aba.col_values(colunas.COLS["id"].idx + 1))
    coluna_marcas = com_retry(
        lambda: aba.col_values(colunas.COLS[colunas.CHAVE_CARIMBO].idx + 1))

    with conexao() as conn:
        ultimo = _meta_ler(conn, "ultimo_carimbo", "")
        cur = conn.execute("SELECT id FROM analisesps.sps")
        ids_no_banco = {str(r[0]) for r in cur.fetchall()}
        cur.close()

    linhas_mudadas: list[int] = []
    ids_na_planilha: set[str] = set()
    for numero in range(colunas.PRIMEIRA_LINHA_DADOS, len(coluna_ids) + 1):
        sp_id = str(coluna_ids[numero - 1] or "").strip()
        if not sp_id:
            continue
        ids_na_planilha.add(sp_id)
        marca = str(coluna_marcas[numero - 1] or "").strip() \
            if numero - 1 < len(coluna_marcas) else ""
        if sp_id not in ids_no_banco or (marca and marca > ultimo):
            linhas_mudadas.append(numero)

    # Busca em lote apenas as linhas mudadas, fatiado para não estourar o
    # tamanho do pedido quando muitas mudam de uma vez.
    novas = 0
    maior = ultimo
    for inicio in range(0, len(linhas_mudadas), FAIXAS_POR_LOTE):
        fatia = linhas_mudadas[inicio:inicio + FAIXAS_POR_LOTE]
        faixas = [f"A{n}:{colunas.ULTIMA_LETRA}{n}" for n in fatia]
        blocos = com_retry(lambda f=faixas: aba.batch_get(f))
        registros = [colunas.linha_para_dicionario(b[0])
                     for b in blocos if b and b[0]]
        if registros:
            with conexao() as conn:
                novas += gravar_registros(conn, registros)
            carimbo = _maior_carimbo(registros)
            if carimbo > maior:
                maior = carimbo
        anotar("trazendo as SPs alteradas", f"{novas} de {len(linhas_mudadas)}")

    # Excluídas: estavam no banco e sumiram da planilha.
    sumidas = sorted(ids_no_banco - ids_na_planilha)
    removidas = 0
    if sumidas:
        with conexao() as conn:
            for inicio in range(0, len(sumidas), 500):
                lote = sumidas[inicio:inicio + 500]
                marcadores = ",".join(["?"] * len(lote))
                cur = conn.execute(
                    f"DELETE FROM analisesps.sps WHERE id IN ({marcadores})",
                    tuple(lote))
                removidas += cur.rowcount or 0
                cur.close()
            conn.commit()

    with conexao() as conn:
        if maior and maior != ultimo:
            _meta_gravar(conn, "ultimo_carimbo", maior)
        from .horario import agora
        _meta_gravar(conn, "ultima_sincronizacao", agora().isoformat())

    logger.info("Análise de SPs: sincronização — %d alteradas, %d removidas.",
                novas, removidas)
    return {"alteradas": novas, "removidas": removidas,
            "conferidas": len(ids_na_planilha)}


# ---------------------------------------------------------------------------
# 3. A fila de volta para a planilha
# ---------------------------------------------------------------------------
def drenar_fila(anotar=None) -> dict:
    """Grava na planilha o que está pendente e limpa o que for confirmado.

    Só sai da fila o que o Google confirmar. Falha de rede deixa tudo onde
    está, para a próxima rodada tentar de novo — nada se perde no caminho."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)

    with conexao() as conn:
        cur = conn.execute(
            "SELECT sp_id, coluna, valor FROM analisesps.fila "
            " ORDER BY criado_em LIMIT 2000")
        pendentes = cur.fetchall()
        cur.close()

    if not pendentes:
        return {"gravadas": 0, "restantes": 0}

    anotar("devolvendo alterações para a planilha", f"{len(pendentes)} pendentes")
    aba = _aba_sps()

    # Onde cada SP mora na planilha. Uma leitura da coluna A resolve todas.
    coluna_ids = com_retry(lambda: aba.col_values(colunas.COLS["id"].idx + 1))
    linha_da_sp = {}
    for numero in range(colunas.PRIMEIRA_LINHA_DADOS, len(coluna_ids) + 1):
        sp_id = str(coluna_ids[numero - 1] or "").strip()
        if sp_id and sp_id not in linha_da_sp:
            linha_da_sp[sp_id] = numero

    atualizacoes, gravados, perdidos = [], [], []
    for sp_id, chave, valor in pendentes:
        numero = linha_da_sp.get(str(sp_id))
        coluna = colunas.COLS.get(chave)
        if numero is None or coluna is None:
            perdidos.append((sp_id, chave))
            continue
        atualizacoes.append({"range": f"{coluna.letra}{numero}",
                             "values": [[valor or ""]]})
        gravados.append((sp_id, chave))

    if atualizacoes:
        com_retry(lambda: aba.batch_update(atualizacoes,
                                           value_input_option="USER_ENTERED"))

    with conexao() as conn:
        for sp_id, chave in gravados:
            conn.execute(
                "DELETE FROM analisesps.fila WHERE sp_id = ? AND coluna = ?",
                (sp_id, chave))
            conn.execute(
                "UPDATE analisesps.log_alteracoes SET status = 'enviado', "
                "       enviado_em = now() "
                " WHERE sp_id = ? AND coluna = ? AND status = 'pendente'",
                (sp_id, chave))
        for sp_id, chave in perdidos:
            # Não some da fila: a SP pode voltar a existir (linha filtrada,
            # planilha em edição). Some a contagem de tentativas, e a tela de
            # Log mostra o motivo.
            conn.execute(
                "UPDATE analisesps.fila SET tentativas = tentativas + 1, "
                "       ultimo_erro = ? WHERE sp_id = ? AND coluna = ?",
                ("ID não encontrado na planilha — tentará de novo", sp_id, chave))
        conn.commit()
        cur = conn.execute("SELECT count(*) FROM analisesps.fila")
        restantes = cur.fetchone()[0]
        cur.close()

    logger.info("Análise de SPs: fila — %d gravadas, %d sem linha, %d restantes.",
                len(gravados), len(perdidos), restantes)
    return {"gravadas": len(gravados), "sem_linha": len(perdidos),
            "restantes": restantes}


# ---------------------------------------------------------------------------
# Apoios
# ---------------------------------------------------------------------------
def sincronizar_apoios(anotar=None) -> dict:
    """Traz as duas planilhas de apoio: contas por centro de custo e a
    documentação fiscal por SP."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    contas = fiscais = 0

    anotar("trazendo as contas de pagamento")
    try:
        valores = com_retry(_aba(PLANILHA_SPS, "C. Diários").get_all_values)
        linhas = [(str(v[0]).strip(), str(v[1]).strip() if len(v) > 1 else "")
                  for v in valores[1:] if v and str(v[0]).strip()]
        if linhas:
            with conexao() as conn:
                conn.executemany(
                    "INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                    "VALUES (?, ?) ON CONFLICT (codigo) DO UPDATE SET "
                    "conta_pagamento = EXCLUDED.conta_pagamento", linhas)
                conn.commit()
            contas = len(linhas)
    except Exception:  # noqa: BLE001 — apoio que falta não derruba a carga
        logger.exception("Análise de SPs: falhou ler 'C. Diários'")

    anotar("trazendo a documentação fiscal")
    try:
        valores = com_retry(_aba(PLANILHA_FISCAL, ABA_FISCAL).get_all_values)
        linhas = [(str(v[0]).strip(), str(v[1]).strip() if len(v) > 1 else "")
                  for v in valores[1:] if v and str(v[0]).strip()]
        if linhas:
            with conexao() as conn:
                conn.executemany(
                    "INSERT INTO analisesps.sp_fiscal (sp_id, doc_fiscal) "
                    "VALUES (?, ?) ON CONFLICT (sp_id) DO UPDATE SET "
                    "doc_fiscal = EXCLUDED.doc_fiscal", linhas)
                conn.commit()
            fiscais = len(linhas)
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou ler a planilha fiscal")

    return {"contas": contas, "fiscais": fiscais}


# ---------------------------------------------------------------------------
# Agenda, feriados e as listas do rateio
#
# Três abas curtas, na mesma planilha de credenciais. São dezenas de linhas
# cada, então aqui `get_all_values()` é legítimo — a regra contra ele vale para
# a aba de 59 mil linhas, não para uma de trinta.
# ---------------------------------------------------------------------------
def sincronizar_agenda(anotar=None) -> dict:
    """Traz a aba Agenda e a aba Feriados."""
    from . import agenda
    from .credenciais import SHEET_CREDENCIAIS
    from .db import conexao
    from .formatos import para_data

    anotar = anotar or (lambda *a, **k: None)
    anotar("trazendo a agenda")

    compromissos = 0
    try:
        valores = com_retry(_aba(SHEET_CREDENCIAIS, agenda.ABA_AGENDA).get_all_values)
        registros = []
        if valores:
            cabecalho = [str(x).strip() for x in valores[0]]
            posicao = {c: cabecalho.index(c) for c in agenda.COLUNAS
                       if c in cabecalho}
            for linha in valores[1:]:
                if not any(str(x).strip() for x in linha):
                    continue
                registro = {
                    c: (str(linha[posicao[c]]).strip()
                        if c in posicao and posicao[c] < len(linha) else "")
                    for c in agenda.COLUNAS}
                if registro.get("id"):
                    registros.append(registro)
        if registros:
            with conexao() as conn:
                compromissos = agenda.gravar(conn, registros)
    except Exception:  # noqa: BLE001 — agenda que falta não derruba a carga
        logger.exception("Análise de SPs: falhou ler a aba Agenda")

    feriados = 0
    try:
        valores = com_retry(
            _aba(SHEET_CREDENCIAIS, agenda.ABA_FERIADOS).get_all_values)
        dias = []
        for linha in (valores[1:] if valores else []):
            dia = para_data(linha[0]) if linha else None
            if dia:
                nome = str(linha[1]).strip() if len(linha) > 1 else ""
                dias.append((dia, nome))
        with conexao() as conn:
            feriados = agenda.gravar_feriados(conn, dias)
    except Exception:  # noqa: BLE001 — a aba de feriados é opcional
        logger.info("Análise de SPs: sem aba de feriados locais (é opcional).")

    return {"compromissos": compromissos, "feriados": feriados}


def escrever_compromisso(registro: dict) -> None:
    """Grava um compromisso na aba Agenda da planilha de Credenciais.

    A planilha é a DONA da agenda: o que se cria pela tela tem de nascer lá,
    senão a próxima sincronização traria de volta um mundo sem ele.

    CRIA A ABA SE ELA NÃO EXISTIR. É a causa mais provável de "a agenda não
    funciona": sem a aba, não há o que trazer, e a tela abre vazia sem
    explicar. Criar com o cabeçalho certo resolve na primeira vez que alguém
    cadastra alguma coisa.

    Atualiza a linha do mesmo `id` quando ela já existe; senão acrescenta no
    fim. Uma leitura da coluna do id resolve as duas."""
    from . import agenda
    from .credenciais import SHEET_CREDENCIAIS

    planilha = com_retry(lambda: cliente().open_by_key(SHEET_CREDENCIAIS))
    try:
        aba = com_retry(lambda: planilha.worksheet(agenda.ABA_AGENDA))
    except Exception:  # noqa: BLE001 — a aba não existe ainda
        logger.info("Análise de SPs: criando a aba %r na planilha de "
                    "Credenciais.", agenda.ABA_AGENDA)
        aba = com_retry(lambda: planilha.add_worksheet(
            title=agenda.ABA_AGENDA, rows=200, cols=len(agenda.COLUNAS)))
        com_retry(lambda: aba.update([list(agenda.COLUNAS)], "A1"))

    valores = com_retry(aba.get_all_values)
    cabecalho = [str(x).strip() for x in valores[0]] if valores else []
    if not cabecalho or "id" not in cabecalho:
        # Aba existente, mas sem cabeçalho reconhecível. Escrever por baixo
        # dela embaralharia o que já está lá — melhor parar e dizer.
        if any(any(str(x).strip() for x in linha) for linha in valores):
            raise RuntimeError(
                f"A aba \"{agenda.ABA_AGENDA}\" da planilha de Credenciais "
                "tem conteúdo mas não tem a linha de cabeçalho com as colunas "
                "esperadas (a primeira precisa se chamar 'id'). Ajuste o "
                "cabeçalho antes de cadastrar por aqui.")
        com_retry(lambda: aba.update([list(agenda.COLUNAS)], "A1"))
        cabecalho = list(agenda.COLUNAS)
        valores = [cabecalho]

    linha = [str(registro.get(c, "") or "") for c in cabecalho]

    coluna_id = cabecalho.index("id") + 1
    ids = com_retry(lambda: aba.col_values(coluna_id))
    numero = None
    for i, valor in enumerate(ids[1:], start=2):
        if str(valor).strip() == str(registro.get("id", "")).strip():
            numero = i
            break

    if numero is None:
        com_retry(lambda: aba.append_row(linha, value_input_option="USER_ENTERED"))
    else:
        fim = _letra_da_coluna(len(cabecalho))
        com_retry(lambda: aba.update([linha], f"A{numero}:{fim}{numero}",
                                     value_input_option="USER_ENTERED"))


def _letra_da_coluna(numero: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA. A agenda tem catorze colunas hoje, mas
    contar na mão é o tipo de coisa que quebra no dia em que passar de vinte
    e seis."""
    letras = ""
    while numero > 0:
        numero, resto = divmod(numero - 1, 26)
        letras = chr(65 + resto) + letras
    return letras


def sincronizar_referencias_rateio(anotar=None) -> dict:
    """Traz as obras (aba 'C. Diários') e as categorias (aba 'Plano Financeiro').

    São as listas que a tela de Ratear oferece. Curtas e mudam pouco, mas
    precisam estar certas: um código de obra errado gera um JSON que o Omie
    aceita e lança no lugar errado."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    anotar("trazendo as listas do rateio")
    obras = categorias = 0

    def _ler(aba_nome, coluna_nome, coluna_codigo):
        valores = com_retry(_aba(PLANILHA_SPS, aba_nome).get_all_values)
        if not valores:
            return []
        cabecalho = [str(x).strip().lower() for x in valores[0]]
        try:
            i_nome = cabecalho.index(coluna_nome.lower())
            i_codigo = cabecalho.index(coluna_codigo.lower())
        except ValueError:
            logger.warning("Análise de SPs: a aba '%s' não tem as colunas "
                           "'%s' e '%s'.", aba_nome, coluna_nome, coluna_codigo)
            return []
        saida = []
        for linha in valores[1:]:
            nome = str(linha[i_nome]).strip() if i_nome < len(linha) else ""
            codigo = str(linha[i_codigo]).strip() if i_codigo < len(linha) else ""
            if nome:
                saida.append((nome, codigo))
        return saida

    for tipo, aba_nome, coluna_nome, coluna_codigo in (
            ("obra", "C. Diários", "Obra", "Código"),
            ("categoria", "Plano Financeiro", "Categoria", "Código")):
        try:
            linhas = _ler(aba_nome, coluna_nome, coluna_codigo)
            if not linhas:
                continue
            with conexao() as conn:
                conn.execute(
                    "DELETE FROM analisesps.referencias_rateio WHERE tipo = ?",
                    (tipo,))
                conn.executemany(
                    "INSERT INTO analisesps.referencias_rateio "
                    "  (tipo, nome, codigo) VALUES (?, ?, ?) "
                    "ON CONFLICT (tipo, nome) DO UPDATE SET "
                    "  codigo = EXCLUDED.codigo",
                    [(tipo, nome, codigo) for nome, codigo in linhas])
                conn.commit()
            if tipo == "obra":
                obras = len(linhas)
            else:
                categorias = len(linhas)
        except Exception:  # noqa: BLE001
            logger.exception("Análise de SPs: falhou ler a aba '%s'", aba_nome)

    return {"obras": obras, "categorias": categorias}


def referencias_rateio() -> dict:
    """As listas que a tela de Ratear oferece, prontas para montar as opções."""
    from .db import consultar
    linhas = consultar(
        "SELECT tipo, nome, coalesce(codigo,'') FROM analisesps.referencias_rateio "
        " ORDER BY tipo, nome")
    saida = {"obras": [], "categorias": []}
    for tipo, nome, codigo in linhas:
        chave = "obras" if tipo == "obra" else "categorias"
        saida[chave].append({"nome": nome, "codigo": codigo})
    return saida
