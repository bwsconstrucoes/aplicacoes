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

import datetime as dt
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

# Relatório do FSist: as notas emitidas CONTRA os CNPJs da BWS. Mesma planilha
# da aba Lançamentos — é para lá que o script do dono já despeja o relatório.
ABA_NOTAS = "Relatório FSIST"


def _normalizar_cabecalho(cabecalho) -> list:
    """O cabeçalho pronto para comparar: sem espaço sobrando, sem caixa."""
    return [" ".join(str(c).split()).strip().lower() for c in cabecalho]


def achar_coluna(cabecalho_normalizado, aceitos):
    """A posição da primeira coluna aceita que existir, ou None.

    ACEITA MAIS DE UM NOME de propósito, e essa peça já se perdeu uma vez: o
    Streamlit procurava "Código Primário" e, se não achasse, "Obra"; a
    conversão ficou só com a segunda — justamente a que a planilha NÃO tem, e
    a lista do rateio nunca carregou desde a estreia (10/09/2026).

    Procurar PELO NOME, e não pela posição, é também o único acerto do script
    que roda na planilha do dono hoje que valia a pena copiar inteiro: uma
    coluna que muda de lugar no relatório deixa de quebrar a importação."""
    for nome in aceitos:
        arrumado = " ".join(str(nome).split()).strip().lower()
        if arrumado in cabecalho_normalizado:
            return cabecalho_normalizado.index(arrumado)
    return None


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


def _abas_existentes(planilha_id: str) -> list[str]:
    """Os nomes das abas que a planilha REALMENTE tem.

    Serve para o recado: dizer "não achei a aba 'C. Diários'" sem dizer quais
    existem obriga a pessoa a adivinhar. Se nem isso der para descobrir,
    devolve lista vazia — o recado fica mais pobre, não vira erro."""
    try:
        return [a.title for a in com_retry(
            lambda: cliente().open_by_key(planilha_id).worksheets())]
    except Exception:  # noqa: BLE001 — é enfeite do recado, não a resposta
        return []


def _explicar_aba(planilha_id: str, nome: str, erro: Exception) -> str:
    """Por que não deu para ler esta aba, em português e com o que ajuda."""
    existentes = _abas_existentes(planilha_id)
    if existentes and nome not in existentes:
        return (f'a aba "{nome}" não existe nesta planilha. '
                f'As que existem são: {", ".join(existentes)}.')
    return f'não deu para ler a aba "{nome}": {erro}'.strip()


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


def _anotar_a_base_em_dia(conn) -> None:
    """Anota a hora e QUANTAS SPs ficaram na base, no fim de uma carga ou de
    uma sincronização.

    A contagem fica guardada porque `count(*)` percorre a tabela inteira, e a
    tela pergunta "quantas SPs há na base" em TODA visita. Contando aqui, no
    processo separado onde um segundo a mais não incomoda ninguém, nenhuma
    tela precisa contar. Ver `consultas.base_carregada`, onde está o porquê
    inteiro.

    A HORA também é gravada pela carga inicial, e não só pela sincronização do
    dia. Uma carga acabada de rodar É a base em dia: sem isto, a tela dizia
    "base de —" até a primeira sincronização passar, e o relógio do alto — que
    é como se sabe de quando é o dado — ficava mudo justamente no dia da
    estreia."""
    from .horario import agora
    quando = agora().isoformat()
    _meta_gravar(conn, "ultima_sincronizacao", quando)
    try:
        cur = conn.execute("SELECT count(*) FROM analisesps.sps")
        linha = cur.fetchone()
        cur.close()
        _meta_gravar(conn, "quantidade", str(linha[0] if linha else 0))
        _meta_gravar(conn, "quantidade_em", quando)
    except Exception:  # noqa: BLE001 — sem a contagem a tela conta sozinha
        logger.exception("Análise de SPs: falhou contar a base no fim da carga")


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
                    # Um segundo atrás, pelo mesmo motivo do delta: a planilha
                    # pode estar sendo escrita enquanto a carga lê, e o que
                    # empatar no segundo da borda seria perdido.
                    _meta_gravar(conn, "ultimo_carimbo",
                                 _marca_dagua(maior_carimbo))
                # A retomada aponta para a PRÓXIMA linha ainda não lida.
                _meta_gravar(conn, "carga_ate_linha", str(fim + 1))

        anotar("trazendo as SPs da planilha",
               f"{gravadas} de aproximadamente {total_linhas - 1}")
        # `bruto` e `registros` saem de escopo aqui: o bloco seguinte não soma
        # memória com este. É o que mantém o pico em poucos MB.
        linha = fim + 1

    with conexao() as conn:
        _meta_gravar(conn, "carga_ate_linha", "")      # terminou: nada a retomar
        _anotar_a_base_em_dia(conn)
    logger.info("Análise de SPs: carga inicial concluída — %d SPs.", gravadas)
    return gravadas


# ---------------------------------------------------------------------------
# 2. Sincronização do dia — só o que mudou
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# AS COLUNAS QUE SÃO CONFERIDAS UMA A UMA, ALÉM DO CARIMBO
#
# POR QUE ISTO EXISTE, e custou uma SP errada na tela do dono em 11/09/2026:
# ele viu a SP 1443253428 como "Pagar" no lote enquanto a planilha já dizia
# "Pago", e a base estava recém-sincronizada.
#
# A causa é o carimbo. Ele é escrito pelo gatilho `onEdit` da planilha, e esse
# gatilho **não dispara quando quem escreve é um script** — e quem alimenta a
# SPsBD são scripts, como o dono confirmou em 11/09. Resultado: a célula muda,
# o carimbo não, e a sincronização do dia nunca reexamina aquela linha.
# Conferido na planilha de verdade: entre as primeiras 63 linhas visíveis, 5
# estão com o carimbo VAZIO.
#
# Linha com carimbo vazio nunca era relida. Nunca mesmo — não era atraso, era
# permanente, até alguém editar a célula na mão.
#
# A CONFERÊNCIA custa uma leitura de coluna a mais por sincronização. É barato
# perto do estrago: status errado na tela de pagamentos faz pagar de novo o que
# já foi pago. Cada coluna acrescentada aqui é mais uma leitura — por isso a
# lista tem só o que decide dinheiro, e não a planilha inteira.
COLUNAS_CONFERIDAS = ["status_pgt"]

# O carimbo se repete: um script que grava 800 linhas de uma vez carimba todas
# com O MESMO SEGUNDO (visto na planilha: 58 linhas com "2026-09-04 16:05:23").
# Se a varredura pegar metade dessas linhas, a marca d'água sobe para aquele
# segundo e a outra metade — carimbada igual — nunca mais satisfaz "maior que".
# Some para sempre.
#
# Por isso a marca d'água fica UM SEGUNDO ATRÁS do maior carimbo visto: o
# segundo da borda é reexaminado na rodada seguinte. Custa reler um punhado de
# linhas; evita perder as que empataram.
def _marca_dagua(maior_carimbo: str) -> str:
    """O carimbo guardado, um segundo atrás do maior visto. Ver acima."""
    try:
        quando = dt.datetime.strptime(maior_carimbo[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # Formato diferente do esperado: guarda como veio. Recuar às cegas
        # numa string que não é data faria a marca d'água virar lixo.
        return maior_carimbo
    return (quando - dt.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")


def sincronizar_delta(anotar=None) -> dict:
    """Lê ID e carimbo, busca só as linhas que mudaram, remove as excluídas.

    Lê TAMBÉM as colunas de `COLUNAS_CONFERIDAS` e compara com o que está no
    banco: é isso que alcança a linha cujo carimbo não foi escrito."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    aba = _aba_sps()

    anotar("conferindo o que mudou na planilha")
    coluna_ids = com_retry(lambda: aba.col_values(colunas.COLS["id"].idx + 1))
    coluna_marcas = com_retry(
        lambda: aba.col_values(colunas.COLS[colunas.CHAVE_CARIMBO].idx + 1))
    conferidas = {
        chave: com_retry(lambda c=chave: aba.col_values(colunas.COLS[c].idx + 1))
        for chave in COLUNAS_CONFERIDAS if chave in colunas.COLS}

    with conexao() as conn:
        ultimo = _meta_ler(conn, "ultimo_carimbo", "")
        campos = ", ".join(f'"{c}"' for c in conferidas)
        cur = conn.execute(
            f"SELECT id{', ' + campos if campos else ''} FROM analisesps.sps")
        no_banco = {str(r[0]): r[1:] for r in cur.fetchall()}
        cur.close()
    ids_no_banco = set(no_banco)

    def _celula(coluna: list, numero: int) -> str:
        return str(coluna[numero - 1] or "").strip() \
            if numero - 1 < len(coluna) else ""

    linhas_mudadas: list[int] = []
    ids_na_planilha: set[str] = set()
    pelo_conteudo = 0
    for numero in range(colunas.PRIMEIRA_LINHA_DADOS, len(coluna_ids) + 1):
        sp_id = str(coluna_ids[numero - 1] or "").strip()
        if not sp_id:
            continue
        ids_na_planilha.add(sp_id)
        marca = _celula(coluna_marcas, numero)
        if sp_id not in ids_no_banco or (marca and marca > ultimo):
            linhas_mudadas.append(numero)
            continue
        # O carimbo não acusou. Confere o conteúdo das colunas que decidem
        # dinheiro: é aqui que entra a linha que um script mudou sem carimbar.
        guardado = no_banco.get(sp_id) or ()
        for posicao, chave in enumerate(conferidas):
            na_planilha = _celula(conferidas[chave], numero)
            bruto = guardado[posicao] if posicao < len(guardado) else None
            atual = str(bruto if bruto is not None else "").strip()
            if na_planilha != atual:
                linhas_mudadas.append(numero)
                pelo_conteudo += 1
                break

    if pelo_conteudo:
        # Vai para o log do Render de propósito: é o número que diz quanto o
        # carimbo está deixando passar. Se ele for alto todo dia, o gatilho da
        # planilha não está carimbando o que os scripts escrevem.
        logger.warning(
            "Análise de SPs: %d linha(s) mudaram SEM carimbo novo — achadas "
            "conferindo %s.", pelo_conteudo, ", ".join(conferidas))
        anotar("conferindo o que mudou na planilha",
               f"{pelo_conteudo} sem carimbo novo")

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
            # Um segundo atrás do maior visto — ver `_marca_dagua`.
            _meta_gravar(conn, "ultimo_carimbo", _marca_dagua(maior))
        _anotar_a_base_em_dia(conn)
        # Quantas linhas realmente desceram. A tela mostra a HORA da última
        # sincronização, e a hora é gravada mesmo quando nada mudou — então o
        # relógio batendo NÃO prova que o dado veio. Este número prova.
        _meta_gravar(conn, "ultima_sincronizacao_alteradas", str(novas))
        _meta_gravar(conn, "ultima_sincronizacao_sem_carimbo", str(pelo_conteudo))

    logger.info("Análise de SPs: sincronização — %d alteradas (%d sem carimbo "
                "novo), %d removidas.", novas, pelo_conteudo, removidas)
    return {"alteradas": novas, "removidas": removidas,
            "sem_carimbo": pelo_conteudo,
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
    documentação fiscal por SP.

    O `WHERE ... IS DISTINCT FROM` no fim de cada gravação não é detalhe.
    Sem ele, esta função REESCREVIA todas as linhas das duas tabelas a cada
    passagem, mesmo quando nada havia mudado — e ela passa a cada
    sincronização. Na produção isso apareceu em 10/09/2026, na tela do banco:
    **14,3 MILHÕES** de gravações em `sp_fiscal`, o campeão disparado de todo
    o banco, 34 minutos de tempo de processador num banco que tem um DÉCIMO
    de um núcleo.

    E o custo não é só o tempo: no Postgres, reescrever uma linha com o mesmo
    valor deixa a versão antiga como lixo para o faxineiro recolher depois.
    Dezenas de milhares de linhas de lixo a cada cinco minutos é o que engorda
    a tabela até ela não caber mais na memória do banco — que é exatamente a
    lentidão que se estava caçando.

    Com a condição, a gravação só acontece quando o valor MUDOU de verdade.
    O resultado final é idêntico; o que some é o trabalho inútil."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    contas = fiscais = 0
    avisos: list[str] = []

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
                    "conta_pagamento = EXCLUDED.conta_pagamento "
                    " WHERE contas_diarios.conta_pagamento "
                    "       IS DISTINCT FROM EXCLUDED.conta_pagamento", linhas)
                conn.commit()
            contas = len(linhas)
        else:
            avisos.append('a aba "C. Diários" não trouxe nenhuma conta.')
    except Exception as e:  # noqa: BLE001 — apoio que falta não derruba a carga
        logger.exception("Análise de SPs: falhou ler 'C. Diários'")
        avisos.append(_explicar_aba(PLANILHA_SPS, "C. Diários", e))

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
                    "doc_fiscal = EXCLUDED.doc_fiscal "
                    " WHERE sp_fiscal.doc_fiscal "
                    "       IS DISTINCT FROM EXCLUDED.doc_fiscal", linhas)
                conn.commit()
            fiscais = len(linhas)
        else:
            avisos.append(f'a aba "{ABA_FISCAL}" não trouxe nenhum documento.')
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou ler a planilha fiscal")
        avisos.append(_explicar_aba(PLANILHA_FISCAL, ABA_FISCAL, e))

    return {"contas": contas, "fiscais": fiscais, "avisos": avisos}


# ---------------------------------------------------------------------------
# AS NOTAS EMITIDAS CONTRA O CNPJ DA BWS (relatório do FSist)
#
# O FSist monitora os CNPJs da empresa e entrega o que foi emitido contra eles.
# O dono despeja esse relatório numa aba, e é de lá que se lê — assim o fluxo
# dele não muda e ninguém precisa aprender a subir arquivo.
#
# OS NOMES DE COLUNA E OS APELIDOS SÃO OS DO SCRIPT DELE, copiados de
# propósito: é a parte que aquele script acerta, e o relatório do FSist muda de
# layout entre NF-e e CT-e (num é "Destinatário", noutro é "Tomador").
#
# A CHAVE É A IDENTIDADE, então reimportar o mesmo relatório não duplica nada.
# E só se grava o que MUDOU — `IS DISTINCT FROM` —, pelo mesmo motivo que valeu
# 14,3 milhões de gravações inúteis em 10/09: regravar com o mesmo valor deixa
# lixo que engorda a tabela até ela não caber na memória do banco.
# ---------------------------------------------------------------------------
COLUNAS_DAS_NOTAS = {
    "emissao":     ["Emissão", "Data Emissão", "Data de Emissão"],
    "chave":       ["Chave", "Chave de Acesso"],
    "numero":      ["Número", "Nº", "Num"],
    "serie":       ["Série"],
    "tipo":        ["Tipo"],
    "valor":       ["Valor", "Valor Total"],
    "status":      ["Status", "Situação"],
    "emitente_doc": ["Emitente CNPJ", "Emitente CNPJ/CPF", "CNPJ Emitente"],
    "emitente":    ["Emitente", "Emitente Nome", "Nome Emitente",
                    "Emitente Razão Social"],
    "emitente_uf": ["Emitente UF", "UF Emitente"],
    # No CT-e quem paga o frete é o TOMADOR; na NF-e é o DESTINATÁRIO. Os dois
    # são a BWS, e por isso ocupam a mesma coluna aqui.
    "destinatario_doc": ["Destinatário CNPJ/CPF", "Destinatário CPF/CNPJ",
                         "Destinatário CNPJ", "CNPJ/CPF Destinatário",
                         "Tomador CNPJ/CPF", "Tomador CPF/CNPJ", "Tomador CNPJ"],
    "destinatario": ["Destinatário", "Destinatário Nome", "Nome Destinatário",
                     "Destinatário Razão Social", "Tomador", "Tomador Nome"],
    "chaves_nfe":  ["Chaves NFE Tranporte", "Chaves NFe Transporte",
                    "NFe Chaves", "NFe Chaves (com vírgula)"],
}

# Sem estas duas não há nota: uma linha sem chave não é identificável, e sem
# emitente não há como casar com credor nenhum.
COLUNAS_OBRIGATORIAS_DAS_NOTAS = ["chave"]


def sincronizar_notas_fiscais(anotar=None) -> dict:
    """Traz as notas do relatório do FSist para o banco.

    Devolve {novas, atualizadas, ignoradas, avisos}. `ignoradas` são as linhas
    sem chave — lixo de rodapé, totalizador, linha em branco no meio."""
    from . import fiscal
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    anotar("trazendo as notas emitidas contra a empresa")
    avisos: list = []

    try:
        valores = com_retry(_aba(PLANILHA_FISCAL, ABA_NOTAS).get_all_values)
    except Exception as e:  # noqa: BLE001
        motivo = _explicar_aba(PLANILHA_FISCAL, ABA_NOTAS, e)
        logger.warning("Análise de SPs: notas — %s", motivo)
        return {"novas": 0, "atualizadas": 0, "ignoradas": 0, "avisos": [motivo]}

    # O CABEÇALHO NÃO ESTÁ NA PRIMEIRA LINHA. Na planilha do dono a linha 1 é
    # o título ("Relatório de Notas de Compras") e a 2 é o cabeçalho de verdade.
    # Procurar a linha que TEM a coluna "Chave" é mais robusto do que fixar o
    # número: o dia em que alguém inserir uma linha acima, nada quebra.
    linha_cab = -1
    indices: dict = {}
    for i, linha in enumerate(valores[:10]):
        normalizado = _normalizar_cabecalho(linha)
        achados = {campo: achar_coluna(normalizado, nomes)
                   for campo, nomes in COLUNAS_DAS_NOTAS.items()}
        if all(achados.get(c) is not None for c in COLUNAS_OBRIGATORIAS_DAS_NOTAS):
            linha_cab, indices = i, achados
            break

    if linha_cab < 0:
        # MOSTRA AS PRIMEIRAS LINHAS QUE FORAM OLHADAS, e não uma linha fixa: o
        # cabeçalho pode estar em qualquer uma delas, e apontar a errada manda
        # a pessoa conferir o lugar errado da planilha.
        olhadas = []
        for linha in valores[:3]:
            texto = ", ".join(str(x).strip() for x in linha if str(x).strip())
            if texto:
                olhadas.append(texto[:160])
        motivo = (f'a aba "{ABA_NOTAS}" não tem a coluna "Chave" nas primeiras '
                  "linhas. O que encontrei foi: "
                  + (" | ".join(olhadas) or "(nada)") + ".")
        logger.warning("Análise de SPs: notas — %s", motivo)
        return {"novas": 0, "atualizadas": 0, "ignoradas": 0, "avisos": [motivo]}

    faltando = [c for c, i in indices.items() if i is None]
    if faltando:
        avisos.append("colunas não encontradas (seguindo sem elas): "
                      + ", ".join(sorted(faltando)))

    def pegar(linha, campo):
        i = indices.get(campo)
        return str(linha[i]).strip() if i is not None and i < len(linha) else ""

    registros = []
    ignoradas = 0
    for linha in valores[linha_cab + 1:]:
        chave = fiscal.so_digitos(pegar(linha, "chave"))
        if len(chave) != 44:
            ignoradas += 1
            continue
        # O CNPJ DE QUEM EMITIU SAI DE DENTRO DA CHAVE quando a coluna não
        # veio. São os dígitos 7 a 20, por definição da Receita — mais
        # confiável do que a coluna, que vem com formatação variada.
        emitente_doc = (fiscal.so_digitos(pegar(linha, "emitente_doc"))
                        or fiscal.emitente_da_chave(chave))
        registros.append((
            chave, formatos.para_data(pegar(linha, "emissao")),
            pegar(linha, "numero"), pegar(linha, "serie"), pegar(linha, "tipo"),
            formatos.para_numero(pegar(linha, "valor")),
            pegar(linha, "status"), emitente_doc, pegar(linha, "emitente"),
            pegar(linha, "emitente_uf"),
            fiscal.so_digitos(pegar(linha, "destinatario_doc")),
            pegar(linha, "destinatario"), pegar(linha, "chaves_nfe")))

    if not registros:
        avisos.append(f'a aba "{ABA_NOTAS}" não trouxe nenhuma nota com chave.')
        return {"novas": 0, "atualizadas": 0, "ignoradas": ignoradas,
                "avisos": avisos}

    # O RELÓGIO DO BANCO, e não o de Python: o servidor pode estar em outro
    # fuso, e comparar carimbo do banco com hora daqui erraria a contagem
    # inteira — para mais ou para menos, conforme a diferença.
    with conexao() as conn:
        cur = conn.execute(
            "SELECT count(*), now() FROM analisesps.notas_fiscais")
        linha = cur.fetchone() or [0, None]
        antes, comeco = linha[0], linha[1]
        cur.close()
        conn.executemany(
            "INSERT INTO analisesps.notas_fiscais "
            "  (chave, emissao, numero, serie, tipo, valor, status, "
            "   emitente_doc, emitente, emitente_uf, destinatario_doc, "
            "   destinatario, chaves_nfe) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET "
            "  emissao = EXCLUDED.emissao, numero = EXCLUDED.numero, "
            "  serie = EXCLUDED.serie, tipo = EXCLUDED.tipo, "
            "  valor = EXCLUDED.valor, status = EXCLUDED.status, "
            "  emitente_doc = EXCLUDED.emitente_doc, "
            "  emitente = EXCLUDED.emitente, emitente_uf = EXCLUDED.emitente_uf, "
            "  destinatario_doc = EXCLUDED.destinatario_doc, "
            "  destinatario = EXCLUDED.destinatario, "
            "  chaves_nfe = EXCLUDED.chaves_nfe, importada_em = now() "
            " WHERE notas_fiscais.status IS DISTINCT FROM EXCLUDED.status "
            "    OR notas_fiscais.valor IS DISTINCT FROM EXCLUDED.valor "
            "    OR notas_fiscais.numero IS DISTINCT FROM EXCLUDED.numero "
            "    OR notas_fiscais.emitente_doc IS DISTINCT FROM EXCLUDED.emitente_doc",
            registros)
        conn.commit()
        cur = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE importada_em >= ?) "
            "  FROM analisesps.notas_fiscais", (comeco,))
        linha = cur.fetchone() or [0, 0]
        depois, tocadas = linha[0], linha[1]
        cur.close()

    # TRÊS NÚMEROS, E NÃO DOIS, e o dono pediu exatamente assim em 11/09/2026:
    # *"na hora que você for importar, se aquela informação de nota já estiver
    # dentro, você vai ignorar; e quando importar vai dizer quantos importou,
    # que conseguiu, que já tinha, que não tinha"*.
    #
    # "MUDARAM" É O NÚMERO QUE INTERESSA, e ele não existia antes: uma nota
    # que volta no relatório com status CANCELADA é notícia — pode ser despesa
    # já paga contra documento que não existe mais. Antes ela se escondia no
    # meio das "atualizadas", que na verdade contavam as inalteradas também.
    #
    # O `importada_em` só é tocado quando algo mudou de verdade (é o `WHERE`
    # do ON CONFLICT ali em cima), então este é o número certo.
    novas = depois - antes
    mudaram = max(0, tocadas - novas)
    ja_tinha = max(0, len(registros) - novas - mudaram)
    logger.info("Análise de SPs: notas do FSist — %d lidas, %d novas, "
                "%d mudaram, %d já tinha, %d ignoradas.",
                len(registros), novas, mudaram, ja_tinha, ignoradas)
    return {"novas": novas, "mudaram": mudaram, "ja_tinha": ja_tinha,
            # `atualizadas` fica pelo nome antigo, para nada que já lia isto
            # quebrar — mas quem for escrever recado novo usa os três acima.
            "atualizadas": mudaram, "lidas": len(registros),
            "ignoradas": ignoradas, "avisos": avisos}


# ---------------------------------------------------------------------------
# O PASSADO: o que já está preenchido nos cards
#
# Dos lançamentos da planilha do dono, um terço já tem chave de acesso e
# categoria — preenchidos à mão, ao longo dos meses. Essa informação **só
# existe no card**, porque a base SPsBD não tem esses campos, e o dono decidiu
# NÃO mexer nela.
#
# Por isso o relatório do Pipefy é lido UMA VEZ, para o diário nascer sabendo
# o que já foi feito. Decisão dele, em 11/09/2026: *"à medida que forem
# lançados novos registros, eles vão aparecer. Então não precisa ficar toda
# hora baixando, já que está gravando a informação complementar no outro
# canto."*
#
# A REGRA QUE PROTEGE O TRABALHO: a semeadura NUNCA sobrescreve uma linha que
# já existe aqui. Se este módulo já decidiu alguma coisa sobre uma SP, o que
# veio do relatório é história velha — e história velha não manda em decisão
# nova. `ON CONFLICT DO NOTHING` diz isso ao banco, em vez de confiar em quem
# lembra da regra.
# ---------------------------------------------------------------------------
CHAVE_SEMEADURA = "analise_fiscal_semeada_em"

COLUNAS_DOS_LANCAMENTOS = {
    "sp_id":        ["ID SP", "Código", "ID do Card"],
    "documentacao": ["Documentação Fiscal"],
    "chave":        ["Chave de Acesso", "Chave"],
    "numero_nota":  ["Nº da Nota Fiscal", "N da Nota Fiscal",
                     "Número da Nota Fiscal", "Nº Nota Fiscal"],
}


def semear_analise_do_pipefy(anotar=None, forcar: bool = False) -> dict:
    """Traz para o diário o que já está preenchido nos cards. Roda UMA vez.

    `forcar` existe para o dia em que alguém precisar refazer — e mesmo assim
    não sobrescreve nada que já tenha sido decidido aqui."""
    from . import fiscal
    from .db import conexao
    from .horario import agora

    anotar = anotar or (lambda *a, **k: None)

    with conexao() as conn:
        if not forcar and _meta_ler(conn, CHAVE_SEMEADURA, ""):
            return {"semeadas": 0, "ja_existiam": 0, "pulada": True, "avisos": []}

    anotar("trazendo o que já está preenchido nos cards")
    try:
        valores = com_retry(_aba(PLANILHA_FISCAL, ABA_FISCAL).get_all_values)
    except Exception as e:  # noqa: BLE001
        motivo = _explicar_aba(PLANILHA_FISCAL, ABA_FISCAL, e)
        return {"semeadas": 0, "ja_existiam": 0, "pulada": False,
                "avisos": [motivo]}

    linha_cab, indices = -1, {}
    for i, linha in enumerate(valores[:10]):
        normalizado = _normalizar_cabecalho(linha)
        achados = {campo: achar_coluna(normalizado, nomes)
                   for campo, nomes in COLUNAS_DOS_LANCAMENTOS.items()}
        if achados.get("sp_id") is not None and achados.get("documentacao") is not None:
            linha_cab, indices = i, achados
            break

    if linha_cab < 0:
        olhadas = [", ".join(str(x).strip() for x in l if str(x).strip())[:160]
                   for l in valores[:3]]
        return {"semeadas": 0, "ja_existiam": 0, "pulada": False, "avisos": [
            f'a aba "{ABA_FISCAL}" não tem as colunas "ID SP" e "Documentação '
            f'Fiscal". O que encontrei foi: {" | ".join(x for x in olhadas if x)}.']}

    def pegar(linha, campo):
        i = indices.get(campo)
        return str(linha[i]).strip() if i is not None and i < len(linha) else ""

    registros = []
    for linha in valores[linha_cab + 1:]:
        sp_id = fiscal.so_digitos(pegar(linha, "sp_id"))
        if not sp_id:
            continue
        documentacao = pegar(linha, "documentacao")
        chave = fiscal.so_digitos(pegar(linha, "chave"))
        if len(chave) != 44:
            chave = ""
        # SEM CATEGORIA E SEM CHAVE NÃO HÁ O QUE SEMEAR: essa SP entra na fila
        # normal, e semeá-la como "pendente" só encheria a tabela.
        if not documentacao and not chave:
            continue
        registros.append((
            sp_id,
            "ESCRITA" if documentacao else "PENDENTE",
            documentacao, chave, pegar(linha, "numero_nota"),
            "Sim" if chave else "",
            fiscal.dedutivel(documentacao) if documentacao else None,
            "PIPEFY",
            "veio preenchido no card, antes de esta tela existir"))

    if not registros:
        return {"semeadas": 0, "ja_existiam": 0, "pulada": False, "avisos": [
            f'a aba "{ABA_FISCAL}" não trouxe nenhum lançamento já analisado.']}

    with conexao() as conn:
        cur = conn.execute("SELECT count(*) FROM analisesps.sp_fiscal_analise")
        antes = (cur.fetchone() or [0])[0]
        cur.close()
        conn.executemany(
            "INSERT INTO analisesps.sp_fiscal_analise "
            "  (sp_id, situacao, documentacao, chave, numero_nota, gerou_nota, "
            "   dedutivel, origem, motivo) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (sp_id) DO NOTHING", registros)
        cur = conn.execute("SELECT count(*) FROM analisesps.sp_fiscal_analise")
        depois = (cur.fetchone() or [0])[0]
        cur.close()
        _meta_gravar(conn, CHAVE_SEMEADURA, agora().isoformat())
        conn.commit()

    semeadas = depois - antes
    logger.info("Análise de SPs: semeadura fiscal — %d lidas, %d novas.",
                len(registros), semeadas)
    return {"semeadas": semeadas, "ja_existiam": len(registros) - semeadas,
            "pulada": False, "avisos": []}


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
    avisos: list[str] = []

    # POR QUE ESTA FUNÇÃO DEVOLVE O MOTIVO, E NÃO SÓ A LISTA. Em 10/09/2026 o
    # dono encontrou a tela de Ratear dizendo "as listas ainda não foram
    # carregadas", apertou o botão que a própria tela mandava apertar, o botão
    # disse "concluída", e nada mudou. As três causas possíveis — aba com outro
    # nome, coluna com outro nome, aba vazia — eram engolidas por um `continue`
    # e por um aviso no log do serviço, que ele não tem como ler.
    #
    # Falha silenciosa em botão que a tela manda apertar é armadilha: a pessoa
    # aperta de novo, e de novo, e conclui que o sistema está quebrado. Agora
    # cada motivo volta escrito, chega à mensagem da execução e aparece em
    # Configurações — com os nomes que a planilha REALMENTE tem.
    def _ler(aba_nome, aceitos_nome, aceitos_codigo):
        """Devolve (linhas, motivo). `motivo` é None quando deu certo."""
        try:
            valores = com_retry(_aba(PLANILHA_SPS, aba_nome).get_all_values)
        except Exception as e:  # noqa: BLE001
            return [], _explicar_aba(PLANILHA_SPS, aba_nome, e)
        if not valores:
            return [], f'a aba "{aba_nome}" está vazia.'
        cabecalho = [str(x).strip() for x in valores[0]]
        normalizado = _normalizar_cabecalho(cabecalho)

        i_nome = achar_coluna(normalizado, aceitos_nome)
        i_codigo = achar_coluna(normalizado, aceitos_codigo)
        faltando = ([aceitos_nome] if i_nome is None else []) + \
                   ([aceitos_codigo] if i_codigo is None else [])
        if faltando:
            quais = "; ".join(" ou ".join(f'"{n}"' for n in g) for g in faltando)
            return [], (f'a aba "{aba_nome}" não tem a(s) coluna(s) {quais}. '
                        f'O cabeçalho dela é: {", ".join(cabecalho) or "(vazio)"}.')

        saida = []
        for linha in valores[1:]:
            nome = str(linha[i_nome]).strip() if i_nome < len(linha) else ""
            codigo = str(linha[i_codigo]).strip() if i_codigo < len(linha) else ""
            # AS DUAS COISAS SÃO OBRIGATÓRIAS, como no Streamlit: sem o código
            # do Omie a linha não serve para gerar o JSON, e oferecê-la na
            # lista só levaria a pessoa a montar um rateio que o Omie recusa.
            if nome and codigo:
                saida.append((nome, codigo))
        if not saida:
            return [], (f'a aba "{aba_nome}" tem as colunas certas, mas nenhuma '
                        f'linha com "{aceitos_nome[0]}" e "{aceitos_codigo[0]}" '
                        "preenchidos.")
        return saida, None

    # OS NOMES DE COLUNA SÃO OS DO STREAMLIT, na mesma ordem de preferência —
    # recuperados do código original em 10/09/2026, depois que o dono confirmou
    # o cabeçalho de verdade das duas abas. O primeiro de cada par é o que a
    # planilha realmente usa hoje; o segundo ficou por compatibilidade, que era
    # como o original fazia.
    for tipo, aba_nome, aceitos_nome, aceitos_codigo in (
            ("obra", "C. Diários",
             ["Código Primário", "Obra"], ["Código Omie", "Codigo Omie", "Código"]),
            ("categoria", "Plano Financeiro",
             ["Plano Financeiro", "Categoria"],
             ["Código Omie", "Codigo Omie", "Código"])):
        try:
            linhas, motivo = _ler(aba_nome, aceitos_nome, aceitos_codigo)
            if motivo:
                logger.warning("Análise de SPs: rateio — %s", motivo)
                avisos.append(motivo)
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
        except Exception as e:  # noqa: BLE001
            logger.exception("Análise de SPs: falhou ler a aba '%s'", aba_nome)
            avisos.append(f'falhou gravar o que veio da aba "{aba_nome}": {e}')

    return {"obras": obras, "categorias": categorias, "avisos": avisos}


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
