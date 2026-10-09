# -*- coding: utf-8 -*-
"""
A sincronização da base — o que antes era o botão "Sincronizar" do Streamlit.

RODA EM PROCESSO SEPARADO, e isso não é preciosismo.

O serviço sobe com `--workers 1 --max-requests 150`: o gunicorn REINICIA o
processo a cada ~150 requisições, uma proteção contra vazamento de memória
posta depois do OOM de julho de 2026. Com um worker só, esse reinício leva
junto qualquer thread de fundo. A própria tela de acompanhamento consulta o
servidor a cada poucos segundos; somando o resto do monorepo, o processo se
recicla a cada poucos minutos.

Uma carga longa rodando numa thread NUNCA teria chance de terminar — foi o que
aconteceu três vezes seguidas durante a conversão do painel, sem que a causa
aparecesse em lugar nenhum. Mexer no `--max-requests` não é opção: ele protege
os outros 15 módulos.

Então quem faz o trabalho é `executar_sync.py`, iniciado destacado do worker. O
andamento vai para o banco, e é de lá que a tela lê — sem inventar canal de
comunicação nenhum entre os dois processos.

UMA DE CADA VEZ. A trava não pode ser de memória (processos diferentes não a
enxergam): é o próprio banco que responde se já existe execução viva.

RETOMADA. Cada etapa concluída é marcada no banco. Uma carga interrompida por
publicação de código recomeça da etapa seguinte, e a carga inicial recomeça do
bloco de linhas onde parou — não do começo.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time

logger = logging.getLogger("analisesps.tarefas")

# De quanto em quanto tempo o andamento vai para o banco. Não é a cada bloco:
# numa carga de doze blocos seriam doze escritas só para dizer "ainda estou
# aqui". A cada 10 segundos dá andamento suficiente sem incomodar o banco.
SEGUNDOS_ENTRE_BATIMENTOS = 10

# Depois de quanto tempo sem batimento uma execução é dada por morta.
# Folgado de propósito: um bloco lento da planilha pode passar de um minuto, e
# declarar morta uma carga que está viva seria pior do que esperar demais.
SEGUNDOS_ATE_DAR_POR_MORTA = 180

MODOS = {
    "sincronizar": "Atualização do dia — traz o que mudou na planilha",
    "carga_inicial": "Primeira carga — traz a planilha inteira (demorado)",
    "apoios": "Só as planilhas de apoio (contas e documentação fiscal)",
    "fila": "Só devolver para a planilha as alterações pendentes",
    "comprovantes": "Dar baixa nos comprovantes arrastados para a tela",
    # O cadastro vem do Pipefy pela planilha "Registro de Colaboradores". Este
    # é o botão que o dono pediu em 27/09/2026 para puxar uma alteração de
    # auxílio ou de gratificação "imediatamente", sem esperar nada.
    "colaboradores": "Atualizar o cadastro de colaboradores (traz da planilha)",
    # 09/10/2026: a tela de Faturamento lê do banco; isto traz a aba "Base
    # Faturamento" (que o emissor grava) e a C. Diários. Ver `faturamento.py`.
    "faturamento": "Trazer as notas fiscais emitidas (Base Faturamento)",
    "faturamento_antigas": "Importar as notas antigas da Notas BWS para a Base Faturamento",
    # ⚠️ O PONTO É O GARGALO DA FOLHA: sem ele não há total por obra, não há
    # diária e não há apropriação. Roda no processo separado porque são várias
    # páginas da API do Mobponto, e um mês pode ter dezenas de milhares de dias.
    "ponto": "Trazer o ponto do Mobponto (o mês escolhido na tela da folha)",
    # ⚠️ O AUTOMÁTICO DO DIA — 29/09/2026. O dono: *"Ele já está configurado
    # pra baixar automático diariamente?"* Não estava: nada trazia o ponto sem
    # alguém apertar. Este modo é o que o agendador (cron-job.org) chama, pela
    # mesma porta `/api/sincronizar` da sincronização, com o segredo do módulo.
    # Ele decide o mês sozinho (`ponto.meses_do_ponto_diario`), por isso não
    # precisa da competência escrita pela tela.
    # O ponto de UMA pessoa — o botão do analítico. Ver `ponto.atualizar_pessoa`.
    "ponto_pessoa": "Fila do ponto por pessoa (trazer de novo e lançar batidas)",
    # ⚠️ ESTE ESCREVE NO MOBPONTO (01/10/2026): lança as batidas que faltam
    # num período, pelo analítico do funcionário. Ver `ponto_edicao.lancar`.
    "ponto_lancar": "Lançar no Mobponto as batidas que faltam (analítico da folha)",
    "ponto_diario": "Trazer o ponto sozinho (mês corrente; até o dia 10, o "
                    "anterior também) — retoma o que parou, pula o que já "
                    "entrou hoje",
    "fiscal": "Gravar nos cards do Pipefy a análise fiscal confirmada",
    "fiscal_ia": "Ler com IA os anexos das SPs escolhidas",
    "notas_receita": "Buscar na Receita as notas emitidas contra a BWS",
    # ⚠️ ESTE MODO ESCREVE NO SISTEMA FISCAL. Ver o comentário em `sefaz.py`:
    # a ciência é uma declaração assinada em nome da empresa, autorizada pelo
    # dono em 17/09/2026. É o único modo deste módulo que não é leitura.
    "notas_ciencia": "Dar ciência na Receita e baixar o XML das notas",
}

# QUAIS MODOS APARECEM EM CONFIGURAÇÕES, e quais são trabalho fiscal.
#
# Correção do dono em 13/09/2026: *"ao buscar na Receita as notas emitidas
# contra a BWS, não tem absolutamente nada a ver eu estar com um botão desse
# fora da tela de trabalho. (…) Ler, é pra estar dentro da tela. Gravar nos
# cards, é pra estar dentro da tela."*
#
# A causa do engano era boba, e é o motivo de esta lista existir: a tela de
# Configurações desenhava a lista INTEIRA de `MODOS` como botões, então quem
# criasse um modo novo ganhava um botão lá sem querer. Agora a divisão é
# explícita, e um modo novo só aparece onde alguém escreveu que ele aparece.
MODOS_DA_BASE = ["sincronizar", "carga_inicial", "apoios", "fila",
                 "comprovantes", "colaboradores"]
# ⚠️ "ponto" NÃO ENTRA em MODOS_DA_BASE de propósito: ele precisa saber QUAL MÊS
# trazer, e um botão em Configurações sem essa escolha traria sempre o mesmo mês.
# Ele é disparado pela tela da folha, que pergunta a competência.

# Os modos que trazem o ponto: quando um deles para, alguém é avisado
# (`avisos_ponto`), porque a folha inteira depende dele.
MODOS_DO_PONTO = ("ponto", "ponto_diario")

# ⚠️ A PISTA DA PESSOA (migração 041, 01/10/2026). O dono: *"tentei atualizar um
# ponto, mas deu: já existe uma atualização em andamento (trazendo o ponto). Uma
# coisa não deveria ter nada a ver com a outra."* Estes dois modos mexem em UMA
# pessoa, levam segundos, e não podem ficar presos atrás da carga do mês. Correm
# numa pista própria: no máximo uma viva aqui, e uma viva na pista geral.
MODOS_DA_PESSOA = ("ponto_pessoa", "ponto_lancar")

# As etapas de cada modo, na ordem. Servem para a retomada: o que já foi
# marcado como pronto não roda de novo.
ETAPAS = {
    "carga_inicial": ["carga", "apoios", "fila"],
    "sincronizar": ["fila", "delta", "apoios"],
    "apoios": ["apoios"],
    "fila": ["fila"],
    "comprovantes": ["comprovantes"],
    "colaboradores": ["colaboradores"],
    "faturamento": ["faturamento"],
    "faturamento_antigas": ["faturamento_antigas", "faturamento"],
    "ponto": ["ponto"],
    "ponto_diario": ["ponto_diario"],
    "ponto_pessoa": ["ponto_pessoa"],
    "ponto_lancar": ["ponto_lancar"],
    "fiscal": ["fiscal"],
    "fiscal_ia": ["fiscal_ia"],
    "notas_receita": ["notas_receita"],
    "notas_ciencia": ["notas_ciencia"],
}


# ---------------------------------------------------------------------------
# O que a tela mostra
# ---------------------------------------------------------------------------
def _fila_do_ponto_pronta() -> bool:
    from . import ponto_fila
    return ponto_fila._pronto()


def pista_do(modo: str) -> str:
    """"pessoa" para os modos de uma pessoa só; "geral" para o resto."""
    return "pessoa" if modo in MODOS_DA_PESSOA else "geral"


def estado(pista: str = "geral") -> dict:
    """Lido do banco — a única fonte que os dois processos enxergam.

    `pista`: "geral" (o padrão — o que Configurações e a barra de andamento
    mostram) ou "pessoa" (ver `MODOS_DA_PESSOA`)."""
    marcas = ",".join(["?"] * len(MODOS_DA_PESSOA))
    filtro = (f"tipo IN ({marcas})" if pista == "pessoa"
              else f"tipo NOT IN ({marcas})")
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT id, tipo, disparo, inicio, etapa, progresso, visto_em, "
            "       (visto_em IS NOT NULL AND "
            "        now() - visto_em < make_interval(secs => ?)) AS viva "
            f"  FROM analisesps.execucoes WHERE fim IS NULL AND {filtro} "
            " ORDER BY inicio DESC LIMIT 1",
            (SEGUNDOS_ATE_DAR_POR_MORTA,) + tuple(MODOS_DA_PESSOA))
    except Exception:      # banco fora do ar, ou migração ainda não aplicada
        return {"rodando": False, "detalhe": None, "interrompida": None}

    if not linha:
        return {"rodando": False, "detalhe": None, "interrompida": None}

    detalhe = {
        "id": linha[0], "tipo": linha[1], "disparo": linha[2],
        "inicio": linha[3], "etapa": linha[4], "progresso": linha[5],
        "visto_em": linha[6],
    }
    if linha[7]:
        return {"rodando": True, "detalhe": detalhe, "interrompida": None}
    # Aberta mas sem batimento: morreu. Dizer isso é melhor do que mostrar
    # "rodando" para sempre — ou, pior, mostrar a falha ANTERIOR como atual.
    return {"rodando": False, "detalhe": None, "interrompida": detalhe}


def ultima_concluida() -> dict | None:
    """A última atualização que terminou, ou None se não houver.

    Protegida como a `estado()` acima, e pelo mesmo motivo: esta função
    alimenta a tela de Configurações, que é o ÚNICO lugar com o botão que
    aplica as migrações. Antes de elas rodarem a tabela `execucoes` não
    existe — e uma exceção aqui derrubava justamente a tela que o dono
    precisa abrir para sair desse estado."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT tipo, disparo, inicio, fim, ok, mensagem, linhas "
            "  FROM analisesps.execucoes WHERE fim IS NOT NULL "
            " ORDER BY fim DESC LIMIT 1")
    except Exception:  # noqa: BLE001 — banco fora do ar, ou migração ainda não aplicada
        logger.exception("Análise de SPs: não consegui ler a última execução")
        return None
    if not linha:
        return None
    return {"tipo": linha[0], "disparo": linha[1], "inicio": linha[2],
            "fim": linha[3], "ok": linha[4], "mensagem": linha[5],
            "linhas": linha[6]}


# ---------------------------------------------------------------------------
# O registro da execução
# ---------------------------------------------------------------------------
def _fechar_orfas(conn) -> int:
    """Encerra execuções abertas de um processo que morreu.

    Sem isto, uma carga interrompida ficaria "em aberto" para sempre e a tela
    nunca mais mostraria o resultado de nenhuma atualização. Só alcança as que
    pararam de bater ponto — uma carga viva não é encerrada por engano."""
    cur = conn.execute(
        "UPDATE analisesps.execucoes SET fim = now(), ok = FALSE, mensagem = ? "
        " WHERE fim IS NULL "
        "   AND (visto_em IS NULL OR now() - visto_em >= make_interval(secs => ?))"
        " RETURNING tipo",
        ("Interrompida: o serviço reiniciou durante a atualização. Nada foi "
         "corrompido — é só rodar de novo, que ela retoma de onde parou.",
         SEGUNDOS_ATE_DAR_POR_MORTA))
    tipos = [linha[0] for linha in cur.fetchall()]
    quantas = len(tipos)
    cur.close()
    conn.commit()
    if quantas:
        logger.warning("Análise de SPs: %d execução(ões) órfã(s) encerrada(s).",
                       quantas)
    if any(t in MODOS_DO_PONTO for t in tipos):
        # O ponto morreu com o serviço (uma publicação, um reinício do Render).
        # Só se descobre aqui, quando a próxima tarefa abre — e é aqui que se
        # avisa. A carga retoma da página em que parou na próxima chamada.
        from . import avisos_ponto
        avisos_ponto.avisar_que_parou(
            "o serviço reiniciou durante a carga (publicação ou reinício do "
            "Render).")
    return quantas


def _abrir_execucao(conn, modo: str, disparo: str) -> int:
    _fechar_orfas(conn)
    cur = conn.execute(
        "INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
        "VALUES (?, ?, ?, now()) RETURNING id", (modo, disparo, "começando"))
    execucao_id = cur.fetchone()[0]
    cur.close()
    conn.commit()
    return execucao_id


def _fechar_execucao(conn, execucao_id: int, ok: bool, mensagem: str,
                     linhas: int | None) -> None:
    conn.execute(
        "UPDATE analisesps.execucoes SET fim = now(), ok = ?, mensagem = ?, "
        "       linhas = ?, etapa = NULL, progresso = NULL WHERE id = ?",
        (ok, str(mensagem)[:2000], linhas, execucao_id))
    conn.commit()


def _carimbar(execucao_id: int, etapa: str, progresso: str) -> None:
    """Grava onde a atualização está, e que ela continua viva.

    Falha aqui nunca derruba a atualização: perder o andamento é chato, perder
    a carga é caro."""
    from .db import conexao
    try:
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.execucoes SET etapa = ?, progresso = ?, "
                "       visto_em = now() WHERE id = ?",
                (str(etapa)[:200], str(progresso or "")[:200], execucao_id))
            conn.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: não consegui gravar o andamento")


def _etapas_feitas(conn, execucao_id: int) -> set[str]:
    cur = conn.execute(
        "SELECT etapas_ok FROM analisesps.execucoes WHERE id = ?", (execucao_id,))
    linha = cur.fetchone()
    cur.close()
    try:
        return set(json.loads(linha[0])) if linha and linha[0] else set()
    except (ValueError, TypeError):
        return set()


def _marcar_etapa_feita(execucao_id: int, etapa: str) -> None:
    from .db import conexao
    with conexao() as conn:
        feitas = _etapas_feitas(conn, execucao_id)
        feitas.add(etapa)
        conn.execute(
            "UPDATE analisesps.execucoes SET etapas_ok = ? WHERE id = ?",
            (json.dumps(sorted(feitas)), execucao_id))
        conn.commit()


def _apoios_recentes() -> bool:
    """As planilhas de apoio foram relidas há menos de uma hora?

    Na dúvida responde NÃO: deixar de trazer um dado é pior do que trazê-lo
    uma vez a mais."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT extract(epoch FROM (now() - valor::timestamptz)) / 60 "
            "  FROM analisesps.meta WHERE chave = 'apoios_em'")
    except Exception:  # noqa: BLE001 — carimbo ilegível ou banco fora
        return False
    if not linha or linha[0] is None:
        return False
    return float(linha[0]) < MINUTOS_ENTRE_APOIOS_AUTOMATICOS


def _marcar_apoios_feitos() -> None:
    """Anota a hora em que as planilhas de apoio foram relidas."""
    try:
        from . import sincronizacao
        from .db import conexao
        from .horario import agora
        with conexao() as conn:
            sincronizacao._meta_gravar(conn, "apoios_em", agora().isoformat())
    except Exception:  # noqa: BLE001 — sem o carimbo, relê da próxima vez
        logger.exception("Análise de SPs: falhou anotar a hora dos apoios")


# ---------------------------------------------------------------------------
# O trabalho
# ---------------------------------------------------------------------------
def executar_trabalho(modo: str, execucao_id: int) -> bool:
    """Faz a sincronização inteira. Chamado pelo processo separado.

    Recebe a execução já aberta: quem clicou no botão a abriu, para a tela ter
    o que mostrar mesmo antes de este processo começar."""
    from . import sincronizacao
    from .db import conexao
    from .horario import agora

    inicio = agora()
    ultimo_batimento = [0.0]
    total_linhas = [0]
    recado_apoios = [""]
    recado_receita = [""]

    # Quem pediu: "tela aberta" é o disparo automático de 5 em 5 minutos;
    # qualquer outra coisa é gente apertando botão. A diferença decide se as
    # planilhas de apoio são relidas agora (ver a constante lá em cima).
    try:
        with conexao() as conn:
            cur = conn.execute(
                "SELECT disparo FROM analisesps.execucoes WHERE id = ?",
                (execucao_id,))
            linha = cur.fetchone()
            cur.close()
        quem_disparou = str((linha or [""])[0] or "")
        automatica = quem_disparou == "tela aberta"
    except Exception:  # noqa: BLE001 — na dúvida, trata como pedido de gente
        logger.exception("Análise de SPs: não consegui saber quem disparou")
        quem_disparou = ""
        automatica = False

    def anotar(etapa: str, progresso: str = "") -> None:
        """Vai para o banco de tempos em tempos, não a cada bloco."""
        if time.time() - ultimo_batimento[0] < SEGUNDOS_ENTRE_BATIMENTOS:
            return
        ultimo_batimento[0] = time.time()
        _carimbar(execucao_id, etapa, progresso)

    def mudar_etapa(etapa: str, progresso: str = "") -> None:
        """Mudança de etapa vai na hora, sem esperar o intervalo."""
        ultimo_batimento[0] = time.time()
        _carimbar(execucao_id, etapa, progresso)

    try:
        with conexao() as conn:
            feitas = _etapas_feitas(conn, execucao_id)
        if feitas:
            logger.info("Análise de SPs: retomando — já feito: %s",
                        ", ".join(sorted(feitas)))

        for etapa in ETAPAS.get(modo, ["delta"]):
            if etapa in feitas:
                continue

            if etapa == "fila":
                mudar_etapa("devolvendo alterações para a planilha")
                sincronizacao.drenar_fila(anotar)

            elif etapa == "carga":
                mudar_etapa("trazendo as SPs da planilha", "começando")
                with conexao() as conn:
                    retomar = sincronizacao._meta_ler(conn, "carga_ate_linha", "")
                de = int(retomar) if str(retomar).strip().isdigit() else 0
                if de:
                    logger.info("Análise de SPs: carga retomando da linha %d.", de)
                total_linhas[0] = sincronizacao.carga_inicial(anotar, retomar_de=de)

            elif etapa == "delta":
                mudar_etapa("conferindo o que mudou na planilha")
                resultado = sincronizacao.sincronizar_delta(anotar)
                total_linhas[0] = resultado.get("alteradas", 0)

            elif etapa == "comprovantes":
                # A BAIXA NÃO PODE RODAR DENTRO DO WORKER, e é o mesmo motivo
                # da carga: ela fala com Omie, Pipefy, Sheets e Dropbox e leva
                # minutos, enquanto o gunicorn recicla o processo a cada ~150
                # requisições. Por isso ela mora aqui, no processo separado.
                mudar_etapa("dando baixa nos comprovantes")
                from . import comprovantes as _comprovantes
                c = _comprovantes.processar_pendentes(anotar)
                total_linhas[0] = c.get("lotes", 0)
                # O QUE FOI DESTRAVADO ENTRA NO RECADO. *"Clico nele e nada
                # acontece"* — parte disso era o botão não alcançar o lote
                # parado; a outra parte era não contar o que fez.
                recado_apoios[0] = (
                    f"{c.get('lotes', 0)} arquivo(s) processado(s)"
                    + (f", {c['falhas']} com falha" if c.get("falhas") else "")
                    + (f", {c['destravados']} destravado(s) da fila"
                       if c.get("destravados") else "")
                    + (f", {c['desempatados']} baixado(s) no desempate de mesmo valor"
                       if c.get("desempatados") else "")
                    + (f", {c['sem_arquivo']} sem o arquivo no servidor"
                       if c.get("sem_arquivo") else ""))

            elif etapa == "ponto_pessoa" and _fila_do_ponto_pronta():
                # A FILA (migração 042): este processo é o trabalhador dela e
                # resolve todos os pedidos esperando, um por vez. O resultado de
                # cada um fica no próprio pedido — ver `ponto_fila`.
                from . import ponto_fila as _fila
                mudar_etapa("resolvendo a fila do ponto")
                r = _fila.processar(anotar)
                total_linhas[0] = r["feitos"]
                recado_apoios[0] = (
                    f"fila do ponto: {r['feitos']} pedido(s) resolvido(s)"
                    + (f", {r['falhas']} com falha (o motivo está em cada um)"
                       if r["falhas"] else ""))

            elif etapa == "ponto_pessoa":
                # QUEM e DE QUE MÊS: vem do banco, escrito pela tela antes de
                # disparar — mesmo motivo do "ponto". (Caminho de antes da fila,
                # para o intervalo entre publicar e apertar o botão da 042.)
                from . import ponto as _ponto
                with conexao() as conn:
                    alvo = sincronizacao._meta_ler(conn, "ponto_pessoa_alvo", "")
                partes = str(alvo or "").split("|")
                if len(partes) < 3 or not partes[0].isdigit() or not partes[1].isdigit():
                    raise RuntimeError("não sei de quem trazer o ponto. Abra o "
                                       "analítico da pessoa e aperte de novo.")
                mudar_etapa("trazendo o ponto de uma pessoa")
                r = _ponto.atualizar_pessoa(int(partes[0]), int(partes[1]),
                                            partes[2], partes[3] if len(partes) > 3 else "",
                                            anotar)
                if not r["achou"]:
                    raise RuntimeError(
                        "não achei esta pessoa no ponto do Mobponto de "
                        f"{int(partes[1]):02d}/{partes[0]} — olhei as páginas "
                        f"{', '.join(str(x) for x in r['olhadas'])}. Se ela bateu "
                        "ponto no mês, traga o mês inteiro de novo.")
                total_linhas[0] = r["dias"]
                recado_apoios[0] = (
                    f"{r['dias']} dia(s) desta pessoa, pela página {r['pagina']} "
                    f"(olhei {len(r['olhadas'])} página(s)); os outros "
                    f"{max(0, r['pessoas_da_pagina'] - 1)} dessa página vieram "
                    "atualizados junto")

            elif etapa == "ponto_lancar":
                # O PEDIDO vem do banco, escrito pela tela antes de disparar:
                # quem, que período, que obra. O plano é REFEITO aqui, com o
                # ponto trazido de novo — ver `ponto_edicao.lancar`.
                import json as _json
                from . import ponto_edicao as _edicao
                with conexao() as conn:
                    bruto = sincronizacao._meta_ler(conn, "ponto_lancar_pedido", "")
                try:
                    pedido = _json.loads(bruto or "{}")
                except ValueError:
                    pedido = {}
                if not pedido.get("cpf"):
                    raise RuntimeError("não sei o que lançar. Abra o analítico "
                                       "da pessoa e peça de novo.")
                mudar_etapa("lançando o ponto no Mobponto")
                feito = _edicao.lancar(pedido, anotar)
                total_linhas[0] = len(feito["enviadas"])
                recado_apoios[0] = _edicao.recado_do_lancamento(feito)
                if feito["falhou"]:
                    raise RuntimeError(recado_apoios[0])

            elif etapa == "ponto_diario":
                # Um mês que falha não impede o outro: os dois são tentados, e
                # a falha de qualquer um vira falha da execução — visível na
                # tela — DEPOIS de o outro ter entrado.
                from . import ponto as _ponto
                recados, falhas = [], []
                for ano, mes in _ponto.meses_do_ponto_diario():
                    # Retomar o que parou, trazer o que ainda não veio hoje,
                    # pular o que já entrou inteiro hoje — é o que deixa o
                    # agendador chamar de hora em hora sem martelar o Mobponto.
                    # Ver `ponto.o_que_fazer_no_automatico`.
                    decisao = _ponto.o_que_fazer_no_automatico(ano, mes)
                    if decisao == "pular":
                        recados.append(f"{mes:02d}/{ano}: já entrou hoje")
                        continue
                    # Duas tentativas: a segunda RETOMA da página em que a
                    # primeira parou (ver `ponto.carregar`), então uma queda
                    # de rede no meio não custa o dia.
                    for tentativa in (1, 2):
                        mudar_etapa(f"trazendo o ponto de {mes:02d}/{ano}"
                                    + (" (2ª tentativa)" if tentativa == 2 else ""))
                        try:
                            p = _ponto.carregar(ano, mes, anotar,
                                                quem=quem_disparou or "agendador")
                            total_linhas[0] += p.get("dias", 0)
                            recados.append(
                                f"{mes:02d}/{ano}: {p.get('dias', 0)} dia(s) de "
                                f"{p.get('pessoas', 0)} pessoa(s)"
                                + (" — " + "; ".join(p["avisos"]) if p.get("avisos") else ""))
                            break
                        except Exception as e:  # noqa: BLE001 — o outro mês segue
                            logger.exception("Análise de SPs: ponto de %02d/%d falhou "
                                             "(tentativa %d)", mes, ano, tentativa)
                            if tentativa == 2:
                                falhas.append(f"{mes:02d}/{ano}: {e}")
                            else:
                                time.sleep(30)
                recado_apoios[0] = " | ".join(recados)
                if falhas:
                    raise RuntimeError(
                        "ponto que NÃO entrou — " + " | ".join(falhas)
                        + (" (entrou: " + "; ".join(recados) + ")" if recados else ""))

            elif etapa == "fiscal":
                # NO PROCESSO SEPARADO pelo mesmo motivo da baixa: são até
                # duzentos cards falando com a API do Pipefy, e dentro do
                # worker isso seguraria uma das quatro threads por minutos.
                mudar_etapa("gravando a análise fiscal nos cards")
                from . import fiscal as _fiscal
                f = _fiscal.escrever_nos_cards(anotar)
                total_linhas[0] = f.get("escritas", 0)
                recado_apoios[0] = (
                    f"{f.get('escritas', 0)} card(s) gravado(s)"
                    + (f", {f['falhas']} recusado(s)" if f.get("falhas") else "")
                    + (f", {f['pendentes']} ainda na fila"
                       if f.get("pendentes") else ""))

            elif etapa == "fiscal_ia":
                # BAIXAR E LER CADA ANEXO leva segundos por SP; trinta SPs são
                # minutos. Dentro do worker isso seguraria uma das quatro
                # threads do gunicorn — e a fila de quem escolheu fica no
                # banco, não em memória, porque o processo pode ser reiniciado.
                mudar_etapa("lendo os anexos com IA")
                from . import fiscal_ia as _fiscal_ia
                from .db import conexao as _conexao
                with _conexao() as _conn:
                    _cur = _conn.execute(
                        "SELECT sp_id FROM analisesps.sp_fiscal_analise "
                        " WHERE situacao = 'NA_FILA_IA' ORDER BY decidida_em")
                    _ids = [str(r[0]) for r in _cur.fetchall()]
                    _cur.close()
                i = _fiscal_ia.analisar(_ids, anotar)
                total_linhas[0] = i.get("lidas", 0)
                recado_apoios[0] = (
                    f"{i.get('lidas', 0)} anexo(s) lido(s) pela IA"
                    + (f", {i['sem_anexo']} sem anexo" if i.get("sem_anexo") else "")
                    + (f", {len(i['falhas'])} com falha" if i.get("falhas") else ""))

            elif etapa == "notas_receita":
                # NO PROCESSO SEPARADO como tudo o que fala com fora: são
                # vários lotes por CNPJ, cada um uma ida à Receita.
                mudar_etapa("buscando notas na Receita")
                from . import sefaz as _sefaz
                b = _sefaz.buscar_tudo(anotar)
                total_linhas[0] = b.get("trazidas", 0)
                # ⚠️ O RECADO DIZ AS DUAS COISAS, e a segunda é a que ele
                # procura: quantos documentos vieram E quantas notas eram novas
                # aqui. "0 nota(s) trazida(s)" sozinho se lê como "não
                # funcionou", mesmo quando a Receita respondeu certo e só não
                # tinha novidade.
                #
                # E A FALHA APARECE POR CNPJ. Antes vinha um erro só, o último,
                # e as outras cinco linhas sumiam — foi assim que o erro da UF
                # ficou escondido atrás do "0 nota(s)".
                novas = sum(int(p.get("novas") or 0) for p in b.get("por_cnpj", []))
                # ⚠️ QUANTOS DOCUMENTOS INTEIROS CHEGARAM — 17/09/2026. É o
                # retorno da ciência dada na rodada anterior, e é a única forma
                # de saber que ela funcionou: a Receita não avisa, ela
                # simplesmente passa a entregar o XML. Sem este número, quem
                # deu ciência não descobre se valeu.
                guardados = sum(int(p.get("xmls_guardados") or 0)
                                for p in b.get("por_cnpj", []))
                falhas = [f"{p['cnpj'][:8]}… {p['tipo']}: {p['erro']}"
                          for p in b.get("por_cnpj", []) if p.get("erro")]
                recado_apoios[0] = (
                    f"{b.get('trazidas', 0)} documento(s) recebido(s) da "
                    f"Receita, {novas} nota(s) nova(s)"
                    + (f", {guardados} documento(s) inteiro(s) guardado(s) no "
                       "Drive" if guardados else "")
                    + (f" — {b['erro']}" if b.get("erro") else "")
                    + (" — FALHOU em: " + "; ".join(falhas[:4]) if falhas else ""))

            elif etapa == "notas_ciencia":
                # NO PROCESSO SEPARADO como tudo que fala com fora: é uma ida à
                # Receita por nota, com assinatura digital em cada uma.
                mudar_etapa("dando ciência nas notas na Receita")
                from . import notas_arquivo as _notas_arquivo
                c = _notas_arquivo.manifestar_pendentes(
                    anotar, quem=quem_disparou or "manual")
                total_linhas[0] = c.get("manifestadas", 0)
                # ⚠️ O RECADO DIZ O QUE VEM DEPOIS. A ciência não traz o
                # documento na hora: ela LIBERA o documento, que chega no
                # próximo lote da distribuição. Sem essa frase, quem clica acha
                # que falhou.
                recado_apoios[0] = (
                    f"{c.get('manifestadas', 0)} nota(s) com ciência dada"
                    + (f", {c['ja_existiam']} já tinham" if c.get("ja_existiam")
                       else "")
                    + (f", {c['falhas']} recusada(s)" if c.get("falhas") else "")
                    + (f" (de {c['olhadas']} olhadas)" if c.get("olhadas") else "")
                    + (f" — {c['erro']}" if c.get("erro") else "")
                    + ". O XML de cada uma chega na PRÓXIMA busca na Receita, "
                      "e é guardado no Drive.")

            elif etapa == "faturamento_antigas":
                mudar_etapa("trazendo as notas antigas para a Base Faturamento")
                from . import faturamento as _faturamento
                c = _faturamento.importar_antigas(anotar)
                recado_apoios[0] = (f"{c['gravadas']} nota(s) antiga(s) levada(s) à "
                                    "Base Faturamento"
                                    + (f" — FALTAM {c['faltam']} (rode de novo)"
                                       if c["faltam"] else "")
                                    + ". ")

            elif etapa == "faturamento":
                # As notas emitidas para a tela de Faturamento (09/10/2026).
                mudar_etapa("trazendo as notas fiscais emitidas")
                from . import faturamento as _faturamento
                c = _faturamento.carregar(anotar)
                total_linhas[0] = c["notas"]
                recado_apoios[0] = recado_apoios[0] + (f"{c['notas']} nota(s) fiscal(is) e "
                                    f"{c['obras']} código(s) de obra"
                                    + ("" if not c["avisos"]
                                       else " — " + " ".join(c["avisos"])))

            elif etapa == "colaboradores":
                # NO PROCESSO SEPARADO como toda leitura de planilha grande:
                # são ~3.500 linhas em faixas de coluna, várias idas ao Sheets.
                # Dentro do worker isso seguraria uma das quatro threads do
                # gunicorn — e o gunicorn recicla o processo a cada 1000
                # requisições, o que mataria a leitura no meio.
                mudar_etapa("trazendo o cadastro de colaboradores")
                from . import colaboradores as _colaboradores
                c = _colaboradores.atualizar(anotar)
                total_linhas[0] = c.get("pessoas", 0)
                # ⚠️ OS AVISOS ENTRAM NO RECADO, e é o ponto todo: se uma
                # coluna de auxílio não foi encontrada, o campo fica em branco
                # e o pagamento sai a menos. Pagamento a menos ninguém nota tão
                # rápido quanto a mais — então isto tem de estar na cara de
                # quem apertou o botão, não no log do serviço.
                recado_apoios[0] = (
                    f"{c.get('pessoas', 0)} pessoa(s) no cadastro"
                    + (f", {c['com_id_fortes']} com o código do Fortes"
                       if c.get("com_id_fortes") else "")
                    + (f", {c['ignoradas']} linha(s) sem CPF válido"
                       if c.get("ignoradas") else "")
                    + (" — ATENÇÃO: " + "; ".join(c["avisos"])
                       if c.get("avisos") else ""))

            elif etapa == "ponto":
                # QUAL MÊS: vem do banco, escrito pela tela antes de disparar.
                # Passar pelo banco em vez de por parâmetro é o que faz a
                # retomada funcionar — o processo separado pode ser reiniciado.
                mudar_etapa("trazendo o ponto do Mobponto")
                from . import ponto as _ponto
                with conexao() as conn:
                    alvo = sincronizacao._meta_ler(conn, "ponto_competencia", "")
                partes = str(alvo or "").split("-")
                if len(partes) != 2 or not all(p.strip().isdigit() for p in partes):
                    raise RuntimeError(
                        "não sei de qual mês trazer o ponto. Escolha a "
                        "competência na tela da folha e dispare de lá.")
                # Duas tentativas, como no automático: a segunda RETOMA da
                # página em que a primeira parou (ver `ponto.carregar`). Cada
                # página já tem uns 8 minutos de paciência lá dentro; isto é
                # para a queda que passa disso.
                for tentativa_do_mes in (1, 2):
                    try:
                        p = _ponto.carregar(int(partes[0]), int(partes[1]), anotar,
                                            quem=quem_disparou or "manual")
                        break
                    except _ponto.ErroDoPonto as e:
                        if tentativa_do_mes == 2 or "credencial" in str(e) \
                                or "certificado" in str(e) or "não devolveu" in str(e):
                            raise
                        logger.warning("Análise de SPs: o ponto parou (%s) — "
                                       "retomo em 2 minutos.", e)
                        mudar_etapa("o Mobponto parou de responder",
                                    "retomo de onde parou em 2 minutos")
                        time.sleep(120)
                total_linhas[0] = p.get("dias", 0)
                # ⚠️ OS CAMPOS QUE VIERAM ENTRAM NO RECADO. É a descoberta que
                # destrava o mapeamento da obra e das marcações: sem eles na
                # cara de quem apertou, a informação ficaria no log do serviço,
                # que ele não tem como ler.
                recado_apoios[0] = (
                    f"{p.get('dias', 0)} dia(s) de {p.get('pessoas', 0)} "
                    f"pessoa(s), {p.get('paginas_lidas', 0)} de "
                    f"{p.get('paginas', 0)} página(s)"
                    + (" · campos de cada dia: " + ", ".join(p.get("campos") or [])
                       if p.get("campos") else "")
                    + (" — ATENÇÃO: " + "; ".join(p["avisos"])
                       if p.get("avisos") else ""))

            elif etapa == "apoios":
                if automatica and _apoios_recentes():
                    logger.info("Análise de SPs: planilhas de apoio ainda "
                                "recentes — pulando nesta automática.")
                else:
                    mudar_etapa("trazendo as planilhas de apoio")
                    a = sincronizacao.sincronizar_apoios(anotar)
                    sincronizacao.sincronizar_agenda(anotar)
                    r = sincronizacao.sincronizar_referencias_rateio(anotar)
                    # AS NOTAS DO FSIST vêm junto com o resto do apoio. Elas
                    # existiam e estavam testadas desde 11/09, mas NINGUÉM AS
                    # CHAMAVA: a tabela ficaria vazia para sempre, e a
                    # conciliação fiscal não teria contra o que casar.
                    # Achado em 12/09 procurando quem importava o relatório.
                    # A BUSCA NA RECEITA VEM ANTES do relatório do FSist,
                    # e a ordem importa: o relatório é a fonte do passado e
                    # pode trazer a mesma nota com status mais velho. Quem
                    # chega depois manda, então o FSist por último garante que
                    # uma nota cancelada NO RELATÓRIO não seja sobrescrita pelo
                    # "autorizada" que a Receita entregou antes do cancelamento.
                    try:
                        from . import sefaz as _sefaz
                        if _sefaz.configurado():
                            b = _sefaz.buscar_tudo(anotar)
                            recado_receita[0] = (
                                f"Receita: {b.get('trazidas', 0)} nota(s)"
                                + (f" — {b['erro']}" if b.get("erro") else ""))
                    except Exception as e:  # noqa: BLE001 — não derruba o apoio
                        logger.exception("Análise de SPs: falhou a busca na "
                                         "Receita")
                        recado_receita[0] = f"Receita: falhou ({e})"
                    try:
                        n = sincronizacao.sincronizar_notas_fiscais(anotar)
                    except Exception as e:  # noqa: BLE001 — não derruba o apoio
                        logger.exception("Análise de SPs: falhou importar as "
                                         "notas do FSist")
                        n = {"novas": 0, "mudaram": 0, "ja_tinha": 0,
                             "avisos": [f"notas do FSist: {e}"]}
                    _marcar_apoios_feitos()
                    # O QUE VEIO, E O QUE NÃO VEIO, VAI PARA A MENSAGEM DA
                    # EXECUÇÃO — que é o que a tela de Configurações mostra.
                    # Antes esta etapa terminava dizendo "0 SPs", e um motivo
                    # que só existia no log do serviço; quem aperta o botão não
                    # tem como ler log. Ver `sincronizar_referencias_rateio`.
                    recado_apoios[0] = (
                        f"documentação fiscal: {a.get('fiscais', 0)} · "
                        f"contas: {a.get('contas', 0)} · "
                        f"obras: {r.get('obras', 0)} · "
                        f"categorias: {r.get('categorias', 0)} · "
                        # OS TRÊS NÚMEROS DAS NOTAS, como o dono pediu: o que
                        # entrou, o que MUDOU (uma nota que volta cancelada é
                        # notícia) e o que já estava lá.
                        f"notas: {n.get('novas', 0)} nova(s), "
                        f"{n.get('mudaram', 0)} mudou/mudaram, "
                        f"{n.get('ja_tinha', 0)} já tinha"
                        + (" · " + recado_receita[0] if recado_receita[0] else ""))
                    # SEM REPETIR: a aba "C. Diários" é lida por dois
                    # caminhos (as contas e as obras). Quando ela falta, as
                    # duas leituras reclamam a mesma coisa, e o recado saía
                    # com a frase duplicada.
                    problemas = list(dict.fromkeys(
                        (a.get("avisos") or []) + (r.get("avisos") or [])
                        + (n.get("avisos") or [])))
                    if problemas:
                        recado_apoios[0] += " — " + " ".join(problemas)

            _marcar_etapa_feita(execucao_id, etapa)

        duracao = (agora() - inicio).total_seconds()
        # ⚠️ "colaboradores" ENTROU NESTA LISTA em 27/09/2026, e por pouco não
        # entrou: sem ele, a tela terminaria dizendo "3.480 SPs em 0,2 min."
        # depois de atualizar o CADASTRO. Não são SPs, são pessoas — e número
        # com o nome errado é pior que número nenhum, porque parece certo.
        if modo in ("apoios", "comprovantes", "fiscal", "fiscal_ia",
                    "notas_receita", "notas_ciencia", "colaboradores", "ponto",
                    "faturamento", "faturamento_antigas",
                    "ponto_pessoa", "ponto_lancar"):
            # Neste modo nenhuma SP é trazida: dizer "0 SPs" fazia a tela
            # parecer que nada aconteceu justamente quando algo aconteceu.
            mensagem = (recado_apoios[0]
                        or "planilhas de apoio ainda recentes — nada a refazer.")
        else:
            mensagem = (f"{total_linhas[0]:,} SPs em {duracao / 60:.1f} min."
                        .replace(",", "."))
            if recado_apoios[0]:
                mensagem += " Apoio — " + recado_apoios[0]
        logger.info("Análise de SPs: %s concluída — %s", modo, mensagem)
        with conexao() as conn:
            _fechar_execucao(conn, execucao_id, True, mensagem, total_linhas[0])
        return True

    except Exception as e:  # noqa: BLE001 — a falha tem de aparecer na tela
        logger.exception("Análise de SPs: falha em %s", modo)
        try:
            with conexao() as conn:
                _fechar_execucao(conn, execucao_id, False, str(e), None)
        except Exception:  # noqa: BLE001
            logger.exception("Análise de SPs: não consegui registrar a falha")
        if modo in MODOS_DO_PONTO:
            # *"que sejamos avisados"* — o ponto que para não pode depender de
            # alguém abrir a tela para ser descoberto.
            from . import avisos_ponto
            avisos_ponto.avisar_que_parou(str(e))
        return False


# ---------------------------------------------------------------------------
# A FILA DE COMPROVANTES ANDA SOZINHA (08/10/2026)
#
# O dono: *"por que essa fila trava? 10 lote(s) parado(s) há mais de 15
# minutos."* O comprovante arrastado tenta começar na hora; se outra tarefa
# está rodando (a atualização da tela aberta, o ponto, o cadastro…), o disparo
# é recusado e o lote fica ESPERANDO — "para a próxima". Mas a próxima era a
# próxima de COMPROVANTES, e nada disparava uma: a atualização do dia não dá
# baixa em comprovante. O lote ficava parado até alguém apertar "Retomar".
#
# Agora toda tarefa da pista geral, ao terminar (bem ou mal), olha se há
# comprovante esperando e, havendo, começa a baixa. Não entra em ciclo: a baixa
# tira cada lote de ESPERANDO (PRONTO ou FALHOU), e só se encadeia de novo se
# chegou lote novo enquanto ela rodava.
# ---------------------------------------------------------------------------
def encadear_comprovantes(modo: str) -> dict | None:
    """Começa a baixa dos comprovantes que ficaram esperando. Nunca levanta."""
    if pista_do(modo) != "geral":
        return None
    try:
        from .db import consultar_um
        from .comprovantes import MINUTOS_PARA_ABANDONADO
        # RODANDO há muito tempo também conta: é o lote cujo processo morreu
        # no meio (publicação, reinício). A baixa o destrava antes de começar.
        linha = consultar_um(
            "SELECT count(*) FROM analisesps.comprovantes_lote "
            " WHERE situacao = 'ESPERANDO' OR (situacao = 'RODANDO' "
            "   AND recebido_em < now() - (? || ' minutes')::interval)",
            (str(int(MINUTOS_PARA_ABANDONADO)),))
        if linha and linha[0]:
            logger.info("Análise de SPs: %d lote(s) de comprovantes esperando — "
                        "começando a baixa depois de '%s'.", linha[0], modo)
            return disparar("comprovantes", disparo="fila de comprovantes")
    except Exception:  # noqa: BLE001 — sem a tabela, ou banco fora: fica para o botão
        logger.exception("Análise de SPs: não consegui encadear os comprovantes")
        return None
    # Sem comprovante esperando: a carga do Faturamento pedida enquanto outra
    # tarefa rodava (09/10/2026 — *"cliquei em atualizar e apareceu: já existe
    # uma atualização em andamento"*). Com comprovante, ela vem na volta
    # seguinte: a baixa termina e passa por aqui de novo.
    if modo != "faturamento" and _pedido_pendente("faturamento", apagar=True):
        logger.info("Análise de SPs: carga do faturamento pedida durante '%s' — "
                    "começando agora.", modo)
        return disparar("faturamento", disparo="pedida durante outra tarefa")
    return None


CHAVE_PEDIDO = "pedido_pendente_"


def pedir_depois(modo: str) -> None:
    """Guarda que `modo` foi pedido e recusado (outra tarefa rodando): ele
    começa sozinho quando ela terminar (`encadear_comprovantes`)."""
    try:
        from .db import conexao
        from .sincronizacao import _meta_gravar
        with conexao() as conn:
            _meta_gravar(conn, CHAVE_PEDIDO + modo, "1")
    except Exception:  # noqa: BLE001 — sem isso, fica para o próximo clique
        logger.exception("Análise de SPs: não consegui guardar o pedido de %s", modo)


def _pedido_pendente(modo: str, apagar: bool = False) -> bool:
    try:
        from .db import conexao
        with conexao() as conn:
            cur = conn.execute("SELECT valor FROM analisesps.meta WHERE chave = ?",
                               (CHAVE_PEDIDO + modo,))
            linha = cur.fetchone()
            cur.close()
            if linha and linha[0] == "1" and apagar:
                conn.execute("DELETE FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_PEDIDO + modo,))
                conn.commit()
            return bool(linha and linha[0] == "1")
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: não consegui ler o pedido de %s", modo)
        return False


# ---------------------------------------------------------------------------
# O disparo
# ---------------------------------------------------------------------------
def _iniciar_processo(modo: str, execucao_id: int) -> None:
    """Inicia o processo separado, DESTACADO do worker do gunicorn.

    "Destacado" é o ponto: sem isto o processo morre junto quando o gunicorn
    recicla o worker — que é exatamente o que matava a carga."""
    comando = [sys.executable, "-m", "app.apps.analisesps.executar_sync",
               modo, str(execucao_id)]
    raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

    extras: dict = {}
    if os.name == "posix":
        # Sessão própria: o sinal que o gunicorn manda ao worker não o alcança.
        extras["start_new_session"] = True
    else:
        extras["creationflags"] = (getattr(subprocess, "DETACHED_PROCESS", 0)
                                   | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))

    subprocess.Popen(comando, cwd=raiz, stdin=subprocess.DEVNULL,
                     stdout=None, stderr=None, close_fds=True, **extras)
    logger.info("Análise de SPs: '%s' iniciada em processo separado (execução %d).",
                modo, execucao_id)


# De quanto em quanto tempo a TELA ABERTA pode pedir uma sincronização.
#
# O Streamlit buscava atualizações a cada 90 segundos. Aqui a tela também
# pergunta a cada 90 s, mas só DISPARA a sincronização se a última tiver mais
# de cinco minutos: com quatro pessoas com a tela aberta o dia inteiro, um
# disparo a cada 90 s seriam quarenta sincronizações por hora, todas lendo a
# planilha. Cinco minutos é fresco o bastante para contas a pagar e é uma
# leitura da planilha a cada cinco minutos, no pior caso.
MINUTOS_ENTRE_SYNC_DA_TELA = 5

# De quanto em quanto tempo as planilhas de APOIO são relidas, quando quem
# pediu a sincronização foi a tela aberta e não uma pessoa.
#
# Elas não são o dado principal: são a documentação fiscal por SP, as contas
# por centro de custo, a agenda e as listas do rateio. Mudam raramente — mas
# vinham sendo relidas INTEIRAS a cada cinco minutos, junto com as SPs.
#
# O estrago apareceu na tela do banco em 10/09/2026: a gravação da
# documentação fiscal era, disparada, a consulta mais chamada de todo o banco
# — **14,3 milhões de vezes**. E cada passagem ainda baixa a planilha inteira
# do Google, na instância de 2 GB que já morreu de memória uma vez.
#
# Uma hora é folgado para dado de apoio, e quem precisar na hora tem dois
# caminhos que continuam imediatos: o botão de atualizar e o modo "Só as
# planilhas de apoio". A trava vale SÓ para o disparo automático.
MINUTOS_ENTRE_APOIOS_AUTOMATICOS = 60


def _minutos_desde_a_ultima_sincronizacao():
    """Quantos minutos desde a última sincronização, ou None se nunca houve
    uma (ou se o banco não respondeu — e aí não se dispara nada)."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT extract(epoch FROM (now() - valor::timestamptz)) / 60 "
            "  FROM analisesps.meta WHERE chave = 'ultima_sincronizacao'")
    except Exception:  # noqa: BLE001 — banco fora, ou carimbo ilegível
        logger.exception("Análise de SPs: não consegui ler a última "
                         "sincronização")
        return None
    if not linha or linha[0] is None:
        return None
    return float(linha[0])


def manter_fresco() -> dict:
    """Chamado pela tela aberta, de 90 em 90 segundos.

    Dispara a sincronização quando ela está velha, e não faz nada quando está
    recente ou quando já há uma rodando. É o que substitui o agendador
    externo: se o cron nunca for configurado — e não há sinal de que tenha
    sido —, a base só se atualizaria quando alguém apertasse o botão em
    Configurações. Com isto, quem estiver com a tela aberta mantém a base
    viva para todo mundo.

    Nunca levanta: é chamado de fundo, e uma falha aqui não pode aparecer na
    cara de quem só estava olhando a lista."""
    try:
        if estado()["rodando"]:
            return {"disparou": False, "motivo": "já está rodando"}
        minutos = _minutos_desde_a_ultima_sincronizacao()
        if minutos is not None and minutos < MINUTOS_ENTRE_SYNC_DA_TELA:
            return {"disparou": False, "motivo": "recente"}
        resultado = disparar("sincronizar", disparo="tela aberta")
        return {"disparou": bool(resultado.get("ok")),
                "motivo": resultado.get("erro") or "disparada"}
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou manter a base fresca")
        return {"disparou": False, "motivo": "falhou"}


def disparar(modo: str, disparo: str = "manual") -> dict:
    """Começa a sincronização. Devolve o que dizer a quem pediu."""
    if modo not in MODOS:
        return {"ok": False, "erro": f"Modo desconhecido: {modo}"}

    from .db import conexao

    atual = estado(pista_do(modo))
    if atual["rodando"]:
        etapa = (atual["detalhe"] or {}).get("etapa", "começando")
        return {"ok": False,
                "erro": f"Já existe uma atualização em andamento ({etapa}). "
                        "Espere ela terminar."}

    try:
        with conexao() as conn:
            execucao_id = _abrir_execucao(conn, modo, disparo)
    except Exception as e:  # noqa: BLE001
        # A migração 004 põe um índice que só deixa existir UMA execução viva.
        # Chegar aqui quer dizer que outra requisição abriu a dela entre a
        # pergunta lá em cima e esta linha — o que passou a ser comum quando a
        # tela voltou a buscar atualizações de 90 em 90 segundos, com quatro
        # pessoas perguntando quase ao mesmo tempo. Recusar é o certo: a
        # atualização que já começou faz o mesmo trabalho.
        if ("ux_execucao_viva" in str(e) or "duplicate key" in str(e).lower()) \
                and modo in MODOS_DA_PESSOA:
            # Sem a migração 041, o índice antigo ainda deixa UMA viva no
            # sistema inteiro — e a pista da pessoa esbarra na carga do mês.
            return {"ok": False,
                    "erro": "Há outra tarefa rodando e o banco ainda não foi "
                            'atualizado para rodar as duas juntas: aperte '
                            '"Aplicar atualizações do banco" em Configurações. '
                            "Até lá, espere a outra terminar."}
        if "ux_execucao_viva" in str(e) or "duplicate key" in str(e).lower():
            logger.info("Análise de SPs: pedido de atualização recusado — "
                        "outra começou no mesmo instante.")
            return {"ok": False,
                    "erro": "Outra atualização acabou de começar. "
                            "Espere ela terminar."}
        logger.exception("Análise de SPs: não consegui abrir a execução")
        return {"ok": False, "erro": f"Não consegui começar: {e}"}

    try:
        _iniciar_processo(modo, execucao_id)
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: não consegui iniciar o processo")
        with conexao() as conn:
            _fechar_execucao(conn, execucao_id, False,
                             f"Não consegui iniciar a atualização: {e}", None)
        return {"ok": False, "erro": f"Não consegui iniciar a atualização: {e}"}

    return {"ok": True, "modo": modo, "descricao": MODOS[modo],
            "execucao": execucao_id}


# Os modos que a tela de Documentação Fiscal dispara. O resultado do último de
# cada um é mostrado lá — ver `ultimas_por_tipo`.
MODOS_FISCAIS = ["notas_receita", "notas_ciencia", "apoios", "fiscal_ia",
                 "fiscal", "fila"]


def ultimo_fim() -> str:
    """Quando a última tarefa CONCLUÍDA terminou, como texto comparável.

    Serve para a tela perceber que uma rodada curta aconteceu: ela guarda este
    valor ao disparar e recarrega quando ele muda. Sem isso, só dava para
    perceber o fim de uma tarefa que a tela conseguisse flagrar RODANDO — e
    tarefa que dura menos que o intervalo da pergunta nunca é flagrada."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT max(fim) FROM analisesps.execucoes WHERE fim IS NOT NULL")
        return str(linha[0]) if linha and linha[0] else ""
    except Exception:  # noqa: BLE001 — acessório: a tela funciona sem isto
        logger.exception("Análise de SPs: não consegui ler o fim da última "
                         "execução")
        return ""


def ultimas_por_tipo(tipos: list) -> dict:
    """A última execução CONCLUÍDA de cada tipo pedido.

    Existe por causa de uma reclamação do dono em 13/09/2026: *"eu clico gravar
    no Pipefy, aí diz que está rodando no servidor, mas como é que a gente sabe
    se rodou, se não rodou, se terminou? (…) Não aparece nada na tela, a tela
    continua do mesmo jeito. Não deveria ter alguma coisa dizendo que gravou,
    uma confirmação?"*

    Ele está certo, e o dado sempre existiu: cada execução grava quando
    terminou, se deu certo e um recado em português ("12 card(s) gravado(s), 2
    recusado(s)"). Isso aparecia SÓ na tela de Configurações, que não é onde o
    trabalho acontece. Aqui a tela de onde o botão foi apertado passa a mostrar
    o que ele fez.

    UMA CONSULTA SÓ, com `DISTINCT ON`: uma por tipo seriam cinco varreduras da
    mesma tabela num banco que tem um décimo de um núcleo."""
    tipos = [t for t in (tipos or []) if t]
    if not tipos:
        return {}
    try:
        from .db import consultar
        marcas = ",".join(["?"] * len(tipos))
        linhas = consultar(
            "SELECT DISTINCT ON (tipo) tipo, fim, ok, mensagem, linhas, disparo "
            "  FROM analisesps.execucoes "
            f" WHERE fim IS NOT NULL AND tipo IN ({marcas}) "
            " ORDER BY tipo, fim DESC", tuple(tipos))
    except Exception:  # noqa: BLE001 — banco fora do ar, ou migração por aplicar
        logger.exception("Análise de SPs: não consegui ler as últimas execuções")
        return {}
    nomes = ["tipo", "fim", "ok", "mensagem", "linhas", "disparo"]
    return {l[0]: dict(zip(nomes, l)) for l in linhas}


def ultima_do_tipo(tipo: str) -> dict | None:
    """A última execução DE UM TIPO, terminada ou em andamento.

    ⚠️ EXISTE POR UMA RECLAMAÇÃO REPETIDA TRÊS VEZES. O dono, sobre o ponto:
    *"clico em trazer o ponto, sistema diz que vai trazer e NÃO TRAZ nada. Não sei
    se ele conseguiu conectar, se tá indo, se não tá, ninguém sabe de nada."*

    O registro da tentativa SEMPRE existiu — com `ok`, com a mensagem e com o erro
    da API dentro dela. O que faltava era a tela mostrar. Falha que só aparece no
    log do serviço é falha que o dono não tem como ler, e aí o botão vira caixa
    preta: aperta, nada acontece, e não há como saber por quê.

    Devolve também `em_andamento`, para a tela distinguir "está trabalhando" de
    "terminou e deu isso"."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT tipo, disparo, inicio, fim, ok, mensagem, linhas, visto_em, "
            "       etapa, progresso "
            "  FROM analisesps.execucoes WHERE tipo = ? "
            " ORDER BY inicio DESC LIMIT 1", (str(tipo),))
    except Exception:  # noqa: BLE001 — banco atrasado não pode derrubar a tela
        logger.exception("Análise de SPs: não consegui ler a última execução "
                         "de %s", tipo)
        return None
    if not linha:
        return None
    return {"tipo": linha[0], "disparo": linha[1], "inicio": linha[2],
            "fim": linha[3], "ok": linha[4], "mensagem": linha[5],
            "linhas": linha[6], "visto_em": linha[7],
            "etapa": linha[8] if len(linha) > 8 else None,
            "progresso": linha[9] if len(linha) > 9 else None,
            "em_andamento": linha[3] is None}
