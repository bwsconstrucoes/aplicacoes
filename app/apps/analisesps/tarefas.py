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
    "fiscal": "Gravar nos cards do Pipefy a análise fiscal confirmada",
}

# As etapas de cada modo, na ordem. Servem para a retomada: o que já foi
# marcado como pronto não roda de novo.
ETAPAS = {
    "carga_inicial": ["carga", "apoios", "fila"],
    "sincronizar": ["fila", "delta", "apoios"],
    "apoios": ["apoios"],
    "fila": ["fila"],
    "comprovantes": ["comprovantes"],
    "fiscal": ["fiscal"],
}


# ---------------------------------------------------------------------------
# O que a tela mostra
# ---------------------------------------------------------------------------
def estado() -> dict:
    """Lido do banco — a única fonte que os dois processos enxergam."""
    try:
        from .db import consultar_um
        linha = consultar_um(
            "SELECT id, tipo, disparo, inicio, etapa, progresso, visto_em, "
            "       (visto_em IS NOT NULL AND "
            "        now() - visto_em < make_interval(secs => ?)) AS viva "
            "  FROM analisesps.execucoes WHERE fim IS NULL "
            " ORDER BY inicio DESC LIMIT 1", (SEGUNDOS_ATE_DAR_POR_MORTA,))
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
        "   AND (visto_em IS NULL OR now() - visto_em >= make_interval(secs => ?))",
        ("Interrompida: o serviço reiniciou durante a atualização. Nada foi "
         "corrompido — é só rodar de novo, que ela retoma de onde parou.",
         SEGUNDOS_ATE_DAR_POR_MORTA))
    quantas = cur.rowcount or 0
    cur.close()
    conn.commit()
    if quantas:
        logger.warning("Análise de SPs: %d execução(ões) órfã(s) encerrada(s).",
                       quantas)
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
        automatica = bool(linha) and str(linha[0] or "") == "tela aberta"
    except Exception:  # noqa: BLE001 — na dúvida, trata como pedido de gente
        logger.exception("Análise de SPs: não consegui saber quem disparou")
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
                recado_apoios[0] = (
                    f"{c.get('lotes', 0)} arquivo(s) processado(s)"
                    + (f", {c['falhas']} com falha" if c.get("falhas") else ""))

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

            elif etapa == "apoios":
                if automatica and _apoios_recentes():
                    logger.info("Análise de SPs: planilhas de apoio ainda "
                                "recentes — pulando nesta automática.")
                else:
                    mudar_etapa("trazendo as planilhas de apoio")
                    a = sincronizacao.sincronizar_apoios(anotar)
                    sincronizacao.sincronizar_agenda(anotar)
                    r = sincronizacao.sincronizar_referencias_rateio(anotar)
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
                        f"categorias: {r.get('categorias', 0)}")
                    # SEM REPETIR: a aba "C. Diários" é lida por dois
                    # caminhos (as contas e as obras). Quando ela falta, as
                    # duas leituras reclamam a mesma coisa, e o recado saía
                    # com a frase duplicada.
                    problemas = list(dict.fromkeys(
                        (a.get("avisos") or []) + (r.get("avisos") or [])))
                    if problemas:
                        recado_apoios[0] += " — " + " ".join(problemas)

            _marcar_etapa_feita(execucao_id, etapa)

        duracao = (agora() - inicio).total_seconds()
        if modo in ("apoios", "comprovantes", "fiscal"):
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

    atual = estado()
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
