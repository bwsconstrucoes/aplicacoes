# ============================================================================
# ERP — core/comum/tarefas.py
# A fila de trabalho pesado: o que não cabe no tempo de um clique.
#
# O PROBLEMA QUE ISTO RESOLVE
#
# Importar 100 cards do Pipefy, ler um lote de documentos com IA, emitir nota e
# esperar a prefeitura responder. Enquanto qualquer uma dessas coisas roda, ela
# segura UMA DAS QUATRO linhas de atendimento do serviço — que é o mesmo
# serviço dos outros treze módulos. O resultado é o sistema inteiro lento para
# todo mundo, e nem quem pediu entende por quê.
#
# Aqui o clique só ENFILEIRA e volta na hora. O trabalho acontece numa linha
# separada, e a tela acompanha.
#
# AS QUATRO DECISÕES QUE SUSTENTAM ESTE ARQUIVO
#
#   1. A FILA VIVE NO BANCO. O serviço se reinicia sozinho de tempos em tempos
#      (a faxina de memória do gunicorn). Fila na memória perderia o trabalho
#      no meio, calada. Aqui cada trabalho é uma linha que sobrevive ao
#      reinício e é retomada.
#
#   2. UMA LINHA DE TRABALHO SÓ, e de propósito. Duas linhas fariam duas
#      importações grandes disputarem a mesma máquina de 2 GB — que é o
#      problema que viemos resolver, com outro nome. A fila é para ORDENAR o
#      trabalho pesado, não para multiplicá-lo.
#
#   3. QUEM MORRE NO MEIO VOLTA PARA A FILA. `batida_em` é o sinal de vida:
#      quem para de bater foi interrompido. Sem isso um trabalho ficaria
#      "executando" para sempre e ninguém saberia que precisa refazer.
#
#   4. TENTATIVA TEM TETO. Falhou três vezes, para e explica. Repetir para
#      sempre um trabalho que sempre falha gasta máquina e esconde o defeito.
#
# COMO SE USA, de fora:
#
#   registrar("importar_pipefy", _executor)          — uma vez, no import
#   tarefas.enfileirar(s, "importar_pipefy", {...},  — no clique
#                      rotulo="Importar 37 cards", usuario=u)
#
# O executor recebe `(sessao, parametros, andamento)` e devolve um dicionário
# que vira o resultado. `andamento(passo, total, mensagem)` é o que a tela
# mostra — chamar à vontade, a gravação é represada.
# ============================================================================
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import Tarefa

logger = logging.getLogger(__name__)

SITUACOES = ("PENDENTE", "EXECUTANDO", "CONCLUIDA", "FALHADA", "CANCELADA")

MAX_TENTATIVAS = 3
# Sem sinal de vida por este tempo = o serviço reiniciou no meio do trabalho.
# Folgado de propósito: uma leitura de IA demorada é lenta, não morta.
SEM_SINAL_MINUTOS = 10
# De quanto em quanto tempo o andamento desce ao banco. Uma gravação por item
# faria a fila custar mais que o trabalho dela.
INTERVALO_BATIDA = 3.0
# Quanto o trabalhador dorme quando não há nada na fila.
DESCANSO = 5.0
# Teto do histórico que a tela mostra.
MAX_NA_TELA = 100

# tipo → função que sabe executar. Preenchido no import dos módulos que têm
# trabalho pesado; um tipo sem executor registrado falha na hora, com recado.
_EXECUTORES: dict[str, Callable[..., Any]] = {}

# TIPOS QUE NÃO SE REPETEM SOZINHOS.
#
# Repetir é bom para trabalho que só lê ou recalcula. É PERIGOSO para trabalho
# que produz efeito irreversível do lado de fora: emitir nota duas vezes na
# prefeitura cria duas notas de verdade, e desfazer isso é cancelamento com
# justificativa, não um clique. Aqui a primeira falha já é a última, com o
# motivo escrito — refazer passa a ser decisão de gente.
_SEM_REPETICAO: set[str] = set()


def registrar_sem_repeticao(tipo: str) -> None:
    """Marca um tipo cujo efeito é irreversível: falhou, não tenta de novo."""
    _SEM_REPETICAO.add(tipo)

_trabalhador: Optional[threading.Thread] = None
_trava = threading.Lock()
_acordar = threading.Event()


def registrar(tipo: str, funcao: Callable[..., Any]) -> None:
    """Diz quem sabe executar este tipo de trabalho."""
    _EXECUTORES[tipo] = funcao


def _agora() -> datetime:
    return datetime.now(timezone.utc)


def _ligado() -> bool:
    """A linha de trabalho em segundo plano está ligada neste processo?

    Desligada, a fila continua funcionando — só não anda sozinha. É assim que
    a suíte roda: ela enfileira e manda executar na hora, sem thread nenhuma
    atravessando os testes.
    """
    return os.getenv("ERP_TAREFAS", "1").strip() not in ("0", "nao", "não", "off")


# ---------------------------------------------------------------------------
# Enfileirar
# ---------------------------------------------------------------------------
def enfileirar(s: Session, tipo: str, parametros: Optional[dict[str, Any]] = None, *,
               rotulo: str, usuario: Optional[Usuario] = None) -> Tarefa:
    """Põe o trabalho na fila e volta na hora.

    Recusa tipo sem executor: enfileirar algo que ninguém sabe fazer criaria
    uma linha que nunca sai de PENDENTE, e a pessoa ficaria olhando para ela.
    """
    if tipo not in _EXECUTORES:
        raise ErroValidacao(f"Trabalho desconhecido: {tipo!r}.")
    if not (rotulo or "").strip():
        raise ErroValidacao("Todo trabalho precisa de um nome que a pessoa leia.")
    t = Tarefa(tipo=tipo, rotulo=rotulo.strip()[:200], parametros=parametros or {},
               situacao="PENDENTE", usuario_id=usuario.id if usuario else None)
    s.add(t)
    s.flush()
    logger.info("ERP/tarefas: enfileirado #%s %s — %s", t.id, tipo, t.rotulo)
    acordar()
    return t


def enfileirar_unico(s: Session, tipo: str, parametros: Optional[dict[str, Any]] = None, *,
                     rotulo: str, usuario: Optional[Usuario] = None,
                     frescor_minutos: int = 10) -> Optional[Tarefa]:
    """Enfileira SÓ SE fizer diferença. Devolve None quando não fez.

    Serve para o trabalho que uma tela dispara ao abrir — recalcular a agenda,
    por exemplo. Sem isto, dez pessoas abrindo a mesma tela criariam dez
    trabalhos idênticos na fila, e o nono recalcularia o que o oitavo acabou de
    calcular. Pior: a fila encheria de repetição e o trabalho de verdade
    esperaria atrás dela.

    Dois motivos para não enfileirar: já existe um igual esperando ou rodando,
    ou um igual terminou há pouco — e "há pouco" é o `frescor_minutos`, que
    quem chama escolhe, porque só ele sabe de quanto em quanto tempo o
    resultado dele muda.
    """
    andando = s.scalars(select(Tarefa).where(
        Tarefa.tipo == tipo,
        Tarefa.situacao.in_(("PENDENTE", "EXECUTANDO")))).first()
    if andando is not None:
        acordar()
        return None
    if frescor_minutos > 0:
        limite = _agora() - timedelta(minutes=frescor_minutos)
        recente = s.scalars(select(Tarefa).where(
            Tarefa.tipo == tipo, Tarefa.situacao == "CONCLUIDA",
            Tarefa.concluido_em.is_not(None),
            Tarefa.concluido_em >= limite)).first()
        if recente is not None:
            return None
    return enfileirar(s, tipo, parametros, rotulo=rotulo, usuario=usuario)


def acordar() -> None:
    """Garante que existe alguém trabalhando na fila.

    Chamada no enfileiramento e na primeira requisição depois de o serviço
    subir — esta segunda importa: sem ela, um trabalho que ficou pendente de
    antes do reinício esperaria alguém enfileirar outro para ser notado.
    """
    global _trabalhador
    if not _ligado():
        return
    with _trava:
        if _trabalhador is not None and _trabalhador.is_alive():
            _acordar.set()
            return
        _trabalhador = threading.Thread(target=_laco, name="erp-tarefas", daemon=True)
        _trabalhador.start()
        logger.info("ERP/tarefas: linha de trabalho em segundo plano iniciada")


# ---------------------------------------------------------------------------
# O trabalhador
# ---------------------------------------------------------------------------
def _laco() -> None:
    """Roda para sempre: recupera órfãs, pega a próxima, executa, repete.

    Nada aqui pode derrubar o processo: é uma linha de fundo dentro do serviço
    que atende as telas dos catorze módulos. Todo erro é registrado e a linha
    continua.
    """
    from app.apps.erp.db.database import get_session

    while True:
        try:
            with get_session() as s:
                recuperar_orfas(s)
                s.commit()
            feito = False
            while True:
                tarefa_id = _pegar_proxima()
                if tarefa_id is None:
                    break
                feito = True
                executar(tarefa_id)
            if not feito:
                _acordar.wait(timeout=DESCANSO)
                _acordar.clear()
        except Exception:
            # Tabela ainda não criada (migração 055 pendente), banco fora do ar,
            # o que for: registra e descansa mais, em vez de girar em falso.
            logger.warning("ERP/tarefas: a fila não pôde ser lida agora", exc_info=True)
            _acordar.wait(timeout=30)
            _acordar.clear()


def _pegar_proxima() -> Optional[int]:
    """Toma a próxima da fila, marcando-a como EXECUTANDO num passo só.

    `FOR UPDATE SKIP LOCKED` para que, se um dia houver mais de um processo,
    dois trabalhadores não peguem o mesmo trabalho. Hoje há um só — mas a
    garantia custa nada e evita uma surpresa silenciosa no futuro.
    """
    from app.apps.erp.db.database import get_session

    with get_session() as s:
        linha = s.execute(text("""
            UPDATE tarefas SET situacao = 'EXECUTANDO',
                               tentativas = tentativas + 1,
                               iniciado_em = now(), batida_em = now(),
                               erro = NULL
            WHERE id = (SELECT id FROM tarefas WHERE situacao = 'PENDENTE'
                        ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING id
        """)).first()
        s.commit()
        return int(linha[0]) if linha else None


def executar(tarefa_id: int, sessao: Optional[Session] = None) -> None:
    """Executa UM trabalho já tomado da fila.

    Sem `sessao`, abre a sua própria — que é o caso da linha de fundo: o
    trabalho acontece depois, pode durar minutos, e segurar a transação de uma
    tela por esse tempo prenderia uma conexão do banco à toa.

    Com `sessao`, usa a que veio. É o caminho de quem já está numa transação e
    quer o trabalho feito agora — a suíte, principalmente.
    """
    from app.apps.erp.db.database import get_session
    import contextlib

    @contextlib.contextmanager
    def _sessao():
        if sessao is not None:
            yield sessao
        else:
            with get_session() as nova:
                yield nova

    with _sessao() as s:
        t = s.get(Tarefa, tarefa_id)
        if t is None or t.situacao != "EXECUTANDO":
            return
        funcao = _EXECUTORES.get(t.tipo)
        if funcao is None:
            _encerrar(s, t, "FALHADA",
                      erro=f"Nenhum executor registrado para {t.tipo!r}. "
                           f"O trabalho ficou registrado e não se perdeu.")
            s.commit()
            return

        ultima = [0.0]

        def andamento(passo: int, total: Optional[int] = None, mensagem: str = "") -> None:
            """O que a tela mostra. A gravação é represada de propósito."""
            t.passo = max(0, int(passo or 0))
            if total is not None:
                t.total = max(0, int(total))
            if mensagem:
                t.mensagem = str(mensagem)[:300]
            agora = time.monotonic()
            if agora - ultima[0] >= INTERVALO_BATIDA:
                ultima[0] = agora
                t.batida_em = _agora()
                try:
                    s.commit()
                except Exception:
                    logger.warning("ERP/tarefas: não deu para gravar o andamento de #%s",
                                   t.id, exc_info=True)
                    s.rollback()

        try:
            resultado = funcao(s, dict(t.parametros or {}), andamento)
            t = s.get(Tarefa, tarefa_id)          # a sessão pode ter sido revertida
            _encerrar(s, t, "CONCLUIDA",
                      resultado=resultado if isinstance(resultado, dict) else {"ok": True})
            s.commit()
            logger.info("ERP/tarefas: #%s concluído", tarefa_id)
        except Exception as e:
            s.rollback()
            t = s.get(Tarefa, tarefa_id)
            if t is None:
                return
            # Falhou: volta para a fila enquanto houver tentativa; senão para e
            # explica. Repetir para sempre esconderia o defeito — e há trabalho
            # que não pode repetir NENHUMA vez, porque já mexeu no mundo lá fora.
            if t.tipo in _SEM_REPETICAO:
                _encerrar(s, t, "FALHADA", erro=str(e)[:2000])
                logger.error("ERP/tarefas: #%s falhou e NÃO se repete sozinho",
                             tarefa_id, exc_info=True)
            elif t.tentativas < MAX_TENTATIVAS:
                t.situacao = "PENDENTE"
                t.erro = str(e)[:2000]
                t.batida_em = _agora()
                logger.warning("ERP/tarefas: #%s falhou na tentativa %s — vai tentar de novo",
                               tarefa_id, t.tentativas, exc_info=True)
            else:
                _encerrar(s, t, "FALHADA", erro=str(e)[:2000])
                logger.error("ERP/tarefas: #%s desistiu depois de %s tentativas",
                             tarefa_id, t.tentativas, exc_info=True)
            s.commit()


def _encerrar(s: Session, t: Tarefa, situacao: str, *,
              resultado: Optional[dict[str, Any]] = None, erro: str = "") -> None:
    t.situacao = situacao
    t.concluido_em = _agora()
    t.batida_em = _agora()
    if resultado is not None:
        t.resultado = resultado
    if erro:
        t.erro = erro


def recuperar_orfas(s: Session) -> int:
    """Quem estava executando quando o serviço reiniciou volta para a fila.

    É o que separa "está trabalhando" de "morreu no meio". Sem isto, um
    trabalho interrompido ficaria EXECUTANDO para sempre e ninguém saberia que
    precisa refazer.
    """
    limite = _agora() - timedelta(minutes=SEM_SINAL_MINUTOS)
    orfas = s.scalars(select(Tarefa).where(
        Tarefa.situacao == "EXECUTANDO",
        Tarefa.batida_em.is_(None) | (Tarefa.batida_em < limite))).all()
    for t in orfas:
        if t.tipo in _SEM_REPETICAO:
            # O caso perigoso: pode ter chegado ao destino antes de morrer.
            # Refazer sozinho criaria o efeito duas vezes lá fora.
            _encerrar(s, t, "FALHADA",
                      erro="O serviço reiniciou no meio deste trabalho, e ele "
                           "não pode ser refeito sozinho — pode já ter chegado "
                           "ao destino. Confira antes de pedir de novo.")
        elif t.tentativas < MAX_TENTATIVAS:
            t.situacao = "PENDENTE"
            t.mensagem = "O serviço reiniciou no meio — retomando."
        else:
            _encerrar(s, t, "FALHADA",
                      erro="O serviço reiniciou no meio do trabalho mais vezes "
                           "do que o limite de tentativas. Peça de novo.")
    if orfas:
        logger.info("ERP/tarefas: %d trabalho(s) interrompido(s) recuperado(s)", len(orfas))
    return len(orfas)


# ---------------------------------------------------------------------------
# Executar na hora — o caminho da suíte e do "não vale a pena enfileirar"
# ---------------------------------------------------------------------------
def executar_agora(s: Session, tarefa_id: int) -> Tarefa:
    """Executa o trabalho sem passar pela linha de fundo.

    A suíte usa este caminho: prova o trabalho de verdade, sem thread nenhuma
    atravessando os testes.
    """
    t = s.get(Tarefa, tarefa_id)
    if t is None:
        raise ErroValidacao("Trabalho não encontrado.")
    if t.situacao not in ("PENDENTE", "EXECUTANDO"):
        raise ErroValidacao(f"Este trabalho já está {t.situacao.lower()}.")
    t.situacao = "EXECUTANDO"
    t.tentativas += 1
    t.iniciado_em = t.iniciado_em or _agora()
    t.batida_em = _agora()
    s.commit()
    executar(t.id, sessao=s)
    s.expire_all()
    return s.get(Tarefa, tarefa_id)


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
def _ler(t: Tarefa) -> dict[str, Any]:
    def _quando(v):
        return v.astimezone().strftime("%d/%m/%Y %H:%M") if v else None

    segundos = None
    if t.iniciado_em:
        fim = t.concluido_em or t.batida_em or _agora()
        segundos = max(0, int((fim - t.iniciado_em).total_seconds()))
    return {
        "id": t.id, "tipo": t.tipo, "rotulo": t.rotulo, "situacao": t.situacao,
        "passo": t.passo, "total": t.total, "mensagem": t.mensagem,
        "tentativas": t.tentativas, "erro": t.erro, "resultado": t.resultado,
        "criado_em": _quando(t.criado_em), "iniciado_em": _quando(t.iniciado_em),
        "concluido_em": _quando(t.concluido_em), "segundos": segundos,
        "terminou": t.situacao in ("CONCLUIDA", "FALHADA", "CANCELADA"),
    }


def ler(s: Session, tarefa_id: int) -> dict[str, Any]:
    t = s.get(Tarefa, tarefa_id)
    if t is None:
        raise ErroValidacao("Trabalho não encontrado.")
    return _ler(t)


def listar(s: Session, *, usuario: Optional[Usuario] = None,
           situacao: str = "", limite: int = 40) -> dict[str, Any]:
    """Os trabalhos recentes. Sem `usuario`, os de todo mundo.

    Não tem escopo por obra: o que aparece aqui é "importação de 37 cards" e
    "emissão da nota 412", não o conteúdo dos registros. Quem tem a ação de ver
    a fila vê a fila inteira, porque metade dela não explicaria nada.
    """
    stmt = select(Tarefa).order_by(Tarefa.id.desc())
    if usuario is not None:
        stmt = stmt.where(Tarefa.usuario_id == usuario.id)
    if situacao:
        stmt = stmt.where(Tarefa.situacao == situacao)
    linhas = [_ler(t) for t in s.scalars(stmt.limit(min(limite, MAX_NA_TELA))).all()]
    contagem = {c: int(n) for c, n in s.execute(text(
        "SELECT situacao, count(*) FROM tarefas GROUP BY situacao")).all()}
    return {
        "tarefas": linhas,
        "na_fila": contagem.get("PENDENTE", 0),
        "executando": contagem.get("EXECUTANDO", 0),
        "falhadas": contagem.get("FALHADA", 0),
        "ligado": _ligado(),
    }


def cancelar(s: Session, tarefa_id: int, *, usuario: Optional[Usuario] = None) -> Tarefa:
    """Tira da fila o que ainda não começou.

    O que já está EXECUTANDO não é interrompido: matar um trabalho no meio
    deixaria metade do serviço feito e ninguém saberia qual metade.
    """
    t = s.get(Tarefa, tarefa_id)
    if t is None:
        raise ErroValidacao("Trabalho não encontrado.")
    if t.situacao == "EXECUTANDO":
        raise ErroValidacao(
            "Este trabalho já começou. Interromper no meio deixaria parte do "
            "serviço feita sem ninguém saber qual parte — espere terminar.")
    if t.situacao != "PENDENTE":
        raise ErroValidacao(f"Este trabalho já está {t.situacao.lower()}.")
    _encerrar(s, t, "CANCELADA", erro="Cancelado antes de começar.")
    logger.info("ERP/tarefas: #%s cancelado por %s", t.id,
                getattr(usuario, "nome", "sistema"))
    return t


def tentar_de_novo(s: Session, tarefa_id: int, *,
                   usuario: Optional[Usuario] = None) -> Tarefa:
    """Recoloca na fila um trabalho que falhou, zerando as tentativas."""
    t = s.get(Tarefa, tarefa_id)
    if t is None:
        raise ErroValidacao("Trabalho não encontrado.")
    if t.situacao not in ("FALHADA", "CANCELADA"):
        raise ErroValidacao("Só faz sentido repetir o que falhou ou foi cancelado.")
    t.situacao = "PENDENTE"
    t.tentativas = 0
    t.passo = 0
    t.erro = None
    t.mensagem = "Recolocado na fila."
    t.concluido_em = None
    logger.info("ERP/tarefas: #%s recolocado na fila por %s", t.id,
                getattr(usuario, "nome", "sistema"))
    acordar()
    return t
