# -*- coding: utf-8 -*-
"""
A atualização roda FORA do processo que atende as telas — e por que isso importa.

Três cargas seguidas morreram em produção sem deixar rastro. A causa não era o
código do painel: o serviço sobe com

    gunicorn ... --workers 1 --max-requests 150 --max-requests-jitter 40

e `--max-requests` manda o gunicorn **reiniciar o processo** a cada ~150
requisições — uma proteção contra vazamento de memória, posta depois do OOM de
julho de 2026. Com um worker só, esse reinício leva junto qualquer thread de
fundo. A própria tela de acompanhamento consultava o servidor a cada 5 segundos:
12 requisições por minuto, mais o resto do monorepo. O processo se reciclava a
cada poucos minutos, e uma carga de horas nunca teria chance de terminar.

Mexer no `--max-requests` não era opção: ele protege os outros 14 módulos.

O que se prova aqui:
  - o disparo cria um processo SEPARADO e destacado, não uma thread;
  - esse processo continua vivo depois de quem o iniciou morrer — que é a
    diferença entre a carga terminar e não terminar;
  - a trava de "uma de cada vez" mora no BANCO, porque processos diferentes não
    enxergam a memória um do outro;
  - o trabalho em si registra início, andamento e fim, e uma falha no meio vira
    mensagem na tela em vez de silêncio.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.banco

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture()
def banco_do_painel():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    url = url_de_teste_segura(bruto)
    os.environ["DATABASE_URL"] = url

    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner

    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), f"migração falhou: {resultado}"

    def _limpar():
        with painel_db.conexao() as conn:
            conn.execute("TRUNCATE TABLE execucoes")
            conn.commit()

    _limpar()
    yield url
    _limpar()
    painel_db._engine = None


# ---------------------------------------------------------------------------
# 1. É processo separado, não thread
# ---------------------------------------------------------------------------
def test_disparar_cria_processo_e_nao_thread(banco_do_painel, monkeypatch):
    """Se voltasse a ser thread, a carga voltaria a morrer com o reinício do
    worker — e de novo sem deixar rastro."""
    from app.apps.painel import tarefas

    chamadas = []
    monkeypatch.setattr(tarefas, "_iniciar_processo",
                        lambda modo, eid: chamadas.append((modo, eid)))

    resposta = tarefas.disparar("so_numeros", "manual")
    assert resposta["ok"] is True
    assert len(chamadas) == 1
    assert chamadas[0][0] == "so_numeros"
    assert chamadas[0][1] == resposta["execucao"]

    # a execução já existe no banco antes de o processo começar: a tela tem o
    # que mostrar desde o primeiro instante
    from app.apps.painel import consultas
    andamento = consultas.execucao_em_andamento()
    assert andamento["id"] == resposta["execucao"]
    assert andamento["viva"] is True


def test_o_processo_e_destacado_de_quem_o_inicia(banco_do_painel):
    """"Destacado" é o ponto todo: sem sessão própria, o sinal que o gunicorn
    manda ao worker alcança o filho e mata a carga junto."""
    import inspect
    from app.apps.painel import tarefas

    codigo = inspect.getsource(tarefas._iniciar_processo)
    assert "start_new_session" in codigo          # POSIX (é onde o Render roda)
    assert "DETACHED_PROCESS" in codigo           # Windows, para desenvolver aqui
    assert "executar_sync" in codigo


def test_um_processo_separado_sobrevive_a_morte_de_quem_o_criou(banco_do_painel):
    """A prova de fogo, com processos de verdade.

    Um processo "pai" inicia um "filho" destacado e morre em seguida. O filho
    tem de continuar e terminar o trabalho. É exatamente a situação do gunicorn
    reciclando o worker no meio da carga."""
    marcador = os.path.join(os.environ.get("TEMP", "/tmp"), "painel_filho_vivo.txt")
    if os.path.exists(marcador):
        os.remove(marcador)

    filho = (
        "import sys, time\n"
        "time.sleep(2.5)\n"
        "open(sys.argv[1], 'w').write('cheguei ao fim')\n"
    )
    pai = (
        "import subprocess, sys, os\n"
        "extras = {'start_new_session': True} if os.name == 'posix' else "
        "{'creationflags': getattr(subprocess,'DETACHED_PROCESS',0)"
        " | getattr(subprocess,'CREATE_NEW_PROCESS_GROUP',0)}\n"
        f"subprocess.Popen([sys.executable, '-c', {filho!r}, sys.argv[1]],\n"
        "                 stdin=subprocess.DEVNULL, close_fds=True, **extras)\n"
    )

    processo_pai = subprocess.Popen([sys.executable, "-c", pai, marcador])
    processo_pai.wait(timeout=30)
    assert processo_pai.returncode == 0

    # o pai já morreu; o filho ainda estava dormindo quando isso aconteceu
    assert not os.path.exists(marcador), "o filho terminou antes do pai morrer"

    limite = time.time() + 20
    while time.time() < limite and not os.path.exists(marcador):
        time.sleep(0.3)

    assert os.path.exists(marcador), \
        "o processo filho morreu junto com o pai — a carga voltaria a ser perdida"
    os.remove(marcador)


# ---------------------------------------------------------------------------
# 2. A trava mora no banco
# ---------------------------------------------------------------------------
def test_nao_deixa_comecar_duas_ao_mesmo_tempo(banco_do_painel, monkeypatch):
    """Antes a trava era uma variável em memória. Com o trabalho em outro
    processo, memória não serve: dois processos não a enxergam. Quem responde
    "já tem uma rodando" é o banco."""
    from app.apps.painel import tarefas

    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, eid: None)

    primeira = tarefas.disparar("carga_inicial", "manual")
    assert primeira["ok"] is True

    segunda = tarefas.disparar("carga_inicial", "manual")
    assert segunda["ok"] is False
    assert "já existe" in segunda["erro"].lower()


def test_execucao_morta_nao_bloqueia_a_proxima(banco_do_painel, monkeypatch):
    """Se a trava do banco não soubesse distinguir viva de morta, uma carga
    interrompida travaria o painel para sempre."""
    from app.apps.painel import tarefas
    from app.apps.painel.db import conexao

    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, eid: None)
    primeira = tarefas.disparar("carga_inicial", "manual")

    with conexao() as conn:
        conn.execute(
            "UPDATE execucoes SET visto_em = now() - interval '45 minutes' "
            " WHERE id = ?", (primeira["execucao"],))
        conn.commit()

    segunda = tarefas.disparar("carga_inicial", "manual")
    assert segunda["ok"] is True
    assert segunda["execucao"] != primeira["execucao"]


def test_modo_desconhecido_nao_abre_execucao(banco_do_painel):
    from app.apps.painel import consultas, tarefas
    assert tarefas.disparar("inventado")["ok"] is False
    assert consultas.execucao_em_andamento() is None


# ---------------------------------------------------------------------------
# 3. O trabalho registra o que aconteceu
# ---------------------------------------------------------------------------
def test_trabalho_bem_sucedido_fecha_a_execucao_com_o_resultado(banco_do_painel,
                                                                monkeypatch):
    from app.apps.painel import consultas, tarefas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import fato

    monkeypatch.setattr(fato, "reconstruir", lambda conn: (185422, 2240))

    with conexao() as conn:
        execucao_id = tarefas._abrir_execucao(conn, "so_numeros", "manual")
    assert tarefas.executar_trabalho("so_numeros", execucao_id) is True

    ultima = consultas.atualizado_em()
    assert ultima["ok"] is True
    assert "185.422" in ultima["mensagem"]
    assert ultima["linhas"] == 185422
    assert consultas.execucao_em_andamento() is None      # não ficou aberta


def test_falha_no_meio_vira_mensagem_na_tela(banco_do_painel, monkeypatch):
    """Silêncio é o pior resultado possível: foi o que fez o dono passar horas
    sem saber o que estava acontecendo."""
    from app.apps.painel import consultas, tarefas
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import fato

    def _explodir(_conn):
        raise RuntimeError("o OMIE recusou a chave")

    monkeypatch.setattr(fato, "reconstruir", _explodir)

    with conexao() as conn:
        execucao_id = tarefas._abrir_execucao(conn, "so_numeros", "manual")
    assert tarefas.executar_trabalho("so_numeros", execucao_id) is False

    ultima = consultas.atualizado_em()
    assert ultima["ok"] is False
    assert "o OMIE recusou a chave" in ultima["mensagem"]
    assert consultas.execucao_em_andamento() is None


def test_o_ponto_de_entrada_do_processo_recusa_argumento_errado():
    """Chamado errado tem de falhar com mensagem, não com um traceback."""
    from app.apps.painel.executar_sync import main
    assert main([]) == 2
    assert main(["so_numeros"]) == 2
