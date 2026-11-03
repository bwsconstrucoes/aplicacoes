"""O gancho de saída do worker (gunicorn.conf.py) — ver CONTEXTO.md, 03/11/2026."""

import importlib.util
import os
import pathlib
import subprocess
import sys
import textwrap
import threading
import time

RAIZ = pathlib.Path(__file__).resolve().parents[1]


def _carregar():
    spec = importlib.util.spec_from_file_location("gunicorn_conf", RAIZ / "gunicorn.conf.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Worker:
    def __init__(self, pid):
        self.pid = pid


class _Servidor:
    class log:  # noqa: N801 — imita o logger do gunicorn
        @staticmethod
        def warning(*a, **k):
            pass


def test_no_processo_mestre_o_gancho_nao_arma_nada():
    """O mestre também chama worker_exit ao recolher um worker morto: lá não pode sair."""
    conf = _carregar()
    antes = {t.name for t in threading.enumerate()}
    conf.worker_exit(_Servidor, _Worker(os.getpid() + 1))
    assert "prazo-de-saida" not in {t.name for t in threading.enumerate()} - antes


def test_cronometro_e_daemon_e_nao_segura_a_saida_normal():
    conf = _carregar()
    assert conf.PRAZO_SAIDA_S <= 60
    criados = []
    original = threading.Timer

    def timer_falso(*a, **k):
        t = original(3600, lambda: None)
        criados.append(t)
        return t

    threading.Timer = timer_falso
    try:
        conf.worker_exit(_Servidor, _Worker(os.getpid()))
    finally:
        threading.Timer = original
    assert len(criados) == 1 and criados[0].daemon
    criados[0].cancel()


def test_processo_preso_numa_thread_sai_no_prazo(tmp_path):
    """Reproduz a saída do worker com uma requisição presa: sem o gancho, o
    processo esperaria a thread para sempre."""
    script = tmp_path / "preso.py"
    script.write_text(textwrap.dedent(f"""
        import importlib.util, os, threading, time
        spec = importlib.util.spec_from_file_location("c", {str(RAIZ / "gunicorn.conf.py")!r})
        c = importlib.util.module_from_spec(spec); spec.loader.exec_module(c)
        c.PRAZO_SAIDA_S = 1
        class W: pid = os.getpid()
        class S:
            class log:
                @staticmethod
                def warning(*a, **k): print("AVISO", a[0] % a[1:] if len(a) > 1 else a[0], flush=True)
        threading.Thread(target=time.sleep, args=(120,), name="requisicao-presa").start()
        c.worker_exit(S, W())
    """))
    inicio = time.monotonic()
    r = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=60)
    assert time.monotonic() - inicio < 30
    assert r.returncode == 0
    assert "requisicao-presa" in r.stdout
