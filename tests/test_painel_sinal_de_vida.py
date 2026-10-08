# -*- coding: utf-8 -*-
"""
Atualização calada não é atualização morta.

07/10/2026: "Atualizar um período" foi dada por morta no passo "atualizando o
plano de contas e os cadastros" — a leitura dos fornecedores não dava sinal a
cada página, e 10 minutos de silêncio eram tratados como morte (e o vigia
disparava outra por cima).
"""
from __future__ import annotations

import subprocess
import sys
import time

import pytest

from app.apps.painel import executar_sync
from app.apps.painel.sync import espelho, omie_client


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="lê /proc")
def test_reconhece_o_processo_da_execucao():
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)",
                             executar_sync.MODULO, "periodo", "987654"])
    try:
        for _ in range(50):
            if executar_sync.processo_vivo(987654):
                break
            time.sleep(0.1)
        assert executar_sync.processo_vivo(987654)
        assert not executar_sync.processo_vivo(98765)
    finally:
        proc.kill()
        proc.wait()
    assert not executar_sync.processo_vivo(987654)


def test_toda_chamada_ao_omie_da_sinal_de_vida_sem_mudar_o_texto(monkeypatch):
    vistos = []
    espelho.definir_progresso(lambda etapa, detalhe: vistos.append((etapa, detalhe)))
    try:
        espelho._progresso("atualizando o plano de contas e os cadastros",
                           "fornecedores e clientes: página 3 de 20")
        vistos.clear()

        class Resposta:
            status_code = 200

            @staticmethod
            def json():
                return {"ok": 1}

        cli = omie_client.OmieClient("k", "s")
        monkeypatch.setattr(cli.sessao, "post", lambda *a, **k: Resposta())
        assert cli._call("u", "Listar", {}) == {"ok": 1}
        assert vistos == [("atualizando o plano de contas e os cadastros",
                           "fornecedores e clientes: página 3 de 20")]
    finally:
        espelho.definir_progresso(None)


def test_os_cadastros_dao_sinal_a_cada_pagina():
    import inspect
    fonte = inspect.getsource(espelho.sync_incremental)
    assert "fornecedores e clientes: página" in fonte
    assert "plano de contas: página" in fonte
