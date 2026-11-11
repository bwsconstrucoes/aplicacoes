# -*- coding: utf-8 -*-
"""
ATUALIZAR OS COLABORADORES COM OUTRA TAREFA RODANDO — 16/10/2026.

O dono: "algo errado na atualização dos colaboradores: Já existe uma
atualização em andamento (trazendo as SPs alteradas) (…) já tá há muito tempo
nisso". O pedido vai para a FILA e começa sozinho quando a outra terminar; e a
frase diz em que pé a outra está e desde que horas.
"""
import datetime as dt

import pytest

from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco


def test_COLABORADORES_com_o_cadastro_ja_rodando_vai_para_a_fila(app, monkeypatch):
    from app.apps.analisesps import tarefas
    ocupado = {"rodando": True, "interrompida": None,
               "detalhe": {"etapa": "trazendo as SPs alteradas", "progresso": "1200 de 5400",
                           "inicio": dt.datetime(2026, 10, 16, 12, 12, tzinfo=dt.timezone.utc)}}
    monkeypatch.setattr(tarefas, "estado", lambda pista="geral": ocupado)
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = cliente.post("/analisesps/api/sincronizar", json={"modo": "colaboradores"},
                         headers={"Accept": "application/json"}).get_json()
    assert r["ok"] and r["na_fila"]
    assert "1200 de 5400, desde 09:12" in r["aviso"]
    assert tarefas._pedido_pendente("colaboradores")

    disparos = []
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": True}))
    tarefas.encadear_comprovantes("sincronizar")
    assert disparos == ["colaboradores"] and not tarefas._pedido_pendente("colaboradores")


def test_outro_modo_continua_RECUSANDO_com_a_frase_do_andamento(app, monkeypatch):
    from app.apps.analisesps import tarefas
    monkeypatch.setattr(tarefas, "estado", lambda pista="geral": {
        "rodando": True, "interrompida": None,
        "detalhe": {"etapa": "trazendo as SPs alteradas", "progresso": "10 de 20"}})
    r = tarefas.disparar("apoios")
    assert not r["ok"] and r["ocupado"] and "10 de 20" in r["erro"]


def test_as_PISTAS_geral_e_do_cadastro_sao_independentes(app):
    """Com a 054 aplicada: a sincronização das SPs viva não segura o cadastro."""
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao
    assert tarefas.pista_do("colaboradores") == "cadastro"
    assert tarefas.pista_do("sincronizar") == "geral"
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     " VALUES ('sincronizar', 'tela aberta', 'trazendo as SPs alteradas', now())")
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     " VALUES ('colaboradores', 'manual', 'lendo a planilha', now())")
        conn.commit()
    assert tarefas.estado()["detalhe"]["tipo"] == "sincronizar"
    assert tarefas.estado("cadastro")["detalhe"]["tipo"] == "colaboradores"
    with pytest.raises(Exception):
        with conexao() as conn:
            conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, visto_em) "
                         " VALUES ('colaboradores', 'manual', now())")
            conn.commit()
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        a = cliente.get("/analisesps/api/andamento?pista=cadastro").get_json()
    assert a["rodando"] and a["etapa"] == "lendo a planilha"


def test_as_PISTAS_do_PONTO_e_dos_APOIOS_nao_seguram_as_SPs(app, monkeypatch):
    """31/10/2026 (migração 055): *"agora preciso que seja atualizado somente
    SPsBD e ele dá essa mensagem: Já existe uma atualização em andamento
    (buscando notas na Receita…)"*. A Receita (apoios) e o ponto do mês correm
    em pistas próprias; a geral é das SPs — e o botão Atualizar só traz as SPs."""
    from app.apps.analisesps import tarefas
    from app.apps.analisesps.db import conexao
    assert tarefas.ETAPAS["sincronizar"] == ["fila", "delta"], "o Atualizar é só a SPsBD"
    for modo, pista in (("notas_receita", "apoios"), ("apoios", "apoios"),
                        ("faturamento", "apoios"), ("ponto", "ponto"),
                        ("ponto_diario", "ponto"), ("sincronizar", "geral"),
                        ("comprovantes", "comprovantes")):
        assert tarefas.pista_do(modo) == pista, modo
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     " VALUES ('apoios', 'tela aberta', 'buscando notas na Receita', now())")
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     " VALUES ('ponto', 'manual', 'trazendo o ponto', now())")
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     " VALUES ('sincronizar', 'manual', 'trazendo as SPs alteradas', now())")
        conn.commit()
    assert tarefas.estado()["detalhe"]["tipo"] == "sincronizar"
    assert tarefas.estado("apoios")["detalhe"]["tipo"] == "apoios"
    assert tarefas.estado("ponto")["detalhe"]["tipo"] == "ponto"
    with pytest.raises(Exception):
        with conexao() as conn:      # uma viva por pista, continua valendo
            conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, visto_em) "
                         " VALUES ('notas_receita', 'manual', now())")
            conn.commit()
    # A BAIXA DOS COMPROVANTES corre junto da sincronização (migração 056, 11/11/2026).
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, etapa, visto_em) "
                     " VALUES ('comprovantes', 'manual', 'dando baixa nos comprovantes', now())")
        conn.commit()
    assert tarefas.estado("comprovantes")["detalhe"]["tipo"] == "comprovantes"
    assert tarefas.estado()["detalhe"]["tipo"] == "sincronizar", "a geral não vê a baixa"
    with pytest.raises(Exception):
        with conexao() as conn:      # uma baixa viva por vez
            conn.execute("INSERT INTO analisesps.execucoes (tipo, disparo, visto_em) "
                         " VALUES ('comprovantes', 'manual', now())")
            conn.commit()


def test_os_APOIOS_saem_sozinhos_de_hora_em_hora_na_pista_deles(app, monkeypatch):
    from app.apps.analisesps import tarefas
    disparos = []
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append((modo, disparo)) or {"ok": True}))
    monkeypatch.setattr(tarefas, "_apoios_recentes", lambda: False)
    assert tarefas.apoios_se_preciso("tela aberta")["disparou"]
    assert disparos == [("apoios", "tela aberta")]
    monkeypatch.setattr(tarefas, "_apoios_recentes", lambda: True)
    assert not tarefas.apoios_se_preciso("tela aberta")["disparou"]
