# -*- coding: utf-8 -*-
"""
Reler os pagamentos ano a ano, retomando de onde parou.

06/10/2026, o dono, vendo "página 1 de 2717" depois de a leitura anterior ter
passado da 247: *"não aproveita o que já tinha lido?"*. Não aproveitava — era
uma transação só. Agora cada ano é gravado ao terminar, e a próxima tentativa
pula os anos feitos.
"""
from __future__ import annotations

import datetime as dt
import os

import pytest


@pytest.fixture()
def base():
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura
    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)
    from app.apps.painel import db as painel_db
    from app.apps.painel import migracoes_runner
    from app.apps.painel.sync import espelho
    painel_db._engine = None
    assert not migracoes_runner.aplicar_pendentes().get("erro")
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE movimentos")
        conn.execute("TRUNCATE TABLE movimentos_sem_titulo")
        conn.execute("DELETE FROM config WHERE chave = ?", (espelho.CHAVE_ANOS_RELIDOS,))
        # um pagamento VELHO de 2016, que a releitura tem de trocar
        conn.execute("INSERT INTO movimentos (ncodtitulo, cnatureza, ddtpagamento, nvalpago,"
                     " cliquidado, sync_em) VALUES (1, 'P', '10/03/2016', 999, 'S', 'x')")
        conn.commit()
    yield painel_db
    with painel_db.conexao() as conn:   # a campanha aberta não vaza para outro teste
        conn.execute("DELETE FROM config WHERE chave = ?", (espelho.CHAVE_ANOS_RELIDOS,))
        conn.commit()
    painel_db._engine = None


def _mov(cod, dia):
    return {"detalhes": {"nCodTitulo": cod, "cNatureza": "P", "dDtPagamento": dia},
            "resumo": {"cLiquidado": "S", "nValPago": 100.0}}


class ClienteFalso:
    """Um pagamento por ano; pode cair num ano escolhido."""

    def __init__(self, cair_em=None):
        self.cair_em = cair_em
        self.anos_lidos = []

    def listar_movimentos(self, *, param_extra=None, **_k):
        ano = int(param_extra["dDtPagtoDe"][-4:])
        if ano == self.cair_em:
            raise RuntimeError("o serviço reiniciou")
        self.anos_lidos.append(ano)
        yield 1, 1, 1, [_mov(ano, f"10/03/{ano}")]


def _contar(base, ano):
    with base.conexao() as conn:
        return conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(nvalpago),0) FROM movimentos"
            " WHERE ddtpagamento LIKE ?", (f"%/{ano}",)).fetchone()


@pytest.mark.banco
def test_o_corte_no_meio_nao_manda_de_volta_ao_comeco(base):
    from app.apps.painel.sync import espelho
    hoje = dt.date(2018, 6, 1)

    caiu = ClienteFalso(cair_em=2017)
    with pytest.raises(RuntimeError):
        espelho.reler_pagamentos_por_ano(hoje=hoje, cli=caiu)
    assert caiu.anos_lidos == [2015, 2016]
    # 2016 foi trocado e ficou gravado: o 999 velho saiu, o 100 novo entrou
    assert tuple(_contar(base, 2016)) == (1, 100)
    with base.conexao() as conn:
        assert espelho._anos_relidos(conn) == [2015, 2016]

    segue = ClienteFalso()
    r = espelho.reler_pagamentos_por_ano(hoje=hoje, cli=segue)
    assert segue.anos_lidos == [2017, 2018]          # pulou o que já tinha
    assert r["relidos"] == [2017, 2018] and r["pulados"] == [2015, 2016]
    # terminou: a próxima releitura é nova
    with base.conexao() as conn:
        assert espelho._anos_relidos(conn) == []
    assert tuple(_contar(base, 2018)) == (1, 100)


@pytest.mark.banco
def test_o_ano_que_caiu_no_meio_nao_fica_pela_metade(base):
    """O ano é apagado e regravado na mesma transação: se cair no meio dele, o
    que havia antes continua lá."""
    from app.apps.painel.sync import espelho

    class CaiNaSegundaPagina(ClienteFalso):
        def listar_movimentos(self, *, param_extra=None, **_k):
            ano = int(param_extra["dDtPagtoDe"][-4:])
            if ano == 2016:
                yield 1, 2, 2, [_mov(2016, "11/03/2016")]
                raise RuntimeError("caiu na página 2")
            yield 1, 1, 1, [_mov(ano, f"10/03/{ano}")]

    with pytest.raises(RuntimeError):
        espelho.reler_pagamentos_por_ano(hoje=dt.date(2017, 1, 1), cli=CaiNaSegundaPagina())
    assert tuple(_contar(base, 2016)) == (1, 999)      # o velho, intacto


@pytest.mark.banco
def test_nenhuma_atualizacao_recalcula_com_a_releitura_pela_metade(base, monkeypatch):
    """O perigo de verdade (06/10/2026): a releitura cortada deixou anos sem
    pagamento. Uma "Só refazer os números" — ou a da madrugada — mostraria
    títulos pagos como em aberto. Antes de recalcular, ela termina a releitura."""
    from app.apps.painel import tarefas
    from app.apps.painel.sync import espelho, fato

    with base.conexao() as conn:
        espelho._marcar_anos_relidos(conn, [2015])      # campanha aberta
        conn.commit()
    ordem = []
    monkeypatch.setattr(espelho, "reler_pagamentos_por_ano",
                        lambda *a, **k: ordem.append("releitura") or
                        {"relidos": [], "pulados": [], "movimentos": 0})
    monkeypatch.setattr(fato, "reconstruir", lambda conn: ordem.append("recalculo") or (1, 1))
    monkeypatch.setattr(tarefas, "_carimbar", lambda *a, **k: None)
    with base.conexao() as conn:
        eid = tarefas._abrir_execucao(conn, "so_numeros", "manual")
    assert tarefas.executar_trabalho("so_numeros", eid) is True
    assert ordem == ["releitura", "recalculo"]


@pytest.mark.banco
def test_a_releitura_antiga_sem_marca_tambem_trava_o_recalculo(base):
    """A de 06/10/2026 é de antes da marca: o que a denuncia é uma "Reler todos
    os pagamentos" que não terminou bem, sem nenhuma bem-sucedida depois."""
    from app.apps.painel import tarefas
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE execucoes")
        conn.execute("INSERT INTO execucoes (tipo, disparo, fim, ok, etapa) VALUES"
                     " ('pagamentos', 'manual', now(), FALSE, 'baixando')")
        nova = conn.execute("INSERT INTO execucoes (tipo, disparo) VALUES"
                            " ('rapida', 'agendado') RETURNING id").fetchone()[0]
        conn.commit()
    assert tarefas._releitura_pendente(nova)
    with base.conexao() as conn:
        conn.execute("INSERT INTO execucoes (tipo, disparo, fim, ok) VALUES"
                     " ('pagamentos', 'manual', now(), TRUE)")
        conn.commit()
    assert not tarefas._releitura_pendente(nova)
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE execucoes")
        conn.commit()
