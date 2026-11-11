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
        conn.execute("DELETE FROM config WHERE chave = ?", (espelho.CHAVE_MESES_RELIDOS,))
        # um pagamento VELHO de 2016, que a releitura tem de trocar
        conn.execute("INSERT INTO movimentos (ncodtitulo, cnatureza, ddtpagamento, nvalpago,"
                     " cliquidado, sync_em) VALUES (1, 'P', '10/03/2016', 999, 'S', 'x')")
        conn.commit()
    yield painel_db
    with painel_db.conexao() as conn:   # a campanha aberta não vaza para outro teste
        conn.execute("DELETE FROM config WHERE chave = ?", (espelho.CHAVE_ANOS_RELIDOS,))
        conn.execute("DELETE FROM config WHERE chave = ?", (espelho.CHAVE_MESES_RELIDOS,))
        conn.commit()
    painel_db._engine = None


def _mov(cod, dia):
    return {"detalhes": {"nCodTitulo": cod, "cNatureza": "P", "dDtPagamento": dia},
            "resumo": {"cLiquidado": "S", "nValPago": 100.0}}


def _janela(param_extra):
    de = dt.datetime.strptime(param_extra["dDtPagtoDe"], "%d/%m/%Y").date()
    ate = dt.datetime.strptime(param_extra["dDtPagtoAte"], "%d/%m/%Y").date()
    return de, ate


class ClienteFalso:
    """Um pagamento por ano, em 10/03; pode cair num ano escolhido. Desde
    11/11/2026 a releitura pergunta mês a mês: responde só a janela pedida."""

    def __init__(self, cair_em=None):
        self.cair_em = cair_em
        self.anos_lidos = []
        self.janelas = []

    def listar_movimentos(self, *, param_extra=None, **_k):
        de, ate = _janela(param_extra)
        self.janelas.append((de, ate))
        if de.year == self.cair_em:
            raise RuntimeError("o serviço reiniciou")
        if de.year not in self.anos_lidos:
            self.anos_lidos.append(de.year)
        marco = dt.date(de.year, 3, 10)
        yield 1, 1, 1, ([_mov(de.year, marco.strftime("%d/%m/%Y"))]
                        if de <= marco <= ate else [])


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
    """O MÊS é apagado e regravado na mesma transação: se cair no meio dele, o
    que havia antes continua lá."""
    from app.apps.painel.sync import espelho

    class CaiNaSegundaPagina(ClienteFalso):
        def listar_movimentos(self, *, param_extra=None, **_k):
            de, _ate = _janela(param_extra)
            if (de.year, de.month) == (2016, 3):
                yield 1, 2, 2, [_mov(2016, "11/03/2016")]
                raise RuntimeError("caiu na página 2")
            yield from super().listar_movimentos(param_extra=param_extra)

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


@pytest.mark.banco
def test_escolher_os_anos_le_so_eles(base):
    """07/10/2026, o dono: "deveria poder eu selecionar o ano ou fazer tudo"."""
    from app.apps.painel.sync import espelho
    hoje = dt.date(2018, 6, 1)
    r = espelho.preparar_releitura([2016, 2018, 1999], hoje=hoje)
    assert r == {"aberta_antes": False, "anos": [2016, 2018]}
    cli = ClienteFalso()
    espelho.reler_pagamentos_por_ano(hoje=hoje, cli=cli)
    assert cli.anos_lidos == [2016, 2018]
    # nenhum escolhido = todos
    assert espelho.preparar_releitura([], hoje=hoje)["anos"] == [2015, 2016, 2017, 2018]


@pytest.mark.banco
def test_escolher_anos_com_releitura_pela_metade_termina_a_pendente(base):
    """O buraco da releitura cortada não pode ficar para trás porque alguém
    escolheu outro ano."""
    from app.apps.painel.sync import espelho
    hoje = dt.date(2018, 6, 1)
    with pytest.raises(RuntimeError):
        espelho.reler_pagamentos_por_ano(hoje=hoje, cli=ClienteFalso(cair_em=2017))
    r = espelho.preparar_releitura([2015], hoje=hoje)
    assert r == {"aberta_antes": True, "anos": [2017, 2018]}


@pytest.mark.banco
def test_o_botao_manda_os_anos(base, monkeypatch):
    from app.apps.painel import tarefas
    monkeypatch.setattr(tarefas, "_iniciar_processo", lambda modo, eid: None)
    with base.conexao() as conn:
        conn.execute("TRUNCATE TABLE execucoes")
        conn.commit()
    r = tarefas.disparar("pagamentos", anos=[2025])
    assert r["ok"] and "Anos: 2025." in r["descricao"]
    from app.apps.painel.sync import espelho
    with base.conexao() as conn:
        feitos = espelho._anos_relidos(conn)
        conn.execute("TRUNCATE TABLE execucoes")
        conn.commit()
    assert 2025 not in feitos and 2024 in feitos


@pytest.mark.banco
def test_o_corte_no_meio_do_ano_segue_do_mes_seguinte(base):
    """11/11/2026, o dono: "tento, tento e nunca fecha" — 2021 morria na página
    324 de 378 e a tentativa seguinte recomeçava 2021 da página 1."""
    from app.apps.painel.sync import espelho

    class CaiEmJunho(ClienteFalso):
        def listar_movimentos(self, *, param_extra=None, **_k):
            de, _ate = _janela(param_extra)
            if (de.year, de.month) == (2016, 6):
                raise RuntimeError("o serviço reiniciou")
            yield from super().listar_movimentos(param_extra=param_extra)

    hoje = dt.date(2016, 12, 31)
    with base.conexao() as conn:
        espelho._marcar_anos_relidos(conn, [2015])
        conn.commit()
    with pytest.raises(RuntimeError):
        espelho.reler_pagamentos_por_ano(hoje=hoje, cli=CaiEmJunho())
    # janeiro a maio ficaram gravados, março com o pagamento novo
    assert tuple(_contar(base, 2016)) == (1, 100)
    with base.conexao() as conn:
        assert espelho._meses_relidos(conn) == {2016: [1, 2, 3, 4, 5]}

    segue = ClienteFalso()
    r = espelho.reler_pagamentos_por_ano(hoje=hoje, cli=segue)
    assert [j[0].month for j in segue.janelas] == [6, 7, 8, 9, 10, 11, 12]
    assert r["relidos"] == [2016]
    with base.conexao() as conn:
        assert espelho._meses_relidos(conn) == {}
        assert espelho._anos_relidos(conn) == []


def test_a_etapa_nova_nao_herda_o_texto_da_anterior():
    """A tela mostrava "obras dos lançamentos" com "2021: página 324 de 378"."""
    import inspect

    from app.apps.painel import tarefas
    assert "espelho._ultimo_andamento[:] = [etapa, detalhe]" in inspect.getsource(
        tarefas.executar_trabalho)
