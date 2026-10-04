# -*- coding: utf-8 -*-
"""Ponto — o pedido de ajuste a partir do dia e as travas contra o pedido
errado (o que acontecia no Pipefy). Regras puras, sem banco."""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto.core import ajustes
from app.apps.ponto.erros import ErroDeValidacao

DIA = dt.date(2026, 10, 5)
PREV = ajustes.marcas_previstas([(420, 660), (720, 1020)])        # 07-11, 12-17


def _conferir(novos, existentes=(), pendentes=(), dia=None, previstas=PREV, agora=None):
    ajustes.conferir_pedido(dia=dia or {"situacao": "INCOMPLETO"}, data=DIA, novos_min=list(novos),
                            existentes_min=list(existentes), pendentes_min=list(pendentes),
                            previstas=previstas, agora_min=agora)


class TestOQueFalta:
    def test_nomes_dos_horarios_da_escala(self):
        assert [p["rotulo"] for p in PREV] == ["Entrada", "Saída para o intervalo", "Volta do intervalo", "Saída"]
        assert [p["hora"] for p in PREV] == ["07:00", "11:00", "12:00", "17:00"]

    def test_faltou_a_volta_do_almoco_e_a_saida(self):
        f = ajustes.faltantes(PREV, [422, 661])
        assert [x["rotulo"] for x in f] == ["Volta do intervalo", "Saída"] and f[0]["sugestao"] == "12:00"

    def test_batida_atrasada_ainda_conta_como_a_dela(self):
        assert ajustes.faltantes(PREV, [470, 660, 725, 1080]) == []          # entrou 07:50, saiu 18:00

    def test_uma_batida_nao_vale_por_duas(self):
        assert [x["rotulo"] for x in ajustes.faltantes(PREV, [690])] == ["Entrada", "Volta do intervalo", "Saída"]

    def test_turno_da_noite(self):
        p = ajustes.marcas_previstas([(1140, 1860)])                       # 19:00 → 07:00
        assert [x["hora"] for x in p] == ["19:00", "07:00"]
        assert ajustes.faltantes(p, [1142]) [0]["rotulo"] == "Saída"


class TestTravas:
    def test_pedido_certo_passa(self):
        _conferir([720, 1020], existentes=[420, 660])

    def test_ja_existe_batida_perto(self):
        with pytest.raises(ErroDeValidacao, match="já existe batida às 07:02"):
            _conferir([430], existentes=[422])

    def test_ja_existe_pedido_esperando(self):
        with pytest.raises(ErroDeValidacao, match="pedido esperando decisão para as 12:00"):
            _conferir([730], existentes=[420, 660], pendentes=[720])

    def test_dois_horarios_iguais_no_mesmo_pedido(self):
        with pytest.raises(ErroDeValidacao, match="perto demais"):
            _conferir([720, 735], existentes=[420])

    def test_dia_completo(self):
        with pytest.raises(ErroDeValidacao, match="já tem as 4 batidas"):
            _conferir([900], existentes=[420, 660, 720, 1020])

    def test_pedido_demais(self):
        with pytest.raises(ErroDeValidacao, match="pedido demais"):
            _conferir([720, 900, 1020], existentes=[420, 660])

    def test_dia_justificado(self):
        with pytest.raises(ErroDeValidacao, match="justificado"):
            _conferir([420], dia={"situacao": "ABONADO", "ocorrencia": {"id": 9, "rotulo": "Atestado"}})

    def test_futuro(self):
        with pytest.raises(ErroDeValidacao, match="ainda não chegou"):
            _conferir([420], dia={"situacao": "FUTURO"})
        with pytest.raises(ErroDeValidacao, match="17:00 ainda não chegou"):
            _conferir([1020], existentes=[420], agora=900)

    def test_sem_escala_aceita_ate_seis(self):
        _conferir([420, 720], previstas=[])
        with pytest.raises(ErroDeValidacao):
            _conferir([400, 500, 600, 700, 800, 900, 1000], previstas=[])

    def test_motivos_tem_texto(self):
        assert "CELULAR" in ajustes.MOTIVOS and all(ajustes.MOTIVOS.values())
