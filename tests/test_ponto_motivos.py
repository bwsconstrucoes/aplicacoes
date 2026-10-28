# -*- coding: utf-8 -*-
"""Ponto — os motivos das Validações em categorias (28/10/2026, core/motivos.py): a batida em
conferência pelo texto do motivo, e o ajuste pelos códigos de credibilidade.py."""
from __future__ import annotations

from app.apps.ponto.core import credibilidade as cr
from app.apps.ponto.core import motivos as mo


def test_cada_pedaco_do_motivo_vira_uma_categoria():
    texto = ("fora da área da obra: 320 m da obra AP-01 (raio 150 m) — bateu explicando o motivo; "
             "justificativa da pessoa: estava no depósito; rosto não confere com a foto cadastral (82%)")
    assert mo.da_conferencia(texto) == ["FORA_DA_AREA", "EXPLICOU", "ROSTO_OUTRA"]
    assert mo.da_conferencia("na borda da cerca: 160 m da obra (raio 150 m; GPS com precisão de 30 m)") == ["BORDA"]
    assert mo.da_conferencia("obra sem coordenada cadastrada") == ["SEM_COORDENADA"]
    assert mo.da_conferencia("sem foto no aparelho da obra") == ["SEM_FOTO"]
    assert mo.da_conferencia("sem internet: hora do aparelho fora do esperado") == ["SEM_INTERNET"]
    assert mo.da_conferencia("nenhum rosto na foto; foto sem rosto visível") == ["SEM_ROSTO"]
    assert mo.da_conferencia("Fora da cerca: 900 m") == ["FORA_DA_AREA"]               # maiúscula e acento não importam


def test_motivo_desconhecido_nunca_some():
    assert mo.da_conferencia("algo que ninguém previu") == ["OUTRO"]
    assert mo.da_conferencia(None) == ["OUTRO"]


def test_os_rotulos_cobrem_os_codigos_do_ajuste():
    r = mo.rotulos()
    s = cr.sinais(pedido_por={"nome": "Carlos", "propria": False}, origem="RESPONSAVEL", hora="21:30", obra="BR-02",
                  batidas=[{"hora": "07:00", "obra": "AP-01"}], pedidos_no_dia=1, previstas=["07:00", "17:00"],
                  situacao="INCOMPLETO", feriado=None, fotos_ruins=["07:00 — o rosto não confere"],
                  em_conferencia=[], fora_da_area=["às 06:58"], dias_depois=10, dias_com_ajuste=7, negados_recentes=1)
    assert set(s["codigos"]) == {"AJ_POR_OUTRO", "AJ_SO_UMA", "AJ_OBRA_DIFERENTE", "AJ_LONGE_DA_ESCALA", "AJ_FOTO",
                                 "AJ_FORA_DA_AREA", "AJ_TARDIO", "AJ_MUITOS", "AJ_NEGADO"}
    assert all(c in r and r[c]["grupo"] == mo.GRUPO_AJUSTE for c in s["codigos"])
    assert all(c in r for c in mo.da_conferencia("fila rápida: 5 ou mais pessoas; pessoa afastada no cadastro"))
