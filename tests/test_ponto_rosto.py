# -*- coding: utf-8 -*-
"""Ponto — a conferência do rosto pela AWS (pedido do dono, 06/10/2026). As
regras de exclusão e a leitura da resposta da AWS, sem banco e sem AWS."""
from __future__ import annotations

import datetime as dt

from app.apps.ponto.core import rosto


def _m(dia_semana: int, hh: int, mm: int = 0) -> dt.datetime:
    base = dt.datetime(2026, 10, 5, hh, mm)            # 05/10/2026 é uma segunda
    return base + dt.timedelta(days=dia_semana)


def test_padrao_confere_tudo():
    assert rosto.entra_na_regra(dict(rosto.PADRAO), obra_id=1, momento_local=_m(0, 7), primeira_do_dia=False)


def test_exclusoes_por_obra_dia_horario_e_so_a_primeira():
    cfg = {**rosto.PADRAO, "obras_excluidas": [9], "dias_excluidos": [6],
           "horarios_excluidos": [{"de": "11:00", "ate": "13:30"}, {"de": "22:00", "ate": "05:00"}]}
    assert not rosto.entra_na_regra(cfg, obra_id=9, momento_local=_m(0, 7), primeira_do_dia=True)
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(6, 7), primeira_do_dia=True)   # domingo
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(0, 12), primeira_do_dia=True)
    assert rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(0, 13, 30), primeira_do_dia=True)
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(0, 23), primeira_do_dia=True)  # vira a noite
    assert not rosto.entra_na_regra(cfg, obra_id=1, momento_local=_m(1, 4), primeira_do_dia=True)
    assert rosto.entra_na_regra({**cfg, "so_primeira_do_dia": True}, obra_id=1, momento_local=_m(0, 7),
                                primeira_do_dia=True)
    assert not rosto.entra_na_regra({**cfg, "so_primeira_do_dia": True}, obra_id=1, momento_local=_m(0, 17),
                                    primeira_do_dia=False)


class _AwsFalsa:
    def __init__(self, resposta=None, erro=None):
        self.resposta, self.erro = resposta, erro

    def compare_faces(self, **_):
        if self.erro:
            raise self.erro
        return self.resposta


class InvalidParameterException(Exception):
    pass


def test_le_a_resposta_da_aws():
    assert rosto.comparar(_AwsFalsa({"FaceMatches": [{"Similarity": 98.4}]}), b"a", b"b", 90) == \
        ("MESMA_PESSOA", 98.4, "")
    r = rosto.comparar(_AwsFalsa({"FaceMatches": [{"Similarity": 61.0}]}), b"a", b"b", 90)
    assert r[0] == "OUTRA_PESSOA" and "61 %" in r[2]
    assert rosto.comparar(_AwsFalsa({"FaceMatches": [], "UnmatchedFaces": [{}]}), b"a", b"b", 90)[0] == "OUTRA_PESSOA"
    assert rosto.comparar(_AwsFalsa({"FaceMatches": [], "UnmatchedFaces": []}), b"a", b"b", 90)[0] == "SEM_ROSTO"
    assert rosto.comparar(_AwsFalsa(erro=InvalidParameterException("no face")), b"a", b"b", 90)[0] == "SEM_ROSTO"
