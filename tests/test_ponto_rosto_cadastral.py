# -*- coding: utf-8 -*-
"""Ponto — a foto com o rosto à vista que dava "sem rosto" (28/10/2026, o dono: "está dizendo
que não foi detectado um rosto e está claramente correto, na hora que a gente abre a foto").
A AWS recusa comparar quando a foto de CADASTRO não tem rosto; agora isso é dito como tal, e a
batida sem rosto ganha uma segunda tentativa, com a foto clareada e ampliada."""
from __future__ import annotations

import io

from app.apps.ponto.core import confianca, fotos_cadastro, rosto


class InvalidParameterException(Exception):
    pass


class _Aws:
    def __init__(self, respostas, rostos_na_cadastral=True):
        self.respostas, self.rostos, self.chamadas = list(respostas), rostos_na_cadastral, 0

    def compare_faces(self, **_):
        self.chamadas += 1
        r = self.respostas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    def detect_faces(self, **_):
        return {"FaceDetails": [{}] if self.rostos else []}


def _jpeg():
    from PIL import Image
    s = io.BytesIO()
    Image.new("RGB", (320, 240), (90, 80, 70)).save(s, format="JPEG")
    return s.getvalue()


def test_a_foto_de_cadastro_sem_rosto_nao_vira_batida_sem_rosto():
    r = rosto.comparar(_Aws([InvalidParameterException("no face")], rostos_na_cadastral=False), b"a", b"b", 90)
    assert r[0] == "SEM_CADASTRAL" and "foto de CADASTRO" in r[2]
    # com rosto na cadastral, a culpa é mesmo da batida
    assert rosto.comparar(_Aws([InvalidParameterException("no face")]), b"a", b"b", 90)[0] == "SEM_ROSTO"


def test_a_batida_sem_rosto_ganha_uma_segunda_tentativa_clareada():
    aws = _Aws([{"FaceMatches": [], "UnmatchedFaces": []}, {"FaceMatches": [{"Similarity": 97.0}]}])
    assert rosto.comparar(aws, b"cad", _jpeg(), 90) == ("MESMA_PESSOA", 97.0, "")
    assert aws.chamadas == 2
    aws = _Aws([{"FaceMatches": [], "UnmatchedFaces": []}, {"FaceMatches": [], "UnmatchedFaces": []}])
    assert rosto.comparar(aws, b"cad", _jpeg(), 90)[0] == "SEM_ROSTO"
    realcada = rosto.realcar(_jpeg())
    from PIL import Image
    assert max(Image.open(io.BytesIO(realcada)).size) == 1600


def test_a_foto_de_cadastro_fica_suspeita_com_sem_rosto_em_serie():
    s = fotos_cadastro.suspeita
    assert s([fotos_cadastro.CADASTRAL_SEM_ROSTO, "MESMA_PESSOA"])
    assert s(["SEM_ROSTO", "SEM_ROSTO", "MESMA_PESSOA"])
    assert not s(["SEM_ROSTO", "MESMA_PESSOA", "MESMA_PESSOA"])
    assert s(["OUTRA_PESSOA", "OUTRA_PESSOA", "MESMA_PESSOA"])


def test_o_nivel_explicado_em_uma_frase():
    d = confianca.config({})
    novo = confianca.nivel(0, 0, d)
    assert "todo mundo começa" in confianca.porque(novo, None, d)
    falha = {"quando": "20/10", "hora": "07:00", "rotulo": "o rosto NÃO confere", "resultado": "OUTRA_PESSOA"}
    assert "20/10" in confianca.porque(novo, falha, d) and "Faltam" in confianca.porque(novo, falha, d)
    reinc = confianca.nivel(0, int(d["reincidente_falhas"]), d)
    assert "Reincidente" in confianca.porque(reinc, falha, d)
