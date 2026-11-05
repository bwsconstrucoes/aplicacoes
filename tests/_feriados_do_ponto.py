"""Feriados e obras NO PONTO, para os testes da folha do Análise de SPs.

Desde 04/11/2026 a folha lê os feriados do ponto (`folha_calendario.FeriadosDoPonto`).
Os testes de auxílio, diaristas e da tela de Feriados precisam do schema do ponto
e de controle total sobre quais feriados existem no período.

⚠️ O PONTO NASCE COM OS FERIADOS NACIONAIS DE 2026 E 2027 (migração 002 dele), e os
testes do ponto contam com eles. Por isso esta fixture GUARDA a tabela inteira na
entrada, esvazia, e DEVOLVE tudo na saída — apagar os nacionais quebraria o teste
do ponto que rodar depois no mesmo trabalhador.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests.test_ponto_gestao_banco import _schema_ponto2  # noqa: F401

PREFIXO = "FC-"


class Ponto:
    def __init__(self, banco):
        self.banco = banco

    def _feriado(self, data, nome, abrangencia, uf=None, municipio=None, obra_id=None):
        with self.banco.connect() as conn:
            conn.execute(text(
                "INSERT INTO ponto.feriados (data, nome, abrangencia, uf, municipio, obra_id) "
                "VALUES (:d, :n, :a, :uf, :m, :o)"),
                {"d": data, "n": nome, "a": abrangencia, "uf": uf, "m": municipio, "o": obra_id})
            conn.commit()

    def nacional(self, data: dt.date, nome="Feriado"):
        self._feriado(data, nome, "NACIONAL")

    def estadual(self, data: dt.date, uf: str, nome="Estadual"):
        self._feriado(data, nome, "ESTADUAL", uf=uf)

    def municipal(self, data: dt.date, uf: str, municipio: str, nome="Municipal"):
        self._feriado(data, nome, "MUNICIPAL", uf=uf, municipio=municipio)

    def da_obra(self, data: dt.date, codigo: str, nome="Da obra"):
        self._feriado(data, nome, "OBRA", obra_id=self.obra_id(codigo))

    def obra(self, codigo: str, nome: str, uf=None, municipio=None) -> int:
        assert codigo.startswith(PREFIXO), "obra de teste começa com FC-"
        with self.banco.connect() as conn:
            obra_id = conn.execute(text(
                "INSERT INTO obras (codigo, nome, uf, municipio, status) "
                "VALUES (:c, :n, :uf, :m, 'ATIVA') RETURNING id"),
                {"c": codigo, "n": nome, "uf": uf, "m": municipio}).scalar_one()
            conn.commit()
        return obra_id

    def obra_id(self, codigo: str) -> int:
        with self.banco.connect() as conn:
            return conn.execute(text("SELECT id FROM obras WHERE codigo = :c"),
                                {"c": codigo}).scalar_one()

    def contar(self) -> int:
        with self.banco.connect() as conn:
            return conn.execute(text("SELECT count(*) FROM ponto.feriados")).scalar_one()


@pytest.fixture
def feriados_ponto(_schema_ponto2, banco, banco_analisesps):
    """O ponto com a tabela de feriados VAZIA, e as obras FC-* para o teste."""
    from app.apps.analisesps import folha_calendario
    from app.apps.ponto.core import base_obras

    with banco.connect() as conn:
        guardados = [dict(r._mapping) for r in conn.execute(text("SELECT * FROM ponto.feriados"))]
        conn.execute(text("DELETE FROM ponto.feriados"))
        conn.execute(text("DELETE FROM obras WHERE codigo LIKE 'FC-%'"))
        # O que a busca automática já buscou (feriados_busca.py), para cada teste começar do zero.
        conn.execute(text("DELETE FROM ponto.parametros WHERE chave = 'feriados.busca'"))
        conn.commit()
    base_obras.esquecer()
    folha_calendario.esquecer_leituras()
    yield Ponto(banco)
    folha_calendario.esquecer_leituras()
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM ponto.feriados"))
        conn.execute(text("DELETE FROM obras WHERE codigo LIKE 'FC-%'"))
        conn.execute(text("DELETE FROM ponto.parametros WHERE chave = 'feriados.busca'"))
        for linha in guardados:
            colunas = ", ".join(linha)
            conn.execute(text(f"INSERT INTO ponto.feriados ({colunas}) VALUES "
                              f"({', '.join(':' + c for c in linha)})"), linha)
        conn.commit()
