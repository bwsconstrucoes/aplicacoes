"""Os feriados da agenda vêm do cadastro do ponto (10/11/2026).

O dono: *"os feriados devem vir do cadastro do app ponto"*. Valem os nacionais e os
estaduais/municipais da cidade da sede; os de obra, nunca.
"""
import datetime as dt

import pytest

from app.apps.analisesps import agenda
from tests._feriados_do_ponto import _schema_ponto2, feriados_ponto  # noqa: F401

D = dt.date


def _f(data, abr, uf=None, municipio=None, nome="X"):
    return {"data": data, "nome": nome, "abrangencia": abr, "uf": uf, "municipio": municipio}


def test_so_nacionais_e_os_da_cidade_da_sede():
    linhas = [_f(D(2026, 10, 12), "NACIONAL", nome="Aparecida"),
              _f(D(2026, 3, 6), "ESTADUAL", "PE", nome="Revolução"),
              _f(D(2026, 7, 16), "MUNICIPAL", "PE", "Recife", nome="N. Sra. do Carmo"),
              _f(D(2026, 3, 12), "MUNICIPAL", "PE", "Olinda", nome="Olinda"),
              _f(D(2026, 8, 5), "ESTADUAL", "PB", nome="Paraíba"),
              _f(D(2026, 9, 9), "OBRA", nome="Da obra")]
    sem_sede = agenda.feriados_que_valem(linhas, {"uf": "", "municipio": ""})
    assert set(sem_sede) == {D(2026, 10, 12)}, "sem sede, só os nacionais"
    recife = agenda.feriados_que_valem(linhas, {"uf": "pe", "municipio": "RECIFE"})
    assert set(recife) == {D(2026, 10, 12), D(2026, 3, 6), D(2026, 7, 16)}
    so_uf = agenda.feriados_que_valem(linhas, {"uf": "PE", "municipio": ""})
    assert set(so_uf) == {D(2026, 10, 12), D(2026, 3, 6)}


def test_a_sede_e_conferida():
    with pytest.raises(ValueError):
        agenda.gravar_sede("Pernambuco", "Recife")
    with pytest.raises(ValueError):
        agenda.gravar_sede("", "Recife")


@pytest.mark.banco
def test_a_agenda_le_o_ponto_e_a_sede_escolhida(feriados_ponto):
    p = feriados_ponto
    p.nacional(D(2026, 11, 20), "Consciência Negra")
    p.estadual(D(2026, 3, 6), "PE", "Revolução Pernambucana")
    p.municipal(D(2026, 7, 16), "PE", "Recife", "Nossa Senhora do Carmo")
    p.municipal(D(2026, 3, 12), "PE", "Olinda", "Aniversário de Olinda")
    agenda.gravar_sede("", "")
    assert set(agenda.feriados_do_ponto()) == {D(2026, 11, 20)}
    agenda.gravar_sede("pe", "Recife")
    assert agenda.sede() == {"uf": "PE", "municipio": "Recife"}
    valem = agenda.feriados_do_ponto()
    assert set(valem) == {D(2026, 11, 20), D(2026, 3, 6), D(2026, 7, 16)}
    assert valem[D(2026, 7, 16)] == "Nossa Senhora do Carmo"
    do_ano = agenda.feriados_do_ano(2026)
    assert D(2026, 7, 16) in do_ano and D(2026, 12, 25) in do_ano, "os calculados continuam"
    assert D(2026, 3, 12) not in do_ano, "o de Olinda não move a agenda de Recife"
    # Uma conta que vence no feriado municipal da sede e posterga: vai para o dia 17.
    assert agenda.ajustar_dia_util(D(2026, 7, 16), "posterga", do_ano) == D(2026, 7, 17)
