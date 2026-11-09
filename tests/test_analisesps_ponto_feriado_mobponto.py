"""O feriado nacional que o MobPonto não tinha — 05/11/2026.

O dono: *"esse feriado não foi aplicado no MobPonto (…) aí está vindo como falta.
Você consegue, mesmo sendo MobPonto, forçar a entender que é feriado?"*
"""
import datetime as dt

D = dt.date


def _dia(data, presenca="", falta="", marcacoes=None):
    return {"data": data, "marcacoes": marcacoes or ["", "", "", ""],
            "presenca": presenca, "falta": falta}


def _com_feriados(monkeypatch, nacionais):
    from app.apps.analisesps import folha_calendario
    monkeypatch.setattr(folha_calendario, "feriados_no_periodo",
                        lambda inicio, fim, obra="": [
                            {"data": d, "descricao": n, "obra": obra} for d, n in nacionais
                            if inicio <= d <= fim and not obra])


def test_FALTA_do_MobPonto_no_12_10_vira_feriado(monkeypatch):
    from app.apps.analisesps import ponto
    _com_feriados(monkeypatch, [(D(2026, 10, 12), "Nossa Senhora Aparecida")])
    dias = [_dia(D(2026, 10, 12), "Falta", "Falta sem justificativa"),
            _dia(D(2026, 10, 13), "Falta", "Falta sem justificativa")]
    ponto._feriado_nacional_por_cima(dias, 2026, 10)
    assert (dias[0]["presenca"], dias[0]["falta"]) == ("Feriado", "")
    assert dias[0]["feriado_forcado"] == "Nossa Senhora Aparecida"
    assert dias[1]["falta"] == "Falta sem justificativa", "o dia normal continua falta"


def test_quem_TRABALHOU_no_feriado_e_o_atestado_ficam_como_vieram(monkeypatch):
    from app.apps.analisesps import ponto
    _com_feriados(monkeypatch, [(D(2026, 10, 12), "Nossa Senhora Aparecida")])
    trabalhou = _dia(D(2026, 10, 12), "Presença", "", ["OBRA1", "OBRA1", "", ""])
    atestado = _dia(D(2026, 10, 12), "Atestado", "")
    ponto._feriado_nacional_por_cima([trabalhou, atestado], 2026, 10)
    assert trabalhou["presenca"] == "Presença" and atestado["presenca"] == "Atestado"


def test_e_a_falta_forcada_a_feriado_nao_entra_mais_como_ausencia(monkeypatch):
    """A ponta que o dono vê: o 12/10 sai da lista do "Aplicar desconto"."""
    from app.apps.analisesps import folha_auxilio, ponto
    _com_feriados(monkeypatch, [(D(2026, 10, 12), "Nossa Senhora Aparecida")])
    dias = ponto._feriado_nacional_por_cima(
        [_dia(D(2026, 10, 12), "Falta", "Falta sem justificativa")], 2026, 10)
    assert folha_auxilio.ausencia_do_dia(dias[0]) == ""


def test_sem_o_ponto_nada_muda(monkeypatch):
    from app.apps.analisesps import ponto
    _com_feriados(monkeypatch, [])
    dias = [_dia(D(2026, 10, 12), "Falta", "Falta sem justificativa")]
    ponto._feriado_nacional_por_cima(dias, 2026, 10)
    assert dias[0]["falta"] == "Falta sem justificativa"
