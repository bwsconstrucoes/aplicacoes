# -*- coding: utf-8 -*-
"""Ponto — a cerca que BLOQUEIA e a obra detectada pela localização (decisão do
dono, 04/10/2026). As regras puras, sem banco."""
from __future__ import annotations

from app.apps.ponto.core import geo, marcacoes

A = {"id": 1, "codigo": "A", "latitude": -3.7275, "longitude": -38.5270, "raio_metros": 200}
B = {"id": 2, "codigo": "B", "latitude": -3.7400, "longitude": -38.5270, "raio_metros": 300}
SEM = {"id": 3, "codigo": "S", "latitude": None, "longitude": None, "raio_metros": 200}


def _lugar(lat, lon, precisao=0, enviada=None, no_tablet=False, modo="BLOQUEAR", porque=""):
    situacao, detectada, d = geo.localizar_obra(lat, lon, precisao, [A, B, SEM])
    return marcacoes.decidir_lugar(situacao=situacao, detectada=detectada, distancia=d, enviada=enviada,
                                   no_tablet=no_tablet, precisao=precisao, modo=lambda o: modo,
                                   latitude=lat, longitude=lon, justificativa=porque)


class TestLocalizarObra:
    def test_dentro_de_cada_obra(self):
        assert geo.localizar_obra(-3.7276, -38.5271, 10, [A, B, SEM])[:2] == (geo.DENTRO, A)
        assert geo.localizar_obra(-3.7401, -38.5270, 10, [A, B, SEM])[:2] == (geo.DENTRO, B)

    def test_borda_conta_a_precisao_ate_150_m(self):
        # ~261 m de A (raio 200)
        assert geo.localizar_obra(-3.72985, -38.5270, 80, [A])[0] == geo.BORDA
        assert geo.localizar_obra(-3.72985, -38.5270, 20, [A])[0] == geo.FORA
        # precisão absurda não vira passe livre: a folga para em 150 m
        assert geo.localizar_obra(-3.7320, -38.5270, 5000, [A])[0] == geo.FORA

    def test_fora_devolve_a_mais_perto(self):
        situacao, obra, d = geo.localizar_obra(-3.7600, -38.5270, 0, [A, B])
        assert situacao == geo.FORA and obra is B and 2000 < d < 2400

    def test_sem_localizacao(self):
        assert geo.localizar_obra(None, None, 0, [A])[0] == geo.SEM_LOCAL
        assert geo.localizar_obra(0, 0, 0, [A])[0] == geo.SEM_LOCAL

    def test_distancia_legivel(self):
        assert geo.distancia_legivel(850) == "850 m" and geo.distancia_legivel(1430) == "1,4 km"


class TestDecidirLugar:
    def test_a_obra_e_a_da_cerca_nao_a_escolhida(self):
        obra, recusa, analise = _lugar(-3.7401, -38.5270, enviada=A)
        assert obra is B and recusa is None and analise is None

    def test_fora_bloqueia_e_diz_a_distancia(self):
        obra, recusa, _ = _lugar(-3.7600, -38.5270, enviada=A)
        assert obra is None and recusa.startswith("fora da área da obra") and "obra A" in recusa

    def test_fora_sem_obra_escolhida_nao_cita_a_mais_perto(self):
        # 10/10/2026: "às vezes a pessoa não tem nada a ver com aquela obra"
        for no_tablet in (True, False):
            obra, recusa, _ = _lugar(-3.7600, -38.5270, no_tablet=no_tablet)
            assert obra is None and "não confere com nenhuma obra cadastrada" in recusa
            assert "obra A" not in recusa and "obra B" not in recusa and " km" not in recusa

    def test_obra_que_analisa_aceita_fora(self):
        obra, recusa, analise = _lugar(-3.7600, -38.5270, enviada=A, modo="ANALISAR")
        assert obra is A and recusa is None

    def test_borda_aceita_para_conferencia(self):
        obra, recusa, analise = _lugar(-3.72985, -38.5270, precisao=80, enviada=A)
        assert obra is A and recusa is None and "na borda da cerca" in analise

    def test_obra_sem_coordenada_nao_bloqueia(self):
        obra, recusa, _ = _lugar(-3.9, -38.9, enviada=SEM)
        assert obra is SEM and recusa is None
        # o ponto da obra, não: ele só bate na obra que a localização identifica (10/10/2026)
        obra, recusa, _ = _lugar(-3.9, -38.9, enviada=SEM, no_tablet=True)
        assert obra is None and "não tem coordenada cadastrada" in recusa

    def test_sem_localizacao_o_celular_explica_e_o_ponto_da_obra_nao_bate(self):
        """Decisão do dono, 09/10/2026: o ponto da obra só bate com a localização;
        o celular da pessoa bate sem ela escolhendo a obra e explicando."""
        obra, recusa, _ = _lugar(None, None, enviada=A)
        assert obra is None and "ligue a localização" in recusa
        obra, recusa, _ = _lugar(None, None, enviada=A, porque="o GPS do celular não pega aqui")
        assert obra is A and recusa is None
        obra, recusa, _ = _lugar(None, None, enviada=A, no_tablet=True, porque="qualquer coisa")
        assert obra is None and "ponto da obra só bate com a localização" in recusa

    def test_fora_da_obra_explicando_vai_para_conferencia(self):
        """Decisão do dono, 09/10/2026: "não tá na obra, alerta; e se a pessoa ainda
        for bater, explicar o motivo e o ponto ir para conferência"."""
        obra, recusa, analise = _lugar(-3.7600, -38.5270, enviada=A, porque="comprando material")
        assert obra is A and recusa is None and "fora da área da obra" in analise and "explicando" in analise
        obra, recusa, _ = _lugar(-3.7600, -38.5270, enviada=A, porque="   ")
        assert obra is None and "explique o motivo" in recusa
        # o ponto da obra fora da cerca não ganha essa saída: o aparelho saiu da obra
        obra, recusa, _ = _lugar(-3.7600, -38.5270, enviada=A, no_tablet=True, porque="x")
        assert obra is None and "explique" not in recusa

    def test_sem_obra_e_sem_localizacao(self):
        obra, recusa, _ = _lugar(None, None, enviada=None)
        assert obra is None and recusa


class TestPeriodoDeContrato:
    def test_antes_do_inicio_e_depois_da_saida(self):
        import datetime as dt
        p = {"admissao": dt.date(2026, 10, 5), "demissao": dt.date(2026, 10, 20)}
        assert "antes da data de início (05/10/2026)" in marcacoes.fora_do_contrato(p, dt.date(2026, 10, 4))
        assert marcacoes.fora_do_contrato(p, dt.date(2026, 10, 5)) is None
        assert marcacoes.fora_do_contrato(p, dt.date(2026, 10, 20)) is None      # o dia da saída se bate
        assert "depois da data de saída" in marcacoes.fora_do_contrato(p, dt.date(2026, 10, 21))
        assert marcacoes.fora_do_contrato({}, dt.date(2026, 10, 21)) is None


class TestCopiaDoRegistro:
    def test_de_duas_em_duas_horas_e_so_de_dia(self):
        import datetime as dt
        from app.apps.ponto import horario
        from app.apps.ponto.core import registro
        dez = dt.datetime(2026, 10, 5, 10, 0, tzinfo=horario.FUSO)
        assert registro.precisa_copiar(None, dez) is True
        assert registro.precisa_copiar({"inicio": dez - dt.timedelta(hours=1), "fim": dez}, dez) is False
        assert registro.precisa_copiar({"inicio": dez - dt.timedelta(hours=3), "fim": dez}, dez) is True
        assert registro.precisa_copiar({"inicio": dez - dt.timedelta(hours=3), "fim": None}, dez) is False
        assert registro.precisa_copiar(None, dez.replace(hour=22)) is False
