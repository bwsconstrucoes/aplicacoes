# -*- coding: utf-8 -*-
"""Ponto, fase 2 — as regras puras: escalas, etapas de aprovação, PIN,
competência, documentos e o encaixe com as permissões do ERP. Sem banco."""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.ponto.core import (acesso, alertas, banco, competencias, documentos, escalas,
                                 espelho, ocorrencias)
from app.apps.ponto.erros import ErroDeValidacao


class TestEscalas:
    def test_semana_normal_e_conversao(self):
        s = escalas.validar_semana({"0": [["07:00", "11:00"], ["12:00", "17:00"]], "4": [["07:00", "16:00"]]})
        assert s == {"0": [["07:00", "11:00"], ["12:00", "17:00"]], "4": [["07:00", "16:00"]]}
        e = escalas.para_apuracao({"tipo": "SEMANAL", "semana": s})
        assert e.periodos(dt.date(2026, 10, 5)) == [(420, 660), (720, 1020)]     # segunda
        assert e.periodos(dt.date(2026, 10, 6)) == []                             # terça: descanso

    def test_turno_da_noite_atravessa_a_meia_noite(self):
        e = escalas.para_apuracao({"tipo": "SEMANAL", "semana": {"0": [["22:00", "06:00"]]}})
        assert e.periodos(dt.date(2026, 10, 5)) == [(1320, 1800)]

    def test_horario_ruim_e_sobreposicao_sao_recusados(self):
        with pytest.raises(ErroDeValidacao):
            escalas.validar_semana({"0": [["7h", "11:00"]]})
        with pytest.raises(ErroDeValidacao):
            escalas.validar_semana({"0": [["07:00", "12:00"], ["11:00", "17:00"]]})
        with pytest.raises(ErroDeValidacao):
            escalas.validar_semana({"9": [["07:00", "11:00"]]})
        with pytest.raises(ErroDeValidacao):
            escalas.validar_semana({"0": [["07:00", "23:30"]]})   # mais de 16 h

    def test_horas_semanais(self):
        obra = {"tipo": "SEMANAL", "semana": {str(d): [["07:00", "11:00"], ["12:00", "17:00"]] for d in range(4)}
                | {"4": [["07:00", "11:00"], ["12:00", "16:00"]]}}
        assert escalas.horas_semanais(obra) == 44.0
        assert escalas.horas_semanais({"tipo": "CICLO_12X36", "ciclo_entrada": "19:00",
                                       "ciclo_saida": "07:00"}) == 42.0


class TestEtapas:
    def test_cada_tipo_tem_caminho_e_efeito_conhecido(self):
        assert ocorrencias.ETAPAS["ATESTADO"] == ("DP",)
        # o PADRÃO é tudo no DP (04/10/2026); a regra em vigor é a da tela (validacao.py)
        assert ocorrencias.ETAPAS["AJUSTE_BATIDA"] == ("DP",)
        assert ocorrencias.ETAPAS["COMPENSACAO"] == ("DP",)
        assert set(espelho.ABONO_POR_TIPO) <= set(ocorrencias.ETAPAS)
        assert set(espelho.ROTULO_TIPO) == set(ocorrencias.ETAPAS)

    def test_ferias_nao_se_pede_pelo_celular(self):
        assert "FERIAS" not in ocorrencias.PEDIDOS_DO_APP and "ABONO" not in ocorrencias.PEDIDOS_DO_APP

    def test_saude_e_sigilosa(self):
        quem = ocorrencias.Quem(nome="Supervisor", supervisor=True, dp=False)
        bruta = {"id": 1, "tipo": "ATESTADO", "colaborador_id": 7, "colaborador_nome": "João",
                 "cpf": "52998224725", "obra_codigo": "A", "obra_principal_codigo": "A",
                 "data_inicio": dt.date(2026, 10, 1), "data_fim": dt.date(2026, 10, 2),
                 "horario": None, "dia_trabalhado": None, "descricao": "", "cid": "J11",
                 "medico": "Dra. X", "crm": "123", "documento_id": 9, "leitura_ia": {"cid": "J11"},
                 "status": "AGUARDANDO_DP", "origem": "APP", "solicitado_por": "João",
                 "criado_em": dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)}
        j = ocorrencias._para_json(bruta, quem)
        assert j["cid"] is None and j["medico"] is None and j["documento_id"] is None
        assert j["leitura_ia"] is None and j["tem_documento"] is True and j["sigiloso"] is True
        dp = ocorrencias.Quem(nome="DP", dp=True)
        assert ocorrencias._para_json(bruta, dp)["cid"] == "J11"
        o_proprio = ocorrencias.Quem(nome="João", colaborador_id=7)
        assert ocorrencias._para_json(bruta, o_proprio)["documento_id"] == 9


class TestAcesso:
    def test_pin_facil_e_recusado(self):
        for ruim in ("123456", "000000", "111111", "12345", "abcdef"):
            with pytest.raises(ErroDeValidacao):
                acesso.validar_pin(ruim)
        assert acesso.validar_pin("481 927") == "481927"

    def test_hash_com_sal(self):
        h1, h2 = acesso._hash("481927"), acesso._hash("481927")
        assert h1 != h2 and acesso._confere("481927", h1) and not acesso._confere("481928", h1)
        assert not acesso._confere("481927", None)


class TestCompetenciaEDocumento:
    def test_datas_da_competencia(self):
        assert competencias.primeiro_dia("2026-02") == dt.date(2026, 2, 1)
        assert competencias.ultimo_dia(dt.date(2026, 2, 1)) == dt.date(2026, 2, 28)
        assert competencias.ultimo_dia(dt.date(2028, 2, 1)) == dt.date(2028, 2, 29)
        with pytest.raises(ErroDeValidacao):
            competencias.primeiro_dia("fevereiro")

    def test_tipo_do_documento_pelo_conteudo(self):
        assert documentos._tipo_pelo_conteudo(b"%PDF-1.7 ...") == "application/pdf"
        assert documentos._tipo_pelo_conteudo(b"\xff\xd8\xff\xe0....") == "image/jpeg"
        assert documentos._tipo_pelo_conteudo(b"MZ\x90\x00 executavel") is None

    def test_somar_meses_do_banco(self):
        assert banco._somar_meses(dt.date(2026, 11, 1), 3) == dt.date(2027, 2, 1)
        assert banco.PRAZO_MESES == {"BANCO_6_MESES": 6, "BANCO_12_MESES": 12}


class TestAlertas:
    def test_todo_codigo_tem_gravidade_e_rotulo(self):
        assert set(alertas.GRAVIDADE) == set(alertas.ROTULO)
        assert alertas.DO_DIA <= set(alertas.GRAVIDADE)

    def test_horas_em_texto(self):
        assert alertas._hhmm(545) == "9h05" and alertas._hhmm(-90) == "-1h30"


class TestEncaixeNoErp:
    def test_acoes_do_ponto_tem_nome_e_secao(self):
        from app.apps.erp.core.auth import secoes
        from app.apps.erp.core.auth.permissoes import ACAO_ROTULOS, PERMISSOES
        acoes = {"ver_ponto", "tratar_ponto", "aprovar_afastamento", "fechar_competencia",
                 "configurar_ponto"}
        assert acoes <= set(PERMISSOES) and acoes <= set(ACAO_ROTULOS)
        cobertas = {a for s in secoes.SECOES for a in s["ler"] + s["editar"]}
        assert acoes <= cobertas

    def test_o_menu_ganhou_o_modulo_e_toda_aba_exige_ver_ponto(self):
        from app.apps.erp import routes as R
        from app.apps.ponto import gestao
        assert any(m["chave"] == "ponto" for m in R.MODULOS)
        for _, _, endpoint in gestao.ABAS:
            assert R._REGISTRO_PERMISSOES[endpoint]["GET"] == "ver_ponto"

    def test_o_atestado_tem_rota_propria_do_dp(self):
        from app.apps.erp import routes as R
        mapa = R._REGISTRO_PERMISSOES
        assert mapa["erp.ponto_api_documento_sigiloso"]["GET"] == "aprovar_afastamento"
        assert mapa["erp.ponto_api_decidir_dp"]["POST"] == "aprovar_afastamento"
        assert mapa["erp.ponto_api_decidir_supervisor"]["POST"] == "tratar_ponto"
        assert mapa["erp.ponto_api_criar_afastamento"]["POST"] == "aprovar_afastamento"
        assert mapa["erp.ponto_api_competencia_fechar"]["POST"] == "fechar_competencia"
