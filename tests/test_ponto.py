# -*- coding: utf-8 -*-
"""Ponto eletrônico — as regras puras, sem banco.

Dia de trabalho por jornada, cerca da obra, autorização do aparelho por perfil,
a decisão VALIDA/EM_ANALISE, o hash encadeado, a leitura de planilha e o guarda
de credencial. Rodam em menos de um segundo."""
from __future__ import annotations

import base64
import datetime as dt
import io

import pytest

from app.apps.ponto import auth, horario
from app.apps.ponto.core import dispositivos, fotos, geo, importacao, marcacoes
from app.apps.ponto.erros import ErroDeValidacao

FUSO = horario.FUSO


def local(ano, mes, dia, hora, minuto=0):
    return dt.datetime(ano, mes, dia, hora, minuto, tzinfo=FUSO)


# ---------------------------------------------------------------------------
# Dia de trabalho
# ---------------------------------------------------------------------------
class TestDataReferencia:
    def test_padrao_e_o_dia_da_batida(self):
        assert horario.data_referencia(local(2026, 10, 3, 7, 58), "PADRAO_4") == dt.date(2026, 10, 3)
        assert horario.data_referencia(local(2026, 10, 3, 23, 59), "PADRAO_4") == dt.date(2026, 10, 3)

    def test_vigia_diurno_e_o_dia_da_batida(self):
        assert horario.data_referencia(local(2026, 10, 3, 6), "VIGIA_DIURNO_2") == dt.date(2026, 10, 3)

    def test_vigia_noturno_antes_das_10h_e_do_dia_anterior(self):
        assert horario.data_referencia(local(2026, 10, 4, 6, 5), "VIGIA_NOTURNO_2") == dt.date(2026, 10, 3)
        assert horario.data_referencia(local(2026, 10, 4, 9, 59), "VIGIA_NOTURNO_2") == dt.date(2026, 10, 3)
        assert horario.data_referencia(local(2026, 10, 4, 0, 0), "VIGIA_NOTURNO_2") == dt.date(2026, 10, 3)

    def test_vigia_noturno_as_10h_ja_e_o_proprio_dia(self):
        assert horario.data_referencia(local(2026, 10, 4, 10, 0), "VIGIA_NOTURNO_2") == dt.date(2026, 10, 4)
        assert horario.data_referencia(local(2026, 10, 4, 18, 0), "VIGIA_NOTURNO_2") == dt.date(2026, 10, 4)

    def test_virada_de_mes_e_de_ano(self):
        assert horario.data_referencia(local(2026, 11, 1, 5), "VIGIA_NOTURNO_2") == dt.date(2026, 10, 31)
        assert horario.data_referencia(local(2027, 1, 1, 2), "VIGIA_NOTURNO_2") == dt.date(2026, 12, 31)

    def test_instante_em_utc_e_convertido_antes_de_decidir(self):
        # 02:30 UTC = 23:30 do dia anterior em Fortaleza.
        utc = dt.datetime(2026, 10, 4, 2, 30, tzinfo=dt.timezone.utc)
        assert horario.data_referencia(utc, "PADRAO_4") == dt.date(2026, 10, 3)
        assert horario.texto(utc) == "2026-10-03T23:30:00-03:00"

    def test_ler_iso_sem_fuso_e_hora_local(self):
        m = horario.ler_iso("2026-10-03T07:58:00")
        assert m.utcoffset() == dt.timedelta(hours=-3)
        assert horario.ler_iso("2026-10-03T10:58:00Z").astimezone(FUSO).hour == 7
        assert horario.ler_iso("") is None
        with pytest.raises(ValueError):
            horario.ler_iso("ontem de manhã")


# ---------------------------------------------------------------------------
# Cerca
# ---------------------------------------------------------------------------
class TestCerca:
    # Praça do Ferreira (Fortaleza) e um ponto ~111 m ao norte.
    OBRA = (-3.7275, -38.5270)

    def test_distancia_de_um_grau_de_latitude(self):
        d = geo.distancia_metros(0, 0, 1, 0)
        assert 111_000 < d < 111_300

    def test_dentro_e_fora_do_raio(self):
        dentro, dist, motivo = geo.avaliar_cerca(-3.7280, -38.5270, *self.OBRA, 200)
        assert dentro is True and 50 < dist < 60 and motivo is None
        dentro, dist, motivo = geo.avaliar_cerca(-3.7300, -38.5270, *self.OBRA, 200)
        assert dentro is False and 270 < dist < 285
        assert "fora da cerca" in motivo and "raio 200 m" in motivo

    def test_obra_sem_coordenada_nao_decide(self):
        dentro, dist, motivo = geo.avaliar_cerca(-3.7, -38.5, None, None, 200)
        assert dentro is None and dist is None and "sem coordenada" in motivo

    def test_batida_sem_localizacao_nao_decide(self):
        dentro, _, motivo = geo.avaliar_cerca(None, None, *self.OBRA, 200)
        assert dentro is None and "sem localização" in motivo

    def test_zero_zero_nao_e_coordenada(self):
        assert not geo.coordenada_valida(0, 0)
        assert not geo.coordenada_valida("abc", 1)
        assert geo.coordenada_valida("-3.7", "-38.5")


# ---------------------------------------------------------------------------
# Aparelho por perfil
# ---------------------------------------------------------------------------
class TestAutorizacaoDoAparelho:
    def aparelho(self, **extra):
        base = {"status": "APROVADO", "perfil": "COMPARTILHADO", "colaborador_id": None}
        base.update(extra)
        return base

    def test_pendente_e_bloqueado_recusam(self):
        assert "pendente" in dispositivos.autorizado_para(self.aparelho(status="PENDENTE"), 1, 10, set(), set())
        assert "bloqueado" in dispositivos.autorizado_para(self.aparelho(status="BLOQUEADO"), 1, 10, set(), set())

    def test_compartilhado_aceita_qualquer_pessoa(self):
        assert dispositivos.autorizado_para(self.aparelho(), 1, 10, set(), set()) is None
        assert dispositivos.autorizado_para(self.aparelho(), 99, 10, set(), set()) is None

    def test_individual_so_o_dono(self):
        a = self.aparelho(perfil="INDIVIDUAL", colaborador_id=7)
        assert dispositivos.autorizado_para(a, 7, 10, set(), set()) is None
        assert "outra pessoa" in dispositivos.autorizado_para(a, 8, 10, set(), set())

    def test_lista_so_quem_esta_nela(self):
        a = self.aparelho(perfil="LISTA")
        assert dispositivos.autorizado_para(a, 7, 10, {7, 8}, set()) is None
        assert "fora da lista" in dispositivos.autorizado_para(a, 9, 10, {7, 8}, set())

    def test_obras_do_aparelho_vazio_e_todas(self):
        assert dispositivos.autorizado_para(self.aparelho(), 1, 10, set(), set()) is None
        assert dispositivos.autorizado_para(self.aparelho(), 1, 10, set(), {10, 11}) is None
        assert "não vale nesta obra" in dispositivos.autorizado_para(self.aparelho(), 1, 12, set(), {10, 11})

    def test_uuid_valido(self):
        assert dispositivos.validar_uuid("3f2504e0-4f89-11d3-9a0c-0305e82c3301")
        for ruim in ("", "curto", "x" * 65, "tem espaço aqui dentro!!", None):
            with pytest.raises(ErroDeValidacao):
                dispositivos.validar_uuid(ruim)


# ---------------------------------------------------------------------------
# A decisão da batida
# ---------------------------------------------------------------------------
class TestDecisao:
    def decidir(self, **kw):
        base = dict(origem="PWA", dentro_da_cerca=True, motivo_cerca=None, diferenca_relogio_s=0,
                    situacao_pessoa="ATIVO", obra_na_lista_da_pessoa=True)
        base.update(kw)
        return marcacoes.decidir(**base)

    def test_tudo_certo_e_valida(self):
        assert self.decidir() == ("VALIDA", [])

    def test_fora_da_cerca_vai_para_analise_e_nao_e_recusada(self):
        status, motivos = self.decidir(dentro_da_cerca=False, motivo_cerca="fora da cerca: 300 m")
        assert status == "EM_ANALISE" and motivos == ["fora da cerca: 300 m"]

    def test_celular_sem_localizacao_vai_para_analise(self):
        status, motivos = self.decidir(dentro_da_cerca=None, motivo_cerca="batida sem localização")
        assert status == "EM_ANALISE"

    def test_idface_e_manual_sem_coordenada_sao_validas(self):
        for origem in ("IDFACE", "MANUAL"):
            status, _ = self.decidir(origem=origem, dentro_da_cerca=None,
                                     motivo_cerca="obra sem coordenada cadastrada")
            assert status == "VALIDA"

    def test_relogio_fora_da_tolerancia(self):
        assert self.decidir(diferenca_relogio_s=299)[0] == "VALIDA"
        status, motivos = self.decidir(diferenca_relogio_s=-301)
        assert status == "EM_ANALISE" and "relógio" in motivos[0]

    def test_pessoa_afastada_e_obra_fora_da_lista_acumulam_motivos(self):
        status, motivos = self.decidir(situacao_pessoa="AFASTADO", obra_na_lista_da_pessoa=False)
        assert status == "EM_ANALISE"
        assert motivos == ["pessoa afastada no cadastro", "obra fora da lista da pessoa"]

    def test_hash_encadeado_muda_com_qualquer_campo(self):
        m = dt.datetime(2026, 10, 3, 11, 0, tzinfo=dt.timezone.utc)
        h1 = marcacoes.hash_da_marcacao("0" * 64, 1, 7, 10, m, "PWA")
        assert h1 != marcacoes.hash_da_marcacao("0" * 64, 2, 7, 10, m, "PWA")
        assert h1 != marcacoes.hash_da_marcacao("a" * 64, 1, 7, 10, m, "PWA")
        assert h1 != marcacoes.hash_da_marcacao("0" * 64, 1, 7, 10, m + dt.timedelta(seconds=1), "PWA")
        assert h1 == marcacoes.hash_da_marcacao("0" * 64, 1, 7, 10, m.astimezone(FUSO), "PWA")

    def test_origem_desconhecida(self):
        assert marcacoes.validar_origem("idface") == "IDFACE"
        assert marcacoes.validar_origem(None) == "PWA"
        with pytest.raises(ErroDeValidacao):
            marcacoes.validar_origem("PAPEL")


# ---------------------------------------------------------------------------
# Foto
# ---------------------------------------------------------------------------
def _imagem_png(largura=1600, altura=1200) -> bytes:
    from PIL import Image
    saida = io.BytesIO()
    Image.new("RGB", (largura, altura), (120, 80, 40)).save(saida, format="PNG")
    return saida.getvalue()


class TestFoto:
    def test_reduz_e_vira_jpeg(self):
        dados, w, h = fotos.reduzir(_imagem_png())
        assert (w, h) == (800, 600)
        assert dados[:3] == b"\xff\xd8\xff" and len(dados) <= fotos.MAX_GUARDADO_BYTES

    def test_aceita_data_url(self):
        b64 = "data:image/png;base64," + base64.b64encode(_imagem_png(10, 10)).decode()
        assert fotos.decodificar_base64(b64) == _imagem_png(10, 10)

    def test_preparar_devolve_hash_do_que_sera_guardado(self):
        pronta = fotos.preparar(base64.b64encode(_imagem_png(900, 300)).decode())
        assert (pronta.largura, pronta.altura) == (800, 267)
        assert pronta.hash == fotos.sha256(pronta.dados) and len(pronta.hash) == 64

    def test_nome_do_arquivo_nao_leva_cpf(self):
        m = dt.datetime(2026, 10, 3, 11, 5, 9, tzinfo=dt.timezone.utc)
        assert fotos.nome_do_arquivo(m, 42, 7) == "2026-10-03_080509_colab42_nsr7.jpg"

    def test_sem_pasta_configurada_o_drive_nao_esta_configurado(self, monkeypatch):
        monkeypatch.delenv(fotos.VARIAVEL_PASTA, raising=False)
        assert fotos.drive_configurado() is False

    def test_recusa_lixo(self):
        with pytest.raises(ErroDeValidacao):
            fotos.decodificar_base64("isto não é base64!!")
        with pytest.raises(ErroDeValidacao):
            fotos.reduzir(b"nao sou imagem")
        with pytest.raises(ErroDeValidacao):
            fotos.decodificar_base64("")


# ---------------------------------------------------------------------------
# Planilha
# ---------------------------------------------------------------------------
class TestPlanilha:
    def test_cabecalhos_com_acento_caixa_e_espaco(self):
        mapa = importacao.mapear_colunas(["CPF", "Nome do Colaborador", "Obra", "Centro de Custo",
                                          "Tipo de Jornada", "coluna que ninguém pediu"])
        assert mapa == {"cpf": 0, "nome": 1, "obra": 2, "centro_custo": 3, "tipo_jornada": 4}

    def test_csv_com_ponto_e_virgula_e_linhas_vazias(self):
        linhas = importacao.ler_csv("cpf;nome;obra\n123;Fulano;OB-1\n;;\n456;Beltrano;OB-2\n")
        registros = importacao.montar_registros(linhas)
        assert [r["linha"] for r in registros] == [2, 4]
        assert registros[1] == {"linha": 4, "cpf": "456", "nome": "Beltrano", "obra": "OB-2"}

    def test_csv_com_virgula(self):
        linhas = importacao.ler_csv("codigo,nome,latitude,longitude\nOB-1,Escola,-3.7,-38.5\n")
        r = importacao.montar_registros(linhas)[0]
        assert r["codigo"] == "OB-1" and r["latitude"] == "-3.7"

    def test_sem_coluna_conhecida_e_erro(self):
        with pytest.raises(ErroDeValidacao):
            importacao.montar_registros([["a", "b"], ["1", "2"]])

    def test_numero_vindo_do_excel_perde_o_ponto_zero(self):
        assert importacao._valor(12345678901.0) == "12345678901"
        assert importacao._valor(None) == ""

    def test_apelidos_de_jornada(self):
        assert importacao.jornada_de("") == "PADRAO_4"
        assert importacao.jornada_de("Vigia Noturno") == "VIGIA_NOTURNO_2"
        assert importacao.jornada_de("vigia_diurno_2") == "VIGIA_DIURNO_2"
        with pytest.raises(ErroDeValidacao):
            importacao.jornada_de("meio período")

    def test_xlsx_de_verdade(self, tmp_path):
        from openpyxl import Workbook
        livro = Workbook()
        aba = livro.active
        aba.append(["Código", "Nome", "Latitude", "Longitude", "Raio"])
        aba.append(["OB-7", "Creche", -3.71, -38.52, 150])
        aba.append([None, None, None, None, None])
        caminho = tmp_path / "obras.xlsx"
        livro.save(caminho)
        registros = importacao.ler_tabela(str(caminho))
        assert registros == [{"linha": 2, "codigo": "OB-7", "nome": "Creche", "latitude": "-3.71",
                              "longitude": "-38.52", "raio_metros": "150"}]

    def test_extensao_desconhecida(self, tmp_path):
        arquivo = tmp_path / "obras.pdf"
        arquivo.write_bytes(b"%PDF")
        with pytest.raises(ErroDeValidacao):
            importacao.ler_tabela(str(arquivo))


# ---------------------------------------------------------------------------
# Credenciais
# ---------------------------------------------------------------------------
class TestCredenciais:
    def test_confere_em_bytes_aceita_acento(self):
        assert auth.confere("segredo-ção", "segredo-ção")
        assert not auth.confere("outro", "segredo-ção")
        assert not auth.confere(None, "x") and not auth.confere("x", "")

    def test_token_e_hash(self):
        t = auth.gerar_token()
        assert len(t) >= 40 and auth.confere_hash(t, auth.hash_token(t))
        assert not auth.confere_hash(t + "x", auth.hash_token(t))
        assert not auth.confere_hash(None, auth.hash_token(t))

    def test_teto_de_registros_por_ip(self):
        auth._registros.clear()
        base = 1_000_000.0
        for i in range(auth.REGISTROS_POR_HORA_POR_IP):
            assert auth.registro_permitido("10.0.0.1", base + i)
        assert not auth.registro_permitido("10.0.0.1", base + 100)
        assert auth.registro_permitido("10.0.0.2", base + 100)
        assert auth.registro_permitido("10.0.0.1", base + 3601)   # a janela passou
        auth._registros.clear()

    def test_rota_sem_declaracao_e_recusada(self, monkeypatch):
        """O guarda fecha a rota que esqueceu de declarar — e deixa passar a
        que declarou. Um blueprint de mentira, porque o de verdade já está
        registrado e o Flask não aceita rota nova nele depois disso."""
        from flask import Blueprint, Flask
        monkeypatch.setenv(auth.VARIAVEL_CHAVE, "chave-de-teste")
        falso = Blueprint("ponto_teste", __name__, url_prefix="/t")
        falso.before_request(auth.exigir_credencial)
        falso.add_url_rule("/esqueci", "esqueci", lambda: "aberta")
        falso.add_url_rule("/declarei", "declarei", auth.exige_chave(lambda: "ok"))
        a = Flask(__name__)
        a.register_blueprint(falso)
        c = a.test_client()
        cabecalho = {"X-API-Key": "chave-de-teste"}
        resposta = c.get("/t/esqueci", headers=cabecalho)
        assert resposta.status_code == 403 and resposta.get_json()["ok"] is False
        assert c.get("/t/declarei", headers=cabecalho).status_code == 200

    def test_sem_chave_configurada_fecha(self, monkeypatch):
        from flask import Flask
        from app.apps.ponto import routes
        monkeypatch.delenv(auth.VARIAVEL_CHAVE, raising=False)
        a = Flask(__name__)
        a.register_blueprint(routes.bp)
        c = a.test_client()
        assert c.get("/ponto/api/obras").status_code == 503
        assert c.get("/ponto/api/obras", headers={"X-API-Key": "qualquer"}).status_code == 503

    def test_chave_errada_e_401_e_publica_segue(self, monkeypatch):
        from flask import Flask
        from app.apps.ponto import routes
        monkeypatch.setenv(auth.VARIAVEL_CHAVE, "certa")
        a = Flask(__name__)
        a.register_blueprint(routes.bp)
        c = a.test_client()
        assert c.get("/ponto/api/obras").status_code == 401
        assert c.get("/ponto/api/obras", headers={"X-API-Key": "errada"}).status_code == 401
        saude = c.get("/ponto/health")
        assert saude.status_code == 200 and saude.get_json()["chave_configurada"] is True
        assert saude.get_json()["fuso"] == "America/Fortaleza"
