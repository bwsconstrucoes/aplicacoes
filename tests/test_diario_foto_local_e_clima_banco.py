# -*- coding: utf-8 -*-
"""
A FOTO SABE ONDE FOI TIRADA, E O CLIMA É MEDIDO (migração 127).

Ele, em 24/11/2026, olhando o diário de obra:

    *"Se eu precisar que as minhas fotos tenham geolocalização e que na
     legenda dessa foto apareça essa geolocalização e ainda que fique um
     carimbo da foto com a data e geolocalização que ela foi retirada."*

E, sobre o clima: *"você disse que ia trazer baseado no registro do tempo (…)
e eu não vi em canto nenhum no diário. Isso aqui ficou feito?"* Não tinha
ficado — a coluna existia desde 08/11 e nunca foi preenchida.

⚠️ O DEFEITO DE BASE QUE ISTO FECHA: para caber no PDF, a foto era reduzida e
REGRAVADA, e regravar apaga o EXIF — onde o celular grava a hora e o GPS. A
informação chegava e era destruída no envio. **O teste que guarda isso é o
`test_o_local_sobrevive_ao_envio`**: se alguém inverter a ordem outra vez,
ele acusa.

⚑ NENHUM TESTE AQUI ENCOSTA NA INTERNET. O serviço de clima é trocado por um
dublê — o que se prova é a regra, não o Open-Meteo.
"""
from __future__ import annotations

import io
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import text

from app.apps.erp.core import clima as svc_clima
from app.apps.erp.core import diario as svc
from app.apps.erp.core import diario_fotos as fotos

pytestmark = pytest.mark.banco


# ---------------------------------------------------------------------------
# Cenário
# ---------------------------------------------------------------------------
def _ontem():
    return date.today() - timedelta(days=1)


def _jpeg(tamanho=(800, 600)) -> bytes:
    """Uma foto sem nada dentro — como a que sai de um computador."""
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", tamanho, (120, 150, 170)).save(buf, format="JPEG")
    return buf.getvalue()


def _jpeg_com_exif(lat=-3.7319, lon=-38.5267, quando=None,
                   precisao=None, tamanho=(800, 600)) -> bytes:
    """JPEG com GPS e hora no EXIF — como a que sai de um celular.

    Montado com o próprio Pillow para o teste não ganhar dependência nova.
    O EXIF guarda ângulo em grau/minuto/segundo, cada um uma fração.
    """
    from PIL import Image
    from PIL.TiffImagePlugin import IFDRational

    im = Image.new("RGB", tamanho, (120, 150, 170))
    exif = im.getexif()
    if quando:
        # 36867 = DateTimeOriginal, dentro do bloco de detalhes (0x8769)
        exif.get_ifd(0x8769)[36867] = quando.strftime("%Y:%m:%d %H:%M:%S")
    gps = exif.get_ifd(0x8825)

    def _dms(valor):
        valor = abs(valor)
        g = int(valor)
        m = int((valor - g) * 60)
        s = (valor - g - m / 60) * 3600
        return (IFDRational(g), IFDRational(m),
                IFDRational(int(round(s * 10000)), 10000))

    gps[1] = "S" if lat < 0 else "N"
    gps[2] = _dms(lat)
    gps[3] = "W" if lon < 0 else "E"
    gps[4] = _dms(lon)
    if precisao is not None:
        gps[11] = IFDRational(int(precisao))
    buf = io.BytesIO()
    im.save(buf, format="JPEG", exif=exif.tobytes())
    return buf.getvalue()


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Quem lança", email="foto.local@teste.bws",
                ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMINISTRATIVO_OBRA)
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.fixture
def obra(sessao_real):
    oid = sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome, status, fase, latitude, longitude,
                           data_inicio, data_termino)
        VALUES ('FOTOGPS', 'Creche Vila Nova', 'ATIVA', 'EM_EXECUCAO',
                -3.7319, -38.5267, :i, :f)
        RETURNING id"""),
        {"i": date.today() - timedelta(days=200),
         "f": date.today() + timedelta(days=200)}).scalar()
    sessao_real.flush()
    return oid


@pytest.fixture
def dia(sessao_real, obra, dono):
    quando = _ontem()
    svc.salvar(sessao_real, obra, quando, {"relato": "Concretagem."}, dono)
    sessao_real.flush()
    return quando


def _anexo(s, anexo_id):
    return s.execute(text("""
        SELECT tirada_em, latitude, longitude, precisao_m, origem_local
          FROM anexos WHERE id = :i"""), {"i": anexo_id}).mappings().first()


# ---------------------------------------------------------------------------
# 1. O LOCAL SOBREVIVE AO ENVIO — o teste que guarda o defeito de base
# ---------------------------------------------------------------------------
def test_o_local_sobrevive_ao_envio(sessao_real, obra, dia, dono):
    """⚠️ SE ALGUÉM INVERTER A ORDEM (reduzir antes de ler), este teste cai.

    Foi exatamente o que aconteceu de 14 a 24/11/2026: a foto era regravada
    para caber no PDF, o EXIF ia junto, e o GPS do celular se perdia sem
    ninguém notar.
    """
    tirada = datetime.combine(dia, datetime.min.time()).replace(hour=14,
                                                                minute=37)
    r = fotos.anexar(sessao_real, obra, dia,
                     _jpeg_com_exif(quando=tirada, precisao=8),
                     "laje.jpg", legenda="Laje do bloco B", usuario=dono)
    sessao_real.flush()

    guardado = _anexo(sessao_real, r["id"])
    assert guardado["origem_local"] == "EXIF"
    assert round(float(guardado["latitude"]), 3) == -3.732
    assert round(float(guardado["longitude"]), 3) == -38.527
    assert guardado["precisao_m"] == 8
    assert guardado["tirada_em"].hour == 14


def test_a_foto_ganha_do_aparelho(sessao_real, obra, dia, dono):
    """A foto traz o local de ONDE FOI TIRADA; o aparelho traz o de onde está
    quem envia. Foto da galeria mandada três dias depois tem o local certo
    dentro dela — a posição do celular seria a do escritório."""
    r = fotos.anexar(sessao_real, obra, dia, _jpeg_com_exif(),
                     "laje.jpg", usuario=dono,
                     local_do_aparelho={"latitude": -23.5, "longitude": -46.6})
    sessao_real.flush()
    guardado = _anexo(sessao_real, r["id"])
    assert guardado["origem_local"] == "EXIF"
    assert round(float(guardado["latitude"]), 1) == -3.7


def test_sem_exif_vale_o_aparelho_e_fica_marcado(sessao_real, obra, dia, dono):
    r = fotos.anexar(sessao_real, obra, dia, _jpeg(), "sem.jpg", usuario=dono,
                     local_do_aparelho={"latitude": -3.7319,
                                        "longitude": -38.5267,
                                        "precisao_m": 12})
    sessao_real.flush()
    guardado = _anexo(sessao_real, r["id"])
    assert guardado["origem_local"] == "APARELHO"
    assert guardado["precisao_m"] == 12


def test_foto_antiga_sem_nada_entra_igual(sessao_real, obra, dia, dono):
    """Foto sem localização é foto do mesmo jeito. Ela só não leva carimbo."""
    r = fotos.anexar(sessao_real, obra, dia, _jpeg(), "antiga.jpg",
                     usuario=dono)
    sessao_real.flush()
    guardado = _anexo(sessao_real, r["id"])
    assert guardado["origem_local"] is None
    assert guardado["latitude"] is None


def test_coordenada_impossivel_do_aparelho_e_recusada(sessao_real, obra, dia,
                                                      dono):
    """Coordenada errada põe a obra no meio do oceano no mapa."""
    r = fotos.anexar(sessao_real, obra, dia, _jpeg(), "torta.jpg",
                     usuario=dono,
                     local_do_aparelho={"latitude": 999, "longitude": -38.5})
    sessao_real.flush()
    assert _anexo(sessao_real, r["id"])["origem_local"] is None


# ---------------------------------------------------------------------------
# 2. O CARIMBO
# ---------------------------------------------------------------------------
def test_o_carimbo_nao_apaga_a_foto(sessao_real):
    """Ele: *"o carimbo fica sobre a foto, mas ele não apaga os dados da
    foto (…) como se fosse uma marca d'água viva sobre a imagem."*

    Prova objetiva: a faixa do carimbo continua com VÁRIAS cores — se fosse
    tarja sólida, teria uma só.
    """
    from PIL import Image
    original = _jpeg_com_exif()
    saida = fotos.carimbar(original, quando=datetime(2026, 11, 24, 14, 37),
                           latitude=-3.7319, longitude=-38.5267,
                           precisao_m=8, obra="094 · CRECHE VILA NOVA")
    im = Image.open(io.BytesIO(saida))
    L, A = im.size
    faixa = im.crop((0, int(A * 0.90), L, A)).convert("RGB")
    assert len(faixa.getcolors(maxcolors=100000) or []) > 20, \
        "a faixa do carimbo ficou chapada — virou tarja, não véu"


def test_foto_sem_local_nao_e_carimbada(sessao_real):
    """⚠️ Carimbar sem local seria o sistema AFIRMANDO onde a foto foi
    tirada sem saber — num papel que a fiscalização assina."""
    original = _jpeg()
    assert fotos.carimbar(original, quando=datetime(2026, 11, 24),
                          latitude=None, longitude=None) == original


def test_a_coordenada_se_le_no_padrao_do_brasil():
    assert fotos.coordenada_por_extenso(-3.7319, -38.5267) \
        == "-3,731900, -38,526700"


def test_a_listagem_traz_o_local_pronto_para_ler(sessao_real, obra, dia, dono):
    fotos.anexar(sessao_real, obra, dia, _jpeg_com_exif(precisao=8),
                 "laje.jpg", usuario=dono)
    sessao_real.flush()
    lista = fotos.listar(sessao_real, fotos._diario(sessao_real, obra, dia)["id"])
    assert lista[0]["local"].startswith("-3,73")
    assert lista[0]["local_rotulo"] == "local gravado pela câmera"


# ---------------------------------------------------------------------------
# 3. O CLIMA MEDIDO
# ---------------------------------------------------------------------------
def _servico_falso(monkeypatch, resposta):
    monkeypatch.setattr(svc_clima, "consultar",
                        lambda lat, lon, dia: resposta)


def test_o_clima_e_medido_e_guardado(sessao_real, obra, dia, monkeypatch):
    medido = svc_clima._traduzir(
        {"daily": {"precipitation_sum": [12.4], "precipitation_hours": [3.0],
                   "temperature_2m_max": [31.2], "temperature_2m_min": [24.1],
                   "windspeed_10m_max": [18.0]}}, -3.7319, -38.5267, dia)
    _servico_falso(monkeypatch, medido)

    r = svc_clima.medir(sessao_real, obra, dia)
    sessao_real.flush()
    assert r["chuva_mm"] == 12.4
    assert r["resumo"] == "12,4 mm de chuva em 3 h"
    assert r["fonte"] == "open-meteo"
    guardado = sessao_real.execute(text(
        "SELECT clima_medido, clima_medido_em FROM diarios_obra WHERE data = :d"),
        {"d": dia}).mappings().first()
    assert guardado["clima_medido"]["chuva_mm"] == 12.4
    assert guardado["clima_medido_em"] is not None


def test_medido_uma_vez_nao_se_pergunta_de_novo(sessao_real, obra, dia,
                                                monkeypatch):
    """Uma consulta por obra e por dia, uma vez na vida. O serviço é externo
    e a cota não é infinita."""
    idas = {"n": 0}

    def _conta(lat, lon, d):
        idas["n"] += 1
        return svc_clima._traduzir(
            {"daily": {"precipitation_sum": [0.0],
                       "precipitation_hours": [0.0]}}, lat, lon, d)

    monkeypatch.setattr(svc_clima, "consultar", _conta)
    svc_clima.medir(sessao_real, obra, dia)
    sessao_real.flush()
    svc_clima.medir(sessao_real, obra, dia)
    sessao_real.flush()
    assert idas["n"] == 1


def test_nao_mede_o_futuro(sessao_real, obra, dono, monkeypatch):
    """Previsão não é prova."""
    amanha = date.today() + timedelta(days=1)
    svc.salvar(sessao_real, obra, amanha, {"relato": "a fazer"}, dono)
    sessao_real.flush()
    _servico_falso(monkeypatch, {"chuva_mm": 9.9})
    assert svc_clima.medir(sessao_real, obra, amanha) is None


def test_obra_sem_coordenada_nao_mede(sessao_real, dono, monkeypatch):
    """Chutar pelo município poria a medição a quilômetros do canteiro."""
    oid = sessao_real.execute(text("""
        INSERT INTO obras (codigo, nome, status, fase)
        VALUES ('SEMGPS', 'Obra sem coordenada', 'ATIVA', 'EM_EXECUCAO')
        RETURNING id""")).scalar()
    sessao_real.flush()
    svc.salvar(sessao_real, oid, _ontem(), {"relato": "x"}, dono)
    sessao_real.flush()
    _servico_falso(monkeypatch, {"chuva_mm": 9.9})
    assert svc_clima.medir(sessao_real, oid, _ontem()) is None


def test_servico_fora_do_ar_nao_derruba_nada(sessao_real, obra, dia,
                                             monkeypatch):
    _servico_falso(monkeypatch, None)
    assert svc_clima.medir(sessao_real, obra, dia) is None
    assert sessao_real.execute(text(
        "SELECT clima_medido FROM diarios_obra WHERE data = :d"),
        {"d": dia}).scalar() is None


def test_a_medicao_guarda_a_procedencia(sessao_real, dia):
    """⚑ Prova sem procedência não é prova — é o comentário que está na
    migração 092 desde que a coluna nasceu."""
    medido = svc_clima._traduzir(
        {"daily": {"precipitation_sum": [0.4], "precipitation_hours": [0.0]}},
        -3.7319, -38.5267, dia)
    assert medido["fonte"] == "open-meteo"
    assert medido["latitude"] == -3.7319 and medido["dia"] == dia.isoformat()
    assert medido["consultado_em"]
    assert medido["resumo"] == "sem chuva registrada"
