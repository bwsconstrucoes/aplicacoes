"""A busca automática de feriados (05/11/2026) — `ponto/core/feriados_busca.py`.

A internet é trocada por arquivos de mentira no formato da base pública (feriados-brasil):
o que se testa é o que vai para `ponto.feriados`, e quando a busca vai ou não vai buscar.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from tests._feriados_do_ponto import _schema_ponto2, feriados_ponto  # noqa: F401

pytestmark = pytest.mark.banco
D = dt.date
# Janeiro: o corte (1º do mês anterior) fica em dezembro, e todo 2026 entra.
HOJE = D(2026, 1, 10)

MUNICIPIOS = [
    {"codigo_ibge": 2609600, "nome": "Olinda"},
    {"codigo_ibge": 2611606, "nome": "Recife"},
    {"codigo_ibge": 2304400, "nome": "Fortaleza"},
    {"codigo_ibge": 2312908, "nome": "Sobral"},
]
ESTADUAL = {2026: [
    {"data": "06/03/2026", "nome": "Revolução Pernambucana", "uf": "PE"},
    {"data": "25/03/2026", "nome": "Data Magna do Ceará", "uf": "CE"},
    {"data": "19/10/2026", "nome": "Dia do Piauí", "uf": "PI"},
]}
MUNICIPAL = {2026: [
    {"data": "12/03/2026", "nome": "Aniversário de Olinda", "codigo_ibge": 2609600},
    {"data": "16/07/2026", "nome": "Nossa Senhora do Carmo", "codigo_ibge": 2611606},
    {"data": "15/08/2026", "nome": "Nossa Senhora da Assunção", "codigo_ibge": 2304400},
]}


@pytest.fixture
def internet(monkeypatch):
    """A base pública de mentira; `pedidos` guarda cada endereço pedido."""
    from app.apps.ponto.core import feriados_busca as fb
    pedidos = []

    def baixar(url):
        pedidos.append(url)
        if url == fb.URL_MUNICIPIOS:
            return MUNICIPIOS
        ano = int(url.rsplit("/", 1)[1].split(".")[0])
        base = ESTADUAL if "/estadual/" in url else MUNICIPAL
        if ano not in base:
            raise fb.AnoNaoPublicado(url)
        return base[ano]

    monkeypatch.setattr(fb, "baixar_json", baixar)
    return pedidos


def _buscar(hoje=HOJE):
    from app.apps.ponto import db
    from app.apps.ponto.core import feriados_busca as fb
    with db.conexao() as conn:
        return fb.buscar(conn, hoje, por="teste")


def _feriados(banco):
    with banco.connect() as conn:
        return sorted((r.data, r.abrangencia, r.uf or "", r.municipio or "", r.nome) for r in conn.execute(
            text("SELECT data, abrangencia, uf, municipio, nome FROM ponto.feriados")))


def test_busca_o_nacional_e_so_os_estados_e_municipios_das_obras(feriados_ponto, internet, banco):
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    feriados_ponto.obra("FC-SOBRAL", "Escola Sobral", uf="CE", municipio="Sobral")

    r = _buscar()

    todos = _feriados(banco)
    assert len([f for f in todos if f[1] == "NACIONAL"]) == 9
    assert [f for f in todos if f[1] == "ESTADUAL"] == [
        (D(2026, 3, 6), "ESTADUAL", "PE", "", "Revolução Pernambucana"),
        (D(2026, 3, 25), "ESTADUAL", "CE", "", "Data Magna do Ceará")]
    # O Piauí não tem obra: não entra. Recife e Fortaleza também não.
    assert [f for f in todos if f[1] == "MUNICIPAL"] == [
        (D(2026, 3, 12), "MUNICIPAL", "PE", "Olinda", "Aniversário de Olinda")]
    assert r["anos"][2026]["sem_feriado"] == ["Sobral/CE"]
    assert r["gravados"] == 12


def test_e_o_feriado_buscado_vale_para_a_obra_na_folha(feriados_ponto, internet):
    """A ponta que interessa ao dono: o municipal buscado desconta no auxílio."""
    from app.apps.analisesps import folha_calendario as fc
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    _buscar()
    fc.esquecer_leituras()
    assert [f["data"] for f in fc.feriados_no_periodo(D(2026, 3, 1), D(2026, 3, 31), "FC-OLINDA")] \
        == [D(2026, 3, 6), D(2026, 3, 12)]


def test_a_segunda_rodada_nao_vai_a_internet(feriados_ponto, internet):
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    _buscar()
    internet.clear()
    r = _buscar()
    assert internet == [] and r["gravados"] == 0


def test_obra_em_estado_NOVO_busca_so_o_que_e_novo(feriados_ponto, internet, banco):
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    _buscar()
    internet.clear()
    feriados_ponto.obra("FC-FORTAL", "Posto Fortaleza", uf="CE", municipio="Fortaleza")

    r = _buscar()

    assert r["anos"][2026]["estadual"] == 1 and r["anos"][2026]["municipal"] == 1
    assert r["anos"][2026]["nacional"] == 0
    assert (D(2026, 8, 15), "MUNICIPAL", "CE", "Fortaleza", "Nossa Senhora da Assunção") in _feriados(banco)


def test_o_feriado_EXCLUIDO_a_mao_nao_volta(feriados_ponto, internet, banco):
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    _buscar()
    with banco.connect() as conn:
        conn.execute(text("DELETE FROM ponto.feriados WHERE abrangencia = 'MUNICIPAL'"))
        conn.commit()
    _buscar()
    assert not [f for f in _feriados(banco) if f[1] == "MUNICIPAL"]


def test_o_cadastrado_a_mao_no_mesmo_dia_fica_como_esta(feriados_ponto, internet, banco):
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    feriados_ponto.municipal(D(2026, 3, 12), "PE", "Olinda", "Nome dado pelo DP")
    _buscar()
    assert [f[4] for f in _feriados(banco) if f[1] == "MUNICIPAL"] == ["Nome dado pelo DP"]


def test_municipio_com_nome_fora_do_IBGE_e_apontado(feriados_ponto, internet):
    feriados_ponto.obra("FC-ERRADA", "Obra", uf="PE", municipio="Olindaa")
    r = _buscar()
    assert r["anos"][2026]["nao_reconhecidos"] == ["Olindaa/PE"]


def test_em_DEZEMBRO_busca_tambem_o_ano_seguinte_e_espera_a_base_publicar(feriados_ponto, internet, banco):
    from app.apps.ponto import db
    from app.apps.ponto.core import feriados_busca as fb
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")

    r = _buscar(D(2026, 12, 2))

    assert any("2027" in a for a in r["avisos"])
    # O nacional de 2027 é calculado aqui, sem depender da base.
    assert (D(2027, 9, 7), "NACIONAL", "", "", "Independência do Brasil") in _feriados(banco)
    with db.conexao() as conn:
        # 2027 fica pendente, mas não é tentado de novo na mesma semana.
        assert 2027 not in fb.o_que_falta(conn, D(2026, 12, 3))


def test_a_rodada_automatica_nunca_levanta(feriados_ponto, monkeypatch):
    from app.apps.ponto.core import feriados_busca as fb
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")

    def sem_internet(url):
        raise ConnectionError("sem internet")
    monkeypatch.setattr(fb, "baixar_json", sem_internet)
    monkeypatch.setattr(fb, "na_madrugada", lambda momento=None: True)     # dentro da janela, para valer o teste
    assert fb.buscar_se_preciso("teste", sempre=True) is None


def test_o_PASSADO_nao_muda(feriados_ponto, internet, banco):
    """Buscado em 05/11, só grava de 01/10 em diante: um feriado de março mudaria o
    auxílio de março recalculado e a falta de um mês já fechado no ponto."""
    feriados_ponto.obra("FC-OLINDA", "Creche Olinda", uf="PE", municipio="Olinda")
    _buscar(D(2026, 11, 5))
    datas = [f[0] for f in _feriados(banco)]
    assert datas and min(datas) >= D(2026, 10, 1)
    assert D(2026, 10, 12) in datas


def test_lugares_ignora_obra_sem_UF_e_normaliza_acento():
    from app.apps.ponto.core import feriados_busca as fb
    r = fb.lugares([{"uf": "pe", "municipio": "São Lourenço da Mata"}, {"uf": "", "municipio": "X"},
                    {"uf": "PE", "municipio": "SAO LOURENCO DA MATA"}])
    assert r["ufs"] == {"PE"}
    assert list(r["municipios"]) == [("PE", "SAO LOURENCO DA MATA")]


def test_a_busca_automatica_so_roda_de_madrugada(monkeypatch):
    """13/11/2026, o dono: "o ideal seria rodar apenas na madrugada". Das 2h às 5h (Fortaleza); fora disso a
    conferência automática não faz nada — nem consulta o banco. O botão "Buscar agora" chama `buscar` direto."""
    import datetime as dt
    from app.apps.ponto.core import feriados_busca as fb
    fortaleza = dt.timezone(dt.timedelta(hours=-3))
    assert fb.na_madrugada(dt.datetime(2026, 11, 14, 2, 0, tzinfo=fortaleza))
    assert fb.na_madrugada(dt.datetime(2026, 11, 14, 4, 59, tzinfo=fortaleza))
    assert not fb.na_madrugada(dt.datetime(2026, 11, 14, 5, 0, tzinfo=fortaleza))
    assert not fb.na_madrugada(dt.datetime(2026, 11, 14, 14, 0, tzinfo=fortaleza))
    monkeypatch.setattr(fb, "na_madrugada", lambda momento=None: False)
    monkeypatch.setattr(fb.db, "conexao", lambda: (_ for _ in ()).throw(AssertionError("não devia ir ao banco")))
    assert fb.buscar_se_preciso("teste", sempre=True) is None
