"""As outras folhas da pessoa — o cruzamento das telas da folha (05/11/2026).

O dono: *"enxergar dentro de despesa colaboradores uma informaçãozinha (…) quanto
recebeu na folha da contabilidade, com alimentação, transporte, gratificados (…) o
que vai definir se a informação vai ser exibida é o que está ou não selecionado."*
"""
from decimal import Decimal as D

import pytest

from app.apps.analisesps import folhas_cruzadas as fc

ZE = "99713349334"


def _i(folha, valor, obs="", tipo=""):
    return {"folha": folha, "valor": D(valor), "obs": obs, "tipo": tipo}


# ---------------------------------------------------------------------------
# As etiquetas de uma pessoa
# ---------------------------------------------------------------------------
def test_a_tela_nao_se_repete_e_a_ordem_e_a_das_abas():
    itens = [_i("gratificados", "500", "1ª quinz."), _i("dc", "150", "alimentação", "Despesas com Alimentação"),
             _i("contab_quinzena", "1850"), _i("transporte", "220"), _i("alimentacao", "330")]
    na_dc = fc.etiquetas_da_pessoa(itens, "dc")
    assert [e["rotulo"] for e in na_dc] == ["Contab. quinz.", "Alim.", "Transp.", "Grat."]
    na_alimentacao = fc.etiquetas_da_pessoa(itens, "alimentacao")
    assert "Alim." not in [e["rotulo"] for e in na_alimentacao]
    assert "Transp." in [e["rotulo"] for e in na_alimentacao], "o outro auxílio aparece"


def test_duas_metades_da_mesma_folha_somam_numa_etiqueta():
    itens = [_i("diarista", "520", "1ª quinz."), _i("diarista", "480", "fim do mês")]
    (e,) = fc.etiquetas_da_pessoa(itens, "dc")
    assert e["valor"] == D("1000") and e["obs"] == "1ª quinz., fim do mês"


def test_a_contabilidade_fica_SEPARADA_por_quinzena_e_fim_do_mes():
    """Decisão de 05/11/2026: separadas ajudam a ver o que foi pago duas vezes."""
    itens = [_i("contab_quinzena", "900"), _i("contab_mes", "950")]
    assert [e["rotulo"] for e in fc.etiquetas_da_pessoa(itens, "dc")] == [
        "Contab. quinz.", "Contab. fim do mês"]
    assert fc.etiquetas_da_pessoa(itens, "importar") == [], "na contabilidade, nenhuma das duas"


def test_ALERTA_diarista_e_contabilidade_ao_mesmo_tempo():
    itens = [_i("diarista", "1040"), _i("contab_mes", "1850"), _i("alimentacao", "330")]
    na_diarista = {e["rotulo"]: e for e in fc.etiquetas_da_pessoa(itens, "diaristas")}
    assert na_diarista["Contab. fim do mês"]["alerta"], "o alerta olha a folha da tela aberta também"
    assert "diarista" in na_diarista["Contab. fim do mês"]["titulo"]
    assert not na_diarista["Alim."]["alerta"]


def test_ALERTA_alimentacao_no_auxilio_E_na_DC_so_pelo_tipo():
    com_alim = [_i("alimentacao", "330"), _i("dc", "150", "alimentação", "Despesas com Alimentação")]
    assert all(e["alerta"] for e in fc.etiquetas_da_pessoa(com_alim, "gratificados"))
    outro_tipo = [_i("alimentacao", "330"), _i("dc", "80", "diárias de viagem", "Diárias de Viagem")]
    assert not any(e["alerta"] for e in fc.etiquetas_da_pessoa(outro_tipo, "gratificados"))
    transp = [_i("transporte", "220"), _i("dc", "50", "transporte", "Despesas com Transporte")]
    assert all(e["alerta"] for e in fc.etiquetas_da_pessoa(transp, "avulso"))


def test_quem_nao_esta_em_outra_folha_nao_ganha_etiqueta():
    assert fc.etiquetas_da_pessoa([], "dc") == []
    assert fc.etiquetas_da_pessoa([_i("dc", "150")], "dc") == []


# ---------------------------------------------------------------------------
# O resumo da competência: guardado, esquecido, e uma fonte não derruba as outras
# ---------------------------------------------------------------------------
@pytest.fixture
def fontes(monkeypatch):
    chamadas = []

    def boa(ano, mes):
        chamadas.append((ano, mes))
        yield "997.133.493-34", {"folha": "alimentacao", "valor": D("330")}
        yield "123", {"folha": "alimentacao", "valor": D("1")}           # CPF torto: fora
        yield ZE, {"folha": "transporte", "valor": D("0")}                # zero: fora

    def quebrada(ano, mes):
        raise RuntimeError("planilha fora do ar")
        yield  # noqa

    monkeypatch.setattr(fc, "FONTES", [("Auxílios", boa), ("Despesas com colaboradores", quebrada)])
    fc.esquecer()
    yield chamadas
    fc.esquecer()


def test_o_resumo_le_normaliza_e_avisa_a_fonte_que_falhou(fontes):
    r = fc.resumo(2026, 10)
    assert list(r["pessoas"]) == [ZE]
    assert r["pessoas"][ZE][0]["valor"] == D("330")
    assert r["avisos"] and "Despesas com colaboradores" in r["avisos"][0]


def test_o_resumo_fica_guardado_ate_alguem_salvar_selecao(fontes):
    fc.resumo(2026, 10)
    fc.resumo(2026, 10)
    assert len(fontes) == 1, "a segunda visita usa o guardado"
    fc.esquecer()
    fc.resumo(2026, 10)
    assert len(fontes) == 2


# ---------------------------------------------------------------------------
# A fonte da contabilidade: o líquido, sem quem foi tirado, CPF pelo cadastro
# ---------------------------------------------------------------------------
def test_a_contabilidade_tira_quem_esta_FORA_e_acha_o_cpf_pelo_codigo(monkeypatch):
    from app.apps.analisesps import colaboradores, folha_apropriacao_guardada as guardada, folha_arquivo as fa
    monkeypatch.setattr(fa, "listar", lambda teto=60: [
        {"id": 1, "ano": 2026, "mes": 10, "tipo": "quinzena"},
        {"id": 2, "ano": 2026, "mes": 10, "tipo": "fim_de_mes"},
        {"id": 3, "ano": 2026, "mes": 9, "tipo": "fim_de_mes"}])
    linhas = {1: [{"id_fortes": "13", "cpf": "", "valor": D("900")},
                  {"id_fortes": "20", "cpf": "03513441363", "valor": D("800")}],
              2: [{"id_fortes": "13", "cpf": ZE, "valor": D("950")}]}
    monkeypatch.setattr(fa, "abrir", lambda i: {"linhas": linhas.get(i, [])})
    monkeypatch.setattr(colaboradores, "de_para_do_fortes", lambda: {"000013": {"cpf": ZE}})
    monkeypatch.setattr(guardada, "ajustes_do_pagamento", lambda a, m, t: (
        {"03513441363": {"fora": True}} if t == "quinzena" else {}))
    saida = list(fc._da_contabilidade(2026, 10))
    assert saida == [(ZE, {"folha": "contab_quinzena", "valor": D("900")}),
                     (ZE, {"folha": "contab_mes", "valor": D("950")})]


# ---------------------------------------------------------------------------
# A rota e o esquecer depois de salvar
# ---------------------------------------------------------------------------
from tests.test_analisesps_telas import _como_mestre, app  # noqa: E402,F401


def test_a_rota_entrega_as_etiquetas_formatadas(app, monkeypatch):
    monkeypatch.setattr(fc, "resumo", lambda ano, mes, recarregar=False: {
        "pessoas": {ZE: [_i("contab_quinzena", "1850"), _i("diarista", "1040")]}, "avisos": []})
    d = _como_mestre(app).get(
        "/analisesps/api/folha/outras-folhas?ano=2026&mes=10&tela=dc").get_json()
    assert d["ok"] and d["competencia"] == "10/2026"
    (a, b) = d["pessoas"][ZE]
    assert a["rotulo"] == "Contab. quinz." and a["valor"] == "1.850,00"
    assert b["alerta"] is True and a["alerta"] is True


def test_competencia_invalida_e_recusada(app):
    r = _como_mestre(app).get("/analisesps/api/folha/outras-folhas?ano=1&mes=13&tela=dc")
    assert r.status_code == 400


def test_salvar_selecao_numa_tela_da_folha_esquece_o_cruzamento(app, monkeypatch):
    esquecidos = []
    monkeypatch.setattr(fc, "esquecer", lambda *a, **k: esquecidos.append(1))
    from app.apps.analisesps import dc
    monkeypatch.setattr(dc, "salvar_selecao", lambda decisoes, quem="": {"gravados": 1, "limpos": 0})
    r = _como_mestre(app).post("/analisesps/api/folha/dc/selecao", json={"decisoes": []})
    assert r.status_code < 400, r.get_data(as_text=True)
    assert esquecidos


def test_as_seis_telas_da_folha_pedem_as_etiquetas():
    import pathlib
    pasta = pathlib.Path(__file__).resolve().parents[1] / "app" / "apps" / "analisesps" / "templates"
    for nome in ("aberta", "auxilio", "diaristas", "dc", "avulso", "gratificados"):
        assert '{% include "_outras_folhas.html" %}' in (pasta / f"analisesps_folha_{nome}.html").read_text(
            encoding="utf-8"), nome


# ---------------------------------------------------------------------------
# Com banco: as seis fontes rodam de verdade, pelas funções das telas
# ---------------------------------------------------------------------------
from tests.test_analisesps_dc import ATIVO, banco_dc  # noqa: E402,F401


@pytest.mark.banco
def test_as_seis_fontes_rodam_no_banco_e_a_DC_entra(banco_dc):
    """Nenhuma fonte pode estourar com o banco de verdade (aviso é estouro); a DC,
    que tem linha selecionada no exemplo, aparece na pessoa."""
    fc.esquecer()
    r = fc.resumo(2026, 10, recarregar=True)
    assert r["avisos"] == []
    assert [i["folha"] for i in r["pessoas"][ATIVO]] == ["dc"]
    assert fc.etiquetas_da_pessoa(r["pessoas"][ATIVO], "dc") == []
    (e,) = fc.etiquetas_da_pessoa(r["pessoas"][ATIVO], "gratificados")
    assert e["rotulo"] == "DC" and e["valor"] == D("150.00") and e["obs"] == "alimentação"
    fc.esquecer()
