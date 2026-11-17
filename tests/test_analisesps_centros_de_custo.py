"""Os centros de custo da contabilidade × o ponto (11/11/2026).

O dono: *"gerar um relatório onde o funcionário está trabalhando no ponto (…)
baseado no ponto, eu vou saber qual é o centro de custo da contabilidade que ele
deverá estar cadastrado (…) comparar a folha da contabilidade e entender se (…)
precisa haver remanejo."*
"""
import io

import pytest

from app.apps.analisesps import centros_de_custo as cc

OLINDA = "140.01 - OBRA CREPEOLINDA CONST CRECHES OLINDA PE/SETOR PRODUÇÃO"
AREIAS = "137.01 - OBRA CREPEAREIAS CONST DE CRECHES PERNAM/SETOR PRODUÇÃO"
CENTROS = {"CREPEOLINDA": OLINDA, "CREPEAREIAS": AREIAS}


def _linha(setor_codigo, setor_nome, nome="FULANO"):
    return {"id_fortes": "000123", "nome": nome, "cpf": "99713349334",
            "setor_codigo": setor_codigo, "setor_nome": setor_nome}


def _dia(obra):
    return {"marcacoes": [obra] * 4, "presenca": "Presença", "falta": ""}


def test_o_codigo_do_centro_e_a_comparacao():
    assert cc.codigo_do_centro("001.07 - CONSTRUTORA/ESCRITORIO NORDESTE") == "001.07"
    assert cc.mesmo_centro("140.01 - OBRA X", OLINDA)
    assert cc.mesmo_centro("140", OLINDA), "o nível de cima casa"
    assert not cc.mesmo_centro("137.01 - Y", OLINDA)
    assert not cc.mesmo_centro("140.1", "140.10")


def test_a_obra_principal_e_a_de_mais_dias():
    dias = [_dia("CREPEOLINDA")] * 3 + [_dia("CREPEAREIAS")] * 5 + [
        {"marcacoes": [], "presenca": "", "falta": "FALTA"}]
    assert cc.obras_do_ponto(dias) == [{"obra": "CREPEAREIAS", "dias": 5},
                                       {"obra": "CREPEOLINDA", "dias": 3}]


def test_as_situacoes():
    no = lambda *pares: [{"obra": o, "dias": n} for o, n in pares]  # noqa: E731
    certo = cc.comparar_pessoa(_linha("140.01", "OBRA CREPEOLINDA/SETOR PRODUÇÃO"),
                               no(("CREPEOLINDA", 10)), CENTROS, True)
    assert certo["situacao"] == cc.OK
    errado = cc.comparar_pessoa(_linha("137.01", "OBRA CREPEAREIAS/SETOR PRODUÇÃO"),
                                no(("CREPEOLINDA", 10)), CENTROS, True)
    assert errado["situacao"] == cc.REMANEJAR and errado["centro_esperado"] == OLINDA
    assert "remanejar de 137.01" in errado["critica"]
    dividido = cc.comparar_pessoa(_linha("137.01", "OBRA CREPEAREIAS/SETOR PRODUÇÃO"),
                                  no(("CREPEOLINDA", 10), ("CREPEAREIAS", 4)), CENTROS, True)
    assert dividido["situacao"] == cc.CONFERIR and "mais de uma obra" in dividido["critica"]
    # Afastado INSS / Desativar: sem ponto é coerente; com ponto, crítica.
    inss = _linha("001.08", "CONSTRUTORA/AFASTADO INSS")
    assert cc.comparar_pessoa(inss, [], CENTROS, True)["situacao"] == cc.COERENTE
    desativar = cc.comparar_pessoa(_linha("001.09", "CONSTRUTORA/DESATIVAR"),
                                   no(("CREPEOLINDA", 2)), CENTROS, True)
    assert desativar["situacao"] == cc.CONFERIR and "bateu ponto 2 dia(s)" in desativar["critica"]
    # Sem ponto, sem centro na AN, sem CPF.
    assert "sem ponto" in cc.comparar_pessoa(_linha("140.01", "X"), [], CENTROS, True)["critica"]
    assert "coluna AN" in cc.comparar_pessoa(_linha("140.01", "X"), no(("NOVA", 3)),
                                             CENTROS, True)["critica"]
    assert "ID Fortes" in cc.comparar_pessoa(_linha("140.01", "X"), [], CENTROS, False)["critica"]


def test_a_coluna_AN_pelo_nome_e_pela_posicao():
    cab = ["Código"] + [f"c{i}" for i in range(1, 39)] + ["Centro de Custo Contabilidade"]
    cab[2] = "Código Primário"
    cab[3] = "Nome da Obra"
    linha = ["OLI"] + [""] * 39
    linha[2], linha[3], linha[39] = "CREPEOLINDA", "Creche Olinda", OLINDA
    sem = ["ARE"] + [""] * 39
    sem[2] = "CREPEAREIAS"
    mapa = cc.mapa_da_c_diarios([cab, linha, sem])
    assert mapa["achou_pelo_nome"] and mapa["coluna"] == "AN"
    assert mapa["centros"]["CREPEOLINDA"] == OLINDA and mapa["centros"]["OLI"] == OLINDA
    assert mapa["obras_sem_centro"] == ["CREPEAREIAS"]
    cab[39] = "outro título"
    assert cc.mapa_da_c_diarios([cab, linha])["centros"]["CREPEOLINDA"] == OLINDA, \
        "sem o nome, a coluna AN"


@pytest.fixture
def folha_de_outubro(monkeypatch):
    from app.apps.analisesps import colaboradores, folha_arquivo
    from app.apps.analisesps import ponto as mod_ponto
    folha = {"id": 7, "ano": 2026, "mes": 10, "tipo": "fim_de_mes", "competencia": "10/2026",
             "rotulo_do_tipo": "fim de mês", "tem_setor": True, "linhas": [
                 dict(_linha("140.01", "OBRA CREPEOLINDA/SETOR PRODUÇÃO", "CERTO"), cpf="1" * 11),
                 dict(_linha("137.01", "OBRA CREPEAREIAS/SETOR PRODUÇÃO", "ERRADO"), cpf="2" * 11),
                 dict(_linha("001.08", "CONSTRUTORA/AFASTADO INSS", "AFASTADO"), cpf="3" * 11)]}
    monkeypatch.setattr(folha_arquivo, "abrir", lambda i: folha if int(i) == 7 else None)
    monkeypatch.setattr(folha_arquivo, "listar", lambda teto=60: [folha])
    monkeypatch.setattr(colaboradores, "de_para_do_fortes", lambda: {})
    monkeypatch.setattr(mod_ponto, "dias_por_cpf", lambda a, m: {
        "1" * 11: [_dia("CREPEOLINDA")] * 8, "2" * 11: [_dia("CREPEOLINDA")] * 8})
    monkeypatch.setattr(cc, "ler_mapa", lambda recarregar=False: {
        "centros": CENTROS, "coluna": "AN", "titulo": "Centro de Custo Contabilidade",
        "achou_pelo_nome": True, "obras_sem_centro": []})
    return folha


def test_a_montagem_e_a_planilha_para_a_contabilidade(folha_de_outubro):
    m = cc.montar(7)
    assert [(p["nome"], p["situacao"]) for p in m["pessoas"]] == [
        ("ERRADO", cc.REMANEJAR), ("CERTO", cc.OK), ("AFASTADO", cc.COERENTE)]
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(cc.planilha(m)))
    assert wb.sheetnames == ["Remanejos", "Conferência"]
    linhas = [r for r in wb["Remanejos"].iter_rows(min_row=4, values_only=True)]
    assert linhas == [("000123", "ERRADO", "137.01 - OBRA CREPEAREIAS/SETOR PRODUÇÃO", OLINDA,
                       f"CREPEOLINDA (8 dia(s)) → {OLINDA}")]


from tests.test_analisesps_telas import _como_mestre, app  # noqa: E402,F401


def test_a_tela_e_a_planilha_pela_rota(app, folha_de_outubro):
    c = _como_mestre(app)
    html = c.get("/analisesps/folha/7/centros-de-custo").get_data(as_text=True)
    assert "Remanejar" in html and "ERRADO" in html and "remanejar de 137.01" in html
    so = c.get("/analisesps/folha/7/centros-de-custo?situacao=ok").get_data(as_text=True)
    assert "CERTO" in so and "ERRADO" not in so
    r = c.get("/analisesps/folha/7/centros-de-custo.xlsx")
    assert r.status_code == 200 and r.data[:2] == b"PK"
    assert c.get("/analisesps/folha/999/centros-de-custo").status_code == 404
