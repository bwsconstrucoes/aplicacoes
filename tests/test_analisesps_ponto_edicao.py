# -*- coding: utf-8 -*-
"""
01/10/2026 — lançar o ponto no Mobponto a partir da janela do funcionário.

*"de forma que eu possa ajustar uma única batida, ou um dia todo ou um período
todo (…) não lançar informação que sobreponha o que já existe (…) o padrão é
entrada 7 horas, almoço 12, retorno 13 e saída às 17 de segunda a quinta, ou às
16 na sexta-feira."*

Nenhum teste fala com o Mobponto: a chamada é trocada por um dublê.
"""
import datetime as dt

import pytest

from tests.test_analisesps_telas import app  # noqa: F401 — a fixture
from tests.test_analisesps_telas import (SENHA_CONSULTA, _como_mestre,
                                         _dias_do_mes, _preparar_folha_aberta,
                                         como)

D = dt.date


def _dia(data, horas=("", "", "", ""), obras=None, presenca="", falta=""):
    obras = obras or [("X" if h else "") for h in horas]
    return {"data": data, "horas": list(horas), "marcacoes": list(obras),
            "presenca": presenca, "falta": falta}


# ---------------------------------------------------------------------------
# O HORÁRIO PADRÃO E O ENCAIXE
# ---------------------------------------------------------------------------
def test_o_horario_padrao_e_7_12_13_17_e_16_NA_SEXTA():
    from app.apps.analisesps import ponto_edicao as pe
    assert pe.horario_do_dia(D(2026, 9, 14)) == ["07:00", "12:00", "13:00", "17:00"]  # seg
    assert pe.horario_do_dia(D(2026, 9, 17)) == ["07:00", "12:00", "13:00", "17:00"]  # qui
    assert pe.horario_do_dia(D(2026, 9, 18)) == ["07:00", "12:00", "13:00", "16:00"]  # sex


@pytest.mark.parametrize("existentes,faltam", [
    ([], ["07:00", "12:00", "13:00", "17:00"]),
    (["07:05"], ["12:00", "13:00", "17:00"]),
    # Bateu só o retorno: é por HORÁRIO, não por posição.
    (["13:05"], ["07:00", "12:00", "17:00"]),
    (["06:50", "17:10"], ["12:00", "13:00"]),
    (["11:50", "13:10"], ["07:00", "17:00"]),
    (["07:00", "12:00", "13:00", "17:00"], []),
])
def test_encaixa_SO_O_QUE_FALTA(existentes, faltam):
    from app.apps.analisesps import ponto_edicao as pe
    assert pe.encaixar(existentes, ["07:00", "12:00", "13:00", "17:00"]) == faltam


def test_encaixa_pelo_MAIS_PROXIMO_e_recusa_o_impossivel():
    from app.apps.analisesps import ponto_edicao as pe
    padrao = ["07:00", "12:00", "13:00", "17:00"]
    # Três batidas em volta do almoço: casam com almoço/retorno/saída? Não — o
    # mais próximo é almoço, retorno e… a saída fica longe; a entrada também.
    # O menor desvio deixa a ENTRADA faltando.
    assert pe.encaixar(["12:10", "12:20", "12:30"], padrao) == ["07:00"]
    assert pe.encaixar(["06:00", "06:10", "06:20"], padrao) == ["17:00"]
    assert pe.encaixar(["18:00", "18:10", "18:20"], padrao) == ["07:00"]
    # Duas batidas na mesma hora: não há ordem possível — o dia fica para a mão.
    assert pe.encaixar(["07:00", "07:00"], padrao) is None


# ---------------------------------------------------------------------------
# O PLANO DO PERÍODO — o exemplo dele, com as datas de setembro/2026
# ---------------------------------------------------------------------------
def test_o_EXEMPLO_DELE_16_a_30_com_o_dia_21_batido():
    """*"bateu o ponto dia 16 ao dia 30 (…) mas se no dia 21 tenha um ponto
    batido (…) vamos aplicar do dia 16 ao 20, e do dia 22 ao 30."*"""
    from app.apps.analisesps import ponto_edicao as pe
    dias = [_dia(D(2026, 9, 21), ("07:00", "12:00", "13:00", "17:00"))]
    plano = pe.planejar(dias, "2026-09-16", "2026-09-30")
    por_data = {d["data"]: d for d in plano["dias"]}
    assert por_data["2026-09-21"]["lancar"] == []
    assert por_data["2026-09-21"]["motivo"] == "o dia já está completo"
    # Fim de semana fica de fora.
    assert por_data["2026-09-19"]["motivo"] == "fim de semana"
    assert por_data["2026-09-20"]["motivo"] == "fim de semana"
    # Sexta sai às 16h.
    assert por_data["2026-09-18"]["lancar"] == ["07:00", "12:00", "13:00", "16:00"]
    assert por_data["2026-09-16"]["lancar"] == ["07:00", "12:00", "13:00", "17:00"]
    # 16 a 30/09/2026: 11 dias úteis, menos o 21 = 10 dias, 40 batidas.
    assert plano["dias_com_lancamento"] == 10
    assert plano["batidas"] == 40


def test_dia_com_UMA_batida_recebe_as_TRES_que_faltam():
    from app.apps.analisesps import ponto_edicao as pe
    plano = pe.planejar([_dia(D(2026, 9, 21), ("07:02", "", "", ""))],
                        "2026-09-21", "2026-09-21")
    assert plano["dias"][0]["lancar"] == ["12:00", "13:00", "17:00"]


@pytest.mark.parametrize("dia,motivo", [
    (_dia(D(2026, 9, 22), falta="Atestado médico"), "falta lançada"),
    (_dia(D(2026, 9, 22), presenca="Feriado"), "o ponto diz Feriado"),
    (_dia(D(2026, 9, 22), presenca="Férias"), "o ponto diz Férias"),
    (_dia(D(2026, 9, 22), ("", "", "", ""), obras=["CRE1", "", "", ""]), "sem hora"),
])
def test_dia_parado_ou_estranho_NAO_E_TOCADO(dia, motivo):
    from app.apps.analisesps import ponto_edicao as pe
    plano = pe.planejar([dia], "2026-09-22", "2026-09-22")
    assert plano["dias"][0]["lancar"] == []
    assert motivo in plano["dias"][0]["motivo"]


def test_ausencia_sem_falta_lancada_E_PREENCHIDA():
    """O Mobponto marca "FALTA" na presença de quem não bateu — é justamente o
    dia que ele quer preencher. Só a falta LANÇADA (atestado…) segura o dia."""
    from app.apps.analisesps import ponto_edicao as pe
    plano = pe.planejar([_dia(D(2026, 9, 22), presenca="FALTA")],
                        "2026-09-22", "2026-09-22")
    assert len(plano["dias"][0]["lancar"]) == 4


def test_feriado_e_ferias_DO_SISTEMA_ficam_de_fora():
    from app.apps.analisesps import ponto_edicao as pe
    plano = pe.planejar([], "2026-09-07", "2026-09-09",
                        feriados={D(2026, 9, 7): "Independência"},
                        ferias={D(2026, 9, 8)})
    motivos = [d["motivo"] for d in plano["dias"]]
    assert motivos[0] == "feriado (Independência)"
    assert motivos[1] == "férias cadastradas"
    assert plano["dias"][2]["lancar"]


def test_batida_AVULSA_e_uma_so_e_nao_encosta_em_outra():
    from app.apps.analisesps import ponto_edicao as pe
    dias = [_dia(D(2026, 9, 22), ("07:00", "", "", ""))]
    assert pe.planejar(dias, "2026-09-22", "2026-09-22",
                       hora_avulsa="17:00")["dias"][0]["lancar"] == ["17:00"]
    perto = pe.planejar(dias, "2026-09-22", "2026-09-22", hora_avulsa="07:03")
    assert perto["dias"][0]["lancar"] == [] and "perto" in perto["dias"][0]["motivo"]
    # Avulsa num fim de semana escolhido sozinho: é o pequeno ajuste, vale.
    sabado = pe.planejar([], "2026-09-19", "2026-09-19", hora_avulsa="08:00")
    assert sabado["dias"][0]["lancar"] == ["08:00"]


def test_periodo_ao_contrario_ou_grande_demais_e_RECUSADO():
    from app.apps.analisesps import ponto_edicao as pe
    with pytest.raises(pe.ErroDaEdicao):
        pe.planejar([], "2026-09-20", "2026-09-10")
    with pytest.raises(pe.ErroDaEdicao):
        pe.planejar([], "2026-08-01", "2026-09-30")


# ---------------------------------------------------------------------------
# O LANÇAMENTO (processo separado)
# ---------------------------------------------------------------------------
@pytest.fixture
def configurado(monkeypatch):
    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic teste")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave-de-teste")
    monkeypatch.setenv("MOBPONTO_RESPONSAVEL_CPF", "111.222.333-96")
    monkeypatch.setenv("MOBPONTO_RESPONSAVEL_NOME", "MARCELO")


aplicadas = []   # o que foi posto na cópia baixada, por dia


def _dublar(monkeypatch, respostas=(), dias_antes=None, dias_depois=None):
    from app.apps.analisesps import folha_calendario, ponto, ponto_edicao
    mandados, trazidos = [], []
    aplicadas.clear()
    fila = list(respostas)

    def falso(payload):
        mandados.append(dict(payload))
        return fila.pop(0) if fila else (True, '{"status": true}')
    monkeypatch.setattr(ponto_edicao, "_mandar", falso)
    monkeypatch.setattr(ponto_edicao, "_registrar", lambda *a, **k: None)
    monkeypatch.setattr(ponto_edicao, "PAUSA_ENTRE_BATIDAS", 0)
    monkeypatch.setattr(ponto, "atualizar_pessoa",
                        lambda *a, **k: trazidos.append(a) or {"achou": True})
    monkeypatch.setattr(ponto, "aplicar_batidas_na_copia",
                        lambda ano, mes, cpf, nome, data, novas:
                        aplicadas.append((data, list(novas))) or True)
    monkeypatch.setattr(ponto, "dias_de_um_cpf",
                        lambda a, m, c: list(dias_antes or []))
    monkeypatch.setattr(folha_calendario, "feriados_no_periodo", lambda *a: [])
    monkeypatch.setattr(folha_calendario, "ferias_que_cruzam", lambda *a: [])
    # As obras da C. Diários — as mesmas do Mobponto.
    monkeypatch.setattr(ponto_edicao, "obras_permitidas",
                        lambda: ["CRE1", "XYZ9", "SEDE"])
    return mandados, trazidos


def _pedido(**extra):
    base = {"ano": 2026, "mes": 9, "cpf": "99713349334", "nome": "GERLANIO",
            "de": "2026-09-16", "ate": "2026-09-18", "obra": "cre1",
            "justificativa": "esqueceu de bater", "quem": "MARCELO"}
    base.update(extra)
    return base


def test_lancar_NAO_VAI_AO_MOBPONTO_antes_nem_depois_e_aplica_na_copia(configurado,
                                                                       monkeypatch):
    """O dono, 01/10/2026: *"não tem sentido buscar antes, leva muito tempo. A base
    de informações já existe, precisa somente aplicar."*"""
    from app.apps.analisesps import ponto_edicao as pe
    mandados, trazidos = _dublar(monkeypatch)
    feito = pe.lancar(_pedido())
    assert trazidos == [], "foi ao Mobponto buscar a pessoa"
    assert len(mandados) == 12          # 16, 17 e 18/09 × 4
    p = mandados[0]
    assert p["type_data"] == "CAD_EDT_PONTO" and p["acao"] == "C"
    assert p["cpf_funcionario"] == "99713349334"
    assert p["cpf_responsavel"] == "11122233396" and p["nome_responsavel"] == "MARCELO"
    assert p["dt_ponto_new"] == "2026-09-16 07:00" and p["local"] == "CRE1"
    assert mandados[-1]["dt_ponto_new"] == "2026-09-18 16:00"   # sexta
    assert feito["falhou"] is None
    assert "12 batida(s) lançada(s) em 3 dia(s)" in pe.recado_do_lancamento(feito)
    # Cada dia lançado foi posto na cópia, com as horas que o Mobponto aceitou.
    assert [d for d, _ in aplicadas] == ["2026-09-16", "2026-09-17", "2026-09-18"]
    assert aplicadas[2][1][-1] == ("16:00", "CRE1")


def test_o_plano_e_REFEITO_com_o_ponto_novo(configurado, monkeypatch):
    """Se alguém bateu depois da carga, o lançamento não sobrepõe."""
    from app.apps.analisesps import ponto_edicao as pe
    dias = [_dia(D(2026, 9, 17), ("07:00", "12:00", "13:00", "17:00"))]
    mandados, _ = _dublar(monkeypatch, dias_antes=dias)
    pe.lancar(_pedido())
    assert not any(m["dt_ponto_new"].startswith("2026-09-17") for m in mandados)
    assert len(mandados) == 8


def test_PARA_na_primeira_que_falha(configurado, monkeypatch):
    from app.apps.analisesps import ponto_edicao as pe
    mandados, _ = _dublar(monkeypatch, [(True, "ok"), (None, "NÃO SEI SE GRAVOU")])
    feito = pe.lancar(_pedido())
    assert len(mandados) == 2
    assert feito["falhou"]["talvez_gravou"] is True
    recado = pe.recado_do_lancamento(feito)
    assert "PAROU em 16/09 12:00" in recado and "confira no Mobponto" in recado
    # A que entrou foi para a cópia; a que não se sabe, não.
    assert aplicadas == [("2026-09-16", [("07:00", "CRE1")])]


def test_obra_FORA_DA_C_DIARIOS_e_recusada_antes_de_mandar(configurado, monkeypatch):
    """*"As obras do Mobponto são as mesmas do cadastro C. Diários."*"""
    from app.apps.analisesps import ponto_edicao as pe
    mandados, _ = _dublar(monkeypatch)
    with pytest.raises(pe.ErroDaEdicao, match="C. Diários"):
        pe.lancar(_pedido(obra="OBRA INVENTADA"))
    assert mandados == []


def test_sem_a_lista_da_C_DIARIOS_nada_e_lancado(configurado, monkeypatch):
    from app.apps.analisesps import ponto_edicao as pe
    mandados, _ = _dublar(monkeypatch)
    monkeypatch.setattr(pe, "obras_permitidas", lambda: [])
    with pytest.raises(pe.ErroDaEdicao, match="planilhas de apoio"):
        pe.lancar(_pedido())
    assert mandados == []


def test_a_lista_de_obras_e_o_CODIGO_PRIMARIO_das_duas_tabelas(monkeypatch):
    from app.apps.analisesps import db, ponto_edicao as pe

    def falso(sql, params=()):
        if "contas_diarios" in sql:
            return [("cre1",), ("XYZ9 ",)]
        return [("CRE1",), ("SEDE",), ("",)]
    monkeypatch.setattr(db, "consultar", falso)
    assert pe.obras_permitidas() == ["CRE1", "SEDE", "XYZ9"]


def test_o_ERRO_DE_CERTIFICADO_e_remendado_como_na_leitura(configurado, monkeypatch):
    """Falha real, 01/10/2026: *"PAROU em 16/09 07:00 (…) CERTIFICATE_VERIFY_FAILED
    (…) unable to get local issuer certificate"*. A leitura completava a cadeia
    sozinha; o envio não. O erro de certificado acontece ANTES de o pedido sair,
    então repetir uma vez não duplica a batida."""
    import requests
    from app.apps.analisesps import ponto, ponto_edicao as pe
    monkeypatch.setattr(pe, "_CONFIANCA_REMENDADA", None)
    chamadas = []

    class Resposta:
        ok, status_code, text = True, 200, '{"status": true}'
        def json(self):
            return {"status": True}

    def post(url, data=None, headers=None, verify=None, timeout=None):
        chamadas.append(verify)
        if len(chamadas) == 1:
            raise requests.exceptions.SSLError(
                "[SSL: CERTIFICATE_VERIFY_FAILED] unable to get local issuer certificate")
        return Resposta()
    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(ponto, "_intermediario_do_servidor", lambda url: "PEM")
    monkeypatch.setattr(ponto, "_pacote_com_o_extra", lambda pacote, extra: "/tmp/remendo.pem")
    ok, _ = pe._mandar({"type_data": "CAD_EDT_PONTO"})
    assert ok is True
    assert chamadas[-1] == "/tmp/remendo.pem" and len(chamadas) == 2
    # A próxima batida já sai com o pacote remendado, sem tropeçar de novo.
    pe._mandar({"type_data": "CAD_EDT_PONTO"})
    assert chamadas[-1] == "/tmp/remendo.pem" and len(chamadas) == 3


def test_sem_conseguir_remendar_o_recado_diz_que_NADA_FOI_GRAVADO(configurado, monkeypatch):
    import requests
    from app.apps.analisesps import ponto, ponto_edicao as pe
    monkeypatch.setattr(pe, "_CONFIANCA_REMENDADA", None)

    def post(*a, **k):
        raise requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED")
    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(ponto, "_intermediario_do_servidor", lambda url: "")
    ok, recado = pe._mandar({})
    assert ok is False and "Nada foi gravado" in recado and "MOBPONTO_CA_EXTRA" in recado


def test_sem_o_responsavel_NAO_GRAVA_e_diz_o_que_criar(monkeypatch):
    from app.apps.analisesps import ponto_edicao as pe
    monkeypatch.setenv("MOBPONTO_AUTHORIZATION", "Basic teste")
    monkeypatch.setenv("MOBPONTO_API_KEY", "chave")
    monkeypatch.delenv("MOBPONTO_RESPONSAVEL_CPF", raising=False)
    monkeypatch.delenv("MOBPONTO_RESPONSAVEL_NOME", raising=False)
    mandados, _ = _dublar(monkeypatch)
    with pytest.raises(pe.ErroDaEdicao, match="MOBPONTO_RESPONSAVEL_CPF"):
        pe.lancar(_pedido())
    assert mandados == []


# ---------------------------------------------------------------------------
# AS ROTAS E A TELA
# ---------------------------------------------------------------------------
def test_a_rota_do_PLANO_nao_grava_nada(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    mandados, _ = _dublar(monkeypatch)
    r = _como_mestre(app).post("/analisesps/api/folha/ponto/plano", json={
        "folha_id": 1, "cpf": "99713349334", "de": "2026-09-14",
        "ate": "2026-09-15", "obra": "CRE1"})
    d = r.get_json()
    assert r.status_code == 200 and d["ok"], d
    assert d["plano"]["batidas"] == 8
    assert mandados == []


def test_a_rota_de_LANCAR_dispara_o_processo_separado(app, configurado, monkeypatch):
    """Sem a fila (antes da migração 042): o pedido vai para o `meta`."""
    from app.apps.analisesps import ponto_fila, sincronizacao, tarefas
    monkeypatch.setattr(ponto_fila, "_pronto", lambda: False)
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    gravado, disparos = {}, []
    monkeypatch.setattr(sincronizacao, "_meta_gravar",
                        lambda conn, k, v: gravado.update({k: v}))
    monkeypatch.setattr(tarefas, "disparar",
                        lambda modo, disparo="": disparos.append(modo) or {"ok": True})

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
    from app.apps.analisesps import db
    monkeypatch.setattr(db, "conexao", lambda: _Conn())
    r = _como_mestre(app).post("/analisesps/api/folha/ponto/lancar", json={
        "folha_id": 1, "cpf": "99713349334", "nome": "GERLANIO",
        "de": "2026-09-14", "ate": "2026-09-15", "obra": "cre1",
        "justificativa": "bateu no lugar errado"})
    assert r.status_code == 200, r.get_json()
    assert disparos == ["ponto_lancar"]
    assert '"obra": "CRE1"' in gravado["ponto_lancar_pedido"]


def test_as_rotas_RECUSAM_quem_nao_esta_na_folha_e_outro_mes(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    cliente = _como_mestre(app)
    fora = cliente.post("/analisesps/api/folha/ponto/plano", json={
        "folha_id": 1, "cpf": "52998224725", "de": "2026-09-14", "obra": "A"})
    assert fora.status_code == 404
    outro = cliente.post("/analisesps/api/folha/ponto/plano", json={
        "folha_id": 1, "cpf": "99713349334", "de": "2026-08-14", "obra": "A"})
    assert outro.status_code == 400


def test_lancar_no_mobponto_e_de_quem_OPERA_a_folha_nao_so_do_mestre(app, configurado,
                                                                     monkeypatch):
    """O dono, 01/10/2026: a pessoa do DP, com a Folha e marcada para alterar, não
    conseguia lançar no ponto."""
    from app.apps.analisesps import auth
    assert auth.e_so_do_mestre("analisesps.folha_ponto_lancar") is False
    assert auth.e_so_do_mestre("analisesps.folha_ponto_plano") is False
    # Quem só CONSULTA continua sem poder.
    r = como(app, SENHA_CONSULTA).post("/analisesps/api/folha/ponto/lancar", json={})
    assert r.status_code in (302, 403, 404)


def test_quem_opera_SO_A_FOLHA_ve_o_quadro_de_lancar(app, configurado, monkeypatch):
    from app.apps.analisesps import auth, usuarios
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    monkeypatch.setattr(usuarios, "buscar_por_id", lambda uid: {
        "id": 7, "login": "ana", "nome": "ANA", "telas": ["folha"],
        "pode_operar": True, "mestre": False, "ativo": True})
    c = app.test_client()
    with c.session_transaction() as s:
        s[auth.CHAVE_SESSAO] = auth.OPERADOR
        s[auth.CHAVE_USUARIO] = 7
        s[auth.CHAVE_NOME] = "ANA"
    html = c.get("/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "Ver o que vai ser lançado" in html
    r = c.post("/analisesps/api/folha/ponto/plano", json={
        "folha_id": 1, "cpf": "99713349334", "de": "2026-09-14",
        "ate": "2026-09-14", "obra": "CRE1"})
    assert r.status_code == 200 and r.get_json()["plano"]["batidas"] == 4


def test_o_analitico_tem_o_quadro_de_LANCAR(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert 'id="lancar-ponto"' in html and "Ver o que vai ser lançado" in html
    # A obra é escolhida numa LISTA da C. Diários, com a do ponto já marcada.
    assert '<select class="lancar-obra"' in html
    assert '<option value="CRE1" selected>' in html
    assert '<option value="SEDE"' in html
    assert 'class="link-btn lancar-dia"' in html
    assert 'value="2026-09-01"' in html and 'value="2026-09-15"' in html
    # O quadro vem ANTES da tabela do dia a dia: não se rola a tela para achar.
    assert html.index('id="lancar-ponto"') < html.index("ponto-dia-a-dia")


def test_sem_a_lista_da_C_DIARIOS_o_quadro_DIZ_e_nao_oferece(app, configurado, monkeypatch):
    from app.apps.analisesps import ponto_edicao as pe
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    monkeypatch.setattr(pe, "obras_permitidas", lambda: [])
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "planilhas de apoio" in html
    assert "Ver o que vai ser lançado" not in html


def test_sem_configuracao_o_quadro_DIZ_O_QUE_FALTA(app, monkeypatch):
    monkeypatch.delenv("MOBPONTO_RESPONSAVEL_CPF", raising=False)
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "MOBPONTO_RESPONSAVEL_CPF" in html
    assert "Ver o que vai ser lançado" not in html


def test_a_pagina_de_imprimir_NAO_tem_o_lancar(app, configurado, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334").get_data(as_text=True)
    assert "lancar-ponto" not in html and "lancar-dia" not in html


def test_com_tarefa_de_pessoa_rodando_o_pedido_novo_NAO_SOBRESCREVE_o_anterior(
        app, configurado, monkeypatch):
    """O pedido mora num lugar só e o processo o lê ao começar: escrever o de B
    enquanto o de A está para começar faria A trabalhar para B. (Sem a fila.)"""
    from app.apps.analisesps import ponto_fila, sincronizacao, tarefas
    monkeypatch.setattr(ponto_fila, "_pronto", lambda: False)
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    gravado = []
    monkeypatch.setattr(sincronizacao, "_meta_gravar",
                        lambda conn, k, v: gravado.append(k))
    monkeypatch.setattr(tarefas, "estado", lambda pista="geral": {
        "rodando": pista == "pessoa", "detalhe": {"etapa": "lançando o ponto"}})
    cliente = _como_mestre(app)
    r = cliente.post("/analisesps/api/folha/ponto/lancar", json={
        "folha_id": 1, "cpf": "99713349334", "de": "2026-09-14",
        "ate": "2026-09-14", "obra": "CRE1", "justificativa": "bateu errado"})
    assert r.status_code == 409 and "continua sozinha" in r.get_json()["erro"]
    r = cliente.post("/analisesps/api/folha/ponto/pessoa", json={
        "folha_id": 1, "cpf": "99713349334"})
    assert r.status_code == 409
    assert gravado == [], "o pedido de quem está rodando foi sobrescrito"


# ---------------------------------------------------------------------------
# A FILA (migração 042): o pedido entra e a tela volta na hora
# ---------------------------------------------------------------------------
def test_com_a_FILA_o_lancamento_entra_nela_e_nao_espera(app, configurado, monkeypatch):
    from app.apps.analisesps import ponto_fila
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    entrou, cutucadas = [], []
    monkeypatch.setattr(ponto_fila, "_pronto", lambda: True)
    monkeypatch.setattr(ponto_fila, "enfileirar", lambda tipo, ano, mes, cpf, nome="",
                        pedido=None, quem="": entrou.append((tipo, cpf, pedido)) or
                        {"id": 7, "posicao": 2, "repetido": False})
    monkeypatch.setattr(ponto_fila, "cutucar", lambda: cutucadas.append(1))
    cliente = _como_mestre(app)
    r = cliente.post("/analisesps/api/folha/ponto/lancar", json={
        "folha_id": 1, "cpf": "99713349334", "nome": "GERLANIO",
        "de": "2026-09-14", "ate": "2026-09-15", "obra": "cre1",
        "justificativa": "bateu no lugar errado"})
    assert r.get_json() == {"ok": True, "fila_id": 7, "posicao": 2}
    assert entrou[0][0] == "lancar" and entrou[0][2]["obra"] == "CRE1"
    r = cliente.post("/analisesps/api/folha/ponto/pessoa", json={
        "folha_id": 1, "cpf": "99713349334", "nome": "GERLANIO"})
    assert r.get_json()["fila_id"] == 7 and entrou[1][0] == "pessoa"
    assert len(cutucadas) == 2


def test_o_analitico_mostra_o_ULTIMO_PEDIDO_da_pessoa(app, configurado, monkeypatch):
    import datetime as _dt
    from app.apps.analisesps import ponto_fila
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _dublar(monkeypatch)
    monkeypatch.setattr(ponto_fila, "ultimo_da_pessoa", lambda cpf: {
        "id": 9, "tipo": "lancar", "rotulo": "lançar batidas", "situacao": "esperando",
        "posicao": 1, "progresso": "", "mensagem": "", "fim": None,
        "criado_em": _dt.datetime(2026, 10, 1, 12)})
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert 'data-pendente="1"' in html and 'data-id="9"' in html
    assert "1 pedido(s) antes deste" in html


def test_quem_so_tem_a_FOLHA_entra_pelo_endereco_do_modulo_SEM_ERRO(app, monkeypatch):
    """O dono, 01/10/2026: *"disponibilizei para uma pessoa uma única tela, Folha
    de pagamento. Quando ela entra dá uma mensagem de erro."* O endereço do
    módulo mandava sempre para as Solicitações, que ela não tem."""
    from app.apps.analisesps import auth, usuarios
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(usuarios, "buscar_por_id", lambda uid: {
        "id": 7, "login": "ana", "nome": "ANA", "telas": ["folha"],
        "pode_operar": True, "mestre": False, "ativo": True})
    c = app.test_client()
    with c.session_transaction() as s:
        s[auth.CHAVE_SESSAO] = auth.OPERADOR
        s[auth.CHAVE_USUARIO] = 7
        s[auth.CHAVE_NOME] = "ANA"
    r = c.get("/analisesps/")
    assert r.status_code == 302 and r.headers["Location"].endswith("/analisesps/folha")
    assert c.get("/analisesps/", follow_redirects=True).status_code == 200
    # A marca do topo leva ao início, não às Solicitações.
    html = c.get("/analisesps/folha/1").get_data(as_text=True)
    assert 'class="topo-marca" href="/analisesps/"' in html


def test_cadastro_SEM_TELA_nenhuma_recebe_recado_e_nao_um_laco(app, monkeypatch):
    from app.apps.analisesps import auth, usuarios
    monkeypatch.setattr(usuarios, "buscar_por_id", lambda uid: {
        "id": 8, "login": "bia", "nome": "BIA", "telas": [],
        "pode_operar": False, "mestre": False, "ativo": True})
    c = app.test_client()
    with c.session_transaction() as s:
        s[auth.CHAVE_SESSAO] = auth.CONSULTA
        s[auth.CHAVE_USUARIO] = 8
        s[auth.CHAVE_NOME] = "BIA"
    r = c.get("/analisesps/")
    assert r.status_code == 403 and "Nenhuma tela liberada" in r.get_data(as_text=True)


# ---------------------------------------------------------------------------
# 01/10/2026 — "e se quisermos colocar outra obra? Deveríamos poder abrir algum
# modal de seleção" (quem não tem ponto, ao lado do "usar esta")
# ---------------------------------------------------------------------------
def test_quem_nao_tem_ponto_ganha_OUTRA_OBRA_com_a_lista_da_C_DIARIOS(app, monkeypatch):
    from app.apps.analisesps import ponto_edicao
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(ponto_edicao, "obras_permitidas",
                        lambda: ["CRE1", "CREPEIGARASSU1", "SEDE"])
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert 'class="link-btn escolher-obra"' in html and "outra obra…" in html
    assert 'id="dialogo-obra"' in html
    assert '<option value="CREPEIGARASSU1">' in html
    # A sugestão de digitar a obra (ajustar) também conhece a lista da C. Diários.
    lista = html[html.index('<datalist id="obras-da-folha">'):]
    assert '<option value="SEDE">' in lista[:lista.index("</datalist>")]
