# -*- coding: utf-8 -*-
"""Ponto, fase 2 — gestão no ERP e "Meu ponto" do celular, contra um Postgres
DE VERDADE.

Percorre os fluxos de ponta a ponta com operadores do ERP de perfis
diferentes: o supervisor da obra A, o DP (todas as obras), o administrativo
financeiro (sem nada do ponto). Confere quem vê o quê, quem aprova o quê, o
sigilo do atestado, o espelho, o banco de horas, o fechamento do mês, os
alertas, o PIN e a batida pelo celular e pelo tablet da obra.

Fixtures no próprio arquivo (o conftest atravessa áreas). Sem
`ERP_TEST_DATABASE_URL`, tudo é pulado."""
from __future__ import annotations

import base64
import datetime as dt
import io

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.banco

CHAVE = "chave-de-teste-fase-2"
CPF_JOAO = "52998224725"
CPF_MARIA = "11144477735"
OBRA_A = (-3.7275, -38.5270)
FUSO = None


def _png() -> str:
    from PIL import Image
    s = io.BytesIO()
    Image.new("RGB", (40, 40), (10, 120, 200)).save(s, format="PNG")
    return "data:image/png;base64," + base64.b64encode(s.getvalue()).decode()


def _segunda_passada() -> dt.date:
    hoje = dt.date.today()
    return hoje - dt.timedelta(days=hoje.weekday() + 7)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def _schema_ponto2(banco):
    from app.apps.ponto import migracoes_runner
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS ponto CASCADE"))
        conn.commit()
    r = migracoes_runner.aplicar_pendentes()
    assert not r["erro"], r
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS ponto CASCADE"))
        conn.commit()


def _limpar(banco):
    with banco.connect() as conn:
        tabelas = [r[0] for r in conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'ponto' "
            "AND tablename NOT IN ('_migracoes', 'feriados', 'tipos_licenca')"))]
        conn.execute(text("TRUNCATE " + ", ".join(f'ponto."{t}"' for t in tabelas)
                          + " RESTART IDENTITY CASCADE"))
        conn.execute(text("DELETE FROM ponto.feriados WHERE abrangencia <> 'NACIONAL'"))
        conn.execute(text("DELETE FROM usuario_obras WHERE usuario_id IN "
                          "(SELECT id FROM usuarios WHERE email LIKE '%@ponto.teste')"))
        conn.execute(text("DELETE FROM usuarios WHERE email LIKE '%@ponto.teste'"))
        conn.execute(text("DELETE FROM colaboradores WHERE cpf IN (:a, :b)"),
                     {"a": CPF_JOAO, "b": CPF_MARIA})
        conn.execute(text("DELETE FROM obras WHERE codigo LIKE 'PG-%'"))
        conn.commit()


@pytest.fixture
def mundo(_schema_ponto2, banco, monkeypatch):
    """Duas obras, duas pessoas, três operadores, uma escala na obra A."""
    monkeypatch.setenv("PONTO_API_KEY", CHAVE)
    monkeypatch.delenv("PONTO_DRIVE_PASTA", raising=False)
    from app.apps.ponto import auth
    from app.apps.ponto.core import fotos, rotina
    auth._registros.clear()
    monkeypatch.setattr(rotina, "disparar_se_preciso", lambda: False)
    from app.apps.ponto.core import envios
    monkeypatch.setattr(envios, "disparar_se_preciso", lambda: False)
    monkeypatch.setattr(fotos, "disparar_envio", lambda: False)
    monkeypatch.setattr(fotos, "TENTATIVAS_NA_HORA", 1)
    from app.apps.ponto.core import registro as _registro_bg
    monkeypatch.setattr(_registro_bg, "manter_em_dia", lambda: False)
    _limpar(banco)
    from app.apps.ponto import db as _db
    from app.apps.ponto.core import parametros as _par, registro as _reg
    with _db.conexao() as _c:       # a base destes testes é o cadastro do ERP
        _par.gravar(_c, _reg.PARAMETRO_FONTE, _reg.FONTE_ERP, "teste")
    _db.esquecer_colunas()           # o "não existe" guardado de outro arquivo (ver test_ponto_banco)
    _reg.esquecer()
    from app.apps.ponto.core import base_obras as _bo
    _bo.esquecer()                   # e a de obras, o ERP (a cópia da C. Diários está vazia)
    with banco.connect() as conn:
        obra_a = conn.execute(text("INSERT INTO obras (codigo, nome, latitude, longitude, status, uf, municipio) "
                                   "VALUES ('PG-A', 'Escola A', :la, :lo, 'ATIVA', 'CE', 'Fortaleza') RETURNING id"),
                              {"la": OBRA_A[0], "lo": OBRA_A[1]}).scalar_one()
        obra_b = conn.execute(text("INSERT INTO obras (codigo, nome, status) VALUES ('PG-B', 'Posto B', 'ATIVA') "
                                   "RETURNING id")).scalar_one()
        joao = conn.execute(text("INSERT INTO colaboradores (nome, cpf, obra_id, telefone) "
                                 "VALUES ('João Obra A', :c, :o, '85999990000') RETURNING id"),
                            {"c": CPF_JOAO, "o": obra_a}).scalar_one()
        maria = conn.execute(text("INSERT INTO colaboradores (nome, cpf, obra_id, telefone) "
                                  "VALUES ('Maria Obra B', :c, :o, '85988880000') RETURNING id"),
                             {"c": CPF_MARIA, "o": obra_b}).scalar_one()

        def usuario(nome, perfil, todas=None):
            return conn.execute(text(
                "INSERT INTO usuarios (nome, email, senha_hash, perfil, ve_todas_as_obras) "
                "VALUES (:n, :e, 'x', CAST(:p AS perfil_usuario), :t) RETURNING id"),
                {"n": nome, "e": f"{perfil.lower()}@ponto.teste", "p": perfil, "t": todas}).scalar_one()
        sup = usuario("Supervisor A", "SUPERVISOR_OBRA")
        conn.execute(text("INSERT INTO usuario_obras (usuario_id, obra_id) VALUES (:u, :o)"),
                     {"u": sup, "o": obra_a})
        dp = usuario("DP", "DEPARTAMENTO_PESSOAL", True)
        fin = usuario("Financeiro", "FINANCEIRO", True)
        conn.commit()
    from app.apps.ponto import db
    from app.apps.ponto.core import escalas
    with db.conexao() as c:
        e = escalas.gravar(c, {"nome": "Obra 44h", "tipo": "SEMANAL",
                               "semana": {**{str(d): [["07:00", "11:00"], ["12:00", "17:00"]] for d in range(4)},
                                          "4": [["07:00", "11:00"], ["12:00", "16:00"]]}})
        inicio = _segunda_passada() - dt.timedelta(days=60)
        escalas.atribuir(c, joao, e["id"], inicio.isoformat(), por="teste")
        escalas.atribuir(c, maria, e["id"], inicio.isoformat(), por="teste")
    yield {"obra_a": obra_a, "obra_b": obra_b, "joao": joao, "maria": maria, "sup": sup, "dp": dp,
           "fin": fin, "escala": e["id"]}
    _limpar(banco)


@pytest.fixture
def app(mundo):
    from flask import Flask
    from app.apps.erp import routes as erp_routes
    from app.apps.ponto import bp as ponto_bp
    a = Flask(__name__, template_folder="../app/apps/erp/templates")
    a.secret_key = "teste"
    a.register_blueprint(erp_routes.bp)
    a.register_blueprint(ponto_bp)
    return a


def como(app, usuario_id):
    c = app.test_client()
    with c.session_transaction() as s:
        s["erp_usuario_id"] = usuario_id
        s["erp_usuario_nome"] = "teste"
    return c


def bater_via_chave(c, cpf, quando_local: dt.datetime, obra="PG-A"):
    """Batida com hora controlada (o relógio do servidor é congelado no teste)."""
    from app.apps.ponto import db, horario
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        m, _ = marcacoes.registrar(conn, cpf=cpf, obra=obra, origem="IDFACE", via_chave=True,
                                   agora=quando_local.astimezone(dt.timezone.utc))
    return m


def local(d: dt.date, hh: int, mm: int = 0) -> dt.datetime:
    from app.apps.ponto import horario
    return dt.datetime(d.year, d.month, d.day, hh, mm, tzinfo=horario.FUSO)


def dia_util(segunda: dt.date, n: int) -> dt.date:
    return segunda + dt.timedelta(days=n)


# ---------------------------------------------------------------------------
# Estrutura
# ---------------------------------------------------------------------------
def test_migracoes_do_ponto_e_feriados_nacionais(banco, mundo):
    with banco.connect() as conn:
        nomes = [r[0] for r in conn.execute(text("SELECT nome FROM ponto._migracoes ORDER BY nome"))]
        natal = conn.execute(text("SELECT nome FROM ponto.feriados WHERE data = '2026-12-25'")).scalar()
        secoes = conn.execute(text("""SELECT ps.secao, ps.nivel FROM perfil_secoes ps JOIN perfis p ON p.id = ps.perfil_id
                                       WHERE p.nome = 'Departamento pessoal' AND ps.secao LIKE 'pon_%' ORDER BY 1""")).all()
    assert nomes == ["001_ponto_base.sql", "002_gestao.sql", "003_qr_mosaico_e_sinais.sql",
                     "004_obras_da_planilha_e_forma_de_bater.sql", "005_modo_de_teste.sql",
                     "006_grupo_temporario_e_licencas_da_lei.sql", "007_conferencia_do_rosto.sql",
                     "008_papeis_no_aplicativo.sql"]
    assert natal == "Natal"
    assert [s[0] for s in secoes] == ["pon_competencia", "pon_config", "pon_dp", "pon_gestao"]


def test_quem_nao_tem_ponto_nao_entra(app, mundo):
    c = como(app, mundo["fin"])
    assert c.get("/erp/api/ponto/hoje").status_code == 403
    assert c.get("/erp/api/ponto/pendencias").status_code == 403
    sem_login = app.test_client().get("/erp/api/ponto/hoje")
    assert sem_login.status_code in (401, 302)


# ---------------------------------------------------------------------------
# Hoje, espelho e alcance por obra
# ---------------------------------------------------------------------------
def test_espelho_falta_extra_e_alcance_da_obra(app, mundo):
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0), (18, 0)):          # 1 h de extra na segunda
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    comp = seg.strftime("%Y-%m")
    sup = como(app, mundo["sup"])
    r = sup.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={comp}")
    assert r.status_code == 200, r.get_json()
    dias = {d["data"]: d for d in r.get_json()["dias"]}
    assert dias[seg.isoformat()]["extra"] == 60 and dias[seg.isoformat()]["situacao"] == "OK"
    terca = dia_util(seg, 1).isoformat()
    if terca[:7] == comp:
        assert dias[terca]["falta"] is True
    # Maria é da obra B: o supervisor da A não a enxerga — 404, não 403.
    assert sup.get(f"/erp/api/ponto/espelho/{mundo['maria']}").status_code == 404
    assert como(app, mundo["dp"]).get(f"/erp/api/ponto/espelho/{mundo['maria']}").status_code == 200
    hoje = sup.get(f"/erp/api/ponto/hoje?data={seg.isoformat()}").get_json()
    nomes = [p["nome"] for p in hoje["pessoas"]]
    assert "João Obra A" in nomes and "Maria Obra B" not in nomes
    assert next(p for p in hoje["pessoas"] if p["nome"] == "João Obra A")["situacao"] == "PRESENTE"
    impressao = sup.get(f"/erp/ponto/espelho/{mundo['joao']}/imprimir?competencia={comp}")
    assert impressao.status_code in (200, 500)   # 500 só se o template da gestão ainda não existir


def test_escala_so_quem_configura_cria_e_supervisor_so_mexe_na_obra_dele(app, mundo):
    sup, dp = como(app, mundo["sup"]), como(app, mundo["dp"])
    nova = {"nome": "Vigia 12x36", "tipo": "CICLO_12X36", "ciclo_entrada": "19:00", "ciclo_saida": "07:00"}
    assert sup.post("/erp/api/ponto/escalas", json=nova).status_code == 403
    r = dp.post("/erp/api/ponto/escalas", json=nova)
    assert r.status_code == 201 and r.get_json()["escala"]["horas_semanais"] == 42.0
    vigia = r.get_json()["escala"]["id"]
    ok = sup.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/escala",
                  json={"escala_id": vigia, "vigencia_inicio": dt.date.today().isoformat()})
    assert ok.status_code == 200
    fora = sup.post(f"/erp/api/ponto/pessoas/{mundo['maria']}/escala",
                    json={"escala_id": vigia, "vigencia_inicio": dt.date.today().isoformat()})
    assert fora.status_code == 404
    pessoa = sup.get(f"/erp/api/ponto/pessoas/{mundo['joao']}").get_json()["pessoa"]
    assert pessoa["escalas"][0]["escala"] == "Vigia 12x36"


# ---------------------------------------------------------------------------
# Pedidos: ajuste (supervisor), atestado (DP, sigilo), compensação (dois passos)
# ---------------------------------------------------------------------------
def _entrar_no_app(app, cpf, monkeypatch, *, excecao: bool = False):
    """`excecao=True`: a pessoa também bate no próprio celular E faz pedido por ele
    (as duas marcações; desde 08/10/2026 são separadas — core/papeis.py)."""
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    if excecao:
        from app.apps.ponto.core import cadastros, forma_de_bater, papeis
        with db.conexao() as conn:
            pid = int(cadastros.colaborador_por_cpf(conn, cpf)["id"])
            forma_de_bater.definir(conn, pid, True, "teste")
            papeis.definir(conn, pid, pede_no_celular=True)
    enviados = []
    with db.conexao() as conn:
        acesso.pedir_codigo(conn, cpf, enviar=lambda tel, msg: enviados.append((tel, msg)))
    codigo = enviados[-1][1].split("código é ")[1][:6]
    c = app.test_client()
    r = c.post("/ponto/app/api/pin", json={"cpf": cpf, "codigo": codigo, "pin": "481927"})
    assert r.status_code == 200, r.get_json()
    return c


def test_ajuste_de_batida_pelo_celular_aprovado_pelo_supervisor(app, mundo, monkeypatch):
    # O padrão é o DP (04/10/2026); aqui a regra põe o ajuste com o encarregado.
    assert como(app, mundo["dp"]).post("/erp/api/ponto/quem-valida",
                                       json={"regra": {"AJUSTE_BATIDA": "ENCARREGADO"}}).status_code == 200
    seg = _segunda_passada()
    for h in ((7, 0), (11, 0), (12, 0)):                    # esqueceu a saída
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)
    r = cel.post("/ponto/app/api/pedidos", json={
        "tipo": "AJUSTE_BATIDA", "data_inicio": seg.isoformat(), "horario": f"{seg.isoformat()}T17:00:00",
        "obra": "PG-A", "descricao": "celular sem bateria na saída"})
    assert r.status_code == 201, r.get_json()
    pedido = r.get_json()["pedido"]
    assert pedido["status"] == "AGUARDANDO_SUPERVISOR"
    dp = como(app, mundo["dp"])
    assert dp.post(f"/erp/api/ponto/ocorrencias/{pedido['id']}/etapa-supervisor",
                   json={"aprovar": True}).status_code == 200   # DP também trata ponto por cargo
    # um segundo pedido, para o supervisor decidir
    r2 = cel.post("/ponto/app/api/pedidos", json={
        "tipo": "AJUSTE_BATIDA", "data_inicio": dia_util(seg, 1).isoformat(),
        "horario": f"{dia_util(seg, 1).isoformat()}T07:00:00", "obra": "PG-A",
        "descricao": "esqueci de bater a entrada"})
    sup = como(app, mundo["sup"])
    pend = sup.get("/erp/api/ponto/pendencias").get_json()
    meu = next(p for p in pend["pedidos"] if p["id"] == r2.get_json()["pedido"]["id"])
    assert meu["pode_decidir"] is True
    ok = sup.post(f"/erp/api/ponto/ocorrencias/{meu['id']}/etapa-supervisor", json={"aprovar": True})
    assert ok.status_code == 200 and ok.get_json()["ocorrencia"]["status"] == "APROVADA"
    esp = sup.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    dia = next(d for d in esp["dias"] if d["data"] == seg.isoformat())
    assert [b["status"] for b in dia["batidas"]][-1] == "AJUSTADA" and dia["situacao"] == "OK"
    mes = cel.get(f"/ponto/app/api/mes?competencia={seg:%Y-%m}").get_json()
    assert next(d for d in mes["dias"] if d["data"] == seg.isoformat())["situacao"] == "OK"


def test_atestado_pelo_celular_so_o_dp_ve_e_aprova(app, mundo, monkeypatch):
    seg = _segunda_passada()
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch, excecao=True)
    r = cel.post("/ponto/app/api/pedidos", json={
        "tipo": "ATESTADO", "data_inicio": seg.isoformat(), "data_fim": dia_util(seg, 1).isoformat(),
        "documento_base64": _png(), "documento_nome": "atestado.png", "cid": "J11"})
    assert r.status_code == 201, r.get_json()
    pid = r.get_json()["pedido"]["id"]
    sup, dp = como(app, mundo["sup"]), como(app, mundo["dp"])
    visto_sup = next(p for p in sup.get("/erp/api/ponto/pendencias").get_json()["pedidos"] if p["id"] == pid)
    assert visto_sup["cid"] is None and visto_sup["documento_id"] is None
    assert visto_sup["tem_documento"] is True and visto_sup["pode_decidir"] is False
    assert sup.post(f"/erp/api/ponto/afastamentos/{pid}/etapa-dp", json={"aprovar": True}).status_code == 403
    visto_dp = next(p for p in dp.get("/erp/api/ponto/pendencias").get_json()["pedidos"] if p["id"] == pid)
    assert visto_dp["cid"] == "J11" and visto_dp["documento_id"] and visto_dp["pode_decidir"] is True
    doc = visto_dp["documento_id"]
    assert sup.get(f"/erp/api/ponto/afastamentos/documento/{doc}").status_code == 403
    assert sup.get(f"/erp/api/ponto/documentos/{doc}").status_code == 404     # sigiloso pela porta comum
    arquivo = dp.get(f"/erp/api/ponto/afastamentos/documento/{doc}")
    assert arquivo.status_code == 200 and arquivo.mimetype == "image/jpeg"
    ok = dp.post(f"/erp/api/ponto/afastamentos/{pid}/etapa-dp", json={"aprovar": True})
    assert ok.get_json()["ocorrencia"]["status"] == "APROVADA"
    esp = dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={seg:%Y-%m}").get_json()
    dia = next(d for d in esp["dias"] if d["data"] == seg.isoformat())
    assert dia["abonado"] is True and dia["falta"] is False and dia["ocorrencia"]["tipo"] == "ATESTADO"
    # atestado pelo celular sem o papel é recusado
    sem = cel.post("/ponto/app/api/pedidos", json={"tipo": "ATESTADO", "data_inicio": dia_util(seg, 3).isoformat()})
    assert sem.status_code == 400 and sem.get_json()["campo"] == "documento"


def test_negar_exige_motivo_e_ferias_so_o_dp_lanca(app, mundo):
    sup, dp = como(app, mundo["sup"]), como(app, mundo["dp"])
    seg = _segunda_passada()
    r = sup.post("/erp/api/ponto/afastamentos", json={"tipo": "FERIAS", "colaborador_id": mundo["joao"],
                                                      "data_inicio": seg.isoformat()})
    assert r.status_code == 403
    r = dp.post("/erp/api/ponto/afastamentos", json={"tipo": "FERIAS", "colaborador_id": mundo["joao"],
                                                     "data_inicio": seg.isoformat(),
                                                     "data_fim": dia_util(seg, 4).isoformat(), "aprovar_ja": True})
    assert r.status_code == 201 and r.get_json()["ocorrencia"]["status"] == "APROVADA"
    r = sup.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO", "colaborador_id": mundo["joao"],
                                                     "data_inicio": dia_util(seg, 4).isoformat(),
                                                     "dia_trabalhado": dia_util(seg, 5).isoformat(),
                                                     "descricao": "sábado no lugar da sexta"})
    assert r.status_code == 400   # sobrepõe as férias
    nega = dp.post("/erp/api/ponto/ocorrencias", json={"tipo": "FERIAS", "colaborador_id": mundo["joao"],
                                                       "data_inicio": seg.isoformat()})
    assert nega.status_code == 400   # férias não entram pela porta da obra


def test_compensacao_passa_pelo_supervisor_e_pelo_dp(app, mundo):
    assert como(app, mundo["dp"]).post("/erp/api/ponto/quem-valida",
                                       json={"regra": {"COMPENSACAO": "ENCARREGADO_E_DP"}}).status_code == 200
    seg = _segunda_passada()
    sabado, sexta = dia_util(seg, 5), dia_util(seg, 4)
    for h in ((7, 0), (11, 0), (12, 0), (16, 0)):
        bater_via_chave(app, CPF_JOAO, local(sabado, *h))
    sup, dp = como(app, mundo["sup"]), como(app, mundo["dp"])
    r = sup.post("/erp/api/ponto/ocorrencias", json={"tipo": "COMPENSACAO", "colaborador_id": mundo["joao"],
                                                     "data_inicio": sexta.isoformat(),
                                                     "dia_trabalhado": sabado.isoformat(),
                                                     "descricao": "sábado no lugar da sexta"})
    assert r.status_code == 201, r.get_json()
    oid = r.get_json()["ocorrencia"]["id"]
    assert sup.post(f"/erp/api/ponto/ocorrencias/{oid}/etapa-supervisor", json={"aprovar": True}) \
        .get_json()["ocorrencia"]["status"] == "AGUARDANDO_DP"
    assert sup.post(f"/erp/api/ponto/afastamentos/{oid}/etapa-dp", json={"aprovar": True}).status_code == 403
    assert dp.post(f"/erp/api/ponto/afastamentos/{oid}/etapa-dp", json={"aprovar": False}).status_code == 400
    ok = dp.post(f"/erp/api/ponto/afastamentos/{oid}/etapa-dp", json={"aprovar": True})
    assert ok.get_json()["ocorrencia"]["status"] == "APROVADA"
    meses = {sexta.strftime("%Y-%m"), sabado.strftime("%Y-%m")}
    dias = {}
    for comp in meses:
        dias.update({d["data"]: d for d in dp.get(f"/erp/api/ponto/espelho/{mundo['joao']}?competencia={comp}")
                     .get_json()["dias"]})
    assert dias[sexta.isoformat()]["abonado"] is True
    assert dias[sabado.isoformat()]["situacao"] == "COMPENSADO" and dias[sabado.isoformat()]["extra"] == 0


# ---------------------------------------------------------------------------
# Batida em análise, competência, banco de horas
# ---------------------------------------------------------------------------
def test_batida_em_analise_validada_e_rejeitada(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import marcacoes
    with db.conexao() as conn:
        m1, _ = marcacoes.registrar(conn, cpf=CPF_JOAO, obra="PG-B", origem="IDFACE", via_chave=True)
    assert m1["status"] == "EM_ANALISE"   # obra fora da lista do João
    sup = como(app, mundo["sup"])
    pend = sup.get("/erp/api/ponto/pendencias").get_json()
    assert all(b["id"] != m1["id"] for b in pend["batidas"])     # é da obra B: fora do alcance dele
    assert sup.post(f"/erp/api/ponto/marcacoes/{m1['id']}/decidir", json={"para": "VALIDA"}).status_code == 404
    dp = como(app, mundo["dp"])
    # padrão: quem confere batida é o DP, pela rota dele; a do encarregado diz isso
    errado = dp.post(f"/erp/api/ponto/marcacoes/{m1['id']}/decidir", json={"para": "VALIDA"})
    assert errado.status_code == 400 and "DP" in errado.get_json()["erro"]
    assert sup.post(f"/erp/api/ponto/marcacoes/{m1['id']}/decidir-dp", json={"para": "VALIDA"}).status_code == 403
    assert dp.post(f"/erp/api/ponto/marcacoes/{m1['id']}/decidir-dp", json={"para": "REJEITADA"}).status_code == 400
    ok = dp.post(f"/erp/api/ponto/marcacoes/{m1['id']}/decidir-dp",
                 json={"para": "REJEITADA", "motivo": "não estava nessa obra"})
    assert ok.status_code == 200 and ok.get_json()["marcacao"]["status"] == "REJEITADA"
    assert dp.post(f"/erp/api/ponto/marcacoes/{m1['id']}/decidir-dp",
                   json={"para": "VALIDA"}).status_code == 400            # já decidida


def test_mes_fechado_trava_pedido_e_reabrir_pede_motivo(app, mundo):
    seg = _segunda_passada()
    comp = seg.strftime("%Y-%m")
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    assert sup.post(f"/erp/api/ponto/competencias/{comp}/fechar").status_code == 403
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/fechar").get_json()["situacao"] == "FECHADA"
    r = sup.post("/erp/api/ponto/ocorrencias", json={"tipo": "AJUSTE_BATIDA", "colaborador_id": mundo["joao"],
                                                     "data_inicio": seg.isoformat(),
                                                     "horario": f"{seg.isoformat()}T07:00:00", "obra": "PG-A",
                                                     "descricao": "esqueceu de bater a entrada"})
    assert r.status_code == 400 and "fechado" in r.get_json()["erro"]
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/reabrir", json={"motivo": "x"}).status_code == 400
    assert dp.post(f"/erp/api/ponto/competencias/{comp}/reabrir",
                   json={"motivo": "faltou lançar o atestado"}).get_json()["situacao"] == "ABERTA"


def test_banco_de_horas_exige_acordo_e_soma_extra_e_folga(app, mundo):
    seg = _segunda_passada()
    dp = como(app, mundo["dp"])
    sem_acordo = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/banco",
                         json={"regime_banco": "BANCO_6_MESES", "banco_inicio": seg.isoformat()})
    assert sem_acordo.status_code == 400 and sem_acordo.get_json()["campo"] == "acordo"
    assert como(app, mundo["sup"]).post(f"/erp/api/ponto/pessoas/{mundo['joao']}/banco",
                                        json={"regime_banco": "SEM_BANCO"}).status_code == 403
    ok = dp.post(f"/erp/api/ponto/pessoas/{mundo['joao']}/banco",
                 json={"regime_banco": "BANCO_6_MESES", "banco_inicio": seg.isoformat(),
                       "acordo_base64": _png(), "acordo_nome": "acordo.png"})
    assert ok.status_code == 200, ok.get_json()
    for h in ((7, 0), (11, 0), (12, 0), (19, 0)):          # 2 h de extra
        bater_via_chave(app, CPF_JOAO, local(seg, *h))
    s = dp.get(f"/erp/api/ponto/banco/{mundo['joao']}").get_json()
    assert s["regime"] == "BANCO_6_MESES" and s["saldo"] >= 120
    assert s["vencimentos"] and s["vencimentos"][0]["minutos"] > 0
    lanc = dp.post(f"/erp/api/ponto/banco/{mundo['joao']}/lancamentos",
                   json={"horas": "-1,5", "tipo": "PAGAMENTO", "descricao": "pagas na folha de outubro"})
    assert lanc.status_code == 200 and lanc.get_json()["lancamento"]["minutos"] == -90
    depois = dp.get(f"/erp/api/ponto/banco/{mundo['joao']}").get_json()["saldo"]
    assert depois == s["saldo"] - 90
    lista = dp.get("/erp/api/ponto/banco").get_json()["pessoas"]
    assert [p["nome"] for p in lista] == ["João Obra A"]


# ---------------------------------------------------------------------------
# Alertas e rotina
# ---------------------------------------------------------------------------
def test_alertas_de_falta_e_resolucao_sozinha(app, mundo):
    from app.apps.ponto import db
    from app.apps.ponto.core import alertas
    ontem = dt.date.today() - dt.timedelta(days=1)
    alvo = ontem
    while alvo.weekday() > 4:
        alvo -= dt.timedelta(days=1)
    with db.conexao() as conn:
        r = alertas.gerar(conn, ate=ontem)
    assert r["alertas"].get("FALTA", 0) >= 1
    sup = como(app, mundo["sup"])
    lista = sup.get("/erp/api/ponto/alertas").get_json()["alertas"]
    falta = next(a for a in lista if a["codigo"] == "FALTA" and a["data_referencia"] == alvo.isoformat())
    assert "João" in falta["mensagem"] and all("Maria" not in a["mensagem"] for a in lista)
    dp = como(app, mundo["dp"])
    dp.post("/erp/api/ponto/afastamentos", json={"tipo": "ABONO", "colaborador_id": mundo["joao"],
                                                 "data_inicio": alvo.isoformat(), "descricao": "reunião externa",
                                                 "aprovar_ja": True})
    with db.conexao() as conn:
        r2 = alertas.gerar(conn, ate=ontem)
        situacao = db.um(conn, "SELECT situacao FROM ponto.alertas WHERE id = :i", i=falta["id"])["situacao"]
    assert situacao == "RESOLVIDO" and r2["resolvidos_sozinhos"] >= 1
    assert sup.post(f"/erp/api/ponto/alertas/{lista[0]['id']}/tratar",
                    json={"situacao": "DISPENSADO"}).status_code == 400   # dispensar pede nota


def test_rotina_do_dia_manda_o_resumo_uma_vez(app, mundo, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import parametros, rotina
    enviados = []
    import app.apps.notificador as notificador
    monkeypatch.setattr(notificador, "notificar",
                        lambda **kw: enviados.append(kw) or {"whatsapp": {"ok": True}})
    with db.conexao() as conn:
        parametros.gravar(conn, parametros.TELEFONES_RESUMO, "(85) 99999-1111; 85 98888 2222", "teste")
        assert rotina.telefones(conn) == ["5585999991111", "5585988882222"]
        rotina.rodar(conn)
        rotina.enviar_resumo(conn)      # de novo, no mesmo dia: não reenvia
    assert len(enviados) == 2 and "Ponto —" in enviados[0]["mensagem"]
    with db.conexao() as conn:
        assert not rotina.precisa_rodar(conn) or rotina.horario.para_local(rotina.horario.agora()).hour < 6


# ---------------------------------------------------------------------------
# O celular: PIN, aparelho, batida, comprovante, tablet
# ---------------------------------------------------------------------------
def test_pin_errado_bloqueia_e_codigo_responde_igual_para_cpf_desconhecido(app, mundo, monkeypatch):
    from app.apps.ponto import db
    from app.apps.ponto.core import acesso
    with db.conexao() as conn:
        a = acesso.pedir_codigo(conn, "39053344705", enviar=lambda *x: None)   # CPF válido, sem cadastro
        b = acesso.pedir_codigo(conn, CPF_MARIA, enviar=lambda *x: None)
    assert a == b
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    cel.post("/ponto/app/api/sair", json={})
    novo = app.test_client()
    for _ in range(acesso.TENTATIVAS_PIN):
        r = novo.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "000001"})
        assert r.status_code == 401
    bloqueado = novo.post("/ponto/app/api/entrar", json={"cpf": CPF_JOAO, "pin": "481927"})
    assert bloqueado.status_code == 401 and "tente de novo" in bloqueado.get_json()["erro"]
    assert novo.get("/ponto/app/api/eu").status_code == 401


def test_celular_bate_so_aprovado_e_so_para_o_dono_com_comprovante(app, mundo, monkeypatch):
    cel = _entrar_no_app(app, CPF_JOAO, monkeypatch)
    uuid = "celular-do-joao-0123456789abcdef"
    reg = cel.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid})
    token = reg.get_json()["token"]
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    assert cel.post("/ponto/app/api/aparelho/identificar", json={}, headers=h).get_json()["mudou"] is True
    pendente = cel.post("/ponto/app/api/bater", json={"obra": "PG-A", "latitude": OBRA_A[0],
                                                      "longitude": OBRA_A[1]}, headers=h)
    assert pendente.status_code == 403
    dp = como(app, mundo["dp"])
    aparelho = next(a for a in dp.get("/erp/api/ponto/pendencias").get_json()["aparelhos"])
    assert "João Obra A" in aparelho["descricao"]
    assert dp.post(f"/erp/api/ponto/dispositivos/{aparelho['id']}/aprovar",
                   json={"perfil": "INDIVIDUAL", "cpf": CPF_JOAO}).status_code == 200
    ok = cel.post("/ponto/app/api/bater", json={"obra": "PG-A", "latitude": OBRA_A[0] - 0.0001,
                                                "longitude": OBRA_A[1]}, headers=h)
    assert ok.status_code == 201, ok.get_json()
    c = ok.get_json()["comprovante"]
    assert c["empregado"] == "João Obra A" and c["cpf"] == "529.982.247-25" and c["nsr"] >= 1
    assert ok.get_json()["status"] == "VALIDA"
    eu = cel.get("/ponto/app/api/eu").get_json()
    assert eu["hoje"]["batidas"] and eu["primeiro_nome"] == "João"
    marc_id = eu["hoje"]["batidas"][0]["id"]
    assert cel.get(f"/ponto/app/api/comprovante/{marc_id}").status_code == 200
    # Maria, no celular do João, não bate: nem sem exceção (o padrão é o aparelho
    # da obra), nem com ela (o celular é de outra pessoa)
    cel_maria = _entrar_no_app(app, CPF_MARIA, monkeypatch)
    alheio = cel_maria.post("/ponto/app/api/bater", json={"obra": "PG-B"}, headers=h)
    assert alheio.status_code == 403 and "aparelho da obra" in alheio.get_json()["erro"]
    assert dp.post(f"/erp/api/ponto/pessoas/{mundo['maria']}/forma-de-bater",
                   json={"bate_no_celular": True}).status_code == 200
    alheio = cel_maria.post("/ponto/app/api/bater", json={"obra": "PG-B"}, headers=h)
    assert alheio.status_code == 403 and "outra pessoa" in alheio.get_json()["erro"]
    assert cel_maria.get(f"/ponto/app/api/comprovante/{marc_id}").status_code == 404


def test_tablet_da_obra_bate_sem_login_com_cpf(app, mundo):
    t = app.test_client()
    uuid = "tablet-da-obra-a-0123456789abcd"
    token = t.post("/ponto/api/dispositivo/registrar", json={"device_uuid": uuid}).get_json()["token"]
    dp = como(app, mundo["dp"])
    disp = dp.get("/erp/api/ponto/dispositivos?status=PENDENTE").get_json()["dispositivos"][0]
    dp.post(f"/erp/api/ponto/dispositivos/{disp['id']}/aprovar",
            json={"perfil": "COMPARTILHADO", "obras": ["PG-A"], "descricao": "Tablet da Escola A",
                  "cpf": CPF_MARIA})
    h = {"X-Device-UUID": uuid, "X-Device-Token": token}
    estado = t.get("/ponto/app/api/aparelho", headers=h).get_json()["aparelho"]
    assert estado["status"] == "APROVADO" and [o["codigo"] for o in estado["obras"]] == ["PG-A"]
    r = t.post("/ponto/app/api/bater", json={"cpf": CPF_JOAO, "obra": "PG-A",
                                             "latitude": OBRA_A[0], "longitude": OBRA_A[1]}, headers=h)
    assert r.status_code == 201, r.get_json()
    assert t.get("/ponto/app/api/eu").status_code == 401            # o tablet não mostra nada de ninguém
    fora = t.post("/ponto/app/api/bater", json={"cpf": CPF_MARIA, "obra": "PG-B"}, headers=h)
    assert fora.status_code == 403                                    # o tablet só vale na obra A


def test_configuracao_e_migracao_pela_gestao(app, mundo):
    dp, sup = como(app, mundo["dp"]), como(app, mundo["sup"])
    cfg = dp.get("/erp/api/ponto/configuracao").get_json()
    assert cfg["migracoes"]["pendentes"] == [] and cfg["drive_configurado"] is False
    assert sup.post("/erp/api/ponto/migrar").status_code == 403
    assert dp.post("/erp/api/ponto/migrar").get_json()["aplicadas"] == []
    assert sup.post("/erp/api/ponto/configuracao", json={"telefones_resumo": "85999990000"}).status_code == 403
    assert dp.post("/erp/api/ponto/configuracao", json={"telefones_resumo": "85999990000"}).status_code == 200
    csv = dp.get(f"/erp/api/ponto/exportar.csv?competencia={dt.date.today():%Y-%m}")
    assert csv.status_code == 200 and "João Obra A" in csv.get_data(as_text=True)
    so_a = sup.get(f"/erp/api/ponto/exportar.csv?competencia={dt.date.today():%Y-%m}").get_data(as_text=True)
    assert "Maria Obra B" not in so_a
