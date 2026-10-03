# -*- coding: utf-8 -*-
"""Ponto eletrônico — o fluxo inteiro contra um Postgres DE VERDADE.

Sobe o schema `ponto` com a migração real sobre o banco do ERP (já construído
pela fixture `banco` do conftest), cria obra e pessoa no ERP, registra e aprova
um aparelho, bate dentro e fora da cerca, consulta por período e confere NSR,
corrente de hash, recusas e o contrato com as tabelas do ERP.

As fixtures moram AQUI, e não no `conftest.py`, de propósito: o conftest
atravessa áreas, e o módulo ponto não altera nada fora dele sem o dono pedir.

Sem `ERP_TEST_DATABASE_URL`, são pulados e a suíte segue."""
from __future__ import annotations

import base64
import datetime as dt
import io
import os
from pathlib import Path

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.banco

CHAVE = "chave-de-teste-do-ponto"
UUID_TABLET = "tablet-obra-01-3f2504e04f8911d3"
UUID_CELULAR = "celular-joao-9a0c0305e82c3301aa"
CPF_JOAO = "52998224725"      # CPF válido de exemplo
CPF_MARIA = "11144477735"     # CPF válido de exemplo
CPF_DESLIGADO = "12345678909"
OBRA_LAT, OBRA_LON = -3.7275, -38.5270


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def _schema_ponto(banco):
    """Constrói o schema `ponto` UMA vez por sessão, pela migração de verdade."""
    from app.apps.ponto import migracoes_runner
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS ponto CASCADE"))
        conn.commit()
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado["erro"], resultado
    yield
    with banco.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS ponto CASCADE"))
        conn.commit()


@pytest.fixture
def ponto(_schema_ponto, banco, monkeypatch):
    """Tabelas do ponto vazias, cadastro mínimo no ERP e a chave configurada.
    Devolve um dicionário com os ids criados."""
    monkeypatch.setenv("PONTO_API_KEY", CHAVE)
    from app.apps.ponto import auth
    auth._registros.clear()
    _limpar(banco)
    with banco.connect() as conn:
        obra = conn.execute(text(
            "INSERT INTO obras (codigo, nome, latitude, longitude, status) "
            "VALUES ('PT-01', 'Escola do Bairro', :lat, :lon, 'ATIVA') RETURNING id"),
            {"lat": OBRA_LAT, "lon": OBRA_LON}).scalar_one()
        obra_sem_geo = conn.execute(text(
            "INSERT INTO obras (codigo, nome, status) VALUES ('PT-02', 'Posto sem mapa', 'ATIVA') "
            "RETURNING id")).scalar_one()
        obra_encerrada = conn.execute(text(
            "INSERT INTO obras (codigo, nome, status) VALUES ('PT-03', 'Obra velha', 'ENCERRADA') "
            "RETURNING id")).scalar_one()
        joao = conn.execute(text(
            "INSERT INTO colaboradores (nome, cpf, obra_id) VALUES ('João da Silva', :cpf, :o) "
            "RETURNING id"), {"cpf": CPF_JOAO, "o": obra}).scalar_one()
        maria = conn.execute(text(
            "INSERT INTO colaboradores (nome, cpf, obra_id) VALUES ('Maria Souza', :cpf, :o) "
            "RETURNING id"), {"cpf": CPF_MARIA, "o": obra}).scalar_one()
        desligado = conn.execute(text(
            "INSERT INTO colaboradores (nome, cpf, obra_id, situacao) "
            "VALUES ('Ex-colaborador', :cpf, :o, 'DESLIGADO') RETURNING id"),
            {"cpf": CPF_DESLIGADO, "o": obra}).scalar_one()
        conn.commit()
    yield {"obra": obra, "obra_sem_geo": obra_sem_geo, "obra_encerrada": obra_encerrada,
           "joao": joao, "maria": maria, "desligado": desligado}
    # ⚠️ LIMPA NA SAÍDA, não só na entrada. Estes testes gravam DE VERDADE (as
    # rotas confirmam a transação), e outros arquivos do mesmo trabalhador do
    # xdist contam as obras do banco esperando zero. Em 03/10/2026 as três obras
    # PT-* que sobravam derrubaram 11 testes de `test_obra_do_documento_banco.py`.
    _limpar(banco)


def _limpar(banco):
    with banco.connect() as conn:
        tabelas = [r[0] for r in conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'ponto' AND tablename <> '_migracoes'"))]
        if tabelas:
            conn.execute(text("TRUNCATE " + ", ".join(f'ponto."{t}"' for t in tabelas)
                              + " RESTART IDENTITY CASCADE"))
        conn.execute(text("DELETE FROM colaboradores WHERE cpf IN (:a, :b, :c, :d)"),
                     {"a": CPF_JOAO, "b": CPF_MARIA, "c": CPF_DESLIGADO, "d": "39053344705"})
        conn.execute(text("DELETE FROM obras WHERE codigo LIKE 'PT-%'"))
        conn.commit()


@pytest.fixture
def cliente(ponto):
    from flask import Flask
    from app.apps.ponto import routes
    a = Flask(__name__)
    a.register_blueprint(routes.bp)
    return a.test_client()


def com_chave(**extra):
    return {"X-API-Key": CHAVE, **extra}


def registrar_aparelho(cliente, uuid=UUID_TABLET, descricao="Tablet da obra"):
    r = cliente.post("/ponto/api/dispositivo/registrar",
                     json={"device_uuid": uuid, "descricao": descricao})
    assert r.status_code == 201, r.get_json()
    corpo = r.get_json()
    return corpo["dispositivo"]["id"], corpo["token"]


def aprovar(cliente, dispositivo_id, **dados):
    base = {"perfil": "COMPARTILHADO", "aprovado_por": "Teste"}
    base.update(dados)
    r = cliente.post(f"/ponto/api/dispositivos/{dispositivo_id}/aprovar", json=base,
                     headers=com_chave())
    assert r.status_code == 200, r.get_json()
    return r.get_json()["dispositivo"]


def bater(cliente, token, uuid=UUID_TABLET, cpf=CPF_JOAO, obra="PT-01",
          lat=-3.7276, lon=-38.5271, **extra):
    corpo = {"device_uuid": uuid, "cpf": cpf, "obra": obra, "latitude": lat, "longitude": lon}
    corpo.update(extra)
    return cliente.post("/ponto/api/marcacao", json=corpo, headers={"X-Device-Token": token})


# ---------------------------------------------------------------------------
# Migração e contrato com o ERP
# ---------------------------------------------------------------------------
def test_a_migracao_roda_e_nao_tem_pendentes(cliente):
    r = cliente.get("/ponto/api/admin/migracoes", headers=com_chave())
    assert r.status_code == 200
    estado = r.get_json()
    assert estado["pendentes"] == [] and estado["aplicadas"][0]["nome"] == "001_ponto_base.sql"
    saude = cliente.get("/ponto/health").get_json()
    assert saude["banco"] == "ok" and saude["migracoes_pendentes"] == 0


def test_contrato_com_o_erp(banco, ponto):
    """As colunas do ERP que o ponto lê. Se o ERP renomear uma, falha AQUI."""
    with banco.connect() as conn:
        colunas = {(r[0], r[1]) for r in conn.execute(text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name IN ('obras', 'colaboradores')"))}
    from app.apps.erp.core.documentos import drive as drive_erp
    for nome in ("enviar", "_servico", "_procurar_pasta", "ErroDrive"):
        assert hasattr(drive_erp, nome), f"o ponto usa erp.core.documentos.drive.{nome}"
    for esperada in [("obras", "id"), ("obras", "codigo"), ("obras", "nome"), ("obras", "status"),
                     ("obras", "latitude"), ("obras", "longitude"),
                     ("colaboradores", "id"), ("colaboradores", "nome"), ("colaboradores", "cpf"),
                     ("colaboradores", "obra_id"), ("colaboradores", "situacao"),
                     ("colaboradores", "matricula")]:
        assert esperada in colunas, f"o ponto lê {esperada} e ela não existe mais no ERP"


# ---------------------------------------------------------------------------
# Aparelhos
# ---------------------------------------------------------------------------
def test_registrar_aparelho_entra_pendente_com_token_unico(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    assert len(token) >= 40
    de_novo = cliente.post("/ponto/api/dispositivo/registrar", json={"device_uuid": UUID_TABLET})
    assert de_novo.status_code == 200
    assert de_novo.get_json()["novo"] is False and "token" not in de_novo.get_json()
    lista = cliente.get("/ponto/api/dispositivos?status=PENDENTE", headers=com_chave()).get_json()
    assert lista["quantidade"] == 1 and lista["dispositivos"][0]["id"] == dispositivo_id


def test_registro_sem_uuid_e_400(cliente):
    r = cliente.post("/ponto/api/dispositivo/registrar", json={"device_uuid": "x"})
    assert r.status_code == 400 and r.get_json()["campo"] == "device_uuid"


def test_aparelho_pendente_nao_bate_e_a_recusa_fica_registrada(cliente):
    _, token = registrar_aparelho(cliente)
    r = bater(cliente, token)
    assert r.status_code == 403 and "pendente" in r.get_json()["erro"]
    recusas = cliente.get("/ponto/api/recusas", headers=com_chave()).get_json()["recusas"]
    assert len(recusas) == 1 and recusas[0]["device_uuid"] == UUID_TABLET
    assert recusas[0]["cpf_informado"] == CPF_JOAO
    marcacoes = cliente.get("/ponto/api/marcacoes?data_inicio=2000-01-01&data_fim=2000-01-31",
                            headers=com_chave()).get_json()
    assert marcacoes["quantidade"] == 0


def test_token_errado_e_recusado_sem_dizer_se_o_uuid_existe(cliente):
    registrar_aparelho(cliente)
    r = bater(cliente, "token-chutado")
    assert r.status_code == 403 and r.get_json()["erro"] == "aparelho desconhecido ou token inválido"
    r2 = bater(cliente, "token-chutado", uuid="uuid-que-nao-existe-0123456789")
    assert r2.get_json()["erro"] == r.get_json()["erro"]


def test_aprovar_individual_exige_dono_e_so_o_dono_bate(cliente, ponto):
    dispositivo_id, token = registrar_aparelho(cliente, UUID_CELULAR, "Celular do João")
    r = cliente.post(f"/ponto/api/dispositivos/{dispositivo_id}/aprovar",
                     json={"perfil": "INDIVIDUAL", "aprovado_por": "RH"}, headers=com_chave())
    assert r.status_code == 400 and r.get_json()["campo"] == "colaborador_id"
    aparelho = aprovar(cliente, dispositivo_id, perfil="INDIVIDUAL", cpf=CPF_JOAO)
    assert aparelho["status"] == "APROVADO" and aparelho["dono"]["nome"] == "João da Silva"
    assert bater(cliente, token, uuid=UUID_CELULAR).status_code == 201
    r = bater(cliente, token, uuid=UUID_CELULAR, cpf=CPF_MARIA)
    assert r.status_code == 403 and "outra pessoa" in r.get_json()["erro"]


def test_lista_e_obras_do_aparelho(cliente, ponto):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id, perfil="LISTA", autorizados=[CPF_MARIA], obras=["PT-01"])
    assert bater(cliente, token, cpf=CPF_MARIA).status_code == 201
    assert "fora da lista" in bater(cliente, token, cpf=CPF_JOAO).get_json()["erro"]
    r = bater(cliente, token, cpf=CPF_MARIA, obra="PT-02")
    assert r.status_code == 403 and "não vale nesta obra" in r.get_json()["erro"]
    # troca a lista: agora João entra, Maria sai
    r = cliente.post(f"/ponto/api/dispositivos/{dispositivo_id}/autorizar",
                     json={"autorizados": [CPF_JOAO]}, headers=com_chave())
    assert r.status_code == 200 and r.get_json()["dispositivo"]["qtd_autorizados"] == 1
    assert bater(cliente, token, cpf=CPF_JOAO).status_code == 201


def test_bloquear_mata_o_token(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    assert bater(cliente, token).status_code == 201
    r = cliente.post(f"/ponto/api/dispositivos/{dispositivo_id}/bloquear",
                     json={"motivo": "aparelho perdido", "por": "RH"}, headers=com_chave())
    assert r.status_code == 200 and r.get_json()["dispositivo"]["status"] == "BLOQUEADO"
    r = bater(cliente, token, cpf=CPF_MARIA)
    assert r.status_code == 403 and "bloqueado" in r.get_json()["erro"]


def test_aparelho_inexistente_e_404(cliente):
    r = cliente.post("/ponto/api/dispositivos/999/bloquear", json={"motivo": "x", "por": "y"},
                     headers=com_chave())
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# A batida
# ---------------------------------------------------------------------------
def test_batida_dentro_da_cerca_e_valida_com_nsr_e_corrente(cliente, banco):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, timestamp_dispositivo=dt.datetime.now(dt.timezone.utc).isoformat())
    assert r.status_code == 201, r.get_json()
    m = r.get_json()["marcacao"]
    assert m["status"] == "VALIDA" and m["motivo_analise"] is None
    assert m["nsr"] == 1 and m["dentro_da_cerca"] is True and m["distancia_metros"] < 30
    assert m["obra"]["codigo"] == "PT-01" and m["origem"] == "PWA" and m["dispositivo_id"] == dispositivo_id
    assert m["horario"].endswith("-03:00")
    assert m["data_referencia"] == dt.date.fromisoformat(m["horario"][:10]).isoformat()
    # a segunda batida (de outra pessoa) encadeia com a primeira
    r2 = bater(cliente, token, cpf=CPF_MARIA)
    m2 = r2.get_json()["marcacao"]
    assert m2["nsr"] == 2 and m2["hash"] != m["hash"]
    with banco.connect() as conn:
        linhas = conn.execute(text("SELECT nsr, hash_encadeado FROM ponto.marcacoes ORDER BY nsr")).all()
    assert [l[0] for l in linhas] == [1, 2]
    # aparelho registra o último uso
    lista = cliente.get("/ponto/api/dispositivos", headers=com_chave()).get_json()["dispositivos"]
    assert lista[0]["ultimo_uso_em"] is not None


def test_batida_repetida_em_60s_devolve_a_mesma(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    primeira = bater(cliente, token)
    segunda = bater(cliente, token)
    assert primeira.status_code == 201 and segunda.status_code == 200
    assert segunda.get_json()["repetida"] is True
    assert segunda.get_json()["marcacao"]["nsr"] == primeira.get_json()["marcacao"]["nsr"]


def test_fora_da_cerca_e_aceita_em_analise(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, lat=-3.7400, lon=-38.5270)   # ~1,4 km ao sul
    assert r.status_code == 201
    m = r.get_json()["marcacao"]
    assert m["status"] == "EM_ANALISE" and m["dentro_da_cerca"] is False
    assert "fora da cerca" in m["motivo_analise"] and m["distancia_metros"] > 1000


def test_obra_sem_coordenada_e_celular_sem_localizacao_vao_para_analise(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, obra="PT-02")
    assert r.status_code == 201
    m = r.get_json()["marcacao"]
    assert m["status"] == "EM_ANALISE" and "sem coordenada" in m["motivo_analise"]
    assert "fora da lista da pessoa" in m["motivo_analise"]   # PT-02 não é obra do João
    r = bater(cliente, token, cpf=CPF_MARIA, lat=None, lon=None)
    assert "sem localização" in r.get_json()["marcacao"]["motivo_analise"]


def test_relogio_do_aparelho_fora_da_tolerancia(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    atrasado = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=20)).isoformat()
    m = bater(cliente, token, timestamp_dispositivo=atrasado).get_json()["marcacao"]
    assert m["status"] == "EM_ANALISE" and "relógio" in m["motivo_analise"]
    assert m["timestamp_dispositivo"] is not None


def test_pessoa_desligada_e_obra_encerrada_sao_recusadas(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, cpf=CPF_DESLIGADO)
    assert r.status_code == 403 and "desligada" in r.get_json()["erro"]
    r = bater(cliente, token, obra="PT-03")
    assert r.status_code == 403 and "encerrada" in r.get_json()["erro"]
    r = bater(cliente, token, cpf="39053344705")   # CPF válido, ninguém com ele
    assert r.status_code == 403 and "não cadastrada" in r.get_json()["erro"]
    assert cliente.get("/ponto/api/recusas", headers=com_chave()).get_json()["quantidade"] == 3


def test_entrada_ruim_e_400_com_o_campo(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    assert bater(cliente, token, cpf="123").get_json()["campo"] == "cpf"
    assert bater(cliente, token, lat="norte", lon=1).get_json()["campo"] == "latitude"
    assert bater(cliente, token, timestamp_dispositivo="ontem").get_json()["campo"] == "timestamp_dispositivo"
    sem_token = cliente.post("/ponto/api/marcacao", json={"device_uuid": UUID_TABLET, "cpf": CPF_JOAO,
                                                          "obra": "PT-01"})
    assert sem_token.status_code == 401


def _foto_base64(largura=1200, altura=1600) -> str:
    from PIL import Image
    saida = io.BytesIO()
    Image.new("RGB", (largura, altura), (200, 150, 100)).save(saida, format="JPEG")
    return "data:image/jpeg;base64," + base64.b64encode(saida.getvalue()).decode()


def test_batida_com_foto_sobe_para_o_drive_e_so_a_ficha_fica_no_banco(cliente, banco, monkeypatch):
    from app.apps.ponto.core import fotos
    enviados = []

    def drive_falso(conn, dados, nome, momento):
        enviados.append((nome, len(dados)))
        return "drive-abc123"
    monkeypatch.setattr(fotos, "_subir_no_drive", drive_falso)
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, foto_base64=_foto_base64())
    assert r.status_code == 201, r.get_json()
    m = r.get_json()["marcacao"]
    assert m["tem_foto"] is True and len(m["foto_hash"]) == 64
    assert len(enviados) == 1 and enviados[0][0].endswith(f"_colab{m['colaborador_id']}_nsr{m['nsr']}.jpg")
    with banco.connect() as conn:
        f = conn.execute(text("SELECT tamanho, largura, altura, sha256, drive_file_id, conteudo, "
                              "enviada_em FROM ponto.fotos")).one()
    assert f[0] <= 300 * 1024 and (f[1], f[2]) == (600, 800) and f[3] == m["foto_hash"]
    assert f[4] == "drive-abc123" and f[5] is None and f[6] is not None
    r = bater(cliente, token, cpf=CPF_MARIA, foto_base64="isto não é foto")
    assert r.status_code == 400 and r.get_json()["campo"] == "foto_base64"


def test_drive_fora_do_ar_nao_derruba_a_batida_e_a_foto_espera_na_fila(cliente, banco, monkeypatch):
    from app.apps.ponto.core import fotos
    monkeypatch.setattr(fotos, "TENTATIVAS_NA_HORA", 1)
    monkeypatch.delenv(fotos.VARIAVEL_PASTA, raising=False)      # Drive não configurado
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, foto_base64=_foto_base64())
    assert r.status_code == 201 and r.get_json()["marcacao"]["tem_foto"] is True
    with banco.connect() as conn:
        f = conn.execute(text("SELECT drive_file_id, conteudo, tentativas, ultimo_erro "
                              "FROM ponto.fotos")).one()
    assert f[0] is None and f[1] is not None and f[2] == 1 and "não configurado" in f[3]
    assert cliente.get("/ponto/health").get_json()["fotos_na_fila"] == 1

    # o Drive volta: a fila esvazia e os bytes somem do banco
    monkeypatch.setattr(fotos, "_subir_no_drive", lambda conn, dados, nome, momento: "drive-xyz")
    r = cliente.post("/ponto/api/admin/fotos/enviar-pendentes", headers=com_chave())
    assert r.status_code == 200 and r.get_json()["enviadas"] == 1 and r.get_json()["restantes"] == 0
    with banco.connect() as conn:
        f = conn.execute(text("SELECT drive_file_id, conteudo FROM ponto.fotos")).one()
    assert f[0] == "drive-xyz" and f[1] is None
    assert cliente.get("/ponto/health").get_json()["fotos_na_fila"] == 0


def test_idface_e_manual_entram_pela_chave(cliente):
    r = cliente.post("/ponto/api/marcacao", json={"cpf": CPF_JOAO, "obra": "PT-01", "origem": "IDFACE"},
                     headers=com_chave())
    assert r.status_code == 201, r.get_json()
    m = r.get_json()["marcacao"]
    assert m["origem"] == "IDFACE" and m["status"] == "VALIDA" and m["dentro_da_cerca"] is None
    r = cliente.post("/ponto/api/marcacao", json={"cpf": CPF_MARIA, "obra": "PT-01", "origem": "MANUAL"},
                     headers=com_chave())
    assert r.status_code == 400 and r.get_json()["campo"] == "registrado_por"
    r = cliente.post("/ponto/api/marcacao", json={"cpf": CPF_MARIA, "obra": "PT-01", "origem": "MANUAL",
                                                  "registrado_por": "Encarregado Pedro"},
                     headers=com_chave())
    assert r.status_code == 201 and r.get_json()["marcacao"]["registrado_por"] == "Encarregado Pedro"


def test_origem_idface_sem_chave_e_recusada(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    r = bater(cliente, token, origem="IDFACE")
    assert r.status_code == 403 and "só com chave" in r.get_json()["erro"]


def test_vigia_noturno_antes_das_10h_pertence_ao_dia_anterior(cliente, banco, ponto, monkeypatch):
    from app.apps.ponto import horario
    with banco.connect() as conn:
        conn.execute(text("INSERT INTO ponto.colaborador_config (colaborador_id, tipo_jornada) "
                          "VALUES (:c, 'VIGIA_NOTURNO_2')"), {"c": ponto["joao"]})
        conn.commit()
    # 06:00 de Fortaleza = 09:00 UTC
    madrugada = dt.datetime(2026, 10, 4, 9, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(horario, "agora", lambda: madrugada)
    r = cliente.post("/ponto/api/marcacao", json={"cpf": CPF_JOAO, "obra": "PT-01", "origem": "IDFACE"},
                     headers=com_chave())
    m = r.get_json()["marcacao"]
    assert m["horario"] == "2026-10-04T06:00:00-03:00" and m["data_referencia"] == "2026-10-03"
    r = cliente.post("/ponto/api/marcacao", json={"cpf": CPF_MARIA, "obra": "PT-01", "origem": "IDFACE"},
                     headers=com_chave())
    assert r.get_json()["marcacao"]["data_referencia"] == "2026-10-04"   # jornada padrão


# ---------------------------------------------------------------------------
# Consultas e cadastros
# ---------------------------------------------------------------------------
def test_consulta_por_periodo_cpf_obra_e_status(cliente):
    dispositivo_id, token = registrar_aparelho(cliente)
    aprovar(cliente, dispositivo_id)
    bater(cliente, token)
    bater(cliente, token, cpf=CPF_MARIA, lat=-3.75, lon=-38.5270)   # fora da cerca
    hoje = dt.date.today()
    ini, fim = (hoje - dt.timedelta(days=1)).isoformat(), (hoje + dt.timedelta(days=1)).isoformat()
    todas = cliente.get(f"/ponto/api/marcacoes?data_inicio={ini}&data_fim={fim}",
                        headers=com_chave()).get_json()
    assert todas["quantidade"] == 2 and len(todas["dias"]) == 2 and todas["truncado"] is False
    dia_maria = next(d for d in todas["dias"] if d["cpf"] == CPF_MARIA)
    assert dia_maria["em_analise"] == 1 and dia_maria["obras"] == ["PT-01"]
    so_joao = cliente.get(f"/ponto/api/marcacoes?cpf={CPF_JOAO}&data_inicio={ini}&data_fim={fim}",
                          headers=com_chave()).get_json()
    assert so_joao["quantidade"] == 1 and so_joao["marcacoes"][0]["nome"] == "João da Silva"
    analise = cliente.get(f"/ponto/api/marcacoes?status=EM_ANALISE&data_inicio={ini}&data_fim={fim}",
                          headers=com_chave()).get_json()
    assert analise["quantidade"] == 1
    por_obra = cliente.get(f"/ponto/api/marcacoes?obra=PT-02&data_inicio={ini}&data_fim={fim}",
                           headers=com_chave()).get_json()
    assert por_obra["quantidade"] == 0


def test_consulta_recusa_periodo_longo_e_data_ruim(cliente):
    r = cliente.get("/ponto/api/marcacoes?data_inicio=2026-01-01&data_fim=2026-04-01", headers=com_chave())
    assert r.status_code == 400 and "62 dias" in r.get_json()["erro"]
    r = cliente.get("/ponto/api/marcacoes?data_inicio=01/10/2026", headers=com_chave())
    assert r.status_code == 400 and r.get_json()["campo"] == "data_inicio"
    r = cliente.get("/ponto/api/marcacoes", headers=com_chave())
    assert r.status_code == 400


def test_listas_de_colaboradores_e_obras(cliente, ponto):
    pessoas = cliente.get("/ponto/api/colaboradores", headers=com_chave()).get_json()
    nomes = [p["nome"] for p in pessoas["colaboradores"]]
    assert "João da Silva" in nomes and "Ex-colaborador" not in nomes
    joao = next(p for p in pessoas["colaboradores"] if p["cpf"] == CPF_JOAO)
    assert joao["obra_principal"]["codigo"] == "PT-01" and joao["tipo_jornada"] == "PADRAO_4"
    todos = cliente.get("/ponto/api/colaboradores?todos=1", headers=com_chave()).get_json()
    assert "Ex-colaborador" in [p["nome"] for p in todos["colaboradores"]]
    obras = cliente.get("/ponto/api/obras", headers=com_chave()).get_json()
    codigos = [o["codigo"] for o in obras["obras"]]
    assert "PT-01" in codigos and "PT-02" in codigos and "PT-03" not in codigos
    pt01 = next(o for o in obras["obras"] if o["codigo"] == "PT-01")
    assert pt01["raio_metros"] == 200 and pt01["latitude"] == OBRA_LAT
    por_obra = cliente.get("/ponto/api/colaboradores?obra=PT-01", headers=com_chave()).get_json()
    assert por_obra["quantidade"] == 2


def test_pedido_de_ajuste(cliente):
    r = cliente.post("/ponto/api/ajustes", json={
        "cpf": CPF_JOAO, "data_referencia": "2026-10-01", "tipo": "INCLUSAO",
        "justificativa": "Esqueci de bater na saída do almoço", "solicitado_por": "João",
        "horario_proposto": "2026-10-01T13:00:00", "obra_proposta": "PT-01"}, headers=com_chave())
    assert r.status_code == 201, r.get_json()
    a = r.get_json()["ajuste"]
    assert a["status"] == "PENDENTE" and a["horario_proposto"] == "2026-10-01T13:00:00-03:00"
    curta = cliente.post("/ponto/api/ajustes", json={
        "cpf": CPF_JOAO, "data_referencia": "2026-10-01", "tipo": "INCLUSAO",
        "justificativa": "curta", "solicitado_por": "João"}, headers=com_chave())
    assert curta.status_code == 400 and curta.get_json()["campo"] == "justificativa"


# ---------------------------------------------------------------------------
# Importadores
# ---------------------------------------------------------------------------
def test_importar_obras_cria_configura_e_nao_sobrescreve_coordenada(banco, ponto):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros, importacao
    registros = importacao.montar_registros([
        ["Código", "Nome", "Latitude", "Longitude", "Raio", "Centro de Custo"],
        ["PT-01", "Escola do Bairro", "-3.9", "-38.9", "300", "CC-1"],      # já existe, geo diferente
        ["PT-02", "Posto sem mapa", "-3.80", "-38.60", "", ""],            # existe sem geo: preenche
        ["PT-09", "Quadra nova", "", "", "150", "CC-9"],                   # nova, sem geo
        ["", "Sem código", "", "", "", ""],                                # erro
        ["PT-10", "Coordenada torta", "abc", "1", "", ""],                 # erro
    ])
    with db.conexao() as conn:
        simulado = importacao.importar_obras(conn, registros, gravar=False)
        assert len(simulado["criadas"]) == 1 and len(simulado["atualizadas"]) == 2
        assert len(simulado["erros"]) == 2 and len(simulado["avisos"]) == 1
        assert cadastros.obra_por_codigo(conn, "PT-09") is None       # simulação não grava
        relatorio = importacao.importar_obras(conn, registros, gravar=True)
        assert relatorio["gravou"] is True
        pt01 = cadastros.obra_por_codigo(conn, "PT-01")
        assert float(pt01["latitude"]) == OBRA_LAT and pt01["raio_metros"] == 300   # geo mantida, raio novo
        assert pt01["centro_custo"] == "CC-1"
        pt02 = cadastros.obra_por_codigo(conn, "PT-02")
        assert float(pt02["latitude"]) == -3.80
        pt09 = cadastros.obra_por_codigo(conn, "PT-09")
        assert pt09 and pt09["latitude"] is None and pt09["raio_metros"] == 150
        # reexecutar não duplica
        de_novo = importacao.importar_obras(conn, registros, gravar=True)
        assert de_novo["criadas"] == [] and len(de_novo["atualizadas"]) == 3
        forcado = importacao.importar_obras(conn, registros[:1], gravar=True,
                                            sobrescrever_coordenadas=True)
        assert forcado["avisos"] == []
        assert float(cadastros.obra_por_codigo(conn, "PT-01")["latitude"]) == -3.9


def test_importar_colaboradores_cria_no_erp_e_configura_no_ponto(banco, ponto):
    from app.apps.ponto import db
    from app.apps.ponto.core import cadastros, importacao
    registros = importacao.montar_registros([
        ["CPF", "Nome", "Obra", "Centro de Custo", "Tipo de Jornada"],
        ["529.982.247-25", "João da Silva", "PT-02", "CC-2", "Vigia Noturno"],   # existe; obra diverge
        ["390.533.447-05", "Carlos Novo", "PT-01", "CC-1", ""],                  # novo
        ["111", "CPF ruim", "PT-01", "", ""],                                    # erro
        ["862.419.580-00", "Obra errada", "PT-99", "", ""],                      # erro: obra não existe
    ])
    with db.conexao() as conn:
        simulado = importacao.importar_colaboradores(conn, registros, gravar=False)
        assert len(simulado["criados"]) == 1 and len(simulado["atualizados"]) == 1
        assert len(simulado["erros"]) == 2 and len(simulado["avisos"]) == 1
        assert cadastros.colaborador_por_cpf(conn, "39053344705") is None
        relatorio = importacao.importar_colaboradores(conn, registros, gravar=True)
        assert relatorio["gravou"]
        joao = cadastros.colaborador_por_cpf(conn, CPF_JOAO)
        assert joao["tipo_jornada"] == "VIGIA_NOTURNO_2" and joao["centro_custo"] == "CC-2"
        assert joao["obra_codigo"] == "PT-01"          # o ERP manda: a obra NÃO mudou
        carlos = cadastros.colaborador_por_cpf(conn, "39053344705")
        assert carlos and carlos["obra_codigo"] == "PT-01" and carlos["tipo_jornada"] == "PADRAO_4"
        assert carlos["situacao"] == "ATIVO"
        de_novo = importacao.importar_colaboradores(conn, registros, gravar=True)
        assert de_novo["criados"] == [] and len(de_novo["atualizados"]) == 2


def test_sem_chave_configurada_tudo_fecha_menos_o_registro(cliente, monkeypatch):
    monkeypatch.delenv("PONTO_API_KEY")
    assert cliente.get("/ponto/api/obras", headers=com_chave()).status_code == 503
    assert cliente.post("/ponto/api/marcacao", json={}, headers=com_chave()).status_code == 401
    r = cliente.post("/ponto/api/dispositivo/registrar", json={"device_uuid": UUID_TABLET})
    assert r.status_code == 201
    assert cliente.get("/ponto/health").get_json()["chave_configurada"] is False
