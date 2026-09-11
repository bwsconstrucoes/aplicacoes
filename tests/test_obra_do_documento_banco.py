"""A obra que nasce do documento — e o único lugar que cria obra.

Pedido do dono em 10/09/2026, na mesma conversa, duas coisas:

  *"eu posso criar a obra tanto pela administração como posso criar a obra por
  obras, e lá aparecem menos campos. Então acho que tem que unificar isso aí:
  se a gente tem o painel de obras, não tem mais que ter obras em
  administração."*

  *"nós havíamos conversado sobre a criação de obras a partir de um documento,
  da leitura de um documento. Então isso ficaria associado a obras."*

A CHAMADA À IA É DUBLADA. O que precisa de prova não é o modelo: é o que o
sistema faz com a resposta dele — e, sobretudo, o que ele RECUSA fazer.

O que se prova:

  1. Criar obra é uma rota só, no painel, e ela pede alçada de configuração.
  2. O documento cria a obra E é arquivado dentro dela, na MESMA transação:
     obra sem o contrato que a criou, ou contrato guardado numa obra que não
     chegou a existir, seriam os dois piores resultados possíveis.
  3. A trava por tipo continua de pé: documento que não prova um campo não
     grava esse campo, nem que a tela mande.
  4. Obra duplicada é ATRAVANCADA. Mesma matrícula CNO ou mesmo número de
     contrato param a criação até alguém dizer que sabe que é outra obra —
     duas obras para o mesmo contrato partem o histórico em dois e nenhum
     relatório fecha.
  5. O código da obra não se inventa: sem ele (ou sem nome) a criação é
     recusada, e nada fica gravado pela metade.
  6. A alíquota de ISS nasce nas duas colunas iguais — a antiga e a que a
     tributação lê —, senão o cadastro diz uma coisa e a nota diz outra.
"""
from __future__ import annotations

import io
import json
from decimal import Decimal

import pytest

from app.apps.erp.core.arquivo import catalogo, preenchimento
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (Empresa, Obra,
                                              PerfilUsuario as P, Usuario)
from app.apps.erp.db.models.financeiro import Documento
from tests.conftest import como

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 contrato da obra nova"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin de obras", email="obra.nova@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    obreiro = Usuario(nome="Lançador", email="lanca.obra@teste.local", ativo=True,
                      senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.LANCADOR)
    emp = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", ativo=True)
    s.add_all([admin, obreiro, emp])
    s.flush()
    return {"s": s, "admin": admin, "obreiro": obreiro, "empresa": emp}


def _dublar_leitura(monkeypatch, resposta):
    def falso(conteudo, nome_arquivo, instrucao, dica=""):
        falso.instrucao = instrucao
        d = dict(resposta)
        d["texto_extraido"] = "texto do documento"
        d["origem_leitura"] = "PDF_TEXTO"
        return d
    falso.instrucao = ""
    monkeypatch.setattr("app.apps.erp.core.documentos.leitor.ler_com_instrucao", falso)
    return falso


def _arquivo(nome="contrato.pdf"):
    return (io.BytesIO(PDF), nome)


# ---------------------------------------------------------------------------
# 1. UM LUGAR SÓ PARA CRIAR OBRA
# ---------------------------------------------------------------------------
def test_a_rota_antiga_de_configuracoes_nao_existe_mais(app_real, cenario):
    """Dois formulários faziam a mesma obra nascer diferente conforme a porta."""
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/config/obra", json={"codigo": "X1", "nome": "Obra X"})

    assert r.status_code == 404


def test_criar_obra_pelo_painel_com_o_cadastro_inteiro(app_real, cenario):
    r = como(app_real, cenario["admin"].id).post("/erp/api/obras/nova", json={
        "codigo": "escpe18", "nome": "Escola do Eusébio",
        "cliente": "MUNICIPIO DE EUSEBIO", "cnpj_cliente": "07705674000106",
        "contrato": "268/2025", "objeto": "CONSTRUCAO DE ESCOLA",
        "municipio": "EUSEBIO", "uf": "ce", "cno": "90.025.25410/76",
        "valor_contrato": "1.250.000,00", "aliquota_iss_pct": "3,5"})

    assert r.status_code == 200, r.get_data(as_text=True)
    obra = cenario["s"].query(Obra).filter(Obra.codigo == "ESCPE18").one()
    assert obra.nome == "Escola do Eusébio"
    assert obra.cliente == "MUNICIPIO DE EUSEBIO"
    assert obra.contrato == "268/2025"
    assert obra.uf == "CE", "a UF é normalizada para maiúscula"
    assert obra.valor_contrato == Decimal("1250000.00")


def test_o_endereco_entra_no_cadastro_direto(app_real, cenario):
    """O dono estranhou não achar o endereço no formulário: *"é um campo
    importantíssimo"*. Ele existia só na ficha; agora entra na criação."""
    r = como(app_real, cenario["admin"].id).post("/erp/api/obras/nova", json={
        "codigo": "ESCPE18", "nome": "Escola do Eusébio",
        "cep": "61760-000", "endereco": "AVENIDA CENTRAL",
        "numero_endereco": "1500", "bairro": "CENTRO",
        "municipio": "EUSEBIO", "uf": "CE"})

    assert r.status_code == 200, r.get_data(as_text=True)
    obra = cenario["s"].query(Obra).one()
    assert obra.endereco == "AVENIDA CENTRAL"
    assert obra.numero_endereco == "1500"
    assert obra.bairro == "CENTRO"
    assert obra.cep == "61760000", "o CEP é guardado só com dígitos"


def test_quem_nao_configura_nao_cria_obra(app_real, cenario):
    r = como(app_real, cenario["obreiro"].id).post(
        "/erp/api/obras/nova", json={"codigo": "X1", "nome": "Obra X"})

    assert r.status_code == 403
    assert cenario["s"].query(Obra).count() == 0


def test_codigo_repetido_e_recusado(app_real, cenario):
    c = como(app_real, cenario["admin"].id)
    c.post("/erp/api/obras/nova", json={"codigo": "ESCPE18", "nome": "Escola"})
    r = c.post("/erp/api/obras/nova", json={"codigo": "escpe18", "nome": "Outra"})

    assert r.status_code == 400
    assert "ESCPE18" in r.get_json()["erro"]
    assert cenario["s"].query(Obra).count() == 1


# ---------------------------------------------------------------------------
# 6. A ALÍQUOTA DE ISS NASCE NAS DUAS COLUNAS IGUAIS
# ---------------------------------------------------------------------------
def test_a_aliquota_de_iss_nasce_na_coluna_que_a_tributacao_le(app_real, cenario):
    """Havia duas colunas: a tela de tributação escreve numa, o cadastro antigo
    escrevia na outra, e a emissão automática lia justamente a que ninguém
    alimentava."""
    como(app_real, cenario["admin"].id).post("/erp/api/obras/nova", json={
        "codigo": "ESCPE18", "nome": "Escola", "aliquota_iss_pct": "3,5"})

    obra = cenario["s"].query(Obra).one()
    assert obra.aliquota_iss_pct == Decimal("3.5000")
    assert obra.aliquota_iss == Decimal("3.50"), "a coluna antiga não pode divergir"


def test_a_emissao_automatica_le_a_aliquota_da_tela_de_tributacao(cenario):
    from app.apps.erp.core.notas_emitidas.automatica import _aliquota_iss
    obra = Obra(codigo="SO-PCT", nome="Só na nova",
                aliquota_iss_pct=Decimal("2.0000"))
    antiga = Obra(codigo="SO-ANTIGA", nome="Só na antiga",
                  aliquota_iss=Decimal("5.00"))

    assert _aliquota_iss(obra) == Decimal("2.0000")
    assert _aliquota_iss(antiga) == Decimal("5.00"), "obra antiga não pode travar"


# ---------------------------------------------------------------------------
# 2. LER NÃO GRAVA; CRIAR E ARQUIVAR ACONTECEM JUNTOS
# ---------------------------------------------------------------------------
def test_ler_o_documento_nao_cria_nada(app_real, cenario, monkeypatch):
    falso = _dublar_leitura(monkeypatch, {
        "tipo_codigo": "CONTRATO-OBRA", "confianca": "ALTA",
        "emissao": "2026-03-10", "validade": "2027-03-09",
        "dados_extraidos": {"contrato": "268/2025", "valor_contrato": "1250000.00",
                            "objeto": "CONSTRUCAO DE ESCOLA. Segunda frase.",
                            "cliente": "MUNICIPIO DE EUSEBIO"}})
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/obras/documento/ler",
        data={"arquivo": _arquivo()}, content_type="multipart/form-data")

    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    assert "dados_extraidos" in falso.instrucao
    campos = {c["campo"]: c for c in d["cadastro"]["campos"]}
    assert campos["contrato"]["valor"] == "268/2025"
    assert campos["contrato"]["marcar"] is True, "obra nova: nada a sobrescrever"
    assert campos["contrato"]["conflito"] is False
    assert d["cadastro"]["nome_sugerido"] == "CONSTRUCAO DE ESCOLA"
    # A obra não existe ainda — cobrar "de quem é o documento" aqui seria
    # cobrar o que esta operação está resolvendo, e rebaixaria a confiança da
    # leitura por um motivo que não é defeito.
    assert "de quem é o documento" not in d["sugestao"]["faltando"]
    assert d["sugestao"]["confianca"] == "ALTA"
    assert cenario["s"].query(Obra).count() == 0
    assert cenario["s"].query(Documento).count() == 0


def test_o_documento_cria_a_obra_e_e_arquivado_nela(app_real, cenario, monkeypatch):
    _dublar_leitura(monkeypatch, {})
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/obras/documento",
        data={"arquivo": _arquivo(), "tipo": "CONTRATO-OBRA",
              "codigo": "ESCPE18", "nome": "Escola do Eusébio",
              "emissao": "2026-03-10", "validade": "2027-03-09",
              "campos": json.dumps({"contrato": "268/2025",
                                    "valor_contrato": "1250000.00",
                                    "cliente": "MUNICIPIO DE EUSEBIO"})},
        content_type="multipart/form-data")

    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    obra = s.query(Obra).one()
    assert obra.codigo == "ESCPE18"
    assert obra.contrato == "268/2025"
    assert obra.valor_contrato == Decimal("1250000.00")
    assert d["preenchido"]["quantidade"] == 3
    documento = s.query(Documento).one()
    assert documento.obra_id == obra.id
    assert d["documento"]["nome"].startswith("CONTRATO-OBRA_ESCPE18")


@pytest.fixture
def app_transacional(sessao_real, monkeypatch):
    """Como o `app_real` do conftest, mas com o desfazer que a produção tem.

    O `app_real` entrega a sessão do teste e não desfaz nada quando a rota
    estoura — em produção quem desfaz é o `get_session` do ERP, que fica fora
    do alcance do dublê. Sem reproduzir isso aqui, "a obra não fica para trás"
    não seria provado: seria só afirmado.
    """
    import contextlib

    from flask import Flask

    from app.apps.erp import routes

    @contextlib.contextmanager
    def _como_em_producao():
        try:
            yield sessao_real
        except Exception:
            sessao_real.rollback()
            raise
    monkeypatch.setattr(routes, "get_session", _como_em_producao)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def test_campo_recusado_nao_deixa_obra_nem_documento_para_tras(app_transacional,
                                                               cenario, monkeypatch):
    """A trava por tipo derruba a operação inteira — obra pela metade é pior
    que obra nenhuma, porque ninguém percebe."""
    _dublar_leitura(monkeypatch, {})
    r = como(app_transacional, cenario["admin"].id).post(
        "/erp/api/obras/documento",
        data={"arquivo": _arquivo(), "tipo": "SEGURO",
              "codigo": "ESCPE18", "nome": "Escola", "validade": "2027-03-09",
              "campos": json.dumps({"valor_contrato": "9999999.00"})},
        content_type="multipart/form-data")

    assert r.status_code == 400
    assert "não preenche" in r.get_json()["erro"]
    assert cenario["s"].query(Obra).count() == 0
    assert cenario["s"].query(Documento).count() == 0


def test_sem_codigo_nao_se_cria_obra(app_real, cenario, monkeypatch):
    _dublar_leitura(monkeypatch, {})
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/obras/documento",
        data={"arquivo": _arquivo(), "tipo": "CONTRATO-OBRA", "nome": "Escola",
              "campos": json.dumps({})},
        content_type="multipart/form-data")

    assert r.status_code == 400
    assert cenario["s"].query(Obra).count() == 0


def test_quem_nao_configura_nao_cria_obra_por_documento(app_real, cenario, monkeypatch):
    _dublar_leitura(monkeypatch, {})
    r = como(app_real, cenario["obreiro"].id).post(
        "/erp/api/obras/documento",
        data={"arquivo": _arquivo(), "tipo": "CONTRATO-OBRA",
              "codigo": "ESCPE18", "nome": "Escola", "campos": json.dumps({})},
        content_type="multipart/form-data")

    assert r.status_code == 403
    assert cenario["s"].query(Obra).count() == 0


# ---------------------------------------------------------------------------
# 3. A TRAVA POR TIPO, NA SUGESTÃO
# ---------------------------------------------------------------------------
def test_o_seguro_nao_sugere_valor_de_contrato(cenario):
    r = preenchimento.sugerir_para_nova_obra(
        cenario["s"], "SEGURO",
        {"dados_extraidos": {"seguro_garantia": "AP-99", "valor_contrato": "500000.00"}})
    campos = {c["campo"] for c in r["campos"]}

    assert campos == {"seguro_garantia"}


def test_tipo_que_nao_alimenta_cadastro_avisa_em_vez_de_inventar(cenario):
    r = preenchimento.sugerir_para_nova_obra(
        cenario["s"], "DIARIO-OBRA", {"dados_extraidos": {"contrato": "268/2025"}})

    assert r["tipo_nao_preenche"] is True
    assert r["campos"] == []


def test_o_nome_sugerido_cai_no_contratante_quando_nao_ha_objeto(cenario):
    r = preenchimento.sugerir_para_nova_obra(
        cenario["s"], "CONTRATO-OBRA",
        {"dados_extraidos": {"cliente": "MUNICIPIO DE EUSEBIO", "municipio": "EUSEBIO"}})

    assert r["nome_sugerido"] == "MUNICIPIO DE EUSEBIO — EUSEBIO"


# ---------------------------------------------------------------------------
# 4. OBRA DUPLICADA É ATRAVANCADA
# ---------------------------------------------------------------------------
@pytest.fixture
def ja_existe(cenario):
    obra = Obra(codigo="ESCPE18", nome="Escola do Eusébio",
                cno="90.025.25410/76", contrato="268/2025")
    cenario["s"].add(obra)
    cenario["s"].flush()
    return obra


def test_o_mesmo_cno_pontuado_de_outro_jeito_e_reconhecido(cenario, ja_existe):
    achadas = preenchimento.obras_parecidas(cenario["s"], cno="90025254107 6")

    assert [o["codigo"] for o in achadas] == ["ESCPE18"]
    assert "CNO" in achadas[0]["motivos"][0]


def test_o_mesmo_numero_de_contrato_e_reconhecido(cenario, ja_existe):
    achadas = preenchimento.obras_parecidas(cenario["s"], contrato=" 268 / 2025 ")

    assert [o["codigo"] for o in achadas] == ["ESCPE18"]


def test_obra_diferente_nao_e_apontada(cenario, ja_existe):
    assert preenchimento.obras_parecidas(
        cenario["s"], cno="11.111.11111/11", contrato="999/2030") == []


def test_obra_sem_cno_nem_contrato_nao_casa_com_a_que_tambem_nao_tem(cenario):
    cenario["s"].add(Obra(codigo="VAZIA", nome="Sem documentos"))
    cenario["s"].flush()

    assert preenchimento.obras_parecidas(cenario["s"], cno="", contrato="") == []


def test_a_criacao_para_quando_a_obra_ja_existe(app_real, cenario, ja_existe,
                                                monkeypatch):
    _dublar_leitura(monkeypatch, {})
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/obras/documento",
        data={"arquivo": _arquivo(), "tipo": "CONTRATO-OBRA",
              "codigo": "ESCPE18B", "nome": "Escola do Eusébio (2)",
              "campos": json.dumps({"contrato": "268/2025"})},
        content_type="multipart/form-data")

    assert r.status_code == 400
    d = r.get_json()
    assert [o["codigo"] for o in d["parecidas"]] == ["ESCPE18"]
    assert cenario["s"].query(Obra).count() == 1, "não pode nascer a segunda"
    assert cenario["s"].query(Documento).count() == 0


def test_confirmando_que_e_outra_obra_a_criacao_segue(app_real, cenario, ja_existe,
                                                      monkeypatch):
    """A guarda avisa e para; ela não decide pela pessoa. Contrato guarda-chuva
    com duas obras existe, e o sistema não pode ser mais teimoso que o mundo."""
    _dublar_leitura(monkeypatch, {})
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/obras/documento",
        data={"arquivo": _arquivo(), "tipo": "CONTRATO-OBRA",
              "codigo": "ESCPE18B", "nome": "Escola do Eusébio (2)",
              "confirmar_duplicada": "1", "validade": "2027-03-09",
              "campos": json.dumps({"contrato": "268/2025"})},
        content_type="multipart/form-data")

    assert r.status_code == 200, r.get_data(as_text=True)
    assert cenario["s"].query(Obra).count() == 2


def test_a_leitura_ja_avisa_das_parecidas_antes_de_gravar(app_real, cenario,
                                                          ja_existe, monkeypatch):
    _dublar_leitura(monkeypatch, {
        "tipo_codigo": "CONTRATO-OBRA", "confianca": "ALTA",
        "dados_extraidos": {"contrato": "268/2025"}})
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/obras/documento/ler",
        data={"arquivo": _arquivo()}, content_type="multipart/form-data")

    assert r.status_code == 200
    assert [o["codigo"] for o in r.get_json()["cadastro"]["parecidas"]] == ["ESCPE18"]
