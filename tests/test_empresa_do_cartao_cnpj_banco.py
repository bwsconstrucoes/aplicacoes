"""A EMPRESA QUE NASCE DO CARTÃO CNPJ.

Dono, 17/09/2026, ao ver que a obra passou a exigir empresa: *"e eu conseguiria
cadastrá-la a partir do Cartão CNPJ?"*.

É o mesmo princípio que já vale para a obra (contrato) e para o colaborador: o
documento que já existe preenche o cadastro, em vez de alguém redigitar o que
está no papel — e o documento fica arquivado na empresa, no mesmo gesto.

O QUE ESTES TESTES SEGURAM

  1. **Quem grava é a pessoa.** A leitura sugere; o que vai para o banco é o que
     foi confirmado na tela, com as correções dela.
  2. **CNPJ repetido é avisado ANTES**, não na hora de gravar: descobrir depois
     de conferir tudo é o tipo de coisa que faz perder o trabalho.
  3. **Empresa e documento na MESMA transação**: empresa criada com o cartão
     perdido, ou cartão guardado numa empresa que não nasceu, são os dois piores
     resultados possíveis.
  4. **Situação cadastral que não é ATIVA vira aviso** — empresa baixada não
     emite nota, e é melhor saber antes de montar obra em cima dela.

A CHAMADA À IA É DUBLADA: o que precisa de prova é o que o sistema faz com a
resposta, não o modelo.
"""
from __future__ import annotations

import io as _io
import json

import pytest

from app.apps.erp.core.arquivo import catalogo, leitura, preenchimento
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import Empresa, PerfilUsuario as P, Usuario
from app.apps.erp.db.models.financeiro import Documento
from tests.conftest import como

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 cartao cnpj"
CARTAO = {
    "razao_social": "BWS CONSTRUCOES E SERVICOS LTDA",
    "nome_fantasia": "BWS",
    "cnpj": "11.222.333/0001-81",
    "logradouro": "AV DOM LUIS", "numero": "500", "bairro": "MEIRELES",
    "municipio": "FORTALEZA", "uf": "CE", "cep": "60160-230",
    "situacao_cadastral": "ATIVA",
}


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin", email="cartao.cnpj@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    s.add(admin)
    s.flush()
    return {"s": s, "admin": admin}


def _dublar_leitura(monkeypatch, extraidos, tipo="CARTAO-CNPJ"):
    def falso(conteudo, nome, instrucao, dica=""):
        return {"tipo_codigo": tipo, "tipo_motivo": "comprovante da Receita",
                "dono_especie": "EMPRESA", "dono_nome": extraidos.get("razao_social", ""),
                "confianca": "ALTA", "dados_extraidos": extraidos,
                "texto_extraido": "comprovante de inscrição"}
    monkeypatch.setattr("app.apps.erp.core.documentos.leitor.ler_com_instrucao", falso)


# ---------------------------------------------------------------------------
# 1. O QUE A LEITURA PROPÕE
# ---------------------------------------------------------------------------
def test_o_cartao_preenche_os_campos_da_empresa(cenario):
    r = preenchimento.sugerir_para_nova_empresa(
        cenario["s"], "CARTAO-CNPJ", {"dados_extraidos": CARTAO})
    campos = {c["campo"]: c["valor"] for c in r["campos"]}
    assert campos["razao_social"] == "BWS CONSTRUCOES E SERVICOS LTDA"
    assert campos["cnpj"] == "11222333000181", "CNPJ chega só com dígitos"
    assert campos["municipio"] == "FORTALEZA"
    assert all(c["marcar"] for c in r["campos"])


def test_o_que_o_cartao_nao_diz_volta_em_branco(cenario):
    r = preenchimento.sugerir_para_nova_empresa(
        cenario["s"], "CARTAO-CNPJ",
        {"dados_extraidos": {"razao_social": "X LTDA", "cnpj": "11222333000181"}})
    vazios = {c["campo"] for c in r["campos_vazios"]}
    assert "municipio" in vazios and "cep" in vazios
    assert "razao_social" not in vazios


def test_cnpj_ja_cadastrado_e_avisado_antes_de_gravar(cenario):
    s = cenario["s"]
    s.add(Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", ativo=True))
    s.flush()
    r = preenchimento.sugerir_para_nova_empresa(
        s, "CARTAO-CNPJ", {"dados_extraidos": CARTAO})
    assert r["ja_cadastrada"] is not None
    assert r["ja_cadastrada"]["nome"] == "BWS"


def test_empresa_baixada_vira_aviso(cenario):
    dados = dict(CARTAO, situacao_cadastral="BAIXADA")
    r = preenchimento.sugerir_para_nova_empresa(
        cenario["s"], "CARTAO-CNPJ", {"dados_extraidos": dados})
    assert any("BAIXADA" in a for a in r["avisos"])


def test_sem_cnpj_lido_o_aviso_diz_o_que_fazer(cenario):
    r = preenchimento.sugerir_para_nova_empresa(
        cenario["s"], "CARTAO-CNPJ", {"dados_extraidos": {"razao_social": "X"}})
    assert any("CNPJ" in a for a in r["avisos"])


def test_tipo_que_nao_e_cadastral_nao_preenche_empresa(cenario):
    r = preenchimento.sugerir_para_nova_empresa(
        cenario["s"], "CONTRATO-OBRA", {"dados_extraidos": CARTAO})
    assert r["campos"] == [] and r["tipo_nao_preenche"] is True


# ---------------------------------------------------------------------------
# 2. A PERGUNTA CHEGA À IA
# ---------------------------------------------------------------------------
def test_a_leitura_pergunta_pelos_campos_da_empresa(app_real, cenario, monkeypatch):
    vistas = {}

    def falso(conteudo, nome, instrucao, dica=""):
        vistas["instrucao"] = instrucao
        return {"tipo_codigo": "CARTAO-CNPJ", "dados_extraidos": CARTAO,
                "dono_especie": "EMPRESA", "confianca": "ALTA"}
    monkeypatch.setattr("app.apps.erp.core.documentos.leitor.ler_com_instrucao", falso)

    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/empresas/documento/ler",
        data={"arquivo": (_io.BytesIO(PDF), "cartao.pdf")},
        content_type="multipart/form-data")
    assert r.status_code == 200, r.get_data(as_text=True)
    assert "razao_social" in vistas["instrucao"]
    assert "nome_fantasia" in vistas["instrucao"]
    assert "inscrição estadual" in vistas["instrucao"].lower(), \
        "a instrução diz para NÃO inventar inscrição"


# ---------------------------------------------------------------------------
# 3. CRIAR E ARQUIVAR, JUNTOS
# ---------------------------------------------------------------------------
def test_a_empresa_nasce_e_o_cartao_fica_arquivado_nela(app_real, cenario, monkeypatch):
    _dublar_leitura(monkeypatch, CARTAO)
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/empresas/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cartao.pdf"), "tipo": "CARTAO-CNPJ",
              "campos": json.dumps({"razao_social": "BWS CONSTRUCOES LTDA",
                                    "cnpj": "11222333000181",
                                    "nome_fantasia": "BWS",
                                    "municipio": "FORTALEZA", "uf": "CE"})},
        content_type="multipart/form-data")
    assert r.status_code == 200, r.get_data(as_text=True)
    d = r.get_json()
    assert d["empresa"]["cnpj"] == "11222333000181"

    empresa = s.get(Empresa, d["empresa"]["id"])
    assert empresa.municipio == "FORTALEZA"
    doc = s.query(Documento).filter_by(empresa_id=empresa.id).first()
    assert doc is not None, "o cartão fica guardado NA empresa que ele criou"
    assert doc.tipo_codigo == "CARTAO-CNPJ"


def test_o_que_a_pessoa_corrigiu_e_o_que_vale(app_real, cenario, monkeypatch):
    """A leitura disse uma coisa; a tela mandou outra. Vale a da tela."""
    _dublar_leitura(monkeypatch, CARTAO)
    s = cenario["s"]
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/empresas/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cartao.pdf"), "tipo": "CARTAO-CNPJ",
              "campos": json.dumps({"razao_social": "NOME CORRIGIDO NA TELA LTDA",
                                    "cnpj": "11222333000181"})},
        content_type="multipart/form-data")
    assert r.status_code == 200
    assert s.get(Empresa, r.get_json()["empresa"]["id"]).razao_social \
        == "NOME CORRIGIDO NA TELA LTDA"


def test_cnpj_invalido_nao_deixa_empresa_nem_documento_para_tras(app_real, cenario,
                                                                 monkeypatch):
    _dublar_leitura(monkeypatch, CARTAO)
    s = cenario["s"]
    antes = s.query(Documento).count()
    r = como(app_real, cenario["admin"].id).post(
        "/erp/api/empresas/documento",
        data={"arquivo": (_io.BytesIO(PDF), "cartao.pdf"), "tipo": "CARTAO-CNPJ",
              "campos": json.dumps({"razao_social": "X LTDA", "cnpj": "11111111111111"})},
        content_type="multipart/form-data")
    assert r.status_code == 400
    assert "CNPJ" in r.get_json()["erro"]
    assert s.query(Documento).count() == antes, "nem o documento ficou"


def test_quem_nao_configura_nao_cria_empresa_por_documento(app_real, cenario):
    s = cenario["s"]
    simples = Usuario(nome="Lançador", email="lanca.cartao@teste.local", ativo=True,
                      senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.LANCADOR)
    s.add(simples)
    s.flush()
    r = como(app_real, simples.id).post(
        "/erp/api/empresas/documento/ler",
        data={"arquivo": (_io.BytesIO(PDF), "cartao.pdf")},
        content_type="multipart/form-data")
    assert r.status_code == 403
