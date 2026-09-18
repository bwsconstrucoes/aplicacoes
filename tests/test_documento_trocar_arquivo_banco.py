"""TROCAR O ARQUIVO ERRADO, sem perder o registro.

Dono, 17/09/2026: *"digamos que eu anexei um documento, anexei uma ART e depois
percebi que aquele documento foi o documento errado. E eu queria substituir
aquele documento."*

Sem isto sobravam dois caminhos ruins: apagar e arquivar de novo — o que perde a
trilha, o vínculo com a obra e deixa sem nada quem tinha o link — ou conviver
com o arquivo errado.

O QUE ESTES TESTES SEGURAM

  1. **O registro é o mesmo.** Mesmo id, mesmo tipo, mesmo dono, mesmas datas,
     mesmo nome padronizado: mudou o papel, não o documento.
  2. **O texto de dentro acompanha.** Senão a busca por palavra e a pergunta
     sobre o documento responderiam pelo conteúdo velho — erro silencioso.
  3. **A troca fica na trilha**, com o retrato do que saiu (nome, tamanho,
     impressão digital) e o motivo. O arquivo some; a história, não.
  4. **Escopo continua valendo**: ter a ação "arquivar" não é alcançar ESTE
     documento.
"""
from __future__ import annotations

import io as _io
from datetime import date, timedelta

import pytest

from app.apps.erp.core.arquivo import catalogo, service as svc_arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (Empresa, EscopoVisao, Obra,
                                              PerfilUsuario as P, Usuario)
from app.apps.erp.db.models.financeiro import Anexo, Documento, Evento
from tests.conftest import como

pytestmark = pytest.mark.banco

CERTO = b"%PDF-1.4 a ART certa desta obra"
ERRADO = b"%PDF-1.4 a ART de outra obra, anexada por engano"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    catalogo.aplicar(s)
    admin = Usuario(nome="Admin", email="troca.arq@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    emp = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                  cnpj="11222333000181", ativo=True)
    s.add_all([admin, emp])
    s.flush()
    obra = Obra(codigo="ESCPE18", nome="Escola Planalto", empresa_id=emp.id,
                fase="EM_EXECUCAO")
    s.add(obra)
    s.flush()
    doc = svc_arq.arquivar(s, ERRADO, "art-errada.pdf", tipo_codigo="ART",
                           obra_id=obra.id, emissao=date(2026, 3, 10),
                           usuario=admin)
    return {"s": s, "admin": admin, "obra": obra, "doc": doc}


def test_troca_o_arquivo_e_mantem_o_registro(cenario):
    s, doc = cenario["s"], cenario["doc"]
    antes = {"id": doc.id, "tipo": doc.tipo_codigo, "obra": doc.obra_id,
             "nome": doc.nome_padronizado, "emissao": doc.emissao}

    svc_arq.substituir_arquivo(s, doc.id, CERTO, "art-certa.pdf",
                               motivo="era a ART de outra obra",
                               usuario=cenario["admin"])

    depois = s.get(Documento, antes["id"])
    assert depois.tipo_codigo == antes["tipo"]
    assert depois.obra_id == antes["obra"]
    assert depois.nome_padronizado == antes["nome"], "o nome guardado não muda"
    assert depois.emissao == antes["emissao"]
    assert depois.nome_original == "art-certa.pdf"


def test_o_arquivo_que_volta_e_o_novo(cenario):
    s, doc = cenario["s"], cenario["doc"]
    svc_arq.substituir_arquivo(s, doc.id, CERTO, "art-certa.pdf",
                               usuario=cenario["admin"])
    from app.apps.erp.core.documentos.armazenamento import conteudo_de
    assert conteudo_de(s, s.get(Anexo, s.get(Documento, doc.id).anexo_id)) == CERTO


def test_o_arquivo_antigo_nao_fica_pendurado(cenario):
    """Arquivo sem dono é lixo que ninguém encontra — e ocupa espaço."""
    s, doc = cenario["s"], cenario["doc"]
    velho = doc.anexo_id
    svc_arq.substituir_arquivo(s, doc.id, CERTO, "art-certa.pdf",
                               usuario=cenario["admin"])
    assert s.get(Anexo, velho) is None


def test_o_texto_de_busca_acompanha_o_arquivo_novo(cenario):
    """Senão a busca por palavra continuaria achando o documento errado."""
    s, doc = cenario["s"], cenario["doc"]
    svc_arq.substituir_arquivo(s, doc.id, CERTO, "art-certa.pdf",
                               usuario=cenario["admin"])
    texto = (s.get(Documento, doc.id).texto or "")
    assert "engano" not in texto


def test_a_troca_fica_na_trilha_com_o_retrato_do_que_saiu(cenario):
    s, doc = cenario["s"], cenario["doc"]
    svc_arq.substituir_arquivo(s, doc.id, CERTO, "art-certa.pdf",
                               motivo="era a ART de outra obra",
                               usuario=cenario["admin"])
    ev = s.query(Evento).filter_by(acao="ARQUIVO_SUBSTITUIDO").first()
    assert ev is not None and ev.entidade_id == doc.id
    assert ev.detalhe["motivo"] == "era a ART de outra obra"
    assert ev.detalhe["saiu"]["nome"], "o nome do arquivo que saiu fica registrado"
    assert ev.detalhe["saiu"]["hash"], "e a impressão digital dele também"


def test_o_mesmo_arquivo_de_novo_e_recusado(cenario):
    """Trocar por um arquivo idêntico é engano, não operação."""
    s, doc = cenario["s"], cenario["doc"]
    with pytest.raises(ErroValidacao) as e:
        svc_arq.substituir_arquivo(s, doc.id, ERRADO, "art-errada.pdf",
                                   usuario=cenario["admin"])
    assert "idêntico" in str(e.value)


def test_arquivo_vazio_e_recusado(cenario):
    s, doc = cenario["s"], cenario["doc"]
    with pytest.raises(ErroValidacao):
        svc_arq.substituir_arquivo(s, doc.id, b"", "nada.pdf",
                                   usuario=cenario["admin"])


def test_documento_inexistente_responde_nao_encontrado(cenario):
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    with pytest.raises(ErroNaoEncontrado):
        svc_arq.substituir_arquivo(cenario["s"], 999999, CERTO, "x.pdf",
                                   usuario=cenario["admin"])


def test_quem_nao_alcanca_a_obra_nao_troca_o_arquivo_dela(app_real, cenario):
    """Fora do escopo responde 'não encontrado', nunca 'sem permissão'."""
    s = cenario["s"]
    de_outra = Usuario(nome="Gestor de outra obra", email="outra.obra@teste.local",
                       ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                       perfil=P.GESTOR_OBRA, escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s.add(de_outra)
    s.flush()
    r = como(app_real, de_outra.id).post(
        f"/erp/api/documentos/{cenario['doc'].id}/arquivo",
        data={"arquivo": (_io.BytesIO(CERTO), "art.pdf")},
        content_type="multipart/form-data")
    assert r.status_code == 404
