"""Perguntar sobre UM documento do acervo — escopo, honestidade e citação.

O PEDIDO, do dono, em 12/09/2026: *"tem um contrato de uma obra e eu quero
perguntar alguma coisa sobre ele"*.

COM BANCO DE VERDADE porque a primeira trava é o recorte por obra, que vive no
`WHERE` — e o dublê da suíte ignora `WHERE`. Sem banco, o teste do parceiro que
NÃO pode ler o contrato da outra obra passaria mesmo com o buraco aberto.

As três coisas que estes testes seguram:

  1. quem não alcança o documento na tela ouve "não encontrado" — nunca "sem
     permissão", que confirmaria a existência do documento;
  2. documento que é IMAGEM responde que é imagem, em vez de deixar a IA
     opinar sobre um documento que ninguém leu;
  3. o trecho citado é conferido dentro do documento antes de ir para a tela —
     citação inventada é descartada e a resposta sai marcada.
"""
from __future__ import annotations

from typing import Any

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.core.perguntas import documentos as svc_doc
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import Anexo, Documento, DocumentoTipo

pytestmark = pytest.mark.banco

CONTRATO = (
    "CONTRATO DE EMPREITADA GLOBAL\n"
    "Cláusula 8ª — O prazo de garantia dos serviços é de 5 (cinco) anos, "
    "contados do recebimento definitivo da obra.\n"
    "Cláusula 9ª — A multa por atraso é de 2% (dois por cento) sobre o valor "
    "da parcela em atraso.\n")


class _RespostaFalsa:
    """O formato que a biblioteca da OpenAI devolve, no mínimo que o código lê."""

    def __init__(self, conteudo: str):
        msg = type("M", (), {"content": conteudo})()
        self.choices = [type("C", (), {"message": msg})()]
        self.usage = None


def _openai_que_responde(conteudo: str):
    class _Completions:
        def create(self, **kwargs):
            _Completions.ultimo = kwargs
            return _RespostaFalsa(conteudo)

    class _Chat:
        completions = _Completions()

    class _Cliente:
        def __init__(self, api_key=None):
            self.chat = _Chat()

    return _Cliente


@pytest.fixture
def acervo(sessao_real, monkeypatch):
    s = sessao_real
    monkeypatch.setenv("OPENAI_API_KEY", "chave-de-teste")

    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    tipo = DocumentoTipo(codigo="CONTRATO-OBRA", nome="Contrato da obra",
                         grupo="OBRA", dono="OBRA", sigilo="ABERTO")
    s.add_all([creche, escola, tipo])
    s.flush()

    def arquivar(nome, obra, texto):
        anexo = Anexo(entidade_tipo="obra", entidade_id=obra.id,
                      nome_arquivo=nome, mime_type="application/pdf",
                      conteudo=b"x", hash_sha256=f"hash-{nome}",
                      guardado_em="BANCO")
        s.add(anexo)
        s.flush()
        d = Documento(tipo_codigo=tipo.codigo, anexo_id=anexo.id,
                      nome_padronizado=nome, obra_id=obra.id, texto=texto)
        s.add(d)
        s.flush()
        return d

    def pessoa(nome, email, perfil, obra=None):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil,
                    escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
        s.add(u)
        s.flush()
        if obra is not None:
            s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
            s.flush()
        return u

    dados = {
        "contrato_creche": arquivar("CONTRATO-CRECHE.pdf", creche, CONTRATO),
        "contrato_escola": arquivar("CONTRATO-ESCOLA.pdf", escola, CONTRATO),
        "escaneado": arquivar("CONTRATO-FOTO.pdf", creche, None),
        "diretor": pessoa("Diretora", "dir@bws.local", P.DIRETOR_FINANCEIRO),
        "parceiro": pessoa("Parceiro da creche", "parc@bws.local", P.PARCEIRO,
                           obra=creche),
    }
    s.commit()
    return dados


# ---------------------------------------------------------------------------
# 1. O RECORTE POR OBRA
# ---------------------------------------------------------------------------
def test_parceiro_pergunta_sobre_o_contrato_da_obra_dele(acervo, monkeypatch):
    monkeypatch.setattr(
        "openai.OpenAI",
        _openai_que_responde(
            '{"achou": true, "resposta": "Cinco anos.", '
            '"trechos": ["O prazo de garantia dos serviços é de 5 (cinco) anos"]}'),
        raising=False)
    r = svc_doc.perguntar_sobre(
        _sessao(acervo), acervo["parceiro"],
        documento_id=acervo["contrato_creche"].id,
        pergunta="qual o prazo de garantia?")
    assert r["achou"] is True
    assert r["trechos"], "a citação conferida tinha de vir junto da resposta"


def test_parceiro_NAO_alcanca_o_contrato_de_outra_obra(acervo):
    """O buraco que este teste fecha: ter a ação de ver o acervo não é alcançar
    ESTE documento. E a recusa é "não encontrado" — dizer "sem permissão" para
    um número que existe confirmaria a existência dele, e varrer os números
    mapearia o acervo sem abrir nada."""
    with pytest.raises(ErroNaoEncontrado):
        svc_doc.perguntar_sobre(
            _sessao(acervo), acervo["parceiro"],
            documento_id=acervo["contrato_escola"].id,
            pergunta="qual o prazo de garantia?")


def test_quem_ve_tudo_alcanca_qualquer_obra(acervo, monkeypatch):
    monkeypatch.setattr(
        "openai.OpenAI",
        _openai_que_responde('{"achou": false, "resposta": "Não está escrito.", '
                             '"trechos": []}'),
        raising=False)
    r = svc_doc.perguntar_sobre(
        _sessao(acervo), acervo["diretor"],
        documento_id=acervo["contrato_escola"].id,
        pergunta="qual o reajuste?")
    assert r["achou"] is False


# ---------------------------------------------------------------------------
# 2. O DOCUMENTO QUE NINGUÉM CONSEGUE LER
# ---------------------------------------------------------------------------
def test_documento_escaneado_diz_que_e_imagem_em_vez_de_inventar(acervo, monkeypatch):
    """Decisão do dono: *"não ler escaneados por hora"*.

    O desfecho proibido é a IA responder mesmo assim. Aqui ela nem é chamada —
    e se fosse, este teste quebraria, porque o cliente falso explode.
    """
    def _explode(*a, **k):
        raise AssertionError("a IA não pode ser chamada para documento sem texto")
    monkeypatch.setattr("openai.OpenAI", _explode, raising=False)

    r = svc_doc.perguntar_sobre(
        _sessao(acervo), acervo["diretor"],
        documento_id=acervo["escaneado"].id,
        pergunta="qual o prazo de garantia?")
    assert r["achou"] is False
    assert "imagem" in r["resposta"].lower()


# ---------------------------------------------------------------------------
# 3. A CITAÇÃO CONFERIDA — o que separa "o contrato diz" de "a IA acha"
# ---------------------------------------------------------------------------
def test_citacao_inventada_e_descartada_e_a_resposta_sai_marcada(acervo, monkeypatch):
    """A IA devolve uma citação plausível que NÃO está no contrato. Na tela ela
    seria indistinguível de uma verdadeira — e é exatamente o que o dono não
    tem como conferir."""
    monkeypatch.setattr(
        "openai.OpenAI",
        _openai_que_responde(
            '{"achou": true, "resposta": "Dez anos.", '
            '"trechos": ["O prazo de garantia dos serviços é de 10 (dez) anos"]}'),
        raising=False)
    r = svc_doc.perguntar_sobre(
        _sessao(acervo), acervo["diretor"],
        documento_id=acervo["contrato_creche"].id,
        pergunta="qual o prazo de garantia?")
    assert r["trechos"] == [], "a citação inventada chegou à tela"
    assert r["conferido"] is False
    assert r["achou"] is False, (
        "resposta sem nenhum trecho conferível não pode se apresentar como "
        "achada — a tela usa isso para avisar que não dá para confiar")


def test_resposta_fora_do_formato_nao_derruba_a_pergunta(acervo, monkeypatch):
    monkeypatch.setattr("openai.OpenAI",
                        _openai_que_responde("isto não é json"), raising=False)
    r = svc_doc.perguntar_sobre(
        _sessao(acervo), acervo["diretor"],
        documento_id=acervo["contrato_creche"].id,
        pergunta="qual a multa?")
    assert r["achou"] is False
    assert r["resposta"]


def _sessao(acervo):
    """A sessão real, alcançada pelo objeto que veio do fixture."""
    from sqlalchemy.orm import object_session
    return object_session(acervo["contrato_creche"])


# ---------------------------------------------------------------------------
# 4. O DOCUMENTO NOVO JÁ NASCE LEGÍVEL
# ---------------------------------------------------------------------------
def test_documento_arquivado_a_mao_ja_guarda_o_texto_de_dentro(acervo):
    """O buraco mais silencioso de todos: quem arrasta um contrato para a tela
    e preenche o cadastro à mão não passava pela leitura por IA — e o documento
    era guardado SEM texto nenhum. Para a busca e para a pergunta, ele não
    existia, e ninguém tinha como perceber."""
    import fitz

    from app.apps.erp.core.arquivo import service as svc_arq

    s = _sessao(acervo)
    doc = fitz.open()
    doc.new_page().insert_text((72, 100), "Clausula unica: reajuste pelo INCC.",
                               fontsize=11)
    pdf = doc.tobytes()
    doc.close()

    obra = acervo["contrato_creche"].obra_id
    d = svc_arq.arquivar(s, pdf, "contrato-novo.pdf",
                         tipo_codigo="CONTRATO-OBRA", obra_id=obra,
                         usuario=acervo["diretor"])
    s.flush()
    assert d.texto and "reajuste pelo INCC" in d.texto, (
        "o documento foi arquivado mudo — não dá para perguntar sobre ele nem "
        "encontrá-lo pela busca por palavra")
