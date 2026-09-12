"""Excluir documento do Arquivo é DESTRUTIVO — e precisa do mesmo recorte da tela.

A falha, achada na varredura de 11/09/2026: `excluir` não conferia nada.
Quem tem a ação "arquivar" (hoje: ADMIN, diretoria, FINANCEIRO, DP e gestor de
obra) apagava QUALQUER documento pelo número, inclusive de faixa de sigilo que
ele nem enxerga na tela — o FINANCEIRO e o gestor de obra não veem documento
PESSOAL, e podiam apagar um.

Apagar leva junto o arquivo guardado. Não é "ver o que não devia": é destruir o
que não devia.

COM BANCO DE VERDADE porque o recorte vive no `WHERE` e o dublê da suíte o
ignora.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select

from app.apps.erp.core.arquivo import service as svc_arq
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.db.models.cadastros import (
    Colaborador, EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import Anexo, Documento, DocumentoTipo

pytestmark = pytest.mark.banco


@pytest.fixture
def acervo(sessao_real):
    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    aberto = DocumentoTipo(codigo="CONTRATO-OBRA", nome="Contrato da obra",
                           grupo="OBRA", dono="OBRA", sigilo="ABERTO")
    pessoal = DocumentoTipo(codigo="FOLHA", nome="Documento de pessoal",
                            grupo="PESSOAL", dono="PESSOA", sigilo="PESSOAL")
    pedreiro = Colaborador(nome="José da Silva", cpf="12345678901")
    s.add_all([creche, escola, aberto, pessoal, pedreiro])
    s.flush()

    def arquivar(tipo, nome, obra=None, colaborador=None):
        anexo = Anexo(entidade_tipo="documento", entidade_id=0,
                      nome_arquivo=nome, mime_type="application/pdf",
                      conteudo=b"x", hash_sha256=f"hash-{nome}",
                      guardado_em="BANCO")
        s.add(anexo)
        s.flush()
        d = Documento(tipo_codigo=tipo.codigo, anexo_id=anexo.id,
                      nome_padronizado=nome,
                      obra_id=obra.id if obra else None,
                      colaborador_id=colaborador.id if colaborador else None)
        s.add(d)
        s.flush()
        return d

    def pessoa(nome, email, perfil, escopo=None):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil,
                    escopo_visao=escopo or EscopoVisao.PROPRIOS)
        s.add(u)
        s.flush()
        return u

    dono = pessoa("Marcelo", "chefe@teste.bws.local", P.ADMIN)
    financeiro = pessoa("Financeiro", "fin@teste.bws.local", P.FINANCEIRO)
    da_creche = pessoa("Adm da Creche", "adm@teste.bws.local",
                       P.ADMINISTRATIVO_OBRA, EscopoVisao.OBRAS_DESIGNADAS)
    s.add(UsuarioObra(usuario_id=da_creche.id, obra_id=creche.id))
    s.flush()
    return {
        "s": s, "dono": dono, "financeiro": financeiro, "da_creche": da_creche,
        "contrato_creche": arquivar(aberto, "Contrato Creche", creche),
        "contrato_escola": arquivar(aberto, "Contrato Escola", escola),
        "folha": arquivar(pessoal, "Acordo de jornada", colaborador=pedreiro),
    }


def _existe(s, doc_id):
    return s.scalar(select(Documento.id).where(Documento.id == doc_id)) is not None


def test_financeiro_nao_apaga_documento_de_faixa_que_nao_enxerga(acervo):
    """O FINANCEIRO não vê documento PESSOAL na tela. Também não pode apagá-lo
    pelo número."""
    d = acervo
    with pytest.raises(ErroNaoEncontrado):
        svc_arq.excluir(d["s"], d["folha"].id, d["financeiro"])
    assert _existe(d["s"], d["folha"].id)


def test_preso_a_obra_nao_apaga_documento_de_outra_obra(acervo):
    """Quem responde pela creche não apaga o contrato da escola — mesmo tendo
    a ação de arquivar concedida no cadastro."""
    d = acervo
    with pytest.raises(ErroNaoEncontrado):
        svc_arq.excluir(d["s"], d["contrato_escola"].id, d["da_creche"])
    assert _existe(d["s"], d["contrato_escola"].id)


def test_a_recusa_e_igual_a_de_documento_inexistente(acervo):
    """Dizer "sem permissão" para um número que existe confirmaria que ele
    existe, e varrer os números mapearia o acervo."""
    d = acervo
    with pytest.raises(ErroNaoEncontrado) as fora:
        svc_arq.excluir(d["s"], d["contrato_escola"].id, d["da_creche"])
    with pytest.raises(ErroNaoEncontrado) as inexistente:
        svc_arq.excluir(d["s"], 99999999, d["da_creche"])
    assert str(fora.value) == str(inexistente.value)


def test_dentro_do_escopo_continua_apagando(acervo):
    """A trava não pode virar impedimento: o que é da pessoa, ela apaga."""
    d = acervo
    svc_arq.excluir(d["s"], d["contrato_creche"].id, d["da_creche"])
    d["s"].flush()
    assert not _existe(d["s"], d["contrato_creche"].id)


def test_o_dono_apaga_qualquer_um(acervo):
    d = acervo
    svc_arq.excluir(d["s"], d["folha"].id, d["dono"])
    d["s"].flush()
    assert not _existe(d["s"], d["folha"].id)
