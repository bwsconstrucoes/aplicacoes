"""A nota SOLTA é assunto de quem cruza. As demais pessoas veem o que já casou.

Decisão do dono em 12/09/2026: *"esse negócio de ver as notas acho que deve
ficar restrito ao pessoal do financeiro. Demais verão notas que já estão
associadas"*.

Por que a trava é a AÇÃO de cruzar, e não o cargo: hoje ela é do financeiro por
cargo, mas o ERP permite marcá-la numa pessoa (migração 032), e é justamente o
caso do comprador — é ele quem sabe de que pedido cada nota é. Amarrar ao cargo
faria a regra mentir no dia em que o dono marcasse a caixinha para alguém.

E o motivo de fechar: a nota solta ou é compra legítima que ninguém lançou, ou
é nota emitida contra a empresa sem autorização. Nos dois casos, quem não pode
cruzar não pode fazer nada com ela.

COM BANCO DE VERDADE porque o recorte vive no `WHERE` e passa por `JOIN`.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.core.notas import cruzamento
from app.apps.erp.db.models.cadastros import (
    Categoria, Empresa, EscopoVisao, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario, UsuarioObra, UsuarioPermissao,
)
from app.apps.erp.db.models.financeiro import (
    DocumentoFiscal, FormaPagamento, Rateio, SituacaoNota, StatusTitulo,
    TipoDocFiscal, TipoTitulo, Titulo, TituloItem,
)

pytestmark = pytest.mark.banco

CNPJ_NOSSO = "11222333000181"
CNPJ_FORN = "71000004000127"


@pytest.fixture
def notas(sessao_real):
    s = sessao_real
    emp = Empresa(razao_social="BWS Construções Exemplo", cnpj=CNPJ_NOSSO)
    forn = Fornecedor(razao_social="Pedreira Exemplo", cnpj_cpf=CNPJ_FORN,
                      tipo_pessoa=TipoPessoa.PJ, ativo=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    cat = Categoria(codigo="9.9.97", descricao="Conta do teste de notas")
    s.add_all([emp, forn, creche, escola, cat])
    s.flush()

    def pessoa(nome, email, perfil, obra=None, acoes=()):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil,
                    escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
        s.add(u)
        s.flush()
        if obra is not None:
            s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
        for acao in acoes:
            s.add(UsuarioPermissao(usuario_id=u.id, acao=acao, concedida=True))
        s.flush()
        return u

    financeiro = pessoa("Financeiro", "fin@teste.bws.local", P.FINANCEIRO)
    gestor = pessoa("Gestor de obras", "gestor@teste.bws.local", P.GESTOR_OBRA)
    supervisor = pessoa("Supervisor da creche", "sup@teste.bws.local",
                        P.SUPERVISOR_OBRA, creche)
    # O comprador NÃO é do financeiro por cargo — a ação é marcada nele.
    comprador = pessoa("Comprador", "compras@teste.bws.local", P.LANCADOR,
                       acoes=("cruzar_notas",))

    def titulo(sp, obra):
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao="brita",
                   valor_bruto=Decimal("500"), valor_liquido=Decimal("500"),
                   competencia=date(2026, 9, 1), categoria_id=cat.id,
                   forma_pagamento=FormaPagamento.PIX,
                   status=StatusTitulo.APROVADO, solicitante_id=financeiro.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal("500"),
                     percentual=Decimal("100.0000")))
        it = TituloItem(titulo_id=t.id, ordem=1, descricao="Compra na obra",
                        valor=Decimal("500"), data_despesa=date(2026, 9, 1),
                        obra_id=obra.id)
        s.add(it)
        s.flush()
        return it

    def nota(chave, numero, item=None):
        n = DocumentoFiscal(
            tipo=TipoDocFiscal.NFE, chave_acesso=chave, numero=numero, serie="1",
            emitente_doc=CNPJ_FORN, emitente_nome="Pedreira Exemplo",
            destinatario_doc=CNPJ_NOSSO, valor_total=Decimal("500"),
            data_emissao=date(2026, 9, 5), situacao=SituacaoNota.DESCONHECIDA,
            origem="TESTE", empresa_id=emp.id,
            titulo_item_id=item.id if item else None,
            conferencia="CASADA" if item else "PENDENTE")
        s.add(n)
        s.flush()
        return n

    return {
        "s": s, "financeiro": financeiro, "gestor": gestor,
        "supervisor": supervisor, "comprador": comprador,
        "solta": nota("1" * 44, "1001"),
        "da_creche": nota("2" * 44, "1002", titulo("SP-CRE-1", creche)),
        "da_escola": nota("3" * 44, "1003", titulo("SP-ESC-1", escola)),
    }


def _numeros(s, usuario):
    return {n["numero"] for n in cruzamento.listar(s, usuario=usuario)["notas"]}


def test_o_financeiro_ve_a_nota_solta(notas):
    """É dele o assunto: ou é compra que ninguém lançou, ou é nota emitida
    contra a empresa sem autorização."""
    d = notas
    assert _numeros(d["s"], d["financeiro"]) == {"1001", "1002", "1003"}


def test_o_comprador_tambem_ve_a_solta_porque_ele_cruza(notas):
    """A trava é a AÇÃO, não o cargo — e o comprador recebeu a ação marcada no
    cadastro dele. É ele quem sabe de que pedido a nota é."""
    d = notas
    assert "1001" in _numeros(d["s"], d["comprador"])


def test_o_gestor_de_obras_so_ve_as_ja_associadas(notas):
    """Ele enxerga todas as obras, mas não cruza nota: a solta não é assunto
    dele."""
    d = notas
    assert _numeros(d["s"], d["gestor"]) == {"1002", "1003"}


def test_o_supervisor_ve_so_as_associadas_da_obra_dele(notas):
    """As duas travas valendo juntas: já associada E da obra dele."""
    d = notas
    assert _numeros(d["s"], d["supervisor"]) == {"1002"}


def test_abrir_pela_url_nao_fura_o_recorte(notas):
    """Listagem não é trava: o detalhe por número tem de recusar igual."""
    d = notas
    cruzamento.exigir_nota_no_escopo(d["s"], d["financeiro"], d["solta"].id)
    for alvo in (d["solta"], d["da_escola"]):
        with pytest.raises(ErroNaoEncontrado):
            cruzamento.exigir_nota_no_escopo(d["s"], d["supervisor"], alvo.id)


def test_a_recusa_e_igual_a_de_nota_inexistente(notas):
    d = notas
    with pytest.raises(ErroNaoEncontrado) as fora:
        cruzamento.exigir_nota_no_escopo(d["s"], d["supervisor"], d["solta"].id)
    with pytest.raises(ErroNaoEncontrado) as inexistente:
        cruzamento.exigir_nota_no_escopo(d["s"], d["supervisor"], 99999999)
    assert str(fora.value) == str(inexistente.value)


def test_o_resumo_da_tela_conta_o_que_a_pessoa_ve(notas):
    """O resumo é somado sobre as linhas devolvidas — se ele contasse a base
    inteira, o número de 'pendentes' entregaria a existência da nota solta."""
    d = notas
    resumo = cruzamento.listar(d["s"], usuario=d["supervisor"])["resumo"]
    assert resumo["quantidade"] == 1
    assert resumo["pendentes"] == 0
