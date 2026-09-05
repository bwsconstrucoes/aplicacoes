"""Cadastro de Suprimentos contra Postgres de verdade.

A sessão dublada ignora `WHERE` e não guarda o que foi inserido — então o
caminho completo (criar categoria → criar insumo → aparecer na tela de gestão)
só se prova aqui. E é justamente esse caminho que estava quebrado: sem tela
para criar categoria de insumo, nada em Suprimentos podia ser testado.

Também aqui: os dados de exemplo, criados e removidos de ponta a ponta, com a
recusa quando algo já foi usado de verdade.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros.plano_padrao import aplicar_plano
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import cadastro as svc
from app.apps.erp.core.suprimentos import exemplo as svc_exemplo
from app.apps.erp.core.suprimentos import fornecedores as svc_forn
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Insumo, InsumoCategoria, Obra, PerfilUsuario as P,
    SuprimentoItem, SuprimentoSolicitacao, TipoTitulo, Usuario,
)

pytestmark = pytest.mark.banco


def _admin(s, email="admin.cadastro@teste.bws.local"):
    u = Usuario(nome="Admin do cadastro", email=email,
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    s.add(u)
    s.flush()
    return u


@pytest.fixture
def plano(sessao_real):
    """O plano financeiro padrão, como a empresa carrega pela tela.

    O banco de teste nasce vazio de plano — ele é carga de uma vez, não
    migração. Sem isto não há conta de despesa para um insumo apontar.
    """
    aplicar_plano(sessao_real)
    sessao_real.flush()
    return sessao_real


def _conta(s, codigo, descricao, tipos, grupo_codigo="3", grupo="Custos de obra"):
    c = Categoria(codigo=codigo, descricao=descricao,
                  tipos_permitidos=[TipoTitulo(t) for t in tipos],
                  grupo_codigo=grupo_codigo, grupo_nome=grupo,
                  subgrupo_nome="Materiais aplicados", natureza="RESULTADO")
    s.add(c)
    s.flush()
    return c


# ---------------------------------------------------------------------------
# O caminho que estava quebrado
# ---------------------------------------------------------------------------
def test_criar_categoria_e_depois_o_insumo_que_a_usa(sessao_real):
    s = sessao_real
    usuario = _admin(s)
    conta = _conta(s, "3.1.90", "Cimento de teste", ["T1_MATERIAL_NFE"])

    categoria = svc.criar_categoria(s, {"nome": "Cimento de teste"}, usuario)
    s.flush()
    insumo = svc.criar_insumo(s, {"descricao": "Cimento CP-II de teste 50kg",
                                  "categoria_insumo_id": categoria.id,
                                  "categoria_id": conta.id,
                                  "unidade": "SC"}, usuario)
    s.flush()

    tela = svc.gerenciar_insumos(s)
    linha = next(l for l in tela["insumos"] if l["id"] == insumo.id)
    assert linha["categoria_insumo"] == "Cimento de teste"
    assert linha["conta"].startswith("3.1.90")
    assert linha["unidade"] == "SC"


def test_a_categoria_repetida_e_recusada_pela_regra_antes_do_banco(sessao_real):
    s = sessao_real
    usuario = _admin(s)
    svc.criar_categoria(s, {"nome": "Repetida de teste"}, usuario)
    s.flush()
    with pytest.raises(ErroValidacao, match="Já existe a categoria"):
        svc.criar_categoria(s, {"nome": "REPETIDA DE TESTE"}, usuario)


def test_a_lista_de_contas_de_compra_exclui_receita_no_plano_de_verdade(plano):
    """O plano de contas real é carregado pelas migrações. Se a regra de
    filtragem estiver errada, é aqui que aparece — o dublê não tem plano."""
    codigos = {c["codigo"] for c in svc.contas_de_compra(plano)}
    assert codigos, "o plano padrão deveria ter contas de compra"
    assert not any(c.startswith("1.") for c in codigos), "receita não é insumo"
    assert not any(c.startswith("9.") for c in codigos), \
        "movimentação financeira não é insumo"
    assert "3.1.01" in codigos, "cimento tem de estar entre as contas oferecidas"


def test_editar_uma_celula_sozinha_no_banco_preserva_o_resto(sessao_real):
    s = sessao_real
    usuario = _admin(s)
    conta = _conta(s, "3.1.91", "Conta de teste", ["T1_MATERIAL_NFE"])
    categoria = svc.criar_categoria(s, {"nome": "Categoria de teste"}, usuario)
    s.flush()
    insumo = svc.criar_insumo(s, {"descricao": "Insumo de teste para editar",
                                  "categoria_insumo_id": categoria.id,
                                  "categoria_id": conta.id, "unidade": "SC"}, usuario)
    s.flush()

    svc.editar_insumo(s, insumo.id, {"descricao": "Insumo de teste renomeado"}, usuario)
    s.flush()
    s.refresh(insumo)
    assert insumo.descricao == "Insumo de teste renomeado"
    assert insumo.categoria_id == conta.id
    assert insumo.unidade == "SC"


# ---------------------------------------------------------------------------
# Fornecedor: as categorias que decidem quem recebe cotação
# ---------------------------------------------------------------------------
def test_marcar_e_desmarcar_categorias_do_fornecedor(sessao_real):
    s = sessao_real
    usuario = _admin(s)
    cimento = svc.criar_categoria(s, {"nome": "Cimento p/ fornecedor"}, usuario)
    aco = svc.criar_categoria(s, {"nome": "Aço p/ fornecedor"}, usuario)
    s.flush()

    forn = svc_forn.criar(s, {
        "tipo_pessoa": "PJ", "cnpj_cpf": svc_exemplo._digito_cnpj("710000990001"),
        "razao_social": "FORNECEDOR DE TESTE LTDA",
        "porte": "DISTRIBUIDOR", "regioes_atuacao": ["CE"],
        "canais_cotacao": ["EMAIL"], "categorias": [cimento.id, aco.id],
        "email": "fulano@fornecedordeteste.exemplo",
        "contato_nome": "Fulano de Teste"}, usuario)
    s.flush()

    assert sorted(svc_forn.para_cotar(s, [cimento.id])[0]["categorias_ids"]) == \
        sorted([cimento.id, aco.id])

    svc_forn.editar(s, forn.id, {"categorias": [aco.id]}, usuario)
    s.flush()
    assert [f["id"] for f in svc_forn.para_cotar(s, [cimento.id])] == []
    assert [f["id"] for f in svc_forn.para_cotar(s, [aco.id])] == [forn.id]


# ---------------------------------------------------------------------------
# Dados de exemplo, de ponta a ponta
# ---------------------------------------------------------------------------
def test_trazer_e_remover_os_dados_de_exemplo(plano):
    s = plano
    usuario = _admin(s)
    s.add(Obra(codigo="EX-01", nome="Obra de exemplo", status="ATIVA"))
    s.flush()

    antes_insumos = len(s.query(Insumo).all())
    antes_fornecedores = len(s.query(Fornecedor).all())

    relatorio = svc_exemplo.criar(s, usuario)
    s.flush()
    assert relatorio["marcas"]["insumo_categorias"] == len(svc_exemplo.CATALOGO)
    assert relatorio["marcas"]["fornecedores"] == len(svc_exemplo.FORNECEDORES)
    assert relatorio["marcas"]["suprimento_solicitacoes"] == len(svc_exemplo.SOLICITACOES)
    assert svc_exemplo.situacao(s)["presente"] is True

    svc_exemplo.remover(s, usuario)
    s.flush()
    assert len(s.query(Insumo).all()) == antes_insumos
    assert len(s.query(Fornecedor).all()) == antes_fornecedores
    assert svc_exemplo.situacao(s)["presente"] is False


def test_o_exemplo_nao_se_traz_duas_vezes(plano):
    s = plano
    usuario = _admin(s)
    svc_exemplo.criar(s, usuario)
    s.flush()
    with pytest.raises(ErroValidacao, match="já estão no sistema"):
        svc_exemplo.criar(s, usuario)


def test_a_remocao_e_recusada_se_o_insumo_de_exemplo_ja_foi_usado(plano):
    s = plano
    usuario = _admin(s)
    obra = Obra(codigo="EX-02", nome="Obra de exemplo 2", status="ATIVA")
    s.add(obra)
    s.flush()
    svc_exemplo.criar(s, usuario)
    s.flush()

    # Um pedido DE VERDADE usando um insumo que veio do exemplo.
    insumo = s.query(Insumo).filter(
        Insumo.descricao == "Cimento CP-II-Z 32 saco 50kg").one()
    sol = SuprimentoSolicitacao(numero="SS-EXEMPLO-1", titulo="pedido de verdade",
                                solicitante_id=usuario.id)
    s.add(sol)
    s.flush()
    s.add(SuprimentoItem(solicitacao_id=sol.id, numero=1, insumo_id=insumo.id,
                         quantidade=Decimal("5"), unidade="SC", obra_id=obra.id))
    s.flush()

    with pytest.raises(ErroValidacao, match="solicitação de verdade"):
        svc_exemplo.remover(s, usuario)
    assert svc_exemplo.situacao(s)["presente"] is True, "nada foi removido"
