"""PROJETO agrupa obras, e o alcance do operador passa a ter três alturas.

Pedido do dono em 13/09/2026, em duas partes que se encaixam:

  *"com projetos eu faço uma associação de algumas obras e coloco todas dentro
  do projeto (…) tudo que eu for visualizar em relação a elas — relatórios,
  resultados, custos — eu poder visualizar o projeto, ou seja, o somatório
  daquelas obras"*

  *"a gente poder adicionar ao usuário a obra, ou um projeto, ou todas as
  obras, ou uma empresa ou outra empresa"*

COM BANCO DE VERDADE, e não com o dublê, porque **tudo isto vive no `WHERE`**:
o alcance é resolvido na consulta (e não copiado na marcação), e o somatório
por projeto é um `GROUP BY` com `JOIN`. O dublê devolveria tudo e os testes
passariam sem provar nada.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth import permissoes
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import projetos as svc_projetos
from app.apps.erp.core.cadastros import vinculos
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.relatorios import resumo
from app.apps.erp.db.models.cadastros import (
    Categoria, Empresa, EscopoVisao, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    FormaPagamento, Rateio, StatusTitulo, TipoTitulo, Titulo,
)
from sqlalchemy import select

pytestmark = pytest.mark.banco

SENHA = gerar_hash("senha-de-teste-1234")


@pytest.fixture
def cenario(sessao_real):
    """Duas empresas, um projeto com duas obras, e uma obra solta em cada uma."""
    s = sessao_real
    alfa = Empresa(razao_social="Construtora Alfa", cnpj="11222333000181")
    beta = Empresa(razao_social="Construtora Beta", cnpj="11222333000262")
    forn = Fornecedor(razao_social="Pedreira Exemplo", cnpj_cpf="71000004000127",
                      tipo_pessoa=TipoPessoa.PJ, ativo=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="9.9.96", descricao="Conta do teste de projetos",
                    natureza="RESULTADO")
    s.add_all([alfa, beta, forn, cat])
    s.flush()

    creche1 = Obra(codigo="CRECHE-1", nome="Creche do Eusébio", empresa_id=alfa.id)
    creche2 = Obra(codigo="CRECHE-2", nome="Creche do Pecém", empresa_id=alfa.id)
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto", empresa_id=alfa.id)
    posto = Obra(codigo="POSTO", nome="Posto de saúde", empresa_id=beta.id)
    s.add_all([creche1, creche2, escola, posto])
    s.flush()

    # Quem lançou: os títulos exigem solicitante, e aqui ele é só o autor —
    # o que se testa é a OBRA do rateio, não a autoria.
    autor = Usuario(nome="Quem lançou", email="autor@teste.bws.local",
                    senha_hash=SENHA, perfil=P.FINANCEIRO,
                    ve_todas_as_obras=True)
    s.add(autor)
    s.flush()

    projeto = svc_projetos.criar(s, {
        "codigo": "creches-2026", "nome": "Creches 2026",
        "obras": [creche1.id, creche2.id]}, None)

    def gasto(obra, valor):
        t = Titulo(numero_sp=f"SP-{obra.codigo}", tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao=f"Brita da {obra.codigo}",
                   valor_bruto=Decimal(valor), valor_liquido=Decimal(valor),
                   competencia=date(2026, 9, 1), categoria_id=cat.id,
                   forma_pagamento=FormaPagamento.PIX,
                   status=StatusTitulo.APROVADO, solicitante_id=autor.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal(valor),
                     percentual=Decimal("100.0000")))
        s.flush()
        return t

    titulos = {o.codigo: gasto(o, v) for o, v in
               ((creche1, "1000"), (creche2, "2000"), (escola, "400"), (posto, "700"))}
    s.commit()
    return {"s": s, "alfa": alfa, "beta": beta, "projeto": projeto,
            "creche1": creche1, "creche2": creche2, "escola": escola,
            "posto": posto, "titulos": titulos}


def _pessoa(s, chave, **marcas):
    u = Usuario(nome=f"Teste {chave}", email=f"{chave}@teste.bws.local",
                senha_hash=SENHA, perfil=P.ADMINISTRATIVO_OBRA,
                ve_todas_as_obras=False,
                escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s.add(u)
    s.flush()
    for obra in marcas.get("obras", ()):
        s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
    vinculos.definir_alcance_do_operador(
        s, u.id,
        projetos=[p["id"] if isinstance(p, dict) else p.id
                  for p in marcas.get("projetos", ())],
        empresas=[e.id for e in marcas.get("empresas", ())])
    s.flush()
    return u


def _titulos_visiveis(s, usuario) -> set[str]:
    stmt = permissoes.aplicar_escopo(select(Titulo.numero_sp), s, usuario)
    return set(s.scalars(stmt).all())


# ---------------------------------------------------------------------------
# 1. O alcance dito no PROJETO alcança as obras dele
# ---------------------------------------------------------------------------
def test_quem_tem_o_projeto_enxerga_as_obras_do_projeto(cenario):
    d = cenario
    pessoa = _pessoa(d["s"], "do-projeto", projetos=[d["projeto"]])
    assert _titulos_visiveis(d["s"], pessoa) == {"SP-CRECHE-1", "SP-CRECHE-2"}


def test_obra_que_entra_no_projeto_DEPOIS_ja_entra_no_alcance(cenario):
    """A promessa que fez o alcance ser resolvido na consulta, e não copiado.

    Sem isto, cada obra nova exigiria voltar no cadastro de cada pessoa — e é
    exatamente o tipo de passo que se esquece, deixando gente sem ver o que
    deveria (ou, pior, vendo o que não deveria).
    """
    d = cenario
    pessoa = _pessoa(d["s"], "do-projeto-2", projetos=[d["projeto"]])
    assert "SP-ESCOLA" not in _titulos_visiveis(d["s"], pessoa)

    svc_projetos.editar(d["s"], d["projeto"]["id"], {
        "obras": [d["creche1"].id, d["creche2"].id, d["escola"].id]}, None)
    d["s"].flush()
    assert _titulos_visiveis(d["s"], pessoa) == {
        "SP-CRECHE-1", "SP-CRECHE-2", "SP-ESCOLA"}


def test_quem_tem_a_EMPRESA_enxerga_as_obras_daquele_cnpj(cenario):
    d = cenario
    pessoa = _pessoa(d["s"], "da-alfa", empresas=[d["alfa"]])
    assert _titulos_visiveis(d["s"], pessoa) == {
        "SP-CRECHE-1", "SP-CRECHE-2", "SP-ESCOLA"}
    assert "SP-POSTO" not in _titulos_visiveis(d["s"], pessoa)


def test_as_tres_alturas_se_somam(cenario):
    """Empresa, projeto e obra não são exclusivos: quem tem os três, soma."""
    d = cenario
    pessoa = _pessoa(d["s"], "somado", obras=[d["posto"]], projetos=[d["projeto"]])
    assert _titulos_visiveis(d["s"], pessoa) == {
        "SP-CRECHE-1", "SP-CRECHE-2", "SP-POSTO"}


def test_sem_nada_marcado_nao_enxerga_nada(cenario):
    """O padrão continua sendo NEGAR — inclusive com projeto no sistema."""
    d = cenario
    pessoa = _pessoa(d["s"], "sem-nada")
    assert _titulos_visiveis(d["s"], pessoa) == set()


def test_o_detalhe_concorda_com_a_listagem(cenario):
    """Listagem e detalhe passam pelo mesmo `aplicar_escopo` — e é isso que
    impede a URL de furar o recorte."""
    d = cenario
    pessoa = _pessoa(d["s"], "conferindo", projetos=[d["projeto"]])
    visiveis = _titulos_visiveis(d["s"], pessoa)
    for codigo, titulo in d["titulos"].items():
        esperado = f"SP-{codigo}" in visiveis
        assert permissoes.pode_ver_titulo(d["s"], pessoa, titulo.id) is esperado


def test_tirar_a_obra_do_projeto_tira_ela_do_alcance(cenario):
    d = cenario
    pessoa = _pessoa(d["s"], "perdeu", projetos=[d["projeto"]])
    svc_projetos.editar(d["s"], d["projeto"]["id"],
                        {"obras": [d["creche1"].id]}, None)
    d["s"].flush()
    assert _titulos_visiveis(d["s"], pessoa) == {"SP-CRECHE-1"}


# ---------------------------------------------------------------------------
# 2. O somatório por projeto no relatório
# ---------------------------------------------------------------------------
def test_o_relatorio_soma_as_obras_do_projeto(cenario):
    """É o que ele pediu em uma frase: ver o projeto, não obra a obra."""
    d = cenario
    quem_ve_tudo = Usuario(nome="Diretor", email="dir@teste.bws.local",
                           senha_hash=SENHA, perfil=P.DIRETOR_FINANCEIRO,
                           ve_todas_as_obras=True)
    d["s"].add(quem_ve_tudo)
    d["s"].flush()

    r = resumo(d["s"], "projeto", {"especie": "pagar"}, quem_ve_tudo)
    por_chave = {l["chave"]: l["total"] for l in r["linhas"]}
    assert por_chave["CRECHES-2026 · Creches 2026"] == 3000.0   # 1000 + 2000
    # Obra fora de projeto não some: cai numa linha própria, com esse nome.
    assert por_chave["Sem projeto"] == 1100.0                   # 400 + 700


def test_o_somatorio_do_projeto_respeita_o_recorte_de_quem_pergunta(cenario):
    """Duas pessoas fazendo a MESMA pergunta recebem números diferentes, e é o
    certo: o relatório por projeto passa pelo mesmo recorte do resto."""
    d = cenario
    pessoa = _pessoa(d["s"], "so-uma-creche", obras=[d["creche1"]])
    r = resumo(d["s"], "projeto", {"especie": "pagar"}, pessoa)
    por_chave = {l["chave"]: l["total"] for l in r["linhas"]}
    assert por_chave == {"CRECHES-2026 · Creches 2026": 1000.0}


# ---------------------------------------------------------------------------
# 3. O cadastro do projeto
# ---------------------------------------------------------------------------
def test_obra_pertence_a_um_projeto_so(cenario):
    """Dois projetos com a mesma obra fariam o somatório contá-la duas vezes."""
    d = cenario
    outro = svc_projetos.criar(d["s"], {"codigo": "OUTRO", "nome": "Outro projeto",
                                        "obras": [d["creche1"].id]}, None)
    d["s"].flush()
    assert {o["codigo"] for o in svc_projetos.obter(
        d["s"], outro["id"])["obras"]} == {"CRECHE-1"}
    assert {o["codigo"] for o in svc_projetos.obter(
        d["s"], d["projeto"]["id"])["obras"]} == {"CRECHE-2"}


def test_nao_se_arquiva_projeto_com_obra_dentro(cenario):
    d = cenario
    with pytest.raises(ErroValidacao):
        svc_projetos.arquivar(d["s"], d["projeto"]["id"], None)
    svc_projetos.editar(d["s"], d["projeto"]["id"], {"obras": []}, None)
    assert svc_projetos.arquivar(
        d["s"], d["projeto"]["id"], None)["ativo"] is False


def test_codigo_repetido_e_recusado(cenario):
    d = cenario
    with pytest.raises(ErroValidacao):
        svc_projetos.criar(d["s"], {"codigo": "CRECHES-2026",
                                    "nome": "Outro com o mesmo código"}, None)


def test_a_listagem_nao_entrega_o_nome_das_obras(cenario):
    """A lista é aberta a quem entra no ERP porque o projeto é filtro de tela.
    Por isso ela não traz o nome das obras de dentro — quem é preso a uma obra
    não tem por que saber o nome das outras. O detalhe, esse exige configurar.
    """
    d = cenario
    for p in svc_projetos.listar(d["s"]):
        assert "obras" not in p
        assert p["quantas_obras"] >= 0


# ---------------------------------------------------------------------------
# 4. A pergunta do assistente sobre o custo de um PROJETO
# ---------------------------------------------------------------------------
def test_o_assistente_responde_o_custo_somado_do_projeto(cenario):
    """*"quanto custou o projeto Creches 2026"* tem de vir somado, e não obra
    a obra — é a frase do dono virada resposta."""
    from app.apps.erp.core.perguntas.respostas import custo_da_obra

    d = cenario
    quem = Usuario(nome="Diretor", email="dir2@teste.bws.local", senha_hash=SENHA,
                   perfil=P.DIRETOR_FINANCEIRO, ve_todas_as_obras=True)
    d["s"].add(quem)
    d["s"].flush()

    r = custo_da_obra(d["s"], quem, projeto="Creches 2026")
    assert len(r["linhas"]) == 1
    assert r["linhas"][0]["comprometido"] == 3000.0
    assert "CRECHES" in r["frase"] and "2 obra(s)" in r["frase"]
    assert r["colunas"][0]["rotulo"] == "Projeto"


def test_projeto_que_nao_existe_recebe_nao_sei_e_nao_um_chute(cenario):
    """Número errado com cara de certo é pior que resposta nenhuma."""
    from app.apps.erp.core.perguntas.respostas import custo_da_obra

    d = cenario
    quem = Usuario(nome="Diretor", email="dir3@teste.bws.local", senha_hash=SENHA,
                   perfil=P.DIRETOR_FINANCEIRO, ve_todas_as_obras=True)
    d["s"].add(quem)
    d["s"].flush()
    r = custo_da_obra(d["s"], quem, projeto="Projeto que nunca existiu")
    assert r["linhas"] == []
    assert "Não achei o projeto" in r["frase"]
