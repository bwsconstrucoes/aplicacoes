"""O cadastro de Suprimentos: categoria de insumo, unidade e o insumo em si.

Este arquivo nasceu de um defeito real: não havia como criar uma categoria de
insumo pela tela. Sem categoria não se cadastra insumo, sem insumo não se pede
material — o módulo inteiro ficava intransitável, e nada acusava isso.

O que não pode falhar:
  - a conta do plano oferecida para um insumo é de DESPESA ou MATERIAL;
    receita, tributo, folha e movimentação financeira não entram;
  - categoria repetida (com acento, com caixa diferente) é recusada;
  - editar uma célula sozinha não apaga as outras por omissão;
  - insumo não se apaga, desativa-se;
  - os indicadores contam o que está incompleto — é para isso que a tela serve.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.suprimentos import cadastro as svc
from app.apps.erp.db.models.cadastros import (
    Categoria, Insumo, InsumoCategoria, PerfilUsuario as P, UnidadeCompra,
)

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Marcelo")


def conta(id_, codigo, descricao, tipos, grupo="Custos de obra",
          grupo_codigo="3", ativo=True):
    return Categoria(id=id_, codigo=codigo, descricao=descricao,
                     tipos_permitidos=list(tipos), grupo_codigo=grupo_codigo,
                     grupo_nome=grupo, subgrupo_nome="Materiais aplicados",
                     descricao_uso="", ativo=ativo, natureza="RESULTADO")


CIMENTO = lambda: conta(10, "3.1.01", "Cimento e argamassas", ["T1_MATERIAL_NFE"])
RECEITA = lambda: conta(20, "1.1.01", "Receita de obras", [],
                        grupo="Receitas", grupo_codigo="1")
TRANSFERENCIA = lambda: conta(30, "9.1.01", "Transferência entre contas",
                              ["T14_EXCECAO_SEM_NOTA"],
                              grupo="Movimentações financeiras", grupo_codigo="9")
LOCACAO = lambda: conta(40, "3.3.01", "Locação de equipamentos", ["T4_LOCACAO"])


def cenario(*extras):
    return SessaoFalsa(CIMENTO(), RECEITA(), TRANSFERENCIA(), LOCACAO(),
                       UnidadeCompra(codigo="SC", descricao="Saco", ordem=1, ativo=True),
                       UnidadeCompra(codigo="UN", descricao="Unidade", ordem=2, ativo=True),
                       ADMIN, *extras)


# ---------------------------------------------------------------------------
# A conta do plano oferecida
# ---------------------------------------------------------------------------
def test_a_conta_de_receita_nao_e_oferecida_para_insumo():
    codigos = {c["codigo"] for c in svc.contas_de_compra(cenario())}
    assert "1.1.01" not in codigos, "insumo não aponta para conta de receita"


def test_a_movimentacao_financeira_nao_e_oferecida():
    codigos = {c["codigo"] for c in svc.contas_de_compra(cenario())}
    assert "9.1.01" not in codigos, "transferência entre contas não é despesa"


def test_material_e_locacao_sao_oferecidos():
    codigos = {c["codigo"] for c in svc.contas_de_compra(cenario())}
    assert {"3.1.01", "3.3.01"} <= codigos


def test_conta_desativada_sai_da_lista():
    s = SessaoFalsa(conta(11, "3.1.99", "Outros", ["T1_MATERIAL_NFE"], ativo=False))
    assert svc.contas_de_compra(s) == []


def test_a_lista_vem_agrupada_para_a_tela_montar_o_grupo():
    achou = next(c for c in svc.contas_de_compra(cenario()) if c["codigo"] == "3.1.01")
    assert achou["grupo"] == "Custos de obra"
    assert achou["subgrupo"] == "Materiais aplicados"


# ---------------------------------------------------------------------------
# Categorias de insumo — o que faltava
# ---------------------------------------------------------------------------
def test_criar_categoria_gera_codigo_sozinho():
    s = cenario()
    c = svc.criar_categoria(s, {"nome": "Cimento e argamassa"}, ADMIN)
    assert c.codigo == "CAT-0001"
    assert c.nome == "Cimento e argamassa"
    assert c.ativo is True


def test_o_codigo_continua_de_onde_parou():
    s = cenario(InsumoCategoria(id=1, codigo="CAT-0007", nome="Aço", ativo=True))
    assert svc.criar_categoria(s, {"nome": "Elétrica"}, ADMIN).codigo == "CAT-0008"


@pytest.mark.parametrize("repetido", ["Elétrica", "eletrica", "ELÉTRICA", " Elétrica "])
def test_categoria_repetida_e_recusada_mesmo_com_acento_e_caixa(repetido):
    s = cenario(InsumoCategoria(id=1, codigo="CAT-0001", nome="Elétrica", ativo=True))
    with pytest.raises(ErroValidacao, match="Já existe a categoria"):
        svc.criar_categoria(s, {"nome": repetido}, ADMIN)


def test_categoria_sem_nome_e_recusada():
    with pytest.raises(ErroValidacao, match="nome"):
        svc.criar_categoria(cenario(), {"nome": " "}, ADMIN)


def test_renomear_para_o_nome_de_outra_e_recusado():
    s = cenario(InsumoCategoria(id=1, codigo="CAT-0001", nome="Elétrica", ativo=True),
                InsumoCategoria(id=2, codigo="CAT-0002", nome="Hidráulica", ativo=True))
    with pytest.raises(ErroValidacao, match="Já existe"):
        svc.editar_categoria(s, 2, {"nome": "elétrica"}, ADMIN)


def test_renomear_a_propria_categoria_e_permitido():
    s = cenario(InsumoCategoria(id=1, codigo="CAT-0001", nome="Elétrica", ativo=True))
    c = svc.editar_categoria(s, 1, {"nome": "Elétrica e automação"}, ADMIN)
    assert c.nome == "Elétrica e automação"


def test_desativar_categoria_nao_a_apaga():
    s = cenario(InsumoCategoria(id=1, codigo="CAT-0001", nome="Elétrica", ativo=True))
    c = svc.editar_categoria(s, 1, {"ativo": False}, ADMIN)
    assert c.ativo is False
    assert s.removidos == []


def test_categoria_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.editar_categoria(cenario(), 99, {"nome": "X"}, ADMIN)


def test_a_lista_de_categorias_diz_quantos_insumos_cada_uma_tem():
    s = cenario(
        InsumoCategoria(id=1, codigo="CAT-0001", nome="Cimento", ativo=True),
        InsumoCategoria(id=2, codigo="CAT-0002", nome="Aço", ativo=True),
        Insumo(id=1, codigo="INS-0001", descricao="Cimento CP-II",
               categoria_insumo_id=1, categoria_id=10, unidade="SC", ativo=True),
        Insumo(id=2, codigo="INS-0002", descricao="Argamassa",
               categoria_insumo_id=1, categoria_id=10, unidade="SC", ativo=True),
        Insumo(id=3, codigo="INS-0003", descricao="Aço CA-50 antigo",
               categoria_insumo_id=2, categoria_id=10, unidade="UN", ativo=False))
    por_nome = {c["nome"]: c["insumos"] for c in svc.listar_categorias(s)}
    assert por_nome["Cimento"] == 2
    assert por_nome["Aço"] == 0, "insumo inativo não conta como uso"


# ---------------------------------------------------------------------------
# Unidades
# ---------------------------------------------------------------------------
def test_criar_unidade_repetida_e_recusada():
    with pytest.raises(ErroValidacao, match="já existe"):
        svc.criar_unidade(cenario(), {"codigo": "sc", "descricao": "Saco"}, ADMIN)


def test_unidade_sem_significado_escrito_e_recusada():
    with pytest.raises(ErroValidacao, match="por extenso"):
        svc.criar_unidade(cenario(), {"codigo": "RL", "descricao": ""}, ADMIN)


def test_criar_unidade_normaliza_a_sigla():
    u = svc.criar_unidade(cenario(), {"codigo": " rl ", "descricao": "Rolo"}, ADMIN)
    assert u.codigo == "RL"


# ---------------------------------------------------------------------------
# Insumos
# ---------------------------------------------------------------------------
def _categoria():
    return InsumoCategoria(id=1, codigo="CAT-0001", nome="Cimento", ativo=True)


def test_cadastro_direto_de_insumo():
    s = cenario(_categoria())
    i = svc.criar_insumo(s, {"descricao": "Cimento CP-II 50kg",
                             "categoria_insumo_id": 1, "categoria_id": 10,
                             "unidade": "SC"}, ADMIN)
    assert i.codigo == "INS-0001"
    assert i.unidade == "SC"
    assert i.ativo is True


def test_insumo_apontando_para_conta_de_receita_e_recusado():
    s = cenario(_categoria())
    with pytest.raises(ErroValidacao, match="não é conta de despesa ou material"):
        svc.criar_insumo(s, {"descricao": "Cimento CP-II", "categoria_insumo_id": 1,
                             "categoria_id": 20, "unidade": "SC"}, ADMIN)


def test_insumo_sem_categoria_de_insumo_e_recusado():
    s = cenario(_categoria())
    with pytest.raises(ErroValidacao, match="categoria de insumo"):
        svc.criar_insumo(s, {"descricao": "Cimento CP-II", "categoria_id": 10}, ADMIN)


def test_insumo_sem_conta_do_plano_e_recusado():
    s = cenario(_categoria())
    with pytest.raises(ErroValidacao, match="conta do plano"):
        svc.criar_insumo(s, {"descricao": "Cimento CP-II",
                             "categoria_insumo_id": 1}, ADMIN)


def test_insumo_com_unidade_inexistente_e_recusado():
    s = cenario(_categoria())
    with pytest.raises(ErroValidacao, match="não está cadastrada"):
        svc.criar_insumo(s, {"descricao": "Cimento CP-II", "categoria_insumo_id": 1,
                             "categoria_id": 10, "unidade": "XPT"}, ADMIN)


def test_insumo_com_o_mesmo_nome_de_outro_e_recusado():
    s = cenario(_categoria(),
                Insumo(id=1, codigo="INS-0001", descricao="Cimento CP-II 50kg",
                       categoria_insumo_id=1, categoria_id=10, unidade="SC", ativo=True))
    with pytest.raises(ErroValidacao, match="Já existe o insumo"):
        svc.criar_insumo(s, {"descricao": "cimento cp-ii 50KG",
                             "categoria_insumo_id": 1, "categoria_id": 10,
                             "unidade": "SC"}, ADMIN)


def test_editar_so_a_unidade_nao_apaga_a_conta_nem_a_categoria():
    """A tela manda uma célula sozinha. Se o resto sumisse por omissão, uma
    correção de unidade zeraria a conta do plano — e o insumo pararia de virar
    previsão de pagamento sem ninguém entender por quê."""
    s = cenario(_categoria(),
                Insumo(id=1, codigo="INS-0001", descricao="Cimento CP-II 50kg",
                       categoria_insumo_id=1, categoria_id=10, unidade="SC",
                       ativo=True, locavel=False))
    i = svc.editar_insumo(s, 1, {"unidade": "UN"}, ADMIN)
    assert i.unidade == "UN"
    assert i.categoria_id == 10
    assert i.categoria_insumo_id == 1
    assert i.descricao == "Cimento CP-II 50kg"


def test_desativar_insumo_nao_o_apaga():
    s = cenario(_categoria(),
                Insumo(id=1, codigo="INS-0001", descricao="Cimento CP-II 50kg",
                       categoria_insumo_id=1, categoria_id=10, unidade="SC",
                       ativo=True, locavel=False))
    i = svc.editar_insumo(s, 1, {"ativo": False}, ADMIN)
    assert i.ativo is False
    assert s.removidos == [], "insumo apagado levaria junto preço e solicitações"


def test_editar_insumo_para_conta_de_receita_e_recusado():
    s = cenario(_categoria(),
                Insumo(id=1, codigo="INS-0001", descricao="Cimento CP-II 50kg",
                       categoria_insumo_id=1, categoria_id=10, unidade="SC",
                       ativo=True, locavel=False))
    with pytest.raises(ErroValidacao, match="não é conta de despesa"):
        svc.editar_insumo(s, 1, {"categoria_id": 20}, ADMIN)


def test_insumo_inexistente_responde_nao_encontrado():
    with pytest.raises(ErroNaoEncontrado):
        svc.editar_insumo(cenario(_categoria()), 99, {"unidade": "UN"}, ADMIN)


# ---------------------------------------------------------------------------
# Os números do topo da tela
# ---------------------------------------------------------------------------
def test_os_indicadores_contam_o_que_esta_incompleto():
    s = cenario(
        _categoria(),
        Insumo(id=1, codigo="INS-0001", descricao="Completo",
               categoria_insumo_id=1, categoria_id=10, unidade="SC",
               ativo=True, locavel=False),
        Insumo(id=2, codigo="INS-0002", descricao="Sem conta",
               categoria_insumo_id=1, categoria_id=None, unidade="SC",
               ativo=True, locavel=False),
        Insumo(id=3, codigo="INS-0003", descricao="Sem unidade",
               categoria_insumo_id=1, categoria_id=10, unidade=None,
               ativo=True, locavel=True),
        Insumo(id=4, codigo="INS-0004", descricao="Aposentado",
               categoria_insumo_id=1, categoria_id=10, unidade="SC",
               ativo=False, locavel=False))
    k = svc.gerenciar_insumos(s)["indicadores"]
    assert k["total"] == 4
    assert k["ativos"] == 3
    assert k["inativos"] == 1
    assert k["sem_conta"] == 1
    assert k["sem_unidade"] == 1
    assert k["locaveis"] == 1


def test_a_tela_recebe_tambem_as_listas_de_escolha():
    d = svc.gerenciar_insumos(cenario(_categoria()))
    assert {"insumos", "categorias", "unidades", "contas", "indicadores"} <= set(d)
    assert all(c["codigo"] != "1.1.01" for c in d["contas"]), \
        "a tela de insumos não pode oferecer conta de receita"


def test_o_catalogo_para_escolha_traz_so_o_que_esta_ativo():
    s = cenario(_categoria(),
                InsumoCategoria(id=2, codigo="CAT-0002", nome="Aposentada", ativo=False),
                Insumo(id=1, codigo="INS-0001", descricao="Vivo",
                       categoria_insumo_id=1, categoria_id=10, unidade="SC", ativo=True),
                Insumo(id=2, codigo="INS-0002", descricao="Morto",
                       categoria_insumo_id=1, categoria_id=10, unidade="SC", ativo=False))
    d = svc.catalogo_para_escolha(s)
    assert [i["descricao"] for i in d["insumos"]] == ["Vivo"]
    assert [c["nome"] for c in d["categorias"]] == ["Cimento"]
