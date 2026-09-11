"""A conta nova entra no GRUPO dela, e não num limbo.

O dono cadastrou uma conta dentro de "Custos de obra" em 10/09/2026 e ela não
apareceu lá: *"ele cadastrou, porque acusa que aquela numeração já está sendo
utilizada. Então é erro de visualização."*

Estava gravada mesmo — e sem grupo. O plano da BWS tem três níveis ("3.1.01" é
a conta 01 do subgrupo 3.1, dentro do grupo 3), e o cadastro pela tela não
preenchia grupo nem subgrupo. Efeito: a conta ia para um "Sem grupo" no alto da
lista, longe de onde quem a criou foi procurar, e ficava fora dos totais por
grupo no relatório.

COM BANCO DE VERDADE porque tudo aqui depende de `WHERE` — achar a conta irmã,
achar a maior ordem do subgrupo, recusar código repetido —, e a sessão dublada
ignora `WHERE`.

O que se prova:

  1. O grupo e o subgrupo saem do CÓDIGO da conta.
  2. Os NOMES saem de uma conta irmã — senão a conta nova entraria com um
     rótulo diferente do das vizinhas, que é outro jeito de parecer sumida.
  3. Conta feita à mão nasce PERSONALIZADA: "Instalar plano padrão" não pode
     reescrever a descrição que o dono escolheu.
  4. O conserto das que já nasceram tortas não inventa nada nem encosta em
     conta que já está no lugar.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import categorias as svc
from app.apps.erp.db.models.cadastros import (Categoria, PerfilUsuario as P,
                                              Usuario)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    """Duas contas de verdade do plano, para servirem de irmãs."""
    s = sessao_real
    admin = Usuario(nome="Admin plano", email="plano@teste.bws.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    s.add_all([
        admin,
        Categoria(codigo="3.1.01", descricao="Cimento e Concreto",
                  grupo_codigo="3", grupo_nome="Custos de obra",
                  subgrupo_codigo="3.1", subgrupo_nome="Material", ordem=5),
        Categoria(codigo="3.2.01", descricao="Mão de obra própria",
                  grupo_codigo="3", grupo_nome="Custos de obra",
                  subgrupo_codigo="3.2", subgrupo_nome="Mão de obra", ordem=9),
    ])
    s.flush()
    return {"s": s, "admin": admin}


def test_a_conta_nova_cai_no_grupo_e_no_subgrupo_do_codigo(cenario):
    cat = svc.criar(cenario["s"], {"codigo": "3.1.97",
                                   "descricao": "Outros materiais"}, cenario["admin"])

    assert cat.grupo_codigo == "3"
    assert cat.grupo_nome == "Custos de obra"
    assert cat.subgrupo_codigo == "3.1"
    assert cat.subgrupo_nome == "Material"


def test_a_conta_nova_fica_depois_das_irmas_do_subgrupo(cenario):
    """Ordem 0 jogaria a conta nova para o começo da lista inteira."""
    cat = svc.criar(cenario["s"], {"codigo": "3.1.97",
                                   "descricao": "Outros materiais"}, cenario["admin"])

    assert cat.ordem == 6, "logo depois da 3.1.01, que está em 5"


def test_conta_feita_a_mao_nasce_personalizada(cenario):
    cat = svc.criar(cenario["s"], {"codigo": "3.1.97",
                                   "descricao": "Do jeito do dono"}, cenario["admin"])

    assert cat.personalizada is True


def test_subgrupo_que_ainda_nao_existe_usa_o_codigo_como_nome(cenario):
    """Feio, mas visível e corrigível — melhor que a conta sumir da tela."""
    cat = svc.criar(cenario["s"], {"codigo": "3.9.01",
                                   "descricao": "Categoria nova"}, cenario["admin"])

    assert cat.grupo_codigo == "3" and cat.grupo_nome == "Custos de obra"
    assert cat.subgrupo_codigo == "3.9" and cat.subgrupo_nome == "3.9"


def test_o_grupo_dado_de_fora_manda_mais_que_o_codigo(cenario):
    cat = svc.criar(cenario["s"], {
        "codigo": "3.1.97", "descricao": "X",
        "grupo_codigo": "5", "grupo_nome": "Despesas administrativas"}, cenario["admin"])

    assert cat.grupo_codigo == "5" and cat.grupo_nome == "Despesas administrativas"


def test_codigo_sem_ponto_nao_inventa_grupo_nenhum(cenario):
    """Chutar o código inteiro como grupo criaria um grupo de uma conta só —
    pior que deixar em branco, porque PARECE certo."""
    cat = svc.criar(cenario["s"], {"codigo": "9",
                                   "descricao": "Conta solta"}, cenario["admin"])

    assert cat.grupo_codigo is None
    assert cat.subgrupo_codigo is None


# ---------------------------------------------------------------------------
# O conserto das que já nasceram sem grupo
# ---------------------------------------------------------------------------
def test_ajeitar_poe_a_conta_orfa_no_grupo_certo(cenario):
    s = cenario["s"]
    orfa = Categoria(codigo="3.1.97", descricao="Nasceu sem grupo")
    s.add(orfa)
    s.flush()

    r = svc.ajeitar_sem_grupo(s, cenario["admin"])

    assert orfa.grupo_codigo == "3" and orfa.grupo_nome == "Custos de obra"
    assert orfa.subgrupo_codigo == "3.1"
    assert r["quantidade"] == 1
    assert r["ajeitadas"][0]["grupo"] == "3 · Custos de obra"


def test_ajeitar_nao_encosta_em_quem_ja_esta_no_lugar(cenario):
    r = svc.ajeitar_sem_grupo(cenario["s"], cenario["admin"])

    assert r["quantidade"] == 0


def test_ajeitar_ignora_conta_cujo_codigo_nao_diz_nada(cenario):
    """Código sem ponto não permite deduzir grupo — inventar seria pior."""
    s = cenario["s"]
    solta = Categoria(codigo="SEM-CODIGO", descricao="Conta estranha")
    s.add(solta)
    s.flush()

    r = svc.ajeitar_sem_grupo(s, cenario["admin"])

    assert r["quantidade"] == 0
    assert solta.grupo_codigo is None
