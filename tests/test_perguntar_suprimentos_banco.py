"""As perguntas de Suprimentos — o catálogo e a fila de pedidos.

O dono pediu uma delas com estas palavras: *"me manda uma lista dos insumos
cadastrados na categoria tal"*.

DUAS NATUREZAS CONVIVEM NESTE GRUPO, e a diferença é o que este arquivo
protege:

  · o CATÁLOGO (insumos, categorias, preços) é cadastro da empresa e NÃO se
    recorta por obra — quem enxerga Suprimentos enxerga o catálogo inteiro;
  · a FILA DE PEDIDOS é da obra, e passa pelo mesmo filtro por pessoa da tela
    de Solicitações.

Confundir as duas é o erro caro: recortar o catálogo esconderia insumo de quem
precisa cadastrar; não recortar a fila mostraria o pedido de uma obra para o
administrativo de outra.

COM BANCO DE VERDADE porque a fila depende do filtro por pessoa, que vive no
`WHERE`, e o catálogo depende de junção com categoria e preço.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.perguntas import catalogo
from app.apps.erp.db.models.cadastros import (
    Categoria, Insumo, InsumoCategoria, Obra, PerfilUsuario as P, Usuario,
)

from conftest import como

pytestmark = pytest.mark.banco


@pytest.fixture
def base(sessao_real):
    """Seis insumos em três categorias, um deles sem conta do plano."""
    s = sessao_real
    chefe = Usuario(nome="Marcelo", email="chefe@bws.test",
                    senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMIN)
    forasteiro = Usuario(nome="Contador", email="cont@bws.test",
                         senha_hash=gerar_hash("senha-de-teste"),
                         perfil=P.DEPARTAMENTO_PESSOAL)
    obra = Obra(codigo="OBRA-A", nome="Creche")
    conta = Categoria(codigo="3.1.01", descricao="Cimento e Concreto Usinado",
                      natureza="RESULTADO", tipos_permitidos=[])
    hidraulico = InsumoCategoria(codigo="CAT-0001", nome="Hidráulico")
    agregados = InsumoCategoria(codigo="CAT-0002", nome="Agregados")
    eletrico = InsumoCategoria(codigo="CAT-0003", nome="Material Elétrico")
    s.add_all([chefe, forasteiro, obra, conta, hidraulico, agregados, eletrico])
    s.flush()

    itens = [
        ("INS-0001", "Tubo PVC 100mm", hidraulico, conta, "M"),
        ("INS-0002", "Joelho PVC 100mm", hidraulico, conta, "UN"),
        ("INS-0003", "Registro de gaveta 1/2", hidraulico, conta, "UN"),
        ("INS-0004", "Areia grossa", agregados, conta, "M3"),
        ("INS-0005", "Brita 1", agregados, None, "M3"),      # sem conta
        ("INS-0006", "Cabo flexível 2,5mm", eletrico, conta, "M"),
    ]
    for codigo, descricao, categoria, conta_plano, unidade in itens:
        s.add(Insumo(codigo=codigo, descricao=descricao, unidade=unidade,
                     categoria_insumo_id=categoria.id,
                     categoria_id=conta_plano.id if conta_plano else None,
                     ativo=True))
    s.flush()
    return {"chefe": chefe, "forasteiro": forasteiro, "sessao": s, "obra": obra}


def _responder(base, quem, chave, **parametros):
    return catalogo.responder(chave, base["sessao"], base[quem], parametros)


# ---------------------------------------------------------------------------
# O catálogo
# ---------------------------------------------------------------------------
def test_lista_os_insumos_de_uma_categoria(base):
    """A pergunta como o dono a fez."""
    r = _responder(base, "chefe", "insumos_da_categoria", categoria="Hidráulico")

    assert r["quantas"] == 3
    assert {l["descricao"] for l in r["linhas"]} == {
        "Tubo PVC 100mm", "Joelho PVC 100mm", "Registro de gaveta 1/2"}
    assert "3 insumo(s) na categoria 'Hidráulico'" in r["frase"]


def test_a_busca_da_categoria_aceita_um_pedaco_do_nome(base):
    """Ninguém digita "Material Elétrico" inteiro, com acento e tudo."""
    r = _responder(base, "chefe", "insumos_da_categoria", categoria="elétr")
    assert r["quantas"] == 1


def test_sem_categoria_responde_o_catalogo_inteiro(base):
    r = _responder(base, "chefe", "insumos_da_categoria")
    assert r["quantas"] == 6
    assert "3 categoria(s)" in r["frase"]


def test_a_busca_ignora_acento(base):
    """Ninguém digita acento numa busca: quem procura "Hidráulico" escreve
    "hidra". A primeira versão comparava direto e devolvia "nenhum insumo
    nessa categoria" sobre uma categoria cheia — o pior tipo de resposta
    errada, porque parece certa."""
    assert _responder(base, "chefe", "insumos_da_categoria",
                      categoria="hidra")["quantas"] == 3
    assert _responder(base, "chefe", "insumos_da_categoria",
                      categoria="ELETRICO")["quantas"] == 1


def test_categoria_que_nao_existe_diz_o_que_existe(base):
    """"Não achei" sem dizer o que existe manda a pessoa adivinhar."""
    vazio = _responder(base, "chefe", "insumos_da_categoria", categoria="zzzz")

    assert vazio["quantas"] == 0
    assert "Nenhum insumo na categoria 'zzzz'" in vazio["frase"]
    assert "categorias" in vazio["frase"]


def test_acha_o_insumo_sem_conta_do_plano(base):
    r = _responder(base, "chefe", "insumos_sem_conta_do_plano")
    assert [l["descricao"] for l in r["linhas"]] == ["Brita 1"]
    assert "conta do plano" in r["observacao"]


def test_o_preco_precisa_do_nome_do_insumo(base):
    """Sem nome, a resposta pede o nome em vez de despejar o catálogo."""
    r = _responder(base, "chefe", "preco_do_insumo")
    assert r["linhas"] == []
    assert "Diga o nome do insumo" in r["frase"]


def test_o_preco_acha_pelo_pedaco_do_nome_e_avisa_quando_nao_ha_preco(base):
    r = _responder(base, "chefe", "preco_do_insumo", insumo="PVC")

    assert r["quantas"] == 2
    assert "nenhum tem preço registrado ainda" in r["frase"]
    assert "ÚLTIMO preço" in r["observacao"]


# ---------------------------------------------------------------------------
# A fila de pedidos — esta SE recorta por pessoa
# ---------------------------------------------------------------------------
def test_a_fila_de_pedidos_passa_pelo_filtro_da_tela_de_solicitacoes(base):
    """Não é uma consulta nova: é a mesma função que a tela usa, então o
    recorte por pessoa vem de graça e não pode divergir dela."""
    from app.apps.erp.core.suprimentos import solicitacao as svc_sol

    r = _responder(base, "chefe", "pedidos_de_material_pendentes")
    da_tela = svc_sol.listar_itens(base["sessao"], base["chefe"])

    assert r["quantas"] <= len(da_tela)
    assert "alcance de quem perguntou" in r["observacao"]


# ---------------------------------------------------------------------------
# A fronteira do grupo
# ---------------------------------------------------------------------------
def test_quem_nao_ve_suprimentos_nao_alcanca_o_grupo(app_real, base):
    cliente = como(app_real, base["forasteiro"].id)
    assert cliente.get("/erp/api/perguntas/suprimentos").status_code == 403
    assert cliente.post("/erp/api/perguntar/suprimentos",
                        json={"chave": "insumos_da_categoria"}).status_code == 403


def test_a_rota_do_financeiro_nao_responde_pergunta_de_suprimentos(app_real, base):
    """Senão a ação de Suprimentos seria contornada pela rota do financeiro."""
    r = como(app_real, base["chefe"].id).post(
        "/erp/api/perguntar/financeiro", json={"chave": "insumos_da_categoria"})
    assert r.status_code == 404


def test_quem_ve_suprimentos_pergunta_e_recebe(app_real, base):
    cliente = como(app_real, base["chefe"].id)
    lista = cliente.get("/erp/api/perguntas/suprimentos")
    resposta = cliente.post("/erp/api/perguntar/suprimentos",
                            json={"chave": "insumos_da_categoria",
                                  "parametros": {"categoria": "Agregados"}})

    assert lista.status_code == 200
    assert len(lista.get_json()["perguntas"]) == 4
    assert resposta.status_code == 200
    assert resposta.get_json()["resposta"]["quantas"] == 2


# ---------------------------------------------------------------------------
# O teto de linhas — a conta é sobre tudo, o corte é só do que aparece
# ---------------------------------------------------------------------------
def test_lista_grande_e_cortada_mas_o_numero_continua_verdadeiro(base):
    """A base real tem 3.285 insumos: devolver todos travaria o navegador. O
    perigo seria o NÚMERO passar a ser o do corte — aí a resposta mentiria."""
    from app.apps.erp.core.perguntas.respostas import TETO_DE_LINHAS

    s = base["sessao"]
    categoria = s.scalars(text("SELECT id FROM insumo_categorias LIMIT 1")).first()
    for n in range(TETO_DE_LINHAS + 20):
        s.add(Insumo(codigo=f"INS-9{n:04d}", descricao=f"Item de teste {n}",
                     categoria_insumo_id=categoria, ativo=True))
    s.flush()

    r = _responder(base, "chefe", "insumos_da_categoria")

    assert r["quantas"] == TETO_DE_LINHAS + 26, "o número tem de ser o de VERDADE"
    assert r["mostradas"] == TETO_DE_LINHAS
    assert len(r["linhas"]) == TETO_DE_LINHAS
    assert "o corte é só do que aparece" in r["observacao"]
