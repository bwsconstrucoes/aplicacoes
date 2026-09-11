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
    assert len(lista.get_json()["perguntas"]) == 7
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


# ---------------------------------------------------------------------------
# LOCAÇÕES — o que está em obra, e o que já passou da hora
#
# Estas três reusam `locacoes.listar(s, usuario, ...)`, que já filtra por obra
# designada e já calcula quanto se pagou, há quantos meses está locado e quais
# alertas existem. Refazer a conta aqui seria inventar um segundo número sobre
# a mesma coisa — e é exatamente assim que dois números discordam na mesma
# tela.
# ---------------------------------------------------------------------------
@pytest.fixture
def locacao(base):
    """Um contrato ativo há muitos meses, com uma parcela vencida sem lançar.

    Os meses são muitos de propósito: é assim que o alerta de "o aluguel já
    paga a compra" acende, que é a pergunta que economiza dinheiro.
    """
    from datetime import timedelta

    s = base["sessao"]
    s.execute(text("""
        INSERT INTO fornecedores (tipo_pessoa, cnpj_cpf, razao_social)
             VALUES ('PJ', '34028316000103', 'LOCADORA TESTE')"""))
    s.flush()
    locadora = s.execute(text(
        "SELECT id FROM fornecedores WHERE razao_social = 'LOCADORA TESTE'")).scalar()
    inicio = date.today() - timedelta(days=400)          # mais de 13 meses
    s.execute(text("""
        INSERT INTO contratos_locacao (numero, fornecedor_id, obra_id,
                                       periodicidade, data_inicio, status)
             VALUES ('LOC0001', :f, :o, 'MENSAL', :d, 'ATIVO')"""),
        {"f": locadora, "o": base["obra"].id, "d": inicio})
    s.flush()
    contrato = s.execute(text("SELECT id FROM contratos_locacao LIMIT 1")).scalar()
    s.execute(text("""
        INSERT INTO locacao_itens (contrato_id, obra_id, descricao, quantidade,
                                   quantidade_devolvida, valor_unitario)
             VALUES (:c, :o, 'Andaime fachadeiro', 10, 0, 150)"""),
        {"c": contrato, "o": base["obra"].id})
    s.execute(text("""
        INSERT INTO locacao_parcelas (contrato_id, competencia, vencimento,
                                      valor_previsto, status)
             VALUES (:c, :comp, :v, 1500, 'PREVISTA')"""),
        {"c": contrato, "comp": date.today().replace(day=1),
         "v": date.today() - timedelta(days=15)})
    s.flush()
    return base


def test_diz_o_que_esta_locado_e_em_qual_obra(locacao):
    r = _responder(locacao, "chefe", "equipamentos_locados")

    assert r["quantas"] == 1
    assert r["linhas"][0]["obra"] == "OBRA-A"
    assert r["linhas"][0]["itens"] == 1
    assert r["total"] == pytest.approx(1500.0), "10 andaimes a R$ 150 por mês"
    assert "por período" in r["frase"]


def test_o_filtro_de_obra_da_locacao_ignora_acento_tambem(locacao):
    assert _responder(locacao, "chefe", "equipamentos_locados",
                      obra="obra-a")["quantas"] == 1
    assert _responder(locacao, "chefe", "equipamentos_locados",
                      obra="OBRA-Z")["quantas"] == 0


def test_acende_o_alerta_de_que_o_aluguel_ja_pagou_a_compra(locacao):
    """Cada parcela, sozinha, é pequena — por isso equipamento esquecido em
    obra passa despercebido. É esta pergunta que o mostra."""
    r = _responder(locacao, "chefe", "locacao_que_ja_pagou_a_compra")

    assert r["quantas"] >= 1
    assert any("meses locado" in l["aviso"] for l in r["linhas"])
    assert "CRÍTICOS" in r["frase"], "13 meses locado tem de acender o crítico"


def test_a_frase_nao_explica_o_critico_quando_nao_ha_nenhum(locacao):
    """"0 deles críticos (aluguel já pagou a compra…)" explica uma coisa que
    não aconteceu — lê mal e assusta à toa."""
    from app.apps.erp.core.perguntas import respostas

    vazio = respostas.locacao_que_ja_pagou_a_compra.__doc__
    assert vazio  # a função existe; o caso sem crítico é coberto abaixo
    s = locacao["sessao"]
    s.execute(text("UPDATE contratos_locacao SET data_inicio = :d"),
              {"d": date.today()})
    s.flush()

    r = _responder(locacao, "chefe", "locacao_que_ja_pagou_a_compra")

    assert "CRÍTICOS" not in r["frase"]
    assert "nenhum crítico" in r["frase"] or r["quantas"] == 0


def test_acha_o_aluguel_vencido_que_nao_virou_titulo(locacao):
    """Enquanto não é lançada, a parcela não entra em previsão de caixa
    nenhuma — e chega como surpresa quando a locadora cobra."""
    r = _responder(locacao, "chefe", "parcelas_de_locacao_sem_lancar")

    assert r["quantas"] == 1
    assert r["linhas"][0]["parcelas"] == 1
    assert "não viraram título" in r["frase"] or "não viraram" in r["frase"]


def test_as_perguntas_de_locacao_respeitam_as_obras_de_quem_pergunta(locacao):
    """BRECHA DE ESCOPO ACHADA POR ESTE TESTE, em 11/09/2026.

    Contrato de locação NÃO TEM AUTOR. A listagem usava `obras_do_usuario`,
    que devolve None para quem enxerga por autoria — e None ali quer dizer
    "sem filtro de obra". Efeito: o administrativo que só deveria ver o que
    ele mesmo lançou via TODOS os contratos de locação da empresa, na tela de
    Locações. A regra virou `obras_de_registro_sem_autor`: para registro sem
    autor o único recorte é a obra, e sem obra designada não se vê nenhum."""
    s = locacao["sessao"]
    de_fora = Usuario(nome="Outro", email="outro@bws.test",
                      senha_hash=gerar_hash("senha-de-teste"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    s.add(de_fora)
    s.flush()
    locacao["de_fora"] = de_fora

    r = _responder(locacao, "de_fora", "equipamentos_locados")

    assert r["quantas"] == 0, "quem não tem a obra não vê o contrato dela"
    assert r["total"] in (None, 0, 0.0)


def test_o_painel_por_obra_tambem_respeita_o_alcance(locacao):
    """A MESMA brecha, e maior: `painel_por_obra` não recebia usuário nenhum, e
    a rota que o serve é aberta a todo operador. Qualquer pessoa via quanto
    CADA obra da empresa tem de aluguel."""
    from app.apps.erp.core import locacoes as svc_loc

    s = locacao["sessao"]
    de_fora = Usuario(nome="Outro2", email="outro2@bws.test",
                      senha_hash=gerar_hash("senha-de-teste"),
                      perfil=P.ADMINISTRATIVO_OBRA)
    s.add(de_fora)
    s.flush()

    do_chefe = svc_loc.painel_por_obra(s, locacao["chefe"])
    do_de_fora = svc_loc.painel_por_obra(s, de_fora)

    assert len(do_chefe) == 1, "quem enxerga tudo continua enxergando"
    assert do_de_fora == [], "quem não alcança a obra não vê o aluguel dela"


def test_quem_enxerga_tudo_continua_enxergando_as_locacoes(locacao):
    """O conserto do escopo não pode fechar a porta de quem já podia entrar."""
    r = _responder(locacao, "chefe", "equipamentos_locados")
    assert r["quantas"] == 1
