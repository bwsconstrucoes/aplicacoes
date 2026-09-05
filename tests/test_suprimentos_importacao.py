"""Carga inicial de fornecedores e insumos, a partir das planilhas em uso.

São 111 fornecedores e 115 insumos que o dono já mantém em planilha. O que não
pode falhar na hora de trazer isso para o ERP:

  - rodar duas vezes NÃO pode duplicar (fornecedor casa por CNPJ, insumo pela
    descrição) — a carga vai ser feita em partes, e vai ser refeita;
  - cabeçalho com acento, maiúscula ou nome antigo tem de ser aceito: quem
    exporta a planilha não deveria ter de editar arquivo;
  - categoria que não existe é RELATADA, nunca inventada — é assim que uma base
    de cadastro apodrece;
  - a prévia não pode gravar nada.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.importadores.suprimentos import (
    _chave, importar_fornecedores_csv, importar_insumos_csv,
)
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, FornecedorCategoria, FornecedorContato,
    FornecedorPorte, Insumo, InsumoCategoria, PerfilUsuario as P, TipoPessoa,
)

from conftest import SessaoFalsa, novo_usuario

# CNPJs válidos de teste (dígito verificador confere) — nenhum fornecedor real.
CNPJ_A = "11444777000161"
CNPJ_B = "34028316000103"

CABECALHO = ("ID,RAZÃO SOCIAL,NOME DO FORNECEDOR,CNPJ/CPF,Contato,Email,Telefone,"
             "Categoria de Insumo,Cidade,Região de Atuação,Envio de Cotações,"
             "Porte do Fornecedor")


def _csv_fornecedores(*linhas: str) -> bytes:
    return ("\n".join([CABECALHO, *linhas])).encode("utf-8")


@pytest.fixture
def sessao():
    return SessaoFalsa(InsumoCategoria(id=1, codigo="AGR", nome="Agregados"),
                       InsumoCategoria(id=2, codigo="PIN", nome="Pintura"))


@pytest.fixture
def admin():
    return novo_usuario(1, P.ADMIN)


# ---------------------------------------------------------------------------
# Fornecedores
# ---------------------------------------------------------------------------
def test_cria_o_fornecedor_com_regiao_porte_e_canal(sessao, admin):
    conteudo = _csv_fornecedores(
        f"1,PEDREIRA SAO JOSE LTDA,Pedreira SJ,{CNPJ_A},MARCELO,v@x.com.br,"
        f"85999990000,Agregados,FORTALEZA,\"CE, RMF\",Email,Fábrica")

    rel = importar_fornecedores_csv(sessao, conteudo, admin)

    assert rel["criados"] == 1 and rel["rejeitados"] == []
    forn = next(o for o in sessao.adicionados if isinstance(o, Fornecedor))
    assert forn.cnpj_cpf == CNPJ_A
    assert forn.tipo_pessoa is TipoPessoa.PJ
    assert forn.porte is FornecedorPorte.FABRICA
    assert forn.regioes_atuacao == ["CE", "RMF"]
    assert forn.canais_cotacao == ["EMAIL"]


def test_o_contato_da_planilha_vira_cotador(sessao, admin):
    conteudo = _csv_fornecedores(
        f"1,TINTAS BOAS LTDA,Tintas,{CNPJ_A},ANA,ana@x.com.br,8598888,Pintura,"
        f"FORTALEZA,RMF,\"Email, Whatsapp\",Distribuidor")

    importar_fornecedores_csv(sessao, conteudo, admin)

    contato = next(o for o in sessao.adicionados if isinstance(o, FornecedorContato))
    assert contato.nome == "ANA" and contato.email == "ana@x.com.br"
    assert contato.recebe_cotacao is not False
    forn = next(o for o in sessao.adicionados if isinstance(o, Fornecedor))
    assert forn.canais_cotacao == ["EMAIL", "WHATSAPP"]


def test_liga_o_fornecedor_as_categorias_que_ele_vende(sessao, admin):
    conteudo = _csv_fornecedores(
        f"1,MISTA LTDA,Mista,{CNPJ_A},JOAO,j@x.com.br,85977777,"
        f"\"Agregados; Pintura\",FORTALEZA,BR,Email,Distribuidor")

    rel = importar_fornecedores_csv(sessao, conteudo, admin)

    ligacoes = {o.categoria_insumo_id for o in sessao.adicionados
                if isinstance(o, FornecedorCategoria)}
    assert ligacoes == {1, 2}
    assert rel["categorias_nao_encontradas"] == []


def test_categoria_desconhecida_e_relatada_e_nao_criada(sessao, admin):
    conteudo = _csv_fornecedores(
        f"1,ESTRANHA LTDA,Estranha,{CNPJ_A},JOAO,j@x.com.br,85977777,"
        f"Foguetes Espaciais,FORTALEZA,BR,Email,Distribuidor")

    rel = importar_fornecedores_csv(sessao, conteudo, admin)

    assert rel["categorias_nao_encontradas"] == ["Foguetes Espaciais"]
    assert not any(isinstance(o, InsumoCategoria) for o in sessao.adicionados), (
        "inventar categoria na importação é como a base começa a apodrecer")


def test_rodar_de_novo_nao_duplica_fornecedor(admin):
    ja_existe = Fornecedor(id=9, tipo_pessoa=TipoPessoa.PJ, cnpj_cpf=CNPJ_A,
                           razao_social="PEDREIRA SAO JOSE LTDA")
    s = SessaoFalsa(ja_existe, InsumoCategoria(id=1, codigo="AGR", nome="Agregados"))
    conteudo = _csv_fornecedores(
        f"1,PEDREIRA SAO JOSE LTDA,Pedreira SJ,{CNPJ_A},MARCELO,novo@x.com.br,"
        f"85999990000,Agregados,FORTALEZA,RMF,Email,Fábrica")

    rel = importar_fornecedores_csv(s, conteudo, admin)

    assert rel["criados"] == 0 and rel["atualizados"] == 1
    assert not any(isinstance(o, Fornecedor) for o in s.adicionados)
    assert ja_existe.email == "novo@x.com.br", "o que veio na planilha atualiza"
    assert ja_existe.porte is FornecedorPorte.FABRICA


def test_documento_em_branco_e_recusado_com_a_linha(sessao, admin):
    conteudo = _csv_fornecedores(
        "1,SEM DOCUMENTO LTDA,Sem doc,,JOAO,j@x.com.br,85977777,Agregados,"
        "FORTALEZA,RMF,Email,Distribuidor")

    rel = importar_fornecedores_csv(sessao, conteudo, admin)

    assert rel["criados"] == 0
    assert rel["rejeitados"][0]["linha"] == 2
    assert "branco" in rel["rejeitados"][0]["motivo"].lower()


def test_a_previa_nao_grava_nada(sessao, admin):
    conteudo = _csv_fornecedores(
        f"1,PREVIA LTDA,Previa,{CNPJ_A},JOAO,j@x.com.br,85977777,Agregados,"
        f"FORTALEZA,RMF,Email,Fábrica")

    rel = importar_fornecedores_csv(sessao, conteudo, admin, simular=True)

    assert rel["criados"] == 1 and rel["simulacao"] is True
    assert sessao.adicionados == [], "prévia que grava não é prévia"


def test_linha_em_branco_no_fim_da_planilha_e_ignorada(sessao, admin):
    conteudo = _csv_fornecedores(
        f"1,UMA LTDA,Uma,{CNPJ_A},JOAO,j@x.com.br,85977777,Agregados,F,RMF,Email,Fábrica",
        ",,,,,,,,,,,", ",,,,,,,,,,,")

    rel = importar_fornecedores_csv(sessao, conteudo, admin)

    assert rel["criados"] == 1 and rel["rejeitados"] == []


# ---------------------------------------------------------------------------
# Insumos
# ---------------------------------------------------------------------------
def _csv_insumos(*linhas: str) -> bytes:
    cab = "Insumos,Categoria do Insumo,Plano Financeiro,UND"
    return ("\n".join([cab, *linhas])).encode("utf-8")


@pytest.fixture
def sessao_insumos():
    return SessaoFalsa(
        InsumoCategoria(id=1, codigo="AGR", nome="Agregados"),
        Categoria(id=50, codigo="3.1.01", descricao="Agregados (Areia, Brita, Arisco)"))


def test_cria_insumo_com_categoria_e_conta_do_plano(sessao_insumos, admin):
    conteudo = _csv_insumos("Pó de Pedra,Agregados,\"Agregados (Areia, Brita, Arisco)\",M3")

    rel = importar_insumos_csv(sessao_insumos, conteudo, admin)

    assert rel["criados"] == 1 and rel["sem_conta_do_plano"] == 0
    insumo = next(o for o in sessao_insumos.adicionados if isinstance(o, Insumo))
    assert insumo.descricao == "Pó de Pedra"
    assert insumo.codigo == "INS-0001"
    assert insumo.categoria_insumo_id == 1 and insumo.categoria_id == 50
    assert insumo.unidade == "M3"


def test_insumo_sem_conta_do_plano_entra_mas_e_contado(sessao_insumos, admin):
    """Sem a conta, o pedido não vira previsão apropriada — o dono precisa
    saber quantos ficaram assim."""
    conteudo = _csv_insumos("Coisa Nova,Agregados,,UN")

    rel = importar_insumos_csv(sessao_insumos, conteudo, admin)

    assert rel["criados"] == 1 and rel["sem_conta_do_plano"] == 1


def test_rodar_de_novo_nao_duplica_insumo(admin):
    s = SessaoFalsa(Insumo(id=3, codigo="INS-0003", descricao="Pó de Pedra"),
                    InsumoCategoria(id=1, codigo="AGR", nome="Agregados"))
    conteudo = _csv_insumos("PÓ DE PEDRA,Agregados,,M3")

    rel = importar_insumos_csv(s, conteudo, admin)

    assert rel["criados"] == 0 and rel["atualizados"] == 1
    assert not any(isinstance(o, Insumo) for o in s.adicionados), (
        "acento e caixa diferentes não podem virar um segundo cadastro")


def test_a_numeracao_continua_de_onde_parou(admin):
    s = SessaoFalsa(Insumo(id=3, codigo="INS-0007", descricao="Antigo"),
                    InsumoCategoria(id=1, codigo="AGR", nome="Agregados"))

    importar_insumos_csv(s, _csv_insumos("Novo Item,Agregados,,UN"), admin)

    novo = next(o for o in s.adicionados if isinstance(o, Insumo))
    assert novo.codigo == "INS-0008"


def test_cabecalho_alternativo_e_aceito(sessao_insumos, admin):
    """A planilha muda de nome de coluna com o tempo; o importador não pode
    quebrar por causa disso."""
    conteudo = ("Descrição do Insumo,Sub-Categoria,Conta do Plano,Unidade\n"
                "Brita 0,Agregados,\"Agregados (Areia, Brita, Arisco)\",M3").encode("utf-8")

    rel = importar_insumos_csv(sessao_insumos, conteudo, admin)

    assert rel["criados"] == 1


def test_chave_ignora_acento_caixa_e_espaco():
    assert _chave("  Rep. de Fábrica ") == _chave("REP. DE FABRICA")
    assert _chave("Pó de Pedra") == _chave("po  de pedra")


# ---------------------------------------------------------------------------
# As rotas: a prévia não grava, a condição inválida não entra, e sem alçada
# ninguém carrega nada
# ---------------------------------------------------------------------------
import contextlib

from flask import Flask


def _cliente(sessao, monkeypatch, usuario_id=1):
    from app.apps.erp import routes
    app = Flask(__name__)
    app.secret_key = "teste"
    app.register_blueprint(routes.bp)
    monkeypatch.setattr(routes, "get_session",
                        lambda: contextlib.nullcontext(sessao))
    c = app.test_client()
    with c.session_transaction() as web:
        web["erp_usuario_id"] = usuario_id
    return c


def test_a_rota_de_previa_nao_grava(monkeypatch):
    import io
    s = SessaoFalsa(novo_usuario(1, P.ADMIN),
                    InsumoCategoria(id=1, codigo="AGR", nome="Agregados"))
    c = _cliente(s, monkeypatch)
    arquivo = (io.BytesIO(_csv_fornecedores(
        f"1,PREVIA LTDA,Previa,{CNPJ_A},JOAO,j@x.com.br,8597,Agregados,F,RMF,Email,Fábrica")),
        "fornecedores.csv")

    r = c.post("/erp/api/suprimentos/importar/fornecedores?simular=1",
               data={"arquivo": arquivo}, content_type="multipart/form-data")

    assert r.status_code == 200
    rel = r.get_json()["relatorio"]
    assert rel["simulacao"] is True and rel["criados"] == 1
    assert not any(isinstance(o, Fornecedor) for o in s.adicionados)
    assert s.desfeita is True, "a prévia tem de desfazer o que tocou na sessão"


def test_carga_sem_arquivo_avisa(monkeypatch):
    s = SessaoFalsa(novo_usuario(1, P.ADMIN))
    r = _cliente(s, monkeypatch).post("/erp/api/suprimentos/importar/insumos",
                                      data={}, content_type="multipart/form-data")
    assert r.status_code == 400 and "csv" in r.get_json()["erro"].lower()


def test_tipo_de_carga_desconhecido_e_recusado(monkeypatch):
    s = SessaoFalsa(novo_usuario(1, P.ADMIN))
    r = _cliente(s, monkeypatch).post("/erp/api/suprimentos/importar/planetas", data={})
    assert r.status_code == 400


def test_condicao_que_nao_gera_parcela_e_recusada_antes_de_gravar(monkeypatch):
    """Sem entrada e sem prazo, o defeito só apareceria no primeiro pedido que
    usasse a condição."""
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    s = SessaoFalsa(novo_usuario(1, P.ADMIN))
    c = _cliente(s, monkeypatch)

    r = c.post("/erp/api/suprimentos/condicoes",
               json={"nome": "Nunca vence", "entrada_percentual": 0, "dias": []})

    assert r.status_code == 400
    assert not any(isinstance(o, CondicaoPagamento) for o in s.adicionados)


def test_condicao_valida_e_gravada_com_os_dias_em_ordem(monkeypatch):
    from app.apps.erp.db.models.cadastros import CondicaoPagamento
    s = SessaoFalsa(novo_usuario(1, P.ADMIN))
    c = _cliente(s, monkeypatch)

    r = c.post("/erp/api/suprimentos/condicoes",
               json={"nome": "30% + 56/28 dias", "entrada_percentual": 30,
                     "dias": [56, 28, 28]})

    assert r.status_code == 200
    nova = next(o for o in s.adicionados if isinstance(o, CondicaoPagamento))
    assert nova.dias == [28, 56], "prazo repetido e fora de ordem tem de ser normalizado"


def test_quem_nao_administra_insumos_nao_carrega_nem_cadastra(monkeypatch):
    s = SessaoFalsa(novo_usuario(1, P.ADMINISTRATIVO_OBRA))
    c = _cliente(s, monkeypatch)

    assert c.post("/erp/api/suprimentos/importar/insumos", data={}).status_code == 403
    assert c.post("/erp/api/suprimentos/condicoes", json={}).status_code == 403
    assert c.get("/erp/api/suprimentos/cadastros").status_code != 403, (
        "ver os cadastros é de quem pede material também")
