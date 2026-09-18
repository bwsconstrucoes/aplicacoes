"""VÁRIOS CONTATOS POR FORNECEDOR, e o CNPJ repetido que virou isso.

18/09/2026, o dono explicou de onde vinham os 126 CNPJs repetidos da planilha:

    "Um comprador cadastrou, aí depois um segundo comprador cadastrou de novo.
    Mas veja que tem contatos diferentes — tem fornecedores que têm mais de uma
    pessoa que atende. Um atende entregas num determinado estado, outro entrega
    outro."

Ou seja: não era só sujeira. Era um cadastro sem lugar para o segundo vendedor,
e a pessoa resolveu criando a empresa de novo.

O que estes testes seguram:

1. **Duas linhas com o mesmo CNPJ viram UM fornecedor com DOIS contatos** — e
   não uma sobrescrevendo a outra.
2. **Categoria, região e canal SOMAM.** Cada comprador sabia de um pedaço; a
   união é a direção segura para um filtro, porque categoria a menos significa
   fornecedor que nunca mais é cotado naquilo.
3. **Quem cadastrou vira a observação do contato** — pedido dele, com todas as
   letras.
4. **Uma linha ruim não derruba a carga.** Este é o teste que nasceu de um
   defeito de verdade: `core/cadastros/fornecedores` definia uma classe
   `ErroValidacao` PRÓPRIA, homônima da do `auditoria`, e o `except` do
   importador não a pegava. Uma célula com CNPJ e CPF juntos derrubou 1.773
   linhas boas.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select

from app.apps.erp.core.importadores.suprimentos import importar_fornecedores_csv
from app.apps.erp.core.suprimentos import fornecedores as svc
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorContato, InsumoCategoria, PerfilUsuario as P, Usuario,
)
from app.apps.erp.core.auth.service import gerar_hash

pytestmark = pytest.mark.banco

CNPJ = "11444777000161"
CABECALHO = ("RAZÃO SOCIAL,NOME DO FORNECEDOR,CNPJ/CPF,Contato,Email,Telefone,"
             "Observação do contato,Categoria de Insumo,Cidade,"
             "Região de Atuação,Envio de Cotações,Porte do Fornecedor,"
             "Responsável pelo Registro,Data de Registro")


def _csv(*linhas: str) -> bytes:
    return ("\n".join([CABECALHO, *linhas])).encode("utf-8")


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    s.add_all([InsumoCategoria(codigo="CIM", nome="Cimento"),
               InsumoCategoria(codigo="EPI", nome="EPI")])
    chefe = Usuario(nome="Chefe", email="chefe.fc@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    s.add(chefe); s.flush()
    return {"s": s, "chefe": chefe}


def _fornecedor(s):
    return s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == CNPJ)).first()


def _contatos(s, forn_id):
    return sorted([c for c in s.scalars(select(FornecedorContato)).all()
                   if c.fornecedor_id == forn_id], key=lambda c: c.nome)


# ---------------------------------------------------------------------------
# O CNPJ repetido que vira um fornecedor com dois vendedores
# ---------------------------------------------------------------------------
def test_duas_linhas_do_mesmo_cnpj_viram_um_fornecedor_com_dois_contatos(cenario):
    s, chefe = cenario["s"], cenario["chefe"]

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022",
        f"STOCK EPI E EQUIPAMENTOS,Stock,{CNPJ},MARIO,mario@x.com.br,8590002222,,EPI,"
        f"FORTALEZA,CE,Whatsapp,Distribuidor,JAQUELINE,05/09/2022"), chefe)
    s.flush()

    forn = _fornecedor(s)
    assert forn is not None
    contatos = _contatos(s, forn.id)
    assert [c.nome for c in contatos] == ["ELIAS", "MARIO"]
    assert [c.email for c in contatos] == ["elias@x.com.br", "mario@x.com.br"]


def test_categoria_regiao_e_canal_somam_entre_as_duas_linhas(cenario):
    """Cada comprador sabia de um pedaço. Categoria a menos = fornecedor que
    nunca mais é cotado naquilo, e ninguém percebe."""
    s, chefe = cenario["s"], cenario["chefe"]

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022",
        f"STOCK LTDA,Stock,{CNPJ},MARIO,mario@x.com.br,8590002222,,EPI,"
        f"FORTALEZA,CE,Whatsapp,Distribuidor,JAQUELINE,05/09/2022"), chefe)
    s.flush()

    forn = _fornecedor(s)
    categorias = {c["nome"] for c in
                  [f for f in svc.gerenciar(s)["fornecedores"] if f["id"] == forn.id][0]
                  and []} or set(
        [f for f in svc.gerenciar(s)["fornecedores"] if f["id"] == forn.id][0]["categorias"])
    assert categorias == {"Cimento", "EPI"}
    assert set(forn.regioes_atuacao) == {"RMF", "CE"}
    assert set(forn.canais_cotacao) == {"EMAIL", "WHATSAPP"}


def test_quem_cadastrou_vira_a_observacao_do_contato(cenario):
    """Dono: *"bota como observação a pessoa que cadastrou ele, porque aí já dá
    uma diferenciação"*."""
    s, chefe = cenario["s"], cenario["chefe"]

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO GOMES - 8599,01/09/2022"), chefe)
    s.flush()

    contato = _contatos(s, _fornecedor(s).id)[0]
    assert contato.observacao == "Cadastrado por GERLANIO GOMES em 01/09/2022."


def test_a_observacao_da_planilha_vence_a_de_quem_cadastrou(cenario):
    """Quando alguém escreveu do que o vendedor trata, é isso que importa."""
    s, chefe = cenario["s"], cenario["chefe"]

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,"
        f"atende o interior,Cimento,FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022"),
        chefe)
    s.flush()

    assert _contatos(s, _fornecedor(s).id)[0].observacao == "atende o interior"


def test_o_email_do_fornecedor_nao_e_sobrescrito_pela_segunda_linha(cenario):
    """A segunda linha não é "mais nova": é outra pessoa que cadastrou. Deixar
    a última vencer fazia o e-mail do fornecedor ser o do segundo vendedor, ao
    acaso da ordem das linhas."""
    s, chefe = cenario["s"], cenario["chefe"]

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022",
        f"STOCK LTDA,Stock,{CNPJ},MARIO,mario@x.com.br,8590002222,,EPI,"
        f"SOBRAL,CE,Email,Distribuidor,JAQUELINE,05/09/2022"), chefe)
    s.flush()

    forn = _fornecedor(s)
    assert forn.email == "elias@x.com.br"
    assert forn.municipio == "FORTALEZA"


def test_rodar_de_novo_completa_o_contato_em_vez_de_ignorar(cenario):
    """Corrigir a planilha e rodar de novo tem de melhorar o cadastro."""
    s, chefe = cenario["s"], cenario["chefe"]
    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022"), chefe)
    s.flush()

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,"
        f"atende o interior,Cimento,FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022"),
        chefe)
    s.flush()

    contatos = _contatos(s, _fornecedor(s).id)
    assert len(contatos) == 1, "mesmo nome não vira contato novo"
    assert contatos[0].email == "elias@x.com.br"
    assert contatos[0].observacao == "atende o interior"


# ---------------------------------------------------------------------------
# UMA LINHA RUIM NÃO DERRUBA A CARGA
# ---------------------------------------------------------------------------
def test_linha_com_documento_impossivel_e_recusada_e_as_outras_entram(cenario):
    """O defeito que isto pega é real: `core/cadastros/fornecedores` tinha uma
    classe `ErroValidacao` própria, homônima da do `auditoria`. O `except` do
    importador não a pegava, e UMA célula com CNPJ e CPF juntos derrubou a
    carga inteira de 1.773 linhas."""
    s, chefe = cenario["s"], cenario["chefe"]

    r = importar_fornecedores_csv(s, _csv(
        f"BOA LTDA,Boa,{CNPJ},ELIAS,elias@x.com.br,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022",
        "RUIM LTDA,Ruim,5564374500016007395071417,JOAO,j@x.com.br,8590003333,,"
        "Cimento,FORTALEZA,RMF,Email,Distribuidor,RUAN,26/05/2026",
        "OUTRA BOA LTDA,Outra,34028316000103,MARIA,m@x.com.br,8590004444,,EPI,"
        "FORTALEZA,CE,Email,Fábrica,RUAN,26/05/2026"), chefe)
    s.flush()

    assert r["criados"] == 2, "as duas boas entraram"
    assert len(r["rejeitados"]) == 1
    assert r["rejeitados"][0]["linha"] == 3
    assert "dígito verificador" in r["rejeitados"][0]["motivo"]


# ---------------------------------------------------------------------------
# Editar o contato pela tela
# ---------------------------------------------------------------------------
def test_editar_contato_guarda_a_observacao(cenario):
    s, chefe = cenario["s"], cenario["chefe"]
    forn = svc.base.criar(s, {"tipo_pessoa": "PJ", "cnpj_cpf": CNPJ,
                              "razao_social": "STOCK LTDA"}, chefe)
    s.flush()
    c = svc.acrescentar_contato(s, forn.id, {"nome": "ELIAS", "email": "e@x.com.br"}, chefe)
    s.flush()

    svc.editar_contato(s, c.id, {"observacao": "atende o interior do Ceará",
                                 "funcao": "vendedor"}, chefe)
    s.flush()

    assert c.observacao == "atende o interior do Ceará"
    assert c.funcao == "vendedor"


def test_contato_nao_pode_ficar_sem_email_e_sem_telefone(cenario):
    """Contato que não recebe cotação não serve para nada — e o banco recusa."""
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    s, chefe = cenario["s"], cenario["chefe"]
    forn = svc.base.criar(s, {"tipo_pessoa": "PJ", "cnpj_cpf": CNPJ,
                              "razao_social": "STOCK LTDA"}, chefe)
    s.flush()
    c = svc.acrescentar_contato(s, forn.id, {"nome": "ELIAS", "email": "e@x.com.br"}, chefe)
    s.flush()

    with pytest.raises(ErroValidacao, match="e-mail ou o telefone"):
        svc.editar_contato(s, c.id, {"email": "", "telefone": ""}, chefe)


def test_a_observacao_escrita_por_gente_nunca_e_apagada_pela_automatica(cenario):
    """O contrário do teste acima: "atende o interior" não vira "cadastrado
    por fulano" só porque a carga rodou de novo com a planilha antiga."""
    s, chefe = cenario["s"], cenario["chefe"]
    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,"
        f"atende o interior,Cimento,FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022"),
        chefe)
    s.flush()

    importar_fornecedores_csv(s, _csv(
        f"STOCK LTDA,Stock,{CNPJ},ELIAS,elias@x.com.br,8590001111,,Cimento,"
        f"FORTALEZA,RMF,Email,Distribuidor,GERLANIO,01/09/2022"), chefe)
    s.flush()

    assert _contatos(s, _fornecedor(s).id)[0].observacao == "atende o interior"
