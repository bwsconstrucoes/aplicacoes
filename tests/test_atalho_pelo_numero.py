"""O NÚMERO VIRA LINK — /erp/ir/<numero>.

22/09/2026, pedido do dono:

  *"Cada lançamento, cadastro, uma obra, um título a pagar, um pedido de
  compra — a gente tem uma numeração, e essa numeração a gente tem um link que
  a gente possa acessar (…) da mesma forma que eu consigo acessar um card do
  Pipefy. Porque de repente eu encaminho via celular o número de um registro,
  e a pessoa só clica e puf, abre o sistema."*

O que estes testes defendem, e que é onde a ideia morre se quebrar:

1. O link entende o número que a PESSOA tem na mão (000123, PC-0001,
   CRECHE01), não o número interno do banco, que ninguém vê.
2. Quem clica sem estar logado entra e VOLTA para o registro. Antes o login
   jogava todo mundo no início, e o link mandado por WhatsApp morria na porta.
3. Número que não existe e número que a pessoa não pode ver respondem IGUAL.
   Dizer "existe, mas você não pode" já entrega que o número existe.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum import atalho
from app.apps.erp.core.titulos import service as svc_tit
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Obra, PerfilUsuario as P, TipoPessoa, Usuario,
)

from conftest import como

pytestmark = pytest.mark.banco


def _pessoa(s, apelido, perfil=P.ADMIN):
    u = Usuario(nome=apelido, email=f"{apelido}@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                telefone="5585900000000")
    s.add(u)
    s.flush()
    return u


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    obra = Obra(codigo="ATALHO01", nome="Obra do teste de atalho", status="ATIVA")
    cat = Categoria(codigo="3.1.97", descricao="Material do atalho", ativo=True,
                    grupo_codigo="3", grupo_nome="Custos")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="MADEIREIRA DO ATALHO LTDA", ativo=True)
    s.add_all([obra, cat, forn])
    s.flush()
    dono = _pessoa(s, "dono-atalho")
    titulo = svc_tit.criar_titulo(s, {
        "tipo": "T14_EXCECAO_SEM_NOTA",
        "fornecedor_id": forn.id, "categoria_id": cat.id,
        "descricao": "compra para o teste do atalho",
        "valor_bruto": "500.00", "forma_pagamento": "DINHEIRO",
        "justificativa_excecao": "sem nota fiscal, teste",
        "parcelas": [{"vencimento": (date.today() + timedelta(days=30)).isoformat(),
                      "valor": "500.00"}],
        "rateios": [{"obra_id": obra.id, "valor": "500.00"}],
    }, dono)
    s.flush()
    return {"s": s, "obra": obra, "forn": forn, "dono": dono, "titulo": titulo}


# ---------------------------------------------------------------------------
# 1. O número que a pessoa tem na mão
# ---------------------------------------------------------------------------
def test_o_numero_do_lancamento_leva_a_ficha_dele(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).get(f"/erp/ir/{c['titulo'].numero_sp}")

    assert r.status_code == 302
    assert r.headers["Location"] == f"/erp/titulos?titulo={c['titulo'].id}"


def test_o_codigo_da_obra_leva_ao_cadastro_dela(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).get("/erp/ir/ATALHO01")

    assert r.status_code == 302
    assert r.headers["Location"] == f"/erp/obras?obra={c['obra'].id}"


@pytest.mark.parametrize("digitado", ["ATALHO01", "atalho01", " ATALHO01 "])
def test_maiuscula_minuscula_e_espaco_nao_atrapalham(digitado, cenario, app_real):
    """A pessoa cola de um WhatsApp, de um PDF, do próprio sistema. O número
    chega sujo de jeitos diferentes e é sempre o mesmo registro."""
    c = cenario
    r = como(app_real, c["dono"].id).get(f"/erp/ir/{digitado}")
    assert r.status_code == 302
    assert f"obra={c['obra'].id}" in r.headers["Location"]


def test_o_lancamento_e_achado_sem_os_zeros_da_frente(cenario, app_real):
    """Ninguém fala "zero zero zero cento e vinte e três" no telefone."""
    c = cenario
    sem_zeros = c["titulo"].numero_sp.lstrip("0")
    r = como(app_real, c["dono"].id).get(f"/erp/ir/{sem_zeros}")
    assert r.status_code == 302
    assert f"titulo={c['titulo'].id}" in r.headers["Location"]


def test_o_enfeite_que_a_pessoa_digita_junto_e_ignorado(cenario):
    """"SP 000123", "nº 000123", "#000123" — tudo o mesmo lançamento."""
    for bruto in ["SP 000123", "sp-000123", "Nº 000123", "#000123", " 000123 "]:
        assert atalho.limpar(bruto) == "000123", bruto


# ---------------------------------------------------------------------------
# 2. O caminho de quem clica no WhatsApp
# ---------------------------------------------------------------------------
def test_quem_nao_esta_logado_e_levado_ao_login_com_o_destino_junto(app_real):
    """Sem isto, o link mandado por WhatsApp morre na porta: a pessoa entra e
    cai no início, tendo de caçar o registro na mão."""
    r = app_real.test_client().get("/erp/ir/000123")

    assert r.status_code == 302
    assert r.headers["Location"] == "/erp/entrar?proximo=/erp/ir/000123"


def test_a_tela_de_entrada_guarda_o_destino_e_diz_por_que(app_real):
    r = app_real.test_client().get("/erp/entrar?proximo=/erp/ir/000123")

    corpo = r.get_data(as_text=True)
    assert 'name="proximo" value="/erp/ir/000123"' in corpo
    assert "Entre para abrir o registro" in corpo


def test_depois_de_entrar_a_pessoa_vai_para_onde_ia(cenario, app_real):
    c = cenario
    cliente = app_real.test_client()
    r = cliente.post("/erp/entrar", data={
        "email": c["dono"].email, "senha": "senha-de-teste-123",
        "proximo": f"/erp/ir/{c['titulo'].numero_sp}"})

    assert r.status_code == 302
    assert r.headers["Location"] == f"/erp/ir/{c['titulo'].numero_sp}"


@pytest.mark.parametrize("destino", [
    "https://site-de-fora.com/roubar",
    "//site-de-fora.com/roubar",
    "/outra-coisa/qualquer",
    "http://localhost/erp/inicio",
])
def test_o_login_NAO_leva_para_fora_do_sistema(destino, cenario, app_real):
    """O golpe clássico: um link que passa pela tela de entrada da BWS e joga a
    pessoa, já convencida de que está no sistema, num site de fora pedindo a
    senha. Só caminho que começa em /erp/ passa."""
    c = cenario
    r = app_real.test_client().post("/erp/entrar", data={
        "email": c["dono"].email, "senha": "senha-de-teste-123",
        "proximo": destino})

    assert r.status_code == 302
    assert r.headers["Location"] == "/erp/inicio", f"aceitou {destino!r}"


# ---------------------------------------------------------------------------
# 3. O que não existe e o que não se pode ver respondem igual
# ---------------------------------------------------------------------------
def test_numero_que_nao_existe_responde_nao_encontrado(cenario, app_real):
    c = cenario
    r = como(app_real, c["dono"].id).get("/erp/ir/PC-9999")

    assert r.status_code == 404
    corpo = r.get_data(as_text=True)
    assert "Não achei esse número" in corpo
    assert "<html" in corpo.lower(), "tem de ser uma tela, não JSON cru"


def test_quem_nao_tem_a_acao_recebe_a_MESMA_resposta_de_inexistente(cenario, app_real):
    """Dizer "existe, mas você não pode" já entrega que o número existe — e
    varrer números mapearia o sistema sem abrir um registro.

    A cotação serve de prova porque `comprar` é ação estreita de verdade: só
    ADMIN e diretor financeiro a têm. O lançamento não serviria — `ver_titulos`
    é de todo mundo, e o recorte por obra é que limita o que cada um enxerga.
    """
    from app.apps.erp.db.models.cadastros import Cotacao

    c = cenario
    cot = Cotacao(numero="COT-9001", titulo="Cotação do teste de atalho",
                  criado_por=c["dono"].id)
    c["s"].add(cot)
    c["s"].flush()
    dp = _pessoa(c["s"], "dp-atalho", P.DEPARTAMENTO_PESSOAL)

    r_existe = como(app_real, dp.id).get("/erp/ir/COT-9001")
    r_nao_existe = como(app_real, dp.id).get("/erp/ir/COT-9999")

    assert r_existe.status_code == r_nao_existe.status_code == 404
    # A ÚNICA diferença aceitável é o número ecoado de volta — que é o que a
    # própria pessoa digitou. Tirando ele, as duas respostas têm de ser iguais
    # byte a byte: qualquer palavra a mais numa delas seria a dica de que
    # aquele número existe.
    existe = r_existe.get_data(as_text=True).replace("COT-9001", "X")
    nao_existe = r_nao_existe.get_data(as_text=True).replace("COT-9999", "X")
    assert existe == nao_existe, "a resposta tem de ser indistinguível"


def test_quem_TEM_a_acao_chega_na_cotacao(cenario, app_real):
    """O outro lado da moeda: a recusa acima é por permissão, não por defeito."""
    from app.apps.erp.db.models.cadastros import Cotacao

    c = cenario
    cot = Cotacao(numero="COT-9002", titulo="Cotação que abre",
                  criado_por=c["dono"].id)
    c["s"].add(cot)
    c["s"].flush()

    r = como(app_real, c["dono"].id).get("/erp/ir/COT-9002")
    assert r.status_code == 302
    assert r.headers["Location"] == f"/erp/suprimentos/cotacoes?cotacao={cot.id}"


def test_numero_sem_formato_de_numero_nao_vai_ao_banco(cenario):
    """Proteção de porta: o que nem parece um número é recusado antes de
    virar consulta."""
    c = cenario
    for bruto in ["", "   ", "'; DROP TABLE titulos; --", "a" * 80, "<script>"]:
        assert atalho.achar(c["s"], bruto) == [], bruto


# ---------------------------------------------------------------------------
# 4. O link que a tela copia é o mesmo que a rota entende
# ---------------------------------------------------------------------------
def test_o_link_montado_bate_com_a_rota_que_o_atende(cenario, app_real):
    """Se os dois divergirem, o botão de copiar passa a gerar link quebrado —
    e ninguém descobre até alguém do outro lado reclamar."""
    c = cenario
    with app_real.test_request_context():
        link = atalho.link_pelo_numero(c["titulo"].numero_sp, absoluto=False)
    assert link == f"/erp/ir/{c['titulo'].numero_sp}"

    r = como(app_real, c["dono"].id).get(link)
    assert r.status_code == 302


def test_todo_destino_aponta_para_tela_que_existe(app_real):
    """Uma tela renomeada quebraria o atalho calada. Isto pega na hora."""
    from flask import url_for
    with app_real.test_request_context():
        for d in atalho.DESTINOS:
            endereco = url_for(d.endpoint, **{d.parametro: 1})
            assert endereco.startswith("/erp/"), f"{d.chave}: {endereco}"


def test_toda_acao_declarada_nos_destinos_existe_de_verdade():
    """Ação escrita errada aqui fecharia o atalho para todo mundo, em silêncio."""
    from app.apps.erp.core.auth.permissoes import PERMISSOES
    for d in atalho.DESTINOS:
        assert d.acao in PERMISSOES, f"{d.chave} declara ação inexistente: {d.acao}"
