"""As perguntas do financeiro respondidas por código — com banco de verdade.

É o primeiro degrau do assistente que o dono pediu: *"eu poder fazer qualquer
pergunta ao sistema e, se houver dado daquela pergunta, que ele me retorne."*
A pergunta PREVISTA é respondida por função escrita e testada, e não por uma
consulta que a IA inventa na hora — que acerta quase sempre e erra em silêncio
no resto.

COM BANCO DE VERDADE porque a coisa mais importante aqui é o ESCOPO, e escopo
vive no `WHERE`: a sessão dublada devolve todos os objetos e diria que está
tudo certo.

O que se prova:

  1. **A resposta respeita quem perguntou.** O administrativo de uma obra
     recebe a conta feita só sobre o que ele já veria na tela de Títulos. Se o
     assistente furasse isso, seria uma porta dos fundos para a base inteira.
  2. As contas estão certas: vencimento, atraso, o que está parado.
  3. Toda resposta diz DE ONDE VEIO e traz a ressalva quando há uma.
  4. Pergunta desconhecida responde "não encontrado", e parâmetro que a função
     não declara não passa.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.core.perguntas import catalogo
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    Parcela, StatusParcela, StatusTitulo, Titulo,
)

from conftest import como, hoje

pytestmark = pytest.mark.banco



@pytest.fixture
def cenario(sessao_real):
    """Duas obras, duas pessoas e quatro títulos com vencimentos diferentes.

    O administrativo enxerga só a obra A. É esse recorte que as perguntas têm
    de respeitar.
    """
    s = sessao_real
    chefe = Usuario(nome="Marcelo", email="chefe@bws.test",
                    senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMIN)
    adm = Usuario(nome="Rafael", email="rafa@bws.test",
                  senha_hash=gerar_hash("senha-de-teste"),
                  perfil=P.ADMINISTRATIVO_OBRA, escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    obra_a = Obra(codigo="OBRA-A", nome="Creche")
    obra_b = Obra(codigo="OBRA-B", nome="Escola")
    s.add_all([chefe, adm, obra_a, obra_b])
    s.flush()
    s.add(UsuarioObra(usuario_id=adm.id, obra_id=obra_a.id))
    s.execute(text("""
        INSERT INTO fornecedores (tipo_pessoa, cnpj_cpf, razao_social)
             VALUES ('PJ', '11444777000161', 'FORNECEDOR TESTE')"""))
    s.flush()
    forn = s.execute(text("SELECT id FROM fornecedores LIMIT 1")).scalar()
    # Título exige conta do plano; a pergunta não olha para ela, mas o banco
    # sim — e um objeto pela metade falharia por motivo errado.
    s.execute(text("""
        INSERT INTO categorias (codigo, descricao, natureza, tipos_permitidos)
             VALUES ('3.1.01', 'Cimento e Concreto Usinado', 'RESULTADO', '{}')"""))
    s.flush()
    conta = s.execute(text("SELECT id FROM categorias LIMIT 1")).scalar()

    def _titulo(numero, obra, vencimento, valor, status=StatusTitulo.APROVADO):
        t = Titulo(numero_sp=numero, fornecedor_id=forn, categoria_id=conta,
                   descricao=f"Compra {numero}",
                   valor_bruto=Decimal(valor), valor_liquido=Decimal(valor),
                   competencia=hoje().replace(day=1), status=status,
                   solicitante_id=chefe.id, tipo="T1_MATERIAL_NFE",
                   forma_pagamento="PIX")
        s.add(t)
        s.flush()
        s.execute(text("""
            INSERT INTO rateios (titulo_id, obra_id, valor, percentual)
                 VALUES (:t, :o, :v, 100)"""),
            {"t": t.id, "o": obra.id, "v": valor})
        s.add(Parcela(titulo_id=t.id, numero=1, vencimento=vencimento,
                      valor=Decimal(valor), status=StatusParcela.ABERTA))
        s.flush()
        return t

    titulos = {
        "vencido_a": _titulo("SP-1", obra_a, hoje() - timedelta(days=10), "1000.00"),
        "hoje_a": _titulo("SP-2", obra_a, hoje(), "500.00"),
        "hoje_b": _titulo("SP-3", obra_b, hoje(), "700.00"),
        "parado_a": _titulo("SP-4", obra_a, hoje() + timedelta(days=3), "300.00",
                            StatusTitulo.AGUARDANDO_APROVACAO),
    }
    return {"chefe": chefe, "adm": adm, "titulos": titulos, "sessao": s}


def _responder(cenario, quem, chave, **parametros):
    return catalogo.responder(chave, cenario["sessao"], cenario[quem], parametros)


# ---------------------------------------------------------------------------
# 1. O ESCOPO — a parte que não pode falhar
# ---------------------------------------------------------------------------
def test_a_resposta_e_calculada_no_escopo_de_quem_pergunta(cenario):
    """O assistente não pode ser porta dos fundos: o administrativo da obra A
    não enxerga o título da obra B nem pelo número, nem pela soma."""
    do_chefe = _responder(cenario, "chefe", "a_pagar_no_periodo")
    do_adm = _responder(cenario, "adm", "a_pagar_no_periodo")

    assert sorted(l["numero_sp"] for l in do_chefe["linhas"]) == ["SP-2", "SP-3"]
    assert [l["numero_sp"] for l in do_adm["linhas"]] == ["SP-2"]
    assert do_chefe["total"] == pytest.approx(1200.0)
    assert do_adm["total"] == pytest.approx(500.0), \
        "a SOMA também tem de ficar dentro do escopo, não só a lista"


def test_o_escopo_vale_para_todas_as_perguntas(cenario):
    """Basta uma pergunta esquecer o escopo para a base inteira vazar."""
    for chave in ("panorama_de_vencimentos", "a_pagar_no_periodo",
                  "vencidos_sem_pagar", "esperando_decisao", "sem_documento"):
        r = _responder(cenario, "adm", chave)
        numeros = {l.get("numero_sp") for l in r["linhas"] if l.get("numero_sp")}
        assert "SP-3" not in numeros, f"{chave} vazou título de outra obra"


def test_o_panorama_soma_so_o_que_a_pessoa_ve(cenario):
    do_chefe = _responder(cenario, "chefe", "panorama_de_vencimentos")
    do_adm = _responder(cenario, "adm", "panorama_de_vencimentos")

    hoje_chefe = next(l for l in do_chefe["linhas"] if l["faixa"] == "Vence hoje")
    hoje_adm = next(l for l in do_adm["linhas"] if l["faixa"] == "Vence hoje")
    assert hoje_chefe["valor"] == pytest.approx(1200.0)
    assert hoje_adm["valor"] == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# 2. As contas
# ---------------------------------------------------------------------------
def test_a_pagar_conta_pelo_vencimento_da_parcela(cenario):
    r = _responder(cenario, "chefe", "a_pagar_no_periodo")
    assert all(l["vencimento"] == hoje().isoformat() for l in r["linhas"])
    assert "SP-1" not in [l["numero_sp"] for l in r["linhas"]], \
        "o que venceu ontem não é 'a pagar hoje'"


def test_a_pagar_aceita_periodo_e_obra(cenario):
    tudo = _responder(cenario, "chefe", "a_pagar_no_periodo",
                      de=(hoje() - timedelta(days=30)).isoformat(),
                      ate=(hoje() + timedelta(days=30)).isoformat())
    so_a = _responder(cenario, "chefe", "a_pagar_no_periodo",
                      de=(hoje() - timedelta(days=30)).isoformat(),
                      ate=(hoje() + timedelta(days=30)).isoformat(), obra="OBRA-A")

    assert len(tudo["linhas"]) == 4
    assert {l["numero_sp"] for l in so_a["linhas"]} == {"SP-1", "SP-2", "SP-4"}


def test_vencido_traz_o_atraso_em_dias_e_o_mais_antigo_primeiro(cenario):
    r = _responder(cenario, "chefe", "vencidos_sem_pagar")
    assert [l["numero_sp"] for l in r["linhas"]] == ["SP-1"]
    assert r["linhas"][0]["atraso"] == 10


def test_o_que_esta_parado_diz_de_quem_e_a_vez(cenario):
    r = _responder(cenario, "chefe", "esperando_decisao")
    assert [l["numero_sp"] for l in r["linhas"]] == ["SP-4"]
    assert "aprova" in r["linhas"][0]["de_quem"]


def test_sem_documento_pega_os_titulos_em_aberto(cenario):
    """Nenhum título do cenário tem anexo, então todos os em aberto entram."""
    r = _responder(cenario, "chefe", "sem_documento")
    assert {l["numero_sp"] for l in r["linhas"]} == {"SP-1", "SP-2", "SP-3", "SP-4"}


# ---------------------------------------------------------------------------
# 3. Toda resposta se explica
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("chave", [
    "panorama_de_vencimentos", "a_pagar_no_periodo", "vencidos_sem_pagar",
    "esperando_decisao", "sem_documento",
])
def test_toda_resposta_diz_de_onde_veio_e_tem_frase(cenario, chave):
    """Número sem caminho de volta não dá para auditar — e na primeira resposta
    errada o dono para de confiar em todas."""
    r = _responder(cenario, "chefe", chave)
    assert r["frase"], "a resposta precisa de uma frase em português"
    assert r["de_onde_veio"]["tela"].startswith("/erp/")
    assert r["de_onde_veio"]["explicacao"]
    assert r["pergunta"] and r["chave"] == chave


@pytest.mark.parametrize("chave", [
    "panorama_de_vencimentos", "a_pagar_no_periodo", "vencidos_sem_pagar",
    "esperando_decisao", "sem_documento",
])
def test_a_frase_nao_e_corrompida_pela_formatacao_do_dinheiro(cenario, chave):
    """Defeito que a tela mostrou: a troca de ponto por vírgula estava sendo
    aplicada à FRASE INTEIRA e não ao número, e "a pagar de 01/09 a 31/12,
    somando" saía como "31/12. somando". O X é o passo do meio da troca — se
    ele aparecer, a formatação vazou para o texto."""
    frase = _responder(cenario, "chefe", chave)["frase"]
    assert "X" not in frase, f"a formatação do dinheiro vazou para a frase: {frase}"
    assert ". somando" not in frase
    assert "R$" in frase


def test_a_frase_escreve_o_dinheiro_como_se_le_em_portugues(cenario):
    from app.apps.erp.core.perguntas.respostas import _reais
    assert _reais(1234.5) == "R$ 1.234,50"
    assert _reais(1428747.14) == "R$ 1.428.747,14"
    assert _reais(0) == "R$ 0,00"


def test_a_situacao_sai_em_portugues_e_nao_como_o_banco_guarda(cenario):
    """"aguardando aprovacao", sem acento e com sublinhado, é o banco falando."""
    r = _responder(cenario, "chefe", "esperando_decisao")
    assert r["linhas"][0]["situacao"] == "esperando aprovação"
    assert "_" not in r["frase"]


def test_a_pagar_avisa_que_conta_pelo_vencimento(cenario):
    """"A pagar" tem mais de uma leitura; a que foi usada tem de estar dita."""
    r = _responder(cenario, "chefe", "a_pagar_no_periodo")
    assert "VENCIMENTO" in r["observacao"]
    assert "bloqueado" in r["observacao"]


# ---------------------------------------------------------------------------
# 4. As fronteiras
# ---------------------------------------------------------------------------
def test_pergunta_desconhecida_responde_nao_encontrado(cenario):
    with pytest.raises(ErroNaoEncontrado):
        _responder(cenario, "chefe", "quanto_e_dois_mais_dois")


def test_data_chega_como_TEXTO_da_tela_e_e_convertida(cenario):
    """A tela manda "2026-09-10", não um objeto de data — e a IA, amanhã, vai
    mandar igual. Sem converter no catálogo, a comparação com o vencimento
    estoura. Foi assim que este defeito apareceu."""
    r = _responder(cenario, "chefe", "a_pagar_no_periodo",
                   de=hoje().isoformat(), ate=hoje().isoformat())
    assert {l["numero_sp"] for l in r["linhas"]} == {"SP-2", "SP-3"}


def test_data_impossivel_nao_derruba_a_resposta(cenario):
    """Data digitada errada vira "não informado" — a pergunta responde o
    padrão dela em vez de estourar na cara de quem perguntou."""
    r = _responder(cenario, "chefe", "a_pagar_no_periodo", de="30/02/2026")
    assert r["frase"]


def test_parametro_que_a_pergunta_nao_declara_e_ignorado(cenario):
    """A tela — e amanhã a IA — não pode injetar argumento que a função não
    esperava."""
    r = _responder(cenario, "chefe", "esperando_decisao", obra="OBRA-A",
                   coisa_estranha="x")
    assert r["quantas"] == 1


def test_a_rota_do_financeiro_recusa_pergunta_de_outro_grupo(app_real, cenario):
    """Responder pergunta de outro grupo por aqui passaria por cima da ação
    daquele grupo. Fora do alcance responde 404, nunca 403."""
    r = como(app_real, cenario["chefe"].id).post(
        "/erp/api/perguntar/financeiro", json={"chave": "coisa_de_outro_grupo"})
    assert r.status_code == 404


def test_a_rota_responde_no_escopo_de_quem_esta_logado(app_real, cenario):
    cliente = como(app_real, cenario["adm"].id)
    lista = cliente.get("/erp/api/perguntas/financeiro")
    resposta = cliente.post("/erp/api/perguntar/financeiro",
                            json={"chave": "a_pagar_no_periodo"})

    assert lista.status_code == 200
    assert len(lista.get_json()["perguntas"]) == 5
    assert "funcao" not in lista.get_json()["perguntas"][0], \
        "a função não pode viajar para o navegador"
    assert resposta.status_code == 200
    assert [l["numero_sp"] for l in resposta.get_json()["resposta"]["linhas"]] == ["SP-2"]
