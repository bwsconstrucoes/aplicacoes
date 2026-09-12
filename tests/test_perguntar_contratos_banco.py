"""A régua do recebimento — "quanto falta receber" nas quatro leituras.

O dono desfez esta pergunta ele mesmo, em 10/09/2026:

    "Eu posso entender quanto falta receber do que já foi emitido nota, ou seja
    do faturado; quanto falta receber do contrato inteiro, tendo sido medido ou
    não; quanto falta receber do que já está medido; (…) Realmente aí uma
    pergunta que tem várias variáveis."

Todas as leituras estão certas — elas são etapas de uma mesma esteira:

    CONTRATO (+aditivos) → MEDIDO → FATURADO (nota) → RECEBIDO

A decisão que virou código: em vez de escolher uma leitura e responder um
número (certo para uma, errado para as outras três), a resposta mostra a RÉGUA
INTEIRA. Assim a leitura que ele queria já está na tela.

COM BANCO DE VERDADE porque a aritmética atravessa contrato, medições, notas e
recebimentos, e a sessão dublada ignora `WHERE` e `JOIN`.

O que se prova:

  1. As três leituras dão números DIFERENTES sobre o mesmo contrato — e cada
     uma bate com a etapa dela.
  2. A esteira fecha: o que falta receber do contrato = o que falta medir + o
     que falta receber do medido.
  3. A aritmética é a MESMA da tela do contrato (reusa `quadro`), então os dois
     números não podem divergir.
  4. Quem não tem a ação `ver_contratos` não alcança este grupo — nem a lista.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.perguntas import catalogo
from app.apps.erp.core.titulos import quadro as svc_quadro
from app.apps.erp.db.models.cadastros import (
    Obra, PerfilUsuario as P, Usuario,
)

from conftest import como, hoje

pytestmark = pytest.mark.banco



@pytest.fixture
def contrato(sessao_real):
    """Um contrato de R$ 100.000 com R$ 60.000 medidos, R$ 40.000 faturados e
    R$ 25.000 recebidos.

    Os números foram escolhidos para as três leituras darem resultados
    diferentes e verificáveis de cabeça:
      · falta receber do CONTRATO  = 100.000 − 25.000 = 75.000
      · falta receber do MEDIDO    =  60.000 − 25.000 = 35.000
      · falta receber do FATURADO  =  40.000 − 25.000 = 15.000
    """
    s = sessao_real
    chefe = Usuario(nome="Marcelo", email="chefe@bws.test",
                    senha_hash=gerar_hash("senha-de-teste"), perfil=P.ADMIN)
    peao = Usuario(nome="Rafael", email="rafa@bws.test",
                   senha_hash=gerar_hash("senha-de-teste"),
                   perfil=P.ADMINISTRATIVO_OBRA)
    obra = Obra(codigo="OBRA-A", nome="Creche")
    s.add_all([chefe, peao, obra])
    s.flush()
    s.execute(text("""
        INSERT INTO fornecedores (tipo_pessoa, cnpj_cpf, razao_social)
             VALUES ('PJ', '11444777000161', 'CLIENTE TESTE')"""))
    s.execute(text("""
        INSERT INTO categorias (codigo, descricao, natureza, tipos_permitidos)
             VALUES ('1.1.01', 'Receita de obras', 'RESULTADO', '{}')"""))
    s.flush()
    forn = s.execute(text("SELECT id FROM fornecedores LIMIT 1")).scalar()
    conta = s.execute(text("SELECT id FROM categorias LIMIT 1")).scalar()
    s.execute(text("""
        INSERT INTO contratos (fornecedor_id, obra_id, tipo, objeto, valor_total,
                               vigencia_inicio, status)
             VALUES (:f, :o, 'OBRA', 'Construção da creche', 100000,
                     :d, 'ATIVO')"""),
        {"f": forn, "o": obra.id, "d": hoje()})
    s.flush()
    contrato_id = s.execute(text("SELECT id FROM contratos LIMIT 1")).scalar()

    # Duas medições: R$ 40.000 (faturada e recebida em parte) e R$ 20.000 (sem nota)
    for numero, valor, nota in ((1, "40000.00", "NF-100"), (2, "20000.00", None)):
        s.execute(text("""
            INSERT INTO titulos (numero_sp, tipo, fornecedor_id, categoria_id,
                                 contrato_id, numero_medicao, descricao,
                                 valor_bruto, valor_liquido, competencia, status,
                                 solicitante_id, forma_pagamento, especie,
                                 notas_fiscais)
                 VALUES (:n, 'T2_SERVICO_NFSE', :f, :c, :ct, :m, :d, :v, :v,
                         :comp, 'APROVADO', :u, 'PIX', 'RECEBER', :nf)"""),
            {"n": f"MED-{numero}", "f": forn, "c": conta, "ct": contrato_id,
             "m": numero, "d": f"Medição {numero}", "v": Decimal(valor),
             "comp": hoje().replace(day=1), "u": chefe.id,
             "nf": [nota] if nota else []})
    s.flush()
    medicao = s.execute(text(
        "SELECT id FROM titulos WHERE numero_sp = 'MED-1'")).scalar()

    # O "faturado" do quadro vem da NOTA EMITIDA, não do campo de texto do
    # título — a nota é o documento que existe de verdade, e é ela que o
    # cliente recebe. Preencher só o campo de texto passaria no dublê e
    # falharia aqui, que é justamente o ponto deste arquivo.
    s.execute(text("""
        INSERT INTO empresas (razao_social, cnpj)
             VALUES ('BWS CONSTRUCOES', '11444777000161')
        ON CONFLICT DO NOTHING"""))
    s.flush()
    empresa = s.execute(text("SELECT id FROM empresas LIMIT 1")).scalar()
    s.execute(text("""
        INSERT INTO notas_emitidas (empresa_id, numero_dps, numero_nota,
                                    titulo_id, obra_id, situacao, valor_bruto,
                                    valor_liquido, data_emissao)
             VALUES (:e, 1, 'NF-100', :t, :o, 'EMITIDA', 40000, 40000, :d)"""),
        {"e": empresa, "t": medicao, "o": obra.id, "d": hoje()})
    s.flush()

    # Recebimento de R$ 25.000 na medição faturada
    s.execute(text("""
        INSERT INTO parcelas (titulo_id, numero, vencimento, valor, status)
             VALUES (:t, 1, :d, 40000, 'ABERTA')"""),
        {"t": medicao, "d": hoje()})
    s.flush()
    parcela = s.execute(text(
        "SELECT id FROM parcelas WHERE titulo_id = :t"), {"t": medicao}).scalar()
    s.execute(text("""
        INSERT INTO contas_bancarias (banco_codigo, agencia, conta, descricao)
             VALUES ('237', '0001', '12345-6', 'Conta de teste')"""))
    s.flush()
    conta_banco = s.execute(text("SELECT id FROM contas_bancarias LIMIT 1")).scalar()
    s.execute(text("""
        INSERT INTO pagamentos (parcela_id, conta_bancaria_id, valor_pago,
                                data_pagamento, meio)
             VALUES (:p, :cb, 25000, :d, 'PIX')"""),
        {"p": parcela, "cb": conta_banco, "d": hoje()})
    s.flush()
    return {"chefe": chefe, "peao": peao, "sessao": s, "id": contrato_id}


def _responder(contrato, quem, chave, **parametros):
    return catalogo.responder(chave, contrato["sessao"], contrato[quem], parametros)


# ---------------------------------------------------------------------------
# 1 e 2. As três leituras, e a esteira que fecha
# ---------------------------------------------------------------------------
def test_as_tres_leituras_dao_numeros_diferentes_e_todas_certas(contrato):
    r = _responder(contrato, "chefe", "falta_receber")
    linha = r["linhas"][0]

    assert linha["falta_receber_do_contrato"] == pytest.approx(75000.0)
    assert linha["falta_receber_do_medido"] == pytest.approx(35000.0)
    assert linha["a_receber"] == pytest.approx(15000.0)


def test_a_resposta_mostra_a_regua_inteira_e_nao_um_numero_so(contrato):
    """O ponto da decisão do dono: ele não precisa ter acertado a pergunta."""
    r = _responder(contrato, "chefe", "falta_receber")
    colunas = {c["chave"] for c in r["colunas"]}

    assert {"falta_receber_do_contrato", "falta_receber_do_medido",
            "a_receber"} <= colunas
    assert "CONTRATO" in r["frase"] and "MEDIDO" in r["frase"] and "NOTA" in r["frase"]
    assert "três leituras" in r["observacao"].lower() or \
           "TRÊS leituras" in r["observacao"]


def test_a_esteira_fecha(contrato):
    """Falta receber do contrato = falta medir + falta receber do medido.
    Se isso não fechar, um dos números está errado — e não dá para saber qual
    olhando só para ele."""
    r = _responder(contrato, "chefe", "falta_receber")
    linha = r["linhas"][0]
    falta_medir = linha["vigente"] - linha["medido"]

    assert linha["falta_receber_do_contrato"] == pytest.approx(
        falta_medir + linha["falta_receber_do_medido"])


def test_medido_sem_nota_acha_a_medicao_que_nao_virou_cobranca(contrato):
    r = _responder(contrato, "chefe", "medido_sem_nota")
    assert r["total"] == pytest.approx(20000.0)
    assert "20.000,00" in r["frase"]


def test_faturado_sem_receber_acha_o_que_esta_na_mao_do_cliente(contrato):
    r = _responder(contrato, "chefe", "faturado_sem_receber")
    assert r["total"] == pytest.approx(15000.0)


def test_o_filtro_por_obra_funciona(contrato):
    achou = _responder(contrato, "chefe", "falta_receber", obra="OBRA-A")
    nao_achou = _responder(contrato, "chefe", "falta_receber", obra="OBRA-Z")

    assert len(achou["linhas"]) == 1
    assert nao_achou["linhas"] == []
    assert "Nenhum contrato encontrado" in nao_achou["frase"]


# ---------------------------------------------------------------------------
# 3. Não pode divergir da tela
# ---------------------------------------------------------------------------
def test_o_numero_da_pergunta_e_o_mesmo_da_tela_do_contrato(contrato):
    """Já aconteceu neste arquivo: a lista descontava o reajuste do saldo e o
    quadro não, e os dois números apareciam na mesma sessão sobre o mesmo
    contrato. Quem visse isso perderia a confiança nos dois — com razão."""
    da_pergunta = _responder(contrato, "chefe", "falta_receber")["linhas"][0]
    da_tela = svc_quadro.quadro(contrato["sessao"], contrato["id"])["totais"]

    for campo in ("vigente", "medido", "faturado", "recebido",
                  "falta_receber_do_contrato", "falta_receber_do_medido",
                  "a_receber"):
        assert da_pergunta[campo] == pytest.approx(da_tela[campo]), campo


def test_toda_resposta_de_contrato_diz_de_onde_veio(contrato):
    for chave in ("falta_receber", "medido_sem_nota", "faturado_sem_receber"):
        r = _responder(contrato, "chefe", chave)
        assert r["de_onde_veio"]["tela"] == "/erp/contratos"
        assert r["frase"] and "X" not in r["frase"]


# ---------------------------------------------------------------------------
# 4. A fronteira do grupo
# ---------------------------------------------------------------------------
def test_quem_nao_tem_a_acao_nao_alcanca_o_grupo_de_contratos(app_real, contrato):
    """O quadro mostra o contrato de ponta a ponta e não se recorta por obra
    designada sem mentir no total — por isso a ação é estreita."""
    cliente = como(app_real, contrato["peao"].id)
    assert cliente.get("/erp/api/perguntas/contratos").status_code == 403
    assert cliente.post("/erp/api/perguntar/contratos",
                        json={"chave": "falta_receber"}).status_code == 403


def test_a_rota_do_financeiro_nao_responde_pergunta_de_contrato(app_real, contrato):
    """Senão a ação estreita do grupo de contratos seria contornada pela rota
    larga do financeiro."""
    r = como(app_real, contrato["chefe"].id).post(
        "/erp/api/perguntar/financeiro", json={"chave": "falta_receber"})
    assert r.status_code == 404


def test_quem_tem_a_acao_ve_as_perguntas_de_contrato(app_real, contrato):
    cliente = como(app_real, contrato["chefe"].id)
    lista = cliente.get("/erp/api/perguntas/contratos")
    resposta = cliente.post("/erp/api/perguntar/contratos",
                            json={"chave": "falta_receber"})

    assert lista.status_code == 200
    assert {p["chave"] for p in lista.get_json()["perguntas"]} == {
        "falta_receber", "medido_sem_nota", "faturado_sem_receber"}
    assert resposta.status_code == 200
    assert resposta.get_json()["resposta"]["linhas"][0][
        "falta_receber_do_contrato"] == pytest.approx(75000.0)
