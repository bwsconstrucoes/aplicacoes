# -*- coding: utf-8 -*-
"""
A única parte do painel que ESCREVE num sistema de fora.

Todo o resto lê. Aqui um erro não mostra um número torto numa tela: altera o
cadastro da empresa no OMIE, e desfazer é trabalho manual, título a título. O
dono disse a frase que define o tom: *"é realmente algo sério, eu não posso
falhar nem errar"*.

O que se prova aqui são as quatro proteções, uma por uma:
  1. sem a senha própria configurada, executar é impossível;
  2. simulação é o padrão — só sai do ensaio quem pede E acerta a senha;
  3. título rateado entre obras é RECUSADO, porque trocar o departamento
     apagaria o rateio;
  4. tudo o que foi enviado fica registrado, inclusive o que deu errado.

E, no meio disso, a regra que faz a alteração ser cirúrgica: parte do cadastro
CONSULTADO no OMIE, e não de um dicionário montado à mão — o Alterar substitui
o título inteiro, então o que não voltar se perde.
"""
from __future__ import annotations

import os

import pytest

from app.apps.painel.sync import omie_escrita

pytestmark = pytest.mark.banco


# ===========================================================================
# 1. Preparar a alteração — sem banco, sem rede
# ===========================================================================
CADASTRO_DO_OMIE = {
    "codigo_lancamento_omie": 123,
    "codigo_categoria": "1.01.01",
    "numero_documento": "NF 900",
    "valor_documento": 1000.0,
    "distribuicao": [{"cCodDep": "D1", "nPerDep": 60},
                     {"cCodDep": "D2", "nPerDep": 40}],
    "categorias": [{"codigo_categoria": "1.01.01", "percentual": 100}],
    # os que o OMIE devolve e recusa de volta
    "info": {"dInc": "01/01/2025"}, "status_titulo": "PAGO",
    "valor_pago": 1000.0, "nCodTitulo": 999, "baixa_realizada": "S",
}


def test_o_que_muda_e_so_o_que_foi_pedido():
    """Valor, data e documento NUNCA mudam: esses títulos têm baixa e
    conciliação, e mexer neles quebraria a contabilidade."""
    novo, _ = omie_escrita.preparar_alteracao(CADASTRO_DO_OMIE,
                                              codigo_categoria="2.02.02")
    assert novo["codigo_categoria"] == "2.02.02"
    assert novo["valor_documento"] == 1000.0
    assert novo["numero_documento"] == "NF 900"
    assert novo["distribuicao"] == CADASTRO_DO_OMIE["distribuicao"]


def test_os_campos_so_de_leitura_nao_voltam_para_o_omie():
    """Mandá-los de volta faz a chamada inteira falhar, e a mensagem do OMIE
    não diz qual foi o culpado."""
    novo, _ = omie_escrita.preparar_alteracao(CADASTRO_DO_OMIE,
                                              codigo_categoria="2.02.02")
    for campo in omie_escrita.SO_LEITURA:
        assert campo not in novo, f"{campo} não pode voltar para o OMIE"


def test_trocar_a_categoria_tira_a_categoria_multipla():
    """Deixar o array `categorias` junto faz o OMIE não saber qual das duas
    vale."""
    novo, _ = omie_escrita.preparar_alteracao(CADASTRO_DO_OMIE,
                                              codigo_categoria="2.02.02")
    assert "categorias" not in novo


def test_trocar_o_departamento_poe_cem_por_cento_no_novo():
    """É a regra que torna a troca destrutiva — e por isso ela é explícita e
    tem trava na camada de cima."""
    novo, mudancas = omie_escrita.preparar_alteracao(CADASTRO_DO_OMIE,
                                                     cod_departamento="D9")
    assert novo["distribuicao"] == [{"cCodDep": "D9", "nPerDep": 100}]
    assert any("D1, D2 → D9" in m for m in mudancas), mudancas


def test_sem_pedir_nada_nada_muda():
    novo, mudancas = omie_escrita.preparar_alteracao(CADASTRO_DO_OMIE)
    assert mudancas == []
    assert novo["codigo_categoria"] == "1.01.01"


def test_o_tipo_do_titulo_sai_do_rotulo_do_fato():
    assert omie_escrita.tipo_do_titulo("2. Contas a Pagar") == "pagar"
    assert omie_escrita.tipo_do_titulo("1. Contas a Receber") == "receber"


# ===========================================================================
# 2. As proteções — com banco de verdade
# ===========================================================================
@pytest.fixture()
def base_de_saneamento():
    """Dois títulos: um em uma obra só, outro rateado entre duas."""
    from tests.conftest import VARIAVEL_BANCO_TESTE, url_de_teste_segura

    bruto = os.environ.get(VARIAVEL_BANCO_TESTE, "").strip()
    if not bruto:
        pytest.skip(f"{VARIAVEL_BANCO_TESTE} não definida — testes com banco pulados")
    os.environ["DATABASE_URL"] = url_de_teste_segura(bruto)

    from app.apps.painel import consultas, db as painel_db, migracoes_runner
    painel_db._engine = None
    resultado = migracoes_runner.aplicar_pendentes()
    assert not resultado.get("erro"), resultado

    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM rateio")
        conn.execute("DELETE FROM alteracoes_omie")
        for codigo, dep in ((501, "CASA"), (502, "CASA"), (502, "PREDIO")):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao,"
                " categoria, codigo_categoria, departamento, razao_social,"
                " numero_documento, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar','DRE','Pago','Serviços','1.01',"
                "         ?,'FORNECEDOR','NF 1',-100,0,0,0)", (codigo, dep))
        # o 502 esta rateado entre duas obras — e o que a trava protege
        conn.execute("INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep,"
                     " cdesdep, nperdep, nvaldep) VALUES (501,1,'D1','CASA',100,100)")
        conn.execute("INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep,"
                     " cdesdep, nperdep, nvaldep) VALUES (502,1,'D1','CASA',60,60)")
        conn.execute("INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep,"
                     " cdesdep, nperdep, nvaldep) VALUES (502,2,'D2','PREDIO',40,40)")
        conn.commit()
    consultas.esquecer_listas()
    yield
    with painel_db.conexao() as conn:
        conn.execute("TRUNCATE TABLE fato")
        conn.execute("DELETE FROM rateio")
        conn.commit()


class ClienteFalso:
    """Um OMIE de mentira: registra o que seria enviado, sem rede."""

    def __init__(self, explode=False):
        self.consultados, self.enviados, self.explode = [], [], explode

    def consultar_titulo(self, codigo, tipo):
        self.consultados.append((codigo, tipo))
        return dict(CADASTRO_DO_OMIE, codigo_lancamento_omie=codigo)

    def alterar_titulo(self, cadastro, tipo):
        if self.explode:
            raise RuntimeError("o OMIE recusou")
        self.enviados.append((cadastro, tipo))
        return {"codigo_status": "0", "descricao_status": "alterado"}


def test_o_rateio_e_lido_do_espelho(base_de_saneamento):
    from app.apps.painel import saneamento
    assert saneamento.rateio_do_titulo(501) == ["CASA"]
    assert sorted(saneamento.rateio_do_titulo(502)) == ["CASA", "PREDIO"]


def test_titulo_rateado_entre_obras_e_recusado(base_de_saneamento):
    """A proteção mais importante: trocar o departamento apagaria o rateio, e
    ninguém reconstrói isso depois."""
    from app.apps.painel import saneamento
    cliente = ClienteFalso()
    r = saneamento.aplicar([501, 502], departamento_novo="D9",
                           simulacao=True, cliente=cliente)
    por_codigo = {l["codigo"]: l for l in r["linhas"]}
    assert por_codigo[502]["resultado"].startswith("RECUSADO")
    assert "CASA" in por_codigo[502]["resultado"]
    assert not por_codigo[501]["resultado"].startswith("RECUSADO")
    # e o recusado nem chega a ser consultado no OMIE
    assert 502 not in [c for c, _ in cliente.consultados]


def test_quem_aceita_desfazer_o_rateio_passa(base_de_saneamento):
    """A trava protege de engano, não impede uma decisão consciente."""
    from app.apps.painel import saneamento
    r = saneamento.aplicar([502], departamento_novo="D9", simulacao=True,
                           aceita_desfazer_rateio=True, cliente=ClienteFalso())
    assert not r["linhas"][0]["resultado"].startswith("RECUSADO")


def test_trocar_so_a_categoria_nao_esbarra_na_trava(base_de_saneamento):
    """A trava é do departamento: mexer só na categoria não apaga rateio
    nenhum."""
    from app.apps.painel import saneamento
    r = saneamento.aplicar([502], categoria_nova="2.02", simulacao=True,
                           cliente=ClienteFalso())
    assert not r["linhas"][0]["resultado"].startswith("RECUSADO")


def test_o_ensaio_nao_envia_nada(base_de_saneamento):
    from app.apps.painel import saneamento
    cliente = ClienteFalso()
    r = saneamento.aplicar([501], categoria_nova="2.02", simulacao=True,
                           cliente=cliente)
    assert r["simulacao"] is True
    assert cliente.enviados == [], "o ensaio não pode enviar"
    assert "Ensaio" in r["linhas"][0]["resultado"]
    assert r["linhas"][0]["mudancas"], "mas tem de mostrar o que mudaria"


def test_executar_de_verdade_envia_e_registra(base_de_saneamento, monkeypatch):
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "segredo-de-execucao")
    cliente = ClienteFalso()
    r = saneamento.aplicar([501], categoria_nova="2.02", simulacao=False,
                           cliente=cliente)
    assert r["alterados"] == 1
    assert len(cliente.enviados) == 1
    cadastro, tipo = cliente.enviados[0]
    assert cadastro["codigo_categoria"] == "2.02" and tipo == "pagar"

    registros = saneamento.historico()
    assert registros and registros[0]["codigo"] == 501
    assert registros[0]["ok"] is True
    assert "1.01.01 → 2.02" in registros[0]["mudancas"]


def test_sem_a_senha_configurada_executar_e_recusado(base_de_saneamento, monkeypatch):
    """Padrão NEGAR: sem a senha própria no ambiente, alterar fica desligado."""
    from app.apps.painel import saneamento
    monkeypatch.delenv("PAINEL_SENHA_ESCRITA", raising=False)
    assert saneamento.escrita_configurada() is False
    cliente = ClienteFalso()
    r = saneamento.aplicar([501], categoria_nova="2.02", simulacao=False,
                           cliente=cliente)
    assert r["ok"] is False
    assert "desligada" in r["erro"]
    assert cliente.enviados == []


def test_a_senha_de_execucao_aceita_acento(monkeypatch):
    """Mesma armadilha do login, que derrubou a tela em 04/09: `compare_digest`
    recusa texto fora do ASCII e ESTOURA em vez de devolver False."""
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "execução-2026")
    assert saneamento.senha_de_escrita_confere("execução-2026") is True
    assert saneamento.senha_de_escrita_confere("execucao-2026") is False
    monkeypatch.delenv("PAINEL_SENHA_ESCRITA")
    assert saneamento.senha_de_escrita_confere("qualquer") is False


def test_um_titulo_com_erro_nao_derruba_o_lote(base_de_saneamento, monkeypatch):
    """E o erro fica registrado — é justamente quando o registro importa."""
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "x")
    r = saneamento.aplicar([501], categoria_nova="2.02", simulacao=False,
                           cliente=ClienteFalso(explode=True))
    assert r["ok"] is True
    assert "ERRO" in r["linhas"][0]["resultado"]
    registros = saneamento.historico()
    assert registros[0]["ok"] is False
    assert "recusou" in registros[0]["retorno"]


def test_sem_escolher_o_que_mudar_nao_faz_nada(base_de_saneamento):
    from app.apps.painel import saneamento
    r = saneamento.aplicar([501], simulacao=True, cliente=ClienteFalso())
    assert r["ok"] is False and "Escolha" in r["erro"]


def test_o_lote_tem_teto(base_de_saneamento, monkeypatch):
    """O limite não é técnico: é para um engano de seleção não virar um estrago
    de mil títulos antes de alguém perceber."""
    from app.apps.painel import saneamento
    monkeypatch.setattr(saneamento, "TETO_POR_LOTE", 1)
    r = saneamento.aplicar([501, 502], categoria_nova="2.02", simulacao=True,
                           cliente=ClienteFalso())
    assert r["ok"] is False and "limite por vez" in r["erro"]


# ===========================================================================
# 3. A tela — a senha é exigida AQUI, na porta
# ===========================================================================
@pytest.fixture()
def cliente_web(base_de_saneamento, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    c = app.test_client()
    c.post("/painel/entrar", data={"senha": "segredo-de-teste"})
    return c


def test_a_tela_avisa_que_isto_escreve_no_omie(cliente_web):
    """Quem abre tem de saber, antes de qualquer coisa, que aqui não é o
    painel que muda: é o OMIE."""
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert "altera o cadastro no OMIE" in html
    assert "apaga qualquer rateio" in html


def test_sem_a_senha_configurada_o_botao_de_executar_vem_desligado(cliente_web,
                                                                   monkeypatch):
    monkeypatch.delenv("PAINEL_SENHA_ESCRITA", raising=False)
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert "execução está desligada neste serviço" in html
    assert "disabled" in html


def test_executar_com_senha_errada_nao_envia_nada(cliente_web, monkeypatch):
    """A porta. Errar a senha aqui não pode virar meio envio."""
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "a-senha-certa")
    chamou = []
    monkeypatch.setattr(saneamento, "aplicar",
                        lambda *a, **k: chamou.append(1) or {"ok": True})

    r = cliente_web.post("/painel/explorador/alterar?busca=FORNECEDOR", data={
        "codigo": "501", "categoria_nova": "2.02",
        "executar": "1", "senha": "chute"})
    assert r.status_code == 200
    assert "Senha de execução incorreta" in r.get_data(as_text=True)
    assert not chamou, "nem chegou a tentar alterar"


def test_ensaiar_nao_pede_senha(cliente_web, monkeypatch):
    """O ensaio é seguro por construção: ele não envia. Pedir senha para
    ensaiar só faria as pessoas pularem o ensaio."""
    from app.apps.painel import saneamento
    pedidos = []
    monkeypatch.setattr(saneamento, "aplicar",
                        lambda *a, **k: pedidos.append(k) or
                        {"ok": True, "simulacao": True, "linhas": [],
                         "quantos": 0, "alterados": 0, "recusados": 0})
    r = cliente_web.post("/painel/explorador/alterar?busca=FORNECEDOR", data={
        "codigo": "501", "categoria_nova": "2.02", "executar": "0"})
    assert r.status_code == 200
    assert pedidos and pedidos[0]["simulacao"] is True


def test_executar_com_a_senha_certa_chega_na_alteracao(cliente_web, monkeypatch):
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "a-senha-certa")
    pedidos = []
    monkeypatch.setattr(saneamento, "aplicar",
                        lambda *a, **k: pedidos.append(k) or
                        {"ok": True, "simulacao": False, "linhas": [],
                         "quantos": 1, "alterados": 1, "recusados": 0})
    r = cliente_web.post("/painel/explorador/alterar?busca=FORNECEDOR", data={
        "codigo": "501", "categoria_nova": "2.02",
        "executar": "1", "senha": "a-senha-certa"})
    assert r.status_code == 200
    assert pedidos and pedidos[0]["simulacao"] is False


def test_alterar_exige_login_como_todo_o_resto(base_de_saneamento, monkeypatch):
    monkeypatch.setenv("PAINEL_SENHA", "segredo-de-teste")
    from app.main import create_app
    app = create_app()
    app.config.update(TESTING=True)
    r = app.test_client().post("/painel/explorador/alterar",
                               data={"codigo": "501", "categoria_nova": "2.02"})
    assert r.status_code == 302
