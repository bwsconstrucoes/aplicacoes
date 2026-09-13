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
                " categoria, codigo_categoria, grupo, departamento, projeto,"
                " conta_corrente, razao_social,"
                " numero_documento, pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar','DRE','Pago','Serviços','1.01',"
                "         'Custo de obra',?,'ALFA','Conta 1','FORNECEDOR',"
                "         'NF 1',-100,0,0,0)", (codigo, dep))
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
# 2b. Uma alteração diferente por título — a edição na própria lista
# ===========================================================================
# A tela deixou de mandar "um valor para todos" e passou a mandar o destino de
# cada título. Duas linhas do mesmo título rateado não podem virar duas
# chamadas ao OMIE: lá é um cadastro só.

def test_cada_titulo_pode_ir_para_um_lugar_diferente(base_de_saneamento):
    """O que o dono pediu com todas as letras: editar linha a linha, e depois
    mandar tudo de uma vez."""
    from app.apps.painel import saneamento
    cliente = ClienteFalso()
    r = saneamento.aplicar(
        [{"codigo": 501, "categoria": "2.02"},
         {"codigo": 502, "categoria": "3.03"}],
        simulacao=True, cliente=cliente)
    assert r["ok"] is True
    por_codigo = {l["codigo"]: l for l in r["linhas"]}
    assert por_codigo[501]["categoria_pedida"] == "2.02"
    assert por_codigo[502]["categoria_pedida"] == "3.03"
    assert any("2.02" in m for m in por_codigo[501]["mudancas"])
    assert any("3.03" in m for m in por_codigo[502]["mudancas"])


def test_o_mesmo_titulo_repetido_vira_uma_chamada_so(base_de_saneamento, monkeypatch):
    """Um título rateado em três obras aparece em três linhas da tela. Se cada
    linha virasse um envio, o OMIE receberia o mesmo título três vezes."""
    from app.apps.painel import saneamento
    monkeypatch.setenv("PAINEL_SENHA_ESCRITA", "x")
    cliente = ClienteFalso()
    r = saneamento.aplicar(
        [{"codigo": 502, "categoria": "2.02"},
         {"codigo": 502, "categoria": "2.02"},
         {"codigo": 502, "categoria": "2.02"}],
        simulacao=False, cliente=cliente)
    assert r["quantos"] == 1
    assert len(cliente.enviados) == 1


def test_titulo_repetido_com_destinos_diferentes_vale_o_ultimo(base_de_saneamento):
    """Não dá para somar dois destinos contraditórios — e escolher em silêncio o
    primeiro esconderia do usuário a última coisa que ele clicou."""
    from app.apps.painel import saneamento
    alvos = saneamento.normalizar_alvos(
        [{"codigo": 501, "categoria": "2.02"},
         {"codigo": 501, "categoria": "9.99"}])
    assert alvos == {501: ("9.99", "")}


def test_titulo_sem_destino_nenhum_e_ignorado(base_de_saneamento):
    """Marcar uma linha e não escolher nada não pode virar uma chamada vazia."""
    from app.apps.painel import saneamento
    alvos = saneamento.normalizar_alvos(
        [{"codigo": 501, "categoria": "", "departamento": ""},
         {"codigo": 502, "departamento": "D9"}])
    assert alvos == {502: ("", "D9")}


def test_a_forma_antiga_continua_valendo(base_de_saneamento):
    """Sem JavaScript, a tela manda os marcados e UM destino para todos. É o
    caminho que funciona com o navegador travado — não pode morrer."""
    from app.apps.painel import saneamento
    alvos = saneamento.normalizar_alvos([501, 502], categoria_nova="2.02")
    assert alvos == {501: ("2.02", ""), 502: ("2.02", "")}


def test_a_trava_do_rateio_olha_o_destino_DAQUELE_titulo(base_de_saneamento):
    """A trava é por título: quem só troca a categoria passa, mesmo no mesmo
    envio em que outro título troca de obra."""
    from app.apps.painel import saneamento
    r = saneamento.aplicar(
        [{"codigo": 502, "categoria": "2.02"},          # só categoria: passa
         {"codigo": 501, "departamento": "D9"}],        # troca obra, sem rateio
        simulacao=True, cliente=ClienteFalso())
    por_codigo = {l["codigo"]: l for l in r["linhas"]}
    # o rateado passa PORQUE o destino dele não mexe em obra...
    assert not por_codigo[502]["resultado"].startswith("RECUSADO")
    assert any("2.02" in m for m in por_codigo[502]["mudancas"]), \
        "e a categoria dele tem de mudar de verdade, não sobrar sem destino"
    # ...enquanto o outro, no MESMO envio, troca de obra
    assert not por_codigo[501]["resultado"].startswith("RECUSADO")
    assert any("D9" in m for m in por_codigo[501]["mudancas"])


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


# ===========================================================================
# 4. A tela: UMA lista só, e é nela que se edita
# ===========================================================================
# O dono abriu a versão anterior e reprovou, em 13/09/2026: os filtros estavam
# no alto em vez de à esquerda, escolher mais de um item exigia saber segurar
# Ctrl, e havia DUAS listas — a de procurar e outra dentro do bloco de alterar.
# A frase dele: "basta uma lista e a gente vai trabalhar em cima dessa lista".

def test_a_tela_tem_uma_lista_so(cliente_web):
    """Duas tabelas de lançamentos era o defeito. A do resumo por categoria não
    conta: ela agrupa, não lista lançamento."""
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert html.count('id="lista-explorador"') == 1
    # a marca da segunda lista de antes: caixas `name="codigo"` numa tabela
    assert 'name="codigo"' not in html


def test_os_filtros_ficam_na_barra_da_esquerda(cliente_web):
    """Como em todas as outras telas do painel — e como o dono pediu."""
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert "sem-filtros" not in html, "a barra lateral não pode estar desligada"
    assert 'id="form-filtros"' in html


def test_cada_filtro_aceita_marcar_mais_de_um(cliente_web):
    """Antes era `<select multiple>`: dava para escolher vários, mas só quem
    sabia segurar Ctrl descobria. Agora é caixa de marcar, uma por item."""
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    for campo in ("analise", "grupo", "categoria", "obra", "projeto",
                  "conta", "situacao"):
        assert f'data-filtro="{campo}"' in html, campo
        assert f'type="checkbox" name="{campo}"' in html, campo
    # e nenhum deles pode ter voltado a ser a lista de rolagem de antes, onde
    # escolher dois itens exigia segurar Ctrl. (O "Tipo" segue sendo lista: são
    # três opções que se excluem, a pagar OU a receber.)
    assert "multiple" not in html, "filtro de marcar não pode virar select múltiplo"


def test_os_filtros_longos_tem_busca(cliente_web):
    """A lista de obras passa de cem. Sem busca, achar uma é rolar até topar."""
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert "Buscar obra…" in html
    assert "Buscar categoria…" in html


def test_dois_filtros_ao_mesmo_tempo_filtram_os_dois(cliente_web):
    """Marcar duas obras tem de trazer as duas — e só elas."""
    from app.apps.painel import consultas
    pedido = {"obras": ["CASA", "PREDIO"], "categorias": ["Serviços"],
              "analises": [], "grupos": [], "projetos": [], "contas": [],
              "situacoes": [], "tipo": "", "busca": "", "com_trf": False,
              "de": "", "ate": ""}
    linhas = consultas.explorar(pedido)["linhas"]
    assert linhas, "os dois filtros juntos não podem zerar a busca"
    assert {l["departamento"] for l in linhas} == {"CASA", "PREDIO"}


def test_a_lista_traz_o_codigo_do_titulo_em_cada_linha(cliente_web):
    """É o que amarra as linhas do mesmo título rateado: editar uma edita
    todas, porque no OMIE é um cadastro só."""
    html = cliente_web.get("/painel/explorador?busca=FORNECEDOR").get_data(as_text=True)
    assert 'data-codigo="501"' in html
    assert 'data-campo="categoria"' in html and 'data-campo="obra"' in html


def test_a_alteracao_chega_por_titulo_pela_tela(cliente_web, monkeypatch):
    """O caminho inteiro, da tela ao saneamento: três listas paralelas viram
    um destino por título."""
    from app.apps.painel import saneamento
    pedidos = []
    monkeypatch.setattr(saneamento, "aplicar",
                        lambda alvos, *a, **k: pedidos.append(alvos) or
                        {"ok": True, "simulacao": True, "linhas": [],
                         "quantos": 0, "alterados": 0, "recusados": 0})
    r = cliente_web.post("/painel/explorador/alterar?busca=FORNECEDOR", data={
        "alvo_codigo": ["501", "502"],
        "alvo_categoria": ["2.02", ""],
        "alvo_departamento": ["", "D9"],
        "executar": "0"})
    assert r.status_code == 200
    assert pedidos[0] == [{"codigo": "501", "categoria": "2.02", "departamento": ""},
                          {"codigo": "502", "categoria": "", "departamento": "D9"}]


# ===========================================================================
# 5. O Explorador não esconde nada
# ===========================================================================
# 13/09/2026: o dono procurou uma devolução de aporte de 24/12/2025, conciliada
# nessa data. Uma devolução do mesmo dia aparecia, essa não — e uma saída de
# transferência também não. A causa: a categoria delas está marcada como
# transferência no OMIE, e a tela escondia TRF por padrão. Numa tela cujo
# trabalho é ACHAR classificação errada, esconder uma classe inteira é o
# oposto do trabalho. "Aqui era pra aparecer todos os lançamentos igual como
# aparece no relatório de conta corrente do OMIE."

@pytest.fixture()
def base_com_transferencia(base_de_saneamento):
    from app.apps.painel import consultas, db as painel_db
    with painel_db.conexao() as conn:
        for codigo, analise, quem in ((7777, "TRF", "SOCIO FULANO"),
                                      (7778, "Fluxo de Caixa", "SOCIO BELTRANO")):
            conn.execute(
                "INSERT INTO fato (codigo_lancamento, tipo, analise, situacao, data,"
                " categoria, codigo_categoria, grupo, departamento, projeto,"
                " conta_corrente, razao_social, numero_documento,"
                " pago_recebido, a_pagar_receber, juros, multa)"
                " VALUES (?,'2. Contas a Pagar',?,'Pago','2025-12-24',"
                "         'Devolução de aporte','2.09','Aportes','CASA','ALFA',"
                "         'BRADESCO 123',?,'DEV 24-12',-50000,0,0,0)",
                (codigo, analise, quem))
        conn.commit()
    consultas.esquecer_listas()
    yield


def _pedido(**extra):
    base = {"analises": [], "grupos": [], "categorias": [], "obras": [],
            "projetos": [], "contas": [], "situacoes": [], "tipo": "",
            "busca": "", "com_trf": False, "de": "", "ate": ""}
    base.update(extra)
    return base


def test_transferencia_aparece_sem_precisar_marcar_nada(base_com_transferencia):
    """O caso exato do dono: dois lançamentos no mesmo dia, um deles em
    categoria de transferência. Os DOIS têm de aparecer."""
    from app.apps.painel import consultas
    linhas = consultas.explorar(_pedido(de="2025-12-24", ate="2025-12-24"))["linhas"]
    quem = {l["razao_social"] for l in linhas}
    assert {"SOCIO FULANO", "SOCIO BELTRANO"} <= quem, \
        "a tela de achar erro de classificação não pode esconder uma classe inteira"


def test_quem_quiser_cortar_por_analise_ainda_corta(base_com_transferencia):
    """Mostrar tudo por padrão não pode custar o filtro: marcar Análise = TRF
    continua trazendo só as transferências."""
    from app.apps.painel import consultas
    linhas = consultas.explorar(
        _pedido(de="2025-12-24", ate="2025-12-24", analises=["TRF"]))["linhas"]
    assert {l["razao_social"] for l in linhas} == {"SOCIO FULANO"}

    linhas = consultas.explorar(
        _pedido(de="2025-12-24", ate="2025-12-24",
                analises=["Fluxo de Caixa"]))["linhas"]
    assert {l["razao_social"] for l in linhas} == {"SOCIO BELTRANO"}


def test_as_telas_de_analise_continuam_tirando_a_transferencia(base_com_transferencia):
    """A mudança é SÓ do Explorador. No DRE e na Visão Geral a transferência
    tem de continuar fora: é dinheiro trocando de conta da própria empresa, e
    somá-la contaria o mesmo valor duas vezes."""
    from app.apps.painel import consultas
    filtro = consultas.Filtros(excluir_trf=True)
    onde, _ = filtro.where()
    assert "analise <> 'TRF'" in onde
