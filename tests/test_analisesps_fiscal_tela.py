# -*- coding: utf-8 -*-
"""Documentação Fiscal — a tela refeita em 13/09/2026, e o que a segura.

POR QUE ESTE ARQUIVO EXISTE. O dono usou a primeira versão da tela e listou
oito coisas erradas de uma vez. A frase que resume: *"não dá pra fazer a gestão
dessa documentação fiscal da forma que está"*. A maioria dos defeitos não era
de cálculo — era de a tela responder a pergunta ERRADA, e nenhum teste pega
isso sozinho. O que dá para travar, está travado aqui:

  - o filtro principal é o do trabalho fiscal, e não o de pagamento;
  - o bloco "Situação" (pendências, boleto duplicado) NÃO aparece aqui;
  - a barra vai na coluna do esqueleto, e não numa grade dentro do conteúdo
    (era isso que produzia a margem branca à esquerda);
  - as ações de trabalho fiscal moram na tela de trabalho, não em Configurações;
  - dá para escrever a documentação à mão, que era o buraco maior;
  - a conferência da chave recusa o que dá para recusar antes de gravar.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# O RECORTE — as duas listas não podem divergir
# ---------------------------------------------------------------------------
def test_as_categorias_que_exigem_nota_sao_as_MESMAS_da_conciliacao():
    """O SQL do filtro tem a lista escrita dentro dele; a conciliação tem a
    dela em Python. Se as duas divergirem, o total da tela conta uma coisa e a
    linha da lista diz outra — e ninguém tem como desconfiar."""
    from app.apps.analisesps import consultas, fiscal
    assert set(consultas.CATEGORIAS_QUE_EXIGEM_NOTA) == fiscal.EXIGEM_NOTA


def test_todo_recorte_do_filtro_tem_rotulo_em_portugues():
    """Recorte sem rótulo apareceria na tela com o nome técnico, ou não
    apareceria — e um filtro que existe e ninguém vê é um filtro que não
    existe."""
    from app.apps.analisesps import consultas
    rotulados = {chave for chave, _ in consultas.ROTULOS_FISCAIS}
    assert rotulados == set(consultas.SITUACOES_FISCAIS)


def test_o_recorte_fiscal_entra_no_mesmo_WHERE_dos_outros():
    """Uma montagem de WHERE só. Se o filtro da tela e a conta do painel
    fossem montados em dois lugares, o dia em que um ganhasse um recorte a
    mais o outro passaria a mentir em silêncio."""
    from app.apps.analisesps import consultas
    onde, _ = consultas._condicoes({"fiscais": ["sem_marcacao"]})
    assert any("sp_fiscal" in pedaco for pedaco in onde)


def test_recorte_desconhecido_e_IGNORADO_e_nao_vira_SQL():
    """Endereço com `fiscais=apaga_tudo` não pode virar comando."""
    from app.apps.analisesps import consultas
    onde, params = consultas._condicoes({"fiscais": ["DROP TABLE analisesps.sps"]})
    assert onde == [] and params == []


# ---------------------------------------------------------------------------
# ESCREVER À MÃO — o buraco que ele achou usando a tela
#
# *"Tudo aquilo que você sugeriu (…) mas o que você NÃO sugeriu, como é que eu
# adiciono a informação? Porque a planilha ela me permite adicionar, e a tela
# não permite."*
# ---------------------------------------------------------------------------
CHAVE_ACME = ("35" "2609" "11222333000181" "55" "001" "000000123"
              "1" "00000001" "0")


def test_a_chave_precisa_ter_44_numeros():
    from app.apps.analisesps import fiscal
    with pytest.raises(fiscal.ErroDeEntrada) as e:
        fiscal.conferir_chave("123456")
    assert "44" in str(e.value)


def test_a_chave_de_um_modelo_desconhecido_e_recusada():
    """Posições 21 e 22 dizem o modelo. Um número trocado ali faz a chave
    parecer válida e apontar para documento que não existe."""
    from app.apps.analisesps import fiscal
    quebrada = CHAVE_ACME[:20] + "99" + CHAVE_ACME[22:]
    with pytest.raises(fiscal.ErroDeEntrada) as e:
        fiscal.conferir_chave(quebrada)
    assert "NF-e" in str(e.value)


def test_a_chave_de_OUTRO_CNPJ_e_recusada_com_o_motivo():
    """É o defeito que esta tela existe para achar — nota de um lançamento
    colada em outro. Recusar na digitação é mais barato que descobrir no card."""
    from app.apps.analisesps import fiscal
    with pytest.raises(fiscal.ErroDeEntrada) as e:
        fiscal.conferir_chave(CHAVE_ACME, {"documento": "99.888.777/0001-99"})
    assert "outro CNPJ" in str(e.value)


def test_a_chave_do_PROPRIO_credor_passa():
    from app.apps.analisesps import fiscal
    assert fiscal.conferir_chave(
        CHAVE_ACME, {"documento": "11.222.333/0001-81"}) == CHAVE_ACME


def test_a_chave_com_pontuacao_e_aceita():
    """Quem copia do PDF traz espaço no meio. Recusar por isso seria só
    atrapalhar."""
    from app.apps.analisesps import fiscal
    com_espaco = " ".join(CHAVE_ACME[i:i + 4] for i in range(0, 44, 4))
    assert fiscal.conferir_chave(com_espaco) == CHAVE_ACME


def test_sem_categoria_e_sem_chave_a_gravacao_e_recusada(monkeypatch):
    """Gravar "nada" apagaria o que já estava lá sem a pessoa pedir."""
    from app.apps.analisesps import fiscal
    monkeypatch.setattr(fiscal, "guardar_decisao",
                        lambda *a, **k: pytest.fail("não devia gravar"))
    with pytest.raises(fiscal.ErroDeEntrada):
        fiscal.decidir_a_mao("1", "", "", "eu")


def test_categoria_inventada_e_recusada(monkeypatch):
    from app.apps.analisesps import fiscal
    monkeypatch.setattr(fiscal, "guardar_decisao",
                        lambda *a, **k: pytest.fail("não devia gravar"))
    with pytest.raises(fiscal.ErroDeEntrada):
        fiscal.decidir_a_mao("1", "Qualquer Coisa", "", "eu")


def test_so_a_chave_JA_BASTA_e_a_categoria_sai_de_dentro_dela(monkeypatch):
    """O modelo do documento está na própria chave, então não é palpite — é o
    mesmo caminho da proposta automática."""
    from app.apps.analisesps import fiscal
    gravado = {}
    monkeypatch.setattr(
        fiscal, "guardar_decisao",
        lambda sp_id, doc, chave, motivo, conf, quem, origem="PESSOA":
            gravado.update(sp=sp_id, doc=doc, chave=chave, origem=origem))

    saida = fiscal.decidir_a_mao("77", "", CHAVE_ACME, "Marcelo")
    assert saida["documentacao"] == "NF-e (Mercadoria)"
    assert gravado["chave"] == CHAVE_ACME
    assert gravado["origem"] == "PESSOA"


def test_um_CTe_digitado_a_mao_vira_CTe_e_nao_NFe(monkeypatch):
    from app.apps.analisesps import fiscal
    monkeypatch.setattr(fiscal, "guardar_decisao", lambda *a, **k: None)
    cte = CHAVE_ACME[:20] + "57" + CHAVE_ACME[22:]
    assert fiscal.decidir_a_mao("77", "", cte, "eu")["documentacao"] \
        == "CT-e (Frete)"


def test_a_categoria_escolhida_MANDA_sobre_a_da_chave(monkeypatch):
    """Quem digitou está olhando o documento; o sistema, não. A chave continua
    sendo gravada — as duas informações convivem."""
    from app.apps.analisesps import fiscal
    monkeypatch.setattr(fiscal, "guardar_decisao", lambda *a, **k: None)
    saida = fiscal.decidir_a_mao("77", "Nota de Débito/Fatura", CHAVE_ACME, "eu")
    assert saida["documentacao"] == "Nota de Débito/Fatura"
    assert saida["chave"] == CHAVE_ACME


# ---------------------------------------------------------------------------
# ONDE CADA BOTÃO MORA
# ---------------------------------------------------------------------------
def test_o_trabalho_fiscal_NAO_aparece_mais_em_configuracoes():
    """*"Ao buscar na Receita as notas emitidas contra a BWS, não tem
    absolutamente nada a ver eu estar com um botão desse fora da tela de
    trabalho."*

    A causa do engano vale o teste: a tela de Configurações desenhava a lista
    INTEIRA de modos como botões, então um modo novo ganhava um botão lá sem
    querer."""
    from app.apps.analisesps import tarefas
    for modo in ("notas_receita", "fiscal", "fiscal_ia"):
        assert modo not in tarefas.MODOS_DA_BASE


def test_toda_acao_da_tela_fiscal_e_um_modo_QUE_EXISTE():
    """Botão que dispara um modo inexistente falharia só no clique."""
    from app.apps.analisesps import tarefas, web
    for acao in web.ACOES_FISCAIS:
        assert acao["modo"] in tarefas.MODOS


def test_as_duas_fontes_de_nota_continuam_na_tela():
    """Pergunta do dono: *"eu não vou poder importar o relatório do FSist
    mais?"* Vai — a Receita só devolve o recente, e o histórico entra pelo
    relatório. As duas precisam estar ao alcance de quem trabalha."""
    from app.apps.analisesps import web
    modos = [a["modo"] for a in web.ACOES_FISCAIS]
    assert "notas_receita" in modos and "apoios" in modos


def test_todo_modo_da_base_e_conhecido():
    from app.apps.analisesps import tarefas
    for modo in tarefas.MODOS_DA_BASE:
        assert modo in tarefas.MODOS


# ---------------------------------------------------------------------------
# A ESTRUTURA DA TELA — o que produziu a margem branca
# ---------------------------------------------------------------------------
def _template(nome):
    import pathlib
    from app.apps.analisesps import db
    caminho = (pathlib.Path(db.__file__).parent / "templates" / nome)
    return caminho.read_text(encoding="utf-8")


def test_a_tela_fiscal_usa_a_COLUNA_de_filtros_do_esqueleto():
    """*"O filtro ficou deslocado pra direita, ficou uma margem branca do lado
    esquerdo."*

    A causa: a tela montava uma SEGUNDA grade dentro da área de conteúdo, e a
    coluna de filtros do esqueleto ficava vazia — 288px de branco antes da
    barra. Quem usa o bloco certo não tem como reproduzir isso."""
    texto = _template("analisesps_fiscal.html")
    assert "{% block filtros %}" in texto
    assert '<div class="corpo">' not in texto


def test_a_visao_por_nota_DISPENSA_a_coluna_em_vez_de_deixar_vazia():
    """Os recortes da barra são do lançamento; aqui a linha é a nota. Deixar a
    coluna vazia daria a mesma margem branca."""
    texto = _template("analisesps_fiscal_notas.html")
    assert "sem-filtros" in texto


def test_a_barra_fiscal_NAO_traz_o_bloco_de_pagamento():
    """*"Esse primeiro filtro aqui que fica aberto, a situação, pendência,
    risco, isso aqui já tira logo. Esse aqui já não tem absolutamente nada a
    ver com isso aqui."*"""
    texto = _template("analisesps_filtros_fiscal.html")
    assert 'name="situacoes"' not in texto
    assert "boleto_duplicado" not in texto


def test_a_barra_fiscal_abre_NO_recorte_do_trabalho():
    """O filtro principal da tela é o que ela serve para responder."""
    texto = _template("analisesps_filtros_fiscal.html")
    assert 'name="fiscais"' in texto
    assert texto.index("O que fazer") < texto.index("Tipo de despesa")


def test_a_ordenacao_continua_na_barra():
    """Ele olhou e disse: *"olha esse filtro aqui de ordenar. Não, pode
    deixar."*"""
    assert 'name="ordem"' in _template("analisesps_filtros_fiscal.html")


# ---------------------------------------------------------------------------
# A TELA MONTA, E MONTA COM O QUE ELE PEDIU
#
# Erro em template não aparece em teste de regra: ele espera alguém abrir a
# página. Aqui as consultas são dubladas — o que está sob teste é a TELA.
# ---------------------------------------------------------------------------
import datetime as dt          # noqa: E402
from decimal import Decimal    # noqa: E402

from flask import Flask        # noqa: E402

SENHA_OPERADOR = "operador-de-teste"
SENHA_CONSULTA = "consulta-de-teste"


@pytest.fixture
def app_fiscal(monkeypatch):
    from app.apps.analisesps import consultas, fiscal, preferencias, tarefas, web

    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)

    sp = {"id": "1443253428", "credor": "ACME MATERIAIS LTDA",
          "documento": "11.222.333/0001-81", "valor_num": Decimal("269.00"),
          "vencimento_d": dt.date(2026, 9, 10), "status_pgt": "Pago",
          "tipo_despesa": "Material", "anexo_link": "http://anexo",
          "card_link": "http://card", "nf": "123"}

    monkeypatch.setattr(consultas, "base_carregada",
                        lambda: {"pronta": True, "quantidade": 59055,
                                 "ultima": "2026-09-02T18:00:00"})
    monkeypatch.setattr(consultas, "listar", lambda f, **k: [sp])
    monkeypatch.setattr(consultas, "resumo", lambda f: {
        "quantidade": 1, "total": Decimal("269.00"),
        "quantidade_pagar": 0, "total_pagar": Decimal("0")})
    monkeypatch.setattr(consultas, "opcoes_de_filtro", lambda carimbo=None: {
        "status_pgt": ["Pagar", "Pago"], "conta": [], "forma": [],
        "tipo_despesa": ["Material"], "projeto": [], "responsavel": [],
        "centro_custo": ["OBRA-12"], "status_agend": []})
    monkeypatch.setattr(consultas, "painel_fiscal", lambda f: {
        "total": 59055, "sem_marcacao": 1200, "ja_marcado": 57855,
        "provavel_erro": 37, "com_chave": 400, "na_fila_ia": 5,
        "confirmada": 9, "escrita": 800, "sem_anexo": 60,
        "valor_sem_marcacao": Decimal("4500000.00")})
    monkeypatch.setattr(fiscal, "painel_notas", lambda: {
        "total": 900, "canceladas": 11, "sem_lancamento": 42, "ctes": 30,
        "ultima": None})
    monkeypatch.setattr(fiscal, "conciliar", lambda linhas: [{
        "sp": sp, "analise": {}, "nota": None, "segunda": None, "porques": [],
        "grupo": "SEM_PAR", "propoe": False, "documentacao": "", "chave": "",
        "motivo": "procurei e não encontrei nota que combine.",
        "confianca": 0}])
    monkeypatch.setattr(fiscal, "notas_orfas", lambda pagina=1: ([{
        "chave": CHAVE_ACME, "emissao": dt.date(2026, 9, 1), "numero": "123",
        "valor": Decimal("269.00"), "status": "Autorizada",
        "emitente_doc": "11222333000181", "emitente": "ACME",
        "categoria": "NF-e (Mercadoria)"}], 42))
    monkeypatch.setattr(fiscal, "sps_possiveis_da_nota", lambda nota, **k: [sp])
    monkeypatch.setattr(tarefas, "estado",
                        lambda: {"rodando": False, "detalhe": None,
                                 "interrompida": None})
    monkeypatch.setattr(preferencias, "ler", lambda pessoa, chave: {})
    monkeypatch.setattr(preferencias, "gravar", lambda pessoa, chave, valor: None)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def entrar(app, senha=SENHA_OPERADOR):
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": senha, "nome": "MARCELO"})
    return cliente


def test_a_tela_fiscal_monta(app_fiscal):
    r = entrar(app_fiscal).get("/analisesps/fiscal?f=1")
    assert r.status_code == 200
    assert "ACME MATERIAIS" in r.get_data(as_text=True)


def test_os_totalizadores_aparecem_com_os_numeros_em_portugues(app_fiscal):
    """*"Onde é que eu vejo aqui como é que está a situação, uma espécie de
    totalizadores, pra saber o que que está faltando (…) onde é que eu tenho
    que focar?"* Ponto no milhar, senão o número não se lê."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "Sem documentação" in html
    assert "1.200" in html
    assert "Provavelmente errado" in html and "37" in html


def test_cada_totalizador_e_um_ATALHO_para_o_proprio_recorte(app_fiscal):
    """Ver um número e ter de ir procurá-lo no filtro seria meio caminho."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "fiscais=sem_marcacao" in html
    assert "fiscais=provavel_erro" in html


def test_os_numeros_do_alto_sao_da_BASE_e_os_das_etiquetas_sao_DA_PAGINA(app_fiscal):
    """Os dois convivem e dizem coisas diferentes. Sem o aviso "nesta página",
    quem lesse acharia que discordam entre si."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "nesta página" in html


def test_as_acoes_de_trabalho_fiscal_estao_NA_TELA(app_fiscal):
    """*"Ler, é pra estar dentro da tela. Gravar nos cards, é pra estar dentro
    da tela."*"""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert 'data-modo="notas_receita"' in html
    assert 'data-modo="fiscal_ia"' in html
    assert 'data-modo="fiscal"' in html
    assert 'data-modo="apoios"' in html


def test_quem_so_CONSULTA_nao_ve_os_botoes_que_disparam_trabalho(app_fiscal):
    html = entrar(app_fiscal, SENHA_CONSULTA).get(
        "/analisesps/fiscal?f=1").get_data(as_text=True)
    assert 'data-modo="notas_receita"' not in html
    assert "fiscal-mao" not in html


def test_toda_linha_tem_como_ESCREVER_A_MAO(app_fiscal):
    """O buraco maior: a tela só sabia aprovar proposta, e o trabalho que sobra
    é justamente o que não tem proposta nenhuma."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "fiscal-mao" in html
    assert 'id="dlg-mao"' in html


def test_as_duas_visoes_aparecem_das_duas_telas(app_fiscal):
    """*"Eu preciso de duas visões."* E uma tem de levar à outra."""
    cliente = entrar(app_fiscal)
    lancamento = cliente.get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "Por lançamento" in lancamento and "Por nota" in lancamento

    nota = cliente.get("/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "Por lançamento" in nota and "Por nota" in nota


def test_a_visao_por_nota_monta_e_deixa_ASSOCIAR(app_fiscal):
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "Notas sem lançamento" in html
    assert "fiscal-associar" in html
    assert "42" in html          # o total de órfãs, do painel


def test_mexer_no_filtro_NAO_joga_de_volta_para_a_outra_visao(app_fiscal):
    """A barra manda a visão junto; sem isso, marcar uma caixa do lado da nota
    devolveria a pessoa para o lado do lançamento."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "visao=notas" in html


# ---------------------------------------------------------------------------
# O FILTRO QUE SUMIA
#
# *"Eu saio e volto e o filtro que eu estou trabalhando eles somem. Eu vou pra
# configurações pra fazer alguma coisa, aí volto pra cá e o filtro some."*
# ---------------------------------------------------------------------------
def test_o_filtro_da_tela_fiscal_VOLTA_como_foi_deixado(app_fiscal, monkeypatch):
    from app.apps.analisesps import preferencias

    gaveta = {}
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: gaveta.__setitem__(chave, valor))
    monkeypatch.setattr(preferencias, "ler",
                        lambda pessoa, chave: gaveta.get(chave, {}))

    cliente = entrar(app_fiscal)
    cliente.get("/analisesps/fiscal?f=1&fiscais=provavel_erro")
    assert gaveta[preferencias.FILTRO_FISCAL] == {"fiscais": ["provavel_erro"]}

    # Voltar pelo menu, sem nada na barra de endereço, traz o filtro de volta.
    resposta = cliente.get("/analisesps/fiscal")
    assert resposta.status_code == 302
    assert "fiscais=provavel_erro" in resposta.headers["Location"]


def test_o_filtro_fiscal_NAO_atropela_o_de_solicitacoes(app_fiscal, monkeypatch):
    """Gavetas separadas: são perguntas diferentes, e quem trabalha nas duas
    telas no mesmo dia perderia o recorte toda vez."""
    from app.apps.analisesps import preferencias

    gaveta = {}
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: gaveta.__setitem__(chave, valor))
    monkeypatch.setattr(preferencias, "ler",
                        lambda pessoa, chave: gaveta.get(chave, {}))

    cliente = entrar(app_fiscal)
    cliente.get("/analisesps/fiscal?f=1&fiscais=sem_marcacao")
    assert preferencias.FILTRO not in gaveta
    assert preferencias.FILTRO_FISCAL in gaveta


# ---------------------------------------------------------------------------
# A GRAVAÇÃO À MÃO, pela rota
# ---------------------------------------------------------------------------
def test_quem_so_consulta_NAO_grava_a_mao(app_fiscal):
    r = entrar(app_fiscal, SENHA_CONSULTA).post(
        "/analisesps/api/fiscal/mao",
        json={"sp": "1443253428", "chave": CHAVE_ACME})
    assert r.status_code in (302, 403)


def test_a_recusa_por_dado_errado_NAO_vira_erro_de_sistema(app_fiscal, monkeypatch):
    """Recusa esperada responde 400 com a frase para a pessoa ler. Devolver
    500 "deu erro" faria ela procurar defeito onde não há."""
    from app.apps.analisesps import consultas

    monkeypatch.setattr(consultas, "uma",
                        lambda sp_id: {"id": sp_id,
                                       "documento": "99.888.777/0001-99"})
    r = entrar(app_fiscal).post("/analisesps/api/fiscal/mao",
                                json={"sp": "1", "chave": CHAVE_ACME})
    assert r.status_code == 400
    assert "outro CNPJ" in r.get_json()["erro"]


def test_gravar_a_mao_passa_pelo_MESMO_diario_da_decisao_aprovada(
        app_fiscal, monkeypatch):
    """Um segundo caminho de gravação seria a chance de a tela e o card
    divergirem."""
    from app.apps.analisesps import consultas, fiscal

    monkeypatch.setattr(consultas, "uma",
                        lambda sp_id: {"id": sp_id,
                                       "documento": "11.222.333/0001-81"})
    gravado = {}
    monkeypatch.setattr(
        fiscal, "guardar_decisao",
        lambda sp_id, doc, chave, motivo, conf, quem, origem="PESSOA":
            gravado.update(sp=sp_id, doc=doc, quem=quem, origem=origem))

    r = entrar(app_fiscal).post("/analisesps/api/fiscal/mao",
                                json={"sp": "1", "chave": CHAVE_ACME})
    assert r.status_code == 200 and r.get_json()["ok"]
    assert gravado["doc"] == "NF-e (Mercadoria)"
    assert gravado["quem"] == "MARCELO"
    assert gravado["origem"] == "PESSOA"
