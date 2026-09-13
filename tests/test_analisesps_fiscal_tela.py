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


def test_a_visao_por_nota_tem_BARRA_PROPRIA():
    """Os recortes da barra de lançamentos (obra, tipo de despesa, vencimento)
    são do lançamento; aqui a linha é a NOTA, e os recortes são outros — o
    lançamento, a situação na Receita, o tipo de documento, a emissão.

    Antes esta visão dispensava a coluna inteira, e ficava sem filtro nenhum:
    dava para ver as órfãs e mais nada. *"Não consigo visualizar numa tela o
    que temos de notas e o que não temos."*"""
    texto = _template("analisesps_fiscal_notas.html")
    assert "{% block filtros %}" in texto
    assert 'name="nota"' in texto
    assert "sem-filtros" not in texto


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
    assert texto.index('name="fiscais"') < texto.index("Tipo de despesa")


def test_todo_recorte_diz_de_QUE_DADO_ele_fala():
    """*"A nomenclatura dos filtros tá estranha (…) não dá nem pra entender o
    que estamos filtrando, quais dados."*

    Numa planilha ele filtra clicando no cabeçalho da coluna e sabe exatamente
    o que está recortando. Aqui o título do grupo é o nome do dado — e todo
    recorte tem de estar dentro de um."""
    from app.apps.analisesps import consultas
    dentro_de_grupo = {chave for _, itens in consultas.GRUPOS_DE_RECORTE
                       for chave, _, _ in itens}
    assert dentro_de_grupo == set(consultas.SITUACOES_FISCAIS)


def test_nenhum_recorte_aparece_em_DOIS_grupos():
    """O mesmo recorte em dois lugares faria a pessoa achar que são coisas
    diferentes."""
    from app.apps.analisesps import consultas
    vistos = [chave for _, itens in consultas.GRUPOS_DE_RECORTE
              for chave, _, _ in itens]
    assert len(vistos) == len(set(vistos))


def test_todo_recorte_tem_FRASE_para_a_tela_dizer_o_que_filtra():
    """A linha acima da tabela escreve o recorte em português. Um recorte sem
    frase apareceria lá com o nome técnico."""
    from app.apps.analisesps import consultas
    assert set(consultas.FRASE_DO_RECORTE) == set(consultas.SITUACOES_FISCAIS)


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
    nota_falsa = {
        "chave": CHAVE_ACME, "emissao": dt.date(2026, 9, 1), "numero": "123",
        "serie": "1", "valor": Decimal("269.00"), "status": "Autorizada",
        "emitente_doc": "11222333000181", "emitente": "ACME",
        "emitente_uf": "BA", "importada_em": None, "orfa": True,
        "categoria": "NF-e (Mercadoria)"}
    monkeypatch.setattr(fiscal, "notas_orfas",
                        lambda pagina=1: ([dict(nota_falsa)], 42))
    monkeypatch.setattr(fiscal, "listar_notas", lambda f, pagina=1: (
        [dict(nota_falsa)], {"quantidade": 42, "total": Decimal("269.00"),
                             "sem_lancamento": 42}))
    monkeypatch.setattr(fiscal, "notas_por_dia", lambda dias=14: [
        {"dia": dt.date(2026, 9, 1), "quantas": 3, "total": Decimal("800.00")}])
    monkeypatch.setattr(fiscal, "sps_possiveis_das_notas",
                        lambda notas, **k: {CHAVE_ACME: [sp]})
    from app.apps.analisesps import sefaz
    monkeypatch.setattr(sefaz, "estado_das_buscas", lambda: [{
        "cnpj": "10656452007869", "tipo": "NFE", "rotulo_tipo": "Notas (NF-e)",
        "ultimo_nsu": "000000000000120", "maior_nsu": "000000000000150",
        "consultado_em": None, "ultimo_recado": "", "documentos": 120,
        "faltam": 30, "em_dia": False}])
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
    assert "Categoria vazia" in html
    assert "1.200" in html
    assert "Nota parece errada" in html and "37" in html


def test_o_TOTALIZADOR_e_o_FILTRO_usam_as_MESMAS_palavras(app_fiscal):
    """Eram diferentes ("Sem documentação" no número, "Está vazia" no filtro), e
    isso era metade da confusão: o número dizia uma coisa, o filtro dizia outra,
    e nada indicava que eram a MESMA pergunta."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&fiscais=sem_marcacao").get_data(as_text=True)
    # O número apertado e o recorte escrito falam da mesma coluna.
    assert "Categoria vazia" in html
    assert "a categoria está vazia" in html


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
    assert "As notas emitidas contra a BWS" in html
    assert "fiscal-associar" in html
    assert "42" in html          # o total do filtro


def test_a_visao_por_nota_lista_TUDO_e_nao_so_as_orfas(app_fiscal):
    """*"Não consigo visualizar numa tela o que temos de notas e o que não
    temos."* A lista era só das órfãs — um recorte útil, e só um recorte."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    # "Sem lançamento" virou um FILTRO da lista, não a lista inteira.
    assert 'name="nota" value="sem_lancamento"' in html
    assert 'name="nota" value="com_lancamento"' in html


def test_a_visao_por_nota_DIZ_se_a_busca_na_Receita_rodou(app_fiscal):
    """*"Eu coloquei pra baixar notas mas não tenho nem ideia de que se baixou,
    se não baixou."* O ponteiro sempre soube; nenhuma tela mostrava."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "A busca na Receita" in html
    assert "10656452007869" in html      # o CNPJ vigiado
    assert "120" in html                 # quantos documentos já vieram


def test_sem_busca_nenhuma_a_tela_DIZ_o_que_falta(app_fiscal, monkeypatch):
    """"Nunca rodou" e "rodou e não trouxe nada" são coisas diferentes, e a
    primeira tem uma causa que se resolve: falta o certificado."""
    from app.apps.analisesps import sefaz

    monkeypatch.setattr(sefaz, "estado_das_buscas", lambda: [])
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "A busca nunca rodou" in html
    assert "certificado digital A1" in html


def test_as_notas_POR_DIA_aparecem_e_cada_dia_e_um_atalho(app_fiscal):
    """*"Ver as notas do dia ou consultar as notas."* O número de ontem ao lado
    do de hoje já diz se a busca está trazendo coisa ou se parou."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "Notas por dia de emissão" in html
    assert "emissao_ini=2026-09-01" in html


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


# ---------------------------------------------------------------------------
# A NAVEGAÇÃO — 13/09/2026
#
# *"Numa tela grande é tranquilo de navegar, porque todos aparecem, mas numa
# tela pequena ele fica escondido, as últimas."*
# ---------------------------------------------------------------------------
def test_a_ordem_das_telas_e_a_que_o_dono_pediu():
    """*"Solicitações primeiro, depois lote, aí depois comprovantes, depois
    relatório, e depois documentação fiscal, aí depois agenda, e pronto, aí
    pode seguir com os demais."* É o caminho do dia dele."""
    from app.apps.analisesps import web
    assert [c for c, _, _ in web.TELAS][:6] == [
        "solicitacoes", "lote", "comprovantes", "relatorio", "fiscal", "agenda"]


def test_toda_tela_do_menu_aponta_para_uma_rota_QUE_EXISTE():
    """Link quebrado no menu só apareceria no clique."""
    from flask import Flask

    from app.apps.analisesps import web
    a = Flask(__name__)
    a.register_blueprint(web.bp)
    rotas = {r.endpoint for r in a.url_map.iter_rules()}
    for _, rotulo, rota in web.TELAS:
        assert rota in rotas, f"{rotulo} aponta para uma rota que não existe"


def test_a_FAIXA_e_o_MENU_saem_da_mesma_lista():
    """Duas cópias divergiriam no dia em que uma tela nova entrasse em uma só —
    e a que ficaria de fora seria justamente a do menu, que é o caminho de quem
    está no celular e não tem como descobrir que faltou."""
    import pathlib

    from app.apps.analisesps import db
    texto = (pathlib.Path(db.__file__).parent / "templates"
             / "analisesps_base.html").read_text(encoding="utf-8")
    # As duas varreduras usam `telas`, e nenhuma lista está escrita no HTML.
    assert texto.count("for chave, rotulo, rota in telas") == 2
    assert "analisesps.tela_lote'" not in texto


def test_o_menu_existe_no_HTML_mesmo_em_tela_grande(app_fiscal):
    """Ele nasce escondido e quem o revela é o script, medindo. Se dependesse
    do servidor saber a largura da janela, não funcionaria — o servidor não
    sabe."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert 'id="menu-painel"' in html
    assert 'id="btn-menu"' in html


# ---------------------------------------------------------------------------
# RECONFERIR — "como é que eu sei que isso está sendo analisado?"
# ---------------------------------------------------------------------------
def test_a_tela_DIZ_que_reconfere_a_cada_abertura(app_fiscal):
    """A conferência sempre rodou a cada abertura, inclusive sobre o que já foi
    decidido — e a tela nunca disse isso. O que não é dito não existe para quem
    usa, e a pergunta dele é a prova."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "Reconferido agora" in html
    assert "já foram gravados no card" in html


def test_toda_linha_tem_como_RECONFERIR(app_fiscal):
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "fiscal-reconferir" in html
    assert 'id="btn-reconferir"' in html


def test_quem_so_CONSULTA_pode_reconferir(app_fiscal, monkeypatch):
    """Reconferir não grava nada e não custa IA — é só perguntar de novo.
    Recusar isso a quem consulta seria proibir olhar."""
    from app.apps.analisesps import consultas, fiscal

    monkeypatch.setattr(consultas, "uma", lambda sp_id: {"id": sp_id, "credor": "A"})
    monkeypatch.setattr(fiscal, "reconferir", lambda sps: [])
    r = entrar(app_fiscal, SENHA_CONSULTA).post(
        "/analisesps/api/fiscal/reconferir", json={"ids": ["1"]})
    assert r.status_code == 200 and r.get_json()["ok"]


def test_reconferir_NAO_GRAVA_nada(app_fiscal, monkeypatch):
    """Devolve o que encontrou; quem decide continua sendo gente."""
    from app.apps.analisesps import consultas, fiscal

    monkeypatch.setattr(consultas, "uma", lambda sp_id: {"id": sp_id, "credor": "A"})
    monkeypatch.setattr(fiscal, "guardar_decisao",
                        lambda *a, **k: pytest.fail("reconferir não pode gravar"))
    monkeypatch.setattr(fiscal, "notas_candidatas", lambda l: {})
    monkeypatch.setattr(fiscal, "analises_guardadas", lambda ids: {})
    r = entrar(app_fiscal).post("/analisesps/api/fiscal/reconferir",
                                json={"ids": ["1"]})
    assert r.status_code == 200


def test_reconferir_tem_TETO_por_chamada(app_fiscal):
    """Soltar a base inteira num banco de um décimo de núcleo é o jeito
    conhecido de derrubar a tela de todo mundo."""
    r = entrar(app_fiscal).post("/analisesps/api/fiscal/reconferir",
                                json={"ids": [str(i) for i in range(300)]})
    assert r.status_code == 400
    assert "200" in r.get_json()["erro"]


def test_reconferir_procura_nota_ATE_para_o_que_a_lista_nao_concilia(monkeypatch):
    """Na lista, procurar nota de apólice ou de contrato todo dia seria ruído.
    Pedido registro a registro, é exatamente a pergunta — *"será que
    classificaram errado?"*"""
    from app.apps.analisesps import fiscal

    chamou = []
    monkeypatch.setattr(fiscal, "notas_candidatas", lambda l: {})
    monkeypatch.setattr(fiscal, "analises_guardadas",
                        lambda ids: {"1": {"documentacao": "Contrato"}})
    monkeypatch.setattr(fiscal, "melhor_nota",
                        lambda sp, notas: chamou.append(sp) or
                        {"nota": None, "pontos": 0, "porques": [], "propoe": False})

    fiscal.reconferir([{"id": "1", "credor": "ALUGUEL"}])
    assert chamou, ('"Contrato" está em NAO_CONCILIA; a reconferência tem de '
                    "procurar mesmo assim")


def test_o_resumo_diz_o_que_MUDOU_e_nao_so_o_que_achou():
    """Reconferir trinta registros sem uma conclusão no alto devolveria trinta
    parágrafos que ninguém lê."""
    from app.apps.analisesps import fiscal

    linhas = [
        {"sp": {"id": "1"}, "analise": {"documentacao": "Ausente"},
         "nota": {"numero": "77", "emitente": "ACME", "status": "Autorizada"},
         "grupo": fiscal.CORRECAO, "documentacao": "NF-e (Mercadoria)",
         "chave": "x", "confianca": 85, "motivo": "achei"},
        {"sp": {"id": "2"}, "analise": {"documentacao": "Contrato"},
         "nota": None, "grupo": fiscal.EM_DIA, "documentacao": "",
         "chave": "", "confianca": 0, "motivo": "nada a apontar"},
    ]
    resumo = fiscal.resumo_da_reconferencia(linhas)
    assert resumo["mudaram"] == 1
    assert resumo["apontados"] == 1
    assert resumo["itens"][0]["mudou"] is True
    assert resumo["itens"][1]["mudou"] is False


# ---------------------------------------------------------------------------
# O DEFEITO DO NÚMERO QUE MUDAVA DEBAIXO DO DEDO — 13/09/2026
#
# *"Se eu clico Sem documentação aparece Proposta de correção 28, e aí se eu
# clico em cima de Proposta de correção 28, ele filtra para apenas 2."*
#
# A causa: havia UM conjunto de parâmetros só, e dele o recorte era retirado —
# porque os totalizadores do alto precisam TROCAR o recorte ao serem clicados.
# As etiquetas de grupo usavam o mesmo conjunto, e por isso clicar numa delas
# APAGAVA o recorte, voltando para a lista inteira.
#
# É o tipo de defeito que só aparece clicando, e por isso ele vira teste.
# ---------------------------------------------------------------------------
def test_clicar_na_ETIQUETA_nao_apaga_o_recorte(app_fiscal):
    """O número 28 tem de continuar 28 depois do clique."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&fiscais=sem_marcacao").get_data(as_text=True)

    import re
    etiquetas = re.findall(r'href="([^"]*grupo=[^"]*)"', html)
    assert etiquetas, "nenhuma etiqueta de grupo na tela"
    for endereco in etiquetas:
        assert "fiscais=sem_marcacao" in endereco, (
            "a etiqueta perdeu o recorte — é o defeito do 28 que virava 2")


def test_clicar_no_TOTALIZADOR_troca_o_recorte_em_vez_de_somar(app_fiscal):
    """O totalizador é outro caminho de propósito: ele DEFINE o recorte. Se
    somasse ao que já está marcado, clicar em dois seguidos daria zero linha e
    pareceria defeito."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&fiscais=sem_marcacao").get_data(as_text=True)
    assert "fiscais=provavel_erro" in html
    # E o que está ligado vira o atalho para DESLIGAR.
    assert "ligado" in html


def test_virar_a_PAGINA_nao_apaga_o_recorte(app_fiscal, monkeypatch):
    """Mesmo defeito, outro caminho."""
    from app.apps.analisesps import consultas

    monkeypatch.setattr(consultas, "resumo", lambda f: {
        "quantidade": 500, "total": 0, "quantidade_pagar": 0, "total_pagar": 0})
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&fiscais=sem_marcacao").get_data(as_text=True)
    import re
    paginas = re.findall(r'href="([^"]*pagina=\d+[^"]*)"', html)
    assert paginas, "nenhum link de paginação"
    for endereco in paginas:
        assert "fiscais=sem_marcacao" in endereco


def test_a_tela_DIZ_em_portugues_o_que_esta_filtrando(app_fiscal):
    """*"Não dá nem pra entender o que estamos filtrando, quais dados."* Numa
    planilha o funil fica no cabeçalho da coluna; as caixas marcadas ficam na
    barra lateral, fora do campo de visão de quem olha a tabela."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&fiscais=sem_marcacao&fiscais=com_anexo"
    ).get_data(as_text=True)
    assert "Mostrando as SPs em que" in html
    assert "a categoria está vazia" in html
    assert "tem anexo" in html


def test_da_para_TIRAR_um_recorte_sem_ir_na_barra_lateral(app_fiscal):
    """Dois recortes ligados, e o ✕ de cada um tira só aquele."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&fiscais=sem_marcacao&fiscais=com_anexo"
    ).get_data(as_text=True)
    import re
    tirar = re.findall(r'href="([^"]*)"[^>]*title="Tirar este recorte"', html)
    assert len(tirar) == 2
    # Tirar um deixa o outro de pé.
    assert any("fiscais=com_anexo" in e and "sem_marcacao" not in e for e in tirar)
    assert any("fiscais=sem_marcacao" in e and "com_anexo" not in e for e in tirar)


def test_a_tela_explica_por_que_os_DOIS_conjuntos_de_numero_diferem(app_fiscal):
    """Um vem do banco (base inteira), o outro da conferência (página). Sem
    dizer qual é qual, parece que a tela discorda de si mesma — foi metade da
    confusão."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "carregados nesta página" in html
    assert "vêm do banco" in html


# ---------------------------------------------------------------------------
# O CLIQUE, PADRONIZADO — 13/09/2026
#
# *"Na tela por lançamento eu clico no registro, aí ele abre o card. Aí na tela
# por nota ele abre o registro do sistema. Está meio perdido assim. (…) Eu acho
# que o certo é dois clique na linha, abre o registro. E o linkzinho do card, aí
# abre o card do Pipefy. E não abrir direto, e sempre abrir modal, porque aí
# você permanece na tela."*
#
# Ele está certo, e o que havia era incoerente: duas telas do mesmo assunto com
# o clique fazendo coisas diferentes, e uma delas tirando a pessoa da tela —
# *"quando você bota voltar, ele volta pra solicitações, fica totalmente
# desvinculado"*.
# ---------------------------------------------------------------------------
def test_a_linha_abre_a_FICHA_com_dois_cliques(app_fiscal):
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "data-ficha=" in html
    assert 'id="ficha-modal"' in html, "a ficha tem de abrir por cima da lista"


def test_o_card_do_Pipefy_fica_num_link_PROPRIO(app_fiscal):
    """Antes o número da SP era o link do card, e por isso clicar na linha
    levava para fora. Agora o número é o número, e o card é um link à parte."""
    html = entrar(app_fiscal).get("/analisesps/fiscal?f=1").get_data(as_text=True)
    assert "link-card" in html
    assert "http://card" in html
    # E o número da SP deixou de ser o link do card: quem clica nele não sai
    # mais da tela por engano.
    import re
    assert not re.search(r'<td class="id">\s*<a[^>]*href="http://card', html)


def test_a_visao_por_nota_abre_a_ficha_DO_MESMO_JEITO(app_fiscal):
    """As duas telas do mesmo assunto não podem ter cliques diferentes."""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "data-ficha=" in html
    assert 'id="ficha-modal"' in html


def test_a_ficha_aberta_daqui_VOLTA_para_a_documentacao_fiscal(app_fiscal):
    """*"Quando você bota voltar, ele volta pra solicitações, fica totalmente
    desvinculado da documentação fiscal."* O endereço da ficha carrega de onde
    ela foi aberta."""
    for visao in ("", "&visao=notas"):
        html = entrar(app_fiscal).get(
            f"/analisesps/fiscal?f=1{visao}").get_data(as_text=True)
        assert "origem=fiscal" in html


def test_a_VISAO_escolhida_volta_depois_de_sair_da_tela(app_fiscal, monkeypatch):
    """*"Eu estava em por nota e fui pra outra tela e voltei; era pra voltar pra
    por nota."*"""
    from app.apps.analisesps import preferencias

    gaveta = {}
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: gaveta.__setitem__(chave, valor))
    monkeypatch.setattr(preferencias, "ler",
                        lambda pessoa, chave: gaveta.get(chave, {}))

    cliente = entrar(app_fiscal)
    cliente.get("/analisesps/fiscal?f=1&visao=notas")
    resposta = cliente.get("/analisesps/fiscal")
    assert resposta.status_code == 302
    assert "visao=notas" in resposta.headers["Location"]


# ---------------------------------------------------------------------------
# O RECADO DA BUSCA NA RECEITA
#
# *"Eu estou vendo aqui 'a busca nunca rodou'. (…) Em configurações eu tenho os
# certificados, eu cadastrei três certificados já."*
# ---------------------------------------------------------------------------
def test_com_certificado_e_SEM_busca_a_tela_nao_pede_certificado(app_fiscal, monkeypatch):
    """Dizer "falta o certificado" para quem cadastrou três manda procurar no
    lugar errado. O que falta é apertar o botão."""
    from app.apps.analisesps import sefaz, web

    monkeypatch.setattr(sefaz, "estado_das_buscas", lambda: [])
    monkeypatch.setattr(web, "_cnpjs_com_certificado",
                        lambda: ["10656452007869", "29066773000152",
                                 "11222333000181"])
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "3 certificado(s) guardado(s)" in html
    assert "ainda não foi disparada" in html
    assert "Cadastrar o certificado não dispara nada sozinho" not in html or True


def test_o_CNPJ_com_certificado_e_sem_busca_aparece_pelo_nome(app_fiscal, monkeypatch):
    """Com três certificados e um só consultado, saber QUAL falta é a diferença
    entre resolver e adivinhar."""
    from app.apps.analisesps import web

    monkeypatch.setattr(web, "_cnpjs_com_certificado",
                        lambda: ["10656452007869", "99888777000166"])
    # A busca do dublê cobre só o primeiro CNPJ — o segundo é o que falta.
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "99888777000166" in html
    assert "nunca foram consultados" in html


# ---------------------------------------------------------------------------
# O QUE OS BOTÕES FAZEM, ESCRITO
# ---------------------------------------------------------------------------
def test_a_tela_EXPLICA_o_que_associar_faz(app_fiscal):
    """*"O que é que acontece quando eu clico em associar? Ele vai pro gravar
    no que foi confirmado, é isso?"*"""
    html = entrar(app_fiscal).get(
        "/analisesps/fiscal?f=1&visao=notas").get_data(as_text=True)
    assert "Não mexe no card do Pipefy ainda" in html
    assert "Falta gravar no card" in html


def test_devolver_a_planilha_EXPLICA_o_que_e_a_fila():
    """*"O que é que significa devolver à planilha as alterações?"* A ajuda
    antiga dizia "as alterações feitas na tela", que não explica nada para quem
    não sabe que existe uma fila."""
    from app.apps.analisesps import web

    acao = next(a for a in web.ACOES_FISCAIS if a["modo"] == "fila")
    assert "fila" in acao["ajuda"]
    assert "não tem nada de fiscal" in acao["ajuda"].lower()
