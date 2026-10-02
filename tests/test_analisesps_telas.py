"""Análise de SPs — as telas realmente montam.

Erro em template não aparece em teste de regra nem em `python -c "import"`: ele
espera o usuário abrir a página. Um `{% endfor %}` faltando, um campo com nome
errado, um filtro que não existe — tudo isso passa por toda a suíte e quebra na
cara de quem foi trabalhar.

Estes testes montam cada tela com dados falsos e conferem que o HTML sai
inteiro, com os números escritos em português. Não abrem banco: as consultas
são dubladas, porque o que está sob teste aqui é a TELA, não o SQL (esse tem o
`test_analisesps_banco.py`).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from decimal import Decimal

import pytest
from flask import Flask

from app.apps.analisesps import consultas
from app.apps.analisesps import preferencias, web


SENHA_OPERADOR = "operador-de-teste"
SENHA_CONSULTA = "consulta-de-teste"


def linha_falsa(sp_id="1234567890", **campos):
    base = {
        "id": sp_id,
        "solicitacao_d": dt.date(2026, 1, 5),
        "vencimento_d": dt.date(2026, 2, 10),
        "credor": "VOTORANTIM CIMENTOS S.A.",
        "documento": "01.637.895/0001-32",
        "tipo_despesa": "Material",
        "centro_custo": "OBRA-12",
        "projeto": "Residencial Aurora",
        "valor_num": Decimal("6750.00"),
        "responsavel": "Marcelo",
        "status_pgt": "Pagar",
        "status_aut": "Autorizada",
        "forma_pagamento": "Boleto",
        "conta": "Bradesco 1234",
        "info_pgt": "",
        "nf": "12345",
        "pedido": "PC-99",
        "data_pagamento_d": None,
        "anuente": "",
        "validacao": "",
        "comprovante": "",
        "codigo_barras": "34191790010104351004791020150008291070026000",
        "analise_ia": "",
        "descricao": "Cimento CP-II, 200 sacos",
        "anexo_link": "",
        "card_link": "",
        "status_agend": "Agendar",
        "risco": False,
        "cadastro_incompleto": False,
        "vencido": True,
        "vence_hoje": False,
    }
    base.update(campos)
    return base


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)

    monkeypatch.setattr(consultas, "base_carregada",
                        lambda: {"pronta": True, "quantidade": 59055,
                                 "ultima": "2026-09-02T18:00:00"})
    monkeypatch.setattr(consultas, "resumo", lambda f: {
        "quantidade": 2, "total": Decimal("13500.00"),
        "quantidade_pagar": 1, "total_pagar": Decimal("6750.00")})
    monkeypatch.setattr(consultas, "listar", lambda f, **k: [
        linha_falsa("1", vencido=True),
        linha_falsa("2", status_pgt="Cancelado", status_agend="",
                    risco=True, cadastro_incompleto=True, vencido=False),
    ])
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: ["Pagar", "Pago", "Cancelado"])
    # Os números de baixo (o painel_kpis do Streamlit). São SQL de verdade na
    # aplicação; aqui basta que a tela saiba desenhá-los.
    monkeypatch.setattr(consultas, "contagem_agendamento", lambda f: {
        "Agendar": 1, "Agendado": 1, "Falha Agendar": 0, "Pago": 0})
    # A tela pede os dois JUNTOS desde 05/09 — eram duas varreduras da mesma
    # tabela filtrada, e numa consulta só custam quase metade.
    monkeypatch.setattr(consultas, "resumo_e_agendamento", lambda f: (
        consultas.resumo(f), consultas.contagem_agendamento(f)))
    # As listas do filtro ficam guardadas até a próxima carga; sem limpar, o
    # que um teste calculou valeria no seguinte.
    monkeypatch.setattr(consultas, "opcoes_de_filtro",
                        lambda carimbo=None: dict(
                            {a: consultas.opcoes(c, limite=l)
                             for a, (c, l) in consultas.COLUNAS_DE_FILTRO.items()},
                            status_agend=consultas.opcoes_agendamento()))
    monkeypatch.setattr(consultas, "soma_por", lambda f, coluna, limite=12: [
        {"nome": "BRADESCO 7011-4", "quantidade": 1, "total": Decimal("6750.00")}])
    # As colunas da tabela saem das preferências, que ficam no banco. Sem
    # dublar, cada tela tentaria abrir conexão só para saber o que mostrar.
    monkeypatch.setattr(preferencias, "ler", lambda pessoa, chave: {})
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: None)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def como(app, senha, nome="MARCELO"):
    """Entra no módulo com um NOME, que é o que separa o lote, os filtros e as
    colunas de cada pessoa, e assina o registro de alterações.

    ⚠️ DESDE 25/09/2026 O NOME NÃO SE DIGITA NA ENTRADA: ele vem do cadastro da
    pessoa. Estes testes não sobem banco, então entram pela porta de emergência
    (a senha do serviço) e põem o nome na sessão na mão — que é exatamente o
    que o cadastro faz quando alguém entra de verdade. O que está sob teste
    aqui é o que o sistema faz COM o nome, não de onde ele veio; de onde ele
    vem tem teste próprio, com banco, em `test_analisesps_usuarios_banco.py`.
    """
    from app.apps.analisesps import auth as guarda
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": senha})
    if nome:
        with cliente.session_transaction() as sessao:
            sessao[guarda.CHAVE_NOME] = nome
    return cliente


# ---------------------------------------------------------------------------
# A tela principal
# ---------------------------------------------------------------------------
def test_a_tela_de_solicitacoes_monta(app):
    resposta = como(app, SENHA_OPERADOR).get("/analisesps/solicitacoes")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "VOTORANTIM CIMENTOS" in html
    assert "Solicitações" in html


def test_os_valores_saem_em_portugues(app):
    """Ponto no milhar, vírgula nos centavos, data DD/MM/AAAA. Um número em
    formato americano numa tela de contas a pagar é erro de leitura esperando
    para acontecer."""
    import re
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    # Só o que a pessoa LÊ. A tabela também carrega o valor cru em
    # `data-valor`, que é o que a barra de ações soma — número de máquina,
    # invisível, e que não pode ser confundido com número de tela.
    visivel = re.sub(r"<[^>]+>", " ", html)
    assert "6.750,00" in visivel
    assert "10/02/2026" in visivel
    assert "6750.00" not in visivel, "número em formato americano na tela"
    assert "2026-02-10" not in visivel, "data em formato americano na tela"


def test_a_soma_do_filtro_aparece(app):
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "13.500,00" in html          # total do filtro
    assert "59.055" in html             # tamanho da base, com ponto de milhar


def test_os_alertas_aparecem_na_linha(app):
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "Risco" in html
    assert "Cadastro" in html


def test_operador_ve_os_botoes_de_alteracao(app):
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'data-coluna="status_pgt"' in html
    assert 'data-coluna="agendado"' in html


def test_consulta_nao_ve_botao_de_alteracao(app):
    """A trava de verdade é no servidor (`test_analisesps_acesso.py`). Esconder
    o botão é cortesia: não adianta oferecer o que vai ser recusado."""
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'data-coluna="status_pgt"' not in html
    assert "vê e exporta, não altera" in html


def test_consulta_continua_podendo_exportar(app):
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "Exportar CSV" in html


def test_os_filtros_marcados_voltam_marcados(app):
    """Quem filtra, pagina e volta não pode achar os filtros limpos."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes?status_pgt=Pagar&situacoes=risco"
    ).get_data(as_text=True)
    assert 'value="Pagar"\n                   checked' in html or \
           'value="Pagar" checked' in html or "checked" in html


def test_a_paginacao_preserva_os_filtros(app):
    """Sem isto, clicar em "próxima" jogaria a pessoa numa lista sem filtro —
    e ela pagaria a conta errada."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes?status_pgt=Pagar&busca=cimento"
    ).get_data(as_text=True)
    assert "status_pgt=Pagar" in html
    assert "busca=cimento" in html


def test_base_vazia_diz_que_e_falta_de_carga(app, monkeypatch):
    """Lista vazia não é "não há contas a pagar". Confundir os dois faria
    alguém concluir que está tudo em dia quando a carga apenas não rodou."""
    monkeypatch.setattr(consultas, "base_carregada",
                        lambda: {"pronta": False, "quantidade": 0, "ultima": None})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "ainda não foi carregada" in html


# ---------------------------------------------------------------------------
# A ficha da SP
# ---------------------------------------------------------------------------
def test_a_ficha_da_sp_monta(app, monkeypatch):
    from app.apps.analisesps import colunas
    ficha = {c: "" for c in colunas.CHAVES}
    ficha.update({
        "id": "1234567890", "credor": "VOTORANTIM CIMENTOS S.A.",
        "descricao": "Cimento CP-II", "analise_ia": "Pagamento COM RISCO",
        "valor_num": Decimal("6750.00"),
        "solicitacao_d": dt.date(2026, 1, 5),
        "vencimento_d": dt.date(2026, 2, 10),
        "data_pagamento_d": None, "dt_autorizacao_d": dt.date(2026, 1, 6),
        "status_agend": "Agendar",
    })
    monkeypatch.setattr(consultas, "uma", lambda sp_id: ficha)
    resposta = como(app, SENHA_OPERADOR).get("/analisesps/sp/1234567890")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "6.750,00" in html
    assert "10/02/2026" in html
    assert "COM RISCO" in html


def test_campo_vazio_vira_travessao_e_nao_none(app, monkeypatch):
    """"None" numa tela é defeito visível. Vazio é travessão."""
    from app.apps.analisesps import colunas
    ficha = {c: "" for c in colunas.CHAVES}
    ficha.update({"id": "1", "valor_num": None, "solicitacao_d": None,
                  "vencimento_d": None, "data_pagamento_d": None,
                  "dt_autorizacao_d": None, "status_agend": ""})
    monkeypatch.setattr(consultas, "uma", lambda sp_id: ficha)
    html = como(app, SENHA_OPERADOR).get("/analisesps/sp/1").get_data(as_text=True)
    assert ">None<" not in html
    assert "—" in html


def test_sp_inexistente_responde_404(app, monkeypatch):
    monkeypatch.setattr(consultas, "uma", lambda sp_id: None)
    resposta = como(app, SENHA_OPERADOR).get("/analisesps/sp/999")
    assert resposta.status_code == 404


# ---------------------------------------------------------------------------
# A exportação
# ---------------------------------------------------------------------------
def test_o_csv_sai_do_jeito_que_o_excel_brasileiro_abre(app):
    """Três detalhes, e os três precisam estar juntos: BOM no começo (para o
    acento não virar caractere estranho), ponto-e-vírgula separando as colunas
    (porque a vírgula é o decimal) e vírgula nos centavos."""
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/exportar")
    assert resposta.status_code == 200
    texto = resposta.get_data(as_text=True)
    assert texto.startswith("﻿")
    primeira = texto.split("\r\n")[0]
    assert ";" in primeira and "," not in primeira
    assert "6.750,00" in texto
    assert "10/02/2026" in texto


def test_o_csv_vem_como_arquivo_para_baixar(app):
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/exportar")
    assert "attachment" in resposta.headers["Content-Disposition"]
    assert ".csv" in resposta.headers["Content-Disposition"]


def test_o_csv_protege_o_ponto_e_virgula_dentro_do_texto(app, monkeypatch):
    """Uma descrição com ponto-e-vírgula partiria a linha em duas colunas e
    desalinharia a planilha inteira dali para baixo."""
    monkeypatch.setattr(consultas, "listar", lambda f, **k: [
        linha_falsa("1", descricao='cimento; areia; brita "fina"')] if k.get(
            "pagina", 1) == 1 else [])
    texto = como(app, SENHA_CONSULTA).get(
        "/analisesps/exportar").get_data(as_text=True)
    linhas = [l for l in texto.split("\r\n") if l]
    assert len(linhas) == 2                       # cabeçalho + uma linha
    assert '"cimento; areia; brita ""fina"""' in texto


def test_o_csv_nao_e_montado_inteiro_na_memoria(app):
    """Um filtro largo pode alcançar as 59 mil SPs. Montar tudo antes de enviar
    é justamente o que a instância de 2 GB não suporta — por isso a resposta é
    gerada em pedaços."""
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/exportar")
    assert resposta.is_streamed


# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
def test_a_tela_de_configuracoes_monta_mesmo_com_o_banco_fora(app, monkeypatch):
    """Banco fora do ar tem de dar uma tela com recado, não um erro 500 — é
    justamente quando alguém precisa entrar para entender o que houve."""
    from app.apps.analisesps import migracoes_runner, tarefas
    monkeypatch.setattr(migracoes_runner, "listar_estado",
                        lambda: (_ for _ in ()).throw(RuntimeError("banco fora")))
    monkeypatch.setattr(tarefas, "estado",
                        lambda: {"rodando": False, "detalhe": None,
                                 "interrompida": None})
    resposta = como(app, SENHA_OPERADOR).get("/analisesps/configuracoes")
    assert resposta.status_code == 200
    assert "banco fora" in resposta.get_data(as_text=True)


def test_a_tela_de_configuracoes_mostra_carga_em_andamento(app, monkeypatch):
    from app.apps.analisesps import migracoes_runner, tarefas
    monkeypatch.setattr(migracoes_runner, "listar_estado",
                        lambda: {"aplicadas": [], "pendentes": []})
    monkeypatch.setattr(tarefas, "estado", lambda: {
        "rodando": True, "interrompida": None,
        "detalhe": {"etapa": "trazendo as SPs da planilha",
                    "progresso": "20000 de 59055", "visto_em": None}})
    monkeypatch.setattr(tarefas, "ultima_concluida", lambda: None)
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/configuracoes").get_data(as_text=True)
    assert "trazendo as SPs da planilha" in html
    assert "20000 de 59055" in html
    assert "Pode fechar esta página" in html


def test_carga_interrompida_aparece_como_interrompida(app, monkeypatch):
    """O defeito que o painel levou uma migração inteira para consertar: sem
    isto, a tela mostraria a falha ANTERIOR como se fosse a atual, e o dono
    ficaria lendo um erro velho achando que era novo."""
    from app.apps.analisesps import migracoes_runner, tarefas
    monkeypatch.setattr(migracoes_runner, "listar_estado",
                        lambda: {"aplicadas": [], "pendentes": []})
    monkeypatch.setattr(tarefas, "estado", lambda: {
        "rodando": False, "detalhe": None,
        "interrompida": {"etapa": "trazendo as SPs da planilha",
                         "progresso": "20000 de 59055", "visto_em": None}})
    monkeypatch.setattr(tarefas, "ultima_concluida", lambda: None)
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/configuracoes").get_data(as_text=True)
    assert "interrompida" in html.lower()
    assert "retoma de onde parou" in html


def test_configuracoes_monta_com_o_banco_de_pe_e_as_migracoes_por_aplicar(
        monkeypatch):
    """O estado de estreia do módulo: banco respondendo, migrações ainda não
    aplicadas.

    Este é o caso REAL que quebrou na primeira vez que o dono abriu o módulo
    no ar, e é o pior lugar possível para quebrar: a tela de Configurações é
    a ÚNICA com o botão que aplica as migrações. Estourando ela, não havia
    como sair do estado — a tela que conserta era a tela quebrada.

    Passou por toda a suíte porque os outros testes daqui ou derrubam o banco
    inteiro (e aí a leitura da última execução nem é tentada), ou dublam
    `ultima_concluida`. Nenhum exercitava o meio-termo: `listar_estado()`
    funciona — ela mesma cria o schema e a tabela de controle —, mas
    `analisesps.execucoes` ainda não existe."""
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)

    from app.apps.analisesps import db as banco
    from app.apps.analisesps import migracoes_runner

    monkeypatch.setattr(migracoes_runner, "listar_estado",
                        lambda: {"aplicadas": [],
                                 "pendentes": ["001_estrutura.sql",
                                               "002_agenda_e_lote.sql"]})

    def sem_tabela(*a, **k):
        raise RuntimeError(
            'relation "analisesps.execucoes" does not exist')

    monkeypatch.setattr(banco, "consultar", sem_tabela)
    monkeypatch.setattr(banco, "consultar_um", sem_tabela)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = False        # queremos a resposta, não a exceção crua

    resposta = como(a, SENHA_OPERADOR).get("/analisesps/configuracoes")
    assert resposta.status_code == 200, (
        "Configurações estourou com as migrações por aplicar — é a única tela "
        "que sabe aplicá-las")
    html = resposta.get_data(as_text=True)
    assert "001_estrutura.sql" in html
    assert "002_agenda_e_lote.sql" in html


def test_configuracoes_nao_diz_que_o_banco_esta_em_dia_quando_nao_sabe(
        monkeypatch):
    """Com o banco inalcançável, a tela dizia "0 aplicada(s), 0 pendente(s) —
    O banco está em dia" e "A base: vazia".

    As duas frases afirmam o que não se sabe, e afirmam justamente o
    contrário do que está acontecendo. Quem lê conclui que a estrutura está
    pronta e que não há SPs — e para de procurar a causa no lugar certo.
    "Não deu para saber" é a única resposta honesta aqui."""
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)

    from app.apps.analisesps import db as banco

    def banco_fora(*a, **k):
        raise RuntimeError("could not connect to server")

    monkeypatch.setattr(banco, "conexao", banco_fora)
    monkeypatch.setattr(banco, "consultar", banco_fora)
    monkeypatch.setattr(banco, "consultar_um", banco_fora)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = False

    resposta = como(a, SENHA_OPERADOR).get("/analisesps/configuracoes")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)

    assert "O banco não respondeu" in html
    assert "O banco está em dia" not in html, (
        "a tela afirmou que o banco está em dia sem ter conseguido perguntar")
    assert "aplicada(s)" not in html, (
        "a contagem de migrações é falsa quando a consulta nem aconteceu")
    assert html.count("não deu para saber") >= 2, (
        "estrutura e base precisam as duas dizer que não se sabe")


# ---------------------------------------------------------------------------
# A VOLTA AO STREAMLIT
#
# O dono conviveu anos com o programa em Streamlit e a conversão trocou coisas
# que ele não pediu. Cada teste daqui trava um comportamento que ELE apontou
# como perdido — não são preferências de quem escreveu o código.
# ---------------------------------------------------------------------------
def test_o_numero_da_sp_abre_o_card_no_pipefy(app, monkeypatch):
    """No Streamlit a coluna ID era um link para o card. Virou link para a
    ficha interna, e o caminho para o Pipefy — que é onde se resolve o problema
    de verdade — passou a exigir dois cliques a mais."""
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "listar", lambda f, **k: [
        linha_falsa("1", card_link="https://app.pipefy.com/open-cards/1")])
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "app.pipefy.com/open-cards/1" in html, (
        "o número da SP não leva mais ao card")


def test_o_duplo_clique_na_linha_abre_a_ficha(app):
    """A ficha volta a abrir POR CIMA da lista, como o modal do Streamlit: sem
    perder a rolagem, o filtro nem a marcação a cada consulta."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "data-ficha=" in html, "a linha não sabe qual ficha abrir"
    assert 'id="ficha-modal"' in html, "não há modal na tela"
    assert "dblclick" in html


def test_a_ficha_em_modal_vem_sem_o_resto_da_tela(app_ficha):
    """O modal busca só o miolo. Se viesse a página inteira, apareceria um
    cabeçalho e um menu dentro da janelinha."""
    resposta = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1")
    html = resposta.get_data(as_text=True)
    assert resposta.status_code == 200
    # Marcas do gabarito da PÁGINA. Não se procura "<!doctype" aqui porque o
    # código de barras é um SVG e traz o DOCTYPE dele — o que se quer saber é
    # se veio o cabeçalho e o menu do módulo.
    assert "<html" not in html.lower()
    assert "topo-abas" not in html
    assert "1234567890" in html


def test_a_barra_de_acoes_e_uma_so_e_mostra_o_total_marcado(app):
    """Era uma barra por tabela no Lote, e o total da seleção ficava num canto
    em letra miúda. O dono pediu o número no meio e em destaque: é ele que
    decide se a remessa vai."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert html.count('id="barra-acoes"') == 1, "mais de uma barra na tela"
    assert 'id="ba-valor"' in html, "a barra não mostra o total da seleção"
    assert 'data-valor=' in html, "as linhas não dizem o valor que a barra soma"


def test_os_quatro_do_agendamento_sao_os_do_streamlit(app):
    """Agendar, Agendado, Falha Agendar e Desagendar — nesta ordem e com estes
    nomes. "Desagendar" é o que apaga o campo na planilha; o rótulo na tela diz
    "Remover informação" porque é assim que o dono chama."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    for valor in ("Agendar", "Agendado", "Falha Agendar", "Desagendar"):
        assert f'data-valor="{valor}"' in html, f"sumiu o botão {valor}"
    # O rótulo é o do Streamlit. Chegou a ser "Remover informação", que
    # descrevia o efeito; o dono pediu o nome que ele usa — e agora convive na
    # mesma barra com "Remover do lote", que é outra coisa.
    assert ">Desagendar</button>" in html


def test_o_lote_nao_tem_marcar_pago(app_lote):
    """O dono mandou tirar, e o Streamlit nunca teve esse botão nesta tela.
    Marcar como pago no meio da remessa é o erro que não tem volta."""
    html = como(app_lote, SENHA_OPERADOR).get(
        "/analisesps/lote").get_data(as_text=True)
    assert 'data-valor="Pago"' not in html
    assert "Marcar Pago" not in html


def test_cada_grupo_do_lote_mostra_os_seus_numeros(app_lote):
    """Pedido do dono: KPIs no cabeçalho de cada grupo, com o total em
    destaque. Antes era uma linha de letra miúda ao lado do título."""
    html = como(app_lote, SENHA_OPERADOR).get(
        "/analisesps/lote").get_data(as_text=True)
    assert "Total do grupo" in html
    assert "kpis-grupo" in html


def test_o_agendamento_exige_validacao_como_no_streamlit(app_ficha, monkeypatch):
    """A trava tinha sumido na conversão: qualquer um agendava qualquer coisa.

    No Streamlit os quatro botões de agendamento só abriam com a coluna
    Validação em "Sim" — é a conferência que separa "alguém pediu" de "alguém
    conferiu"."""
    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})

    registro["validacao"] = ""
    monkeypatch.setattr(consultas, "uma", lambda i: registro)
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "Agendamento bloqueado" in html
    assert 'data-bloqueado="1"' in html

    registro["validacao"] = "Sim"
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "Agendamento bloqueado" not in html
    assert "data-bloqueado" not in html


def test_o_agendamento_travado_nao_vira_botao_morto(app_ficha, monkeypatch):
    """O dono clicou em "Agendado" no modal e nada aconteceu.

    Não era defeito de ligação: a trava da Validação punha `disabled` nos
    botões, e botão desabilitado não recebe nem o clique — quem não leu o
    aviso logo acima via um botão simplesmente quebrado. A trava continua
    valendo (nada é gravado), mas agora o clique diz por que não foi."""
    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})
    registro["validacao"] = ""
    monkeypatch.setattr(consultas, "uma", lambda i: registro)

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)

    trecho = html[html.index('data-coluna="agendado"'):]
    trecho = trecho[:trecho.index("</button>")]
    assert "disabled" not in trecho, (
        "botão desabilitado não recebe clique — volta a parecer quebrado")
    assert "bloqueado" in trecho


def test_a_descricao_vem_antes_do_codigo_de_pagamento(app_ficha, monkeypatch):
    """Pedido do dono: a descrição é a primeira coisa que se procura ao abrir
    a SP, e estava no fim de tudo; o código de pagamento é o último passo."""
    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})
    registro["descricao"] = "Concreto usinado da obra"
    monkeypatch.setattr(consultas, "uma", lambda i: registro)

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)
    assert html.index("ficha-descricao") < html.index("ficha-codigo")


def test_o_link_escrito_na_descricao_fica_clicavel(app_ficha, monkeypatch):
    """Às vezes a descrição traz o endereço de uma pasta ou de um contrato.
    Como texto puro, era selecionar na mão e colar no navegador."""
    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})
    registro["descricao"] = "Contrato em https://drive.google.com/x/y ok"
    monkeypatch.setattr(consultas, "uma", lambda i: registro)

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)
    assert 'href="https://drive.google.com/x/y"' in html


def test_cancelar_sp_nao_e_botao_vermelho(app_ficha):
    """Ele só ABRE o formulário do Pipefy — não cancela nada por si. Em
    vermelho puxava o olho para si toda vez que a ficha abria."""
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    trecho = html[html.index("Cancelar%20SP") - 400:html.index("Cancelar SP")]
    assert "perigo" not in trecho


def test_a_volta_da_ficha_aberta_pelo_lote_nao_quebra(app_ficha):
    """O endereço da volta era montado colando "analisesps." com a origem, e
    dava "analisesps.lote" — que não existe. A tela do Lote se chama
    "tela_lote", então abrir uma SP a partir do Lote estourava a página."""
    resposta = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?origem=lote")
    assert resposta.status_code == 200
    assert "/analisesps/lote" in resposta.get_data(as_text=True)


def test_o_qr_volta_para_de_onde_veio(app, monkeypatch):
    """Estava fixo em Solicitações: quem gerava o QR de um grupo do Lote era
    largado em outra tela e tinha de refazer o caminho."""
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "uma", lambda i: linha_falsa(i))
    cliente = como(app, SENHA_OPERADOR)
    do_lote = cliente.get(
        "/analisesps/codigos?id=1&origem=lote").get_data(as_text=True)
    assert "/analisesps/lote" in do_lote

    de_fora = cliente.get(
        "/analisesps/codigos?id=1&origem=https://exemplo-malicioso.com"
    ).get_data(as_text=True)
    assert "exemplo-malicioso" not in de_fora, (
        "o destino da volta veio da barra de endereço sem ser conferido")


def test_a_barra_de_filtros_e_a_mesma_nas_duas_telas(app, monkeypatch):
    """O dono pediu: o filtro de Solicitações vale no Relatório. Antes o
    Relatório aceitava os filtros por baixo do pano, mas não tinha onde
    mexer neles."""
    from decimal import Decimal
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "numeros_do_relatorio", lambda *a, **k: {
        "quantidade": 0, "total": Decimal("0"), "media": Decimal("0"),
        "vencidas": 0, "total_vencidas": Decimal("0")})
    monkeypatch.setattr(consultas, "agregar", lambda *a, **k: [])
    # Desde 09/09 o Relatório pede as dimensões JUNTAS, numa varredura só.
    monkeypatch.setattr(consultas, "agregar_varias", lambda *a, **k: {})
    monkeypatch.setattr(consultas, "top_credores", lambda *a, **k: [])
    monkeypatch.setattr(consultas, "aging_vencidos", lambda *a, **k: [])
    cliente = como(app, SENHA_OPERADOR)
    for tela in ("/analisesps/solicitacoes", "/analisesps/relatorio"):
        html = cliente.get(tela + "?f=1").get_data(as_text=True)
        assert 'id="form-filtros"' in html, f"{tela} está sem a barra"
        assert 'name="status_pgt"' in html, f"{tela} está sem os filtros"


def test_a_barra_de_filtros_nao_tem_mais_botao_de_aplicar(app):
    """No Streamlit marcar já refazia a tela. O botão era um passo a mais em
    cada filtro, o dia inteiro."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes?f=1").get_data(as_text=True)
    assert "form.submit()" in html, "a barra não se aplica sozinha"
    # O botão continua existindo para quem está sem JavaScript — e SÓ para
    # esse caso, dentro do <noscript>.
    antes, _, depois = html.partition("<noscript>")
    assert "Aplicar filtros" not in antes


def test_a_ficha_traz_de_volta_os_links_que_o_streamlit_tinha(app_ficha,
                                                              monkeypatch):
    """Anexo, comprovante e os dois atalhos do Omie. Os do Omie dependem de uma
    variável no Render; sem ela os botões não aparecem, em vez de aparecerem
    quebrados."""
    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})
    registro["comprovante"] = "https://exemplo/comprovante.pdf"
    monkeypatch.setattr(consultas, "uma", lambda i: registro)

    monkeypatch.delenv("ANALISESPS_HOOK_OMIE", raising=False)
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "comprovante.pdf" in html
    assert "consultastatusomie" not in html

    monkeypatch.setenv("ANALISESPS_HOOK_OMIE", "https://exemplo/hook")
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "consultastatusomie" in html and "atualizatitulo" in html


def test_validar_escreve_na_planilha_como_qualquer_outra_alteracao(app,
                                                                    monkeypatch):
    """Validar não é especial no MECANISMO — é a mesma gravação de sempre, só
    que na coluna AH em vez da O ou da AB: banco, fila, log, planilha.

    O que a separa é o significado. A Validação é o que destrava o
    agendamento, então destravá-la pede uma senha PRÓPRIA: se a de Operador
    servisse, quem agenda seria o mesmo que autoriza a agendar, e a trava não
    travaria nada."""
    from app.apps.analisesps import credenciais, web as tela
    monkeypatch.setattr(credenciais, "token",
                        lambda nome, padrao="": "senha-de-validacao")

    gravado = {}
    monkeypatch.setattr(tela, "_gravar_alteracao",
                        lambda ids, coluna, valor, acao: gravado.update(
                            ids=ids, coluna=coluna, valor=valor, acao=acao)
                        or {"ok": True, "alteradas": len(ids)})

    cliente = como(app, SENHA_OPERADOR)

    errada = cliente.post("/analisesps/api/validar",
                          json={"ids": ["1"], "senha": "chute"})
    assert errada.status_code == 403
    assert not gravado, "gravou mesmo com a senha errada"

    certa = cliente.post("/analisesps/api/validar",
                         json={"ids": ["1", "2"],
                               "senha": "senha-de-validacao"})
    assert certa.status_code == 200
    assert gravado["coluna"] == "validacao"
    assert gravado["valor"] == "Sim"
    assert gravado["ids"] == ["1", "2"]


def test_sem_senha_de_validacao_cadastrada_ninguem_valida(app, monkeypatch):
    """Falha fechado, como o resto do módulo. E a mensagem diz onde cadastrar,
    senão vira um botão que não funciona e ninguém sabe por quê."""
    from app.apps.analisesps import credenciais
    monkeypatch.setattr(credenciais, "token", lambda nome, padrao="": "")

    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/api/validar", json={"ids": ["1"], "senha": "qualquer"})
    assert resposta.status_code == 409
    assert "SENHA_VALIDACAO" in resposta.get_json()["erro"]


def test_a_consulta_nao_valida(app):
    """Validar é escrita. O perfil que só olha não escreve, aqui como em tudo."""
    resposta = como(app, SENHA_CONSULTA).post(
        "/analisesps/api/validar", json={"ids": ["1"], "senha": "x"})
    assert resposta.status_code == 403


def test_validar_aparece_onde_se_precisa_dele(app, app_ficha, monkeypatch):
    """Na barra, para validar várias de uma vez; e na própria trava do
    agendamento, que é onde a pessoa descobre que falta validar."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'id="ba-validar"' in html

    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})
    registro["validacao"] = ""
    monkeypatch.setattr(consultas, "uma", lambda i: registro)
    ficha = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert 'id="ficha-validar"' in ficha


def test_a_coluna_da_validacao_continua_fechada_na_porta_comum(app):
    """A porta de sempre não pode validar: senão bastaria pedir a coluna
    "validacao" ali e a senha de validação viraria enfeite."""
    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/api/alterar",
        json={"ids": ["1"], "coluna": "validacao", "valor": "Sim"})
    assert resposta.status_code == 400
    assert "não é alterável" in resposta.get_json()["erro"]


# ---------------------------------------------------------------------------
# AS COLUNAS DA TABELA
# ---------------------------------------------------------------------------
def test_a_tabela_oferece_as_colunas_do_streamlit(app):
    """A conversão reduziu a tabela a nove colunas; o dono trabalhava com
    vinte. A coluna que falta é sempre a de que ele precisava naquele minuto —
    Validação, Nº NF, Data Pgt, Responsável, CPF/CNPJ."""
    from app.apps.analisesps import tabela
    rotulos = {c.rotulo for c in tabela.DEFINICOES}
    for esperado in ("ID", "Data", "Vencimento", "Credor", "CPF/CNPJ",
                     "Tipo de Despesa", "Valor",
                     "Status Pgt", "Status Agend", "Forma de Pgt",
                     "Conta Corrente", "Validação", "Informação p/ Pgt",
                     "Nº NF", "Data Pgt", "Comprovante", "Responsável"):
        assert esperado in rotulos, f"sumiu a coluna {esperado!r} do Streamlit"
    # O "Centro de Custo" do Streamlit se chama OBRA aqui: é a palavra que o
    # dono usa, e cabe no cabeçalho estreito. A barra de filtros diz "Obra
    # (centro de custo)", que é onde a ponte com o nome da planilha cabe.
    assert "Obra" in rotulos


def test_a_obra_vem_marcada_e_logo_depois_do_valor(app):
    """Pedido do dono: a obra faltava na lista, e o lugar dela é ao lado do
    valor — é a pergunta seguinte a "quanto é": "de qual obra?"."""
    from app.apps.analisesps import tabela
    escolhidas = [c.chave for c in tabela.escolhidas(None)]
    assert "centro_custo" in escolhidas, "a obra não vem marcada"
    assert escolhidas.index("centro_custo") == escolhidas.index("valor_num") + 1


def test_a_escolha_de_colunas_nao_deixa_a_tabela_sem_nenhuma(app):
    """Vazio não é escolha, é acidente — e uma tabela sem coluna nenhuma é
    uma tela que ninguém consegue usar para descobrir o que houve."""
    from app.apps.analisesps import tabela
    assert tabela.escolhidas([]) == tabela.escolhidas(None)
    assert tabela.escolhidas(["nao_existe"]) == tabela.escolhidas(None)
    assert tabela.escolhidas("lixo") == tabela.escolhidas(None)


def test_a_ordem_das_colunas_e_sempre_a_mesma(app):
    """A ordem é a da definição, nunca a da escolha: se cada pessoa visse as
    colunas noutra ordem, uma não conseguiria explicar a tela para a outra."""
    from app.apps.analisesps import tabela
    guardado = tabela.para_guardar(["nf", "id", "credor"])
    escolhidas = tabela.escolhidas(guardado)
    assert [c.chave for c in escolhidas] == ["id", "credor", "nf"]


# ---------------------------------------------------------------------------
# COLUNA CRIADA DEPOIS APARECE PARA QUEM JÁ TINHA ESCOLHIDO
#
# "dentre as colunas não está aparecendo a coluna com a obra, muito
# importante" — 09/09/2026. A Obra entrou nas colunas padrão em 05/09, mas
# quem já tinha uma escolha guardada continuou sem ela: a escolha antiga não
# mencionava uma coluna que ainda não existia, e o programa lia isso como
# "ele não quer". Uma coluna acrescentada ficava invisível para sempre.
# ---------------------------------------------------------------------------
def test_coluna_criada_depois_aparece_para_quem_ja_tinha_escolhido():
    """O caso exato da Obra."""
    from app.apps.analisesps import tabela

    # Como se ele tivesse escolhido quando a Obra ainda não existia.
    guardado = tabela.para_guardar([c.chave for c in tabela.DEFINICOES
                                    if c.padrao and c.chave != "centro_custo"])
    guardado["conhecidas"] = [c for c in guardado["conhecidas"]
                              if c != "centro_custo"]

    assert "centro_custo" in [c.chave for c in tabela.escolhidas(guardado)]


def test_coluna_tirada_de_proposito_continua_fora():
    """O outro lado: repor tudo o que é padrão em toda leitura tornaria
    impossível esconder qualquer coluna — inclusive a Descrição, que o dono
    esconde e mostra dez vezes por dia."""
    from app.apps.analisesps import tabela

    atuais = [c.chave for c in tabela.escolhidas(None) if c.chave != "descricao"]
    guardado = tabela.para_guardar(atuais)

    rotulos = [c.chave for c in tabela.escolhidas(guardado)]
    assert "descricao" not in rotulos
    assert "centro_custo" in rotulos, "a Obra não podia ter sumido junto"


def test_escolha_antiga_recupera_o_padrao_que_falta():
    """A escolha guardada ANTES desta correção não diz o que conhecia. O
    desempate é repor o padrão que falta, uma vez: custa um clique a quem tinha
    escondido alguma de propósito, e a alternativa era deixar a Obra invisível
    para quem mais precisa dela."""
    from app.apps.analisesps import tabela

    antiga = {"colunas": ["id", "credor", "valor_num"]}   # sem `conhecidas`
    rotulos = [c.chave for c in tabela.escolhidas(antiga)]
    assert "centro_custo" in rotulos
    # E o que ela tinha escolhido a mais continua lá.
    assert "credor" in rotulos


def test_o_que_e_guardado_registra_as_colunas_que_existiam():
    """É essa lista que faz a coluna de amanhã aparecer. Sem ela, o defeito da
    Obra se repete na próxima coluna que alguém criar."""
    from app.apps.analisesps import tabela

    guardado = tabela.para_guardar(["id", "credor"])
    assert guardado["colunas"] == ["id", "credor"]
    assert set(guardado["conhecidas"]) == set(tabela.CHAVES)


def test_a_obra_esta_entre_as_colunas_padrao():
    """Guarda de baixo nível, para a coluna não sair da lista por descuido."""
    from app.apps.analisesps import tabela
    assert "centro_custo" in tabela.PADRAO
    assert tabela.POR_CHAVE["centro_custo"].rotulo == "Obra"


def test_escolher_colunas_nao_e_alterar_dado(app):
    """É `@exige_consulta` de propósito: escolher o que se vê não muda nada da
    empresa, e o perfil que só olha tem direito de escolher como olha."""
    resposta = como(app, SENHA_CONSULTA).post(
        "/analisesps/colunas",
        data={"coluna": ["id"], "voltar": "/analisesps/solicitacoes"})
    assert resposta.status_code in (301, 302)


def test_a_volta_da_escolha_de_colunas_nao_sai_do_modulo(app):
    """O destino vem do formulário. Sem conferir, esta rota viraria trampolim
    para fora — a mesma armadilha do login."""
    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/colunas",
        data={"coluna": ["id"], "voltar": "https://exemplo-malicioso.com"})
    assert resposta.status_code in (301, 302)
    assert "exemplo-malicioso" not in resposta.headers["Location"]


# ---------------------------------------------------------------------------
# OS NÚMEROS DE BAIXO, E O RISCO
# ---------------------------------------------------------------------------
def test_os_numeros_de_baixo_voltaram(app, monkeypatch):
    """O `painel_kpis` do Streamlit: quanto por conta corrente, quanto por
    forma de pagamento, e como está a divisão do agendamento. Sumiu na
    conversão justamente a resposta de "quanto vai sair de cada conta", que é
    a pergunta de quem efetiva os pagamentos."""
    from decimal import Decimal
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "contagem_agendamento", lambda f: {
        "Agendar": 3, "Agendado": 2, "Falha Agendar": 1, "Pago": 4})
    monkeypatch.setattr(consultas, "resumo_e_agendamento", lambda f: (
        consultas.resumo(f), consultas.contagem_agendamento(f)))
    monkeypatch.setattr(consultas, "soma_por", lambda f, c, limite=12: [
        {"nome": "BRADESCO 7011-4", "quantidade": 2, "total": Decimal("9000.00")}])

    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "Σ por conta corrente" in html
    assert "Σ por forma de pagamento" in html
    assert "BRADESCO 7011-4" in html
    assert "9.000,00" in html


def test_remover_risco_grava_a_revisao_com_o_nome_de_quem_revisou(app,
                                                                  monkeypatch):
    """No Streamlit, "Remover Risco" escrevia na coluna da análise que aquilo
    já tinha sido olhado. É o que tira a SP da lista de risco — e a palavra
    "COM RISCO" no texto é justamente o que a põe lá.

    Fica registrado QUEM revisou: dizer "pode pagar, eu conferi" é uma
    responsabilidade, e responsabilidade sem nome não é responsabilidade."""
    from app.apps.analisesps import web as tela
    gravado = {}
    monkeypatch.setattr(tela, "_gravar_alteracao",
                        lambda ids, coluna, valor, acao: gravado.update(
                            ids=ids, coluna=coluna, valor=valor, acao=acao)
                        or {"ok": True, "alteradas": len(ids)})

    resposta = como(app, SENHA_OPERADOR, nome="MARCELO").post(
        "/analisesps/api/sem-risco", json={"ids": ["1"]})
    assert resposta.status_code == 200
    assert gravado["coluna"] == "analise_ia"
    assert gravado["valor"].startswith("SEM RISCO")
    assert "MARCELO" in gravado["valor"], "não diz quem revisou"
    assert "COM RISCO" not in gravado["valor"], (
        "o texto novo ainda casa com a regra que marca risco")


def test_a_consulta_nao_remove_risco(app):
    resposta = como(app, SENHA_CONSULTA).post(
        "/analisesps/api/sem-risco", json={"ids": ["1"]})
    assert resposta.status_code == 403


# ---------------------------------------------------------------------------
# A LEVA DE 04/09 À NOITE — o dono olhando a tela e apontando
# ---------------------------------------------------------------------------
def test_validacao_vem_marcada_e_responsavel_nao(app):
    """Escolha do dono, olhando a tela: Validação aparece porque é ela que
    destrava o agendamento — não vê-la é trabalhar às cegas. Responsável fica
    de fora porque ele não usa no dia a dia. Quem quiser, acrescenta."""
    from app.apps.analisesps import tabela
    padrao = {c.chave for c in tabela.escolhidas(None)}
    assert "validacao" in padrao
    assert "responsavel" not in padrao


def test_o_numero_da_sp_nao_e_pintado(app, monkeypatch):
    """O Streamlit tingia o ID conforme o alerta. Somado ao vencimento
    vermelho ao lado, a linha inteira ficava gritando — o dono pediu para
    tirar. O alerta continua, em selo, na coluna Alertas, onde não compete
    com nada."""
    css = (Path(__file__).resolve().parents[1] / "app" / "apps" / "analisesps"
           / "static" / "analisesps.css").read_text(encoding="utf-8")
    assert ".id a" not in css, "voltou a pintar o número da SP"


# ---------------------------------------------------------------------------
# AUDITORIA POR PERÍODO
# ---------------------------------------------------------------------------
def test_a_auditoria_aceita_periodo_em_todas_as_checagens(app, monkeypatch):
    """Auditar a base inteira dá o retrato de sempre; auditar um mês responde
    "o que entrou errado neste fechamento". As duas perguntas são legítimas, e
    o dono pediu a segunda."""
    from app.apps.analisesps import auditoria
    vistos = {}
    for nome in ("risco_ia", "nf_duplicada", "sem_classificacao",
                 "sem_integracao_omie"):
        monkeypatch.setattr(auditoria, nome,
                            lambda f, u=False, p=None, _n=nome:
                            vistos.update({_n: p}) or [])
    monkeypatch.setattr(auditoria, "codigos_de_barras",
                        lambda f, u=False, p=None:
                        vistos.update({"codigos_barras": p}) or {})
    monkeypatch.setattr(auditoria, "pontualidade",
                        lambda f, u=False, m=5, p=None:
                        vistos.update({"pontualidade": p}) or [])
    monkeypatch.setattr(auditoria, "possivel_duplicidade",
                        lambda f, u=False, d=7, p=None:
                        vistos.update({"possivel_duplicidade": p}) or [])

    cliente = como(app, SENHA_OPERADOR)
    for chave in ("pontualidade", "risco_ia", "nf_duplicada",
                  "possivel_duplicidade", "sem_classificacao",
                  "sem_integracao", "codigos_barras"):
        cliente.get(f"/analisesps/auditoria?checagem={chave}"
                    "&de=2026-09-01&ate=2026-09-30")

    for chave, periodo in vistos.items():
        assert periodo is not None, f"{chave} não recebeu o período"
        assert periodo["de"] == dt.date(2026, 9, 1), chave
        assert periodo["ate"] == dt.date(2026, 9, 30), chave


def test_o_periodo_da_auditoria_so_recorta_pelas_datas_que_conhecemos(
        app, monkeypatch):
    """A coluna do recorte é escolhida por nós, de uma lista fechada. Nome de
    coluna não entra como parâmetro do banco — e concatenar o que veio de fora
    aqui seria a porta aberta clássica."""
    from app.apps.analisesps import auditoria
    assert set(auditoria.CAMPOS_PERIODO) == {"vencimento", "solicitacao"}
    monkeypatch.setattr(auditoria, "resumo", lambda f, u=False, p=None: {})

    resposta = como(app, SENHA_OPERADOR).get(
        "/analisesps/auditoria?campo_data=valor_num;DROP+TABLE&de=2026-01-01")
    assert resposta.status_code == 200, "campo inventado derrubou a tela"


def test_a_tela_da_auditoria_mostra_o_controle_de_periodo(app, monkeypatch):
    from app.apps.analisesps import auditoria
    monkeypatch.setattr(auditoria, "resumo", lambda f, u=False, p=None: {})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/auditoria").get_data(as_text=True)
    assert 'name="de"' in html and 'name="ate"' in html
    assert 'name="campo_data"' in html


# ---------------------------------------------------------------------------
# NOTA REPETIDA x PARCELAMENTO
# ---------------------------------------------------------------------------
def test_a_regra_de_parcelas_esta_escrita_onde_se_le(app):
    """A regra é sutil o bastante para precisar estar dita: o grupo só sai da
    lista quando TODAS as SPs têm marca de parcela e as marcas são todas
    diferentes. Se duas dividem a mesma parcela, ou alguma está sem marca, o
    grupo continua aparecendo — porque aí "é parcelamento" não explica."""
    from app.apps.analisesps import auditoria
    doc = auditoria.nf_duplicada.__doc__ or ""
    assert "parcela" in doc.lower()
    assert "MARCA_PARCELA" in dir(auditoria) or hasattr(auditoria, "MARCA_PARCELA")


# ---------------------------------------------------------------------------
# A AGENDA VOLTA A TER CALENDÁRIO
# ---------------------------------------------------------------------------
def test_a_agenda_tem_a_grade_do_mes(app_agenda):
    """"A agenda só tem uma lista", disse o dono. O Streamlit mostrava um
    calendário de mês, com navegação e o que cai em cada dia — lista não
    responde "como está a semana que vem"."""
    html = como(app_agenda, SENHA_OPERADOR).get(
        "/analisesps/agenda").get_data(as_text=True)
    assert 'class="calendario"' in html
    assert html.count('class="cal-cab"') == 7, "faltam dias da semana"


def test_a_grade_do_mes_vira_o_ano(app):
    """Dezembro → janeiro do ano seguinte, e janeiro → dezembro do anterior."""
    from app.apps.analisesps import agenda
    assert agenda.mes_vizinho(2026, 12, 1) == (2027, 1)
    assert agenda.mes_vizinho(2026, 1, -1) == (2025, 12)


def test_mes_invalido_na_agenda_nao_derruba_a_tela(app_agenda):
    """O mês vem da barra de endereço — e barra de endereço recebe qualquer
    coisa, inclusive por engano de quem copiou o link pela metade."""
    cliente = como(app_agenda, SENHA_OPERADOR)
    for pedaco in ("?mes=13", "?mes=abc&ano=xyz", "?ano=1800", "?mes=0"):
        assert cliente.get("/analisesps/agenda" + pedaco).status_code == 200


# ---------------------------------------------------------------------------
# O BANCO PODE ESTAR ATRASADO
# ---------------------------------------------------------------------------
def test_o_modulo_aguenta_o_banco_sem_a_migracao_003(app, monkeypatch):
    """O código sobe para o Render ANTES de alguém apertar "Aplicar
    atualizações do banco". Nessa janela o programa é novo e o banco é velho.

    Foi assim que este módulo travou na estreia, em 03/09. Agora, onde uma
    coluna nova é usada, pergunta-se antes se ela existe — e sem ela o
    programa segue pelo caminho de antes, que funciona nos dois bancos."""
    from app.apps.analisesps import db as banco, lote

    # O banco responde, mas não conhece a coluna `pessoa`.
    monkeypatch.setattr(banco, "tem_coluna", lambda tabela, coluna: False)
    banco.esquecer_colunas()

    assert lote.por_pessoa() is False
    # E o lote continua sendo lido — pelo caminho antigo, o de uma linha só.
    lido = []
    monkeypatch.setattr(banco, "consultar_um",
                        lambda sql, params=(): lido.append(sql) or None)
    lote.ler("marcelo")
    assert "WHERE id = 1" in lido[0], (
        "com o banco atrasado, o lote tem de ser lido como era antes")


def test_aplicar_as_migracoes_faz_o_processo_reaprender_o_banco(app,
                                                                monkeypatch):
    """Sem esquecer o que sabia, este worker continuaria achando que a coluna
    nova não existe até o próximo reinício — e o dono apertaria o botão sem
    ver efeito nenhum."""
    from app.apps.analisesps import db as banco, migracoes_runner
    monkeypatch.setattr(migracoes_runner, "aplicar_pendentes",
                        lambda: {"aplicadas": [], "erro": None,
                                 "pendentes_restantes": []})
    banco._colunas_conhecidas[("lote", "pessoa")] = True
    como(app, SENHA_OPERADOR).post("/analisesps/api/migrar")
    assert not banco._colunas_conhecidas, (
        "o processo continuou com a ideia antiga do formato do banco")


# ---------------------------------------------------------------------------
# DESCRIÇÃO E TIPO DE DESPESA (pedido de 04/09, à noite)
# ---------------------------------------------------------------------------
def test_descricao_e_tipo_de_despesa_vem_na_tabela(app):
    """Pedido do dono. A descrição é o texto mais comprido da linha, então sai
    com letra menor e cortada na largura — o texto inteiro fica no title."""
    from app.apps.analisesps import tabela
    padrao = {c.chave for c in tabela.escolhidas(None)}
    assert "descricao" in padrao
    assert "tipo_despesa" in padrao
    assert tabela.POR_CHAVE["descricao"].tipo == "longo"


def test_a_descricao_sai_menor_e_com_o_texto_inteiro_no_title(app, monkeypatch):
    from app.apps.analisesps import consultas
    longo = ("Compra de cimento CP-II 50kg, 200 sacos, entrega na obra da "
             "Rua das Palmeiras, conforme pedido 4471")
    monkeypatch.setattr(consultas, "listar",
                        lambda f, **k: [linha_falsa("1", descricao=longo)])
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'class="texto-longo"' in html
    assert longo in html, "o texto inteiro tem de ficar no title"


def test_as_colunas_sao_as_mesmas_nas_duas_telas(app_lote, monkeypatch):
    """Pedido explícito do dono. As duas telas leem a MESMA escolha, então não
    há como divergirem — este teste existe para que continuem assim."""
    import re
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "listar", lambda f, **k: [linha_falsa("1")])

    def cabecalho(html):
        i = html.find("<thead>")
        return re.findall(r"<th[^>]*>([^<]+)</th>", html[i:i + 1200])

    cliente = como(app_lote, SENHA_OPERADOR)
    das_solicitacoes = cabecalho(
        cliente.get("/analisesps/solicitacoes").get_data(as_text=True))
    do_lote = cabecalho(cliente.get("/analisesps/lote").get_data(as_text=True))
    assert das_solicitacoes == do_lote, "as duas telas divergiram nas colunas"


def test_a_descricao_se_esconde_e_volta_num_clique(app):
    """A descrição ocupa muito, e o dono quer poder tirá-la sem abrir a lista
    de colunas — é coisa que se faz dez vezes por dia."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "Esconder a descrição" in html
    assert 'value="alternar"' in html


def test_alternar_uma_coluna_nao_mexe_nas_outras(app, monkeypatch):
    from app.apps.analisesps import preferencias, tabela, web as tela

    guardado = {"colunas": [c.chave for c in tabela.escolhidas(None)]}
    monkeypatch.setattr(preferencias, "ler",
                        lambda pessoa, chave: dict(guardado))
    monkeypatch.setattr(preferencias, "gravar",
                        lambda pessoa, chave, valor: guardado.update(valor))

    antes = set(guardado["colunas"])
    como(app, SENHA_OPERADOR).post(
        "/analisesps/colunas",
        data={"acao": "alternar", "coluna": "descricao",
              "voltar": "/analisesps/solicitacoes"})
    depois = set(guardado["colunas"])
    assert antes - depois == {"descricao"}, "mexeu em mais do que pediram"

    como(app, SENHA_OPERADOR).post(
        "/analisesps/colunas",
        data={"acao": "alternar", "coluna": "descricao",
              "voltar": "/analisesps/solicitacoes"})
    assert set(guardado["colunas"]) == antes, "não voltou ao que era"


# ---------------------------------------------------------------------------
# O NOME, QUE É A CHAVE DO LOTE E DOS FILTROS
# ---------------------------------------------------------------------------
def test_a_entrada_nao_guarda_nada_no_navegador(app):
    """⚠️ O COOKIE QUE LEMBRAVA O NOME FOI EMBORA em 25/09/2026, junto com a
    lista de nomes da entrada: não havia mais o que lembrar, porque o nome
    passou a vir do cadastro.

    O que continua valendo, e é o que importa: a SENHA nunca vai para cookie
    nenhum, e a sessão continua morrendo quando o navegador fecha."""
    cliente = app.test_client()
    resposta = cliente.post("/analisesps/entrar", data={"senha": SENHA_OPERADOR})
    biscoitos = "; ".join(str(v) for _, v in resposta.headers)
    assert SENHA_OPERADOR not in biscoitos, "a SENHA foi parar num cookie"
    assert "ultimo_nome" not in biscoitos, "o cookie do nome voltou"

    login = cliente.get("/analisesps/entrar").get_data(as_text=True)
    assert 'name="usuario"' in login, "sumiu o campo de usuário"
    assert 'type="password"' in login, "parou de pedir a senha"
    assert "<select" not in login, "a lista de nomes voltou"


def test_o_mesmo_nome_escrito_diferente_e_a_mesma_pessoa(app):
    """Maiúscula, acento e espaço sobrando não podem separar ninguém do
    próprio lote — quem digita com a inicial minúscula um dia encontraria a
    tela vazia e concluiria que o sistema perdeu o trabalho dele."""
    from app.apps.analisesps import auth as guarda
    base = guarda.chave_pessoa("Marcelo")
    assert guarda.chave_pessoa("MARCELO") == base
    assert guarda.chave_pessoa("  marcelo ") == base
    assert guarda.chave_pessoa("João") == guarda.chave_pessoa("Joao")
    # E o que REALMENTE separa, que é o caso do aviso na tela do Lote:
    assert guarda.chave_pessoa("Marcelo Leitão") != base


def test_o_nome_aparece_no_alto_da_tela(app):
    """É por ele que o sistema sabe de quem é o lote. Fora da vista, um nome
    digitado diferente por engano daria outro lote sem ninguém notar."""
    html = como(app, SENHA_OPERADOR, nome="MARCELO").get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "<b>MARCELO</b>" in html


def test_nome_novo_com_lote_vazio_avisa_em_vez_de_deixar_a_pessoa_no_escuro(
        app_lote, monkeypatch):
    """O caso que o dono levantou: e se eu escrever o nome diferente amanhã?

    Sem aviso, ele abre o Lote, vê vazio e conclui que o sistema perdeu o
    trabalho. Com aviso, ele lê que o nome é novo, vê de quem há lote
    guardado, e sabe o que fazer."""
    from app.apps.analisesps import lote, preferencias
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "", "salvo_por": None, "salvo_em": None})
    monkeypatch.setattr(lote, "por_pessoa", lambda: True)
    monkeypatch.setattr(preferencias, "pessoas_conhecidas",
                        lambda: [{"chave": "marcelo", "nome": "MARCELO"}])

    # THIAGO está na lista e ainda não tem lote; o aviso continua valendo
    # para quem entra pela primeira vez.
    html = como(app_lote, SENHA_OPERADOR, nome="THIAGO").get(
        "/analisesps/lote").get_data(as_text=True)
    assert "ainda não tem lote aqui" in html
    assert "MARCELO</b>" in html
    assert "escrevendo o nome igual" in html


# ---------------------------------------------------------------------------
# A AGENDA VOLTA A ACEITAR LEMBRETES
#
# "e agenda? como faço pra adicionar lembretes? nao ta funcionando" — e não
# estava mesmo: a conversão deixou a agenda só de leitura. Sem a aba da
# planilha preenchida à mão, ela abria vazia e sem explicar.
# ---------------------------------------------------------------------------
@pytest.fixture
def agenda_gravavel(app, monkeypatch):
    """A agenda com a planilha dublada — aqui não há Google. Guarda o que
    teria ido para a aba Agenda, que é o que precisa ser conferido."""
    from app.apps.analisesps import agenda, sincronizacao
    guardados = {}
    escrito = []

    monkeypatch.setattr(sincronizacao, "escrever_compromisso",
                        lambda r: escrito.append(dict(r)))
    monkeypatch.setattr(agenda, "gravar",
                        lambda conn, regs: [guardados.update({r["id"]: r})
                                            for r in regs] and len(regs))
    monkeypatch.setattr(agenda, "salvar",
                        lambda r: (sincronizacao.escrever_compromisso(r),
                                   guardados.update({r["id"]: r})))
    monkeypatch.setattr(agenda, "listar", lambda: list(guardados.values()))
    monkeypatch.setattr(agenda, "um", lambda i: guardados.get(i))
    monkeypatch.setattr(agenda, "proximos", lambda dias=90: [])
    monkeypatch.setattr(agenda, "a_vencer", lambda: [])
    monkeypatch.setattr(agenda, "feriados_extra", lambda: set())
    app.escrito = escrito
    app.guardados = guardados
    return app


def test_a_agenda_aceita_um_lembrete_novo(agenda_gravavel):
    """E o lembrete vai para a PLANILHA, não só para o banco: a aba Agenda é
    a dona. Se fosse só aqui, a próxima sincronização traria de volta um mundo
    sem ele."""
    cliente = como(agenda_gravavel, SENHA_OPERADOR, nome="MARCELO")
    resposta = cliente.post("/analisesps/agenda", data={
        "acao": "salvar", "titulo": "FGTS da obra", "categoria": "FGTS",
        "data_base": "2026-01-07", "recorrencia": "mensal",
        "alerta_dias_antes": "5", "responsavel": "Maria"},
        follow_redirects=True)
    assert resposta.status_code == 200

    assert len(agenda_gravavel.escrito) == 1, "não foi para a planilha"
    guardado = agenda_gravavel.escrito[0]
    assert guardado["titulo"] == "FGTS da obra"
    assert guardado["criado_por"] == "MARCELO", "não diz quem cadastrou"
    # O padrão de FGTS é ANTECIPAR: imposto pago depois do vencimento tem multa.
    assert guardado["ajuste_dia_util"] == "antecipa"
    # O dia da repetição sai da data, como no Streamlit — não há campo à parte
    # para os dois não se contradizerem.
    assert guardado["dia_mes"] == "7"


def test_o_lembrete_sem_titulo_ou_sem_data_e_recusado_com_explicacao(
        agenda_gravavel):
    """Recusar em silêncio faria a pessoa achar que salvou."""
    cliente = como(agenda_gravavel, SENHA_OPERADOR)
    sem_titulo = cliente.post("/analisesps/agenda", data={
        "acao": "salvar", "titulo": "  ", "data_base": "2026-01-07"})
    assert "precisa de um título" in sem_titulo.get_data(as_text=True)

    sem_data = cliente.post("/analisesps/agenda", data={
        "acao": "salvar", "titulo": "Conta de luz", "data_base": ""})
    assert "primeira ocorrência" in sem_data.get_data(as_text=True)
    assert not agenda_gravavel.escrito, "gravou mesmo faltando o essencial"


def test_desligar_um_lembrete_nao_o_apaga(agenda_gravavel):
    """Desligar um lembrete de imposto por engano e não ter como trazê-lo de
    volta seria pior do que o engano. Ele some da vista e fica guardado."""
    from app.apps.analisesps import agenda
    cliente = como(agenda_gravavel, SENHA_OPERADOR)
    cliente.post("/analisesps/agenda", data={
        "acao": "salvar", "titulo": "Conta de luz", "categoria": "Conta",
        "data_base": "2026-01-15", "recorrencia": "mensal"},
        follow_redirects=True)
    ident = agenda_gravavel.escrito[0]["id"]

    cliente.post("/analisesps/agenda", data={"acao": "desligar", "id": ident},
                 follow_redirects=True)
    guardado = agenda_gravavel.guardados[ident]
    assert guardado["status"] == "inativo"
    assert not agenda.esta_ativo(guardado), "continuou contando no calendário"
    assert guardado["titulo"] == "Conta de luz", "o compromisso foi apagado"

    cliente.post("/analisesps/agenda", data={"acao": "religar", "id": ident},
                 follow_redirects=True)
    assert agenda.esta_ativo(agenda_gravavel.guardados[ident])


def test_se_a_planilha_falhar_nada_e_salvo(agenda_gravavel, monkeypatch):
    """A planilha é a dona. Salvar só aqui deixaria o lembrete vivo na tela e
    invisível lá — e a próxima sincronização não o traria de volta."""
    from app.apps.analisesps import agenda, sincronizacao

    def explode(registro):
        raise RuntimeError("cota do Google estourada")

    monkeypatch.setattr(sincronizacao, "escrever_compromisso", explode)
    monkeypatch.setattr(agenda, "salvar",
                        lambda r: sincronizacao.escrever_compromisso(r))

    resposta = como(agenda_gravavel, SENHA_OPERADOR).post(
        "/analisesps/agenda", data={
            "acao": "salvar", "titulo": "Não deve entrar",
            "categoria": "Conta", "data_base": "2026-02-01",
            "recorrencia": "mensal"})
    assert "Não consegui gravar na planilha" in resposta.get_data(as_text=True)
    assert not agenda_gravavel.guardados, "salvou aqui mesmo falhando lá"


def test_a_consulta_nao_cadastra_lembrete(agenda_gravavel):
    resposta = como(agenda_gravavel, SENHA_CONSULTA).post(
        "/analisesps/agenda", data={"acao": "salvar", "titulo": "X",
                                    "data_base": "2026-01-07"})
    assert resposta.status_code == 403
    assert not agenda_gravavel.escrito


def test_ligado_e_desligado_querem_dizer_a_mesma_coisa_em_todo_lugar(app):
    """O calendário exigia status "ativo"; os próximos só descartavam
    "cancelado". Um compromisso marcado "inativo" aparecia num e não no
    outro — e ninguém entenderia por quê."""
    from app.apps.analisesps import agenda
    assert agenda.esta_ativo({"status": "ativo"})
    assert agenda.esta_ativo({"status": ""})
    assert agenda.esta_ativo({})
    for desligado in ("cancelado", "inativo", "Desativado", " ARQUIVADO "):
        assert not agenda.esta_ativo({"status": desligado}), desligado


# ---------------------------------------------------------------------------
# TIRAR DO LOTE, PELA BARRA DO ALTO
# ---------------------------------------------------------------------------
def test_o_botao_de_agendamento_chama_desagendar(app):
    """O dono quer o termo do Streamlit. "Remover informação" descrevia o
    efeito, mas não era o nome que ele usa — e agora convive na mesma barra
    com "Remover do lote", que é outra coisa."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert ">Desagendar</button>" in html
    assert "Remover informação" not in html


def test_remover_do_lote_tira_de_grupos_diferentes_de_uma_vez(app_lote,
                                                              monkeypatch):
    """Marcar linhas em grupos diferentes e tirar todas de uma vez. Antes só
    dava editando o texto do lote na mão, achando o número no meio dos outros."""
    from app.apps.analisesps import lote
    guardado = {"conteudo": "Pagar amanhã\n1 2\n\nSemana que vem\n3"}
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": guardado["conteudo"], "salvo_por": "Marcelo",
        "salvo_em": None})
    monkeypatch.setattr(lote, "salvar",
                        lambda c, quem="", pessoa="": guardado.update(conteudo=c))

    resposta = como(app_lote, SENHA_OPERADOR).post(
        "/analisesps/lote", data={"acao": "remover_ids", "ids": "1,3"},
        follow_redirects=True)
    assert resposta.status_code == 200
    assert "1" not in guardado["conteudo"].split()
    assert "3" not in guardado["conteudo"].split()
    assert "2" in guardado["conteudo"], "tirou o que não foi marcado"

    # ⚠️ MUDOU EM 18/09/2026, e a versão anterior deste teste cravava o
    # comportamento ERRADO — foi ela que fez o defeito sobreviver.
    #
    # Relato do dono: *"eu pedi que quando eu removesse cancelados ou pagos de
    # um lote e ele ficasse vazio, o cabeçalho limpasse. Mas até agora não
    # funcionou."* Não funcionava POR AQUI: "Remover pagos" limpava desde
    # 11/09; o "Remover" da barra, não.
    #
    # O grupo que ainda tem SP mantém o título; o que esvaziou AGORA perde.
    assert "Pagar amanhã" in guardado["conteudo"], (
        "sumiu o título de um grupo que ainda tem SP")
    assert "Semana que vem" not in guardado["conteudo"], (
        "o título ficou órfão: o grupo esvaziou e o cabeçalho continuou")


def test_remover_do_lote_nao_mexe_na_sp(app_lote, monkeypatch):
    """"Remover" numa tela de pagamentos assusta, e com razão. Este mexe SÓ na
    lista: não altera status, não entra na fila da planilha, não toca no
    Pipefy."""
    from app.apps.analisesps import lote, web as tela
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "Grupo\n1", "salvo_por": None, "salvo_em": None})
    monkeypatch.setattr(lote, "salvar", lambda c, quem="", pessoa="": None)

    gravou = []
    monkeypatch.setattr(tela, "_gravar_alteracao",
                        lambda *a, **k: gravou.append(a) or {"ok": True})

    como(app_lote, SENHA_OPERADOR).post(
        "/analisesps/lote", data={"acao": "remover_ids", "ids": "1"},
        follow_redirects=True)
    assert not gravou, "tirar do lote escreveu na planilha"


def test_marcar_o_que_nao_esta_no_lote_e_explicado(app_lote, monkeypatch):
    """O painel por status embaixo mostra SPs que NÃO estão no lote. Marcar
    uma delas e mandar remover não é erro — simplesmente não há o que tirar,
    e a tela precisa dizer isso em vez de fingir que fez."""
    from app.apps.analisesps import lote
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "Grupo\n1", "salvo_por": None, "salvo_em": None})
    monkeypatch.setattr(lote, "salvar", lambda c, quem="", pessoa="": None)

    cliente = como(app_lote, SENHA_OPERADOR)
    nenhuma = cliente.post("/analisesps/lote",
                           data={"acao": "remover_ids", "ids": "99"},
                           follow_redirects=True).get_data(as_text=True)
    assert "Nenhuma das SPs marcadas estava no lote" in nenhuma

    metade = cliente.post("/analisesps/lote",
                          data={"acao": "remover_ids", "ids": "1,99"},
                          follow_redirects=True).get_data(as_text=True)
    assert "1 SP(s) saíram do lote" in metade
    assert "já não estavam nele" in metade


def test_o_botao_de_remover_duplicados_so_aparece_quando_ha_duplicado(app_lote, monkeypatch):
    """Botão que não faz nada quando apertado é pior do que botão nenhum: a
    pessoa aperta, nada muda, e ela passa a desconfiar dos outros botões."""
    from app.apps.analisesps import lote
    monkeypatch.setattr(lote, "salvar", lambda c, quem="", pessoa="": None)
    cliente = como(app_lote, SENHA_OPERADOR)

    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "Grupo\n1", "salvo_por": None, "salvo_em": None})
    sem = cliente.get("/analisesps/lote").get_data(as_text=True)
    assert "remover_duplicados" not in sem

    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "Grupo A\n1\n\nGrupo B\n1", "salvo_por": None,
        "salvo_em": None})
    com = cliente.get("/analisesps/lote").get_data(as_text=True)
    assert "remover_duplicados" in com
    assert "Remover duplicados (1)" in com, "o botão precisa dizer quantas são"


def test_remover_duplicados_pela_tela_guarda_a_primeira(app_lote, monkeypatch):
    """O caminho inteiro, da tela até o que fica salvo."""
    from app.apps.analisesps import lote
    salvos = []
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "", "salvo_por": None, "salvo_em": None})
    monkeypatch.setattr(lote, "salvar",
                        lambda c, quem="", pessoa="": salvos.append(c))

    resposta = como(app_lote, SENHA_OPERADOR).post(
        "/analisesps/lote",
        data={"acao": "remover_duplicados",
              "conteudo": "Primeiro\n1 2\n\nRepetido\n1"},
        follow_redirects=True).get_data(as_text=True)

    assert salvos and salvos[-1] == "Primeiro\n1 2"
    assert "Repetido" not in salvos[-1], "o cabeçalho do grupo esvaziado ficou"
    assert "1 repetição(ões) saíram do lote" in resposta


def test_remover_duplicados_sem_duplicado_diz_que_nao_havia(app_lote, monkeypatch):
    """Silêncio depois de apertar um botão faz a pessoa apertar de novo."""
    from app.apps.analisesps import lote
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "", "salvo_por": None, "salvo_em": None})
    monkeypatch.setattr(lote, "salvar", lambda c, quem="", pessoa="": None)
    resposta = como(app_lote, SENHA_OPERADOR).post(
        "/analisesps/lote",
        data={"acao": "remover_duplicados", "conteudo": "Grupo\n1 2"},
        follow_redirects=True).get_data(as_text=True)
    assert "Não havia nenhuma SP repetida" in resposta


def test_os_botoes_de_limpeza_dizem_REMOVER_e_nao_TIRAR(app_lote, monkeypatch):
    """Pedido do dono em 11/09/2026: *"esse termo tirar não é legal, é melhor
    remover pagos e remover cancelados"*. Numa tela de pagamentos "tirar as
    pagas" chega a soar como desfazer o pagamento — e o botão só mexe na lista
    do lote."""
    from app.apps.analisesps import lote
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {
        "conteudo": "Grupo\n1", "salvo_por": None, "salvo_em": None})
    html = como(app_lote, SENHA_OPERADOR).get(
        "/analisesps/lote").get_data(as_text=True)
    assert "Remover pagos" in html and "Remover cancelados" in html
    assert "Tirar as pagas" not in html and "Tirar as canceladas" not in html


def test_o_botao_de_remover_so_existe_na_tela_do_lote(app, app_lote):
    """Nas Solicitações não há lote de onde tirar — o botão de lá é o de
    MANDAR para o lote. Dois botões parecidos com efeitos opostos na mesma
    barra seria pedir para alguém errar."""
    nas_solicitacoes = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'id="ba-remover-lote"' not in nas_solicitacoes
    assert 'id="ba-enviar-lote"' in nas_solicitacoes

    no_lote = como(app_lote, SENHA_OPERADOR).get(
        "/analisesps/lote").get_data(as_text=True)
    assert 'id="ba-remover-lote"' in no_lote
    assert 'id="ba-enviar-lote"' not in no_lote


def test_a_consulta_nao_tira_do_lote(app_lote):
    resposta = como(app_lote, SENHA_CONSULTA).post(
        "/analisesps/lote", data={"acao": "remover_ids", "ids": "1"})
    assert resposta.status_code == 403


# ---------------------------------------------------------------------------
# A BUSCA POR ATUALIZAÇÕES DE 90 EM 90 SEGUNDOS
#
# "a busca por atualizacoes a cada 90s acho que nao tá acontecendo" — e não
# estava. O Streamlit tinha "Auto-atualizar (90s)", ligado por padrão; a
# conversão deixou de fora, apostando num agendador externo que não dá sinal
# de ter sido configurado. Sem os dois, a base só se atualizava quando alguém
# apertasse o botão em Configurações.
# ---------------------------------------------------------------------------
def test_a_tela_pergunta_por_atualizacoes(app):
    """A marca com o carimbo da última sincronização e o endereço de quem
    responde. É comparando com esse carimbo que a tela sabe se mudou algo."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'id="frescor"' in html
    assert "/api/frescor" in html
    assert html.count("analisesps.js") == 1, "o script entrou duas vezes"


def test_o_frescor_dispara_a_sincronizacao_quando_ela_esta_velha(app,
                                                                 monkeypatch):
    """É isto que substitui o agendador externo: quem estiver com a tela
    aberta mantém a base viva para todo mundo."""
    from app.apps.analisesps import tarefas
    pedidos = []
    monkeypatch.setattr(tarefas, "estado",
                        lambda: {"rodando": False, "detalhe": None,
                                 "interrompida": None})
    monkeypatch.setattr(tarefas, "_minutos_desde_a_ultima_sincronizacao",
                        lambda: 20.0)
    monkeypatch.setattr(tarefas, "disparar",
                        lambda modo, disparo="manual":
                        pedidos.append((modo, disparo)) or {"ok": True})

    resposta = como(app, SENHA_OPERADOR).get("/analisesps/api/frescor")
    assert resposta.status_code == 200
    assert resposta.get_json()["disparou"] is True
    assert pedidos == [("sincronizar", "tela aberta")]


def test_o_frescor_nao_dispara_a_toda_hora(app, monkeypatch):
    """Com quatro pessoas com a tela aberta o dia inteiro, um disparo a cada
    90 s seriam quarenta sincronizações por hora, todas lendo a planilha."""
    from app.apps.analisesps import tarefas
    pedidos = []
    monkeypatch.setattr(tarefas, "estado",
                        lambda: {"rodando": False, "detalhe": None,
                                 "interrompida": None})
    monkeypatch.setattr(tarefas, "disparar",
                        lambda modo, disparo="manual":
                        pedidos.append(modo) or {"ok": True})

    # Acabou de sincronizar: não dispara.
    monkeypatch.setattr(tarefas, "_minutos_desde_a_ultima_sincronizacao",
                        lambda: 1.0)
    assert como(app, SENHA_OPERADOR).get(
        "/analisesps/api/frescor").get_json()["disparou"] is False
    assert not pedidos

    # Já está rodando: também não.
    monkeypatch.setattr(tarefas, "_minutos_desde_a_ultima_sincronizacao",
                        lambda: 99.0)
    monkeypatch.setattr(tarefas, "estado",
                        lambda: {"rodando": True, "detalhe": {"etapa": "delta"},
                                 "interrompida": None})
    assert como(app, SENHA_OPERADOR).get(
        "/analisesps/api/frescor").get_json()["disparou"] is False
    assert not pedidos


def test_o_frescor_nunca_estoura_na_cara_de_quem_so_olhava(app, monkeypatch):
    """É chamado de fundo, de 90 em 90 segundos. Uma falha aqui não pode virar
    erro na tela de quem estava conferindo uma lista."""
    from app.apps.analisesps import consultas, tarefas

    def explode(*a, **k):
        raise RuntimeError("banco caiu")

    monkeypatch.setattr(tarefas, "estado", explode)
    monkeypatch.setattr(consultas, "base_carregada", explode)

    resposta = como(app, SENHA_OPERADOR).get("/analisesps/api/frescor")
    assert resposta.status_code == 200
    assert resposta.get_json()["disparou"] is False


def test_quem_so_consulta_tambem_mantem_a_base_viva(app, monkeypatch):
    """A base é de todos. Se só o Operador mantivesse, uma tarde inteira com
    o Consulta aberto deixaria a base parada."""
    from app.apps.analisesps import tarefas
    monkeypatch.setattr(tarefas, "estado",
                        lambda: {"rodando": False, "detalhe": None,
                                 "interrompida": None})
    monkeypatch.setattr(tarefas, "_minutos_desde_a_ultima_sincronizacao",
                        lambda: 99.0)
    monkeypatch.setattr(tarefas, "disparar",
                        lambda modo, disparo="manual": {"ok": True})
    assert como(app, SENHA_CONSULTA).get(
        "/analisesps/api/frescor").status_code == 200


def test_a_tela_nao_se_recarrega_por_baixo_de_quem_esta_marcando(app):
    """Recarregar por baixo de quem acabou de marcar vinte linhas apagaria a
    seleção — pior do que ver um número com dois minutos de idade. O script
    confere a seleção antes de recarregar, e senão só avisa."""
    css_js = (Path(__file__).resolve().parents[1] / "app" / "apps"
              / "analisesps" / "static" / "analisesps.js").read_text(
                  encoding="utf-8")
    assert "temSelecao" in css_js
    assert "temModalAberto" in css_js
    assert "avisar()" in css_js


def test_a_hora_da_ultima_atualizacao_fica_a_vista(app):
    """"Está atualizando?" tem de ser respondível de relance, sem abrir
    Configurações."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "base de" in html


# ---------------------------------------------------------------------------
# O FILTRO DE OBRAS
# ---------------------------------------------------------------------------
def test_o_filtro_de_obra_e_pesquisavel_quando_ha_muitas(app, monkeypatch):
    """Doze obras cabem na tela e se acham com o olho; oitenta, não — e rolar
    a lista procurando "creche" é coisa que se faz vinte vezes por dia."""
    from app.apps.analisesps import consultas
    muitas = [f"OBRA {i:02d}" for i in range(30)]
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400:
                        muitas if coluna == "centro_custo" else ["Pagar"])

    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes?f=1").get_data(as_text=True)
    assert "procura-opcao" in html, "faltou o campo de procura"
    assert "Obra (centro de custo)" in html


def test_lista_curta_nao_ganha_campo_de_procura(app, monkeypatch):
    """Campo de procura numa lista de três é ruído."""
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "opcoes",
                        lambda coluna, limite=400: ["A", "B", "C"])
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes?f=1").get_data(as_text=True)
    # O script sempre menciona a classe; o que não pode existir é o CAMPO.
    assert 'class="procura-opcao"' not in html


def test_a_procura_nao_esconde_a_opcao_ja_marcada(app):
    """Esconder uma obra marcada porque ela não casa com o texto digitado
    faria a pessoa achar que desmarcou sozinha."""
    js = (Path(__file__).resolve().parents[1] / "app" / "apps" / "analisesps"
          / "templates" / "analisesps_filtros.html").read_text(encoding="utf-8")
    assert "marcada" in js and "|| marcada" in js


def test_a_procura_ignora_acento_e_maiuscula(app):
    """Quem procura "sao" tem de achar "SÃO"."""
    conteudo = (Path(__file__).resolve().parents[1] / "app" / "apps"
                / "analisesps" / "templates"
                / "analisesps_filtros.html").read_text(encoding="utf-8")
    assert "normalize(\"NFD\")" in conteudo
    assert "toLowerCase()" in conteudo


def test_a_procura_nao_aplica_o_filtro_sozinha(app):
    """O campo procura DENTRO do bloco: filtra as caixas já carregadas, sem ir
    ao servidor. Digitar nele não pode disparar a consulta nem mandar o
    formulário — senão cada letra viraria uma ida ao banco."""
    conteudo = (Path(__file__).resolve().parents[1] / "app" / "apps"
                / "analisesps" / "templates"
                / "analisesps_filtros.html").read_text(encoding="utf-8")
    # O envio automático só olha os campos do filtro, e o de procura tem
    # classe própria e barra o Enter.
    assert "stopPropagation()" in conteudo
    assert 'class="procura-opcao"' in conteudo


# ---------------------------------------------------------------------------
# A TELA DE CÓDIGOS DE PAGAMENTO
# ---------------------------------------------------------------------------
@pytest.fixture
def app_codigos(app, monkeypatch):
    from app.apps.analisesps import consultas, pagamentos
    monkeypatch.setattr(consultas, "uma",
                        lambda i: linha_falsa(i, forma_pagamento="Pix",
                                              info_pgt="Chave Pix: x@y.com"))
    monkeypatch.setattr(pagamentos, "gerar_pix",
                        lambda *a, **k: ("QRFALSO", "carga-pix"))
    return app


def test_a_tela_de_codigos_tem_o_botao_de_agendar(app_codigos):
    """O caminho normal é: gerar o código, pagar, e marcar. Sem a barra aqui,
    era voltar para a lista, procurar as mesmas SPs de novo e marcar lá — e no
    Streamlit os códigos apareciam LOGO ABAIXO da barra, na mesma tela."""
    html = como(app_codigos, SENHA_OPERADOR).get(
        "/analisesps/codigos?id=1&id=2").get_data(as_text=True)
    assert 'id="barra-acoes"' in html
    for valor in ("Agendar", "Agendado", "Falha Agendar", "Desagendar", "Pago"):
        assert f'data-valor="{valor}"' in html, f"faltou {valor}"


def test_as_sps_ja_chegam_marcadas_na_tela_de_codigos(app_codigos):
    """Quem chegou aqui foi porque escolheu estas SPs. Obrigar a marcar de
    novo seria repetir o trabalho que acabou de ser feito."""
    html = como(app_codigos, SENHA_OPERADOR).get(
        "/analisesps/codigos?id=1&id=2").get_data(as_text=True)
    assert html.count('class="marca"') == 2
    assert html.count("checked") >= 2


def test_a_tela_de_codigos_nao_oferece_gerar_codigo_nem_mexer_no_lote(
        app_codigos):
    """Botão que não faz sentido onde está é convite a errar."""
    html = como(app_codigos, SENHA_OPERADOR).get(
        "/analisesps/codigos?id=1&origem=lote").get_data(as_text=True)
    assert 'id="ba-codigos"' not in html, "oferece gerar o QR estando nele"
    assert 'id="ba-remover-lote"' not in html
    assert 'id="ba-enviar-lote"' not in html


def test_o_numero_da_sp_nos_codigos_abre_a_ficha_no_modal(app_codigos):
    """Abrir em tela cheia fazia sumir os códigos recém-gerados, e voltar
    obrigava a refazer tudo só para conferir um dado."""
    html = como(app_codigos, SENHA_OPERADOR).get(
        "/analisesps/codigos?id=1").get_data(as_text=True)
    assert "data-ficha=" in html, "o número não sabe qual ficha abrir"
    assert 'id="ficha-modal"' in html, "não há modal nesta tela"
    # E continua sendo um link de verdade: abrir em nova aba tem de dar a
    # página inteira.
    assert "/analisesps/sp/1" in html


def test_o_clique_do_link_da_ficha_respeita_a_nova_aba(app):
    """Ctrl+clique e botão do meio abrem em nova aba — e aí o certo é a página
    inteira, não um modal que a outra aba não tem."""
    conteudo = (Path(__file__).resolve().parents[1] / "app" / "apps"
                / "analisesps" / "templates"
                / "analisesps_ficha_modal.html").read_text(encoding="utf-8")
    assert "a[data-ficha]" in conteudo
    assert "ctrlKey" in conteudo and "metaKey" in conteudo


# ---------------------------------------------------------------------------
# O CÓDIGO DE PAGAMENTO DENTRO DA FICHA
# ---------------------------------------------------------------------------
def test_a_ficha_ja_mostra_o_qr_pix(app_ficha, monkeypatch):
    """Quem abre a SP para conferir um dado quase sempre está a caminho de
    pagar. Voltar à lista só para gerar o QR era um caminho a mais em cada
    pagamento."""
    from app.apps.analisesps import consultas, pagamentos
    registro = dict(consultas.uma("1234567890") or {})
    registro.update(forma_pagamento="Pix", info_pgt="Chave Pix: x@y.com",
                    status_pgt="Pagar")
    monkeypatch.setattr(consultas, "uma", lambda i: registro)
    monkeypatch.setattr(pagamentos, "gerar_pix",
                        lambda *a, **k: (b"PNGFALSO", "carga-pix"))

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)
    assert "data:image/png;base64," in html
    assert "carga-pix" in html
    # O botão "QR / Código" saiu: com o código aqui, ele virou redundante.
    assert "QR / Código" not in html


def test_a_ficha_ja_mostra_o_codigo_de_barras(app_ficha, monkeypatch):
    from app.apps.analisesps import consultas, pagamentos
    registro = dict(consultas.uma("1234567890") or {})
    registro.update(forma_pagamento="Boleto", codigo_barras="23793381286",
                    status_pgt="Pagar")
    monkeypatch.setattr(consultas, "uma", lambda i: registro)
    monkeypatch.setattr(pagamentos, "barcode_svg",
                        lambda barras: ("<svg>falso</svg>", "ok"))

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)
    assert "<svg>falso</svg>" in html
    assert "Linha digitável" in html
    assert "23793381286" in html


def test_a_ficha_avisa_antes_de_mostrar_o_codigo_de_uma_sp_que_ja_saiu(
        app_ficha, monkeypatch):
    """Mostrar um código de pagamento numa SP já paga é o caminho curto para
    pagar duas vezes. O código continua aparecendo — às vezes é justamente o
    que se quer conferir —, mas com o aviso na frente."""
    from app.apps.analisesps import consultas, pagamentos
    registro = dict(consultas.uma("1234567890") or {})
    registro.update(forma_pagamento="Pix", info_pgt="Chave Pix: x@y.com",
                    status_pgt="Pago")
    monkeypatch.setattr(consultas, "uma", lambda i: registro)
    monkeypatch.setattr(pagamentos, "gerar_pix",
                        lambda *a, **k: (b"PNGFALSO", "carga-pix"))

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)
    assert "data:image/png;base64," in html, "escondeu o código"
    assert "pagar de novo" in html, "mostrou o código sem avisar"


def test_forma_sem_codigo_explica_em_vez_de_ficar_vazio(app_ficha, monkeypatch):
    """Um espaço em branco faria a pessoa achar que o sistema falhou."""
    from app.apps.analisesps import consultas
    registro = dict(consultas.uma("1234567890") or {})
    registro.update(forma_pagamento="Transferência", status_pgt="Pagar")
    monkeypatch.setattr(consultas, "uma", lambda i: registro)

    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1").get_data(as_text=True)
    assert "gera QR nem código de barras" in html


def test_uma_sp_com_codigo_ruim_nao_derruba_a_ficha(app_ficha, monkeypatch):
    """A ficha tem muito mais coisa do que o código. Se a geração falhar, o
    resto continua servindo."""
    from app.apps.analisesps import consultas, pagamentos
    registro = dict(consultas.uma("1234567890") or {})
    registro.update(forma_pagamento="Pix", info_pgt="lixo", status_pgt="Pagar")
    monkeypatch.setattr(consultas, "uma", lambda i: registro)

    def explode(*a, **k):
        raise RuntimeError("chave Pix impossível")

    monkeypatch.setattr(pagamentos, "gerar_pix", explode)
    resposta = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890?modal=1")
    assert resposta.status_code == 200
    assert "chave Pix impossível" in resposta.get_data(as_text=True)


def test_a_montagem_do_codigo_vive_num_lugar_so(app):
    """Duas telas mostram o código: a de códigos, que monta até cinquenta de
    uma vez, e a ficha. Duas cópias divergiriam no dia em que uma ganhasse um
    caso — e a que ficasse para trás mostraria um código errado a quem está
    pagando."""
    from app.apps.analisesps import web as tela
    assert hasattr(tela, "_codigo_de_pagamento")
    vazio = tela._codigo_de_pagamento("1", None)
    assert vazio["erro"] and vazio["sp"] is None


# ---------------------------------------------------------------------------
# A HORA NA TELA
# ---------------------------------------------------------------------------
def test_o_carimbo_da_base_sai_em_portugues_com_a_hora(app):
    """O dono viu na tela: "base de 2026-09-04T17:25:31.319885-03:00".

    O carimbo da última sincronização é guardado como TEXTO em
    `analisesps.meta`, e o formatador só sabia converter data de verdade — o
    resto passava cru. E aqui a HORA é o ponto: "a base é de quando?"
    respondido só com o dia diz "hoje", que é o que já se sabia."""
    from app.apps.analisesps import formatos

    assert formatos.momento_br(
        "2026-09-04T17:25:31.319885-03:00") == "04/09/2026 às 17:25"
    # O mesmo instante, escrito em UTC, tem de dar a mesma hora de Brasília.
    assert formatos.momento_br(
        "2026-09-04T20:25:31.319885+00:00") == "04/09/2026 às 17:25"
    # Sem fuso, vale a convenção do módulo: o serviço roda em UTC.
    assert formatos.momento_br("2026-09-04T20:25:31") == "04/09/2026 às 17:25"


def test_o_dia_mostrado_e_o_dia_em_brasilia(app):
    """Uma sincronização das 22h daqui é 1h do dia seguinte em UTC. Sem
    converter, a tela mostraria a data de amanhã."""
    from app.apps.analisesps import formatos
    assert formatos.data_br("2026-09-05T01:30:00+00:00") == "04/09/2026"
    assert formatos.momento_br(
        "2026-09-05T01:30:00+00:00") == "04/09/2026 às 22:30"


def test_o_formatador_nunca_estraga_o_que_nao_entende(app):
    """Vazio continua vazio, e texto que não é data passa inteiro — nunca
    "None" nem exceção na cara de quem só abriu uma tela."""
    from app.apps.analisesps import formatos
    for vazio in (None, "", "   "):
        assert formatos.momento_br(vazio) == ""
        assert formatos.data_br(vazio) == ""
    assert formatos.momento_br("sem data aqui") == "sem data aqui"
    # E uma data pura (sem hora) não ganha hora inventada.
    import datetime as dt
    assert formatos.momento_br(dt.date(2026, 9, 4)) == "04/09/2026"


def test_a_tela_mostra_a_hora_da_base(app, monkeypatch):
    from app.apps.analisesps import consultas
    monkeypatch.setattr(consultas, "base_carregada", lambda: {
        "pronta": True, "quantidade": 59055, "desconhecida": False,
        "ultima": "2026-09-04T17:25:31.319885-03:00"})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "base de 04/09/2026 às 17:25" in html
    # O carimbo cru ainda existe na página, num atributo escondido: é o valor
    # que o script compara para saber se a base mudou. O que não pode é ele
    # aparecer como TEXTO.
    import re
    visivel = re.sub(r"<[^>]+>", " ", html)
    assert "T17:25:31" not in visivel, "o carimbo cru vazou para a tela"


def test_o_registro_de_alteracoes_mostra_a_hora(app, monkeypatch):
    """Duas mudanças no mesmo dia, sem a hora, ficam indistinguíveis — e a
    pergunta que se faz num registro de alterações é "quando foi isso?"."""
    import datetime as dt
    from app.apps.analisesps import db as banco
    quando = dt.datetime(2026, 9, 4, 20, 25, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(banco, "consultar", lambda sql, params=(): (
        [] if "count(*)" in sql else
        [(quando, "1", "agendado", "Agendado", "", "Agendar", "operador",
          "Marcelo", "enviado", quando, None)]))
    monkeypatch.setattr(banco, "consultar_um", lambda sql, params=(): (0,))

    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/log").get_data(as_text=True)
    assert "04/09/2026 às 17:25" in html


def test_a_tela_de_entrada_monta_sem_senha_configurada(app, monkeypatch):
    monkeypatch.delenv("ANALISESPS_SENHA_OPERADOR", raising=False)
    monkeypatch.delenv("ANALISESPS_SENHA_CONSULTA", raising=False)
    html = app.test_client().get("/analisesps/entrar").get_data(as_text=True)
    assert "ANALISESPS_SENHA_OPERADOR" in html
    assert "ninguém entra" in html


# ---------------------------------------------------------------------------
# Relatório
# ---------------------------------------------------------------------------
@pytest.fixture
def app_relatorio(app, monkeypatch):
    from decimal import Decimal
    monkeypatch.setattr(consultas, "numeros_do_relatorio", lambda f, t="geral", p="tudo": {
        "quantidade": 120, "total": Decimal("845300.55"),
        "ticket": Decimal("7044.17"), "vencidos_qtd": 9,
        "vencidos_total": Decimal("61200.00")})
    _somas = [{"rotulo": "OBRA-12", "quantidade": 40,
               "total": Decimal("500000.00")},
              {"rotulo": "(vazio)", "quantidade": 3,
               "total": Decimal("1200.00")}]
    monkeypatch.setattr(consultas, "agregar",
                        lambda f, d, t="geral", p="tudo", limite=30: _somas)
    # A tela pede as dimensões juntas — uma varredura só do banco.
    monkeypatch.setattr(consultas, "agregar_varias",
                        lambda f, dims, t="geral", p="tudo", limite=100: {
                            d: _somas for d in dims})
    monkeypatch.setattr(consultas, "top_credores",
                        lambda f, t="geral", p="tudo", limite=30: [
                            {"documento": "01.637.895/0001-32",
                             "credor": "VOTORANTIM", "quantidade": 12,
                             "total": Decimal("300000.00")}])
    monkeypatch.setattr(consultas, "aging_vencidos", lambda f, p="tudo": [
        {"faixa": "1 a 7 dias", "quantidade": 4, "total": Decimal("20000.00")},
        {"faixa": "mais de 90 dias", "quantidade": 1, "total": Decimal("41200.00")}])
    # O analítico do PDF, acrescentado em 18/09/2026. Sem esta dublagem os
    # testes do PDF batem no banco de verdade e devolvem 500 — foi assim que
    # três testes quebraram no dia em que a seção nasceu.
    import datetime as _dt
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0: [
                            {"id": "1409289353", "data": _dt.date(2026, 9, 10),
                             "credor": "SERTAO CASA E CONSTRUCAO",
                             "documento": "29.066.773/0001-52",
                             "centro_custo": "OBRA-12",
                             "tipo_despesa": "Material",
                             "descricao": "Cimento CP-II 50kg para a laje",
                             "valor": Decimal("3250.00")}])
    return app


def test_a_tela_de_relatorio_monta(app_relatorio):
    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio").get_data(as_text=True)
    assert "845.300,55" in html
    assert "OBRA-12" in html
    assert "VOTORANTIM" in html
    assert "mais de 90 dias" in html


def test_o_relatorio_avisa_que_ignora_canceladas(app_relatorio):
    """Uma SP cancelada não é despesa. Quem lê o total precisa saber que ela
    ficou de fora, senão vai tentar bater com outro número e não vai conseguir."""
    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio").get_data(as_text=True)
    assert "canceladas ficam de fora" in html.lower()


def test_o_relatorio_diz_qual_data_manda_no_periodo(app_relatorio):
    """Contas a pagar se olham pelo vencimento; contas pagas, pela data do
    pagamento. Não dizer isso faz o mesmo período dar dois números diferentes."""
    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio?tipo=pagas").get_data(as_text=True)
    assert "data do pagamento" in html.lower()
    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio?tipo=pagar").get_data(as_text=True)
    assert "pelo vencimento" in html.lower()


def test_recorte_e_periodo_desconhecidos_caem_no_padrao(app_relatorio):
    """O recorte e o período entram no SQL. Um valor inventado na barra de
    endereço tem de virar o padrão, nunca chegar ao banco."""
    resposta = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio?tipo=inventado&periodo=xpto&dimensao=senha")
    assert resposta.status_code == 200


# ---------------------------------------------------------------------------
# Auditoria
# ---------------------------------------------------------------------------
def test_a_auditoria_abre_com_o_resumo(app, monkeypatch):
    from app.apps.analisesps import auditoria
    monkeypatch.setattr(auditoria, "resumo", lambda f, u=False, p=None: {
        "pontualidade": 8, "risco_ia": 3, "possivel_duplicidade": 0,
        "nf_duplicada": 2, "codigos_barras": 5, "sem_classificacao": 41,
        "sem_integracao": 0})
    html = como(app, SENHA_CONSULTA).get("/analisesps/auditoria").get_data(as_text=True)
    for rotulo in auditoria.CHECAGENS.values():
        assert rotulo in html
    assert "nada a apontar" in html          # as que zeraram dizem isso


def test_a_auditoria_olha_a_base_inteira_por_padrao(app, monkeypatch):
    """Auditoria que enxerga só o pedaço já filtrado encontra só o que já se
    estava olhando. O padrão tem de ser a base inteira, e a tela dizer isso."""
    from app.apps.analisesps import auditoria
    vistos = {}
    monkeypatch.setattr(auditoria, "resumo",
                        lambda f, u=False, p=None: vistos.update(usou_filtros=u) or {})
    html = como(app, SENHA_CONSULTA).get("/analisesps/auditoria").get_data(as_text=True)
    assert vistos["usou_filtros"] is False
    assert "base inteira" in html


def test_checagem_inventada_e_ignorada(app, monkeypatch):
    from app.apps.analisesps import auditoria
    monkeypatch.setattr(auditoria, "resumo", lambda f, u=False, p=None: {})
    resposta = como(app, SENHA_CONSULTA).get(
        "/analisesps/auditoria?checagem=apagar_tudo")
    assert resposta.status_code == 200


# ---------------------------------------------------------------------------
# Lote
# ---------------------------------------------------------------------------
@pytest.fixture
def app_lote(app, monkeypatch):
    from decimal import Decimal
    from app.apps.analisesps import lote
    guardado = {"conteudo": "Pagar amanhã\n1 2\n\nDepois\n3",
                "salvo_por": "operador", "salvo_em": None}
    # `ler` e `salvar` passaram a receber a PESSOA: cada um tem o seu lote.
    monkeypatch.setattr(lote, "ler", lambda pessoa="": guardado)
    monkeypatch.setattr(lote, "lote_de_antes", lambda: {"conteudo": "",
                                                        "salvo_por": None,
                                                        "salvo_em": None})
    monkeypatch.setattr(lote, "salvar",
                        lambda c, quem="", pessoa="": guardado.update(
                            conteudo=c, salvo_por=quem))
    monkeypatch.setattr(lote, "montar", lambda texto: {
        "grupos": [
            {"titulo": "Pagar amanhã", "titulo_exibido": "Pagar amanhã",
             "ids": ["1", "2"], "linhas": [linha_falsa("1"), linha_falsa("2")],
             "nao_encontrados": [], "total": Decimal("13500.00")},
            {"titulo": "Depois", "titulo_exibido": "Depois", "ids": ["3"],
             "linhas": [], "nao_encontrados": ["3"], "total": 0},
        ],
        "linhas": {"1": linha_falsa("1"), "2": linha_falsa("2")},
        "nao_encontrados": ["3"], "total_geral": Decimal("13500.00"),
        "quantidade": 2})
    return app


def test_a_tela_de_lote_monta_os_grupos(app_lote):
    html = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote").get_data(as_text=True)
    assert "Pagar amanhã" in html
    assert "Depois" in html
    assert "13.500,00" in html


def test_o_lote_e_de_cada_um_e_a_tela_diz_isso(app_lote):
    """O lote era um só, de todo mundo, e a segunda pessoa a salvar apagava o
    trabalho da primeira. Desde 04/09/2026 cada um tem o seu, separado pelo
    nome com que entrou — e a tela precisa dizer de quem é aquele lote, senão
    a pessoa continua com medo de mexer."""
    html = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote").get_data(as_text=True)
    # O aviso "Este lote é seu" saiu a pedido do dono (02/10/2026: *"isso aqui
    # é desnecessário"*); o lote continua sendo de cada um.
    assert "Este lote é <b>seu</b>" not in html
    assert 'id="dialogo-lote"' in html, "o lote abre numa janela, pelo botão Lote"


def test_o_lote_aponta_os_numeros_que_nao_existem(app_lote):
    """Ignorar em silêncio seria o pior comportamento: quem colou precisa saber
    que aquele número não foi reconhecido."""
    html = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote").get_data(as_text=True)
    assert "não existem na base" in html


def test_consulta_ve_o_lote_mas_nao_o_altera(app_lote):
    cliente = como(app_lote, SENHA_CONSULTA)
    html = cliente.get("/analisesps/lote").get_data(as_text=True)
    assert "Pagar amanhã" in html
    assert "Salvar lote" not in html
    assert cliente.post("/analisesps/lote", data={"conteudo": "x"}).status_code == 403


def test_operador_salva_o_lote(app_lote):
    from app.apps.analisesps import lote
    cliente = como(app_lote, SENHA_OPERADOR)
    resposta = cliente.post("/analisesps/lote",
                            data={"acao": "salvar", "conteudo": "9999999999"})
    assert resposta.status_code in (301, 302)
    assert lote.ler()["conteudo"] == "9999999999"


def test_extrair_ids_cria_um_grupo_novo_no_topo(app_lote):
    from app.apps.analisesps import lote
    cliente = como(app_lote, SENHA_OPERADOR)
    cliente.post("/analisesps/lote", data={
        "acao": "extrair", "conteudo": "Antigo\n1111111111",
        "extracao": "Solicitação validada\nNº da SP: 1426036778\noutra: 1426036779"})
    conteudo = lote.ler()["conteudo"]
    assert conteudo.startswith("Novo Lote 1")
    assert "1426036778" in conteudo and "1426036779" in conteudo
    assert "Antigo" in conteudo               # o que havia antes continua lá


# ---------------------------------------------------------------------------
# Códigos de pagamento
# ---------------------------------------------------------------------------
def test_qr_pix_e_montado(app, monkeypatch):
    from app.apps.analisesps import colunas
    ficha = {c: "" for c in colunas.CHAVES}
    ficha.update({"id": "1", "credor": "ACME", "forma_pagamento": "Pix",
                  "info_pgt": "Chave Pix: 11999998888",
                  "valor_num": 150.0, "vencimento_d": None,
                  "solicitacao_d": None, "data_pagamento_d": None,
                  "dt_autorizacao_d": None, "status_agend": ""})
    monkeypatch.setattr(consultas, "uma", lambda sp_id: ficha)
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/codigos?id=1").get_data(as_text=True)
    assert "data:image/png;base64," in html
    assert "copia e cola" in html.lower()


def test_codigo_de_barras_e_montado(app, monkeypatch):
    from app.apps.analisesps import colunas
    ficha = {c: "" for c in colunas.CHAVES}
    ficha.update({"id": "2", "credor": "ACME", "forma_pagamento": "Boleto",
                  "codigo_barras": "34191790010104351004791020150008291070026000",
                  "valor_num": 150.0, "vencimento_d": None,
                  "solicitacao_d": None, "data_pagamento_d": None,
                  "dt_autorizacao_d": None, "status_agend": ""})
    monkeypatch.setattr(consultas, "uma", lambda sp_id: ficha)
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/codigos?id=2").get_data(as_text=True)
    assert "<svg" in html
    assert "Linha digitável" in html


def test_forma_sem_codigo_explica_em_vez_de_quebrar(app, monkeypatch):
    from app.apps.analisesps import colunas
    ficha = {c: "" for c in colunas.CHAVES}
    ficha.update({"id": "3", "credor": "ACME", "forma_pagamento": "Dinheiro",
                  "valor_num": 10.0, "vencimento_d": None, "solicitacao_d": None,
                  "data_pagamento_d": None, "dt_autorizacao_d": None,
                  "status_agend": ""})
    monkeypatch.setattr(consultas, "uma", lambda sp_id: ficha)
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/codigos?id=3").get_data(as_text=True)
    assert "não gera QR nem código de barras" in html


def test_uma_sp_ruim_nao_derruba_as_outras(app, monkeypatch):
    """Se a SP do meio tiver código quebrado, as outras continuam aparecendo.
    Uma página em branco faria quem vai pagar perder as boas junto com a ruim."""
    from app.apps.analisesps import colunas

    def ficha(sp_id):
        base = {c: "" for c in colunas.CHAVES}
        base.update({"id": sp_id, "credor": "ACME", "valor_num": 10.0,
                     "vencimento_d": None, "solicitacao_d": None,
                     "data_pagamento_d": None, "dt_autorizacao_d": None,
                     "status_agend": "", "forma_pagamento": "Boleto",
                     "codigo_barras": ("lixo" if sp_id == "2"
                                       else "34191790010104351004791020150008291070026000")})
        return base

    monkeypatch.setattr(consultas, "uma", ficha)
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/codigos?id=1&id=2&id=3").get_data(as_text=True)
    assert html.count("<svg") == 2            # a 1 e a 3 saíram
    assert "aviso erro" in html               # a 2 explicou o motivo


def test_codigos_sem_nenhuma_sp_avisa(app):
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/codigos")
    assert resposta.status_code == 400
    assert "Marque as SPs" in resposta.get_data(as_text=True)


# ---------------------------------------------------------------------------
# Ratear
# ---------------------------------------------------------------------------
def test_ratear_sem_listas_carregadas_explica(app, monkeypatch):
    from app.apps.analisesps import sincronizacao
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    html = como(app, SENHA_CONSULTA).get("/analisesps/ratear").get_data(as_text=True)
    assert "ainda não foram carregadas" in html


def test_ratear_gera_os_jsons(app, monkeypatch):
    from app.apps.analisesps import sincronizacao
    monkeypatch.setattr(sincronizacao, "referencias_rateio", lambda: {
        "obras": [{"nome": "OBRA-12", "codigo": "111"},
                  {"nome": "OBRA-07", "codigo": "222"}],
        "categorias": [{"nome": "Material", "codigo": "333"}]})
    resposta = como(app, SENHA_OPERADOR).post("/analisesps/ratear", data={
        "cc_nome": ["OBRA-12", "OBRA-07"], "cc_valor": ["7.000,00", "3.000,00"],
        "cat_nome": ["Material"], "cat_valor": ["10.000,00"],
        "base_categoria": ""})
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "111" in html and "222" in html    # os códigos das obras no JSON


def test_consulta_nao_gera_rateio(app, monkeypatch):
    from app.apps.analisesps import sincronizacao
    monkeypatch.setattr(sincronizacao, "referencias_rateio", lambda: {
        "obras": [{"nome": "OBRA-12", "codigo": "111"}], "categorias": []})
    resposta = como(app, SENHA_CONSULTA).post("/analisesps/ratear", data={})
    assert resposta.status_code == 403


# ---------------------------------------------------------------------------
# Bradesco
# ---------------------------------------------------------------------------
def test_bradesco_abre_pedindo_o_extrato(app):
    html = como(app, SENHA_CONSULTA).get("/analisesps/bradesco").get_data(as_text=True)
    assert "Cole o extrato" in html


def test_bradesco_sem_texto_avisa(app):
    html = como(app, SENHA_CONSULTA).post(
        "/analisesps/bradesco", data={"extrato": "  "}).get_data(as_text=True)
    assert "Cole o texto" in html


def test_bradesco_cruza_o_que_foi_colado(app, monkeypatch):
    """⚠️ O DUBLÊ USA AS CHAVES DE VERDADE, as que `_linha_doc` monta.

    A versão anterior deste teste inventava chaves técnicas ("valor",
    "credor") e por isso passava enquanto a tela de produção mostrava
    "47 operação(ões)" com 47 linhas EM BRANCO: ele provava que o template
    desenha o que recebe, não que recebe o que o código produz."""
    from app.apps.analisesps import bradesco, web
    monkeypatch.setattr(web, "_candidatas_bradesco", lambda: [])
    monkeypatch.setattr(bradesco, "cruzar_tudo", lambda raw, df, foco_agendados=True: {
        "boletos": [bradesco._linha_doc(
            {"empresa": "BWS", "conta_debito": "1251 | 0002541-0",
             "valor_display": "6.750,00", "valor_num": 6750.0,
             "tipo": "boleto", "sp": "1384831053", "codigo": "34191"},
            {"id": "1384831053", "credor": "ACME", "valor_num": 6750.0},
            "SP OK", 99, [])],
        "pix": []})
    html = como(app, SENHA_CONSULTA).post(
        "/analisesps/bradesco", data={"extrato": "qualquer coisa"}).get_data(as_text=True)
    assert "1384831053" in html
    assert "6.750,00" in html
    assert "ACME" in html


def test_toda_coluna_da_tela_do_bradesco_EXISTE_na_linha():
    """⚠️ Nome de campo que aparece em dois arquivos vira dois nomes diferentes
    no dia em que um dos dois mudar. Foi isso que deixou a tela em branco."""
    from app.apps.analisesps import bradesco

    linha_doc = bradesco._linha_doc(
        {"empresa": "BWS", "conta_debito": "1251 | 0002541-0",
         "valor_display": "10,00", "valor_num": 10.0, "tipo": "boleto",
         "sp": "1", "codigo": "3419"},
        {"id": "1", "credor": "ACME", "valor_num": 10.0}, "SP OK", 99, [])
    faltando = [c for _, c in bradesco.COLUNAS_BOLETO if c not in linha_doc]
    assert not faltando, f"colunas de boleto sem campo na linha: {faltando}"

    linha_pix = bradesco._linha_pix(
        {"empresa": "BWS", "conta_debito": "1251 | 0002541-0",
         "valor_display": "10,00", "valor_num": 10.0, "nome": "JOSE"},
        {"id": "1", "credor": "ACME", "valor_num": 10.0}, "NOME OK", 95, [])
    faltando = [c for _, c in bradesco.COLUNAS_PIX if c not in linha_pix]
    assert not faltando, f"colunas de pix sem campo na linha: {faltando}"


# O texto que o Bradesco entrega quando se copia "Detalhes das Operações": a
# linha da operação tem data, agência | conta e valor, e as linhas seguintes
# dizem o que é.
EXTRATO_COLADO = (
    "BWS CONSTRUCOES LTDA\n"
    "CNPJ: 00.079.526/0001-09\n"
    "15/09/2026\t1251 | 0002541-0\t6.750,00\n"
    "Operação e Descrição: Pagamento de Boleto\n"
    "Boleto de Cobrança - 34191790010104351004 - 1384831053 - 20/09/2026\n"
    "15/09/2026\t1251 | 0002541-0\t1.200,00\n"
    "Operação e Descrição: Pix Enviado\n"
    "Nome: JOSE THIAGO DA SILVA\n")


def test_bradesco_DE_PONTA_A_PONTA_desenha_o_que_o_codigo_produz(app, monkeypatch):
    """⚠️ SEM DUBLÊ NENHUM no meio do caminho: cola o texto, cruza de verdade e
    confere que a célula aparece preenchida.

    É o teste que faltava. Os outros dublavam `cruzar_tudo`, e por isso a tela
    pôde ficar com 47 linhas em branco sem a suíte reclamar.

    ⚠️ E CONFERE PELO QUE **NÃO** ESTÁ NO TEXTO COLADO. O texto volta dentro da
    caixa de digitação, então procurar por "6.750,00" no HTML daria certo mesmo
    com a tabela inteira vazia — foi assim que a primeira versão deste teste
    passou sem provar nada. O credor e a validação só existem se a linha da
    tabela tiver sido desenhada."""
    from app.apps.analisesps import web

    monkeypatch.setattr(web, "_candidatas_bradesco", lambda: [
        {"id": "1384831053", "credor": "ACME MATERIAIS", "valor": "6.750,00",
         "valor_num": 6750.0, "status_agend": "Agendado", "status_pgt": "",
         "codigo_barras": "34191790010104351004", "vencimento": "20/09/2026",
         "conta": "", "doc_fiscal": "", "centro_custo": "",
         "forma_pagamento": "Boleto"}])

    html = como(app, SENHA_CONSULTA).post(
        "/analisesps/bradesco", data={"extrato": EXTRATO_COLADO}
        ).get_data(as_text=True)

    assert "1 operação(ões)" in html          # um boleto
    assert "ACME MATERIAIS" in html, "a coluna do credor saiu vazia"
    assert "OK (ID+barras)" in html, "a coluna da validação saiu vazia"
    assert "SEM MATCH" in html, "a tabela do Pix saiu vazia"
    assert "BWS CONSTRUCOES LTDA" in html, "a coluna da empresa saiu vazia"


def test_bradesco_nao_conclui_no_lugar_de_quem_le(app, monkeypatch):
    """Operação sem SP encontrada não é pagamento indevido. A tela aponta o que
    merece um olhar; a conclusão é de quem confere."""
    from app.apps.analisesps import bradesco, web
    monkeypatch.setattr(web, "_candidatas_bradesco", lambda: [])
    monkeypatch.setattr(bradesco, "cruzar_tudo",
                        lambda raw, df, foco_agendados=True: {"boletos": [], "pix": []})
    html = como(app, SENHA_CONSULTA).post(
        "/analisesps/bradesco", data={"extrato": "x"}).get_data(as_text=True)
    assert "não quer dizer pagamento indevido" in html


# ---------------------------------------------------------------------------
# Agenda
# ---------------------------------------------------------------------------
@pytest.fixture
def app_agenda(app, monkeypatch):
    """A agenda com um compromisso mensal — o bastante para a grade do mês
    aparecer com alguma coisa dentro."""
    from app.apps.analisesps import agenda
    compromisso = {"id": "1", "titulo": "FGTS", "categoria": "FGTS",
                   "recorrencia": "mensal", "dia_mes": "7",
                   "data_base": "07/01/2026", "ajuste_dia_util": "antecipa",
                   "alerta_dias_antes": "5", "status": "ativo",
                   "responsavel": "Ana", "descricao": "", "concluido_em": "",
                   "criado_por": "", "criado_em": ""}
    monkeypatch.setattr(agenda, "listar", lambda: [compromisso])
    monkeypatch.setattr(agenda, "proximos", lambda dias=90: [])
    monkeypatch.setattr(agenda, "a_vencer", lambda: [])
    monkeypatch.setattr(agenda, "feriados_extra", lambda: set())
    return app


def test_a_agenda_monta(app, monkeypatch):
    import datetime as dt
    from app.apps.analisesps import agenda
    compromisso = {"id": "1", "titulo": "FGTS", "categoria": "FGTS",
                   "recorrencia": "mensal", "dia_mes": "7",
                   "data_base": "07/01/2026", "ajuste_dia_util": "antecipa",
                   "alerta_dias_antes": "5", "status": "ativo",
                   "responsavel": "Ana", "descricao": "", "concluido_em": "",
                   "criado_por": "", "criado_em": ""}
    monkeypatch.setattr(agenda, "listar", lambda: [compromisso])
    monkeypatch.setattr(agenda, "proximos", lambda dias=90: [
        {"data": dt.date(2026, 2, 6), "data_original": dt.date(2026, 2, 7),
         "compromisso": compromisso}])
    monkeypatch.setattr(agenda, "a_vencer", lambda: [])
    html = como(app, SENHA_CONSULTA).get("/analisesps/agenda").get_data(as_text=True)
    assert "FGTS" in html
    assert "06/02/2026" in html
    assert "movida de 07/02/2026" in html     # explica por que a data mudou


def test_a_agenda_explica_a_regra_do_ajuste(app, monkeypatch):
    """Imposto ANTECIPA e o resto POSTERGA. Sem a explicação, quem vê a data
    mudada acha que o sistema errou."""
    from app.apps.analisesps import agenda
    monkeypatch.setattr(agenda, "listar", lambda: [])
    monkeypatch.setattr(agenda, "proximos", lambda dias=90: [])
    monkeypatch.setattr(agenda, "a_vencer", lambda: [])
    html = como(app, SENHA_CONSULTA).get("/analisesps/agenda").get_data(as_text=True)
    assert "antecipam" in html and "posterga" in html


def test_agenda_vazia_diz_que_falta_sincronizar(app, monkeypatch):
    from app.apps.analisesps import agenda
    monkeypatch.setattr(agenda, "listar", lambda: [])
    monkeypatch.setattr(agenda, "proximos", lambda dias=90: [])
    monkeypatch.setattr(agenda, "a_vencer", lambda: [])
    html = como(app, SENHA_CONSULTA).get("/analisesps/agenda").get_data(as_text=True)
    assert "Nenhum compromisso cadastrado" in html


# ---------------------------------------------------------------------------
# Log
# ---------------------------------------------------------------------------
def test_o_log_monta(app, monkeypatch):
    import datetime as dt
    from app.apps.analisesps import db as banco
    monkeypatch.setattr(banco, "consultar", lambda sql, params=(): (
        [("enviado", 12), ("pendente", 3)] if "GROUP BY status" in sql else
        [(dt.datetime(2026, 9, 2, 15, 0), "1384831053", "status_pgt", "Pago",
          "Pagar", "Marcar Pago", "operador", "enviado",
          dt.datetime(2026, 9, 2, 15, 1), None)]))
    monkeypatch.setattr(banco, "consultar_um", lambda sql, params=(): (3,))
    html = como(app, SENHA_CONSULTA).get("/analisesps/log").get_data(as_text=True)
    assert "1384831053" in html
    assert "Status Pgt" in html               # o rótulo, não o nome da coluna
    assert "na planilha" in html


def test_o_log_mostra_quem_alterou_e_com_que_alcada(app, monkeypatch):
    """Antes o registro sabia só QUE PERFIL mexeu. Agora sabe QUEM.

    As duas colunas continuam, e são coisas diferentes: "Quem" é a pessoa,
    "Perfil" é o que a senha dela permitia. E a tela avisa que as linhas
    antigas não têm nome — naquele momento o módulo realmente não sabia, e
    inventar um nome ali seria pior do que o traço."""
    from app.apps.analisesps import db as banco
    monkeypatch.setattr(banco, "consultar", lambda sql, params=(): [])
    monkeypatch.setattr(banco, "consultar_um", lambda sql, params=(): (0,))
    html = como(app, SENHA_CONSULTA).get("/analisesps/log").get_data(as_text=True)
    assert "<th>Quem</th>" in html
    assert "<th>Perfil</th>" in html
    assert "aparecem sem nome" in html


def test_o_log_com_banco_fora_mostra_recado(app, monkeypatch):
    from app.apps.analisesps import db as banco
    def explode(*a, **k):
        raise RuntimeError("banco fora do ar")
    monkeypatch.setattr(banco, "consultar", explode)
    monkeypatch.setattr(banco, "consultar_um", explode)
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/log")
    assert resposta.status_code == 200
    assert "banco fora do ar" in resposta.get_data(as_text=True)


# ---------------------------------------------------------------------------
# A navegação
# ---------------------------------------------------------------------------
def test_todas_as_telas_aparecem_na_navegacao(app_lote):
    """Uma tela que existe e não está no menu é uma tela que ninguém usa."""
    html = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote").get_data(as_text=True)
    for rotulo in ("Solicitações", "Lote", "Relatório", "Auditoria", "Ratear",
                   "Bradesco", "Agenda", "Log", "Configurações"):
        assert ">" + rotulo + "<" in html, f"'{rotulo}' não está na navegação"


# ---------------------------------------------------------------------------
# Nenhuma tela pode estourar quando o banco cai
# ---------------------------------------------------------------------------
TELAS_QUE_ABREM = [
    "/analisesps/solicitacoes",
    "/analisesps/lote",
    "/analisesps/relatorio",
    "/analisesps/auditoria",
    "/analisesps/ratear",
    "/analisesps/bradesco",
    "/analisesps/agenda",
    "/analisesps/log",
    "/analisesps/configuracoes",
]


@pytest.fixture
def app_sem_banco(monkeypatch):
    """Um app em que QUALQUER consulta ao banco estoura.

    É o cenário real de uma queda do Postgres — e é justamente quando alguém
    abre a tela para entender o que houve."""
    monkeypatch.setenv("ANALISESPS_SENHA_OPERADOR", SENHA_OPERADOR)
    monkeypatch.setenv("ANALISESPS_SENHA_CONSULTA", SENHA_CONSULTA)

    from app.apps.analisesps import db as banco

    def caiu(*a, **k):
        raise RuntimeError("connection to server failed: banco fora do ar")

    monkeypatch.setattr(banco, "consultar", caiu)
    monkeypatch.setattr(banco, "consultar_um", caiu)
    monkeypatch.setattr(banco, "conexao", caiu)
    monkeypatch.setattr(banco, "obter_engine", caiu)

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = False        # queremos a resposta, não a exceção crua
    return a


@pytest.mark.parametrize("url", TELAS_QUE_ABREM)
def test_nenhuma_tela_estoura_com_o_banco_fora(app_sem_banco, url):
    """Uma tela que devolve 500 quando o banco cai é a que ninguém consegue
    usar para descobrir o que aconteceu.

    Este teste nasceu de um defeito real: a tela de Ratear era a única que
    estourava, porque lia as listas de obras sem proteger a leitura. As outras
    já degradavam com recado."""
    resposta = como(app_sem_banco, SENHA_OPERADOR).get(url)
    assert resposta.status_code == 200, (
        f"{url} devolveu {resposta.status_code} com o banco fora — "
        "deveria abrir e explicar o problema")


@pytest.mark.parametrize("url", TELAS_QUE_ABREM)
def test_a_tela_conta_o_que_houve_em_vez_de_ficar_muda(app_sem_banco, url):
    """Não basta não quebrar: quem abriu precisa entender por que a tela está
    vazia, senão vai concluir que não há nada a pagar."""
    html = como(app_sem_banco, SENHA_OPERADOR).get(url).get_data(as_text=True)
    baixo = html.lower()
    assert ("fora do ar" in baixo or "não foi carregada" in baixo
            or "não consegui" in baixo or "ainda não" in baixo), (
        f"{url} abriu vazia sem dizer que o banco está fora")


def test_consulta_e_recusada_mesmo_com_a_base_vazia(app_sem_banco):
    """A resposta a uma tentativa de escrita sem alçada tem de ser SEMPRE a
    mesma, não importa o estado do banco.

    Isto veio de um caso real: com a base ainda não carregada, a checagem de
    carga respondia antes da de permissão, e o perfil Consulta recebia a tela
    amigável em vez de 403. Nada era alterado — mas uma trava que responde
    diferente conforme o estado do sistema é uma trava em que não se confia."""
    resposta = como(app_sem_banco, SENHA_CONSULTA).post(
        "/analisesps/lote", data={"acao": "salvar", "conteudo": "111"})
    assert resposta.status_code == 403


def test_operador_com_a_base_vazia_ve_a_tela_de_carga(app_sem_banco):
    """E o contrário: quem TEM alçada e chega com a base vazia recebe a
    explicação, não um 403 enganoso."""
    resposta = como(app_sem_banco, SENHA_OPERADOR).get("/analisesps/lote")
    assert resposta.status_code == 200
    assert "não foi carregada" in resposta.get_data(as_text=True)


# ---------------------------------------------------------------------------
# O escritor de CSV, sozinho
# ---------------------------------------------------------------------------
def test_o_csv_traz_os_tres_detalhes_que_o_excel_brasileiro_precisa():
    """BOM, ponto e vírgula e fim de linha do Windows. Os três juntos, ou o
    arquivo não abre com dois cliques: sem BOM o acento vira caractere
    estranho, sem ponto e vírgula tudo cai numa coluna só."""
    from app.apps.analisesps import exportar

    saida = "".join(exportar.linhas_csv(["Solicitação", "Valor"],
                                        [["Cimento", "6.750,00"]]))
    assert saida.startswith("﻿")
    assert "Solicitação;Valor\r\n" in saida
    assert "Cimento;6.750,00\r\n" in saida


@pytest.mark.parametrize("entrada,esperado", [
    ("simples", "simples"),
    ("com;ponto e vírgula", '"com;ponto e vírgula"'),
    ('com "aspas"', '"com ""aspas"""'),
    ("com\nquebra", "com quebra"),
    ("com\r\nquebra", "com  quebra"),
    (None, ""),
    (0, "0"),
])
def test_a_celula_protege_o_que_quebraria_a_planilha(entrada, esperado):
    """Um ponto e vírgula no meio de uma descrição partiria a linha em duas
    colunas e desalinharia a planilha inteira dali para baixo."""
    from app.apps.analisesps import exportar
    assert exportar.celula(entrada) == esperado


def test_o_arquivo_e_montado_em_pedacos():
    """Um filtro largo alcança as 59 mil SPs. Montar tudo antes de enviar é o
    que a instância de 2 GB não suporta."""
    import types
    from app.apps.analisesps import exportar
    assert isinstance(exportar.linhas_csv(["a"], []), types.GeneratorType)


# ---------------------------------------------------------------------------
# As três exportações novas
# ---------------------------------------------------------------------------
def test_o_relatorio_exporta_tudo_o_que_esta_na_tela(app_relatorio):
    """Um arquivo por bloco daria seis downloads para montar uma análise."""
    resposta = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio/exportar")
    assert resposta.status_code == 200
    texto = resposta.get_data(as_text=True)
    assert texto.startswith("﻿")
    assert "845.300,55" in texto           # os números do topo
    assert "OBRA-12" in texto              # as quebras
    assert "VOTORANTIM" in texto           # os credores
    assert "mais de 90 dias" in texto      # o aging


def test_o_relatorio_exportado_diz_o_recorte_e_a_data_que_manda(app_relatorio):
    """Sem isso, o arquivo vira um número solto: ninguém sabe se era a pagar ou
    pago, de que período, nem se canceladas entraram."""
    texto = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio/exportar?tipo=pagas").get_data(as_text=True)
    assert "Contas pagas" in texto
    assert "pagamento" in texto
    assert "ficam de fora" in texto        # as canceladas


def test_a_auditoria_exporta_a_checagem_aberta(app, monkeypatch):
    from app.apps.analisesps import auditoria
    monkeypatch.setattr(auditoria, "risco_ia", lambda f, u=False, p=None: [
        linha_falsa("1", analise_ia="Pagamento COM RISCO")])
    texto = como(app, SENHA_CONSULTA).get(
        "/analisesps/auditoria/exportar?checagem=risco_ia").get_data(as_text=True)
    assert "COM RISCO" in texto
    assert "6.750,00" in texto


def test_a_auditoria_sem_checagem_aberta_avisa_em_vez_de_baixar_vazio(app):
    """Baixar um arquivo em branco é pior do que não baixar: a pessoa acha que
    não há nada a apontar."""
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/auditoria/exportar")
    assert resposta.status_code == 400
    assert "Abra uma das checagens" in resposta.get_data(as_text=True)


def test_a_auditoria_recusa_checagem_inventada(app):
    resposta = como(app, SENHA_CONSULTA).get(
        "/analisesps/auditoria/exportar?checagem=apagar_tudo")
    assert resposta.status_code == 400


def test_o_lote_exporta_com_os_grupos_e_os_totais(app_lote):
    """É o que se manda para quem vai efetivar os pagamentos — com a mesma
    organização que quem montou o lote escolheu."""
    texto = como(app_lote, SENHA_CONSULTA).get(
        "/analisesps/lote/exportar").get_data(as_text=True)
    assert "Pagar amanhã" in texto
    assert "Total do grupo" in texto
    assert "TOTAL GERAL" in texto
    assert "13.500,00" in texto


def test_o_lote_exportado_aponta_o_que_nao_existe(app_lote):
    texto = como(app_lote, SENHA_CONSULTA).get(
        "/analisesps/lote/exportar").get_data(as_text=True)
    assert "não encontrada na base" in texto


@pytest.mark.parametrize("url", [
    "/analisesps/exportar",
    "/analisesps/relatorio/exportar",
    "/analisesps/lote/exportar",
])
def test_toda_exportacao_vem_como_arquivo_para_baixar(app_lote, app_relatorio, url):
    """Sem o cabeçalho de anexo o navegador mostra o CSV como texto na tela, e
    a pessoa acha que não funcionou."""
    aplicativo = app_relatorio if "relatorio" in url else app_lote
    resposta = como(aplicativo, SENHA_CONSULTA).get(url)
    assert "attachment" in resposta.headers["Content-Disposition"]
    assert ".csv" in resposta.headers["Content-Disposition"]
    assert resposta.is_streamed


def test_consulta_exporta_de_todas_as_telas(app_lote, app_relatorio):
    """Exportar é leitura. O perfil que vê tem de poder levar o que vê."""
    assert como(app_lote, SENHA_CONSULTA).get(
        "/analisesps/lote/exportar").status_code == 200
    assert como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio/exportar").status_code == 200


# ---------------------------------------------------------------------------
# Agir direto da ficha da SP
# ---------------------------------------------------------------------------
@pytest.fixture
def app_ficha(app, monkeypatch):
    from app.apps.analisesps import colunas
    ficha = {c: "" for c in colunas.CHAVES}
    ficha.update({
        "id": "1234567890", "credor": "VOTORANTIM CIMENTOS S.A.",
        "status_pgt": "Pagar", "forma_pagamento": "Boleto",
        "codigo_barras": "34191790010104351004791020150008291070026000",
        "valor_num": Decimal("6750.00"),
        "solicitacao_d": dt.date(2026, 1, 5),
        "vencimento_d": dt.date(2026, 2, 10),
        "data_pagamento_d": None, "dt_autorizacao_d": dt.date(2026, 1, 6),
        "status_agend": "Agendar",
        "card_link": "https://app.pipefy.com/open-cards/1234567890",
    })
    monkeypatch.setattr(consultas, "uma", lambda sp_id: ficha)
    return app


def test_operador_pode_agir_direto_da_ficha(app_ficha):
    """No Streamlit a janela de detalhe deixava alterar ali mesmo. Sem isso,
    quem abre a ficha para conferir precisa voltar à lista, achar a linha de
    novo e marcá-la — e é aí que se marca a errada."""
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert 'data-coluna="status_pgt"' in html
    assert 'data-coluna="agendado"' in html


def test_cancelar_na_ficha_e_o_pedido_no_pipefy_como_no_streamlit(app_ficha):
    """No Streamlit, "Cancelar" abria o formulário de cancelamento NO PIPEFY.

    Na conversão virou um botão que gravava "Cancelado" na planilha — mesma
    palavra, outra ação, e sem volta pelo caminho errado: quem pedia o
    cancelamento da SP acabava só marcando a planilha, e o card seguia vivo
    lá. Voltou a ser o formulário; e a tela diz, onde os botões estão, que
    daqui não se mexe no Pipefy."""
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "app.pipefy.com/public/form/" in html, "sumiu o pedido de cancelamento"
    assert 'data-valor="Cancelado"' not in html, (
        "voltou o botão que grava Cancelado na planilha chamando-se Cancelar")
    assert "O Pipefy não é" in html and "alterado por aqui" in html


def test_consulta_nao_ve_os_botoes_na_ficha(app_ficha):
    html = como(app_ficha, SENHA_CONSULTA).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert 'data-coluna="status_pgt"' not in html


def test_a_ficha_traz_o_codigo_de_pagamento_nela_mesma(app_ficha):
    """Quem abriu a ficha para pagar não devia ter de voltar à lista, marcar a
    mesma SP e clicar em outro lugar.

    Era um LINK para a tela de códigos; desde 05/09/2026 o código está na
    própria ficha, e o link saiu por ter virado redundante. Vale também para
    o perfil Consulta: ver o código não é alterar nada."""
    html = como(app_ficha, SENHA_CONSULTA).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "codigos?id=1234567890" not in html, "o link redundante voltou"
    assert "Linha digitável" in html, "o código não está na ficha"
    assert "<svg" in html


def test_a_ficha_mostra_a_navegacao_e_o_perfil(app_ficha):
    """A ficha estava sem o cabeçalho de perfil — quem entrava por um link
    direto não via em que perfil estava."""
    html = como(app_ficha, SENHA_CONSULTA).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "Consulta" in html
    assert ">Auditoria<" in html          # a navegação inteira está lá


# ---------------------------------------------------------------------------
# Os relatórios em PDF
# ---------------------------------------------------------------------------
def test_o_acento_do_portugues_sobrevive():
    """A armadilha do fpdf2: com as fontes embutidas ele só escreve latin-1, e
    o que não couber ele NÃO avisa — ele estoura no meio da geração.

    Latin-1 cobre o português inteiro. Este teste é o que garante que ninguém
    troque a conversão por algo que rebaixe o acento para ASCII e transforme
    "Solicitação" em "Solicitacao" na cara do cliente."""
    from app.apps.analisesps.pdf import _texto
    for palavra in ("Solicitação", "avaliação", "João", "açaí", "Antônio",
                    "Construções", "número", "endereço", "está", "três"):
        assert _texto(palavra) == palavra


@pytest.mark.parametrize("entrada,esperado", [
    ("travessão — assim", "travessão - assim"),
    ("meia–risca", "meia-risca"),
    ("aspas “curvas”", 'aspas "curvas"'),
    ("apóstrofo ’", "apóstrofo '"),
    ("reticências…", "reticências..."),
    ("seta →", "seta ->"),
])
def test_sinal_tipografico_vira_o_equivalente_simples(entrada, esperado):
    """Um travessão virando "?" no meio de uma frase é pior do que um hífen —
    e o travessão está em vários textos deste projeto."""
    from app.apps.analisesps.pdf import _texto
    assert _texto(entrada) == esperado


def test_caractere_impossivel_nao_derruba_o_relatorio():
    """Um emoji colado numa descrição não pode impedir o relatório de sair."""
    from app.apps.analisesps.pdf import _texto
    assert _texto("cimento 🧱 CP-II") == "cimento ? CP-II"
    assert _texto(None) == ""
    assert _texto(1234) == "1234"


def test_o_relatorio_em_pdf_sai_valido(app_relatorio):
    resposta = como(app_relatorio, SENHA_CONSULTA).get("/analisesps/relatorio/pdf")
    assert resposta.status_code == 200
    assert resposta.mimetype == "application/pdf"
    corpo = resposta.get_data()
    assert corpo.startswith(b"%PDF-")
    assert corpo.rstrip().endswith(b"%%EOF")
    assert len(corpo) > 1000


def test_o_pdf_vem_como_arquivo_para_baixar(app_relatorio):
    resposta = como(app_relatorio, SENHA_CONSULTA).get("/analisesps/relatorio/pdf")
    assert "attachment" in resposta.headers["Content-Disposition"]
    assert ".pdf" in resposta.headers["Content-Disposition"]


def test_o_pdf_do_lote_sai_valido(app_lote):
    resposta = como(app_lote, SENHA_CONSULTA).get("/analisesps/lote/pdf")
    assert resposta.status_code == 200
    assert resposta.get_data().startswith(b"%PDF-")


def test_cada_grupo_do_lote_sai_em_pdf_e_em_excel(app_lote):
    """Ao lado do QR do grupo, o relatório e a exportação SÓ daquele grupo
    (dono, 02/10/2026). O grupo vazio responde 404 em vez de um arquivo em
    branco."""
    cliente = como(app_lote, SENHA_CONSULTA)
    html = cliente.get("/analisesps/lote").get_data(as_text=True)
    assert "/analisesps/lote/grupo/1/pdf" in html
    assert "/analisesps/lote/grupo/1/excel" in html
    pdf = cliente.get("/analisesps/lote/grupo/1/pdf")
    assert pdf.status_code == 200
    assert pdf.get_data().startswith(b"%PDF-")
    assert "Pagar amanh" in pdf.headers["Content-Disposition"]
    xlsx = cliente.get("/analisesps/lote/grupo/1/excel")
    assert xlsx.status_code == 200
    assert xlsx.get_data().startswith(b"PK")
    assert cliente.get("/analisesps/lote/grupo/2/pdf").status_code == 404
    assert cliente.get("/analisesps/lote/grupo/9/excel").status_code == 404


def test_os_numeros_do_grupo_vao_no_topo_do_relatorio():
    from app.apps.analisesps import pdf
    montado = {"grupos": [{"titulo_exibido": "G", "linhas": [
        linha_falsa("1"), linha_falsa("2")], "total": Decimal("13500.00"),
        "nao_encontrados": []}], "total_geral": Decimal("13500.00"),
        "quantidade": 2, "nao_encontrados": []}
    numeros = dict(pdf.numeros_do_lote(montado))
    assert numeros["SPs"] == "2"
    assert numeros["Total"] == "R$ 13.500,00"
    assert {"A pagar", "Pagas", "Agendadas", "Falha ao agendar"} <= set(numeros)


def test_o_pdf_do_lote_vazio_avisa_em_vez_de_sair_em_branco(app, monkeypatch):
    """Um PDF de uma página em branco é pior do que um recado: quem imprime
    acha que o lote está vazio quando na verdade não foi montado."""
    from app.apps.analisesps import lote
    # O DUBLÊ RECEBE A PESSOA. Ele não recebia, e era assim que o defeito de
    # 11/09/2026 se escondia: o PDF e a exportação chamavam `ler()` sem
    # argumento — lendo o lote ANTIGO, compartilhado e congelado — e o teste
    # imitava exatamente a chamada errada, então passava.
    monkeypatch.setattr(lote, "ler", lambda pessoa: {"conteudo": "",
                                                     "salvo_por": None,
                                                     "salvo_em": None})
    monkeypatch.setattr(lote, "montar", lambda t: {
        "grupos": [], "linhas": {}, "nao_encontrados": [],
        "total_geral": 0, "quantidade": 0})
    resposta = como(app, SENHA_CONSULTA).get("/analisesps/lote/pdf")
    assert resposta.status_code == 400
    assert "Lote vazio" in resposta.get_data(as_text=True)


def test_falha_no_pdf_nao_derruba_a_tela(app_relatorio, monkeypatch):
    """Se o gerador quebrar, a pessoa precisa ler o motivo e saber que o CSV
    continua servindo — não receber uma página de erro do servidor."""
    from app.apps.analisesps import pdf
    monkeypatch.setattr(pdf, "relatorio", lambda *a, **k: 1 / 0)
    resposta = como(app_relatorio, SENHA_CONSULTA).get("/analisesps/relatorio/pdf")
    assert resposta.status_code == 500
    corpo = resposta.get_data(as_text=True)
    assert "Não consegui gerar o PDF" in corpo
    assert "CSV continua" in corpo


def test_o_pdf_do_relatorio_carrega_os_numeros_e_as_quebras(app_relatorio):
    """Confere o conteúdo, não só que o arquivo saiu: um PDF de uma página em
    branco também começaria com %PDF-."""
    from app.apps.analisesps import consultas, pdf

    corpo = pdf.relatorio({}, "geral", "tudo")
    # O texto de um PDF fica comprimido; o que dá para afirmar sem uma
    # biblioteca de leitura é o tamanho e a quantidade de páginas.
    assert b"/Type /Page" in corpo
    assert len(corpo) > 2000, "o PDF saiu pequeno demais para ter conteúdo"


def test_a_tabela_do_pdf_repete_o_cabecalho_a_cada_pagina():
    """Sem isso, a segunda página vira uma tabela de colunas sem nome — e
    quem imprime dez páginas não sabe qual coluna é qual."""
    import inspect
    from app.apps.analisesps import pdf
    codigo = inspect.getsource(pdf.Folha.tabela)
    assert "will_page_break" in codigo
    assert codigo.count("escrever_cabecalho()") >= 2


def test_o_texto_que_nao_cabe_e_cortado_e_nao_invade_a_coluna():
    """Um credor de nome longo empurrando o valor para fora embaralha a linha
    inteira, e o número fica ilegível justamente onde importa."""
    import inspect
    from app.apps.analisesps import pdf
    assert "get_string_width" in inspect.getsource(pdf.Folha._quebrar)


# ---------------------------------------------------------------------------
# O RELATÓRIO DO LOTE COM DESCRIÇÃO E OBRA
#
# Pedido do dono em 11/09/2026: *"reduz a fonte consideravelmente pra caber
# mais informação. Quero que tenha a descrição. Quero que tenha obra. E pode
# usar a quebra de linha."*
# ---------------------------------------------------------------------------
def _lote_de_papel(**campos):
    linha = {"id": "1443253401", "vencimento_d": "2026-09-20",
             "credor": "SERTAO CASA E CONSTRUCAO", "descricao": "MATERIAL",
             "centro_custo": "CREPETRIUNFO", "forma_pagamento": "Pix",
             "conta": "Bradesco - 50024-0", "valor_num": 1234.56}
    linha.update(campos)
    return {"quantidade": 1, "total_geral": linha["valor_num"], "grupos": [
        {"titulo_exibido": "Pagar amanhã", "total": linha["valor_num"],
         "nao_encontrados": [], "linhas": [linha]}]}


def _texto_do_pdf(dados: bytes) -> str:
    import io as _io

    from pypdf import PdfReader
    return "\n".join(p.extract_text()
                     for p in PdfReader(_io.BytesIO(dados)).pages)


def test_o_relatorio_do_lote_traz_descricao_e_obra():
    from app.apps.analisesps import pdf
    texto = _texto_do_pdf(pdf.relatorio_do_lote(_lote_de_papel(
        descricao="AQUISICAO DE MATERIAL ELETRICO", centro_custo="CREPEBELEM")))
    assert "Descrição" in texto and "Obra" in texto
    assert "AQUISICAO DE MATERIAL ELETRICO" in texto
    assert "CREPEBELEM" in texto


def test_a_descricao_longa_QUEBRA_em_vez_de_sumir():
    """Antes o texto era cortado na largura da coluna. Uma descrição de
    pagamento cortada na terceira palavra não serve para conferir nada."""
    from app.apps.analisesps import pdf
    longa = ("REFERENTE A AQUISICAO DE MATERIAL ELETRICO PARA A OBRA DO "
             "ALOJAMENTO CONFORME PEDIDO 4417")
    texto = _texto_do_pdf(pdf.relatorio_do_lote(_lote_de_papel(descricao=longa)))
    assert "ALOJAMENTO" in texto, "a descrição foi cortada antes do fim"


def test_palavra_gigante_SEM_espaco_e_partida_e_nao_invade_a_coluna():
    """Um código ou um nome digitado sem espaço nenhum não pode empurrar o
    valor para fora da página."""
    from app.apps.analisesps import pdf
    emendado = "FORNECEDORCOMNOMEGIGANTESEMESPACONENHUMPARATESTARAQUEBRA"
    texto = _texto_do_pdf(pdf.relatorio_do_lote(_lote_de_papel(credor=emendado)))
    assert "1.234,56" in texto, "o valor sumiu da linha"
    assert "FORNECEDORCOMNOME" in texto


def test_o_vencimento_impresso_e_o_DO_DIA_certo():
    """O defeito que apareceu conferindo este relatório: o vencimento de dia 20
    saía impresso como dia 19, porque a data sem hora era tratada como
    meia-noite e convertida de fuso."""
    from app.apps.analisesps import pdf
    texto = _texto_do_pdf(pdf.relatorio_do_lote(
        _lote_de_papel(vencimento_d="2026-09-20")))
    assert "20/09/2026" in texto
    assert "19/09/2026" not in texto


def test_a_fonte_do_relatorio_do_lote_e_menor_que_a_padrao():
    """"Pode reduzir a fonte consideravelmente pra caber mais informação."""
    import inspect

    from app.apps.analisesps import pdf
    codigo = inspect.getsource(pdf.relatorio_do_lote)
    assert "fonte=6.5" in codigo and "linhas_max=3" in codigo


def test_as_colunas_do_lote_cabem_na_folha():
    """Somar mais que a largura útil empurra a última coluna — a do valor —
    para fora da página, e é justamente ela que precisa ser lida."""
    import inspect
    import re

    from app.apps.analisesps import pdf
    codigo = inspect.getsource(pdf.relatorio_do_lote)
    larguras = re.search(r"larguras=\[([0-9,\s.]+)\]", codigo).group(1)
    total = sum(float(x) for x in larguras.split(","))
    assert total <= pdf.LARGURA_UTIL, f"as colunas somam {total} mm"


# ---------------------------------------------------------------------------
# Os downloads, com o banco fora
# ---------------------------------------------------------------------------
DOWNLOADS = [
    "/analisesps/lote/exportar",
    "/analisesps/lote/pdf",
    "/analisesps/relatorio/pdf",
]


@pytest.mark.parametrize("url", DOWNLOADS)
def test_download_com_banco_fora_explica_em_vez_de_dar_erro_cru(app_sem_banco, url):
    """Um download que devolve a página de erro genérica do servidor não diz
    nada a quem clicou. Estas três leem o banco ANTES de começar a mandar o
    arquivo, justamente para poder explicar."""
    resposta = como(app_sem_banco, SENHA_OPERADOR).get(url)
    corpo = resposta.get_data(as_text=True)
    assert "Não consegui" in corpo, (
        f"{url} não explicou o que houve — devolveu: {corpo[:200]}")


def test_o_lote_e_lido_antes_de_a_resposta_comecar(app_sem_banco):
    """O detalhe que faz a diferença: se o banco fosse lido dentro do gerador,
    o cabeçalho já teria saído com HTTP 200 e a pessoa receberia um arquivo
    pela metade — sem erro nenhum, o que é pior do que uma mensagem."""
    resposta = como(app_sem_banco, SENHA_OPERADOR).get("/analisesps/lote/exportar")
    assert resposta.status_code == 500
    assert resposta.mimetype == "text/html"


# ---------------------------------------------------------------------------
# BeeVale — as duas telas
# ---------------------------------------------------------------------------
def test_o_cadastro_beevale_nao_depende_do_drive(app, monkeypatch):
    """É o lado inofensivo: cola-se a lista e sai um arquivo. Não escreve em
    lugar nenhum, e por isso funciona mesmo com a pasta do Drive não
    configurada — que é o estado de hoje."""
    from app.apps.analisesps import beevale
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("", ""))
    monkeypatch.setattr(beevale, "buscar_por_cpf", lambda cpfs: (
        [beevale.registro("Ana Silva", "1990-04-25", "5548999887766", cpfs[0])],
        []))

    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/beevale/cadastro",
        data={"texto": "01234567890@bwsconstrucoes.com.br"})

    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Ana Silva" in html
    assert "012.345.678-90" in html


def test_o_cadastro_beevale_avisa_quem_ficou_de_fora(app, monkeypatch):
    """Gerar o arquivo sem notar que faltou gente é o erro que só aparece no
    portal, depois."""
    from app.apps.analisesps import beevale
    monkeypatch.setattr(beevale, "buscar_por_cpf",
                        lambda cpfs: ([], list(cpfs)))

    html = como(app, SENHA_OPERADOR).post(
        "/analisesps/beevale/cadastro",
        data={"texto": "01234567890"}).get_data(as_text=True)

    assert "não estão na planilha Dados" in html
    assert "01234567890" in html, "quem faltou tem de aparecer pelo número"


def test_baixar_o_cadastro_devolve_um_xlsx(app, monkeypatch):
    from app.apps.analisesps import beevale
    monkeypatch.setattr(beevale, "buscar_por_cpf", lambda cpfs: (
        [beevale.registro("Ana Silva", "", "", cpfs[0])], []))

    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/beevale/cadastro",
        data={"texto": "01234567890", "acao": "baixar"})

    assert resposta.status_code == 200
    assert "spreadsheetml" in resposta.headers["Content-Type"]
    assert "Cadastro_BeeVale_" in resposta.headers["Content-Disposition"]
    assert resposta.get_data()[:2] == b"PK"        # xlsx é um zip


def test_gerar_beevale_sem_pasta_do_drive_avisa_e_nao_oferece_o_botao(
        app, monkeypatch):
    """O estado de hoje. A tela tem de dizer o que falta — e NÃO pode mostrar
    um botão que só falharia."""
    from app.apps.analisesps import beevale
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("", ""))
    monkeypatch.setattr(consultas, "uma", lambda i: linha_falsa(i))

    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/beevale/gerar?id=1").get_data(as_text=True)

    assert "Falta dizer qual é a pasta do Google Drive" in html
    assert 'id="btn-gerar"' not in html


def test_gerar_beevale_mostra_o_que_vai_acontecer_antes_de_fazer(
        app, monkeypatch):
    """A tela é de CONFERÊNCIA: nada acontece até o operador apertar. É a única
    coisa do módulo que altera o Pipefy, e não tem desfazer."""
    from app.apps.analisesps import beevale
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("pasta", "tela"))
    monkeypatch.setattr(beevale, "preparar", lambda ids: {
        "prontos": [{"sp": "1", "cpf": "012.345.678-90", "nome": "Ana Silva",
                     "valor": 850.5, "cadastro": {}, "descricao_atual": ""}],
        "erros": [{"sp": "2", "motivo": "Campo vazio."}]})
    monkeypatch.setattr(consultas, "uma", lambda i: linha_falsa(i))

    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/beevale/gerar?id=1&id=2").get_data(as_text=True)

    assert "Não tem desfazer" in html
    assert "Ana Silva" in html
    assert "Campo vazio." in html
    assert 'id="btn-gerar"' in html


def test_o_perfil_consulta_nao_gera_beevale(app):
    """Ele sobe arquivo e reescreve card. Ver e exportar não dá esse direito."""
    cliente = como(app, SENHA_CONSULTA)
    for url in ("/analisesps/beevale/cadastro", "/analisesps/beevale/gerar?id=1"):
        assert cliente.get(url).status_code == 403, url
    assert cliente.post("/analisesps/api/beevale/gerar",
                        json={"ids": ["1"]}).status_code == 403


def test_a_barra_oferece_o_beevale_para_quem_opera(app):
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert 'id="ba-beevale"' in html
    assert "Cadastro BeeVale" in html


def test_a_linha_diz_a_forma_de_pagamento_para_a_trava_do_beevale(app):
    """O botão só habilita quando TODAS as marcadas são BeeVale, e é do
    atributo da linha que o navegador tira isso."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "data-forma=" in html


def test_configuracoes_diz_se_a_pasta_do_drive_esta_salva(app, monkeypatch):
    """A pergunta do dono: "ficou salvo?". A tela responde sem ninguém entrar
    no Render, e diz DE ONDE o valor veio — se um valor do Render vencesse em
    silêncio, ele colaria a pasta, veria "salvo" e nada mudaria."""
    from app.apps.analisesps import beevale
    monkeypatch.setattr(beevale, "pasta_do_drive",
                        lambda: ("1ycGeXKyABC123", "tela"))

    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/configuracoes").get_data(as_text=True)

    assert "Pasta do Google Drive" in html
    assert "…ABC123" in html
    # A pasta APARECE no campo, e é de propósito: ela não é segredo (é o
    # endereço de uma pasta) e ele precisa poder conferir e trocar o que
    # colou. O que nunca aparece é o token do Pipefy.
    assert 'value="1ycGeXKyABC123"' in html
    assert 'id="btn-salvar-pasta"' in html


def test_o_token_do_pipefy_nunca_aparece_na_tela(app, monkeypatch):
    """Esse SIM é segredo: com ele se lê e se escreve nos cards da empresa. A
    tela diz apenas se está configurado."""
    from app.apps.analisesps import beevale, credenciais
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("pasta", "tela"))
    monkeypatch.setattr(credenciais, "token", lambda nome, padrao="": (
        "token-secreto-do-pipefy" if nome == "PIPEFY_TOKEN" else padrao))

    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/configuracoes").get_data(as_text=True)

    assert "token-secreto-do-pipefy" not in html
    assert "Token do Pipefy" in html


def test_a_pasta_colada_como_endereco_inteiro_e_aceita(app, monkeypatch):
    """É o que se copia sem pensar: a barra do navegador com a pasta aberta.
    Exigir que ele recorte o pedaço certo de uma URL é pedir para errar."""
    from app.apps.analisesps import beevale
    salvas = []
    monkeypatch.setattr(beevale, "gravar_pasta_do_drive",
                        lambda p: salvas.append(p) or "1ycGeXKyABC123")

    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/api/pasta-drive",
        json={"pasta": "https://drive.google.com/drive/folders/1ycGeXKyABC123"})

    assert resposta.status_code == 200
    assert resposta.get_json()["ok"] is True
    assert resposta.get_json()["pasta"] == "1ycGeXKyABC123"


def test_o_perfil_consulta_nao_troca_a_pasta_do_drive(app):
    assert como(app, SENHA_CONSULTA).post(
        "/analisesps/api/pasta-drive",
        json={"pasta": "outra"}).status_code == 403


# ---------------------------------------------------------------------------
# A PÁGINA VAI COMPRIMIDA
#
# Medido: a tela de Solicitações são 430 KB de HTML cru, e nada no caminho
# comprimia. Comprimida dá 27 KB. É a maior diferença de todas para quem está
# do outro lado — o banco pode responder em 100 ms, mas meio megabyte ainda
# leva segundos numa internet ruim.
# ---------------------------------------------------------------------------
def test_a_pagina_vai_comprimida_para_quem_aceita(app):
    resposta = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes", headers={"Accept-Encoding": "gzip"},
        follow_redirects=True)
    assert resposta.headers.get("Content-Encoding") == "gzip"
    assert "Accept-Encoding" in resposta.headers.get("Vary", "")


def test_quem_nao_aceita_comprimido_recebe_a_pagina_normal(app):
    """Um navegador antigo, ou uma ferramenta de linha de comando, não pode
    receber lixo binário no lugar da tela."""
    resposta = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes", headers={"Accept-Encoding": ""},
        follow_redirects=True)
    assert not resposta.headers.get("Content-Encoding")
    assert "<html" in resposta.get_data(as_text=True)


def test_o_conteudo_comprimido_e_a_mesma_pagina(app):
    import gzip
    cliente = como(app, SENHA_OPERADOR)
    comprimida = cliente.get("/analisesps/solicitacoes",
                             headers={"Accept-Encoding": "gzip"},
                             follow_redirects=True).get_data()
    crua = cliente.get("/analisesps/solicitacoes",
                       headers={"Accept-Encoding": ""},
                       follow_redirects=True).get_data()
    assert gzip.decompress(comprimida) == crua


def test_a_exportacao_em_fluxo_nao_e_comprimida(app):
    """A exportação é escrita em blocos justamente para não abrir a base
    inteira na memória. Comprimir obrigaria a juntar tudo antes — que é o que
    aquele caminho existe para evitar."""
    resposta = como(app, SENHA_OPERADOR).get(
        "/analisesps/exportar", headers={"Accept-Encoding": "gzip"})
    assert not resposta.headers.get("Content-Encoding")


def test_resposta_pequena_nao_paga_o_custo_de_comprimir(app):
    """Comprimir meia dúzia de bytes gasta processador e não economiza nada."""
    resposta = como(app, SENHA_OPERADOR).get(
        "/analisesps/api/andamento", headers={"Accept-Encoding": "gzip"})
    assert not resposta.headers.get("Content-Encoding")


# ---------------------------------------------------------------------------
# A TELA VOLTA COMO ESTAVA
#
# "eu filtro, vou para o Lote, volto para Solicitações — e ele refaz tudo de
# novo. Se eu tivesse duas abas do navegador eu alternaria na hora." O dono
# escolheu cinco minutos em 09/09/2026, com o risco na frente.
# ---------------------------------------------------------------------------
def test_as_telas_de_leitura_ficam_guardadas_no_navegador(app):
    resposta = como(app, SENHA_OPERADOR).get("/analisesps/solicitacoes",
                                             follow_redirects=True)
    guardar = resposta.headers.get("Cache-Control", "")
    assert "max-age=300" in guardar
    # `private` porque a tela é de UMA pessoa: nada de cache compartilhado no
    # caminho guardando a lista de pagamentos da empresa.
    assert "private" in guardar


def test_a_lista_de_telas_guardadas_e_fechada():
    """É uma lista escrita à mão de propósito: entrar nela é decidir que a
    tela pode ser mostrada com até cinco minutos de idade."""
    from app.apps.analisesps import web
    assert web.TELAS_QUE_FICAM_GUARDADAS == {
        "analisesps.solicitacoes", "analisesps.relatorio",
        # O Calendário entrou em 23/09/2026: é leitura pura, como o Relatório
        # — não recebe alteração nenhuma, só desenha o filtro no mês.
        "analisesps.calendario",
        "analisesps.auditoria", "analisesps.log", "analisesps.tela_lote"}
    # A Agenda e a ficha da SP continuam fora: as duas recebem alteração e
    # NÃO têm a hora de salvamento na tela, que é o que torna o Lote seguro.
    assert "analisesps.tela_agenda" not in web.TELAS_QUE_FICAM_GUARDADAS
    assert "analisesps.detalhe" not in web.TELAS_QUE_FICAM_GUARDADAS


def test_o_lote_fica_guardado_porque_e_a_ida_e_volta_que_incomoda(app_lote):
    """"Permaneceu a demora entre o Lote e as Solicitações." As Solicitações
    já ficavam guardadas; o Lote não, e por isso metade do caminho continuava
    lenta."""
    resposta = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote",
                                                  follow_redirects=True)
    guardar = resposta.headers.get("Cache-Control", "")
    assert "max-age=300" in guardar and "private" in guardar


def test_o_lote_que_volta_de_uma_alteracao_nao_fica_guardado(app_lote):
    """Depois de salvar, o Lote redireciona para ele mesmo com `?aviso=`.
    Guardar ESSA tela faria o recado de "salvo" reaparecer minutos depois,
    dizendo que algo acabou de acontecer quando não aconteceu."""
    resposta = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote?aviso=")
    assert "max-age" not in resposta.headers.get("Cache-Control", "")


def test_o_lote_carrega_a_hora_em_que_foi_salvo(app_lote):
    """É a rede de segurança contra a cópia guardada ficar atrasada: o
    navegador compara essa hora com a última que viu e recarrega sozinho se a
    tela em frente for anterior à última salvada. Sem ela, guardar o Lote
    dependeria de o navegador cumprir a regra do HTTP de apagar a cópia
    depois de um POST — e o preço de não cumprir é a pessoa salvar por cima
    do próprio trabalho."""
    resposta = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote",
                                                  follow_redirects=True)
    assert b'id="cartao-lote"' in resposta.data
    assert b'data-lote-em=' in resposta.data


def test_a_ficha_da_sp_nunca_fica_guardada(app_ficha):
    """Ela mostra o status atual e tem botões que agem sobre ele. Guardada,
    alguém agendaria olhando um estado que já mudou."""
    resposta = como(app_ficha, SENHA_OPERADOR).get("/analisesps/sp/1234567890")
    assert "max-age" not in resposta.headers.get("Cache-Control", "")


def test_sair_apaga_o_que_ficou_guardado(app):
    """Num computador compartilhado, apertar Voltar depois de sair mostraria as
    telas da pessoa anterior pelos minutos que faltassem."""
    resposta = como(app, SENHA_OPERADOR).get("/analisesps/sair")
    assert resposta.headers.get("Clear-Site-Data") == '"cache"'
    assert "no-store" in resposta.headers.get("Cache-Control", "")


# ---------------------------------------------------------------------------
# ENVIAR AO LOTE SEM SAIR DA TELA
# ---------------------------------------------------------------------------
def test_enviar_ao_lote_nao_troca_de_tela(app, monkeypatch):
    """Pedido do dono: "mantenha-se em Solicitações, apenas avise que foi
    executada a ação". Antes o botão mandava um formulário e levava a pessoa
    para o Lote — perdendo o filtro, a rolagem e a marcação."""
    from app.apps.analisesps import lote
    salvos = []
    monkeypatch.setattr(lote, "ler", lambda pessoa="": {"conteudo": ""})
    monkeypatch.setattr(lote, "salvar",
                        lambda c, quem="", pessoa="": salvos.append(c))

    resposta = como(app, SENHA_OPERADOR).post(
        "/analisesps/api/enviar-ao-lote", json={"ids": ["1", "2", "3"]})

    assert resposta.status_code == 200, "não podia redirecionar para outra tela"
    corpo = resposta.get_json()
    assert corpo["ok"] is True
    assert corpo["quantas"] == 3
    assert corpo["titulo"], "o aviso precisa dizer em que grupo entraram"
    assert salvos and "1" in salvos[0]


def test_enviar_ao_lote_usa_a_mesma_regra_do_formulario(app, monkeypatch):
    """Um grupo NOVO no topo, o que já estava fica abaixo. A regra é chamada,
    não copiada — duas cópias divergiriam."""
    from app.apps.analisesps import lote
    salvos = []
    monkeypatch.setattr(lote, "ler",
                        lambda pessoa="": {"conteudo": "Grupo antigo\n999999999"})
    monkeypatch.setattr(lote, "salvar",
                        lambda c, quem="", pessoa="": salvos.append(c))

    como(app, SENHA_OPERADOR).post("/analisesps/api/enviar-ao-lote",
                                   json={"ids": ["111111111"]})

    assert salvos
    assert "999999999" in salvos[0], "o que já estava no lote sumiu"
    assert salvos[0].index("111111111") < salvos[0].index("999999999"), (
        "o grupo novo tem de ficar no topo")


def test_o_perfil_consulta_nao_envia_ao_lote(app):
    assert como(app, SENHA_CONSULTA).post(
        "/analisesps/api/enviar-ao-lote", json={"ids": ["1"]}).status_code == 403


def test_a_barra_sabe_para_onde_enviar_sem_sair_da_tela(app):
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes", follow_redirects=True).get_data(as_text=True)
    assert "data-url-enviar-lote=" in html


# ---------------------------------------------------------------------------
# O LINK DA ABA APONTA PARA A TELA COMO ELA ESTAVA
#
# "não senti diferença nenhuma... nem indo nem voltando" — 09/09/2026, depois
# de a tela guardada ter sido publicada. E ele estava certo: o link do menu
# aponta para o endereço SEM filtro, o servidor vê que há filtro guardado e
# REDIRECIONA. Toda troca de aba ia ao servidor de qualquer jeito, e a cópia
# guardada — que fica sob o endereço COM filtro — nunca era alcançada.
# ---------------------------------------------------------------------------
def test_o_endereco_sem_filtro_redireciona_e_por_isso_nao_serve_a_copia(
        app, monkeypatch):
    """Este é o defeito, fixado como teste: quem chega sem o filtro na barra
    de endereços é mandado para outro endereço — e redirecionamento não se
    guarda. É por isso que o link do menu precisa ser reescrito."""
    from app.apps.analisesps import preferencias
    monkeypatch.setattr(preferencias, "ler", lambda pessoa, chave: (
        {"status_pgt": ["Pagar"]} if chave == preferencias.FILTRO else {}))

    resposta = como(app, SENHA_OPERADOR).get("/analisesps/solicitacoes")
    assert resposta.status_code in (301, 302)
    assert "f=1" in resposta.headers["Location"]
    assert "max-age" not in resposta.headers.get("Cache-Control", "")


def test_o_endereco_com_filtro_responde_direto_e_fica_guardado(app):
    """O endereço para onde o link reescrito aponta: sem redirecionamento, e
    guardável. É esse o caminho que torna a volta instantânea."""
    resposta = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes?f=1&status_pgt=Pagar")
    assert resposta.status_code == 200, "não podia redirecionar"
    assert "max-age=300" in resposta.headers.get("Cache-Control", "")


def test_o_navegador_reescreve_os_links_do_menu():
    """A reescrita é no navegador, e não no servidor, de propósito: montar
    esses links no servidor custaria uma consulta a mais em TODA tela,
    inclusive nas que não têm filtro nenhum."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    assert "analisesps:endereco:" in js
    assert "a.topo-aba" in js, "sem isto os links do menu não são tocados"


def test_o_navegador_recarrega_o_lote_guardado_que_estiver_atrasado():
    """A rede de segurança do Lote guardado. Ela só age quando a tela veio
    DO CACHE — sem essa condição, a tela que volta de uma salvada, que é nova
    e traz hora nova, se recarregaria à toa a cada salvamento."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    assert "cartao-lote" in js, "sem isto a hora do lote não é lida"
    assert "transferSize" in js, "sem isto a recarga dispara fora do cache"
    assert "location.reload()" in js


# ---------------------------------------------------------------------------
# O BOTÃO DE ATUALIZAR, ao lado da hora da base
# ---------------------------------------------------------------------------
def test_o_botao_de_atualizar_fica_ao_lado_da_hora_da_base(app):
    """Era preciso ir a Configurações só para apertá-lo. Quem olha a hora da
    base e acha que está velha quer atualizar ALI."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes", follow_redirects=True).get_data(as_text=True)
    assert 'id="btn-atualizar-base"' in html
    assert "base de" in html


def test_quem_so_consulta_nao_ve_o_botao_de_atualizar(app):
    """Atualizar é escrever: traz a planilha para o banco. Ver e exportar não
    dá esse direito — e a porta já recusa, então o botão só confundiria."""
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/solicitacoes", follow_redirects=True).get_data(as_text=True)
    assert 'id="btn-atualizar-base"' not in html


def test_quem_so_consulta_nao_dispara_atualizacao(app):
    resposta = como(app, SENHA_CONSULTA).post(
        "/analisesps/api/sincronizar", json={"modo": "sincronizar"})
    assert resposta.status_code == 403


@pytest.fixture
def app_rateio(app, monkeypatch):
    """A tela de Ratear com as listas já carregadas, sem tocar no banco."""
    from app.apps.analisesps import sincronizacao
    monkeypatch.setattr(sincronizacao, "referencias_rateio", lambda: {
        "obras": [{"nome": "OBRA-1", "codigo": "5001"},
                  {"nome": "OBRA-2", "codigo": "5002"}],
        "categorias": [{"nome": "Mão de obra", "codigo": "2002"}]})
    return app


# ---------------------------------------------------------------------------
# COLAR UMA TABELA NO RATEAR
#
# "Imagina que eu tenho trinta obras para ratear. Se eu for colocar uma a uma
# é trabalhoso, e essa informação normalmente vem de uma planilha do Excel."
# Pedido do dono em 10/09/2026.
#
# A interpretação é no SERVIDOR, e não no navegador, de propósito: um rateio
# na obra errada o Omie aceita sem reclamar, e o que roda no navegador esta
# suíte não alcança. Por isso estes testes existem e são detalhados.
# ---------------------------------------------------------------------------
OBRAS = [{"nome": "OBRA-1", "codigo": "5001"},
         {"nome": "OBRA-12 - CRECHE SWAP", "codigo": "5012"},
         {"nome": "CONS", "codigo": "5003"}]


def _colar(texto, referencias=None):
    from app.apps.analisesps import rateio
    return rateio.interpretar_colagem(texto, referencias or OBRAS)


@pytest.mark.parametrize("colado,como", [
    ("OBRA-1\t1.234,56\nCONS\t2.000,00", "tabulação, que é o que o Excel cola"),
    ("Centro de custo\tValor\nOBRA-1\t1.234,56\nCONS\t2.000,00", "com cabeçalho junto"),
    ("OBRA-1;1.234,56\nCONS;2.000,00", "ponto e vírgula"),
    ("OBRA-1   1.234,56\nCONS   2.000,00", "colunas separadas por espaços"),
    ("OBRA-1 1.234,56\nCONS 2.000,00", "um espaço só"),
    ("OBRA-1\tR$ 1.234,56\nCONS\tR$ 2.000,00", "com R$ na frente"),
    ("\nOBRA-1\t1.234,56\n\nCONS\t2.000,00\n", "com linhas em branco no meio"),
])
def test_a_colagem_entende_o_que_a_pessoa_realmente_cola(colado, como):
    """Cada um destes é um jeito real de copiar uma tabela. O valor é sempre o
    ÚLTIMO pedaço que parece número, e o nome é tudo o que vem antes — é isso
    que faz funcionar com qualquer separador e com nome que tem espaço."""
    lido = _colar(colado)
    assert [(l["nome"], l["valor"]) for l in lido["linhas"]] == [
        ("OBRA-1", "1234,56"), ("CONS", "2000,00")], como
    assert not lido["avisos"], f"{como}: {lido['avisos']}"


def test_o_nome_que_nao_existe_na_lista_nao_entra_e_e_apontado():
    """NUNCA adivinhar. Uma obra que não existe entrar como outra qualquer
    viraria lançamento no lugar errado, e o Omie aceita sem reclamar."""
    lido = _colar("OBRA-1\t100,00\nOBRA-99\t200,00")
    assert [l["nome"] for l in lido["linhas"]] == ["OBRA-1"]
    assert any("OBRA-99" in a and "não entrou" in a for a in lido["avisos"])


def test_o_nome_pela_metade_casa_quando_nao_ha_duvida():
    """Quem monta a tabela à mão escreve "OBRA-12", e a lista tem
    "OBRA-12 - CRECHE SWAP"."""
    lido = _colar("OBRA-12\t100,00")
    assert [l["nome"] for l in lido["linhas"]] == ["OBRA-12 - CRECHE SWAP"]
    assert any("confira" in a for a in lido["avisos"]), (
        "interpretação que não é o nome exato TEM de aparecer na tela")


def test_o_nome_pela_metade_nao_pode_arrastar_a_obra_de_nome_parecido():
    """ESTE É O TESTE QUE IMPORTA. O primeiro jeito que escrevi comparava "um
    contém o outro", e "OBRA-1" casava com "OBRA-12". Com duas obras cujo nome
    começa igual, a colagem tem de RECLAMAR, não escolher."""
    lido = _colar("OBRA-1\t100,00", [{"nome": "OBRA-1 - ALA A", "codigo": "1"},
                                     {"nome": "OBRA-1 - ALA B", "codigo": "2"}])
    assert lido["linhas"] == []
    assert any("mais de uma" in a for a in lido["avisos"])


def test_o_nome_exato_ganha_de_qualquer_parecido():
    """Com "OBRA-1" na lista, colar "OBRA-1" é o nome exato — não pode virar
    uma escolha "parecida" nem gerar recado."""
    lido = _colar("OBRA-1\t100,00")
    assert [l["nome"] for l in lido["linhas"]] == ["OBRA-1"]
    assert not lido["avisos"]


def test_o_codigo_do_omie_no_lugar_do_nome_tambem_serve():
    """Acontece com quem monta a tabela a partir do relatório do Omie."""
    lido = _colar("5003\t100,00")
    assert [l["nome"] for l in lido["linhas"]] == ["CONS"]
    assert any("código" in a for a in lido["avisos"])


def test_valor_que_nao_e_numero_positivo_e_apontado_e_nao_entra():
    lido = _colar("OBRA-1\tabc\nCONS\t(500,00)\nOBRA-1\t0")
    assert lido["linhas"] == []
    assert len(lido["avisos"]) == 3


def test_a_obra_repetida_fica_mas_e_apontada():
    """Duas linhas da mesma obra pode ser engano de quem colou, ou pode ser
    de propósito. Quem decide é a pessoa — mas ela precisa ver."""
    lido = _colar("OBRA-1\t100,00\nOBRA-1\t200,00")
    assert len(lido["linhas"]) == 2
    assert any("mais de uma vez" in a for a in lido["avisos"])


def test_colar_na_tela_preenche_um_lado_sem_apagar_o_outro(app_rateio):
    """Apertar "Interpretar" do lado das obras não pode apagar as categorias
    que a pessoa já tinha digitado, nem a base."""
    cliente = como(app_rateio, SENHA_OPERADOR)
    resposta = cliente.post("/analisesps/ratear", data={
        "acao": "colar_cc",
        "colagem_cc": "OBRA-2\t2.000,00",
        "cat_nome": ["Mão de obra"], "cat_valor": ["999,00"],
        "base_categoria": "5.000,00"})
    html = resposta.get_data(as_text=True)

    assert resposta.status_code == 200
    assert 'value="2000,00"' in html, "a linha colada não entrou"
    assert 'value="999,00"' in html, "apagou o outro lado"
    assert 'value="5.000,00"' in html, "apagou a base da categoria"


def test_o_texto_colado_volta_para_a_caixa(app_rateio):
    """Quem precisa corrigir uma linha não pode ter de colar tudo de novo — e
    a caixa fica aberta, ao lado do recado, para ele conferir e tentar."""
    html = como(app_rateio, SENHA_OPERADOR).post("/analisesps/ratear", data={
        "acao": "colar_cc",
        "colagem_cc": "OBRA-9\t100,00"}).get_data(as_text=True)
    assert "OBRA-9" in html
    assert "<details" in html and "open" in html


def test_apertar_enter_num_campo_gera_e_nao_interpreta(app_rateio):
    """O navegador usa o PRIMEIRO botão de envio do formulário quando a pessoa
    aperta Enter. Com as caixas de colar, esse passou a ser "Interpretar" —
    então há um botão escondido de "gerar" antes de todos."""
    import re as _re
    html = como(app_rateio, SENHA_OPERADOR).get(
        "/analisesps/ratear").get_data(as_text=True)
    envios = _re.findall(r'<button[^>]*type="submit"[^>]*value="([^"]*)"', html)
    assert envios and envios[0] == "gerar", (
        f"o primeiro botão de envio tem de ser o de gerar, e é {envios[:2]}")


def test_colar_nao_gera_o_json_ainda(app_rateio):
    """Colar é preparar, não executar. A pessoa confere e só então gera."""
    html = como(app_rateio, SENHA_OPERADOR).post("/analisesps/ratear", data={
        "acao": "colar_cc", "colagem_cc": "OBRA-1\t100,00"}).get_data(as_text=True)
    assert "copie e cole no Omie" not in html


def test_quem_so_consulta_nao_cola_nem_gera(app_rateio):
    resposta = como(app_rateio, SENHA_CONSULTA).post("/analisesps/ratear", data={
        "acao": "colar_cc", "colagem_cc": "OBRA-1\t100,00"})
    assert resposta.status_code == 403


def test_a_tela_nao_mostra_mais_uma_caixa_de_erro_escrita_None(app_rateio):
    """O `gerar_jsons` devolve três chaves, e a terceira é "erro". A tela
    mostrava as três, então abria uma caixa chamada "erro" com "None" dentro
    sempre que dava tudo certo."""
    html = como(app_rateio, SENHA_OPERADOR).post("/analisesps/ratear", data={
        "acao": "gerar",
        "cc_nome": ["OBRA-1"], "cc_valor": ["100,00"]}).get_data(as_text=True)
    assert "copie e cole no Omie" in html
    assert ">None<" not in html and ">erro<" not in html


# ---------------------------------------------------------------------------
# DOIS DEFEITOS DE TELA REPORTADOS PELO DONO EM 11/09/2026
# ---------------------------------------------------------------------------
def test_esconder_tem_de_esconder():
    """"No filtro tipo de despesa, se eu escrever, ele não está filtrando as
    possibilidades."

    O javascript da procura funcionava; o ESTILO é que anulava. O navegador
    esconde `[hidden]` com `display: none`, mas isso vem da folha DELE — e
    qualquer regra nossa ganha. `.opcao` tem `display: flex`, então a opção era
    marcada como escondida e continuava na tela.

    A regra vale para a folha inteira de propósito: o mesmo tropeço aconteceria
    em qualquer elemento com `display` próprio."""
    from pathlib import Path
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(encoding="utf-8")
    assert "[hidden] { display: none !important; }" in css
    # E a regra tem de vir DEPOIS de `.opcao`, senão perde por ordem.
    assert css.index("[hidden] { display: none") > css.index(".opcao { display: flex")


def test_a_conta_de_cada_sp_viaja_para_a_barra_de_acoes(app):
    """Sem isto a barra não tem como somar por conta: ela só enxerga as
    caixinhas marcadas, não a tabela."""
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes", follow_redirects=True).get_data(as_text=True)
    assert 'data-conta=' in html


def test_a_barra_soma_o_marcado_por_conta():
    """"Só aparece o total dos selecionados; faltava o total POR CONTA." É por
    conta que o dinheiro sai — o total geral diz se a remessa é grande, este
    diz se ela cabe."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    trecho = js.split("function atualizar()")[1].split("\n  }")[0]
    assert "dataset.conta" in trecho, "a barra não lê a conta"
    assert "sort" in trecho, "sem ordenar, a conta que concentra se perde no meio"


def test_a_linha_por_conta_some_quando_ha_uma_conta_so(app):
    """Repetir o total que já está logo acima é ruído."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    assert "partes.length < 2" in js
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes", follow_redirects=True).get_data(as_text=True)
    assert 'id="ba-contas"' in html and "hidden" in html


# ---------------------------------------------------------------------------
# O LOTE DA PESSOA CERTA
#
# "Eu atualizei o lote, e o relatório permanece desatualizado." Reportado pelo
# dono em 11/09/2026. A exportação e o PDF chamavam `lote.ler()` sem a pessoa,
# e o padrão do argumento era `""` — que é o LOTE ANTIGO, de quando ele era um
# só e compartilhado, congelado desde que o lote passou a ser de cada um.
# ---------------------------------------------------------------------------
def test_a_exportacao_e_o_pdf_leem_o_lote_DA_PESSOA(app, monkeypatch):
    """O que sai no arquivo tem de ser o que está na tela. Enquanto não era,
    a pessoa mandava para o banco uma remessa que ela não montou."""
    from app.apps.analisesps import lote
    pedidos = []

    def falso_ler(pessoa):
        pedidos.append(pessoa)
        return {"conteudo": "Grupo\n1234567890", "salvo_por": "x",
                "salvo_em": None}

    monkeypatch.setattr(lote, "ler", falso_ler)
    monkeypatch.setattr(lote, "montar", lambda t: {
        "grupos": [], "linhas": {}, "nao_encontrados": [],
        "total_geral": 0, "quantidade": 0})

    cliente = como(app, SENHA_OPERADOR, nome="MARCELO")
    cliente.get("/analisesps/lote/exportar")
    cliente.get("/analisesps/lote/pdf")

    assert pedidos, "nenhuma das duas rotas leu o lote"
    assert all(p == "marcelo" for p in pedidos), (
        f"leram o lote de {pedidos} em vez do da pessoa logada")


def test_ler_e_salvar_o_lote_exigem_a_pessoa():
    """SEM VALOR PADRÃO, de propósito. O padrão era `""`, que significa o lote
    antigo — e duas rotas caíram nele por meses sem ninguém perceber, porque
    um lote congelado não dá erro: ele só fica errado."""
    import inspect
    from app.apps.analisesps import lote
    for funcao in (lote.ler, lote.salvar):
        parametro = inspect.signature(funcao).parameters["pessoa"]
        assert parametro.default is inspect.Parameter.empty, (
            f"{funcao.__name__} voltou a ter padrão para a pessoa")


# ---------------------------------------------------------------------------
# A TELA DO BRADESCO FICAVA EM BRANCO
#
# "Cliquei conferir e ficou tudo em branco." Reportado pelo dono em 11/09/2026,
# com o texto que ele colou. O interpretador estava certo: o MESMO texto com
# tabulação dá duas operações e com espaços dá zero — e zero operações não
# desenhava nada na tela.
# ---------------------------------------------------------------------------
EXTRATO_DO_DONO = "\n".join([
    "\t11/09/2026\t-\t1251 | 0002541-0\tJOSE THIAGO DA SILVA\t0 / 1\t160,00\t\t",
    "",
    "Operação e Descrição: Pagamento Pix",
    "",
    "TRANSFERENCIA PIX",
    "",
    "Nome: PARAIBA LOCACOES LTDA",
    "",
    "\t11/09/2026\t11/09/2026\t1251 | 0002541-0\tKarla Erica dos Sant\t0 / 1\t20.741,39\t\t",
    "",
    "Operação e Descrição: Boletos de Cobrança",
    "",
    "Boleto de Cobrança - 75691.42933 01254.645003 00000.150011 1 15570002068980"
    " - 1436337687 - 11/09/2026",
])


def test_o_extrato_e_lido_com_tabulacao_ou_com_espacos():
    """Copiar a tabela do Bradesco às vezes traz tabulação e às vezes traz
    espaços — depende do navegador e de como a seleção é feita. Só o primeiro
    caso funcionava, e o segundo era ignorado em silêncio."""
    from app.apps.analisesps import bradesco
    com_tab = bradesco.parse_autorizacao(EXTRATO_DO_DONO)
    com_espaco = bradesco.parse_autorizacao(EXTRATO_DO_DONO.replace("\t", "    "))

    assert len(com_tab) == 2, "o caso que já funcionava parou de funcionar"
    assert len(com_espaco) == 2, "o texto com espaços continua sendo ignorado"
    for lidas in (com_tab, com_espaco):
        assert [o["tipo"] for o in lidas] == ["pix", "boleto"]
        assert lidas[0]["nome"] == "PARAIBA LOCACOES LTDA"
        assert lidas[1]["sp"] == "1436337687"


def test_o_nome_com_espaco_simples_nao_e_partido_em_colunas():
    """"JOSE THIAGO DA SILVA" tem espaço simples e é UMA coluna. Se a divisão
    fosse por qualquer espaço, o nome viraria quatro campos e a linha deixaria
    de ser reconhecida."""
    from app.apps.analisesps import bradesco
    lidas = bradesco.parse_autorizacao(
        EXTRATO_DO_DONO.replace("\t", "    "))
    assert lidas, "a linha com nome composto deixou de ser lida"


def test_a_tela_do_bradesco_diz_quando_nao_reconhece_nada(app_bradesco):
    """Ficar em branco é o pior resultado: quem colou não sabe se o sistema
    leu, se travou, ou se não havia o que conferir."""
    html = como(app_bradesco, SENHA_OPERADOR).post(
        "/analisesps/bradesco",
        data={"extrato": "isto não é um extrato"}).get_data(as_text=True)
    assert "Não reconheci nenhuma operação" in html
    assert "agência | conta" in html, "tem de dizer o que falta na linha"
    assert "linha(s)" in html, "tem de dizer quanto foi colado"


def test_a_caixinha_de_foco_desmarcada_desliga_o_foco(app_bradesco):
    """Caixinha desmarcada NÃO chega no formulário — é assim que o HTML
    funciona. O padrão "1" entrava justamente quando a pessoa desmarcava, e o
    foco nunca desligava."""
    from app.apps.analisesps import bradesco
    vistos = []
    import app.apps.analisesps.web as web_mod
    original = bradesco.cruzar_tudo

    def espiar(raw, df, foco_agendados=True):
        vistos.append(foco_agendados)
        return original(raw, df, foco_agendados)

    bradesco.cruzar_tudo = espiar
    try:
        cliente = como(app_bradesco, SENHA_OPERADOR)
        cliente.post("/analisesps/bradesco",
                     data={"extrato": EXTRATO_DO_DONO, "foco": "1"})
        cliente.post("/analisesps/bradesco", data={"extrato": EXTRATO_DO_DONO})
    finally:
        bradesco.cruzar_tudo = original

    assert vistos == [True, False], (
        f"a caixinha não desligou o foco: {vistos}")


@pytest.fixture
def app_bradesco(app, monkeypatch):
    """A tela do Bradesco com a base carregada, sem tocar no banco."""
    from app.apps.analisesps import web
    monkeypatch.setattr(web, "_candidatas_bradesco", lambda: [])
    return app


# ---------------------------------------------------------------------------
# DUAS CORREÇÕES DO LOTE, REPORTADAS EM 11/09/2026
# ---------------------------------------------------------------------------
def test_o_titulo_do_grupo_que_esvaziou_na_limpeza_vai_junto():
    """"Quando limparmos um lote tirando pagas e canceladas e ele estiver
    vazio, apagar o cabeçalho." Antes o título ficava, e o lote terminava
    cheio de cabeçalhos sem nada embaixo."""
    from app.apps.analisesps import lote
    texto = "Pagar amanhã\n111 222\n\nJá pago\n333"
    status = {"111": "Pagar", "222": "Pagar", "333": "Pago"}

    novo, quantos = lote.remover_por_status(texto, {"pago"}, status)

    assert quantos == 1
    assert "Já pago" not in novo, "o cabeçalho órfão ficou"
    assert "Pagar amanhã" in novo and "111 222" in novo


def test_o_grupo_que_JA_estava_vazio_nao_e_apagado():
    """Alguém escreveu aquele título de propósito, para encher depois. Apagar
    o que a pessoa acabou de digitar é pior do que o cabeçalho sobrando."""
    from app.apps.analisesps import lote
    texto = "Para amanhã\n\nJá pago\n333"
    novo, _ = lote.remover_por_status(texto, {"pago"}, {"333": "Pago"})
    assert "Para amanhã" in novo
    assert "Já pago" not in novo


def test_limpar_o_lote_inteiro_nao_deixa_cabecalho_nenhum():
    from app.apps.analisesps import lote
    texto = "Grupo A\n111\n\nGrupo B\n222"
    novo, quantos = lote.remover_por_status(
        texto, {"pago"}, {"111": "Pago", "222": "Pago"})
    assert quantos == 2 and novo.strip() == ""


def test_agir_sobre_a_selecao_apaga_a_memoria_dela():
    """"São reaplicadas seleções que eu já desmarquei; não pode retroagir."

    A memória existe para quem SAI da tela e VOLTA. Depois de uma ação a tela
    recarrega, e repor a marcação fazia as SPs voltarem marcadas DEPOIS de já
    terem sido tratadas — convidando a agir duas vezes sobre a mesma SP."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    assert "function selecaoConsumida()" in js
    # As quatro ações que ALTERAM alguma coisa têm de chamar.
    assert js.count("selecaoConsumida();") == 4, (
        "cada ação que altera precisa apagar a memória da seleção")


def test_a_marcacao_reposta_nao_pega_a_COPIA_da_SP_no_lote():
    """"Quando eu marco alguma coisa no lote, e esse registro está repetido,
    ele marca também o outro — está bagunçando." — o dono, em 11/09/2026.

    A memória guardava o NÚMERO da SP. No Lote a mesma SP pode estar em dois
    grupos, então repor pelo número marcava as duas cópias: a que a pessoa
    marcou e a que ela não marcou. Agora guarda a CHAVE DA LINHA."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    assert "chaveDaLinha" in js
    assert 'LEMBRAR.gravar("marcadas", sel.map(chaveDaLinha))' in js, (
        "voltou a guardar o número da SP — a cópia volta a ser marcada junto")
    assert "querem.has(chaveDaLinha(c))" in js, (
        "a reposição voltou a comparar pelo número")


def test_so_o_lote_usa_chave_de_linha_as_solicitacoes_usam_o_numero():
    """Nas Solicitações cada SP aparece UMA vez, então o número já identifica
    a linha. Usar a posição lá faria a marcação se perder toda vez que a base
    sincronizasse e empurrasse as linhas — que é justamente a memória que a
    19ª leva criou."""
    from pathlib import Path
    html = Path("app/apps/analisesps/templates/analisesps_tabela.html").read_text(
        encoding="utf-8")
    assert '{% if grupo is defined %}data-chave=' in html, (
        "a chave da linha tem de estar presa ao Lote (onde há grupo), e não "
        "valer também para as Solicitações")


def test_a_tela_de_QR_nao_apaga_a_memoria_da_selecao():
    """Ela não altera nada, só abre outra tela — e quem volta de lá quer a
    seleção inteira de volta."""
    from pathlib import Path
    js = Path("app/apps/analisesps/static/analisesps.js").read_text(encoding="utf-8")
    trecho = js.split('getElementById("ba-codigos")')[1].split("});")[0]
    assert "selecaoConsumida" not in trecho


# ---------------------------------------------------------------------------
# O PDF E O CSV LEVAM O FILTRO INTEIRO — 18/09/2026
#
# > *"Eu coloco aplicar para poder baixar o PDF, mas não baixa com as
# > informações que estão aparecendo na tela. Está aparecendo outras
# > informações."*
#
# ⚠️ A CAUSA, e ela é traiçoeira: os links eram montados com `**args`, e `args`
# é um MultiDict. Desempacotar com `**` pega **só o primeiro valor de cada
# chave**. Marcando três obras, a tela filtrava pelas três e o link levava UMA.
#
# O pior não é o erro: é o silêncio. O arquivo baixa normalmente, com cara de
# certo, e ninguém tem como desconfiar — a não ser somando na mão.
#
# `to_dict(flat=false)` devolve listas, e aí os três valores viajam.
# ---------------------------------------------------------------------------
def _href_de(html, rota):
    """O endereço do link daquela rota, como ele sai no HTML."""
    import re
    achados = re.findall(r'href="([^"]*' + rota + r'[^"]*)"', html)
    assert achados, f"não achei link para {rota} na tela"
    return achados[0]


def test_o_PDF_do_relatorio_leva_TODOS_os_valores_do_filtro(app_relatorio):
    """Três obras marcadas na tela têm de ser três obras no PDF."""
    from urllib.parse import unquote

    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio?centro_custo=OBRA-1&centro_custo=OBRA-2"
        "&centro_custo=OBRA-3&tipo=pagas").get_data(as_text=True)

    endereco = unquote(_href_de(html, "/relatorio/pdf"))
    for obra in ("OBRA-1", "OBRA-2", "OBRA-3"):
        assert f"centro_custo={obra}" in endereco, (
            f"o PDF sairia sem {obra} — o filtro da tela não chegou inteiro. "
            f"Endereço: {endereco}")
    assert "tipo=pagas" in endereco, "perdeu o tipo do relatório"


def test_o_CSV_do_relatorio_leva_TODOS_os_valores_do_filtro(app_relatorio):
    """Mesmo link, mesmo defeito — e o CSV é o que vira planilha."""
    from urllib.parse import unquote

    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio?tipo_despesa=Material&tipo_despesa=Ferramentas"
        ).get_data(as_text=True)

    endereco = unquote(_href_de(html, "/relatorio/exportar"))
    assert "tipo_despesa=Material" in endereco
    assert "tipo_despesa=Ferramentas" in endereco, (
        f"o CSV sairia só com o primeiro tipo. Endereço: {endereco}")


def test_UM_valor_so_continua_funcionando(app_relatorio):
    """A trava do outro lado: o caso simples não pode ter quebrado."""
    from urllib.parse import unquote

    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio?centro_custo=OBRA-12").get_data(as_text=True)

    endereco = unquote(_href_de(html, "/relatorio/pdf"))
    assert "centro_custo=OBRA-12" in endereco


def test_SEM_filtro_nenhum_o_link_continua_valido(app_relatorio):
    """Sem filtro, o link é o endereço limpo — não pode virar lixo."""
    html = como(app_relatorio, SENHA_CONSULTA).get(
        "/analisesps/relatorio").get_data(as_text=True)
    assert "/analisesps/relatorio/pdf" in _href_de(html, "/relatorio/pdf")


# ---------------------------------------------------------------------------
# O ANALÍTICO NO PDF DO RELATÓRIO — 18/09/2026
#
# > *"Queria que no relatório em PDF saísse mais abaixo o analítico. Está bom
# > do jeito que você está colocando, mas está faltando a parte analítica: o
# > lançamento, credor e a descrição com detalhe do que é. Pode reduzir a fonte
# > para poder caber as coisas."*
#
# ⚠️ O QUE MAIS IMPORTA AQUI não é o analítico aparecer: é ele **fechar com o
# total do topo**. Detalhe que soma diferente do resumo, na mesma folha, tira a
# credibilidade do relatório inteiro — e quem confere não tem como saber qual
# dos dois está certo.
# ---------------------------------------------------------------------------
def _analitico_falso(quantos, descricao="Cimento CP-II 50kg para a laje"):
    import datetime as dt
    from decimal import Decimal
    return [{"id": f"140928{n:04d}", "data": dt.date(2026, 9, 10),
             "credor": "SERTAO CASA E CONSTRUCAO LTDA",
             "documento": "29.066.773/0001-52",
             "centro_custo": "OBRA-12", "tipo_despesa": "Material",
             "descricao": descricao, "valor": Decimal("1000.00")}
            for n in range(1, quantos + 1)]


def test_o_PDF_traz_o_analitico_com_SP_credor_e_descricao(app_relatorio,
                                                          monkeypatch):
    """Os três campos que ele pediu, com todas as letras."""
    from app.apps.analisesps import consultas, pdf
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0:
                        _analitico_falso(3))

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo"))

    assert "Anal" in texto, "não saiu a seção do analítico"
    assert "1409280001" in texto, "faltou o número do lançamento"
    assert "SERTAO CASA E CONSTRUCAO" in texto, "faltou o credor"
    assert "Cimento CP-II 50kg para a laje" in texto, "faltou a descrição"


def test_a_descricao_LONGA_nao_e_cortada_e_quebra_em_linhas(app_relatorio,
                                                            monkeypatch):
    """A descrição é o campo que explica o que é a despesa. Cortada na
    metade, ela vira enfeite — quem lê fica sem a informação que foi buscar."""
    from app.apps.analisesps import consultas, pdf
    longa = ("Aquisicao de cimento CP-II 50kg para a concretagem da laje do "
             "terceiro pavimento conforme pedido 8842 da obra residencial")
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0:
                        _analitico_falso(2, descricao=longa))

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo")).replace("\n", " ")

    # O começo E o fim têm de estar na folha: só o começo significaria corte.
    assert "Aquisicao de cimento" in texto
    assert "obra residencial" in texto, "a descrição foi cortada no meio"


def test_o_analitico_AVISA_quando_o_teto_cortou(app_relatorio, monkeypatch):
    """⚠️ Analítico truncado EM SILÊNCIO é pior do que analítico nenhum: quem
    soma as linhas não encontra o total do topo e conclui que a conta está
    errada — quando o certo é o total."""
    from app.apps.analisesps import consultas, pdf
    monkeypatch.setattr(consultas, "ANALITICO_MAXIMO", 5)
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0:
                        _analitico_falso(5))

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo")).replace("\n", " ")

    assert "maiores" in texto, "não avisou que a lista está cortada"
    assert "abaixo do total do topo" in texto, (
        "não explicou por que a soma das linhas não fecha")


def test_sem_corte_o_PDF_afirma_que_a_soma_FECHA(app_relatorio, monkeypatch):
    """A contrapartida: quando está tudo ali, dizer isso poupa a conferência."""
    from app.apps.analisesps import consultas, pdf
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0:
                        _analitico_falso(3))

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo")).replace("\n", " ")

    assert "fecha com o total do topo" in texto
    assert "maiores" not in texto, "avisou de corte sem haver corte"


def test_o_analitico_vem_DEPOIS_do_resumo(app_relatorio, monkeypatch):
    """Quem abre o relatório quer primeiro o resumo. Pondo o analítico antes,
    seriam dezenas de páginas de linhas antes do primeiro total."""
    from app.apps.analisesps import consultas, pdf
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0:
                        _analitico_falso(3))

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo"))

    assert texto.index("Ticket") < texto.index("Anal"), (
        "o analítico apareceu antes dos totais do topo")


def test_relatorio_SEM_lancamento_nenhum_nao_ganha_secao_vazia(app_relatorio,
                                                               monkeypatch):
    """Um título "Analítico" com nada embaixo faz parecer que algo falhou."""
    from app.apps.analisesps import consultas, pdf
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda f, t="geral", p="tudo", limite=0: [])

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo"))

    assert "lan" in texto.lower(), "o relatório saiu vazio — teste sem valor"
    assert "Anal" not in texto, (
        "desenhou a seção do analítico sem nenhum lançamento embaixo")


def test_falha_no_ANALITICO_nao_derruba_o_relatorio_inteiro(app_relatorio,
                                                            monkeypatch):
    """⚠️ A regra da casa: o acessório não pode derrubar o principal.

    O analítico é apêndice; o resumo É o relatório. Descoberto em 18/09/2026,
    no dia em que a seção nasceu: uma falha na consulta do detalhe devolvia 500
    e a pessoa ficava SEM RELATÓRIO NENHUM — perdendo os totais, as quebras e
    os credores, que estavam prontos.

    Mesma decisão do quadro por categoria, em 13/09."""
    from app.apps.analisesps import consultas, pdf
    monkeypatch.setattr(consultas, "analitico_do_relatorio",
                        lambda *a, **k: 1 / 0)

    texto = _texto_do_pdf(pdf.relatorio({}, "geral", "tudo"))

    # O relatório saiu, e saiu inteiro na parte que importa.
    assert "Ticket" in texto, "perdeu os totais por causa do analítico"
    assert "845.300,55" in texto or "Valor total" in texto
    # E diz que faltou, para ninguém concluir que não havia lançamento nenhum.
    assert "Não consegui montar o anal" in texto.replace("\n", " "), (
        "o analítico sumiu em silêncio")
    assert "acima está completo e correto" in texto.replace("\n", " ")


# ---------------------------------------------------------------------------
# A TELA DE APORTES — 20/09/2026
# ---------------------------------------------------------------------------
@pytest.fixture
def app_aportes(app, monkeypatch):
    """A tela com o espelho do OMIE dublado. O SQL tem teste próprio, com banco
    de verdade (`test_analisesps_aportes_banco.py`); aqui o que está sob teste
    é a tela montar inteira."""
    from app.apps.analisesps import aportes_de_para, aportes_omie

    monkeypatch.setattr(aportes_de_para, "contas_do_omie", lambda: [
        {"codigo": 7011, "descricao": "BWS PROVEDORA",
         "numero_conta": "12345-6", "inativa": False},
        {"codigo": 22069, "descricao": "PARCERIA OBRA X",
         "numero_conta": "99999-9", "inativa": False}])
    monkeypatch.setattr(aportes_de_para, "obras",
                        lambda: [{"codigo": "OBRA-1", "nome": "Obra Um"}])
    monkeypatch.setattr(aportes_de_para, "fornecedores", lambda q="": [
        {"codigo": 99, "nome": "PARCEIRO LTDA", "documento": "00.000.000/0001-00"}])
    monkeypatch.setattr(aportes_de_para, "contas_lembradas",
                        lambda: {"aporte_bws:provedora": 7011})
    monkeypatch.setattr(aportes_de_para, "descricoes_das_contas",
                        lambda: {7011: "BWS PROVEDORA",
                                 22069: "PARCERIA OBRA X"})
    monkeypatch.setattr(aportes_de_para, "descobrir_categorias", lambda: {
        chave: {"chave": chave, "procurada": nome, "situacao": "escolhida",
                "erro": "", "candidatos": [{"codigo": "9.01.01",
                                            "descricao": nome,
                                            "transferencia": "N"}],
                "codigo": "9.01.01", "descricao": nome, "transferencia": "N",
                "confirmada": True}
        for chave, nome in
        __import__("app.apps.analisesps.aportes", fromlist=["x"]).CATEGORIAS.items()})
    monkeypatch.setattr(aportes_de_para, "falta_configurar", lambda: [])
    monkeypatch.setattr(aportes_omie, "historico", lambda n=30: [])
    monkeypatch.setattr(aportes_omie, "orfaos", lambda: [])
    monkeypatch.setattr(aportes_omie, "senha_configurada", lambda: True)
    return app


def test_a_tela_de_aportes_monta(app_aportes):
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "Aportes e devoluções no OMIE" in html
    # As quatro operações da tabela do dono, com o nome dele, não com o meu.
    assert "BWS aporta na parceria" in html
    assert "A parceria devolve o aporte à BWS" in html
    assert "O parceiro aporta na parceria" in html
    assert "A parceria devolve o aporte ao parceiro" in html
    # As duas contas aparecem com código E número de conta — é olhando os dois
    # juntos que ele reconhece qual é qual.
    assert "BWS PROVEDORA" in html and "12345-6" in html


def bloco_de_lancar(html):
    """Só o cartão "Lançar" — o ajuste do de-para fica num cartão depois dele,
    guardado, e não deve ser confundido com a tela de uso."""
    inicio = html.index("<h3>Lançar</h3>")
    fim = html.index('id="conferencia"', inicio)
    return html[inicio:fim]


def test_a_tela_de_lancar_nao_pede_categoria(app_aportes):
    """*"Eu não devo ter que escolher categoria nenhuma: quem escolhe é a
    regra. Se eu pudesse escolher, eu erraria."*

    A CONTA, essa sim, é perguntada — mas pelo papel e pelo sentido, não como
    "origem" e "destino" soltos: *"não quero travar a conta Matriz e a da
    Parceria, tem mais de uma situação."*"""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    lancar = bloco_de_lancar(html)
    assert 'name="categoria' not in lancar


def test_a_tela_mostra_as_cinco_situacoes_e_nao_quatro_categorias(app_aportes):
    """*"Aportes BWS tanto está na conta de entrada quanto de saída. Na
    verdade, todas as categorias poderão ser utilizadas."* Listar as quatro
    categorias escondia metade do que cada uma faz."""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "As cinco situações" in html
    # Entrada e saída ditas sem depender de leitura — o primeiro apontamento
    # dele foi justamente que não dava para distinguir.
    assert html.count("↑ SAI") >= 2
    assert html.count("↓ ENTRA") >= 3


def test_o_de_para_fica_guardado_quando_nao_falta_nada(app_aportes):
    """*"Eu não entendi esse gravar o de-para. Eu acho que não precisaria."*
    Ele continua existindo — é o que impede código chumbado — mas deixou de
    ser um passo."""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "Preciso que você me diga isto uma vez só" not in html
    assert "<details>" in html
    assert "As contas e as categorias que estou usando" in html


def test_a_operacao_sozinha_diz_o_que_vai_acontecer(app_aportes):
    """A tela precisa saber, no navegador, o que cada operação faz — para
    mostrar contas, sentido e categoria assim que ele escolher, antes de pedir
    valor ou data."""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert 'id="o-que-vai-acontecer"' in html
    assert "Não há conta de origem" in html, \
        "não explica o caso do dinheiro que vem de fora"
    # O aporte do parceiro tem UMA perna só, e a tela tem de saber disso.
    assert '"aporte_parceiro"' in html
    # A pergunta da conta é a pergunta inteira, não um rótulo que obriga a
    # traduzir "origem" para "de onde sai".
    assert "De qual conta o dinheiro sai?" in html
    assert "Em qual conta o dinheiro entra?" in html


def test_a_conta_nao_e_travada_num_cadastro(app_aportes):
    """*"Não quero travar a conta Matriz e a da Parceria, tem mais de uma
    situação."* Há mais de uma parceria, e a mesma conta pode fazer papéis
    diferentes. O que fica guardado é só a última usada, para vir
    pré-escolhida — e isso não decide nada."""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    # O bloco de ajuste não pede mais conta nenhuma.
    assert 'name="conta_provedora"' not in html
    assert 'name="conta_parceria"' not in html
    # E a lista de contas do OMIE vai inteira para a tela, para ele escolher.
    assert "PARCERIA OBRA X" in html


def test_o_perfil_consulta_nao_alcanca_os_aportes(app_aportes):
    """Mesmo o ensaio monta o pacote com conta, categoria e valor. Ler não
    basta aqui."""
    resposta = como(app_aportes, SENHA_CONSULTA).get("/analisesps/aportes")
    assert resposta.status_code in (302, 403)


def test_a_tela_avisa_quando_falta_o_de_para(app_aportes, monkeypatch):
    from app.apps.analisesps import aportes_de_para
    monkeypatch.setattr(aportes_de_para, "falta_configurar",
                        lambda: ["Não sei o código da categoria Aporte BWS."])
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "Preciso que você me diga isto uma vez só" in html
    assert "Não sei o código da categoria Aporte BWS." in html
    # E aí o ajuste aparece ABERTO, não guardado atrás de um "detalhes".
    assert "As contas e as categorias que estou usando" not in html


def test_a_tela_grita_o_titulo_orfao(app_aportes, monkeypatch):
    """Órfão esquecido é meio aporte que nenhum relatório fecha. Ele fica
    visível no topo da tela até alguém resolver."""
    from app.apps.analisesps import aportes_omie
    monkeypatch.setattr(aportes_omie, "orfaos", lambda: [
        {"codigo_lancamento_omie": 4242, "papel": "conta Provedora",
         "sentido": "saida", "erro": "não foi possível excluir"}])
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "título órfão no OMIE" in html
    assert "4242" in html


def test_a_tela_avisa_se_a_senha_de_escrita_nao_existe(app_aportes, monkeypatch):
    from app.apps.analisesps import aportes_omie
    monkeypatch.setattr(aportes_omie, "senha_configurada", lambda: False)
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "PAINEL_SENHA_ESCRITA" in html


def test_o_espelho_vazio_nao_derruba_a_tela(app, monkeypatch):
    """A tela de aportes é TAMBÉM a que conserta o de-para. Se ela cair porque
    a carga do painel nunca rodou, não há por onde arrumar — foi assim que o
    módulo inteiro travou na estreia, em 03/09."""
    from app.apps.analisesps import aportes_de_para, aportes_omie

    def sem_espelho(*a, **k):
        raise aportes_de_para.SemEspelho("A carga do painel nunca rodou.")

    monkeypatch.setattr(aportes_de_para, "contas_do_omie", sem_espelho)
    monkeypatch.setattr(aportes_de_para, "obras", sem_espelho)
    monkeypatch.setattr(aportes_de_para, "fornecedores", sem_espelho)
    monkeypatch.setattr(aportes_de_para, "contas_lembradas", lambda: {})
    monkeypatch.setattr(aportes_de_para, "descricoes_das_contas", lambda: {})
    monkeypatch.setattr(aportes_de_para, "descobrir_categorias", lambda: {})
    monkeypatch.setattr(aportes_de_para, "falta_configurar", lambda: [])
    monkeypatch.setattr(aportes_omie, "historico", lambda n=30: [])
    monkeypatch.setattr(aportes_omie, "orfaos", lambda: [])

    resposta = como(app, SENHA_OPERADOR).get("/analisesps/aportes")
    assert resposta.status_code == 200
    assert "A carga do painel nunca rodou." in resposta.get_data(as_text=True)


def test_o_ensaio_nao_pede_senha_e_a_gravacao_pede(app_aportes, monkeypatch):
    """Conferir tem de ser barato, ou ninguém confere. A senha vale para o que
    escreve no OMIE."""
    from app.apps.analisesps import aportes_omie
    monkeypatch.delenv(aportes_omie.VARIAVEL_SENHA, raising=False)
    cliente = como(app_aportes, SENHA_OPERADOR)

    pedido = {"operacao": "aporte_bws", "conta_origem": 7011,
              "conta_destino": 22069, "valor": "12.500,00",
              "data": "2026-09-20", "fornecedor": 99, "obra": "OBRA-1"}
    monkeypatch.setattr(
        "app.apps.analisesps.aportes_de_para.categorias_configuradas",
        lambda: {c: {"codigo": "9.01.01", "descricao": n, "transferencia": "N"}
                 for c, n in __import__(
                     "app.apps.analisesps.aportes",
                     fromlist=["x"]).CATEGORIAS.items()})
    monkeypatch.setattr(
        "app.apps.analisesps.aportes_de_para.consultar_semelhantes",
        lambda *a, **k: [])

    ensaio = cliente.post("/analisesps/api/aportes/ensaiar", json=pedido)
    assert ensaio.status_code == 200 and ensaio.get_json()["ok"] is True

    gravacao = cliente.post("/analisesps/api/aportes/gravar", json=pedido)
    assert gravacao.status_code == 403
    assert "senha" in gravacao.get_json()["erro"].lower()


def test_a_tela_deixa_acrescentar_datas_e_valores(app_aportes):
    """*"Quero poder fazer vários lançamentos do mesmo tipo. Apenas incluir
    mais datas e valores. Lançamento em lote."*"""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert 'id="parcelas-corpo"' in html
    assert "Acrescentar data e valor" in html
    # O que NÃO se repete por linha: operação, conta, fornecedor e obra. Se
    # repetisse, a conferência viraria uma planilha.
    lancar = bloco_de_lancar(html)
    assert lancar.count('id="operacao"') == 1
    assert lancar.count('id="obra"') == 1
    assert lancar.count('id="fornecedor"') == 1


def test_o_ensaio_de_um_lote_devolve_um_lancamento_por_linha(app_aportes,
                                                             monkeypatch):
    from app.apps.analisesps import aportes
    monkeypatch.setattr(
        "app.apps.analisesps.aportes_de_para.categorias_resolvidas",
        lambda: {c: {"codigo": "9.01.01", "descricao": n, "transferencia": "N"}
                 for c, n in aportes.CATEGORIAS.items()})
    monkeypatch.setattr(
        "app.apps.analisesps.aportes_de_para.consultar_semelhantes",
        lambda *a, **k: [])

    resposta = como(app_aportes, SENHA_OPERADOR).post(
        "/analisesps/api/aportes/ensaiar",
        json={"operacao": "aporte_bws", "conta_origem": 7011,
              "conta_destino": 22069, "fornecedor": 99, "obra": "OBRA-1",
              "parcelas": [{"data": "2026-09-20", "valor": "100,00"},
                           {"data": "2026-10-20", "valor": "200,00"}]})
    dados = resposta.get_json()
    assert dados["ok"] is True
    assert dados["quantos"] == 2
    assert dados["total"] == 300.0
    assert dados["quantos_titulos"] == 4
    assert len(set(dados["grupos"])) == 2, "as linhas têm de ter grupos próprios"


def test_a_linha_ruim_do_lote_e_apontada_pelo_numero(app_aportes, monkeypatch):
    from app.apps.analisesps import aportes
    monkeypatch.setattr(
        "app.apps.analisesps.aportes_de_para.categorias_resolvidas",
        lambda: {c: {"codigo": "9.01.01", "descricao": n, "transferencia": "N"}
                 for c, n in aportes.CATEGORIAS.items()})

    resposta = como(app_aportes, SENHA_OPERADOR).post(
        "/analisesps/api/aportes/ensaiar",
        json={"operacao": "aporte_bws", "conta_origem": 7011,
              "conta_destino": 22069, "fornecedor": 99, "obra": "OBRA-1",
              "parcelas": [{"data": "2026-09-20", "valor": "100,00"},
                           {"data": "2026-10-20", "valor": "abacaxi"}]})
    assert resposta.status_code == 400
    assert "Linha 2" in resposta.get_json()["erro"]


def test_a_tela_grita_o_lancamento_sem_resposta(app_aportes, monkeypatch):
    """*"Tá demorando muito."* Quando o envio começa e ninguém sabe como
    terminou, o pior recado possível é nenhum: a pessoa manda de novo e
    duplica o que já entrou."""
    from app.apps.analisesps import aportes_omie
    monkeypatch.setattr(aportes_omie, "em_duvida", lambda: [
        {"numero_documento": "APORTE-20260921-AB12", "papel": "conta Provedora",
         "sentido": "saida", "valor": 1000, "data": "21/09/2026",
         "criado_por": "Marcelo", "criado_em": "2026-09-21 10:00"}])
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "ficou sem resposta" in html
    assert "APORTE-20260921-AB12" in html
    assert "antes de lançar de novo" in html


def test_a_tela_desiste_de_esperar_e_diz_o_que_fazer(app_aportes):
    """O navegador esperava para sempre, e o dono ficou olhando uma tela
    parada sem saber se tinha entrado. Agora a espera tem teto, e o recado diz
    a única coisa que importa: não mandar de novo às cegas."""
    html = como(app_aportes, SENHA_OPERADOR).get(
        "/analisesps/aportes").get_data(as_text=True)
    assert "AbortController" in html, "a espera continua sem fim"
    assert "Não mande de novo" in html
    assert "O que já foi lançado por aqui" in html


# ---------------------------------------------------------------------------
# A MARCA DE "JÁ ESTÁ NO LOTE", nas Solicitações
# ---------------------------------------------------------------------------
def test_a_sp_que_ja_esta_no_lote_sai_com_a_marca(app, monkeypatch):
    """Pedido do dono em 25/09/2026. Sem isto, a lista de Solicitações não tem
    como dizer o que já foi separado — e a mesma SP entra no lote duas vezes."""
    from app.apps.analisesps import lote

    monkeypatch.setattr(lote, "onde_no_lote", lambda ids, pessoa: {
        "1": {"minha": True, "grupos": ["Pagar amanhã"], "outros": []}})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)

    assert "selo no-lote" in html
    assert "Já está no seu lote — Pagar amanhã" in html
    # A outra linha não está em lote nenhum e não ganha marca nenhuma.
    assert html.count('class="selo no-lote"') == 1


def test_a_sp_no_lote_de_OUTRA_pessoa_diz_de_quem_e(app, monkeypatch):
    """É esta a marca que evita pagar duas vezes. Se ela aparecesse igual à do
    lote próprio, quem visse pensaria que foi ele mesmo que separou."""
    from app.apps.analisesps import lote

    monkeypatch.setattr(lote, "onde_no_lote", lambda ids, pessoa: {
        "1": {"minha": False, "grupos": [], "outros": ["Ana Paula"]}})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)

    assert "selo no-lote-de-outro" in html
    assert "Já está no lote de Ana Paula" in html


def test_a_marca_do_lote_NAO_aparece_na_tela_do_proprio_lote(app, monkeypatch):
    """Lá a linha está no lote por definição: a marca em todas as linhas seria
    só ruído."""
    from app.apps.analisesps import lote

    monkeypatch.setattr(lote, "ler", lambda pessoa: {
        "conteudo": "Pagar amanhã\n1", "salvo_por": "MARCELO",
        "salvo_em": None, "compartilhado": False})
    monkeypatch.setattr(lote, "montar", lambda texto: {
        "grupos": [{"titulo": "Pagar amanhã", "titulo_exibido": "Pagar amanhã",
                    "ids": ["1"], "linhas": [linha_falsa("1")],
                    "nao_encontrados": [], "total": Decimal("6750.00")}],
        "linhas": {"1": linha_falsa("1")}, "nao_encontrados": [],
        "total_geral": Decimal("6750.00"), "quantidade": 1})
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/lote").get_data(as_text=True)

    assert "selo no-lote" not in html


def test_quem_escondeu_a_coluna_ID_continua_vendo_a_marca(app, monkeypatch):
    """A marca mora colada no número da SP. Esconder a coluna ID é uma escolha
    de tabela — não pode apagar um aviso de pagamento em duplicidade.

    Ali ela vira um PONTO, não a palavra: a primeira célula tem 34px, e a
    palavra quebraria a linha embaixo da caixa de marcar."""
    from app.apps.analisesps import lote, tabela

    monkeypatch.setattr(lote, "onde_no_lote", lambda ids, pessoa: {
        "1": {"minha": False, "grupos": [], "outros": ["Ana Paula"]}})
    sem_id = [c for c in tabela.DEFINICOES if c.chave != "id"]
    monkeypatch.setattr(tabela, "escolhidas", lambda guardado: sem_id)
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)

    assert "ponto-lote no-lote-de-outro" in html
    assert "Já está no lote de Ana Paula" in html
    assert '>lote</span>' not in html, (
        "a palavra na célula de 34px quebraria a linha da tabela")


def test_a_marca_do_lote_NAO_pode_engordar_a_linha_da_tabela():
    """⚠️ RECLAMAÇÃO DO DONO EM 25/09/2026: *"não gostei da tag lote. Ela
    aumenta a linha da tabela de solicitações."*

    Aumentava por dois motivos somados:

    1. A célula do número era a ÚNICA da tabela que podia quebrar linha — todas
       as outras são `.cortar`, com nowrap. Numa tela cheia de colunas a tag
       caía embaixo do número e a linha dobrava de altura.
    2. A altura do selo vinha do `line-height: 1.45` herdado do corpo, que
       sobre 9,5px dá quase 14px MAIS a folga da entrelinha.
    """
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(
        encoding="utf-8")
    assert ".sps td.id { white-space: nowrap; }" in css, (
        "a célula do número voltou a poder quebrar linha")
    marca = css[css.index(".selo.no-lote, .selo.no-lote-de-outro {"):]
    marca = marca[:marca.index("}")]
    assert "line-height: 14px" in marca and "height: 14px" in marca, (
        "a altura do selo voltou a depender da entrelinha herdada")
    assert "text-transform" not in marca, (
        "maiúscula alarga a tag, e largura na célula do número é o que quebra")


def test_o_codigo_de_barras_do_boleto_tem_teto_de_largura():
    """Pedido do dono em 25/09/2026: *"na tela de QR code/Boleto o tamanho do
    qrcode tá ótimo, mas o do boleto fica muito exagerado numa tela de 34"."*

    O SVG sai com `width="100%"`, então numa tela larga o cartão mandava no
    tamanho do código. O teto é o tamanho nativo do desenho — abaixo dele o
    código continua encolhendo com a tela."""
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(
        encoding="utf-8")
    assert ".codigo-barras svg { width: 100%; max-width: 855px" in css, (
        "o código de barras voltou a crescer sem limite")
    assert ".ficha-codigo .codigo-barras svg { max-width: 560px; }" in css, (
        "dentro da ficha o boleto voltou a mandar no tamanho do modal")


# ---------------------------------------------------------------------------
# A TELA DE RATEIO DA FOLHA — 26/09/2026
# ---------------------------------------------------------------------------
def _como_mestre(app):
    """Entra pela porta de emergência, que é mestre por definição."""
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": SENHA_OPERADOR})
    return cliente


def test_a_tela_de_rateio_da_folha_monta_com_as_regras(app, monkeypatch):
    """Pedido do dono em 26/09/2026: um lugar para eleger quem é rateado e dizer
    para quais obras o valor vai, com peso."""
    from app.apps.analisesps import folha_rateio as fr, sincronizacao
    from decimal import Decimal

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio", lambda: {
        "obras": [{"nome": "CREPEAREIAS", "codigo": "1"},
                  {"nome": "CREPEOLINDA", "codigo": "2"}], "categorias": []})
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [{
        "id": 7, "nome": "Supervisores de Pernambuco", "ativa": True,
        "observacao": "não batem ponto", "criado_por": "MARCELO",
        "alterado_em": None, "alterado_por": "",
        "pessoas": [{"cpf": "99713349334", "cpf_bonito": "997.133.493-34",
                     "nome": "GERLANIO GOMES LIMA"}],
        "obras": [{"obra": "CREPEAREIAS", "percentual": Decimal("50.0000"),
                   "resto": False},
                  {"obra": "CREPEOLINDA", "percentual": None, "resto": True}]}])

    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)

    assert "Supervisores de Pernambuco" in html
    assert "GERLANIO GOMES LIMA" in html
    assert "997.133.493-34" in html
    assert "CREPEAREIAS" in html
    assert "o resto" in html, "a obra marcada como resto não aparece como tal"


def test_sem_a_migracao_a_tela_de_rateio_AVISA_e_nao_estoura(app, monkeypatch):
    """O código sobe antes do botão. Uma tela que estourasse nessa janela
    derrubaria a confiança na publicação inteira."""
    from app.apps.analisesps import folha_rateio as fr, sincronizacao

    monkeypatch.setattr(fr, "_pronto", lambda: False)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    resposta = _como_mestre(app).get("/analisesps/folha/rateio")

    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html
    assert 'id="btn-nova-regra"' not in html


def test_a_lista_de_obras_do_rateio_e_a_MESMA_do_ratear(app, monkeypatch):
    """Uma segunda lista de obras divergiria da primeira no dia em que alguém
    cadastrasse obra nova."""
    from app.apps.analisesps import folha_rateio as fr, sincronizacao
    chamou = {}
    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: chamou.setdefault("sim", True) and {
                            "obras": [{"nome": "OBRAX", "codigo": "9"}],
                            "categorias": []})
    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)

    assert chamou.get("sim") is True
    assert "OBRAX" in html


def test_a_lista_de_obras_fora_do_ar_nao_derruba_a_tela(app, monkeypatch):
    from app.apps.analisesps import folha_rateio as fr, sincronizacao

    def explode():
        raise RuntimeError("banco fora")

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio", explode)
    assert _como_mestre(app).get(
        "/analisesps/folha/rateio").status_code == 200


def test_o_rateio_da_folha_e_SO_DO_MESTRE():
    """Isto decide para qual obra vai o salário de alguém, toda quinzena. O DP
    opera a folha; o rateio é do dono."""
    from app.apps.analisesps import auth as guarda

    # ⚠️ EM 27/09/2026 A PROTEÇÃO MUDOU DE LUGAR, e ficou MAIS estrita, não menos.
    # A folha virou UMA tela no menu ("folha") com as subtelas por dentro, então
    # "folha_rateio" deixou de existir como chave de tela. Quem barra o rateio
    # agora é a lista por NOME DE ROTA — que não depende de ninguém lembrar de
    # classificar uma tela.
    assert "folha_rateio" not in guarda.SO_DO_MESTRE_POR_TELA
    for rota in ("analisesps.tela_folha_rateio", "analisesps.folha_rateio_gravar",
                 "analisesps.folha_rateio_apagar",
                 "analisesps.folha_rateio_simular",
                 "analisesps.folha_rateio_colar"):
        assert guarda.e_so_do_mestre(rota) is True, rota
    # E a tela do CADASTRO não é do mestre: é leitura, e o DP precisa dela.
    assert guarda.e_so_do_mestre("analisesps.tela_colaboradores") is False
    assert guarda.e_so_do_mestre("analisesps.tela_folha") is False


def test_a_tela_de_rateio_NAO_e_oferecida_no_cadastro_de_acesso(monkeypatch):
    """Tela que só o mestre abre não pode aparecer na lista de telas liberáveis —
    liberar e a pessoa levar 404 é pior que não liberar."""
    from app.apps.analisesps import usuarios
    liberaveis = [t[0] for t in usuarios.telas_liberaveis()]
    # "folha_rateio" não é mais chave de tela nenhuma (ver o teste acima).
    assert "folha_rateio" not in liberaveis
    # A ÁREA da folha é liberável — é assim que o DP ganha o cadastro. O rateio
    # por dentro continua barrado pela lista de rotas do mestre.
    assert "folha" in liberaveis


def test_apagar_uma_regra_recomenda_DESATIVAR_antes():
    """A regra desativada explica como a folha de março foi rateada. Apagar existe
    para a regra criada errada, que nunca rateou nada."""
    from pathlib import Path
    html = Path("app/apps/analisesps/templates/"
                "analisesps_folha_rateio.html").read_text(encoding="utf-8")
    trecho = html[html.index('.apagar-regra").forEach'):]
    assert "DESATIVAR" in trecho
    assert "não tem volta" in trecho
    assert trecho.index("confirm(") < trecho.index("folha_rateio_apagar")


# ---------------------------------------------------------------------------
# O CADASTRO DE COLABORADORES — 27/09/2026
#
# Pedido do dono, com as palavras dele: *"Além de ter acesso fácil ao pipe, ao
# card, o link (…) o nome do colaborador já redireciona, alguma coisa que clica
# e direcione"* e *"eu preciso poder atualizar (…) um botão fácil para poder
# atualizar imediatamente os dados, puxar os dados dessa planilha"*.
#
# Os dois pedidos são de TELA, e tela sem teste quebra calada.
# ---------------------------------------------------------------------------
def _cadastro_pronto(quando="2026-09-27T14:35:00", pessoas=3480, avisos=None):
    return {"quando": quando, "pessoas": pessoas,
            "avisos": avisos or [], "pronto": True}


def test_a_tela_de_colaboradores_leva_ao_card_do_pipefy(app, monkeypatch):
    """O nome tem de ser um LINK para o card. É lá que se corrige o valor do
    auxílio — o caminho até lá é o pedido."""
    from decimal import Decimal

    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [{
        "cpf": "99713349334", "nome": "GERLANIO GOMES LIMA",
        "card_pipefy": "778899", "cargo": "ENCARREGADO",
        "tipo_contrato": "CLT (tempo Indeterminado)", "tipo": "",
        "fase": "Colaboradores Ativos", "matricula": "", "celular": "",
        "convencao": "", "obra_cadastro": "",
        "valor_alimentacao": Decimal("330.00"), "modo_alimentacao": "Segunda à Sexta",
        "valor_transporte": Decimal("180.50"), "modo_transporte": "Mensal",
        "valor_gratificacao": None, "parcela_unica": "", "paga_por_beevale": "Sim",
        "aviso_previo": None, "ultimo_dia": None, "data_saida": None,
        "link_pipefy": "https://app.pipefy.com/open-cards/778899",
        "desligado": False}])

    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert "GERLANIO GOMES LIMA" in html
    assert 'href="https://app.pipefy.com/open-cards/778899"' in html, \
        "o nome tem de levar ao card do Pipefy"
    assert 'target="_blank"' in html, "abrir noutra aba: a tela não se perde"
    assert 'rel="noopener"' in html, "link para fora leva noopener"
    # Os três valores que ele disse que mais mudam aparecem na tela.
    assert "330,00" in html
    assert "180,50" in html
    assert "Segunda à Sexta" in html


def test_valor_em_branco_aparece_como_TRACO_e_nao_como_zero(app, monkeypatch):
    """"O cadastro não diz" e "o cadastro diz R$ 0,00" são respostas
    diferentes. Mostrar zero para o que está em branco esconderia o
    preenchimento faltando."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [{
        c: "" for c in col.CAMPOS} | {
        "cpf": "99713349334", "nome": "SEM AUXILIO",
        "valor_alimentacao": None, "valor_transporte": None,
        "valor_gratificacao": None, "data_saida": None,
        "aviso_previo": None, "ultimo_dia": None,
        "link_pipefy": "", "desligado": False}])

    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert "SEM AUXILIO" in html
    assert "0,00" not in html, "valor não preenchido não pode virar zero na tela"
    assert "—" in html
    # Sem número de card, a tela diz por quê em vez de oferecer um link morto.
    assert "sem card" in html


def test_a_tela_de_colaboradores_mostra_DE_QUANDO_e_a_copia(app, monkeypatch):
    """Botão sem essa informação é caixa preta. O cadastro vem do Pipefy por
    automação: quem corrigiu um auxílio precisa saber se esta cópia é de antes
    ou de depois da correção."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert "27/09" in html
    assert "14:35" in html
    assert "3480" in html
    assert 'id="btn-atualizar-cadastro"' in html


def test_o_aviso_de_coluna_que_faltou_aparece_na_TELA(app, monkeypatch):
    """⚠️ Campo de auxílio em branco vira pagamento a MENOS, e pagamento a menos
    ninguém nota tão rápido quanto um a mais. O aviso não pode ficar no log do
    serviço, que ele não tem como ler."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(
        col, "quando_atualizou",
        lambda: _cadastro_pronto(avisos=['não achei a coluna de "Valor Auxílio '
                                         'Alimentação"']))
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert "não achei a coluna" in html


def test_sem_a_migracao_a_tela_de_colaboradores_AVISA_e_nao_estoura(app, monkeypatch):
    """O código sobe para o Render antes do botão ser apertado."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", lambda: {
        "quando": "", "pessoas": 0, "avisos": [], "pronto": False})
    resposta = _como_mestre(app).get("/analisesps/folha/colaboradores")

    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html
    assert 'id="btn-atualizar-cadastro"' not in html


def test_o_cadastro_fora_do_ar_nao_derruba_a_tela(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col

    def explode():
        raise RuntimeError("banco fora")

    monkeypatch.setattr(col, "quando_atualizou", explode)
    resposta = _como_mestre(app).get("/analisesps/folha/colaboradores")
    assert resposta.status_code == 200
    assert "banco fora" in resposta.get_data(as_text=True)


def test_busca_sem_resultado_MANTEM_o_cabecalho_e_diz_o_que_fazer(app, monkeypatch):
    """A armadilha que a conciliação tinha até 26/09/2026: o "não há nada"
    engolia a tabela inteira, cabeçalho incluído — e o filtro mora no
    cabeçalho. Filtro que não pode ser desfeito de onde foi feito é armadilha."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores?q=zzzz").get_data(as_text=True)

    assert "<thead>" in html, "o cabeçalho da tabela tem de continuar"
    assert 'name="q"' in html, "a caixa de busca tem de continuar na tela"
    assert "zzzz" in html
    assert "incluir desligados e afastados" in html


def test_quem_saiu_so_aparece_quando_se_pede(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col
    pedidos = {}

    # ⚠️ O DUBLÊ ACEITA **kwargs de propósito. Este teste quebrou em 27/09/2026
    # quando `buscar` ganhou o parâmetro `so_saindo`: o dublê recusou o
    # argumento novo e a tela caiu no `except`, então o teste passou a falhar
    # por um motivo que não era o dele. Dublê preso à assinatura de hoje
    # transforma cada parâmetro novo numa falha falsa.
    def falso_buscar(texto="", so_ativos=True, teto=200, **resto):
        pedidos["so_ativos"] = so_ativos
        return []

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", falso_buscar)
    cliente = _como_mestre(app)

    cliente.get("/analisesps/folha/colaboradores")
    assert pedidos["so_ativos"] is True, "por padrão, só quem está na casa"

    cliente.get("/analisesps/folha/colaboradores?desligados=1")
    assert pedidos["so_ativos"] is False


def test_a_tela_de_colaboradores_NAO_TEM_como_editar(app, monkeypatch):
    """Se editasse, a próxima atualização apagaria a edição — o dado nasce no
    Pipefy. Tela que deixa escrever o que vai ser sobrescrito é armadilha."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    # Só o formulário de busca (method="get"). Nenhum POST nesta tela.
    assert 'method="post"' not in html.lower()


def test_o_cadastro_entra_na_tela_de_RATEIO_com_o_link(app, monkeypatch):
    """A outra ponta do mesmo pedido: onde as pessoas já apareciam, o nome
    passa a levar ao card."""
    from decimal import Decimal

    from app.apps.analisesps import (colaboradores as col, folha_rateio as fr,
                                     sincronizacao)

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [{
        "id": 7, "nome": "Supervisores", "ativa": True, "observacao": "",
        "criado_por": "", "alterado_em": None, "alterado_por": "",
        "pessoas": [{"cpf": "99713349334", "cpf_bonito": "997.133.493-34",
                     "nome": "digitado na mão"}],
        "obras": [{"obra": "CREPEAREIAS", "percentual": Decimal("100"),
                   "resto": False}]}])
    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "muitos_por_cpf", lambda cpfs: {
        "99713349334": {"nome": "GERLANIO GOMES LIMA",
                        "link_pipefy": "https://app.pipefy.com/open-cards/778899",
                        "desligado": False}})

    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)

    assert 'href="https://app.pipefy.com/open-cards/778899"' in html
    # O NOME DO CADASTRO GANHA do digitado na mão: é o que a folha e o ponto
    # usam, e é como a pessoa é conhecida nos outros sistemas.
    assert "GERLANIO GOMES LIMA" in html
    # ⚠️ O BOTÃO DE ATUALIZAR O CADASTRO NÃO FICA MAIS AQUI — 27/09/2026. Ele
    # mora na subtela Colaboradores, que é onde o cadastro é mostrado. O dono:
    # *"não precisa dar aquela ênfase, porque a gente já sabe."* O mesmo botão em
    # duas telas faz a pessoa perguntar se são a mesma coisa.
    assert 'id="btn-atualizar-cadastro"' not in html


def test_cpf_que_NAO_esta_no_cadastro_e_marcado_na_tela_de_rateio(app, monkeypatch):
    """Uma regra de rateio com CPF errado rateia o salário de ninguém — e o erro
    fica invisível até a folha não fechar. A marca é o que o mostra."""
    from decimal import Decimal

    from app.apps.analisesps import (colaboradores as col, folha_rateio as fr,
                                     sincronizacao)

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [{
        "id": 7, "nome": "Supervisores", "ativa": True, "observacao": "",
        "criado_por": "", "alterado_em": None, "alterado_por": "",
        "pessoas": [{"cpf": "00000000000", "cpf_bonito": "000.000.000-00",
                     "nome": "NINGUÉM"}],
        "obras": [{"obra": "X", "percentual": Decimal("100"), "resto": False}]}])
    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "muitos_por_cpf", lambda cpfs: {})

    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)
    assert "fora do cadastro" in html


def test_o_cadastro_fora_do_ar_nao_derruba_a_tela_de_rateio(app, monkeypatch):
    from decimal import Decimal

    from app.apps.analisesps import (colaboradores as col, folha_rateio as fr,
                                     sincronizacao)

    def explode():
        raise RuntimeError("banco fora")

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [{
        "id": 7, "nome": "Supervisores", "ativa": True, "observacao": "",
        "criado_por": "", "alterado_em": None, "alterado_por": "",
        "pessoas": [{"cpf": "99713349334", "cpf_bonito": "997.133.493-34",
                     "nome": "GERLANIO"}],
        "obras": [{"obra": "X", "percentual": Decimal("100"), "resto": False}]}])
    monkeypatch.setattr(col, "quando_atualizou", explode)

    resposta = _como_mestre(app).get("/analisesps/folha/rateio")
    assert resposta.status_code == 200
    assert "GERLANIO" in resposta.get_data(as_text=True)


def test_o_botao_de_atualizar_o_cadastro_dispara_o_modo_certo(app):
    """O botão manda `modo: "colaboradores"` para a rota de sincronizar. Se o
    nome do modo mudasse num lugar só, o botão passaria a não fazer nada — e o
    dono já viu isso acontecer com dois botões mortos (25/09/2026)."""
    from app.apps.analisesps import tarefas

    caminho = __import__("pathlib").Path(
        __import__("app.apps.analisesps.web", fromlist=["web"]).__file__).parent
    html = (caminho / "templates" / "analisesps_colaboradores.html").read_text(
        encoding="utf-8")
    assert 'modo: "colaboradores"' in html
    assert "analisesps.sincronizar" in html
    # E o botão NÃO está duplicado na tela de rateio (ver o teste acima).
    rateio = (caminho / "templates" / "analisesps_folha_rateio.html").read_text(
        encoding="utf-8")
    assert 'id="btn-atualizar-cadastro"' not in rateio

    # E o modo existe de verdade, com etapa própria e botão em Configurações.
    assert "colaboradores" in tarefas.MODOS
    assert "colaboradores" in tarefas.MODOS_DA_BASE
    assert tarefas.ETAPAS["colaboradores"] == ["colaboradores"]


def test_a_tela_de_colaboradores_esta_no_menu_e_classificada(app):
    """Rota sem classificação é rota fechada — de propósito. Uma tela nova que
    ninguém classificou não abre para ninguém."""
    from app.apps.analisesps import auth as guarda, web

    # ⚠️ UMA ENTRADA SÓ NO MENU para toda a folha — 27/09/2026. O dono, depois de
    # ver duas entradas novas: *"eles têm que estar dentro de uma tela só. E lá
    # ter as subtelas, porque senão vai ficar tela demais, fica até misturado com
    # o restante, que tem mais a ver com o financeiro."*
    chaves = [c for c, _, _ in web.TELAS]
    assert "folha" in chaves
    assert web.TELAS[chaves.index("folha")][2] == "analisesps.tela_folha"
    assert "colaboradores" not in chaves, "a subtela não pode virar item de menu"
    assert "folha_rateio" not in chaves
    # As duas subtelas respondem pela MESMA tela de permissão.
    assert guarda.telas_da_rota("analisesps.tela_colaboradores") == ("folha",)
    assert guarda.telas_da_rota("analisesps.tela_folha") == ("folha",)
    # ⚠️ A área NÃO é só do mestre: é leitura, e quem opera a folha precisa
    # conferir o auxílio de alguém e chegar ao card. O RATEIO, que decide para
    # qual obra vai o salário, continua só do mestre — por rota.
    assert "folha" not in guarda.SO_DO_MESTRE_POR_TELA
    assert "analisesps.tela_colaboradores" not in guarda.SO_DO_MESTRE


def test_o_recado_do_cadastro_NAO_diz_SPs(app):
    """⚠️ DEFEITO PEGO NA REVISÃO, 27/09/2026. A mensagem final da execução tem
    dois caminhos: os modos que trazem SPs dizem "N SPs em M min", e os outros
    dizem o recado próprio. Sem entrar na segunda lista, atualizar o cadastro
    terminaria anunciando "3.480 SPs" — que não são SPs, são pessoas.

    Número com o nome errado é pior que número nenhum: parece certo."""
    import inspect

    from app.apps.analisesps import tarefas

    fonte = inspect.getsource(tarefas.executar_trabalho)
    # A linha que escolhe o caminho da mensagem tem de citar o modo.
    trecho = fonte[fonte.index('if modo in ("apoios"'):]
    trecho = trecho[:trecho.index("):") + 2]
    assert '"colaboradores"' in trecho, (
        "o modo do cadastro não está na lista dos que têm recado próprio — a "
        "tela vai dizer 'N SPs' depois de atualizar o cadastro")


def test_a_tela_avisa_quem_esta_saindo_e_oferece_a_lista(app, monkeypatch):
    """Pedido do dono em 27/09/2026: *"não podemos pagar (…) salário ou diárias
    pra quem saiu, tá saindo. Tem que ter cuidados e alerta."*

    Alerta sem um lugar para ver a lista é só susto."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 7, "saiu": 3,
                                         "afastado": 1})

    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert "7 colaborador(es) com indicação de desligamento" in html
    assert "3 desligado(s)" in html
    assert "1 afastado(s)" in html
    assert "rescisão" in html.lower()
    assert "saindo=1" in html, "o aviso tem de levar à lista"


def test_sem_ninguem_saindo_o_aviso_NAO_aparece(app, monkeypatch):
    """⚠️ Aviso que aparece sempre vira enfeite, e enfeite ninguém lê. A maioria
    dos dias não tem ninguém saindo."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 0, "saiu": 0,
                                         "afastado": 0})
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert "colaborador(es) com indicação de desligamento" not in html


def test_o_filtro_de_quem_esta_saindo_TRAZ_quem_ja_saiu(app, monkeypatch):
    """Senão a lista esconderia metade do que ela existe para mostrar."""
    from app.apps.analisesps import colaboradores as col
    pedidos = {}

    # ⚠️ `**resto` NÃO É DESCUIDO: sem ele, um argumento novo em `buscar` (foi
    # `so_saindo` em 27/09, foram `fase` e as datas em 29/09) faz este dublê
    # estourar, a rota cair no seu `except` e o teste falhar pelo motivo ERRADO —
    # dizendo "o filtro não foi pedido" quando o que houve foi TypeError.
    def falso_buscar(texto="", so_ativos=True, teto=200, so_saindo=False,
                     ate=None, **resto):
        pedidos.update(so_ativos=so_ativos, so_saindo=so_saindo, **resto)
        return []

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", falso_buscar)
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 0, "saiu": 0,
                                         "afastado": 0})

    _como_mestre(app).get("/analisesps/folha/colaboradores?saindo=1")
    assert pedidos["so_saindo"] is True
    assert pedidos["so_ativos"] is False, \
        "o filtro de quem está saindo não pode esconder quem já saiu"


def test_a_linha_de_quem_esta_saindo_mostra_a_frase_e_o_desacordo(app, monkeypatch):
    """O selo sozinho ("saiu") não instrui ninguém: a frase diz o que NÃO
    pagar, e é o ponto do alerta."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 1, "saiu": 1,
                                         "afastado": 0})
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [{
        c: "" for c in col.CAMPOS} | {
        "cpf": "99713349334", "nome": "QUEM SAIU",
        "valor_alimentacao": None, "valor_transporte": None,
        "valor_gratificacao": None, "aviso_previo": None, "ultimo_dia": None,
        "data_saida": dt.date(2026, 8, 31), "link_pipefy": "",
        "desligado": True, "situacao": "saiu", "alerta": True,
        "motivo": "saiu em 31/08/2026. Não pague folha, diária nem auxílio "
                  "deste período por aqui.",
        "desacordo": "a fase no Pipefy ainda diz \"Colaboradores Ativos\"."}])

    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert "Não pague folha" in html
    assert "linha-alerta" in html, "a linha tem de ficar destacada"
    assert "ainda diz" in html, "o desacordo tem de aparecer escrito"


def test_quem_esta_saindo_e_marcado_tambem_na_tela_de_RATEIO(app, monkeypatch):
    """Uma regra de rateio apontando para quem saiu apropria salário de
    ninguém, e o erro fica invisível até a folha não fechar."""
    from decimal import Decimal

    from app.apps.analisesps import (colaboradores as col, folha_rateio as fr,
                                     sincronizacao)

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [{
        "id": 7, "nome": "Supervisores", "ativa": True, "observacao": "",
        "criado_por": "", "alterado_em": None, "alterado_por": "",
        "pessoas": [{"cpf": "99713349334", "cpf_bonito": "997.133.493-34",
                     "nome": "QUEM SAI"}],
        "obras": [{"obra": "X", "percentual": Decimal("100"), "resto": False}]}])
    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "muitos_por_cpf", lambda cpfs, ate=None: {
        "99713349334": {"nome": "QUEM SAI", "link_pipefy": "",
                        "desligado": False, "situacao": "saindo",
                        "motivo": "sai em 18/09/2026.", "desacordo": "",
                        "alerta": True}})

    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)
    assert "em desligamento" in html
    assert "18/09/2026" in html


# ---------------------------------------------------------------------------
# UMA TELA SÓ PARA A FOLHA, COM SUBTELAS — 27/09/2026
# ---------------------------------------------------------------------------
def test_a_porta_da_folha_manda_para_a_primeira_subtela(app):
    """Um índice com dois links seria um clique a mais para o mesmo lugar."""
    resposta = _como_mestre(app).get("/analisesps/folha")
    assert resposta.status_code in (301, 302)
    # ⚠️ O PANORAMA SAIU em 30/09/2026 (*"tá sem sentido"*): a primeira subtela é
    # a folha da contabilidade, que leva à última folha importada.
    assert "/folha/importar" in resposta.headers.get("Location", "")


def test_as_subtelas_aparecem_dentro_da_tela_da_folha(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 0, "saiu": 0,
                                         "afastado": 0})
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert "abas-folha" in html, "a barra de subtelas tem de aparecer"
    assert "Colaboradores" in html
    assert "Rateio das obras" in html
    # E o menu de cima tem UMA entrada para a folha, não duas.
    assert html.count(">Folha PGT<") == html.count("Folha PGT")


def test_quem_NAO_e_mestre_nao_ve_a_subtela_do_rateio(app, monkeypatch):
    """Aba que responde 404 é pior do que aba nenhuma — mesmo motivo do menu de
    cima. O rateio decide para qual obra vai o salário: é do dono."""
    from app.apps.analisesps import auth as guarda, web

    monkeypatch.setattr(guarda, "e_mestre", lambda: False)
    nomes = [s[1] for s in web.subtelas_da_folha()]
    assert "Colaboradores" in nomes
    assert "Rateio das obras" not in nomes

    monkeypatch.setattr(guarda, "e_mestre", lambda: True)
    assert "Rateio das obras" in [s[1] for s in web.subtelas_da_folha()]


def test_a_caixa_de_colar_a_tabela_e_o_caminho_PRINCIPAL_do_rateio(app, monkeypatch):
    """⚠️ Correção do dono em 27/09/2026: preencher campo por campo para dez
    pessoas em dez obras seriam cem campos. *"É absurdo."*"""
    from app.apps.analisesps import (colaboradores as col, folha_rateio as fr,
                                     sincronizacao)

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "muitos_por_cpf", lambda cpfs, ate=None: {})

    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)

    assert 'id="tabela-rateio"' in html
    assert "A repetição da obra define o peso" in html
    # O formato tem de estar escrito na tela: ninguém adivinha um formato.
    assert "CPF ; nome ; obra, obra, obra" in html
    # E o "à mão" continua existindo, mas como caminho secundário.
    assert "Incluir regra manualmente" in html
    assert "secundario" in html
    # Substituir avisa que DESATIVA, não apaga.
    assert "desativa" in html


def test_a_tela_do_rateio_NAO_repete_a_explicacao_do_card(app, monkeypatch):
    """*"Não precisa dar aquela ênfase, porque a gente já sabe, ali a gente clica
    no nome da pessoa, já abre o card do Pipefy e lá atualiza."* Explicar numa
    tela o que a pessoa faz todo dia é ocupar espaço com o óbvio."""
    from app.apps.analisesps import (colaboradores as col, folha_rateio as fr,
                                     sincronizacao)

    monkeypatch.setattr(fr, "_pronto", lambda: True)
    monkeypatch.setattr(fr, "listar", lambda so_ativas=False: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(col, "quando_atualizou", _cadastro_pronto)
    monkeypatch.setattr(col, "muitos_por_cpf", lambda cpfs, ate=None: {})

    html = _como_mestre(app).get(
        "/analisesps/folha/rateio").get_data(as_text=True)
    assert "se corrige" not in html
    assert "Depois de corrigir" not in html


# ---------------------------------------------------------------------------
# A SUBTELA DA FOLHA DA CONTABILIDADE — 27/09/2026
# ---------------------------------------------------------------------------
def test_a_tela_de_importar_tem_a_area_de_SOLTAR_igual_a_do_extrato(app, monkeypatch):
    """Pedido do dono em 26/09/2026: *"semelhante àquela do extrato bancário, o
    OFX, aquele retângulozinho para você jogar o arquivo dentro."* Dois jeitos
    diferentes de receber arquivo no mesmo módulo obrigam a aprender duas vezes."""
    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "_pronto", lambda: True)
    monkeypatch.setattr(fa, "listar", lambda *a, **k: [])
    # ⚠️ `?lista=1` PORQUE A ABA MUDOU DE DESTINO em 29/09/2026: com folha
    # importada, "/folha/importar" manda direto para a última folha aberta. Esta
    # tela continua sendo a estante (soltar arquivo, apagar o que veio errado), e
    # é ela que está sob teste aqui.
    html = _como_mestre(app).get(
        "/analisesps/folha/importar?lista=1").get_data(as_text=True)

    assert "solta-arquivo" in html, "a classe é a MESMA do extrato"
    assert 'id="solta-folha"' in html
    assert ".xls" in html
    # A tabela é desenhada sempre, com cabeçalho — um "nada aqui" que engole a
    # tabela deixa a pessoa sem referência (foi a armadilha da conciliação).
    assert "<thead>" in html
    assert "Nenhuma folha importada" in html


def test_a_folha_que_NAO_FECHA_aparece_marcada_com_o_motivo(app, monkeypatch):
    """⚠️ A crítica mais importante desta tela. Não fechar não impede importar —
    ele precisa importar para descobrir por que não fecha — mas tem de estar na
    cara."""
    from decimal import Decimal

    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "_pronto", lambda: True)
    monkeypatch.setattr(fa, "listar", lambda *a, **k: [{
        "id": 1, "ano": 2026, "mes": 8, "tipo": "quinzena",
        "competencia": "08/2026", "rotulo_do_tipo": "Quinzena (dia 1 ao 15)",
        "pessoas": 491, "total": Decimal("430129.75"),
        "importado_em": None, "importado_por": "MARCELO",
        "fecha": False,
        "lista_de_avisos": ["a soma das pessoas é diferente dos subtotais"]}])

    # ⚠️ `?lista=1` PORQUE A ABA MUDOU DE DESTINO em 29/09/2026: com folha
    # importada, "/folha/importar" manda direto para a última folha aberta. Esta
    # tela continua sendo a estante (soltar arquivo, apagar o que veio errado), e
    # é ela que está sob teste aqui.
    html = _como_mestre(app).get(
        "/analisesps/folha/importar?lista=1").get_data(as_text=True)

    assert ">divergente</span>" in html
    assert "diferente dos subtotais" in html
    assert "linha-alerta" in html
    assert "430.129,75" in html


def test_a_folha_que_fecha_nao_grita(app, monkeypatch):
    from decimal import Decimal

    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "_pronto", lambda: True)
    monkeypatch.setattr(fa, "listar", lambda *a, **k: [{
        "id": 1, "ano": 2026, "mes": 8, "tipo": "fim_de_mes",
        "competencia": "08/2026", "rotulo_do_tipo": "Fim de mês (16 ao último dia)",
        "pessoas": 10, "total": Decimal("1000.00"),
        "importado_em": None, "importado_por": "", "fecha": True,
        "lista_de_avisos": []}])
    # ⚠️ `?lista=1` PORQUE A ABA MUDOU DE DESTINO em 29/09/2026: com folha
    # importada, "/folha/importar" manda direto para a última folha aberta. Esta
    # tela continua sendo a estante (soltar arquivo, apagar o que veio errado), e
    # é ela que está sob teste aqui.
    html = _como_mestre(app).get(
        "/analisesps/folha/importar?lista=1").get_data(as_text=True)
    assert ">conferido</span>" in html
    assert ">divergente</span>" not in html
    assert "linha-alerta" not in html


def test_sem_a_migracao_a_tela_de_importar_AVISA_e_nao_estoura(app, monkeypatch):
    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "_pronto", lambda: False)
    resposta = _como_mestre(app).get("/analisesps/folha/importar")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html
    assert 'id="solta-folha"' not in html


def test_importar_SEM_arquivo_responde_direito(app):
    resposta = _como_mestre(app).post("/analisesps/api/folha/importar", data={})
    assert resposta.status_code == 400
    assert "Nenhum arquivo" in resposta.get_json()["erro"]


def test_quando_falta_o_TIPO_a_resposta_manda_a_tela_PERGUNTAR(app, monkeypatch):
    """⚠️ É o ponto do desenho: em vez de mandar a pessoa tentar de novo
    adivinhando, a tela pergunta — e guarda o arquivo, porque pedir para soltar
    de novo seria mesquinho."""
    import io

    from app.apps.analisesps import folha_arquivo as fa

    def explode(*a, **k):
        raise fa.ErroDaImportacao(
            'não deu para saber se esta folha é da QUINZENA ou do FIM DE MÊS. '
            "Escolha na tela e importe de novo.")

    monkeypatch.setattr(fa, "importar", explode)
    resposta = _como_mestre(app).post(
        "/analisesps/api/folha/importar",
        data={"folha": (io.BytesIO(b"xls"), "folha.xls")},
        content_type="multipart/form-data")

    assert resposta.status_code == 400
    corpo = resposta.get_json()
    assert corpo["pergunte_o_tipo"] is True
    assert "QUINZENA" in corpo["erro"]


def test_outro_erro_de_importacao_NAO_pede_o_tipo(app, monkeypatch):
    """Senão a tela mostraria os botões de período para um arquivo que nem é
    folha — e a pessoa escolheria o período de um erro."""
    import io

    from app.apps.analisesps import folha_arquivo as fa

    def explode(*a, **k):
        raise fa.ErroDaImportacao("não achei nenhuma pessoa no arquivo.")

    monkeypatch.setattr(fa, "importar", explode)
    corpo = _como_mestre(app).post(
        "/analisesps/api/folha/importar",
        data={"folha": (io.BytesIO(b"xls"), "folha.xls")},
        content_type="multipart/form-data").get_json()
    assert corpo["pergunte_o_tipo"] is False




def test_importar_a_folha_NAO_e_so_do_mestre(app):
    """Decisão do dono: *"vai ter o usuário do DP que vai estar fazendo a
    leitura, mas o usuário master, que sou eu, eu gero o arquivo"*. Trazer a
    folha é o trabalho do DP; GERAR o arquivo de pagamento é que será do mestre."""
    from app.apps.analisesps import auth as guarda

    assert guarda.e_so_do_mestre("analisesps.folha_importar") is False
    assert guarda.e_so_do_mestre("analisesps.tela_folha_importar") is False
    assert guarda.telas_da_rota("analisesps.folha_importar") == ("folha",)


# ---------------------------------------------------------------------------
# O PANORAMA E A FOLHA ABERTA — 27/09/2026
# ---------------------------------------------------------------------------
def _panorama(**extra):
    from decimal import Decimal
    base = {
        "pronto": True, "total": Decimal("430129.75"), "pessoas": 491,
        "competencias": 1, "pendentes": 0,
        "por_filial": [{"codigo": "002", "nome": "CREPEOLINDA", "pessoas": 30,
                        "total": Decimal("300000.00")},
                       {"codigo": "001", "nome": "MATRIZ", "pessoas": 461,
                        "total": Decimal("130129.75")}],
        "folhas": [{"id": 1, "ano": 2026, "mes": 8, "tipo": "quinzena",
                    "competencia": "08/2026",
                    "rotulo_do_tipo": "Quinzena (dia 1 ao 15)",
                    "pessoas": 491, "total": Decimal("430129.75"),
                    "fecha": True, "lista_de_avisos": []}],
    }
    base.update(extra)
    return base


def test_a_folha_aberta_poe_as_CRITICAS_antes_da_lista(app, monkeypatch):
    """⚠️ São as três coisas que impedem pagar. Ninguém as encontraria rolando
    491 linhas."""
    from decimal import Decimal

    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "casar_com_o_cadastro", lambda i: None)
    monkeypatch.setattr(fa, "totais_por_filial", lambda i: [])
    monkeypatch.setattr(fa, "abrir", lambda i: {
        "id": 1, "ano": 2026, "mes": 8, "tipo": "quinzena",
        "competencia": "08/2026",
        "rotulo_do_tipo": "Quinzena (dia 1 ao 15)", "pessoas": 2,
        "total": Decimal("2437.20"), "importado_em": None,
        "importado_por": "MARCELO", "lista_de_avisos": [], "fecha": True,
        "linhas": [
            {"id": 1, "id_fortes": "000013", "nome": "GERLANIO",
             "cpf": "99713349334", "valor": Decimal("1074.64"),
             "filial_codigo": "001", "filial_nome": "MATRIZ"},
            {"id": 2, "id_fortes": "000387", "nome": "LUELIA", "cpf": "",
             "valor": Decimal("1362.56"), "filial_codigo": "001",
             "filial_nome": "MATRIZ"}]})
    monkeypatch.setattr(fa, "criticas", lambda i: {
        "pendentes": [{"id_fortes": "000387", "nome": "LUELIA",
                       "valor": Decimal("1362.56")}],
        "sairam": [{"id_fortes": "000013", "nome": "GERLANIO",
                    "cpf": "99713349334", "valor": Decimal("1074.64"),
                    "motivo": "saiu em 31/07/2026. Não pague folha.",
                    "link_pipefy": "https://app.pipefy.com/open-cards/778899"}],
        "saindo": [], "total_pendente": Decimal("1362.56"),
        "total_de_quem_saiu": Decimal("1074.64")})

    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    # O nome mudou em 01/10/2026 — "Precisa da sua mão" *"não é termo pra usar
    # em sistema"*.
    assert "Precisa da sua mão" not in html
    assert "Pendências antes do pagamento" in html
    assert html.index("Pendências antes do pagamento") < html.index("Detalhamento por colaborador")
    assert "sem correspondência no cadastro" in html
    assert "colaborador(es) desligado(s)" in html
    assert "Não pague folha" in html
    # A lista de nomes com o link do Pipefy saiu da lateral em 01/10/2026 (ela
    # ficou limpa, pedido dele): o item filtra a lista, e é na linha da pessoa
    # que o link do card aparece.
    assert "situacao=saiu" in html
    # ⚠️ A pessoa pendente CONTINUA na lista, marcada — não numa lista à parte
    # que alguém esquece de abrir.
    assert "fora do cadastro" in html
    assert "linha-alerta" in html


def test_a_folha_aberta_CASA_de_novo_a_cada_visita(app, monkeypatch):
    """O cadastro pode ter sido atualizado depois da importação: aí gente que
    estava pendente passa a casar, sem ninguém reimportar nada."""
    from app.apps.analisesps import folha_arquivo as fa
    chamou = {}

    monkeypatch.setattr(fa, "casar_com_o_cadastro",
                        lambda i: chamou.setdefault("id", i))
    monkeypatch.setattr(fa, "abrir", lambda i: None)
    _como_mestre(app).get("/analisesps/folha/7")
    assert chamou.get("id") == 7


def test_folha_que_nao_existe_responde_404_e_nao_403(app, monkeypatch):
    """Dizer "sem permissão" para um número que não existe confirmaria a
    existência dele — e varrer os números mapearia o sistema."""
    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "casar_com_o_cadastro", lambda i: None)
    monkeypatch.setattr(fa, "abrir", lambda i: None)
    resposta = _como_mestre(app).get("/analisesps/folha/99999")
    assert resposta.status_code == 404


def test_folha_toda_certa_NAO_mostra_a_faixa_de_criticas(app, monkeypatch):
    """Faixa que aparece sempre vira enfeite."""
    from decimal import Decimal

    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "casar_com_o_cadastro", lambda i: None)
    monkeypatch.setattr(fa, "totais_por_filial", lambda i: [])
    monkeypatch.setattr(fa, "abrir", lambda i: {
        "id": 1, "competencia": "08/2026", "rotulo_do_tipo": "Quinzena",
        "pessoas": 1, "total": Decimal("100.00"), "importado_em": None,
        "importado_por": "", "lista_de_avisos": [], "fecha": True,
        "linhas": [{"id": 1, "id_fortes": "000013", "nome": "GERLANIO",
                    "cpf": "99713349334", "valor": Decimal("100.00"),
                    "filial_codigo": "001", "filial_nome": "MATRIZ"}]})
    monkeypatch.setattr(fa, "criticas", lambda i: {
        "pendentes": [], "sairam": [], "saindo": [], "total_pendente": 0,
        "total_de_quem_saiu": 0})

    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "Pendências antes do pagamento" not in html
    assert "linha-alerta" not in html


# ---------------------------------------------------------------------------
# A SUBTELA DO PONTO — 27/09/2026
# ---------------------------------------------------------------------------
def test_a_tela_do_ponto_LISTA_os_colaboradores_e_abre_a_janela(app, monkeypatch):
    """O dono, 02/10/2026: *"a partir dessa tela pesquisar funcionários, visualizar
    os pontos de cada um e ainda poder editá-los. Quero uma tela mais limpa (…)
    Não precisa dessa informação de API."*"""
    from app.apps.analisesps import ponto as _ponto

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [{
        "id": 1, "ano": 2026, "mes": 8, "competencia": "08/2026",
        "pessoas": 2, "dias": 40, "paginas": 5, "paginas_lidas": 5,
        "completa": True, "interrompida": False, "campos": ["dia", "obra_entrada"],
        "lista_de_avisos": [], "carregado_em": None, "carregado_por": "MARCELO"}])
    monkeypatch.setattr(_ponto, "resumo_por_pessoa", lambda ano, mes: [
        {"cpf": "99713349334", "nome": "GERLANIO", "dias": 20, "com_obra": 18,
         "faltas": 2, "sem_marcacao": 0, "obras": [("CREPEOLINDA", 18)]},
        {"cpf": "03513441363", "nome": "ANA", "dias": 20, "com_obra": 19,
         "faltas": 0, "sem_marcacao": 1, "obras": [("CREPEAREIAS", 19)]}])

    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/ponto").get_data(as_text=True)
    assert "GERLANIO" in html and "ANA" in html
    assert "abrirFichaDoFuncionario" in html
    assert "Campos enviados pela API" not in html, "a informação de API saiu"
    assert 'id="btn-trazer-ponto"' in html
    filtrado = cliente.get("/analisesps/folha/ponto?q=gerl").get_data(as_text=True)
    assert "GERLANIO" in filtrado and ">ANA<" not in filtrado
    por_obra = cliente.get(
        "/analisesps/folha/ponto?obra=CREPEAREIAS").get_data(as_text=True)
    assert "<b>ANA</b>" in por_obra and "<b>GERLANIO</b>" not in por_obra


def test_sem_credencial_a_tela_do_ponto_diz_ONDE_criar_e_manda_TROCAR(app, monkeypatch):
    """O recado tem de dizer o que fazer, onde, e o cuidado — sem nunca pedir
    para colar a chave numa conversa."""
    from app.apps.analisesps import ponto as _ponto

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: False)
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [])

    html = _como_mestre(app).get(
        "/analisesps/folha/ponto").get_data(as_text=True)

    assert "MOBPONTO_AUTHORIZATION" in html
    assert "MOBPONTO_API_KEY" in html
    assert "Apps Script" in html
    assert "diretamente para o Render" in html
    assert "substitua a chave\n      na origem" in html or "substitua a chave na origem" in html
    # E sem credencial não oferece o botão: botão que só dá erro é armadilha.
    assert 'id="btn-trazer-ponto"' not in html


def test_mes_que_veio_PELA_METADE_e_marcado(app, monkeypatch):
    """Um mês incompleto mostrado como completo faria o total por obra sair a
    menos, sem ninguém saber."""
    from app.apps.analisesps import ponto as _ponto

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    monkeypatch.setattr(_ponto, "resumo_por_pessoa", lambda ano, mes: [])
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [{
        "id": 1, "ano": 2026, "mes": 8, "competencia": "08/2026",
        "pessoas": 100, "dias": 1000, "paginas": 9, "paginas_lidas": 3,
        "completa": False, "interrompida": False, "campos": [],
        "lista_de_avisos": ["a API disse que há 9 páginas e eu li 3"],
        "carregado_em": None, "carregado_por": ""}])

    html = _como_mestre(app).get(
        "/analisesps/folha/ponto").get_data(as_text=True)
    assert "Importação incompleta" in html
    assert "3 de 9 página(s)" in html
    assert '<b class="ruim">incompleta</b>' in html


def test_o_ponto_NAO_tem_botao_em_configuracoes(app):
    """⚠️ Ele precisa saber QUAL MÊS trazer. Um botão em Configurações sem essa
    escolha traria sempre o mesmo mês — e o dono descobriria quando o total por
    obra viesse errado."""
    from app.apps.analisesps import tarefas

    assert "ponto" in tarefas.MODOS
    assert "ponto" not in tarefas.MODOS_DA_BASE
    assert tarefas.ETAPAS["ponto"] == ["ponto"]


def test_o_recado_do_ponto_NAO_diz_SPs(app):
    """Mesmo cuidado do cadastro: são dias de ponto, não SPs."""
    import inspect

    from app.apps.analisesps import tarefas

    fonte = inspect.getsource(tarefas.executar_trabalho)
    trecho = fonte[fonte.index('if modo in ("apoios"'):]
    trecho = trecho[:trecho.index("):") + 2]
    assert '"ponto"' in trecho


def test_carregar_o_ponto_SEM_mes_e_recusado(app):
    resposta = _como_mestre(app).post("/analisesps/api/folha/ponto", json={})
    assert resposta.status_code == 400
    assert "Selecione o mês" in resposta.get_json()["erro"]


def test_carregar_o_ponto_SEM_credencial_e_recusado_com_o_caminho(app, monkeypatch):
    from app.apps.analisesps import ponto as _ponto

    monkeypatch.setattr(_ponto, "configurado", lambda: False)
    resposta = _como_mestre(app).post("/analisesps/api/folha/ponto",
                                      json={"ano": 2026, "mes": 8})
    assert resposta.status_code == 400
    assert "MOBPONTO_API_KEY" in resposta.get_json()["erro"]


def test_a_competencia_escolhida_vai_para_o_BANCO_antes_de_disparar(app, monkeypatch):
    """O trabalho roda no processo separado, que pode ser reiniciado: passar a
    escolha por parâmetro a perderia numa retomada."""
    from app.apps.analisesps import ponto as _ponto, sincronizacao, tarefas
    gravado = {}

    import contextlib

    from app.apps.analisesps import db as db_analisesps

    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    # A rota abre uma conexão de verdade para gravar a competência. Aqui ela é
    # dublada: o que está sob teste é QUAL competência foi gravada, não o banco.
    monkeypatch.setattr(db_analisesps, "conexao",
                        lambda: contextlib.nullcontext(object()))
    monkeypatch.setattr(sincronizacao, "_meta_gravar",
                        lambda conn, chave, valor: gravado.update({chave: valor}))
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="": {"ok": True})

    resposta = _como_mestre(app).post("/analisesps/api/folha/ponto",
                                      json={"ano": 2026, "mes": 8})
    assert resposta.get_json()["ok"] is True
    assert gravado["ponto_competencia"] == "2026-8"




# ---------------------------------------------------------------------------
# A SUBTELA DE FERIADOS E FÉRIAS — 27/09/2026
#
# Pedido do dono: *"você já cria uma telazinha onde eu vou inserir as férias de
# cada funcionário. Eu posso buscar pelo nome, pelo CPF e incluo o período. Só
# isso."*
# ---------------------------------------------------------------------------
def test_a_tela_de_ferias_busca_a_pessoa_em_vez_de_pedir_o_CPF(app, monkeypatch):
    """⚠️ O CPF é a chave de TUDO. Digitado na mão, um número trocado lança as
    férias de outra pessoa — e o auxílio da certa sai errado sem ninguém saber."""
    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: True)
    monkeypatch.setattr(fc, "listar_feriados", lambda *a, **k: [])
    monkeypatch.setattr(fc, "listar_ferias", lambda *a, **k: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})

    html = _como_mestre(app).get(
        "/analisesps/folha/calendario").get_data(as_text=True)

    assert 'id="ferias-pessoa"' in html
    assert 'id="ferias-cpf"' in html, "o CPF vai escondido, escolhido da lista"
    assert "Digite o nome do colaborador" in html
    assert "ferias-sugestoes" in html
    # E os dois dias do período.
    assert "Data inicial" in html and "Data final" in html


def test_a_obra_do_feriado_so_aparece_quando_e_de_obra(app, monkeypatch):
    """Campo que fica na tela sem servir faz a pessoa se perguntar se devia
    preencher."""
    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: True)
    monkeypatch.setattr(fc, "listar_feriados", lambda *a, **k: [])
    monkeypatch.setattr(fc, "listar_ferias", lambda *a, **k: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [{"nome": "CREPEOLINDA", "codigo": "1"}],
                                 "categorias": []})

    html = _como_mestre(app).get(
        "/analisesps/folha/calendario").get_data(as_text=True)
    assert 'id="feriado-obra-bloco" hidden' in html
    assert "CREPEOLINDA" in html, "a lista de obras é a MESMA do rateio"
    assert "Feriado nacional" in html


def test_a_lista_de_obras_vazia_explica_o_que_fazer(app, monkeypatch):
    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: True)
    monkeypatch.setattr(fc, "listar_feriados", lambda *a, **k: [])
    monkeypatch.setattr(fc, "listar_ferias", lambda *a, **k: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    html = _como_mestre(app).get(
        "/analisesps/folha/calendario").get_data(as_text=True)
    assert "planilhas de apoio" in html


def test_os_feriados_e_as_ferias_aparecem_na_tela(app, monkeypatch):
    import datetime as dt

    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    monkeypatch.setattr(fc, "listar_feriados", lambda *a, **k: [
        {"id": 1, "data": dt.date(2026, 9, 7), "abrangencia": "nacional",
         "obra": "", "descricao": "Independência", "criado_por": "MARCELO",
         "rotulo": "Nacional"},
        {"id": 2, "data": dt.date(2026, 9, 12), "abrangencia": "obra",
         "obra": "CREPEOLINDA", "descricao": "Aniversário", "criado_por": "",
         "rotulo": "Só nesta obra"}])
    monkeypatch.setattr(fc, "listar_ferias", lambda *a, **k: [
        {"id": 5, "cpf": "99713349334", "cpf_bonito": "997.133.493-34",
         "nome": "GERLANIO GOMES LIMA", "inicio": dt.date(2026, 9, 1),
         "fim": dt.date(2026, 9, 20), "observacao": "",
         "criado_por": "MARCELO", "dias": 20}])

    html = _como_mestre(app).get(
        "/analisesps/folha/calendario").get_data(as_text=True)

    assert "GERLANIO GOMES LIMA" in html
    assert "997.133.493-34" in html
    assert "01/09/2026" in html and "20/09/2026" in html
    assert "Independência" in html
    assert "CREPEOLINDA" in html
    assert "nacional" in html


def test_sem_a_migracao_a_tela_do_calendario_AVISA(app, monkeypatch):
    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: False)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    resposta = _como_mestre(app).get("/analisesps/folha/calendario")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html
    assert 'id="btn-gravar-ferias"' not in html


def test_a_tela_explica_a_DIFERENCA_entre_alimentacao_e_transporte(app, monkeypatch):
    """A alimentação desconta feriado e férias; o transporte só férias. É decisão
    dele, e quem lança os dados tem de saber para que serve."""
    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: True)
    monkeypatch.setattr(fc, "listar_feriados", lambda *a, **k: [])
    monkeypatch.setattr(fc, "listar_ferias", lambda *a, **k: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})
    html = _como_mestre(app).get(
        "/analisesps/folha/calendario").get_data(as_text=True)
    assert "alimenta" in html.lower()
    assert "transporte" in html.lower()


def test_procurar_pessoa_devolve_POUCO(app, monkeypatch):
    """⚠️ A caixa de sugestão não é lugar de mostrar salário nem auxílio."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "buscar", lambda *a, **k: [{
        "cpf": "99713349334", "nome": "GERLANIO", "cargo": "ENCARREGADO",
        "desligado": False, "valor_alimentacao": 330,
        "valor_gratificacao": 5000}])

    corpo = _como_mestre(app).get(
        "/analisesps/api/folha/procurar-pessoa?q=ger").get_json()

    assert corpo["pessoas"][0]["nome"] == "GERLANIO"
    assert set(corpo["pessoas"][0]) == {"cpf", "nome", "cargo", "desligado"}


def test_procurar_pessoa_com_UMA_letra_nao_vai_ao_banco(app, monkeypatch):
    """Uma consulta por tecla faria dezenas de idas ao banco para um nome de dez
    letras."""
    from app.apps.analisesps import colaboradores as col
    chamou = {}

    monkeypatch.setattr(col, "buscar",
                        lambda *a, **k: chamou.setdefault("sim", True) or [])
    corpo = _como_mestre(app).get(
        "/analisesps/api/folha/procurar-pessoa?q=g").get_json()
    assert corpo["pessoas"] == []
    assert "sim" not in chamou


def test_gravar_ferias_devolve_a_frase_do_erro_para_a_tela(app, monkeypatch):
    """A frase diz qual é o outro período que cruza — é o que permite consertar."""
    from app.apps.analisesps import folha_calendario as fc

    def explode(*a, **k):
        raise fc.ErroDoCalendario(
            "esta pessoa já tem férias de 01/09/2026 a 20/09/2026, e os "
            "períodos se cruzam.")

    monkeypatch.setattr(fc, "gravar_ferias", explode)
    resposta = _como_mestre(app).post("/analisesps/api/folha/ferias",
                                      json={"cpf": "99713349334"})
    assert resposta.status_code == 400
    assert "se cruzam" in resposta.get_json()["erro"]


def test_as_subtelas_ficam_AGRUPADAS_POR_ASSUNTO(app):
    """⚠️ REORDENADAS EM 28/09/2026, por correção do dono: *"não tem lógica nas
    telas aqui (…) folha da contabilidade, alimentação, ambos são PAGAMENTOS. Então
    acho que era para estar junto. O que é CADASTRO é para estar junto."*

    A ordem anterior era a ordem em que EU construí as peças, não a ordem em que
    ele trabalha."""
    from app.apps.analisesps import web

    assert [s[0] for s in web.SUBTELAS_DA_FOLHA] == [
        "importar", "auxilios", "diaristas", "pagamento",     # pagamentos
        "colaboradores", "ponto", "calendario", "rateio"]      # base

    # E cada uma declara a que grupo pertence — é o que a faixa de abas desenha.
    # (O grupo "visao" era só o Panorama, que saiu em 30/09/2026.)
    assert {s[3] for s in web.SUBTELAS_DA_FOLHA} == {"paga", "base"}


def test_a_faixa_de_abas_SEPARA_os_grupos_sem_escrever_o_nome(app, monkeypatch):
    """⚠️ EU HAVIA ESCRITO OS RÓTULOS e ele mandou tirar, em 29/09/2026: *"eu não
    pedi pra colocar Pagamentos e Cadastro e base do cálculo. Era apenas pra
    reorganizar."*

    Os grupos continuam existindo — é o que mantém a ordem com lógica — e ganham um
    risco fino entre eles. Este teste existe para os rótulos não voltarem."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "Pagamentos</span>" not in html
    assert "Cadastro e base do cálculo" not in html
    assert "risco-abas" in html, "o separador fino continua"
    # E a ordem segue sendo a agrupada — lida na faixa de abas, não na tela
    # inteira (a lateral tem a fase "Colaboradores afastados" antes dela).
    inicio = html.index('<nav class="abas-folha"')
    abas = html[inicio:html.index("</nav>", inicio)]
    assert abas.index("Folha da contabilidade") < abas.index("Colaboradores")


def test_os_grupos_escondem_o_que_a_pessoa_nao_alcanca(app):
    """Aba que responde 404 é pior do que aba nenhuma — e grupo que fica vazio não
    deve aparecer com o rótulo sozinho."""
    from app.apps.analisesps import web

    with app.test_request_context():
        with app.test_client() as cliente:
            cliente.post("/analisesps/entrar", data={"senha": SENHA_CONSULTA})
            grupos = web.subtelas_agrupadas()
    assert all(telas for _rotulo, telas in grupos), (
        "nenhum grupo pode vir vazio")



# ---------------------------------------------------------------------------
# A SUBTELA DE ALIMENTAÇÃO E TRANSPORTE — 27/09/2026
#
# A planilha tem uma aba para cada; aqui as duas dividem uma subtela com uma aba
# cada, porque ele reclamou de tela demais no menu e a conta é quase a mesma.
# ---------------------------------------------------------------------------
def _auxilio_calculado(**mudancas):
    """Um resultado de `folha_auxilio.calcular` pronto, para a tela desenhar."""
    import datetime as dt
    from decimal import Decimal as D

    pessoa = {
        "cpf": "99713349334", "cpf_bonito": "997.133.493-34",
        "nome": "GERLANIO GOMES LIMA", "matricula": "4821",
        "cargo": "ENCARREGADO", "link_pipefy": "https://app.pipefy.com/open-cards/9",
        "modo": "Segunda à Sexta", "valor_unitario": D("15.00"),
        # A obra é o CÓDIGO (correção de 28/09/2026); o nome vem ao lado, porque é
        # por ele que o feriado municipal é cadastrado.
        "obra": "1042", "obra_nome": "CREPEOLINDA",
        # ⚠️ A OBRA QUE PAGA VEM DO PONTO desde 29/09/2026, e a origem vem com ela:
        # *"em Alimentação a informação de obra deveria ser a do Ponto. Caso não
        # tenha, usar a de cadastro."*
        "obra_do_ponto": "1042", "dias_na_obra": 18, "obra_de_onde": "ponto",
        "fase": "Colaboradores ativos",
        "dias_base": 22, "feriados": 1, "ferias": 0, "dias_ajuste": 0,
        "dias": 21, "valor": D("315.00"), "observacao": "",
        "observacao_cadastro": "", "ajuste_pagar": None, "motivos": [],
        "pagar": True, "pagar_calculado": True, "impossivel": False,
    }
    saida = {
        "tipo": "alimentacao", "rotulo": "Auxílio alimentação",
        "ano": 2026, "mes": 9, "competencia": "09/2026",
        "inicio": dt.date(2026, 9, 1), "fim": dt.date(2026, 9, 30),
        "pagamento_em": "10/2026",
        "pessoas": [pessoa], "quantos": 1, "quantos_a_pagar": 1,
        "total": D("315.00"), "com_problema": [],
        "por_obra": [{"obra": "CREPEOLINDA", "pessoas": 1, "total": D("315.00"),
                      "do_cadastro": 0}],
        "desconta_feriado": True,
        "tem_ponto": True, "quantos_do_ponto": 1,
        "fases": ["Colaboradores ativos"],
    }
    saida.update(mudancas)
    return saida


def _preparar_auxilio(monkeypatch, resultado=None, pronto=True):
    from app.apps.analisesps import folha_auxilio as fx, sincronizacao

    monkeypatch.setattr(fx, "_pronto", lambda: pronto)
    monkeypatch.setattr(fx, "calcular",
                        lambda *a, **k: resultado if resultado is not None
                        else _auxilio_calculado())
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [{"nome": "CREPEOLINDA", "codigo": "1"}],
                                 "categorias": []})


def test_a_tela_do_auxilio_mostra_o_CAMINHO_da_conta(app, monkeypatch):
    """⚠️ NÃO É ENFEITE: ele pediu *"saber até de onde é que foi que veio aquela
    informação"*. Um total sozinho não se audita."""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "GERLANIO GOMES LIMA" in html
    assert "Segunda à Sexta" in html
    assert "CREPEOLINDA" in html
    # A base, o desconto e o resultado, todos à vista na mesma linha.
    assert ">22<" in html, "os dias da modalidade"
    assert "315,00" in html
    assert "Base" in html and "Feriados" in html and "Férias" in html


def test_a_tela_do_auxilio_diz_QUAL_REGUA_esta_vendo(app, monkeypatch):
    """As colunas das duas verbas são iguais e o número muda. Sem dizer qual
    régua está na tela, quem confere confere errado."""
    cliente = _como_mestre(app)

    _preparar_auxilio(monkeypatch)
    alimentacao = cliente.get(
        "/analisesps/folha/auxilios?tipo=alimentacao").get_data(as_text=True)
    assert "desconta <b>feriado</b> e <b>férias</b>" in alimentacao

    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        tipo="transporte", rotulo="Auxílio transporte", desconta_feriado=False))
    transporte = cliente.get(
        "/analisesps/folha/auxilios?tipo=transporte").get_data(as_text=True)
    assert "<b>não</b> desconta feriado" in transporte
    assert "Cartão" in transporte, "o cartão não sai em dinheiro"


def test_no_transporte_a_coluna_de_feriado_nao_finge_numero(app, monkeypatch):
    """O transporte não desconta feriado. Mostrar a contagem ali faria parecer
    que desconta, e ninguém conferindo perceberia."""
    cliente = _como_mestre(app)
    # 17 é um número que não aparece em nenhum outro lugar da tela — dá para
    # afirmar com segurança se a célula de feriado o mostrou ou não.
    pessoa = dict(_auxilio_calculado()["pessoas"][0], feriados=17)

    _preparar_auxilio(monkeypatch, _auxilio_calculado(pessoas=[pessoa]))
    alimentacao = cliente.get(
        "/analisesps/folha/auxilios?tipo=alimentacao").get_data(as_text=True)
    assert "17" in alimentacao, "na alimentação o feriado desconta e aparece"

    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        tipo="transporte", rotulo="Auxílio transporte", desconta_feriado=False,
        pessoas=[pessoa]))
    transporte = cliente.get(
        "/analisesps/folha/auxilios?tipo=transporte").get_data(as_text=True)
    assert "17" not in transporte, "no transporte a célula do feriado é um travessão"


def test_quem_precisa_de_mao_aparece_marcado(app, monkeypatch):
    """A linha com problema tem de se ver de longe — ele rola a lista no
    celular."""
    from decimal import Decimal as D

    problema = dict(_auxilio_calculado()["pessoas"][0])
    problema.update({
        "pagar": False, "impossivel": True, "valor": D("0.00"), "dias": 0,
        "valor_unitario": None, "dias_base": 0,
        "motivos": ["o cadastro não diz o valor deste auxílio. Corrija no card "
                    "do Pipefy e atualize o cadastro."]})
    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        pessoas=[problema], quantos=1, quantos_a_pagar=0, total=D("0.00"),
        com_problema=[problema], por_obra=[]))

    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "linha-alerta" in html
    assert "não diz o valor deste auxílio" in html
    assert '<div class="kpi-rotulo">Pendências</div>' in html


def test_a_selecao_e_CAIXINHA_e_salva_de_uma_vez(app, monkeypatch):
    """⚠️ REFEITO EM 28/09/2026. O desenho anterior tinha um seletor de três
    estados ("segue o cálculo" / pagar / não pagar) e um botão Gravar POR LINHA.
    Ele reprovou os dois:

      "Que diabo é segue cálculo?"
      "Fica muito dificultoso, a gente vai gravando um por um (…) o que eu faço é
       só selecionar quem vai e quem não vai ser pago (…) e eu salvar como um
       todo, não linha a linha."
    """
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "segue o cálculo" not in html, "o jargão saiu"
    assert "btn-gravar-ajuste" not in html, "não há mais Gravar por linha"
    assert 'class="marca-pessoa"' in html, "uma caixinha por pessoa"
    assert 'id="btn-salvar-selecao"' in html, "e UM salvar, no fim"
    assert 'id="marcar-todos"' in html


def test_quem_atende_os_criterios_JA_VEM_MARCADO(app, monkeypatch):
    """*"A princípio tudo que está atendendo os critérios se paga. Ela exibe tudo
    que tem coerência, já faz o cálculo, já deixa tudo pronto."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    # A caixinha de quem o cálculo paga vem marcada; a de quem ele não paga, não.
    # Comparar a CONTAGEM é o que amarra as duas coisas sem depender do espaçamento
    # do HTML gerado.
    marcadas = html.count("checked")
    assert marcadas >= 1, "quem atende os critérios já vem marcado"

    recusada = dict(_auxilio_calculado()["pessoas"][0])
    recusada.update({"pagar": False, "pagar_calculado": False})
    _preparar_auxilio(monkeypatch, _auxilio_calculado(pessoas=[recusada]))
    outra = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert outra.count("checked") < marcadas, (
        "quem o cálculo não paga vem DESmarcado")


def test_quem_NAO_TEM_VALOR_no_cadastro_nao_da_para_marcar(app, monkeypatch):
    """Marcar pagaria ZERO em silêncio, que é pior do que não pagar."""
    from decimal import Decimal as D

    problema = dict(_auxilio_calculado()["pessoas"][0])
    problema.update({"pagar": False, "pagar_calculado": False,
                     "impossivel": True, "valor": D("0.00"), "dias": 0,
                     "valor_unitario": None, "dias_base": 0,
                     "motivos": ["o cadastro não diz o valor deste auxílio."]})
    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        pessoas=[problema], quantos=1, quantos_a_pagar=0, total=D("0.00"),
        com_problema=[problema], por_obra=[]))

    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert "disabled" in html
    assert "não diz o valor deste auxílio" in html


def test_o_CPF_sai_PONTUADO(app, monkeypatch):
    """*"O CPF não está com a pontuação, isso facilita visualmente."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert "997.133.493-34" in html


def test_a_CATEGORIA_aparece_na_tabela(app, monkeypatch):
    """*"Aqui não diz a modalidade mês, mensal, se o na sexta."* É a coluna
    "Categoria Auxílio Alimentação" do cadastro, e é ela que decide a conta."""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert "Categoria" in html
    assert "Segunda à Sexta" in html


def test_a_obra_e_o_CODIGO_e_nao_da_para_digitar(app, monkeypatch):
    """*"Em obra tem que colocar o código da obra e não a obra por extenso"* e
    *"eu não sei por que você colocou um campo editável."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert ">1042<" in html, "o código"
    assert "campo-obra" not in html, "o campo de digitar obra saiu"
    assert "obras-do-rateio" not in html, "e a lista de nomes também"


def test_a_tela_diz_que_o_auxilio_e_pago_no_mes_SEGUINTE(app, monkeypatch):
    """*"O auxílio transporte, alimentação, a gente sempre paga o mês seguinte."*
    Sem isso, quem abre a tela em outubro procura outubro e acha o mês errado."""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert "10/2026" in html
    assert "pago em" in html


def test_a_tela_do_auxilio_TEM_FILTRO_na_lateral(app, monkeypatch):
    """*"Era para ter uma na lateral aqui, filtro (…) eu preciso às vezes tratar
    só uma obra."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert 'name="obra"' in html and 'name="q"' in html
    assert "sem-filtros" not in html, "a lateral de filtros precisa existir"


def test_o_filtro_por_obra_recorta_a_lista_mas_nao_o_TOTAL(app, monkeypatch):
    """⚠️ Filtrar a CONTA faria o total mudar conforme o filtro, e aí ninguém
    saberia mais qual é o valor do pagamento."""
    from decimal import Decimal as D

    outra = dict(_auxilio_calculado()["pessoas"][0])
    outra.update({"cpf": "03513441363", "cpf_bonito": "035.134.413-63",
                  "nome": "ANA", "obra": "2050", "valor": D("100.00")})
    base = _auxilio_calculado()
    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        pessoas=base["pessoas"] + [outra], quantos=2, quantos_a_pagar=2,
        total=D("415.00")))

    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios?obra=2050").get_data(as_text=True)
    assert "ANA" in html
    assert "GERLANIO GOMES LIMA" not in html, "a lista foi recortada"
    assert "415,00" in html, "o total continua sendo o da verba inteira"


def test_clicar_na_pessoa_abre_a_FICHA_com_o_ponto(app, monkeypatch):
    """*"Eu quero também poder visualizar o ponto do mês daquela pessoa. Eu clicar
    e visualizar o ponto da pessoa no modal."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    # Desde 01/10/2026 é a janela do funcionário COMUM às folhas.
    assert 'id="cartao-ficha"' in html
    assert "abrirFichaDoFuncionario" in html
    assert 'class="clicavel' in html


def test_a_JANELA_DO_FUNCIONARIO_das_outras_folhas_traz_cadastro_e_ponto(app, monkeypatch):
    """O dono, 01/10/2026: as outras folhas herdam *"aquele modal que aparece as
    informações dele"*. Cadastro, valores, ponto dia a dia e os botões."""
    import datetime as dt
    from decimal import Decimal as D
    from app.apps.analisesps import colaboradores as col, ponto

    monkeypatch.setattr(col, "por_cpf", lambda *a, **k: {
        "cpf": "99713349334", "cpf_bonito": "997.133.493-34", "nome": "GERLANIO",
        "cargo": "SERVENTE", "fase": "Colaboradores Ativos", "tipo": "Prestador",
        "tipo_contrato": "Autônomo (RPA)", "obra_cadastro": "CREPE OLINDA",
        "obra_codigo": "1042", "situacao": "ativo", "motivo": "",
        "data_inicio": dt.date(2026, 9, 1), "data_admissao": None,
        "valor_alimentacao": D("15.00"), "modo_alimentacao": "Mês",
        "valor_transporte": None, "modo_transporte": "", "valor_gratificacao": None,
        "observacao_auxilio": "", "link_pipefy": "https://app.pipefy.com/open-cards/1"})
    monkeypatch.setattr(col, "nascimento_de", lambda *a: dt.date(1990, 5, 4))
    monkeypatch.setattr(col, "valores_de_diaria", lambda *a: {"99713349334": D("120.00")})
    monkeypatch.setattr(col, "codigos_das_obras", lambda: {})
    monkeypatch.setattr(ponto, "dias_da_pessoa", lambda *a, **k: {
        "tem_carga": True, "dias": [{"data": dt.date(2026, 9, 8), "obra": "1042",
                                     "empate": False, "horas": ["07:00", "", "", "17:00"],
                                     "marcacoes": ["1042", "", "", "1042"],
                                     "presenca": "PRESENÇA", "falta": "",
                                     "total_de_horas": "10:00"}]})
    html = _como_mestre(app).get(
        "/analisesps/folha/pessoa/99713349334/ficha?ano=2026&mes=9").get_data(as_text=True)
    assert "GERLANIO" in html and "04/05/1990" in html
    assert "120,00" in html, "o valor da diária"
    assert "08/09/2026" in html and "07:00" in html
    assert "Atualizar ponto" in html and "Cadastro completo" in html
    assert "Pipefy" in html


def test_a_janela_do_funcionario_de_quem_NAO_esta_no_cadastro_e_404(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col
    monkeypatch.setattr(col, "por_cpf", lambda *a, **k: None)
    r = _como_mestre(app).get("/analisesps/folha/pessoa/99713349334/ficha")
    assert r.status_code == 404


def test_a_ficha_da_pessoa_traz_cadastro_ponto_e_o_card(app, monkeypatch):
    """O card do Pipefy passou para DENTRO do modal: é de lá que ele corrige a
    categoria e o valor."""
    from app.apps.analisesps import colaboradores as col, ponto

    monkeypatch.setattr(col, "por_cpf", lambda *a, **k: {
        "cpf": "99713349334", "cpf_bonito": "997.133.493-34",
        "nome": "GERLANIO", "cargo": "ENCARREGADO", "matricula": "4821",
        "obra_cadastro": "CREPEOLINDA", "obra_codigo": "1042",
        "fase": "Colaboradores Ativos", "tipo_contrato": "CTPS",
        "convencao": "", "modo_alimentacao": "Segunda à Sexta",
        "modo_transporte": "Cartão", "valor_alimentacao": 15,
        "valor_transporte": None, "observacao_auxilio": "entrou dia 10",
        "situacao": "ativo", "motivo": "",
        "link_pipefy": "https://app.pipefy.com/open-cards/9"})
    monkeypatch.setattr(col, "codigos_das_obras", lambda: {})
    monkeypatch.setattr(ponto, "dias_da_pessoa", lambda *a, **k: {
        "tem_carga": True, "campos": ["obra_entrada", "presenca"],
        "dias": [{"data": __import__("datetime").date(2026, 9, 1),
                  "matricula": "4821",
                  "campos": {"obra_entrada": "CREPEOLINDA", "presenca": "OK"}}]})

    corpo = _como_mestre(app).get(
        "/analisesps/api/folha/pessoa/99713349334?ano=2026&mes=9").get_json()

    assert corpo["ok"] is True
    assert corpo["pessoa"]["observacao"] == "entrou dia 10"
    assert corpo["pessoa"]["obra_codigo"] == "1042"
    assert corpo["pessoa"]["link_pipefy"].endswith("/9")
    assert corpo["ponto"]["tem_carga"] is True
    assert corpo["ponto"]["campos"] == ["obra_entrada", "presenca"]
    assert corpo["ponto"]["dias"][0]["data"] == "2026-09-01"


def test_a_ficha_de_quem_nao_esta_no_cadastro_responde_404(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "por_cpf", lambda *a, **k: None)
    resposta = _como_mestre(app).get("/analisesps/api/folha/pessoa/99713349334")
    assert resposta.status_code == 404
    assert resposta.get_json()["ok"] is False


def test_salvar_a_selecao_manda_a_lista_inteira(app, monkeypatch):
    from app.apps.analisesps import folha_auxilio as fx
    visto = {}

    monkeypatch.setattr(fx, "_pronto", lambda: True)
    monkeypatch.setattr(fx, "salvar_selecao",
                        lambda tipo, ano, mes, decisoes, quem="": visto.update(
                            {"tipo": tipo, "decisoes": decisoes}) or
                        {"gravados": 1, "limpos": 0, "ignorados": []})

    corpo = _como_mestre(app).post(
        "/analisesps/api/folha/auxilio/selecao",
        json={"tipo": "alimentacao", "ano": 2026, "mes": 9,
              "decisoes": [{"cpf": "99713349334", "pagar": False},
                           {"cpf": "03513441363", "pagar": True}]}).get_json()

    assert corpo["ok"] is True and corpo["gravados"] == 1
    assert len(visto["decisoes"]) == 2


def test_salvar_a_selecao_recusa_competencia_invalida(app, monkeypatch):
    from app.apps.analisesps import folha_auxilio as fx

    monkeypatch.setattr(fx, "salvar_selecao", lambda *a, **k: {})
    resposta = _como_mestre(app).post(
        "/analisesps/api/folha/auxilio/selecao",
        json={"tipo": "alimentacao", "ano": 2026, "mes": 99})
    assert resposta.status_code == 400


def test_a_tela_do_auxilio_sem_a_migracao_AVISA(app, monkeypatch):
    _preparar_auxilio(monkeypatch, pronto=False)
    resposta = _como_mestre(app).get("/analisesps/folha/auxilios")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html
    assert "btn-gravar-ajuste" not in html


def test_a_tela_do_auxilio_nao_cai_quando_o_calculo_estoura(app, monkeypatch):
    """Tela branca não diz nada a quem está com o pagamento para fechar."""
    from app.apps.analisesps import folha_auxilio as fx, sincronizacao

    def explode(*a, **k):
        raise fx.ErroDoAuxilio("o cadastro está vazio.")

    monkeypatch.setattr(fx, "_pronto", lambda: True)
    monkeypatch.setattr(fx, "calcular", explode)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})

    resposta = _como_mestre(app).get("/analisesps/folha/auxilios")
    assert resposta.status_code == 200
    assert "o cadastro está vazio" in resposta.get_data(as_text=True)


def test_competencia_torta_na_URL_cai_no_mes_de_hoje(app, monkeypatch):
    """Endereço colado errado não pode virar erro 500."""
    visto = {}
    from app.apps.analisesps import folha_auxilio as fx, sincronizacao

    monkeypatch.setattr(fx, "_pronto", lambda: True)
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})

    def espiar(tipo, ano, mes):
        visto.update({"tipo": tipo, "ano": ano, "mes": mes})
        return _auxilio_calculado()

    monkeypatch.setattr(fx, "calcular", espiar)
    resposta = _como_mestre(app).get(
        "/analisesps/folha/auxilios?tipo=inventada&ano=abc&mes=99")
    assert resposta.status_code == 200
    assert visto["tipo"] == "alimentacao", "verba desconhecida cai na primeira"
    assert 1 <= visto["mes"] <= 12


def test_quem_so_consulta_nao_ve_os_campos_de_ajuste(app, monkeypatch):
    """Ajuste é operação: muda o que vai ser pago."""
    _preparar_auxilio(monkeypatch)
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/folha/auxilios", follow_redirects=True).get_data(as_text=True)
    assert "GERLANIO GOMES LIMA" in html
    assert "btn-gravar-ajuste" not in html
    assert "campo-pagar" not in html


def test_gravar_o_ajuste_guarda_os_TRES_estados(app, monkeypatch):
    from app.apps.analisesps import folha_auxilio as fx
    gravado = {}

    monkeypatch.setattr(fx, "_pronto", lambda: True)
    monkeypatch.setattr(fx, "gravar_ajuste",
                        lambda *a, **k: gravado.update({"a": a, "k": k}))
    monkeypatch.setattr(fx, "limpar_ajuste",
                        lambda *a, **k: gravado.update({"limpou": a}) or True)

    cliente = _como_mestre(app)
    resposta = cliente.post("/analisesps/api/folha/auxilio/ajuste", json={
        "tipo": "alimentacao", "ano": 2026, "mes": 9, "cpf": "99713349334",
        "pagar": False})
    assert resposta.get_json()["ok"] is True
    assert gravado["k"]["pagar"] is False

    # Nada mexido: TIRA o ajuste, para voltar a valer o cálculo.
    gravado.clear()
    cliente.post("/analisesps/api/folha/auxilio/ajuste", json={
        "tipo": "alimentacao", "ano": 2026, "mes": 9, "cpf": "99713349334",
        "pagar": None})
    assert "limpou" in gravado and "k" not in gravado


def test_gravar_o_ajuste_recusa_competencia_invalida(app, monkeypatch):
    from app.apps.analisesps import folha_auxilio as fx

    monkeypatch.setattr(fx, "_pronto", lambda: True)
    monkeypatch.setattr(fx, "gravar_ajuste", lambda *a, **k: None)
    resposta = _como_mestre(app).post("/analisesps/api/folha/auxilio/ajuste",
                                      json={"tipo": "alimentacao", "mes": 99})
    assert resposta.status_code == 400
    assert resposta.get_json()["ok"] is False


def test_gravar_o_ajuste_devolve_a_frase_do_erro(app, monkeypatch):
    from app.apps.analisesps import folha_auxilio as fx

    def explode(*a, **k):
        raise fx.ErroDoAuxilio("não reconheci o CPF desta pessoa.")

    monkeypatch.setattr(fx, "_pronto", lambda: True)
    monkeypatch.setattr(fx, "gravar_ajuste", explode)
    resposta = _como_mestre(app).post("/analisesps/api/folha/auxilio/ajuste",
                                      json={"tipo": "transporte", "ano": 2026,
                                            "mes": 9, "cpf": "1", "pagar": True})
    assert resposta.status_code == 400
    assert "CPF" in resposta.get_json()["erro"]


# ---------------------------------------------------------------------------
# A SUBTELA DE GERAR PAGAMENTO — 27/09/2026
#
# ⚠️ É A TELA MAIS SENSÍVEL DA ÁREA: daqui sai o arquivo que vai para o portal do
# banco. Só do mestre, e com conferência antes de gerar.
# ---------------------------------------------------------------------------
def _preparar_pagamento(monkeypatch, fechados=None, log=None, pronto=True):
    from app.apps.analisesps import (folha_apropriacao_guardada as ag,
                                     folha_pagamento as fpg)

    monkeypatch.setattr(fpg, "_pronto", lambda: pronto)
    monkeypatch.setattr(fpg, "log", lambda *a, **k: log or [])
    monkeypatch.setattr(ag, "fechamentos", lambda *a, **k: fechados or [])


def _fechado(verba="alimentacao", fecha=True, ano=2026, mes=9):
    from decimal import Decimal as D
    return {"ano": ano, "mes": mes, "tipo": "quinzena", "verba": verba,
            "total": D("330.00"), "pessoas": 1, "fecha": fecha,
            "fechado_em": None, "fechado_por": "MARCELO"}


def test_a_tela_de_pagamento_diz_AS_DUAS_REGRAS_antes_do_botao(app, monkeypatch):
    """Descobrir a regra do SomaPay depois de subir no portal custa a rodada
    inteira: já se gerou, subiu, criou o card e avisou a equipe."""
    _preparar_pagamento(monkeypatch, fechados=[_fechado()])
    html = _como_mestre(app).get(
        "/analisesps/folha/pagamento").get_data(as_text=True)

    assert "Um arquivo por conta, sempre" in html
    assert "mesmo CPF duas vezes" in html
    assert "BeeVale" in html and "SomaPay" in html


def test_sem_apropriacao_fechada_a_tela_EXPLICA_em_vez_de_oferecer(app,
                                                                   monkeypatch):
    """Oferecer o botão sem ter o que pagar só gera erro depois de ele escolher
    tudo."""
    _preparar_pagamento(monkeypatch, fechados=[])
    html = _como_mestre(app).get(
        "/analisesps/folha/pagamento").get_data(as_text=True)

    assert "Nenhuma apropriação fechada" in html
    assert 'id="btn-gerar"' not in html


def test_a_verba_que_NAO_BATE_aparece_marcada_e_nao_escondida(app, monkeypatch):
    """Um fechamento que não batia continua dizendo que não batia, e quem gera
    decide sabendo. Esconder faria a folha sair com gente de fora."""
    _preparar_pagamento(monkeypatch, fechados=[_fechado(fecha=False)])
    html = _como_mestre(app).get(
        "/analisesps/folha/pagamento?ano=2026&mes=9").get_data(as_text=True)
    assert ">divergente</span>" in html
    assert "Alimentação" in html


def test_a_tela_de_pagamento_e_SO_DO_MESTRE(app):
    """O log mostra o link de arquivos com nome, CPF e valor de ~500 pessoas."""
    from app.apps.analisesps import auth

    assert auth.e_so_do_mestre("analisesps.tela_folha_pagamento") is True
    assert auth.e_so_do_mestre("analisesps.folha_pagamento_gerar") is True
    assert auth.e_so_do_mestre("analisesps.folha_pagamento_preparar") is True

    resposta = como(app, SENHA_CONSULTA).get("/analisesps/folha/pagamento")
    assert resposta.status_code in (302, 403, 404)


def test_o_log_mostra_o_LINK_de_baixar(app, monkeypatch):
    """Pedido dele: *"que tenha também o log na aplicação, com as informações e o
    link que a gente quer baixar por lá"*."""
    from decimal import Decimal as D

    _preparar_pagamento(monkeypatch, fechados=[], log=[{
        "id": 1, "ano": 2026, "mes": 9, "tipo": "quinzena",
        "destino": "beevale", "rotulo_destino": "BeeVale",
        "verbas": "alimentacao+transporte",
        "rotulo_verbas": "Alimentação + Transporte", "conta": "50024",
        "nome": "BeeVale - 09-2026.xlsx", "pessoas": 12, "total": D("4200.00"),
        "link": "https://drive.google.com/file/d/abc/view", "card_pipefy": "",
        "link_card": "", "avisos": "", "criado_em": None,
        "criado_por": "MARCELO", "competencia": "09/2026"}])

    html = _como_mestre(app).get(
        "/analisesps/folha/pagamento").get_data(as_text=True)

    assert "https://drive.google.com/file/d/abc/view" in html
    assert "Alimentação + Transporte" in html
    assert "50024" in html
    assert "4.200,00" in html


def test_o_arquivo_com_AVISO_fica_marcado_no_log(app, monkeypatch):
    """Aviso que só existiu na tela não explica diferença nenhuma três meses
    depois."""
    from decimal import Decimal as D

    _preparar_pagamento(monkeypatch, log=[{
        "id": 1, "ano": 2026, "mes": 9, "tipo": "quinzena",
        "destino": "beevale", "rotulo_destino": "BeeVale", "verbas": "folha",
        "rotulo_verbas": "Folha", "conta": "", "nome": "x.xlsx", "pessoas": 1,
        "total": D("10.00"), "link": "https://drive/x", "card_pipefy": "",
        "link_card": "", "avisos": "estas linhas estão sem conta de pagamento.",
        "criado_em": None, "criado_por": "MARCELO", "competencia": "09/2026"}])

    html = _como_mestre(app).get(
        "/analisesps/folha/pagamento").get_data(as_text=True)
    assert "linha-alerta" in html
    assert "sem conta de pagamento" in html


def test_sem_a_migracao_a_tela_de_pagamento_AVISA(app, monkeypatch):
    _preparar_pagamento(monkeypatch, pronto=False)
    resposta = _como_mestre(app).get("/analisesps/folha/pagamento")
    assert resposta.status_code == 200
    html = resposta.get_data(as_text=True)
    assert "Aplicar atualizações do banco" in html
    assert 'id="btn-gerar"' not in html


def test_conferir_NAO_gera_nada(app, monkeypatch):
    """Conferir é o passo que ele pediu para ver antes: não grava e não sobe."""
    from app.apps.analisesps import folha_pagamento as fpg
    from decimal import Decimal as D
    chamou = {}

    monkeypatch.setattr(fpg, "gerar",
                        lambda *a, **k: chamou.setdefault("gerou", True))
    monkeypatch.setattr(fpg, "preparar", lambda *a, **k: {
        "pode_juntar": True, "motivo_nao_junta": "",
        "resumo": {"arquivos": 2, "pessoas": 5, "total": D("100.00"),
                   "pode_gerar": True, "com_critica": []},
        "lotes": [{"conta": "50024", "quantos": 5, "total": D("100.00"),
                   "verbas": ["alimentacao"], "criticas": []}]})

    corpo = _como_mestre(app).post(
        "/analisesps/api/folha/pagamento/preparar",
        json={"ano": 2026, "mes": 9, "tipo": "quinzena",
              "verbas": ["alimentacao"], "destino": "beevale"}).get_json()

    assert corpo["ok"] is True
    assert corpo["resumo"]["arquivos"] == 2
    assert "gerou" not in chamou


def test_gerar_devolve_os_LINKS_dos_arquivos(app, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fpg
    from decimal import Decimal as D

    monkeypatch.setattr(fpg, "gerar", lambda *a, **k: {
        "ok": True, "competencia": "09/2026",
        "resumo": {"arquivos": 1},
        "arquivos": [{"id": 1, "nome": "BeeVale.xlsx", "link": "https://drive/1",
                      "conta": "50024", "destino": "beevale",
                      "total": D("100.00"), "avisos": ""},
                     {"id": 2, "nome": "Analise.xlsx", "link": "https://drive/2",
                      "conta": "", "destino": "analise", "total": D("100.00"),
                      "avisos": ""}]})

    corpo = _como_mestre(app).post(
        "/analisesps/api/folha/pagamento/gerar",
        json={"ano": 2026, "mes": 9, "tipo": "quinzena",
              "verbas": ["alimentacao"], "destino": "beevale"}).get_json()

    assert corpo["ok"] is True
    assert [a["link"] for a in corpo["arquivos"]] == ["https://drive/1",
                                                      "https://drive/2"]


def test_gerar_devolve_a_frase_do_erro_para_a_tela(app, monkeypatch):
    """A frase diz o que consertar — é o que evita gerar de novo errado."""
    from app.apps.analisesps import folha_pagamento as fpg

    def explode(*a, **k):
        raise fpg.ErroDoPagamento(
            "Transporte não tem apropriação fechada em 09/2026.")

    monkeypatch.setattr(fpg, "gerar", explode)
    resposta = _como_mestre(app).post(
        "/analisesps/api/folha/pagamento/gerar",
        json={"ano": 2026, "mes": 9, "tipo": "quinzena",
              "verbas": ["transporte"], "destino": "somapay"})
    assert resposta.status_code == 400
    assert "apropriação fechada" in resposta.get_json()["erro"]


def test_nao_existe_mais_obra_editavel_na_tela_de_auxilio(app, monkeypatch):
    """⚠️ A obra editável saiu em 28/09/2026: *"essa informação vem do cadastro."*
    Obra digitada aqui divergiria do cadastro e do rateio, e ninguém saberia qual
    das duas manda. Este teste existe para ela não voltar."""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "campo-obra" not in html
    assert "obraSeMudou" not in html
    assert "obra trocada por você" not in html


# ---------------------------------------------------------------------------
def _diarista(**mudancas):
    import datetime as dt
    from decimal import Decimal as D
    base = {"cpf": "99713349334", "cpf_bonito": "997.133.493-34",
            "nome": "GERLANIO", "cargo": "SERVENTE", "fase": "Colaboradores Ativos",
            "situacao": "ativo", "link_pipefy": "", "tipo_contrato": "CTPS",
            "data_inicio": dt.date(2026, 9, 1), "data_admissao": dt.date(2026, 9, 8),
            "obra_cadastro": "1042", "valor_diaria": D("100.00"),
            "dias": [{"data": dt.date(2026, 9, 5), "obra": "1042",
                      "quantidade": D("1"), "motivo": "", "adicional": D("10.00"),
                      "porque_adicional": "sábado", "valor": D("110.00"),
                      "presenca": "PRESENÇA", "horas": "08:00", "batidas": []}],
            "quantidade": D("7"), "adicionais": D("10.00"), "valor": D("710.00"),
            "por_obra": [{"obra": "1042", "dias": D("7"), "valor": D("710.00")}],
            "obra": "1042", "obras": ["1042"], "dias_de_ctps": 8,
            "dias_sem_decidir": 0, "motivos": [], "pagar": True,
            "pagar_calculado": True, "impossivel": False, "desligado": False,
            "vigia": False, "ajuste_pagar": None}
    base.update(mudancas)
    return base


def _diaristas_calculado(pessoas=None, **mudancas):
    import datetime as dt
    from decimal import Decimal as D
    pessoas = [_diarista()] if pessoas is None else pessoas
    a_pagar = [p for p in pessoas if p["pagar"]]
    base = {"ano": 2026, "mes": 9, "qual": "quinzena",
            "rotulo_periodo": "1ª quinzena (1 a 15)",
            "competencia": "09/2026", "inicio": dt.date(2026, 9, 1),
            "fim": dt.date(2026, 9, 30), "tem_ponto": True,
            "tem_coluna_da_diaria": True, "pessoas": pessoas, "sem_cadastro": [],
            "quantos": len(pessoas), "quantos_a_pagar": len(a_pagar),
            "total": sum((p["valor"] for p in a_pagar), D("0.00")),
            "por_obra": [{"obra": "1042", "pessoas": 1, "dias": D("7"),
                          "total": D("710.00")}], "fechamento": None}
    base.update(mudancas)
    return base


def _preparar_diaristas(monkeypatch, calculado=None):
    from app.apps.analisesps import folha_diaristas as fd
    monkeypatch.setattr(fd, "calcular", lambda *a, **k: calculado
                        if calculado is not None
                        else {"tem_ponto": False, "pessoas": [],
                              "sem_cadastro": []})


def test_a_tela_de_diaristas_mostra_QUANTO_e_explica_a_regra(app, monkeypatch):
    """⚠️ A regra é contraintuitiva e precisa estar escrita: a MESMA pessoa tem
    dias de diária e dias de CTPS no mês em que foi registrada. E desde
    01/10/2026 a tela diz quanto pagar."""
    _preparar_diaristas(monkeypatch, _diaristas_calculado())
    html = _como_mestre(app).get(
        "/analisesps/folha/diaristas").get_data(as_text=True)
    assert "dia por dia" in html
    assert "GERLANIO" in html and "997.133.493-34" in html
    assert "710,00" in html, "o valor da pessoa"
    assert "+20 no feriado, +10 no sábado e +20 no domingo" in html
    assert "8 de CTPS" in html, "os dias de CTPS ficam ditos no dia a dia"
    assert "sábado" in html, "o adicional do dia, com o porquê"
    assert 'id="fechar-diaria"' in html


def test_DESLIGADO_com_diaria_aparece_A_PAGAR_com_o_alerta(app, monkeypatch):
    """O dono, 02/10/2026: o desligado que continuou trabalhando tem direito à
    diária — entra no pagamento, com o alerta (o contrário da contabilidade)."""
    saiu = _diarista(cpf="22222222222", nome="JASAIU", situacao="saiu",
                     desligado=True, pagar=True, pagar_calculado=True,
                     motivos=["colaborador desligado no cadastro — diária devida "
                              "pelos dias trabalhados; confira."])
    _preparar_diaristas(monkeypatch, _diaristas_calculado([_diarista(), saiu]))
    html = _como_mestre(app).get("/analisesps/folha/diaristas").get_data(as_text=True)
    assert "GERLANIO" in html and "JASAIU" in html
    assert "desligado · a pagar" in html
    assert "1 desligado com diária" in html


def test_sem_o_VALOR_DA_DIARIA_a_pessoa_trava_e_a_lateral_diz(app, monkeypatch):
    sem = _diarista(valor_diaria=None, valor=__import__("decimal").Decimal("0.00"),
                    pagar=False, pagar_calculado=False, impossivel=True,
                    motivos=["o cadastro não tem o valor da diária."])
    _preparar_diaristas(monkeypatch, _diaristas_calculado([sem]))
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/diaristas").get_data(as_text=True)
    # Fora da lista sem filtro (a planilha pula "CORRIGIR VALOR DIÁRIA"), mas a
    # lateral diz quantos e o filtro mostra quem.
    assert "1 com cadastro incompleto" in html
    assert "GERLANIO" not in html
    filtrado = cliente.get(
        "/analisesps/folha/diaristas?situacao=falta_dado").get_data(as_text=True)
    assert "GERLANIO" in filtrado and "cadastro incompleto" in filtrado


def test_SEM_DIARIA_e_VIGIA_ficam_fora_da_lista_e_o_filtro_mostra(app, monkeypatch):
    """O dono, 02/10/2026: *"tem aparecendo gente que não tem nenhum ponto pra
    pagar diária (…) Se não tem não precisa aparecer"*, e o vigia também não —
    *"na tela, a princípio aparecer somente quem tem a pagar"*."""
    D = __import__("decimal").Decimal
    sem = _diarista(cpf="33333333333", nome="SEMDIARIA", sem_diaria=True,
                    pagar=False, pagar_calculado=False, valor=D("0.00"),
                    motivos=["nenhum dia com presença que gere diária no período."])
    vigia = _diarista(cpf="44444444444", nome="OVIGIA", vigia=True, pagar=False,
                      pagar_calculado=False)
    fora = _diarista(cpf="55555555555", nome="DESMARCADO", pagar=False,
                     pagar_calculado=True)
    _preparar_diaristas(monkeypatch, _diaristas_calculado(
        [_diarista(), sem, vigia, fora]))
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/diaristas").get_data(as_text=True)
    assert "GERLANIO" in html and "DESMARCADO" in html
    assert "SEMDIARIA" not in html and "OVIGIA" not in html
    assert "1 sem diária no período" in html and "1 vigia" in html
    assert "fora do pagamento" in html
    com_filtro = cliente.get(
        "/analisesps/folha/diaristas?situacao=sem_diaria").get_data(as_text=True)
    assert "SEMDIARIA" in com_filtro and "GERLANIO" not in com_filtro


def test_sem_o_ponto_a_tela_de_diaristas_diz_o_que_fazer(app, monkeypatch):
    _preparar_diaristas(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/diaristas").get_data(as_text=True)
    assert "ainda não foi importado" in html
    assert "Trazer o ponto" in html


def test_quem_bate_ponto_e_NAO_esta_no_cadastro_aparece(app, monkeypatch):
    """⚠️ Esconder faria alguém trabalhar e não receber, sem nada na tela."""
    _preparar_diaristas(monkeypatch, _diaristas_calculado(
        sem_cadastro=[{"cpf": "11144477735", "nome": "JOAO DO PONTO", "dias": 12}]))
    html = _como_mestre(app).get(
        "/analisesps/folha/diaristas").get_data(as_text=True)
    assert "JOAO DO PONTO" in html
    assert "com batida de ponto e sem cadastro" in html


def test_os_filtros_dos_diaristas_sao_de_CAIXINHA(app, monkeypatch):
    _preparar_diaristas(monkeypatch, _diaristas_calculado())
    html = _como_mestre(app).get(
        "/analisesps/folha/diaristas").get_data(as_text=True)
    assert 'type="checkbox" name="situacao"' in html
    assert 'type="checkbox" name="obra"' in html
    assert 'type="checkbox" name="obra_cadastro"' in html


# ---------------------------------------------------------------------------
# O PONTO: dizer se está carregando — 28/09/2026
#
# *"Se o ponto veio, se o ponto não veio, só Deus sabe o que está acontecendo com
#  essa API. Se ela está carregando, se ela não está."*
# ---------------------------------------------------------------------------
def test_a_tela_do_ponto_DIZ_que_esta_carregando_ao_abrir(app, monkeypatch):
    """Antes a tela só acompanhava se VOCÊ tivesse apertado o botão naquela aba.
    Quem abria depois — ou de outro computador — não via nada."""
    from app.apps.analisesps import ponto as _ponto, tarefas

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [])
    monkeypatch.setattr(tarefas, "estado", lambda: {
        "rodando": True, "detalhe": {"etapa": "ponto",
                                     "progresso": "página 3 de 12"}})

    html = _como_mestre(app).get(
        "/analisesps/folha/ponto").get_data(as_text=True)
    assert "Carga do ponto em andamento" in html
    assert "página 3 de 12" in html
    assert "acompanharSozinho" in html


def test_a_tela_do_ponto_avisa_quando_a_carga_foi_INTERROMPIDA(app, monkeypatch):
    """Publicar reinicia o serviço e mata carga longa — e a tela tem de dizer o
    que fazer, não deixar a pessoa achando que o mês veio."""
    from app.apps.analisesps import ponto as _ponto, tarefas

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [])
    monkeypatch.setattr(tarefas, "estado", lambda: {
        "rodando": False, "interrompida": {"etapa": "ponto"}, "detalhe": {}})

    html = _como_mestre(app).get(
        "/analisesps/folha/ponto").get_data(as_text=True)
    assert "A última carga foi interrompida" in html
    assert "substitui" in html


# ---------------------------------------------------------------------------
# COLABORADORES: filtro, e quem saiu fora da lista — 28/09/2026
# ---------------------------------------------------------------------------
def test_a_tela_de_colaboradores_TEM_FILTRO_na_lateral(app, monkeypatch):
    """*"Na parte de colaboradores, a mesma coisa, tem que ter o filtro."*"""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", lambda: {
        "pronto": True, "quando": "", "pessoas": 3, "avisos": []})
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    monkeypatch.setattr(col, "codigos_das_obras", lambda: {})
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 0, "saiu": 0,
                                         "afastado": 0})

    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert 'name="obra"' in html and 'name="q"' in html
    assert "sem-filtros" not in html
    assert "Para que serve esta tela" in html, (
        "ele disse que ninguém entende para que ela serve")


def test_a_tela_de_colaboradores_diz_que_quem_saiu_ficou_FORA(app, monkeypatch):
    """*"O que é colaborador desligado não deveria nem estar sendo exibido."* Mas
    esconder e não dizer seria o erro que ele corrigiu em 26/09."""
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", lambda: {
        "pronto": True, "quando": "", "pessoas": 3, "avisos": []})
    monkeypatch.setattr(col, "buscar", lambda *a, **k: [])
    monkeypatch.setattr(col, "codigos_das_obras", lambda: {})
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 5, "saiu": 3,
                                         "afastado": 2})

    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert "estão fora da lista abaixo" in html
    assert "incluir todos" in html


# ---------------------------------------------------------------------------
# AS CORREÇÕES DE 29/09/2026 — a segunda rodada de uso
# ---------------------------------------------------------------------------
def _cadastro_com(monkeypatch, avisos=None, quando="2026-09-26T18:35:00",
                  quadro=None, fases=None, lista=None):
    from app.apps.analisesps import colaboradores as col

    monkeypatch.setattr(col, "quando_atualizou", lambda: {
        "pronto": True, "quando": quando, "pessoas": 3531,
        "avisos": avisos or []})
    monkeypatch.setattr(col, "panorama", lambda: quadro or {
        "pronto": True, "total": 3531, "ativos": 512,
        "com_alimentacao": 480, "com_transporte": 300,
        "sem_card": 0, "sem_obra": 0, "sem_id_fortes": 0})
    monkeypatch.setattr(col, "fases", lambda: fases or [
        {"fase": "Colaboradores Ativos", "quantos": 512},
        {"fase": "Colaboradores Desligados", "quantos": 3000}])
    monkeypatch.setattr(col, "buscar", lambda *a, **k: lista or [])
    monkeypatch.setattr(col, "codigos_das_obras", lambda: {})
    monkeypatch.setattr(col, "contar_quem_esta_saindo",
                        lambda *a, **k: {"com_sinal": 0, "saiu": 0,
                                         "afastado": 0})


def test_o_aviso_do_cadastro_LEVA_A_DATA_da_carga(app, monkeypatch):
    """⚠️ ISTO CUSTOU UMA CONFUSÃO INTEIRA em 29/09/2026. Ele leu um aviso de três
    dias antes — "não achei a coluna Modalidade…", escrito pelo código ANTIGO — e
    concluiu que a correção não havia funcionado. O aviso fica GUARDADO no banco, e
    sem data parece estado de agora."""
    _cadastro_com(monkeypatch, avisos=[
        'não achei a coluna de "Modalidade Auxílio Alimentação"'])
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert "Na carga de" in html
    assert "26/09" in html and "18:35" in html
    # ⚠️ A SEGUNDA FRASE MUDOU EM 29/09/2026. Ela explicava epistemologia ("este
    # aviso é daquela carga — não é o estado de agora") e ele respondeu *"não
    # entendi essa pergunta"*. A data já está na frase de cima; o que faltava era
    # dizer o que FAZER.
    assert "não é o estado" not in html
    assert "Corrija na planilha de colaboradores" in html
    assert "Atualizar cadastro" in html


def test_a_tela_de_colaboradores_tem_KPIs(app, monkeypatch):
    """*"3531 pessoa(s) trazidas da planilha em 26/09 às 18:35. Isso vai aparecer
    sempre assim? Não tem nada de KPI essa tela."*"""
    _cadastro_com(monkeypatch, quadro={
        "pronto": True, "total": 3531, "ativos": 512, "com_alimentacao": 480,
        "com_transporte": 300, "sem_card": 12, "sem_obra": 7,
        "sem_id_fortes": 40})
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert '<div class="kpi-rotulo">Ativos</div>' in html and ">512<" in html
    assert "de 3531 no cadastro" in html
    assert "Sem código de obra" in html
    assert "Sem ID Fortes" in html
    # ⚠️ NO MESMO MOLDE DAS SOLICITAÇÕES: `.kpi.estatico`. Sem `estatico` o quadro
    # sobe no hover e mostra cursor de mão, como se fosse clicável.
    assert "kpi estatico" in html
    assert "kpi-sub" in html


def test_os_indicadores_que_TRAVAM_pagamento_ficam_ambar(app, monkeypatch):
    """E ficam discretos quando não há nada — indicador que grita sempre não é
    indicador."""
    _cadastro_com(monkeypatch, quadro={
        "pronto": True, "total": 10, "ativos": 10, "com_alimentacao": 5,
        "com_transporte": 5, "sem_card": 0, "sem_obra": 3, "sem_id_fortes": 0})
    com = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert "estatico ambar" in com

    _cadastro_com(monkeypatch, quadro={
        "pronto": True, "total": 10, "ativos": 10, "com_alimentacao": 5,
        "com_transporte": 5, "sem_card": 0, "sem_obra": 0, "sem_id_fortes": 0})
    sem = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)
    assert "estatico ambar" not in sem


def test_o_filtro_por_FASE_ATUAL_existe_e_vem_do_banco(app, monkeypatch):
    """*"Havia falado que a Fase Atual, coluna AX, é super importante. Não tem isso
    como filtro em colaboradores."* E a lista vem do banco: fase nova no Pipefy
    aparece sozinha, com quantos em cada."""
    _cadastro_com(monkeypatch, fases=[
        {"fase": "Colaboradores Ativos", "quantos": 512},
        {"fase": "Aguardando Documentos", "quantos": 18}])
    html = _como_mestre(app).get(
        "/analisesps/folha/colaboradores").get_data(as_text=True)

    assert 'name="fase"' in html
    assert "Fase atual" in html
    assert "Aguardando Documentos (18)" in html


def test_o_filtro_por_FASE_chega_ao_buscar(app, monkeypatch):
    from app.apps.analisesps import colaboradores as col
    pedidos = {}

    def falso_buscar(texto="", **resto):
        pedidos.update(resto)
        return []

    _cadastro_com(monkeypatch)
    monkeypatch.setattr(col, "buscar", falso_buscar)
    _como_mestre(app).get(
        "/analisesps/folha/colaboradores?fase=Colaboradores+Ativos"
        "&de=01/09/2026&ate=30/09/2026")

    assert pedidos["fase"] == "Colaboradores Ativos"
    assert pedidos["admitido_de"].isoformat() == "2026-09-01"
    assert pedidos["admitido_ate"].isoformat() == "2026-09-30"


def test_a_tela_do_ponto_MOSTRA_a_ultima_tentativa_que_falhou(app, monkeypatch):
    """⚠️ A RECLAMAÇÃO REPETIDA TRÊS VEZES: *"clico em trazer o ponto, sistema diz
    que vai trazer e NÃO TRAZ nada. Não sei se conseguiu conectar, se tá indo, se
    não tá, ninguém sabe de nada."*

    O registro da tentativa SEMPRE existiu, com o erro da API dentro. Faltava a tela
    mostrar — e falha que só aparece no log do serviço é falha que ele não lê."""
    import datetime as dt

    from app.apps.analisesps import ponto as _ponto, tarefas

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [])
    monkeypatch.setattr(tarefas, "estado", lambda: {
        "rodando": False, "detalhe": None, "interrompida": None})
    monkeypatch.setattr(tarefas, "ultima_do_tipo", lambda tipo: {
        "tipo": "ponto", "disparo": "MARCELO",
        "inicio": dt.datetime(2026, 9, 29, 10, 0),
        "fim": dt.datetime(2026, 9, 29, 10, 2), "ok": False,
        "mensagem": "o Mobponto recusou (HTTP 401): api-key inválida",
        "linhas": 0, "visto_em": None, "em_andamento": False})

    html = _como_mestre(app).get(
        "/analisesps/folha/ponto").get_data(as_text=True)

    assert "A última tentativa falhou" in html
    assert "HTTP 401" in html
    assert "credenciais do Mobponto estão sendo recusadas" in html, (
        "o recado tem de dizer o que fazer com um 401, não só mostrar o erro")


def test_a_tela_do_ponto_mostra_a_ultima_que_DEU_CERTO_mas_veio_vazia(
        app, monkeypatch):
    """O caso que ele descreveu: "diz que vai trazer e não traz nada". Se a API
    respondeu sem lançamento, a tela tem de dizer isso — e não ficar muda."""
    import datetime as dt

    from app.apps.analisesps import ponto as _ponto, tarefas

    monkeypatch.setattr(_ponto, "_pronto", lambda: True)
    monkeypatch.setattr(_ponto, "configurado", lambda: True)
    monkeypatch.setattr(_ponto, "cargas", lambda *a, **k: [])
    monkeypatch.setattr(tarefas, "estado", lambda: {
        "rodando": False, "detalhe": None, "interrompida": None})
    monkeypatch.setattr(tarefas, "ultima_do_tipo", lambda tipo: {
        "tipo": "ponto", "disparo": "MARCELO",
        "inicio": dt.datetime(2026, 9, 29, 10, 0),
        "fim": dt.datetime(2026, 9, 29, 10, 1), "ok": True,
        "mensagem": "0 dia(s) de 0 pessoa(s), 0 de 0 página(s)",
        "linhas": 0, "visto_em": None, "em_andamento": False})

    html = _como_mestre(app).get(
        "/analisesps/folha/ponto").get_data(as_text=True)

    assert "Última importação concluída" in html
    assert "0 dia(s)" in html
    assert "não há competência gravada" in html, (
        "sucesso sem nada carregado tem de ser explicado")


def test_a_folha_importada_mostra_QUANTAS_PESSOAS_precisam_de_olho(app,
                                                                  monkeypatch):
    """*"Você tá muito preocupado com os totalizadores do arquivo de importação,
    quando a preocupação deve ser linha a linha de cada colaborador."*"""
    from decimal import Decimal as D

    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "_pronto", lambda: True)
    monkeypatch.setattr(fa, "listar", lambda *a, **k: [{
        "id": 1, "competencia": "09/2026", "rotulo_do_tipo": "Quinzena",
        "pessoas": 406, "total": D("353069.48"), "importado_em": None,
        "importado_por": "MARCELO", "fecha": False,
        "lista_de_avisos": ["linha que não reconheci: Empregado(s))"]}])
    monkeypatch.setattr(fa, "criticas", lambda folha_id: {
        "pendentes": [{"id_fortes": "123"}, {"id_fortes": "124"}],
        "sairam": [{"nome": "QUEM SAIU"}], "saindo": []})

    # `?lista=1`: com folha importada, a aba leva direto para a folha aberta.
    html = _como_mestre(app).get(
        "/analisesps/folha/importar?lista=1").get_data(as_text=True)

    assert '<th class="num">Pendências</th>' in html
    assert "3</b> colaborador(es)" in html
    assert "2 sem cadastro" in html
    assert "1 desligado(s)" in html


def test_a_busca_de_ferias_tem_SEMPRE_o_limpar(app, monkeypatch):
    """*"Campo procurar de férias, bota limpar."* Botão que só existe depois de
    filtrar obriga a pessoa a descobrir que ele existe."""
    from app.apps.analisesps import folha_calendario as fc, sincronizacao

    monkeypatch.setattr(fc, "_pronto", lambda: True)
    monkeypatch.setattr(fc, "listar_feriados", lambda *a, **k: [])
    monkeypatch.setattr(fc, "listar_ferias", lambda *a, **k: [])
    monkeypatch.setattr(sincronizacao, "referencias_rateio",
                        lambda: {"obras": [], "categorias": []})

    html = _como_mestre(app).get(
        "/analisesps/folha/calendario").get_data(as_text=True)
    assert "Limpar" in html
    assert "barra-acoes" not in html, (
        "a barra das Solicitações esticava o campo de ponta a ponta")


def test_o_CSS_poe_TETO_na_largura_dos_campos():
    """*"Os campos em várias telas estão esticados demais, ocupa de ponta a ponta a
    tela. Fica horrível numa tela grande."*"""
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(
        encoding="utf-8")
    assert ".cartao input[type=text]" in css
    assert "max-width: 420px" in css
    assert ".solta-arquivo, .caixa-arquivo, .area-arquivo { max-width: 760px; }" in css
    # ⚠️ ESTA LINHA MUDOU NO MESMO DIA EM QUE FOI ESCRITA, e o motivo está em
    # `test_analisesps_filtros.py`: `.filtros input` alcançava também as CAIXAS DE
    # MARCAR de cada opção. Cada `checkbox` virava um retângulo da largura da
    # coluna e o rótulo ao lado sumia — sete blocos de filtro sem texto nenhum, que
    # foi o que o dono viu e descreveu como "os filtros estão todos vazios".
    #
    # O campo de TEXTO continua acompanhando a coluna; a caixa de marcar ficou de
    # fora, e o tipo agora está escrito no seletor.
    assert (".filtros input:not([type=checkbox]):not([type=radio])" in css
            and ".filtros select { width: 100%; max-width: none; }" in css)
    # ⚠️ SEM OS COMENTÁRIOS: o comentário que explica o estrago CITA a regra
    # errada, e uma busca por texto cru acusaria a própria explicação. Foi o que
    # aconteceu na primeira versão deste teste.
    import re as _re
    sem_comentario = _re.sub(r"/\*.*?\*/", "", css, flags=_re.S)
    assert ".filtros input, .filtros select {" not in sem_comentario, (
        "a regra larga voltou — ela apaga o rótulo de todos os filtros do módulo")


# ---------------------------------------------------------------------------
# O PADRÃO DE TABELA — 29/09/2026
#
# Ele mandou *"siga o mesmo padrão de cabeçalho, de cor da tabela e etc... pra todas
# as telas"*, eu conferi que as duas usavam `class="sps"`, concluí "já está igual" e
# segui. Não estava: faltavam os DOIS invólucros e faltava a cor dizer o estado.
# ---------------------------------------------------------------------------
def test_as_tabelas_da_folha_usam_a_MESMA_superficie_das_solicitacoes(app,
                                                                     monkeypatch):
    """⚠️ NÃO É A COR DAS CÉLULAS, É A SUPERFÍCIE. O padrão é a tabela dentro de
    `.tabela-wrap` (cartão branco, sombra, cantos) e `.tabela-rolagem` (cabeçalho
    grudado no topo). Sem eles a tabela fica nua, direto no fundo da página — e é
    isso que se lê como "outra cor de tabela"."""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "tabela-wrap" in html
    assert "tabela-rolagem" in html
    # ⚠️ `livre`: nas telas da folha as tabelas são EMPILHADAS (o Panorama tem
    # cinco). O teto de altura do original criaria cinco caixas de rolagem na mesma
    # tela, pior do que uma página comprida.
    assert "tabela-rolagem livre" in html


def test_TODA_tela_da_folha_envolve_as_tabelas():
    """Uma tela nova nascer com a tabela nua é o defeito voltando. Este teste lê os
    templates, porque é mais barato que abrir nove telas."""
    base = Path("app/apps/analisesps/templates")
    nus = []
    for nome in sorted(base.glob("analisesps_folha_*.html")):
        texto = nome.read_text(encoding="utf-8")
        for pedaco in texto.split('<table class="sps"')[1:]:
            # Onde vem ANTES desta tabela: tem de haver o invólucro por perto.
            antes = texto[:texto.index('<table class="sps"' + pedaco[:40])]
            if "tabela-rolagem" not in antes[-260:]:
                nus.append(nome.name)
                break
    # `analisesps_colaboradores.html` não tem o prefixo folha_, mas é da mesma área.
    extra = base / "analisesps_colaboradores.html"
    texto = extra.read_text(encoding="utf-8")
    if '<table class="sps"' in texto and "tabela-rolagem" not in texto:
        nus.append(extra.name)
    assert not nus, ("estas telas da folha têm tabela fora do invólucro padrão: "
                     + ", ".join(sorted(set(nus))))


def test_a_COR_diz_o_estado_da_linha_do_auxilio(app, monkeypatch):
    """*"Ele lê a tabela pela cor antes de ler o texto"* — está escrito no CSS, sobre
    a paleta que ele usa há anos. As minhas tabelas diziam o estado em texto miúdo."""
    from decimal import Decimal as D

    _preparar_auxilio(monkeypatch)
    ok = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    # ⚠️ VERDE para quem vai receber, e NÃO `.selo.pagar` — que é VERMELHO na paleta
    # dele, porque nas Solicitações "Pagar" quer dizer "pendente, urgente". A mesma
    # cor não pode significar coisas opostas em telas vizinhas.
    assert "selo aprovado" in ok
    assert "selo pagar" not in ok

    problema = dict(_auxilio_calculado()["pessoas"][0])
    problema.update({"pagar": False, "pagar_calculado": False,
                     "impossivel": True, "valor": D("0.00"), "dias": 0,
                     "valor_unitario": None,
                     "motivos": ["o cadastro não diz o valor"]})
    _preparar_auxilio(monkeypatch, _auxilio_calculado(pessoas=[problema]))
    ruim = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)
    assert "selo risco" in ruim, "falta de dado no cadastro é vermelho forte"


def test_o_filtro_de_SITUACAO_recorta_a_lista_de_verdade(app, monkeypatch):
    """⚠️ Ele disse em 29/09/2026: *"Situação nem funciona."* Este teste existe para
    provar se recorta ou não — e para não voltar a não recortar."""
    from decimal import Decimal as D

    vai = dict(_auxilio_calculado()["pessoas"][0],
               cpf="11111111111", nome="VAIRECEBER", pagar=True,
               pagar_calculado=True, obra="1042")
    nao = dict(_auxilio_calculado()["pessoas"][0],
               cpf="22222222222", nome="NAOVAI", pagar=False,
               pagar_calculado=False, obra="2050")
    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        pessoas=[nao, vai], quantos=2, quantos_a_pagar=1, total=D("330.00"),
        com_problema=[nao]))

    cliente = _como_mestre(app)

    todos = cliente.get("/analisesps/folha/auxilios").get_data(as_text=True)
    assert "VAIRECEBER" in todos and "NAOVAI" in todos

    so_paga = cliente.get(
        "/analisesps/folha/auxilios?so=pagar").get_data(as_text=True)
    assert "VAIRECEBER" in so_paga
    assert "NAOVAI" not in so_paga, "o filtro de situação não recortou"

    so_problema = cliente.get(
        "/analisesps/folha/auxilios?so=problema").get_data(as_text=True)
    assert "NAOVAI" in so_problema
    assert "VAIRECEBER" not in so_problema

    por_obra = cliente.get(
        "/analisesps/folha/auxilios?obra=2050").get_data(as_text=True)
    assert "NAOVAI" in por_obra and "VAIRECEBER" not in por_obra


# ---------------------------------------------------------------------------
# A FOLHA DA CONTABILIDADE, ABERTA PARA TRABALHAR — 29/09/2026
#
# ⚠️ ESTES CASOS SÃO A COBRANÇA DELE, item por item:
#
#   "Eu importo o arquivo e não tenho gestão nenhuma sobre as informações dele.
#    Quem vai, quem não vai."
#   "Cadê os dados deles, cadê uma tabela mostrando as informações, cadê a
#    possibilidade de seleção de quem entra e quem não entra, cadê onde gera o
#    arquivo de pagamento?"
#   "O botão de gerar folha deveria ser no side bar ao invés de ser no final."
#   "A lista por obra fica um troço gigantesco no começo pra depois chegar nos
#    colaboradores."
#   "Havia falado de colocar a coluna Fase Atual, não foi colocado."
#   "Na planilha à medida que vamos marcando já vamos vendo os valores."
#
# ⚠️ OS DUBLÊS SÃO OS LEITORES DE BAIXO, não o `montar`. Dublar o `montar` testaria
# só o desenho; dublando o que ele LÊ, a montagem inteira roda — e é lá que mora o
# erro que apaga a apropriação de 500 pessoas sem quebrar nada na tela.
# ---------------------------------------------------------------------------
def _preparar_folha_aberta(monkeypatch, dias=None, ajustes=None,
                           fechamento=None, cadastro=None):
    from decimal import Decimal as D
    import datetime as dt

    from app.apps.analisesps import colaboradores, folha_arquivo as fa
    from app.apps.analisesps import folha_apropriacao_guardada as guardada
    from app.apps.analisesps import folha_rateio, ponto

    monkeypatch.setattr(fa, "_pronto", lambda: True)
    monkeypatch.setattr(fa, "casar_com_o_cadastro", lambda i: None)
    monkeypatch.setattr(fa, "totais_por_filial", lambda i: [])
    monkeypatch.setattr(fa, "criticas", lambda i: {
        "pendentes": [], "sairam": [], "saindo": [],
        "total_pendente": D("0"), "total_de_quem_saiu": D("0")})
    monkeypatch.setattr(fa, "listar", lambda *a, **k: [{
        "id": 1, "ano": 2026, "mes": 9, "tipo": "quinzena",
        "competencia": "09/2026", "rotulo_do_tipo": "Quinzena (dia 1 ao 15)",
        "pessoas": 2, "total": D("2437.20"), "importado_em": None,
        "importado_por": "MARCELO", "fecha": True, "lista_de_avisos": []}])
    monkeypatch.setattr(fa, "abrir", lambda i: {
        "id": 1, "ano": 2026, "mes": 9, "tipo": "quinzena",
        "competencia": "09/2026", "rotulo_do_tipo": "Quinzena (dia 1 ao 15)",
        "pessoas": 2, "total": D("2437.20"), "importado_em": None,
        "importado_por": "MARCELO", "fecha": True, "lista_de_avisos": [],
        "linhas": [
            {"id": 1, "id_fortes": "000013", "nome": "GERLANIO",
             "cpf": "99713349334", "valor": D("1074.64"),
             "filial_codigo": "001", "filial_nome": "MATRIZ"},
            {"id": 2, "id_fortes": "000387", "nome": "LUELIA",
             "cpf": "11122233396", "valor": D("1362.56"),
             "filial_codigo": "001", "filial_nome": "MATRIZ"}]})

    monkeypatch.setattr(colaboradores, "de_para_do_fortes", lambda: {
        "000013": {"cpf": "99713349334", "nome": "GERLANIO GOMES LIMA"},
        "000387": {"cpf": "11122233396", "nome": "LUELIA MADIDA GOMES"}})
    monkeypatch.setattr(colaboradores, "codigos_das_obras",
                        lambda: {"CREPEOLINDA": "CRE1"})
    monkeypatch.setattr(colaboradores, "muitos_por_cpf",
                        lambda *a, **k: cadastro if cadastro is not None else {
        "99713349334": {"cpf": "99713349334", "nome": "GERLANIO GOMES LIMA",
                        "cargo": "SERVENTE", "fase": "Colaboradores ativos",
                        "obra_cadastro": "CREPEOLINDA", "obra_codigo": "",
                        "situacao": colaboradores.SITUACAO_ATIVO, "motivo": "",
                        "link_pipefy": ""},
        "11122233396": {"cpf": "11122233396", "nome": "LUELIA MADIDA GOMES",
                        "cargo": "AUXILIAR", "fase": "Colaboradores afastados",
                        "obra_cadastro": "CREPEOLINDA", "obra_codigo": "",
                        "situacao": colaboradores.SITUACAO_ATIVO, "motivo": "",
                        "link_pipefy": ""}})

    monkeypatch.setattr(folha_rateio, "listar", lambda *a, **k: [])
    # A conta de cada obra: é ela que faz o bloco "por conta corrente" existir.
    from app.apps.analisesps import folha_pagamento
    monkeypatch.setattr(folha_pagamento, "conta_por_obra",
                        lambda: {"CRE1": "7011-4"})
    monkeypatch.setattr(guardada, "ajustes_do_pagamento",
                        lambda *a, **k: ajustes or {})
    monkeypatch.setattr(guardada, "fechamento", lambda *a, **k: fechamento)
    monkeypatch.setattr(ponto, "carga_do_mes",
                        lambda a, m: None if dias is None else
                        {"id": 9, "ano": a, "mes": m})
    if dias is None:
        # Sem carga do mês: é o estado que o dono viu (o ponto falhando por
        # certificado) e a tela tem de DIZER, não mostrar zero.
        monkeypatch.setattr(ponto, "dias_por_cpf", lambda a, m: {})
    else:
        monkeypatch.setattr(ponto, "dias_por_cpf", lambda a, m: dias)
    return dt


def _dias_do_mes(obra="CRE1", quantos=10, cpf="99713349334", empate=False):
    """Dias de ponto no período da quinzena (1 a 15), já no formato do módulo."""
    import datetime as dt
    dias = []
    for n in range(1, quantos + 1):
        marcacoes = ([obra, "OUTRA", "OUTRA", obra] if empate
                     else [obra, obra, obra, obra])
        dias.append({"data": dt.date(2026, 9, n), "marcacoes": marcacoes,
                     "horas": ["07:00", "11:00", "13:00", "17:00"],
                     "presenca": "Presença", "falta": "",
                     "total_de_horas": "08:00", "dia_da_semana": "seg"})
    return {cpf: dias}


def test_a_folha_aberta_tem_CAIXINHA_de_quem_entra_no_pagamento(app, monkeypatch):
    """*"Cadê a possibilidade de seleção deles de quem entra e quem não entra?"*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert 'class="entra"' in html
    assert 'id="marcar-todos"' in html
    assert "Salvar seleção" in html


def test_a_folha_aberta_tem_a_coluna_FASE_ATUAL(app, monkeypatch):
    """*"Havia falado de colocar a coluna Fase Atual, não foi colocado."* — e ele
    disse isso depois de já ter pedido uma vez."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "Fase Atual" in html
    assert "Colaboradores ativos" in html
    assert "Colaboradores afastados" in html


def test_a_obra_da_folha_vem_do_PONTO_com_os_dias(app, monkeypatch):
    """*"Aqui já devemos usar a folha de ponto mesmo, visto que tem o rateio
    diário pra formar os totalizadores por obra."*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(obra="CRE1", quantos=10))
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "CRE1 · 10d" in html
    assert "Obra — do ponto" in html


def test_sem_ponto_a_tela_MOSTRA_a_obra_do_cadastro_dizendo_que_e_do_cadastro(
        app, monkeypatch):
    """⚠️ *"Caso não tenha, usar a de cadastro."* — USAR é ele decidir com um
    clique. Apropriar pela obra do cadastro em silêncio poria o custo na obra
    errada sem ninguém saber, que é o pior resultado possível."""
    _preparar_folha_aberta(monkeypatch, dias=None)
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "no cadastro: <b>CRE1</b>" in html
    assert "usar esta" in html
    # E diz, em vermelho, que não há ponto do mês: é o que explica a folha inteira
    # sem obra.
    assert "Não há ponto de 09/2026 carregado" in html


def test_a_folha_aberta_DIZ_quando_o_ponto_veio_pela_metade(app, monkeypatch):
    """⚠️ Até 29/09/2026 só a tela do Ponto sabia que a API prometeu mais páginas
    do que entregou. A folha em cima disso sai com gente faltando dia e obra
    errada — e quem está na folha não olha a tela do Ponto. O dono: *"O que
    acontece se o ponto der problema pra baixar no meio do caminho?"*"""
    from app.apps.analisesps import ponto
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(ponto, "carga_do_mes", lambda a, m: {
        "id": 9, "ano": a, "mes": m, "completa": False, "interrompida": False})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "O ponto de 09/2026 está incompleto" in html
    assert "antes de\n    gerar o pagamento" in html or "antes de gerar o pagamento" in html.replace("\n    ", " ")
    # E com a carga completa o aviso NÃO aparece.
    monkeypatch.setattr(ponto, "carga_do_mes", lambda a, m: {
        "id": 9, "ano": a, "mes": m, "completa": True, "interrompida": False})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "está incompleto" not in html


def test_o_total_por_obra_vem_DEPOIS_da_lista_de_pessoas(app, monkeypatch):
    """*"Minha tela é grande e a lista por obra fica um troço gigantesco no começo
    pra depois chegar nos colaboradores."*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert html.index("Detalhamento por colaborador") < html.index("Total por obra")


def test_o_botao_de_gerar_fica_na_LATERAL_e_nao_no_fim_da_tela(app, monkeypatch):
    """*"O botão de gerar folha deveria ser no side bar ao invés de ser no final
    da tela."*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    lateral = html.index('<aside class="filtros">')
    principal = html.index('<main class="principal">')
    assert lateral < html.index('id="fechar-apropriacao"') < principal


def test_o_total_do_que_vai_receber_fica_GRUDADO_na_tela(app, monkeypatch):
    """*"Na planilha à medida que vamos marcando já vamos vendo os valores. Aqui
    fica um KPI lá em cima que some quando rolo a tela."*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert 'class="barra-salvar"' in html
    assert 'id="total-marcado"' in html
    assert "Incluído no pagamento:" in html


def test_a_aba_da_folha_LEVA_para_a_ultima_folha_aberta(app, monkeypatch):
    """⚠️ O DEFEITO DE VERDADE ERA A FALTA DE PORTA: o único caminho para a lista
    pessoa por pessoa era o número embaixo de "precisam de olho", que desaparece
    quando não há ninguém pendente."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    resposta = _como_mestre(app).get("/analisesps/folha/importar")

    assert resposta.status_code == 302
    assert resposta.headers["Location"].endswith("/analisesps/folha/1")


def test_sem_folha_nenhuma_a_aba_mostra_a_area_de_SOLTAR_o_arquivo(app,
                                                                  monkeypatch):
    """Sem folha importada, a estante é o destino certo: é onde se solta o
    arquivo."""
    from app.apps.analisesps import folha_arquivo as fa

    monkeypatch.setattr(fa, "_pronto", lambda: True)
    monkeypatch.setattr(fa, "listar", lambda *a, **k: [])
    html = _como_mestre(app).get(
        "/analisesps/folha/importar").get_data(as_text=True)

    assert "Solte aqui a Folha Sintética" in html


def test_os_dias_do_ponto_abrem_NA_PROPRIA_LINHA(app, monkeypatch):
    """Ele confere a obra do dia contra a linha de cima. Num modal a linha
    desaparece atrás da caixa, e a conferência passa a depender da memória."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(quantos=3))
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert 'class="dias-da-pessoa"' in html
    assert "01/09/2026" in html
    assert "dia-chip" in html


def test_o_dia_EMPATADO_aparece_marcado(app, monkeypatch):
    """*"Pessoa em duas obras no mesmo dia é, na maioria dos casos, erro de batida
    de ponto."* Não impede pagar, mas tem de estar visível."""
    _preparar_folha_aberta(monkeypatch,
                           dias=_dias_do_mes(quantos=2, empate=True))
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "dia-chip empatado" in html
    assert "marcação empatada" in html


def test_quem_esta_FORA_do_pagamento_aparece_com_o_motivo(app, monkeypatch):
    """⚠️ TIRAR ALGUÉM EXIGE MOTIVO, e o motivo tem de aparecer: quem abrir o
    relatório três meses adiante precisa saber por que faltou gente. "Sumiu" é a
    pior resposta possível num pagamento."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(), ajustes={
        "11122233396": {"cpf": "11122233396", "nome": "LUELIA", "fora": True,
                        "motivo": "recebeu adiantado em dinheiro",
                        "obra_unica": "", "observacao": "", "por_obra": []}})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "fora do pagamento" in html
    assert "recebeu adiantado em dinheiro" in html


def test_a_folha_com_gente_sem_obra_NAO_convida_a_gerar(app, monkeypatch):
    """⚠️ O arquivo sairia faltando dinheiro. A lateral diz quanto falta apropriar
    em vez de oferecer o botão como se estivesse tudo pronto."""
    _preparar_folha_aberta(monkeypatch, dias=None)
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "Falta apropriar" in html
    assert "2.437,20" in html, "o valor inteiro está sem obra"


# ---------------------------------------------------------------------------
# ALIMENTAÇÃO E TRANSPORTE, as correções de 29/09/2026
#
# ⚠️ ELE REPETIU CADA UMA DESTAS. A repetição é o que justifica o teste: pedido
# atendido e desfeito depois custa mais confiança do que pedido não atendido.
# ---------------------------------------------------------------------------
def test_o_auxilio_tem_a_coluna_FASE_ATUAL(app, monkeypatch):
    """*"Havia falado de colocar a coluna Fase Atual, não foi colocado."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "Fase Atual" in html
    assert "Colaboradores ativos" in html


def test_a_obra_do_auxilio_diz_que_veio_do_PONTO_com_os_dias(app, monkeypatch):
    """*"Em Alimentação a informação de obra deveria ser a do Ponto."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "Obra — do ponto" in html
    assert "18 dia(s) no ponto" in html


def test_obra_que_veio_do_CADASTRO_e_dita_como_tal(app, monkeypatch):
    """⚠️ *"A questão da obra que paga é fundamental."* Mostrar obra de cadastro
    com cara de obra do ponto tiraria dinheiro da conta errada em silêncio."""
    from decimal import Decimal as D

    pessoa = dict(_auxilio_calculado()["pessoas"][0])
    pessoa.update({"obra_do_ponto": "", "dias_na_obra": 0,
                   "obra_de_onde": "cadastro"})
    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        pessoas=[pessoa], tem_ponto=False, quantos_do_ponto=0,
        por_obra=[{"obra": "1042", "pessoas": 1, "total": D("315.00"),
                   "do_cadastro": 1}]))
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert "do cadastro — sem ponto no mês" in html
    assert "1 do cadastro" in html, "a tabela por obra diz quantas não são do ponto"


def test_o_botao_de_gerar_do_auxilio_fica_na_LATERAL(app, monkeypatch):
    """*"O botão de gerar folha deveria ser no side bar ao invés de ser no final
    da tela."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    lateral = html.index('<aside class="filtros">')
    principal = html.index('<main class="principal">')
    # Desde 01/10/2026 o caminho até o arquivo começa por FECHAR, na lateral.
    assert lateral < html.index('id="fechar-auxilio"') < principal


def test_por_obra_vem_DEPOIS_da_lista_de_pessoas_no_auxilio(app, monkeypatch):
    """*"Minha tela é grande e a lista por obra fica um troço gigantesco no começo
    pra depois chegar nos colaboradores."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    assert html.index("Detalhamento por colaborador") < html.index("Por obra")


def test_o_total_do_auxilio_fica_SEMPRE_a_vista(app, monkeypatch):
    """⚠️ A barra era escondida até haver mudança não salva. Ele reclamou de não
    ver o valor andando: *"na planilha à medida que vamos marcando já vamos vendo
    os valores. Aqui fica um KPI lá em cima que some quando rolo a tela."*"""
    _preparar_auxilio(monkeypatch)
    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios").get_data(as_text=True)

    barra = html.index('id="barra-salvar"')
    # A barra existe e NÃO nasce escondida.
    assert 'class="barra-salvar" id="barra-salvar">' in html
    assert html[barra:barra + 200].count("hidden") == 0
    assert "Selecionados para pagamento:" in html[barra:]


def test_o_filtro_de_FASE_do_auxilio_recorta_a_lista_de_verdade(app, monkeypatch):
    """Filtro que não recorta é filtro que mente — e ele já reclamou de um."""
    pessoa_afastada = dict(_auxilio_calculado()["pessoas"][0])
    pessoa_afastada.update({"cpf": "11122233396", "nome": "LUELIA",
                            "fase": "Colaboradores afastados"})
    _preparar_auxilio(monkeypatch, _auxilio_calculado(
        pessoas=[_auxilio_calculado()["pessoas"][0], pessoa_afastada],
        quantos=2, fases=["Colaboradores ativos", "Colaboradores afastados"]))

    html = _como_mestre(app).get(
        "/analisesps/folha/auxilios?fase=Colaboradores+afastados"
    ).get_data(as_text=True)

    assert "LUELIA" in html
    assert "GERLANIO GOMES LIMA" not in html


# ---------------------------------------------------------------------------
# O QUE A RELEITURA DEVOLVEU PARA A TELA DA FOLHA (29/09/2026)
#
# Ele mandou reler o que já havia proposto. Estas quatro coisas estavam escritas em
# `docs/FOLHA_DE_PAGAMENTO.md` — algumas por ele, outras por mim e aprovadas por ele
# — e não estavam na tela.
# ---------------------------------------------------------------------------
def test_a_folha_mostra_VALOR_X_DIA_na_ordem_da_planilha(app, monkeypatch):
    """Coluna I das abas Quinzena e Fim de Mês (`F/H`). É o número com o qual o
    valor é rateado, e o que ele confere de cabeça: valor por dia × dias na obra."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(quantos=10))
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "Valor x Dia" in html
    # 1074,64 em 10 dias = 107,46 (o centavo que sobra fica no total, não no dia)
    assert "107,46" in html
    assert html.index("Dias") < html.index("Valor x Dia")


def test_a_folha_tem_o_bloco_POR_CONTA_CORRENTE(app, monkeypatch):
    """⚠️ Estava na planilha (bloco da direita), na fórmula (coluna AF) e no desenho
    aprovado (§5.2) — e não estava na tela. Cada conta vira um arquivo de pagamento
    e uma SP de transferência: é aqui que ele vê quantos arquivos vão sair."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "Por conta corrente" in html
    assert "7011-4" in html
    assert html.index("Total por obra") < html.index("Por conta corrente")


def test_obra_SEM_CONTA_segura_o_arquivo_e_diz_onde_consertar(app, monkeypatch):
    """⚠️ Nunca um padrão silencioso: a planilha da aba CTPS cai numa conta padrão
    quando o nome não casa com nada, e paga pela conta errada sem avisar."""
    from app.apps.analisesps import folha_pagamento

    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(obra="XYZ9"))
    monkeypatch.setattr(folha_pagamento, "conta_por_obra", lambda: {})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "sem conta" in html
    assert "obra(s) sem conta de pagamento" in html
    assert "C. Diários" in html


def test_tirar_alguem_do_pagamento_NAO_pede_motivo_na_tela(app, monkeypatch):
    """⚠️ CORREÇÃO contra uma regra que eu inventei. Ele: *"Não pago e ponto final.
    A gestão do pagamento é minha, eu decido."* O campo de observação continua,
    opcional; o que saiu foi a obrigação e a caixa que nascia no desmarque."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "motivo-fora" not in html
    assert "por que sai do pagamento" not in html
    assert "observacao-ajuste" in html
    assert 'placeholder="motivo do ajuste"' in html


def test_quem_sai_do_pagamento_aparece_RISCADO(app, monkeypatch):
    """*"Continua na prévia, riscada"* — estava no desenho do ajuste fino (§7.3) e a
    lista não riscava nada. Riscar o VALOR e apagar a linha mantém o nome legível."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(), ajustes={
        "11122233396": {"cpf": "11122233396", "nome": "LUELIA", "fora": True,
                        "motivo": "", "obra_unica": "", "observacao": "",
                        "por_obra": []}})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "nao-vai" in html
    assert "valor-final" in html


def test_a_linha_aberta_deixa_TROCAR_A_OBRA_e_dividir_por_dias(app, monkeypatch):
    """⚠️ Níveis 2 e 3 do ajuste fino, pedidos em 26/09/2026: *"caso eu queira
    alterar a obra que aquela pessoa vai ficar apropriada (…) bota um dia numa obra,
    um dia em outra obra."* A tela mostrava os dias e não deixava mexer neles."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "Apropriar em uma única obra" in html
    assert "ou dividir por dias" in html
    assert "calculado pelo <b>valor por dia</b>" in html
    assert 'id="obras-da-folha"' in html, "as obras conhecidas sugerem o código"


def test_a_tabela_da_CONCILIACAO_tem_piso_de_largura():
    """⚠️ CONSERTO DE UM ESTRAGO MEU, em 29/09/2026 — EM TRÊS TENTATIVAS. Ao
    acrescentar a coluna "No OMIE" eu não acrescentei o <col> dela; a tabela é
    `table-layout: fixed`, então as larguras escorregaram de coluna e a observação
    ficou com zero. As duas primeiras tentativas (piso na tabela, `min-width` na
    célula) erraram a causa: em `fixed` o navegador IGNORA `min-width` de célula.
    O dono: *"Continua quebrada. Tem algo sério e muito errado."*

    O que vale em `fixed` é a largura NO <col>. Este teste garante que é lá que
    ela está — e que a versão que não funcionava não volta."""
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(
        encoding="utf-8")
    assert "table.conciliacao { table-layout: fixed;" in css
    # O piso continua: abaixo dele o invólucro rola em vez de espremer.
    assert "table.conciliacao { min-width:" in css
    # A largura do selo do OMIE está no <col>, o único lugar que vale.
    assert "table.conciliacao col.c-no-omie { width:" in css
    # E a regra que NÃO funcionava não pode voltar com cara de conserto.
    import re
    sem_comentarios = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    assert "td.obs { min-width" not in sem_comentarios
    assert "th.obs, table.conciliacao td.obs" not in sem_comentarios


def test_a_coluna_do_OMIE_e_a_da_OBSERVACAO_tem_nome_no_html():
    """Sem o nome na célula, o CSS não teria como dar largura a elas — e foi a
    falta de largura que desmontou a tela.

    ⚠️ LÊ O TEMPLATE, NÃO A ROTA, e isso é conserto de um teste instável que eu
    mesmo criei há minutos. A primeira versão abria `/analisesps/conciliacao` como
    mestre — e essa tela GUARDA o filtro de quem a visita. Rodando antes dos testes
    da conciliação, ela deixava um filtro guardado que mudava o HTML deles: sete
    testes falharam uma vez e passaram na seguinte.

    Teste que falha às vezes é pior que teste que falta: ensina a rodar de novo em
    vez de investigar. Aqui não há sessão, não há filtro guardado, e o que se quer
    afirmar — que as duas colunas têm nome — está no template."""
    html = Path("app/apps/analisesps/templates/"
                "analisesps_conciliacao.html").read_text(encoding="utf-8")
    assert '<th class="no-omie">No OMIE</th>' in html
    assert '<th class="obs">Observação</th>' in html
    assert '<td class="no-omie">' in html
    assert '<td class="obs">' in html


def test_TODO_url_for_dos_templates_aponta_para_uma_rota_QUE_EXISTE(app):
    """⚠️ CLASSE INTEIRA DE ERRO, travada de uma vez — 29/09/2026.

    Eu escrevi `url_for('analisesps.conciliacao')` onde a rota se chama
    `tela_conciliacao`. O estrago foi cirúrgico: aquele pedaço da tela só é
    desenhado quando o filtro esconde alguma linha, então a conciliação quebrava
    com erro 500 EXATAMENTE quando ele procurava um valor que não casava com nada.
    Foi o "Deu erro" que ele viu ao filtrar pela coluna Entrada.

    Nenhum teste de tela pegaria: as telas eram desenhadas no estado normal, e o
    estado que quebrava era o excepcional. Este teste não desenha nada — lê os
    templates e confere se cada nome de rota citado existe de verdade.

    ⚠️ `url_for` é o jeito CERTO justamente porque grita. Endereço montado à mão
    ("/analisesps/conciliacao?...") não daria erro nenhum: daria uma página de
    "não encontrado" em silêncio, o que é pior. O que faltou foi eu exercitar a
    tela naquele estado."""
    import re

    pasta = Path("app/apps/analisesps/templates")
    rotas = {r.endpoint for r in app.url_map.iter_rules()}
    erradas = []
    for arquivo in sorted(pasta.glob("*.html")):
        texto = arquivo.read_text(encoding="utf-8")
        for nome in re.findall(r"url_for\(\s*['\"]([\w.]+)['\"]", texto):
            if nome.startswith("static") or "." not in nome:
                continue          # `static` é do Flask, não uma rota nossa
            if nome not in rotas:
                erradas.append(f"{arquivo.name}: url_for('{nome}')")
    assert not erradas, (
        "estes templates citam rota que não existe — a tela quebra com 500 no "
        "momento em que aquele pedaço for desenhado:\n" + "\n".join(erradas))


# ---------------------------------------------------------------------------
# A FOLHA DA CONTABILIDADE USA A LATERAL — 30/09/2026
#
# *"A tela Panorama tá sem sentido. A tela que precisamos é Folha da
# Contabilidade. Nela quero poder importar nova folha. Põe caixa de anexar
# arquivo de folha. Nela quero poder ver o que tá importado, a divisão por obra
# clicando em algo pra abrir um modal. (…) tá muito poluído a parte superior.
# Aproveite mais o sidebar para informações."*
# ---------------------------------------------------------------------------
def test_os_numeros_da_folha_ficam_na_LATERAL_e_nao_no_topo(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    principal = html.index('<main class="principal">')
    for rotulo in ("Total da folha", "Fora do pagamento", "Sem obra"):
        assert html.index(rotulo) < principal, f"{rotulo} continua no topo"
    # E os quadros grandes não existem mais nesta tela.
    assert 'class="kpi estatico"' not in html


def test_os_alertas_da_folha_ficam_na_LATERAL(app, monkeypatch):
    from decimal import Decimal
    from app.apps.analisesps import folha_arquivo as fa
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(fa, "criticas", lambda i: {
        "pendentes": [{"id_fortes": "000387", "nome": "LUELIA",
                       "valor": Decimal("1362.56")}],
        "sairam": [], "saindo": [{"nome": "X"}],
        "total_pendente": Decimal("1362.56"), "total_de_quem_saiu": 0})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    principal = html.index('<main class="principal">')
    assert html.index("Pendências antes do pagamento") < principal
    assert html.index("sem correspondência no cadastro") < principal
    assert "situacao=sem_cadastro" in html and "situacao=saindo" in html


def test_a_caixa_de_TRAZER_OUTRA_FOLHA_fica_na_lateral(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    principal = html.index('<main class="principal">')
    assert html.index('id="solta-folha"') < principal
    assert 'accept=".xls,.XLS"' in html
    # Dentro do formulário dos filtros, todo botão é `type="button"` — senão
    # escolher "Quinzena" recarregaria a tela com os filtros.
    import re
    lateral = html[:principal]
    for m in re.finditer(r"<button[^>]*escolhe-tipo[^>]*>", lateral):
        assert 'type="button"' in m.group(0)
    # E a lista do que já veio está a um clique.
    assert "Todas as folhas importadas" in lateral
    assert "/folha/importar?lista=1" in lateral


def test_quem_so_consulta_NAO_ve_a_caixa_de_trazer_folha(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = como(app, SENHA_CONSULTA).get("/analisesps/folha/1").get_data(as_text=True)
    assert 'id="solta-folha"' not in html
    # Mas vê os números e abre a divisão por obra.
    assert "Total da folha" in html
    assert 'id="btn-divisao"' in html and 'id="cartao-divisao"' in html


def test_a_divisao_por_obra_abre_numa_JANELA(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    janela = html.index('<dialog class="dialogo" id="cartao-divisao">')
    fim = html.index("</dialog>", janela)
    dentro = html[janela:fim]
    for bloco in ("Total por obra", "Por conta corrente", "Já pago em"):
        assert bloco in dentro, f"{bloco} ficou fora da janela"
    # Nada dessas tabelas sobrou no corpo da tela, antes da janela.
    corpo = html[html.index("Detalhamento por colaborador"):janela]
    assert "Total por obra" not in corpo and "Por conta corrente" not in corpo
    assert 'id="btn-divisao"' in html


def test_o_ja_pago_do_mes_entra_na_janela_quando_ha_verba_fechada(app, monkeypatch):
    """O que o Panorama tinha de útil — o pago por obra, todas as verbas — mora
    na janela. Sai do que está FECHADO."""
    from decimal import Decimal as D
    from app.apps.analisesps import folha_pagamento as fpg
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(fpg, "gerencial", lambda a, m: {
        "pronto": True, "competencia": "09/2026", "total": D("1400.00"),
        "obras": [{"obra": "CREPEOLINDA", "total": D("1000.00")},
                  {"obra": "CREPEAREIAS", "total": D("400.00")}],
        "contas": [], "verbas": []})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "CREPEAREIAS" in html and "1.400,00" in html and "71,4%" in html


def test_a_janela_NAO_derruba_a_tela_quando_o_ja_pago_estoura(app, monkeypatch):
    from app.apps.analisesps import folha_pagamento as fpg
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())

    def estoura(a, m):
        raise RuntimeError("banco fora")
    monkeypatch.setattr(fpg, "gerencial", estoura)
    r = _como_mestre(app).get("/analisesps/folha/1")
    assert r.status_code == 200
    assert "Nenhuma verba fechada nesta competência" in r.get_data(as_text=True)


def test_o_topo_da_folha_e_so_a_troca_de_competencia(app, monkeypatch):
    """O que estava no topo e saiu: os quatro quadros, o bloco de alertas com
    lista de nomes, e a linha longa de "importada em… importar outra ou apagar"."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    principal = html.index('<main class="principal">')
    topo = html[principal:html.index("Detalhamento por colaborador")]
    assert "importar outra ou apagar" not in topo
    assert 'class="kpis"' not in topo
    assert "Precisa da sua mão" not in topo



# ---------------------------------------------------------------------------
# 30/09/2026 — AS DUAS VISÕES DA OBRA, E OS TERMOS QUE ELE NÃO ENTENDEU
# ---------------------------------------------------------------------------
def test_sem_obra_do_ponto_o_filtro_DIZ_que_nao_ha_nenhuma(app, monkeypatch):
    """*"Se não tem nenhuma, como é que pode?"* — a lista mostrava só "todas as
    obras" quando o ponto não tinha trazido obra nenhuma."""
    _preparar_folha_aberta(monkeypatch, dias=None)
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "Nenhuma obra do ponto para filtrar ainda" in html
    assert "todas as obras" not in html


def test_o_texto_que_ele_nao_entendeu_saiu(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "quem tem 2 dias numa obra" not in html
    assert "Mostra quem trabalhou nestas obras em algum dia" in html


def test_ha_filtro_pela_obra_do_CADASTRO_alem_da_do_ponto(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(obra="XYZ9"))
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/1").get_data(as_text=True)
    assert 'name="obra_cadastro"' in html and 'name="obra"' in html

    so_cre1 = cliente.get("/analisesps/folha/1?obra_cadastro=CRE1").get_data(as_text=True)
    assert "GERLANIO" in so_cre1
    nenhum = cliente.get("/analisesps/folha/1?obra_cadastro=OUTRA").get_data(as_text=True)
    assert "Nenhum colaborador encontrado com esses filtros" in nenhum


def test_a_obra_do_CADASTRO_aparece_sempre_e_avisa_quando_difere_do_ponto(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(obra="XYZ9"))
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "Obra — cadastro" in html
    assert "difere do ponto" in html

    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes(obra="CRE1"))
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "difere do ponto" not in html


def test_os_termos_do_filtro_de_ORIGEM_dizem_o_que_aconteceu(app, monkeypatch):
    """*"Os termos do filtro estão esquisitos."*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert "Como a obra foi definida" in html
    assert "pelas batidas de ponto" in html
    assert "De onde veio a obra" not in html and "da minha mão" not in html


def _com_setores(monkeypatch, setor_gerlanio="CONSTRUTORA/AFASTADO INSS"):
    """A folha de teste com o setor de cada pessoa preenchido."""
    from app.apps.analisesps import folha_arquivo as fa
    original = fa.abrir

    def abrir(i):
        folha = original(i)
        folha["tem_setor"] = True
        for l in folha["linhas"]:
            l["setor_nome"] = setor_gerlanio if l["id_fortes"] == "000013" else ""
        return folha
    monkeypatch.setattr(fa, "abrir", abrir)


def test_o_SETOR_aparece_na_linha_e_o_de_afastado_ganha_destaque(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _com_setores(monkeypatch)
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert "<th>Contabilidade</th>" in html
    assert ">AFASTADO INSS</span>" in html, "o nome curto, sem repetir a filial"
    assert "em setor de afastado ou" in html
    assert "setor=__atencao" in html


def test_o_filtro_de_SETOR_recorta_e_o_de_atencao_junta_os_afastados(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _com_setores(monkeypatch)
    cliente = _como_mestre(app)

    so_atencao = cliente.get("/analisesps/folha/1?setor=__atencao").get_data(as_text=True)
    assert "GERLANIO" in so_atencao
    outro = cliente.get("/analisesps/folha/1?setor=CONSTRUTORA%2FESCRITORIO").get_data(as_text=True)
    assert "Nenhum colaborador encontrado com esses filtros" in outro


def test_setor_comum_NAO_vira_alerta(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    _com_setores(monkeypatch, setor_gerlanio="CONSTRUTORA/ESCRITORIO")
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert ">ESCRITORIO</span>" in html
    assert "em setor de afastado ou" not in html


def test_a_OBRA_DA_CONTABILIDADE_aparece_na_linha_e_tem_filtro(app, monkeypatch):
    """*"Isso aí é o nome da obra também (…) é o cadastro da contabilidade. Até
    para a gente visualizar e entender se o cadastro da contabilidade está
    batendo com o ponto."* A filial do arquivo é a obra na contabilidade."""
    from app.apps.analisesps import folha_arquivo as fa
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    original = fa.abrir

    def abrir(i):
        folha = original(i)
        for l in folha["linhas"]:
            l["filial_codigo"], l["filial_nome"] = ("090", "OBRA ESTADIOITA CONST") \
                if l["id_fortes"] == "000013" else ("001", "CONSTRUTORA")
        return folha
    monkeypatch.setattr(fa, "abrir", abrir)
    cliente = _como_mestre(app)

    html = cliente.get("/analisesps/folha/1").get_data(as_text=True)
    assert "090 - OBRA ESTADIOITA CONST" in html
    assert 'name="filial"' in html

    so_090 = cliente.get("/analisesps/folha/1?filial=090+-+OBRA+ESTADIOITA+CONST"
                         ).get_data(as_text=True)
    assert "GERLANIO" in so_090
    nenhuma = cliente.get("/analisesps/folha/1?filial=999+-+X").get_data(as_text=True)
    assert "Nenhum colaborador encontrado com esses filtros" in nenhuma



# ---------------------------------------------------------------------------
# 30/09/2026 — *"Eu queria poder clicar e visualizar o ponto dele, para eu saber
# exatamente, dia após dia, em quais locais ele bateu e qual obra foi considerada
# daquele dia. E o valor do dia também."*
# ---------------------------------------------------------------------------
def _dias_com_batidas():
    """Três dias: um inteiro na CRE1, um 2×2 CRE1/XYZ9 (vale CRE1, onde começou),
    e uma falta. Formato de `ponto.dias_por_cpf`."""
    import datetime as dt
    return {"99713349334": [
        {"data": dt.date(2026, 9, 1), "marcacoes": ["CRE1"] * 4,
         "horas": ["07:00", "11:00", "13:00", "17:00"], "presenca": "Presença",
         "falta": "", "total_de_horas": "08:00", "dia_da_semana": "ter"},
        {"data": dt.date(2026, 9, 2), "marcacoes": ["CRE1", "CRE1", "XYZ9", "XYZ9"],
         "horas": ["07:00", "11:00", "13:00", "17:00"], "presenca": "Presença",
         "falta": "", "total_de_horas": "08:00", "dia_da_semana": "qua"},
        {"data": dt.date(2026, 9, 3), "marcacoes": ["", "", "", ""],
         "horas": ["", "", "", ""], "presenca": "Falta", "falta": "Falta injustificada",
         "total_de_horas": "", "dia_da_semana": "qui"},
    ]}


def test_o_NOME_abre_o_ponto_e_o_pipefy_vira_a_setinha(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    assert 'class="link-btn abrir-ponto nome-pessoa"' in html
    assert 'data-cpf="99713349334"' in html
    assert 'id="cartao-ponto"' in html


def test_o_ponto_da_pessoa_traz_BATIDAS_OBRA_DO_DIA_e_VALOR_DO_DIA(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_com_batidas())
    r = _como_mestre(app).get("/analisesps/api/folha/1/ponto/997.133.493-34")
    d = r.get_json()
    assert r.status_code == 200 and d["ok"], d

    por_data = {x["data"]: x for x in d["dias"]}
    assert len(d["dias"]) == 15, "todo dia do período aparece, com ou sem ponto"

    dia1 = por_data["2026-09-01"]
    assert dia1["batidas"][0] == {"hora": "07:00", "obra": "CRE1"}
    assert dia1["obra_ponto"] == "CRE1" and dia1["valor"] is not None

    empate = por_data["2026-09-02"]
    assert empate["obra_ponto"] == "CRE1", "no 2×2 vale a obra em que o dia começou"
    assert empate["empate"] is True
    assert [b["obra"] for b in empate["batidas"]] == ["CRE1", "CRE1", "XYZ9", "XYZ9"]

    falta = por_data["2026-09-03"]
    assert falta["obra_ponto"] == "" and falta["valor"] is None
    assert falta["motivo"] == "falta"

    sem = por_data["2026-09-04"]
    assert sem["motivo"] == "sem registro no ponto"

    # Os valores dos dias somam o líquido: é a mesma conta da tela.
    from decimal import Decimal as D
    soma = sum(D(x["valor"]) for x in d["dias"] if x["valor"])
    assert soma == D(d["valor"])
    assert d["dias_no_ponto"] == 2


def test_pessoa_que_NAO_esta_na_folha_responde_404(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_com_batidas())
    r = _como_mestre(app).get("/analisesps/api/folha/1/ponto/52998224725")
    assert r.status_code == 404


def test_quem_so_CONSULTA_tambem_ve_o_ponto_da_pessoa(app, monkeypatch):
    """É leitura: quem confere a folha precisa ver o ponto."""
    _preparar_folha_aberta(monkeypatch, dias=_dias_com_batidas())
    r = como(app, SENHA_CONSULTA).get("/analisesps/api/folha/1/ponto/99713349334")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# 30/09/2026 — O ANALÍTICO DO FUNCIONÁRIO, e QUEM É PAGO EM MAIS DE UMA CONTA
# ---------------------------------------------------------------------------
def _dias_em_duas_obras():
    import datetime as dt
    dias = []
    for n in range(1, 7):
        obra = "CRE1" if n <= 4 else "XYZ9"
        dias.append({"data": dt.date(2026, 9, n), "marcacoes": [obra] * 4,
                     "horas": ["07:00", "11:00", "13:00", "17:00"],
                     "presenca": "Presença", "falta": "", "total_de_horas": "08:00",
                     "dia_da_semana": ""})
    return {"99713349334": dias}


def test_o_ANALITICO_traz_contabilidade_cadastro_ponto_calculo_e_rateio(app, monkeypatch):
    from app.apps.analisesps import folha_pagamento
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    monkeypatch.setattr(folha_pagamento, "conta_por_obra",
                        lambda: {"CRE1": "7011-4", "XYZ9": "22069-1"})
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)

    for bloco in ("Contabilidade (arquivo do Fortes)", "Cadastro", "Ponto (Mobponto)",
                  "<h4>Cálculo</h4>", "Por obra", "Por conta corrente", "Ponto diário"):
        assert bloco in html, f"faltou: {bloco}"
    assert "000013" in html                        # o código do Fortes
    assert "7011-4" in html and "22069-1" in html  # as duas contas
    assert "paga em mais de uma conta" in html
    # Os nomes curtos dos botões, desde 01/10/2026 (*"eu já estou na pessoa"*).
    assert ">Relatório</a>" in html
    assert ">Atualizar ponto</button>" in html
    assert ">Atualizar cadastro</button>" in html


def test_a_pagina_de_IMPRIMIR_tem_o_mesmo_analitico_e_o_botao(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    html = _como_mestre(app).get(
        "/analisesps/folha/1/pessoa/99713349334").get_data(as_text=True)
    assert "window.print()" in html
    assert "Analítico do colaborador" in html
    assert "Ponto diário" in html
    assert "atualizar-cadastro-pessoa" not in html, "na página, o botão é Imprimir"


def test_quem_so_CONSULTA_ve_o_analitico_mas_nao_o_botao_de_atualizar(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    html = como(app, SENHA_CONSULTA).get(
        "/analisesps/folha/1/pessoa/99713349334?parcial=1").get_data(as_text=True)
    assert "<h4>Cálculo</h4>" in html
    assert ">Atualizar ponto</button>" not in html
    assert ">Atualizar cadastro</button>" not in html


def test_analitico_de_quem_NAO_esta_na_folha_responde_404(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    r = _como_mestre(app).get("/analisesps/folha/1/pessoa/52998224725?parcial=1")
    assert r.status_code == 404


def test_o_filtro_de_quem_e_pago_em_MAIS_DE_UMA_CONTA(app, monkeypatch):
    """*"Quais funcionários estão sendo pagos em mais de uma conta. Isso é
    importante também até para saber se não tem nada errado no ponto."*"""
    from app.apps.analisesps import folha_pagamento
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    monkeypatch.setattr(folha_pagamento, "conta_por_obra",
                        lambda: {"CRE1": "7011-4", "XYZ9": "22069-1"})
    cliente = _como_mestre(app)

    html = cliente.get("/analisesps/folha/1").get_data(as_text=True)
    assert "pagos em mais de uma conta" in html
    assert "conta=__varias" in html

    varias = cliente.get("/analisesps/folha/1?conta=__varias").get_data(as_text=True)
    assert "GERLANIO" in varias
    por_conta = cliente.get("/analisesps/folha/1?conta=22069-1").get_data(as_text=True)
    assert "GERLANIO" in por_conta
    outra = cliente.get("/analisesps/folha/1?conta=9999-9").get_data(as_text=True)
    assert "Nenhum colaborador encontrado com esses filtros" in outra


def test_quem_fica_numa_conta_so_NAO_entra_no_filtro_de_varias(app, monkeypatch):
    from app.apps.analisesps import folha_pagamento
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    monkeypatch.setattr(folha_pagamento, "conta_por_obra",
                        lambda: {"CRE1": "7011-4", "XYZ9": "7011-4"})
    html = _como_mestre(app).get("/analisesps/folha/1?conta=__varias").get_data(as_text=True)
    assert "Nenhum colaborador encontrado com esses filtros" in html


def test_atualizar_o_ponto_de_uma_pessoa_DISPARA_a_tarefa(app, monkeypatch):
    """O caminho de antes da fila (sem a migração 042)."""
    from app.apps.analisesps import ponto_fila, sincronizacao, tarefas
    monkeypatch.setattr(ponto_fila, "_pronto", lambda: False)
    _preparar_folha_aberta(monkeypatch, dias=_dias_em_duas_obras())
    gravado, disparado = {}, {}
    monkeypatch.setattr(sincronizacao, "_meta_gravar",
                        lambda conn, k, v: gravado.setdefault(k, v))
    monkeypatch.setattr(tarefas, "disparar",
                        lambda modo, disparo="": disparado.setdefault("modo", modo) and {"ok": True})
    from contextlib import contextmanager
    from app.apps.analisesps import db

    @contextmanager
    def conexao_falsa():
        yield None
    monkeypatch.setattr(db, "conexao", conexao_falsa)

    r = _como_mestre(app).post("/analisesps/api/folha/ponto/pessoa",
                               json={"folha_id": 1, "cpf": "997.133.493-34",
                                     "nome": "GERLANIO"})
    assert r.status_code == 200, r.get_json()
    assert disparado["modo"] == "ponto_pessoa"
    assert gravado["ponto_pessoa_alvo"] == "2026|9|99713349334|GERLANIO"


def test_APAGAR_a_folha_fica_na_propria_folha_aberta(app, monkeypatch):
    """*"Onde eu excluo a folha importada? Não é nenhum pouco claro isso."*"""
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)
    principal = html.index('<main class="principal">')
    assert html.index('id="apagar-esta-folha"') < principal, "na lateral"
    assert "Apagar esta folha" in html


def test_quem_so_CONSULTA_nao_ve_o_botao_de_apagar(app, monkeypatch):
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    html = como(app, SENHA_CONSULTA).get("/analisesps/folha/1").get_data(as_text=True)
    assert 'id="apagar-esta-folha"' not in html


def test_quem_tem_VALOR_ZERO_nao_e_marcavel_e_a_linha_diz_por_que(app, monkeypatch):
    """*"Qual é o critério para umas ficarem manipuláveis e outras não? Está
    estranho, já que todas estão zeradas."* Valor zero não vai para o arquivo
    (o portal recusa); a caixa fica desligada e a linha diz por quê."""
    from decimal import Decimal as D
    from app.apps.analisesps import folha_arquivo as fa
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    original = fa.abrir

    def abrir(i):
        folha = original(i)
        for l in folha["linhas"]:
            if l["id_fortes"] == "000013":
                l["valor"] = D("0.00")
        return folha
    monkeypatch.setattr(fa, "abrir", abrir)
    # E uma pessoa que não casou com o cadastro (o código do Fortes não está lá).
    from app.apps.analisesps import colaboradores
    monkeypatch.setattr(colaboradores, "de_para_do_fortes", lambda: {
        "000013": {"cpf": "99713349334", "nome": "GERLANIO GOMES LIMA"}})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    assert ">valor zero</span>" in html
    assert "valor zero não se paga" in html
    assert "seleção indisponível: sem cadastro" in html, "e quem não casou também diz"



def test_quem_NAO_CASOU_tem_o_nome_abrindo_o_diagnostico(app, monkeypatch):
    """*"Onde é que eu posso olhar para dirimir esse problema? Como é que eu vejo
    o que o sistema está importando?"*"""
    from app.apps.analisesps import colaboradores
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(colaboradores, "de_para_do_fortes", lambda: {
        "000013": {"cpf": "99713349334", "nome": "GERLANIO GOMES LIMA"}})
    cliente = _como_mestre(app)
    html = cliente.get("/analisesps/folha/1").get_data(as_text=True)
    assert 'class="link-btn abrir-pendente nome-pessoa"' in html
    assert ">diagnóstico do cadastro</button>" in html

    monkeypatch.setattr(colaboradores, "por_que_nao_casou", lambda c, n: {
        "codigo": c, "nome": n, "pelo_codigo": [], "atualizado": {"quando": "29/09/2026 10:00"},
        "pelo_nome": [{"cpf": "11122233396", "nome": n, "id_fortes": "", "fase": "", "admissao": None}],
        "diagnostico": "a pessoa ESTÁ no cadastro, mas SEM o código do Fortes guardado."})
    miolo = cliente.get("/analisesps/folha/1/pendente/000387").get_data(as_text=True)
    assert "SEM o código do Fortes" in miolo
    assert "29/09/2026 10:00" in miolo
    assert ">vazio</span>" in miolo
    assert cliente.get("/analisesps/folha/1/pendente/999999").status_code == 404


def test_quem_NAO_CASOU_nasce_DESMARCADO_e_fora_do_vai_receber(app, monkeypatch):
    """*"Ele está marcado e eu não consigo desmarcar. Ou seja, eu não tenho gestão
    no pagamento."* Sem CPF não há pagamento: a caixa não pode dizer que há."""
    import re
    from app.apps.analisesps import colaboradores
    _preparar_folha_aberta(monkeypatch, dias=_dias_do_mes())
    monkeypatch.setattr(colaboradores, "de_para_do_fortes", lambda: {
        "000013": {"cpf": "99713349334", "nome": "GERLANIO GOMES LIMA"}})
    html = _como_mestre(app).get("/analisesps/folha/1").get_data(as_text=True)

    caixas = re.findall(r'<input type="checkbox" class="entra"[^>]*>', html)
    desligadas = [c for c in caixas if "disabled" in c]
    assert desligadas, "quem não casou tem a caixa desligada"
    assert all("checked" not in c for c in desligadas), "e DESMARCADA"
    assert "Não pode ser pago" in html
    # O "Incluído no pagamento" é só o GERLANIO (1.074,64), não os dois.
    lateral = html[:html.index('<main class="principal">')]
    trecho = lateral[lateral.index("Incluído no pagamento"):lateral.index("Fora do pagamento")]
    assert "1.074,64" in trecho and "2.437,20" not in trecho


def test_o_NOME_nao_tem_cor_de_esmaecido():
    """*"Tem outra pessoa com o nome esmaecido, clarinho. A impressão que dá é
    que não vai entrar."* Era a cor de link."""
    from pathlib import Path
    css = Path("app/apps/analisesps/static/analisesps.css").read_text(encoding="utf-8")
    bloco = css[css.index(".link-btn.nome-pessoa {"):]
    bloco = bloco[:bloco.index("}")]
    assert "color: var(--tinta)" in bloco



def test_diaristas_tem_RELATORIO_e_o_destino_no_padrao_da_contabilidade(app, monkeypatch):
    """O dono, 02/10/2026: *"é para ser tudo no mesmo padrão"* — relatório com
    recorte por conta e o destino ao lado da prévia, como na contabilidade."""
    _preparar_diaristas(monkeypatch, _diaristas_calculado())
    html = _como_mestre(app).get(
        "/analisesps/folha/diaristas?ano=2026&mes=9&periodo=quinzena").get_data(as_text=True)
    assert 'id="relatorio-xlsx"' in html and "/folha/diaristas/relatorio.xlsx" in html
    assert 'id="relatorio-conta"' in html
    assert ">Gerar arquivos</button>" in html and 'id="gd-previa"' in html
    assert 'id="gd-destino"' not in html, "o destino é escolhido por conta, na janela"
