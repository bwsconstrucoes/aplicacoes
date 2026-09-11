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
    """Entra no módulo. O NOME é obrigatório desde 04/09/2026 — ele separa o
    lote e os filtros de cada pessoa, e assina o registro de alterações."""
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": senha, "nome": nome})
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
def test_o_navegador_lembra_o_nome_mas_nunca_a_senha(app):
    """O dono perguntou se o nome ficaria gravado. Fica — NESTE navegador, e
    só o nome. A sessão continua morrendo quando o navegador fecha: é ela que
    diz que alguém digitou a senha, e isso não se lembra."""
    from app.apps.analisesps import auth as guarda

    cliente = app.test_client()
    resposta = cliente.post("/analisesps/entrar",
                            data={"senha": SENHA_OPERADOR, "nome": "MARCELO"})
    biscoitos = "; ".join(str(v) for _, v in resposta.headers)
    assert guarda.COOKIE_NOME in biscoitos, "o nome não ficou lembrado"
    assert SENHA_OPERADOR not in biscoitos, "a SENHA foi parar num cookie"

    cliente.get("/analisesps/sair")
    login = cliente.get("/analisesps/entrar").get_data(as_text=True)
    # Desde 09/09 o nome é escolhido numa LISTA, não digitado: o que o
    # navegador lembra é qual opção já vem marcada.
    assert '<option value="MARCELO"' in login, "o nome sumiu da lista"
    assert "selected" in login, "a opção lembrada não veio marcada"
    assert 'type="password"' in login, "parou de pedir a senha"


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
    assert "Maiúscula e acento não fazem diferença" in html


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
    # Os títulos ficam mesmo quando o grupo esvazia: apagá-los faria a remessa
    # perder a divisão que alguém montou.
    assert "Pagar amanhã" in guardado["conteudo"]
    assert "Semana que vem" in guardado["conteudo"]


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
    assert "Este lote é <b>seu</b>" in html
    assert "sobrescreve" in html.lower(), "tem de dizer que ninguém apaga o do outro"


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
    from app.apps.analisesps import bradesco, web
    monkeypatch.setattr(web, "_candidatas_bradesco", lambda: [])
    monkeypatch.setattr(bradesco, "cruzar_tudo", lambda raw, df, foco_agendados=True: {
        "boletos": [{"empresa": "BWS", "conta_debito": "1234", "valor": "6.750,00",
                     "id": "1384831053", "credor": "ACME", "alertas": "",
                     "codigo_barras": "34191", "diff": ""}],
        "pix": []})
    html = como(app, SENHA_CONSULTA).post(
        "/analisesps/bradesco", data={"extrato": "qualquer coisa"}).get_data(as_text=True)
    assert "1384831053" in html
    assert "6.750,00" in html


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
    assert "get_string_width" in inspect.getsource(pdf.Folha.tabela)


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
