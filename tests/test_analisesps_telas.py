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


def como(app, senha, nome="Marcelo"):
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
    assert "<nav" not in html and "<!doctype" not in html.lower()
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
    assert "disabled" in html

    registro["validacao"] = "Sim"
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "Agendamento bloqueado" not in html


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
                     "Tipo de Despesa", "Centro de Custo", "Valor",
                     "Status Pgt", "Status Agend", "Forma de Pgt",
                     "Conta Corrente", "Validação", "Informação p/ Pgt",
                     "Nº NF", "Data Pgt", "Comprovante", "Responsável"):
        assert esperado in rotulos, f"sumiu a coluna {esperado!r} do Streamlit"


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
    escolhidas = tabela.escolhidas(["nf", "id", "credor"])
    assert [c.chave for c in escolhidas] == ["id", "credor", "nf"]


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

    resposta = como(app, SENHA_OPERADOR, nome="Marcelo").post(
        "/analisesps/api/sem-risco", json={"ids": ["1"]})
    assert resposta.status_code == 200
    assert gravado["coluna"] == "analise_ia"
    assert gravado["valor"].startswith("SEM RISCO")
    assert "Marcelo" in gravado["valor"], "não diz quem revisou"
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
                            data={"senha": SENHA_OPERADOR, "nome": "Marcelo"})
    biscoitos = "; ".join(str(v) for _, v in resposta.headers)
    assert guarda.COOKIE_NOME in biscoitos, "o nome não ficou lembrado"
    assert SENHA_OPERADOR not in biscoitos, "a SENHA foi parar num cookie"

    cliente.get("/analisesps/sair")
    login = cliente.get("/analisesps/entrar").get_data(as_text=True)
    assert 'value="Marcelo"' in login, "o campo não veio preenchido"
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
    html = como(app, SENHA_OPERADOR, nome="Marcelo").get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "<b>Marcelo</b>" in html


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
                        lambda: [{"chave": "marcelo", "nome": "Marcelo"}])

    html = como(app_lote, SENHA_OPERADOR, nome="Marcelo Leitao").get(
        "/analisesps/lote").get_data(as_text=True)
    assert "ainda não tem lote aqui" in html
    assert "Marcelo</b>" in html
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
    cliente = como(agenda_gravavel, SENHA_OPERADOR, nome="Marcelo")
    resposta = cliente.post("/analisesps/agenda", data={
        "acao": "salvar", "titulo": "FGTS da obra", "categoria": "FGTS",
        "data_base": "2026-01-07", "recorrencia": "mensal",
        "alerta_dias_antes": "5", "responsavel": "Maria"},
        follow_redirects=True)
    assert resposta.status_code == 200

    assert len(agenda_gravavel.escrito) == 1, "não foi para a planilha"
    guardado = agenda_gravavel.escrito[0]
    assert guardado["titulo"] == "FGTS da obra"
    assert guardado["criado_por"] == "Marcelo", "não diz quem cadastrou"
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
    monkeypatch.setattr(consultas, "agregar",
                        lambda f, d, t="geral", p="tudo", limite=30: [
                            {"rotulo": "OBRA-12", "quantidade": 40,
                             "total": Decimal("500000.00")},
                            {"rotulo": "(vazio)", "quantidade": 3,
                             "total": Decimal("1200.00")}])
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


def test_a_ficha_leva_direto_ao_codigo_de_pagamento(app_ficha):
    """Quem abriu a ficha para pagar não devia ter de voltar à lista, marcar a
    mesma SP e clicar em outro lugar."""
    html = como(app_ficha, SENHA_CONSULTA).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "codigos?id=1234567890" in html


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
    monkeypatch.setattr(lote, "ler", lambda: {"conteudo": "", "salvo_por": None,
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
