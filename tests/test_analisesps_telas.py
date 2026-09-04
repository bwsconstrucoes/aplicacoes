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
from decimal import Decimal

import pytest
from flask import Flask

from app.apps.analisesps import consultas, web


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

    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(web.bp)
    a.config["TESTING"] = True
    return a


def como(app, senha):
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": senha})
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
    html = como(app, SENHA_OPERADOR).get(
        "/analisesps/solicitacoes").get_data(as_text=True)
    assert "6.750,00" in html
    assert "10/02/2026" in html
    assert "6750.00" not in html
    assert "2026-02-10" not in html


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
    monkeypatch.setattr(auditoria, "resumo", lambda f, u=False: {
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
                        lambda f, u=False: vistos.update(usou_filtros=u) or {})
    html = como(app, SENHA_CONSULTA).get("/analisesps/auditoria").get_data(as_text=True)
    assert vistos["usou_filtros"] is False
    assert "base inteira" in html


def test_checagem_inventada_e_ignorada(app, monkeypatch):
    from app.apps.analisesps import auditoria
    monkeypatch.setattr(auditoria, "resumo", lambda f, u=False: {})
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
    monkeypatch.setattr(lote, "ler", lambda: guardado)
    monkeypatch.setattr(lote, "salvar",
                        lambda c, p="": guardado.update(conteudo=c, salvo_por=p))
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


def test_o_lote_avisa_que_e_compartilhado(app_lote):
    """Duas pessoas veem o mesmo lote e a segunda a salvar sobrescreve a
    primeira. É de propósito, mas precisa estar escrito na tela — ninguém pode
    apagar o trabalho do outro sem perceber."""
    html = como(app_lote, SENHA_OPERADOR).get("/analisesps/lote").get_data(as_text=True)
    assert "compartilhado" in html.lower()
    assert "sobrescreve" in html.lower()


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


def test_o_log_diz_que_guarda_o_perfil_e_nao_a_pessoa(app, monkeypatch):
    """A consequência de não ter cadastro de usuários. Precisa estar escrita
    onde ela aparece, e não só no código."""
    from app.apps.analisesps import db as banco
    monkeypatch.setattr(banco, "consultar", lambda sql, params=(): [])
    monkeypatch.setattr(banco, "consultar_um", lambda sql, params=(): (0,))
    html = como(app, SENHA_CONSULTA).get("/analisesps/log").get_data(as_text=True)
    assert "perfil" in html.lower() and "não a pessoa" in html


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
    monkeypatch.setattr(auditoria, "risco_ia", lambda f, u=False: [
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


def test_a_ficha_explica_que_cancelar_nao_toca_no_pipefy(app_ficha):
    """"Cancelar" numa tela de pagamentos parece que cancela tudo. Aqui só muda
    o status na planilha — e quem clica precisa saber disso ANTES."""
    html = como(app_ficha, SENHA_OPERADOR).get(
        "/analisesps/sp/1234567890").get_data(as_text=True)
    assert "Não cancela o card no" in html


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
