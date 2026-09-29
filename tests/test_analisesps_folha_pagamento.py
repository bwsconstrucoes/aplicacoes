# -*- coding: utf-8 -*-
"""
Gerar o pagamento da folha — 27/09/2026, com banco de verdade.

⚠️ POR QUE COM BANCO: este módulo só faz sentido lendo o que está guardado. O que
ele promete e o dublê da suíte não alcança:

  - **só se paga apropriação FECHADA.** Se o arquivo pudesse sair de um cálculo em
    memória, recarregar o ponto depois mudaria a explicação de um dinheiro que já
    saiu;
  - **verba sem fechamento RECUSA a geração inteira.** Gerar pela metade sai com
    cara de completo, e a pessoa recebe a menos sem nada avisando;
  - **a conta vem da OBRA**, pelo caminho obra → código → conta, que são duas
    tabelas diferentes e um `JOIN` que o dublê ignora;
  - **o log guarda o link** — é como ele baixa o arquivo depois.
"""
import pathlib
from decimal import Decimal as D

import pytest

pytestmark = pytest.mark.banco

GERLANIO = "99713349334"
ANA = "03513441363"


@pytest.fixture
def banco_pagamento(banco_analisesps):
    """O schema `analisesps` limpo para este teste.

    ⚠️ ERA UM REFAZ-TUDO: `DROP SCHEMA` mais os 36 arquivos de migração, A CADA
    TESTE. Com 727 testes de banco na suíte, isso dava ~26 mil execuções de
    arquivo SQL por rodada para construir sempre a mesma coisa — e era a maior
    conta do tempo que o dono cobrou em 29/09/2026.

    Agora o schema nasce uma vez por sessão e as tabelas são esvaziadas entre os
    testes. O isolamento é o mesmo: tabelas vazias e contadores de `id` zerados.
    Ver `banco_analisesps` no `conftest.py`."""

def _obras_com_conta():
    """A obra CREPEOLINDA paga pela conta 50024; CREPEAREIAS, pela 50025."""
    from app.apps.analisesps.db import conexao
    with conexao() as conn:
        for nome, codigo in [("CREPEOLINDA", "1"), ("CREPEAREIAS", "2"),
                             ("SEM CONTA", "3")]:
            conn.execute(
                "INSERT INTO analisesps.referencias_rateio (tipo, nome, codigo) "
                " VALUES ('obra', ?, ?)", (nome, codigo))
        for codigo, conta in [("1", "50024"), ("2", "50025"), ("3", "")]:
            conn.execute(
                "INSERT INTO analisesps.contas_diarios (codigo, conta_pagamento) "
                " VALUES (?, ?)", (codigo, conta))
        conn.commit()


def _fechar(verba="alimentacao", obra="CREPEOLINDA", valor="330.00"):
    from app.apps.analisesps import folha_apropriacao_guardada as ag
    ag.fechar(2026, 9, "quinzena", {
        "pessoas": [{"cpf": GERLANIO, "nome": "GERLANIO",
                     "nome_cadastro": "GERLANIO GOMES LIMA", "fora": False,
                     "por_obra": [{"obra": obra, "dias": 11, "valor": D(valor),
                                   "origem": "ponto"}]}],
        "total_da_folha": D(valor), "total_apropriado": D(valor), "fecha": True,
    }, verba=verba, quem="MARCELO")


def test_a_conta_vem_da_OBRA_pelo_caminho_de_duas_tabelas(banco_pagamento):
    """⚠️ É A OBRA QUE DEFINE DE ONDE O DINHEIRO SAI. O caminho é obra → código →
    conta, e as duas pontas vivem em tabelas diferentes."""
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    contas = fp.conta_por_obra()
    assert contas["CREPEOLINDA"] == "50024"
    assert contas["CREPEAREIAS"] == "50025"
    # ⚠️ OBRA SEM CONTA NÃO DESAPARECE: entra vazia, e o gerador transforma isso
    # numa crítica que diz onde consertar.
    assert contas["SEM CONTA"] == ""


def test_so_se_paga_apropriacao_FECHADA(banco_pagamento):
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    with pytest.raises(fp.ErroDoPagamento) as erro:
        fp.linhas_para_pagar(2026, 9, "quinzena", ["alimentacao"])
    assert "apropriação fechada" in str(erro.value)


def test_verba_sem_fechamento_RECUSA_a_geracao_inteira(banco_pagamento):
    """Gerar pela metade sai com cara de completo, e a pessoa recebe a menos."""
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="alimentacao")
    with pytest.raises(fp.ErroDoPagamento) as erro:
        fp.linhas_para_pagar(2026, 9, "quinzena", ["alimentacao", "transporte"])
    assert "Transporte" in str(erro.value)


def test_as_linhas_saem_prontas_para_o_gerador(banco_pagamento):
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar()
    linhas = fp.linhas_para_pagar(2026, 9, "quinzena", ["alimentacao"])
    assert len(linhas) == 1
    assert linhas[0]["conta"] == "50024"
    assert linhas[0]["obra"] == "CREPEOLINDA"
    assert linhas[0]["verba"] == "alimentacao"
    assert linhas[0]["valor"] == D("330.00")
    assert linhas[0]["nome"] == "GERLANIO GOMES LIMA", (
        "o nome do cadastro é o que o banco confere contra o CPF")


def test_sem_verba_nenhuma_nao_gera(banco_pagamento):
    from app.apps.analisesps import folha_pagamento as fp
    with pytest.raises(fp.ErroDoPagamento):
        fp.linhas_para_pagar(2026, 9, "quinzena", [])


def test_preparar_mostra_o_que_vai_sair_ANTES_de_sair(banco_pagamento):
    """*"Mostrar, antes de gerar, quantos arquivos vão sair e com que total cada
    um."* E nada é gravado nem sobe para o Drive."""
    from app.apps.analisesps import folha_geracao as g, folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="alimentacao", valor="330.00")
    _fechar(verba="transporte", valor="200.00")

    plano = fp.preparar(2026, 9, "quinzena", ["alimentacao", "transporte"],
                        g.BEEVALE, juntar_verbas=True)
    assert plano["resumo"]["arquivos"] == 1, "juntou as duas verbas num arquivo"
    assert plano["resumo"]["total"] == D("530.00")
    assert plano["pode_juntar"] is True
    assert fp.log() == [], "preparar não grava nada"


def test_no_somapay_a_tela_DIZ_por_que_nao_junta(banco_pagamento):
    """Em vez de deixar marcar e devolver erro depois."""
    from app.apps.analisesps import folha_geracao as g, folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="alimentacao", valor="330.00")
    _fechar(verba="transporte", valor="200.00")

    plano = fp.preparar(2026, 9, "quinzena", ["alimentacao", "transporte"],
                        g.SOMAPAY, juntar_verbas=True)
    assert plano["pode_juntar"] is False
    assert "mesmo CPF duas vezes" in plano["motivo_nao_junta"]
    assert plano["resumo"]["arquivos"] == 2, "separou sozinho"


def test_gerar_sobe_DOIS_arquivos_e_registra_no_log(banco_pagamento,
                                                    monkeypatch):
    """⚠️ SEMPRE AO MENOS DOIS: o de pagamento e o de análise. *"Tem que ter no
    mínimo o do arquivo de pagamento e um de análise da folha."*"""
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)
    subidos = []

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta,
                        **k: subidos.append((nome, len(conteudo))) or
                        {"id": f"id{len(subidos)}",
                         "link": f"https://drive/{len(subidos)}"})

    _obras_com_conta()
    _fechar()
    saida = fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE,
                     quem="MARCELO")

    assert len(saida["arquivos"]) == 2
    assert len(subidos) == 2
    nomes = [n for n, _ in subidos]
    assert any(n.startswith("BeeVale") for n in nomes)
    assert any(n.startswith("Analise da folha") for n in nomes)

    registro = fp.log()
    assert len(registro) == 2
    assert registro[0]["link"].startswith("https://drive/")
    assert {r["destino"] for r in registro} == {"beevale", "analise"}
    pagamento = [r for r in registro if r["destino"] == "beevale"][0]
    assert pagamento["conta"] == "50024"
    assert pagamento["total"] == D("330.00")
    assert pagamento["criado_por"] == "MARCELO"
    assert pagamento["rotulo_verbas"] == "Alimentação"
    assert pagamento["competencia"] == "09/2026"


def test_gerar_com_AVISO_nao_gera_sem_ele_pedir(banco_pagamento, monkeypatch):
    """O aviso existe porque o arquivo sairia errado. Mas há aviso que ele conhece
    e aceita, então existe saída — e ela é explícita."""
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)
    subidos = []

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo", lambda conteudo, nome, pasta,
                        **k: subidos.append(nome) or
                        {"id": "x", "link": "https://drive/x"})

    _obras_com_conta()
    _fechar(obra="SEM CONTA")          # obra sem conta de pagamento

    with pytest.raises(fp.ErroDoPagamento) as erro:
        fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE, quem="MARCELO")
    assert "aviso" in str(erro.value)
    assert subidos == [], "não subiu nada"
    assert fp.log() == []

    saida = fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE,
                     quem="MARCELO", forcar=True)
    assert len(saida["arquivos"]) == 2
    # ⚠️ O AVISO FICA GUARDADO NO LOG. Aviso que só existiu na tela não explica
    # diferença nenhuma três meses depois.
    pagamento = [r for r in fp.log() if r["destino"] == "beevale"][0]
    assert "C. Diários" in pagamento["avisos"]


def test_gerar_duas_vezes_deixa_DUAS_linhas_no_log(banco_pagamento, monkeypatch):
    """De propósito: apagar a primeira esconderia que houve duas — e é justamente
    isso que alguém precisa ver quando o portal recebeu dois arquivos."""
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo",
                        lambda *a, **k: {"id": "x", "link": "https://drive/x"})
    _obras_com_conta()
    _fechar()

    fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE, quem="MARCELO")
    fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE, quem="MARCELO")
    assert len(fp.log()) == 4


def test_o_card_se_amarra_DEPOIS_ao_arquivo(banco_pagamento, monkeypatch):
    """Criar card é passo separado e opcional: um erro no Pipefy não pode desfazer
    um arquivo que já está no Drive."""
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo",
                        lambda *a, **k: {"id": "x", "link": "https://drive/x"})
    _obras_com_conta()
    _fechar()
    saida = fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE,
                     quem="MARCELO")

    primeiro = saida["arquivos"][0]["id"]
    assert fp.registrar_card(primeiro, "1234", "https://pipefy/1234") is True
    registro = [r for r in fp.log() if r["id"] == primeiro][0]
    assert registro["card_pipefy"] == "1234"
    assert registro["link_card"] == "https://pipefy/1234"


def test_o_log_filtra_por_competencia(banco_pagamento, monkeypatch):
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo",
                        lambda *a, **k: {"id": "x", "link": "https://drive/x"})
    _obras_com_conta()
    _fechar()
    fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE, quem="MARCELO")

    assert len(fp.log(ano=2026, mes=9)) == 2
    assert fp.log(ano=2026, mes=10) == []


def test_o_panorama_do_log_diz_quantos_e_quando(banco_pagamento, monkeypatch):
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)

    assert fp.panorama_do_log()["arquivos"] == 0
    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("PASTA", "teste"))
    monkeypatch.setattr(drive, "subir_arquivo",
                        lambda *a, **k: {"id": "x", "link": "https://drive/x"})
    _obras_com_conta()
    _fechar()
    fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE, quem="MARCELO")

    panorama = fp.panorama_do_log()
    assert panorama["pronto"] is True
    assert panorama["arquivos"] == 2
    assert panorama["ultimo"] is not None


# ---------------------------------------------------------------------------
# O GERENCIAL — por obra, por conta e por verba
#
# *"Saber qual é o total por obra, porque isso já ajuda nessa questão do rateio."*
# ---------------------------------------------------------------------------
def test_o_gerencial_junta_TODOS_os_pagamentos_do_mes(banco_pagamento):
    """A pergunta dele é do MÊS, não do pagamento: quinzena e fim de mês somam."""
    from app.apps.analisesps import folha_apropriacao_guardada as ag
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="folha", obra="CREPEOLINDA", valor="1000.00")
    ag.fechar(2026, 9, "fim_de_mes", {
        "pessoas": [{"cpf": ANA, "nome": "ANA", "nome_cadastro": "ANA S",
                     "fora": False,
                     "por_obra": [{"obra": "CREPEAREIAS", "dias": 10,
                                   "valor": D("400.00"), "origem": "ponto"}]}],
        "total_da_folha": D("400.00"), "total_apropriado": D("400.00"),
        "fecha": True}, verba="folha", quem="MARCELO")

    saida = fp.gerencial(2026, 9)
    assert saida["pronto"] is True
    assert saida["total"] == D("1400.00")
    assert [(o["obra"], o["total"]) for o in saida["obras"]] == [
        ("CREPEOLINDA", D("1000.00")), ("CREPEAREIAS", D("400.00"))]


def test_o_gerencial_agrupa_POR_CONTA_pela_obra(banco_pagamento):
    """A conta vem da obra, e é ela que diz quantos pagamentos vão sair."""
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="folha", obra="CREPEOLINDA", valor="1000.00")
    saida = fp.gerencial(2026, 9)
    assert [(c["conta"], c["total"]) for c in saida["contas"]] == [
        ("50024", D("1000.00"))]


def test_obra_SEM_CONTA_aparece_marcada_no_gerencial(banco_pagamento):
    """⚠️ Esconder aqui faria a surpresa aparecer só na hora de pagar: é justamente
    isto que trava a geração do arquivo."""
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="folha", obra="SEM CONTA", valor="500.00")
    saida = fp.gerencial(2026, 9)
    assert [c["conta"] for c in saida["contas"]] == ["(sem conta)"]


def test_o_gerencial_separa_POR_VERBA(banco_pagamento):
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="folha", valor="1000.00")
    _fechar(verba="alimentacao", valor="330.00")
    saida = fp.gerencial(2026, 9)
    assert [(v["rotulo"], v["total"]) for v in saida["verbas"]] == [
        ("Folha", D("1000.00")), ("Alimentação", D("330.00"))]


def test_o_percentual_do_gerencial_e_o_MESMO_que_vai_no_card(banco_pagamento):
    """Duas contas de percentual em dois lugares divergiriam no primeiro
    arredondamento — e o rateio do card deixaria de bater com a tela."""
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    _fechar(verba="folha", obra="CREPEOLINDA", valor="200.00")
    _fechar(verba="alimentacao", obra="CREPEAREIAS", valor="100.00")
    saida = fp.gerencial(2026, 9)
    assert sum(p["percentual"] for p in saida["percentuais"]) == D("100")


def test_mes_sem_nada_fechado_devolve_vazio_e_nao_estoura(banco_pagamento):
    from app.apps.analisesps import folha_pagamento as fp

    _obras_com_conta()
    saida = fp.gerencial(2026, 10)
    assert saida["pronto"] is True
    assert saida["obras"] == [] and saida["total"] == D("0.00")


def test_sem_a_pasta_do_drive_nao_gera_NADA(banco_pagamento, monkeypatch):
    """⚠️ Confere antes de montar: sem isso o erro só apareceria na subida do
    primeiro arquivo — depois de gerar tudo — e viraria um 500 em vez de um recado
    dizendo onde configurar."""
    from app.apps.analisesps import (beevale, drive, folha_geracao as g,
                                     folha_pagamento as fp)
    subidos = []

    monkeypatch.setattr(beevale, "pasta_do_drive", lambda: ("", ""))
    monkeypatch.setattr(drive, "subir_arquivo",
                        lambda *a, **k: subidos.append(1) or {"id": "", "link": ""})
    _obras_com_conta()
    _fechar()

    with pytest.raises(fp.ErroDoPagamento) as erro:
        fp.gerar(2026, 9, "quinzena", ["alimentacao"], g.BEEVALE, quem="MARCELO")
    assert "pasta do Drive" in str(erro.value)
    assert subidos == []
    assert fp.log() == []


# ---------------------------------------------------------------------------
# A CONTA DA OBRA — duas regras da planilha que eu não seguia (29/09/2026)
# ---------------------------------------------------------------------------
def test_a_conta_vai_ate_a_PRIMEIRA_VIRGULA():
    """⚠️ Regra da planilha, coluna AE: `REGEXEXTRACT(...;"^[^,]+")`. Uma obra pode
    ter mais de uma conta cadastrada, e vale a primeira. Sem isto o arquivo sairia
    endereçado a "7011-4, 22069-8" — uma conta que não existe."""
    from app.apps.analisesps.folha_pagamento import _primeira_conta

    assert _primeira_conta("7011-4, 22069-8") == "7011-4"
    assert _primeira_conta("  2541-0  ") == "2541-0"
    assert _primeira_conta("") == ""
    assert _primeira_conta(None) == ""


def test_a_conta_e_achavel_pelo_NOME_e_pelo_CODIGO_da_obra(monkeypatch):
    """⚠️ CONSERTO DE 29/09/2026. A apropriação identifica a obra pelo que o PONTO
    escreve na marcação, que pode ser o CÓDIGO; este dicionário era só por nome.
    Quem procurasse por código não achava conta nenhuma, e TODA linha da folha
    viraria a crítica "obra sem conta" — um arquivo inteiro barrado por um de/para
    que existia e não era consultado."""
    from app.apps.analisesps import folha_pagamento

    def falsa_consulta(sql, params=None):
        if "contas_diarios" in sql:
            return [("CRE1", "50024-0, 7011-4")]
        return [("CREPEOLINDA", "CRE1")]

    monkeypatch.setattr("app.apps.analisesps.db.consultar", falsa_consulta)
    contas = folha_pagamento.conta_por_obra()

    assert contas["CREPEOLINDA"] == "50024-0", "pelo nome, e só a primeira conta"
    assert contas["CRE1"] == "50024-0", "e pelo código também"
