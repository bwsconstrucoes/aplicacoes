# -*- coding: utf-8 -*-
"""
A IMPORTAÇÃO DOS DIÁRIOS JÁ ESCRITOS (migração 099).

Ele mandou a planilha com 1.427 diários em 10/11/2026: *"eu gostaria de
importar essa base que nós temos aí para dentro do diário de obras, porque aí
a gente poderia dar prosseguimento ao que a gente já tem."*

⚑ POR QUE TANTO TESTE PARA UMA IMPORTAÇÃO: ela escreve 1.380 documentos de uma
vez. Erro aqui não é uma tela torta — é mil e trezentos diários errados, que
alguém teria de apagar um a um.
"""
from __future__ import annotations

import io
from datetime import date

import pytest
from sqlalchemy import text

from app.apps.erp.core import diario_importacao as imp
from app.apps.erp.core.comum.auditoria import ErroValidacao

banco_ = pytest.mark.banco


# ---------------------------------------------------------------------------
# SEM BANCO — a leitura e a normalização
# ---------------------------------------------------------------------------
def _planilha(linhas, cabecalho=None):
    """Monta um .xlsx na memória, igual ao que ele manda."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(cabecalho or [
        "Concatenar", "Data do Diário", "Obra", "Tempo Manhã", "Tempo Tarde",
        "Relato Diário", "Responsável pelo Informação", "Efetivo Total",
        "Efetivo 1", "Efetivo 2", "ID"])
    for l in linhas:
        ws.append(l)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _linha(obra="ESCCYNIRA", data="05/01/2026", manha="Bom", tarde="Bom",
           relato="Concretagem da laje.", quem="FULANO - 81999999999",
           total=3, e1="1 - PEDREIRO\n2 - SERVENTE", e2=None, ident="123"):
    return [f"{data}-{obra}", data, obra, manha, tarde, relato, quem, total,
            e1, e2, ident]


def test_o_efetivo_vira_funcao_e_quantidade():
    """"1 - ENGENHEIRO CIVIL\\n17 - SERVENTE" é como a planilha guarda."""
    pessoas, sobras = imp.ler_efetivo("1 - ENGENHEIRO CIVIL\n17 - SERVENTE")
    assert pessoas == [{"funcao": "Engenheiro civil", "quantidade": 1},
                       {"funcao": "Servente", "quantidade": 17}]
    assert sobras == []


def test_a_funcao_deixa_de_ser_grito():
    """CAIXA ALTA no meio de uma tabela parece grito, e sai assim no papel."""
    pessoas, _ = imp.ler_efetivo("2 - MESTRE DE OBRAS")
    assert pessoas[0]["funcao"] == "Mestre de obras"


def test_a_mesma_funcao_nas_duas_colunas_vira_uma_linha():
    pessoas, _ = imp.ler_efetivo("2 - SERVENTE", "3 - SERVENTE")
    assert pessoas == [{"funcao": "Servente", "quantidade": 5}]


def test_linha_de_efetivo_que_nao_entendo_nao_some_em_silencio():
    """⚑ Importação que engole o que não entendeu é importação que perde dado
    sem avisar. A sobra volta para a conferência mostrar."""
    pessoas, sobras = imp.ler_efetivo("1 - PEDREIRO\nmais ou menos uns 5 caras")
    assert pessoas == [{"funcao": "Pedreiro", "quantidade": 1}]
    assert sobras == ["mais ou menos uns 5 caras"]


def test_o_tempo_e_lido_dos_seis_jeitos_que_a_planilha_escreve():
    dados = _planilha([
        _linha(manha="Bom", tarde="BOM"),
        _linha(data="06/01/2026", manha="Chuva", tarde="CHUVOSO"),
        _linha(data="07/01/2026", manha="NUBLADO", tarde="Nublado"),
    ])
    linhas = imp.ler_planilha(dados)
    assert [l["tempo_manha"] for l in linhas] == ["BOM", "CHUVOSO", "NUBLADO"]
    assert [l["tempo_tarde"] for l in linhas] == ["BOM", "CHUVOSO", "NUBLADO"]


def test_tempo_que_eu_nao_conheco_fica_vazio_em_vez_de_chutar():
    linhas = imp.ler_planilha(_planilha([_linha(manha="garoa fina")]))
    assert linhas[0]["tempo_manha"] is None


def test_a_coluna_e_achada_pelo_nome_e_nao_pela_posicao():
    """A planilha é exportada por outra ferramenta; a ordem muda sem aviso."""
    dados = _planilha(
        [["ESCCYNIRA", "05/01/2026", "Fiz a laje."]],
        cabecalho=["Obra", "Data do Diário", "Relato Diário"])
    linhas = imp.ler_planilha(dados)
    assert linhas[0]["codigo_obra"] == "ESCCYNIRA"
    assert linhas[0]["relato"] == "Fiz a laje."


def test_planilha_sem_a_coluna_da_obra_diz_o_que_achou():
    """O recado tem de ajudar quem está com o arquivo na mão."""
    dados = _planilha([["05/01/2026", "Fiz a laje."]],
                      cabecalho=["Data do Diário", "Relato Diário"])
    with pytest.raises(ErroValidacao) as e:
        imp.ler_planilha(dados)
    assert "obra" in str(e.value).lower()


def test_arquivo_que_nao_e_planilha_avisa_em_portugues():
    with pytest.raises(ErroValidacao) as e:
        imp.ler_planilha(b"isto nao e um xlsx")
    assert ".xlsx" in str(e.value)


def test_duas_linhas_do_mesmo_dia_sao_JUNTADAS_e_nao_descartadas():
    """⚑ 38 pares obra+dia vêm repetidos no arquivo dele, e são dois casos: o
    mesmo relato enviado duas vezes, e DOIS RELATOS DIFERENTES do mesmo dia.
    Escolher "o último" jogaria fora meio dia de obra sem ninguém perceber."""
    linhas = imp.ler_planilha(_planilha([
        _linha(relato="Escavação da vala.", total=8, e1="8 - SERVENTE"),
        _linha(relato="Demolição da calçada.", total=9, e1="9 - SERVENTE"),
    ]))
    juntas, n = imp.juntar_repetidos(linhas)
    assert n == 1 and len(juntas) == 1
    assert "Escavação da vala." in juntas[0]["relato"]
    assert "Demolição da calçada." in juntas[0]["relato"]
    # Do efetivo fica o MAIOR: o menor costuma ser o envio incompleto.
    assert juntas[0]["efetivo"] == [{"funcao": "Servente", "quantidade": 9}]


def test_relato_repetido_identico_nao_vira_texto_dobrado():
    linhas = imp.ler_planilha(_planilha([
        _linha(relato="Mesma coisa."), _linha(relato="Mesma coisa.")]))
    juntas, _ = imp.juntar_repetidos(linhas)
    assert juntas[0]["relato"] == "Mesma coisa."


@pytest.mark.parametrize("relato", [
    "FOLGA", '"FOLGA"', "DOMINGO", "FERIADO", "RECESSO",
    "FERIADO - NÃO HOUVE EXPEDIENTE", "FINAL DE SEMANA - NÃO HOUVE EXPEDIENTE",
    "* SEM ATIVIDADES REALIZADAS NO DIA.", "SEM EXPENDIENTE ( CARNAVAL )",
    "SABADO CONPENSADO", "sem expediente por conta da chuva",
])
def test_o_dia_sem_obra_e_reconhecido_de_qualquer_jeito_que_escrevam(relato):
    """⚑ O CASAMENTO EXATO NÃO BASTAVA. O arquivo dele escreve a mesma coisa
    de dez jeitos, erro de digitação incluído ("SEM EXPENDIENTE"). Cada um que
    escapasse viraria um dia trabalhado no papel."""
    assert imp.parece_sem_expediente(relato) is True


@pytest.mark.parametrize("relato", [
    "AGUARDANDO LICENÇA",
    "3- concretagem adiantada por causa do feriado da semana que vem",
    "1- piso intertravado  2- cobogó atrás da cozinha  3- armação do pilar",
    "", None,
])
def test_o_que_NAO_pode_virar_dia_sem_expediente(relato):
    """Duas armadilhas aqui, e as duas apagariam informação:

    1. "AGUARDANDO LICENÇA" (106 linhas no arquivo) não é folga — é obra
       PARADA esperando terceiro, que é o que sustenta pleito de prazo.
    2. Relato de trabalho de verdade onde a palavra "feriado" aparece. Por
       isso o marcador só vale em texto CURTO.
    """
    assert imp.parece_sem_expediente(relato) is False


# ---------------------------------------------------------------------------
# COM BANCO
# ---------------------------------------------------------------------------
@pytest.fixture
def dono(sessao_real):
    """Quem já tinha escrito o dia aqui, à mão, antes da importação chegar."""
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Administrativo da obra", email="adm@importacao.teste",
                ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMINISTRATIVO_OBRA,
                telefone="5585900000019")
    sessao_real.add(u)
    sessao_real.flush()
    return u


def _obra(sessao, codigo, nome=None):
    return sessao.execute(text("""
        INSERT INTO obras (codigo, nome, status)
        VALUES (:c, :n, 'ATIVA') RETURNING id"""),
        {"c": codigo, "n": nome or codigo.title()}).scalar()


@banco_
def test_a_planilha_nao_cria_obra(sessao_real):
    """⚑ Inventar obra a partir de um código encheria o cadastro de duplicata,
    e ninguém perceberia até a primeira medição sair errada."""
    antes = sessao_real.execute(text("SELECT count(*) FROM obras")).scalar()
    r = imp.importar(sessao_real, _planilha([_linha(obra="NAOEXISTE")]),
                     None, simular=False)
    sessao_real.flush()
    depois = sessao_real.execute(text("SELECT count(*) FROM obras")).scalar()
    assert antes == depois
    assert r["obras_nao_encontradas"] == ["NAOEXISTE"]
    assert r["gravados"] == 0


@banco_
def test_simular_e_o_padrao_e_nao_grava_nada(sessao_real):
    _obra(sessao_real, "ESCTESTE1")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([_linha(obra="ESCTESTE1")]), None)
    sessao_real.flush()
    assert r["simulado"] is True and r["gravaria"] == 1 and r["gravados"] == 0
    assert sessao_real.execute(text(
        "SELECT count(*) FROM diarios_obra WHERE importado_de IS NOT NULL"
    )).scalar() == 0


@banco_
def test_gravando_entra_o_diario_com_o_efetivo_e_a_procedencia(sessao_real):
    obra = _obra(sessao_real, "ESCTESTE2")
    sessao_real.flush()
    imp.importar(sessao_real, _planilha([_linha(
        obra="ESCTESTE2", relato="Concretagem da laje do bloco A.",
        quem="LUCAS ALVES - 81989819127", ident="777",
        e1="1 - ENGENHEIRO CIVIL\n17 - SERVENTE")]), None, simular=False)
    sessao_real.flush()
    d = sessao_real.execute(text("""
        SELECT relato, informado_por, importado_de, importado_id, situacao, id
          FROM diarios_obra WHERE obra_id = :o"""), {"o": obra}).mappings().one()
    assert d["relato"] == "Concretagem da laje do bloco A."
    assert d["informado_por"] == "LUCAS ALVES - 81989819127"
    assert d["importado_de"] == "planilha" and d["importado_id"] == "777"
    # ⚑ ENTRA ABERTO, não fechado: ele ainda vai querer melhorar o texto.
    assert d["situacao"] == "ABERTO"
    pessoas = sessao_real.execute(text("""
        SELECT funcao, quantidade, origem FROM diario_efetivo_extra
         WHERE diario_id = :i ORDER BY id"""), {"i": d["id"]}).mappings().all()
    assert [(p["funcao"], p["quantidade"]) for p in pessoas] == [
        ("Engenheiro civil", 1), ("Servente", 17)]
    assert {p["origem"] for p in pessoas} == {"IMPORTADO"}


@banco_
def test_rodar_o_arquivo_duas_vezes_nao_duplica(sessao_real):
    """⚑ O ACIDENTE MAIS COMUM DE IMPORTAÇÃO. Sem isto, o segundo passe
    dobraria o efetivo de 1.380 dias."""
    _obra(sessao_real, "ESCTESTE3")
    sessao_real.flush()
    dados = _planilha([_linha(obra="ESCTESTE3")])
    imp.importar(sessao_real, dados, None, simular=False)
    sessao_real.flush()
    r2 = imp.importar(sessao_real, dados, None, simular=False)
    sessao_real.flush()
    assert r2["gravados"] == 0
    assert any("já tem diário" in p["motivo"] for p in r2["pulados"])


@banco_
def test_dia_que_ja_existe_no_erp_nao_e_sobrescrito(sessao_real, dono):
    """⚑ DIÁRIO É DOCUMENTO. A planilha não manda no que o sistema já tem."""
    from app.apps.erp.core import diario as svc
    obra = _obra(sessao_real, "ESCTESTE4")
    sessao_real.flush()
    svc.salvar(sessao_real, obra, date(2026, 1, 5),
               {"relato": "O QUE A OBRA ESCREVEU AQUI."}, dono)
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([_linha(
        obra="ESCTESTE4", relato="o que vinha da planilha")]), None,
        simular=False)
    sessao_real.flush()
    assert r["gravados"] == 0
    assert sessao_real.execute(text(
        "SELECT relato FROM diarios_obra WHERE obra_id = :o"),
        {"o": obra}).scalar() == "O QUE A OBRA ESCREVEU AQUI."


@banco_
def test_o_dono_pode_apontar_a_obra_que_nao_casou(sessao_real):
    """O código da planilha não bate com nada, e ele diz qual é na tela."""
    obra = _obra(sessao_real, "ESCTESTE5", "Escola do Teste Cinco")
    sessao_real.flush()
    dados = _planilha([_linha(obra="CODIGOVELHO")])
    assert imp.importar(sessao_real, dados, None)["obras_nao_encontradas"] \
        == ["CODIGOVELHO"]
    r = imp.importar(sessao_real, dados, None, simular=False,
                     mapa_obras={"CODIGOVELHO": obra})
    sessao_real.flush()
    assert r["gravados"] == 1
    assert sessao_real.execute(text(
        "SELECT count(*) FROM diarios_obra WHERE obra_id = :o"),
        {"o": obra}).scalar() == 1


@banco_
def test_folga_vira_dia_sem_expediente_e_o_texto_continua_escrito(sessao_real):
    obra = _obra(sessao_real, "ESCTESTE6")
    sessao_real.flush()
    imp.importar(sessao_real, _planilha([
        _linha(obra="ESCTESTE6", relato="FOLGA", total=0, e1=None),
        _linha(obra="ESCTESTE6", data="06/01/2026",
               relato="AGUARDANDO LICENÇA", total=0, e1=None, ident="124"),
    ]), None, simular=False)
    sessao_real.flush()
    linhas = {str(l["data"]): l for l in sessao_real.execute(text(
        "SELECT data, relato, sem_expediente FROM diarios_obra WHERE obra_id = :o"),
        {"o": obra}).mappings().all()}
    assert linhas["2026-01-05"]["sem_expediente"] is True
    assert linhas["2026-01-05"]["relato"] == "FOLGA"
    # Obra parada esperando licença NÃO é folga.
    assert linhas["2026-01-06"]["sem_expediente"] is False


@banco_
def test_linha_sem_relato_e_devolvida_como_problema(sessao_real):
    """O diário exige relato; a importação não pode criar linha vazia que
    conte como "dia preenchido" no calendário."""
    _obra(sessao_real, "ESCTESTE7")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([
        _linha(obra="ESCTESTE7", relato="")]), None, simular=False)
    sessao_real.flush()
    assert r["gravados"] == 0
    assert r["problemas"] and r["problemas"][0]["motivo"] == "sem relato"


@banco_
def test_linha_sem_data_legivel_e_devolvida_como_problema(sessao_real):
    _obra(sessao_real, "ESCTESTE8")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([
        _linha(obra="ESCTESTE8", data="quinta que vem")]), None)
    assert r["problemas"] and "data" in r["problemas"][0]["motivo"]


@banco_
def test_o_diario_importado_abre_na_tela_com_o_efetivo_por_funcao(sessao_real):
    """O teste que fecha o ciclo: importou, e o dia abre direito."""
    from app.apps.erp.core import diario as svc
    obra = _obra(sessao_real, "ESCTESTE9")
    sessao_real.flush()
    imp.importar(sessao_real, _planilha([_linha(
        obra="ESCTESTE9", e1="1 - PEDREIRO\n17 - SERVENTE", total=18)]),
        None, simular=False)
    sessao_real.flush()
    d = svc.abrir_dia(sessao_real, obra, date(2026, 1, 5))
    assert d["existe"] is True
    assert d["diario"]["efetivo_importado"] == [
        {"funcao": "Pedreiro", "quantidade": 1},
        {"funcao": "Servente", "quantidade": 17}]
    # ⚑ E NÃO aparece como terceirizado: são coisas diferentes.
    assert d["diario"]["extras"] == []
    assert d["diario"]["informado_por"]


@banco_
def test_o_papel_do_dia_importado_nao_sai_com_zero_pessoa(sessao_real):
    """⚑ O EFETIVO IMPORTADO CONTA COMO PRÓPRIO. Sem isso o papel de 1.380
    dias sairia declarando que ninguém trabalhou."""
    from app.apps.erp.core import diario as svc
    obra = _obra(sessao_real, "ESCTESTE10")
    sessao_real.flush()
    imp.importar(sessao_real, _planilha([_linha(
        obra="ESCTESTE10", e1="1 - PEDREIRO\n17 - SERVENTE", total=18)]),
        None, simular=False)
    sessao_real.flush()
    pagina = svc.relatorio(sessao_real, obra, date(2026, 1, 5))["paginas"][0]
    assert pagina["efetivo"]["total"] == 18
    assert pagina["efetivo"]["total_proprio"] == 18
    assert pagina["efetivo"]["total_terceirizado"] == 0


@banco_
def test_a_busca_acha_o_que_foi_importado(sessao_real):
    """De nada adianta importar 1.380 diários que a busca não enxerga."""
    from app.apps.erp.core import diario as svc
    _obra(sessao_real, "ESCTESTE11")
    sessao_real.flush()
    imp.importar(sessao_real, _planilha([_linha(
        obra="ESCTESTE11", relato="Assentamento de piso intertravado.")]),
        None, simular=False)
    sessao_real.flush()
    assert svc.procurar(sessao_real, "intertravado")["achados"]


@banco_
def test_a_conferencia_mostra_a_LISTA_do_efetivo_e_nao_so_a_soma(sessao_real):
    """Ele, em 13/11/2026, olhando a conferência: *"tem 4 colunas: obra, dia,
    relato e pessoas, a quantidade. Mas não está importado aqui a listagem das
    pessoas. É apenas não está aparecendo nesse quadro de leitura, ou é porque
    não importa? Porque tem que importar."*

    Importa — função por função. Era a CONFERÊNCIA que resumia tudo num
    número, e aí ela não servia para o que existe: conferir. Quem lê "23
    pessoas" não tem como saber se a leitura entendeu "17 - SERVENTE" ou
    engoliu uma linha.
    """
    _obra(sessao_real, "ESCLISTA1")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([_linha(
        obra="ESCLISTA1", ident="L-1",
        e1="1 - ENGENHEIRO CIVIL\n2 - PEDREIRO\n17 - SERVENTE")]), None)
    linha = r["amostra"][0]
    assert linha["pessoas"] == 20
    por_funcao = {e["funcao"]: e["quantidade"] for e in linha["efetivo"]}
    # A função deixa de ser grito ("SERVENTE" vira "Servente") — regra que
    # este arquivo já guardava em outro teste.
    assert por_funcao == {"Engenheiro civil": 1, "Pedreiro": 2, "Servente": 17}


@banco_
def test_a_conferencia_mostra_a_linha_que_NAO_entendeu(sessao_real):
    """Linha fora do padrão era descartada em silêncio — e silêncio numa
    conferência é o pior resultado possível."""
    _obra(sessao_real, "ESCLISTA2")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([_linha(
        obra="ESCLISTA2", ident="L-2",
        e1="2 - PEDREIRO\ndois serventes\n17 - SERVENTE")]), None)
    linha = r["amostra"][0]
    assert "dois serventes" in linha["sobras"]
    assert linha["pessoas"] == 19          # só o que deu para contar
