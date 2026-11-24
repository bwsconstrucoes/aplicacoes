# -*- coding: utf-8 -*-
"""
COMPLETAR O CADASTRO DAS OBRAS PELA C. DIÁRIOS (13/11/2026).

Ele, olhando o cabeçalho do diário: *"a gente cadastrou as obras, importou lá
na planilha C. Diários, tem todas as informações das obras, mas não está
trazendo aqui para o cabeçalho do diário."*

⚑ POR QUE TANTO TESTE: isto ESCREVE NO CADASTRO DA OBRA, que é a fonte do
papel que a fiscalização assina. Trocar o objeto de um contrato por engano não
é tela torta — é documento errado assinado por terceiro.
"""
from __future__ import annotations

import io
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.importadores import obras_cdiarios as imp

banco_ = pytest.mark.banco

CABECALHO = ["Obra", "Código Primário", "Centro de Custo", "Município", "UF",
             "Valor", "Alíquota ISS", "Tributação", "CNO", "Cliente",
             "CNPJ Cliente", "Endereço Cliente", "Contrato", "Objeto",
             "Código Omie", "Empresa"]


def _planilha(linhas, cabecalho=None, aba="Centro de Custo"):
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = aba
    ws.append(cabecalho or CABECALHO)
    for l in linhas:
        ws.append(l)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _linha(**troca):
    base = {
        "Obra": "AEROARACATI", "Código Primário": "AEROARACATI",
        "Centro de Custo": "Aeroporto de Aracati", "Município": "Aracati-CE",
        "UF": "CE", "Valor": "R$ 1.234.567,89", "Alíquota ISS": "5,00%",
        "Tributação": "SERVICO", "CNO": "12.345.67890/12",
        "Cliente": "PREFEITURA DE ARACATI",
        "CNPJ Cliente": "07.954.555/0001-90",
        "Endereço Cliente": "Rua da Prefeitura, 1 — Centro",
        "Contrato": "CT 014/2025", "Objeto": "Reforma do terminal de passageiros",
        "Código Omie": "9901", "Empresa": "BWS CONSTRUÇÕES",
    }
    base.update(troca)
    return [base.get(c, "") for c in CABECALHO]


# ---------------------------------------------------------------------------
# SEM BANCO — a leitura
# ---------------------------------------------------------------------------
def test_le_a_aba_centro_de_custo_mesmo_nao_sendo_a_primeira():
    """A C. Diários tem várias abas; a que interessa é a Centro de Custo."""
    import openpyxl
    wb = openpyxl.Workbook()
    wb.active.title = "Resumo"
    wb.active.append(["qualquer coisa"])
    ws = wb.create_sheet("Centro de Custo")
    ws.append(CABECALHO)
    ws.append(_linha())
    buf = io.BytesIO()
    wb.save(buf)
    linhas = imp.ler_planilha(buf.getvalue())
    assert len(linhas) == 1
    assert linhas[0]["contrato"] == "CT 014/2025"


def test_municipio_vem_colado_com_a_uf_e_e_separado():
    """A planilha escreve "Aracati-CE"; o cadastro guarda os dois separados,
    porque é assim que o papel do diário imprime."""
    linhas = imp.ler_planilha(_planilha([_linha(**{"UF": ""})]))
    assert linhas[0]["municipio"] == "Aracati"
    assert linhas[0]["uf"] == "CE"


def test_valor_em_reais_vira_numero():
    linhas = imp.ler_planilha(_planilha([_linha()]))
    assert linhas[0]["valor_contrato"] == Decimal("1234567.89")
    assert linhas[0]["aliquota_iss"] == Decimal("5.00")


@pytest.mark.parametrize("escrito,esperado", [
    ("1.234.567,89", Decimal("1234567.89")),
    ("1,234,567.89", Decimal("1234567.89")),
    ("5", Decimal("5")),
    ("", None), ("—", None), ("não informado", None),
])
def test_numero_escrito_de_qualquer_jeito_nao_derruba_a_leitura(escrito, esperado):
    """⚑ Uma célula torta não pode derrubar a leitura de 300 obras: ela vira
    campo não preenchido, que o relatório mostra."""
    assert imp._numero(escrito) == esperado


def test_planilha_sem_a_coluna_do_codigo_e_recusada_com_o_que_tem():
    with pytest.raises(ErroValidacao) as e:
        imp.ler_planilha(_planilha([["x"]], cabecalho=["Qualquer"]))
    assert "Qualquer" in str(e.value)


def test_as_colunas_nao_aproveitadas_sao_ditas_com_o_motivo():
    """⚑ O endereço do CLIENTE não é o endereço da OBRA. Pôr a sede da
    prefeitura no campo Endereço do diário seria escrever mentira num
    documento que a fiscalização assina — então ele fica de fora, e a tela
    DIZ que ficou."""
    ignoradas = {c["coluna"]: c["motivo"]
                 for c in imp.colunas_ignoradas(_planilha([_linha()]))}
    assert "endereco cliente" in ignoradas
    assert "endereço do cliente" in ignoradas["endereco cliente"]
    assert "empresa" in ignoradas


def test_a_primeira_coluna_nao_entra_nas_ignoradas():
    """Ela É usada — por posição, como o segundo código da obra. Listá-la como
    "não aproveitada" era dizer que ignoro justamente o que acha a obra."""
    colunas = [c["coluna"] for c in imp.colunas_ignoradas(_planilha([_linha()]))]
    assert "obra" not in colunas


@pytest.mark.parametrize("campo,valor,esperado", [
    ("valor_contrato", Decimal("2500000.00"), "R$ 2.500.000,00"),
    ("valor_contrato", Decimal("0.50"), "R$ 0,50"),
    ("aliquota_iss", Decimal("5"), "5,00%"),
    ("contrato", "CT 014/2025", "CT 014/2025"),
])
def test_a_conferencia_mostra_o_valor_como_gente_le(campo, valor, esperado):
    """"2500000.00" é número de banco de dados. Quem confere 300 obras precisa
    bater o olho e reconhecer R$ 2.500.000,00."""
    assert imp._como_se_le(campo, valor) == esperado


# ---------------------------------------------------------------------------
# COM BANCO
# ---------------------------------------------------------------------------
def _obra(sessao, codigo, **campos):
    colunas = ["codigo", "nome", "status"] + list(campos)
    valores = {"codigo": codigo, "nome": codigo.title(), "status": "ATIVA",
               **campos}
    pedacos = ", ".join(f":{c}" for c in colunas)
    return sessao.execute(text(
        f"INSERT INTO obras ({', '.join(colunas)}) VALUES ({pedacos}) "
        f"RETURNING id"), valores).scalar()


@banco_
def test_simular_e_o_padrao_e_nao_grava_nada(sessao_real):
    oid = _obra(sessao_real, "CDTESTE1")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([_linha(**{"Código Primário": "CDTESTE1",
                                                      "Obra": "CDTESTE1"})]), None)
    sessao_real.flush()
    assert r["simulado"] is True
    assert r["completaria"] == 1
    assert sessao_real.execute(text("SELECT contrato FROM obras WHERE id = :i"),
                               {"i": oid}).scalar() is None


@banco_
def test_gravando_completa_o_cadastro(sessao_real):
    oid = _obra(sessao_real, "CDTESTE2")
    sessao_real.flush()
    r = imp.importar(sessao_real,
                     _planilha([_linha(**{"Código Primário": "CDTESTE2",
                                          "Obra": "CDTESTE2"})]),
                     None, simular=False)
    sessao_real.flush()
    assert r["completadas"] == 1
    linha = sessao_real.execute(text(
        "SELECT contrato, objeto, municipio, uf, cliente, cnpj_cliente, "
        "       valor_contrato, aliquota_iss, cno "
        "  FROM obras WHERE id = :i"), {"i": oid}).mappings().first()
    assert linha["contrato"] == "CT 014/2025"
    assert linha["objeto"] == "Reforma do terminal de passageiros"
    assert linha["municipio"] == "Aracati"
    assert linha["uf"] == "CE"
    assert linha["cliente"] == "PREFEITURA DE ARACATI"
    assert linha["valor_contrato"] == Decimal("1234567.89")


@banco_
def test_a_planilha_nao_cria_obra(sessao_real):
    """⚑ A C. Diários tem obras de anos. Criar cadastro a partir dela seria
    encher o sistema de duplicata para depois limpar à mão."""
    antes = sessao_real.execute(text("SELECT count(*) FROM obras")).scalar()
    r = imp.importar(sessao_real,
                     _planilha([_linha(**{"Código Primário": "NAOEXISTECD",
                                          "Obra": "NAOEXISTECD"})]),
                     None, simular=False)
    sessao_real.flush()
    assert sessao_real.execute(text("SELECT count(*) FROM obras")).scalar() == antes
    assert "NAOEXISTECD" in r["nao_encontradas"]


@banco_
def test_o_que_ja_esta_preenchido_nao_e_trocado_mas_a_divergencia_aparece(sessao_real):
    """⚑ O que alguém digitou aqui vale mais que a planilha. Mas calar a
    divergência seria pior: o relatório mostra os dois lados."""
    oid = _obra(sessao_real, "CDTESTE3", contrato="CT 999/2020")
    sessao_real.flush()
    r = imp.importar(sessao_real,
                     _planilha([_linha(**{"Código Primário": "CDTESTE3",
                                          "Obra": "CDTESTE3"})]),
                     None, simular=False)
    sessao_real.flush()
    assert sessao_real.execute(text("SELECT contrato FROM obras WHERE id = :i"),
                               {"i": oid}).scalar() == "CT 999/2020"
    choque = [c for c in r["conflitos"] if c["campo"] == "contrato"]
    assert choque and choque[0]["no_erp"] == "CT 999/2020"
    assert choque[0]["na_planilha"] == "CT 014/2025"
    assert choque[0]["regravado"] is False
    # e o resto da linha entrou do mesmo jeito
    assert sessao_real.execute(text("SELECT objeto FROM obras WHERE id = :i"),
                               {"i": oid}).scalar() is not None


@banco_
def test_regravar_troca_o_que_estava_preenchido(sessao_real):
    oid = _obra(sessao_real, "CDTESTE4", contrato="CT 999/2020")
    sessao_real.flush()
    imp.importar(sessao_real,
                 _planilha([_linha(**{"Código Primário": "CDTESTE4",
                                      "Obra": "CDTESTE4"})]),
                 None, simular=False, regravar=True)
    sessao_real.flush()
    assert sessao_real.execute(text("SELECT contrato FROM obras WHERE id = :i"),
                               {"i": oid}).scalar() == "CT 014/2025"


@banco_
def test_a_obra_casa_tambem_pelo_codigo_da_primeira_coluna(sessao_real):
    """A mesma obra tem DOIS códigos na planilha — o primário e o da coluna A.
    O card do Pipefy já trouxe ora um, ora outro (ver `emissaonf/cdiarios.py`),
    e por isso os dois valem aqui."""
    oid = _obra(sessao_real, "CDSEGUNDO5")
    sessao_real.flush()
    r = imp.importar(sessao_real,
                     _planilha([_linha(**{"Obra": "CDSEGUNDO5",
                                          "Código Primário": "OUTROCODIGO5"})]),
                     None, simular=False)
    sessao_real.flush()
    assert r["completadas"] == 1
    assert sessao_real.execute(text("SELECT contrato FROM obras WHERE id = :i"),
                               {"i": oid}).scalar() == "CT 014/2025"


@banco_
def test_o_relatorio_diz_o_que_a_planilha_nao_tem(sessao_real):
    """⚑ METADE DO VALOR DO RELATÓRIO: importar e achar que acabou é o jeito
    de o cabeçalho do diário seguir pela metade sem ninguém entender por quê.

    ⚠️ A LISTA ENCOLHEU EM 24/11/2026, e o motivo vale guardar: ela dizia que
    responsável técnico, ART, ordem de serviço e as datas de início e término
    "nenhuma planilha traz". Essa frase vinha da planilha de DIÁRIOS, onde
    essas colunas vêm vazias mesmo — nunca tinha sido conferida contra a
    C. DIÁRIOS, onde todas existem e estão preenchidas. Sobraram dois.
    """
    _obra(sessao_real, "CDTESTE6")
    sessao_real.flush()
    r = imp.importar(sessao_real,
                     _planilha([_linha(**{"Código Primário": "CDTESTE6",
                                          "Obra": "CDTESTE6"})]), None)
    campos = {f["campo"]: f for f in r["so_no_cadastro"]}
    assert set(campos) == {"crea_obra", "data_ordem_servico"}
    assert campos["crea_obra"]["obras_sem"] >= 1


@banco_
def test_obra_repetida_na_planilha_entra_uma_vez_so(sessao_real):
    """A C. Diários repete a obra em mais de uma linha (os dois códigos). A
    primeira manda; as outras não desfazem o que ela fez."""
    _obra(sessao_real, "CDTESTE7")
    sessao_real.flush()
    r = imp.importar(sessao_real, _planilha([
        _linha(**{"Código Primário": "CDTESTE7", "Obra": "CDTESTE7"}),
        _linha(**{"Código Primário": "CDTESTE7", "Obra": "CDTESTE7",
                  "Contrato": "CT OUTRO"}),
    ]), None, simular=False)
    sessao_real.flush()
    assert r["completadas"] == 1
    assert sessao_real.execute(text(
        "SELECT contrato FROM obras WHERE codigo = 'CDTESTE7'")).scalar() \
        == "CT 014/2025"


# ===========================================================================
# ESPELHAR A C. DIÁRIOS (24/11/2026)
#
# Ele: *"os dados reais e atualizados estão lá (…) qualquer outro dado ele
# deve ser atualizado para o ERP, porque de repente tem alguma coisa que a
# gente mudou, porque estava mal escrito, porque precisou alterar o endereço
# (…) lá o trabalho manual já foi feito lá. Eu quero só replicar ele pro ERP.
# Porque senão o ERP vai nascer desatualizado."*
#
# ⚠️ E O DEFEITO DE RAIZ, achado no mesmo dia: a aba se chama "C. Diários" na
# planilha de verdade, e a leitura só aceitava "Centro de Custo" — caindo na
# PRIMEIRA aba do arquivo. Lendo o arquivo inteiro baixado do Google, isso é
# a aba de clientes do OMIE. A importação lia a planilha errada e devolvia
# "nenhuma obra encontrada" sem ninguém entender por quê.
# ===========================================================================
CAB_COMPLETO = CABECALHO + [
    "Endereço", "Data de Início", "Data de Término", "Ordem de Serviço",
    "Responsável Técnico", "Fiscalização", "Data de Início Vigência",
    "Data de Término Vigência", "Nº ART", "Órgão Resumido",
    "Data Base p/ Reajuste", "Status da Obra", "Coordenadas Geográficas"]


def _linha_completa(**troca):
    base = {
        "Obra": "AEROARACATI", "Código Primário": "AEROARACATI",
        "Centro de Custo": "Aeroporto de Aracati", "Município": "Aracati-CE",
        "UF": "CE", "Valor": "R$ 1.234.567,89", "Alíquota ISS": "5,00%",
        "Tributação": "SERVICO", "CNO": "12.345.67890/12",
        "Cliente": "PREFEITURA DE ARACATI",
        "CNPJ Cliente": "07.954.555/0001-90",
        "Endereço Cliente": "Rua da Prefeitura, 1 — Centro",
        "Contrato": "CT 014/2025",
        "Objeto": "Reforma do terminal de passageiros",
        "Código Omie": "9901", "Empresa": "BWS CONSTRUÇÕES",
        "Endereço": "Av. Beira Rio, s/n — Aracati",
        "Data de Início": "12/03/2026", "Data de Término": "10/09/2026",
        "Ordem de Serviço": "OS 07/2026",
        "Responsável Técnico": "Eng. Fulano de Tal",
        "Fiscalização": "Eng. Beltrana",
        "Data de Início Vigência": "01/03/2026",
        "Data de Término Vigência": "28/02/2027",
        "Nº ART": "CE20260012345", "Órgão Resumido": "PMA",
        "Data Base p/ Reajuste": "01/01/2026",
        "Status da Obra": "Em Execução",
        "Coordenadas Geográficas": "-4,5622, -37,7700",
    }
    base.update(troca)
    return [base.get(c, "") for c in CAB_COMPLETO]


def _xlsx(linhas, aba="C. Diários"):
    import openpyxl
    wb = openpyxl.Workbook()
    # a primeira aba é OUTRA, de propósito: é assim no arquivo de verdade
    wb.active.title = "ClientesOmie"
    wb.active.append(["codigo_cliente_omie", "cnpj_cpf", "razao_social"])
    ws = wb.create_sheet(aba)
    ws.append(CAB_COMPLETO)
    for l in linhas:
        ws.append(l)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# A ABA
# ---------------------------------------------------------------------------
def test_acha_a_aba_chamada_c_diarios():
    """O nome de verdade da aba. Antes de 24/11/2026 ela não era encontrada e
    a leitura caía na primeira aba do arquivo."""
    linhas = imp.ler_planilha(_xlsx([_linha_completa()], aba="C. Diários"))
    assert len(linhas) == 1 and linhas[0]["codigo"] == "AEROARACATI"


def test_acha_a_aba_chamada_centro_de_custo():
    """O nome antigo continua valendo: planilha de quem já baixou não quebra."""
    linhas = imp.ler_planilha(_xlsx([_linha_completa()], aba="Centro de Custo"))
    assert len(linhas) == 1


def test_sem_a_aba_certa_RECUSA_em_vez_de_ler_a_primeira():
    """⚠️ O CONSERTO QUE MAIS IMPORTA. Cair na primeira aba é ler a planilha
    errada calado — e foi o que deixou o cadastro das obras pela metade."""
    with pytest.raises(ErroValidacao) as e:
        imp.ler_planilha(_xlsx([_linha_completa()], aba="Outra Coisa"))
    assert "Outra Coisa" in str(e.value) and "ClientesOmie" in str(e.value)


# ---------------------------------------------------------------------------
# OS CAMPOS NOVOS
# ---------------------------------------------------------------------------
def test_le_os_campos_que_faltavam():
    from datetime import date
    l = imp.ler_planilha(_xlsx([_linha_completa()]))[0]
    assert l["endereco"] == "Av. Beira Rio, s/n — Aracati"
    assert l["data_inicio"] == date(2026, 3, 12)
    assert l["data_termino"] == date(2026, 9, 10)
    assert l["vigencia_inicio"] == date(2026, 3, 1)
    assert l["vigencia_fim"] == date(2027, 2, 28)
    assert l["data_base_orcamento"] == date(2026, 1, 1)
    assert l["ordem_servico"] == "OS 07/2026"
    assert l["responsavel_tecnico"] == "Eng. Fulano de Tal"
    assert l["engenheiro_fiscal"] == "Eng. Beltrana"
    assert l["art_rrt"] == "CE20260012345"
    assert l["orgao_resumido"] == "PMA"


def test_a_data_e_lida_como_dia_mes_ano():
    """⚠️ "03/04/2026" é 3 de abril. Lido ao contrário, o diário imprimiria o
    prazo errado e ninguém desconfiaria."""
    from datetime import date
    l = imp.ler_planilha(_xlsx([_linha_completa(
        **{"Data de Início": "03/04/2026"})]))[0]
    assert l["data_inicio"] == date(2026, 4, 3)


def test_data_escrita_torta_vira_nada_e_nao_derruba_a_leitura():
    l = imp.ler_planilha(_xlsx([_linha_completa(
        **{"Data de Início": "qualquer coisa"})]))[0]
    assert l["data_inicio"] is None and l["codigo"] == "AEROARACATI"


def test_as_coordenadas_viram_dois_campos():
    l = imp.ler_planilha(_xlsx([_linha_completa()]))[0]
    assert l["latitude"] == Decimal("-4.5622")
    assert l["longitude"] == Decimal("-37.7700")


def test_coordenada_fora_do_planeta_e_recusada():
    """Coordenada errada põe a obra no meio do oceano no mapa."""
    l = imp.ler_planilha(_xlsx([_linha_completa(
        **{"Coordenadas Geográficas": "-999, 500"})]))[0]
    assert l["latitude"] is None and l["longitude"] is None


# ---------------------------------------------------------------------------
# A FASE — os oito valores que a planilha dele realmente usa
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("escrito,esperado", [
    ("Em Execução", "EM_EXECUCAO"),
    ("Concluída", "CONCLUIDA"),
    ("Concluída com Dívida", "CONCLUIDA_COM_DIVIDA"),
    ("Acervo Técnico", "ACERVO_TECNICO"),
    ("Distratada", "DISTRATADA"),
    ("Criação Centros", "CRIACAO"),
    ("Aguardando O.S", "AGUARDANDO_OS"),
])
def test_os_valores_de_status_da_planilha_viram_fase(escrito, esperado):
    """Os oito valores foram CONTADOS na planilha de verdade em 24/11/2026,
    não supostos. Falta só "Especial", que não tem par — ver abaixo."""
    l = imp.ler_planilha(_xlsx([_linha_completa(
        **{"Status da Obra": escrito})]))[0]
    assert l["fase"] == esperado


def test_status_sem_par_no_erp_nao_vira_fase_nenhuma():
    """⚠️ "Especial" (1 obra na planilha dele). Chutar uma fase mexeria no que
    aparece na tela do diário — a obra fica com a que já tem, e o relatório
    avisa."""
    l = imp.ler_planilha(_xlsx([_linha_completa(
        **{"Status da Obra": "Especial"})]))[0]
    assert l["fase"] is None and l["fase_texto"] == "Especial"


# ---------------------------------------------------------------------------
# COM BANCO — a fase e o espelho
# ---------------------------------------------------------------------------
@banco_
def test_a_fase_entra_pelo_servico_e_guarda_historico(sessao_real):
    """⚑ A FASE NÃO É UM UPDATE SOLTO. Mudar de fase guarda histórico, trilha
    de auditoria e as datas de conclusão e recebimento. Escrevê-la junto dos
    outros campos perderia tudo isso em 183 obras de uma vez."""
    oid = _obra(sessao_real, "CDFASE1", fase="CRIACAO")
    sessao_real.flush()
    r = imp.importar(sessao_real, _xlsx([_linha_completa(
        **{"Código Primário": "CDFASE1", "Obra": "CDFASE1",
           "Status da Obra": "Concluída"})]), None, simular=False)
    sessao_real.flush()

    assert r["fases_mudadas_total"] == 1
    linha = sessao_real.execute(text(
        "SELECT fase, data_conclusao FROM obras WHERE id = :o"),
        {"o": oid}).first()
    assert linha[0] == "CONCLUIDA" and linha[1] is not None
    assert sessao_real.execute(text(
        "SELECT count(*) FROM obra_fases WHERE obra_id = :o"),
        {"o": oid}).scalar() == 1


@banco_
def test_conferir_nao_muda_fase_nenhuma(sessao_real):
    oid = _obra(sessao_real, "CDFASE2", fase="CRIACAO")
    sessao_real.flush()
    r = imp.importar(sessao_real, _xlsx([_linha_completa(
        **{"Código Primário": "CDFASE2", "Obra": "CDFASE2",
           "Status da Obra": "Distratada"})]), None)
    sessao_real.flush()
    assert r["fases_mudadas_total"] == 1          # o relatório mostra
    assert sessao_real.execute(text("SELECT fase FROM obras WHERE id = :o"),
                               {"o": oid}).scalar() == "CRIACAO"


@banco_
def test_fase_que_ja_esta_certa_nao_vira_registro(sessao_real):
    """Rodar a sincronização duas vezes não pode encher o histórico de fase de
    toda obra com uma mudança que não houve."""
    oid = _obra(sessao_real, "CDFASE3", fase="EM_EXECUCAO")
    sessao_real.flush()
    dados = _xlsx([_linha_completa(**{"Código Primário": "CDFASE3",
                                      "Obra": "CDFASE3",
                                      "Status da Obra": "Em Execução"})])
    r = imp.importar(sessao_real, dados, None, simular=False)
    sessao_real.flush()
    assert r["fases_mudadas_total"] == 0
    assert sessao_real.execute(text(
        "SELECT count(*) FROM obra_fases WHERE obra_id = :o"),
        {"o": oid}).scalar() == 0


@banco_
def test_fase_que_o_erp_nao_conhece_e_relatada_e_nao_mexe(sessao_real):
    oid = _obra(sessao_real, "CDFASE4", fase="EM_EXECUCAO")
    sessao_real.flush()
    r = imp.importar(sessao_real, _xlsx([_linha_completa(
        **{"Código Primário": "CDFASE4", "Obra": "CDFASE4",
           "Status da Obra": "Especial"})]), None, simular=False)
    sessao_real.flush()
    assert [f["na_planilha"] for f in r["fases_desconhecidas"]] == ["Especial"]
    assert sessao_real.execute(text("SELECT fase FROM obras WHERE id = :o"),
                               {"o": oid}).scalar() == "EM_EXECUCAO"


@banco_
def test_espelhar_troca_o_que_estava_desatualizado(sessao_real):
    """Ele: *"de repente tem alguma coisa que a gente mudou, porque estava mal
    escrito, porque precisou alterar o endereço."* Com `regravar`, a planilha
    manda."""
    oid = _obra(sessao_real, "CDESP1", endereco="Endereço velho, 1",
                objeto="Objeto escrito errado")
    sessao_real.flush()
    imp.importar(sessao_real, _xlsx([_linha_completa(
        **{"Código Primário": "CDESP1", "Obra": "CDESP1",
           "Endereço": "Av. Nova, 500", "Objeto": "Objeto corrigido"})]),
        None, simular=False, regravar=True)
    sessao_real.flush()
    linha = sessao_real.execute(text(
        "SELECT endereco, objeto, codigo FROM obras WHERE id = :o"),
        {"o": oid}).first()
    assert linha[0] == "Av. Nova, 500" and linha[1] == "Objeto corrigido"
    assert linha[2] == "CDESP1", "o código da obra NUNCA é trocado"


@banco_
def test_sem_espelhar_o_cadastro_vence_e_o_relatorio_mostra(sessao_real):
    oid = _obra(sessao_real, "CDESP2", endereco="Endereço velho, 1")
    sessao_real.flush()
    r = imp.importar(sessao_real, _xlsx([_linha_completa(
        **{"Código Primário": "CDESP2", "Obra": "CDESP2",
           "Endereço": "Av. Nova, 500"})]), None, simular=False)
    sessao_real.flush()
    assert sessao_real.execute(text("SELECT endereco FROM obras WHERE id = :o"),
                               {"o": oid}).scalar() == "Endereço velho, 1"
    assert any(c["campo"] == "endereco" for c in r["conflitos"])


# ===========================================================================
# LER A PLANILHA ONLINE (24/11/2026)
#
# Ele: *"pra que eu vou fazer download de planilha se a planilha é online?
# Qual o sentido de eu ficar fazendo download para fazer importação? Por que
# tu não atualiza direto a planilha que está online?"*
#
# ⚑ NADA AQUI ENCOSTA NO GOOGLE. O que se prova é o que quebra na prática:
# que as duas fontes passam pela MESMA régua, e que a falha volta por extenso
# para a tela em vez de virar "não aconteceu nada".
# ===========================================================================
class _AbaFalsa:
    def __init__(self, titulo, linhas):
        self.title = titulo
        self._linhas = linhas

    def get_all_values(self):
        return self._linhas


class _LivroFalso:
    def __init__(self, abas):
        self._abas = abas

    def worksheets(self):
        return self._abas

    def worksheet(self, titulo):
        return next(a for a in self._abas if a.title == titulo)


def _google_falso(monkeypatch, abas=None, estoura=None):
    import app.apps.analisesps.credenciais as cred

    def _cliente():
        if estoura:
            raise RuntimeError(estoura)

        class _C:
            def open_by_key(self, _):
                return _LivroFalso(abas or [])
        return _C()

    monkeypatch.setattr(cred, "cliente", _cliente, raising=False)
    monkeypatch.setattr(cred, "com_retry", lambda fn, *a, **k: fn(),
                        raising=False)


def _grade():
    """O cabeçalho e uma linha, como a planilha devolve: lista de listas."""
    base = _linha_completa()
    return [CAB_COMPLETO, [str(c) for c in base]]


def test_online_le_a_aba_e_entende_as_linhas(monkeypatch):
    _google_falso(monkeypatch, abas=[
        _AbaFalsa("ClientesOmie", [["cnpj_cpf"], ["000"]]),
        _AbaFalsa("C. Diários", _grade())])
    r = imp.ler_online("qualquer")
    assert r["erro"] == "" and r["aba"] == "C. Diários"
    assert len(r["linhas"]) == 1
    assert r["linhas"][0]["codigo"] == "AEROARACATI"
    assert r["linhas"][0]["fase"] == "EM_EXECUCAO"


def test_online_e_arquivo_dao_exatamente_a_mesma_coisa(monkeypatch):
    """⚑ A RÉGUA É UMA SÓ. Duas fontes com duas leituras divergiriam no
    primeiro ajuste, e ninguém perceberia até um campo entrar errado."""
    _google_falso(monkeypatch, abas=[_AbaFalsa("C. Diários", _grade())])
    do_google = imp.ler_online("qualquer")["linhas"]
    do_arquivo = imp.ler_planilha(_xlsx([_linha_completa()]))
    assert do_google == do_arquivo


def test_online_sem_a_aba_certa_diz_quais_abas_viu(monkeypatch):
    _google_falso(monkeypatch, abas=[_AbaFalsa("Outra Coisa", [["x"], ["1"]])])
    r = imp.ler_online("qualquer")
    assert r["linhas"] == [] and "Outra Coisa" in r["erro"]


def test_online_devolve_o_erro_por_extenso(monkeypatch):
    """⚠️ A LIÇÃO DE 17/11/2026, que custou uma rodada dele: *"Meu, não tá
    funcionando. Você precisa me mostrar o que tá acontecendo."* Log é para
    quem abre o Render; ele abre a TELA."""
    _google_falso(monkeypatch, estoura="credencial do Google inválida")
    r = imp.ler_online("qualquer")
    assert r["linhas"] == [] and "credencial" in r["erro"]


def test_online_com_a_aba_vazia_avisa(monkeypatch):
    _google_falso(monkeypatch, abas=[_AbaFalsa("C. Diários", [CAB_COMPLETO])])
    r = imp.ler_online("qualquer")
    assert r["linhas"] == [] and "vazia" in r["erro"]


@banco_
def test_importar_aceita_as_linhas_prontas_do_online(sessao_real, monkeypatch):
    oid = _obra(sessao_real, "CDONLINE1", fase="CRIACAO")
    sessao_real.flush()
    _google_falso(monkeypatch, abas=[_AbaFalsa("C. Diários", [
        CAB_COMPLETO,
        [str(c) for c in _linha_completa(**{"Código Primário": "CDONLINE1",
                                            "Obra": "CDONLINE1",
                                            "Status da Obra": "Concluída"})]])])
    lido = imp.ler_online("qualquer")
    r = imp.importar(sessao_real, None, None, simular=False,
                     linhas=lido["linhas"])
    sessao_real.flush()
    assert r["fases_mudadas_total"] == 1
    assert sessao_real.execute(text("SELECT fase FROM obras WHERE id = :o"),
                               {"o": oid}).scalar() == "CONCLUIDA"


def test_importar_sem_arquivo_e_sem_linhas_recusa():
    with pytest.raises(ErroValidacao):
        imp.importar(None, None, None)
