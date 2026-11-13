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
    """⚑ METADE DO VALOR DO RELATÓRIO. Sem isto dá para importar e achar que o
    cabeçalho ficou completo — quando responsável técnico, CREA, ART, ordem de
    serviço e as datas continuam vazios, e é a data de início que faz o quadro
    de prazo do diário existir."""
    _obra(sessao_real, "CDTESTE6")
    sessao_real.flush()
    r = imp.importar(sessao_real,
                     _planilha([_linha(**{"Código Primário": "CDTESTE6",
                                          "Obra": "CDTESTE6"})]), None)
    campos = {f["campo"]: f for f in r["so_no_cadastro"]}
    assert "responsavel_tecnico" in campos
    assert "data_inicio" in campos
    assert campos["data_inicio"]["obras_sem"] >= 1


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
