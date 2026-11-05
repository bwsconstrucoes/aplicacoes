# -*- coding: utf-8 -*-
"""CARGO, CONVENÇÃO e o PISO COM HISTÓRICO (migração 085).

Pedido do dono, 05/11/2026, sobre a planilha de cargos do Pipefy: *"veja que
ela tá mal construída. Mistura Cargo e convenção. Isso é pra ser distinto. (…)
Ao mesmo tempo tem a questão salarial e as alterações salariais pra serem
gravadas."*

Os testes sem banco defendem a NORMALIZAÇÃO — que é onde o defeito nasce, e
onde eu já errei uma vez (ver `test_o_espaco_ao_redor_do_hifen_colapsa`).
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.cadastros import cargos as svc


# ---------------------------------------------------------------------------
# O CBO: cinco formatos na mesma planilha
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto, esperado", [
    ("7152-10", "715210"),          # 87 linhas assim
    ("715505.0", "715505"),         # 24 linhas: o Sheets converteu em número
    ("411005", "411005"),           # 7 linhas só com dígitos
    ("72411-0", "724110"),          # 1 linha com máscara errada
    ("", None),
    (None, None),
    ("  7152-10  ", "715210"),
])
def test_o_cbo_fica_so_com_digitos(bruto, esperado):
    assert svc.normalizar_cbo(bruto) == esperado


def test_o_cbo_sem_digito_nenhum_vira_vazio_em_vez_de_lixo():
    assert svc.normalizar_cbo("não informado") is None


@pytest.mark.parametrize("digitos, exibido", [
    ("715210", "7152-10"), ("4110050", "41100-50"), ("", ""),
])
def test_a_mascara_do_cbo_e_so_exibicao(digitos, exibido):
    assert svc.cbo_com_mascara(digitos) == exibido


def test_os_tres_formatos_do_mesmo_cbo_viram_um():
    """É o que evita PEDREIRO virar três cargos."""
    assert len({svc.normalizar_cbo(x) for x in ("7152-10", "715210", "715210.0")}) == 1


# ---------------------------------------------------------------------------
# A CONVENÇÃO: 16 na planilha, 13 de verdade
# ---------------------------------------------------------------------------
def test_o_espaco_ao_redor_do_hifen_colapsa():
    """DEFEITO QUE EU COMETI e a planilha de verdade acusou: colapsar só espaço
    repetido deixava "MT- Mato Grosso" e "MT-Mato Grosso" como duas convenções
    — e aí o mesmo cargo ganhava dois pisos, que é o defeito que a importação
    existe para consertar. Deu 15 onde devia dar 13."""
    assert svc.normalizar_nome("MT- Mato Grosso") == svc.normalizar_nome("MT-Mato Grosso")
    assert svc.normalizar_nome("SP - Feticom") == svc.normalizar_nome("SP-Feticom")


@pytest.mark.parametrize("a, b", [
    ("Sem Convenção", "SEM CONVENÇÃO"),        # caixa
    ("Sem Convenção", "Sem Convencao"),        # acento
    ("CE-Fortaleza", "ce-fortaleza"),
    ("SP-GUARUJÁ", "SP-Guarujá"),
    ("CE -  Interior", "CE-Interior"),         # espaço antes e depois
])
def test_a_mesma_convencao_escrita_diferente_e_uma_so(a, b):
    assert svc.normalizar_nome(a) == svc.normalizar_nome(b)


def test_convencoes_de_verdade_diferentes_continuam_diferentes():
    """A normalização não pode ser tão agressiva que junte o que é distinto."""
    nomes = ["CE-Fortaleza", "CE-Interior", "CE-Barbalha", "CE-Juazeiro do Norte",
             "PE-Pernambuco", "PE-Petrolina", "SP-Campinas", "SP-Feticom",
             "SP-GUARUJÁ", "MT-Mato Grosso", "MA-Imperatriz", "BA-Sobradinho",
             "Sem Convenção"]
    assert len({svc.normalizar_nome(n) for n in nomes}) == 13


# ---------------------------------------------------------------------------
# A CATEGORIA: 19 na planilha, ~13 de verdade
# ---------------------------------------------------------------------------
def test_o_sufixo_da_convencao_sai_da_categoria():
    """"PROFISSIONAL (MA)" é PROFISSIONAL — o "(MA)" repete a convenção, que já
    é a coluna ao lado. Com o sufixo, ninguém soma "quantos profissionais a
    empresa tem"."""
    assert svc.normalizar_categoria("PROFISSIONAL (MA)") == "PROFISSIONAL"
    assert svc.normalizar_categoria("SERVENTE (MA)") == "SERVENTE"
    assert svc.normalizar_categoria("PESSOAL DE APOIO ADM. (MA)") == "PESSOAL DE APOIO ADM."


@pytest.mark.parametrize("bruto", ["NÃO QUALIFICADO", "não qualificados",
                                   "Não qualificados", "nao qualificado"])
def test_as_quatro_grafias_de_nao_qualificado_viram_uma(bruto):
    assert svc.normalizar_categoria(bruto) == "NAO QUALIFICADO"


def test_categoria_vazia_continua_vazia():
    """Duas linhas da planilha não têm categoria. Inventar uma seria pior."""
    assert svc.normalizar_categoria("") is None
    assert svc.normalizar_categoria(None) is None


# ---------------------------------------------------------------------------
# O nome do cargo
# ---------------------------------------------------------------------------
def test_o_nome_do_cargo_ignora_caixa_e_acento():
    assert svc.normalizar_nome("Pedreiro") == svc.normalizar_nome("PEDREIRO")
    assert svc.normalizar_nome("ELETRICISTA") == svc.normalizar_nome("eletricista")


def test_erro_de_digitacao_NAO_e_consertado_sozinho():
    """A planilha tem ALMOXARIFADO onde queria ALMOXARIFE e PEDEEIRO onde
    queria PEDREIRO. Corrigir calado criaria cargo que ninguém pediu — vira
    aviso na tela, e o dono decide."""
    assert svc.normalizar_nome("ALMOXARIFADO") != svc.normalizar_nome("ALMOXARIFE")
    assert svc.normalizar_nome("PEDEEIRO") != svc.normalizar_nome("PEDREIRO")


# ===========================================================================
# COM BANCO: a planilha de 135 linhas virando cadastro, e o piso com história
# ===========================================================================
import datetime as dt          # noqa: E402
from decimal import Decimal    # noqa: E402

from sqlalchemy import text    # noqa: E402

from app.apps.erp.core.importadores import cargos_planilha  # noqa: E402

bancos = pytest.mark.banco


def _planilha(linhas, cabecalho=None):
    """Monta um .xlsx igual ao que o dono exportou do Pipefy."""
    import io
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.append(cabecalho or ["Cargo e Convenção:", "Cargo:", "Convenção:", "Categoria:",
                            "Salário:", "CBO", "Valor Diária", "Código", "Título", "Status"])
    for l in linhas:
        ws.append(l)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


@bancos
def test_a_planilha_separa_cargo_de_convencao(sessao_real):
    """O pedido dele, em uma frase: PEDREIRO é UM cargo com DOIS pisos."""
    conteudo = _planilha([
        ["PEDREIRO (CE-Fortaleza)", "PEDREIRO", "CE-Fortaleza", "PROFISSIONAL",
         "2230", "7152-10", "", "397631555", "PEDREIRO (CE-Fortaleza)", "Ativo"],
        ["PEDREIRO (MA-Imperatriz)", "PEDREIRO", "MA-Imperatriz", "PROFISSIONAL (MA)",
         "1608.23", "7152-10", "", "400968208", "PEDREIRO (MA-Imperatriz)", "Ativo"],
    ])
    r = cargos_planilha.importar(sessao_real, conteudo, gravar=True)
    sessao_real.flush()

    assert (r["cargos_distintos"], r["convencoes_distintas"], r["cruzamentos"]) == (1, 2, 2)
    lido = cargos_planilha.svc.listar(sessao_real)
    assert [c["nome"] for c in lido["cargos"]] == ["PEDREIRO"]
    assert {c["nome"] for c in lido["convencoes"]} == {"CE-Fortaleza", "MA-Imperatriz"}
    pisos = {c["convencao"]: c["salario_piso"] for c in lido["cruzamentos"]}
    assert pisos == {"CE-Fortaleza": 2230.0, "MA-Imperatriz": 1608.23}
    # o sufixo "(MA)" da categoria saiu
    assert {c["categoria"] for c in lido["cruzamentos"]} == {"PROFISSIONAL"}


@bancos
def test_as_grafias_da_mesma_convencao_colapsam_no_banco(sessao_real):
    conteudo = _planilha([
        ["X (MT-Mato Grosso)", "VIGIA", "MT-Mato Grosso", "", "1500", "5174-20", "", "1", "", "Ativo"],
        ["Y (MT- Mato Grosso)", "SOLDADOR", "MT- Mato Grosso", "", "1600", "7243-15", "", "2", "", "Ativo"],
    ])
    cargos_planilha.importar(sessao_real, conteudo, gravar=True)
    sessao_real.flush()
    assert len(cargos_planilha.svc.listar(sessao_real)["convencoes"]) == 1


@bancos
def test_o_mesmo_cargo_com_dois_salarios_vira_aviso_e_nao_escolha(sessao_real):
    """A planilha de verdade tem dois casos. Escolher calado faria o sistema
    afirmar um piso que ninguém conferiu."""
    conteudo = _planilha([
        ["A", "MESTRE DE OBRAS", "Sem Convenção", "SEM CATEGORIA", "2478.47", "7102-05", "", "1", "", "Ativo"],
        ["B", "MESTRE DE OBRAS", "Sem Convenção", "SEM CATEGORIA", "2940.26", "7102-05", "", "2", "", "Ativo"],
    ])
    r = cargos_planilha.importar(sessao_real, conteudo, gravar=True)
    sessao_real.flush()

    assert r["cruzamentos"] == 1
    assert any("2940.26" in a and "2478.47" in a for a in r["avisos"]), r["avisos"]
    piso = cargos_planilha.svc.listar(sessao_real)["cruzamentos"][0]["salario_piso"]
    assert piso == 2478.47            # valeu a primeira, e o aviso diz


@bancos
def test_cargo_concluido_no_pipefy_entra_inativo_e_nao_desaparece(sessao_real):
    """Há gente cadastrada no cargo. Cargo que desaparece deixa a ficha dela
    sem cargo nenhum."""
    conteudo = _planilha([
        ["A", "AUXILIAR DE CARPINTEIRO", "CE-Fortaleza", "MEIO-PROFISSIONAL",
         "1680", "715505.0", "", "402413772", "AUXILIAR DE CARPINTEIRO (CE-Fortaleza)", "Concluído"],
    ])
    r = cargos_planilha.importar(sessao_real, conteudo, gravar=True)
    sessao_real.flush()

    assert any("INATIVO" in a for a in r["avisos"])
    todos = cargos_planilha.svc.listar(sessao_real, so_ativos=False)["cruzamentos"]
    assert len(todos) == 1 and todos[0]["ativo"] is False
    assert todos[0]["cbo"] == "7155-05"          # o 715505.0 virou máscara certa


@bancos
def test_simular_nao_grava_nada(sessao_real):
    conteudo = _planilha([
        ["A", "PINTOR", "CE-Fortaleza", "PROFISSIONAL", "2230", "7166-10", "", "1", "", "Ativo"],
    ])
    r = cargos_planilha.importar(sessao_real, conteudo, gravar=False)
    sessao_real.flush()

    assert r["gravou"] is False and r["cruzamentos"] == 1
    assert cargos_planilha.svc.listar(sessao_real)["cargos"] == []


@bancos
def test_importar_duas_vezes_nao_duplica(sessao_real):
    """A planilha vai ser reimportada quando o dissídio sair."""
    conteudo = _planilha([
        ["A", "FERREIRO", "CE-Fortaleza", "PROFISSIONAL", "2230", "7153-15", "", "1", "", "Ativo"],
    ])
    cargos_planilha.importar(sessao_real, conteudo, gravar=True)
    sessao_real.flush()
    r2 = cargos_planilha.importar(sessao_real, conteudo, gravar=True)
    sessao_real.flush()

    lido = cargos_planilha.svc.listar(sessao_real)
    assert len(lido["cargos"]) == 1 and len(lido["cruzamentos"]) == 1
    assert r2["pisos_abertos"] == 0           # piso igual não abre linha nova


# ---------------------------------------------------------------------------
# O PISO COM HISTÓRIA — *"as alterações salariais pra serem gravadas"*
# ---------------------------------------------------------------------------
@bancos
def test_o_piso_novo_fecha_o_anterior_e_a_historia_fica(sessao_real):
    svc = cargos_planilha.svc
    cargo = svc.achar_ou_criar_cargo(sessao_real, "PEDREIRO", cbo="7152-10")
    conv = svc.achar_ou_criar_convencao(sessao_real, "CE-Fortaleza")
    cc = svc.cruzamento(sessao_real, cargo.id, conv.id, criar=True)
    svc.definir_piso(sessao_real, cc.id, salario="2045",
                     vigencia=dt.date(2025, 1, 1), motivo="convenção 2025")
    svc.definir_piso(sessao_real, cc.id, salario="2230",
                     vigencia=dt.date(2026, 1, 1), motivo="dissídio 2026")
    sessao_real.flush()

    h = svc.historico_do_piso(sessao_real, cc.id)
    assert [(x["salario_piso"], x["vigente"]) for x in h] == [(2230.0, True), (2045.0, False)]
    assert h[1]["vigencia_fim"] == "2026-01-01"
    assert cc.salario_piso == Decimal("2230.00")


@bancos
def test_o_piso_de_uma_data_passada_e_o_que_valia_naquele_mes(sessao_real):
    """É o que permite conferir a folha de um mês antigo. Guardar só o valor de
    hoje é apagar a prova do que foi pago."""
    svc = cargos_planilha.svc
    cargo = svc.achar_ou_criar_cargo(sessao_real, "VIGIA")
    conv = svc.achar_ou_criar_convencao(sessao_real, "CE-Fortaleza")
    cc = svc.cruzamento(sessao_real, cargo.id, conv.id, criar=True)
    svc.definir_piso(sessao_real, cc.id, salario="2045", vigencia=dt.date(2025, 1, 1))
    svc.definir_piso(sessao_real, cc.id, salario="2230", vigencia=dt.date(2026, 1, 1))
    sessao_real.flush()

    assert svc.piso_em(sessao_real, cc.id, dt.date(2025, 6, 30)) == Decimal("2045.00")
    assert svc.piso_em(sessao_real, cc.id, dt.date(2026, 6, 30)) == Decimal("2230.00")
    assert svc.piso_em(sessao_real, cc.id, dt.date(2024, 1, 1)) is None   # antes de existir


@bancos
def test_piso_com_vigencia_anterior_ao_vigente_e_recusado(sessao_real):
    """Senão o histórico fica com duas linhas abertas e o sistema passa a ter
    dois pisos vigentes — cada tela escolhendo um."""
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    svc = cargos_planilha.svc
    cargo = svc.achar_ou_criar_cargo(sessao_real, "SOLDADOR")
    conv = svc.achar_ou_criar_convencao(sessao_real, "CE-Interior")
    cc = svc.cruzamento(sessao_real, cargo.id, conv.id, criar=True)
    svc.definir_piso(sessao_real, cc.id, salario="1839.50", vigencia=dt.date(2026, 1, 1))
    sessao_real.flush()

    with pytest.raises(ErroValidacao):
        svc.definir_piso(sessao_real, cc.id, salario="1700", vigencia=dt.date(2025, 6, 1))


@bancos
def test_piso_sem_valor_e_recusado(sessao_real):
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    svc = cargos_planilha.svc
    cargo = svc.achar_ou_criar_cargo(sessao_real, "ALMOXARIFE")
    conv = svc.achar_ou_criar_convencao(sessao_real, "Sem Convenção")
    cc = svc.cruzamento(sessao_real, cargo.id, conv.id, criar=True)
    with pytest.raises(ErroValidacao):
        svc.definir_piso(sessao_real, cc.id, salario="")


@bancos
def test_o_salario_brasileiro_com_ponto_de_milhar_e_entendido(sessao_real):
    """A planilha vem "1910.65" da exportação, mas gente digita "1.910,65"."""
    svc = cargos_planilha.svc
    cargo = svc.achar_ou_criar_cargo(sessao_real, "CARPINTEIRO")
    conv = svc.achar_ou_criar_convencao(sessao_real, "BA-Sobradinho")
    cc = svc.cruzamento(sessao_real, cargo.id, conv.id, criar=True)
    svc.definir_piso(sessao_real, cc.id, salario="1.910,65")
    sessao_real.flush()
    assert cc.salario_piso == Decimal("1910.65")
