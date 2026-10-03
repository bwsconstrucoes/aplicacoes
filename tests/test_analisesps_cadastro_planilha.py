# -*- coding: utf-8 -*-
"""
As planilhas de CADASTRO do BeeVale e da SomaPay — 03/10/2026.

O dono: *"às vezes pode acontecer de eu querer pagar um arquivo, aí, opa, dois
não estão cadastrados ainda. Aí vou lá, gera a planilha dessas duas pessoas,
cadastra, e processa novamente."*
"""
import datetime as dt
import io
from decimal import Decimal as D

import openpyxl
import pytest

from app.apps.analisesps import cadastro_planilha as cp, colaboradores

FICHA = {"cpf": "99713349334", "nome": "Gerlanio Gomes de Lima",
         "celular": "(85) 98888-7777", "matricula": "99713349334A",
         "tipo_contrato": "CTPS", "data_admissao": dt.date(2024, 2, 1),
         "nascimento": dt.date(1990, 5, 3), "cargo": "PEDREIRO", "obra_codigo": "",
         "documentos": {"rg": "2003.123-4", "rg_emissao": "10/01/2010",
                        "rg_orgao": "SSP-CE", "rg_uf": "ce", "nome_mae": "Maria",
                        "sexo": "Masculino", "cep": "60.000-000"}}


@pytest.fixture
def ficha(monkeypatch):
    monkeypatch.setattr(colaboradores, "documentos_de",
                        lambda cpfs: {"99713349334": dict(FICHA)})


def test_a_planilha_do_BEEVALE_tem_as_colunas_do_modelo_e_o_email_do_pagamento(ficha):
    conteudo, nome, avisos = cp.gerar("beevale", ["997.133.493-34"])
    aba = openpyxl.load_workbook(io.BytesIO(conteudo)).active
    assert [c.value for c in aba[1]] == cp.COLUNAS_BEEVALE
    assert [c.value for c in aba[2]] == [
        "GERLANIO GOMES DE LIMA", "GERLANIO LIMA", "99713349334",
        "99713349334@bwsconstrucoes.com.br", "03/05/1990", "55", "(85) 98888-7777"]
    assert nome.endswith(".xlsx") and avisos == []


def test_a_planilha_da_SOMAPAY_sai_do_MODELO_deles_a_partir_da_linha_12(ficha):
    conteudo, nome, avisos = cp.gerar("somapay", ["99713349334", "03513441363"],
                                      {"03513441363": "LUELIA"})
    aba = openpyxl.load_workbook(io.BytesIO(conteudo)).worksheets[0]
    assert aba["A11"].value.startswith("CPF")
    linha = [c.value for c in aba[12]][:23]
    assert linha[:10] == ["99713349334", "GERLANIO GOMES DE LIMA", "03/05/1990",
                          "99713349334", "20031234", "10/01/2010", "SSP", "CE",
                          "MARIA", "M"]
    assert linha[11] == "60000000" and linha[18:20] == ["85", "988887777"]
    assert linha[22] == "CLT (tempo Indeterminado)"
    assert aba["B13"].value == "LUELIA"
    assert any("LUELIA" in a and "fora do cadastro" in a for a in avisos)
    assert nome.endswith(".xls")


def test_o_tipo_de_contrato_cai_na_LISTA_do_modelo():
    assert cp.tipo_de_contrato("Autônomo (RPA)") == "Autônomo (RPA)"
    assert cp.tipo_de_contrato("Prestador de Serviço") == "Autônomo (RPA)"
    assert cp.tipo_de_contrato("Pró-labore") == "Pro labore"
    assert cp.tipo_de_contrato("CTPS") == "CLT (tempo Indeterminado)"
    assert all(cp.tipo_de_contrato(t) in cp.TIPOS_DE_CONTRATO + ("",)
               for t in ("Estágio", "PJ", "qualquer"))


def test_sem_ninguem_ou_destino_estranho_RECUSA():
    with pytest.raises(cp.ErroDoCadastro):
        cp.gerar("beevale", [])
    with pytest.raises(cp.ErroDoCadastro):
        cp.gerar("outro", ["99713349334"])



def test_a_tela_oferece_a_PLANILHA_DE_CADASTRO_nas_folhas():
    from pathlib import Path
    pasta = Path(__file__).resolve().parents[1] / "app/apps/analisesps/templates"
    for tela in ("analisesps_folha_aberta.html", "analisesps_folha_auxilio.html",
                 "analisesps_folha_diaristas.html"):
        assert '_cadastro_planilha.html' in (pasta / tela).read_text(), tela
