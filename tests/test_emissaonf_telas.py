# -*- coding: utf-8 -*-
"""
As telas da emissão, exercitadas de verdade.

Por que este arquivo existe: em 07/10/2026 o dono tentou abrir o diagnóstico e
levou "acesso não autorizado". O diagnóstico não estava quebrado — faltava o
token no endereço, e **não havia link nenhum para ele na tela**, então a única
forma de chegar lá era digitar o endereço e saber o token de cor.

Nenhum teste tocava nas telas, então isso passou. Agora toca.
"""
import os

import pytest

from app.main import app as app_real


TOKEN = "TOKEN-DE-TESTE"


@pytest.fixture
def cliente(monkeypatch):
    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    return app_real.test_client()


def _texto(resposta):
    return resposta.get_data(as_text=True)


# --------------------------------------------------------------------------- #
# A porta
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("rota", ["/emissao/diag", "/emissao/recuperar",
                                  "/emissao/regerar", "/emissao/nacional_xml"])
def test_sem_o_token_do_link_nenhuma_tela_abre(cliente, rota):
    assert cliente.get(rota).status_code == 403


def test_com_o_token_o_diagnostico_abre(cliente):
    assert cliente.get(f"/emissao/diag?token={TOKEN}").status_code == 200


# --------------------------------------------------------------------------- #
# O diagnóstico separa os dois tokens — foi a confusão que custou tempo
# --------------------------------------------------------------------------- #
def test_o_diagnostico_diz_qual_token_faz_o_que(cliente):
    corpo = _texto(cliente.get(f"/emissao/diag?token={TOKEN}"))
    assert "TOKEN DE INTEGRAÇÃO DA PREFEITURA" in corpo
    assert "TOKEN DESTE LINK" in corpo
    assert "APIs de Integração" in corpo          # onde conseguir o da prefeitura


def test_o_diagnostico_diz_que_o_token_da_prefeitura_falta(cliente, monkeypatch):
    for nome in ("EL_NFSE_TOKEN", "EL_TOKEN", "NFSE_TOKEN", "TOKEN_PREFEITURA",
                 "TOKEN_NFSE", "EMISSAO_NF_EL_TOKEN"):
        monkeypatch.delenv(nome, raising=False)
    corpo = _texto(cliente.get(f"/emissao/diag?token={TOKEN}"))
    assert "Encontrado: NÃO" in corpo


def test_o_diagnostico_acha_o_token_e_diz_de_onde(cliente, monkeypatch):
    monkeypatch.setenv("EL_NFSE_TOKEN", "abcd1234-token-da-prefeitura-9876")
    corpo = _texto(cliente.get(f"/emissao/diag?token={TOKEN}"))
    assert "Encontrado: SIM" in corpo
    assert "variável de ambiente EL_NFSE_TOKEN" in corpo
    # o valor NUNCA aparece inteiro: só os últimos dígitos, para identificar
    assert "abcd1234-token-da-prefeitura-9876" not in corpo
    assert "...9876" in corpo


def test_o_diagnostico_nunca_mostra_valor_de_credencial(cliente, monkeypatch):
    """A regra da área: segredo não entra no chat nem na tela. O diagnóstico
    lista os NOMES das credenciais, para achar o token com rótulo errado, e
    nunca os valores."""
    monkeypatch.setenv("EMISSAO_NF_CERTIFICADO_SENHA", "senha-secreta-do-certificado")
    corpo = _texto(cliente.get(f"/emissao/diag?token={TOKEN}"))
    assert "senha-secreta-do-certificado" not in corpo


# --------------------------------------------------------------------------- #
# Chegar até o diagnóstico sem decorar endereço
# --------------------------------------------------------------------------- #
def test_a_tela_de_emissao_tem_link_para_as_ferramentas_com_o_token_dentro(cliente):
    """Era o que faltava: sem o link, abrir o diagnóstico exige digitar o
    endereço E saber o token de cor — e sem o token a página responde 403, que
    parece defeito e não é."""
    corpo = _texto(cliente.get(f"/emissao/?token={TOKEN}", follow_redirects=True))
    for rota in ("diag", "recuperar", "regerar"):
        assert f"/emissao/{rota}?token={TOKEN}" in corpo


# --------------------------------------------------------------------------- #
# O ambiente aparece na tela
# --------------------------------------------------------------------------- #
def test_quando_travado_em_homologacao_o_diagnostico_avisa(cliente, monkeypatch):
    monkeypatch.setenv("EMISSAO_NF_AMBIENTE", "HOMOLOGACAO")
    corpo = _texto(cliente.get(f"/emissao/diag?token={TOKEN}"))
    assert "HOMOLOGAÇÃO" in corpo


def test_por_padrao_o_diagnostico_diz_que_esta_em_producao(cliente, monkeypatch):
    monkeypatch.delenv("EMISSAO_NF_AMBIENTE", raising=False)
    corpo = _texto(cliente.get(f"/emissao/diag?token={TOKEN}"))
    assert "PRODUÇÃO" in corpo
