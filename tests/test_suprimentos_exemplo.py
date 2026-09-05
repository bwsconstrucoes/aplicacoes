"""Dados de exemplo para simular o fluxo — e a remoção exata deles.

A diferença entre "dado de exemplo" e "sujeira permanente na base" é toda
nestes testes:

  - o que foi criado fica MARCADO por id; a remoção não usa heurística de
    nome, que erraria no dia em que alguém cadastrar "Cimento CP-II" de
    verdade;
  - se algo de exemplo já foi usado num pedido de verdade, a remoção é
    RECUSADA inteira — meia remoção é pior que nenhuma;
  - trazer duas vezes é recusado: dois conjuntos de exemplo na base é pior
    que nenhum.
"""
from __future__ import annotations

import json

import pytest

from app.apps.erp.core.cadastros.validadores import cnpj_valido
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import exemplo as svc
from app.apps.erp.db.models.cadastros import (
    Parametro, PerfilUsuario as P, SuprimentoItem,
)

from conftest import SessaoFalsa, novo_usuario

ADMIN = novo_usuario(1, P.ADMIN, nome="Marcelo")


def marca(**dados):
    return Parametro(chave=svc.CHAVE, valor=json.dumps(dados))


# ---------------------------------------------------------------------------
# Os documentos gerados
# ---------------------------------------------------------------------------
def test_todo_cnpj_de_exemplo_passa_no_validador_do_erp():
    """O validador é o mesmo para dado de teste e dado de verdade. Se um CNPJ
    de exemplo não fechasse, o botão falharia no meio e deixaria metade."""
    for _razao, _fantasia, base, *_resto in svc.FORNECEDORES:
        documento = svc._digito_cnpj(base)
        assert len(documento) == 14
        assert cnpj_valido(documento), f"{base} gerou CNPJ inválido: {documento}"


def test_os_documentos_de_exemplo_nao_se_repetem():
    documentos = [svc._digito_cnpj(f[2]) for f in svc.FORNECEDORES]
    assert len(set(documentos)) == len(documentos)


# ---------------------------------------------------------------------------
# O catálogo em si
# ---------------------------------------------------------------------------
def test_todo_insumo_de_exemplo_aponta_para_conta_de_custo_de_obra():
    for _categoria, itens in svc.CATALOGO:
        for descricao, _unidade, conta in itens:
            assert conta.startswith("3."), \
                f"{descricao} aponta para {conta}, que não é custo de obra"


def test_toda_solicitacao_de_exemplo_cita_insumo_que_o_exemplo_cria():
    conhecidos = {d for _c, itens in svc.CATALOGO for d, _u, _conta in itens}
    for titulo, _prioridade, itens in svc.SOLICITACOES:
        for descricao, _q, _e in itens:
            assert descricao in conhecidos, \
                f"{titulo} pede {descricao!r}, que o exemplo não cadastra"


def test_todo_fornecedor_de_exemplo_vende_categoria_que_o_exemplo_cria():
    conhecidas = {nome for nome, _itens in svc.CATALOGO}
    for razao, *resto in svc.FORNECEDORES:
        for categoria in resto[6]:
            assert categoria in conhecidas, \
                f"{razao} atende {categoria!r}, que o exemplo não cria"


# ---------------------------------------------------------------------------
# Situação
# ---------------------------------------------------------------------------
def test_sem_marca_a_tela_diz_que_nao_ha_nada():
    assert svc.situacao(SessaoFalsa(ADMIN))["presente"] is False


def test_com_marca_a_tela_conta_o_que_existe():
    s = SessaoFalsa(marca(insumos=[1, 2, 3], fornecedores=[9]), ADMIN)
    d = svc.situacao(s)
    assert d["presente"] is True
    por_chave = {x["chave"]: x["quantos"] for x in d["resumo"]}
    assert por_chave == {"insumos": 3, "fornecedores": 1}


def test_marca_ilegivel_nao_derruba_a_tela():
    s = SessaoFalsa(Parametro(chave=svc.CHAVE, valor="{isto não é json"), ADMIN)
    assert svc.situacao(s)["presente"] is False


# ---------------------------------------------------------------------------
# Trazer
# ---------------------------------------------------------------------------
def test_trazer_duas_vezes_e_recusado():
    s = SessaoFalsa(marca(insumos=[1]), ADMIN)
    with pytest.raises(ErroValidacao, match="já estão no sistema"):
        svc.criar(s, ADMIN)


# ---------------------------------------------------------------------------
# Remover
# ---------------------------------------------------------------------------
def test_remover_sem_ter_trazido_e_recusado():
    with pytest.raises(ErroValidacao, match="Não há dados de exemplo"):
        svc.remover(SessaoFalsa(ADMIN), ADMIN)


def test_remover_e_recusado_se_um_insumo_ja_foi_usado_de_verdade(monkeypatch):
    """O caso que justifica a trava: o dono testou, gostou, e alguém já pediu
    material de verdade usando um insumo de exemplo. Apagar levaria junto o
    pedido de verdade."""
    monkeypatch.setattr(svc, "_onde_fornecedor_aparece", lambda: [])
    s = SessaoFalsa(
        marca(insumos=[7], suprimento_solicitacoes=[100]),
        SuprimentoItem(id=1, solicitacao_id=555, numero=1, insumo_id=7,
                       quantidade=10, unidade="SC", obra_id=1),
        ADMIN)
    with pytest.raises(ErroValidacao, match="solicitação de verdade"):
        svc.remover(s, ADMIN)
    assert s.removidos == [], "a recusa vem antes de apagar a primeira linha"


def test_o_proprio_item_de_exemplo_nao_conta_como_uso_de_verdade(monkeypatch):
    monkeypatch.setattr(svc, "_onde_fornecedor_aparece", lambda: [])
    s = SessaoFalsa(
        marca(insumos=[7], suprimento_solicitacoes=[100]),
        SuprimentoItem(id=1, solicitacao_id=100, numero=1, insumo_id=7,
                       quantidade=10, unidade="SC", obra_id=1),
        ADMIN)
    assert svc._o_que_ja_foi_usado(s, {"insumos": [7],
                                       "suprimento_solicitacoes": [100]}) == []


def test_remover_apaga_a_marca_no_fim(monkeypatch):
    monkeypatch.setattr(svc, "_onde_fornecedor_aparece", lambda: [])
    parametro = marca(insumos=[], fornecedores=[], insumo_categorias=[])
    s = SessaoFalsa(parametro, ADMIN)
    svc.remover(s, ADMIN)
    assert parametro in s.removidos, "sem apagar a marca, 'trazer' ficaria travado"
