# -*- coding: utf-8 -*-
"""
TODA ALTERAÇÃO DO CADASTRO FICA REGISTRADA — com quem, quando, de e para.

⚑ POR QUE ESTE ARQUIVO EXISTE (18/11/2026). Ele:

> *"Combinamos de toda alteração no cadastro ser registrada. Alterei um campo e
> não aconteceu. TUDO precisa estar registrado no histórico. Houve uma mudança
> no campo nome, precisa registrar quem e quando e o valor de antes e o
> atual."*

Tinha razão, e a falha era de **duas naturezas somadas** — o que explica por
que ninguém tinha notado:

  1. **O que se gravava era quase nada.** O evento "ALTERADO" guardava o nome
     NOVO e um retrato de TRÊS campos (cargo, salário, obra). Trocar o nome
     gravava o nome novo e nenhum valor de antes: o registro existia e não
     respondia à pergunta dele.
  2. **E não havia onde ver.** A aba "Histórico" da ficha mostra vínculos e
     alterações salariais, que são outra coisa. O evento não aparecia em tela
     nenhuma.

⚠️ POR QUE O RETRATO É DE TODAS AS COLUNAS: lista escolhida à mão é exatamente
o que falhou — ela tinha três campos e o cadastro tem oitenta.
"""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.erp.core.cadastros import pessoas as svc

pytestmark = pytest.mark.banco

CPF = "52998224725"


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP da trilha", email="dp@trilha.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000003")
    sessao_real.add(u)
    sessao_real.flush()
    return u


def _trilha(sessao_real, cid):
    return svc.alteracoes_do_cadastro(sessao_real, cid)


# ---------------------------------------------------------------------------
# O CASO QUE ELE DESCREVEU
# ---------------------------------------------------------------------------
def test_trocar_o_NOME_registra_quem_quando_de_e_para(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva"}, dono)
    sessao_real.flush()

    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva Souza"}, dono)
    sessao_real.flush()

    linhas = _trilha(sessao_real, c.id)
    nome = next(l for l in linhas if l["campo"] == "nome")
    assert nome["de"] == "Maria da Silva"
    assert nome["para"] == "Maria da Silva Souza"
    assert nome["rotulo"] == "Nome"
    assert nome["quem"] == "DP da trilha"
    assert nome["quando"]            # quando, com hora


def test_o_cadastro_criado_aparece_como_marco_inicial(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva"}, dono)
    sessao_real.flush()

    linhas = _trilha(sessao_real, c.id)
    assert any(l.get("criacao") for l in linhas)


# ---------------------------------------------------------------------------
# CAMPO POR CAMPO, e não "mexeu no cadastro"
# ---------------------------------------------------------------------------
def test_tres_campos_mudados_dao_TRES_linhas(sessao_real, dono):
    """A pergunta que a trilha responde é sempre sobre UM campo ("quando mudou
    o nome?"), não sobre a gravação."""
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva",
                                 "telefone": "85999990000"}, dono)
    sessao_real.flush()

    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria Souza",
                             "telefone": "85988880000",
                             "municipio": "Fortaleza"}, dono)
    sessao_real.flush()

    campos = {l["campo"] for l in _trilha(sessao_real, c.id) if l["campo"]}
    assert {"nome", "telefone", "municipio"} <= campos


def test_campo_que_nasce_vazio_registra_o_de_como_vazio(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva"}, dono)
    sessao_real.flush()

    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva",
                             "nome_mae": "Ana da Silva"}, dono)
    sessao_real.flush()

    mae = next(l for l in _trilha(sessao_real, c.id) if l["campo"] == "nome_mae")
    assert mae["de"] == "" and mae["para"] == "Ana da Silva"


def test_DATA_e_DINHEIRO_aparecem_legiveis(sessao_real, dono):
    """Valor convertido pelo banco (data, numérico) tem de ser comparado DEPOIS
    do flush, senão a trilha mostra diferença onde não houve."""
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva",
                                 "admissao": "2026-01-05", "salario": "2.000,00"}, dono)
    sessao_real.flush()

    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva",
                             "admissao": "2026-02-01", "salario": "2.500,00"}, dono)
    sessao_real.flush()

    linhas = {l["campo"]: l for l in _trilha(sessao_real, c.id)}
    assert linhas["admissao"]["de"] == "2026-01-05"
    assert linhas["admissao"]["para"] == "2026-02-01"
    assert "2000" in linhas["salario"]["de"].replace(".", "")
    assert "2500" in linhas["salario"]["para"].replace(".", "")


# ---------------------------------------------------------------------------
# O QUE A TRILHA **NÃO** FAZ — e isto importa tanto quanto o que ela faz
# ---------------------------------------------------------------------------
def test_gravar_SEM_MUDAR_NADA_nao_vira_linha(sessao_real, dono):
    """⚠️ Abrir e fechar o formulário encheria o histórico de "alterado" sem
    nada alterado — e aí ninguém mais acha a alteração de verdade no meio."""
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva",
                                 "telefone": "85999990000"}, dono)
    sessao_real.flush()
    antes = len(_trilha(sessao_real, c.id))

    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva",
                             "telefone": "85999990000"}, dono)
    sessao_real.flush()

    assert len(_trilha(sessao_real, c.id)) == antes


def test_os_carimbos_do_banco_nao_entram_na_trilha(sessao_real, dono):
    """`atualizado_em` muda em TODA gravação. Na trilha, ele afogaria o que
    importa — e não é informação da pessoa."""
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva"}, dono)
    sessao_real.flush()
    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria Souza"}, dono)
    sessao_real.flush()

    campos = {l["campo"] for l in _trilha(sessao_real, c.id)}
    assert "atualizado_em" not in campos and "criado_em" not in campos
    assert "id" not in campos


def test_toda_coluna_do_cadastro_tem_rotulo_em_portugues():
    """⚠️ Campo sem rótulo aparece na trilha com o nome técnico
    ("aux_alimentacao"), que é linguagem de banco e não de gente."""
    from app.apps.erp.db.models.cadastros import Colaborador

    sem = [col.name for col in Colaborador.__table__.columns
           if col.name not in svc._FORA_DA_TRILHA
           and col.name not in svc.CAMPO_ROTULOS]
    assert sem == [], (
        "estas colunas vão aparecer na trilha com o nome técnico: "
        + ", ".join(sem) + ". Dê um rótulo em CAMPO_ROTULOS.")


def test_a_ficha_entrega_a_trilha_para_a_tela(sessao_real, dono):
    """Registrar sem mostrar é o mesmo que não registrar — foi metade da falha
    que ele apontou."""
    c = svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria da Silva"}, dono)
    sessao_real.flush()
    svc.salvar(sessao_real, {"cpf": CPF, "nome": "Maria Souza"}, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert "mudancas" in f
    assert any(m["campo"] == "nome" for m in f["mudancas"])
