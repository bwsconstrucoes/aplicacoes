"""TROCAR O CÓDIGO DA OBRA — e o defeito silencioso que impedia isso.

22/09/2026, o dono usando a tela de Obras:

    *"Tentei alterar o código de uma obra, ele salva, mas quando volta pro
    cadastro não muda."*

O FORMATO DO DEFEITO vale mais que o defeito: a tela SEMPRE mandou o `codigo`,
e a rota tinha uma lista fixa dos campos que aceita — `codigo` não estava nela.
O que não está na lista é descartado **sem reclamar**. Então o salvamento
respondia "ok", a tela recarregava do banco, e o código voltava o mesmo.

**Erro que responde sucesso é pior que erro que responde erro**: não há o que
investigar, e a pessoa fica achando que fez errado.

Com banco de verdade porque o que se prova aqui vive no `WHERE` (não existe
outra obra com este código) e na gravação — nada que a sessão dublada, que
ignora `WHERE`, consiga fingir.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    Obra, PerfilUsuario as P, Usuario,
)

from conftest import como

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    a = Obra(codigo="CODOBR-A", nome="Creche do Bairro Novo", status="ATIVA")
    b = Obra(codigo="CODOBR-B", nome="Escola do Centro", status="ATIVA")
    admin = Usuario(nome="Marcelo", email="codobra@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN,
                    telefone="5585977770000")
    s.add_all([a, b, admin])
    s.flush()
    return {"s": s, "a": a, "b": b, "admin": admin}


def _salvar(app, usuario_id, obra_id, corpo):
    return como(app, usuario_id).post(f"/erp/api/obras/{obra_id}", json=corpo)


def test_trocar_o_codigo_GRAVA_de_verdade(cenario, app_real):
    """É o teste do pedido: o que a tela manda tem de chegar no banco."""
    c = cenario
    r = _salvar(app_real, c["admin"].id, c["a"].id, {"codigo": "CRECHENOVA26"})

    assert r.get_json()["ok"] is True
    assert r.get_json()["obra"]["codigo"] == "CRECHENOVA26", \
        "a resposta tem de devolver o código NOVO — era aqui que voltava o antigo"
    c["s"].refresh(c["a"])
    assert c["a"].codigo == "CRECHENOVA26"


def test_maiusculas_e_espacos_sao_normalizados(cenario, app_real):
    """Mesma regra da criação. Código com caixa diferente é o mesmo código, e
    guardar dos dois jeitos faria a mesma obra parecer duas."""
    c = cenario
    _salvar(app_real, c["admin"].id, c["a"].id, {"codigo": "  creche nova 26 "})
    c["s"].refresh(c["a"])
    assert c["a"].codigo == "CRECHE NOVA 26"


def test_codigo_que_ja_e_de_outra_obra_e_recusado(cenario, app_real):
    """Duas obras com o mesmo código fazem relatório somar coisa errada e
    ninguém percebe — o erro aparece meses depois, num número."""
    c = cenario
    r = _salvar(app_real, c["admin"].id, c["a"].id, {"codigo": "CODOBR-B"})

    assert r.status_code == 400
    corpo = r.get_json()
    assert corpo["ok"] is False
    assert "Escola do Centro" in corpo["erro"], \
        "dizer QUAL obra já usa o código poupa a pessoa de ir procurar"
    c["s"].refresh(c["a"])
    assert c["a"].codigo == "CODOBR-A", "nada pode ter mudado"


def test_codigo_vazio_e_recusado(cenario, app_real):
    c = cenario
    r = _salvar(app_real, c["admin"].id, c["a"].id, {"codigo": "   "})

    assert r.status_code == 400
    assert "não pode ficar vazio" in r.get_json()["erro"]
    c["s"].refresh(c["a"])
    assert c["a"].codigo == "CODOBR-A"


def test_salvar_o_MESMO_codigo_nao_reclama_de_duplicidade(cenario, app_real):
    """A tela manda o formulário inteiro a cada salvamento, com o código
    dentro. Se o próprio código fosse lido como "já existe", editar o telefone
    da obra passaria a ser impossível."""
    c = cenario
    r = _salvar(app_real, c["admin"].id, c["a"].id,
                {"codigo": "CODOBR-A", "cliente": "Prefeitura"})

    assert r.get_json()["ok"] is True
    c["s"].refresh(c["a"])
    assert c["a"].codigo == "CODOBR-A"
    assert c["a"].cliente == "Prefeitura"


def test_a_troca_fica_registrada_na_trilha(cenario, app_real):
    """Renomear obra confunde quem procura por ela depois. O de → para fica
    guardado para responder "cadê a CODOBR-A?"."""
    from sqlalchemy import select
    from app.apps.erp.db.models.financeiro import Evento

    c = cenario
    _salvar(app_real, c["admin"].id, c["a"].id, {"codigo": "CRECHENOVA26"})

    ev = c["s"].scalars(select(Evento).where(
        Evento.entidade_tipo == "obra", Evento.entidade_id == c["a"].id,
        Evento.acao == "CODIGO_ALTERADO")).first()
    assert ev is not None
    assert ev.detalhe["de"] == "CODOBR-A"
    assert ev.detalhe["para"] == "CRECHENOVA26"


def test_os_lancamentos_da_obra_seguem_intactos(cenario, app_real):
    """Trocar o código é RENOMEAR, não criar outra obra: o que aponta para ela
    aponta pelo número interno. Se isto quebrasse, o custo da obra zeraria."""
    c = cenario
    antes = c["a"].id
    _salvar(app_real, c["admin"].id, c["a"].id, {"codigo": "CRECHENOVA26"})
    c["s"].refresh(c["a"])
    assert c["a"].id == antes
