"""Lançamento com aval em duas pessoas (core/titulos/aval.py).

A regra: nenhum título chega ao caixa com uma assinatura só. Quem lança é a
primeira; a segunda depende do perfil de quem lançou. E ninguém — nem o
diretor — avaliza o próprio lançamento.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.apps.erp.core.titulos import aval
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, UsuarioObra
from app.apps.erp.db.models.financeiro import Rateio, StatusTitulo

from conftest import SessaoFalsa, novo_titulo, novo_usuario


# ---------------------------------------------------------------------------
# Quem precisa de aval
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("perfil, precisa", [
    (P.ADMINISTRATIVO_OBRA, True),
    (P.LANCADOR, True),
    (P.FINANCEIRO, True),
    (P.SUPERVISOR_OBRA, True),
    (P.GESTOR_OBRA, True),
    (P.DIRETOR_FINANCEIRO, False),   # instância final
    (P.ADMIN, False),
])
def test_exige_aval_por_perfil(perfil, precisa):
    assert aval.exige_aval(novo_usuario(1, perfil)) is precisa


@pytest.mark.parametrize("perfil, pode_ver", [
    (P.ADMINISTRATIVO_OBRA, False),
    (P.LANCADOR, False),
    (P.CONSULTA, False),
    (P.FINANCEIRO, True),
    (P.DIRETOR_FINANCEIRO, True),
])
def test_dados_de_pagamento_escondidos_de_quem_nao_deve_ver(perfil, pode_ver):
    assert aval.pode_ver_dados_pagamento(novo_usuario(1, perfil)) is pode_ver


# ---------------------------------------------------------------------------
# Segregação: ninguém assina o próprio lançamento
# ---------------------------------------------------------------------------
def test_ninguem_avaliza_o_proprio_lancamento():
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    titulo = novo_titulo(1, solicitante_id=7)
    s = SessaoFalsa(lancou, titulo)

    pode, porque = aval.pode_avalizar(s, titulo, lancou)

    assert pode is False
    assert "não avaliza o próprio" in porque


def test_nem_o_admin_avaliza_o_proprio_lancamento():
    """Segregação vale para todos, senão a dupla confirmação é formalidade."""
    admin = novo_usuario(1, P.ADMIN)
    titulo = novo_titulo(1, solicitante_id=1)
    s = SessaoFalsa(admin, titulo)

    pode, _ = aval.pode_avalizar(s, titulo, admin)

    assert pode is False


# ---------------------------------------------------------------------------
# Estado do título
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status", [
    StatusTitulo.RASCUNHO, StatusTitulo.APROVADO, StatusTitulo.PAGO,
    StatusTitulo.CANCELADO,
])
def test_so_avaliza_titulo_aguardando_aval(status):
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    diretor = novo_usuario(2, P.DIRETOR_FINANCEIRO)
    titulo = novo_titulo(1, solicitante_id=7, status=status)
    s = SessaoFalsa(lancou, diretor, titulo)

    pode, porque = aval.pode_avalizar(s, titulo, diretor)

    assert pode is False
    assert status.value in porque


# ---------------------------------------------------------------------------
# Quem pode assinar, por perfil de quem lançou
# ---------------------------------------------------------------------------
def test_supervisor_da_obra_avaliza_lancamento_do_administrativo():
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)
    titulo = novo_titulo(1, solicitante_id=7)
    s = SessaoFalsa(lancou, supervisor, titulo,
                    Rateio(id=1, titulo_id=1, obra_id=10),
                    UsuarioObra(id=1, usuario_id=2, obra_id=10))

    pode, porque = aval.pode_avalizar(s, titulo, supervisor)

    assert pode is True, porque


def test_supervisor_de_outra_obra_nao_avaliza():
    """O escopo por obra não é decoração: supervisor só assina o que é dele."""
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)
    titulo = novo_titulo(1, solicitante_id=7)
    s = SessaoFalsa(lancou, supervisor, titulo,
                    Rateio(id=1, titulo_id=1, obra_id=10),
                    UsuarioObra(id=1, usuario_id=2, obra_id=99))  # outra obra

    pode, porque = aval.pode_avalizar(s, titulo, supervisor)

    assert pode is False
    assert "sob sua supervisão" in porque


def test_lancamento_do_financeiro_exige_diretor_e_nao_supervisor():
    lancou = novo_usuario(7, P.FINANCEIRO)
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)
    diretor = novo_usuario(3, P.DIRETOR_FINANCEIRO)
    titulo = novo_titulo(1, solicitante_id=7)

    s1 = SessaoFalsa(lancou, supervisor, titulo)
    pode_supervisor, porque = aval.pode_avalizar(s1, titulo, supervisor)

    s2 = SessaoFalsa(lancou, diretor, titulo)
    pode_diretor, _ = aval.pode_avalizar(s2, titulo, diretor)

    assert pode_supervisor is False
    assert "diretor financeiro" in porque
    assert pode_diretor is True


def test_gestor_de_obras_avaliza_qualquer_obra_do_administrativo():
    """Diferente do supervisor, o gestor não é limitado por obra."""
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    gestor = novo_usuario(2, P.GESTOR_OBRA)
    titulo = novo_titulo(1, solicitante_id=7)
    s = SessaoFalsa(lancou, gestor, titulo, Rateio(id=1, titulo_id=1, obra_id=10))

    pode, porque = aval.pode_avalizar(s, titulo, gestor)

    assert pode is True, porque


# ---------------------------------------------------------------------------
# marcar_para_aval
# ---------------------------------------------------------------------------
def test_marcar_para_aval_trava_titulo_de_quem_precisa():
    titulo = novo_titulo(1, solicitante_id=7, status=StatusTitulo.AGUARDANDO_APROVACAO)
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)

    travou = aval.marcar_para_aval(SessaoFalsa(), titulo, lancou)

    assert travou is True
    assert titulo.exige_aval is True
    assert titulo.status == StatusTitulo.AGUARDANDO_AVAL


def test_marcar_para_aval_nao_trava_lancamento_do_diretor():
    titulo = novo_titulo(1, solicitante_id=3, status=StatusTitulo.AGUARDANDO_APROVACAO)
    diretor = novo_usuario(3, P.DIRETOR_FINANCEIRO)

    travou = aval.marcar_para_aval(SessaoFalsa(), titulo, diretor)

    assert travou is False
    assert titulo.status == StatusTitulo.AGUARDANDO_APROVACAO


def test_titulo_bloqueado_pela_analise_continua_bloqueado():
    """O aval vem depois da revisão do financeiro, não por cima dela."""
    titulo = novo_titulo(1, solicitante_id=7, status=StatusTitulo.BLOQUEADO)
    lancou = novo_usuario(7, P.ADMINISTRATIVO_OBRA)

    aval.marcar_para_aval(SessaoFalsa(), titulo, lancou)

    assert titulo.status == StatusTitulo.BLOQUEADO


# ---------------------------------------------------------------------------
# A assinatura precisa denunciar adulteração
# ---------------------------------------------------------------------------
def test_assinatura_muda_quando_o_valor_assinado_muda():
    usuario = novo_usuario(2, P.DIRETOR_FINANCEIRO)
    quando = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    resumo = {"numero_sp": "SP-1", "valor_liquido": "1000.00"}
    adulterado = {"numero_sp": "SP-1", "valor_liquido": "10000.00"}

    a1 = aval._assinar(resumo, usuario, quando)
    a2 = aval._assinar(adulterado, usuario, quando)

    assert a1 != a2
    assert len(a1) == 64  # sha256 em hexadecimal


def test_assinatura_e_estavel_para_o_mesmo_conteudo():
    usuario = novo_usuario(2, P.DIRETOR_FINANCEIRO)
    quando = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    resumo = {"numero_sp": "SP-1", "valor_liquido": "1000.00"}

    assert aval._assinar(resumo, usuario, quando) == aval._assinar(resumo, usuario, quando)


def test_assinaturas_de_pessoas_diferentes_nao_colidem():
    quando = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    resumo = {"numero_sp": "SP-1", "valor_liquido": "1000.00"}

    a1 = aval._assinar(resumo, novo_usuario(2, P.DIRETOR_FINANCEIRO), quando)
    a2 = aval._assinar(resumo, novo_usuario(3, P.ADMIN), quando)

    assert a1 != a2
