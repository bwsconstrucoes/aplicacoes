"""Despesa com colaborador: cadeia supervisor → DP → diretor (core/pessoal.py).

Cada degrau tem dono. O DP entra depois do supervisor porque só ele conhece o
cadastro e sabe se a verba é devida. E quem lançou não aprova.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core import pessoal
from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao
from app.apps.erp.db.models.cadastros import PerfilUsuario as P
from app.apps.erp.db.models.financeiro import DespesaColaborador

from conftest import SessaoFalsa, novo_usuario


def _despesa(id_=1, status="AGUARDANDO_SUPERVISOR", criado_por=9):
    return DespesaColaborador(id=id_, numero="DC-001", status=status,
                              criado_por=criado_por)


# ---------------------------------------------------------------------------
# A cadeia em si
# ---------------------------------------------------------------------------
def test_cadeia_tem_tres_degraus_na_ordem_certa():
    assert [e[0] for e in pessoal.CADEIA] == [
        "AGUARDANDO_SUPERVISOR", "AGUARDANDO_DP", "AGUARDANDO_DIRETOR"]


def test_supervisor_passa_a_despesa_para_o_dp():
    d = _despesa()
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)
    s = SessaoFalsa(d, supervisor)

    saida = pessoal.aprovar(s, 1, supervisor)

    assert d.status == "AGUARDANDO_DP"
    assert d.aprovado_supervisor == 2
    assert d.aprovado_supervisor_em is not None
    assert saida["proxima_etapa"] == "departamento pessoal"


def test_dp_passa_a_despesa_para_o_diretor():
    d = _despesa(status="AGUARDANDO_DP")
    dp = novo_usuario(3, P.DEPARTAMENTO_PESSOAL)
    s = SessaoFalsa(d, dp)

    saida = pessoal.aprovar(s, 1, dp)

    assert d.status == "AGUARDANDO_DIRETOR"
    assert d.aprovado_dp == 3
    assert saida["proxima_etapa"] == "diretor financeiro"


def test_diretor_fecha_a_cadeia():
    d = _despesa(status="AGUARDANDO_DIRETOR")
    diretor = novo_usuario(4, P.DIRETOR_FINANCEIRO)
    s = SessaoFalsa(d, diretor)

    saida = pessoal.aprovar(s, 1, diretor)

    assert d.status == "APROVADA"
    assert d.aprovado_diretor == 4
    assert saida["proxima_etapa"] is None


def test_percurso_completo_dos_tres_degraus():
    d = _despesa()
    s = SessaoFalsa(d, novo_usuario(2, P.SUPERVISOR_OBRA),
                    novo_usuario(3, P.DEPARTAMENTO_PESSOAL),
                    novo_usuario(4, P.DIRETOR_FINANCEIRO))

    pessoal.aprovar(s, 1, novo_usuario(2, P.SUPERVISOR_OBRA))
    pessoal.aprovar(s, 1, novo_usuario(3, P.DEPARTAMENTO_PESSOAL))
    pessoal.aprovar(s, 1, novo_usuario(4, P.DIRETOR_FINANCEIRO))

    assert d.status == "APROVADA"
    assert (d.aprovado_supervisor, d.aprovado_dp, d.aprovado_diretor) == (2, 3, 4)
    assert len(s.eventos) == 3, "cada degrau tem de deixar rastro na auditoria"


# ---------------------------------------------------------------------------
# Cada degrau é de quem é
# ---------------------------------------------------------------------------
def test_supervisor_nao_assina_o_degrau_do_dp():
    d = _despesa(status="AGUARDANDO_DP")
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)
    s = SessaoFalsa(d, supervisor)

    with pytest.raises(ErroPermissao) as erro:
        pessoal.aprovar(s, 1, supervisor)

    assert "departamento pessoal" in str(erro.value)


def test_dp_nao_assina_o_degrau_do_diretor():
    d = _despesa(status="AGUARDANDO_DIRETOR")
    dp = novo_usuario(3, P.DEPARTAMENTO_PESSOAL)
    s = SessaoFalsa(d, dp)

    with pytest.raises(ErroPermissao) as erro:
        pessoal.aprovar(s, 1, dp)

    assert "diretor financeiro" in str(erro.value)


def test_administrativo_de_obra_nao_aprova_nenhum_degrau():
    d = _despesa()
    administrativo = novo_usuario(2, P.ADMINISTRATIVO_OBRA)
    s = SessaoFalsa(d, administrativo)

    with pytest.raises(ErroPermissao):
        pessoal.aprovar(s, 1, administrativo)


def test_gestor_de_obra_tambem_cobre_o_degrau_do_supervisor():
    d = _despesa()
    gestor = novo_usuario(2, P.GESTOR_OBRA)
    s = SessaoFalsa(d, gestor)

    pessoal.aprovar(s, 1, gestor)

    assert d.status == "AGUARDANDO_DP"


# ---------------------------------------------------------------------------
# Segregação
# ---------------------------------------------------------------------------
def test_quem_lancou_nao_aprova_a_propria_despesa():
    d = _despesa(criado_por=2)
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)
    s = SessaoFalsa(d, supervisor)

    with pytest.raises(ErroPermissao) as erro:
        pessoal.aprovar(s, 1, supervisor)

    assert "não a aprova" in str(erro.value)


@pytest.mark.parametrize("perfil", [P.ADMIN, P.DIRETOR_FINANCEIRO])
def test_admin_e_diretor_sao_a_excecao_declarada(perfil):
    """São a instância final: travar aqui emperraria o sistema."""
    d = _despesa(status="AGUARDANDO_DIRETOR", criado_por=4)
    usuario = novo_usuario(4, perfil)
    s = SessaoFalsa(d, usuario)

    pessoal.aprovar(s, 1, usuario)

    assert d.status == "APROVADA"


# ---------------------------------------------------------------------------
# Estados sem etapa pendente
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status", ["APROVADA", "DEVOLVIDA", "CANCELADA"])
def test_despesa_sem_etapa_pendente_nao_aceita_aprovacao(status):
    d = _despesa(status=status)
    diretor = novo_usuario(4, P.DIRETOR_FINANCEIRO)
    s = SessaoFalsa(d, diretor)

    with pytest.raises(ErroValidacao) as erro:
        pessoal.aprovar(s, 1, diretor)

    assert "não há aprovação pendente" in str(erro.value)


def test_despesa_inexistente_e_recusada():
    diretor = novo_usuario(4, P.DIRETOR_FINANCEIRO)

    with pytest.raises(ErroValidacao) as erro:
        pessoal.aprovar(SessaoFalsa(), 42, diretor)

    assert "não encontrada" in str(erro.value)
