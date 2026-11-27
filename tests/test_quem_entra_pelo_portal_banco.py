"""QUEM ENTRA PELO PORTAL É DECIDIDO PELO PERFIL, NÃO PELO CARGO (27/11/2026).

Pedido dele, com todas as letras: *"o cargo base não deveria influenciar em
nada. quem deve mandar é o perfil."*

Veio de um prejuízo real: uma pessoa do DP era levada ao portal toda vez que
entrava no ERP. O perfil de acesso dela abria meio sistema; quem a desviava era
o **cargo base** do cadastro — e nenhuma tela dizia isso.

Precisa de banco de verdade porque a decisão lê `perfis` e `perfil_secoes`.
"""
import pytest

from app.apps.erp.core.auth import secoes as cat
from app.apps.erp.core.auth.permissoes import entra_pelo_portal
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

pytestmark = pytest.mark.banco


def _perfil(sessao_real, nome: str, secoes: dict[str, str]) -> int:
    from sqlalchemy import text

    pid = sessao_real.execute(
        text("INSERT INTO perfis (nome, descricao, ativo) "
             "VALUES (:n, '', true) RETURNING id"), {"n": nome}).scalar()
    for secao, nivel in secoes.items():
        sessao_real.execute(
            text("INSERT INTO perfil_secoes (perfil_id, secao, nivel) "
                 "VALUES (:p, :s, :n)"), {"p": pid, "s": secao, "n": nivel})
    return pid


def _pessoa(sessao_real, email: str, cargo: P, perfil_id=None) -> int:
    from sqlalchemy import text

    return sessao_real.execute(
        text("INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, "
             "                      perfil_id) "
             "VALUES ('Fulano', :e, 'x', :c, true, :p) RETURNING id"),
        {"e": email, "c": cargo.value, "p": perfil_id}).scalar()


def test_cargo_de_obra_nao_prende_quem_tem_perfil_de_escritorio(sessao_real):
    """O caso dela: cargo de obra, perfil que abre o DP. Fica no ERP."""
    pid = _perfil(sessao_real, "DP do teste",
                  {"pes_colaboradores": "EDITAR", "pes_solicitacoes": "EDITAR"})
    uid = _pessoa(sessao_real, "dp-portal@teste.com", P.ADMINISTRATIVO_OBRA, pid)
    assert not entra_pelo_portal(sessao_real, uid, P.ADMINISTRATIVO_OBRA)


def test_cargo_de_escritorio_nao_segura_quem_so_tem_o_diario(sessao_real):
    """E o contrário também: o cargo não manda nem para dentro."""
    pid = _perfil(sessao_real, "Gente de obra do teste", {"obr_diario": "EDITAR"})
    uid = _pessoa(sessao_real, "obra-portal@teste.com", P.FINANCEIRO, pid)
    assert entra_pelo_portal(sessao_real, uid, P.FINANCEIRO)


def test_perfil_que_nao_abre_nada_entra_pelo_portal(sessao_real):
    """Cair numa tela vazia seria pior do que cair no portal."""
    pid = _perfil(sessao_real, "Perfil vazio do teste", {})
    uid = _pessoa(sessao_real, "vazio-portal@teste.com", P.FINANCEIRO, pid)
    assert entra_pelo_portal(sessao_real, uid, P.FINANCEIRO)


def test_sem_perfil_nenhum_o_cargo_ainda_decide(sessao_real):
    """A ponte que segura o ERP de pé: sem perfil, não há outra coisa.

    Some sozinha quando todo mundo tiver perfil — e é o que faz a mudança de
    hoje não abrir o escritório inteiro para quem é da obra e nunca recebeu
    perfil nenhum.
    """
    uid = _pessoa(sessao_real, "semperfil-portal@teste.com", P.ADMINISTRATIVO_OBRA)
    assert entra_pelo_portal(sessao_real, uid, P.ADMINISTRATIVO_OBRA)
    outro = _pessoa(sessao_real, "semperfil2-portal@teste.com", P.FINANCEIRO)
    assert not entra_pelo_portal(sessao_real, outro, P.FINANCEIRO)


def test_a_secao_do_portal_existe_no_catalogo(sessao_real):
    """Se `obr_diario` for renomeada, a regra vira 'todo mundo pelo portal'."""
    for chave in cat.SECOES_QUE_O_PORTAL_BASTA:
        assert chave in cat.POR_CHAVE, f"seção {chave} saiu do catálogo"
