"""O alcance de quem lança é configuração por PESSOA, não regra do cargo.

Até aqui, ADMINISTRATIVO_OBRA e LANCADOR viam sempre e só o que eles mesmos
lançaram. Agora o cadastro do operador escolhe entre:

    PROPRIOS          — só os lançamentos dele (padrão, o mais restritivo)
    OBRAS_DESIGNADAS  — tudo das obras associadas a ele

O que estes testes seguram, e que é o que importa: **o silêncio fecha**. Quem
não tem o campo preenchido — cadastro antigo, objeto recém-criado, valor
estranho vindo do banco — cai em PROPRIOS. Ampliar o alcance só acontece
quando alguém escolhe ampliar.

E seguram também a promessa de sempre: listagem e detalhe passam pelo MESMO
`aplicar_escopo`, então não há como o detalhe abrir o que a lista esconde.
"""
from __future__ import annotations

from sqlalchemy import select

from app.apps.erp.core.auth import permissoes
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, PerfilUsuario as P, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import Titulo

from conftest import SessaoFalsa, novo_usuario


def _sql(stmt) -> str:
    """WHERE compilado, para conferir por qual regra a consulta foi filtrada."""
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


def _consulta_de_titulos():
    return select(Titulo.id)


# ---------------------------------------------------------------------------
# O padrão fecha
# ---------------------------------------------------------------------------
def test_operador_sem_configuracao_fica_no_mais_restritivo():
    """Cadastro antigo não ganha alcance novo ao subir a migração."""
    antigo = novo_usuario(7, P.ADMINISTRATIVO_OBRA)      # campo nunca preenchido

    assert permissoes.escopo_visao(antigo) is EscopoVisao.PROPRIOS


def test_valor_estranho_no_banco_fica_no_mais_restritivo():
    """Se o dado vier corrompido, o erro tem de ser para o lado que fecha."""
    torto = novo_usuario(7, P.ADMINISTRATIVO_OBRA, escopo_visao="QUALQUER_COISA")

    assert permissoes.escopo_visao(torto) is EscopoVisao.PROPRIOS


def test_administrativo_no_padrao_enxerga_so_o_que_lancou():
    de_obra = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
    s = SessaoFalsa(de_obra, UsuarioObra(id=1, usuario_id=7, obra_id=30))

    sql = _sql(permissoes.aplicar_escopo(_consulta_de_titulos(), s, de_obra))

    assert "solicitante_id = 7" in sql
    # mesmo tendo obra associada, o rateio não entra: ninguém pediu para ampliar
    assert "rateios" not in sql


# ---------------------------------------------------------------------------
# Ampliar é escolha, e vale para listagem e detalhe
# ---------------------------------------------------------------------------
def test_administrativo_ampliado_enxerga_as_obras_designadas():
    de_obra = novo_usuario(7, P.ADMINISTRATIVO_OBRA,
                           escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s = SessaoFalsa(de_obra, UsuarioObra(id=1, usuario_id=7, obra_id=30))

    sql = _sql(permissoes.aplicar_escopo(_consulta_de_titulos(), s, de_obra))

    assert "rateios" in sql          # passou a alcançar o que está rateado na obra
    assert "obra_id IN (30)" in sql
    assert "solicitante_id = 7" in sql   # e continua vendo o que ele mesmo lançou


def test_lancador_tambem_pode_ser_ampliado():
    """A configuração é da pessoa; vale para todo perfil que filtra por autoria."""
    lancador = novo_usuario(9, P.LANCADOR,
                            escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s = SessaoFalsa(lancador, UsuarioObra(id=1, usuario_id=9, obra_id=44))

    sql = _sql(permissoes.aplicar_escopo(_consulta_de_titulos(), s, lancador))

    assert "obra_id IN (44)" in sql


def test_ampliado_sem_obra_designada_nao_vira_ver_tudo():
    """Lista de obras vazia tem de sobrar autoria, nunca a base inteira."""
    de_obra = novo_usuario(7, P.ADMINISTRATIVO_OBRA,
                           escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s = SessaoFalsa(de_obra)                     # nenhuma obra associada

    sql = _sql(permissoes.aplicar_escopo(_consulta_de_titulos(), s, de_obra))

    assert "solicitante_id = 7" in sql
    assert "rateios" not in sql


def test_detalhe_usa_a_mesma_regra_da_listagem():
    """`pode_ver_titulo` passa por `aplicar_escopo` — não há caminho paralelo."""
    de_obra = novo_usuario(7, P.ADMINISTRATIVO_OBRA,
                           escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    chamadas: list[str] = []
    original = permissoes.aplicar_escopo
    try:
        def espiao(stmt, s, usuario):
            chamadas.append(usuario.email)
            return original(stmt, s, usuario)
        permissoes.aplicar_escopo = espiao
        s = SessaoFalsa(de_obra, escalares=[None])

        assert permissoes.pode_ver_titulo(s, de_obra, 999) is False
    finally:
        permissoes.aplicar_escopo = original

    assert chamadas == [de_obra.email]


# ---------------------------------------------------------------------------
# Escopo de obra acompanha a escolha
# ---------------------------------------------------------------------------
def test_ampliado_passa_a_ter_escopo_de_obra():
    """Quem enxerga POR obra fica preso às obras dele também no detalhe da obra."""
    de_obra = novo_usuario(7, P.ADMINISTRATIVO_OBRA,
                           escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s = SessaoFalsa(de_obra, UsuarioObra(id=1, usuario_id=7, obra_id=30))

    assert permissoes.obras_do_usuario(s, de_obra) == [30]
    assert permissoes.pode_ver_obra(s, de_obra, 30) is True
    assert permissoes.pode_ver_obra(s, de_obra, 31) is False


def test_supervisor_nao_muda_de_comportamento():
    """A novidade não podia mexer em quem já tinha regra própria."""
    supervisor = novo_usuario(2, P.SUPERVISOR_OBRA)      # sem escopo_visao
    s = SessaoFalsa(supervisor, UsuarioObra(id=1, usuario_id=2, obra_id=30))

    sql = _sql(permissoes.aplicar_escopo(_consulta_de_titulos(), s, supervisor))

    assert "obra_id IN (30)" in sql
    assert permissoes.obras_do_usuario(s, supervisor) == [30]


def test_quem_ve_tudo_continua_vendo_tudo():
    financeiro = novo_usuario(3, P.FINANCEIRO,
                              escopo_visao=EscopoVisao.PROPRIOS)
    s = SessaoFalsa(financeiro)

    stmt = permissoes.aplicar_escopo(_consulta_de_titulos(), s, financeiro)

    assert "WHERE" not in _sql(stmt)
    assert permissoes.obras_do_usuario(s, financeiro) is None
