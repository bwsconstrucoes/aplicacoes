"""Bloco 1 — escopo de objeto nos anexos, nos dados de pagamento e nos avisos.

Alçada responde "este perfil pode esta ação?". Aqui responde-se a outra
pergunta: "pode NESTE registro?". Sem isso, quem tem a ação alcança qualquer
id — e os ids são sequenciais.

Regra de resposta: fora do escopo devolve "não encontrado", igual a um id que
não existe. Dizer "sem permissão" confirmaria a existência do registro.
"""
from __future__ import annotations

import contextlib

import pytest
from flask import Flask

from app.apps.erp import routes
from app.apps.erp.core.auth import permissoes
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.db.models.cadastros import PerfilUsuario as P, UsuarioObra
from app.apps.erp.db.models.financeiro import (
    Anexo, ContratoServico, Parcela,
)

from conftest import SessaoFalsa, novo_usuario


ADMIN = novo_usuario(1, P.ADMIN)
DE_OBRA = novo_usuario(7, P.ADMINISTRATIVO_OBRA)
SUPERVISOR = novo_usuario(2, P.SUPERVISOR_OBRA)


# ---------------------------------------------------------------------------
# Título
# ---------------------------------------------------------------------------
def test_titulo_dentro_do_escopo_e_visivel():
    s = SessaoFalsa(DE_OBRA, escalares=[1])          # a consulta achou

    assert permissoes.pode_ver_titulo(s, DE_OBRA, 1) is True


def test_titulo_fora_do_escopo_nao_e_visivel():
    s = SessaoFalsa(DE_OBRA, escalares=[None])       # a consulta não achou

    assert permissoes.pode_ver_titulo(s, DE_OBRA, 999) is False


def test_titulo_fora_do_escopo_responde_nao_encontrado():
    s = SessaoFalsa(DE_OBRA, escalares=[None])

    with pytest.raises(ErroNaoEncontrado) as erro:
        permissoes.exigir_titulo_no_escopo(s, DE_OBRA, 999)

    assert "não encontrado" in str(erro.value).lower()


# ---------------------------------------------------------------------------
# Obra
# ---------------------------------------------------------------------------
def test_perfil_que_ve_todas_as_obras_passa():
    assert permissoes.pode_ver_obra(SessaoFalsa(), ADMIN, 42) is True


def test_supervisor_so_ve_a_obra_designada():
    s = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    assert permissoes.pode_ver_obra(s, SUPERVISOR, 10) is True
    assert permissoes.pode_ver_obra(s, SUPERVISOR, 99) is False


def test_obra_de_outro_supervisor_responde_nao_encontrado():
    s = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_obra_no_escopo(s, SUPERVISOR, 99)


# ---------------------------------------------------------------------------
# Anexo — item 1 da auditoria
# ---------------------------------------------------------------------------
def _anexo(id_=1, tipo="titulo", entidade_id=5):
    return Anexo(id=id_, entidade_tipo=tipo, entidade_id=entidade_id,
                 nome_arquivo="comprovante.pdf")


def test_anexo_de_titulo_fora_do_escopo_e_negado():
    s = SessaoFalsa(DE_OBRA, _anexo(), escalares=[None])

    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_anexo_no_escopo(s, DE_OBRA, 1)


def test_anexo_de_titulo_no_escopo_e_liberado():
    s = SessaoFalsa(DE_OBRA, _anexo(), escalares=[1])

    a = permissoes.exigir_anexo_no_escopo(s, DE_OBRA, 1)

    assert a.nome_arquivo == "comprovante.pdf"


def test_anexo_inexistente_e_anexo_fora_do_escopo_dizem_a_mesma_coisa():
    """Se as mensagens diferissem, a diferença viraria um detector de ids."""
    inexistente = SessaoFalsa(DE_OBRA)
    fora = SessaoFalsa(DE_OBRA, _anexo(), escalares=[None])

    with pytest.raises(ErroNaoEncontrado) as e1:
        permissoes.exigir_anexo_no_escopo(inexistente, DE_OBRA, 1)
    with pytest.raises(ErroNaoEncontrado) as e2:
        permissoes.exigir_anexo_no_escopo(fora, DE_OBRA, 1)

    assert str(e1.value) == str(e2.value)


def test_anexo_de_obra_segue_o_escopo_da_obra():
    s = SessaoFalsa(SUPERVISOR, _anexo(tipo="obra", entidade_id=99),
                    UsuarioObra(id=1, usuario_id=2, obra_id=10))

    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_anexo_no_escopo(s, SUPERVISOR, 1)


def test_anexo_de_medicao_segue_a_obra_do_contrato():
    s = SessaoFalsa(SUPERVISOR,
                    _anexo(tipo="contrato_servico", entidade_id=3),
                    ContratoServico(id=3, obra_id=99),
                    UsuarioObra(id=1, usuario_id=2, obra_id=10))

    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_anexo_no_escopo(s, SUPERVISOR, 1)


def test_cadastro_central_fica_com_quem_ve_todas_as_obras():
    """Fornecedor não pertence a obra nenhuma: na dúvida, fecha."""
    de_obra = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_entidade_no_escopo(de_obra, SUPERVISOR, "fornecedor", 4)

    # o admin passa sem erro
    permissoes.exigir_entidade_no_escopo(SessaoFalsa(), ADMIN, "fornecedor", 4)


# ---------------------------------------------------------------------------
# Parcela — item 4 da auditoria (dados bancários do credor)
# ---------------------------------------------------------------------------
def test_parcela_de_titulo_fora_do_escopo_e_negada():
    s = SessaoFalsa(DE_OBRA, Parcela(id=1, titulo_id=5), escalares=[None])

    with pytest.raises(ErroNaoEncontrado):
        permissoes.exigir_parcela_no_escopo(s, DE_OBRA, 1)


def test_parcela_de_titulo_no_escopo_passa():
    s = SessaoFalsa(DE_OBRA, Parcela(id=1, titulo_id=5), escalares=[1])

    permissoes.exigir_parcela_no_escopo(s, DE_OBRA, 1)


def test_parcela_inexistente_responde_igual_a_fora_do_escopo():
    inexistente = SessaoFalsa(DE_OBRA)
    fora = SessaoFalsa(DE_OBRA, Parcela(id=1, titulo_id=5), escalares=[None])

    with pytest.raises(ErroNaoEncontrado) as e1:
        permissoes.exigir_parcela_no_escopo(inexistente, DE_OBRA, 1)
    with pytest.raises(ErroNaoEncontrado) as e2:
        permissoes.exigir_parcela_no_escopo(fora, DE_OBRA, 1)

    assert str(e1.value) == str(e2.value)


# ---------------------------------------------------------------------------
# Ponta a ponta: a resposta HTTP é 404, não 403
# ---------------------------------------------------------------------------
@pytest.fixture
def app():
    a = Flask(__name__)
    a.secret_key = "teste"
    a.register_blueprint(routes.bp)
    return a


def _chamar(app, monkeypatch, caminho, sessao, usuario_id, metodo="get"):
    @contextlib.contextmanager
    def _fake():
        yield sessao

    monkeypatch.setattr(routes, "get_session", _fake)
    with app.test_client() as c:
        with c.session_transaction() as flask_sessao:
            flask_sessao["erp_usuario_id"] = usuario_id
        return getattr(c, metodo)(caminho)


# Cada teste abaixo cobre UMA rota. Não bastava testar as funções de escopo:
# a função pode estar correta e a rota simplesmente não chamá-la — foi o que
# o teste de mutação mostrou quando só existia o primeiro destes.
def test_baixar_anexo_fora_do_escopo_devolve_404(app, monkeypatch):
    sessao = SessaoFalsa(DE_OBRA, _anexo(), escalares=[None])

    r = _chamar(app, monkeypatch, "/erp/anexo/1", sessao, 7)

    assert r.status_code == 404
    assert "não encontrado" in r.get_json()["erro"].lower()


def test_excluir_anexo_fora_do_escopo_devolve_404(app, monkeypatch):
    """Apagar era pior que ler: o anexo mora só no banco, sem segunda cópia."""
    sessao = SessaoFalsa(DE_OBRA, _anexo(), escalares=[None])

    r = _chamar(app, monkeypatch, "/erp/api/anexos/1", sessao, 7, "delete")

    assert r.status_code == 404
    assert sessao.adicionados == []


def test_listar_anexos_de_titulo_fora_do_escopo_devolve_404(app, monkeypatch):
    sessao = SessaoFalsa(DE_OBRA, escalares=[None])

    r = _chamar(app, monkeypatch, "/erp/api/anexos/titulo/5", sessao, 7)

    assert r.status_code == 404


def test_dados_bancarios_de_parcela_fora_do_escopo_devolvem_404(app, monkeypatch):
    """Supervisor TEM a ação de ver dados de pagamento — mas não neste título.

    Por isso o perfil aqui é supervisor e não administrativo: com o
    administrativo o guard barraria antes por alçada, e o teste não provaria
    nada sobre escopo.
    """
    sessao = SessaoFalsa(SUPERVISOR, Parcela(id=1, titulo_id=5), escalares=[None])

    r = _chamar(app, monkeypatch, "/erp/api/pagamentos/detalhe/1", sessao, 2)

    assert r.status_code == 404


def test_interessados_de_titulo_fora_do_escopo_devolve_404(app, monkeypatch):
    """A porta lateral: entrar na lista de avisos de um título alheio.

    O título EXISTE no dublê de propósito. Se não existisse, a rota devolveria
    404 por conta própria e o teste passaria mesmo sem a trava de escopo —
    provando nada. Existindo, só a trava explica o 404.
    """
    from app.apps.erp.db.models.financeiro import Titulo

    sessao = SessaoFalsa(DE_OBRA, Titulo(id=5, solicitante_id=999),
                         escalares=[None])

    r = _chamar(app, monkeypatch, "/erp/api/interessados/5", sessao, 7)

    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Bloco 3 — o detalhe segue exatamente o escopo da listagem
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("caminho", [
    "/erp/api/titulos/5",
    "/erp/api/titulos/5/historico",
    "/erp/api/prestacao/5",
])
def test_detalhe_de_titulo_fora_do_escopo_devolve_404(app, monkeypatch, caminho):
    """A listagem já filtrava; o detalhe por número era a porta aberta ao lado."""
    sessao = SessaoFalsa(DE_OBRA, escalares=[None])

    r = _chamar(app, monkeypatch, caminho, sessao, 7)

    assert r.status_code == 404


def test_titulo_no_escopo_nao_e_barrado():
    """A trava não pode fechar o que a listagem abriria.

    Verificado aqui, e não pela rota: a rota devolve 404 tanto para "fora do
    escopo" quanto para "não existe" — que é exatamente a indistinguibilidade
    pedida. Justamente por isso ela não serve para provar o caminho legítimo.
    """
    s = SessaoFalsa(DE_OBRA, escalares=[1])

    permissoes.exigir_titulo_no_escopo(s, DE_OBRA, 5)   # não levanta


def test_despesa_com_colaborador_de_obra_alheia_e_negada(app, monkeypatch):
    from app.apps.erp.db.models.financeiro import DespesaColaborador

    sessao = SessaoFalsa(SUPERVISOR, DespesaColaborador(id=3, obra_id=99),
                         UsuarioObra(id=1, usuario_id=2, obra_id=10))

    r = _chamar(app, monkeypatch, "/erp/api/dc/3", sessao, 2)

    assert r.status_code == 404


def test_ficha_de_colaborador_de_obra_alheia_e_negada(app, monkeypatch):
    """Histórico de pagamento de pessoa física — o dado mais sensível do módulo."""
    from app.apps.erp.db.models.cadastros import Colaborador

    sessao = SessaoFalsa(SUPERVISOR, Colaborador(id=8, obra_id=99),
                         UsuarioObra(id=1, usuario_id=2, obra_id=10))

    r = _chamar(app, monkeypatch, "/erp/api/colaboradores/8/historico", sessao, 2)

    assert r.status_code == 404


def test_historico_geral_de_entidade_alheia_e_negado(app, monkeypatch):
    sessao = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    r = _chamar(app, monkeypatch, "/erp/api/historico/obra/99", sessao, 2)

    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Bloco 2 — obra alheia não abre, nem para leitura
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("caminho", [
    "/erp/api/obras/99",
    "/erp/api/obras/99/titulos",
    "/erp/api/obras/99/fases",
])
def test_supervisor_nao_abre_obra_que_nao_e_dele(app, monkeypatch, caminho):
    """Supervisor sem designação para a obra 99 — 'sem exceções entre obras'."""
    sessao = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    r = _chamar(app, monkeypatch, caminho, sessao, 2)

    assert r.status_code == 404


def test_simulacao_de_tributacao_de_obra_alheia_e_negada(app, monkeypatch):
    sessao = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    r = _chamar(app, monkeypatch, "/erp/api/obras/99/tributacao", sessao, 2, "post")

    assert r.status_code == 404


def test_obra_designada_passa_pelo_escopo(app, monkeypatch):
    """A trava não pode fechar o que é legítimo: a obra 10 é dele."""
    sessao = SessaoFalsa(SUPERVISOR, UsuarioObra(id=1, usuario_id=2, obra_id=10))

    r = _chamar(app, monkeypatch, "/erp/api/obras/10/fases", sessao, 2)

    assert r.status_code != 404


def test_nao_da_para_se_incluir_em_titulo_alheio_para_receber_comprovante(app, monkeypatch):
    """O item 20 da auditoria, no caminho que realmente vazava.

    Quem entra na lista de interessados passa a receber o aviso de pagamento
    com o comprovante anexo. Sem escopo aqui, o resto do controle era inútil.
    """
    from app.apps.erp.db.models.financeiro import Titulo, TituloInteressado

    sessao = SessaoFalsa(DE_OBRA, Titulo(id=5, solicitante_id=999),
                         escalares=[None])

    r = _chamar(app, monkeypatch, "/erp/api/interessados/5", sessao, 7, "post")

    assert r.status_code == 404
    incluidos = [o for o in sessao.adicionados if isinstance(o, TituloInteressado)]
    assert incluidos == [], "ninguém pode ter entrado na lista de avisos"
