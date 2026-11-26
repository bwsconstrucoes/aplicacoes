"""O modal-ponte da pessoa (26/11/2026) — `/erp/api/pessoa/<id>/ambiente`.

O dono: *"todas as telas que têm relação com a pessoa, eu poder abrir de um
único local (…) Abre um modal com todas as abas que podemos clicar que se
relacionam com o colaborador. Mas tudo abrindo dentro do mesmo modal."* E, no
mesmo dia: *"na parte de operadores, por exemplo, não é todo mundo quem pode
visualizar. Isso tem que estar atrelado ao perfil do usuário."*
"""
from __future__ import annotations

import pytest

from app.apps.erp.db.models.cadastros import Colaborador, Obra, PerfilUsuario as P, Usuario

pytestmark = pytest.mark.banco


@pytest.fixture
def pessoa(sessao_real):
    o = Obra(codigo="AMB-01", nome="Obra do ambiente", status="ATIVA")
    sessao_real.add(o)
    sessao_real.flush()
    c = Colaborador(nome="PESSOA DO AMBIENTE", cpf="71460238001", situacao="ATIVO",
                    obra_id=o.id, vinculo="CTPS", etapa="CONCLUIDO")
    sessao_real.add(c)
    sessao_real.flush()
    return c


def _gente(sessao_real, perfil, apelido):
    from app.apps.erp.core.auth.service import gerar_hash
    u = Usuario(nome=apelido, email=f"{apelido}@amb.teste", ativo=True,
                senha_hash=gerar_hash("x" * 12), perfil=perfil)
    sessao_real.add(u)
    sessao_real.flush()
    return u


def _abas(app_real, usuario, pessoa):
    from tests.conftest import como                      # type: ignore
    r = como(app_real, usuario.id).get(f"/erp/api/pessoa/{pessoa.id}/ambiente")
    return r.status_code, (r.get_json() or {})


def test_o_administrador_ve_todas_as_abas_e_o_cabecalho(app_real, sessao_real, pessoa):
    st, d = _abas(app_real, _gente(sessao_real, P.ADMIN, "adm"), pessoa)
    assert st == 200
    assert [a["chave"] for a in d["abas"]] == [
        "cadastro", "documentos", "demandas", "ponto", "pagamentos", "mensagens", "operador"]
    assert d["pessoa"]["nome"] == "PESSOA DO AMBIENTE"
    assert d["pessoa"]["cpf"] == "714.602.380-01"
    # Cada aba é a tela que já existe, embutida — nada de outra janela.
    assert all("ambiente=1" in a["url"] or "area=ponto" in a["url"] for a in d["abas"])


def test_o_dp_nao_ve_o_operador(app_real, sessao_real, pessoa):
    """Operador é de quem gere operadores — não do DP."""
    st, d = _abas(app_real, _gente(sessao_real, P.DEPARTAMENTO_PESSOAL, "dp"), pessoa)
    assert st == 200
    chaves = [a["chave"] for a in d["abas"]]
    assert "operador" not in chaves
    assert "cadastro" in chaves and "ponto" in chaves


def test_quem_nao_ve_pessoal_nem_ponto_nao_abre(app_real, sessao_real, pessoa):
    from app.apps.erp.core.auth.permissoes import pode
    sem = next((p for p in P if not pode(Usuario(perfil=p), "ver_pessoal")
                and not pode(Usuario(perfil=p), "ver_ponto")), None)
    if sem is None:
        pytest.skip("todo perfil vê pessoal ou ponto")
    st, _ = _abas(app_real, _gente(sessao_real, sem, "sem-pessoal"), pessoa)
    assert st in (403, 404)


def test_cada_tela_embutida_abre(app_real, sessao_real, pessoa):
    """As abas do pessoal respondem — sem menu, só o pedaço pedido."""
    from tests.conftest import como                      # type: ignore
    adm = _gente(sessao_real, P.ADMIN, "adm2")
    cli = como(app_real, adm.id)
    for url in (f"/erp/colaboradores/cadastro/{pessoa.id}?ambiente=1&mostrar=cadastro",
                f"/erp/colaboradores/pessoa/{pessoa.id}?ambiente=1&mostrar=operador"):
        r = cli.get(url)
        assert r.status_code == 200, url
        html = r.get_data(as_text=True)
        assert 'class="embutido"' in html and "const AMBIENTE = true" in html


def test_a_permissao_de_abrir_vem_de_ver_pessoal_ou_o_ponto():
    from app.apps.erp.core.auth.permissoes import ACOES_IMPLICADAS, pode
    assert set(ACOES_IMPLICADAS["abrir_pessoa"]) == {"ver_pessoal", "ver_ponto"}
    for perfil in P:
        u = Usuario(perfil=perfil)
        if pode(u, "ver_pessoal") or pode(u, "ver_ponto"):
            assert pode(u, "abrir_pessoa"), perfil
