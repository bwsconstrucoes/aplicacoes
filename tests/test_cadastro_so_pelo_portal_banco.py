"""Pessoa NOVA só entra pelo portal (26/11/2026).

O dono: *"Vamos eliminar essa possibilidade de cadastro colaborador dentro do
ERP e deixar apenas no portal. E a gente concentra o regramento todo lá."* E,
no mesmo dia, sobre a aba Demandas: *"Todo pleito é pelo portal."*
"""
from __future__ import annotations

import pathlib

import pytest

from app.apps.erp.db.models.cadastros import Colaborador, PerfilUsuario as P, Usuario

pytestmark = pytest.mark.banco

TELA = (pathlib.Path(__file__).resolve().parents[1] / "app" / "apps" / "erp"
        / "templates" / "erp_colaboradores.html").read_text(encoding="utf-8")


@pytest.fixture
def dp(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    u = Usuario(nome="dp-portal", email="dp-portal@teste", ativo=True,
                senha_hash=gerar_hash("x" * 12), perfil=P.DEPARTAMENTO_PESSOAL)
    sessao_real.add(u)
    sessao_real.flush()
    return u


@pytest.mark.parametrize("url", ["/erp/api/colaboradores/completo", "/erp/api/colaboradores"])
def test_o_erp_nao_cria_pessoa_nova(app_real, dp, url):
    from tests.conftest import como                      # type: ignore
    r = como(app_real, dp.id).post(url, json={"cpf": "71460238001", "nome": "Nova Pelo ERP"})
    assert r.status_code == 400
    assert "Portal da obra" in r.get_json()["erro"]


def test_o_erp_continua_editando_quem_existe(app_real, sessao_real, dp):
    from tests.conftest import como                      # type: ignore
    c = Colaborador(nome="JA EXISTE NO ERP", cpf="28625587887", situacao="ATIVO")
    sessao_real.add(c)
    sessao_real.flush()
    r = como(app_real, dp.id).post("/erp/api/colaboradores/completo",
                                   json={"cpf": "286.255.878-87", "nome": "JA EXISTE NO ERP",
                                         "telefone": "85999990000"})
    assert r.status_code == 200, r.get_json()


def test_a_tela_nao_tem_mais_os_botoes_que_abriam_pedido_por_fora():
    assert 'id="btn-novo"' not in TELA, "+ Colaborador"
    assert 'id="dem-novo"' not in TELA, "Mandar para a contabilidade"
    assert 'id="af-afastar"' not in TELA, "Afastar"
