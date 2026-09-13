"""O painel de Obras: quem pode ESCOLHER a obra, e quem pode ver o DINHEIRO
dela. São duas perguntas diferentes — e até 11/09/2026 eram uma só.

A rota `/erp/api/obras` alimenta cinco telas. Em quatro delas ela é a lista de
onde se ESCOLHE a obra (arquivar documento, marcar um compromisso na agenda,
filtrar contratos e notas). Fechar a lista inteira para quem enxerga "só o que
eu lancei" deixaria essa pessoa sem conseguir arquivar um documento numa obra.

Só que obra é registro SEM AUTOR. Quem enxerga por autoria não tinha recorte
nenhum aqui, e via valor de contrato, gasto, recebido e margem de TODAS as
obras da empresa. É a mesma brecha das Locações, do mesmo dia.

A separação que este arquivo tranca: identificação aberta, números fechados —
e em branco, nunca zero, porque zero seria o sistema AFIRMANDO que a obra não
gastou nada.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)

from conftest import como

pytestmark = pytest.mark.banco

DINHEIRO = ("valor_contrato", "valor_vigente", "aditivos", "gasto",
            "recebido", "margem")


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    a = Obra(codigo="OBRA-A", nome="Creche", valor_contrato=1_000_000)
    b = Obra(codigo="OBRA-B", nome="Escola", valor_contrato=2_000_000)
    s.add_all([a, b])
    s.flush()

    def pessoa(nome, email, perfil, escopo=None):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste"), perfil=perfil,
                    escopo_visao=escopo or EscopoVisao.PROPRIOS)
        s.add(u)
        s.flush()
        return u

    chefe = pessoa("Marcelo", "chefe@bws.test", P.ADMIN)
    lancador = pessoa("Lançador", "lanc@bws.test", P.LANCADOR)
    da_obra = pessoa("Administrativo da A", "adm@bws.test",
                     P.ADMINISTRATIVO_OBRA, EscopoVisao.OBRAS_DESIGNADAS)
    s.add(UsuarioObra(usuario_id=da_obra.id, obra_id=a.id))
    s.flush()
    return {"a": a, "b": b, "chefe": chefe, "lancador": lancador,
            "da_obra": da_obra}


def _painel(app_real, usuario):
    resposta = como(app_real, usuario.id).get("/erp/api/obras")
    assert resposta.status_code == 200
    return {o["codigo"]: o for o in (resposta.get_json().get("obras") or [])}


def test_quem_enxerga_so_o_que_lanca_continua_podendo_escolher_a_obra(
        cenario, app_real):
    """A lista NÃO fecha — senão ninguém arquiva documento em obra."""
    obras = _painel(app_real, cenario["lancador"])
    assert set(obras) == {"OBRA-A", "OBRA-B"}


def test_mas_nao_ve_o_dinheiro_de_obra_nenhuma(cenario, app_real):
    obras = _painel(app_real, cenario["lancador"])
    for codigo, o in obras.items():
        assert o["numeros"] is False, codigo
        for campo in DINHEIRO:
            assert o[campo] is None, f"{codigo}.{campo} vazou"


def test_em_branco_e_nao_zero(cenario, app_real):
    """Zero seria o sistema afirmando que a obra não gastou nada. Não é isso
    que ele sabe — ele sabe que aquela pessoa não pode ver."""
    obras = _painel(app_real, cenario["lancador"])
    assert obras["OBRA-A"]["valor_contrato"] is None
    assert obras["OBRA-A"]["valor_contrato"] != 0


def test_quem_e_designado_a_uma_obra_ve_o_dinheiro_so_dela(cenario, app_real):
    obras = _painel(app_real, cenario["da_obra"])
    assert obras["OBRA-A"]["numeros"] is True
    assert obras["OBRA-A"]["valor_contrato"] == 1_000_000
    assert "OBRA-B" not in obras     # nem aparece: o filtro por obra já corta


def test_o_chefe_ve_tudo(cenario, app_real):
    obras = _painel(app_real, cenario["chefe"])
    assert obras["OBRA-A"]["numeros"] is True
    assert obras["OBRA-B"]["valor_contrato"] == 2_000_000


def test_a_tela_mostra_traco_quando_o_numero_esta_fechado():
    """De nada adianta a rota mandar em branco se a tela escrever 0,00.

    `numero(null)` devolve "0,00" — então a tela PRECISA olhar a marca antes
    de formatar. Esta varredura existe porque o defeito seria invisível: a
    tela ficaria bonita, mentindo.
    """
    from pathlib import Path
    tela = Path("app/apps/erp/templates/erp_obras.html").read_text(encoding="utf-8")
    for campo in ("valor_vigente", "recebido", "gasto", "margem"):
        pedacos = tela.split(f"numero(o.{campo})")
        assert len(pedacos) > 1, f"o.{campo} sumiu da tela de Obras"
        for antes in pedacos[:-1]:
            # A marca tem de estar logo antes — não em qualquer lugar da tela.
            assert 'o.numeros===false?"—":' in antes[-60:], (
                f"A tela formata o.{campo} sem conferir se a pessoa pode ver.")


def test_os_totalizadores_tambem_respeitam_o_que_a_pessoa_pode_ver():
    """Somar valores em branco dá zero, e "R$ 0,00" no topo da tela AFIRMA
    que a empresa não tem contrato nenhum. É falso, e é o tipo de número que
    o dono leria sem desconfiar. Quando não há nada a somar, sai traço."""
    from pathlib import Path
    tela = Path("app/apps/erp/templates/erp_obras.html").read_text(encoding="utf-8")
    assert "const veNumeros = l.some(o => o.numeros !== false);" in tela
    for campo in ("valor_vigente", "recebido", "gasto"):
        assert f"moeda(soma(o=>o.{campo}))" not in tela, (
            f"O totalizador de {campo} voltou a somar direto — vai mostrar "
            f"R$ 0,00 para quem não pode ver os números.")
