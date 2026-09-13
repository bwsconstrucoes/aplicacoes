"""A lista de colaboradores respeita a obra de quem pergunta.

Princípio que o dono firmou em 12/09/2026: *"o ideal é sempre limitar as
informações a quem está associado a cada obra"*.

A falha: `listar_colaboradores` nunca recebeu o usuário. Quem tem a ação
"ver_pessoal" — e isso inclui o supervisor e o administrativo, que são presos
a obra — via TODO colaborador da empresa, com **CPF, chave Pix, valor da
diária e auxílios**. Não é número de obra alheia: é dado pessoal de gente que
trabalha em outra frente.

Quem enxerga por ASSUNTO continua enxergando tudo: o Departamento Pessoal
revisa a folha inteira, é o trabalho dele.

COM BANCO DE VERDADE porque o recorte vive no `WHERE`.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.pessoal import listar_colaboradores
from app.apps.erp.db.models.cadastros import (
    Colaborador, EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def equipe(sessao_real):
    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    s.add_all([creche, escola])
    s.flush()

    def gente(nome, cpf, obra):
        c = Colaborador(nome=nome, cpf=cpf, regime="CLT", situacao="ATIVO",
                        obra_id=obra.id if obra else None,
                        pix_chave=f"pix-{cpf}")
        s.add(c)
        s.flush()
        return c

    def pessoa(nome, email, perfil, obra=None):
        u = Usuario(nome=nome, email=email,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil,
                    escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
        s.add(u)
        s.flush()
        if obra is not None:
            s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
            s.flush()
        return u

    return {
        "s": s,
        "pedreiro_creche": gente("José da Creche", "11111111111", creche),
        "pedreiro_escola": gente("Maria da Escola", "22222222222", escola),
        "do_escritorio": gente("Ana do Escritório", "33333333333", None),
        "supervisor": pessoa("Supervisor da creche", "sup@teste.bws.local",
                             P.SUPERVISOR_OBRA, creche),
        "dp": pessoa("Departamento Pessoal", "dp@teste.bws.local",
                     P.DEPARTAMENTO_PESSOAL),
        "dono": pessoa("Marcelo", "chefe@teste.bws.local", P.ADMIN),
    }


def _nomes(linhas):
    return {l["nome"] for l in linhas}


def test_supervisor_so_ve_quem_e_da_obra_dele(equipe):
    d = equipe
    nomes = _nomes(listar_colaboradores(d["s"], usuario=d["supervisor"]))
    assert "José da Creche" in nomes
    assert "Maria da Escola" not in nomes, "viu gente de obra alheia"


def test_colaborador_sem_obra_nao_aparece_para_quem_e_preso_a_obra(equipe):
    """Quem não está ligado a obra nenhuma é do escritório — e o escritório
    não é a frente de quem responde por uma obra."""
    d = equipe
    nomes = _nomes(listar_colaboradores(d["s"], usuario=d["supervisor"]))
    assert "Ana do Escritório" not in nomes


def test_o_departamento_pessoal_continua_vendo_a_folha_inteira(equipe):
    """O DP enxerga por ASSUNTO, não por obra: recortar aqui quebraria o
    trabalho dele."""
    d = equipe
    nomes = _nomes(listar_colaboradores(d["s"], usuario=d["dp"]))
    assert {"José da Creche", "Maria da Escola", "Ana do Escritório"} <= nomes


def test_quem_enxerga_tudo_continua_enxergando(equipe):
    d = equipe
    nomes = _nomes(listar_colaboradores(d["s"], usuario=d["dono"]))
    assert len(nomes) == 3


def test_o_filtro_por_obra_nao_fura_o_recorte(equipe):
    """Pedir explicitamente a obra alheia não devolve a obra alheia."""
    d = equipe
    linhas = listar_colaboradores(
        d["s"], obra_id=d["pedreiro_escola"].obra_id, usuario=d["supervisor"])
    assert _nomes(linhas) == set()
