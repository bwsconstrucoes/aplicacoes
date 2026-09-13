"""A agenda respeita a obra de quem olha.

Decisão do dono em 12/09/2026: *"o ideal é sempre limitar as informações a quem
está associado a cada obra"*. Até então a agenda mostrava a empresa inteira
para todo mundo que tivesse a ação de vê-la — inclusive para quem responde por
uma obra só.

A regra do aviso SEM OBRA (certidão da empresa, obrigação fiscal) é a mesma já
usada no Arquivo, e pelo mesmo motivo: quem é de DENTRO e responde por obra
continua alcançando, porque é a papelada de que precisa; o PARCEIRO, que é de
FORA, não alcança.

COM BANCO DE VERDADE porque o recorte vive no `WHERE`.
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from app.apps.erp.core.agenda import service as svc
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import AgendaEvento

from conftest import hoje

pytestmark = pytest.mark.banco


@pytest.fixture
def calendario(sessao_real):
    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    s.add_all([creche, escola])
    s.flush()

    def aviso(chave, titulo, obra=None, origem="CERTIDAO"):
        e = AgendaEvento(chave=chave, origem=origem, titulo=titulo,
                         quando=hoje() + timedelta(days=10),
                         avisar_em=hoje() - timedelta(days=1),
                         obra_id=obra.id if obra else None, situacao="ABERTO")
        s.add(e)
        s.flush()
        return e

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
        "da_creche": aviso("A:1", "Seguro da creche vence", creche),
        "da_escola": aviso("A:2", "Seguro da escola vence", escola),
        "da_empresa": aviso("A:3", "Certidão FGTS da BWS vence"),
        "anotacao_escola": aviso("MANUAL:9", "Levar declaração", escola,
                                 origem="MANUAL"),
        "supervisor": pessoa("Supervisor da creche", "sup@teste.bws.local",
                             P.SUPERVISOR_OBRA, creche),
        "parceiro": pessoa("Parceiro da creche", "parceiro@teste.bws.local",
                           P.PARCEIRO, creche),
        "dono": pessoa("Marcelo", "chefe@teste.bws.local", P.ADMIN),
        "escola": escola,
    }


def _titulos(s, usuario):
    return {e["titulo"] for e in svc.listar(s, usuario=usuario)["eventos"]}


def test_o_supervisor_ve_a_obra_dele_e_o_que_e_da_empresa(calendario):
    """Esconder a certidão vencida de quem vai ao órgão não protege nada."""
    d = calendario
    vistos = _titulos(d["s"], d["supervisor"])
    assert "Seguro da creche vence" in vistos
    assert "Certidão FGTS da BWS vence" in vistos
    assert "Seguro da escola vence" not in vistos


def test_o_parceiro_ve_so_a_obra_dele(calendario):
    """Obrigação da BWS não é assunto de quem é de fora."""
    d = calendario
    vistos = _titulos(d["s"], d["parceiro"])
    assert vistos == {"Seguro da creche vence"}


def test_quem_enxerga_tudo_continua_enxergando(calendario):
    d = calendario
    assert len(_titulos(d["s"], d["dono"])) == 4


def test_a_contagem_da_tela_de_inicio_bate_com_a_agenda(calendario):
    """Bolinha dizendo um número e tela mostrando outro é defeito que ninguém
    reporta e todo mundo desconfia."""
    d = calendario
    for quem in (d["supervisor"], d["parceiro"], d["dono"]):
        contados = svc.contagem(d["s"], usuario=quem)["abertos"]
        assert contados == len(_titulos(d["s"], quem))


def test_nao_se_resolve_aviso_de_obra_alheia(calendario):
    """Listagem não é trava: quem grava é a função."""
    d = calendario
    with pytest.raises(ErroNaoEncontrado):
        svc.resolver(d["s"], d["da_escola"].id, usuario=d["supervisor"])


def test_nao_se_apaga_anotacao_de_obra_alheia(calendario):
    d = calendario
    with pytest.raises(ErroNaoEncontrado):
        svc.apagar_manual(d["s"], d["anotacao_escola"].id,
                          usuario=d["supervisor"])


def test_a_recusa_e_igual_a_de_aviso_inexistente(calendario):
    d = calendario
    with pytest.raises(ErroNaoEncontrado) as fora:
        svc.resolver(d["s"], d["da_escola"].id, usuario=d["supervisor"])
    with pytest.raises(ErroNaoEncontrado) as inexistente:
        svc.resolver(d["s"], 99999999, usuario=d["supervisor"])
    assert str(fora.value) == str(inexistente.value)


def test_o_que_e_da_obra_dele_continua_resolvendo(calendario):
    """A trava não pode virar impedimento."""
    d = calendario
    e = svc.resolver(d["s"], d["da_creche"].id, usuario=d["supervisor"])
    assert e.situacao == "RESOLVIDO"


def test_nao_se_anota_na_obra_de_outro(calendario):
    d = calendario
    with pytest.raises(ErroNaoEncontrado):
        svc.criar_manual(d["s"], titulo="Levar papel", quando=hoje(),
                         obra_id=d["escola"].id, usuario=d["supervisor"])
