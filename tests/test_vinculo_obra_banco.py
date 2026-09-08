"""Operador e obra ligados pelos dois lados — sem que um lado apague o outro.

O dono pediu para marcar quem responde pela obra tanto pelo cadastro do
operador quanto pelo cadastro da obra, "porque facilita o manuseio". Duas telas
mexendo na mesma ligação é onde as coisas divergem calado.

E havia uma armadilha pronta: a tela do operador APAGAVA todos os vínculos dele
e recriava. Marcar o responsável pela tela da obra e depois salvar o operador
apagaria a marca — sem erro, sem aviso, e a cobrança do mês seguinte iria para
ninguém.

Com banco porque tudo aqui é `WHERE`, `DELETE` e chave única — nada que o dublê
de sessão consiga fingir.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import vinculos
from app.apps.erp.db.models.cadastros import (
    Obra, PerfilUsuario as P, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    a = Obra(codigo="OBRAVINC-A", nome="Obra A")
    b = Obra(codigo="OBRAVINC-B", nome="Obra B")
    ruan = Usuario(nome="Ruan", email="ruan.v@teste.local", ativo=True,
                   senha_hash=gerar_hash("senha-de-teste-123"),
                   perfil=P.ADMINISTRATIVO_OBRA, telefone="5585999990000")
    cleide = Usuario(nome="Cleide", email="cleide.v@teste.local", ativo=True,
                     senha_hash=gerar_hash("senha-de-teste-123"),
                     perfil=P.ADMINISTRATIVO_OBRA, telefone="5585988880000")
    s.add_all([a, b, ruan, cleide]); s.flush()
    return {"s": s, "a": a, "b": b, "ruan": ruan, "cleide": cleide}


def _marca(s, usuario_id, obra_id):
    v = s.scalars(select(UsuarioObra).where(
        UsuarioObra.usuario_id == usuario_id,
        UsuarioObra.obra_id == obra_id)).first()
    return None if v is None else bool(v.responsavel)


def test_marcar_pela_obra_dá_acesso_a_quem_ainda_nao_tinha(cenario):
    """Quem responde precisa conseguir abrir — senão recebe cobrança de uma
    tela que não abre para ele."""
    s, a, ruan = cenario["s"], cenario["a"], cenario["ruan"]
    assert _marca(s, ruan.id, a.id) is None            # não estava ligado

    r = vinculos.definir_responsaveis_da_obra(s, a.id, [ruan.id])
    assert r["passaram_a_enxergar"] == [ruan.id]
    assert _marca(s, ruan.id, a.id) is True


def test_dois_respondem_pela_mesma_obra(cenario):
    """Decisão do dono: "se tiverem dois, os dois recebem"."""
    s, a = cenario["s"], cenario["a"]
    vinculos.definir_responsaveis_da_obra(s, a.id, [cenario["ruan"].id,
                                                    cenario["cleide"].id])
    quem = [o["nome"] for o in vinculos.operadores_da_obra(s, a.id)
            if o["responsavel"]]
    assert sorted(quem) == ["Cleide", "Ruan"]


def test_salvar_o_operador_nao_apaga_a_marca_feita_na_tela_da_obra(cenario):
    """O defeito que este módulo veio impedir.

    A tela do operador manda só a lista de obras — ela não sabe nada sobre
    responsabilidade. Se ela apagar e recriar, a marca some sem aviso.
    """
    s, a, b, ruan = cenario["s"], cenario["a"], cenario["b"], cenario["ruan"]
    vinculos.definir_responsaveis_da_obra(s, a.id, [ruan.id])
    assert _marca(s, ruan.id, a.id) is True

    # a tela do operador salva as obras dele, sem mencionar responsabilidade
    vinculos.definir_obras_do_operador(s, ruan.id, [a.id, b.id])

    assert _marca(s, ruan.id, a.id) is True, "a marca da tela da obra sumiu"
    assert _marca(s, ruan.id, b.id) is False


def test_tirar_a_obra_do_operador_tira_a_responsabilidade_junto(cenario):
    """Não pode sobrar responsável por obra que ele nem enxerga mais."""
    s, a, ruan = cenario["s"], cenario["a"], cenario["ruan"]
    vinculos.definir_responsaveis_da_obra(s, a.id, [ruan.id])
    vinculos.definir_obras_do_operador(s, ruan.id, [])          # tirou tudo
    assert _marca(s, ruan.id, a.id) is None


def test_desmarcar_pela_obra_tira_a_responsabilidade_mas_nao_o_acesso(cenario):
    """Tirar visão é outra decisão, e não pode acontecer de raspão."""
    s, a, ruan = cenario["s"], cenario["a"], cenario["ruan"]
    vinculos.definir_responsaveis_da_obra(s, a.id, [ruan.id])
    vinculos.definir_responsaveis_da_obra(s, a.id, [])
    assert _marca(s, ruan.id, a.id) is False, "perdeu o acesso junto"


def test_a_conferencia_pergunta_a_quem_esta_marcado(cenario):
    """A ponta que importa: é daqui que sai o destinatário da cobrança."""
    from app.apps.erp.core import locacoes_conferencia as conf
    s, a = cenario["s"], cenario["a"]
    assert conf.responsaveis_da_obra(s, a.id) == []
    vinculos.definir_responsaveis_da_obra(s, a.id, [cenario["ruan"].id,
                                                    cenario["cleide"].id])
    assert sorted(conf.responsaveis_da_obra(s, a.id)) == sorted(
        [cenario["ruan"].id, cenario["cleide"].id])


def test_o_banco_recusa_ligar_a_mesma_pessoa_duas_vezes_a_mesma_obra(cenario):
    """Sem esta trava, as duas telas criariam linhas paralelas e passariam a
    discordar sobre quem responde."""
    from sqlalchemy.exc import IntegrityError
    s, a, ruan = cenario["s"], cenario["a"], cenario["ruan"]
    s.add(UsuarioObra(usuario_id=ruan.id, obra_id=a.id)); s.flush()
    s.add(UsuarioObra(usuario_id=ruan.id, obra_id=a.id))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()
