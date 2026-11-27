# -*- coding: utf-8 -*-
"""SALVAR O CADASTRO NÃO PODE TIRAR PERMISSÃO DE NINGUÉM (27/11/2026).

⚑ O ESTRAGO QUE ELE ENCONTROU NO PRIMEIRO DIA, com as palavras dele:

    *"Eu entrei aqui em uma determinada pessoa para poder liberar o perfil e
    coloquei ele como administrativo de obra, salvei (…) aparece ajuste, mas a
    caixinha não está marcada. Aparece lançar o diário de obra, aí ajuste."*

O caminho, e ele é o caminho NORMAL de liberar alguém:

  1. a pessoa não tem perfil — e, desde 27/11/2026, quem não tem perfil não
     pode nada: o "ajuste fino" abre com as 64 caixinhas desmarcadas;
  2. ele escolhe o perfil de acesso e aperta Salvar;
  3. o salvar grava o CADASTRO primeiro e as CAIXINHAS depois;
  4. o servidor compara as caixinhas (todas desmarcadas) com o perfil NOVO e
     grava um **"não" explícito** para cada permissão que o perfil acabou de
     conceder.

Resultado: o perfil certo e a pessoa trancada, sem nada na tela explicando —
só a palavra "ajuste" ao lado de cada permissão.

O conserto tem duas pontas, e as duas estão aqui: o navegador só grava o
ajuste fino quando alguém MEXEU nele (`PERMISSOES_MEXIDAS`, em
`erp_operador_form.html`), e a rota continua aceitando um pedido vazio para
apagar os ajustes de uma pessoa — que é o botão "Voltar ao que o perfil dá".
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.db.models.cadastros import PerfilUsuario as P

pytestmark = pytest.mark.banco


def _perfil_com_diario(s) -> int:
    pid = s.execute(text(
        "INSERT INTO perfis (nome, descricao, ativo, entra_pelo_portal) "
        "VALUES ('Obra do estrago', '', true, true) RETURNING id")).scalar()
    s.execute(text("INSERT INTO perfil_secoes (perfil_id, secao, nivel) "
                   "VALUES (:p, 'obr_diario', 'EDITAR')"), {"p": pid})
    return pid


def _pessoa_sem_perfil(s, email: str) -> int:
    return s.execute(text(
        "INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo) "
        "VALUES ('Gente da obra', :e, 'x', 'ADMINISTRATIVO_OBRA', true) "
        "RETURNING id"), {"e": email}).scalar()


def test_o_caminho_que_trancava_a_pessoa(app_real, sessao_real):
    """Reproduz o estrago: gravar o ajuste fino velho contra o perfil novo.

    Este teste chama a rota do jeito que a tela chamava ANTES do conserto —
    mandando as 64 caixinhas desmarcadas logo depois de trocar o perfil. Serve
    para o conserto não ser desfeito sem que alguém veja o estrago acontecer.
    """
    from conftest import como
    from app.apps.erp.core.auth.permissoes import PERMISSOES

    pid = _perfil_com_diario(sessao_real)
    uid = _pessoa_sem_perfil(sessao_real, "estrago@teste.bws.local")
    sessao_real.execute(text("UPDATE usuarios SET perfil_id = :p WHERE id = :u"),
                        {"p": pid, "u": uid})
    sessao_real.commit()

    dono = sessao_real.execute(text(
        "INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, perfil_id) "
        "VALUES ('Dono', 'dono.estrago@teste.bws.local', 'x', 'ADMIN', true, "
        "        (SELECT id FROM perfis WHERE nome = 'Cargo ADMIN')) "
        "RETURNING id")).scalar()
    sessao_real.commit()

    c = como(app_real, dono)
    # como a tela fazia antes: TODAS desmarcadas, porque foi o que ela leu
    # quando a pessoa ainda não tinha perfil
    r = c.post(f"/erp/api/usuarios/{uid}/permissoes",
               json={"permissoes": {a: False for a in PERMISSOES}})
    assert r.status_code == 200
    gravadas = sessao_real.execute(text(
        "SELECT acao FROM usuario_permissoes WHERE usuario_id = :u "
        "   AND NOT concedida"), {"u": uid}).all()
    assert any(g[0] == "lancar_diario" for g in gravadas), (
        "o estrago não aconteceu — se a regra mudou, este teste precisa mudar "
        "junto, mas de propósito")

    # e o botão "Voltar ao que o perfil dá" desfaz tudo num clique
    assert c.post(f"/erp/api/usuarios/{uid}/permissoes",
                  json={"permissoes": {}}).status_code == 200
    sobrou = sessao_real.execute(text(
        "SELECT count(*) FROM usuario_permissoes WHERE usuario_id = :u"),
        {"u": uid}).scalar()
    assert sobrou == 0


def test_a_tela_so_grava_o_ajuste_fino_se_alguem_mexeu(app_real):
    """A outra ponta do conserto mora no navegador, e é conferida no texto.

    Um teste de tela de verdade exigiria navegador; o que dá para travar aqui
    é que a trava não suma do arquivo sem alguém notar.
    """
    from pathlib import Path

    forma = Path("app/apps/erp/templates/erp_operador_form.html").read_text(encoding="utf-8")
    assert "PERMISSOES_MEXIDAS" in forma
    assert "caixas.length && PERMISSOES_MEXIDAS" in forma, (
        "o salvar voltou a gravar o ajuste fino sem ninguém ter mexido nele")
    assert "op-limpar-ajustes" in forma, "sumiu o botão de desfazer os ajustes"
