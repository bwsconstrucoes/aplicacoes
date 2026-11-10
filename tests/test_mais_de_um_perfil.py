# -*- coding: utf-8 -*-
"""
MAIS DE UM PERFIL DE ACESSO NA MESMA PESSOA (migração 095).

Pedido dele em 09/11/2026, depois de eu recomendar o contrário:

    *"Pode ser que eu queira que uma pessoa utilize mais de um perfil de
    acesso. Digamos que uma pessoa é do financeiro, mas eu quero que ele
    acesse diário de obras."*

**A REGRA QUE ESTE ARQUIVO PROTEGE: perfil SÓ SOMA acesso, nunca tira.** Foi
assim que a ambiguidade de "qual perfil ganha" deixou de existir. Quem tira é
a marcação por pessoa (migração 032), e ela continua vencendo os perfis.

Quase tudo aqui precisa de BANCO DE VERDADE: a decisão sai de um `JOIN` entre
`usuarios`, `usuario_perfis`, `perfis` e `perfil_secoes` — exatamente o que a
sessão dublada da suíte ignora.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth import permissoes as perm
from app.apps.erp.core.auth import secoes as cat
from app.apps.erp.core.comum.auditoria import ErroValidacao

banco_ = pytest.mark.banco


# ---------------------------------------------------------------------------
# SEM BANCO — a conta que decide o nível
# ---------------------------------------------------------------------------
def test_dois_perfis_ficam_com_o_maior_nivel_de_cada_secao():
    """É ISTO QUE TIRA A AMBIGUIDADE. Um perfil diz "só olhar", o outro "olhar
    e mexer": vale "olhar e mexer". Sem essa regra sobraria a pergunta "qual
    deles ganha?", e um cadastro que ninguém entende seis meses depois."""
    junto = perm._somar_niveis({"fin_titulos": cat.LER},
                               {"fin_titulos": cat.EDITAR})
    assert junto["fin_titulos"] == cat.EDITAR


def test_a_ordem_dos_perfis_nao_muda_o_resultado():
    """Se mudasse, o acesso de uma pessoa dependeria da ordem em que alguém
    marcou as caixinhas — e duas pessoas iguais teriam acessos diferentes."""
    a = perm._somar_niveis({"obr_diario": cat.EDITAR}, {"obr_diario": cat.LER})
    b = perm._somar_niveis({"obr_diario": cat.LER}, {"obr_diario": cat.EDITAR})
    assert a == b == {"obr_diario": cat.EDITAR}


def test_perfil_acrescentado_nunca_rebaixa_o_principal():
    """O teste que impede o pior defeito possível aqui: marcar um perfil
    PEQUENO achando que ele restringe a pessoa. Ele não restringe — e, se um
    dia passasse a restringir, alguém perderia acesso em silêncio."""
    junto = perm._somar_niveis(
        {"fin_titulos": cat.EDITAR, "fin_pagar": cat.EDITAR},
        {"fin_titulos": cat.LER})
    assert junto["fin_titulos"] == cat.EDITAR
    assert junto["fin_pagar"] == cat.EDITAR


def test_secao_que_so_o_acrescimo_tem_entra():
    junto = perm._somar_niveis({"fin_titulos": cat.EDITAR},
                               {"obr_diario": cat.EDITAR})
    assert junto == {"fin_titulos": cat.EDITAR, "obr_diario": cat.EDITAR}


def test_sem_acrescimo_nenhum_nada_muda():
    so = {"fin_titulos": cat.LER}
    assert perm._somar_niveis(so, {}) == so


# ---------------------------------------------------------------------------
# A JANELA ENTRE PUBLICAR E APERTAR O BOTÃO
# ---------------------------------------------------------------------------
def test_sem_a_tabela_os_acrescimos_sao_nenhum_e_nao_uma_duvida():
    """⚑ O TESTE MAIS IMPORTANTE DESTE ARQUIVO.

    Entre publicar o código e apertar "Aplicar atualizações do banco", a
    tabela `usuario_perfis` ainda não existe. A leitura dos acréscimos tem de
    responder "nenhum" — e NÃO derrubar a leitura do perfil principal junto.
    Se ela respondesse "não sei", a empresa inteira cairia no cargo antigo
    nessa janela, que é a armadilha conhecida deste repositório.
    """
    class SessaoQueNaoTemATabela:
        def execute(self, *a, **k):
            raise RuntimeError('relation "usuario_perfis" does not exist')

    assert perm._marcacoes_dos_perfis_acrescentados(
        SessaoQueNaoTemATabela(), 1) == {}


# ---------------------------------------------------------------------------
# COM BANCO — o caso que ele descreveu, inteiro
# ---------------------------------------------------------------------------
def _perfil(s, nome: str) -> int:
    """O id de um perfil PELO NOME, conferindo que ele existe.

    ⚑ Conferir não é zelo excessivo: na migração 092 eu usei 'Parceiro' onde o
    perfil se chama 'Parceiro da obra'. O `INSERT ... JOIN perfis ON nome` não
    casou, não inseriu nada e NÃO DEU ERRO — e a fiscalização ficou sem acesso.
    """
    pid = s.execute(text("SELECT id FROM perfis WHERE nome = :n"),
                    {"n": nome}).scalar()
    assert pid is not None, f"perfil {nome!r} não existe no banco"
    return pid


@pytest.fixture
def pessoa(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Gente do financeiro", email="dois.perfis@teste",
                ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.FINANCEIRO, telefone="5585900000011")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@banco_
def test_o_caso_dele_alguem_do_financeiro_que_lanca_diario(sessao_real, pessoa):
    """*"Digamos que uma pessoa é do financeiro, mas eu quero que ele acesse
    diário de obras."* É este teste, do começo ao fim."""
    pessoa.perfil_id = _perfil(sessao_real, "Administrativo financeiro")
    sessao_real.flush()
    antes = perm.acoes_do_perfil_no_banco(sessao_real, pessoa.id)
    assert "lancar_diario" not in antes, "o financeiro não lança diário sozinho"

    sessao_real.execute(text(
        "INSERT INTO usuario_perfis (usuario_id, perfil_id) VALUES (:u, :p)"),
        {"u": pessoa.id, "p": _perfil(sessao_real, "Diário de obra")})
    sessao_real.flush()

    depois = perm.acoes_do_perfil_no_banco(sessao_real, pessoa.id)
    assert "lancar_diario" in depois          # ganhou o que faltava
    assert antes <= depois                    # e não perdeu NADA do que tinha


@banco_
def test_perfil_arquivado_na_lista_nao_concede_nada(sessao_real, pessoa):
    """Arquivar um perfil tem de tirar o acesso de quem o tinha acrescentado.
    Se não tirasse, arquivar seria uma falsa sensação de ter fechado a porta."""
    pessoa.perfil_id = _perfil(sessao_real, "Consulta")
    extra = _perfil(sessao_real, "Diário de obra")
    sessao_real.execute(text(
        "INSERT INTO usuario_perfis (usuario_id, perfil_id) VALUES (:u, :p)"),
        {"u": pessoa.id, "p": extra})
    sessao_real.execute(text("UPDATE perfis SET ativo = FALSE WHERE id = :p"),
                        {"p": extra})
    sessao_real.flush()
    assert "lancar_diario" not in perm.acoes_do_perfil_no_banco(
        sessao_real, pessoa.id)


@banco_
def test_a_marcacao_da_pessoa_continua_podendo_TIRAR(sessao_real, pessoa):
    """Perfil soma; quem tira é a marcação por pessoa. Essa ordem é a que
    permite dar dois perfis a alguém e ainda assim fechar uma porta."""
    pessoa.perfil_id = _perfil(sessao_real, "Administrativo financeiro")
    sessao_real.execute(text(
        "INSERT INTO usuario_perfis (usuario_id, perfil_id) VALUES (:u, :p)"),
        {"u": pessoa.id, "p": _perfil(sessao_real, "Diário de obra")})
    sessao_real.execute(text(
        "INSERT INTO usuario_permissoes (usuario_id, acao, concedida) "
        "VALUES (:u, 'lancar_diario', FALSE)"), {"u": pessoa.id})
    sessao_real.flush()

    assert "lancar_diario" in perm.acoes_do_perfil_no_banco(sessao_real, pessoa.id)
    assert perm.pode_com_banco(sessao_real, pessoa, "lancar_diario") is False


@banco_
def test_sem_perfil_nenhum_continua_valendo_o_cargo_antigo(sessao_real, pessoa):
    """`None` quer dizer "não sei" e cai no cargo — é o que manteve o ERP de pé
    nas migrações anteriores, e não pode ter mudado agora."""
    pessoa.perfil_id = None
    sessao_real.flush()
    assert perm.acoes_do_perfil_no_banco(sessao_real, pessoa.id) is None


@banco_
def test_so_os_acrescimos_ja_bastam_para_decidir(sessao_real, pessoa):
    """Pessoa sem perfil principal mas com acréscimo não cai no cargo antigo:
    ela TEM perfil, e ele é que manda."""
    pessoa.perfil_id = None
    sessao_real.execute(text(
        "INSERT INTO usuario_perfis (usuario_id, perfil_id) VALUES (:u, :p)"),
        {"u": pessoa.id, "p": _perfil(sessao_real, "Diário de obra")})
    sessao_real.flush()
    acoes = perm.acoes_do_perfil_no_banco(sessao_real, pessoa.id)
    assert acoes is not None
    assert "lancar_diario" in acoes
    assert "pagar" not in acoes               # e nada além do diário


# ---------------------------------------------------------------------------
# PELA TELA
# ---------------------------------------------------------------------------
@pytest.fixture
def admin(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="Dona do sistema", email="admin.perfis@teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.ADMIN, telefone="5585900000012")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@banco_
def test_a_tela_grava_e_devolve_os_perfis_acrescentados(app_real, sessao_real,
                                                        pessoa, admin):
    from tests.conftest import como
    principal = _perfil(sessao_real, "Administrativo financeiro")
    extra = _perfil(sessao_real, "Diário de obra")
    c = como(app_real, admin.id)

    r = c.post(f"/erp/api/usuarios/{pessoa.id}",
               json={"perfil_id": principal, "perfis_extras": [extra]})
    assert r.status_code == 200, r.data[:300]

    lista = c.get("/erp/api/usuarios").get_json()["usuarios"]
    dela = next(x for x in lista if x["id"] == pessoa.id)
    assert dela["perfil_id"] == principal
    assert dela["perfis_extras"] == [extra]


@banco_
def test_o_perfil_principal_nao_vira_acrescimo_de_si_mesmo(app_real,
                                                           sessao_real,
                                                           pessoa, admin):
    """Se virasse, trocar o principal deixaria para trás um acréscimo
    invisível com o perfil velho — e a pessoa continuaria com um acesso que a
    tela diz que ela não tem mais."""
    from tests.conftest import como
    principal = _perfil(sessao_real, "Administrativo financeiro")
    r = como(app_real, admin.id).post(
        f"/erp/api/usuarios/{pessoa.id}",
        json={"perfil_id": principal, "perfis_extras": [principal]})
    assert r.status_code == 200
    assert _extras(sessao_real, pessoa.id) == []


@banco_
def test_desmarcar_na_tela_tira_o_acesso(app_real, sessao_real, pessoa, admin):
    from tests.conftest import como
    extra = _perfil(sessao_real, "Diário de obra")
    c = como(app_real, admin.id)
    c.post(f"/erp/api/usuarios/{pessoa.id}", json={"perfis_extras": [extra]})
    assert _extras(sessao_real, pessoa.id) == [extra]
    c.post(f"/erp/api/usuarios/{pessoa.id}", json={"perfis_extras": []})
    assert _extras(sessao_real, pessoa.id) == []


@banco_
def test_perfil_que_nao_existe_e_recusado_com_recado(app_real, sessao_real,
                                                     pessoa, admin):
    from tests.conftest import como
    r = como(app_real, admin.id).post(
        f"/erp/api/usuarios/{pessoa.id}", json={"perfis_extras": [999999]})
    assert r.status_code == 400
    assert "não existe" in r.get_json()["erro"]


def _extras(s, usuario_id: int) -> list[int]:
    return [l[0] for l in s.execute(text(
        "SELECT perfil_id FROM usuario_perfis WHERE usuario_id = :i "
        "ORDER BY perfil_id"), {"i": usuario_id}).all()]
