"""QUEM ENTRA PELO PORTAL É DECIDIDO PELO PERFIL, NÃO PELO CARGO (27/11/2026).

Pedido dele, com todas as letras: *"o cargo base não deveria influenciar em
nada. quem deve mandar é o perfil."*

Veio de um prejuízo real: uma pessoa do DP era levada ao portal toda vez que
entrava no ERP. O perfil de acesso dela abria meio sistema; quem a desviava era
o **cargo base** do cadastro — e nenhuma tela dizia isso.

Precisa de banco de verdade porque a decisão lê `perfis` e `perfil_secoes`.
"""
import pytest


from app.apps.erp.core.auth.permissoes import entra_pelo_portal
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

pytestmark = pytest.mark.banco


def _perfil(sessao_real, nome: str, secoes: dict[str, str],
            *, pelo_portal: bool = False) -> int:
    from sqlalchemy import text

    pid = sessao_real.execute(
        text("INSERT INTO perfis (nome, descricao, ativo, entra_pelo_portal) "
             "VALUES (:n, '', true, :p) RETURNING id"),
        {"n": nome, "p": pelo_portal}).scalar()
    for secao, nivel in secoes.items():
        sessao_real.execute(
            text("INSERT INTO perfil_secoes (perfil_id, secao, nivel) "
                 "VALUES (:p, :s, :n)"), {"p": pid, "s": secao, "n": nivel})
    return pid


def _pessoa(sessao_real, email: str, cargo: P, perfil_id=None) -> int:
    from sqlalchemy import text

    return sessao_real.execute(
        text("INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, "
             "                      perfil_id) "
             "VALUES ('Fulano', :e, 'x', :c, true, :p) RETURNING id"),
        {"e": email, "c": cargo.value, "p": perfil_id}).scalar()


def test_cargo_de_obra_nao_prende_quem_tem_perfil_de_escritorio(sessao_real):
    """O caso dela: cargo de obra, perfil que abre o DP. Fica no ERP."""
    pid = _perfil(sessao_real, "DP do teste",
                  {"pes_colaboradores": "EDITAR", "pes_solicitacoes": "EDITAR"})
    uid = _pessoa(sessao_real, "dp-portal@teste.com", P.ADMINISTRATIVO_OBRA, pid)
    assert not entra_pelo_portal(sessao_real, uid, P.ADMINISTRATIVO_OBRA)


def test_cargo_de_escritorio_nao_segura_quem_tem_perfil_de_obra(sessao_real):
    """E o contrário também: o cargo não manda nem para dentro.

    A marcação "entra pelo portal" é do PERFIL (migração 130); o cargo da
    pessoa pode ser o que for.
    """
    pid = _perfil(sessao_real, "Gente de obra do teste",
                  {"obr_diario": "EDITAR", "pes_solicitacoes": "EDITAR"},
                  pelo_portal=True)
    uid = _pessoa(sessao_real, "obra-portal@teste.com", P.FINANCEIRO, pid)
    assert entra_pelo_portal(sessao_real, uid, P.FINANCEIRO)


def test_perfil_que_nao_abre_nada_nao_entra_em_lugar_nenhum(sessao_real):
    """Perfil sem nada marcado não é "gente de obra": é cadastro pela metade.

    Mandar essa pessoa para o portal seria fingir que ela tem acesso a alguma
    coisa. Ela não tem.
    """
    from app.apps.erp.core.auth.permissoes import acoes_e_portal

    pid = _perfil(sessao_real, "Perfil vazio do teste", {})
    uid = _pessoa(sessao_real, "vazio-portal@teste.com", P.FINANCEIRO, pid)
    acoes, portal = acoes_e_portal(sessao_real, uid, P.FINANCEIRO)
    assert acoes == set()
    assert portal is False


def test_sem_perfil_nenhum_a_pessoa_nao_tem_acesso_a_nada(sessao_real):
    """⚑ A REGRA DELE, 27/11/2026, e ela derrubou a ponte que havia aqui.

    *"Se o cara não fez o cadastramento, ponto final, ele não tem absolutamente
    acesso a nada, ele é apenas um colaborador cadastrado. Ele só iria ter a
    possibilidade de ser liberado a acessar alguma coisa no momento que ele
    solicitasse o acesso e fosse liberado e definido um perfil."*

    Antes, quem não tinha perfil caía no cargo antigo — e ele chamou isso de
    gambiarra, com razão: era o cargo mandando por uma porta dos fundos.
    """
    from app.apps.erp.core.auth.permissoes import acoes_e_portal

    for email, cargo in (("semperfil-obra@teste.com", P.ADMINISTRATIVO_OBRA),
                         ("semperfil-fin@teste.com", P.FINANCEIRO)):
        uid = _pessoa(sessao_real, email, cargo)
        acoes, portal = acoes_e_portal(sessao_real, uid, cargo)
        assert acoes == set(), f"{cargo.value} ganhou acesso sem perfil"
        assert portal is False, f"{cargo.value} foi parar no portal sem perfil"


def test_o_cargo_so_vale_quando_nao_da_para_saber(sessao_real):
    """A única sobrevivência do cargo: quando a pessoa nem é encontrada.

    Não é regra de negócio — é a rede que impede o ERP inteiro de cair na
    janela entre publicar o código e apertar o botão do banco, que é a
    armadilha conhecida deste repositório.
    """
    from app.apps.erp.core.auth.permissoes import acoes_e_portal

    acoes, portal = acoes_e_portal(sessao_real, 99_999_999, P.ADMINISTRATIVO_OBRA)
    assert acoes is None
    assert portal is True


def test_a_marcacao_do_perfil_e_quem_decide_a_porta(sessao_real):
    """⚑ A SEGUNDA TENTATIVA, e o porquê da primeira não servir.

    Eu havia DEDUZIDO quem entra pelo portal: "quem não abre nada além do
    diário". O próprio portal derruba essa conta — ele lista colaboradores e
    abre solicitação ao DP, então quem trabalha na obra PRECISA dessas
    permissões para usá-lo, e pela dedução deixaria de ser "da obra"
    justamente por ter o que precisa para trabalhar.

    Portal e telas de dentro são duas PORTAS para as mesmas permissões.
    """
    cheio = {"obr_diario": "EDITAR", "pes_solicitacoes": "EDITAR",
             "pes_colaboradores": "LER"}
    obra = _perfil(sessao_real, "Obra com o que precisa", cheio, pelo_portal=True)
    escritorio = _perfil(sessao_real, "Escritório com o mesmo", cheio)

    u1 = _pessoa(sessao_real, "porta-obra@teste.com", P.FINANCEIRO, obra)
    u2 = _pessoa(sessao_real, "porta-escritorio@teste.com", P.FINANCEIRO, escritorio)

    assert entra_pelo_portal(sessao_real, u1, P.FINANCEIRO)
    assert not entra_pelo_portal(sessao_real, u2, P.FINANCEIRO)


def test_sem_perfil_a_tela_recusa_e_explica(app_real, sessao_real):
    """Ponta a ponta: a pessoa sem perfil não abre nada, nem o portal.

    E a recusa **diz o motivo certo**. Mandar "esta tela não é sua" para quem
    nunca foi liberado faria a pessoa tentar tela por tela até desistir — e
    ligar para o dono.
    """
    from conftest import como

    uid = _pessoa(sessao_real, "bloqueado@teste.com", P.ADMINISTRATIVO_OBRA)
    sessao_real.commit()
    c = como(app_real, uid)
    for rota in ("/erp/inicio", "/erp/titulos", "/erp/colaboradores", "/erp/portal"):
        assert c.get(rota).status_code == 403, rota
    pagina = c.get("/erp/inicio").get_data(as_text=True)
    assert "ainda não foi liberado" in pagina


def test_o_administrador_sem_perfil_ainda_conserta_o_sistema(app_real, sessao_real):
    """Trancar quem destranca não é decisão, é acidente.

    Com o acesso vindo só do perfil, um ADMIN sem perfil ficaria de fora do
    sistema — e ninguém mais poderia dar perfil a ninguém. As telas que
    consertam o sistema continuam abertas para ele.
    """
    from conftest import como

    uid = _pessoa(sessao_real, "adminsemperfil@teste.com", P.ADMIN)
    sessao_real.commit()
    c = como(app_real, uid)
    assert c.get("/erp/configuracoes").status_code == 200


def test_quem_entra_pelo_portal_e_desviado_das_telas_de_dentro(app_real, sessao_real):
    """A marcação do perfil tem efeito de verdade na tela.

    O diário continua abrindo por fora do portal — é o que a obra lança.
    """
    from conftest import como

    pid = _perfil(sessao_real, "Obra de verdade", {"obr_diario": "EDITAR"},
                  pelo_portal=True)
    uid = _pessoa(sessao_real, "desviado@teste.com", P.FINANCEIRO, pid)
    sessao_real.commit()
    c = como(app_real, uid)
    r = c.get("/erp/inicio")
    assert r.status_code == 302 and r.headers["Location"].endswith("/erp/portal")
    assert c.get("/erp/obras/diario").status_code == 200
