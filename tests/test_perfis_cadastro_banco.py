"""O perfil virou CADASTRO — e quem decide passa a ser ele (migração 065).

Pedido do dono em 13/09/2026, no modelo do banco dele: *"eu cadastro usuários
e cadastro perfil. O perfil eu digo: esse perfil tem acesso a isso, aquilo e
aquilo outro. E o usuário está dentro daquele perfil (…) só que tem uma
diferença do banco, porque tem a questão da obra"*.

COM BANCO DE VERDADE, e não com o dublê, por três motivos que o dublê não
alcança: a leitura do perfil é um `JOIN`, o padrão da coluna
`ve_todas_as_obras` é do banco, e o que se prova aqui é justamente a promessa
da migração — **ninguém perde nem ganha acesso no dia da virada**.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth import perfis as svc, secoes as cat
from app.apps.erp.core.auth.permissoes import (
    PERMISSOES, acoes_do_perfil_no_banco, decidir, pode_com_banco,
    ve_todas_as_obras,
)
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Perfil, PerfilUsuario as P, Usuario,
)

pytestmark = pytest.mark.banco

SENHA = gerar_hash("senha-de-teste-1234")

# Cada cargo de antes e o perfil pronto que a migração criou para ele.
CARGO_E_PERFIL = {
    P.ADMIN: "Administrador",
    P.DIRETOR_FINANCEIRO: "Diretor financeiro",
    P.FINANCEIRO: "Administrativo financeiro",
    P.GESTOR_OBRA: "Gestor de obras",
    P.SUPERVISOR_OBRA: "Supervisor de obras",
    P.ADMINISTRATIVO_OBRA: "Administrativo de obra",
    P.DEPARTAMENTO_PESSOAL: "Departamento pessoal",
    P.APROVADOR: "Aprovador",
    P.LANCADOR: "Lançador",
    P.CONSULTA: "Consulta",
    P.PARCEIRO: "Parceiro da obra",
}


def _modulos_do_menu(html: str) -> list[str]:
    """Os botões de MÓDULO que a página desenhou, na ordem."""
    import re
    return re.findall(r'class="modulo-btn[^"]*"[^>]*>([^<]+)</a>', html)


def _abas_do_menu(html: str) -> list[str]:
    """As abas do módulo aberto."""
    import re
    return re.findall(r'class="topo-aba[^"]*"\s*\n?\s*href="[^"]*">([^<]+)</a>', html)


def _pessoa(s, chave, cargo, *, perfil_id=None, todas=False):
    u = Usuario(nome=f"Teste {chave}", email=f"{chave}@teste.bws.local",
                senha_hash=SENHA, perfil=cargo, perfil_id=perfil_id,
                ve_todas_as_obras=todas, escopo_visao=EscopoVisao.PROPRIOS)
    s.add(u)
    s.flush()
    return u


def _perfil_chamado(s, nome) -> Perfil:
    p = s.scalars(
        text("SELECT id FROM perfis WHERE nome = :n").bindparams(n=nome)).first()
    assert p, f"a migração 065 devia ter criado o perfil “{nome}”"
    return s.get(Perfil, p)


# ---------------------------------------------------------------------------
# 1. A promessa da migração: ninguém perde nem ganha acesso na virada
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("cargo", sorted(CARGO_E_PERFIL, key=lambda c: c.value))
def test_o_perfil_pronto_responde_igual_ao_cargo_antigo(sessao_real, cargo):
    """Ação por ação, o perfil semeado tem de dar a MESMA resposta do cargo.

    É a única prova de que a migração não mexeu na vida de ninguém — e foi ela
    que pegou a primeira versão do arquivo `.sql`, escrita à mão, que dava
    aprovação a quem hoje só confirma.
    """
    s = sessao_real
    perfil = _perfil_chamado(s, CARGO_E_PERFIL[cargo])
    pelo_cadastro = svc.acoes_do_perfil(s, perfil.id)
    divergentes = [
        acao for acao in sorted(PERMISSOES)
        if decidir(cargo, acao, {}) != decidir(cargo, acao, {}, pelo_cadastro)
    ]
    assert divergentes == [], (
        f"o perfil “{perfil.nome}” mudaria o acesso de quem é {cargo.value} "
        f"nestas ações: {divergentes}")


def test_todo_operador_saiu_da_migracao_com_um_perfil(sessao_real):
    """Ninguém pode ficar sem perfil: sem perfil, vale o cargo antigo, e a
    tela do dono deixaria de mandar naquela pessoa sem nada avisar."""
    s = sessao_real
    u = _pessoa(s, "sem-perfil-ainda", P.LANCADOR)
    s.execute(text("UPDATE usuarios u SET perfil_id = p.id FROM perfis p "
                   "WHERE u.id = :i AND p.nome = 'Lançador'"), {"i": u.id})
    s.flush()
    orfaos = s.execute(text(
        "SELECT count(*) FROM usuarios WHERE perfil_id IS NULL")).scalar()
    assert orfaos == 0


# ---------------------------------------------------------------------------
# 2. Quem manda agora é o cadastro, não o cargo
# ---------------------------------------------------------------------------
def test_o_perfil_do_cadastro_vence_o_cargo(sessao_real):
    """O caso que resume a mudança: um LANÇADOR (cargo que não paga) apontado
    para um perfil que abre os pagamentos PASSA a pagar — sem tocar em código,
    que é exatamente o que o dono pediu."""
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Tesouraria da obra",
                           "secoes": {"fin_pagar": cat.EDITAR}}, None)
    u = _pessoa(s, "lancador-que-paga", P.LANCADOR, perfil_id=perfil["id"])
    assert pode_com_banco(s, u, "pagar") is True
    assert pode_com_banco(s, u, "ver_erp") is True       # a porta de entrada
    assert pode_com_banco(s, u, "configurar") is False   # e nada além


def test_o_perfil_do_cadastro_tambem_FECHA_o_que_o_cargo_abria(sessao_real):
    """A metade que importa mais: um FINANCEIRO apontado para um perfil
    pequeno perde o que o cargo dava. Se só somasse, o cadastro seria enfeite."""
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Só consulta de notas",
                           "secoes": {"fin_notas": cat.LER}}, None)
    u = _pessoa(s, "financeiro-restrito", P.FINANCEIRO, perfil_id=perfil["id"])
    assert pode_com_banco(s, u, "ver_notas") is True
    assert pode_com_banco(s, u, "pagar") is False
    assert pode_com_banco(s, u, "conciliar") is False


def test_perfil_sem_secao_nenhuma_nao_abre_nem_a_porta(sessao_real):
    """O padrão é NADA: perfil recém-criado não deixa a pessoa nem entrar."""
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Perfil recém-criado"}, None)
    u = _pessoa(s, "ninguem", P.ADMINISTRATIVO_OBRA, perfil_id=perfil["id"])
    assert acoes_do_perfil_no_banco(s, u.id) == set()
    assert pode_com_banco(s, u, "ver_erp") is False
    assert pode_com_banco(s, u, "lancar") is False


def test_sem_perfil_apontado_vale_o_cargo(sessao_real):
    """A janela entre publicar e apertar o botão: quem ainda não tem perfil
    continua funcionando pelo cargo, como antes."""
    s = sessao_real
    u = _pessoa(s, "so-cargo", P.FINANCEIRO)
    assert acoes_do_perfil_no_banco(s, u.id) is None
    assert pode_com_banco(s, u, "pagar") is True


def test_a_marcacao_na_pessoa_ainda_vence_o_perfil(sessao_real):
    """As exceções por pessoa (migração 032) continuam valendo POR CIMA do
    perfil — é o "fulano autoriza enquanto o diretor está de férias"."""
    from app.apps.erp.db.models.cadastros import UsuarioPermissao
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Perfil que não aprova",
                           "secoes": {"fin_lancar": cat.EDITAR}}, None)
    u = _pessoa(s, "com-excecao", P.LANCADOR, perfil_id=perfil["id"])
    assert pode_com_banco(s, u, "aprovar") is False
    s.add(UsuarioPermissao(usuario_id=u.id, acao="aprovar", concedida=True))
    s.flush()
    assert pode_com_banco(s, u, "aprovar") is True


# ---------------------------------------------------------------------------
# 3. As obras são do OPERADOR, não do perfil
# ---------------------------------------------------------------------------
def test_duas_pessoas_do_mesmo_perfil_alcancam_obras_diferentes(sessao_real):
    """A diferença que o próprio dono apontou para o banco dele: o perfil diz
    o QUE se faz; as obras, ONDE — e elas são de cada pessoa."""
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Engenheiro de obra",
                           "secoes": {"fin_lancar": cat.EDITAR}}, None)
    de_tudo = _pessoa(s, "ve-tudo", P.LANCADOR, perfil_id=perfil["id"], todas=True)
    da_obra = _pessoa(s, "ve-a-obra", P.LANCADOR, perfil_id=perfil["id"])
    assert ve_todas_as_obras(de_tudo) is True
    assert ve_todas_as_obras(da_obra) is False
    assert pode_com_banco(s, de_tudo, "lancar") == pode_com_banco(s, da_obra, "lancar")


def test_sem_ninguem_dizer_nada_a_coluna_fica_NULA_e_vale_o_cargo(sessao_real):
    """Nulo aqui quer dizer "ninguém disse" — e aí vale o cargo, como antes.

    O caminho até esta decisão vale o registro: a coluna nasceu NOT NULL com
    padrão FALSE, e isso quebrou o cadastro de operador novo (o ORM manda a
    coluna no INSERT mesmo sem valor). Pior que o erro seria o silêncio: com
    padrão FALSE, todo cadastro antigo viraria "não enxerga nada" sem ninguém
    ter decidido isso. A migração escreve a decisão de TODO MUNDO — é o teste
    logo abaixo — e o nulo sobra só para o cadastro feito por outro caminho.
    """
    s = sessao_real
    u = Usuario(nome="Recém-cadastrado", email="novo@teste.bws.local",
                senha_hash=SENHA, perfil=P.ADMINISTRATIVO_OBRA)
    s.add(u)
    s.flush()
    s.refresh(u)
    assert u.ve_todas_as_obras is None
    assert ve_todas_as_obras(u) is False      # o cargo dele não vê tudo


def test_a_migracao_nao_deixa_ninguem_sem_decisao(sessao_real):
    """Depois da 065, nenhuma pessoa fica com a pergunta das obras em aberto."""
    s = sessao_real
    em_aberto = s.execute(text(
        "SELECT count(*) FROM usuarios WHERE ve_todas_as_obras IS NULL")).scalar()
    assert em_aberto == 0


# ---------------------------------------------------------------------------
# 4. O cadastro em si: criar, editar, arquivar
# ---------------------------------------------------------------------------
def test_editar_o_perfil_muda_a_vida_de_quem_esta_nele(sessao_real):
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Comprador da obra",
                           "secoes": {"sup_solicitar": cat.EDITAR}}, None)
    u = _pessoa(s, "comprador", P.LANCADOR, perfil_id=perfil["id"])
    assert pode_com_banco(s, u, "comprar") is False
    svc.editar(s, perfil["id"], {"secoes": {"sup_solicitar": cat.EDITAR,
                                            "sup_comprar": cat.EDITAR}}, None)
    assert pode_com_banco(s, u, "comprar") is True


def test_nome_repetido_e_recusado(sessao_real):
    s = sessao_real
    svc.criar(s, {"nome": "Perfil único"}, None)
    with pytest.raises(ErroValidacao):
        svc.criar(s, {"nome": "perfil único"}, None)


def test_secao_ou_nivel_inventado_e_recusado(sessao_real):
    s = sessao_real
    with pytest.raises(ErroValidacao):
        svc.criar(s, {"nome": "Com seção inventada",
                      "secoes": {"tela_que_nao_existe": cat.LER}}, None)
    with pytest.raises(ErroValidacao):
        svc.criar(s, {"nome": "Com nível inventado",
                      "secoes": {"fin_pagar": "TUDO"}}, None)


def test_nao_se_arquiva_perfil_com_gente_dentro(sessao_real):
    """Arquivar com gente dentro devolveria essas pessoas ao cargo antigo —
    ou seja, daria de volta o acesso que o dono acabou de tirar."""
    s = sessao_real
    perfil = svc.criar(s, {"nome": "Perfil povoado",
                           "secoes": {"fin_lancar": cat.EDITAR}}, None)
    u = _pessoa(s, "dentro-do-perfil", P.LANCADOR, perfil_id=perfil["id"])
    with pytest.raises(ErroValidacao):
        svc.arquivar(s, perfil["id"], None)
    u.perfil_id = None
    s.flush()
    assert svc.arquivar(s, perfil["id"], None)["ativo"] is False


def test_o_catalogo_cobre_todas_as_acoes_do_sistema(sessao_real):
    """Ação que não está em seção nenhuma é ação que o dono não consegue
    conceder pela tela — e ela ficaria fechada para sempre, sem explicação."""
    cobertas = {cat.ACAO_DE_ENTRADA}
    for s_ in cat.SECOES:
        cobertas |= set(s_["ler"]) | set(s_["editar"])
    assert sorted(set(PERMISSOES) - cobertas) == []
    assert sorted(cobertas - set(PERMISSOES)) == []


# ---------------------------------------------------------------------------
# 5. O perfil ESCONDE a área que ele não libera (13/09/2026)
# ---------------------------------------------------------------------------
def test_perfil_so_de_financeiro_nao_ve_nada_de_suprimentos(sessao_real, app_real):
    """A frase do dono, virada teste: *"se a pessoa está liberada apenas pra
    visualizar lançamento financeiro, ela não tem que ver nada do suprimento.
    Não vai ver cadastro de suprimento, de insumo, pedidos de compra"*.

    Duas metades, e as duas contam: a TELA some do menu, e a rota RECUSA. A
    primeira é conforto; a segunda é a trava.
    """
    from conftest import como

    s = sessao_real
    perfil = svc.criar(s, {"nome": "Só financeiro",
                           "secoes": {"fin_lancar": cat.EDITAR,
                                      "fin_titulos": cat.LER}}, None)
    u = _pessoa(s, "so-financeiro", P.LANCADOR, perfil_id=perfil["id"], todas=True)
    s.commit()
    cliente = como(app_real, u.id)

    # A trava: as telas de Suprimentos e de Obras recusam.
    for rota in ("/erp/suprimentos", "/erp/suprimentos/insumos",
                 "/erp/suprimentos/pedidos", "/erp/obras", "/erp/configuracoes"):
        assert cliente.get(rota).status_code == 403, rota

    # O que ela abre continua abrindo.
    for rota in ("/erp/lancar", "/erp/titulos"):
        assert cliente.get(rota).status_code == 200, rota

    # E o menu não oferece o que vai responder 403. Confere-se no MENU, e não
    # na página inteira: "Suprimentos" aparece em comentário de código dentro
    # da tela, e procurar a palavra solta daria um verde falso ao contrário.
    pagina = cliente.get("/erp/titulos").get_data(as_text=True)
    assert _modulos_do_menu(pagina) == ["Financeiro", "Administração"]
    assert "Pedidos" not in _abas_do_menu(pagina)


def test_quem_tem_suprimentos_continua_vendo_suprimentos(sessao_real, app_real):
    """A outra ponta: esconder por perfil não pode esconder de quem tem."""
    from conftest import como

    s = sessao_real
    perfil = svc.criar(s, {"nome": "Comprador",
                           "secoes": {"sup_comprar": cat.EDITAR,
                                      "sup_cadastros": cat.EDITAR}}, None)
    u = _pessoa(s, "comprador-completo", P.LANCADOR,
                perfil_id=perfil["id"], todas=True)
    s.commit()
    cliente = como(app_real, u.id)
    assert cliente.get("/erp/suprimentos").status_code == 200
    pagina = cliente.get("/erp/suprimentos").get_data(as_text=True)
    assert "Suprimentos" in _modulos_do_menu(pagina)
