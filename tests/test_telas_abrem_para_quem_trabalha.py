"""A TELA ABRE PARA QUEM TEM O TRABALHO DELA (27/11/2026).

O dono, depois de olhar a lista de permissões de uma pessoa do DP: *"to
achando que essa lista do que pode fazer não condiz muito com a realidade"* —
e, em seguida, *"pelo que to vendo tem que revisar tudo"*.

A conta deu razão a ele: **8 telas não abriam para quem tinha justamente o
trabalho delas**, por 17 marcações diferentes. Quem podia "Cadastrar e editar
colaboradores" não abria Pessoal. Quem podia "Fechar e reabrir o mês do ponto"
não abria o Ponto. Nenhum erro na tela — a área simplesmente não existia no
menu, e a pessoa ficava com um sistema vazio.

O furo NÃO estava no perfil cadastrado (lá o "só olhar" sempre vem junto com o
"olhar e mexer"), e sim nos outros dois caminhos que concedem ação: o cargo em
código e a marcação por pessoa. Estes testes travam a regra nos três.
"""
import pytest

from app.apps.erp.core.auth import secoes as cat
from app.apps.erp.core.auth.permissoes import (ACAO_ROTULOS, ACOES_IMPLICADAS,
                                               PERMISSOES, decidir)
from app.apps.erp.db.models.cadastros import PerfilUsuario as P

# Pares (verbo do "olhar e mexer", porta do "só olhar") de todas as seções.
TRABALHO_E_PORTA = sorted({(verbo, porta)
                           for s in cat.SECOES
                           for porta in s["ler"]
                           for verbo in s["editar"]})


def test_ha_pares_a_conferir():
    """Guarda contra o teste passar por não ter achado nada para conferir."""
    assert len(TRABALHO_E_PORTA) >= 20


@pytest.mark.parametrize("verbo,porta", TRABALHO_E_PORTA)
def test_quem_tem_o_trabalho_abre_a_tela(verbo, porta):
    """Marcação solta de um verbo já abre a tela onde esse verbo se exerce."""
    assert decidir(P.CONSULTA, porta, {}, {verbo}), (
        f"{ACAO_ROTULOS.get(verbo, verbo)!r} não abre "
        f"{ACAO_ROTULOS.get(porta, porta)!r}")


def test_o_caso_que_o_dono_encontrou():
    """O conjunto real da pessoa do DP: só verbos, nenhuma porta marcada.

    Antes do conserto este conjunto abria cinco telas e deixava o DP sem
    Pessoal e sem Ponto — com "Cadastrar e editar colaboradores" marcado.
    """
    from app.apps.erp import routes as rotas

    dela = {"abrir_pessoa", "aprovar_afastamento", "arquivar", "atender_dp",
            "cancelar_titulo", "conferir_cpf_colaborador", "configurar_ponto",
            "editar_colaboradores", "encaminhar", "fechar_competencia",
            "lancar", "lancar_dc", "pedir_contabilidade", "solicitar_dp",
            "tocar_processo", "tratar_agenda", "ver_erp"}
    pode = {a: decidir(P.DEPARTAMENTO_PESSOAL, a, {}, dela) for a in PERMISSOES}
    modulos, _ = rotas._menu_da_pessoa(pode)
    nomes = {m["nome"] for m in modulos}
    assert "Pessoal" in nomes, "o DP continua sem as telas de Pessoal"
    assert "Ponto" in nomes, "o DP continua sem as telas do Ponto"
    abas = {r for m in modulos for _, r, _ in m["abas"]}
    assert {"Colaboradores", "Despesas com colaborador"} <= abas


def test_desmarcar_a_mao_continua_fechando():
    """A implicação concede; o "não" explícito no cadastro continua vencendo.

    Sem isto, a correção de hoje tiraria do dono a capacidade de fechar uma
    tela para uma pessoa específica — que é o motivo da marcação existir.
    """
    assert not decidir(P.DEPARTAMENTO_PESSOAL, "ver_pessoal",
                       {"ver_pessoal": False}, {"editar_colaboradores"})


def test_a_tabela_de_implicacoes_nasce_do_catalogo():
    """Seção nova entra certa sem ninguém lembrar de mexer na tabela."""
    for s in cat.SECOES:
        for porta in s["ler"]:
            for verbo in s["editar"]:
                assert verbo in ACOES_IMPLICADAS.get(porta, ()), (
                    f"seção {s['chave']}: {verbo} não implica {porta}")


def test_toda_aba_do_menu_tem_secao_que_a_abre():
    """Marcar seções tem de alcançar TODA tela do menu.

    Tela cujo ingresso nenhuma seção concede só se abre mexendo em código — e
    aí o dono não consegue montar o perfil que ele quer.
    """
    from app.apps.erp import routes as rotas

    concedidas = {a for s in cat.SECOES for a in (*s["ler"], *s["editar"])}
    # `ver_erp` é a porta de entrada: vem de QUALQUER seção acima de NADA.
    orfas = [f"{m['nome']}›{rot}"
             for m in rotas.MODULOS for _, rot, rota in m["abas"]
             if rotas._acao_da_aba(rota) not in concedidas
             and rotas._acao_da_aba(rota) != cat.ACAO_DE_ENTRADA]
    assert not orfas, f"telas que nenhuma seção abre: {orfas}"


def test_a_tabela_de_implicacoes_nao_tem_ciclo():
    """Ciclo aqui derruba o ERP INTEIRO, e sem erro que explique.

    `decidir` resolve implicação chamando a si mesma. Se a ação A passar a ser
    implicada por B e B por A — duas seções que troquem "só olhar" e "olhar e
    mexer" entre si —, toda decisão de permissão vira recursão infinita, na
    guarda que roda antes de TODA rota. Hoje não há ciclo; este teste é para
    continuar não havendo.
    """
    cor: dict[str, str] = {}

    def visita(no: str, caminho: list[str]) -> None:
        cor[no] = "cinza"
        for outro in ACOES_IMPLICADAS.get(no, ()):
            assert cor.get(outro) != "cinza", (
                "ciclo nas implicações: " + " → ".join(caminho + [no, outro]))
            if cor.get(outro) is None:
                visita(outro, caminho + [no])
        cor[no] = "preto"

    for acao in list(ACOES_IMPLICADAS):
        if cor.get(acao) is None:
            visita(acao, [])


def test_decidir_responde_para_perfil_que_nao_abre_nada():
    """O pior caso: perfil cadastrado vazio, toda ação consultada."""
    for acao in PERMISSOES:
        assert decidir(P.CONSULTA, acao, {}, set()) is False
