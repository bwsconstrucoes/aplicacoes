"""OS CHECKLISTS DO DP, CONFIGURÁVEIS PELA TELA — com banco de verdade.

Pedido dele em 18/11/2026: *"vi que você importou checklist do Pipefy, só que
eles estão todos desatualizados. Precisa de uma tela pra configurar eles de
forma fácil."*

POR QUE `@pytest.mark.banco`: a regra inteira é de busca — o configurado do
VÍNCULO ganha do configurado de todos, que ganha do padrão do código; e o
índice único da ordem é o que impede a lista sair embaralhada. A `SessaoFalsa`
ignora `WHERE` e aprovaria qualquer uma dessas coisas errada.

Cobre:
  1. sem nada configurado, vale o padrão do código (o do pipe);
  2. configurado, vale o que está na tela;
  3. o do vínculo ganha do que vale para todos;
  4. ⚠️ MUDAR O MODELO NÃO MEXE NO QUE JÁ ESTÁ ABERTO — a trava que protege o
     trabalho que o DP já marcou;
  5. lista vazia volta ao padrão, e não deixa o tipo sem passo nenhum;
  6. item repetido e espaço sobrando são limpos; teto de 40;
  7. tipo que não existe é recusado.
"""
from __future__ import annotations

import pytest
from sqlalchemy import text

from app.apps.erp.core import pessoal_solicitacoes as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao

pytestmark = pytest.mark.banco

TIPO = "ATUALIZACAO_DADOS"


@pytest.fixture(autouse=True)
def _esquecer_a_tabela():
    """O módulo guarda se a tabela existe (é consulta por solicitação aberta).
    Entre testes isso precisa ser esquecido, senão um teste herda a memória do
    outro."""
    svc._TEM_TABELA = None
    yield
    svc._TEM_TABELA = None


def test_sem_configurar_vale_o_padrao_do_codigo(sessao_real):
    do_codigo = svc.checklist_do_tipo(TIPO)
    assert do_codigo, "este teste precisa de um tipo que tenha checklist"

    assert svc.checklist_do_tipo(TIPO, s=sessao_real) == do_codigo
    assert svc.checklist_configurado(sessao_real, TIPO) == ()


def test_configurado_manda_sobre_o_padrao(sessao_real):
    svc.gravar_checklist(sessao_real, TIPO,
                         ["Conferir o documento", "Atualizar o cadastro",
                          "Avisar a obra"])

    assert svc.checklist_do_tipo(TIPO, s=sessao_real) == (
        "Conferir o documento", "Atualizar o cadastro", "Avisar a obra")


def test_a_ordem_digitada_e_a_ordem_que_sai(sessao_real):
    passos = [f"Passo {n}" for n in range(1, 13)]
    svc.gravar_checklist(sessao_real, TIPO, passos)

    assert list(svc.checklist_do_tipo(TIPO, s=sessao_real)) == passos


def test_o_do_vinculo_ganha_do_que_vale_para_todos(sessao_real):
    svc.gravar_checklist(sessao_real, "DESLIGAMENTO", ["Para todo mundo"])
    svc.gravar_checklist(sessao_real, "DESLIGAMENTO", ["Só para carteira"],
                         vinculo="CTPS")

    assert svc.checklist_do_tipo("DESLIGAMENTO", "CTPS", s=sessao_real) \
        == ("Só para carteira",)
    assert svc.checklist_do_tipo("DESLIGAMENTO", "PRESTADOR", s=sessao_real) \
        == ("Para todo mundo",)
    assert svc.checklist_do_tipo("DESLIGAMENTO", s=sessao_real) \
        == ("Para todo mundo",)


# ---------------------------------------------------------------------------
# ⚠️ 4. O QUE JÁ ESTÁ ABERTO NÃO MUDA
#
# É a trava que protege o trabalho do DP: os passos são COPIADOS para a
# solicitação na hora em que ela é aberta. Se mudassem no meio, ele perderia o
# que já marcou e o histórico diria que conferiu algo que nunca esteve na lista.
# ---------------------------------------------------------------------------
def test_mudar_o_modelo_NAO_mexe_na_solicitacao_ja_aberta(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import (
        Colaborador, Obra, PerfilUsuario, Usuario)

    dono = Usuario(nome="DP de teste", email="dp.checklist@teste.com",
                   senha_hash=gerar_hash("x" * 12), ativo=True,
                   perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL)
    obra = Obra(codigo="OBRCK", nome="Obra do Checklist", status="ATIVA")
    sessao_real.add(obra)
    sessao_real.flush()
    c = Colaborador(nome="PESSOA DA SOLICITACAO", cpf="11144477735",
                    situacao="ATIVO", vinculo="CTPS", obra_id=obra.id)
    sessao_real.add_all([dono, c])
    sessao_real.flush()

    svc.gravar_checklist(sessao_real, TIPO, ["Antigo um", "Antigo dois"])
    pedido = svc.abrir(sessao_real,
                       {"tipo": TIPO, "colaborador_id": c.id,
                        "obra_id": obra.id,
                        "descricao": "Mudou o endereço"}, dono)

    svc.gravar_checklist(sessao_real, TIPO, ["Novo um", "Novo dois", "Novo três"])

    itens = sessao_real.execute(text(
        "SELECT descricao FROM dp_solicitacao_itens "
        " WHERE solicitacao_id = :s ORDER BY ordem"),
        {"s": pedido["id"]}).scalars().all()
    assert itens == ["Antigo um", "Antigo dois"], (
        "a solicitação aberta tem de guardar os passos de quando foi aberta")

    # … e a PRÓXIMA já nasce com os novos
    outro = svc.abrir(sessao_real,
                      {"tipo": TIPO, "colaborador_id": c.id,
                       "obra_id": obra.id,
                       "descricao": "Mudou o telefone"}, dono)
    novos = sessao_real.execute(text(
        "SELECT descricao FROM dp_solicitacao_itens "
        " WHERE solicitacao_id = :s ORDER BY ordem"),
        {"s": outro["id"]}).scalars().all()
    assert novos == ["Novo um", "Novo dois", "Novo três"]


def test_lista_vazia_volta_ao_padrao_e_nao_deixa_sem_passo(sessao_real):
    do_codigo = svc.checklist_do_tipo(TIPO)
    svc.gravar_checklist(sessao_real, TIPO, ["Alguma coisa"])
    assert svc.checklist_do_tipo(TIPO, s=sessao_real) == ("Alguma coisa",)

    r = svc.gravar_checklist(sessao_real, TIPO, [])

    assert r["voltou_ao_padrao"] is True
    assert svc.checklist_do_tipo(TIPO, s=sessao_real) == do_codigo, (
        "esvaziar tem de voltar ao padrão, nunca deixar o tipo sem passo nenhum")


def test_repetido_e_espaco_sobrando_sao_limpos(sessao_real):
    r = svc.gravar_checklist(sessao_real, TIPO, [
        "  Conferir   o   documento  ", "Conferir o documento", "", "   ",
        "Avisar a obra"])

    assert r["itens"] == ["Conferir o documento", "Avisar a obra"]


def test_checklist_gigante_e_recusado(sessao_real):
    with pytest.raises(ErroValidacao) as e:
        svc.gravar_checklist(sessao_real, TIPO,
                             [f"Passo {n}" for n in range(60)])
    assert "40" in str(e.value)


def test_tipo_inventado_e_recusado(sessao_real):
    with pytest.raises(ErroValidacao):
        svc.gravar_checklist(sessao_real, "NAO_EXISTE", ["x"])


def test_vinculo_inventado_e_recusado(sessao_real):
    with pytest.raises(ErroValidacao):
        svc.gravar_checklist(sessao_real, TIPO, ["x"], vinculo="ESTAGIARIO")


def test_a_tela_mostra_o_original_ao_lado_do_atual(sessao_real):
    """Sem o original à vista, "voltar ao padrão" é uma promessa que ninguém
    tem como conferir."""
    svc.gravar_checklist(sessao_real, TIPO, ["Só isto"])

    linhas = svc.checklists_para_a_tela(sessao_real)
    meu = [x for x in linhas if x["tipo"] == TIPO and x["vinculo"] is None]

    assert meu, "o tipo tem de aparecer na tela de configuração"
    assert meu[0]["itens"] == ["Só isto"]
    assert meu[0]["padrao"] == list(svc.checklist_do_tipo(TIPO))
    assert meu[0]["configurado"] and meu[0]["mudou"]
