"""Custo da obra: comprometido e executado, e só conta de DRE.

A DEFINIÇÃO, dada pelo dono em 12/09/2026, e que destravou o grupo de Obras no
assistente: *"o custo normalmente está associado só às despesas de DRE, nada de
fluxo. E é o custo executado e o custo comprometido — são essas duas visões que
a gente tem"*.

Três regras, e cada uma tem um teste aqui:

  1. só conta de DRE (natureza RESULTADO) — transferência entre contas e aporte
     são dinheiro mudando de lugar, não custo;
  2. COMPROMETIDO é o que já foi lançado e ainda vale;
  3. EXECUTADO é o que saiu do caixa de verdade.

E as duas visões aparecem JUNTAS. Mostrar uma só seria escolher pelo dono em
silêncio — e a diferença entre elas é o que ele ainda tem para pagar.

COM BANCO DE VERDADE porque a conta é agregada em SQL e o recorte por obra vive
no `WHERE`.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.perguntas import respostas
from app.apps.erp.db.models.cadastros import (
    Categoria, ContaBancaria, Empresa, EscopoVisao, Fornecedor, Obra,
    PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    EspecieTitulo, FormaPagamento, Pagamento, Parcela, Rateio, StatusParcela,
    StatusTitulo, TipoTitulo, Titulo,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def obra_com_custo(sessao_real):
    s = sessao_real
    emp = Empresa(razao_social="BWS Construções Exemplo", cnpj="11222333000181")
    s.add(emp)
    s.flush()
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio", empresa_id=emp.id)
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto", empresa_id=emp.id)
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    # conta de DRE (custo de obra) e conta de FLUXO (transferência)
    custo = Categoria(codigo="3.1.01", descricao="Cimento", grupo_codigo="3",
                      grupo_nome="Custos de obra", natureza="RESULTADO")
    fluxo = Categoria(codigo="9.1.01", descricao="Transferência entre contas",
                      grupo_codigo="9", grupo_nome="Movimentações",
                      natureza="FLUXO")
    receita = Categoria(codigo="1.1.01", descricao="Receita de medição",
                        grupo_codigo="1", grupo_nome="Receitas",
                        natureza="RESULTADO")
    s.add_all([creche, escola, forn, custo, fluxo, receita])
    s.flush()
    conta = ContaBancaria(descricao="Bradesco", banco_codigo="237",
                          agencia="1234", conta="56789-0")
    s.add(conta)
    s.flush()

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

    diretor = pessoa("Diretora", "dir.custo@teste.bws.local", P.DIRETOR_FINANCEIRO)
    # SUPERVISOR_OBRA e não GESTOR_OBRA: o gestor está em VE_TUDO — ele
    # enxerga todas as obras da empresa, por decisão antiga do cadastro de
    # perfis. Quem é preso às obras designadas é o supervisor (sempre) e o
    # administrativo/lançador (quando marcado assim no cadastro).
    da_creche = pessoa("Supervisor da creche", "sup.custo@teste.bws.local",
                       P.SUPERVISOR_OBRA, obra=creche)

    def titulo(sp, obra, conta_plano, valor, *, pago=None,
               especie=EspecieTitulo.PAGAR, status=StatusTitulo.APROVADO):
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao="lançamento do teste",
                   valor_bruto=valor, valor_liquido=valor,
                   competencia=date(2026, 9, 1), categoria_id=conta_plano.id,
                   forma_pagamento=FormaPagamento.PIX, status=status,
                   especie=especie, solicitante_id=diretor.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=valor,
                     percentual=Decimal("100.0000"), categoria_id=conta_plano.id))
        p = Parcela(titulo_id=t.id, numero=1, valor=valor,
                    vencimento=date(2026, 10, 10), status=StatusParcela.ABERTA)
        s.add(p)
        s.flush()
        if pago:
            s.add(Pagamento(parcela_id=p.id, conta_bancaria_id=conta.id,
                            data_pagamento=date(2026, 10, 10),
                            valor_pago=Decimal(str(pago)),
                            meio=FormaPagamento.PIX))
            p.status = (StatusParcela.PAGA if Decimal(str(pago)) >= valor
                        else StatusParcela.ABERTA)
            s.flush()
        return t

    # Creche: 10.000 de custo, 4.000 já pagos
    titulo("SP8001", creche, custo, Decimal("10000"), pago="4000")
    # Creche: 7.000 de TRANSFERÊNCIA — não é custo, é dinheiro mudando de lugar
    titulo("SP8002", creche, fluxo, Decimal("7000"))
    # Creche: 50.000 a RECEBER — não é custo
    titulo("SP8003", creche, receita, Decimal("50000"),
           especie=EspecieTitulo.RECEBER)
    # Creche: 3.000 CANCELADO — não compromete nada
    titulo("SP8004", creche, custo, Decimal("3000"),
           status=StatusTitulo.CANCELADO)
    # Escola: 2.000 de custo, nada pago
    titulo("SP8005", escola, custo, Decimal("2000"))
    s.commit()
    return {"diretor": diretor, "da_creche": da_creche,
            "creche": creche, "escola": escola}


def _linha(r, codigo):
    return next((l for l in r["linhas"] if codigo in l["obra"]), None)


# ---------------------------------------------------------------------------
# As duas visões
# ---------------------------------------------------------------------------
def test_as_duas_visoes_aparecem_juntas(obra_com_custo, sessao_real):
    """Mostrar uma só seria escolher pelo dono em silêncio — e a diferença
    entre elas é justamente o que ele ainda tem para pagar."""
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"],
                                obra="CRECHE")
    l = _linha(r, "CRECHE")
    assert l["comprometido"] == 10000.0
    assert l["executado"] == 4000.0
    assert l["a_executar"] == 6000.0


def test_a_frase_diz_os_dois_numeros(obra_com_custo, sessao_real):
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"],
                                obra="CRECHE")
    assert "comprometido" in r["frase"]
    assert "executado" in r["frase"]


# ---------------------------------------------------------------------------
# Só conta de DRE
# ---------------------------------------------------------------------------
def test_transferencia_entre_contas_NAO_e_custo(obra_com_custo, sessao_real):
    """Conta de FLUXO é dinheiro mudando de lugar. Somá-la inflaria o custo da
    obra em 7.000 sem que nada tivesse sido consumido."""
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"],
                                obra="CRECHE")
    assert _linha(r, "CRECHE")["comprometido"] == 10000.0, (
        "entrou conta de fluxo no custo da obra")


def test_o_que_a_obra_vai_receber_NAO_e_custo(obra_com_custo, sessao_real):
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"],
                                obra="CRECHE")
    assert _linha(r, "CRECHE")["comprometido"] == 10000.0


def test_titulo_cancelado_nao_compromete_nada(obra_com_custo, sessao_real):
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"],
                                obra="CRECHE")
    assert _linha(r, "CRECHE")["comprometido"] == 10000.0


# ---------------------------------------------------------------------------
# Escopo — a resposta muda conforme quem pergunta
# ---------------------------------------------------------------------------
def test_quem_e_preso_a_uma_obra_so_ve_a_dele(obra_com_custo, sessao_real):
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["da_creche"])
    assert [l["obra"] for l in r["linhas"]] == ["CRECHE · Creche do Eusébio"]


def test_obra_fora_do_alcance_responde_que_nao_achou(obra_com_custo, sessao_real):
    """Não diz "sem permissão": para quem pergunta, aquela obra simplesmente
    não existe."""
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["da_creche"],
                               obra="ESCOLA")
    assert r["linhas"] == []
    assert "não achei" in r["frase"].lower()


def test_sem_obra_traz_todas_as_que_alcanca(obra_com_custo, sessao_real):
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"])
    assert {l["obra"].split(" ·")[0] for l in r["linhas"]} == {"CRECHE", "ESCOLA"}


def test_a_resposta_explica_a_definicao(obra_com_custo, sessao_real):
    """Quem lê precisa saber o que está contando — senão o número volta a ser
    ambíguo na cabeça de quem recebe."""
    r = respostas.custo_da_obra(sessao_real, obra_com_custo["diretor"])
    obs = r["observacao"].lower()
    assert "comprometido" in obs and "executado" in obs
    assert "dre" in obs or "resultado" in obs
