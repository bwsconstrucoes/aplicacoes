"""A EMPRESA DONA DA CONTA, E A EMPRESA QUE PAGA O TÍTULO.

Pedido do dono em 22/09/2026, olhando a tela de contas bancárias: *"uma coisa
que acho que precisa fazer, ou não? Associar banco a uma empresa."*

Ele achou um buraco real. A conta bancária era uma lista solta — descrição,
banco, agência, conta — e não pertencia a empresa nenhuma. Em toda tela que
escolhe conta apareciam as contas de TODOS os CNPJs misturadas, e nada impedia
pagar a despesa de uma empresa pelo caixa de outra. Esse erro não se conserta
na tela: o dinheiro saiu do CNPJ errado, vira acerto entre empresas e passa por
movimentação bancária de verdade.

E o título também não sabia de que empresa era — a empresa só existia
indiretamente, pela obra do rateio. Sem os dois lados não há o que comparar.

A LINHA QUE ESTES TESTES DEFENDEM, e ela é sutil:

  * AVISAR, NÃO BLOQUEAR. Pagar por outra empresa acontece de propósito — é
    empréstimo entre elas, e precisa ficar REGISTRADO como tal, não escondido
    nem impedido. Travar de saída faria a pessoa pagar por fora do sistema, e
    aí o ERP não saberia de nada.
  * Falta de cadastro não inventa conferência: sem empresa dos dois lados, não
    há o que comparar, e o pagamento segue.
"""
from __future__ import annotations

import json

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.pagamentos import service as svc_pag
from app.apps.erp.core.titulos import service as svc_tit
from app.apps.erp.db.models.cadastros import (
    ContaBancaria, Empresa, FormaPagamento, Obra,
)
from app.apps.erp.db.models.financeiro import (
    Parcela, Rateio, StatusParcela, StatusTitulo, Titulo,
)

from conftest import SessaoFalsa, novo_usuario


def _cenario(*, empresa_titulo=1, empresa_conta=1, status=StatusTitulo.APROVADO):
    """Um título aprovado, uma parcela aberta e uma conta para pagar."""
    bws = Empresa(id=1, razao_social="BWS Construções LTDA", cnpj="11222333000181")
    outra = Empresa(id=2, razao_social="Segunda Construtora LTDA", cnpj="44555666000199")
    conta = ContaBancaria(id=9, descricao="Bradesco 1234-5", banco_codigo="237",
                          agencia="1234", conta="56789-0", ativo=True,
                          empresa_id=empresa_conta)
    titulo = Titulo(id=70, numero_sp="SP-0070", descricao="Cimento da fundação",
                    fornecedor_id=5, categoria_id=3,
                    valor_bruto=Decimal("1000.00"), valor_retencoes=Decimal("0.00"),
                    valor_liquido=Decimal("1000.00"), competencia=date(2026, 9, 1),
                    forma_pagamento=FormaPagamento.PIX, status=status,
                    empresa_id=empresa_titulo)
    parcela = Parcela(id=700, titulo_id=70, numero=1, valor=Decimal("1000.00"),
                      vencimento=date(2026, 9, 30), status=StatusParcela.ABERTA)
    parcela.titulo = titulo
    titulo.parcelas = [parcela]
    return SessaoFalsa(bws, outra, conta, titulo, parcela)


def _baixar(s, **extra):
    return svc_pag.registrar_pagamento(
        s, parcela_id=700, conta_bancaria_id=9,
        data_pagamento=date(2026, 9, 25),
        usuario=novo_usuario(1, "ADMIN"), **extra)


# ---------------------------------------------------------------------------
# A conferência na hora de pagar
# ---------------------------------------------------------------------------
def test_mesma_empresa_paga_sem_atrito():
    """O caso normal não pode ganhar um clique a mais — senão a trava vira
    incômodo e alguém desliga o sistema da rotina."""
    pg = _baixar(_cenario())
    assert pg.conta_bancaria_id == 9


def test_conta_de_outra_empresa_e_recusada_ate_alguem_confirmar():
    s = _cenario(empresa_titulo=1, empresa_conta=2)
    with pytest.raises(svc_pag.ErroEmpresaDiferente) as e:
        _baixar(s)
    recado = str(e.value)
    assert "BWS Construções LTDA" in recado, "tem de dizer de quem é o título"
    assert "Segunda Construtora LTDA" in recado, "e de quem é a conta"
    assert "acerto entre elas" in recado


def test_confirmado_o_pagamento_entra_E_FICA_ESCRITO_na_trilha():
    """É o ponto todo do desenho: pagar por outra empresa é permitido, e por
    isso mesmo precisa ficar registrado. Sem o registro, "saiu da conta errada"
    some no meio dos pagamentos normais."""
    s = _cenario(empresa_titulo=1, empresa_conta=2)
    pg = _baixar(s, confirmar_outra_empresa=True)
    assert pg.conta_bancaria_id == 9
    # o detalhe vai como JSON para a coluna `detalhe` da trilha
    detalhe = json.loads(s.eventos[-1]["dt"])
    assert "outra_empresa" in detalhe
    assert "Segunda Construtora LTDA" in detalhe["outra_empresa"]


def test_pagamento_normal_nao_polui_a_trilha_com_o_aviso():
    s = _cenario()
    _baixar(s)
    assert "outra_empresa" not in json.loads(s.eventos[-1]["dt"])


@pytest.mark.parametrize("no_titulo,na_conta", [(None, 2), (1, None), (None, None)])
def test_sem_cadastro_dos_dois_lados_nao_inventa_conferencia(no_titulo, na_conta):
    """Falta de cadastro é falta de cadastro — não é "empresa diferente". Barrar
    aqui pararia o financeiro por um campo em branco de outra tela."""
    pg = _baixar(_cenario(empresa_titulo=no_titulo, empresa_conta=na_conta))
    assert pg.conta_bancaria_id == 9


def test_a_confirmacao_nao_atropela_as_outras_travas():
    """Confirmar a empresa não pode virar um "sim" para tudo: parcela de título
    que ainda não está aprovado continua recusada."""
    s = _cenario(empresa_titulo=1, empresa_conta=2, status=StatusTitulo.EM_ANALISE)
    with pytest.raises(ErroValidacao) as e:
        _baixar(s, confirmar_outra_empresa=True)
    assert "APROVADO" in str(e.value)


# ---------------------------------------------------------------------------
# De que empresa é o título — deduzido pela obra do rateio
# ---------------------------------------------------------------------------
def _obras(*empresas):
    return [Obra(id=10 + i, codigo=f"OB-{i}", nome=f"Obra {i}", empresa_id=e,
                 status="ATIVA")
            for i, e in enumerate(empresas)]


def test_empresa_do_titulo_vem_da_obra_do_rateio():
    """O dono confirmou a premissa com todas as letras — *"sempre uma empresa
    só"* —, e é ela que torna a dedução honesta em vez de chute."""
    s = SessaoFalsa(*_obras(7, 7))
    rateios = [Rateio(obra_id=10, valor=Decimal("500")),
               Rateio(obra_id=11, valor=Decimal("500"))]
    assert svc_tit._empresa_do_rateio(s, rateios) == 7


def test_rateio_entre_empresas_diferentes_e_recusado_no_lancamento():
    """Um título vira UM pagamento, e ele sai do caixa de um CNPJ. A recusa é
    no lançamento de propósito: depois, separar envolve o fornecedor."""
    s = SessaoFalsa(Empresa(id=1, razao_social="BWS Construções LTDA", cnpj="11222333000181"),
                    Empresa(id=2, razao_social="Segunda Construtora LTDA", cnpj="44555666000199"),
                    *_obras(1, 2))
    rateios = [Rateio(obra_id=10, valor=Decimal("500")),
               Rateio(obra_id=11, valor=Decimal("500"))]
    with pytest.raises(ErroValidacao) as e:
        svc_tit._empresa_do_rateio(s, rateios)
    recado = str(e.value)
    assert "BWS Construções LTDA" in recado and "Segunda Construtora LTDA" in recado
    assert "paga por dois" in recado


def test_obra_sem_empresa_nao_trava_o_lancamento():
    """Travar o financeiro por um campo em branco de OUTRO cadastro é o tipo de
    rigor que faz a pessoa voltar para a planilha."""
    s = SessaoFalsa(*_obras(None, None))
    rateios = [Rateio(obra_id=10, valor=Decimal("500")),
               Rateio(obra_id=11, valor=Decimal("500"))]
    assert svc_tit._empresa_do_rateio(s, rateios) is None


def test_uma_obra_com_empresa_e_outra_sem_adota_a_que_existe():
    """Meia falta de cadastro não é conflito: há uma empresa só em jogo, e é
    ela. O contrário deixaria o título sem empresa por causa de um cadastro
    incompleto, e aí a conferência do pagamento nunca aconteceria."""
    s = SessaoFalsa(*_obras(7, None))
    rateios = [Rateio(obra_id=10, valor=Decimal("500")),
               Rateio(obra_id=11, valor=Decimal("500"))]
    assert svc_tit._empresa_do_rateio(s, rateios) == 7


# ---------------------------------------------------------------------------
# COM BANCO DE VERDADE — o que o dublê não alcança
#
# `listar` FILTRA por empresa desde 22/09/2026, e filtro vive no WHERE: a
# sessão dublada ignora WHERE e devolveria tudo, dando um verde falso.
# ---------------------------------------------------------------------------
@pytest.mark.banco
class TestComBanco:

    @pytest.fixture
    def duas_empresas(self, sessao_real):
        from app.apps.erp.core.cadastros import contas as svc_contas
        s = sessao_real
        a = Empresa(razao_social="Alfa Construções LTDA", nome_fantasia="Alfa",
                    cnpj="11222333000181")
        b = Empresa(razao_social="Beta Construções LTDA", nome_fantasia="Beta",
                    cnpj="44555666000199")
        s.add_all([a, b])
        s.flush()
        s.add_all([
            ContaBancaria(descricao="Bradesco Alfa", banco_codigo="237",
                          agencia="1111", conta="11111-1", empresa_id=a.id),
            ContaBancaria(descricao="Itaú Beta", banco_codigo="341",
                          agencia="2222", conta="22222-2", empresa_id=b.id),
            # A conta que nasceu ANTES da migração 080 — sem dona.
            ContaBancaria(descricao="Caixa antiga", banco_codigo="104",
                          agencia="3333", conta="33333-3"),
        ])
        s.flush()
        return {"s": s, "a": a, "b": b, "svc": svc_contas}

    def test_listar_por_empresa_traz_so_as_contas_dela(self, duas_empresas):
        c = duas_empresas
        so_alfa = c["svc"].listar(c["s"], empresa_id=c["a"].id)
        assert [x["descricao"] for x in so_alfa] == ["Bradesco Alfa"]

    def test_sem_empresa_a_lista_vem_inteira_e_diz_quem_esta_sem_dona(self, duas_empresas):
        """A conta antiga não some da lista — some seria pior: ninguém
        arrumaria o que não vê."""
        c = duas_empresas
        todas = c["svc"].listar(c["s"])
        assert len(todas) == 3
        orfa = [x for x in todas if not x["empresa_id"]]
        assert [x["descricao"] for x in orfa] == ["Caixa antiga"]

    def test_o_bloco_para_copiar_traz_a_empresa_DA_CONTA(self, duas_empresas):
        """ERA UM DEFEITO DE VERDADE, achado ao ligar a conta à empresa: o
        bloco trazia SEMPRE a razão social e o CNPJ da empresa padrão, em cima
        da agência e conta de qualquer uma. Mandar para um cliente o CNPJ de
        uma empresa com a conta de outra é justamente o que este bloco existe
        para evitar — e não aparece na conferência, porque os dois lados estão
        certos sozinhos."""
        c = duas_empresas
        por_nome = {x["descricao"]: x["texto_para_copiar"]
                    for x in c["svc"].listar(c["s"])}
        assert "Alfa Construções LTDA" in por_nome["Bradesco Alfa"]
        assert "Beta" not in por_nome["Bradesco Alfa"]
        assert "Beta Construções LTDA" in por_nome["Itaú Beta"]
        assert "Alfa" not in por_nome["Itaú Beta"]

    def test_conta_sem_dona_nao_inventa_cabecalho_de_empresa(self, duas_empresas):
        c = duas_empresas
        texto = [x for x in c["svc"].listar(c["s"])
                 if x["descricao"] == "Caixa antiga"][0]["texto_para_copiar"]
        assert "CNPJ:" not in texto, "sem dona, não empresta o CNPJ de ninguém"
        assert "Agência: 3333" in texto

    def test_conta_nova_sem_empresa_e_recusada(self, duas_empresas):
        c = duas_empresas
        with pytest.raises(ErroValidacao) as e:
            c["svc"].criar(c["s"], {"descricao": "Santander", "banco_codigo": "033",
                                    "agencia": "4444", "conta": "44444-4"})
        assert "empresa dona da conta" in str(e.value)

    def test_marcar_a_dona_de_uma_conta_antiga(self, duas_empresas):
        c = duas_empresas
        antiga = [x for x in c["svc"].listar(c["s"])
                  if x["descricao"] == "Caixa antiga"][0]
        c["svc"].definir_empresa(c["s"], antiga["id"], c["b"].id)
        depois = c["svc"].listar(c["s"], empresa_id=c["b"].id)
        assert sorted(x["descricao"] for x in depois) == ["Caixa antiga", "Itaú Beta"]
