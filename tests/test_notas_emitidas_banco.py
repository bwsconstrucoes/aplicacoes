"""A tela de controle das notas emitidas — com banco de verdade.

O dono descreveu esta tela coluna por coluna, e disse para que ela serve:
*"às vezes a contabilidade precisa gerar um relatório das informações — valor
da nota, tributos e tal."*

Ele mesmo pediu a crítica sobre ela ser ou não a tela de títulos a receber:
*"talvez isso seja a mesma coisa que o título a receber, ou não, não sei. Aí
você vai fazer essa crítica."* Não é — e os testes abaixo mostram por quê: uma
medição vira DUAS notas, e uma nota é cancelada sem o título mudar nada.

O que se prova:

  1. Cada tributo tem SUA coluna (ISS, IR, INSS, PIS, COFINS, CSLL) — somar
     tudo estragaria o relatório da contabilidade.
  2. Os totais contam só o que está EMITIDO: nota cancelada somada com nota
     válida é como um relatório fiscal começa a mentir.
  3. Uma medição pode virar duas notas, e as duas aparecem.
  4. Nota registrada do portal fecha o buraco na numeração.
  5. Cancelar exige motivo e não devolve o número para a fila.
  6. A ordenação é decrescente — quem abre quer a última nota.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.notas_emitidas import listagem as svc
from app.apps.erp.core.notas_emitidas import numeracao
from app.apps.erp.db.models.cadastros import (Categoria, ContaBancaria, Empresa,
                                              FormaPagamento as FormaConta,
                                              Fornecedor, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               NotaEmitida, Pagamento, Parcela,
                                               Rateio, StatusParcela,
                                               StatusTitulo, TipoTitulo, Titulo)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    empresa = Empresa(razao_social="BWS Construções LTDA", nome_fantasia="BWS",
                      cnpj="11222333000181", emissao_ambiente="PRODUCAO",
                      emissao_serie="1", emissao_modo="MANUAL")
    cliente = Fornecedor(razao_social="Prefeitura Exemplo", cnpj_cpf="11111111000191",
                         tipo_pessoa=TipoPessoa.PJ, ativo=True,
                         regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    cat = Categoria(codigo="1.1.01", descricao="Receita de medição")
    conta = ContaBancaria(descricao="Bradesco 1234-5", banco_codigo="237",
                          agencia="1234", conta="56789-0", ativo=True)
    u = Usuario(nome="Financeiro", email="notas@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    s.add_all([empresa, cliente, obra, cat, conta, u])
    s.flush()
    return {"s": s, "empresa": empresa, "cliente": cliente, "obra": obra,
            "categoria": cat, "conta": conta, "usuario": u}


def _titulo(cenario, numero="1", valor="100000.00"):
    s = cenario["s"]
    t = Titulo(numero_sp=f"REC-{numero}", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.RECEBER, fornecedor_id=cenario["cliente"].id,
               descricao=f"Medição {numero}", valor_bruto=Decimal(valor),
               valor_liquido=Decimal(valor), competencia=date(2026, 8, 1),
               categoria_id=cenario["categoria"].id,
               forma_pagamento=FormaPagamento.BOLETO, status=StatusTitulo.APROVADO,
               solicitante_id=cenario["usuario"].id, numero_medicao=numero,
               periodo_inicio=date(2026, 8, 1), periodo_fim=date(2026, 8, 31))
    s.add(t)
    s.flush()
    s.add(Rateio(titulo_id=t.id, obra_id=cenario["obra"].id,
                 valor=Decimal(valor), percentual=Decimal("100")))
    s.flush()
    return t


# ---------------------------------------------------------------------------
# 1. Cada tributo na sua coluna
# ---------------------------------------------------------------------------
def test_cada_tributo_tem_sua_coluna(cenario):
    """A contabilidade lança ISS numa conta e INSS em outra. Do total somado
    ninguém volta atrás."""
    s = cenario["s"]
    t = _titulo(cenario)
    svc.registrar_manual(
        s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
        numero_nota="1201", emissao=date(2026, 9, 1),
        valor_bruto=Decimal("100000.00"),
        retencoes={"iss": "5000.00", "ir": "1500.00", "inss": "3500.00",
                   "pis": "650.00", "cofins": "3000.00", "csll": "1000.00"},
        usuario=cenario["usuario"])

    linha = svc.listar(s)["notas"][0]
    assert linha["retencoes"]["iss"] == 5000.00
    assert linha["retencoes"]["inss"] == 3500.00
    assert linha["retencoes"]["csll"] == 1000.00
    assert linha["retido"] == 14650.00
    assert linha["valor_liquido"] == 85350.00, "líquido é bruto menos o retido"


def test_o_relatorio_traz_o_total_por_tributo(cenario):
    s = cenario["s"]
    for n, iss in (("1", "5000.00"), ("2", "2500.00")):
        t = _titulo(cenario, n)
        svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                             numero_nota=f"120{n}", valor_bruto=Decimal("100000.00"),
                             retencoes={"iss": iss}, usuario=cenario["usuario"])
    resumo = svc.listar(s)["resumo"]
    assert resumo["por_tributo"]["iss"] == 7500.00
    assert resumo["retido"] == 7500.00


# ---------------------------------------------------------------------------
# 2. Cancelada fica FORA dos totais
# ---------------------------------------------------------------------------
def test_nota_cancelada_nao_entra_nos_totais(cenario):
    """Somar cancelada com válida é como um relatório fiscal começa a mentir."""
    s = cenario["s"]
    t = _titulo(cenario)
    boa = svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                               numero_nota="1201", valor_bruto=Decimal("100000.00"),
                               usuario=cenario["usuario"])
    ruim = svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                                numero_nota="1202", valor_bruto=Decimal("100000.00"),
                                usuario=cenario["usuario"])
    numeracao.cancelar(s, ruim.id, motivo="Emitida em duplicidade pelo portal.",
                       usuario=cenario["usuario"])

    d = svc.listar(s)
    assert d["resumo"]["quantidade"] == 2, "as duas continuam aparecendo"
    assert d["resumo"]["emitidas"] == 1
    assert d["resumo"]["canceladas"] == 1
    assert d["resumo"]["bruto"] == 100000.00, "só a válida soma"
    assert boa.situacao == "EMITIDA"


def test_cancelar_exige_motivo(cenario):
    s = cenario["s"]
    t = _titulo(cenario)
    nota = svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                                numero_nota="1201", usuario=cenario["usuario"])
    with pytest.raises(ErroValidacao):
        numeracao.cancelar(s, nota.id, motivo="  ")


def test_cancelar_nao_devolve_o_numero_para_a_fila(cenario):
    """A prefeitura pode ter recebido a declaração; reusar o número daria
    duplicidade do lado dela."""
    s = cenario["s"]
    t = _titulo(cenario)
    nota = svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                                numero_nota="1201", usuario=cenario["usuario"])
    queimado = nota.numero_dps
    numeracao.cancelar(s, nota.id, motivo="Valor errado.", usuario=cenario["usuario"])

    seguinte = numeracao.reservar(s, cenario["empresa"], titulo_id=t.id,
                                  usuario=cenario["usuario"])
    assert seguinte.numero_dps == queimado + 1


# ---------------------------------------------------------------------------
# 3. Uma medição, duas notas — a razão de a tela existir
# ---------------------------------------------------------------------------
def test_uma_medicao_pode_virar_duas_notas(cenario):
    """É o que separa esta tela da de títulos a receber: o título é um só, e
    os documentos fiscais são dois."""
    s = cenario["s"]
    t = _titulo(cenario, valor="100000.00")
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", valor_bruto=Decimal("60000.00"),
                         usuario=cenario["usuario"])
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1202", valor_bruto=Decimal("40000.00"),
                         usuario=cenario["usuario"])

    d = svc.listar(s)
    assert d["resumo"]["emitidas"] == 2
    assert d["resumo"]["bruto"] == 100000.00
    assert {l["numero_sp"] for l in d["notas"]} == {"REC-1"}, "o título é o mesmo"


def test_a_nota_herda_a_obra_do_rateio(cenario):
    s = cenario["s"]
    t = _titulo(cenario)
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", usuario=cenario["usuario"])
    assert svc.listar(s)["notas"][0]["obra"] == "ESCPLANALTO"


# ---------------------------------------------------------------------------
# 4. O recebimento vem do TÍTULO
# ---------------------------------------------------------------------------
def test_o_recebimento_aparece_com_data_e_conta(cenario):
    s = cenario["s"]
    t = _titulo(cenario)
    p = Parcela(titulo_id=t.id, numero=1, vencimento=date(2026, 9, 10),
                valor=Decimal("100000.00"), status=StatusParcela.PAGA)
    s.add(p)
    s.flush()
    s.add(Pagamento(parcela_id=p.id, conta_bancaria_id=cenario["conta"].id,
                    valor_pago=Decimal("100000.00"), meio=FormaConta.TED,
                    data_pagamento=date(2026, 9, 20)))
    s.flush()
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", valor_bruto=Decimal("100000.00"),
                         usuario=cenario["usuario"])

    linha = svc.listar(s)["notas"][0]
    assert linha["recebimento"]["valor"] == 100000.00
    assert linha["recebimento"]["em"] == "2026-09-20"
    assert linha["recebimento"]["conta"] == "Bradesco 1234-5"


# ---------------------------------------------------------------------------
# 5. Registrar do portal fecha o buraco
# ---------------------------------------------------------------------------
def test_registrar_a_nota_do_portal_fecha_a_numeracao(cenario):
    """Buraco na sequência é a pergunta clássica da fiscalização."""
    s = cenario["s"]
    t = _titulo(cenario)
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", usuario=cenario["usuario"])
    conf = numeracao.conferir(s, cenario["empresa"])
    assert conf["integra"] is True
    assert conf["proximo"] == 2


def test_a_mesma_nota_registrada_duas_vezes_e_recusada(cenario):
    s = cenario["s"]
    t = _titulo(cenario)
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", usuario=cenario["usuario"])
    with pytest.raises(ErroValidacao):
        svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                             numero_nota="1201", usuario=cenario["usuario"])


def test_nota_sem_numero_e_recusada(cenario):
    s = cenario["s"]
    with pytest.raises(ErroValidacao):
        svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=None,
                             numero_nota="   ", usuario=cenario["usuario"])


def test_emissao_no_futuro_e_recusada(cenario):
    s = cenario["s"]
    with pytest.raises(ErroValidacao):
        svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=None,
                             numero_nota="1201", emissao=date(2099, 1, 1),
                             usuario=cenario["usuario"])


# ---------------------------------------------------------------------------
# 6. A ordenação e os filtros
# ---------------------------------------------------------------------------
def test_a_lista_vem_da_mais_recente_para_a_mais_antiga(cenario):
    """Quem abre a tela quer a última nota, não a primeira de 2019."""
    s = cenario["s"]
    t = _titulo(cenario)
    for n in ("1201", "1202", "1203"):
        svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                             numero_nota=n, usuario=cenario["usuario"])
    numeros = [l["numero_dps"] for l in svc.listar(s)["notas"]]
    assert numeros == sorted(numeros, reverse=True)


def test_o_filtro_por_situacao_funciona(cenario):
    s = cenario["s"]
    t = _titulo(cenario)
    nota = svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                                numero_nota="1201", usuario=cenario["usuario"])
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1202", usuario=cenario["usuario"])
    numeracao.cancelar(s, nota.id, motivo="Erro no valor.", usuario=cenario["usuario"])

    assert len(svc.listar(s, situacao="EMITIDA")["notas"]) == 1
    assert len(svc.listar(s, situacao="CANCELADA")["notas"]) == 1


def test_a_busca_acha_pelo_numero_e_pelo_cliente(cenario):
    s = cenario["s"]
    t = _titulo(cenario)
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", usuario=cenario["usuario"])
    assert len(svc.listar(s, busca="1201")["notas"]) == 1
    assert len(svc.listar(s, busca="prefeitura")["notas"]) == 1
    assert len(svc.listar(s, busca="não existe")["notas"]) == 0


def test_homologacao_aparece_marcada(cenario):
    """Nota de teste não vale como nota, e a tela tem de dizer isso."""
    s = cenario["s"]
    cenario["empresa"].emissao_ambiente = "HOMOLOGACAO"
    s.flush()
    t = _titulo(cenario)
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="TESTE-1", usuario=cenario["usuario"])
    d = svc.listar(s)
    assert d["resumo"]["em_homologacao"] == 1
    assert d["notas"][0]["ambiente"] == "HOMOLOGACAO"


def test_empresa_inexistente_da_erro_claro(cenario):
    with pytest.raises(ErroValidacao):
        svc.registrar_manual(cenario["s"], empresa_id=999999, titulo_id=None,
                             numero_nota="1201")


def test_a_recusa_da_nota_repetida_nao_apaga_a_nota_anterior(cenario):
    """A recusa desfaz SÓ a gravação recusada.

    Um `rollback()` inteiro levaria junto o que estivesse pendente na mesma
    transação — e a tela devolveria erro tendo apagado trabalho que ninguém
    pediu para apagar.
    """
    s = cenario["s"]
    t = _titulo(cenario)
    svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                         numero_nota="1201", usuario=cenario["usuario"])
    with pytest.raises(ErroValidacao):
        svc.registrar_manual(s, empresa_id=cenario["empresa"].id, titulo_id=t.id,
                             numero_nota="1201", usuario=cenario["usuario"])

    d = svc.listar(s)
    emitidas = [l for l in d["notas"] if l["situacao"] == "EMITIDA"]
    assert [l["numero_nota"] for l in emitidas] == ["1201"], \
        "a primeira nota continua registrada depois da recusa da segunda"
