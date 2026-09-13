"""Retenção de garantia e alçada por valor na empreita — com banco de verdade.

Duas coisas que estavam no roteiro desde o começo e ficaram para o fim.

A RETENÇÃO é o costume da construção: guarda-se uma parte de cada medição (na
BWS, 5%) e devolve-se no fim, quando o serviço passou pelo período de garantia.
O defeito que ela corrige é sempre o mesmo na planilha: retém-se direitinho por
doze medições e, no fim, ninguém sabe quanto ficou retido nem quando devolver.

A ALÇADA existe porque, antes dela, uma empreita de oitocentos reais e uma de
oitocentos mil passavam pela mesma porta.

O que se prova:

  1. A garantia sai de cada medição e o líquido a pagar já vem descontado.
  2. Ela incide sobre o MEDIDO, não sobre o líquido — senão encolheria
     justamente na medição que abate adiantamento.
  3. O valor retido fica GRAVADO na medição: mudar o percentual depois não
     reescreve o passado.
  4. A devolução vira TÍTULO A PAGAR, uma vez só, e antes do fim exige motivo.
  5. Quem aprova o contrato depende do VALOR dele, pela faixa mais estreita
     que o comporta.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroPermissao, ErroValidacao
from app.apps.erp.core.titulos import empreita as svc
from app.apps.erp.db.models.cadastros import (Categoria, Fornecedor, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario)
from app.apps.erp.db.models.financeiro import (ContratoMedicao, ContratoServico,
                                               EmpreitaAlcada, Titulo)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    if not s.query(EmpreitaAlcada).count():
        s.add_all([
            EmpreitaAlcada(valor_ate=Decimal("50000.00"),
                           perfis=["ADMIN", "DIRETOR_FINANCEIRO", "FINANCEIRO",
                                   "GESTOR_OBRA", "SUPERVISOR_OBRA"],
                           descricao="Empreita pequena"),
            EmpreitaAlcada(valor_ate=Decimal("200000.00"),
                           perfis=["ADMIN", "DIRETOR_FINANCEIRO", "FINANCEIRO",
                                   "GESTOR_OBRA"],
                           descricao="Empreita média"),
            EmpreitaAlcada(valor_ate=None, perfis=["ADMIN", "DIRETOR_FINANCEIRO"],
                           descricao="Empreita grande: só direção"),
        ])
        s.flush()
    quem = Usuario(nome="Gestor", email="empreita-gestor@teste.local", ativo=True,
                   senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.GESTOR_OBRA)
    supervisor = Usuario(nome="Supervisor", email="empreita-sup@teste.local",
                         ativo=True, senha_hash=gerar_hash("senha-de-teste-123"),
                         perfil=P.SUPERVISOR_OBRA)
    diretor = Usuario(nome="Diretor", email="empreita-dir@teste.local", ativo=True,
                      senha_hash=gerar_hash("senha-de-teste-123"),
                      perfil=P.DIRETOR_FINANCEIRO)
    prestador = Fornecedor(razao_social="Empreiteiro Silva ME", cnpj_cpf="22333444000155",
                           tipo_pessoa=TipoPessoa.PJ, ativo=True,
                           regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="ESCPLANALTO", nome="Escola Planalto")
    cat = Categoria(codigo="3.1.05", descricao="Mão de obra de terceiros")
    s.add_all([quem, supervisor, diretor, prestador, obra, cat])
    s.flush()
    return {"s": s, "gestor": quem, "supervisor": supervisor, "diretor": diretor,
            "prestador": prestador, "obra": obra, "categoria": cat}


def _concluir(cenario, contrato):
    contrato.status = "CONCLUIDO"
    cenario["s"].flush()
    return contrato


def _conta_homologada(cenario):
    """O prestador precisa de conta homologada para receber por Pix — dado
    bancário vive no cadastro, e a devolução da garantia não é exceção."""
    from app.apps.erp.db.models.cadastros import (FormaPagamento, FornecedorConta,
                                                  StatusConta)
    s = cenario["s"]
    conta = FornecedorConta(fornecedor_id=cenario["prestador"].id,
                            forma=FormaPagamento.PIX, pix_tipo="CNPJ",
                            pix_chave="22333444000155",
                            titular_nome="Empreiteiro Silva ME",
                            titular_doc="22333444000155",
                            status=StatusConta.HOMOLOGADA)
    s.add(conta)
    s.flush()
    return conta


def _contrato(cenario, *, valor="100000.00", pct="5.00", status="VIGENTE",
              criado_por=None):
    s = cenario["s"]
    c = ContratoServico(
        numero=svc.proximo_numero(s), obra_id=cenario["obra"].id,
        fornecedor_id=cenario["prestador"].id, categoria_id=cenario["categoria"].id,
        objeto="Alvenaria de vedação", modo="MEDICAO",
        valor_total=Decimal(valor), valor_aditivos=Decimal("0.00"),
        retencao_garantia_pct=Decimal(pct), exige_foto=False, status=status,
        criado_por=(criado_por or cenario["supervisor"]).id)
    s.add(c)
    s.flush()
    return c


def _medir(cenario, contrato, valor="20000.00", inicio=date(2026, 8, 1),
           fim=date(2026, 8, 31)):
    return svc.registrar_medicao(cenario["s"], contrato.id, {
        "valor_medido": valor, "periodo_inicio": inicio.isoformat(),
        "periodo_fim": fim.isoformat()}, cenario["supervisor"])


# ---------------------------------------------------------------------------
# 1 e 2. A retenção sai de cada medição
# ---------------------------------------------------------------------------
def test_a_garantia_sai_de_cada_medicao(cenario):
    c = _contrato(cenario, pct="5.00")
    m = _medir(cenario, c, "20000.00")
    assert m.valor_medido == Decimal("20000.00")
    assert m.valor_retido == Decimal("1000.00"), "5% de 20.000"
    assert m.valor_liquido == Decimal("19000.00"), "o que vai virar título"


def test_contrato_sem_retencao_nao_retem_nada(cenario):
    c = _contrato(cenario, pct="0")
    m = _medir(cenario, c, "20000.00")
    assert m.valor_retido == Decimal("0.00")
    assert m.valor_liquido == Decimal("20000.00")


def test_a_garantia_incide_sobre_o_medido_nao_sobre_o_liquido(cenario):
    """Se incidisse sobre o líquido, ela encolheria justamente na medição que
    abate adiantamento — e no fim do contrato faltaria garantia."""
    s = cenario["s"]
    c = _contrato(cenario, pct="5.00")
    # um adiantamento de 5.000 já pago
    from app.apps.erp.core.titulos.service import proximo_numero_sp
    from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                                   StatusTitulo, TipoTitulo)
    s.add(Titulo(numero_sp=proximo_numero_sp(s), tipo=TipoTitulo.T5_EMPREITEIRO,
                 especie=EspecieTitulo.PAGAR, fornecedor_id=cenario["prestador"].id,
                 descricao="Adiantamento do contrato", valor_bruto=Decimal("5000.00"),
                 valor_liquido=Decimal("5000.00"), competencia=date(2026, 8, 1),
                 categoria_id=cenario["categoria"].id,
                 forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
                 solicitante_id=cenario["gestor"].id, contrato_servico_id=c.id,
                 adiantamento_contrato=True))
    s.flush()

    m = _medir(cenario, c, "20000.00")
    assert m.valor_adiantamento_abatido == Decimal("5000.00")
    assert m.valor_retido == Decimal("1000.00"), "5% dos 20.000 medidos, não dos 15.000"
    assert m.valor_liquido == Decimal("14000.00"), "20.000 − 5.000 − 1.000"


def test_medicao_que_ficaria_negativa_e_recusada(cenario):
    s = cenario["s"]
    c = _contrato(cenario, pct="5.00")
    from app.apps.erp.core.titulos.service import proximo_numero_sp
    from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                                   StatusTitulo, TipoTitulo)
    s.add(Titulo(numero_sp=proximo_numero_sp(s), tipo=TipoTitulo.T5_EMPREITEIRO,
                 especie=EspecieTitulo.PAGAR, fornecedor_id=cenario["prestador"].id,
                 descricao="Adiantamento grande", valor_bruto=Decimal("20000.00"),
                 valor_liquido=Decimal("20000.00"), competencia=date(2026, 8, 1),
                 categoria_id=cenario["categoria"].id,
                 forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
                 solicitante_id=cenario["gestor"].id, contrato_servico_id=c.id,
                 adiantamento_contrato=True))
    s.flush()
    with pytest.raises(ErroValidacao) as e:
        _medir(cenario, c, "20000.00")
    assert "ficaria negativa" in str(e.value)


def test_percentual_absurdo_e_recusado_em_portugues(cenario):
    """Quase sempre é vírgula no lugar errado."""
    with pytest.raises(ErroValidacao) as e:
        svc._pct_garantia("50,5")
    assert "não faz sentido" in str(e.value)
    assert "5%" in str(e.value), "o erro diz qual é o usual"


# ---------------------------------------------------------------------------
# 3. O valor retido é gravado, não recalculado
# ---------------------------------------------------------------------------
def test_mudar_o_percentual_nao_reescreve_o_passado(cenario):
    """Aditivo que muda o percentual não pode mudar a conta de março."""
    s = cenario["s"]
    c = _contrato(cenario, pct="5.00")
    _medir(cenario, c, "20000.00")

    c.retencao_garantia_pct = Decimal("10.00")
    s.flush()
    _medir(cenario, c, "20000.00", inicio=date(2026, 9, 1), fim=date(2026, 9, 30))

    retidos = [m.valor_retido for m in s.query(ContratoMedicao)
               .filter_by(contrato_id=c.id).order_by(ContratoMedicao.numero).all()]
    assert retidos == [Decimal("1000.00"), Decimal("2000.00")]
    assert svc._retido(s, c.id) == Decimal("3000.00")


def test_o_saldo_mostra_quanto_esta_retido(cenario):
    c = _contrato(cenario, pct="5.00")
    _medir(cenario, c, "20000.00")
    est = svc.saldo(cenario["s"], c.id)
    assert est["retido"] == 1000.00
    assert est["retencao_pct"] == 5.0
    assert est["retencao_liberada_em"] is None


def test_medicao_cancelada_nao_conta_na_garantia(cenario):
    s = cenario["s"]
    c = _contrato(cenario, pct="5.00")
    m = _medir(cenario, c, "20000.00")
    m.status = "CANCELADA"
    s.flush()
    assert svc._retido(s, c.id) == Decimal("0.00")


# ---------------------------------------------------------------------------
# 4. A devolução
# ---------------------------------------------------------------------------
def test_a_devolucao_vira_titulo_a_pagar(cenario):
    """É dinheiro saindo: passa pela mesma aprovação, baixa e comprovante de
    qualquer pagamento. Um 'acerto' fora desse caminho não deixaria rastro."""
    s = cenario["s"]
    _conta_homologada(cenario)
    c = _contrato(cenario, pct="5.00")
    _medir(cenario, c, "20000.00")
    _medir(cenario, c, "30000.00", inicio=date(2026, 9, 1), fim=date(2026, 9, 30))
    _concluir(cenario, c)

    r = svc.liberar_garantia(s, c.id, cenario["diretor"])
    assert r["valor"] == 2500.00, "5% de 50.000"

    titulo = s.get(Titulo, r["titulo_id"])
    assert titulo.valor_liquido == Decimal("2500.00")
    assert titulo.contrato_servico_id == c.id
    assert c.retencao_liberada_em == date.today()
    assert c.retencao_liberada_por == cenario["diretor"].id


def test_a_garantia_nao_se_libera_duas_vezes(cenario):
    """Liberar de novo pagaria duas vezes."""
    s = cenario["s"]
    _conta_homologada(cenario)
    c = _contrato(cenario, pct="5.00")
    _medir(cenario, c, "20000.00")
    _concluir(cenario, c)
    svc.liberar_garantia(s, c.id, cenario["diretor"])
    with pytest.raises(ErroValidacao) as e:
        svc.liberar_garantia(s, c.id, cenario["diretor"])
    assert "já foi liberada" in str(e.value)


def test_liberar_antes_do_fim_exige_motivo(cenario):
    """É o caso em que alguém vai perguntar por quê, meses depois."""
    s = cenario["s"]
    _conta_homologada(cenario)
    c = _contrato(cenario, pct="5.00", status="VIGENTE")
    _medir(cenario, c, "20000.00")
    with pytest.raises(ErroValidacao) as e:
        svc.liberar_garantia(s, c.id, cenario["diretor"])
    assert "ainda está VIGENTE" in str(e.value)

    r = svc.liberar_garantia(s, c.id, cenario["diretor"],
                             motivo="Acordo com o prestador, autorizado pela diretoria.")
    assert r["valor"] == 1000.00
    assert c.retencao_motivo.startswith("Acordo")


def test_contrato_sem_retencao_nao_tem_o_que_liberar(cenario):
    s = cenario["s"]
    c = _contrato(cenario, pct="0")
    _medir(cenario, c, "20000.00")
    _concluir(cenario, c)
    with pytest.raises(ErroValidacao) as e:
        svc.liberar_garantia(s, c.id, cenario["diretor"])
    assert "Não há garantia retida" in str(e.value)


def test_a_lista_diz_de_quem_a_bws_esta_com_dinheiro(cenario):
    """A pergunta que a planilha não responde."""
    s = cenario["s"]
    _conta_homologada(cenario)
    aberto = _contrato(cenario, pct="5.00")
    _medir(cenario, aberto, "20000.00")
    _concluir(cenario, aberto)
    andando = _contrato(cenario, pct="5.00", status="VIGENTE")
    _medir(cenario, andando, "10000.00")

    lista = svc.garantias_a_liberar(s)
    assert len(lista) == 2
    assert lista[0]["pode_liberar"] is True, "o concluído vem primeiro"
    assert lista[0]["retido"] == 1000.00
    assert lista[0]["fornecedor"] == "Empreiteiro Silva ME"
    assert lista[1]["pode_liberar"] is False

    svc.liberar_garantia(s, aberto.id, cenario["diretor"])
    assert len(svc.garantias_a_liberar(s)) == 1, "o liberado sai da lista"


# ---------------------------------------------------------------------------
# 5. A alçada por valor
# ---------------------------------------------------------------------------
def test_a_faixa_escolhida_e_a_mais_estreita_que_comporta(cenario):
    """Sem isso, um contrato de 300 mil entraria na faixa dos 50 mil só porque
    ela aparece primeiro."""
    s = cenario["s"]
    assert "SUPERVISOR_OBRA" in svc.alcada_de(s, "10000")["perfis"]
    assert "SUPERVISOR_OBRA" not in svc.alcada_de(s, "80000")["perfis"]
    assert svc.alcada_de(s, "80000")["valor_ate"] == 200000.00
    assert svc.alcada_de(s, "500000")["perfis"] == ["ADMIN", "DIRETOR_FINANCEIRO"]


def test_o_supervisor_aprova_a_empreita_pequena(cenario):
    s = cenario["s"]
    c = _contrato(cenario, valor="20000.00", status="AGUARDANDO_AVAL",
                  criado_por=cenario["gestor"])
    svc.aprovar_contrato(s, c.id, cenario["supervisor"])
    assert c.status == "VIGENTE"


def test_o_supervisor_nao_aprova_a_empreita_grande(cenario):
    """Uma empreita de oitocentos reais e uma de oitocentos mil não podem
    passar pela mesma porta."""
    s = cenario["s"]
    c = _contrato(cenario, valor="300000.00", status="AGUARDANDO_AVAL",
                  criado_por=cenario["gestor"])
    with pytest.raises(ErroPermissao) as e:
        svc.aprovar_contrato(s, c.id, cenario["supervisor"])
    assert "quem aprova é" in str(e.value)
    assert "DIRETOR_FINANCEIRO" in str(e.value)

    svc.aprovar_contrato(s, c.id, cenario["diretor"])
    assert c.status == "VIGENTE"


def test_o_aditivo_entra_na_conta_da_alcada(cenario):
    """Contrato de 40 mil aditivado para 60 mil sai da faixa do supervisor —
    senão bastaria cadastrar pequeno e aditivar depois."""
    s = cenario["s"]
    c = _contrato(cenario, valor="40000.00", status="AGUARDANDO_AVAL",
                  criado_por=cenario["gestor"])
    c.valor_aditivos = Decimal("20000.00")
    s.flush()
    with pytest.raises(ErroPermissao):
        svc.aprovar_contrato(s, c.id, cenario["supervisor"])


def test_quem_cadastra_continua_nao_aprovando(cenario):
    s = cenario["s"]
    c = _contrato(cenario, valor="20000.00", status="AGUARDANDO_AVAL",
                  criado_por=cenario["supervisor"])
    with pytest.raises(ErroPermissao) as e:
        svc.aprovar_contrato(s, c.id, cenario["supervisor"])
    assert "não é quem o aprova" in str(e.value)


def test_sem_faixas_cadastradas_vale_a_regra_antiga(cenario, monkeypatch):
    """Migração não aplicada não pode parar a operação: o contrário —
    recusar tudo — quebraria a empreita por causa de um botão."""
    s = cenario["s"]
    monkeypatch.setattr(svc, "listar_alcadas", lambda sessao: [])
    s.query(EmpreitaAlcada).delete()
    s.flush()
    faixa = svc.alcada_de(s, "999999")
    assert "SUPERVISOR_OBRA" in faixa["perfis"]
