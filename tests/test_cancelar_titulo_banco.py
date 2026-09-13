"""Cancelar título: quem lançou desfaz o próprio, e quem lançou é avisado.

DUAS DECISÕES DO DONO, em 12/09/2026:

  1. *"Liberado o lançamento que não está baixado ou conciliado"* — quem lançou
     cancela o PRÓPRIO lançamento sem depender do financeiro. Antes, corrigir o
     próprio engano exigia interromper outra pessoa, e o lançamento errado
     ficava no ar até alguém ter tempo.
  2. *"A pessoa que lançou vai receber aquela informação de que o título foi
     cancelado e qual o motivo"* — cancelamento em silêncio é pior que
     lançamento errado: o errado alguém corrige, o silencioso ninguém vê.

E UMA FALHA QUE APARECEU AO FAZER ISSO. A única trava era
`parcela.status == PAGA`. Parcela pode ter PAGAMENTO registrado sem estar
marcada PAGA — baixa parcial, baixa que o robô lançou e ninguém fechou. Nesses
casos o cancelamento passava, e o pagamento ficava pendurado num título
cancelado: dinheiro que saiu da conta e sumiu do relatório.

COM BANCO DE VERDADE porque o escopo vive no `WHERE` e a trava nova é uma
consulta a pagamentos e conciliações.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroPermissao, ErroValidacao)
from app.apps.erp.core.titulos import service as svc
from app.apps.erp.db.models.cadastros import (
    Categoria, ContaBancaria, Empresa, EscopoVisao, Fornecedor, Obra,
    PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    Conciliacao, Extrato, FormaPagamento, Pagamento, Parcela, Rateio,
    StatusParcela, StatusTitulo, TipoTitulo, Titulo,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    emp = Empresa(razao_social="BWS Construções Exemplo", cnpj="11222333000181")
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="2.1.01", descricao="Material")
    s.add_all([emp, creche, escola, forn, cat])
    s.flush()
    conta = ContaBancaria(descricao="Bradesco da obra", banco_codigo="237",
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

    lancou = pessoa("Ana do administrativo", "ana@teste.bws.local",
                    P.ADMINISTRATIVO_OBRA, obra=creche)
    outro = pessoa("Bruno do administrativo", "bruno@teste.bws.local",
                   P.ADMINISTRATIVO_OBRA, obra=creche)
    financeiro = pessoa("Carla do financeiro", "carla@teste.bws.local",
                        P.FINANCEIRO)

    def titulo(sp, obra, quem, status=StatusTitulo.EM_ANALISE):
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id, descricao="compra de areia",
                   valor_bruto=Decimal("500"), valor_liquido=Decimal("500"),
                   competencia=date(2026, 9, 1), categoria_id=cat.id,
                   forma_pagamento=FormaPagamento.PIX, status=status,
                   solicitante_id=quem.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal("500"),
                     percentual=Decimal("100.0000")))
        s.add(Parcela(titulo_id=t.id, numero=1, valor=Decimal("500"),
                      vencimento=date(2026, 10, 10), status=StatusParcela.ABERTA))
        s.flush()
        return t

    dados = {"lancou": lancou, "outro": outro, "financeiro": financeiro,
             "creche": creche, "escola": escola, "conta": conta,
             "da_ana": titulo("SP0001", creche, lancou),
             "do_bruno": titulo("SP0002", creche, outro),
             "de_outra_obra": titulo("SP0003", escola, outro)}
    s.commit()
    return dados


def _motivo():
    return "lancei duplicado por engano"


# ---------------------------------------------------------------------------
# 1. QUEM LANÇOU CANCELA O PRÓPRIO — o pedido do dono
# ---------------------------------------------------------------------------
def test_quem_lancou_cancela_o_proprio_lancamento(cenario, sessao_real):
    """Antes de 12/09/2026 isto era impossível: a rota exigia alçada de
    aprovar, e quem lançou tinha de interromper o financeiro."""
    t = svc.cancelar(sessao_real, cenario["da_ana"].id, _motivo(),
                     cenario["lancou"])
    assert t.status == StatusTitulo.CANCELADO
    assert all(p.status == StatusParcela.CANCELADA for p in t.parcelas)


def test_quem_lancou_NAO_cancela_o_lancamento_de_outra_pessoa(cenario, sessao_real):
    """Liberar o próprio não é liberar o dos outros. A recusa aqui é "sem
    permissão" e não "não encontrado" de propósito: o título é da mesma obra,
    a pessoa JÁ o vê na tela — esconder a existência dele não esconderia nada,
    e um recado claro evita que ela fique tentando."""
    with pytest.raises(ErroPermissao):
        svc.cancelar(sessao_real, cenario["do_bruno"].id, _motivo(),
                     cenario["lancou"])


def test_titulo_de_outra_obra_responde_como_inexistente(cenario, sessao_real):
    """Fora do recorte é "não encontrado", nunca "sem permissão" — dizer "sem
    permissão" confirmaria que aquele número existe."""
    with pytest.raises(ErroNaoEncontrado):
        svc.cancelar(sessao_real, cenario["de_outra_obra"].id, _motivo(),
                     cenario["lancou"])


def test_financeiro_cancela_o_lancamento_de_qualquer_um(cenario, sessao_real):
    t = svc.cancelar(sessao_real, cenario["do_bruno"].id, _motivo(),
                     cenario["financeiro"])
    assert t.status == StatusTitulo.CANCELADO


def test_motivo_continua_obrigatorio(cenario, sessao_real):
    with pytest.raises(ErroValidacao):
        svc.cancelar(sessao_real, cenario["da_ana"].id, "  ", cenario["lancou"])


# ---------------------------------------------------------------------------
# 2. O QUE O DINHEIRO JÁ TOCOU NÃO SE CANCELA
# ---------------------------------------------------------------------------
def _pagar(s, titulo, conta, *, marcar_paga: bool):
    p = titulo.parcelas[0]
    pg = Pagamento(parcela_id=p.id, conta_bancaria_id=conta.id,
                   data_pagamento=date(2026, 10, 10), valor_pago=Decimal("500"),
                   meio=FormaPagamento.PIX)
    s.add(pg)
    if marcar_paga:
        p.status = StatusParcela.PAGA
    s.flush()
    return pg


def test_titulo_com_parcela_paga_nao_cancela(cenario, sessao_real):
    _pagar(sessao_real, cenario["da_ana"], cenario["conta"], marcar_paga=True)
    with pytest.raises(ErroValidacao) as e:
        svc.cancelar(sessao_real, cenario["da_ana"].id, _motivo(),
                     cenario["financeiro"])
    assert "estorno" in str(e.value).lower()


def test_pagamento_registrado_SEM_a_parcela_marcada_paga_tambem_barra(cenario, sessao_real):
    """A FALHA QUE ESTE TESTE FECHA. A trava antiga olhava só
    `parcela.status == PAGA`. Baixa parcial, baixa em processamento ou baixa
    lançada pelo robô deixam o PAGAMENTO gravado com a parcela ainda ABERTA —
    e o cancelamento passava. O pagamento ficava pendurado num título que
    "não existe mais": dinheiro que saiu da conta e sumiu do relatório.
    """
    _pagar(sessao_real, cenario["da_ana"], cenario["conta"], marcar_paga=False)
    with pytest.raises(ErroValidacao) as e:
        svc.cancelar(sessao_real, cenario["da_ana"].id, _motivo(),
                     cenario["financeiro"])
    assert "pagamento" in str(e.value).lower()


def test_conciliado_com_o_extrato_diz_que_e_conciliacao(cenario, sessao_real):
    """O recado importa: "desfaça a conciliação primeiro" é acionável;
    "não pode" não é."""
    s = sessao_real
    pg = _pagar(s, cenario["da_ana"], cenario["conta"], marcar_paga=False)
    ex = Extrato(conta_bancaria_id=cenario["conta"].id,
                 data_lancamento=date(2026, 10, 10), valor=Decimal("-500"),
                 historico="PIX ENVIADO", hash_linha="linha-unica-do-teste")
    s.add(ex)
    s.flush()
    s.add(Conciliacao(pagamento_id=pg.id, extrato_id=ex.id, metodo="MANUAL"))
    s.flush()

    with pytest.raises(ErroValidacao) as e:
        svc.cancelar(s, cenario["da_ana"].id, _motivo(), cenario["financeiro"])
    assert "concilia" in str(e.value).lower()


def test_conciliacao_DESFEITA_nao_impede_mais(cenario, sessao_real):
    """Desfazer a conciliação tem de destravar de verdade — senão o recado
    "desfaça primeiro" seria mentira, e a pessoa desfaria à toa."""
    from datetime import datetime, timezone

    s = sessao_real
    pg = _pagar(s, cenario["da_ana"], cenario["conta"], marcar_paga=False)
    ex = Extrato(conta_bancaria_id=cenario["conta"].id,
                 data_lancamento=date(2026, 10, 10), valor=Decimal("-500"),
                 historico="PIX ENVIADO", hash_linha="outra-linha-do-teste")
    s.add(ex)
    s.flush()
    s.add(Conciliacao(pagamento_id=pg.id, extrato_id=ex.id, metodo="MANUAL",
                      desfeita_em=datetime.now(timezone.utc)))
    s.flush()

    with pytest.raises(ErroValidacao) as e:
        svc.cancelar(s, cenario["da_ana"].id, _motivo(), cenario["financeiro"])
    # continua barrado pelo PAGAMENTO, não pela conciliação
    assert "concilia" not in str(e.value).lower()
    assert "pagamento" in str(e.value).lower()


# ---------------------------------------------------------------------------
# 3. O AVISO A QUEM LANÇOU
# ---------------------------------------------------------------------------
def test_quem_lancou_e_avisado_com_o_motivo(cenario, sessao_real, monkeypatch):
    """O pedido do dono. O motivo vai INTEIRO: "foi cancelado" sem o porquê
    obriga a pessoa a ligar para perguntar, e aí o aviso não economizou nada."""
    from app.apps.erp.core import notificacoes

    cenario["lancou"].telefone = "5585999990000"
    sessao_real.flush()
    enviadas = []

    def _falso(**kw):
        enviadas.append(kw)
        return {"ok": True}
    monkeypatch.setattr("app.apps.notificador.enviar_telegram", _falso,
                        raising=False)

    svc.cancelar(sessao_real, cenario["da_ana"].id,
                 "nota veio em duplicidade", cenario["financeiro"])
    r = notificacoes.avisar_cancelamento(
        sessao_real, cenario["da_ana"].id, motivo="nota veio em duplicidade",
        cancelado_por=cenario["financeiro"])

    assert r["ok"] is True
    assert len(enviadas) == 1
    texto = enviadas[0]["mensagem"]
    assert "SP0001" in texto
    assert "nota veio em duplicidade" in texto
    assert cenario["financeiro"].nome in texto


def test_quem_cancelou_nao_recebe_aviso_de_si_mesmo(cenario, sessao_real, monkeypatch):
    """Ela acabou de cancelar, na tela. Mandar mensagem para ela seria ruído —
    e ruído faz a pessoa parar de ler os avisos que importam."""
    from app.apps.erp.core import notificacoes

    cenario["lancou"].telefone = "5585999990000"
    sessao_real.flush()
    enviadas = []
    monkeypatch.setattr("app.apps.notificador.enviar_telegram",
                        lambda **kw: enviadas.append(kw) or {"ok": True},
                        raising=False)

    svc.cancelar(sessao_real, cenario["da_ana"].id, _motivo(), cenario["lancou"])
    r = notificacoes.avisar_cancelamento(
        sessao_real, cenario["da_ana"].id, motivo=_motivo(),
        cancelado_por=cenario["lancou"])

    assert enviadas == []
    assert r["situacao"] == "SEM_DESTINO"


def test_aviso_nao_sai_duas_vezes(cenario, sessao_real, monkeypatch):
    """Cancelar é definitivo — não há o que repetir. Aviso repetido faz a
    pessoa achar que cancelaram de novo."""
    from app.apps.erp.core import notificacoes

    cenario["lancou"].telefone = "5585999990000"
    sessao_real.flush()
    enviadas = []
    monkeypatch.setattr("app.apps.notificador.enviar_telegram",
                        lambda **kw: enviadas.append(kw) or {"ok": True},
                        raising=False)

    svc.cancelar(sessao_real, cenario["da_ana"].id, _motivo(),
                 cenario["financeiro"])
    for _ in range(2):
        notificacoes.avisar_cancelamento(
            sessao_real, cenario["da_ana"].id, motivo=_motivo(),
            cancelado_por=cenario["financeiro"])
    assert len(enviadas) == 1


def test_falha_no_aviso_nao_desfaz_o_cancelamento(cenario, sessao_real, monkeypatch):
    """O cancelamento já aconteceu e está registrado. Derrubar por causa do
    aviso faria a pessoa cancelar de novo — e o segundo cancelamento seria
    recusado, deixando-a sem entender se cancelou ou não."""
    from app.apps.erp.core import notificacoes

    cenario["lancou"].telefone = "5585999990000"
    sessao_real.flush()

    def _explode(**kw):
        raise RuntimeError("Telegram fora do ar")
    monkeypatch.setattr("app.apps.notificador.enviar_telegram", _explode,
                        raising=False)

    t = svc.cancelar(sessao_real, cenario["da_ana"].id, _motivo(),
                     cenario["financeiro"])
    r = notificacoes.avisar_cancelamento(
        sessao_real, cenario["da_ana"].id, motivo=_motivo(),
        cancelado_por=cenario["financeiro"])

    assert t.status == StatusTitulo.CANCELADO
    assert r["ok"] is False and r["situacao"] == "FALHA"
