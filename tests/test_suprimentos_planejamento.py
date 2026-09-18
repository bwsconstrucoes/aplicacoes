"""O DISPARO AUTOMÁTICO DE COTAÇÃO — o sistema monta, a pessoa confere.

Pedido do dono em 18/09/2026: *"a gente poderia receber uma demanda de
suprimento e já disparar cotações (…) o sistema, através de categoria de
fornecedor, já planeja um disparo (…) ele vai apenas validar aquela sugestão do
sistema, editar uma ou outra coisa e disparar"*.

A LINHA QUE ESTES TESTES DEFENDEM: o sistema faz o trabalho braçal e PARA.
Planejar não grava nada, não abre cotação e não manda e-mail. O custo de uma
sugestão ruim é um clique para desmarcar; o de um disparo errado é o fornecedor
bom parar de responder.

O resto:

1. **Agrupa por categoria e por município da obra** — cimento e luminária no
   mesmo pedido voltam pela metade dos dois lados, e preço de duas pontas do
   estado não é um preço só.
2. **A ordem é por urgência**, e cada linha carrega o PORQUÊ: ordem sem motivo
   parece arbitrária.
3. **Fornecedor desligado do automático não é sugerido** — e insumo sem
   categoria é RELATADO, não escondido.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.suprimentos import planejamento as svc
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorCategoria, FornecedorPorte, Insumo, InsumoCategoria,
    Obra, PerfilUsuario as P, PrioridadeSolicitacao, StatusItemSuprimento,
    SuprimentoItem, SuprimentoSolicitacao, TipoPessoa,
)

from conftest import SessaoFalsa, novo_usuario

HOJE = date(2026, 9, 18)


def _cenario(*, porte=FornecedorPorte.FABRICA, municipio_forn="FORTALEZA",
             automatico=True, status=StatusItemSuprimento.SOLICITACAO,
             prioridade=PrioridadeSolicitacao.NORMAL, previsao=None,
             categoria_do_insumo=1):
    categoria = InsumoCategoria(id=1, codigo="CIM", nome="Cimento")
    obra = Obra(id=10, codigo="OB-01", nome="Creche", municipio="FORTALEZA")
    insumo = Insumo(id=100, codigo="1", descricao="Cimento CP-II 50kg",
                    categoria_insumo_id=categoria_do_insumo, unidade="SC")
    forn = Fornecedor(id=5, tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CIMENTOS DO CEARA LTDA", email="v@x.com.br",
                      municipio=municipio_forn, porte=porte,
                      regioes_atuacao=["CE"], canais_cotacao=["EMAIL"],
                      cotacao_automatica=automatico, ativo=True)
    ligacao = FornecedorCategoria(fornecedor_id=5, categoria_insumo_id=1)
    sol = SuprimentoSolicitacao(id=7, numero="SP-0007", titulo="Fundação",
                                solicitante_id=1, prioridade=prioridade,
                                previsao_entrega=previsao)
    item = SuprimentoItem(id=70, solicitacao_id=7, numero=1, insumo_id=100,
                          quantidade=Decimal("100"), quantidade_recebida=Decimal("0"),
                          unidade="SC", obra_id=10, status=status)
    return SessaoFalsa(categoria, obra, insumo, forn, ligacao, sol, item)


def test_o_plano_agrupa_por_categoria_e_municipio():
    r = svc.planejar(_cenario(), hoje=HOJE)

    assert len(r["blocos"]) == 1
    bloco = r["blocos"][0]
    assert bloco["categoria"] == "Cimento"
    assert bloco["municipio"] == "FORTALEZA"
    assert bloco["quantos_itens"] == 1


def test_planejar_nao_grava_nada():
    """É a linha inteira deste módulo: sugerir não é disparar."""
    s = _cenario()
    svc.planejar(s, hoje=HOJE)
    assert s.adicionados == []


def test_sugere_quem_vende_a_categoria_e_diz_por_que():
    """Lista sem motivo vira oráculo, e contra oráculo ninguém discorda."""
    r = svc.planejar(_cenario(), hoje=HOJE)

    forns = r["blocos"][0]["fornecedores"]
    assert len(forns) == 1
    assert forns[0]["sugerido"] is True
    assert "mesma cidade" in forns[0]["por_que"]
    assert "fábrica" in forns[0]["por_que"]


def test_fornecedor_de_outra_cidade_entra_mas_pontua_menos():
    r = svc.planejar(_cenario(municipio_forn="RECIFE"), hoje=HOJE)
    forns = r["blocos"][0]["fornecedores"]
    assert len(forns) == 1
    assert "mesma cidade" not in forns[0]["por_que"]


def test_fornecedor_fora_do_automatico_nao_e_sugerido():
    """Dono: *"tenho um fornecedor pequeno que eu não costumo mandar para
    cotar, ou foi uma compra única"*."""
    r = svc.planejar(_cenario(automatico=False), hoje=HOJE)
    assert r["blocos"][0]["fornecedores"] == []


def test_insumo_sem_categoria_e_relatado_e_nao_some():
    """Sem categoria não há como achar fornecedor — e esconder isso faria o
    item sumir da fila sem ninguém entender por quê."""
    r = svc.planejar(_cenario(categoria_do_insumo=None), hoje=HOJE)

    assert r["blocos"] == []
    assert len(r["sem_categoria"]) == 1
    assert r["sem_categoria"][0]["insumo"] == "Cimento CP-II 50kg"


@pytest.mark.parametrize("status", [
    StatusItemSuprimento.ANALISE_PROPOSTAS,
    StatusItemSuprimento.AUTORIZACAO,
    StatusItemSuprimento.PEDIDO_EMITIDO,
    StatusItemSuprimento.ENTREGUE,
])
def test_item_que_ja_passou_da_cotacao_nao_volta_para_a_fila(status):
    """Cotar de novo o que já foi cotado é o jeito mais rápido de o fornecedor
    achar que a BWS não se organiza."""
    r = svc.planejar(_cenario(status=status), hoje=HOJE)
    assert r["blocos"] == []


def test_item_ja_recebido_por_inteiro_nao_entra():
    s = _cenario()
    item = [o for o in s.objetos if isinstance(o, SuprimentoItem)][0]
    item.quantidade_recebida = item.quantidade

    assert svc.planejar(s, hoje=HOJE)["blocos"] == []


# ---------------------------------------------------------------------------
# A urgência, e o porquê dela
# ---------------------------------------------------------------------------
def test_previsao_vencida_vira_atrasado_com_os_dias_no_motivo():
    r = svc.planejar(_cenario(previsao=HOJE - timedelta(days=6)), hoje=HOJE)
    bloco = r["blocos"][0]
    assert bloco["urgencia"] == "ATRASADO"
    assert "6 dia(s)" in bloco["motivo"]


def test_prioridade_alta_e_dita_com_o_motivo():
    r = svc.planejar(_cenario(prioridade=PrioridadeSolicitacao.ALTA), hoje=HOJE)
    assert r["blocos"][0]["urgencia"] == "ALTA"
    assert "quem pediu" in r["blocos"][0]["motivo"]


def test_previsao_chegando_vira_media():
    r = svc.planejar(_cenario(previsao=HOJE + timedelta(days=3)), hoje=HOJE)
    assert r["blocos"][0]["urgencia"] == "MEDIA"


def test_sem_prazo_e_sem_prioridade_e_normal_e_sem_motivo():
    r = svc.planejar(_cenario(), hoje=HOJE)
    assert r["blocos"][0]["urgencia"] == "NORMAL"
    assert r["blocos"][0]["motivo"] == ""


def test_o_escopo_por_obra_vale_no_plano():
    """Quem não alcança a obra não planeja compra para ela."""
    assert svc.planejar(_cenario(), obras_permitidas=[], hoje=HOJE)["blocos"] == []
    assert svc.planejar(_cenario(), obras_permitidas=[10], hoje=HOJE)["blocos"]


def test_sem_nada_pendente_a_resposta_e_clara():
    s = SessaoFalsa()
    r = svc.planejar(s, hoje=HOJE)
    assert r["blocos"] == []
    assert "Nenhum item" in r["observacao"]


def test_item_ja_numa_cotacao_aberta_nao_e_sugerido(monkeypatch):
    """O status não denuncia: item em SOLICITACAO pode ter entrado numa cotação
    hoje de manhã. Sugerir o que a criação recusaria faz a tela prometer
    trabalho que sempre falha — foi o que aconteceu no primeiro teste dela."""
    from app.apps.erp.core.suprimentos import cotacao as svc_cot

    s = _cenario()
    monkeypatch.setattr(svc_cot, "_itens_em_cotacao_aberta", lambda _s, ids: {70})

    r = svc.planejar(s, hoje=HOJE)

    assert r["blocos"] == []
    assert r["em_cotacao_aberta"] == 1
    assert "já estão numa cotação aberta" in r["observacao"]
