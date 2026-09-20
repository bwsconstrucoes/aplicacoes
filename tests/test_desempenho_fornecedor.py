# ============================================================================
# A MEMÓRIA DO FORNECEDOR — o que vai tornar o sistema inteligente.
#
# O dono, 18/09/2026: *"aí você já tem que deixar ele pronto para se tornar
# inteligente: os dados que a gente vai trabalhar já estarem sendo guardados"*.
#
# O TESTE QUE MAIS IMPORTA AQUI É O DA TRAVA. Um fornecedor chamado uma vez que
# respondeu tem 100% de resposta, e ordenar a lista por esse número poria o
# desconhecido na frente de quem atende a empresa há dois anos. Enquanto não há
# amostra, o histórico NÃO PODE mexer em nada — e é isso que se prova abaixo.
# ============================================================================
from datetime import date, datetime, timedelta, timezone

from app.apps.erp.core.suprimentos import desempenho
from app.apps.erp.db.models.cadastros import (
    CotacaoFornecedor, CotacaoPreco, EnvioEmail, PedidoCompra, PedidoItem,
    Recebimento, RecebimentoItem, StatusPedidoCompra,
)
from tests.conftest import SessaoFalsa

BASE = datetime(2026, 9, 1, tzinfo=timezone.utc)


def _envio(coluna_id, dia):
    return EnvioEmail(
        id=1000 + coluna_id, entidade_tipo="cotacao", entidade_id=1,
        destinatario_tipo="cotacao_fornecedor", destinatario_id=coluna_id,
        para=["v@f.com"], copia=[], assunto="c", corpo="c", anexos=[],
        situacao="ENVIADO", criado_em=BASE + timedelta(days=dia))


def _coluna(id_, fornecedor_id=7, **extra):
    return CotacaoFornecedor(id=id_, cotacao_id=1, fornecedor_id=fornecedor_id,
                             frete=0, desconto=0, acrescimo_percentual=0,
                             ordem=0, contatos_ids=[], cobrancas=0,
                             sem_interesse=extra.pop("sem_interesse", False),
                             **extra)


def test_sem_amostra_o_historico_nao_mexe_em_nada():
    """Uma cotação respondida não é 100% de confiabilidade — é uma cotação."""
    s = SessaoFalsa(_coluna(1), _envio(1, 0),
                    CotacaoPreco(id=1, cotacao_fornecedor_id=1, cotacao_item_id=1,
                                 preco_unitario=10,
                                 registrado_em=BASE + timedelta(days=1)))
    f = desempenho.por_fornecedor(s)[7]
    assert f["confiavel"] is False
    assert f["pontos"] == 0
    assert "faltam" in f["resumo"]


def test_com_amostra_quem_sempre_responde_sobe():
    objetos = []
    for i in range(1, 4):
        objetos += [_coluna(i), _envio(i, i),
                    CotacaoPreco(id=i, cotacao_fornecedor_id=i, cotacao_item_id=1,
                                 preco_unitario=10,
                                 registrado_em=BASE + timedelta(days=i + 2))]
    f = desempenho.por_fornecedor(SessaoFalsa(*objetos))[7]
    assert f["confiavel"] is True
    assert f["taxa_resposta"] == 1.0
    assert f["dias_resposta"] == 2.0
    assert f["pontos"] > 0


def test_quem_nunca_responde_CAI_na_lista_e_nao_so_deixa_de_subir():
    """Ficar em zero deixaria o fornecedor sumido empatado com o desconhecido
    — e ele é pior que o desconhecido, porque já teve três chances."""
    objetos = []
    for i in range(1, 5):
        objetos += [_coluna(i), _envio(i, i)]
    f = desempenho.por_fornecedor(SessaoFalsa(*objetos))[7]
    assert f["taxa_resposta"] == 0.0
    assert f["pontos"] < 0


def test_quem_avisa_que_nao_vai_cotar_conta_como_quem_respondeu():
    objetos = []
    for i in range(1, 4):
        objetos += [_coluna(i, sem_interesse=True), _envio(i, i)]
    f = desempenho.por_fornecedor(SessaoFalsa(*objetos))[7]
    assert f["recusas"] == 3
    assert f["taxa_resposta"] == 1.0
    assert f["pontos"] > 0


def test_mapa_montado_e_nunca_disparado_nao_conta_como_convite():
    """Punir o fornecedor por um e-mail que o comprador não mandou seria medir
    o nosso atraso e chamar de desempenho dele."""
    s = SessaoFalsa(_coluna(1), _coluna(2), _envio(1, 0))
    f = desempenho.por_fornecedor(s)[7]
    assert f["convites"] == 1


def test_entrega_parcial_nao_conta_como_entrega_no_prazo():
    """Mandar 10% no dia combinado não é cumprir o prazo."""
    objetos = [
        PedidoCompra(id=1, numero="PC-1", fornecedor_id=7, frete=0, desconto=0,
                     status=StatusPedidoCompra.AUTORIZADO, criado_por=1,
                     previsao_entrega=date(2026, 9, 10)),
        PedidoItem(id=1, pedido_id=1, suprimento_item_id=1, numero=1,
                   quantidade=10, preco_unitario=5),
        PedidoItem(id=2, pedido_id=1, suprimento_item_id=2, numero=2,
                   quantidade=10, preco_unitario=5),
        Recebimento(id=1, pedido_id=1, data=date(2026, 9, 9), recebido_por=1),
        RecebimentoItem(id=1, recebimento_id=1, pedido_item_id=1, quantidade=10),
    ]
    f = desempenho.por_fornecedor(SessaoFalsa(*objetos))[7]
    assert f["pedidos"] == 1
    assert f["entregas"] == 0


def test_entrega_completa_no_prazo_conta():
    objetos = [
        PedidoCompra(id=1, numero="PC-1", fornecedor_id=7, frete=0, desconto=0,
                     status=StatusPedidoCompra.AUTORIZADO, criado_por=1,
                     previsao_entrega=date(2026, 9, 10)),
        PedidoItem(id=1, pedido_id=1, suprimento_item_id=1, numero=1,
                   quantidade=10, preco_unitario=5),
        Recebimento(id=1, pedido_id=1, data=date(2026, 9, 9), recebido_por=1),
        RecebimentoItem(id=1, recebimento_id=1, pedido_item_id=1, quantidade=10),
    ]
    f = desempenho.por_fornecedor(SessaoFalsa(*objetos))[7]
    assert f["entregas"] == 1
    assert f["entregas_no_prazo"] == 1
    assert f["taxa_prazo"] == 1.0


def test_atraso_entra_na_media_de_dias():
    objetos = [
        PedidoCompra(id=1, numero="PC-1", fornecedor_id=7, frete=0, desconto=0,
                     status=StatusPedidoCompra.AUTORIZADO, criado_por=1,
                     previsao_entrega=date(2026, 9, 10)),
        PedidoItem(id=1, pedido_id=1, suprimento_item_id=1, numero=1,
                   quantidade=10, preco_unitario=5),
        Recebimento(id=1, pedido_id=1, data=date(2026, 9, 17), recebido_por=1),
        RecebimentoItem(id=1, recebimento_id=1, pedido_item_id=1, quantidade=10),
    ]
    f = desempenho.por_fornecedor(SessaoFalsa(*objetos))[7]
    assert f["entregas_no_prazo"] == 0
    assert f["dias_atraso"] == 7.0


def test_pedido_cancelado_nao_conta_contra_o_fornecedor():
    objetos = [
        PedidoCompra(id=1, numero="PC-1", fornecedor_id=7, frete=0, desconto=0,
                     status=StatusPedidoCompra.CANCELADO, criado_por=1,
                     previsao_entrega=date(2026, 9, 10)),
        PedidoItem(id=1, pedido_id=1, suprimento_item_id=1, numero=1,
                   quantidade=10, preco_unitario=5),
    ]
    assert desempenho.por_fornecedor(SessaoFalsa(*objetos)) == {}


def test_fornecedor_nunca_chamado_tem_ficha_vazia_e_honesta():
    f = desempenho.de_um(SessaoFalsa(), 99)
    assert f["convites"] == 0
    assert f["pontos"] == 0
    assert "nunca foi chamado" in f["resumo"]
