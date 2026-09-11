"""O relatório de compras — com banco de verdade.

O dono pediu duas coisas na mesma frase: "de pedido a gente vai conseguir ver
tudo de uma obra" e "eu quero ver tudo que foi de cimento". São a mesma
consulta lida dos dois lados, e é isso que se prova aqui.

Com banco porque tudo o que importa está no SQL: os `JOIN` que ligam o item do
pedido à obra e ao insumo (passando pela solicitação), o filtro de status, e o
ESCOPO — quem enxerga só as obras dele não pode descobrir o gasto das outras
por um relatório. O dublê ignora `WHERE` e `JOIN`; aqui nada disso é fingido.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from app.apps.erp.core.suprimentos import compras as svc
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, Fornecedor, Insumo, InsumoCategoria, Obra, PedidoCompra,
    PedidoItem, PerfilUsuario as P, PrioridadeSolicitacao, RegimeTributario,
    StatusItemSuprimento, StatusPedidoCompra, SuprimentoItem,
    SuprimentoSolicitacao, TipoPessoa, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Comprador", email="compras@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    a = Obra(codigo="OBRACOMPRA-A", nome="Obra A")
    b = Obra(codigo="OBRACOMPRA-B", nome="Obra B")
    cat = InsumoCategoria(codigo="CAT-CIM", nome="Aglomerantes")
    forn = Fornecedor(razao_social="Fornecedor de compras", cnpj_cpf="71000003000146",
                      tipo_pessoa=TipoPessoa.PJ, ativo=True, e_fornecedor=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    s.add_all([u, a, b, cat, forn]); s.flush()
    cimento = Insumo(codigo="INS-CIM", descricao="Cimento CP-II 50kg",
                     categoria_insumo_id=cat.id, unidade="SC", ativo=True)
    ceramica = Insumo(codigo="INS-CER", descricao="Cerâmica 60x60",
                      categoria_insumo_id=cat.id, unidade="M2", ativo=True)
    s.add_all([cimento, ceramica]); s.flush()

    def compra(numero, obra, insumo, qtd, preco, status=StatusPedidoCompra.AUTORIZADO):
        sol = SuprimentoSolicitacao(numero=f"SS-{numero}", titulo=f"Compra {numero}",
                                    solicitante_id=u.id,
                                    prioridade=PrioridadeSolicitacao.NORMAL)
        s.add(sol); s.flush()
        # O banco exige quem autorizou (ck_pedido_autorizado): pedido
        # autorizado sem autor é justamente o que a trava impede.
        autorizado = status == StatusPedidoCompra.AUTORIZADO
        ped = PedidoCompra(numero=f"PC-{numero}", fornecedor_id=forn.id,
                           status=status, criado_por=u.id,
                           autorizado_por=u.id if autorizado else None,
                           autorizado_em=datetime.now(timezone.utc) if autorizado else None)
        s.add(ped); s.flush()
        item = SuprimentoItem(solicitacao_id=sol.id, numero=1, insumo_id=insumo.id,
                              quantidade=Decimal(str(qtd)), unidade=insumo.unidade,
                              obra_id=obra.id,
                              status=StatusItemSuprimento.PEDIDO_EMITIDO)
        s.add(item); s.flush()
        s.add(PedidoItem(pedido_id=ped.id, suprimento_item_id=item.id, numero=1,
                         quantidade=Decimal(str(qtd)),
                         preco_unitario=Decimal(str(preco))))
        s.flush()
        return ped

    compra("C1", a, cimento, 100, "40.00")      # 4.000 na obra A
    compra("C2", b, cimento, 50, "42.00")       # 2.100 na obra B
    compra("C3", a, ceramica, 200, "35.00")     # 7.000 na obra A
    compra("C4", a, cimento, 10, "39.00",       # NÃO autorizado: não conta
           status=StatusPedidoCompra.AGUARDANDO_AUTORIZACAO)
    s.commit()
    return {"s": s, "usuario": u, "obra_a": a, "obra_b": b,
            "cimento": cimento, "ceramica": ceramica}


def test_por_obra_soma_o_que_a_obra_comprou_e_abre_ate_o_insumo(cenario):
    d = svc.arvore(cenario["s"], agrupar="obra")
    grupos = {g["rotulo"]: g for g in d["grupos"]}
    assert grupos["OBRACOMPRA-A"]["total"] == 11000.0     # 4.000 + 7.000
    assert grupos["OBRACOMPRA-B"]["total"] == 2100.0
    filhos = {f["rotulo"]: f for f in grupos["OBRACOMPRA-A"]["filhos"]}
    assert filhos["Cimento CP-II 50kg"]["total"] == 4000.0
    assert filhos["Cerâmica 60x60"]["total"] == 7000.0


def test_tudo_que_foi_de_cimento_soma_as_obras(cenario):
    """A pergunta do dono, ao pé da letra."""
    d = svc.arvore(cenario["s"], agrupar="insumo",
                   insumo_id=cenario["cimento"].id)
    assert len(d["grupos"]) == 1
    cimento = d["grupos"][0]
    assert cimento["rotulo"] == "Cimento CP-II 50kg"
    assert cimento["total"] == 6100.0                    # 4.000 + 2.100
    assert cimento["quantidade"] == 150.0
    obras = {f["rotulo"]: f["total"] for f in cimento["filhos"]}
    assert obras == {"OBRACOMPRA-A": 4000.0, "OBRACOMPRA-B": 2100.0}


def test_pedido_sem_autorizacao_nao_entra_na_conta(cenario):
    """Somá-lo faria o relatório dizer que a obra gastou o que não gastou."""
    sem = svc.arvore(cenario["s"], agrupar="obra")
    com = svc.arvore(cenario["s"], agrupar="obra", incluir_pendentes=True)
    assert sem["total_geral"]["total"] == 13100.0
    assert com["total_geral"]["total"] == 13490.0        # + 10 × 39,00


def test_quem_so_enxerga_uma_obra_nao_ve_o_gasto_da_outra(cenario):
    """O escopo entra na CONSULTA. Um relatório não pode ser a porta dos
    fundos por onde se descobre o que a tela de títulos esconde."""
    s = cenario["s"]
    preso = Usuario(nome="Preso à obra A", email="preso@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"),
                    perfil=P.SUPERVISOR_OBRA)
    s.add(preso); s.flush()
    s.add(UsuarioObra(usuario_id=preso.id, obra_id=cenario["obra_a"].id))
    s.flush()

    d = svc.arvore(s, agrupar="obra", usuario=preso)
    assert [g["rotulo"] for g in d["grupos"]] == ["OBRACOMPRA-A"]
    assert d["total_geral"]["total"] == 11000.0

    # e nem pelo caminho do insumo ele alcança a obra B
    so_cimento = svc.arvore(s, agrupar="insumo", usuario=preso,
                            insumo_id=cenario["cimento"].id)
    assert so_cimento["grupos"][0]["total"] == 4000.0


def test_a_linha_a_linha_traz_o_que_a_exportacao_precisa(cenario):
    linhas = svc.linhas(cenario["s"])
    assert len(linhas) == 3
    uma = linhas[0]
    for campo in ("data", "pedido", "obra", "insumo", "unidade", "quantidade",
                  "preco_unitario", "total", "fornecedor", "solicitacao"):
        assert campo in uma, f"a exportação precisa de '{campo}'"
