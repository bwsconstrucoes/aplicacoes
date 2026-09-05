"""Suprimentos contra Postgres de verdade.

A sessão dublada ignora `WHERE` — então tudo que depende de filtro no banco
precisa ser conferido aqui. Neste módulo:

  - o ESCOPO dos itens de suprimento (a regra nova desta entrega): quem só vê
    os próprios pedidos não pode enxergar o da obra do vizinho;
  - as RESTRIÇÕES que as migrações 033–037 criaram, uma a uma;
  - a reserva que impede o mesmo item de entrar em dois pedidos vivos — a
    garantia está na chave primária, e é aqui que isso se prova.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.suprimentos import solicitacao as svc_sol
from app.apps.erp.db.models.cadastros import (
    Categoria, EscopoVisao, Insumo, InsumoCategoria, Obra, PedidoCompra,
    PedidoItem, PedidoItemReserva, PerfilUsuario as P, StatusItemSuprimento as ST,
    SuprimentoItem, SuprimentoSolicitacao, Usuario, UsuarioObra,
)

pytestmark = pytest.mark.banco


def _usuario(s, nome, email, perfil, escopo=EscopoVisao.PROPRIOS):
    u = Usuario(nome=nome, email=email, senha_hash=gerar_hash("senha-de-teste-1234"),
                perfil=perfil, escopo_visao=escopo)
    s.add(u)
    s.flush()
    return u


def _obra(s, codigo):
    o = Obra(codigo=codigo, nome=f"Obra {codigo}", status="ATIVA")
    s.add(o)
    s.flush()
    return o


def _insumo(s, codigo="INS-9001"):
    cat = InsumoCategoria(codigo=f"C{codigo[-3:]}", nome=f"Categoria {codigo}")
    s.add(cat)
    s.flush()
    i = Insumo(codigo=codigo, descricao=f"Insumo {codigo}", unidade="M",
               categoria_insumo_id=cat.id)
    s.add(i)
    s.flush()
    return i


def _pedido_de(s, usuario, obra, insumo, quantidade="10"):
    sol = SuprimentoSolicitacao(numero=f"SS-{usuario.id:04d}{obra.id:02d}",
                                titulo="pedido de teste", solicitante_id=usuario.id)
    s.add(sol)
    s.flush()
    item = SuprimentoItem(solicitacao_id=sol.id, numero=1, insumo_id=insumo.id,
                          quantidade=Decimal(quantidade), unidade="M",
                          obra_id=obra.id, status=ST.SOLICITACAO)
    s.add(item)
    s.flush()
    return sol, item


# ---------------------------------------------------------------------------
# Escopo, com WHERE de verdade
# ---------------------------------------------------------------------------
def test_quem_so_ve_os_proprios_nao_enxerga_o_pedido_do_outro(sessao_real):
    s = sessao_real
    obra = _obra(s, "ESC-A")
    insumo = _insumo(s, "INS-9101")
    dono = _usuario(s, "Dono do pedido", "dono@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    outro = _usuario(s, "Outro", "outro@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    _pedido_de(s, dono, obra, insumo)
    s.flush()

    assert len(svc_sol.listar_itens(s, dono)) == 1
    assert svc_sol.listar_itens(s, outro) == [], (
        "o administrativo de obra em 'só os meus' não pode ver o pedido de outro")


def test_quem_ve_as_obras_designadas_enxerga_o_pedido_daquela_obra(sessao_real):
    s = sessao_real
    obra = _obra(s, "ESC-B")
    insumo = _insumo(s, "INS-9102")
    dono = _usuario(s, "Dono", "dono2@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    vizinho = _usuario(s, "Vizinho", "vizinho@teste.bws.local", P.ADMINISTRATIVO_OBRA,
                       escopo=EscopoVisao.OBRAS_DESIGNADAS)
    s.add(UsuarioObra(usuario_id=vizinho.id, obra_id=obra.id))
    _pedido_de(s, dono, obra, insumo)
    s.flush()

    assert len(svc_sol.listar_itens(s, vizinho)) == 1


def test_obras_designadas_nao_abre_a_obra_que_nao_e_dele(sessao_real):
    s = sessao_real
    obra_dele = _obra(s, "ESC-C1")
    obra_alheia = _obra(s, "ESC-C2")
    insumo = _insumo(s, "INS-9103")
    dono = _usuario(s, "Dono", "dono3@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    vizinho = _usuario(s, "Vizinho", "vizinho3@teste.bws.local", P.ADMINISTRATIVO_OBRA,
                       escopo=EscopoVisao.OBRAS_DESIGNADAS)
    s.add(UsuarioObra(usuario_id=vizinho.id, obra_id=obra_dele.id))
    _pedido_de(s, dono, obra_alheia, insumo)
    s.flush()

    assert svc_sol.listar_itens(s, vizinho) == []


def test_quem_ve_tudo_enxerga_o_pedido_de_qualquer_um(sessao_real):
    s = sessao_real
    obra = _obra(s, "ESC-D")
    insumo = _insumo(s, "INS-9104")
    dono = _usuario(s, "Dono", "dono4@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    financeiro = _usuario(s, "Financeiro", "fin@teste.bws.local", P.FINANCEIRO)
    _pedido_de(s, dono, obra, insumo)
    s.flush()

    assert len(svc_sol.listar_itens(s, financeiro)) == 1


# ---------------------------------------------------------------------------
# As restrições que as migrações criaram
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tabela", [
    "unidades_compra", "condicoes_pagamento", "fornecedor_categorias",
    "fornecedor_contatos", "insumo_solicitacoes", "suprimento_solicitacoes",
    "suprimento_itens", "cotacoes", "cotacao_itens", "cotacao_fornecedores",
    "cotacao_precos", "precos_historico", "pedidos_compra", "pedido_itens",
    "pedido_item_reserva", "previsoes_pagamento", "recebimentos",
    "recebimento_itens",
])
def test_as_migracoes_criaram_as_tabelas(sessao_real, tabela):
    existe = sessao_real.execute(text(
        "SELECT to_regclass(:t)"), {"t": tabela}).scalar()
    assert existe is not None, f"{tabela} não existe — migração não aplicou"


def test_as_unidades_e_condicoes_vieram_povoadas(sessao_real):
    """A carga inicial faz parte da migração: sem unidade cadastrada, nenhuma
    solicitação entra."""
    unidades = sessao_real.execute(text("SELECT count(*) FROM unidades_compra")).scalar()
    condicoes = sessao_real.execute(text("SELECT count(*) FROM condicoes_pagamento")).scalar()
    assert unidades >= 16 and condicoes >= 20


def test_condicao_sem_entrada_e_sem_prazo_e_recusada_pelo_banco(sessao_real):
    """A regra vale mesmo que um caminho novo esqueça de conferir.

    Este teste já pegou um defeito de verdade: a trava original usava
    `array_length`, que devolve NULO para vetor vazio — e CHECK com resultado
    nulo passa. A trava não travava nada.
    """
    with pytest.raises(IntegrityError):
        sessao_real.execute(text(
            "INSERT INTO condicoes_pagamento (nome, entrada_percentual, dias) "
            "VALUES ('nunca vence', 0, '{}')"))


def test_quantidade_recebida_nao_passa_da_pedida(sessao_real):
    s = sessao_real
    obra = _obra(s, "CK-A")
    insumo = _insumo(s, "INS-9201")
    dono = _usuario(s, "Dono", "ck@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    sol, item = _pedido_de(s, dono, obra, insumo, "10")

    item.quantidade_recebida = Decimal("11")
    with pytest.raises(IntegrityError):
        s.flush()


def test_preco_de_cotacao_tem_de_ser_positivo(sessao_real):
    s = sessao_real
    with pytest.raises(IntegrityError):
        s.execute(text(
            "INSERT INTO precos_historico (insumo_id, preco_unitario, tipo) "
            "VALUES (NULL, 0, 'COTADO')"))


def test_a_reserva_impede_o_mesmo_item_em_dois_pedidos(sessao_real):
    """A garantia é a chave primária da tabela de reserva — e é aqui que isso
    deixa de ser promessa e vira fato."""
    s = sessao_real
    obra = _obra(s, "RES-A")
    insumo = _insumo(s, "INS-9301")
    dono = _usuario(s, "Dono", "res@teste.bws.local", P.ADMINISTRATIVO_OBRA)
    sol, item = _pedido_de(s, dono, obra, insumo)

    from app.apps.erp.db.models.cadastros import Fornecedor, TipoPessoa
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="FORNECEDOR RESERVA LTDA")
    s.add(forn)
    s.flush()
    for numero in ("PC-9001", "PC-9002"):
        p = PedidoCompra(numero=numero, fornecedor_id=forn.id, criado_por=dono.id)
        s.add(p)
        s.flush()
        s.add(PedidoItemReserva(suprimento_item_id=item.id, pedido_id=p.id))
        if numero == "PC-9001":
            s.flush()
        else:
            with pytest.raises(IntegrityError):
                s.flush()


def test_pedido_autorizado_sem_quem_autorizou_e_recusado(sessao_real):
    """Meses depois, ninguém saberia quem liberou a compra."""
    s = sessao_real
    dono = _usuario(s, "Dono", "aut@teste.bws.local", P.ADMIN)
    from app.apps.erp.db.models.cadastros import Fornecedor, TipoPessoa
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="34028316000103",
                      razao_social="FORNECEDOR AUT LTDA")
    s.add(forn)
    s.flush()

    with pytest.raises(IntegrityError):
        s.execute(text(
            "INSERT INTO pedidos_compra (numero, fornecedor_id, status, criado_por) "
            "VALUES ('PC-9999', :f, 'AUTORIZADO', :u)"),
            {"f": forn.id, "u": dono.id})


# ---------------------------------------------------------------------------
# Zerar o movimento: o que sai e, principalmente, o que FICA
# ---------------------------------------------------------------------------
def test_zerar_leva_o_movimento_e_deixa_os_cadastros(sessao_real):
    """A prova que importa: depois de zerar, o insumo e o fornecedor que
    custaram uma importação continuam lá, e a solicitação sumiu."""
    from app.apps.erp.core.suprimentos import limpeza
    from app.apps.erp.db.models.cadastros import Fornecedor, TipoPessoa

    s = sessao_real
    obra = _obra(s, "ZER-A")
    insumo = _insumo(s, "INS-9401")
    dono = _usuario(s, "Dono", "zerar@teste.bws.local", P.ADMIN)
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="FORNECEDOR ZERAR LTDA")
    s.add(forn)
    sol, item = _pedido_de(s, dono, obra, insumo)
    s.flush()

    antes = limpeza.resumo(s)
    assert antes["total"] >= 2, "solicitação e item deveriam estar contados"
    assert antes["impedimentos"] == []

    relatorio = limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, dono)
    s.flush()

    assert relatorio["total"] >= 2
    assert s.execute(text("SELECT count(*) FROM suprimento_itens")).scalar() == 0
    assert s.execute(text("SELECT count(*) FROM suprimento_solicitacoes")).scalar() == 0
    # E o que não pode sair:
    assert s.get(Insumo, insumo.id) is not None
    assert s.get(Fornecedor, forn.id) is not None
    assert s.execute(text("SELECT count(*) FROM unidades_compra")).scalar() >= 16
    assert s.execute(text("SELECT count(*) FROM condicoes_pagamento")).scalar() >= 20


def test_zerar_de_novo_nao_reclama_e_nao_apaga_nada(sessao_real):
    """Rodar duas vezes é o que uma pessoa nervosa faz."""
    from app.apps.erp.core.suprimentos import limpeza
    dono = _usuario(s := sessao_real, "Dono", "zerar2@teste.bws.local", P.ADMIN)

    limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, dono)
    segundo = limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, dono)

    assert segundo["total"] == 0


def test_zerar_recusa_quando_a_previsao_virou_titulo(sessao_real):
    """Com título, isto virou dinheiro no financeiro — e a limpeza para."""
    from app.apps.erp.core.suprimentos import limpeza
    from app.apps.erp.db.models.cadastros import (
        Fornecedor, PrevisaoPagamento, TipoPessoa,
    )
    from app.apps.erp.db.models.financeiro import (
        FormaPagamento, StatusTitulo, TipoTitulo, Titulo,
    )
    from datetime import date as _date
    from decimal import Decimal as _D

    s = sessao_real
    dono = _usuario(s, "Dono", "zerar3@teste.bws.local", P.ADMIN)
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="34028316000103",
                      razao_social="FORNECEDOR TITULO LTDA")
    cat = Categoria(codigo="9.9.97", descricao="Compras teste")
    s.add_all([forn, cat])
    s.flush()
    titulo = Titulo(numero_sp="SP-ZER-1", tipo=TipoTitulo.T5_EMPREITEIRO,
                    fornecedor_id=forn.id, descricao="x", valor_bruto=_D("10"),
                    valor_liquido=_D("10"), competencia=_date(2026, 9, 1),
                    categoria_id=cat.id, forma_pagamento=FormaPagamento.PIX,
                    status=StatusTitulo.APROVADO, solicitante_id=dono.id)
    pedido = PedidoCompra(numero="PC-ZER-1", fornecedor_id=forn.id, criado_por=dono.id)
    s.add_all([titulo, pedido])
    s.flush()
    s.add(PrevisaoPagamento(pedido_id=pedido.id, numero=1,
                            vencimento=_date(2026, 10, 1), valor=_D("10"),
                            titulo_id=titulo.id))
    s.flush()

    with pytest.raises(Exception, match="já virou título"):
        limpeza.zerar(s, limpeza.FRASE_DE_CONFIRMACAO, dono)

    assert s.execute(text("SELECT count(*) FROM pedidos_compra")).scalar() == 1
