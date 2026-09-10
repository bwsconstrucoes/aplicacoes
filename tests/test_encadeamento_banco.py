"""Encadeamento entre telas — com banco de verdade.

Princípio registrado no ROTEIRO desde o começo, nas palavras do dono: clicar
na obra, na conta, no credor, na compra e ir para o cadastro correspondente,
"como o conexão database do Pipefy".

Sem isso, quem está conferindo um título abre outra aba, procura a obra de
novo numa lista de dezenas e perde o lugar da conferência.

Com banco porque o que pode dar errado é do banco e da permissão: o número do
registro tem de vir no que a tela recebe, a tela de destino tem de abrir com
aquele número no endereço, e o link NÃO pode aparecer para quem receberia um
"sem permissão" do outro lado.

O que se prova:

  1. A lista e a ficha do título trazem os números que ligam aos cadastros.
  2. O rateio traz a obra de cada linha — título rateado tem mais de uma.
  3. O pedido de compra que originou o título vem com número, para virar link.
  4. As quatro telas de destino abrem com o registro no endereço.
  5. O link só é oferecido a quem pode abrir o destino: um financeiro não vê o
     elo para o pedido de compra, que é de ADMIN e diretoria.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (Categoria, Fornecedor, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa, Usuario)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               Parcela, Pedido, Rateio,
                                               StatusParcela, StatusTitulo,
                                               TipoTitulo, Titulo)
from tests.conftest import como

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    admin = Usuario(nome="Admin do elo", email="elo.admin@teste.local", ativo=True,
                    senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    fin = Usuario(nome="Financeiro do elo", email="elo.fin@teste.local", ativo=True,
                  senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    forn = Fornecedor(razao_social="Comercial Vale do Sol LTDA",
                      nome_fantasia="Vale do Sol", cnpj_cpf="33444555000166",
                      tipo_pessoa=TipoPessoa.PJ, ativo=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra_a = Obra(codigo="OBRA-A", nome="Escola A")
    obra_b = Obra(codigo="OBRA-B", nome="Creche B")
    cat = Categoria(codigo="3.1.02", descricao="Material de construção")
    s.add_all([admin, fin, forn, obra_a, obra_b, cat])
    s.flush()
    return {"s": s, "admin": admin, "financeiro": fin, "fornecedor": forn,
            "obra_a": obra_a, "obra_b": obra_b, "categoria": cat}


def _titulo(cenario, *, rateado=False, pedido_id=None):
    s = cenario["s"]
    t = Titulo(numero_sp="SP-00777", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.PAGAR, fornecedor_id=cenario["fornecedor"].id,
               descricao="Cimento e areia", valor_bruto=Decimal("1000.00"),
               valor_liquido=Decimal("1000.00"), competencia=date(2026, 8, 1),
               categoria_id=cenario["categoria"].id, pedido_id=pedido_id,
               forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
               solicitante_id=cenario["admin"].id)
    s.add(t)
    s.flush()
    s.add(Parcela(titulo_id=t.id, numero=1, vencimento=date(2026, 12, 1),
                  valor=Decimal("1000.00"), status=StatusParcela.ABERTA))
    if rateado:
        s.add(Rateio(titulo_id=t.id, obra_id=cenario["obra_a"].id,
                     valor=Decimal("600.00"), percentual=Decimal("60")))
        s.add(Rateio(titulo_id=t.id, obra_id=cenario["obra_b"].id,
                     valor=Decimal("400.00"), percentual=Decimal("40")))
    else:
        s.add(Rateio(titulo_id=t.id, obra_id=cenario["obra_a"].id,
                     valor=Decimal("1000.00"), percentual=Decimal("100")))
    s.flush()
    return t


# ---------------------------------------------------------------------------
# 1 a 3. OS NÚMEROS QUE LIGAM
# ---------------------------------------------------------------------------
def test_a_lista_traz_os_numeros_dos_cadastros(app_real, cenario):
    t = _titulo(cenario)
    r = como(app_real, cenario["admin"].id).get("/erp/api/titulos")
    assert r.status_code == 200
    linha = next(x for x in r.get_json()["titulos"] if x["id"] == t.id)
    assert linha["fornecedor_id"] == cenario["fornecedor"].id
    assert linha["categoria_id"] == cenario["categoria"].id
    assert linha["obra_ids"] == [cenario["obra_a"].id]


def test_titulo_rateado_traz_as_duas_obras(app_real, cenario):
    """Duas obras é o caso em que o elo da linha NÃO pode existir: para onde
    ele iria? As duas aparecem no rateio, dentro da ficha."""
    t = _titulo(cenario, rateado=True)
    c = como(app_real, cenario["admin"].id)
    linha = next(x for x in c.get("/erp/api/titulos").get_json()["titulos"]
                 if x["id"] == t.id)
    assert sorted(linha["obra_ids"]) == sorted([cenario["obra_a"].id,
                                                cenario["obra_b"].id])

    ficha = c.get(f"/erp/api/titulos/{t.id}").get_json()["titulo"]
    obras_no_rateio = sorted(r["obra_id"] for r in ficha["rateios"])
    assert obras_no_rateio == sorted([cenario["obra_a"].id, cenario["obra_b"].id])


def test_a_compra_que_originou_o_titulo_vem_com_numero(app_real, cenario):
    s = cenario["s"]
    p = Pedido(numero="PC-2026-0042", fornecedor_id=cenario["fornecedor"].id)
    s.add(p)
    s.flush()
    t = _titulo(cenario, pedido_id=p.id)
    ficha = (como(app_real, cenario["admin"].id)
             .get(f"/erp/api/titulos/{t.id}").get_json()["titulo"])
    assert ficha["pedido"] == {"id": p.id, "numero": "PC-2026-0042"}


def test_titulo_sem_compra_nao_inventa_pedido(app_real, cenario):
    t = _titulo(cenario)
    ficha = (como(app_real, cenario["admin"].id)
             .get(f"/erp/api/titulos/{t.id}").get_json()["titulo"])
    assert ficha["pedido"] is None


# ---------------------------------------------------------------------------
# 4. AS TELAS DE DESTINO ABREM COM O REGISTRO NO ENDEREÇO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("endereco", [
    "/erp/obras?obra={obra}",
    "/erp/configuracoes?conta={conta}",
    "/erp/suprimentos/fornecedores?fornecedor={forn}",
    "/erp/suprimentos/pedidos?pedido=1",
])
def test_a_tela_de_destino_abre_com_o_registro_no_endereco(app_real, cenario, endereco):
    alvo = endereco.format(obra=cenario["obra_a"].id, conta=cenario["categoria"].id,
                           forn=cenario["fornecedor"].id)
    r = como(app_real, cenario["admin"].id).get(alvo)
    assert r.status_code == 200, f"{alvo} não abriu: HTTP {r.status_code}"


# ---------------------------------------------------------------------------
# 5. O LINK RESPEITA A PERMISSÃO DO DESTINO
# ---------------------------------------------------------------------------
def test_o_elo_para_a_compra_nao_e_oferecido_a_quem_nao_ve_pedido(app_real, cenario):
    """Link que responde 'sem permissão' é pior do que texto puro: promete uma
    porta que não abre. A trava continua sendo o @permissao da rota — isto é
    só a tela não oferecer."""
    html = como(app_real, cenario["financeiro"].id).get("/erp/titulos").get_data(as_text=True)
    assert "pedido: false" in html.replace("  ", " ")

    html_admin = como(app_real, cenario["admin"].id).get("/erp/titulos").get_data(as_text=True)
    assert "pedido: true" in html_admin.replace("  ", " ")


def test_a_porta_de_entrada_tambem_conhece_as_permissoes(app_real, cenario):
    """A tela de início usa o mesmo molde das outras. Quando ela deixou de
    receber `pode`, o ERP inteiro respondeu 500 na porta de entrada."""
    r = como(app_real, cenario["admin"].id).get("/erp/inicio")
    assert r.status_code == 200
    assert "PODE_ABRIR" in r.get_data(as_text=True)
