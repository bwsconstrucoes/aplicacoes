"""A limpeza por área, contra Postgres de verdade.

Aqui está o que a sessão dublada não consegue provar, e que é justamente o
miolo do desenho:

  - a ORDEM sai das chaves estrangeiras do banco, e apagar nessa ordem funciona
    de fato — sem ela o Postgres recusa no meio e sobra metade;
  - a RECUSA acontece quando algo de fora aponta para o que sairia, em vez de
    apagar em cascata;
  - e, depois de zerar, os CADASTROS continuam lá.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.manutencao import limpeza
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, Obra, PerfilUsuario as P, RegimeTributario,
    TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    FormaPagamento, Parcela, StatusTitulo, TipoTitulo, Titulo,
)

pytestmark = pytest.mark.banco


def _cenario(s):
    """Um título com parcela — o par que prova a ordem (filho antes do pai)."""
    u = Usuario(nome="Admin limpeza", email="limpeza@teste.bws.local",
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    o = Obra(codigo="LIMP-1", nome="Obra limpeza", status="ATIVA")
    f = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                   razao_social="FORNECEDOR LIMPEZA LTDA",
                   regime_tributario=RegimeTributario.NAO_INFORMADO)
    c = Categoria(codigo="9.9.90", descricao="Limpeza teste")
    s.add_all([u, o, f, c])
    s.flush()
    t = Titulo(numero_sp="SP-LIMP-1", tipo=TipoTitulo.T5_EMPREITEIRO,
               fornecedor_id=f.id, descricao="titulo de teste",
               valor_bruto=Decimal("100"), valor_liquido=Decimal("100"),
               competencia=date(2026, 9, 1), categoria_id=c.id,
               forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
               solicitante_id=u.id)
    s.add(t)
    s.flush()
    s.add(Parcela(titulo_id=t.id, numero=1, vencimento=date(2026, 10, 1),
                  valor=Decimal("100")))
    s.flush()
    return u, o, f, c, t


def _conta(s, tabela):
    return int(s.execute(text(f"SELECT count(*) FROM {tabela}")).scalar() or 0)


# ---------------------------------------------------------------------------
# A ordem, lida do banco
# ---------------------------------------------------------------------------
def test_a_ordem_vem_do_banco_e_poe_a_parcela_antes_do_titulo(sessao_real):
    dependencias = limpeza._dependencias(sessao_real)
    ordem = limpeza.ordenar(["titulos", "parcelas"], dependencias)
    assert ordem.index("parcelas") < ordem.index("titulos")


def test_toda_area_so_lista_tabela_que_existe_no_banco(sessao_real):
    """Área que cita tabela inexistente derrubaria a contagem inteira."""
    for chave in limpeza.AREAS:
        tabelas = limpeza._tabelas_de([chave])
        existentes = limpeza._existentes(sessao_real, tabelas)
        assert set(existentes) == set(tabelas), (
            f"a área {chave} cita tabela que não existe: "
            f"{sorted(set(tabelas) - set(existentes))}")


# ---------------------------------------------------------------------------
# Apagar de verdade
# ---------------------------------------------------------------------------
def test_zerar_o_financeiro_leva_titulo_e_parcela_e_deixa_os_cadastros(sessao_real):
    s = sessao_real
    u, o, f, c, t = _cenario(s)

    antes = limpeza.resumo(s, ["financeiro"])
    assert antes["total"] >= 2
    assert antes["impedimentos"] == []

    relatorio = limpeza.zerar(s, ["financeiro"], limpeza.FRASE_DE_CONFIRMACAO, u)
    s.flush()

    assert relatorio["total"] >= 2
    assert _conta(s, "titulos") == 0
    assert _conta(s, "parcelas") == 0
    # E o que jamais pode sair:
    assert s.get(Obra, o.id) is not None
    assert s.get(Fornecedor, f.id) is not None
    assert s.get(Categoria, c.id) is not None
    assert s.get(Usuario, u.id) is not None


def test_zerar_de_novo_nao_reclama(sessao_real):
    s = sessao_real
    u, *_ = _cenario(s)

    limpeza.zerar(s, ["financeiro"], limpeza.FRASE_DE_CONFIRMACAO, u)
    segundo = limpeza.zerar(s, ["financeiro"], limpeza.FRASE_DE_CONFIRMACAO, u)

    assert segundo["total"] == 0


def test_recusa_quando_o_suprimento_aponta_para_o_titulo(sessao_real):
    """Zerar só o financeiro com uma previsão de suprimentos apontando para um
    título tem de PARAR — e dizer qual área falta marcar."""
    from app.apps.erp.db.models.cadastros import PedidoCompra, PrevisaoPagamento

    s = sessao_real
    u, o, f, c, t = _cenario(s)
    pedido = PedidoCompra(numero="PC-LIMP-1", fornecedor_id=f.id, criado_por=u.id)
    s.add(pedido)
    s.flush()
    s.add(PrevisaoPagamento(pedido_id=pedido.id, numero=1,
                            vencimento=date(2026, 10, 1), valor=Decimal("100"),
                            titulo_id=t.id))
    s.flush()

    with pytest.raises(ErroValidacao, match="suprimentos"):
        limpeza.zerar(s, ["financeiro"], limpeza.FRASE_DE_CONFIRMACAO, u)

    assert _conta(s, "titulos") == 1, "nada pode ter saído antes da recusa"


def test_marcando_as_duas_areas_a_limpeza_passa(sessao_real):
    from app.apps.erp.db.models.cadastros import PedidoCompra, PrevisaoPagamento

    s = sessao_real
    u, o, f, c, t = _cenario(s)
    pedido = PedidoCompra(numero="PC-LIMP-2", fornecedor_id=f.id, criado_por=u.id)
    s.add(pedido)
    s.flush()
    s.add(PrevisaoPagamento(pedido_id=pedido.id, numero=1,
                            vencimento=date(2026, 10, 1), valor=Decimal("100"),
                            titulo_id=t.id))
    s.flush()

    limpeza.zerar(s, ["financeiro", "suprimentos"], limpeza.FRASE_DE_CONFIRMACAO, u)
    s.flush()

    assert _conta(s, "titulos") == 0
    assert _conta(s, "pedidos_compra") == 0
    assert s.get(Fornecedor, f.id) is not None


# ---------------------------------------------------------------------------
# ZERAR O CADASTRO DE OBRAS — pedido do dono em 10/09/2026
#
# *"Na parte de banco e limpeza eu vou precisar zerar essas obras. Coloca
# também aqui, pra poder limpar as obras."*
#
# Isto apaga CADASTRO, e o desenho todo é para o estrago ser difícil:
#   - nenhuma área de movimento alcança as obras;
#   - o colaborador NÃO é apagado junto: ele só deixa de estar ligado à obra;
#   - e continua recusando em vez de apagar em cascata.
# ---------------------------------------------------------------------------
def test_zerar_obras_apaga_a_obra_e_solta_o_colaborador(sessao_real):
    from app.apps.erp.db.models.cadastros import Colaborador

    s = sessao_real
    u = Usuario(nome="Admin obras", email="limpobra@teste.bws.local",
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    o = Obra(codigo="LIMP-OBRA", nome="Obra que vai sair", status="ATIVA")
    s.add_all([u, o])
    s.flush()
    pedro = Colaborador(nome="Pedro Pedreiro", cpf="39053344705",
                        regime="CLT", obra_id=o.id)
    s.add(pedro)
    s.flush()

    previa = limpeza.resumo(s, ["cadastro_obras"])
    assert previa["impedimentos"] == [], previa["impedimentos"]
    assert {"tabela": "colaboradores", "coluna": "obra_id", "linhas": 1} \
        in previa["desligamentos"]

    r = limpeza.zerar(s, ["cadastro_obras"], limpeza.FRASE_DE_CONFIRMACAO, u)
    s.flush()
    s.expire_all()

    assert _conta(s, "obras") == 0
    viva = s.get(Colaborador, pedro.id)
    assert viva is not None, "o colaborador NÃO pode sair junto com a obra"
    assert viva.obra_id is None
    assert any(x["tabela"] == "colaboradores" for x in r["desligamentos"])


def test_zerar_obras_recusa_enquanto_houver_titulo_apontando(sessao_real):
    """O rateio do título aponta para a obra. Zerar a obra sem zerar o
    financeiro apagaria a referência de um lançamento vivo."""
    from app.apps.erp.db.models.financeiro import Rateio

    s = sessao_real
    u, o, f, c, t = _cenario(s)
    s.add(Rateio(titulo_id=t.id, obra_id=o.id, categoria_id=c.id,
                 valor=Decimal("100")))
    s.flush()

    with pytest.raises(ErroValidacao) as e:
        limpeza.zerar(s, ["cadastro_obras"], limpeza.FRASE_DE_CONFIRMACAO, u)

    assert "financeiro" in str(e.value)
    assert _conta(s, "obras") >= 1, "nada pode ter saído"


def test_zerar_obras_junto_com_o_financeiro_funciona(sessao_real):
    from app.apps.erp.db.models.financeiro import Rateio

    s = sessao_real
    u, o, f, c, t = _cenario(s)
    s.add(Rateio(titulo_id=t.id, obra_id=o.id, categoria_id=c.id,
                 valor=Decimal("100")))
    s.flush()

    limpeza.zerar(s, ["financeiro", "cadastro_obras"],
                  limpeza.FRASE_DE_CONFIRMACAO, u)
    s.flush()

    assert _conta(s, "obras") == 0
    assert _conta(s, "titulos") == 0
    # e os cadastros que não foram marcados continuam
    assert s.get(Fornecedor, f.id) is not None
    assert s.get(Categoria, c.id) is not None
    assert s.get(Usuario, u.id) is not None


def test_a_designacao_de_obra_do_operador_sai_junto(sessao_real):
    """Deixar o operador apontando para uma obra que não existe mais o banco
    nem permitiria — e a designação sozinha não significa nada."""
    from app.apps.erp.db.models.cadastros import UsuarioObra

    s = sessao_real
    u = Usuario(nome="Supervisor", email="sup.limp@teste.bws.local",
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    o = Obra(codigo="LIMP-DESIG", nome="Obra designada", status="ATIVA")
    s.add_all([u, o])
    s.flush()
    s.add(UsuarioObra(usuario_id=u.id, obra_id=o.id))
    s.flush()

    limpeza.zerar(s, ["cadastro_obras"], limpeza.FRASE_DE_CONFIRMACAO, u)
    s.flush()

    assert _conta(s, "usuario_obras") == 0
    assert s.get(Usuario, u.id) is not None, "o operador continua existindo"


def test_nenhuma_area_de_movimento_leva_obra_junto(sessao_real):
    """Marcar tudo que é movimento não pode fazer uma obra sumir."""
    s = sessao_real
    u = Usuario(nome="Admin", email="mov.limp@teste.bws.local",
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    o = Obra(codigo="LIMP-FICA", nome="Obra que fica", status="ATIVA")
    s.add_all([u, o])
    s.flush()

    movimento = [chave for chave in limpeza.AREAS
                 if chave not in limpeza.AREAS_DE_CADASTRO]
    limpeza.zerar(s, movimento, limpeza.FRASE_DE_CONFIRMACAO, u)
    s.flush()

    assert s.get(Obra, o.id) is not None
