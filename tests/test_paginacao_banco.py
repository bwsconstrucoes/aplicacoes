"""A paginação das listas, e os dois defeitos que ela corrigiu — com banco.

As listas do ERP paravam nos 500 registros mais novos e não diziam. O corte em
si era um incômodo; o que estava por baixo dele era pior:

  - o FILTRO de situação era aplicado em Python DEPOIS de trazer os 500 mais
    novos. Filtrar por "bloqueado" não achava nada se os 500 mais novos não
    tivessem nenhum — mesmo havendo dezenas mais antigos;
  - as SOMAS DO TOPO somavam só esses 500 e se apresentavam como "total". Uma
    lista cortada é um incômodo; um total que soma metade da base e se chama
    total é um número que MENTE — e ninguém confere um número que o sistema deu.

O que se prova:

  1. A paginação devolve a página pedida e o total de verdade.
  2. O filtro de situação alcança o que é ANTIGO.
  3. As somas do topo cobrem o filtro inteiro, não a página.
  4. Página, contagem e somas nunca divergem — vêm da mesma consulta.
  5. O escopo por obra continua valendo em todas as três.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum import paginacao
from app.apps.erp.core.titulos import service as svc
from app.apps.erp.db.models.cadastros import (Categoria, EscopoVisao, Fornecedor,
                                              Obra, PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa,
                                              Usuario, UsuarioObra)
from app.apps.erp.db.models.financeiro import (EspecieTitulo, FormaPagamento,
                                               Parcela, Rateio, StatusParcela,
                                               StatusTitulo, TipoTitulo, Titulo)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Financeiro", email="pag@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    forn = Fornecedor(razao_social="Fornecedor Exemplo", cnpj_cpf="33444555000166",
                      tipo_pessoa=TipoPessoa.PJ, ativo=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra_a = Obra(codigo="OBRA-A", nome="Obra A")
    obra_b = Obra(codigo="OBRA-B", nome="Obra B")
    cat = Categoria(codigo="3.1.02", descricao="Material")
    s.add_all([u, forn, obra_a, obra_b, cat])
    s.flush()
    return {"s": s, "usuario": u, "fornecedor": forn, "obra_a": obra_a,
            "obra_b": obra_b, "categoria": cat}


def _titulo(cenario, n, *, status=StatusTitulo.APROVADO, valor="1000.00",
            obra=None, vencimento=None):
    s = cenario["s"]
    t = Titulo(numero_sp=f"SP-{n:05d}", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.PAGAR, fornecedor_id=cenario["fornecedor"].id,
               descricao=f"Compra {n}", valor_bruto=Decimal(valor),
               valor_liquido=Decimal(valor), competencia=date(2026, 8, 1),
               categoria_id=cenario["categoria"].id,
               forma_pagamento=FormaPagamento.PIX, status=status,
               solicitante_id=cenario["usuario"].id)
    s.add(t)
    s.flush()
    s.add(Parcela(titulo_id=t.id, numero=1,
                  vencimento=vencimento or date(2026, 12, 1),
                  valor=Decimal(valor), status=StatusParcela.ABERTA))
    s.add(Rateio(titulo_id=t.id, obra_id=(obra or cenario["obra_a"]).id,
                 valor=Decimal(valor), percentual=Decimal("100")))
    s.flush()
    return t


# ---------------------------------------------------------------------------
# 1. A paginação
# ---------------------------------------------------------------------------
def test_a_pagina_traz_o_pedido_e_o_total_de_verdade(cenario):
    s = cenario["s"]
    for n in range(1, 26):
        _titulo(cenario, n)

    p1 = svc.pagina_de_titulos(s, pagina=1, tamanho=10)
    assert len(p1["itens"]) == 10
    assert p1["total"] == 25
    assert p1["paginas"] == 3
    assert p1["tem_mais"] is True
    assert p1["resumo"] == "1–10 de 25"

    p3 = svc.pagina_de_titulos(s, pagina=3, tamanho=10)
    assert len(p3["itens"]) == 5
    assert p3["tem_mais"] is False
    assert p3["resumo"] == "21–25 de 25"


def test_a_pagina_alem_do_fim_devolve_vazio_sem_quebrar(cenario):
    s = cenario["s"]
    _titulo(cenario, 1)
    p = svc.pagina_de_titulos(s, pagina=99, tamanho=10)
    assert p["itens"] == []
    assert p["total"] == 1
    assert p["resumo"] == "nenhum registro"


def test_as_paginas_nao_repetem_nem_pulam(cenario):
    """O erro clássico de paginação: o mesmo registro em duas páginas."""
    s = cenario["s"]
    for n in range(1, 31):
        _titulo(cenario, n)
    vistos = []
    for pg in (1, 2, 3):
        vistos += [t.numero_sp for t in
                   svc.pagina_de_titulos(s, pagina=pg, tamanho=10)["itens"]]
    assert len(vistos) == 30
    assert len(set(vistos)) == 30, "nenhum repetido"


def test_o_tamanho_da_pagina_tem_teto(cenario):
    """Memória dividida com treze módulos: consulta gulosa derruba o vizinho."""
    s = cenario["s"]
    _titulo(cenario, 1)
    p = svc.pagina_de_titulos(s, pagina=1, tamanho=99999)
    assert p["tamanho"] == paginacao.TETO


def test_tamanho_e_pagina_estranhos_nao_quebram(cenario):
    s = cenario["s"]
    _titulo(cenario, 1)
    p = svc.pagina_de_titulos(s, pagina="abacaxi", tamanho="")
    assert p["pagina"] == 1
    assert p["tamanho"] == paginacao.TAMANHO


# ---------------------------------------------------------------------------
# 2. O filtro alcança o que é antigo
# ---------------------------------------------------------------------------
def test_o_filtro_de_situacao_acha_o_titulo_antigo(cenario):
    """Este é o defeito: filtrar por "bloqueado" não achava nada se os títulos
    mais novos não tivessem nenhum — mesmo havendo um antigo."""
    s = cenario["s"]
    antigo = _titulo(cenario, 1, status=StatusTitulo.BLOQUEADO)
    for n in range(2, 40):
        _titulo(cenario, n, status=StatusTitulo.APROVADO)

    p = svc.pagina_de_titulos(s, status=["BLOQUEADO"], pagina=1, tamanho=10)
    assert p["total"] == 1
    assert p["itens"][0].id == antigo.id


def test_o_filtro_aceita_mais_de_uma_situacao(cenario):
    s = cenario["s"]
    _titulo(cenario, 1, status=StatusTitulo.BLOQUEADO)
    _titulo(cenario, 2, status=StatusTitulo.AGUARDANDO_APROVACAO)
    _titulo(cenario, 3, status=StatusTitulo.APROVADO)
    p = svc.pagina_de_titulos(s, status=["BLOQUEADO", "AGUARDANDO_APROVACAO"])
    assert p["total"] == 2


def test_a_busca_entra_na_contagem(cenario):
    s = cenario["s"]
    for n in range(1, 15):
        _titulo(cenario, n)
    _titulo(cenario, 99)
    p = svc.pagina_de_titulos(s, busca="SP-00099", tamanho=5)
    assert p["total"] == 1


# ---------------------------------------------------------------------------
# 3 e 4. As somas cobrem o filtro inteiro
# ---------------------------------------------------------------------------
def test_as_somas_cobrem_tudo_e_nao_so_a_pagina(cenario):
    """Um total que soma metade da base e se chama total é um número que
    mente — e ninguém confere um número que o sistema deu."""
    s = cenario["s"]
    for n in range(1, 51):
        _titulo(cenario, n, valor="100.00")

    p = svc.pagina_de_titulos(s, pagina=1, tamanho=10)
    somas = svc.somar_titulos(s)
    assert len(p["itens"]) == 10, "a tela mostra dez"
    assert somas["quantidade"] == 50, "mas a soma conhece os cinquenta"
    assert somas["total"] == 5000.00


def test_as_somas_respeitam_o_mesmo_filtro_da_pagina(cenario):
    """Página, contagem e somas vêm da mesma consulta — não podem divergir."""
    s = cenario["s"]
    for n in range(1, 6):
        _titulo(cenario, n, status=StatusTitulo.BLOQUEADO, valor="200.00")
    for n in range(6, 20):
        _titulo(cenario, n, status=StatusTitulo.APROVADO, valor="100.00")

    filtros = {"status": ["BLOQUEADO"]}
    p = svc.pagina_de_titulos(s, tamanho=2, **filtros)
    somas = svc.somar_titulos(s, **filtros)
    assert p["total"] == somas["quantidade"] == 5
    assert somas["total"] == 1000.00
    assert somas["bloqueado"] == 1000.00
    assert somas["qtd_bloqueado"] == 5


def test_a_soma_de_vencendo_usa_o_vencimento_mais_proximo(cenario):
    s = cenario["s"]
    perto = date.today() + timedelta(days=3)
    _titulo(cenario, 1, vencimento=perto, valor="500.00")
    _titulo(cenario, 2, vencimento=date.today() + timedelta(days=90), valor="700.00")
    somas = svc.somar_titulos(s)
    assert somas["qtd_vencendo"] == 1
    assert somas["vencendo"] == 500.00


def test_titulo_pago_nao_conta_como_vencendo(cenario):
    """"Vencem em 7 dias" é sobre o que ainda vai sair da conta."""
    s = cenario["s"]
    _titulo(cenario, 1, status=StatusTitulo.PAGO,
            vencimento=date.today() + timedelta(days=2))
    assert svc.somar_titulos(s)["qtd_vencendo"] == 0


# ---------------------------------------------------------------------------
# 5. O escopo continua valendo
# ---------------------------------------------------------------------------
def test_o_escopo_por_obra_vale_na_pagina_e_na_soma(cenario):
    """Se o escopo valesse só na lista, o total do topo entregaria o valor das
    obras que a pessoa não pode ver."""
    s = cenario["s"]
    for n in range(1, 6):
        _titulo(cenario, n, obra=cenario["obra_a"], valor="100.00")
    for n in range(6, 9):
        _titulo(cenario, n, obra=cenario["obra_b"], valor="100.00")

    supervisor = Usuario(nome="Supervisor", email="pag-sup@teste.local", ativo=True,
                         senha_hash=gerar_hash("senha-de-teste-123"),
                         perfil=P.SUPERVISOR_OBRA, escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
    s.add(supervisor)
    s.flush()
    s.add(UsuarioObra(usuario_id=supervisor.id, obra_id=cenario["obra_a"].id))
    s.flush()

    p = svc.pagina_de_titulos(s, usuario=supervisor, tamanho=50)
    somas = svc.somar_titulos(s, usuario=supervisor)
    assert p["total"] == 5, "só a obra designada"
    assert somas["quantidade"] == 5
    assert somas["total"] == 500.00

    tudo = svc.somar_titulos(s, usuario=cenario["usuario"])
    assert tudo["quantidade"] == 8, "o financeiro vê as duas obras"


def test_todos_os_quadrinhos_saem_da_mesma_conta(cenario):
    """Metade somando a base inteira e metade somando a página deixaria dois
    números diferentes sobre a mesma coisa na MESMA tela — pior que os dois
    errados, porque quem vê não sabe em qual acreditar."""
    s = cenario["s"]
    for n in range(1, 41):
        _titulo(cenario, n, status=StatusTitulo.APROVADO, valor="100.00")
    _titulo(cenario, 90, status=StatusTitulo.BLOQUEADO, valor="50.00",
            vencimento=date.today() - timedelta(days=3))

    somas = svc.somar_titulos(s)
    assert somas["qtd_aprovado"] == 40
    assert somas["aprovado"] == 4000.00
    assert somas["qtd_atrasado"] == 1, "vencido e ainda aberto"
    assert somas["atrasado"] == 50.00
    assert somas["por_situacao"]["APROVADO"] == 40
    assert somas["por_situacao"]["BLOQUEADO"] == 1
    # e a soma das partes fecha com o todo
    assert sum(somas["por_situacao"].values()) == somas["quantidade"] == 41


def test_a_contagem_por_situacao_enxerga_o_antigo(cenario):
    """A caixinha do filtro contava só o que estava carregado: uma situação
    sem registro na página aparecia zerada, e isso desencoraja o clique
    justamente quando há registros mais antigos."""
    s = cenario["s"]
    _titulo(cenario, 1, status=StatusTitulo.BLOQUEADO)
    for n in range(2, 60):
        _titulo(cenario, n, status=StatusTitulo.APROVADO)

    somas = svc.somar_titulos(s)
    assert somas["por_situacao"]["BLOQUEADO"] == 1, \
        "mesmo sendo o mais antigo de todos"
