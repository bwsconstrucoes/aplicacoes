"""Varredura adversarial do núcleo financeiro — 11/09/2026.

Pedido do dono, com todas as letras: *"o casamento das informações bancárias de
conciliação, de extratos, com a informação de baixa, isso aí é extremamente
sensível"*.

Cada teste aqui nasceu de uma falha REAL encontrada na varredura e reproduzida
antes de ser corrigida. Eles ficam como rede: se a correção for desfeita por
engano, o teste cai.

COM BANCO DE VERDADE porque tudo o que está sendo provado mora no `WHERE`, em
restrição única ou em trava de linha — e o dublê da suíte ignora os três.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.auth.permissoes import aplicar_escopo, condicao_escopo_sql
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.core.pagamentos import conciliacao as svc_conc
from app.apps.erp.core.pagamentos import service as svc_pag
from app.apps.erp.core.relatorios import resumo
from app.apps.erp.db.models.cadastros import (
    Categoria, ContaBancaria, EscopoVisao, Fornecedor, Obra,
    PerfilUsuario as P, RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
    UsuarioPermissao,
)
from app.apps.erp.db.models.financeiro import (
    Conciliacao, Extrato, FormaPagamento, Pagamento, Parcela, Rateio,
    StatusParcela, StatusTitulo, Titulo, TipoTitulo,
)

pytestmark = pytest.mark.banco


def _titulo(s, *, sp, fornecedor, categoria, obra, solicitante,
            valor="1000.00", parcelas=(("2026-10-01", "1000.00"),),
            status=StatusTitulo.APROVADO):
    v = Decimal(valor)
    t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
               fornecedor_id=fornecedor.id, descricao=f"lançamento {sp}",
               valor_bruto=v, valor_liquido=v, competencia=date(2026, 9, 1),
               categoria_id=categoria.id, forma_pagamento=FormaPagamento.PIX,
               status=status, solicitante_id=solicitante.id)
    s.add(t)
    s.flush()
    for i, (venc, val) in enumerate(parcelas, start=1):
        s.add(Parcela(titulo_id=t.id, numero=i, vencimento=date.fromisoformat(venc),
                      valor=Decimal(val)))
    s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=v,
                 percentual=Decimal("100.0000")))
    s.flush()
    return t


@pytest.fixture
def base(sessao_real):
    """Duas obras, dois supervisores — cada um designado a uma só."""
    s = sessao_real
    chefe = Usuario(nome="Financeiro", email="fin@teste.bws.local",
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.FINANCEIRO)
    sup_a = Usuario(nome="Supervisor A", email="supa@teste.bws.local",
                    senha_hash=gerar_hash("senha-de-teste-1234"),
                    perfil=P.SUPERVISOR_OBRA)
    lancador = Usuario(nome="Lançador", email="lanc@teste.bws.local",
                       senha_hash=gerar_hash("senha-de-teste-1234"),
                       perfil=P.LANCADOR, escopo_visao=EscopoVisao.PROPRIOS)
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="2.1.01", descricao="Material de construção",
                    grupo_codigo="2", grupo_nome="Custo de obra",
                    natureza="RESULTADO")
    obra_a = Obra(codigo="OBRA-A", nome="Creche do Eusébio", cliente="Prefeitura",
                  fase="EM_EXECUCAO")
    obra_b = Obra(codigo="OBRA-B", nome="Escola de Maracanaú", cliente="Prefeitura",
                  fase="EM_EXECUCAO")
    conta1 = ContaBancaria(descricao="Bradesco", banco_codigo="237",
                           agencia="1", conta="111")
    conta2 = ContaBancaria(descricao="Itaú", banco_codigo="341",
                           agencia="2", conta="222")
    s.add_all([chefe, sup_a, lancador, forn, cat, obra_a, obra_b, conta1, conta2])
    s.flush()
    s.add(UsuarioObra(usuario_id=sup_a.id, obra_id=obra_a.id))
    s.flush()
    return {"s": s, "chefe": chefe, "sup_a": sup_a, "lancador": lancador,
            "forn": forn, "cat": cat, "obra_a": obra_a, "obra_b": obra_b,
            "conta1": conta1, "conta2": conta2}


# ---------------------------------------------------------------------------
# 1. Relatórios respeitam o escopo — quem enxerga uma obra não soma a empresa
# ---------------------------------------------------------------------------
def test_relatorio_nao_mostra_obra_alheia(base):
    """A falha: o supervisor da obra A abria Relatórios e via o custo da obra B.

    Os relatórios somam com SQL escrito à mão e nunca passaram pelo recorte que
    as listagens usam. Era silencioso: número certo, obra errada.
    """
    d = base
    s = d["s"]
    _titulo(s, sp="SP-A1", fornecedor=d["forn"], categoria=d["cat"],
            obra=d["obra_a"], solicitante=d["chefe"], valor="1000.00")
    _titulo(s, sp="SP-B1", fornecedor=d["forn"], categoria=d["cat"],
            obra=d["obra_b"], solicitante=d["chefe"], valor="7000.00")
    s.flush()

    tudo = resumo(s, "obra", {}, d["chefe"])
    assert round(tudo["total"], 2) == 8000.00

    so_a = resumo(s, "obra", {}, d["sup_a"])
    chaves = [l["chave"] for l in so_a["linhas"]]
    assert round(so_a["total"], 2) == 1000.00, "supervisor somou obra que não é dele"
    assert all("OBRA-B" not in c for c in chaves)


def test_escopo_sql_igual_ao_orm(base):
    """As duas escritas do MESMO recorte têm de devolver os mesmos títulos.

    Uma é para consulta do SQLAlchemy (listagens), a outra é pedaço de WHERE
    (relatórios). Se alguém mudar só uma, este teste acusa.
    """
    from sqlalchemy import text

    d = base
    s = d["s"]
    _titulo(s, sp="SP-A1", fornecedor=d["forn"], categoria=d["cat"],
            obra=d["obra_a"], solicitante=d["chefe"])
    _titulo(s, sp="SP-B1", fornecedor=d["forn"], categoria=d["cat"],
            obra=d["obra_b"], solicitante=d["chefe"])
    _titulo(s, sp="SP-L1", fornecedor=d["forn"], categoria=d["cat"],
            obra=d["obra_b"], solicitante=d["lancador"])
    s.flush()

    for quem in (d["chefe"], d["sup_a"], d["lancador"]):
        pelo_orm = set(s.scalars(aplicar_escopo(select(Titulo.id), s, quem)).all())
        onde, params = condicao_escopo_sql(s, quem)
        pelo_sql = {r[0] for r in s.execute(
            text(f"SELECT t.id FROM titulos t WHERE {onde}"), params)}
        assert pelo_orm == pelo_sql, f"recortes divergiram para {quem.perfil.value}"


def test_pago_e_aberto_olham_o_pagamento_e_nao_a_situacao(base):
    """A falha: título de duas parcelas com UMA paga aparecia com o valor
    inteiro em aberto e zero pago, porque a conta olhava a situação do título
    (PAGO_PARCIAL ≠ 'PAGO') em vez de somar os pagamentos."""
    d = base
    s = d["s"]
    t = _titulo(s, sp="SP-MEIO", fornecedor=d["forn"], categoria=d["cat"],
                obra=d["obra_a"], solicitante=d["chefe"], valor="1000.00",
                parcelas=(("2026-10-01", "600.00"), ("2026-11-01", "400.00")))
    primeira = s.scalars(select(Parcela).where(
        Parcela.titulo_id == t.id, Parcela.numero == 1)).one()
    svc_pag.registrar_pagamento(
        s, parcela_id=primeira.id, conta_bancaria_id=d["conta1"].id,
        data_pagamento=date(2026, 10, 1), usuario=d["chefe"])
    s.flush()

    r = resumo(s, "obra", {}, d["chefe"])
    assert round(r["total_pago"], 2) == 600.00
    assert round(r["total_aberto"], 2) == 400.00


# ---------------------------------------------------------------------------
# 2. Baixa: uma parcela, um pagamento — e só dentro do escopo
# ---------------------------------------------------------------------------
def test_parcela_nao_aceita_dois_pagamentos(base):
    """Sem a restrição do banco, dois cliques simultâneos gravavam a saída
    duas vezes. A conferência em Python não bastava: ela lê antes de gravar."""
    d = base
    s = d["s"]
    t = _titulo(s, sp="SP-DUPLA", fornecedor=d["forn"], categoria=d["cat"],
                obra=d["obra_a"], solicitante=d["chefe"])
    parcela = s.scalars(select(Parcela).where(Parcela.titulo_id == t.id)).one()
    svc_pag.registrar_pagamento(
        s, parcela_id=parcela.id, conta_bancaria_id=d["conta1"].id,
        data_pagamento=date(2026, 10, 1), usuario=d["chefe"])
    s.flush()

    # o caminho normal já barra pela situação da parcela
    with pytest.raises(ErroValidacao):
        svc_pag.registrar_pagamento(
            s, parcela_id=parcela.id, conta_bancaria_id=d["conta1"].id,
            data_pagamento=date(2026, 10, 1), usuario=d["chefe"])

    # e o banco barra mesmo quem grave direto, sem passar pela conferência
    s.add(Pagamento(parcela_id=parcela.id, conta_bancaria_id=d["conta1"].id,
                    data_pagamento=date(2026, 10, 2), valor_pago=Decimal("1000.00"),
                    meio=FormaPagamento.PIX, executado_por=d["chefe"].id))
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_baixa_fora_do_escopo_responde_nao_encontrado(base, app_real):
    """Ter alçada para pagar não autoriza a pagar a parcela da obra de outro.

    A falha: a rota de baixa NUNCA conferiu o escopo do objeto. Não bastava a
    ação "pagar" ser estreita — ela pode ser concedida a uma pessoa presa a
    obra pelo cadastro (migração 032), e foi assim que este teste montou o
    cenário. E a recusa tem de ser "não encontrado": dizer "sem permissão"
    para um número que existe confirmaria que ele existe.
    """
    from tests.conftest import como

    d = base
    s = d["s"]
    s.add(UsuarioPermissao(usuario_id=d["sup_a"].id, acao="pagar", concedida=True))
    s.flush()
    minha = _titulo(s, sp="SP-A9", fornecedor=d["forn"], categoria=d["cat"],
                    obra=d["obra_a"], solicitante=d["chefe"])
    alheia = _titulo(s, sp="SP-B9", fornecedor=d["forn"], categoria=d["cat"],
                     obra=d["obra_b"], solicitante=d["chefe"])
    p_minha = s.scalars(select(Parcela).where(Parcela.titulo_id == minha.id)).one()
    p_alheia = s.scalars(select(Parcela).where(Parcela.titulo_id == alheia.id)).one()

    cliente = como(app_real, d["sup_a"].id)
    corpo = {"conta_bancaria_id": d["conta1"].id, "data_pagamento": "2026-10-01",
             "avisar": False}

    r = cliente.post("/erp/api/pagamentos/baixar",
                     json=dict(corpo, itens=[{"parcela_id": p_alheia.id}]))
    assert r.status_code == 404, "pagou parcela de obra que não é dele"
    assert s.get(Parcela, p_alheia.id).status != StatusParcela.PAGA

    r = cliente.post("/erp/api/pagamentos/baixar",
                     json=dict(corpo, itens=[{"parcela_id": p_minha.id}]))
    assert r.status_code == 200 and r.get_json()["ok"]


# ---------------------------------------------------------------------------
# 3. Conciliação: o casamento do extrato com a baixa
# ---------------------------------------------------------------------------
def _pagar(d, obra, sp, conta):
    s = d["s"]
    t = _titulo(s, sp=sp, fornecedor=d["forn"], categoria=d["cat"], obra=obra,
                solicitante=d["chefe"], valor="1000.00")
    parcela = s.scalars(select(Parcela).where(Parcela.titulo_id == t.id)).one()
    pg = svc_pag.registrar_pagamento(
        s, parcela_id=parcela.id, conta_bancaria_id=conta.id,
        data_pagamento=date(2026, 10, 1), usuario=d["chefe"])
    s.flush()
    return pg


def _linha(d, conta, hashlinha, valor="-1000.00"):
    s = d["s"]
    e = Extrato(conta_bancaria_id=conta.id, data_lancamento=date(2026, 10, 1),
                valor=Decimal(valor), historico="PIX CONSTRUTORA ALFA",
                nome_contraparte="CONSTRUTORA ALFA LTDA", hash_linha=hashlinha)
    s.add(e)
    s.flush()
    return e


def test_nao_concilia_entre_contas_diferentes(base):
    """A linha do banco X só comprova pagamento saído do banco X. A tela já só
    oferecia candidatos da mesma conta — mas quem GRAVA é a função, e listagem
    não é trava."""
    d = base
    pg = _pagar(d, d["obra_a"], "SP-CC1", d["conta1"])
    linha_outra_conta = _linha(d, d["conta2"], "hash-outra-conta")
    with pytest.raises(ErroValidacao) as erro:
        svc_conc.conciliar_manual(d["s"], pg.id, linha_outra_conta.id, d["chefe"])
    assert "outra conta" in str(erro.value).lower()


def test_linha_ja_conciliada_recusa_em_portugues(base):
    """Antes, a segunda tentativa chegava ao banco e voltava com texto de
    programador ('duplicate key value violates unique constraint') na tela."""
    d = base
    s = d["s"]
    pg1 = _pagar(d, d["obra_a"], "SP-CC2", d["conta1"])
    pg2 = _pagar(d, d["obra_a"], "SP-CC3", d["conta1"])
    linha = _linha(d, d["conta1"], "hash-unica")
    svc_conc.conciliar_manual(s, pg1.id, linha.id, d["chefe"])
    s.flush()
    with pytest.raises(ErroValidacao) as erro:
        svc_conc.conciliar_manual(s, pg2.id, linha.id, d["chefe"])
    texto = str(erro.value)
    assert "já está conciliada" in texto
    assert "constraint" not in texto.lower()


def test_conciliacao_desfeita_libera_a_linha(base):
    """A migração 031 prometeu: "desfeita, a linha volta a ficar livre". As
    restrições antigas da tabela nunca foram removidas e desmentiam isso — o
    sistema oferecia a linha como livre e o banco recusava."""
    d = base
    s = d["s"]
    pg = _pagar(d, d["obra_a"], "SP-CC4", d["conta1"])
    linha = _linha(d, d["conta1"], "hash-desfaz")
    c = svc_conc.conciliar_manual(s, pg.id, linha.id, d["chefe"])
    s.flush()
    svc_pag.desfazer_conciliacao(s, c.id, "erro de digitação na baixa", d["chefe"])
    s.flush()

    livres = [e.id for e in svc_pag.extratos_nao_conciliados(s)]
    assert linha.id in livres, "a tela oferece a linha como livre"
    svc_conc.conciliar_manual(s, pg.id, linha.id, d["chefe"])
    s.flush()   # e agora o banco aceita, como a 031 prometia
    vigentes = s.scalars(select(Conciliacao.id).where(
        Conciliacao.extrato_id == linha.id,
        Conciliacao.desfeita_em.is_(None))).all()
    assert len(vigentes) == 1
