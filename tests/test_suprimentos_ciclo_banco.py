"""O ciclo da compra visto de ponta a ponta — com banco de verdade.

O dono, em 20/11/2026, frisou dois problemas que existem hoje:

  1. "o fechamento do ciclo, que se completa com a entrega do material de
     forma integral" — e a pendência tem de chegar ao coordenador;
  2. "muitas vezes chega material e a obra atrasa o lançamento das notas e
     boleto. E o financeiro fica cego da informação."

O que se prova aqui é que o sistema ENXERGA cada um desses pontos, cruzando o
que outras telas gravam: o recebimento da obra, a nota vinculada ao pedido e
o título que nasceu da nota. E, junto, o local de entrega decidido no
fechamento ("prever quando a entrega não for na obra, por exemplo na sede").

Com banco porque tudo aqui é `JOIN` e `WHERE` — o dublê devolveria todos os
pedidos para todo mundo e o teste passaria por motivo errado.
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest
from conftest import hoje
from sqlalchemy import select

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import empresas as svc_emp
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.suprimentos import acompanhamento as svc_acomp
from app.apps.erp.core.suprimentos import cotacao as svc_cot
from app.apps.erp.core.suprimentos import envio as svc_envio
from app.apps.erp.core.suprimentos import pedido as svc_ped
from app.apps.erp.core.suprimentos import projecao as svc_proj
from app.apps.erp.core.suprimentos import recebimento as svc_rec
from app.apps.erp.db.models.cadastros import (
    Categoria, CondicaoPagamento, CotacaoFornecedor, CotacaoItem,
    Fornecedor, Insumo, InsumoCategoria, Obra, PedidoItem, PerfilUsuario as P,
    RegimeTributario, SuprimentoItem, SuprimentoSolicitacao, TipoPessoa, Usuario,
    UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    DocumentoFiscal, FormaPagamento, Parcela, SituacaoNota, StatusParcela,
    StatusTitulo, TipoDocFiscal, TipoTitulo, Titulo,
)

pytestmark = pytest.mark.banco



@pytest.fixture
def chave(monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setenv("ERP_CHAVE_SEGREDOS", Fernet.generate_key().decode())


@pytest.fixture
def base(sessao_real, chave):
    """Duas obras da mesma empresa, um fornecedor, dois insumos e uma cotação
    com os dois itens (um de cada obra) já com preço: 10 × 100 e 20 × 50."""
    s = sessao_real
    u = Usuario(nome="Coordenador", email="coordenador@teste.bws.local",
                senha_hash=gerar_hash("senha-de-teste-1234"), perfil=P.ADMIN)
    s.add(u); s.flush()
    empresa = svc_emp.criar(s, {
        "razao_social": "BWS TESTE LTDA", "cnpj": "71000001000184",
        "nome_fantasia": "BWS", "logradouro": "Rua da Sede", "numero": "10",
        "bairro": "Centro", "municipio": "Eusébio", "uf": "CE",
        "cep": "61760-000"}, u)
    s.flush()
    obra_a = Obra(codigo="CICLO-A", nome="Creche A", status="ATIVA",
                  empresa_id=empresa.id, endereco="Rua Projetada 11",
                  bairro="Três Marias", municipio="Floresta", uf="PE",
                  cep="56408-024")
    obra_b = Obra(codigo="CICLO-B", nome="Escola B", status="ATIVA",
                  empresa_id=empresa.id, endereco="Av. Central",
                  numero_endereco="900", municipio="Barbalha", uf="CE")
    cat = InsumoCategoria(codigo="CIC-1", nome="Pedras")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="71000005000150",
                      razao_social="GRANITOS DO TESTE LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO,
                      email="vendas@granitos.exemplo", canais_cotacao=["EMAIL"],
                      regioes_atuacao=[])
    cond = CondicaoPagamento(nome="30/60 dias (teste do ciclo)", entrada_percentual=0,
                             dias=[30, 60])
    s.add_all([obra_a, obra_b, cat, forn, cond]); s.flush()
    bancada = Insumo(codigo="CIC-BAN", descricao="Bancada de granito",
                     unidade="M2", categoria_insumo_id=cat.id)
    peitoril = Insumo(codigo="CIC-PEI", descricao="Peitoril de granito",
                      unidade="M2", categoria_insumo_id=cat.id)
    s.add_all([bancada, peitoril]); s.flush()

    sol = SuprimentoSolicitacao(numero="SS-CICLO-1", titulo="granitos",
                                solicitante_id=u.id,
                                previsao_entrega=hoje() + timedelta(days=20))
    s.add(sol); s.flush()
    item_a = SuprimentoItem(solicitacao_id=sol.id, numero=1, insumo_id=bancada.id,
                            quantidade=Decimal("10"), unidade="M2", obra_id=obra_a.id)
    item_b = SuprimentoItem(solicitacao_id=sol.id, numero=2, insumo_id=peitoril.id,
                            quantidade=Decimal("20"), unidade="M2", obra_id=obra_b.id)
    s.add_all([item_a, item_b]); s.flush()

    cot = svc_cot.criar(s, {"titulo": "Granitos", "itens": [item_a.id, item_b.id]}, u)
    s.flush()
    svc_cot.adicionar_fornecedor(s, cot.id, {"fornecedor_id": forn.id}, u)
    s.flush()
    coluna = s.scalars(select(CotacaoFornecedor).where(
        CotacaoFornecedor.cotacao_id == cot.id)).one()
    coluna.condicao_pagamento_id = cond.id
    linhas = {l.suprimento_item_id: l for l in s.scalars(select(CotacaoItem).where(
        CotacaoItem.cotacao_id == cot.id)).all()}
    svc_cot.lancar_preco(s, coluna.id, linhas[item_a.id].id, "100,00", u)
    svc_cot.lancar_preco(s, coluna.id, linhas[item_b.id].id, "50,00", u)
    s.flush()
    return {"s": s, "u": u, "empresa": empresa, "obra_a": obra_a, "obra_b": obra_b,
            "forn": forn, "cond": cond, "cot": cot, "coluna": coluna,
            "linha_a": linhas[item_a.id], "linha_b": linhas[item_b.id],
            "item_a": item_a, "item_b": item_b}


def _fechar(b, *, linhas=None, autorizar=True, previsao=None, **dados):
    s = b["s"]
    pedido = svc_ped.fechar_do_mapa(
        s, b["cot"].id, b["coluna"].id,
        [l.id for l in (linhas or [b["linha_a"], b["linha_b"]])],
        {"previsao_entrega": (previsao or hoje() + timedelta(days=10)).isoformat(),
         **dados}, b["u"])
    s.flush()
    if autorizar:
        svc_ped.autorizar(s, pedido.id, b["u"])
        s.flush()
    return pedido


def _receber(b, pedido, quantidades, *, dias_atras=0):
    s = b["s"]
    linhas = {l.suprimento_item_id: l for l in s.scalars(select(PedidoItem).where(
        PedidoItem.pedido_id == pedido.id)).all()}
    svc_rec.registrar(s, pedido.id, {
        "data": (hoje() - timedelta(days=dias_atras)).isoformat(),
        "itens": [{"pedido_item_id": linhas[item.id].id, "quantidade": str(qtd)}
                  for item, qtd in quantidades]}, b["u"])
    s.flush()


def _nota(b, pedido, valor, *, numero="1001"):
    n = DocumentoFiscal(tipo=TipoDocFiscal.NFE, numero=numero, serie="1",
                        chave_acesso=f"{numero:0>44}",
                        emitente_doc="71000005000150", valor_total=Decimal(valor),
                        data_emissao=hoje(), situacao=SituacaoNota.AUTORIZADA,
                        origem="TESTE", pedido_compra_id=pedido.id)
    b["s"].add(n); b["s"].flush()
    return n


def _titulo(b, nota, valor, *, pago=False, sp="SP-CICLO-1"):
    s = b["s"]
    cat = s.scalars(select(Categoria)).first()
    if cat is None:
        cat = Categoria(codigo="2.1.99", descricao="Material")
        s.add(cat); s.flush()
    t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
               fornecedor_id=b["forn"].id, descricao="granitos",
               valor_bruto=Decimal(valor), valor_liquido=Decimal(valor),
               competencia=hoje().replace(day=1), categoria_id=cat.id,
               forma_pagamento=FormaPagamento.PIX,
               status=StatusTitulo.PAGO if pago else StatusTitulo.APROVADO,
               solicitante_id=b["u"].id, documento_fiscal_id=nota.id)
    s.add(t); s.flush()
    s.add(Parcela(titulo_id=t.id, numero=1, valor=Decimal(valor),
                  vencimento=hoje() + timedelta(days=30),
                  status=StatusParcela.PAGA if pago else StatusParcela.ABERTA))
    s.flush()
    return t


def _linha(b, pedido, **kw):
    d = svc_acomp.painel(b["s"], b["u"], incluir_fechados=True, **kw)
    return next(p for p in d["pedidos"] if p["id"] == pedido.id), d


# ---------------------------------------------------------------------------
# O local de entrega, decidido no fechamento
# ---------------------------------------------------------------------------
def test_sem_dizer_nada_a_entrega_e_na_obra_de_cada_item(base):
    pedido = _fechar(base)
    texto = svc_envio.montar_pedido(base["s"], pedido.id, base["empresa"])["corpo"]
    assert "ENTREGAR EM: Rua Projetada 11, Três Marias, Floresta, PE - CEP 56408-024" in texto
    assert "ENTREGAR EM: Av. Central, 900, Barbalha, CE" in texto
    assert "NÃO é no canteiro" not in texto
    # um bloco por endereço, cada um com o item da sua obra
    assert texto.index("Rua Projetada 11") < texto.index("Bancada de granito") \
        < texto.index("Av. Central") < texto.index("Peitoril de granito")


def test_a_sede_recebe_o_material_de_uma_obra_e_a_outra_continua_no_canteiro(base):
    pedido = _fechar(base, locais_entrega=[
        {"obra_id": base["obra_a"].id, "tipo": "SEDE"}])
    texto = svc_envio.montar_pedido(base["s"], pedido.id, base["empresa"])["corpo"]
    assert "ENTREGAR EM: Rua da Sede, 10, Centro, Eusébio, CE - CEP 61760-000" in texto
    assert "Sede — BWS" in texto and "NÃO é no canteiro" in texto
    assert "Material da(s) obra(s): CICLO-A" in texto
    assert "Rua Projetada 11" not in texto          # a obra A não recebe lá
    assert "ENTREGAR EM: Av. Central, 900, Barbalha, CE" in texto   # a B, sim
    detalhe = svc_ped.detalhar(base["s"], pedido.id)
    por_obra = {i["obra"]: i["entrega_em"] for i in detalhe["itens"]}
    assert por_obra["CICLO-A"]["na_obra"] is False
    assert por_obra["CICLO-B"]["na_obra"] is True


def test_as_duas_obras_na_sede_viram_um_bloco_so(base):
    pedido = _fechar(base, locais_entrega=[
        {"obra_id": base["obra_a"].id, "tipo": "SEDE"},
        {"obra_id": base["obra_b"].id, "tipo": "SEDE"}])
    rel = svc_ped.relatorio_para_o_fornecedor(base["s"], pedido.id)
    assert len(rel["locais"]) == 1
    assert len(rel["locais"][0]["itens"]) == 2
    assert rel["locais"][0]["na_obra"] is False


def test_outro_endereco_exige_o_endereco_escrito(base):
    with pytest.raises(ErroValidacao, match="endereço completo"):
        _fechar(base, locais_entrega=[
            {"obra_id": base["obra_a"].id, "tipo": "OUTRO", "endereco": "ali"}])


def test_local_de_obra_que_nao_esta_no_pedido_e_recusado(base):
    with pytest.raises(ErroValidacao, match="não está neste pedido"):
        _fechar(base, linhas=[base["linha_a"]], locais_entrega=[
            {"obra_id": base["obra_b"].id, "tipo": "SEDE"}])


def test_o_local_muda_depois_do_fechamento_mas_nao_depois_de_chegar_material(base):
    pedido = _fechar(base)
    svc_ped.redefinir_entrega(base["s"], pedido.id, [
        {"obra_id": base["obra_b"].id, "tipo": "OUTRO", "rotulo": "Depósito",
         "endereco": "Rodovia CE-040, km 12, Aquiraz - CE"}], base["u"])
    base["s"].flush()
    texto = svc_envio.montar_pedido(base["s"], pedido.id, base["empresa"])["corpo"]
    assert "Rodovia CE-040, km 12" in texto and "Local: Depósito" in texto

    _receber(base, pedido, [(base["item_a"], 10)])
    with pytest.raises(ErroValidacao, match="Já chegou material"):
        svc_ped.redefinir_entrega(base["s"], pedido.id, [], base["u"])


def test_o_pedido_leva_as_instrucoes_da_nota_e_para_quem_faturar(base):
    pedido = _fechar(base)
    texto = svc_envio.montar_pedido(base["s"], pedido.id, base["empresa"])["corpo"]
    assert "Faturar para: BWS TESTE LTDA — CNPJ 71.000.001/0001-84" in texto
    assert f"Pedido BWS {pedido.numero}" in texto
    assert "Faturar apenas os itens listados" in texto
    assert "(solicitação SS-CICLO-1)" in texto


def test_o_fechamento_mostra_o_que_esta_livre_e_o_que_ja_virou_pedido(base):
    antes = svc_ped.preparar_fechamento(base["s"], base["cot"].id)
    f = antes["fornecedores"][0]
    assert f["livres"] == 2 and all(i["menor"] for i in f["itens"])
    assert {o["rotulo"] for o in antes["obras"]} == {"CICLO-A · Creche A",
                                                     "CICLO-B · Escola B"}
    assert antes["obras"][0]["sede"]["tem"] is True

    pedido = _fechar(base, linhas=[base["linha_a"]], autorizar=False)
    depois = svc_ped.preparar_fechamento(base["s"], base["cot"].id)["fornecedores"][0]
    assert depois["livres"] == 1
    preso = next(i for i in depois["itens"] if not i["livre"])
    assert pedido.numero in preso["motivo"]


# ---------------------------------------------------------------------------
# O painel do coordenador: o próximo passo que falta
# ---------------------------------------------------------------------------
def test_o_ciclo_anda_passo_a_passo_ate_fechar(base):
    """A história inteira de uma compra de R$ 2.000 — e o que a tela diz em
    cada ponto dela."""
    pedido = _fechar(base, autorizar=False)
    linha, _ = _linha(base, pedido)
    assert linha["ciclo"] == "AGUARDANDO_AUTORIZACAO" and linha["de_quem"] == "diretoria"

    svc_ped.autorizar(base["s"], pedido.id, base["u"]); base["s"].flush()
    linha, _ = _linha(base, pedido)
    assert linha["ciclo"] == "AGUARDANDO_ENTREGA" and linha["total"] == 2000.0
    assert linha["pagamento"]["previsto"] == 2000.0
    assert linha["pagamento"]["sem_conta"] == 2000.0

    # chega a obra A inteira e metade da B → pendência na mão do coordenador
    _receber(base, pedido, [(base["item_a"], 10), (base["item_b"], 8)], dias_atras=4)
    linha, d = _linha(base, pedido)
    assert linha["ciclo"] == "ENTREGA_PARCIAL" and linha["de_quem"] == "fornecedor"
    assert linha["entrega"]["percentual"] == 70            # 1.400 de 2.000
    assert [(i["insumo"], i["saldo"]) for i in linha["pendentes"]] == [
        ("Peitoril de granito", 12.0)]
    assert d["destaques"]["itens_pendentes"] == 1
    # …e o material que chegou está sem nota: é o financeiro no escuro
    assert linha["nota"]["recebido_sem_nota"] == 1400.0
    assert linha["nota"]["dias_sem_nota"] == 4
    assert d["destaques"]["recebido_sem_nota"] == 1400.0
    assert any(a["tipo"] == "SEM_NOTA" and a["grave"] for a in linha["alertas"])

    # chega o resto: entrega integral, mas ainda falta a nota
    _receber(base, pedido, [(base["item_b"], 12)])
    linha, _ = _linha(base, pedido)
    assert linha["ciclo"] == "ENTREGUE_SEM_NOTA" and linha["de_quem"] == "obra"
    assert linha["entrega"]["completa"] and not linha["pendentes"]

    # a nota é vinculada mas ninguém lançou a conta: continua com a obra
    nota = _nota(base, pedido, "2000.00")
    linha, _ = _linha(base, pedido)
    assert linha["ciclo"] == "ENTREGUE_SEM_NOTA"
    assert linha["nota"]["recebido_sem_nota"] == 0.0
    assert any(a["tipo"] == "NOTA_SEM_TITULO" for a in linha["alertas"])

    # a conta é lançada: agora depende do financeiro
    titulo = _titulo(base, nota, "2000.00")
    linha, d = _linha(base, pedido)
    assert linha["ciclo"] == "ENTREGUE_A_PAGAR" and linha["de_quem"] == "financeiro"
    assert linha["pagamento"]["a_pagar"] == 2000.0 and linha["pagamento"]["sem_conta"] == 0.0
    assert linha["pagamento"]["titulos"] == ["SP-CICLO-1"]

    # paga: ciclo fechado, e some da lista de quem precisa agir
    titulo.status = StatusTitulo.PAGO
    for p in base["s"].scalars(select(Parcela).where(Parcela.titulo_id == titulo.id)):
        p.status = StatusParcela.PAGA
    base["s"].flush()
    linha, _ = _linha(base, pedido)
    assert linha["ciclo"] == "FECHADO" and linha["pagamento"]["pago"] == 2000.0
    abertos = svc_acomp.painel(base["s"], base["u"])
    assert pedido.id not in [p["id"] for p in abertos["pedidos"]]
    assert abertos["destaques"]["fechados"] == 1


def test_entrega_atrasada_diz_quantos_dias(base):
    pedido = _fechar(base, previsao=hoje() - timedelta(days=9))
    linha, d = _linha(base, pedido)
    assert linha["ciclo"] == "ENTREGA_ATRASADA"
    assert linha["entrega"]["dias_de_atraso"] == 9
    assert d["destaques"]["atrasados"] == 1


def test_nota_cancelada_nao_conta(base):
    pedido = _fechar(base)
    _receber(base, pedido, [(base["item_a"], 10), (base["item_b"], 20)])
    nota = _nota(base, pedido, "2000.00")
    nota.situacao = SituacaoNota.CANCELADA
    base["s"].flush()
    linha, _ = _linha(base, pedido)
    assert linha["nota"]["quantidade"] == 0
    assert linha["nota"]["recebido_sem_nota"] == 2000.0


def test_quem_ve_so_uma_obra_nao_ve_o_pedido_da_outra(base):
    s = base["s"]
    so_a = _fechar(base, linhas=[base["linha_a"]])
    so_b = _fechar(base, linhas=[base["linha_b"]])
    gestor = Usuario(nome="Gestor da A", email="gestor.a@teste.bws.local",
                     senha_hash=gerar_hash("senha-de-teste-1234"),
                     perfil=P.SUPERVISOR_OBRA)
    s.add(gestor); s.flush()
    s.add(UsuarioObra(usuario_id=gestor.id, obra_id=base["obra_a"].id)); s.flush()

    vistos = [p["id"] for p in svc_acomp.painel(s, gestor)["pedidos"]]
    assert so_a.id in vistos and so_b.id not in vistos
    documentos = {l["referencia"] for l in svc_proj.projetar(s, gestor)["linhas"]}
    assert so_a.numero in documentos and so_b.numero not in documentos


# ---------------------------------------------------------------------------
# A projeção: o que ainda vai virar conta a pagar
# ---------------------------------------------------------------------------
def test_mapa_aberto_entra_como_estimativa_pela_data_em_que_a_obra_pediu(base):
    d = svc_proj.projetar(base["s"], base["u"])
    assert {l["grau"] for l in d["linhas"]} == {"EM_COTACAO"}
    assert d["totais"]["EM_COTACAO"] == 2000.0
    # 30/60 dias a partir da data pedida pela obra (hoje + 20)
    vencimentos = sorted({l["vencimento"] for l in d["linhas"]})
    assert vencimentos == [(hoje() + timedelta(days=50)).isoformat(),
                           (hoje() + timedelta(days=80)).isoformat()]


def test_pedido_fechado_sai_da_estimativa_e_vira_a_autorizar_depois_firme(base):
    s = base["s"]
    pedido = _fechar(base, linhas=[base["linha_a"]], autorizar=False)
    d = svc_proj.projetar(s, base["u"])
    assert d["totais"]["A_AUTORIZAR"] == 1000.0      # 10 × 100, obra A
    assert d["totais"]["EM_COTACAO"] == 1000.0       # sobrou o da obra B
    assert d["totais"]["FIRME"] == 0.0

    svc_ped.autorizar(s, pedido.id, base["u"]); s.flush()
    d = svc_proj.projetar(s, base["u"])
    assert d["totais"]["FIRME"] == 1000.0 and d["totais"]["A_AUTORIZAR"] == 0.0
    firmes = [l for l in d["linhas"] if l["grau"] == "FIRME"]
    assert [l["valor"] for l in firmes] == [500.0, 500.0]
    assert firmes[0]["vencimento"] == (hoje() + timedelta(days=40)).isoformat()


def test_a_projecao_reparte_por_obra_e_fecha_com_o_total(base):
    _fechar(base, frete="100.00")                     # total 2.100
    d = svc_proj.projetar(base["s"], base["u"])
    por_obra = {}
    for l in d["linhas"]:
        por_obra[l["obra"]] = round(por_obra.get(l["obra"], 0) + l["valor"], 2)
    assert por_obra == {"CICLO-A": 1050.0, "CICLO-B": 1050.0}
    assert d["totais"]["FIRME"] == 2100.0


def test_o_que_ja_virou_conta_a_pagar_sai_da_projecao(base):
    """Senão a mesma dívida apareceria aqui e em Financeiro › Pagamentos."""
    pedido = _fechar(base)
    _receber(base, pedido, [(base["item_a"], 10)])
    nota = _nota(base, pedido, "1000.00")
    _titulo(base, nota, "1000.00")
    d = svc_proj.projetar(base["s"], base["u"])
    assert d["totais"]["FIRME"] == 1000.0
    # abateu a parcela mais antiga inteira; sobrou só a segunda
    assert {l["parcela"] for l in d["linhas"]} == {"2/2"}


def test_parcela_prevista_que_venceu_sem_conta_lancada_aparece_como_vencida(base):
    """O alerta do financeiro: a data combinada passou e ninguém lançou a nota."""
    pedido = _fechar(base, previsao=hoje() - timedelta(days=45))
    _receber(base, pedido, [(base["item_a"], 10), (base["item_b"], 20)], dias_atras=40)
    d = svc_proj.projetar(base["s"], base["u"])
    assert d["vencido_sem_conta"] == {"valor": 1000.0, "parcelas": 2, "pedidos": 1}
    assert d["meses"][0]["mes"] == "VENCIDO"
    vencidas = [l for l in d["linhas"] if l["vencida"]]
    assert all(l["material"] == "chegou" for l in vencidas)


def test_a_projecao_diz_o_que_ficou_de_fora(base):
    s = base["s"]
    sol = s.scalars(select(SuprimentoSolicitacao)).first()
    insumo = s.scalars(select(Insumo)).first()
    s.add(SuprimentoItem(solicitacao_id=sol.id, numero=3, insumo_id=insumo.id,
                         quantidade=Decimal("5"), unidade="M2",
                         obra_id=base["obra_a"].id))
    s.flush()
    d = svc_proj.projetar(s, base["u"])
    assert d["fora_da_conta"]["solicitados_sem_cotacao"] == 1


def test_repartir_fecha_o_centavo():
    partes = svc_proj.repartir(Decimal("100.00"), {1: Decimal("1"), 2: Decimal("1"),
                                                   3: Decimal("1")})
    assert sum(partes.values()) == Decimal("100.00")


# ---------------------------------------------------------------------------
# A porta da obra: receber o que é dela, e só o que é dela
# ---------------------------------------------------------------------------
def _da_obra(base, obra, email):
    s = base["s"]
    u = Usuario(nome=f"Administrativo {obra.codigo}", email=email,
                senha_hash=gerar_hash("senha-de-teste-1234"),
                perfil=P.ADMINISTRATIVO_OBRA)
    s.add(u); s.flush()
    s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id)); s.flush()
    return u


def test_a_obra_ve_so_os_itens_dela_e_sem_preco(base):
    pedido = _fechar(base)
    da_a = _da_obra(base, base["obra_a"], "adm.a@teste.bws.local")
    d = svc_rec.a_receber(base["s"], da_a)
    assert [p["numero"] for p in d["esperando"]] == [pedido.numero]
    itens = d["esperando"][0]["itens"]
    assert [i["insumo"] for i in itens] == ["Bancada de granito"]
    assert itens[0]["saldo"] == 10.0 and "preco_unitario" not in itens[0]
    assert "Rua Projetada 11" in itens[0]["entrega_em"]


def test_quem_ve_por_autoria_nao_enxerga_a_empresa_inteira(base):
    """A armadilha de `obras_do_usuario`: ela devolve "sem filtro" para quem
    enxerga só o que lançou — e pedido de compra não tem autor para filtrar."""
    _fechar(base)
    s = base["s"]
    solto = Usuario(nome="Sem obra", email="sem.obra@teste.bws.local",
                    senha_hash=gerar_hash("senha-de-teste-1234"),
                    perfil=P.ADMINISTRATIVO_OBRA)
    s.add(solto); s.flush()
    assert svc_rec.a_receber(s, solto)["esperando"] == []
    assert svc_acomp.painel(s, solto)["pedidos"] == []
    assert svc_proj.projetar(s, solto)["linhas"] == []


def test_a_obra_nao_recebe_material_da_outra(base):
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
    pedido = _fechar(base)
    s = base["s"]
    da_a = _da_obra(base, base["obra_a"], "adm.a2@teste.bws.local")
    linhas = {l.suprimento_item_id: l for l in s.scalars(select(PedidoItem).where(
        PedidoItem.pedido_id == pedido.id)).all()}
    with pytest.raises(ErroNaoEncontrado):
        svc_rec.registrar(s, pedido.id, {"itens": [
            {"pedido_item_id": linhas[base["item_b"].id].id, "quantidade": "5"}]}, da_a)
    # a dela, recebe — e o pedido só da obra B ela nem encontra
    svc_rec.registrar(s, pedido.id, {"itens": [
        {"pedido_item_id": linhas[base["item_a"].id].id, "quantidade": "10"}]}, da_a)
    s.flush()
    assert svc_rec.a_receber(s, da_a)["esperando"] == []      # o dela chegou todo
    da_b = _da_obra(base, base["obra_b"], "adm.b@teste.bws.local")
    assert len(svc_rec.a_receber(s, da_b)["esperando"]) == 1  # o da B continua


def test_quem_recebeu_ve_que_ainda_deve_a_nota(base):
    pedido = _fechar(base)
    da_a = _da_obra(base, base["obra_a"], "adm.a3@teste.bws.local")
    _receber(base, pedido, [(base["item_a"], 10)], dias_atras=5)
    d = svc_rec.a_receber(base["s"], da_a)
    assert [(n["numero"], n["dias"]) for n in d["sem_nota"]] == [(pedido.numero, 5)]
    _nota(base, pedido, "1000.00")
    assert svc_rec.a_receber(base["s"], da_a)["sem_nota"] == []
