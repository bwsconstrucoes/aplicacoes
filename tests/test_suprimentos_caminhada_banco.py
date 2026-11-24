"""A CAMINHADA: a compra percorrida de ponta a ponta, pelas ROTAS, por pessoas
de perfis diferentes.

⚑ POR QUE ESTE ARQUIVO EXISTE (24/11/2026).

O dono, ao avaliar se o suprimento podia ir para a produção: *"sofri muito com
o DP. Passei de uma semana pra conseguir liberar (...) o maior problema no DP
foi o FLUXO INCOMPLETO."*

Teste de regra isolada não acha fluxo incompleto: cada regra passa sozinha, e o
caminho morre no encontro de duas. Foi percorrendo o caminho inteiro, à mão,
que apareceram a conta do pedido chegando ao financeiro como "sem pedido", o
almoxarifado que nunca fechava e o fato de ninguém ser avisado de nada.

Aqui a caminhada roda sozinha a cada mudança. Cinco pessoas — o administrativo
da obra, o engenheiro (supervisor), o comprador, o diretor e o financeiro —
fazem o que fariam na vida real, cada uma com o que o perfil dela alcança. Se
uma mudança futura quebrar um passo do meio (uma permissão, um status, uma
rota), cai aqui, com o nome do passo, e não na frente da obra.

O QUE ELA NÃO COBRE: o clique nas telas (percorre as rotas que as telas
chamam), o e-mail e a IA de verdade.
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest
from conftest import hoje
from homologacao import como
from sqlalchemy import select, text

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import empresas as svc_emp
from app.apps.erp.db.models.cadastros import (
    Categoria, CondicaoPagamento, ContaBancaria, Fornecedor, FornecedorConta,
    Insumo, InsumoCategoria, Obra, PerfilUsuario as P, RegimeTributario, StatusConta,
    TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import FormaPagamento

pytestmark = pytest.mark.banco


@pytest.fixture
def mundo(app_real, sessao_real, monkeypatch):
    """Uma empresa com o mínimo que a tela de Prontidão exige, e cinco pessoas."""
    import contextlib
    from app.apps.erp import routes
    s = sessao_real

    # COMO NA PRODUÇÃO: rota que termina em erro DESFAZ o que tinha gravado.
    # O `app_real` comum entrega a mesma sessão sem desfazer nada — e numa
    # caminhada isso deixa o resto de um passo recusado vazar para o seguinte
    # (o recebimento recusado por falta de motivo ficava gravado).
    @contextlib.contextmanager
    def _sessao_que_desfaz():
        try:
            yield s
        except BaseException:
            s.rollback()
            raise
    monkeypatch.setattr(routes, "get_session", _sessao_que_desfaz)

    def pessoa(nome, perfil):
        u = Usuario(nome=nome, email=f"{nome.lower().replace(' ', '-')}@caminhada.bws.local",
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil)
        s.add(u)
        s.flush()
        return u

    comprador = pessoa("Comprador", P.ADMIN)
    # O comprador da caminhada NÃO é administrador nas recusas: o perfil é
    # trocado no passo em que a regra "quem fecha não autoriza" é provada.
    diretor = pessoa("Diretor", P.DIRETOR_FINANCEIRO)
    engenheiro = pessoa("Engenheiro", P.SUPERVISOR_OBRA)
    obra = pessoa("Adm da obra", P.ADMINISTRATIVO_OBRA)
    financeiro = pessoa("Financeiro", P.FINANCEIRO)

    emp = svc_emp.criar(s, {"razao_social": "BWS CAMINHADA LTDA", "cnpj": "71000001000184",
                            "nome_fantasia": "BWS", "logradouro": "Rua da Sede", "numero": "10",
                            "bairro": "Centro", "municipio": "Eusébio", "uf": "CE",
                            "cep": "61760-000"}, comprador)
    s.flush()
    oa = Obra(codigo="CAM-A", nome="Creche A", status="ATIVA", empresa_id=emp.id,
              endereco="Rua 1", municipio="Floresta", uf="PE")
    ob = Obra(codigo="CAM-B", nome="Escola B", status="ATIVA", empresa_id=emp.id,
              endereco="Rua 2", municipio="Barbalha", uf="CE")
    cat = InsumoCategoria(codigo="CAM", nome="Ferramentas da caminhada")
    conta = s.scalars(select(Categoria)).first()
    if conta is None:
        conta = Categoria(codigo="2.1.90", descricao="Material de obra")
        s.add(conta)
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="71000005000150",
                      razao_social="ALMEIDA CAMINHADA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO,
                      email="vendas@forn.exemplo", canais_cotacao=["EMAIL"], regioes_atuacao=[])
    s.add_all([oa, ob, cat, forn])
    s.flush()
    pix = FornecedorConta(fornecedor_id=forn.id, forma=FormaPagamento.PIX, pix_tipo="CNPJ",
                          pix_chave="71000005000150", titular_nome=forn.razao_social,
                          titular_doc="71000005000150", status=StatusConta.HOMOLOGADA,
                          homologada_por=comprador.id)
    banco = ContaBancaria(descricao="Conta movimento", banco_codigo="237", agencia="1234",
                          conta="56789-0", empresa_id=emp.id)
    insumos = [Insumo(codigo=f"CAM{n}", descricao=d, unidade="UN", categoria_insumo_id=cat.id,
                      categoria_id=conta.id)
               for n, d in enumerate(("Disco de corte da caminhada", "Trena da caminhada",
                                      "Escada da caminhada"), 1)]
    s.add_all([pix, banco, *insumos])
    for u in (engenheiro, obra):
        s.add(UsuarioObra(usuario_id=u.id, obra_id=oa.id))
    s.flush()
    cond = s.scalars(select(CondicaoPagamento)).first()
    s.commit()          # só fecha o ponto de retorno: o banco volta limpo no fim do teste

    class Mundo:
        pass
    m = Mundo()
    m.s, m.app = s, app_real
    m.comprador_u = comprador
    m.comprador, m.diretor, m.engenheiro, m.obra, m.financeiro = (
        como(app_real, u.id) for u in (comprador, diretor, engenheiro, obra, financeiro))
    m.oa, m.ob, m.forn, m.insumos = oa.id, ob.id, forn.id, [i.id for i in insumos]
    m.conta, m.pix, m.banco, m.cat = conta.id, pix.id, banco.id, cat.id
    m.cond = cond.id if cond else None
    m.venc = (hoje() + timedelta(days=30)).isoformat()
    return m


def _passo(nome, resposta, espera=200):
    corpo = resposta.get_json(silent=True) or {}
    assert resposta.status_code == espera, (
        f"PASSO QUEBRADO — {nome}: esperava HTTP {espera}, veio {resposta.status_code}. "
        f"{corpo.get('erro') or resposta.get_data(as_text=True)[:300]}")
    return corpo


def _um(m, consulta, **p):
    return m.s.execute(text(consulta), p).scalar()


def _pedir(m, quem, titulo, insumo, obra=None):
    _passo(f"{titulo}: pedir material", quem.post("/erp/api/suprimentos/solicitacoes", json={
        "titulo": titulo, "previsao_entrega": (hoje() + timedelta(days=12)).isoformat(),
        "itens": [{"insumo_id": insumo, "obra_id": obra or m.oa, "quantidade": "10",
                   "unidade": "UN"}]}))
    return _um(m, "select max(id) from suprimento_itens")


def test_compra_normal_do_pedido_da_obra_ao_pagamento(mundo):
    m = mundo
    # 1. a obra pede; pedir para obra alheia é recusado
    item = _pedir(m, m.obra, "Ferramentas da fundação", m.insumos[0])
    _passo("obra pede para obra que não é dela", m.obra.post(
        "/erp/api/suprimentos/solicitacoes", json={"titulo": "Outra obra", "itens": [
            {"insumo_id": m.insumos[0], "obra_id": m.ob, "quantidade": "1", "unidade": "UN"}]}), 400)
    # 2. sem o ok da engenharia o comprador não cota; quem pediu não libera
    cotar = {"titulo": "Ferramentas", "itens": [item]}
    _passo("comprador cota antes do ok", m.comprador.post("/erp/api/suprimentos/cotacoes", json=cotar), 400)
    _passo("obra libera o próprio pedido", m.obra.post(
        "/erp/api/suprimentos/itens/liberar", json={"itens": [item]}), 403)
    pend = _passo("engenheiro vê o que espera por ele", m.engenheiro.get("/erp/api/pendencias-comigo"))
    assert "liberar" in {p["chave"] for p in pend["pendencias"]}
    _passo("engenheiro libera", m.engenheiro.post("/erp/api/suprimentos/itens/liberar", json={"itens": [item]}))
    # 3. cotação, preço e fechamento
    _passo("comprador abre a cotação", m.comprador.post("/erp/api/suprimentos/cotacoes", json=cotar))
    cot = _um(m, "select max(id) from cotacoes")
    _passo("comprador põe o fornecedor no mapa", m.comprador.post(
        f"/erp/api/suprimentos/cotacoes/{cot}/fornecedores", json={"fornecedor_id": m.forn}))
    col = _um(m, "select id from cotacao_fornecedores where cotacao_id = :c", c=cot)
    linha = _um(m, "select id from cotacao_itens where cotacao_id = :c", c=cot)
    _passo("comprador lança o preço", m.comprador.post(f"/erp/api/suprimentos/cotacoes/{cot}/precos", json={
        "precos": [{"cotacao_fornecedor_id": col, "cotacao_item_id": linha, "preco": "19,00"}]}))
    _passo("comprador prepara o fechamento", m.comprador.get(f"/erp/api/suprimentos/cotacoes/{cot}/fechamento"))
    ped = _passo("comprador fecha o pedido", m.comprador.post(
        f"/erp/api/suprimentos/cotacoes/{cot}/fechar", json={
            "cotacao_fornecedor_id": col, "itens": [linha], "condicao_pagamento_id": m.cond,
            "previsao_entrega": (hoje() + timedelta(days=7)).isoformat()}))["id"]
    # 4. quem fechou não autoriza o próprio (o administrador é a exceção)
    m.comprador_u.perfil = P.DIRETOR_FINANCEIRO
    m.s.flush()
    _passo("quem fechou autoriza o próprio pedido", m.comprador.post(
        f"/erp/api/suprimentos/pedidos/{ped}/autorizar", json={}), 400)
    m.comprador_u.perfil = P.ADMIN
    m.s.flush()
    _passo("engenheiro autoriza pedido", m.engenheiro.post(
        f"/erp/api/suprimentos/pedidos/{ped}/autorizar", json={}), 403)
    autorizado = _passo("diretor autoriza", m.diretor.post(
        f"/erp/api/suprimentos/pedidos/{ped}/autorizar", json={}))
    assert "envio" in autorizado, "autorizar tem de tentar mandar o pedido ao fornecedor"
    proj = _passo("financeiro vê a projeção", m.financeiro.get("/erp/api/suprimentos/projecao"))
    assert proj["totais"]["FIRME"] == 190.0
    # 5. a obra recebe E lança; sem nota só com motivo
    a_receber = _passo("obra vê o que está para chegar", m.obra.get("/erp/api/suprimentos/recebimentos"))
    assert [p["pedido_id"] for p in a_receber["esperando"]] == [ped]
    pi = _um(m, "select id from pedido_itens where pedido_id = :p", p=ped)
    chegou = {"data": hoje().isoformat(), "itens": [{"pedido_item_id": pi, "quantidade": "10"}]}
    _passo("obra recebe sem lançar e sem motivo", m.obra.post(
        f"/erp/api/suprimentos/pedidos/{ped}/receber-e-lancar",
        json={**chegou, "financeiro": {"sem_lancamento": True, "motivo": ""}}), 400)
    feito = _passo("obra recebe e lança a nota", m.obra.post(
        f"/erp/api/suprimentos/pedidos/{ped}/receber-e-lancar", json={**chegou, "nota_numero": "1001",
            "financeiro": {"documento_numero": "1001", "valor": "190,00", "forma_pagamento": "PIX",
                           "data_emissao": hoje().isoformat(), "fornecedor_conta_id": m.pix,
                           "parcelas": [{"vencimento": m.venc, "valor": "190.00"}]}}))
    titulo = feito["titulo"]["id"]
    # a conta chega ao financeiro limpa: com pedido, com nota, sem crítica falsa
    criticas = {c["codigo"] for c in (_um(
        m, "select criticas from analises where titulo_id = :t", t=titulo) or [])}
    assert not ({"B1", "A10"} & criticas), criticas
    assert _um(m, "select count(*) from documentos_fiscais where pedido_compra_id = :p", p=ped) == 1
    assert feito["titulo"]["status"] == "AGUARDANDO_AVAL"
    # 6. aval → aprovação → pagamento
    antes = _passo("financeiro aprova antes do aval", m.financeiro.post(
        "/erp/api/titulos/acao", json={"acao": "aprovar", "ids": [titulo]}))
    assert antes["erros"] and not antes["processados"]
    _passo("engenheiro dá o aval", m.engenheiro.post(f"/erp/api/avais/{titulo}", json={"decisao": "CONFIRMADO"}))
    aprovado = _passo("financeiro aprova", m.financeiro.post(
        "/erp/api/titulos/acao", json={"acao": "aprovar", "ids": [titulo]}))
    assert aprovado["processados"], aprovado
    parcela = _um(m, "select id from parcelas where titulo_id = :t", t=titulo)
    pago = _passo("financeiro paga", m.financeiro.post("/erp/api/pagamentos/baixar", json={
        "conta_bancaria_id": m.banco, "data_pagamento": hoje().isoformat(),
        "itens": [{"parcela_id": parcela, "valor_pago": "190.00"}]}))
    assert pago["pagas"] and not pago["erros"], pago
    # 7. o ciclo fecha para o comprador
    visto = _passo("comprador vê o ciclo", m.comprador.get("/erp/api/suprimentos/acompanhamento?fechados=1"))
    ciclo = next(p for p in visto["pedidos"] if p["id"] == ped)
    assert ciclo["ciclo"] == "FECHADO", ciclo["ciclo"]


def test_compra_direta_recusa_cancelamento_e_almoxarifado(mundo):
    m = mundo
    item = _pedir(m, m.engenheiro, "Escada urgente", m.insumos[2])
    assert _um(m, "select liberacao from suprimento_itens where id = :i", i=item) == "LIBERADO"
    direto = {"fornecedor_id": m.forn, "condicao_pagamento_id": m.cond,
              "previsao_entrega": (hoje() + timedelta(days=3)).isoformat(),
              "itens": [{"suprimento_item_id": item, "preco_unitario": "110,00"}]}
    _passo("compra direta sem justificativa", m.comprador.post("/erp/api/suprimentos/pedidos", json=direto), 400)
    direto["justificativa_sem_cotacao"] = "único fornecedor com a escada em estoque na região"
    ped = _passo("compra direta", m.comprador.post("/erp/api/suprimentos/pedidos", json=direto))["id"]
    _passo("diretor recusa", m.diretor.post(f"/erp/api/suprimentos/pedidos/{ped}/recusar",
                                           json={"motivo": "cotar com mais um fornecedor"}))
    assert _um(m, "select status from suprimento_itens where id = :i", i=item) == "SOLICITACAO"
    ped = _passo("comprador fecha de novo", m.comprador.post("/erp/api/suprimentos/pedidos", json=direto))["id"]
    _passo("diretor autoriza", m.diretor.post(f"/erp/api/suprimentos/pedidos/{ped}/autorizar", json={}))
    _passo("diretor cancela", m.diretor.post(f"/erp/api/suprimentos/pedidos/{ped}/cancelar",
                                            json={"motivo": "fornecedor ficou sem estoque"}))
    assert _um(m, "select status from suprimento_itens where id = :i", i=item) == "SOLICITACAO"
    # tem no estoque: almoxarifado → entregue → a obra confirma, sem nota
    mover = f"/erp/api/suprimentos/itens/{item}/situacao"
    _passo("obra move o item à mão", m.obra.post(mover, json={"status": "ALMOXARIFADO"}), 403)
    _passo("comprador marca RECEBIDO à mão", m.comprador.post(mover, json={"status": "RECEBIDO"}), 400)
    _passo("comprador manda para o almoxarifado", m.comprador.post(mover, json={"status": "ALMOXARIFADO"}))
    _passo("comprador marca entregue", m.comprador.post(mover, json={"status": "ENTREGUE"}))
    lista = _passo("obra vê o material do almoxarifado", m.obra.get("/erp/api/suprimentos/recebimentos"))
    assert [i["item_id"] for i in lista["do_almoxarifado"]] == [item]
    _passo("obra confirma que chegou", m.obra.post(
        f"/erp/api/suprimentos/itens/{item}/chegou-do-almoxarifado", json={}))
    assert _um(m, "select status from suprimento_itens where id = :i", i=item) == "RECEBIDO"


def test_material_fora_do_cadastro_entra_no_pedido_sozinho(mundo):
    m = mundo
    _passo("obra pede só material novo", m.obra.post("/erp/api/suprimentos/solicitacoes", json={
        "titulo": "Impermeabilização", "itens": [], "itens_novos": [{
            "descricao": "Manta asfáltica da caminhada", "obra_id": m.oa, "quantidade": "12",
            "unidade": "UN", "explicacao": "manta que impermeabiliza a laje, colada a quente"}]}))
    pend = _passo("comprador vê o pedido de cadastro", m.comprador.get(
        "/erp/api/suprimentos/insumos/solicitacoes?pendentes=1"))["solicitacoes"]
    cadastro = next(p for p in pend if p["descricao"].startswith("Manta asfáltica da caminhada"))
    assert cadastro["pedidos_esperando"]
    decidir = f"/erp/api/suprimentos/insumos/solicitacoes/{cadastro['id']}"
    _passo("financeiro cadastra o insumo", m.financeiro.post(decidir, json={"acao": "cadastrar"}), 403)
    _passo("comprador cadastra o insumo", m.comprador.post(decidir, json={
        "acao": "cadastrar", "categoria_insumo_id": m.cat, "categoria_id": m.conta}))
    nasceu = m.s.execute(text(
        "select liberacao, status from suprimento_itens order by id desc limit 1")).first()
    assert tuple(nasceu) == ("PENDENTE", "SOLICITACAO")      # o ok da engenharia continua valendo


def test_compra_por_fora_trava_ate_a_ciencia_e_vira_pedido_tardio(mundo):
    m = mundo
    estado = _passo("a trava nasce ligada", m.comprador.get("/erp/api/suprimentos/fora-do-fluxo"))
    assert estado["modo"] == "TRAVAR"
    fora = {"fornecedor_id": m.forn, "categoria_id": m.conta, "descricao": "cimento de urgência",
            "valor_bruto": "480.00", "documento_numero": "2001", "forma_pagamento": "PIX",
            "tipo": "T1_MATERIAL_NFE", "fornecedor_conta_id": m.pix,
            "parcelas": [{"vencimento": m.venc, "valor": "480.00"}],
            "rateios": [{"obra_id": m.oa, "valor": "480.00"}], "retencoes": [], "interessados": []}
    sem_motivo = _passo("obra lança material sem pedido e sem motivo",
                        m.obra.post("/erp/api/lancamento", json=fora), 409)
    assert sem_motivo.get("precisa_motivo") is True
    titulo = _passo("obra lança com o motivo", m.obra.post("/erp/api/lancamento", json={
        **fora, "motivo_sem_pedido": "faltou cimento na concretagem de sábado, comprei ao lado"}))["titulo"]
    assert titulo["status"] == "BLOQUEADO" and titulo["fora_do_fluxo_trava"] is True
    tid = titulo["id"]
    # o financeiro não aprova nem paga enquanto ninguém deu a ciência
    aprovar = _passo("financeiro aprova a conta travada", m.financeiro.post(
        "/erp/api/titulos/acao", json={"acao": "aprovar", "ids": [tid]}))
    assert not aprovar["processados"]
    parcela = _um(m, "select id from parcelas where titulo_id = :t", t=tid)
    pagar = _passo("financeiro paga a conta travada", m.financeiro.post("/erp/api/pagamentos/baixar", json={
        "conta_bancaria_id": m.banco, "data_pagamento": hoje().isoformat(),
        "itens": [{"parcela_id": parcela, "valor_pago": "480.00"}]}))
    assert not pagar["pagas"]
    # quem lançou não dá a ciência; informa o que comprou; a engenharia dá a ciência
    _passo("obra dá a ciência do próprio lançamento", m.obra.post(
        f"/erp/api/suprimentos/fora-do-fluxo/{tid}/autorizar", json={}), 403)
    informado = _passo("obra informa o que comprou", m.obra.post(
        f"/erp/api/suprimentos/fora-do-fluxo/{tid}/regularizar", json={"itens": [
            {"insumo_id": m.insumos[0], "quantidade": "48", "unidade": "UN", "preco_unitario": "10,00"}]}))
    assert informado["aguardando_ciencia"] is True
    assert _um(m, "select count(*) from pedido_titulos where titulo_id = :t", t=tid) == 0
    _passo("engenheiro dá a ciência", m.engenheiro.post(
        f"/erp/api/suprimentos/fora-do-fluxo/{tid}/autorizar", json={"observacao": "ciente"}))
    assert _um(m, "select status from titulos where id = :t", t=tid) == "AGUARDANDO_APROVACAO"
    tardio = m.s.execute(text(
        "select p.status, u.nome from pedido_titulos pt join pedidos_compra p on p.id = pt.pedido_id "
        "join usuarios u on u.id = p.autorizado_por where pt.titulo_id = :t"), {"t": tid}).first()
    assert tuple(tardio) == ("AUTORIZADO", "Engenheiro")
