"""A HOMOLOGAÇÃO DA CONTA DO CREDOR — a porta que faltava.

Achado em 22/09/2026 ao percorrer a cadeia inteira do ERP num banco recém
criado, do cadastro do CNPJ até a conciliação: `homologar_conta` existia na
regra de negócio desde sempre e NÃO TINHA QUEM A CHAMASSE. Nenhuma rota,
nenhum botão, nenhum teste.

O efeito era o pior possível — e invisível na leitura do código, porque cada
peça sozinha estava certa: todo título pago por Pix ou TED nasce BLOQUEADO
enquanto a conta do credor não é homologada; num banco novo, nenhuma conta
podia ser homologada; logo, nenhum pagamento por Pix ou TED chegaria ao fim.

Dois controles que estes testes defendem:

1. QUEM CADASTROU A CONTA NÃO A HOMOLOGA. É a trava contra o golpe da troca
   de conta bancária. Até aqui a regra estava escrita na docstring e não no
   código — a promessa existia, a trava não.
2. HOMOLOGAR NÃO APROVA. A conta liberada devolve o título para a fila de
   aprovação; quem aprova continua sendo outra pessoa, com alçada.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.apps.erp.core.auth.permissoes import pode
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.cadastros import fornecedores as svc_forn
from app.apps.erp.core.comum.auditoria import ErroPermissao, registrar_evento
from app.apps.erp.core.titulos import service as svc_tit
from app.apps.erp.db.models.cadastros import (
    Categoria, FormaPagamento, Fornecedor, FornecedorConta, Obra,
    PerfilUsuario as P, StatusConta, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import StatusTitulo

from conftest import como

pytestmark = pytest.mark.banco


def _pessoa(s, apelido, perfil=P.ADMIN):
    u = Usuario(nome=apelido, email=f"{apelido}@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=perfil,
                telefone="5585900000000")
    s.add(u)
    s.flush()
    return u


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    obra = Obra(codigo="HOMOLOG1", nome="Obra do teste de homologação", status="ATIVA")
    cat = Categoria(codigo="3.1.98", descricao="Material do teste", ativo=True,
                    grupo_codigo="3", grupo_nome="Custos")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="MADEIREIRA DO TESTE LTDA", ativo=True)
    s.add_all([obra, cat, forn])
    s.flush()
    lancador = _pessoa(s, "quem-lanca")
    conferente = _pessoa(s, "quem-confere")
    conta = FornecedorConta(fornecedor_id=forn.id, forma=FormaPagamento.PIX,
                            pix_tipo="CNPJ", pix_chave="11444777000161",
                            titular_nome=forn.razao_social, titular_doc=forn.cnpj_cpf,
                            status=StatusConta.PENDENTE)
    s.add(conta)
    s.flush()
    # a conta foi cadastrada por quem lança — é o caso real que a trava cobre
    registrar_evento(s, "fornecedor_conta", conta.id, "CRIADA",
                     {"forma": "PIX"}, lancador.id)
    s.flush()
    return {"s": s, "obra": obra, "cat": cat, "forn": forn, "conta": conta,
            "lancador": lancador, "conferente": conferente}


def _titulo(c):
    return svc_tit.criar_titulo(c["s"], {
        "tipo": "T14_EXCECAO_SEM_NOTA",
        "fornecedor_id": c["forn"].id, "categoria_id": c["cat"].id,
        "descricao": "compra de material para a obra",
        "valor_bruto": "500.00", "forma_pagamento": "PIX",
        "fornecedor_conta_id": c["conta"].id,
        "justificativa_excecao": "sem nota fiscal, teste",
        "parcelas": [{"vencimento": (date.today() + timedelta(days=30)).isoformat(),
                      "valor": "500.00"}],
        "rateios": [{"obra_id": c["obra"].id, "valor": "500.00"}],
    }, c["lancador"])


# ---------------------------------------------------------------------------
# A porta existe
# ---------------------------------------------------------------------------
def test_a_rota_de_homologar_existe_e_libera_a_conta(cenario, app_real):
    """O teste que pega a falta da porta: sem rota, isto dá 404."""
    c = cenario
    r = como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})

    assert r.status_code == 200, r.get_data(as_text=True)[:200]
    assert r.get_json()["status"] == "HOMOLOGADA"
    assert c["s"].get(FornecedorConta, c["conta"].id).status == StatusConta.HOMOLOGADA


def test_a_fila_de_contas_pendentes_mostra_o_que_espera_conferencia(cenario, app_real):
    """Sem esta lista a pendência é invisível: só apareceria para quem
    esbarrasse num título travado, e o dinheiro pararia sem ninguém saber."""
    c = cenario
    r = como(app_real, c["conferente"].id).get("/erp/api/credores/contas/pendentes")

    contas = r.get_json()["contas"]
    minha = [x for x in contas if x["id"] == c["conta"].id]
    assert len(minha) == 1, "a conta pendente tem de aparecer na fila"
    assert minha[0]["credor"] == "MADEIREIRA DO TESTE LTDA"
    assert minha[0]["cadastrada_por"] == "quem-lanca", \
        "quem conferir precisa saber quem cadastrou — é o que ele está checando"


def test_a_conta_homologada_sai_da_fila(cenario, app_real):
    c = cenario
    como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})

    contas = como(app_real, c["conferente"].id).get(
        "/erp/api/credores/contas/pendentes").get_json()["contas"]
    assert [x for x in contas if x["id"] == c["conta"].id] == []


# ---------------------------------------------------------------------------
# A trava contra o golpe da troca de conta
# ---------------------------------------------------------------------------
def test_quem_cadastrou_a_conta_NAO_a_homologa(cenario):
    """O controle inteiro. Se um dia isto passar, quem lança escolhe sozinho
    para onde o dinheiro vai — que é exatamente o golpe da troca de conta."""
    c = cenario
    with pytest.raises(ErroPermissao) as e:
        svc_forn.homologar_conta(c["s"], c["conta"].id, c["lancador"])

    assert "não pode homologá-la" in str(e.value)
    assert c["s"].get(FornecedorConta, c["conta"].id).status == StatusConta.PENDENTE


def test_pela_rota_tambem_recusa_quem_cadastrou(cenario, app_real):
    """A trava tem de valer na porta, não só na função — quem chama a rota
    direto não passa pela tela."""
    c = cenario
    r = como(app_real, c["lancador"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})

    assert r.status_code == 403
    assert c["s"].get(FornecedorConta, c["conta"].id).status == StatusConta.PENDENTE


def test_a_fila_avisa_a_quem_nao_pode_homologar_antes_do_clique(cenario, app_real):
    c = cenario
    contas = como(app_real, c["lancador"].id).get(
        "/erp/api/credores/contas/pendentes").get_json()["contas"]
    minha = [x for x in contas if x["id"] == c["conta"].id][0]
    assert minha["posso_homologar"] is False

    contas = como(app_real, c["conferente"].id).get(
        "/erp/api/credores/contas/pendentes").get_json()["contas"]
    minha = [x for x in contas if x["id"] == c["conta"].id][0]
    assert minha["posso_homologar"] is True


def test_quem_nao_tem_a_acao_nao_homologa(cenario, app_real):
    """LANÇADOR não confere destino de pagamento — e a recusa vem do guard,
    antes de a regra rodar."""
    c = cenario
    lancador_puro = _pessoa(c["s"], "so-lanca", P.LANCADOR)
    assert pode(lancador_puro, "homologar_conta_credor") is False

    r = como(app_real, lancador_puro.id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})
    assert r.status_code in (403, 404)
    assert c["s"].get(FornecedorConta, c["conta"].id).status == StatusConta.PENDENTE


def test_a_acao_de_homologar_nao_vem_junto_de_pagar(cenario):
    """Se viesse, o dono não teria como dar uma sem a outra — e a separação
    entre conferir o destino e soltar o dinheiro deixaria de existir."""
    from app.apps.erp.core.auth import secoes

    secao_pagar = [x for x in secoes.SECOES if x["chave"] == "fin_pagar"][0]
    assert "homologar_conta_credor" not in secao_pagar["editar"]
    assert any(x["chave"] == "fin_homologar_conta" for x in secoes.SECOES)


# ---------------------------------------------------------------------------
# Homologar destrava o título — mas não o aprova
# ---------------------------------------------------------------------------
def test_homologar_com_o_titulo_junto_devolve_ele_para_a_fila(cenario, app_real):
    """A ida única que fecha o beco: homologa e reanalisa no mesmo clique. Sem
    isso a pessoa homologa, acha que resolveu, e o título continua parado."""
    c = cenario
    t = _titulo(c)
    assert t.status == StatusTitulo.BLOQUEADO

    r = como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar",
        json={"reanalisar_titulo_id": t.id})

    corpo = r.get_json()
    assert corpo["titulo"]["status"] == "AGUARDANDO_APROVACAO"
    assert c["s"].get(type(t), t.id).status == StatusTitulo.AGUARDANDO_APROVACAO


def test_homologar_NAO_aprova_o_titulo(cenario, app_real):
    """Conferir a conta não é liberar o pagamento: a aprovação continua sendo
    de outra pessoa. Se homologar aprovasse, teria virado atalho."""
    c = cenario
    t = _titulo(c)
    como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar",
        json={"reanalisar_titulo_id": t.id})

    assert c["s"].get(type(t), t.id).status != StatusTitulo.APROVADO


def test_reanalise_que_falha_nao_desfaz_a_homologacao(cenario, app_real):
    """A homologação é o ato principal, e foi conferida por gente. Desfazê-la
    porque o título não quis reanalisar jogaria fora o trabalho certo."""
    c = cenario
    t = _titulo(c)
    svc_tit.cancelar(c["s"], t.id, "teste", c["lancador"])
    c["s"].flush()

    r = como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar",
        json={"reanalisar_titulo_id": t.id})

    corpo = r.get_json()
    assert corpo["ok"] is True and corpo["status"] == "HOMOLOGADA"
    assert "aviso_reanalise" in corpo, "o recado do que não deu tem de aparecer"


# ---------------------------------------------------------------------------
# O que continua recusado
# ---------------------------------------------------------------------------
def test_conta_bloqueada_nao_volta_por_homologacao(cenario, app_real):
    c = cenario
    c["conta"].status = StatusConta.BLOQUEADA
    c["s"].flush()

    r = como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})
    assert r.status_code == 400
    assert "bloqueada" in r.get_json()["erro"].lower()


def test_conta_sem_titular_nao_homologa(cenario, app_real):
    """O titular é O QUE SE CONFERE. Sem ele não há o que comparar com o
    cadastro do credor, e a homologação seria um carimbo vazio."""
    c = cenario
    c["conta"].titular_nome = None
    c["s"].flush()

    r = como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})
    assert r.status_code == 400
    assert "titular" in r.get_json()["erro"].lower()


def test_homologar_duas_vezes_nao_quebra(cenario, app_real):
    c = cenario
    como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})
    r = como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})
    assert r.status_code == 200 and r.get_json()["status"] == "HOMOLOGADA"


def test_a_homologacao_fica_na_trilha_com_quem_liberou(cenario, app_real):
    """Auditoria: sem isso não há como responder 'quem liberou esta conta?'."""
    from sqlalchemy import select
    from app.apps.erp.db.models.financeiro import Evento

    c = cenario
    como(app_real, c["conferente"].id).post(
        f"/erp/api/credores/contas/{c['conta'].id}/homologar", json={})

    ev = c["s"].scalars(select(Evento).where(
        Evento.entidade_tipo == "fornecedor_conta",
        Evento.entidade_id == c["conta"].id,
        Evento.acao == "HOMOLOGADA")).first()
    assert ev is not None and ev.usuario_id == c["conferente"].id
    conta = c["s"].get(FornecedorConta, c["conta"].id)
    assert conta.homologada_por == c["conferente"].id and conta.homologada_em is not None
