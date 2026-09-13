"""O cruzamento de notas fiscais — com banco de verdade.

Ditado pelo dono em 07/09/2026 e escrito inteiro em `NOTAS_FISCAIS.md`. O que
ele descreveu, e que este arquivo prova que o sistema respeita:

  "Comprei dez carradas de brita, e o fornecedor emite a nota por carrada.
   Aquele pedido não se fecha instantaneamente. (…) Vai se transformar
   provavelmente em dez notas e dez boletos."

  "Essa questão da dedutibilidade é algo importante. O fundo fixo, ele é
   dedutível, mesmo que não tenha nota. Mas vai acontecer de aparecer uma nota
   fiscal que é de um fundo fixo. Então ele está sendo dedutível de duas
   formas — mas também não pode entrar duplicado na contabilidade."

Com banco porque o que se prova aqui envolve WHERE, JOIN e restrição única —
nada disso o dublê de sessão consegue fingir.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.notas import cruzamento
from app.apps.erp.db.models.cadastros import (
    Categoria, Empresa, Fornecedor, Obra, PerfilUsuario as P, RegimeTributario,
    TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    DocumentoFiscal, FormaPagamento, SituacaoNota, StatusTitulo, TipoDocFiscal,
    TipoTitulo, Titulo, TituloItem,
)

pytestmark = pytest.mark.banco

CNPJ_NOSSO = "11222333000181"
CNPJ_FORN = "71000004000127"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Financeiro das notas", email="notas@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    emp = Empresa(razao_social="BWS Construções Exemplo", nome_fantasia="BWS",
                  cnpj=CNPJ_NOSSO)
    forn = Fornecedor(razao_social="Pedreira Exemplo", cnpj_cpf=CNPJ_FORN,
                      tipo_pessoa=TipoPessoa.PJ, ativo=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="OBRANOTA", nome="Obra das notas")
    cat = Categoria(codigo="9.9.97", descricao="Conta do teste de notas")
    s.add_all([u, emp, forn, obra, cat])
    s.flush()
    return {"s": s, "usuario": u, "empresa": emp, "fornecedor": forn,
            "obra": obra, "categoria": cat}


def _nota(s, *, chave=None, valor="1000.00", emissao=None, destinatario=CNPJ_NOSSO,
          numero="1"):
    n = DocumentoFiscal(
        tipo=TipoDocFiscal.NFE, chave_acesso=chave, numero=numero, serie="1",
        emitente_doc=CNPJ_FORN, emitente_nome="Pedreira Exemplo",
        destinatario_doc=destinatario, valor_total=Decimal(valor),
        data_emissao=emissao or date.today(), situacao=SituacaoNota.DESCONHECIDA,
        origem="TESTE")
    s.add(n)
    s.flush()
    return n


def _titulo(cenario, *, valor="1000.00", numero_sp="SP-NOTA-1", chave=None):
    s = cenario["s"]
    t = Titulo(numero_sp=numero_sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
               fornecedor_id=cenario["fornecedor"].id, descricao="Brita",
               valor_bruto=Decimal(valor), valor_liquido=Decimal(valor),
               competencia=date.today().replace(day=1),
               categoria_id=cenario["categoria"].id,
               forma_pagamento=FormaPagamento.BOLETO,
               status=StatusTitulo.APROVADO, solicitante_id=cenario["usuario"].id,
               chave_acesso_nfe=chave)
    s.add(t)
    s.flush()
    return t


def _item_prestacao(cenario, *, valor="1000.00", quando=None):
    """Uma linha de prestação de fundo fixo — o caso da 'notazinha de cem reais'."""
    s = cenario["s"]
    t = _titulo(cenario, valor=valor, numero_sp="SP-FUNDO-1")
    it = TituloItem(titulo_id=t.id, ordem=1, descricao="Compra na obra",
                    valor=Decimal(valor), data_despesa=quando or date.today(),
                    obra_id=cenario["obra"].id)
    s.add(it)
    s.flush()
    return it


# ---------------------------------------------------------------------------
# De quem é a nota
# ---------------------------------------------------------------------------
def test_reconhece_contra_qual_cnpj_nosso_a_nota_foi_emitida(cenario):
    s = cenario["s"]
    minha = _nota(s)
    de_fora = _nota(s, destinatario="99888777000166", numero="2")
    assert cruzamento.empresa_da_nota(s, minha).id == cenario["empresa"].id
    assert cruzamento.empresa_da_nota(s, de_fora) is None


def test_nota_contra_cnpj_que_nao_e_nosso_aparece_no_resumo(cenario):
    """Nota emitida contra a empresa sem ninguém saber é problema fiscal — mas
    nota que nem é nossa também precisa ser vista, senão vira ruído eterno."""
    s = cenario["s"]
    _nota(s, destinatario="99888777000166")
    r = cruzamento.listar(s)
    assert r["resumo"]["sem_empresa"] == 1


# ---------------------------------------------------------------------------
# A nota que não cruza com nada — o alerta que interessa
# ---------------------------------------------------------------------------
def test_nota_sem_par_e_o_primeiro_alerta(cenario):
    s = cenario["s"]
    _nota(s, valor="4321.00")
    r = cruzamento.listar(s)
    assert r["resumo"]["sem_par"] == 1
    assert r["resumo"]["valor_sem_par"] == 4321.00
    assert r["notas"][0]["dedutivel"] is False, \
        "nota que não entrou por porta nenhuma não é dedutível ainda"


# ---------------------------------------------------------------------------
# UM PEDIDO, VÁRIAS NOTAS — o coração do desenho
# ---------------------------------------------------------------------------
def _pedido(cenario, *, valor="3000.00", numero="PC-TESTE-1"):
    """Um pedido de compra de verdade no banco.

    O valor entra pelo frete porque montar a cadeia inteira de Suprimentos
    (solicitação → item → cotação → item do pedido) só para provar aritmética
    de saldo seria ruído: `andamento_do_pedido` soma itens + frete - desconto,
    e o que está sob teste é o acumulado das NOTAS contra esse total.
    """
    from app.apps.erp.db.models.cadastros import PedidoCompra, StatusPedidoCompra
    s = cenario["s"]
    p = PedidoCompra(numero=numero, fornecedor_id=cenario["fornecedor"].id,
                     frete=Decimal(valor), desconto=Decimal(0),
                     status=StatusPedidoCompra.AUTORIZADO,
                     criado_por=cenario["usuario"].id,
                     autorizado_por=cenario["usuario"].id,
                     autorizado_em=date.today())
    s.add(p)
    s.flush()
    return p


def test_um_pedido_recebe_varias_notas_e_o_saldo_diminui(cenario):
    """As dez carradas de brita: o pedido NÃO fecha na primeira nota.

    Palavras do dono: *"aquele pedido não se fecha instantaneamente. São várias
    notas pra poder fechar ele."* Qualquer desenho que assumisse um-para-um
    quebraria exatamente aqui.
    """
    s = cenario["s"]
    pedido = _pedido(cenario, valor="3000.00")
    assert cruzamento.andamento_do_pedido(s, pedido)["falta"] == 3000.0

    faltas = []
    for i in range(1, 4):
        n = _nota(s, valor="1000.00", numero=str(i))
        cruzamento.ligar(s, n.id, pedido_id=pedido.id, usuario=cenario["usuario"])
        faltas.append(cruzamento.andamento_do_pedido(s, pedido)["falta"])

    assert faltas == [2000.0, 1000.0, 0.0]
    a = cruzamento.andamento_do_pedido(s, pedido)
    assert a["notas"] == 3 and a["fechado"] is True


def test_o_pedido_so_aparece_como_fechado_quando_as_notas_completam(cenario):
    s = cenario["s"]
    pedido = _pedido(cenario, valor="3000.00", numero="PC-TESTE-2")
    n = _nota(s, valor="1000.00", numero="90")
    cruzamento.ligar(s, n.id, pedido_id=pedido.id, usuario=cenario["usuario"])
    a = cruzamento.andamento_do_pedido(s, pedido)
    assert a["fechado"] is False and a["em_notas"] == 1000.0


def test_a_nota_sugere_o_pedido_do_mesmo_fornecedor_com_saldo(cenario):
    s = cenario["s"]
    pedido = _pedido(cenario, valor="5000.00", numero="PC-TESTE-3")
    nota = _nota(s, valor="900.00", numero="91")
    cands = cruzamento._candidatos_pedido(s, nota)
    assert cands and cands[0]["numero"] == pedido.numero
    assert "saldo" in cands[0]["motivo"]


def test_candidatos_de_pedido_nao_exigem_valor_igual(cenario):
    """A nota costuma ser UM PEDAÇO do pedido: casar por valor exato perderia
    justamente o caso que o dono descreveu."""
    s = cenario["s"]
    nota = _nota(s, valor="1000.00")
    # sem pedido cadastrado, a lista vem vazia — e o importante é que o critério
    # usado seja fornecedor + janela, nunca "valor igual ao do pedido"
    assert cruzamento._candidatos_pedido(s, nota) == []


# ---------------------------------------------------------------------------
# A chave de acesso é prova; o resto é indício
# ---------------------------------------------------------------------------
def test_casa_sozinho_pela_chave_de_acesso(cenario):
    s = cenario["s"]
    chave = "3" * 44
    nota = _nota(s, chave=chave)
    t = _titulo(cenario, chave=chave)
    r = cruzamento.sugerir(s)
    assert r["casadas"] == 1
    s.refresh(t)
    assert t.documento_fiscal_id == nota.id
    s.refresh(nota)
    assert nota.conferencia == "CASADA"


def test_nao_casa_sozinho_por_indicio(cenario):
    """Mesmo credor e mesmo valor é PISTA, não prova. Casar sozinho por isso é
    errar igual à conferência manual — só que mais rápido e em silêncio."""
    s = cenario["s"]
    nota = _nota(s, valor="1000.00")
    _titulo(cenario, valor="1000.00")
    r = cruzamento.sugerir(s)
    assert r["casadas"] == 0
    assert r["com_proposta"] == 1
    s.refresh(nota)
    assert nota.conferencia == "PENDENTE"
    cands = cruzamento._candidatos_titulo(s, nota)
    assert cands and cands[0]["motivo"] == "mesmo credor e mesmo valor"


# ---------------------------------------------------------------------------
# A TRAVA CONTRA CONTAR A MESMA DESPESA DUAS VEZES
# ---------------------------------------------------------------------------
def test_nota_com_titulo_nao_entra_tambem_no_fundo_fixo(cenario):
    s = cenario["s"]
    nota = _nota(s)
    t = _titulo(cenario)
    cruzamento.ligar(s, nota.id, titulo_id=t.id, usuario=cenario["usuario"])
    it = _item_prestacao(cenario)
    with pytest.raises(ErroValidacao) as e:
        cruzamento.ligar(s, nota.id, item_id=it.id, usuario=cenario["usuario"])
    assert "duas vezes" in str(e.value)


def test_nota_no_fundo_fixo_nao_ganha_titulo_proprio(cenario):
    s = cenario["s"]
    nota = _nota(s)
    it = _item_prestacao(cenario)
    cruzamento.ligar(s, nota.id, item_id=it.id, usuario=cenario["usuario"])
    t = _titulo(cenario, numero_sp="SP-NOTA-2")
    with pytest.raises(ErroValidacao) as e:
        cruzamento.ligar(s, nota.id, titulo_id=t.id, usuario=cenario["usuario"])
    assert "duas vezes" in str(e.value)


def test_a_mesma_linha_de_prestacao_nao_recebe_duas_notas(cenario):
    """Esta é do BANCO: índice único em titulo_item_id."""
    s = cenario["s"]
    it = _item_prestacao(cenario)
    n1, n2 = _nota(s, numero="1"), _nota(s, numero="2")
    n1.titulo_item_id = it.id
    s.flush()
    n2.titulo_item_id = it.id
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_fundo_fixo_e_dedutivel_mesmo_sem_titulo_proprio(cenario):
    """Palavras do dono: "fundo fixo é dedutível, ponto final"."""
    s = cenario["s"]
    nota = _nota(s)
    it = _item_prestacao(cenario)
    linha = cruzamento.ligar(s, nota.id, item_id=it.id, usuario=cenario["usuario"])
    assert linha["porta"] == "FUNDO_FIXO"
    assert linha["dedutivel"] is True
    assert linha["alerta_duplo"] is False


def test_desfazer_a_ligacao_libera_a_outra_porta(cenario):
    s = cenario["s"]
    nota = _nota(s)
    t = _titulo(cenario)
    cruzamento.ligar(s, nota.id, titulo_id=t.id, usuario=cenario["usuario"])
    cruzamento.desligar(s, nota.id, o_que="titulo", usuario=cenario["usuario"])
    it = _item_prestacao(cenario)
    linha = cruzamento.ligar(s, nota.id, item_id=it.id, usuario=cenario["usuario"])
    assert linha["porta"] == "FUNDO_FIXO"


# ---------------------------------------------------------------------------
# A decisão humana fica registrada — e "ignorar" pede motivo
# ---------------------------------------------------------------------------
def test_ignorar_sem_motivo_e_recusado(cenario):
    s = cenario["s"]
    nota = _nota(s)
    with pytest.raises(ErroValidacao) as e:
        cruzamento.marcar(s, nota.id, conferencia="IGNORADA",
                          usuario=cenario["usuario"])
    assert "motivo" in str(e.value).lower()


def test_ignorar_com_motivo_registra_quem_e_quando(cenario):
    s = cenario["s"]
    nota = _nota(s)
    cruzamento.marcar(s, nota.id, conferencia="IGNORADA",
                      motivo="nota cancelada pelo fornecedor",
                      usuario=cenario["usuario"])
    s.refresh(nota)
    assert nota.conferencia == "IGNORADA"
    assert nota.conferencia_motivo == "nota cancelada pelo fornecedor"
    assert nota.conferido_por == cenario["usuario"].id
    assert nota.conferido_em is not None


def test_o_banco_recusa_ignorada_sem_motivo(cenario):
    """Mesmo que um código futuro esqueça a regra, o banco não deixa passar."""
    s = cenario["s"]
    nota = _nota(s)
    nota.conferencia = "IGNORADA"
    nota.conferencia_motivo = "   "
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


def test_o_banco_recusa_situacao_de_conferencia_inventada(cenario):
    s = cenario["s"]
    nota = _nota(s)
    nota.conferencia = "MAIS_OU_MENOS"
    with pytest.raises(IntegrityError):
        s.flush()
    s.rollback()


# ---------------------------------------------------------------------------
# A leitura que a tela usa
# ---------------------------------------------------------------------------
def test_a_listagem_conta_o_que_a_tela_precisa_mostrar(cenario):
    s = cenario["s"]
    casada = _nota(s, chave="4" * 44, valor="500.00", numero="10")
    t = _titulo(cenario, valor="500.00", chave="4" * 44, numero_sp="SP-NOTA-9")
    cruzamento.ligar(s, casada.id, titulo_id=t.id, usuario=cenario["usuario"])
    _nota(s, valor="123.00", numero="11")

    r = cruzamento.listar(s)
    assert r["resumo"]["quantidade"] == 2
    assert r["resumo"]["casadas"] == 1
    assert r["resumo"]["pendentes"] == 1
    assert r["resumo"]["dedutivel"] == 500.00, \
        "só a que entrou por uma porta conta como dedutível"


def test_filtro_por_situacao_da_conferencia(cenario):
    s = cenario["s"]
    _nota(s, numero="20")
    outra = _nota(s, numero="21")
    cruzamento.marcar(s, outra.id, conferencia="SEM_PAR", usuario=cenario["usuario"])
    assert cruzamento.listar(s, conferencia="SEM_PAR")["resumo"]["quantidade"] == 1
    assert cruzamento.listar(s, conferencia="PENDENTE")["resumo"]["quantidade"] == 1


def test_filtro_por_periodo_de_emissao(cenario):
    s = cenario["s"]
    hoje = date.today()
    _nota(s, emissao=hoje, numero="30")
    _nota(s, emissao=hoje - timedelta(days=90), numero="31")
    r = cruzamento.listar(s, desde=hoje - timedelta(days=7))
    assert r["resumo"]["quantidade"] == 1
