"""Encaminhar informação por WhatsApp — o que sai, para quem, e o que fica.

O PEDIDO, do dono, em 12/09/2026: *"às vezes a gente quer encaminhar alguma
informação pra alguém (…) referente a um título financeiro. O pessoal pede
informação, você quer encaminhar pra um operador, pra um número que a gente
adicionar lá"*.

COM BANCO DE VERDADE porque a primeira trava é o recorte por obra, que vive no
`WHERE` — sem banco, o teste do que NÃO pode ser encaminhado passaria mesmo com
o buraco aberto.

As quatro coisas que estes testes seguram:

  1. só se encaminha o que a pessoa pode VER — senão encaminhar viraria a
     porta dos fundos do controle de acesso;
  2. fica REGISTRADO quem mandou o quê para quem — a mensagem sai do sistema e
     o ERP não controla o que acontece depois; o que ele pode fazer é dar nome
     ao que saiu;
  3. número mal digitado é RECUSADO antes de sair;
  4. o texto leva o que o pessoal pedia — credor, valor, vencimento, forma de
     pagamento, descrição e obra.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.apps.erp.core import encaminhar as svc
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
from app.apps.erp.db.models.cadastros import (
    Categoria, EscopoVisao, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import (
    FormaPagamento, Parcela, Rateio, StatusParcela, StatusTitulo, TipoTitulo,
    Titulo,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real, monkeypatch):
    s = sessao_real
    creche = Obra(codigo="CRECHE", nome="Creche do Eusébio")
    escola = Obra(codigo="ESCOLA", nome="Escola do Planalto")
    forn = Fornecedor(tipo_pessoa=TipoPessoa.PJ, cnpj_cpf="11444777000161",
                      razao_social="CONSTRUTORA ALFA LTDA",
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="2.1.01", descricao="Material")
    s.add_all([creche, escola, forn, cat])
    s.flush()

    def pessoa(nome, email, perfil, obra=None, telefone=None):
        u = Usuario(nome=nome, email=email, telefone=telefone,
                    senha_hash=gerar_hash("senha-de-teste-1234"), perfil=perfil,
                    escopo_visao=EscopoVisao.OBRAS_DESIGNADAS)
        s.add(u)
        s.flush()
        if obra is not None:
            s.add(UsuarioObra(usuario_id=u.id, obra_id=obra.id))
            s.flush()
        return u

    da_creche = pessoa("Ana da creche", "ana.e@teste.bws.local",
                       P.ADMINISTRATIVO_OBRA, obra=creche,
                       telefone="5585999990000")
    financeiro = pessoa("Carla do financeiro", "carla.e@teste.bws.local",
                        P.FINANCEIRO, telefone="5585988880000")
    sem_fone = pessoa("Davi sem telefone", "davi.e@teste.bws.local",
                      P.LANCADOR)

    def titulo(sp, obra):
        t = Titulo(numero_sp=sp, tipo=TipoTitulo.T1_MATERIAL_NFE,
                   fornecedor_id=forn.id,
                   descricao="Compra de areia média — lote 12",
                   valor_bruto=Decimal("1500"), valor_liquido=Decimal("1500"),
                   competencia=date(2026, 9, 1), categoria_id=cat.id,
                   forma_pagamento=FormaPagamento.PIX,
                   status=StatusTitulo.APROVADO, solicitante_id=financeiro.id)
        s.add(t)
        s.flush()
        s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal("1500"),
                     percentual=Decimal("100.0000")))
        s.add(Parcela(titulo_id=t.id, numero=1, valor=Decimal("1500"),
                      vencimento=date(2026, 10, 10),
                      status=StatusParcela.ABERTA))
        s.flush()
        return t

    dados = {"da_creche": da_creche, "financeiro": financeiro,
             "sem_fone": sem_fone,
             "na_creche": titulo("SP9001", creche),
             "na_escola": titulo("SP9002", escola)}
    s.commit()
    return dados


@pytest.fixture
def correio(monkeypatch):
    """O notificador, dublado — nada sai deste contêiner."""
    saiu = []

    def _falso(**kw):
        saiu.append(kw)
        return {"whatsapp": {"ok": True}, "telegram": {"ok": None}}
    monkeypatch.setattr("app.apps.notificador.notificar", _falso, raising=False)
    return saiu


# ---------------------------------------------------------------------------
# 1. O RECORTE POR OBRA
# ---------------------------------------------------------------------------
def test_nao_se_encaminha_o_que_nao_se_pode_ver(cenario, sessao_real, correio):
    """A brecha que este teste fecha: sem o recorte, bastaria mandar o número
    de um título de outra obra para receber o conteúdo dele no próprio
    celular — e o controle de acesso do ERP inteiro viraria enfeite."""
    with pytest.raises(ErroNaoEncontrado):
        svc.enviar(sessao_real, cenario["da_creche"], tipo="titulo",
                   registro_id=cenario["na_escola"].id,
                   usuarios=[cenario["da_creche"].id])
    assert correio == [], "chegou a sair mensagem de um título fora do escopo"


def test_a_previa_tambem_passa_pelo_recorte(cenario, sessao_real):
    """A prévia mostra o texto ANTES de enviar. Se ela não conferisse o
    escopo, o vazamento aconteceria na tela, sem precisar enviar nada."""
    with pytest.raises(ErroNaoEncontrado):
        svc.montar(sessao_real, cenario["da_creche"], tipo="titulo",
                   registro_id=cenario["na_escola"].id)


def test_quem_alcanca_a_obra_encaminha(cenario, sessao_real, correio):
    r = svc.enviar(sessao_real, cenario["da_creche"], tipo="titulo",
                   registro_id=cenario["na_creche"].id,
                   usuarios=[cenario["financeiro"].id])
    assert r["ok"] is True
    assert r["enviados"] == ["Carla do financeiro"]
    assert len(correio) == 1


# ---------------------------------------------------------------------------
# 2. O TEXTO — o que o pessoal pedia por WhatsApp
# ---------------------------------------------------------------------------
def test_o_texto_leva_credor_valor_vencimento_forma_e_obra(cenario, sessao_real):
    _, msg = svc.texto_do_titulo(sessao_real, cenario["na_creche"].id)
    assert "SP9001" in msg
    assert "CONSTRUTORA ALFA LTDA" in msg
    assert "1.500,00" in msg
    assert "10/10/2026" in msg
    assert "PIX" in msg
    assert "CRECHE" in msg
    assert "areia" in msg


def test_o_recado_de_quem_encaminha_vem_antes_da_informacao(cenario, sessao_real, correio):
    """É o recado que explica por que aquilo chegou. Embaixo da informação
    ninguém leria."""
    svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
               registro_id=cenario["na_creche"].id,
               usuarios=[cenario["da_creche"].id],
               observacao="segue o que você pediu")
    msg = correio[0]["mensagem"]
    assert msg.index("segue o que você pediu") < msg.index("SP9001")


# ---------------------------------------------------------------------------
# 3. O NÚMERO AVULSO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("bruto,esperado", [
    ("85 99999-0000", "5585999990000"),
    ("(85) 99999-0000", "5585999990000"),
    ("5585999990000", "5585999990000"),
    ("8533334444", "558533334444"),
])
def test_telefone_aceito_vira_sempre_o_mesmo_formato(bruto, esperado):
    assert svc.normalizar_telefone(bruto) == esperado


@pytest.mark.parametrize("ruim", ["", "1234", "abc", "99999-0000"])
def test_numero_que_nao_parece_telefone_e_recusado(ruim):
    """Um dígito a menos manda a informação para lugar nenhum — e a pessoa
    acha que mandou, que é o pior desfecho."""
    with pytest.raises(ErroValidacao):
        svc.normalizar_telefone(ruim)


def test_numero_avulso_sai_e_fica_registrado(cenario, sessao_real, correio):
    r = svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
                   registro_id=cenario["na_creche"].id,
                   numeros=["85 98888-7777"])
    assert r["ok"] is True
    assert correio[0]["telefone"] == "5585988887777"


# ---------------------------------------------------------------------------
# 4. O REGISTRO — o que dá nome ao que saiu
# ---------------------------------------------------------------------------
def _registros(s, titulo_id):
    return s.execute(text(
        "SELECT destino, situacao, destinatario_id FROM notificacoes "
        " WHERE evento = 'ENCAMINHADO' AND titulo_id = :t"),
        {"t": titulo_id}).all()


def test_fica_registrado_quem_mandou_para_quem(cenario, sessao_real, correio):
    """Sem o registro, um vazamento não teria nem começo de investigação."""
    svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
               registro_id=cenario["na_creche"].id,
               usuarios=[cenario["da_creche"].id], numeros=["85 98888-7777"])
    linhas = _registros(sessao_real, cenario["na_creche"].id)
    assert len(linhas) == 2
    assert {l[1] for l in linhas} == {"ENVIADO"}
    assert "5585988887777" in {l[0] for l in linhas}


def test_quem_nao_tem_telefone_fica_marcado_e_nao_some(cenario, sessao_real, correio):
    """Sumir calado faria a pessoa achar que mandou. O recado diz que é
    cadastro faltando, que é uma coisa que ela resolve."""
    r = svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
                   registro_id=cenario["na_creche"].id,
                   usuarios=[cenario["sem_fone"].id])
    assert r["sem_destino"] == ["Davi sem telefone"]
    assert correio == []
    linhas = _registros(sessao_real, cenario["na_creche"].id)
    assert [l[1] for l in linhas] == ["IGNORADO"]


def test_reenviar_e_um_evento_novo_e_nao_um_repetido(cenario, sessao_real, correio):
    """Ao contrário do aviso automático, reenviar é EXATAMENTE o que se espera
    de um encaminhamento — "manda de novo que eu apaguei"."""
    for _ in range(2):
        svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
                   registro_id=cenario["na_creche"].id,
                   usuarios=[cenario["da_creche"].id])
    assert len(correio) == 2
    assert len(_registros(sessao_real, cenario["na_creche"].id)) == 2


def test_muitos_destinos_de_uma_vez_e_recusado(cenario, sessao_real, correio):
    """Encaminhar para vinte pessoas é criar um grupo sem querer."""
    with pytest.raises(ErroValidacao):
        svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
                   registro_id=cenario["na_creche"].id,
                   numeros=[f"8599999{i:04d}" for i in range(svc.MAX_DESTINOS + 1)])
    assert correio == []


def test_sem_destino_nenhum_e_recusado(cenario, sessao_real):
    with pytest.raises(ErroValidacao):
        svc.enviar(sessao_real, cenario["financeiro"], tipo="titulo",
                   registro_id=cenario["na_creche"].id)


def test_tipo_desconhecido_e_recusado(cenario, sessao_real):
    with pytest.raises(ErroValidacao):
        svc.enviar(sessao_real, cenario["financeiro"], tipo="folha_de_pagamento",
                   registro_id=1, numeros=["85 99999-0000"])
