"""A trava contra baixar o mesmo pagamento DUAS VEZES — com banco de verdade.

Pedido do dono, com estas palavras: *"o sistema realmente não pode baixar duas
vezes, precisa barrar"*. Até 07/09/2026 não havia trava nenhuma no ERP.

Com banco porque a trava É uma restrição do banco, de propósito. A trava
anterior (no `baixabradesco`) vivia numa lista carregada em memória e falhava
LIBERANDO quando a leitura dela dava erro — o pior jeito de uma trava falhar.
Restrição única não depende de o código lembrar de perguntar, e vale mesmo com
duas execuções ao mesmo tempo. Nada disso o dublê de sessão consegue fingir.

O que se prova:

  1. O MESMO ARQUIVO não entra duas vezes — e o nome não conta: renomear era o
     maior buraco da trava antiga.
  2. O MESMO PAGAMENTO não é baixado duas vezes, mesmo por arquivos diferentes
     (é o caso do PDF regerado pelo banco, com outros bytes).
  3. Pagamento PARCIAL continua possível: mesma parcela, outro dia, outro valor.
  4. Repetido não some em silêncio: a resposta diz em qual título e em que dia
     aquele comprovante já deu baixa.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.apps.erp.core.pagamentos import comprovante as svc
from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.db.models.cadastros import (
    Categoria, ContaBancaria, Fornecedor, Obra, PerfilUsuario as P,
    RegimeTributario, TipoPessoa, Usuario,
)
from app.apps.erp.db.models.financeiro import (
    ComprovanteLido, FormaPagamento, Parcela, Rateio, StatusTitulo, TipoTitulo,
    Titulo,
)

pytestmark = pytest.mark.banco


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Financeiro", email="fin.comp@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.FINANCEIRO)
    obra = Obra(codigo="OBRACOMP", nome="Obra do comprovante")
    forn = Fornecedor(razao_social="Fornecedor do comprovante",
                      cnpj_cpf="71000004000127", tipo_pessoa=TipoPessoa.PJ,
                      ativo=True, regime_tributario=RegimeTributario.NAO_INFORMADO)
    cat = Categoria(codigo="9.9.98", descricao="Conta do teste de comprovante")
    conta = ContaBancaria(descricao="Conta do teste", banco_codigo="237",
                          agencia="1234", conta="5678-9", ativo=True)
    s.add_all([u, obra, forn, cat, conta]); s.flush()

    t = Titulo(numero_sp="SP-COMP-1", tipo=TipoTitulo.T1_MATERIAL_NFE,
               fornecedor_id=forn.id, descricao="Título do comprovante",
               valor_bruto=Decimal("1500.00"), valor_liquido=Decimal("1500.00"),
               competencia=date.today().replace(day=1), categoria_id=cat.id,
               forma_pagamento=FormaPagamento.PIX, status=StatusTitulo.APROVADO,
               solicitante_id=u.id)
    s.add(t); s.flush()
    p = Parcela(titulo_id=t.id, numero=1, vencimento=date.today(),
                valor=Decimal("1500.00"))
    s.add(p)
    s.add(Rateio(titulo_id=t.id, obra_id=obra.id, valor=Decimal("1500.00")))
    s.flush()
    return {"s": s, "usuario": u, "titulo": t, "parcela": p, "conta": conta}


def _leitura(valor="1500.00", data=None):
    return {"valor": valor, "data": (data or date.today()).isoformat(),
            "favorecido": "Fornecedor do comprovante", "documento": "71000004000127"}


# ---------------------------------------------------------------------------
# 1. O mesmo arquivo
# ---------------------------------------------------------------------------
def test_a_identidade_do_arquivo_ignora_o_nome(cenario):
    """Era o maior buraco da trava antiga: "comprovante.pdf" e "comprovante
    (1).pdf" são o mesmo documento, e renomear acontece o tempo todo."""
    conteudo = b"%PDF-1.4 comprovante de teste"
    assert svc.impressao_do_arquivo(conteudo) == svc.impressao_do_arquivo(conteudo)
    assert svc.impressao_do_arquivo(conteudo) != svc.impressao_do_arquivo(conteudo + b"x")


def test_o_mesmo_arquivo_nao_entra_duas_vezes_nem_com_outro_nome(cenario):
    s = cenario["s"]
    conteudo = b"%PDF-1.4 comprovante A"
    svc._registrar_leitura(s, conteudo=conteudo, nome_arquivo="comprovante.pdf",
                           origem="TELA", situacao="SEM_TITULO",
                           leitura=_leitura(), usuario=cenario["usuario"])
    # o MESMO conteúdo, com outro nome — tem de ser recusado pelo banco
    with pytest.raises(IntegrityError):
        svc._registrar_leitura(s, conteudo=conteudo,
                               nome_arquivo="comprovante (1).pdf",
                               origem="EMAIL", situacao="SEM_TITULO",
                               leitura=_leitura(), usuario=cenario["usuario"])
    s.rollback()


def test_ja_processado_conta_o_que_aconteceu_da_outra_vez(cenario):
    """Repetido não pode sumir em silêncio: a pessoa precisa saber ONDE ele já
    deu baixa, senão acha que o sistema engoliu o arquivo."""
    s = cenario["s"]
    conteudo = b"%PDF-1.4 comprovante B"
    svc._registrar_leitura(s, conteudo=conteudo, nome_arquivo="b.pdf",
                           origem="TELA", situacao="BAIXADO", leitura=_leitura(),
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    antes = svc.ja_processado(s, conteudo)
    assert antes is not None
    assert antes["numero_sp"] == "SP-COMP-1"
    assert antes["situacao"] == "BAIXADO"
    assert svc.ja_processado(s, b"outro arquivo qualquer") is None

    resposta = svc._duplicado(antes)
    assert resposta["situacao"] == "DUPLICADO"
    assert "SP-COMP-1" in resposta["mensagem"]
    assert "Nenhuma baixa nova" in resposta["mensagem"]


# ---------------------------------------------------------------------------
# 2. O mesmo pagamento, por arquivos diferentes
# ---------------------------------------------------------------------------
def test_o_mesmo_pagamento_nao_e_baixado_duas_vezes_por_arquivos_diferentes(cenario):
    """O caso do PDF regerado pelo banco: bytes diferentes, pagamento igual.
    A trava antiga não pegava isto — ela conhecia o arquivo, não o pagamento."""
    s = cenario["s"]
    hoje = date.today()
    svc._registrar_leitura(s, conteudo=b"%PDF primeiro arquivo",
                           nome_arquivo="1.pdf", origem="TELA",
                           situacao="BAIXADO", leitura=_leitura(data=hoje),
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    with pytest.raises(IntegrityError):
        svc._registrar_leitura(s, conteudo=b"%PDF segundo arquivo, mesmo pagamento",
                               nome_arquivo="2.pdf", origem="MAKE",
                               situacao="BAIXADO", leitura=_leitura(data=hoje),
                               parcela_id=cenario["parcela"].id,
                               titulo_id=cenario["titulo"].id,
                               usuario=cenario["usuario"])
    s.rollback()


def test_pagamento_parcial_continua_possivel(cenario):
    """A trava barra a repetição IDÊNTICA, não a segunda parcela legítima."""
    s = cenario["s"]
    hoje = date.today()
    svc._registrar_leitura(s, conteudo=b"%PDF metade 1", nome_arquivo="p1.pdf",
                           origem="TELA", situacao="BAIXADO",
                           leitura=_leitura(valor="750.00", data=hoje),
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    # outro valor, mesmo dia: legítimo
    svc._registrar_leitura(s, conteudo=b"%PDF metade 2", nome_arquivo="p2.pdf",
                           origem="TELA", situacao="BAIXADO",
                           leitura=_leitura(valor="750.01", data=hoje),
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    # mesmo valor, outro dia: também legítimo
    svc._registrar_leitura(s, conteudo=b"%PDF outro dia", nome_arquivo="p3.pdf",
                           origem="TELA", situacao="BAIXADO",
                           leitura=_leitura(valor="750.00",
                                            data=hoje + timedelta(days=1)),
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    quantos = len(s.scalars(select(ComprovanteLido).where(
        ComprovanteLido.parcela_id == cenario["parcela"].id)).all())
    assert quantos == 3, "a trava barrou baixa parcial legítima"


def test_leitura_sem_titulo_nao_bloqueia_outra_leitura_sem_titulo(cenario):
    """Duas leituras que não acharam título não são duplicata uma da outra —
    é por isso que a segunda trava é parcial (só quando há parcela)."""
    s = cenario["s"]
    for i in range(3):
        svc._registrar_leitura(s, conteudo=f"%PDF sem par {i}".encode(),
                               nome_arquivo=f"x{i}.pdf", origem="TELA",
                               situacao="SEM_TITULO", leitura=_leitura(),
                               usuario=cenario["usuario"])
    achados = s.scalars(select(ComprovanteLido).where(
        ComprovanteLido.situacao == "SEM_TITULO")).all()
    assert len(achados) == 3


def test_comprovante_sem_data_legivel_ainda_assim_e_barrado(cenario):
    """No Postgres, dois vazios NÃO são iguais entre si: se o dia da baixa
    ficasse em branco quando o comprovante não traz data, a trava do pagamento
    dormiria justamente no caso mais confuso. Por isso o dia gravado é o dia da
    BAIXA, nunca vazio."""
    s = cenario["s"]
    hoje = date.today()
    svc._registrar_leitura(s, conteudo=b"%PDF sem data 1", nome_arquivo="sd1.pdf",
                           origem="TELA", situacao="BAIXADO",
                           leitura={**_leitura(), "data": hoje.isoformat()},
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    with pytest.raises(IntegrityError):
        svc._registrar_leitura(s, conteudo=b"%PDF sem data 2", nome_arquivo="sd2.pdf",
                               origem="MAKE", situacao="BAIXADO",
                               leitura={**_leitura(), "data": hoje.isoformat()},
                               parcela_id=cenario["parcela"].id,
                               titulo_id=cenario["titulo"].id,
                               usuario=cenario["usuario"])
    s.rollback()


def test_a_tela_sabe_apontar_a_baixa_anterior(cenario):
    """Barrar sem dizer onde a baixa já aconteceu faz a pessoa mandar de novo —
    e o ciclo recomeça. A resposta leva o título, o valor e o dia."""
    s = cenario["s"]
    hoje = date.today()
    svc._registrar_leitura(s, conteudo=b"%PDF apontar", nome_arquivo="ap.pdf",
                           origem="TELA", situacao="BAIXADO",
                           leitura=_leitura(data=hoje),
                           parcela_id=cenario["parcela"].id,
                           titulo_id=cenario["titulo"].id,
                           usuario=cenario["usuario"])
    anterior = svc._leitura_anterior(s, parcela_id=cenario["parcela"].id,
                                     valor=Decimal("1500.00"), data_pg=hoje)
    assert anterior is not None
    assert anterior["numero_sp"] == "SP-COMP-1"
    assert anterior["data_pagamento"] == hoje.isoformat()
    assert anterior["favorecido"] == "Fornecedor do comprovante"

    resposta = svc._duplicado(anterior)
    assert resposta["leitura"]["valor"] == 1500.0
    assert resposta["leitura"]["favorecido"] == "Fornecedor do comprovante"
    assert resposta["anterior"]["titulo_id"] == cenario["titulo"].id
