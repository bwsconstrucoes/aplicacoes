"""Os anexos que vêm junto com a SP importada do Pipefy — com banco de verdade.

O dono vai migrar cerca de 70 títulos das obras deste ano, e pediu — desde o
começo — que os ANEXOS viessem junto. Sem eles a SP chega sem a nota e sem o
comprovante, que é justamente o que se precisa consultar depois.

Este arquivo existia como buraco: a mecânica dos anexos não tinha teste
nenhum. O download em si é dublado (não se baixa da internet numa suíte); o
que se prova é tudo em volta dele, que é onde os erros moram.

O que se prova:

  1. O campo de anexo do Pipefy vem em formatos diferentes conforme o campo —
     lista de objetos, texto JSON, uma URL solta — e todos têm de ser lidos.
  2. Cada campo de anexo vira a CATEGORIA certa no ERP: DANFE é nota,
     comprovante é comprovante. É isso que faz o arquivo ser achado depois.
  3. Um arquivo que não baixa NÃO interrompe a importação: fica relatado com o
     motivo, e os outros continuam.
  4. Arquivo grande demais é recusado com motivo, e não derruba nada.
  5. CAMPO DE ANEXO DESCONHECIDO É DENUNCIADO. É o risco real da migração: se
     o pipe tiver um campo que este importador não conhece, o arquivo ficaria
     para trás e o relatório diria "0 anexos" sem nada parecer errado.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.auth.service import gerar_hash
from app.apps.erp.core.importadores import pipefy_cards as imp
from app.apps.erp.db.models.cadastros import (Categoria, Fornecedor, Obra,
                                              PerfilUsuario as P,
                                              RegimeTributario, TipoPessoa, Usuario)
from app.apps.erp.db.models.financeiro import (Anexo, EspecieTitulo, FormaPagamento,
                                               StatusTitulo, TipoTitulo, Titulo)

pytestmark = pytest.mark.banco

PDF = b"%PDF-1.4 anexo vindo do Pipefy"


@pytest.fixture
def cenario(sessao_real):
    s = sessao_real
    u = Usuario(nome="Importador", email="imp@teste.local", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"), perfil=P.ADMIN)
    forn = Fornecedor(razao_social="Fornecedor Exemplo", cnpj_cpf="33444555000166",
                      tipo_pessoa=TipoPessoa.PJ, ativo=True,
                      regime_tributario=RegimeTributario.NAO_INFORMADO)
    obra = Obra(codigo="OBRA-A", nome="Obra A")
    cat = Categoria(codigo="3.1.02", descricao="Material")
    s.add_all([u, forn, obra, cat])
    s.flush()
    from decimal import Decimal
    from datetime import date
    t = Titulo(numero_sp="SP-00001", tipo=TipoTitulo.T1_MATERIAL_NFE,
               especie=EspecieTitulo.PAGAR, fornecedor_id=forn.id,
               descricao="Compra", valor_bruto=Decimal("100.00"),
               valor_liquido=Decimal("100.00"), competencia=date(2026, 8, 1),
               categoria_id=cat.id, forma_pagamento=FormaPagamento.PIX,
               status=StatusTitulo.APROVADO, solicitante_id=u.id)
    s.add(t)
    s.flush()
    return {"s": s, "usuario": u, "titulo": t}


class _RespostaFalsa:
    def __init__(self, conteudo=PDF, erro=None):
        self._c, self._erro = conteudo, erro

    def raise_for_status(self):
        if self._erro:
            raise RuntimeError(self._erro)

    def iter_content(self, tamanho):
        yield self._c


def _dublar_download(monkeypatch, resposta=None, erros=None):
    """A internet, dublada. Guarda quais URLs foram pedidas.

    Cada URL devolve um conteúdo DIFERENTE de propósito: o armazenamento
    guarda por hash e não duplica o mesmo arquivo na mesma entidade — com
    bytes iguais, dois anexos virariam um só e o teste provaria outra coisa.
    """
    pedidas = []

    def falso(url, **kw):
        pedidas.append(url)
        if erros and url in erros:
            return _RespostaFalsa(erro=erros[url])
        if resposta is not None:
            return resposta
        return _RespostaFalsa(conteudo=PDF + url.encode())

    monkeypatch.setattr(imp.requests, "get", falso)
    return pedidas


# ---------------------------------------------------------------------------
# 1. OS FORMATOS EM QUE O PIPEFY MANDA O ANEXO
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("valor,esperado", [
    ('[{"url": "https://x/a.pdf"}]', ["https://x/a.pdf"]),
    ('["https://x/a.pdf", "https://x/b.pdf"]', ["https://x/a.pdf", "https://x/b.pdf"]),
    ([{"url": "https://x/a.pdf"}], ["https://x/a.pdf"]),
    ("https://x/a.pdf", ["https://x/a.pdf"]),
    ("https://x/a.pdf, https://x/b.pdf", ["https://x/a.pdf", "https://x/b.pdf"]),
    ("", []), (None, []), ("[]", []),
    ("nao-e-url", []),
])
def test_o_campo_de_anexo_e_lido_em_todos_os_formatos(valor, esperado):
    assert imp.extrair_urls_anexo(valor) == esperado


# ---------------------------------------------------------------------------
# 2. CADA CAMPO VIRA A CATEGORIA CERTA
# ---------------------------------------------------------------------------
def test_a_danfe_entra_como_nota_e_o_comprovante_como_comprovante(cenario, monkeypatch):
    """É a categoria que faz o arquivo ser achado depois — e é o que separa
    'a nota deste título' de 'um arquivo qualquer'."""
    s = cenario["s"]
    _dublar_download(monkeypatch)
    trazidos = imp.baixar_anexos_do_card(
        s, {"danfe": '["https://x/nota.pdf"]',
            "comprovante": '["https://x/comp.pdf"]'},
        cenario["titulo"].id, cenario["usuario"])
    s.flush()

    assert len(trazidos) == 2
    anexos = {a.nome_arquivo: a.categoria_anexo for a in s.query(Anexo).all()}
    assert anexos["nota.pdf"] == "NOTA"
    assert anexos["comp.pdf"] == "COMPROVANTE"


def test_o_anexo_fica_preso_ao_titulo(cenario, monkeypatch):
    s = cenario["s"]
    _dublar_download(monkeypatch)
    imp.baixar_anexos_do_card(s, {"anexos": '["https://x/a.pdf"]'},
                              cenario["titulo"].id, cenario["usuario"])
    s.flush()
    a = s.query(Anexo).one()
    assert (a.entidade_tipo, a.entidade_id) == ("titulo", cenario["titulo"].id)
    assert "Pipefy" in (a.descricao or "")


def test_o_nome_do_arquivo_vem_da_url_e_e_desescapado(cenario, monkeypatch):
    s = cenario["s"]
    _dublar_download(monkeypatch)
    imp.baixar_anexos_do_card(
        s, {"anexos": '["https://x/pasta/Nota%20Fiscal%201234.pdf"]'},
        cenario["titulo"].id, cenario["usuario"])
    s.flush()
    # O espaço vira sublinhado ao guardar: nome de arquivo com espaço quebra
    # em portal de licitação e em download. O que importa é que o "%20" da URL
    # não chega ao nome.
    assert s.query(Anexo).one().nome_arquivo == "Nota_Fiscal_1234.pdf"


def test_o_mesmo_arquivo_em_dois_campos_nao_vira_dois_anexos(cenario, monkeypatch):
    """O armazenamento guarda por hash: o mesmo PDF anexado em dois campos do
    card não ocupa espaço duas vezes nem aparece duplicado na ficha."""
    s = cenario["s"]
    _dublar_download(monkeypatch, resposta=_RespostaFalsa())     # bytes iguais
    imp.baixar_anexos_do_card(
        s, {"danfe": '["https://x/a.pdf"]', "anexos": '["https://x/b.pdf"]'},
        cenario["titulo"].id, cenario["usuario"])
    s.flush()
    assert s.query(Anexo).count() == 1


# ---------------------------------------------------------------------------
# 3 e 4. QUANDO UM ARQUIVO NÃO VEM
# ---------------------------------------------------------------------------
def test_um_arquivo_que_falha_nao_interrompe_os_outros(cenario, monkeypatch):
    """Numa migração de 70 títulos, uma URL expirada não pode derrubar tudo."""
    s = cenario["s"]
    _dublar_download(monkeypatch, erros={"https://x/ruim.pdf": "HTTP 403"})
    trazidos = imp.baixar_anexos_do_card(
        s, {"anexos": '["https://x/ruim.pdf", "https://x/bom.pdf"]'},
        cenario["titulo"].id, cenario["usuario"])
    s.flush()

    com_erro = [t for t in trazidos if t.get("erro")]
    assert len(com_erro) == 1 and "403" in com_erro[0]["erro"]
    assert s.query(Anexo).count() == 1, "o bom tem de ter entrado"


def test_arquivo_grande_demais_e_recusado_com_motivo(cenario, monkeypatch):
    s = cenario["s"]
    grande = b"x" * (imp.MAX_ANEXO_BYTES + 1024)
    _dublar_download(monkeypatch, resposta=_RespostaFalsa(conteudo=grande))
    trazidos = imp.baixar_anexos_do_card(
        s, {"anexos": '["https://x/enorme.pdf"]'},
        cenario["titulo"].id, cenario["usuario"])
    s.flush()
    assert "MB" in trazidos[0]["erro"]
    assert s.query(Anexo).count() == 0


# ---------------------------------------------------------------------------
# 5. O RISCO DE VERDADE: O CAMPO QUE O IMPORTADOR NÃO CONHECE
# ---------------------------------------------------------------------------
def _card_com_campo(fid, tipo="attachment", valor='["https://x/a.pdf"]', rotulo="Anexo X"):
    return {"id": "123", "title": "SP", "fields": [
        {"field": {"id": fid, "label": rotulo, "type": tipo},
         "name": rotulo, "value": valor, "report_value": None}]}


def test_campo_de_anexo_desconhecido_e_denunciado(cenario):
    """Sem isto o arquivo ficaria para trás e o relatório diria '0 anexos'.
    Silêncio é o pior resultado de uma migração: a SP entra parecendo completa
    e a nota fiscal dela sumiu."""
    achados = imp.campos_de_anexo_desconhecidos(
        _card_com_campo("nota_do_fornecedor_v2", rotulo="Nota do fornecedor"))
    assert len(achados) == 1
    assert achados[0]["campo"] == "nota_do_fornecedor_v2"
    assert achados[0]["rotulo"] == "Nota do fornecedor"
    assert achados[0]["quantos"] == 1


def test_campo_conhecido_nao_e_denunciado(cenario):
    assert imp.campos_de_anexo_desconhecidos(_card_com_campo("danfe")) == []


def test_campo_de_anexo_vazio_nao_e_denunciado(cenario):
    """Campo de anexo que ninguém preencheu não é problema de ninguém."""
    assert imp.campos_de_anexo_desconhecidos(
        _card_com_campo("qualquer_coisa", valor="[]")) == []


def test_campo_que_nao_e_anexo_nao_e_denunciado(cenario):
    assert imp.campos_de_anexo_desconhecidos(
        _card_com_campo("observacoes", tipo="long_text",
                        valor="https://x/a.pdf")) == []


def test_a_denuncia_chega_ao_relatorio_da_importacao(cenario, monkeypatch):
    s = cenario["s"]
    _dublar_download(monkeypatch)
    card = _card_com_campo("campo_novo_de_anexo", rotulo="Contrato assinado")
    # o card não tem credor nem parcela: vira pendência, e é o que interessa —
    # a denúncia do anexo tem de aparecer mesmo assim.
    rel = imp.importar_cards(s, [card], cenario["usuario"], baixar_anexos=True)
    assert rel["anexos_ignorados"] == [
        {"card": "123", "campo": "campo_novo_de_anexo",
         "rotulo": "Contrato assinado", "quantos": 1}]


def test_sem_baixar_anexos_nao_ha_denuncia(cenario):
    """Quem desmarcou 'trazer anexos' escolheu não trazer — cobrar seria ruído."""
    s = cenario["s"]
    rel = imp.importar_cards(s, [_card_com_campo("campo_novo")],
                             cenario["usuario"], baixar_anexos=False)
    assert rel["anexos_ignorados"] == []
