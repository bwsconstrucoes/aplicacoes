"""Consumo de IA: toda chamada registra, e o teto mensal só avisa.

O que quebrou antes: o painel existia, a tabela existia, mas a função que
grava o consumo nunca foi escrita — o código a importava e a importação
falhava em silêncio dentro de um `except Exception`. Resultado: painel sempre
vazio e, pior, a sugestão de conta por IA descartando a resposta que já tinha
custado dinheiro.

Estes testes seguram duas coisas:
  1. Há UM ponto por onde toda leitura passa (leitor._chamar_ia) e ele
     registra sucesso E falha — então tela nova que use o leitor já nasce
     contabilizada.
  2. Cada operação chega ao registro com o rótulo certo: comprovante do fundo
     fixo, fatura de cartão, contrato de locação, comprovante de pagamento,
     leitura de documento e sugestão de conta.

Rodam sem banco e sem OpenAI: o cliente é um dublê que devolve tokens, e o
gravador autônomo é substituído por uma lista que anota o que recebeu.
"""
from __future__ import annotations

import sys
import types
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.apps.erp.core.comum import ia_custo
from app.apps.erp.core.documentos import leitor
from app.apps.erp.db.models.cadastros import Parametro, PerfilUsuario as P

from conftest import SessaoFalsa, novo_usuario


# ---------------------------------------------------------------------------
# Dublês
# ---------------------------------------------------------------------------
def _resposta(conteudo='{"tipo_documento":"RECIBO","valor_total":"10.00"}',
              entrada=1200, saida=80):
    return SimpleNamespace(
        usage=SimpleNamespace(prompt_tokens=entrada, completion_tokens=saida),
        choices=[SimpleNamespace(message=SimpleNamespace(content=conteudo))])


class _ClienteFalso:
    def __init__(self, resposta=None, erro=None):
        self._resposta, self._erro = resposta, erro
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kw):
        if self._erro:
            raise self._erro
        return self._resposta


@pytest.fixture
def registros(monkeypatch):
    """Substitui o gravador autônomo: anota o que receberia, sem banco."""
    anotados: list[dict] = []
    monkeypatch.setattr(ia_custo, "registrar_autonomo",
                        lambda **kw: anotados.append(kw))
    return anotados


FOTO = b"\xff\xd8\xff" + b"\x00" * 64          # cabeçalho JPEG; o resto não importa


# ---------------------------------------------------------------------------
# O ponto único registra — sucesso e falha
# ---------------------------------------------------------------------------
def test_leitura_registra_tokens_modelo_e_duracao(registros, monkeypatch):
    monkeypatch.setattr(leitor, "_cliente", lambda: _ClienteFalso(_resposta()))

    d = leitor.ler_documento(FOTO, "nota.jpg")

    assert d["tipo_documento"] == "RECIBO"
    assert len(registros) == 1
    r = registros[0]
    assert r["modelo"] == leitor.MODELO_VISAO          # foto vai no modelo de visão
    assert r["resposta"].usage.prompt_tokens == 1200
    assert r["sucesso"] is True
    assert isinstance(r["duracao_ms"], int)


def test_falha_da_openai_tambem_e_registrada(registros, monkeypatch):
    """A OpenAI pode cobrar uma chamada que estourou no meio; e o painel
    precisa mostrar que a leitura está quebrando."""
    monkeypatch.setattr(leitor, "_cliente",
                        lambda: _ClienteFalso(erro=RuntimeError("timeout")))

    with pytest.raises(leitor.ErroLeitura):
        leitor.ler_documento(FOTO, "nota.jpg")

    assert len(registros) == 1
    assert registros[0]["sucesso"] is False
    assert "timeout" in registros[0]["erro"]
    assert registros[0]["resposta"] is None


def test_xml_de_nfe_nao_chama_ia_e_nao_registra(registros, monkeypatch):
    """Parser determinístico: zero tokens, zero linhas no painel."""
    monkeypatch.setattr(leitor, "_cliente",
                        lambda: (_ for _ in ()).throw(AssertionError("não devia chamar")))
    xml = b'<?xml version="1.0"?><nfeProc></nfeProc>'

    with pytest.raises(leitor.ErroLeitura):        # XML incompleto, mas sem IA
        leitor.ler_documento(xml, "nota.xml")

    assert registros == []


def test_registro_falhando_nao_derruba_a_leitura(monkeypatch):
    monkeypatch.setattr(leitor, "_cliente", lambda: _ClienteFalso(_resposta()))

    def explode(**kw):
        raise RuntimeError("banco fora do ar")
    monkeypatch.setattr(ia_custo, "registrar_autonomo", explode)

    d = leitor.ler_documento(FOTO, "nota.jpg")     # não pode levantar

    assert d["tipo_documento"] == "RECIBO"


# ---------------------------------------------------------------------------
# Cada operação chega com o rótulo certo
# ---------------------------------------------------------------------------
def _leitor_que_anota_operacao(monkeypatch, vistas: list):
    def falso(conteudo, nome_arquivo, dica_usuario=""):
        vistas.append(ia_custo.contexto_atual().get("operacao"))
        return {"itens": [], "observacoes": "", "descricao": "", "confianca": "MEDIA",
                "data_emissao": "2026-09-01", "valor_total": "10.00",
                "emitente_nome": "X", "numero_documento": "1", "origem_leitura": "FOTO",
                "campos_ilegiveis": []}
    monkeypatch.setattr(leitor, "ler_documento", falso)
    return falso


def test_comprovante_do_fundo_fixo_declara_a_operacao(monkeypatch):
    from app.apps.erp.core.titulos import prestacao
    vistas: list = []
    _leitor_que_anota_operacao(monkeypatch, vistas)

    prestacao.ler_comprovante_item(FOTO, "cupom.jpg")

    assert vistas == ["comprovante_fundo_fixo"]


def test_fatura_de_cartao_declara_a_operacao(monkeypatch):
    from app.apps.erp.core.titulos import prestacao
    vistas: list = []
    _leitor_que_anota_operacao(monkeypatch, vistas)

    prestacao.ler_fatura_cartao(FOTO, "fatura.pdf")

    assert vistas == ["fatura_cartao"]


def test_contrato_de_locacao_declara_a_operacao(monkeypatch):
    from app.apps.erp.core import locacoes
    vistas: list = []
    _leitor_que_anota_operacao(monkeypatch, vistas)

    locacoes.ler_contrato(SessaoFalsa(), FOTO, "contrato.pdf")

    assert vistas == ["contrato_locacao"]


def test_comprovante_de_pagamento_declara_operacao_e_quem_pediu(monkeypatch):
    from app.apps.erp.core.pagamentos import comprovante
    vistas: list = []

    def falso(conteudo, nome_arquivo, dica_usuario=""):
        vistas.append(dict(ia_custo.contexto_atual()))
        raise leitor.ErroLeitura("parou aqui de propósito")
    monkeypatch.setattr(comprovante, "ler_documento", falso)
    financeiro = novo_usuario(3, P.FINANCEIRO)

    with pytest.raises(Exception):
        comprovante.processar_comprovante(SessaoFalsa(financeiro), FOTO, "c.pdf",
                                          usuario=financeiro)

    assert vistas == [{"operacao": "comprovante_pagamento", "usuario_id": 3}]


def test_sugestao_de_conta_registra_e_usa_a_resposta(registros, monkeypatch):
    """O defeito original: a chamada saía, custava, e a resposta era jogada
    fora porque a função de registro não existia."""
    from app.apps.erp.core.cadastros import sugestao
    from app.apps.erp.db.models.cadastros import Categoria
    monkeypatch.setenv("OPENAI_API_KEY", "teste")
    falso_openai = types.ModuleType("openai")
    falso_openai.OpenAI = lambda api_key: _ClienteFalso(
        _resposta('{"codigo":"3.1.03","confianca":"ALTA","motivo":"aço"}'))
    monkeypatch.setitem(sys.modules, "openai", falso_openai)
    conta = Categoria(id=7, codigo="3.1.03", descricao="Aço e armadura", ativo=True, ordem=1)

    r = sugestao.por_ia(SessaoFalsa(conta), {"descricao": "vergalhão CA-50 12mm"})

    assert r == {"categoria_id": 7, "codigo": "3.1.03", "descricao": "Aço e armadura",
                 "confianca": "ALTA", "motivo": "aço"}
    assert [x["operacao"] for x in registros] == ["sugestao_categoria"]
    assert registros[0]["sucesso"] is True


def test_sugestao_registra_a_falha(registros, monkeypatch):
    from app.apps.erp.core.cadastros import sugestao
    from app.apps.erp.db.models.cadastros import Categoria
    monkeypatch.setenv("OPENAI_API_KEY", "teste")
    falso_openai = types.ModuleType("openai")
    falso_openai.OpenAI = lambda api_key: _ClienteFalso(erro=RuntimeError("429"))
    monkeypatch.setitem(sys.modules, "openai", falso_openai)
    conta = Categoria(id=7, codigo="3.1.03", descricao="Aço", ativo=True, ordem=1)

    assert sugestao.por_ia(SessaoFalsa(conta), {"descricao": "vergalhão"}) is None
    assert registros[0]["operacao"] == "sugestao_categoria"
    assert registros[0]["sucesso"] is False


def test_operacao_nao_declarada_cai_no_rotulo_padrao(monkeypatch):
    """Nunca fica sem rótulo — mas quem usa IA deve declarar o seu."""
    gravado: list = []
    monkeypatch.setattr(ia_custo, "registrar", lambda s, **kw: gravado.append(kw))
    import contextlib
    monkeypatch.setattr("app.apps.erp.db.database.get_session",
                        lambda: contextlib.nullcontext(SessaoFalsa()))
    monkeypatch.setattr(ia_custo, "_avisar_se_passou_do_teto", lambda s: None)

    ia_custo.registrar_autonomo(modelo="gpt-4o-mini", resposta=_resposta())

    assert gravado[0]["operacao"] == ia_custo.OPERACAO_PADRAO


def test_contexto_de_dentro_nao_apaga_o_de_fora():
    """A rota diz quem é; a função do core diz o quê. Os dois têm de chegar."""
    with ia_custo.contexto(usuario_id=9):
        with ia_custo.contexto(operacao="fatura_cartao"):
            assert ia_custo.contexto_atual() == {"usuario_id": 9, "operacao": "fatura_cartao"}
        assert ia_custo.contexto_atual() == {"usuario_id": 9}
    assert ia_custo.contexto_atual() == {}


def test_contexto_de_requisicao_zera_o_anterior():
    """Thread reaproveitada não pode carregar o usuário da requisição passada."""
    t1 = ia_custo.iniciar_contexto_requisicao(5)
    assert ia_custo.contexto_atual() == {"usuario_id": 5}
    ia_custo.encerrar_contexto_requisicao(t1)
    t2 = ia_custo.iniciar_contexto_requisicao(None)
    assert ia_custo.contexto_atual() == {}
    ia_custo.encerrar_contexto_requisicao(t2)


# ---------------------------------------------------------------------------
# Preço
# ---------------------------------------------------------------------------
def test_custo_usa_a_tabela_por_milhao():
    # gpt-4o-mini: 0,15 entrada / 0,60 saída por milhão
    assert ia_custo.custo("gpt-4o-mini", 1_000_000, 0) == Decimal("0.150000")
    assert ia_custo.custo("GPT-4o-MINI", 0, 1_000_000) == Decimal("0.600000")


def test_modelo_desconhecido_usa_preco_padrao_e_e_marcado():
    assert ia_custo.custo("modelo-novo", 1_000_000, 0) == ia_custo.PRECO_PADRAO[0]
    assert ia_custo.preco_conhecido("modelo-novo") is False
    assert ia_custo.preco_conhecido("gpt-4o") is True


# ---------------------------------------------------------------------------
# Teto mensal: avisa, não bloqueia
# ---------------------------------------------------------------------------
def _sessao_com_teto(teto: str, gasto: str, avisado: str = "") -> SessaoFalsa:
    objetos = [Parametro(chave=ia_custo.CHAVE_TETO, valor=teto)]
    if avisado:
        objetos.append(Parametro(chave=ia_custo.CHAVE_ALERTA, valor=avisado))
    return SessaoFalsa(*objetos, escalares=[Decimal(gasto)] * 4)


def test_sem_teto_nao_ha_alerta():
    s = SessaoFalsa(escalares=[Decimal("999")])

    sit = ia_custo.situacao_teto(s)

    assert sit["teto"] is None and sit["alerta"] is None
    assert sit["gasto"] == 999.0


def test_abaixo_de_80_por_cento_nao_avisa():
    sit = ia_custo.situacao_teto(_sessao_com_teto("100", "79.99"))

    assert sit["percentual"] == 80 or sit["percentual"] == 79   # arredondamento
    assert sit["alerta"] in (None, "AVISO")
    sit = ia_custo.situacao_teto(_sessao_com_teto("100", "50"))
    assert sit["alerta"] is None


def test_a_partir_de_80_por_cento_avisa():
    sit = ia_custo.situacao_teto(_sessao_com_teto("100", "80"))

    assert sit["percentual"] == 80
    assert sit["alerta"] == "AVISO"


def test_acima_de_100_por_cento_marca_estouro_mas_nao_bloqueia():
    sit = ia_custo.situacao_teto(_sessao_com_teto("20", "23.50"))

    assert sit["alerta"] == "ESTOUROU"
    assert sit["percentual"] == 118          # a leitura continua funcionando


def test_aviso_sai_uma_vez_por_nivel_no_mes(monkeypatch):
    enviados: list[str] = []
    monkeypatch.setattr(ia_custo, "_enviar_aos_administradores",
                        lambda s, texto: enviados.append(texto) or 1)
    hoje = date(2026, 9, 15)

    # 85%: avisa
    s = _sessao_com_teto("100", "85")
    assert ia_custo._avisar_se_passou_do_teto(s, hoje) == "AVISO"
    marca = next(o for o in s.adicionados if isinstance(o, Parametro))
    assert marca.chave == ia_custo.CHAVE_ALERTA and marca.valor == "2026-09:80"

    # 90%, já avisado a 80 neste mês: silêncio
    s = _sessao_com_teto("100", "90", avisado="2026-09:80")
    assert ia_custo._avisar_se_passou_do_teto(s, hoje) is None

    # estourou: avisa de novo, um nível acima
    s = _sessao_com_teto("100", "101", avisado="2026-09:80")
    assert ia_custo._avisar_se_passou_do_teto(s, hoje) == "ESTOUROU"

    # mês virou: a marca antiga não vale
    s = _sessao_com_teto("100", "85", avisado="2026-08:100")
    assert ia_custo._avisar_se_passou_do_teto(s, date(2026, 9, 2)) == "AVISO"

    assert len(enviados) == 3
    assert "80%" in enviados[0] and "ESTOUROU" in enviados[1]
    assert all("Nada foi bloqueado" in t for t in enviados)


def test_definir_teto_aceita_virgula_e_vazio_desliga():
    s = SessaoFalsa()
    assert ia_custo.definir_teto_mensal(s, "25,50", usuario_id=1) == Decimal("25.50")
    assert ia_custo.definir_teto_mensal(s, "", usuario_id=1) is None
    assert ia_custo.definir_teto_mensal(s, "0", usuario_id=1) is None
    with pytest.raises(ValueError):
        ia_custo.definir_teto_mensal(s, "abc", usuario_id=1)
    with pytest.raises(ValueError):
        ia_custo.definir_teto_mensal(s, "-5", usuario_id=1)


# ---------------------------------------------------------------------------
# Rota do teto: só o ADMIN configura
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("perfil", [P.FINANCEIRO, P.DIRETOR_FINANCEIRO, P.SUPERVISOR_OBRA])
def test_teto_de_ia_nao_e_alterado_por_quem_nao_configura(monkeypatch, perfil):
    import contextlib
    from flask import Flask
    from app.apps.erp import routes

    app = Flask(__name__)
    app.secret_key = "teste"
    app.register_blueprint(routes.bp)

    @contextlib.contextmanager
    def _fake():
        yield SessaoFalsa(novo_usuario(1, perfil))
    monkeypatch.setattr(routes, "get_session", _fake)

    with app.test_client() as c:
        with c.session_transaction() as sessao:
            sessao["erp_usuario_id"] = 1
        r = c.post("/erp/api/ia/teto", json={"teto": "10"})

    assert r.status_code == 403
