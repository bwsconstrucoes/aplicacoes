# -*- coding: utf-8 -*-
"""
Nota com valor errado: o cancelamento e o valor da substituição.

Por que este arquivo existe — 17/11/2026. Uma nota da obra CREPEEXU, medição 19,
saiu por **R$ 13.053,64** quando devia ser **R$ 13.053,54** — dez centavos a
mais. O dono corrigiu o Valor da Medição no card, atualizou o título no Omie, e
foi substituir a nota. Não tinha caminho:

1. **A tela propunha um valor e se recusava a emiti-lo.** Na substituição, o
   valor-base da nota nova era o da nota ANTIGA, cego ao card. Ela propunha
   13.053,64, e a validação logo abaixo barrava: *"Valor desta nota (R$
   13.053,64) é MAIOR que o Valor da Medição (R$ 13.053,54)"*. A tela brigando
   com ela mesma.
2. **Substituir não resolve valor menor, e o caminho de cancelamento não existia
   aqui.** O município só aceita substituição por valor igual ou maior; valor
   menor é cancelamento. Mas cancelar no portal não destravava nada: o slot do
   card continuava "válido" e consumia o teto da medição, então a nota nova era
   barrada. O único jeito de marcar a antiga como cancelada era o
   `/emissao/recuperar`, que **exige o XML de uma nota nova** — a que ainda não
   podia ser emitida.
3. **E o número ainda podia não casar.** O slot guarda os 13 dígitos do padrão
   nacional; quem digita costuma ter o curto na mão.
"""
import os
import sys
from decimal import Decimal

import pytest

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

from app.main import app as app_real          # noqa: E402
import validacao as val                       # noqa: E402
import pipefy_update as pfu                   # noqa: E402

TOKEN = "TOKEN-DE-TESTE"
CARD = "1447316614"

# Os números reais do caso de 17/11/2026.
NOTA_LONGO = "2600000003377"
NOTA_CURTO = "3377"
# O slot do card guarda o valor CRU do Pipefy (formato BR); o Valor da Medição
# chega já normalizado pelo `extrair_card` (ponto decimal). São formatos
# diferentes no mesmo card, e é assim que a produção entrega.
VALOR_ERRADO = "13.053,64"    # o que a nota saiu (slot, cru)
VALOR_CERTO = "13053.54"      # o que o card diz agora (normalizado)


def _card(valor_medicao=VALOR_CERTO, slots=(("A", "Válida", NOTA_LONGO, VALOR_ERRADO),)):
    """Card da medição com os slots que interessam."""
    por_id = {}
    for slot, status, numero, valor in slots:
        f = pfu.CAMPOS_SLOT[slot]
        por_id[f["status"]] = status
        por_id[f["numero"]] = numero
        por_id[f["valor"]] = valor
    return {"card_id": CARD, "codigo_obra": "CREPEEXU", "numero_medicao": "19",
            "valor_medicao": valor_medicao, "bdi": "0", "emissao_nf": "Sim",
            "tipo_medicao": "", "banco": "", "campos_por_id": por_id, "campos_raw": [],
            "omie_integracao": "CREPEEXU-19", "tipo_documento": "", "empenho": "",
            "periodo_ini": "01/10/2026", "periodo_fim": "31/10/2026"}


# --------------------------------------------------------------------------- #
# [3] O número da nota a ignorar casa pelo sequencial
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("digitado", [NOTA_CURTO, NOTA_LONGO, "3.377"])
def test_a_nota_em_substituicao_sai_do_teto_nos_dois_formatos(digitado):
    slots = val.slots_preenchidos(_card(), ignorar_numero=digitado)
    assert [x["valida"] for x in slots] == [False], \
        "a nota que está sendo substituída não pode contar no teto da medição"


def test_nota_de_outro_numero_continua_contando_no_teto():
    slots = val.slots_preenchidos(_card(), ignorar_numero="3378")
    assert [x["valida"] for x in slots] == [True]


# --------------------------------------------------------------------------- #
# [1] O valor-base da substituição respeita o teto do card
# --------------------------------------------------------------------------- #
def _valor_base_da_substituicao(card, nota_sub):
    """Chama a REGRA de verdade (`worker.valor_base_substituicao`) e desdobra o
    resultado no que os testes querem olhar: (usado, antiga, cabe).

    A regra foi tirada de dentro do `preparar` justamente para caber num teste: o
    `preparar` fala com o Pipefy, com a C. Diários e com o certificado, e nada
    disso tem a ver com escolher o valor.
    """
    import worker
    texto, ajuste = worker.valor_base_substituicao(card, nota_sub)
    import substituicao as _sub
    alvo = _sub._chave_numero(nota_sub)
    antiga = outras = Decimal("0")
    for s in val.slots_preenchidos(card):
        if _sub._chave_numero(s["numero"]) == alvo:
            antiga = Decimal(str(s["valor"]))
        elif s["valida"]:
            outras += Decimal(str(s["valor"]))
    cabe = val._dec_norm(card.get("valor_medicao")) - outras
    if ajuste:                       # quando ajusta, a própria regra diz os números
        assert ajuste["antiga"] == antiga and ajuste["cabe"] == cabe
        assert str(ajuste["usado"]) == str(Decimal(texto))
    return Decimal(texto), antiga, cabe


def test_nota_fora_dos_slots_nao_ganha_valor_default():
    """Sem achar a nota antiga não há de onde tirar valor — e aí o cálculo segue
    pelo card, como numa emissão comum."""
    import worker
    assert worker.valor_base_substituicao(_card(), "9999") == (None, None)


def test_o_valor_da_substituicao_cabe_no_teto_corrigido_do_card():
    """O caso de 17/11: o card foi corrigido para menos, e o card ganha."""
    usado, antiga, cabe = _valor_base_da_substituicao(_card(), NOTA_CURTO)
    assert antiga == Decimal("13053.64")
    assert cabe == Decimal("13053.54")
    assert usado == Decimal("13053.54"), \
        "a tela não pode propor um valor que ela mesma vai barrar"


def test_substituicao_normal_continua_pelo_valor_da_nota_antiga():
    """Quando o card comporta, nada muda — é o caso de toda substituição normal."""
    card = _card(valor_medicao="20000.00")
    usado, antiga, _cabe = _valor_base_da_substituicao(card, NOTA_LONGO)
    assert usado == antiga == Decimal("13053.64")


def test_o_teto_desconta_as_outras_notas_validas():
    """Medição faturada em partes: o que cabe é o teto menos as OUTRAS válidas."""
    card = _card(valor_medicao="20000.00",
                 slots=(("A", "Válida", NOTA_LONGO, VALOR_ERRADO),
                        ("B", "Válida", "2600000003390", "9.000,00")))
    usado, antiga, cabe = _valor_base_da_substituicao(card, NOTA_LONGO)
    assert antiga == Decimal("13053.64")
    assert cabe == Decimal("11000.00")
    assert usado == Decimal("11000.00")


def test_nota_cancelada_nao_entra_na_conta_do_que_cabe():
    card = _card(valor_medicao="20000.00",
                 slots=(("A", "Válida", NOTA_LONGO, VALOR_ERRADO),
                        ("B", "Cancelada", "2600000003390", "9.000,00")))
    _usado, _antiga, cabe = _valor_base_da_substituicao(card, NOTA_LONGO)
    assert cabe == Decimal("20000.00")


def test_com_o_valor_ajustado_a_validacao_para_de_barrar():
    """O fecho da história: com o valor do card, os dois bloqueios somem."""
    class _R:
        valor_total = Decimal("13053.54")
        iss = inss = Decimal("0")
        federais_retidos = {}
    r = val.checar(_card(), _R(), ignorar_numero=NOTA_CURTO)
    assert [b for b in r["bloqueios"] if "MAIOR que o Valor" in b or "excede" in b] == []


def test_com_o_valor_da_nota_antiga_a_validacao_barrava():
    """O mesmo card, com o valor que a tela propunha antes: barrado nos dois."""
    class _R:
        valor_total = Decimal("13053.64")
        iss = inss = Decimal("0")
        federais_retidos = {}
    r = val.checar(_card(), _R(), ignorar_numero=NOTA_CURTO)
    assert any("MAIOR que o Valor" in b for b in r["bloqueios"])
    assert any("excede" in b for b in r["bloqueios"])


# --------------------------------------------------------------------------- #
# [2] A tela que registra uma nota cancelada no portal
# --------------------------------------------------------------------------- #
@pytest.fixture
def cliente(monkeypatch):
    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    return app_real.test_client()


class _AbaFalsa:
    def __init__(self, coluna_f):
        self._f = list(coluna_f)
        self.escrito = []

    def col_values(self, n):
        return self._f

    def update(self, faixa, valores, value_input_option=None):
        self.escrito.append((faixa, valores[0][0]))


@pytest.fixture
def mundo(monkeypatch):
    """Card, planilha e Omie dublados — nada sai daqui."""
    import app.apps.emissaonf.web as web

    feito = {"pipefy": [], "omie": []}
    aba = _AbaFalsa(["Nº Nota", NOTA_LONGO])
    card = _card()

    monkeypatch.setattr(web, "_card_aba_e_cred",
                        lambda cid: (card, aba, {"PIPEFY_TOKEN": "t"}))
    monkeypatch.setattr(web._pipefy, "get_card", lambda cid, tk: {})
    monkeypatch.setattr(web._pipefy, "extrair_card", lambda bruto: card)
    monkeypatch.setattr(web._sub.pf, "executar",
                        lambda mutation, token: feito["pipefy"].append(mutation))

    def remover(creds, integracao, numero):
        feito["omie"].append((integracao, numero))
        return {}, ""

    monkeypatch.setattr(web.omie, "remover_documento", remover)
    return web, feito, aba, card


def _registrar(cliente, **extra):
    dados = {"token": TOKEN, "card_id": CARD, "numero": NOTA_CURTO}
    dados.update(extra)
    return cliente.post("/emissao/cancelada", data=dados).get_data(as_text=True)


def test_a_tela_existe_e_explica_que_nao_cancela_na_prefeitura(cliente):
    corpo = cliente.get(f"/emissao/cancelada?token={TOKEN}").get_data(as_text=True)
    assert "não cancela nada na prefeitura" in corpo
    assert "consome o teto da medição" in corpo


def test_sem_o_token_a_tela_nao_abre(cliente):
    assert cliente.get("/emissao/cancelada").status_code == 403


def test_registrar_marca_o_slot_a_planilha_e_tira_do_omie(cliente, mundo):
    _web, feito, aba, _card_ = mundo
    corpo = _registrar(cliente)
    # o slot do card
    assert len(feito["pipefy"]) == 1
    assert "Cancelada" in feito["pipefy"][0]
    # a linha da planilha
    assert aba.escrito == [("L2", "CANCELADA")]
    # o número fora do título do Omie
    assert feito["omie"] == [("CREPEEXU-19", NOTA_CURTO)]
    assert "cancelada nos três lugares" in corpo


def test_o_numero_curto_acha_a_nota_gravada_no_formato_longo(cliente, mundo):
    """O slot e a planilha têm 2600000003377; ele digita 3377."""
    _web, feito, aba, _c = mundo
    _registrar(cliente, numero=NOTA_CURTO)
    assert "Cancelada" in feito["pipefy"][0]
    assert aba.escrito and aba.escrito[0][0] == "L2"


def test_o_motivo_entra_na_observacao_da_planilha(cliente, mundo):
    _web, _feito, aba, _c = mundo
    _registrar(cliente, motivo="valor da medição estava errado")
    assert aba.escrito == [("L2", "CANCELADA — valor da medição estava errado")]


def test_a_tela_diz_quanto_ainda_cabe_emitir(cliente, mundo):
    """É a informação que destrava a reemissão: o dono precisa saber que o teto
    voltou a caber a nota nova."""
    corpo = _registrar(cliente)
    assert "Cabe emitir até" in corpo


def test_a_tela_avisa_que_as_retencoes_do_titulo_nao_foram_mexidas(cliente, mundo):
    corpo = _registrar(cliente)
    assert "retenções do título no Omie continuam as da nota cancelada" in corpo


def test_numero_que_nao_esta_em_slot_nenhum_grita(cliente, mundo):
    corpo = _registrar(cliente, numero="9999")
    assert "NÃO foi cancelada" in corpo
    assert "NÃO CANCELADO NO CARD" in corpo


def test_sem_card_ou_sem_numero_a_tela_pede_de_volta(cliente, mundo):
    corpo = cliente.post("/emissao/cancelada",
                         data={"token": TOKEN, "numero": NOTA_CURTO}).get_data(as_text=True)
    assert "Preciso do número do card" in corpo


# --------------------------------------------------------------------------- #
# O banner da substituição não pode prometer o que o botão recusa
# --------------------------------------------------------------------------- #
def test_o_banner_diz_o_que_o_botao_emitir_faz_de_verdade(cliente):
    """Duas versões deste banner foram cobradas em 17/11/2026: a primeira
    PROMETIA um cancelamento que o botão não fazia; a segunda EXPLICAVA dois
    caminhos para o dono navegar à mão. Agora ele diz o que o botão faz."""
    fonte = open(os.path.join(_EMISSAONF, "web.py"), encoding="utf-8").read()
    assert "Ao clicar em <b>Emitir</b>, eu confiro os valores" in fonte
    assert "Ao emitir, a NF" not in fonte, \
        "a promessa antiga não pode ter sobrado em lugar nenhum"
    assert "O botão Emitir NÃO faz substituição" not in fonte, \
        "agora ele faz — a tela pergunta e cuida do resto"


# --------------------------------------------------------------------------- #
# O FLUXO ÚNICO: a tela decide, pergunta uma vez, e faz o resto
#
# 17/11/2026, pedido do dono depois de bater em três paredes seguidas:
#
#   "o correto é a própria automação entender o que é que precisa ser feito e
#    sugerir. Se na hora que for tentar emitir a nota, for percebido que é
#    necessário cancelamento, aí (…) questionar se é para cancelar, cancelar e
#    depois emitir. (…) para o pipe e para o homem não importa se é substituição
#    ou se é cancelamento. E nem para a planilha. (…) O procedimento
#    diferenciado fica só no sistema da prefeitura."
# --------------------------------------------------------------------------- #
@pytest.fixture
def emissao(monkeypatch):
    """A tela de emissão com o card dublado — nada sai daqui."""
    import app.apps.emissaonf.web as web
    card = _card()
    monkeypatch.setattr(web, "_so_o_card", lambda cid: (card, {"PIPEFY_TOKEN": "t"}))
    return web, card


def _tentar_emitir(cliente, **extra):
    dados = {"token": TOKEN, "card_id": CARD, "confirmo": "on",
             "nota_substituida": NOTA_LONGO}
    dados.update(extra)
    return cliente.post("/emissao/emitir", data=dados).get_data(as_text=True)


def test_valor_menor_vai_direto_para_a_tela_de_cancelar(cliente, emissao):
    """O caso real: nota de 13.053,64 e card corrigido para 13.053,54.

    Valor menor é cancelamento, e o cancelamento é do sistema — então a tela já
    abre na confirmação, sem fazer ele escolher entre caminhos."""
    corpo = _tentar_emitir(cliente)
    assert "valor MENOR, então o caminho é CANCELAR" in corpo
    assert "13.053,64" in corpo and "13.053,54" in corpo
    assert "Cancelar a NF 2600000003377 e emitir a nota nova" in corpo


def test_valor_maior_vai_direto_para_a_tela_de_substituir(cliente, emissao):
    """Valor igual ou maior dá para substituir — e a substituição também é do
    sistema: a chave da nota antiga vai dentro da declaração da nota nova."""
    corpo = _tentar_emitir(cliente, valor="20.000,00")
    assert "igual ou maior, então dá para SUBSTITUIR" in corpo
    assert "Substituir a NF 2600000003377 pela nota nova" in corpo
    assert "a plataforma nacional cancela a antiga" in corpo
    assert "Você não precisa abrir o portal" in corpo


def test_a_tela_diz_que_o_sistema_faz_as_duas_coisas(cliente, emissao):
    """O ponto do desenho: uma ação só, e o sistema cuida das duas pontas."""
    corpo = _tentar_emitir(cliente)
    assert "cancelo a NF 2600000003377" in corpo
    assert "na prefeitura" in corpo
    assert "numa só vez" in corpo


def test_nada_e_emitido_antes_de_ele_confirmar(cliente, emissao, monkeypatch):
    """A tela de pergunta não pode emitir nada — nem preparar o card."""
    web, _card_ = emissao
    chamou = []
    monkeypatch.setattr(web._worker, "preparar",
                        lambda *a, **k: chamou.append(1) or {})
    corpo = _tentar_emitir(cliente)
    assert chamou == [], "não pode nem preparar a emissão antes da confirmação"
    assert "nada sai de cena e nada é" in corpo


def test_sem_conseguir_ler_o_card_os_dois_caminhos_sao_oferecidos(cliente, monkeypatch):
    """Nunca se chuta um caminho: sem os valores, quem escolhe é ele.

    E a tela não pode ficar inalcançável por causa disso — lição de 07/10/2026,
    quando o aviso vinha depois de carregar o card e quem tentava substituir
    recebia uma mensagem sobre um parâmetro do link."""
    import app.apps.emissaonf.web as web

    def explode(cid):
        raise RuntimeError("Pipefy fora do ar")

    monkeypatch.setattr(web, "_so_o_card", explode)
    corpo = _tentar_emitir(cliente)
    assert "Não consegui conferir os valores" in corpo
    assert "value='cancelar'" in corpo and "value='substituir'" in corpo
    assert "checked" not in corpo, "sem os valores não se marca nenhuma opção"


def test_nota_fora_dos_slots_tambem_oferece_os_dois(cliente, emissao):
    corpo = _tentar_emitir(cliente, nota_substituida="9999")
    assert "não está em nenhum dos slots" in corpo
    assert "value='cancelar'" in corpo and "value='substituir'" in corpo


def test_o_aviso_do_bdi_vazio_aparece_antes_do_portal(cliente, emissao):
    """O campo silencioso: vazio não barra nada e faz a retenção sair a mais.
    Avisar antes de ele mexer no portal é mais barato que depois."""
    corpo = _tentar_emitir(cliente)
    assert "Valor BDI Diferenciado" in corpo
    assert "retenções vão a mais" in corpo


def test_substituir_sem_confirmar_nao_faz_nada(cliente, emissao):
    """A substituição também tira uma nota de cena, então também pede confirmação."""
    corpo = _tentar_emitir(cliente, ja_fiz_no_portal="on", acao_portal="substituir")
    assert "não marcou a confirmação" in corpo


def test_a_recuperacao_abre_preenchida_pelo_link(cliente):
    corpo = cliente.get(f"/emissao/recuperar?token={TOKEN}&card_id={CARD}"
                        f"&nota_substituida={NOTA_LONGO}").get_data(as_text=True)
    assert f"value='{CARD}'" in corpo
    assert f"value='{NOTA_LONGO}'" in corpo


# --------------------------------------------------------------------------- #
# O CANCELAMENTO NA PREFEITURA — o sistema faz, ele só confirma
#
# 17/11/2026: "Não é tudo automático, não? A automação cancela e emite nova nota?
# (…) E por que tu não faz tudo?" Não havia motivo bom: o endereço da API já
# estava no código, o schema do pedido está versionado em xsd_nacional/ e a
# assinatura é a mesma da declaração. Faltava montar o XML do evento.
# --------------------------------------------------------------------------- #
CHAVE = "2" * 50


def test_o_pedido_de_cancelamento_valida_contra_o_schema_oficial():
    """A única conferência de verdade que existe sem cancelar uma nota: o schema
    oficial da prefeitura, versionado em `xsd_nacional/`."""
    from lxml import etree
    import evento_cancelamento as ev
    xsd = etree.XMLSchema(etree.parse(
        os.path.join(_EMISSAONF, "xsd_nacional", "pedRegEvento_v1.01.xsd")))
    xml = ev.montar_xml(CHAVE, "28520007952600", "1",
                        "Valor da medicao corrigido; nota substituida por outra", True)
    doc = etree.fromstring(xml)
    assert xsd.validate(doc), [e.message for e in xsd.error_log]


def test_o_identificador_do_pedido_tem_a_forma_que_o_schema_exige():
    """PRE + chave (50) + tipo do evento (6) = PRE + 56 dígitos."""
    import re
    import evento_cancelamento as ev
    ident = ev.gerar_id_pedido(CHAVE)
    assert re.fullmatch(r"PRE[0-9]{56}", ident)
    assert ident.endswith("101101")


def test_chave_com_tamanho_errado_e_barrada_aqui_nao_na_prefeitura():
    import evento_cancelamento as ev
    with pytest.raises(ev.DadoInvalido) as e:
        ev.montar_xml("123", "28520007952600", "1", "Motivo com tamanho suficiente", True)
    assert "50 dígitos" in str(e.value)


def test_motivo_curto_e_barrado_aqui(a_prefeitura_exige_de_15_a_255=None):
    import evento_cancelamento as ev
    with pytest.raises(ev.DadoInvalido) as e:
        ev.montar_xml(CHAVE, "28520007952600", "1", "errado", True)
    assert "15 a 255" in str(e.value)


def test_motivo_inexistente_e_barrado_aqui():
    import evento_cancelamento as ev
    with pytest.raises(ev.DadoInvalido) as e:
        ev.montar_xml(CHAVE, "28520007952600", "7", "Motivo com tamanho suficiente", True)
    assert "não existe" in str(e.value)


def test_homologacao_e_producao_vao_no_pedido():
    from lxml import etree
    import evento_cancelamento as ev
    for producao, esperado in ((True, "1"), (False, "2")):
        doc = etree.fromstring(ev.montar_xml(CHAVE, "28520007952600", "1",
                                             "Motivo com tamanho suficiente", producao))
        amb = doc.find(".//{http://www.sped.fazenda.gov.br/nfse}tpAmb")
        assert amb.text == esperado


def test_a_tela_de_cancelamento_pede_confirmacao_e_motivo(cliente, emissao):
    corpo = _tentar_emitir(cliente)
    assert "Cancelar na prefeitura e emitir" in corpo
    assert "NÃO SE DESFAZ" in corpo
    assert "motivo_codigo" in corpo and "motivo_texto" in corpo
    assert "confirmo_cancelamento" in corpo
    # o caminho de escape continua existindo
    assert "Já fiz no portal" in corpo


def test_sem_marcar_a_confirmacao_nada_acontece(cliente, emissao, monkeypatch):
    web, _c = emissao
    chamou = []
    monkeypatch.setattr(web._worker, "preparar", lambda *a, **k: chamou.append(1) or {})
    corpo = _tentar_emitir(cliente, ja_fiz_no_portal="on", acao_portal="cancelar")
    assert "não marcou a confirmação" in corpo
    assert chamou == [], "nem a emissão pode começar sem a confirmação"
