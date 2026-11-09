# -*- coding: utf-8 -*-
"""
A substituição de nota: o cancelamento da nota ANTIGA.

Por que este arquivo existe — 09/11/2026. O dono substituiu a NF 2600000003352
pelo portal da prefeitura, rodou o `/emissao/recuperar` com o número dela no
campo "nota substituída", e **a nota antiga não foi cancelada em lugar nenhum**:
continuou válida no slot do card e sem marca na planilha. Ele descobriu olhando,
e cancelou os dois à mão.

Eram três defeitos somados, e qualquer um deles bastava:

1. **O número não casava.** No padrão nacional a nota é gravada com 13 dígitos
   (ano + sequencial). O slot e a coluna F da planilha têm `2600000003352`; quem
   digita no campo põe o número que tem na mão, às vezes o curto, `3352`. A
   comparação era por dígitos crus, então "3352" != "2600000003352" e nada era
   encontrado.
2. **O cancelamento só rodava com "concluir completo" marcado.** Sem a marca, o
   número da nota substituída era lido e ignorado, em silêncio.
3. **Um erro no `concluir` levava o cancelamento embora.** Os dois estavam no
   mesmo `try`: qualquer exceção no bookkeeping da nota nova pulava o
   cancelamento da antiga.

E havia um quarto, que é o que impediu de perceber: quando o cancelamento não
achava a nota, a mensagem saía como uma linha no meio de um log de dezenas de
linhas. Agora o resultado dos três passos aparece em destaque, com ❌.
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
import substituicao as sub                    # noqa: E402
import pipefy_update as pfu                   # noqa: E402

TOKEN = "TOKEN-DE-TESTE"
CARD = "1447316614"

# A nota real da substituição de 09/11/2026, nos dois formatos.
LONGO = "2600000003352"
CURTO = "3352"


def _card_com_slot(slot, numero):
    """Um card de mentira com o número da nota gravado em UM slot."""
    return {"card_id": CARD,
            "campos_por_id": {pfu.CAMPOS_SLOT[slot]["numero"]: numero}}


# --------------------------------------------------------------------------- #
# [1] O número: curto e longo são a MESMA nota, nos dois sentidos
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("gravado,digitado", [
    (LONGO, CURTO),     # o caso real de 09/11/2026
    (LONGO, LONGO),
    (CURTO, LONGO),     # nota antiga (modelo ABRASF) e o dono digita o longo
    (CURTO, CURTO),
    ("3.352", CURTO),   # o Pipefy às vezes devolve com ponto de milhar
])
def test_o_slot_da_nota_antiga_e_achado_nos_dois_formatos(gravado, digitado):
    card = _card_com_slot("B", gravado)
    assert sub.localizar_slot_por_numero(card, digitado) == "B"


def test_numero_de_outra_nota_nao_acha_slot():
    card = _card_com_slot("B", LONGO)
    assert sub.localizar_slot_por_numero(card, "3353") is None
    assert sub.localizar_slot_por_numero(card, "2600000003353") is None


def test_o_aviso_de_slot_nao_encontrado_diz_que_a_antiga_continua_valendo():
    """O que faltou em 09/11: a mensagem tinha de ser impossível de passar batida."""
    card = _card_com_slot("B", LONGO)
    r = sub.cancelar_no_card(card, "9999", token="x")
    assert r["ok"] is False
    assert "NÃO CANCELADO NO CARD" in r["msg"]
    assert "continua valendo" in r["msg"]


class _AbaFalsa:
    """A 'Notas BWS' reduzida ao que a substituição usa: coluna F e update em L."""
    def __init__(self, coluna_f):
        self._f = list(coluna_f)
        self.escrito = []

    def col_values(self, n):
        assert n == 6, "a substituição localiza pela coluna F (Nº Nota)"
        return self._f

    def update(self, faixa, valores, value_input_option=None):
        self.escrito.append((faixa, valores[0][0]))


@pytest.mark.parametrize("gravado,digitado", [
    (LONGO, CURTO),
    (LONGO, LONGO),
    (CURTO, LONGO),
])
def test_a_linha_da_planilha_e_achada_nos_dois_formatos(gravado, digitado):
    aba = _AbaFalsa(["Nº Nota", "2600000003351", gravado])
    r = sub.cancelar_na_planilha(aba, digitado, novo_numero="2600000003399")
    assert r["ok"] is True and r["linha"] == 3
    faixa, texto = aba.escrito[0]
    assert faixa == "L3"
    assert texto.startswith("CANCELADA")
    assert "2600000003399" in texto


def test_o_aviso_de_linha_nao_encontrada_diz_que_a_linha_ficou_sem_marca():
    aba = _AbaFalsa(["Nº Nota", LONGO])
    r = sub.cancelar_na_planilha(aba, "9999")
    assert r["ok"] is False
    assert "NÃO MARCADO NA PLANILHA" in r["msg"]
    assert aba.escrito == [], "não pode escrever em linha nenhuma quando não achou"


# --------------------------------------------------------------------------- #
# A tela /emissao/recuperar
# --------------------------------------------------------------------------- #
XML_NACIONAL = (
    "<?xml version='1.0' encoding='UTF-8'?>"
    "<NFSe xmlns='http://www.sped.fazenda.gov.br/nfse'>"
    "<infNFSe Id='NFS2304285200079526000109000012600000003399'>"
    "<nNFSe>2600000003399</nNFSe><dhProc>2026-11-09T10:00:00-03:00</dhProc>"
    "<valores><vServ>100000.00</vServ><vISSQN>2000.00</vISSQN>"
    "<vRetIRRF>1200.00</vRetIRRF></valores>"
    "</infNFSe></NFSe>"
)


class _ResultadoFalso:
    """O que a trava de valor lê do recálculo do card."""
    def __init__(self, valor_total, iss, inss, federais_retidos):
        self.valor_total = Decimal(valor_total)
        self.iss = Decimal(iss)
        self.inss = Decimal(inss)
        self.federais_retidos = {k: Decimal(v) for k, v in federais_retidos.items()}


# O card que continua produzindo EXATAMENTE a nota emitida (nada limpo ainda).
R_IGUAL = _ResultadoFalso("100000.00", "2000.00", "0", {"IR": "1200.00"})


@pytest.fixture
def cliente(monkeypatch):
    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    return app_real.test_client()


@pytest.fixture
def espiao(monkeypatch):
    """Substitui o miolo do /emissao/recuperar por dublês que só anotam a chamada."""
    import app.apps.emissaonf.web as web

    chamadas = {"preparar": 0, "concluir": 0, "completar": 0, "efeitos": []}
    ctx = {"cred": {"PIPEFY_TOKEN": "t"}, "card": {"card_id": CARD}, "gc": None,
           "r": R_IGUAL}

    def preparar(card_id, **kw):
        chamadas["preparar"] += 1
        return ctx

    def concluir(*a, **kw):
        chamadas["concluir"] += 1

    def completar(*a, **kw):
        chamadas["completar"] += 1

    def efeitos(c, nota_sub, numero, buf):
        chamadas["efeitos"].append((nota_sub, numero))
        return {"numero_antigo": nota_sub, "card": "slot B marcado",
                "planilha": "linha 3 marcada", "omie": "nº trocado",
                "ok_card": True, "ok_planilha": True, "ok_omie": True}

    monkeypatch.setattr(web._worker, "preparar", preparar)
    monkeypatch.setattr(web._concluir, "concluir", concluir)
    monkeypatch.setattr(web._compl, "completar", completar)
    monkeypatch.setattr(web, "_efeitos_substituicao", efeitos)
    return web, chamadas


def _recuperar(cliente, **extra):
    dados = {"token": TOKEN, "card_id": CARD, "xml": XML_NACIONAL}
    dados.update(extra)
    r = cliente.post("/emissao/recuperar", data=dados)
    return r.get_data(as_text=True)


# --------------------------------------------------------------------------- #
# [2] O cancelamento NÃO depende mais da caixa "concluir completo"
# --------------------------------------------------------------------------- #
def test_nota_substituida_sem_completo_ainda_cancela_a_antiga(cliente, espiao):
    """O defeito nº 2: sem a marca, o número era lido e ignorado em silêncio."""
    _web, ch = espiao
    corpo = _recuperar(cliente, nota_substituida=CURTO)
    assert ch["efeitos"] == [(CURTO, "2600000003399")]
    assert ch["concluir"] == 0 and ch["completar"] == 1
    assert "cancelada nos três lugares" in corpo


def test_nota_substituida_com_completo_cancela_a_antiga(cliente, espiao):
    _web, ch = espiao
    _recuperar(cliente, nota_substituida=CURTO, completo="on")
    assert ch["efeitos"] == [(CURTO, "2600000003399")]
    assert ch["concluir"] == 1 and ch["completar"] == 0


def test_sem_nota_substituida_nada_e_cancelado(cliente, espiao):
    """Recuperação comum (sem substituição) não pode mexer em nota nenhuma."""
    _web, ch = espiao
    corpo = _recuperar(cliente)
    assert ch["efeitos"] == []
    assert "Substituição" not in corpo


# --------------------------------------------------------------------------- #
# [3] Erro no bookkeeping da nota nova NÃO leva o cancelamento embora
# --------------------------------------------------------------------------- #
def test_erro_no_concluir_nao_impede_o_cancelamento_da_antiga(cliente, espiao, monkeypatch):
    """O defeito nº 3: os dois estavam no mesmo try."""
    web, ch = espiao

    def explode(*a, **kw):
        raise RuntimeError("Omie fora do ar")

    monkeypatch.setattr(web._concluir, "concluir", explode)
    corpo = _recuperar(cliente, nota_substituida=LONGO, completo="on")
    assert ch["efeitos"] == [(LONGO, "2600000003399")], \
        "o cancelamento da nota antiga tem de rodar mesmo com o concluir quebrado"
    assert "ERRO no concluir completo" in corpo
    assert "Omie fora do ar" in corpo


def test_erro_na_entrega_nao_impede_o_cancelamento_da_antiga(cliente, espiao, monkeypatch):
    web, ch = espiao

    def explode(*a, **kw):
        raise RuntimeError("Drive fora do ar")

    monkeypatch.setattr(web._compl, "completar", explode)
    _recuperar(cliente, nota_substituida=LONGO)
    assert ch["efeitos"] == [(LONGO, "2600000003399")]


def test_card_que_nao_carrega_avisa_que_o_cancelamento_nao_foi_tentado(cliente, espiao, monkeypatch):
    web, ch = espiao

    def explode(*a, **kw):
        raise RuntimeError("card sem obra na C. Diários")

    monkeypatch.setattr(web._worker, "preparar", explode)
    corpo = _recuperar(cliente, nota_substituida=LONGO, completo="on")
    assert ch["efeitos"] == []
    assert ch["concluir"] == 0, "sem o card não há como conferir os valores — não grava"
    assert "NÃO foi cancelada" in corpo
    assert "não tentado" in corpo


# --------------------------------------------------------------------------- #
# [4] O resultado do cancelamento aparece em DESTAQUE, não no meio do log
# --------------------------------------------------------------------------- #
def test_quando_um_dos_tres_passos_falha_a_tela_grita(cliente, espiao, monkeypatch):
    web, _ch = espiao

    def efeitos_meia_boca(c, nota_sub, numero, buf):
        return {"numero_antigo": nota_sub,
                "card": "⚠️ NÃO CANCELADO NO CARD: o nº não está em nenhum slot",
                "planilha": "linha 3 marcada", "omie": "nº trocado",
                "ok_card": False, "ok_planilha": True, "ok_omie": True}

    monkeypatch.setattr(web, "_efeitos_substituicao", efeitos_meia_boca)
    corpo = _recuperar(cliente, nota_substituida=CURTO, completo="on")
    assert "NÃO foi cancelada" in corpo
    assert "Slot do card (Pipefy)" in corpo
    assert "❌" in corpo and "✅" in corpo
    assert "precisa da sua mão" in corpo


def test_o_formulario_diz_que_os_dois_formatos_de_numero_servem(cliente):
    corpo = cliente.get(f"/emissao/recuperar?token={TOKEN}").get_data(as_text=True)
    assert "2600000003352" in corpo          # o exemplo do número longo
    assert "cancela a nota antiga" in corpo


# --------------------------------------------------------------------------- #
# A trava de valor: o card limpo não pode gravar retenção errada no Omie
#
# Por que: a conclusão limpa doze campos de entrada do card, entre eles o
# "Valor BDI diferenciado". Numa nota que JÁ concluiu — toda substituição manual
# é esse caso — o recálculo sai do card vazio. Na nota de 09/11/2026, de BDI
# R$ 1.313.502,08, isso daria R$ 72.242,62 de INSS e R$ 13.135,02 de ISS a mais,
# gravados em silêncio no título do Omie.
# --------------------------------------------------------------------------- #
def test_valores_batendo_com_o_xml_gravam_normalmente(cliente, espiao):
    _web, ch = espiao
    corpo = _recuperar(cliente, completo="on")
    assert ch["concluir"] == 1
    assert "NADA foi gravado" not in corpo


def test_card_com_bdi_limpo_nao_grava_e_explica(cliente, espiao, monkeypatch):
    web, ch = espiao
    # o card sem o BDI: o valor cai, e com ele ISS e INSS saem diferentes
    r_limpo = _ResultadoFalso("307397.42", "15135.02", "72242.62", {"IR": "3688.77"})

    def preparar(card_id, **kw):
        return {"cred": {"PIPEFY_TOKEN": "t"}, "card": {"card_id": CARD}, "gc": None,
                "r": r_limpo}

    monkeypatch.setattr(web._worker, "preparar", preparar)
    corpo = _recuperar(cliente, completo="on")
    assert ch["concluir"] == 0, "não pode gravar retenção diferente da que a nota destacou"
    assert "NADA foi gravado" in corpo
    assert "Valor BDI diferenciado" in corpo
    assert "Valor da nota" in corpo and "ISS" in corpo and "INSS" in corpo


def test_a_trava_de_valor_nao_impede_o_cancelamento_da_nota_antiga(cliente, espiao, monkeypatch):
    """A trava vale para o que se GRAVA da nota nova. Cancelar a antiga é outro
    assunto, não depende de valor nenhum, e tem de acontecer de todo jeito."""
    web, ch = espiao
    r_limpo = _ResultadoFalso("307397.42", "15135.02", "72242.62", {"IR": "3688.77"})

    def preparar(card_id, **kw):
        return {"cred": {"PIPEFY_TOKEN": "t"}, "card": {"card_id": CARD}, "gc": None,
                "r": r_limpo}

    monkeypatch.setattr(web._worker, "preparar", preparar)
    _recuperar(cliente, nota_substituida=CURTO, completo="on")
    assert ch["concluir"] == 0
    assert ch["efeitos"] == [(CURTO, "2600000003399")]


def test_gravar_mesmo_divergindo_passa_por_cima_da_trava(cliente, espiao, monkeypatch):
    """A decisão é do dono: ele lê a divergência e decide. Mas tem de ser
    explícito, e o log registra que não foi conferido."""
    web, ch = espiao
    r_limpo = _ResultadoFalso("307397.42", "15135.02", "72242.62", {"IR": "3688.77"})

    def preparar(card_id, **kw):
        return {"cred": {"PIPEFY_TOKEN": "t"}, "card": {"card_id": CARD}, "gc": None,
                "r": r_limpo}

    monkeypatch.setattr(web._worker, "preparar", preparar)
    corpo = _recuperar(cliente, completo="on", ignorar_divergencia="on")
    assert ch["concluir"] == 1
    assert "NÃO foram conferidos" in corpo


def test_a_entrega_simples_nao_passa_pela_trava(cliente, espiao, monkeypatch):
    """Sem "concluir completo" não se grava valor nenhum — só Drive/links. A trava
    ali atrapalharia sem proteger nada."""
    web, ch = espiao
    r_limpo = _ResultadoFalso("307397.42", "15135.02", "72242.62", {"IR": "3688.77"})

    def preparar(card_id, **kw):
        return {"cred": {"PIPEFY_TOKEN": "t"}, "card": {"card_id": CARD}, "gc": None,
                "r": r_limpo}

    monkeypatch.setattr(web._worker, "preparar", preparar)
    corpo = _recuperar(cliente)
    assert ch["completar"] == 1
    assert "NADA foi gravado" not in corpo


# --------------------------------------------------------------------------- #
# A leitura do XML que a trava usa: só o que foi RETIDO conta
# --------------------------------------------------------------------------- #
def test_tributo_nao_retido_nao_e_divergencia():
    """O modelo nacional só traz no XML o tributo retido (regra do E0699), e o
    motor fiscal só põe em federais_retidos o que retém. Comparar alíquota cheia
    daria divergência falsa em toda nota sem retenção federal."""
    import conferir_emitido as conf
    r = _ResultadoFalso("100000.00", "2000.00", "0", {"IR": "1200.00"})
    assert conf.divergencias(r, XML_NACIONAL) == []


def test_tributo_que_a_nota_retem_e_o_card_nao_e_divergencia():
    import conferir_emitido as conf
    r = _ResultadoFalso("100000.00", "2000.00", "0", {})
    (msg,) = conf.divergencias(r, XML_NACIONAL)
    assert "IR" in msg and "card não retém" in msg


def test_um_centavo_de_arredondamento_nao_e_divergencia():
    import conferir_emitido as conf
    r = _ResultadoFalso("100000.01", "2000.00", "0", {"IR": "1200.00"})
    assert conf.divergencias(r, XML_NACIONAL) == []


def test_o_modelo_antigo_abrasf_tambem_e_lido():
    import conferir_emitido as conf
    xml = ("<CompNfse><Nfse><InfNfse><Numero>3076</Numero>"
           "<ValoresNfse><ValorServicos>50000.00</ValorServicos>"
           "<ValorIss>1000.00</ValorIss><ValorInss>5500.00</ValorInss>"
           "</ValoresNfse></InfNfse></Nfse></CompNfse>")
    r = _ResultadoFalso("50000.00", "1000.00", "5500.00", {})
    assert conf.divergencias(r, xml) == []
    r_errado = _ResultadoFalso("50000.00", "1000.00", "0", {})
    (msg,) = conf.divergencias(r_errado, xml)
    assert "INSS" in msg
