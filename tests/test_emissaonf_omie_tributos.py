# -*- coding: utf-8 -*-
"""
Conferir e equalizar os tributos do título no Omie.

As TRÊS operações que o dono quer manter do Apps Script da planilha (09/10/2026):
*"os scripts, eles apenas para consulta, equalização e atualização da parte de
tributos no Omie (…) a operação se limitará ao que eu disse e não mais a uma
série de outras funções que foram criadas"*.

O que estes testes vigiam, e cada um já custou dinheiro de alguém em algum
sistema:

1. **A direção da verdade.** A nota manda, o título obedece. Equalizar na
   direção errada mudaria o registro da nota para caber no título.
2. **O rateio fecha ao centavo.** Um título cobre várias notas; se a soma das
   partes não der o total exato, a conferência acusa divergência de um centavo em
   TODA nota — e alarme que sempre aparece deixa de ser lido.
3. **Imposto não retido não entra na soma.** Somar o que não foi retido infla a
   retenção do título e a baixa sai errada. É o problema que o dono descreve.
4. **Nota cancelada fica fora.** Era o que o Apps Script já fazia.
5. **Escrever no Omie exige confirmação marcada.** É sistema financeiro.
"""
import os
import sys
from decimal import Decimal

import pytest

_EMISSAONF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "app", "apps", "emissaonf")
if _EMISSAONF not in sys.path:
    sys.path.insert(0, _EMISSAONF)

import omie                      # noqa: E402
import omie_conferencia as oc    # noqa: E402

TOKEN = "TOKEN-DE-TESTE"


def _nota(numero, valor, **kw):
    d = {"nota_numero": numero, "valor_total": valor, "status": "valida",
         "omie_codigo_integracao": "PLG-A"}
    d.update(kw)
    return d


def _tres_notas():
    return [
        _nota("3271", "169.228,34", iss="5.076,85", retem_iss="S",
              ir="2.030,74", retem_ir="S", pis="1.099,98", retem_pis="N"),
        _nota("3272", "84.614,17", iss="2.538,43", retem_iss="S",
              ir="1.015,37", retem_ir="S", pis="549,99", retem_pis="N"),
        _nota("3273", "10.000,00", status="cancelada", iss="300,00", retem_iss="S"),
    ]


# --------------------------------------------------------------------------- #
# O rateio — a única parte do Apps Script que era regra de negócio
# --------------------------------------------------------------------------- #
def test_o_rateio_fecha_ao_centavo():
    partes = omie.ratear("1000.00", ["169228.34", "84614.17", "25152.69"])
    assert sum(partes) == Decimal("1000.00")


@pytest.mark.parametrize("total,pesos", [
    ("1.00", [1, 1, 1]),            # o clássico: 100 centavos entre 3
    ("0.01", [1, 1]),               # um centavo entre dois
    ("7615.28", [169228.34, 84614.17]),
    ("0.03", [1, 1, 1, 1, 1, 1, 1]),
])
def test_o_rateio_fecha_ao_centavo_em_qualquer_divisao(total, pesos):
    assert sum(omie.ratear(total, pesos)) == Decimal(total)


def test_o_residual_vai_para_a_nota_de_MAIOR_valor():
    """Regra do Apps Script, mantida: o centavo sobrando vai para a maior. Sem um
    critério fixo, duas rodadas com os mesmos dados dariam resultados diferentes."""
    partes = omie.ratear("1.00", [3, 1, 1])
    assert partes[0] > partes[1]
    assert sum(partes) == Decimal("1.00")


def test_rateio_sem_peso_nenhum_e_recusado_em_vez_de_dividir_igual():
    """Dividir igualmente seria inventar um critério que ninguém pediu."""
    with pytest.raises(ValueError, match="pesos"):
        omie.ratear("100.00", [0, 0])


def test_uma_nota_so_leva_o_titulo_inteiro():
    assert omie.ratear("7615.28", [169228.34]) == [Decimal("7615.28")]


# --------------------------------------------------------------------------- #
# A soma: o que o título DEVERIA ter
# --------------------------------------------------------------------------- #
def test_imposto_nao_retido_nao_entra_na_soma():
    """O PIS das duas notas existe como valor mas NÃO foi retido. Somá-lo
    inflaria a retenção do título e a baixa sairia errada."""
    soma = oc.somar_tributos(_tres_notas())
    assert soma["iss"] == Decimal("7615.28")      # 5.076,85 + 2.538,43
    assert soma["pis"] == Decimal("0")


def test_nota_cancelada_fica_fora_da_soma():
    notas = _tres_notas()
    assert "3273" not in [n["nota_numero"] for n in oc.notas_que_contam(notas)]
    assert oc.somar_tributos(notas)["iss"] == Decimal("7615.28")   # sem os 300,00


def test_nota_substituida_tambem_fica_fora():
    notas = [_nota("3271", "100,00", iss="3,00", retem_iss="S"),
             _nota("3272", "100,00", iss="3,00", retem_iss="S", status="substituida")]
    assert oc.somar_tributos(notas)["iss"] == Decimal("3.00")


def test_nota_sem_titulo_fica_de_fora_do_agrupamento():
    """Sem código de integração não há título para conferir — e inventar um
    seria pior que deixar a nota de fora."""
    notas = [_nota("3271", "100,00"), _nota("3272", "100,00", omie_codigo_integracao="")]
    grupos = oc.agrupar_por_titulo(notas)
    assert list(grupos) == ["PLG-A"] and len(grupos["PLG-A"]) == 1


# --------------------------------------------------------------------------- #
# A divergência: quais tributos não batem
# --------------------------------------------------------------------------- #
def test_a_divergencia_diz_QUAIS_tributos_e_nao_so_que_existe():
    """Gravar no Omie sem dizer o que vai mudar é o tipo de coisa que não se faz
    num sistema financeiro."""
    titulo = {"iss": Decimal("7000.00"), "ir": Decimal("3046.11")}
    soma = oc.somar_tributos(_tres_notas())
    assert oc.precisa_equalizar(titulo, soma) == ["iss"]


def test_diferenca_de_um_centavo_nao_conta_como_divergencia():
    titulo = {"iss": Decimal("7615.29")}
    assert oc.precisa_equalizar(titulo, {"iss": Decimal("7615.28")}) == []


def test_titulo_igual_a_soma_nao_precisa_de_nada():
    soma = oc.somar_tributos(_tres_notas())
    assert oc.precisa_equalizar(dict(soma), soma) == []


# --------------------------------------------------------------------------- #
# A leitura da resposta do Omie — números plausíveis que erram em silêncio
# --------------------------------------------------------------------------- #
def test_le_os_tributos_do_titulo_mesmo_aninhados():
    """A resposta do Omie aninha de formas diferentes conforme a chamada."""
    resposta = {"conta_receber_cadastro": {
        "codigo_lancamento_omie": 99887766,
        "codigo_lancamento_integracao": "PLG-A",
        "numero_documento_fiscal": "3271/3272",
        "valor_documento": 253842.51,
        "valor_iss": 7000.00, "retem_iss": "S",
        "valor_ir": 3046.11, "retem_ir": "S",
        "valor_pis": 0, "retem_pis": "N",
    }}
    t = omie.ler_titulo(resposta)
    assert t["codigo_lancamento"] == "99887766"
    assert t["numero_documento"] == "3271/3272"
    assert t["valor_titulo"] == Decimal("253842.51")
    assert t["iss"] == Decimal("7000") and t["retem_iss"] == "S"
    assert t["pis"] == Decimal("0") and t["retem_pis"] == "N"


def test_campo_ausente_no_omie_vira_zero_e_nao_estoura():
    t = omie.ler_titulo({"conta_receber_cadastro": {"valor_iss": 10}})
    assert t["iss"] == Decimal("10") and t["inss"] == Decimal("0")


# --------------------------------------------------------------------------- #
# A escrita: só os tributos, e nada além
# --------------------------------------------------------------------------- #
def test_a_atualizacao_NAO_leva_o_numero_do_documento():
    """O número da nota no título é assunto da emissão (ela acumula '3001/3072').
    Mandá-lo aqui faria a equalização sobrescrever esse acúmulo por tabela."""
    param = omie.montar_param_tributos("PLG-A", {"iss": Decimal("7615.28")})
    assert "numero_documento_fiscal" not in param
    assert param["codigo_lancamento_integracao"] == "PLG-A"
    assert param["valor_iss"] == 7615.28
    assert param["retem_iss"] == "S"


def test_tributo_zerado_vai_como_NAO_retido():
    param = omie.montar_param_tributos("PLG-A", {"pis": Decimal("0")})
    assert param["valor_pis"] == 0.0 and param["retem_pis"] == "N"


def test_tributo_que_nao_foi_passado_nao_entra_no_param():
    """Mandar um campo ausente como zero APAGARIA uma retenção legítima do
    título — e o dono só descobriria na baixa."""
    param = omie.montar_param_tributos("PLG-A", {"iss": Decimal("1")})
    assert not any(k.endswith("_inss") for k in param)


# --------------------------------------------------------------------------- #
# A tela: escrever no Omie exige confirmação
# --------------------------------------------------------------------------- #
@pytest.fixture
def cliente(monkeypatch):
    from app.main import app
    monkeypatch.setenv("EMISSAO_NF_TOKEN", TOKEN)
    return app.test_client()


def test_sem_token_a_tela_nao_abre(cliente):
    assert cliente.get("/emissao/omie").status_code == 403


def test_a_tela_explica_a_direcao_da_verdade(cliente):
    corpo = cliente.get(f"/emissao/omie?token={TOKEN}").get_data(as_text=True)
    assert "nota manda" in corpo
    assert "cancelada" in corpo
    assert "fechando ao" in corpo and "centavo" in corpo


def test_a_tela_so_escreve_no_omie_com_a_caixa_marcada(cliente, monkeypatch):
    from app.apps.emissaonf import web as servindo
    escritas = []
    monkeypatch.setattr(servindo.omie, "alterar_tributos",
                        lambda *a, **k: escritas.append(a))
    monkeypatch.setattr(servindo.omie, "consultar",
                        lambda cred, cod: {"valor_iss": 1.0})
    class _GC:
        def open_by_key(self, _k):
            return object()

    monkeypatch.setattr(servindo._worker, "cliente_gspread", lambda: _GC())
    monkeypatch.setattr(servindo._worker, "ler_credenciais", lambda gc: {})

    class _WS:
        def get_all_values(self):
            import base_faturamento as bf
            linha = [""] * len(bf.CAB)
            linha[bf.IDX["nota_numero"]] = "3271"
            linha[bf.IDX["valor_total"]] = "100,00"
            linha[bf.IDX["iss"]] = "3,00"
            linha[bf.IDX["retem_iss"]] = "S"
            linha[bf.IDX["omie_codigo_integracao"]] = "PLG-A"
            return [bf.CAB, linha]

        def batch_update(self, *a, **k):
            pass

    monkeypatch.setattr(servindo._bfat, "_ws", lambda planilha: _WS())

    sem = cliente.post("/emissao/omie", data={"token": TOKEN, "limite": "5"})
    assert escritas == [], "sem a caixa marcada, nada vai para o Omie"
    assert "SÓ CONFERIR" in sem.get_data(as_text=True)

    com = cliente.post("/emissao/omie", data={"token": TOKEN, "limite": "5",
                                              "confirmo_escrever": "on"})
    assert len(escritas) == 1, "com a caixa marcada, o título divergente é gravado"
    assert "escreve no Omie" in com.get_data(as_text=True)


# --------------------------------------------------------------------------- #
# A TRAVA que impede o pior estrago desta tela
#
# O emissor nunca gravou tributo nenhum até 09/10/2026, e os blocos de fórmula da
# planilha são de uma metodologia abandonada. Então as ~3.300 notas antigas
# chegam à base SEM tributo declarado. Sem esta trava, a soma daria zero, a
# equalização veria divergência em tudo e ZERARIA as retenções no Omie — que são
# a única cópia que existe delas.
# --------------------------------------------------------------------------- #
def test_nota_sem_tributo_declarado_nao_e_conferivel():
    antigas = [_nota("3271", "169.228,34"), _nota("3272", "84.614,17")]
    assert oc.tem_tributos_declarados(antigas) is False


def test_nota_nova_com_tributo_declarado_e_conferivel():
    assert oc.tem_tributos_declarados(_tres_notas()) is True


def test_uma_nota_declarando_ja_torna_o_titulo_conferivel():
    mistura = [_nota("3271", "100,00"),
               _nota("3272", "100,00", iss="3,00", retem_iss="S")]
    assert oc.tem_tributos_declarados(mistura) is True


def test_so_a_cancelada_declarando_NAO_torna_conferivel():
    """Cancelada fica fora de tudo — inclusive de decidir se há o que conferir."""
    so_cancelada = [_nota("3271", "100,00"),
                    _nota("3272", "100,00", iss="3,00", retem_iss="S",
                          status="cancelada")]
    assert oc.tem_tributos_declarados(so_cancelada) is False


def test_a_tela_NAO_equaliza_nota_antiga_mesmo_com_a_caixa_marcada(cliente, monkeypatch):
    """A trava vale mesmo com autorização: autorizar equalizar não é autorizar
    apagar a retenção que o Omie tem e a nota não tem."""
    from app.apps.emissaonf import web as servindo
    import base_faturamento as bf

    escritas = []
    monkeypatch.setattr(servindo.omie, "alterar_tributos",
                        lambda *a, **k: escritas.append(a))
    monkeypatch.setattr(servindo.omie, "consultar",
                        lambda cred, cod: {"valor_iss": 7000.0, "retem_iss": "S"})

    class _GC:
        def open_by_key(self, _k):
            return object()

    monkeypatch.setattr(servindo._worker, "cliente_gspread", lambda: _GC())
    monkeypatch.setattr(servindo._worker, "ler_credenciais", lambda gc: {})

    class _WS:
        def get_all_values(self):
            linha = [""] * len(bf.CAB)
            linha[bf.IDX["nota_numero"]] = "3271"
            linha[bf.IDX["valor_total"]] = "169.228,34"
            linha[bf.IDX["omie_codigo_integracao"]] = "PLG-A"
            # sem NENHUM tributo declarado — a nota antiga
            return [bf.CAB, linha]

        def batch_update(self, *a, **k):
            pass

    monkeypatch.setattr(servindo._bfat, "_ws", lambda planilha: _WS())

    r = cliente.post("/emissao/omie", data={"token": TOKEN, "limite": "5",
                                            "confirmo_escrever": "on"})
    corpo = r.get_data(as_text=True)
    assert escritas == [], "nota sem tributo declarado nunca vai para o Omie"
    assert "sem tributo na NOTA" in corpo
    assert "NÃO equalizável" in corpo


# --------------------------------------------------------------------------- #
# O campo numero_documento_fiscal e o limite de 20 caracteres
#
# Defeito real, visto em 09/10/2026 na nota 2600000003294:
#
#   "O número máximo de caracteres permitido para o elemento
#    [NUMERO_DOCUMENTO_FISCAL] é de 20. O número de caracteres informado foi
#    de 32!"
#
# Com os números de 4 dígitos do modelo antigo caberiam QUATRO notas
# ("3283/3294/3295/3296" = 19). Com os de 13 dígitos do padrão nacional, DUAS já
# não cabem — e o título ficou sem o número daquela nota.
# --------------------------------------------------------------------------- #
def test_duas_notas_nacionais_nao_cabiam_no_campo():
    """A prova do defeito, para ninguém "simplificar" a conversão de volta."""
    assert len("2600000003283/2600000003294") > omie.LIMITE_NUMERO_DOCUMENTO


def test_o_numero_vai_CURTO_para_o_omie():
    """Três motivos: cabe; é o número pelo qual o dono procura; e é o formato que
    os títulos antigos já têm — misturar faria o mesmo título ter dois jeitos de
    escrever nota."""
    assert omie.numero_curto("2600000003294") == "3294"
    assert omie.numero_curto("3270") == "3270"
    assert omie.numero_curto("") == ""


@pytest.mark.parametrize("atual,novo,esperado", [
    ("3270", "2600000003283", "3270/3283"),
    ("3270/3283", "2600000003294", "3270/3283/3294"),
    ("2600000003283", "2600000003294", "3283/3294"),      # o campo já longo é encurtado
    ("", "2600000003294", "3294"),
])
def test_o_campo_e_montado_curto_e_cabe(atual, novo, esperado):
    doc, descartados = omie.montar_numero_documento(atual, novo=novo)
    assert doc == esperado
    assert len(doc) <= omie.LIMITE_NUMERO_DOCUMENTO
    assert descartados == []


def test_numero_repetido_nao_duplica():
    doc, _ = omie.montar_numero_documento("3283/3294", novo="2600000003294")
    assert doc == "3283/3294"


def test_quando_nao_cabe_saem_os_MAIS_ANTIGOS_e_isso_e_avisado():
    """O card tem cinco slots, e cinco números de 4 dígitos passam de 20. Sai o
    mais antigo: a nota recém-emitida é a que alguém está procurando agora, e
    perder a nova em silêncio seria o pior dos dois."""
    doc, descartados = omie.montar_numero_documento("3283/3294/3295/3296",
                                                    novo="2600000003297")
    assert doc == "3294/3295/3296/3297"
    assert descartados == ["3283"], "o descartado é devolvido para virar aviso"
    assert len(doc) <= omie.LIMITE_NUMERO_DOCUMENTO


def test_cancelar_nota_remove_mesmo_informando_o_numero_longo():
    """O campo guarda o curto; quem cancela informa o número que tem na mão."""
    doc, _ = omie.montar_numero_documento("3283/3294", remover="2600000003294")
    assert doc == "3283"


def test_o_limite_vale_em_qualquer_combinacao():
    atual = "/".join(str(3290 + i) for i in range(8))
    doc, descartados = omie.montar_numero_documento(atual, novo="2600000003299")
    assert len(doc) <= omie.LIMITE_NUMERO_DOCUMENTO
    assert descartados, "com oito notas, alguma tem de sair — e ser avisada"


def test_a_tela_de_acertar_o_numero_nao_toca_nas_retencoes(cliente, monkeypatch):
    """A reparação mexe num campo só. Mandar retenção aqui sobrescreveria a
    equalização que já estava certa no título."""
    from app.apps.emissaonf import web as servindo
    import base_faturamento as bf

    enviados = []
    monkeypatch.setattr(servindo.omie, "_post",
                        lambda call, param, creds, **k: enviados.append((call, param)))
    monkeypatch.setattr(servindo.omie, "consultar",
                        lambda cred, cod: {"numero_documento_fiscal": "3283"})

    class _GC:
        def open_by_key(self, _k):
            return object()

    monkeypatch.setattr(servindo._worker, "cliente_gspread", lambda: _GC())
    monkeypatch.setattr(servindo._worker, "ler_credenciais", lambda gc: {})

    class _WS:
        def get_all_values(self):
            linha = [""] * len(bf.CAB)
            linha[bf.IDX["nota_numero"]] = "2600000003294"
            linha[bf.IDX["omie_codigo_integracao"]] = "PLG-A"
            return [bf.CAB, linha]

    monkeypatch.setattr(servindo._bfat, "_ws", lambda planilha: _WS())

    corpo = cliente.post("/emissao/omie_numero",
                         data={"token": TOKEN, "numero": "3294"}).get_data(as_text=True)
    assert len(enviados) == 1
    _call, param = enviados[0]
    assert param["numero_documento_fiscal"] == "3283/3294"
    assert not any(k.startswith("valor_") or k.startswith("retem_") for k in param), (
        "a reparação do número não pode levar retenção nenhuma")
    assert "retenções NÃO foram tocadas" in corpo


def test_acertar_numero_de_nota_que_nao_esta_na_base_avisa(cliente, monkeypatch):
    from app.apps.emissaonf import web as servindo
    import base_faturamento as bf

    class _GC:
        def open_by_key(self, _k):
            return object()

    monkeypatch.setattr(servindo._worker, "cliente_gspread", lambda: _GC())
    monkeypatch.setattr(servindo._worker, "ler_credenciais", lambda gc: {})
    monkeypatch.setattr(servindo._bfat, "_ws",
                        lambda planilha: type("W", (), {"get_all_values": lambda s: [bf.CAB]})())
    corpo = cliente.post("/emissao/omie_numero",
                         data={"token": TOKEN, "numero": "9999"}).get_data(as_text=True)
    assert "não está na Base Faturamento" in corpo
