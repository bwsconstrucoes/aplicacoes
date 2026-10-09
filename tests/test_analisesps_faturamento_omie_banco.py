# -*- coding: utf-8 -*-
"""
FATURAMENTO › CONFERIR O TÍTULO NO OMIE — 09/10/2026, com banco de verdade.

O dono: *"a gente precisa poder fazer aquela consulta do título ao Omie, para
ver se, para compatibilizar"*. E, na mesma mensagem, as colunas escolhidas
("quero a mesma coisa de Solicitações") e os arquivos como ícones.

O Omie é dublado: o que se confere aqui é o que a tela FAZ com a resposta —
rateio entre as notas do título, divergência, gravação na base e no banco.
"""
import pytest

from tests.test_analisesps_faturamento_banco import Aba, OBRAS
from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco


def _bfat():
    from app.apps.analisesps import faturamento
    return faturamento._emissor()[0]


def linha_da_base(**campos):
    bfat = _bfat()
    d = dict.fromkeys(bfat.CAB, "")
    d.update(campos)
    return [d[c] for c in bfat.CAB]


class AbaBase(Aba):
    """A "Base Faturamento" com o que a conferência usa: ler, achar a linha
    pelo número e regravar em lote."""

    def __init__(self, valores):
        super().__init__(valores)
        self.gravacoes = []

    def col_values(self, coluna):
        return [linha[coluna - 1] if len(linha) >= coluna else "" for linha in self.valores]

    def batch_get(self, intervalos):
        saida = []
        for intervalo in intervalos:
            numero = int(intervalo.split(":")[0][1:])
            saida.append([self.valores[numero - 1]])
        return saida

    def batch_update(self, pedidos, value_input_option=None):
        for p in pedidos:
            numero = int(p["range"].split(":")[0][1:])
            self.valores[numero - 1] = p["values"][0]
            self.gravacoes.append(numero)


class Planilha:
    def __init__(self, aba):
        self.aba = aba

    def worksheet(self, nome):
        assert nome == "Base Faturamento"
        return self.aba


class OmieFalso:
    """Responde ao ConsultarContaReceber; conta as chamadas."""

    def __init__(self, titulos):
        self.titulos = titulos
        self.chamadas = []

    def _call(self, url, call, param):
        codigo = param["codigo_lancamento_integracao"]
        self.chamadas.append(codigo)
        return self.titulos[codigo]


@pytest.fixture
def base(app, monkeypatch):
    """Duas notas no mesmo título (INT-1), uma sozinha (INT-2) e uma sem título."""
    from app.apps.analisesps import faturamento, sincronizacao, tarefas
    bfat = _bfat()
    valores = [list(bfat.CAB),
               linha_da_base(nota_numero="3283", nota_sequencial="3283",
                             data_emissao="2026-10-08", obra_codigo="IFSPSAOJOSE",
                             valor_total="10000.00", pis="65.00", retem_pis="S",
                             omie_codigo_integracao="INT-1",
                             link_xml="https://drive/xml/3283",
                             link_nfse_nacional="https://drive/danfse/3283"),
               linha_da_base(nota_numero="3284", nota_sequencial="3284",
                             data_emissao="2026-10-09", obra_codigo="IFSPSAOJOSE",
                             valor_total="5000.00", pis="32.50", retem_pis="S",
                             omie_codigo_integracao="INT-1"),
               linha_da_base(nota_numero="3285", nota_sequencial="3285",
                             data_emissao="2026-10-09", obra_codigo="CREPEEXU",
                             valor_total="1000.00", omie_codigo_integracao="INT-2"),
               linha_da_base(nota_numero="3286", nota_sequencial="3286",
                             data_emissao="2026-10-09", obra_codigo="CREPEEXU",
                             valor_total="500.00")]
    aba = AbaBase(valores)
    abas = {(faturamento.PLANILHA_NOTAS, faturamento.ABA_BASE): aba,
            (faturamento.PLANILHA_OBRAS, "Centro de Custo"): Aba(OBRAS)}
    monkeypatch.setattr(sincronizacao, "_aba", lambda p, n: abas[(p, n)])
    monkeypatch.setattr(tarefas, "disparar", lambda *a, **k: {"ok": False})
    assert faturamento.carregar()["notas"] == 4
    return aba


TITULO_QUE_BATE = {"codigo_lancamento_integracao": "INT-1", "codigo_lancamento_omie": 99,
                   "valor_documento": 15000, "valor_pis": 97.50, "retem_pis": "S",
                   "numero_documento_fiscal": "3283/3284"}


def test_a_FICHA_confere_o_titulo_rateia_entre_as_notas_e_grava_na_base(base):
    from app.apps.analisesps import faturamento
    omie = OmieFalso({"INT-1": TITULO_QUE_BATE})
    r = faturamento.conferir_uma_no_omie("3284", cliente_omie=omie, planilha=Planilha(base))
    assert r["ok"] and r["notas"] == 2 and r["fora"] == [] and r["conferivel"], r
    assert omie.chamadas == ["INT-1"], "uma chamada ao Omie, para as duas notas do título"
    assert sorted(base.gravacoes) == [2, 3], "só as linhas daquele título são regravadas"
    # a tela já mostra, sem esperar a próxima carga
    n = faturamento.uma("3283")
    pis = {t["nome"]: t for t in n["tributos"]}["PIS"]
    assert str(pis["omie"]) == "65.00", "o PIS do título rateado pelo valor da nota"
    assert n["divergencia_tributos"] == "N" and n["omie_conferido_em"]
    assert n["omie_numero_documento"] == "3283/3284"


def test_a_FICHA_diz_o_que_nao_bate(base):
    from app.apps.analisesps import faturamento
    omie = OmieFalso({"INT-1": dict(TITULO_QUE_BATE, valor_pis=90)})
    r = faturamento.conferir_uma_no_omie("3283", cliente_omie=omie, planilha=Planilha(base))
    assert r["ok"] and r["fora"] == ["pis"]
    assert faturamento.uma("3283")["divergencia_tributos"] == "S"


def test_nota_SEM_TITULO_nao_consulta_nada(base):
    from app.apps.analisesps import faturamento
    omie = OmieFalso({})
    r = faturamento.conferir_uma_no_omie("3286", cliente_omie=omie, planilha=Planilha(base))
    assert not r["ok"] and "Protocolos" in r["erro"] and omie.chamadas == []


def test_a_CONFERENCIA_DE_TODAS_comeca_pelos_nunca_conferidos(base):
    from app.apps.analisesps import faturamento
    omie = OmieFalso({"INT-1": TITULO_QUE_BATE,
                      "INT-2": {"codigo_lancamento_integracao": "INT-2",
                                "valor_documento": 1000, "valor_iss": 50, "retem_iss": "S"}})
    r = faturamento.conferir_no_omie(cliente_omie=omie, planilha=Planilha(base))
    assert r["titulos"] == 2 and r["conferidos"] == 2 and r["sem_codigo"] == 1
    # INT-2 não tem tributo na nota: conferido, mas "falta equalizar" — não divergente
    assert r["sem_tributo"] == 1 and r["divergentes"] == 0
    assert faturamento.uma("3285")["omie_conferido_em"]


def test_a_TELA_escolhe_colunas_mostra_icones_e_o_selo_do_omie(app, base):
    from app.apps.analisesps import faturamento
    faturamento.conferir_uma_no_omie("3283", cliente_omie=OmieFalso({"INT-1": TITULO_QUE_BATE}),
                                     planilha=Planilha(base))
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        tela = cliente.get("/analisesps/faturamento?f=1&de=2026-01-01").get_data(as_text=True)
        assert "<th>Tomador</th>" in tela and ">bate<" in tela
        assert 'aria-label="XML"' in tela and 'aria-label="DANFSe (nacional)"' in tela
        assert ">XML</a>" not in tela, "os arquivos viram ícones"
        cliente.post("/analisesps/colunas", data={"tabela": "faturamento",
                                                  "coluna": ["valor", "omie"],
                                                  "voltar": "/analisesps/faturamento"})
        tela = cliente.get("/analisesps/faturamento?f=1&de=2026-01-01").get_data(as_text=True)
        ficha = cliente.get("/analisesps/faturamento/nota/3283").get_data(as_text=True)
    assert "<th>Tomador</th>" not in tela and '<th class="direita">Valor</th>' in tela
    assert "<th>Nota</th>" in tela, "a nota nunca sai: é a linha"
    assert 'name="tabela" value="faturamento"' in tela
    assert "Esconder a descrição" not in tela, "o atalho é da tabela de SPs"
    assert "Consultar título no Omie" in ficha and "INT-1" in ficha


def test_CONFERIR_TODAS_com_outra_tarefa_rodando_fica_na_fila(app, monkeypatch):
    from app.apps.analisesps import tarefas
    disparos = []
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": False}))
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = cliente.post("/analisesps/faturamento/omie", data={})
    assert "fila" in r.location and tarefas._pedido_pendente("faturamento_omie")
    monkeypatch.setattr(tarefas, "disparar", lambda modo, disparo="manual": (
        disparos.append(modo) or {"ok": True}))
    tarefas.encadear_comprovantes("colaboradores")
    assert disparos[-1] == "faturamento_omie"
    assert not tarefas._pedido_pendente("faturamento_omie")

