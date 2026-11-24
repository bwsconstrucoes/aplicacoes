# -*- coding: utf-8 -*-
"""O DDA do Bradesco colado e cruzado com as SPs (24/11/2026) — `dda.py`."""
import datetime as dt
from decimal import Decimal as D
from pathlib import Path

import pytest

from tests.test_analisesps_telas import app  # noqa: F401 — a fixture

EXEMPLO = (Path(__file__).parent / "dados" / "dda_bradesco_exemplo.txt").read_text(encoding="utf-8")


# --- a leitura --------------------------------------------------------------
def test_le_cada_boleto_e_confere_com_o_total_do_banco():
    from app.apps.analisesps import dda
    r = dda.ler(EXEMPLO)
    assert len(r["boletos"]) == 6 and r["confere"] and r["nao_lidos"] == 0
    assert r["do_banco"] == {"boletos": 6, "total": D("14945.78")}
    um = r["boletos"][0]
    assert um["vencimento"] == dt.date(2026, 11, 24) and um["limite"] == dt.date(2026, 11, 24)
    assert um["pagador_cnpj"] == "00079526000370", "o DDA põe um zero a mais na frente"
    assert um["beneficiario_nome"] == "3 TABELIONATO DE PROTESTO DE REC"
    assert um["beneficiario_doc"] == "29640584000141"
    assert um["banco"] == "237 - BCO BRADESCO S.A." and um["valor"] == D("8761.20")
    assert um["numero_documento"] == "83354126" and len(um["barras"]) == 44
    assert um["linha_digitavel"] == "23792891079000007024467002824901116400000876120"


def test_os_casos_dificeis_da_leitura():
    from app.apps.analisesps import dda
    b = dda.ler(EXEMPLO)["boletos"]
    assert b[1]["limite"] is None, "sem a data limite, a leitura não se desloca"
    assert b[2]["beneficiario_final_nome"] == "Associacao Sotud Energia Renovaveis"
    assert b[2]["beneficiario_final_doc"] == "56932477000160"
    assert b[2]["numero_documento"] == "", "os asteriscos não são número"
    assert b[3]["valor"] == D("930.00"), "valor em branco no alto: vale o valor do documento"
    assert dda.filial(b[0]["pagador_cnpj"]) == "filial 0003"
    assert dda.filial(b[1]["pagador_cnpj"]) == "matriz"


def test_texto_sem_boleto_nao_guarda_nada():
    from app.apps.analisesps import dda
    assert dda.ler("qualquer coisa")["boletos"] == []
    with pytest.raises(dda.ErroDoDDA):
        dda.guardar(dda.ler("qualquer coisa"))


# --- o cruzamento -------------------------------------------------------------
def _b(**kw):
    base = {"barras": "", "linha_digitavel": "", "beneficiario_doc": "09722463000131",
            "beneficiario_final_doc": "", "valor": D("2098.65"),
            "vencimento": dt.date(2026, 11, 25), "numero_documento": "0770780 02"}
    base.update(kw)
    return base


def _s(**kw):
    base = {"id": "1", "credor": "TINTAS", "doc": "09722463000131", "valor": D("2098.65"),
            "vencimento": dt.date(2026, 11, 25), "barras": "", "nf": "", "status_pgt": "Pagar"}
    base.update(kw)
    return base


@pytest.mark.parametrize("sp,nota_minima,palavra", [
    (_s(barras="X" * 44, doc="1"), 100, "código de barras"),
    (_s(), 92, "CNPJ, valor e vencimento"),
    (_s(doc="09722463000210"), 92, "raiz"),
    (_s(vencimento=dt.date(2026, 12, 20), nf="770780"), 90, "nº do documento"),
    (_s(vencimento=dt.date(2026, 11, 28)), 82, "3 dia"),
    (_s(vencimento=dt.date(2027, 3, 1)), 72, "vencimento diferente"),
    (_s(doc="99999999000199"), 60, "título vendido"),
])
def test_a_nota_de_cada_par(sp, nota_minima, palavra):
    from app.apps.analisesps import dda
    n, porque = dda.nota(_b(barras="X" * 44), sp)
    assert n >= nota_minima and palavra in porque


def test_valor_diferente_e_credor_diferente_nao_casa():
    from app.apps.analisesps import dda
    assert dda.nota(_b(), _s(doc="99999999000199", valor=D("10")))[0] == 0


@pytest.fixture
def banco_dda(banco_analisesps):
    from app.apps.analisesps.db import conexao
    lido_ = None
    with conexao() as conn:
        for sp in [
            # o Tabelionato: lançado com a linha digitável do boleto
            ("1400000001", "3 TABELIONATO", "29.640.584/0001-41", "8761.20", "2026-11-24",
             "23792.89107 90000.070244 67002.824901 1 16400000876120", "", "Pagar"),
            # Tintas Iquine: dois lançamentos iguais; o nº da nota desempata
            ("1400000002", "TINTAS IQUINE", "09.722.463/0001-31", "2098.65", "2026-12-25",
             "", "770781", "Pagar"),
            ("1400000003", "TINTAS IQUINE", "09.722.463/0001-31", "2098.65", "2026-12-25",
             "", "770780", "Pagar"),
            # Storehost: já PAGA — e o boleto ainda no DDA
            ("1400000004", "STOREHOST", "49.003.392/0001-31", "859.56", "2026-11-24", "",
             "", "Pago"),
            # cancelada não conta
            ("1400000005", "REDEMAQUINAS", "06.094.934/0001-16", "930.00", "2026-11-25", "",
             "", "Cancelado"),
        ]:
            conn.execute("INSERT INTO analisesps.sps (id, credor, documento, valor_num, "
                         " vencimento_d, codigo_barras, nf, status_pgt) VALUES "
                         " (?, ?, ?, ?, ?, ?, ?, ?)", sp)
        conn.commit()
    return lido_


def test_cruzar_com_as_SPs_de_verdade(banco_dda):
    from app.apps.analisesps import dda
    dda.guardar(dda.ler(EXEMPLO), "MARCELO")
    lista = dda.listar(dda.filtros_da_tela({}))
    por = {b["beneficiario_nome"] + "|" + b["numero_documento"]: b for b in lista["boletos"]}
    tab = por["3 TABELIONATO DE PROTESTO DE REC|83354126"]
    assert tab["situacao"] == dda.LANCADO and tab["sp"]["id"] == "1400000001"
    assert tab["sp"]["porque"] == "mesmo código de barras"
    assert por["TINTAS IQUINE LTDA|0770780 02"]["sp"]["id"] == "1400000003"
    assert por["TINTAS IQUINE LTDA|0770781 02"]["sp"]["id"] == "1400000002"
    assert por["STOREHOST BRASIL|0157000411"]["situacao"] == dda.SP_JA_PAGA
    assert por["REDEMAQUINAS COMERCIO S M EQU|057092"]["situacao"] == dda.SEM_LANCAMENTO, \
        "SP cancelada não vale como lançamento"
    contagem = {s: q for s, _, q, _ in lista["contagem"]}
    assert contagem[dda.SEM_LANCAMENTO] == 2 and lista["quantos"] == 6
    assert lista["boletos"][0]["situacao"] == dda.SEM_LANCAMENTO, "o que pede ação vem primeiro"


def test_colar_de_novo_nao_duplica_e_o_vinculo_a_mao_vale(banco_dda):
    from app.apps.analisesps import dda
    primeira = dda.guardar(dda.ler(EXEMPLO), "MARCELO")
    segunda = dda.guardar(dda.ler(EXEMPLO), "MARCELO")
    assert primeira["novos"] == 6 and segunda["novos"] == 0 and segunda["ja_conhecidos"] == 6
    lista = dda.listar(dda.filtros_da_tela({}))
    rede = next(b for b in lista["boletos"] if b["beneficiario_nome"].startswith("REDEMAQ"))
    dda.vincular(rede["id"], "NENHUMA", "MARCELO")
    cora = next(b for b in lista["boletos"] if b["banco"].startswith("403"))
    dda.vincular(cora["id"], "1400000004", "MARCELO")
    por = {b["id"]: b for b in dda.listar(dda.filtros_da_tela({}))["boletos"]}
    assert por[rede["id"]]["situacao"] == dda.SEM_SP_CONFIRMADO
    assert por[cora["id"]]["sp"]["id"] == "1400000004" and "à mão" in por[cora["id"]]["sp"]["porque"]
    with pytest.raises(dda.ErroDoDDA):
        dda.vincular(cora["id"], "abc")


def test_a_tela_cola_e_mostra(banco_dda, app):  # noqa: F811
    from tests.test_analisesps_telas import _como_mestre
    cliente = _como_mestre(app)
    vazio = cliente.get("/analisesps/dda").get_data(as_text=True)
    assert "Nenhuma colagem ainda" in vazio
    r = cliente.post("/analisesps/dda/colar", data={"texto": EXEMPLO})
    from urllib.parse import unquote_plus
    assert r.status_code == 302 and "Confere com o total do banco" in unquote_plus(r.headers["Location"])
    html = cliente.get("/analisesps/dda").get_data(as_text=True)
    assert 'id="tabela-dda"' in html and "Sem lançamento" in html and "1400000001" in html
    assert "Confere com o total do banco" in html
