# -*- coding: utf-8 -*-
"""
CONSULTAR NO OMIE E EQUALIZAR A SPsBD — 08/10/2026, com banco de verdade.

O dono: *"seleciono registros, clico em consultar Omie, modal com status de
pagamento; se pago, poder selecionar e clicar em Marcar Pago para equalizar os
dados após leitura do card (…) se o card não estiver na fase 'Pago / Alimentar
Omie', fazer esse movimento também."* O Omie e o Pipefy são dublados.
"""
import pytest

from tests.test_analisesps_banco import semear, sp
from tests.test_analisesps_usuarios_banco import (  # noqa: F401 — fixtures
    SENHA_MESTRE_OPERADOR, app, banco_acesso)

pytestmark = pytest.mark.banco


class OmieFalso:
    def __init__(self, status):
        self.status, self.pedidos = status, []

    def _call(self, url, call, param):
        codigo = param["codigo_lancamento_integracao"]
        self.pedidos.append((call, codigo))
        if codigo not in self.status:
            raise RuntimeError("ERROR: Lançamento não encontrado")
        return {"status_titulo": self.status[codigo], "valor_pago": 100}


@pytest.fixture
def cena(app, monkeypatch):
    from app.apps.analisesps import pagamento_omie, pipefy, tarefas, web
    web._CONSULTADAS.clear()
    pagamento_omie._GUARDADO.clear()
    semear([
        sp("1000000001", credor="PAGO SEM BAIXA", valor="100,00", status_pgt="Pagar"),
        sp("1000000002", credor="PAGO COMPLETO", valor="50,00", status_pgt="Pago",
           data_pagamento="01/10/2026", comprovante="https://ja", codigo_integracao="IntX2"),
        sp("1000000003", credor="EM ABERTO", valor="70,00", status_pgt="Pagar"),
        sp("1000000004", credor="SEM TITULO", valor="10,00", status_pgt="Pagar"),
    ])
    omie = OmieFalso({"Int1000000001": "PAGO", "IntX2": "PAGO",
                      "Int1000000003": "A VENCER"})
    monkeypatch.setattr(pagamento_omie, "_cliente", lambda: omie)
    monkeypatch.setattr(tarefas, "disparar", lambda *a, **k: {"ok": True})
    movidos = []
    monkeypatch.setattr(pipefy, "ler_pagamentos", lambda ids, token=None: {
        "1000000001": {"fase_id": "999", "fase": "Pagar", "data": "2026-10-07",
                       "comprovante": "https://comprovante/1", "banco": "Bradesco - 50024-0"},
        "1000000002": {"fase_id": pipefy.FASE_PAGO_ALIMENTAR_OMIE,
                       "fase": "Pago / Alimentar Omie", "data": "", "comprovante": "",
                       "banco": ""},
    })
    monkeypatch.setattr(pipefy, "mover_cards", lambda ids, fase, token=None: (
        movidos.extend(ids) or {"movidos": list(ids), "erros": {}}))
    cliente = app.test_client()
    cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
    return cliente, omie, movidos


def _sp(id_, *campos):
    from app.apps.analisesps.db import consultar_um
    return consultar_um(f"SELECT {', '.join(campos)} FROM analisesps.sps WHERE id = ?", (id_,))


def _fila(id_):
    from app.apps.analisesps.db import consultar
    return dict(consultar("SELECT coluna, valor FROM analisesps.fila WHERE sp_id = ?", (id_,)))


def test_a_CONSULTA_mostra_o_status_do_Omie_e_o_que_falta_na_planilha(cena):
    cliente, omie, _ = cena
    r = cliente.post("/analisesps/api/omie/consultar", json={
        "ids": ["1000000001", "1000000002", "1000000003", "1000000004"]}).get_json()
    por_id = {l["id"]: l for l in r["linhas"]}
    assert por_id["1000000001"]["pago"] and por_id["1000000001"]["equalizar"]
    assert por_id["1000000001"]["falta"] == ["status", "data", "comprovante"]
    assert por_id["1000000002"]["pago"] and not por_id["1000000002"]["equalizar"]
    assert por_id["1000000003"]["status_omie"] == "A VENCER" and not por_id["1000000003"]["pago"]
    assert "não encontrado" in por_id["1000000004"]["erro"]
    # código da coluna P quando existe; "Int" + SP quando não
    assert ("ConsultarContaPagar", "IntX2") in omie.pedidos
    assert _fila("1000000001") == {}, "consultar não grava nada"


def test_MARCAR_PAGO_equaliza_status_data_comprovante_conta_e_move_o_card(cena):
    cliente, _, movidos = cena
    cliente.post("/analisesps/api/omie/consultar", json={"ids": ["1000000001", "1000000002"]})
    r = cliente.post("/analisesps/api/omie/marcar-pago",
                     json={"ids": ["1000000001", "1000000002"]}).get_json()
    assert r["ok"], r
    import datetime as dt
    assert _sp("1000000001", "status_pgt", "data_pagamento", "data_pagamento_d",
               "comprovante") == ("Pago", "07/10/2026", dt.date(2026, 10, 7),
                                  "https://comprovante/1")
    assert _fila("1000000001") == {"status_pgt": "Pago", "data_pagamento": "07/10/2026",
                                   "comprovante": "https://comprovante/1", "_ak": "50024-0"}
    # o card sem data/comprovante NÃO apaga o que a planilha já tinha
    assert _sp("1000000002", "data_pagamento", "comprovante") == ("01/10/2026", "https://ja")
    assert movidos == ["1000000001"], "só o que não está em Pago / Alimentar Omie"
    sps = r["complemento"]["sps"]
    assert sps["1000000001"]["movido"] and sps["1000000001"]["gravou"] == ["data", "comprovante", "conta"]
    assert sps["1000000002"]["faltou"] == ["data", "comprovante", "conta"]


def test_so_marca_pelo_modal_o_que_o_Omie_disse_PAGO(cena):
    cliente, _, movidos = cena
    r = cliente.post("/analisesps/api/omie/marcar-pago", json={"ids": ["1000000001"]})
    assert r.status_code == 400 and "Consulte de novo" in r.get_json()["erro"]
    cliente.post("/analisesps/api/omie/consultar", json={"ids": ["1000000003"]})
    r = cliente.post("/analisesps/api/omie/marcar-pago", json={"ids": ["1000000003"]})
    assert r.status_code == 400
    assert _sp("1000000003", "status_pgt") == ("Pagar",) and movidos == []


def test_o_MARCAR_PAGO_de_sempre_tambem_grava_data_e_comprovante_mas_nao_move(cena):
    cliente, _, movidos = cena
    r = cliente.post("/analisesps/api/alterar", json={
        "ids": ["1000000001"], "coluna": "status_pgt", "valor": "Pago",
        "acao": "Marcar Pago"}).get_json()
    assert r["ok"] and r["complemento"]["sps"]["1000000001"]["gravou"]
    assert _sp("1000000001", "data_pagamento", "comprovante") == (
        "07/10/2026", "https://comprovante/1")
    assert movidos == []


def test_PIPEFY_fora_nao_desfaz_o_Pago(cena, monkeypatch):
    from app.apps.analisesps import pipefy
    cliente, _, _ = cena

    def fora(*a, **k):
        raise pipefy.ErroDoPipefy("O token do Pipefy não está configurado")
    monkeypatch.setattr(pipefy, "ler_pagamentos", fora)
    r = cliente.post("/analisesps/api/alterar", json={
        "ids": ["1000000001"], "coluna": "status_pgt", "valor": "Pago"}).get_json()
    assert r["ok"] and "token do Pipefy" in r["complemento"]["erro"]
    assert _sp("1000000001", "status_pgt") == ("Pago",)


def test_a_coluna_AK_aparece_com_nome_no_log():
    from app.apps.analisesps import colunas
    assert colunas.COLS["_ak"].letra == "AK"
    assert colunas.ROTULOS["_ak"] == "Conta do Pagamento"


def test_quando_o_OMIE_PEDE_PAUSA_a_consulta_para_e_diz_quanto_esperar(cena, monkeypatch):
    """09/10/2026: *"tem que contornar essas mensagens: o Omie bloqueou as
    chamadas por consumo excessivo e pediu 60 segundos"*. A consulta para ali
    (insistir prolonga o bloqueio), devolve as que faltaram como pendentes e o
    tempo pedido — a janela continua sozinha."""
    from app.apps.analisesps import pagamento_omie
    from app.apps.painel.sync.omie_client import OmieBloqueada
    cliente, omie, _ = cena
    original = omie._call

    def bloqueia_no_segundo(url, call, param):
        if len(omie.pedidos) >= 1:
            omie.pedidos.append((call, "BLOQUEADO"))
            raise OmieBloqueada(60, "consumo excessivo")
        return original(url, call, param)
    monkeypatch.setattr(omie, "_call", bloqueia_no_segundo)
    r = cliente.post("/analisesps/api/omie/consultar", json={
        "ids": ["1000000001", "1000000003", "1000000002"]}).get_json()
    assert r["ok"] and r["espera"] == 60
    por_id = {l["id"]: l for l in r["linhas"]}
    assert por_id["1000000001"]["pago"] and not por_id["1000000001"]["pendente"]
    assert por_id["1000000003"]["pendente"] and por_id["1000000002"]["pendente"]
    assert not por_id["1000000003"]["erro"], "pausa não é erro"
    assert len(omie.pedidos) == 2, "parou no bloqueio — não insistiu no terceiro"

    # Depois da pausa, a janela pede SÓ as pendentes; a que já veio não é
    # perguntada de novo (o Omie bloqueia pergunta repetida).
    monkeypatch.setattr(omie, "_call", original)
    omie.pedidos.clear()
    r = cliente.post("/analisesps/api/omie/consultar", json={
        "ids": ["1000000003", "1000000002", "1000000001"]}).get_json()
    assert r["espera"] == 0 and not any(l["pendente"] for l in r["linhas"])
    assert ("ConsultarContaPagar", "Int1000000001") not in omie.pedidos


def test_AGENDAR_SP_com_chave_ATUALIZAR_e_recusado_no_servidor(app, monkeypatch):
    """09/10/2026: *"fazer esse bloqueio de impedir que ela seja colocada em
    agendar (…) e ter uma tag de atualizar a Pix na listagem"*."""
    from tests.test_analisesps_banco import semear, sp
    from app.apps.analisesps import tarefas
    monkeypatch.setattr(tarefas, "disparar", lambda *a, **k: {"ok": True})
    semear([sp("1000000101", forma_pagamento="BeeVale", info_pgt="Chave Pix: Atualizar Chave",
               status_pgt="Pagar", valor="10,00", vencimento="10/10/2026", validacao="Sim"),
            sp("1000000102", forma_pagamento="BeeVale", info_pgt="Chave Pix: 12345678901",
               status_pgt="Pagar", valor="10,00", vencimento="10/10/2026", validacao="Sim")])
    with app.test_client() as cliente:
        cliente.post("/analisesps/entrar", data={"senha": SENHA_MESTRE_OPERADOR})
        r = cliente.post("/analisesps/api/alterar", json={
            "ids": ["1000000101", "1000000102"], "coluna": "agendado", "valor": "Agendar"})
        assert r.status_code == 409 and "1000000101" in r.get_json()["erro"]
        assert "1000000102" not in r.get_json()["erro"]
        assert _sp("1000000102", "agendado") == ("",), "recusa o pedido inteiro"
        # sem a presa, agenda normalmente; e DESAGENDAR a presa continua livre
        assert cliente.post("/analisesps/api/alterar", json={
            "ids": ["1000000102"], "coluna": "agendado", "valor": "Agendar"}).get_json()["ok"]
        assert cliente.post("/analisesps/api/alterar", json={
            "ids": ["1000000101"], "coluna": "agendado", "valor": ""}).get_json()["ok"]
        lista = cliente.get("/analisesps/solicitacoes?f=1&busca=10000001",
                            follow_redirects=True).get_data(as_text=True)
        ficha = cliente.get("/analisesps/sp/1000000101").get_data(as_text=True)
    assert lista.count("pix-atualizar") >= 1 and "Atualizar Pix" in lista
    assert "Chave Pix a atualizar" in ficha and "data-motivo" in ficha
