# -*- coding: utf-8 -*-
"""A mensageria com banco de verdade: a migração 083 e o que o dublê não alcança
(contagem no registro, teto, política gravada, seções dos perfis)."""
from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import text

from app.apps.mensageria import core

pytestmark = pytest.mark.banco


@pytest.fixture
def conn(banco):
    with banco.connect() as c:
        trans = c.begin()
        c.execute(text("DELETE FROM mensageria.envios"))
        c.execute(text("DELETE FROM mensageria.parametros"))
        c.execute(text("DELETE FROM mensageria.tipos"))
        yield c
        trans.rollback()


def test_a_migracao_083_criou_o_schema_e_as_secoes(conn):
    tabelas = {r[0] for r in conn.execute(text(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mensageria'"))}
    assert {"tipos", "envios", "parametros"} <= tabelas
    linhas = conn.execute(text("""SELECT p.nome, ps.nivel FROM perfil_secoes ps JOIN perfis p ON p.id = ps.perfil_id
                                   WHERE ps.secao = 'adm_mensagens' ORDER BY p.nome""")).all()
    assert dict(linhas) == {"Administrador": "EDITAR", "Departamento pessoal": "LER",
                            "Diretor financeiro": "LER"}


def test_o_catalogo_e_semeado_e_a_politica_da_tela_prevalece(conn):
    tipos = {t["chave"]: t for t in core.listar_tipos(conn)}
    assert tipos["ponto.qr"]["politica"] == core.WHATSAPP
    assert tipos["erp.titulo_pago"]["politica"] == core.TELEGRAM
    core.gravar_politica(conn, "ponto.qr", core.TELEGRAM, "Marcelo")
    # Semear de novo não desfaz o que a tela decidiu
    tipos = {t["chave"]: t for t in core.listar_tipos(conn)}
    assert tipos["ponto.qr"]["politica"] == core.TELEGRAM and tipos["ponto.qr"]["atualizado_por"] == "Marcelo"
    with pytest.raises(ValueError):
        core.gravar_politica(conn, "ponto.qr", "FAX")
    with pytest.raises(ValueError):
        core.gravar_politica(conn, "nao.existe", core.TELEGRAM)


def test_decidir_com_a_chave_geral_do_whatsapp(conn):
    assert core.decidir(conn, "ponto.qr").canais == ()           # WhatsApp nasce desligado
    core.gravar_parametro(conn, core.WHATSAPP_LIGADO, "1", "Marcelo")
    assert core.decidir(conn, "ponto.qr").canais == ("whatsapp",)
    assert core.decidir(conn, "erp.titulo_pago").canais == ("telegram",)
    assert core.decidir(conn, "tipo.inventado").motivo == "tipo fora do catálogo"
    assert core.whatsapp_permitido.__doc__  # existe para o ponto


def test_o_teto_conta_so_whatsapp_enviado(conn):
    core.gravar_parametro(conn, core.POR_HORA, "2", "x")
    core.gravar_parametro(conn, core.POR_DIA, "3", "x")
    for status in ("ENVIADO", "FALHOU", "LIMITE"):
        core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5585", cpf="", texto="a",
                       nome_arquivo="", status=status, detalhe="")
    core.registrar(conn, tipo="erp.titulo_pago", canal="telegram", destinatario="1", cpf="", texto="a",
                   nome_arquivo="", status="ENVIADO", detalhe="")
    assert core.contagem_whatsapp(conn) == {"hora": 1, "dia": 1}
    assert core.cabe_no_teto(conn) == (True, "")
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5585", cpf="", texto="a",
                   nome_arquivo="", status="ENVIADO", detalhe="")
    cabe, motivo = core.cabe_no_teto(conn)
    assert cabe is False and "por hora" in motivo
    assert "QR Code do ponto (2 hoje)" == core.tipo_que_mais_consome(conn, dt.datetime.now(dt.timezone.utc))


def test_registro_lista_filtra_e_apaga_o_velho(conn):
    core.registrar(conn, tipo="ponto.qr", canal="whatsapp", destinatario="5585999990000", cpf="12345678901",
                   texto="seu QR", nome_arquivo="qr.jpg", status="ENVIADO", detalhe="", origem="ponto")
    core.registrar(conn, tipo="erp.titulo_pago", canal="telegram", destinatario="777", cpf="", texto="pago",
                   nome_arquivo="", status="SEM_DESTINO", detalhe="sem TelegramID")
    assert len(core.listar_envios(conn)) == 2
    assert [l["tipo"] for l in core.listar_envios(conn, canal="telegram")] == ["erp.titulo_pago"]
    assert [l["tipo"] for l in core.listar_envios(conn, busca="QR")] == ["ponto.qr"]
    assert [l["tipo"] for l in core.listar_envios(conn, status="SEM_DESTINO")] == ["erp.titulo_pago"]
    conn.execute(text("UPDATE mensageria.envios SET criado_em = now() - interval '91 days' WHERE tipo = 'ponto.qr'"))
    assert core.limpar_antigos(conn) == 1
    assert [l["tipo"] for l in core.listar_envios(conn, dias=90)] == ["erp.titulo_pago"]


def test_aviso_de_teto_sai_uma_vez_por_hora(conn):
    agora = dt.datetime.now(dt.timezone.utc)
    assert core._deve_avisar_limite(conn, agora) is True
    assert core._deve_avisar_limite(conn, agora + dt.timedelta(minutes=30)) is False
    assert core._deve_avisar_limite(conn, agora + dt.timedelta(minutes=61)) is True
