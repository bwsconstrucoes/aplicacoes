# -*- coding: utf-8 -*-
"""Ponto — o AFD e o AEJ da Portaria 671 (06/11/2026), conferidos contra o leiaute oficial
(app/apps/ponto/docs/leiaute_*_portaria_671.txt): o tamanho de cada campo, o CRC-16/KERMIT do cabeçalho, a corrente do
SHA-256 das marcações, o trailer e a linha da assinatura; no AEJ, os registros e as contagens."""
from __future__ import annotations

import datetime as dt
import hashlib

import pytest
from sqlalchemy import text

from app.apps.ponto.core import arquivos_fiscais as af
from tests.test_ponto_gestao_banco import (CPF_JOAO, CPF_MARIA, _schema_ponto2, _segunda_passada, app,  # noqa: F401
                                           bater_via_chave, como, local, mundo)


def test_o_crc16_kermit_e_o_formato_da_data():
    assert af.crc16_kermit("123456789") == "2189"                  # o exemplo da própria Portaria
    assert len(af.dh(dt.datetime(2026, 11, 6, 15, 30, 42, tzinfo=dt.timezone.utc))) == 24
    assert af.dh(dt.datetime(2026, 11, 6, 15, 30, 42, tzinfo=dt.timezone.utc)) == "2026-11-06T12:30:00-0300"


@pytest.mark.banco
def test_o_afd_e_o_aej_de_uma_empresa(app, mundo, banco):
    seg = _segunda_passada()
    with banco.connect() as conn:
        eid = conn.execute(text("INSERT INTO empresas (razao_social, cnpj) VALUES ('BWS Construções Ltda', "
                                "'12.345.678/0001-95') RETURNING id")).scalar()
        conn.execute(text("UPDATE obras SET empresa_id = :e WHERE id = :o"), {"e": eid, "o": mundo["obra_a"]})
        conn.commit()
    for h in (7, 11, 12, 17):
        bater_via_chave(app, CPF_JOAO, local(seg, h, 0))
    bater_via_chave(app, CPF_JOAO, local(seg, 17, 3))                 # a repetida
    bater_via_chave(app, CPF_MARIA, local(seg, 7, 0), obra="PG-B")    # outra empresa (obra B sem empresa): fica fora
    dp = como(app, mundo["dp"])
    url = f"/erp/api/ponto/arquivos-fiscais/afd?empresa_id={eid}&de={seg}&ate={seg}"
    sem = dp.get(url)
    assert sem.status_code == 400 and "INPI" in sem.get_json()["erro"]            # o cabeçalho pede o registro
    cfg = {"inpi": "12345678901234567", "desenvolvedor_cnpj": "12345678000195",
           "desenvolvedor_nome": "BWS Construções Ltda", "desenvolvedor_email": "ti@bws.com.br"}
    assert como(app, mundo["sup"]).post("/erp/api/ponto/arquivos-fiscais", json=cfg).status_code == 403
    r = dp.post("/erp/api/ponto/arquivos-fiscais", json=cfg).get_json()
    assert r["configuracao"]["inpi"] == cfg["inpi"] and any(e["id"] == eid for e in r["empresas"])

    afd = dp.get(url)
    assert afd.status_code == 200, afd.get_data(as_text=True)[:300]
    assert 'filename="AFD1234567890123456712345678000195REP_P.txt"' in afd.headers["Content-Disposition"]
    bruto = afd.get_data()
    assert bruto.endswith(b"\r\n") and b"\r\n\r\n" not in bruto
    linhas = bruto.decode("latin-1").split("\r\n")[:-1]
    cab, *marcas, trailer, assinatura = linhas
    assert len(cab) == 302 and cab[:10] == "0000000001" and cab[10] == "1" and cab[11:25] == "12345678000195"
    assert cab[189:206] == "12345678901234567" and cab[206:216] == seg.isoformat() and cab[250:253] == "004"
    assert af.crc16_kermit(cab[:298]) == cab[298:]
    assert len(marcas) == 5 and all(len(m) == 137 and m[9] == "7" for m in marcas)
    assert all(m[34:46] == "0" + CPF_JOAO for m in marcas)
    anterior = ""
    for m in marcas:                                                    # a corrente do SHA-256
        assert hashlib.sha256((m[:73] + anterior).encode("latin-1")).hexdigest() == m[73:]
        anterior = m[73:]
    assert [int(m[:9]) for m in marcas] == sorted(int(m[:9]) for m in marcas)
    assert trailer == "999999999" + "0" * 45 + "000000005" + "9"
    assert assinatura == "ASSINATURA_DIGITAL_EM_ARQUIVO_P7S".ljust(100)

    aej = dp.get(url.replace("/afd?", "/aej?"))
    assert aej.status_code == 200, aej.get_data(as_text=True)[:300]
    reg = [l.split("|") for l in aej.get_data().decode("latin-1").split("\r\n")[:-1]]
    tipos = [r[0] for r in reg[:-1]]
    assert tipos[0] == "01" and tipos[1] == "02" and tipos[-2:] == ["08", "99"]
    assert reg[0][2] == "12345678000195" and reg[0][9] == "002" and reg[1] == ["02", "1", "3", "12345678901234567"]
    vinc = [r for r in reg if r[0] == "03"]
    assert vinc == [["03", "1", CPF_JOAO, "João Obra A"]]
    m05 = [r for r in reg if r[0] == "05"]
    validas = [r for r in m05 if r[4] in ("E", "S")]
    assert [r[4] for r in validas] == ["E", "S", "E", "S"] and [r[5] for r in validas] == ["001", "001", "002", "002"]
    assert validas[0][7].startswith("H") and all(r[6] == "O" for r in validas)
    assert any(r[4] == "D" and "repetida" in r[8] for r in m05)
    assert any(r[0] == "04" for r in reg)
    trailer = next(r for r in reg if r[0] == "99")
    assert trailer[1:] == ["1", "1", "1", str(sum(r[0] == "04" for r in reg)), str(len(m05)), "0",
                           str(sum(r[0] == "07" for r in reg)), "1"]
    assert reg[-1] == ["ASSINATURA_DIGITAL_EM_ARQUIVO_P7S".ljust(100)]
    assert como(app, mundo["sup"]).get(url).status_code == 403                    # é do DP
