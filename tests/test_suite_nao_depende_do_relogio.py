"""A suíte não pode quebrar só porque a rodada atravessou a meia-noite.

Aconteceu em 12/09/2026: seis testes falharam sem que nada no sistema tivesse
mudado. A rodada com banco de verdade dura sete minutos, começou dia 11 e
terminou dia 12. Os seis comparavam com um `HOJE = date.today()` escrito no
TOPO do arquivo — calculado UMA vez, quando o pytest importa o módulo, lá no
começo da rodada. O ERP pergunta as horas na hora de executar. Passada a
meia-noite, os dois discordavam em um dia.

Não era defeito do sistema. Mas importa: o GitHub Actions roda a cada envio,
inclusive de madrugada, e rodada vermelha sem causa real ensina a equipe a
ignorar rodada vermelha — e aí a vermelha de verdade passa batida.

Esta varredura é ESTRUTURAL: lê o código dos próprios testes.
"""
from __future__ import annotations

import pathlib
import re

TESTES = pathlib.Path(__file__).resolve().parent

# Calcular a data uma vez, no topo do módulo, é o que cria a discordância.
# Dentro de uma função está certo: ali a data é lida no momento do uso.
_NO_TOPO = re.compile(
    r"^[A-Z_]+ *= *(date\.today\(\)|datetime\.(now|today)\(\))", re.M)


def test_nenhum_teste_congela_a_data_no_topo_do_arquivo():
    culpados = []
    for arquivo in sorted(TESTES.glob("test_*.py")):
        if _NO_TOPO.search(arquivo.read_text(encoding="utf-8")):
            culpados.append(arquivo.name)
    assert not culpados, (
        "Estes testes calculam a data uma vez, quando o arquivo é importado — "
        "e passam a discordar do ERP se a rodada virar a meia-noite: "
        f"{', '.join(culpados)}. Use `hoje()` do conftest, que lê o relógio no "
        f"momento do uso.")


def test_o_hoje_do_conftest_le_o_relogio_a_cada_chamada():
    """Prova que `hoje()` não guarda o valor: trocando o relógio, ele muda."""
    import datetime as _dt

    import conftest

    class _Outro(_dt.date):
        @classmethod
        def today(cls):
            return _dt.date(1999, 12, 31)

    original = _dt.date
    try:
        _dt.date = _Outro
        assert conftest.hoje() == _dt.date(1999, 12, 31)
    finally:
        _dt.date = original
    assert conftest.hoje() != _dt.date(1999, 12, 31)
