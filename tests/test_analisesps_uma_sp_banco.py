"""O "Atualizar da planilha" da ficha — relê só a linha de UMA SP (03/11/2026).

O dono: *"às vezes atualizei uma única linha da planilha e queria que ela fosse
atualizada no sistema imediatamente sem precisar buscar pela atualização
completa."* Ver `sincronizacao.atualizar_uma_sp`.
"""

from __future__ import annotations

import pytest

from tests.test_analisesps_banco import (
    AbaFalsa, _linha_da_planilha, _status_no_banco, semear, sp,
)

pytestmark = pytest.mark.banco


class AbaQueGrava(AbaFalsa):
    """A aba falsa que também recebe o que a fila devolve à planilha."""

    def __init__(self, linhas):
        super().__init__(linhas)
        self.gravado = []

    def batch_update(self, atualizacoes, value_input_option=None):
        from app.apps.analisesps import colunas
        letras = {c.letra: c.idx for c in colunas._DEFS}
        for a in atualizacoes:
            import re
            letra, n = re.match(r"([A-Z]+)(\d+)", a["range"]).groups()
            self.linhas[int(n) - 1][letras[letra]] = a["values"][0][0]
            self.gravado.append(a["range"])


def _ligar(monkeypatch, linhas):
    from app.apps.analisesps import sincronizacao
    aba = AbaQueGrava([["cabeçalho"]] + linhas)
    monkeypatch.setattr(sincronizacao, "_aba_sps", lambda: aba)
    return aba


def test_traz_so_a_linha_da_sp_pedida(banco_analisesps, monkeypatch):
    from app.apps.analisesps import colunas, sincronizacao
    semear([sp("1", status_pgt="Pagar"), sp("2", status_pgt="Pagar"),
            sp("3", status_pgt="Pagar")])
    aba = _ligar(monkeypatch, [
        _linha_da_planilha("1", "Pago", ""),
        _linha_da_planilha("2", "Pago", ""),
        _linha_da_planilha("3", "Pago", ""),
    ])

    r = sincronizacao.atualizar_uma_sp("2")

    assert r["achada"] and r["linha"] == 3
    assert r["mudou"] == [colunas.COLS["status_pgt"].rotulo]
    assert _status_no_banco("2") == "Pago"
    # As outras continuam como estavam: só a linha pedida foi lida.
    assert _status_no_banco("1") == "Pagar" and _status_no_banco("3") == "Pagar"
    assert aba.faixas_pedidas == [f"A3:{colunas.ULTIMA_LETRA}3"]


def test_linha_igual_diz_que_nada_mudou(banco_analisesps, monkeypatch):
    from app.apps.analisesps import sincronizacao
    semear([sp("9", status_pgt="Pagar")])
    from app.apps.analisesps.db import consultar_um
    from app.apps.analisesps import colunas
    campos = [c.chave for c in colunas.GUARDADAS]
    guardado = consultar_um("SELECT " + ", ".join(f'"{c}"' for c in campos)
                            + " FROM analisesps.sps WHERE id = '9'")
    linha = [""] * len(colunas._DEFS)
    for c, v in zip(colunas.GUARDADAS, guardado):
        linha[c.idx] = "" if v is None else str(v)
    _ligar(monkeypatch, [linha])

    assert sincronizacao.atualizar_uma_sp("9")["mudou"] == []


def test_sp_que_nao_esta_na_planilha_nao_e_apagada(banco_analisesps, monkeypatch):
    """Excluir da base é trabalho da sincronização completa, que confere tudo."""
    from app.apps.analisesps import sincronizacao
    semear([sp("5", status_pgt="Pagar")])
    _ligar(monkeypatch, [_linha_da_planilha("6", "Pago", "")])

    r = sincronizacao.atualizar_uma_sp("5")

    assert r["achada"] is False
    assert _status_no_banco("5") == "Pagar"


def test_a_alteracao_pendente_na_fila_vai_antes_de_ler(banco_analisesps, monkeypatch):
    """⚠️ O RISCO DESTE BOTÃO: marcou "Pago" pela tela, a alteração ainda está
    na fila, e a planilha ainda diz "Pagar". Ler primeiro traria o "Pagar" de
    volta e apagaria a marcação."""
    from app.apps.analisesps import sincronizacao
    from app.apps.analisesps.db import conexao
    semear([sp("7", status_pgt="Pago"), sp("8", status_pgt="Pagar")])
    with conexao() as conn:
        conn.execute("INSERT INTO analisesps.fila (sp_id, coluna, valor) "
                     "VALUES ('7', 'status_pgt', 'Pago'), ('8', 'status_pgt', 'Pago')")
        conn.commit()
    aba = _ligar(monkeypatch, [_linha_da_planilha("7", "Pagar", ""),
                               _linha_da_planilha("8", "Pagar", "")])

    r = sincronizacao.atualizar_uma_sp("7")

    assert r["devolvidas"] == 1
    assert _status_no_banco("7") == "Pago", "a marcação feita pela tela se perdeu"
    # A fila da OUTRA SP fica para a sincronização completa.
    from app.apps.analisesps.db import consultar
    assert [l[0] for l in consultar("SELECT sp_id FROM analisesps.fila")] == ["8"]
    assert len(aba.gravado) == 1

