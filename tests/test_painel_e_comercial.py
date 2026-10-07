# -*- coding: utf-8 -*-
"""07/10/2026, o dono: os credores com "&" saíam "A F &amp; CIA MINERACAO LTDA".
O OMIE manda o "&" escrito como "&amp;" em alguns nomes."""
from app.apps.painel.sync import espelho, fato


def test_o_e_comercial_volta_a_ser_e_comercial():
    assert espelho._s("A F &amp; CIA MINERACAO LTDA ") == "A F & CIA MINERACAO LTDA"
    assert espelho._s("A F & CIA") == "A F & CIA"
    assert espelho._s(None) == "" and espelho._s(123) == "123"
    # o que já está guardado também sai certo no recálculo
    assert fato._sem_entidade("A F &amp; CIA MINERACAO LTDA") == "A F & CIA MINERACAO LTDA"
    assert fato._sem_entidade(None) == ""


class _Conexao:
    def __init__(self, respostas):
        self.respostas = respostas

    def execute(self, sql, params=()):
        conn = self

        class Cursor:
            def fetchall(self_):
                for chave, linhas in conn.respostas.items():
                    if chave in sql:
                        return linhas
                return []

            def close(self_):
                pass
        return Cursor()


def test_o_credor_sai_certo_no_fato(monkeypatch):
    from app.apps.painel.sync import projetos
    monkeypatch.setattr(projetos, "projetos_da_tela", lambda conn: {})
    conn = _Conexao({"FROM clientes": [(1, "A F &amp; CIA MINERACAO LTDA", "1")],
                     "FROM cat": [("2.01", "Pedra &amp; areia", "Insumos", "1", "N", "")],
                     "FROM contas_correntes": [(9, "Banco &amp; Cia")]})
    cat, cli, _proj, ccorr = fato.carregar_catalogos(conn)
    assert cli[1][0] == "A F & CIA MINERACAO LTDA"
    assert cat["2.01"][0] == "Pedra & areia"
    assert ccorr[9] == "Banco & Cia"
