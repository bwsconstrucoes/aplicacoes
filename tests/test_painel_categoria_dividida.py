# -*- coding: utf-8 -*-
"""
Título com DUAS categorias se divide entre elas — 09/10/2026.

O dono: títulos de empréstimo divididos no OMIE entre a devolução do principal
(fluxo) e os juros (DRE) "não estão aparecendo nada, nem a porção de juros nem
a do principal". O painel guardava uma categoria por título — a de maior valor
— e jogava o título inteiro nela: os juros nunca chegavam ao DRE.
"""
from __future__ import annotations

import pytest

from app.apps.painel.sync import espelho
from tests.test_painel_carga import (_movimento_do_omie, _titulo_do_omie,  # noqa: F401
                                     espelho_limpo)


def test_so_guarda_a_lista_quando_ha_duas_ou_mais():
    um = _titulo_do_omie(1, categorias=[{"codigo_categoria": "1.01", "percentual": 100}])
    assert espelho._linhas_categorias(um) == []
    dois = _titulo_do_omie(2, categorias=[
        {"codigo_categoria": "2.10.01", "percentual": 80, "valor": 800},
        {"codigo_categoria": "2.10.02", "percentual": 20, "valor": 200}])
    assert espelho._linhas_categorias(dois) == [
        (2, 1, "2.10.01", 80.0, 800.0), (2, 2, "2.10.02", 20.0, 200.0)]


@pytest.mark.banco
def test_o_emprestimo_aparece_dividido_entre_principal_e_juros(espelho_limpo):
    from app.apps.painel.db import conexao
    from app.apps.painel.sync import fato
    with conexao() as conn:
        conn.execute("TRUNCATE TABLE titulo_categorias")
        conn.execute("INSERT INTO cat (codigo, descricao, grupo, codigo_dre, transferencia)"
                     " VALUES ('2.10.01', 'Pagamento de Empréstimo', 'Financiamentos', '', 'N'),"
                     "        ('2.10.02', 'Juros de Empréstimo', 'Despesas Financeiras', '4.01', 'N')")
        titulo = _titulo_do_omie(
            90, valor=1000.0, natureza="P", codigo_categoria="",
            status_titulo="PAGO",
            categorias=[{"codigo_categoria": "2.10.01", "percentual": 80, "valor": 800},
                        {"codigo_categoria": "2.10.02", "percentual": 20, "valor": 200}])
        espelho.gravar_titulos(conn, [titulo], "P")
        mov = _movimento_do_omie(90, pago=1000.0)
        mov["detalhes"].update(cNatureza="P", cGrupo="CONTA_A_PAGAR", cStatus="PAGO")
        espelho.gravar_movimentos(conn, [mov])
        fato.reconstruir_fato(conn)
        linhas = conn.execute(
            "SELECT categoria, analise, pago_recebido::float8 FROM fato"
            " WHERE codigo_lancamento = 90 ORDER BY 3").fetchall()
        assert linhas == [("Pagamento de Empréstimo", "Fluxo de Caixa", -800.0),
                          ("Juros de Empréstimo", "DRE", -200.0)]
        # relido com uma categoria só, a divisão some
        espelho.gravar_titulos(conn, [_titulo_do_omie(90, valor=1000.0, natureza="P",
                                                      codigo_categoria="2.10.01",
                                                      status_titulo="PAGO")], "P")
        assert conn.execute("SELECT COUNT(*) FROM titulo_categorias").fetchone()[0] == 0
        conn.execute("TRUNCATE TABLE titulo_categorias")
        conn.commit()


@pytest.mark.banco
def test_reler_os_titulos_retoma_de_onde_parou(espelho_limpo):
    from app.apps.painel.db import conexao

    class Omie:
        def __init__(self, falhar_na=None):
            self.pedidos = []
            self.falhar_na = falhar_na

        def _paginas(self, natureza, pagina_inicial):
            for p in range(pagina_inicial, 4):
                self.pedidos.append((natureza, p))
                if (natureza, p) == self.falhar_na:
                    raise RuntimeError("caiu")
                yield p, 3, 3, [_titulo_do_omie(p * 10 + (natureza == "R"), natureza=natureza)]

        def listar_contas_pagar(self, pagina_inicial=1, **_k):
            return self._paginas("P", pagina_inicial)

        def listar_contas_receber(self, pagina_inicial=1, **_k):
            return self._paginas("R", pagina_inicial)

    with conexao() as conn:
        conn.execute("DELETE FROM config WHERE chave = ?", (espelho.CHAVE_RELEITURA_TITULOS,))
        conn.commit()
    primeira = Omie(falhar_na=("R", 2))
    with pytest.raises(RuntimeError):
        espelho.reler_titulos(cli=primeira)
    segunda = Omie()
    assert espelho.reler_titulos(cli=segunda) == {"contareceber": 3}
    # o a pagar já tinha terminado; o a receber volta UMA página antes
    assert segunda.pedidos == [("R", 1), ("R", 2), ("R", 3)]
    with conexao() as conn:
        assert conn.execute("SELECT 1 FROM config WHERE chave = ?",
                            (espelho.CHAVE_RELEITURA_TITULOS,)).fetchone() is None
