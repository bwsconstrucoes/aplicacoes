"""Falha inesperada não conta a intimidade do sistema para a tela.

O dono viu isso acontecer em 11/09/2026: perguntou uma coisa ao assistente e
recebeu de volta a lista de colunas de uma tabela do banco. O painel do
assistente foi corrigido na hora; a varredura mostrou que o mesmo saía por
OUTRAS 148 rotas, porque todas devolviam o texto cru da exceção.

Três problemas no mesmo lugar: quem lê não é programador; o texto cru conta
como o sistema é feito por dentro; e perde-se a informação de QUAL foi a
falha, que é o que serve para procurar no registro depois.

Este arquivo é uma varredura ESTRUTURAL: ela lê o próprio código das rotas.
Sem banco, sem subir aplicação.
"""
from __future__ import annotations

import pathlib
import re

RAIZ = pathlib.Path(__file__).resolve().parents[1]
ROTAS = RAIZ / "app" / "apps" / "erp" / "routes.py"


def test_nenhuma_rota_devolve_o_texto_cru_da_excecao():
    texto = ROTAS.read_text(encoding="utf-8")
    cruas = re.findall(r'"erro": str\(e\)\}\), 500', texto)
    assert not cruas, (
        f"{len(cruas)} rota(s) ainda devolvem o texto cru da exceção numa falha "
        f"inesperada. Use `recado_de_falha(e)`: ele fala português, não conta "
        f"nada de dentro do sistema e carrega o código que casa com o registro "
        f"do servidor.")


def test_o_recado_fala_portugues_e_nao_repete_o_erro_tecnico():
    from app.apps.erp.core.comum.formato import recado_de_falha

    recado = recado_de_falha(
        RuntimeError('psycopg2.OperationalError: could not translate host name '
                     '"db-prod-01.interno" to address\nLINE 1: SELECT ...'))
    assert "db-prod-01" not in recado
    assert "SELECT" not in recado
    assert "psycopg2" not in recado
    assert "Não consegui concluir" in recado


def test_banco_atrasado_continua_dizendo_o_que_fazer():
    """A ÚNICA falha explicada em detalhe, porque quem lê resolve sozinho.

    O código do ERP sobe para o Render ANTES de o botão do banco ser apertado.
    Nessa janela o Postgres responde "coluna não existe" — e esconder isso
    atrás de "falha do sistema" tiraria da pessoa a informação que resolve o
    problema em dez segundos. Foi o que derrubou o ERP em 02/09/2026.
    """
    from app.apps.erp.core.comum.formato import recado_de_falha

    recado = recado_de_falha(
        ValueError('column "usuarios.escopo_visao" does not exist'))
    assert "desatualizado" in recado
    assert "Aplicar atualiza" in recado
    assert "escopo_visao" not in recado


def test_o_codigo_e_estavel_para_a_mesma_falha():
    """Se a pessoa disser o código duas vezes, tem de ser o mesmo código —
    senão não serve para procurar no registro."""
    from app.apps.erp.core.comum.formato import recado_de_falha

    erro = KeyError("conta_bancaria_id")
    assert recado_de_falha(erro) == recado_de_falha(KeyError("conta_bancaria_id"))
    assert recado_de_falha(erro) != recado_de_falha(KeyError("outra_coisa"))


def test_o_recado_pode_dizer_o_que_estava_sendo_feito():
    from app.apps.erp.core.comum.formato import recado_de_falha

    assert "ao gerar o relatório" in recado_de_falha(
        RuntimeError("x"), acao="gerar o relatório")
