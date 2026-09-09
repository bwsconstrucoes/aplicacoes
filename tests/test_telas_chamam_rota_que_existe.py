# ============================================================================
# ERP — as telas só podem chamar endereço que EXISTE
#
# POR QUE ESTE ARQUIVO EXISTE
#
# A tela de Obras pedia a lista de contas bancárias em `/erp/api/contas`. Essa
# rota nunca existiu. Como a chamada estava dentro de um `try`, o erro sumia
# sem barulho: nada aparecia vermelho, nenhum aviso — apenas o campo "conta
# bancária" do cadastro da obra abria SEMPRE vazio, e quem cadastrava obra não
# tinha como escolher a conta. Ninguém descobriria isso lendo código.
#
# É o mesmo defeito de sempre, de outra forma: a tela pede uma coisa, o
# servidor não tem, e o silêncio é a resposta. Aqui a suíte lê cada endereço
# que as telas pedem e confere contra as rotas que o Flask realmente registrou.
#
# COMO LIDA COM ENDEREÇO MONTADO NA HORA. Quem decide se o endereço existe é
# o PRÓPRIO Flask, com a mesma tabela de rotas que atende em produção: o
# pedaço variável (`${id}`) vira um valor qualquer e o endereço é submetido ao
# casamento de rotas de verdade. Assim ninguém precisa reimplementar as regras
# do Flask aqui — e o teste não erra por conta própria. Query string
# (`?obra_id=3`) é cortada: ela não muda a rota.
# ============================================================================
from __future__ import annotations

import re
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
TELAS = sorted((RAIZ / "app/apps/erp/templates").glob("*.html"))

# Endereços que a tela pede de propósito fora do padrão de rota (download
# direto, página, arquivo). Cada um com o motivo escrito.
FORA_DA_CONFERENCIA = {
    "/erp/sair",          # link de logout, não é chamada de dados
}

# Dois jeitos de escrever o endereço na tela, e a diferença importa: dentro
# de crase o pedaço `${...}` pode conter aspas ("obra_id="), então só a crase
# fecha o texto. Com aspas simples ou duplas, é a própria aspa que fecha.
PEDIDOS = [
    re.compile(r"(?:api|fetch)\(\s*`(/erp[^`]*)`"),
    re.compile(r"(?:api|fetch)\(\s*[\"'](/erp[^\"']*)[\"']"),
]


def _endereco_testavel(caminho: str) -> str:
    """Deixa o endereço pronto para o casador de rotas do Flask."""
    # Onde o pedaço variável está diz o que ele é, e isso decide o que fazer:
    #
    #   .../locacoes/${id}      — vem depois de "/": é um PEDAÇO DO CAMINHO.
    #                             Vira "1"; qualquer valor prova que a rota
    #                             existe, e "1" serve para número e para texto.
    #   .../colaboradores${q}   — vem colado no nome: é FILTRO, montado na hora
    #                             ("?busca=x&obra_id=3"). Filtro não muda a
    #                             rota, então o endereço termina ali.
    caminho = re.sub(r"(?<=/)\$\{[^}]*\}", "1", caminho)
    caminho = re.split(r"\$\{", caminho)[0]
    return caminho.split("?")[0].split("#")[0] or "/"


@pytest.fixture(scope="module")
def casador():
    """O casador de rotas do próprio Flask, com o blueprint do ERP montado."""
    from flask import Flask
    from app.apps.erp import bp
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app.url_map.bind("bws.local")


def _existe(casador, endereco: str) -> bool:
    from werkzeug.exceptions import MethodNotAllowed, NotFound
    try:
        casador.match(endereco)
    except MethodNotAllowed:
        return True          # a rota existe, só não aceita GET — basta
    except NotFound:
        return False
    return True


@pytest.mark.parametrize("tela", TELAS, ids=lambda p: p.name)
def test_a_tela_so_pede_endereco_que_o_servidor_tem(tela: Path, casador) -> None:
    """Todo `/erp/...` que a tela chama tem de casar com uma rota registrada.

    Falhou? Ou a rota não foi criada, ou o endereço está escrito diferente do
    que o servidor registrou. Não adianta pôr a chamada dentro de um `try`:
    o campo continua vazio e ninguém vê o erro.
    """
    texto = tela.read_text(encoding="utf-8")
    faltando = []
    achados = {p for regex in PEDIDOS for p in regex.findall(texto)}
    for pedido in achados:
        if pedido in FORA_DA_CONFERENCIA:
            continue
        if not _existe(casador, _endereco_testavel(pedido)):
            faltando.append(f"{tela.name}: pede '{pedido}' e o servidor não "
                            f"tem essa rota")
    assert not faltando, "\n".join(sorted(faltando))
