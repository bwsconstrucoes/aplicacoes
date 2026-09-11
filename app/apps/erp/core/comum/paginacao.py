# ============================================================================
# ERP — core/comum/paginacao.py
# Uma página de cada vez, e o TOTAL de verdade.
#
# O QUE HAVIA ANTES, E POR QUE ERA PIOR DO QUE PARECE
#
# As listas do ERP paravam nos 500 registros mais novos — e não diziam. Três
# consequências, da menos para a mais grave:
#
#   1. o que era mais antigo ficava inalcançável sem filtrar;
#   2. a tela não avisava que estava escondendo alguma coisa;
#   3. e os NÚMEROS DO TOPO somavam só os 500 trazidos. Uma lista cortada é
#      um incômodo; um total que soma metade da base e se apresenta como
#      "total" é um número que MENTE — e ninguém confere um número que o
#      sistema deu.
#
# A REGRA DESTE MÓDULO: a consulta filtrada é montada UMA VEZ e serve às três
# perguntas — a página, a contagem e as somas. Elas não podem divergir porque
# não existem separadas. É o mesmo princípio do escopo de obra: um caminho só.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

logger = logging.getLogger(__name__)

# Quantos itens por página. Cabe numa tela de computador sem rolar até o fim
# do mundo, e é pequeno o bastante para a consulta voltar rápido.
TAMANHO = 200

# Teto de segurança por página. Existe para o dia em que alguém passar
# `tamanho=99999` na barra de endereço: a memória do serviço é dividida com
# treze módulos, e uma consulta gulosa derruba o vizinho.
TETO = 1000


def _limpar(valor: Any, padrao: int, minimo: int, maximo: int) -> int:
    try:
        n = int(valor)
    except (TypeError, ValueError):
        return padrao
    return max(minimo, min(n, maximo))


def paginar(s: Session, stmt: Select, *, pagina: Any = 1,
            tamanho: Any = TAMANHO) -> dict[str, Any]:
    """Devolve a página pedida e quantos existem NO TOTAL.

    O total sai de uma contagem sobre a MESMA consulta filtrada — é isso que
    permite a tela dizer "200 de 1.340" em vez de "200" e deixar a pessoa
    imaginando o resto.
    """
    pagina = _limpar(pagina, 1, 1, 100000)
    tamanho = _limpar(tamanho, TAMANHO, 1, TETO)

    # A contagem ignora ordenação e carregamentos: contar não precisa de nada
    # disso, e pedi-los faria o banco trabalhar à toa em cima de milhares de
    # linhas que ninguém vai ver.
    contagem = select(func.count()).select_from(
        stmt.order_by(None).options().subquery())
    total = int(s.scalar(contagem) or 0)

    itens = list(s.scalars(
        stmt.offset((pagina - 1) * tamanho).limit(tamanho)).all())
    mostrados_ate = (pagina - 1) * tamanho + len(itens)
    return {
        "itens": itens,
        "pagina": pagina,
        "tamanho": tamanho,
        "total": total,
        "paginas": max(1, (total + tamanho - 1) // tamanho),
        "tem_mais": mostrados_ate < total,
        "de": ((pagina - 1) * tamanho + 1) if itens else 0,
        "ate": mostrados_ate,
        # A frase pronta, calculada aqui para todas as telas dizerem a mesma
        # coisa do mesmo jeito.
        "resumo": (f"{(pagina - 1) * tamanho + 1}–{mostrados_ate} de {total}"
                   if itens else "nenhum registro"),
    }


def como_dicionario(pagina: dict[str, Any], serializar) -> dict[str, Any]:
    """A mesma página, com os itens já convertidos para a tela."""
    saida = dict(pagina)
    saida["itens"] = [serializar(i) for i in pagina["itens"]]
    return saida
