# -*- coding: utf-8 -*-
"""
Auxílio alimentação e auxílio transporte.

As duas verbas são quase a mesma conta, e a diferença é decisão do dono, de
27/09/2026:

    "O fato de se estar de férias e um feriado em dia de semana poderia sim afetar
     o cálculo do auxílio transporte. (…) Um único dia não precisaria, mas férias,
     como são mais dias, sim, deveríamos proporcionalizar."

| | Alimentação | Transporte |
|---|---|---|
| desconta **feriado** | sim | **não** |
| desconta **férias** | sim | sim |

⚠️ O TRANSPORTE NÃO DESCONTAVA NADA na planilha (§7.14.7): as colunas de feriado
existiam, eram calculadas e **não entravam na conta**. Ele decidiu que férias
passam a descontar. Isto aqui é a decisão dele, não o que a planilha fazia.

A CONTA, do jeito que a planilha faz (§7.14.6), por modalidade:

    Mês               valor fixo, não conta dia
    Mensal            todos os dias do mês − 3
    Segunda à Sexta   dias úteis (sábado e domingo fora)
    Segunda à Quinta  dias úteis menos as sextas

E depois: `dias = base − feriados − férias + ajuste`, `valor = valor do dia × dias`.

⚠️ QUEM SAIU, QUEM ESTÁ AFASTADO E QUEM TEM "CARTÃO" NÃO RECEBE. As duas primeiras
são a mesma exclusão que as abas da planilha já fazem pela fase; a terceira é só do
transporte, e também vem da planilha (`AA != 'Cartão'`).
"""
from __future__ import annotations

import calendar
import datetime as dt
import logging
from decimal import Decimal

from . import colaboradores, folha_calendario

logger = logging.getLogger("analisesps.folha_auxilio")

ALIMENTACAO = "alimentacao"
TRANSPORTE = "transporte"
TIPOS = (ALIMENTACAO, TRANSPORTE)

ROTULO_DO_TIPO = {ALIMENTACAO: "Auxílio alimentação",
                  TRANSPORTE: "Auxílio transporte"}

CENTAVO = Decimal("0.01")

# As modalidades que o cadastro usa, como a planilha as escreve. A comparação é
# sem acento e sem caixa — o cadastro é digitado por gente.
MODO_FIXO = "mes"                    # "Mês": valor fechado, não conta dia
MODO_MENSAL = "mensal"               # todos os dias do mês − 3
MODO_SEG_SEX = "segunda a sexta"
MODO_SEG_QUI = "segunda a quinta"

# O que o transporte NÃO paga em dinheiro, porque a pessoa tem cartão.
MODO_CARTAO = "cartao"


class ErroDoAuxilio(RuntimeError):
    """Não deu para calcular ou gravar. A frase vai para a tela."""


def _pronto() -> bool:
    """A migração 033 já rodou? Enquanto não, a tela avisa em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.auxilio_ajuste LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


def _sem_acento(texto) -> str:
    """Para comparar modalidade: "Segunda à Quinta" e "segunda a quinta" são a
    mesma coisa, e o cadastro é digitado por gente."""
    import unicodedata
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    return " ".join("".join(c for c in cru if not unicodedata.combining(c))
                    .lower().split())


def periodo_do_mes(ano: int, mes: int) -> tuple:
    """O primeiro e o último dia do mês.

    ⚠️ O AUXÍLIO É POR MÊS INTEIRO, não por quinzena — é o que a planilha faz
    (§7.14.6). Usar o período da quinzena pagaria metade do transporte de alguém
    duas vezes."""
    ultimo = calendar.monthrange(int(ano), int(mes))[1]
    return dt.date(int(ano), int(mes), 1), dt.date(int(ano), int(mes), ultimo)


def dias_da_modalidade(modo: str, inicio, fim) -> tuple:
    """`(dias, conta_sabado, conta_sexta)` da modalidade, antes dos descontos.

    Devolve também COMO contar, porque o desconto de feriado e de férias tem de
    usar a mesma régua: descontar um feriado de sexta de quem não trabalha sexta
    tiraria um dia que ninguém ia pagar."""
    limpo = _sem_acento(modo)
    if limpo == MODO_FIXO:
        # Valor fechado: um "dia" só, que multiplica o valor inteiro.
        return 1, False, True
    if limpo == MODO_MENSAL:
        # Todos os dias do mês menos três — é a fórmula da planilha, e o "−3" é
        # dela, não meu. Não desconta fim de semana, então a régua conta tudo.
        return max(0, (fim - inicio).days + 1 - 3), True, True
    if limpo == MODO_SEG_QUI:
        return folha_calendario.dias_uteis(inicio, fim, sexta=False), False, False
    if limpo == MODO_SEG_SEX:
        return folha_calendario.dias_uteis(inicio, fim), False, True
    # Modalidade desconhecida: NÃO chuta. Zero dias, e a tela diz que não sabe —
    # pagar por uma régua inventada é pior que não pagar e alguém reclamar.
    return 0, False, True


def _valor(bruto) -> Decimal | None:
    if bruto is None:
        return None
    try:
        return Decimal(str(bruto)).quantize(CENTAVO)
    except Exception:  # noqa: BLE001
        return None


def calcular_pessoa(tipo: str, ficha: dict, inicio, fim,
                    ajuste: dict | None = None) -> dict:
    """A conta de uma pessoa. Devolve tudo o que a tela mostra, passo a passo.

    ⚠️ DEVOLVE O CAMINHO INTEIRO, não só o valor: base, feriados, férias, ajuste e
    total. É o que ele pediu para o relatório — *"saber até de onde é que foi que
    veio aquela informação"*. Um valor sozinho não se audita."""
    ajuste = ajuste or {}
    e_alimentacao = tipo == ALIMENTACAO
    modo = (ficha.get("modo_alimentacao") if e_alimentacao
            else ficha.get("modo_transporte")) or ""
    valor_unitario = _valor(ficha.get("valor_alimentacao") if e_alimentacao
                            else ficha.get("valor_transporte"))

    saida = {
        "cpf": ficha.get("cpf", ""),
        "nome": ficha.get("nome", ""),
        "cargo": ficha.get("cargo", ""),
        "link_pipefy": ficha.get("link_pipefy", ""),
        "modo": modo,
        "valor_unitario": valor_unitario,
        "obra": (ajuste.get("obra") or ficha.get("obra_cadastro") or ""),
        "obra_ajustada": bool(ajuste.get("obra")),
        "dias_base": 0, "feriados": 0, "ferias": 0,
        "dias_ajuste": int(ajuste.get("dias") or 0),
        "dias": 0, "valor": Decimal("0.00"),
        "observacao": ajuste.get("observacao", ""),
        # O QUE ELE MARCOU À MÃO, do jeito que está guardado: True, False ou NULO.
        # A tela precisa dos três estados separados para desenhar o seletor —
        # "segue o cálculo" não é a mesma coisa que "não pagar", e mostrar os dois
        # iguais faria o padrão parecer decisão dele.
        "ajuste_pagar": ajuste.get("pagar"),
        "motivos": [],
        "pagar": True,
        # True quando NÃO HÁ o que pagar (falta valor ou modalidade no cadastro).
        # Diferente de `pagar=False`, que é decisão. Ver o comentário abaixo.
        "impossivel": False,
    }

    # ⚠️ A ORDEM AQUI IMPORTA, e foi um teste que me obrigou a arrumá-la.
    #
    # Há dois motivos diferentes para alguém não receber, e eles NÃO são a mesma
    # coisa:
    #
    #   POLÍTICA  — saiu, está afastada, tem cartão. O valor DÁ para calcular; o
    #               que se decidiu é não pagar. Aqui a última palavra é dele: se
    #               ele marcar, paga o valor calculado.
    #   IMPOSSÍVEL— o cadastro não diz o valor, ou não diz a modalidade. Não há
    #               valor nenhum para pagar, e marcar "pagar" pagaria ZERO em
    #               silêncio — que é pior do que não pagar.
    #
    # Na primeira versão os dois caminhos saíam da função antes de consultar o
    # ajuste, e o "pagar mesmo assim" não funcionava justamente nos casos que mais
    # precisam dele.
    situacao = ficha.get("situacao")
    if situacao in (colaboradores.SITUACAO_SAIU,
                    colaboradores.SITUACAO_AFASTADO):
        saida["pagar"] = False
        saida["motivos"].append(ficha.get("motivo")
                                or "não está ativa no cadastro.")
    elif situacao == colaboradores.SITUACAO_SAINDO:
        # Não trava: pode haver valor devido até o último dia. Mas fica dito.
        saida["motivos"].append(ficha.get("motivo") or "está saindo.")

    if not e_alimentacao and _sem_acento(modo) == MODO_CARTAO:
        saida["pagar"] = False
        saida["motivos"].append(
            "o cadastro diz \"Cartão\" — o transporte dela não sai em dinheiro.")

    # OS IMPEDIMENTOS DE VERDADE: sem estes não há o que pagar, e o ajuste dele
    # não muda isso — marcar "pagar" pagaria zero sem dizer.
    if valor_unitario is None:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            "o cadastro não diz o valor deste auxílio. Corrija no card do Pipefy "
            "e atualize o cadastro.")
        return _decidir(saida, ajuste)
    if not modo:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            "o cadastro não diz a modalidade (Mês, Mensal, Segunda à Sexta ou "
            "Segunda à Quinta).")
        return _decidir(saida, ajuste)

    base, sabado, sexta = dias_da_modalidade(modo, inicio, fim)
    if base <= 0:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            f'não sei contar os dias da modalidade "{modo}". As que eu conheço '
            "são: Mês, Mensal, Segunda à Sexta e Segunda à Quinta.")
        return _decidir(saida, ajuste)
    saida["dias_base"] = base

    # VALOR FIXO ("Mês") não desconta dia: é valor fechado.
    if _sem_acento(modo) == MODO_FIXO:
        saida["dias"] = 1
        saida["valor"] = valor_unitario
        if saida["dias_ajuste"]:
            saida["motivos"].append(
                "o ajuste de dias não vale para quem recebe valor fechado "
                '("Mês").')
        return _decidir(saida, ajuste)

    # OS DESCONTOS, com a MESMA régua da modalidade.
    if e_alimentacao:
        saida["feriados"] = folha_calendario.dias_de_feriado_no_periodo(
            inicio, fim, saida["obra"], sabado=sabado, sexta=sexta)
    saida["ferias"] = folha_calendario.dias_de_ferias_no_periodo(
        saida["cpf"], inicio, fim, sabado=sabado, sexta=sexta)

    dias = base - saida["feriados"] - saida["ferias"] + saida["dias_ajuste"]
    # ⚠️ NUNCA NEGATIVO: um ajuste torto não pode virar valor a devolver.
    saida["dias"] = max(0, dias)
    saida["valor"] = (valor_unitario * saida["dias"]).quantize(CENTAVO)
    if saida["dias"] == 0:
        saida["motivos"].append("não sobrou nenhum dia a pagar neste mês.")
    return _decidir(saida, ajuste)


def _decidir(saida: dict, ajuste: dict) -> dict:
    """A última palavra é dele: `pagar` marcado à mão ganha do cálculo.

    ⚠️ NULO É DIFERENTE DE FALSO. Nulo quer dizer "não mexi" — vale o cálculo.
    Falso é "eu decidi não pagar". Confundir os dois faria o padrão virar
    decisão."""
    if ajuste.get("pagar") is False:
        saida["pagar"] = False
        saida["motivos"].append("você desmarcou esta pessoa.")
    elif ajuste.get("pagar") is True and not saida["pagar"]:
        if saida.get("impossivel"):
            # Marcar não resolve falta de valor no cadastro: pagaria zero em
            # silêncio. O recado diz o que consertar, e onde.
            saida["motivos"].append(
                "você marcou para pagar, mas ainda falta o dado no cadastro — "
                "sem ele não há valor nenhum a pagar.")
        else:
            saida["pagar"] = True
            saida["motivos"].append("você marcou para pagar mesmo assim.")
    return saida


def calcular(tipo: str, ano: int, mes: int) -> dict:
    """A verba inteira do mês. Devolve as pessoas e os totais."""
    if tipo not in TIPOS:
        raise ErroDoAuxilio(f'não conheço a verba "{tipo}".')

    inicio, fim = periodo_do_mes(ano, mes)
    ate = fim
    fichas = colaboradores.buscar(so_ativos=False, teto=5000, ate=ate)
    ajustes = ajustes_do_mes(tipo, ano, mes)

    pessoas = []
    for ficha in fichas:
        # SÓ QUEM TEM ESTE AUXÍLIO NO CADASTRO entra na lista. É o que a planilha
        # faz (`WHERE Y != ''`): quem não recebe a verba não é problema a
        # resolver, é gente que não recebe.
        tem = (ficha.get("valor_alimentacao") if tipo == ALIMENTACAO
               else ficha.get("valor_transporte"))
        modo = (ficha.get("modo_alimentacao") if tipo == ALIMENTACAO
                else ficha.get("modo_transporte"))
        if tem is None and not modo:
            continue
        pessoas.append(calcular_pessoa(tipo, ficha, inicio, fim,
                                       ajustes.get(ficha["cpf"])))

    # ⚠️ QUEM PRECISA DE MÃO VEM PRIMEIRO. `False` ordena antes de `True`, então a
    # chave é `pagar` direto — na primeira versão eu escrevi `not pagar`, e a
    # lista saía ao contrário: os certos na frente e os problemas no fim, onde
    # ninguém rola até.
    pessoas.sort(key=lambda p: (p["pagar"], (p["nome"] or "").lower()))
    a_pagar = [p for p in pessoas if p["pagar"] and p["valor"] > 0]

    por_obra: dict = {}
    for p in a_pagar:
        chave = p["obra"] or "(sem obra)"
        atual = por_obra.setdefault(chave, {"obra": chave, "pessoas": 0,
                                            "total": Decimal("0.00")})
        atual["pessoas"] += 1
        atual["total"] += p["valor"]

    return {
        "tipo": tipo,
        "rotulo": ROTULO_DO_TIPO[tipo],
        "ano": int(ano), "mes": int(mes),
        "competencia": f"{int(mes):02d}/{int(ano)}",
        "inicio": inicio, "fim": fim,
        "pessoas": pessoas,
        "quantos": len(pessoas),
        "quantos_a_pagar": len(a_pagar),
        "total": sum((p["valor"] for p in a_pagar), Decimal("0.00")),
        "com_problema": [p for p in pessoas if not p["pagar"]],
        "por_obra": sorted(por_obra.values(), key=lambda o: -o["total"]),
        "desconta_feriado": tipo == ALIMENTACAO,
    }


# ---------------------------------------------------------------------------
# Os ajustes dele
# ---------------------------------------------------------------------------
def ajustes_do_mes(tipo: str, ano: int, mes: int) -> dict:
    """`{cpf: {...}}` do que foi mexido à mão nesta verba e competência."""
    from .db import consultar
    if not _pronto():
        return {}
    linhas = consultar(
        "SELECT cpf, pagar, dias, obra, observacao, alterado_por "
        "  FROM analisesps.auxilio_ajuste "
        " WHERE tipo = ? AND ano = ? AND mes = ?",
        (tipo, int(ano), int(mes)))
    return {l[0]: {"pagar": l[1], "dias": l[2], "obra": l[3],
                   "observacao": l[4], "alterado_por": l[5]} for l in linhas}


def gravar_ajuste(tipo: str, ano: int, mes: int, cpf: str, pagar=None,
                  dias=None, obra: str = "", observacao: str = "",
                  quem: str = "") -> None:
    """Guarda o que ele mexeu. É o que sobrevive ao recálculo."""
    from .db import conexao
    from .folha_rateio import so_digitos

    if not _pronto():
        raise ErroDoAuxilio(
            'a tabela dos ajustes ainda não existe. Aperte "Aplicar '
            'atualizações do banco" em Configurações.')
    if tipo not in TIPOS:
        raise ErroDoAuxilio(f'não conheço a verba "{tipo}".')
    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        raise ErroDoAuxilio("não reconheci o CPF desta pessoa.")

    if dias is not None:
        try:
            dias = int(dias)
        except (TypeError, ValueError):
            raise ErroDoAuxilio("o ajuste de dias tem de ser um número.")
        # Um ajuste absurdo é quase sempre digitação, e mudaria o valor em
        # centenas de reais sem ninguém notar.
        if abs(dias) > 62:
            raise ErroDoAuxilio(
                f"{dias} dias de ajuste é mais que dois meses. Confira o número.")

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.auxilio_ajuste "
            "  (tipo, ano, mes, cpf, pagar, dias, obra, observacao, "
            "   alterado_por) VALUES (?,?,?,?,?,?,?,?,?) "
            " ON CONFLICT (tipo, ano, mes, cpf) DO UPDATE SET "
            "   pagar = EXCLUDED.pagar, dias = EXCLUDED.dias, "
            "   obra = EXCLUDED.obra, observacao = EXCLUDED.observacao, "
            "   alterado_em = now(), alterado_por = EXCLUDED.alterado_por",
            (tipo, int(ano), int(mes), digitos, pagar, dias,
             " ".join(str(obra or "").split()).upper(),
             " ".join(str(observacao or "").split())[:300],
             str(quem or "")[:120]))
        conn.commit()


def limpar_ajuste(tipo: str, ano: int, mes: int, cpf: str) -> bool:
    """Tira o ajuste — a pessoa volta a seguir o cálculo."""
    from .db import conexao
    from .folha_rateio import so_digitos
    if not _pronto():
        return False
    with conexao() as conn:
        cur = conn.execute(
            "DELETE FROM analisesps.auxilio_ajuste "
            " WHERE tipo = ? AND ano = ? AND mes = ? AND cpf = ?",
            (tipo, int(ano), int(mes), so_digitos(cpf)))
        apagou = bool(cur.rowcount and cur.rowcount > 0)
        cur.close()
        conn.commit()
    return apagou
