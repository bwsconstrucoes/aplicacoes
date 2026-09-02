# -*- coding: utf-8 -*-
"""
As perguntas que as telas fazem ao banco.

Cada funcao aqui devolve numeros ja somados — nunca a base inteira. E o que
substitui o `pandas` que rodava no PC: em vez de abrir 185 mil linhas na memoria
e somar em Python, o Postgres soma e devolve uma dezena de linhas.

Vocabulario, o mesmo das telas antigas:
  EXECUTADO    — o que ja foi efetivamente pago ou recebido (regime de caixa).
  EM ABERTO    — o saldo que ainda falta pagar ou receber.
  COMPROMETIDO — os dois somados; e a leitura do DRE (regime de competencia).
Receita entra positiva, despesa negativa: somar a coluna ja da o resultado.
"""
from __future__ import annotations

from .db import consultar

REC = "1. Contas a Receber"
PAG = "2. Contas a Pagar"

# "Foi pago?" nao e "tem data": um titulo em aberto tambem tem data (a de
# vencimento). Quem responde e o texto da situacao, como no painel antigo.
PAGO = "situacao ~* '(pago|recebido|conciliado)'"
# Imposto retido na fonte: o cliente reteve, nao virou caixa da BWS. Entra na
# receita bruta e sai da liquida.
RETIDO = "categoria ILIKE '%Retido%'"

EXECUTADO = f"CASE WHEN {PAGO} THEN pago_recebido ELSE 0 END"
EM_ABERTO = "a_pagar_receber"
COMPROMETIDO = f"({EXECUTADO} + {EM_ABERTO})"


class Filtros:
    """Os filtros da barra lateral, traduzidos para um WHERE."""

    def __init__(self, anos=None, projetos=None, departamentos=None, excluir_trf=True):
        self.anos = [int(a) for a in (anos or [])]
        self.projetos = list(projetos or [])
        self.departamentos = list(departamentos or [])
        self.excluir_trf = bool(excluir_trf)

    def where(self, extra: str = "", params_extra=None) -> tuple[str, list]:
        """Devolve (trecho SQL, parametros). Filtro vazio = tudo.

        `extra` entra por ULTIMO no WHERE, entao os parametros dele vao no fim
        da lista — e por isso que ele recebe os proprios parametros aqui, em vez
        de o chamador ter de adivinhar a ordem."""
        partes, params = [], []
        if self.anos:
            # Titulos em aberto sem data sao backlog de hoje, sem ano de
            # realizacao. O filtro de ano nao pode descarta-los, senao "a pagar"
            # e "a receber" somem da tela.
            partes.append("(ano = ANY(?) OR data IS NULL)")
            params.append(self.anos)
        if self.projetos:
            partes.append("projeto = ANY(?)")
            params.append(self.projetos)
        if self.departamentos:
            partes.append("departamento = ANY(?)")
            params.append(self.departamentos)
        if self.excluir_trf:
            partes.append("analise <> 'TRF'")
        if extra:
            partes.append(extra)
            params.extend(params_extra or [])
        return (" WHERE " + " AND ".join(partes)) if partes else "", params

    def resumo(self) -> list[str]:
        """Descricao curta dos filtros ativos, para os chips no topo da tela."""
        chips = []
        if self.anos:
            chips.append("Ano: " + ", ".join(str(a) for a in sorted(self.anos)))
        if self.projetos:
            chips.append("Projeto: " + ", ".join(self.projetos))
        if self.departamentos:
            chips.append("Obra: " + ", ".join(self.departamentos))
        if self.excluir_trf:
            chips.append("Sem transferências")
        return chips


# ---------------------------------------------------------------------------
# Opcoes dos filtros e estado da base
# ---------------------------------------------------------------------------
def opcoes_de_filtro() -> dict:
    """Anos, projetos e obras que existem na base. Sao valores distintos —
    algumas centenas de linhas, nao a base inteira."""
    anos = [a for (a,) in consultar(
        "SELECT DISTINCT ano FROM fato WHERE ano BETWEEN 2015 AND 2100 ORDER BY ano DESC")]
    projetos = [p for (p,) in consultar(
        "SELECT DISTINCT projeto FROM fato WHERE COALESCE(projeto,'') <> '' ORDER BY projeto")]
    obras = [d for (d,) in consultar(
        "SELECT DISTINCT departamento FROM fato "
        " WHERE COALESCE(departamento,'') <> '' ORDER BY departamento")]
    return {"anos": anos, "projetos": projetos, "obras": obras}


def atualizado_em() -> dict | None:
    """Quando a base foi atualizada pela ultima vez, e como foi."""
    linha = consultar(
        "SELECT tipo, disparo, inicio, fim, ok, mensagem, linhas_fato "
        "  FROM execucoes WHERE fim IS NOT NULL ORDER BY inicio DESC LIMIT 1")
    if not linha:
        return None
    tipo, disparo, inicio, fim, ok, mensagem, linhas = linha[0]
    return {"tipo": tipo, "disparo": disparo, "inicio": inicio, "fim": fim,
            "ok": ok, "mensagem": mensagem, "linhas": linhas}


def base_vazia() -> bool:
    """True quando ainda nao houve nenhuma carga — a tela avisa em vez de
    mostrar tudo zerado como se fosse a verdade."""
    return consultar("SELECT COUNT(*) FROM fato")[0][0] == 0


# ---------------------------------------------------------------------------
# Visao Geral
# ---------------------------------------------------------------------------
def resultado_dre(f: Filtros) -> dict:
    """Receita liquida, despesa e resultado — nas duas leituras (comprometido e
    executado). A receita liquida exclui as retencoes: quem ficou com elas foi o
    cliente, nao a BWS."""
    where, params = f.where("analise = 'DRE'")
    sql = f"""
        SELECT
          SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {COMPROMETIDO} ELSE 0 END),
          SUM(CASE WHEN tipo = ?                    THEN {COMPROMETIDO} ELSE 0 END),
          SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {EXECUTADO}    ELSE 0 END),
          SUM(CASE WHEN tipo = ?                    THEN {EXECUTADO}    ELSE 0 END)
        FROM fato{where}"""
    rec_c, desp_c, rec_e, desp_e = consultar(sql, [REC, PAG, REC, PAG] + params)[0]
    rec_c, desp_c = float(rec_c or 0), float(desp_c or 0)
    rec_e, desp_e = float(rec_e or 0), float(desp_e or 0)
    return {"receita": rec_c, "despesa": desp_c, "resultado": rec_c + desp_c,
            "receita_exec": rec_e, "despesa_exec": desp_e,
            "resultado_exec": rec_e + desp_e}


def dre_por_ano(f: Filtros) -> list[dict]:
    """Receita, despesa e resultado ano a ano — o grafico da Visao Geral."""
    where, params = f.where("analise = 'DRE' AND ano BETWEEN 2015 AND 2100")
    sql = f"""
        SELECT ano,
          SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {COMPROMETIDO} ELSE 0 END),
          SUM(CASE WHEN tipo = ?                    THEN {COMPROMETIDO} ELSE 0 END)
        FROM fato{where} GROUP BY ano ORDER BY ano"""
    saida = []
    for ano, receita, despesa in consultar(sql, [REC, PAG] + params):
        receita, despesa = float(receita or 0), float(despesa or 0)
        saida.append({"ano": ano, "receita": receita, "despesa": despesa,
                      "resultado": receita + despesa})
    return saida


def caixa(f: Filtros) -> dict:
    """Entradas, saidas e geracao de caixa — o que efetivamente circulou.

    Inclui aportes e devolucoes (Analise = Fluxo de Caixa), que ficam fora do
    resultado mas sao dinheiro de verdade. Exclui as retencoes de receita: elas
    nunca passaram pela conta da BWS."""
    where, params = f.where(f"{PAGO} AND NOT (tipo = ? AND {RETIDO})", [REC])
    sql = f"""
        SELECT SUM(CASE WHEN pago_recebido > 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN pago_recebido < 0 THEN pago_recebido ELSE 0 END)
          FROM fato{where}"""
    entradas, saidas = consultar(sql, params)[0]
    entradas, saidas = float(entradas or 0), float(saidas or 0)
    return {"entradas": entradas, "saidas": saidas, "geracao": entradas + saidas}


def caixa_por_ano(f: Filtros) -> list[dict]:
    """Geracao de caixa por ano, com o acumulado."""
    where, params = f.where(
        f"{PAGO} AND NOT (tipo = ? AND {RETIDO}) AND ano BETWEEN 2015 AND 2100", [REC])
    sql = f"SELECT ano, SUM(pago_recebido) FROM fato{where} GROUP BY ano ORDER BY ano"
    saida, acumulado = [], 0.0
    for ano, valor in consultar(sql, params):
        valor = float(valor or 0)
        acumulado += valor
        saida.append({"ano": ano, "valor": valor, "acumulado": acumulado})
    return saida


# ---------------------------------------------------------------------------
# DRE
# ---------------------------------------------------------------------------
def dre_linhas(f: Filtros) -> dict:
    """As linhas do DRE: receita bruta, retencoes, receita liquida, despesas por
    grupo e o resultado — nas tres leituras."""
    where, params = f.where("analise = 'DRE'")
    sql = f"""
        SELECT tipo, ({RETIDO}) AS retido,
               COALESCE(NULLIF(grupo,''), '(sem grupo)') AS nome,
               SUM({EXECUTADO}), SUM({EM_ABERTO})
          FROM fato{where}
         GROUP BY 1, 2, 3"""
    receita_liq = [0.0, 0.0]
    retencoes = [0.0, 0.0]
    despesas: dict[str, list[float]] = {}
    for tipo, retido, nome, executado, aberto in consultar(sql, params):
        executado, aberto = float(executado or 0), float(aberto or 0)
        if tipo == REC:
            alvo = retencoes if retido else receita_liq
            alvo[0] += executado
            alvo[1] += aberto
        else:
            linha = despesas.setdefault(nome, [0.0, 0.0])
            linha[0] += executado
            linha[1] += aberto

    def _linha(nome, par):
        return {"linha": nome, "executado": par[0], "aberto": par[1],
                "comprometido": par[0] + par[1]}

    # despesas sao negativas: ordenar crescente poe a maior despesa em cima
    grupos = sorted(despesas.items(), key=lambda kv: kv[1][0] + kv[1][1])
    bruta = [receita_liq[0] + retencoes[0], receita_liq[1] + retencoes[1]]
    total_desp = [sum(v[0] for v in despesas.values()),
                  sum(v[1] for v in despesas.values())]
    resultado = [receita_liq[0] + total_desp[0], receita_liq[1] + total_desp[1]]
    return {
        "receita_bruta": _linha("Receita bruta", bruta),
        "retencoes": _linha("(−) Impostos retidos na fonte", retencoes),
        "receita_liquida": _linha("Receita líquida", receita_liq),
        "despesas": [_linha(nome, par) for nome, par in grupos],
        "total_despesas": _linha("Total de despesas", total_desp),
        "resultado": _linha("Resultado", resultado),
    }


def despesas_por(f: Filtros, quebra: str = "grupo", visao: str = "comprometido",
                 limite: int = 25) -> list[dict]:
    """Despesas por grupo ou por categoria, da maior para a menor."""
    coluna = "categoria" if quebra == "categoria" else "grupo"
    medida = {"executado": EXECUTADO, "aberto": EM_ABERTO}.get(visao, COMPROMETIDO)
    where, params = f.where("analise = 'DRE' AND tipo = ?", [PAG])
    sql = (f"SELECT COALESCE(NULLIF({coluna},''), '(sem {coluna})'), SUM({medida}) "
           f"  FROM fato{where} GROUP BY 1 HAVING SUM({medida}) <> 0 "
           f" ORDER BY SUM({medida}) ASC LIMIT {int(limite)}")
    return [{"nome": nome, "valor": float(valor or 0)}
            for nome, valor in consultar(sql, params)]


def receita_por_obra(f: Filtros, limite: int = 25) -> list[dict]:
    """Receita por obra: o que ja entrou, o que o cliente reteve e o que falta."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [REC])
    sql = f"""
        SELECT COALESCE(NULLIF(departamento,''), '(sem obra)'),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM(CASE WHEN     ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM({EM_ABERTO})
          FROM fato{where} GROUP BY 1
         ORDER BY 2 DESC LIMIT {int(limite)}"""
    saida = []
    for obra, recebido, retido, aberto in consultar(sql, params):
        recebido, retido = float(recebido or 0), float(retido or 0)
        aberto = float(aberto or 0)
        saida.append({"obra": obra, "recebido": recebido, "retido": retido,
                      "a_receber": aberto, "bruto": recebido + retido + aberto})
    return saida


def top_credores(f: Filtros, limite: int = 20) -> list[dict]:
    """Para quem mais se pagou — ou para quem ainda se deve."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [PAG])
    sql = f"""
        SELECT COALESCE(NULLIF(razao_social,''), '(sem fornecedor)'),
               SUM({EXECUTADO}), SUM({EM_ABERTO}), COUNT(*)
          FROM fato{where} GROUP BY 1
         ORDER BY SUM({COMPROMETIDO}) ASC LIMIT {int(limite)}"""
    return [{"nome": nome, "pago": float(pago or 0), "aberto": float(aberto or 0),
             "titulos": qtd}
            for nome, pago, aberto, qtd in consultar(sql, params)]


# ---------------------------------------------------------------------------
# Fluxo de Caixa
# ---------------------------------------------------------------------------
def caixa_por_mes(f: Filtros) -> list[dict]:
    """Entradas, saídas e o caixa acumulado, mês a mês.

    Só o que foi efetivamente pago ou recebido. As retenções de receita ficam
    de fora: o cliente as reteve, nunca passaram pela conta da BWS."""
    where, params = f.where(f"{PAGO} AND NOT (tipo = ? AND {RETIDO}) AND data IS NOT NULL",
                            [REC])
    # GROUP BY 1 (a posicao da coluna), nao `GROUP BY mes`: a tabela `fato` TEM
    # uma coluna chamada `mes`, e o Postgres daria preferencia a ela em vez do
    # apelido — agrupando pelo mes do ano, sem separar 2024 de 2025.
    sql = f"""
        SELECT date_trunc('month', data)::date AS inicio_do_mes,
               SUM(CASE WHEN pago_recebido > 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN pago_recebido < 0 THEN pago_recebido ELSE 0 END)
          FROM fato{where}
         GROUP BY 1 ORDER BY 1"""
    saida, acumulado = [], 0.0
    for mes, entradas, saidas in consultar(sql, params):
        entradas, saidas = float(entradas or 0), float(saidas or 0)
        liquido = entradas + saidas
        acumulado += liquido
        saida.append({"mes": mes, "rotulo": mes.strftime("%m/%Y"),
                      "entradas": entradas, "saidas": saidas,
                      "liquido": liquido, "acumulado": acumulado})
    return saida


# ---------------------------------------------------------------------------
# Resultado por obra / projeto
# ---------------------------------------------------------------------------
def resultado_por(f: Filtros, nivel: str = "projeto", medida: str = "comprometido",
                  limite: int = 40) -> list[dict]:
    """Receita líquida, despesa e resultado, por projeto ou por obra.

    Mesma base do DRE, e a receita é a LÍQUIDA — sem as retenções."""
    coluna = "departamento" if nivel == "obra" else "projeto"
    valor = EXECUTADO if medida == "executado" else COMPROMETIDO
    where, params = f.where("analise = 'DRE'")
    # O resultado (receita + despesa) e repetido no ORDER BY em vez de `2 + 3`:
    # no Postgres um numero solto no ORDER BY e a posicao da coluna, mas dentro
    # de uma conta ele vira a constante — `2 + 3` ordenaria por 5, sempre igual.
    resultado = (f"SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {valor} ELSE 0 END) "
                 f"+ SUM(CASE WHEN tipo = ? THEN {valor} ELSE 0 END)")
    sql = f"""
        SELECT COALESCE(NULLIF({coluna},''), '(sem {coluna})'),
               SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ?                    THEN {valor} ELSE 0 END)
          FROM fato{where} GROUP BY 1
         ORDER BY {resultado} DESC LIMIT {int(limite)}"""
    saida = []
    # ordem dos parametros: os dois do SELECT, os do WHERE, os dois do ORDER BY
    for nome, receita, despesa in consultar(sql, [REC, PAG] + params + [REC, PAG]):
        receita, despesa = float(receita or 0), float(despesa or 0)
        saida.append({"nome": nome, "receita": receita, "despesa": despesa,
                      "resultado": receita + despesa})
    return saida


# ---------------------------------------------------------------------------
# Comprometido vs Executado
# ---------------------------------------------------------------------------
def comprometido_vs_executado(f: Filtros, nivel: str = "projeto",
                              tipo: str = "pagar", limite: int = 40) -> list[dict]:
    """Quanto de cada obra já foi executado e quanto ainda falta.

    Diferente do resultado: aqui não se somam receita e despesa, olha-se um lado
    de cada vez — quanto daquilo que a obra vai custar já saiu, ou quanto do que
    ela vai render já entrou."""
    coluna = "departamento" if nivel == "obra" else "projeto"
    alvo = PAG if tipo == "pagar" else REC
    where, params = f.where("tipo = ?", [alvo])
    sql = f"""
        SELECT COALESCE(NULLIF({coluna},''), '(sem {coluna})'),
               SUM({EXECUTADO}), SUM({EM_ABERTO})
          FROM fato{where} GROUP BY 1
         ORDER BY ABS(SUM({COMPROMETIDO})) DESC LIMIT {int(limite)}"""
    saida = []
    for nome, executado, a_executar in consultar(sql, params):
        executado, a_executar = float(executado or 0), float(a_executar or 0)
        comprometido = executado + a_executar
        saida.append({
            "nome": nome, "executado": executado, "a_executar": a_executar,
            "comprometido": comprometido,
            "pct": (abs(executado) / abs(comprometido) * 100) if comprometido else 0.0,
        })
    return saida
