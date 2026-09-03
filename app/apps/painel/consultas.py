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
    from .horario import para_brasilia
    tipo, disparo, inicio, fim, ok, mensagem, linhas = linha[0]
    # convertido aqui, na fonte: se cada tela convertesse por conta propria,
    # uma esqueceria e mostraria hora de Londres sem ninguem notar
    return {"tipo": tipo, "disparo": disparo,
            "inicio": para_brasilia(inicio), "fim": para_brasilia(fim),
            "ok": ok, "mensagem": mensagem, "linhas": linhas}


# Sem carimbo por mais que isto, a execucao e dada como morta. Generoso de
# proposito: uma pagina lenta do OMIE nao pode ser confundida com um servidor
# que caiu.
MINUTOS_SEM_SINAL_ATE_MORTA = 10


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
# Juros e multa efetivamente PAGOS sao despesa financeira: entram no DRE, na
# linha "Juros e Multas Pagos", e somam no total. Ficaram de fora da primeira
# versao desta tela, e o resultado saia maior do que era.
ENCARGO = f"CASE WHEN {PAGO} THEN (juros + multa) ELSE 0 END"


def dre_linhas(f: Filtros) -> dict:
    """O DRE, linha a linha, na MESMA ordem e com os MESMOS rotulos da tela que
    o dono construiu:

        Receita Bruta de Servicos
        (-) Retencoes na fonte          <- negativa, ela reconstroi o bruto
        = Receita Liquida
        (em branco)
          <um grupo de despesa por linha, indentado>
          Juros e Multas Pagos          <- so aparece se houver
        = Total Custos/Despesas
        (em branco)
        = RESULTADO

    Cada linha vem nas tres leituras: executado, em aberto e comprometido.
    """
    where, params = f.where("analise = 'DRE'")
    sql = f"""
        SELECT tipo, ({RETIDO}) AS retido,
               COALESCE(NULLIF(grupo,''), '(sem grupo)') AS nome,
               SUM({EXECUTADO}), SUM({EM_ABERTO}), SUM({ENCARGO})
          FROM fato{where}
         GROUP BY 1, 2, 3"""
    receita_liquida = [0.0, 0.0]
    retencoes = [0.0, 0.0]
    despesas: dict[str, list[float]] = {}
    encargo = 0.0
    for tipo, retido, nome, executado, aberto, enc in consultar(sql, params):
        executado, aberto = float(executado or 0), float(aberto or 0)
        if tipo == REC:
            alvo = retencoes if retido else receita_liquida
            alvo[0] += executado
            alvo[1] += aberto
        else:
            linha = despesas.setdefault(nome, [0.0, 0.0])
            linha[0] += executado
            linha[1] += aberto
            encargo += float(enc or 0)

    def _linha(rotulo, executado, aberto, estilo=""):
        return {"linha": rotulo, "executado": executado, "aberto": aberto,
                "comprometido": executado + aberto, "estilo": estilo}

    bruta = [receita_liquida[0] + retencoes[0], receita_liquida[1] + retencoes[1]]
    total_desp = [sum(v[0] for v in despesas.values()) + encargo,
                  sum(v[1] for v in despesas.values())]

    linhas = [
        _linha("Receita Bruta de Serviços", bruta[0], bruta[1], "destaque"),
        # negativa de proposito: e ela que explica a diferenca entre bruto e liquido
        _linha("(−) Retenções na fonte", -retencoes[0], -retencoes[1], "sub"),
        _linha("= Receita Líquida", receita_liquida[0], receita_liquida[1], "destaque"),
        {"linha": "", "estilo": "branco"},
    ]
    # em ordem alfabetica, como na tela antiga — ordenar por valor faz a lista
    # dancar a cada mudanca de filtro, e nao se acha mais nada
    for nome in sorted(despesas):
        linhas.append(_linha("  " + nome, despesas[nome][0], despesas[nome][1]))
    if abs(encargo) > 0.005:
        linhas.append(_linha("  Juros e Multas Pagos", encargo, 0.0))
    linhas.append(_linha("= Total Custos/Despesas", total_desp[0], total_desp[1],
                         "destaque"))
    linhas.append({"linha": "", "estilo": "branco"})
    linhas.append(_linha("= RESULTADO", receita_liquida[0] + total_desp[0],
                         receita_liquida[1] + total_desp[1], "total"))

    return {
        "linhas": linhas,
        # os cinco numeros do topo, os mesmos da tela antiga
        "receita_liquida": receita_liquida[0] + receita_liquida[1],
        "retencoes": retencoes[0] + retencoes[1],
        "receita_bruta": bruta[0] + bruta[1],
        "despesas": total_desp[0] + total_desp[1],
        "resultado": (receita_liquida[0] + receita_liquida[1]
                      + total_desp[0] + total_desp[1]),
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
    linhas = [{"nome": nome, "valor": float(valor or 0)}
              for nome, valor in consultar(sql, params)]
    # o percentual e sobre o total das despesas mostradas, como na tela antiga
    total = sum(l["valor"] for l in linhas) or 1.0
    for linha in linhas:
        linha["pct_total"] = abs(linha["valor"] / total * 100)
    return linhas


def encargo_pago(f: Filtros) -> float:
    """Juros e multa efetivamente pagos, no mesmo recorte que o DRE usa."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [PAG])
    linhas = consultar(f"SELECT COALESCE(SUM({ENCARGO}), 0) FROM fato{where}", params)
    return float(linhas[0][0] or 0) if linhas else 0.0


def despesas_por_categoria_com_encargo(f: Filtros, limite: int = 1000) -> list[dict]:
    """As despesas por categoria MAIS a linha dos encargos, como na planilha antiga.

    Juros e multa pagos entram no DRE, mas nao tem categoria propria no plano
    financeiro do OMIE. Sem acrescentar a linha, a aba de categorias soma menos
    que a aba do DRE — o mesmo arquivo mostrando dois totais diferentes, que e
    exatamente o tipo de coisa que faz perder a confianca no relatorio inteiro.

    A tela antiga fazia isso de proposito, e so na planilha: na tela a aba de
    despesas continua sendo o que veio do plano de contas.
    """
    linhas = despesas_por(f, quebra="categoria", limite=limite)
    encargo = encargo_pago(f)
    if abs(encargo) > 0.005:
        # mesmo rotulo da linha do DRE: e por ele que se liga uma aba na outra
        linhas.append({"nome": "Juros e Multas Pagos", "valor": encargo,
                       "pct_total": 0.0})
        # o percentual e sobre o total mostrado, entao refaz com a linha nova
        total = sum(l["valor"] for l in linhas) or 1.0
        for linha in linhas:
            linha["pct_total"] = abs(linha["valor"] / total * 100)
        linhas.sort(key=lambda l: l["valor"])
    return linhas


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


# ---------------------------------------------------------------------------
# Necessidade de Caixa
# ---------------------------------------------------------------------------
# Esta tela IGNORA os filtros da barra lateral, de propósito — como na versão
# antiga. Ela responde "a empresa inteira precisou do banco, e quando?", e essa
# régua tem de ser fixa: se mudasse com o filtro, não seria régua.

# Como o OMIE nomeia cada tipo de movimento financeiro. Se o plano de contas
# mudar os nomes, é aqui que se ajusta — as três telas leem daqui.
E_EMPRESTIMO = "categoria ~* 'Empr[eé]st'"
E_APORTE = "categoria ILIKE '%Aporte%'"
E_DIVIDENDO = "categoria ILIKE '%Dividendo%'"
E_APLICACAO = "categoria ~* '(Aplica|Resgate)'"

# Base da simulação: só o que virou caixa, com data, e diferente de zero.
_BASE_CAIXA = f"{PAGO} AND data IS NOT NULL AND pago_recebido <> 0"


def caixa_mensal_por_obra() -> list[tuple]:
    """Quanto cada obra gerou ou consumiu de caixa, mês a mês.

    Base do DRE (operação), sem as retenções de receita. Devolve
    (início do mês, obra, valor) — algumas milhares de linhas, não a base."""
    sql = f"""
        SELECT date_trunc('month', data)::date,
               COALESCE(NULLIF(departamento,''), '(sem obra)'),
               SUM(pago_recebido)
          FROM fato
         WHERE {_BASE_CAIXA} AND analise = 'DRE'
           AND NOT (tipo = ? AND {RETIDO})
         GROUP BY 1, 2 ORDER BY 1, 2"""
    return [(mes, obra, float(valor or 0)) for mes, obra, valor in consultar(sql, [REC])]


def financeiro_mensal() -> list[dict]:
    """As fontes de dinheiro que NÃO vêm da operação, mês a mês.

    Empréstimo tomado e principal pago, aporte recebido, dividendo e devolução
    de aporte pagos, e um "outros" para o que sobra (venda de ativo, aumento de
    capital). Aplicação e resgate ficam fora: são o caixa mudando de bolso."""
    sql = f"""
        SELECT date_trunc('month', data)::date,
               SUM(CASE WHEN {E_EMPRESTIMO} AND pago_recebido > 0
                        THEN pago_recebido ELSE 0 END) AS emprestimo_tomado,
               SUM(CASE WHEN {E_EMPRESTIMO} AND pago_recebido < 0
                        THEN pago_recebido ELSE 0 END) AS emprestimo_pago,
               SUM(CASE WHEN {E_APORTE} AND pago_recebido > 0
                        THEN pago_recebido ELSE 0 END) AS aporte_recebido,
               SUM(CASE WHEN ({E_APORTE} AND pago_recebido < 0) OR {E_DIVIDENDO}
                        THEN pago_recebido ELSE 0 END) AS dividendo_pago,
               SUM(CASE WHEN NOT ({E_EMPRESTIMO}) AND NOT ({E_APORTE})
                         AND NOT ({E_DIVIDENDO}) AND NOT ({E_APLICACAO})
                        THEN pago_recebido ELSE 0 END) AS outros
          FROM fato
         WHERE {_BASE_CAIXA} AND analise = 'Fluxo de Caixa'
         GROUP BY 1 ORDER BY 1"""
    campos = ("emprestimo_tomado", "emprestimo_pago", "aporte_recebido",
              "dividendo_pago", "outros")
    return [dict(mes=linha[0], **{c: float(v or 0) for c, v in zip(campos, linha[1:])})
            for linha in consultar(sql)]


def obra_para_projeto() -> dict:
    """A que projeto cada obra pertence. Quando a obra aparece com mais de um
    projeto (dado inconsistente na planilha), vale o mais frequente."""
    sql = """
        SELECT departamento, projeto, COUNT(*) AS quantas
          FROM fato
         WHERE COALESCE(departamento,'') <> ''
         GROUP BY 1, 2 ORDER BY 1, 3 DESC"""
    mapa = {}
    for obra, projeto, _quantas in consultar(sql):
        if obra not in mapa:                       # o primeiro é o mais frequente
            mapa[obra] = (projeto or "").strip()
    return mapa


# ---------------------------------------------------------------------------
# Receita de Obra — por medição
# ---------------------------------------------------------------------------
# Uma medição é o que a obra faturou num período. No OMIE ela vira vários
# títulos (as parcelas), sem número de documento que os ligue — o elo é a
# observação. A chave que junta tudo isso é gravada na própria linha do fato
# (migração 004), então quem agrupa é o banco.

def medicoes(f: Filtros, visao: str = "todas", limite: int = 300) -> list[dict]:
    """As medições de obra, da maior para a menor.

    Bruto = o que já entrou + o que o cliente reteve + o que falta receber.
    `visao`: 'todas', 'a_receber' (só com saldo) ou 'quitadas'."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [REC])
    tendo = {
        "a_receber": "HAVING ABS(SUM(a_pagar_receber)) > 0.005",
        "quitadas": "HAVING ABS(SUM(a_pagar_receber)) <= 0.005",
    }.get(visao, "")
    sql = f"""
        SELECT COALESCE(NULLIF(medicao_rotulo,''), '(sem medição)'),
               MAX(razao_social), MAX(departamento), MAX(projeto),
               MAX(numero_documento), MAX(link), MAX(data),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM(CASE WHEN     ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM(a_pagar_receber)
          FROM fato{where}
         GROUP BY 1 {tendo}
         ORDER BY ABS(SUM({COMPROMETIDO})) DESC LIMIT {int(limite)}"""
    saida = []
    for (rotulo, cliente, obra, projeto, documento, link, data,
         recebido, retido, a_receber) in consultar(sql, params):
        recebido, retido = float(recebido or 0), float(retido or 0)
        a_receber = float(a_receber or 0)
        bruto = recebido + retido + a_receber
        if abs(bruto) <= 0.005:
            continue
        if abs(recebido) > 0.005 and abs(a_receber) <= 0.005:
            situacao = "Recebida"
        elif abs(recebido) > 0.005:
            situacao = "Recebida em parte"
        else:
            situacao = "A receber"
        saida.append({
            "medicao": rotulo, "cliente": cliente or "", "obra": obra or "",
            "projeto": projeto or "", "documento": documento or "",
            "link": link or "", "data": data,
            "recebido": recebido, "retido": retido, "a_receber": a_receber,
            "bruto": bruto, "situacao": situacao,
        })
    return saida


def total_das_medicoes(f: Filtros, visao: str = "todas") -> dict:
    """Os totais das medições — somados pelo banco, não pela lista da tela.

    A tela mostra as 300 maiores; o total tem de ser de TODAS, senão o rodapé
    não bate com o DRE."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [REC])
    tendo = {
        "a_receber": "HAVING ABS(SUM(a_pagar_receber)) > 0.005",
        "quitadas": "HAVING ABS(SUM(a_pagar_receber)) <= 0.005",
    }.get(visao, "")
    sql = f"""
        SELECT COUNT(*), SUM(recebido), SUM(retido), SUM(aberto)
          FROM (
            SELECT SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END) AS recebido,
                   SUM(CASE WHEN     ({RETIDO}) THEN {EXECUTADO} ELSE 0 END) AS retido,
                   SUM(a_pagar_receber) AS aberto
              FROM fato{where}
             GROUP BY COALESCE(NULLIF(medicao_rotulo,''), '(sem medição)') {tendo}
          ) AS por_medicao"""
    quantas, recebido, retido, aberto = consultar(sql, params)[0]
    recebido, retido = float(recebido or 0), float(retido or 0)
    aberto = float(aberto or 0)
    return {"quantas": quantas or 0, "recebido": recebido, "retido": retido,
            "a_receber": aberto, "bruto": recebido + retido + aberto}


def recebimentos_da_medicao(medicao: str, limite: int = 200) -> list[dict]:
    """Cada entrada de dinheiro que quitou uma medição, com data e valor exatos.

    Vem da outra tabela (`fato_recebimentos`), que abre por movimento: um título
    recebido em três parcelas aparece aqui como três linhas."""
    sql = """
        SELECT data, valor, juros, multa, desconto, conta_corrente, parcela,
               origem, numero_documento
          FROM fato_recebimentos
         WHERE medicao = ?
         ORDER BY data NULLS LAST, id
         LIMIT %d""" % int(limite)
    campos = ("data", "valor", "juros", "multa", "desconto", "conta_corrente",
              "parcela", "origem", "numero_documento")
    return [dict(zip(campos, linha)) for linha in consultar(sql, (medicao,))]


def outras_receitas(f: Filtros, limite: int = 60) -> list[dict]:
    """Receita que não é de obra: rendimento, estorno, devolução."""
    where, params = f.where(
        "analise = 'DRE' AND tipo = ? AND categoria <> 'Receita de Obras' "
        f"AND NOT ({RETIDO})", [REC])
    sql = f"""
        SELECT categoria, SUM({EXECUTADO}), SUM({EM_ABERTO}), COUNT(*)
          FROM fato{where} GROUP BY 1
        HAVING ABS(SUM({COMPROMETIDO})) > 0.005
         ORDER BY ABS(SUM({COMPROMETIDO})) DESC LIMIT {int(limite)}"""
    return [{"categoria": c, "recebido": float(r or 0), "a_receber": float(a or 0),
             "titulos": n} for c, r, a, n in consultar(sql, params)]


# ---------------------------------------------------------------------------
# Prestação de Contas — a base
# ---------------------------------------------------------------------------
# Três consultas pequenas, cada uma na granularidade exata que a conta precisa.
# Trazer a base crua e agrupar aqui seria voltar ao problema que este painel
# resolveu: são 185 mil linhas.

# Lancamento sem data existe: titulo cujo vencimento nao pode ser lido. Ele NAO
# some da apuracao — o valor e real e conta no resultado da obra. So nao da para
# rateá-lo por mes, e o rateio o devolve como sobra, com o motivo escrito.
SEM_DATA = "(sem data)"


def _medida(medida: str) -> str:
    return EXECUTADO if medida == "executado" else COMPROMETIDO


def apuracao_por_obra_mes(medida: str = "comprometido") -> list[dict]:
    """Receita líquida, retenções e despesas de cada obra, mês a mês.

    É a base de tudo na prestação de contas. Umas poucas milhares de linhas
    (obras × meses), não a base inteira."""
    valor = _medida(medida)
    sql = f"""
        SELECT COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'),
               COALESCE(NULLIF(departamento,''), '(sem obra)'),
               COALESCE(NULLIF(projeto,''), ''),
               SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ? AND     ({RETIDO}) THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ?                    THEN {valor} ELSE 0 END)
          FROM fato
         WHERE analise = 'DRE'
         GROUP BY 1, 2, 3
        HAVING ABS(SUM({valor})) > 0.005
         ORDER BY 1, 2"""
    campos = ("mes", "obra", "projeto", "receita_liquida", "retencoes", "despesas")
    return [dict(zip(campos, (linha[0], linha[1], linha[2],
                             float(linha[3] or 0), float(linha[4] or 0),
                             float(linha[5] or 0))))
            for linha in consultar(sql, [REC, REC, PAG])]


def custo_de_pessoal_por_obra_mes(grupo_pessoal: str,
                                  medida: str = "comprometido") -> list[tuple]:
    """Quanto cada obra gastou com pessoal, mês a mês.

    É o "driver" do rateio: o custo administrativo é dividido entre as obras na
    proporção do pessoal de cada uma. A ideia por trás: obra com mais gente
    consome mais estrutura."""
    valor = _medida(medida)
    sql = f"""
        SELECT COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'),
               COALESCE(NULLIF(departamento,''), '(sem obra)'),
               ABS(SUM({valor}))
          FROM fato
         WHERE analise = 'DRE' AND tipo = ?
           AND TRIM(COALESCE(grupo,'')) = ?
         GROUP BY 1, 2 HAVING ABS(SUM({valor})) > 0.005"""
    return [(mes, obra, float(v or 0))
            for mes, obra, v in consultar(sql, [PAG, grupo_pessoal])]


def despesa_administrativa(deptos_admin, medida: str = "comprometido") -> list[dict]:
    """As despesas dos departamentos administrativos, abertas por grupo e
    categoria — é o que as regras de rateio selecionam."""
    if not deptos_admin:
        return []
    valor = _medida(medida)
    sql = f"""
        SELECT COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'), departamento,
               TRIM(COALESCE(grupo,'')), TRIM(COALESCE(categoria,'')),
               SUM({valor})
          FROM fato
         WHERE analise = 'DRE' AND tipo = ?
           AND departamento = ANY(?)
         GROUP BY 1, 2, 3, 4 HAVING ABS(SUM({valor})) > 0.005"""
    campos = ("mes", "depto", "grupo", "categoria", "valor")
    return [dict(zip(campos, (l[0], l[1], l[2], l[3], float(l[4] or 0))))
            for l in consultar(sql, [PAG, list(deptos_admin)])]


def grupos_e_categorias() -> dict:
    """O que existe na base, para montar as regras sem digitar nome à mão."""
    grupos = [g for (g,) in consultar(
        "SELECT DISTINCT TRIM(grupo) FROM fato "
        " WHERE analise='DRE' AND COALESCE(TRIM(grupo),'') <> '' ORDER BY 1")]
    categorias = [c for (c,) in consultar(
        "SELECT DISTINCT TRIM(categoria) FROM fato "
        " WHERE analise='DRE' AND COALESCE(TRIM(categoria),'') <> '' ORDER BY 1")]
    return {"grupos": grupos, "categorias": categorias}


def execucao_em_andamento() -> dict | None:
    """A atualização que ainda não terminou, se houver — e se ela está viva.

    "Viva" é ter carimbado a hora recentemente. Uma execução que parou de
    carimbar morreu junto com o processo (quase sempre um reinício do serviço,
    que acontece a cada publicação de código). Sem essa distinção, a tela
    mostrava a falha ANTERIOR como se fosse a atual — e quem lia ficava
    diagnosticando um erro velho."""
    from .horario import para_brasilia

    linha = consultar(
        "SELECT id, tipo, disparo, inicio, etapa, progresso, visto_em, "
        "       EXTRACT(EPOCH FROM (now() - COALESCE(visto_em, inicio))) "
        "  FROM execucoes WHERE fim IS NULL ORDER BY inicio DESC LIMIT 1")
    if not linha:
        return None
    (execucao_id, tipo, disparo, inicio, etapa,
     progresso, visto_em, silencio) = linha[0]
    silencio = float(silencio or 0)
    return {
        "id": execucao_id, "tipo": tipo, "disparo": disparo,
        "inicio": para_brasilia(inicio),
        "etapa": etapa or "começando",
        "progresso": progresso or "",
        "detalhe_progresso": progresso or "",
        "visto_em": para_brasilia(visto_em),
        "silencio_minutos": round(silencio / 60, 1),
        "viva": silencio < MINUTOS_SEM_SINAL_ATE_MORTA * 60,
    }


def etapas_da_carga() -> list[dict]:
    """Quais partes da primeira carga ja terminaram.

    Serve para a tela dizer "vai retomar da etapa 5" em vez de deixar o dono
    achando que vai esperar tudo de novo — e para ele poder decidir recomecar
    do zero se desconfiar do que ja entrou."""
    from .sync.espelho import ETAPAS_DA_CARGA, PREFIXO_ETAPA

    try:
        feitas = {nome[len(PREFIXO_ETAPA):] for (nome,) in consultar(
            "SELECT entidade FROM sync_state WHERE entidade LIKE ?",
            (PREFIXO_ETAPA + "%",))}
    except Exception:
        feitas = set()
    return [{"chave": chave, "rotulo": rotulo, "pronta": chave in feitas}
            for chave, rotulo in ETAPAS_DA_CARGA]


def resultado_mensal(f: Filtros, medida: str = "executado") -> list[dict]:
    """Receita, despesa e resultado acumulado mês a mês — o gráfico "Fluxo
    Financeiro" que fica dentro da tela do DRE.

    São duas leituras diferentes, e a distinção importa:

    - **executado**: só o que foi pago ou recebido, pelo mês em que o dinheiro
      andou. O acumulado mostra a geração de caixa já efetivada.
    - **comprometido**: realizado mais em aberto, pelo mês da data do título
      (pagamento quando houve, senão vencimento). Mostra a geração projetada.
    """
    if medida == "comprometido":
        valor = COMPROMETIDO
        extra = "data IS NOT NULL"
    else:
        valor = "pago_recebido"
        extra = f"{PAGO} AND data IS NOT NULL"

    where, params = f.where(f"analise = 'DRE' AND {extra} AND NOT (tipo = ? AND {RETIDO})",
                            [REC])
    sql = f"""
        SELECT to_char(data, 'YYYY-MM'),
               SUM(CASE WHEN tipo = ? THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ? THEN {valor} ELSE 0 END)
          FROM fato{where}
         GROUP BY 1 ORDER BY 1"""
    saida, acumulado = [], 0.0
    for mes, receita, despesa in consultar(sql, [REC, PAG] + params):
        receita, despesa = float(receita or 0), float(despesa or 0)
        resultado = receita + despesa
        acumulado += resultado
        ano, _, m = (mes or "").partition("-")
        saida.append({"mes": mes, "rotulo": f"{m}/{ano}", "receita": receita,
                      "despesa": despesa, "resultado": resultado,
                      "acumulado": acumulado})
    return saida


# ---------------------------------------------------------------------------
# Despesas Analítico — lançamento a lançamento
# ---------------------------------------------------------------------------
# É a tela que responde "de onde veio esse número". Sem ela, o painel mostra
# totais que ninguém consegue conferir — e um total que não se abre não se
# discute com fornecedor nenhum.

ORDENS = {
    "valor": "ABS({medida}) DESC",
    "data": "data DESC NULLS LAST",
    "credor": "razao_social ASC",
    "categoria": "categoria ASC",
}


def analitico_despesas(f: Filtros, grupo="", categoria="", credor="",
                       busca="", visao="comprometido", ordem="valor",
                       pagina=1, por_pagina=200) -> dict:
    """Os lançamentos de despesa, um por linha, com filtros próprios.

    Devolve também os TOTAIS de toda a seleção — não só da página. O rodapé
    somando apenas as 200 linhas visíveis seria pior que não ter rodapé.
    """
    medida = {"executado": EXECUTADO, "aberto": EM_ABERTO}.get(visao, COMPROMETIDO)

    condicoes, extras = ["analise = 'DRE'", "tipo = ?"], [PAG]
    if grupo:
        condicoes.append("COALESCE(NULLIF(grupo,''), '(sem grupo)') = ?")
        extras.append(grupo)
    if categoria:
        condicoes.append("COALESCE(NULLIF(categoria,''), '(sem categoria)') = ?")
        extras.append(categoria)
    if credor:
        condicoes.append("COALESCE(NULLIF(razao_social,''), '(sem fornecedor)') = ?")
        extras.append(credor)
    if busca:
        # uma caixa de busca que varre o que a pessoa lê na tela: fornecedor,
        # categoria, documento e a observação do título
        condicoes.append("(razao_social ILIKE ? OR categoria ILIKE ? "
                         " OR numero_documento ILIKE ? OR observacao ILIKE ?)")
        extras.extend([f"%{busca}%"] * 4)

    where, params = f.where(" AND ".join(condicoes), extras)

    total_sql = f"""
        SELECT COUNT(*), SUM({EXECUTADO}), SUM({EM_ABERTO}),
               SUM({ENCARGO})
          FROM fato{where}"""
    quantos, executado, aberto, encargo = consultar(total_sql, params)[0]

    ordenacao = ORDENS.get(ordem, ORDENS["valor"]).format(medida=medida)
    pagina = max(int(pagina or 1), 1)
    sql = f"""
        SELECT data, razao_social, cnpj_cpf, grupo, categoria, departamento,
               projeto, numero_documento, observacao, conta_corrente, situacao,
               {EXECUTADO}, {EM_ABERTO}, juros, multa, link
          FROM fato{where}
         ORDER BY {ordenacao}
         LIMIT {int(por_pagina)} OFFSET {int((pagina - 1) * por_pagina)}"""
    campos = ("data", "credor", "cnpj", "grupo", "categoria", "obra", "projeto",
              "documento", "observacao", "conta", "situacao", "pago", "a_pagar",
              "juros", "multa", "link")
    linhas = []
    for bruta in consultar(sql, params):
        linha = dict(zip(campos, bruta))
        for campo in ("pago", "a_pagar", "juros", "multa"):
            linha[campo] = float(linha[campo] or 0)
        linha["total"] = linha["pago"] + linha["a_pagar"]
        linhas.append(linha)

    quantos = quantos or 0
    return {
        "linhas": linhas,
        "quantos": quantos,
        "pagina": pagina,
        "paginas": max((quantos + por_pagina - 1) // por_pagina, 1),
        "por_pagina": por_pagina,
        "total_pago": float(executado or 0),
        "total_a_pagar": float(aberto or 0),
        "total_encargo": float(encargo or 0),
        "total": float(executado or 0) + float(aberto or 0) + float(encargo or 0),
    }


def opcoes_do_analitico(f: Filtros) -> dict:
    """Grupos e categorias que existem DENTRO do recorte atual.

    Listar os 110 do plano de contas quando o filtro deixou 6 obriga a procurar
    entre opções que não trazem nada."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [PAG])
    grupos = [g for (g,) in consultar(
        f"SELECT DISTINCT COALESCE(NULLIF(grupo,''), '(sem grupo)') "
        f"  FROM fato{where} ORDER BY 1", params)]
    categorias = [c for (c,) in consultar(
        f"SELECT DISTINCT COALESCE(NULLIF(categoria,''), '(sem categoria)') "
        f"  FROM fato{where} ORDER BY 1", params)]
    return {"grupos": grupos, "categorias": categorias}


# ---------------------------------------------------------------------------
# Aportes e devoluções — o bloco que fica no fim do DRE
# ---------------------------------------------------------------------------
# Aporte NÃO entra no resultado: é dinheiro que o sócio ou o parceiro coloca (ou
# retira) da obra, não receita nem despesa. Mas na hora de avaliar uma obra ele é
# essencial — uma obra pode estar no vermelho e mesmo assim pagando as contas
# porque alguém injetou dinheiro. Por isso o bloco vive na mesma tela do DRE,
# com os mesmos filtros, mas separado da tabela.
#
# Tudo aqui é em CAIXA (só o que entrou ou saiu de fato): saldo de sócio é
# posição financeira, não competência.

# A classificação é a MESMA do `classificar_aporte` em Python — só que escrita em
# SQL, para o Postgres agrupar sem trazer linha nenhuma para cá. Gerar o SQL a
# partir do mesmo dicionário é o que garante que as duas não divirjam: mexer na
# lista de padrões conserta os dois lugares de uma vez.
_CAT_SIMPLES = ("translate(lower(COALESCE(categoria,'')), "
                "'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc')")


def _sql_tipo_aporte() -> str:
    from .sync.fato import TIPOS_APORTE, _APORTE_GENERICO, _sem_acento

    def ramo(padroes, tipo):
        # Os padrões são só letras e espaços, então a alternância do regex é
        # segura — e cabe numa linha, ao contrário de um OR por padrão.
        alternativas = "|".join(_sem_acento(p) for p in padroes)
        return f"WHEN {_CAT_SIMPLES} ~ '{alternativas}' THEN '{tipo}'"

    ramos = [ramo(padroes, tipo) for tipo, padroes in TIPOS_APORTE.items()]
    ramos.append(ramo(_APORTE_GENERICO, "Outros aportes"))
    return "CASE " + " ".join(ramos) + " END"


def _sql_tipos_no_saldo() -> str:
    from .sync.fato import TIPOS_NO_SALDO
    return ", ".join(f"'{t}'" for t in sorted(TIPOS_NO_SALDO))


TIPO_APORTE = _sql_tipo_aporte()
NO_SALDO = _sql_tipos_no_saldo()

# Nome de quem aportou e obra onde entrou — com o mesmo rótulo de "faltando" que
# a tela antiga usava, senão o vazio some no meio da tabela.
_SOCIO = "COALESCE(NULLIF(TRIM(razao_social),''), '(sem contraparte)')"
_OBRA = "COALESCE(NULLIF(TRIM(departamento),''), '(sem obra)')"

# Entrada é o que o sócio colocou; saída, o que voltou para ele.
_APORTADO = "SUM(CASE WHEN pago_recebido > 0 THEN pago_recebido ELSE 0 END)"
_DEVOLVIDO = "SUM(CASE WHEN pago_recebido < 0 THEN -pago_recebido ELSE 0 END)"


def _agregado_de_aporte(f: Filtros, chaves: list[str]) -> list[dict]:
    """Aportado / devolvido / saldo agrupado pelas colunas pedidas."""
    where, params = f.where(
        f"{PAGO} AND ({TIPO_APORTE}) IN ({NO_SALDO})")
    grupos = ", ".join(str(i + 1) for i in range(len(chaves)))
    sql = f"""
        SELECT {', '.join(chaves)}, {_APORTADO}, {_DEVOLVIDO}, COUNT(*)
          FROM fato{where}
         GROUP BY {grupos}"""
    n = len(chaves)
    saida = []
    for linha in consultar(sql, params):
        aportado, devolvido = float(linha[n] or 0), float(linha[n + 1] or 0)
        saida.append({"chaves": list(linha[:n]), "aportado": aportado,
                      "devolvido": devolvido, "saldo": aportado - devolvido,
                      "lancamentos": linha[n + 2]})
    saida.sort(key=lambda x: -x["saldo"])
    return saida


def aportes(f: Filtros) -> dict:
    """O bloco inteiro de aportes, do jeito que a tela antiga mostrava.

    Devolve os quatro recortes (sócio, obra, tipo, lançamentos), o quadro de
    dividendos — que fica FORA do saldo — e os três totais do topo."""
    por_socio = [dict(l, socio=l["chaves"][0])
                 for l in _agregado_de_aporte(f, [_SOCIO])]
    por_obra = [dict(l, obra=l["chaves"][0], socio=l["chaves"][1])
                for l in _agregado_de_aporte(f, [_OBRA, _SOCIO])]
    por_tipo = [dict(l, obra=l["chaves"][0], tipo=l["chaves"][1])
                for l in _agregado_de_aporte(f, [_OBRA, TIPO_APORTE])]

    # "Falta p/ igualar": a distância até o MAIOR aportador da mesma obra. É uma
    # referência de igualdade, não uma cobrança — o sistema não conhece a quota
    # que os sócios combinaram entre si.
    maior = {}
    for l in por_obra:
        maior[l["obra"]] = max(maior.get(l["obra"], l["saldo"]), l["saldo"])
    for l in por_obra:
        l["falta"] = maior[l["obra"]] - l["saldo"]

    total_ap = sum(l["aportado"] for l in por_socio)
    total_dev = sum(l["devolvido"] for l in por_socio)
    for l in por_socio:
        l["pct"] = (l["aportado"] / total_ap * 100) if total_ap else 0.0

    return {
        "por_socio": por_socio, "por_obra": por_obra, "por_tipo": por_tipo,
        "dividendos": dividendos_por_socio(f),
        "lancamentos": lancamentos_de_aporte(f),
        "aportado": total_ap, "devolvido": total_dev,
        "saldo": total_ap - total_dev,
        "tem_dados": bool(por_socio),
    }


def dividendos_por_socio(f: Filtros) -> list[dict]:
    """Dividendo é distribuição de LUCRO, não devolução de capital.

    Por isso ele não abate o saldo de aporte — abater faria parecer que o sócio
    retirou o que colocou, o que não aconteceu. Fica em quadro próprio."""
    where, params = f.where(f"{PAGO} AND ({TIPO_APORTE}) = 'Dividendos'")
    sql = f"""
        SELECT {_SOCIO},
               SUM(CASE WHEN pago_recebido > 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN pago_recebido < 0 THEN -pago_recebido ELSE 0 END),
               COUNT(*)
          FROM fato{where}
         GROUP BY 1"""
    saida = [{"socio": socio, "recebido": float(receb or 0), "pago": float(pago or 0),
              "liquido": float(pago or 0) - float(receb or 0), "lancamentos": quantos}
             for socio, receb, pago, quantos in consultar(sql, params)]
    saida.sort(key=lambda x: -x["liquido"])
    return saida


# Teto do detalhamento na tela. A versão antiga mostrava tudo porque já tinha a
# base inteira na memória — que é justamente o que não se faz mais aqui. Quem
# precisa da lista completa baixa o Excel, que sai sem teto.
LIMITE_LANCAMENTOS = 400


def lancamentos_de_aporte(f: Filtros, limite: int | None = LIMITE_LANCAMENTOS) -> dict:
    where, params = f.where(f"{PAGO} AND ({TIPO_APORTE}) IS NOT NULL")
    (quantos,) = consultar(f"SELECT COUNT(*) FROM fato{where}", params)[0]
    teto = f" LIMIT {int(limite)}" if limite else ""
    sql = f"""
        SELECT data, {_OBRA}, {_SOCIO}, {TIPO_APORTE},
               COALESCE(NULLIF(categoria,''), '(sem categoria)'),
               pago_recebido, COALESCE(conta_corrente,''),
               COALESCE(numero_documento,''), COALESCE(observacao,'')
          FROM fato{where}
         ORDER BY 2, 3, 1{teto}"""
    campos = ("data", "obra", "socio", "tipo", "categoria", "valor",
              "conta", "documento", "observacao")
    return {"quantos": quantos, "teto": limite,
            "linhas": [dict(zip(campos, linha)) for linha in consultar(sql, params)]}


def resultado_dividendos(f: Filtros) -> dict:
    """A ponte entre RESULTADO e DIVIDENDO, obra por obra.

    O resultado sai do DRE, em caixa (só o que foi pago ou recebido). O dividendo
    sai do fluxo — ele não é despesa, é distribuição do resultado já apurado. Os
    dois nunca se somam: um alimenta o outro.

        Disponível = resultado realizado − dividendos já pagos
    """
    where_r, params_r = f.where(f"analise = 'DRE' AND {PAGO}")
    resultado = {obra: float(valor or 0) for obra, valor in consultar(
        f"SELECT {_OBRA}, SUM(pago_recebido) FROM fato{where_r} GROUP BY 1",
        params_r)}

    where_d, params_d = f.where(
        f"{PAGO} AND ({TIPO_APORTE}) = 'Dividendos' AND pago_recebido < 0")
    pagos = {obra: float(valor or 0) for obra, valor in consultar(
        f"SELECT {_OBRA}, SUM(-pago_recebido) FROM fato{where_d} GROUP BY 1",
        params_d)}

    linhas = [{"obra": obra,
               "resultado": resultado.get(obra, 0.0),
               "dividendos": pagos.get(obra, 0.0),
               "disponivel": resultado.get(obra, 0.0) - pagos.get(obra, 0.0)}
              for obra in set(resultado) | set(pagos)]
    linhas.sort(key=lambda x: -x["resultado"])

    return {
        "linhas": linhas,
        "resultado": sum(l["resultado"] for l in linhas),
        "dividendos": sum(l["dividendos"] for l in linhas),
        "disponivel": sum(l["disponivel"] for l in linhas),
        "tem_dados": bool(linhas),
    }


def hipotese_de_distribuicao(por_socio: list[dict], disponivel: float) -> list[dict]:
    """Reparte o disponível na proporção do que cada um aportou.

    É simulação, e a tela diz isso: o sistema NÃO conhece a quota acordada entre
    os sócios. Serve para dar ordem de grandeza, não para fechar conta."""
    base = [l for l in por_socio if l["saldo"] > 0]
    total = sum(l["saldo"] for l in base)
    if not base or total <= 0 or disponivel <= 0:
        return []
    return [{"socio": l["socio"], "saldo": l["saldo"],
             "pct": l["saldo"] / total * 100,
             "valor": l["saldo"] / total * disponivel} for l in base]
