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

import datetime as dt

from .db import consultar

REC = "1. Contas a Receber"
PAG = "2. Contas a Pagar"

# "Foi pago?" nao e "tem data": um titulo em aberto tambem tem data (a de
# vencimento). Quem responde e o texto da situacao, como no painel antigo.
# "Foi pago" — a MESMA regra de sempre, só que calculada pelo banco na gravação
# em vez de linha a linha em toda consulta. A coluna `pago` nasce na migração
# 009 com exatamente esta expressão; trocar aqui não muda número nenhum, muda
# QUANDO a conta é feita.
#
# Medido em 17/09/2026, numa base de 144 mil linhas, na consulta do ano do DRE:
# 112 ms com a expressão, 29 ms com a coluna. Era 71% do tempo de cada tela.
PAGO = "pago"
# Imposto retido na fonte: o cliente reteve, nao virou caixa da BWS. Desde
# 23/09/2026 a linha segue o estado do titulo: realizado quando quitado, em
# aberto quando o titulo ainda esta em aberto. Nas telas de receita, "retido"
# e o COMPROMETIDO da linha (o que ja foi e o que ainda vai ser retido), e "a
# receber" e so o liquido — o retido nunca vai entrar na conta. Entra na
# receita bruta e sai da liquida.
RETIDO = "categoria ILIKE '%Retido%'"
# O que e MEDICAO na Receita de Obra: a receita de obras e o imposto retido dela.
# Rendimento, estorno e devolucao NAO sao medicao — o dono viu os dois juntos na
# mesma lista em 22/09/2026 e pediu que ficassem so no bloco "Outras receitas".
RECEITA_DE_OBRA = f"(categoria = 'Receita de Obras' OR {RETIDO})"

EXECUTADO = f"CASE WHEN {PAGO} THEN pago_recebido ELSE 0 END"
EM_ABERTO = "a_pagar_receber"
COMPROMETIDO = f"({EXECUTADO} + {EM_ABERTO})"


class Filtros:
    """Os filtros da barra lateral, traduzidos para um WHERE."""

    def __init__(self, anos=None, projetos=None, departamentos=None,
                 excluir_trf=True, contas=None):
        self.anos = [int(a) for a in (anos or [])]
        self.projetos = list(projetos or [])
        self.departamentos = list(departamentos or [])
        self.excluir_trf = bool(excluir_trf)
        # A conta corrente entrou em 21/09/2026, com o Extrato. Ela e tambem o
        # escopo de quem so pode ver certas contas — e por isso mora aqui, no
        # mesmo lugar que a obra: e o unico ponto por onde toda tela passa.
        self.contas = list(contas or [])

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
        if self.contas:
            partes.append("COALESCE(conta_corrente,'') = ANY(?)")
            params.append(self.contas)
        if self.excluir_trf:
            partes.append("analise <> 'TRF'")
        if extra:
            partes.append(extra)
            params.extend(params_extra or [])
        return (" WHERE " + " AND ".join(partes)) if partes else "", params

    def resumo(self) -> list[str]:
        """Descricao curta dos filtros ativos, para os chips no topo da tela."""
        chips = []
        if self.contas:
            chips.append("Conta: " + ", ".join(sorted(self.contas)))
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
# ---------------------------------------------------------------------------
# As listas que enfeitam toda tela — guardadas, nao refeitas a cada clique
# ---------------------------------------------------------------------------
# Anos, projetos, obras, grupos e categorias sao `SELECT DISTINCT` sobre o fato
# inteiro. Sao cinco varreduras de 185 mil linhas ANTES de a tela chegar na
# consulta que interessa, e elas rodavam em toda abertura de toda tela — para
# devolver sempre a mesma coisa, porque essas listas so mudam quando entra
# carga nova, uma vez por dia.
#
# Guardadas na memoria do processo, com o CARIMBO da ultima carga como chave:
# terminou uma atualizacao, o carimbo muda e tudo e refeito sozinho. Nao ha
# como servir lista velha depois de uma carga.
#
# Cabe na memoria sem susto (sao algumas centenas de textos curtos) e o gunicorn
# recicla o processo a cada ~150 requisicoes de qualquer jeito.
_LEMBRADO: dict[tuple, object] = {}
_CARIMBO_LEMBRADO = [""]


def _carimbo_da_base() -> str:
    """Muda quando uma atualizacao termina. E a chave de tudo que fica guardado.

    Perguntado UMA VEZ POR REQUISICAO. Antes ia ao banco a cada `_lembrando`, e
    no Analitico eram tres idas para trazer a mesma resposta — 3 das 8 consultas
    da tela eram a mesma pergunta. A consulta e barata, mas cada uma e uma
    viagem de rede ate o banco, que fica em outro servico.

    Guardado no `g` do Flask, que morre junto com a requisicao: nao ha risco de
    carimbo velho sobrevivendo a uma carga nova. FORA de requisicao — na carga,
    que roda em processo separado — nao guarda nada e pergunta sempre, porque um
    processo que vive horas nao pode carregar uma resposta congelada."""
    def _consultar():
        linhas = consultar("SELECT MAX(fim) FROM execucoes WHERE fim IS NOT NULL")
        return str(linhas[0][0]) if linhas else "sem-carga"

    try:
        from flask import g, has_request_context
        if not has_request_context():
            return _consultar()
    except Exception:                 # sem Flask por perto: pergunta e pronto
        return _consultar()

    if not hasattr(g, "_painel_carimbo"):
        g._painel_carimbo = _consultar()
    return g._painel_carimbo


def esquecer_listas() -> None:
    """Joga fora o que esta guardado. Usado pelos testes e depois de uma carga."""
    _LEMBRADO.clear()
    _CARIMBO_LEMBRADO[0] = ""


def _lembrando(chave: tuple, calcular):
    """Devolve o que ja foi calculado para este carimbo, ou calcula e guarda."""
    carimbo = _carimbo_da_base()
    if carimbo != _CARIMBO_LEMBRADO[0]:
        # entrou carga nova: tudo que estava guardado fala da base velha
        _LEMBRADO.clear()
        _CARIMBO_LEMBRADO[0] = carimbo
    if chave not in _LEMBRADO:
        _LEMBRADO[chave] = calcular()
    return _LEMBRADO[chave]


def opcoes_de_filtro() -> dict:
    """Anos, projetos e obras que existem na base. Sao valores distintos —
    algumas centenas de linhas, nao a base inteira."""
    def calcular():
        anos = [a for (a,) in consultar(
            "SELECT DISTINCT ano FROM fato WHERE ano BETWEEN 2015 AND 2100 ORDER BY ano DESC")]
        projetos = [p for (p,) in consultar(
            "SELECT DISTINCT projeto FROM fato WHERE COALESCE(projeto,'') <> '' ORDER BY projeto")]
        obras = [d for (d,) in consultar(
            "SELECT DISTINCT departamento FROM fato "
            " WHERE COALESCE(departamento,'') <> '' ORDER BY departamento")]
        contas = [c for (c,) in consultar(
            "SELECT DISTINCT conta_corrente FROM fato "
            " WHERE COALESCE(conta_corrente,'') <> '' ORDER BY conta_corrente")]
        return {"anos": anos, "projetos": projetos, "obras": obras,
                "contas": contas}

    return _lembrando(("opcoes_de_filtro",), calcular)


def atualizado_em(so_concluidas: bool = False) -> dict | None:
    """Quando a base foi atualizada pela ultima vez, e como foi.

    Com `so_concluidas`, ignora as execucoes que MORRERAM no meio — as que o
    faxineiro de orfas fechou, e que se reconhecem pela etapa preenchida
    (quem chega ao fim sozinho zera a etapa). Serve para a tela de
    Configuracoes: quando a caixa vermelha de interrupcao ja esta contando essa
    historia, repetir a mesma coisa aqui em cima so confunde — a pergunta que
    esta linha responde passa a ser "e quando a base foi atualizada de
    verdade pela ultima vez?"."""
    condicao = "fim IS NOT NULL" + (" AND etapa IS NULL" if so_concluidas else "")
    linha = consultar(
        "SELECT tipo, disparo, inicio, fim, ok, mensagem, linhas_fato "
        f"  FROM execucoes WHERE {condicao} ORDER BY inicio DESC LIMIT 1")
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
    mostrar tudo zerado como se fosse a verdade.

    `LIMIT 1` e nao `COUNT(*)`: a pergunta e "existe alguma linha?", e contar as
    185 mil para responder isso custava uma varredura da tabela inteira em TODA
    abertura de TODA tela."""
    return not consultar("SELECT 1 FROM fato LIMIT 1")


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
          SUM(CASE WHEN tipo = ? THEN {COMPROMETIDO_COM_ENCARGO} ELSE 0 END),
          SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {EXECUTADO}    ELSE 0 END),
          SUM(CASE WHEN tipo = ? THEN {EXECUTADO_COM_ENCARGO} ELSE 0 END)
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
          SUM(CASE WHEN tipo = ? THEN {COMPROMETIDO_COM_ENCARGO} ELSE 0 END)
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
        SELECT SUM(CASE WHEN {MOVIMENTO_DE_CAIXA} > 0
                        THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END),
               SUM(CASE WHEN {MOVIMENTO_DE_CAIXA} < 0
                        THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END)
          FROM fato{where}"""
    entradas, saidas = consultar(sql, params)[0]
    entradas, saidas = float(entradas or 0), float(saidas or 0)
    return {"entradas": entradas, "saidas": saidas, "geracao": entradas + saidas}


def caixa_por_ano(f: Filtros) -> list[dict]:
    """Geracao de caixa por ano, com o acumulado."""
    where, params = f.where(
        f"{PAGO} AND NOT (tipo = ? AND {RETIDO}) AND ano BETWEEN 2015 AND 2100", [REC])
    sql = (f"SELECT ano, SUM({MOVIMENTO_DE_CAIXA}) FROM fato{where} "
           f"GROUP BY ano ORDER BY ano")
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
# Como se chama o titulo que NAO foi apropriado a nenhuma obra. Ate 08/09/2026
# eram cinco literais espalhados dizendo "(sem obra)"; o painel Streamlit passou
# a usar "(nao apropriado)", que e mais honesto — o titulo existe, o que falta e
# a apropriacao — e e por esse rotulo que se procura o que precisa ser saneado.
# Um lugar so: o Explorador filtra por ele, e dois nomes diferentes para a mesma
# coisa fariam a busca nao achar nada.
SEM_OBRA = "(não apropriado)"
# Mesmo padrao do SEM_OBRA: um rotulo so, em toda a tela, para o que
# nao tem fornecedor — senao filtrar por ele vira adivinhacao.
SEM_FORNECEDOR = "(sem fornecedor)"
# Mesmo padrao: um rotulo so para o que nao tem categoria. Sem ele, "achar o que
# esta sem classificacao" — que e o trabalho do Explorador — dependia de sorte.
SEM_CATEGORIA = "(sem categoria)"
OBRA_OU_SEM = f"COALESCE(NULLIF(TRIM(departamento),''), '{SEM_OBRA}')"

ENCARGO = f"CASE WHEN {PAGO} THEN (juros + multa) ELSE 0 END"

# O dinheiro que ANDOU numa linha: o principal mais os encargos pagos. Juros
# pago sai da conta corrente como qualquer outro pagamento.
MOVIMENTO_DE_CAIXA = f"(pago_recebido + {ENCARGO})"

# O VALOR DE FATO DE UMA DESPESA inclui os encargos. Use estes dois, e nao o
# COMPROMETIDO/EXECUTADO crus, em qualquer soma de DESPESA.
#
# Por que existem: ate 08/09/2026 so o DRE e o Analitico somavam juros e multa.
# O resto do painel — Visao Geral, Resultado por Obra, Comprometido x Executado,
# as telas de caixa e a Prestacao de Contas — somava so o principal, e o
# resultado saia MAIOR do que e. O dono viu numa obra: R$ 931.718,04 na Visao
# Geral contra R$ 888.419,91 no DRE, diferenca de R$ 43.298,13 — os juros, ao
# centavo. Nao era defeito da conversao: o Streamlit original fazia igual, e ele
# decidiu que juros e multa sao despesa em todo lugar.
#
# So valem para tipo = PAG. Do lado da RECEITA, juros recebido e receita
# financeira — outra conversa, e o dono nao pediu.
COMPROMETIDO_COM_ENCARGO = f"({COMPROMETIDO} + {ENCARGO})"
EXECUTADO_COM_ENCARGO = f"({EXECUTADO} + {ENCARGO})"


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
        SELECT {OBRA_OU_SEM},
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM(CASE WHEN     ({RETIDO}) THEN {COMPROMETIDO} ELSE 0 END),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EM_ABERTO} ELSE 0 END)
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
               SUM({EXECUTADO_COM_ENCARGO}), SUM({EM_ABERTO}), COUNT(*)
          FROM fato{where} GROUP BY 1
         ORDER BY SUM({COMPROMETIDO_COM_ENCARGO}) ASC LIMIT {int(limite)}"""
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
               SUM(CASE WHEN {MOVIMENTO_DE_CAIXA} > 0
                        THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END),
               SUM(CASE WHEN {MOVIMENTO_DE_CAIXA} < 0
                        THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END)
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
    valor_desp = _medida_de_despesa(medida)
    resultado = (f"SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {valor} ELSE 0 END) "
                 f"+ SUM(CASE WHEN tipo = ? THEN {valor_desp} ELSE 0 END)")
    sql = f"""
        SELECT COALESCE(NULLIF({coluna},''), '(sem {coluna})'),
               SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ? THEN {valor_desp} ELSE 0 END)
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
    # do lado de PAGAR, o executado inclui os encargos: juros pago e dinheiro
    # que ja saiu da obra. Do lado de RECEBER nao — juros recebido e receita
    # financeira, outra conversa.
    executado = EXECUTADO_COM_ENCARGO if alvo == PAG else EXECUTADO
    comprometido = COMPROMETIDO_COM_ENCARGO if alvo == PAG else COMPROMETIDO
    where, params = f.where("tipo = ?", [alvo])
    sql = f"""
        SELECT COALESCE(NULLIF({coluna},''), '(sem {coluna})'),
               SUM({executado}), SUM({EM_ABERTO})
          FROM fato{where} GROUP BY 1
         ORDER BY ABS(SUM({comprometido})) DESC LIMIT {int(limite)}"""
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
               {OBRA_OU_SEM},
               SUM({MOVIMENTO_DE_CAIXA})
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
    # Lembrado até a próxima carga: desde 23/09/2026 quem tem acesso por
    # PROJETO passa por aqui a cada pedido — e varrer o fato a cada clique
    # seria a tela mais lenta do painel só para saber de quem é a obra.
    def calcular():
        sql = """
            SELECT departamento, projeto, COUNT(*) AS quantas
              FROM fato
             WHERE COALESCE(departamento,'') <> ''
             GROUP BY 1, 2 ORDER BY 1, 3 DESC"""
        mapa = {}
        for obra, projeto, _quantas in consultar(sql):
            if obra not in mapa:                   # o primeiro é o mais frequente
                mapa[obra] = (projeto or "").strip()
        return mapa

    return dict(_lembrando(("obra_para_projeto",), calcular))


def obras_dos_projetos(projetos) -> list[str]:
    """Todas as obras que pertencem a estes projetos, hoje — inclusive as que
    entraram na base depois de o acesso ter sido dado."""
    alvo = {str(p).strip() for p in (projetos or ()) if str(p).strip()}
    if not alvo:
        return []
    return sorted(o for o, p in obra_para_projeto().items() if p in alvo)


# ---------------------------------------------------------------------------
# Receita de Obra — por medição
# ---------------------------------------------------------------------------
# Uma medição é o que a obra faturou num período. No OMIE ela vira vários
# títulos (as parcelas), sem número de documento que os ligue — o elo é a
# observação. A chave que junta tudo isso é gravada na própria linha do fato
# (migração 004), então quem agrupa é o banco.

def medicoes(f: Filtros, visao: str = "todas", limite: int = 300) -> list[dict]:
    """A receita de obra, UM TÍTULO POR LINHA, da mais recente para a mais antiga.

    Até 22/09/2026 a linha era a MEDIÇÃO — todos os títulos cuja observação diz
    a mesma medição da mesma obra, somados. O dono conferiu uma linha contra o
    OMIE, achou um título oito vezes menor, e decidiu: *"não fica legal
    agrupado, confunde, tem que separar mesmo os títulos"*. A medição continua
    escrita ao lado, como rótulo; o número é o do título.

    Bruto = o que já entrou + o que o cliente reteve + o que falta receber.
    `visao`: 'todas', 'a_receber' (só com saldo) ou 'quitadas'."""
    where, params = f.where(f"analise = 'DRE' AND tipo = ? AND {RECEITA_DE_OBRA}", [REC])
    tendo = {
        "a_receber": "HAVING ABS(SUM(a_pagar_receber)) > 0.005",
        "quitadas": "HAVING ABS(SUM(a_pagar_receber)) <= 0.005",
    }.get(visao, "")
    sql = f"""
        SELECT COALESCE(NULLIF(MAX(medicao_rotulo),''), '(sem medição)'),
               MAX(razao_social), MAX(departamento), MAX(projeto),
               MAX(numero_documento), MAX(link), MAX(data),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM(CASE WHEN     ({RETIDO}) THEN {COMPROMETIDO} ELSE 0 END),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EM_ABERTO} ELSE 0 END),
               codigo_lancamento
          FROM fato{where}
         GROUP BY codigo_lancamento {tendo}
         ORDER BY MAX(data) DESC NULLS LAST, codigo_lancamento LIMIT {int(limite)}"""
    saida = []
    for (rotulo, cliente, obra, projeto, documento, link, data,
         recebido, retido, a_receber, codigo) in consultar(sql, params):
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
            "codigo": codigo, "medicao": rotulo, "cliente": cliente or "",
            "obra": obra or "", "projeto": projeto or "",
            "documento": documento or "", "link": link or "", "data": data,
            "recebido": recebido, "retido": retido, "a_receber": a_receber,
            "bruto": bruto, "situacao": situacao,
        })
    return saida


def total_das_medicoes(f: Filtros, visao: str = "todas") -> dict:
    """Os totais da receita de obra — somados pelo banco, não pela lista.

    A tela mostra os 300 títulos mais recentes; o total tem de ser de TODOS,
    senão o rodapé não bate com o DRE. `quantas` conta títulos."""
    where, params = f.where(f"analise = 'DRE' AND tipo = ? AND {RECEITA_DE_OBRA}", [REC])
    tendo = {
        "a_receber": "HAVING ABS(SUM(a_pagar_receber)) > 0.005",
        "quitadas": "HAVING ABS(SUM(a_pagar_receber)) <= 0.005",
    }.get(visao, "")
    sql = f"""
        SELECT COUNT(*), SUM(recebido), SUM(retido), SUM(aberto)
          FROM (
            SELECT SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END) AS recebido,
                   SUM(CASE WHEN     ({RETIDO}) THEN {COMPROMETIDO} ELSE 0 END) AS retido,
                   SUM(CASE WHEN NOT ({RETIDO}) THEN {EM_ABERTO} ELSE 0 END) AS aberto
              FROM fato{where}
             GROUP BY codigo_lancamento {tendo}
          ) AS por_titulo"""
    quantas, recebido, retido, aberto = consultar(sql, params)[0]
    recebido, retido = float(recebido or 0), float(retido or 0)
    aberto = float(aberto or 0)
    return {"quantas": quantas or 0, "recebido": recebido, "retido": retido,
            "a_receber": aberto, "bruto": recebido + retido + aberto}


def recebimentos_da_medicao(medicao: str, limite: int = 200) -> list[dict]:
    """Cada entrada de dinheiro que quitou uma medição, com data e valor exatos.

    Vem da outra tabela (`fato_recebimentos`), que abre por movimento: um título
    recebido em três parcelas aparece aqui como três linhas."""
    return _recebimentos("medicao = ?", [medicao], limite)


def recebimentos_do_titulo(codigo, limite: int = 200) -> list[dict]:
    """O mesmo, para UM título — a linha da Receita de Obra é o título."""
    return _recebimentos("codigo_lancamento = ?", [int(codigo)], limite)


def _recebimentos(condicao: str, params, limite: int) -> list[dict]:
    sql = f"""
        SELECT data, valor, juros, multa, desconto, conta_corrente, parcela,
               origem, numero_documento
          FROM fato_recebimentos
         WHERE {condicao}
         ORDER BY data NULLS LAST, id
         LIMIT {int(limite)}"""
    campos = ("data", "valor", "juros", "multa", "desconto", "conta_corrente",
              "parcela", "origem", "numero_documento")
    return [dict(zip(campos, linha)) for linha in consultar(sql, params)]


def titulos_da_medicao(medicao: str, limite: int = 200) -> list[dict]:
    """Os títulos do OMIE que compõem UMA medição, um por linha.

    É o que permite conferir a linha da Receita de Obra contra o OMIE: a
    medição junta os títulos cuja observação diz a mesma medição da mesma obra
    (várias notas, principal e reajuste, fontes de recurso diferentes). Quando
    a observação está errada num título, ele cai na medição errada — e é aqui
    que isso aparece, com o número para achar o título lá."""
    return _titulos_de_receita("medicao_rotulo = ?", [medicao], limite)


def titulo_da_receita(codigo, f: "Filtros | None" = None) -> dict | None:
    """UM título de receita, ou None quando ele não existe — ou não está no
    recorte de quem pergunta. O recorte entra aqui de propósito: é o que impede
    alguém preso a uma obra de abrir o título de outra pelo número."""
    where, params = (f or Filtros()).where("codigo_lancamento = ?", [int(codigo)])
    linhas = _titulos_de_receita(where[len(" WHERE "):], params, 1)
    return linhas[0] if linhas else None


def _titulos_de_receita(condicao: str, params, limite: int) -> list[dict]:
    sql = f"""
        SELECT codigo_lancamento, MAX(numero_documento), MAX(razao_social),
               MAX(departamento), MAX(data), MAX(observacao), MAX(link),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EXECUTADO} ELSE 0 END),
               SUM(CASE WHEN     ({RETIDO}) THEN {COMPROMETIDO} ELSE 0 END),
               SUM(CASE WHEN NOT ({RETIDO}) THEN {EM_ABERTO} ELSE 0 END),
               MAX(medicao_rotulo)
          FROM fato
         WHERE analise = 'DRE' AND tipo = ? AND {condicao}
         GROUP BY codigo_lancamento
         ORDER BY MAX(data) NULLS LAST, codigo_lancamento
         LIMIT {int(limite)}"""
    campos = ("codigo", "documento", "cliente", "obra", "data", "observacao",
              "link", "recebido", "retido", "a_receber", "medicao")
    saida = []
    for linha in consultar(sql, [REC] + list(params)):
        d = dict(zip(campos, linha))
        for campo in ("recebido", "retido", "a_receber"):
            d[campo] = float(d[campo] or 0)
        d["bruto"] = d["recebido"] + d["retido"] + d["a_receber"]
        d["documento"] = d["documento"] or ""
        saida.append(d)
    return saida


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


def _medida_de_despesa(medida: str) -> str:
    """A mesma medida, com os encargos dentro. Use nas somas de tipo = PAG."""
    return (EXECUTADO_COM_ENCARGO if medida == "executado"
            else COMPROMETIDO_COM_ENCARGO)


def apuracao_por_obra_mes(medida: str = "comprometido") -> list[dict]:
    """Receita líquida, retenções e despesas de cada obra, mês a mês.

    É a base de tudo na prestação de contas. Umas poucas milhares de linhas
    (obras × meses), não a base inteira."""
    valor = _medida(medida)
    valor_desp = _medida_de_despesa(medida)
    sql = f"""
        SELECT COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'),
               {OBRA_OU_SEM},
               COALESCE(NULLIF(projeto,''), ''),
               SUM(CASE WHEN tipo = ? AND NOT ({RETIDO}) THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ? AND     ({RETIDO}) THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ? THEN {valor_desp} ELSE 0 END)
          FROM fato
         WHERE analise = 'DRE'
         GROUP BY 1, 2, 3
        HAVING ABS(SUM({valor})) + ABS(SUM({ENCARGO})) > 0.005
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
               {OBRA_OU_SEM},
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
    valor_desp = _medida_de_despesa(medida)
    sql = f"""
        SELECT COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'), departamento,
               TRIM(COALESCE(grupo,'')), TRIM(COALESCE(categoria,'')),
               SUM({valor_desp})
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
        valor_desp = COMPROMETIDO_COM_ENCARGO
        extra = "data IS NOT NULL"
    else:
        valor = "pago_recebido"
        valor_desp = MOVIMENTO_DE_CAIXA
        extra = f"{PAGO} AND data IS NOT NULL"

    where, params = f.where(f"analise = 'DRE' AND {extra} AND NOT (tipo = ? AND {RETIDO})",
                            [REC])
    sql = f"""
        SELECT to_char(data, 'YYYY-MM'),
               SUM(CASE WHEN tipo = ? THEN {valor} ELSE 0 END),
               SUM(CASE WHEN tipo = ? THEN {valor_desp} ELSE 0 END)
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
    "vencimento": "data_vencimento DESC NULLS LAST",
    # o que mais demorou a ser pago primeiro: e a pergunta que a coluna de
    # atraso existe para responder
    "atraso": "(data_pagamento - data_vencimento) DESC NULLS LAST",
    "credor": "razao_social ASC",
    "categoria": "categoria ASC",
}


# Por qual data a faixa filtra. A de sempre continua sendo a padrao: e a que o
# DRE e o fluxo de caixa usam, e trocar isso por baixo mudaria o significado da
# tela sem ninguem pedir.
BASES_DE_DATA = {
    "movimento": ("data", "Pagamento ou vencimento"),
    "vencimento": ("data_vencimento", "Só o vencimento"),
    "pagamento": ("data_pagamento", "Só o pagamento"),
}


def duas_datas_prontas() -> bool:
    """As colunas de vencimento e pagamento ja foram preenchidas?

    Elas nascem vazias: a migracao cria a coluna, quem preenche e a proxima
    atualizacao da base. Enquanto isso a tela precisa DIZER isso, em vez de
    mostrar uma coluna de travessoes e deixar a pessoa achar que a informacao
    nao existe."""
    def calcular():
        return bool(consultar(
            "SELECT 1 FROM fato WHERE data_vencimento IS NOT NULL LIMIT 1"))

    # so muda quando o fato e refeito, e ai o carimbo muda junto
    return _lembrando(("duas_datas_prontas",), calcular)


def extrato_da_conta(f: Filtros, busca="", categoria="", de="", ate="",
                     ordem="data", pagina=1, por_pagina=200) -> dict:
    """O EXTRATO: o que entrou e o que saiu de uma conta corrente, por data.

    Pedido do dono em 21/09/2026, para poder conferir lado a lado com o OMIE:
    *"seria até similar com o relatório analítico. Só que ao invés de ser o da
    obra, seria o da conta corrente (…) e ali só iriam poder ser vistos os
    lançamentos que aconteceram na conta corrente."*

    TRES DIFERENCAS que separam isto do Analitico, e cada uma e o ponto:

    1. **so o que virou dinheiro.** Titulo em aberto nao entra — extrato e
       caixa, nao compromisso. Por isso nao ha coluna de vencimento: ela nao
       significa nada aqui, e o dono disse isso com todas as letras;
    2. **as duas pontas juntas**, entrada e saida, na ordem da data — que e
       como o extrato do banco mostra e como da para comparar;
    3. **NAO filtra por DRE.** Tarifa bancaria e rendimento ficam de fora do
       resultado porque o plano financeiro do OMIE nao lhes da conta de DRE
       (ver a conversa de 21/09). No extrato eles APARECEM, porque saiu e
       entrou dinheiro de verdade — que e justamente o que o dono nao estava
       conseguindo achar.
    """
    condicoes, extras = [PAGO, "ABS(pago_recebido) > 0.005"], []

    if de:
        condicoes.append("data >= CAST(? AS DATE)")
        extras.append(de)
    if ate:
        condicoes.append("data <= CAST(? AS DATE)")
        extras.append(ate)
    if categoria:
        condicoes.append("COALESCE(NULLIF(categoria,''), '(sem categoria)') = ?")
        extras.append(categoria)
    if busca:
        # o mesmo campo unico do Analitico: nome, documento ou observacao
        condicoes.append("(razao_social ILIKE ? OR numero_documento ILIKE ?"
                         " OR observacao ILIKE ? OR cnpj_cpf ILIKE ?)")
        alvo = f"%{busca}%"
        extras.extend([alvo, alvo, alvo, alvo])

    # O extrato mostra o dinheiro andando entre contas tambem: transferencia
    # SAI do resultado, mas nao sai do extrato — ela aconteceu na conta.
    sem_corte = Filtros(anos=f.anos, projetos=f.projetos,
                        departamentos=f.departamentos, contas=f.contas,
                        excluir_trf=False)
    where, params = sem_corte.where(" AND ".join(condicoes), extras)

    (linhas_total, entradas, saidas) = consultar(
        f"""SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN pago_recebido > 0
                                     THEN pago_recebido ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN pago_recebido < 0
                                     THEN -pago_recebido ELSE 0 END), 0)
              FROM fato{where}""", params)[0]

    ordens = {"data": "data, codigo_lancamento",
              "data_desc": "data DESC, codigo_lancamento",
              "valor": "ABS(pago_recebido) DESC"}
    ordenacao = ordens.get(ordem, ordens["data"])
    pagina = max(1, int(pagina or 1))
    salto = (pagina - 1) * por_pagina

    campos = ("data", "codigo", "razao_social", "cnpj", "conta", "categoria",
              "obra", "documento", "observacao", "valor", "link")
    linhas = [dict(zip(campos, linha)) for linha in consultar(
        f"""SELECT data, codigo_lancamento, razao_social, cnpj_cpf,
                   COALESCE(conta_corrente,'(sem conta)'),
                   COALESCE(NULLIF(categoria,''), '(sem categoria)'),
                   {OBRA_OU_SEM}, numero_documento, observacao,
                   pago_recebido, link
              FROM fato{where}
             ORDER BY {ordenacao}
             LIMIT {int(por_pagina)} OFFSET {int(salto)}""", params)]
    for l in linhas:
        l["valor"] = float(l["valor"] or 0)

    return {"linhas": linhas, "quantos": linhas_total or 0,
            "entradas": float(entradas or 0), "saidas": float(saidas or 0),
            "liquido": float(entradas or 0) - float(saidas or 0),
            "pagina": pagina, "por_pagina": por_pagina,
            "paginas": max(1, -(-(linhas_total or 0) // por_pagina))}


# ---------------------------------------------------------------------------
# O Calendário — o caixa dia a dia, num mês
# ---------------------------------------------------------------------------
# Pedido do dono em 22/09/2026: "um calendário grande na tela (...) cada dia
# tem um resuminho de valores pagos ou recebidos (...) você bate o olho e já vê
# toda a evolução dia após dia. E quando clicar no dia, ele expande com o
# detalhamento: fornecedor, categoria, valor — e dali abrir o Pipefy."
#
# É CAIXA, e a mesma régua do Fluxo de Caixa: só o que foi pago ou recebido de
# fato, pela data em que aconteceu, com os encargos pagos, sem as retenções de
# receita (o cliente as reteve, nunca passaram pela conta). O mês do calendário
# fecha com a linha do mesmo mês no Fluxo de Caixa — é o que permite conferir.
TIPOS_DO_CALENDARIO = {
    "": "Recebido, pago e a pagar",
    "recebido": "Só recebimentos",
    "pago": "Só pagamentos",
    "a_pagar": "Só a pagar (em aberto, pelo vencimento)",
}

# DRE ou fluxo (dono, 23/09/2026): "é importante poder ver só os lançamentos
# de fluxo, ou só os de DRE — tudo junto atrapalha". DRE é o que entra no
# resultado; fluxo é o resto que mexe no caixa (empréstimo, aporte, dividendo,
# aplicação — e transferência, quando a barra lateral a inclui).
ANALISES_DO_CALENDARIO = {
    "": "DRE e fluxo",
    "dre": "Só DRE (entra no resultado)",
    "fluxo": "Só fluxo (fora do resultado)",
}
_CONDICAO_DA_ANALISE = {
    "dre": "analise = 'DRE'",
    "fluxo": "COALESCE(analise,'') <> 'DRE'",
}

# O terceiro numero de cada dia (dono, 23/09/2026): "o que esta a pagar de
# cada dia, em laranja — num dia que ja passou, venceu; num dia que nao
# chegou, esta a vencer". E o titulo em aberto, no dia do VENCIMENTO. Quando
# o OMIE nao trouxe o vencimento em separado, vale a data de sempre — que, no
# titulo em aberto, ja e o vencimento.
DIA_DO_VENCIMENTO = "COALESCE(data_vencimento, data)"
A_PAGAR_EM_ABERTO = f"tipo = '{PAG}' AND ABS({EM_ABERTO}) > 0.005"


def _condicoes_do_calendario(tipo="", grupo="", categoria="", busca="",
                             em_aberto: bool = False, analise=""):
    """O WHERE comum ao mes e ao detalhe do dia.

    `em_aberto=False` e o CAIXA (pago e recebido, pela data em que
    aconteceu); `em_aberto=True` e o A PAGAR (titulo em aberto, pelo dia do
    vencimento). Os filtros proprios da tela valem nos dois."""
    if em_aberto:
        condicoes = [A_PAGAR_EM_ABERTO, f"{DIA_DO_VENCIMENTO} IS NOT NULL"]
        extras: list = []
    else:
        condicoes = [PAGO, "data IS NOT NULL", f"ABS({MOVIMENTO_DE_CAIXA}) > 0.005",
                     f"NOT (tipo = ? AND {RETIDO})"]
        extras = [REC]
        if tipo == "recebido":
            condicoes.append(f"{MOVIMENTO_DE_CAIXA} > 0")
        elif tipo == "pago":
            condicoes.append(f"{MOVIMENTO_DE_CAIXA} < 0")
    if analise in _CONDICAO_DA_ANALISE:
        condicoes.append(_CONDICAO_DA_ANALISE[analise])
    if grupo:
        condicoes.append("COALESCE(NULLIF(grupo,''), '(sem grupo)') = ?")
        extras.append(grupo)
    if categoria:
        condicoes.append("COALESCE(NULLIF(categoria,''), '(sem categoria)') = ?")
        extras.append(categoria)
    if busca:
        condicoes.append("(razao_social ILIKE ? OR categoria ILIKE ?"
                         " OR numero_documento ILIKE ? OR observacao ILIKE ?)")
        extras.extend([f"%{busca}%"] * 4)
    return condicoes, extras


def _dia_vazio() -> dict:
    return {"entradas": 0.0, "saidas": 0.0, "liquido": 0.0, "quantos": 0,
            "a_pagar": 0.0, "quantos_a_pagar": 0, "vencido": False}


def calendario_do_mes(f: Filtros, ano: int, mes: int, *, tipo="", grupo="",
                      categoria="", busca="", analise="", hoje=None) -> dict:
    """Entradas, saídas, o que está a pagar e quantos lançamentos em cada dia
    do mês — e os totais.

    Duas consultas agrupadas pelo dia (no máximo 31 linhas cada): o caixa,
    pela data em que aconteceu, e o A PAGAR em aberto, pelo vencimento. O que
    venceu antes de `hoje` é "vencido"; o resto, "a vencer"."""
    import datetime as _dt
    hoje = hoje or _dt.date.today()
    inicio = _dt.date(int(ano), int(mes), 1)
    fim = (_dt.date(inicio.year + (inicio.month == 12),
                    1 if inicio.month == 12 else inicio.month + 1, 1))
    dias: dict = {}
    entradas = saidas = 0.0
    quantos = 0
    maior_entrada = maior_saida = None

    if tipo != "a_pagar":
        condicoes, extras = _condicoes_do_calendario(
            tipo, grupo, categoria, busca, analise=analise)
        condicoes += ["data >= CAST(? AS DATE)", "data < CAST(? AS DATE)"]
        extras += [inicio.isoformat(), fim.isoformat()]
        where, params = f.where(" AND ".join(condicoes), extras)
        sql = f"""
            SELECT data AS dia_do_calendario,
                   SUM(CASE WHEN {MOVIMENTO_DE_CAIXA} > 0
                            THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END),
                   SUM(CASE WHEN {MOVIMENTO_DE_CAIXA} < 0
                            THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END),
                   COUNT(*)
              FROM fato{where}
             GROUP BY 1 ORDER BY 1"""
        for dia, entrou, saiu, n in consultar(sql, params):
            entrou, saiu = float(entrou or 0), float(saiu or 0)
            d = dias.setdefault(dia, _dia_vazio())
            d.update({"entradas": entrou, "saidas": saiu,
                      "liquido": entrou + saiu, "quantos": int(n or 0)})
            entradas += entrou
            saidas += saiu
            quantos += int(n or 0)
            if entrou > 0 and (maior_entrada is None or entrou > maior_entrada[1]):
                maior_entrada = (dia, entrou)
            if saiu < 0 and (maior_saida is None or saiu < maior_saida[1]):
                maior_saida = (dia, saiu)

    a_pagar = vencido = a_vencer = 0.0
    quantos_a_pagar = quantos_vencidos = quantos_a_vencer = 0
    maior_a_pagar = None
    if tipo in ("", "a_pagar"):
        condicoes, extras = _condicoes_do_calendario(
            tipo, grupo, categoria, busca, em_aberto=True, analise=analise)
        condicoes += [f"{DIA_DO_VENCIMENTO} >= CAST(? AS DATE)",
                      f"{DIA_DO_VENCIMENTO} < CAST(? AS DATE)"]
        extras += [inicio.isoformat(), fim.isoformat()]
        where, params = f.where(" AND ".join(condicoes), extras)
        sql = f"""
            SELECT {DIA_DO_VENCIMENTO} AS dia_a_pagar, SUM({EM_ABERTO}), COUNT(*)
              FROM fato{where}
             GROUP BY 1 ORDER BY 1"""
        for dia, aberto, n in consultar(sql, params):
            aberto = float(aberto or 0)
            d = dias.setdefault(dia, _dia_vazio())
            d["a_pagar"] = aberto
            d["quantos_a_pagar"] = int(n or 0)
            d["vencido"] = dia < hoje
            a_pagar += aberto
            quantos_a_pagar += int(n or 0)
            if dia < hoje:
                vencido += aberto
                quantos_vencidos += int(n or 0)
            else:
                a_vencer += aberto
                quantos_a_vencer += int(n or 0)
            if aberto < 0 and (maior_a_pagar is None or aberto < maior_a_pagar[1]):
                maior_a_pagar = (dia, aberto)

    return {
        "inicio": inicio, "hoje": hoje, "dias": dict(sorted(dias.items())),
        "entradas": entradas, "saidas": saidas, "liquido": entradas + saidas,
        "quantos": quantos,
        "dias_com_movimento": sum(1 for d in dias.values() if d["quantos"]),
        "maior_entrada": maior_entrada, "maior_saida": maior_saida,
        "a_pagar": a_pagar, "a_pagar_vencido": vencido, "a_pagar_a_vencer": a_vencer,
        "quantos_a_pagar": quantos_a_pagar, "maior_a_pagar": maior_a_pagar,
        "quantos_vencidos": quantos_vencidos, "quantos_a_vencer": quantos_a_vencer,
    }


def lancamentos_do_dia(f: Filtros, dia: str, *, tipo="", grupo="", categoria="",
                       busca="", analise="", limite: int = 500, hoje=None) -> list[dict]:
    """O que entrou, o que saiu e o que está a pagar num dia, um lançamento
    por linha — o detalhe que abre ao clicar no dia. Com os mesmos filtros do
    calendário, para o total do detalhe fechar com o número do quadradinho."""
    import datetime as _dt
    hoje = hoje or _dt.date.today()
    campos = ("data", "codigo", "tipo", "razao_social", "cnpj", "grupo",
              "categoria", "obra", "projeto", "documento", "observacao",
              "conta", "valor", "encargo", "link", "analise")
    linhas: list[dict] = []
    if tipo != "a_pagar":
        condicoes, extras = _condicoes_do_calendario(
            tipo, grupo, categoria, busca, analise=analise)
        condicoes.append("data = CAST(? AS DATE)")
        extras.append(dia)
        where, params = f.where(" AND ".join(condicoes), extras)
        for bruta in consultar(
                f"""SELECT data, codigo_lancamento, tipo AS lancamento_do_dia,
                           razao_social, cnpj_cpf,
                           COALESCE(NULLIF(grupo,''), '(sem grupo)'),
                           COALESCE(NULLIF(categoria,''), '(sem categoria)'),
                           {OBRA_OU_SEM}, projeto, numero_documento, observacao,
                           COALESCE(conta_corrente,'(sem conta)'),
                           {MOVIMENTO_DE_CAIXA}, {ENCARGO}, link,
                           COALESCE(analise,'')
                      FROM fato{where}
                     ORDER BY ABS({MOVIMENTO_DE_CAIXA}) DESC, codigo_lancamento
                     LIMIT {int(limite)}""", params):
            l = dict(zip(campos, bruta))
            l["valor"] = float(l["valor"] or 0)
            l["encargo"] = float(l["encargo"] or 0)
            l["natureza"] = "Recebimento" if l["valor"] > 0 else "Pagamento"
            l["em_aberto"] = False
            linhas.append(l)
    if tipo in ("", "a_pagar"):
        condicoes, extras = _condicoes_do_calendario(
            tipo, grupo, categoria, busca, em_aberto=True, analise=analise)
        condicoes.append(f"{DIA_DO_VENCIMENTO} = CAST(? AS DATE)")
        extras.append(dia)
        where, params = f.where(" AND ".join(condicoes), extras)
        vencido = _dt.date.fromisoformat(dia) < hoje
        for bruta in consultar(
                f"""SELECT {DIA_DO_VENCIMENTO}, codigo_lancamento,
                           tipo AS lancamento_a_pagar,
                           razao_social, cnpj_cpf,
                           COALESCE(NULLIF(grupo,''), '(sem grupo)'),
                           COALESCE(NULLIF(categoria,''), '(sem categoria)'),
                           {OBRA_OU_SEM}, projeto, numero_documento, observacao,
                           COALESCE(conta_corrente,'(sem conta)'),
                           {EM_ABERTO}, 0, link, COALESCE(analise,'')
                      FROM fato{where}
                     ORDER BY ABS({EM_ABERTO}) DESC, codigo_lancamento
                     LIMIT {int(limite)}""", params):
            l = dict(zip(campos, bruta))
            l["valor"] = float(l["valor"] or 0)
            l["encargo"] = 0.0
            l["natureza"] = "Vencido" if vencido else "A pagar"
            l["em_aberto"] = True
            linhas.append(l)
    return linhas


def origem_da_conta(codigo) -> dict | None:
    """POR QUE o painel diz que este título foi pago nesta conta.

    23/09/2026, o dono no Calendário: "está dizendo que esse pagamento foi
    pago numa conta, quando no comprovante ele foi pago noutra". O painel não
    inventa a conta — ele lê o espelho do OMIE, nesta ordem:

      1. a BAIXA BANCÁRIA (o débito/crédito que o OMIE lança na conta por onde
         o dinheiro andou), quando ela existe e fecha com o valor pago;
      2. senão, a BAIXA CONSOLIDADA (o resumo do título), que costuma repetir
         a conta do título;
      3. sem baixa nenhuma, a conta PREVISTA no título.

    Esta função mostra as três coisas lado a lado, com a MESMA regra da carga
    (`_escolher_recebimentos`) marcando qual perna valeu. Se a conta errada
    está na própria baixa, o conserto é no OMIE; se está só na previsão, é a
    baixa bancária que falta lá."""
    from .sync.fato import escolher_perna_da_conta
    try:
        codigo = int(str(codigo).strip())
    except (TypeError, ValueError):
        return None
    contas = {c: (d or "").strip() for c, d in consultar(
        "SELECT codigo, descricao FROM contas_correntes")}

    def _nome(c):
        if c in (None, ""):
            return ""
        try:
            return contas.get(int(c)) or str(c)
        except (TypeError, ValueError):
            return str(c)

    titulo = consultar(
        "SELECT id_conta_corrente, valor_documento::float8, status_titulo,"
        "       numero_documento, data_vencimento"
        "  FROM titulos WHERE codigo_lancamento_omie = ?", [codigo])
    no_painel = consultar(
        f"SELECT COALESCE(conta_corrente,''), SUM(pago_recebido)"
        f"  FROM fato WHERE codigo_lancamento = ? AND NOT ({RETIDO})"
        f" GROUP BY 1 ORDER BY 2", [codigo])
    if not titulo and not no_painel:
        return None
    movs = []
    for (dpg, vpg, vliq, ncc, liq, cst, grp) in consultar(
            "SELECT ddtpagamento, nvalpago::float8, nvalliquido::float8, ncodcc,"
            "       cliquidado, cstatus, cgrupo"
            "  FROM movimentos WHERE ncodtitulo = ? ORDER BY ddtpagamento", [codigo]):
        movs.append({"data": dpg or "", "valor": float(vpg or 0.0),
                     "liquido": float(vliq or 0.0), "conta": ncc,
                     "liquidado": (liq or "").strip(), "status": (cst or "").strip(),
                     "grupo": (grp or "").strip()})
    realizado = abs(sum(float(v or 0) for _c, v in no_painel))
    # a MESMA decisão da carga: qual perna dá a conta, e por qual regra
    perna_da_conta, regra = escolher_perna_da_conta(movs, realizado)

    def _perna(m):
        if m.get("liquidado") == "N":
            return "previsão (não conta)"
        if m.get("liquidado") == "S" or m.get("liquido", 0.0) > 0.005:
            return "baixa consolidada"
        if m.get("data") and m.get("valor", 0.0) > 0.005:
            return "baixa bancária"
        return "previsão (não conta)"

    pernas = [{"data": m["data"], "valor": m["valor"], "conta": _nome(m["conta"]),
               "tipo": _perna(m), "status": m["status"], "grupo": m["grupo"],
               "valeu": m is perna_da_conta} for m in movs]
    prevista = _nome(titulo[0][0]) if titulo else ""
    return {
        "codigo": codigo,
        "documento": (titulo[0][3] if titulo else "") or "",
        "status": (titulo[0][2] if titulo else "") or "",
        "conta_prevista": prevista,
        "no_painel": [{"conta": c or "(sem conta)", "valor": float(v or 0)}
                      for c, v in no_painel],
        "pernas": pernas,
        "regra": regra,
        "tem_baixa_bancaria": any(p["tipo"] == "baixa bancária" for p in pernas),
    }


def transferencias_entre_contas(f: Filtros, de="", ate="", destino="",
                                limite=500, contas_visiveis=None) -> dict:
    """De qual conta para qual conta o dinheiro foi — os DOIS lados juntos.

    21/09/2026, o dono: *"se eu quiser filtrar, eu quero ver todas as
    transferências num determinado período da conta tal para a conta tal.
    Consigo visualizar isso aí?"*

    Hoje, não: **no OMIE uma transferência são DOIS lançamentos separados**, um
    saindo de uma conta e outro entrando na outra, sem nada que ligue um ao
    outro. Filtrando a conta de origem, ele via a saída e nao via o destino.

    O PAREAMENTO E POR DATA E VALOR, e a limitacao esta dita na tela: se duas
    transferencias do MESMO valor acontecerem no MESMO dia, o par pode trocar de
    destino. Nao ha como fazer melhor sem um vinculo que o OMIE nao guarda — e
    inventar um vinculo que nao existe seria pior que mostrar o que se sabe.

    O QUE NAO PAREOU vem junto, a parte, e e a informacao mais util da tela:
    saida sem entrada do outro lado quase sempre quer dizer que o outro
    lancamento nao esta classificado como transferencia."""
    condicoes = [PAGO, "analise = 'TRF'", "ABS(pago_recebido) > 0.005"]
    extras = []
    if de:
        condicoes.append("data >= CAST(? AS DATE)")
        extras.append(de)
    if ate:
        condicoes.append("data <= CAST(? AS DATE)")
        extras.append(ate)

    # SEM o filtro de conta: para achar o outro lado e preciso enxergar as duas
    # pontas. O recorte por conta entra depois, em Python, ja sabendo o par.
    amplo = Filtros(anos=f.anos, projetos=f.projetos, excluir_trf=False)
    where, params = amplo.where(" AND ".join(condicoes), extras)

    linhas = consultar(
        f"""SELECT data, COALESCE(conta_corrente,'(sem conta)'), pago_recebido,
                   codigo_lancamento, razao_social,
                   COALESCE(NULLIF(categoria,''), '(sem categoria)')
              FROM fato{where}
             ORDER BY data, ABS(pago_recebido) DESC
             LIMIT 20000""", params)

    saidas, entradas = [], []
    for data, conta, valor, cod, razao, categoria in linhas:
        registro = {"data": data, "conta": conta, "valor": abs(float(valor or 0)),
                    "codigo": cod, "razao_social": razao or "",
                    "categoria": categoria}
        (saidas if float(valor or 0) < 0 else entradas).append(registro)

    # Pareia saida com entrada de MESMA data e MESMO valor. Cada entrada so
    # serve a uma saida — senao uma transferencia apareceria duas vezes.
    por_chave = {}
    for e in entradas:
        por_chave.setdefault((e["data"], round(e["valor"], 2)), []).append(e)

    pares, sem_par = [], []
    for s_ in saidas:
        disponiveis = por_chave.get((s_["data"], round(s_["valor"], 2)), [])
        par = next((e for e in disponiveis if e["conta"] != s_["conta"]), None)
        if par is None:
            sem_par.append(dict(s_, sentido="saída sem entrada do outro lado"))
            continue
        disponiveis.remove(par)
        pares.append({"data": s_["data"], "valor": s_["valor"],
                      "origem": s_["conta"], "destino": par["conta"],
                      "codigo_saida": s_["codigo"], "codigo_entrada": par["codigo"],
                      "categoria": s_["categoria"]})
    for restantes in por_chave.values():
        for e in restantes:
            sem_par.append(dict(e, sentido="entrada sem saída do outro lado"))

    # O recorte da tela: a conta filtrada aparece como origem OU como destino —
    # "as transferencias DESTA conta" sao as duas coisas.
    if f.contas:
        alvo = set(f.contas)
        pares = [p for p in pares
                 if p["origem"] in alvo or p["destino"] in alvo]
        sem_par = [x for x in sem_par if x["conta"] in alvo]
    if destino:
        pares = [p for p in pares if p["destino"] == destino]
        sem_par = []

    # Quem so pode ver certas contas nao pode descobrir o NOME das outras por
    # aqui. O dinheiro foi para algum lugar, e isso ele ve; para onde, nao.
    if contas_visiveis is not None:
        podem = set(contas_visiveis)
        for par in pares:
            for ponta in ("origem", "destino"):
                if par[ponta] not in podem:
                    par[ponta] = "(outra conta)"

    pares.sort(key=lambda p: (p["data"] or dt.date(1900, 1, 1), -p["valor"]))
    total = sum(p["valor"] for p in pares)
    return {"pares": pares[:limite], "sem_par": sem_par[:limite],
            "quantos": len(pares), "total": total,
            "quantos_sem_par": len(sem_par)}


def categorias_do_extrato(f: Filtros) -> list[str]:
    """As categorias que aparecem no recorte — para o filtro da tela."""
    sem_corte = Filtros(anos=f.anos, projetos=f.projetos,
                        departamentos=f.departamentos, contas=f.contas,
                        excluir_trf=False)
    where, params = sem_corte.where(f"{PAGO} AND ABS(pago_recebido) > 0.005")
    return [c for (c,) in consultar(
        f"""SELECT DISTINCT COALESCE(NULLIF(categoria,''), '(sem categoria)')
              FROM fato{where} ORDER BY 1 LIMIT 300""", params)]


def analitico_despesas(f: Filtros, grupo="", categoria="", credor="",
                       busca="", visao="comprometido", ordem="valor",
                       de="", ate="", base="movimento",
                       pagina=1, por_pagina=200) -> dict:
    """Os lançamentos de despesa, um por linha, com filtros próprios.

    `de` e `ate` são a faixa de data, no formato AAAA-MM-DD (o que o calendário
    do navegador manda). A data é a mesma da coluna: pagamento quando o título
    foi quitado, vencimento quando ainda está em aberto — por isso a faixa
    responde "o que saiu (ou vence) nesse período".

    Devolve também os TOTAIS de toda a seleção — não só da página. O rodapé
    somando apenas as 200 linhas visíveis seria pior que não ter rodapé.
    """
    medida = {"executado": EXECUTADO, "aberto": EM_ABERTO}.get(visao, COMPROMETIDO)

    condicoes, extras = ["analise = 'DRE'", "tipo = ?"], [PAG]

    # A VISÃO FILTRA, não só ordena. Até 04/09/2026 ela mudava apenas por qual
    # coluna a lista era ordenada: escolher "Só a pagar" e clicar em Aplicar
    # devolvia a mesma lista, com as contas já quitadas no meio, mostrando zero
    # na coluna "A pagar". O rótulo prometia um recorte que não existia.
    #
    # "Só pagas" inclui a linha em que só os encargos foram pagos: juros e multa
    # quitados são dinheiro que saiu, mesmo com o principal ainda em aberto.
    if visao == "aberto":
        condicoes.append(f"ABS({EM_ABERTO}) > 0.005")
    elif visao == "executado":
        condicoes.append(f"ABS({EXECUTADO}) + ABS({ENCARGO}) > 0.005")
    # lançamento sem data fica de fora quando há faixa: não há como dizer se
    # ele cai dentro dela, e incluir "por via das dúvidas" faria o total da
    # faixa não bater com a soma das linhas que a pessoa está vendo
    # CAST explícito: a data chega como texto do calendário do navegador, e
    # deixar o banco adivinhar o tipo é o tipo de coisa que funciona num e
    # quebra no outro
    coluna_data = BASES_DE_DATA.get(base, BASES_DE_DATA["movimento"])[0]
    if de:
        condicoes.append(f"{coluna_data} >= CAST(? AS DATE)")
        extras.append(de)
    if ate:
        condicoes.append(f"{coluna_data} <= CAST(? AS DATE)")
        extras.append(ate)
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
               {EXECUTADO}, {EM_ABERTO}, juros, multa, link,
               situacao_vencimento, pedido_compra, medicao_rotulo,
               codigo_lancamento, data_vencimento, data_pagamento,
               -- atraso em dias: so faz sentido no que ja foi pago. Subtracao de
               -- DATE no Postgres ja devolve o numero de dias, sem funcao nenhuma
               CASE WHEN data_pagamento IS NOT NULL AND data_vencimento IS NOT NULL
                    THEN data_pagamento - data_vencimento END
          FROM fato{where}
         ORDER BY {ordenacao}
         LIMIT {int(por_pagina)} OFFSET {int((pagina - 1) * por_pagina)}"""
    campos = ("data", "credor", "cnpj", "grupo", "categoria", "obra", "projeto",
              "documento", "observacao", "conta", "situacao", "pago", "a_pagar",
              "juros", "multa", "link",
              # já existiam no banco e não apareciam em lugar nenhum: o atraso
              # (quitado / vencido / a vencer), a compra que originou o gasto,
              # a medição em que ele caiu e o número para procurar no OMIE
              "vencimento", "pedido", "medicao", "lancamento",
              # as duas datas separadas, e a diferenca entre elas
              "data_vencimento", "data_pagamento", "atraso")
    linhas = []
    for bruta in consultar(sql, params):
        linha = dict(zip(campos, bruta))
        for campo in ("pago", "a_pagar", "juros", "multa"):
            linha[campo] = float(linha[campo] or 0)
        linha["total"] = linha["pago"] + linha["a_pagar"]
        # A "medição" de uma despesa comum É o número do documento: quando não
        # há medição na observação, `chave_medicao` cai em "DOC:<documento>" e o
        # rótulo vira o próprio documento. Mostrar isso numa coluna chamada
        # Medição repete o dado ao lado e faz parecer que toda nota é medição.
        # Vazio aqui quer dizer o que tem de querer: esta despesa não está
        # amarrada a nenhuma medição.
        if (linha.get("medicao") or "").strip() == (linha.get("documento") or "").strip():
            linha["medicao"] = ""
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
    entre opções que não trazem nada.

    Guardado por recorte: quem fica numa obra o dia inteiro paga essas duas
    varreduras uma vez, não a cada clique."""
    where, params = f.where("analise = 'DRE' AND tipo = ?", [PAG])

    def calcular():
        grupos = [g for (g,) in consultar(
            f"SELECT DISTINCT COALESCE(NULLIF(grupo,''), '(sem grupo)') "
            f"  FROM fato{where} ORDER BY 1", params)]
        categorias = [c for (c,) in consultar(
            f"SELECT DISTINCT COALESCE(NULLIF(categoria,''), '(sem categoria)') "
            f"  FROM fato{where} ORDER BY 1", params)]
        return {"grupos": grupos, "categorias": categorias}

    return _lembrando(("opcoes_do_analitico", where, tuple(map(str, params))), calcular)


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
    """A mesma decisão do `classificar_aporte`, escrita em SQL.

    A ordem dos ramos é a regra, e cada um está aqui por um motivo:

    1. o LADO PROVEDOR vira NULL — não é aporte, é o espelho de um. Contá-lo
       faz o mesmo dinheiro aparecer duas vezes na lista;
    2. o CÓDIGO do plano financeiro decide, quando existe: ele não depende de
       alguém ter digitado o nome certo;
    3. o NOME fica como rede, para os lançamentos anteriores ao plano de
       17/09/2026;
    4. em qualquer aporte, QUEM aportou sai da CONTRAPARTE, não do rótulo — o
       financeiro troca "Aportes BWS" por "Aportes Parceiros" sem querer, e o
       dono pediu que isso não prejudicasse a análise."""
    from .sync.fato import (CODIGOS_APORTE, CODIGOS_LADO_PROVEDOR,
                            PADRAO_EMPRESA_DA_CASA, TIPOS_APORTE,
                            _APORTE_GENERICO, _sem_acento)

    # `strpos` e nao `LIKE '%...%'` de proposito: este SQL as vezes roda sem
    # parametro nenhum, e ai o driver nao faz substituicao — um `%%` ficaria
    # literal e o LIKE nao casaria com nada, calado.
    origem = ("CASE WHEN strpos(lower(COALESCE(razao_social,'')), "
              f"'{PADRAO_EMPRESA_DA_CASA}') > 0 THEN 'Aporte BWS' "
              "ELSE 'Aporte de Parceiro' END")

    def ramo(padroes, tipo):
        # Os padrões são só letras e espaços, então a alternância do regex é
        # segura — e cabe numa linha, ao contrário de um OR por padrão.
        alternativas = "|".join(_sem_acento(p) for p in padroes)
        destino = origem if tipo in ("Aporte de Parceiro", "Aporte BWS") \
            else f"'{tipo}'"
        return f"WHEN {_CAT_SIMPLES} ~ '{alternativas}' THEN {destino}"

    provedor = ", ".join(f"'{c}'" for c in sorted(CODIGOS_LADO_PROVEDOR))
    ramos = [f"WHEN COALESCE(codigo_categoria,'') IN ({provedor}) THEN NULL"]
    for cod, tipo in sorted(CODIGOS_APORTE.items()):
        destino = origem if tipo in ("Aporte de Parceiro", "Aporte BWS") \
            else f"'{tipo}'"
        ramos.append(f"WHEN COALESCE(codigo_categoria,'') = '{cod}' THEN {destino}")
    ramos += [ramo(padroes, tipo) for tipo, padroes in TIPOS_APORTE.items()]
    ramos.append(ramo(_APORTE_GENERICO, "Outros aportes"))
    return "CASE " + " ".join(ramos) + " END"


def _sql_tipos_no_saldo() -> str:
    from .sync.fato import TIPOS_NO_SALDO
    return ", ".join(f"'{t}'" for t in sorted(TIPOS_NO_SALDO))


# A decisão "é aporte, e de que tipo?" mora na coluna `tipo_aporte`, escrita na
# montagem do fato (migração 017). A expressão em SQL fica como REDE, só para
# as linhas ainda não recalculadas (NULL): assim a tela continua certa entre a
# migração e o próximo "Só refazer os números" — lenta nessas linhas, mas
# certa. Depois disso, cada consulta lê um texto pronto em vez de refazer dez
# expressões regulares por linha, e o bloco que levava dois minutos abre em
# menos de um segundo.
#
# '' na coluna quer dizer "não é aporte"; o NULLIF devolve isso como NULL, que
# é o que todas as consultas já esperavam.
TIPO_APORTE = f"NULLIF(COALESCE(tipo_aporte, {_sql_tipo_aporte()}), '')"
NO_SALDO = _sql_tipos_no_saldo()

# Nome de quem aportou e obra onde entrou — com o mesmo rótulo de "faltando" que
# a tela antiga usava, senão o vazio some no meio da tabela.
_SOCIO = "COALESCE(NULLIF(TRIM(razao_social),''), '(sem contraparte)')"
_OBRA = OBRA_OU_SEM

# AGRUPAR PELO DOCUMENTO, NAO PELO NOME.
#
# 20/09/2026. O dono procurava uma devolucao de R$ 784.647,07 que o bloco de
# aportes nao mostrava. A cascata de conferencia provou que corte nenhum a
# estava comendo: o valor esta na base e entra na soma geral. O que o escondia
# era o AGRUPAMENTO — o bloco somava por razao social, e a mesma empresa com
# dois cadastros no OMIE (ou com o nome escrito de dois jeitos) virava duas
# linhas, cada uma parecendo menor do que a empresa e.
#
# Agora a identidade e o CNPJ/CPF, so com os digitos, e o nome vira so rotulo.
# Sem documento, cai no nome em maiusculas — que e o que dava antes, nunca pior.
_SOCIO_ID = ("COALESCE("
             "NULLIF(regexp_replace(COALESCE(cnpj_cpf, ''), '[^0-9]', '', 'g'), ''),"
             "NULLIF(UPPER(TRIM(COALESCE(razao_social, ''))), ''),"
             "'(sem contraparte)')")
# Dentro do grupo os nomes podem divergir; mostra-se um deles, sempre o mesmo.
_SOCIO_ROTULO = "COALESCE(MIN(NULLIF(TRIM(razao_social), '')), '(sem contraparte)')"
_SOCIO_AGRUPADO = (_SOCIO_ID, _SOCIO_ROTULO)

# APORTE só conta quando ENTRA na obra; DEVOLUÇÃO só quando SAI.
#
# Não basta olhar o sinal, e não basta olhar o nome — precisa dos dois juntos.
# O motivo está na nota de `TIPOS_NO_SALDO` (`sync/fato.py`): quando a BWS põe
# dinheiro numa obra, o MESMO nome ("Aportes BWS") aparece dos dois lados — saída
# da conta da matriz e entrada na conta da obra. O aporte de verdade é o que
# entra; o outro é o registro de onde o dinheiro saiu.
#
# Com esta regra o lado da matriz cai fora sozinho, porque tem sempre o sinal
# contrário ao que o nome dele diz. Vale também para a volta: "Devolução de
# Aportes BWS" entrando na matriz é positivo, e devolução só conta quando sai.
#
# Antes disto o bloco decidia só pelo sinal. Na tela do dono, em 17/09/2026, a
# BWS aparecia com aportado R$ 1.677.455,70 e devolvido O MESMO VALOR, ao
# centavo: saldo zero. O painel dizia que ela não tinha nada aplicado na obra,
# quando tinha 1,67 milhão.
_E_DEVOLUCAO = f"({TIPO_APORTE}) = 'Devolução de Aporte'"
_APORTADO = ("SUM(CASE WHEN pago_recebido > 0 AND NOT " + _E_DEVOLUCAO +
             " THEN pago_recebido ELSE 0 END)")
_DEVOLVIDO = ("SUM(CASE WHEN pago_recebido < 0 AND " + _E_DEVOLUCAO +
              " THEN -pago_recebido ELSE 0 END)")


def _sem_cortar_transferencia(f: Filtros) -> Filtros:
    """Os mesmos filtros da tela, MENOS o corte de transferência entre contas.

    21/09/2026. Todas as telas descartam `analise = 'TRF'` por padrão, e com
    razão: dinheiro andando entre contas da própria empresa não é receita nem
    despesa, e contá-lo infla tudo.

    **O bloco de aportes é a exceção, e é a exceção porque o assunto dele é
    exatamente esse dinheiro.** Aporte da BWS para a obra ANDA entre contas da
    empresa — se a categoria estiver marcada como transferência no plano
    financeiro do OMIE, o corte engole o bloco inteiro e ninguém vê por quê.

    O que impedia a duplicação era esse corte; hoje quem impede é a exclusão do
    LADO PROVEDOR pelo código da categoria, que é o jeito certo de fazer: tira
    o espelho da operação e mantém a operação."""
    return Filtros(anos=f.anos, projetos=f.projetos,
                   departamentos=f.departamentos, excluir_trf=False)


def _agregado_de_aporte(f: Filtros, chaves: list) -> list[dict]:
    """Aportado / devolvido / saldo agrupado pelas colunas pedidas.

    Uma chave e o proprio texto SQL, quando agrupar e mostrar sao a mesma coisa,
    ou um par (agrupar_por, mostrar) — como no socio, que agrupa pelo documento
    e mostra o nome."""
    where, params = _sem_cortar_transferencia(f).where(
        f"{PAGO} AND ({TIPO_APORTE}) IN ({NO_SALDO})")
    agrupar = [c[0] if isinstance(c, tuple) else c for c in chaves]
    mostrar = [c[1] if isinstance(c, tuple) else c for c in chaves]
    grupos = ", ".join(agrupar)
    sql = f"""
        SELECT {', '.join(mostrar)}, {_APORTADO}, {_DEVOLVIDO}, COUNT(*)
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
                 for l in _agregado_de_aporte(f, [_SOCIO_AGRUPADO])]
    por_obra = [dict(l, obra=l["chaves"][0], socio=l["chaves"][1])
                for l in _agregado_de_aporte(f, [_OBRA, _SOCIO_AGRUPADO])]
    # O recorte "por tipo" saiu em 22/09/2026 — o dono: "tá errado, acho que
    # nem precisa dela; tô achando os dados em duplicidade". Era a mesma soma
    # do "por obra" aberta por outro eixo, e confundia mais do que dizia.

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
        "por_socio": por_socio, "por_obra": por_obra,
        "dividendos": dividendos_por_socio(f),
        "lancamentos": lancamentos_de_aporte(f),
        "aportado": total_ap, "devolvido": total_dev,
        "saldo": total_ap - total_dev,
        "tem_dados": bool(por_socio),
    }


def aportes_na_base_inteira() -> dict:
    """Aportado e devolvido SEM filtro nenhum — o bloco do DRE usa os filtros da
    barra lateral, e essa diferenca e invisivel para quem olha a tela.

    21/09/2026, o dono: *"a parte dos aportes continua sem apresentar todos os
    numeros"*. Eu vinha comparando a conferencia (que roda sem filtro) com o
    bloco do DRE (que roda COM os filtros da tela) e dizendo que tinham de bater
    — nao tinham por que bater. Um ano selecionado na barra lateral ja explica
    uma devolucao "sumida".

    Duas somas indexadas; o bloco pode mostrar sempre, sem pesar."""
    f = Filtros(excluir_trf=False)
    where, params = f.where(f"{PAGO} AND ({TIPO_APORTE}) IN ({NO_SALDO})")
    (ap, dev, n) = consultar(
        f"""SELECT COALESCE({_APORTADO}, 0), COALESCE({_DEVOLVIDO}, 0), COUNT(*)
              FROM fato{where}""", params)[0]

    # E ONDE ESTA O RESTO — obra por obra.
    #
    # 21/09/2026: filtrado na obra Mercado Barbalha, o dono via R$ 887 mil de
    # devolucao e esperava R$ 3,3 milhoes. Na base inteira havia R$ 5,4 milhoes.
    # Ou seja, o dinheiro estava la — em lancamentos que NAO estao apropriados
    # aquela obra. Dizer so "o filtro esconde X" nao resolve: ele precisa saber
    # ONDE o resto esta para poder apropriar no OMIE.
    #
    # "(nao apropriado)" e a linha que mais importa: e a que ele conserta.
    por_obra = [{"obra": obra, "aportado": float(a or 0),
                 "devolvido": float(d or 0), "lancamentos": q}
                for obra, a, d, q in consultar(
        f"""SELECT {_OBRA}, COALESCE({_APORTADO}, 0), COALESCE({_DEVOLVIDO}, 0),
                   COUNT(*)
              FROM fato{where}
             GROUP BY 1
             ORDER BY 3 DESC, 2 DESC
             LIMIT 30""", params)]
    return {"aportado": float(ap or 0), "devolvido": float(dev or 0),
            "lancamentos": n or 0, "por_obra": por_obra}


def dividendos_por_socio(f: Filtros) -> list[dict]:
    """Dividendo é distribuição de LUCRO, não devolução de capital.

    Por isso ele não abate o saldo de aporte — abater faria parecer que o sócio
    retirou o que colocou, o que não aconteceu. Fica em quadro próprio."""
    where, params = _sem_cortar_transferencia(f).where(
        f"{PAGO} AND ({TIPO_APORTE}) = 'Dividendos'")
    sql = f"""
        SELECT {_SOCIO_ID} AS socio_do_dividendo, {_SOCIO_ROTULO},
               SUM(CASE WHEN pago_recebido > 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN pago_recebido < 0 THEN -pago_recebido ELSE 0 END),
               COUNT(*)
          FROM fato{where}
         GROUP BY {_SOCIO_ID}"""
    # o socio_id vai junto para a tela abrir os lancamentos DAQUELE socio —
    # o nome nao serve de chave (ver _SOCIO_ID)
    saida = [{"socio_id": sid, "socio": socio, "recebido": float(receb or 0),
              "pago": float(pago or 0),
              "liquido": float(pago or 0) - float(receb or 0), "lancamentos": quantos}
             for sid, socio, receb, pago, quantos in consultar(sql, params)]
    saida.sort(key=lambda x: -x["liquido"])
    return saida


def lancamentos_de_dividendo(f: Filtros, *, socio_id: str = "", obra: str = "",
                             sentido: str = "pago", com_transferencias: bool = True,
                             limite: int = 500) -> dict:
    """Os lançamentos por trás de um número de dividendo, um por linha.

    Pedido do dono em 24/09/2026: *"não tem nenhum canto que eu clique e me
    sejam listados os dividendos — não sei qual é o título, a data, nada, só
    tem um valor"*.

    Mesmo critério dos quadros: categoria com "dividendo" ou "distribuição de
    lucro(s)" no nome, pago ou recebido. `sentido`: "pago" (o que saiu, o
    distribuído), "recebido" (o que entrou com esse nome) ou "todos".
    `com_transferencias` segue o quadro de onde veio o clique: o bloco de
    aportes não corta transferência; o "Resultado × dividendos", sim."""
    base = _sem_cortar_transferencia(f) if com_transferencias else f
    condicoes = [PAGO, f"({TIPO_APORTE}) = 'Dividendos'"]
    extras: list = []
    if sentido == "pago":
        condicoes.append("pago_recebido < 0")
    elif sentido == "recebido":
        condicoes.append("pago_recebido > 0")
    if socio_id:
        condicoes.append(f"({_SOCIO_ID}) = ?")
        extras.append(socio_id)
    if obra:
        condicoes.append(f"({_OBRA}) = ?")
        extras.append(obra)
    where, params = base.where(" AND ".join(condicoes), extras)
    campos = ("data", "codigo", "socio", "cnpj", "obra", "projeto", "categoria",
              "conta", "documento", "observacao", "link", "valor", "analise")
    linhas = []
    for bruta in consultar(
            f"""SELECT data, codigo_lancamento AS lancamento_de_dividendo,
                       COALESCE(NULLIF(TRIM(razao_social),''), '(sem contraparte)'),
                       cnpj_cpf, {_OBRA}, projeto,
                       COALESCE(NULLIF(categoria,''), '(sem categoria)'),
                       COALESCE(conta_corrente,''), numero_documento, observacao,
                       link, pago_recebido, COALESCE(analise,'')
                  FROM fato{where}
                 ORDER BY data DESC NULLS LAST, codigo_lancamento
                 LIMIT {int(limite)}""", params):
        linha = dict(zip(campos, bruta))
        linha["valor"] = float(linha["valor"] or 0)
        linhas.append(linha)
    (quantos, total) = consultar(
        f"SELECT COUNT(*), COALESCE(SUM(pago_recebido), 0) FROM fato{where}", params)[0]
    return {"linhas": linhas, "quantos": int(quantos or 0),
            "total": float(total or 0)}


# Teto do detalhamento na tela. A versão antiga mostrava tudo porque já tinha a
# base inteira na memória — que é justamente o que não se faz mais aqui. Quem
# precisa da lista completa baixa o Excel, que sai sem teto.
LIMITE_LANCAMENTOS = 400


def lancamentos_de_aporte(f: Filtros, limite: int | None = LIMITE_LANCAMENTOS) -> dict:
    where, params = _sem_cortar_transferencia(f).where(
        f"{PAGO} AND ({TIPO_APORTE}) IS NOT NULL")
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
    # Sem as retenções de receita: o cliente as reteve, nunca passaram pela
    # conta — a mesma régua do Fluxo de Caixa e do quadro "o dinheiro da obra"
    where_r, params_r = f.where(
        f"analise = 'DRE' AND {PAGO} AND NOT (tipo = ? AND {RETIDO})", [REC])
    resultado = {obra: float(valor or 0) for obra, valor in consultar(
        f"SELECT {_OBRA}, SUM({MOVIMENTO_DE_CAIXA}) FROM fato{where_r} GROUP BY 1",
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


def caixa_com_socios(f: Filtros) -> dict:
    """O dinheiro da obra, com os sócios dentro — obra por obra.

    Pedido do dono em 23/09/2026: *"o que eu tenho de positivo? Receitas e
    aportes. De negativo? Despesas, devolução de aportes e distribuição de
    lucros. Seria legal visualizar isso nesse local."*

    Tudo em CAIXA, só o que foi pago ou recebido de fato:

        receitas recebidas (sem as retenções) + despesas pagas = resultado
        + aportes que entraram − devoluções que saíram − dividendos pagos
        = saldo com os sócios

    Uma consulta só, com uma soma condicional por coluna. Dividendo que
    ENTROU (dinheiro recebido com nome de dividendo) fica numa coluna à parte
    e NÃO entra no saldo: distribuição é o que saiu — o que entrou com esse
    nome é coisa para conferir, não para abater."""
    where, params = _sem_cortar_transferencia(f).where(PAGO)
    sql = f"""
        SELECT {_OBRA} AS caixa_com_socios,
               SUM(CASE WHEN analise = 'DRE' AND tipo = '{REC}' AND NOT ({RETIDO})
                        THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END),
               SUM(CASE WHEN analise = 'DRE' AND tipo = '{PAG}'
                        THEN {MOVIMENTO_DE_CAIXA} ELSE 0 END),
               SUM(CASE WHEN ({TIPO_APORTE}) IN ({NO_SALDO})
                         AND ({TIPO_APORTE}) <> 'Devolução de Aporte'
                         AND pago_recebido > 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN ({TIPO_APORTE}) = 'Devolução de Aporte'
                         AND pago_recebido < 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN ({TIPO_APORTE}) = 'Dividendos'
                         AND pago_recebido < 0 THEN pago_recebido ELSE 0 END),
               SUM(CASE WHEN ({TIPO_APORTE}) = 'Dividendos'
                         AND pago_recebido > 0 THEN pago_recebido ELSE 0 END)
          FROM fato{where}
         GROUP BY 1"""
    campos = ("receitas", "despesas", "aportes", "devolucoes",
              "dividendos", "dividendos_recebidos")
    linhas = []
    for bruta in consultar(sql, params):
        linha = {"obra": bruta[0]}
        for campo, valor in zip(campos, bruta[1:]):
            linha[campo] = float(valor or 0)
        linha["resultado"] = linha["receitas"] + linha["despesas"]
        linha["saldo"] = (linha["resultado"] + linha["aportes"]
                          + linha["devolucoes"] + linha["dividendos"])
        if any(abs(linha[c]) > 0.005 for c in campos):
            linhas.append(linha)
    linhas.sort(key=lambda l: l["saldo"])
    total = {c: sum(l[c] for l in linhas)
             for c in campos + ("resultado", "saldo")}
    return {"linhas": linhas, "total": total, "tem_dados": bool(linhas)}


TRIBUTOS_RETIDOS = (("ir", "IR"), ("iss", "ISS"), ("inss", "INSS"),
                    ("pis", "PIS"), ("cofins", "COFINS"), ("csll", "CSLL"))


def retencoes_por_tributo(f: Filtros, visao: str = "todas", limite: int = 300) -> dict:
    """As retenções da receita, título a título, abertas por tributo.

    Pedido do dono em 23/09/2026, no DRE: *"na parte de retenções, consegue o
    detalhamento para clicar e ver o que é de cada tributo?"*

    O valor retido vem do fato (com os filtros da tela e o rateio por obra); a
    abertura por tributo vem do cadastro do título no espelho do OMIE (IR, ISS,
    INSS, PIS, COFINS, CSLL). Quando o título foi rateado entre obras, cada
    tributo entra na MESMA proporção do retido que coube ao recorte.
    `visao`: "quitadas" (executado), "a_receber" (em aberto) ou "todas"."""
    valor = {"quitadas": EXECUTADO, "a_receber": EM_ABERTO}.get(visao, COMPROMETIDO)
    where, params = f.where(f"analise = 'DRE' AND tipo = ? AND {RETIDO}", [REC])
    colunas_t = ", ".join(f"t.valor_{c}::float8" for c, _r in TRIBUTOS_RETIDOS)
    sql = f"""
        SELECT r.cod, r.quem, r.doc, r.obra, r.data, r.link, r.retido, {colunas_t}
          FROM (SELECT codigo_lancamento AS cod, MAX(razao_social) AS quem,
                       MAX(numero_documento) AS doc, MAX(departamento) AS obra,
                       MAX(data) AS data, MAX(link) AS link,
                       SUM({valor}) AS retido
                  FROM fato{where}
                 GROUP BY codigo_lancamento) AS r
          LEFT JOIN titulos t ON t.codigo_lancamento_omie = r.cod
         WHERE ABS(r.retido) > 0.005
         ORDER BY r.data DESC NULLS LAST, r.cod
         LIMIT 5000"""
    linhas, totais = [], {c: 0.0 for c, _r in TRIBUTOS_RETIDOS}
    totais["sem_detalhe"] = 0.0
    for cod, quem, doc, obra, data, link, retido, *tributos in consultar(sql, params):
        retido = float(retido or 0)
        do_titulo = [float(v or 0) for v in tributos]
        soma = sum(do_titulo)
        linha = {"codigo": cod, "cliente": quem or "", "documento": doc or "",
                 "obra": obra or "", "data": data, "link": link or "",
                 "retido": retido, "sem_detalhe": 0.0}
        if soma > 0.005:
            for (chave, _r), v in zip(TRIBUTOS_RETIDOS, do_titulo):
                linha[chave] = retido * v / soma
        else:
            for chave, _r in TRIBUTOS_RETIDOS:
                linha[chave] = 0.0
            linha["sem_detalhe"] = retido
        for chave in totais:
            totais[chave] += linha[chave]
        linhas.append(linha)
    return {"linhas": linhas[:int(limite)], "quantos": len(linhas),
            "total": sum(l["retido"] for l in linhas), "totais": totais,
            "tributos": [{"chave": c, "rotulo": r} for c, r in TRIBUTOS_RETIDOS]}


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


# ---------------------------------------------------------------------------
# Explorador de lançamentos
# ---------------------------------------------------------------------------
# Todas as outras telas olham só o DRE. Esta olha a BASE INTEIRA — DRE, Fluxo de
# Caixa e transferências — porque ela existe para achar o que está classificado
# errado, e o erro quase sempre é o lançamento estar na análise errada.
#
# Veio do painel Streamlit (documento de passagem de 08/09/2026): é a tela com
# que se encontra o título sem apropriação, o aporte lançado como despesa, o
# empréstimo fora da categoria certa.

# Quantas linhas a tela mostra. A planilha leva tudo: aqui o teto existe para o
# navegador não morrer com trinta mil linhas de tabela.
TETO_DO_EXPLORADOR = 3000

# Onde a busca por texto procura. São os três campos que a pessoa lê para
# reconhecer um lançamento.
BUSCA_DO_EXPLORADOR = ("razao_social", "numero_documento", "observacao")


def opcoes_do_explorador() -> dict:
    """As listas dos filtros, tiradas do que EXISTE na base.

    Sem filtro de análise: o explorador enxerga tudo, e uma lista que só
    mostrasse o DRE esconderia justamente o que se procura."""
    def _distintos(coluna, rotulo_vazio=None):
        onde = "" if rotulo_vazio else f" WHERE COALESCE(TRIM({coluna}),'') <> ''"
        sql = (f"SELECT DISTINCT COALESCE(NULLIF(TRIM({coluna}),''), "
               f"       '{rotulo_vazio}') FROM fato{onde} ORDER BY 1"
               if rotulo_vazio else
               f"SELECT DISTINCT TRIM({coluna}) FROM fato{onde} ORDER BY 1")
        return [v for (v,) in consultar(sql)]

    def calcular():
        return {
            "analises": _distintos("analise"),
            "grupos": _distintos("grupo"),
            "categorias": _distintos("categoria", SEM_CATEGORIA),
            "obras": _distintos("departamento", SEM_OBRA),
            "projetos": _distintos("projeto"),
            "contas": _distintos("conta_corrente"),
            "situacoes": _distintos("situacao"),
        }

    return _lembrando(("opcoes_do_explorador",), calcular)


# Quantos fornecedores a barra lateral desenha. NAO e teto de busca: e teto de
# HTML. Cada nome vira uma caixa de marcar no navegador, e uma base de verdade
# tem milhares deles — desenhar todos custou 1,16 MB de pagina e deixou a tela
# lenta assim que subiu, em 13/09/2026. O painel ja morreu de memoria uma vez.
TETO_DE_FORNECEDORES = 600

# Quanta folga a busca por valor dá. NÃO é capricho: o valor chega do OMIE numa
# coluna de ponto flutuante de 4 bytes, que acima de uns R$ 131 mil não guarda
# centavo. Procurar exato erra por um centavo e jura que o lançamento não
# existe. Meio real acha o que se procura sem confundir dois títulos.
TOLERANCIA_DE_VALOR = 0.5


def fornecedores_do_recorte(dados: dict | None) -> dict:
    """Os fornecedores QUE APARECEM na lista que está na tela.

    Sai das linhas já buscadas — nenhuma consulta a mais. Isso importa: a
    primeira versão disto perguntava ao banco de novo e a tela passou de 34 para
    126 ms, além de desenhar os milhares de nomes da base inteira (1,16 MB de
    página, 86% do peso dela). O dono sentiu na hora: "o painel tá super lento
    agora".

    Tirar da própria lista tem outra vantagem, que não é consolo: a barra
    lateral passa a oferecer exatamente o que está à vista. Quem filtrou
    "Devolução de Aportes" escolhe entre os poucos fornecedores daquilo, em vez
    de rolar milhares de nomes que não vêm ao caso."""
    if not dados or not dados.get("linhas"):
        return {"itens": [], "cortou": False, "sem_recorte": True}
    nomes = sorted({(l.get("razao_social") or "").strip() or SEM_FORNECEDOR
                    for l in dados["linhas"]})
    return {"itens": nomes[:TETO_DE_FORNECEDORES],
            "cortou": len(nomes) > TETO_DE_FORNECEDORES or bool(dados.get("cortou")),
            "sem_recorte": False}


COLUNAS_DO_EXPLORADOR = (
    "codigo_lancamento", "data", "tipo", "analise", "grupo", "categoria",
    "codigo_categoria", "departamento", "projeto", "razao_social",
    "numero_documento", "conta_corrente", "situacao",
    # As duas datas separadas: e com elas que se acha o lancamento no OMIE.
    # A coluna `data` e a DERIVADA (pagamento se quitado, senao vencimento) e
    # sozinha nao diz qual das duas e — o que atrapalha justamente quem esta
    # conferindo o painel contra o OMIE lado a lado.
    "data_vencimento", "data_pagamento",
    "pago_recebido", "a_pagar_receber", "observacao",
    # O link do Pipefy, quando o documento tem cartao la. 22/09/2026, o dono:
    # "quero o link pra acessar o pipefy quando pertinente".
    "link",
)


def _valor_procurado(texto: str):
    """O número que a pessoa digitou, ou None se não for número.

    Aceita os formatos que aparecem na tela do OMIE e no copiar-colar:
    `784.647,07`, `784647,07`, `784647.07`, `784647`. Texto com letra não é
    valor — senão procurar por "NF 100" viraria uma busca por cem reais."""
    limpo = (texto or "").strip().replace("R$", "").replace(" ", "")
    if not limpo or any(c.isalpha() for c in limpo):
        return None
    if "," in limpo:                      # vírgula decimal: ponto é milhar
        limpo = limpo.replace(".", "").replace(",", ".")
    try:
        return round(abs(float(limpo)), 2)
    except ValueError:
        return None


# Quantos títulos a busca por valor devolve, no máximo. Um valor redondo
# ("1000") bate com muitos títulos; a lista entra na consulta como parâmetro e
# não pode crescer sem teto.
TETO_DE_TITULOS_POR_VALOR = 5000


def _titulos_com_o_valor(pedido: dict, valor: float) -> list:
    """Os títulos cuja SOMA (todas as obras juntas) bate com o valor.

    Uma consulta só, com o resultado guardado no próprio pedido: a lista, os
    totais e o resumo montam o mesmo WHERE, e refazer a soma três vezes seria
    pagar três vezes pela mesma resposta."""
    guardado = pedido.get("_titulos_com_o_valor")
    if guardado and guardado[0] == valor:
        return guardado[1]
    codigos = [c for (c,) in consultar(
        f"""SELECT codigo_lancamento FROM fato
             GROUP BY codigo_lancamento
            HAVING ABS(ABS(SUM(pago_recebido)) - ?) < {TOLERANCIA_DE_VALOR}
                OR ABS(ABS(SUM(a_pagar_receber)) - ?) < {TOLERANCIA_DE_VALOR}
             LIMIT {TETO_DE_TITULOS_POR_VALOR}""", [valor, valor])]
    pedido["_titulos_com_o_valor"] = (valor, codigos)
    return codigos


def _onde_do_explorador(pedido: dict) -> tuple[str, list]:
    """Monta o WHERE do explorador a partir do que a pessoa escolheu."""
    condicoes, params = [], []

    tipo = pedido.get("tipo") or ""
    if tipo in ("pagar", "receber"):
        condicoes.append("tipo = ?")
        params.append(PAG if tipo == "pagar" else REC)

    # O EXPLORADOR NÃO ESCONDE NADA. As telas de análise (DRE, Visão Geral,
    # Resultado por Obra) tiram as transferências de propósito: são dinheiro
    # trocando de conta da própria empresa, e somá-las contaria o mesmo valor
    # duas vezes. Aqui é o contrário — a tela existe para ACHAR o que está
    # classificado errado, e "lançado numa categoria marcada como transferência
    # no OMIE" é justamente um desses erros. Esconder o que se procura é o
    # oposto do trabalho.
    #
    # Até 13/09/2026 o padrão escondia TRF, e isso custou caro: o dono procurou
    # uma devolução de aporte de 24/12/2025 conciliada, viu a devolução do lado
    # aparecer e essa não, e não havia nada na tela explicando a diferença — a
    # categoria dela está marcada como transferência no OMIE. A frase dele:
    # "aqui era pra aparecer todos os lançamentos igual como aparece no
    # relatório de conta corrente do OMIE".
    #
    # Quem quiser cortar por análise usa a lista Análise da barra lateral, onde
    # DRE, Fluxo de Caixa e TRF são três caixas de marcar.
    analises = [a for a in pedido.get("analises") or [] if a]
    if analises:
        condicoes.append("analise = ANY(?)")
        params.append(analises)

    for campo, coluna in (("grupos", "grupo"),
                          ("projetos", "projeto"), ("contas", "conta_corrente"),
                          ("situacoes", "situacao")):
        escolhidos = [v for v in pedido.get(campo) or [] if v]
        if escolhidos:
            condicoes.append(f"TRIM(COALESCE({coluna},'')) = ANY(?)")
            params.append(escolhidos)

    # A categoria tem rotulo proprio para o vazio, como a obra: e o que permite
    # PEDIR o que esta sem classificacao, em vez de procurar por sorte.
    categorias = [c for c in pedido.get("categorias") or [] if c]
    if categorias:
        condicoes.append(
            f"COALESCE(NULLIF(TRIM(categoria),''), '{SEM_CATEGORIA}') = ANY(?)")
        params.append(categorias)

    obras = [o for o in pedido.get("obras") or [] if o]
    if obras:
        condicoes.append(f"{OBRA_OU_SEM} = ANY(?)")
        params.append(obras)

    fornecedores = [f for f in pedido.get("fornecedores") or [] if f]
    if fornecedores:
        condicoes.append(
            f"COALESCE(NULLIF(TRIM(razao_social),''), '{SEM_FORNECEDOR}') = ANY(?)")
        params.append(fornecedores)

    busca = (pedido.get("busca") or "").strip()
    if busca:
        # Procurar pelo NUMERO DO TITULO do OMIE tem de funcionar. Sem isso, a
        # única maneira de perguntar "este título está no painel?" era procurar
        # pelo nome do fornecedor e conferir a olho — e se o nome não estiver
        # preenchido, nem isso. Em 13/09/2026 o dono passou meia hora atrás de um
        # lançamento por falta desta busca.
        alternativas = [f"{c} ILIKE ?" for c in BUSCA_DO_EXPLORADOR]
        valores = [f"%{busca}%"] * len(BUSCA_DO_EXPLORADOR)
        if busca.isdigit():
            alternativas.append("codigo_lancamento = ?")
            valores.append(int(busca))
        # Procurar por VALOR. É o dado que a pessoa sempre tem à mão quando está
        # conferindo o painel contra o OMIE lado a lado — e era o único jeito de
        # perguntar "este lançamento está aqui?" que a tela não aceitava.
        # Digitar 784.647,07 ou 784647.07 ou 784647 tem de achar o mesmo.
        valor = _valor_procurado(busca)
        if valor is not None:
            # O VALOR DO TITULO INTEIRO, não o da linha. O painel quebra o
            # título por obra: um título rateado entre três obras vira três
            # linhas, cada uma com uma FRAÇÃO do valor. Nenhuma delas tem o
            # número que está na tela do OMIE.
            #
            # Foi assim que três devoluções de aporte pareceram sumidas em
            # 13/09/2026 — estavam na base o tempo todo, partidas entre obras.
            # Comparar linha a linha acharia só o que está numa obra só, que é
            # justamente o caso fácil.
            # POR TOLERANCIA, nao por igualdade — e o motivo importa.
            #
            # O valor chega do OMIE numa coluna REAL (ponto flutuante de 4
            # bytes, ~7 digitos significativos), entao acima de uns R$ 131 mil
            # ele PERDE CENTAVOS: os R$ 784.647,07 da tela do OMIE estao
            # gravados aqui como 784.647,06. Comparar exato errava por um
            # centavo e dizia que o lancamento nao existia.
            #
            # Foi assim que uma devolucao de aporte de 24/12/2025 pareceu sumida
            # a tarde inteira de 13/09/2026. Ela estava na base o tempo todo.
            #
            # E A SOMA POR TÍTULO É FEITA UMA VEZ, ANTES — não dentro do WHERE.
            #
            # 22/09/2026, o dono: "Tela montada em 373542 ms — 5 consultas ao
            # banco". A versão anterior punha a soma como subconsulta dentro do
            # OR ("codigo_lancamento IN (SELECT ... GROUP BY ...)"). Com 120
            # mil títulos, o resultado não cabe na memória de trabalho do
            # banco, e o Postgres deixa de guardá-lo numa tabela de hash: passa
            # a REFAZER a soma da base inteira para cada uma das 185 mil linhas.
            # Três consultas assim são seis minutos. Agora os títulos que batem
            # com o valor são achados numa consulta só, e entram na condição
            # como lista pronta.
            alternativas.append(
                f"(ABS(ABS(pago_recebido) - ?) < {TOLERANCIA_DE_VALOR}"
                f" OR ABS(ABS(a_pagar_receber) - ?) < {TOLERANCIA_DE_VALOR})")
            valores.extend([valor, valor])
            codigos = _titulos_com_o_valor(pedido, valor)
            if codigos:
                alternativas.append("codigo_lancamento = ANY(?)")
                valores.append(codigos)
        condicoes.append("(" + " OR ".join(alternativas) + ")")
        params.extend(valores)

    # A faixa de data DEIXA PASSAR o que não tem data — ao contrário do
    # Analítico. Aqui a pergunta é "onde está o lançamento errado", e lançamento
    # sem data é justamente um dos que se procura.
    if pedido.get("de"):
        condicoes.append("(data IS NULL OR data >= CAST(? AS DATE))")
        params.append(pedido["de"])
    if pedido.get("ate"):
        condicoes.append("(data IS NULL OR data <= CAST(? AS DATE))")
        params.append(pedido["ate"])

    where = (" WHERE " + " AND ".join(condicoes)) if condicoes else ""
    return where, params


def explorar(pedido: dict, limite: int = TETO_DO_EXPLORADOR) -> dict:
    """Os lançamentos que atendem ao pedido, com os totais da seleção INTEIRA.

    `titulos` conta códigos distintos: um título rateado entre três obras vira
    três linhas aqui, e dizer que são três títulos enganaria quem for alterar."""
    where, params = _onde_do_explorador(pedido)

    totais = consultar(
        f"""SELECT COUNT(*), COUNT(DISTINCT codigo_lancamento),
                   COALESCE(SUM(pago_recebido), 0),
                   COALESCE(SUM(a_pagar_receber), 0)
              FROM fato{where}""", params)[0]

    colunas = ", ".join(COLUNAS_DO_EXPLORADOR)
    linhas = [dict(zip(COLUNAS_DO_EXPLORADOR, bruta)) for bruta in consultar(
        f"""SELECT {colunas} FROM fato{where}
             ORDER BY data DESC NULLS LAST, codigo_lancamento DESC
             LIMIT {int(limite)}""", params)]
    for linha in linhas:
        for campo in ("pago_recebido", "a_pagar_receber"):
            linha[campo] = float(linha[campo] or 0)

    return {
        "linhas": linhas,
        "quantos": totais[0] or 0,
        "titulos": totais[1] or 0,
        "pago": float(totais[2] or 0),
        "aberto": float(totais[3] or 0),
        "cortou": (totais[0] or 0) > len(linhas),
    }


def resumo_do_explorador(pedido: dict, limite: int = 300) -> list[dict]:
    """Análise × Grupo × Categoria da seleção — para enxergar o padrão antes de
    sair alterando título a título."""
    where, params = _onde_do_explorador(pedido)
    sql = f"""
        SELECT COALESCE(NULLIF(TRIM(analise),''), '(sem análise)'),
               COALESCE(NULLIF(TRIM(grupo),''), '(sem grupo)'),
               COALESCE(NULLIF(TRIM(categoria),''), '(sem categoria)'),
               COUNT(*), COALESCE(SUM(pago_recebido), 0),
               COALESCE(SUM(a_pagar_receber), 0)
          FROM fato{where}
         GROUP BY 1, 2, 3
         ORDER BY COUNT(*) DESC LIMIT {int(limite)}"""
    campos = ("analise", "grupo", "categoria", "linhas", "pago", "aberto")
    return [dict(zip(campos, (l[0], l[1], l[2], l[3],
                             float(l[4] or 0), float(l[5] or 0))))
            for l in consultar(sql, params)]


def categorias_para_alterar() -> list[dict]:
    """As categorias do OMIE, para escolher a nova numa lista em vez de digitar
    um código à mão. Fora as TOTALIZADORAS: elas são somatório de outras, e
    lançar um título numa delas não faz sentido no OMIE."""
    def calcular():
        return [{"codigo": c, "descricao": d, "onde": ("DRE" if (dre or "").strip()
                                                       else "Fluxo de Caixa")}
                for c, d, dre in consultar(
                    "SELECT codigo, descricao, codigo_dre FROM cat "
                    " WHERE COALESCE(UPPER(TRIM(totalizadora)),'N') <> 'S' "
                    "   AND COALESCE(TRIM(descricao),'') <> '' ORDER BY descricao")]
    return _lembrando(("categorias_para_alterar",), calcular)


def departamentos_para_alterar() -> list[dict]:
    """As obras do OMIE, pelo espelho do rateio. É de lá que sai o código, que é
    o que o OMIE quer — o nome sozinho não serve para alterar."""
    def calcular():
        return [{"codigo": c, "nome": n} for c, n in consultar(
            "SELECT DISTINCT ccoddep, cdesdep FROM rateio "
            " WHERE COALESCE(TRIM(cdesdep),'') <> '' ORDER BY cdesdep")]
    return _lembrando(("departamentos_para_alterar",), calcular)


# ---------------------------------------------------------------------------
# Conferência: as duas definições de "foi pago" concordam?
# ---------------------------------------------------------------------------
# Existem duas, e elas DIVERGEM (descoberto em 13/09/2026, investigando valores
# errados no bloco de Aportes e Dividendos do DRE):
#
#   CARGA (`sync/fato.py`): quitado = o texto do status diz pago/recebido/
#     conciliado **OU** a baixa do OMIE diz liquidado ("cLiquidado = S").
#   TELAS (o `PAGO` acima):  só a primeira metade — o texto do status.
#
# Um título liquidado cujo status use outra palavra ("Quitado", "Baixado") tem o
# valor gravado como realizado na base e NÃO É CONTADO POR NENHUMA TELA. O
# dinheiro existe no painel e não aparece em lugar nenhum. A regra do `PAGO`
# aparece em dez lugares — DRE, Visão Geral, fluxo de caixa, aportes.
#
# Esta função NÃO CORRIGE NADA. Ela mede o estrago, para a decisão de corrigir
# ser tomada com o número na mão. A carga grava a própria decisão na coluna
# `situacao_vencimento` ('Quitado'), então dá para comparar as duas sem adivinhar.

INVISIVEL_PARA_AS_TELAS = (
    f"situacao_vencimento = 'Quitado' AND NOT ({PAGO}) AND pago_recebido <> 0")


# A migracao 010 trocou o tipo das colunas de dinheiro para NUMERIC, mas isso
# NAO devolve o centavo que ja se perdeu: o 784.647,06 gravado continua
# 784.647,06. Os valores so voltam a ficar certos depois de uma rebaixa completa
# do OMIE — e essa e a parte que um humano tem de mandar rodar.
#
# Sem este aviso na tela, a migracao daria a impressao de ter resolvido, e os
# numeros continuariam errados em silencio. Isto e pior que nao ter consertado.
MIGRACAO_DO_DINHEIRO = "010_dinheiro_exato.sql"


def recarga_total_pendente() -> dict:
    """A base ainda tem valores com centavo errado, esperando carga inicial?

    Responde comparando duas datas que o proprio sistema ja guarda: quando a
    migracao 010 foi aplicada e quando a ultima carga inicial terminou bem. Se a
    carga veio depois, nao ha nada a fazer e o aviso some sozinho."""
    aplicada = consultar(
        "SELECT aplicada_em FROM painel._migracoes WHERE nome = ?",
        [MIGRACAO_DO_DINHEIRO])
    if not aplicada:
        return {"pendente": False}
    quando = aplicada[0][0]
    depois = consultar(
        "SELECT fim FROM execucoes"
        " WHERE tipo = 'carga_inicial' AND ok AND fim IS NOT NULL AND fim >= ?"
        " ORDER BY fim DESC LIMIT 1", [quando])
    return {"pendente": not depois, "migracao_em": quando,
            "carga_em": depois[0][0] if depois else None}


def movimentos_fora_do_painel() -> dict:
    """O dinheiro que andou na conta e NUNCA apareceu em tela nenhuma.

    20/09/2026, o dono: *"esse movimento sem titulo, eu nao sei exatamente quem
    sao e de qual forma afeta. como saber?"* — e nao dava para saber, porque a
    carga descartava esses movimentos antes de gravar. Desde a migracao 012 eles
    ficam guardados (fora de todo numero de tela) e esta funcao os mostra.

    Sao DUAS coisas diferentes, e a tela separa:

      1. movimento sem titulo nenhum — lancado direto na conta corrente;
      2. movimento que aponta para um titulo que o painel nao tem — titulo
         excluido no OMIE depois, ou que a carga nao trouxe.

    A primeira e dinheiro que o painel nao conhece. A segunda e sintoma de base
    desatualizada, e costuma sumir com uma carga completa."""
    (quantos, valor, entrou, saiu) = consultar(
        """SELECT COUNT(*),
                  COALESCE(SUM(ABS(COALESCE(nvalpago, 0))), 0),
                  COALESCE(SUM(CASE WHEN cnatureza = 'R'
                                    THEN ABS(COALESCE(nvalpago, 0)) ELSE 0 END), 0),
                  COALESCE(SUM(CASE WHEN cnatureza <> 'R'
                                    THEN ABS(COALESCE(nvalpago, 0)) ELSE 0 END), 0)
             FROM movimentos_sem_titulo""")[0]

    por_ano = [{"ano": ano or "(sem data)", "linhas": n, "valor": float(v or 0)}
               for ano, n, v in consultar(
        """SELECT CASE WHEN ddtpagamento ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                       THEN substr(ddtpagamento, 7, 4) END,
                  COUNT(*), COALESCE(SUM(ABS(COALESCE(nvalpago, 0))), 0)
             FROM movimentos_sem_titulo
            GROUP BY 1 ORDER BY 1 DESC NULLS LAST LIMIT 20""")]

    por_categoria = [{"categoria": (desc or cod or "(sem categoria)"),
                      "codigo": cod or "", "linhas": n, "valor": float(v or 0)}
                     for cod, desc, n, v in consultar(
        """SELECT m.ccodcateg, c.descricao, COUNT(*),
                  COALESCE(SUM(ABS(COALESCE(m.nvalpago, 0))), 0)
             FROM movimentos_sem_titulo m
             LEFT JOIN cat c ON c.codigo = m.ccodcateg
            GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 30""")]

    por_conta = [{"conta": desc or (str(cod) if cod else "(sem conta)"),
                  "linhas": n, "valor": float(v or 0)}
                 for cod, desc, n, v in consultar(
        """SELECT m.ncodcc, cc.descricao, COUNT(*),
                  COALESCE(SUM(ABS(COALESCE(m.nvalpago, 0))), 0)
             FROM movimentos_sem_titulo m
             LEFT JOIN contas_correntes cc ON cc.codigo = m.ncodcc
            GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 30""")]

    maiores = [{"data": dtp or "—", "natureza": "Entrada" if nat == "R" else "Saída",
                "conta": conta or "(sem conta)",
                "categoria": categ or "(sem categoria)",
                "contraparte": (cli or "").strip() or "(sem contraparte)",
                "valor": float(v or 0), "status": (st or "").strip()}
               for dtp, nat, conta, categ, cli, v, st in consultar(
        """SELECT m.ddtpagamento, m.cnatureza, cc.descricao,
                  COALESCE(c.descricao, m.ccodcateg), cl.razao_social,
                  ABS(COALESCE(m.nvalpago, 0)), m.cstatus
             FROM movimentos_sem_titulo m
             LEFT JOIN contas_correntes cc ON cc.codigo = m.ncodcc
             LEFT JOIN cat c  ON c.codigo = m.ccodcateg
             LEFT JOIN clientes cl ON cl.codigo = m.ncodcliente
            ORDER BY ABS(COALESCE(m.nvalpago, 0)) DESC LIMIT 50""")]

    # o segundo caso: o movimento tem titulo, mas o titulo nao esta na base
    (orfaos, valor_orfaos) = consultar(
        """SELECT COUNT(*), COALESCE(SUM(ABS(COALESCE(m.nvalpago, 0))), 0)
             FROM movimentos m
            WHERE NOT EXISTS (SELECT 1 FROM titulos t
                               WHERE t.codigo_lancamento_omie = m.ncodtitulo)""")[0]

    return {"linhas": quantos or 0, "valor": float(valor or 0),
            "entrou": float(entrou or 0), "saiu": float(saiu or 0),
            "por_ano": por_ano, "por_categoria": por_categoria,
            "por_conta": por_conta, "maiores": maiores,
            "orfaos": orfaos or 0, "valor_orfaos": float(valor_orfaos or 0)}


def conferencia_do_pago() -> dict:
    """Quanto dinheiro a carga deu por realizado e as telas não enxergam.

    Devolve o total, a contagem, e a lista dos textos de situação envolvidos —
    que é o que diz QUAIS palavras o `PAGO` está deixando escapar."""
    (quantos, valor, titulos) = consultar(
        f"""SELECT COUNT(*), COALESCE(SUM(ABS(pago_recebido)), 0),
                   COUNT(DISTINCT codigo_lancamento)
              FROM fato WHERE {INVISIVEL_PARA_AS_TELAS}""")[0]

    situacoes = [{"situacao": sit or "(vazia)", "linhas": n,
                  "valor": float(v or 0)}
                 for sit, n, v in consultar(
        f"""SELECT situacao, COUNT(*), COALESCE(SUM(ABS(pago_recebido)), 0)
              FROM fato WHERE {INVISIVEL_PARA_AS_TELAS}
             GROUP BY 1 ORDER BY 2 DESC LIMIT 30""")]

    categorias = [{"categoria": c or "(sem categoria)", "analise": a or "—",
                   "linhas": n, "valor": float(v or 0)}
                  for c, a, n, v in consultar(
        f"""SELECT categoria, analise, COUNT(*),
                   COALESCE(SUM(ABS(pago_recebido)), 0)
              FROM fato WHERE {INVISIVEL_PARA_AS_TELAS}
             GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 30""")]

    # o outro lado da moeda: o status diz pago mas a carga nao achou realizado.
    # Nao some dinheiro por aqui, mas ajuda a saber se as duas regras batem.
    (ao_contrario,) = consultar(
        f"""SELECT COUNT(*) FROM fato
             WHERE {PAGO} AND situacao_vencimento <> 'Quitado'""")[0]

    return {"linhas": quantos or 0, "titulos": titulos or 0,
            "valor": float(valor or 0), "situacoes": situacoes,
            "categorias": categorias, "ao_contrario": ao_contrario or 0}


def cobertura_das_observacoes() -> dict:
    """Quantos títulos têm a observação do OMIE, e quantos não têm.

    A observação NÃO vem na listagem do OMIE: ela só chega consultando um título
    por vez — ~116 mil chamadas para contas a pagar, horas de trabalho. Isso é
    feito por um script separado, à mão (`backfill_observacoes`).

    O dono desconfiou disso em 17/09/2026 — "as observações dos títulos,
    completos, não estão vindo para o painel" — e estava certo. Sem este número
    na tela, a única forma de saber era ler o código."""
    (total, com_obs, tentados) = consultar(
        """SELECT COUNT(*),
                  COUNT(*) FILTER (WHERE TRIM(COALESCE(observacao,'')) <> ''),
                  COUNT(*) FILTER (WHERE observacao_sync IS NOT NULL)
             FROM titulos
            WHERE UPPER(COALESCE(status_titulo,'')) <> 'CANCELADO'""")[0]
    por_natureza = [{"natureza": "A pagar" if n == "P" else "A receber",
                     "total": t, "com_obs": c}
                    for n, t, c in consultar(
        """SELECT natureza, COUNT(*),
                  COUNT(*) FILTER (WHERE TRIM(COALESCE(observacao,'')) <> '')
             FROM titulos
            WHERE UPPER(COALESCE(status_titulo,'')) <> 'CANCELADO'
            GROUP BY 1 ORDER BY 1""")]
    return {"total": total or 0, "com_obs": com_obs or 0,
            "tentados": tentados or 0, "por_natureza": por_natureza,
            "pct": round((com_obs or 0) * 100 / (total or 1), 1)}


def conferencia_das_contas() -> dict:
    """A conta que o relatorio mostra e a de onde o dinheiro saiu?

    22/09/2026, o dono: *"refiz os numeros do painel, mas o problema das contas
    permaneceu"*. O OMIE guarda cada baixa em duas pernas: a CONSOLIDADA (o
    resumo do titulo, que repete a conta do titulo — a da previsao) e a
    BANCARIA (uma por conta que o dinheiro tocou). Esta conferencia mede, na
    base de verdade, quantas pernas de cada tipo existem e em quantas a conta
    e DIFERENTE da do titulo. Se a consolidada quase nunca difere e a bancaria
    difere, o relatorio tem de ler a bancaria — e e isso que o `fato` faz
    desde hoje.

    So mede. Nao altera nada."""
    por_perna = {}
    for perna, pernas, diferentes, sem_conta in consultar(
        """SELECT CASE WHEN m.cliquidado = 'S' OR COALESCE(m.nvalliquido, 0) > 0
                       THEN 'consolidada' ELSE 'bancaria' END,
                  COUNT(*),
                  COUNT(*) FILTER (WHERE m.ncodcc IS NOT NULL
                                     AND m.ncodcc::text <> COALESCE(t.id_conta_corrente::text, '')),
                  COUNT(*) FILTER (WHERE m.ncodcc IS NULL)
             FROM movimentos m
             JOIN titulos t ON t.codigo_lancamento_omie = m.ncodtitulo
            WHERE COALESCE(m.nvalpago, 0) > 0 AND COALESCE(m.cliquidado, '') <> 'N'
            GROUP BY 1"""):
        por_perna[perna] = {"pernas": pernas or 0, "diferentes": diferentes or 0,
                            "sem_conta": sem_conta or 0}

    exemplos = [{"titulo": cod, "natureza": "Receber" if nat == "R" else "Pagar",
                 "prevista": prev or "(sem conta)", "bancaria": banc or "(sem conta)",
                 "valor": float(v or 0), "data": dtp or "—"}
                for cod, nat, prev, banc, v, dtp in consultar(
        """SELECT t.codigo_lancamento_omie, t.natureza, cp.descricao, cb.descricao,
                  m.nvalpago::float8, m.ddtpagamento
             FROM movimentos m
             JOIN titulos t ON t.codigo_lancamento_omie = m.ncodtitulo
             LEFT JOIN contas_correntes cp ON cp.codigo = t.id_conta_corrente
             LEFT JOIN contas_correntes cb ON cb.codigo = m.ncodcc
            WHERE NOT (m.cliquidado = 'S' OR COALESCE(m.nvalliquido, 0) > 0)
              AND COALESCE(m.nvalpago, 0) > 0 AND COALESCE(m.cliquidado, '') <> 'N'
              AND m.ncodcc IS NOT NULL
              AND m.ncodcc::text <> COALESCE(t.id_conta_corrente::text, '')
            ORDER BY m.nvalpago DESC LIMIT 30""")]

    bancaria = por_perna.get("bancaria", {"pernas": 0, "diferentes": 0, "sem_conta": 0})
    consolidada = por_perna.get("consolidada", {"pernas": 0, "diferentes": 0, "sem_conta": 0})
    return {"bancaria": bancaria, "consolidada": consolidada, "exemplos": exemplos,
            # sem perna bancaria a base NAO TEM a conta real: ai o conserto no
            # fato nao muda nada e a conta tem de vir de outra listagem do OMIE
            "tem_perna_bancaria": bancaria["pernas"] > 0}


def conferencia_dos_juros(configuradas) -> dict:
    """Onde estão os juros de empréstimo na base — categoria por categoria.

    22/09/2026, o dono: "na controladoria tem 1,6 milhão, no painel só vejo
    191 mil". Esta conferência não decide nada: lista toda categoria cujo nome
    fala em juro, empréstimo, financiamento, IOF, encargo ou amortização, com a
    análise em que o OMIE a põe (DRE ou Fluxo de Caixa), quanto foi pago e
    quanto está em aberto — e marca quais delas a prestação de contas está
    contando. O que estiver no Fluxo de Caixa não é despesa para o painel:
    parcela de empréstimo com o juro dentro é exatamente isso."""
    alvos = {a.strip().lower() for a in (configuradas or []) if a and a.strip()}
    linhas = []
    for cat, cod, analise, tipo, titulos, pago, aberto in consultar(f"""
        SELECT TRIM(categoria), MAX(COALESCE(codigo_categoria,'')), analise, tipo,
               COUNT(DISTINCT codigo_lancamento),
               COALESCE(SUM({EXECUTADO}), 0), COALESCE(SUM(a_pagar_receber), 0)
          FROM fato
         WHERE translate(lower(COALESCE(categoria,'')), 'áàâãéêíóôõúç', 'aaaaeeiooouc')
               ~ '(jur|emprest|financ|\\miof\\M|encarg|amortiz)'
         GROUP BY 1, 3, 4
         ORDER BY 6"""):
        linhas.append({"categoria": cat, "codigo": cod, "analise": analise or "",
                       "tipo": "Receber" if tipo == REC else "Pagar",
                       "titulos": titulos, "pago": float(pago or 0),
                       "aberto": float(aberto or 0),
                       "configurada": (cat or "").strip().lower() in alvos})
    (encargos,) = consultar(f"""
        SELECT COALESCE(SUM({ENCARGO}), 0) AS encargos_de_atraso FROM fato
         WHERE tipo = ?""", [PAG])[0]
    return {"linhas": linhas, "encargos_de_atraso": float(encargos or 0),
            "configuradas": sorted(alvos),
            "total_configurado_dre": sum(abs(l["pago"]) for l in linhas
                                         if l["configurada"] and l["analise"] == "DRE")}


def conferencia_dos_aportes(f: "Filtros | None" = None) -> dict:
    """De onde a diferença do bloco de Aportes vem — corte a corte.

    O dono, em 14/09/2026: o bloco mostra R$ 567 mil de devolvido para uma
    empresa, e um único título dela é de R$ 784 mil. Ou seja, o bloco está
    comendo lançamentos — e a pergunta "quais?" não tinha resposta na tela.

    Em vez de apostar em qual dos cortes é o culpado, esta função mostra a
    CASCATA: parte de tudo o que tem categoria de aporte, sem corte nenhum, e
    desce um degrau por vez, dizendo quanto cada um levou. O degrau que come o
    valor que falta é o culpado, e aparece na tela em vez de na minha cabeça.

    Os degraus, na ordem em que o código os aplica:
      1. tudo o que é aporte — já SEM o lado provedor, que é o espelho da
         operação e apareceria em dobro;
      2. menos o que NÃO entra no saldo — hoje, só Dividendos;
      3. menos o que o `PAGO` não reconhece como pago;
      4. o que sobra é o que o bloco mostra.

    O corte de transferência entre contas SAIU desta lista em 21/09/2026, junto
    com a mudança no bloco: aporte entre contas da própria empresa é exatamente
    o assunto aqui, e o corte engolia tudo. Quem impede a duplicação agora é a
    exclusão do lado provedor, pelo código da categoria — ver
    `_sem_cortar_transferencia`. Quanto o lado provedor representa vem à parte,
    em `lado_provedor`, para não virar mistério."""
    f = _sem_cortar_transferencia(f or Filtros())

    def _soma(extra):
        where, params = f.where(extra)
        (ap, dev, n) = consultar(
            f"""SELECT COALESCE({_APORTADO}, 0), COALESCE({_DEVOLVIDO}, 0), COUNT(*)
                  FROM fato{where}""", params)[0]
        return {"aportado": float(ap or 0), "devolvido": float(dev or 0),
                "linhas": n or 0}

    e_aporte = f"({TIPO_APORTE}) IS NOT NULL"
    no_saldo = f"({TIPO_APORTE}) IN ({NO_SALDO})"

    passos = [
        ("Tudo o que é aporte (sem o lado provedor)", _soma(e_aporte)),
        ("Só o que entra no saldo (tira Dividendos)", _soma(no_saldo)),
        ("Tirando o que o painel não reconhece como pago",
         _soma(f"{no_saldo} AND {PAGO}")),
    ]
    for i, (_rotulo, valores) in enumerate(passos):
        anterior = passos[i - 1][1] if i else None
        valores["comeu_devolvido"] = (
            round(anterior["devolvido"] - valores["devolvido"], 2) if anterior else 0.0)
        valores["comeu_aportado"] = (
            round(anterior["aportado"] - valores["aportado"], 2) if anterior else 0.0)

    # e QUEM foi comido, para nao virar outro numero sem nome
    where_pago, params_pago = f.where(f"{no_saldo} AND NOT ({PAGO}) AND pago_recebido <> 0")
    comidos_pago = _linhas_de_aporte_comidas(where_pago, params_pago)

    # O lado provedor nao entra em numero nenhum do bloco — de proposito. Mas
    # ele tem de aparecer em ALGUM lugar, senao vira o proximo misterio: o dono
    # ve os lancamentos no OMIE e nao os acha no painel.
    from .sync.fato import CODIGOS_LADO_PROVEDOR
    lista = ", ".join(f"'{c}'" for c in sorted(CODIGOS_LADO_PROVEDOR))
    where_prov, params_prov = f.where(
        f"COALESCE(codigo_categoria,'') IN ({lista}) AND {PAGO}")
    (n_prov, v_prov) = consultar(
        f"""SELECT COUNT(*), COALESCE(SUM(ABS(pago_recebido)), 0)
              FROM fato{where_prov}""", params_prov)[0]
    lado_provedor = {"linhas": n_prov or 0, "valor": float(v_prov or 0),
                     "codigos": sorted(CODIGOS_LADO_PROVEDOR),
                     "maiores": _linhas_de_aporte_comidas(where_prov, params_prov)}

    return {"passos": passos, "comidos_trf": [],
            "comidos_pago": comidos_pago,
            "lado_provedor": lado_provedor,
            "por_contraparte": _devolucoes_por_contraparte(f)}


def _devolucoes_por_contraparte(f: Filtros) -> list[dict]:
    """Toda devolucao que entra no bloco, somada por documento E por nome.

    Existe para responder "cade meu valor?" sem ninguem ter de acreditar em
    mim. Quando a cascata mostra que corte nenhum comeu o dinheiro — foi o caso
    em 20/09/2026 —, o que sobra e o agrupamento: a empresa aparece dividida em
    duas linhas porque tem dois cadastros no OMIE, ou porque o nome esta escrito
    de dois jeitos. Aqui os dois aparecem lado a lado: quantos NOMES diferentes
    o mesmo documento tem, e quanto cada um leva."""
    where, params = f.where(
        f"{PAGO} AND ({TIPO_APORTE}) IN ({NO_SALDO}) AND pago_recebido < 0 "
        f"AND ({TIPO_APORTE}) = 'Devolução de Aporte'")
    sql = f"""
        SELECT {_SOCIO_ID}, {_SOCIO_ROTULO},
               COUNT(DISTINCT NULLIF(TRIM(COALESCE(razao_social, '')), '')),
               STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(razao_social, '')), ''),
                          ' | '),
               SUM(-pago_recebido), COUNT(*)
          FROM fato{where}
         GROUP BY {_SOCIO_ID}
         ORDER BY 5 DESC
         LIMIT 60"""
    return [{"documento": doc, "nome": nome, "nomes": quantos_nomes or 0,
             "todos_os_nomes": todos or "", "devolvido": float(v or 0),
             "lancamentos": n}
            for doc, nome, quantos_nomes, todos, v, n in consultar(sql, params)]


def _linhas_de_aporte_comidas(where, params) -> list[dict]:
    """Os lançamentos que um degrau da cascata cortou, do maior para o menor."""
    campos = ("codigo", "data", "socio", "categoria", "analise", "situacao",
              "obra", "valor")
    return [dict(zip(campos, (c, d, (so or "").strip() or "(sem contraparte)",
                              cat, an, sit, ob, float(v or 0))))
            for c, d, so, cat, an, sit, ob, v in consultar(
        f"""SELECT codigo_lancamento, data, razao_social, categoria, analise,
                   situacao, {OBRA_OU_SEM}, pago_recebido
              FROM fato{where}
             ORDER BY ABS(pago_recebido) DESC LIMIT 50""", params)]


def titulos_que_sumiram(valor_procurado=None) -> dict:
    """Títulos que a carga BAIXOU do OMIE e que não viraram linha nenhuma.

    Se um lançamento existe no OMIE e não aparece em tela nenhuma, só há dois
    caminhos: a carga nunca o baixou, ou baixou e descartou ao montar as linhas.
    Esta conferência separa os dois — e é a diferença entre caçar na carga e
    caçar na montagem.

    Hoje o único descarte declarado é o status CANCELADO (`sync/fato.py`), mas
    a conferência não confia nisso: ela compara as duas tabelas e mostra o que
    achar, com o status de cada um. Se aparecer status que ninguém esperava, é
    justamente o que se quer saber.

    Nasceu em 13/09/2026, depois de uma tarde inteira de hipóteses minhas
    derrubadas uma a uma pelo dono sobre uma devolução de aporte de 24/12/2025
    que não aparecia. Perguntar ao banco é mais barato que adivinhar."""
    sql_base = """
          FROM titulos t
         WHERE NOT EXISTS (SELECT 1 FROM fato f
                            WHERE f.codigo_lancamento = t.codigo_lancamento_omie)"""

    (quantos, valor) = consultar(
        f"SELECT COUNT(*), COALESCE(SUM(ABS(valor_documento))::numeric, 0){sql_base}")[0]

    por_status = [{"situacao": st or "(vazio)", "quantos": n, "valor": float(v or 0)}
                  for st, n, v in consultar(
        f"""SELECT t.status_titulo, COUNT(*),
                   COALESCE(SUM(ABS(t.valor_documento))::numeric, 0){sql_base}
             GROUP BY 1 ORDER BY 2 DESC LIMIT 30""")]

    # e, quando se procura um valor especifico, os titulos com aquele valor —
    # esteja ele nas linhas ou nao. E a pergunta "onde foi parar este numero?"
    achados = []
    if valor_procurado is not None:
        achados = [{"codigo": c, "natureza": nat, "valor": float(v or 0),
                    "situacao": st or "", "documento": doc or "",
                    "vencimento": venc or "", "nas_telas": bool(n)}
                   for c, nat, v, st, doc, venc, n in consultar(
            """SELECT t.codigo_lancamento_omie, t.natureza, t.valor_documento,
                      t.status_titulo, t.numero_documento, t.data_vencimento,
                      (SELECT COUNT(*) FROM fato f
                        WHERE f.codigo_lancamento = t.codigo_lancamento_omie)
                 FROM titulos t
                -- POR TOLERANCIA, nao por igualdade. Ate a migracao 010,
                -- `valor_documento` era REAL (ponto flutuante de 4 bytes), que
                -- so guarda ~7 digitos significativos: 784.647,07 virava
                -- 784.647,06 ao ser gravado, e comparar exato nunca achava
                -- lancamento grande nenhum. Hoje a coluna e NUMERIC e guarda o
                -- centavo — mas os valores ANTIGOS so voltam a ficar certos
                -- depois de uma carga inicial, e quem procura aqui esta
                -- justamente atras de um numero que nao esta batendo. A folga
                -- de meio real fica: ela acha o que se procura sem confundir
                -- titulos, e nao custa nada.
                WHERE ABS(ABS(COALESCE(t.valor_documento, 0)) - ?) < 0.5
                ORDER BY 1 LIMIT 50""", [valor_procurado])]

    return {"quantos": quantos or 0, "valor": float(valor or 0),
            "por_status": por_status, "achados": achados,
            "procurado": valor_procurado}


# ---------------------------------------------------------------------------
# Rateio da Administração — as séries mensais que a simulação consome
# ---------------------------------------------------------------------------
# Tudo aqui é REGIME DE CAIXA (só o que foi pago ou recebido), porque a pergunta
# da tela é sobre necessidade de caixa: quem ficou negativo, quando, e quanto
# disso o banco cobriu em juros.

def receita_mensal_por_obra() -> list[tuple]:
    """Receita recebida por mês × obra, sem o imposto retido na fonte.

    É um dos dois critérios de rateio: quem faturou mais no período carrega
    mais da administração."""
    sql = f"""
        SELECT date_trunc('month', data)::date, {OBRA_OU_SEM},
               SUM({EXECUTADO})
          FROM fato
         WHERE {_BASE_CAIXA} AND analise = 'DRE'
           AND tipo = ? AND NOT ({RETIDO})
         GROUP BY 1, 2 ORDER BY 1, 2"""
    return [(m, o, float(v or 0)) for m, o, v in consultar(sql, [REC])]


def pessoal_mensal_por_obra(grupo_pessoal: str) -> list[tuple]:
    """Despesa com pessoal paga por mês × obra, em módulo.

    O outro critério: obra com mais gente consome mais estrutura."""
    sql = f"""
        SELECT date_trunc('month', data)::date, {OBRA_OU_SEM},
               ABS(SUM({EXECUTADO_COM_ENCARGO}))
          FROM fato
         WHERE {_BASE_CAIXA} AND analise = 'DRE' AND tipo = ?
           AND TRIM(COALESCE(grupo,'')) = ?
         GROUP BY 1, 2 ORDER BY 1, 2"""
    return [(m, o, float(v or 0)) for m, o, v in consultar(sql, [PAG, grupo_pessoal])]


def custo_da_matriz_por_categoria(depto_matriz: str) -> list[dict]:
    """O que a matriz gastou, por categoria — para escolher o que suprimir do
    bolo antes de ratear."""
    sql = f"""
        SELECT COALESCE(NULLIF(TRIM(categoria),''), '(sem categoria)'),
               SUM({EXECUTADO_COM_ENCARGO})
          FROM fato
         WHERE {_BASE_CAIXA} AND analise = 'DRE' AND tipo = ?
           AND {OBRA_OU_SEM} = ?
         GROUP BY 1 HAVING ABS(SUM({EXECUTADO_COM_ENCARGO})) > 0.005
         ORDER BY SUM({EXECUTADO_COM_ENCARGO}) ASC"""
    return [{"categoria": c, "valor": float(v or 0)}
            for c, v in consultar(sql, [PAG, depto_matriz])]


def matriz_mensal(depto_matriz: str, categoria_juros: str,
                  suprimidas=()) -> list[tuple]:
    """Despesa e receita da matriz, mês a mês, já sem os juros de empréstimo e
    sem as categorias suprimidas.

    Os juros saem daqui porque eles NÃO são rateados pelo critério: são
    alocados a quem estava com o caixa negativo, que é outra conta."""
    fora = [categoria_juros] + [c for c in suprimidas if c]
    sql = f"""
        SELECT date_trunc('month', data)::date,
               SUM(CASE WHEN tipo = ? AND NOT (TRIM(COALESCE(categoria,'')) = ANY(?))
                        THEN {EXECUTADO_COM_ENCARGO} ELSE 0 END),
               SUM(CASE WHEN tipo = ? THEN {EXECUTADO} ELSE 0 END)
          FROM fato
         WHERE {_BASE_CAIXA} AND analise = 'DRE'
           AND {OBRA_OU_SEM} = ?
         GROUP BY 1 ORDER BY 1"""
    return [(m, float(d or 0), float(r or 0))
            for m, d, r in consultar(sql, [PAG, fora, REC, depto_matriz])]


def juros_de_emprestimo_mensal(categoria_juros: str) -> list[tuple]:
    """Os juros de empréstimo pagos por mês, em QUALQUER departamento.

    Eles não pertencem a uma obra: são o preço de o caixa da empresa ter ficado
    negativo, e a tela os aloca a quem cavou o buraco."""
    sql = f"""
        SELECT date_trunc('month', data)::date, SUM({EXECUTADO_COM_ENCARGO})
          FROM fato
         WHERE {_BASE_CAIXA} AND tipo = ?
           AND TRIM(COALESCE(categoria,'')) = ?
         GROUP BY 1 ORDER BY 1"""
    return [(m, float(v or 0)) for m, v in consultar(sql, [PAG, categoria_juros])]


def departamentos_administrativos() -> list[str]:
    """As obras cujo nome parece de administração — para a tela já sugerir a
    matriz em vez de fazer procurar numa lista de 174."""
    return [d for (d,) in consultar(
        f"SELECT DISTINCT {OBRA_OU_SEM} FROM fato "
        " WHERE departamento ILIKE '%BWS%' OR departamento ILIKE '%CONS%' "
        "    OR departamento ILIKE '%ADM%' ORDER BY 1")]


# ---------------------------------------------------------------------------
# A montagem do cenário: as contas da matriz, abertas para escolher
# ---------------------------------------------------------------------------
# O dono, sobre a tela antiga de regras: "não adianta uma coisa que eu tenho que
# digitar coisa por coisa para sair colocando. Tem que ser um negócio realmente
# fácil de fazer."
#
# Por isso a tela não pede cadastro: mostra o que a matriz gastou, do maior para
# o menor, com um campo de percentual ao lado. Estas consultas alimentam essa
# lista em três níveis — grupo, categoria e o lançamento individual.

def lancamentos_administrativos(deptos_admin, medida: str = "comprometido", *,
                                grupo: str = "", categoria: str = "",
                                limite: int = 400) -> list[dict]:
    """Os lançamentos da matriz, um a um, para marcar exceções.

    O teto existe porque esta lista é para ESCOLHER, não para somar: quem quer o
    total tem o nível de cima, que sai agregado do banco. Sem teto, abrir um
    grupo grande traria milhares de linhas para dentro de um `select` da tela."""
    if not deptos_admin:
        return []
    valor = _medida_de_despesa(medida)
    onde = ["analise = 'DRE'", "tipo = ?", "departamento = ANY(?)"]
    params = [PAG, list(deptos_admin)]
    if grupo:
        onde.append("TRIM(COALESCE(grupo,'')) = ?")
        params.append(grupo)
    if categoria:
        onde.append("TRIM(COALESCE(categoria,'')) = ?")
        params.append(categoria)
    sql = f"""
        SELECT codigo_lancamento,
               COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'),
               data,
               TRIM(COALESCE(grupo,'')), TRIM(COALESCE(categoria,'')),
               COALESCE(razao_social,''), COALESCE(numero_documento,''),
               COALESCE(observacao,''), COALESCE(link,''),
               SUM({valor})
          FROM fato
         WHERE {' AND '.join(onde)}
         GROUP BY 1,2,3,4,5,6,7,8,9
        HAVING ABS(SUM({valor})) > 0.005
         ORDER BY ABS(SUM({valor})) DESC
         LIMIT {int(limite)}"""
    campos = ("codigo", "mes", "data", "grupo", "categoria", "credor",
              "documento", "observacao", "link", "valor")
    return [dict(zip(campos, (str(l[0] or ""), l[1], l[2], l[3], l[4], l[5],
                              l[6], l[7], l[8], float(l[9] or 0))))
            for l in consultar(sql, params)]


def lancamentos_administrativos_por_codigo(deptos_admin, codigos,
                                           medida: str = "comprometido") -> list[dict]:
    """Só os lançamentos marcados um a um — para a conta tirá-los do agregado.

    Sem isto o mesmo dinheiro contaria duas vezes: uma dentro do balde da
    categoria e outra com o percentual próprio."""
    if not deptos_admin or not codigos:
        return []
    valor = _medida_de_despesa(medida)
    sql = f"""
        SELECT codigo_lancamento,
               COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'),
               TRIM(COALESCE(grupo,'')), TRIM(COALESCE(categoria,'')),
               SUM({valor}),
               MIN(COALESCE(razao_social,'')), MIN(COALESCE(numero_documento,''))
          FROM fato
         WHERE analise = 'DRE' AND tipo = ? AND departamento = ANY(?)
           AND codigo_lancamento = ANY(?)
         GROUP BY 1, 2, 3, 4"""
    numeros = []
    for c in codigos:
        try:
            numeros.append(int(str(c).strip()))
        except (TypeError, ValueError):
            continue
    if not numeros:
        return []
    # credor e documento vao junto para a tela dizer O QUE foi marcado — um
    # numero de lancamento sozinho nao diz nada a quem for auditar
    campos = ("codigo", "mes", "grupo", "categoria", "valor", "credor", "documento")
    return [dict(zip(campos, (str(l[0] or ""), l[1], l[2], l[3], float(l[4] or 0),
                              l[5], l[6])))
            for l in consultar(sql, [PAG, list(deptos_admin), numeros])]


def receita_por_obra_mes(medida: str = "comprometido") -> list[tuple]:
    """O faturamento de cada obra, mês a mês — a outra régua do rateio.

    Mesmo formato do `custo_de_pessoal_por_obra_mes` (mês em texto 'AAAA-MM'),
    para as duas réguas serem intercambiáveis sem a conta saber qual é qual.
    As retenções ficam de fora: imposto retido não é dinheiro da obra."""
    valor = _medida(medida)
    sql = f"""
        SELECT COALESCE(to_char(data, 'YYYY-MM'), '{SEM_DATA}'),
               {OBRA_OU_SEM},
               ABS(SUM({valor}))
          FROM fato
         WHERE analise = 'DRE' AND tipo = ? AND NOT ({RETIDO})
         GROUP BY 1, 2 HAVING ABS(SUM({valor})) > 0.005"""
    return [(mes, obra, float(v or 0))
            for mes, obra, v in consultar(sql, [REC])]
