# -*- coding: utf-8 -*-
"""
OS CARDS DO PIPEFY PARA A FOLHA.

⚠️ DESDE 02/10/2026 É SÓ A SOLICITAÇÃO DE PAGAMENTO, COM RATEIO MÚLTIPLO — uma SP
por conta, no padrão do script do BeeVale. O card de Despesa com Colaboradores
deixou de ser criado (decisão do dono). Ver o bloco "SÓ A SOLICITAÇÃO DE
PAGAMENTO" mais abaixo. O histórico abaixo conta como se chegou ao desenho do
Make, que valeu até essa data.

⚠️ A FORMA DO CARD NÃO É DECISÃO MINHA: É A DO MAKE, QUE FUNCIONA. Em 01/10/2026 o
primeiro lançamento de verdade foi recusado pelo Pipefy ("Data de Vencimento",
"Tipo de Despesa", "Requisição Solicitada por um Terceiro?", "Responsável pela
Solicitação"… obrigatórios), e o dono cobrou com razão:

    "existe a criação de dois cards. O script está funcionando 100%, você precisa
     olhar com detalhe a forma que o card é criado, não precisa errar."

A primeira versão deste módulo criava UM card por conta, só com descrição, valor e
links — um desenho meu, não o do processo. O cenário `DP - FIN - Botão Folha de
Pagamento (🆕SP)` (blueprint lido campo a campo em 01/10/2026) faz outra coisa:

1. **UM card de Despesa com Colaboradores** (pipe 301433085) por pagamento, com o
   total, o tipo de despesa do grupo, "Pgt Conjunto", o responsável fixo, e **cada
   obra como centro de custo** (até 75 pares centro + valor), mais o **código do
   departamento no OMIE** de cada uma — que vem da aba "C. Diários".
2. **UMA SP de Transferência de Recursos** (pipe 301426645) **por conta de
   origem**, com o valor daquela conta, ligada ao card de Despesa.
3. Os links das planilhas de pagamento e de análise no card de Despesa, e as
   conexões nos dois sentidos (`conex_o_dc` na SP, `conex_o_sp` na Despesa).

Os números fixos (responsável, banco, etiqueta, tipos de despesa, Pix e CNPJ da
BWS) são os do blueprint, copiados como estão. Os ids dos campos também — inclusive
os pares 62 e 72, cujo campo de valor tem id `valor_centro_de_custo_63` e
`valor_centro_de_custo_73`: é o id que o Pipefy deu ao campo quando foi criado, e o
Make grava certo. (Eu tinha anotado isso como defeito em `docs/FOLHA_DE_PAGAMENTO.md`
§4; não é — corrigido lá.)

O que continua decisão do dono (26/09/2026): **gerar o arquivo não cria card**
(D14) e **regerar é normal** (D15). Lançar é um passo à parte, com prévia.

⚠️ NADA AQUI É ADIVINHADO. Antes de criar qualquer coisa, a prévia confere: todos
os campos que o Make usa existem nos dois pipes; toda obra tem código do OMIE e
centro de custo no Pipefy; toda linha tem conta; e o que está fechado bate, conta
a conta, com os arquivos gerados. Qualquer falha impede o lançamento inteiro — card
criado no Pipefy não se apaga por aqui.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import unicodedata
from decimal import ROUND_DOWN, Decimal

from . import folha_geracao as geracao
from . import formatos, pipefy

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

# Os dois pipes do cenário.
PIPE_DESPESA = "301433085"        # Despesa com Colaboradores
PIPE_SP = "301426645"             # Solicitações Financeiro (SP)

# Os números fixos do blueprint. São ids de registros do Pipefy.
RESPONSAVEL = "383926874"         # responsável pela solicitação / solicitante
BANCO_DO_PAGAMENTO = "395832004"
# ⚠️ AS ETIQUETAS DA SP (05/10/2026). Era UMA, "Transferência de recurso"
# (307726886), e o dono mandou trocar: *"eu não quero mais utilizar essa
# etiqueta (…) você vai sempre colocar duas etiquetas"* — "Folha de Pgt" em
# toda SP das folhas (folha, alimentação, transporte, DC e diaristas), mais a
# do destino do arquivo daquela conta: "BeeVale" ou "Somapay".
ETIQUETA_FOLHA = "318116255"
ETIQUETA_DO_DESTINO = {"beevale": "317521565", "somapay": "318116254"}
PIX_DA_BWS = "a7398865-d869-4437-b7a9-fc6fe904c4d7"   # chave aleatória
CNPJ_DA_BWS = "00.079.526/0001-09"

# O "grupo" do Make — o nome que vira título da SP — e os dois tipos de despesa
# (um id no pipe de Despesa, outro no de SP). Copiados do `switch(157.grupo; …)`.
GRUPOS = {
    ("folha", "quinzena"): ("Folha de Pagamento - Quinzena", "386045084", "383928967"),
    ("folha", "fim_de_mes"): ("Folha de Pagamento - Fim de Mês", "386045084", "383928967"),
    ("transporte", ""): ("Auxílio Transporte", "386045060", "383846062"),
    ("alimentacao", ""): ("Auxílio Alimentação", "386045055", "383846061"),
    ("diaria", ""): ("Pagamento de Diárias", "404847566", "383928967"),
    ("gratificacao", ""): ("Gratificados e Mensalistas", "386045084", "383928967"),
    # A DC (03/10/2026): o tipo de despesa vem de cada linha (o Record ID do
    # tipo de despesa na aba "Plano Financeiro"), não do grupo.
    ("dc", ""): ("Despesas com Colaboradores", "", ""),
}

# O que o `geraspbeevale.gs` do dono punha na SP da DC e as outras não têm: o
# campo de automação "BeeVale". (A etiqueta dele, 317521565, é a "BeeVale" —
# hoje entra pela regra geral das etiquetas, `etiquetas_da_sp`.)
AUTOMACAO_DA_DC = "BeeVale"

# Os tetos do cenário: 75 pares de centro de custo no card de Despesa, e a conexão
# de volta liga até 10 SPs.
MAXIMO_DE_CENTROS = 75
MAXIMO_DE_SPS = 10

# Os ids dos campos de valor que não seguem o padrão — ver a docstring.
_VALOR_DO_PAR = {62: "valor_centro_de_custo_63", 63: "valor_centro_de_custo_63_1",
                 72: "valor_centro_de_custo_73", 73: "valor_centro_de_custo_73_1"}

# O relógio do Make é o da BWS.
FUSO = "America/Fortaleza"

CHAVE_DO_ANDAMENTO = "folha_cards_rodada:{}"


# ===========================================================================
# ⚠️ DESDE 02/10/2026: SÓ A SOLICITAÇÃO DE PAGAMENTO, COM RATEIO MÚLTIPLO
#
# O dono: *"para a folha da contabilidade, a gente gerava dois cards (…) um
# dentro do processo do financeiro, outro dentro do processo do despesa
# colaborador. Então, eu quero matar um. Eu quero deixar só o do financeiro (…)
# gerar um card com rateio múltiplo, nesse padrão aí que você já identificou."*
#
# O padrão é o do script do BeeVale (planilha de Diaristas): uma SP por conta,
# "Rateio múltiplo entre centros de custo" = Sim, e o campo "Rateio múltiplo" com
# o trecho de JSON do OMIE — a distribuição por obra (código do departamento no
# OMIE, nome, percentual com 7 casas fechando 100%) e a categoria. Pix, chave
# aleatória "Atualizar Chave". A descrição diz tudo: competência, obras, valor
# por obra, os links. O card de Despesa com Colaboradores NÃO é mais criado.
#
# ⚠️ OS CÓDIGOS NÃO SÃO ESCRITOS AQUI, SÃO PROCURADOS PELO NOME: o tipo de
# despesa (registro do Pipefy, no campo "Tipo de Despesa" da SP) e a categoria
# do OMIE (no plano financeiro que a carga do painel traz). Na planilha eles
# vinham da aba oculta "PlanoFinanceiro", pela descrição da verba — a mesma
# descrição usada aqui. Não achou, ou achou dois: a prévia diz e nada é criado.
# ===========================================================================
DESCRICAO_DA_VERBA = {
    "folha": "Salários e Ordenados",
    "diaria": "Salários e Ordenados",
    "gratificacao": "Gratificações e Extras",
    "alimentacao": "Despesas com Alimentação",
    "transporte": "Despesas com Transporte",
}

# Quem recebe, conforme o destino do arquivo daquela conta. BeeVale: os dados do
# script do BeeVale. SomaPay: a transferência vai para a conta Somapay da própria
# BWS (as contas Somapay usam o CNPJ da Somapay — ver baixabradesco/README), então
# fica o CNPJ da BWS, como a SP do Make já fazia.
FAVORECIDO = {
    "beevale": {"local": "BEE VALE PAGAMENTOS E BENEFICIOS LTDA",
                "cnpj": "31.749.082/0001-03", "documentacao": "BeeVale"},
    # ⚠️ O "local" É O "NOME DO CREDOR", obrigatório no pipe. Ia vazio e o Pipefy
    # recusou o primeiro lançamento (03/10/2026). O dono: *"o credor é BWS
    # CONSTRUCOES LTDA"*.
    "somapay": {"local": "BWS CONSTRUCOES LTDA", "cnpj": CNPJ_DA_BWS,
                "documentacao": ""},
}
CHAVE_PIX_A_ATUALIZAR = "Atualizar Chave"
CASAS_DO_PERCENTUAL = 7


class ErroDosCards(RuntimeError):
    """Não deu para lançar. A frase vai inteira para a tela."""


# ---------------------------------------------------------------------------
# PEQUENOS
# ---------------------------------------------------------------------------
def _chave(texto) -> str:
    sem = unicodedata.normalize("NFKD", " ".join(str(texto or "").split()))
    return "".join(c for c in sem if not unicodedata.combining(c)).upper()


def _valor(numero) -> str:
    """Dinheiro como o Pipefy aceita: ponto decimal, sem milhar."""
    return f"{Decimal(str(numero or 0)).quantize(CENTAVO)}"


def _agora() -> _dt.datetime:
    try:
        from zoneinfo import ZoneInfo
        return _dt.datetime.now(ZoneInfo(FUSO))
    except Exception:  # noqa: BLE001 — sem base de fusos, UTC-3 fixo
        return _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=-3)))


def campos_do_par(n: int) -> tuple:
    """(centro de custo, valor, departamento OMIE) do par `n`, com os ids do Make."""
    return (f"centro_de_custo_{n}",
            _VALOR_DO_PAR.get(n, f"valor_centro_de_custo_{n}"),
            f"departamento_omie_c_digo_centro_de_custo_{n}")


def grupo_da_verba(verba: str, tipo: str) -> tuple:
    verba = str(verba or "").strip().lower()
    achado = GRUPOS.get((verba, tipo if verba == "folha" else ""))
    if not achado:
        raise ErroDosCards(
            f'não é possível lançar a verba "{geracao.rotulo_da_verba(verba)}" no Pipefy: '
            "o cenário do Make não possui grupo para ela.")
    return achado


# ⚠️ A PRIMEIRA LINHA DA DESCRIÇÃO É "Conta Origem: <conta>", EXATAMENTE ASSIM.
# O dono, 06/10/2026: *"a gente coloca conta origem dois pontos e o número da
# conta (…) a Análise de SPs faz uma leitura e, a partir dela, (…) a gente
# utiliza esse parâmetro para poder substituir a conta, alterar a conta. (…) Tem
# que colocar em toda a geração de arquivos, determinando qual é a conta de saída
# do recurso."* É LIDA POR MÁQUINA: não traduzir, não mudar maiúscula, não pôr
# nada antes. Exemplo dele: "Conta Origem: 92945-8".
ROTULO_CONTA_ORIGEM = "Conta Origem: "


def _com_conta_origem(conta: str, linhas: list) -> list:
    """As linhas da descrição com a conta de saída do recurso na PRIMEIRA."""
    conta = " ".join(str(conta or "").split())
    return ([f"{ROTULO_CONTA_ORIGEM}{conta}"] if conta else []) + linhas


def descricao_do_card(competencia: str, tipo: str, verbas, conta: str,
                      pessoas: int, total, link_pagamento: str = "",
                      link_analise: str = "") -> str:
    """O texto do card. ⚠️ ELE É A PROVA: quem abrir o card meses depois precisa
    saber de que competência é, que verbas entraram, de qual conta saiu e onde está
    o arquivo — sem depender de ninguém lembrar."""
    rotulo_tipo = {"quinzena": "Quinzena (dias 1 a 15)",
                   "fim_de_mes": "Fim de mês (16 ao último dia)"}.get(
                       str(tipo or ""), str(tipo or ""))
    linhas = [
        f"Competência: {competencia}",
        f"Pagamento: {rotulo_tipo}" if rotulo_tipo else "",
        "Verbas: " + " + ".join(geracao.rotulo_da_verba(v) for v in (verbas or [])),
        f"Pessoas: {pessoas}",
        # O valor com vírgula e ponto de milhar: o card é lido por gente, e
        # "4200.00" num card de despesa se confunde com quatro reais.
        f"Total: R$ {formatos.moeda(total)}",
    ]
    if link_pagamento:
        linhas.append(f"Planilha de pagamento: {link_pagamento}")
    if link_analise:
        linhas.append(f"Planilha de análise: {link_analise}")
    corpo = _com_conta_origem(conta, [l for l in linhas if l])
    return "\n".join(corpo + ["", "Gerado pelo Análise de SPs."])


# ---------------------------------------------------------------------------
# A RODADA: os arquivos gerados juntos, e o arquivo de análise que os fecha
# ---------------------------------------------------------------------------
def rodada(analise_id: int) -> dict:
    """O arquivo de análise e os de pagamento gerados NA MESMA VEZ.

    `gerar` registra os de pagamento e, por último, o de análise — então a rodada
    é tudo o que entrou no log, da mesma competência e pagamento, entre a análise
    anterior e esta."""
    from . import folha_pagamento as fpg
    todos = fpg.log(teto=400)
    analise = next((a for a in todos if a["id"] == int(analise_id)), None)
    if not analise:
        raise ErroDosCards("arquivo não encontrado no log de arquivos gerados.")
    if analise["destino"] != fpg.ANALISE:
        raise ErroDosCards(
            "o lançamento no Pipefy é feito pelo arquivo de ANÁLISE, que consolida o "
            "pagamento completo — não por um arquivo de conta.")
    mesmos = [a for a in todos if a["ano"] == analise["ano"]
              and a["mes"] == analise["mes"] and a["tipo"] == analise["tipo"]]
    piso = max((a["id"] for a in mesmos
                if a["destino"] == fpg.ANALISE and a["id"] < analise["id"]),
               default=0)
    da_rodada = [a for a in mesmos if a["destino"] != fpg.ANALISE
                 and piso < a["id"] < analise["id"]]
    # O PDF do relatório de cada conta é da rodada, mas não é arquivo de
    # pagamento: não soma no total nem vira SP — vai como link no card.
    arquivos = sorted((a for a in da_rodada if a["destino"] != fpg.RELATORIO),
                      key=lambda a: a["id"])
    relatorios = {a["conta"]: a for a in da_rodada if a["destino"] == fpg.RELATORIO}
    if not arquivos:
        raise ErroDosCards("arquivos de pagamento desta rodada não encontrados.")
    return {"analise": analise, "arquivos": arquivos, "relatorios": relatorios,
            "verbas": [v for v in (analise["verbas"] or "").split("+") if v]}


def _codigos_omie() -> dict:
    """`{obra: código do OMIE}` — da aba "C. Diários" (Código Primário → Código
    Omie), que é de onde o dono disse que ele vem."""
    from .db import consultar
    saida = {}
    for nome, codigo in consultar(
            "SELECT nome, coalesce(codigo, '') FROM analisesps.referencias_rateio "
            " WHERE tipo = 'obra'"):
        if _chave(nome) and str(codigo or "").strip():
            saida[_chave(nome)] = str(codigo).strip()
    # A apropriação pode identificar a obra pelo próprio código do OMIE (é o que o
    # ponto às vezes escreve — ver `folha_pagamento.conta_por_obra`).
    for codigo in list(saida.values()):
        saida.setdefault(_chave(codigo), codigo)
    return saida


def _achador(campo: dict, sondas=(), recusas=()) -> tuple:
    """Como um campo do Pipefy recebe um valor escolhido pelo NOME.

    Devolve (função nome → valor ou None, descrição do campo). Conexão com tabela:
    o id do registro de mesmo nome; conexão com pipe: o card de mesmo título;
    lista de opções: a opção de mesmo nome; texto: o próprio nome. Dois
    registros com o mesmo nome não casam — nada é escolhido no escuro.

    ⚠️ QUANDO O PIPEFY NÃO DIZ A QUE A CONEXÃO ESTÁ LIGADA (03/10/2026: a prévia
    de 09/2026 parou em "conexão de tipo não suportado"), a tabela é descoberta
    a partir de `sondas` — ids de registros que sabidamente estão nela (os que o
    cenário do Make gravava no Tipo de Despesa). `recusas`: o que o Pipefy
    respondeu à pergunta completa, para a tela dizer o porquê."""
    campo = campo or {}
    tipo = campo.get("tipo") or ""
    ligado = campo.get("ligado_a") or {}
    if tipo == "connector":
        if ligado.get("tipo") == "pipe" and ligado.get("id"):
            pipe_ligado = ligado["id"]

            def achar_card(nome):
                try:
                    cards = pipefy.cards_por_titulo(pipe_ligado, nome)
                except pipefy.ErroDoPipefy:
                    logger.exception("Folha: não consegui procurar %r no pipe %s",
                                     nome, pipe_ligado)
                    return None
                achados = [c for c in cards if _chave(c["nome"]) == _chave(nome)]
                return achados[0]["id"] if len(achados) == 1 else None
            return achar_card, f'pipe "{ligado.get("nome") or pipe_ligado}"'
        como = ""
        if ligado.get("tipo") != "tabela" or not ligado.get("id"):
            ligado, falhas = {}, []
            for sonda in sondas or ():
                try:
                    tabela = pipefy.tabela_do_registro(sonda)
                except pipefy.ErroDoPipefy as e:
                    falhas.append(str(e))
                    continue
                if tabela:
                    ligado = {"tipo": "tabela", **tabela}
                    como = " — descoberta pelo registro que o Make usava"
                    break
            if not ligado:
                porque = "; ".join(list(recusas or ()) + falhas)[:600]
                return (lambda nome: None), (
                    "o Pipefy não informou a que tabela a conexão está ligada"
                    + (f" ({porque})" if porque else ""))
        registros = pipefy.registros_da_tabela(ligado["id"])
        por_nome: dict = {}
        for r in registros:
            por_nome.setdefault(_chave(r["nome"]), []).append(r["id"])

        def achar(nome):
            achados = por_nome.get(_chave(nome)) or []
            return achados[0] if len(achados) == 1 else None
        return achar, f'tabela "{ligado.get("nome") or ligado["id"]}"{como}'
    if tipo in ("select", "radio_vertical", "radio_horizontal"):
        opcoes = {_chave(o): o for o in campo.get("opcoes") or []}
        return (lambda nome: opcoes.get(_chave(nome))), "lista de opções"
    return (lambda nome: str(nome or "").strip() or None), "texto"


def _categoria_do_omie(descricao: str) -> tuple:
    """(código, erro) da categoria do OMIE com esta descrição, no espelho do
    painel. Exige UMA, ativa, com a descrição igual (sem acento e caixa)."""
    from .conciliacao_omie import categorias_do_omie
    try:
        # O plano inteiro (poucas centenas de linhas), comparado sem acento: no
        # OMIE a descrição pode estar em maiúsculas e sem acento.
        candidatas = categorias_do_omie(busca="", limite=5000)
    except Exception as e:  # noqa: BLE001
        return "", f"não foi possível ler o plano financeiro do OMIE: {e}"
    iguais = [c for c in candidatas
              if _chave(c["descricao"]) == _chave(descricao) and not c["inativa"]]
    if len(iguais) == 1:
        return str(iguais[0]["codigo"]), ""
    if not iguais:
        return "", (f'categoria "{descricao}" não encontrada no plano financeiro do '
                    "OMIE (espelho da carga do painel).")
    return "", (f'há {len(iguais)} categorias "{descricao}" ativas no OMIE ('
                + ", ".join(c["codigo"] for c in iguais) + ") — não é possível escolher.")


# ---------------------------------------------------------------------------
# O PLANO FINANCEIRO DA PLANILHA — 03/10/2026
#
# O dono: *"Nessa planilha temos essa informação (…) Record ID | Plano
# Financeiro | Record ID | Código Omie T | Código Omie, e o ID para lançar no
# Pipefy seria o Record ID."* É a aba "Plano Financeiro" da planilha das SPs (a
# mesma que o rateio já lê). Dela saem o registro do Tipo de Despesa (Record ID)
# e a categoria do OMIE (Código Omie). O Pipefy e o espelho do painel ficam como
# reserva, para o nome que a aba não tiver.
# ---------------------------------------------------------------------------
def _plano_financeiro() -> tuple:
    """(`{nome sem acento: [{'record_id', 'codigo_omie'}]}`, aviso). Lê a aba na
    hora; se a leitura falhar, usa o que a última sincronização guardou."""
    from . import sincronizacao
    por_nome: dict = {}
    try:
        linhas = sincronizacao.ler_plano_financeiro()
        aviso = ""
    except Exception as e:  # noqa: BLE001 — cai no guardado
        logger.warning("Folha: não consegui ler a aba Plano Financeiro: %s", e)
        aviso = f'aba "Plano Financeiro" não lida ({e})'
        try:
            linhas = [{"nome": n, "record_id": r, "codigo_omie": ""}
                      for n, r in sincronizacao.plano_pipefy_guardado().items()]
        except Exception:  # noqa: BLE001
            logger.exception("Folha: não consegui ler o Plano Financeiro guardado")
            linhas = []
    for l in linhas:
        por_nome.setdefault(_chave(l["nome"]), []).append(l)
    return por_nome, aviso


def _unico(plano: dict, nome: str, campo: str) -> str:
    """O valor de `campo` da linha com este nome — só se for UM só."""
    valores = {str(l.get(campo) or "").strip() for l in plano.get(_chave(nome)) or []}
    valores.discard("")
    return valores.pop() if len(valores) == 1 else ""


def _achador_do_plano(plano: dict, aviso: str, sp: dict) -> tuple:
    """O Tipo de Despesa pelo Record ID da aba; o nome que não estiver nela (ou
    estiver com dois ids diferentes) é procurado no próprio Pipefy."""
    reserva: list = []

    def pelo_pipefy(nome):
        if not reserva:
            try:
                # As sondas: os registros de Tipo de Despesa que o Make gravava.
                sondas = list(dict.fromkeys(g[2] for g in GRUPOS.values() if g[2]))
                reserva.append(_achador(sp["todos"].get("tipo_de_despesa"),
                                        sondas, sp.get("recusas") or ()))
            except pipefy.ErroDoPipefy as e:
                reserva.append(((lambda n: None),
                                f"não foi possível ler os tipos de despesa do Pipefy: {e}"))
        return reserva[0][0](nome)

    def achar(nome):
        return _unico(plano, nome, "record_id") or pelo_pipefy(nome)
    # Para a frase do bloqueio dizer o que o Pipefy respondeu na reserva.
    achar.reserva = reserva

    como = 'Record ID da aba "Plano Financeiro"' + (f" — {aviso}" if aviso else "")
    return achar, como


def _categoria_da_verba(descricao: str, plano: dict) -> tuple:
    """(código, erro): o Código Omie da aba; sem ele, o espelho do painel."""
    codigo = _unico(plano, descricao, "codigo_omie")
    if codigo:
        return codigo, ""
    return _categoria_do_omie(descricao)


def rateio_multiplo(obras: list, codigo_categoria: str, categorias=None) -> str:
    """O texto do campo "Rateio múltiplo": o JSON do OMIE, como o script do
    BeeVale escreve (`_formatRateioText_`). `obras`: [{obra, omie, valor}].
    `categorias`: [{codigo, valor}] quando a SP tem mais de uma (a DC, em que
    cada solicitação tem o seu tipo de despesa); sem elas, uma só, 100%."""
    from .rateio import _percentuais_min_erro
    validas = [o for o in obras if Decimal(str(o["valor"])) > 0]
    pct = _percentuais_min_erro([float(o["valor"]) for o in validas],
                                CASAS_DO_PERCENTUAL)
    distribuicao = [{"cCodDep": o["omie"], "cDesDep": o["obra"], "nPerDep": p,
                     "nValDep": None} for o, p in zip(validas, pct)]
    total = sum((Decimal(str(o["valor"])) for o in validas), Decimal("0.00"))
    cats = [c for c in categorias or [] if Decimal(str(c["valor"])) > 0]
    if len(cats) > 1:
        pct_cat = _percentuais_min_erro([float(c["valor"]) for c in cats],
                                        CASAS_DO_PERCENTUAL)
        categorias = [{"codigo_categoria": c["codigo"], "percentual": p,
                       "valor": float(Decimal(str(c["valor"])).quantize(CENTAVO))}
                      for c, p in zip(cats, pct_cat)]
    else:
        categorias = [{"codigo_categoria": cats[0]["codigo"] if cats else codigo_categoria,
                       "percentual": 100, "valor": float(total.quantize(CENTAVO))}]
    compacto = {"separators": (",", ":"), "ensure_ascii": False}
    return ('"distribuicao": ' + json.dumps(distribuicao, **compacto) + "\n"
            + '"categorias": ' + json.dumps(categorias, **compacto))


# Os campos que o Make usa em cada pipe. Se algum sumir ou mudar de id no Pipefy,
# a prévia diz qual — em vez de o card sair com o valor caindo em lugar nenhum.
CAMPOS_DA_SP = (
    "data", "data_de_pagamento", "descri_o", "valor", "colaborador_solicitante",
    "tipo_de_pagamento", "link_planilha_de_an_lise", "tipo_de_despesa",
    "selecione_o_procedimento", "alimenta_o_de_equipe", "parcelas",
    "chave_pix_aleat_ria", "radio_horizontal_t_tulo", "tipo", "cnpj",
    "rateio_m_ltiplo_entre_centros_de_custo", "rateio_m_ltiplo",
    "a_despesa_gerou_emiss_o_de_nota_fiscal",
    "requisi_o_solicitada_por_um_terceiro", "valida_o_sp_1", "etiquetas")


def _ler_pipe(pipe_id: str) -> dict:
    try:
        pipe = pipefy.campos_do_pipe(pipe_id)
    except pipefy.ErroDoPipefy as e:
        raise ErroDosCards(str(e)) from e
    inicio = pipe.get("campos") or {}
    fases = pipe.get("campos_das_fases") or {}
    return {"nome": pipe.get("nome") or pipe_id, "inicio": inicio,
            "todos": {**fases, **inicio}, "recusas": pipe.get("recusas") or []}


def conferir_pipe() -> dict:
    """Confere que o pipe de SP tem todos os campos usados. NÃO CRIA NADA."""
    pipe = _ler_pipe(PIPE_SP)
    faltam = [c for c in CAMPOS_DA_SP if c not in pipe["todos"]]
    return {"pipes": [{"pipe": PIPE_SP, "nome": pipe["nome"],
                       "quantos_campos": len(pipe["todos"]), "faltam": faltam,
                       "obrigatorios": [d.get("label") or cid
                                        for cid, d in pipe["inicio"].items()
                                        if d.get("obrigatorio")]}]}


# ---------------------------------------------------------------------------
# A PRÉVIA — exatamente o que vai ser criado. NÃO CRIA NADA.
# ---------------------------------------------------------------------------
def previa(analise_id: int, contas=None) -> dict:
    """Os cards que vão ser criados, com cada valor, e o que impede de criar.

    `bloqueios` vazio é a única situação em que `lancar` cria alguma coisa.
    `contas`: só os arquivos dessas contas (None = todos) — ver `_chave_do_lote`."""
    return _previa(analise_id, contas=contas)[0]


# ---------------------------------------------------------------------------
# LANÇAR CONTA POR CONTA — 02/10/2026
#
# O dono: *"Lançar no Pipefy, eu seleciono e gero. Só que só dá para eu gerar
# tudo junto, mas se eu quiser gerar separado, gerar um, não gerar o outro (…)
# eu preciso ter essa liberdade."*
#
# Cada lançamento cria, por verba, UM card de Despesa com as obras das contas
# escolhidas e uma SP por conta escolhida — o mesmo desenho do Make, recortado.
# Lançar todas de uma vez continua sendo exatamente o de antes (a chave do
# andamento é a verba). Lançar só parte cria uma Despesa daquele lote, guardada
# sob "verba@contas"; o que falta sai depois, noutra Despesa. Uma conta já
# lançada não entra de novo em lote nenhum.
# ---------------------------------------------------------------------------
def _chave_do_lote(verba: str, contas, todas) -> str:
    if not contas or set(contas) >= set(todas):
        return verba
    return f"{verba}@{'+'.join(sorted(contas))}"


def contas_lancadas(andamento: dict, verba: str) -> dict:
    """`{conta: sp}` de tudo o que já foi lançado desta verba, em qualquer lote."""
    saida = {}
    for chave, estado in (andamento or {}).items():
        if chave == verba or chave.startswith(verba + "@"):
            for conta, sp in ((estado or {}).get("sps") or {}).items():
                if (sp or {}).get("id"):
                    saida[conta] = sp
    return saida


def _previa(analise_id: int, ler_pipes: bool = True, contas=None) -> tuple:
    """(prévia, pipe de SP) — o pipe lido uma vez só."""
    from . import folha_pagamento as fpg

    r = rodada(analise_id)
    analise, arquivos = r["analise"], r["arquivos"]
    todas_as_contas = sorted({" ".join(str(a["conta"] or "").split()) for a in arquivos})
    escolhidas = ([" ".join(str(c or "").split()) for c in contas]
                  if contas else list(todas_as_contas))
    andamento_atual = _andamento(analise["id"])
    ano, mes, tipo = analise["ano"], analise["mes"], analise["tipo"]
    bloqueios: list = []

    sp = _ler_pipe(PIPE_SP) if ler_pipes else None
    achar_tipo, como_tipo = (lambda nome: nome), "texto"
    plano, aviso_plano = _plano_financeiro()
    if ler_pipes:
        faltam = [c for c in CAMPOS_DA_SP if c not in sp["todos"]]
        if faltam:
            bloqueios.append(
                f'o pipe "{sp["nome"]}" não tem o(s) campo(s) ' + ", ".join(faltam)
                + ". O pipe foi alterado — nenhum card foi criado.")
        achar_tipo, como_tipo = _achador_do_plano(plano, aviso_plano, sp)

    omie = _codigos_omie()
    destino_da_conta = {" ".join(str(a["conta"] or "").split()): a.get("destino") or ""
                        for a in arquivos}
    grupos, esperado_por_conta = [], {}
    for verba in r["verbas"]:
        try:
            nome_grupo, _tipo_dc, _tipo_sp = grupo_da_verba(verba, tipo)
        except ErroDosCards as e:
            bloqueios.append(str(e))
            continue
        if verba == "dc":
            grupo_dc = _grupo_da_dc(analise, r, escolhidas, todas_as_contas,
                                    andamento_atual, achar_tipo, ler_pipes, omie,
                                    destino_da_conta, bloqueios, esperado_por_conta)
            if grupo_dc:
                grupos.append(grupo_dc)
            continue
        try:
            linhas = [l for l in fpg.linhas_para_pagar(ano, mes, tipo, [verba])
                      if l["valor"] > 0
                      and " ".join(str(l.get("conta") or "").split()) in escolhidas]
        except fpg.ErroDoPagamento as e:
            bloqueios.append(str(e))
            continue
        chave = _chave_do_lote(verba, escolhidas, todas_as_contas)
        ja = contas_lancadas(andamento_atual, verba)
        repetidas = [c for c in escolhidas
                     if c in ja and ((andamento_atual.get(chave) or {}).get("sps") or {}).get(c) is None]
        if repetidas:
            bloqueios.append(
                f"{geracao.rotulo_da_verba(verba)}: a(s) conta(s) "
                + ", ".join(repetidas) + " já foi(ram) lançada(s) no Pipefy. "
                "Desmarque-a(s) para lançar as demais.")
        if not linhas:
            continue

        # O tipo de despesa e a categoria do OMIE, pelo nome da verba.
        descricao_verba = DESCRICAO_DA_VERBA.get(verba, "")
        tipo_sp = achar_tipo(descricao_verba) if descricao_verba else None
        if ler_pipes and not tipo_sp:
            reserva = getattr(achar_tipo, "reserva", None)
            bloqueios.append(
                f'tipo de despesa "{descricao_verba}" sem Record ID na aba "Plano '
                'Financeiro" da planilha das SPs (ou com dois ids diferentes)'
                + (f"; no Pipefy também não: {reserva[0][1]}" if reserva else "")
                + (f" — {como_tipo}" if "não lida" in como_tipo else "") + ".")
        categoria, erro_categoria = (_categoria_da_verba(descricao_verba, plano)
                                     if descricao_verba else ("", "verba sem categoria"))
        if erro_categoria:
            bloqueios.append(erro_categoria)

        por_conta: dict = {}
        for l in linhas:
            conta = " ".join(str(l.get("conta") or "").split())
            obra = _chave(l["obra"]) or "(SEM OBRA)"
            c = por_conta.setdefault(conta, {"obras": {}, "pessoas": set(),
                                             "valor": Decimal("0.00")})
            c["obras"][obra] = c["obras"].get(obra, Decimal("0.00")) + l["valor"]
            c["pessoas"].add(l["cpf"])
            c["valor"] += l["valor"]
        sps, pessoas = [], set()
        for conta, c in sorted(por_conta.items()):
            esperado_por_conta[conta] = esperado_por_conta.get(
                conta, Decimal("0.00")) + c["valor"]
            pessoas |= c["pessoas"]
            if not conta:
                bloqueios.append(
                    f"{geracao.rotulo_da_verba(verba)}: R$ {formatos.moeda(c['valor'])} "
                    'sem conta de origem (obra sem conta na aba "C. Diários").')
            obras = []
            for obra, valor in sorted(c["obras"].items(), key=lambda x: -x[1]):
                codigo = omie.get(obra, "")
                if not codigo:
                    bloqueios.append(
                        f'a obra "{obra}" não tem Código Omie na aba "C. Diários".')
                obras.append({"obra": obra, "valor": valor, "omie": codigo})
            arquivo = next((a for a in arquivos if a["conta"] == conta
                            and verba in (a["verbas"] or "").split("+")), {})
            destino = destino_da_conta.get(conta) or ""
            sp_item = {"conta": conta, "valor": c["valor"], "pessoas": len(c["pessoas"]),
                       "link": arquivo.get("link") or "", "destino": destino,
                       "link_relatorio": ((r.get("relatorios") or {}).get(conta)
                                          or {}).get("link") or "",
                       "obras": obras,
                       "rateio": rateio_multiplo(obras, categoria) if obras else ""}
            sp_item["etiquetas"] = rotulo_das_etiquetas(sp_item)
            sp_item["descricao"] = descricao_da_sp(
                analise["competencia"], tipo, verba, sp_item, analise.get("link") or "")
            sps.append(sp_item)

        grupos.append({
            "verba": verba, "chave": chave,
            "rotulo_verba": geracao.rotulo_da_verba(verba),
            "grupo": nome_grupo, "tipo_sp": tipo_sp or "",
            "tipo_despesa": descricao_verba, "categoria": categoria,
            "total": sum((x["valor"] for x in sps), Decimal("0.00")),
            "pessoas": len(pessoas), "sps": sps})

    # ⚠️ O QUE ESTÁ FECHADO TEM DE SER O QUE FOI GERADO. Se alguém refez o
    # fechamento depois de gerar, o card contaria uma história e o arquivo outra.
    gerado_por_conta: dict = {}
    for a in arquivos:
        if " ".join(str(a["conta"] or "").split()) not in escolhidas:
            continue
        gerado_por_conta[a["conta"]] = gerado_por_conta.get(
            a["conta"], Decimal("0.00")) + Decimal(str(a["total"]))
    if grupos and gerado_por_conta != esperado_por_conta:
        bloqueios.append(
            "o fechamento foi alterado após a geração destes arquivos (divergência "
            "nos valores por conta). Gere os arquivos novamente antes de lançar.")

    # O RELATÓRIO EM PDF (05/10/2026): a SP leva o link dele; geração sem PDF
    # (de antes de 03/10/2026, ou cujo PDF falhou) iria sem, calada. O dono:
    # *"o relatório em PDF também está indo junto? (…) aqui não está informando
    # nada"*. Não bloqueia — avisa, e diz como resolver.
    sem_pdf = sorted({sp["conta"] or "(sem conta)" for g in grupos for sp in g["sps"]
                      if not sp.get("link_relatorio")})
    avisos = ([f"A(s) conta(s) {', '.join(sem_pdf)} não têm o relatório em PDF nesta "
               "geração (gerada antes de 03/10/2026, ou o PDF falhou): a SP vai sem o "
               "link do relatório. Para ir com ele, exclua esta geração em Arquivos "
               "gerados e gere de novo."] if sem_pdf else [])

    andamento = andamento_atual
    if not grupos and not bloqueios:
        bloqueios.append("nenhum valor a lançar nas contas selecionadas.")
    lancadas = sorted({c for v in r["verbas"] for c in contas_lancadas(andamento, v)})
    return {"analise": analise["id"], "competencia": analise["competencia"],
            "contas": todas_as_contas, "escolhidas": escolhidas,
            "contas_lancadas": lancadas,
            "tipo": tipo, "link_analise": analise["link"],
            "como_tipo": como_tipo, "grupos": grupos,
            "bloqueios": list(dict.fromkeys(bloqueios)), "avisos": avisos,
            "andamento": andamento, "ja_lancado": _completo(andamento, grupos),
            "arquivos": [a["id"] for a in arquivos]}, sp


# ---------------------------------------------------------------------------
# A DC — DESPESAS COM COLABORADORES (03/10/2026)
#
# O dono: *"faz uma movimentação dos cards para uma outra fase (…) e gera uma
# solicitação de pagamento (…) colocar os links (…) do arquivo de pagamento"*.
# É o que o `geraspbeevale.gs` fazia, conta por conta: uma SP com o rateio por
# obra E por categoria (cada solicitação tem o seu tipo de despesa), o tipo de
# despesa da SP = o da maior categoria, e depois os cards de origem marcados
# (`mover_card` = Sim) e movidos para a fase de processados.
#
# As linhas vêm do LOTE guardado na geração (`dc.lote_da_analise`), não da
# planilha: o que vai para o Pipefy é exatamente o que foi para o arquivo.
# ---------------------------------------------------------------------------
def _grupo_da_dc(analise, r, escolhidas, todas_as_contas, andamento_atual,
                 achar_tipo, ler_pipes, omie, destino_da_conta, bloqueios,
                 esperado_por_conta):
    from . import dc
    lote = dc.lote_da_analise(analise["id"])
    if not lote:
        bloqueios.append(
            "Despesas com colaboradores: o registro do que esta geração levou não foi "
            "encontrado (gerada antes da atualização do banco?). Exclua e gere de novo.")
        return None
    chave = _chave_do_lote("dc", escolhidas, todas_as_contas)
    ja = contas_lancadas(andamento_atual, "dc")
    repetidas = [c for c in escolhidas
                 if c in ja and ((andamento_atual.get(chave) or {}).get("sps") or {}).get(c) is None]
    if repetidas:
        bloqueios.append("Despesas com colaboradores: a(s) conta(s) " + ", ".join(repetidas)
                         + " já foi(ram) lançada(s) no Pipefy. Desmarque-a(s) para lançar "
                         "as demais.")
    linhas = [l for l in lote["linhas"] if l["valor"] > 0
              and " ".join(str(l.get("conta") or "").split()) in escolhidas]
    if not linhas:
        return None

    por_conta: dict = {}
    for l in linhas:
        conta = " ".join(str(l.get("conta") or "").split())
        c = por_conta.setdefault(conta, {"obras": {}, "categorias": {}, "tipos": {},
                                         "pessoas": set(), "cards": set(),
                                         "valor": Decimal("0.00")})
        obra = _chave(l["obra"]) or "(SEM OBRA)"
        c["obras"][obra] = c["obras"].get(obra, Decimal("0.00")) + l["valor"]
        cat = str(l.get("categoria") or "").strip()
        c["categorias"][cat] = c["categorias"].get(cat, Decimal("0.00")) + l["valor"]
        nome_tipo = l.get("tipo_despesa") or ""
        t = c["tipos"].setdefault(nome_tipo, {"valor": Decimal("0.00"),
                                              "record_id": l.get("record_id") or ""})
        t["valor"] += l["valor"]
        c["pessoas"].add(l["cpf"])
        if l.get("card_id"):
            c["cards"].add(str(l["card_id"]))
        c["valor"] += l["valor"]
        if not cat:
            bloqueios.append(f'tipo de despesa "{nome_tipo}" sem Código Omie na aba '
                             '"Plano Financeiro" (categoria do rateio).')

    sps, pessoas, nomes_dos_tipos = [], set(), set()
    for conta, c in sorted(por_conta.items()):
        esperado_por_conta[conta] = esperado_por_conta.get(conta, Decimal("0.00")) + c["valor"]
        pessoas |= c["pessoas"]
        if not conta:
            bloqueios.append(f"Despesas com colaboradores: R$ {formatos.moeda(c['valor'])} "
                             'sem conta de origem (obra sem conta na aba "C. Diários").')
        obras = []
        for obra, valor in sorted(c["obras"].items(), key=lambda x: -x[1]):
            codigo = omie.get(obra, "")
            if not codigo:
                bloqueios.append(f'a obra "{obra}" não tem Código Omie na aba "C. Diários".')
            obras.append({"obra": obra, "valor": valor, "omie": codigo})
        # O tipo de despesa da SP: o da MAIOR parte (o script usava o primeiro e
        # avisava); o detalhe por categoria fica no rateio.
        maior_nome, maior = max(c["tipos"].items(), key=lambda x: x[1]["valor"])
        nomes_dos_tipos |= set(c["tipos"])
        tipo_sp = maior["record_id"] or (achar_tipo(maior_nome) if ler_pipes else "")
        if ler_pipes and not tipo_sp:
            bloqueios.append(f'tipo de despesa "{maior_nome}" sem Record ID na aba "Plano '
                             'Financeiro" da planilha das SPs.')
        categorias = [{"codigo": k, "valor": v} for k, v in
                      sorted(c["categorias"].items(), key=lambda x: -x[1]) if k]
        arquivo = next((a for a in r["arquivos"] if a["conta"] == conta), {})
        destino = destino_da_conta.get(conta) or ""
        sp_item = {"conta": conta, "valor": c["valor"], "pessoas": len(c["pessoas"]),
                   "link": arquivo.get("link") or "", "destino": destino,
                   "link_relatorio": ((r.get("relatorios") or {}).get(conta)
                                      or {}).get("link") or "",
                   "obras": obras, "tipo_sp": tipo_sp or "",
                   "cards_de_origem": sorted(c["cards"]),
                   "tipos": [{"nome": k, "valor": v["valor"]} for k, v in
                             sorted(c["tipos"].items(), key=lambda x: -x[1]["valor"])],
                   "rateio": rateio_multiplo(obras, "", categorias) if obras else ""}
        if len(c["tipos"]) > 1:
            sp_item["observacao"] = (
                f"Mais de um tipo de despesa nesta conta: a SP vai como "
                f'"{maior_nome}" (o de maior valor); o detalhe por categoria vai no rateio.')
        sp_item["etiquetas"] = rotulo_das_etiquetas(sp_item)
        sp_item["descricao"] = descricao_da_dc(sp_item, analise.get("link") or "")
        sps.append(sp_item)

    return {
        "verba": "dc", "chave": chave, "rotulo_verba": geracao.rotulo_da_verba("dc"),
        # O título da SP: a data, como o script fazia.
        "grupo": _agora().strftime("%d/%m/%Y"),
        "tipo_sp": sps[0]["tipo_sp"] if sps else "",
        "tipo_despesa": ", ".join(sorted(nomes_dos_tipos)),
        "categoria": ", ".join(sorted({k for c in por_conta.values()
                                       for k in c["categorias"] if k})),
        "total": sum((x["valor"] for x in sps), Decimal("0.00")),
        "pessoas": len(pessoas), "sps": sps, "dc_lote": lote["id"],
        "cards_de_origem": sorted({x for sp in sps for x in sp["cards_de_origem"]}),
    }


def descricao_da_dc(sp: dict, link_analise: str) -> str:
    """A descrição da SP da DC — a do script (`_buildCardDescription_`), com o
    valor por obra e por tipo de despesa e o relatório."""
    from . import dc
    destino = geracao.ROTULO_DO_DESTINO.get(sp.get("destino") or "", "")
    linhas = _com_conta_origem(sp.get("conta"), [
        "Despesas com colaboradores",
        f"Arquivo de pagamento: {destino}" if destino else "",
        f"Valor: R$ {formatos.moeda(sp['valor'])}",
    ])
    if sp.get("destino") == "beevale":
        # Só informado, como no script: a SP vai pelo valor original.
        com_taxa = (Decimal(str(sp["valor"])) * (1 + dc.ACRESCIMO_BEEVALE / 100)
                    ).quantize(CENTAVO, rounding=ROUND_DOWN)
        linhas.append(f"Valor BeeVale (+{formatos.moeda(dc.ACRESCIMO_BEEVALE)}%): "
                      f"R$ {formatos.moeda(com_taxa)}")
    cards = sp.get("cards_de_origem") or []
    linhas += [f"Colaboradores: {sp.get('pessoas', 0)}",
               f"Qtd IDs de origem: {len(cards)}",
               f"IDs de origem: {', '.join(cards)}", "", "Valor por obra:"]
    linhas += [f"- {o['obra']}: R$ {formatos.moeda(o['valor'])}" for o in sp.get("obras") or []]
    if sp.get("tipos"):
        linhas += ["", "Valor por tipo de despesa:"]
        linhas += [f"- {t['nome']}: R$ {formatos.moeda(t['valor'])}" for t in sp["tipos"]]
    linhas.append("")
    if sp.get("link"):
        linhas.append(f"Planilha de pagamento: {sp['link']}")
    if sp.get("link_relatorio"):
        linhas.append(f"Relatório (PDF): {sp['link_relatorio']}")
    if link_analise:
        linhas.append(f"Planilha de análise: {link_analise}")
    return "\n".join([l for i, l in enumerate(linhas)
                      if l or (i and linhas[i - 1])] + ["", "Gerado pelo Análise de SPs."])


def descricao_da_sp(competencia: str, tipo: str, verba: str, sp: dict,
                    link_analise: str) -> str:
    """O texto do campo "Descrição da despesa" (`descri_o`). ⚠️ ELE É A PROVA:
    quem abrir o card meses depois precisa saber de que competência é, de qual
    conta saiu, quanto foi para cada obra e onde estão os arquivos — sem depender
    de ninguém lembrar (dono, 02/10/2026: *"colocar as informações da folha,
    colocar as obras, colocar valor por obra, colocar os links (…) a
    competência"*)."""
    rotulo_tipo = {"quinzena": "Quinzena (dias 1 a 15)",
                   "fim_de_mes": "Fim de mês (16 ao último dia)"}.get(
                       str(tipo or ""), str(tipo or ""))
    destino = geracao.ROTULO_DO_DESTINO.get(sp.get("destino") or "", "")
    linhas = _com_conta_origem(sp.get("conta"), [
        f"{geracao.rotulo_da_verba(verba)} — competência {competencia}",
        f"Pagamento: {rotulo_tipo}" if rotulo_tipo else "",
        f"Arquivo de pagamento: {destino}" if destino else "",
        f"Colaboradores: {sp.get('pessoas', 0)}",
        f"Total: R$ {formatos.moeda(sp['valor'])}",
        "",
        "Valor por obra:",
    ])
    linhas += [f"- {o['obra']}: R$ {formatos.moeda(o['valor'])}"
               for o in sp.get("obras") or []]
    linhas.append("")
    if sp.get("link"):
        linhas.append(f"Planilha de pagamento: {sp['link']}")
    # O relatório em PDF da conta (dono, 03/10/2026: *"colocar o link tanto do
    # arquivo de pagamento quanto o relatório também"*).
    if sp.get("link_relatorio"):
        linhas.append(f"Relatório (PDF): {sp['link_relatorio']}")
    if link_analise:
        linhas.append(f"Planilha de análise: {link_analise}")
    return "\n".join([l for i, l in enumerate(linhas)
                      if l or (i and linhas[i - 1])] + ["", "Gerado pelo Análise de SPs."])


# ---------------------------------------------------------------------------
# O ANDAMENTO — o que já foi criado, para continuar de onde parou
# ---------------------------------------------------------------------------
def _andamento(analise_id: int) -> dict:
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_DO_ANDAMENTO.format(int(analise_id)),))
        return json.loads(linha[0]) if linha and linha[0] else {}
    except Exception:  # noqa: BLE001
        logger.exception("Folha: não consegui ler o andamento dos cards")
        return {}


def _guardar_andamento(analise_id: int, andamento: dict) -> None:
    """Grava DEPOIS DE CADA card criado: se a próxima chamada cair, apertar de
    novo continua de onde parou, sem criar o mesmo card duas vezes."""
    from .db import conexao
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.meta (chave, valor) VALUES (?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor",
            (CHAVE_DO_ANDAMENTO.format(int(analise_id)),
             json.dumps(andamento, ensure_ascii=False)))
        conn.commit()


def _completo(andamento: dict, grupos: list) -> bool:
    return bool(grupos) and all(
        (andamento.get(g.get("chave") or g["verba"]) or {}).get("ligado")
        for g in grupos)


# ---------------------------------------------------------------------------
# OS CAMPOS DE CADA CARD — os do Make, valor por valor
# ---------------------------------------------------------------------------
ROTULO_DA_ETIQUETA = {"318116255": "Folha de Pgt", "317521565": "BeeVale",
                      "318116254": "Somapay"}


def rotulo_das_etiquetas(sp: dict) -> str:
    return " + ".join(ROTULO_DA_ETIQUETA.get(e, e) for e in etiquetas_da_sp(sp))


def etiquetas_da_sp(sp: dict) -> list:
    """`["Folha de Pgt", "BeeVale" | "Somapay"]` (os ids) — ver ETIQUETA_FOLHA.
    Destino desconhecido: só a da folha."""
    destino = ETIQUETA_DO_DESTINO.get(str(sp.get("destino") or "").strip().lower())
    return [ETIQUETA_FOLHA] + ([destino] if destino else [])


def campos_da_sp(grupo: dict, sp: dict, link_analise: str,
                 agora: _dt.datetime) -> list:
    """Os campos da Solicitação de Pagamento, no padrão do script do BeeVale
    (`_createPipefyCard_`), com Pix e chave aleatória "Atualizar Chave"."""
    favorecido = FAVORECIDO.get(sp.get("destino") or "", FAVORECIDO["somapay"])
    campos = [
        ("data", agora.strftime("%d/%m/%Y %H:%M")),
        ("data_de_pagamento", agora.strftime("%d/%m/%Y")),
        ("descri_o", sp["descricao"]), ("valor", _valor(sp["valor"])),
        ("radio_horizontal_t_tulo", "Pessoa Jurídica"),
        ("local", favorecido["local"]),
        ("tipo_de_pagamento", "Pix"),
        ("rateio_m_ltiplo_entre_centros_de_custo", "Sim"),
        ("rateio_m_ltiplo", sp["rateio"]),
        ("a_despesa_gerou_emiss_o_de_nota_fiscal", "Não"),
        ("tipo", "Aleatória"),
        ("selecione_o_procedimento", "Solicitar Pagamento"),
        ("cnpj", favorecido["cnpj"]),
        # O Make preenchia os dois campos de CNPJ do pipe (blueprint 2).
        ("cnpj_1", favorecido["cnpj"]),
        ("requisi_o_solicitada_por_um_terceiro", "Não"),
        ("alimenta_o_de_equipe", "Não"),
        ("colaborador_solicitante", RESPONSAVEL),
        ("parcelas", "1x Parcela"),
        ("tipo_de_despesa", sp.get("tipo_sp") or grupo["tipo_sp"]),
        ("chave_pix_aleat_ria", CHAVE_PIX_A_ATUALIZAR),
        ("documenta_o_fiscal", favorecido["documentacao"]),
        ("link_planilha_de_an_lise", link_analise),
        ("valida_o_sp_1", "Sim"), ("lan_amento_via_api", "Sim"),
        ("etiquetas", etiquetas_da_sp(sp)),
        ("autoriza_o_dupla", "SIM"), ("anu_ncia_sp", "Sim"),
    ]
    if grupo.get("verba") == "dc" and sp.get("destino") == "beevale":
        campos.append(("automa_o_2", AUTOMACAO_DA_DC))
    return [{"campo": c, "valor": v} for c, v in campos]


# ⚠️ VÃO NA CRIAÇÃO MESMO SENDO DE FASE. "Lançamento via API" = Sim é o que
# dispensa os Anexos obrigatórios: mandado depois, o Pipefy recusava a criação
# por falta de anexo (03/10/2026). O Make mandava os dois na criação (blueprint 2).
NA_CRIACAO_SEMPRE = ("lan_amento_via_api", "valida_o_sp_1")


def _separar(valores: list, inicio: dict) -> tuple:
    """(os que vão na criação, os que vão depois). Campo do formulário inicial vai
    na criação — é ali que o Pipefy cobra os obrigatórios; campo de fase vai
    depois, gravado no card já criado (salvo `NA_CRIACAO_SEMPRE`)."""
    def na(v):
        return v["campo"] in inicio or v["campo"] in NA_CRIACAO_SEMPRE
    na_criacao = [v for v in valores if na(v) and v["valor"] != ""]
    depois = [v for v in valores if not na(v) and v["valor"] != ""]
    return na_criacao, depois


# ---------------------------------------------------------------------------
# LANÇAR — sem volta
# ---------------------------------------------------------------------------
def lancar(analise_id: int, quem: str = "", contas=None) -> dict:
    """Cria uma SP por conta, com o rateio múltiplo por obra. ⚠️ SEM VOLTA.

    Card criado no Pipefy não se apaga por aqui. Por isso só roda com a prévia
    limpa, e grava o andamento a cada card — se cair no meio, apertar de novo
    continua de onde parou."""
    from . import folha_pagamento as fpg

    vista, sp_pipe = _previa(analise_id, contas=contas)
    if vista["bloqueios"]:
        raise ErroDosCards("nenhum card foi criado: " + " ".join(vista["bloqueios"]))
    if vista["ja_lancado"] and _cards_da_dc_a_mover(vista):
        # A SP já existe e só falta mover os cards de origem (o Pipefy recusou
        # da outra vez): apertar de novo termina isso, sem criar nada.
        movidos = _mover_cards_da_dc(analise_id, vista["grupos"], vista["andamento"])
        if movidos["falhas"]:
            raise ErroDosCards("as SPs já existem, mas o Pipefy recusou mover "
                               + "; ".join(movidos["falhas"]) + ".")
        return {"ok": True, "competencia": vista["competencia"], "despesas": [],
                "sps": [], "criados": [], "cards_movidos": movidos["movidos"]}
    if vista["ja_lancado"]:
        raise ErroDosCards(
            "este pagamento já foi lançado no Pipefy. Para relançar, cancele os cards "
            "no Pipefy e gere os arquivos novamente.")

    r = rodada(analise_id)
    andamento = vista["andamento"]
    agora = _agora()
    criados = []
    # Só os campos que o pipe tem: um campo opcional que não exista (o "local",
    # por exemplo) não pode derrubar o card inteiro.
    existentes = sp_pipe["todos"]
    try:
        for grupo in vista["grupos"]:
            estado = andamento.setdefault(grupo["chave"], {"sps": {}})
            for sp in grupo["sps"]:
                feito = (estado["sps"].get(sp["conta"]) or {})
                if not feito.get("id"):
                    valores = [v for v in campos_da_sp(grupo, sp, vista["link_analise"], agora)
                               if v["campo"] in existentes]
                    na_criacao, depois = _separar(valores, sp_pipe["inicio"])
                    card = pipefy.criar_card(PIPE_SP, grupo["grupo"], na_criacao)
                    feito = {"id": card["id"], "link": card["link"], "depois": depois}
                    estado["sps"][sp["conta"]] = feito
                    _guardar_andamento(analise_id, andamento)
                    criados.append(f"SP {card['id']}")
                if not feito.get("completa"):
                    if feito.get("depois"):
                        pipefy.atualizar_campos(feito["id"], feito["depois"])
                    feito["completa"] = True
                    feito.pop("depois", None)
                    _guardar_andamento(analise_id, andamento)
            estado["ligado"] = True
            _guardar_andamento(analise_id, andamento)
    except pipefy.ErroDoPipefy as e:
        logger.exception("Folha: lançamento no Pipefy parou no meio")
        ja = (" Já criados: " + ", ".join(criados) + "." if criados else "")
        raise ErroDosCards(
            f"o Pipefy recusou a operação durante o lançamento: {e}.{ja} Uma nova "
            "tentativa retoma do ponto de parada, sem duplicar o que já foi criado.") from e

    # Amarra no log: cada arquivo de conta à(s) SP(s) dele; a análise a todas.
    for arquivo in r["arquivos"]:
        sps = [contas_lancadas(andamento, v).get(arquivo["conta"])
               for v in (arquivo["verbas"] or "").split("+")]
        sps = [s for s in sps if s]
        if sps:
            fpg.registrar_card(arquivo["id"], ",".join(s["id"] for s in sps),
                               sps[0]["link"])
    todas = [sp for v in r["verbas"] for sp in contas_lancadas(andamento, v).values()]
    if todas:
        fpg.registrar_card(analise_id, ",".join(s["id"] for s in todas), todas[0]["link"])
    logger.info("Folha: %s lançado no Pipefy por %s — %s.",
                vista["competencia"], quem or "(sem nome)",
                ", ".join(criados) or "nada novo")
    # A DC: com a SP criada, os cards de origem vão para a fase de processados.
    # Uma recusa aqui NÃO desfaz a SP — vai como aviso, e lançar de novo termina.
    movidos = _mover_cards_da_dc(analise_id, vista["grupos"], andamento)
    return {"ok": True, "competencia": vista["competencia"], "despesas": [],
            "cards_movidos": movidos["movidos"],
            "avisos": ([f"SP criada, mas o Pipefy recusou mover {'; '.join(movidos['falhas'])}. "
                        "Clique em lançar de novo para terminar."] if movidos["falhas"] else []),
            "sps": [{"conta": sp["conta"],
                     "id": (andamento[g["chave"]]["sps"][sp["conta"]])["id"],
                     "link": (andamento[g["chave"]]["sps"][sp["conta"]])["link"]}
                    for g in vista["grupos"] for sp in g["sps"]],
            "criados": criados}


def _cards_da_dc_a_mover(vista: dict) -> list:
    feitos = set((vista.get("andamento") or {}).get("_dc_movidos") or [])
    return [c for g in vista.get("grupos") or [] if g.get("verba") == "dc"
            for c in g.get("cards_de_origem") or [] if c not in feitos]


def _mover_cards_da_dc(analise_id: int, grupos: list, andamento: dict) -> dict:
    """Move os cards de origem das SPs da DC já criadas. Guarda cada card movido
    no andamento: se cair no meio, a próxima vez continua de onde parou."""
    from . import dc
    feitos = set(andamento.get("_dc_movidos") or [])
    saida = {"movidos": [], "falhas": []}
    for g in grupos:
        if g.get("verba") != "dc":
            continue
        estado = (andamento.get(g["chave"]) or {}).get("sps") or {}
        for sp in g["sps"]:
            if not (estado.get(sp["conta"]) or {}).get("completa"):
                continue
            for card in sp.get("cards_de_origem") or []:
                if card in feitos:
                    continue
                try:
                    dc.mover_card_de_origem(card)
                except pipefy.ErroDoPipefy as e:
                    saida["falhas"].append(f"o card {card} ({e})")
                    continue
                feitos.add(card)
                saida["movidos"].append(card)
                andamento["_dc_movidos"] = sorted(feitos)
                _guardar_andamento(analise_id, andamento)
        lote = dc.lote_da_analise(analise_id)
        if lote and not lote["cards_movidos"] and all(
                str(l["card_id"]) in feitos for l in lote["linhas"] if l.get("card_id")):
            dc.marcar_cards_movidos(lote["id"])
    if saida["movidos"]:
        logger.info("DC: %d card(s) de origem movidos para a fase de processados.",
                    len(saida["movidos"]))
    return saida
