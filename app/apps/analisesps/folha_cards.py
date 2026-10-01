# -*- coding: utf-8 -*-
"""
OS CARDS DO PIPEFY PARA A FOLHA — cópia fiel do cenário do Make.

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
from decimal import Decimal

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
ETIQUETA_DA_SP = "307726886"
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
}

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
        f"Conta de pagamento: {conta}" if conta else "",
        f"Pessoas: {pessoas}",
        # O valor com vírgula e ponto de milhar: o card é lido por gente, e
        # "4200.00" num card de despesa se confunde com quatro reais.
        f"Total: R$ {formatos.moeda(total)}",
    ]
    if link_pagamento:
        linhas.append(f"Planilha de pagamento: {link_pagamento}")
    if link_analise:
        linhas.append(f"Planilha de análise: {link_analise}")
    corpo = [l for l in linhas if l]
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
    arquivos = sorted((a for a in mesmos if a["destino"] != fpg.ANALISE
                       and piso < a["id"] < analise["id"]), key=lambda a: a["id"])
    if not arquivos:
        raise ErroDosCards("arquivos de pagamento desta rodada não encontrados.")
    return {"analise": analise, "arquivos": arquivos,
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


def _centros_do_pipefy(campos: dict) -> tuple:
    """Como o campo "Centro de Custo" do pipe de Despesa recebe a obra.

    Devolve (função obra → valor ou None, descrição do campo). Se o campo é uma
    conexão com uma tabela, a obra vira o id do registro de mesmo nome; se é
    lista de opções, a opção de mesmo nome; se é texto, o próprio código."""
    campo = campos.get("centro_de_custo_1") or {}
    tipo = campo.get("tipo") or ""
    ligado = campo.get("ligado_a") or {}
    if tipo == "connector":
        if ligado.get("tipo") != "tabela" or not ligado.get("id"):
            return (lambda obra: None), "conexão de tipo não suportado"
        registros = pipefy.registros_da_tabela(ligado["id"])
        por_nome: dict = {}
        for r in registros:
            por_nome.setdefault(_chave(r["nome"]), []).append(r["id"])

        def achar(obra):
            achados = por_nome.get(_chave(obra)) or []
            return achados[0] if len(achados) == 1 else None
        return achar, f'tabela "{ligado.get("nome") or ligado["id"]}"'
    if tipo in ("select", "radio_vertical", "radio_horizontal"):
        opcoes = {_chave(o): o for o in campo.get("opcoes") or []}
        return (lambda obra: opcoes.get(_chave(obra))), "lista de opções"
    return (lambda obra: _chave(obra) or None), "texto"


# Os campos que o Make usa em cada pipe. Se algum sumir ou mudar de id no Pipefy,
# a prévia diz qual — em vez de o card sair com o valor caindo em lugar nenhum.
CAMPOS_DA_DESPESA = (
    "data", "data_de_pagamento", "descri_o", "valor", "tipo_de_despesa", "op_o",
    "respons_vel_pela_solicita_ox", "banco_do_pagamento", "valor_total_pago",
    "valida_o_dc", "link_para_planilha_de_pagamento",
    "link_para_planilha_de_an_lise", "conex_o_sp")
CAMPOS_DA_SP = (
    "data", "data_de_pagamento", "descri_o", "valor", "colaborador_solicitante",
    "tipo_de_pagamento", "link_planilha_de_an_lise", "tipo_de_despesa",
    "selecione_o_procedimento", "alimenta_o_de_equipe", "parcelas",
    "chave_pix_aleat_ria", "radio_horizontal_t_tulo", "tipo", "cnpj",
    "valida_o_sp_1", "conex_o_dc_id", "etiquetas", "conex_o_dc")


def _ler_pipe(pipe_id: str) -> dict:
    try:
        pipe = pipefy.campos_do_pipe(pipe_id)
    except pipefy.ErroDoPipefy as e:
        raise ErroDosCards(str(e)) from e
    inicio = pipe.get("campos") or {}
    fases = pipe.get("campos_das_fases") or {}
    return {"nome": pipe.get("nome") or pipe_id, "inicio": inicio,
            "todos": {**fases, **inicio}}


def conferir_pipe() -> dict:
    """Confere que os dois pipes têm todos os campos que o Make usa. NÃO CRIA NADA."""
    saida = []
    for pipe_id, usados in ((PIPE_DESPESA, CAMPOS_DA_DESPESA), (PIPE_SP, CAMPOS_DA_SP)):
        pipe = _ler_pipe(pipe_id)
        exigidos = list(usados)
        if pipe_id == PIPE_DESPESA:
            exigidos += list(campos_do_par(1))
        faltam = [c for c in exigidos if c not in pipe["todos"]]
        saida.append({"pipe": pipe_id, "nome": pipe["nome"],
                      "quantos_campos": len(pipe["todos"]), "faltam": faltam,
                      "obrigatorios": [d.get("label") or cid
                                       for cid, d in pipe["inicio"].items()
                                       if d.get("obrigatorio")]})
    return {"pipes": saida}


# ---------------------------------------------------------------------------
# A PRÉVIA — exatamente o que vai ser criado. NÃO CRIA NADA.
# ---------------------------------------------------------------------------
def previa(analise_id: int) -> dict:
    """Os cards que vão ser criados, com cada valor, e o que impede de criar.

    `bloqueios` vazio é a única situação em que `lancar` cria alguma coisa."""
    return _previa(analise_id)[0]


def _previa(analise_id: int, ler_pipes: bool = True) -> tuple:
    """(prévia, pipe de Despesa, pipe de SP) — os pipes lidos uma vez só."""
    from . import folha_pagamento as fpg

    r = rodada(analise_id)
    analise, arquivos = r["analise"], r["arquivos"]
    ano, mes, tipo = analise["ano"], analise["mes"], analise["tipo"]
    bloqueios: list = []

    despesa = _ler_pipe(PIPE_DESPESA) if ler_pipes else None
    sp = _ler_pipe(PIPE_SP) if ler_pipes else None
    centro_de, como_centro = (lambda obra: _chave(obra)), "texto"
    if ler_pipes:
        for pipe, usados in ((despesa, CAMPOS_DA_DESPESA), (sp, CAMPOS_DA_SP)):
            faltam = [c for c in usados if c not in pipe["todos"]]
            if faltam:
                bloqueios.append(
                    f'o pipe "{pipe["nome"]}" não tem mais o(s) campo(s) '
                    + ", ".join(faltam) + " utilizados pelo Make. O pipe foi alterado — "
                    "nenhum card foi criado.")
        try:
            centro_de, como_centro = _centros_do_pipefy(despesa["inicio"])
        except pipefy.ErroDoPipefy as e:
            bloqueios.append(f"não foi possível ler os centros de custo do Pipefy: {e}")

    omie = _codigos_omie()
    grupos, esperado_por_conta = [], {}
    for verba in r["verbas"]:
        try:
            nome_grupo, tipo_dc, tipo_sp = grupo_da_verba(verba, tipo)
        except ErroDosCards as e:
            bloqueios.append(str(e))
            continue
        try:
            linhas = [l for l in fpg.linhas_para_pagar(ano, mes, tipo, [verba])
                      if l["valor"] > 0]
        except fpg.ErroDoPagamento as e:
            bloqueios.append(str(e))
            continue

        por_obra: dict = {}
        por_conta: dict = {}
        pessoas = set()
        for l in linhas:
            obra = _chave(l["obra"]) or "(SEM OBRA)"
            por_obra[obra] = por_obra.get(obra, Decimal("0.00")) + l["valor"]
            conta = " ".join(str(l.get("conta") or "").split())
            por_conta[conta] = por_conta.get(conta, Decimal("0.00")) + l["valor"]
            pessoas.add(l["cpf"])
        for conta, valor in por_conta.items():
            esperado_por_conta[conta] = esperado_por_conta.get(
                conta, Decimal("0.00")) + valor
        total = sum(por_obra.values(), Decimal("0.00"))

        centros = []
        for obra, valor in sorted(por_obra.items()):
            centro = centro_de(obra)
            codigo = omie.get(obra, "")
            if not codigo:
                bloqueios.append(
                    f'a obra "{obra}" não tem Código Omie na aba "C. Diários".')
            if not centro:
                bloqueios.append(
                    f'a obra "{obra}" não tem centro de custo no Pipefy '
                    f"({como_centro}) com esse nome.")
            centros.append({"obra": obra, "valor": valor, "centro": centro or "",
                            "omie": codigo})
        if len(centros) > MAXIMO_DE_CENTROS:
            bloqueios.append(
                f"{geracao.rotulo_da_verba(verba)}: {len(centros)} obras, e o card "
                f"de Despesa comporta apenas {MAXIMO_DE_CENTROS} pares de centro de custo.")

        links = [a["link"] for a in arquivos
                 if verba in (a["verbas"] or "").split("+") and a["link"]]
        sps = []
        for conta, valor in sorted(por_conta.items()):
            if not conta:
                bloqueios.append(
                    f"{geracao.rotulo_da_verba(verba)}: R$ {formatos.moeda(valor)} "
                    'sem conta de origem (obra sem conta na aba "C. Diários").')
            link = next((a["link"] for a in arquivos if a["conta"] == conta
                         and verba in (a["verbas"] or "").split("+")), "")
            sps.append({"conta": conta, "valor": valor, "link": link})
        if len(sps) > MAXIMO_DE_SPS:
            bloqueios.append(
                f"{geracao.rotulo_da_verba(verba)}: {len(sps)} contas de origem, "
                f"e o card de Despesa comporta apenas {MAXIMO_DE_SPS} SPs.")

        grupos.append({
            "verba": verba, "rotulo_verba": geracao.rotulo_da_verba(verba),
            "grupo": nome_grupo, "tipo_dc": tipo_dc, "tipo_sp": tipo_sp,
            "total": total, "pessoas": len(pessoas), "centros": centros,
            "sps": sps, "links_pagamento": links,
            "descricao": descricao_do_card(
                analise["competencia"], tipo, [verba], "", len(pessoas), total,
                " ; ".join(links), analise["link"])})

    # ⚠️ O QUE ESTÁ FECHADO TEM DE SER O QUE FOI GERADO. Se alguém refez o
    # fechamento depois de gerar, o card contaria uma história e o arquivo outra.
    gerado_por_conta: dict = {}
    for a in arquivos:
        gerado_por_conta[a["conta"]] = gerado_por_conta.get(
            a["conta"], Decimal("0.00")) + Decimal(str(a["total"]))
    if grupos and gerado_por_conta != esperado_por_conta:
        bloqueios.append(
            "o fechamento foi alterado após a geração destes arquivos (divergência "
            "nos valores por conta). Gere os arquivos novamente antes de lançar.")

    andamento = _andamento(analise["id"])
    return {"analise": analise["id"], "competencia": analise["competencia"],
            "tipo": tipo, "link_analise": analise["link"],
            "como_centro": como_centro, "grupos": grupos,
            "bloqueios": list(dict.fromkeys(bloqueios)),
            "andamento": andamento, "ja_lancado": _completo(andamento, grupos),
            "arquivos": [a["id"] for a in arquivos]}, despesa, sp


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
        (andamento.get(g["verba"]) or {}).get("ligado") for g in grupos)


# ---------------------------------------------------------------------------
# OS CAMPOS DE CADA CARD — os do Make, valor por valor
# ---------------------------------------------------------------------------
def campos_da_despesa(grupo: dict, agora: _dt.datetime) -> list:
    """Os campos do card de Despesa, na forma do módulo 2 do cenário."""
    quando = agora.strftime("%d/%m/%Y %H:%M")
    campos = [
        ("data", quando), ("data_de_pagamento", quando),
        ("descri_o", grupo["descricao"]), ("valor", _valor(grupo["total"])),
        ("tipo_de_despesa", grupo["tipo_dc"]),
        ("op_o", "Pgt Conjunto"), ("respons_vel_pela_solicita_ox", RESPONSAVEL),
    ]
    for n, centro in enumerate(grupo["centros"], start=1):
        id_centro, id_valor, id_omie = campos_do_par(n)
        campos += [(id_centro, centro["centro"]), (id_valor, _valor(centro["valor"])),
                   (id_omie, centro["omie"])]
    campos += [("banco_do_pagamento", BANCO_DO_PAGAMENTO),
               ("valor_total_pago", _valor(grupo["total"])), ("valida_o_dc", "Sim")]
    return [{"campo": c, "valor": v} for c, v in campos]


def campos_depois_da_despesa(grupo: dict, link_analise: str) -> list:
    """Os links das planilhas — que no Make entram depois de o card existir."""
    return [{"campo": "link_para_planilha_de_pagamento",
             "valor": " ; ".join(grupo["links_pagamento"])},
            {"campo": "link_para_planilha_de_an_lise", "valor": link_analise}]


def campos_da_sp(grupo: dict, sp: dict, id_despesa: str, link_analise: str,
                 agora: _dt.datetime) -> list:
    """Os campos da SP de Transferência de Recursos, na forma do módulo 165."""
    descricao = f"Conta Origem: {sp['conta']}\n{grupo['descricao']}"
    if sp.get("link"):
        descricao += f"\nPlanilha de pagamento desta conta: {sp['link']}"
    campos = [
        ("data", agora.strftime("%d/%m/%Y")),
        ("data_de_pagamento", (agora + _dt.timedelta(days=1)).strftime("%d/%m/%Y")),
        ("descri_o", descricao), ("valor", _valor(sp["valor"])),
        ("colaborador_solicitante", RESPONSAVEL), ("tipo_de_pagamento", "Pix"),
        ("link_planilha_de_an_lise", link_analise),
        ("tipo_de_despesa", grupo["tipo_sp"]),
        ("selecione_o_procedimento", "Transferência de Recursos"),
        ("alimenta_o_de_equipe", "Não"), ("parcelas", "1x Parcela"),
        ("chave_pix_aleat_ria", PIX_DA_BWS),
        ("radio_horizontal_t_tulo", "Pessoa Jurídica"), ("tipo", "Aleatória"),
        ("cnpj", CNPJ_DA_BWS),
        ("valida_o_sp_1", "Sim"), ("conex_o_dc_id", str(id_despesa)),
        ("etiquetas", ETIQUETA_DA_SP),
    ]
    return [{"campo": c, "valor": v} for c, v in campos]


def _separar(valores: list, inicio: dict) -> tuple:
    """(os que vão na criação, os que vão depois). Campo do formulário inicial vai
    na criação — é ali que o Pipefy cobra os obrigatórios; campo de fase vai
    depois, gravado no card já criado."""
    na_criacao = [v for v in valores if v["campo"] in inicio and v["valor"] != ""]
    depois = [v for v in valores if v["campo"] not in inicio and v["valor"] != ""]
    return na_criacao, depois


# ---------------------------------------------------------------------------
# LANÇAR — sem volta
# ---------------------------------------------------------------------------
def lancar(analise_id: int, quem: str = "") -> dict:
    """Cria o card de Despesa e as SPs de cada conta, como o Make.

    ⚠️ CHAMADA SEM VOLTA: card criado no Pipefy não se apaga por aqui. Por isso só
    roda com a prévia limpa, e grava o andamento a cada card — se cair no meio,
    apertar de novo continua de onde parou."""
    from . import folha_pagamento as fpg

    vista, despesa, sp_pipe = _previa(analise_id)
    if vista["bloqueios"]:
        raise ErroDosCards("nenhum card foi criado: " + " ".join(vista["bloqueios"]))
    if vista["ja_lancado"]:
        raise ErroDosCards(
            "pagamento já lançado no Pipefy. Para relançar, cancele os cards "
            "no Pipefy e gere os arquivos novamente.")

    r = rodada(analise_id)
    andamento = vista["andamento"]
    agora = _agora()
    criados = []
    try:
        for grupo in vista["grupos"]:
            estado = andamento.setdefault(grupo["verba"], {"sps": {}})
            if not estado.get("despesa"):
                na_criacao, depois = _separar(campos_da_despesa(grupo, agora),
                                              despesa["inicio"])
                card = pipefy.criar_card(PIPE_DESPESA, agora.strftime("%d/%m/%Y"),
                                         na_criacao)
                estado.update({"despesa": card["id"], "link": card["link"],
                               "depois": depois})
                _guardar_andamento(analise_id, andamento)
                criados.append(f"Despesa {card['id']}")
            if not estado.get("despesa_completa"):
                pipefy.atualizar_campos(
                    estado["despesa"], (estado.get("depois") or [])
                    + campos_depois_da_despesa(grupo, vista["link_analise"]))
                estado["despesa_completa"] = True
                estado.pop("depois", None)
                _guardar_andamento(analise_id, andamento)

            for sp in grupo["sps"]:
                feito = (estado["sps"].get(sp["conta"]) or {})
                if not feito.get("id"):
                    na_criacao, depois = _separar(
                        campos_da_sp(grupo, sp, estado["despesa"],
                                     vista["link_analise"], agora),
                        sp_pipe["inicio"])
                    card = pipefy.criar_card(PIPE_SP, grupo["grupo"], na_criacao)
                    feito = {"id": card["id"], "link": card["link"],
                             "depois": depois}
                    estado["sps"][sp["conta"]] = feito
                    _guardar_andamento(analise_id, andamento)
                    criados.append(f"SP {card['id']}")
                if not feito.get("ligada"):
                    pipefy.atualizar_campos(
                        feito["id"], (feito.get("depois") or [])
                        + [{"campo": "conex_o_dc", "valor": [estado["despesa"]]}])
                    feito["ligada"] = True
                    feito.pop("depois", None)
                    _guardar_andamento(analise_id, andamento)

            if not estado.get("ligado"):
                pipefy.atualizar_campos(estado["despesa"], [{
                    "campo": "conex_o_sp",
                    "valor": [s["id"] for s in estado["sps"].values()]}])
                estado["ligado"] = True
                _guardar_andamento(analise_id, andamento)
    except pipefy.ErroDoPipefy as e:
        logger.exception("Folha: lançamento no Pipefy parou no meio")
        ja = (" Já criados: " + ", ".join(criados) + "." if criados else "")
        raise ErroDosCards(
            f"o Pipefy recusou a operação durante o lançamento: {e}.{ja} Uma nova "
            "tentativa retoma do ponto de parada, sem duplicar o que já foi criado.") from e

    # Amarra no log: cada arquivo de conta à(s) SP(s) dela; a análise à(s)
    # Despesa(s).
    for arquivo in r["arquivos"]:
        sps = [((andamento.get(v) or {}).get("sps") or {}).get(arquivo["conta"])
               for v in (arquivo["verbas"] or "").split("+")]
        sps = [s for s in sps if s]
        if sps:
            fpg.registrar_card(arquivo["id"], ",".join(s["id"] for s in sps),
                               sps[0]["link"])
    despesas = [andamento[g["verba"]] for g in vista["grupos"]]
    fpg.registrar_card(analise_id, ",".join(d["despesa"] for d in despesas),
                       despesas[0]["link"])
    logger.info("Folha: %s lançado no Pipefy por %s — %s.",
                vista["competencia"], quem or "(sem nome)",
                ", ".join(criados) or "nada novo")
    return {"ok": True, "competencia": vista["competencia"],
            "despesas": [{"id": d["despesa"], "link": d["link"]} for d in despesas],
            "sps": [{"conta": conta, "id": s["id"], "link": s["link"]}
                    for d in despesas for conta, s in d["sps"].items()],
            "criados": criados}
