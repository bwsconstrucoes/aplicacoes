# -*- coding: utf-8 -*-
"""
A BASE CONSOLIDADA das notas emitidas — uma linha por nota, com tudo.

Por que ela existe (pedido do dono em 09/10/2026, com estas palavras): *"essa
planilha é um lixo, é uma bagunça (…) eu quero fazer o controle de notas numa
tela de Faturamento, e consolidar todos os dados que estão nas planilhas Notas
BWS, Notas BWS Links e Controle Nacional numa coisa só"*.

Hoje a informação de UMA nota está espalhada por **cinco** lugares:

| Onde | O que tem lá |
|---|---|
| `Notas BWS` | número, data, valores, recebimento — e 60 colunas de cruzamento |
| `Notas BWS Links` | os links do XML, da NFS-e e do recibo |
| `Controle Nacional` | o fechamento nacional (chave, DANFSe) |
| `C. Diários` (outra planilha) | obra, contrato, tributação, alíquota, **empresa** |
| `Protocolos` (outra planilha) | o card do Pipefy e o código de integração do Omie |

E três coisas importantes **não estão em lugar nenhum**: o **período da medição**,
o **corpo da nota** (a discriminação) e a **empresa/SCP** que faturou — porque nem
toda obra é faturada no CNPJ da BWS.

Esta aba junta tudo. Ela é a fonte da tela de Faturamento (área Análise de SPs) e,
no futuro, o que substitui a gestão feita na "Notas BWS".

**Enquanto a transição não acabar, o emissor grava nos DOIS lugares.** É trabalho
duplicado de propósito: decisão do dono, para a base nova ser conferida com a
antiga ao lado antes de a antiga sair de cena.

**A chave de uma linha é o número da nota.** No modelo nacional ele tem 13
dígitos (ano + sequencial); o sequencial também é gravado, em coluna própria,
porque é por ele que se procura e é ele que a numeração usa.
"""
from __future__ import annotations

import datetime

ABA = "Base Faturamento"

FUSO_BRASILIA = datetime.timezone(datetime.timedelta(hours=-3))

# A ordem aqui É a ordem das colunas da aba. Acrescentar campo novo vai no FIM:
# a aba já gravada não se reorganiza, e inserir no meio desalinharia tudo.
CAB = [
    # --- identificação da nota -------------------------------------------- #
    "nota_numero",            # o número OFICIAL (13 dígitos no nacional)
    "nota_sequencial",        # o nosso sequencial (3283) — busca e numeração
    "modelo",                 # nacional | abrasf
    "chave_acesso",           # 50 dígitos (só nacional)
    "cod_verificacao",        # só modelo antigo
    "data_emissao",           # AAAA-MM-DD
    "competencia",            # AAAA-MM
    "status",                 # valida | cancelada | substituida
    "observacao",
    # --- a obra: SÓ a chave de cruzamento --------------------------------- #
    # Correção de 09/10/2026, pedido do dono: *"informação que vem da C. Diários
    # não precisa entrar na base, a gente vai cruzar"*. Então contrato, município,
    # centro de custo, tributação, **empresa** e **SCP** saíram daqui — são
    # atributos da OBRA, e a tela os busca na C. Diários pelo código.
    #
    # O que fica é o que é FATO DA NOTA, congelado no dia da emissão, e isso é
    # diferente: a alíquota que a nota aplicou é fiscal; a tributação cadastrada
    # na obra pode mudar amanhã.
    "obra_codigo",            # a chave do cruzamento
    "aliquota_iss",           # a alíquota que a NOTA aplicou (não a do cadastro)
    # --- medição ----------------------------------------------------------- #
    "medicao_numero",
    "medicao_periodo_ini",    # NUNCA foi gravado antes
    "medicao_periodo_fim",    # NUNCA foi gravado antes
    "tipo_documento",
    "tipo_medicao",
    # --- tomador ----------------------------------------------------------- #
    "tomador_cnpj",
    "tomador_nome",
    "tomador_municipio",
    # --- valores da nota --------------------------------------------------- #
    "valor_total",
    "valor_servicos",
    "valor_materiais",
    "base_iss",
    "valor_liquido_previsto",  # valor − TODAS as retenções
    # --- tributos: o que a NOTA declarou ----------------------------------- #
    # Os TRÊS estados destes campos são diferentes, e a diferença importa:
    #   vazio       = não se sabe (nota antiga que ele ainda não equalizou)
    #   0,00 com N  = a nota NÃO reteve esse tributo
    #   valor com S = a nota reteve
    # Vazio é o que impede a tela do Omie de equalizar um título às cegas.
    "pis", "cofins", "ir", "csll", "inss", "iss",
    # O "retido ou não" existe por um motivo só, e é o que o dono pediu:
    # **compatibilizar com o Omie**. O Omie tem os dois campos (valor e retém), e
    # só o que foi retido entra na soma que vai para o título.
    "retem_pis", "retem_cofins", "retem_ir", "retem_csll", "retem_inss", "retem_iss",
    # IBS e CBS: os tributos da reforma. A plataforma nacional já os calcula e
    # devolve no XML da nota (com a redução de 50% da construção civil), e o dono
    # pediu as duas colunas porque "pode ser que isso aí seja necessário".
    "ibs", "cbs",
    # --- tributos: o que está no título do OMIE ---------------------------- #
    "omie_pis", "omie_cofins", "omie_ir", "omie_csll", "omie_inss", "omie_iss",
    "omie_codigo_integracao",
    "omie_codigo_lancamento",
    "omie_numero_documento",
    "omie_valor_titulo",
    "omie_conferido_em",
    "divergencia_tributos",    # S/N: a nota e o Omie batem?
    # --- recebimento -------------------------------------------------------- #
    "data_recebimento",
    "valor_recebido",
    "banco_conta",
    "divergencia_recebimento",  # recebido − líquido previsto
    # --- ligações e documentos ---------------------------------------------- #
    "card_id",                 # Pipefy
    "link_card",
    "link_xml",
    "link_nfse_municipal",
    "link_nfse_nacional",
    "link_recibo",
    "id_dps",
    # --- o corpo da nota ---------------------------------------------------- #
    "discriminacao",           # NUNCA foi gravado antes
    # --- auditoria ---------------------------------------------------------- #
    "origem",                  # emissor | consolidacao | manual
    "atualizado_em",
]

IDX = {nome: i for i, nome in enumerate(CAB)}

STATUS_VALIDA = "valida"
STATUS_CANCELADA = "cancelada"
STATUS_SUBSTITUIDA = "substituida"


def _agora() -> str:
    return datetime.datetime.now(FUSO_BRASILIA).strftime("%d/%m/%Y %H:%M")


def _txt(v) -> str:
    return "" if v is None else str(v).strip()


def status_da_observacao(observacao) -> str:
    """Lê o status da nota a partir da coluna Observação da "Notas BWS".

    Não é invenção: é como a planilha e os scripts do Apps Script já funcionam —
    o rateio do Omie ignora as linhas com "CANCELADA" na coluna L, e a
    substituição escreve ali o aviso. A base nova passa a ter o status num campo
    próprio, em vez de escondido num texto livre.
    """
    t = _txt(observacao).upper()
    if "CANCEL" in t:
        return STATUS_CANCELADA
    if "SUBSTITU" in t:
        return STATUS_SUBSTITUIDA
    return STATUS_VALIDA


def linha_vazia() -> list:
    return [""] * len(CAB)


def montar_linha(dados: dict) -> list:
    """Monta a linha da aba a partir de um dicionário com os nomes do cabeçalho.

    Campo que não vier fica vazio **de propósito**: a base nasce incompleta para
    as notas antigas (período da medição e discriminação não existem para elas) e
    completa para as novas. Era o pedido: *"não vamos ter a completude dos dados,
    mas para frente a gente passa a ter"*.
    """
    linha = linha_vazia()
    desconhecidos = [k for k in dados if k not in IDX]
    if desconhecidos:
        raise KeyError(f"Campo que não existe na Base Faturamento: {desconhecidos}")
    for nome, valor in dados.items():
        linha[IDX[nome]] = _txt(valor)
    if not linha[IDX["atualizado_em"]]:
        linha[IDX["atualizado_em"]] = _agora()
    return linha


# --------------------------------------------------------------------------- #
# De onde cada coisa vem — o mapa das planilhas de hoje
#
# Os índices são 0-based e vieram do cabeçalho REAL da "Notas BWS" (linha 2),
# lido em 09/10/2026. A aba tem 65+ colunas, e a maioria é cruzamento de dados
# com outras planilhas — o dono foi explícito: o que interessa é E:O e BB:BM.
# --------------------------------------------------------------------------- #
# E:O — o que o emissor grava e o que a gestão usa
NB_ANO = 4
NB_NUMERO = 5
NB_DATA_EMISSAO = 6
NB_VALOR = 7
NB_OBRA = 8
NB_MEDICAO = 9
NB_RF = 10
NB_OBSERVACAO = 11
NB_DATA_RECEBIMENTO = 12
NB_VALOR_RECEBIDO = 13
NB_LIQUIDO_DESTAQUE = 14
# ⚠️ **DE P A BA NÃO SE LÊ NADA, e isto é correção de 09/10/2026.** A primeira
# versão lia o bloco T:Y como se fossem os tributos da nota. O dono corrigiu:
#
#   *"Não existe aquilo dali, aquilo são repetições, é outra metodologia que eu
#    utilizava, dali é lixo. (…) Eu comentei que eles são da coluna BB em diante
#    só."*
#
# A planilha tem o mesmo conjunto PIS/COFINS/IR/CSLL/INSS/ISS **três vezes**
# (T:AB, AC:AK, AL:AT), mais CPRB e "REGIME ESPECIAL" — tudo de uma metodologia
# abandonada. Ler dali encheria a base de números plausíveis e errados, que é o
# pior resultado possível: ninguém desconfia de um número com cara de certo.
#
# BB:BM — **os tributos EQUALIZADOS**, em pares valor/retém. O dono esclareceu
# em 09/10/2026 o que esse bloco é:
#
#   *"A parte de tributos Omie, aquilo dali eu criei exatamente para equalizar.
#    Já está tudo equalizado ali. E o que não tiver, talvez tenha alguns que
#    estão em branco, mas são poucos, são as mais recentes."*
#
# Ou seja: não é "o que o Omie tem" por acaso — é o valor **acordado** entre a
# nota e o título, conferido por ele ao longo do tempo. É, portanto, o melhor
# (e o único) registro que existe dos tributos das notas antigas, e é o LADO DA
# NOTA da base que ele preenche.
#
# As colunas `omie_*` da base ficam para o que a consulta ao Omie devolver
# AGORA: é comparando as duas que se vê se o título saiu do lugar depois.
#
# A ORDEM é a do Apps Script (PIS, COFINS, **CSLL, IR**, ISS, INSS) e NÃO a do
# nosso cabeçalho: trocar uma pela outra põe o ISS no lugar do IR.
NB_TRIBUTOS = {
    "pis": 53, "cofins": 55, "csll": 57,
    "ir": 59, "iss": 61, "inss": 63,
}
# O "retido ou não" de cada um, na coluna seguinte à do valor. O dono quer essa
# informação por um motivo só: compatibilizar com o Omie.
NB_TRIBUTOS_RETEM = {k: v + 1 for k, v in NB_TRIBUTOS.items()}


def de_notas_bws(linha: list) -> dict:
    """Traduz uma linha da "Notas BWS" para os campos da base consolidada."""
    def c(i):
        return _txt(linha[i]) if i < len(linha) else ""

    d = {
        "nota_numero": c(NB_NUMERO),
        "data_emissao": c(NB_DATA_EMISSAO),
        "valor_total": c(NB_VALOR),
        "obra_codigo": c(NB_OBRA),
        "medicao_numero": c(NB_MEDICAO),
        "observacao": c(NB_OBSERVACAO),
        "status": status_da_observacao(c(NB_OBSERVACAO)),
        "data_recebimento": c(NB_DATA_RECEBIMENTO),
        "valor_recebido": c(NB_VALOR_RECEBIDO),
        "valor_liquido_previsto": c(NB_LIQUIDO_DESTAQUE),
        "origem": "consolidacao",
    }
    # Os tributos EQUALIZADOS de BB:BM entram como o lado da NOTA: é o valor
    # acordado, e para as notas antigas é o único registro que existe. As poucas
    # linhas em branco são as mais recentes, que ele ainda não equalizou — e
    # vazio aqui é o que impede a tela do Omie de mexer nelas.
    for nome, idx in NB_TRIBUTOS.items():
        d[nome] = c(idx)
        d[f"retem_{nome}"] = (c(NB_TRIBUTOS_RETEM[nome]).upper()[:1] or "")
    return d


def _positivo(v) -> bool:
    try:
        return _decimal(v) > 0
    except Exception:
        return False


def _decimal(v):
    from decimal import Decimal
    t = _txt(v).replace("R$", "").replace(" ", "")
    if not t:
        return Decimal("0")
    # a planilha é pt-BR: 1.234,56
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    return Decimal(t)


def divergencia(a, b, tolerancia="0.01") -> str:
    """S quando os dois valores NÃO batem (acima da tolerância de centavos)."""
    from decimal import Decimal
    try:
        dif = abs(_decimal(a) - _decimal(b))
    except Exception:
        return ""
    return "S" if dif > Decimal(tolerancia) else "N"


def conferir_tributos(d: dict) -> str:
    """A nota e o título do Omie batem? É a conferência que o dono fazia à mão.

    Só responde quando há valor do Omie para comparar — sem isso, "não bate"
    seria mentira: significaria apenas que ninguém consultou o Omie ainda.
    """
    impostos = ("pis", "cofins", "ir", "csll", "inss", "iss")
    tem_omie = any(_positivo(d.get(f"omie_{i}")) for i in impostos)
    if not tem_omie:
        return ""
    # Compara APENAS o que a nota declarou. Imposto que a nota não declarou é o
    # caso da nota antiga (o emissor nunca gravou tributo) — e acusá-lo como
    # divergente marcaria TODAS elas, escondendo as divergências de verdade.
    # Declarado e ausente no Omie, isso sim é divergência.
    declarados = [i for i in impostos if _txt(d.get(i))]
    if not declarados:
        return ""
    for imposto in declarados:
        if divergencia(d.get(imposto), d.get(f"omie_{imposto}")) == "S":
            return "S"
    return "N"


def conferir_recebimento(d: dict) -> str:
    """O que entrou em conta bate com o líquido previsto?

    É a pergunta que fecha o ciclo e a razão de metade dos scripts do Apps
    Script existirem: quando não bate, o tributo no Omie precisa de ajuste para
    a baixa sair correta. Aqui ela fica respondida num campo, e não numa
    conferência manual."""
    if not _txt(d.get("valor_recebido")):
        return ""
    return divergencia(d.get("valor_recebido"), d.get("valor_liquido_previsto"))


# --------------------------------------------------------------------------- #
# As outras fontes — índices das abas que existem hoje
# --------------------------------------------------------------------------- #
# "Notas BWS Links": A id ("num - obra") | B numero | C ano | D obra |
#                    E nome_base | F link municipal | G link nacional | H recibo
LK_NUMERO, LK_MUN, LK_NAC, LK_REC = 1, 5, 6, 7

# "Protocolos" (importada da planilha de bases): A chave "OBRA-MED" |
#               B — | C card_id do Pipefy | D código de integração do Omie
PR_CHAVE, PR_CARD, PR_INTEGRACAO = 0, 2, 3

# "Controle Nacional": ver controle_nacional.CAB
CN_NUMERO, CN_CARD, CN_CODVERIF, CN_TOMA_CNPJ = 0, 1, 2, 6
CN_DCOMPET, CN_LINK_NAC, CN_LINK_MUN, CN_CHAVE, CN_LINK_REC = 8, 11, 12, 13, 14

LINK_CARD_PIPEFY = "https://app.pipefy.com/open-cards/"


def indexar(linhas: list, coluna: int, normalizar=None) -> dict:
    """Indexa linhas por uma coluna. A PRIMEIRA ocorrência ganha.

    Primeira e não última de propósito: estas abas têm duplicatas conhecidas (o
    dono citou "deduplicar as ~20 mil linhas" da Notas BWS Links como pendência
    desde setembro), e trocar de critério faria a base mudar de valor entre duas
    rodadas da consolidação sem nada ter mudado na origem.
    """
    saida = {}
    for linha in linhas:
        if coluna >= len(linha):
            continue
        chave = _txt(linha[coluna])
        if normalizar:
            chave = normalizar(chave)
        if chave and chave not in saida:
            saida[chave] = linha
    return saida


def _so_digitos(v) -> str:
    return "".join(c for c in _txt(v) if c.isdigit())


def chave_obra_medicao(obra, medicao) -> str:
    """A chave que a aba Protocolos usa: CÓDIGO-MEDIÇÃO, em maiúsculas."""
    return f"{_txt(obra).upper()}-{_txt(medicao).upper()}"


def cruzar(d: dict, proto=None, link=None, ctrl=None) -> dict:
    """Completa a linha com o que vem das outras abas de NOTAS.

    **A C. Diários não entra aqui**, e é decisão do dono (09/10/2026): obra,
    contrato, tributação, empresa e SCP são atributos da OBRA e a tela os cruza
    pelo código. O que a base guarda é fato da nota.

    Não sobrescreve o que já veio preenchido: a "Notas BWS" é a fonte dos valores
    da nota, e as outras abas só acrescentam."""
    def por(nome, valor):
        if valor and not d.get(nome):
            d[nome] = _txt(valor)

    if proto is not None:
        card = _txt(proto[PR_CARD]) if PR_CARD < len(proto) else ""
        por("card_id", card)
        if card:
            por("link_card", LINK_CARD_PIPEFY + card)
        if PR_INTEGRACAO < len(proto):
            por("omie_codigo_integracao", proto[PR_INTEGRACAO])

    if link is not None:
        for nome, i in (("link_nfse_municipal", LK_MUN),
                        ("link_nfse_nacional", LK_NAC),
                        ("link_recibo", LK_REC)):
            if i < len(link):
                por(nome, link[i])

    if ctrl is not None:
        def c(i):
            return _txt(ctrl[i]) if i < len(ctrl) else ""
        por("chave_acesso", c(CN_CHAVE))
        por("cod_verificacao", c(CN_CODVERIF))
        por("competencia", c(CN_DCOMPET))
        por("tomador_cnpj", c(CN_TOMA_CNPJ))
        por("card_id", c(CN_CARD))
        por("link_nfse_nacional", c(CN_LINK_NAC))
        por("link_nfse_municipal", c(CN_LINK_MUN))
        por("link_recibo", c(CN_LINK_REC))
        if c(CN_CHAVE) and not d.get("modelo"):
            d["modelo"] = "nacional"

    if not d.get("modelo"):
        # Chave de 50 dígitos só existe no nacional; sem ela, é o modelo antigo.
        d["modelo"] = "nacional" if len(_so_digitos(d.get("chave_acesso"))) == 50 else "abrasf"
    if not d.get("competencia") and d.get("data_emissao"):
        dt = _txt(d["data_emissao"])
        if len(dt) == 10 and dt[2] == "/":          # DD/MM/AAAA
            d["competencia"] = f"{dt[6:]}-{dt[3:5]}"
        elif len(dt) >= 7 and dt[4] == "-":         # AAAA-MM-DD
            d["competencia"] = dt[:7]
    if not d.get("nota_sequencial"):
        import worker
        seq = worker.sequencial_da_nota(d.get("nota_numero"))
        d["nota_sequencial"] = str(seq) if seq else ""
    d["divergencia_tributos"] = conferir_tributos(d)
    d["divergencia_recebimento"] = conferir_recebimento(d)
    return d


# --------------------------------------------------------------------------- #
# A planilha
# --------------------------------------------------------------------------- #
def _ws(planilha):
    try:
        return planilha.worksheet(ABA)
    except Exception:
        ws = planilha.add_worksheet(title=ABA, rows=6000, cols=len(CAB) + 4)
        ws.update(f"A1:{_col(len(CAB) - 1)}1", [CAB])
        ws.freeze(rows=1)
        return ws


def _col(indice: int) -> str:
    """0 → A, 25 → Z, 26 → AA. O cabeçalho tem 71 colunas (até BS)."""
    nome, i = "", indice + 1
    while i:
        i, resto = divmod(i - 1, 26)
        nome = chr(65 + resto) + nome
    return nome


def numeros_na_base(ws) -> dict:
    """{numero_da_nota: linha}. É o que torna a consolidação repetível: rodar de
    novo não duplica, continua de onde parou."""
    col = ws.col_values(IDX["nota_numero"] + 1)
    saida = {}
    for i, v in enumerate(col[1:], start=2):
        n = _txt(v)
        if n:
            saida.setdefault(n, i)
    return saida


def gravar(ws, dados: dict, linha_existente: int | None = None) -> str:
    """Grava a nota. Acrescenta se for nova, atualiza a linha se já existir.

    Devolve "nova" ou "atualizada" — o chamador usa isso no resumo, e saber a
    diferença importa: a consolidação roda várias vezes.
    """
    linha = montar_linha(dados)
    if linha_existente:
        ws.update(f"A{linha_existente}:{_col(len(CAB) - 1)}{linha_existente}",
                  [linha], value_input_option="USER_ENTERED")
        return "atualizada"
    ws.append_row(linha, value_input_option="USER_ENTERED", table_range="A1")
    return "nova"


def gravar_varias(ws, linhas: list[list]) -> int:
    """Acrescenta várias linhas de uma vez.

    Uma chamada por lote, e não uma por nota: são ~3.300 notas antigas, e o
    Google tem limite de chamadas por minuto. Uma nota por chamada estouraria a
    cota e deixaria a consolidação pela metade — já aconteceu nesta área com
    leitura de aba grande (ver CONTEXTO.md §3.7).
    """
    if not linhas:
        return 0
    ws.append_rows(linhas, value_input_option="USER_ENTERED", table_range="A1")
    return len(linhas)


# Quantas notas por rodada. O monorepo atende 4 pedidos por vez (1 worker, 4
# threads): uma consolidação que leia 3.300 notas de uma vez prende uma thread
# por minutos e já derrubou o serviço inteiro em 07/10/2026. Então vai em lotes,
# e a tela manda rodar de novo até acabar.
LOTE_PADRAO = 300


def consolidar(planilha, limite: int = LOTE_PADRAO) -> dict:
    """Preenche a Base Faturamento a partir das abas que existem hoje.

    Repetível e incremental: cada rodada processa até `limite` notas que ainda
    não estão na base. Rodar de novo continua; rodar duas vezes não duplica.

    Não lê a C. Diários: obra, contrato, tributação, empresa e SCP são atributos
    da OBRA, e a tela os cruza pelo código da obra (decisão do dono, 09/10/2026).
    """
    ws_base = _ws(planilha)
    ja_tem = numeros_na_base(ws_base)

    ws_notas = planilha.worksheet("Notas BWS")
    notas = ws_notas.get_all_values()

    def _aba(nome):
        try:
            return planilha.worksheet(nome).get_all_values()
        except Exception as e:
            print(f"  [aviso] aba '{nome}' não lida ({type(e).__name__}: {e}) — "
                  f"os campos que vêm dela ficam vazios nesta rodada.")
            return []

    links = indexar(_aba("Notas BWS Links"), LK_NUMERO)
    ctrl = indexar(_aba("Controle Nacional"), CN_NUMERO)
    protos = indexar(_aba("Protocolos"), PR_CHAVE, normalizar=lambda v: v.upper())

    novas, puladas = [], 0
    for linha in notas:
        if len(novas) >= limite:
            break
        numero = _txt(linha[NB_NUMERO]) if NB_NUMERO < len(linha) else ""
        if not numero or not numero[0].isdigit():      # cabeçalho e linhas de total
            continue
        if numero in ja_tem:
            puladas += 1
            continue
        d = de_notas_bws(linha)
        proto = protos.get(chave_obra_medicao(d["obra_codigo"], d["medicao_numero"]))
        cruzar(d, proto=proto, link=links.get(numero), ctrl=ctrl.get(numero))
        novas.append(montar_linha(d))

    gravadas = gravar_varias(ws_base, novas)
    faltam = sum(1 for linha in notas
                 if NB_NUMERO < len(linha)
                 and _txt(linha[NB_NUMERO])[:1].isdigit()
                 and _txt(linha[NB_NUMERO]) not in ja_tem) - gravadas
    return {"gravadas": gravadas, "ja_estavam": puladas, "faltam": max(faltam, 0),
            "total_na_base": len(ja_tem) + gravadas}


def ler_linhas(ws) -> list[dict]:
    """A base como lista de dicionários, cada um com `_linha` (a linha da aba).

    Uma leitura só da aba inteira, e não uma por nota: o Google limita chamadas
    por minuto, e é assim que a conferência do Omie consegue agrupar as notas
    por título antes de falar com ele."""
    vals = ws.get_all_values()
    if not vals:
        return []
    cab = [c.strip() for c in vals[0]]
    saida = []
    for i, row in enumerate(vals[1:], start=2):
        if not any(_txt(c) for c in row):
            continue
        d = {nome: (_txt(row[j]) if j < len(row) else "")
             for j, nome in enumerate(cab) if nome in IDX}
        d["_linha"] = i
        saida.append(d)
    return saida


def gravar_lote(ws, registros: list[dict]) -> int:
    """Regrava várias linhas de uma vez (uma chamada, não uma por nota).

    Cada registro traz `_linha`. Sem o lote, conferir 300 notas no Omie faria 300
    chamadas de escrita e estouraria a cota do Google no meio — e a base ficaria
    metade conferida, metade não, sem ninguém saber onde parou.
    """
    pedidos = []
    for d in registros:
        linha = d.get("_linha")
        if not linha:
            continue
        valores = montar_linha({k: v for k, v in d.items() if k in IDX})
        pedidos.append({"range": f"A{linha}:{_col(len(CAB) - 1)}{linha}",
                        "values": [valores]})
    if not pedidos:
        return 0
    ws.batch_update(pedidos, value_input_option="USER_ENTERED")
    return len(pedidos)
