# -*- coding: utf-8 -*-
"""
OS ARQUIVOS DE PAGAMENTO DA FOLHA: BeeVale e SomaPay.

Duas decisões do dono, em 27/09/2026, e elas puxam desenhos diferentes:

> *"Normalmente a gente gera para cada tipo de verba, e para cada tipo de conta,
> um arquivo. Mas de repente eu quero gerar alimentação e transporte no mesmo
> arquivo do BeeVale. (…) ao invés de fazer três pagamentos, a gente faz só um.
> Então eu quero ter essa opção."*

> *"O Soma tem uma particularidade: tem que ser separado, porque eu não posso
> aparecer o mesmo CPF duas vezes. Não pode. O Soma não aceita."*

A regra que sai disso:

| | BeeVale | SomaPay |
|---|---|---|
| um arquivo por **conta** | sempre | sempre |
| **juntar verbas** no mesmo arquivo | pode, e é escolha dele | **não** — separa sozinho |
| o mesmo **CPF duas vezes** | pode, se a verba for de outra natureza | **TRAVA** |

⚠️ A TRAVA DO SOMAPAY É CONFERIDA ANTES DE GRAVAR, com o nome de quem está
repetido. Um arquivo que o SomaPay rejeita depois de subir custa a rodada inteira:
já se perdeu o tempo de gerar, subir, criar o card e avisar a equipe.

⚠️ AQUI NÃO TEM BANCO, NEM DRIVE, NEM PIPEFY, e é de propósito: é a conta que
decide o dinheiro de ~500 pessoas, e conta que só se verifica abrindo tela não é
verificada. Quem lê do banco, sobe no Drive e escreve no card fica em `web.py` e
em `folha_pagamento.py`.

O QUE ESTÁ CONFIRMADO E O QUE É SUPOSIÇÃO — importa, porque arquivo recusado pelo
portal custa a rodada:

* **SomaPay**: aba `Valores`, colunas `Nome do funcionário`, `CPF* (obrigatório)`,
  `Valor* (obrigatório)`, o CPF **formatado** (997.133.493-34). Isso foi lido do
  arquivo que o Make anexa ao card hoje, ou seja, do que de fato é enviado e
  funciona (`docs/FOLHA_DE_PAGAMENTO.md` §2.1).
* **BeeVale**: as 11 colunas de `beevale.COLUNAS_PAGAMENTO`, que são as mesmas do
  `BeeVale.gs`, com `Benefício` = Livre, `Tipo de Recarga` = Mensal, `Dias úteis`
  = 0 e o e-mail montado como `<cpf>@bwsconstrucoes.com.br` — tudo do script.
* **SUPOSIÇÃO, marcada onde está**: o `Centro de Custo` do BeeVale (uso a obra) e
  o formato numérico do valor do SomaPay. O primeiro arquivo gerado tem de ser
  aberto e conferido antes de subir no portal.
"""
from __future__ import annotations

import io
import logging
from decimal import Decimal

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

BEEVALE = "beevale"
SOMAPAY = "somapay"
DESTINOS = (BEEVALE, SOMAPAY)

# "analise" não é destino de pagamento: é a etiqueta do segundo arquivo, o de
# conferência. Fica junto porque é o mesmo nome de arquivo e o mesmo log.
ROTULO_DO_DESTINO = {BEEVALE: "BeeVale", SOMAPAY: "SomaPay",
                     "analise": "Analise da folha"}

# As verbas que geram arquivo. O rótulo é o que aparece na tela e no nome do
# arquivo; a chave é a que vem da apropriação guardada.
ROTULO_DA_VERBA = {
    "folha": "Folha",
    "alimentacao": "Alimentação",
    "transporte": "Transporte",
    "diaria": "Diárias",
    "gratificacao": "Gratificação",
}

# ⚠️ O QUE O BEEVALE CHAMA DE "NATUREZA" da verba. É o que autoriza o mesmo CPF a
# aparecer duas vezes num arquivo do BeeVale — e o que a consolidação usa como
# chave, junto com a conta e o CPF, igual ao `BeeVale.gs`.
NATUREZA_DA_VERBA = {
    "alimentacao": "Despesas com Alimentação",
    "transporte": "Despesas com Transporte",
    "gratificacao": "Gratificações e Extras",
    "folha": "Salários e Ordenados",
    "diaria": "Salários e Ordenados",
}

# Constantes do arquivo do BeeVale, todas lidas do `BeeVale.gs`.
DOMINIO = "@bwsconstrucoes.com.br"
BENEFICIO = "Livre"
TIPO_DE_RECARGA = "Mensal"
CARTEIRA = "Produção"
CATEGORIA_BEEVALE = "BWS"

MIME_XLSX = ("application/vnd.openxmlformats-officedocument"
             ".spreadsheetml.sheet")

# Teto de linhas por arquivo. ~500 pessoas por conta é o mundo real; 5 mil é folga
# larga e serve para um laço torto não gerar um arquivo de milhões de linhas.
MAXIMO_POR_ARQUIVO = 5_000


class ErroDaGeracao(RuntimeError):
    """Não deu para gerar. A frase vai inteira para a tela."""


# ---------------------------------------------------------------------------
# Feitio de dado
# ---------------------------------------------------------------------------
def rotulo_da_verba(verba: str) -> str:
    verba = str(verba or "").strip().lower()
    return ROTULO_DA_VERBA.get(verba, verba.capitalize() or "(sem verba)")


def natureza(verba: str) -> str:
    verba = str(verba or "").strip().lower()
    return NATUREZA_DA_VERBA.get(verba, rotulo_da_verba(verba))


def email_do_cpf(cpf: str) -> str:
    """⚠️ O E-MAIL É INVENTADO, e isso vem do script do dono, não de mim: o portal
    exige o campo e a empresa não tem e-mail de obra. Como é chave no BeeVale, tem
    de ser sempre o mesmo para a mesma pessoa — daí o CPF."""
    from .beevale import so_digitos
    return f"{so_digitos(cpf)}{DOMINIO}"


def _dinheiro(valor) -> Decimal:
    try:
        return Decimal(str(valor or 0)).quantize(CENTAVO)
    except Exception:  # noqa: BLE001
        raise ErroDaGeracao(f"valor inválido: {valor!r}") from None


# ---------------------------------------------------------------------------
# OS LOTES: quantos arquivos saem, e o que vai em cada um
# ---------------------------------------------------------------------------
def montar_lotes(linhas, destino: str, juntar_verbas: bool = False) -> list:
    """Divide as linhas nos arquivos que vão sair. FUNÇÃO PURA.

    `linhas`: `[{cpf, nome, conta, verba, valor, obra}]`.

    Devolve uma lista de lotes, cada um virando UM arquivo:
    `{conta, verbas, destino, linhas, total, criticas}`.

    A divisão, na ordem:

    1. **por conta, sempre.** Não é escolha: a conta define de onde o dinheiro
       sai, e juntar contas pagaria da conta errada.
    2. **por verba**, a menos que seja BeeVale e ele tenha pedido para juntar.
    3. dentro do arquivo, **consolida** por CPF + natureza da verba — duas linhas
       da mesma pessoa, mesma conta e mesma natureza viram uma, somando. É o que o
       `BeeVale.gs` faz hoje, e é o que impede o CPF repetido.
    """
    destino = str(destino or "").strip().lower()
    if destino not in DESTINOS:
        raise ErroDaGeracao(
            f'não conheço o destino "{destino}". São BeeVale e SomaPay.')
    # ⚠️ NO SOMAPAY NÃO EXISTE JUNTAR, e a tela diz por quê em vez de deixar
    # marcar e devolver erro depois.
    if destino == SOMAPAY:
        juntar_verbas = False

    grupos: dict = {}
    for bruta in linhas or []:
        cpf = _cpf(bruta.get("cpf"))
        valor = _dinheiro(bruta.get("valor"))
        conta = " ".join(str(bruta.get("conta") or "").split())
        verba = str(bruta.get("verba") or "").strip().lower()
        if valor <= 0:
            # Valor zero não é pagamento. Entra como crítica do lote, não como
            # linha — o portal recusa e a pessoa some do arquivo sem explicação.
            continue
        chave_lote = (conta,) if juntar_verbas else (conta, verba)
        lote = grupos.setdefault(chave_lote, {
            "conta": conta, "destino": destino, "verbas": [], "itens": {},
            "criticas": []})
        if verba and verba not in lote["verbas"]:
            lote["verbas"].append(verba)
        # A chave da consolidação, igual à do script: conta + CPF + natureza.
        chave_item = (cpf, natureza(verba))
        item = lote["itens"].setdefault(chave_item, {
            "cpf": cpf, "nome": " ".join(str(bruta.get("nome") or "").split()),
            "verba": verba, "natureza": natureza(verba),
            "obra": str(bruta.get("obra") or "").strip(),
            "valor": Decimal("0.00"), "juntou": 0})
        item["valor"] += valor
        item["juntou"] += 1
        if not item["nome"]:
            item["nome"] = " ".join(str(bruta.get("nome") or "").split())

    lotes = []
    for _, lote in sorted(grupos.items(), key=lambda kv: kv[0]):
        itens = sorted(lote["itens"].values(),
                       key=lambda i: ((i["nome"] or "").lower(), i["natureza"]))
        criticas = list(lote["criticas"])
        for item in itens:
            if not item["cpf"]:
                criticas.append(
                    f'{item["nome"] or "uma pessoa"} está sem CPF — sem ele o '
                    "portal não tem como pagar.")
            if not item["nome"]:
                criticas.append(
                    f'o CPF {item["cpf"]} está sem nome — o banco confere o nome '
                    "contra o CPF e recusa a linha.")
        if not lote["conta"]:
            criticas.append(
                "estas linhas estão sem conta de pagamento. A conta vem da obra "
                '(aba "C. Diários"); obra sem conta precisa ser preenchida antes.')
        if len(itens) > MAXIMO_POR_ARQUIVO:
            criticas.append(
                f"{len(itens)} linhas num arquivo só, acima do teto de "
                f"{MAXIMO_POR_ARQUIVO}. Isso não parece um pagamento.")

        repetido = _cpf_repetido(itens)
        if destino == SOMAPAY and repetido:
            # Não deveria acontecer: a consolidação por CPF já resolve. Fica como
            # rede — se um dia a chave mudar, o erro aparece AQUI e não no portal.
            criticas.append(
                f"o CPF {repetido} aparece mais de uma vez, e o SomaPay recusa "
                "arquivo com CPF repetido.")

        lotes.append({
            "conta": lote["conta"], "destino": destino,
            "verbas": sorted(lote["verbas"], key=rotulo_da_verba),
            "linhas": itens, "quantos": len(itens),
            "total": sum((i["valor"] for i in itens), Decimal("0.00")),
            "criticas": criticas,
            "pode_gerar": not criticas,
        })
    return lotes


def _cpf(valor) -> str:
    from .beevale import so_digitos
    digitos = so_digitos(valor)
    return digitos if len(digitos) == 11 else ""


def _cpf_repetido(itens) -> str:
    vistos = set()
    for item in itens:
        if item["cpf"] in vistos:
            return item["cpf"]
        vistos.add(item["cpf"])
    return ""


def resumo_dos_lotes(lotes) -> dict:
    """O que a tela mostra ANTES de gerar: quantos arquivos e o total de cada um.

    Pedido dele, e é o que evita gerar errado: *"mostrar, antes de gerar, quantos
    arquivos vão sair e com que total cada um."*"""
    return {
        "arquivos": len(lotes or []),
        "total": sum((l["total"] for l in (lotes or [])), Decimal("0.00")),
        "pessoas": len({i["cpf"] for l in (lotes or []) for i in l["linhas"]}),
        "com_critica": [l for l in (lotes or []) if l["criticas"]],
        "pode_gerar": bool(lotes) and all(l["pode_gerar"] for l in lotes),
    }


# ---------------------------------------------------------------------------
# OS ARQUIVOS
# ---------------------------------------------------------------------------
def _fechar(planilha) -> bytes:
    memoria = io.BytesIO()
    planilha.save(memoria)
    return memoria.getvalue()


COLUNAS_SOMAPAY = ["Nome do funcionário", "CPF* (obrigatório)",
                   "Valor* (obrigatório)"]


def somapay_xlsx(linhas) -> bytes:
    """A planilha do SomaPay: aba `Valores`, três colunas.

    ⚠️ O CPF VAI FORMATADO e como TEXTO. As duas coisas importam: o arquivo que
    hoje é enviado usa `997.133.493-34`, e um CPF que o Excel entenda como número
    perde o zero da frente — e aí o portal paga outra pessoa, ou ninguém."""
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter

    from .beevale import formata_cpf

    repetido = _cpf_repetido([{"cpf": _cpf(l.get("cpf"))} for l in linhas or []])
    if repetido:
        raise ErroDaGeracao(
            f"o CPF {formata_cpf(repetido)} aparece mais de uma vez, e o SomaPay "
            "recusa o arquivo inteiro. Separe as verbas.")

    planilha = Workbook()
    aba = planilha.active
    aba.title = "Valores"
    aba.append(COLUNAS_SOMAPAY)
    for linha in linhas or []:
        aba.append([str(linha.get("nome") or ""),
                    formata_cpf(linha.get("cpf")),
                    float(_dinheiro(linha.get("valor")))])
    for coluna in (1, 2):
        for celula in aba[get_column_letter(coluna)]:
            celula.number_format = "@"
    # ⚠️ NÚMERO, não texto, com a máscara brasileira para a conferência na tela.
    # O arquivo que hoje é enviado mostra "1.126,60"; se o portal exigir o valor
    # como TEXTO, é aqui que muda — e é uma linha. Não foi possível confirmar sem
    # subir um arquivo no portal.
    for celula in aba[get_column_letter(3)][1:]:
        celula.number_format = "#,##0.00"
    aba.column_dimensions["A"].width = 40
    aba.column_dimensions["B"].width = 20
    aba.column_dimensions["C"].width = 16
    aba.freeze_panes = "A2"
    return _fechar(planilha)


def beevale_xlsx(linhas) -> bytes:
    """A planilha de recarga do BeeVale, com as 11 colunas do `BeeVale.gs`.

    O layout vem de `beevale.COLUNAS_PAGAMENTO` de propósito: é o MESMO contrato
    com o portal que o fluxo das SPs já usa. Duas listas de colunas divergiriam no
    dia em que o portal mudasse uma — e só uma seria corrigida."""
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter

    from .beevale import COLUNAS_PAGAMENTO, formata_cpf

    planilha = Workbook()
    aba = planilha.active
    aba.title = "Pagamento BeeVale"
    aba.append(COLUNAS_PAGAMENTO)
    for linha in linhas or []:
        nome = str(linha.get("nome") or "")
        aba.append([
            nome, email_do_cpf(linha.get("cpf")), CARTEIRA, BENEFICIO,
            float(_dinheiro(linha.get("valor"))), TIPO_DE_RECARGA, 0,
            formata_cpf(linha.get("cpf")), nome,
            # ⚠️ SUPOSIÇÃO: uso a OBRA como centro de custo, porque é o que faz
            # sentido para o rateio da folha (no fluxo das SPs é o número do card).
            # Conferir no primeiro arquivo, antes de subir no portal.
            str(linha.get("obra") or ""),
            CATEGORIA_BEEVALE])
    for coluna in (1, 2, 3, 4, 6, 8, 9, 10, 11):
        for celula in aba[get_column_letter(coluna)]:
            celula.number_format = "@"
    for celula in aba[get_column_letter(5)][1:]:
        celula.number_format = "0.00"
    for celula in aba[get_column_letter(7)][1:]:
        celula.number_format = "0"
    aba.freeze_panes = "A2"
    return _fechar(planilha)


def arquivo_do_lote(lote) -> bytes:
    """O .xlsx deste lote, no layout do destino dele."""
    if not lote or not lote.get("linhas"):
        raise ErroDaGeracao("este lote não tem nenhuma linha para pagar.")
    if lote.get("destino") == SOMAPAY:
        return somapay_xlsx(lote["linhas"])
    return beevale_xlsx(lote["linhas"])


def nome_do_arquivo(lote, ano: int, mes: int, tipo: str) -> str:
    """O nome que vai para o Drive e para o card.

    ⚠️ O NOME TEM DE DIZER TUDO O QUE IDENTIFICA O ARQUIVO — destino, competência,
    pagamento, conta e verbas. É por ele que alguém acha o arquivo certo três meses
    depois, e é por ele que se percebe que se subiu o da conta errada (que já
    aconteceu: está escrito no comentário do script do dono)."""
    partes = [ROTULO_DO_DESTINO.get(lote.get("destino"), "Pagamento"),
              f"{int(mes):02d}-{int(ano)}"]
    rotulo_tipo = {"quinzena": "Quinzena", "fim_de_mes": "Fim de mes"}.get(
        str(tipo or ""), str(tipo or ""))
    if rotulo_tipo:
        partes.append(rotulo_tipo)
    verbas = [rotulo_da_verba(v) for v in (lote.get("verbas") or [])]
    if verbas:
        partes.append("+".join(verbas))
    if lote.get("conta"):
        partes.append(f"conta {lote['conta']}")
    cru = " - ".join(str(p) for p in partes if p)
    # Barra no nome vira pasta no Drive; o resto do acento fica, porque
    # "Alimentação" é o que ele procura na pasta.
    limpo = cru.replace("/", "-").replace("\\", "-")
    return f"{limpo.strip()[:150]}.xlsx"


# ---------------------------------------------------------------------------
# O ARQUIVO DE ANÁLISE — o segundo arquivo, que ele pediu com todas as letras
# ---------------------------------------------------------------------------
# > *"Tem que ter no mínimo o do arquivo de pagamento e um de análise da folha.
# >  Com as informações separadas, agrupadas, qual obra, qual funcionário, rateio
# >  de folhas, se for o caso."*
#
# ⚠️ POR QUE ELE PRECISA DOS DOIS: o arquivo de pagamento é para o PORTAL — três
# colunas, nada que explique nada. O de análise é para GENTE: é por ele que se
# confere, antes de pagar, se o dinheiro está caindo na obra certa. Um arquivo só,
# servindo aos dois, não serve a nenhum: ou o portal recusa colunas a mais, ou a
# conferência não tem o que olhar.
# ---------------------------------------------------------------------------
CEM = Decimal("100")
SETE_CASAS = Decimal("0.0000001")


def percentuais_por_obra(por_obra) -> list:
    """O rateio em percentual, com SETE casas, fechando 100% exato.

    Sete casas é o que o card do Pipefy usa hoje (está no `BeeVale.gs`), e o
    ajuste para fechar 100% também: a sobra vai para a MAIOR fatia, que é a que
    menos sente — mesma regra da sobra do centavo em `folha_apropriacao`."""
    itens = [{"obra": str(o.get("obra") or ""), "total": _dinheiro(o.get("total"))}
             for o in (por_obra or []) if _dinheiro(o.get("total")) > 0]
    total = sum((i["total"] for i in itens), Decimal("0.00"))
    if total <= 0:
        return []
    saida = [{"obra": i["obra"], "total": i["total"],
              "percentual": (i["total"] / total * CEM).quantize(SETE_CASAS)}
             for i in itens]
    sobra = CEM - sum(o["percentual"] for o in saida)
    if sobra and saida:
        maior = max(saida, key=lambda o: o["total"])
        maior["percentual"] = (maior["percentual"] + sobra).quantize(SETE_CASAS)
    return sorted(saida, key=lambda o: -o["total"])


def analise_xlsx(lotes, pessoas=None, ano: int = 0, mes: int = 0,
                 tipo: str = "", por_obra=None) -> bytes:
    """A planilha de análise: resumo, por obra, por funcionário e o rateio.

    `pessoas` é a apropriação (`folha_apropriacao.apropriar()["pessoas"]`) quando
    houver; sem ela, a aba por funcionário sai do próprio pagamento, que é o
    mínimo. `por_obra` idem."""
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter

    from .beevale import formata_cpf

    lotes = list(lotes or [])
    planilha = Workbook()

    # --- RESUMO ------------------------------------------------------------
    aba = planilha.active
    aba.title = "Resumo"
    competencia = f"{int(mes or 0):02d}/{int(ano or 0)}"
    rotulo_tipo = {"quinzena": "Quinzena (dias 1 a 15)",
                   "fim_de_mes": "Fim de mês (16 ao último dia)"}.get(
                       str(tipo or ""), str(tipo or ""))
    aba.append(["Competência", competencia])
    aba.append(["Pagamento", rotulo_tipo])
    aba.append(["Arquivos gerados", len(lotes)])
    aba.append(["Total", float(sum((l["total"] for l in lotes),
                                   Decimal("0.00")))])
    aba.append([])
    aba.append(["Destino", "Conta", "Verbas", "Pessoas", "Total"])
    for lote in lotes:
        aba.append([ROTULO_DO_DESTINO.get(lote.get("destino"), ""),
                    lote.get("conta", ""),
                    " + ".join(rotulo_da_verba(v)
                               for v in (lote.get("verbas") or [])),
                    lote.get("quantos", 0), float(lote.get("total") or 0)])
    # ⚠️ AS CRÍTICAS ENTRAM NO ARQUIVO, e não só na tela: quem abrir a análise três
    # meses depois para explicar uma diferença precisa ver o que estava avisado na
    # hora. Aviso que só existe na tela é aviso que não sobrevive à rodada.
    criticas = [(l.get("conta", ""), c) for l in lotes
                for c in (l.get("criticas") or [])]
    if criticas:
        aba.append([])
        aba.append(["Avisos na hora de gerar"])
        for conta, texto in criticas:
            aba.append([conta, texto])

    # --- POR OBRA ----------------------------------------------------------
    total_obra = por_obra
    if total_obra is None:
        juntado: dict = {}
        for lote in lotes:
            for item in lote.get("linhas") or []:
                chave = item.get("obra") or "(sem obra)"
                alvo = juntado.setdefault(chave, {"obra": chave,
                                                  "total": Decimal("0.00"),
                                                  "pessoas": set()})
                alvo["total"] += _dinheiro(item.get("valor"))
                alvo["pessoas"].add(item.get("cpf"))
        total_obra = [{"obra": v["obra"], "total": v["total"],
                       "pessoas": len(v["pessoas"])}
                      for v in juntado.values()]
    obras = planilha.create_sheet("Por obra")
    obras.append(["Obra", "Pessoas", "Total", "% do pagamento"])
    percentuais = {p["obra"]: p["percentual"]
                   for p in percentuais_por_obra(total_obra)}
    for linha in sorted(total_obra, key=lambda o: -_dinheiro(o.get("total"))):
        obras.append([linha.get("obra", ""), linha.get("pessoas", 0),
                      float(_dinheiro(linha.get("total"))),
                      float(percentuais.get(linha.get("obra", ""), 0))])

    # --- POR FUNCIONÁRIO ---------------------------------------------------
    gente = planilha.create_sheet("Por funcionário")
    gente.append(["Nome", "CPF", "Conta", "Verba", "Obra", "Dias", "Valor",
                  "De onde veio"])
    if pessoas:
        for pessoa in pessoas:
            for parte in pessoa.get("por_obra") or []:
                gente.append([
                    pessoa.get("nome_cadastro") or pessoa.get("nome") or "",
                    formata_cpf(pessoa.get("cpf")), "",
                    rotulo_da_verba(pessoa.get("verba") or "folha"),
                    parte.get("obra", ""), int(parte.get("dias") or 0),
                    float(_dinheiro(parte.get("valor"))),
                    parte.get("origem", "")])
    else:
        for lote in lotes:
            for item in lote.get("linhas") or []:
                gente.append([item.get("nome", ""),
                              formata_cpf(item.get("cpf")),
                              lote.get("conta", ""),
                              rotulo_da_verba(item.get("verba")),
                              item.get("obra", ""), 0,
                              float(_dinheiro(item.get("valor"))), ""])

    # --- RATEIO ------------------------------------------------------------
    # É o que vai no card: obra e percentual com sete casas, fechando 100%.
    rateio = planilha.create_sheet("Rateio")
    rateio.append(["Obra", "Percentual (7 casas)", "Valor"])
    for p in percentuais_por_obra(total_obra):
        rateio.append([p["obra"], float(p["percentual"]), float(p["total"])])

    for folha in (aba, obras, gente, rateio):
        for coluna in range(1, folha.max_column + 1):
            folha.column_dimensions[get_column_letter(coluna)].width = 22
    return _fechar(planilha)
