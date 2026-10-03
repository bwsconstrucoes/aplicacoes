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
import re
from decimal import Decimal
from pathlib import Path

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

BEEVALE = "beevale"
SOMAPAY = "somapay"
DESTINOS = (BEEVALE, SOMAPAY)

# "analise" não é destino de pagamento: é a etiqueta do segundo arquivo, o de
# conferência. Fica junto porque é o mesmo nome de arquivo e o mesmo log.
ROTULO_DO_DESTINO = {BEEVALE: "BeeVale", SOMAPAY: "SomaPay",
                     "analise": "Analise da folha",
                     # O relatório em PDF de cada conta, gerado junto (03/10/2026).
                     "relatorio": "Relatório (PDF)"}

# As verbas que geram arquivo. O rótulo é o que aparece na tela e no nome do
# arquivo; a chave é a que vem da apropriação guardada.
ROTULO_DA_VERBA = {
    "folha": "Folha",
    "alimentacao": "Alimentação",
    "transporte": "Transporte",
    "diaria": "Diárias",
    "gratificacao": "Gratificação",
    # As solicitações de despesa com colaboradores do Pipefy (03/10/2026).
    "dc": "Despesas com colaboradores",
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
def montar_lotes_por_conta(linhas, destinos: dict, padrao: str,
                           juntar_verbas: bool = False) -> list:
    """Os lotes com o DESTINO ESCOLHIDO POR CONTA. FUNÇÃO PURA.

    O dono, 02/10/2026: *"e se eu quiser gerar uns arquivos Soma e outros
    BeeVale?"* — e a resposta dele: o agrupamento é por conta, e cada conta tem
    o seu seletor. `destinos` é `{conta: "beevale"|"somapay"}`; conta fora do
    mapa vai para o `padrao`."""
    escolhidos = {" ".join(str(c or "").split()): str(d or "").strip().lower()
                  for c, d in (destinos or {}).items()}
    for d in escolhidos.values():
        if d not in DESTINOS:
            raise ErroDaGeracao(
                f'destino "{d}" não reconhecido. Destinos aceitos: BeeVale e SomaPay.')
    por_destino: dict = {}
    for linha in linhas or []:
        conta = " ".join(str(linha.get("conta") or "").split())
        por_destino.setdefault(escolhidos.get(conta) or padrao, []).append(linha)
    lotes = []
    for destino in sorted(por_destino):
        lotes += montar_lotes(por_destino[destino], destino, juntar_verbas)
    return sorted(lotes, key=lambda l: (l.get("conta") or "", l.get("destino") or ""))


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
            f'destino "{destino}" não reconhecido. Destinos aceitos: BeeVale e SomaPay.')
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
        # A linha pode trazer a própria natureza e a carteira do BeeVale — é o
        # caso da DC, em que cada solicitação tem o seu tipo de despesa (o
        # `geraspbeevale.gs` consolida por CPF + carteira + categoria).
        nat = str(bruta.get("natureza") or "").strip() or natureza(verba)
        carteira = str(bruta.get("carteira") or "").strip()
        chave_item = (cpf, nat, carteira)
        item = lote["itens"].setdefault(chave_item, {
            "cpf": cpf, "nome": " ".join(str(bruta.get("nome") or "").split()),
            "verba": verba, "natureza": nat, "carteira": carteira,
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
                    f'{item["nome"] or "colaborador"} sem CPF — sem ele o '
                    "portal não efetua o pagamento.")
            if not item["nome"]:
                criticas.append(
                    f'o CPF {item["cpf"]} está sem nome — o banco valida o nome '
                    "contra o CPF e recusa a linha.")
        if not lote["conta"]:
            criticas.append(
                "linhas sem conta de pagamento. A conta é definida pela obra "
                '(aba "C. Diários"); preencha a conta da obra antes de gerar.')
        if len(itens) > MAXIMO_POR_ARQUIVO:
            criticas.append(
                f"{len(itens)} linhas em um único arquivo, acima do teto de "
                f"{MAXIMO_POR_ARQUIVO}. Quantidade incompatível com um pagamento.")

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


# ⚠️ O ARQUIVO DO SOMAPAY SAI DO MODELO DELES, PREENCHIDO — NÃO É MONTADO AQUI.
# Em 01/10/2026 o dono avisou que o arquivo gerado não passava no portal, nem
# acrescentando as linhas de cabeçalho, nem salvando como .xls, e mandou o modelo
# que o próprio site do Soma fornece. As diferenças, todas reais:
#
#   1. a aba do modelo se chama "Planilha de Folha de Pagamento" (a nossa,
#      "Valores");
#   2. os dados começam na LINHA 12 — antes vêm o título, as instruções e o
#      cabeçalho na linha 11 (a nossa começava na linha 2);
#   3. o CPF é SÓ OS 11 DÍGITOS, como texto ("Deve conter 11 dígitos. Ex.:
#      01234567891") — a nossa mandava "997.133.493-34", com ponto e traço;
#   4. o modelo é um arquivo do Excel novo com o nome terminando em ".xls".
#
# Em vez de imitar cada detalhe (e errar o quinto), o sistema abre o modelo e
# escreve só as três células de cada linha, a partir da 12, com o estilo que o
# modelo já tem nelas. Todo o resto — aba, instruções, logotipo, formatos — é o
# arquivo deles, byte a byte. O modelo está em `modelos/somapay_modelo.xlsx` (sem
# o nome de quem o criou, que vinha nas propriedades).
MODELO_SOMAPAY = Path(__file__).with_name("modelos") / "somapay_modelo.xlsx"
PRIMEIRA_LINHA_SOMAPAY = 12
ABA_SOMAPAY = "Planilha de Folha de Pagamento"
EXTENSAO_SOMAPAY = ".xls"     # a mesma do modelo (o conteúdo é o do Excel novo)

# As três células vazias de uma linha do modelo, e os estilos de dado dele: 21 é
# o texto (nome e CPF), 22 é o dinheiro em reais.
_CELULAS_VAZIAS = re.compile(
    r'<c r="A(\d+)" s="\d+"/><c r="B\1" s="\d+"/><c r="C\1" s="\d+"/>')
_ESTILO_TEXTO, _ESTILO_DINHEIRO = "21", "22"


def _celulas_somapay(linha: int, nome: str, cpf: str, valor) -> str:
    from xml.sax.saxutils import escape
    nome = "".join(c for c in str(nome or "") if c >= " ")
    return (f'<c r="A{linha}" s="{_ESTILO_TEXTO}" t="inlineStr"><is>'
            f'<t xml:space="preserve">{escape(nome)}</t></is></c>'
            f'<c r="B{linha}" s="{_ESTILO_TEXTO}" t="inlineStr"><is>'
            f"<t>{cpf}</t></is></c>"
            f'<c r="C{linha}" s="{_ESTILO_DINHEIRO}"><v>{_dinheiro(valor)}</v></c>')


def somapay_xlsx(linhas) -> bytes:
    """O arquivo do SomaPay: o modelo deles, com uma pessoa por linha a partir
    da linha 12 — nome, CPF (11 dígitos, texto) e valor (número em reais)."""
    import io
    import zipfile

    from .beevale import formata_cpf

    itens = [(str(l.get("nome") or ""), _cpf(l.get("cpf")), l.get("valor"))
             for l in linhas or []]
    repetido = _cpf_repetido([{"cpf": cpf} for _, cpf, _ in itens])
    if repetido:
        raise ErroDaGeracao(
            f"o CPF {formata_cpf(repetido)} aparece mais de uma vez, e o SomaPay "
            "recusa o arquivo inteiro. Gere as verbas em arquivos separados.")

    with zipfile.ZipFile(MODELO_SOMAPAY) as modelo:
        partes = [(info, modelo.read(info.filename)) for info in modelo.infolist()]
    caminho_da_aba = "xl/worksheets/sheet1.xml"
    folha = next(d for i, d in partes if i.filename == caminho_da_aba).decode("utf-8")

    ultima = PRIMEIRA_LINHA_SOMAPAY + len(itens) - 1
    usadas = set()

    def preencher(achado):
        linha = int(achado.group(1))
        if PRIMEIRA_LINHA_SOMAPAY <= linha <= ultima:
            usadas.add(linha)
            return _celulas_somapay(linha, *itens[linha - PRIMEIRA_LINHA_SOMAPAY])
        return achado.group(0)

    folha = _CELULAS_VAZIAS.sub(preencher, folha)
    # O modelo tem linhas até a 1000. Mais gente que isso ganha linhas novas, no
    # fim, com o mesmo estilo.
    extras = "".join(
        f'<row r="{n}" ht="22.5" customHeight="true">'
        + _celulas_somapay(n, *itens[n - PRIMEIRA_LINHA_SOMAPAY]) + "</row>"
        for n in range(PRIMEIRA_LINHA_SOMAPAY, ultima + 1) if n not in usadas)
    if extras:
        folha = folha.replace("</sheetData>", extras + "</sheetData>", 1)
        folha = re.sub(r'<dimension ref="A1:([A-Z]+)\d+"/>',
                       lambda m: f'<dimension ref="A1:{m.group(1)}{ultima}"/>',
                       folha, count=1)

    memoria = io.BytesIO()
    with zipfile.ZipFile(memoria, "w", zipfile.ZIP_DEFLATED) as saida:
        for info, dados in partes:
            saida.writestr(info, folha.encode("utf-8")
                           if info.filename == caminho_da_aba else dados)
    return memoria.getvalue()


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
            nome, email_do_cpf(linha.get("cpf")),
            str(linha.get("carteira") or "").strip() or CARTEIRA, BENEFICIO,
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
        raise ErroDaGeracao("lote sem linhas a pagar.")
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
    if str(tipo or "") == "dc":
        # A DC não tem competência e pode sair várias vezes no mês: o nome leva
        # o dia e a hora da geração, para um arquivo não se confundir com outro.
        from .horario import agora
        partes[1] = agora().strftime("%d-%m-%Y %Hh%M")
    rotulo_tipo = {"quinzena": "Quinzena", "fim_de_mes": "Fim de mes",
                   "dc": ""}.get(str(tipo or ""), str(tipo or ""))
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
    extensao = EXTENSAO_SOMAPAY if lote.get("destino") == SOMAPAY else ".xlsx"
    return f"{limpo.strip()[:150]}{extensao}"


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
                 tipo: str = "", por_obra=None, detalhe=None) -> bytes:
    """A planilha de análise: resumo, por obra, por funcionário e o rateio.

    `detalhe` são as linhas ANTES da consolidação do arquivo — uma por pessoa e
    obra, com os dias (`folha_pagamento.linhas_para_pagar`). É o que o `gerar`
    e a prévia passam.

    ⚠️ CONSERTO DE 01/10/2026. Sem o detalhe, as abas saíam das linhas do
    ARQUIVO, que juntam a pessoa por conta: quem tinha duas obras pagas pela
    mesma conta aparecia só na primeira — com o dinheiro todo nela, inclusive
    no "Por obra" e no "Rateio" —, e os dias saíam zerados.

    `pessoas` (a apropriação) e `por_obra` continuam aceitos; sem nenhum dos
    três, as abas saem do próprio pagamento, que é o mínimo."""
    detalhe = [d for d in (detalhe or []) if _dinheiro(d.get("valor")) > 0]
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
        aba.append(["Avisos da geração"])
        for conta, texto in criticas:
            aba.append([conta, texto])

    # --- POR OBRA ----------------------------------------------------------
    total_obra = por_obra
    if total_obra is None and detalhe:
        juntado = {}
        for item in detalhe:
            chave = str(item.get("obra") or "").strip() or "(sem obra)"
            alvo = juntado.setdefault(chave, {"obra": chave,
                                              "total": Decimal("0.00"),
                                              "pessoas": set()})
            alvo["total"] += _dinheiro(item.get("valor"))
            alvo["pessoas"].add(item.get("cpf"))
        total_obra = [{"obra": v["obra"], "total": v["total"],
                       "pessoas": len(v["pessoas"])} for v in juntado.values()]
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
    if detalhe:
        for item in sorted(detalhe, key=lambda i: (
                str(i.get("nome") or "").lower(), str(i.get("obra") or ""))):
            gente.append([item.get("nome", ""), formata_cpf(item.get("cpf")),
                          item.get("conta", ""), rotulo_da_verba(item.get("verba")),
                          item.get("obra", ""), int(item.get("dias") or 0),
                          float(_dinheiro(item.get("valor"))),
                          item.get("origem", "")])
    elif pessoas:
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
