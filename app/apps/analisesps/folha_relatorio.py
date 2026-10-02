# -*- coding: utf-8 -*-
"""
O RELATÓRIO DA FOLHA ABERTA — em Excel e em PDF. 01/10/2026.

Pedido dele: *"relatórios excel e pdf (…) o relatório do que eu visualizo em tela,
além de poder ver o agrupamento do pagamento. Por obra, por conta e etc."*

O que sai, nas duas versões:

  - a LISTA DA TELA, com os mesmos filtros que estão marcados na lateral — o
    relatório é "o que estou vendo", não a folha inteira (a não ser que não haja
    filtro nenhum). O cabeçalho diz quais filtros valiam;
  - os AGRUPAMENTOS: por obra (do ponto), por conta de pagamento, por obra da
    contabilidade (filial), por setor e por situação.

⚠️ OS AGRUPAMENTOS DE PAGAMENTO SÓ SOMAM QUEM VAI RECEBER (`entra`). Quem você
tirou, quem não casou com o cadastro e quem tem valor zero aparecem na lista e no
agrupamento por situação, mas NÃO no por obra e no por conta: esses dois são "de
onde sai o dinheiro", e somar quem não vai ser pago diria que uma obra custa mais
do que vai custar. O por obra divide cada pessoa pelos dias em cada obra — a
mesma divisão do arquivo de pagamento.

⚠️ NÃO É O FECHAMENTO. Sai da conta de agora: recarregar o ponto ou mexer nas
marcações muda o relatório. O cabeçalho diz isso.

⚠️ SÓ QUEM RECEBE, desde 02/10/2026. O dono: *"está aparecendo as pessoas que
não entram, que se não entra, não entra, não precisa sair desse relatório. Faz é
confundir."* A lista e os agrupamentos levam só quem está incluído no pagamento.

⚠️ E O RELATÓRIO PODE SAIR POR CONTA (mesmo dia): *"e se eu quiser só o PDF de
uma determinada conta (…) se eu precisar desses relatórios divididos, ou se eu
precisar juntos"*. `recortar_por_conta` deixa em cada pessoa só a parte paga
pela conta pedida — quem trabalhou em obras de duas contas aparece nas duas, cada
uma com a sua parte, e a soma das contas fecha com o total.

Este módulo não lê banco: recebe o que `folha_gestao.montar` já montou para a tela
e as contas das obras. Por isso a tela e o relatório não têm como divergir.
"""
from __future__ import annotations

import io
from decimal import Decimal

CENTAVO = Decimal("0.01")
SEM_OBRA = "(sem obra)"
SEM_CONTA = "(sem conta)"

ROTULO_DO_FILTRO = {
    "conta": "Conta de pagamento",
    "obra": "Obra do ponto",
    "obra_cadastro": "Obra do cadastro",
    "situacao": "Situação",
    "fase": "Fase Atual",
    "filial": "Obra da contabilidade",
    "setor": "Setor da contabilidade",
    "origem": "Como a obra foi definida",
}
ROTULO_DO_VALOR_ESPECIAL = {
    "__varias": "pagas em mais de uma conta",
    "__atencao": "afastados e desativar",
    "ponto": "pelas batidas de ponto",
    "regra": "pela regra de rateio",
    "mao": "ajuste manual na linha",
}

MIME_XLSX = ("application/vnd.openxmlformats-officedocument"
             ".spreadsheetml.sheet")


def _dinheiro(valor) -> Decimal:
    try:
        return Decimal(str(valor or 0)).quantize(CENTAVO)
    except Exception:  # noqa: BLE001 — texto estranho vira zero, não 500
        return Decimal("0.00")


def _percentual(parte, total) -> Decimal:
    total = _dinheiro(total)
    if total <= 0:
        return Decimal("0.00")
    return (_dinheiro(parte) * 100 / total).quantize(CENTAVO)


def filtros_em_texto(filtros, rotulo_da_situacao=None) -> list:
    """`["Obra do ponto: ABC, DEF", …]` — o que estava marcado, em português."""
    from .folha_gestao import CHAVES_DE_FILTRO, _marcados
    rotulo_da_situacao = rotulo_da_situacao or {}
    saida = []
    busca = " ".join(str((filtros or {}).get("busca") or "").split())
    if busca:
        saida.append(f'Procura: "{busca}"')
    for chave in CHAVES_DE_FILTRO:
        valores = sorted(_marcados(filtros, chave))
        if not valores:
            continue
        legiveis = [rotulo_da_situacao.get(v) if chave == "situacao" else
                    ROTULO_DO_VALOR_ESPECIAL.get(v, v) for v in valores]
        saida.append(f"{ROTULO_DO_FILTRO[chave]}: "
                     + ", ".join(str(v) for v in legiveis if v))
    return saida


def agrupamentos(pessoas, contas_por_obra) -> dict:
    """Os agrupamentos do relatório. FUNÇÃO PURA.

    `pessoas`: as linhas da tela (`montar()["pessoas"]`, já filtradas).
    `contas_por_obra`: `{OBRA: conta}`."""
    contas_por_obra = {str(k).upper(): v for k, v in (contas_por_obra or {}).items()}
    pagas = [p for p in (pessoas or []) if p.get("entra")]
    total_pago = sum((_dinheiro(p.get("valor")) for p in pagas), Decimal("0.00"))

    # --- por obra e por conta: a divisão de cada pessoa pelos dias -------
    obras: dict = {}
    contas: dict = {}
    for p in pagas:
        partes = p.get("por_obra") or []
        if not partes:
            partes = [{"obra": "", "dias": p.get("dias_no_ponto") or 0,
                       "valor": p.get("valor")}]
        for parte in partes:
            obra = " ".join(str(parte.get("obra") or "").split()).upper()
            conta = contas_por_obra.get(obra, "") if obra else ""
            valor = _dinheiro(parte.get("valor"))
            o = obras.setdefault(obra or SEM_OBRA, {
                "obra": obra or SEM_OBRA, "conta": conta or SEM_CONTA,
                "pessoas": set(), "dias": 0, "valor": Decimal("0.00")})
            o["pessoas"].add(p.get("cpf") or p.get("id_fortes"))
            o["dias"] += int(parte.get("dias") or 0)
            o["valor"] += valor
            c = contas.setdefault(conta or SEM_CONTA, {
                "conta": conta or SEM_CONTA, "obras": set(), "pessoas": set(),
                "valor": Decimal("0.00")})
            c["obras"].add(obra or SEM_OBRA)
            c["pessoas"].add(p.get("cpf") or p.get("id_fortes"))
            c["valor"] += valor

    por_obra = [{**o, "pessoas": len(o["pessoas"]),
                 "percentual": _percentual(o["valor"], total_pago)}
                for o in obras.values()]
    por_obra.sort(key=lambda o: (-o["valor"], o["obra"]))
    por_conta = [{**c, "obras": sorted(c["obras"]), "pessoas": len(c["pessoas"]),
                  "percentual": _percentual(c["valor"], total_pago)}
                 for c in contas.values()]
    # A sem conta vem primeiro: é a que segura o arquivo.
    por_conta.sort(key=lambda c: (c["conta"] != SEM_CONTA, -c["valor"]))

    # --- por pessoa inteira: filial, setor (só quem recebe) ---------------
    def juntar(campo, vazio):
        grupos: dict = {}
        for p in pagas:
            chave = " ".join(str(p.get(campo) or "").split()) or vazio
            g = grupos.setdefault(chave, {"nome": chave, "pessoas": 0,
                                          "valor": Decimal("0.00")})
            g["pessoas"] += 1
            g["valor"] += _dinheiro(p.get("valor"))
        saida = [{**g, "percentual": _percentual(g["valor"], total_pago)}
                 for g in grupos.values()]
        return sorted(saida, key=lambda g: (-g["valor"], g["nome"]))

    # --- por situação: TODO MUNDO da lista, inclusive quem não recebe ----
    situacoes: dict = {}
    for p in (pessoas or []):
        chave = p.get("situacao_rotulo") or p.get("situacao") or "-"
        s = situacoes.setdefault(chave, {"nome": chave, "pessoas": 0,
                                         "valor": Decimal("0.00")})
        s["pessoas"] += 1
        s["valor"] += _dinheiro(p.get("valor"))

    return {
        "total_pago": total_pago,
        "pessoas_pagas": len(pagas),
        "por_obra": por_obra,
        "por_conta": por_conta,
        "por_filial": juntar("filial", "(sem filial)"),
        "por_setor": juntar("setor_curto", "(sem setor)"),
        "por_situacao": sorted(situacoes.values(),
                               key=lambda s: (-s["pessoas"], s["nome"])),
    }


def _conta_da_obra(obra, contas_por_obra) -> str:
    obra = " ".join(str(obra or "").split()).upper()
    return (contas_por_obra.get(obra, "") if obra else "") or SEM_CONTA


def contas_da_folha(pessoas, contas_por_obra) -> list:
    """As contas que pagam alguém nesta lista — as opções do relatório."""
    contas_por_obra = {str(k).upper(): v for k, v in (contas_por_obra or {}).items()}
    achadas = set()
    for p in pessoas or []:
        if not p.get("entra"):
            continue
        for parte in (p.get("por_obra") or [{"obra": ""}]):
            achadas.add(_conta_da_obra(parte.get("obra"), contas_por_obra))
    return sorted(achadas, key=lambda c: (c == SEM_CONTA, c))


def recortar_por_conta(pessoas, conta: str, contas_por_obra) -> list:
    """Cada pessoa só com a parte paga pela `conta`. Quem não tem parte nela
    sai da lista."""
    contas_por_obra = {str(k).upper(): v for k, v in (contas_por_obra or {}).items()}
    saida = []
    for p in pessoas or []:
        partes = p.get("por_obra") or [{"obra": "", "dias": p.get("dias_no_ponto") or 0,
                                        "valor": p.get("valor")}]
        dela = [x for x in partes
                if _conta_da_obra(x.get("obra"), contas_por_obra) == conta]
        if not dela:
            continue
        valor = sum((_dinheiro(x.get("valor")) for x in dela), Decimal("0.00"))
        saida.append({
            **p, "por_obra": dela, "valor": valor,
            "dias_no_ponto": sum(int(x.get("dias") or 0) for x in dela),
            "obras_resumo": ", ".join(
                f"{x.get('obra') or SEM_OBRA} ({int(x.get('dias') or 0)})" for x in dela),
            "contas": [conta]})
    return saida


def montar(montado: dict, contas_por_obra: dict, conta: str = "") -> dict:
    """Tudo o que os dois formatos precisam, a partir do que a tela montou.
    `conta`: recorta o relatório numa conta de pagamento (vazio = todas)."""
    from .folha_gestao import ROTULO_DA_SITUACAO

    folha = montado.get("folha") or {}
    pessoas = [p for p in (montado.get("pessoas") or []) if p.get("entra")]
    if conta:
        pessoas = recortar_por_conta(pessoas, conta, contas_por_obra)
    lista = sum((_dinheiro(p.get("valor")) for p in pessoas), Decimal("0.00"))
    return {
        "conta": conta,
        "titulo": f"Folha da contabilidade {folha.get('competencia', '')}"
                  + (f" — conta {conta}" if conta else ""),
        "subtitulo": str(folha.get("rotulo_do_tipo") or ""),
        "competencia": str(folha.get("competencia") or ""),
        "filtros": filtros_em_texto(montado.get("filtros"), ROTULO_DA_SITUACAO),
        "fechada": bool(montado.get("fechamento")),
        "pessoas": pessoas,
        "quantas": len(pessoas),
        "de_quantas": int((montado.get("totais") or {}).get("pessoas") or 0),
        "total_da_lista": lista,
        "grupos": agrupamentos(pessoas, contas_por_obra),
    }


def _aviso_de_origem(dados) -> str:
    base = ("Gerado com os valores atuais, conforme a tela."
            if not dados["fechada"] else
            "Gerado com os valores atuais, conforme a tela (apropriação "
            "fechada; o arquivo de pagamento é gerado a partir do fechamento).")
    return base + (" Os totais por obra e por conta consideram apenas os valores a "
                   "pagar; cada colaborador é rateado pelos dias em cada obra.")


def _sim_nao(p) -> str:
    return "sim" if p.get("entra") else "não"


# ---------------------------------------------------------------------------
# EXCEL
# ---------------------------------------------------------------------------
def excel(dados: dict) -> bytes:
    """Uma aba por assunto: a lista da tela e cada agrupamento."""
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter

    planilha = Workbook()
    negrito = Font(bold=True)
    MOEDA = '#,##0.00'

    def aba_nova(titulo, cabecalho, primeira=False):
        aba = planilha.active if primeira else planilha.create_sheet()
        aba.title = titulo
        aba.append(cabecalho)
        for celula in aba[1]:
            celula.font = negrito
        aba.freeze_panes = "A2"
        return aba

    def larguras(aba, medidas):
        for i, largura in enumerate(medidas, start=1):
            aba.column_dimensions[get_column_letter(i)].width = largura

    def moeda(aba, colunas):
        for linha in aba.iter_rows(min_row=2):
            for i in colunas:
                linha[i].number_format = MOEDA

    # --- RESUMO --------------------------------------------------------
    resumo = aba_nova("Resumo", [dados["titulo"], dados["subtitulo"]], primeira=True)
    resumo.append(["Pessoas a pagar", dados["quantas"]])
    resumo.append(["Total de pessoas na folha", dados["de_quantas"]])
    resumo.append(["Total a pagar", float(dados["grupos"]["total_pago"])])
    resumo.append([])
    resumo.append(["Filtros"])
    resumo.cell(row=resumo.max_row, column=1).font = negrito
    for texto in dados["filtros"] or ["nenhum — folha completa"]:
        resumo.append([texto])
    resumo.append([])
    resumo.append([_aviso_de_origem(dados)])
    resumo.cell(row=4, column=2).number_format = MOEDA
    larguras(resumo, [34, 40])

    # --- A LISTA DA TELA ----------------------------------------------
    from .folha_rateio import cpf_bonito
    lista = aba_nova("Pessoas", [
        "Nome", "CPF", "Código Fortes", "Obra da contabilidade",
        "Setor", "Fase Atual", "Obra do ponto (dias)", "Obra do cadastro",
        "Conta(s)", "Dias", "Valor x dia", "Valor", "Situação"])
    for p in dados["pessoas"]:
        lista.append([
            p.get("nome_na_tela") or "",
            cpf_bonito(p.get("cpf") or "") if p.get("cpf") else "",
            p.get("id_fortes") or "", p.get("filial") or "",
            p.get("setor_curto") or "", p.get("fase") or "",
            p.get("obras_resumo") or "", p.get("obra_do_cadastro") or "",
            ", ".join(c for c in (p.get("contas") or []) if c),
            int(p.get("dias_no_ponto") or 0),
            float(p["valor_por_dia"]) if p.get("valor_por_dia") is not None else None,
            float(_dinheiro(p.get("valor"))), p.get("situacao_rotulo") or ""])
    moeda(lista, (10, 11))
    larguras(lista, [38, 15, 12, 30, 22, 18, 34, 18, 18, 6, 12, 13, 18])

    # --- OS AGRUPAMENTOS ----------------------------------------------
    g = dados["grupos"]
    aba = aba_nova("Por obra", ["Obra", "Conta", "Pessoas", "Dias", "Valor",
                                "% do total a pagar"])
    for o in g["por_obra"]:
        aba.append([o["obra"], o["conta"], o["pessoas"], o["dias"],
                    float(o["valor"]), float(o["percentual"])])
    aba.append(["Total", "", "", "", float(g["total_pago"]), 100.0])
    moeda(aba, (4,))
    larguras(aba, [30, 16, 10, 8, 15, 20])

    aba = aba_nova("Por conta", ["Conta", "Obras", "Pessoas", "Valor",
                                 "% do total a pagar"])
    for c in g["por_conta"]:
        aba.append([c["conta"], ", ".join(c["obras"]), c["pessoas"],
                    float(c["valor"]), float(c["percentual"])])
    aba.append(["Total", "", "", float(g["total_pago"]), 100.0])
    moeda(aba, (3,))
    larguras(aba, [16, 60, 10, 15, 20])

    for titulo, chave, rotulo in (("Por obra da contabilidade", "por_filial", "Filial"),
                                  ("Por setor", "por_setor", "Setor")):
        aba = aba_nova(titulo[:31], [rotulo, "Pessoas", "Valor",
                                     "% do total a pagar"])
        for linha in g[chave]:
            aba.append([linha["nome"], linha["pessoas"], float(linha["valor"]),
                        float(linha["percentual"])])
        moeda(aba, (2,))
        larguras(aba, [40, 10, 15, 20])

    memoria = io.BytesIO()
    planilha.save(memoria)
    return memoria.getvalue()


# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------
def _moeda_br(valor) -> str:
    texto = f"{_dinheiro(valor):,.2f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def _pct_br(valor) -> str:
    return f"{_dinheiro(valor):.2f}".replace(".", ",") + "%"


def pdf(dados: dict) -> bytes:
    """Os agrupamentos primeiro (é o que se lê numa reunião) e a lista depois."""
    from .pdf import Folha

    doc = Folha(dados["titulo"], dados["subtitulo"])
    g = dados["grupos"]
    doc.numeros([
        ("Pessoas a pagar", f"{dados['quantas']}"),
        ("Total a pagar", "R$ " + _moeda_br(g["total_pago"])),
    ])
    doc.observacao("Filtros: " + ("; ".join(dados["filtros"])
                                  if dados["filtros"] else
                                  "nenhum - folha completa") + ". "
                   + _aviso_de_origem(dados))

    doc.titulo_secao("Por conta de pagamento")
    doc.tabela(["Conta", "Obras", "Pessoas", "Valor", "%"],
               [[c["conta"], ", ".join(c["obras"]), str(c["pessoas"]),
                 _moeda_br(c["valor"]), _pct_br(c["percentual"])]
                for c in g["por_conta"]]
               + [["Total", "", "", _moeda_br(g["total_pago"]), "100,00%"]],
               larguras=[24, 104, 18, 26, 18], direita=(2, 3, 4), linhas_max=3)

    doc.titulo_secao("Por obra (do ponto)")
    doc.tabela(["Obra", "Conta", "Pessoas", "Dias", "Valor", "%"],
               [[o["obra"], o["conta"], str(o["pessoas"]), str(o["dias"]),
                 _moeda_br(o["valor"]), _pct_br(o["percentual"])]
                for o in g["por_obra"]]
               + [["Total", "", "", "", _moeda_br(g["total_pago"]), "100,00%"]],
               larguras=[62, 30, 20, 18, 34, 26], direita=(2, 3, 4, 5))

    for titulo, chave in (("Por obra da contabilidade", "por_filial"),
                          ("Por setor da contabilidade", "por_setor")):
        if g[chave]:
            doc.titulo_secao(titulo)
            doc.tabela([titulo.split(" ", 1)[1].capitalize(), "Pessoas", "Valor", "%"],
                       [[l["nome"], str(l["pessoas"]), _moeda_br(l["valor"]),
                         _pct_br(l["percentual"])] for l in g[chave]],
                       larguras=[110, 22, 34, 24], direita=(1, 2, 3))

    doc.titulo_secao(f"Pessoa por pessoa ({dados['quantas']})")
    doc.tabela(["Nome", "Contabilidade", "Obra do ponto", "Conta",
                "Dias", "Valor", "Situação"],
               [[p.get("nome_na_tela") or "", p.get("filial") or "",
                 p.get("obras_resumo") or "-",
                 ", ".join(c for c in (p.get("contas") or []) if c) or "-",
                 str(int(p.get("dias_no_ponto") or 0)),
                 _moeda_br(p.get("valor")), p.get("situacao_rotulo") or ""]
                for p in dados["pessoas"]],
               larguras=[54, 32, 40, 18, 10, 20, 16], direita=(4, 5),
               fonte=7, linhas_max=2)
    return doc.bytes()


def nome_do_arquivo(dados: dict, extensao: str) -> str:
    competencia = dados["competencia"].replace("/", "-")
    conta = (" - conta " + dados["conta"].replace("(", "").replace(")", "")
             if dados.get("conta") else "")
    filtrado = " - filtrado" if dados["filtros"] else ""
    return f"Folha {competencia}{conta}{filtrado}.{extensao}"


def zip_por_conta(montado: dict, contas_por_obra: dict, extensao: str) -> tuple:
    """Um relatório por conta de pagamento, num .zip. Devolve (bytes, nome)."""
    import zipfile
    contas = contas_da_folha(montado.get("pessoas"), contas_por_obra)
    memoria = io.BytesIO()
    with zipfile.ZipFile(memoria, "w", zipfile.ZIP_DEFLATED) as pacote:
        for conta in contas:
            dados = montar(montado, contas_por_obra, conta)
            conteudo = excel(dados) if extensao == "xlsx" else pdf(dados)
            pacote.writestr(nome_do_arquivo(dados, extensao), conteudo)
    competencia = str((montado.get("folha") or {}).get("competencia") or "").replace("/", "-")
    return memoria.getvalue(), f"Folha {competencia} - por conta ({extensao}).zip"
