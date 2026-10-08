# -*- coding: utf-8 -*-
"""
Monta a linha da aba 'Notas BWS' (colunas A–P). As colunas Q–BA são fórmulas
na planilha e NÃO são escritas (preenchem sozinhas).

Mapeamento (confirmado pelo cabeçalho + linha real CREPEEXU/3067):
 A Código Obra | B MM/YYYY | C MM | D Mês | E Ano | F Nº Nota | G Data Emissão
 H Valor da Nota | I Código Obra | J Nº Med. | K RF | L Observação
 M Data de Recebimento | N Valor Recebido em Conta
 O Valor a ser Recebido pelo Destaque (= líquido, valor - retenções)
 P Valor Líquido Tributado (= valor - todos os federais cheios: PIS 0,65 / COFINS 3 / IR 1,2 / CSLL 1,08 / INSS / ISS)
"""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from preview import brl

MESES = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
         "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]


def _num_medicao(card) -> str:
    """Nº da medição para a coluna J. Quando o 'Tipo de Documento' do card é de
    REAJUSTE (ex.: 'Solicitação de Pagamento de Medição de Reajuste'), o número
    recebe o sufixo 'R' (ex.: '7R')."""
    num = str(card.get("numero_medicao", "") or "").strip()
    tipo_doc = str(card.get("tipo_documento", "") or "").upper()
    if num and "REAJUSTE" in tipo_doc and not num.upper().endswith("R"):
        return f"{num}R"
    return num


@dataclass
class ValoresDaNota:
    """Os valores da nota lidos do XML dela — só o que a linha da planilha usa.

    Existe porque, depois de emitir, o card do Pipefy tem **doze campos
    limpos** (valor parcial, tipo de medição, alíquotas, banco…). Recalcular a
    nota a partir do card dias depois daria números diferentes dos que foram
    realmente emitidos. O XML é a fonte da verdade.
    """
    valor_total: Decimal
    valor_liquido: Decimal
    inss: Decimal
    iss: Decimal
    ir: Decimal
    pis: Decimal
    cofins: Decimal


def _d(v) -> Decimal:
    try:
        return Decimal(str(v).strip() or 0)
    except Exception:
        return Decimal("0")


def _cheio(do_xml: Decimal, aliquota: Decimal, total: Decimal) -> Decimal:
    """O federal cheio: o do XML quando houve retenção, senão a alíquota padrão.

    A coluna P desconta os federais cheios independentemente de retenção, e o XML
    só traz os retidos."""
    if do_xml > 0:
        return do_xml
    return (aliquota * total).quantize(Decimal("0.01"))


def valores_do_xml(xml_texto: str) -> ValoresDaNota:
    """Lê os valores da nota, aceitando o modelo NACIONAL e o antigo (ABRASF).

    Decide pelo conteúdo, e não por quem chama: há XML dos dois modelos
    arquivado, e quem precisa consertar uma linha da planilha não tem como saber
    em qual deles a nota saiu.

    **PIS, COFINS e IR podem não estar no XML, e isso é esperado:** eles só
    aparecem quando foram RETIDOS. A coluna P da planilha ("Valor Líquido
    Tributado"), porém, desconta os federais **cheios**, retidos ou não — é como
    a planilha sempre foi. Então: havendo o valor no XML, vale o do XML (é o que
    a nota realmente destacou, inclusive com alíquota diferenciada); não havendo,
    aplica-se a alíquota padrão sobre o total. É o mesmo número que o motor
    fiscal teria produzido nos dois casos.

    O que isto NÃO recupera: nota emitida com alíquota diferenciada e **sem**
    retenção daquele tributo. O campo de alíquota do card é um dos doze que a
    conclusão limpa, então esse dado não existe mais em lugar nenhum — a coluna P
    sai com a alíquota padrão. Afeta só a coluna P, que é informativa.
    """
    import xml.etree.ElementTree as ET
    from tributacao import ALIQ_PIS, ALIQ_COFINS, ALIQ_IR

    root = ET.fromstring(xml_texto.encode("utf-8") if isinstance(xml_texto, str) else xml_texto)
    for el in root.iter():                      # tira o namespace para os find funcionarem
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

    def t(*caminhos):
        for c in caminhos:
            el = root.find(".//" + c)
            if el is not None and (el.text or "").strip():
                return el.text.strip()
        return ""

    if root.find(".//infNFSe") is not None:      # modelo NACIONAL
        total = _d(t("vServ"))
        inss, iss = _d(t("vRetCP")), _d(t("vISSQN"))
        ir = _d(t("vRetIRRF"))
        pis = _d(t("vPis", "vRetPIS"))
        cofins = _d(t("vCofins", "vRetCofins"))
        csll = _d(t("vRetCSLL"))
    else:                                        # modelo antigo (ABRASF)
        total = _d(t("ValorServicos"))
        inss, iss = _d(t("ValorInss")), _d(t("ValorIss"))
        ir = _d(t("ValorIr"))
        pis = _d(t("ValorPis"))
        cofins = _d(t("ValorCofins"))
        csll = _d(t("ValorCsll"))

    # ⚠️ O líquido é CALCULADO, e não lido do `vLiq` da nota — isto não é
    # preferência. O `vLiq` do modelo nacional NÃO desconta PIS e COFINS: na nota
    # 3283 (08/10/2026) ele veio R$ 24.222,04 enquanto o que a BWS recebe de
    # fato é R$ 23.303,97, porque PIS (163,49) e COFINS (754,58) foram retidos.
    # A coluna O da planilha é "valor a ser recebido", ou seja valor menos TODAS
    # as retenções — então ela sai do mesmo jeito que o motor fiscal calcula.
    #
    # Só entram as retenções que estão NO XML: imposto não retido não aparece lá
    # (é a regra do E0699), então presença quer dizer retenção.
    liquido = total - inss - iss - ir - pis - cofins - csll

    return ValoresDaNota(
        valor_total=total,
        valor_liquido=liquido,
        inss=inss,
        iss=iss,
        # Daqui para baixo é a coluna P, que desconta os federais CHEIOS: o do
        # XML quando houve retenção, a alíquota padrão quando não houve.
        ir=_cheio(ir, ALIQ_IR, total),
        pis=_cheio(pis, ALIQ_PIS, total),
        cofins=_cheio(cofins, ALIQ_COFINS, total),
    )


def montar_linha(card, obra, r, numero, data_emissao_iso) -> list:
    a, m, d = data_emissao_iso.split("-")
    mes_nome = MESES[int(m)]
    valor = r.valor_total

    # P: Valor Líquido Tributado = valor - todos os federais a cheio, cada um
    # arredondado a 2 casas ANTES de somar (igual à planilha). CSLL aqui é 1,08%.
    csll_108 = (Decimal("0.0108") * valor).quantize(Decimal("0.01"))
    liquido_tributado = (valor - r.inss - r.iss - r.ir - r.pis - r.cofins - csll_108)

    return [
        card.get("codigo_obra", ""),                 # A Código Obra
        f"{mes_nome} / {a}",                          # B MM/YYYY
        str(int(m)),                                  # C MM
        mes_nome.upper(),                             # D Mês
        a,                                            # E Ano
        numero,                                       # F Nº Nota
        f"{int(d):02d}/{int(m):02d}/{a}",             # G Data Emissão
        brl(valor),                                   # H Valor da Nota
        card.get("codigo_obra", ""),                  # I Código Obra
        _num_medicao(card),                           # J Nº Med. ("7R" se Reajuste)
        "",                                           # K RF
        "",                                           # L Observação
        "",                                           # M Data de Recebimento
        "",                                           # N Valor Recebido em Conta
        brl(r.valor_liquido),                         # O Valor a ser Recebido pelo Destaque (líquido)
        brl(liquido_tributado),                       # P Valor Líquido Tributado
    ]


def ja_existe(ws, numero) -> bool:
    """True se o número já está na coluna F (Nº Nota) da Notas BWS."""
    col = ws.col_values(6)  # F
    alvo = str(numero).strip()
    return any(c.strip() == alvo for c in col)


def gravar_linha(ws, card, obra, r, numero, data_emissao_iso) -> bool:
    """Acrescenta a linha A–P na Notas BWS. Não grava se o número já existir."""
    if ja_existe(ws, numero):
        return False
    linha = montar_linha(card, obra, r, numero, data_emissao_iso)
    ws.append_row(linha, value_input_option="USER_ENTERED", table_range="A1")
    return True


def gravar_links(ws, numero, obra, ano, nome_base, link_municipal="", link_nacional="", link_recibo="") -> None:
    """Acrescenta a linha na Notas BWS Links.
    Colunas: A id | B numero | C ano | D obra | E nome_base |
             F link NFS-e (municipal) | G link NFS-e Nacional | H link Recibo
    """
    linha = [f"{numero} - {obra}", numero, ano, obra, nome_base,
             link_municipal, link_nacional, link_recibo]
    ws.append_row(linha, value_input_option="USER_ENTERED", table_range="A1")


def atualizar_links_nota(ws, numero, link_municipal=None, link_nacional=None, link_recibo=None) -> bool:
    """Preenche os links na linha já existente (casada pela coluna B):
    F=municipal, G=nacional, H=recibo. Atualiza só os passados (não-None)."""
    col = ws.col_values(2)   # coluna B = numero
    alvo = str(numero).strip()
    for i, v in enumerate(col, start=1):
        if str(v).strip() == alvo:
            if link_municipal is not None:
                ws.update(f"F{i}", [[link_municipal]], value_input_option="USER_ENTERED")
            if link_nacional is not None:
                ws.update(f"G{i}", [[link_nacional]], value_input_option="USER_ENTERED")
            if link_recibo is not None:
                ws.update(f"H{i}", [[link_recibo]], value_input_option="USER_ENTERED")
            return True
    return False
