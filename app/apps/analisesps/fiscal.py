# -*- coding: utf-8 -*-
"""
A conciliação fiscal: qual nota emitida contra o CNPJ da BWS é de qual SP.

O QUE SE ESTÁ RESOLVENDO. O fornecedor emite a nota contra a BWS. O FSist
monitora os CNPJs da empresa e entrega um relatório do que foi emitido. De um
lado há o lançamento (a SP, com credor, valor e às vezes o número da nota); do
outro, a nota. Ligar os dois era trabalho de olho, uma a uma.

O ERRO DE CONCEITO QUE ESTE ARQUIVO CORRIGE, e ele custou o acerto da análise
que rodava na planilha até 11/09/2026: lá, o CPF/CNPJ do credor valia
**30 pontos** quando batia com o DESTINATÁRIO da nota, e **8** quando batia com
o EMITENTE. Está de cabeça para baixo.

O relatório é de notas emitidas CONTRA a BWS: o destinatário é SEMPRE a BWS, em
todas as linhas — não distingue nota nenhuma de nota nenhuma. Quem emitiu é o
fornecedor, ou seja, **o credor do lançamento**. E isso não é estatística, é
estrutura: os dígitos 7 a 20 da chave de acesso são, pelo layout da NF-e, o
CNPJ de quem emitiu.

A REGRA QUE GOVERNA O ARQUIVO INTEIRO: casar errado é pior do que não casar.
Uma nota ligada à SP errada vira dedução indevida, e o erro não aparece na
tela — aparece na contabilidade, meses depois. Por isso nada aqui decide
sozinho acima de uma linha: o sistema PROPÕE com um grau de confiança e diz
por quê; quem confirma é gente.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
import unicodedata

logger = logging.getLogger("analisesps.fiscal")

# ---------------------------------------------------------------------------
# AS CATEGORIAS DO CAMPO "Documentação Fiscal" DO CARD
#
# A lista é a do Pipefy, na ordem dele. A dedutibilidade saiu da aba de apoio
# da planilha do dono — é a tabela DELE, não uma opinião daqui — e as três que
# faltavam lá (BeeVale, Férias ou PL, Rescisões) ele respondeu em 11/09/2026:
# as três são dedutíveis.
# ---------------------------------------------------------------------------
NAO_DEDUTIVEIS = {
    "Ausente",
    "Nota Cancelada",
    "Reanalisar",
    "Emissão Futura",
    "Não Dedutível",
}

CATEGORIAS = [
    "NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)",
    "NFC-e (Cupom Fiscal eletrônico)", "Guia de Tributo", "Seguros",
    "Taxas Diversas", "Contrato", "Contrato (Alterar Titularidade)",
    "BeeVale", "Nota de Débito/Fatura", "Ausente",
    "Aguardando Nota (Ilegível)", "Aguardando Nota (Não Anexada)",
    "Nota Cancelada", "Reanalisar", "Fundo Fixo", "Emissão Futura",
    "Rescisões (TRCT e Multa)", "Férias ou PL", "Presente", "Não Dedutível",
]


def dedutivel(categoria: str) -> bool:
    """Aquela categoria dá dedução? A tabela é do dono, não deste código."""
    return str(categoria or "").strip() not in NAO_DEDUTIVEIS


# ---------------------------------------------------------------------------
# O QUE NEM SE TENTA CONCILIAR
#
# Herdado do script da planilha, que já acertava nisto: uma apólice de seguro,
# um contrato de aluguel ou uma guia de tributo NÃO TÊM nota eletrônica para
# casar. Procurar par para elas só produziria ruído e faria a pessoa desconfiar
# do resto da tela.
# ---------------------------------------------------------------------------
NAO_CONCILIA = {
    "NFC-e (Cupom Fiscal eletrônico)", "Guia de Tributo", "Seguros",
    "Taxas Diversas", "Contrato", "Contrato (Alterar Titularidade)",
    "Nota de Débito/Fatura", "Fundo Fixo", "Rescisões (TRCT e Multa)",
    "Férias ou PL", "Presente", "Não Dedutível", "BeeVale",
}

# ---------------------------------------------------------------------------
# O QUE O TIPO DE DESPESA DO CARD SUGERE
#
# Tirado do histórico do próprio dono, não inventado: na planilha dele, em 18
# dos 19 tipos de despesa a categoria escolhida foi SEMPRE a mesma. Serve para
# PROPOR e poupar o clique — nunca para decidir sozinho, e a tela sempre mostra
# que foi uma proposta.
# ---------------------------------------------------------------------------
CATEGORIA_POR_TIPO_DESPESA = {
    "veiculos (taxas, impostos, multas)": "Seguros",
    "locacao de equipamentos": "Nota de Débito/Fatura",
    "agua e energia": "Nota de Débito/Fatura",
    "internet, telefonia e sistemas": "Nota de Débito/Fatura",
    "alugueis e condominios": "Contrato",
    "multas e processos trabalhistas": "Rescisões (TRCT e Multa)",
    "cartorios, crea, taxas": "Taxas Diversas",
}

# Os tipos de material, que no histórico do dono deram sempre NF-e. São muitos
# e crescem com o cadastro da empresa, então a regra é por PALAVRA e não por
# lista fechada — e vale só quando nenhuma regra exata acima casou.
PALAVRAS_DE_MERCADORIA = (
    "material", "ferramenta", "parafuso", "ferragen", "telha", "argamassa",
    "impermeabilizante", "aditivo", "cola", "pintura", "forro", "hidraulic",
    "eletric", "climatizac", "cabeamento",
)


def categoria_sugerida(tipo_despesa: str) -> str:
    """A categoria que o histórico do dono sugere para este tipo de despesa."""
    arrumado = _texto(tipo_despesa).lower()
    if not arrumado:
        return ""
    if arrumado in CATEGORIA_POR_TIPO_DESPESA:
        return CATEGORIA_POR_TIPO_DESPESA[arrumado]
    if any(p in arrumado for p in PALAVRAS_DE_MERCADORIA):
        return "NF-e (Mercadoria)"
    return ""


# ---------------------------------------------------------------------------
# Arrumar o que vem da planilha
# ---------------------------------------------------------------------------
def _texto(s) -> str:
    limpo = unicodedata.normalize("NFD", str(s or ""))
    limpo = "".join(c for c in limpo if not unicodedata.combining(c))
    return " ".join(limpo.split()).strip()


def so_digitos(s) -> str:
    return re.sub(r"\D", "", str(s or ""))


def emitente_da_chave(chave: str) -> str:
    """O CNPJ de quem emitiu, lido de DENTRO da chave de acesso.

    Os 44 dígitos da chave têm posição fixa definida pela Receita, e os
    dígitos 7 a 20 são o CNPJ do emitente. Ler daí é mais confiável do que a
    coluna do relatório, que vem com formatação variada — e é o que prova que
    o credor do lançamento é o emitente, não o destinatário."""
    digitos = so_digitos(chave)
    return digitos[6:20] if len(digitos) == 44 else ""


def mesmo_documento(a: str, b: str) -> bool:
    """Dois CPF/CNPJ são da mesma pessoa?

    PARA CNPJ, COMPARA SÓ A RAIZ (os oito primeiros dígitos). Matriz e filial
    têm CNPJ diferente e são o mesmo fornecedor — a nota sai da filial que
    entregou, e o cadastro do credor quase sempre tem a matriz. Exigir os
    catorze dígitos faria a conciliação perder justamente os casos comuns."""
    x, y = so_digitos(a), so_digitos(b)
    if not x or not y:
        return False
    if len(x) == 14 and len(y) == 14:
        return x[:8] == y[:8]
    return x == y


def mesmo_numero_de_nota(a, b) -> bool:
    """O número da nota, sem os zeros à esquerda ("000123" é "123")."""
    x = so_digitos(a).lstrip("0")
    y = so_digitos(b).lstrip("0")
    return bool(x) and x == y


def _nome_parecido(a: str, b: str) -> bool:
    x = re.sub(r"[^A-Z0-9 ]", "", _texto(a).upper())
    y = re.sub(r"[^A-Z0-9 ]", "", _texto(b).upper())
    if not x or not y:
        return False
    return x in y or y in x


def _para_data(v):
    if isinstance(v, dt.date):
        return v
    s = str(v or "").strip()
    for formato in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s[:10], formato).date()
        except ValueError:
            continue
    return None


def _para_numero(v):
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v or "").replace("R$", "").strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# A PONTUAÇÃO
#
# Cada pedaço vale o quanto ele REALMENTE distingue uma nota das outras — e é
# nisso que a versão da planilha errava. A soma máxima é 100 de propósito: o
# número que aparece na tela é lido como "confiança", e uma escala que passa
# de cem confunde quem decide.
# ---------------------------------------------------------------------------
# Quem emitiu a nota é o credor do lançamento. É o sinal mais forte que existe,
# e é o que a versão antiga valorizava ao contrário.
PONTOS_EMITENTE = 35
# O número da nota, quando quem lançou já o digitou no card. Sozinho não basta
# (fornecedores diferentes repetem número), mas junto com o emitente fecha.
PONTOS_NUMERO = 25
# O valor. Exato vale muito; perto vale pouco, porque frete e desconto mexem
# no total e valores redondos se repetem entre notas diferentes.
PONTOS_VALOR_EXATO = 25
PONTOS_VALOR_PERTO = 12
# O nome, para quando o cadastro do CNPJ está errado — acontece.
PONTOS_NOME = 10
# A data só CONFIRMA; nunca escolhe. Por isso vale pouco.
PONTOS_DATA = 5

# Quanto o valor pode variar e ainda contar como "perto": dez reais. Acima
# disso é outra nota, não arredondamento.
TOLERANCIA_VALOR = 10.0

# A nota é emitida ANTES de a despesa vencer, e o vencimento costuma ser em
# até três meses. A versão da planilha aceitava 180 dias para os dois lados, o
# que na prática não filtrava nada.
# O SINAL IMPORTA, e eu já o inverti uma vez: a conta é (vencimento − emissão).
# A nota vem ANTES do vencimento, então essa conta é POSITIVA no caso normal, e
# pode ser grande num parcelamento longo. Negativa é nota emitida DEPOIS do
# vencimento — acontece, mas pouco, e por isso a folga desse lado é curta.
DIAS_ANTES = 400          # emitida até 400 dias antes de vencer
DIAS_DEPOIS = 30          # emitida até 30 dias depois de vencer


def pontuar(lancamento: dict, nota: dict) -> tuple[int, list]:
    """Quanto esta nota combina com este lançamento, e POR QUÊ.

    Devolve (0 a 100, razões em português). As razões não são enfeite: é o que
    a tela mostra para a pessoa poder discordar com conhecimento de causa."""
    pontos = 0
    porques: list = []

    # 1. Quem emitiu. Lê o CNPJ de dentro da chave quando a coluna vier suja.
    emitente = nota.get("emitente_doc") or emitente_da_chave(nota.get("chave"))
    if mesmo_documento(lancamento.get("documento"), emitente):
        pontos += PONTOS_EMITENTE
        porques.append("o CNPJ do credor é o de quem emitiu a nota")

    # 2. O número que quem lançou digitou no card.
    if mesmo_numero_de_nota(lancamento.get("nf"), nota.get("numero")):
        pontos += PONTOS_NUMERO
        porques.append(f"o nº da nota bate ({nota.get('numero')})")

    # 3. O valor.
    valor_sp = _para_numero(lancamento.get("valor"))
    valor_nota = _para_numero(nota.get("valor"))
    if valor_sp is not None and valor_nota is not None:
        diferenca = abs(valor_sp - valor_nota)
        if diferenca < 0.005:
            pontos += PONTOS_VALOR_EXATO
            porques.append("o valor é igual")
        elif diferenca <= TOLERANCIA_VALOR:
            pontos += PONTOS_VALOR_PERTO
            porques.append(f"o valor difere em R$ {diferenca:.2f}".replace(".", ","))

    # 4. O nome, quando o documento não ajudou.
    if _nome_parecido(lancamento.get("credor"), nota.get("emitente")):
        pontos += PONTOS_NOME
        porques.append("o nome do credor bate com o do emitente")

    # 5. A data, só para confirmar.
    emissao = _para_data(nota.get("emissao"))
    if emissao:
        for campo in ("vencimento", "data_pagamento"):
            alvo = _para_data(lancamento.get(campo))
            if alvo and -DIAS_DEPOIS <= (alvo - emissao).days <= DIAS_ANTES:
                pontos += PONTOS_DATA
                porques.append("a data de emissão é compatível")
                break

    # A nota cancelada não é uma candidata melhor por ser cancelada — mas
    # também não pode ser escondida: se ela É a nota daquele lançamento, quem
    # analisa PRECISA ver, porque pagar com nota cancelada é problema fiscal.
    # Por isso o desconto é pequeno, e o motivo vai escrito.
    if _texto(nota.get("status")).upper() == "CANCELADA":
        pontos -= 10
        porques.append("ATENÇÃO: esta nota está CANCELADA")

    return max(0, min(100, pontos)), porques


# A partir de quanto o sistema PROPÕE a nota. Abaixo disso ele mostra a
# candidata, mas diz que não confia — a pessoa é quem decide.
CONFIANCA_PARA_PROPOR = 60

# A diferença mínima para a melhor candidata ser considerada SOZINHA. Duas
# notas empatadas quase sempre são o mesmo fornecedor no mesmo dia, e escolher
# uma delas no par ou ímpar é exatamente o erro que este arquivo existe para
# não cometer.
MARGEM_SOBRE_A_SEGUNDA = 15


def melhor_nota(lancamento: dict, notas: list) -> dict:
    """A nota que mais combina com este lançamento, com o porquê e a dúvida.

    Devolve sempre um dicionário, mesmo sem candidata — a tela precisa poder
    dizer "procurei e não achei", que é diferente de "não procurei"."""
    avaliadas = []
    for nota in notas:
        pontos, porques = pontuar(lancamento, nota)
        if pontos > 0:
            avaliadas.append({"nota": nota, "pontos": pontos, "porques": porques})
    if not avaliadas:
        return {"nota": None, "pontos": 0, "porques": [], "empate": False,
                "propoe": False}

    avaliadas.sort(key=lambda x: -x["pontos"])
    melhor = avaliadas[0]
    segunda = avaliadas[1] if len(avaliadas) > 1 else None
    empate = bool(segunda and melhor["pontos"] - segunda["pontos"] < MARGEM_SOBRE_A_SEGUNDA)

    melhor["empate"] = empate
    melhor["segunda"] = segunda["nota"] if segunda else None
    melhor["propoe"] = melhor["pontos"] >= CONFIANCA_PARA_PROPOR and not empate
    if empate:
        melhor["porques"] = melhor["porques"] + [
            "há outra nota quase tão parecida — confira antes de confirmar"]
    return melhor
