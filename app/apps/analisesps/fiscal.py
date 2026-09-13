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
# A CATEGORIA VEM DE DENTRO DA CHAVE — não de palpite
#
# Os dígitos 21 e 22 da chave de acesso são o MODELO do documento, por
# definição da Receita. Conferido nas chaves reais da planilha do dono, e bate
# exatamente com o que ele classificou à mão.
#
# Achada a nota, a categoria é CERTEZA. É a diferença entre propor e adivinhar.
# ---------------------------------------------------------------------------
MODELOS = {
    "55": "NF-e (Mercadoria)",
    "57": "CT-e (Frete)",
    "65": "NFC-e (Cupom Fiscal eletrônico)",
}


def categoria_da_chave(chave: str) -> str:
    """A categoria do documento, lida de dentro da própria chave."""
    digitos = so_digitos(chave)
    if len(digitos) != 44:
        return ""
    return MODELOS.get(digitos[20:22], "")


# ---------------------------------------------------------------------------
# O QUE O TIPO DE DESPESA SUGERE — e o papel dele é PEQUENO
#
# A proposta inicial era usar o tipo de despesa para apontar divergência de
# categoria. O DONO RECUSOU em 11/09/2026, e com razão:
#
#   "Categoria de despesa não vai ser regra para dedutibilidade ou não, porque
#    você pode comprar um material elétrico SEM nota fiscal. Então nesse caso
#    vai ser não dedutível. O FATO DE TER A NOTA FISCAL é que vai ser o
#    balizador. A simples divergência de material elétrico nem adianta mostrar."
#
# Ele está certo, e isso derruba a regra por palavra ("material" → NF-e) que
# existia aqui: sugerir NF-e para uma compra sem nota seria exatamente o erro
# que ele apontou.
#
# SOBRA UM PAPEL, e só este: as despesas que NUNCA têm nota eletrônica. Aluguel
# tem contrato, veículo tem apólice, água e energia têm fatura, cartório tem
# recibo de taxa. Nessas, e só quando nenhuma nota foi encontrada, o tipo de
# despesa diz qual documento procurar.
# ---------------------------------------------------------------------------
CATEGORIA_SEM_NOTA_ELETRONICA = {
    "veiculos (taxas, impostos, multas)": "Seguros",
    "locacao de equipamentos": "Nota de Débito/Fatura",
    "agua e energia": "Nota de Débito/Fatura",
    "internet, telefonia e sistemas": "Nota de Débito/Fatura",
    "alugueis e condominios": "Contrato",
    "multas e processos trabalhistas": "Rescisões (TRCT e Multa)",
    "cartorios, crea, taxas": "Taxas Diversas",
}


def categoria_sugerida(tipo_despesa: str) -> str:
    """A categoria para quando NÃO há nota — e só para o que nunca tem nota.

    Fora dessa lista, sem nota é Ausente ou Não Dedutível, e este módulo não
    chuta."""
    return CATEGORIA_SEM_NOTA_ELETRONICA.get(_texto(tipo_despesa).lower(), "")


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
    """Valor virando número, venha ele da planilha ou do banco.

    O `Decimal` É O CASO DO BANCO, e ignorá-lo custou caro: a coluna `valor` da
    tabela de notas é NUMERIC, e o psycopg2 devolve NUMERIC como `Decimal` —
    que não é `int` nem `float`. Sem esta linha, `Decimal("269.00")` caía no
    caminho do texto brasileiro, onde o ponto é separador de milhar: virava
    **26.900**.

    O estrago não aparecia na tela. Ele aparecia como ponto que faltava: o
    valor NUNCA batia, e toda conciliação perdia os 25 pontos do valor exato.
    Achado em 12/09/2026 por um teste que esperava a SP de mesmo valor em
    primeiro lugar e recebeu a outra."""
    import decimal

    if isinstance(v, decimal.Decimal):
        return float(v)
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


# ---------------------------------------------------------------------------
# DE ONDE SAI CADA NÚMERO DO LANÇAMENTO — e por que isto é uma função
#
# ⚠️ DEFEITO ACHADO EM 13/09/2026, e ele estava calado desde a estreia da tela.
#
# A mesma SP pontuava **65% na janela de conferência e 35% na lista**. A causa:
# a base guarda cada valor DUAS VEZES — o texto que veio da planilha
# (`valor`, `vencimento`) e a versão já convertida (`valor_num`,
# `vencimento_d`). A LISTA da tela traz só as convertidas; a ficha completa traz
# as duas. A pontuação lia só as de texto.
#
# Resultado: na tela de verdade, TODA SP perdia os 25 pontos do valor e os 5 da
# data — 30 de 100. E o corte para propor é 60. Ou seja: **o sistema quase
# nunca propunha**, e as duas pilhas ("aprovar em lote o que é certo, decidir um
# a um o que tem dúvida") nunca chegaram a existir de verdade. Tudo caía na
# pilha da dúvida, e ninguém tinha como desconfiar — 35% parece um número
# legítimo.
#
# É primo do defeito do Decimal (12/09), e a lição é a mesma: **quando o mesmo
# dado tem duas formas, a leitura tem de aceitar as duas** — e num lugar só,
# senão a próxima leitura esquece de novo.
# ---------------------------------------------------------------------------
def valor_do_lancamento(lancamento: dict):
    """O valor da SP, venha ele convertido ou como texto da planilha."""
    bruto = lancamento.get("valor_num")
    if bruto is None or bruto == "":
        bruto = lancamento.get("valor")
    return _para_numero(bruto)


def datas_do_lancamento(lancamento: dict) -> list:
    """As datas que servem para conferir a emissão, com o nome de cada uma.

    Vencimento e pagamento, nesta ordem, cada uma na forma que existir."""
    saida = []
    for convertida, texto_, rotulo in (
            ("vencimento_d", "vencimento", "vencimento"),
            ("data_pagamento_d", "data_pagamento", "pagamento")):
        bruto = lancamento.get(convertida)
        if bruto is None or bruto == "":
            bruto = lancamento.get(texto_)
        data = _para_data(bruto)
        if data:
            saida.append((rotulo, data))
    return saida


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

    # 3. O valor. Ver `valor_do_lancamento`: ele vem em duas formas, e ler só
    #    uma delas foi o defeito de 13/09/2026.
    valor_sp = valor_do_lancamento(lancamento)
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
        for _rotulo, alvo in datas_do_lancamento(lancamento):
            if -DIAS_DEPOIS <= (alvo - emissao).days <= DIAS_ANTES:
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

    # ⚠️ NOTA CANCELADA NUNCA VEM PROPOSTA, por mais que ela combine.
    #
    # Pedido do dono em 13/09/2026: *"quando tiver a nota cancelada na tela de
    # associação, tem que deixar em vermelhinho o cancelado, pra a gente não
    # associar a uma nota cancelada sem perceber."* A cor resolve para quem
    # olha — e a marcação em lote existe justamente para quem NÃO olha linha a
    # linha. Uma cancelada pré-marcada entraria no "Confirmar as marcadas" sem
    # ninguém ver, que é o contrário do que ele pediu.
    #
    # Ela continua APARECENDO, e tem de aparecer: se aquela é mesmo a nota do
    # lançamento, quem analisa precisa saber que ela foi cancelada — é problema
    # fiscal, não informação a esconder. Só não vem decidida.
    if melhor.get("nota") and _texto(
            melhor["nota"].get("status")).upper() == "CANCELADA":
        melhor["propoe"] = False
        melhor["porques"] = melhor["porques"] + [
            "NÃO marquei esta: nota cancelada é decisão de gente"]
    return melhor


# ---------------------------------------------------------------------------
# AS CRÍTICAS: o que este módulo aponta, e o que ele PROPÕE
#
# O dono decidiu em 11/09/2026: PROPOR a correção, não só apontar. E disse por
# quê — "o humano às vezes esquece de visualizar; ele erra na categorização, em
# coisas até meio óbvias, de regras que a gente já definiu. É muito falho o
# olho humano."
#
# O QUE GOVERNA TUDO AQUI, e vem da correção dele no mesmo dia: **a existência
# da nota é o balizador**, não o tipo de despesa. Comprar material elétrico sem
# nota é não dedutível, e apontar isso como divergência seria ruído.
#
# DUAS PILHAS, E ELAS EXISTEM POR CAUSA DE UM RISCO REAL. Propor cria "fadiga
# de aprovação": se vinte e oito de trinta estão sempre certas, na terceira
# semana ninguém confere mais — é o mesmo olho cansado, só que mais rápido.
# Por isso o que tem dúvida NÃO vem marcado, e é decidido um a um.
# ---------------------------------------------------------------------------

# As categorias que dizem "não há documento". Achar a nota depois é justamente
# a correção que vale a pena — foi o que o dono descreveu: "colocado algo não
# dedutível de uma coisa que não foi localizada naquele momento, mas que depois
# ela surge".
DIZEM_QUE_NAO_HA_NOTA = {
    "Ausente", "Não Dedutível", "Reanalisar", "Emissão Futura",
    "Aguardando Nota (Ilegível)", "Aguardando Nota (Não Anexada)", "",
}

# As que afirmam que existe nota eletrônica. Se não há chave nem nota achada,
# alguém classificou sem documento.
EXIGEM_NOTA = {"NF-e (Mercadoria)", "NFS-e (Serviço)", "CT-e (Frete)",
               "NFC-e (Cupom Fiscal eletrônico)"}

# Os grupos em que a tela separa o que encontrou. A ordem é a da urgência.
CRITICO, CORRECAO, DUVIDA, SEM_PAR, EM_DIA = (
    "CRITICO", "CORRECAO", "DUVIDA", "SEM_PAR", "EM_DIA")


def avaliar(sp: dict, analise: dict, escolha: dict) -> dict:
    """O que fazer com esta SP, com o porquê escrito.

    `sp` é a linha da base; `analise` é o que já está gravado no diário (pode
    vir vazio); `escolha` é o que `melhor_nota` devolveu.

    Devolve {grupo, propoe, documentacao, chave, motivo, confianca}. `propoe`
    só é verdadeiro quando NÃO há dúvida — é o que decide em qual das duas
    pilhas a linha cai."""
    analise = analise or {}
    escolha = escolha or {}
    hoje = str(analise.get("documentacao") or "").strip()
    chave_no_card = so_digitos(analise.get("chave"))
    nota = escolha.get("nota")
    confianca = int(escolha.get("pontos") or 0)
    porques = list(escolha.get("porques") or [])

    def resposta(grupo, propoe, documentacao="", chave="", motivo=""):
        return {"grupo": grupo, "propoe": propoe,
                "documentacao": documentacao, "chave": chave,
                "motivo": motivo, "confianca": confianca}

    # 1. A NOTA JÁ ESTÁ NO CARD, E ESTÁ CANCELADA. É o mais grave da lista:
    #    despesa paga contra documento que não existe mais. Nunca é proposta —
    #    o que fazer é decisão de gente.
    if nota and chave_no_card and so_digitos(nota.get("chave")) == chave_no_card:
        if _texto(nota.get("status")).upper() == "CANCELADA":
            paga = _texto(sp.get("status_pgt")).lower() == "pago"
            return resposta(CRITICO, False, motivo=(
                "a nota deste card está CANCELADA"
                + (" e a despesa já foi paga" if paga else "")
                + ". Precisa de decisão, não de correção automática."))
        return resposta(EM_DIA, False, motivo="a nota do card confere")

    # 2. A CHAVE DO CARD NÃO É DESTA SP. Valor ou emitente divergentes é o
    #    sintoma clássico da troca de anexo entre lançamentos, que o dono
    #    descreveu: "colocar uma nota de um registro para outro".
    if chave_no_card and nota and so_digitos(nota.get("chave")) != chave_no_card:
        return resposta(DUVIDA, False, motivo=(
            "a chave que está no card não parece ser desta SP; achei outra "
            "nota que combina melhor. Confira se as notas não foram trocadas "
            "entre dois lançamentos."))

    # 3. ACHEI A NOTA, E O CARD DIZ QUE NÃO HÁ NOTA. É A CORREÇÃO QUE VALE.
    #    A categoria sai de DENTRO da chave, então não é palpite.
    if nota and hoje in DIZEM_QUE_NAO_HA_NOTA:
        categoria = categoria_da_chave(nota.get("chave")) or "NF-e (Mercadoria)"
        antes = f'está como "{hoje}"' if hoje else "está sem categoria"
        motivo = (f"{antes}, mas a nota foi encontrada — "
                  + "; ".join(porques) + ".")
        if escolha.get("propoe"):
            return resposta(CORRECAO, True, categoria,
                            so_digitos(nota.get("chave")), motivo)
        return resposta(DUVIDA, False, categoria,
                        so_digitos(nota.get("chave")),
                        motivo + " Não tenho certeza suficiente para propor.")

    # 4. O CARD AFIRMA QUE HÁ NOTA, E NÃO HÁ NENHUMA. Classificado sem
    #    documento — ou a nota ainda não chegou no relatório do FSist.
    if hoje in EXIGEM_NOTA and not chave_no_card and not nota:
        return resposta(DUVIDA, False, motivo=(
            f'está como "{hoje}", mas não há chave no card e não encontrei '
            "nota que combine. Pode ser nota que ainda não veio no relatório."))

    # 5. O CARD TEM CHAVE E A NOTA NÃO APARECEU NO RELATÓRIO DO FSIST. Ainda
    #    dá para conferir alguma coisa sem o relatório: o CNPJ de quem emitiu
    #    está DENTRO da chave. Se ele não é o credor desta SP, a chave é de
    #    outro lançamento — a troca de anexo outra vez, e desta vez detectada
    #    sem depender de achar a nota certa.
    if chave_no_card and not nota:
        emitente = emitente_da_chave(chave_no_card)
        if emitente and not mesmo_documento(sp.get("documento"), emitente):
            return resposta(DUVIDA, False, motivo=(
                "a chave que está no card foi emitida por outro CNPJ, e não "
                "pelo credor desta SP. Confira se as notas não foram trocadas "
                "entre dois lançamentos."))
        return resposta(EM_DIA, False, motivo=(
            "o card já tem a chave; a nota não veio no relatório do FSist."))

    # 6. NÃO ACHEI NADA, E O CARD TAMBÉM NÃO DIZ NADA. Aqui — e só aqui — o
    #    tipo de despesa ajuda, e apenas para o que nunca tem nota eletrônica.
    if not nota and not chave_no_card:
        sugerida = categoria_sugerida(sp.get("tipo_despesa"))
        if sugerida and hoje != sugerida:
            return resposta(DUVIDA, False, sugerida, "", (
                f'não há nota eletrônica para "{sp.get("tipo_despesa")}" — '
                f'o documento costuma ser "{sugerida}".'))
        return resposta(SEM_PAR, False, motivo=(
            "procurei e não encontrei nota que combine com esta SP."))

    return resposta(EM_DIA, False, motivo="nada a apontar")


def notas_sem_lancamento(notas: list, chaves_usadas: set) -> list:
    """As notas emitidas contra a BWS que não estão em lançamento nenhum.

    É A SEGUNDA VISÃO, e é ela que fecha com a contabilidade. Nas palavras do
    dono: *"se tem uma nota emitida, tem uma despesa para estar associada"*.
    Nota órfã é problema fiscal, e hoje ninguém a enxerga.

    As canceladas ficam de fora: nota cancelada sem despesa é o esperado, não
    um achado."""
    usadas = {so_digitos(c) for c in (chaves_usadas or set()) if c}
    saida = []
    for nota in notas:
        chave = so_digitos(nota.get("chave"))
        if not chave or chave in usadas:
            continue
        if _texto(nota.get("status")).upper() == "CANCELADA":
            continue
        saida.append(nota)
    return saida


# ---------------------------------------------------------------------------
# A CONCILIAÇÃO INTEIRA, ligada ao banco
#
# O CUIDADO QUE GOVERNA ESTA PARTE É DESEMPENHO, e não é preciosismo: são 59
# mil SPs e milhares de notas, e o banco tem UM DÉCIMO DE UM NÚCLEO. Pontuar
# toda SP contra toda nota seriam centenas de milhões de comparações — a tela
# nunca abriria.
#
# O que evita isso: a nota candidata de um lançamento quase sempre foi emitida
# PELO CREDOR DELE. Então busca-se, para a página de SPs que está na tela, só
# as notas daqueles CNPJs (mais as do número de nota que já está no card). De
# centenas de milhões, cai para algumas dezenas por SP.
# ---------------------------------------------------------------------------
def _raiz(documento) -> str:
    """Os oito primeiros dígitos de um CNPJ — matriz e filial juntas.

    A nota sai da filial que entregou, e o cadastro do credor quase sempre tem
    a matriz. Buscar pelos catorze dígitos perderia justamente o caso comum;
    quem decide de fato é `mesmo_documento`, na pontuação."""
    digitos = so_digitos(documento)
    return digitos[:8] if len(digitos) == 14 else digitos


def notas_candidatas(lancamentos: list) -> dict:
    """As notas que PODEM ser de alguma destas SPs, agrupadas por raiz de CNPJ.

    Uma consulta só para a página inteira. Traz também as notas cujo número
    bate com o que já está no card — é o caso em que quem lançou digitou o
    número e o CNPJ do credor está errado no cadastro."""
    from .db import consultar

    raizes = {_raiz(l.get("documento")) for l in lancamentos}
    raizes = {r for r in raizes if len(r) >= 8}
    numeros = {so_digitos(l.get("nf")).lstrip("0") for l in lancamentos}
    numeros = {n for n in numeros if n}
    if not raizes and not numeros:
        return {}

    condicoes, params = [], []
    if raizes:
        condicoes.append("left(emitente_doc, 8) IN (%s)"
                         % ",".join(["?"] * len(raizes)))
        params.extend(sorted(raizes))
    if numeros:
        condicoes.append("ltrim(regexp_replace(numero, '\\D', '', 'g'), '0') IN (%s)"
                         % ",".join(["?"] * len(numeros)))
        params.extend(sorted(numeros))

    linhas = consultar(
        "SELECT chave, emissao, numero, serie, tipo, valor, status, "
        "       emitente_doc, emitente, destinatario_doc, destinatario "
        "  FROM analisesps.notas_fiscais "
        " WHERE " + " OR ".join(condicoes), tuple(params))

    nomes = ["chave", "emissao", "numero", "serie", "tipo", "valor", "status",
             "emitente_doc", "emitente", "destinatario_doc", "destinatario"]
    por_raiz: dict = {}
    for linha in linhas:
        nota = dict(zip(nomes, linha))
        # Indexa pela raiz do emitente E pelo número: as duas portas de busca.
        chaves = {_raiz(nota["emitente_doc"] or emitente_da_chave(nota["chave"]))}
        numero = so_digitos(nota["numero"]).lstrip("0")
        if numero:
            chaves.add("n:" + numero)
        for k in chaves:
            if k:
                por_raiz.setdefault(k, []).append(nota)
    return por_raiz


def _para_esta_sp(lancamento: dict, por_raiz: dict) -> list:
    """As notas candidatas DESTA SP, sem repetir."""
    candidatas, vistas = [], set()
    numero = so_digitos(lancamento.get("nf")).lstrip("0")
    for k in (_raiz(lancamento.get("documento")), ("n:" + numero) if numero else ""):
        for nota in por_raiz.get(k, []) if k else []:
            if nota["chave"] not in vistas:
                vistas.add(nota["chave"])
                candidatas.append(nota)
    return candidatas


def analises_guardadas(ids: list) -> dict:
    """O diário: o que já se sabe sobre estas SPs.

    DUAS PORTAS, E AS DUAS VALEM. `sp_fiscal_analise` é o diário deste módulo
    (o que o card trazia quando a tela nasceu, mais toda decisão tomada aqui).
    `sp_fiscal` é o espelho do card que a planilha de apoio traz a cada carga.
    O diário manda quando existe, porque é mais novo; onde ele está vazio, vale
    o que está no card.

    POR QUE ISSO É IMPORTANTE E NÃO É DETALHE. Achado ao abrir a tela contra
    banco de verdade em 13/09/2026: os totalizadores contavam as duas portas
    (é SQL, em `consultas.SITUACOES_FISCAIS`) e a LISTA só olhava o diário. O
    resultado era o painel dizer "3 já categorizados" e a linha mostrar "—" na
    coluna "Está como" — duas afirmações contrárias na mesma tela, e nenhuma
    delas com jeito de errada. Agora as duas leem a mesma coisa."""
    from .db import consultar
    if not ids:
        return {}
    marcadores = ",".join(["?"] * len(ids))
    linhas = consultar(
        "SELECT s.sp_id, a.situacao, "
        "       coalesce(nullif(btrim(coalesce(a.documentacao, '')), ''), "
        "                btrim(coalesce(x.doc_fiscal, ''))) AS documentacao, "
        "       a.chave, a.numero_nota, a.dedutivel, a.origem, a.motivo, "
        "       a.confianca, a.decidida_por "
        "  FROM (SELECT sp_id FROM analisesps.sp_fiscal_analise "
        f"        WHERE sp_id IN ({marcadores}) "
        "        UNION "
        "        SELECT sp_id FROM analisesps.sp_fiscal "
        f"        WHERE sp_id IN ({marcadores})) s "
        "  LEFT JOIN analisesps.sp_fiscal_analise a ON a.sp_id = s.sp_id "
        "  LEFT JOIN analisesps.sp_fiscal x ON x.sp_id = s.sp_id",
        tuple(str(i) for i in ids) * 2)
    nomes = ["sp_id", "situacao", "documentacao", "chave", "numero_nota",
             "dedutivel", "origem", "motivo", "confianca", "decidida_por"]
    return {str(l[0]): dict(zip(nomes, l)) for l in linhas}


def conciliar(lancamentos: list) -> list:
    """Junta SP, nota e diário. Devolve o que a tela mostra, linha a linha.

    NÃO ESCREVE NADA. Separar o "decidir" do "gravar" é o que permite mostrar
    a proposta antes de ela virar fato — e é a diferença entre propor e
    adivinhar."""
    por_raiz = notas_candidatas(lancamentos)
    diario = analises_guardadas([l.get("id") for l in lancamentos])

    saida = []
    for sp in lancamentos:
        analise = diario.get(str(sp.get("id")), {})
        hoje = str(analise.get("documentacao") or "").strip()
        # O que não tem nota eletrônica para procurar não entra na conciliação
        # — herdado do script da planilha, que já acertava nisto. Procurar par
        # para uma apólice só produziria ruído e faria a pessoa desconfiar do
        # resto da tela.
        if hoje in NAO_CONCILIA:
            escolha = {"nota": None, "pontos": 0, "porques": [], "propoe": False}
        else:
            escolha = melhor_nota(sp, _para_esta_sp(sp, por_raiz))
        veredito = avaliar(sp, analise, escolha)
        saida.append({
            "sp": sp,
            "analise": analise,
            "nota": escolha.get("nota"),
            "segunda": escolha.get("segunda"),
            "porques": escolha.get("porques") or [],
            **veredito,
        })
    return saida


def contar_por_grupo(linhas: list) -> list:
    """Quantas em cada grupo, na ordem da urgência — os números do alto."""
    contagem: dict = {}
    for linha in linhas:
        contagem[linha["grupo"]] = contagem.get(linha["grupo"], 0) + 1
    return [(g, ROTULOS_GRUPO[g], contagem.get(g, 0)) for g in ORDEM_GRUPOS
            if contagem.get(g)]


ROTULOS_GRUPO = {
    CRITICO: "Precisa de decisão",
    CORRECAO: "Proposta de correção",
    DUVIDA: "Em dúvida",
    SEM_PAR: "Sem nota encontrada",
    EM_DIA: "Em dia",
}

# A ordem da urgência: o que pede ação primeiro, o que está certo por último.
ORDEM_GRUPOS = [CRITICO, CORRECAO, DUVIDA, SEM_PAR, EM_DIA]


# ---------------------------------------------------------------------------
# GRAVAR A DECISÃO — no diário, ainda NÃO no card
#
# Duas coisas diferentes, e a diferença é o que permite tentar de novo quando o
# Pipefy recusa: `decidida_em` é quando alguém escolheu; `escrita_em` é quando
# o card aceitou. Enquanto a segunda estiver vazia, a decisão está pendente de
# escrita e volta na próxima leva.
# ---------------------------------------------------------------------------
PENDENTE, PROPOSTA, CONFIRMADA, ESCRITA = (
    "PENDENTE", "PROPOSTA", "CONFIRMADA", "ESCRITA")

# Esperando a IA ler o anexo. A fila mora no BANCO, e não em memória: o
# processo separado pode ser reiniciado no meio, e quem escolheu trinta SPs
# não pode perder a escolha por causa disso.
NA_FILA_IA = "NA_FILA_IA"


def por_na_fila_da_ia(sp_ids: list, quem: str) -> int:
    """Marca as SPs escolhidas para a IA ler o anexo. Devolve quantas entraram.

    NÃO ATROPELA DECISÃO JÁ TOMADA: uma SP já confirmada ou já escrita no card
    fica como está. Quem quiser refazer a análise dela desfaz primeiro — e isso
    é de propósito, porque mandar a IA reescrever por cima do que uma pessoa
    decidiu é exatamente o contrário de "a IA propõe, nunca decide"."""
    from .db import conexao

    entraram = 0
    with conexao() as conn:
        for sp_id in sp_ids:
            cur = conn.execute(
                "INSERT INTO analisesps.sp_fiscal_analise "
                "  (sp_id, situacao, origem, decidida_por, decidida_em) "
                "VALUES (?, ?, 'IA', ?, now()) "
                "ON CONFLICT (sp_id) DO UPDATE SET "
                "  situacao = ?, decidida_por = EXCLUDED.decidida_por, "
                "  decidida_em = now() "
                " WHERE analisesps.sp_fiscal_analise.situacao "
                "       NOT IN (?, ?)",
                (str(sp_id), NA_FILA_IA, quem or "", NA_FILA_IA,
                 CONFIRMADA, ESCRITA))
            entraram += cur.rowcount or 0
            cur.close()
        conn.commit()
    return entraram


def guardar_decisao(sp_id: str, documentacao: str, chave: str, motivo: str,
                    confianca: int, quem: str, origem: str = "PESSOA") -> None:
    """Registra o que vai ser escrito no card. NÃO fala com o Pipefy."""
    from .db import conexao

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.sp_fiscal_analise "
            "  (sp_id, situacao, documentacao, chave, numero_nota, dedutivel, "
            "   confianca, origem, motivo, decidida_por, decidida_em) "
            "VALUES (?, ?, ?, ?, '', ?, ?, ?, ?, ?, now()) "
            "ON CONFLICT (sp_id) DO UPDATE SET "
            "  situacao = EXCLUDED.situacao, "
            "  documentacao = EXCLUDED.documentacao, chave = EXCLUDED.chave, "
            "  dedutivel = EXCLUDED.dedutivel, confianca = EXCLUDED.confianca, "
            "  origem = EXCLUDED.origem, motivo = EXCLUDED.motivo, "
            "  decidida_por = EXCLUDED.decidida_por, decidida_em = now(), "
            "  escrita_em = NULL, erro_escrita = ''",
            (str(sp_id), CONFIRMADA, documentacao, so_digitos(chave),
             dedutivel(documentacao), int(confianca or 0), origem,
             str(motivo or "")[:1000], quem or ""))
        conn.commit()


def a_escrever_no_card(limite: int = 200) -> list:
    """As decisões que ainda não chegaram ao card.

    `escrita_em` vazio é o que separa "decidido" de "gravado". Uma decisão que
    o Pipefy recusou continua aqui, e volta na próxima leva — nada se perde
    porque a API deu erro."""
    from .db import consultar
    linhas = consultar(
        "SELECT sp_id, documentacao, chave, motivo, erro_escrita "
        "  FROM analisesps.sp_fiscal_analise "
        " WHERE situacao = ? AND escrita_em IS NULL "
        " ORDER BY decidida_em LIMIT ?", (CONFIRMADA, int(limite)))
    return [{"sp_id": l[0], "documentacao": l[1], "chave": l[2],
             "motivo": l[3], "erro_anterior": l[4]} for l in linhas]


def marcar_escritas(cards_ok: list, falhas: dict) -> None:
    """Anota quem o card aceitou e quem recusou, com o motivo.

    QUEM FALHOU NÃO PERDE A DECISÃO: continua com `escrita_em` vazio e volta na
    próxima leva. O motivo fica gravado para a tela poder dizer por que aquela
    SP não foi — "o Pipefy não confirmou" é informação, "sumiu" não é."""
    from .db import conexao

    with conexao() as conn:
        for sp_id in cards_ok or []:
            conn.execute(
                "UPDATE analisesps.sp_fiscal_analise "
                "   SET situacao = ?, escrita_em = now(), erro_escrita = '' "
                " WHERE sp_id = ?", (ESCRITA, str(sp_id)))
        for sp_id, motivo in (falhas or {}).items():
            conn.execute(
                "UPDATE analisesps.sp_fiscal_analise "
                "   SET erro_escrita = ? WHERE sp_id = ?",
                (str(motivo)[:500], str(sp_id)))
        conn.commit()


def escrever_nos_cards(anotar=None, limite: int = 200) -> dict:
    """Leva as decisões confirmadas para os cards do Pipefy.

    RODA NO PROCESSO SEPARADO. São até duzentos cards por rodada, cada um
    falando com a API — dentro do worker isso seguraria uma das quatro threads
    do gunicorn por minutos.

    O NÚMERO DA SP **É** O NÚMERO DO CARD: a coluna A da planilha é o id do
    card do Pipefy, e é assim que o BeeVale já faz."""
    from . import pipefy

    anotar = anotar or (lambda *a, **k: None)
    pendentes = a_escrever_no_card(limite)
    if not pendentes:
        return {"escritas": 0, "falhas": 0, "pendentes": 0}

    anotar("gravando a análise fiscal nos cards", f"{len(pendentes)} card(s)")
    # O número da nota sai da chave que foi decidida — não de um campo à parte
    # que poderia discordar dela.
    atualizacoes = [{"card": p["sp_id"], "documentacao": p["documentacao"],
                     "chave": p["chave"], "numero": ""} for p in pendentes]
    resultado = pipefy.atualizar_documentacao_fiscal(atualizacoes)
    marcar_escritas(resultado.get("ok"), resultado.get("falhas"))

    escritas = len(resultado.get("ok") or [])
    falhas = len(resultado.get("falhas") or {})
    logger.info("Análise de SPs: análise fiscal gravada em %d card(s), "
                "%d recusado(s).", escritas, falhas)
    return {"escritas": escritas, "falhas": falhas,
            "pendentes": len(a_escrever_no_card(limite))}


# ---------------------------------------------------------------------------
# A SEGUNDA VISÃO, ligada ao banco: nota -> lançamento
#
# É ela que fecha com a contabilidade. Nas palavras do dono: *"se tem uma nota
# emitida, tem uma despesa para estar associada"*. Nota órfã é problema fiscal,
# e hoje ninguém a enxerga.
# ---------------------------------------------------------------------------
def notas_orfas(pagina: int = 1, por_pagina: int = 200) -> tuple:
    """As notas que não estão em lançamento nenhum, e quantas são ao todo.

    A CONTA É FEITA NO BANCO, com `NOT EXISTS`. Trazer as notas todas para
    Python e cruzar aqui seria carregar milhares de linhas na memória de uma
    instância que já morreu disso — e o `NOT EXISTS` usa o índice da chave.

    As CANCELADAS ficam de fora: nota cancelada sem despesa é o esperado, não
    um achado. Listá-las faria esta visão nascer cheia de ruído."""
    from .db import consultar, consultar_um

    onde = ("""
         WHERE upper(trim(coalesce(status, ''))) <> 'CANCELADA'
           AND NOT EXISTS (
               SELECT 1 FROM analisesps.sp_fiscal_analise a
                WHERE regexp_replace(coalesce(a.chave, ''), '\\D', '', 'g')
                      = notas_fiscais.chave)""")

    total = consultar_um(
        "SELECT count(*) FROM analisesps.notas_fiscais" + onde)
    pagina = max(1, int(pagina or 1))
    linhas = consultar(
        "SELECT chave, emissao, numero, valor, status, emitente_doc, emitente "
        "  FROM analisesps.notas_fiscais" + onde +
        " ORDER BY emissao DESC NULLS LAST, numero LIMIT ? OFFSET ?",
        (int(por_pagina), (pagina - 1) * int(por_pagina)))

    nomes = ["chave", "emissao", "numero", "valor", "status",
             "emitente_doc", "emitente"]
    notas = []
    for linha in linhas:
        nota = dict(zip(nomes, linha))
        # A categoria sai de DENTRO da chave — é certeza, não palpite. Saber
        # que aquela órfã é um CT-e já diz onde procurar a despesa.
        nota["categoria"] = categoria_da_chave(nota["chave"])
        notas.append(nota)
    return notas, (total[0] if total else 0)


# Quantas SPs de cada CNPJ são trazidas para depois escolher as melhores de
# cada nota. Cinco aparecem na tela; buscar 25 dá margem para o desempate por
# valor sem trazer a base inteira.
SPS_POR_CNPJ = 25

_RAIZ_SQL = r"left(regexp_replace(coalesce(documento, ''), '\D', '', 'g'), 8)"
_CAMPOS_SP = "id, credor, valor_num, vencimento_d, status_pgt, nf"
_NOMES_SP = ["id", "credor", "valor_num", "vencimento_d", "status_pgt", "nf"]


def sps_possiveis_das_notas(notas: list, quantas: int = 5) -> dict:
    """As SPs que PODEM ser de cada nota — o caminho inverso da conciliação.

    ⚠️ DUAS CONSULTAS PARA A PÁGINA INTEIRA, e não uma por nota. Esta busca
    nasceu dentro de um laço: 200 notas na tela viravam **200 consultas**, cada
    uma varrendo as 59 mil SPs por uma expressão sem índice.

    MEDIDO EM 13/09/2026, com 59.000 SPs e 4.000 notas: **28 segundos** só para
    montar as candidatas — e isso nesta máquina, muito mais rápida que o banco
    do Render (um décimo de um núcleo). Em produção a tela simplesmente não
    abria, e foi assim que o dono relatou: *"a tela por nota não abre"*.

    A primeira consulta pega as SPs de valor EXATAMENTE igual ao de alguma
    nota — é o caso que fecha o par, e o que se perderia em qualquer recorte
    por quantidade. A segunda pega as mais recentes de cada CNPJ, para haver o
    que mostrar quando o valor não bate."""
    from .db import consultar

    if not notas:
        return {}

    # Cada nota, com a raiz do CNPJ de quem emitiu e o valor dela.
    pedidos = []
    for nota in notas:
        emitente = nota.get("emitente_doc") or emitente_da_chave(nota.get("chave"))
        raiz = _raiz(emitente)
        if raiz:
            pedidos.append((so_digitos(nota.get("chave")), raiz,
                            _para_numero(nota.get("valor"))))
    if not pedidos:
        return {chave: [] for chave in
                (so_digitos(n.get("chave")) for n in notas)}

    raizes = sorted({r for _, r, _ in pedidos})
    marcas = ",".join(["?"] * len(raizes))
    por_raiz: dict = {}
    # A nota inteira, para a conferência de cada candidata poder olhar o número
    # e a data — e não só o valor.
    por_chave = {so_digitos(n.get("chave")): n for n in notas}

    def guardar(linha):
        sp = dict(zip(_NOMES_SP, linha[:len(_NOMES_SP)]))
        por_raiz.setdefault(linha[len(_NOMES_SP)], []).append(sp)

    # 1. As de valor igual ao de alguma nota desta página. São as que fecham o
    #    par, e é por elas que se começa a conferir.
    valores = sorted({v for _, _, v in pedidos if v is not None})
    if valores:
        for linha in consultar(
                f"SELECT {_CAMPOS_SP}, {_RAIZ_SQL} AS raiz "
                "  FROM analisesps.sps "
                f" WHERE {_RAIZ_SQL} IN ({marcas}) "
                f"   AND valor_num IN ({','.join(['?'] * len(valores))})",
                tuple(raizes) + tuple(valores)):
            guardar(linha)

    # 2. As mais recentes de cada CNPJ, para haver o que mostrar quando o valor
    #    não bate. `row_number()` faz o corte DENTRO do banco — trazer tudo e
    #    cortar em Python seria carregar a base na memória de uma instância que
    #    já morreu disso em julho.
    for linha in consultar(
            f"SELECT {_CAMPOS_SP}, raiz FROM ("
            f"  SELECT {_CAMPOS_SP}, {_RAIZ_SQL} AS raiz, "
            f"         row_number() OVER (PARTITION BY {_RAIZ_SQL} "
            "                             ORDER BY vencimento_d DESC NULLS LAST,"
            "                                      id) AS n"
            f"    FROM analisesps.sps WHERE {_RAIZ_SQL} IN ({marcas})) t "
            " WHERE n <= ?", tuple(raizes) + (int(SPS_POR_CNPJ),)):
        guardar(linha)

    # A escolha de cada nota: as de valor igual primeiro, sem repetir.
    #
    # ⚠️ E CADA CANDIDATA VAI COM O PORQUÊ ESCRITO. Cobrança do dono em
    # 13/09/2026: *"você bota aqui a nota e bota 'mesmo valor'. Mas gera dúvida:
    # você está comparando o mesmo valor de quê? Do mesmo fornecedor, do mesmo
    # número de nota fiscal? Como é que você chegou a essa informação? Era
    # interessante ampliar essa informação, mesmo que esse seja o critério, pelo
    # menos para dar segurança a quem está fazendo essa associação."*
    #
    # Ele está certo, e "mesmo valor" sozinho é pior que nada: dá ar de
    # conferência a uma coincidência. Duas notas do mesmo fornecedor no mesmo
    # mês com o mesmo valor existem — e é exatamente aí que se associa a errada.
    saida: dict = {}
    for chave, raiz, valor in pedidos:
        nota = por_chave.get(chave, {})
        vistas, escolhidas = set(), []
        candidatas = sorted(
            por_raiz.get(raiz, []),
            key=lambda sp: 0 if (valor is not None
                                 and _para_numero(sp.get("valor_num")) == valor)
            else 1)
        for sp in candidatas:
            if sp["id"] in vistas:
                continue
            vistas.add(sp["id"])
            escolhidas.append(dict(sp, **_porque_esta_candidata(sp, nota)))
            if len(escolhidas) >= quantas:
                break
        saida[chave] = escolhidas
    return saida


# Quanto o valor pode diferir e ainda ser "perto" — o mesmo teto da conciliação
# do outro lado. Dois números diferentes para a mesma ideia dariam telas que
# discordam.
def _porque_esta_candidata(sp: dict, nota: dict) -> dict:
    """Por que ESTA SP é candidata a ESTA nota, item por item.

    O que decide quem entra na lista é UM critério só — o CNPJ do emitente da
    nota é o do credor da SP. Os demais são conferência, e é deles que vem a
    segurança de quem clica em "associar": valor, número da nota digitado no
    card e data compatível."""
    valor_sp = valor_do_lancamento(sp)
    valor_nota = _para_numero(nota.get("valor"))
    diferenca = (abs(valor_sp - valor_nota)
                 if valor_sp is not None and valor_nota is not None else None)

    numero_nota = _texto(nota.get("numero"))
    bate_numero = mesmo_numero_de_nota(sp.get("nf"), numero_nota)

    emissao = _para_data(nota.get("emissao"))
    bate_data = False
    if emissao:
        for _rotulo, alvo in datas_do_lancamento(sp):
            if -DIAS_DEPOIS <= (alvo - emissao).days <= DIAS_ANTES:
                bate_data = True
                break

    razoes = [{
        "rotulo": "CNPJ do credor é o de quem emitiu",
        "bate": True,          # é por isso que ela está na lista
        "detalhe": _texto(sp.get("documento")) or _texto(nota.get("emitente_doc")),
    }]

    if diferenca is None:
        razoes.append({"rotulo": "Valor", "bate": False,
                       "detalhe": "não deu para comparar"})
    elif diferenca < 0.005:
        razoes.append({"rotulo": "Valor igual", "bate": True,
                       "detalhe": f"os dois de R$ {valor_nota:,.2f}"
                                  .replace(",", "X").replace(".", ",")
                                  .replace("X", ".")})
    else:
        razoes.append({
            "rotulo": "Valor DIFERENTE", "bate": False,
            "detalhe": (f"a SP é R$ {valor_sp:,.2f} e a nota R$ {valor_nota:,.2f}"
                        .replace(",", "X").replace(".", ",").replace("X", ".")),
        })

    razoes.append({
        "rotulo": "Nº da nota no card",
        "bate": bate_numero,
        "detalhe": (f"os dois {numero_nota}" if bate_numero
                    else (f"o card diz {_texto(sp.get('nf'))} e a nota é "
                          f"{numero_nota}" if _texto(sp.get("nf"))
                          else "o card está sem o nº da nota")),
    })
    razoes.append({
        "rotulo": "Data compatível",
        "bate": bate_data,
        "detalhe": ("emissão perto do vencimento/pagamento" if bate_data
                    else "a emissão não bate com as datas da SP"),
    })

    return {
        "razoes": razoes,
        "valor_igual": bool(diferenca is not None and diferenca < 0.005),
        "confere": sum(1 for r in razoes if r["bate"]),
        "de": len(razoes),
    }


def sps_possiveis_da_nota(nota: dict, quantas: int = 5) -> list:
    """Uma nota só. Passa pelo MESMO caminho da página inteira, para as duas
    não divergirem no dia em que a regra de escolha mudar."""
    return sps_possiveis_das_notas([nota], quantas).get(
        so_digitos(nota.get("chave")), [])
# ---------------------------------------------------------------------------
# OS NÚMEROS DO LADO DA NOTA
#
# Pedido do dono em 13/09/2026: *"onde é que eu vejo aqui como é que está a
# situação (…) pra saber o que que está faltando, onde é que eu tenho que
# focar"*. Do lado do lançamento os números saem de `consultas.painel_fiscal`;
# aqui saem os do lado da NOTA, que é a outra metade da mesma gestão.
#
# Uma consulta só, com `FILTER`. Ver o porquê em `consultas.painel_fiscal`.
# ---------------------------------------------------------------------------
def painel_notas() -> dict:
    """Quantas notas entraram, de onde vieram e quantas estão órfãs."""
    from .db import consultar_um

    sem_lancamento = (
        "upper(trim(coalesce(status, ''))) <> 'CANCELADA' "
        " AND NOT EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "                  WHERE regexp_replace(coalesce(a.chave, ''), "
        "                                       '\\D', '', 'g') "
        "                        = notas_fiscais.chave)")

    linha = consultar_um(
        "SELECT count(*), "
        "       count(*) FILTER (WHERE upper(trim(coalesce(status,''))) "
        "                              = 'CANCELADA'), "
        f"       count(*) FILTER (WHERE {sem_lancamento}), "
        # O CT-e É CONTADO PELA CHAVE, e não pela coluna `tipo`.
        #
        # Achado ao exercitar a tela contra banco de verdade em 13/09/2026: a
        # coluna vem preenchida de jeitos diferentes conforme a porta de
        # entrada — a Receita grava "CT-e", e o relatório do FSist grava o que
        # estiver escrito na coluna "Tipo" da planilha, que ninguém controla.
        # Contar por ela dava ZERO com CT-e na base.
        #
        # As posições 21 e 22 da chave são o modelo do documento, definição da
        # Receita: é a mesma certeza que `categoria_da_chave` usa, e vale para
        # toda nota, tenha vindo por onde tiver vindo.
        "       count(*) FILTER (WHERE substring(chave from 21 for 2) = '57'), "
        # ⚠️ O ALARME. Nota cancelada que JÁ está num lançamento — o fornecedor
        # cancelou depois de a gente associar. Não aparece na lista de órfãs
        # (ela não é órfã, está associada), e por isso ficava invisível
        # justamente por estar "resolvida".
        f"       count(*) FILTER (WHERE {SQL_CANCELADA_EM_USO}), "
        "       max(importada_em) "
        "  FROM analisesps.notas_fiscais")
    nomes = ["total", "canceladas", "sem_lancamento", "ctes",
             "canceladas_em_uso", "ultima"]
    if not linha:
        return {n: 0 for n in nomes}
    return dict(zip(nomes, linha))


# ---------------------------------------------------------------------------
# ESCREVER À MÃO — o que o sistema NÃO propôs
#
# Correção do dono em 13/09/2026: *"tudo aquilo que você sugeriu (…) mas o que
# você não sugeriu, como é que eu adiciono a informação? Porque a planilha ela
# me permite adicionar, e a tela não permite."*
#
# Ele está certo e o buraco era grande: a tela só sabia APROVAR proposta. Numa
# lista em que boa parte não tem proposta nenhuma — é justamente o trabalho que
# sobra —, não haver como digitar transforma a tela em relatório, e o trabalho
# volta para a planilha. É o contrário do que ela existe para fazer.
#
# A decisão à mão entra pelo MESMO caminho da decisão aprovada
# (`guardar_decisao`), com origem PESSOA: assim ela é gravada no card pela
# mesma leva, aparece no mesmo diário e conta nos mesmos totais. Um segundo
# caminho de gravação seria a chance de a tela e o card divergirem.
# ---------------------------------------------------------------------------
class ErroDeEntrada(ValueError):
    """Dado recusado, com a mensagem já pronta para a tela."""


def conferir_chave(chave: str, sp: dict = None) -> str:
    """Devolve a chave só com dígitos, ou recusa dizendo por quê.

    A CONFERÊNCIA É A PARTE ÚTIL. Uma chave digitada errada não dá erro: ela
    grava no card uma nota que não é a da despesa, e ninguém descobre — é
    exatamente o defeito que esta tela existe para achar. Então confere-se o
    que dá para conferir sozinho: o tamanho, o modelo do documento (que sai das
    posições 21 e 22) e, quando a SP é conhecida, o CNPJ de quem emitiu, que
    mora dentro da própria chave."""
    limpa = so_digitos(chave)
    if not limpa:
        return ""
    if len(limpa) != 44:
        raise ErroDeEntrada(
            f"a chave de acesso tem 44 números; esta tem {len(limpa)}.")
    if limpa[20:22] not in MODELOS:
        raise ErroDeEntrada(
            "esta chave não é de NF-e, NFC-e nem CT-e — confira se não faltou "
            "ou sobrou algum número.")
    if sp:
        emitente = emitente_da_chave(limpa)
        credor = so_digitos(sp.get("documento"))
        if emitente and len(credor) == 14 and not mesmo_documento(credor, emitente):
            raise ErroDeEntrada(
                "esta chave foi emitida por outro CNPJ, e não pelo credor "
                "desta SP. Confira se não é a nota de outro lançamento.")
    return limpa


def decidir_a_mao(sp_id: str, documentacao: str, chave: str, quem: str,
                  sp: dict = None) -> dict:
    """Grava a categoria (e a chave, quando houver) digitada por uma pessoa.

    Devolve o que ficou gravado, para a tela mostrar sem recarregar."""
    documentacao = str(documentacao or "").strip()
    if documentacao and documentacao not in CATEGORIAS:
        raise ErroDeEntrada(f'"{documentacao}" não é uma categoria conhecida.')

    limpa = conferir_chave(chave, sp)
    if not documentacao and not limpa:
        raise ErroDeEntrada("escolha a categoria ou informe a chave de acesso.")

    # CATEGORIA DEDUZIDA DA CHAVE quando a pessoa informou só a chave. É o
    # mesmo caminho da proposta automática: o modelo do documento está dentro
    # da chave, então não é palpite.
    if not documentacao:
        documentacao = categoria_da_chave(limpa) or "NF-e (Mercadoria)"

    motivo = "informado à mão" + (" com a chave conferida" if limpa else "")
    guardar_decisao(sp_id, documentacao, limpa, motivo, 100, quem,
                    origem="PESSOA")
    return {"sp_id": str(sp_id), "documentacao": documentacao, "chave": limpa,
            "dedutivel": dedutivel(documentacao)}


def uma_nota(chave: str) -> dict:
    """A nota guardada, pela chave. Vazio quando não existe aqui."""
    from .db import consultar_um
    limpa = so_digitos(chave)
    if len(limpa) != 44:
        return {}
    linha = consultar_um(
        "SELECT chave, emissao, numero, valor, status, emitente_doc, emitente "
        "  FROM analisesps.notas_fiscais WHERE chave = ?", (limpa,))
    if not linha:
        return {}
    nomes = ["chave", "emissao", "numero", "valor", "status",
             "emitente_doc", "emitente"]
    return dict(zip(nomes, linha))


# ---------------------------------------------------------------------------
# RECONFERIR — o que já está gravado continua sendo olhado
#
# Pergunta do dono em 13/09/2026: *"aquela varredura pra conferir se o que nós
# já temos está ok, como é que eu sei se isso está acontecendo? É toda vez que
# eu abro, é uma vez? E se eu quiser fazer uma reanálise das informações que a
# gente já gravou, já salvou? E se eu quiser selecionar um determinado registro
# e reprocessar ele pra ver se está batendo? E se o que tiver pra trás tiver
# coisa errada, como é que eu sei que isso está sendo analisado?"*
#
# A RESPOSTA HONESTA, e é por isso que esta parte existe:
#
#   1. A conferência roda A CADA ABERTURA da tela, sobre os lançamentos que
#      estão na página — e isso vale também para o que já foi decidido e já foi
#      gravado no card. Nada é "conferido uma vez e esquecido". Só que a tela
#      não dizia isso em lugar nenhum, e o que não é dito não existe para quem
#      usa.
#   2. O que está FORA da página não era reconferido enquanto ninguém chegasse
#      nela. Os três sintomas mais graves de erro antigo — nota cancelada,
#      categoria que afirma nota sem haver chave, e chave de outro CNPJ — são
#      varridos sobre a BASE INTEIRA pelo recorte "Provavelmente errado"
#      (`consultas.SQL_FISCAL_PROVAVEL_ERRO`), que é SQL. Esse é o número que
#      responde "tem coisa errada para trás?".
#   3. FALTAVA reconferir SOB DEMANDA um registro escolhido. É o que entra
#      aqui.
#
# O QUE `reconferir` FAZ DE DIFERENTE da conciliação da tela: ela procura nota
# MESMO para as categorias que normalmente não se concilia (apólice, contrato,
# guia de tributo). Na lista isso seria ruído — procurar nota de aluguel todo
# dia; pedido registro a registro, é exatamente o que se quer, porque a pergunta
# passa a ser "será que classificaram errado?". NÃO GRAVA NADA: devolve o que
# encontrou, e quem decide continua sendo gente.
# ---------------------------------------------------------------------------
def reconferir(sps: list) -> list:
    """Refaz a conferência destas SPs agora, inclusive as já decididas.

    `sps` são as linhas da base (o que `consultas.listar`/`uma` devolvem)."""
    por_raiz = notas_candidatas(sps)
    diario = analises_guardadas([s.get("id") for s in sps])

    saida = []
    for sp in sps:
        analise = diario.get(str(sp.get("id")), {})
        # SEM O ATALHO do `NAO_CONCILIA`: aqui a pergunta é justamente se a
        # categoria que está lá é a certa.
        escolha = melhor_nota(sp, _para_esta_sp(sp, por_raiz))
        veredito = avaliar(sp, analise, escolha)
        saida.append({
            "sp": sp, "analise": analise, "nota": escolha.get("nota"),
            "porques": escolha.get("porques") or [], **veredito,
        })
    return saida


def resumo_da_reconferencia(linhas: list) -> dict:
    """Uma frase por SP, para a tela dizer o que mudou sem recarregar tudo."""
    saida = []
    for l in linhas:
        antes = str((l.get("analise") or {}).get("documentacao") or "").strip()
        achou = l.get("nota") or {}
        saida.append({
            "sp": str(l["sp"].get("id")),
            "grupo": l.get("grupo"),
            "rotulo": ROTULOS_GRUPO.get(l.get("grupo"), l.get("grupo")),
            "antes": antes or "sem categoria",
            "proposta": l.get("documentacao") or "",
            "chave": l.get("chave") or "",
            "confianca": int(l.get("confianca") or 0),
            "motivo": l.get("motivo") or "",
            "nota": ({"numero": achou.get("numero"),
                      "emitente": achou.get("emitente"),
                      "status": achou.get("status")} if achou else None),
            # O QUE MUDA A VIDA DE QUEM LÊ: mudou ou continua igual? Sem esta
            # linha, reconferir trinta registros devolveria trinta parágrafos
            # e nenhuma conclusão.
            "mudou": bool(l.get("documentacao")) and l.get("documentacao") != antes,
        })
    return {"itens": saida,
            "mudaram": sum(1 for i in saida if i["mudou"]),
            "apontados": sum(1 for i in saida
                             if i["grupo"] in (CRITICO, CORRECAO, DUVIDA))}


# ---------------------------------------------------------------------------
# A PROVA — os dois lados abertos, campo a campo
#
# Cobrança do dono em 13/09/2026, e ela derruba o desenho anterior: *"você
# sugere e eu quero ver de forma completa os dados do que você está sugerindo.
# Os dados do relatório FSist. Como faço? Ou quero ver os dados do registro,
# não dá pra ver pra validar. Isso pra eu ter que confiar somente no que você
# observou."*
#
# Ele está certo. A tela mostrava a CONCLUSÃO ("nota 1430 · FORNECEDOR") e
# escondia o que a sustenta. Numa tela cujo trabalho é achar erro, pedir
# confiança cega é o pior arranjo possível: quem confere sem poder ver vira
# carimbo, e carimbo não acha nada.
#
# ENTÃO AQUI SAI TUDO: a SP inteira, a nota inteira, a conta dos pontos regra a
# regra (inclusive as que NÃO pontuaram, que são as que explicam por que a
# confiança não foi maior), e TODAS as candidatas consideradas — não só a
# vencedora. Ver a segunda colocada é o que permite discordar da escolha.
# ---------------------------------------------------------------------------

# Como cada regra da pontuação aparece na tela, e quanto ela vale. A ordem é a
# do peso: quem lê quer saber primeiro o que mais decidiu.
REGRAS_DA_PONTUACAO = [
    ("emitente", "CNPJ do credor é o de quem emitiu", PONTOS_EMITENTE),
    ("numero", "Nº da nota digitado no card bate", PONTOS_NUMERO),
    ("valor", "Valor", PONTOS_VALOR_EXATO),
    ("nome", "Nome do credor parecido com o do emitente", PONTOS_NOME),
    ("data", "Data de emissão compatível", PONTOS_DATA),
]


def _conferir_regras(lancamento: dict, nota: dict) -> list:
    """Regra a regra: bateu, não bateu, e com que número de cada lado.

    O QUE NÃO BATEU É O MAIS IMPORTANTE AQUI. "Confiança 35%" não diz nada;
    "o valor difere em R$ 12,00 e o nº da nota do card está vazio" diz onde
    olhar."""
    saida = []

    emitente = nota.get("emitente_doc") or emitente_da_chave(nota.get("chave"))
    saida.append({
        "chave": "emitente", "rotulo": "CNPJ do credor é o de quem emitiu",
        "bateu": mesmo_documento(lancamento.get("documento"), emitente),
        "pontos": PONTOS_EMITENTE,
        "no_lancamento": _texto(lancamento.get("documento")),
        "na_nota": _texto(emitente),
    })

    saida.append({
        "chave": "numero", "rotulo": "Nº da nota digitado no card bate",
        "bateu": mesmo_numero_de_nota(lancamento.get("nf"), nota.get("numero")),
        "pontos": PONTOS_NUMERO,
        "no_lancamento": _texto(lancamento.get("nf")) or "(vazio)",
        "na_nota": _texto(nota.get("numero")),
    })

    valor_sp = valor_do_lancamento(lancamento)
    valor_nota = _para_numero(nota.get("valor"))
    diferenca = (abs(valor_sp - valor_nota)
                 if valor_sp is not None and valor_nota is not None else None)
    saida.append({
        "chave": "valor", "rotulo": "Valor",
        "bateu": diferenca is not None and diferenca < 0.005,
        "quase": diferenca is not None and 0.005 <= diferenca <= TOLERANCIA_VALOR,
        "pontos": PONTOS_VALOR_EXATO,
        "no_lancamento": valor_sp, "na_nota": valor_nota,
        "diferenca": diferenca,
    })

    saida.append({
        "chave": "nome", "rotulo": "Nome do credor parecido com o do emitente",
        "bateu": _nome_parecido(lancamento.get("credor"), nota.get("emitente")),
        "pontos": PONTOS_NOME,
        "no_lancamento": _texto(lancamento.get("credor")),
        "na_nota": _texto(nota.get("emitente")),
    })

    emissao = _para_data(nota.get("emissao"))
    bateu_data, quando = False, ""
    if emissao:
        for rotulo, alvo in datas_do_lancamento(lancamento):
            if -DIAS_DEPOIS <= (alvo - emissao).days <= DIAS_ANTES:
                from .formatos import data_br
                bateu_data, quando = True, f"{rotulo} em {data_br(alvo)}"
                break
    saida.append({
        "chave": "data", "rotulo": "Data de emissão compatível",
        "bateu": bateu_data, "pontos": PONTOS_DATA,
        "no_lancamento": quando or "(sem data compatível)",
        "na_nota": _escrever("emissao", emissao),
    })

    return saida


# Os campos da nota que a tela mostra, na ordem em que se confere um documento
# fiscal. TODOS, e de propósito: o pedido foi ver "de forma completa".
CAMPOS_DA_NOTA = [
    ("numero", "Número"), ("serie", "Série"), ("emissao", "Emissão"),
    ("valor", "Valor"), ("status", "Situação"), ("tipo", "Tipo"),
    ("emitente", "Emitente"), ("emitente_doc", "CNPJ do emitente"),
    ("emitente_uf", "UF"), ("destinatario", "Destinatário"),
    ("destinatario_doc", "CNPJ do destinatário"),
    ("chaves_nfe", "NF-e dentro deste CT-e"), ("chave", "Chave de acesso"),
    ("importada_em", "Entrou aqui em"),
]


def nota_completa(chave: str) -> dict:
    """A nota inteira, como ela está guardada. Sem recorte."""
    from .db import consultar_um
    limpa = so_digitos(chave)
    if len(limpa) != 44:
        return {}
    campos = [c for c, _ in CAMPOS_DA_NOTA]
    linha = consultar_um(
        f"SELECT {', '.join(campos)} FROM analisesps.notas_fiscais "
        " WHERE chave = ?", (limpa,))
    return dict(zip(campos, linha)) if linha else {}


# Como cada campo da nota é escrito para gente ler. Um documento fiscal com
# data 2026-09-01 e valor 269.00 numa tela em português é erro de leitura
# esperando acontecer — e esta é exatamente a tela em que ele vai comparar
# número com número.
_COMO_ESCREVER = {
    "emissao": "data", "valor": "dinheiro", "importada_em": "momento",
}


def _escrever(campo: str, valor) -> str:
    if valor is None or valor == "":
        return ""
    from .formatos import data_br, moeda, momento_br
    jeito = _COMO_ESCREVER.get(campo)
    if jeito == "data":
        return data_br(valor)
    if jeito == "dinheiro":
        return moeda(valor)
    if jeito == "momento":
        return momento_br(valor)
    return str(valor)


def _nota_para_a_tela(nota: dict) -> dict:
    """A nota vira texto, campo a campo, com o rótulo em português."""
    completa = nota_completa(nota.get("chave")) or dict(nota)
    return {
        "chave": so_digitos(completa.get("chave") or nota.get("chave")),
        "campos": [{"rotulo": rotulo,
                    "valor": _escrever(campo, completa.get(campo))}
                   for campo, rotulo in CAMPOS_DA_NOTA],
    }


def comparar(lancamento: dict) -> dict:
    """Tudo o que sustenta (ou derruba) a proposta desta SP.

    NÃO É RESUMO: é a SP inteira, todas as candidatas com a conta dos pontos
    aberta regra a regra, e o que já está gravado no diário. Quem confere
    precisa poder discordar com o dado na mão."""
    por_raiz = notas_candidatas([lancamento])
    candidatas = _para_esta_sp(lancamento, por_raiz)
    diario = analises_guardadas([lancamento.get("id")]).get(
        str(lancamento.get("id")), {})

    avaliadas = []
    for nota in candidatas:
        pontos, porques = pontuar(lancamento, nota)
        avaliadas.append({
            "pontos": pontos,
            "porques": porques,
            "propoe": pontos >= CONFIANCA_PARA_PROPOR,
            "regras": _conferir_regras(lancamento, nota),
            "categoria_pela_chave": categoria_da_chave(nota.get("chave")),
            **_nota_para_a_tela(nota),
        })
    avaliadas.sort(key=lambda x: -x["pontos"])

    escolha = melhor_nota(lancamento, candidatas)
    veredito = avaliar(lancamento, diario, escolha)

    return {
        "sp": str(lancamento.get("id")),
        "diario": {
            "documentacao": _texto(diario.get("documentacao")),
            "chave": so_digitos(diario.get("chave")),
            "origem": _texto(diario.get("origem")),
            "situacao": _texto(diario.get("situacao")),
            "por": _texto(diario.get("decidida_por")),
            "motivo": _texto(diario.get("motivo")),
        },
        "veredito": {
            "grupo": veredito.get("grupo"),
            "rotulo": ROTULOS_GRUPO.get(veredito.get("grupo"), ""),
            "proposta": veredito.get("documentacao") or "",
            "chave": veredito.get("chave") or "",
            "confianca": veredito.get("confianca"),
            "motivo": veredito.get("motivo"),
            "propoe": bool(veredito.get("propoe")),
        },
        "candidatas": avaliadas,
        # QUANTAS FORAM OLHADAS, mesmo que nenhuma tenha servido. "Não achei" e
        # "não procurei" são coisas diferentes, e só este número separa as duas.
        "olhadas": len(candidatas),
        "corte": CONFIANCA_PARA_PROPOR,
    }


# ---------------------------------------------------------------------------
# A LISTA DE TODAS AS NOTAS — o que existe, não só o que está órfão
#
# Pedido do dono em 13/09/2026: *"não consigo visualizar numa tela o que temos
# de notas e o que não temos. Ver as notas do dia ou consultar as notas, ver as
# informações do que foi emitido contra a BWS, conforme vemos no FSist e no
# relatório que baixamos e como víamos na planilha."*
#
# A tela só tinha a lista das ÓRFÃS — as notas sem lançamento. É um recorte
# útil e é só um recorte: quem quer conferir "chegou a nota da semana?" ou
# "quantas vieram hoje?" não tinha onde olhar. Agora a lista é de tudo, e as
# órfãs viram um filtro dela.
# ---------------------------------------------------------------------------
NOTAS_POR_PAGINA = 200

SEM_LANCAMENTO_SQL = (
    "NOT EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
    "             WHERE regexp_replace(coalesce(a.chave, ''), '\\D', '', 'g') "
    "                   = notas_fiscais.chave)")

# Os recortes da lista de notas. Mesma ideia da tela de lançamentos: o nome do
# grupo diz de que dado se está falando.
# O CNPJ DE QUEM EMITIU, com a coluna e, se ela vier vazia, a chave. Os dígitos
# 7 a 20 são o emitente por definição da Receita — mais confiável que a coluna,
# que chega com formatação variada.
EMITENTE_DA_NOTA = ("coalesce(nullif(btrim(coalesce(emitente_doc, '')), ''), "
                    "         substring(chave from 7 for 14))")

# EXISTE UMA SP DO MESMO CNPJ COM O MESMO VALOR? É o par que se fecha sem
# pensar — e o dono pediu o filtro em 13/09/2026: *"nessas que estão aqui já
# verdinha pra associar (…) tem outra sim, o valor é diferente. Tem que ter um
# tratamento aí."*
SQL_TEM_SP_DE_VALOR_IGUAL = f"""
EXISTS (SELECT 1 FROM analisesps.sps s
         WHERE left(regexp_replace(coalesce(s.documento, ''), '\\D', '', 'g'), 8)
               = left({EMITENTE_DA_NOTA}, 8)
           AND s.valor_num = notas_fiscais.valor)"""

SQL_TEM_SP_DO_CREDOR = f"""
EXISTS (SELECT 1 FROM analisesps.sps s
         WHERE left(regexp_replace(coalesce(s.documento, ''), '\\D', '', 'g'), 8)
               = left({EMITENTE_DA_NOTA}, 8))"""

# ⚠️ A NOTA CANCELADA QUE JÁ ESTÁ NUM LANÇAMENTO. É o caso que o dono descreveu
# em 13/09/2026, e é o mais grave desta tela: *"imagina, o fornecedor emitiu e
# cancelou a nota. E a gente associou, pagou, e a nota virou cancelada. A gente
# tem que ter um local de visualização disso, facilmente poder tratar isso aí,
# ligar pro fornecedor e pedir uma nova nota (…) é algo que tem que dar
# destaque."*
#
# Ele está certo: despesa paga contra documento que não existe mais é problema
# fiscal, e não se descobre olhando a lista de órfãs — a nota cancelada NÃO é
# órfã, ela está associada. Ficava invisível justamente por estar resolvida.
SQL_CANCELADA_EM_USO = (
    "upper(btrim(coalesce(status, ''))) = 'CANCELADA' "
    " AND EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
    "              WHERE regexp_replace(coalesce(a.chave, ''), '\\D', '', 'g') "
    "                    = notas_fiscais.chave)")

GRUPOS_DE_NOTA = [
    ("O lançamento", [
        ("sem_lancamento", "Sem lançamento",
         "nota emitida que não está em SP nenhuma"),
        ("com_lancamento", "Já está num lançamento",
         "a chave está gravada em alguma SP"),
    ]),
    ("O par com a SP", [
        ("casa_valor", "Tem SP do mesmo valor",
         "existe SP do mesmo CNPJ com o valor idêntico — é o par que fecha"),
        ("sem_valor_igual", "Tem SP do credor, mas de outro valor",
         "há SP daquele CNPJ e nenhuma com o mesmo valor; precisa de olho"),
        ("sem_sp_do_credor", "Nenhuma SP daquele CNPJ",
         "a despesa pode não ter sido lançada ainda"),
    ]),
    ("A situação na Receita", [
        ("autorizada", "Autorizada", ""),
        ("cancelada", "Cancelada",
         "documento que não existe mais; pagar contra ele é problema fiscal"),
        ("cancelada_em_uso", "⚠️ Cancelada E já associada a uma SP",
         "o fornecedor cancelou depois de a gente associar — pedir nota nova"),
    ]),
    ("O tipo de documento", [
        ("nfe", "NF-e (mercadoria)", "modelo 55, lido de dentro da chave"),
        ("cte", "CT-e (frete)", "modelo 57"),
        ("nfce", "NFC-e (cupom)", "modelo 65"),
    ]),
]

RECORTES_DE_NOTA = {
    "sem_lancamento": SEM_LANCAMENTO_SQL,
    "com_lancamento": "NOT (" + SEM_LANCAMENTO_SQL + ")",
    "casa_valor": SQL_TEM_SP_DE_VALOR_IGUAL,
    "sem_valor_igual": (SQL_TEM_SP_DO_CREDOR + " AND NOT ("
                        + SQL_TEM_SP_DE_VALOR_IGUAL + ")"),
    "sem_sp_do_credor": "NOT (" + SQL_TEM_SP_DO_CREDOR + ")",
    "cancelada_em_uso": SQL_CANCELADA_EM_USO,
    "autorizada": "upper(trim(coalesce(status,''))) <> 'CANCELADA'",
    "cancelada": "upper(trim(coalesce(status,''))) = 'CANCELADA'",
    "nfe": "substring(chave from 21 for 2) = '55'",
    "cte": "substring(chave from 21 for 2) = '57'",
    "nfce": "substring(chave from 21 for 2) = '65'",
}

FRASE_DA_NOTA = {
    "sem_lancamento": "não está em lançamento nenhum",
    "com_lancamento": "já está num lançamento",
    "casa_valor": "tem SP do mesmo CNPJ e do mesmo valor",
    "sem_valor_igual": "tem SP do credor, mas nenhuma do mesmo valor",
    "sem_sp_do_credor": "não tem nenhuma SP daquele CNPJ",
    "cancelada_em_uso": "está cancelada E já associada a uma SP",
    "autorizada": "está autorizada",
    "cancelada": "está cancelada",
    "nfe": "é NF-e (mercadoria)",
    "cte": "é CT-e (frete)",
    "nfce": "é NFC-e (cupom)",
}


def _where_das_notas(filtros: dict) -> tuple:
    """Traduz os recortes da tela em SQL. Tudo entra como parâmetro."""
    onde, params = [], []

    for chave in (filtros.get("recortes") or []):
        if chave in RECORTES_DE_NOTA:
            onde.append("(" + RECORTES_DE_NOTA[chave] + ")")

    busca = str(filtros.get("busca") or "").strip()
    if busca:
        # Número, chave, CNPJ ou nome do emitente — os quatro jeitos de
        # procurar uma nota que alguém tem na mão.
        alvo = ("lower(coalesce(numero,'') || ' ' || coalesce(chave,'') || ' ' "
                "|| coalesce(emitente_doc,'') || ' ' || coalesce(emitente,''))")
        for termo in [t.strip().lower() for t in busca.split(",") if t.strip()]:
            onde.append(f"{alvo} LIKE ?")
            params.append("%" + termo.replace("\\", "\\\\")
                          .replace("%", r"\%").replace("_", r"\_") + "%")

    for campo, operador in (("emissao_ini", ">="), ("emissao_fim", "<=")):
        valor = filtros.get(campo)
        if valor:
            onde.append(f"emissao {operador} ?")
            params.append(valor)

    return (" WHERE " + " AND ".join(onde)) if onde else "", params


def listar_notas(filtros: dict, pagina: int = 1) -> tuple:
    """Uma página de notas, com o total e a soma. Três perguntas, uma varredura."""
    from .db import consultar, consultar_um

    where, params = _where_das_notas(filtros or {})
    resumo = consultar_um(
        "SELECT count(*), coalesce(sum(valor), 0), "
        f"       count(*) FILTER (WHERE {SEM_LANCAMENTO_SQL}) "
        f"  FROM analisesps.notas_fiscais{where}", tuple(params))

    pagina = max(1, int(pagina or 1))
    linhas = consultar(
        "SELECT chave, emissao, numero, serie, valor, status, emitente_doc, "
        "       emitente, emitente_uf, importada_em, "
        f"       {SEM_LANCAMENTO_SQL} AS orfa, "
        # EM QUAL SP ELA ESTÁ. Dizer só "já está num lançamento" é meio
        # caminho: na nota cancelada, saber QUAL SP é o que permite agir — ver
        # se foi paga e ligar para o fornecedor. Sem o número, ele teria de
        # procurar a chave na outra tela.
        "       (SELECT a.sp_id FROM analisesps.sp_fiscal_analise a "
        "         WHERE regexp_replace(coalesce(a.chave, ''), '\\D', '', 'g') "
        "               = notas_fiscais.chave LIMIT 1) AS sp_id, "
        "       (SELECT s.status_pgt FROM analisesps.sp_fiscal_analise a "
        "          JOIN analisesps.sps s ON s.id = a.sp_id "
        "         WHERE regexp_replace(coalesce(a.chave, ''), '\\D', '', 'g') "
        "               = notas_fiscais.chave LIMIT 1) AS sp_status "
        f"  FROM analisesps.notas_fiscais{where} "
        " ORDER BY emissao DESC NULLS LAST, numero DESC "
        " LIMIT ? OFFSET ?",
        tuple(params) + (NOTAS_POR_PAGINA, (pagina - 1) * NOTAS_POR_PAGINA))

    nomes = ["chave", "emissao", "numero", "serie", "valor", "status",
             "emitente_doc", "emitente", "emitente_uf", "importada_em", "orfa",
             "sp_id", "sp_status"]
    notas = []
    for linha in linhas:
        nota = dict(zip(nomes, linha))
        nota["categoria"] = categoria_da_chave(nota["chave"])
        notas.append(nota)

    return notas, {
        "quantidade": resumo[0] if resumo else 0,
        "total": resumo[1] if resumo else 0,
        "sem_lancamento": resumo[2] if resumo else 0,
    }


def notas_por_dia(dias: int = 14) -> list:
    """Quantas notas entraram por dia de emissão, nos últimos dias.

    Responde à pergunta que ele fez primeiro — *"ver as notas do dia"* — sem
    obrigar a filtrar: o número de ontem ao lado do de hoje já diz se a busca
    está trazendo coisa ou parou."""
    from .db import consultar

    linhas = consultar(
        "SELECT emissao, count(*), coalesce(sum(valor), 0) "
        "  FROM analisesps.notas_fiscais "
        " WHERE emissao IS NOT NULL "
        "   AND emissao >= (now() AT TIME ZONE 'America/Sao_Paulo')::date "
        "                   - make_interval(days => ?) "
        " GROUP BY emissao ORDER BY emissao DESC", (int(dias),))
    return [{"dia": l[0], "quantas": l[1], "total": l[2]} for l in linhas]
