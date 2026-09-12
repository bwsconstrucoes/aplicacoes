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
    """O diário: o que já foi decidido sobre estas SPs."""
    from .db import consultar
    if not ids:
        return {}
    marcadores = ",".join(["?"] * len(ids))
    linhas = consultar(
        "SELECT sp_id, situacao, documentacao, chave, numero_nota, dedutivel, "
        "       origem, motivo, confianca, decidida_por "
        f"  FROM analisesps.sp_fiscal_analise WHERE sp_id IN ({marcadores})",
        tuple(str(i) for i in ids))
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


def sps_possiveis_da_nota(nota: dict, quantas: int = 5) -> list:
    """As SPs que PODEM ser desta nota — o caminho inverso da conciliação.

    Busca pelo CNPJ de quem emitiu, que é o credor do lançamento, e pelo valor.
    Sem isso a segunda visão diria "esta nota está órfã" e pararia ali, o que é
    meio caminho: quem vai resolver precisa de por onde começar."""
    from .db import consultar

    emitente = nota.get("emitente_doc") or emitente_da_chave(nota.get("chave"))
    raiz = _raiz(emitente)
    if not raiz:
        return []
    valor = _para_numero(nota.get("valor"))
    linhas = consultar(
        "SELECT id, credor, valor_num, vencimento_d, status_pgt, nf "
        "  FROM analisesps.sps "
        " WHERE left(regexp_replace(coalesce(documento, ''), '\\D', '', 'g'), 8) = ? "
        " ORDER BY CASE WHEN valor_num = ? THEN 0 ELSE 1 END, vencimento_d DESC "
        " LIMIT ?", (raiz, valor, int(quantas)))
    nomes = ["id", "credor", "valor_num", "vencimento_d", "status_pgt", "nf"]
    return [dict(zip(nomes, linha)) for linha in linhas]
