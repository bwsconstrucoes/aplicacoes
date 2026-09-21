# -*- coding: utf-8 -*-
"""
Aportes e devoluções de aporte no OMIE — A REGRA.

Este arquivo é o coração da funcionalidade, e não escreve nada em lugar
nenhum: ele decide **quais títulos precisam nascer** a partir do que o dono
escolheu na tela. A escrita no OMIE mora no `aportes_omie.py`, ao lado.

A separação é de propósito. Nas palavras do dono no briefing: *"a regra da
tabela é o coração disto: se ela errar, meus relatórios de aporte mentem e eu
não tenho como perceber."* Regra que não faz chamada de rede é regra que dá
para testar exaustivamente — e é o que os testes fazem.

⚠️ A TABELA, DO PLANO FINANCEIRO DE VERDADE (21/09/2026):

    Conta Provedora
      Entrada:  Devolução de Aportes BWS .... dinheiro que voltou da parceria
      Saída:    Aportes BWS ................. dinheiro que vai para a parceria

    Conta Parceria
      Entrada:  Aporte Parceiros ............ dinheiro que entra do parceiro
      Entrada:  Aporte BWS .................. dinheiro que entra da BWS
      Saída:    Devolução de Aportes ........ devolução à BWS ou ao parceiro

Lendo em português:

  · **BWS aporta:** sai da provedora ("Aportes BWS") e entra na parceria
    ("Aporte BWS"). **São duas categorias diferentes**, com códigos
    diferentes, para o mesmo dinheiro visto dos dois lados.
  · **O dinheiro da BWS volta:** sai da parceria ("Devolução de Aportes") e
    entra na provedora ("Devolução de Aportes BWS").
  · **O parceiro aporta:** entra só na parceria ("Aporte Parceiros"). Um
    lançamento só — o dinheiro vem de fora da empresa.
  · **O parceiro recebe de volta:** sai só da parceria ("Devolução de
    Aportes"), a mesma categoria da devolução à BWS.

⚠️ O BRIEFING DE SETEMBRO DIZIA O CONTRÁRIO — *"o mesmo nome dos dois lados"* —
e a primeira versão foi construída assim. Ele corrigiu olhando a tela pronta:
*"você colocou Aportes BWS que ENTRA, conta Parceria, o mesmo código do que
SAI. Não são."* Fica registrado porque o erro era plausível e passaria de
novo: os nomes são quase iguais, e um lançamento com a categoria do lado
errado não acusa nada em tela nenhuma.

⚠️ O DONO NÃO ESCOLHE CATEGORIA, E ISSO É DECISÃO DELE, não economia minha:
*"eu não devo ter que escolher categoria nenhuma: quem escolhe é a regra, a
partir da operação e das contas. Se eu pudesse escolher, eu erraria."*

⚠️ NENHUM CÓDIGO DE CATEGORIA OU DE CONTA APARECE NESTE ARQUIVO. O OMIE não
aceita o nome da categoria — exige o código ("2.01.05"). Mas o dono mexe no
plano financeiro dele, e código chumbado aqui viraria, no dia em que ele
mexesse, **lançamento errado em silêncio**: o pior defeito possível, porque
nada na tela denuncia. O código sai do de-para conferível (`aportes_de_para.py`,
gravado na migração 018), descoberto pela descrição.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, datetime

logger = logging.getLogger("analisesps.aportes")

# Os papéis que a regra conhece. O papel é o que a REGRA entende; qual conta
# do OMIE faz esse papel é o dono quem escolhe, em cada lançamento.
#
# ⚠️ "PROVEDORA", NÃO "MATRIZ" — é o nome que ele usa (21/09/2026), e o nome
# certo: o que define aquele lado não é ser a matriz da empresa, é ser a conta
# DE ONDE O DINHEIRO VEM para a parceria. Pode ser outra conta qualquer.
PROVEDORA = "provedora"
PARCERIA = "parceria"

PAPEL_ROTULO = {
    PROVEDORA: "conta Provedora",
    PARCERIA: "conta da Parceria",
}

# ---------------------------------------------------------------------------
# AS CINCO CATEGORIAS
#
# ⚠️ ELAS SÃO CINCO, NÃO QUATRO, E NENHUMA SE REPETE DOS DOIS LADOS. Esta é a
# correção de 21/09/2026, e ela desmente o que o próprio briefing dizia em
# setembro (*"o mesmo nome dos dois lados — o que distingue é a conta e o
# sentido"*). O dono mandou o plano financeiro de verdade:
#
#   Conta Provedora
#     Entrada: Devolução de Aportes BWS ......... 1.02.95
#     Saída:   Aportes BWS ...................... 2.08.97
#
#   Conta Parceria
#     Entrada: Aporte Parceiros ................. 1.02.02
#     Entrada: Aporte BWS ....................... 1.02.94
#     Saída:   Devolução de Aportes ............. 2.08.02
#
# ⚠️ REPARE EM "Aportes BWS" (saída da provedora) E "Aporte BWS" (entrada na
# parceria): nomes quase iguais, CÓDIGOS DIFERENTES, e são o mesmo dinheiro
# visto dos dois lados. Trocar um pelo outro é o erro mais fácil de cometer
# aqui e o mais difícil de perceber depois — por isso cada lado tem a sua
# própria chave, e a tela mostra os dois códigos lado a lado.
#
# Os códigos acima estão NESTE COMENTÁRIO e em lugar nenhum do programa: eles
# saem do plano financeiro, pela descrição, e o dono pode corrigir cada um na
# tela. Ele mexe no plano; código chumbado viraria lançamento errado calado.
# ---------------------------------------------------------------------------
CATEGORIAS = {
    "aportes_bws_saida": "Aportes BWS",
    "aporte_bws_entrada": "Aporte BWS",
    "aporte_parceiros": "Aporte Parceiros",
    "devolucao_aportes": "Devolução de Aportes",
    "devolucao_aportes_bws": "Devolução de Aportes BWS",
}

# O que cada uma significa, com as palavras dele. Vai para a tela: é o que
# permite distinguir "Aportes BWS" de "Aporte BWS" sem decorar código.
CATEGORIA_EXPLICACAO = {
    "aportes_bws_saida":
        "Dinheiro que sai de uma conta provedora para conta parceria.",
    "aporte_bws_entrada":
        "Dinheiro que entra da BWS na conta Parceria, proveniente da conta "
        "provedora.",
    "aporte_parceiros":
        "Dinheiro que entra do Parceiro.",
    "devolucao_aportes":
        "Devolução dos aportes à BWS ou ao Parceiro.",
    "devolucao_aportes_bws":
        "Dinheiro que entrou na conta provedora, proveniente da conta "
        "parceria.",
}

SAIDA = "saida"
ENTRADA = "entrada"

# Sentido → que tipo de título nasce no OMIE. Sai dinheiro: conta a PAGAR.
# Entra dinheiro: conta a RECEBER.
NATUREZA = {SAIDA: "P", ENTRADA: "R"}
NATUREZA_ROTULO = {"P": "Conta a pagar", "R": "Conta a receber"}
SENTIDO_ROTULO = {SAIDA: "Saída", ENTRADA: "Entrada"}


class Perna:
    """Um dos lados da operação — ou seja, UM título a nascer no OMIE."""

    __slots__ = ("papel", "sentido", "categoria", "campo_conta")

    def __init__(self, papel: str, sentido: str, categoria: str):
        self.papel = papel
        self.sentido = sentido
        self.categoria = categoria
        # De qual campo da tela sai a conta desta perna. Dinheiro que sai vem
        # da conta de ORIGEM; dinheiro que entra vai para a de DESTINO.
        self.campo_conta = "origem" if sentido == SAIDA else "destino"


# ---------------------------------------------------------------------------
# AS QUATRO OPERAÇÕES
#
# Duas são internas (dois títulos, porque o dinheiro anda entre duas contas da
# própria empresa) e duas são com o parceiro (um título só, porque o dinheiro
# vem de fora ou vai para fora).
# ---------------------------------------------------------------------------
OPERACOES = {
    "aporte_bws": {
        "rotulo": "BWS aporta na parceria",
        "explicacao": "O dinheiro sai da conta Provedora e entra na conta da "
                      "Parceria. Nasce um título de cada lado, e cada um com "
                      "a SUA categoria — não são a mesma.",
        "pernas": [
            Perna(PROVEDORA, SAIDA, "aportes_bws_saida"),
            Perna(PARCERIA, ENTRADA, "aporte_bws_entrada"),
        ],
    },
    "devolucao_bws": {
        "rotulo": "A parceria devolve o aporte à BWS",
        "explicacao": "O dinheiro sai da conta da Parceria e volta para a "
                      "conta Provedora. Nasce um título de cada lado.",
        "pernas": [
            Perna(PARCERIA, SAIDA, "devolucao_aportes"),
            Perna(PROVEDORA, ENTRADA, "devolucao_aportes_bws"),
        ],
    },
    "aporte_parceiro": {
        "rotulo": "O parceiro aporta na parceria",
        "explicacao": "O dinheiro vem de fora da empresa e entra na conta da "
                      "Parceria. Nasce um título só.",
        "pernas": [
            Perna(PARCERIA, ENTRADA, "aporte_parceiros"),
        ],
    },
    "devolucao_parceiro": {
        "rotulo": "A parceria devolve o aporte ao parceiro",
        "explicacao": "O dinheiro sai da conta da Parceria e vai para fora da "
                      "empresa. Nasce um título só.",
        "pernas": [
            Perna(PARCERIA, SAIDA, "devolucao_aportes"),
        ],
    },
}

ORDEM_DAS_OPERACOES = ["aporte_bws", "devolucao_bws",
                       "aporte_parceiro", "devolucao_parceiro"]


# ---------------------------------------------------------------------------
# AS CINCO SITUAÇÕES — 20/09/2026, depois de ele ver a tela
#
# ⚠️ O QUE ELE APONTOU EM 20/09: *"você tem que identificar melhor o que é
# entrada financeira e o que é saída."* A tela listava as categorias sem dizer
# em que conta e em que sentido cada uma vive.
#
# ⚠️ E EM 21/09 VEIO A CORREÇÃO MAIOR: *"você colocou Aportes BWS que ENTRA,
# conta Parceria, o mesmo código do que SAI. Não são."* Cada situação tem a
# SUA categoria, com o seu próprio código — inclusive as duas de nome quase
# igual ("Aportes BWS" que sai da provedora e "Aporte BWS" que entra na
# parceria).
#
# A ordem abaixo é a da lista que ele mandou: primeiro a conta provedora,
# depois a parceria.
# ---------------------------------------------------------------------------
SITUACOES = [
    (PROVEDORA, ENTRADA, "devolucao_aportes_bws"),
    (PROVEDORA, SAIDA, "aportes_bws_saida"),
    (PARCERIA, ENTRADA, "aporte_parceiros"),
    (PARCERIA, ENTRADA, "aporte_bws_entrada"),
    (PARCERIA, SAIDA, "devolucao_aportes"),
]


def situacoes_da_categoria(chave: str) -> list:
    """Em que situações esta categoria é usada. Mais de uma, quase sempre."""
    return [(p, sd) for p, sd, c in SITUACOES if c == chave]


def resumo_das_pernas(operacao: str) -> list:
    """O que a operação faz, em português, para a tela mostrar ANTES de pedir
    qualquer outra coisa. Sem contas e sem códigos — só o movimento."""
    op = OPERACOES.get(operacao) or {}
    return [{
        "papel": perna.papel,
        "papel_rotulo": PAPEL_ROTULO[perna.papel],
        "sentido": perna.sentido,
        "sentido_rotulo": SENTIDO_ROTULO[perna.sentido],
        "natureza_rotulo": NATUREZA_ROTULO[NATUREZA[perna.sentido]],
        "categoria_nome": CATEGORIAS[perna.categoria],
        "categoria_chave": perna.categoria,
    } for perna in op.get("pernas", [])]


class ErroDeRegra(Exception):
    """O que o dono escolheu não fecha com a regra — e a tela explica por quê.

    Sempre com a frase pronta em português: quem lê é ele, não um programador.
    """


# ---------------------------------------------------------------------------
# NORMALIZAÇÃO DE TEXTO — para casar descrição de categoria
# ---------------------------------------------------------------------------
def achatar(texto: str) -> str:
    """"Devolução de Aportes " -> "devolucao de aportes".

    Sem acento e sem maiúscula porque o plano financeiro é digitado por gente:
    "Devolucao de Aportes" e "DEVOLUÇÃO DE APORTES" são a mesma coisa, e errar
    isso faria a tela dizer "não encontrei" para uma categoria que existe.
    """
    s = unicodedata.normalize("NFKD", str(texto or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip().lower()


# ---------------------------------------------------------------------------
# VALOR E DATA
# ---------------------------------------------------------------------------
def ler_valor(bruto) -> float:
    """"1.234,56" / "R$ 1.234,56" / 1234.56 -> 1234.56. Recusa zero e negativo."""
    if isinstance(bruto, (int, float)):
        valor = float(bruto)
    else:
        s = str(bruto or "").strip().replace("R$", "").strip()
        if not s:
            raise ErroDeRegra("Informe o valor do aporte.")
        # "1.234,56" é o jeito brasileiro; "1234.56" também é aceito, e o que
        # distingue os dois é a vírgula.
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            valor = float(s)
        except ValueError:
            raise ErroDeRegra(f"Não entendi o valor \"{bruto}\".") from None
    if valor <= 0:
        raise ErroDeRegra("O valor tem de ser maior que zero.")
    # Teto de sanidade: não é regra de negócio, é rede contra dedo escorregado
    # num campo que manda dinheiro para o OMIE.
    if valor > 100_000_000:
        raise ErroDeRegra("Valor acima de cem milhões — confira antes de seguir.")
    return round(valor, 2)


def ler_data(bruto) -> date:
    """Aceita "aaaa-mm-dd" (o que o campo de data do navegador manda) e
    "dd/mm/aaaa" (o que gente digita)."""
    if isinstance(bruto, date):
        return bruto
    s = str(bruto or "").strip()
    if not s:
        raise ErroDeRegra("Informe a data.")
    for formato in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, formato).date()
        except ValueError:
            continue
    raise ErroDeRegra(f"Não entendi a data \"{bruto}\".")


def data_br(d: date) -> str:
    """O OMIE recebe e devolve data em dd/mm/aaaa."""
    return d.strftime("%d/%m/%Y")


# ---------------------------------------------------------------------------
# O NÚMERO E A OBSERVAÇÃO — automáticos, e o dono pode editar
# ---------------------------------------------------------------------------
def numero_automatico(grupo: str) -> str:
    """O MESMO número nos dois títulos de uma operação interna.

    É isto que permite achar o par depois: sem um número comum, dois títulos
    em contas diferentes são dois lançamentos soltos, e descobrir que um é a
    contrapartida do outro vira arqueologia.
    """
    return f"APORTE-{grupo}"


def observacao_automatica(operacao: str, perna: Perna, quem: str,
                          numero: str) -> str:
    """Em português, dizendo o que é e amarrando o par.

    Quem lê isto é o contador, seis meses depois, dentro do OMIE — não tem
    esta tela à mão nem sabe que ela existe.
    """
    op = OPERACOES.get(operacao, {})
    lado = PAPEL_ROTULO.get(perna.papel, perna.papel)
    partes = [
        f"{op.get('rotulo', operacao)} — {SENTIDO_ROTULO[perna.sentido].lower()} "
        f"da {lado}.",
        f"Contrapartida: {numero}.",
        "Lançado pela Análise de SPs" + (f" por {quem}." if quem else "."),
    ]
    return " ".join(partes)


# ---------------------------------------------------------------------------
# O PLANO — o que a tela mostra ANTES de gravar
# ---------------------------------------------------------------------------
def planejar(*, operacao: str, conta_origem=None, conta_destino=None,
             valor=None, data=None, fornecedor=None, fornecedor_nome: str = "",
             obra: str = "", obra_nome: str = "", quem: str = "",
             baixar: bool = True, grupo: str = "",
             descricoes: dict | None = None, categorias: dict | None = None,
             numero: str = "", observacoes: dict | None = None) -> dict:
    """Monta os títulos que vão nascer. NÃO fala com o OMIE nem com o banco.

    `descricoes` é {código da conta → nome}, só para a tela mostrar o nome em
    vez do número; `categorias` é o de-para chave → {codigo, descricao}. Os
    dois entram por parâmetro justamente para esta função continuar sendo
    pura — quem lê o banco é quem chama.

    Levanta `ErroDeRegra` com a frase pronta sempre que o que foi escolhido
    não fecha. Nunca "conserta" a escolha por conta própria: o dono pediu que
    a tela pare e diga, e parar e dizer é o comportamento correto quando a
    alternativa é um lançamento errado que ninguém percebe.
    """
    if operacao not in OPERACOES:
        raise ErroDeRegra("Escolha o que você está lançando.")
    op = OPERACOES[operacao]
    categorias = categorias or {}
    observacoes = observacoes or {}

    valor_num = ler_valor(valor)
    data_d = ler_data(data)

    if not fornecedor:
        raise ErroDeRegra("Escolha o fornecedor (o cadastro do OMIE que "
                          "aparece no título).")
    # A obra é obrigatória por decisão do dono (20/09/2026). O motivo, nas
    # palavras da escolha dele: sem departamento o aporte existe no OMIE mas
    # some de qualquer visão por obra, e ninguém percebe olhando a tela.
    if not str(obra or "").strip():
        raise ErroDeRegra("Escolha a obra. Sem ela o aporte entra no OMIE mas "
                          "não aparece em nenhuma visão por obra.")

    grupo = str(grupo or "").strip() or _novo_grupo(data_d)
    numero = str(numero or "").strip() or numero_automatico(grupo)

    escolhidas = {"origem": conta_origem, "destino": conta_destino}
    titulos = []
    usadas = []
    for perna in op["pernas"]:
        # ⚠️ A CONTA É DESTE LANÇAMENTO, NÃO DE UM CADASTRO — 20/09/2026.
        #
        # A versão anterior travava uma conta para "matriz" e outra para
        # "parceria", apontadas uma vez só. O dono derrubou isso com uma frase:
        # *"não quero travar a conta Matriz e a da Parceria, tem mais de uma
        # situação."* Há mais de uma parceria, e a mesma conta pode fazer
        # papéis diferentes conforme o que se está lançando.
        #
        # O que a OPERAÇÃO decide continua sendo o que importa: o papel de
        # cada lado, o sentido do dinheiro e — por consequência — a categoria.
        # O que ele diz é apenas QUAL conta faz aquele papel desta vez. Assim
        # não existe "combinação que a regra não prevê": não há nada com que
        # confrontar a escolha dele, porque não há mais cadastro fixo.
        escolhida = escolhidas.get(perna.campo_conta)
        if escolhida in (None, "", 0):
            raise ErroDeRegra(
                f"Escolha a conta de onde o dinheiro "
                f"{'SAI' if perna.sentido == SAIDA else 'ENTRA'} "
                f"({PAPEL_ROTULO[perna.papel]}).")
        codigo_papel = int(escolhida)
        usadas.append(codigo_papel)
        conta_papel = {"codigo": codigo_papel,
                       "descricao": (descricoes or {}).get(codigo_papel, "")}

        cat = categorias.get(perna.categoria) or {}
        codigo_categoria = str(cat.get("codigo") or "").strip()
        if not codigo_categoria:
            raise ErroDeRegra(
                f"Não sei o código da categoria "
                f"\"{CATEGORIAS[perna.categoria]}\" no seu plano financeiro. "
                f"Abra o de-para em Configurações e aponte qual é — o OMIE "
                f"não aceita o nome, só o código.")

        chave_obs = f"{perna.papel}_{perna.sentido}"
        titulos.append({
            "papel": perna.papel,
            "papel_rotulo": PAPEL_ROTULO[perna.papel],
            "sentido": perna.sentido,
            "sentido_rotulo": SENTIDO_ROTULO[perna.sentido],
            "natureza": NATUREZA[perna.sentido],
            "natureza_rotulo": NATUREZA_ROTULO[NATUREZA[perna.sentido]],
            "categoria_chave": perna.categoria,
            "categoria_nome": cat.get("descricao")
                              or CATEGORIAS[perna.categoria],
            "codigo_categoria": codigo_categoria,
            "categoria_e_transferencia": str(
                cat.get("transferencia") or "").upper().startswith("S"),
            "id_conta_corrente": int(codigo_papel),
            "conta_descricao": conta_papel.get("descricao") or "",
            "codigo_cliente_fornecedor": int(fornecedor),
            "fornecedor_nome": fornecedor_nome or "",
            "cod_departamento": str(obra).strip(),
            "departamento_nome": obra_nome or "",
            "valor": valor_num,
            "data": data_d,
            "data_br": data_br(data_d),
            "numero_documento": numero,
            "observacao": (observacoes.get(chave_obs)
                           or observacao_automatica(operacao, perna, quem,
                                                    numero)),
            "codigo_integracao": f"{grupo}-{perna.papel[:3]}-{perna.sentido[:3]}",
            "baixar": bool(baixar),
        })

    # A MESMA CONTA DOS DOIS LADOS não é aporte: é dinheiro saindo e entrando
    # no mesmo lugar. Como não há mais cadastro fixo para conferir a escolha,
    # esta é a única incoerência que o sistema CONSEGUE enxergar sozinho — e
    # ela é sempre engano.
    if len(usadas) > 1 and len(set(usadas)) == 1:
        raise ErroDeRegra(
            "As duas contas são a mesma. Nesta operação o dinheiro sai de uma "
            "conta e entra em outra — do jeito que está, ele sairia e entraria "
            "no mesmo lugar.")

    return {
        "operacao": operacao,
        "operacao_rotulo": op["rotulo"],
        "explicacao": op["explicacao"],
        "grupo": grupo,
        "numero_documento": numero,
        "valor": valor_num,
        "data": data_d,
        "data_br": data_br(data_d),
        "baixar": bool(baixar),
        "quem": quem,
        "titulos": titulos,
        "interna": len(titulos) > 1,
    }


# ---------------------------------------------------------------------------
# LANÇAMENTO EM LOTE — 20/09/2026
#
# Pedido dele, com as palavras dele: *"quero poder fazer vários lançamentos do
# mesmo tipo. Apenas incluir mais datas e valores. Lançamento em lote."*
#
# ⚠️ O QUE VARIA É SÓ DATA E VALOR, e isso não é limitação — é o desenho. A
# operação, as contas, o fornecedor e a obra são os MESMOS para todas as
# linhas, então há uma decisão só a conferir. Deixar cada linha ter a sua
# operação transformaria a conferência numa planilha, e é exatamente na
# conferência que este recurso não pode ser barato.
#
# ⚠️ CADA LINHA É UM LANÇAMENTO INDEPENDENTE, com o seu próprio grupo e o seu
# próprio número de documento. Isso importa na hora em que algo dá errado: uma
# linha que falha não leva as outras junto, e cada uma tem o seu par amarrado
# por dentro.
# ---------------------------------------------------------------------------
MAX_PARCELAS = 50


def ler_parcelas(linhas) -> list:
    """[{data, valor}] -> [(date, float)], validado e sem repetição boba.

    Levanta `ErroDeRegra` com a frase pronta: quem lê é ele.
    """
    if not linhas:
        raise ErroDeRegra("Informe pelo menos uma data e um valor.")
    if len(linhas) > MAX_PARCELAS:
        raise ErroDeRegra(
            f"São no máximo {MAX_PARCELAS} lançamentos por vez. Cada um vira "
            f"título no OMIE, e um lote grande demais leva ao bloqueio por "
            f"excesso de chamadas.")

    saida, vistas = [], set()
    for i, linha in enumerate(linhas, start=1):
        try:
            d = ler_data((linha or {}).get("data"))
            v = ler_valor((linha or {}).get("valor"))
        except ErroDeRegra as e:
            raise ErroDeRegra(f"Linha {i}: {e}") from None
        # DATA E VALOR IGUAIS DUAS VEZES quase sempre é linha duplicada sem
        # querer — e duas vezes o mesmo aporte no mesmo dia é o erro caro
        # deste recurso. Recusar é melhor do que avisar: aqui o certo é ele
        # apagar a linha, não confirmar.
        if (d, v) in vistas:
            raise ErroDeRegra(
                f"Linha {i}: já existe outra linha com {data_br(d)} e o mesmo "
                f"valor. Se são dois aportes mesmo, lance um de cada vez — "
                f"assim ninguém confunde com linha repetida.")
        vistas.add((d, v))
        saida.append((d, v))
    return saida


def planejar_lote(*, parcelas, **comuns) -> list:
    """Um plano por linha de data e valor, com o resto igual em todas."""
    comuns.pop("valor", None)
    comuns.pop("data", None)
    comuns.pop("grupo", None)
    # O número vem de cada grupo; um número comum a todas faria os pares de
    # lançamentos diferentes parecerem o mesmo par.
    comuns.pop("numero", None)
    return [planejar(valor=valor, data=data, **comuns)
            for data, valor in ler_parcelas(parcelas)]


def _novo_grupo(d: date) -> str:
    """Identificador curto e legível, que vai para o número do documento.

    Data + seis caracteres aleatórios. A data na frente serve para o dono
    reconhecer o lançamento na lista sem abrir; o sorteio evita que dois
    aportes do mesmo dia colidam no código de integração — e é justamente
    essa colisão que o OMIE usa para recusar a gravação repetida.
    """
    import secrets
    return f"{d.strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"


# ---------------------------------------------------------------------------
# A CRÍTICA: "isto aqui não será uma simples transferência?"
# ---------------------------------------------------------------------------
# Pedido do dono, e ele explicou o porquê melhor do que eu explicaria:
# *"classificar errado suja todos os meus relatórios."* No painel a distinção
# já existe — a categoria do OMIE tem a marca `transferencia = 'S'`, e esses
# lançamentos ficam de fora do DRE de propósito. Um aporte lançado como
# transferência some dos relatórios de aporte; uma transferência lançada como
# aporte infla o capital da obra. Os dois erros doem, e nenhum dos dois grita.
#
# ⚠️ É AVISO, NÃO BLOQUEIO — com todas as letras no briefing. Ele confirma e
# segue, mas confirma sabendo.
# ---------------------------------------------------------------------------
def criticar(plano: dict, movimentacoes: list) -> list:
    """Frases prontas sobre o que foi encontrado. Lista vazia = nada a dizer.

    `movimentacoes` vem de quem consultou o banco (ver `consultar_semelhantes`
    no `aportes_de_para.py`): cada item é um dicionário com valor, data, contas e a
    descrição da categoria.
    """
    avisos = []
    for m in movimentacoes or []:
        avisos.append(
            f"Achei uma movimentação de {_reais(m.get('valor'))} entre as "
            f"contas {m.get('conta_a') or '?'} e {m.get('conta_b') or '?'} em "
            f"{m.get('data') or '?'}"
            + (f", classificada como \"{m['categoria']}\"" if m.get("categoria")
               else "")
            + ". Isto que você está lançando pode ser a mesma coisa — "
              "transferência entre contas suas, não aporte.")

    # A segunda crítica não olha o histórico, olha o que está sendo lançado: se
    # a categoria escolhida pela regra estiver marcada como transferência no
    # plano financeiro, o lançamento vai nascer fora do DRE.
    for t in plano.get("titulos", []):
        if t.get("categoria_e_transferencia"):
            avisos.append(
                f"A categoria \"{t['categoria_nome']}\" está marcada como "
                f"transferência no seu plano financeiro. Lançamentos assim "
                f"ficam FORA do DRE de propósito — se este aporte tem de "
                f"aparecer nos relatórios, a marca precisa sair no OMIE.")
    return avisos


def _reais(valor) -> str:
    try:
        return ("R$ " + f"{float(valor):,.2f}"
                .replace(",", "·").replace(".", ",").replace("·", "."))
    except (TypeError, ValueError):
        return "R$ ?"
