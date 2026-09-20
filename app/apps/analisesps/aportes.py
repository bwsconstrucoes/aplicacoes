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

⚠️ A TABELA, DO JEITO QUE ELE DESENHOU E JÁ LANÇOU NO OMIE À MÃO:

    Conta          Entra/Sai   Nome (categoria) no OMIE
    ------------   ---------   -------------------------
    Matriz         Saída       Aportes BWS
    Parceria       Entrada     Aportes BWS
    Parceria       Entrada     Aportes Parceiros
    Parceria       Saída       Devolução de Aportes
    Matriz         Entrada     Devolução de Aportes BWS

Lendo em português:

  · **BWS aporta:** sai da matriz e entra na parceria, com o MESMO nome dos
    dois lados ("Aportes BWS"). O que distingue é a conta e o sentido.
  · **O dinheiro da BWS volta:** sai da parceria ("Devolução de Aportes") e
    entra na matriz ("Devolução de Aportes BWS").
  · **O parceiro aporta:** entra só na parceria ("Aportes Parceiros"). Um
    lançamento só — o dinheiro vem de fora da empresa.
  · **O parceiro recebe de volta:** sai só da parceria ("Devolução de
    Aportes").

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
# do OMIE faz esse papel é o dono quem aponta, na tela de configuração.
MATRIZ = "matriz"
PARCERIA = "parceria"

PAPEL_ROTULO = {
    MATRIZ: "conta Matriz da BWS",
    PARCERIA: "conta da Parceria",
}

# As quatro categorias, com o nome EXATO do plano financeiro do dono. A chave
# é o nome interno (o que o código usa); o valor é o que se procura no plano.
CATEGORIAS = {
    "aportes_bws": "Aportes BWS",
    "aportes_parceiros": "Aportes Parceiros",
    "devolucao_aportes": "Devolução de Aportes",
    "devolucao_aportes_bws": "Devolução de Aportes BWS",
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
        "explicacao": "O dinheiro sai da conta Matriz e entra na conta da "
                      "Parceria. Nasce um título de cada lado.",
        "pernas": [
            Perna(MATRIZ, SAIDA, "aportes_bws"),
            Perna(PARCERIA, ENTRADA, "aportes_bws"),
        ],
    },
    "devolucao_bws": {
        "rotulo": "A parceria devolve o aporte à BWS",
        "explicacao": "O dinheiro sai da conta da Parceria e volta para a "
                      "conta Matriz. Nasce um título de cada lado.",
        "pernas": [
            Perna(PARCERIA, SAIDA, "devolucao_aportes"),
            Perna(MATRIZ, ENTRADA, "devolucao_aportes_bws"),
        ],
    },
    "aporte_parceiro": {
        "rotulo": "O parceiro aporta na parceria",
        "explicacao": "O dinheiro vem de fora da empresa e entra na conta da "
                      "Parceria. Nasce um título só.",
        "pernas": [
            Perna(PARCERIA, ENTRADA, "aportes_parceiros"),
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
# ⚠️ O QUE ELE APONTOU: *"você tem que identificar melhor o que é entrada
# financeira e o que é saída. Porque, por exemplo, Aportes BWS, ele tanto está
# na conta de entrada quanto de saída. Na verdade, todas as categorias poderão
# ser utilizadas."*
#
# A tela mostrava as QUATRO categorias numa lista, como se cada uma fosse uma
# coisa só. Não são: "Aportes BWS" é usada DUAS vezes — saindo da matriz e
# entrando na parceria. Listada uma vez, ela esconde metade do que faz.
#
# O que existe de verdade são CINCO SITUAÇÕES (conta + sentido + categoria),
# que é exatamente como ele desenhou a tabela no briefing. É assim que a tela
# mostra agora, e a ordem é a dele.
# ---------------------------------------------------------------------------
SITUACOES = [
    (MATRIZ, SAIDA, "aportes_bws"),
    (PARCERIA, ENTRADA, "aportes_bws"),
    (PARCERIA, ENTRADA, "aportes_parceiros"),
    (PARCERIA, SAIDA, "devolucao_aportes"),
    (MATRIZ, ENTRADA, "devolucao_aportes_bws"),
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
             contas: dict | None = None, categorias: dict | None = None,
             numero: str = "", observacoes: dict | None = None) -> dict:
    """Monta os títulos que vão nascer. NÃO fala com o OMIE nem com o banco.

    `contas` é o de-para papel → {codigo, descricao}; `categorias` é o de-para
    chave → {codigo, descricao}. Os dois entram por parâmetro justamente para
    esta função continuar sendo pura — quem lê o banco é quem chama.

    Levanta `ErroDeRegra` com a frase pronta sempre que o que foi escolhido
    não fecha. Nunca "conserta" a escolha por conta própria: o dono pediu que
    a tela pare e diga, e parar e dizer é o comportamento correto quando a
    alternativa é um lançamento errado que ninguém percebe.
    """
    if operacao not in OPERACOES:
        raise ErroDeRegra("Escolha o que você está lançando.")
    op = OPERACOES[operacao]
    contas = contas or {}
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
    for perna in op["pernas"]:
        conta_papel = contas.get(perna.papel) or {}
        codigo_papel = conta_papel.get("codigo")
        if not codigo_papel:
            raise ErroDeRegra(
                f"Falta apontar qual conta do OMIE é a "
                f"{PAPEL_ROTULO[perna.papel]}. Isso se faz uma vez só, na "
                f"própria tela de Aportes, em Configurações.")

        # ⚠️ A CONTA NÃO É MAIS PERGUNTADA — 20/09/2026, pedido dele depois de
        # ver a tela: *"na hora que eu fosse mais embaixo definir o que está
        # acontecendo, você pergunta o que você está lançando; então ele já
        # define quais contas seriam utilizadas."*
        #
        # Faz sentido e é mais seguro: a operação determina as contas pela
        # regra que ele desenhou, então perguntá-las era oferecer a chance de
        # montar uma combinação que não existe. Quem não manda conta nenhuma
        # recebe a da regra.
        #
        # O confronto abaixo continua, para quem MANDA uma conta: é ele que
        # impede uma tela antiga, ou uma chamada direta, de gravar um
        # lançamento com a conta trocada e a categoria do outro lado.
        escolhida = escolhidas.get(perna.campo_conta)
        if escolhida in (None, "", 0):
            escolhida = codigo_papel
        if int(escolhida) != int(codigo_papel):
            # ⚠️ AQUI É ONDE A TELA RECUSA E EXPLICA. O dono escolheu poder
            # apontar as contas à mão (20/09/2026) sabendo deste preço: dá
            # para montar uma combinação que a regra não prevê, e aí o certo
            # é parar — não adivinhar qual categoria ele quis dizer.
            onde = ("de origem" if perna.campo_conta == "origem"
                    else "de destino")
            raise ErroDeRegra(
                f"Nesta operação, a conta {onde} tem de ser a "
                f"{PAPEL_ROTULO[perna.papel]} "
                f"({conta_papel.get('descricao') or codigo_papel}). "
                f"A regra de aporte que você desenhou não cobre a combinação "
                f"escolhida — se ela mudou, atualize o de-para em "
                f"Configurações.")

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
