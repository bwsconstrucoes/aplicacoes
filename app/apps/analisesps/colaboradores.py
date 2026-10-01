# -*- coding: utf-8 -*-
"""
O espelho do cadastro de colaboradores — a planilha "Registro de Colaboradores".

O CAMINHO DO DADO, e ele importa para entender esta tela:

    Pipefy (o card da pessoa)
       ↓  automação do dono, que já existia
    planilha "Registro de Colaboradores", aba "Dados Documentos"
       ↓  o botão "Atualizar cadastro" (este módulo)
    tabela analisesps.colaborador

Nada aqui é digitado, e é de propósito. Pedido do dono em 27/09/2026:

    "Às vezes é preciso fazer a alteração do auxílio de alimentação, do valor
     de um auxílio de transporte, ou um valor da gratificação. (…) eu preciso
     poder atualizar as informações que estão na análise SP que espelham o que
     está na planilha (…) um botão fácil para poder atualizar imediatamente."

Então o lugar de corrigir é o **card do Pipefy**; daqui sai só o link para ele
(`link_do_card`). Se alguém pudesse editar nesta tela, a próxima atualização
apagaria a edição — e ninguém saberia por quê.

⚠️ O QUE ESTÁ AQUI É CÓPIA, E PODE ESTAR VELHA. Por isso `quando_atualizou()`
existe e a tela mostra a hora: quem vai decidir dinheiro precisa saber se o
valor é de antes ou de depois da última mexida no card.

SÓ 30 DAS 78 COLUNAS, e não foi eu quem escolheu: a própria planilha já tem
uma aba oculta ("CadastroColaboradores") que alimenta as folhas de alimentação,
transporte, GM e diaristas, e ela importa exatamente estas colunas. Lido das
fórmulas em 27/09/2026. Duas razões para não trazer as outras 48:

  1. Custo. Cada botão apertado leria mais que o dobro de células, numa
     instância de 2 GB dividida com 17 módulos.
  2. Dado pessoal que não serve para nada aqui. Endereço, nome da mãe, RG,
     PIS e salário ficam na planilha. O que este módulo não busca não pode
     vazar por ele.

MEMÓRIA. A aba tem ~3.500 linhas por 78 colunas. Lê-se em BLOCOS, e de cada
bloco só as faixas de coluna que interessam — ver `_faixas_das_colunas`.
"""
from __future__ import annotations

import logging
import os

from . import formatos
from .credenciais import cliente, com_retry
from .sincronizacao import (_aba, _explicar_aba, _normalizar_cabecalho,
                            achar_coluna)

logger = logging.getLogger("analisesps.colaboradores")

PLANILHA_COLABORADORES = os.getenv(
    "ANALISESPS_SHEET_COLABORADORES",
    "1fqi4QUOVGUd1_4Gg4vK5qP_IMOSgFaw8DD9MDgmM3vo")
ABA_COLABORADORES = "Dados Documentos"

# ⚠️ A SEGUNDA ABA: o de/para ID Fortes → CPF.
#
# A Folha Sintética da contabilidade traz **código do empregado e nome** — não
# traz CPF. O ponto, o cadastro, o rateio e o pagamento são todos por CPF. O que
# liga os dois mundos é este de/para, e ele mora numa aba própria da mesma
# planilha.
#
# Vem no MESMO botão "Atualizar cadastro": duas atualizações separadas para a
# mesma planilha seria pedir para alguém esquecer uma delas — e a folha passaria
# a não achar gente que está cadastrada.
ABA_ID_FORTES = "ID Fortes"

# Os nomes que as duas colunas podem ter. O primeiro de cada lista é o mais
# provável; a carga AVISA com o cabeçalho de verdade quando não acha nenhum,
# porque adivinhar posição aqui trocaria o código de uma pessoa pelo de outra.
COLUNAS_DO_ID_FORTES = ["ID Fortes", "Id Fortes", "Código", "Codigo",
                        "Matrícula", "Matricula", "Código Fortes"]
COLUNAS_DO_CPF_NO_DE_PARA = ["CPF", "CPF (Cadastro de Pessoa Física)",
                             "CPF Números", "cpf"]

# ⚠️ O CÓDIGO DO FORTES NA PRÓPRIA FICHA — 30/09/2026.
#
# O dono: *"vários colaboradores estão no cadastro, mas diz que não tá"* — e
# mostrou a ficha de uma pessoa admitida em 16/09 com o código 004031 na ÚLTIMA
# coluna da aba "Dados Documentos". O sistema só lia a aba separada "ID Fortes",
# e quem ainda não estava nela aparecia na folha como "fora do cadastro", sem CPF
# e sem pagamento — com o código escrito na ficha.
#
# Agora a ficha manda: o código vem da coluna da própria pessoa, e a aba "ID
# Fortes" só completa quem não tem código na ficha. Quando as duas discordam,
# vale a ficha, e a tela diz quem.
#
# ⚠️ "Matrícula" NÃO entra aqui, ao contrário da lista da aba separada: na ficha
# ela é outra coisa (o CPF com uma letra no fim, "127348894A").
COLUNAS_DO_ID_NA_FICHA = ["ID Fortes", "Id Fortes", "Código Fortes",
                          "Codigo Fortes"]


def normalizar_id_fortes(valor) -> str:
    """O código como a folha da contabilidade o escreve: seis dígitos, com os
    zeros da frente.

    A planilha às vezes guarda o código como NÚMERO, e aí "004031" chega como
    "4031" — que não casaria com o "004031" da folha. Código que não é só
    dígitos fica como veio (sem espaços), para não inventar nada."""
    texto = "".join(str(valor or "").split())
    if texto.endswith(".0") and texto[:-2].isdigit():
        texto = texto[:-2]
    if texto.isdigit() and len(texto) < 6:
        return texto.zfill(6)
    return texto

# O cabeçalho está na linha 1. A LINHA 2 NÃO É DADO: ela guarda o número de
# cada coluna (1, 2, 3…), e serve a uma fórmula da aba "Dados Gerais" que monta
# o endereço da coluna com INDIRECT. Ler a linha 2 como pessoa criaria um
# colaborador chamado "2". Os dados começam na 3 — é o que a própria planilha
# faz nas abas que consultam esta (`QUERY('Dados Documentos'!A3:AX; …)`).
LINHA_DO_CABECALHO = 1
PRIMEIRA_LINHA_DADOS = 3

# Quantas linhas por leitura. 500 × 30 colunas = 15 mil textos por bloco.
LINHAS_POR_BLOCO = 500

# Teto de segurança: se a aba vier com um número de linhas absurdo (fórmula
# esticada, aba corrompida), a carga para em vez de tentar ler milhões de
# células. O cadastro tem ~3.500 pessoas; 50 mil é folga de sobra.
MAXIMO_DE_LINHAS = 50_000

# ---------------------------------------------------------------------------
# AS COLUNAS QUE INTERESSAM, PELO NOME QUE ELAS TÊM NA PLANILHA
#
# PELO NOME, NUNCA PELA POSIÇÃO. Esta é a regra da casa (ver `achar_coluna` em
# `sincronizacao.py`) e ela tem preço pago: em 26/09/2026 eu afirmei ao dono que
# um carregamento estava lendo a coluna errada porque olhei o cabeçalho de uma
# CÓPIA da planilha. Leitura por posição quebra calada no dia em que alguém
# insere uma coluna; leitura por nome avisa.
#
# Cada entrada é: campo no banco -> nomes aceitos, do mais provável ao menos.
# O primeiro é o nome que a planilha REALMENTE usa hoje.
# ---------------------------------------------------------------------------
COLUNAS = {
    "cpf": ["CPF (Cadastro de Pessoa Física)", "CPF", "CPF Números"],
    # O ID Fortes NÃO vem desta aba: vem da aba "ID Fortes" (ver `ABA_ID_FORTES`).
    # Fica fora de `COLUNAS` de propósito — procurá-lo aqui geraria um aviso de
    # coluna faltando toda vez, e aviso que sempre aparece vira enfeite.
    "nome": ["Nome Completo", "Nome", "Colaborador"],
    "card_pipefy": ["Nº Registro Pipefy", "N Registro Pipefy",
                    "Numero Registro Pipefy", "Registro Pipefy"],
    "matricula": ["Matrícula", "Matricula"],
    "celular": ["Celular", "Telefone"],
    "cargo": ["Cargo [ ]", "Cargo"],
    "tipo": ["Tipo"],
    "tipo_contrato": ["Tipo de Contrato"],
    "fase": ["Fase Atual"],
    "convencao": ["Convenção", "Convencao"],
    # ⚠️ A OBRA SÃO DUAS COLUNAS, e confundi-las foi correção do dono em
    # 28/09/2026: *"em obra tem que colocar o CÓDIGO da obra e não a obra por
    # extenso. Todo mundo tem código da obra."* O extenso serve para ler; o
    # CÓDIGO é o que casa com a conta de pagamento (aba "C. Diários") e com o
    # rateio. As telas mostram o código.
    "obra_cadastro": ["Objeto Obra [ ]", "Objeto Obra"],
    "obra_codigo": ["Código da Obra", "Codigo da Obra", "Cód. Obra",
                    "Cod Obra", "Código Obra", "Codigo Obra"],
    "valor_gratificacao": ["Valor da Gratificação", "Valor da Gratificacao"],
    "parcela_unica": ["Recebe Parcela Única", "Recebe Parcela Unica"],
    "aviso_previo": ["Data do Aviso Prévio", "Data do Aviso Previo"],
    "ultimo_dia": ["Último dia Trabalhado", "Ultimo dia Trabalhado"],
    "data_saida": ["Data de Saída", "Data de Saida"],
    # ⚠️ AS DUAS DATAS QUE DECIDEM DIARISTA × CTPS, por dia (ver `folha_vinculo`).
    # Sem elas todo dia de ponto cai em "falta data para decidir", e a tela de
    # diaristas não tem o que mostrar. Os nomes são os mais prováveis — a fórmula
    # original chama as colunas de "Data de Início" (U) e "Data de Admissão" (V).
    "data_inicio": ["Data de Início", "Data de Inicio", "Início", "Data Início"],
    "data_admissao": ["Data de Admissão", "Data de Admissao", "Admissão",
                      "Data Admissão"],
}

# ---------------------------------------------------------------------------
# OS AUXÍLIOS — as colunas que o dono disse que mais mudam, e as únicas cujo
# nome eu NÃO tenho confirmado.
#
# Por que ficam separadas: as fórmulas exportadas em 27/09/2026 provam QUE elas
# existem e O QUE fazem (a aba Alimentação usa uma como modalidade e a outra
# como valor; a aba Transporte idem, e lá "Cartão" quer dizer "não paga em
# dinheiro"). O que as fórmulas NÃO mostram é o TÍTULO dessas colunas, porque
# elas chegam por IMPORTRANGE de uma faixa (Col65…Col68), sem nome.
#
# Então aqui vão os nomes mais prováveis. Quando a carga não achar uma delas,
# ela AVISA com o cabeçalho de verdade em vez de gravar em branco calada —
# valor de auxílio em branco vira pagamento a menos, e ninguém repara num
# pagamento a menos tão rápido quanto num a mais.
# ⚠️ CONFIRMADOS PELO DONO EM 28/09/2026 — e o nome era CATEGORIA, não
# "Modalidade". Palavras dele, olhando a planilha:
#
#   "Existe aqui na coluna BM, o nome da coluna é Categoria Auxílio Alimentação.
#    Aí na coluna seguinte tem Valor Auxílio Alimentação. E o transporte, mesma
#    coisa: na coluna BO tem Categoria Auxílio Transporte, e na seguinte tem
#    Valor Auxílio Transporte. E ainda tem a coluna BQ, que é observação."
#
# ⚠️ FOI ISSO QUE FEZ A TELA NÃO CALCULAR NADA. Eu havia chutado "Modalidade
# Auxílio Alimentação"; a coluna não existia, o campo ficava vazio, e cada pessoa
# caía em "o cadastro não diz a modalidade" — zero dias, zero valor, tudo com
# cara de ajuste pendente. Os nomes chutados ficam ao final da lista como
# alternativa, mas o PRIMEIRO é o que a planilha usa.
COLUNAS_DOS_AUXILIOS = {
    "modo_alimentacao": ["Categoria Auxílio Alimentação",
                         "Categoria Auxilio Alimentacao",
                         "Categoria Alimentação",
                         "Modalidade Auxílio Alimentação",
                         "Modalidade Alimentação"],
    "valor_alimentacao": ["Valor Auxílio Alimentação",
                          "Valor Auxilio Alimentacao",
                          "Valor do Auxílio Alimentação",
                          "Valor Alimentação"],
    "modo_transporte": ["Categoria Auxílio Transporte",
                        "Categoria Auxilio Transporte",
                        "Categoria Transporte",
                        "Modalidade Auxílio Transporte",
                        "Modalidade Transporte"],
    "valor_transporte": ["Valor Auxílio Transporte",
                         "Valor Auxilio Transporte",
                         "Valor do Auxílio Transporte",
                         "Valor Transporte"],
    # A coluna BQ. Vale para os dois auxílios — é onde o DP escreve o porquê de
    # uma exceção, e é o que ele quer ver clicando na pessoa.
    # ⚠️ "OBSERVAÇÃO AJUDA DE CUSTO" — o nome completo, que ele deu em
    # 29/09/2026 depois de eu ter guardado só "Observação". A planilha tem mais de
    # uma coluna de observação; procurar pelo nome curto poderia casar com a
    # errada, e aí a tela mostraria a observação de outro assunto.
    "observacao_auxilio": ["Observação Ajuda de Custo",
                           "Observacao Ajuda de Custo",
                           "Observação Ajuda Custo", "Observação Auxílio",
                           "Observação", "Observacao"],
    "paga_por_beevale": ["Paga por BeeVale", "BeeVale", "Pagamento BeeVale"],
}

# ---------------------------------------------------------------------------
# QUAIS COLUNAS FALTANDO VIRAM AVISO NA TELA — e quais ficam caladas
#
# ⚠️ ESTA SEPARAÇÃO EXISTE POR CORREÇÃO DO DONO, em 26/09/2026, sobre outra
# crítica que eu ia criar: *"no ponto tem mais informação de pessoas do que tem
# na folha de pagamento"* — ou seja, avisar sobre o que é normal transforma o
# aviso em ruído, e ruído faz a pessoa ignorar o aviso que importa.
#
# Então avisa só o que muda decisão ou dinheiro:
#
#   - os cinco dos AUXÍLIOS, porque campo em branco vira pagamento a menos;
#   - o número do CARD, porque sem ele o link para o Pipefy não existe — e o
#     link é metade do que o dono pediu;
#   - o que decide QUEM entra em pagamento (tipo de contrato, fase, data de
#     saída) e QUANTO (gratificação, parcela única).
#
# O resto — matrícula, celular, cargo, convenção, obra do cadastro, aviso prévio,
# último dia — fica em branco calado. É informação de conferência: a falta dela
# não faz ninguém receber errado.
COLUNAS_QUE_AVISAM = frozenset({
    "card_pipefy", "tipo_contrato", "fase", "data_saida",
    "valor_gratificacao", "parcela_unica",
    # ⚠️ O CÓDIGO DA OBRA AVISA porque sem ele a tela de auxílio não consegue
    # agrupar por obra, e é por obra que ele confere e decide o rateio. Ficar
    # em branco calado foi o que ele viu em 28/09/2026 ("aqui tem vários vazios").
    "obra_codigo",
} | {"modo_alimentacao", "valor_alimentacao",
     "modo_transporte", "valor_transporte"})

# ⚠️ "Paga por BeeVale" SAIU DA LISTA DE AVISOS em 29/09/2026, e o motivo é que o
# aviso era lixo: o dono disse *"não entendi essa pergunta"* sobre ele, e ao
# procurar descobri que **o campo não é lido em lugar nenhum do sistema**. Eu
# estava pedindo o nome de uma coluna para preencher um campo que nada consulta.
#
# A carga continua tentando achá-la (o nome está em `COLUNAS_DOS_AUXILIOS`); só não
# enche a tela por causa dela. Quando a geração de arquivo precisar de verdade
# saber quem recebe por BeeVale, o aviso volta — e aí com um motivo que se explica.
#
# A regra geral: só avisa o que muda decisão ou dinheiro AGORA. Aviso sem
# consequência é o que faz ninguém ler os avisos que importam.

# ⚠️ O QUE PARA DE FUNCIONAR quando cada coluna falta. É isto que vai no aviso, em
# vez de uma pergunta: quem lê a tela precisa saber o TAMANHO do problema para
# decidir se corre atrás agora ou depois. "Este campo fica em branco" não diz nada.
EFEITO_DE_FALTAR = {
    "modo_alimentacao":
        "o auxílio ALIMENTAÇÃO não calcula para ninguém (sem a categoria não há "
        "como contar os dias).",
    "valor_alimentacao":
        "o auxílio ALIMENTAÇÃO sai zerado (sem valor do dia não há o que "
        "multiplicar).",
    "modo_transporte":
        "o auxílio TRANSPORTE não calcula para ninguém.",
    "valor_transporte":
        "o auxílio TRANSPORTE sai zerado.",
    "obra_codigo":
        "as telas não conseguem agrupar por obra — e é por obra que você confere "
        "e decide o rateio.",
    "card_pipefy":
        "o nome da pessoa não leva ao card, e é no card que se corrige o auxílio.",
    "tipo_contrato":
        "a regra de diarista × CTPS não decide, porque ela olha o contrato.",
    "fase":
        "quem foi desligado deixa de ser escondido da lista — e entra em "
        "pagamento sem ninguém ver.",
    "data_saida":
        "quem já saiu deixa de ser travado no pagamento.",
    "valor_gratificacao":
        "a gratificação sai zerada.",
    "parcela_unica":
        "quem recebe parcela única deixa de ser reconhecido.",
}

DATAS = ("aviso_previo", "ultimo_dia", "data_saida",
         "data_inicio", "data_admissao")
NUMEROS = ("valor_alimentacao", "valor_transporte", "valor_gratificacao")

# Os campos que a tela pede sem parar, na ordem em que a tabela os guarda.
# ⚠️ `id_fortes` NÃO ENTRA EM `CAMPOS`, e não é esquecimento: `CAMPOS` é o que a
# carga da aba "Dados Documentos" grava, e o ID Fortes vem de OUTRA aba. Se
# entrasse aqui, cada carga do cadastro sobrescreveria o ID com vazio — e a folha
# deixaria de achar as pessoas na atualização seguinte. Ele é gravado à parte, por
# `atualizar_ids_fortes`, e LIDO junto (ver `CAMPOS_LIDOS`).
CAMPOS = ["cpf", "nome", "card_pipefy", "matricula", "celular", "cargo",
          "tipo", "tipo_contrato", "fase", "convencao", "obra_cadastro",
          "obra_codigo", "observacao_auxilio",
          "valor_alimentacao", "modo_alimentacao",
          "valor_transporte", "modo_transporte", "valor_gratificacao",
          "parcela_unica", "paga_por_beevale",
          "aviso_previo", "ultimo_dia", "data_saida",
          "data_inicio", "data_admissao"]

# O que as telas leem: os campos gravados pela aba principal MAIS o ID Fortes.
CAMPOS_LIDOS = CAMPOS + ["id_fortes"]


class ErroDoCadastro(RuntimeError):
    """Não deu para trazer o cadastro. A frase vai inteira para a tela."""


def _pronto() -> bool:
    """A migração 028 já rodou? Enquanto não, a tela avisa em vez de estourar.

    O código sobe para o Render ANTES de alguém apertar "Aplicar atualizações
    do banco" — sem esta guarda, a tela de Rateio cairia no intervalo entre as
    duas coisas."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.colaborador LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


# ---------------------------------------------------------------------------
# O link para o card do Pipefy
# ---------------------------------------------------------------------------
ENDERECO_DO_CARD = "https://app.pipefy.com/open-cards/"


def link_do_card(card) -> str:
    """O endereço do card da pessoa no Pipefy, ou "" quando não há número.

    É a mesma montagem que a planilha faz na coluna BX
    (`="https://app.pipefy.com/open-cards/"&B`), de propósito: dois jeitos de
    montar o mesmo link divergiriam no dia em que o Pipefy mudasse o endereço.

    Só dígitos porque o número chega da planilha como texto e às vezes com
    espaço ou com ".0" pendurado de quem o tratou como número."""
    digitos = "".join(c for c in str(card or "") if c.isdigit())
    return f"{ENDERECO_DO_CARD}{digitos}" if digitos else ""


# ---------------------------------------------------------------------------
# A leitura da planilha
# ---------------------------------------------------------------------------
def letra_da_coluna(indice_zero: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA. Recebe índice a partir de zero."""
    letras = ""
    numero = indice_zero + 1
    while numero > 0:
        numero, resto = divmod(numero - 1, 26)
        letras = chr(65 + resto) + letras
    return letras


def _faixas_das_colunas(indices: list) -> list:
    """Agrupa colunas vizinhas em faixas, para pedir tudo numa chamada só.

    Recebe [0, 1, 4, 22, 23, 24, 25] e devolve [[0, 1], [4], [22, 23, 24, 25]].

    POR QUE AGRUPAR em vez de ler de A até a última: é o que faz a leitura
    trazer 30 colunas em vez de 78. As colunas que não interessam ficam na
    planilha — inclusive endereço, nome da mãe, RG, PIS e salário, que este
    módulo não tem por que conhecer."""
    grupos: list = []
    for i in sorted(set(int(x) for x in indices)):
        if grupos and i == grupos[-1][-1] + 1:
            grupos[-1].append(i)
        else:
            grupos.append([i])
    return grupos


def _bloco_vazio(quantas: int, largura: int) -> list:
    return [[""] * largura for _ in range(quantas)]


def _juntar(blocos: list, grupos: list, quantas_linhas: int) -> list:
    """Remonta as linhas a partir das faixas lidas separadamente.

    O Sheets devolve uma matriz por faixa, e ELAS NÃO TÊM O MESMO TAMANHO: uma
    faixa cujas últimas células estão vazias volta mais curta, e uma linha com
    o fim vazio volta com menos células. Quem juntar por posição sem cuidado
    desloca dado de uma pessoa para outra — e aí um auxílio vai para o CPF
    errado.

    Devolve uma lista de dicionários {índice da coluna: texto}, uma por linha
    pedida, sempre com `quantas_linhas` itens."""
    linhas = [dict() for _ in range(quantas_linhas)]
    for grupo, bloco in zip(grupos, blocos):
        bloco = bloco or []
        for r in range(quantas_linhas):
            celulas = bloco[r] if r < len(bloco) else []
            for posicao, indice in enumerate(grupo):
                valor = celulas[posicao] if posicao < len(celulas) else ""
                linhas[r][indice] = str(valor or "").strip()
    return linhas


def _achar_colunas(cabecalho: list) -> tuple[dict, list]:
    """Onde está cada coluna que interessa. Devolve (posições, avisos)."""
    normalizado = _normalizar_cabecalho(cabecalho)
    posicoes: dict = {}
    avisos: list = []

    for campo, aceitos in list(COLUNAS.items()) + list(COLUNAS_DOS_AUXILIOS.items()):
        i = achar_coluna(normalizado, aceitos)
        if i is not None:
            posicoes[campo] = i
            continue
        # CPF e nome: sem eles não há cadastro nenhum. Quem chamou decide parar,
        # e o recado de lá diz o cabeçalho de verdade.
        if campo in ("cpf", "nome"):
            avisos.append(
                f'não achei a coluna de "{aceitos[0]}" na aba '
                f'"{ABA_COLABORADORES}".')
        elif campo in COLUNAS_QUE_AVISAM:
            # ⚠️ O AVISO DIZ O QUE PARA DE FUNCIONAR, e não faz pergunta. Ele
            # terminava com "me diga o nome exato dela na planilha" — uma pergunta
            # numa tela onde não há como responder. Reclamação dele em 29/09/2026:
            # *"não entendi essa pergunta."*
            avisos.append(
                f'a coluna "{aceitos[0]}" não existe na planilha com esse nome, '
                f"então {EFEITO_DE_FALTAR.get(campo, 'este campo fica em branco')}")
        # else: fica em branco calado. Ver COLUNAS_QUE_AVISAM.

    # O código do Fortes na ficha: opcional e calado quando falta — a aba "ID
    # Fortes" continua sendo o caminho de quem não tem a coluna.
    i_id = achar_coluna(normalizado, COLUNAS_DO_ID_NA_FICHA)
    if i_id is not None:
        posicoes["id_fortes"] = i_id

    return posicoes, avisos


def _registro(linha: dict, posicoes: dict) -> dict | None:
    """Uma pessoa, já com data e dinheiro convertidos. None quando não serve."""
    from .folha_rateio import so_digitos

    cru = {campo: linha.get(indice, "") for campo, indice in posicoes.items()}
    cpf = so_digitos(cru.get("cpf"))
    # SEM CPF A LINHA NÃO SERVE: o CPF é a chave em tudo — no ponto, na folha
    # da contabilidade, no rateio. Uma pessoa sem CPF aqui não casaria com
    # nada, e ocuparia lugar na lista fingindo que existe.
    if len(cpf) != 11:
        return None

    registro = {"cpf": cpf,
                # Fora de CAMPOS de propósito: ver `_gravar_ids_da_ficha`.
                "_id_fortes_ficha": normalizar_id_fortes(cru.get("id_fortes"))}
    for campo in CAMPOS[1:]:
        valor = cru.get(campo, "")
        if campo in DATAS:
            registro[campo] = formatos.para_data(valor)
        elif campo in NUMEROS:
            registro[campo] = formatos.para_numero(valor)
        else:
            registro[campo] = str(valor or "").strip()
    return registro


SQL_GRAVAR = (
    "INSERT INTO analisesps.colaborador (" + ", ".join(CAMPOS) + ") "
    "VALUES (" + ", ".join(["?"] * len(CAMPOS)) + ") "
    "ON CONFLICT (cpf) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in CAMPOS if c != "cpf")
    + ", atualizado_em = now()"
)


def _gravar(conn, registros: list) -> int:
    if not registros:
        return 0
    conn.executemany(
        SQL_GRAVAR,
        [tuple(r.get(c) for c in CAMPOS) for r in registros])
    conn.commit()
    return len(registros)


def _gravar_ids_da_ficha(da_ficha: dict) -> dict:
    """Grava o código do Fortes que veio na ficha de cada pessoa.

    `da_ficha` é `{cpf: codigo}`, só com quem tem código preenchido — VAZIO NA
    FICHA NÃO APAGA NADA: a pessoa pode ter o código vindo da aba "ID Fortes".

    ⚠️ UM CÓDIGO, UMA PESSOA. Se o mesmo código aparece em duas fichas, vale a
    primeira e o repetido vira aviso (o salário de uma iria para a obra da
    outra). E se o código estava gravado em OUTRA pessoa — vindo da aba
    separada, numa carga antiga — ele sai de lá: a ficha manda.

    Devolve `{"gravados", "repetidos"}`.
    """
    from .db import conexao
    if not da_ficha or not tem_id_fortes():
        return {"gravados": 0, "repetidos": []}
    dono: dict = {}
    repetidos: list = []
    for cpf, codigo in da_ficha.items():
        if codigo in dono and dono[codigo] != cpf:
            repetidos.append(codigo)
            continue
        dono[codigo] = cpf
    with conexao() as conn:
        conn.executemany(
            "UPDATE analisesps.colaborador SET id_fortes = '' "
            " WHERE id_fortes = ? AND cpf <> ?",
            [(codigo, cpf) for codigo, cpf in dono.items()])
        conn.executemany(
            "UPDATE analisesps.colaborador SET id_fortes = ? WHERE cpf = ?",
            [(codigo, cpf) for codigo, cpf in dono.items()])
        conn.commit()
    return {"gravados": len(dono), "repetidos": sorted(set(repetidos))}


def atualizar(anotar=None) -> dict:
    """Traz o cadastro da planilha para a tabela. É o que o botão chama.

    ATUALIZA, NÃO SUBSTITUI (`ON CONFLICT … DO UPDATE`): a tabela nunca fica
    vazia no meio do caminho, então uma leitura interrompida deixa o cadastro
    velho inteiro em vez de deixar buraco. Quem foi desligado não é apagado —
    continua com a data de saída, que é o que tira a pessoa das listas de
    pagamento e o que permite conferir uma folha antiga depois.

    Devolve quantas pessoas entraram e os avisos que a tela deve mostrar."""
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    if not _pronto():
        raise ErroDoCadastro(
            "a tabela do cadastro ainda não existe. Aperte "
            '"Aplicar atualizações do banco" em Configurações e tente de novo.')

    anotar("abrindo a planilha do cadastro")
    try:
        aba = _aba(PLANILHA_COLABORADORES, ABA_COLABORADORES)
    except Exception as e:  # noqa: BLE001
        raise ErroDoCadastro(
            _explicar_aba(PLANILHA_COLABORADORES, ABA_COLABORADORES, e)) from e

    # O cabeçalho primeiro, e só ele: uma linha custa quase nada e é o que
    # diz onde cada coluna está hoje.
    try:
        cabecalho = com_retry(
            lambda: aba.row_values(LINHA_DO_CABECALHO)) or []
    except Exception as e:  # noqa: BLE001
        raise ErroDoCadastro(
            f'não deu para ler o cabeçalho da aba "{ABA_COLABORADORES}": '
            f"{e}") from e

    posicoes, avisos = _achar_colunas(cabecalho)
    if "cpf" not in posicoes or "nome" not in posicoes:
        raise ErroDoCadastro(
            f'a aba "{ABA_COLABORADORES}" não tem a coluna do CPF ou a do '
            f"nome, e sem as duas não há cadastro. O cabeçalho dela é: "
            f"{', '.join(str(c) for c in cabecalho if str(c).strip()) or '(vazio)'}.")

    grupos = _faixas_das_colunas(list(posicoes.values()))
    total_linhas = min(int(com_retry(lambda: aba.row_count) or 0),
                       MAXIMO_DE_LINHAS)
    logger.info("Análise de SPs: cadastro — a aba tem %d linhas, lendo %d "
                "coluna(s) em %d faixa(s).",
                total_linhas, len(posicoes), len(grupos))

    gravadas = 0
    ignoradas = 0
    da_ficha: dict = {}
    linha = PRIMEIRA_LINHA_DADOS
    while linha <= total_linhas:
        fim = min(linha + LINHAS_POR_BLOCO - 1, total_linhas)
        faixas = [f"{letra_da_coluna(g[0])}{linha}:{letra_da_coluna(g[-1])}{fim}"
                  for g in grupos]
        try:
            blocos = com_retry(lambda f=faixas: aba.batch_get(f))
        except Exception as e:  # noqa: BLE001
            raise ErroDoCadastro(
                f"não deu para ler as linhas {linha} a {fim} do cadastro: "
                f"{e}") from e

        linhas = _juntar(list(blocos), grupos, fim - linha + 1)
        registros = []
        for bruta in linhas:
            r = _registro(bruta, posicoes)
            if r is None:
                # Linha em branco no fim da aba é o caso comum e não é notícia.
                if any(bruta.values()):
                    ignoradas += 1
                continue
            if r.get("_id_fortes_ficha"):
                da_ficha.setdefault(r["cpf"], r["_id_fortes_ficha"])
            registros.append(r)

        if registros:
            with conexao() as conn:
                gravadas += _gravar(conn, registros)
        anotar("trazendo o cadastro de colaboradores",
               f"{gravadas} pessoa(s)")
        # `blocos`, `linhas` e `registros` saem de escopo aqui: o bloco
        # seguinte não soma memória com este.
        linha = fim + 1

    if ignoradas:
        avisos.append(
            f"{ignoradas} linha(s) da planilha ficaram de fora por não terem "
            "um CPF válido de 11 dígitos.")

    # ⚠️ O DE/PARA DO ID FORTES VEM NO MESMO BOTÃO. Sem ele a folha da
    # contabilidade não acha ninguém: ela traz código e nome, e todo o resto do
    # sistema é por CPF. Duas atualizações separadas para a mesma planilha seria
    # pedir para alguém esquecer uma delas.
    #
    # Num `try` largo porque o cadastro já está gravado a esta altura: um
    # tropeço aqui não pode desfazer o que deu certo.
    fortes = {"casados": 0}
    ficha = {"gravados": 0, "repetidos": []}
    try:
        ficha = _gravar_ids_da_ficha(da_ficha)
        if ficha["repetidos"]:
            avisos.append(
                f"{len(ficha['repetidos'])} código(s) do Fortes aparecem em mais "
                f"de uma ficha ({', '.join(ficha['repetidos'][:5])}"
                f"{'…' if len(ficha['repetidos']) > 5 else ''}). Valeu a "
                "primeira; enquanto não for corrigido, o salário de uma pessoa "
                "pode ir para a obra de outra.")
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou gravar o ID Fortes da ficha")
        avisos.append(f"não deu para gravar o código do Fortes das fichas: {e}")
    try:
        fortes = atualizar_ids_fortes(anotar, da_ficha=da_ficha)
        avisos.extend(fortes.get("avisos") or [])
    except Exception as e:  # noqa: BLE001
        logger.exception("Análise de SPs: falhou o de/para do ID Fortes")
        avisos.append(f"não deu para trazer o de/para do ID Fortes: {e}")

    with conexao() as conn:
        from .sincronizacao import _meta_gravar
        from .horario import agora
        _meta_gravar(conn, "cadastro_atualizado_em", agora().isoformat())
        _meta_gravar(conn, "cadastro_quantidade", str(gravadas))
        _meta_gravar(conn, "cadastro_avisos", " | ".join(avisos))

    logger.info("Análise de SPs: cadastro atualizado — %d pessoa(s), "
                "%d aviso(s).", gravadas, len(avisos))
    return {"pessoas": gravadas, "ignoradas": ignoradas, "avisos": avisos,
            "com_id_fortes": ficha.get("gravados", 0) + fortes.get("casados", 0)}


# ---------------------------------------------------------------------------
# O DE/PARA ID FORTES → CPF
# ---------------------------------------------------------------------------
def atualizar_ids_fortes(anotar=None, da_ficha: dict | None = None) -> dict:
    """Lê a aba "ID Fortes" e grava o código de cada pessoa no cadastro.

    ⚠️ RODA JUNTO com `atualizar`, no mesmo botão: duas atualizações separadas
    para a mesma planilha seria pedir para alguém esquecer uma delas — e a folha
    passaria a não achar gente que ESTÁ cadastrada, sem nada na tela explicando.

    NÃO APAGA O QUE JÁ ESTÁ GRAVADO quando a aba não vem ou está vazia: devolve
    aviso e deixa como está. Zerar o de/para por causa de uma aba renomeada faria
    a folha inteira virar "pendente de cadastro" de uma hora para outra.

    ⚠️ DESDE 30/09/2026 ELA SÓ COMPLETA. `da_ficha` (`{cpf: codigo}`) é o que
    veio na coluna "ID Fortes" da própria ficha — e a ficha manda: quem tem
    código lá não é tocado aqui, e quando as duas discordam a tela diz quem.

    Devolve `{"casados", "sem_cadastro", "repetidos", "divergentes", "avisos"}`.
    """
    from .db import conexao

    anotar = anotar or (lambda *a, **k: None)
    da_ficha = da_ficha or {}
    codigos_da_ficha = set(da_ficha.values())
    if not _pronto():
        return {"casados": 0, "sem_cadastro": [], "repetidos": [],
                "avisos": ["a tabela do cadastro ainda não existe."]}
    if not tem_id_fortes():
        return {"casados": 0, "sem_cadastro": [], "repetidos": [],
                "avisos": ['falta a atualização 030 do banco para guardar o ID '
                           'Fortes. Aperte "Aplicar atualizações do banco".']}

    anotar("trazendo o de/para do ID Fortes")
    try:
        aba = _aba(PLANILHA_COLABORADORES, ABA_ID_FORTES)
        valores = com_retry(lambda: aba.get_all_values()) or []
    except Exception as e:  # noqa: BLE001
        return {"casados": 0, "sem_cadastro": [], "repetidos": [],
                "avisos": [_explicar_aba(PLANILHA_COLABORADORES,
                                         ABA_ID_FORTES, e)]}

    # ESTA ABA É PEQUENA (~1.000 linhas por 8 colunas), então `get_all_values` é
    # aceitável aqui — ao contrário da aba principal, que tem 78 colunas e é lida
    # em faixas. O teto existe para o caso de a aba vir esticada por fórmula.
    if len(valores) > 20_000:
        valores = valores[:20_000]
    if not valores:
        return {"casados": 0, "sem_cadastro": [], "repetidos": [],
                "avisos": [f'a aba "{ABA_ID_FORTES}" está vazia — o de/para '
                           "anterior foi mantido."]}

    # O CABEÇALHO PODE NÃO ESTAR NA PRIMEIRA LINHA: esta aba tem título acima da
    # tabela em algumas versões. Procura nas primeiras linhas a que tem as duas
    # colunas, em vez de assumir a linha 1.
    posicoes = None
    for i, linha in enumerate(valores[:10]):
        normalizado = _normalizar_cabecalho(linha)
        i_id = achar_coluna(normalizado, COLUNAS_DO_ID_FORTES)
        i_cpf = achar_coluna(normalizado, COLUNAS_DO_CPF_NO_DE_PARA)
        if i_id is not None and i_cpf is not None:
            posicoes = (i, i_id, i_cpf)
            break
    if posicoes is None:
        cabecalhos = " / ".join(
            ", ".join(str(c) for c in linha if str(c).strip())
            for linha in valores[:3]) or "(vazio)"
        return {"casados": 0, "sem_cadastro": [], "repetidos": [],
                "avisos": [f'na aba "{ABA_ID_FORTES}" não achei as colunas de '
                           f'"{COLUNAS_DO_ID_FORTES[0]}" e "'
                           f'{COLUNAS_DO_CPF_NO_DE_PARA[0]}". As primeiras '
                           f"linhas dela são: {cabecalhos}. Me diga os nomes "
                           "certos e eu ajusto."]}

    linha_do_cabecalho, i_id, i_cpf = posicoes
    from .folha_rateio import so_digitos

    de_para: dict = {}
    repetidos: list = []
    for linha in valores[linha_do_cabecalho + 1:]:
        id_fortes = normalizar_id_fortes(
            linha[i_id] if i_id < len(linha) else "")
        cpf = so_digitos(linha[i_cpf] if i_cpf < len(linha) else "")
        if not id_fortes or len(cpf) != 11:
            continue
        if id_fortes in de_para and de_para[id_fortes] != cpf:
            # ⚠️ O MESMO CÓDIGO PARA DUAS PESSOAS é o pior erro possível aqui:
            # o salário de uma iria para a obra da outra. Não é resolvido
            # calado — vira crítica, e o primeiro vale (para não trocar o que
            # já estava certo por um duplicado digitado depois).
            repetidos.append(id_fortes)
            continue
        de_para[id_fortes] = cpf

    if not de_para:
        return {"casados": 0, "sem_cadastro": [], "repetidos": repetidos,
                "avisos": [f'a aba "{ABA_ID_FORTES}" tem as colunas certas, mas '
                           "nenhuma linha com código e CPF válido — o de/para "
                           "anterior foi mantido."]}

    # GRAVA SÓ EM QUEM ESTÁ NO CADASTRO. Um ID Fortes de alguém que não está
    # cadastrado não tem onde morar, e é notícia: a folha vai encontrar esse
    # código e não vai achar a pessoa.
    casados = 0
    sem_cadastro: list = []
    divergentes: list = []
    with conexao() as conn:
        for id_fortes, cpf in de_para.items():
            # A ficha manda: quem tem código lá não é tocado, e um código que a
            # ficha deu a OUTRA pessoa não é repetido aqui.
            if cpf in da_ficha or id_fortes in codigos_da_ficha:
                if da_ficha.get(cpf) != id_fortes:
                    divergentes.append(id_fortes)
                continue
            cur = conn.execute(
                "UPDATE analisesps.colaborador SET id_fortes = ? WHERE cpf = ?",
                (id_fortes, cpf))
            if cur.rowcount and cur.rowcount > 0:
                casados += 1
            else:
                sem_cadastro.append(id_fortes)
            cur.close()
        conn.commit()

    avisos = []
    if repetidos:
        avisos.append(
            f"{len(repetidos)} código(s) do Fortes aparecem para mais de uma "
            f"pessoa na aba \"{ABA_ID_FORTES}\" ({', '.join(repetidos[:5])}"
            f"{'…' if len(repetidos) > 5 else ''}). Enquanto isso não for "
            "corrigido, o salário de uma pode ir para a obra de outra.")
    if sem_cadastro:
        avisos.append(
            f"{len(sem_cadastro)} código(s) do Fortes são de gente que não está "
            "no cadastro. A folha vai encontrar esses códigos e não vai achar a "
            "pessoa.")
    if divergentes:
        avisos.append(
            f"{len(divergentes)} código(s) da aba \"{ABA_ID_FORTES}\" discordam "
            f"da ficha da pessoa ({', '.join(sorted(divergentes)[:5])}"
            f"{'…' if len(divergentes) > 5 else ''}). Valeu a ficha; vale a pena "
            "corrigir a aba para as duas não contarem histórias diferentes.")

    logger.info("Análise de SPs: de/para do ID Fortes — %d casado(s), "
                "%d sem cadastro, %d repetido(s).",
                casados, len(sem_cadastro), len(repetidos))
    return {"casados": casados, "sem_cadastro": sem_cadastro,
            "repetidos": repetidos, "divergentes": sorted(divergentes),
            "avisos": avisos}


def tem_id_fortes() -> bool:
    """A migração 030 já rodou? A coluna pode não existir ainda."""
    from .db import tem_coluna
    try:
        # ⚠️ SEM O SCHEMA NO NOME: `tem_coluna` já acrescenta o schema. Passar
        # "analisesps.colaborador" faz a consulta procurar uma tabela com esse
        # nome literal e responder SEMPRE que a coluna não existe — foi o que
        # aconteceu, e o teste da carga pegou.
        return tem_coluna("colaborador", "id_fortes")
    except Exception:  # noqa: BLE001
        return False


def de_para_do_fortes() -> dict:
    """`{"000013": {"cpf": ..., "nome": ...}}` — o que a apropriação pede.

    É a ponte entre a folha da contabilidade (que só traz o código) e todo o
    resto (que é por CPF). Ver `folha_apropriacao.apropriar`, parâmetro
    `cadastro_por_id`."""
    from .db import consultar
    if not _pronto() or not tem_id_fortes():
        return {}
    linhas = consultar(
        "SELECT id_fortes, cpf, nome FROM analisesps.colaborador "
        " WHERE id_fortes <> ''")
    # A chave sai normalizada (seis dígitos): um código gravado antes como
    # "4031" continua casando com o "004031" da folha.
    return {normalizar_id_fortes(l[0]): {"cpf": l[1], "nome": l[2]}
            for l in linhas}


# ---------------------------------------------------------------------------
# POR QUE ESTA PESSOA NÃO CASOU — o diagnóstico que a folha abre
#
# Ele, em 30/09/2026: *"aquele exemplo do Abraão continua aparecendo como não
# casaram com o cadastro (…) eu olhei a planilha e está lá. Como é que a gente
# resolve isso? Onde é que eu posso olhar? Como é que eu vejo o que o sistema está
# importando?"* A resposta é mostrar O QUE O SISTEMA TEM, e não o que deveria ter.
# ---------------------------------------------------------------------------
def por_que_nao_casou(id_fortes: str, nome: str) -> dict:
    """O que o cadastro guardado tem para este código e para este nome.

    Devolve `{"pelo_codigo": [...], "pelo_nome": [...], "atualizado": {...},
    "diagnostico": str}` — `diagnostico` é a frase que diz o que fazer.
    """
    from .db import consultar
    if not _pronto():
        return {"pelo_codigo": [], "pelo_nome": [], "atualizado": {},
                "diagnostico": "a tabela do cadastro ainda não existe."}
    codigo = normalizar_id_fortes(id_fortes)
    campos = "cpf, nome, " + ("id_fortes" if tem_id_fortes() else "''") + \
             ", fase, data_admissao"

    def _pessoa(l):
        return {"cpf": l[0], "nome": l[1], "id_fortes": l[2] or "",
                "fase": l[3] or "", "admissao": l[4]}

    pelo_codigo = []
    if codigo and tem_id_fortes():
        pelo_codigo = [_pessoa(l) for l in consultar(
            f"SELECT {campos} FROM analisesps.colaborador "
            " WHERE ltrim(id_fortes, '0') = ltrim(?, '0') AND id_fortes <> ''",
            (codigo,))]
    # Pelo nome: o primeiro e o último, para pegar "ABRAAO … NOGUEIRA" mesmo com
    # nome do meio abreviado ou diferente.
    partes = [p for p in " ".join(str(nome or "").split()).lower().split() if len(p) > 1]
    pelo_nome = []
    if partes:
        pelo_nome = [_pessoa(l) for l in consultar(
            f"SELECT {campos} FROM analisesps.colaborador "
            " WHERE lower(nome) LIKE ? AND lower(nome) LIKE ? "
            " ORDER BY nome LIMIT 10", (f"{partes[0]}%", f"%{partes[-1]}%"))]
    atualizado = quando_atualizou()

    if pelo_codigo:
        diagnostico = ("o código está no cadastro — a folha deve casar na próxima "
                       "vez que for aberta.")
    elif pelo_nome and all(not p["id_fortes"] for p in pelo_nome):
        diagnostico = (
            "a pessoa ESTÁ no cadastro, mas SEM o código do Fortes guardado. Se a "
            "ficha dela na planilha tem o código na coluna \"ID Fortes\", o "
            "cadastro foi atualizado antes de o sistema passar a ler essa coluna "
            "(30/09/2026) — aperte \"Atualizar cadastro\" em Colaboradores. Se "
            "depois disso continuar assim, a coluna dela está vazia na planilha.")
    elif pelo_nome:
        outros = ", ".join(sorted({p["id_fortes"] for p in pelo_nome if p["id_fortes"]}))
        diagnostico = (
            f"a pessoa está no cadastro com OUTRO código do Fortes ({outros}), e "
            f"a folha da contabilidade diz {codigo}. Um dos dois está errado: "
            "confira a ficha na planilha e o arquivo do Fortes.")
    else:
        diagnostico = (
            "não achei ninguém com esse nome no cadastro guardado. Ou a ficha não "
            "tem um CPF válido de 11 dígitos (sem CPF a linha é ignorada na carga), "
            "ou o cadastro não foi atualizado desde que ela entrou — aperte "
            "\"Atualizar cadastro\" em Colaboradores.")
    return {"codigo": codigo, "nome": nome, "pelo_codigo": pelo_codigo,
            "pelo_nome": pelo_nome, "atualizado": atualizado,
            "diagnostico": diagnostico}


# ---------------------------------------------------------------------------
# O que as telas perguntam
# ---------------------------------------------------------------------------
def quando_atualizou() -> dict:
    """De quando é a cópia. É o que a tela mostra ao lado do botão.

    Sem isto o botão seria uma caixa preta: aperta, algo acontece, e não há
    como saber se aconteceu. O dono já reclamou disso num botão que dizia
    "concluída" sem ter feito nada (10/09/2026)."""
    from .sincronizacao import _meta_ler
    from .db import conexao
    vazio = {"quando": "", "pessoas": 0, "avisos": [], "pronto": False}
    if not _pronto():
        return vazio
    try:
        with conexao() as conn:
            quando = _meta_ler(conn, "cadastro_atualizado_em", "")
            quantos = _meta_ler(conn, "cadastro_quantidade", "0")
            avisos = _meta_ler(conn, "cadastro_avisos", "")
    except Exception:  # noqa: BLE001 — a tela abre mesmo sem isto
        logger.exception("Análise de SPs: não consegui ler o estado do cadastro")
        return vazio
    return {
        "quando": quando,
        "pessoas": int(quantos) if str(quantos).strip().isdigit() else 0,
        "avisos": [a for a in (avisos or "").split(" | ") if a],
        "pronto": True,
    }


# ---------------------------------------------------------------------------
# QUEM SAIU, QUEM ESTÁ SAINDO, QUEM ESTÁ AFASTADO
#
# Pedido do dono em 27/09/2026, e é o cuidado mais caro desta área:
#
#   "Não podemos pagar esse tipo de verba indenizatória ou ainda pagar salário
#    ou diárias pra quem saiu, tá saindo. Tem que ter cuidados e alerta."
#
# ⚠️ POR QUE ISTO JÁ DÁ PARA FAZER, sem esperar o relatório que ele vai mandar:
# o cadastro que vem do Pipefy já traz **data do aviso prévio, último dia
# trabalhado, data de saída e a fase**. O relatório de demissão que ele
# mencionou será uma SEGUNDA fonte, para conferir uma contra a outra — não é a
# primeira.
#
# A FASE MANDA, e não fui eu quem decidiu: as folhas de alimentação e de
# transporte já excluem `Colaboradores Desligados` e `Colaboradores Afastados`
# pela fase (lido das fórmulas, ver docs/FOLHA_DE_PAGAMENTO.md §7.14.6). Fazer
# diferente aqui criaria duas respostas para a mesma pergunta.
#
# O DESACORDO ENTRE OS SINAIS É NOTÍCIA, não é para ser resolvido calado. Fase
# dizendo desligado sem data de saída, ou último dia já passado sem saída
# lançada, é cadastro pela metade — e cadastro pela metade é como se paga quem
# já saiu. Quem tem desacordo continua APARECENDO na lista, marcado. Esconder
# o caso inconsistente é o erro que o dono já me corrigiu em 26/09/2026:
# *"não pode ficar oculto, escondido."*
# ---------------------------------------------------------------------------
SITUACAO_ATIVO = "ativo"
SITUACAO_SAINDO = "saindo"
SITUACAO_SAIU = "saiu"
SITUACAO_AFASTADO = "afastado"

# Os valores que a coluna "Fase Atual" usa no Pipefy. São os mesmos textos que
# as abas de alimentação e transporte comparam.
FASE_DESLIGADO = "colaboradores desligados"
FASE_AFASTADO = "colaboradores afastados"

# ⚠️ A FASE CASA POR PEDAÇO, NÃO POR IGUALDADE — e isto é correção de 29/09/2026.
# O dono, pela segunda vez: *"você continua exibindo Colaboradores Desligados na
# tela de cadastro. Esses devem aparecer ocultos. Eu já havia dito isso."*
#
# A comparação era `lower(fase) = 'colaboradores desligados'`, exata. Qualquer
# variação na planilha — "Desligados", "Colaborador Desligado", um espaço a mais,
# "Colaboradores Desligados " — deixava de casar, e a pessoa voltava para a lista
# sem nada avisando. Procurar o PEDAÇO ("desligad", "afastad") cobre as variações
# de plural e de singular de uma vez, e é o que um cadastro digitado por gente
# exige.
PEDACO_DESLIGADO = "desligad"
PEDACO_AFASTADO = "afastad"


def fase_diz_desligado(fase) -> bool:
    """A fase do Pipefy indica desligamento?

    ⚠️ UM LUGAR SÓ, de propósito. O `WHERE` que ESCONDE da lista e a regra que
    CLASSIFICA a pessoa tinham de concordar sempre — e não concordavam: o WHERE
    usava igualdade exata e a classificação também, mas eu corrigi um e quase
    deixei o outro. Se divergirem, a tela esconde alguém que ela mesma diria estar
    ativo, ou pior, mostra como ativo quem ela esconde do pagamento."""
    return PEDACO_DESLIGADO in " ".join(str(fase or "").split()).lower()


def fase_diz_afastado(fase) -> bool:
    """A fase do Pipefy indica afastamento? Ver `fase_diz_desligado`."""
    return PEDACO_AFASTADO in " ".join(str(fase or "").split()).lower()


def _hoje():
    from .horario import agora
    return agora().date()


def situacao_no_pagamento(ficha: dict, ate=None) -> dict:
    """Esta pessoa pode receber, considerando um pagamento até `ate`?

    `ate` é o ÚLTIMO DIA DO PERÍODO que se está pagando — não é hoje. Pagar a
    quinzena de 1 a 15 no dia 20 é normal; quem saiu no dia 18 trabalhou a
    quinzena inteira e recebe. Usar "hoje" no lugar do fim do período
    bloquearia um pagamento devido.

    Devolve:
      situacao  — ativo / saindo / saiu / afastado
      motivo    — a frase que vai para a tela, em português
      trava     — True quando NÃO se deve gerar pagamento de folha, diária ou
                  auxílio para esta pessoa neste período
      desacordo — "" ou a frase que descreve sinais que se contradizem
    """
    ate = ate or _hoje()
    fase = " ".join(str(ficha.get("fase") or "").split()).lower()
    saida = ficha.get("data_saida")
    ultimo = ficha.get("ultimo_dia")
    aviso = ficha.get("aviso_previo")

    desacordo = ""
    if fase_diz_desligado(fase) and not saida:
        desacordo = ("a fase no Pipefy diz desligado, mas o cadastro não tem "
                     "data de saída — confira antes de pagar.")
    elif saida and fase and not fase_diz_desligado(fase):
        desacordo = (f"o cadastro tem data de saída, mas a fase no Pipefy "
                     f"ainda diz \"{ficha.get('fase')}\".")
    elif ultimo and not saida and ultimo <= ate:
        desacordo = ("o último dia trabalhado já passou e não há data de saída "
                     "lançada — o desligamento está pela metade.")

    def resposta(situacao, motivo, trava):
        return {"situacao": situacao, "motivo": motivo, "trava": trava,
                "desacordo": desacordo}

    # 1. JÁ SAIU — nada de folha, diária ou auxílio novo.
    if saida and saida <= ate:
        return resposta(
            SITUACAO_SAIU,
            f"saiu em {saida.strftime('%d/%m/%Y')}. Não pague folha, diária "
            "nem auxílio deste período por aqui.",
            True)
    if fase_diz_desligado(fase):
        return resposta(
            SITUACAO_SAIU,
            "a fase no Pipefy diz desligado. Não pague por aqui até "
            "confirmar.",
            True)

    # 2. ESTÁ SAINDO — pode haver valor devido, mas não o período inteiro, e
    #    verba indenizatória (rescisão) NÃO sai por este caminho.
    if saida and saida > ate:
        return resposta(
            SITUACAO_SAINDO,
            f"sai em {saida.strftime('%d/%m/%Y')}. Confira o que é devido só "
            "até lá; rescisão não se paga por aqui.",
            False)
    if ultimo and ultimo <= ate:
        return resposta(
            SITUACAO_SAINDO,
            f"o último dia trabalhado foi {ultimo.strftime('%d/%m/%Y')}. "
            "Confira o que é devido só até lá.",
            False)
    if ultimo:
        return resposta(
            SITUACAO_SAINDO,
            f"último dia previsto em {ultimo.strftime('%d/%m/%Y')}.",
            False)
    if aviso:
        return resposta(
            SITUACAO_SAINDO,
            f"aviso prévio em {aviso.strftime('%d/%m/%Y')} — está saindo.",
            False)

    # 3. AFASTADO — não recebe auxílio alimentação nem transporte. É a mesma
    #    exclusão que as abas da planilha já fazem pela fase.
    if fase_diz_afastado(fase):
        return resposta(
            SITUACAO_AFASTADO,
            "está afastado. Não pague auxílio alimentação nem transporte.",
            True)

    return resposta(SITUACAO_ATIVO, "", False)


def _dicionario(linha, ate=None) -> dict:
    from .folha_rateio import cpf_bonito

    registro = {campo: linha[i] for i, campo in enumerate(CAMPOS_LIDOS)}
    registro["link_pipefy"] = link_do_card(registro.get("card_pipefy"))
    # ⚠️ O CPF SAI PONTUADO PARA A TELA. Pedido do dono em 28/09/2026: *"o CPF
    # não está com a pontuação, isso facilita visualmente."* O banco continua
    # guardando só dígitos — quem compara compara dígito, quem lê lê pontuado.
    registro["cpf_bonito"] = cpf_bonito(registro.get("cpf"))
    registro["desligado"] = registro.get("data_saida") is not None
    registro.update(situacao_no_pagamento(registro, ate))
    # `alerta` é o que a tela usa para decidir se destaca a linha: qualquer
    # coisa que não seja "ativo e sem contradição" merece ser vista.
    registro["alerta"] = (registro["situacao"] != SITUACAO_ATIVO
                          or bool(registro["desacordo"]))
    return registro


def por_cpf(cpf: str, ate=None) -> dict | None:
    """Uma pessoa, pelo CPF. None quando não está no cadastro."""
    from .db import consultar_um
    from .folha_rateio import so_digitos
    if not _pronto():
        return None
    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        return None
    linha = consultar_um(
        "SELECT " + ", ".join(CAMPOS_LIDOS) + " FROM analisesps.colaborador "
        " WHERE cpf = ?", (digitos,))
    return _dicionario(linha, ate) if linha else None


def muitos_por_cpf(cpfs, ate=None) -> dict:
    """Vários de uma vez, para a tela não consultar um por um.

    A tela de Rateio mostra dezenas de pessoas; uma consulta por pessoa faria
    dezenas de idas ao banco a cada visita."""
    from .db import consultar
    from .folha_rateio import so_digitos
    if not _pronto():
        return {}
    limpos = sorted({so_digitos(c) for c in (cpfs or [])
                     if len(so_digitos(c)) == 11})
    if not limpos:
        return {}
    marcadores = ", ".join(["?"] * len(limpos))
    linhas = consultar(
        "SELECT " + ", ".join(CAMPOS_LIDOS) + " FROM analisesps.colaborador "
        f" WHERE cpf IN ({marcadores})", tuple(limpos))
    return {l[0]: _dicionario(l, ate) for l in linhas}


def buscar(texto: str = "", so_ativos: bool = True, teto: int = 200,
           so_saindo: bool = False, ate=None, fase: str = "",
           admitido_de=None, admitido_ate=None) -> list:
    """Procura por nome ou por CPF. Lista curta, para caixa de busca.

    `teto` existe porque o cadastro tem ~3.500 pessoas e desenhar tudo numa
    tela não ajuda ninguém."""
    from .db import consultar
    from .folha_rateio import so_digitos
    if not _pronto():
        return []

    condicoes = []
    params: list = []
    procurado = " ".join(str(texto or "").split())
    if procurado:
        digitos = so_digitos(procurado)
        if len(digitos) >= 3:
            condicoes.append("(lower(nome) LIKE ? OR cpf LIKE ?)")
            params += [f"%{procurado.lower()}%", f"%{digitos}%"]
        else:
            condicoes.append("lower(nome) LIKE ?")
            params.append(f"%{procurado.lower()}%")
    if so_ativos:
        # ⚠️ ESTA REGRA MUDOU EM 28/09/2026, e as duas versões são dele.
        #
        # Em 26/09 ele disse, sobre quem tem a FASE dizendo desligado mas sem data
        # de saída: *"não pode ficar oculto, escondido"* — e eu passei a esconder
        # só quem tinha data de saída.
        #
        # Em 28/09, vendo a tela: *"o que é colaborador desligado (…) não deveria
        # nem estar sendo exibido. Ele está desligado, ele não está trabalhando."*
        #
        # A leitura que concilia as duas: a lista do dia a dia é de quem está
        # trabalhando, e quem está desligado sai dela — MAS não desaparece do
        # sistema. A tela conta quantos foram escondidos e tem um clique para
        # trazê-los. Esconder e não dizer é que seria o erro de 26/09.
        condicoes.append("data_saida IS NULL")
        condicoes.append("lower(coalesce(fase, '')) NOT LIKE ?")
        condicoes.append("lower(coalesce(fase, '')) NOT LIKE ?")
        params += [f"%{PEDACO_DESLIGADO}%", f"%{PEDACO_AFASTADO}%"]
    if so_saindo:
        # Qualquer sinal de saída. A conta fina de quem está saindo × quem já
        # saiu é de `situacao_no_pagamento`; aqui só se traz quem tem sinal.
        condicoes.append(
            "(data_saida IS NOT NULL OR ultimo_dia IS NOT NULL "
            " OR aviso_previo IS NOT NULL "
            " OR lower(coalesce(fase, '')) LIKE ? "
            " OR lower(coalesce(fase, '')) LIKE ?)")
        params += [f"%{PEDACO_DESLIGADO}%", f"%{PEDACO_AFASTADO}%"]

    # ⚠️ A FASE ATUAL É O CORTE MAIS USADO — *"a Fase Atual, coluna AX, é super
    # importante"* (dono, 29/09/2026). Casa exato pelo texto que a tela ofereceu, e
    # a tela oferece só o que existe no banco.
    if fase:
        if fase == "(sem fase)":
            condicoes.append("coalesce(btrim(fase), '') = ''")
        else:
            condicoes.append("btrim(fase) = ?")
            params.append(fase.strip())
    # E as datas, que ele pediu junto: *"tem tanta informação nos dados que podemos
    # usar também como filtro. Datas por exemplo."* A de ADMISSÃO é a que recorta
    # "quem entrou neste mês", que é a pergunta do dia a dia.
    if admitido_de:
        condicoes.append("data_admissao >= ?")
        params.append(admitido_de)
    if admitido_ate:
        condicoes.append("data_admissao <= ?")
        params.append(admitido_ate)

    onde = (" WHERE " + " AND ".join(condicoes)) if condicoes else ""
    linhas = consultar(
        "SELECT " + ", ".join(CAMPOS_LIDOS) + " FROM analisesps.colaborador "
        + onde + " ORDER BY nome LIMIT ?", tuple(params) + (int(teto),))
    return [_dicionario(l, ate) for l in linhas]


def contar_quem_esta_saindo(ate=None) -> dict:
    """Quantas pessoas têm sinal de saída ou de afastamento no cadastro.

    POR QUE UMA CONTA À PARTE, em vez de contar a lista da tela: a lista tem
    teto de 200. Um número que só conta o que caberia na tela é pior que número
    nenhum — ele diria "3 saindo" havendo trinta.

    Conta pelos SINAIS (data ou fase); a classificação fina de cada pessoa é de
    `situacao_no_pagamento`, que precisa do fim do período."""
    from .db import consultar_um
    if not _pronto():
        return {"com_sinal": 0, "saiu": 0, "afastado": 0}
    ate = ate or _hoje()
    linha = consultar_um(
        "SELECT "
        "  sum(CASE WHEN data_saida IS NOT NULL OR ultimo_dia IS NOT NULL "
        "            OR aviso_previo IS NOT NULL "
        "            OR lower(coalesce(fase, '')) LIKE ? "
        "            OR lower(coalesce(fase, '')) LIKE ? THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN (data_saida IS NOT NULL AND data_saida <= ?) "
        "            OR lower(coalesce(fase, '')) LIKE ? THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN lower(coalesce(fase, '')) LIKE ? THEN 1 ELSE 0 END) "
        " FROM analisesps.colaborador",
        (f"%{PEDACO_DESLIGADO}%", f"%{PEDACO_AFASTADO}%", ate,
         f"%{PEDACO_DESLIGADO}%", f"%{PEDACO_AFASTADO}%"))
    com_sinal, saiu, afastado = (linha or (0, 0, 0))
    return {"com_sinal": int(com_sinal or 0), "saiu": int(saiu or 0),
            "afastado": int(afastado or 0)}


# ---------------------------------------------------------------------------
# O CÓDIGO DA OBRA
# ---------------------------------------------------------------------------
def codigos_das_obras() -> dict:
    """`{NOME DA OBRA: código}`, das referências do rateio.

    ⚠️ EXISTE PARA O CADASTRO ANTIGO. O dono pediu que as telas mostrem o CÓDIGO
    da obra, não o nome por extenso — e o código passou a ser lido do cadastro
    (migração 036). Mas o cadastro só ganha o código na PRÓXIMA atualização, e
    algumas linhas podem nunca tê-lo. Nesses casos dá para chegar nele pelo nome,
    que é o que esta função permite.

    Uma consulta só, e o resultado é usado para a lista inteira — resolver obra
    por obra faria uma ida ao banco por pessoa."""
    try:
        from . import sincronizacao
        obras = (sincronizacao.referencias_rateio() or {}).get("obras") or []
    except Exception:  # noqa: BLE001 — é apoio: sem ele mostra-se o que há
        logger.exception("Colaboradores: não consegui ler os códigos das obras")
        return {}
    return {" ".join(str(o.get("nome") or "").split()).upper(): str(o.get("codigo") or "")
            for o in obras if o.get("nome")}


def resolver_obra(ficha: dict, por_nome: dict | None = None) -> str:
    """O código da obra desta pessoa, do jeito mais confiável disponível.

    A ordem: o código que o cadastro traz; senão, o código da obra cujo NOME
    bate. Devolve "" quando não há como saber — e "" na tela é uma pergunta
    aberta, que é melhor do que um nome por extenso no lugar de um código."""
    codigo = " ".join(str(ficha.get("obra_codigo") or "").split())
    if codigo:
        return codigo
    nome = " ".join(str(ficha.get("obra_cadastro") or "").split()).upper()
    return (por_nome or {}).get(nome, "")


def panorama() -> dict:
    """Os números do cadastro, para o quadro do alto da tela.

    ⚠️ Pedido do dono em 29/09/2026: *"3531 pessoa(s) trazidas da planilha em 26/09
    às 18:35. Isso vai aparecer sempre assim? Não tem nada de KPI essa tela."*

    Ele tem razão: a tela dizia só quantas linhas vieram e quando. Isso é registro
    de carga, não informação de trabalho. O que decide o que ele faz é: quantos
    estão trabalhando, quantos têm cada auxílio, e **quantos estão com o cadastro
    pela metade** — porque é isso que trava pagamento.

    Uma consulta só: são ~3.500 linhas e a tela abre a cada filtro."""
    from .db import consultar_um
    if not _pronto():
        return {"pronto": False}
    linha = consultar_um(
        "SELECT count(*), "
        "  sum(CASE WHEN data_saida IS NULL "
        "        AND lower(coalesce(fase, '')) NOT LIKE ? "
        "        AND lower(coalesce(fase, '')) NOT LIKE ? THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN valor_alimentacao IS NOT NULL "
        "        OR coalesce(modo_alimentacao, '') <> '' THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN valor_transporte IS NOT NULL "
        "        OR coalesce(modo_transporte, '') <> '' THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN coalesce(card_pipefy, '') = '' THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN coalesce(obra_codigo, '') = '' THEN 1 ELSE 0 END), "
        "  sum(CASE WHEN coalesce(id_fortes, '') = '' THEN 1 ELSE 0 END) "
        " FROM analisesps.colaborador",
        (f"%{PEDACO_DESLIGADO}%", f"%{PEDACO_AFASTADO}%"))
    total, ativos, com_ali, com_tra, sem_card, sem_obra, sem_fortes = (
        linha or (0,) * 7)
    return {
        "pronto": True,
        "total": int(total or 0), "ativos": int(ativos or 0),
        "com_alimentacao": int(com_ali or 0), "com_transporte": int(com_tra or 0),
        "sem_card": int(sem_card or 0), "sem_obra": int(sem_obra or 0),
        "sem_id_fortes": int(sem_fortes or 0),
    }


def fases() -> list:
    """As Fases Atuais que existem no cadastro, com quantos em cada.

    ⚠️ Pedido dele em 29/09/2026: *"havia falado que a Fase Atual, coluna AX, é
    super importante. Não tem isso como filtro em colaboradores."*

    É a coluna que diz em que ponto do processo a pessoa está no Pipefy — e por isso
    é o corte mais usado. Sai do banco, não de uma lista escrita à mão: fase nova no
    Pipefy aparece aqui sozinha."""
    from .db import consultar
    if not _pronto():
        return []
    linhas = consultar(
        "SELECT coalesce(NULLIF(btrim(fase), ''), '(sem fase)'), count(*) "
        "  FROM analisesps.colaborador GROUP BY 1 ORDER BY count(*) DESC")
    return [{"fase": l[0], "quantos": int(l[1] or 0)} for l in linhas]
