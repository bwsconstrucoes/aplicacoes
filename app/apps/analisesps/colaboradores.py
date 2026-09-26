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
    "obra_cadastro": ["Objeto Obra [ ]", "Objeto Obra", "Código da Obra"],
    "valor_gratificacao": ["Valor da Gratificação", "Valor da Gratificacao"],
    "parcela_unica": ["Recebe Parcela Única", "Recebe Parcela Unica"],
    "aviso_previo": ["Data do Aviso Prévio", "Data do Aviso Previo"],
    "ultimo_dia": ["Último dia Trabalhado", "Ultimo dia Trabalhado"],
    "data_saida": ["Data de Saída", "Data de Saida"],
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
COLUNAS_DOS_AUXILIOS = {
    "modo_alimentacao": ["Modalidade Auxílio Alimentação",
                         "Modalidade Alimentação", "Auxílio Alimentação",
                         "Tipo Auxílio Alimentação", "Alimentação"],
    "valor_alimentacao": ["Valor Auxílio Alimentação",
                          "Valor do Auxílio Alimentação",
                          "Valor Alimentação"],
    "modo_transporte": ["Modalidade Auxílio Transporte",
                        "Modalidade Transporte", "Auxílio Transporte",
                        "Tipo Auxílio Transporte", "Transporte"],
    "valor_transporte": ["Valor Auxílio Transporte",
                         "Valor do Auxílio Transporte",
                         "Valor Transporte"],
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
} | {"modo_alimentacao", "valor_alimentacao",
     "modo_transporte", "valor_transporte", "paga_por_beevale"})

DATAS = ("aviso_previo", "ultimo_dia", "data_saida")
NUMEROS = ("valor_alimentacao", "valor_transporte", "valor_gratificacao")

# Os campos que a tela pede sem parar, na ordem em que a tabela os guarda.
CAMPOS = ["cpf", "nome", "card_pipefy", "matricula", "celular", "cargo",
          "tipo", "tipo_contrato", "fase", "convencao", "obra_cadastro",
          "valor_alimentacao", "modo_alimentacao",
          "valor_transporte", "modo_transporte", "valor_gratificacao",
          "parcela_unica", "paga_por_beevale",
          "aviso_previo", "ultimo_dia", "data_saida"]


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
            avisos.append(
                f'não achei a coluna de "{aceitos[0]}" — este campo vai ficar '
                f"em branco. Me diga o nome exato dela na planilha.")
        # else: fica em branco calado. Ver COLUNAS_QUE_AVISAM.

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

    registro = {"cpf": cpf}
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

    with conexao() as conn:
        from .sincronizacao import _meta_gravar
        from .horario import agora
        _meta_gravar(conn, "cadastro_atualizado_em", agora().isoformat())
        _meta_gravar(conn, "cadastro_quantidade", str(gravadas))
        _meta_gravar(conn, "cadastro_avisos", " | ".join(avisos))

    logger.info("Análise de SPs: cadastro atualizado — %d pessoa(s), "
                "%d aviso(s).", gravadas, len(avisos))
    return {"pessoas": gravadas, "ignoradas": ignoradas, "avisos": avisos}


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


def _dicionario(linha) -> dict:
    registro = {campo: linha[i] for i, campo in enumerate(CAMPOS)}
    registro["link_pipefy"] = link_do_card(registro.get("card_pipefy"))
    registro["desligado"] = registro.get("data_saida") is not None
    return registro


def por_cpf(cpf: str) -> dict | None:
    """Uma pessoa, pelo CPF. None quando não está no cadastro."""
    from .db import consultar_um
    from .folha_rateio import so_digitos
    if not _pronto():
        return None
    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        return None
    linha = consultar_um(
        "SELECT " + ", ".join(CAMPOS) + " FROM analisesps.colaborador "
        " WHERE cpf = ?", (digitos,))
    return _dicionario(linha) if linha else None


def muitos_por_cpf(cpfs) -> dict:
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
        "SELECT " + ", ".join(CAMPOS) + " FROM analisesps.colaborador "
        f" WHERE cpf IN ({marcadores})", tuple(limpos))
    return {l[0]: _dicionario(l) for l in linhas}


def buscar(texto: str = "", so_ativos: bool = True, teto: int = 200) -> list:
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
        condicoes.append("data_saida IS NULL")

    onde = (" WHERE " + " AND ".join(condicoes)) if condicoes else ""
    linhas = consultar(
        "SELECT " + ", ".join(CAMPOS) + " FROM analisesps.colaborador "
        + onde + " ORDER BY nome LIMIT ?", tuple(params) + (int(teto),))
    return [_dicionario(l) for l in linhas]
