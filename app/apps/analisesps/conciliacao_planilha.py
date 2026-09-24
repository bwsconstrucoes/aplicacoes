# -*- coding: utf-8 -*-
"""
A LEITURA DA PLANILHA "Controle de Conciliação" — o histórico que já existe.

O dono mantém uma aba por conta desde 2024, e disse o que importa:
*"já tem muita informação aqui, eu quero manter"*. Este módulo lê uma aba e
devolve as linhas ENTENDIDAS, sem gravar nada — quem grava é a tela, depois
que ele conferiu a amostra.

⚠️ CADA BANCO ESCREVE DE UM JEITO, e as cinco formas que ele mandou estão
todas aqui. Por isso as colunas são achadas pelo NOME do cabeçalho, e não pela
posição: a posição muda de aba para aba, o nome quase não muda.

    Bradesco   DATA | DESCRIÇÃO | TIPO | CRÉDITO | DEBITO | SALDO | … | Conciliado
    BB         Data | Lançamento | Detalhes | N° documento | Valor | Tipo Lançamento
    Santander  Data | Histórico | Documento | Valor (R$) | Saldo (R$)
    Sicredi    Data | Descrição | Tipo | Valor | Saldo

⚠️ TRÊS ARMADILHAS QUE OS EXEMPLOS DELE REVELARAM, e que um leitor ingênuo
teria engolido caladas:

1. **"TIPO" QUER DIZER DUAS COISAS.** No Bradesco a coluna Tipo traz o NÚMERO
   do documento (1798917); no Sicredi traz o TIPO do lançamento (PIX_DEB). A
   decisão é pelo conteúdo, não pelo nome.
2. **O BB TEM LINHAS QUE NÃO SÃO LANÇAMENTO** — "Saldo Anterior" e "Saldo do
   dia", com valor 0,00. Ele avisou: *"isso aí é ignorável, nem para entrar"*.
   Importá-las encheria o extrato de linhas falsas e ainda assim somaria
   zero — ninguém notaria olhando o saldo.
3. **O SANTANDER REPETE O BLOCO DE COLUNAS** (Data|Histórico|Documento|Valor|
   Saldo aparece duas vezes na mesma linha de cabeçalho). Ler os dois blocos
   como se fossem um duplicaria tudo. Vale o PRIMEIRO, e o segundo é avisado
   se tiver dado.

⚠️ NÃO SE LÊ COR. Até 24/09/2026 o "conciliado" de várias abas era uma célula
pintada de amarelo — e ler cor exige outra API do Google, mais cara e mais
frágil. O dono resolveu isso na origem: padronizou a palavra "Conciliado" em
texto, na última coluna. Este leitor depende disso, e é de propósito.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal

logger = logging.getLogger("analisesps.conciliacao")

# Quantas linhas de uma aba se lê de uma vez. As abas dele têm alguns
# milhares; o teto existe porque este serviço já morreu de falta de memória.
MAX_LINHAS = 30000

# Quantas linhas da amostra a tela mostra antes de o dono confirmar.
AMOSTRA = 12


class ErroDaPlanilha(RuntimeError):
    """Aba ilegível ou fora do padrão. A mensagem já vai para a tela."""


# O que NÃO é lançamento. Ele apontou o caso do BB; a lista vale para todos,
# porque saldo é resultado e não movimento.
_NAO_E_LANCAMENTO = (
    "saldo anterior", "saldo do dia", "saldo atual", "saldo final",
    "saldo em conta", "s a l d o", "total do dia", "saldo bloqueado",
)

_CABECALHOS = {
    "data": ("data", "dt"),
    "descricao": ("descrição", "descricao", "histórico", "historico",
                  "lançamento", "lancamento"),
    "detalhe": ("detalhes", "detalhe", "complemento"),
    "documento": ("documento", "n° documento", "nº documento", "no documento",
                  "num documento", "número documento"),
    "tipo": ("tipo", "tipo lançamento", "tipo lancamento"),
    "credito": ("crédito", "credito"),
    "debito": ("débito", "debito", "debíto"),
    "valor": ("valor", "valor (r$)", "valor r$"),
    "saldo": ("saldo", "saldo (r$)", "saldo r$"),
    "conciliado": ("conciliado", "status conciliação", "status conciliacao",
                   "conciliação", "conciliacao"),
}


def _limpo(texto) -> str:
    return re.sub(r"\s+", " ", str(texto or "")).strip()


def _chave(texto) -> str:
    return _limpo(texto).lower()


def achar_cabecalho(valores: list) -> tuple[int, dict, list]:
    """Onde está o cabeçalho e o que cada coluna é.

    Devolve `(indice_da_linha, {papel: coluna}, sobras)`. `sobras` são as
    colunas que não têm papel conhecido E têm cabeçalho — é delas que sai a
    observação, como o dono pediu: *"algum dado que tenha entre as colunas da
    parte numérica e a coluna L, a gente vai colocar como observação"*.

    ⚠️ O CABEÇALHO NÃO É SEMPRE A PRIMEIRA LINHA. Algumas abas têm título ou
    linha em branco antes. Procura-se nas 10 primeiras a que tenha "data" e
    pelo menos uma coluna de valor — exigir só "data" acharia qualquer coisa.
    """
    for i, linha in enumerate(valores[:10]):
        papeis: dict = {}
        for col, bruto in enumerate(linha):
            nome = _chave(bruto)
            if not nome:
                continue
            for papel, apelidos in _CABECALHOS.items():
                if papel in papeis:      # ⚠️ o PRIMEIRO bloco manda: o
                    continue             # Santander repete as colunas todas
                if nome in apelidos:
                    papeis[papel] = col
                    break
        tem_valor = ("valor" in papeis or "credito" in papeis
                     or "debito" in papeis)
        if "data" in papeis and tem_valor:
            return i, papeis, _colunas_de_observacao(linha, papeis)
    raise ErroDaPlanilha(
        "Não achei o cabeçalho desta aba. Ele precisa ter uma coluna de "
        "DATA e uma de VALOR (ou CRÉDITO e DÉBITO) nas dez primeiras linhas.")


def letra_da_coluna(indice: int) -> str:
    """0 -> "A", 7 -> "H". Para nomear coluna que não tem cabeçalho."""
    nome = ""
    indice += 1
    while indice:
        indice, resto = divmod(indice - 1, 26)
        nome = chr(65 + resto) + nome
    return nome


def _colunas_de_observacao(cabecalho: list, papeis: dict) -> list:
    """Quais colunas viram OBSERVAÇÃO do lançamento.

    ⚠️ ESTA REGRA FOI CORRIGIDA EM 24/09/2026, e o defeito era grave: nenhuma
    observação foi importada na primeira vez. A regra antiga só olhava colunas
    com CABEÇALHO preenchido — e nas abas do Bradesco dele as colunas de
    anotação (H, J, K) **não têm cabeçalho nenhum**. O sistema importou tudo,
    disse que deu certo, e deixou dois anos de anotação para trás em silêncio.

    A regra dele, com todas as letras: *"a coluna subsequente ao último dado
    (...) porque tem uns que o último dado, o saldo, fica numa coluna e outra
    coluna. Então seria a informação subsequente."*

    Ou seja: **tudo o que vem DEPOIS da última coluna de dado e ANTES da
    coluna de Conciliado**, com ou sem cabeçalho. Coluna sem nome é chamada
    pela letra dela na planilha ("H"), para a observação dizer de onde veio.
    """
    de_dado = [papeis[p] for p in
               ("data", "descricao", "detalhe", "documento", "tipo",
                "credito", "debito", "valor", "saldo") if p in papeis]
    if not de_dado:
        return []
    depois_do_ultimo = max(de_dado)
    # A coluna de Conciliado fecha o intervalo. Sem ela, vale até o fim.
    fim = papeis.get("conciliado", len(cabecalho))

    sobras = []
    for col in range(depois_do_ultimo + 1, max(fim, depois_do_ultimo + 1)):
        rotulo = _limpo(cabecalho[col]) if col < len(cabecalho) else ""
        sobras.append((col, rotulo or f"coluna {letra_da_coluna(col)}"))

    # E uma coluna com cabeçalho que tenha sobrado NO MEIO dos dados também
    # conta: é anotação igual, só está em lugar incomum.
    usadas = set(papeis.values()) | {c for c, _ in sobras}
    for col in range(depois_do_ultimo):
        rotulo = _limpo(cabecalho[col]) if col < len(cabecalho) else ""
        if col not in usadas and rotulo:
            sobras.append((col, rotulo))
    return sorted(sobras)


def _data(bruto) -> date | None:
    texto = _limpo(bruto)
    if not texto:
        return None
    for formato in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    return None


def _numero(bruto):
    """"-3.374,40" -> Decimal("-3374.40"). Devolve None quando não é número."""
    texto = _limpo(bruto).replace("R$", "").strip()
    if not texto:
        return None
    negativo = texto.startswith("-") or (texto.startswith("(")
                                         and texto.endswith(")"))
    texto = re.sub(r"[^\d,.]", "", texto)
    if not texto:
        return None
    # Formato do Brasil: ponto separa milhar, vírgula separa centavos.
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        valor = Decimal(texto)
    except Exception:  # noqa: BLE001 — célula com lixo não derruba a aba
        return None
    return -valor if (negativo and valor > 0) else valor


def _e_documento(valores: list) -> bool:
    """A coluna "Tipo" traz NÚMERO de documento ou TIPO de lançamento?

    ⚠️ Decidido pelo CONTEÚDO, porque o nome mente: no Bradesco "Tipo" é
    1798917 (o documento); no Sicredi é PIX_DEB (o tipo). Olhar só o nome
    jogaria o número do documento fora numa aba e o texto "PIX_DEB" no campo
    de documento na outra.
    """
    cheios = [v for v in valores if _limpo(v)][:40]
    if not cheios:
        return False
    numericos = sum(1 for v in cheios if re.fullmatch(r"[\d\s./-]+", _limpo(v)))
    return numericos >= max(1, int(len(cheios) * 0.8))


def interpretar(valores: list, aba: str = "") -> dict:
    """Uma aba inteira, entendida. NÃO grava nada.

    Devolve as linhas prontas para virar extrato, mais o que foi descartado e
    por quê — porque "importei 812 de 840" sem dizer o que houve com as 28 é
    a informação pela metade que faz desconfiar do resultado inteiro.
    """
    if not valores:
        raise ErroDaPlanilha("A aba está vazia.")
    if len(valores) > MAX_LINHAS:
        raise ErroDaPlanilha(
            f"A aba tem {len(valores)} linhas, acima do teto de {MAX_LINHAS}.")

    inicio, papeis, sobras = achar_cabecalho(valores)
    corpo = valores[inicio + 1:]

    # A coluna "Tipo" só vira documento se o conteúdo dela for número.
    col_tipo = papeis.get("tipo")
    tipo_e_documento = False
    if col_tipo is not None and "documento" not in papeis:
        tipo_e_documento = _e_documento(
            [l[col_tipo] for l in corpo if len(l) > col_tipo])

    def celula(linha, papel):
        col = papeis.get(papel)
        if col is None or len(linha) <= col:
            return ""
        return _limpo(linha[col])

    linhas, descartadas = [], []
    for numero, linha in enumerate(corpo, start=inicio + 2):
        if not any(_limpo(c) for c in linha):
            continue                     # linha em branco não é descarte

        quando = _data(celula(linha, "data"))
        descricao = celula(linha, "descricao")
        detalhe = celula(linha, "detalhe")
        if detalhe and detalhe.lower() not in descricao.lower():
            descricao = f"{descricao} — {detalhe}".strip(" —")

        # ⚠️ AS LINHAS QUE NÃO SÃO LANÇAMENTO. "Saldo do dia" com valor 0,00
        # entraria calada e encheria o extrato de linha falsa.
        if any(m in descricao.lower() for m in _NAO_E_LANCAMENTO):
            descartadas.append({"linha": numero, "porque": "é saldo, não lançamento",
                                "texto": descricao[:80]})
            continue
        if not quando:
            descartadas.append({"linha": numero, "porque": "sem data válida",
                                "texto": (descricao or _limpo(linha[0]))[:80]})
            continue

        # O valor: ou uma coluna com sinal, ou o par crédito/débito.
        if "valor" in papeis:
            valor = _numero(celula(linha, "valor"))
            # O BB diz o sentido numa coluna à parte; quando o valor vem sem
            # sinal, é ela que manda.
            sentido = celula(linha, "tipo").lower()
            if valor is not None and valor > 0 and sentido.startswith("saída"):
                valor = -valor
        else:
            credito = _numero(celula(linha, "credito")) or Decimal("0")
            debito = _numero(celula(linha, "debito")) or Decimal("0")
            # ⚠️ O DÉBITO JÁ VEM NEGATIVO nas abas do Bradesco (-675,87). Somar
            # em vez de subtrair é o que mantém isso certo nos dois casos.
            valor = credito + (debito if debito < 0 else -debito)

        if valor is None:
            descartadas.append({"linha": numero, "porque": "sem valor",
                                "texto": descricao[:80]})
            continue
        if valor == 0:
            descartadas.append({"linha": numero, "porque": "valor zerado",
                                "texto": descricao[:80]})
            continue

        documento = celula(linha, "documento")
        if not documento and tipo_e_documento:
            documento = celula(linha, "tipo")

        conciliado = _chave(celula(linha, "conciliado")).startswith("conciliad")

        # ⚠️ AS SOBRAS VIRAM OBSERVAÇÃO — pedido dele: *"algum dado que tenha
        # entre as colunas da parte numérica e a coluna L, a gente vai colocar
        # como observação do lançamento"*. Cada pedaço vem com o nome da
        # coluna, senão vira um amontoado sem sentido daqui a um ano.
        anotacoes = []
        for col, rotulo in sobras:
            if len(linha) > col and _limpo(linha[col]):
                anotacoes.append(f"{rotulo}: {_limpo(linha[col])}")

        linhas.append({
            "data": quando,
            "descricao": descricao[:500],
            "documento": documento[:60],
            "valor": valor,
            "conciliado": conciliado,
            "observacao": " · ".join(anotacoes)[:1000],
            "saldo_planilha": _numero(celula(linha, "saldo")),
        })

    if not linhas:
        raise ErroDaPlanilha(
            "Não achei nenhum lançamento nesta aba. Confira se o cabeçalho "
            "está na aba certa e se as colunas de data e valor têm conteúdo.")

    return {
        "aba": aba,
        "cabecalho_na_linha": inicio + 1,
        "colunas": {papel: col for papel, col in papeis.items()},
        "tipo_e_documento": tipo_e_documento,
        "observacao_veio_de": [rotulo for _col, rotulo in sobras],
        "linhas": linhas,
        "descartadas": descartadas,
        "conciliadas": sum(1 for l in linhas if l["conciliado"]),
        "periodo_ini": min(l["data"] for l in linhas),
        "periodo_fim": max(l["data"] for l in linhas),
        "soma": sum(l["valor"] for l in linhas),
        "conferencia_do_saldo": _conferir_saldo(linhas),
    }


def _conferir_saldo(linhas: list) -> dict:
    """A planilha traz o saldo de cada linha. Ele bate com a soma dos valores?

    ⚠️ É A ÚNICA PROVA DE QUE A LEITURA ENTENDEU OS SINAIS. Ler "débito" como
    positivo dobraria o saldo e nada mais acusaria: as linhas estariam todas
    lá, com as datas certas e os históricos certos. Só o saldo denuncia.

    A conta é feita na ordem da planilha, do primeiro ao último lançamento com
    saldo preenchido, e compara a VARIAÇÃO do saldo com a soma dos valores.
    """
    com_saldo = [l for l in linhas if l.get("saldo_planilha") is not None]
    if len(com_saldo) < 2:
        return {"deu": False, "porque": "a aba não traz saldo em duas linhas"}

    # As abas vêm ora do mais antigo para o mais novo, ora ao contrário. A
    # ordem da PLANILHA é a que vale para o saldo corrido dela.
    primeiro, ultimo = com_saldo[0], com_saldo[-1]
    variacao = ultimo["saldo_planilha"] - primeiro["saldo_planilha"]
    meio = sum(l["valor"] for l in com_saldo[1:])
    if variacao == meio:
        return {"deu": True, "bate": True}
    # Do mais novo para o mais antigo, a variação troca de sinal.
    if variacao == -sum(l["valor"] for l in com_saldo[:-1]):
        return {"deu": True, "bate": True, "ordem_invertida": True}
    return {"deu": True, "bate": False,
            "variacao": str(variacao), "soma": str(meio)}


# ---------------------------------------------------------------------------
# DE ONDE VEM A ABA — a planilha do Google, lida com a credencial de sempre
# ---------------------------------------------------------------------------
CHAVE_PLANILHA = "conciliacao_planilha_id"


def planilha_guardada() -> str:
    """O identificador da planilha "Controle de Conciliação", se já foi salvo."""
    from .db import consultar_um
    try:
        linha = consultar_um("SELECT valor FROM analisesps.meta WHERE chave = ?",
                             (CHAVE_PLANILHA,))
    except Exception:  # noqa: BLE001 — antes da migração é estado normal
        return ""
    return (linha or ("",))[0] or ""


def guardar_planilha(bruto: str) -> str:
    """Aceita o endereço inteiro copiado da barra e guarda só o identificador.

    Exigir que a pessoa recorte o pedaço certo de uma URL é pedir para errar —
    a mesma regra do campo da pasta do Drive, que já é assim.
    """
    texto = str(bruto or "").strip()
    achado = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]{20,})", texto)
    identificador = achado.group(1) if achado else texto
    if identificador and not re.fullmatch(r"[A-Za-z0-9_-]{20,}", identificador):
        raise ErroDaPlanilha(
            "Isso não parece o endereço de uma planilha do Google. Cole o "
            "endereço da barra do navegador, ou só o identificador.")
    from .db import conexao
    with conexao() as con:
        con.execute(
            "INSERT INTO analisesps.meta (chave, valor) VALUES (?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor",
            (CHAVE_PLANILHA, identificador))
        con.commit()
    return identificador


def abas(planilha_id: str) -> list[str]:
    """Os nomes das abas que a planilha tem — para o dono escolher qual trazer."""
    from .credenciais import cliente, com_retry
    if not str(planilha_id or "").strip():
        raise ErroDaPlanilha("Nenhuma planilha configurada.")
    try:
        return [a.title for a in com_retry(
            lambda: cliente().open_by_key(planilha_id).worksheets())]
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        raise ErroDaPlanilha(
            "Não consegui abrir a planilha. Confira se o identificador está "
            "certo e se ela foi compartilhada com a conta de serviço do "
            f"Google. ({str(e)[:160]})") from e


def ler_aba(planilha_id: str, aba: str) -> dict:
    """Lê UMA aba e devolve o que `interpretar` entendeu dela.

    ⚠️ UMA ABA POR VEZ, SEMPRE. Abrir as vinte de uma vez traria a planilha
    inteira (8 MB) para a memória do serviço — e este serviço já morreu disso
    uma vez (CONTEXTO §9). Uma por vez também é como o dono vai conferir:
    olhando a amostra antes de confirmar.
    """
    from .credenciais import cliente, com_retry
    if not str(aba or "").strip():
        raise ErroDaPlanilha("Escolha a aba.")
    try:
        folha = com_retry(
            lambda: cliente().open_by_key(planilha_id).worksheet(aba))
        valores = com_retry(folha.get_all_values)
    except Exception as e:  # noqa: BLE001
        raise ErroDaPlanilha(
            f"Não consegui ler a aba \"{aba}\". ({str(e)[:160]})") from e
    return interpretar(valores, aba)


def amostra_para_a_tela(lido: dict) -> dict:
    """O que a tela mostra antes de o dono confirmar a importação.

    ⚠️ A AMOSTRA É O PONTO DE CONTROLE. É aqui que ele vê se as colunas foram
    entendidas — se o valor virou negativo onde devia, se a observação pegou a
    coluna certa. Importar sem mostrar isso seria pedir confiança cega em cima
    de dois anos de trabalho dele.
    """
    return {
        "aba": lido.get("aba", ""),
        "cabecalho_na_linha": lido.get("cabecalho_na_linha"),
        "total": len(lido.get("linhas") or []),
        "conciliadas": lido.get("conciliadas", 0),
        "periodo_ini": lido["periodo_ini"].isoformat() if lido.get("periodo_ini") else "",
        "periodo_fim": lido["periodo_fim"].isoformat() if lido.get("periodo_fim") else "",
        "soma": str(lido.get("soma") or 0),
        "observacao_veio_de": lido.get("observacao_veio_de") or [],
        "tipo_e_documento": lido.get("tipo_e_documento", False),
        "conferencia_do_saldo": lido.get("conferencia_do_saldo") or {},
        "descartadas": (lido.get("descartadas") or [])[:12],
        "descartadas_total": len(lido.get("descartadas") or []),
        "amostra": [{
            "data": l["data"].strftime("%d/%m/%Y"),
            "descricao": l["descricao"][:90],
            "documento": l["documento"],
            "valor": str(l["valor"]),
            "conciliado": l["conciliado"],
            "observacao": l["observacao"][:80],
        } for l in (lido.get("linhas") or [])[:AMOSTRA]],
    }
