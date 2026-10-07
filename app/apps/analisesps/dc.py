# -*- coding: utf-8 -*-
"""
DESPESAS COM COLABORADORES (DC) — 03/10/2026.

O dono: *"são solicitações que a gente chama de despesa com colaboradores. Elas
entram no Pipefy, num determinado pipe. Lá a gente faz um tratamento delas e,
quando elas chegam a uma determinada fase, aparecem na aba Data da planilha (…)
o que vale mesmo é a Data (…) o objetivo é gerar a planilha de pagamento do
BeeVale. E permitir também pagar via SomaPay."*

O caminho, no padrão das outras folhas:

1. **A fonte é a aba "Data"** da planilha da DC (o Make a alimenta, uma linha
   por colaborador de cada solicitação — colunas A…N, a ordem do `doPost` do
   script dele). A tela lê a aba na hora (aba curta: só o que está pendente).
2. **Cada linha ganha o que a aba "DC" da planilha acrescentava**, agora do
   banco: nome, função e tipo pelo CPF (cadastro), a diária cadastrada, a conta
   de pagamento pela obra ("C. Diários"), o código OMIE da obra, a categoria e o
   Record ID do tipo de despesa ("Plano Financeiro") e a carteira do BeeVale
   (aba "Data base BeeVale" da planilha da DC).
3. **Gerar arquivos** é o mesmo de todas as folhas (destino por conta, prévia,
   definitivo, relatório PDF por conta, análise), e o que foi gerado fica num
   LOTE (`dc_lote`/`dc_linha`, migração 047) — é o que tira as linhas da tela,
   alimenta a SP do Pipefy e a crítica de duplicidade (mesmo CPF e tipo de
   despesa gerados nos últimos 10 dias, como o script fazia).
4. **No Pipefy** (pela aba Arquivos gerados), uma SP por conta, com o rateio por
   obra e por categoria, e os cards de origem marcados (`mover_card` = Sim) e
   movidos para a fase de processados — o que o script fazia ao final.

⚠️ A aba "Data" NÃO é apagada: o script apagava as linhas processadas; aqui o
lote guardado é que as esconde. Nada é escrito na planilha.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import time
from decimal import Decimal

from . import colaboradores, formatos
from .folha_rateio import cpf_bonito, so_digitos

logger = logging.getLogger("analisesps.dc")

# A planilha da DC (dono, 03/10/2026, e o `doPost` do script dele).
PLANILHA_DC = os.getenv("ANALISESPS_SHEET_DC",
                        "124EVrS86jBdGS7KzTJ5qI1MuiU25W9QguMAPFqhj1iM")
ABA_DATA = "Data"
ABAS_DAS_CARTEIRAS = ("Data base BeeVale", "Data Base BeeVale", "DataBase BeeVale",
                      "Database BeeVale", "Data base Beevale")

TIPO = "dc"           # o "pagamento" no registro de arquivos
VERBA = "dc"
CARTEIRA_PADRAO = "Produção"
ACRESCIMO_BEEVALE = Decimal("1.5")     # % do BeeVale, como o script: só informado
DIAS_DA_DUPLICIDADE = 10
CENTAVO = Decimal("0.01")

# O que o script fazia com o card de origem depois de gerar.
CAMPO_MOVER = "mover_card"
FASE_PROCESSADO = 340593562

# As colunas da aba "Data", na ordem do `doPost` (A…N).
COLUNAS_DATA = ("card_id", "data_solicitacao", "data_vencimento", "centro_custo",
                "tipo_despesa", "periodo", "valor_diaria_flag", "responsavel",
                "requerente", "cpf", "quantidade", "valor", "valor_diaria",
                "descricao")
TETO_DE_LINHAS = 5_000
VALIDADE_DA_LEITURA = 60          # segundos: a aba é lida de novo depois disso

_cache: dict = {}


class ErroDaDC(RuntimeError):
    """A frase vai inteira para a tela."""


def _sem_acento(texto) -> str:
    import unicodedata
    cru = unicodedata.normalize("NFKD", " ".join(str(texto or "").split()))
    return "".join(c for c in cru if not unicodedata.combining(c)).lower()


def _pronto() -> bool:
    """A migração 047 já rodou?"""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.dc_lote LIMIT 1")
        return True
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# A FONTE: a aba "Data"
# ---------------------------------------------------------------------------
def _ler_aba(nome: str) -> list:
    from .sincronizacao import _aba, com_retry
    return com_retry(_aba(PLANILHA_DC, nome).get_all_values)


def ler_data(recarregar: bool = False) -> list:
    """As linhas da aba "Data", como dicionários. Guardadas por 60 segundos."""
    agora = time.time()
    guardado = _cache.get("data")
    if guardado and not recarregar and agora - guardado[0] < VALIDADE_DA_LEITURA:
        return guardado[1]
    from .sincronizacao import _explicar_aba
    try:
        valores = _ler_aba(ABA_DATA)
    except Exception as e:  # noqa: BLE001
        raise ErroDaDC(
            "não foi possível ler a aba \"Data\" da planilha da DC — "
            + _explicar_aba(PLANILHA_DC, ABA_DATA, e)
            + " Se for permissão, compartilhe a planilha com o e-mail da conta de "
            "serviço do Google (o mesmo das outras planilhas).") from e
    linhas = []
    for n, bruta in enumerate(valores[:TETO_DE_LINHAS], start=1):
        celulas = (list(bruta) + [""] * len(COLUNAS_DATA))[:len(COLUNAS_DATA)]
        linha = {c: str(v or "").strip() for c, v in zip(COLUNAS_DATA, celulas)}
        # O cabeçalho (ou linha em branco) não tem número de card nem CPF.
        if not so_digitos(linha["card_id"]) or len(so_digitos(linha["cpf"])) != 11:
            continue
        linha["card_id"] = so_digitos(linha["card_id"])
        linha["cpf"] = so_digitos(linha["cpf"])
        linha["linha_da_aba"] = n
        linhas.append(linha)
    _cache["data"] = (agora, linhas)
    return linhas


def lida_em():
    """Quando a aba "Data" foi lida pela última vez (horário de Brasília), ou
    None se ainda não foi."""
    guardado = _cache.get("data")
    if not guardado:
        return None
    from .horario import FUSO
    import datetime as _dt
    return _dt.datetime.fromtimestamp(guardado[0], tz=_dt.timezone.utc).astimezone(FUSO)


# ⚠️ A TABELA DAS CARTEIRAS FICA AQUI, GRAVADA (06/10/2026). Ela vinha só da aba
# "Data base BeeVale" da planilha da DC — e, sem conseguir ler a aba, tudo saía
# como "Produção", com aviso. O dono, que já a tinha passado no dia anterior:
# *"não pode ser assim (…) eu já disse quais são os tipos, por que não grava
# logo"*. "Gratiticações" é a grafia do PORTAL (*"tá errado mesmo, gratiti"*).
# A aba, quando lida, ainda acrescenta ou corrige linhas — mas não é mais
# necessária.
CARTEIRAS_DA_DC = {
    "Despesas com Alimentação": "Auxílio Alimentação",
    "Despesas com Transporte": "Despesas com Transporte",
    "Diárias": "Diárias",
    "Gratificações e Extras": "Gratiticações e Extras",
    "Produção": "Produção",
    "Salários e Ordenados": "Diárias",
}


# O tipo da DC que não tem linha própria no Plano Financeiro → a linha usada.
# ⚠️ "Diárias" não existe no Plano Financeiro (07/10/2026: o lançamento da DC
# barrou "sem Código Omie" e "sem Record ID"). Nos diaristas a diária é lançada
# como "Salários e Ordenados" (`folha_cards.DESCRICAO_DA_VERBA`); a DC usa a
# mesma — só quando o nome não estiver no plano.
TIPO_NO_PLANO = {"Diárias": "Salários e Ordenados"}


def classificacao_no_plano(tipo: str, plano: dict) -> tuple:
    """(Código Omie da categoria, Record ID do Tipo de Despesa) do tipo da DC: a
    linha do Plano Financeiro com o mesmo nome; sem ela, a de `TIPO_NO_PLANO`."""
    from . import folha_cards
    chave_tipo = _sem_acento(tipo)
    do_plano = (plano.get(folha_cards._chave(tipo))
                or plano.get(folha_cards._chave(next(
                    (v for k, v in TIPO_NO_PLANO.items() if _sem_acento(k) == chave_tipo),
                    ""))) or [])
    categoria = next((str(x.get("codigo_omie") or "") for x in do_plano
                      if x.get("codigo_omie")), "")
    record = next((str(x.get("record_id") or "") for x in do_plano
                   if x.get("record_id")), "")
    return categoria, record


def carteiras(recarregar: bool = False) -> tuple:
    """(`{tipo de despesa sem acento: carteira}`, aviso) — a tabela gravada
    (`CARTEIRAS_DA_DC`), mais o que a aba "Data base BeeVale" da planilha da DC
    trouxer (dono: *"converte a categoria do plano financeiro em como ela é
    chamada no BeeVale: despesa com alimentação no BeeVale é auxílio
    alimentação"*). Aba ilegível não é mais aviso: a tabela gravada basta."""
    agora = time.time()
    guardado = _cache.get("carteiras")
    if guardado and not recarregar and agora - guardado[0] < 600:
        return guardado[1]
    mapa = {_sem_acento(tipo): carteira for tipo, carteira in CARTEIRAS_DA_DC.items()}
    for nome in ABAS_DAS_CARTEIRAS:
        try:
            valores = _ler_aba(nome)
        except Exception:  # noqa: BLE001 — a tabela gravada vale sozinha
            continue
        mapa.update(_carteiras_de(valores))
        break
    resposta = (mapa, "")
    _cache["carteiras"] = (agora, resposta)
    return resposta


def _carteiras_de(valores) -> dict:
    """Acha as colunas pelo nome — a do plano financeiro (ou categoria/despesa) e
    a da carteira (ou BeeVale) — em qualquer das 5 primeiras linhas."""
    for i, cab in enumerate(valores[:5]):
        nomes = [_sem_acento(c) for c in cab]
        # "Tipo DC" | "Tipo BeeVale" é o cabeçalho da aba dele (05/10/2026).
        i_cat = next((j for j, c in enumerate(nomes) if c and any(
            p in c for p in ("plano", "categoria", "despesa", "tipo dc"))), None)
        i_cart = next((j for j, c in enumerate(nomes) if c and j != i_cat and any(
            p in c for p in ("carteira", "beevale", "bee vale"))), None)
        if i_cat is None or i_cart is None:
            continue
        mapa = {}
        for linha in valores[i + 1:]:
            cat = linha[i_cat] if i_cat < len(linha) else ""
            cart = linha[i_cart] if i_cart < len(linha) else ""
            if str(cat).strip() and str(cart).strip():
                mapa[_sem_acento(cat)] = " ".join(str(cart).split())
        return mapa
    return {}


# ---------------------------------------------------------------------------
# O que já foi gerado, a seleção e a duplicidade
# ---------------------------------------------------------------------------
def _gerados() -> dict:
    """`{chave: (lote, criado_em)}` das linhas que já entraram numa geração."""
    from .db import consultar
    if not _pronto():
        return {}
    return {l[0]: (l[1], l[2]) for l in consultar(
        "SELECT li.chave, lo.id, lo.criado_em FROM analisesps.dc_linha li "
        "  JOIN analisesps.dc_lote lo ON lo.id = li.lote_id")}


def sps_das_linhas() -> dict:
    """`{chave: {"id", "link", "conta"}}` — a SP do Pipefy que pagou cada linha
    já gerada. O dono, 06/10/2026: *"no relatório dela é importante que saia o
    registro da SP"*.

    A ponte: a linha guarda o lote e a conta; o lote, a geração (análise); o
    andamento do lançamento daquela geração, a SP de cada conta. Linha gerada e
    ainda não lançada fica fora — não há SP para dizer."""
    from .db import consultar
    from . import folha_cards
    if not _pronto():
        return {}
    linhas = consultar(
        "SELECT li.chave, li.conta, lo.analise_id FROM analisesps.dc_linha li "
        "  JOIN analisesps.dc_lote lo ON lo.id = li.lote_id")
    por_analise: dict = {}
    saida = {}
    for chave, conta, analise_id in linhas:
        if analise_id not in por_analise:
            por_analise[analise_id] = folha_cards.contas_lancadas(
                folha_cards._andamento(int(analise_id)), VERBA)
        sp = por_analise[analise_id].get(" ".join(str(conta or "").split()))
        if sp and sp.get("id"):
            saida[chave] = {"id": str(sp["id"]), "link": sp.get("link") or "",
                            "conta": conta}
    return saida


def _recentes() -> dict:
    """`{(cpf, tipo sem acento): [{data, valor, card_id}]}` — o gerado nos
    últimos 10 dias, para a crítica de duplicidade (a do script)."""
    from .db import consultar
    if not _pronto():
        return {}
    saida: dict = {}
    for cpf, tipo, valor, card, quando in consultar(
            "SELECT li.cpf, li.tipo_despesa, li.valor, li.card_id, lo.criado_em "
            "  FROM analisesps.dc_linha li JOIN analisesps.dc_lote lo ON lo.id = li.lote_id "
            " WHERE lo.criado_em >= now() - (? * interval '1 day')",
            (DIAS_DA_DUPLICIDADE,)):
        saida.setdefault((cpf, _sem_acento(tipo)), []).append(
            {"valor": Decimal(str(valor or 0)), "card_id": card, "quando": quando})
    return saida


def ajustes() -> dict:
    """`{chave: {pagar, obra}}` — as exceções da tela."""
    from .db import consultar
    if not _pronto():
        return {}
    return {l[0]: {"pagar": l[1], "obra": l[2] or ""} for l in consultar(
        "SELECT chave, pagar, obra FROM analisesps.dc_ajuste")}


def salvar_selecao(decisoes, quem: str = "") -> dict:
    """Guarda quem vai e quem não vai — só as exceções (como no auxílio)."""
    from .db import conexao
    if not _pronto():
        raise ErroDaDC('atualização do banco pendente (047). Clique em "Aplicar '
                       'atualizações do banco" em Configurações.')
    calculado = calcular()
    por_chave = {p["chave"]: p for p in calculado["pessoas"]}
    gravados = limpos = 0
    with conexao() as conn:
        for item in decisoes or []:
            chave = str((item or {}).get("chave") or "")
            p = por_chave.get(chave)
            if not p:
                continue
            querido = bool((item or {}).get("pagar"))
            if querido == bool(p["pagar_calculado"]) or (querido and p["impossivel"]):
                cur = conn.execute(
                    "UPDATE analisesps.dc_ajuste SET pagar = NULL WHERE chave = ?", (chave,))
                limpos += 1 if (cur.rowcount or 0) > 0 else 0
                cur.close()
                continue
            conn.execute(
                "INSERT INTO analisesps.dc_ajuste (chave, pagar, alterado_por) "
                "VALUES (?, ?, ?) ON CONFLICT (chave) DO UPDATE SET "
                "pagar = EXCLUDED.pagar, alterado_em = now(), "
                "alterado_por = EXCLUDED.alterado_por",
                (chave, querido, str(quem or "")[:120]))
            gravados += 1
        conn.execute("DELETE FROM analisesps.dc_ajuste "
                     " WHERE pagar IS NULL AND coalesce(obra, '') = ''")
        conn.commit()
    return {"gravados": gravados, "limpos": limpos}


def escolher_obra(chave: str, obra: str, quem: str = "") -> None:
    """A obra que paga uma linha, escolhida à mão (vazio = a da solicitação)."""
    from .db import conexao
    if not _pronto():
        raise ErroDaDC('atualização do banco pendente (047).')
    obra = " ".join(str(obra or "").split()).upper()[:60]
    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.dc_ajuste (chave, obra, alterado_por) VALUES (?, ?, ?) "
            "ON CONFLICT (chave) DO UPDATE SET obra = EXCLUDED.obra, "
            "alterado_em = now(), alterado_por = EXCLUDED.alterado_por",
            (str(chave), obra, str(quem or "")[:120]))
        conn.execute("DELETE FROM analisesps.dc_ajuste "
                     " WHERE pagar IS NULL AND coalesce(obra, '') = ''")
        conn.commit()


# ---------------------------------------------------------------------------
# O CÁLCULO
# ---------------------------------------------------------------------------
# O que a coluna G da aba Data ("valor da diária") traz quando a solicitação
# pede o pagamento pela diária CADASTRADA. Lido por pedaço de palavra, porque o
# texto exato vem do formulário do Pipefy.
PEDE_DIARIA_CADASTRADA = ("cadastr", "sim")


def pede_diaria_cadastrada(flag) -> bool:
    texto = _sem_acento(flag)
    return bool(texto) and any(p in texto for p in PEDE_DIARIA_CADASTRADA)


def valor_da_linha(linha: dict, diaria_cadastrada) -> tuple:
    """(valor, motivos, de onde veio) — o valor a pagar de uma linha da aba Data.

    A diária "do cadastro" é a MESMA coluna que a tela de Diaristas usa
    (`colaboradores.valores_de_diaria` — coluna 49 de "Dados Documentos");
    confirmado pelo dono em 04/10/2026.

    O dono, 03/10/2026: *"A DC pode vir com valor ou não quando se trata de
    diária. Ela pode pedir que seja paga pelo valor de diária cadastrada, nesse
    caso o sistema calcula."* Então, em ordem:

    1. a solicitação pede a diária CADASTRADA (coluna G) → quantidade × diária
       do cadastro — mesmo que traga outro valor, que fica dito;
    2. o valor informado;
    3. quantidade × diária informada na solicitação;
    4. quantidade × diária do cadastro (a solicitação de diária sem valor);
    5. nada disso → sem valor, a linha não entra."""
    motivos = []
    qtd = formatos.para_numero(linha.get("quantidade")) or Decimal("0")
    informado = formatos.para_numero(linha.get("valor")) or Decimal("0")
    diaria_inf = formatos.para_numero(linha.get("valor_diaria")) or Decimal("0")
    diaria_cad = Decimal(str(diaria_cadastrada)) if diaria_cadastrada else Decimal("0")

    if pede_diaria_cadastrada(linha.get("valor_diaria_flag")):
        if qtd > 0 and diaria_cad > 0:
            if informado > 0 or diaria_inf > 0:
                motivos.append("a solicitação pede a diária cadastrada: o valor "
                               "informado nela foi desconsiderado.")
            return (qtd * diaria_cad).quantize(CENTAVO), motivos, "cadastro"
        motivos.append("a solicitação pede a diária cadastrada, mas "
                       + ("o colaborador não tem diária no cadastro."
                          if qtd > 0 else "não informa a quantidade de diárias."))
        return Decimal("0.00"), motivos, ""

    if diaria_inf and diaria_cad and diaria_inf != diaria_cad:
        motivos.append(f"diária informada R$ {formatos.moeda(diaria_inf)} difere da "
                       f"cadastrada R$ {formatos.moeda(diaria_cad)}.")
    if informado > 0:
        return informado.quantize(CENTAVO), motivos, "informado"
    if qtd > 0 and diaria_inf > 0:
        return (qtd * diaria_inf).quantize(CENTAVO), motivos, "informada"
    if qtd > 0 and diaria_cad > 0:
        return (qtd * diaria_cad).quantize(CENTAVO), motivos, "cadastro"
    motivos.append("sem valor: a solicitação não informa o valor, e não há "
                   + ("diária (nem na solicitação, nem no cadastro)." if qtd > 0
                      else "quantidade de diárias para calcular."))
    return Decimal("0.00"), motivos, ""


ORIGEM_DO_VALOR = {"informado": "informado",
                   "informada": "diária informada",
                   "cadastro": "diária do cadastro"}


def _chaves(linhas) -> list:
    """A chave de cada linha: card + CPF + tipo de despesa (e a ocorrência, se
    a mesma combinação se repetir na aba)."""
    vistos: dict = {}
    saida = []
    for l in linhas:
        base = f"{l['card_id']}|{l['cpf']}|{_sem_acento(l['tipo_despesa'])}"
        vistos[base] = vistos.get(base, 0) + 1
        saida.append(base if vistos[base] == 1 else f"{base}|{vistos[base]}")
    return saida


def calcular(recarregar: bool = False, mostrar_geradas: bool = False) -> dict:
    """As linhas da aba Data, prontas para a tela e para o arquivo."""
    from . import folha_cards, folha_pagamento as fpg

    base = {"pessoas": [], "avisos": [], "total": Decimal("0.00"), "quantos": 0,
            "quantos_a_pagar": 0, "por_obra": [], "com_problema": [],
            "geradas": 0, "pronto": _pronto()}
    linhas = ler_data(recarregar)
    # QUANDO A ABA FOI LIDA (dono, 05/10/2026: *"seria interessante ter a
    # informação do momento em que ela foi atualizada, data e hora"*).
    base["lida_em"] = lida_em()
    cpfs = sorted({l["cpf"] for l in linhas})
    fichas = colaboradores.muitos_por_cpf(cpfs) if cpfs else {}
    diarias = colaboradores.valores_de_diaria(cpfs) if cpfs else {}
    contas = fpg.conta_por_obra()
    omie = folha_cards._codigos_omie()
    plano, aviso_plano = folha_cards._plano_financeiro()
    mapa_carteiras, aviso_carteiras = carteiras(recarregar)
    for aviso in (aviso_plano, aviso_carteiras):
        if aviso:
            base["avisos"].append(aviso)
    gerados = _gerados()
    recentes = _recentes()
    mao = ajustes()
    # A SP de cada linha já lançada — só quando as geradas aparecem.
    sps = sps_das_linhas() if mostrar_geradas else {}

    pessoas = []
    for linha, chave in zip(linhas, _chaves(linhas)):
        gerada = chave in gerados
        if gerada:
            base["geradas"] += 1
            if not mostrar_geradas:
                continue
        ficha = fichas.get(linha["cpf"]) or {}
        aj = mao.get(chave) or {}
        valor, motivos, origem_valor = valor_da_linha(linha, diarias.get(linha["cpf"]))
        obra_solicitada = " ".join(linha["centro_custo"].split()).upper()
        obra = aj.get("obra") or obra_solicitada
        tipo = " ".join(linha["tipo_despesa"].split())
        chave_tipo = _sem_acento(tipo)
        # ⚠️ "Diárias" não existe no Plano Financeiro (07/10/2026: o lançamento da
        # DC barrou "sem Código Omie" e "sem Record ID"). Nos diaristas a diária
        # é lançada como "Salários e Ordenados" (`folha_cards.DESCRICAO_DA_VERBA`);
        # a DC usa a mesma — só quando o nome não estiver no plano.
        categoria, record = classificacao_no_plano(tipo, plano)
        p = {
            "chave": chave, "card_id": linha["card_id"],
            "link_card": f"https://app.pipefy.com/open-cards/{linha['card_id']}",
            "cpf": linha["cpf"], "cpf_bonito": cpf_bonito(linha["cpf"]),
            "nome": ficha.get("nome") or "", "cargo": ficha.get("cargo") or "",
            "tipo_contrato": ficha.get("tipo_contrato") or ficha.get("tipo") or "",
            "fase": ficha.get("fase") or "", "situacao": ficha.get("situacao") or "",
            "link_pipefy": ficha.get("link_pipefy") or "",
            "obra": obra, "obra_solicitada": obra_solicitada,
            "obra_trocada": bool(aj.get("obra")), "obras": [obra] if obra else [],
            "obra_cadastro": ficha.get("obra_codigo") or "",
            "conta": contas.get(obra, ""), "omie": omie.get(folha_cards._chave(obra), ""),
            "tipo_despesa": tipo, "categoria": categoria, "record_id": record,
            "carteira": mapa_carteiras.get(chave_tipo) or CARTEIRA_PADRAO,
            "carteira_achada": chave_tipo in mapa_carteiras,
            "quantidade": formatos.para_numero(linha["quantidade"]) or Decimal("0"),
            "valor_informado": formatos.para_numero(linha["valor"]),
            "origem_valor": origem_valor,
            "rotulo_origem_valor": ORIGEM_DO_VALOR.get(origem_valor, ""),
            "pede_diaria_cadastrada": pede_diaria_cadastrada(linha["valor_diaria_flag"]),
            "texto_da_diaria": " ".join(linha["valor_diaria_flag"].split())[:60],
            "valor_diaria_informada": formatos.para_numero(linha["valor_diaria"]),
            "valor_diaria_cadastrada": diarias.get(linha["cpf"]),
            "valor": valor, "descricao": linha["descricao"],
            "requerente": linha["requerente"], "responsavel": linha["responsavel"],
            "periodo": linha["periodo"], "data_solicitacao": linha["data_solicitacao"],
            "data_vencimento": linha["data_vencimento"], "gerada": gerada,
            "sp": sps.get(chave),
            "motivos": motivos, "duplicidade": recentes.get((linha["cpf"], chave_tipo)) or [],
            "impossivel": False,
        }
        if not ficha:
            p["impossivel"] = True
            p["motivos"].append("CPF fora do cadastro de colaboradores — sem o nome, "
                                "o banco recusa o pagamento.")
        if valor <= 0:
            p["impossivel"] = True
        if not obra:
            p["motivos"].append("solicitação sem centro de custo (obra).")
        elif not p["conta"]:
            p["motivos"].append(f'a obra {obra} não tem conta de pagamento na aba "C. Diários".')
        if obra and not p["omie"]:
            p["motivos"].append(f'a obra {obra} não tem Código Omie na aba "C. Diários".')
        if tipo and not categoria:
            p["motivos"].append(f'tipo de despesa "{tipo}" sem Código Omie na aba "Plano Financeiro".')
        if p["situacao"] == colaboradores.SITUACAO_SAIU:
            p["motivos"].append("colaborador desligado no cadastro — confira antes de pagar.")
        if p["duplicidade"]:
            d = p["duplicidade"][0]
            p["motivos"].append(
                f"possível duplicidade: {tipo} deste CPF gerado em "
                f"{d['quando'].strftime('%d/%m/%Y') if d.get('quando') else '?'} "
                f"(R$ {formatos.moeda(d['valor'])}, card {d['card_id']}).")
        p["pagar_calculado"] = not p["impossivel"] and bool(p["conta"])
        p["pagar"] = (p["pagar_calculado"] if aj.get("pagar") is None
                      else bool(aj["pagar"]) and not p["impossivel"])
        p["ajuste_pagar"] = aj.get("pagar")
        pessoas.append(p)

    pessoas.sort(key=lambda p: (p["pagar"], (p["nome"] or "").lower(), p["card_id"]))
    a_pagar = [p for p in pessoas if p["pagar"] and p["valor"] > 0 and not p["gerada"]]
    sem_carteira = sorted({p["tipo_despesa"] for p in a_pagar if not p.get("carteira_achada")})
    if sem_carteira:
        base["avisos"].append(
            "tipo de despesa sem carteira do BeeVale na tabela: " + ", ".join(sem_carteira)
            + f' — vai como "{CARTEIRA_PADRAO}". Diga qual é a carteira para gravar.')
    por_obra: dict = {}
    for p in a_pagar:
        o = por_obra.setdefault(p["obra"] or "(sem obra)", {
            "obra": p["obra"] or "(sem obra)", "pessoas": 0, "total": Decimal("0.00"),
            "conta": p["conta"]})
        o["pessoas"] += 1
        o["total"] += p["valor"]
    base["resumos"] = {campo: resumo_por(a_pagar, campo)
                       for campo in ("obra", "conta", "tipo_despesa")}
    base.update({
        "pessoas": pessoas, "quantos": len(pessoas), "quantos_a_pagar": len(a_pagar),
        "total": sum((p["valor"] for p in a_pagar), Decimal("0.00")),
        "por_obra": sorted(por_obra.values(), key=lambda o: -o["total"]),
        "com_problema": [p for p in pessoas if p["impossivel"] or not p["conta"]],
        "duplicadas": [p for p in pessoas if p["duplicidade"] and not p["gerada"]],
        "cards": len({p["card_id"] for p in pessoas}),
    })
    return base


# A VISÃO AGRUPADA (dono, 03/10/2026: *"seria interessante podermos visualizar
# de forma mais agrupada o que está para ser pago. Agrupar por obra etc."*).
AGRUPAMENTOS = [("obra", "Obra"), ("conta", "Conta"),
                ("tipo_despesa", "Tipo de despesa"), ("card_id", "Solicitação"),
                ("cpf", "Colaborador"), ("", "Sem agrupar")]
AGRUPAMENTO_PADRAO = "obra"


def _rotulo_do_grupo(p: dict, campo: str) -> str:
    if campo == "card_id":
        return f"card {p['card_id']}"
    if campo == "cpf":
        return p["nome"] or p["cpf_bonito"]
    return p.get(campo) or {"obra": "(sem obra)", "conta": "(sem conta)"}.get(
        campo, "(sem tipo)")


def resumo_por(linhas, campo: str) -> list:
    """`[{rotulo, linhas, pessoas, total}]` do que vai ser pago, por `campo`,
    do maior para o menor."""
    grupos: dict = {}
    for p in linhas:
        rotulo = _rotulo_do_grupo(p, campo)
        g = grupos.setdefault(rotulo, {"rotulo": rotulo, "linhas": 0, "cpfs": set(),
                                       "total": Decimal("0.00")})
        g["linhas"] += 1
        g["cpfs"].add(p["cpf"])
        g["total"] += p["valor"]
    return [{"rotulo": g["rotulo"], "linhas": g["linhas"], "pessoas": len(g["cpfs"]),
             "total": g["total"]}
            for g in sorted(grupos.values(), key=lambda g: (-g["total"], g["rotulo"]))]


def agrupar(pessoas, campo: str) -> list:
    """A lista da tela em grupos (`folha_lista.agrupar`, a mesma dos auxílios)."""
    from . import folha_lista
    return folha_lista.agrupar(pessoas, campo, rotulo=_rotulo_do_grupo,
                               pendente=lambda p: p["impossivel"] or not p["conta"])


def linhas_a_pagar(calculado: dict | None = None) -> list:
    """As linhas marcadas, no formato da geração (`folha_pagamento.gerar`), com a
    natureza (o tipo de despesa) e a carteira do BeeVale de cada uma."""
    calculado = calcular() if calculado is None else calculado
    return [{"cpf": p["cpf"], "nome": p["nome"], "obra": p["obra"], "conta": p["conta"],
             "verba": VERBA, "valor": p["valor"],
             "dias": int(p["quantidade"] or 0), "origem": "dc",
             "natureza": p["tipo_despesa"], "carteira": p["carteira"],
             "chave": p["chave"], "card_id": p["card_id"],
             "tipo_despesa": p["tipo_despesa"], "categoria": p["categoria"],
             "record_id": p["record_id"]}
            for p in calcular_pessoas_a_pagar(calculado)]


def calcular_pessoas_a_pagar(calculado: dict) -> list:
    return [p for p in calculado["pessoas"]
            if p["pagar"] and p["valor"] > 0 and not p["gerada"]]


# ---------------------------------------------------------------------------
# O LOTE — o que cada geração levou
# ---------------------------------------------------------------------------
def registrar_lote(analise_id, linhas, destino_da_conta: dict, quem: str = "") -> int:
    """Guarda o que a geração levou (e com isso esconde da tela)."""
    from .db import conexao
    if not _pronto():
        raise ErroDaDC('atualização do banco pendente (047).')
    total = sum((Decimal(str(l["valor"])) for l in linhas), Decimal("0.00"))
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.dc_lote (analise_id, criado_por, total) "
            "VALUES (?, ?, ?) RETURNING id", (analise_id, str(quem or "")[:120], total))
        lote = cur.fetchone()[0]
        cur.close()
        conn.executemany(
            "INSERT INTO analisesps.dc_linha (lote_id, chave, card_id, cpf, nome, obra, "
            "  conta, tipo_despesa, categoria, record_id, carteira, valor, destino) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(lote, l["chave"], l["card_id"], l["cpf"], l["nome"], l["obra"],
              l["conta"], l["tipo_despesa"], l["categoria"], l["record_id"],
              l["carteira"], Decimal(str(l["valor"])),
              destino_da_conta.get(l["conta"], "")) for l in linhas])
        conn.commit()
    _cache.pop("data", None)
    logger.info("DC: lote %s gravado por %s — %d linha(s), R$ %s.", lote,
                quem or "(sem nome)", len(linhas), total)
    return lote


def apagar_lotes(analise_ids) -> int:
    from .db import conexao
    ids = [int(i) for i in analise_ids or [] if str(i).strip().isdigit()]
    if not ids or not _pronto():
        return 0
    marcas = ", ".join("?" for _ in ids)
    with conexao() as conn:
        cur = conn.execute(f"DELETE FROM analisesps.dc_lote WHERE analise_id IN ({marcas})",
                           tuple(ids))
        n = cur.rowcount or 0
        cur.close()
        conn.commit()
    return n


def lote_da_analise(analise_id: int) -> dict | None:
    """`{id, cards_movidos, linhas: [...]}` do lote desta geração."""
    from .db import consultar, consultar_um
    if not _pronto():
        return None
    l = consultar_um("SELECT id, cards_movidos FROM analisesps.dc_lote "
                     " WHERE analise_id = ? ORDER BY id DESC LIMIT 1", (int(analise_id),))
    if not l:
        return None
    linhas = [{"chave": x[0], "card_id": x[1], "cpf": x[2], "nome": x[3], "obra": x[4],
               "conta": x[5], "tipo_despesa": x[6], "categoria": x[7],
               "record_id": x[8], "carteira": x[9],
               "valor": Decimal(str(x[10] or 0)), "destino": x[11]}
              for x in consultar(
                  "SELECT chave, card_id, cpf, nome, obra, conta, tipo_despesa, "
                  "       categoria, record_id, carteira, valor, destino "
                  "  FROM analisesps.dc_linha WHERE lote_id = ? ORDER BY nome", (l[0],))]
    return {"id": l[0], "cards_movidos": bool(l[1]), "linhas": linhas}


def mover_card_de_origem(card_id) -> None:
    """Marca `mover_card` = Sim e move o card de origem para a fase de
    processados — o que o script fazia ao final. ⚠️ SEM VOLTA por aqui."""
    from . import pipefy
    pipefy.atualizar_campos(card_id, [{"campo": CAMPO_MOVER, "valor": "Sim"}])
    pipefy.mover_card(card_id, FASE_PROCESSADO)


def marcar_cards_movidos(lote_id: int) -> None:
    from .db import conexao
    with conexao() as conn:
        conn.execute("UPDATE analisesps.dc_lote SET cards_movidos = true WHERE id = ?",
                     (int(lote_id),))
        conn.commit()


# ---------------------------------------------------------------------------
# O RELATÓRIO — o mesmo das outras folhas
# ---------------------------------------------------------------------------
def montado_do_relatorio(calculado: dict, pessoas=None, filtros=None,
                         so_a_pagar: bool = False, geradas_entram: bool = False,
                         sp: str = "") -> dict:
    """O montado de `folha_relatorio`: uma linha por solicitação × colaborador.

    `geradas_entram`: o relatório de um pagamento JÁ FEITO (refeito depois do
    lançamento, com o número da SP) — as linhas dele estão geradas, e é delas
    que ele fala. `sp`: o número da SP do Pipefy, que vai no subtítulo."""
    from .horario import agora
    pessoas = calculado["pessoas"] if pessoas is None else pessoas
    linhas = []
    for p in pessoas:
        entra = bool(p["pagar"] and p["valor"] > 0
                     and (geradas_entram or not p["gerada"]))
        if so_a_pagar and not entra:
            continue
        por_obra = [{"obra": p["obra"], "dias": p["quantidade"], "valor": p["valor"]}]
        linhas.append({
            "entra": entra, "cpf": p["cpf"], "nome_na_tela": p["nome"] or p["cpf_bonito"],
            "cargo": p["cargo"], "fase": p["fase"], "por_obra": por_obra, "por_dia": [],
            "dias_no_ponto": p["quantidade"], "valor": p["valor"],
            "valor_por_dia": p["valor_diaria_informada"] or p["valor_diaria_cadastrada"],
            "obras_resumo": p["obra"] or "-", "obra_do_cadastro": p["obra_cadastro"],
            "contas": [p["conta"]] if p["conta"] else [],
            "situacao_rotulo": f"{p['tipo_despesa']} · card {p['card_id']}"
                               + (f" · SP {p['sp']['id']}" if p.get("sp") else "")})
    hoje = agora().strftime("%d/%m/%Y")
    rotulo = "Solicitações do Pipefy" + (f" · SP nº {sp}" if sp else "")
    return {"titulo": f"Despesas com colaboradores {hoje}",
            "prefixo_arquivo": "Despesas com colaboradores",
            "folha": {"competencia": hoje, "rotulo_do_tipo": rotulo},
            "pessoas": linhas, "filtros_texto": [], "filtros": filtros or {},
            "totais": {"pessoas": len(pessoas)}, "fechamento": None}
