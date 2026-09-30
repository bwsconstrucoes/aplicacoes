# -*- coding: utf-8 -*-
"""
GERAR O PAGAMENTO DA FOLHA: do que está guardado até o arquivo no Drive.

Este módulo é a ORQUESTRAÇÃO — ele lê do banco, chama a conta, sobe no Drive e
registra no log. A conta em si não está aqui:

    folha_apropriacao.py           a conta pura (de quem é o dinheiro)
    folha_apropriacao_guardada.py  o que ele ajustou e o que já foi pago
    folha_geracao.py               o layout dos arquivos e a divisão em lotes
    ESTE                           junta tudo e devolve os links

⚠️ SÓ SE GERA PAGAMENTO DE APROPRIAÇÃO FECHADA. Não é burocracia: o arquivo vai
para o portal e o dinheiro sai. Se o arquivo pudesse sair de um cálculo em memória,
recarregar o ponto depois mudaria a explicação de um dinheiro que já saiu — e o
rateio do mês seguinte, que ele decide olhando o total por obra, sairia sobre
número que não foi o pago.

⚠️ O QUE ELE DECIDIU SOBRE OS ARQUIVOS (27/09/2026):

  - **tudo no Drive**, sem Dropbox: *"pode ser tudo Drive, sem problema nenhum"*;
  - **link público, e está decidido**: *"questão de ser público, link, não tem
    problema, pode seguir como está (…) e o que é que alguém vai fazer com isso?
    Pagar o funcionário?"* — o risco está escrito em `docs/FOLHA_DE_PAGAMENTO.md`
    §8 e a decisão é dele;
  - **no mínimo DOIS arquivos**: o de pagamento e o de análise;
  - **o card recebe o link do arquivo**, e o log fica aqui na aplicação.
"""
from __future__ import annotations

import logging
from decimal import Decimal

from . import folha_apropriacao_guardada as guardada
from . import folha_geracao as geracao

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

# A etiqueta do arquivo de conferência no log. Não é destino de pagamento — é o
# segundo arquivo, o que gente lê.
ANALISE = "analise"


class ErroDoPagamento(RuntimeError):
    """Não deu para gerar. A frase vai inteira para a tela."""


def _pronto() -> bool:
    """A migração 035 já rodou?"""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.folha_arquivo_gerado LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


# ---------------------------------------------------------------------------
# DE QUAL CONTA SAI O DINHEIRO DE CADA OBRA
# ---------------------------------------------------------------------------
def _primeira_conta(texto) -> str:
    """A conta até a PRIMEIRA VÍRGULA.

    ⚠️ REGRA DA PLANILHA, não minha: a coluna `AE` das abas Quinzena e Fim de Mês
    faz `REGEXEXTRACT(...;"^[^,]+")`. Lido em 27/09/2026 e anotado em
    `docs/FOLHA_DE_PAGAMENTO.md` §7.10.5 com a explicação: **uma obra pode ter mais
    de uma conta, e vale a primeira**.

    Sem isto, uma obra com duas contas cadastradas viraria um nome de conta
    inexistente ("7011-4, 22069-8"), e o arquivo sairia endereçado a lugar nenhum."""
    return str(texto or "").split(",")[0].strip()


def conta_por_obra() -> dict:
    """`{obra: conta de pagamento}` — pelo NOME e também pelo CÓDIGO.

    ⚠️ A CONTA VEM DA OBRA, e não da pessoa: é a obra que define de onde o dinheiro
    sai. O caminho é obra → código → conta, e as duas pontas já existem no banco: o
    nome e o código em `referencias_rateio`, a conta em `contas_diarios` (a aba
    "C. Diários" da planilha de apoio).

    ⚠️ INDEXADO PELAS DUAS PONTAS, e isto é conserto de 29/09/2026. A apropriação da
    folha identifica a obra pelo que o PONTO escreve na marcação, que pode ser o
    código; este dicionário era só por nome. Quem procurasse por código não achava
    conta nenhuma, e TODA linha da folha viraria a crítica "obra sem conta" — um
    arquivo inteiro barrado por um de/para que existia e não era consultado.

    ⚠️ OBRA SEM CONTA NÃO DESAPARECE DAQUI: entra com conta vazia, e o gerador
    transforma isso numa crítica que diz onde consertar. Omitir a obra faria o
    recado virar "obra não existe", que é o problema errado.

    ⚠️ E NUNCA UM PADRÃO SILENCIOSO. A planilha da aba CTPS resolve a conta por
    expressão sobre o nome da obra e cai numa conta padrão quando nada casa
    (§7.14.4) — obra nova paga pela conta errada sem avisar. Aqui é tabela, e o que
    falta é crítica."""
    # ⚠️ O CAMINHO ESTAVA ERRADO ATÉ 30/09/2026, E ERA DINHEIRO SEM CONTA. O dono:
    # *"tem várias obras que está dizendo que não tem conta. Mas é impossível. Lá
    # na planilha C. Diários tem essas contas."* Tinha.
    #
    # As duas tabelas vêm da MESMA aba ("C. Diários": Código Primário | Conta de
    # Pagamento | Projeto | Código Omie):
    #   - `contas_diarios`:     codigo = CÓDIGO PRIMÁRIO  → conta;
    #   - `referencias_rateio`: nome   = CÓDIGO PRIMÁRIO, codigo = CÓDIGO OMIE.
    # E eu procurava a conta pelo CÓDIGO OMIE — um número que não existe em
    # `contas_diarios`. Só achava por coincidência. Agora a conta é achada pelo
    # código da obra (direto), e a ponte pelo código do OMIE continua valendo
    # para quem o usar.
    from .db import consultar

    def _chave(texto) -> str:
        return " ".join(str(texto or "").split()).upper()

    contas = {}
    try:
        for codigo, conta in consultar(
                "SELECT codigo, coalesce(conta_pagamento, '') "
                "  FROM analisesps.contas_diarios"):
            if _chave(codigo):
                contas[_chave(codigo)] = _primeira_conta(conta)
    except Exception:  # noqa: BLE001 — tabela pode não existir em base nova
        logger.exception("Folha: não consegui ler as contas das obras")
        return {}

    # A própria linha da C. Diários: a obra, pelo código dela, e a conta.
    saida = dict(contas)
    try:
        for nome, codigo in consultar(
                "SELECT nome, coalesce(codigo, '') "
                "  FROM analisesps.referencias_rateio WHERE tipo = 'obra'"):
            nome, codigo = _chave(nome), _chave(codigo)
            conta = contas.get(nome) or contas.get(codigo, "")
            if nome and not saida.get(nome):
                saida[nome] = conta
            # O CÓDIGO (do OMIE) TAMBÉM É CHAVE — só quando não colide com o nome
            # de outra obra, que seria ambiguidade sobre dinheiro.
            if codigo and codigo not in saida:
                saida[codigo] = conta
    except Exception:  # noqa: BLE001
        logger.exception("Folha: não consegui ler as obras")
    return saida


# ---------------------------------------------------------------------------
# AS LINHAS A PAGAR, DO QUE JÁ ESTÁ FECHADO
# ---------------------------------------------------------------------------
def linhas_para_pagar(ano: int, mes: int, tipo: str, verbas) -> list:
    """As linhas de pagamento das verbas pedidas, do fechamento guardado.

    Devolve `[{cpf, nome, obra, conta, verba, valor}]` — exatamente o que
    `folha_geracao.montar_lotes` espera."""
    from .db import consultar

    pedidas = [str(v or "").strip().lower() for v in (verbas or []) if v]
    if not pedidas:
        raise ErroDoPagamento("escolha pelo menos uma verba para pagar.")

    contas = conta_por_obra()
    linhas = []
    faltando = []
    for verba in pedidas:
        fechamento = guardada.fechamento(ano, mes, tipo, verba)
        if not fechamento:
            faltando.append(verba)
            continue
        for cpf, nome, obra, dias, valor in consultar(
                "SELECT cpf, nome, obra, dias, valor "
                "  FROM analisesps.apropriacao_linha "
                " WHERE apropriacao_id = ? ORDER BY nome", (fechamento["id"],)):
            obra = str(obra or "").strip().upper()
            linhas.append({"cpf": cpf, "nome": nome, "obra": obra,
                           "conta": contas.get(obra, ""), "verba": verba,
                           "dias": int(dias or 0),
                           "valor": Decimal(str(valor or 0)).quantize(CENTAVO)})
    if faltando:
        # ⚠️ RECUSA EM VEZ DE GERAR PELA METADE. Um arquivo faltando uma verba sai
        # com cara de completo, e a pessoa recebe a menos sem nada avisando.
        quais = ", ".join(geracao.rotulo_da_verba(v) for v in faltando)
        raise ErroDoPagamento(
            f"{quais} não tem apropriação fechada em {int(mes):02d}/{int(ano)}. "
            "Feche a apropriação dessa verba antes de gerar o pagamento.")
    return linhas


def preparar(ano: int, mes: int, tipo: str, verbas, destino: str,
             juntar_verbas: bool = False) -> dict:
    """O que vai sair, ANTES de sair. Nada é gravado nem sobe para o Drive.

    É o passo que ele pediu: *"mostrar, antes de gerar, quantos arquivos vão sair e
    com que total cada um"*."""
    linhas = linhas_para_pagar(ano, mes, tipo, verbas)
    lotes = geracao.montar_lotes(linhas, destino, juntar_verbas)
    resumo = geracao.resumo_dos_lotes(lotes)
    return {
        "ano": int(ano), "mes": int(mes), "tipo": tipo,
        "competencia": f"{int(mes):02d}/{int(ano)}",
        "destino": destino, "juntar_verbas": bool(juntar_verbas),
        "verbas": [str(v).lower() for v in (verbas or [])],
        "lotes": lotes, "resumo": resumo,
        # No SomaPay não existe juntar, e a tela diz por quê em vez de oferecer.
        "pode_juntar": destino == geracao.BEEVALE,
        "motivo_nao_junta": (
            "" if destino == geracao.BEEVALE else
            "o SomaPay não aceita o mesmo CPF duas vezes no arquivo, então cada "
            "verba sai no seu."),
    }


# ---------------------------------------------------------------------------
# GERAR DE VERDADE: sobe no Drive e registra no log
# ---------------------------------------------------------------------------
def gerar(ano: int, mes: int, tipo: str, verbas, destino: str,
          juntar_verbas: bool = False, quem: str = "",
          forcar: bool = False) -> dict:
    """Gera os arquivos, sobe no Drive e registra. Devolve os links.

    ⚠️ SÃO SEMPRE AO MENOS DOIS ARQUIVOS: o de pagamento (um por conta) e o de
    análise. Ele foi explícito: *"tem que ter no mínimo o do arquivo de pagamento e
    um de análise da folha"*.

    `forcar` gera mesmo com crítica. Existe porque há crítica que ele conhece e
    aceita (obra sem conta numa linha de valor pequeno, por exemplo), e travar sem
    saída faria a folha parar por um detalhe. Mas o padrão é NÃO gerar: o aviso
    existe justamente porque o arquivo sairia errado."""
    from . import drive
    from .beevale import pasta_do_drive

    if not _pronto():
        raise ErroDoPagamento(
            'a tabela do log ainda não existe. Aperte "Aplicar atualizações do '
            'banco" em Configurações e tente de novo.')

    plano = preparar(ano, mes, tipo, verbas, destino, juntar_verbas)
    lotes = plano["lotes"]
    if not lotes:
        raise ErroDoPagamento(
            "não há nada a pagar com essas verbas nesta competência.")
    if not plano["resumo"]["pode_gerar"] and not forcar:
        quantos = len(plano["resumo"]["com_critica"])
        raise ErroDoPagamento(
            f"{quantos} arquivo(s) têm aviso e eu não gerei. Veja a lista na tela: "
            "cada aviso diz o que consertar. Se você já sabe o que é e quer gerar "
            "assim mesmo, marque a opção de gerar com aviso.")

    # ⚠️ CONFERE A PASTA ANTES DE MONTAR NADA. Sem isso, o erro só apareceria na
    # subida do primeiro arquivo — depois de gerar tudo — e viraria um 500 em vez de
    # um recado dizendo onde configurar.
    pasta, _ = pasta_do_drive()
    if not pasta:
        raise ErroDoPagamento(
            "a pasta do Drive não está configurada. Em Configurações, cole o "
            "identificador da pasta onde os arquivos devem ficar.")
    gerados = []
    for lote in lotes:
        nome = geracao.nome_do_arquivo(lote, ano, mes, tipo)
        conteudo = geracao.arquivo_do_lote(lote)
        subido = drive.subir_arquivo(conteudo, nome, pasta)
        gerados.append(_registrar(
            ano, mes, tipo, lote["destino"],
            "+".join(lote.get("verbas") or []), lote.get("conta", ""),
            nome, lote.get("quantos", 0), lote.get("total") or 0,
            subido, "; ".join(lote.get("criticas") or []), quem))

    # O ARQUIVO DE ANÁLISE, sempre, e por último: se algo falhar antes, ninguém
    # fica com um relatório de um pagamento que não foi gerado.
    analise = geracao.analise_xlsx(lotes, ano=ano, mes=mes, tipo=tipo)
    nome_analise = geracao.nome_do_arquivo(
        {"destino": ANALISE, "conta": "", "verbas": plano["verbas"]},
        ano, mes, tipo)
    subido = drive.subir_arquivo(analise, nome_analise, pasta)
    gerados.append(_registrar(
        ano, mes, tipo, ANALISE, "+".join(plano["verbas"]), "", nome_analise,
        plano["resumo"]["pessoas"], plano["resumo"]["total"], subido, "", quem))

    logger.info(
        "Folha: %d arquivo(s) de pagamento gerados para %02d/%d (%s, %s) por %s.",
        len(gerados), int(mes), int(ano), tipo, destino, quem or "(sem nome)")
    return {"ok": True, "arquivos": gerados, "resumo": plano["resumo"],
            "competencia": plano["competencia"]}


def _registrar(ano, mes, tipo, destino, verbas, conta, nome, pessoas, total,
               subido, avisos, quem) -> dict:
    """Uma linha no log, com o link que ele baixa por aqui."""
    from .db import conexao
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO analisesps.folha_arquivo_gerado "
            "  (ano, mes, tipo, destino, verbas, conta, nome_arquivo, pessoas, "
            "   total, drive_id, link, avisos, criado_por) "
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id, criado_em",
            (int(ano), int(mes), str(tipo or ""), str(destino or ""),
             str(verbas or ""), str(conta or ""), str(nome or "")[:300],
             int(pessoas or 0), Decimal(str(total or 0)).quantize(CENTAVO),
             str((subido or {}).get("id") or ""),
             str((subido or {}).get("link") or ""), str(avisos or "")[:1000],
             str(quem or "")[:120]))
        linha = cur.fetchone()
        cur.close()
        conn.commit()
    return {"id": linha[0], "criado_em": linha[1], "nome": nome,
            "destino": destino, "conta": conta, "verbas": verbas,
            "pessoas": int(pessoas or 0),
            "total": Decimal(str(total or 0)).quantize(CENTAVO),
            "link": str((subido or {}).get("link") or ""),
            "avisos": avisos}


def registrar_card(arquivo_id: int, card: str, link_card: str = "") -> bool:
    """Amarra o card do Pipefy ao arquivo já gerado.

    Passo separado de propósito (decidido em 26/09/2026): criar card é opcional, e
    um erro no Pipefy não pode desfazer um arquivo que já está no Drive."""
    from .db import conexao
    if not _pronto():
        return False
    with conexao() as conn:
        cur = conn.execute(
            "UPDATE analisesps.folha_arquivo_gerado "
            "   SET card_pipefy = ?, link_card = ? WHERE id = ?",
            (str(card or "")[:60], str(link_card or "")[:400], int(arquivo_id)))
        mudou = cur.rowcount or 0
        cur.close()
        conn.commit()
    return mudou > 0


def log(teto: int = 100, ano: int = 0, mes: int = 0) -> list:
    """O log dos arquivos gerados, do mais novo para o mais antigo.

    É o que ele pediu: *"que tenha também o log na aplicação (…) com as informações
    e o link que a gente quer baixar por lá."*"""
    from .db import consultar
    if not _pronto():
        return []
    onde, params = "", []
    if ano and mes:
        onde = " WHERE ano = ? AND mes = ?"
        params = [int(ano), int(mes)]
    linhas = consultar(
        "SELECT id, ano, mes, tipo, destino, verbas, conta, nome_arquivo, "
        "       pessoas, total, link, card_pipefy, link_card, avisos, "
        "       criado_em, criado_por "
        "  FROM analisesps.folha_arquivo_gerado" + onde +
        " ORDER BY criado_em DESC, id DESC LIMIT ?", tuple(params + [int(teto)]))
    return [{
        "id": l[0], "ano": l[1], "mes": l[2], "tipo": l[3], "destino": l[4],
        "rotulo_destino": geracao.ROTULO_DO_DESTINO.get(l[4], l[4].capitalize()),
        "verbas": l[5], "rotulo_verbas": " + ".join(
            geracao.rotulo_da_verba(v) for v in (l[5] or "").split("+") if v),
        "conta": l[6], "nome": l[7], "pessoas": int(l[8] or 0),
        "total": Decimal(str(l[9] or 0)).quantize(CENTAVO), "link": l[10],
        "card_pipefy": l[11], "link_card": l[12], "avisos": l[13],
        "criado_em": l[14], "criado_por": l[15],
        "competencia": f"{int(l[2] or 0):02d}/{int(l[1] or 0)}",
    } for l in linhas]


def panorama_do_log() -> dict:
    """Uma linha de resumo para o alto da tela."""
    from .db import consultar_um
    if not _pronto():
        return {"pronto": False, "arquivos": 0, "ultimo": None}
    linha = consultar_um(
        "SELECT COUNT(*), MAX(criado_em) FROM analisesps.folha_arquivo_gerado")
    return {"pronto": True, "arquivos": int((linha or [0])[0] or 0),
            "ultimo": (linha or [0, None])[1]}


# ---------------------------------------------------------------------------
# O GERENCIAL: por obra, por conta e por verba, num lugar só
# ---------------------------------------------------------------------------
# > *"Tem que ter informação gerencial, né, tipo dashboard, para poder estar vendo
# >  qual é o total por obra, porque isso já ajuda nessa questão do rateio. (…)
# >  conseguir em um ambiente visualizar tudo, saber como é que está a distribuição
# >  por obra, por conta."*
#
# ⚠️ ISTO NÃO É ENFEITE: É ENTRADA DO TRABALHO DELE. Ele disse por quê — é olhando o
# total por obra que decide o rateio do mês ("as obras que estão em evidência").
#
# ⚠️ E SAI DO CONGELADO, não de um recálculo. O rateio do mês seguinte se decide
# sobre o que FOI PAGO, não sobre o que a conta diria hoje: recarregar o ponto de
# setembro, em outubro, mudaria o número embaixo de uma decisão já tomada.
# ---------------------------------------------------------------------------
def gerencial(ano: int, mes: int) -> dict:
    """Os totais da competência: por verba, por obra e por conta.

    Junta TODOS os pagamentos fechados do mês (quinzena e fim de mês, todas as
    verbas), porque a pergunta dele é do mês, não do pagamento."""
    from .db import consultar

    if not guardada._pronto():
        return {"pronto": False, "verbas": [], "obras": [], "contas": [],
                "total": Decimal("0.00"), "fechamentos": []}

    fechados = [f for f in guardada.fechamentos(teto=200)
                if f["ano"] == int(ano) and f["mes"] == int(mes)]
    if not fechados:
        return {"pronto": True, "verbas": [], "obras": [], "contas": [],
                "total": Decimal("0.00"), "fechamentos": []}

    contas = conta_por_obra()
    por_verba: dict = {}
    por_obra: dict = {}
    por_conta: dict = {}
    total = Decimal("0.00")

    for fechamento in fechados:
        guardado = guardada.fechamento(ano, mes, fechamento["tipo"],
                                       fechamento["verba"])
        if not guardado:
            continue
        verba = fechamento["verba"]
        for obra, pessoas, soma in consultar(
                "SELECT obra, COUNT(DISTINCT cpf), SUM(valor) "
                "  FROM analisesps.apropriacao_linha "
                " WHERE apropriacao_id = ? GROUP BY obra", (guardado["id"],)):
            valor = Decimal(str(soma or 0)).quantize(CENTAVO)
            obra = str(obra or "") or "(sem obra)"
            total += valor

            alvo = por_verba.setdefault(verba, {
                "verba": verba, "rotulo": geracao.rotulo_da_verba(verba),
                "total": Decimal("0.00"), "pessoas": 0})
            alvo["total"] += valor
            alvo["pessoas"] += int(pessoas or 0)

            alvo = por_obra.setdefault(obra, {"obra": obra,
                                              "total": Decimal("0.00"),
                                              "verbas": set()})
            alvo["total"] += valor
            alvo["verbas"].add(verba)

            # ⚠️ A CONTA VEM DA OBRA. Obra sem conta cai em "(sem conta)" e fica
            # visível: é ela que trava a geração do arquivo depois, e esconder aqui
            # faria a surpresa aparecer só na hora de pagar.
            chave = contas.get(obra) or "(sem conta)"
            alvo = por_conta.setdefault(chave, {"conta": chave,
                                                "total": Decimal("0.00"),
                                                "obras": set()})
            alvo["total"] += valor
            alvo["obras"].add(obra)

    return {
        "pronto": True, "ano": int(ano), "mes": int(mes),
        "competencia": f"{int(mes):02d}/{int(ano)}",
        "total": total,
        "verbas": sorted(por_verba.values(), key=lambda v: -v["total"]),
        "obras": sorted(
            [{"obra": o["obra"], "total": o["total"],
              "verbas": sorted(geracao.rotulo_da_verba(v) for v in o["verbas"])}
             for o in por_obra.values()], key=lambda o: -o["total"]),
        "contas": sorted(
            [{"conta": c["conta"], "total": c["total"],
              "obras": len(c["obras"])} for c in por_conta.values()],
            key=lambda c: -c["total"]),
        "fechamentos": fechados,
        # O rateio pronto, em percentual — é o número que ele leva para a decisão.
        "percentuais": geracao.percentuais_por_obra(
            [{"obra": o["obra"], "total": o["total"]}
             for o in por_obra.values()]),
    }
