# -*- coding: utf-8 -*-
"""
A CONCILIAÇÃO BANCÁRIA — o controle paralelo que hoje vive numa planilha.

Pedido do dono em 24/09/2026. Ele mantém, há anos, uma planilha "Controle de
Conciliação" com uma aba por conta: cola o extrato do banco, marca o que já
bateu no OMIE, e anota pendência. A conciliação de verdade continua no OMIE —
esta é a visão dele, que ele considera mais legível, e o lugar onde cabe a
ANOTAÇÃO, que o OMIE não tem.

⚠️ ISTO NÃO É UM SEGUNDO RAZÃO. Aqui não se lança nada: só entra o que o banco
diz (o OFX) ou o que já estava na planilha. O que a pessoa faz é MARCAR e
ANOTAR. Confundir os dois papéis faria o sistema virar uma fonte de verdade
paralela ao OMIE, que é exatamente o que ninguém quer manter.

⚠️ TUDO AQUI TEM DE SOBREVIVER À MIGRAÇÃO NÃO APLICADA. O código sobe para o
Render antes de o dono apertar "Aplicar atualizações do banco" — e uma tela
que estoure nesse intervalo derruba a confiança na publicação inteira. Por
isso toda leitura passa por `_pronto()`.
"""
from __future__ import annotations

import logging
from decimal import Decimal

logger = logging.getLogger("analisesps.conciliacao")

# Quantas linhas a tela mostra de uma vez. A conciliação se faz olhando uma
# conta num período — não varrendo 60 mil linhas.
POR_PAGINA = 300

# O teto de linhas de um arquivo só. Um OFX de um ano tem alguns milhares.
MAX_LINHAS_POR_ARQUIVO = 20000


class ErroDaConciliacao(RuntimeError):
    """Falha com mensagem pronta para a tela."""


def _pronto() -> bool:
    """A migração 019 já rodou? Enquanto não, a tela avisa em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.conciliacao_conta LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela ainda não existe é estado normal
        return False


def estado() -> dict:
    """O que a tela precisa saber antes de desenhar qualquer coisa."""
    if not _pronto():
        return {"pronto": False, "contas": 0, "linhas": 0}
    from .db import consultar_um
    linha = consultar_um(
        "SELECT (SELECT count(*) FROM analisesps.conciliacao_conta WHERE ativa), "
        "       (SELECT count(*) FROM analisesps.conciliacao_extrato)")
    return {"pronto": True, "contas": int((linha or (0, 0))[0] or 0),
            "linhas": int((linha or (0, 0))[1] or 0)}


# ---------------------------------------------------------------------------
# AS CONTAS
# ---------------------------------------------------------------------------
def contas(so_ativas: bool = True) -> list[dict]:
    """As contas cadastradas, na ordem em que o dono quer vê-las."""
    if not _pronto():
        return []
    from .db import consultar
    onde = " WHERE ativa" if so_ativas else ""
    linhas = consultar(
        "SELECT id, nome, banco, agencia, numero, ofx_bankid, ofx_acctid, "
        "       aba_planilha, ativa, ordem, observacao "
        f"  FROM analisesps.conciliacao_conta{onde} "
        " ORDER BY ordem, lower(nome)")
    nomes = ["id", "nome", "banco", "agencia", "numero", "ofx_bankid",
             "ofx_acctid", "aba_planilha", "ativa", "ordem", "observacao"]
    return [dict(zip(nomes, linha)) for linha in linhas]


def gravar_conta(dados: dict, quem: str = "") -> int:
    """Cria ou atualiza uma conta. Devolve o id."""
    nome = str(dados.get("nome") or "").strip()
    if not nome:
        raise ErroDaConciliacao("A conta precisa de um nome.")
    from .db import conexao

    campos = {
        "nome": nome,
        "banco": str(dados.get("banco") or "").strip(),
        "agencia": str(dados.get("agencia") or "").strip(),
        "numero": str(dados.get("numero") or "").strip(),
        "ofx_bankid": _so_digitos(dados.get("ofx_bankid")),
        "ofx_acctid": str(dados.get("ofx_acctid") or "").strip(),
        "aba_planilha": str(dados.get("aba_planilha") or "").strip(),
        "observacao": str(dados.get("observacao") or "").strip(),
        "ordem": int(dados.get("ordem") or 0),
        "ativa": bool(dados.get("ativa", True)),
    }
    conta_id = dados.get("id")
    with conexao() as con:
        if conta_id:
            con.execute(
                "UPDATE analisesps.conciliacao_conta SET nome=?, banco=?, "
                "       agencia=?, numero=?, ofx_bankid=?, ofx_acctid=?, "
                "       aba_planilha=?, observacao=?, ordem=?, ativa=? "
                " WHERE id=?",
                (campos["nome"], campos["banco"], campos["agencia"],
                 campos["numero"], campos["ofx_bankid"], campos["ofx_acctid"],
                 campos["aba_planilha"], campos["observacao"], campos["ordem"],
                 campos["ativa"], int(conta_id)))
            con.commit()
            logger.info("Conciliação: %s alterou a conta %s.", quem or "?", nome)
            return int(conta_id)
        cur = con.execute(
            "INSERT INTO analisesps.conciliacao_conta "
            "  (nome, banco, agencia, numero, ofx_bankid, ofx_acctid, "
            "   aba_planilha, observacao, ordem, ativa) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
            (campos["nome"], campos["banco"], campos["agencia"],
             campos["numero"], campos["ofx_bankid"], campos["ofx_acctid"],
             campos["aba_planilha"], campos["observacao"], campos["ordem"],
             campos["ativa"]))
        novo = cur.fetchone()[0]
        con.commit()
    logger.info("Conciliação: %s criou a conta %s.", quem or "?", nome)
    return int(novo)


def _so_digitos(valor) -> str:
    import re
    return re.sub(r"\D", "", str(valor or ""))


def conta_do_extrato(bankid: str, acctid: str) -> dict | None:
    """De qual conta é este arquivo? — respondido pelo próprio arquivo.

    ⚠️ A COMPARAÇÃO DA CONTA IGNORA PONTUAÇÃO E ZEROS À ESQUERDA. O mesmo
    banco escreve "0007011-4" num arquivo e "70114" no outro, e um cadastro
    que só casasse por igualdade exata mandaria o dono escolher a conta na
    mão — justo o que ele pediu para não ter de fazer.
    """
    if not _pronto():
        return None
    # ⚠️ ZERO À ESQUERDA CAI DOS DOIS LADOS, banco e conta. O Bradesco manda
    # "237" num arquivo e "0237" no outro, e a conta vem ora "0007011-4", ora
    # "70114". Comparar cru faria o sistema perguntar de qual conta é o
    # arquivo — que é exatamente o que o dono pediu para não ter de responder.
    alvo_banco = _so_digitos(bankid).lstrip("0")
    alvo_conta = _so_digitos(acctid).lstrip("0")
    if not alvo_conta:
        return None
    for conta in contas(so_ativas=False):
        guardado_banco = _so_digitos(conta["ofx_bankid"]).lstrip("0")
        if alvo_banco and guardado_banco and guardado_banco != alvo_banco:
            continue
        guardada = _so_digitos(conta["ofx_acctid"]).lstrip("0")
        if guardada and guardada == alvo_conta:
            return conta
    return None


# ---------------------------------------------------------------------------
# CONFERIR ANTES DE GRAVAR — o pedido mais específico do dono
#
# *"Eu quero, digamos que eu estou numa dúvida, um extrato antigo, eu jogar um
# OFX e o sistema me dizer: ó, esse OFX aqui, todos os lançamentos já estavam
# registrados desse período. Ou então: nesse OFX tinha um lançamento no dia tal
# que não estava lançado."*
#
# ⚠️ POR ISSO CONFERIR E GRAVAR SÃO DUAS COISAS, e a tela chama as duas em
# ordem. Um botão só, que grava enquanto conta o que fez, responderia à
# pergunta DEPOIS de ter agido — e aí a resposta "estava tudo lá" não teria
# como ser verificada, porque ela mesma teria mudado o mundo.
# ---------------------------------------------------------------------------
def conferir(conta_id: int, lido) -> dict:
    """O que este arquivo traz de novo para esta conta. NÃO grava nada.

    Devolve as linhas separadas em `novas` e `ja_estavam`, mais o que sobra do
    lado de cá: as linhas que ESTE sistema tem no período do arquivo e que o
    arquivo NÃO traz. Essa terceira lista é a que acusa linha digitada errada
    ou lançamento que o banco estornou.
    """
    from .db import consultar
    from .conciliacao_ofx import impressao_da_linha

    if len(lido.lancamentos) > MAX_LINHAS_POR_ARQUIVO:
        raise ErroDaConciliacao(
            f"O arquivo tem {len(lido.lancamentos)} lançamentos, acima do teto "
            f"de {MAX_LINHAS_POR_ARQUIVO}. Traga por períodos menores.")

    por_impressao = {}
    for lanc in lido.lancamentos:
        por_impressao[impressao_da_linha(conta_id, lanc)] = lanc

    ja = set()
    if por_impressao and _pronto():
        marcas = list(por_impressao)
        # Em blocos: uma cláusula IN com dez mil marcas é recusada por
        # tamanho, e o arquivo grande é justamente o caso de dúvida antiga.
        for i in range(0, len(marcas), 500):
            bloco = marcas[i:i + 500]
            achadas = consultar(
                "SELECT impressao FROM analisesps.conciliacao_extrato "
                f" WHERE conta_id = ? AND impressao IN ({','.join(['?'] * len(bloco))})",
                tuple([conta_id] + bloco))
            ja.update(linha[0] for linha in achadas)

    novas = [(marca, lanc) for marca, lanc in por_impressao.items()
             if marca not in ja]

    # O outro lado: o que temos no período e o arquivo não traz.
    so_aqui = []
    if lido.periodo_ini and lido.periodo_fim and _pronto():
        linhas = consultar(
            "SELECT id, data, descricao, valor, origem, conciliado "
            "  FROM analisesps.conciliacao_extrato "
            " WHERE conta_id = ? AND data >= ? AND data <= ? "
            "   AND impressao <> ALL(?) "
            " ORDER BY data, id",
            (conta_id, lido.periodo_ini, lido.periodo_fim,
             list(por_impressao) or [""]))
        so_aqui = [dict(zip(["id", "data", "descricao", "valor", "origem",
                             "conciliado"], linha)) for linha in linhas]

    return {
        "conta_id": conta_id,
        "periodo_ini": lido.periodo_ini,
        "periodo_fim": lido.periodo_fim,
        "saldo": lido.saldo,
        "saldo_em": lido.saldo_em,
        "lidas": len(lido.lancamentos),
        "novas": novas,
        "ja_estavam": len(lido.lancamentos) - len(novas),
        "so_aqui": so_aqui,
        "arquivo_repetido": _arquivo_ja_veio(lido.impressao),
    }


def _arquivo_ja_veio(impressao: str) -> dict | None:
    """Este arquivo EXATO já foi importado? Responde antes de qualquer conta."""
    if not impressao or not _pronto():
        return None
    from .db import consultar_um
    linha = consultar_um(
        "SELECT a.id, a.nome_arquivo, a.importado_em, a.importado_por, "
        "       a.linhas_novas, c.nome "
        "  FROM analisesps.conciliacao_arquivo a "
        "  JOIN analisesps.conciliacao_conta c ON c.id = a.conta_id "
        " WHERE a.impressao = ? ORDER BY a.importado_em DESC LIMIT 1",
        (impressao,))
    if not linha:
        return None
    return dict(zip(["id", "nome_arquivo", "importado_em", "importado_por",
                     "linhas_novas", "conta"], linha))


def importar(conta_id: int, lido, nome_arquivo: str = "",
             quem: str = "") -> dict:
    """Grava o que for novo. Reimportar o mesmo extrato não duplica nada.

    ⚠️ A GARANTIA NÃO É ESTE CÓDIGO, É O BANCO: existe um índice único por
    (conta, impressão), e a gravação usa `ON CONFLICT DO NOTHING`. Duas
    pessoas soltando o mesmo arquivo no mesmo segundo é o caso que uma
    conferência feita só aqui em cima não pega.
    """
    from .db import conexao
    from .conciliacao_ofx import impressao_da_linha

    conferido = conferir(conta_id, lido)
    novas = conferido["novas"]

    with conexao() as con:
        cur = con.execute(
            "INSERT INTO analisesps.conciliacao_arquivo "
            "  (conta_id, nome_arquivo, periodo_ini, periodo_fim, "
            "   linhas_lidas, linhas_novas, linhas_repetidas, impressao, "
            "   saldo_ofx, saldo_ofx_em, importado_por) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
            (conta_id, nome_arquivo[:200], lido.periodo_ini, lido.periodo_fim,
             conferido["lidas"], len(novas), conferido["ja_estavam"],
             lido.impressao, lido.saldo, lido.saldo_em, quem))
        arquivo_id = int(cur.fetchone()[0])

        gravadas = 0
        adotadas = 0
        for marca, lanc in novas:
            # ⚠️ ANTES DE CRIAR, PROCURA a mesma linha vinda da planilha: ela é
            # o mesmo lançamento, e traz a anotação do dono junto. Ver o bloco
            # "O ENCONTRO DAS DUAS FONTES", mais abaixo.
            if _adotar_linha_da_planilha(con, conta_id, lanc, marca):
                adotadas += 1
                continue
            descricao = (lanc.memo or "").strip()
            if lanc.nome and lanc.nome not in descricao:
                descricao = f"{lanc.nome} — {descricao}".strip(" —")
            cur = con.execute(
                "INSERT INTO analisesps.conciliacao_extrato "
                "  (conta_id, data, descricao, documento, valor, origem, "
                "   arquivo_id, impressao, fitid) "
                "VALUES (?, ?, ?, ?, ?, 'ofx', ?, ?, ?) "
                "ON CONFLICT (conta_id, impressao) DO NOTHING",
                (conta_id, lanc.data, descricao[:500],
                 (lanc.documento or "")[:60], lanc.valor, arquivo_id, marca,
                 (lanc.fitid or "")[:120]))
            gravadas += (cur.rowcount or 0)
        con.commit()

    logger.info("Conciliação: %s importou %s na conta %s — %s nova(s) e %s "
                "adotada(s) da planilha, de %s.", quem or "?",
                nome_arquivo or "extrato", conta_id, gravadas, adotadas,
                conferido["lidas"])
    return dict(conferido, gravadas=gravadas, adotadas=adotadas,
                arquivo_id=arquivo_id)


# ---------------------------------------------------------------------------
# A LISTA, OS FILTROS E O SALDO
#
# *"O manuseio fluido, rápido — no Excel a gente vai só alterando a coluna e
# colocando alguma observação."* O que isso quer dizer aqui: marcar conciliado
# e escrever observação NÃO recarregam a tela; e o filtro é da barra lateral,
# como no resto do módulo.
# ---------------------------------------------------------------------------
SITUACOES = {
    "todos": "Tudo",
    "pendentes": "Falta conciliar",
    "conciliados": "Já conciliado",
    "com_observacao": "Com observação",
}


def _onde(f: dict) -> tuple[str, list]:
    """O filtro da barra, traduzido. Tudo por parâmetro, nunca costurado."""
    onde = ["conta_id = ?"]
    params: list = [int(f.get("conta_id") or 0)]

    situacao = f.get("situacao") or "todos"
    if situacao == "pendentes":
        onde.append("NOT conciliado")
    elif situacao == "conciliados":
        onde.append("conciliado")
    elif situacao == "com_observacao":
        onde.append("btrim(coalesce(observacao,'')) <> ''")

    if f.get("data_ini"):
        onde.append("data >= ?")
        params.append(f["data_ini"])
    if f.get("data_fim"):
        onde.append("data <= ?")
        params.append(f["data_fim"])

    busca = str(f.get("busca") or "").strip().lower()
    if busca:
        # Cada termo separado por vírgula tem de aparecer — a mesma regra da
        # busca das Solicitações, para não haver duas buscas diferentes no
        # mesmo sistema.
        for termo in [t.strip() for t in busca.split(",") if t.strip()]:
            onde.append("(lower(descricao) || ' ' || lower(coalesce(documento,''))"
                        " || ' ' || lower(coalesce(observacao,''))) LIKE ?")
            params.append(f"%{termo.replace('%', '').replace('_', '')}%")

    if f.get("valor_ini") is not None:
        onde.append("abs(valor) >= ?")
        params.append(abs(Decimal(str(f["valor_ini"]))))
    if f.get("valor_fim") is not None:
        onde.append("abs(valor) <= ?")
        params.append(abs(Decimal(str(f["valor_fim"]))))

    sentido = f.get("sentido") or ""
    if sentido == "entrada":
        onde.append("valor > 0")
    elif sentido == "saida":
        onde.append("valor < 0")

    return " WHERE " + " AND ".join(onde), params


def listar(f: dict, pagina: int = 1) -> list[dict]:
    """Uma página do extrato da conta, com o SALDO CORRIDO já calculado.

    ⚠️ O SALDO CORRIDO É DO BANCO, NÃO DA PÁGINA. Somar em Python só as 300
    linhas da tela daria um "saldo" que recomeça a cada página — um número com
    cara de certo e sem sentido nenhum. Aqui a soma é uma janela sobre a conta
    INTEIRA até aquela linha, ordenada por data; o filtro recorta o que se vê,
    não o que se soma.
    """
    if not _pronto():
        return []
    from .db import consultar
    onde, params = _onde(f)
    pagina = max(1, int(pagina or 1))

    linhas = consultar(
        "SELECT id, data, descricao, documento, valor, conciliado, "
        "       observacao, origem, conciliado_por, conciliado_em, saldo "
        "  FROM ( "
        "    SELECT e.*, sum(valor) OVER (PARTITION BY conta_id "
        "                                 ORDER BY data, id "
        "                                 ROWS UNBOUNDED PRECEDING) AS saldo "
        "      FROM analisesps.conciliacao_extrato e "
        "     WHERE conta_id = ? "
        "  ) t "
        f"{onde} ORDER BY data DESC, id DESC LIMIT ? OFFSET ?",
        tuple([int(f.get("conta_id") or 0)] + params
              + [POR_PAGINA, (pagina - 1) * POR_PAGINA]))
    nomes = ["id", "data", "descricao", "documento", "valor", "conciliado",
             "observacao", "origem", "conciliado_por", "conciliado_em", "saldo"]
    return [dict(zip(nomes, linha)) for linha in linhas]


def resumo(f: dict) -> dict:
    """Os números do recorte — somados pelo BANCO, sobre o filtro inteiro."""
    if not _pronto():
        return {"quantidade": 0, "entradas": 0, "saidas": 0, "saldo": 0,
                "pendentes": 0, "pendentes_valor": 0, "com_observacao": 0}
    from .db import consultar_um
    onde, params = _onde(f)
    linha = consultar_um(
        "SELECT count(*), "
        "       coalesce(sum(valor) FILTER (WHERE valor > 0), 0), "
        "       coalesce(sum(valor) FILTER (WHERE valor < 0), 0), "
        "       coalesce(sum(valor), 0), "
        "       count(*) FILTER (WHERE NOT conciliado), "
        "       coalesce(sum(valor) FILTER (WHERE NOT conciliado), 0), "
        "       count(*) FILTER (WHERE btrim(coalesce(observacao,'')) <> '') "
        f"  FROM analisesps.conciliacao_extrato{onde}", tuple(params))
    nomes = ["quantidade", "entradas", "saidas", "saldo", "pendentes",
             "pendentes_valor", "com_observacao"]
    return dict(zip(nomes, linha or (0,) * 7))


def saldo_da_conta(conta_id: int, ate=None) -> Decimal:
    """O saldo da conta até uma data — a soma de tudo, sem filtro nenhum.

    É o número que se compara com o saldo que o banco declara no OFX. Ele
    IGNORA o filtro da tela de propósito: saldo com filtro não é saldo.
    """
    if not _pronto():
        return Decimal("0")
    from .db import consultar_um
    if ate:
        linha = consultar_um(
            "SELECT coalesce(sum(valor), 0) FROM analisesps.conciliacao_extrato "
            " WHERE conta_id = ? AND data <= ?", (int(conta_id), ate))
    else:
        linha = consultar_um(
            "SELECT coalesce(sum(valor), 0) FROM analisesps.conciliacao_extrato "
            " WHERE conta_id = ?", (int(conta_id),))
    return (linha or (Decimal("0"),))[0] or Decimal("0")


def marcar(ids: list, conciliado: bool, quem: str = "") -> int:
    """Marca (ou desmarca) linhas como conciliadas. Devolve quantas mudaram."""
    numeros = [int(i) for i in (ids or []) if str(i).strip().isdigit()]
    if not numeros:
        return 0
    from .db import conexao
    marcas = ",".join(["?"] * len(numeros))
    with conexao() as con:
        cur = con.execute(
            "UPDATE analisesps.conciliacao_extrato "
            "   SET conciliado = ?, "
            "       conciliado_em = CASE WHEN ? THEN now() ELSE NULL END, "
            "       conciliado_por = CASE WHEN ? THEN ? ELSE '' END, "
            "       alterado_em = now(), alterado_por = ? "
            f" WHERE id IN ({marcas})",
            tuple([bool(conciliado), bool(conciliado), bool(conciliado),
                   quem, quem] + numeros))
        mudadas = cur.rowcount or 0
        con.commit()
    logger.info("Conciliação: %s %s %s linha(s).", quem or "?",
                "conciliou" if conciliado else "desmarcou", mudadas)
    return mudadas


def anotar(linha_id: int, texto: str, quem: str = "") -> str:
    """A observação de uma linha. É o que a planilha tinha e o OMIE não tem."""
    from .db import conexao
    texto = str(texto or "").strip()[:1000]
    with conexao() as con:
        con.execute(
            "UPDATE analisesps.conciliacao_extrato "
            "   SET observacao = ?, alterado_em = now(), alterado_por = ? "
            " WHERE id = ?", (texto, quem, int(linha_id)))
        con.commit()
    return texto


def acrescentar_a_mao(conta_id: int, data, descricao: str, valor,
                      documento: str = "", observacao: str = "",
                      quem: str = "") -> int:
    """Uma linha que o banco não trouxe e precisa existir.

    ⚠️ MARCADA COMO 'mao', SEMPRE. Quem olhar a lista daqui a um ano precisa
    distinguir o que o banco disse do que uma pessoa digitou — são coisas com
    peso diferente numa conferência.
    """
    import hashlib
    from datetime import datetime
    from .db import conexao

    valor = Decimal(str(valor or 0)).quantize(Decimal("0.01"))
    # A digital de uma linha digitada leva o instante: duas linhas idênticas
    # digitadas de propósito continuam sendo duas.
    marca = hashlib.sha256(
        f"mao|conta{conta_id}|{data}|{valor}|{descricao}|"
        f"{datetime.now().isoformat()}".encode("utf-8")).hexdigest()
    with conexao() as con:
        cur = con.execute(
            "INSERT INTO analisesps.conciliacao_extrato "
            "  (conta_id, data, descricao, documento, valor, observacao, "
            "   origem, impressao, alterado_por, alterado_em) "
            "VALUES (?, ?, ?, ?, ?, ?, 'mao', ?, ?, now()) RETURNING id",
            (int(conta_id), data, str(descricao or "")[:500],
             str(documento or "")[:60], valor, str(observacao or "")[:1000],
             marca, quem))
        novo = int(cur.fetchone()[0])
        con.commit()
    logger.info("Conciliação: %s acrescentou uma linha à mão na conta %s.",
                quem or "?", conta_id)
    return novo


# ---------------------------------------------------------------------------
# O QUE A TELA MOSTRA DEPOIS DE LER UM ARQUIVO
# ---------------------------------------------------------------------------
def resumo_para_a_tela(conferido: dict, conta_id: int,
                       nome_arquivo: str = "") -> dict:
    """A conferência traduzida para quem vai ler — e não para quem programou.

    ⚠️ AS LINHAS NOVAS VÃO EM AMOSTRA, NÃO INTEIRAS. Um extrato de um ano tem
    milhares; despejar tudo numa resposta JSON trava o navegador e não ajuda
    ninguém a decidir. Quem quiser ver tudo importa e olha na lista, que tem
    filtro e paginação.
    """
    novas = conferido.get("novas") or []
    amostra = [{
        "data": lanc.data.isoformat(),
        "valor": str(lanc.valor),
        "descricao": (lanc.memo or "")[:120],
    } for _marca, lanc in novas[:15]]

    conta = next((c for c in contas(so_ativas=False)
                  if c["id"] == int(conta_id)), None)
    saldo_aqui = saldo_da_conta(conta_id, conferido.get("periodo_fim"))
    saldo_banco = conferido.get("saldo")

    return {
        "conta_id": conta_id,
        "conta": (conta or {}).get("nome", ""),
        "arquivo": nome_arquivo,
        "periodo_ini": (conferido["periodo_ini"].isoformat()
                        if conferido.get("periodo_ini") else ""),
        "periodo_fim": (conferido["periodo_fim"].isoformat()
                        if conferido.get("periodo_fim") else ""),
        "lidas": conferido.get("lidas", 0),
        "novas": len(novas),
        "ja_estavam": conferido.get("ja_estavam", 0),
        "gravadas": conferido.get("gravadas"),
        "amostra": amostra,
        "amostra_parcial": len(novas) > len(amostra),
        "so_aqui": [{
            "data": l["data"].isoformat() if l.get("data") else "",
            "descricao": (l.get("descricao") or "")[:120],
            "valor": str(l.get("valor") or 0),
            "origem": l.get("origem", ""),
        } for l in (conferido.get("so_aqui") or [])[:15]],
        "so_aqui_total": len(conferido.get("so_aqui") or []),
        "arquivo_repetido": _arquivo_para_a_tela(
            conferido.get("arquivo_repetido")),
        # ⚠️ A CONFERÊNCIA QUE VALE MAIS QUE TODAS: o saldo que o banco declara
        # no próprio arquivo contra o que existe aqui. Se os dois baterem, o
        # extrato está completo — e isso nenhuma contagem de linhas prova.
        "saldo_banco": str(saldo_banco) if saldo_banco is not None else "",
        "saldo_aqui": str(saldo_aqui),
        "saldo_bate": (saldo_banco is not None
                       and Decimal(str(saldo_banco)) == Decimal(str(saldo_aqui))),
    }


def _arquivo_para_a_tela(achado):
    if not achado:
        return None
    return {
        "nome_arquivo": achado.get("nome_arquivo", ""),
        "conta": achado.get("conta", ""),
        "importado_por": achado.get("importado_por", ""),
        "importado_em": (achado["importado_em"].strftime("%d/%m/%Y às %H:%M")
                         if achado.get("importado_em") else ""),
        "linhas_novas": achado.get("linhas_novas", 0),
    }


def ultimos_arquivos(conta_id: int, quantos: int = 8) -> list[dict]:
    """O que já foi importado nesta conta — responde "já mandei este mês?"."""
    if not _pronto() or not conta_id:
        return []
    from .db import consultar
    linhas = consultar(
        "SELECT nome_arquivo, periodo_ini, periodo_fim, linhas_lidas, "
        "       linhas_novas, importado_em, importado_por "
        "  FROM analisesps.conciliacao_arquivo "
        " WHERE conta_id = ? ORDER BY importado_em DESC LIMIT ?",
        (int(conta_id), int(quantos)))
    nomes = ["nome_arquivo", "periodo_ini", "periodo_fim", "linhas_lidas",
             "linhas_novas", "importado_em", "importado_por"]
    return [dict(zip(nomes, linha)) for linha in linhas]


def lembrar_conta_do_extrato(conta_id: int, bankid: str, acctid: str,
                             quem: str = "") -> None:
    """Grava no cadastro o banco e a conta que vieram no arquivo.

    É o que faz o "de qual conta é este extrato?" ser perguntado UMA vez só.
    Não sobrescreve o que já estiver lá: um cadastro certo não pode ser
    trocado por um arquivo que veio de outra conta por engano.
    """
    if not (bankid or acctid):
        return
    from .db import conexao
    with conexao() as con:
        con.execute(
            "UPDATE analisesps.conciliacao_conta "
            "   SET ofx_bankid = CASE WHEN ofx_bankid = '' THEN ? ELSE ofx_bankid END, "
            "       ofx_acctid = CASE WHEN ofx_acctid = '' THEN ? ELSE ofx_acctid END "
            " WHERE id = ?",
            (_so_digitos(bankid), str(acctid or "").strip(), int(conta_id)))
        con.commit()
    logger.info("Conciliação: %s apontou o extrato %s/%s para a conta %s.",
                quem or "?", bankid, acctid, conta_id)


# ---------------------------------------------------------------------------
# A PLANILHA ANTIGA — trazer o histórico sem perder o que foi anotado nele
# ---------------------------------------------------------------------------
def impressao_da_planilha(conta_id: int, linha: dict, ordem: int = 1) -> str:
    """A identidade de uma linha que veio da planilha.

    Não há FITID: a planilha é uma cópia colada do extrato, sem o
    identificador do banco. Então a identidade é data + valor + descrição
    normalizada, mais a ORDEM da repetição — pelo mesmo motivo do OFX: dois
    pagamentos iguais no mesmo dia são dois, e tratá-los como um faria o
    histórico importado divergir do que estava na planilha.
    """
    import hashlib
    import re as _re
    texto = _re.sub(r"\s+", " ", str(linha.get("descricao") or "")).strip().lower()
    base = (f"planilha|{linha['data'].isoformat()}|{linha['valor']}|{texto}"
            f"|#{ordem}")
    return hashlib.sha256(f"conta{conta_id}|{base}".encode("utf-8")).hexdigest()


def importar_da_planilha(conta_id: int, lido: dict, quem: str = "") -> dict:
    """Grava as linhas de uma aba. Reimportar a mesma aba não duplica nada.

    ⚠️ O QUE FOI ANOTADO NA PLANILHA VEM JUNTO — o "Conciliado" e as colunas
    soltas viram marca e observação. Trazer só os números e deixar o dono
    remarcar dois anos de conciliação tornaria a importação inútil.
    """
    from .db import conexao

    linhas = lido.get("linhas") or []
    vistas: dict = {}
    gravadas = repetidas = 0

    with conexao() as con:
        for linha in linhas:
            marca_base = (linha["data"], linha["valor"],
                          (linha.get("descricao") or "").strip().lower())
            vistas[marca_base] = vistas.get(marca_base, 0) + 1
            marca = impressao_da_planilha(conta_id, linha, vistas[marca_base])
            cur = con.execute(
                "INSERT INTO analisesps.conciliacao_extrato "
                "  (conta_id, data, descricao, documento, valor, conciliado, "
                "   conciliado_por, conciliado_em, observacao, origem, "
                "   impressao) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, "
                "        CASE WHEN ? THEN now() ELSE NULL END, ?, "
                "        'planilha', ?) "
                "ON CONFLICT (conta_id, impressao) DO NOTHING",
                (conta_id, linha["data"], (linha.get("descricao") or "")[:500],
                 (linha.get("documento") or "")[:60], linha["valor"],
                 bool(linha.get("conciliado")),
                 quem if linha.get("conciliado") else "",
                 bool(linha.get("conciliado")),
                 (linha.get("observacao") or "")[:1000], marca))
            if cur.rowcount:
                gravadas += 1
            else:
                repetidas += 1
        con.commit()

    logger.info("Conciliação: %s importou a aba %s na conta %s — %s de %s.",
                quem or "?", lido.get("aba", "?"), conta_id, gravadas,
                len(linhas))
    return {"gravadas": gravadas, "repetidas": repetidas,
            "lidas": len(linhas), "aba": lido.get("aba", ""),
            "conciliadas": lido.get("conciliadas", 0),
            "descartadas": len(lido.get("descartadas") or [])}


# ---------------------------------------------------------------------------
# ⚠️ O ENCONTRO DAS DUAS FONTES — o problema que só aparece na segunda semana
#
# O dono importa a planilha (anos de histórico, com o que ele anotou) e depois
# solta um OFX do mesmo período. As duas linhas são o MESMO lançamento, mas a
# do banco tem FITID e a da planilha não — identidades diferentes, e o extrato
# duplicaria inteiro. Pior: a cópia nova viria sem a anotação dele.
#
# Por isso, antes de inserir uma linha do OFX, procura-se uma linha da
# PLANILHA igual em data e valor que ainda não tenha sido confirmada pelo
# banco. Achando, ela é ADOTADA: ganha o FITID e a identidade do banco, e
# mantém a marca de conciliado e a observação que já tinha.
# ---------------------------------------------------------------------------
def _adotar_linha_da_planilha(con, conta_id: int, lanc, marca: str) -> bool:
    """Achou a mesma linha vinda da planilha? Então é ela, e não uma nova."""
    cur = con.execute(
        "SELECT id FROM analisesps.conciliacao_extrato "
        " WHERE conta_id = ? AND data = ? AND valor = ? "
        "   AND origem = 'planilha' AND fitid = '' "
        " ORDER BY id LIMIT 1",
        (conta_id, lanc.data, lanc.valor))
    achada = cur.fetchone()
    if not achada:
        return False
    con.execute(
        "UPDATE analisesps.conciliacao_extrato "
        "   SET impressao = ?, fitid = ?, alterado_em = now() "
        " WHERE id = ?",
        (marca, (lanc.fitid or "")[:120], int(achada[0])))
    return True
