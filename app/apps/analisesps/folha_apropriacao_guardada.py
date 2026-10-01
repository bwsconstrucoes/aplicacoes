# -*- coding: utf-8 -*-
"""
A APROPRIAÇÃO GUARDADA: o que ele mexeu à mão, e o que já foi pago.

⚠️ ESTE MÓDULO EXISTE PARA QUE `folha_apropriacao.py` CONTINUE SEM BANCO. Lá é
só conta — função pura, verificável sem subir nada. Aqui é o banco: ler, gravar
e congelar. A conta não muda; o que muda é o que sobrevive.

São DUAS coisas diferentes, e confundi-las é o erro que esta docstring existe
para impedir:

**1. O AJUSTE** (`apropriacao_ajuste`) — o que ELE decidiu, poucas linhas por
pagamento. Pedido dele em 26/09/2026:

    "Eu preciso ter a liberdade de escolher, de desmarcar uma pessoa para gerar o
     pagamento ou não (…) caso eu queira alterar a obra que aquela pessoa vai
     ficar apropriada (…) eu não altero a base do ponto, eu altero só a planilha
     naquele momento."

Guarda-se **o ajuste, não o resultado**. Se eu guardasse o resultado com o ajuste
dentro, uma correção no cadastro — ou um ponto que veio pela metade e foi
recarregado — deixaria de aparecer na tela, e ninguém entenderia por quê.

**2. O FECHAMENTO** (`apropriacao` + `apropriacao_linha`) — o resultado
CONGELADO de um pagamento que já foi gerado. Escrito uma vez, nunca recalculado.
Depois que o arquivo foi para o banco, "qual obra pagou o salário do Fulano em
09/2026" tem UMA resposta, para sempre. Recalcular isso faria recarregar o ponto
de setembro, em outubro, mudar a história de um dinheiro que já saiu — e o rateio
do mês seguinte, que ele decide olhando o total por obra, sairia sobre número que
não foi o pago.

⚠️ O FECHAMENTO NÃO É AUTOMÁTICO. Quem fecha é quem gera o arquivo de pagamento,
de propósito: fechar é dizer "foi isto que eu paguei", e sistema nenhum tem como
dizer isso sozinho.
"""
from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

logger = logging.getLogger("analisesps.folha")

CENTAVO = Decimal("0.01")

# A verba padrão do fechamento: a folha da contabilidade. As outras ("alimentacao",
# "transporte", "diaria") congelam na mesma tabela, com a sua etiqueta.
VERBA_FOLHA = "folha"

# Teto de linhas num fechamento. ~500 pessoas × poucas obras cada; 20 mil é folga
# de sobra, e serve para um laço torto não escrever milhões de linhas em silêncio.
MAXIMO_DE_LINHAS = 20_000


class ErroDaApropriacao(RuntimeError):
    """Não deu para guardar. A frase vai inteira para a tela."""


def _pronto() -> bool:
    """A migração 034 já rodou? O código sobe para o Render antes do botão ser
    apertado, então a tela tem de avisar em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.apropriacao_ajuste LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


def _competencia(ano, mes, tipo) -> tuple:
    """Confere a competência antes de tocar no banco."""
    from .folha_arquivo import TIPOS
    try:
        ano, mes = int(ano), int(mes)
    except (TypeError, ValueError):
        raise ErroDaApropriacao("competência inválida.") from None
    if not (2000 <= ano <= 2100) or not (1 <= mes <= 12):
        raise ErroDaApropriacao("competência inválida.")
    tipo = str(tipo or "").strip()
    if tipo not in TIPOS:
        raise ErroDaApropriacao(
            f'tipo de pagamento "{tipo}" não reconhecido. Valores aceitos: "quinzena" e "fim_de_mes".')
    return ano, mes, tipo


def _obra(texto) -> str:
    """A obra sempre em MAIÚSCULAS, como em `folha_apropriacao`.

    Duas grafias da mesma obra ("Crepeolinda" e "CREPEOLINDA") virariam duas
    linhas no total por obra — e é justamente esse total que ele usa para decidir
    o rateio do mês."""
    return " ".join(str(texto or "").split()).upper()[:120]


def _dinheiro(valor) -> Decimal:
    try:
        return Decimal(str(valor or 0)).quantize(CENTAVO)
    except (InvalidOperation, ValueError):
        raise ErroDaApropriacao(f"valor inválido: {valor!r}") from None


# ---------------------------------------------------------------------------
# 1. O QUE ELE MEXEU À MÃO
# ---------------------------------------------------------------------------
def ajustes_do_pagamento(ano: int, mes: int, tipo: str) -> dict:
    """`{cpf: ajuste}` no formato EXATO que `apropriar_pessoa` espera.

    É o encaixe entre o banco e a conta pura: quem chama `folha_apropriacao.
    apropriar` passa isto em `ajustes_por_cpf` e não precisa saber que existe
    banco."""
    from .db import consultar
    if not _pronto():
        return {}
    ano, mes, tipo = _competencia(ano, mes, tipo)

    ajustes: dict = {}
    linhas = consultar(
        "SELECT id, cpf, nome, fora, motivo, obra_unica, observacao, "
        "       alterado_em, alterado_por "
        "  FROM analisesps.apropriacao_ajuste "
        " WHERE ano = ? AND mes = ? AND tipo = ?", (ano, mes, tipo))
    for l in linhas:
        ajustes[l[1]] = {
            "id": l[0], "cpf": l[1], "nome": l[2], "fora": bool(l[3]),
            "motivo": l[4] or "", "obra_unica": l[5] or "",
            "observacao": l[6] or "", "alterado_em": l[7],
            "alterado_por": l[8] or "", "por_obra": []}
    if not ajustes:
        return {}

    # As divisões à mão, de uma vez — uma consulta por pessoa faria dezenas de
    # idas ao banco para montar uma tela.
    por_id = {a["id"]: a for a in ajustes.values()}
    marcas = ",".join(["?"] * len(por_id))
    for l in consultar(
            "SELECT ajuste_id, obra, dias, valor "
            "  FROM analisesps.apropriacao_ajuste_obra "
            f" WHERE ajuste_id IN ({marcas}) ORDER BY obra",
            tuple(por_id)):
        alvo = por_id.get(l[0])
        if alvo is not None:
            alvo["por_obra"].append({"obra": l[1], "dias": int(l[2] or 0),
                                     "valor": _dinheiro(l[3])})
    # ⚠️ QUANDO HÁ DIVISÃO, ELA MANDA e `obra_unica` sai de cena. Deixar os dois
    # preenchidos faria `apropriar_pessoa` usar a divisão e a tela mostrar a obra
    # única — dois números diferentes para a mesma pessoa, na mesma tela.
    for a in ajustes.values():
        if a["por_obra"]:
            a["obra_unica"] = ""
    return ajustes


def gravar_ajuste(ano: int, mes: int, tipo: str, cpf: str, nome: str = "",
                  fora: bool = False, motivo: str = "", obra_unica: str = "",
                  por_obra=None, observacao: str = "", quem: str = "") -> int:
    """Guarda o que ele decidiu para UMA pessoa neste pagamento. Devolve o id.

    ⚠️ TIRAR ALGUÉM DO PAGAMENTO **NÃO** EXIGE MOTIVO — e isto é correção de
    29/09/2026, contra uma regra que eu inventei. Eu exigia o motivo e escrevi aqui
    que "sumiu é a pior resposta possível num pagamento". Ele respondeu:

        *"Quem não entra de onde do arquivo de pagamento? Não pago e ponto final.
        A gestão do pagamento é minha, eu decido."*

    Ele está certo e o erro é de quem eu pensei que estava servindo: o que ele faz
    hoje na planilha é marcar `Pagar QZ` / `Não Pagar` numa célula — um tique, sem
    campo de justificativa. Exigir motivo transformava um clique em formulário, e
    em 400 linhas isso é trabalho inventado por mim para tranquilizar a mim.

    O campo continua existindo e **opcional**: quando ele quiser escrever, o
    relatório mostra. O que saiu é a obrigação."""
    from .db import conexao
    from .folha_rateio import cpf_bonito, cpf_valido, so_digitos

    if not _pronto():
        raise ErroDaApropriacao(
            'tabela de ajustes não encontrada no banco. Clique em "Aplicar atualizações '
            'do banco" em Configurações e repita a operação.')
    ano, mes, tipo = _competencia(ano, mes, tipo)

    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        raise ErroDaApropriacao("selecione o colaborador — CPF incompleto.")
    if not cpf_valido(digitos):
        raise ErroDaApropriacao(
            f"o CPF {cpf_bonito(digitos)} tem dígito verificador inválido.")

    fora = bool(fora)
    motivo = " ".join(str(motivo or "").split())[:300]

    unica = _obra(obra_unica)
    partes = []
    for p in (por_obra or []):
        obra = _obra((p or {}).get("obra"))
        if not obra:
            raise ErroDaApropriacao("uma das linhas está sem obra.")
        partes.append({"obra": obra, "dias": int((p or {}).get("dias") or 0),
                       "valor": _dinheiro((p or {}).get("valor"))})
    if partes and unica:
        # Os dois caminhos juntos é ambiguidade sobre dinheiro: a divisão manda, e
        # isso fica DITO em vez de escolhido em silêncio.
        raise ErroDaApropriacao(
            "selecione apenas uma opção: obra única ou divisão por "
            "obra. As duas juntas tornam o valor ambíguo.")
    if not fora and not unica and not partes:
        raise ErroDaApropriacao(
            "informe o tratamento deste colaborador: retirar do pagamento, apropriar "
            "em obra única ou dividir entre obras.")

    # ⚠️ OBRA REPETIDA NA DIVISÃO É ERRO DE DIGITAÇÃO, e somar as duas caladamente
    # esconderia o erro dentro de um total que parece certo.
    vistas = [p["obra"] for p in partes]
    repetida = next((o for o in vistas if vistas.count(o) > 1), "")
    if repetida:
        raise ErroDaApropriacao(
            f'a obra "{repetida}" aparece mais de uma vez na divisão. '
            "Consolide as linhas em uma só.")

    nome = " ".join(str(nome or "").split())[:160]
    observacao = " ".join(str(observacao or "").split())[:300]
    with conexao() as conn:
        # Substitui: um ajuste por pessoa e pagamento. Apagar-e-inserir na MESMA
        # transação, porque um UPDATE deixaria as linhas filhas antigas para trás.
        conn.execute(
            "DELETE FROM analisesps.apropriacao_ajuste "
            " WHERE ano = ? AND mes = ? AND tipo = ? AND cpf = ?",
            (ano, mes, tipo, digitos))
        cur = conn.execute(
            "INSERT INTO analisesps.apropriacao_ajuste "
            "  (ano, mes, tipo, cpf, nome, fora, motivo, obra_unica, "
            "   observacao, alterado_por) "
            " VALUES (?,?,?,?,?,?,?,?,?,?) RETURNING id",
            (ano, mes, tipo, digitos, nome, fora, motivo, unica, observacao,
             str(quem or "")[:120]))
        novo = cur.fetchone()[0]
        cur.close()
        for p in partes:
            conn.execute(
                "INSERT INTO analisesps.apropriacao_ajuste_obra "
                "  (ajuste_id, obra, dias, valor) VALUES (?,?,?,?)",
                (novo, p["obra"], p["dias"], p["valor"]))
        conn.commit()
    logger.info(
        "Análise de SPs: apropriação de %s em %02d/%d (%s) ajustada por %s "
        "(fora=%s, obra=%s, partes=%d).",
        digitos, mes, ano, tipo, quem or "(sem nome)", fora, unica or "—",
        len(partes))
    return novo


def limpar_ajuste(ano: int, mes: int, tipo: str, cpf: str) -> bool:
    """Tira o ajuste: a pessoa volta a seguir o ponto e a regra de rateio."""
    from .db import conexao
    from .folha_rateio import so_digitos
    if not _pronto():
        return False
    ano, mes, tipo = _competencia(ano, mes, tipo)
    with conexao() as conn:
        cur = conn.execute(
            "DELETE FROM analisesps.apropriacao_ajuste "
            " WHERE ano = ? AND mes = ? AND tipo = ? AND cpf = ?",
            (ano, mes, tipo, so_digitos(cpf)))
        quantas = cur.rowcount or 0
        cur.close()
        conn.commit()
    return quantas > 0


def listar_ajustes(ano: int, mes: int, tipo: str) -> list:
    """Os ajustes deste pagamento, para a tela mostrar o que foi mexido."""
    ajustes = ajustes_do_pagamento(ano, mes, tipo)
    return sorted(ajustes.values(), key=lambda a: (a["nome"] or "").lower())


# ---------------------------------------------------------------------------
# 2. O FECHAMENTO — o que já foi pago
# ---------------------------------------------------------------------------
def linhas_do_apropriado(apropriado: dict) -> list:
    """As linhas que o fechamento grava: `(cpf, nome, obra, dias, valor, origem)`.

    ⚠️ SEPARADA DO `fechar` PARA A PRÉVIA USAR A MESMA REGRA. A prévia do arquivo
    de pagamento (`folha_pagamento.previa`) tem de sair igual ao que o fechamento
    gravaria — se cada uma montasse as linhas do seu jeito, a prévia mostraria um
    arquivo e o fechamento pagaria outro."""
    linhas = []
    for pessoa in (apropriado or {}).get("pessoas") or []:
        if pessoa.get("fora"):
            continue
        for parte in pessoa.get("por_obra") or []:
            linhas.append((
                str(pessoa.get("cpf") or "")[:11],
                str(pessoa.get("nome_cadastro") or pessoa.get("nome") or "")[:160],
                _obra(parte.get("obra")), int(parte.get("dias") or 0),
                _dinheiro(parte.get("valor")),
                str(parte.get("origem") or "")[:20]))
    return linhas


def fechar(ano: int, mes: int, tipo: str, apropriado: dict,
           verba: str = VERBA_FOLHA, quem: str = "") -> int:
    """Congela o resultado de `folha_apropriacao.apropriar`. Devolve o id.

    `apropriado` é o dicionário que a conta devolveu — este módulo não recalcula
    nada, só grava. Refechar a mesma competência SUBSTITUI o fechamento anterior.

    ⚠️ FECHA MESMO QUANDO NÃO BATE, e de propósito: a tela é que decide se deixa
    gerar o arquivo. Um fechamento que não fechava tem de ficar guardado dizendo
    que não fechava — apagar essa informação é apagar a explicação de uma
    diferença que alguém vai procurar depois."""
    from .db import conexao
    if not _pronto():
        raise ErroDaApropriacao(
            'tabelas da apropriação não encontradas no banco. Clique em "Aplicar '
            'atualizações do banco" em Configurações.')
    ano, mes, tipo = _competencia(ano, mes, tipo)
    verba = " ".join(str(verba or VERBA_FOLHA).split()).lower()[:40] or VERBA_FOLHA
    if not isinstance(apropriado, dict) or "pessoas" not in apropriado:
        raise ErroDaApropriacao("apropriação não recebida para gravação.")

    linhas = linhas_do_apropriado(apropriado)
    if len(linhas) > MAXIMO_DE_LINHAS:
        raise ErroDaApropriacao(
            f"a apropriação tem {len(linhas)} linhas, acima do teto de "
            f"{MAXIMO_DE_LINHAS}. Quantidade incompatível com um pagamento — verifique "
            "antes de gravar.")

    with conexao() as conn:
        conn.execute(
            "DELETE FROM analisesps.apropriacao "
            " WHERE ano = ? AND mes = ? AND tipo = ? AND verba = ?",
            (ano, mes, tipo, verba))
        cur = conn.execute(
            "INSERT INTO analisesps.apropriacao "
            "  (ano, mes, tipo, verba, total_da_folha, total_apropriado, "
            "   pessoas, fecha, fechado_por) "
            " VALUES (?,?,?,?,?,?,?,?,?) RETURNING id",
            (ano, mes, tipo, verba,
             _dinheiro(apropriado.get("total_da_folha")),
             _dinheiro(apropriado.get("total_apropriado")),
             len([p for p in (apropriado.get("pessoas") or [])
                  if not p.get("fora")]),
             bool(apropriado.get("fecha")), str(quem or "")[:120]))
        novo = cur.fetchone()[0]
        cur.close()
        for linha in linhas:
            conn.execute(
                "INSERT INTO analisesps.apropriacao_linha "
                "  (apropriacao_id, cpf, nome, obra, dias, valor, origem) "
                " VALUES (?,?,?,?,?,?,?)", (novo,) + linha)
        conn.commit()
    logger.info(
        "Análise de SPs: apropriação de %02d/%d (%s, %s) FECHADA por %s — "
        "%d linha(s), fecha=%s.", mes, ano, tipo, verba, quem or "(sem nome)",
        len(linhas), bool(apropriado.get("fecha")))
    return novo


def fechamento(ano: int, mes: int, tipo: str, verba: str = VERBA_FOLHA):
    """O fechamento guardado, ou None quando este pagamento não foi fechado."""
    from .db import consultar_um
    if not _pronto():
        return None
    ano, mes, tipo = _competencia(ano, mes, tipo)
    linha = consultar_um(
        "SELECT id, total_da_folha, total_apropriado, pessoas, fecha, "
        "       fechado_em, fechado_por "
        "  FROM analisesps.apropriacao "
        " WHERE ano = ? AND mes = ? AND tipo = ? AND verba = ?",
        (ano, mes, tipo, str(verba or VERBA_FOLHA).lower()))
    if not linha:
        return None
    return {"id": linha[0], "ano": ano, "mes": mes, "tipo": tipo,
            "verba": str(verba or VERBA_FOLHA).lower(),
            "total_da_folha": _dinheiro(linha[1]),
            "total_apropriado": _dinheiro(linha[2]),
            "pessoas": int(linha[3] or 0), "fecha": bool(linha[4]),
            "fechado_em": linha[5], "fechado_por": linha[6] or ""}


def esta_fechado(ano: int, mes: int, tipo: str,
                 verba: str = VERBA_FOLHA) -> bool:
    """⚠️ A PERGUNTA QUE PROTEGE HISTÓRIA DE PAGAMENTO. Quem for reimportar a folha
    ou recarregar o ponto de uma competência fechada tem de avisar antes."""
    return fechamento(ano, mes, tipo, verba) is not None


def totais_por_obra(ano: int, mes: int, tipo: str,
                    verba: str = VERBA_FOLHA) -> list:
    """Quanto cada obra levou neste pagamento, do fechamento guardado.

    ⚠️ É O NÚMERO COM QUE ELE DECIDE O RATEIO DO MÊS SEGUINTE — *"é olhando o
    total por obra que eu decido o rateio"*. Por isso vem do congelado, e não de
    um recálculo: o rateio se decide sobre o que foi pago, não sobre o que a conta
    diria hoje."""
    from .db import consultar
    guardado = fechamento(ano, mes, tipo, verba)
    if not guardado:
        return []
    linhas = consultar(
        "SELECT obra, COUNT(DISTINCT cpf), SUM(valor) "
        "  FROM analisesps.apropriacao_linha "
        " WHERE apropriacao_id = ? GROUP BY obra ORDER BY SUM(valor) DESC",
        (guardado["id"],))
    return [{"obra": l[0], "pessoas": int(l[1] or 0), "total": _dinheiro(l[2])}
            for l in linhas]


def por_pessoa(cpf: str, teto: int = 60) -> list:
    """Onde o dinheiro desta pessoa entrou, pagamento por pagamento.

    É a resposta de auditoria: *"em qual obra o salário do Fulano caiu?"*."""
    from .db import consultar
    from .folha_rateio import so_digitos
    if not _pronto():
        return []
    digitos = so_digitos(cpf)
    if len(digitos) != 11:
        return []
    linhas = consultar(
        "SELECT a.ano, a.mes, a.tipo, a.verba, l.obra, l.dias, l.valor, l.origem "
        "  FROM analisesps.apropriacao_linha l "
        "  JOIN analisesps.apropriacao a ON a.id = l.apropriacao_id "
        " WHERE l.cpf = ? "
        " ORDER BY a.ano DESC, a.mes DESC, a.tipo, l.obra LIMIT ?",
        (digitos, int(teto)))
    return [{"ano": l[0], "mes": l[1], "tipo": l[2], "verba": l[3],
             "obra": l[4], "dias": int(l[5] or 0), "valor": _dinheiro(l[6]),
             "origem": l[7] or ""} for l in linhas]


def reabrir(ano: int, mes: int, tipo: str, verba: str = VERBA_FOLHA,
            quem: str = "") -> bool:
    """Apaga o fechamento. ⚠️ É DESTRUTIVO E APAGA HISTÓRIA DE PAGAMENTO.

    Existe porque errar o fechamento acontece — gerar o arquivo com uma pessoa a
    menos, por exemplo. Mas quem apertar isto está dizendo "o que está guardado
    não foi o que eu paguei", e a tela tem de deixar isso claro e pedir
    confirmação."""
    from .db import conexao
    if not _pronto():
        return False
    ano, mes, tipo = _competencia(ano, mes, tipo)
    with conexao() as conn:
        cur = conn.execute(
            "DELETE FROM analisesps.apropriacao "
            " WHERE ano = ? AND mes = ? AND tipo = ? AND verba = ?",
            (ano, mes, tipo, str(verba or VERBA_FOLHA).lower()))
        quantas = cur.rowcount or 0
        cur.close()
        conn.commit()
    if quantas:
        logger.warning(
            "Análise de SPs: fechamento de %02d/%d (%s, %s) APAGADO por %s.",
            mes, ano, tipo, verba, quem or "(sem nome)")
    return quantas > 0


def fechamentos(teto: int = 40) -> list:
    """Os pagamentos já fechados, do mais novo para o mais antigo."""
    from .db import consultar
    if not _pronto():
        return []
    linhas = consultar(
        "SELECT ano, mes, tipo, verba, total_apropriado, pessoas, fecha, "
        "       fechado_em, fechado_por FROM analisesps.apropriacao "
        " ORDER BY ano DESC, mes DESC, tipo LIMIT ?", (int(teto),))
    return [{"ano": l[0], "mes": l[1], "tipo": l[2], "verba": l[3],
             "total": _dinheiro(l[4]), "pessoas": int(l[5] or 0),
             "fecha": bool(l[6]), "fechado_em": l[7],
             "fechado_por": l[8] or ""} for l in linhas]
