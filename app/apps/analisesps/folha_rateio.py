# -*- coding: utf-8 -*-
"""
As regras de rateio da folha — para quem o ponto não pode apropriar.

Pedido do dono em 26/09/2026:

    *"Em algum local a gente eleger as pessoas que vão ser rateadas e, para cada
    uma — ou para um grupo, porque pode ser que tenha variação entre uma e outra
    — definir para quais obras o valor dela vai ser rateado. Um detalhe: pode ser
    que uma obra entre mais que a outra. Tipo assim, uma obra é 50% e o restante
    dividido entre outras."*

Dois casos que não têm dia de ponto utilizável:

1. **Quem não bate ponto por causa da função** — supervisores de obra. Não existe
   dia nenhum para ratear.
2. **Quem bate ponto na MATRIZ ou na FILIAL** (`CONS`, `BWSNE`). O dia existe, mas
   aponta para a matriz de propósito; o valor tem de ir para as obras.

⚠️ ISTO É CADASTRO, NÃO AJUSTE DE UMA FOLHA. O ajuste fino morre com a folha dele
(corrige erro de ponto daquela quinzena). Aqui é como a pessoa SEMPRE é
apropriada — vale para toda quinzena até alguém mudar. Misturar as duas coisas
obrigaria a refazer o mesmo trabalho para as mesmas dez pessoas a cada quinzena.
"""
from __future__ import annotations

import logging
import re
from decimal import ROUND_HALF_UP, Decimal

logger = logging.getLogger("analisesps.folha")

CEM = Decimal("100")
CENTAVO = Decimal("0.01")
SO_DIGITOS = re.compile(r"\D+")


class ErroDoRateio(RuntimeError):
    """Regra que não fecha. A frase vai inteira para a tela."""


def _pronto() -> bool:
    """A migração 027 já rodou? Enquanto não, a tela avisa em vez de estourar."""
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.folha_regra_rateio LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — tabela que ainda não existe é normal
        return False


# ---------------------------------------------------------------------------
# CPF
# ---------------------------------------------------------------------------
def so_digitos(cpf) -> str:
    return SO_DIGITOS.sub("", str(cpf or ""))


def cpf_valido(cpf) -> bool:
    """Confere o dígito verificador.

    ⚠️ NÃO É FRESCURA DE VALIDAÇÃO: é o CPF que liga a folha da contabilidade ao
    ponto e ao pagamento. Um dígito trocado aqui faz a regra de rateio nunca
    encontrar a pessoa — e ela volta a ser apropriada pelo ponto da matriz, sem
    ninguém perceber que a regra estava lá, escrita, e não pegou."""
    d = so_digitos(cpf)
    if len(d) != 11 or d == d[0] * 11:
        return False
    for tamanho in (9, 10):
        soma = sum(int(d[i]) * (tamanho + 1 - i) for i in range(tamanho))
        resto = (soma * 10) % 11
        if resto == 10:
            resto = 0
        if resto != int(d[tamanho]):
            return False
    return True


def cpf_bonito(cpf) -> str:
    d = so_digitos(cpf)
    if len(d) != 11:
        return d
    return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"


# ---------------------------------------------------------------------------
# A REGRA: conferir e distribuir
# ---------------------------------------------------------------------------
def conferir_obras(obras) -> list:
    """Arruma e confere as obras de uma regra. Levanta `ErroDoRateio`.

    Cada obra é `{"obra": str, "percentual": Decimal|None, "resto": bool}`.

    As duas formas que o dono descreveu, e que precisam conviver:

    - **tudo com percentual**, somando exatamente 100;
    - **parte com percentual e parte "o resto"** — "uma obra é 50% e o restante
      dividido entre outras". O resto é dividido em partes iguais entre as obras
      marcadas assim, e ninguém precisa fazer a conta de cabeça.

    ⚠️ SOMAR 99 OU 101 É RECUSADO, e não arredondado por conta própria. Rateio
    que não fecha 100% esconde ou inventa dinheiro — e a diferença apareceria
    depois, num total de obra que ninguém consegue explicar."""
    arrumadas = []
    vistas = set()
    for bruta in obras or []:
        nome = str((bruta or {}).get("obra") or "").strip().upper()
        if not nome:
            continue
        if nome in vistas:
            raise ErroDoRateio(f'A obra "{nome}" está duas vezes na regra.')
        vistas.add(nome)
        resto = bool((bruta or {}).get("resto"))
        bruto = (bruta or {}).get("percentual")
        percentual = None
        if not resto and str(bruto or "").strip() != "":
            try:
                percentual = Decimal(
                    str(bruto).replace("%", "").replace(",", ".").strip()
                ).quantize(Decimal("0.0001"))
            except Exception as e:  # noqa: BLE001
                raise ErroDoRateio(
                    f'O percentual da obra "{nome}" não é um número.') from e
            if percentual <= 0:
                raise ErroDoRateio(
                    f'O percentual da obra "{nome}" tem de ser maior que zero.')
            if percentual > CEM:
                raise ErroDoRateio(
                    f'O percentual da obra "{nome}" passa de 100%.')
        elif not resto:
            raise ErroDoRateio(
                f'Diga o percentual da obra "{nome}", ou marque-a como "o resto".')
        arrumadas.append({"obra": nome, "percentual": percentual, "resto": resto})

    if not arrumadas:
        raise ErroDoRateio("Escolha ao menos uma obra para a regra.")

    fixas = [o for o in arrumadas if not o["resto"]]
    restos = [o for o in arrumadas if o["resto"]]
    soma = sum((o["percentual"] for o in fixas), Decimal("0"))

    if restos:
        if soma >= CEM:
            raise ErroDoRateio(
                f"Os percentuais já somam {soma:.2f}%, então não sobra nada para "
                f"{'a obra' if len(restos) == 1 else 'as obras'} marcada"
                f"{'' if len(restos) == 1 else 's'} como \"o resto\".")
    elif soma != CEM:
        raise ErroDoRateio(
            f"Os percentuais somam {soma:.2f}% — têm de somar 100%. "
            "Ou marque uma obra como \"o resto\" e eu faço a conta.")
    return arrumadas


def percentuais_efetivos(obras) -> list:
    """As obras com o percentual JÁ RESOLVIDO — o "resto" vira número.

    Serve para a tela mostrar o que vai acontecer antes de acontecer, e é a
    mesma conta que `distribuir` usa. Uma segunda conta em outro lugar
    divergiria no primeiro arredondamento."""
    arrumadas = conferir_obras(obras)
    fixas = [o for o in arrumadas if not o["resto"]]
    restos = [o for o in arrumadas if o["resto"]]
    soma = sum((o["percentual"] for o in fixas), Decimal("0"))
    saida = []
    if restos:
        cada = ((CEM - soma) / len(restos)).quantize(Decimal("0.0001"))
    for obra in arrumadas:
        pct = obra["percentual"] if not obra["resto"] else cada
        saida.append({"obra": obra["obra"], "percentual": pct,
                      "resto": obra["resto"]})
    # ⚠️ O RESTO PODE NÃO FECHAR EXATO (100 - 50 dividido por 3 = 16,6667 três
    # vezes = 50,0001). A sobra do PERCENTUAL vai para a maior fatia, pelo mesmo
    # motivo da sobra do centavo: a maior fatia é a que menos sente.
    total = sum(o["percentual"] for o in saida)
    if total != CEM and saida:
        maior = max(saida, key=lambda o: o["percentual"])
        maior["percentual"] = (maior["percentual"] + (CEM - total)).quantize(
            Decimal("0.0001"))
    return saida


def distribuir(valor, obras) -> list:
    """Divide um valor entre as obras da regra. Devolve `[{obra, valor}]`.

    ⚠️ A SOMA DAS PARTES É IGUAL AO VALOR, SEMPRE — e é isso que este código
    existe para garantir. Dividir e arredondar cada parte deixa sobra ou falta de
    centavos, e essa diferença vira uma obra com custo errado e um arquivo de
    pagamento que não bate com a folha.

    A sobra vai para a **maior fatia**, que é a mesma regra que o dono escolheu
    para o rateio por dias em 26/09/2026 ("a obra com mais dias")."""
    total = Decimal(str(valor or 0)).quantize(CENTAVO)
    efetivos = percentuais_efetivos(obras)

    partes = []
    somado = Decimal("0")
    for obra in efetivos:
        parte = (total * obra["percentual"] / CEM).quantize(
            CENTAVO, rounding=ROUND_HALF_UP)
        partes.append({"obra": obra["obra"], "percentual": obra["percentual"],
                       "valor": parte})
        somado += parte

    if partes and somado != total:
        maior = max(partes, key=lambda p: p["percentual"])
        maior["valor"] = (maior["valor"] + (total - somado)).quantize(CENTAVO)
    return partes


# ---------------------------------------------------------------------------
# ONDE AS REGRAS FICAM
# ---------------------------------------------------------------------------
FALTA_MIGRAR = ('A parte do rateio da folha ainda não foi ligada no banco. '
                'Em Configurações, aperte "Aplicar atualizações do banco".')


def listar(so_ativas: bool = False) -> list:
    """Todas as regras, com as pessoas e as obras de cada uma.

    Uma consulta por tabela, não uma por regra: são poucas regras, mas o hábito
    de perguntar dentro do laço é o que deixa tela lenta sem ninguém notar."""
    if not _pronto():
        return []
    from .db import consultar

    onde = " WHERE ativa" if so_ativas else ""
    regras = {}
    ordem = []
    for linha in consultar(
            "SELECT id, nome, ativa, observacao, criado_por, alterado_em, "
            "       alterado_por "
            f"  FROM analisesps.folha_regra_rateio{onde} "
            " ORDER BY ativa DESC, lower(nome)"):
        regras[linha[0]] = {
            "id": linha[0], "nome": linha[1], "ativa": bool(linha[2]),
            "observacao": linha[3] or "", "criado_por": linha[4] or "",
            "alterado_em": linha[5], "alterado_por": linha[6] or "",
            "pessoas": [], "obras": []}
        ordem.append(linha[0])
    if not regras:
        return []

    for linha in consultar(
            "SELECT regra_id, cpf, nome FROM analisesps.folha_regra_pessoa "
            " ORDER BY nome, cpf"):
        if linha[0] in regras:
            regras[linha[0]]["pessoas"].append(
                {"cpf": linha[1], "cpf_bonito": cpf_bonito(linha[1]),
                 "nome": linha[2] or ""})
    for linha in consultar(
            "SELECT regra_id, obra, percentual, resto "
            "  FROM analisesps.folha_regra_obra ORDER BY ordem, id"):
        if linha[0] in regras:
            regras[linha[0]]["obras"].append(
                {"obra": linha[1], "percentual": linha[2],
                 "resto": bool(linha[3])})
    return [regras[i] for i in ordem]


def buscar(regra_id: int) -> dict | None:
    for regra in listar():
        if regra["id"] == int(regra_id):
            return regra
    return None


def regra_da_pessoa(cpf: str) -> dict | None:
    """A regra ATIVA desta pessoa, se houver. É o que a folha pergunta."""
    if not _pronto():
        return None
    from .db import consultar_um
    d = so_digitos(cpf)
    linha = consultar_um(
        "SELECT p.regra_id FROM analisesps.folha_regra_pessoa p "
        "  JOIN analisesps.folha_regra_rateio r ON r.id = p.regra_id "
        " WHERE p.cpf = ? AND r.ativa LIMIT 1", (d,))
    return buscar(linha[0]) if linha else None


def gravar(dados: dict, quem: str = "") -> int:
    """Cria ou altera uma regra, com as pessoas e as obras. Devolve o id.

    ⚠️ CONFERE ANTES DE ESCREVER, E ESCREVE TUDO NUMA TRANSAÇÃO. Uma regra
    gravada pela metade — com as obras novas e as pessoas antigas — ratearia
    dinheiro de gente para obra errada e ninguém saberia de onde veio."""
    if not _pronto():
        raise ErroDoRateio(FALTA_MIGRAR)
    from .db import conexao, consultar

    nome = str(dados.get("nome") or "").strip()
    if not nome:
        raise ErroDoRateio("Dê um nome à regra — é como você vai achá-la depois.")
    obras = conferir_obras(dados.get("obras"))

    pessoas = []
    vistos = set()
    for bruta in dados.get("pessoas") or []:
        cpf = so_digitos((bruta or {}).get("cpf"))
        if not cpf:
            continue
        if not cpf_valido(cpf):
            raise ErroDoRateio(
                f"O CPF {cpf_bonito(cpf) or cpf} não é válido — confira os "
                "dígitos. Sem o CPF certo a regra não encontra a pessoa, e ela "
                "volta a ser apropriada pelo ponto sem ninguém perceber.")
        if cpf in vistos:
            raise ErroDoRateio(
                f"O CPF {cpf_bonito(cpf)} está duas vezes nesta regra.")
        vistos.add(cpf)
        pessoas.append((cpf, str((bruta or {}).get("nome") or "").strip()[:120]))
    if not pessoas:
        raise ErroDoRateio("Escolha ao menos uma pessoa para a regra.")

    regra_id = dados.get("id")
    regra_id = int(regra_id) if str(regra_id or "").strip().isdigit() else None
    ativa = bool(dados.get("ativa", True))

    # ⚠️ A CONFERÊNCIA QUE O BANCO NÃO FAZ — ver o comentário da migração 027: o
    # Postgres não aceita subconsulta na condição de um índice parcial, e a
    # coluna `ativa` mora na outra tabela. Então é aqui, e tem teste.
    if ativa:
        marcas = ",".join(["?"] * len(pessoas))
        ja = consultar(
            "SELECT p.cpf, p.nome, r.nome FROM analisesps.folha_regra_pessoa p "
            "  JOIN analisesps.folha_regra_rateio r ON r.id = p.regra_id "
            f" WHERE r.ativa AND p.cpf IN ({marcas}) "
            + ("AND p.regra_id <> ?" if regra_id else ""),
            tuple([c for c, _n in pessoas] + ([regra_id] if regra_id else [])))
        if ja:
            cpf, nome_pessoa, nome_regra = ja[0]
            raise ErroDoRateio(
                f"{nome_pessoa or cpf_bonito(cpf)} já está na regra "
                f'"{nome_regra}". Uma pessoa em duas regras ativas não tem '
                "resposta certa: tire dela de uma das duas.")

    with conexao() as con:
        if regra_id:
            cur = con.execute(
                "UPDATE analisesps.folha_regra_rateio "
                "   SET nome = ?, ativa = ?, observacao = ?, "
                "       alterado_em = now(), alterado_por = ? "
                " WHERE id = ?",
                (nome, ativa, str(dados.get("observacao") or "").strip()[:500],
                 quem, regra_id))
            if not (cur.rowcount or 0):
                raise ErroDoRateio("Esta regra não existe mais.")
            con.execute("DELETE FROM analisesps.folha_regra_obra "
                        " WHERE regra_id = ?", (regra_id,))
            con.execute("DELETE FROM analisesps.folha_regra_pessoa "
                        " WHERE regra_id = ?", (regra_id,))
        else:
            cur = con.execute(
                "INSERT INTO analisesps.folha_regra_rateio "
                "  (nome, ativa, observacao, criado_por) VALUES (?, ?, ?, ?) "
                "RETURNING id",
                (nome, ativa, str(dados.get("observacao") or "").strip()[:500],
                 quem))
            regra_id = int(cur.fetchone()[0])

        for i, obra in enumerate(obras):
            con.execute(
                "INSERT INTO analisesps.folha_regra_obra "
                "  (regra_id, obra, percentual, resto, ordem) "
                "VALUES (?, ?, ?, ?, ?)",
                (regra_id, obra["obra"], obra["percentual"], obra["resto"], i))
        for cpf, nome_pessoa in pessoas:
            con.execute(
                "INSERT INTO analisesps.folha_regra_pessoa (regra_id, cpf, nome) "
                "VALUES (?, ?, ?)", (regra_id, cpf, nome_pessoa))
        con.commit()

    logger.info("Folha: %s gravou a regra de rateio %s (%s) — %s pessoa(s), "
                "%s obra(s).", quem or "?", regra_id, nome, len(pessoas),
                len(obras))
    return regra_id


def apagar(regra_id: int, quem: str = "") -> bool:
    """Apaga a regra. As pessoas e as obras dela vão junto (cascade).

    ⚠️ DESATIVAR É QUASE SEMPRE MELHOR QUE APAGAR: a regra desativada explica
    como a folha de março foi rateada. A tela diz isso; apagar existe para a
    regra criada errada, que nunca rateou nada."""
    if not _pronto():
        raise ErroDoRateio(FALTA_MIGRAR)
    from .db import conexao
    with conexao() as con:
        cur = con.execute("DELETE FROM analisesps.folha_regra_rateio WHERE id = ?",
                          (int(regra_id),))
        apagadas = cur.rowcount or 0
        con.commit()
    logger.warning("Folha: %s APAGOU a regra de rateio %s.", quem or "?",
                   regra_id)
    return bool(apagadas)
