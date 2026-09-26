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
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

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


# ---------------------------------------------------------------------------
# REPETIR A OBRA É O PESO DELA — e colar a tabela de uma vez
#
# ⚠️ POR QUE ISTO EXISTE, nas palavras do dono em 27/09/2026:
#
#   "Imagina, eu tenho 10 funcionários, eu quero ratear em 10 obras diferentes.
#    Aí imagina preencher 10 funcionários 10 vezes cada campozinho. 10 vezes 10
#    dá 100. Imagina preencher 100 campos. É absurdo. (…) Para eu não precisar
#    trabalhar com percentual, imagina que eu coloco código da obra 1, código da
#    obra 2 e código da obra 2 de novo. O que é que o sistema teria que
#    entender? 66% para uma e os 33% para outra."
#
# Ele está certo, e o formulário campo-por-campo era meu erro de desenho: eu
# resolvi o CASO de uma regra e ignorei o VOLUME de dez.
#
# O MODELO NO BANCO NÃO MUDA. A repetição é só a forma de ENTRAR: ela é contada
# e virada percentual aqui, e daí para baixo é o mesmo `conferir_obras` de
# sempre. Uma segunda forma de guardar rateio divergiria da primeira no dia em
# que alguém mexesse numa só.
# ---------------------------------------------------------------------------
def obras_por_repeticao(codigos) -> list:
    """`["A", "B", "B"]` -> A com 33,3333% e B com 66,6667%.

    O peso de cada obra é QUANTAS VEZES ela aparece. Fecha exatamente 100%: a
    sobra do arredondamento vai para a obra de maior peso, que é a mesma regra
    que `distribuir` usa para a sobra de centavo (decisão do dono em
    26/09/2026 — "pode botar na obra com mais dias").
    """
    contagem: dict = {}
    for bruto in codigos or []:
        nome = " ".join(str(bruto or "").split()).upper()
        if not nome:
            continue
        contagem[nome] = contagem.get(nome, 0) + 1
    if not contagem:
        raise ErroDoRateio("Escolha ao menos uma obra para a regra.")

    total = sum(contagem.values())
    # A de maior peso primeiro; empate, a que foi digitada primeiro. Assim a
    # sobra cai sempre no mesmo lugar, e duas colagens iguais dão o mesmo
    # resultado — rateio que muda sozinho entre duas rodadas é impossível de
    # conferir.
    ordem = sorted(contagem.items(), key=lambda p: (-p[1], list(contagem).index(p[0])))

    # Todas por baixo primeiro (ROUND_DOWN), e a sobra inteira vai para a
    # PRIMEIRA — que é a de maior peso. Fazer o contrário (a última fechar a
    # conta) daria a sobra para a obra de MENOR peso, e foi exatamente o defeito
    # que apareceu no primeiro teste desta função.
    saida = [{"obra": nome, "resto": False,
              "percentual": (CEM * Decimal(vezes) / Decimal(total)).quantize(
                  Decimal("0.0001"), rounding=ROUND_DOWN)}
             for nome, vezes in ordem]
    sobra = CEM - sum((o["percentual"] for o in saida), Decimal("0"))
    if sobra:
        saida[0]["percentual"] = (saida[0]["percentual"] + sobra).quantize(
            Decimal("0.0001"))
    return saida


def ler_tabela_de_rateio(texto: str) -> list:
    """Lê a tabela colada e devolve as regras prontas para gravar.

    UMA LINHA POR PESSOA:

        997.133.493-34 ; GERLANIO GOMES LIMA ; CREPEAREIAS, CREPEOLINDA, CREPEOLINDA

    O separador entre os três blocos é `;` ou tabulação (é o que sai ao copiar
    de uma planilha). As obras vêm separadas por vírgula, e **repetir a obra é o
    peso dela**.

    O NOME DA PESSOA É OPCIONAL: quem manda é o CPF, e o nome de verdade vem do
    cadastro. Aceita também `CPF ; obras` sem nome no meio — é o que acontece
    quando se cola duas colunas em vez de três.

    JUNTA QUEM TEM A MESMA DISTRIBUIÇÃO numa regra só. Dez pessoas com o mesmo
    rateio não são dez regras: são uma, com dez pessoas. É o que faz a tela
    continuar legível depois de colar trinta linhas.

    Devolve `[{"obras": [...], "pessoas": [...], "linhas": [n, ...]}, ...]`.
    Levanta `ErroDoRateio` com o número da linha quando algo não dá para ler —
    "não entendi a tabela" mandaria a pessoa procurar agulha em trinta linhas.
    """
    grupos: dict = {}
    ordem: list = []

    for numero, bruta in enumerate(str(texto or "").splitlines(), start=1):
        linha = bruta.strip()
        if not linha or linha.startswith("#"):
            continue
        # `;` e tabulação separam os blocos. A vírgula NÃO separa blocos: ela
        # separa obras, e também aparece dentro de nome ("SOUSA, ANA").
        partes = [p.strip() for p in re.split(r"[;\t]+", linha) if p.strip()]
        if len(partes) < 2:
            raise ErroDoRateio(
                f"Linha {numero}: falta o CPF ou a lista de obras. O formato é "
                "CPF ; nome ; obra, obra, obra — o nome pode faltar.")

        cpf = so_digitos(partes[0])
        if len(cpf) != 11:
            raise ErroDoRateio(
                f'Linha {numero}: "{partes[0]}" não é um CPF de 11 dígitos.')
        if not cpf_valido(cpf):
            raise ErroDoRateio(
                f"Linha {numero}: o CPF {cpf_bonito(cpf)} tem dígito "
                "verificador errado — confira se não trocou um número.")

        # O último bloco é sempre o das obras. O do meio, quando existe, é o
        # nome — e se houver mais de três blocos, o nome é o que está no meio.
        obras_cru = partes[-1]
        nome = " ".join(partes[1:-1]).strip() if len(partes) > 2 else ""
        codigos = [c.strip() for c in obras_cru.split(",") if c.strip()]
        if not codigos:
            raise ErroDoRateio(
                f"Linha {numero}: não achei nenhuma obra depois do nome.")

        try:
            obras = obras_por_repeticao(codigos)
        except ErroDoRateio as e:
            raise ErroDoRateio(f"Linha {numero}: {e}") from e

        # A chave do grupo é a distribuição, não a ordem de digitação: quem
        # escreveu "A, B, B" e quem escreveu "B, A, B" tem o MESMO rateio.
        chave = tuple(sorted((o["obra"], str(o["percentual"])) for o in obras))
        if chave not in grupos:
            grupos[chave] = {"obras": obras, "pessoas": [], "linhas": []}
            ordem.append(chave)
        grupos[chave]["pessoas"].append({"cpf": cpf, "nome": nome})
        grupos[chave]["linhas"].append(numero)

    if not ordem:
        raise ErroDoRateio(
            "Não achei nenhuma linha para ler. Uma linha por pessoa: "
            "CPF ; nome ; obra, obra, obra.")

    # A MESMA PESSOA EM DUAS LINHAS é erro, e tem de dizer quais: só uma regra
    # pode valer para alguém (é a trava que já existe na gravação), então duas
    # linhas do mesmo CPF fariam a segunda derrubar a primeira sem avisar.
    onde: dict = {}
    for chave in ordem:
        for pessoa, numero in zip(grupos[chave]["pessoas"], grupos[chave]["linhas"]):
            onde.setdefault(pessoa["cpf"], []).append(numero)
    repetidos = {c: ns for c, ns in onde.items() if len(ns) > 1}
    if repetidos:
        detalhe = "; ".join(
            f"{cpf_bonito(c)} nas linhas {', '.join(str(n) for n in ns)}"
            for c, ns in list(repetidos.items())[:4])
        raise ErroDoRateio(
            f"A mesma pessoa aparece em mais de uma linha: {detalhe}. "
            "Junte as obras dela numa linha só — repetir a obra é o peso.")

    return [grupos[c] for c in ordem]


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


def aplicar_tabela(texto: str, quem: str = "",
                   substituir: bool = True) -> dict:
    """Grava de uma vez todas as regras da tabela colada.

    ⚠️ `substituir=True` DESATIVA as regras que estão valendo antes de criar as
    novas — não apaga. É o que o dono descreveu do trabalho de verdade:

        "O rateio muda todo mês. Não existe um rateio fixo. Todo mês eu tenho
         que analisar as obras que estão em evidência."

    DESATIVAR E NÃO APAGAR é o que preserva a explicação da folha passada: a
    regra desativada continua dizendo como março foi rateado. Apagar deixaria
    um total de obra sem resposta.

    E é também o que faz a colagem FUNCIONAR: só uma regra ativa pode valer para
    uma pessoa (a trava em `gravar`), então sem desativar antes, colar a tabela
    do mês seguinte seria recusado pela do mês anterior.

    Tudo numa transação: uma colagem pela metade — com as regras velhas
    desativadas e as novas não criadas — deixaria gente sem rateio nenhum, e o
    valor cairia na obra do ponto sem ninguém pedir.
    """
    if not _pronto():
        raise ErroDoRateio(FALTA_MIGRAR)
    from .db import conexao

    # Lê e confere TUDO antes de escrever qualquer coisa. Se a linha 28 estiver
    # errada, nada foi mexido.
    grupos = ler_tabela_de_rateio(texto)

    desativadas = 0
    with conexao() as conn:
        if substituir:
            cur = conn.execute(
                "UPDATE analisesps.folha_regra_rateio SET ativa = false, "
                "       alterado_em = now(), alterado_por = ? "
                " WHERE ativa", (quem or "",))
            desativadas = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            cur.close()
        conn.commit()

    criadas = []
    for grupo in grupos:
        # O NOME SAI DAS OBRAS, porque é como ele vai reconhecer a regra na
        # tela. Nome pedido a cada colagem seria uma pergunta por grupo — e em
        # trinta linhas isso é trinta perguntas.
        nomes = [o["obra"] for o in grupo["obras"]]
        nome = " + ".join(nomes)
        if len(nome) > 70:
            nome = f"{nomes[0]} + {len(nomes) - 1} obra(s)"
        criadas.append(gravar({
            "nome": nome,
            "observacao": f"colado da tabela ({len(grupo['pessoas'])} pessoa(s))",
            "ativa": True,
            "pessoas": grupo["pessoas"],
            "obras": grupo["obras"],
        }, quem=quem))

    return {"regras": len(criadas), "ids": criadas,
            "pessoas": sum(len(g["pessoas"]) for g in grupos),
            "desativadas": desativadas}


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
