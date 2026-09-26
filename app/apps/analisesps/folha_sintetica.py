# -*- coding: utf-8 -*-
"""
A leitura da Folha Sintética que a contabilidade manda (Fortes Pessoal).

É a entrada do processo de folha de pagamento: o arquivo que a contabilidade
externa envia por quinzena, com quanto cada pessoa tem a receber.

⚠️ NÃO É UMA TABELA, É UM RELATÓRIO IMPRESSO. O arquivo é um `.xls` antigo
(BIFF, o formato do Excel 97 — `openpyxl` não abre, precisa de `xlrd`), e o
conteúdo é a página de um relatório: cabeçalho a cada página, grupos por filial,
subtotal por grupo e um total geral no fim. Ler isso como planilha perde linha.

O que o arquivo real (08/2026) traz:

    Folha Sintética - Adiantamento de Folha        : 1      ← número da PÁGINA
    Empresa: BWS CONSTRUCOES LTDA - CNPJ: ...      Fortes Pessoal 8.26.0
    Mês/Ano: 08/2026
    Código | Empregado |  |  | Líquido
    001 - CONSTRUTORA                                       ← FILIAL
    000013 | GERLANIO GOMES LIMA |  |  | 1198.84
    ...
    Total: 001 - CONSTRUTORA ...                   5990.68
    090 - OBRA ESTADIOITA CONST ESTADIO ITAITINGA
    ...
    Total: Geral (507 Empregado(s))                445199.96
    Fim

⚠️ O QUE VALE É O CORPO, NÃO O RODAPÉ — decisão do dono em 26/09/2026:

    *"Vamos nos ocupar com as pessoas que aparecem no relatório. E o valor de
    cada uma. Então, se o somatório total final seja maior, ignore isso."*

E a diferença é real: no arquivo de 08/2026 as linhas somam 430.129,75 para 491
pessoas, e o rodapé declara 445.199,96 para 507. As linhas e os subtotais por
filial batem no centavo entre si; quem não bate é o rodapé. A hipótese é que o
rodapé conte a folha inteira e o corpo liste só quem tem adiantamento — mas não
importa: o que se paga é o corpo.

⚠️ A CONFERÊNCIA QUE VALE É OUTRA, e ela fica: **soma das linhas == soma dos
subtotais por filial**. Essas duas são calculadas pelo mesmo relatório sobre as
mesmas linhas; se divergirem, o arquivo chegou truncado ou a leitura errou — e
aí nada pode ser pago com ele.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

logger = logging.getLogger("analisesps.folha")

# O código do empregado no Fortes: seis dígitos, com zeros à esquerda.
#
# ⚠️ OS ZEROS À ESQUERDA FAZEM PARTE DO CÓDIGO e são o motivo de ele viajar como
# TEXTO daqui até o fim. Guardar 13 em vez de 000013 obriga todo cruzamento a
# lembrar de completar com zeros — e o dia em que alguém esquecer, a pessoa não é
# encontrada no cadastro e o pagamento dela some sem erro nenhum.
PADRAO_CODIGO = re.compile(r"^\d{6}$")

# "001 - CONSTRUTORA" / "090 - OBRA ESTADIOITA CONST ESTADIO ITAITINGA"
PADRAO_FILIAL = re.compile(r"^(\d{1,4})\s*-\s*(.+)$")

# "Mês/Ano: 08/2026"
PADRAO_COMPETENCIA = re.compile(r"M[êe]s/Ano:\s*(\d{1,2})\s*/\s*(\d{4})")

# "Total: Geral (507 Empregado(s))"
PADRAO_TOTAL_GERAL = re.compile(r"Total:\s*Geral\s*\((\d+)\s*Empregado", re.I)

CNPJ = re.compile(r"CNPJ:\s*([\d./-]+)")


# ---------------------------------------------------------------------------
# QUINZENA OU FIM DE MÊS — o próprio arquivo diz, e o dono confirma
# ---------------------------------------------------------------------------
# Pergunta dele em 26/09/2026: *"como é que a gente vai saber se a gente está
# tratando de quinzena, se está tratando de fim de mês, e se a gente informa, se
# seleciona para informar de qual arquivo é aquele dali que a gente está
# tratando."*
#
# O TÍTULO DO RELATÓRIO RESPONDE, quando ele diz "Adiantamento": o arquivo real de
# 08/2026 se chama "Folha Sintética - Adiantamento de Folha", e adiantamento é a
# quinzena (dias 1 a 15).
#
# ⚠️ MAS A SUGESTÃO NÃO DECIDE SOZINHA. Não conheço o título do relatório de fim
# de mês — nunca vi um. Então: quando o título diz "adiantamento", a tela vem com
# QUINZENA já escolhido; quando não diz, ela vem SEM escolha e pergunta. Chutar
# "fim de mês" só porque não é adiantamento leria o pedaço errado do ponto (16 ao
# fim em vez de 1 a 15), e o erro sairia como valor plausível na obra errada.
QUINZENA = "quinzena"
FIM_DE_MES = "fim_de_mes"

# O que no título do relatório denuncia a quinzena.
PALAVRAS_DE_ADIANTAMENTO = ("adiantamento", "adiant.", "quinzena")
# E o que denuncia o fechamento.
#
# ⚠️ OS DOIS TÍTULOS DE VERDADE, confirmados pelo dono em 26/09/2026:
#
#     quinzena   → "Folha Sintética - Adiantamento de Folha"
#     fim de mês → "Folha Sintética - Folha de Pagamento"
#
# O de fim de mês é o mais genérico dos dois ("Folha de Pagamento"), e é por isso
# que a ordem da conferência importa: o adiantamento é testado PRIMEIRO. Um
# relatório que fosse "Adiantamento - Folha de Pagamento" tem de cair em quinzena.
PALAVRAS_DE_FECHAMENTO = ("folha de pagamento", "fim de m", "fechamento",
                          "mensal")


def tipo_sugerido(titulo: str) -> str:
    """"quinzena", "fim_de_mes" ou "" quando o título não deixa claro.

    Devolver "" é resposta legítima, e é a mais importante das três: é ela que
    faz a tela PERGUNTAR em vez de adivinhar."""
    t = " ".join(str(titulo or "").lower().split())
    if any(p in t for p in PALAVRAS_DE_ADIANTAMENTO):
        return QUINZENA
    if any(p in t for p in PALAVRAS_DE_FECHAMENTO):
        return FIM_DE_MES
    return ""


class ErroDaFolha(RuntimeError):
    """Arquivo ilegível ou que não é uma Folha Sintética. A frase vai para a tela."""


@dataclass
class LinhaDaFolha:
    """Uma pessoa na folha, como a contabilidade mandou."""
    id_fortes: str          # "000013" — texto, com os zeros
    nome: str
    valor: Decimal
    filial_codigo: str      # "001"
    filial_nome: str


@dataclass
class FolhaLida:
    """O que o arquivo diz, antes de qualquer decisão sobre pagar."""
    titulo: str
    empresa: str
    cnpj: str
    mes: int | None
    ano: int | None
    linhas: list = field(default_factory=list)
    # {codigo: {"nome": ..., "total": Decimal}} — o subtotal que o RELATÓRIO
    # declara para cada filial, não a soma que fizemos.
    filiais: dict = field(default_factory=dict)
    total_declarado: Decimal | None = None      # o rodapé, só para informação
    pessoas_declaradas: int | None = None       # idem
    avisos: list = field(default_factory=list)

    @property
    def tipo_sugerido(self) -> str:
        """O que o TÍTULO do relatório sugere: "quinzena", "fim_de_mes" ou "".

        Vazio quer dizer "não sei" — e a tela tem de perguntar. Ver
        `tipo_sugerido` no alto deste arquivo."""
        return tipo_sugerido(self.titulo)

    @property
    def competencia(self) -> str:
        """"08/2026", para a tela e para o nome dos arquivos gerados."""
        if not self.mes or not self.ano:
            return ""
        return f"{self.mes:02d}/{self.ano}"

    @property
    def total(self) -> Decimal:
        """A soma das linhas — é este o valor que se paga."""
        return sum((l.valor for l in self.linhas), Decimal("0"))

    @property
    def total_das_filiais(self) -> Decimal:
        return sum((f["total"] for f in self.filiais.values()), Decimal("0"))

    @property
    def fecha(self) -> bool:
        """As linhas somam o mesmo que os subtotais por filial?"""
        return self.total == self.total_das_filiais


def _numero(valor) -> Decimal | None:
    """O `Líquido` de uma linha, em qualquer dos formatos que o Fortes usa.

    ⚠️ O MESMO RELATÓRIO MANDA VALOR EM DOIS FORMATOS, e foi o dono quem mostrou,
    em 26/09/2026, ao colar a folha de fim de mês:

        000013  GERLANIO GOMES LIMA      1.074,64     ← texto, ponto de milhar
        000387  LUELIA MADIDA GOMES ...  1362,56      ← texto, sem milhar

    enquanto o arquivo de adiantamento trazia número de verdade na célula
    (`1198.84`). Ou seja: não dá para assumir formato nenhum.

    ⚠️ E AQUI ESTAVA UMA ARMADILHA DE CEM VEZES. A leitura antiga apagava TODO
    ponto e trocava vírgula por ponto. Isso acerta "1.074,64" e "1362,56", mas um
    valor que venha como texto com ponto decimal — "1198.84", que é o que sai de
    uma reexportação — viraria **119884**.

    E o pior: a conferência de fechamento NÃO pegaria. O total da filial vem no
    mesmo formato e inflaria igual, então as duas somas continuariam batendo, e a
    tela diria "fecha" com todo mundo recebendo cem vezes mais.

    A regra agora: **vírgula manda** (é decimal); sem vírgula, um ponto seguido de
    UM ou DOIS dígitos no fim também é decimal; qualquer outro ponto é separador de
    milhar.
    """
    if valor is None or valor == "":
        return None
    if isinstance(valor, (int, float)):
        return Decimal(str(valor)).quantize(Decimal("0.01"))

    texto = " ".join(str(valor).split()).replace("R$", "").strip()
    if not texto:
        return None
    negativo = texto.startswith("-") or (texto.startswith("(")
                                         and texto.endswith(")"))
    texto = texto.strip("()-").strip()

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif re.search(r"\.\d{1,2}$", texto):
        pass                      # já é ponto decimal: "1198.84"
    else:
        texto = texto.replace(".", "")
    try:
        numero = Decimal(texto).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
    return -numero if negativo else numero


def _texto(valor) -> str:
    if valor is None:
        return ""
    if isinstance(valor, float) and valor.is_integer():
        # Célula numérica onde o relatório escreveu um código: 13.0 -> "13".
        return str(int(valor))
    return str(valor).strip()


def interpretar(linhas_brutas) -> FolhaLida:
    """Transforma as linhas do relatório na folha. NÃO abre arquivo nenhum.

    Separado de `ler` de propósito: é aqui que vive toda a regra, e assim ela
    pode ser testada sem depender de um `.xls` de verdade — que é chato de
    montar e esconde o que está sob teste.

    ⚠️ O CABEÇALHO SE REPETE A CADA PÁGINA, e o da FILIAL se repete quando ela
    atravessa a quebra. Tratar repetição como linha nova criaria filial
    duplicada; tratar como fim de grupo perderia as pessoas da segunda metade.
    A saída é acumular por CÓDIGO de filial, e o subtotal (`Total: ...`) manda o
    grupo para o fim.
    """
    lida = FolhaLida(titulo="", empresa="", cnpj="", mes=None, ano=None)
    filial_atual = ("", "")
    vistos: dict = {}

    for bruta in linhas_brutas:
        celulas = [_texto(c) for c in (bruta or [])]
        if not any(celulas):
            continue
        primeira = celulas[0]
        # O `Líquido` é a ÚLTIMA célula com número da linha. O relatório tem
        # colunas vazias no meio (a 3ª e a 4ª), e a posição do valor mudou entre
        # versões do Fortes — pegar a última evita depender disso.
        #
        # ⚠️ A PRIMEIRA CÉLULA FICA DE FORA DA PROCURA, e o teste pegou isto: o
        # código do empregado é "000999", que como número é 999 — então a linha
        # de uma pessoa SEM valor era lida como uma pessoa de R$ 999,00. Pagar
        # 999 reais para quem não tinha valor nenhum é o pior jeito de errar
        # aqui, porque o número parece plausível.
        valor = None
        for celula in reversed(list(bruta or [])[1:]):
            valor = _numero(celula)
            if valor is not None:
                break

        if primeira.lower().startswith("folha sintética") or \
                primeira.lower().startswith("folha sintetica"):
            if not lida.titulo:
                lida.titulo = primeira
            continue
        if primeira.lower().startswith("empresa:"):
            if not lida.empresa:
                junto = " ".join(c for c in celulas[1:] if c)
                lida.empresa = junto
                achado = CNPJ.search(junto)
                lida.cnpj = achado.group(1) if achado else ""
            continue
        if PADRAO_COMPETENCIA.search(primeira):
            achado = PADRAO_COMPETENCIA.search(primeira)
            lida.mes, lida.ano = int(achado.group(1)), int(achado.group(2))
            continue
        if primeira.lower() in ("código", "codigo"):
            continue          # o cabeçalho das colunas, repetido por página
        # O rodapé de página do Fortes: "Continua..." em toda página menos a
        # última, e "Fim" na última. Vêm na ÚLTIMA célula, com a primeira vazia.
        # Sem reconhecer os dois, o arquivo real de 08/2026 gerava onze avisos
        # de "linha que não reconheci" — e aviso que aparece sempre é aviso que
        # ninguém lê, o que estraga os avisos de verdade.
        ultima = celulas[-1].strip().lower() if celulas else ""
        if primeira.lower() == "fim" or ultima in ("fim", "continua...",
                                                   "continua…", "continua"):
            continue

        geral = PADRAO_TOTAL_GERAL.search(primeira)
        if geral:
            lida.pessoas_declaradas = int(geral.group(1))
            lida.total_declarado = valor
            continue

        if primeira.startswith("Total:"):
            # Subtotal da filial. O nome dentro do "Total:" vem CORTADO pelo
            # relatório, então o grupo é achado pelo CÓDIGO, não pelo nome.
            achado = PADRAO_FILIAL.match(primeira[len("Total:"):].strip())
            codigo = achado.group(1) if achado else filial_atual[0]
            if codigo and codigo in vistos:
                vistos[codigo]["total"] = valor or Decimal("0")
            elif codigo:
                vistos[codigo] = {"nome": filial_atual[1],
                                  "total": valor or Decimal("0")}
            continue

        achado = PADRAO_FILIAL.match(primeira)
        if achado and not PADRAO_CODIGO.match(primeira):
            codigo, nome = achado.group(1), achado.group(2).strip()
            filial_atual = (codigo, nome)
            # Nome mais COMPRIDO ganha: a repetição depois da quebra de página às
            # vezes vem cortada, e ficar com a versão cortada faria o relatório
            # mostrar "090 - OBRA ESTADIOI".
            anterior = vistos.get(codigo)
            if anterior is None:
                vistos[codigo] = {"nome": nome, "total": Decimal("0")}
            elif len(nome) > len(anterior["nome"]):
                anterior["nome"] = nome
            continue

        if PADRAO_CODIGO.match(primeira):
            nome = celulas[1] if len(celulas) > 1 else ""
            if valor is None:
                lida.avisos.append(
                    f"a linha do código {primeira} ({nome}) veio sem valor.")
                continue
            lida.linhas.append(LinhaDaFolha(
                id_fortes=primeira, nome=nome, valor=valor,
                filial_codigo=filial_atual[0], filial_nome=filial_atual[1]))
            continue

        # Qualquer outra coisa: não inventa. Guarda o aviso para a tela mostrar.
        lida.avisos.append(f"linha que não reconheci: {primeira[:60]}")

    lida.filiais = vistos
    return lida


def ler(conteudo: bytes) -> FolhaLida:
    """Abre o `.xls` da contabilidade e devolve a folha. Levanta `ErroDaFolha`.

    ⚠️ `xlrd` E NÃO `openpyxl`: o Fortes gera o formato BIFF do Excel 97, e o
    `openpyxl` só lê `.xlsx`. Se um dia a contabilidade passar a mandar `.xlsx`,
    é aqui que se escolhe o leitor — o `interpretar` não muda.
    """
    if not conteudo:
        raise ErroDaFolha("O arquivo chegou vazio.")
    if len(conteudo) > 20 * 1024 * 1024:
        raise ErroDaFolha(
            "O arquivo tem mais de 20 MB. Folha sintética desse tamanho "
            "costuma ser outra coisa — confira se é o relatório certo.")
    try:
        import xlrd
    except ImportError as e:  # pragma: no cover — dependência declarada
        raise ErroDaFolha(
            "Falta a biblioteca que lê o formato antigo do Excel (xlrd). "
            "Avise quem cuida do sistema.") from e
    try:
        livro = xlrd.open_workbook(file_contents=conteudo)
        aba = livro.sheet_by_index(0)
        linhas = [[aba.cell_value(r, c) for c in range(aba.ncols)]
                  for r in range(aba.nrows)]
    except ErroDaFolha:
        raise
    except Exception as e:  # noqa: BLE001 — a tela precisa da frase
        raise ErroDaFolha(
            f"Não consegui abrir o arquivo como Excel antigo (.xls): {e}") from e

    lida = interpretar(linhas)
    if not lida.linhas:
        raise ErroDaFolha(
            "Não achei nenhuma linha de empregado neste arquivo. Confira se é a "
            "Folha Sintética do Fortes.")
    if lida.mes is None or lida.ano is None:
        raise ErroDaFolha(
            'Não achei o "Mês/Ano" no arquivo. Sem a competência não dá para '
            "saber a qual folha ele pertence.")
    return lida
