# -*- coding: utf-8 -*-
"""
Conversão entre o jeito brasileiro de escrever e o jeito do banco.

A planilha guarda tudo como texto, no formato que uma pessoa lê: "6.750,00" e
"31/12/2026". O banco precisa de número e de data de verdade, senão não soma
nem ordena — como texto, "10/01/2026" viria antes de "9/01/2026".

Aqui não entra `pandas`, de propósito. O painel provou que dá para fazer este
trabalho em Python puro sem custo perceptível, e `pandas` não é dependência
deste serviço — acrescentá-la para converter datas seria pagar caro por pouco.
"""
from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal, InvalidOperation

# Tudo que não seja dígito, vírgula, ponto ou sinal de menos.
_LIXO = re.compile(r"[^\d,.\-]")


def para_numero(texto) -> Decimal | None:
    """"6.750,00" -> 6750.00. Devolve None quando não há número nenhum.

    None e zero são coisas diferentes e precisam continuar sendo: uma SP sem
    valor preenchido não é uma SP de R$ 0,00. Somar as duas como zero
    esconderia o preenchimento faltando.

    Aceita as formas que aparecem de fato na planilha: com e sem separador de
    milhar, com "R$" na frente, e o negativo entre parênteses do estilo
    contábil — "(1.000,00)" é menos mil."""
    s = str(texto or "").strip()
    if not s:
        return None

    negativo = s.startswith("(") and s.endswith(")")
    s = _LIXO.sub("", s)
    if not s or s in ("-", ".", ","):
        return None

    if "," in s:
        # Vírgula presente: ela é o decimal, e o ponto é separador de milhar.
        s = s.replace(".", "").replace(",", ".")
    # Sem vírgula, o ponto pode ser milhar ("1.234") ou decimal ("1234.56").
    # Três dígitos depois do último ponto e nenhuma vírgula à vista: é milhar.
    elif re.fullmatch(r"-?\d{1,3}(\.\d{3})+", s):
        s = s.replace(".", "")

    try:
        valor = Decimal(s)
    except InvalidOperation:
        return None
    return -valor if negativo and valor > 0 else valor


# Fora desta faixa é erro de digitação na planilha, não data.
#
# Não é regra inventada: a base real tem SPs com ano 202, 204, 260 e 2925 —
# dedo escorregado em "2024", "2025". Aceitar essas datas seria pior do que
# recusá-las. Um vencimento no ano 202 encabeça qualquer lista ordenada por
# data, e um no ano 2925 nunca vence — os dois envenenariam silenciosamente
# todo filtro por período. Recusadas, elas aparecem como data em branco, que é
# visível e cobrável de quem preencheu.
ANO_MINIMO = 2000
ANO_MAXIMO = 2100


def para_data(texto) -> dt.date | None:
    """"31/12/2026" -> data. Devolve None quando não dá para ler.

    Aceita também o formato do banco (2026-12-31) e o ano com dois dígitos, que
    aparece em lançamentos antigos. Data impossível (31/02) devolve None em vez
    de estourar — dado ruim na planilha não pode derrubar a carga inteira."""
    s = str(texto or "").strip()
    if not s:
        return None

    # Fica só o primeiro pedaço. Descarta a hora quando vier junto
    # ("31/12/2026 14:30") e resolve as 1.664 SPs cuja data de autorização foi
    # gravada em duplicidade, separada por quebra de linha
    # ("24/07/2024\n24/07/2024") — provavelmente uma automação que escreveu
    # duas vezes. As duas metades são iguais; vale a primeira.
    s = s.split("T")[0].split()[0] if s.split() else ""
    if not s:
        return None

    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y", "%d-%m-%Y"):
        try:
            data = dt.datetime.strptime(s, formato).date()
        except ValueError:
            continue
        return data if ANO_MINIMO <= data.year <= ANO_MAXIMO else None
    return None


# ---------------------------------------------------------------------------
# O caminho de volta: do banco para a tela
# ---------------------------------------------------------------------------
def moeda(valor) -> str:
    """1234.5 -> "1.234,50". Sem o "R$", que a tela põe quando quiser."""
    if valor is None:
        return ""
    try:
        n = Decimal(str(valor))
    except (InvalidOperation, ValueError):
        return ""
    inteiro, _, centavos = f"{abs(n):.2f}".partition(".")
    grupos = []
    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]
    grupos.insert(0, inteiro)
    return ("-" if n < 0 else "") + ".".join(grupos) + "," + centavos


def _como_momento(valor):
    """Aceita data, data-e-hora ou o TEXTO de uma delas. None quando não dá.

    O texto existe porque nem tudo que a tela mostra vem de uma coluna de
    data: o carimbo da última sincronização, por exemplo, é guardado como
    texto em `analisesps.meta`. Sem isto, ele aparecia cru na tela —
    "2026-09-04T17:25:31.319885-03:00" no lugar de "04/09/2026 às 17:25"."""
    if isinstance(valor, dt.datetime) or isinstance(valor, dt.date):
        return valor
    texto = str(valor or "").strip()
    if not texto:
        return None
    try:
        return dt.datetime.fromisoformat(texto)
    except ValueError:
        pass
    # Já veio no formato brasileiro? Então não há o que converter.
    return None


def data_br(valor) -> str:
    """Data do banco -> "31/12/2026". Vazio vira vazio, nunca "None"."""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return ""
    momento = _como_momento(valor)
    if isinstance(momento, dt.datetime):
        # Data e hora viram o DIA EM BRASÍLIA. Sem converter, uma
        # sincronização das 22h daqui (1h do dia seguinte em UTC) apareceria
        # com a data de amanhã.
        from .horario import para_brasilia
        return (para_brasilia(momento) or momento).strftime("%d/%m/%Y")
    if isinstance(momento, dt.date):
        return momento.strftime("%d/%m/%Y")
    return str(valor)


def momento_br(valor) -> str:
    """Data E HORA, na hora de Brasília -> "31/12/2026 às 17:25".

    Para o que só faz sentido com a hora: "a base é de quando?". Dizer só o
    dia responderia "hoje", que é justamente o que já se sabia."""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return ""
    momento = _como_momento(valor)
    if momento is None:
        return str(valor)
    if not isinstance(momento, dt.datetime):
        return momento.strftime("%d/%m/%Y")
    from .horario import texto as _texto_br
    return _texto_br(momento)


# ---------------------------------------------------------------------------
# Link dentro de texto livre
#
# A descrição da SP vem digitada por gente, e com frequência traz o endereço
# de uma pasta, de um contrato ou de um comprovante. Como texto puro, era
# preciso selecionar na mão e colar no navegador.
#
# A ordem aqui importa e não é detalhe de estilo: PRIMEIRO escapa o texto
# inteiro, DEPOIS transforma em link o que sobrou. Ao contrário, uma descrição
# com HTML dentro entraria na página como HTML.
# ---------------------------------------------------------------------------
_ENDERECO = re.compile(
    r"""(?xi)
    \b(
        (?:https?://|www\.)         # com esquema, ou começando por www.
        [^\s<>"']+                  # o corpo do endereço
    )
    """)

# Pontuação que quase sempre é da frase, não do endereço: "veja em
# https://x.com/y." termina com o ponto final da frase.
_PONTUACAO_FINAL = ".,;:!?)]}>”’'\""


def com_links(texto) -> str:
    """Texto livre virando HTML seguro, com os endereços já clicáveis.

    Devolve HTML PRONTO — no template vai com `|safe`, senão as tags saem na
    tela como texto. Por isso o escape aqui não é opcional: a descrição vem da
    planilha, que qualquer um edita, e sem ele uma célula com `<script>`
    dentro rodaria na tela de quem abrisse a SP. Usa o `html.escape` da
    biblioteca padrão de propósito — nada de dependência nova para isto."""
    import html as _html

    if texto is None:
        return ""

    seguro = _html.escape(str(texto), quote=True)

    def trocar(achado):
        bruto = achado.group(1)
        rabo = ""
        while bruto and bruto[-1] in _PONTUACAO_FINAL:
            rabo = bruto[-1] + rabo
            bruto = bruto[:-1]
        if not bruto:
            return achado.group(0)
        destino = bruto if bruto.lower().startswith("http") else "https://" + bruto
        return (f'<a href="{destino}" target="_blank" rel="noopener noreferrer">'
                f'{bruto}</a>{rabo}')

    return _ENDERECO.sub(trocar, seguro)
