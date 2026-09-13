# ============================================================================
# ERP — core/comum/formato.py
# Número e dinheiro escritos como se lê em português.
#
# POR QUE EXISTE UM MÓDULO SÓ PARA ISSO. Estas duas funções nasceram dentro do
# relatório de cotação, e logo o alerta da locação precisou das mesmas: "R$
# 576.00 de aluguel depois do combinado" é o computador falando, não o ERP.
# Copiar seria pior — duas cópias divergem, e aí o mesmo valor sai de dois
# jeitos em duas telas. Fica aqui, e quem precisar importa.
#
# Quem escreve para TELA em JavaScript tem o par delas em erp_base.html
# (`moeda` e `numero`). Estas aqui são para o que o servidor monta: relatório,
# PDF, mensagem de alerta, texto de e-mail.
# ============================================================================
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


def _dinheiro_br(valor: Any) -> str:
    """4155.68 → "4.155,68". O relatório é lido por gente e vai para o papel:
    ponto decimal e quatro casas ("35.9000") não se lê em português."""
    if valor in (None, ""):
        return ""
    try:
        d = Decimal(str(valor))
    except (InvalidOperation, ValueError):
        return str(valor)
    # duas casas por padrão; mantém mais só quando o preço realmente as tem
    # (item barato cotado a 0,1250 existe e arredondar mentiria)
    texto = f"{d:.4f}".rstrip("0")
    casas = max(2, len(texto.split(".")[1]) if "." in texto else 0)
    d = d.quantize(Decimal("1." + "0" * casas), rounding=ROUND_HALF_UP)
    inteiro, _, decimais = f"{d:.{casas}f}".partition(".")
    negativo = inteiro.startswith("-")
    inteiro = inteiro.lstrip("-")
    grupos = []
    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]
    grupos.insert(0, inteiro)
    return ("-" if negativo else "") + ".".join(grupos) + "," + decimais


def _quantidade_br(valor: Any) -> str:
    """"14.000" no banco é catorze, não catorze mil — a casa decimal só
    aparece quando existe de verdade."""
    if valor in (None, ""):
        return ""
    try:
        d = Decimal(str(valor))
    except (InvalidOperation, ValueError):
        return str(valor)
    if d == d.to_integral_value():
        return str(int(d))
    return f"{d.normalize():f}".replace(".", ",")


def documento_por_extenso(valor: Any) -> str:
    """71000001000184 → 71.000.001/0001-84; 12345678901 → 123.456.789-01.

    Morava dentro do envio de cotação, onde nasceu. Subiu para cá quando a
    leitura de documento anexado precisou da mesma coisa: catorze dígitos
    seguidos ninguém confere de olho, e duas cópias da mesma função divergem
    no dia em que alguém corrige só uma.
    """
    numeros = "".join(c for c in str(valor or "") if c.isdigit())
    if len(numeros) == 14:
        return (f"{numeros[:2]}.{numeros[2:5]}.{numeros[5:8]}/"
                f"{numeros[8:12]}-{numeros[12:]}")
    if len(numeros) == 11:
        return f"{numeros[:3]}.{numeros[3:6]}.{numeros[6:9]}-{numeros[9:]}"
    return str(valor or "").strip()


def data_br(valor: Any) -> str:
    """2026-09-02 → 02/09/2026. E 2026-09 → 09/2026, que é como se escreve
    competência. O que não for data reconhecível volta como veio — inventar
    uma data a partir de texto estranho seria pior que mostrar o texto."""
    bruto = str(valor or "").strip()
    if not bruto:
        return ""
    partes = bruto.split("T")[0].split("-")
    if len(partes) == 3 and all(x.isdigit() for x in partes):
        a, m, d = partes
        if len(a) == 4:
            return f"{d.zfill(2)}/{m.zfill(2)}/{a}"
    if len(partes) == 2 and all(x.isdigit() for x in partes) and len(partes[0]) == 4:
        return f"{partes[1].zfill(2)}/{partes[0]}"
    return bruto


# ---------------------------------------------------------------------------
# O recado de falha inesperada
# ---------------------------------------------------------------------------
# Até 11/09/2026 toda falha inesperada do ERP devolvia o texto CRU da exceção
# para a tela. O dono viu isso acontecer: perguntou uma coisa ao assistente e
# recebeu de volta a lista de colunas de uma tabela do banco. Três problemas
# no mesmo lugar:
#
#   1. quem lê não é programador — "UndefinedColumn: column t.xpto does not
#      exist" não diz o que fazer;
#   2. o texto cru conta como o sistema é feito por dentro (nome de tabela, de
#      coluna, caminho de arquivo), que é informação para quem quer atacar;
#   3. some a informação útil: qual foi a falha, para poder ser procurada
#      depois no registro do servidor.
#
# Este recado resolve os três: fala português, não conta nada de dentro, e
# carrega um código curto que casa com a linha do log (o log continua com a
# exceção inteira — quem precisa do detalhe é quem conserta, não quem usa).
# ---------------------------------------------------------------------------
# A ÚNICA falha que continua sendo explicada em detalhe, porque quem lê PODE
# resolver sozinho: o banco atrasado. O código do ERP sobe para o Render antes
# de o botão "Aplicar atualizações do banco" ser apertado — e nessa janela o
# Postgres responde "coluna não existe". Foi o que derrubou o ERP em
# 02/09/2026. Esconder isso atrás de "falha do sistema" tiraria da pessoa a
# única informação que resolve o problema em dez segundos.
_BANCO_ATRASADO = ("does not exist", "não existe", "undefinedcolumn",
                   "undefinedtable")

RECADO_BANCO_ATRASADO = (
    "O banco de dados está desatualizado em relação ao sistema: falta aplicar "
    "as atualizações mais recentes. Vá em Configurações e aperte "
    "\"Aplicar atualizações do banco\" — depois disso a tela volta ao normal. "
    "Se você não for ADMIN, peça isso a quem for.")


def recado_de_falha(erro: BaseException, acao: str = "") -> str:
    """Mensagem de falha inesperada para a TELA, com código para o registro.

    Nunca devolve o texto da exceção: nome de tabela, de coluna e caminho de
    arquivo são a intimidade do sistema, e quem lê a tela não é programador. O
    registro do servidor continua com a exceção inteira — quem precisa do
    detalhe é quem conserta.

    A exceção à regra é o banco atrasado, logo acima.
    """
    import hashlib

    texto = f"{type(erro).__name__}: {erro}".lower()
    if any(p in texto for p in _BANCO_ATRASADO):
        return RECADO_BANCO_ATRASADO

    marca = f"{type(erro).__name__}:{erro}"
    codigo = hashlib.sha256(marca.encode("utf-8")).hexdigest()[:6].upper()
    o_que = f" ao {acao}" if acao else ""
    return (f"Não consegui concluir{o_que}. Isso é falha do sistema, não do que "
            f"você preencheu — nada foi gravado pela metade. Se acontecer de "
            f"novo, me mande o código {codigo}.")
