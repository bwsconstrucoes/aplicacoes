# -*- coding: utf-8 -*-
"""
OS ACRÉSCIMOS DA DIÁRIA — e por que eles saíram do código.

Ele, em 10/11/2026, confirmando os números e logo em seguida corrigindo o
lugar deles:

> *"Esses números ainda valem atualmente, mas isso é algo que deva ficar
> dentro de uma configuração, o acréscimo da diária."*

Os valores estavam **escritos no código** do Análise de SPs
(`folha_diaristas.py`: 20 no feriado, 10 no sábado, 20 no domingo, vigia
fora). Mexer neles exigia um programador e um deploy — para um número que é
decisão de negócio e muda com acordo coletivo, aumento ou simples escolha.

⚑ **POR QUE A CONFIGURAÇÃO MORA NO ERP, e não no Análise de SPs**, que é
quem calcula hoje: porque o cadastro mora aqui, a folha está vindo para cá
(*"em breve vamos migrar todas essas telas de pagamento de colaboradores
para o ERP tb"*), e parâmetro guardado no sistema que vai ser desligado
mudaria de casa uma segunda vez. Os dois módulos rodam no MESMO processo e no
MESMO banco, então ler daqui não custa uma volta de rede.

⚑ **OS PADRÕES SÃO EXATAMENTE OS NÚMEROS DE HOJE.** Isso não é detalhe: quem
nunca abrir a tela de configuração continua com a conta que a empresa já faz.
Uma configuração nova que muda o resultado no primeiro dia é uma armadilha —
o número muda, ninguém mexeu em nada, e a desconfiança cai no sistema inteiro.

⚑ **O VIGIA É LISTA, NÃO BOOLEANO.** Hoje a regra exclui uma função só, mas
"as funções que não recebem diária" é a pergunta de verdade — e um booleano
chamado `exclui_vigia` seria o tipo de nome que ninguém sabe editar quando o
caso virar dois. Comparação por nome, sem acento e sem caixa: "VIGIA NOTURNO"
é OUTRA função, e recebe diária (é a regra em vigor na planilha).
"""
from __future__ import annotations

import logging
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Parametro, Usuario

logger = logging.getLogger(__name__)

CHAVES = {
    "feriado": "diaria_acrescimo_feriado",
    "sabado": "diaria_acrescimo_sabado",
    "domingo": "diaria_acrescimo_domingo",
    "sem_diaria": "diaria_funcoes_sem_diaria",
}
# Os números em vigor em 10/11/2026, confirmados por ele.
PADRAO = {"feriado": Decimal("20.00"), "sabado": Decimal("10.00"),
          "domingo": Decimal("20.00")}
FUNCOES_SEM_DIARIA_PADRAO = ("VIGIA",)
# Teto de sanidade. Acréscimo não é a diária: um valor acima disto é quase
# certamente o valor da DIÁRIA digitado no campo errado, e pagá-lo dobraria o
# dia de todo mundo que trabalhou num domingo.
TETO = Decimal("500.00")


def _sem_acento(texto: Any) -> str:
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    return " ".join("".join(c for c in cru if not unicodedata.combining(c))
                    .lower().split())


def _ler(s: Session, chave: str) -> str:
    linha = s.get(Parametro, CHAVES[chave])
    return (linha.valor if linha is not None else "") or ""


def _gravar(s: Session, chave: str, valor: str,
            usuario: Optional[Usuario] = None) -> None:
    nome = CHAVES[chave]
    linha = s.get(Parametro, nome)
    if linha is None:
        s.add(Parametro(chave=nome, valor=valor))
        return
    linha.valor = valor
    if hasattr(linha, "alterado_por"):
        linha.alterado_por = usuario.id if usuario else None


def _dinheiro(bruto: Any, rotulo: str) -> Decimal:
    texto = str(bruto or "").strip().replace("R$", "").strip()
    if not texto:
        return Decimal("0.00")
    # Aceita "20,00" e "20.00": quem digita é gente, e o teclado do celular
    # não concorda com o do computador sobre qual é a vírgula decimal.
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        valor = Decimal(texto).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        raise ErroValidacao(f"Valor inválido no acréscimo de {rotulo}: {bruto!r}.")
    if valor < 0:
        raise ErroValidacao(f"O acréscimo de {rotulo} não pode ser negativo.")
    if valor > TETO:
        raise ErroValidacao(
            f"O acréscimo de {rotulo} ficou em R$ {valor} — acima de "
            f"R$ {TETO}. Acréscimo não é o valor da diária; confira se o "
            f"número não foi digitado no campo errado.")
    return valor


def configuracao(s: Session) -> dict[str, Any]:
    """Os acréscimos em vigor. Nunca levanta: sem nada gravado, vale o padrão."""
    saida: dict[str, Any] = {}
    for chave, padrao in PADRAO.items():
        bruto = _ler(s, chave)
        try:
            saida[chave] = float(Decimal(bruto)) if bruto else float(padrao)
        except (InvalidOperation, ValueError):
            logger.warning("ERP/diárias: valor ilegível em %s (%r) — vale o "
                           "padrão", chave, bruto)
            saida[chave] = float(padrao)
    bruto = _ler(s, "sem_diaria")
    funcoes = [f.strip() for f in bruto.split(",") if f.strip()] if bruto \
        else list(FUNCOES_SEM_DIARIA_PADRAO)
    saida["sem_diaria"] = funcoes
    saida["no_padrao"] = (
        all(Decimal(str(saida[c])) == PADRAO[c] for c in PADRAO)
        and [f.upper() for f in funcoes] == list(FUNCOES_SEM_DIARIA_PADRAO))
    return saida


def configurar(s: Session, dados: dict[str, Any],
               usuario: Optional[Usuario] = None) -> dict[str, Any]:
    antes = configuracao(s)
    rotulos = {"feriado": "feriado", "sabado": "sábado", "domingo": "domingo"}
    for chave, rotulo in rotulos.items():
        if chave in dados:
            _gravar(s, chave, str(_dinheiro(dados[chave], rotulo)), usuario)
    if "sem_diaria" in dados:
        bruto = dados["sem_diaria"]
        lista = bruto if isinstance(bruto, list) else str(bruto or "").split(",")
        limpa = [f.strip().upper() for f in lista if str(f).strip()]
        _gravar(s, "sem_diaria", ", ".join(limpa), usuario)
    s.flush()
    depois = configuracao(s)
    # MUDANÇA DE PARÂMETRO DE PAGAMENTO DEIXA RASTRO. Seis meses depois, "por
    # que o domingo pagou 25?" tem de ter resposta com nome e data.
    registrar_evento(s, "configuracao", 0, "DIARIA_ACRESCIMOS_ALTERADOS",
                     {"antes": antes, "depois": depois},
                     usuario.id if usuario else None)
    return depois


def acrescimo_do_dia(s: Session, *, feriado: bool, dia_da_semana: int) -> Decimal:
    """Quanto se acrescenta à diária NESTE dia.

    `dia_da_semana` segue o Python: 0 = segunda … 5 = sábado, 6 = domingo.

    ⚑ FERIADO VENCE O DIA DA SEMANA, e não soma com ele: feriado que cai no
    sábado paga o acréscimo de feriado, não os dois. Somar seria pagar duas
    vezes pela mesma condição.
    """
    c = configuracao(s)
    if feriado:
        return Decimal(str(c["feriado"]))
    if dia_da_semana == 5:
        return Decimal(str(c["sabado"]))
    if dia_da_semana == 6:
        return Decimal(str(c["domingo"]))
    return Decimal("0.00")


def recebe_diaria(s: Session, funcao: Any) -> bool:
    """Esta função recebe diária? Compara sem acento e sem caixa.

    Igualdade, nunca "contém": "VIGIA NOTURNO" é outra função e **recebe**
    diária — é a regra que a planilha pratica, e trocá-la por "começa com"
    tiraria o pagamento de um turno inteiro sem ninguém pedir.
    """
    nome = _sem_acento(funcao)
    return nome not in {_sem_acento(f) for f in configuracao(s)["sem_diaria"]}
