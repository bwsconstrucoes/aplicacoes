# -*- coding: utf-8 -*-
"""
O VÍNCULO DE CADA DIA: CTPS (vem da contabilidade) ou DIÁRIA.

⚠️ ISTO É TRADUÇÃO FIEL DE UMA FÓRMULA QUE JÁ RODA EM PRODUÇÃO — o dono mandou ela
em 26/09/2026, da coluna AH da aba `Mobponto` da planilha "Folha de Pagamento -
Fortes". Ele mandou porque eu havia dito que não tinha lido as fórmulas (o acesso
que eu tenho às planilhas devolve valores, não fórmulas), e porque eu estava
classificando errado: eu olhava só o `Tipo de Cadastro` da pessoa.

⚠️ E A DESCOBERTA QUE MUDA O DESENHO: **a classificação é POR DIA, não por
pessoa.** A fórmula compara a data do dia de ponto (`C`) com a Data de Início
(`U`) e a Data de Admissão (`V`). Ou seja: a MESMA pessoa pode ter dias de DIÁRIA
e dias de CTPS no mesmo mês — os dias entre começar a trabalhar e ser registrada
são diária; do registro em diante, CTPS.

Classificar por pessoa, como eu ia fazer, jogaria o mês inteiro de quem foi
admitido no meio do período para um lado só — e metade do dinheiro iria para o
método de pagamento errado, com a pessoa recebendo de menos ou de mais.

A fórmula original, para conferência:

    =ARRAYFORMULA(SEERRO(SE(A2:A="";"";
     SE((((U="")*(V="")) + ((V="")*((AA="CLT (tempo Indeterminado)")
                                   +(AA="CLT (tempo determinado)"))));"DT INÍCIO/ADMISSÃO";
     SE(((U<=C)*(C<V));"DIÁRIA";
     SE(((U<=C)*(V=""));"DIÁRIA";
     SE(((U<>"")*(V="")*(X="Prestador de Serviço")*(AA="Autônomo (RPA)"));"DIÁRIA";
     SE(((U=V)*(X="Prestador de Serviço")*(AA="Autônomo (RPA)"));"DIÁRIA";
     SE(((U>V)*(X="Prestador de Serviço")*(AA="Autônomo (RPA)"));"DIÁRIA";
     "CTPS")))))));"NÃO ENCONTRADO"))

    U = Data de Início   V = Data de Admissão   C = a data do dia de ponto
    X = Tipo de Cadastro AA = Tipo de Contrato

⚠️ A ORDEM DOS TESTES É A DA FÓRMULA, e não pode ser "arrumada". Vários casos se
sobrepõem (a regra 4 contém a 5, por exemplo), e a ordem é o que decide qual
vence. Reordenar para ficar mais bonito trocaria a resposta em silêncio.
"""
from __future__ import annotations

import datetime as dt
import logging

logger = logging.getLogger("analisesps.folha")

CTPS = "CTPS"
DIARIA = "DIÁRIA"
FALTA_DATA = "DT INÍCIO/ADMISSÃO"
NAO_ENCONTRADO = "NÃO ENCONTRADO"

PRESTADOR = "prestador de serviço"
RPA = "autônomo (rpa)"
CLT = ("clt (tempo indeterminado)", "clt (tempo determinado)")

# ⚠️ VAZIO VALE COMO "ANTES DE TUDO", porque é isso que a planilha faz: no Google
# Sheets, célula vazia numa comparação de data vale ZERO (30/12/1899). Então:
#
#     Data de Início vazia   →  "início <= o dia"  é VERDADE
#     Data de Admissão vazia →  "o dia < admissão" é FALSO
#
# Não é uma escolha minha; é o comportamento que está em produção há tempo, e
# mudá-lo aqui faria a classificação divergir da planilha justamente nos casos de
# cadastro incompleto — que são os que mais aparecem.
VAZIO = dt.date.min


def _data(valor):
    """A data de uma célula. Vazio vira `VAZIO` (ver o aviso acima)."""
    if isinstance(valor, dt.datetime):
        return valor.date()
    if isinstance(valor, dt.date):
        return valor
    texto = str(valor or "").strip()
    if not texto:
        return VAZIO
    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return dt.datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    return VAZIO


def _igual(texto, alvo) -> bool:
    return " ".join(str(texto or "").split()).strip().lower() == alvo


def classificar_dia(data, cadastro) -> str:
    """CTPS, DIÁRIA, DT INÍCIO/ADMISSÃO ou NÃO ENCONTRADO — para UM dia.

    `cadastro` é `{"inicio", "admissao", "tipo", "contrato"}` — ou None/vazio, que
    é o "não encontrado" do `SEERRO` da fórmula.
    """
    if not cadastro:
        return NAO_ENCONTRADO
    dia = _data(data)
    if dia is VAZIO or dia == VAZIO:
        return NAO_ENCONTRADO

    inicio = _data(cadastro.get("inicio"))
    admissao = _data(cadastro.get("admissao"))
    tipo = cadastro.get("tipo")
    contrato = cadastro.get("contrato")

    sem_inicio = inicio == VAZIO
    sem_admissao = admissao == VAZIO
    e_clt = any(_igual(contrato, c) for c in CLT)
    e_prestador_rpa = _igual(tipo, PRESTADOR) and _igual(contrato, RPA)

    # 1) Falta data para decidir. Ou as duas em branco, ou sem admissão sendo CLT
    #    (CLT sem data de admissão é cadastro pela metade, não é diarista).
    if (sem_inicio and sem_admissao) or (sem_admissao and e_clt):
        return FALTA_DATA
    # 2) O dia caiu entre começar a trabalhar e ser registrada: DIÁRIA.
    if inicio <= dia < admissao:
        return DIARIA
    # 3) Começou e nunca foi registrada.
    if inicio <= dia and sem_admissao:
        return DIARIA
    # 4) a 6) Prestador de Serviço com Autônomo (RPA): diária nos três arranjos de
    #    data que a fórmula lista.
    if e_prestador_rpa and (
            (not sem_inicio and sem_admissao)
            or inicio == admissao
            or inicio > admissao):
        return DIARIA
    return CTPS


def classificar_dias(dias, cadastro) -> dict:
    """Quantos dias de cada vínculo. `dias` é uma lista de datas.

    Devolve `{"CTPS": n, "DIÁRIA": n, ...}` só com o que apareceu — é o que
    permite dizer "esta pessoa tem 7 dias de diária e 8 de CTPS no período", que é
    o caso de quem foi admitido no meio da quinzena."""
    contagem: dict = {}
    for data in dias or []:
        chave = classificar_dia(data, cadastro)
        contagem[chave] = contagem.get(chave, 0) + 1
    return contagem


def da_contabilidade(dias, cadastro) -> bool:
    """Esta pessoa tem ALGUM dia que deveria vir na folha da contabilidade?

    ⚠️ "ALGUM", e não "todos": quem foi admitido no dia 10 tem dias de diária e
    dias de CTPS na mesma quinzena, e os de CTPS têm de vir na folha. Exigir que
    todos fossem CTPS esconderia exatamente o caso que mais dá confusão."""
    return classificar_dias(dias, cadastro).get(CTPS, 0) > 0
