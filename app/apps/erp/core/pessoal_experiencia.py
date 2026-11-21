# -*- coding: utf-8 -*-
"""
ERP — O CONTRATO DE EXPERIÊNCIA: quando acaba, e o que isso muda (15/11/2026).

Pedido dele, com todas as letras:

    *"Se é encerramento de contrato de experiência, nós temos essa informação,
    nós temos campo para ela na base de dados: a data de admissão. (…) A gente
    pode contratar 45 mais 45, 30 mais 90, 30 mais 60, 60 mais 30, ou pode
    contratar 90 dias direto. (…) Eu quero facilitar a seleção do tipo de
    desligamento, principalmente relacionado a contrato de experiência. Porque
    eu vou pedir hoje: não, mas o contrato de experiência encerra tal. A gente
    já tem essa informação."*

    *"Quando é encerramento de contrato de experiência, eu não preciso dar
    aviso. Então essa data de aviso já se torna desnecessária, concorda?"*

⚑ TUDO AQUI SAI DE DOIS CAMPOS QUE JÁ EXISTEM: `colaboradores.admissao` e
`colaboradores.contrato_experiencia` (migração 086). Nada de tabela nova —
a informação estava lá, parada, e ninguém a usava para avisar nada.

AS DUAS REGRAS DE DIREITO QUE O CÓDIGO PRECISA SABER, e elas não são opinião:

1. **O contrato de experiência vale no máximo 90 dias**, e pode ser prorrogado
   UMA única vez dentro desse teto (CLT, art. 445 § único e 451). Por isso os
   arranjos são sempre dois pedaços que somam até 90: 45+45, 30+60, 60+30,
   30+30, ou 90 direto.
2. **No encerramento no prazo não há aviso prévio** — o contrato acaba porque
   o prazo acabou. Aviso prévio existe quando alguém ROMPE o contrato antes do
   fim (art. 481, e aí a regra é outra). É por isso que a tela esconde o campo
   de aviso quando a forma escolhida é o encerramento da experiência: pedir
   uma data que não existe é convidar ao erro.

⚠️ O QUE ESTE ARQUIVO NÃO FAZ, e é de propósito: ele NÃO decide desligamento,
não calcula verba e não conta prazo de pagamento. Ele diz **até quando vai o
contrato** e **em que fase a pessoa está hoje** — para a tela avisar quem pede.
Decisão e cálculo de rescisão são da contabilidade.
"""
from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Optional

# O teto legal. Está aqui com nome para não virar "90" solto no meio de uma
# conta — quem ler daqui a um ano precisa saber que é lei, não escolha.
TETO_LEGAL_DIAS = 90

# Como a planilha e o cadastro escrevem cada arranjo. A leitura é por NÚMEROS,
# não por texto exato: "45+45 dias", "45 + 45", "45+45" e "45 mais 45" são a
# mesma coisa, e o cadastro antigo tem as três grafias.
SEM_EXPERIENCIA = ("", "sem contrato de experiencia", "sem contrato de experiência",
                   "nao", "não", "n/a", "-")


def _numeros(texto: str) -> list[int]:
    return [int(x) for x in re.findall(r"\d+", texto or "")]


def partes(contrato: Optional[str]) -> list[int]:
    """Os pedaços do contrato, em dias. Vazio = não há experiência.

    "45+45 dias" → [45, 45]. "90 dias" → [90]. O que não der para entender
    volta vazio: inventar prazo de contrato seria pior que não avisar nada.
    """
    bruto = (contrato or "").strip()
    if bruto.lower() in SEM_EXPERIENCIA:
        return []
    achados = [n for n in _numeros(bruto) if 0 < n <= TETO_LEGAL_DIAS]
    # "45+45 dias" traz o 45, o 45 — e nada mais. Um terceiro número seria
    # outra coisa escrita no mesmo campo; nesse caso ficamos com os dois
    # primeiros, que é o que a lei permite.
    return achados[:2]


def situacao(admissao: Optional[date], contrato: Optional[str],
             hoje: Optional[date] = None) -> dict[str, Any]:
    """Em que pé está o contrato de experiência desta pessoa.

    Devolve sempre o mesmo formato, tenha experiência ou não — quem chama não
    precisa saber de antemão, e tela que precisa adivinhar é tela que erra.
    """
    hoje = hoje or date.today()
    pedacos = partes(contrato)
    vazio = {"tem": False, "contrato": (contrato or "").strip(),
             "fim_primeira": None, "fim_total": None, "fase": "",
             "dias_para_o_fim": None, "prorrogavel": False,
             "ja_acabou": False, "texto": ""}
    if not admissao or not pedacos:
        if not admissao and pedacos:
            vazio["texto"] = ("Esta pessoa tem contrato de experiência "
                              f"({contrato}), mas o cadastro está SEM a data "
                              "de admissão — sem ela não dá para dizer quando "
                              "acaba.")
        return vazio

    # ⚑ O PRIMEIRO DIA CONTA. Contrato de 45 dias que começa em 1º de março
    # acaba em 14 de abril, não em 15: o dia da admissão é o dia 1.
    fim_primeira = admissao + timedelta(days=pedacos[0] - 1)
    fim_total = (fim_primeira + timedelta(days=pedacos[1])
                 if len(pedacos) > 1 else fim_primeira)
    prorrogavel = len(pedacos) > 1

    if hoje > fim_total:
        fase, dias = "acabou", (hoje - fim_total).days
    elif prorrogavel and hoje > fim_primeira:
        fase, dias = "prorrogado", (fim_total - hoje).days
    else:
        fase, dias = "primeira", (fim_primeira - hoje).days

    return {
        "tem": True,
        "contrato": (contrato or "").strip(),
        "fim_primeira": str(fim_primeira),
        "fim_total": str(fim_total),
        "fase": fase,
        "dias_para_o_fim": dias,
        "prorrogavel": prorrogavel,
        "ja_acabou": fase == "acabou",
        "texto": _em_portugues(pedacos, admissao, fim_primeira, fim_total,
                               fase, dias, prorrogavel),
    }


def _br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _em_portugues(pedacos, admissao, fim_primeira, fim_total, fase, dias,
                  prorrogavel) -> str:
    """A frase que a tela mostra. Escrita aqui, e não na tela, porque o papel
    e o aviso do agente vão querer a mesma."""
    arranjo = " + ".join(f"{n}" for n in pedacos) + " dias"
    inicio = f"Admitida em {_br(admissao)}, contrato de experiência de {arranjo}."
    if fase == "acabou":
        return (f"{inicio} O contrato **já venceu** em {_br(fim_total)} — há "
                f"{dias} dia(s). Se ninguém encerrou, ele virou prazo "
                f"indeterminado, e aí o desligamento NÃO é mais encerramento "
                f"de experiência.")
    if fase == "prorrogado":
        return (f"{inicio} Está na **prorrogação**, que termina em "
                f"{_br(fim_total)} — faltam {dias} dia(s).")
    if prorrogavel:
        return (f"{inicio} A **primeira parte termina em {_br(fim_primeira)}** "
                f"(faltam {dias} dia(s)); sem encerrar, ela se prorroga "
                f"automaticamente até {_br(fim_total)}.")
    return (f"{inicio} **Termina em {_br(fim_total)}** — faltam {dias} dia(s). "
            f"Não há prorrogação neste arranjo.")


# ---------------------------------------------------------------------------
# O QUE ISSO MUDA NO PEDIDO DE DESLIGAMENTO
# ---------------------------------------------------------------------------
# A forma do Pipe, como ele a lê na tela. Só vale para quem tem CARTEIRA: o
# prestador de serviço não tem forma de desligamento, tem data de saída —
# ele disse isso no mesmo dia, e as regras não se enquadram.
FORMA_EXPERIENCIA = "Encerramento de Contrato de Experiência"

# ⚑ O MESMO VALOR CHEGA AQUI DE DUAS FORMAS, e as duas têm de valer: a TELA
# manda o rótulo do Pipe ("Encerramento de Contrato de Experiência"), e o
# SERVIDOR, depois de `traduzir_do_pipe`, trabalha com o código gravado
# ("FIM_EXPERIENCIA"). Aceitar só um dos dois faria a tela esconder o campo e
# o servidor continuar exigindo — que é o pior desfecho possível: a pessoa não
# vê onde preencher e não consegue enviar.
CODIGO_EXPERIENCIA = "FIM_EXPERIENCIA"


def exige_aviso_previo(forma: Optional[str]) -> bool:
    """Encerramento no prazo NÃO tem aviso prévio — o contrato acabou sozinho.

    Ele: *"quando é encerramento de contrato de experiência, eu não preciso
    dar aviso. Então essa data de aviso já se torna desnecessária."*
    """
    chave = _chave(forma)
    return chave not in (_chave(FORMA_EXPERIENCIA), _chave(CODIGO_EXPERIENCIA))


def _chave(texto: Optional[str]) -> str:
    import unicodedata
    cru = unicodedata.normalize("NFD", (texto or "").strip().lower())
    return "".join(c for c in cru if unicodedata.category(c) != "Mn")


def sugestao_de_forma(situacao_exp: dict[str, Any]) -> str:
    """O que a tela deixa pré-escolhido — sugestão, nunca decisão.

    Quem desliga é gente. Mas quando o contrato de experiência está correndo,
    começar com a forma errada é o engano mais caro do formulário: muda verba,
    muda aviso e muda o que a contabilidade faz.
    """
    if situacao_exp.get("tem") and not situacao_exp.get("ja_acabou"):
        return FORMA_EXPERIENCIA
    return ""


# ---------------------------------------------------------------------------
# QUEM ESTÁ PERTO DO FIM DA EXPERIÊNCIA (20/11/2026)
#
# Ele: *"Prorrogação da experiência. Entendo que DP e o administrativo de obra
# devam ser alertados sobre o acontecimento. Poucos dias antes."* A
# prorrogação acontece SOZINHA no fim da primeira parte (e o contrato vira
# indeterminado no fim da segunda): sem aviso, a decisão de encerrar chega
# depois do prazo — e aí custa aviso prévio e multa.
# ---------------------------------------------------------------------------
AVISAR_COM_DIAS = 10


def terminando(s, obras=None, hoje: Optional[date] = None,
               dias: int = AVISAR_COM_DIAS) -> list[dict[str, Any]]:
    """Carteira ativa cuja experiência (1ª parte ou prorrogação) acaba em até
    `dias`. `obras=None` é a base inteira; lista vazia, ninguém."""
    from sqlalchemy import text
    from app.apps.erp.core import obra_atual
    hoje = hoje or date.today()
    if obras is not None and not obras:
        return []
    filtro = ""
    if obras is not None:
        filtro = (" AND " + obra_atual.obras_da_pessoa_sql("c")
                  if obra_atual.tem_tabela(s) else " AND c.obra_id = ANY(:obras)")
    linhas = s.execute(text(f"""
        SELECT c.id, c.nome, c.admissao, c.contrato_experiencia, o.codigo AS obra,
               g.nome AS cargo
          FROM colaboradores c
          LEFT JOIN obras o ON o.id = c.obra_id
          LEFT JOIN cargos g ON g.id = c.cargo_id
         WHERE UPPER(COALESCE(c.vinculo, 'CTPS')) = 'CTPS'
           AND UPPER(COALESCE(c.situacao, 'ATIVO')) = 'ATIVO'
           AND c.demissao IS NULL AND c.admissao IS NOT NULL
           AND c.admissao >= :desde
           AND COALESCE(c.contrato_experiencia, '') <> ''{filtro}
         ORDER BY c.nome"""),
        {"desde": hoje - timedelta(days=TETO_LEGAL_DIAS + 5),
         "obras": obras or []}).mappings().all()
    saida = []
    for l in linhas:
        sit = situacao(l["admissao"], l["contrato_experiencia"], hoje)
        if not sit["tem"] or sit["ja_acabou"]:
            continue
        falta = sit["dias_para_o_fim"]
        if falta is None or falta > dias:
            continue
        primeira = sit["fase"] == "primeira" and sit["prorrogavel"]
        fim = date.fromisoformat(sit["fim_primeira"] if sit["fase"] == "primeira"
                                 else sit["fim_total"])
        saida.append({
            "colaborador_id": l["id"], "pessoa": l["nome"], "obra": l["obra"] or "",
            "cargo": l["cargo"] or "", "fim": str(fim), "faltam_dias": falta,
            "o_que_acontece": (
                f"se ninguém fizer nada, o contrato se PRORROGA sozinho até "
                f"{_br(date.fromisoformat(sit['fim_total']))}" if primeira else
                "se ninguém fizer nada, vira contrato por prazo INDETERMINADO — "
                "depois disso, sair custa aviso prévio e multa de 40%"),
            "para_encerrar": (f"para encerrar no prazo, peça o desligamento "
                              f"(encerramento de experiência) com saída em {_br(fim)}"),
        })
    return saida


# ---------------------------------------------------------------------------
# PASSADA A EXPERIÊNCIA, O CONTRATO VIRA INDETERMINADO — SOZINHO (20/11/2026)
#
# Ele: *"Via de regra assinamos CLT com contrato determinado. Passado o
# período de experiência ele vira indeterminado."* E, perguntado se o sistema
# deveria trocar sozinho: *"quero que altere sozinho o contrato."*
#
# Roda na rodada diária. Troca "CLT (tempo determinado)" por "CLT (tempo
# indeterminado)" no cadastro e no vínculo aberto de quem tem carteira e já
# passou do fim da experiência (prorrogação incluída). Quem está como
# "determinado" SEM contrato de experiência legível fica como está: não dá
# para saber quando acabou — a tela de Regras mostra esses.
#
# ⚑ CUSTO: uma consulta por dia e uma gravação por pessoa que virou.
# ---------------------------------------------------------------------------
def virar_indeterminado(s, hoje: Optional[date] = None) -> dict[str, Any]:
    """Nunca levanta. Devolve quantos viraram."""
    import logging
    from sqlalchemy import text
    from app.apps.erp.core.comum.auditoria import registrar_evento
    hoje = hoje or date.today()
    feito = {"viraram": 0, "sem_data": 0}
    try:
        with s.begin_nested():
            for l in s.execute(text("""
                SELECT id, admissao, contrato_experiencia FROM colaboradores
                 WHERE UPPER(COALESCE(vinculo, 'CTPS')) = 'CTPS'
                   AND tipo_contrato = 'CLT_DETERMINADO'
                   AND UPPER(COALESCE(situacao, 'ATIVO')) = 'ATIVO'""")).mappings().all():
                sit = situacao(l["admissao"], l["contrato_experiencia"], hoje)
                if not sit["tem"]:
                    feito["sem_data"] += 1
                    continue
                if not sit["ja_acabou"]:
                    continue
                s.execute(text("UPDATE colaboradores SET tipo_contrato = 'CLT_INDETERMINADO' "
                               "WHERE id = :i"), {"i": l["id"]})
                s.execute(text("""UPDATE colaborador_vinculos
                                     SET tipo_contrato = 'CLT_INDETERMINADO'
                                   WHERE colaborador_id = :i AND demissao IS NULL
                                     AND tipo_contrato = 'CLT_DETERMINADO'"""),
                          {"i": l["id"]})
                registrar_evento(s, "colaborador", int(l["id"]), "VIROU_INDETERMINADO",
                                 {"fim_da_experiencia": sit["fim_total"],
                                  "por": "a rotina diária: a experiência acabou"}, None)
                feito["viraram"] += 1
        if feito["viraram"]:
            logging.getLogger(__name__).info("ERP/experiência: %s", feito)
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).exception("ERP/experiência: a virada falhou")
    return feito

