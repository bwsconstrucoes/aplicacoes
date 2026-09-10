# ============================================================================
# ERP — core/perguntas/catalogo.py
# A lista das perguntas que o sistema sabe responder, e de quem pode fazê-las.
#
# Este arquivo é a ponte entre `PERGUNTAS.md` (que é conversa escrita, para
# pessoas) e o código que responde. Quando uma pergunta do documento vira
# função, ela entra aqui — e é aqui que o assistente vai procurar, no dia em
# que ele existir. Enquanto ele não existe, a própria tela usa esta lista.
#
# CADA PERGUNTA DIZ A QUE GRUPO PERTENCE, e o grupo é o que decide a ROTA que
# a responde — e, portanto, a permissão. Isso não é burocracia: a regra do
# repositório é que a ação declarada na rota decida sozinha quem entra. Uma
# rota só, respondendo perguntas de pesos diferentes, obrigaria a conferir
# permissão por dentro — e aí a declaração da rota mentiria.
#
# Hoje só existe o grupo "financeiro", que vive sob `ver_erp` + ESCOPO: todo
# operador pode perguntar, e cada um recebe a resposta calculada apenas sobre
# o que ele já poderia ver na tela de Títulos. Grupo novo (suprimentos,
# contratos) ganha rota própria com a ação própria dele.
# ============================================================================
from __future__ import annotations

from typing import Any, Callable

from app.apps.erp.core.perguntas import respostas

# tipo do parâmetro → o que a tela desenha
DATA, TEXTO = "data", "texto"


def _p(nome: str, rotulo: str, tipo: str = TEXTO, dica: str = "") -> dict[str, Any]:
    return {"nome": nome, "rotulo": rotulo, "tipo": tipo, "dica": dica}


# A ORDEM É A DA TELA, e ela não é alfabética de propósito: começa pelo que se
# pergunta todo dia e termina no que se pergunta uma vez por mês.
CATALOGO: list[dict[str, Any]] = [
    {
        "chave": "panorama_de_vencimentos",
        "grupo": "financeiro",
        "pergunta": "Como está o caixa dos próximos dias?",
        "exemplos": ["o que vence hoje", "tem muita coisa vencida?",
                     "como está a semana"],
        "parametros": [],
        "funcao": respostas.panorama_de_vencimentos,
    },
    {
        "chave": "a_pagar_no_periodo",
        "grupo": "financeiro",
        "pergunta": "O que tem a pagar num período?",
        "exemplos": ["o que tem a pagar hoje",
                     "o que tem a pagar esta semana na obra CREPETRIUNFO"],
        "parametros": [
            _p("de", "De", DATA, "em branco = hoje"),
            _p("ate", "Até", DATA, "em branco = o mesmo dia"),
            _p("obra", "Obra", TEXTO, "código ou parte dele; em branco = todas"),
        ],
        "funcao": respostas.a_pagar_no_periodo,
    },
    {
        "chave": "vencidos_sem_pagar",
        "grupo": "financeiro",
        "pergunta": "O que está vencido e não foi pago?",
        "exemplos": ["o que está atrasado", "tem conta vencida na obra tal?"],
        "parametros": [_p("obra", "Obra", TEXTO, "em branco = todas")],
        "funcao": respostas.vencidos_sem_pagar,
    },
    {
        "chave": "esperando_decisao",
        "grupo": "financeiro",
        "pergunta": "O que está parado esperando decisão, e de quem é a vez?",
        "exemplos": ["onde a fila está parada", "o que está esperando aprovação"],
        "parametros": [],
        "funcao": respostas.esperando_decisao,
    },
    {
        "chave": "sem_documento",
        "grupo": "financeiro",
        "pergunta": "Quais títulos estão sem documento anexado?",
        "exemplos": ["o que está sem nota", "falta documento em qual lançamento"],
        "parametros": [],
        "funcao": respostas.sem_documento,
    },
]

POR_CHAVE: dict[str, dict[str, Any]] = {p["chave"]: p for p in CATALOGO}


def do_grupo(grupo: str) -> list[dict[str, Any]]:
    return [p for p in CATALOGO if p["grupo"] == grupo]


def para_a_tela(grupo: str = "") -> list[dict[str, Any]]:
    """O catálogo sem a função — o que a tela precisa para montar a lista."""
    escolhidas = do_grupo(grupo) if grupo else CATALOGO
    return [{k: v for k, v in p.items() if k != "funcao"} for p in escolhidas]


def _converter(valor: Any, tipo: str) -> Any:
    """Texto da tela vira o tipo que a função espera.

    Isto existe porque a tela (e amanhã a IA) mandam TEXTO: uma data escolhida
    no calendário chega como "2026-09-10", e a função que compara com
    vencimento receberia uma string. Converter aqui, uma vez, protege todas as
    perguntas — e uma data impossível vira "não informado" em vez de derrubar
    a resposta.
    """
    from datetime import date as _date

    if tipo != DATA or isinstance(valor, _date):
        return valor
    try:
        return _date.fromisoformat(str(valor).strip())
    except ValueError:
        return None


def responder(chave: str, s: Any, usuario: Any,
              parametros: dict[str, Any]) -> dict[str, Any]:
    """Chama a função da pergunta, passando só os parâmetros que ela declara.

    Filtrar pelos parâmetros DECLARADOS é o que impede a tela (ou, amanhã, a
    IA) de injetar argumento que a função não esperava.
    """
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado

    pergunta = POR_CHAVE.get((chave or "").strip())
    if pergunta is None:
        raise ErroNaoEncontrado("Pergunta desconhecida.")
    tipos = {p["nome"]: p["tipo"] for p in pergunta["parametros"]}
    limpos = {}
    for nome, bruto in (parametros or {}).items():
        if nome not in tipos or not bruto:
            continue
        valor = _converter(bruto, tipos[nome])
        if valor not in (None, ""):
            limpos[nome] = valor
    funcao: Callable = pergunta["funcao"]
    resposta = funcao(s, usuario, **limpos)
    resposta["chave"] = pergunta["chave"]
    resposta["pergunta"] = pergunta["pergunta"]
    return resposta
