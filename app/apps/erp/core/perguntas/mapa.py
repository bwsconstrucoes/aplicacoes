# -*- coding: utf-8 -*-
"""
O MAPA — o que o sistema guarda, dito em português de negócio.

⚑ POR QUE ESTE ARQUIVO EXISTE (27/11/2026). O dono desmontou o caminho que a
gente vinha seguindo:

> *"A pergunta que a gente vai fazer é algo meio que infinito. Não tem como ser
> previsto tudo (…) eu não queria perder tempo tendo que digitar alguma
> pergunta que na verdade vai surgir no momento. Então nessa lógica nunca vai
> funcionar. (…) Eu estava pensando se a gente não poderia construir algo
> dinâmico."*

Ele tem razão, e o número prova: o `PERGUNTAS.md` tinha **219 perguntas
escritas** e o catálogo do assistente respondia **20**. Nove por cento, e a
distância crescendo — cada coisa nova que o ERP ganha nasce invisível para o
assistente, porque alguém precisaria escrever a consulta dela à mão.

**A virada:** em vez de uma lista de PERGUNTAS, um MAPA do que existe. A
pergunta é montada na hora, contra o mapa. O catálogo escrito à mão continua
valendo para o que já está lá (é exato e não custa IA); o mapa cobre o resto.

---------------------------------------------------------------------------
O QUE É UMA VISÃO AQUI, E POR QUE NÃO É "A TABELA"
---------------------------------------------------------------------------

O mapa **não** entrega as 144 tabelas do ERP para a IA escrever SQL em cima.
Isso seria rápido de fazer e errado por três motivos:

1. **Recorte.** A pessoa só pode ver as obras dela. Uma consulta inventada
   livremente sobre `colaboradores` mostraria a empresa inteira — o
   assistente viraria a porta dos fundos do controle de acesso.
2. **Significado.** `situacao`, `etapa`, `vinculo` e `regime` são quatro
   coisas diferentes que parecem a mesma. A IA escolheria a errada em
   silêncio.
3. **Conferência.** Com 144 tabelas não há como testar o que ela vai montar.

Então o mapa é feito de **VISÕES**: um punhado de consultas que nós
escrevemos, **já com o recorte da pessoa aplicado**, com nomes e colunas em
português de negócio. A IA só enxerga essas visões — e só pode escrever SELECT
sobre elas. O que ela monta é conferível, porque o vocabulário é pequeno.

⚠️ **A COLUNA `obra_id` NÃO SAI NAS VISÕES DE PROPÓSITO.** O recorte já foi
aplicado dentro delas; deixar o número da obra à mão só serviria para a IA
tentar filtrar de novo, por um número que ela não tem como saber.

---------------------------------------------------------------------------
COMO ISTO NÃO ENVELHECE
---------------------------------------------------------------------------

Foi assim que o `PERGUNTAS.md` morreu: escrito à mão, divergiu do código.
Aqui, duas travas:

- Cada visão declara as colunas que entrega, e **há teste rodando a visão
  contra um banco de verdade** e conferindo que ela entrega exatamente essas
  colunas. Campo que muda de nome quebra a suíte, não a resposta do dono.
- A "varredura dos papéis" (`scripts/varrer_perguntas.py`) roda de novo a cada
  mudança e diz o que o mapa deixou de alcançar.
"""
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Usuario

# ---------------------------------------------------------------------------
# AS VISÕES DO PESSOAL
#
# `sql` recebe dois parâmetros, sempre os mesmos: `:todas` (a pessoa enxerga
# tudo?) e `:obras` (quais obras, quando não). Quem monta é `visoes_para()`.
# ---------------------------------------------------------------------------
VISOES: tuple[dict[str, Any], ...] = (
    {
        "nome": "pessoas",
        "o_que_e": "Uma linha por colaborador cadastrado — quem trabalha ou "
                   "já trabalhou na empresa.",
        "colunas": {
            "nome": "nome do colaborador",
            "cpf": "CPF, só os dígitos",
            "obra": "código e nome da obra de cadastro (ex.: '112 · Residencial Vale Verde')",
            "obra_codigo": "só o código da obra, para comparar (ex.: 'CRECHESUAPE')",
            "cargo": "o cargo dela hoje",
            "vinculo": "CTPS (carteira assinada), PRESTADOR, AUTONOMO — como "
                       "a pessoa é contratada",
            "situacao": "ATIVO, AFASTADO, DESLIGADO — a vida do vínculo",
            "etapa": "em que ponto do cadastro o DP está com ela (INICIAL, "
                     "CONFERENCIA, CONCLUIDO, DESLIGADO, …). É a FILA DE "
                     "TRABALHO do DP, não a situação do vínculo",
            "admissao": "data de admissão",
            "demissao": "data de saída, vazia em quem está na casa",
            "mensalista": "true = recebe valor fixo por mês; false = por diária",
            "telefone": "telefone do cadastro",
            "tem_acesso_ao_sistema": "true = já tem login no ERP/portal",
            "contrato_experiencia": "o desenho da experiência, quando há "
                                    "(ex.: '45+45'); vazio em quem não está "
                                    "em experiência",
        },
        "cuidados": [
            "SITUAÇÃO e ETAPA são coisas diferentes: situação é o vínculo "
            "(ATIVO/AFASTADO/DESLIGADO), etapa é o que falta o DP fazer.",
            "Quem está na casa HOJE é situacao <> 'DESLIGADO' e demissao "
            "vazia — demissão preenchida com situação ATIVO acontece na "
            "janela entre pedir e atender.",
        ],
        "sql": """
            SELECT c.nome,
                   c.cpf,
                   CASE WHEN o.codigo IS NULL THEN NULL
                        ELSE o.codigo || ' · ' || o.nome END AS obra,
                   o.codigo                                  AS obra_codigo,
                   g.nome                                    AS cargo,
                   upper(coalesce(c.vinculo, 'CTPS'))        AS vinculo,
                   upper(coalesce(c.situacao, 'ATIVO'))      AS situacao,
                   upper(coalesce(c.etapa, ''))              AS etapa,
                   c.admissao,
                   c.demissao,
                   coalesce(c.mensalista, false)             AS mensalista,
                   c.telefone,
                   (c.usuario_id IS NOT NULL)                AS tem_acesso_ao_sistema,
                   coalesce(c.contrato_experiencia, '')      AS contrato_experiencia
              FROM colaboradores c
              LEFT JOIN obras  o ON o.id = c.obra_id
              LEFT JOIN cargos g ON g.id = c.cargo_id
             WHERE (:todas OR {escopo_pessoa})
        """,
    },
    {
        "nome": "solicitacoes",
        "o_que_e": "Os pedidos que a obra faz ao DP: admissão, desligamento, "
                   "férias, afastamento, cargo, dados, dependente, ajuda de "
                   "custo. Uma linha por pedido.",
        "colunas": {
            "numero": "o número do pedido (ex.: 'SDP-000012')",
            "tipo": "o nome do pedido em português (ex.: 'Solicitação de Férias')",
            "situacao": "Aberta, Em análise, Em andamento, Atendida, "
                        "Recusada, Cancelada",
            "pessoa": "de quem é o pedido",
            "obra": "código e nome da obra do pedido",
            "obra_codigo": "só o código da obra",
            "aberto_em": "quando o pedido foi aberto",
            "atendido_em": "quando o DP concluiu, vazio no que está em aberto",
            "resposta": "o que o DP respondeu",
            "dias_parado": "quantos dias desde a abertura, para o que ainda "
                           "não foi atendido",
        },
        "cuidados": [
            "'Em aberto' quer dizer situacao em (Aberta, Em análise, Em "
            "andamento) — Atendida, Recusada e Cancelada já terminaram.",
            "A obra do pedido é a de quando ele foi aberto; a pessoa pode "
            "ter mudado de obra depois.",
        ],
        "sql": """
            SELECT sol.numero,
                   {tipo_em_portugues}                        AS tipo,
                   {situacao_em_portugues}                    AS situacao,
                   coalesce(c.nome, sol.nome_informado)       AS pessoa,
                   CASE WHEN o.codigo IS NULL THEN NULL
                        ELSE o.codigo || ' · ' || o.nome END  AS obra,
                   o.codigo                                   AS obra_codigo,
                   sol.criado_em::date                        AS aberto_em,
                   sol.atendido_em::date                      AS atendido_em,
                   sol.resposta,
                   CASE WHEN sol.atendido_em IS NULL
                        THEN (CURRENT_DATE - sol.criado_em::date)
                        ELSE NULL END                         AS dias_parado
              FROM dp_solicitacoes sol
              LEFT JOIN colaboradores c ON c.id = sol.colaborador_id
              LEFT JOIN obras o ON o.id = sol.obra_id
             WHERE (:todas OR sol.obra_id = ANY(:obras))
        """,
    },
    {
        "nome": "ferias",
        "o_que_e": "As férias programadas: quem sai, quando e por quantos dias.",
        "colunas": {
            "pessoa": "de quem são as férias",
            "obra": "código e nome da obra",
            "obra_codigo": "só o código da obra",
            "inicio": "primeiro dia de férias",
            "fim": "último dia de férias",
            "dias": "quantos dias",
            "situacao": "PEDIDA, CONFIRMADA, CANCELADA",
            "vendeu_um_terco": "true = vendeu 1/3 das férias (abono)",
            "adiantou_decimo": "true = pediu adiantamento do 13º",
        },
        "cuidados": [
            "Férias CANCELADA continua na tabela — quase sempre a pergunta "
            "quer só as não canceladas.",
            "'Está de férias hoje' é CURRENT_DATE entre inicio e fim, com "
            "situacao <> 'CANCELADA'.",
        ],
        "sql": """
            SELECT c.nome                                     AS pessoa,
                   CASE WHEN o.codigo IS NULL THEN NULL
                        ELSE o.codigo || ' · ' || o.nome END  AS obra,
                   o.codigo                                   AS obra_codigo,
                   f.inicio, f.fim, f.dias,
                   upper(f.situacao)                          AS situacao,
                   coalesce(f.abono_pecuniario, false)        AS vendeu_um_terco,
                   coalesce(f.adiantamento_13, false)         AS adiantou_decimo
              FROM ferias_programadas f
              JOIN colaboradores c ON c.id = f.colaborador_id
              LEFT JOIN obras o ON o.id = coalesce(f.obra_id, c.obra_id)
             WHERE (:todas OR {escopo_pessoa})
        """,
    },
)


# ---------------------------------------------------------------------------
# A VISÃO QUE DEPENDE DO PONTO — e por que ela é separada
#
# `ponto.marcacoes` vive em OUTRO schema, do aplicativo do ponto. Ele pode não
# existir num banco de teste, e o ERP trata isso como normal em todo lugar
# (`_consulta_opcional`, em `core/diario.py`). Aqui vale o mesmo: a visão
# entra no mapa **só quando a tabela existe**. Mapa que promete o que não há
# é a mesma mentira da porta que anuncia o que não tem.
# ---------------------------------------------------------------------------
VISAO_DO_PONTO: dict[str, Any] = {
    "nome": "presenca_na_obra",
    "o_que_e": "Quem bateu ponto, em que obra e em que dia. É o que responde "
               "'quantos trabalharam na obra tal hoje' — gente DE VERDADE no "
               "canteiro, que é diferente de quem está cadastrado nela.",
    "colunas": {
        "pessoa": "quem bateu",
        "obra": "código e nome da obra onde bateu",
        "obra_codigo": "só o código da obra",
        "dia": "o dia da batida",
        "cargo": "o cargo da pessoa",
    },
    "cuidados": [
        "Uma pessoa aparece UMA vez por dia e obra, mesmo batendo quatro "
        "vezes — contar linhas já é contar gente.",
        "Quem está cadastrado na obra e NÃO bateu não aparece aqui. "
        "'Quantos tem na obra' pelo cadastro é a visão `pessoas`; pelo "
        "canteiro, é esta.",
    ],
    "sql": """
        SELECT DISTINCT
               c.nome                                     AS pessoa,
               CASE WHEN o.codigo IS NULL THEN NULL
                    ELSE o.codigo || ' · ' || o.nome END  AS obra,
               o.codigo                                   AS obra_codigo,
               m.data_referencia                          AS dia,
               g.nome                                     AS cargo
          FROM ponto.marcacoes m
          JOIN colaboradores c ON c.id = m.colaborador_id
          LEFT JOIN obras  o ON o.id = m.obra_id
          LEFT JOIN cargos g ON g.id = c.cargo_id
         WHERE m.status <> 'REJEITADA'
           AND (:todas OR m.obra_id = ANY(:obras))
    """,
}


def _caso(coluna: str, de_para: dict[str, str]) -> str:
    """Um CASE que troca o código pelo nome em português, DENTRO do SQL.

    ⚑ Por que no SQL e não depois, em Python: a IA precisa poder FILTRAR por
    ele ("as que estão Em análise"). Se a tradução acontecesse só na volta, a
    coluna que ela enxerga no mapa (`tipo`, `situacao`) não existiria na hora
    de comparar — e o mapa estaria mentindo sobre o que entrega.

    ⚑ E o de-para vem do módulo das solicitações, nunca copiado: duas listas
    para a mesma coisa divergem no primeiro tipo novo.
    """
    if not de_para:
        return coluna
    partes = " ".join(
        f"WHEN {_literal(k)} THEN {_literal(v)}" for k, v in de_para.items())
    return f"CASE {coluna} {partes} ELSE {coluna} END"


def _literal(v: Any) -> str:
    """Texto como literal de SQL. Os valores vêm do nosso próprio código (não
    de fora), mas dobrar a aspa é o que impede um rótulo com apóstrofo —
    "Solicitação d'água", um dia — de quebrar a consulta."""
    return "'" + str(v or "").replace("'", "''") + "'"


def _tem_marcacoes(s: Session) -> bool:
    return s.execute(text(
        "SELECT 1 FROM information_schema.tables "
        " WHERE table_schema = 'ponto' AND table_name = 'marcacoes'")
    ).first() is not None


def visoes_para(s: Session, usuario: Optional[Usuario]) -> list[dict[str, Any]]:
    """As visões que ESTA pessoa pode consultar, já com o recorte dela dentro.

    O recorte é o mesmo das telas do Pessoal (`obras_no_pessoal`) — não há
    regra nova aqui, e isso é de propósito: se o assistente tivesse recorte
    próprio, mudar a permissão numa tela deixaria o assistente para trás.
    """
    from app.apps.erp.core import obra_atual
    from app.apps.erp.core.auth.permissoes import obras_no_pessoal

    obras = obras_no_pessoal(s, usuario) if usuario is not None else []
    # None = enxerga tudo. Lista vazia = não enxerga nada (e aí a visão não
    # devolve linha nenhuma, que é o certo).
    todas = obras is None
    # A pessoa pertence à obra pelo CADASTRO ou pela obra onde bate ponto —
    # a mesma régua do portal e dos avisos. Sem a tabela do ponto, só o
    # cadastro.
    escopo_pessoa = (obra_atual.obras_da_pessoa_sql("c")
                     if obra_atual.tem_tabela(s) else "c.obra_id = ANY(:obras)")

    from app.apps.erp.core import pessoal_solicitacoes as svc_sp

    recheio = {
        "escopo_pessoa": escopo_pessoa,
        "tipo_em_portugues": _caso(
            "sol.tipo", {k: (d.get("nome") or k) for k, d in svc_sp.TIPOS.items()}),
        "situacao_em_portugues": _caso("sol.situacao", svc_sp.SITUACAO_ROTULOS),
    }
    saida = []
    for v in list(VISOES) + ([VISAO_DO_PONTO] if _tem_marcacoes(s) else []):
        sql = v["sql"]
        for marca, valor in recheio.items():
            sql = sql.replace("{" + marca + "}", valor)
        saida.append({**v, "sql": sql})
    return [{**v, "parametros": {"todas": todas, "obras": obras or []}}
            for v in saida]


def para_a_ia(visoes: list[dict[str, Any]]) -> str:
    """O mapa como texto, do jeito que a IA lê.

    Curto de propósito: nome, o que é, colunas com o significado e os
    cuidados. Nada de SQL — a IA não precisa saber de onde vem, e mostrar o
    de dentro só a convidaria a inventar junção com tabela que ela não pode
    tocar.
    """
    partes = []
    for v in visoes:
        linhas = [f"VISÃO {v['nome']} — {v['o_que_e']}", "  colunas:"]
        linhas += [f"    - {c}: {d}" for c, d in v["colunas"].items()]
        if v.get("cuidados"):
            linhas.append("  cuidados:")
            linhas += [f"    - {c}" for c in v["cuidados"]]
        partes.append("\n".join(linhas))
    return "\n\n".join(partes)


def nomes(visoes: list[dict[str, Any]]) -> set[str]:
    return {v["nome"] for v in visoes}
