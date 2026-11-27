# -*- coding: utf-8 -*-
"""
A PERGUNTA MONTADA NA HORA — a cozinha, em vez do menu de 20 pratos.

⚑ O PEDIDO (27/11/2026), e ele derruba o desenho anterior:

> *"A pergunta que a gente vai fazer é algo meio que infinito. Não tem como
> ser previsto tudo (…) eu não queria perder tempo tendo que digitar alguma
> pergunta que na verdade vai surgir no momento. Então nessa lógica nunca vai
> funcionar. (…) Eu estava pensando se a gente não poderia construir algo
> dinâmico."*

COMO FUNCIONA, em três passos:

1. **O catálogo escrito à mão tenta primeiro** (`entender.py`). Pergunta
   conhecida é respondida por código testado: exata, instantânea e **sem
   custo de IA**. Isso não muda.
2. **Não sendo conhecida, a IA monta a consulta** contra o MAPA
   (`mapa.py`) — as visões do Pessoal, já recortadas por quem pergunta.
3. **A consulta passa pelas travas** (`consulta.py`) antes de encostar no
   banco, e a resposta vem **com o caminho à mostra**.

---------------------------------------------------------------------------
POR QUE A RESPOSTA SEMPRE MOSTRA O CAMINHO
---------------------------------------------------------------------------

Porque o dono **não tem como conferir o número**. Ele não vai ler SQL, e não
deve precisar. Então toda resposta montada na hora vem com três coisas em
português: **o que eu entendi**, **onde eu olhei** e **o que eu filtrei**.

É a mesma regra que a resposta sobre documentos já segue desde 11/09/2026
("sem a citação, é a IA falando"), e o motivo é idêntico: só se pode confiar
no que dá para conferir.

E a resposta diz, sem meias palavras, **que foi montada na hora** — ao
contrário das do catálogo, que são código testado. A pessoa tem direito de
saber qual das duas está lendo.

---------------------------------------------------------------------------
OS TRÊS "NÃO SEI", QUE SÃO DIFERENTES
---------------------------------------------------------------------------

Hoje o assistente, quando não entende, chuta duas perguntas quaisquer — foi
assim que "quantos funcionários temos hoje na obra de CRECHESUAPE?" virou "o
que tem a pagar num período?". Aqui os três casos são separados, porque a
saída de cada um é outra:

| O quê | O que a pessoa faz |
|---|---|
| **não entendi a pergunta** | reescreve com outras palavras |
| **entendi, mas o sistema não guarda isso** | fala com quem constrói o ERP |
| **entendi, mas você não alcança** | fala com quem dá permissão |

---------------------------------------------------------------------------
CUSTO
---------------------------------------------------------------------------

Pergunta do catálogo: **zero**. Pergunta nova: **uma chamada de IA** (o mapa
cabe num pedido só — ele tem quatro visões, não 144 tabelas). O teto por
pessoa e o da empresa valem aqui como em todo o resto (`ia_custo`).
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum import ia_custo
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.perguntas import consulta as svc_consulta
from app.apps.erp.core.perguntas import mapa as svc_mapa
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

MODELO = os.getenv("ERP_IA_MODELO_CONSULTA", "gpt-4o-mini")
OPERACAO = "perguntar_na_hora"

INSTRUCAO = """Você traduz uma pergunta de negócio em UMA consulta SQL de leitura.

O banco é PostgreSQL. Você só pode ler das VISÕES descritas abaixo — elas já
vêm filtradas para quem está perguntando. Não existe mais nada: nenhuma outra
tabela, nenhum outro schema.

REGRAS, todas obrigatórias:
- comece a consulta com SELECT (nunca WITH);
- use SOMENTE os nomes de visão e de coluna descritos;
- um comando só, sem ponto e vírgula;
- nada de INSERT/UPDATE/DELETE/CREATE/SET — isto é leitura;
- NÃO tente filtrar por obra para aplicar permissão: isso já está feito;
- quando a pessoa citar uma obra pelo código ou pelo nome, filtre com
  `obra_codigo ILIKE '%...%' OR obra ILIKE '%...%'`;
- se a pergunta pede uma contagem, devolva a contagem, não a lista;
- dê nomes em PORTUGUÊS às colunas do resultado (AS quantos, AS pessoa…).

Responda SOMENTE um JSON, sem texto em volta, com estas chaves:
{
  "da_para": true|false,
  "sql": "a consulta, quando da_para for true",
  "entendi": "o que você entendeu da pergunta, em uma frase, em português",
  "onde_olhei": "em quais visões, em português",
  "filtrei": "os filtros que você aplicou, em português; vazio se nenhum",
  "por_que_nao": "quando da_para for false: por que o mapa não alcança isso"
}

Se a informação pedida NÃO existir nas visões, devolva da_para=false e explique
no por_que_nao o que faltaria. NUNCA invente coluna, visão ou número."""


def _cliente():
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        raise ErroValidacao(
            "Responder pergunta nova depende da IA, e ela não está ligada "
            "neste serviço. As perguntas já conhecidas continuam funcionando.")
    try:
        from openai import OpenAI
    except ImportError:                                   # pragma: no cover
        raise ErroValidacao("Biblioteca da OpenAI indisponível no serviço.")
    return OpenAI(api_key=chave)


def _json_da_resposta(bruto: str) -> dict[str, Any]:
    t = re.sub(r"^```[a-z]*\s*|\s*```$", "", (bruto or "").strip(),
               flags=re.IGNORECASE).strip()
    try:
        d = json.loads(t)
    except Exception:                                      # noqa: BLE001
        raise ErroValidacao(
            "A IA respondeu num formato que eu não entendi. A pergunta não "
            "foi respondida.")
    if not isinstance(d, dict):
        raise ErroValidacao("A IA respondeu num formato inesperado.")
    return d


def montar(s: Session, pergunta: str, visoes: list[dict[str, Any]],
           usuario: Optional[Usuario]) -> dict[str, Any]:
    """Pede à IA a consulta. Não roda nada — só traduz."""
    uid = getattr(usuario, "id", None)
    ia_custo.exigir_saldo_de_ia(s, uid)
    resp = _cliente().chat.completions.create(
        model=MODELO,
        temperature=0,
        messages=[
            {"role": "system", "content": INSTRUCAO},
            {"role": "user", "content":
                f"AS VISÕES:\n\n{svc_mapa.para_a_ia(visoes)}\n\n"
                f"A PERGUNTA: {pergunta}"},
        ])
    ia_custo.registrar(s, modelo=MODELO, operacao=OPERACAO, resposta=resp,
                       usuario_id=uid)
    return _json_da_resposta(resp.choices[0].message.content)


def responder(s: Session, pergunta: str,
              usuario: Optional[Usuario]) -> dict[str, Any]:
    """A pergunta nova, do texto à resposta. Devolve sempre um dicionário com
    `respondeu` — nunca levanta por não saber, porque "não sei" é resposta."""
    texto = (pergunta or "").strip()
    if not texto:
        raise ErroValidacao("Escreva a pergunta.")

    visoes = svc_mapa.visoes_para(s, usuario)
    if not visoes:
        return {"respondeu": False, "tipo": "sem_alcance",
                "recado": "Você não alcança nenhuma obra, então não há o que "
                          "eu consultar por aqui. Quem resolve isso é quem "
                          "cuida do seu acesso."}

    montagem = montar(s, texto, visoes, usuario)
    if not montagem.get("da_para"):
        return {"respondeu": False, "tipo": "fora_do_mapa",
                "recado": (montagem.get("por_que_nao")
                           or "Isso o sistema ainda não guarda."),
                "entendi": montagem.get("entendi", "")}

    try:
        saida = svc_consulta.rodar(s, montagem.get("sql", ""), visoes)
    except svc_consulta.ConsultaRecusada as e:
        return {"respondeu": False, "tipo": "consulta_recusada",
                "recado": str(e), "entendi": montagem.get("entendi", "")}

    return {
        "respondeu": True,
        "montada_na_hora": True,
        "linhas": saida["linhas"],
        "cortou": saida["cortou"],
        # O CAMINHO, em português — ver o cabeçalho deste arquivo.
        "caminho": {
            "entendi": montagem.get("entendi", ""),
            "onde_olhei": montagem.get("onde_olhei", ""),
            "filtrei": montagem.get("filtrei", ""),
        },
        "sql": saida["sql"],
    }
