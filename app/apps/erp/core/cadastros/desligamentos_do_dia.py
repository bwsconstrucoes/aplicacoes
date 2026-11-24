# -*- coding: utf-8 -*-
"""
A VIRADA DO DIA DOS DESLIGAMENTOS — no dia da saída, tudo cai sozinho.

⚑ DECISÃO DELE, 17/11/2026, respondendo à pergunta de quem deve cortar o
acesso no dia da saída:

> *"Não, o ideal é cortar tudo, porque aí corre o risco de a gente esquecer.
> Então, se ela precisar de um contracheque, alguma coisa, ela solicita ao DP e
> o DP manda."*

O problema que isto resolve: desde o conserto do aviso prévio (mesma data), uma
saída marcada para o futuro **não desliga na hora** — e estava certo, porque a
pessoa trabalha até o último dia. Mas aí ficava a outra metade: **nada desligava
quando o dia chegava.** O ponto se resolvia sozinho (ele compara a data com
hoje), e o resto não: o login no ERP e no portal continuava valendo, as
mensagens da empresa continuavam indo, e as listas do Pessoal mostravam a
pessoa como ativa.

**O que esta rotina faz, uma vez por dia:** quem tem data de saída ANTERIOR a
hoje e ainda está marcado como ativo passa a DESLIGADO, a etapa vai para
"Desligado" e o login da pessoa é desativado. Com isso TODO leitor acerta —
inclusive os que ninguém lembrou de conferir —, porque o dado gravado volta a
dizer a verdade em vez de cada tela ter de recalcular.

⚠️ **ISTO NÃO É O AGENTE DE DP DESLIGANDO NINGUÉM.** A regra nº 1 do agente
(`AGENTE_DP.md` §5) é "não desliga". Aqui não há decisão nenhuma: **uma pessoa
do DP já registrou o desligamento, com data**. A rotina só cumpre, no dia, o
que foi autorizado antes — do mesmo jeito que um boleto agendado não é o banco
decidindo pagar.

**Quando roda:** na primeira requisição do ERP de cada dia (o mesmo desenho da
rotina do dia do ponto). Não há cron neste serviço, e não ter dependência
externa é uma vantagem: nada para configurar, nada para quebrar em silêncio.
Depois da primeira vez do dia, é a comparação de uma variável.

⚠️ **O QUE ELA NÃO FAZ, de propósito:** não mexe em quem está com a etapa
"Cancelado" (cadastro que nunca virou gente na empresa) e **nunca** desliga por
"último dia trabalhado" ou "aviso prévio" — esses dois são informação para a
rescisão. Quem decide é a DATA DE SAÍDA, e só ela.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PARAMETRO = "desligamentos.virada_em"

# Marca em memória: evita até a consulta ao parâmetro no resto do dia. O
# processo reinicia a cada 5.000 requisições (`Procfile`), e aí o parâmetro no
# banco é que impede a repetição.
_ultima: dict[str, Optional[str]] = {"dia": None}


def esquecer() -> None:
    """Só para teste: faz a próxima chamada rodar de novo."""
    _ultima["dia"] = None


def _ja_rodou_hoje(s: Session, hoje: date) -> bool:
    linha = s.execute(text("SELECT valor FROM parametros WHERE chave = :c"),
                      {"c": PARAMETRO}).first()
    return bool(linha) and str(linha[0]) == hoje.isoformat()


def _marcar(s: Session, hoje: date) -> None:
    s.execute(text("""
        INSERT INTO parametros (chave, valor) VALUES (:c, :v)
        ON CONFLICT (chave) DO UPDATE SET valor = :v, atualizado_em = now()"""),
        {"c": PARAMETRO, "v": hoje.isoformat()})


def aplicar(s: Session, *, hoje: Optional[date] = None) -> dict[str, Any]:
    """Cumpre os desligamentos cuja data já passou. Devolve o que mexeu."""
    hoje = hoje or date.today()
    pendentes = s.execute(text("""
        SELECT c.id, c.nome, c.cpf, c.demissao, c.etapa, c.usuario_id
          FROM colaboradores c
         WHERE c.demissao IS NOT NULL
           AND c.demissao < :hoje
           AND UPPER(COALESCE(c.situacao, 'ATIVO')) <> 'DESLIGADO'
           AND UPPER(COALESCE(c.etapa, 'CONCLUIDO')) <> 'CANCELADO'
         ORDER BY c.demissao, c.id"""), {"hoje": hoje}).mappings().all()
    # ⚑ QUEM TEM CANCELAMENTO DE DESLIGAMENTO PEDIDO NÃO CAI (20/11/2026).
    # Ele: *"Imagina desligar uma pessoa que vai continuar trabalhando. É uma
    # falha grave, né? Não pode deixar isso acontecer."* Se a obra pediu o
    # cancelamento e o DP ainda não atendeu, a data de saída continua gravada
    # — e sem isto a virada desligaria a pessoa e cortaria o acesso dela no
    # dia. Fica SEGURADA até o DP decidir; o agente cobra o DP todo dia.
    segurados = set(s.execute(text("""
        SELECT colaborador_id FROM dp_solicitacoes
         WHERE tipo = 'CANCELAR_DESLIGAMENTO'
           AND situacao IN ('ABERTA', 'EM_ANALISE')
           AND colaborador_id IS NOT NULL""")).scalars().all())
    if segurados:
        logger.warning("ERP/desligamentos: %s pessoa(s) com data de saída "
                       "vencida SEGURADA(S) por cancelamento pedido",
                       sum(1 for p in pendentes if p["id"] in segurados))
    pendentes = [p for p in pendentes if p["id"] not in segurados]
    if not pendentes:
        return {"pessoas": 0, "acessos": 0, "quem": []}

    from app.apps.erp.core.comum.auditoria import registrar_evento

    acessos = 0
    quem = []
    for p in pendentes:
        s.execute(text("""
            UPDATE colaboradores SET situacao = 'DESLIGADO', etapa = 'DESLIGADO'
             WHERE id = :i"""), {"i": p["id"]})
        if (p["etapa"] or "").upper() != "DESLIGADO":
            s.execute(text("""
                INSERT INTO colaborador_etapas (colaborador_id, de, para, motivo,
                                                automatico, usuario_id)
                VALUES (:c, :de, 'DESLIGADO', :m, TRUE, NULL)"""),
                {"c": p["id"], "de": p["etapa"],
                 "m": f"chegou a data de saída ({p['demissao']:%d/%m/%Y})"})
        # O LOGIN CAI JUNTO — decisão dele: *"o ideal é cortar tudo, porque aí
        # corre o risco de a gente esquecer."* Reativar é um clique do DP, no
        # cadastro da pessoa.
        if p["usuario_id"]:
            mexeu = s.execute(text("""
                UPDATE usuarios SET ativo = FALSE
                 WHERE id = :u AND ativo = TRUE"""), {"u": p["usuario_id"]})
            if mexeu.rowcount:
                acessos += 1
                registrar_evento(s, "usuario", int(p["usuario_id"]),
                                 "ACESSO_BLOQUEADO",
                                 {"motivo": "chegou a data de saída",
                                  "colaborador_id": int(p["id"])}, None)
        registrar_evento(s, "colaborador", int(p["id"]), "DESLIGADO",
                         {"data_saida": str(p["demissao"]),
                          "por": "a virada do dia: a data de saída chegou",
                          "etapa_antes": p["etapa"],
                          "acesso_bloqueado": bool(p["usuario_id"])}, None)
        quem.append({"id": int(p["id"]), "nome": p["nome"],
                     "data_saida": str(p["demissao"])})

    logger.info("ERP/desligamentos: %s pessoa(s) desligada(s) pela data de "
                "saída; %s acesso(s) bloqueado(s)", len(quem), acessos)
    return {"pessoas": len(quem), "acessos": acessos, "quem": quem}


def precisa_rodar() -> bool:
    """A pergunta que TODA requisição do ERP faz. É só uma variável em
    memória: sem isto, o gancho abriria uma sessão de banco por requisição
    para descobrir que não tem nada a fazer."""
    return _ultima["dia"] != date.today().isoformat()


def aplicar_se_preciso(s: Session, *, hoje: Optional[date] = None) -> dict[str, Any]:
    """Uma vez por dia, na primeira requisição do ERP. Nunca levanta erro:
    rotina de fundo não pode derrubar a tela de ninguém.

    Recebe a sessão de quem chama (o gancho do `routes.py`) em vez de abrir a
    sua: é o padrão do ERP, e é o que deixa isto testável sem escrever fora da
    transação do teste.
    """
    hoje = hoje or date.today()
    if _ultima["dia"] == hoje.isoformat():
        return {"pulou": "já rodou neste processo hoje"}
    try:
        if _ja_rodou_hoje(s, hoje):
            _ultima["dia"] = hoje.isoformat()
            return {"pulou": "já rodou hoje"}
        feito = aplicar(s, hoje=hoje)
        _marcar(s, hoje)
        s.commit()
        _ultima["dia"] = hoje.isoformat()
        return feito
    except Exception:                               # noqa: BLE001
        logger.warning("ERP/desligamentos: a virada do dia não rodou",
                       exc_info=True)
        try:
            s.rollback()
        except Exception:                           # noqa: BLE001
            pass
        return {"erro": True}
