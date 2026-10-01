# -*- coding: utf-8 -*-
"""
A FILA DO PONTO POR PESSOA — 01/10/2026 (migração 042).

O dono: *"se eu clicar pra ajeitar o ponto de uma pessoa, posso sair do analítico
dela e ir resolver de outro enquanto o sistema trabalha em segundo plano?"* Podia
sair, mas não podia PEDIR a próxima: era uma pessoa por vez, e o segundo pedido era
recusado. *"Sim, faça uma fila."*

Como funciona:

  - cada pedido — "atualizar o ponto desta pessoa" (`pessoa`) ou "lançar batidas"
    (`lancar`) — vira uma linha em `ponto_fila`, e a tela devolve na hora "na
    fila, N antes deste";
  - UM trabalhador resolve a fila em ordem: a tarefa `ponto_pessoa`, na pista da
    pessoa (migração 041), que roda em paralelo à carga do mês. Ele pega o
    próximo, resolve, escreve o resultado na linha e pega o seguinte, até a fila
    esvaziar;
  - o andamento e o resultado ficam NA LINHA. Quem sai da tela e volta vê o que
    aconteceu — inclusive o motivo de uma falha.

⚠️ O TRABALHADOR PODE FALTAR, e a fila não pode ficar parada por isso. Dois
casos: (1) o pedido entra no instante em que o trabalhador acabou de achar a fila
vazia e está encerrando — o disparo é recusado (pista ocupada) e ninguém pega o
pedido; (2) uma publicação mata o trabalhador no meio de um pedido, que fica
"rodando" para sempre. `cutucar` resolve os dois: chamado a cada consulta de
andamento (a tela pergunta a cada poucos segundos), ele devolve à fila o que ficou
"rodando" sem trabalhador e dispara um trabalhador se há pedido esperando.

⚠️ DEVOLVER À FILA UM LANÇAMENTO INTERROMPIDO É SEGURO, e não por acaso: o
`ponto_edicao.lancar` traz o ponto da pessoa de novo ANTES de mandar e refaz o
plano com ele — o que já entrou no Mobponto na primeira tentativa é visto e não é
mandado de novo.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger("analisesps.ponto")

PESSOA = "pessoa"
LANCAR = "lancar"
TIPOS = (PESSOA, LANCAR)
ROTULO_DO_TIPO = {PESSOA: "Atualização do ponto", LANCAR: "Lançamento de batidas"}

ESPERANDO, RODANDO, FEITO, FALHOU = "esperando", "rodando", "feito", "falhou"


def _pronto() -> bool:
    from .db import tem_coluna
    return tem_coluna("ponto_fila", "situacao")


def _linha(l) -> dict:
    return {"id": l[0], "tipo": l[1], "ano": l[2], "mes": l[3], "cpf": l[4],
            "nome": l[5], "pedido": l[6], "situacao": l[7], "progresso": l[8],
            "mensagem": l[9], "pedido_por": l[10], "criado_em": l[11],
            "inicio": l[12], "fim": l[13],
            "rotulo": ROTULO_DO_TIPO.get(l[1], l[1])}


_CAMPOS = ("id, tipo, ano, mes, cpf, nome, pedido, situacao, progresso, mensagem, "
           "pedido_por, criado_em, inicio, fim")


def enfileirar(tipo: str, ano: int, mes: int, cpf: str, nome: str = "",
               pedido: dict | None = None, quem: str = "") -> dict:
    """Põe o pedido na fila. Devolve `{id, posicao, repetido}`.

    O MESMO PEDIDO DUAS VEZES NÃO ENTRA DUAS VEZES: quem aperta "atualizar" de novo
    para a mesma pessoa, com o pedido dela ainda esperando, recebe o que já está na
    fila. Um lançamento repetido só é igual se o pedido for idêntico."""
    from .db import conexao
    if tipo not in TIPOS:
        raise ValueError(f"tipo de pedido desconhecido: {tipo}")
    texto = json.dumps(pedido or {}, ensure_ascii=False, sort_keys=True)
    with conexao() as conn:
        cur = conn.execute(
            "SELECT id FROM analisesps.ponto_fila "
            " WHERE situacao = ? AND tipo = ? AND cpf = ? AND ano = ? AND mes = ? "
            "   AND pedido = ? ORDER BY id LIMIT 1",
            (ESPERANDO, tipo, cpf, int(ano), int(mes), texto))
        ja = cur.fetchone()
        cur.close()
        if ja:
            return {"id": ja[0], "posicao": posicao(ja[0]), "repetido": True}
        cur = conn.execute(
            "INSERT INTO analisesps.ponto_fila "
            "  (tipo, ano, mes, cpf, nome, pedido, pedido_por) "
            " VALUES (?,?,?,?,?,?,?) RETURNING id",
            (tipo, int(ano), int(mes), cpf, str(nome or "")[:160], texto,
             str(quem or "")[:120]))
        novo = cur.fetchone()[0]
        cur.close()
        conn.commit()
    return {"id": novo, "posicao": posicao(novo), "repetido": False}


def posicao(item_id: int) -> int:
    """Quantos pedidos estão na frente deste (esperando ou rodando)."""
    from .db import consultar_um
    linha = consultar_um(
        "SELECT count(*) FROM analisesps.ponto_fila "
        " WHERE situacao IN (?, ?) AND id < ?", (ESPERANDO, RODANDO, int(item_id)))
    return int((linha or [0])[0] or 0)


def item(item_id: int) -> dict | None:
    from .db import consultar_um
    l = consultar_um(f"SELECT {_CAMPOS} FROM analisesps.ponto_fila WHERE id = ?",
                     (int(item_id),))
    if not l:
        return None
    saida = _linha(l)
    saida["posicao"] = posicao(saida["id"]) if saida["situacao"] == ESPERANDO else 0
    return saida


def recentes(horas: int = 12, teto: int = 30) -> list:
    """Os pedidos das últimas horas, o mais novo primeiro — para a lateral."""
    from .db import consultar
    if not _pronto():
        return []
    linhas = consultar(
        f"SELECT {_CAMPOS} FROM analisesps.ponto_fila "
        " WHERE criado_em > now() - make_interval(hours => ?) "
        "    OR situacao IN (?, ?) "
        " ORDER BY id DESC LIMIT ?", (int(horas), ESPERANDO, RODANDO, int(teto)))
    return [_linha(l) for l in linhas]


def ultimo_da_pessoa(cpf: str, horas: int = 24) -> dict | None:
    """O pedido mais recente desta pessoa, para o analítico mostrar ao abrir."""
    from .db import consultar_um
    if not _pronto():
        return None
    l = consultar_um(
        f"SELECT {_CAMPOS} FROM analisesps.ponto_fila "
        " WHERE cpf = ? AND (criado_em > now() - make_interval(hours => ?) "
        "                    OR situacao IN (?, ?)) "
        " ORDER BY id DESC LIMIT 1", (cpf, int(horas), ESPERANDO, RODANDO))
    if not l:
        return None
    saida = _linha(l)
    saida["posicao"] = posicao(saida["id"]) if saida["situacao"] == ESPERANDO else 0
    return saida


def _pegar_o_proximo() -> dict | None:
    """Marca o próximo da fila como "rodando" e o devolve. None quando vazia."""
    from .db import conexao
    with conexao() as conn:
        cur = conn.execute(
            "UPDATE analisesps.ponto_fila SET situacao = ?, inicio = now(), "
            "       progresso = 'iniciando' "
            " WHERE id = (SELECT id FROM analisesps.ponto_fila "
            "              WHERE situacao = ? ORDER BY id LIMIT 1) "
            f"RETURNING {_CAMPOS}", (RODANDO, ESPERANDO))
        l = cur.fetchone()
        cur.close()
        conn.commit()
    return _linha(l) if l else None


def _anotar_no_item(item_id: int, progresso: str) -> None:
    from .db import conexao
    try:
        with conexao() as conn:
            conn.execute("UPDATE analisesps.ponto_fila SET progresso = ? WHERE id = ?",
                         (str(progresso or "")[:300], int(item_id)))
            conn.commit()
    except Exception:  # noqa: BLE001 — perder o andamento não pode parar o pedido
        logger.exception("Ponto: não consegui anotar o andamento do pedido")


def _terminar(item_id: int, ok: bool, mensagem: str) -> None:
    from .db import conexao
    with conexao() as conn:
        conn.execute(
            "UPDATE analisesps.ponto_fila SET situacao = ?, mensagem = ?, "
            "       progresso = '', fim = now() WHERE id = ?",
            (FEITO if ok else FALHOU, str(mensagem or "")[:2000], int(item_id)))
        conn.commit()


def _resolver(pedido_da_fila: dict, anotar) -> tuple:
    """Faz UM pedido. Devolve (ok, mensagem)."""
    from . import ponto, ponto_edicao

    if pedido_da_fila["tipo"] == PESSOA:
        r = ponto.atualizar_pessoa(pedido_da_fila["ano"], pedido_da_fila["mes"],
                                   pedido_da_fila["cpf"], pedido_da_fila["nome"],
                                   anotar)
        if not r.get("achou"):
            return False, (
                "colaborador não encontrado no ponto do Mobponto de "
                f"{int(pedido_da_fila['mes']):02d}/{pedido_da_fila['ano']} — páginas "
                f"consultadas: {', '.join(str(x) for x in r.get('olhadas') or [])}. Se houve "
                "registro de ponto na competência, importe novamente o mês completo.")
        return True, (f"{r.get('dias', 0)} dia(s) importado(s) do Mobponto "
                      f"(página {r.get('pagina')}).")

    pedido = json.loads(pedido_da_fila["pedido"] or "{}")
    pedido.setdefault("quem", pedido_da_fila.get("pedido_por") or "")
    feito = ponto_edicao.lancar(pedido, anotar)
    return (not feito["falhou"]), ponto_edicao.recado_do_lancamento(feito)


def processar(anotar=None) -> dict:
    """O trabalhador: resolve a fila inteira, um pedido por vez, em ordem.

    Um pedido que falha NÃO para a fila — a falha fica escrita nele, e o próximo
    segue. Quem esperava pelo segundo não tem nada a ver com o primeiro."""
    anotar = anotar or (lambda *a, **k: None)
    feitos, falhas = 0, 0
    while True:
        atual = _pegar_o_proximo()
        if not atual:
            break
        nome = atual["nome"] or atual["cpf"]

        def anotar_aqui(etapa, progresso="", _id=atual["id"], _nome=nome):
            texto = progresso or etapa
            _anotar_no_item(_id, texto)
            anotar(etapa, f"{_nome}: {texto}")

        anotar_aqui(f"{ROTULO_DO_TIPO.get(atual['tipo'])} — {nome}", "iniciando")
        try:
            ok, mensagem = _resolver(atual, anotar_aqui)
        except Exception as e:  # noqa: BLE001 — a falha fica no pedido
            logger.exception("Ponto: falhou o pedido %s da fila", atual["id"])
            ok, mensagem = False, str(e)
        _terminar(atual["id"], ok, mensagem)
        if ok:
            feitos += 1
        else:
            falhas += 1
    return {"feitos": feitos, "falhas": falhas}


def cutucar() -> None:
    """Garante que a fila anda. Ver o cabeçalho: o trabalhador pode faltar.

    Barato quando não há nada a fazer — uma consulta. Nunca levanta: é chamado
    de dentro de consultas de andamento da tela."""
    from . import tarefas
    from .db import conexao, consultar_um
    try:
        if not _pronto():
            return
        pendente = consultar_um(
            "SELECT count(*) FROM analisesps.ponto_fila WHERE situacao IN (?, ?)",
            (ESPERANDO, RODANDO))
        if not pendente or not pendente[0]:
            return
        if tarefas.estado("pessoa").get("rodando"):
            return
        # Ninguém trabalhando e algo "rodando": o trabalhador morreu no meio.
        with conexao() as conn:
            conn.execute(
                "UPDATE analisesps.ponto_fila SET situacao = ?, "
                "       progresso = 'retomando: o serviço foi reiniciado durante a execução' "
                " WHERE situacao = ?", (ESPERANDO, RODANDO))
            conn.commit()
        tarefas.disparar("ponto_pessoa", disparo="fila do ponto")
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: não consegui cutucar a fila")
