# -*- coding: utf-8 -*-
"""
A FOLHA ANALÍTICA, GUARDADA (migração 043) — ver `folha_analitica.py`.

Importar: lê o arquivo, ACHA A FOLHA SINTÉTICA a que ele pertence e guarda o
detalhamento de cada pessoa ligado à competência e ao tipo dessa folha.

⚠️ A FOLHA CERTA SE ACHA PELO DINHEIRO, não pelo nome do arquivo. No mesmo mês
há duas folhas (quinzena e fim de mês), e o título da analítica não diz qual é.
Então compara-se o líquido de cada pessoa com o de cada folha importada do mês:
a que bate é a dela. Nenhuma batendo, a importação RECUSA, dizendo para importar
a sintética antes — guardar o detalhamento na folha errada mostraria a conta de
um pagamento explicando outro.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal

from . import folha_analitica as fan

logger = logging.getLogger("analisesps.folha")

# Quanto do arquivo precisa bater com uma folha para ser dela. Não é 100% porque
# a sintética pode ter sido corrigida e reimportada (uma pessoa a mais, um valor
# acertado) — e não é pouco, para não casar quinzena com fim de mês por acaso.
PARCELA_QUE_PRECISA_BATER = Decimal("0.80")


class ErroDaAnalitica(RuntimeError):
    """A frase vai inteira para a tela."""


def _pronto() -> bool:
    from .db import tem_coluna
    return tem_coluna("folha_analitica_pessoa", "eventos")


def _folha_que_bate(lida: fan.FolhaAnalitica) -> tuple:
    """(folha, resumo da conferência) — a folha importada do mês que bate."""
    from . import folha_arquivo
    candidatas = [f for f in folha_arquivo.listar(teto=60)
                  if f.get("ano") == lida.ano and f.get("mes") == lida.mes]
    com_valor = [p for p in lida.pessoas if p.liquido is not None]
    melhor, resumo_melhor = None, None
    for f in candidatas:
        aberta = folha_arquivo.abrir(f["id"])
        if not aberta:
            continue
        resumo = fan.conferir_com_a_sintetica(lida, aberta["linhas"])
        if resumo_melhor is None or resumo["batem"] > resumo_melhor["batem"]:
            melhor, resumo_melhor = aberta, resumo
    if not melhor or not com_valor or \
            Decimal(resumo_melhor["batem"]) < PARCELA_QUE_PRECISA_BATER * len(com_valor):
        return None, resumo_melhor
    return melhor, resumo_melhor


def importar(conteudo: bytes, nome_do_arquivo: str = "", quem: str = "") -> dict:
    """Guarda a analítica junto da sintética que ela explica. Devolve o resumo."""
    from .db import conexao
    if not _pronto():
        raise ErroDaAnalitica(
            'Atualização do banco da folha analítica não aplicada. '
            'Clique em "Aplicar atualizações do banco" em Configurações.')
    try:
        lida = fan.ler(conteudo)
    except fan.ErroDaFolha as e:
        raise ErroDaAnalitica(str(e)) from e

    folha, resumo = _folha_que_bate(lida)
    if folha is None:
        raise ErroDaAnalitica(
            f"Folha analítica de {lida.competencia} sem folha sintética correspondente "
            "importada nesta competência. Importe primeiro a folha sintética "
            "(ela define se é quinzena ou fim de mês) e, em seguida, a analítica.")

    with conexao() as conn:
        conn.execute("DELETE FROM analisesps.folha_analitica "
                     " WHERE ano = ? AND mes = ? AND tipo = ?",
                     (folha["ano"], folha["mes"], folha["tipo"]))
        cur = conn.execute(
            "INSERT INTO analisesps.folha_analitica "
            "  (ano, mes, tipo, nome_arquivo, pessoas, batem, importado_por) "
            " VALUES (?,?,?,?,?,?,?) RETURNING id",
            (folha["ano"], folha["mes"], folha["tipo"],
             str(nome_do_arquivo or "")[:300], len(lida.pessoas),
             int(resumo["batem"]), str(quem or "")[:120]))
        novo = cur.fetchone()[0]
        cur.close()
        conn.executemany(
            "INSERT INTO analisesps.folha_analitica_pessoa "
            "  (analitica_id, id_fortes, nome, cargo, filial, setor, "
            "   total_proventos, total_descontos, liquido, fgts, admissao, "
            "   dependentes, filhos, horas_mes, salario_contribuicao, base_inss, "
            "   base_fgts, situacao, eventos) "
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(novo, p.id_fortes, p.nome[:160], p.cargo[:160], p.filial[:160],
              p.setor[:160], p.total_proventos, p.total_descontos, p.liquido,
              p.fgts, p.admissao[:20], p.dependentes[:10], p.filhos[:10],
              p.horas_mes[:10], p.salario_contribuicao, p.base_inss, p.base_fgts,
              p.situacao[:500],
              json.dumps([{"codigo": e.codigo, "descricao": e.descricao,
                           "referencia": e.referencia,
                           "provento": str(e.provento), "desconto": str(e.desconto)}
                          for e in p.eventos], ensure_ascii=False))
             for p in lida.pessoas])
        conn.commit()
    logger.info("Folha: analítica de %s (%s) importada por %s — %d pessoa(s), "
                "%d batem com a sintética.", lida.competencia, folha["tipo"],
                quem or "(sem nome)", len(lida.pessoas), resumo["batem"])
    return {"folha_id": folha["id"], "competencia": lida.competencia,
            "tipo": folha["tipo"], "pessoas": len(lida.pessoas),
            "batem": resumo["batem"], "diferentes": len(resumo["diferentes"]),
            "so_na_analitica": len(resumo["so_na_analitica"])}


def da_pessoa(ano: int, mes: int, tipo: str, id_fortes: str) -> dict | None:
    """O contracheque de uma pessoa nesta folha, ou None sem analítica."""
    from .db import consultar_um
    if not _pronto() or not id_fortes:
        return None
    l = consultar_um(
        "SELECT p.nome, p.cargo, p.filial, p.setor, p.total_proventos, "
        "       p.total_descontos, p.liquido, p.fgts, p.admissao, p.dependentes, "
        "       p.filhos, p.horas_mes, p.salario_contribuicao, p.base_inss, "
        "       p.base_fgts, p.situacao, p.eventos, a.importado_em, a.nome_arquivo "
        "  FROM analisesps.folha_analitica a "
        "  JOIN analisesps.folha_analitica_pessoa p ON p.analitica_id = a.id "
        " WHERE a.ano = ? AND a.mes = ? AND a.tipo = ? AND p.id_fortes = ? "
        " LIMIT 1", (int(ano), int(mes), str(tipo), str(id_fortes).zfill(6)))
    if not l:
        return None
    try:
        eventos = json.loads(l[16] or "[]")
    except ValueError:
        eventos = []
    for e in eventos:
        e["provento"] = Decimal(str(e.get("provento") or "0"))
        e["desconto"] = Decimal(str(e.get("desconto") or "0"))
    prov = sum((e["provento"] for e in eventos), Decimal("0.00"))
    desc = sum((e["desconto"] for e in eventos), Decimal("0.00"))
    liquido = l[6]
    return {"nome": l[0], "cargo": l[1], "filial": l[2], "setor": l[3],
            "total_proventos": l[4], "total_descontos": l[5], "liquido": liquido,
            "fgts": l[7], "admissao": l[8], "dependentes": l[9], "filhos": l[10],
            "horas_mes": l[11], "salario_contribuicao": l[12], "base_inss": l[13],
            "base_fgts": l[14], "situacao": l[15], "eventos": eventos,
            "soma_proventos": prov, "soma_descontos": desc,
            "fecha": liquido is not None and prov - desc == Decimal(str(liquido)),
            "importado_em": l[17], "nome_arquivo": l[18]}


def da_folha(ano: int, mes: int, tipo: str) -> dict | None:
    """Se a folha tem analítica importada, e quando. Para a lateral."""
    from .db import consultar_um
    if not _pronto():
        return None
    l = consultar_um(
        "SELECT importado_em, importado_por, pessoas, batem, nome_arquivo "
        "  FROM analisesps.folha_analitica WHERE ano = ? AND mes = ? AND tipo = ?",
        (int(ano), int(mes), str(tipo)))
    if not l:
        return None
    return {"importado_em": l[0], "importado_por": l[1], "pessoas": l[2],
            "batem": l[3], "nome_arquivo": l[4]}
