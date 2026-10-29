# -*- coding: utf-8 -*-
"""
OS TIPOS DE AUSÊNCIA JUSTIFICADA, NUMA TELA SÓ (29/10/2026, migração 017).

O dono: *"a gente tem a parte de licenças e afastamentos, que são por lei, e tem as demais (…) mas
não tem nenhum canto no sistema que a gente possa visualizar e até cadastrar novas (…) e mesmo
excluir alguma. E a gente tem que poder configurar se abona ou se não abona horas."*

Dois grupos, na Configuração › Licenças e abonos:
- **as licenças da lei** (`ponto.tipos_licenca`, `licencas.py`): o pedido de LICENÇA escolhe uma delas;
- **os abonos da empresa** (`ponto.tipos_abono`, aqui): viagem a serviço, dispensa, e o que o DP
  cadastrar — o pedido de ABONO escolhe um deles.
Cada tipo diz se **abona as horas**. O que não abona deixa o dia justificado (aparece no espelho com o
nome), mas as horas são descontadas — como a declaração de comparecimento com "nada".

EXCLUIR: o tipo que nunca foi usado sai de vez; o que já foi usado em algum pedido é só **desligado**
(some das listas de escolha, mas os pedidos antigos continuam com o nome certo).
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao, NaoEncontrado


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "tipos_abono", "codigo")


def _codigo(texto) -> str:
    return "".join(ch for ch in str(texto or "").upper() if ch.isalnum() or ch == "_")[:40]


# ---------------------------------------------------------------------------
# Os abonos da empresa
# ---------------------------------------------------------------------------
def listar_abonos(conn: Connection, *, so_ativos: bool = True) -> list[dict]:
    from .espelho import SUBTIPOS_ABONO
    if not disponivel(conn):         # antes da 017: os dois de sempre, abonando
        return [{"codigo": c, "nome": n, "abona": True, "exige_documento": False, "ativo": True, "usado": True}
                for c, n in SUBTIPOS_ABONO.items()]
    usados = {r["subtipo"] for r in db.todos(conn, "SELECT DISTINCT subtipo FROM ponto.ocorrencias "
                                                   "WHERE tipo = 'ABONO' AND subtipo IS NOT NULL")}
    sql = "SELECT * FROM ponto.tipos_abono" + (" WHERE ativo" if so_ativos else "") + " ORDER BY ordem, nome"
    return [{"codigo": a["codigo"], "nome": a["nome"], "abona": bool(a["abona"]),
             "exige_documento": bool(a["exige_documento"]), "ativo": bool(a["ativo"]),
             "usado": a["codigo"] in usados} for a in db.todos(conn, sql)]


def gravar_abono(conn: Connection, codigo: str, dados: dict, por: str) -> dict:
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 017) antes")
    codigo = _codigo(codigo or dados.get("nome"))
    if not codigo:
        raise ErroDeValidacao("diga o nome do abono", campo="nome")
    nome = " ".join(str(dados.get("nome") or "").split())
    existe = db.um(conn, "SELECT 1 AS x FROM ponto.tipos_abono WHERE codigo = :c", c=codigo)
    if not existe and not nome:
        raise ErroDeValidacao("diga o nome do abono", campo="nome")
    db.executar(conn, """
        INSERT INTO ponto.tipos_abono (codigo, nome, abona, exige_documento, ativo, atualizado_por)
        VALUES (:c, :n, :a, :e, :at, :p)
        ON CONFLICT (codigo) DO UPDATE SET nome = COALESCE(NULLIF(:n, ''), ponto.tipos_abono.nome),
            abona = :a, exige_documento = :e, ativo = :at, atualizado_por = :p, atualizado_em = now()
    """, c=codigo, n=nome[:120], a=bool(dados.get("abona", True)), e=bool(dados.get("exige_documento")),
         at=bool(dados.get("ativo", True)), p=por[:120])
    recarregar(conn)
    return next(a for a in listar_abonos(conn, so_ativos=False) if a["codigo"] == codigo)


# ---------------------------------------------------------------------------
# Excluir (os dois grupos)
# ---------------------------------------------------------------------------
TABELA = {"LICENCA": "tipos_licenca", "ABONO": "tipos_abono"}


def excluir(conn: Connection, grupo: str, codigo: str, por: str) -> str:
    """Devolve "EXCLUIDO" (nunca usado: saiu de vez) ou "DESLIGADO" (já usado: só sai das listas)."""
    grupo = str(grupo or "").upper()
    if grupo not in TABELA:
        raise ErroDeValidacao("grupo desconhecido", campo="grupo")
    if grupo == "ABONO" and not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 017) antes")
    codigo = _codigo(codigo)
    if not db.um(conn, f"SELECT 1 AS x FROM ponto.{TABELA[grupo]} WHERE codigo = :c", c=codigo):
        raise NaoEncontrado("tipo não encontrado")
    usado = db.um(conn, "SELECT 1 AS x FROM ponto.ocorrencias WHERE tipo = :t AND subtipo = :c LIMIT 1",
                  t=grupo, c=codigo)
    if usado:
        db.executar(conn, f"UPDATE ponto.{TABELA[grupo]} SET ativo = FALSE, atualizado_por = :p, "
                          "atualizado_em = now() WHERE codigo = :c", c=codigo, p=por[:120])
        resultado = "DESLIGADO"
    else:
        db.executar(conn, f"DELETE FROM ponto.{TABELA[grupo]} WHERE codigo = :c", c=codigo)
        resultado = "EXCLUIDO"
    recarregar(conn)
    return resultado


# ---------------------------------------------------------------------------
# Para o espelho e o pedido
# ---------------------------------------------------------------------------
def nao_abonam(conn: Connection) -> set[tuple[str, str]]:
    """Os (tipo, subtipo) aprovados que NÃO abonam as horas: o dia fica justificado, mas é descontado."""
    saida: set[tuple[str, str]] = set()
    if db.tem_coluna(conn, "tipos_licenca", "abona"):
        saida |= {("LICENCA", r["codigo"]) for r in db.todos(conn, "SELECT codigo FROM ponto.tipos_licenca "
                                                                    "WHERE NOT abona")}
    if disponivel(conn):
        saida |= {("ABONO", r["codigo"]) for r in db.todos(conn, "SELECT codigo FROM ponto.tipos_abono WHERE NOT abona")}
    return saida


def recarregar(conn: Connection) -> None:
    """Os nomes dos abonos (o dicionário `espelho.SUBTIPOS_ABONO`, mudado NO LUGAR — quem já o importou
    vê a lista nova) e os nomes das licenças guardados pelos pedidos."""
    from . import ocorrencias
    from .espelho import SUBTIPOS_ABONO
    if disponivel(conn):
        nomes = {r["codigo"]: r["nome"] for r in db.todos(conn, "SELECT codigo, nome FROM ponto.tipos_abono")}
        SUBTIPOS_ABONO.clear()
        SUBTIPOS_ABONO.update(nomes)
    ocorrencias._NOME_SUBTIPO.clear()


def abono_valido(conn: Connection, codigo: Optional[str]) -> bool:
    """O abono escolhido no pedido existe e está em uso?"""
    codigo = _codigo(codigo)
    if not codigo:
        return False
    if not disponivel(conn):
        from .espelho import SUBTIPOS_ABONO
        return codigo in SUBTIPOS_ABONO
    return bool(db.um(conn, "SELECT 1 AS x FROM ponto.tipos_abono WHERE codigo = :c AND ativo", c=codigo))
