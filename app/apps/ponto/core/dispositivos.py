# -*- coding: utf-8 -*-
"""
Aparelhos: registrar, aprovar, bloquear, autorizar — e decidir se um aparelho
pode registrar a batida de uma pessoa numa obra.

O ciclo: o celular gera um `device_uuid` e se registra (entra PENDENTE, recebe
o token dele). Alguém com a chave de API aprova, dizendo o perfil e, conforme
o perfil, o dono (INDIVIDUAL) ou a lista (LISTA), e em quais obras o aparelho
vale (vazio = todas). Bloqueado, o aparelho não bate mais nada.

`autorizado_para` é função PURA: recebe o aparelho e a pessoa como dicionários
e devolve o motivo da recusa (ou None). É ela que o teste sem banco percorre.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from sqlalchemy.engine import Connection

from .. import auth, db
from ..erros import ErroDeValidacao, NaoEncontrado

logger = logging.getLogger("ponto.dispositivos")

PERFIS = ("COMPARTILHADO", "INDIVIDUAL", "LISTA")
STATUS = ("PENDENTE", "APROVADO", "BLOQUEADO")

# UUID v4 ou qualquer identificador estável de 16 a 64 caracteres seguros.
_UUID_OK = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{15,63}$")


def validar_uuid(device_uuid) -> str:
    valor = str(device_uuid or "").strip()
    if not _UUID_OK.match(valor):
        raise ErroDeValidacao("device_uuid inválido (16 a 64 caracteres, letras, "
                              "números, '-' e '_')", campo="device_uuid")
    return valor


def validar_perfil(perfil) -> str:
    valor = str(perfil or "").strip().upper()
    if valor not in PERFIS:
        raise ErroDeValidacao(f"perfil desconhecido: {perfil!r} (use {', '.join(PERFIS)})",
                              campo="perfil")
    return valor


# ---------------------------------------------------------------------------
# A regra, pura
# ---------------------------------------------------------------------------
def autorizado_para(dispositivo: dict, colaborador_id: int, obra_id: int,
                    autorizados: set[int], obras_do_aparelho: set[int]) -> Optional[str]:
    """None se o aparelho pode registrar esta pessoa nesta obra; senão o motivo."""
    if dispositivo.get("status") != "APROVADO":
        return f"aparelho {str(dispositivo.get('status', '')).lower() or 'desconhecido'}"
    perfil = dispositivo.get("perfil")
    if perfil == "INDIVIDUAL":
        if dispositivo.get("colaborador_id") != colaborador_id:
            return "aparelho individual de outra pessoa"
    elif perfil == "LISTA":
        if colaborador_id not in autorizados:
            return "pessoa fora da lista do aparelho"
    elif perfil != "COMPARTILHADO":
        return f"perfil de aparelho desconhecido: {perfil}"
    if obras_do_aparelho and obra_id not in obras_do_aparelho:
        return "aparelho não vale nesta obra"
    return None


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def por_uuid(conn: Connection, device_uuid: str) -> Optional[dict]:
    return db.um(conn, "SELECT * FROM ponto.dispositivos WHERE device_uuid = :u",
                 u=device_uuid)


def por_id(conn: Connection, dispositivo_id: int) -> dict:
    d = db.um(conn, "SELECT * FROM ponto.dispositivos WHERE id = :id", id=dispositivo_id)
    if not d:
        raise NaoEncontrado("aparelho não encontrado")
    return d


def autorizados_de(conn: Connection, dispositivo_id: int) -> set[int]:
    return {int(l["colaborador_id"]) for l in db.todos(
        conn, "SELECT colaborador_id FROM ponto.dispositivo_autorizados WHERE dispositivo_id = :d",
        d=dispositivo_id)}


def obras_de(conn: Connection, dispositivo_id: int) -> set[int]:
    return {int(l["obra_id"]) for l in db.todos(
        conn, "SELECT obra_id FROM ponto.dispositivo_obras WHERE dispositivo_id = :d",
        d=dispositivo_id)}


_SQL_DETALHADO = """
        SELECT d.*, c.nome AS dono_nome, c.cpf AS dono_cpf,
               (SELECT count(*) FROM ponto.dispositivo_autorizados a
                 WHERE a.dispositivo_id = d.id) AS qtd_autorizados,
               (SELECT string_agg(o.codigo, ', ' ORDER BY o.codigo)
                  FROM ponto.dispositivo_obras dob JOIN public.obras o ON o.id = dob.obra_id
                 WHERE dob.dispositivo_id = d.id) AS obras
          FROM ponto.dispositivos d
          LEFT JOIN public.colaboradores c ON c.id = d.colaborador_id
"""


def detalhado(conn: Connection, dispositivo_id: int) -> dict:
    """O aparelho com o nome do dono, a contagem da lista e as obras."""
    d = db.um(conn, _SQL_DETALHADO + " WHERE d.id = :id", id=dispositivo_id)
    if not d:
        raise NaoEncontrado("aparelho não encontrado")
    return d


def listar(conn: Connection, status: str | None = None) -> list[dict]:
    sql = _SQL_DETALHADO
    params: dict = {}
    if status:
        valor = str(status).strip().upper()
        if valor not in STATUS:
            raise ErroDeValidacao(f"status desconhecido: {status!r}", campo="status")
        sql += " WHERE d.status = :st"
        params["st"] = valor
    sql += " ORDER BY d.status, d.id DESC"
    return db.todos(conn, sql, **params)


def para_json(d: dict) -> dict:
    from ..horario import texto
    return {
        "id": d["id"], "device_uuid": d["device_uuid"], "descricao": d.get("descricao", ""),
        "perfil": d["perfil"], "status": d["status"],
        "dono": ({"id": d["colaborador_id"], "nome": d.get("dono_nome"),
                  "cpf": d.get("dono_cpf")} if d.get("colaborador_id") else None),
        "qtd_autorizados": int(d.get("qtd_autorizados") or 0),
        "obras": d.get("obras"),
        "aprovado_por": d.get("aprovado_por"), "aprovado_em": texto(d.get("aprovado_em")),
        "bloqueado_em": texto(d.get("bloqueado_em")), "motivo_bloqueio": d.get("motivo_bloqueio"),
        "ultimo_uso_em": texto(d.get("ultimo_uso_em")), "criado_em": texto(d.get("criado_em")),
    }


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------
def registrar(conn: Connection, *, device_uuid: str, descricao: str = "",
              user_agent: str = "", ip: str = "") -> tuple[dict, Optional[str]]:
    """Registra o aparelho como PENDENTE e devolve (aparelho, token).

    Reexecutar com o mesmo uuid NÃO gera token novo e não muda nada: devolve o
    aparelho como está e token None. Senão, qualquer pessoa que descobrisse o
    uuid de um aparelho aprovado trocaria o token dele por um que ela conhece."""
    uuid = validar_uuid(device_uuid)
    existente = por_uuid(conn, uuid)
    if existente:
        logger.info("Ponto: aparelho %s já registrado (%s); nada mudou", uuid, existente["status"])
        return existente, None
    token = auth.gerar_token()
    linha = db.um(conn, """
        INSERT INTO ponto.dispositivos (device_uuid, token_hash, descricao, user_agent, ip_registro)
        VALUES (:u, :h, :d, :ua, :ip) RETURNING *
    """, u=uuid, h=auth.hash_token(token), d=(descricao or "")[:200],
         ua=(user_agent or "")[:300], ip=(ip or "")[:64])
    logger.info("Ponto: aparelho %s registrado como PENDENTE (%s)", uuid, ip or "?")
    return linha, token


def autenticar(conn: Connection, device_uuid: str, token: str | None) -> dict:
    """O aparelho pelo uuid, com o token conferido. Levanta NaoEncontrado para
    uuid desconhecido ou token errado — a mesma resposta nos dois casos, para
    não confirmar a existência de um uuid a quem chuta."""
    uuid = validar_uuid(device_uuid)
    d = por_uuid(conn, uuid)
    if not d or not auth.confere_hash(token, d["token_hash"]):
        raise NaoEncontrado("aparelho não encontrado ou token inválido")
    return d


def aprovar(conn: Connection, dispositivo_id: int, *, perfil: str, aprovado_por: str,
            colaborador_id: int | None = None, descricao: str | None = None,
            autorizados: list[int] | None = None, obras: list[int] | None = None) -> dict:
    d = por_id(conn, dispositivo_id)
    perfil_ok = validar_perfil(perfil)
    if perfil_ok == "INDIVIDUAL" and not colaborador_id:
        raise ErroDeValidacao("aparelho INDIVIDUAL precisa do dono (colaborador)",
                              campo="colaborador_id")
    if perfil_ok == "LISTA" and not autorizados:
        raise ErroDeValidacao("aparelho LISTA precisa de ao menos uma pessoa autorizada",
                              campo="autorizados")
    if not (aprovado_por or "").strip():
        raise ErroDeValidacao("diga quem aprova (aprovado_por)", campo="aprovado_por")
    db.executar(conn, """
        UPDATE ponto.dispositivos
           SET perfil = :p, colaborador_id = :c, status = 'APROVADO',
               aprovado_por = :por, aprovado_em = now(),
               bloqueado_em = NULL, motivo_bloqueio = NULL,
               descricao = COALESCE(:desc, descricao)
         WHERE id = :id
    """, p=perfil_ok, c=(colaborador_id if perfil_ok == "INDIVIDUAL" else None),
         por=aprovado_por.strip()[:120], desc=(descricao.strip()[:200] if descricao else None),
         id=dispositivo_id)
    definir_autorizados(conn, dispositivo_id, autorizados or [])
    definir_obras(conn, dispositivo_id, obras or [])
    logger.info("Ponto: aparelho %s APROVADO como %s por %s", d["device_uuid"], perfil_ok,
                aprovado_por)
    return detalhado(conn, dispositivo_id)


def bloquear(conn: Connection, dispositivo_id: int, *, motivo: str, por: str) -> dict:
    d = por_id(conn, dispositivo_id)
    if not (motivo or "").strip():
        raise ErroDeValidacao("diga o motivo do bloqueio", campo="motivo")
    db.executar(conn, """
        UPDATE ponto.dispositivos SET status = 'BLOQUEADO', bloqueado_em = now(),
               motivo_bloqueio = :m WHERE id = :id
    """, m=f"{motivo.strip()[:300]} (por {por or '?'})", id=dispositivo_id)
    logger.warning("Ponto: aparelho %s BLOQUEADO — %s", d["device_uuid"], motivo)
    return detalhado(conn, dispositivo_id)


def definir_autorizados(conn: Connection, dispositivo_id: int, colaborador_ids: list[int]) -> None:
    db.executar(conn, "DELETE FROM ponto.dispositivo_autorizados WHERE dispositivo_id = :d",
                d=dispositivo_id)
    for c in sorted(set(int(x) for x in colaborador_ids)):
        db.executar(conn, "INSERT INTO ponto.dispositivo_autorizados (dispositivo_id, colaborador_id)"
                          " VALUES (:d, :c) ON CONFLICT DO NOTHING", d=dispositivo_id, c=c)


def definir_obras(conn: Connection, dispositivo_id: int, obra_ids: list[int]) -> None:
    db.executar(conn, "DELETE FROM ponto.dispositivo_obras WHERE dispositivo_id = :d",
                d=dispositivo_id)
    for o in sorted(set(int(x) for x in obra_ids)):
        db.executar(conn, "INSERT INTO ponto.dispositivo_obras (dispositivo_id, obra_id)"
                          " VALUES (:d, :o) ON CONFLICT DO NOTHING", d=dispositivo_id, o=o)


def marcar_uso(conn: Connection, dispositivo_id: int) -> None:
    db.executar(conn, "UPDATE ponto.dispositivos SET ultimo_uso_em = now() WHERE id = :id",
                id=dispositivo_id)
