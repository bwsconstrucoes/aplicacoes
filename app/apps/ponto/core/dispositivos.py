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
                    autorizados: set[int], obras_do_aparelho: set[int], hoje=None) -> Optional[str]:
    """None se o aparelho pode registrar esta pessoa nesta obra; senão o motivo."""
    if dispositivo.get("status") != "APROVADO":
        return f"aparelho {str(dispositivo.get('status', '')).lower() or 'desconhecido'}"
    perfil = dispositivo.get("perfil")
    # TODO APARELHO VENCE (decisão do dono, 06/10/2026): o de grupo em 15 dias de
    # início, os outros em 90; vencido, para de bater até alguém renovar.
    if dispositivo.get("valido_ate"):
        if hoje is None:
            from ..horario import hoje as _hoje
            hoje = _hoje()
        if dispositivo["valido_ate"] < hoje:
            return (f"a liberação deste aparelho venceu em {dispositivo['valido_ate']:%d/%m/%Y} "
                    "— peça a renovação ao RH")
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


def codigo_curto(device_uuid: str) -> str:
    """Os 6 últimos caracteres do identificador, em maiúsculas: o MESMO código
    que a tela do aparelho mostra. Quem aprova confere o código na tela do
    tablet antes de aprovar — senão "Aparelho sem nome" pode ser o celular de
    qualquer funcionário que abriu o endereço, e aprovado como aparelho da obra
    ele bateria por todo mundo."""
    limpo = "".join(ch for ch in str(device_uuid or "") if ch.isalnum())
    return limpo[-6:].upper()


def para_json(d: dict) -> dict:
    from ..horario import texto
    return {
        "id": d["id"], "device_uuid": d["device_uuid"], "codigo": codigo_curto(d["device_uuid"]),
        "descricao": d.get("descricao", ""),
        "perfil": d["perfil"], "status": d["status"],
        "dono": ({"id": d["colaborador_id"], "nome": d.get("dono_nome"),
                  "cpf": d.get("dono_cpf")} if d.get("colaborador_id") else None),
        "qtd_autorizados": int(d.get("qtd_autorizados") or 0),
        "valido_ate": d["valido_ate"].isoformat() if d.get("valido_ate") else None,
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


GRUPO_DIAS_PADRAO, GRUPO_DIAS_MAXIMO = 15, 90
APARELHO_DIAS = 90              # todo aparelho se renova a cada 90 dias (06/10/2026)
AVISO_DIAS = 15                 # o aviso de "vai parar em X dias" começa aqui
AVISO_DIAS_GRUPO = 3            # … no de grupo (que nasce com 15 dias), só perto do fim


def dias_de_aviso(perfil: str) -> int:
    return AVISO_DIAS_GRUPO if perfil == "LISTA" else AVISO_DIAS
GRUPO_DIAS_SEM_USO = 7          # pessoa ou aparelho de grupo sem batida há tantos dias → sugere tirar
APARELHO_DIAS_SEM_USO = 30      # celular ou tablet sem batida há tantos dias → sugere desativar


def validade_do_grupo(valido_ate, hoje=None, *, perfil: str = "LISTA"):
    """PURA. A data em que a liberação vence: o grupo vale 15 dias de início
    (situação passageira), os outros 90; nunca mais de 90."""
    import datetime as _dt
    if hoje is None:
        from ..horario import hoje as _hoje
        hoje = _hoje()
    if valido_ate in (None, ""):
        return hoje + _dt.timedelta(days=GRUPO_DIAS_PADRAO if perfil == "LISTA" else APARELHO_DIAS)
    try:
        data = valido_ate if isinstance(valido_ate, _dt.date) else _dt.date.fromisoformat(str(valido_ate))
    except ValueError:
        raise ErroDeValidacao("data de validade ilegível", campo="valido_ate")
    if data < hoje:
        raise ErroDeValidacao("a data de validade já passou", campo="valido_ate")
    if data > hoje + _dt.timedelta(days=GRUPO_DIAS_MAXIMO):
        raise ErroDeValidacao(f"a liberação vale no máximo {GRUPO_DIAS_MAXIMO} dias — depois, renova-se",
                              campo="valido_ate")
    return data


def aprovar(conn: Connection, dispositivo_id: int, *, perfil: str, aprovado_por: str,
            colaborador_id: int | None = None, descricao: str | None = None,
            autorizados: list[int] | None = None, obras: list[int] | None = None,
            valido_ate=None) -> dict:
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
    if db.tem_coluna(conn, "dispositivos", "valido_ate"):
        db.executar(conn, "UPDATE ponto.dispositivos SET valido_ate = :v WHERE id = :id",
                    v=validade_do_grupo(valido_ate, perfil=perfil_ok), id=dispositivo_id)
    # Aprovar o celular de uma pessoa É cadastrar a exceção dela: o padrão é só
    # o aparelho da obra bater (forma_de_bater.py, decisão do dono de 05/10/2026).
    from . import forma_de_bater
    if perfil_ok == "INDIVIDUAL" and forma_de_bater.em_vigor(conn):
        forma_de_bater.definir(conn, int(colaborador_id), True, f"aprovação do aparelho por {aprovado_por}")
    # UM CELULAR PESSOAL POR PESSOA (pergunta do dono, 06/10/2026: "uma pessoa
    # consegue cadastrar mais de um aparelho no seu CPF?"). Aprovar o novo
    # bloqueia o anterior: celular trocado não fica valendo na mão de outro.
    substituidos = []
    if perfil_ok == "INDIVIDUAL":
        substituidos = [int(l["id"]) for l in db.todos(conn, """
            UPDATE ponto.dispositivos SET status = 'BLOQUEADO', bloqueado_em = now(),
                   motivo_bloqueio = :m
             WHERE colaborador_id = :c AND perfil = 'INDIVIDUAL' AND status = 'APROVADO' AND id <> :id
            RETURNING id""", c=colaborador_id, id=dispositivo_id,
            m=f"trocado por outro celular da mesma pessoa (por {aprovado_por.strip()[:80]})")]
        if substituidos:
            logger.info("Ponto: celular(es) %s bloqueado(s) — a pessoa %s passou a usar o %s",
                        substituidos, colaborador_id, dispositivo_id)
    logger.info("Ponto: aparelho %s APROVADO como %s por %s", d["device_uuid"], perfil_ok,
                aprovado_por)
    return {**detalhado(conn, dispositivo_id), "substituidos": substituidos}


def renovar_grupo(conn: Connection, dispositivo_id: int, *, valido_ate, por: str) -> dict:
    """Renova a liberação do aparelho (qualquer um: o de grupo, o da obra, o
    celular da pessoa). Aparelho bloqueado não se renova — reativa-se."""
    d = por_id(conn, dispositivo_id)
    if d["status"] != "APROVADO":
        raise ErroDeValidacao("aparelho não aprovado — use Reativar")
    data = validade_do_grupo(valido_ate, perfil=d["perfil"])
    db.executar(conn, "UPDATE ponto.dispositivos SET valido_ate = :v WHERE id = :id", v=data, id=dispositivo_id)
    logger.info("Ponto: aparelho %s renovado até %s por %s", dispositivo_id, data, por)
    return detalhado(conn, dispositivo_id)


def vencimento(dispositivo: dict, hoje=None) -> Optional[dict]:
    """PURA. O aviso para a tela do próprio aparelho: {'valido_ate', 'dias',
    'vencido', 'avisar'}; None quando não há prazo."""
    v = dispositivo.get("valido_ate")
    if not v:
        return None
    if hoje is None:
        from ..horario import hoje as _hoje
        hoje = _hoje()
    dias = (v - hoje).days
    return {"valido_ate": v.isoformat(), "dias": dias, "vencido": dias < 0,
            "avisar": dias <= dias_de_aviso(dispositivo.get("perfil", ""))}


def sem_uso_no_grupo(conn: Connection, dispositivo_id: int, dias: int = GRUPO_DIAS_SEM_USO) -> list[dict]:
    """Quem está no grupo e não bate por este aparelho há `dias` dias."""
    return db.todos(conn, """
        SELECT a.colaborador_id, c.nome FROM ponto.dispositivo_autorizados a
          JOIN public.colaboradores c ON c.id = a.colaborador_id
         WHERE a.dispositivo_id = :d
           AND NOT EXISTS (SELECT 1 FROM ponto.marcacoes m WHERE m.dispositivo_id = :d
                            AND m.colaborador_id = a.colaborador_id
                            AND m.timestamp_servidor > now() - make_interval(days => :n))
         ORDER BY c.nome""", d=dispositivo_id, n=dias)


def tirar_sem_uso(conn: Connection, dispositivo_id: int, por: str) -> int:
    fora = sem_uso_no_grupo(conn, dispositivo_id)
    for p in fora:
        db.executar(conn, "DELETE FROM ponto.dispositivo_autorizados WHERE dispositivo_id = :d AND colaborador_id = :c",
                    d=dispositivo_id, c=p["colaborador_id"])
    logger.info("Ponto: %d pessoa(s) sem uso tiradas do grupo do aparelho %s por %s", len(fora), dispositivo_id, por)
    return len(fora)


def aparelhos_a_rever(conn: Connection) -> list[dict]:
    """O que o sistema SUGERE renovar ou desativar (pedidos do dono, 06/10/2026):
      · TODO aparelho (celular da pessoa, da obra, de grupo) vencido ou que vence
        em até 15 dias — "será bloqueado em tantos dias";
      · o de grupo sem batida há 7 dias, ou com gente que não bate mais nele
        ("detectar que aquela situação já não está acontecendo");
      · celular ou tablet sem batida há 30 dias."""
    import datetime as _dt
    from ..horario import hoje as _hoje
    if not db.tem_coluna(conn, "dispositivos", "valido_ate"):
        return []
    hoje = _hoje()
    saida = []
    for g in db.todos(conn, """
            SELECT d.*, c.nome AS dono_nome,
                   (SELECT count(*) FROM ponto.dispositivo_autorizados a WHERE a.dispositivo_id = d.id) AS pessoas,
                   (SELECT max(m.timestamp_servidor) FROM ponto.marcacoes m WHERE m.dispositivo_id = d.id) AS ultima_batida
              FROM ponto.dispositivos d LEFT JOIN public.colaboradores c ON c.id = d.colaborador_id
             WHERE d.status = 'APROVADO' ORDER BY d.valido_ate NULLS LAST, d.id"""):
        motivos, encerrar = [], False
        vence = g.get("valido_ate")
        if vence and vence < hoje:
            motivos.append(f"venceu em {vence:%d/%m/%Y} — parou de bater")
        elif vence and vence <= hoje + _dt.timedelta(days=dias_de_aviso(g["perfil"])):
            dias = (vence - hoje).days
            motivos.append(f"para de bater em {dias} dia(s), em {vence:%d/%m/%Y}" if dias else "para de bater amanhã")
        aprovado_ha = (hoje - (g["aprovado_em"].date() if g.get("aprovado_em") else hoje)).days
        ultima = g.get("ultima_batida")
        parado = lambda n: aprovado_ha >= n and (ultima is None or (hoje - ultima.date()).days >= n)  # noqa: E731
        if g["perfil"] == "LISTA":
            if parado(GRUPO_DIAS_SEM_USO):
                motivos.append(f"nenhuma batida há {GRUPO_DIAS_SEM_USO} dias ou mais — a situação parece ter acabado")
                encerrar = True
            elif aprovado_ha >= GRUPO_DIAS_SEM_USO:
                fora = sem_uso_no_grupo(conn, g["id"])
                if fora:
                    motivos.append(f"{len(fora)} pessoa(s) do grupo não batem nele há {GRUPO_DIAS_SEM_USO} dias: "
                                   + ", ".join(p["nome"].split(" ")[0].title() for p in fora[:5])
                                   + ("…" if len(fora) > 5 else ""))
        elif parado(APARELHO_DIAS_SEM_USO):
            motivos.append(f"nenhuma batida há {APARELHO_DIAS_SEM_USO} dias ou mais")
            encerrar = True
        if motivos:
            saida.append({"id": g["id"], "perfil": g["perfil"],
                          "descricao": g["descricao"] or ("celular da pessoa" if g["perfil"] == "INDIVIDUAL"
                                                          else "aparelho de grupo" if g["perfil"] == "LISTA"
                                                          else "aparelho da obra"),
                          "dono": g.get("dono_nome"), "codigo": codigo_curto(g["device_uuid"]),
                          "pessoas": int(g["pessoas"]), "valido_ate": vence.isoformat() if vence else None,
                          "motivos": motivos, "sugestao": "DESATIVAR" if encerrar else "RENOVAR"})
    return saida


grupos_a_rever = aparelhos_a_rever      # nome antigo


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
