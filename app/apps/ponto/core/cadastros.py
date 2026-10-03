# -*- coding: utf-8 -*-
"""
Obras e pessoas PARA O PONTO — lidas do ERP, afinadas aqui.

O cadastro é do ERP (`public.obras`, `public.colaboradores`); o ponto guarda só
o que o ERP não tem (`ponto.obra_config`, `ponto.colaborador_config`,
`ponto.colaborador_obras`). Este módulo junta as duas metades num dicionário só,
para o resto do ponto nunca precisar saber de onde veio cada campo.

Pessoa sem linha em `colaborador_config` bate ponto com os padrões (jornada
PADRAO_4, ativa). Obra sem linha em `obra_config` usa o raio padrão (200 m).
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.engine import Connection

from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos

from .. import db
from ..erros import ErroDeValidacao
from ..horario import JORNADA_PADRAO, JORNADAS

RAIO_PADRAO_METROS = 200

_SQL_COLABORADOR = """
    SELECT c.id, c.nome, c.cpf, c.matricula, c.obra_id, c.situacao, c.demissao,
           COALESCE(pc.tipo_jornada, :jornada_padrao) AS tipo_jornada,
           pc.centro_custo, pc.foto_cadastral_id,
           COALESCE(pc.ativo, TRUE) AS ativo_no_ponto,
           (pc.colaborador_id IS NOT NULL) AS tem_config,
           o.codigo AS obra_codigo, o.nome AS obra_nome
      FROM public.colaboradores c
      LEFT JOIN ponto.colaborador_config pc ON pc.colaborador_id = c.id
      LEFT JOIN public.obras o ON o.id = c.obra_id
"""

_SQL_OBRA = """
    SELECT o.id, o.codigo, o.nome, o.status, o.municipio, o.uf,
           o.latitude, o.longitude,
           COALESCE(oc.raio_metros, :raio_padrao) AS raio_metros,
           oc.centro_custo,
           COALESCE(oc.ativo, TRUE) AS ativo_no_ponto,
           (oc.obra_id IS NOT NULL) AS tem_config
      FROM public.obras o
      LEFT JOIN ponto.obra_config oc ON oc.obra_id = o.id
"""


def normalizar_cpf(cpf) -> str:
    """Só dígitos, validado. Levanta ErroDeValidacao se não for CPF."""
    digitos = somente_digitos(str(cpf or ""))
    if not cpf_valido(digitos):
        raise ErroDeValidacao("CPF inválido", campo="cpf")
    return digitos


def validar_jornada(tipo: str | None) -> str:
    valor = (tipo or JORNADA_PADRAO).strip().upper()
    if valor not in JORNADAS:
        raise ErroDeValidacao(
            f"tipo de jornada desconhecido: {tipo!r} (use {', '.join(JORNADAS)})",
            campo="tipo_jornada")
    return valor


# ---------------------------------------------------------------------------
# Pessoas
# ---------------------------------------------------------------------------
def colaborador_por_cpf(conn: Connection, cpf: str) -> Optional[dict]:
    """A pessoa pelo CPF (só dígitos). O ERP guarda o CPF só com dígitos; a
    comparação tira a máscara dos dois lados por garantia."""
    return db.um(conn, _SQL_COLABORADOR +
                 " WHERE regexp_replace(c.cpf, '\\D', '', 'g') = :cpf",
                 cpf=somente_digitos(cpf), jornada_padrao=JORNADA_PADRAO)


def colaborador_por_id(conn: Connection, colaborador_id: int) -> Optional[dict]:
    return db.um(conn, _SQL_COLABORADOR + " WHERE c.id = :id",
                 id=colaborador_id, jornada_padrao=JORNADA_PADRAO)


def obras_da_pessoa(conn: Connection, colaborador_id: int) -> set[int]:
    """A principal (do ERP) mais as adicionais (do ponto)."""
    linhas = db.todos(conn, """
        SELECT obra_id FROM public.colaboradores WHERE id = :id AND obra_id IS NOT NULL
        UNION
        SELECT obra_id FROM ponto.colaborador_obras WHERE colaborador_id = :id
    """, id=colaborador_id)
    return {int(l["obra_id"]) for l in linhas}


def listar_colaboradores(conn: Connection, *, so_ativos: bool = True,
                         obra_id: int | None = None) -> list[dict]:
    sql = _SQL_COLABORADOR + " WHERE 1 = 1"
    params: dict = {"jornada_padrao": JORNADA_PADRAO}
    if so_ativos:
        sql += " AND c.situacao <> 'DESLIGADO' AND COALESCE(pc.ativo, TRUE)"
    if obra_id is not None:
        sql += (" AND (c.obra_id = :obra_id OR EXISTS (SELECT 1 FROM ponto.colaborador_obras co"
                " WHERE co.colaborador_id = c.id AND co.obra_id = :obra_id))")
        params["obra_id"] = obra_id
    sql += " ORDER BY c.nome, c.id"
    pessoas = db.todos(conn, sql, **params)
    adicionais = db.todos(conn, """
        SELECT co.colaborador_id, o.id, o.codigo, o.nome
          FROM ponto.colaborador_obras co JOIN public.obras o ON o.id = co.obra_id
         ORDER BY o.codigo""")
    por_pessoa: dict[int, list] = {}
    for a in adicionais:
        por_pessoa.setdefault(int(a["colaborador_id"]), []).append(
            {"id": a["id"], "codigo": a["codigo"], "nome": a["nome"]})
    for p in pessoas:
        p["obras_adicionais"] = por_pessoa.get(int(p["id"]), [])
    return pessoas


def gravar_config_colaborador(conn: Connection, colaborador_id: int, *,
                              tipo_jornada: str | None = None,
                              centro_custo: str | None = None,
                              ativo: bool | None = None) -> None:
    """Cria ou atualiza a linha do ponto para a pessoa. Só mexe no que veio."""
    jornada = validar_jornada(tipo_jornada) if tipo_jornada else None
    db.executar(conn, """
        INSERT INTO ponto.colaborador_config (colaborador_id, tipo_jornada, centro_custo, ativo)
        VALUES (:id, COALESCE(:jornada, :padrao), :cc, COALESCE(:ativo, TRUE))
        ON CONFLICT (colaborador_id) DO UPDATE SET
            tipo_jornada  = COALESCE(:jornada, ponto.colaborador_config.tipo_jornada),
            centro_custo  = COALESCE(:cc, ponto.colaborador_config.centro_custo),
            ativo         = COALESCE(:ativo, ponto.colaborador_config.ativo),
            atualizado_em = now()
    """, id=colaborador_id, jornada=jornada, padrao=JORNADA_PADRAO,
         cc=(centro_custo or None), ativo=ativo)


def definir_obras_adicionais(conn: Connection, colaborador_id: int,
                             obra_ids: list[int]) -> None:
    db.executar(conn, "DELETE FROM ponto.colaborador_obras WHERE colaborador_id = :id",
                id=colaborador_id)
    for obra_id in sorted(set(obra_ids)):
        db.executar(conn, "INSERT INTO ponto.colaborador_obras (colaborador_id, obra_id) "
                          "VALUES (:c, :o) ON CONFLICT DO NOTHING",
                    c=colaborador_id, o=obra_id)


def criar_colaborador_no_erp(conn: Connection, *, nome: str, cpf: str,
                             obra_id: int | None) -> int:
    """Cria a pessoa no cadastro do ERP com o mínimo (nome, CPF, obra). É a
    única escrita do ponto numa tabela do ERP, e só o importador a usa —
    autorizada pelo dono junto com a decisão de reusar o cadastro."""
    linha = db.um(conn, """
        INSERT INTO public.colaboradores (nome, cpf, obra_id, regime, situacao)
        VALUES (:nome, :cpf, :obra_id, 'CLT', 'ATIVO') RETURNING id
    """, nome=nome.strip(), cpf=normalizar_cpf(cpf), obra_id=obra_id)
    return int(linha["id"])


# ---------------------------------------------------------------------------
# Obras
# ---------------------------------------------------------------------------
def obra_por_id(conn: Connection, obra_id: int) -> Optional[dict]:
    return db.um(conn, _SQL_OBRA + " WHERE o.id = :id", id=obra_id,
                 raio_padrao=RAIO_PADRAO_METROS)


def obra_por_codigo(conn: Connection, codigo: str) -> Optional[dict]:
    return db.um(conn, _SQL_OBRA + " WHERE upper(trim(o.codigo)) = upper(trim(:codigo))",
                 codigo=str(codigo), raio_padrao=RAIO_PADRAO_METROS)


def obra_por_nome(conn: Connection, nome: str) -> Optional[dict]:
    return db.um(conn, _SQL_OBRA + " WHERE upper(trim(o.nome)) = upper(trim(:nome))",
                 nome=str(nome), raio_padrao=RAIO_PADRAO_METROS)


def resolver_obra(conn: Connection, referencia) -> Optional[dict]:
    """Aceita o número (id), o código ou o nome exato da obra."""
    if referencia is None or str(referencia).strip() == "":
        return None
    texto = str(referencia).strip()
    if texto.isdigit():
        obra = obra_por_id(conn, int(texto))
        if obra:
            return obra
    return obra_por_codigo(conn, texto) or obra_por_nome(conn, texto)


def listar_obras(conn: Connection, *, so_ativas: bool = True) -> list[dict]:
    sql = _SQL_OBRA + " WHERE 1 = 1"
    if so_ativas:
        sql += " AND o.status = 'ATIVA' AND COALESCE(oc.ativo, TRUE)"
    sql += " ORDER BY o.codigo"
    return db.todos(conn, sql, raio_padrao=RAIO_PADRAO_METROS)


def gravar_config_obra(conn: Connection, obra_id: int, *, raio_metros: int | None = None,
                       centro_custo: str | None = None, ativo: bool | None = None) -> None:
    if raio_metros is not None and not (20 <= int(raio_metros) <= 5000):
        raise ErroDeValidacao("raio deve ficar entre 20 e 5000 metros", campo="raio_metros")
    db.executar(conn, """
        INSERT INTO ponto.obra_config (obra_id, raio_metros, centro_custo, ativo)
        VALUES (:id, COALESCE(:raio, :padrao), :cc, COALESCE(:ativo, TRUE))
        ON CONFLICT (obra_id) DO UPDATE SET
            raio_metros   = COALESCE(:raio, ponto.obra_config.raio_metros),
            centro_custo  = COALESCE(:cc, ponto.obra_config.centro_custo),
            ativo         = COALESCE(:ativo, ponto.obra_config.ativo),
            atualizado_em = now()
    """, id=obra_id, raio=raio_metros, padrao=RAIO_PADRAO_METROS,
         cc=(centro_custo or None), ativo=ativo)


def criar_obra_no_erp(conn: Connection, *, codigo: str, nome: str) -> int:
    """Cria a obra no ERP com o mínimo (código e nome). Só o importador usa."""
    linha = db.um(conn, """
        INSERT INTO public.obras (codigo, nome, status) VALUES (:codigo, :nome, 'ATIVA')
        RETURNING id
    """, codigo=codigo.strip(), nome=nome.strip())
    return int(linha["id"])


def gravar_coordenadas_da_obra(conn: Connection, obra_id: int, latitude, longitude) -> None:
    """Preenche latitude/longitude da obra NO ERP (é lá que elas moram, desde a
    migração 024). Só o importador chama, e só quando a obra ainda não tem."""
    db.executar(conn, "UPDATE public.obras SET latitude = :lat, longitude = :lon, "
                      "atualizado_em = now() WHERE id = :id",
                id=obra_id, lat=latitude, lon=longitude)


def obra_para_json(o: dict) -> dict:
    return {
        "id": o["id"], "codigo": o["codigo"], "nome": o["nome"],
        "municipio": o.get("municipio"), "uf": o.get("uf"),
        "latitude": float(o["latitude"]) if o.get("latitude") is not None else None,
        "longitude": float(o["longitude"]) if o.get("longitude") is not None else None,
        "raio_metros": int(o["raio_metros"]),
        "centro_custo": o.get("centro_custo"),
        "ativa": bool(o.get("status") == "ATIVA" and o.get("ativo_no_ponto", True)),
    }


def colaborador_para_json(c: dict) -> dict:
    return {
        "id": c["id"], "cpf": c["cpf"], "nome": c["nome"], "matricula": c.get("matricula"),
        "obra_principal": ({"id": c["obra_id"], "codigo": c.get("obra_codigo"),
                            "nome": c.get("obra_nome")} if c.get("obra_id") else None),
        "obras_adicionais": c.get("obras_adicionais", []),
        "tipo_jornada": c["tipo_jornada"], "centro_custo": c.get("centro_custo"),
        "situacao": c["situacao"], "ativo_no_ponto": bool(c["ativo_no_ponto"]),
    }
