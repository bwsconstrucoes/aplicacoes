# -*- coding: utf-8 -*-
"""
O MODO DE TESTE: uma obra e as pessoas de teste, sem tocar nas planilhas.

Pedido do dono, 05/10/2026: *"tava querendo fazer uns testes do ponto, mas aí é
ruim: tem que cadastrar a pessoa, tem que cadastrar a localização. (…) Eu queria
fazer teste comigo mesmo. Se tivesse algum canto que eu pudesse gravar uma
geolocalização e colocar meu nome e CPF."*

  A OBRA DE TESTE  `TESTE-PONTO` em `public.obras`, com o status `TESTE_PONTO`
                   (as listas de obra ATIVA do ERP não a mostram). A coordenada é
                   a do lugar onde a pessoa está quando aperta "usar a minha
                   localização". Vale no ponto mesmo com a C. Diários como base
                   (`obra_config.ensaio`).
  A PESSOA DE TESTE  nome, CPF e celular. Se o CPF não existe no ERP, é criada lá
                   (mínimo: nome, CPF, celular, a obra de teste); se existe, só
                   ganha a obra de teste como obra adicional. Bate como ATIVA
                   mesmo fora do Registro de Colaboradores
                   (`colaborador_config.ensaio`), e também no próprio celular.
  O CÓDIGO DE PRIMEIRO ACESSO  para a pessoa de teste, a gestão vê o código na
                   tela (sem depender do WhatsApp). Só para pessoa de teste.
  DESLIGAR         tira a obra de teste do ponto. As batidas feitas ficam (registro
                   de ponto não se apaga), na obra TESTE-PONTO.
"""
from __future__ import annotations

import logging
import re

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao, NaoEncontrado
from . import cadastros, forma_de_bater, geo

logger = logging.getLogger("ponto.ensaio")

CODIGO_OBRA = "TESTE-PONTO"
NOME_OBRA = "TESTE DO PONTO (obra de teste)"
STATUS_ERP = "TESTE_PONTO"
RAIO_PADRAO = 200


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "obra_config", "ensaio") and db.tem_coluna(conn, "colaborador_config", "ensaio")


def _exigir(conn: Connection) -> None:
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 005) antes de usar o modo de teste")


def _obra(conn: Connection):
    return db.um(conn, """
        SELECT o.id, o.latitude, o.longitude, COALESCE(oc.raio_metros, :r) AS raio_metros,
               COALESCE(oc.ativo, TRUE) AS ativa, COALESCE(oc.ensaio, FALSE) AS ensaio
          FROM public.obras o LEFT JOIN ponto.obra_config oc ON oc.obra_id = o.id
         WHERE upper(btrim(o.codigo)) = :c""", c=CODIGO_OBRA, r=RAIO_PADRAO)


def situacao(conn: Connection) -> dict:
    if not disponivel(conn):
        return {"disponivel": False}
    o = _obra(conn)
    pessoas = []
    if o:
        pessoas = db.todos(conn, """
            SELECT c.id, c.nome, c.cpf, c.telefone, (pc.pin_hash IS NOT NULL) AS tem_pin,
                   COALESCE(pc.bate_no_celular, FALSE) AS bate_no_celular,
                   (SELECT count(*) FROM ponto.marcacoes m WHERE m.colaborador_id = c.id
                     AND m.obra_id = :o) AS batidas
              FROM ponto.colaborador_config pc JOIN public.colaboradores c ON c.id = pc.colaborador_id
             WHERE pc.ensaio ORDER BY c.nome""", o=o["id"])
    return {
        "disponivel": True,
        "obra": ({"id": o["id"], "codigo": CODIGO_OBRA,
                  "latitude": float(o["latitude"]) if o["latitude"] is not None else None,
                  "longitude": float(o["longitude"]) if o["longitude"] is not None else None,
                  "raio_metros": int(o["raio_metros"]), "ligada": bool(o["ensaio"] and o["ativa"])}
                 if o else None),
        "pessoas": [{"id": p["id"], "nome": p["nome"], "cpf_final": (p["cpf"] or "")[-3:],
                     "tem_telefone": len(re.sub(r"\D", "", p["telefone"] or "")) >= 10,
                     "tem_pin": bool(p["tem_pin"]), "bate_no_celular": bool(p["bate_no_celular"]),
                     "batidas": int(p["batidas"])} for p in pessoas],
    }


def gravar_obra(conn: Connection, *, latitude=None, longitude=None, coordenada: str = "",
                raio_metros=None, por: str) -> dict:
    """Cria ou move a obra de teste para o lugar informado, e a liga. O lugar
    vem da localização do navegador (latitude, longitude) ou digitado/colado
    (`coordenada`), no mesmo formato da coluna AM da C. Diários."""
    _exigir(conn)
    if str(coordenada or "").strip():
        from . import base_obras
        lido = base_obras.ler_coordenada(coordenada)
        if lido["problema"]:
            raise ErroDeValidacao(f"coordenada: {lido['problema']}", campo="coordenada")
        latitude, longitude = lido["latitude"], lido["longitude"]
    if not geo.coordenada_valida(latitude, longitude):
        raise ErroDeValidacao("localização inválida — permita a localização no navegador e tente de novo")
    raio = int(raio_metros) if raio_metros not in (None, "") else RAIO_PADRAO
    o = _obra(conn)
    if o:
        obra_id = int(o["id"])
    else:
        obra_id = int(db.um(conn, """INSERT INTO public.obras (codigo, nome, status)
                                     VALUES (:c, :n, :s) RETURNING id""",
                            c=CODIGO_OBRA, n=NOME_OBRA, s=STATUS_ERP)["id"])
    cadastros.gravar_coordenadas_da_obra(conn, obra_id, geo.decimal_ou_none(str(latitude)),
                                         geo.decimal_ou_none(str(longitude)))
    cadastros.gravar_config_obra(conn, obra_id, raio_metros=raio, ativo=True)
    db.executar(conn, "UPDATE ponto.obra_config SET ensaio = TRUE, atualizado_em = now() WHERE obra_id = :o",
                o=obra_id)
    logger.info("Ponto: obra de teste em %s, %s (raio %s m) por %s", latitude, longitude, raio, por)
    return situacao(conn)


def desligar(conn: Connection, por: str) -> dict:
    _exigir(conn)
    o = _obra(conn)
    if o:
        cadastros.gravar_config_obra(conn, int(o["id"]), ativo=False)
        logger.info("Ponto: obra de teste desligada por %s", por)
    return situacao(conn)


def gravar_pessoa(conn: Connection, *, nome: str, cpf: str, celular: str, por: str) -> dict:
    _exigir(conn)
    o = _obra(conn)
    if not o:
        raise ErroDeValidacao("grave primeiro a localização da obra de teste")
    cpf_ok = cadastros.normalizar_cpf(cpf)
    tel = re.sub(r"\D", "", celular or "")
    if len(tel) < 10:
        raise ErroDeValidacao("celular com DDD (só números)", campo="celular")
    existente = db.um(conn, "SELECT id FROM public.colaboradores "
                            "WHERE regexp_replace(cpf, '\\D', '', 'g') = :c", c=cpf_ok)
    if existente:
        pessoa_id = int(existente["id"])
        db.executar(conn, "UPDATE public.colaboradores SET telefone = COALESCE(NULLIF(btrim(telefone), ''), :t) "
                          "WHERE id = :id", t=tel, id=pessoa_id)
        db.executar(conn, "INSERT INTO ponto.colaborador_obras (colaborador_id, obra_id) VALUES (:c, :o) "
                          "ON CONFLICT DO NOTHING", c=pessoa_id, o=o["id"])
    else:
        if not (nome or "").strip():
            raise ErroDeValidacao("diga o nome", campo="nome")
        pessoa_id = cadastros.criar_colaborador_no_erp(conn, nome=nome, cpf=cpf_ok, obra_id=int(o["id"]))
        db.executar(conn, "UPDATE public.colaboradores SET telefone = :t WHERE id = :id", t=tel, id=pessoa_id)
    cadastros.gravar_config_colaborador(conn, pessoa_id, ativo=True)
    db.executar(conn, "UPDATE ponto.colaborador_config SET ensaio = TRUE, atualizado_em = now() "
                      "WHERE colaborador_id = :c", c=pessoa_id)
    if forma_de_bater.em_vigor(conn):
        forma_de_bater.definir(conn, pessoa_id, True, f"modo de teste ({por})")
    logger.info("Ponto: pessoa de teste %s (%s) por %s", pessoa_id, "já existia" if existente else "criada", por)
    return situacao(conn)


def codigo_de_acesso(conn: Connection, colaborador_id: int, por: str) -> str:
    """O código de primeiro acesso, para a tela — só para pessoa de teste."""
    from . import acesso
    _exigir(conn)
    p = db.um(conn, """SELECT c.cpf FROM ponto.colaborador_config pc JOIN public.colaboradores c
                         ON c.id = pc.colaborador_id WHERE pc.colaborador_id = :c AND pc.ensaio""",
              c=colaborador_id)
    if not p:
        raise NaoEncontrado("pessoa de teste não encontrada")
    capturado: list[str] = []
    acesso.pedir_codigo(conn, p["cpf"], enviar=lambda tel, msg: capturado.append(msg))
    achado = re.search(r"código é (\d{6})", capturado[-1]) if capturado else None
    if not achado:
        raise ErroDeValidacao("não deu para gerar o código agora (celular sem DDD, ou códigos demais "
                              "na última hora) — espere um pouco e tente de novo")
    logger.info("Ponto: código de primeiro acesso da pessoa de teste %s mostrado a %s", colaborador_id, por)
    return achado.group(1)
