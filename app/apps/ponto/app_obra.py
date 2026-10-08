# -*- coding: utf-8 -*-
"""
A TELA INICIAL DO APLICATIVO e a CONSULTA DO PONTO DAS PESSOAS DA OBRA.

Pedido do dono, 08/10/2026: *"a tela vai ter uma tela principal e dali eu vou
para essas três funcionalidades"* — bater o ponto, incluir atestado,
afastamento, ajuste e compensação, e consultar o ponto dos colaboradores; e,
na folha de uma pessoa, *"clicar no dia para inserir um afastamento, para
solicitar um ajuste"*.

Quem pode o quê e quem enxerga quem mora em `core/papeis.py`. Aqui ficam só as
rotas. A localização vem do aparelho em cada chamada (`lat`, `lon`,
`precisao`), como na batida: a consulta vale na obra em que ele ESTÁ.

Fora do alcance, a pessoa responde 404 "não encontrada" — nunca "sem
permissão" — para o número de outra pessoa não confirmar que ela existe.
"""
from __future__ import annotations

import logging

from flask import request, session

from . import auth, db, horario
from .app_colaborador import _corpo, _ok
from .core import cadastros, dispositivos, espelho, forma_de_bater, ocorrencias, papeis
from .core.ocorrencias import Quem
from .erros import ErroDeValidacao, NaoAutenticado, NaoEncontrado, Recusada
from .routes import bp

logger = logging.getLogger("ponto.app_obra")

ROTULO_PAPEL = {"ADMINISTRATIVO": "administrativo da obra", "RESPONSAVEL_OBRA": "responsável pelo ponto da obra",
                "RESPONSAVEL_EQUIPE": "responsável pelo ponto de equipe"}


def _aparelho(conn):
    uuid = request.headers.get("X-Device-UUID", "")
    token = request.headers.get(auth.CABECALHO_TOKEN, "")
    if not uuid or not token:
        return None
    try:
        return dispositivos.autenticar(conn, uuid, token)
    except Exception:  # noqa: BLE001 — aparelho desconhecido: segue sem aparelho
        return None


def _logado(conn):
    """A pessoa da sessão, se houver e se ainda vale."""
    cid = session.get(auth.SESSAO_COLABORADOR)
    if not cid:
        return None
    p = cadastros.colaborador_por_id(conn, int(cid))
    if not p or p["situacao"] == "DESLIGADO" or not p["ativo_no_ponto"]:
        session.pop(auth.SESSAO_COLABORADOR, None)
        return None
    return p


def _local(fonte=None) -> dict | None:
    fonte = fonte if fonte is not None else request.args
    lat, lon = fonte.get("lat", fonte.get("latitude")), fonte.get("lon", fonte.get("longitude"))
    if lat in (None, "") or lon in (None, ""):
        return None
    try:
        return {"latitude": float(lat), "longitude": float(lon),
                "precisao": float(fonte.get("precisao")) if fonte.get("precisao") not in (None, "") else None}
    except (TypeError, ValueError):
        return None


def _alcance_json(a: papeis.Alcance) -> dict:
    return {"obras": list(a.obras.values()), "de_onde": a.de_onde, "pode_pedir": a.pode_pedir,
            "tem_equipe": bool(a.equipe), "papel": a.papel, "papel_rotulo": ROTULO_PAPEL.get(a.papel),
            "sem_alcance": a.sem_alcance}


@bp.route("/app/api/inicio")
@auth.publica("o que a tela inicial deste aparelho pode mostrar; só o nome de quem entrou")
def app_api_inicio():
    with db.conexao() as conn:
        a = _aparelho(conn)
        p = _logado(conn)
        da_obra = papeis.aparelho_valendo(a) and a["perfil"] != "INDIVIDUAL"
        alc = papeis.alcance(conn, p, a, _local()) if p else papeis.Alcance()
        bate_no_meu = bool(p) and not da_obra and forma_de_bater.pode_no_celular(p, forma_de_bater.em_vigor(conn))
        pede_o_meu = bool(p) and papeis.pede_pelo_proprio_celular(conn, p)
        administrativo = papeis.e_administrativo(conn, p)
    return _ok(
        pessoa=({"nome": p["nome"], "primeiro_nome": p["nome"].split(" ")[0],
                 "administrativo": administrativo} if p else None),
        da_obra=bool(da_obra), perfil=(a or {}).get("perfil"),
        pode={"bater_aqui": bool(da_obra), "bater_o_meu": bate_no_meu,
              "entregar_aqui": bool(da_obra), "pedir_o_meu": pede_o_meu,
              "consultar": not alc.vazio, "pedir_por_outros": bool(alc.pode_pedir and not alc.vazio),
              "entrar_para_consultar": bool(da_obra and not p)},
        alcance=_alcance_json(alc))


def _contexto(conn):
    """A pessoa logada e o alcance dela aqui. Sem sessão, 401."""
    p = _logado(conn)
    if not p:
        raise NaoAutenticado("entre com o seu CPF e PIN")
    return p, papeis.alcance(conn, p, _aparelho(conn), _local(_corpo() if request.method == "POST" else None))


def _alvo(conn, a: papeis.Alcance, colaborador_id: int, *datas) -> dict:
    if a.vazio or not papeis.no_alcance(conn, a, colaborador_id, *datas):
        raise NaoEncontrado("pessoa não encontrada")
    alvo = cadastros.colaborador_por_id(conn, colaborador_id)
    if not alvo:
        raise NaoEncontrado("pessoa não encontrada")
    return alvo


@bp.route("/app/api/equipe")
@auth.exige_colaborador
def app_api_equipe():
    with db.conexao() as conn:
        p, a = _contexto(conn)
        competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
        pessoas = papeis.pessoas_do_alcance(conn, a, competencia, exceto=int(p["id"]))
    return _ok(alcance=_alcance_json(a), competencia=competencia[:7], pessoas=pessoas)


@bp.route("/app/api/equipe/<int:colaborador_id>/mes")
@auth.exige_colaborador
def app_api_equipe_mes(colaborador_id: int):
    from .core import competencias
    with db.conexao() as conn:
        p, a = _contexto(conn)
        competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
        alvo = _alvo(conn, a, colaborador_id, competencias.primeiro_dia(competencia))
        esp = espelho.do_mes(conn, colaborador_id, competencia)
    return _ok(pessoa={"id": alvo["id"], "nome": alvo["nome"], "funcao": alvo.get("funcao"),
                       "obra": alvo.get("obra_codigo"), "cpf_final": alvo["cpf"][-3:],
                       "tem_banco": alvo.get("regime_banco") not in (None, "SEM_BANCO")},
               competencia=esp["competencia"], resumo=esp["resumo"], dias=esp["dias"],
               competencia_fechada=esp["competencia_fechada"], pode_pedir=a.pode_pedir,
               obras=list(a.obras.values()))


def _quem_por_outro(conn, p: dict, a: papeis.Alcance, alvo_id: int) -> Quem:
    if not a.pode_pedir:
        raise Recusada("daqui você consulta, mas não faz pedido por outra pessoa")
    if not papeis.disponivel(conn):
        raise Recusada("aplique as atualizações do ponto (migração 008) para pedir por outra pessoa")
    rotulo = ROTULO_PAPEL.get(a.papel, "responsável")
    return Quem(nome=f"{p['nome']} ({rotulo}, pelo aplicativo)"[:120], pessoas={int(alvo_id)},
                obras=list(a.obras))


def _datas_do_pedido(d: dict):
    from .core.ocorrencias import _data
    datas = []
    for campo in ("data_inicio", "dia_trabalhado"):
        if d.get(campo):
            datas.append(_data(d[campo], campo))
    for h in d.get("horarios") or []:
        datas.append(_data(str(h)[:10], "horarios"))
    return datas


@bp.route("/app/api/equipe/<int:colaborador_id>/pedidos", methods=["POST"])
@auth.exige_colaborador
def app_api_equipe_pedir(colaborador_id: int):
    d = _corpo()
    with db.conexao() as conn:
        p, a = _contexto(conn)
        if int(colaborador_id) == int(p["id"]):
            raise ErroDeValidacao("o seu próprio pedido é feito no “Meu ponto”")
        _alvo(conn, a, colaborador_id, *_datas_do_pedido(d))
        quem = _quem_por_outro(conn, p, a, colaborador_id)
        dados = {k: v for k, v in d.items() if k not in ("aprovar_ja", "latitude", "longitude", "precisao")}
        dados["colaborador_id"] = colaborador_id
        o = ocorrencias.criar(conn, quem, dados, origem="RESPONSAVEL")
    logger.info("Ponto: pedido %s por outra pessoa (%s) feito por %s", o["id"], colaborador_id, p["id"])
    return _ok(pedido={"id": o["id"], "rotulo": o["rotulo"]}), 201


@bp.route("/app/api/equipe/<int:colaborador_id>/ajuste-do-dia", methods=["POST"])
@auth.exige_colaborador
def app_api_equipe_ajuste(colaborador_id: int):
    d = _corpo()
    with db.conexao() as conn:
        p, a = _contexto(conn)
        if int(colaborador_id) == int(p["id"]):
            raise ErroDeValidacao("o seu próprio ajuste é pedido no “Meu ponto”")
        _alvo(conn, a, colaborador_id, *_datas_do_pedido(d))
        quem = _quem_por_outro(conn, p, a, colaborador_id)
        dados = {k: v for k, v in d.items() if k not in ("aprovar_ja", "latitude", "longitude", "precisao")}
        dados["colaborador_id"] = colaborador_id
        criados = ocorrencias.criar_ajuste_do_dia(conn, quem, dados, origem="RESPONSAVEL")
    return _ok(pedidos=criados, quantidade=len(criados)), 201
