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
                "RESPONSAVEL_EQUIPE": "responsável pelo ponto de equipe", "APARELHO_OBRA": "ponto da obra",
                "APARELHO_EQUIPE": "ponto de equipe"}


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
    """No PONTO DA OBRA, as opções são DO APARELHO e não mudam com quem entrou
    (10/10/2026: "ele está meio que misturando os modos"): bater, entregar
    atestado, consultar pelo CPF, mandar o QR por WhatsApp — e o "Meu ponto" de
    quem entrar. No celular da pessoa, as opções são dela."""
    with db.conexao() as conn:
        a = _aparelho(conn)
        p = _logado(conn)
        da_obra = papeis.aparelho_valendo(a) and a["perfil"] != "INDIVIDUAL"
        if da_obra:
            alc = papeis.alcance_do_aparelho(conn, a, _local())
        else:
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
              "consultar": not alc.vazio, "consultar_pelo_cpf": bool(da_obra),
              "pedir_por_outros": bool(alc.pode_pedir and not alc.vazio),
              "qr_por_cpf": bool(da_obra), "meu_qr": bool(p) and not da_obra},
        alcance=_alcance_json(alc))


# A consulta pelo CPF no ponto da obra: a pessoa é "aberta" pelo CPF e fica
# aberta neste aparelho por alguns minutos (na sessão do navegador) — o número
# dela sozinho não abre nada, para ninguém varrer números e mapear quem existe.
CONSULTA_CPF_MINUTOS = 10
SESSAO_CONSULTA = "ponto_consulta_cpf"


def _consultas_abertas() -> dict:
    import time
    agora = time.time()
    abertas = {int(k): v for k, v in (session.get(SESSAO_CONSULTA) or {}).items()
               if agora - float(v) < CONSULTA_CPF_MINUTOS * 60}
    session[SESSAO_CONSULTA] = {str(k): v for k, v in abertas.items()}
    return abertas


def _abrir_consulta(colaborador_id: int) -> None:
    import time
    abertas = _consultas_abertas()
    abertas[int(colaborador_id)] = time.time()
    session[SESSAO_CONSULTA] = {str(k): v for k, v in abertas.items()}


class _Contexto:
    def __init__(self, pessoa, alcance, aparelho, pelo_aparelho: bool):
        self.pessoa, self.alcance, self.aparelho, self.pelo_aparelho = pessoa, alcance, aparelho, pelo_aparelho


def _contexto(conn) -> _Contexto:
    """Quem consulta, e o que alcança aqui. No PONTO DA OBRA vale o alcance do
    APARELHO, com ou sem alguém logado; no celular da pessoa e no computador,
    o da pessoa (o administrativo). Sem nenhum dos dois, 401."""
    local = _local(_corpo() if request.method == "POST" else None)
    a = _aparelho(conn)
    if papeis.aparelho_valendo(a) and a["perfil"] != "INDIVIDUAL":
        # Todo mundo entra com CPF e PIN (10/10/2026: "todo mundo tem que logar"): no
        # ponto da obra, só quem pode entrar nele (o responsável e o administrativo).
        p = _logado(conn)
        if not p:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        return _Contexto(p, papeis.alcance_do_aparelho(conn, a, local), a, True)
    p = _logado(conn)
    if not p:
        raise NaoAutenticado("entre com o seu CPF e PIN")
    return _Contexto(p, papeis.alcance(conn, p, a, local), a, False)


def _alvo(conn, ctx: _Contexto, colaborador_id: int, *datas) -> dict:
    a = ctx.alcance
    if ctx.pelo_aparelho and int(colaborador_id) not in _consultas_abertas():
        raise NaoEncontrado("pessoa não encontrada — consulte pelo CPF")
    if a.vazio or not papeis.no_alcance(conn, a, colaborador_id, *datas):
        raise NaoEncontrado("pessoa não encontrada")
    alvo = cadastros.colaborador_por_id(conn, colaborador_id)
    if not alvo:
        raise NaoEncontrado("pessoa não encontrada")
    return alvo


@bp.route("/app/api/equipe")
@auth.exige_colaborador_ou_aparelho
def app_api_equipe():
    with db.conexao() as conn:
        ctx = _contexto(conn)
        if ctx.pelo_aparelho:
            raise Recusada("no ponto da obra, a consulta é pelo CPF da pessoa")
        competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
        pessoas = papeis.pessoas_do_alcance(conn, ctx.alcance, competencia, exceto=int(ctx.pessoa["id"]))
    return _ok(alcance=_alcance_json(ctx.alcance), competencia=competencia[:7], pessoas=pessoas)


@bp.route("/app/api/equipe/cpf", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_cpf():
    """A consulta pelo CPF (o ponto da obra; vale também para o administrativo).
    Fora do alcance — quem não bateu nem é da obra em que o aparelho está —
    responde "não encontrada", exista o CPF ou não."""
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        if not auth.dentro_do_limite("consulta_cpf", str((ctx.aparelho or {}).get("id") or auth.ip_de_quem_chama()), 120):
            raise Recusada("muitas consultas neste aparelho; espere alguns minutos")
        if ctx.alcance.vazio:
            raise Recusada(ctx.alcance.sem_alcance or "a consulta vale na obra em que o aparelho está")
        pessoa = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(d.get("cpf")))
        if not pessoa or not papeis.no_alcance(conn, ctx.alcance, int(pessoa["id"])):
            raise NaoEncontrado("ninguém com esse CPF batendo ponto nesta obra")
        _abrir_consulta(int(pessoa["id"]))
    return _ok(id=int(pessoa["id"]), nome=pessoa["nome"])


@bp.route("/app/api/equipe/<int:colaborador_id>/mes")
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_mes(colaborador_id: int):
    from .core import competencias
    with db.conexao() as conn:
        ctx = _contexto(conn)
        competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
        alvo = _alvo(conn, ctx, colaborador_id, competencias.primeiro_dia(competencia))
        esp = espelho.do_mes(conn, colaborador_id, competencia)
    return _ok(pessoa={"id": alvo["id"], "nome": alvo["nome"], "funcao": alvo.get("funcao"),
                       "obra": alvo.get("obra_codigo"), "cpf_final": alvo["cpf"][-3:],
                       "tem_banco": alvo.get("regime_banco") not in (None, "SEM_BANCO")},
               competencia=esp["competencia"], resumo=esp["resumo"], dias=esp["dias"],
               competencia_fechada=esp["competencia_fechada"], pode_pedir=ctx.alcance.pode_pedir,
               obras=list(ctx.alcance.obras.values()), pelo_aparelho=ctx.pelo_aparelho)


def _quem_por_outro(conn, ctx: _Contexto, alvo_id: int) -> tuple[Quem, str]:
    a = ctx.alcance
    if not a.pode_pedir:
        raise Recusada("daqui você consulta, mas não faz pedido por outra pessoa")
    if not papeis.disponivel(conn):
        raise Recusada("aplique as atualizações do ponto (migração 008) para pedir por outra pessoa")
    if ctx.pelo_aparelho:
        nome_ap = (ctx.aparelho.get("descricao") or "ponto da obra")[:60]
        quem_lanca = f" — lançado por {ctx.pessoa['nome']}" if ctx.pessoa else ""
        return (Quem(nome=f"no aparelho {nome_ap}{quem_lanca}"[:120], pessoas={int(alvo_id)},
                     obras=list(a.obras)), "APARELHO")
    rotulo = ROTULO_PAPEL.get(a.papel, "responsável")
    return (Quem(nome=f"{ctx.pessoa['nome']} ({rotulo}, pelo aplicativo)"[:120], pessoas={int(alvo_id)},
                 obras=list(a.obras)), "RESPONSAVEL")


def _datas_do_pedido(d: dict):
    from .core.ocorrencias import _data
    datas = []
    for campo in ("data_inicio", "dia_trabalhado"):
        if d.get(campo):
            datas.append(_data(d[campo], campo))
    for h in d.get("horarios") or []:
        datas.append(_data(str(h)[:10], "horarios"))
    return datas


def _proprio(ctx: _Contexto, colaborador_id: int) -> bool:
    return (not ctx.pelo_aparelho) and ctx.pessoa is not None and int(colaborador_id) == int(ctx.pessoa["id"])


@bp.route("/app/api/equipe/<int:colaborador_id>/pedidos", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_pedir(colaborador_id: int):
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        if _proprio(ctx, colaborador_id):
            raise ErroDeValidacao("o seu próprio pedido é feito no “Meu ponto”")
        _alvo(conn, ctx, colaborador_id, *_datas_do_pedido(d))
        quem, origem = _quem_por_outro(conn, ctx, colaborador_id)
        dados = {k: v for k, v in d.items() if k not in ("aprovar_ja", "latitude", "longitude", "precisao")}
        dados["colaborador_id"] = colaborador_id
        o = ocorrencias.criar(conn, quem, dados, origem=origem)
    logger.info("Ponto: pedido %s por outra pessoa (%s) — %s", o["id"], colaborador_id, quem.nome)
    return _ok(pedido={"id": o["id"], "rotulo": o["rotulo"]}), 201


@bp.route("/app/api/equipe/<int:colaborador_id>/ajuste-do-dia", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_ajuste(colaborador_id: int):
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        if _proprio(ctx, colaborador_id):
            raise ErroDeValidacao("o seu próprio ajuste é pedido no “Meu ponto”")
        _alvo(conn, ctx, colaborador_id, *_datas_do_pedido(d))
        quem, origem = _quem_por_outro(conn, ctx, colaborador_id)
        dados = {k: v for k, v in d.items() if k not in ("aprovar_ja", "latitude", "longitude", "precisao")}
        dados["colaborador_id"] = colaborador_id
        criados = ocorrencias.criar_ajuste_do_dia(conn, quem, dados, origem=origem)
    return _ok(pedidos=criados, quantidade=len(criados)), 201
