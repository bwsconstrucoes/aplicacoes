# -*- coding: utf-8 -*-
"""
O blueprint /ponto — API REST em JSON, no formato da casa (`{'ok': …}`).

As rotas só leem a entrada, chamam o `core` e formatam a saída. Regra de
negócio mora em `core/`, nunca aqui. Toda rota declara o que exige
(`auth.py`); a que esquecer é recusada pelo guarda.

Sem `url_prefix` no `main.py`: as rotas já trazem `/ponto`, como o ERP, o
painel e a Análise de SPs.
"""
from __future__ import annotations

import logging

from flask import Blueprint, g, jsonify, request

from . import auth, db, horario, migracoes_runner
from .core import cadastros, consultas, dispositivos, marcacoes, recusas
from .erros import ErroDoPonto

logger = logging.getLogger("ponto.routes")

bp = Blueprint("ponto", __name__, url_prefix="/ponto")
bp.before_request(auth.exigir_credencial)


# ---------------------------------------------------------------------------
# Respostas
# ---------------------------------------------------------------------------
def _ok(**dados):
    return jsonify({"ok": True, **dados})


def _erro(mensagem: str, status: int, **detalhes):
    corpo = {"ok": False, "erro": mensagem}
    if detalhes:
        corpo.update({k: v for k, v in detalhes.items() if v is not None})
    return jsonify(corpo), status


@bp.errorhandler(ErroDoPonto)
def _erro_do_ponto(e: ErroDoPonto):
    return _erro(e.mensagem, e.status, **e.detalhes)


@bp.errorhandler(Exception)
def _erro_inesperado(e: Exception):
    logger.exception("Ponto: erro inesperado em %s", request.path)
    return _erro("erro interno; o detalhe está no log do servidor", 500)


def _corpo() -> dict:
    dados = request.get_json(silent=True)
    if dados is None and request.form:
        dados = request.form.to_dict()
    if not isinstance(dados, dict):
        return {}
    return dados



# ---------------------------------------------------------------------------
# Saúde
# ---------------------------------------------------------------------------
@bp.route("/health", methods=["GET"])
@auth.publica("diz se o módulo subiu; não revela dado nenhum")
def health():
    estado = {"ok": True, "modulo": "ponto", "fuso": horario.NOME_DO_FUSO,
              "agora": horario.texto(horario.agora()),
              "chave_configurada": bool(auth.chave_configurada())}
    try:
        migracoes = migracoes_runner.listar_estado()
        estado["migracoes_pendentes"] = len(migracoes["pendentes"])
        estado["banco"] = "ok"
    except Exception as e:  # noqa: BLE001 — health não derruba, informa
        estado["banco"] = f"indisponível: {type(e).__name__}"
    return jsonify(estado)


# ---------------------------------------------------------------------------
# Aparelhos
# ---------------------------------------------------------------------------
@bp.route("/api/dispositivo/registrar", methods=["POST"])
@auth.publica("o aparelho ainda não tem credencial — entra como PENDENTE, com teto por IP")
def registrar_dispositivo():
    ip = auth.ip_de_quem_chama()
    if not auth.registro_permitido(ip):
        logger.warning("Ponto: teto de registros de aparelho atingido para %s", ip)
        return _erro("muitos registros deste endereço; tente mais tarde", 429)
    dados = _corpo()
    with db.conexao() as conn:
        aparelho, token = dispositivos.registrar(
            conn, device_uuid=dados.get("device_uuid"), descricao=dados.get("descricao", ""),
            user_agent=request.headers.get("User-Agent", ""), ip=ip)
    resposta = {"dispositivo": dispositivos.para_json(aparelho), "novo": token is not None}
    if token:
        resposta["token"] = token
        resposta["aviso"] = "guarde o token: ele não é mostrado de novo"
    return _ok(**resposta), (201 if token else 200)


@bp.route("/api/dispositivos", methods=["GET"])
@auth.exige_chave
def listar_dispositivos():
    with db.conexao() as conn:
        lista = dispositivos.listar(conn, request.args.get("status"))
    return _ok(dispositivos=[dispositivos.para_json(d) for d in lista], quantidade=len(lista))


@bp.route("/api/dispositivos/<int:dispositivo_id>/aprovar", methods=["POST"])
@auth.exige_chave
def aprovar_dispositivo(dispositivo_id: int):
    dados = _corpo()
    with db.conexao() as conn:
        colaborador_id = None
        if dados.get("cpf"):
            pessoa = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(dados["cpf"]))
            if not pessoa:
                return _erro("pessoa não cadastrada", 400, campo="cpf")
            colaborador_id = int(pessoa["id"])
        elif dados.get("colaborador_id"):
            colaborador_id = int(dados["colaborador_id"])
        autorizados = _ids_de_pessoas(conn, dados.get("autorizados"))
        obras = _ids_de_obras(conn, dados.get("obras"))
        aparelho = dispositivos.aprovar(
            conn, dispositivo_id, perfil=dados.get("perfil", "COMPARTILHADO"),
            aprovado_por=dados.get("aprovado_por", ""), colaborador_id=colaborador_id,
            descricao=dados.get("descricao"), autorizados=autorizados, obras=obras)
    return _ok(dispositivo=dispositivos.para_json(aparelho))


@bp.route("/api/dispositivos/<int:dispositivo_id>/bloquear", methods=["POST"])
@auth.exige_chave
def bloquear_dispositivo(dispositivo_id: int):
    dados = _corpo()
    with db.conexao() as conn:
        aparelho = dispositivos.bloquear(conn, dispositivo_id, motivo=dados.get("motivo", ""),
                                         por=dados.get("por", ""))
    return _ok(dispositivo=dispositivos.para_json(aparelho))


@bp.route("/api/dispositivos/<int:dispositivo_id>/autorizar", methods=["POST"])
@auth.exige_chave
def autorizar_dispositivo(dispositivo_id: int):
    """Troca a lista de pessoas e/ou de obras do aparelho. Só mexe no que veio."""
    dados = _corpo()
    with db.conexao() as conn:
        dispositivos.por_id(conn, dispositivo_id)
        if "autorizados" in dados:
            dispositivos.definir_autorizados(conn, dispositivo_id,
                                             _ids_de_pessoas(conn, dados.get("autorizados")))
        if "obras" in dados:
            dispositivos.definir_obras(conn, dispositivo_id, _ids_de_obras(conn, dados.get("obras")))
        aparelho = dispositivos.detalhado(conn, dispositivo_id)
    return _ok(dispositivo=dispositivos.para_json(aparelho))


def _ids_de_pessoas(conn, valores) -> list[int]:
    """Aceita CPFs ou números de colaborador, misturados."""
    from .erros import ErroDeValidacao
    if valores is None:
        return []
    if isinstance(valores, str):
        valores = [p for p in valores.replace(";", ",").split(",") if p.strip()]
    ids = []
    for v in valores:
        texto = str(v).strip()
        digitos = "".join(ch for ch in texto if ch.isdigit())
        if len(digitos) == 11:
            pessoa = cadastros.colaborador_por_cpf(conn, digitos)
            if not pessoa:
                raise ErroDeValidacao(f"pessoa não cadastrada: CPF {texto}", campo="autorizados")
            ids.append(int(pessoa["id"]))
        elif digitos and digitos == texto:
            if not cadastros.colaborador_por_id(conn, int(digitos)):
                raise ErroDeValidacao(f"pessoa não cadastrada: {texto}", campo="autorizados")
            ids.append(int(digitos))
        else:
            raise ErroDeValidacao(f"identificador de pessoa inválido: {texto!r}", campo="autorizados")
    return ids


def _ids_de_obras(conn, valores) -> list[int]:
    """Aceita número, código ou nome exato da obra."""
    from .erros import ErroDeValidacao
    if valores is None:
        return []
    if isinstance(valores, str):
        valores = [p for p in valores.replace(";", ",").split(",") if p.strip()]
    ids = []
    for v in valores:
        obra = cadastros.resolver_obra(conn, v)
        if not obra:
            raise ErroDeValidacao(f"obra não cadastrada: {v!r}", campo="obras")
        ids.append(int(obra["id"]))
    return ids


# ---------------------------------------------------------------------------
# A batida
# ---------------------------------------------------------------------------
@bp.route("/api/marcacao", methods=["POST"])
@auth.exige_aparelho_ou_chave
def registrar_marcacao():
    dados = _corpo()
    via_chave = g.get("ponto_via") == "chave"
    with db.conexao() as conn:
        marcacao, repetida = marcacoes.registrar(
            conn, cpf=dados.get("cpf"), obra=dados.get("obra") or dados.get("obra_id"),
            origem=dados.get("origem", "PWA"), device_uuid=dados.get("device_uuid"),
            device_token=request.headers.get(auth.CABECALHO_TOKEN), via_chave=via_chave,
            latitude=dados.get("latitude"), longitude=dados.get("longitude"),
            timestamp_dispositivo=dados.get("timestamp_dispositivo"),
            foto_base64=dados.get("foto_base64"), registrado_por=dados.get("registrado_por"),
            ip=auth.ip_de_quem_chama())
    return _ok(marcacao=marcacoes.para_json(marcacao), repetida=repetida), (200 if repetida else 201)


@bp.route("/api/ajustes", methods=["POST"])
@auth.exige_chave
def solicitar_ajuste():
    dados = _corpo()
    with db.conexao() as conn:
        ajuste = marcacoes.solicitar_ajuste(
            conn, cpf=dados.get("cpf"), data_referencia=dados.get("data_referencia"),
            tipo=dados.get("tipo"), justificativa=dados.get("justificativa", ""),
            solicitado_por=dados.get("solicitado_por", ""), marcacao_id=dados.get("marcacao_id"),
            horario_proposto=dados.get("horario_proposto"), obra_proposta=dados.get("obra_proposta"))
    ajuste["data_referencia"] = ajuste["data_referencia"].isoformat()
    for campo in ("horario_proposto", "decidido_em", "criado_em"):
        ajuste[campo] = horario.texto(ajuste.get(campo))
    return _ok(ajuste=ajuste), 201


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------
@bp.route("/api/marcacoes", methods=["GET"])
@auth.exige_chave
def listar_marcacoes():
    a = request.args
    with db.conexao() as conn:
        resultado = consultas.listar(conn, cpf=a.get("cpf"), data_inicio=a.get("data_inicio"),
                                     data_fim=a.get("data_fim"), obra=a.get("obra"),
                                     status=a.get("status"))
    return _ok(**resultado)


@bp.route("/api/colaboradores", methods=["GET"])
@auth.exige_chave
def listar_colaboradores():
    a = request.args
    with db.conexao() as conn:
        obra_id = None
        if a.get("obra"):
            obra = cadastros.resolver_obra(conn, a.get("obra"))
            if not obra:
                return _erro("obra não cadastrada", 400, campo="obra")
            obra_id = int(obra["id"])
        pessoas = cadastros.listar_colaboradores(
            conn, so_ativos=a.get("todos", "").lower() not in ("1", "true", "sim"), obra_id=obra_id)
    return _ok(colaboradores=[cadastros.colaborador_para_json(p) for p in pessoas],
               quantidade=len(pessoas))


@bp.route("/api/obras", methods=["GET"])
@auth.exige_chave
def listar_obras():
    with db.conexao() as conn:
        obras = cadastros.listar_obras(
            conn, so_ativas=request.args.get("todas", "").lower() not in ("1", "true", "sim"))
    return _ok(obras=[cadastros.obra_para_json(o) for o in obras], quantidade=len(obras))


@bp.route("/api/recusas", methods=["GET"])
@auth.exige_chave
def listar_recusas():
    with db.conexao() as conn:
        lista = recusas.listar(conn, limite=request.args.get("limite", 200))
    for r in lista:
        r["criado_em"] = horario.texto(r["criado_em"])
    return _ok(recusas=lista, quantidade=len(lista))


# ---------------------------------------------------------------------------
# Administração
# ---------------------------------------------------------------------------
@bp.route("/api/admin/migracoes", methods=["GET"])
@auth.exige_chave
def estado_das_migracoes():
    return _ok(**migracoes_runner.listar_estado())


@bp.route("/api/admin/migrar", methods=["POST"])
@auth.exige_chave
def aplicar_migracoes():
    resultado = migracoes_runner.aplicar_pendentes()
    return _ok(**resultado), (200 if not resultado["erro"] else 500)
