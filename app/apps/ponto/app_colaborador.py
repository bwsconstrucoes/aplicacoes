# -*- coding: utf-8 -*-
"""
O "MEU PONTO" — o aplicativo do colaborador no celular, em /ponto/app.

Pedido do dono, 03/10/2026: "um ambiente mais simples, para poder ser
utilizável no celular, e a pessoa que bateu o ponto poder consultar o seu ponto,
verificar a frequência, se tem falta, se tem atestado… lançar um atestado é
pelo ponto: você anexa e encaminha."

DOIS MODOS NO MESMO ENDEREÇO:
  · CELULAR DA PESSOA — entra com CPF + PIN (criado com código pelo WhatsApp).
    Bate ponto, vê o próprio mês, manda atestado, pede ajuste e compensação,
    acompanha os pedidos e vê o comprovante de cada batida.
  · TABLET DA OBRA — sem login. Só bate: CPF + foto. Não mostra nada de
    ninguém, porque o aparelho é de todos.

O APARELHO continua precisando de aprovação na gestão (fase 1): o celular gera
o identificador dele, se registra e espera. A batida só passa por aparelho
APROVADO — e o celular de uma pessoa (perfil INDIVIDUAL) só bate para ela.

Tudo o que o colaborador vê sai do MESMO cálculo da gestão (`core/espelho.py`):
não há um número no celular e outro no ERP.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os

from flask import Response, g, jsonify, render_template, request, send_from_directory, session

from . import auth, db, horario
from .core import (banco, cadastros, competencias, dispositivos, espelho, marcacoes, acesso,
                   ocorrencias)
from .core.ocorrencias import Quem
from .erros import ErroDeValidacao, NaoAutenticado, NaoEncontrado
from .routes import bp

logger = logging.getLogger("ponto.app")

ENTRADAS_POR_HORA_POR_IP = 60
CODIGOS_POR_HORA_POR_IP = 20
PASTA_ESTATICA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def _ok(**dados):
    return jsonify({"ok": True, **dados})


def _corpo() -> dict:
    dados = request.get_json(silent=True)
    return dados if isinstance(dados, dict) else {}


def _eu() -> int:
    return int(g.ponto_colaborador_id)


def _quem(conn) -> Quem:
    p = cadastros.colaborador_por_id(conn, _eu())
    if not p or p["situacao"] == "DESLIGADO":
        session.pop(auth.SESSAO_COLABORADOR, None)
        raise NaoAutenticado("entre com o seu CPF e PIN")
    return Quem(nome=f"{p['nome']} (pelo celular)", colaborador_id=p["id"])


def _abrir_sessao(pessoa: dict) -> None:
    session.permanent = True
    session[auth.SESSAO_COLABORADOR] = int(pessoa["id"])
    session[auth.SESSAO_NOME] = pessoa["nome"]


# ---------------------------------------------------------------------------
# A página, o manifesto e o service worker
# ---------------------------------------------------------------------------
@bp.route("/app")
@auth.publica("a tela do aplicativo; sem dado nenhum até a pessoa entrar")
def app_pagina():
    return render_template("ponto/app.html")


@bp.route("/app/manifest.webmanifest")
@auth.publica("manifesto do aplicativo instalável")
def app_manifesto():
    manifesto = {
        "name": "BWS Ponto", "short_name": "Ponto", "start_url": "/ponto/app",
        "scope": "/ponto/app", "display": "standalone", "background_color": "#0A1B2E",
        "theme_color": "#12385C", "lang": "pt-BR",
        "icons": [{"src": "/erp/static/icone-192.png", "sizes": "192x192", "type": "image/png"},
                  {"src": "/erp/static/icone-512.png", "sizes": "512x512", "type": "image/png"},
                  {"src": "/erp/static/icone-512-recortavel.png", "sizes": "512x512",
                   "type": "image/png", "purpose": "maskable"}],
    }
    return Response(json.dumps(manifesto, ensure_ascii=False), mimetype="application/manifest+json")


@bp.route("/app/sw.js")
@auth.publica("service worker do aplicativo (só guarda a tela, nunca dado)")
def app_service_worker():
    resposta = send_from_directory(PASTA_ESTATICA, "sw.js", mimetype="application/javascript")
    resposta.headers["Service-Worker-Allowed"] = "/ponto/app"
    resposta.headers["Cache-Control"] = "no-cache"
    return resposta


# ---------------------------------------------------------------------------
# Entrar, criar PIN, sair
# ---------------------------------------------------------------------------
@bp.route("/app/api/codigo", methods=["POST"])
@auth.publica("pedir o código de acesso por WhatsApp; resposta igual exista o CPF ou não")
def app_api_codigo():
    if not auth.dentro_do_limite("codigo", auth.ip_de_quem_chama(), CODIGOS_POR_HORA_POR_IP):
        return jsonify({"ok": False, "erro": "muitos pedidos deste lugar; tente mais tarde"}), 429
    with db.conexao() as conn:
        r = acesso.pedir_codigo(conn, _corpo().get("cpf"), ip=auth.ip_de_quem_chama())
    return _ok(**r)


@bp.route("/app/api/pin", methods=["POST"])
@auth.publica("criar ou trocar o PIN com o código do WhatsApp")
def app_api_pin():
    if not auth.dentro_do_limite("entrar", auth.ip_de_quem_chama(), ENTRADAS_POR_HORA_POR_IP):
        return jsonify({"ok": False, "erro": "muitas tentativas deste lugar; tente mais tarde"}), 429
    d = _corpo()
    with db.conexao() as conn:
        pessoa = acesso.criar_pin(conn, d.get("cpf"), d.get("codigo"), d.get("pin"))
    _abrir_sessao(pessoa)
    return _ok(nome=pessoa["nome"])


@bp.route("/app/api/entrar", methods=["POST"])
@auth.publica("entrar com CPF e PIN")
def app_api_entrar():
    if not auth.dentro_do_limite("entrar", auth.ip_de_quem_chama(), ENTRADAS_POR_HORA_POR_IP):
        return jsonify({"ok": False, "erro": "muitas tentativas deste lugar; tente mais tarde"}), 429
    d = _corpo()
    with db.conexao() as conn:
        pessoa = acesso.entrar(conn, d.get("cpf"), d.get("pin"))
    _abrir_sessao(pessoa)
    return _ok(nome=pessoa["nome"])


@bp.route("/app/api/sair", methods=["POST"])
@auth.publica("sair sempre pode — apagar a própria sessão não revela nada")
def app_api_sair():
    session.pop(auth.SESSAO_COLABORADOR, None)
    session.pop(auth.SESSAO_NOME, None)
    return _ok()


# ---------------------------------------------------------------------------
# O aparelho
# ---------------------------------------------------------------------------
@bp.route("/app/api/aparelho")
@auth.publica("o próprio aparelho pergunta como está; exige o token dele")
def app_api_aparelho():
    uuid = request.headers.get("X-Device-UUID", "")
    token = request.headers.get(auth.CABECALHO_TOKEN, "")
    if not uuid or not token:
        return _ok(aparelho=None)
    with db.conexao() as conn:
        try:
            a = dispositivos.autenticar(conn, uuid, token)
        except Exception:  # noqa: BLE001 — uuid desconhecido ou token velho
            return _ok(aparelho=None)
        obras = dispositivos.obras_de(conn, a["id"])
        dono = cadastros.colaborador_por_id(conn, a["colaborador_id"]) if a["colaborador_id"] else None
        lista_obras = [cadastros.obra_para_json(o) for o in cadastros.listar_obras(conn)
                       if not obras or o["id"] in obras]
    return _ok(aparelho={"status": a["status"], "perfil": a["perfil"], "descricao": a["descricao"],
                         "dono": (dono["nome"].split(" ")[0] if dono else None),
                         "obras": lista_obras})


@bp.route("/app/api/aparelho/identificar", methods=["POST"])
@auth.exige_colaborador
def app_api_aparelho_identificar():
    """O celular que espera aprovação passa a dizer de quem é. Só enquanto
    PENDENTE: aparelho aprovado não muda de dono pelo celular."""
    uuid = request.headers.get("X-Device-UUID", "")
    token = request.headers.get(auth.CABECALHO_TOKEN, "")
    with db.conexao() as conn:
        try:
            a = dispositivos.autenticar(conn, uuid, token)
        except Exception:  # noqa: BLE001
            raise NaoEncontrado("aparelho não encontrado")
        if a["status"] != "PENDENTE":
            return _ok(mudou=False)
        p = cadastros.colaborador_por_id(conn, _eu())
        db.executar(conn, "UPDATE ponto.dispositivos SET descricao = :d, colaborador_id = :c "
                          "WHERE id = :id AND status = 'PENDENTE'",
                    d=f"Celular de {p['nome']} (CPF final {p['cpf'][-3:]})"[:200], c=p["id"],
                    id=a["id"])
    return _ok(mudou=True)


# ---------------------------------------------------------------------------
# Eu, hoje, o mês
# ---------------------------------------------------------------------------
def _obras_da_pessoa(conn, colaborador_id: int) -> list[dict]:
    ids = cadastros.obras_da_pessoa(conn, colaborador_id)
    return [cadastros.obra_para_json(o) for o in cadastros.listar_obras(conn) if o["id"] in ids]


@bp.route("/app/api/eu")
@auth.exige_colaborador
def app_api_eu():
    with db.conexao() as conn:
        quem = _quem(conn)
        p = cadastros.colaborador_por_id(conn, quem.colaborador_id)
        hoje = horario.hoje()
        esp = espelho.montar(conn, p["id"], hoje, hoje)
        pedidos = ocorrencias.listar(conn, quem, status="PENDENTES")
        saldo = banco.saldo(conn, p["id"]) if p.get("regime_banco") not in (None, "SEM_BANCO") else None
        obras = _obras_da_pessoa(conn, p["id"])
    return _ok(nome=p["nome"], primeiro_nome=p["nome"].split(" ")[0],
               cpf_final=p["cpf"][-3:], obra_principal=p.get("obra_codigo"),
               obras=obras, hoje=esp["dias"][0], pedidos_pendentes=len(pedidos),
               banco=({"saldo": saldo["saldo"], "regime": saldo["regime_rotulo"]} if saldo else None))


@bp.route("/app/api/mes")
@auth.exige_colaborador
def app_api_mes():
    with db.conexao() as conn:
        quem = _quem(conn)
        esp = espelho.do_mes(conn, quem.colaborador_id, request.args.get("competencia"))
    return _ok(competencia=esp["competencia"], resumo=esp["resumo"], dias=esp["dias"],
               competencia_fechada=esp["competencia_fechada"])


@bp.route("/app/api/banco")
@auth.exige_colaborador
def app_api_banco():
    with db.conexao() as conn:
        quem = _quem(conn)
        s = banco.saldo(conn, quem.colaborador_id)
    return _ok(**s)


# ---------------------------------------------------------------------------
# Bater
# ---------------------------------------------------------------------------
@bp.route("/app/api/bater", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_bater():
    """No celular da pessoa, o CPF é o da sessão — não se bate por outro. No
    tablet da obra (sem sessão), o CPF vem digitado e o aparelho tem de ser
    compartilhado ou de lista (a regra do aparelho decide)."""
    d = _corpo()
    with db.conexao() as conn:
        if g.get("ponto_via") == "colaborador":
            p = cadastros.colaborador_por_id(conn, _eu())
            if not p:
                raise NaoAutenticado("entre com o seu CPF e PIN")
            cpf = p["cpf"]
        else:
            cpf = d.get("cpf")
        marcacao, repetida = marcacoes.registrar(
            conn, cpf=cpf, obra=d.get("obra"), origem="PWA",
            device_uuid=request.headers.get("X-Device-UUID") or d.get("device_uuid"),
            device_token=request.headers.get(auth.CABECALHO_TOKEN), via_chave=False,
            latitude=d.get("latitude"), longitude=d.get("longitude"),
            timestamp_dispositivo=d.get("timestamp_dispositivo"),
            foto_base64=d.get("foto_base64"), ip=auth.ip_de_quem_chama())
        comprovante = _comprovante(conn, marcacao["id"])
    return _ok(repetida=repetida, comprovante=comprovante,
               status=marcacao["status"], motivo_analise=marcacao.get("motivo_analise")), \
        (200 if repetida else 201)


def _comprovante(conn, marcacao_id: int) -> dict:
    """O comprovante da batida (Portaria 671/2021): empregador, local, NSR,
    data e hora, empregado. Sai na hora da batida e fica consultável depois."""
    m = db.um(conn, """
        SELECT m.nsr, m.timestamp_servidor, m.status, m.hash_encadeado, m.origem,
               c.nome, c.cpf, o.codigo AS obra_codigo, o.nome AS obra_nome, o.municipio, o.uf,
               e.razao_social, e.cnpj
          FROM ponto.marcacoes m
          JOIN public.colaboradores c ON c.id = m.colaborador_id
          JOIN public.obras o ON o.id = m.obra_id
          LEFT JOIN public.empresas e ON e.id = o.empresa_id
         WHERE m.id = :id
    """, id=marcacao_id)
    cpf = m["cpf"]
    return {
        "nsr": m["nsr"], "data_hora": horario.texto(m["timestamp_servidor"]),
        "data": horario.para_local(m["timestamp_servidor"]).strftime("%d/%m/%Y"),
        "hora": horario.para_local(m["timestamp_servidor"]).strftime("%H:%M:%S"),
        "empregado": m["nome"], "cpf": f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}",
        "empregador": m["razao_social"] or "BWS Construções", "cnpj": m["cnpj"],
        "local": f"{m['obra_codigo']} — {m['obra_nome']}"
                 + (f" ({m['municipio']}/{m['uf']})" if m["municipio"] else ""),
        "situacao": m["status"], "registro": "REP-P (ponto por programa)",
        "codigo_de_verificacao": m["hash_encadeado"][:16].upper(),
    }


@bp.route("/app/api/comprovante/<int:marcacao_id>")
@auth.exige_colaborador
def app_api_comprovante(marcacao_id: int):
    with db.conexao() as conn:
        m = db.um(conn, "SELECT colaborador_id FROM ponto.marcacoes WHERE id = :id", id=marcacao_id)
        if not m or m["colaborador_id"] != _eu():
            raise NaoEncontrado("batida não encontrada")
        c = _comprovante(conn, marcacao_id)
    return _ok(comprovante=c)


# ---------------------------------------------------------------------------
# Pedidos: atestado, ajuste, compensação, folga
# ---------------------------------------------------------------------------
@bp.route("/app/api/pedidos")
@auth.exige_colaborador
def app_api_pedidos():
    with db.conexao() as conn:
        quem = _quem(conn)
        lista = ocorrencias.listar(conn, quem, limite=100)
    return _ok(pedidos=lista)


@bp.route("/app/api/pedidos", methods=["POST"])
@auth.exige_colaborador
def app_api_pedir():
    d = _corpo()
    d["colaborador_id"] = _eu()
    d.pop("aprovar_ja", None)
    with db.conexao() as conn:
        quem = _quem(conn)
        o = ocorrencias.criar(conn, quem, d, origem="APP")
    return _ok(pedido=o), 201


@bp.route("/app/api/pedidos/<int:ocorrencia_id>/cancelar", methods=["POST"])
@auth.exige_colaborador
def app_api_cancelar(ocorrencia_id: int):
    with db.conexao() as conn:
        quem = _quem(conn)
        o = ocorrencias.cancelar(conn, ocorrencia_id, quem)
    return _ok(pedido=o)
