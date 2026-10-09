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
  · TABLET DA OBRA — sem login, câmera sempre ligada. A pessoa mostra o QR
    Code no próprio celular OU digita o CPF no teclado grande da tela — sem
    trocar de modo (decisão do dono, 03/10/2026: "deixar só o CPF e QR Code").
    A tela confirma nome e função e tira a foto sozinha. Não mostra nada de
    ninguém além disso, porque o aparelho é de todos.

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
from .core import (banco, cadastros, competencias, dispositivos, envios, espelho, forma_de_bater,
                   fotos, marcacoes, acesso, ocorrencias, papeis, qr, recusas)
from .core.ocorrencias import Quem
from .erros import ErroDeValidacao, NaoAutenticado, NaoEncontrado, Recusada
from .routes import bp

logger = logging.getLogger("ponto.app")

ENTRADAS_POR_HORA_POR_IP = 60
CODIGOS_POR_HORA_POR_IP = 20
# Tablet: uma obra de 80 pessoas faz 80 identificações em 15 minutos às 7h; o
# teto é folgado para isso e baixo para quem quisesse varrer CPFs.
IDENTIFICACOES_POR_HORA_POR_APARELHO = 400
PEDIDOS_DE_QR_POR_HORA_POR_APARELHO = 30
RESPOSTA_DO_QR = ("Se o CPF estiver cadastrado com telefone, o QR Code chega no WhatsApp "
                  "em instantes.")
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
    if not p or p["situacao"] == "DESLIGADO" or not p["ativo_no_ponto"]:
        session.pop(auth.SESSAO_COLABORADOR, None)
        raise NaoAutenticado("entre com o seu CPF e PIN")
    return Quem(nome=f"{p['nome']} (pelo celular)", colaborador_id=p["id"])


def _dar_nome_ao_aparelho(conn, aparelho, pessoa: dict) -> None:
    """O celular que espera aprovação passa a dizer de quem é, NA ENTRADA com CPF
    e PIN (09/10/2026: "chega aparelho sem nome (…) devíamos aproveitar o
    cadastro, visto que é colocado o CPF"). Só enquanto PENDENTE: aparelho
    aprovado não muda de dono pelo celular."""
    if not aparelho or aparelho.get("status") != "PENDENTE":
        return
    db.executar(conn, "UPDATE ponto.dispositivos SET descricao = :d, colaborador_id = :c "
                      "WHERE id = :id AND status = 'PENDENTE'",
                d=f"Celular de {pessoa['nome']} (CPF final {pessoa['cpf'][-3:]})"[:200], c=pessoa["id"],
                id=aparelho["id"])


def _aparelho_que_chama(conn):
    uuid, token = request.headers.get("X-Device-UUID", ""), request.headers.get(auth.CABECALHO_TOKEN, "")
    if not uuid or not token:
        return None
    try:
        return dispositivos.autenticar(conn, uuid, token)
    except Exception:  # noqa: BLE001 — aparelho desconhecido: segue como celular comum
        return None


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


@bp.route("/app/jsQR.js")
@auth.publica("o leitor de QR Code do tablet (biblioteca aberta, Apache 2.0)")
def app_jsqr():
    resposta = send_from_directory(PASTA_ESTATICA, "jsQR.js", mimetype="application/javascript")
    resposta.headers["Cache-Control"] = "public, max-age=604800"
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
        a = _aparelho_que_chama(conn)
        if not (a and a["status"] == "APROVADO" and a["perfil"] != "INDIVIDUAL"):
            _dar_nome_ao_aparelho(conn, a, pessoa)
    _abrir_sessao(pessoa)
    return _ok(nome=pessoa["nome"])


@bp.route("/app/api/entrar", methods=["POST"])
@auth.publica("entrar com CPF e PIN")
def app_api_entrar():
    if not auth.dentro_do_limite("entrar", auth.ip_de_quem_chama(), ENTRADAS_POR_HORA_POR_IP):
        return jsonify({"ok": False, "erro": "muitas tentativas deste lugar; tente mais tarde"}), 429
    d = _corpo()
    with db.conexao() as conn:
        # No aparelho da obra ninguém entra com CPF e PIN: com a sessão aberta,
        # o tablet deixaria de ser da obra e mostraria o mês daquela pessoa a
        # quem passasse na frente.
        a = _aparelho_que_chama(conn)
        pessoa = acesso.entrar(conn, d.get("cpf"), d.get("pin"))
        # No ponto da obra (ou de equipe), só o RESPONSÁVEL entra no "Meu ponto"
        # (pedido do dono, 07/10/2026: "se ela quiser acessar o ponto dela,
        # particular, ela não consegue"). Os outros, cada um pelo seu celular.
        # Desde 08/10/2026 o administrativo de obra também entra (core/papeis.py):
        # ele consulta e pede pelas pessoas da obra em que o aparelho está.
        if a and a["status"] == "APROVADO" and a["perfil"] != "INDIVIDUAL" \
                and a.get("colaborador_id") != pessoa["id"] and not papeis.e_administrativo(conn, pessoa):
            raise Recusada("este é o ponto da obra — só o responsável por ele (ou o administrativo da obra) "
                           "entra aqui")
        _dar_nome_ao_aparelho(conn, a, pessoa)
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
                         "vencimento": dispositivos.vencimento(a) if a["status"] == "APROVADO" else None,
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
        _dar_nome_ao_aparelho(conn, a, cadastros.colaborador_por_id(conn, _eu()))
    return _ok(mudou=True)


# ---------------------------------------------------------------------------
# Eu, hoje, o mês
# ---------------------------------------------------------------------------
def _obras_da_pessoa(conn, colaborador_id: int) -> list[dict]:
    ids = cadastros.obras_da_pessoa(conn, colaborador_id)
    # Com o que a obra faz fora da cerca (bloquear ou mandar para conferência):
    # a tela só oferece "bater escolhendo a obra" quando a batida vai ser aceita
    # (09/10/2026 — a tela prometia conferência e o servidor recusava).
    return [{**cadastros.obra_para_json(o), "fora_da_cerca": marcacoes.modo_fora_da_cerca(conn, int(o["id"]))}
            for o in cadastros.listar_obras(conn) if o["id"] in ids]


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
        no_celular = forma_de_bater.pode_no_celular(p, forma_de_bater.em_vigor(conn))
        pede = papeis.pede_pelo_proprio_celular(conn, p)
        administrativo = papeis.e_administrativo(conn, p)
    return _ok(nome=p["nome"], primeiro_nome=p["nome"].split(" ")[0], bate_no_celular=no_celular,
               pede_no_celular=pede, administrativo=administrativo,
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
    tablet da obra (sem sessão), quem bate é quem o tablet IDENTIFICOU: vem o
    bilhete de `/app/api/tablet/identificar` (QR ou CPF), que vale 2 minutos e
    só neste aparelho. O CPF solto continua aceito, para o tablet de versão
    anterior que ainda estiver aberto em alguma obra."""
    d = _corpo()
    identificacao = None
    with db.conexao() as conn:
        if g.get("ponto_via") == "colaborador":
            p = cadastros.colaborador_por_id(conn, _eu())
            if not p:
                raise NaoAutenticado("entre com o seu CPF e PIN")
            recusa = forma_de_bater.recusa_no_celular(p, forma_de_bater.em_vigor(conn))
            if recusa:
                _recusar_em_separado("celular próprio sem exceção cadastrada", cpf=p["cpf"], obra=d.get("obra"))
                raise Recusada(recusa)
            cpf, identificacao = p["cpf"], "SESSAO"
        elif d.get("bilhete"):
            aparelho = _aparelho_da_obra(conn)
            if qr.bilhete_de_pedido(d["bilhete"]):
                raise Recusada("esta identificação é de pedido — para bater, identifique-se de novo")
            colaborador_id, identificacao = qr.conferir_bilhete(d["bilhete"], aparelho["id"])
            p = cadastros.colaborador_por_id(conn, colaborador_id)
            if not p:
                raise ErroDeValidacao("identifique-se de novo", campo="bilhete")
            cpf = p["cpf"]
        else:
            cpf = d.get("cpf")
        marcacao, repetida = marcacoes.registrar(
            conn, cpf=cpf, obra=d.get("obra"), origem="PWA",
            device_uuid=request.headers.get("X-Device-UUID") or d.get("device_uuid"),
            device_token=request.headers.get(auth.CABECALHO_TOKEN), via_chave=False,
            latitude=d.get("latitude"), longitude=d.get("longitude"), precisao=d.get("precisao"),
            timestamp_dispositivo=d.get("timestamp_dispositivo"),
            foto_base64=d.get("foto_base64"), ip=auth.ip_de_quem_chama(),
            identificacao=identificacao, justificativa=d.get("justificativa"))
        comprovante = _comprovante(conn, marcacao["id"])
    fotos.disparar_envio()
    return _ok(repetida=repetida, comprovante=comprovante,
               status=marcacao["status"], motivo_analise=marcacao.get("motivo_analise")), \
        (200 if repetida else 201)


# ---------------------------------------------------------------------------
# O tablet da obra: identificar por QR ou CPF, e "esqueci meu QR"
# ---------------------------------------------------------------------------
def _aparelho_da_obra(conn) -> dict:
    """O aparelho que chama, conferido: aprovado e de obra (compartilhado ou de
    lista). Celular de uma pessoa não identifica os outros."""
    try:
        a = dispositivos.autenticar(conn, request.headers.get("X-Device-UUID", ""),
                                    request.headers.get(auth.CABECALHO_TOKEN))
    except Exception:  # noqa: BLE001 — uuid desconhecido ou token velho
        raise NaoAutenticado("aparelho não reconhecido — peça para a gestão aprovar de novo")
    if a["status"] != "APROVADO":
        raise Recusada(f"aparelho {a['status'].lower()}")
    if a["perfil"] == "INDIVIDUAL":
        raise Recusada("este é o celular de uma pessoa, não o aparelho da obra")
    v = dispositivos.vencimento(a)
    if v and v["vencido"]:
        raise Recusada(f"a liberação deste aparelho venceu em {dt.date.fromisoformat(v['valido_ate']):%d/%m/%Y} "
                       "— peça a renovação ao RH")
    return a


def _recusar_em_separado(motivo: str, *, cpf: str | None, obra, **detalhes) -> None:
    """A recusa sobrevive ao erro que vem depois (mesma regra da batida)."""
    with db.conexao() as separada:
        recusas.registrar(separada, motivo=motivo, device_uuid=request.headers.get("X-Device-UUID"),
                          cpf=cpf, obra=obra, origem="PWA", ip=auth.ip_de_quem_chama(), **detalhes)


@bp.route("/app/api/tablet/identificar", methods=["POST"])
@auth.exige_aparelho
def app_api_tablet_identificar():
    """Quem está na frente do tablet: pelo QR Code que a câmera leu ou pelo CPF
    digitado. Devolve nome, função e o bilhete da batida. Não registra batida."""
    d = _corpo()
    with db.conexao() as conn:
        aparelho = _aparelho_da_obra(conn)
        if not auth.dentro_do_limite("identificar", str(aparelho["id"]),
                                     IDENTIFICACOES_POR_HORA_POR_APARELHO):
            return jsonify({"ok": False, "erro": "muitas identificações neste aparelho; "
                                                 "espere alguns minutos"}), 429
        obra = cadastros.resolver_obra(conn, d.get("obra")) if d.get("obra") else None
        conteudo = str(d.get("qr") or "").strip()
        lido = None
        if conteudo:
            if conteudo.startswith(qr.PREFIXO_WHATSAPP) and not db.tem_003(conn):
                raise Recusada("o QR Code ainda não foi ativado — digite o CPF")
            lido = qr.ler(conn, conteudo)
            if lido.problema:
                dono = cadastros.colaborador_por_id(conn, lido.colaborador_id) if lido.colaborador_id else None
                motivo = ("QR Code antigo (" + lido.identificacao + ")") if lido.antigo else "QR Code desconhecido"
                _recusar_em_separado(motivo, cpf=(dono or {}).get("cpf"), obra=d.get("obra"))
                raise Recusada(lido.problema)
            pessoa = cadastros.colaborador_por_id(conn, lido.colaborador_id)
            identificacao = lido.identificacao
        else:
            cpf = cadastros.normalizar_cpf(d.get("cpf"))
            pessoa = cadastros.colaborador_por_cpf(conn, cpf)
            if not pessoa:
                _recusar_em_separado("CPF não identificado no tablet", cpf=cpf, obra=d.get("obra"))
                raise Recusada("CPF não encontrado — confira os números")
            identificacao = qr.IDENT_CPF
        if not pessoa or pessoa["situacao"] == "DESLIGADO" or not pessoa["ativo_no_ponto"]:
            raise Recusada("cadastro inativo no ponto — procure a administração da obra")
        periodo = marcacoes.fora_do_contrato(pessoa, horario.data_referencia(horario.agora(), pessoa["tipo_jornada"]))
        if periodo:
            _recusar_em_separado(periodo, cpf=pessoa["cpf"], obra=d.get("obra"))
            raise Recusada(periodo)
        if obra:
            motivo = dispositivos.autorizado_para(
                aparelho, int(pessoa["id"]), int(obra["id"]),
                dispositivos.autorizados_de(conn, aparelho["id"]),
                dispositivos.obras_de(conn, aparelho["id"]))
            if motivo:
                _recusar_em_separado(motivo, cpf=pessoa["cpf"], obra=d.get("obra"))
                raise Recusada(motivo)
        if lido and lido.qr_id:
            qr.registrar_uso(conn, lido.qr_id)
        bilhete = qr.emitir_bilhete(int(pessoa["id"]), int(aparelho["id"]), identificacao,
                                    para_pedido=bool(d.get("para_pedido")))
        dispositivos.marcar_uso(conn, aparelho["id"])
    partes = pessoa["nome"].split()
    return _ok(bilhete=bilhete, nome=pessoa["nome"], primeiro_nome=partes[0].title() if partes else "",
               funcao=pessoa.get("funcao"), identificacao=identificacao,
               validade_segundos=qr.BILHETE_PEDIDO_S if d.get("para_pedido") else qr.BILHETE_VALIDADE_S,
               tem_banco=pessoa.get("regime_banco") not in (None, "SEM_BANCO"))


@bp.route("/app/api/tablet/pedido", methods=["POST"])
@auth.exige_aparelho
def app_api_tablet_pedido():
    """Atestado, licença, ajuste e compensação entregues NO APARELHO DA OBRA
    (ou no de grupo): a pessoa se identifica (QR ou CPF, o mesmo bilhete da
    batida) e o documento é fotografado ali. Decisão do dono, 06/10/2026: quem
    faz pedido é quem tem permissão de bater o ponto."""
    from .core import licencas
    d = _corpo()
    with db.conexao() as conn:
        aparelho = _aparelho_da_obra(conn)
        colaborador_id, _ = qr.conferir_bilhete(d.get("bilhete"), aparelho["id"])
        pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
        if not pessoa:
            raise ErroDeValidacao("identifique-se de novo", campo="bilhete")
        nome_ap = (aparelho.get("descricao") or "aparelho da obra")[:60]
        quem = Quem(nome=f"{pessoa['nome']} — no aparelho {nome_ap}"[:120], colaborador_id=colaborador_id)
        dados = {k: v for k, v in d.items() if k not in ("bilhete", "aprovar_ja", "colaborador_id")}
        dados["colaborador_id"] = colaborador_id
        if not dados.get("obra") and str(dados.get("tipo") or "").upper() == "AJUSTE_BATIDA":
            dados["obra"] = d.get("obra_do_aparelho")
        if not licencas.disponivel(conn):
            raise Recusada("aplique as atualizações do ponto para fazer pedidos no aparelho da obra")
        o = ocorrencias.criar(conn, quem, dados, origem="APARELHO")
    return _ok(pedido={"id": o["id"], "rotulo": o["rotulo"], "etapa": o.get("etapa_atual")}), 201


@bp.route("/app/api/licencas")
@auth.exige_colaborador_ou_aparelho
def app_api_licencas():
    """A lista das licenças da lei, para o formulário."""
    from .core import licencas
    with db.conexao() as conn:
        return _ok(licencas=licencas.listar(conn))


@bp.route("/app/api/tablet/esqueci-qr", methods=["POST"])
@auth.exige_aparelho
def app_api_tablet_esqueci_qr():
    """"Esqueci meu QR": digita o CPF e o QR vai para o WhatsApp do cadastro.
    A resposta é a MESMA exista o CPF ou não."""
    d = _corpo()
    with db.conexao() as conn:
        aparelho = _aparelho_da_obra(conn)
        if not auth.dentro_do_limite("pedir_qr", str(aparelho["id"]), PEDIDOS_DE_QR_POR_HORA_POR_APARELHO):
            return jsonify({"ok": False, "erro": "muitos pedidos neste aparelho; tente mais tarde"}), 429
        if not db.tem_003(conn):
            raise Recusada("o QR Code ainda não foi ativado — digite o CPF")
        pessoa = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(d.get("cpf")))
        if pessoa:
            envios.pedir_qr(conn, int(pessoa["id"]), motivo="PEDIDO",
                            por=f"a própria pessoa, no aparelho {aparelho['id']}")
    envios.enviar_agora()            # depois de gravado (09/10/2026: o pedido esperava na fila)
    return _ok(mensagem=RESPOSTA_DO_QR)


# ---------------------------------------------------------------------------
# O meu QR, no celular de quem entrou com CPF + PIN
# ---------------------------------------------------------------------------
@bp.route("/app/api/meu-qr")
@auth.exige_colaborador
def app_api_meu_qr():
    """O QR que muda a cada 30 segundos. Print de tela não serve depois."""
    import base64 as _b64
    with db.conexao() as conn:
        quem = _quem(conn)
        situacao = qr.situacao_da_pessoa(conn, quem.colaborador_id) if db.tem_003(conn) else None
    conteudo = qr.conteudo_do_app(quem.colaborador_id)
    png = qr.imagem(conteudo)
    return _ok(imagem="data:image/png;base64," + _b64.b64encode(png).decode("ascii"),
               troca_em_segundos=qr.segundos_ate_trocar(), whatsapp=situacao)


@bp.route("/app/api/meu-qr/whatsapp", methods=["POST"])
@auth.exige_colaborador
def app_api_meu_qr_whatsapp():
    with db.conexao() as conn:
        quem = _quem(conn)
        if not db.tem_003(conn):
            raise ErroDeValidacao("o QR Code por WhatsApp ainda não foi ativado")
        r = envios.pedir_qr(conn, quem.colaborador_id, motivo="PEDIDO", por="a própria pessoa")
    if r.get("enfileirado"):
        envios.enviar_agora()        # depois de gravado (09/10/2026: o pedido esperava na fila)
    if not r.get("enfileirado"):
        mensagens = {"sem telefone no cadastro": "seu cadastro está sem telefone — avise o DP",
                     "teto de pedidos do dia": "você já pediu 3 vezes hoje; use o QR desta tela"}
        raise ErroDeValidacao(mensagens.get(r.get("motivo"), "não foi possível pedir agora"))
    return _ok(mensagem="O QR Code chega no seu WhatsApp em instantes.")


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
        "local": cadastros.rotulo_obra(m["obra_codigo"], m["obra_nome"])
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


@bp.route("/app/api/pedidos/ajuste-do-dia", methods=["POST"])
@auth.exige_colaborador
def app_api_ajuste_do_dia():
    """"Corrigir este dia": os horários que faltaram, de uma vez, com o motivo.
    As travas (batida que já existe, dia justificado, pedido repetido) estão em
    core/ajustes.py."""
    d = _corpo()
    d.pop("aprovar_ja", None)
    d["colaborador_id"] = _eu()
    with db.conexao() as conn:
        quem = _quem(conn)
        criados = ocorrencias.criar_ajuste_do_dia(conn, quem, d, origem="APP")
    return _ok(pedidos=criados, quantidade=len(criados)), 201


@bp.route("/app/api/pedidos/<int:ocorrencia_id>/cancelar", methods=["POST"])
@auth.exige_colaborador
def app_api_cancelar(ocorrencia_id: int):
    with db.conexao() as conn:
        quem = _quem(conn)
        o = ocorrencias.cancelar(conn, ocorrencia_id, quem)
    return _ok(pedido=o)
