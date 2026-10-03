# -*- coding: utf-8 -*-
"""
A GESTÃO DO PONTO, dentro do ERP — decisão do dono em 03/10/2026 ("gestão no
ERP, ok").

COMO ELA ENTRA NO ERP, e por que assim: as rotas abaixo são registradas NO
BLUEPRINT DO ERP (`erp.routes.bp`), e não num blueprint paralelo. Com isso a
gestão do ponto herda, sem uma linha a mais, o que o ERP já tem e que não se
recria: o login, a guarda de permissão (padrão NEGAR), o menu que só mostra o
que a pessoa abre, o recorte por obra e o aviso de banco atrasado. O código
mora na pasta do ponto; no ERP entra uma linha só, no fim de `erp/routes.py`,
protegida — se este arquivo falhar ao carregar, o ERP sobe sem o menu do ponto.

QUEM PODE O QUÊ (permissoes.py / secoes.py, seções "Ponto"):
  ver_ponto            olhar — sempre recortado pelas obras da pessoa
  tratar_ponto         batida em análise, ajuste de batida, 1ª etapa de
                       compensação/folga, escala e obras do colaborador, alertas
  aprovar_afastamento  o DP: atestado, licença, férias; abre o atestado; banco
  fechar_competencia   fecha e reabre o mês
  configurar_ponto     escalas, feriados, aparelhos, resumo, atualizações

A AÇÃO DECLARADA DECIDE SOZINHA QUEM ENTRA (regra do CLAUDE.md): por isso
atestado/licença/férias têm rotas próprias (`/afastamentos`), e cada etapa de
aprovação também — a do supervisor e a do DP —, em vez de uma rota que declara
uma coisa e confere outra por dentro.

FORA DO ALCANCE RESPONDE 404, nunca 403: dizer "sem permissão" para o número
de uma pessoa que existe confirmaria que ela existe.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import logging
from functools import wraps

from flask import Response, jsonify, render_template_string, request, session
from sqlalchemy.exc import ProgrammingError

from app.apps.erp import routes as R
from app.apps.erp.db.database import get_session
from app.apps.erp.routes import bp, login_obrigatorio, permissao

from . import db, horario, migracoes_runner
from .core import (alertas, banco, cadastros, competencias, dispositivos, documentos,
                   escalas, espelho, feriados, fotos, leitura_atestado, ocorrencias, painel,
                   parametros, rotina)
from .core.ocorrencias import Quem
from .erros import ErroDeValidacao, ErroDoPonto, NaoEncontrado

logger = logging.getLogger("ponto.gestao")

# ---------------------------------------------------------------------------
# O módulo no menu do ERP
# ---------------------------------------------------------------------------
ABAS = [
    ("ponto_hoje", "Hoje", "erp.ponto_pagina_hoje"),
    ("ponto_espelho", "Espelho", "erp.ponto_pagina_espelho"),
    ("ponto_pendencias", "Pendências", "erp.ponto_pagina_pendencias"),
    ("ponto_pessoas", "Pessoas", "erp.ponto_pagina_pessoas"),
    ("ponto_banco", "Banco de horas", "erp.ponto_pagina_banco"),
    ("ponto_alertas", "Alertas", "erp.ponto_pagina_alertas"),
    ("ponto_config", "Configuração", "erp.ponto_pagina_configuracao"),
]
MODULO = {
    "chave": "ponto", "nome": "Ponto", "sigla": "PON",
    "descricao": "Ponto eletrônico: quem bateu, espelho, pedidos, banco de horas e alertas",
    "cor": "var(--ciano)", "abas": ABAS,
}
ACOES_DA_TELA = ("ver_ponto", "tratar_ponto", "aprovar_afastamento", "fechar_competencia",
                 "configurar_ponto")


def _registrar_no_menu() -> None:
    if any(m["chave"] == "ponto" for m in R.MODULOS):
        return
    posicao = next((i for i, m in enumerate(R.MODULOS) if m["chave"] == "admin"), len(R.MODULOS))
    R.MODULOS.insert(posicao, MODULO)
    R._MODULO_DA_ABA.update({aba[0]: "ponto" for aba in ABAS})


# ---------------------------------------------------------------------------
# As telas do ponto moram na pasta do ponto, mas são desenhadas com a moldura
# do ERP (`erp_base.html`). Lidas pelo caminho, e não pelo carregador do Flask:
# assim a tela abre mesmo num ERP montado sem o blueprint do ponto — que é o
# que a homologação automática do ERP faz, tela por tela.
# ---------------------------------------------------------------------------
_PASTA_TELAS = __import__("os").path.join(__import__("os").path.dirname(__import__("os").path.abspath(__file__)),
                                          "templates", "ponto")


def _render(nome: str, **ctx):
    import os
    with open(os.path.join(_PASTA_TELAS, nome), encoding="utf-8") as f:
        return render_template_string(f.read(), **ctx)


BANCO_DO_PONTO_ATRASADO = ("O ponto ainda não está instalado por completo no banco. Quem configura o "
                           "ponto precisa abrir Ponto › Configuração e apertar “Aplicar atualizações do ponto”.")


def _e_banco_atrasado(e: Exception) -> bool:
    texto = str(getattr(e, "orig", e) or e)
    return "ponto." in texto or 'schema "ponto"' in texto or "does not exist" in texto


# ---------------------------------------------------------------------------
# Quem está agindo
# ---------------------------------------------------------------------------
def _quem() -> Quem:
    from app.apps.erp.core.auth.permissoes import obras_de_registro_sem_autor
    pode = R._pode_agora(*ACOES_DA_TELA)
    # `R.get_session`, e não o nome importado: é o mesmo caminho das rotas do
    # ERP, e o que os testes do ERP substituem pela sessão deles.
    with R.get_session() as s:
        u = R._usuario_logado(s)
        if u is None:
            raise NaoEncontrado("sessão expirada")
        obras = obras_de_registro_sem_autor(s, u)
        return Quem(nome=u.nome, usuario_id=u.id, supervisor=pode.get("tratar_ponto", False),
                    dp=pode.get("aprovar_afastamento", False),
                    obras=(None if obras is None else [int(o) for o in obras]),
                    extras=pode)


def _api(fn):
    """Erros do ponto viram a resposta JSON da casa, com o status certo."""
    @wraps(fn)
    def _envolta(*a, **kw):
        try:
            return fn(*a, **kw)
        except ErroDoPonto as e:
            corpo = {"ok": False, "erro": e.mensagem}
            corpo.update({k: v for k, v in e.detalhes.items() if v is not None})
            return jsonify(corpo), e.status
        except ProgrammingError as e:
            # Código publicado antes de alguém aplicar as atualizações do ponto.
            # 409 e não 500: o serviço está de pé, só falta um passo de alguém.
            if _e_banco_atrasado(e):
                logger.warning("Ponto/gestão: banco do ponto atrasado em %s", request.path)
                return jsonify({"ok": False, "banco_atrasado": True, "erro": BANCO_DO_PONTO_ATRASADO}), 409
            logger.exception("Ponto/gestão: erro de banco em %s", request.path)
            return jsonify({"ok": False, "erro": "erro interno; o detalhe está no log"}), 500
        except Exception:  # noqa: BLE001
            logger.exception("Ponto/gestão: erro inesperado em %s", request.path)
            return jsonify({"ok": False, "erro": "erro interno; o detalhe está no log"}), 500
    return _envolta


def _corpo() -> dict:
    dados = request.get_json(silent=True)
    return dados if isinstance(dados, dict) else {}


def _ok(**dados):
    return jsonify({"ok": True, **dados})


def _exigir_pessoa(conn, quem: Quem, colaborador_id: int) -> dict:
    return ocorrencias.exigir_pessoa_no_alcance(conn, quem, colaborador_id)


def _pagina(aba: str):
    try:
        rotina.disparar_se_preciso()
    except Exception:  # noqa: BLE001 — rotina nunca derruba tela
        logger.warning("Ponto: rotina não disparou", exc_info=True)
    ctx = R._contexto(aba)
    ctx["pode"] = {**ctx.get("pode", {}), **R._pode_agora(*ACOES_DA_TELA)}
    return _render("gestao.html", aba_ponto=aba, **ctx)


def _datas_da_competencia(valor) -> tuple[dt.date, dt.date]:
    comp = competencias.primeiro_dia(valor or horario.hoje())
    return comp, competencias.ultimo_dia(comp)


# ---------------------------------------------------------------------------
# Telas
# ---------------------------------------------------------------------------
@bp.route("/erp/ponto")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_hoje():
    return _pagina("ponto_hoje")


@bp.route("/erp/ponto/espelho")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_espelho():
    return _pagina("ponto_espelho")


@bp.route("/erp/ponto/pendencias")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_pendencias():
    return _pagina("ponto_pendencias")


@bp.route("/erp/ponto/pessoas")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_pessoas():
    return _pagina("ponto_pessoas")


@bp.route("/erp/ponto/banco")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_banco():
    return _pagina("ponto_banco")


@bp.route("/erp/ponto/alertas")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_alertas():
    return _pagina("ponto_alertas")


@bp.route("/erp/ponto/configuracao")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_configuracao():
    return _pagina("ponto_config")


@bp.route("/erp/ponto/espelho/<int:colaborador_id>/imprimir")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_espelho_imprimir(colaborador_id: int):
    """O espelho do mês para imprimir e assinar (o navegador salva em PDF)."""
    quem = _quem()
    with db.conexao() as conn:
        try:
            _exigir_pessoa(conn, quem, colaborador_id)
        except NaoEncontrado:
            return jsonify({"ok": False, "erro": "não encontrado"}), 404
        esp = espelho.do_mes(conn, colaborador_id, request.args.get("competencia"))
    return _render("espelho_impressao.html", e=esp,
                           gerado_em=horario.para_local(horario.agora()).strftime("%d/%m/%Y %H:%M"),
                           gerado_por=quem.nome)


# ---------------------------------------------------------------------------
# Hoje, pessoas e espelho
# ---------------------------------------------------------------------------
@bp.route("/erp/api/ponto/obras")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_obras():
    quem = _quem()
    with db.conexao() as conn:
        obras = cadastros.listar_obras(conn, so_ativas=True)
    if quem.obras is not None:
        obras = [o for o in obras if o["id"] in quem.obras]
    return _ok(obras=[cadastros.obra_para_json(o) for o in obras])


@bp.route("/erp/api/ponto/hoje")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_hoje():
    quem = _quem()
    dia = request.args.get("data")
    obra = request.args.get("obra")
    with db.conexao() as conn:
        obra_id = None
        if obra:
            o = cadastros.resolver_obra(conn, obra)
            if not o or not quem.alcanca_obra(o["id"]):
                raise NaoEncontrado("obra não encontrada")
            obra_id = int(o["id"])
        resultado = painel.hoje(conn, dia=(dt.date.fromisoformat(dia) if dia else None),
                                obras=quem.obras, obra_id=obra_id)
    return _ok(**resultado)


@bp.route("/erp/api/ponto/pessoas")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_pessoas():
    quem = _quem()
    busca = (request.args.get("busca") or "").strip().lower()
    with db.conexao() as conn:
        pessoas = cadastros.listar_colaboradores(
            conn, so_ativos=request.args.get("todos") not in ("1", "true"))
        if quem.obras is not None:
            alcance = set(quem.obras)
            pessoas = [p for p in pessoas if p.get("obra_id") in alcance
                       or any(o["id"] in alcance for o in p["obras_adicionais"])]
        if busca:
            digitos = "".join(ch for ch in busca if ch.isdigit())
            pessoas = [p for p in pessoas if busca in p["nome"].lower()
                       or (digitos and digitos in p["cpf"])]
        vig = {v["colaborador_id"]: v for v in db.todos(conn, """
            SELECT DISTINCT ON (ce.colaborador_id) ce.colaborador_id, e.nome
              FROM ponto.colaborador_escalas ce JOIN ponto.escalas e ON e.id = ce.escala_id
             WHERE ce.vigencia_inicio <= CURRENT_DATE
             ORDER BY ce.colaborador_id, ce.vigencia_inicio DESC""")}
        pins = {r["colaborador_id"] for r in db.todos(
            conn, "SELECT colaborador_id FROM ponto.colaborador_config WHERE pin_hash IS NOT NULL")}
    saida = []
    for p in pessoas:
        j = cadastros.colaborador_para_json(p)
        j["escala"] = (vig.get(p["id"]) or {}).get("nome")
        j["tem_pin"] = p["id"] in pins
        j["regime_banco_rotulo"] = banco.ROTULOS.get(j["regime_banco"], j["regime_banco"])
        saida.append(j)
    return _ok(pessoas=saida, quantidade=len(saida))


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_pessoa(colaborador_id: int):
    quem = _quem()
    with db.conexao() as conn:
        p = _exigir_pessoa(conn, quem, colaborador_id)
        adicionais = db.todos(conn, """SELECT o.id, o.codigo, o.nome FROM ponto.colaborador_obras co
                                         JOIN public.obras o ON o.id = co.obra_id
                                        WHERE co.colaborador_id = :c ORDER BY o.codigo""",
                              c=colaborador_id)
        historico = escalas.historico_da_pessoa(conn, colaborador_id)
        aparelhos = db.todos(conn, """SELECT id, descricao, perfil, status, ultimo_uso_em
                                        FROM ponto.dispositivos WHERE colaborador_id = :c
                                       ORDER BY id DESC""", c=colaborador_id)
    j = cadastros.colaborador_para_json({**p, "obras_adicionais": adicionais})
    j["escalas"] = [{"escala_id": h["escala_id"], "escala": h["escala_nome"], "tipo": h["escala_tipo"],
                     "desde": h["vigencia_inicio"].isoformat(),
                     "ciclo_data_base": h["ciclo_data_base"].isoformat() if h["ciclo_data_base"] else None,
                     "por": h["definido_por"]} for h in historico]
    j["aparelhos"] = [{**a, "ultimo_uso_em": horario.texto(a["ultimo_uso_em"])} for a in aparelhos]
    j["acordo_banco"] = bool(p.get("acordo_documento_id"))
    return _ok(pessoa=j)


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/escala", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_pessoa_escala(colaborador_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        escalas.atribuir(conn, colaborador_id, int(d.get("escala_id") or 0),
                         d.get("vigencia_inicio") or horario.hoje().isoformat(),
                         ciclo_data_base=d.get("ciclo_data_base"), por=quem.nome)
    return _ok()


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/ponto", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_pessoa_ponto(colaborador_id: int):
    """Jornada (vigia/padrão), ativo no ponto e obras adicionais."""
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        if "tipo_jornada" in d or "ativo" in d or "centro_custo" in d:
            cadastros.gravar_config_colaborador(
                conn, colaborador_id, tipo_jornada=d.get("tipo_jornada"),
                centro_custo=d.get("centro_custo"),
                ativo=(bool(d["ativo"]) if "ativo" in d else None))
        if "obras_adicionais" in d:
            ids = []
            for ref in d.get("obras_adicionais") or []:
                o = cadastros.resolver_obra(conn, ref)
                if not o:
                    raise ErroDeValidacao(f"obra não cadastrada: {ref}", campo="obras_adicionais")
                if not quem.alcanca_obra(o["id"]):
                    raise ErroDeValidacao(f"a obra {o['codigo']} não está no seu alcance",
                                          campo="obras_adicionais")
                ids.append(int(o["id"]))
            cadastros.definir_obras_adicionais(conn, colaborador_id, ids)
    return _ok()


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/banco", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_pessoa_banco(colaborador_id: int):
    """Regime de banco de horas — é o DP que decide quem tem."""
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        doc = None
        if d.get("acordo_base64"):
            doc = documentos.guardar(conn, d["acordo_base64"], colaborador_id=colaborador_id,
                                     tipo_ocorrencia="ACORDO_BANCO",
                                     nome_original=str(d.get("acordo_nome") or ""))
        banco.definir_regime(conn, colaborador_id, d.get("regime_banco"),
                             inicio=d.get("banco_inicio"), acordo_documento_id=doc)
    return _ok()


@bp.route("/erp/api/ponto/espelho/<int:colaborador_id>")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_espelho(colaborador_id: int):
    quem = _quem()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        esp = espelho.do_mes(conn, colaborador_id, request.args.get("competencia"))
    return _ok(**esp)


@bp.route("/erp/api/ponto/exportar.csv")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_api_exportar():
    """Totais do mês por pessoa, para a folha. Recortado pelas obras de quem baixa."""
    try:
        return _exportar()
    except ProgrammingError as e:
        if not _e_banco_atrasado(e):
            raise
        return jsonify({"ok": False, "banco_atrasado": True, "erro": BANCO_DO_PONTO_ATRASADO}), 409


def _exportar():
    quem = _quem()
    inicio, fim = _datas_da_competencia(request.args.get("competencia"))
    saida = io.StringIO()
    w = csv.writer(saida, delimiter=";")
    w.writerow(["cpf", "nome", "obra", "dias", "previsto_h", "trabalhado_h", "extra_h",
                "debito_h", "faltas", "abonados", "incompletos", "noturno_h", "em_analise"])
    with db.conexao() as conn:
        pessoas = cadastros.listar_colaboradores(conn, so_ativos=True)
        for p in pessoas:
            if quem.obras is not None and not ocorrencias.pessoa_no_alcance(conn, quem, p["id"]):
                continue
            r = espelho.montar(conn, p["id"], inicio, fim)["resumo"]
            h = lambda m: f"{m / 60:.2f}".replace(".", ",")  # noqa: E731
            w.writerow([p["cpf"], p["nome"], p.get("obra_codigo") or "", r["dias"], h(r["previsto"]),
                        h(r["trabalhado"]), h(r["extra"]), h(r["debito"]), r["faltas"],
                        r["abonados"], r["incompletos"], h(r["noturno"]), r["em_analise"]])
    nome = f"ponto_{inicio:%Y-%m}.csv"
    return Response("﻿" + saida.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{nome}"'})


# ---------------------------------------------------------------------------
# Pendências e decisões
# ---------------------------------------------------------------------------
@bp.route("/erp/api/ponto/pendencias")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_pendencias():
    quem = _quem()
    with db.conexao() as conn:
        pedidos = ocorrencias.listar(conn, quem, status="PENDENTES")
        sql = """SELECT m.id, m.nsr, m.colaborador_id, m.timestamp_servidor, m.data_referencia,
                        m.motivo_analise, m.distancia_metros, m.foto_id, m.obra_id,
                        c.nome, o.codigo AS obra
                   FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
                   JOIN public.obras o ON o.id = m.obra_id WHERE m.status = 'EM_ANALISE'"""
        params = {}
        if quem.obras is not None:
            sql += " AND m.obra_id = ANY(:obras)"
            params["obras"] = quem.obras or [0]
        em_analise = db.todos(conn, sql + " ORDER BY m.timestamp_servidor DESC LIMIT 500", **params)
        aparelhos = (dispositivos.listar(conn, "PENDENTE")
                     if quem.extras.get("configurar_ponto") else [])
    return _ok(
        pedidos=pedidos,
        batidas=[{"id": m["id"], "nsr": m["nsr"], "colaborador_id": m["colaborador_id"],
                  "nome": m["nome"], "obra": m["obra"], "data": m["data_referencia"].isoformat(),
                  "horario": horario.texto(m["timestamp_servidor"]), "motivo": m["motivo_analise"],
                  "distancia_metros": float(m["distancia_metros"]) if m["distancia_metros"] is not None else None,
                  "tem_foto": m["foto_id"] is not None,
                  "pode_decidir": quem.supervisor and quem.alcanca_obra(m["obra_id"])}
                 for m in em_analise],
        aparelhos=[dispositivos.para_json(a) for a in aparelhos])


@bp.route("/erp/api/ponto/marcacoes/<int:marcacao_id>/decidir", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_decidir_batida(marcacao_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        m = db.um(conn, "SELECT obra_id FROM ponto.marcacoes WHERE id = :id", id=marcacao_id)
        if not m or not quem.alcanca_obra(m["obra_id"]):
            raise NaoEncontrado("batida não encontrada")
        from .core import marcacoes as marc
        r = marc.decidir_em_analise(conn, marcacao_id, para=d.get("para"), motivo=d.get("motivo", ""),
                                    usuario_id=quem.usuario_id, usuario_nome=quem.nome)
    return _ok(marcacao=marc.para_json(r))


@bp.route("/erp/api/ponto/marcacoes/<int:marcacao_id>/foto")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_api_foto(marcacao_id: int):
    quem = _quem()
    with db.conexao() as conn:
        m = db.um(conn, "SELECT obra_id, foto_id FROM ponto.marcacoes WHERE id = :id", id=marcacao_id)
        if not m or not m["foto_id"] or not quem.alcanca_obra(m["obra_id"]):
            return jsonify({"ok": False, "erro": "não encontrado"}), 404
        try:
            dados, mime = fotos.baixar(conn, m["foto_id"])
        except LookupError:
            return jsonify({"ok": False, "erro": "foto não disponível"}), 404
    return Response(dados, mimetype=mime, headers={"Cache-Control": "private, max-age=3600"})


@bp.route("/erp/api/ponto/ocorrencias")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_ocorrencias():
    quem = _quem()
    with db.conexao() as conn:
        lista = ocorrencias.listar(conn, quem, status=request.args.get("status") or None,
                                   colaborador_id=request.args.get("colaborador_id", type=int))
    return _ok(ocorrencias=lista)


@bp.route("/erp/api/ponto/ocorrencias", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_criar_ocorrencia():
    """Ajuste de batida, compensação e folga — registrados pela gestão da obra."""
    quem, d = _quem(), _corpo()
    if str(d.get("tipo") or "").upper() not in ("AJUSTE_BATIDA", "COMPENSACAO", "FOLGA_BANCO"):
        raise ErroDeValidacao("atestado, licença e férias são registrados em Afastamentos (DP)",
                              campo="tipo")
    with db.conexao() as conn:
        o = ocorrencias.criar(conn, quem, d, origem="GESTAO")
    return _ok(ocorrencia=o), 201


@bp.route("/erp/api/ponto/afastamentos", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_criar_afastamento():
    """Atestado, licença, férias, afastamento e abono — registrados pelo DP."""
    quem, d = _quem(), _corpo()
    if str(d.get("tipo") or "").upper() not in ("ATESTADO", "LICENCA", "FERIAS", "AFASTAMENTO", "ABONO"):
        raise ErroDeValidacao("tipo de afastamento desconhecido", campo="tipo")
    with db.conexao() as conn:
        o = ocorrencias.criar(conn, quem, d, origem="GESTAO")
    return _ok(ocorrencia=o), 201


@bp.route("/erp/api/ponto/ocorrencias/<int:ocorrencia_id>/etapa-supervisor", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_decidir_supervisor(ocorrencia_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        atual = ocorrencias.obter(conn, ocorrencia_id, quem)
        if atual["etapa_atual"] != ocorrencias.SUPERVISOR:
            raise ErroDeValidacao("este pedido não está na etapa do supervisor", campo="etapa")
        o = ocorrencias.decidir(conn, ocorrencia_id, quem, aprovar=bool(d.get("aprovar")),
                                motivo=d.get("motivo", ""))
    return _ok(ocorrencia=o)


@bp.route("/erp/api/ponto/afastamentos/<int:ocorrencia_id>/etapa-dp", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_decidir_dp(ocorrencia_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        atual = ocorrencias.obter(conn, ocorrencia_id, quem)
        if atual["etapa_atual"] != ocorrencias.DP:
            raise ErroDeValidacao("este pedido não está na etapa do DP", campo="etapa")
        o = ocorrencias.decidir(conn, ocorrencia_id, quem, aprovar=bool(d.get("aprovar")),
                                motivo=d.get("motivo", ""))
    return _ok(ocorrencia=o)


@bp.route("/erp/api/ponto/ocorrencias/<int:ocorrencia_id>/cancelar", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_cancelar_ocorrencia(ocorrencia_id: int):
    quem = _quem()
    with db.conexao() as conn:
        o = ocorrencias.cancelar(conn, ocorrencia_id, quem)
    return _ok(ocorrencia=o)


@bp.route("/erp/api/ponto/afastamentos/<int:ocorrencia_id>/ler", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_ler_atestado(ocorrencia_id: int):
    """A IA lê o atestado e preenche; o DP confere antes de aprovar."""
    quem = _quem()
    R._exigir_saldo_de_ia()
    with db.conexao() as conn:
        ocorrencias.obter(conn, ocorrencia_id, quem)
        leitura = leitura_atestado.ler(conn, ocorrencia_id)
    return _ok(leitura=leitura)


@bp.route("/erp/api/ponto/afastamentos/<int:ocorrencia_id>/corrigir", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_corrigir_afastamento(ocorrencia_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        o = ocorrencias.corrigir_leitura(conn, ocorrencia_id, quem, d)
    return _ok(ocorrencia=o)


@bp.route("/erp/api/ponto/afastamentos/documento/<int:documento_id>")
@login_obrigatorio
@permissao("aprovar_afastamento")
def ponto_api_documento_sigiloso(documento_id: int):
    """O papel do atestado — só para quem aprova afastamento (o DP)."""
    return _servir_documento(documento_id, sigiloso_permitido=True)


@bp.route("/erp/api/ponto/documentos/<int:documento_id>")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_api_documento(documento_id: int):
    """Documento que NÃO é de saúde (acordo de banco, declaração)."""
    return _servir_documento(documento_id, sigiloso_permitido=False)


def _servir_documento(documento_id: int, *, sigiloso_permitido: bool):
    quem = _quem()
    with db.conexao() as conn:
        f = documentos.ficha(conn, documento_id)
        dono = db.um(conn, """
            SELECT colaborador_id FROM ponto.ocorrencias WHERE documento_id = :d
            UNION SELECT colaborador_id FROM ponto.colaborador_config WHERE acordo_documento_id = :d
            LIMIT 1""", d=documento_id)
        if (not f or not dono or (f["sigiloso"] and not sigiloso_permitido)
                or not ocorrencias.pessoa_no_alcance(conn, quem, dono["colaborador_id"])):
            return jsonify({"ok": False, "erro": "não encontrado"}), 404
        try:
            dados, mime, nome = documentos.baixar(conn, documento_id)
        except Exception:  # noqa: BLE001
            logger.exception("Ponto: documento %s não abriu", documento_id)
            return jsonify({"ok": False, "erro": "o Drive não respondeu; tente de novo"}), 503
    return Response(dados, mimetype=mime,
                    headers={"Content-Disposition": f'inline; filename="{nome}"',
                             "Cache-Control": "private, no-store"})


# ---------------------------------------------------------------------------
# Banco de horas
# ---------------------------------------------------------------------------
@bp.route("/erp/api/ponto/banco")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_banco_lista():
    quem = _quem()
    with db.conexao() as conn:
        pessoas = [p for p in cadastros.listar_colaboradores(conn, so_ativos=True)
                   if p.get("regime_banco") not in (None, "SEM_BANCO")
                   and ocorrencias.pessoa_no_alcance(conn, quem, p["id"])]
        linhas = []
        for p in pessoas:
            s = banco.saldo(conn, p["id"])
            linhas.append({"colaborador_id": p["id"], "nome": p["nome"], "obra": p.get("obra_codigo"),
                           "regime": s["regime_rotulo"], "saldo": s["saldo"],
                           "vence_logo": [v for v in s["vencimentos"]
                                          if (dt.date.fromisoformat(v["vence_em"]) - horario.hoje()).days <= 30],
                           "aviso": s.get("aviso")})
    return _ok(pessoas=linhas)


@bp.route("/erp/api/ponto/banco/<int:colaborador_id>")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_banco(colaborador_id: int):
    quem = _quem()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        s = banco.saldo(conn, colaborador_id)
    return _ok(**s)


@bp.route("/erp/api/ponto/banco/<int:colaborador_id>/lancamentos", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_banco_lancar(colaborador_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        try:
            horas = float(str(d.get("horas") or "0").replace(",", "."))
        except ValueError as e:
            raise ErroDeValidacao("horas ilegíveis (ex.: 1,5 ou -2)", campo="horas") from e
        l = banco.lancar(conn, colaborador_id, data=d.get("data") or horario.hoje().isoformat(),
                         minutos=round(horas * 60), tipo=d.get("tipo"), descricao=d.get("descricao", ""),
                         usuario_id=quem.usuario_id, usuario_nome=quem.nome)
    return _ok(lancamento={**l, "data": l["data"].isoformat(), "criado_em": horario.texto(l["criado_em"])})


# ---------------------------------------------------------------------------
# Alertas
# ---------------------------------------------------------------------------
@bp.route("/erp/api/ponto/alertas")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_alertas():
    quem = _quem()
    with db.conexao() as conn:
        lista = alertas.listar(conn, obras=quem.obras,
                               situacao=request.args.get("situacao", "ABERTO") or None,
                               gravidade=request.args.get("gravidade") or None)
    return _ok(alertas=lista, rotulos=alertas.ROTULO)


@bp.route("/erp/api/ponto/alertas/gerar", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_alertas_gerar():
    with db.conexao() as conn:
        r = alertas.gerar(conn)
    return _ok(**r)


@bp.route("/erp/api/ponto/alertas/<int:alerta_id>/tratar", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_alerta_tratar(alerta_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        a = alertas.tratar(conn, alerta_id, situacao=d.get("situacao"), nota=d.get("nota", ""),
                           por=quem.nome, obras=quem.obras)
    return _ok(alerta={"id": a["id"], "situacao": a["situacao"]})


@bp.route("/erp/api/ponto/resumo")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_resumo():
    quem = _quem()
    with db.conexao() as conn:
        texto = alertas.resumo_do_dia(conn, obras=quem.obras)
    return _ok(texto=texto)


@bp.route("/erp/api/ponto/resumo/enviar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_resumo_enviar():
    with db.conexao() as conn:
        r = rotina.enviar_resumo(conn, forcar=True)
    return _ok(**r)


# ---------------------------------------------------------------------------
# Configuração: escalas, feriados, competências, aparelhos, parâmetros
# ---------------------------------------------------------------------------
def _escala_json(e: dict) -> dict:
    return {"id": e["id"], "nome": e["nome"], "tipo": e["tipo"], "semana": e["semana"],
            "ciclo_entrada": e["ciclo_entrada"], "ciclo_saida": e["ciclo_saida"],
            "descricao": e["descricao"], "ativo": e["ativo"],
            "horas_semanais": e.get("horas_semanais", escalas.horas_semanais(e)),
            "pessoas": e.get("pessoas")}


@bp.route("/erp/api/ponto/escalas")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_escalas():
    with db.conexao() as conn:
        lista = escalas.listar(conn)
    return _ok(escalas=[_escala_json(e) for e in lista])


@bp.route("/erp/api/ponto/escalas", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_escala_criar():
    with db.conexao() as conn:
        e = escalas.gravar(conn, _corpo())
    return _ok(escala=_escala_json(e)), 201


@bp.route("/erp/api/ponto/escalas/<int:escala_id>", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_escala_editar(escala_id: int):
    with db.conexao() as conn:
        e = escalas.gravar(conn, _corpo(), escala_id)
    return _ok(escala=_escala_json(e))


@bp.route("/erp/api/ponto/feriados")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_feriados():
    with db.conexao() as conn:
        lista = feriados.listar(conn, ano=request.args.get("ano", type=int))
    return _ok(feriados=[{**f, "data": f["data"].isoformat(), "criado_em": None} for f in lista])


@bp.route("/erp/api/ponto/feriados", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_feriado_criar():
    with db.conexao() as conn:
        f = feriados.gravar(conn, _corpo())
    return _ok(feriado={**f, "data": f["data"].isoformat(), "criado_em": None}), 201


@bp.route("/erp/api/ponto/feriados/<int:feriado_id>/apagar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_feriado_apagar(feriado_id: int):
    with db.conexao() as conn:
        feriados.apagar(conn, feriado_id)
    return _ok()


@bp.route("/erp/api/ponto/competencias")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_competencias():
    with db.conexao() as conn:
        lista = competencias.listar(conn)
    return _ok(competencias=[{**c, "competencia": c["competencia"].strftime("%Y-%m"),
                              "fechada_em": horario.texto(c.get("fechada_em")),
                              "reaberta_em": horario.texto(c.get("reaberta_em"))} for c in lista])


@bp.route("/erp/api/ponto/competencias/<competencia>/fechar", methods=["POST"])
@login_obrigatorio
@permissao("fechar_competencia")
@_api
def ponto_api_competencia_fechar(competencia: str):
    quem = _quem()
    with db.conexao() as conn:
        c = competencias.fechar(conn, competencia, quem.nome)
    return _ok(situacao=c["situacao"])


@bp.route("/erp/api/ponto/competencias/<competencia>/reabrir", methods=["POST"])
@login_obrigatorio
@permissao("fechar_competencia")
@_api
def ponto_api_competencia_reabrir(competencia: str):
    quem = _quem()
    with db.conexao() as conn:
        c = competencias.reabrir(conn, competencia, quem.nome, _corpo().get("motivo", ""))
    return _ok(situacao=c["situacao"])


@bp.route("/erp/api/ponto/dispositivos")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_dispositivos():
    with db.conexao() as conn:
        lista = dispositivos.listar(conn, request.args.get("status") or None)
    return _ok(dispositivos=[dispositivos.para_json(d) for d in lista])


@bp.route("/erp/api/ponto/dispositivos/<int:dispositivo_id>/aprovar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_dispositivo_aprovar(dispositivo_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        colaborador_id = None
        if d.get("cpf"):
            p = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(d["cpf"]))
            if not p:
                raise ErroDeValidacao("pessoa não cadastrada", campo="cpf")
            colaborador_id = int(p["id"])
        autorizados = []
        for cpf in d.get("autorizados") or []:
            p = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(cpf))
            if not p:
                raise ErroDeValidacao(f"pessoa não cadastrada: {cpf}", campo="autorizados")
            autorizados.append(int(p["id"]))
        obras = []
        for ref in d.get("obras") or []:
            o = cadastros.resolver_obra(conn, ref)
            if not o:
                raise ErroDeValidacao(f"obra não cadastrada: {ref}", campo="obras")
            obras.append(int(o["id"]))
        a = dispositivos.aprovar(conn, dispositivo_id, perfil=d.get("perfil", "COMPARTILHADO"),
                                 aprovado_por=quem.nome, colaborador_id=colaborador_id,
                                 descricao=d.get("descricao"), autorizados=autorizados, obras=obras)
    return _ok(dispositivo=dispositivos.para_json(a))


@bp.route("/erp/api/ponto/dispositivos/<int:dispositivo_id>/bloquear", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_dispositivo_bloquear(dispositivo_id: int):
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        a = dispositivos.bloquear(conn, dispositivo_id, motivo=d.get("motivo", ""), por=quem.nome)
    return _ok(dispositivo=dispositivos.para_json(a))


@bp.route("/erp/api/ponto/configuracao")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_configuracao():
    estado = migracoes_runner.listar_estado()
    with db.conexao() as conn:
        tem_parametros = not estado["pendentes"]
        telefones = parametros.ler(conn, parametros.TELEFONES_RESUMO, "") if tem_parametros else ""
        ultima = parametros.ler(conn, parametros.ULTIMA_ROTINA, "") if tem_parametros else ""
        fila_fotos = fotos.pendentes(conn) if tem_parametros else 0
        fila_docs = (db.um(conn, "SELECT count(*) AS n FROM ponto.documentos WHERE drive_file_id IS NULL "
                                 "AND conteudo IS NOT NULL")["n"] if tem_parametros else 0)
    return _ok(migracoes=estado, telefones_resumo=telefones, ultima_rotina=ultima,
               drive_configurado=fotos.drive_configurado(), fila_fotos=fila_fotos,
               fila_documentos=int(fila_docs),
               endereco_app="/ponto/app")


@bp.route("/erp/api/ponto/configuracao", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_configuracao_gravar():
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        if "telefones_resumo" in d:
            parametros.gravar(conn, parametros.TELEFONES_RESUMO, str(d["telefones_resumo"])[:500],
                              quem.nome)
    return _ok()


@bp.route("/erp/api/ponto/migrar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_migrar():
    r = migracoes_runner.aplicar_pendentes()
    if r["erro"]:
        return jsonify({"ok": False, "erro": f"a atualização {r['erro']['migracao']} falhou: "
                                             f"{r['erro']['erro'][:300]}", **r}), 500
    return _ok(**r)


@bp.route("/erp/api/ponto/fila/enviar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_fila_enviar():
    with db.conexao() as conn:
        f = fotos.enviar_pendentes(conn)
        d = documentos.enviar_pendentes(conn)
    return _ok(fotos=f, documentos=d)


_registrar_no_menu()
