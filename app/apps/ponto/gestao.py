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
                   envios, escalas, espelho, feriados, fotos, leitura_atestado, mosaico,
                   ocorrencias, painel, parametros, qr, rotina, validacao)
from .core.ocorrencias import Quem
from .erros import ErroDeValidacao, ErroDoPonto, NaoEncontrado

logger = logging.getLogger("ponto.gestao")

# ---------------------------------------------------------------------------
# O módulo no menu do ERP
# ---------------------------------------------------------------------------
ABAS = [
    ("ponto_hoje", "Hoje", "erp.ponto_pagina_hoje"),
    ("ponto_espelho", "Espelho", "erp.ponto_pagina_espelho"),
    ("ponto_pendencias", "Validações", "erp.ponto_pagina_pendencias"),
    ("ponto_mosaico", "Mosaico", "erp.ponto_pagina_mosaico"),
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
    try:
        from .core import registro
        registro.manter_em_dia()
    except Exception:  # noqa: BLE001
        logger.warning("Ponto: a base de pessoas não foi conferida", exc_info=True)
    try:
        from .core import base_obras, rosto
        base_obras.manter_em_dia()
        rosto.manter_em_dia()
    except Exception:  # noqa: BLE001
        logger.warning("Ponto: a base de obras não foi conferida", exc_info=True)
    try:
        # O endereço público vai no link do aviso do mosaico. Aprendido de quem
        # abre a gestão (é o endereço que essa pessoa usa), sem variável nova.
        with db.conexao() as conn:
            if db.tem_003(conn):
                envios.lembrar_endereco(conn, request.url_root)
    except Exception:  # noqa: BLE001
        pass
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


@bp.route("/erp/ponto/mosaico")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_pagina_mosaico():
    return _pagina("ponto_mosaico")


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
    j["funcao"] = p.get("funcao")
    j["tem_telefone"] = bool(envios.telefone_valido(p.get("telefone")))
    with db.conexao() as conn:
        j["qr"] = qr.situacao_da_pessoa(conn, colaborador_id) if db.tem_003(conn) else None
    return _ok(pessoa=j)


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/qr/enviar", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_pessoa_qr_enviar(colaborador_id: int):
    """Manda um QR Code novo para o WhatsApp da pessoa, na frente da fila."""
    quem = _quem()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        r = envios.pedir_qr(conn, colaborador_id, motivo="GESTAO", por=quem.nome)
        if not r.get("enfileirado"):
            raise ErroDeValidacao(f"não foi possível: {r.get('motivo')}")
        situacao = qr.situacao_da_pessoa(conn, colaborador_id)
    envios.enviar_agora()            # a fila não acordava pelo ERP (09/10/2026)
    return _ok(qr=situacao, whatsapp_configurado=envios.whatsapp_pronto())


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/qr/revogar", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_pessoa_qr_revogar(colaborador_id: int):
    """Celular perdido ou roubado: nenhum QR da pessoa vale mais, até o próximo
    envio. O CPF continua funcionando no tablet."""
    quem = _quem()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        n = qr.revogar_todos(conn, colaborador_id, quem.nome)
        situacao = qr.situacao_da_pessoa(conn, colaborador_id)
    return _ok(revogados=n, qr=situacao)


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
        regra_batida = validacao.quem_valida_batida(conn)
    return _ok(
        pedidos=pedidos,
        batidas=[{"id": m["id"], "nsr": m["nsr"], "colaborador_id": m["colaborador_id"],
                  "nome": m["nome"], "obra": m["obra"], "data": m["data_referencia"].isoformat(),
                  "horario": horario.texto(m["timestamp_servidor"]), "motivo": m["motivo_analise"],
                  "distancia_metros": float(m["distancia_metros"]) if m["distancia_metros"] is not None else None,
                  "tem_foto": m["foto_id"] is not None,
                  "pode_decidir": ((quem.dp if regra_batida == validacao.DP else quem.supervisor)
                                   and quem.alcanca_obra(m["obra_id"])),
                  "quem_valida": regra_batida}
                 for m in em_analise],
        aparelhos=[dispositivos.para_json(a) for a in aparelhos])


def _decidir_batida(marcacao_id: int, quem_valida: str):
    """A batida em conferência. Quem decide é o da regra (Configuração › Quem
    valida): cada um pela sua rota, e a rota do outro responde o que fazer."""
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        m = db.um(conn, "SELECT obra_id FROM ponto.marcacoes WHERE id = :id", id=marcacao_id)
        if not m or not quem.alcanca_obra(m["obra_id"]):
            raise NaoEncontrado("batida não encontrada")
        regra = validacao.quem_valida_batida(conn)
        if regra != quem_valida:
            raise ErroDeValidacao("a conferência das batidas está com " + validacao.ROTULO_OPCAO[regra]
                                  + " (Ponto › Configuração › Quem valida)", campo="etapa")
        from .core import marcacoes as marc
        r = marc.decidir_em_analise(conn, marcacao_id, para=d.get("para"), motivo=d.get("motivo", ""),
                                    usuario_id=quem.usuario_id, usuario_nome=quem.nome)
    return _ok(marcacao=marc.para_json(r))


@bp.route("/erp/api/ponto/marcacoes/<int:marcacao_id>/decidir", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_decidir_batida(marcacao_id: int):
    """O encarregado valida ou rejeita a batida em conferência."""
    return _decidir_batida(marcacao_id, validacao.ENCARREGADO)


@bp.route("/erp/api/ponto/marcacoes/<int:marcacao_id>/decidir-dp", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_decidir_batida_dp(marcacao_id: int):
    """O DP valida ou rejeita a batida em conferência."""
    return _decidir_batida(marcacao_id, validacao.DP)


# ---------------------------------------------------------------------------
# VALIDAÇÕES: tudo o que espera alguém, numa lista só (pedido do dono,
# 04/10/2026: "uma tela onde liste tudo que está pendente para validação (…)
# filtrar por obra, período (…) poder ver os mosaicos também").
# ---------------------------------------------------------------------------
@bp.route("/erp/api/ponto/validacoes")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_validacoes():
    quem = _quem()
    a = request.args
    with db.conexao() as conn:
        obra_id = None
        if a.get("obra"):
            o = cadastros.resolver_obra(conn, a.get("obra"))
            if not o or not quem.alcanca_obra(o["id"]):
                raise NaoEncontrado("obra não encontrada")
            obra_id = int(o["id"])
        from .core import validacoes as val
        r = val.listar(conn, quem, obra_id=obra_id, de=a.get("de"), ate=a.get("ate"),
                       tipos=[t for t in (a.get("tipos") or "").split(",") if t],
                       busca=a.get("busca") or "", so_minhas=a.get("so_minhas") in ("1", "true"),
                       ver_aparelhos=bool(quem.extras.get("configurar_ponto")))
    return _ok(**r)


@bp.route("/erp/api/ponto/registro")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_registro():
    """A base de pessoas do ponto: o Registro de Colaboradores (a cópia que a
    Análise de SPs guarda) ou o cadastro do ERP — e o retrato da base."""
    from .core import registro
    with db.conexao() as conn:
        r = registro.retrato(conn)
        faltam = registro.faltam_no_erp(conn, limite=20) if r.get("disponivel") else []
        automatico = registro.automatico(conn)
    return _ok(**r, exemplos_faltam=[{"nome": f["nome"], "cpf_final": f["cpf"][-3:]} for f in faltam],
               automatico=automatico, ultima_copia=registro.ultima_copia(),
               ritmo={"horas_entre_copias": registro.HORAS_ENTRE_COPIAS,
                      "janela": list(registro.JANELA_DA_COPIA),
                      "minutos_entre_checagens": registro.MINUTOS_ENTRE_CHECAGENS})


@bp.route("/erp/api/ponto/registro/automatico", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_registro_automatico():
    """Liga ou desliga a base em dia sozinha (cópia da planilha de 2 em 2 h e
    cadastro no ERP de quem falta)."""
    from .core import registro
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        parametros.gravar(conn, registro.PARAMETRO_AUTOMATICO, "1" if d.get("ligado") else "0", quem.nome)
        ligado = registro.automatico(conn)
    logger.info("Ponto: base de pessoas em dia sozinha %s por %s", "LIGADA" if ligado else "desligada", quem.nome)
    return _ok(automatico=ligado)


@bp.route("/erp/api/ponto/registro/fonte", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_registro_fonte():
    from .core import registro
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        registro.gravar_fonte(conn, d.get("fonte"), quem.nome)
        r = registro.retrato(conn)
    logger.info("Ponto: base de pessoas passou a ser %s (%s)", d.get("fonte"), quem.nome)
    return _ok(**r)


@bp.route("/erp/api/ponto/registro/cadastrar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_registro_cadastrar():
    """Cadastra no ERP, com o mínimo (nome, CPF, obra), quem está ativo no
    Registro e falta lá — sem isso a pessoa não tem onde pendurar a batida."""
    from .core import registro
    quem = _quem()
    with db.conexao() as conn:
        if not registro.estado(conn, fresco=True)["disponivel"]:
            raise ErroDeValidacao("o Registro de Colaboradores não está disponível neste banco")
        r = registro.cadastrar_faltantes(conn, quem.nome)
        retrato = registro.retrato(conn)
    return _ok(**r, retrato=retrato)


@bp.route("/erp/api/ponto/obras-base")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_obras_base():
    """A base de obras do ponto: a planilha C. Diários (a cópia que o ponto
    guarda) ou o cadastro do ERP — com o que a última leitura encontrou."""
    from .core import base_obras
    with db.conexao() as conn:
        r = base_obras.retrato(conn)
    return _ok(**r)


@bp.route("/erp/api/ponto/obras-base/atualizar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_obras_base_atualizar():
    """Lê a aba C. Diários agora (segundos: algumas centenas de linhas)."""
    from .core import base_obras
    quem = _quem()
    r = base_obras.atualizar(quem.nome)
    if not r.get("ok"):
        raise ErroDeValidacao(f"não deu para ler a planilha: {r.get('erro')}")
    with db.conexao() as conn:
        retrato = base_obras.retrato(conn)
    return _ok(leitura=r, retrato=retrato)


@bp.route("/erp/api/ponto/obras-base/fonte", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_obras_base_fonte():
    from .core import base_obras
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        base_obras.gravar_fonte(conn, d.get("fonte"), quem.nome)
        r = base_obras.retrato(conn)
    logger.info("Ponto: base de obras passou a ser %s (%s)", d.get("fonte"), quem.nome)
    return _ok(**r)


@bp.route("/erp/api/ponto/obras-base/automatico", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_obras_base_automatico():
    from .core import base_obras
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        parametros.gravar(conn, base_obras.PARAMETRO_AUTOMATICO, "1" if d.get("ligado") else "0", quem.nome)
        ligado = base_obras.automatico(conn)
    logger.info("Ponto: base de obras em dia sozinha %s por %s", "LIGADA" if ligado else "desligada", quem.nome)
    return _ok(automatico=ligado)


@bp.route("/erp/api/ponto/excecoes")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_excecoes():
    """Quem foge do padrão (só o aparelho da obra bate; ninguém tem banco)."""
    from .core import forma_de_bater
    quem = _quem()
    with db.conexao() as conn:
        r = forma_de_bater.excecoes(conn, quem.obras)
    return _ok(**r)


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/forma-de-bater", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_pessoa_forma_de_bater(colaborador_id: int):
    """Exceções da pessoa no aplicativo: também bate no próprio celular; faz
    pedidos pelo próprio celular; é administrativo de obra (08/10/2026,
    core/papeis.py). Campo ausente fica como está."""
    from .core import forma_de_bater, papeis
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        if "bate_no_celular" in d:
            forma_de_bater.definir(conn, colaborador_id, bool(d.get("bate_no_celular")), quem.nome)
        if "pede_no_celular" in d or "administrativo_obra" in d:
            papeis.definir(conn, colaborador_id,
                           pede_no_celular=(bool(d["pede_no_celular"]) if "pede_no_celular" in d else None),
                           administrativo_obra=(bool(d["administrativo_obra"]) if "administrativo_obra" in d else None))
            logger.info("Ponto: papéis da pessoa %s no aplicativo — pedidos %s, administrativo %s (por %s)",
                        colaborador_id, d.get("pede_no_celular"), d.get("administrativo_obra"), quem.nome)
        p = cadastros.colaborador_por_id(conn, colaborador_id)
    return _ok(bate_no_celular=bool(p.get("bate_no_celular")), pede_no_celular=bool(p.get("pede_no_celular")),
               administrativo_obra=bool(p.get("administrativo_obra")))


@bp.route("/erp/api/ponto/ensaio")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_ensaio():
    """O modo de teste: a obra de teste e as pessoas de teste."""
    from .core import ensaio
    with db.conexao() as conn:
        return _ok(**ensaio.situacao(conn))


@bp.route("/erp/api/ponto/ensaio/obra", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_ensaio_obra():
    """Grava a obra de teste no lugar onde quem configura está agora."""
    from .core import ensaio
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        r = ensaio.gravar_obra(conn, latitude=d.get("latitude"), longitude=d.get("longitude"),
                               coordenada=str(d.get("coordenada") or ""),
                               raio_metros=d.get("raio_metros"), por=quem.nome)
    return _ok(**r)


@bp.route("/erp/api/ponto/ensaio/desligar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_ensaio_desligar():
    from .core import ensaio
    quem = _quem()
    with db.conexao() as conn:
        return _ok(**ensaio.desligar(conn, quem.nome))


@bp.route("/erp/api/ponto/ensaio/pessoa", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_ensaio_pessoa():
    from .core import ensaio
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        r = ensaio.gravar_pessoa(conn, nome=d.get("nome") or "", cpf=d.get("cpf") or "",
                                 celular=d.get("celular") or "", por=quem.nome)
    return _ok(**r)


@bp.route("/erp/api/ponto/ensaio/pessoa/<int:colaborador_id>/codigo", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_ensaio_codigo(colaborador_id: int):
    """O código de primeiro acesso da pessoa de teste, na tela (sem WhatsApp).
    Pessoa que não é de teste responde 404."""
    from .core import ensaio
    quem = _quem()
    with db.conexao() as conn:
        codigo = ensaio.codigo_de_acesso(conn, colaborador_id, quem.nome)
    return _ok(codigo=codigo)


@bp.route("/erp/api/ponto/licencas")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_licencas():
    """As licenças da lei (CLT art. 473 e leis próprias), para escolher no pedido."""
    from .core import licencas
    with db.conexao() as conn:
        return _ok(licencas=licencas.listar(conn, so_ativos=request.args.get("todas") not in ("1", "true")),
                   disponivel=licencas.disponivel(conn))


@bp.route("/erp/api/ponto/licencas/<codigo>", methods=["POST"])
@login_obrigatorio
@permissao("aprovar_afastamento")
@_api
def ponto_api_licenca_gravar(codigo: str):
    """O DP ajusta dias, limite e documento (a convenção pode dar mais que a lei)."""
    from .core import licencas, ocorrencias as oc
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        if not licencas.disponivel(conn):
            raise ErroDeValidacao("aplique as atualizações do ponto (migração 006) antes")
        t = licencas.gravar(conn, codigo, d, quem.nome)
    oc._NOME_SUBTIPO.clear()
    logger.info("Ponto: licença %s ajustada por %s — %s", codigo, quem.nome, t)
    return _ok(licenca=t)


@bp.route("/erp/api/ponto/rosto")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_rosto():
    """A conferência do rosto (AWS): a regra, o gasto do mês e a estimativa."""
    from .core import rosto
    with db.conexao() as conn:
        r = rosto.retrato(conn)
        obras = [{"id": o["id"], "codigo": o["codigo"], "nome": o["nome"]} for o in cadastros.listar_obras(conn)]
    return _ok(**r, obras=obras)


@bp.route("/erp/api/ponto/rosto", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_rosto_gravar():
    from .core import rosto
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        if not rosto.disponivel(conn):
            raise ErroDeValidacao("aplique as atualizações do ponto (migração 007) antes")
        rosto.gravar(conn, d, quem.nome)
        r = rosto.retrato(conn)
    return _ok(**r)


@bp.route("/erp/api/ponto/rosto/rodar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_rosto_rodar():
    """Uma rodada agora (normalmente roda sozinha a cada 10 minutos)."""
    from .core import rosto
    with db.conexao() as conn:
        feito = rosto.conferir(conn)
    return _ok(rodada=feito)


@bp.route("/erp/api/ponto/quem-valida")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_quem_valida():
    with db.conexao() as conn:
        r = validacao.para_tela(conn)
    return _ok(**r)


@bp.route("/erp/api/ponto/quem-valida", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_quem_valida_gravar():
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        r = validacao.gravar(conn, d.get("regra") or {}, quem.nome)
        tela = validacao.para_tela(conn)
    return _ok(pedidos_realinhados=r["pedidos_realinhados"], **tela)


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


@bp.route("/erp/api/ponto/marcacoes/<int:marcacao_id>/miniatura")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_api_miniatura(marcacao_id: int):
    """A foto pequena, para o mosaico. A foto de uma batida nunca muda: o
    navegador pode guardar por um dia."""
    quem = _quem()
    with db.conexao() as conn:
        m = db.um(conn, "SELECT obra_id, foto_id FROM ponto.marcacoes WHERE id = :id", id=marcacao_id)
        if not m or not m["foto_id"] or not quem.alcanca_obra(m["obra_id"]):
            return jsonify({"ok": False, "erro": "não encontrado"}), 404
        try:
            dados = fotos.miniatura(conn, m["foto_id"])
        except Exception:  # noqa: BLE001 — expurgada, Drive fora do ar
            return jsonify({"ok": False, "erro": "foto não disponível"}), 404
    return Response(dados, mimetype="image/jpeg", headers={"Cache-Control": "private, max-age=86400"})


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/foto-cadastral")
@login_obrigatorio
@permissao("ver_ponto")
def ponto_api_foto_cadastral(colaborador_id: int):
    quem = _quem()
    with db.conexao() as conn:
        try:
            _exigir_pessoa(conn, quem, colaborador_id)
            dados = mosaico.foto_cadastral(conn, colaborador_id)
        except (NaoEncontrado, LookupError):
            return jsonify({"ok": False, "erro": "não encontrado"}), 404
        except Exception:  # noqa: BLE001
            return jsonify({"ok": False, "erro": "foto não disponível"}), 404
    return Response(dados, mimetype="image/jpeg", headers={"Cache-Control": "private, max-age=3600"})


@bp.route("/erp/api/ponto/pessoas/<int:colaborador_id>/foto-cadastral", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_definir_foto_cadastral(colaborador_id: int):
    """"Usar como foto de cadastro": a foto de uma batida da própria pessoa vira
    a referência do mosaico."""
    quem, d = _quem(), _corpo()
    try:
        marcacao_id = int(d.get("marcacao_id"))
    except (TypeError, ValueError):
        raise ErroDeValidacao("diga de qual batida é a foto", campo="marcacao_id")
    with db.conexao() as conn:
        _exigir_pessoa(conn, quem, colaborador_id)
        mosaico.definir_foto_cadastral(conn, colaborador_id, marcacao_id)
    logger.info("Ponto: foto de cadastro da pessoa %s definida por %s (batida %s)",
                colaborador_id, quem.nome, marcacao_id)
    return _ok()


# ---------------------------------------------------------------------------
# Mosaico de fotos
# ---------------------------------------------------------------------------
def _obra_no_alcance(conn, quem: Quem, referencia) -> dict:
    o = cadastros.resolver_obra(conn, referencia)
    if not o or not quem.alcanca_obra(o["id"]):
        raise NaoEncontrado("obra não encontrada")
    return o


def _data(valor, padrao: dt.date) -> dt.date:
    if not valor:
        return padrao
    try:
        return dt.date.fromisoformat(str(valor))
    except ValueError as e:
        raise ErroDeValidacao("data ilegível (AAAA-MM-DD)", campo="data") from e


@bp.route("/erp/api/ponto/mosaico")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_mosaico():
    quem = _quem()
    with db.conexao() as conn:
        o = _obra_no_alcance(conn, quem, request.args.get("obra"))
        r = mosaico.montar(conn, int(o["id"]), _data(request.args.get("data"),
                                                     horario.hoje() - dt.timedelta(days=1)))
    r["pode_conferir"] = bool(quem.supervisor)
    return _ok(**r)


@bp.route("/erp/api/ponto/mosaico/pendentes")
@login_obrigatorio
@permissao("ver_ponto")
@_api
def ponto_api_mosaico_pendentes():
    quem = _quem()
    with db.conexao() as conn:
        lista = mosaico.pendentes(conn, quem.obras)
    return _ok(pendentes=lista)


@bp.route("/erp/api/ponto/mosaico/conferir", methods=["POST"])
@login_obrigatorio
@permissao("tratar_ponto")
@_api
def ponto_api_mosaico_conferir():
    """Confirma o dia. Foto marcada como suspeita manda a batida para análise."""
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        o = _obra_no_alcance(conn, quem, d.get("obra"))
        r = mosaico.conferir(conn, int(o["id"]), _data(d.get("data"), horario.hoje()),
                             usuario_id=quem.usuario_id, usuario_nome=quem.nome,
                             nota=d.get("nota", ""), suspeitas=d.get("suspeitas") or [])
    return _ok(**r)


@bp.route("/erp/api/ponto/mosaico/obras")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_mosaico_obras():
    """Quais obras têm o mosaico obrigatório, e quem pode ser o responsável:
    os usuários do ERP que tratam o ponto (são eles que conseguem confirmar)."""
    from app.apps.erp.core.auth.permissoes import pode_com_banco
    from app.apps.erp.db.models.cadastros import Usuario
    with db.conexao() as conn:
        obras = mosaico.configuracao_das_obras(conn)
    candidatos = []
    with R.get_session() as s:
        for u in s.query(Usuario).filter(Usuario.ativo.is_(True)).order_by(Usuario.nome).all():
            try:
                pode_conferir = pode_com_banco(s, u, "tratar_ponto")
            except Exception:  # noqa: BLE001 — perfil mal configurado não derruba a tela
                pode_conferir = False
            candidatos.append({"id": u.id, "nome": u.nome, "pode_conferir": pode_conferir,
                               "tem_telefone": bool(envios.telefone_valido(u.telefone))})
    return _ok(obras=obras, usuarios=candidatos)


@bp.route("/erp/api/ponto/mosaico/obras/<int:obra_id>", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_mosaico_obra_gravar(obra_id: int):
    d = _corpo()
    with db.conexao() as conn:
        if not cadastros.obra_por_id(conn, obra_id):
            raise NaoEncontrado("obra não encontrada")
        resp = d.get("responsavel_id")
        c = mosaico.configurar_obra(conn, obra_id, obrigatorio=bool(d.get("obrigatorio")),
                                    responsavel_id=(int(resp) if resp not in (None, "", 0, "0") else None))
    return _ok(obra=c)


@bp.route("/erp/api/ponto/cercas")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_cercas():
    """A cerca de cada obra: coordenada (do cadastro do ERP), raio e o que
    fazer fora dela. Mostra quantas batidas a cerca barrou na última semana."""
    from .core import marcacoes as marc
    with db.conexao() as conn:
        obras = cadastros.listar_obras(conn, so_ativas=True)
        tem_modo = db.tem_coluna(conn, "obra_config", "fora_da_cerca")
        tem_duas = db.tem_coluna(conn, "obra_config", "intervalo_pre_assinalado")
        so_duas = ({int(l["obra_id"]) for l in db.todos(
            conn, "SELECT obra_id FROM ponto.obra_config WHERE intervalo_pre_assinalado")} if tem_duas else set())
        barradas = {r["obra"]: int(r["n"]) for r in db.todos(conn, """
            SELECT obra_informada AS obra, count(*) AS n FROM ponto.recusas
             WHERE (motivo LIKE 'fora da área da obra%' OR motivo LIKE 'localização desligada%')
               AND criado_em > now() - interval '7 days'
             GROUP BY obra_informada""")}
        saida = []
        for o in obras:
            j = cadastros.obra_para_json(o)
            j["tem_coordenada"] = j["latitude"] is not None and j["longitude"] is not None
            j["fora_da_cerca"] = marc.modo_fora_da_cerca(conn, int(o["id"])) if tem_modo else "ANALISAR"
            j["barradas_7_dias"] = barradas.get(o["codigo"], 0)
            j["so_entrada_e_saida"] = int(o["id"]) in so_duas
            saida.append(j)
        from .core import base_obras
        base_planilha = base_obras.usando(conn)
    return _ok(obras=saida, modo_disponivel=tem_modo, base_planilha=base_planilha, batidas_disponivel=tem_duas)


@bp.route("/erp/api/ponto/cercas/<int:obra_id>", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_cerca_gravar(obra_id: int):
    """Raio da cerca (20 m a 5 km) e o que fazer fora dela: BLOQUEAR (o padrão)
    ou ANALISAR (aceita e manda para conferência — obra espalhada, estrada)."""
    from .core import marcacoes as marc
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        if not cadastros.obra_por_id(conn, obra_id):
            raise NaoEncontrado("obra não encontrada")
        raio = d.get("raio_metros")
        if raio not in (None, ""):
            try:
                raio = int(raio)
            except (TypeError, ValueError):
                raise ErroDeValidacao("raio em metros, só números", campo="raio_metros")
        else:
            raio = None
        cadastros.gravar_config_obra(conn, obra_id, raio_metros=raio)
        if "fora_da_cerca" in d:
            modo = str(d.get("fora_da_cerca") or "").upper()
            if modo not in marc.MODOS_FORA_DA_CERCA:
                raise ErroDeValidacao("use BLOQUEAR ou ANALISAR", campo="fora_da_cerca")
            if not db.tem_coluna(conn, "obra_config", "fora_da_cerca"):
                raise ErroDeValidacao("aplique as atualizações do ponto antes de mudar isto")
            db.executar(conn, "UPDATE ponto.obra_config SET fora_da_cerca = :m, atualizado_em = now() "
                              "WHERE obra_id = :o", m=modo, o=obra_id)
        if "so_entrada_e_saida" in d:
            # Só entrada e saída — o intervalo pré-assinalado (migração 009, 09/10/2026).
            if not db.tem_coluna(conn, "obra_config", "intervalo_pre_assinalado"):
                raise ErroDeValidacao("aplique as atualizações do ponto (migração 009) antes de mudar isto")
            db.executar(conn, "UPDATE ponto.obra_config SET intervalo_pre_assinalado = :v, atualizado_em = now() "
                              "WHERE obra_id = :o", v=bool(d.get("so_entrada_e_saida")), o=obra_id)
        logger.info("Ponto: cerca da obra %s ajustada por %s (raio %s, fora: %s, só entrada e saída: %s)",
                    obra_id, quem.nome, raio, d.get("fora_da_cerca"), d.get("so_entrada_e_saida"))
        o = cadastros.obra_por_id(conn, obra_id)
        j = cadastros.obra_para_json(o)
        j["fora_da_cerca"] = marc.modo_fora_da_cerca(conn, obra_id)
        j["tem_coordenada"] = j["latitude"] is not None and j["longitude"] is not None
        j["so_entrada_e_saida"] = bool(db.tem_coluna(conn, "obra_config", "intervalo_pre_assinalado") and db.um(
            conn, "SELECT 1 FROM ponto.obra_config WHERE obra_id = :o AND intervalo_pre_assinalado", o=obra_id))
    return _ok(obra=j)


@bp.route("/erp/api/ponto/envios")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_envios():
    """A fila de mensagens: o ritmo, o que saiu e o que espera."""
    with db.conexao() as conn:
        r = envios.resumo(conn)
        ultimos = db.todos(conn, """
            SELECT e.id, e.tipo, e.motivo, e.status, e.agendado_para, e.enviado_em, e.tentativas,
                   e.resultado, e.telefone, c.nome AS colaborador
              FROM ponto.envios e LEFT JOIN public.colaboradores c ON c.id = e.colaborador_id
             ORDER BY e.id DESC LIMIT 60""")
    for e in ultimos:
        e["telefone"] = "***" + (e["telefone"] or "")[-4:]
        e["agendado_para"] = horario.texto(e["agendado_para"])
        e["enviado_em"] = horario.texto(e["enviado_em"])
    return _ok(**r, envios=ultimos)


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
    """Ajuste de batida, compensação, folga — e, desde 06/10/2026, atestado e
    licença com o documento — registrados pelo responsável da obra; quem valida
    é a regra de "Quem valida o quê" (atestado: sempre o DP)."""
    quem, d = _quem(), _corpo()
    if str(d.get("tipo") or "").upper() not in ("AJUSTE_BATIDA", "COMPENSACAO", "FOLGA_BANCO",
                                                "ATESTADO", "LICENCA"):
        raise ErroDeValidacao("férias, afastamento e abono são lançados pelo DP", campo="tipo")
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
        padrao = escalas.escala_padrao(conn)
    return _ok(escalas=[_escala_json(e) for e in lista], padrao_id=(padrao or {}).get("id"))


@bp.route("/erp/api/ponto/escalas/padrao", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_escala_padrao():
    """A escala de quem ainda não tem escala própria — sem ela não há falta."""
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        e = escalas.gravar_padrao(conn, d.get("escala_id"), quem.nome)
    logger.info("Ponto: escala padrão da empresa = %s (%s)", (e or {}).get("nome"), quem.nome)
    return _ok(padrao_id=(e or {}).get("id"))


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
                raise ErroDeValidacao("CPF do responsável não está no cadastro", campo="cpf")
            colaborador_id = int(p["id"])
        else:
            # Sem CPF: vale quem já está no aparelho (quem entrou nele com CPF e
            # PIN, ou o responsável de antes).
            colaborador_id = dispositivos.por_id(conn, dispositivo_id).get("colaborador_id")
        if not colaborador_id:
            # Todo aparelho tem responsável (decisão do dono, 07/10/2026).
            raise ErroDeValidacao("diga o CPF do responsável — quem fica com o aparelho", campo="cpf")
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
        if d.get("manter_grupo") and not autorizados:
            # Alterar um aparelho de grupo sem redigitar o grupo: fica o que já está.
            autorizados = sorted(dispositivos.autorizados_de(conn, dispositivo_id))
        a = dispositivos.aprovar(conn, dispositivo_id, perfil=d.get("perfil", "COMPARTILHADO"),
                                 aprovado_por=quem.nome, colaborador_id=colaborador_id,
                                 descricao=d.get("descricao"), autorizados=autorizados, obras=obras,
                                 valido_ate=d.get("valido_ate"))
    return _ok(dispositivo=dispositivos.para_json(a), substituidos=len(a.get("substituidos") or []))


@bp.route("/erp/api/ponto/dispositivos/<int:dispositivo_id>")
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_dispositivo(dispositivo_id: int):
    """Um aparelho com o grupo dele — para "Alterar" abrir com tudo preenchido."""
    with db.conexao() as conn:
        a = dispositivos.para_json(dispositivos.detalhado(conn, dispositivo_id))
        a["autorizados"] = [{"nome": p["nome"], "cpf": p["cpf"]}
                            for p in dispositivos.autorizados_detalhados(conn, dispositivo_id)]
    return _ok(dispositivo=a)


@bp.route("/erp/api/ponto/dispositivos/<int:dispositivo_id>/renovar", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_dispositivo_renovar(dispositivo_id: int):
    """Renova a permissão de grupo (temporária, no máximo 90 dias)."""
    quem, d = _quem(), _corpo()
    with db.conexao() as conn:
        a = dispositivos.renovar_grupo(conn, dispositivo_id, valido_ate=d.get("valido_ate"), por=quem.nome)
    return _ok(dispositivo=dispositivos.para_json(a))


@bp.route("/erp/api/ponto/dispositivos/<int:dispositivo_id>/tirar-sem-uso", methods=["POST"])
@login_obrigatorio
@permissao("configurar_ponto")
@_api
def ponto_api_dispositivo_tirar_sem_uso(dispositivo_id: int):
    """Tira do grupo quem não bate mais por este aparelho."""
    quem = _quem()
    with db.conexao() as conn:
        dispositivos.por_id(conn, dispositivo_id)
        n = dispositivos.tirar_sem_uso(conn, dispositivo_id, quem.nome)
    return _ok(retirados=n)


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
        tem_003 = tem_parametros and db.tem_003(conn)
        qr_auto = parametros.ler(conn, envios.QR_AUTOMATICO, "") == "1" if tem_003 else False
        aviso_foto = parametros.ler(conn, envios.AVISO_SEM_FOTO, "") == "1" if tem_003 else False
    return _ok(migracoes=estado, telefones_resumo=telefones, ultima_rotina=ultima,
               drive_configurado=fotos.drive_configurado(), fila_fotos=fila_fotos,
               fila_documentos=int(fila_docs),
               endereco_app="/ponto/app",
               qr_envio_automatico=qr_auto, aviso_sem_foto=aviso_foto,
               whatsapp_configurado=envios.whatsapp_pronto(), recursos_003=tem_003)


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
        if "qr_envio_automatico" in d:
            parametros.gravar(conn, envios.QR_AUTOMATICO, envios.validar_ligado(d["qr_envio_automatico"]),
                              quem.nome)
            logger.info("Ponto: envio automático de QR %s por %s",
                        "LIGADO" if d["qr_envio_automatico"] else "desligado", quem.nome)
        if "aviso_sem_foto" in d:
            parametros.gravar(conn, envios.AVISO_SEM_FOTO, envios.validar_ligado(d["aviso_sem_foto"]),
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
