# -*- coding: utf-8 -*-
"""
A CENTRAL DE ASSINATURAS NO APLICATIVO DO PONTO (23/11/2026).

A decisão do dono: *"vamos concentrar o aplicativo do ponto para ser o caminho para a coleta dessas assinaturas,
porque está tudo diretamente associado ao colaborador e o ponto já tem esse tipo de coisa"*. E os dois caminhos que
ele pediu, porque a obra é o mundo real:

1. A PRÓPRIA PESSOA, no "Meu ponto": vê o que tem para assinar — os documentos do DP (aviso prévio, termo de
   compensação…), a folha de ponto do mês fechado e o termo do ponto — e assina com o dedo e o PIN dela.
2. O ADMINISTRATIVO DA OBRA conduz, no celular dele: *"sempre vai ter algum funcionário que não tem muita
   familiaridade com telefone (…) pouco estudo, pouca leitura"*. Na tela (a pessoa assina com o dedo e o PIN), no
   papel (foto do papel assinado; o DP valida) ou a recusa (com duas testemunhas; o DP valida).

ONDE MORA CADA COISA — e não se copia regra:
- os documentos do DP: `dp_termos`, regra em `app/apps/erp/core/termos.py` (ERP, migrações 117 e 126);
- a folha de ponto: `core/assinaturas.py` (ponto); o termo do ponto: `core/termo.py` (ponto).
Este módulo só junta as três listas e passa a palavra para quem decide.

O ALCANCE do administrativo é o de sempre: quem ele alcança e bate ponto (o resumo da equipe). Documento de quem
está fora: "não encontrado".
"""
from __future__ import annotations

import base64
import datetime as dt
import logging

from flask import request

from . import auth, db, horario
from .app_colaborador import _corpo, _ok, pessoa_do_espelho
from .app_obra import _contexto, _equipe_do_termo, _local, _logado
from .erros import ErroDeValidacao, NaoAutenticado, NaoEncontrado
from .routes import bp

logger = logging.getLogger("ponto.app_documentos")

MESES_DE_FOLHA = 3                 # as folhas fechadas que a pessoa ainda pode assinar: as dos últimos 3 meses
MAX_FOTO = 8 * 1024 * 1024


def _erp(fazer):
    """Roda `fazer(s)` numa sessão do ERP e traduz o erro do ERP para o do ponto (a mesma frase)."""
    from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
    from app.apps.erp.db.database import get_session
    try:
        with get_session() as s:
            r = fazer(s)
            s.commit()
            return r
    except ErroValidacao as e:
        raise ErroDeValidacao(str(e)) from None
    except ErroNaoEncontrado as e:
        raise NaoEncontrado(str(e) or "documento não encontrado") from None


def _termo_voltou(s, termo_id: int) -> None:
    """O documento do DP que nasceu de um atendimento: o atendimento anda sozinho (o mesmo do portal da obra)."""
    try:
        from app.apps.erp.core import pessoal_solicitacoes
        pessoal_solicitacoes.termo_voltou(s, termo_id, None)
    except Exception:  # noqa: BLE001 — a assinatura já valeu; o despacho se refaz pelo DP
        logger.exception("Ponto: o atendimento do documento %s não andou sozinho", termo_id)


# ---------------------------------------------------------------------------
# 1. A própria pessoa
# ---------------------------------------------------------------------------
def _eu(conn) -> dict:
    p = _logado(conn)
    if not p:
        raise NaoAutenticado("entre com o seu CPF e PIN")
    return p


def _minhas_folhas(conn, pessoa: dict) -> list[dict]:
    """As folhas de ponto fechadas e ainda sem assinatura dos últimos meses."""
    from .core import assinaturas, competencias
    if not assinaturas.disponivel(conn):
        return []
    saida, comp = [], competencias.primeiro_dia(horario.hoje())
    for _ in range(MESES_DE_FOLHA):
        comp = competencias.primeiro_dia(comp - dt.timedelta(days=1))
        if not competencias.esta_fechada(conn, comp):
            continue
        sit = assinaturas.situacao(conn, [pessoa], comp)
        linha = sit["pessoas"][0] if sit["pessoas"] else {}
        if linha.get("situacao") == "FALTA":
            saida.append({"competencia": sit["competencia"], "prazo": sit["prazo"], "vencido": sit["vencido"]})
    return saida


@bp.route("/app/api/meus-documentos")
@auth.exige_colaborador_ou_aparelho
def app_api_meus_documentos():
    """O que EU tenho para assinar (e o que já assinei dos documentos do DP)."""
    from .core import termo
    from app.apps.erp.core import termos
    with db.conexao() as conn:
        p = _eu(conn)
        cid = int(p["id"])
        folhas = _minhas_folhas(conn, p)
        termo_ponto = (termo.situacao_de(conn, [cid])[cid]["situacao"] == "FALTA") if termo.disponivel(conn) else False
    abertos, historico = _erp(lambda s: (termos.pendentes(s, None, colaborador_id=cid), termos.da_pessoa(s, cid)))
    feitos = [h for h in historico if h["situacao"] != "ABERTO"]
    return _ok(documentos=abertos, folhas=folhas, termo_do_ponto=termo_ponto, assinados=feitos[:20],
               total=len(abertos) + len(folhas) + (1 if termo_ponto else 0))


@bp.route("/app/api/meus-documentos/<int:termo_id>")
@auth.exige_colaborador_ou_aparelho
def app_api_meu_documento(termo_id: int):
    from app.apps.erp.core import termos
    with db.conexao() as conn:
        cid = int(_eu(conn)["id"])
    return _ok(documento=_erp(lambda s: termos.para_assinar(s, termo_id, None, colaborador_id=cid)))


@bp.route("/app/api/meus-documentos/<int:termo_id>", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_assinar_meu_documento(termo_id: int):
    """Eu assino o MEU documento: o traço do dedo e o meu PIN de novo."""
    from app.apps.erp.core import termos
    d = _corpo()
    with db.conexao() as conn:
        p = _eu(conn)
    local = _local(d) or {}

    def fazer(s):
        r = termos.assinar(s, termo_id, None, pin=str(d.get("pin") or ""), assinatura=d.get("assinatura"),
                           conduzido_por={"colaborador_id": int(p["id"]), "nome": p["nome"]},
                           latitude=local.get("latitude"), longitude=local.get("longitude"),
                           ip=auth.ip_de_quem_chama(), pela_pessoa=True)
        _termo_voltou(s, termo_id)
        return r
    r = _erp(fazer)
    logger.info("Ponto: %s assinou o documento %s pelo próprio celular", p["nome"], r.get("numero"))
    return _ok(**r), 201


@bp.route("/app/api/minha-folha")
@auth.exige_colaborador_ou_aparelho
def app_api_minha_folha():
    """A MINHA folha do mês, para eu ler antes de assinar (a mesma do PDF)."""
    from .core import competencias, espelho
    with db.conexao() as conn:
        p = _eu(conn)
        comp = competencias.primeiro_dia(request.args.get("competencia") or horario.hoje())
        esp = espelho.do_mes(conn, int(p["id"]), comp)
    return _ok(pessoa=pessoa_do_espelho(p), competencia=esp["competencia"], resumo=esp["resumo"],
               compensacoes=esp.get("compensacoes") or [],
               dias=[{k: x.get(k) for k in ("data", "dia_semana", "situacao", "previsto", "trabalhado", "extra", "debito")}
                     | {"batidas": [b["hora"] + (" (aj)" if b["status"] == "AJUSTADA" else "") for b in x["batidas"]
                                    if b["status"] != "REJEITADA"],
                        "ocorrencia": (x.get("ocorrencia") or {}).get("rotulo")} for x in esp["dias"]])


@bp.route("/app/api/minha-folha", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_assinar_minha_folha():
    from .core import assinaturas
    d = _corpo()
    with db.conexao() as conn:
        p = _eu(conn)
        local = _local(d) or {}
        r = assinaturas.assinar(conn, colaborador_id=int(p["id"]), competencia=d.get("competencia"),
                                pin=d.get("pin"), assinatura=d.get("assinatura"),
                                conduzida_por={"id": int(p["id"]), "nome": p["nome"]},
                                latitude=local.get("latitude"), longitude=local.get("longitude"),
                                ip=auth.ip_de_quem_chama(), pela_pessoa=True)
    return _ok(**r), 201


# ---------------------------------------------------------------------------
# 2. O administrativo conduz
# ---------------------------------------------------------------------------
def _equipe_ids(conn) -> tuple[object, set[int]]:
    ctx = _contexto(conn)
    if not ctx.pessoa:
        raise NaoAutenticado("entre com o seu CPF e PIN")
    return ctx, {int(p["id"]) for p in _equipe_do_termo(conn, ctx)}


def _do_alcance(s, termo_id: int, equipe: set[int]) -> dict:
    from app.apps.erp.core import termos
    t = termos.ler(s, termo_id)
    if int(t["colaborador_id"]) not in equipe:
        from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
        raise ErroNaoEncontrado("Documento não encontrado.")
    return t


@bp.route("/app/api/equipe/documentos")
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_documentos():
    """Os documentos do DP que a equipe de quem conduz tem para assinar (os atrasados primeiro)."""
    from app.apps.erp.core import termos
    with db.conexao() as conn:
        ctx, equipe = _equipe_ids(conn)
    eu = int(ctx.pessoa["id"])
    lista = _erp(lambda s: termos.pendentes(s, [], pessoas=sorted(equipe - {eu}))) if equipe - {eu} else []
    lista.sort(key=lambda x: (not x["atrasado"], x["prazo"] or "9999", x["pessoa"]))
    return _ok(documentos=lista, atrasados=sum(1 for x in lista if x["atrasado"]))


@bp.route("/app/api/equipe/documentos/<int:termo_id>")
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_documento(termo_id: int):
    from app.apps.erp.core import termos
    with db.conexao() as conn:
        _, equipe = _equipe_ids(conn)

    def fazer(s):
        _do_alcance(s, termo_id, equipe)
        return termos.para_assinar(s, termo_id, None)
    return _ok(documento=_erp(fazer))


@bp.route("/app/api/equipe/documentos/<int:termo_id>", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_documento_colher(termo_id: int):
    """Quem conduz colhe: `acao` = "assinar" (a pessoa, com o dedo e o PIN dela), "papel" (a foto do papel assinado)
    ou "recusa" (o que aconteceu e duas testemunhas). Papel e recusa valem depois que o DP olha."""
    from app.apps.erp.core import termos
    d = _corpo()
    acao = str(d.get("acao") or "assinar")
    with db.conexao() as conn:
        ctx, equipe = _equipe_ids(conn)
    conduz = {"colaborador_id": int(ctx.pessoa["id"]), "nome": ctx.pessoa["nome"]}
    local = _local(d) or {}

    def fazer(s):
        _do_alcance(s, termo_id, equipe)
        if acao == "papel":
            bruto = str(d.get("foto") or "")
            if "," in bruto[:100]:
                bruto = bruto.split(",", 1)[1]
            try:
                conteudo = base64.b64decode(bruto, validate=True)
            except Exception:  # noqa: BLE001
                raise ErroDeValidacao("a foto do papel não chegou — tire de novo") from None
            if not conteudo or len(conteudo) > MAX_FOTO:
                raise ErroDeValidacao("a foto do papel não chegou (ou passou de 8 MB) — tire de novo")
            r = termos.enviar_papel(s, termo_id, None, conteudo=conteudo,
                                    nome_arquivo=str(d.get("nome_arquivo") or "papel_assinado.jpg"), conduzido_por=conduz)
        elif acao == "recusa":
            r = termos.registrar_recusa(s, termo_id, None, motivo=str(d.get("motivo") or ""),
                                        testemunhas=d.get("testemunhas") or [], conduzido_por=conduz)
        else:
            r = termos.assinar(s, termo_id, None, pin=str(d.get("pin") or ""), assinatura=d.get("assinatura"),
                               conduzido_por=conduz, latitude=local.get("latitude"),
                               longitude=local.get("longitude"), ip=auth.ip_de_quem_chama())
        _termo_voltou(s, termo_id)
        return r
    r = _erp(fazer)
    logger.info("Ponto: documento %s — %s, conduzido por %s", r.get("numero"), acao, conduz["nome"])
    return _ok(**r), 201
