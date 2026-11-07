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

import datetime as dt
import logging

from flask import request, session

from . import auth, db, horario
from .app_colaborador import _corpo, _ok, folha_do_espelho, folha_que_nao_abriu, folhas_da_equipe, pessoa_do_espelho
from .core import cadastros, dispositivos, espelho, forma_de_bater, ocorrencias, papeis
from .core.ocorrencias import Quem
from .erros import ErroDeValidacao, NaoAutenticado, NaoEncontrado, Recusada
from .routes import bp

logger = logging.getLogger("ponto.app_obra")

ROTULO_PAPEL = {"ADMINISTRATIVO": "administrativo da obra", "RESPONSAVEL_OBRA": "responsável pelo ponto da obra",
                "RESPONSAVEL_EQUIPE": "responsável pelo ponto de equipe", "APARELHO_OBRA": "ponto da obra",
                "APARELHO_EQUIPE": "ponto de equipe", "LIBERADO": "liberado pelo DP"}


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
    if not papeis.entra_no_app(conn, p):
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
        from .core import negativas, qualidade
        responde_por_equipe = bool(p) and qualidade.e_responsavel(conn, int(p["id"]))
        # os pedidos negados que esta pessoa ainda não viu — os dela e os que pediu por outros
        avisos_de_negativa = negativas.avisos(conn, int(p["id"])) if p else []
        # o termo do ponto (07/11/2026): quem bate e ainda não assinou vê o aviso no começo da tela
        from .core import termo
        termo_pendente = bool(p) and bool(p.get("ativo_no_ponto")) and termo.disponivel(conn) and \
            termo.situacao_de(conn, [int(p["id"])])[int(p["id"])]["situacao"] == "FALTA"
    # No ponto da obra, "você entrou como" é o papel DA PESSOA aqui — não o do
    # aparelho (10/10/2026: a tela dizia "você entrou como ponto da obra").
    papel_aqui = None
    if da_obra and p:
        if a.get("colaborador_id") == p["id"]:
            papel_aqui = "responsável por este aparelho"
        elif administrativo:
            papel_aqui = "administrativo da obra"
    return _ok(
        pessoa=({"nome": p["nome"], "primeiro_nome": p["nome"].split(" ")[0],
                 "administrativo": administrativo, "papel_aqui": papel_aqui,
                 # quem não bate ponto e entra pela liberação (31/10/2026): sem "bater", "meu ponto" e "meu QR"
                 "bate_ponto": bool(p.get("ativo_no_ponto")), "termo_pendente": termo_pendente} if p else None),
        da_obra=bool(da_obra), perfil=(a or {}).get("perfil"),
        pode={"bater_aqui": bool(da_obra), "bater_o_meu": bate_no_meu,
              "entregar_aqui": bool(da_obra), "pedir_o_meu": pede_o_meu,
              "consultar": not alc.vazio, "consultar_pelo_cpf": bool(da_obra),
              "pedir_por_outros": bool(alc.pode_pedir and not alc.vazio),
              "qr_por_cpf": bool(da_obra), "meu_qr": bool(p) and not da_obra and bool(p.get("ativo_no_ponto")),
              # o ranking da equipe de quem responde por um ponto da obra (17/10/2026)
              "qualidade_equipe": responde_por_equipe},
        alcance=_alcance_json(alc), negativas=avisos_de_negativa)


@bp.route("/app/api/ranking")
@auth.exige_colaborador_ou_aparelho
def app_api_ranking():
    """O ranking do ponto (29/10/2026, ranking.py): as obras e os 25 primeiros funcionários, e a posição de
    quem está vendo. Lê a conta GUARDADA — abre na hora; a conta é refeita de 2 em 2 horas."""
    from .core import ranking
    mes = request.args.get("mes") or "atual"
    hoje = horario.hoje()
    comp = f"{hoje:%Y-%m}" if mes != "anterior" else f"{(hoje.replace(day=1) - dt.timedelta(days=1)):%Y-%m}"
    with db.conexao() as conn:
        p = _logado(conn)
        if not p:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        dados = ranking.ler(conn, comp)
    if not dados:
        ranking.disparar_se_preciso()
        return _ok(pronto=False, competencia=comp, na_janela=ranking.na_janela())
    eu = (dados.get("posicoes") or {}).get(str(p["id"]))
    return _ok(pronto=True, **{k: v for k, v in dados.items() if k != "posicoes"}, eu=eu)


@bp.route("/app/api/obra-aviso")
@auth.exige_colaborador_ou_aparelho
def app_api_obra_aviso():
    """A obra escolhida à mão para o ajuste provavelmente está errada? (29/10/2026, obra_suspeita.py)
    `pessoa`: o pedido por outra pessoa — com o mesmo alcance da consulta. `bilhete`: a pessoa identificada no ponto
    da obra (31/10/2026, o dono: "precisa alertar se for selecionada obra que a pessoa não tem batida"). Sem os dois,
    quem está logado."""
    from .core import obra_suspeita
    with db.conexao() as conn:
        alvo_id = request.args.get("pessoa", type=int)
        if request.args.get("bilhete"):
            from .app_colaborador import _quem_no_aparelho
            alvo_id = int(_quem_no_aparelho(conn, {"bilhete": request.args.get("bilhete")})[0]["id"])
        elif alvo_id:
            ctx = _contexto(conn)
            _alvo(conn, ctx, alvo_id)
        else:
            p = _logado(conn)
            if not p:
                raise NaoAutenticado("entre com o seu CPF e PIN")
            alvo_id = int(p["id"])
        avisos = obra_suspeita.da_pessoa(conn, alvo_id, request.args.get("obra"))
    return _ok(avisos=avisos)


@bp.route("/app/api/negativas/<int:ocorrencia_id>/vista", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_negativa_vista(ocorrencia_id: int):
    """O aviso do pedido negado some para quem tocou nele (28/10/2026)."""
    from .core import negativas
    with db.conexao() as conn:
        p = _logado(conn)
        if not p:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        if not negativas.marcar_visto(conn, int(p["id"]), ocorrencia_id):
            raise NaoEncontrado("aviso não encontrado")
    return _ok()


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


def _pessoas_da_equipe(conn, ctx: _Contexto, inicio, fim) -> tuple[dict, set]:
    """Quem é da equipe de quem pede, no período: {id: obra em que aparece (ou None)} e os ids que ele abre daqui.
    O MESMO conjunto para o resumo da equipe e para as folhas da equipe (05/11/2026)."""
    from .core import qualidade
    eu = int(ctx.pessoa["id"])
    obra_aqui, abre = {}, set()
    if not ctx.pelo_aparelho and not ctx.alcance.vazio:
        for p in papeis.pessoas_do_alcance(conn, ctx.alcance, f"{fim:%Y-%m}", exceto=eu):
            obra_aqui[int(p["id"])] = p.get("obra_aqui")
            abre.add(int(p["id"]))
    if qualidade.e_responsavel(conn, eu):
        for i in qualidade.equipe_do_responsavel(conn, eu, inicio, fim):
            if i != eu:
                obra_aqui.setdefault(int(i), None)
    if not obra_aqui:
        raise Recusada(ctx.alcance.sem_alcance or "o resumo é de quem responde pelo ponto de uma obra ou consulta o "
                                                "ponto das pessoas")
    return obra_aqui, abre


@bp.route("/app/api/equipe/resumo")
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_resumo():
    """O RESUMO GERENCIAL do ponto das obras de quem pede (31/10/2026, core/resumo_equipe.py): por obra, quem está em
    dia e quem tem problema — faltas, batida faltando, o que está provavelmente errado, atrasos —, com os atestados e
    os pedidos esperando. Quem aparece: o alcance da consulta (fora do ponto da obra) e quem bateu no aparelho de que
    a pessoa é responsável. `periodo`: mes (padrão) | 7dias | anterior; `obra`: só uma."""
    from .core import qualidade, resumo_equipe
    with db.conexao() as conn:
        ctx = _contexto(conn)
        eu = int(ctx.pessoa["id"]) if ctx.pessoa else None
        if not eu:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        inicio, fim, rotulo = resumo_equipe.periodo(request.args.get("periodo"), horario.hoje())
        obra_aqui, abre = _pessoas_da_equipe(conn, ctx, inicio, fim)
        obra = (request.args.get("obra") or "").strip().upper()
        if obra:
            obra_aqui = {i: o for i, o in obra_aqui.items() if (o or "").upper() == obra or o is None}
        elif len(obra_aqui) > resumo_equipe.MAX_PESSOAS:
            contagem: dict = {}
            for o in obra_aqui.values():
                contagem[o or ""] = contagem.get(o or "", 0) + 1
            return _ok(escolher_obra=[{"obra": o, "pessoas": n} for o, n in sorted(contagem.items()) if o],
                       periodo=rotulo, de=inicio.isoformat(), ate=fim.isoformat())
        grupos = resumo_equipe.montar(conn, list(obra_aqui), inicio, fim,
                                      obra_aqui={i: o for i, o in obra_aqui.items() if o}, abre=abre)
        if obra:
            grupos = [g for g in grupos if g["obra"].upper() == obra]
    return _ok(grupos=grupos, periodo=rotulo, de=inicio.isoformat(), ate=fim.isoformat())


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
        if d.get("escolha") and ctx.aparelho:      # pela busca do nome (27/10/2026)
            from .core import qr
            pessoa = cadastros.colaborador_por_id(conn, qr.conferir_escolha(d["escolha"], int(ctx.aparelho["id"])))
        else:
            pessoa = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(d.get("cpf")))
        if not pessoa or not papeis.no_alcance(conn, ctx.alcance, int(pessoa["id"])):
            raise NaoEncontrado("ninguém com esse CPF batendo ponto nesta obra")
        _abrir_consulta(int(pessoa["id"]))
    return _ok(id=int(pessoa["id"]), nome=pessoa["nome"])


@bp.route("/app/api/obra/buscar-nome", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_obra_buscar_nome():
    """A busca pelo NOME no ponto da obra (27/10/2026, core/busca_nome.py): para quem
    esqueceu o CPF e não tem o QR. 3 letras ou mais, até 6 pessoas, com a foto pequena,
    o nome e a função — nunca o CPF. Só no ponto da obra, com o responsável conectado.
    Cada pessoa vem com uma ESCOLHA assinada, deste aparelho, que serve para bater ou
    para consultar."""
    import base64
    from .core import busca_nome, mosaico, qr
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        if not ctx.pelo_aparelho or not ctx.aparelho:
            raise Recusada("a busca pelo nome é do ponto da obra")
        if not auth.dentro_do_limite("buscar_nome", str(ctx.aparelho["id"]), 240):
            raise Recusada("muitas buscas neste aparelho; espere alguns minutos")
        todas = bool(d.get("todas"))
        if not todas and ctx.alcance.vazio:
            raise Recusada(ctx.alcance.sem_alcance or "a busca vale na obra em que o aparelho está")
        r = busca_nome.buscar(conn, ctx.alcance, d.get("nome"), todas=todas)
        pessoas = []
        for p in r["pessoas"]:
            foto = None
            try:
                foto = "data:image/jpeg;base64," + base64.b64encode(mosaico.foto_cadastral(conn, int(p["id"]))).decode()
            except Exception:  # noqa: BLE001 — sem foto de cadastro (ou o Drive fora): vai só o nome
                foto = None
            pessoas.append({"nome": busca_nome.nome_curto(p["nome"]), "funcao": p.get("funcao"),
                            "obra": p.get("obra_codigo"), "foto": foto,
                            "escolha": qr.emitir_escolha(int(p["id"]), int(ctx.aparelho["id"]))})
        logger.info("Ponto: busca pelo nome no aparelho %s por %s (%s achadas%s)", ctx.aparelho["id"],
                    ctx.pessoa["nome"], len(pessoas), ", todas as obras" if todas else "")
    return _ok(pessoas=pessoas, mais=r["mais"], curta=r["curta"], todas=todas)


@bp.route("/app/api/equipe/<int:colaborador_id>/mes")
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_mes(colaborador_id: int):
    from .core import competencias
    with db.conexao() as conn:
        ctx = _contexto(conn)
        competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
        alvo = _alvo(conn, ctx, colaborador_id, competencias.primeiro_dia(competencia))
        esp = espelho.do_mes(conn, colaborador_id, competencia)
        from .core import negativas
        negativas.nos_dias(conn, colaborador_id, esp["dias"])
    return _ok(pessoa=pessoa_do_espelho(alvo),
               competencia=esp["competencia"], resumo=esp["resumo"], dias=esp["dias"],
               competencia_fechada=esp["competencia_fechada"], pode_pedir=ctx.alcance.pode_pedir,
               obras=list(ctx.alcance.obras.values()), pelo_aparelho=ctx.pelo_aparelho)


def _periodo_da_folha() -> str:
    """O período pedido, como vai no bilhete de impressão (o mês ou o de–até)."""
    return "|".join(str(request.args.get(k) or "") for k in ("competencia", "de", "ate"))


@bp.route("/app/api/equipe/<int:colaborador_id>/espelho/bilhete", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_espelho_bilhete(colaborador_id: int):
    """O bilhete para abrir a folha de outra pessoa (05/11/2026, `qr.emitir_impressao`): pedido pela chamada do
    aplicativo — que leva a identificação do aparelho —, com o MESMO alcance da consulta."""
    from .core import competencias, qr
    from .app_colaborador import periodo_pedido
    with db.conexao() as conn:
        ctx = _contexto(conn)
        periodo = periodo_pedido()
        competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
        _alvo(conn, ctx, colaborador_id, *(periodo or (competencias.primeiro_dia(competencia),)))
    return _ok(bilhete=qr.emitir_impressao(colaborador_id, int(ctx.pessoa["id"]), _periodo_da_folha()))


# ---------------------------------------------------------------------------
# A ASSINATURA DA FOLHA NA TELA (06/11/2026, migração 027, core/assinaturas.py): quem conduz — o operador do ponto da
# obra ou o administrativo — vê a sua equipe do mês (a mesma do resumo), mostra a folha e a pessoa assina com o PIN dela.
# ---------------------------------------------------------------------------
def _equipe_do_mes(conn, ctx: _Contexto, competencia) -> tuple[list[dict], "dt.date"]:
    from .core import competencias
    comp = competencias.primeiro_dia(competencia or (horario.hoje().replace(day=1) - dt.timedelta(days=1)))
    obra_aqui, _ = _pessoas_da_equipe(conn, ctx, comp, competencias.ultimo_dia(comp))
    pessoas = [p for p in (cadastros.colaborador_por_id(conn, i) for i in obra_aqui) if p]
    return pessoas, comp


def _da_equipe(conn, ctx: _Contexto, colaborador_id: int, competencia) -> tuple[dict, "dt.date"]:
    pessoas, comp = _equipe_do_mes(conn, ctx, competencia)
    alvo = next((p for p in pessoas if int(p["id"]) == int(colaborador_id)), None)
    if not alvo:
        raise NaoEncontrado("pessoa não encontrada")
    return alvo, comp


@bp.route("/app/api/assinaturas")
@auth.exige_colaborador_ou_aparelho
def app_api_assinaturas():
    """As folhas do mês da equipe de quem conduz: quem assinou, quem recusou, quem falta, e o prazo (dia 20)."""
    from .core import assinaturas
    with db.conexao() as conn:
        ctx = _contexto(conn)
        pessoas, comp = _equipe_do_mes(conn, ctx, request.args.get("competencia"))
        return _ok(**assinaturas.situacao(conn, pessoas, comp))


@bp.route("/app/api/assinaturas/<int:colaborador_id>/folha")
@auth.exige_colaborador_ou_aparelho
def app_api_assinatura_folha(colaborador_id: int):
    """A folha do mês para mostrar à pessoa antes de ela assinar (a mesma que vai no PDF)."""
    from .core import espelho
    with db.conexao() as conn:
        ctx = _contexto(conn)
        alvo, comp = _da_equipe(conn, ctx, colaborador_id, request.args.get("competencia"))
        esp = espelho.do_mes(conn, colaborador_id, comp)
    return _ok(pessoa=pessoa_do_espelho(alvo), competencia=esp["competencia"], resumo=esp["resumo"],
               dias=[{k: d.get(k) for k in ("data", "dia_semana", "situacao", "previsto", "trabalhado", "extra", "debito")}
                     | {"batidas": [b["hora"] + (" (aj)" if b["status"] == "AJUSTADA" else "") for b in d["batidas"]
                                    if b["status"] != "REJEITADA"],
                        "ocorrencia": (d.get("ocorrencia") or {}).get("rotulo")} for d in esp["dias"]])


@bp.route("/app/api/assinaturas/<int:colaborador_id>", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_assinar(colaborador_id: int):
    """A pessoa assina: o PIN DELA, o traço do dedo; a folha é lacrada e o PDF, arquivado."""
    from .core import assinaturas
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        _, comp = _da_equipe(conn, ctx, colaborador_id, d.get("competencia"))
        local = _local(d) or {}
        r = assinaturas.assinar(conn, colaborador_id=colaborador_id, competencia=comp, pin=d.get("pin"),
                                assinatura=d.get("assinatura"), conduzida_por={"id": ctx.pessoa["id"], "nome": ctx.pessoa["nome"]},
                                dispositivo_id=(ctx.aparelho or {}).get("id"), latitude=local.get("latitude"),
                                longitude=local.get("longitude"), ip=auth.ip_de_quem_chama())
    return _ok(**r), 201


@bp.route("/app/api/assinaturas/<int:colaborador_id>/recusa", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_assinatura_recusa(colaborador_id: int):
    """A pessoa não quis assinar: o motivo e quem conduziu ficam registrados."""
    from .core import assinaturas
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        _, comp = _da_equipe(conn, ctx, colaborador_id, d.get("competencia"))
        local = _local(d) or {}
        r = assinaturas.recusar(conn, colaborador_id=colaborador_id, competencia=comp, motivo=d.get("motivo"),
                                conduzida_por={"id": ctx.pessoa["id"], "nome": ctx.pessoa["nome"]},
                                dispositivo_id=(ctx.aparelho or {}).get("id"), latitude=local.get("latitude"),
                                longitude=local.get("longitude"), ip=auth.ip_de_quem_chama())
    return _ok(**r), 201


# ---------------------------------------------------------------------------
# O TERMO DO PONTO (07/11/2026, migração 028, core/termo.py): a pessoa assina no celular dela (entrou com CPF e PIN)
# ou o operador da obra / o administrativo mostra no aparelho dele — o PIN da pessoa ou, sem PIN, a foto dela na hora.
# ---------------------------------------------------------------------------
@bp.route("/app/api/termo")
@auth.exige_colaborador_ou_aparelho
def app_api_termo():
    """O MEU termo: o texto e se já assinei."""
    from .core import termo
    with db.conexao() as conn:
        p = _logado(conn)
        if not p:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        sit = termo.situacao_de(conn, [int(p["id"])])[int(p["id"])] if termo.disponivel(conn) else {"situacao": "FALTA"}
        return _ok(texto=termo.texto(), situacao=sit, disponivel=termo.disponivel(conn), nome=p["nome"])


@bp.route("/app/api/termo", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_termo_assinar():
    """Eu assino o meu termo: o meu PIN de novo e o traço do dedo."""
    from .core import termo
    d = _corpo()
    with db.conexao() as conn:
        p = _logado(conn)
        if not p:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        local = _local(d) or {}
        a = _aparelho(conn)
        r = termo.assinar(conn, colaborador_id=int(p["id"]), assinatura=d.get("assinatura"), forma="PELA_PESSOA",
                          pin=d.get("pin"), conduzida_por={"id": p["id"], "nome": p["nome"]},
                          dispositivo_id=(a or {}).get("id"), latitude=local.get("latitude"),
                          longitude=local.get("longitude"), ip=auth.ip_de_quem_chama())
    return _ok(**r), 201


def _equipe_do_termo(conn, ctx: _Contexto) -> list[dict]:
    """Quem quem conduz alcança e bate ponto (o mesmo conjunto do resumo da equipe, neste mês)."""
    hoje = horario.hoje()
    obra_aqui, _ = _pessoas_da_equipe(conn, ctx, hoje.replace(day=1), hoje)
    pessoas = [p for p in (cadastros.colaborador_por_id(conn, i) for i in obra_aqui) if p]
    return [p for p in pessoas if cadastros.categoria_no_ponto(p)[0] == "BATE"]


def _da_equipe_do_termo(conn, ctx: _Contexto, colaborador_id: int) -> dict:
    alvo = next((p for p in _equipe_do_termo(conn, ctx) if int(p["id"]) == int(colaborador_id)), None)
    if not alvo:
        raise NaoEncontrado("pessoa não encontrada")
    return alvo


@bp.route("/app/api/termos")
@auth.exige_colaborador_ou_aparelho
def app_api_termos():
    """A equipe de quem conduz: quem já assinou o termo, quem recusou e quem falta (os que faltam primeiro)."""
    from .core import termo
    with db.conexao() as conn:
        ctx = _contexto(conn)
        pessoas = _equipe_do_termo(conn, ctx)
        ids = [int(p["id"]) for p in pessoas]
        sit = termo.situacao_de(conn, ids) if termo.disponivel(conn) else {i: {"situacao": "FALTA"} for i in ids}
        pins = {int(l["colaborador_id"]) for l in db.todos(conn, """SELECT colaborador_id FROM ponto.colaborador_config
                 WHERE colaborador_id = ANY(:ids) AND pin_hash IS NOT NULL""", ids=ids)} if ids else set()
        disponivel = termo.disponivel(conn)
    linhas = sorted(({"id": int(p["id"]), "nome": p["nome"], "funcao": p.get("funcao") or "",
                      "tem_pin": int(p["id"]) in pins, **sit[int(p["id"])]} for p in pessoas),
                    key=lambda x: (x["situacao"] != "FALTA", (x["nome"] or "").upper()))
    return _ok(texto=termo.texto(), pessoas=linhas, disponivel=disponivel,
               faltam=sum(1 for x in linhas if x["situacao"] == "FALTA"))


@bp.route("/app/api/termos/<int:colaborador_id>", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_termo_conduzido(colaborador_id: int):
    """A pessoa assina no aparelho de quem conduz: o PIN dela (ou a foto, sem PIN) e o traço do dedo."""
    from .core import termo
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        _da_equipe_do_termo(conn, ctx, colaborador_id)
        local = _local(d) or {}
        r = termo.assinar(conn, colaborador_id=colaborador_id, assinatura=d.get("assinatura"), forma="CONDUZIDA",
                          pin=d.get("pin"), foto=d.get("foto"),
                          conduzida_por={"id": ctx.pessoa["id"], "nome": ctx.pessoa["nome"]},
                          dispositivo_id=(ctx.aparelho or {}).get("id"), latitude=local.get("latitude"),
                          longitude=local.get("longitude"), ip=auth.ip_de_quem_chama())
    return _ok(**r), 201


@bp.route("/app/api/termos/<int:colaborador_id>/recusa", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_termo_recusa(colaborador_id: int):
    from .core import termo
    d = _corpo()
    with db.conexao() as conn:
        ctx = _contexto(conn)
        _da_equipe_do_termo(conn, ctx, colaborador_id)
        local = _local(d) or {}
        r = termo.recusar(conn, colaborador_id=colaborador_id, motivo=d.get("motivo"),
                          conduzida_por={"id": ctx.pessoa["id"], "nome": ctx.pessoa["nome"]},
                          dispositivo_id=(ctx.aparelho or {}).get("id"), latitude=local.get("latitude"),
                          longitude=local.get("longitude"), ip=auth.ip_de_quem_chama())
    return _ok(**r), 201


# Até quantas folhas num arquivo só: acima disso, escolhe-se a obra (o celular sofre para montar um PDF enorme).
MAX_FOLHAS_DA_EQUIPE = 150


@bp.route("/app/api/equipe/espelhos/bilhete", methods=["POST"])
@auth.exige_colaborador_ou_aparelho
def app_api_equipe_espelhos_bilhete():
    """O bilhete das folhas da EQUIPE (05/11/2026, o dono: "a pessoa tem 40 pessoas que emitem a folha e fazer isso
    um por um"): as pessoas do resumo da equipe no período pedido — `obra` para só uma —, decididas aqui, pela
    chamada do aplicativo, que leva a identificação do aparelho. A página das folhas só confere a assinatura."""
    from .core import cadastros as cad, competencias, qr
    from .app_colaborador import periodo_pedido
    with db.conexao() as conn:
        ctx = _contexto(conn)
        if not ctx.pessoa:
            raise NaoAutenticado("entre com o seu CPF e PIN")
        periodo = periodo_pedido()
        if periodo:
            inicio, fim = periodo
        else:
            inicio = competencias.primeiro_dia(request.args.get("competencia") or horario.hoje())
            fim = competencias.ultimo_dia(inicio)
        obra_aqui, _ = _pessoas_da_equipe(conn, ctx, inicio, fim)
        obra = (request.args.get("obra") or "").strip().upper()
        if obra:
            # quem veio pelo aparelho de que a pessoa é responsável não traz a obra: vale a em que mais bateu no
            # período, ou a do cadastro — a MESMA regra que agrupa o resumo da equipe (resumo_equipe.montar)
            sem_obra = [p for p in (cad.colaborador_por_id(conn, i) for i, o in obra_aqui.items() if o is None) if p]
            if sem_obra:
                lote = espelho.Lote(conn, sem_obra, inicio, fim)
                for p in sem_obra:
                    dias = espelho.montar(conn, int(p["id"]), inicio, fim, lote=lote)["dias"]
                    obra_aqui[int(p["id"])] = cad.obra_mais_batida(dias) or p.get("obra_codigo") or ""
            ids = [i for i, o in obra_aqui.items() if (o or "").upper() == obra]
        else:
            ids = list(obra_aqui)
        if not ids:
            raise NaoEncontrado("ninguém da equipe nessa obra no período")
        if len(ids) > MAX_FOLHAS_DA_EQUIPE:
            raise Recusada(f"são {len(ids)} pessoas — escolha uma obra (até {MAX_FOLHAS_DA_EQUIPE} folhas por vez)")
        logger.info("Ponto: folhas da equipe (%d pessoas, obra %s) pedidas por %s", len(ids), obra or "todas",
                    ctx.pessoa["nome"])
    return _ok(pessoas=len(ids), bilhete=qr.emitir_impressao_equipe(sorted(ids), int(ctx.pessoa["id"]),
                                                                    _periodo_da_folha()))


@bp.route("/app/equipe/espelhos/imprimir")
@auth.exige_colaborador_ou_aparelho
def app_equipe_espelhos_imprimir():
    """As folhas da equipe num arquivo só, com o bilhete pedido pelo aplicativo (acima)."""
    from .core import qr
    from .erros import ErroDoPonto
    try:
        with db.conexao() as conn:
            quem = _logado(conn)
            if not quem:
                raise NaoAutenticado("entre com o seu CPF e PIN")
            ids = qr.conferir_impressao_equipe(request.args.get("bilhete"), int(quem["id"]), _periodo_da_folha())
            if ids is None:
                raise NaoEncontrado("o pedido de impressão venceu — toque de novo em Espelhos da equipe")
            logger.info("Ponto: folhas da equipe (%d) abertas por %s", len(ids), quem["nome"])
            return folhas_da_equipe(conn, ids, quem["nome"])
    except ErroDoPonto as e:
        return folha_que_nao_abriu(e)


@bp.route("/app/equipe/<int:colaborador_id>/espelho/imprimir")
@auth.exige_colaborador_ou_aparelho
def app_equipe_espelho_imprimir(colaborador_id: int):
    """O espelho de uma pessoa da obra para imprimir (28/10/2026): o mesmo alcance da
    consulta — o responsável e o administrativo, só de quem é da obra ou bateu nela;
    no ponto da obra, só da pessoa consultada pelo CPF agora há pouco. Desde 05/11/2026, com o `bilhete` pedido pelo
    aplicativo: a página não leva a identificação do aparelho, e sem ela o ponto da obra não achava a pessoa."""
    from .core import competencias, qr
    from .erros import ErroDoPonto
    try:
        with db.conexao() as conn:
            from .app_colaborador import periodo_pedido
            periodo = periodo_pedido()
            bilhete = request.args.get("bilhete")
            if bilhete:
                quem = _logado(conn)
                if not quem:
                    raise NaoAutenticado("entre com o seu CPF e PIN")
                if not qr.conferir_impressao(bilhete, colaborador_id, int(quem["id"]), _periodo_da_folha()):
                    raise NaoEncontrado("o pedido de impressão venceu — toque de novo em Baixar o espelho")
                logger.info("Ponto: espelho de %s aberto para imprimir por %s (bilhete)", colaborador_id, quem["nome"])
                return folha_do_espelho(conn, colaborador_id, quem["nome"])
            ctx = _contexto(conn)
            competencia = request.args.get("competencia") or horario.hoje().strftime("%Y-%m")
            _alvo(conn, ctx, colaborador_id, *(periodo or (competencias.primeiro_dia(competencia),)))
            logger.info("Ponto: espelho de %s aberto para imprimir por %s", colaborador_id, ctx.pessoa["nome"])
            return folha_do_espelho(conn, colaborador_id, ctx.pessoa["nome"])
    except ErroDoPonto as e:
        return folha_que_nao_abriu(e)


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
                     obras=list(a.obras),
                     extras={"solicitante_id": int(ctx.pessoa["id"])} if ctx.pessoa else {}), "APARELHO")
    rotulo = ROTULO_PAPEL.get(a.papel, "responsável")
    return (Quem(nome=f"{ctx.pessoa['nome']} ({rotulo}, pelo aplicativo)"[:120], pessoas={int(alvo_id)},
                 obras=list(a.obras), extras={"solicitante_id": int(ctx.pessoa["id"])}), "RESPONSAVEL")


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
