# -*- coding: utf-8 -*-
"""
A BASE DE PESSOAS DO PONTO: a planilha "Registro de Colaboradores".

Pedido do dono, 04/10/2026: *"gostaria por enquanto de utilizar como base de
colaboradores a planilha de Registro de Colaboradores. Tem critério de uso dela
de exibição no processo de Análise de SPs."*

DE ONDE VEM, sem ler a planilha de novo: a Análise de SPs já copia a aba "Dados
Documentos" (≈3.500 linhas, que vêm do Pipefy) para o banco, na tabela
`analisesps.colaborador`, pelo botão "Atualizar cadastro" de lá. O ponto LÊ essa
cópia — não escreve nela, não dispara a carga, e não lê a planilha (ler 3.500 ×
78 colunas a cada pergunta não cabe na instância de 2 GB). A cópia é tão nova
quanto a última vez que alguém apertou o botão; a Configuração do ponto mostra
quando foi.

O CRITÉRIO É O MESMO DA ANÁLISE DE SPs (`analisesps/colaboradores.py`), para
não haver duas respostas à pergunta "essa pessoa está trabalhando?":
  · DESLIGADO — data de saída já PASSOU (o próprio dia da saída ainda se
    bate), ou a "Fase Atual" do Pipefy diz desligado (por pedaço: "desligad",
    cobre plural e singular);
  · AFASTADO  — a fase diz afastado ("afastad");
  · ATIVO     — o resto (inclusive quem está em aviso prévio: trabalha até a
    data de saída).

O QUE O REGISTRO MANDA, quando a pessoa está nele: nome, celular (o WhatsApp do
PIN e do QR), cargo (a "função" que o tablet mostra), obra (pelo código), data de
admissão e de saída, e a situação acima. Quem existe no cadastro do ERP e NÃO
está no Registro continua podendo bater — mas a batida vai para conferência
("fora do Registro de Colaboradores"), porque a base agora é a planilha.

A IDENTIDADE CONTINUA SENDO A LINHA DO ERP (`public.colaboradores`): é nela que
batidas, escalas e pedidos se penduram. Quem está no Registro e falta no ERP
entra por um botão na Configuração do ponto, que mostra antes quantos são e
cadastra só o mínimo (nome, CPF, obra) — a mesma escrita que o importador do
Mobponto já fazia, autorizada pelo dono.

⚠️ AMARRA COM A ANÁLISE DE SPs, dita com todas as letras: se aquela área mudar o
nome de `analisesps.colaborador` ou das colunas `cpf`, `nome`, `celular`,
`cargo`, `fase`, `data_saida`, `obra_codigo`, `data_admissao`, `data_inicio`, o
ponto volta sozinho a usar o cadastro do ERP (as colunas são conferidas antes de
usar) — e o teste `test_ponto_registro_banco.py` acusa a mudança.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from . import parametros

logger = logging.getLogger("ponto.registro")

PARAMETRO_FONTE = "pessoas.fonte"
FONTE_REGISTRO, FONTE_ERP = "REGISTRO", "ERP"
FONTE_PADRAO = FONTE_REGISTRO          # o pedido dele
PEDACO_DESLIGADO, PEDACO_AFASTADO = "desligad", "afastad"
OBRIGATORIAS = ("cpf", "nome", "celular", "cargo", "fase", "data_saida")
OPCIONAIS = ("obra_codigo", "data_admissao", "data_inicio")

_cache: dict = {"em": 0.0, "colunas": None, "fonte": None}
CACHE_S = 30


def _colunas(conn: Connection) -> set[str]:
    linhas = db.todos(conn, """SELECT column_name FROM information_schema.columns
                                WHERE table_schema = 'analisesps' AND table_name = 'colaborador'""")
    return {l["column_name"] for l in linhas}


def estado(conn: Connection, *, fresco: bool = False) -> dict:
    """{'usar': bool, 'colunas': set, 'fonte': str, 'disponivel': bool}.
    Guardado por 30 s: a pergunta é feita a cada pessoa lida."""
    agora = time.time()
    if fresco or _cache["colunas"] is None or agora - _cache["em"] > CACHE_S:
        _cache["colunas"] = _colunas(conn)
        # Sem a tabela de parâmetros (banco do ponto antes da 002), vale o
        # padrão — e sem consulta que falhe: erro de SQL estraga a transação.
        _cache["fonte"] = ((parametros.ler(conn, PARAMETRO_FONTE, FONTE_PADRAO) or FONTE_PADRAO)
                           if db.tem_coluna(conn, "parametros", "valor") else FONTE_PADRAO)
        _cache["em"] = agora
    colunas = _cache["colunas"]
    disponivel = all(c in colunas for c in OBRIGATORIAS)
    return {"disponivel": disponivel, "colunas": colunas, "fonte": _cache["fonte"],
            "usar": disponivel and _cache["fonte"] == FONTE_REGISTRO}


def esquecer() -> None:
    _cache["colunas"] = None


def _obra_pela_planilha(conn: Connection) -> Optional[str]:
    from . import base_obras
    return base_obras.obra_pelo_codigo_sql(conn, "r.obra_codigo")


def trechos_sql(conn: Connection) -> Optional[dict]:
    """Os pedaços de SQL que trocam o cadastro do ERP pelo Registro, ou None
    quando a base é o ERP (ou o Registro não está disponível)."""
    e = estado(conn)
    if not e["usar"]:
        return None
    col = e["colunas"]
    hoje = "(now() AT TIME ZONE 'America/Fortaleza')::date"
    # O INÍCIO do contrato é a MENOR entre "Data de Início" e "Data de Admissão"
    # (o diarista começa antes da carteira). LEAST ignora vazio no Postgres.
    datas = [f"r.{c}" for c in ("data_inicio", "data_admissao") if c in col]
    admissao = (f"COALESCE(LEAST({', '.join(datas)}), c.admissao)" if datas else "c.admissao")
    obra_join = ("LEFT JOIN public.obras ro ON r.obra_codigo <> '' "
                 "AND upper(btrim(ro.codigo)) = upper(btrim(r.obra_codigo))") if "obra_codigo" in col else ""
    # O código da obra do Registro casa com o código do ERP ou, com a planilha
    # C. Diários como base de obras, com qualquer dos dois códigos dela.
    pela_planilha = _obra_pela_planilha(conn) if obra_join else None
    obra_id = (f"COALESCE(ro.id, {pela_planilha}, c.obra_id)" if pela_planilha
               else "COALESCE(ro.id, c.obra_id)" if obra_join else "c.obra_id")
    situacao = f"""CASE
        WHEN r.cpf IS NULL THEN CASE WHEN c.situacao = 'ATIVO' THEN 'FORA_DO_REGISTRO' ELSE c.situacao END
        WHEN r.data_saida IS NOT NULL AND r.data_saida < {hoje} THEN 'DESLIGADO'
        WHEN lower(coalesce(r.fase, '')) LIKE '%{PEDACO_DESLIGADO}%' THEN 'DESLIGADO'
        WHEN lower(coalesce(r.fase, '')) LIKE '%{PEDACO_AFASTADO}%' THEN 'AFASTADO'
        ELSE 'ATIVO' END"""
    return {
        "join": ("LEFT JOIN analisesps.colaborador r ON r.cpf = regexp_replace(c.cpf, '\\D', '', 'g') "
                 + obra_join),
        "nome": "COALESCE(NULLIF(btrim(r.nome), ''), c.nome)",
        "telefone": "COALESCE(NULLIF(btrim(r.celular), ''), c.telefone)",
        "funcao": "COALESCE(NULLIF(btrim(r.cargo), ''), f.nome)",
        "obra_id": obra_id,
        "situacao": situacao,
        "admissao": admissao,
        "demissao": "COALESCE(r.data_saida, c.demissao)",
        "no_registro": "(r.cpf IS NOT NULL)",
        "fase": "r.fase",
        "nao_bate": nao_bate_sql(col),
    }


# ---------------------------------------------------------------------------
# QUEM NÃO BATE PONTO PELA PLANILHA (29/10/2026). O dono: *"como ainda estamos usando
# a planilha de colaboradores como base, tem algumas pessoas que têm categorização de
# não bater ponto: na coluna AY, quem é PJ e Pró-labore; na coluna AA, quem está na
# obra ESCRUSSAS; e na coluna AX, quem está como Colaboradores Afastados. Esse
# pessoal é para sair, eles não batem ponto."*
#   · AY é o "Tipo de Contrato" (`tipo_contrato`) e AA a obra (`obra_codigo`; o
#     extenso, `obra_cadastro`, também é olhado). Quem cai aqui fica como "não bate
#     ponto" — o mesmo efeito da marcação feita à mão: sai das listas, não entra no
#     aplicativo, a batida é recusada. A planilha manda: desmarcar à mão não vence.
#   · AX é a "Fase Atual": o AFASTADO já não batia (14/10/2026); agora também sai
#     das listas (`listar_colaboradores`), mas continua entrando no aplicativo para
#     mandar o atestado — a decisão de 14/10/2026 ("pedido ele continua fazendo").
# Comparados sem acento, sem espaço e sem pontuação: "Pró-labore", "PRO LABORE" e
# "Prolabore" são o mesmo; "ESC RUSSAS" e "ESCRUSSAS" também.
# ---------------------------------------------------------------------------
TIPOS_SEM_PONTO = ("pj", "prolabore")
OBRAS_SEM_PONTO = ("ESCRUSSAS",)


def _sem_acento_sql(expr: str) -> str:
    return (f"regexp_replace(lower(translate(coalesce({expr}, ''), 'ÁÀÂÃÉÊÍÓÔÕÚÇáàâãéêíóôõúç', "
            f"'aaaaeeiooouCaaaaeeiooouc')), '[^a-z0-9]', '', 'g')")


def nao_bate_sql(col: set) -> str:
    """O MOTIVO (texto) de a planilha dizer que a pessoa não bate ponto, ou NULL."""
    casos = []
    if "tipo_contrato" in col:
        casos.append(f"WHEN {_sem_acento_sql('r.tipo_contrato')} IN ({', '.join(repr(t) for t in TIPOS_SEM_PONTO)}) "
                     "THEN 'o tipo de contrato na planilha é “' || btrim(r.tipo_contrato) || '” — PJ e pró-labore "
                     "não batem ponto'")
    obras = ", ".join(repr(o.lower()) for o in OBRAS_SEM_PONTO)
    for c in ("obra_codigo", "obra_cadastro"):
        if c in col:
            casos.append(f"WHEN {_sem_acento_sql('r.' + c)} IN ({obras}) "
                         f"THEN 'a obra na planilha é “' || btrim(r.{c}) || '” — quem está nela não bate ponto'")
    if not casos:
        return "NULL::text"
    return "CASE WHEN r.cpf IS NULL THEN NULL " + " ".join(casos) + " ELSE NULL END"


# ---------------------------------------------------------------------------
# Para a Configuração: o retrato, e quem falta no ERP
# ---------------------------------------------------------------------------
def _ativos_sql(col: set) -> str:
    hoje = "(now() AT TIME ZONE 'America/Fortaleza')::date"
    return (f"(r.data_saida IS NULL OR r.data_saida > {hoje}) "
            f"AND lower(coalesce(r.fase, '')) NOT LIKE '%{PEDACO_DESLIGADO}%'")


def retrato(conn: Connection) -> dict:
    e = estado(conn, fresco=True)
    if not e["disponivel"]:
        return {"disponivel": False, "fonte": e["fonte"], "usando": False}
    col = e["colunas"]
    ativos = _ativos_sql(col)
    linha = db.um(conn, f"""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE {ativos}) AS ativos,
               count(*) FILTER (WHERE {ativos} AND lower(coalesce(r.fase, '')) LIKE '%{PEDACO_AFASTADO}%') AS afastados,
               count(*) FILTER (WHERE {ativos} AND c.id IS NULL) AS faltam_no_erp,
               count(*) FILTER (WHERE {ativos} AND ({nao_bate_sql(col)}) IS NOT NULL) AS nao_batem,
               max(r.atualizado_em) AS atualizado_em
          FROM analisesps.colaborador r
          LEFT JOIN public.colaboradores c ON regexp_replace(c.cpf, '\\D', '', 'g') = r.cpf
    """)
    so_erp = db.um(conn, """
        SELECT count(*) AS n FROM public.colaboradores c
         WHERE c.situacao <> 'DESLIGADO'
           AND NOT EXISTS (SELECT 1 FROM analisesps.colaborador r
                            WHERE r.cpf = regexp_replace(c.cpf, '\\D', '', 'g'))""")["n"]
    sem_obra = 0
    if "obra_codigo" in col:
        pela_planilha = _obra_pela_planilha(conn)
        sem_obra = db.um(conn, f"""
            SELECT count(*) AS n FROM analisesps.colaborador r
             WHERE {ativos} AND NOT EXISTS (SELECT 1 FROM public.obras o
                    WHERE r.obra_codigo <> '' AND upper(btrim(o.codigo)) = upper(btrim(r.obra_codigo)))
               {f"AND {pela_planilha} IS NULL" if pela_planilha else ""}""")["n"]
    from .. import horario
    return {"disponivel": True, "fonte": e["fonte"], "usando": e["usar"],
            "total": int(linha["total"]), "ativos": int(linha["ativos"]),
            "afastados": int(linha["afastados"]), "faltam_no_erp": int(linha["faltam_no_erp"]),
            "nao_batem_pela_planilha": int(linha["nao_batem"]),
            "so_no_erp": int(so_erp), "ativos_sem_obra_reconhecida": int(sem_obra),
            "atualizado_em": horario.texto(linha["atualizado_em"])}


def faltam_no_erp(conn: Connection, limite: int = 5000) -> list[dict]:
    """Quem está ATIVO no Registro e não existe no cadastro do ERP."""
    e = estado(conn, fresco=True)
    if not e["disponivel"]:
        return []
    col = e["colunas"]
    obra = ("(SELECT o.id FROM public.obras o WHERE r.obra_codigo <> '' "
            "AND upper(btrim(o.codigo)) = upper(btrim(r.obra_codigo)) LIMIT 1)") if "obra_codigo" in col else "NULL"
    pela_planilha = _obra_pela_planilha(conn) if "obra_codigo" in col else None
    if pela_planilha:
        obra = f"COALESCE({obra}, {pela_planilha})"
    return db.todos(conn, f"""
        SELECT r.cpf, btrim(r.nome) AS nome, {obra} AS obra_id
          FROM analisesps.colaborador r
         WHERE {_ativos_sql(col)} AND btrim(r.nome) <> ''
           AND NOT EXISTS (SELECT 1 FROM public.colaboradores c
                            WHERE regexp_replace(c.cpf, '\\D', '', 'g') = r.cpf)
         ORDER BY r.nome LIMIT :lim""", lim=limite)


def cadastrar_faltantes(conn: Connection, por: str) -> dict:
    """Cria no ERP, com o mínimo, quem está ativo no Registro e falta lá. CPF
    que não passa no dígito verificador NÃO entra (fica contado, para alguém
    corrigir no Pipefy)."""
    from app.apps.erp.core.cadastros.validadores import cpf_valido
    from . import cadastros
    criados, cpf_ruim = 0, []
    for p in faltam_no_erp(conn):
        if not cpf_valido(p["cpf"]):
            cpf_ruim.append(p["nome"])
            continue
        cadastros.criar_colaborador_no_erp(conn, nome=p["nome"], cpf=p["cpf"], obra_id=p["obra_id"])
        criados += 1
    logger.info("Ponto: %d pessoa(s) do Registro cadastradas no ERP por %s (%d com CPF inválido)",
                criados, por, len(cpf_ruim))
    return {"criados": criados, "cpf_invalido": len(cpf_ruim), "exemplos_cpf_invalido": cpf_ruim[:10]}


# ---------------------------------------------------------------------------
# POR QUE NÃO APARECE (pedido do dono, 14/10/2026: "tem uma pessoa em específico
# que não tá aparecendo em Ponto › Pessoas"). A lista mostra só quem bate; quem
# procura alguém que não está nela precisa saber o motivo — e o motivo mora em
# três lugares (a cópia do Registro, o ERP e a marcação do ponto).
# ---------------------------------------------------------------------------
def por_que_nao_aparece(conn: Connection, busca: str, ja_listados: set, limite: int = 10) -> list[dict]:
    """[{nome, cpf_final, motivo}] de quem casa com a busca e NÃO está em
    `ja_listados` (os CPFs que a lista já mostrou). A busca é a da tela: CPF
    (só os dígitos, 11) ou pedaço do nome (3 letras ou mais)."""
    from app.apps.erp.core.cadastros.validadores import cpf_valido
    from . import cadastros
    busca = " ".join(str(busca or "").split()).lower()
    digitos = "".join(ch for ch in busca if ch.isdigit())
    if len(digitos) == 11:
        filtro_r, filtro_c, params = "r.cpf = :cpf", "regexp_replace(c.cpf, '\\D', '', 'g') = :cpf", {"cpf": digitos}
    elif len(busca) >= 3 and not digitos:
        filtro_r, filtro_c, params = "lower(r.nome) LIKE :nome", "lower(c.nome) LIKE :nome", {"nome": f"%{busca}%"}
    else:
        return []
    e = estado(conn, fresco=True)
    do_registro: dict = {}
    if e["usar"]:
        for r in db.todos(conn, f"""SELECT r.cpf, btrim(r.nome) AS nome, r.fase, r.data_saida
                                      FROM analisesps.colaborador r WHERE {filtro_r}
                                     ORDER BY r.nome LIMIT :lim""", lim=limite * 3, **params):
            do_registro[r["cpf"]] = r
    do_erp = {"".join(ch for ch in (c["cpf"] or "") if ch.isdigit()): c for c in db.todos(
        conn, f"SELECT c.cpf FROM public.colaboradores c WHERE {filtro_c} LIMIT :lim", lim=limite * 3, **params)}
    if do_registro:             # o nome no ERP pode ser outro: confere pelo CPF de quem o Registro achou
        for c in db.todos(conn, "SELECT c.cpf FROM public.colaboradores c "
                                "WHERE regexp_replace(c.cpf, '\\D', '', 'g') = ANY(:cpfs)", cpfs=list(do_registro)):
            do_erp.setdefault("".join(ch for ch in (c["cpf"] or "") if ch.isdigit()), c)
    from .. import horario
    hoje = horario.hoje()
    saida = []
    for cpf in sorted(set(do_registro) | set(do_erp), key=lambda x: (do_registro.get(x) or {}).get("nome") or ""):
        if cpf in ja_listados:
            continue
        r = do_registro.get(cpf)
        if cpf not in do_erp:
            fase = (r["fase"] or "").lower()
            if not cpf_valido(cpf):
                motivo = ("está no Registro de Colaboradores, mas o CPF não passa na conferência dos dígitos — "
                          "corrija no Pipefy e atualize o cadastro na Análise de SPs")
            elif r["data_saida"] and r["data_saida"] < hoje:
                motivo = f"desligada: a data de saída no Registro de Colaboradores é {r['data_saida']:%d/%m/%Y}"
            elif PEDACO_DESLIGADO in fase:
                motivo = f"desligada: a Fase Atual no Pipefy diz “{r['fase']}”"
            else:
                motivo = ("está no Registro de Colaboradores, mas ainda não tem cadastro no ERP. Em Ponto › "
                          "Configuração, aperte “Cadastrar no ERP os que faltam” — ou espere até 15 minutos, "
                          "se a base automática estiver ligada")
        else:
            p = cadastros.colaborador_por_cpf(conn, cpf) or {}
            nome = p.get("nome") or (r or {}).get("nome")
            if p.get("situacao") == "DESLIGADO":
                if p.get("demissao") and p["demissao"] < hoje:
                    motivo = (f"desligada: a data de saída {'no Registro de Colaboradores' if p.get('no_registro') else 'no ERP'}"
                              f" é {p['demissao']:%d/%m/%Y}")
                elif p.get("fase_registro") and PEDACO_DESLIGADO in p["fase_registro"].lower():
                    motivo = f"desligada: a Fase Atual no Pipefy diz “{p['fase_registro']}”"
                else:
                    motivo = "desligada no cadastro do ERP"
            elif p.get("nao_bate_motivo"):
                motivo = f"não bate ponto: {p['nao_bate_motivo']}"
            elif not p.get("ativo_no_ponto", True):
                motivo = "marcada no próprio ponto como “não bate ponto”" + (
                    f" — {p['inativo_motivo']}" + (f" (por {p['inativo_por']})" if p.get("inativo_por") else "")
                    if p.get("inativo_motivo") else "")
            elif p.get("situacao") == "AFASTADO":
                motivo = (f"afastada: a Fase Atual no Pipefy diz “{p['fase_registro']}”" if p.get("fase_registro")
                          else "afastada no cadastro do ERP") + " — afastado não bate ponto e sai das listas"
            else:
                continue            # ela está na lista; quem procura só não a alcança
            r = {"nome": nome}
        saida.append({"nome": r["nome"], "cpf_final": cpf[-4:], "motivo": motivo})
        if len(saida) >= limite:
            break
    if not saida and not do_registro and not do_erp:
        quando = horario.texto(db.um(conn, "SELECT max(atualizado_em) AS u FROM analisesps.colaborador")["u"]) \
            if e["usar"] else None
        saida.append({"nome": None, "cpf_final": None, "motivo": (
            "não está no cadastro do ERP"
            + (f" nem na cópia do Registro de Colaboradores (atualizada em {quando}). Se a pessoa entrou na "
               "planilha depois disso, atualize em Análise de SPs › Colaboradores › “Atualizar cadastro”"
               if e["usar"] else ""))})
    return saida


def gravar_fonte(conn: Connection, fonte: str, por: str) -> None:
    from ..erros import ErroDeValidacao
    fonte = str(fonte or "").upper()
    if fonte not in (FONTE_REGISTRO, FONTE_ERP):
        raise ErroDeValidacao("use REGISTRO ou ERP", campo="fonte")
    parametros.gravar(conn, PARAMETRO_FONTE, fonte, por)
    esquecer()


# ---------------------------------------------------------------------------
# A BASE SE MANTÉM SOZINHA (pedido do dono, 05/10/2026: "como atualizo a base
# das pessoas? É algo que precisa ser contínuo").
#
#   1. A CÓPIA DA PLANILHA: de 2 em 2 horas, das 6h às 20h, o ponto pede à
#      Análise de SPs a mesma tarefa do botão "Atualizar cadastro" de lá
#      (`tarefas.disparar("colaboradores")`). Ela roda num processo separado,
#      com a trava dela no banco: se já houver outra tarefa da Análise de SPs
#      rodando, o pedido é recusado e o ponto tenta na próxima rodada.
#   2. QUEM FALTA NO ERP: a cada 15 minutos, quem está ativo no Registro e não
#      existe no ERP é cadastrado com o mínimo (nome, CPF, obra).
#
# Acordado pelas próprias requisições do ponto (o serviço não tem relógio),
# numa linha separada. Desliga na Configuração ("Manter a base em dia sozinho").
# ---------------------------------------------------------------------------
PARAMETRO_AUTOMATICO = "pessoas.automatico"
HORAS_ENTRE_COPIAS = 2
JANELA_DA_COPIA = (6, 20)
MINUTOS_ENTRE_CHECAGENS = 15
_trabalhando = __import__("threading").Lock()
_ultima_checagem = 0.0


def automatico(conn: Connection) -> bool:
    if not db.tem_coluna(conn, "parametros", "valor"):
        return False
    return parametros.ler(conn, PARAMETRO_AUTOMATICO, "1") == "1"


def precisa_copiar(ultima: Optional[dict], agora) -> bool:
    """PURA. A cópia da planilha está velha o bastante para pedir outra?"""
    from .. import horario
    local = horario.para_local(agora)
    if not (JANELA_DA_COPIA[0] <= local.hour < JANELA_DA_COPIA[1]):
        return False
    if not ultima:
        return True
    if ultima.get("fim") is None:            # em andamento
        return False
    referencia = ultima.get("inicio") or ultima.get("fim")
    if referencia.tzinfo is None:
        import datetime as _dt
        referencia = referencia.replace(tzinfo=_dt.timezone.utc)
    return (agora - referencia).total_seconds() >= HORAS_ENTRE_COPIAS * 3600


def _pedir_copia(agora) -> dict:
    import os
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return {"pedida": False, "motivo": "teste"}
    from app.apps.analisesps import tarefas
    if not precisa_copiar(tarefas.ultima_do_tipo("colaboradores"), agora):
        return {"pedida": False, "motivo": "recente ou fora da janela"}
    r = tarefas.disparar("colaboradores", disparo="automático (ponto)")
    if r.get("ok"):
        logger.info("Ponto: pedi à Análise de SPs a atualização do Registro de Colaboradores")
    return {"pedida": bool(r.get("ok")), "motivo": r.get("erro") or "pedida"}


def _trabalhar() -> None:
    from .. import horario
    try:
        # Quem foi desligado perde o acesso (celular, QR, grupo) — valha a base
        # que valer, e com a base automática ligada ou não (06/10/2026).
        try:
            from . import desligamentos
            with db.conexao() as conn:
                desligamentos.aplicar(conn)
        except Exception:  # noqa: BLE001
            logger.warning("Ponto: não consegui fechar o acesso de quem saiu", exc_info=True)
        with db.conexao() as conn:
            if not automatico(conn) or not estado(conn, fresco=True)["usar"]:
                return
        try:
            _pedir_copia(horario.agora())
        except Exception:  # noqa: BLE001 — a Análise de SPs fora não derruba nada
            logger.warning("Ponto: não consegui pedir a cópia do Registro", exc_info=True)
        with db.conexao() as conn:
            if faltam_no_erp(conn, limite=1):
                cadastrar_faltantes(conn, "o próprio ponto (base em dia)")
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: manter a base de pessoas em dia falhou; tenta de novo depois")
    finally:
        _trabalhando.release()


def manter_em_dia() -> bool:
    """Chamada barata em toda requisição do ponto: no máximo uma olhada a cada
    15 minutos, numa linha separada."""
    global _ultima_checagem
    agora = time.time()
    if agora - _ultima_checagem < MINUTOS_ENTRE_CHECAGENS * 60:
        return False
    _ultima_checagem = agora
    if not _trabalhando.acquire(blocking=False):
        return False
    __import__("threading").Thread(target=_trabalhar, name="ponto-base-pessoas", daemon=True).start()
    return True


def ultima_copia() -> Optional[dict]:
    """A última atualização do cadastro na Análise de SPs, para a tela."""
    try:
        from app.apps.analisesps import tarefas
        u = tarefas.ultima_do_tipo("colaboradores")
    except Exception:  # noqa: BLE001
        return None
    if not u:
        return None
    from .. import horario
    return {"inicio": horario.texto(u.get("inicio")), "fim": horario.texto(u.get("fim")),
            "ok": u.get("ok"), "disparo": u.get("disparo"), "mensagem": u.get("mensagem")}
