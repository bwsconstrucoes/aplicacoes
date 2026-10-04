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
  · DESLIGADO — data de saída já chegou, ou a "Fase Atual" do Pipefy diz
    desligado (por pedaço: "desligad", cobre plural e singular);
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


def trechos_sql(conn: Connection) -> Optional[dict]:
    """Os pedaços de SQL que trocam o cadastro do ERP pelo Registro, ou None
    quando a base é o ERP (ou o Registro não está disponível)."""
    e = estado(conn)
    if not e["usar"]:
        return None
    col = e["colunas"]
    hoje = "(now() AT TIME ZONE 'America/Fortaleza')::date"
    admissao = "COALESCE(" + ", ".join([f"r.{c}" for c in ("data_admissao", "data_inicio") if c in col]
                                       + ["c.admissao"]) + ")"
    obra_join = ("LEFT JOIN public.obras ro ON r.obra_codigo <> '' "
                 "AND upper(btrim(ro.codigo)) = upper(btrim(r.obra_codigo))") if "obra_codigo" in col else ""
    obra_id = "COALESCE(ro.id, c.obra_id)" if obra_join else "c.obra_id"
    situacao = f"""CASE
        WHEN r.cpf IS NULL THEN CASE WHEN c.situacao = 'ATIVO' THEN 'FORA_DO_REGISTRO' ELSE c.situacao END
        WHEN r.data_saida IS NOT NULL AND r.data_saida <= {hoje} THEN 'DESLIGADO'
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
    }


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
        sem_obra = db.um(conn, f"""
            SELECT count(*) AS n FROM analisesps.colaborador r
             WHERE {ativos} AND NOT EXISTS (SELECT 1 FROM public.obras o
                    WHERE r.obra_codigo <> '' AND upper(btrim(o.codigo)) = upper(btrim(r.obra_codigo)))""")["n"]
    from .. import horario
    return {"disponivel": True, "fonte": e["fonte"], "usando": e["usar"],
            "total": int(linha["total"]), "ativos": int(linha["ativos"]),
            "afastados": int(linha["afastados"]), "faltam_no_erp": int(linha["faltam_no_erp"]),
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


def gravar_fonte(conn: Connection, fonte: str, por: str) -> None:
    from ..erros import ErroDeValidacao
    fonte = str(fonte or "").upper()
    if fonte not in (FONTE_REGISTRO, FONTE_ERP):
        raise ErroDeValidacao("use REGISTRO ou ERP", campo="fonte")
    parametros.gravar(conn, PARAMETRO_FONTE, fonte, por)
    esquecer()
