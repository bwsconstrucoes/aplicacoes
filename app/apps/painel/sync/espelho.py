# -*- coding: utf-8 -*-
"""
Espelho do OMIE no Postgres (schema `painel`).

Tabelas:
  - titulos          (1 linha por codigo_lancamento_omie; chave estavel)
  - rateio           (N linhas por titulo; distribuicao por departamento/obra)
  - movimentos       (data de pagamento real e valores realizados)
  - cat              (categoria -> descricao/grupo)
  - clientes         (cliente/fornecedor -> razao social/CNPJ)
  - contas_correntes (codigo -> nome da conta)
  - depto_projeto    (obra -> projeto; planilha "C. Diarios", via `projetos`)
  - sync_state       (por entidade: ultima alteracao vista, ultima sync, total)

Este e o mesmo codigo que rodava no PC contra um arquivo SQLite. O que mudou foi
so o destino: agora grava no Postgres, porque o disco do Render e apagado a cada
reinicio e o espelho (120 mil titulos) precisa sobreviver entre um dia e outro.
A regra de negocio nao foi tocada.

As datas continuam guardadas como texto dd/mm/aaaa, como o OMIE devolve; quem
converte e o modulo `fato`.
"""
import os
import json
import time
import hashlib
import tempfile
import logging
import datetime as dt

from .omie_client import OmieClient, OmieAPIError, erro_definitivo

VERSAO = "2026-09-02.1"

# Nada de logging.basicConfig aqui: este modulo e importado dentro do Flask e
# reconfigurar o logging raiz mexeria no log dos outros 14 modulos do monorepo.
log = logging.getLogger("painel.espelho")

# --------------------------------------------------------------------------- 
# Progresso
# --------------------------------------------------------------------------- 
# Quem dispara a atualizacao (o modulo `tarefas`) pluga aqui uma funcao para
# receber o andamento e mostra-lo na tela. Por padrao nao faz nada, entao este
# modulo continua rodando sozinho, sem depender de quem o chama.
#
# E um gancho de modulo, e nao um parametro passado de funcao em funcao, porque
# o progresso nasce dentro de lacos aninhados em meia duzia de funcoes — enfiar
# um argumento em todas elas so para carregar o recado seria pior de ler.
_relator = None


def definir_progresso(funcao):
    """Recebe uma funcao `f(etapa, detalhe)` chamada ao longo da atualizacao."""
    global _relator
    _relator = funcao


def _progresso(etapa, detalhe=""):
    if _relator is None:
        return
    try:
        _relator(etapa, detalhe)
    except Exception:      # informar andamento nunca pode derrubar a carga
        log.exception("Painel: falha ao registrar o andamento (seguindo mesmo assim)")

# Colunas de retencao (so existem em contas a receber, mas tratamos generico).
RETENCOES = ["valor_ir", "valor_iss", "valor_inss", "valor_pis", "valor_cofins", "valor_csll"]

# Diferenca de rateio ate este valor = arredondamento (ignora). Acima = inconsistencia
# de origem (nValDep defasado). Em ambos os casos GRAVAMOS como esta; o adaptador
# (etapa 5) explode por nPerDep x valor_documento, garantindo fechamento com o bruto.
TOL_RATEIO = 0.10


# ----------------------------------------------------------------------------- 
# Schema
# ----------------------------------------------------------------------------- 
# O schema saiu daqui e virou a migracao numerada 001_espelho_omie.sql.
# Motivo: mudanca de estrutura tem de ser explicita e aplicada por quem
# decide, nao acontecer sozinha na primeira vez que alguem abre a tela.


# --------------------------------------------------------------------------- 
# Conexao
# --------------------------------------------------------------------------- 
def conectar():
    """Conexao com o schema `painel` do Postgres, com a mesma interface que este
    codigo usava no sqlite3. Quem cria as tabelas e a migracao numerada aplicada
    pela tela de Configuracoes — nunca este modulo, e nunca no start do servico."""
    from ..db import ConexaoCompat, obter_engine
    return ConexaoCompat(obter_engine().raw_connection())


def _f(v):
    """Para float seguro (a API ja manda numero; protege contra None/''/str)."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, str):
        v = v.replace(".", "").replace(",", ".") if ("," in v and "." in v) else v.replace(",", ".")
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _s(v):
    return "" if v is None else str(v).strip()


def _dalt_para_data(dalt):
    """Converte 'dd/mm/aaaa' -> datetime.date (para comparar incremental). None se invalido."""
    try:
        d, m, a = (dalt or "").split("/")
        return dt.date(int(a), int(m), int(d))
    except Exception:
        return None


def _categoria_principal(rec):
    """Categoria do titulo. Usa o campo unico 'codigo_categoria'; se vier vazio
    (acontece em titulos com rateio de categoria / multi-categoria), pega a
    categoria DOMINANTE (maior valor) do array 'categorias[]'."""
    cc = _s(rec.get("codigo_categoria"))
    if cc:
        return cc
    cats = rec.get("categorias") or []
    if not cats:
        return None
    # escolhe a de maior valor; cai no primeiro item se nao houver valor
    try:
        melhor = max(cats, key=lambda c: _f(c.get("valor")) or 0.0)
    except (ValueError, TypeError):
        melhor = cats[0]
    return _s(melhor.get("codigo_categoria")) or None


def _observacao_titulo(rec):
    """Observacao do titulo. A Omie varia o nome do campo entre endpoints/versoes
    (e em alguns retornos ela vem aninhada em 'cabecTitulo'), entao tentamos os
    apelidos conhecidos antes de desistir."""
    v = _primeiro(rec, "observacao", "observacoes", "cObservacao", "cObsTitulo", "obs")
    if v in (None, ""):
        cab = rec.get("cabecTitulo") or {}
        if isinstance(cab, dict):
            v = _primeiro(cab, "observacao", "cObservacao", "cObsTitulo")
    return _s(v)


def _linha_titulo(rec, natureza):
    """Monta a tupla da tabela 'titulos' a partir de um cadastro do Omie."""
    info = rec.get("info") or {}
    return (
        int(rec["codigo_lancamento_omie"]),
        natureza,
        _f(rec.get("valor_documento")),
        _categoria_principal(rec),
        rec.get("codigo_cliente_fornecedor"),
        rec.get("id_conta_corrente"),
        _s(rec.get("numero_documento")),
        _s(rec.get("numero_documento_fiscal")),
        _s(rec.get("numero_pedido")),
        _s(rec.get("numero_parcela")),
        _s(rec.get("codigo_tipo_documento")),
        _s(rec.get("status_titulo")),
        _observacao_titulo(rec),
        _s(rec.get("data_emissao")),
        _s(rec.get("data_entrada")),
        _s(rec.get("data_registro")),
        _s(rec.get("data_previsao")),
        _s(rec.get("data_vencimento")),
        _f(rec.get("valor_ir")), _f(rec.get("valor_iss")), _f(rec.get("valor_inss")),
        _f(rec.get("valor_pis")), _f(rec.get("valor_cofins")), _f(rec.get("valor_csll")),
        _s(info.get("dInc")), _s(info.get("dAlt")), _s(info.get("hAlt")), _s(info.get("cImpAPI")),
        dt.datetime.now().isoformat(timespec="seconds"),
    )


_COLS_TITULO = (
    "codigo_lancamento_omie, natureza, valor_documento, codigo_categoria, "
    "codigo_cliente_fornecedor, id_conta_corrente, numero_documento, "
    "numero_documento_fiscal, numero_pedido, numero_parcela, codigo_tipo_documento, "
    "status_titulo, observacao, data_emissao, data_entrada, data_registro, data_previsao, "
    "data_vencimento, valor_ir, valor_iss, valor_inss, valor_pis, valor_cofins, "
    "valor_csll, dinc, dalt, halt, cimpapi, sync_em"
)
_PH_TITULO = ",".join(["?"] * len(_COLS_TITULO.split(", ")))


def _linhas_rateio(rec):
    """Explode distribuicao[] em linhas de rateio. Lista vazia se nao houver."""
    out = []
    for i, d in enumerate(rec.get("distribuicao") or []):
        out.append((
            int(rec["codigo_lancamento_omie"]), i,
            _s(d.get("cCodDep")), _s(d.get("cDesDep")),
            _f(d.get("nPerDep")), _f(d.get("nValDep")),
        ))
    return out


# ----------------------------------------------------------------------------- 
# Gravacao (upsert) — mesma logica para API e amostras
# ----------------------------------------------------------------------------- 
def gravar_titulos(conn, registros, natureza):
    """
    Insere/atualiza titulos + rateio para um lote de cadastros.
    Retorna (qtd_titulos, qtd_linhas_rateio, problemas[]).
    problemas: lista de (codigo, motivo) com inconsistencias de rateio.
    """
    titulos, rateios, problemas = [], [], []
    for rec in registros:
        if rec.get("codigo_lancamento_omie") in (None, ""):
            problemas.append((None, "registro sem codigo_lancamento_omie"))
            continue
        cod = int(rec["codigo_lancamento_omie"])
        titulos.append(_linha_titulo(rec, natureza))
        linhas = _linhas_rateio(rec)
        rateios.extend(linhas)
        # validacao: soma do rateio deve fechar com o valor do documento
        if linhas:
            soma = sum(l[5] for l in linhas)
            vdoc = _f(rec.get("valor_documento"))
            if abs(soma - vdoc) > TOL_RATEIO:
                problemas.append((cod, f"rateio {soma:.2f} != documento {vdoc:.2f} (dif {soma - vdoc:+.2f})"))

    cur = conn.cursor()
    # A observacao NAO vem na listagem (so no ConsultarConta*), entao o excluded.
    # observacao e sempre vazio. Sobrescrever apagaria o backfill a cada sync —
    # por isso ela so e substituida quando vier valor de verdade. Mesma logica para
    # observacao_sync, que nem faz parte da listagem.
    _preservar = {"observacao"}
    _sets = []
    for c in _COLS_TITULO.split(", "):
        if c == "codigo_lancamento_omie":
            continue
        if c in _preservar:
            _sets.append(f"{c}=COALESCE(NULLIF(excluded.{c},''), titulos.{c})")
        else:
            _sets.append(f"{c}=excluded.{c}")
    cur.executemany(
        f"INSERT INTO titulos ({_COLS_TITULO}) VALUES ({_PH_TITULO}) "
        f"ON CONFLICT(codigo_lancamento_omie) DO UPDATE SET " + ", ".join(_sets),
        titulos,
    )
    # rateio: apaga o que existia desses titulos e regrava (idempotente)
    if rateios:
        cods = sorted({r[0] for r in rateios})
        cur.executemany("DELETE FROM rateio WHERE codigo_lancamento_omie=?",
                        [(c,) for c in cods])
        cur.executemany(
            "INSERT INTO rateio (codigo_lancamento_omie, seq, ccoddep, cdesdep, nperdep, nvaldep) "
            "VALUES (?,?,?,?,?,?)", rateios)
    conn.commit()
    return len(titulos), len(rateios), problemas


def _atualizar_sync_state(conn, entidade, registros, total_esperado):
    """Grava maior dAlt visto, timestamp e total na sync_state."""
    maior = None
    for rec in registros:
        d = _dalt_para_data((rec.get("info") or {}).get("dAlt"))
        if d and (maior is None or d > maior):
            maior = d
    maior_str = maior.strftime("%d/%m/%Y") if maior else None
    conn.execute(
        "INSERT INTO sync_state (entidade, ultima_dalt, ultima_sync, total_registros) "
        "VALUES (?,?,?,?) ON CONFLICT(entidade) DO UPDATE SET "
        "ultima_dalt=COALESCE(MAX(excluded.ultima_dalt, sync_state.ultima_dalt), excluded.ultima_dalt), "
        "ultima_sync=excluded.ultima_sync, total_registros=excluded.total_registros",
        (entidade, maior_str, dt.datetime.now().isoformat(timespec="seconds"), total_esperado),
    )
    conn.commit()


# ----------------------------------------------------------------------------- 
# Movimentos
# ----------------------------------------------------------------------------- 
_COLS_MOV = (
    "ncodtitulo, cnatureza, cgrupo, cstatus, ccodcateg, ncodcc, ncodcliente, "
    "ddtpagamento, ddtvenc, ddtemissao, ddtregistro, cliquidado, nvalortitulo, "
    "nvalpago, nvalliquido, nvalaberto, njuros, nmulta, ndesconto, sync_em"
)
_PH_MOV = ",".join(["?"] * 20)


def _linha_movimento(mv):
    d = mv.get("detalhes") or {}
    r = mv.get("resumo") or {}
    cod = d.get("nCodTitulo") or d.get("nCodTitRepet")
    return (
        int(cod) if cod not in (None, "") else None,
        _s(d.get("cNatureza")), _s(d.get("cGrupo")), _s(d.get("cStatus")),
        _s(d.get("cCodCateg")), d.get("nCodCC"), d.get("nCodCliente"),
        _s(d.get("dDtPagamento")), _s(d.get("dDtVenc")), _s(d.get("dDtEmissao")),
        _s(d.get("dDtRegistro")), _s(r.get("cLiquidado")), _f(d.get("nValorTitulo")),
        _f(r.get("nValPago")), _f(r.get("nValLiquido")), _f(r.get("nValAberto")),
        _f(r.get("nJuros")), _f(r.get("nMulta")), _f(r.get("nDesconto")),
        dt.datetime.now().isoformat(timespec="seconds"),
    )


def gravar_movimentos(conn, registros):
    """Insere um lote de movimentos. Retorna (qtd, ignorados_sem_titulo)."""
    linhas, ignorados = [], 0
    for mv in registros:
        linha = _linha_movimento(mv)
        if linha[0] is None:
            ignorados += 1
            continue
        linhas.append(linha)
    conn.executemany(
        f"INSERT INTO movimentos ({_COLS_MOV}) VALUES ({_PH_MOV})", linhas)
    conn.commit()
    return len(linhas), ignorados


def movimentos_por_titulo(conn):
    """
    Agrega movimentos por titulo (regra do adaptador):
      - data_pagamento = MAIOR dDtPagamento entre os movimentos liquidados (S)
      - total_pago     = soma de nValPago
      - total_aberto   = soma de nValAberto
      - total_desconto = soma de nDesconto (descontos reduzem o caixa realizado)
      - liquidado      = 'S' se houver ao menos um movimento liquidado
    Retorna dict: ncodtitulo -> (data_pagamento, total_pago, total_aberto, total_desconto, liquidado).
    """
    res = {}
    cur = conn.execute(
        "SELECT ncodtitulo, ddtpagamento, cliquidado, nvalpago, nvalaberto, ndesconto, "
        "njuros, nmulta FROM movimentos")
    agg = {}
    for cod, dpg, liq, vpg, vab, vdesc, vjur, vmul in cur:
        a = agg.setdefault(cod, {"dpg": None, "pago": 0.0, "aberto": 0.0, "desc": 0.0,
                                 "juros": 0.0, "multa": 0.0, "liq": "N"})
        a["aberto"] += (vab or 0.0)
        # SO conta pago/desconto/juros/multa de movimentos efetivamente LIQUIDADOS (cLiquidado=S).
        # Movimentos nao-liquidados (perna de conta corrente, previsao) tambem trazem
        # nValPago e dobrariam o caixa realizado se fossem somados.
        if liq == "S":
            a["liq"] = "S"
            a["pago"] += (vpg or 0.0)
            a["desc"] += (vdesc or 0.0)
            a["juros"] += (vjur or 0.0)
            a["multa"] += (vmul or 0.0)
            d = _dalt_para_data(dpg)  # mesmo parser dd/mm/aaaa
            if d and (a["dpg"] is None or d > a["dpg"]):
                a["dpg"] = d
    for cod, a in agg.items():
        data_str = a["dpg"].strftime("%d/%m/%Y") if a["dpg"] else None
        res[cod] = (data_str, round(a["pago"], 2), round(a["aberto"], 2),
                    round(a["desc"], 2), a["liq"], round(a["juros"], 2), round(a["multa"], 2))
    return res


def movimentos_detalhe(conn, apenas_liquidados=False):
    """
    Movimentos EM DETALHE (sem agregar), por titulo — base do 'Receita Analitico'.

    Devolve TODOS os movimentos por padrao, porque o Omie usa varias pernas para o
    mesmo titulo e quem escolhe e o adaptador:
      - perna de PREVISAO: sem data de pagamento, nValAberto = saldo, cLiquidado='N';
      - BAIXA CONSOLIDADA: cLiquidado='S', nValLiquido > 0, na conta do titulo,
        com o total recebido em UMA linha;
      - CREDITOS BANCARIOS: cLiquidado vazio, nValLiquido = 0, um por entrada real
        (e a abertura em parcelas que interessa).
    Somar tudo dobraria o caixa: a consolidada repete os creditos.

    Retorna dict: ncodtitulo -> [ {data, valor, liquido, juros, multa, desconto,
                                   conta, status, grupo, liquidado}, ... ]
    """
    res = {}
    sql = ("SELECT ncodtitulo, ddtpagamento, nvalpago, nvalliquido, njuros, nmulta, "
           "ndesconto, ncodcc, cstatus, cliquidado, cgrupo, nvalaberto FROM movimentos")
    for (cod, dpg, vpg, vliq, vjur, vmul, vdesc, ncc, cst, liq, grp, vab) in conn.execute(sql):
        if apenas_liquidados and liq != "S":
            continue
        res.setdefault(cod, []).append({
            "data": dpg or "",
            "valor": float(vpg or 0.0),
            "liquido": float(vliq or 0.0),
            "aberto": float(vab or 0.0),
            "juros": float(vjur or 0.0),
            "multa": float(vmul or 0.0),
            "desconto": float(vdesc or 0.0),
            "conta": ncc,
            "status": (cst or "").strip(),
            "grupo": (grp or "").strip(),
            "liquidado": (liq or "").strip(),
        })
    for cod, lst in res.items():
        lst.sort(key=lambda m: (_dalt_para_data(m["data"]) or dt.date(1900, 1, 1)))
    return res


# ----------------------------------------------------------------------------- 
# Catalogos (categorias / clientes)
# ----------------------------------------------------------------------------- 
def gravar_categorias(conn, registros):
    """Grava categorias e resolve 'grupo' = descricao da categoria_superior."""
    linhas = []
    agora = dt.datetime.now().isoformat(timespec="seconds")
    for c in registros:
        codigo = _s(c.get("codigo") or c.get("codigo_categoria"))
        if not codigo:
            continue
        superior = _s(c.get("categoria_superior"))
        dados_dre = c.get("dadosDRE") or {}
        desc_dre = _s(dados_dre.get("descricaoDRE"))
        linhas.append((codigo, _s(c.get("descricao")), superior,
                       _s(c.get("natureza")), _s(c.get("conta_inativa")),
                       _s(c.get("codigo_dre")), _s(c.get("transferencia")),
                       desc_dre, _s(c.get("totalizadora")), agora))
    conn.executemany(
        "INSERT INTO cat (codigo, descricao, categoria_superior, natureza, conta_inativa, "
        "codigo_dre, transferencia, descricao_dre, totalizadora, sync_em) "
        "VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(codigo) DO UPDATE SET "
        "descricao=excluded.descricao, categoria_superior=excluded.categoria_superior, "
        "natureza=excluded.natureza, conta_inativa=excluded.conta_inativa, "
        "codigo_dre=excluded.codigo_dre, transferencia=excluded.transferencia, "
        "descricao_dre=excluded.descricao_dre, totalizadora=excluded.totalizadora, "
        "sync_em=excluded.sync_em",
        linhas)
    # resolve grupo a partir do mapa codigo->descricao ja gravado:
    # grupo = descricao da categoria_superior; se nao houver superior, usa a propria descricao.
    mapa = dict(conn.execute("SELECT codigo, descricao FROM cat").fetchall())
    updates = []
    for cod, desc, sup in conn.execute(
            "SELECT codigo, descricao, categoria_superior FROM cat").fetchall():
        grupo = mapa.get(sup) if sup else None
        updates.append((grupo or desc, cod))
    conn.executemany("UPDATE cat SET grupo=? WHERE codigo=?", updates)
    conn.commit()
    return len(linhas)


def _primeiro(rec, *chaves):
    """Primeiro valor nao-vazio entre varias chaves possiveis (a Omie varia o nome
    do campo entre endpoints; isso evita quebrar se o payload vier diferente)."""
    for k in chaves:
        v = rec.get(k)
        if v not in (None, ""):
            return v
    return None


def gravar_contas_correntes(conn, registros):
    """Grava o catalogo de contas correntes (nCodCC -> descricao)."""
    linhas = []
    agora = dt.datetime.now().isoformat(timespec="seconds")
    for c in registros:
        cod = _primeiro(c, "nCodCC", "codigo", "nCodCCInt")
        if cod in (None, ""):
            continue
        try:
            cod = int(cod)
        except (TypeError, ValueError):
            continue
        desc = _s(_primeiro(c, "descricao", "cDescricao", "nome", "cNome", "cCodCCInt"))
        linhas.append((
            cod, desc,
            _s(_primeiro(c, "tipo_conta", "cTipo")),
            _s(_primeiro(c, "codigo_banco", "cCodBanco", "nome_banco")),
            _s(_primeiro(c, "codigo_agencia", "cAgencia")),
            _s(_primeiro(c, "conta_corrente", "numero_conta", "cConta")),
            _s(_primeiro(c, "nao_ativo", "inativo", "cInativo")),
            agora,
        ))
    conn.executemany(
        "INSERT INTO contas_correntes (codigo, descricao, tipo_conta, codigo_banco, agencia, "
        "numero_conta, inativa, sync_em) VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT(codigo) DO UPDATE SET descricao=excluded.descricao, "
        "tipo_conta=excluded.tipo_conta, codigo_banco=excluded.codigo_banco, "
        "agencia=excluded.agencia, numero_conta=excluded.numero_conta, "
        "inativa=excluded.inativa, sync_em=excluded.sync_em",
        linhas)
    conn.commit()
    return len(linhas)


def sincronizar_contas_correntes(conn, cli):
    """Baixa e grava o catalogo de contas correntes. Cadastro pequeno: 1-2 paginas."""
    tot = 0
    for _pag, _tp, _tr, registros in cli.listar_contas_correntes():
        tot += gravar_contas_correntes(conn, registros)
    conn.execute(
        "INSERT INTO sync_state (entidade, ultima_sync, total_registros) "
        "VALUES ('contas_correntes',?,?) ON CONFLICT(entidade) DO UPDATE SET "
        "ultima_sync=excluded.ultima_sync, total_registros=excluded.total_registros",
        (dt.datetime.now().isoformat(timespec="seconds"), tot))
    conn.commit()
    log.info("OK contas correntes -> %d registros.", tot)
    return tot


def contas_correntes_map(conn):
    """{nCodCC: 'nome da conta'} para o adaptador resolver o codigo do titulo.
    Tolerante a banco antigo (tabela ainda inexistente) -> devolve {}."""
    try:
        cur = conn.execute("SELECT codigo, descricao FROM contas_correntes")
        linhas = cur.fetchall()
        cur.close()
    except Exception:   # tabela ainda nao migrada
        conn.rollback()
        return {}
    return {cod: (desc or "").strip() for cod, desc in linhas if (desc or "").strip()}


def gravar_clientes(conn, registros):
    linhas = []
    agora = dt.datetime.now().isoformat(timespec="seconds")
    for c in registros:
        cod = c.get("codigo_cliente_omie") or c.get("codigo")
        if cod in (None, ""):
            continue
        linhas.append((int(cod), _s(c.get("razao_social")), _s(c.get("nome_fantasia")),
                       _s(c.get("cnpj_cpf")), agora))
    conn.executemany(
        "INSERT INTO clientes (codigo, razao_social, nome_fantasia, cnpj_cpf, sync_em) "
        "VALUES (?,?,?,?,?) ON CONFLICT(codigo) DO UPDATE SET "
        "razao_social=excluded.razao_social, nome_fantasia=excluded.nome_fantasia, "
        "cnpj_cpf=excluded.cnpj_cpf, sync_em=excluded.sync_em",
        linhas)
    conn.commit()
    return len(linhas)


# ----------------------------------------------------------------------------- 
# Carga inicial real (API)
# ----------------------------------------------------------------------------- 
def carga_inicial(env=".env"):
    cli = OmieClient.de_ambiente(env)
    conn = conectar()
    try:
        for entidade, natureza, metodo in (
            ("contapagar", "P", cli.listar_contas_pagar),
            ("contareceber", "R", cli.listar_contas_receber),
        ):
            log.info("=== Carga inicial: %s ===", entidade)
            rotulo = "contas a pagar" if natureza == "P" else "contas a receber"
            _progresso(f"baixando {rotulo} do OMIE", "começando")
            tot_tit = tot_rat = 0
            total_esperado = None
            t0 = time.time()
            todos_para_state = []
            for pagina, total_paginas, total_registros, registros in metodo():
                if total_esperado is None:
                    total_esperado = total_registros
                    log.info("%s: %s titulos em %s paginas.", entidade,
                             f"{total_registros:,}".replace(",", "."), total_paginas)
                qt, qr, problemas = gravar_titulos(conn, registros, natureza)
                tot_tit += qt
                tot_rat += qr
                todos_para_state.extend(registros)
                for cod, motivo in problemas:
                    log.warning("  [%s] rateio: %s", cod, motivo)
                if pagina % 5 == 0 or pagina == total_paginas:
                    _progresso(f"baixando {rotulo} do OMIE",
                               f"página {pagina} de {total_paginas} — "
                               f"{tot_tit:,} títulos".replace(",", "."))
                if pagina % 20 == 0 or pagina == total_paginas:
                    log.info("  pag %d/%d  | titulos=%s  rateio=%s  (%.0fs)",
                             pagina, total_paginas,
                             f"{tot_tit:,}".replace(",", "."),
                             f"{tot_rat:,}".replace(",", "."), time.time() - t0)
            _atualizar_sync_state(conn, entidade, todos_para_state, total_esperado or tot_tit)
            log.info("OK %s -> %s titulos, %s linhas de rateio.",
                     entidade, f"{tot_tit:,}".replace(",", "."),
                     f"{tot_rat:,}".replace(",", "."))

        # ---- Catalogos ----
        log.info("=== Carga inicial: categorias ===")
        _progresso("baixando o plano de contas", "")
        tot = 0
        for pagina, total_paginas, total_registros, registros in cli.listar_categorias():
            tot += gravar_categorias(conn, registros)
            if pagina == total_paginas:
                log.info("OK categorias -> %s registros.", f"{tot:,}".replace(",", "."))
        conn.execute(
            "INSERT INTO sync_state (entidade, ultima_sync, total_registros) VALUES ('categorias',?,?) "
            "ON CONFLICT(entidade) DO UPDATE SET ultima_sync=excluded.ultima_sync, "
            "total_registros=excluded.total_registros",
            (dt.datetime.now().isoformat(timespec="seconds"), tot))

        log.info("=== Carga inicial: clientes/fornecedores ===")
        _progresso("baixando clientes e fornecedores", "")
        tot = 0
        for pagina, total_paginas, total_registros, registros in cli.listar_clientes():
            tot += gravar_clientes(conn, registros)
            if pagina % 20 == 0 or pagina == total_paginas:
                _progresso("baixando clientes e fornecedores",
                           f"página {pagina} de {total_paginas}")
                log.info("  clientes pag %d/%d -> %s", pagina, total_paginas,
                         f"{tot:,}".replace(",", "."))
        conn.execute(
            "INSERT INTO sync_state (entidade, ultima_sync, total_registros) VALUES ('clientes',?,?) "
            "ON CONFLICT(entidade) DO UPDATE SET ultima_sync=excluded.ultima_sync, "
            "total_registros=excluded.total_registros",
            (dt.datetime.now().isoformat(timespec="seconds"), tot))
        conn.commit()

        log.info("=== Carga inicial: contas correntes ===")
        _progresso("baixando as contas correntes", "")
        sincronizar_contas_correntes(conn, cli)

        # ---- Movimentos + Planilha (helpers reutilizaveis) ----
        carregar_movimentos_full(conn, cli)
        sincronizar_planilha(conn)

        _resumo(conn)
    finally:
        conn.close()


def recarregar_titulos(env=".env"):
    """Re-baixa SO contas a pagar + receber + categorias, atualizando os titulos
    no lugar (UPSERT). NAO mexe em movimentos/clientes/planilha. Use para corrigir
    titulos sem categoria (categoria que estava so no array categorias[])."""
    cli = OmieClient.de_ambiente(env)
    conn = conectar()
    try:
        antes = conn.execute(
            "SELECT COUNT(*) FROM titulos WHERE codigo_categoria IS NULL OR codigo_categoria=''").fetchone()[0]
        log.info("Titulos sem categoria ANTES: %s", f"{antes:,}".replace(",", "."))
        for entidade, natureza, metodo in (
            ("contapagar", "P", cli.listar_contas_pagar),
            ("contareceber", "R", cli.listar_contas_receber),
        ):
            log.info("=== Recarregar titulos: %s ===", entidade)
            tot_tit = 0
            t0 = time.time()
            total_esperado = None
            todos_para_state = []
            for pagina, total_paginas, total_registros, registros in metodo():
                if total_esperado is None:
                    total_esperado = total_registros
                    log.info("%s: %s titulos em %s paginas.", entidade,
                             f"{total_registros:,}".replace(",", "."), total_paginas)
                qt, _qr, _pr = gravar_titulos(conn, registros, natureza)
                tot_tit += qt
                todos_para_state.extend(registros)
                if pagina % 20 == 0 or pagina == total_paginas:
                    _progresso("baixando o que mudou no OMIE",
                               f"página {pagina} de {total_paginas}")
                    log.info("  pag %d/%d  | titulos=%s  (%.0fs)", pagina, total_paginas,
                             f"{tot_tit:,}".replace(",", "."), time.time() - t0)
            _atualizar_sync_state(conn, entidade, todos_para_state, total_esperado or tot_tit)

        log.info("=== Recarregar: categorias ===")
        tot = 0
        for pagina, total_paginas, total_registros, registros in cli.listar_categorias():
            tot += gravar_categorias(conn, registros)
        conn.execute(
            "INSERT INTO sync_state (entidade, ultima_sync, total_registros) VALUES ('categorias',?,?) "
            "ON CONFLICT(entidade) DO UPDATE SET ultima_sync=excluded.ultima_sync, "
            "total_registros=excluded.total_registros",
            (dt.datetime.now().isoformat(timespec="seconds"), tot))
        conn.commit()

        log.info("=== Recarregar: contas correntes ===")
        sincronizar_contas_correntes(conn, cli)

        depois = conn.execute(
            "SELECT COUNT(*) FROM titulos WHERE codigo_categoria IS NULL OR codigo_categoria=''").fetchone()[0]
        log.info("Titulos sem categoria DEPOIS: %s (antes era %s)",
                 f"{depois:,}".replace(",", "."), f"{antes:,}".replace(",", "."))
        _resumo(conn)
    finally:
        conn.close()


def carregar_movimentos_full(conn, cli):
    """Carga COMPLETA de movimentos (zera a tabela e baixa tudo). Sem filtro de data."""
    log.info("=== Movimentos (carga completa) ===")
    conn.execute("DELETE FROM movimentos")
    conn.commit()
    tot_mov = ign = 0
    t0 = time.time()
    # ListarMovimentos NAO aceita cTipoData; para baixar tudo basta paginar sem filtro.
    for pagina, total_paginas, total_registros, registros in cli.listar_movimentos():
        if pagina == 1:
            log.info("movimentos: %s registros em %s paginas.",
                     f"{total_registros:,}".replace(",", "."), total_paginas)
        qm, qi = gravar_movimentos(conn, registros)
        tot_mov += qm
        ign += qi
        if pagina % 50 == 0 or pagina == total_paginas:
            _progresso("baixando os pagamentos e recebimentos",
                       f"página {pagina} de {total_paginas} — "
                       f"{tot:,} movimentos".replace(",", "."))
            log.info("  mov pag %d/%d -> %s (%.0fs)", pagina, total_paginas,
                     f"{tot_mov:,}".replace(",", "."), time.time() - t0)
    conn.execute(
        "INSERT INTO sync_state (entidade, ultima_sync, total_registros) VALUES ('movimentos',?,?) "
        "ON CONFLICT(entidade) DO UPDATE SET ultima_sync=excluded.ultima_sync, "
        "total_registros=excluded.total_registros",
        (dt.datetime.now().isoformat(timespec="seconds"), tot_mov))
    conn.commit()
    log.info("OK movimentos -> %s gravados (%d ignorados sem titulo).",
             f"{tot_mov:,}".replace(",", "."), ign)
    return tot_mov


def sincronizar_planilha(conn):
    """De-para departamento->projeto via planilha C. Diarios (nao quebra se faltar credencial)."""
    try:
        from . import projetos
        n = projetos.sincronizar(conn)
        log.info("OK depto_projeto -> %s departamentos mapeados.", n)
        return n
    except Exception as e:
        # rollback obrigatorio: no Postgres a transacao fica abortada apos o erro
        # e todo comando seguinte falharia junto.
        conn.rollback()
        log.warning("De-para obra->projeto nao sincronizado (%s). Confira as variaveis "
                    "GOOGLE_CREDENTIALS_BASE64 e PAINEL_SHEET_PROJETOS.", e)
        return 0


def completar(env=".env"):
    """Baixa SO o que costuma faltar apos a carga (movimentos + planilha), sem refazer
    titulos/catalogos. Use quando a carga inicial morreu no meio dos movimentos."""
    cli = OmieClient.de_ambiente(env)
    conn = conectar()
    try:
        carregar_movimentos_full(conn, cli)
        sincronizar_planilha(conn)
        _resumo(conn)
    finally:
        conn.close()


def carregar_contas_correntes(env=".env"):
    """Baixa SO o catalogo de contas correntes (nCodCC -> nome). Rapido, 1-2 chamadas.
    Use para popular o nome da conta sem refazer titulos/movimentos."""
    cli = OmieClient.de_ambiente(env)
    conn = conectar()
    try:
        sincronizar_contas_correntes(conn, cli)
        for cod, desc in conn.execute(
                "SELECT codigo, descricao FROM contas_correntes ORDER BY descricao"):
            print(f"  {cod:>10} -> {desc}")
    finally:
        conn.close()


# ----------------------------------------------------------------------------- 
# Incremental (etapa 4) — baixa so o que mudou desde a ultima sync
# ----------------------------------------------------------------------------- 
def _varrer_json(obj, prefixo=""):
    """(caminho, valor) para todas as folhas de um payload JSON."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _varrer_json(v, f"{prefixo}.{k}" if prefixo else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _varrer_json(v, f"{prefixo}[{i}]")
    else:
        yield prefixo, obj


def _extrair_observacao(payload):
    """
    Acha a observacao dentro do retorno do Consultar*, sem depender do nome exato
    do campo: procura qualquer chave com 'obs' no nome e valor de texto.
    Retorna (texto, caminho) ou (None, None).
    """
    for caminho, valor in _varrer_json(payload):
        folha = caminho.split(".")[-1].split("[")[0].lower()
        if "obs" in folha and isinstance(valor, str) and valor.strip():
            return valor.strip(), caminho
    return None, None


def backfill_observacoes(env=".env", natureza="R",
                         desde=None, limite=None, sonda=5, reconsultar=False,
                         pausa=None, forcar=False):
    """
    Preenche titulos.observacao consultando UM titulo por vez (ConsultarContaPagar/
    Receber), porque a listagem nao devolve esse campo.

    Custo: 1 chamada por titulo. Contas a receber (~2 mil) leva minutos; contas a
    pagar (~116 mil) leva HORAS — por isso o --desde e o --limite.

    Retomavel de verdade: cada titulo consultado marca observacao_sync, entao uma
    nova execucao pula tudo que ja foi tentado, inclusive quem voltou sem observacao
    (o que no contas a pagar costuma ser a maioria). Use --reconsultar para forcar.
    """
    cli = OmieClient.de_ambiente(env)
    espera = cli.pausa if pausa is None else float(pausa)
    conn = conectar()
    try:
        sql = ("SELECT codigo_lancamento_omie FROM titulos "
               "WHERE natureza=? AND TRIM(COALESCE(observacao,''))='' "
               "AND UPPER(COALESCE(status_titulo,''))<>'CANCELADO'")
        params = [natureza]
        if not reconsultar:
            sql += " AND observacao_sync IS NULL"
        if desde:
            sql += " AND CAST(substr(data_vencimento,7,4) AS INTEGER) >= ?"
            params.append(int(desde))
        sql += " ORDER BY substr(data_vencimento,7,4) DESC, substr(data_vencimento,4,2) DESC"
        if limite:
            sql += f" LIMIT {int(limite)}"
        pendentes = [r[0] for r in conn.execute(sql, params)]
        if not pendentes:
            log.info("Nada a fazer: nenhum titulo pendente nesse filtro. "
                     "(Use --reconsultar para tentar de novo os ja consultados.)")
            return 0

        seg = len(pendentes) * (espera + 0.45)   # 0.45s = latencia tipica por chamada
        log.info("Titulos a consultar: %s (natureza=%s%s). Tempo estimado: %s.",
                 f"{len(pendentes):,}".replace(",", "."), natureza,
                 f", desde {desde}" if desde else "",
                 f"{seg/3600:.1f} h" if seg > 3600 else f"{seg/60:.0f} min")

        # --- SONDA: confere em poucos titulos se a observacao existe mesmo ---
        # Amostra ESPALHADA pela lista: pegar so os primeiros enviesa (sao os de
        # vencimento mais distante, tipicamente provisoes sem anotacao nenhuma).
        n_sonda = min(int(sonda), len(pendentes))
        passo = max(1, len(pendentes) // n_sonda)
        amostra = pendentes[::passo][:n_sonda]

        campos_obs = {}      # caminho -> [preenchidos, vistos, exemplo]
        total_campos = 0
        for cod in amostra:
            try:
                payload = cli.consultar_titulo(cod, natureza)
            except Exception as e:
                log.warning("Sonda: falha ao consultar %s: %s", cod, e)
                continue
            time.sleep(espera)
            folhas = list(_varrer_json(payload))
            total_campos = max(total_campos, len(folhas))
            for caminho, valor in folhas:
                nome = caminho.split(".")[-1].split("[")[0].lower()
                if "obs" in nome or "hist" in nome:
                    reg = campos_obs.setdefault(caminho, [0, 0, ""])
                    reg[1] += 1
                    if isinstance(valor, str) and valor.strip():
                        reg[0] += 1
                        if not reg[2]:
                            reg[2] = valor.strip()[:60]

        log.info("Sonda: %d titulos consultados, %d campos por titulo.",
                 len(amostra), total_campos)
        if not campos_obs:
            log.warning(
                "NENHUM campo de observacao existe no retorno do Consultar (natureza=%s). "
                "Rode  python sync_omie.py --dump-titulo CODIGO --natureza %s  usando um "
                "titulo que voce SABE que tem observacao preenchida no Omie, para confirmar.",
                natureza, natureza)
            return 0

        for caminho, (cheios, vistos, exemplo) in sorted(campos_obs.items()):
            log.info("  campo %s: preenchido em %d de %d  %s",
                     caminho, cheios, vistos, f"-> {exemplo!r}" if exemplo else "")
        achou_campo = sum(c[0] for c in campos_obs.values())
        if not achou_campo:
            log.warning(
                "O campo de observacao EXISTE no retorno, mas veio vazio nos %d titulos "
                "sorteados — provavelmente eles nao tem observacao preenchida no Omie. "
                "Isso NAO significa que o backfill nao funciona. Opcoes: (1) rode com "
                "--limite 200 para testar uma faixa maior; (2) rode --dump-titulo CODIGO "
                "com um titulo que voce sabe que tem observacao; (3) use --forcar para "
                "tocar o backfill mesmo assim.", len(amostra))
            if not forcar:
                return 0
        else:
            log.info("Sonda OK: observacao encontrada em %d dos %d titulos sorteados.",
                     achou_campo, len(amostra))

        # --- BACKFILL ---
        gravados, vazios, erros, inexistentes = 0, 0, 0, 0
        seguidas = 0
        t0 = time.time()
        for i, cod in enumerate(pendentes, start=1):
            agora = dt.datetime.now().isoformat(timespec="seconds")
            try:
                payload = cli.consultar_titulo(cod, natureza)
                seguidas = 0
            except Exception as e:
                # "Lancamento nao cadastrado": o titulo sumiu do Omie depois do sync.
                # Marca como consultado para nao voltar a cada nova execucao.
                if erro_definitivo(e):
                    inexistentes += 1
                    conn.execute("UPDATE titulos SET observacao_sync=? "
                                 "WHERE codigo_lancamento_omie=?", (agora, cod))
                else:
                    erros += 1
                    seguidas += 1
                    log.warning("Falha em %s: %s", cod, e)
                    if seguidas >= 50:
                        conn.commit()
                        log.error("50 falhas seguidas — abortando. Ja gravados: %d. "
                                  "Rode de novo depois; o progresso esta salvo.", gravados)
                        break
                continue
            finally:
                time.sleep(espera)   # respeita o rate limit do Omie
            texto, _c = _extrair_observacao(payload)
            if texto:
                conn.execute("UPDATE titulos SET observacao=?, observacao_sync=? "
                             "WHERE codigo_lancamento_omie=?", (texto, agora, cod))
                gravados += 1
            else:
                conn.execute("UPDATE titulos SET observacao_sync=? "
                             "WHERE codigo_lancamento_omie=?", (agora, cod))
                vazios += 1
            if i % 100 == 0:
                conn.commit()
                falta = (time.time() - t0) / i * (len(pendentes) - i)
                log.info("  %s/%s — com obs: %s | sem: %s | inexistentes: %s | erros: %d "
                         "| restam ~%s",
                         f"{i:,}".replace(",", "."),
                         f"{len(pendentes):,}".replace(",", "."),
                         f"{gravados:,}".replace(",", "."),
                         f"{vazios:,}".replace(",", "."),
                         f"{inexistentes:,}".replace(",", "."), erros,
                         f"{falta/3600:.1f} h" if falta > 3600 else f"{falta/60:.0f} min")
        conn.commit()
        log.info("OK backfill: %s com observacao, %s sem, %s inexistentes no Omie, %d erros.",
                 f"{gravados:,}".replace(",", "."),
                 f"{vazios:,}".replace(",", "."),
                 f"{inexistentes:,}".replace(",", "."), erros)
        if inexistentes:
            log.info("Os %s titulos 'nao cadastrados' ficaram marcados e nao voltam nas "
                     "proximas execucoes. Eles existem no espelho mas nao no Omie — "
                     "provavelmente excluidos depois do ultimo sync.",
                     f"{inexistentes:,}".replace(",", "."))
        return gravados
    finally:
        conn.close()


def dump_titulo(env=".env", codigo=None, natureza="P",
                salvar="titulo_dump.json"):
    """
    Despeja o retorno COMPLETO do ConsultarContaPagar/Receber para UM titulo.
    Sem corte, sem filtro. E o jeito definitivo de descobrir onde (e se) a Omie
    guarda a observacao: pegue no Omie um titulo que voce SABE que tem observacao
    preenchida, anote o codigo e rode isto.
    """
    cli = OmieClient.de_ambiente(env)
    payload = cli.consultar_titulo(codigo, natureza)
    folhas = list(_varrer_json(payload))
    print(f"\nTitulo {codigo} (natureza={natureza}) — {len(folhas)} campos:\n")
    for caminho, valor in sorted(folhas):
        v = "" if valor is None else str(valor)
        marca = "  <<<" if ("obs" in caminho.split(".")[-1].lower() and v.strip()) else ""
        print(f"  {caminho:46s} = {v[:70]}{marca}")
    cands = [(c, v) for c, v in folhas
             if "obs" in c.split(".")[-1].split("[")[0].lower()]
    print("\nCampos com 'obs' no nome:")
    if cands:
        for c, v in cands:
            estado = "PREENCHIDO" if (isinstance(v, str) and v.strip()) else "vazio"
            print(f"  {c:46s} [{estado}] {str(v)[:60]}")
    else:
        print("  NENHUM — a Omie nao expoe observacao para este titulo.")
    if salvar:
        with open(salvar, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"\npayload completo salvo em: {os.path.abspath(salvar)}")
    return payload


def _ler_sync_state(conn, entidade):
    r = conn.execute("SELECT ultima_dalt, ultima_sync FROM sync_state WHERE entidade=?",
                     (entidade,)).fetchone()
    return r if r else (None, None)


def aplicar_incremental_titulos(conn, natureza, registros, cutoff_date):
    """
    Guarda client-side: ignora registros cujo info.dAlt < cutoff_date (defesa caso o
    filtro server-side nao seja respeitado). Faz upsert (idempotente) do restante.
    Retorna (qt, qr, ignorados, maior_dalt, problemas).
    """
    novos, ignorados, maior = [], 0, None
    for rec in registros:
        d = _dalt_para_data((rec.get("info") or {}).get("dAlt"))
        if cutoff_date and d and d < cutoff_date:
            ignorados += 1
            continue
        novos.append(rec)
        if d and (maior is None or d > maior):
            maior = d
    if novos:
        qt, qr, problemas = gravar_titulos(conn, novos, natureza)
    else:
        qt, qr, problemas = 0, 0, []
    return qt, qr, ignorados, maior, problemas


def _apagar_movimentos_janela(conn, ini, fim):
    """Apaga movimentos com data de pagamento dentro de [ini, fim].

    A versao anterior lia as 240 mil linhas para a memoria so para descobrir
    quais apagar — exatamente o que a regra de memoria do CONTEXTO 3.7 proibe
    numa instancia de 2 GB. Aqui o filtro e feito pelo banco. O `~` testa o
    formato antes de converter, senao uma data mal preenchida derruba a query."""
    cur = conn.execute(
        "DELETE FROM movimentos "
        " WHERE ddtpagamento ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' "
        "   AND to_date(ddtpagamento, 'DD/MM/YYYY') BETWEEN ? AND ?",
        (ini, fim))
    apagados = cur.rowcount or 0
    cur.close()
    conn.commit()
    return apagados


def sync_incremental(env=".env", margem_dias=2, com_catalogos=True):
    cli = OmieClient.de_ambiente(env)
    conn = conectar()
    try:
        hoje = dt.date.today()
        hoje_str = hoje.strftime("%d/%m/%Y")

        # ---- Titulos por dAlt (filtro server-side + guarda client-side) ----
        for entidade, natureza, metodo in (
            ("contapagar", "P", cli.listar_contas_pagar),
            ("contareceber", "R", cli.listar_contas_receber),
        ):
            ultima_dalt, _ = _ler_sync_state(conn, entidade)
            if not ultima_dalt:
                log.warning("%s sem ultima_dalt — rode --carga-inicial antes do incremental.", entidade)
                continue
            cutoff = _dalt_para_data(ultima_dalt)
            data_de = cutoff - dt.timedelta(days=margem_dias)
            data_de_str = data_de.strftime("%d/%m/%Y")
            # NOTA: nomes dos params de filtro por alteracao a confirmar no portal Omie.
            # Se nao forem respeitados, a guarda client-side garante correcao (so mais paginas).
            param = {"filtrar_por_data_de": data_de_str, "filtrar_por_data_ate": hoje_str,
                     "filtrar_apenas_alteracao": "S"}
            log.info("=== Incremental %s: alterados desde %s ===", entidade, data_de_str)
            tot = rat = ign = 0
            maior = cutoff
            t0 = time.time()
            for pagina, total_paginas, total_registros, registros in metodo(param_extra=param):
                qt, qr, qi, mx, probs = aplicar_incremental_titulos(conn, natureza, registros, data_de)
                tot += qt; rat += qr; ign += qi
                if mx and (maior is None or mx > maior):
                    maior = mx
                for cod, motivo in probs:
                    log.warning("  [%s] rateio: %s", cod, motivo)
                if pagina % 20 == 0 or pagina == total_paginas:
                    log.info("  pag %d/%d | aplicados=%d ignorados=%d (%.0fs)",
                             pagina, total_paginas, tot, ign, time.time() - t0)
            conn.execute(
                "UPDATE sync_state SET ultima_dalt=?, ultima_sync=?, total_registros="
                "(SELECT COUNT(*) FROM titulos WHERE natureza=?) WHERE entidade=?",
                (maior.strftime("%d/%m/%Y") if maior else ultima_dalt,
                 dt.datetime.now().isoformat(timespec="seconds"), natureza, entidade))
            conn.commit()
            log.info("OK %s -> %d aplicados, %d ignorados, ultima_dalt=%s.",
                     entidade, tot, ign, maior.strftime("%d/%m/%Y") if maior else ultima_dalt)

        # ---- Movimentos por janela de data de pagamento (delete+insert idempotente) ----
        _, ult_sync_mov = _ler_sync_state(conn, "movimentos")
        inicio = hoje - dt.timedelta(days=margem_dias)
        if ult_sync_mov:
            try:
                base = dt.datetime.fromisoformat(ult_sync_mov).date() - dt.timedelta(days=margem_dias)
                inicio = min(inicio, base)
            except Exception:
                pass
        ini_str = inicio.strftime("%d/%m/%Y")
        log.info("=== Incremental movimentos: janela %s a %s ===", ini_str, hoje_str)
        apagados = _apagar_movimentos_janela(conn, inicio, hoje)
        # ListarMovimentos filtra por intervalo de data de PAGAMENTO via dDtPagtoDe/dDtPagtoAte.
        param_mov = {"dDtPagtoDe": ini_str, "dDtPagtoAte": hoje_str}
        tot_mov = 0
        for pagina, total_paginas, total_registros, registros in cli.listar_movimentos(param_extra=param_mov):
            qm, _ = gravar_movimentos(conn, registros)
            tot_mov += qm
        conn.execute("UPDATE sync_state SET ultima_sync=?, total_registros="
                     "(SELECT COUNT(*) FROM movimentos) WHERE entidade='movimentos'",
                     (dt.datetime.now().isoformat(timespec="seconds"),))
        conn.commit()
        log.info("OK movimentos -> janela apagou %d, reinseriu %d.", apagados, tot_mov)

        # ---- Catalogos (baratos; upsert idempotente) ----
        if com_catalogos:
            tot = 0
            for _, _, _, registros in cli.listar_categorias():
                tot += gravar_categorias(conn, registros)
            log.info("categorias atualizadas: %d", tot)
            tot = 0
            for _, _, _, registros in cli.listar_clientes():
                tot += gravar_clientes(conn, registros)
            log.info("clientes/fornecedores atualizados: %d", tot)
            sincronizar_contas_correntes(conn, cli)
            try:
                from . import projetos
                n = projetos.sincronizar(conn)
                log.info("depto_projeto atualizado: %d", n)
            except Exception as e:
                conn.rollback()   # ver comentario em sincronizar_planilha
                log.warning("depto->projeto nao atualizado (%s).", e)

        _resumo(conn)
    finally:
        conn.close()


def _ckpt_path():
    """Onde a varredura de exclusoes anota por onde passou, para poder retomar.

    Fica na pasta temporaria. No Render essa pasta e apagada a cada reinicio, e
    tudo bem: perder o checkpoint so faz a varredura recomecar do zero, nunca
    perde dado — o dado esta no banco."""
    h = hashlib.md5(b"painel-omie").hexdigest()[:10]
    return os.path.join(tempfile.gettempdir(), f"reconcile_omie_{h}.json")


def _ckpt_carregar(caminho, caminho_legado=None):
    for c in (caminho, caminho_legado):
        if not c:
            continue
        try:
            with open(c, "r", encoding="utf-8") as f:
                d = json.load(f)
            return {"ids": set(int(x) for x in d.get("ids", [])),
                    "done": list(d.get("done", [])),
                    "atual": d.get("atual"),
                    "pagina": int(d.get("pagina", 0))}
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            continue
    return None


def _ckpt_salvar(caminho, ids, done, atual, pagina):
    """Salva o checkpoint. BEST EFFORT: se falhar (lock/permissao), apenas avisa
    e segue — nunca derruba o reconcile por causa de um save."""
    payload = {"ids": list(ids), "done": done, "atual": atual, "pagina": pagina,
               "ts": dt.datetime.now().isoformat(timespec="seconds")}
    for tentativa in range(4):
        try:
            tmp = f"{caminho}.{os.getpid()}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            os.replace(tmp, caminho)  # troca atomica
            return True
        except (PermissionError, OSError) as e:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass
            if tentativa < 3:
                time.sleep(1.0)
            else:
                log.warning("Nao consegui salvar o checkpoint (%s). Sigo sem ele "
                            "(no pior caso, recomeca a varredura).", e)
    return False


def _ckpt_remover(*caminhos):
    for c in caminhos:
        try:
            os.remove(c)
        except (FileNotFoundError, OSError):
            pass


def reconcile(env=".env"):
    """
    Varre TODOS os ids de titulos no Omie e remove do espelho os que sumiram
    (exclusoes nao vem pelo incremental). Pesado — rodar semanal.

    RETOMAVEL: salva os ids coletados + a ultima pagina concluida num checkpoint
    (na pasta TEMP do Windows, fora do Dropbox) a cada 50 paginas. Se cair
    (internet/erro), rodar de novo RETOMA de onde parou. O save e best-effort:
    se nao conseguir gravar, ele avisa e continua a varredura mesmo assim.
    """
    SALVAR_CADA = 50
    cli = OmieClient.de_ambiente(env)
    conn = conectar()
    ckpt_file = _ckpt_path()
    ckpt_legado = None
    try:
        ck = _ckpt_carregar(ckpt_file, ckpt_legado)
        if ck:
            ids_omie = ck["ids"]
            done = ck["done"]
            log.info("Checkpoint encontrado: %s ids ja coletados, entidades concluidas=%s, "
                     "retomando '%s' da pagina %d.",
                     f"{len(ids_omie):,}".replace(",", "."), done, ck["atual"], ck["pagina"] + 1)
        else:
            ids_omie = set()
            done = []
            ck = {"atual": None, "pagina": 0}

        entidades = (("contapagar", cli.listar_contas_pagar),
                     ("contareceber", cli.listar_contas_receber))
        for entidade, metodo in entidades:
            if entidade in done:
                continue
            pag_inicial = ck["pagina"] + 1 if ck.get("atual") == entidade and ck["pagina"] else 1
            log.info("=== Reconcile: varrendo ids de %s (a partir da pag %d) ===", entidade, pag_inicial)
            for pagina, total_paginas, total_registros, registros in metodo(pagina_inicial=pag_inicial):
                for r in registros:
                    c = r.get("codigo_lancamento_omie")
                    if c not in (None, ""):
                        ids_omie.add(int(c))
                if pagina % SALVAR_CADA == 0 or pagina == total_paginas:
                    log.info("  %s pag %d/%d", entidade, pagina, total_paginas)
                    _ckpt_salvar(ckpt_file, ids_omie, done, entidade, pagina)
            done.append(entidade)
            _ckpt_salvar(ckpt_file, ids_omie, done, None, 0)

        ids_espelho = {r[0] for r in conn.execute("SELECT codigo_lancamento_omie FROM titulos")}
        excluidos = ids_espelho - ids_omie
        if excluidos:
            params = [(c,) for c in excluidos]
            conn.executemany("DELETE FROM titulos WHERE codigo_lancamento_omie=?", params)
            conn.executemany("DELETE FROM rateio WHERE codigo_lancamento_omie=?", params)
            conn.executemany("DELETE FROM movimentos WHERE ncodtitulo=?", params)
            conn.commit()
        log.info("Reconcile: %d no Omie, %d no espelho, %d excluido(s) removido(s).",
                 len(ids_omie), len(ids_espelho), len(excluidos))
        _ckpt_remover(ckpt_file, ckpt_legado)  # concluiu -> limpa os dois locais
        return len(excluidos)
    finally:
        conn.close()
