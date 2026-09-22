# -*- coding: utf-8 -*-
"""
Monta a tabela `fato` — o que as telas leem.

Um titulo do OMIE vira N linhas aqui: uma por obra a que ele foi apropriado
(rateio), mais uma linha separada de imposto retido quando ha. E a mesma regra
de negocio que rodava no PC gerando o arquivo dados_omie.parquet; o que mudou e
o destino e a forma de percorrer.

POR QUE MUDOU. O arquivo tinha 4 MB em disco e 179 MB quando aberto na memoria.
A instancia do Render tem 2 GB divididos com 14 modulos e ja teve crise de
memoria (CONTEXTO secao 9). Abrir isso a cada tela derrubaria o servico inteiro.
Agora as linhas vao para o banco em blocos, e as telas pedem ao Postgres so a
soma que vao mostrar.

Regras de negocio (inalteradas):
  - Tipo: REC = "1. Contas a Receber" / PAG = "2. Contas a Pagar".
  - Sinal: receitas POSITIVAS, despesas NEGATIVAS.
  - Pago = Situacao contem "Pago|Recebido|Conciliado".
  - Cada recebivel explode em linha LIQUIDA + linha(s) RETIDO.
    Bruto = Liquido + Retido.
  - Rateio: explode por departamento pelo valor apropriado. O que nao estiver
    apropriado vira "(nao apropriado)", para o erro aparecer na tela em vez de
    ser mascarado.
  - Data: data real de pagamento se quitado; senao vencimento.
  - CANCELADO e descartado.
"""
import re
import logging
import datetime as dt

log = logging.getLogger("painel.fato")

VERSAO = "2026-09-02.1"

# Quantos titulos processar por vez. Cada bloco busca seus proprios movimentos e
# rateios, monta as linhas e grava — o pico de memoria fica na casa dos poucos MB
# em vez dos 179 MB de antes, independentemente do tamanho da base.
TAMANHO_BLOCO = 4000

REC, PAG = "1. Contas a Receber", "2. Contas a Pagar"
NAO_APROP = "(não apropriado)"
TOL = 0.10

# Nomes das colunas como as telas antigas as chamavam. Ficam aqui porque a
# construcao dos recebimentos ainda fala nessa lingua; a tabela `fato` usa os
# nomes de banco, em COLUNAS_FATO.
COLUNAS_LOG = [
    "Tipo", "Análise", "Situação", "SituacaoVencimento", "Categoria", "Grupo",
    "Projeto", "Departamento", "Cliente ou Fornecedor (Razão Social)", "CNPJ/CPF",
    "Número do Documento", "Pedido de Compra", "Conta Corrente", "Observação da Conta",
    "Link", "Data", "PagoRecebido", "APagarReceber", "Ano", "Mes",
    "Juros", "Multa",
]

# Heuristico para a "Análise" quando nem o OMIE nem o de-para respondem:
# categorias financeiras/nao-operacionais caem em "Fluxo de Caixa"; o resto em "DRE".
_PALAVRAS_FLUXO = ("aporte", "retirada", "sócio", "socio", "emprést", "emprest",
                   "financiamento", "transfer", "dividendo", "mútuo", "mutuo",
                   "capital", "juros sobre capital")

CATEGORIA_RETIDO = "Impostos Retidos na Fonte"
GRUPO_RETIDO = "Retenções"


# ----------------------------------------------------------------------------- 
# Helpers
# ----------------------------------------------------------------------------- 
def _data_para_dt(s):
    if not s:
        return None
    try:
        d, m, a = str(s).split("/")
        return dt.date(int(a), int(m), int(d))
    except Exception:
        return None


def pipefy_link(doc):
    m = re.fullmatch(r"[A-Z]{2}(\d+)", str(doc).strip().upper())
    return f"https://app.pipefy.com/open-cards/{m.group(1)}" if m else ""


def _analise_por_heuristica(descricao_categoria, grupo):
    txt = f"{descricao_categoria} {grupo}".lower()
    return "Fluxo de Caixa" if any(p in txt for p in _PALAVRAS_FLUXO) else "DRE"


def carregar_de_para(conn):
    """De-para Categoria -> (Análise, Grupo), lido da tabela categoria_de_para.

    Antes isso vinha de um arquivo de 14 MB (dados_log.parquet), a exportacao
    antiga da planilha Log, cuja unica funcao era ensinar essa correspondencia.
    Agora e uma tabela de algumas centenas de linhas. Vale so como rede de
    seguranca: quando o proprio OMIE informa a conta do DRE, ele manda."""
    mapa = {}
    cur = conn.execute("SELECT categoria, analise, grupo FROM categoria_de_para")
    for categoria, analise, grupo in cur.fetchall():
        info = {}
        if (analise or "").strip():
            info["Análise"] = analise.strip()
        if (grupo or "").strip():
            info["Grupo"] = grupo.strip()
        if info:
            mapa[(categoria or "").strip()] = info
    cur.close()
    return mapa


# ----------------------------------------------------------------------------- 
# Carrega catalogos e auxiliares do espelho
# ----------------------------------------------------------------------------- 
def carregar_catalogos(conn):
    """Catalogos pequenos que cabem na memoria sem susto: categorias (172),
    clientes (7 mil), obras (154) e contas correntes (60).

    O que NAO entra aqui, de proposito, sao movimentos (240 mil) e rateios (184
    mil): esses sao buscados bloco a bloco, junto com os titulos que os usam."""
    cat = {}
    cur = conn.execute("SELECT codigo, descricao, grupo, codigo_dre, "
                       "transferencia, descricao_dre FROM cat")
    linhas_cat = cur.fetchall()
    cur.close()
    # Analise/Grupo derivados do proprio OMIE quando disponiveis:
    #   transferencia=S -> TRF ; codigo_dre preenchido -> DRE ; senao -> Fluxo de Caixa.
    # Se a categoria ainda nao foi recarregada do OMIE (codigo_dre e transferencia
    # ambos vazios), analise fica None e o chamador cai no de-para/heuristica.
    for cod, desc, grupo, codigo_dre, transf, desc_dre in linhas_cat:
        if codigo_dre is None and transf is None:
            analise_omie = None
            grupo_final = grupo or desc or ""
        else:
            if str(transf or "").upper() == "S":
                analise_omie = "TRF"
            elif str(codigo_dre or "").strip():
                analise_omie = "DRE"
            else:
                analise_omie = "Fluxo de Caixa"
            grupo_final = (desc_dre or "").strip() if (analise_omie == "DRE" and desc_dre)                 else (grupo or desc or "")
        cat[str(cod)] = (desc or "", grupo_final, analise_omie)

    cli = {}
    cur = conn.execute("SELECT codigo, razao_social, cnpj_cpf FROM clientes")
    for cod, razao, cnpj in cur.fetchall():
        cli[cod] = (razao or "", cnpj or "")
    cur.close()

    cur = conn.execute("SELECT ccoddep, projeto FROM depto_projeto")
    proj = dict(cur.fetchall())
    cur.close()

    cur = conn.execute("SELECT codigo, descricao FROM contas_correntes")
    ccorr = {cod: (desc or "").strip() for cod, desc in cur.fetchall() if (desc or "").strip()}
    cur.close()

    return cat, cli, proj, ccorr


# --------------------------------------------------------------------------- 
# Leitura por blocos: movimentos e rateios so dos titulos do bloco atual
# --------------------------------------------------------------------------- 
def _movimentos_do_bloco(conn, codigos):
    """Agrega os movimentos dos titulos deste bloco. Mesma regra de antes:
      - data_pagamento = MAIOR data entre os movimentos liquidados
      - so movimentos com cLiquidado='S' contam pago/desconto/juros/multa
        (os nao liquidados sao perna de conta corrente e dobrariam o caixa)
      - o saldo aberto soma todos.
      - conta = a conta da BAIXA, nao a da previsao (ver abaixo).

    A CONTA DA BAIXA, e por que ela importa. 21/09/2026, o dono:

        "No OMIE existe a conta de previsao de pagamento e existe a conta onde
        efetivamente foi realizado o pagamento. A informacao que esta sendo
        colocada nesse relatorio analitico e exatamente a primeira. E a primeira
        e errada."

    Ele esta certo, e o erro era silencioso: a coluna saia do titulo
    (`id_conta_corrente`), que e onde se PREVIU pagar. Quem paga por outra conta
    — o que acontece o tempo todo — aparecia no relatorio na conta errada, e
    qualquer analise por conta mentia sem dar sinal alguma.

    Quando ha mais de uma baixa, vale a do MAIOR valor liquidado; empate, a mais
    recente. Nao existe resposta certa para um titulo pago metade em cada conta;
    esta e a que erra menos, e a linha do relatorio e uma so.

    Devolve {codigo: (data, pago, aberto, desconto, liquidado, juros, multa,
                      conta_da_baixa)}."""
    if not codigos:
        return {}
    marcas = ",".join(["?"] * len(codigos))
    cur = conn.execute(
        # ::float8 nao e enfeite: desde a migracao 010 estas colunas sao
        # NUMERIC — o banco devolveria Decimal, que nao soma com float e
        # quebraria a conta aqui. O banco guarda exato; o transporte usa
        # float de 8 bytes, que tem digitos de sobra para centavo.
        "SELECT ncodtitulo, ddtpagamento, cliquidado, nvalpago::float8, "
        "       nvalaberto::float8, ndesconto::float8, njuros::float8, "
        "       nmulta::float8, ncodcc "
        "  FROM movimentos WHERE ncodtitulo IN (" + marcas + ")", codigos)
    agg = {}
    for cod, dpg, liq, vpg, vab, vdesc, vjur, vmul, ncc in cur.fetchall():
        a = agg.setdefault(cod, {"dpg": None, "pago": 0.0, "aberto": 0.0, "desc": 0.0,
                                 "juros": 0.0, "multa": 0.0, "liq": "N",
                                 "conta": None, "peso": -1.0, "quando": None})
        a["aberto"] += (vab or 0.0)
        if liq == "S":
            a["liq"] = "S"
            a["pago"] += (vpg or 0.0)
            a["desc"] += (vdesc or 0.0)
            a["juros"] += (vjur or 0.0)
            a["multa"] += (vmul or 0.0)
            d = _data_para_dt(dpg)
            if d and (a["dpg"] is None or d > a["dpg"]):
                a["dpg"] = d
            # a conta da baixa: maior valor liquidado; empate, a mais recente
            if ncc not in (None, ""):
                peso = abs(vpg or 0.0)
                melhor = (peso > a["peso"]
                          or (peso == a["peso"] and d and a["quando"]
                              and d > a["quando"])
                          or (peso == a["peso"] and a["conta"] is None))
                if melhor:
                    a["conta"], a["peso"], a["quando"] = ncc, peso, d
    cur.close()
    return {cod: (a["dpg"].strftime("%d/%m/%Y") if a["dpg"] else None,
                  round(a["pago"], 2), round(a["aberto"], 2), round(a["desc"], 2),
                  a["liq"], round(a["juros"], 2), round(a["multa"], 2),
                  a["conta"])
            for cod, a in agg.items()}


def _rateio_do_bloco(conn, codigos):
    """{codigo: [(ccoddep, cdesdep, nperdep, nvaldep), ...]} dos titulos do bloco."""
    if not codigos:
        return {}
    marcas = ",".join(["?"] * len(codigos))
    cur = conn.execute(
        "SELECT codigo_lancamento_omie, ccoddep, cdesdep, nperdep, nvaldep::float8 "
        "  FROM rateio WHERE codigo_lancamento_omie IN (" + marcas + ") ORDER BY seq",
        codigos)
    rateio = {}
    for cod, ccod, cdes, nper, nval in cur.fetchall():
        rateio.setdefault(cod, []).append((ccod or "", cdes or "", nper or 0.0, nval or 0.0))
    cur.close()
    return rateio


def _movimentos_detalhe_do_bloco(conn, codigos):
    """Movimentos em detalhe (sem agregar) dos titulos do bloco — base da Receita
    Analitico. O OMIE guarda o mesmo recebimento em varias pernas; quem escolhe
    qual vale e `_escolher_recebimentos`."""
    if not codigos:
        return {}
    marcas = ",".join(["?"] * len(codigos))
    cur = conn.execute(
        "SELECT ncodtitulo, ddtpagamento, nvalpago::float8, nvalliquido::float8, "
        "       njuros::float8, nmulta::float8, ndesconto::float8, ncodcc, cstatus, "
        "       cliquidado, cgrupo, nvalaberto::float8 "
        "  FROM movimentos WHERE ncodtitulo IN (" + marcas + ")", codigos)
    res = {}
    for (cod, dpg, vpg, vliq, vjur, vmul, vdesc, ncc, cst, liq, grp, vab) in cur.fetchall():
        res.setdefault(cod, []).append({
            "data": dpg or "", "valor": float(vpg or 0.0), "liquido": float(vliq or 0.0),
            "aberto": float(vab or 0.0), "juros": float(vjur or 0.0),
            "multa": float(vmul or 0.0), "desconto": float(vdesc or 0.0),
            "conta": ncc, "status": (cst or "").strip(), "grupo": (grp or "").strip(),
            "liquidado": (liq or "").strip(),
        })
    cur.close()
    for lst in res.values():
        lst.sort(key=lambda m: (_data_para_dt(m["data"]) or dt.date(1900, 1, 1)))
    return res


def _blocos_de_titulos(conn, sql_extra="", params=()):
    """Percorre os titulos em blocos de TAMANHO_BLOCO, sempre na mesma ordem.

    Usa cursor do lado do servidor: as 120 mil linhas ficam no Postgres e vem em
    blocos. Por isso esta conexao NAO pode receber commit durante a varredura —
    quem grava usa outra conexao."""
    cur = conn.executar_em_stream(
        "SELECT codigo_lancamento_omie, natureza, valor_documento::float8, "
        "codigo_categoria, "
        "codigo_cliente_fornecedor, id_conta_corrente, numero_documento, numero_pedido, "
        "status_titulo, data_vencimento, valor_ir::float8, valor_iss::float8, "
        "valor_inss::float8, valor_pis::float8, "
        "valor_cofins::float8, valor_csll::float8, observacao FROM titulos " + sql_extra +
        " ORDER BY codigo_lancamento_omie", params, por_vez=TAMANHO_BLOCO)
    try:
        while True:
            bloco = cur.fetchmany(TAMANHO_BLOCO)
            if not bloco:
                break
            yield bloco
    finally:
        cur.close()


# ----------------------------------------------------------------------------- 
# Explode rateio em buckets (com "(nao apropriado)" no que faltar)
# ----------------------------------------------------------------------------- 
def _buckets_rateio(linhas, bruto, proj_map):
    """
    Recebe as linhas de rateio do titulo e o bruto. Retorna lista de buckets:
    (departamento, projeto, fracao) cuja soma de fracoes = 1.0.
    """
    if bruto is None or abs(bruto) <= TOL:
        bruto = bruto or 0.0
    soma = sum(l[3] for l in linhas)
    buckets = []
    if not linhas:
        buckets.append((NAO_APROP, NAO_APROP, 1.0))
        return buckets
    # over-allocation: escala para caber no bruto
    escala = 1.0
    if soma - bruto > TOL and soma > 0:
        escala = bruto / soma
    base = abs(bruto) if abs(bruto) > TOL else 1.0
    for ccod, cdes, _nper, nval in linhas:
        valor = nval * escala
        dep = cdes or ccod or NAO_APROP
        proj = proj_map.get(ccod, "")
        buckets.append((dep, proj, valor / base))
    # under-allocation: o que faltou vira "(nao apropriado)"
    falta = bruto - sum(l[3] for l in linhas) * escala
    if falta > TOL:
        buckets.append((NAO_APROP, NAO_APROP, falta / base))
    return buckets


def _nome_da_conta(ccorr, codigo, reserva=None):
    """Nome da conta corrente a partir do codigo. Sem catalogo, o codigo cru —
    a linha nunca perde a informacao."""
    escolhido = codigo if codigo not in (None, "") else reserva
    if escolhido in (None, ""):
        return ""
    try:
        return ccorr.get(int(escolhido)) or str(escolhido)
    except (TypeError, ValueError):
        return str(escolhido)


def _conta_de_onde_saiu(movs_todos, realizado, reserva):
    """A conta por onde o dinheiro DE FATO andou — e nao a que o titulo diz.

    O OMIE guarda a mesma baixa em duas pernas (ver `_escolher_recebimentos`):
    a CONSOLIDADA, que e o resumo do titulo e carrega a conta DO TITULO — a da
    previsao —, e os creditos/debitos BANCARIOS, um por conta que o dinheiro
    tocou. So a segunda sabe por onde o dinheiro saiu.

    22/09/2026: o conserto do dia anterior trocou a FONTE (do titulo para o
    movimento) mas continuou lendo a perna consolidada — e a tela nao mudou. O
    dono refez os numeros e "o problema das contas permaneceu". O erro era
    escolher a perna, nao a tabela.

    Entre as pernas escolhidas, vale a de maior valor; empate, a mais recente.
    Sem perna com conta, fica a reserva (a consolidada, e depois a previsao):
    a linha nunca perde a informacao."""
    if not movs_todos:
        return reserva
    escolhidos, _origem = _escolher_recebimentos(movs_todos, realizado)
    melhor, peso, quando = None, -1.0, None
    for m in escolhidos:
        conta = m.get("conta")
        if conta in (None, ""):
            continue
        valor = abs(m.get("valor") or 0.0)
        d = _data_para_dt(m.get("data"))
        if valor > peso or (valor == peso and d and quando and d > quando):
            melhor, peso, quando = conta, valor, d
    return melhor if melhor not in (None, "") else reserva


def _parcelas_da_baixa(movs_todos, realizado, juros_total, multa_total,
                       ccorr, icc, conta_unica, ddt, ano, mes, dpago_dt):
    """UMA LINHA POR BAIXA, quando o titulo foi pago em mais de uma.

    21/09/2026, o dono: um titulo pago em dois dias e valores diferentes
    aparecia no relatorio analitico como UM lancamento, com o valor TOTAL e a
    data da ULTIMA parcela.

        "Se voce for olhar no extrato, da uma coisa. Ai voce olha no relatorio
        analitico, da outro valor. Isso confunde."

    O painel ja fazia certo do lado das RECEITAS (`montar_recebimentos`): uma
    medicao recebida em tres parcelas vira tres linhas. Nas despesas esse
    caminho nunca tinha sido ligado. Aqui ele passa a valer para as duas — e
    reusa o mesmo `_escolher_recebimentos`, que e quem sabe desmontar a
    armadilha do OMIE de guardar a mesma baixa em duas pernas. Regra repetida
    divergiria; reusada, nao.

    A SOMA NAO MUDA: cada parcela e escalada por realizado/soma_das_baixas,
    entao o total do titulo continua o mesmo. O que muda e a DATA de cada
    pedaco — e e por isso que um titulo pago metade em marco e metade em abril
    deixa de contar inteiro em abril. Isso e o conserto, nao um efeito colateral.

    Devolve [(valor, juros, multa, ddt, ano, mes, dpago_dt, conta)], sempre com
    pelo menos um item. Baixa unica (a esmagadora maioria) devolve exatamente a
    linha de antes — nao ha razao para mexer no que ja estava certo."""
    uma_so = [(realizado, juros_total, multa_total, ddt, ano, mes, dpago_dt,
               conta_unica)]
    if realizado <= TOL or not movs_todos:
        return uma_so

    movs, _origem = _escolher_recebimentos(movs_todos, realizado)
    if len(movs) < 2:
        return uma_so
    soma = sum(m["valor"] for m in movs)
    if soma <= TOL:
        return uma_so

    fator = realizado / soma
    parcelas = []
    for m in movs:
        d = _data_para_dt(m.get("data"))
        # Juros e multa vao pela MESMA proporcao do principal, e nao pelo que
        # cada movimento carrega: as pernas que o OMIE usa para os creditos
        # bancarios vem com encargo zerado, e usar o proprio faria o total do
        # titulo encolher sem ninguem notar. A soma tem de continuar a mesma.
        peso = m["valor"] / soma
        parcelas.append((
            m["valor"] * fator,
            juros_total * peso,
            multa_total * peso,
            d or ddt,
            d.year if d else ano,
            d.month if d else mes,
            d or dpago_dt,
            _nome_da_conta(ccorr, m.get("conta"), icc),
        ))
    return parcelas


# ----------------------------------------------------------------------------- 
# Monta o DataFrame final
# ----------------------------------------------------------------------------- 
COLUNAS_FATO = (
    "codigo_lancamento", "tipo", "analise", "situacao", "situacao_vencimento",
    # `categoria` e a DESCRICAO, que a tela mostra; `codigo_categoria` e o
    # codigo do OMIE, que a tela de saneamento precisa para ALTERAR o titulo la.
    "categoria", "codigo_categoria", "grupo", "projeto", "departamento",
    "razao_social", "cnpj_cpf",
    "numero_documento", "pedido_compra", "conta_corrente", "observacao", "link",
    "medicao", "medicao_rotulo",
    # `data` e a das telas: pagamento quando quitado, senao vencimento. As duas
    # abaixo sao as datas CRUAS, cada uma a sua — e a diferenca entre elas e o
    # atraso, que nao existia em lugar nenhum do painel.
    "data", "ano", "mes", "data_vencimento", "data_pagamento",
    "pago_recebido", "a_pagar_receber", "juros", "multa",
)


def gerar_linhas_fato(conn):
    """Percorre os titulos em blocos e entrega as linhas do fato, uma a uma.

    E um gerador de proposito: quem chama grava o bloco e segue. Nada da base
    inteira fica na memoria — a versao anterior montava um DataFrame de 185 mil
    linhas antes de gravar qualquer coisa."""
    cat, cli, proj_map, ccorr = carregar_catalogos(conn)
    mapa_log = carregar_de_para(conn)
    hoje = dt.date.today()

    for bloco in _blocos_de_titulos(conn):
        codigos = [linha[0] for linha in bloco]
        mov = _movimentos_do_bloco(conn, codigos)
        rateio = _rateio_do_bloco(conn, codigos)
        # O detalhe de cada baixa — e o que permite abrir o titulo pago em
        # parcelas numa linha por parcela. Ver `_parcelas_da_baixa`.
        detalhe = _movimentos_detalhe_do_bloco(conn, codigos)

        for row in bloco:
            (cod, nat, vdoc, ccat, ccli, icc, ndoc, nped, status, dvenc,
             vir, viss, vinss, vpis, vcofins, vcsll, obs) = row
            status = (status or "").strip()
            if status.upper() == "CANCELADO":
                continue
            bruto = float(vdoc or 0.0)
            ret_total = float((vir or 0) + (viss or 0) + (vinss or 0) +
                              (vpis or 0) + (vcofins or 0) + (vcsll or 0))
            liquido = bruto - ret_total
            is_rec = (nat == "R")
            sinal = 1.0 if is_rec else -1.0
            tipo = REC if is_rec else PAG

            (dpg, pago_mov, aberto_mov, desc_mov, liq_mov, juros_mov,
             multa_mov, conta_da_baixa) = mov.get(
                cod, (None, 0.0, 0.0, 0.0, "N", 0.0, 0.0, None))
            quitado = bool(re.search(r"pago|recebido|conciliado", status, re.I)) or liq_mov == "S"

            # realizado x aberto (parte LIQUIDA).
            # quitado_bruto = quanto do titulo ja foi liquidado, limitado ao liquido.
            # O desconto faz parte do que foi quitado mas NAO entra no caixa (ex.:
            # devolucao 100% descontada -> quitada, mas caixa = 0).
            # aberto = liquido - quitado_bruto (saldo REAL; nao usamos o nValAberto
            # dos movimentos porque ha movimentos "fantasma" com saldo que nao existe).
            if quitado:
                base_real = pago_mov if pago_mov > 0 else liquido
                quitado_bruto = min(base_real, liquido) if liquido >= 0 else base_real
                realizado = max(quitado_bruto - desc_mov, 0.0)
                aberto = max(liquido - quitado_bruto, 0.0)
            else:
                realizado = 0.0
                aberto = liquido

            # data: pagamento se quitado, senao vencimento
            data = dpg if (quitado and dpg) else dvenc
            ddt = _data_para_dt(data)
            ano = ddt.year if ddt else None
            mes = ddt.month if ddt else None
            # e as duas separadas, cada uma a sua. A de pagamento so existe se o
            # titulo foi mesmo quitado: gravar a data de um movimento de titulo
            # em aberto faria parecer pago o que nao foi.
            dvenc_dt = _data_para_dt(dvenc)
            dpago_dt = _data_para_dt(dpg) if quitado else None

            if quitado:
                sit_venc = "Quitado"
            else:
                dv = _data_para_dt(dvenc)
                sit_venc = "Vencido" if (dv and dv < hoje) else "A vencer"

            # categoria / grupo / analise — do OMIE quando disponivel; senao de-para
            desc_cat, grupo, analise = cat.get(str(ccat), (str(ccat or ""), str(ccat or ""), None))
            if not analise:
                info_log = mapa_log.get(desc_cat.strip(), {})
                grupo = info_log.get("Grupo", grupo)
                analise = info_log.get("Análise") or _analise_por_heuristica(desc_cat, grupo)

            # Fornecedor/cliente: NOME vindo do cadastro do OMIE. Se o catalogo
            # ainda nao tem esse codigo, o nome vinha VAZIO — e uma linha sem nome
            # e invisivel para quem procura pelo nome da empresa, que e como as
            # pessoas procuram. O dono perdeu meia hora com isso em 13/09/2026:
            # buscou as devolucoes de aporte de uma empresa pelo nome, achou
            # poucas, e as que faltavam estavam la o tempo todo, sem nome.
            #
            # Mesmo cuidado que a conta corrente logo abaixo ja tinha: sem
            # cadastro, cai para o codigo cru. A linha nunca perde a informacao.
            razao, cnpj = cli.get(ccli, ("", ""))
            if not (razao or "").strip() and ccli not in (None, ""):
                razao = f"(fornecedor {ccli})"
            link = pipefy_link(ndoc)
            # Conta corrente: A CONTA DA BAIXA, nao a da previsao.
            #
            # 21/09/2026, o dono: "existe a conta de previsao de pagamento e
            # existe a conta onde efetivamente foi realizado o pagamento. A
            # informacao que esta sendo colocada nesse relatorio analitico e
            # exatamente a primeira. E a primeira e errada."
            #
            # O titulo carrega a conta onde se PREVIU pagar
            # (`id_conta_corrente`); quem paga por outra conta aparecia no
            # relatorio na conta errada, calado. Titulo ainda EM ABERTO nao tem
            # baixa — ai a previsao e a unica informacao que existe, e continua
            # valendo.
            # E a conta vem da perna BANCARIA da baixa, nao da consolidada: a
            # consolidada repete a conta do titulo. Ver `_conta_de_onde_saiu`.
            reserva = conta_da_baixa if conta_da_baixa not in (None, "") else icc
            codigo_conta = _conta_de_onde_saiu(detalhe.get(cod, []), realizado, reserva)
            if codigo_conta in (None, ""):
                conta = ""
            else:
                # Se o catalogo ainda nao tem esse codigo (conta criada depois
                # do ultimo sync), cai para o codigo cru — a linha nunca perde a
                # informacao.
                try:
                    conta = ccorr.get(int(codigo_conta)) or str(codigo_conta)
                except (TypeError, ValueError):
                    conta = str(codigo_conta)
            observacao = (obs or "").strip()

            buckets = _buckets_rateio(rateio.get(cod, []), bruto, proj_map)

            # Chave da medicao: e o que junta as parcelas de uma mesma medicao,
            # que no OMIE sao titulos separados sem numero de documento. Guardada
            # na linha para a tela poder agrupar no banco.
            chave = chave_medicao(ndoc or "", observacao) or f"COD:{cod}"

            # UMA LINHA POR BAIXA. Pago de uma vez (a esmagadora maioria)
            # devolve exatamente a linha de antes.
            parcelas = _parcelas_da_baixa(
                detalhe.get(cod, []), realizado, juros_mov, multa_mov,
                ccorr, icc, conta, ddt, ano, mes, dpago_dt)
            rotulo = rotulo_medicao(chave)

            for dep, projeto, frac in buckets:
                comum = (cod, tipo, analise, status, sit_venc)
                primeira = None
                for i, parcela in enumerate(parcelas):
                    (p_valor, p_juros, p_multa, p_ddt, p_ano, p_mes,
                     p_dpago, p_conta) = parcela
                    identificacao = (projeto, dep, razao, cnpj, ndoc or "",
                                     nped or "", p_conta, observacao, link,
                                     chave, rotulo,
                                     p_ddt, p_ano, p_mes, dvenc_dt, p_dpago)
                    if primeira is None:
                        primeira = identificacao
                    # O SALDO EM ABERTO E DO TITULO, nao de cada parcela.
                    # Repeti-lo multiplicaria o "a pagar" pelo numero de baixas
                    # — um erro que cresceria com o uso, silencioso.
                    em_aberto = aberto if i == 0 else 0.0
                    # linha LIQUIDA (categoria real). Juros e multa sao os
                    # encargos efetivamente pagos e ficam SEPARADOS do
                    # principal, para virarem linha financeira no DRE.
                    yield comum + (desc_cat, ccat, grupo) + identificacao + (
                        round(sinal * p_valor * frac, 2),
                        round(sinal * em_aberto * frac, 2),
                        round(sinal * p_juros * frac, 2),
                        round(sinal * p_multa * frac, 2))
                # linha RETIDO (so a receber; valor sempre como realizado).
                # UMA so, mesmo com varias baixas: a retencao e do titulo.
                if is_rec and ret_total > TOL and primeira is not None:
                    yield comum + (CATEGORIA_RETIDO, None, GRUPO_RETIDO) + primeira + (
                        round(ret_total * frac, 2), 0.0, 0.0, 0.0)


# -----------------------------------------------------------------------------
# APORTES — classificacao das categorias de aporte / devolucao
# -----------------------------------------------------------------------------
# Aportes NAO entram no resultado (DRE): sao movimento de caixa entre socios/
# parceiros e a empresa. Mas na avaliacao de uma obra eles sao essenciais — uma
# obra pode estar com resultado negativo e ainda assim pagando as contas porque
# alguem injetou dinheiro.
#
# A classificacao e por PADRAO no nome da categoria (case-insensitive, sem exigir
# acento). Ajuste as listas abaixo se o plano financeiro usar outros nomes: o
# a tabela `fato`, as telas e as exportacoes leem todos daqui.
# A ORDEM DESTE DICIONARIO E A REGRA. O casamento e por PEDACO do nome, e o
# primeiro que casar ganha — entao o mais especifico tem de vir primeiro.
#
# Aprendido do jeito caro em 17/09/2026: "Devolucao de Aportes BWS" contem
# "aportes bws" dentro dele. Com o aporte listado antes, a DEVOLUCAO era
# classificada como APORTE — e entrava no bloco com o sinal trocado, sem
# ninguem perceber. Devolucao vem primeiro por isso.
#
# Os quatro nomes que o dono cadastrou no plano financeiro do OMIE (17/09/2026):
#   Aportes BWS, Aportes Parceiros, Devolucao de Aportes, Devolucao de Aportes BWS
TIPOS_APORTE = {
    # DEVOLUCAO PRIMEIRO — ver a nota acima.
    "Devolução de Aporte": ["devolucao de aportes bws", "devolucao de aporte",
                            "devolucao aporte", "devolucoes de aporte",
                            "devolucao de aportes"],
    # dinheiro que ENTRA na obra vindo de socio/parceiro de fora
    "Aporte de Parceiro": ["aportes parceiros", "aporte parceiro",
                           "aportes parceiro", "aporte de parceiro",
                           "aportes de parceiro"],
    # dinheiro proprio da BWS alocado a uma obra/parceria
    "Aporte BWS": ["aportes bws", "aporte bws", "aporte b w s"],
    # distribuicao de resultado
    "Dividendos": ["dividendo", "distribuicao de lucro", "distribuicao de lucros"],
}

_APORTE_GENERICO = ["aporte", "aportes"]

# -----------------------------------------------------------------------------
# O PLANO FINANCEIRO DO DONO, POR CODIGO — 21/09/2026
# -----------------------------------------------------------------------------
# O nome da categoria e frouxo: o pessoal do financeiro troca "Aportes BWS" por
# "Aportes Parceiros" sem querer, e os dois lados da mesma operacao usam nomes
# parecidos. O CODIGO nao tem esse problema. Foi o dono quem passou a tabela:
#
#   CONTA PROVEDORA (a que manda o dinheiro)
#     saida   2.08.97  Aportes BWS
#     entrada 1.02.95  Devolucao de Aportes BWS
#
#   CONTA DA PARCERIA (a obra)
#     entrada 1.02.02  Aportes Parceiros
#     entrada 1.02.94  Aportes BWS
#     saida   2.08.02  Devolucao de Aportes
#
# "Ocorre que para efeitos de aporte, nao devemos considerar os lancamentos da
# conta provedora, senao fica meio duplicado os lancamentos."
#
# E verdade, e ele mostrou o caso: R$ 10,00 de 04/09/2025 aparecia DUAS vezes na
# lista — +10 entrando na 22069 e -10 saindo da 7011. Mesma operacao, dois
# lancamentos. Somar da certo (os sinais se anulam), mas LISTAR mostra dobrado,
# e quem le a lista conclui que o painel esta errado.
#
# Entao o lado provedor sai do bloco inteiro: nao e aporte, e o espelho de um.
CODIGOS_LADO_PROVEDOR = {"2.08.97", "1.02.95"}

# E o lado da parceria, que e o que conta.
CODIGOS_APORTE = {
    "2.08.02": "Devolução de Aporte",
    "1.02.02": "Aporte de Parceiro",
    "1.02.94": "Aporte BWS",
}

# QUEM APORTOU SAI DA CONTRAPARTE, NAO DO ROTULO.
#
# O dono, no mesmo dia: "A diferenca de Aportes BWS e Aportes Parceiros e
# somente a nomenclatura para identificar melhor. E ha varias situacoes que o
# pessoal do financeiro fez o lancamento trocado e nao colocou Aportes BWS."
#
# Ou seja: o rotulo erra, a contraparte nao. No exemplo que ele mandou, a
# entrada estava como "Aportes Parceiros" e a contraparte era BWS CONSTRUCOES
# LTDA (MATRIZ) — dinheiro da BWS com nome de parceiro.
#
# HEURISTICA, e o dono pode corrigir: empresa cujo nome tem "BWS" e a casa.
PADRAO_EMPRESA_DA_CASA = "bws"

# Tipos que compoem o SALDO de aporte do socio/parceiro.
#
# DIVIDENDO fica de fora de proposito: e distribuicao de LUCRO, nao devolucao de
# capital — abater o dividendo do saldo faria parecer que o socio retirou o
# aporte, o que nao ocorreu. Ele continua exibido, num quadro separado.
#
# O QUE SEPARA O APORTE DE VERDADE DO ESPELHO NAO E O NOME, E O SENTIDO DO
# DINHEIRO. Desenho lancado pelo dono no OMIE em 17/09/2026:
#
#   BWS manda para a obra:  saida da MATRIZ  "Aportes BWS"            (negativo)
#                           entrada na OBRA  "Aportes BWS"            (positivo)
#   O dinheiro da BWS volta: saida da OBRA   "Devolucao de Aportes"   (negativo)
#                           entrada MATRIZ   "Devolucao de Aportes BWS" (positivo)
#   Parceiro aporta:        entrada na OBRA  "Aportes Parceiros"      (positivo)
#   Parceiro recebe:        saida da OBRA    "Devolucao de Aportes"   (negativo)
#
# Os dois lados usam o MESMO nome no caso da BWS. Entao o nome nao distingue: o
# que distingue e para onde o dinheiro foi. APORTE so conta quando ENTRA na obra;
# DEVOLUCAO so conta quando SAI. O lado da matriz tem sempre o sinal contrario e
# fica de fora sozinho — sem o painel precisar saber qual conta e qual.
TIPOS_NO_SALDO = {"Aporte de Parceiro", "Aporte BWS", "Devolução de Aporte",
                  "Outros aportes"}


def _sem_acento(texto):
    tab = str.maketrans("áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
                        "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC")
    return str(texto or "").translate(tab).lower().strip()


def classificar_aporte(categoria, codigo=None, razao_social=None):
    """Que tipo de aporte e esta linha — ou None, se nao for aporte.

    O CODIGO manda quando existe: ele e o unico que nao depende de alguem ter
    digitado o nome certo. O nome fica como rede, para os lancamentos antigos,
    anteriores ao plano financeiro de 17/09/2026.

    Devolve None tambem para o LADO PROVEDOR — ele e o espelho da operacao, e
    conta-lo duplicaria a lista."""
    cod = (codigo or "").strip()
    if cod and cod in CODIGOS_LADO_PROVEDOR:
        return None
    if cod and cod in CODIGOS_APORTE:
        tipo = CODIGOS_APORTE[cod]
        return tipo if tipo == "Devolução de Aporte" else _origem(razao_social)

    c = _sem_acento((categoria or "").lower())
    for tipo, padroes in TIPOS_APORTE.items():
        for p in padroes:
            if _sem_acento(p) in c:
                # Aporte: quem aportou sai da contraparte, nao do rotulo.
                if tipo in ("Aporte de Parceiro", "Aporte BWS"):
                    return _origem(razao_social)
                return tipo
    for p in _APORTE_GENERICO:
        if p in c:
            return "Outros aportes"
    return None


def _origem(razao_social):
    """Dinheiro da casa ou do parceiro, pela contraparte."""
    nome = _sem_acento((razao_social or "").lower())
    return "Aporte BWS" if PADRAO_EMPRESA_DA_CASA in nome else "Aporte de Parceiro"


def e_aporte(categoria):
    return classificar_aporte(categoria) is not None


# -----------------------------------------------------------------------------
# Chave de MEDICAO — a mesma nas duas tabelas e nas telas
# -----------------------------------------------------------------------------
# Os titulos a receber do Omie vem SEM numero de documento, e cada parcela de uma
# medicao e um titulo separado. O unico elo entre as parcelas e a observacao — mas
# o texto completo varia entre elas (Data Protocolo, descricao livre), entao agrupar
# pela observacao crua nao junta quase nada (7 grupos contra 260).
# A observacao segue o padrao "OBRA|Medicao No: N / ...". Extraindo obra + numero
# temos uma chave estavel. Quando o padrao nao aparece, caimos na observacao inteira
# (conservador: prefere nao agrupar a agrupar errado).
_RE_MEDICAO = re.compile(r"^\s*([^|]+?)\s*\|.*?medi[cç][aã]o\s*n[ºo°]?\s*:?\s*(\d+)",
                         re.I | re.S)
# Codigo de obra e compacto e sem espacos (CEIFOR5, AREFORTAL, DELEBARRO). Isso
# barra prefixos genericos como "Gerado automaticamente pela importacao do extrato.",
# que senao juntariam medicoes de obras diferentes debaixo da mesma chave.
_RE_COD_OBRA = re.compile(r"^[A-Z0-9ÇÁÉÍÓÚÂÊÔÃÕÀÜ._\-/]{2,25}$")
_DOC_INVALIDO = {"", "nan", "none", "n/d"}

# Uma medicao costuma ser faturada em VARIAS notas (principal e reajuste, fontes de
# recurso diferentes), cada uma com seu numero de documento proprio e recebida em
# data propria. Com PRIORIDADE_MEDICAO=True a chave usa obra+medicao ANTES do
# documento, e essas notas viram uma medicao so, com os recebimentos abertos.
# Com False volta ao comportamento antigo: documento manda, e cada nota e uma
# medicao separada (so agrupa quando o documento esta vazio).
PRIORIDADE_MEDICAO = True


def _chave_por_observacao(observacao):
    """MED:OBRA|N quando a observacao segue o padrao; None caso contrario."""
    obs = str(observacao or "").strip()
    if obs.lower() in _DOC_INVALIDO:
        return None
    m = _RE_MEDICAO.match(obs)
    if not m:
        return None
    obra = m.group(1).strip().upper()
    if not _RE_COD_OBRA.match(obra):
        return None
    return f"MED:{obra}|{int(m.group(2))}"


def chave_medicao(documento, observacao, prioridade_medicao=None):
    """
    Chave de agrupamento de uma medicao. Devolve None quando nao ha nada em que
    se agarrar (o chamador decide o fallback, em geral a propria linha).
    """
    prio = PRIORIDADE_MEDICAO if prioridade_medicao is None else prioridade_medicao
    if prio:
        k = _chave_por_observacao(observacao)
        if k:
            return k
    doc = str(documento or "").strip()
    if doc.lower() not in _DOC_INVALIDO:
        return "DOC:" + doc
    if not prio:
        k = _chave_por_observacao(observacao)
        if k:
            return k
    obs = str(observacao or "").strip()
    if obs.lower() in _DOC_INVALIDO:
        return None
    return "OBS:" + obs


def rotulo_medicao(chave):
    """Texto legivel da chave, para exibir em coluna."""
    c = str(chave or "")
    if c.startswith("MED:"):
        obra, _, num = c[4:].partition("|")
        return f"{obra} | Medição {num}"
    for p in ("DOC:", "OBS:", "ROW:"):
        if c.startswith(p):
            return c[len(p):]
    # COD: e a saida quando NAO DA para saber a medicao — o titulo nao tem
    # numero de documento nem observacao no padrao "OBRA|Medicao No: N". Ai a
    # chave vira o codigo do proprio titulo no OMIE, para ele pelo menos nao se
    # misturar com outro.
    #
    # 21/09/2026 o dono topou com "COD:11255312361" na tela e perguntou o que
    # era. "COD:" nao quer dizer nada para quem le: o rotulo passa a dizer o que
    # e, e a deixar o numero a vista — e por ele que se acha o lancamento no
    # OMIE.
    if c.startswith("COD:"):
        return f"Sem número de medição (título {c[4:]})"
    return c


# ----------------------------------------------------------------------------- 
# Recebimentos/pagamentos ANALITICOS: 1 linha por MOVIMENTO liquidado
# ----------------------------------------------------------------------------- 
# A tabela `fato` tem 1 linha por titulo (x rateio): a data e a MAIOR data de
# pagamento e o valor e a soma. Um titulo recebido em 3 parcelas vira 1 linha so.
# Aqui abrimos por movimento: cada entrada com data e valor exatos.
COLUNAS_RECEB = [
    "Tipo", "Análise", "TipoReceita", "Situação", "Categoria", "Grupo",
    "Projeto", "Departamento", "Cliente ou Fornecedor (Razão Social)", "CNPJ/CPF",
    "Número do Documento", "Observação", "Medição", "Conta Corrente", "Link",
    "Data", "Valor", "Juros", "Multa", "Desconto",
    "Valor do Movimento", "Rateio %", "Parcela", "Recebimentos",
    "Total da Medição", "Origem", "Código Omie", "Ano", "Mes",
]

ORIGEM_MOV = "baixa consolidada"
ORIGEM_CREDITO = "credito bancario"
ORIGEM_SEM_MOV = "titulo quitado sem movimento"


def _escolher_recebimentos(movs, realizado):
    """
    Escolhe QUAIS movimentos representam as entradas reais de caixa do titulo.

    O Omie guarda o mesmo recebimento em duas pernas: uma BAIXA CONSOLIDADA
    (cLiquidado='S', nValLiquido>0, o total numa linha) e os CREDITOS BANCARIOS
    (cLiquidado vazio, nValLiquido=0, um por entrada). Somar as duas dobra o caixa.

    Preferimos os creditos, que e a abertura em parcelas — mas SO quando eles
    reconciliam com a consolidada (ou com o realizado). Se nao fecharem, ficamos com
    a consolidada: uma linha a menos de detalhe e melhor que um total errado.

    Devolve (lista_escolhida, origem).
    """
    # perna de previsao: sem data de pagamento ou sem valor pago.
    # cLiquidado='N' e negacao EXPLICITA (movimento fantasma) e nunca entra —
    # so o flag VAZIO e ambiguo, e e ele que carrega os creditos bancarios.
    pagos = [m for m in movs
             if m.get("data") and m.get("valor", 0.0) > TOL
             and m.get("liquidado") != "N"]
    if not pagos:
        return [], None

    consolidadas = [m for m in pagos
                    if m.get("liquidado") == "S" or m.get("liquido", 0.0) > TOL]
    creditos = [m for m in pagos
                if not (m.get("liquidado") == "S" or m.get("liquido", 0.0) > TOL)]

    soma_cons = sum(m["valor"] for m in consolidadas)
    soma_cred = sum(m["valor"] for m in creditos)

    def _fecha(a, b):
        return abs(a - b) <= max(TOL, abs(b) * 0.001)

    if creditos:
        # bate com a consolidada, ou (quando nao ha consolidada) com o realizado
        if consolidadas and _fecha(soma_cred, soma_cons):
            return creditos, ORIGEM_CREDITO
        if not consolidadas and _fecha(soma_cred, realizado):
            return creditos, ORIGEM_CREDITO
        if not consolidadas:
            return creditos, ORIGEM_CREDITO
    if consolidadas:
        return consolidadas, ORIGEM_MOV
    return creditos, ORIGEM_CREDITO


def montar_recebimentos(conn, natureza="R"):
    """
    Lista de dicionarios com 1 linha por MOVIMENTO liquidado (data e valor exatos de cada
    entrada/saida), explodido pelo rateio para bater com o resto do dashboard.

    Conciliacao: a soma de 'Valor' por titulo e IGUAL ao 'realizado' que a tabela
    `fato` usa (mesma regra: limitado ao liquido, menos desconto). Para isso cada
    movimento e escalado por um fator = realizado_titulo / soma_paga_movimentos.
    'Valor do Movimento' guarda o valor BRUTO do movimento, sem escala nem rateio.

    NAO inclui retencoes nem saldo em aberto: aqui so entra o que virou caixa.
    """
    cat, cli, proj_map, ccorr = carregar_catalogos(conn)
    mapa_log = carregar_de_para(conn)
    linhas = []

    # So os titulos da natureza pedida, em blocos. O resultado tem poucos milhares
    # de linhas: so o que virou caixa.
    filtro = " WHERE natureza=?" if natureza else ""
    params = (natureza,) if natureza else ()
    for bloco in _blocos_de_titulos(conn, filtro, params):
        codigos = [linha[0] for linha in bloco]
        mov = _movimentos_do_bloco(conn, codigos)
        rateio = _rateio_do_bloco(conn, codigos)
        detalhe = _movimentos_detalhe_do_bloco(conn, codigos)

        for row in bloco:
            (cod, nat, vdoc, ccat, ccli, icc, ndoc, nped, status, dvenc,
             vir, viss, vinss, vpis, vcofins, vcsll, obs) = row
            status = (status or "").strip()
            if status.upper() == "CANCELADO":
                continue

            bruto = float(vdoc or 0.0)
            ret_total = float((vir or 0) + (viss or 0) + (vinss or 0) +
                              (vpis or 0) + (vcofins or 0) + (vcsll or 0))
            liquido = bruto - ret_total
            is_rec = (nat == "R")
            sinal = 1.0 if is_rec else -1.0

            _dpg, pago_mov, _ab, desc_mov, liq_mov, _ju, _mu, _conta = \
                mov.get(cod, (None, 0.0, 0.0, 0.0, "N", 0.0, 0.0, None))
            quitado = bool(re.search(r"pago|recebido|conciliado", status, re.I)) or liq_mov == "S"
            if not quitado:
                continue  # nada entrou/saiu: fica so na tabela `fato`, como "em aberto"

            base_real = pago_mov if pago_mov > 0 else liquido
            quitado_bruto = min(base_real, liquido) if liquido >= 0 else base_real
            realizado = max(quitado_bruto - desc_mov, 0.0)
            if realizado <= TOL:
                continue

            desc_cat, grupo, analise = cat.get(str(ccat), (str(ccat or ""), str(ccat or ""), None))
            if not analise:
                info_log = mapa_log.get(desc_cat.strip(), {})
                grupo = info_log.get("Grupo", grupo)
                analise = info_log.get("Análise") or _analise_por_heuristica(desc_cat, grupo)
            tipo_rec = "Obra" if desc_cat.strip() == "Receita de Obras" else "Outras"
            razao, cnpj = cli.get(ccli, ("", ""))
            observacao = (obs or "").strip()
            buckets = _buckets_rateio(rateio.get(cod, []), bruto, proj_map)

            movs_todos = detalhe.get(cod, [])
            movs, origem = _escolher_recebimentos(movs_todos, realizado)
            soma_mov = sum(m["valor"] for m in movs)
            if movs and soma_mov > TOL:
                fator = realizado / soma_mov
            else:
                # Titulo marcado como quitado mas sem movimento aproveitavel no espelho:
                # cria UMA linha sintetica para o total continuar fechando.
                movs = [{"data": _dpg or dvenc, "valor": realizado, "juros": 0.0,
                         "multa": 0.0, "desconto": 0.0, "conta": icc, "status": status}]
                fator = 1.0
                origem = ORIGEM_SEM_MOV

            n_parc = len(movs)
            for i, m in enumerate(movs, start=1):
                ddt = _data_para_dt(m["data"])
                conta_cod = m.get("conta") if m.get("conta") not in (None, "") else icc
                if conta_cod in (None, ""):
                    conta = ""
                else:
                    try:
                        conta = ccorr.get(int(conta_cod)) or str(conta_cod)
                    except (TypeError, ValueError):
                        conta = str(conta_cod)
                for dep, projeto, frac in buckets:
                    linhas.append({
                        "Tipo": REC if is_rec else PAG,
                        "Análise": analise,
                        "TipoReceita": tipo_rec if is_rec else "",
                        "Situação": m.get("status") or status,
                        "Categoria": desc_cat, "Grupo": grupo,
                        "Projeto": projeto, "Departamento": dep,
                        "Cliente ou Fornecedor (Razão Social)": razao, "CNPJ/CPF": cnpj,
                        "Número do Documento": ndoc or "",
                        "Observação": observacao,
                        "Conta Corrente": conta,
                        "Link": pipefy_link(ndoc),
                        "Data": m["data"],
                        "Valor": sinal * m["valor"] * fator * frac,
                        "Juros": sinal * m["juros"] * frac,
                        "Multa": sinal * m["multa"] * frac,
                        "Desconto": sinal * m["desconto"] * frac,
                        "Valor do Movimento": sinal * m["valor"],
                        "Rateio %": round(frac * 100.0, 4),
                        "Parcela": f"{i}/{n_parc}",
                        "_evento": f"{cod}:{i}",
                        "Origem": origem,
                        "Código Omie": cod,
                        "Ano": ddt.year if ddt else None,
                        "Mes": ddt.month if ddt else None,
                    })

    # ------------------------------------------------------------------
    # PARCELAS POR MEDICAO — nao por titulo.
    # No OMIE os titulos a receber vem SEM numero de documento, e cada parcela de
    # uma medicao e um TITULO separado, ligado aos irmaos so pela observacao.
    # Numerar por titulo daria "1/1" em tudo e as parcelas ficariam invisiveis.
    # Usamos a MESMA chave da tela de medicoes: documento quando valido, senao
    # obra + numero extraidos da observacao.
    #
    # Feito em Python puro, sem pandas. Sao poucos milhares de linhas, e trazer o
    # pandas para o servico custaria uns 50 MB de memoria toda vez que o modulo
    # carregasse — numa instancia de 2 GB dividida com 14 modulos.
    # ------------------------------------------------------------------
    for linha in linhas:
        chave = chave_medicao(str(linha["Número do Documento"] or "").strip(),
                              str(linha["Observação"] or "").strip())
        linha["_chave"] = chave or f"COD:{linha['Código Omie']}"
        linha["Medição"] = rotulo_medicao(linha["_chave"])
        linha["_data"] = _data_para_dt(linha["Data"])

    if not linhas:
        return []

    # Um "recebimento" e um EVENTO (titulo + movimento). O rateio duplica linhas
    # do mesmo evento, entao a numeracao anda por evento distinto, nao por linha.
    eventos = {}
    for linha in linhas:
        eventos.setdefault(linha["_evento"], (linha["_chave"], linha["_data"]))

    por_chave = {}
    for evento, (chave, data) in eventos.items():
        por_chave.setdefault(chave, []).append((data, evento))

    indice, total_por_evento = {}, {}
    for chave, lista in por_chave.items():
        # sem data vai para o fim, como o `na_position="last"` fazia
        lista.sort(key=lambda de: (de[0] is None, de[0] or dt.date(1900, 1, 1), de[1]))
        for posicao, (_data, evento) in enumerate(lista, start=1):
            indice[evento] = posicao
            total_por_evento[evento] = len(lista)

    total_medicao = {}
    for linha in linhas:
        total_medicao[linha["_chave"]] = (total_medicao.get(linha["_chave"], 0.0)
                                          + float(linha["Valor"] or 0.0))

    for linha in linhas:
        evento = linha["_evento"]
        linha["Parcela"] = f"{indice[evento]}/{total_por_evento[evento]}"
        linha["Recebimentos"] = total_por_evento[evento]
        linha["Total da Medição"] = round(total_medicao[linha["_chave"]], 2)
        linha["Data"] = linha["_data"]          # date, pronto para o banco
        for campo in ("Valor", "Juros", "Multa", "Desconto",
                      "Valor do Movimento", "Rateio %", "Total da Medição"):
            linha[campo] = round(float(linha[campo] or 0.0), 4)
        for campo in COLUNAS_RECEB:
            if campo not in _CAMPOS_NUMERICOS_RECEB and campo != "Data":
                valor = linha.get(campo)
                linha[campo] = "" if valor is None else str(valor).strip()

    # Agrupa a medicao e, dentro dela, poe na ordem em que o dinheiro entrou.
    linhas.sort(key=lambda l: (l["_chave"], l["_data"] is None,
                               l["_data"] or dt.date(1900, 1, 1),
                               l["Número do Documento"]))
    return linhas


# Campos que sao numero ou data e nao devem virar texto.
_CAMPOS_NUMERICOS_RECEB = {"Valor", "Juros", "Multa", "Desconto",
                           "Valor do Movimento", "Rateio %", "Total da Medição",
                           "Recebimentos", "Ano", "Mes", "Código Omie"}


# --------------------------------------------------------------------------- 
# Gravacao
# --------------------------------------------------------------------------- 
COLUNAS_RECEB_BD = (
    "codigo_lancamento", "tipo", "analise", "tipo_receita", "situacao", "categoria",
    "grupo", "projeto", "departamento", "razao_social", "cnpj_cpf",
    "numero_documento", "observacao", "medicao", "conta_corrente", "link",
    "data", "ano", "mes", "valor", "juros", "multa", "desconto", "valor_movimento",
    "rateio_pct", "parcela", "recebimentos", "total_medicao", "origem",
)

# De COLUNAS_RECEB (nomes de tela, com acento) para as colunas do banco.
_DE_PARA_RECEB = {
    "codigo_lancamento": "Código Omie", "tipo": "Tipo", "analise": "Análise",
    "tipo_receita": "TipoReceita", "situacao": "Situação", "categoria": "Categoria",
    "grupo": "Grupo", "projeto": "Projeto", "departamento": "Departamento",
    "razao_social": "Cliente ou Fornecedor (Razão Social)", "cnpj_cpf": "CNPJ/CPF",
    "numero_documento": "Número do Documento", "observacao": "Observação",
    "medicao": "Medição", "conta_corrente": "Conta Corrente", "link": "Link",
    "data": "Data", "ano": "Ano", "mes": "Mes", "valor": "Valor", "juros": "Juros",
    "multa": "Multa", "desconto": "Desconto", "valor_movimento": "Valor do Movimento",
    "rateio_pct": "Rateio %", "parcela": "Parcela", "recebimentos": "Recebimentos",
    "total_medicao": "Total da Medição", "origem": "Origem",
}


def _insert_de(tabela, colunas):
    marcas = ",".join(["?"] * len(colunas))
    return f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({marcas})"


def reconstruir_fato(conn, tamanho_lote=2000):
    """Regrava a tabela `fato` do zero a partir do espelho. Devolve o total.

    Grava numa transacao so: enquanto o fato novo nao termina, as telas continuam
    enxergando o anterior inteiro; se der erro no meio, nada muda.

    A leitura vai por uma conexao SEPARADA, porque o cursor que varre os titulos
    fica aberto do inicio ao fim e um commit na mesma conexao o fecharia."""
    from ..db import conexao as _conexao_leitura

    with _conexao_leitura() as leitura:
        conn.execute("TRUNCATE TABLE fato")
        sql = _insert_de("fato", COLUNAS_FATO)
        lote, total = [], 0
        for linha in gerar_linhas_fato(leitura):
            lote.append(linha)
            if len(lote) >= tamanho_lote:
                conn.executemany(sql, lote)
                total += len(lote)
                lote = []
        if lote:
            conn.executemany(sql, lote)
            total += len(lote)
        conn.commit()
    log.info("Fato reconstruido: %s linhas.", f"{total:,}".replace(",", "."))
    return total


def reconstruir_recebimentos(conn, tamanho_lote=2000):
    """Regrava a tabela de recebimentos analiticos (1 linha por entrada de caixa)."""
    from ..db import conexao as _conexao_leitura

    with _conexao_leitura() as leitura:
        registros = montar_recebimentos(leitura)
    conn.execute("TRUNCATE TABLE fato_recebimentos")
    if not registros:
        conn.commit()
        return 0

    sql = _insert_de("fato_recebimentos", COLUNAS_RECEB_BD)
    colunas_tela = [_DE_PARA_RECEB[c] for c in COLUNAS_RECEB_BD]
    total = 0
    for inicio in range(0, len(registros), tamanho_lote):
        lote = [tuple(r.get(coluna) for coluna in colunas_tela)
                for r in registros[inicio:inicio + tamanho_lote]]
        conn.executemany(sql, lote)
        total += len(lote)
    conn.commit()
    log.info("Recebimentos reconstruidos: %s linhas.", f"{total:,}".replace(",", "."))
    return total


def semear_de_para(conn, linhas):
    """Grava o de-para Categoria -> (Analise, Grupo). `linhas` sao trincas
    (categoria, analise, grupo). Usado uma vez, para trazer o que estava no
    arquivo antigo de 14 MB, e depois pela tela de Configuracoes."""
    conn.executemany(
        "INSERT INTO categoria_de_para (categoria, analise, grupo, origem) "
        "VALUES (?,?,?,?) ON CONFLICT (categoria) DO UPDATE SET "
        "analise=excluded.analise, grupo=excluded.grupo, origem=excluded.origem",
        [(c, a, g, "log") for c, a, g in linhas])
    conn.commit()
    return len(linhas)


def reconstruir(conn):
    """As duas tabelas que as telas leem, na ordem. Devolve (fato, recebimentos)."""
    n_fato = reconstruir_fato(conn)
    n_receb = reconstruir_recebimentos(conn)
    return n_fato, n_receb
