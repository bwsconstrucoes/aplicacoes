# ============================================================================
# ERP — core/comum/uso.py
# A trilha de auditoria lida como TRABALHO: quem fez o quê, quando, e quanto.
#
# POR QUE ISTO EXISTE. O dono pediu em 10/09/2026, pensando em equipe
# trabalhando de casa: *"eu preciso que o sistema entenda se aquele pessoal
# está trabalhando ou não em determinado dia. (…) o cara começou a tal hora,
# fez isso, fez aquilo, fez conciliação, lançou título, alterou status."*
#
# O dado já existia: a tabela `eventos` é append-only (um gatilho no banco
# recusa UPDATE e DELETE) e registra mais de 140 tipos de ação, com quem,
# quando, em qual registro e o detalhe. Não foi preciso passar a coletar nada —
# só ler o que já estava lá.
#
# ⚠️ O LIMITE, QUE PRECISA APARECER NA TELA E NÃO SÓ AQUI:
# isto NÃO é jornada de trabalho, e o próprio dono confirmou que não é para
# isso — *"na verdade não é pra controlar a jornada não, é só pra entender"*.
# Quem passou a manhã lendo contrato, no telefone com fornecedor ou na obra
# trabalhou e não gerou evento nenhum. O que este relatório mede é ENTREGA
# (quantos títulos, conciliações, medições) e se houve movimento no dia.
# Usado como ponto, puniria justamente quem faz o trabalho que não dá clique.
#
# A tela é ABERTA À EQUIPE por decisão do dono: cada pessoa vê a própria
# semana. Mesmo dado, e deixa de ser vigilância para virar retorno.
#
# EVENTO SEM DONO É DO SISTEMA. `usuario_id` nulo é trabalho que a fila de
# segundo plano fez sozinha (importar cards, recalcular agenda, emitir nota).
# Nunca entra na conta de ninguém — sai numa linha à parte.
# ============================================================================
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

# O ERP é operado no Ceará, que não tem horário de verão. Sem dizer o fuso, o
# banco agrupa por dia em UTC e um lançamento das 22h cai no dia seguinte —
# além de "começou às 8h" virar "começou às 11h" na tela.
FUSO = "America/Fortaleza"

# categoria → como ela se chama na tela, na ordem em que aparece
CATEGORIAS: list[tuple[str, str]] = [
    ("lancamento",   "Lançou despesa"),
    ("analise",      "Analisou e aprovou"),
    ("pagamento",    "Pagou e deu baixa"),
    ("conciliacao",  "Conciliou o banco"),
    ("medicao",      "Contratos, medições e notas"),
    ("compras",      "Comprou"),
    ("locacao",      "Locações"),
    ("pessoal",      "Despesa de colaborador"),
    ("cadastro",     "Cadastrou"),
    ("documento",    "Arquivou documento"),
    ("agenda",       "Agenda"),
    ("pergunta",     "Perguntou ao sistema"),
    ("configuracao", "Configurou o sistema"),
    ("outro",        "Outros"),
]
ROTULO_DA_CATEGORIA = dict(CATEGORIAS)

# entidade da trilha → categoria de trabalho
POR_ENTIDADE: dict[str, str] = {
    "titulo": "lancamento",            # com as exceções de POR_ACAO, abaixo
    "pagamento": "pagamento",
    "lote": "pagamento",
    "conciliacao": "conciliacao",
    "extrato": "conciliacao",
    "movimentacao": "conciliacao",
    "contrato_servico": "medicao",
    "nota_emitida": "medicao",
    "documento": "documento",
    "documento_fiscal": "documento",
    "cotacao": "compras",
    "pedido_compra": "compras",
    "suprimento_solicitacao": "compras",
    "suprimento_item": "compras",
    "insumo_solicitacao": "compras",
    "contrato_locacao": "locacao",
    "locacao_conferencia": "locacao",
    "despesa_colaborador": "pessoal",
    "colaborador": "cadastro",
    "fornecedor": "cadastro",
    "fornecedor_conta": "cadastro",
    "obra": "cadastro",
    "insumo": "cadastro",
    "insumo_categoria": "cadastro",
    "empresa": "cadastro",
    "conta_bancaria": "cadastro",
    "unidade_compra": "cadastro",
    "condicao_pagamento": "cadastro",
    "agenda": "agenda",
    # Perguntar não é entrega — é consulta. Fica em categoria própria para
    # não inflar a produção de ninguém, e porque saber QUEM pergunta muito é
    # informação útil por si só.
    "pergunta": "pergunta",
    "categoria": "configuracao",
    "categoria_depara": "configuracao",
    "parametro": "configuracao",
    "usuario": "configuracao",
    "periodo": "configuracao",
    "indice": "configuracao",
    "configuracao": "configuracao",
    "erp": "configuracao",
    "suprimentos": "configuracao",
}

# (entidade, ação) que fogem da regra da entidade. Quase todas são do título,
# que atravessa a vida inteira do lançamento: nasce, é analisado, é pago.
# Contar tudo como "lançou" faria quem só aprova parecer que lança o dia todo.
POR_ACAO: dict[tuple[str, str], str] = {
    ("titulo", "ANALISADO"): "analise",
    ("titulo", "APROVADO"): "analise",
    ("titulo", "AGUARDANDO_AVAL"): "analise",
    ("titulo", "DEVOLVIDO"): "analise",
    ("titulo", "CANCELADO"): "analise",
    ("titulo", "ALERTAS_ANALISADOS"): "analise",
    ("titulo", "ALERTAS_DUPLICIDADE_ACEITOS"): "analise",
    ("titulo", "DEDUTIBILIDADE_DEFINIDA"): "analise",
    ("titulo", "ITEM_CONFERIDO"): "analise",
    ("titulo", "ENQUADRAMENTO_VENCIDO"): "analise",
    ("titulo", "BAIXA_POR_COMPROVANTE"): "pagamento",
    ("titulo", "BAIXA_CONFIRMADA_COM_COMPROVANTE"): "pagamento",
    ("titulo", "BAIXA_DESFEITA"): "pagamento",
    ("titulo", "ESTORNADO"): "pagamento",
    ("titulo", "MEDICAO_LANCADA"): "medicao",
    ("titulo", "MEDICAO_CLASSIFICADA"): "medicao",
    ("titulo", "PROTOCOLADA"): "medicao",
    ("titulo", "RECEBIMENTO"): "medicao",
    ("titulo", "NOTAS_REGISTRADAS"): "documento",
}


# Como cada entidade se chama para uma pessoa. Sem isto o passo a passo diz
# "insumo_categoria" e "contrato_locacao", que é o banco falando.
NOME_DA_ENTIDADE: dict[str, str] = {
    "titulo": "título", "pagamento": "pagamento", "lote": "lote de pagamento",
    "conciliacao": "conciliação", "extrato": "extrato", "movimentacao": "movimentação",
    "contrato_servico": "contrato de obra", "nota_emitida": "nota emitida",
    "documento": "documento", "documento_fiscal": "nota fiscal recebida",
    "cotacao": "cotação", "pedido_compra": "pedido de compra",
    "suprimento_solicitacao": "pedido de material",
    "suprimento_item": "item do pedido de material",
    "insumo_solicitacao": "pedido de cadastro de insumo",
    "contrato_locacao": "contrato de locação",
    "locacao_conferencia": "conferência de locação",
    "despesa_colaborador": "despesa de colaborador", "colaborador": "colaborador",
    "fornecedor": "fornecedor", "fornecedor_conta": "conta do fornecedor",
    "obra": "obra", "insumo": "insumo", "insumo_categoria": "categoria de insumo",
    "empresa": "empresa", "conta_bancaria": "conta bancária",
    "unidade_compra": "unidade de compra", "condicao_pagamento": "condição de pagamento",
    "agenda": "agenda", "pergunta": "pergunta escrita", "categoria": "conta do plano",
    "categoria_depara": "tradução do plano antigo", "parametro": "parâmetro",
    "usuario": "operador", "periodo": "período", "indice": "índice",
    "configuracao": "configuração", "erp": "sistema", "suprimentos": "suprimentos",
}


def nome_da_entidade(entidade: Optional[str]) -> str:
    ent = (entidade or "").strip()
    return NOME_DA_ENTIDADE.get(ent, ent or "—")


def categoria_de(entidade: Optional[str], acao: Optional[str]) -> str:
    """Em que tipo de trabalho esta ação entra."""
    ent = (entidade or "").strip()
    aca = (acao or "").strip()
    especifica = POR_ACAO.get((ent, aca))
    if especifica:
        return especifica
    return POR_ENTIDADE.get(ent, "outro")


def _periodo(de: Optional[date], ate: Optional[date]) -> tuple[date, date]:
    """Sem período dito, os últimos 7 dias — que é a pergunta de sempre."""
    fim = ate or date.today()
    inicio = de or (fim - timedelta(days=6))
    if inicio > fim:
        inicio, fim = fim, inicio
    return inicio, fim


def _agregado(s: Session, *, de: date, ate: date,
              usuario_id: Optional[int] = None) -> list[tuple]:
    """A trilha somada no BANCO, por pessoa/dia/tipo de ação.

    Agregado de propósito: a tabela `eventos` só cresce, e trazer evento por
    evento para a memória derrubaria a instância, que tem 2 GB e divide o
    processo com os outros módulos do monorepo. O que volta daqui é da ordem
    de pessoas × dias × tipos de ação — o resto da conta é feito em Python,
    onde mora o mapa de categorias.
    """
    filtro = "" if usuario_id is None else " AND e.usuario_id = :u"
    sql = text(f"""
        SELECT e.usuario_id,
               (e.criado_em AT TIME ZONE :fuso)::date        AS dia,
               e.entidade_tipo,
               e.acao,
               COUNT(*)                                      AS quantas,
               MIN(e.criado_em AT TIME ZONE :fuso)           AS primeira,
               MAX(e.criado_em AT TIME ZONE :fuso)           AS ultima
          FROM eventos e
         WHERE (e.criado_em AT TIME ZONE :fuso)::date BETWEEN :de AND :ate
           {filtro}
         GROUP BY e.usuario_id, dia, e.entidade_tipo, e.acao
    """)
    params: dict[str, Any] = {"fuso": FUSO, "de": de, "ate": ate}
    if usuario_id is not None:
        params["u"] = int(usuario_id)
    return list(s.execute(sql, params))


def _hora(v: Any) -> Optional[str]:
    return v.strftime("%H:%M") if v is not None else None


def _juntar(linhas: list[tuple]) -> dict[Any, dict[str, Any]]:
    """Agrupa por (pessoa, dia), somando por categoria de trabalho."""
    dias: dict[Any, dict[str, Any]] = {}
    for usuario_id, dia, entidade, acao, quantas, primeira, ultima in linhas:
        chave = (usuario_id, dia)
        d = dias.setdefault(chave, {
            "usuario_id": usuario_id, "dia": dia, "acoes": 0,
            "primeira": primeira, "ultima": ultima, "por_categoria": {}})
        d["acoes"] += int(quantas or 0)
        if primeira is not None and (d["primeira"] is None or primeira < d["primeira"]):
            d["primeira"] = primeira
        if ultima is not None and (d["ultima"] is None or ultima > d["ultima"]):
            d["ultima"] = ultima
        cat = categoria_de(entidade, acao)
        d["por_categoria"][cat] = d["por_categoria"].get(cat, 0) + int(quantas or 0)
    return dias


def _categorias_ordenadas(por_categoria: dict[str, int]) -> list[dict[str, Any]]:
    return [{"chave": chave, "rotulo": rotulo, "quantas": por_categoria[chave]}
            for chave, rotulo in CATEGORIAS if por_categoria.get(chave)]


def semana_da_pessoa(s: Session, usuario_id: int, *, de: Optional[date] = None,
                     ate: Optional[date] = None) -> dict[str, Any]:
    """Os dias de UMA pessoa: quando começou, quando parou, e o que fez.

    É esta que alimenta a "minha semana" — a mesma tela que a pessoa abre sobre
    si mesma e que o dono abre sobre a equipe.
    """
    inicio, fim = _periodo(de, ate)
    dias = _juntar([l for l in _agregado(s, de=inicio, ate=fim, usuario_id=usuario_id)
                    if l[0] == usuario_id])
    saida = []
    for (_, dia), d in sorted(dias.items(), key=lambda kv: kv[0][1]):
        saida.append({
            "dia": dia.isoformat(),
            "primeira": _hora(d["primeira"]), "ultima": _hora(d["ultima"]),
            "acoes": d["acoes"],
            "categorias": _categorias_ordenadas(d["por_categoria"]),
        })
    return {
        "usuario_id": usuario_id,
        "de": inicio.isoformat(), "ate": fim.isoformat(),
        "dias": saida,
        "dias_com_trabalho": len(saida),
        "acoes": sum(d["acoes"] for d in saida),
        # Dito no retorno, e não só na tela, para quem consumir isto por fora
        # não confundir com controle de jornada.
        "aviso": ("Isto mede o que foi feito NO SISTEMA. Trabalho fora dele — "
                  "ler contrato, falar com fornecedor, ir à obra — não aparece "
                  "aqui. Não é controle de jornada."),
    }


def semana_da_equipe(s: Session, *, de: Optional[date] = None,
                     ate: Optional[date] = None) -> dict[str, Any]:
    """Uma linha por pessoa no período, mais o que o sistema fez sozinho."""
    from sqlalchemy import select

    from app.apps.erp.db.models.cadastros import Usuario

    inicio, fim = _periodo(de, ate)
    dias = _juntar(_agregado(s, de=inicio, ate=fim))

    por_pessoa: dict[Any, dict[str, Any]] = {}
    for (usuario_id, _), d in dias.items():
        p = por_pessoa.setdefault(usuario_id, {
            "usuario_id": usuario_id, "dias_com_trabalho": 0, "acoes": 0,
            "por_categoria": {}, "primeira": None, "ultima": None})
        p["dias_com_trabalho"] += 1
        p["acoes"] += d["acoes"]
        for cat, quantas in d["por_categoria"].items():
            p["por_categoria"][cat] = p["por_categoria"].get(cat, 0) + quantas
        if d["primeira"] is not None and (p["primeira"] is None
                                          or d["primeira"] < p["primeira"]):
            p["primeira"] = d["primeira"]
        if d["ultima"] is not None and (p["ultima"] is None or d["ultima"] > p["ultima"]):
            p["ultima"] = d["ultima"]

    nomes = {u.id: u.nome for u in s.scalars(select(Usuario)).all()}
    pessoas, do_sistema = [], None
    for usuario_id, p in por_pessoa.items():
        linha = {
            "usuario_id": usuario_id,
            "nome": nomes.get(usuario_id) or "—",
            "dias_com_trabalho": p["dias_com_trabalho"],
            "acoes": p["acoes"],
            "categorias": _categorias_ordenadas(p["por_categoria"]),
        }
        if usuario_id is None:
            # Trabalho da fila de segundo plano. Sai à parte para não virar
            # produção de ninguém — e porque some da conta se for ignorado.
            linha["nome"] = "O próprio sistema (trabalho automático)"
            do_sistema = linha
        else:
            pessoas.append(linha)
    pessoas.sort(key=lambda x: (-x["acoes"], x["nome"]))
    return {
        "de": inicio.isoformat(), "ate": fim.isoformat(),
        "pessoas": pessoas, "do_sistema": do_sistema,
        "acoes": sum(p["acoes"] for p in pessoas),
        "aviso": ("Isto mede o que foi feito NO SISTEMA. Trabalho fora dele — "
                  "ler contrato, falar com fornecedor, ir à obra — não aparece "
                  "aqui. Não é controle de jornada."),
    }


def detalhe_do_dia(s: Session, usuario_id: int, dia: date,
                   limite: int = 2000) -> list[dict[str, Any]]:
    """O passo a passo de um dia — a resposta para "o que ele fez ontem?".

    REPETIÇÃO SEGUIDA VIRA UMA LINHA SÓ. Uma carga de planilha grava milhares
    de eventos iguais em segundos, e a primeira versão desta tela devolveu 76
    linhas de "criou insumo" — ninguém lê isso, e o dia inteiro fica ilegível
    por causa de um único minuto de importação. Agora sai
    "insumo criado · 3.251 vezes, das 12:11 às 12:14", que é a mesma verdade
    em uma linha.

    O teto é sobre os eventos LIDOS, não sobre as linhas mostradas, e a tela
    avisa quando ele foi alcançado — senão o dia apareceria cortado sem dizer.
    """
    sql = text("""
        SELECT (e.criado_em AT TIME ZONE :fuso) AS quando,
               e.entidade_tipo, e.entidade_id, e.acao
          FROM eventos e
         WHERE e.usuario_id = :u
           AND (e.criado_em AT TIME ZONE :fuso)::date = :dia
         ORDER BY e.criado_em
         LIMIT :limite
    """)
    linhas = list(s.execute(sql, {"fuso": FUSO, "u": int(usuario_id),
                                  "dia": dia, "limite": int(limite)}))
    passos: list[dict[str, Any]] = []
    for quando, entidade, entidade_id, acao in linhas:
        categoria = categoria_de(entidade, acao)
        anterior = passos[-1] if passos else None
        if (anterior is not None and anterior["entidade"] == entidade
                and anterior["acao"] == acao):
            anterior["quantas"] += 1
            anterior["ate"] = _hora(quando)
            continue
        passos.append({
            "hora": _hora(quando), "ate": _hora(quando), "quantas": 1,
            "entidade": entidade, "entidade_nome": nome_da_entidade(entidade),
            "entidade_id": entidade_id, "acao": acao,
            "o_que": f"{nome_da_entidade(entidade)} {(acao or '').replace('_', ' ').lower()}",
            "categoria": categoria,
            "rotulo": ROTULO_DA_CATEGORIA.get(categoria, "Outros"),
        })
    return passos
