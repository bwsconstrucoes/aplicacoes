# ============================================================================
# ERP — core/manutencao/limpeza.py
# Zerar o movimento do ERP por ÁREA, para recomeçar depois de testar.
#
# O dono precisa testar o sistema inteiro e depois começar o uso de verdade com
# a base limpa. Isto é um botão que apaga dados em produção, e o desenho todo
# existe para tornar difícil o estrago:
#
#   1. APAGA MOVIMENTO, NUNCA CADASTRO. Obra, fornecedor, plano de contas,
#      colaborador, insumo, conta bancária e operador ficam sempre. Refazer
#      isso custa horas e não é "lançamento".
#   2. A ORDEM VEM DO BANCO. As dependências são lidas do próprio Postgres
#      (pg_constraint) e ordenadas — não há lista escrita à mão para apodrecer
#      quando alguém criar uma tabela nova.
#   3. RECUSA EM VEZ DE SURPREENDER. Se algo de FORA das áreas escolhidas
#      aponta para o que sairia, a limpeza para e diz qual área falta marcar.
#      Nunca apaga em cascata por conta própria.
#   4. MOSTRA ANTES. Conta linha por linha, e só então pede a frase digitada.
#   5. DEIXA RASTRO de quem fez, quando, e quanto saiu de cada tabela.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

FRASE_DE_CONFIRMACAO = "ZERAR"

# Tabelas que este botão NUNCA apaga, aconteça o que acontecer. Estão aqui
# porque perdê-las é pior do que qualquer teste sujo: sem `usuarios` ninguém
# entra, sem `_migracoes` o banco esquece o que já rodou, e os cadastros
# custaram uma importação.
JAMAIS = frozenset({
    "usuarios", "usuario_obras", "usuario_categorias", "usuario_permissoes",
    "alcadas", "parametros", "_migracoes",
    "obras", "obra_aditivos", "obra_fases", "obra_interessados",
    "fornecedores", "fornecedor_contas", "fornecedor_contatos",
    "fornecedor_categorias",
    "categorias", "categoria_depara", "contas_bancarias", "contratos",
    "colaboradores", "funcoes",
    "insumos", "insumo_categorias", "unidades_compra", "condicoes_pagamento",
})

# Área → (rótulo, o que sai em palavras, tabelas).
AREAS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "suprimentos": (
        "Suprimentos",
        "solicitações de material, cotações e mapas, pedidos de compra, "
        "previsões de pagamento, recebimentos e o banco de preços",
        ("insumo_solicitacoes", "suprimento_solicitacoes", "suprimento_itens",
         "cotacoes", "cotacao_itens", "cotacao_fornecedores", "cotacao_precos",
         "precos_historico", "pedidos_compra", "pedido_itens",
         "pedido_item_reserva", "previsoes_pagamento", "recebimentos",
         "recebimento_itens"),
    ),
    "financeiro": (
        "Financeiro",
        "títulos e suas parcelas, rateios, retenções, análises, avais, "
        "pagamentos, lotes e avisos enviados",
        ("titulos", "parcelas", "rateios", "retencoes", "analises",
         "titulo_avais", "titulo_itens", "titulo_interessados",
         "titulo_colaboradores", "documentos_fiscais", "pagamentos",
         "lotes", "lote_itens", "movimentacoes", "notificacoes", "sync_queue",
         "pedidos", "periodos_bloqueados"),
    ),
    "conciliacao": (
        "Extratos e conciliação",
        "os extratos bancários importados e as conciliações feitas com eles",
        ("extratos", "conciliacoes"),
    ),
    "pessoal": (
        "Despesas com colaborador",
        "as despesas lançadas para colaboradores e seus itens",
        ("despesas_colaborador", "despesa_colaborador_itens"),
    ),
    "empreitas": (
        "Medições de empreita",
        "as medições e seus itens — os contratos de empreita continuam",
        ("contrato_medicoes", "medicao_itens"),
    ),
    "contratos_empreita": (
        "Contratos de empreita",
        "os próprios contratos de empreita e seus itens",
        ("contratos_servico", "contrato_servico_itens"),
    ),
    "locacoes": (
        "Movimento de locação",
        "as parcelas e as devoluções — os contratos de locação continuam",
        ("locacao_parcelas", "locacao_movimentos"),
    ),
    "contratos_locacao": (
        "Contratos de locação",
        "os próprios contratos de locação e seus itens",
        ("contratos_locacao", "locacao_itens"),
    ),
    "anexos": (
        "Arquivos anexados",
        "notas, comprovantes, contratos e propostas guardados no banco",
        ("anexos",),
    ),
    "ia": (
        "Consumo de IA",
        "o histórico de gasto com leitura por IA — é dinheiro que já saiu, "
        "apagar a conta não desfaz o gasto",
        ("ia_uso",),
    ),
    "auditoria": (
        "Trilha de auditoria",
        "o registro de quem fez o quê. Apagar aqui é perder a memória do "
        "sistema — só faz sentido junto com tudo o mais",
        ("eventos",),
    ),
}


def _tabelas_de(areas: Iterable[str]) -> list[str]:
    escolhidas: list[str] = []
    for area in areas:
        if area not in AREAS:
            raise ErroValidacao(f"Área desconhecida: {area!r}")
        for tabela in AREAS[area][2]:
            if tabela in JAMAIS:                       # cinto e suspensório
                raise ErroValidacao(
                    f"{tabela} é cadastro e nunca sai por aqui.")
            if tabela not in escolhidas:
                escolhidas.append(tabela)
    return escolhidas


def _existentes(s: Session, tabelas: list[str]) -> list[str]:
    """Só o que existe mesmo no banco — tabela de migração ainda não aplicada
    não pode derrubar a limpeza."""
    if not tabelas:
        return []
    achadas = {linha[0] for linha in s.execute(text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = ANY(:t)"),
        {"t": list(tabelas)}).all()}
    return [t for t in tabelas if t in achadas]


def _dependencias(s: Session) -> list[tuple[str, str, str, bool]]:
    """(filho, pai, coluna, obrigatória) para toda chave estrangeira do banco.

    Vem do Postgres justamente para não existir lista escrita à mão: tabela
    nova entra sozinha no cálculo da ordem.
    """
    linhas = s.execute(text(
        "SELECT con.conrelid::regclass::text, con.confrelid::regclass::text, "
        "       a.attname, a.attnotnull "
        "FROM pg_constraint con "
        "JOIN pg_attribute a ON a.attrelid = con.conrelid "
        "                   AND a.attnum = ANY(con.conkey) "
        "WHERE con.contype = 'f'")).all()
    return [(f, p, coluna, bool(obrigatoria)) for f, p, coluna, obrigatoria in linhas]


def ordenar(tabelas: list[str], dependencias: list[tuple[str, str, str, bool]]) -> list[str]:
    """Filho antes de pai. Fora dessa ordem o banco recusa no meio e sobra
    metade apagada — que é o pior resultado possível aqui."""
    dentro = set(tabelas)
    depois_de: dict[str, set[str]] = {t: set() for t in tabelas}
    for filho, pai, _coluna, _obrig in dependencias:
        if filho in dentro and pai in dentro and filho != pai:
            depois_de[pai].add(filho)          # o pai só sai depois do filho

    ordem: list[str] = []
    pendentes = dict(depois_de)
    while pendentes:
        livres = sorted(t for t, faltam in pendentes.items()
                        if not (faltam - set(ordem)))
        if not livres:                          # ciclo: resolve pela ordem dada
            livres = [sorted(pendentes)[0]]
        for t in livres:
            ordem.append(t)
            pendentes.pop(t, None)
    return ordem


def bloqueios(s: Session, tabelas: list[str],
              dependencias: list[tuple[str, str, str, bool]]) -> list[dict[str, Any]]:
    """Quem de FORA aponta para o que sairia — e ainda tem linha apontando.

    A limpeza recusa nesses casos em vez de apagar em cascata. Cascata é como
    se perde, num clique, uma tabela que ninguém pretendia tocar.
    """
    dentro = set(tabelas)
    achados = []
    for filho, pai, coluna, _obrig in dependencias:
        if pai not in dentro or filho in dentro or filho in ("pg_catalog",):
            continue
        try:
            quantas = int(s.execute(text(
                f"SELECT count(*) FROM {filho} WHERE {coluna} IS NOT NULL"
            )).scalar() or 0)
        except Exception:                       # tabela sumiu no meio do caminho
            continue
        if quantas:
            achados.append({"tabela": filho, "aponta_para": pai,
                            "coluna": coluna, "linhas": quantas,
                            "area": _area_de(filho)})
    return achados


def _area_de(tabela: str) -> Optional[str]:
    for chave, (_rotulo, _desc, tabelas) in AREAS.items():
        if tabela in tabelas:
            return chave
    return None


def catalogo() -> list[dict[str, Any]]:
    """As áreas, para a tela montar as caixinhas."""
    return [{"chave": chave, "rotulo": rotulo, "descricao": descricao,
             "tabelas": list(tabelas)}
            for chave, (rotulo, descricao, tabelas) in AREAS.items()]


def resumo(s: Session, areas: Iterable[str]) -> dict[str, Any]:
    """O que sairia, por tabela — sem apagar nada."""
    tabelas = _existentes(s, _tabelas_de(areas))
    dependencias = _dependencias(s)
    ordem = ordenar(tabelas, dependencias)

    detalhe, total = [], 0
    for tabela in ordem:
        quantas = int(s.execute(text(f"SELECT count(*) FROM {tabela}")).scalar() or 0)
        if quantas:
            detalhe.append({"tabela": tabela, "linhas": quantas})
        total += quantas

    impedimentos = bloqueios(s, tabelas, dependencias)
    return {
        "areas": list(areas),
        "total": total,
        "detalhe": detalhe,
        "ordem": ordem,
        "impedimentos": impedimentos,
        "preservado_sempre": sorted(JAMAIS),
        "frase": FRASE_DE_CONFIRMACAO,
    }


def zerar(s: Session, areas: Iterable[str], confirmacao: str,
          usuario: Usuario) -> dict[str, Any]:
    """Apaga o movimento das áreas escolhidas. Só o ADMIN chega aqui (rota)."""
    areas = list(areas)
    if not areas:
        raise ErroValidacao("Escolha ao menos uma área.")
    if (confirmacao or "").strip().upper() != FRASE_DE_CONFIRMACAO:
        raise ErroValidacao(
            f'Para confirmar, digite exatamente "{FRASE_DE_CONFIRMACAO}".')

    tabelas = _existentes(s, _tabelas_de(areas))
    dependencias = _dependencias(s)
    impedimentos = bloqueios(s, tabelas, dependencias)
    if impedimentos:
        faltando = sorted({b["area"] or b["tabela"] for b in impedimentos})
        detalhe = ", ".join(f"{b['tabela']} ({b['linhas']})"
                            for b in impedimentos[:4])
        raise ErroValidacao(
            f"Não dá para zerar assim: {detalhe} ainda aponta para o que sairia. "
            f"Marque também: {', '.join(faltando)}.")

    feitos, total = [], 0
    for tabela in ordenar(tabelas, dependencias):
        resultado = s.execute(text(f"DELETE FROM {tabela}"))
        quantas = int(resultado.rowcount or 0)
        if quantas:
            feitos.append({"tabela": tabela, "linhas": quantas})
        total += quantas

    if "auditoria" not in areas:
        registrar_evento(s, "erp", 0, "MOVIMENTO_ZERADO",
                         {"areas": areas, "total": total, "detalhe": feitos},
                         usuario.id if usuario else None)
    logger.warning("ERP/manutenção: %s zerou %s — %d linha(s): %s",
                   getattr(usuario, "nome", "?"), areas, total, feitos)
    return {"areas": areas, "total": total, "detalhe": feitos}
