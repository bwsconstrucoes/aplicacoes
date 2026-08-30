# ============================================================================
# BWS ERP — core/titulos/derivacao.py
# Deriva o TIPO interno (T1–T14) a partir da CATEGORIA do plano financeiro —
# decisão de projeto: quem lança escolhe a categoria (como no Pipefy);
# os códigos T* nunca aparecem na tela.
#
# Regra:
#   - categoria com 1 tipo permitido  → tipo automático, sem pergunta;
#   - categoria com vários            → uma pergunta amigável ("o que
#     acompanha esta despesa?") com os rótulos humanos dos tipos permitidos;
#   - categoria sem restrição         → pergunta com todos os rótulos.
# ============================================================================
from __future__ import annotations

from app.apps.erp.db.models.cadastros import Categoria, TipoTitulo
from app.apps.erp.core.titulos.regras import MATRIZ

# Rótulo humano da pergunta (sem códigos técnicos)
ROTULO_PERGUNTA: dict[TipoTitulo, str] = {
    TipoTitulo.T1_MATERIAL_NFE: "Nota fiscal de material (NFe/DANFE)",
    TipoTitulo.T2_SERVICO_NFSE: "Nota de serviço (NFSe)",
    TipoTitulo.T3_FRETE_CTE: "Conhecimento de frete (CT-e)",
    TipoTitulo.T4_LOCACAO: "Locação com contrato (sem nota)",
    TipoTitulo.T5_EMPREITEIRO: "Medição de empreiteiro (contrato + NFSe)",
    TipoTitulo.T6_SERVICO_PF_RPA: "Serviço de pessoa física (RPA)",
    TipoTitulo.T7_FOLHA_ENCARGOS: "Folha de pagamento / encargos",
    TipoTitulo.T8_TRIBUTO_GUIA: "Guia de tributo (DARF/GPS/DAM...)",
    TipoTitulo.T9_CONCESSIONARIA: "Fatura de concessionária (água/luz/net)",
    TipoTitulo.T10_FUNDO_FIXO: "Fundo fixo / caixinha",
    TipoTitulo.T11_ADIANTAMENTO: "Adiantamento a fornecedor",
    TipoTitulo.T12_REEMBOLSO: "Reembolso a colaborador",
    TipoTitulo.T13_FINANCIAMENTO: "Parcela de financiamento/empréstimo",
    TipoTitulo.T14_EXCECAO_SEM_NOTA: "Sem documento fiscal (exceção justificada)",
}


def tipos_da_categoria(cat: Categoria) -> list[TipoTitulo]:
    brutos = cat.tipos_permitidos or []
    tipos = []
    for t in brutos:
        tipos.append(t if isinstance(t, TipoTitulo) else TipoTitulo(str(t)))
    return tipos or list(MATRIZ.keys())


def derivar_tipo(cat: Categoria, escolha_rotulo: str | None = None) -> tuple[TipoTitulo | None, list[str]]:
    """Retorna (tipo, opções_de_pergunta). Se tipo vier None, a UI deve
    perguntar usando as opções (rótulos humanos) e chamar de novo com a
    escolha."""
    tipos = tipos_da_categoria(cat)
    if len(tipos) == 1:
        return tipos[0], []
    opcoes = [ROTULO_PERGUNTA[t] for t in tipos]
    if escolha_rotulo:
        for t in tipos:
            if ROTULO_PERGUNTA[t] == escolha_rotulo:
                return t, opcoes
    return None, opcoes


# ordem de preferência quando a categoria aceita vários tipos e o contexto
# não é conclusivo (o primeiro que a categoria aceitar vence)
_PREFERENCIA = [
    TipoTitulo.T1_MATERIAL_NFE, TipoTitulo.T2_SERVICO_NFSE, TipoTitulo.T5_EMPREITEIRO,
    TipoTitulo.T3_FRETE_CTE, TipoTitulo.T4_LOCACAO, TipoTitulo.T9_CONCESSIONARIA,
    TipoTitulo.T8_TRIBUTO_GUIA, TipoTitulo.T7_FOLHA_ENCARGOS, TipoTitulo.T6_SERVICO_PF_RPA,
    TipoTitulo.T10_FUNDO_FIXO, TipoTitulo.T12_REEMBOLSO, TipoTitulo.T13_FINANCIAMENTO,
    TipoTitulo.T11_ADIANTAMENTO, TipoTitulo.T14_EXCECAO_SEM_NOTA,
]


def derivar_por_contexto(cat, contexto: dict) -> TipoTitulo:
    """Deduz o tipo interno a partir da categoria escolhida e do que o
    lançamento traz (documento, forma de pagamento, contrato, pedido).
    Quem lança nunca vê os códigos T*."""
    permitidos = tipos_da_categoria(cat)
    if len(permitidos) == 1:
        return permitidos[0]

    forma = (contexto.get("forma_pagamento") or "").upper()
    tem_doc = bool((contexto.get("documento_numero") or "").strip()
                   or contexto.get("documento_fiscal_id"))
    tipo_doc = (contexto.get("tipo_documento") or "").upper()

    candidatos: list[TipoTitulo] = []
    if contexto.get("contrato_id"):
        candidatos.append(TipoTitulo.T4_LOCACAO)
    if forma == "GUIA":
        candidatos.append(TipoTitulo.T8_TRIBUTO_GUIA)
    if tipo_doc == "NFE":
        candidatos.append(TipoTitulo.T1_MATERIAL_NFE)
    elif tipo_doc == "NFSE":
        candidatos.append(TipoTitulo.T2_SERVICO_NFSE)
    elif tipo_doc == "CTE":
        candidatos.append(TipoTitulo.T3_FRETE_CTE)
    elif tipo_doc in ("RPA",):
        candidatos.append(TipoTitulo.T6_SERVICO_PF_RPA)
    if not tem_doc:
        candidatos.append(TipoTitulo.T14_EXCECAO_SEM_NOTA)

    for t in candidatos:
        if t in permitidos:
            return t
    for t in _PREFERENCIA:
        if t in permitidos:
            return t
    return permitidos[0]
