# ============================================================================
# BWS ERP — core/titulos/regras.py
# Matriz de exigências por tipo de título (T1–T14) — item A10/2.2 da spec.
#
# MODO TRANSIÇÃO (variável ERP_MODO_TRANSICAO, padrão ligado):
#   Enquanto a captura DFe não estiver no ar, exigências de nota fiscal são
#   rebaixadas de BLOQUEIO para CRÍTICA registrada na análise — permitindo
#   operar já, sem perder o rastro do que ficou pendente de documento.
# ============================================================================
from __future__ import annotations

import os
from dataclasses import dataclass, field

from app.apps.erp.db.models.cadastros import TipoTitulo


def modo_transicao() -> bool:
    return os.environ.get("ERP_MODO_TRANSICAO", "1").strip() not in ("0", "false", "False")


@dataclass(frozen=True)
class RegrasTipo:
    rotulo: str
    exige_doc_fiscal: bool = False       # NFe/NFSe/CT-e vinculado
    exige_pedido: bool = False           # three-way match
    exige_contrato: bool = False         # locação/empreitada
    exige_conta_fornecedor: bool = True  # conta homologada p/ PIX/TED
    exige_justificativa: bool = False    # T14
    dedutivel_padrao: bool = True
    dica: str = ""


MATRIZ: dict[TipoTitulo, RegrasTipo] = {
    TipoTitulo.T1_MATERIAL_NFE: RegrasTipo(
        "Material com NFe", exige_doc_fiscal=True, exige_pedido=True,
        dica="Chave de acesso da NFe + nº do pedido."),
    TipoTitulo.T2_SERVICO_NFSE: RegrasTipo(
        "Serviço PJ com NFSe", exige_doc_fiscal=True,
        dica="NFSe + retenções conforme município/regime."),
    TipoTitulo.T3_FRETE_CTE: RegrasTipo(
        "Frete / transporte", exige_doc_fiscal=True,
        dica="CT-e vinculado à(s) NFe(s) transportada(s)."),
    TipoTitulo.T4_LOCACAO: RegrasTipo(
        "Locação (equipamento/imóvel)", exige_contrato=True,
        dica="Contrato vigente + período de referência. Sem NF (não é serviço LC 116)."),
    TipoTitulo.T5_EMPREITEIRO: RegrasTipo(
        "Empreiteiro / subcontratação", exige_doc_fiscal=True, exige_contrato=True,
        dica="Contrato + medição aprovada + NFSe + CNDs."),
    TipoTitulo.T6_SERVICO_PF_RPA: RegrasTipo(
        "Serviço de PF (RPA)",
        dica="INSS 11% + IRRF tabela + custo patronal 20% (exibido ao aprovador)."),
    TipoTitulo.T7_FOLHA_ENCARGOS: RegrasTipo(
        "Folha / encargos", exige_conta_fornecedor=False,
        dica="Origem: integração BWS Encargos / RH."),
    TipoTitulo.T8_TRIBUTO_GUIA: RegrasTipo(
        "Tributo / guia", exige_conta_fornecedor=False,
        dica="Código de barras da guia; vínculo ao título-pai quando retenção."),
    TipoTitulo.T9_CONCESSIONARIA: RegrasTipo(
        "Concessionária / utilidade", exige_conta_fornecedor=False,
        dica="Fatura da unidade consumidora vinculada à obra/sede."),
    TipoTitulo.T10_FUNDO_FIXO: RegrasTipo(
        "Fundo fixo / caixinha", exige_conta_fornecedor=False,
        dica="Prestação de contas com comprovantes; itens sem comprovante = indedutíveis."),
    TipoTitulo.T11_ADIANTAMENTO: RegrasTipo(
        "Adiantamento a fornecedor", exige_pedido=True,
        dica="Vincular ao pedido; baixa obrigatória com nota de encontro."),
    TipoTitulo.T12_REEMBOLSO: RegrasTipo(
        "Reembolso a colaborador", exige_conta_fornecedor=False,
        dica="Comprovantes item a item + aprovação do gestor."),
    TipoTitulo.T13_FINANCIAMENTO: RegrasTipo(
        "Financiamento / parcelamento", exige_conta_fornecedor=False,
        dica="Contrato + plano de parcelas; juros separados do principal."),
    TipoTitulo.T14_EXCECAO_SEM_NOTA: RegrasTipo(
        "Exceção documentada (sem nota)", exige_justificativa=True,
        dedutivel_padrao=False,
        dica="Uso restrito. Marcado INDEDUTÍVEL — custo tributário de 34% reportado."),
}


def regras_de(tipo: TipoTitulo) -> RegrasTipo:
    return MATRIZ[tipo]
