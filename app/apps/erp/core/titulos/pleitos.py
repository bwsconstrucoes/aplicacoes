# ============================================================================
# ERP — core/titulos/pleitos.py
# OS PLEITOS DO FINANCEIRO: tipos, subtipos, campos, condições e documentos.
#
# ⚑ O PEDIDO (27/11/2026), com as palavras dele:
#
#   *"É importante revisar toda a lógica de condicionais dos campos que serão
#   exibidos e obrigatoriedade de forma que haja coerência com cada tipo e
#   subtipo de lançamento. Conforme fizemos no DP, pra cada pleito temos que ter
#   e exibir certos tipos de campo. Não tem sentido por exemplo exibir campo de
#   boleto quando o tipo de pagamento for pix."*
#
#   E, no mesmo dia: *"a mesma lógica deve ser aplicada ao ERP também nos casos
#   em que se mantenha lançamentos em ambos lugares"* — e *"essa lógica e
#   condições dos campos nunca foi revisada no ERP"*.
#
# ⚑ POR ISSO ESTE ARQUIVO É A ÚNICA FONTE, e é de propósito:
#
#   · o PORTAL desenha o formulário a partir daqui;
#   · a tela LANÇAR do ERP lê daqui as formas, os campos e os documentos;
#   · o SERVIDOR confere por aqui o que chega das duas portas (`conferir_*`);
#   · o MAPA que o dono revisa (`PLEITOS_FINANCEIRO.md`) é gerado daqui, e há
#     teste conferindo que o arquivo do repositório é o que este código gera.
#
# Três cópias da mesma regra (tela do portal, tela do ERP, servidor) divergem no
# primeiro mês — e a pessoa descobre a divergência só na recusa do envio, com
# tudo digitado. É o mesmo desenho do DP (`pessoal_solicitacoes.TIPOS`).
#
# ⚑ O VOCABULÁRIO DAS CONDIÇÕES — o mesmo do DP:
#   · `visivel_se`     {campo: [valores]}: o campo só aparece nesses casos;
#   · `obrigatorio_se` {campo: [valores]}: só nesses casos ele é obrigatório;
#   · campo que NÃO está visível é DESCARTADO pelo servidor. Não é cortesia: uma
#     linha digitável esquecida num Pix, ou uma conta de credor esquecida num
#     boleto, é dado que ninguém conferiu indo para dentro do título.
# ============================================================================
from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from app.apps.erp.core.comum.auditoria import ErroValidacao

# ---------------------------------------------------------------------------
# 1. AS FORMAS DE PAGAMENTO — o que cada uma pede
#
# A forma decide DOIS campos, e só eles: a conta do credor (o dinheiro sai para
# uma conta dele) e a linha digitável (o dinheiro sai por um código de barras).
# Um não convive com o outro: Pix não tem código de barras, boleto não tem conta
# de destino — o destino está dentro do código.
# ---------------------------------------------------------------------------
FORMAS: dict[str, dict[str, Any]] = {
    "BOLETO": {
        "rotulo": "Boleto",
        "usa_conta": False, "usa_linha": True, "linha_obrigatoria": True,
        "dica": "A linha digitável é conferida: dígitos, valor e vencimento "
                "têm de bater com a parcela.",
    },
    "PIX": {
        "rotulo": "Pix",
        "usa_conta": True, "usa_linha": False, "linha_obrigatoria": False,
        "dica": "O Pix sai para a chave cadastrada no credor. Chave nova entra "
                "pendente e o financeiro homologa antes de pagar.",
    },
    "TED": {
        "rotulo": "Transferência (TED)",
        "usa_conta": True, "usa_linha": False, "linha_obrigatoria": False,
        "dica": "A transferência sai para a conta cadastrada no credor. Conta "
                "nova entra pendente e o financeiro homologa antes de pagar.",
    },
    "GUIA": {
        "rotulo": "Guia (DARF, GPS, DAM, FGTS)",
        "usa_conta": False, "usa_linha": True, "linha_obrigatoria": False,
        "dica": "A linha digitável da guia é opcional: DARF e GPS nem sempre "
                "trazem uma.",
    },
    "DEBITO_AUTOMATICO": {
        "rotulo": "Débito automático",
        "usa_conta": False, "usa_linha": False, "linha_obrigatoria": False,
        "dica": "Já sai sozinho da conta da empresa — não há conta de credor nem "
                "código de barras a informar.",
    },
}

# ---------------------------------------------------------------------------
# 2. OS DOCUMENTOS — as categorias são as do arquivo (`armazenamento.CATEGORIAS`)
# ---------------------------------------------------------------------------
DOCUMENTOS_ROTULOS = {
    "NOTA": "Nota fiscal", "BOLETO": "Boleto", "GUIA": "Guia",
    "FATURA": "Fatura da concessionária", "RECIBO": "Recibo / RPA assinado",
    "COMPROVANTE": "Comprovante", "CONTRATO": "Contrato", "PROPOSTA": "Proposta",
    "FOTO": "Foto", "OUTRO": "Outro documento",
}

# ---------------------------------------------------------------------------
# 3. PEDIR PAGAMENTO — os subtipos
#
# Cada subtipo diz o TIPO interno (T1…T14, que ninguém vê), as formas que fazem
# sentido para ele, como se chamam e quando se exigem o número e a data do
# documento, os campos próprios dele e os documentos que têm de vir junto.
#
# `onde` diz em que porta o subtipo aparece. Folha, financiamento e
# adiantamento a fornecedor são trabalho do escritório: não estão no portal.
#
# ⚠️ O QUE NÃO É SUBTIPO DAQUI, e tem caminho próprio (o enquadramento já
# recusa quem tenta por fora): locação (Locações — a parcela prevista do
# contrato), empreita (medição autorizada), fundo fixo e cartão (prestação).
# ---------------------------------------------------------------------------
_EMISSAO = {"nome": "data_emissao_doc", "rotulo": "Data de emissão", "tipo": "data"}
_REFERENCIA = {"nome": "mes_referencia", "rotulo": "Mês de referência", "tipo": "mes",
               "dica": "O mês a que a conta se refere (ex.: a fatura de outubro)."}

SUBTIPOS_PAGAMENTO: dict[str, dict[str, Any]] = {
    "NOTA_MATERIAL": {
        "rotulo": "Nota de material (NF-e)", "tipo": "T1_MATERIAL_NFE",
        "explicacao": "compra de material com nota fiscal eletrônica (DANFE)",
        "onde": ("portal", "erp"),
        "formas": ("BOLETO", "PIX", "TED"),
        "numero": {"rotulo": "Nº da nota fiscal", "obrigatorio": True},
        "emissao": {"obrigatorio": True},
        "campos": ({"nome": "chave_acesso", "rotulo": "Chave de acesso (44 dígitos)",
                    "tipo": "texto", "obrigatorio": False,
                    "dica": "Vem sozinha quando a nota é lida. Ajuda a casar com a "
                            "nota que a Receita mostra."},),
        "documentos": ({"categoria": "NOTA", "obrigatorio": True},
                       {"categoria": "BOLETO",
                        "obrigatorio_se": {"forma_pagamento": ["BOLETO"]}}),
    },
    "NOTA_SERVICO": {
        "rotulo": "Nota de serviço (NFS-e)", "tipo": "T2_SERVICO_NFSE",
        "explicacao": "serviço prestado por empresa, com nota fiscal de serviço",
        "onde": ("portal", "erp"),
        "formas": ("BOLETO", "PIX", "TED"),
        "numero": {"rotulo": "Nº da NFS-e", "obrigatorio": True},
        "emissao": {"obrigatorio": True},
        "campos": (),
        "documentos": ({"categoria": "NOTA", "obrigatorio": True},
                       {"categoria": "BOLETO",
                        "obrigatorio_se": {"forma_pagamento": ["BOLETO"]}}),
    },
    "FRETE": {
        "rotulo": "Frete (CT-e)", "tipo": "T3_FRETE_CTE",
        "explicacao": "transporte de carga, com conhecimento de transporte",
        "onde": ("portal", "erp"),
        "formas": ("BOLETO", "PIX", "TED"),
        "numero": {"rotulo": "Nº do CT-e", "obrigatorio": True},
        "emissao": {"obrigatorio": True},
        "campos": (),
        "documentos": ({"categoria": "NOTA", "obrigatorio": True,
                        "rotulo": "O CT-e (conhecimento de transporte)"},
                       {"categoria": "BOLETO",
                        "obrigatorio_se": {"forma_pagamento": ["BOLETO"]}}),
    },
    "PESSOA_FISICA": {
        "rotulo": "Serviço de pessoa física (RPA)", "tipo": "T6_SERVICO_PF_RPA",
        "explicacao": "serviço de autônomo, pago por recibo (RPA)",
        "onde": ("portal", "erp"),
        # Pessoa física não emite boleto: recebe na conta dela.
        "formas": ("PIX", "TED"),
        "numero": {"rotulo": "Nº do RPA", "obrigatorio": False},
        "emissao": {"obrigatorio": True, "rotulo": "Data do recibo"},
        "campos": (),
        "documentos": ({"categoria": "RECIBO", "obrigatorio": True},),
    },
    "CONCESSIONARIA": {
        "rotulo": "Conta de consumo (água, luz, telefone, internet)",
        "tipo": "T9_CONCESSIONARIA",
        "explicacao": "fatura de concessionária da obra ou do escritório",
        "onde": ("portal", "erp"),
        "formas": ("BOLETO", "DEBITO_AUTOMATICO", "PIX"),
        "numero": {"rotulo": "Nº da fatura ou da unidade consumidora",
                   "obrigatorio": False},
        "emissao": None,                       # a referência é o mês, não o dia
        "campos": (dict(_REFERENCIA, obrigatorio=True),),
        "documentos": ({"categoria": "FATURA", "obrigatorio": True},),
    },
    "GUIA_TRIBUTO": {
        "rotulo": "Guia de imposto ou taxa (DARF, GPS, DAM, FGTS)",
        "tipo": "T8_TRIBUTO_GUIA",
        "explicacao": "imposto, taxa ou contribuição pago por guia",
        "onde": ("portal", "erp"),
        "formas": ("GUIA",),
        "numero": {"rotulo": "Nº da guia", "obrigatorio": False},
        "emissao": None,
        "campos": (dict(_REFERENCIA, obrigatorio=True, rotulo="Competência",
                        dica="O mês a que o imposto se refere."),),
        "documentos": ({"categoria": "GUIA", "obrigatorio": True},),
    },
    "SEM_NOTA": {
        "rotulo": "Despesa sem nota fiscal (exceção)", "tipo": "T14_EXCECAO_SEM_NOTA",
        "explicacao": "quando não há nota — vai marcada como indedutível",
        "onde": ("portal", "erp"),
        "formas": ("PIX", "TED", "BOLETO"),
        "numero": {"rotulo": "Nº do recibo", "obrigatorio": False},
        "emissao": {"obrigatorio": False, "rotulo": "Data do recibo"},
        "campos": ({"nome": "justificativa_excecao", "rotulo": "Por que não há nota",
                    "tipo": "texto_longo", "obrigatorio": True, "minimo": 15,
                    "dica": "Despesa sem nota custa até 34% a mais em imposto. "
                            "Diga por que não houve nota."},),
        "documentos": ({"categoria": ("RECIBO", "COMPROVANTE"), "obrigatorio": True,
                        "rotulo": "Recibo ou comprovante"},),
    },
    # ---------------------------------------------------- só no ERP
    "FOLHA_ENCARGOS": {
        "rotulo": "Folha de pagamento ou encargos", "tipo": "T7_FOLHA_ENCARGOS",
        "explicacao": "salários, rescisões e encargos da folha",
        "onde": ("erp",),
        "formas": ("PIX", "TED", "GUIA", "BOLETO"),
        "numero": {"rotulo": "Nº do documento", "obrigatorio": False},
        "emissao": None,
        "campos": (dict(_REFERENCIA, obrigatorio=True, rotulo="Competência da folha"),),
        "documentos": (),
    },
    "FINANCIAMENTO": {
        "rotulo": "Parcela de financiamento ou empréstimo", "tipo": "T13_FINANCIAMENTO",
        "explicacao": "parcela de contrato de financiamento",
        "onde": ("erp",),
        "formas": ("BOLETO", "DEBITO_AUTOMATICO", "TED"),
        "numero": {"rotulo": "Nº do contrato ou da parcela", "obrigatorio": False},
        "emissao": None,
        "campos": (),
        "documentos": ({"categoria": "BOLETO",
                        "obrigatorio_se": {"forma_pagamento": ["BOLETO"]}},),
    },
    "ADIANTAMENTO_FORNECEDOR": {
        "rotulo": "Adiantamento a fornecedor", "tipo": "T11_ADIANTAMENTO",
        "explicacao": "pagamento antes da entrega — a nota vem depois",
        "onde": ("erp",),
        "formas": ("PIX", "TED", "BOLETO"),
        "numero": {"rotulo": "Nº da proposta ou do pedido", "obrigatorio": False},
        "emissao": None,
        "campos": (),
        "documentos": ({"categoria": "BOLETO",
                        "obrigatorio_se": {"forma_pagamento": ["BOLETO"]}},),
    },
}

# O que a leitura do documento diz → o subtipo que se sugere. A pessoa confere;
# a sugestão só poupa o primeiro toque.
# As chaves são as de `documentos/leitor.TIPOS`.
SUBTIPO_PELO_DOCUMENTO = {
    "NFE": "NOTA_MATERIAL", "NFCE": "NOTA_MATERIAL",
    "NFSE": "NOTA_SERVICO", "CTE": "FRETE", "RPA": "PESSOA_FISICA",
    "GUIA": "GUIA_TRIBUTO", "FATURA_CONCESSIONARIA": "CONCESSIONARIA",
    "RECIBO": "SEM_NOTA",
}
# E a etiqueta com que o arquivo lido é guardado — é por ela que se confere se
# o documento exigido veio.
CATEGORIA_PELO_DOCUMENTO = {
    "NFE": "NOTA", "NFCE": "NOTA", "NFSE": "NOTA", "CTE": "NOTA",
    "BOLETO": "BOLETO", "GUIA": "GUIA", "FATURA_CONCESSIONARIA": "FATURA",
    "RECIBO": "RECIBO", "RPA": "RECIBO", "COMPROVANTE": "COMPROVANTE",
    "CONTRATO": "CONTRATO", "ORCAMENTO": "PROPOSTA",
    "PRESTACAO_CONTAS": "COMPROVANTE",
}

# Os campos COMUNS a todo pedido de pagamento, na ordem da tela. O que varia por
# subtipo (número, emissão, referência, justificativa) entra no meio, por
# `campos_do_pagamento`.
CAMPOS_COMUNS_PAGAMENTO_ANTES = (
    {"nome": "subtipo", "rotulo": "O que está sendo pago", "tipo": "lista",
     "obrigatorio": True},
    {"nome": "fornecedor_id", "rotulo": "Credor (quem recebe)", "tipo": "credor",
     "obrigatorio": True,
     "dica": "Não achou? Cadastre na hora — o CNPJ/CPF basta para começar."},
    {"nome": "forma_pagamento", "rotulo": "Forma de pagamento", "tipo": "lista",
     "obrigatorio": True, "dica": "Só aparecem as formas que servem a este tipo."},
    {"nome": "fornecedor_conta_id", "rotulo": "Conta do credor", "tipo": "conta",
     "visivel_se": {"forma_pagamento": ["PIX", "TED"]},
     "obrigatorio_se": {"forma_pagamento": ["PIX", "TED"]},
     "dica": "Sem conta cadastrada, cadastre aqui: ela entra pendente e o "
             "financeiro homologa antes de pagar."},
)
CAMPOS_COMUNS_PAGAMENTO_DEPOIS = (
    {"nome": "descricao", "rotulo": "O que foi comprado ou contratado",
     "tipo": "texto_longo", "obrigatorio": True, "minimo": 5},
    {"nome": "valor_bruto", "rotulo": "Valor do documento", "tipo": "dinheiro",
     "obrigatorio": True},
    {"nome": "obra_id", "rotulo": "Obra", "tipo": "obra", "obrigatorio": True,
     "dica": "No ERP o valor pode ser dividido entre obras (rateio)."},
    {"nome": "categoria_id", "rotulo": "Conta do plano (categoria)", "tipo": "categoria",
     "obrigatorio": True, "dica": "Só aparecem as contas que aceitam este tipo."},
    {"nome": "parcelas", "rotulo": "Vencimento e valor (uma linha por parcela)",
     "tipo": "parcelas", "obrigatorio": True},
    {"nome": "linha_digitavel", "rotulo": "Linha digitável (em cada parcela)",
     "tipo": "texto", "dentro_de": "parcelas",
     "visivel_se": {"forma_pagamento": ["BOLETO", "GUIA"]},
     "obrigatorio_se": {"forma_pagamento": ["BOLETO"]}},
)


def campos_do_pagamento(chave: str) -> list[dict[str, Any]]:
    """Os campos de um subtipo, já na ordem da tela, com a obrigatoriedade."""
    st = SUBTIPOS_PAGAMENTO[chave]
    meio: list[dict[str, Any]] = []
    if st.get("numero"):
        meio.append({"nome": "documento_numero", "tipo": "texto",
                     "rotulo": st["numero"]["rotulo"],
                     "obrigatorio": bool(st["numero"].get("obrigatorio"))})
    if st.get("emissao"):
        meio.append({**_EMISSAO, "rotulo": st["emissao"].get("rotulo", _EMISSAO["rotulo"]),
                     "obrigatorio": bool(st["emissao"].get("obrigatorio"))})
    meio.extend(dict(c) for c in st.get("campos", ()))
    return ([dict(c) for c in CAMPOS_COMUNS_PAGAMENTO_ANTES] + meio
            + [dict(c) for c in CAMPOS_COMUNS_PAGAMENTO_DEPOIS])


# ---------------------------------------------------------------------------
# 4. FUNDO FIXO — prestação de contas
#
# Duas naturezas, e o que muda entre elas é o PERÍODO: a prestação de um
# adiantamento cobre o período em que o dinheiro adiantado foi gasto; o
# reembolso é de despesas soltas. O favorecido é sempre quem presta contas, na
# conta que ELA cadastrou (Meu cadastro) — não há credor a escolher.
#
# ⚠️ O COMPROVANTE DE CADA DESPESA CONTINUA NÃO-BLOQUEANTE, como já era: falta
# de comprovante vira crítica na conferência, não recusa. É a regra em vigor
# (`prestacao.criticar`), e a decisão do dono de 07/09/2026 trata a falta como
# "problema de prestação de contas". Está no mapa como pergunta para ele.
# ---------------------------------------------------------------------------
SUBTIPOS_FUNDO_FIXO: dict[str, dict[str, Any]] = {
    "REEMBOLSO": {
        "rotulo": "Reembolso — gastei do meu bolso",
        "explicacao": "despesas pagas com dinheiro seu, que a empresa devolve",
        "periodo_obrigatorio": False,
    },
    "ADIANTAMENTO": {
        "rotulo": "Prestação de contas de adiantamento",
        "explicacao": "o que foi gasto do dinheiro que a empresa adiantou",
        "periodo_obrigatorio": True,
    },
}

CAMPOS_FUNDO_FIXO = (
    {"nome": "fundo_fixo_tipo", "rotulo": "Que prestação é esta", "tipo": "lista",
     "obrigatorio": True},
    {"nome": "periodo_inicio", "rotulo": "Período — de", "tipo": "data",
     "obrigatorio_se": {"fundo_fixo_tipo": ["ADIANTAMENTO"]}},
    {"nome": "periodo_fim", "rotulo": "Período — até", "tipo": "data",
     "obrigatorio_se": {"fundo_fixo_tipo": ["ADIANTAMENTO"]}},
    {"nome": "itens", "rotulo": "As despesas (uma linha por comprovante)",
     "tipo": "linhas", "obrigatorio": True},
    {"nome": "data_despesa", "rotulo": "Data da despesa", "tipo": "data",
     "dentro_de": "itens", "obrigatorio": True},
    {"nome": "descricao", "rotulo": "O que foi", "tipo": "texto",
     "dentro_de": "itens", "obrigatorio": True, "minimo": 4},
    {"nome": "estabelecimento", "rotulo": "Onde (estabelecimento)", "tipo": "texto",
     "dentro_de": "itens", "obrigatorio": False},
    {"nome": "valor", "rotulo": "Valor", "tipo": "dinheiro",
     "dentro_de": "itens", "obrigatorio": True},
    {"nome": "obra_id", "rotulo": "Obra", "tipo": "obra",
     "dentro_de": "itens", "obrigatorio": True},
    {"nome": "anexo_id", "rotulo": "Comprovante", "tipo": "arquivo",
     "dentro_de": "itens", "obrigatorio": False,
     "dica": "Esperado em toda linha. Sem ele a despesa vai marcada para o "
             "financeiro conferir."},
)

# ---------------------------------------------------------------------------
# 5. EMPREITA — o cadastro do contrato e a medição
#
# ⚑ O CONTRATO POR MEDIÇÃO PASSA A EXIGIR A PLANILHA DE SERVIÇOS (27/11/2026).
# Antes dava para cadastrar "por medição" só com o valor total — e aí não havia
# serviço, unidade nem preço para a medição consumir: a obra media "um valor", e
# o saldo por serviço, que é o que pega medir duas vezes o mesmo metro, não
# existia. Um serviço só continua possível: é a planilha de uma linha.
# Contrato antigo sem planilha continua sendo medido por valor.
# ---------------------------------------------------------------------------
SUBTIPOS_CONTRATO: dict[str, dict[str, Any]] = {
    "MEDICAO": {
        "rotulo": "Por medição de serviço",
        "explicacao": "paga o que foi executado: quantidade × preço de cada serviço",
    },
    "PARCELAS": {
        "rotulo": "Preço fechado, em parcelas",
        "explicacao": "serviço fechado por um valor, pago em parcelas combinadas",
    },
}

CAMPOS_CONTRATO = (
    {"nome": "modo", "rotulo": "Como o contrato é pago", "tipo": "lista",
     "obrigatorio": True},
    {"nome": "obra_id", "rotulo": "Obra", "tipo": "obra", "obrigatorio": True},
    {"nome": "fornecedor_id", "rotulo": "Empreiteiro (empresa ou pessoa)",
     "tipo": "credor", "obrigatorio": True},
    {"nome": "objeto", "rotulo": "Serviço contratado", "tipo": "texto_longo",
     "obrigatorio": True, "minimo": 8},
    {"nome": "itens", "rotulo": "Planilha de serviços", "tipo": "linhas",
     "visivel_se": {"modo": ["MEDICAO"]}, "obrigatorio_se": {"modo": ["MEDICAO"]}},
    {"nome": "descricao", "rotulo": "Serviço", "tipo": "texto", "dentro_de": "itens",
     "obrigatorio": True},
    {"nome": "unidade", "rotulo": "Unidade (m², m, un…)", "tipo": "texto",
     "dentro_de": "itens", "obrigatorio": True},
    {"nome": "quantidade", "rotulo": "Quantidade contratada", "tipo": "numero",
     "dentro_de": "itens", "obrigatorio": True},
    {"nome": "preco_unitario", "rotulo": "Preço unitário", "tipo": "dinheiro",
     "dentro_de": "itens", "obrigatorio": True},
    {"nome": "valor_total", "rotulo": "Valor total do contrato", "tipo": "dinheiro",
     "visivel_se": {"modo": ["PARCELAS"]}, "obrigatorio_se": {"modo": ["PARCELAS"]}},
    {"nome": "parcelas_previstas", "rotulo": "Em quantas parcelas", "tipo": "numero",
     "visivel_se": {"modo": ["PARCELAS"]}, "obrigatorio_se": {"modo": ["PARCELAS"]}},
    {"nome": "data_inicio", "rotulo": "Início", "tipo": "data", "obrigatorio": True},
    {"nome": "data_fim", "rotulo": "Término previsto", "tipo": "data",
     "obrigatorio": False},
    {"nome": "retencao_garantia_pct", "rotulo": "Retenção de garantia (%)",
     "tipo": "percentual", "obrigatorio": False,
     "dica": "Parte de cada medição que fica retida até o fim do serviço."},
    {"nome": "exige_foto", "rotulo": "Exige foto em cada medição", "tipo": "sim_nao",
     "obrigatorio": True},
    {"nome": "observacoes", "rotulo": "Observações", "tipo": "texto_longo",
     "obrigatorio": False},
)

DOCUMENTOS_CONTRATO = ({"categoria": ("CONTRATO", "PROPOSTA"), "obrigatorio": False,
                        "rotulo": "Contrato assinado (quando NÃO é gerado pelo sistema)"},)

CAMPOS_MEDICAO = (
    {"nome": "contrato_id", "rotulo": "Contrato", "tipo": "contrato",
     "obrigatorio": True, "dica": "Só os contratos vigentes das suas obras."},
    {"nome": "periodo_inicio", "rotulo": "Período medido — de", "tipo": "data",
     "obrigatorio": True},
    {"nome": "periodo_fim", "rotulo": "Período medido — até", "tipo": "data",
     "obrigatorio": True},
    {"nome": "itens", "rotulo": "Quanto foi executado de cada serviço",
     "tipo": "linhas", "visivel_se": {"_contrato": ["PLANILHA"]},
     "obrigatorio_se": {"_contrato": ["PLANILHA"]}},
    {"nome": "valor_medido", "rotulo": "Valor desta medição", "tipo": "dinheiro",
     "visivel_se": {"_contrato": ["SEM_PLANILHA", "PARCELAS"]},
     "obrigatorio_se": {"_contrato": ["SEM_PLANILHA", "PARCELAS"]}},
    {"nome": "observacao", "rotulo": "Observação", "tipo": "texto_longo",
     "obrigatorio": False},
)

DOCUMENTOS_MEDICAO = ({"categoria": "FOTO", "obrigatorio_se": {"exige_foto": ["1"]},
                       "rotulo": "Foto do serviço executado"},)

CAMPOS_CANCELAR = (
    {"nome": "motivo", "rotulo": "Por que cancelar", "tipo": "texto_longo",
     "obrigatorio": True, "minimo": 5},
)


# ---------------------------------------------------------------------------
# A REGRA DE CONDIÇÃO — uma função só, usada por todos os `conferir_*`
# ---------------------------------------------------------------------------
def _casa(condicao: Optional[dict[str, list[str]]], dados: dict[str, Any]) -> bool:
    if not condicao:
        return True
    for campo, valores in condicao.items():
        atual = str(dados.get(campo) if dados.get(campo) is not None else "").upper()
        if atual not in {str(v).upper() for v in valores}:
            return False
    return True


def visivel(campo: dict[str, Any], dados: dict[str, Any]) -> bool:
    return _casa(campo.get("visivel_se"), dados)


def obrigatorio(campo: dict[str, Any], dados: dict[str, Any]) -> bool:
    if not visivel(campo, dados):
        return False
    if campo.get("obrigatorio_se"):
        return _casa(campo["obrigatorio_se"], dados)
    return bool(campo.get("obrigatorio"))


def _vazio(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, (list, tuple, dict)):
        return len(v) == 0
    return str(v).strip() == ""


def _dec(v: Any) -> Optional[Decimal]:
    if v is None or str(v).strip() == "":
        return None
    t = str(v).strip().replace("R$", "").replace(" ", "")
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


def _faltando(campos, dados: dict[str, Any], raiz: dict[str, Any]) -> list[str]:
    """Os rótulos dos campos obrigatórios que vieram em branco."""
    faltam = []
    for c in campos:
        if c.get("dentro_de"):
            continue
        if obrigatorio(c, raiz) and _vazio(dados.get(c["nome"])):
            faltam.append(c["rotulo"])
        elif c.get("minimo") and not _vazio(dados.get(c["nome"])) \
                and len(str(dados.get(c["nome"])).strip()) < c["minimo"]:
            faltam.append(f"{c['rotulo']} (mínimo {c['minimo']} letras)")
    return faltam


def _documentos_que_faltam(exigidos, dados: dict[str, Any],
                           declarados: Optional[list[str]]) -> list[str]:
    if declarados is None:
        return []                     # quem chamou não declara anexos: não se confere
    tem = {str(x or "").upper() for x in declarados}
    faltam = []
    for d in exigidos:
        cats = d["categoria"] if isinstance(d["categoria"], tuple) else (d["categoria"],)
        exige = (_casa(d["obrigatorio_se"], dados) if d.get("obrigatorio_se")
                 else bool(d.get("obrigatorio")))
        if exige and not (tem & set(cats)):
            faltam.append(d.get("rotulo") or DOCUMENTOS_ROTULOS.get(cats[0], cats[0]))
    return faltam


def _recusar(faltam: list[str], docs: list[str]) -> None:
    partes = []
    if faltam:
        partes.append("Falta preencher: " + "; ".join(faltam) + ".")
    if docs:
        partes.append("Falta anexar: " + "; ".join(docs) + ".")
    if partes:
        raise ErroValidacao(" ".join(partes))


# ---------------------------------------------------------------------------
# 6. AS CONFERÊNCIAS DO SERVIDOR — as duas portas passam por aqui
# ---------------------------------------------------------------------------
def limpar_pela_forma(dados: dict[str, Any]) -> dict[str, Any]:
    """Descarta o que a forma de pagamento não usa. Vale para TODO lançamento
    das telas, com ou sem subtipo: é a regra mais básica da coerência."""
    forma = FORMAS.get(str(dados.get("forma_pagamento") or "").upper())
    if forma is None:
        return dados
    d = dict(dados)
    if not forma["usa_conta"]:
        d["fornecedor_conta_id"] = None
    if not forma["usa_linha"]:
        d["parcelas"] = [{**p, "linha_digitavel": ""} for p in (d.get("parcelas") or [])]
    return d


def conferir_pagamento(dados: dict[str, Any], *, porta: str,
                       documentos: Optional[list[str]] = None) -> dict[str, Any]:
    """Confere um pedido de pagamento contra o subtipo e devolve os dados LIMPOS,
    já com o `tipo` interno. `porta` é "portal" ou "erp".

    `documentos` é a lista de categorias dos arquivos que vêm junto. `None`
    quer dizer "esta porta não declara anexos" — aí não se confere documento.
    """
    chave = str(dados.get("subtipo") or "").strip().upper()
    st = SUBTIPOS_PAGAMENTO.get(chave)
    if st is None or porta not in st["onde"]:
        raise ErroValidacao("Escolha o que está sendo pago.")
    forma = str(dados.get("forma_pagamento") or "").strip().upper()
    if forma not in st["formas"]:
        permitidas = ", ".join(FORMAS[f]["rotulo"] for f in st["formas"])
        raise ErroValidacao(
            f"{st['rotulo']} não se paga por "
            f"{FORMAS.get(forma, {}).get('rotulo', forma or 'esta forma')}. "
            f"Formas que servem: {permitidas}.")
    d = limpar_pela_forma({**dados, "subtipo": chave, "forma_pagamento": forma})
    campos = campos_do_pagamento(chave)
    # descarta o que este subtipo não mostra (ex.: justificativa num boleto de
    # nota, chave de acesso numa guia)
    nomes = {c["nome"] for c in campos}
    for opcional in ("documento_numero", "data_emissao_doc", "chave_acesso",
                     "mes_referencia", "justificativa_excecao"):
        if opcional not in nomes:
            d.pop(opcional, None)
    # a obra pode vir solta (portal) ou no rateio (ERP)
    if _vazio(d.get("obra_id")) and d.get("rateios"):
        d["obra_id"] = (d["rateios"][0] or {}).get("obra_id")
    faltam = _faltando(campos, d, d)
    # parcelas: vencimento e valor em todas; linha digitável quando a forma pede
    parcelas = d.get("parcelas") or []
    for i, p in enumerate(parcelas, start=1):
        if _vazio(p.get("vencimento")) or not (_dec(p.get("valor")) or 0) > 0:
            faltam.append(f"vencimento e valor da parcela {i}")
        if FORMAS[forma]["linha_obrigatoria"] and _vazio(p.get("linha_digitavel")):
            faltam.append(f"linha digitável da parcela {i}")
    chave_nfe = "".join(ch for ch in str(d.get("chave_acesso") or "") if ch.isdigit())
    if chave_nfe and len(chave_nfe) != 44:
        faltam.append("a chave de acesso tem 44 dígitos")
    _recusar(faltam, _documentos_que_faltam(st["documentos"], d, documentos))
    d["tipo"] = st["tipo"]
    if d.get("mes_referencia"):
        d["competencia"] = str(d["mes_referencia"])[:7]
    if chave_nfe:
        d["chave_acesso"] = chave_nfe
    return d


def conferir_fundo_fixo(dados: dict[str, Any]) -> dict[str, Any]:
    if (dados.get("modalidade") or "FUNDO_FIXO").upper() != "FUNDO_FIXO":
        return dict(dados)             # a fatura de cartão tem regra própria
    tipo = str(dados.get("fundo_fixo_tipo") or "").strip().upper()
    if tipo not in SUBTIPOS_FUNDO_FIXO:
        raise ErroValidacao("Diga se é reembolso ou prestação de adiantamento.")
    d = {**dados, "fundo_fixo_tipo": tipo}
    faltam = _faltando(CAMPOS_FUNDO_FIXO, d, d)
    hoje = date.today().isoformat()
    for i, it in enumerate(d.get("itens") or [], start=1):
        for c in CAMPOS_FUNDO_FIXO:
            if c.get("dentro_de") == "itens" and c.get("obrigatorio") \
                    and _vazio(it.get(c["nome"])):
                faltam.append(f"{c['rotulo'].lower()} da despesa {i}")
        if not _vazio(it.get("valor")) and not (_dec(it.get("valor")) or 0) > 0:
            faltam.append(f"valor da despesa {i}")
    if d.get("periodo_inicio") and d.get("periodo_fim") \
            and str(d["periodo_inicio"]) > str(d["periodo_fim"]):
        faltam.append("o período começa depois de terminar")
    if d.get("periodo_fim") and str(d["periodo_fim"])[:10] > hoje:
        faltam.append("o período não pode terminar no futuro")
    _recusar(faltam, [])
    return d


def conferir_contrato(dados: dict[str, Any],
                      documentos: Optional[list[str]] = None) -> dict[str, Any]:
    modo = str(dados.get("modo") or "").strip().upper()
    if modo not in SUBTIPOS_CONTRATO:
        raise ErroValidacao("Diga como o contrato é pago: por medição ou em parcelas.")
    d = {**dados, "modo": modo}
    # o que o modo não usa é descartado — senão um valor total esquecido num
    # contrato por medição passaria por cima da soma da planilha
    if modo == "MEDICAO":
        d.pop("valor_total", None)
        d.pop("parcelas_previstas", None)
        d["itens"] = [it for it in (d.get("itens") or [])
                      if not all(_vazio(it.get(k)) for k in
                                 ("descricao", "unidade", "quantidade", "preco_unitario"))]
    else:
        d["itens"] = []
        d.pop("quantidade", None)
        d.pop("preco_unitario", None)
        d.pop("unidade", None)
    if _vazio(d.get("exige_foto")):
        d["exige_foto"] = True
    faltam = _faltando(CAMPOS_CONTRATO, d, d)
    for i, it in enumerate(d.get("itens") or [], start=1):
        for c in CAMPOS_CONTRATO:
            if c.get("dentro_de") == "itens" and _vazio(it.get(c["nome"])):
                faltam.append(f"{c['rotulo'].lower()} do serviço {i}")
        for k in ("quantidade", "preco_unitario"):
            if not _vazio(it.get(k)) and not (_dec(it.get(k)) or 0) > 0:
                faltam.append(f"{k.replace('_', ' ')} do serviço {i} maior que zero")
    if modo == "PARCELAS":
        try:
            if int(str(d.get("parcelas_previstas") or "0")) < 1:
                faltam.append("em quantas parcelas (ao menos 1)")
        except ValueError:
            faltam.append("em quantas parcelas (um número)")
    if d.get("data_inicio") and d.get("data_fim") \
            and str(d["data_fim"]) < str(d["data_inicio"]):
        faltam.append("o término é antes do início")
    _recusar(faltam, _documentos_que_faltam(DOCUMENTOS_CONTRATO, d, documentos))
    return d


def jeito_do_contrato(modo: str, tem_planilha: bool) -> str:
    """Como se mede este contrato: pela planilha, por valor (contrato antigo,
    sem planilha) ou pela parcela."""
    if (modo or "").upper() == "PARCELAS":
        return "PARCELAS"
    return "PLANILHA" if tem_planilha else "SEM_PLANILHA"


def conferir_medicao(dados: dict[str, Any], *, jeito: str,
                     contrato_id: Optional[int] = None) -> dict[str, Any]:
    """`contrato_id` vem do endereço da rota (`/empreitas/<id>/medicoes`), não
    do formulário — por isso entra à parte."""
    d = {**dados, "_contrato": jeito}
    if contrato_id:
        d["contrato_id"] = contrato_id
    if jeito == "PLANILHA":
        d.pop("valor_medido", None)
        d["itens"] = [l for l in (d.get("itens") or []) if (_dec(l.get("quantidade")) or 0) > 0]
    else:
        d["itens"] = []
    faltam = _faltando(CAMPOS_MEDICAO, d, d)
    if d.get("periodo_inicio") and d.get("periodo_fim") \
            and str(d["periodo_fim"]) < str(d["periodo_inicio"]):
        faltam.append("o período termina antes de começar")
    _recusar(faltam, [])
    d.pop("_contrato", None)
    return d


# ---------------------------------------------------------------------------
# 7. O CATÁLOGO PARA AS TELAS, e o MAPA para o dono revisar
# ---------------------------------------------------------------------------
def catalogo(porta: str) -> dict[str, Any]:
    """Tudo que uma tela precisa para desenhar os formulários — sem lógica nela."""
    return {
        "formas": {k: dict(v) for k, v in FORMAS.items()},
        "documentos": dict(DOCUMENTOS_ROTULOS),
        "pagamento": [
            {"chave": k, **{x: v[x] for x in ("rotulo", "tipo", "explicacao")},
             "formas": list(v["formas"]), "campos": campos_do_pagamento(k),
             "documentos": [_doc_para_tela(d) for d in v["documentos"]]}
            for k, v in SUBTIPOS_PAGAMENTO.items() if porta in v["onde"]],
        "subtipo_pelo_documento": dict(SUBTIPO_PELO_DOCUMENTO),
        "categoria_pelo_documento": dict(CATEGORIA_PELO_DOCUMENTO),
        "fundo_fixo": {"subtipos": [{"chave": k, **v} for k, v in SUBTIPOS_FUNDO_FIXO.items()],
                       "campos": [dict(c) for c in CAMPOS_FUNDO_FIXO]},
        "contrato": {"subtipos": [{"chave": k, **v} for k, v in SUBTIPOS_CONTRATO.items()],
                     "campos": [dict(c) for c in CAMPOS_CONTRATO],
                     "documentos": [_doc_para_tela(d) for d in DOCUMENTOS_CONTRATO]},
        "medicao": {"campos": [dict(c) for c in CAMPOS_MEDICAO],
                    "documentos": [_doc_para_tela(d) for d in DOCUMENTOS_MEDICAO]},
        "cancelar": {"campos": [dict(c) for c in CAMPOS_CANCELAR]},
    }


def _doc_para_tela(d: dict[str, Any]) -> dict[str, Any]:
    cats = d["categoria"] if isinstance(d["categoria"], tuple) else (d["categoria"],)
    return {"categorias": list(cats),
            "rotulo": d.get("rotulo") or DOCUMENTOS_ROTULOS.get(cats[0], cats[0]),
            "obrigatorio": bool(d.get("obrigatorio")),
            "obrigatorio_se": d.get("obrigatorio_se") or {}}


def _cond_txt(cond: Optional[dict[str, list[str]]]) -> str:
    if not cond:
        return ""
    partes = []
    for campo, valores in cond.items():
        nomes = [FORMAS[v]["rotulo"] if v in FORMAS else
                 SUBTIPOS_FUNDO_FIXO.get(v, {}).get("rotulo")
                 or SUBTIPOS_CONTRATO.get(v, {}).get("rotulo")
                 or {"PLANILHA": "contrato com planilha",
                     "SEM_PLANILHA": "contrato antigo, sem planilha",
                     "PARCELAS": "contrato em parcelas", "1": "sim"}.get(v, v)
                 for v in valores]
        partes.append(" ou ".join(nomes))
    return "; ".join(partes)


def _linha_campo(c: dict[str, Any]) -> str:
    if c.get("obrigatorio_se"):
        obr = f"só se {_cond_txt(c['obrigatorio_se'])}"
    else:
        obr = "sim" if c.get("obrigatorio") else "não"
    vis = f"só se {_cond_txt(c['visivel_se'])}" if c.get("visivel_se") else "sempre"
    nome = ("↳ " if c.get("dentro_de") else "") + c["rotulo"]
    if c.get("minimo"):
        obr += f" (mín. {c['minimo']} letras)"
    return f"| {nome} | {vis} | {obr} |"


def _tabela(campos) -> list[str]:
    return ["| Campo | Aparece | Obrigatório |", "|---|---|---|"] + \
        [_linha_campo(c) for c in campos]


def _docs_txt(docs) -> str:
    if not docs:
        return "nenhum exigido"
    partes = []
    for d in docs:
        t = _doc_para_tela(d)
        if t["obrigatorio_se"]:
            partes.append(f"{t['rotulo']} (só se {_cond_txt(t['obrigatorio_se'])})")
        elif t["obrigatorio"]:
            partes.append(f"{t['rotulo']} (obrigatório)")
        else:
            partes.append(f"{t['rotulo']} (opcional)")
    return "; ".join(partes)


def mapa_em_markdown() -> str:
    """O mapa que o dono revisa. GERADO daqui — não se edita à mão."""
    L: list[str] = [
        "# Pleitos do Financeiro — tipos, subtipos, campos e condições",
        "",
        "> **Gerado pelo sistema a partir de `core/titulos/pleitos.py`.** Não edite",
        "> este arquivo à mão: mude o catálogo e rode",
        "> `python -m app.apps.erp.core.titulos.pleitos` — há teste que recusa o",
        "> arquivo diferente do código. É o mesmo catálogo que o portal desenha, que",
        "> a tela Lançar do ERP lê e que o servidor confere nas duas portas.",
        "",
        "## 1. Formas de pagamento — o que cada uma pede",
        "",
        "| Forma | Conta do credor | Linha digitável |",
        "|---|---|---|",
    ]
    for f in FORMAS.values():
        linha = ("obrigatória" if f["linha_obrigatoria"]
                 else "opcional" if f["usa_linha"] else "não aparece")
        L.append(f"| {f['rotulo']} | {'obrigatória' if f['usa_conta'] else 'não aparece'} "
                 f"| {linha} |")
    L += ["", "Campo que não aparece para a forma escolhida é **descartado** pelo "
          "servidor — nas duas portas.", "",
          "## 2. Pedir pagamento", "",
          "Campos que valem para todos os subtipos, na ordem da tela (os do "
          "subtipo entram depois de \"Conta do credor\"):", ""]
    L += _tabela(CAMPOS_COMUNS_PAGAMENTO_ANTES + CAMPOS_COMUNS_PAGAMENTO_DEPOIS)
    L += ["", "### Os subtipos", "",
          "| Subtipo | Onde | Formas que servem | Nº do documento | Data | Campos próprios | Documentos |",
          "|---|---|---|---|---|---|---|"]
    for k, st in SUBTIPOS_PAGAMENTO.items():
        onde = " e ".join("portal" if o == "portal" else "ERP" for o in st["onde"])
        formas = ", ".join(FORMAS[f]["rotulo"] for f in st["formas"])
        num = (f"{st['numero']['rotulo']} — "
               f"{'obrigatório' if st['numero']['obrigatorio'] else 'opcional'}"
               if st.get("numero") else "—")
        emi = (f"{st['emissao'].get('rotulo', 'Data de emissão')} — "
               f"{'obrigatória' if st['emissao'].get('obrigatorio') else 'opcional'}"
               if st.get("emissao") else "não aparece")
        proprios = "; ".join(
            f"{c['rotulo']} ({'obrigatório' if c.get('obrigatorio') else 'opcional'})"
            for c in st.get("campos", ())) or "—"
        L.append(f"| **{st['rotulo']}** | {onde} | {formas} | {num} | {emi} | "
                 f"{proprios} | {_docs_txt(st['documentos'])} |")
    L += ["", "## 3. Fundo fixo (prestação de contas)", "",
          "| Que prestação | O que é | Período |", "|---|---|---|"]
    for v in SUBTIPOS_FUNDO_FIXO.values():
        L.append(f"| **{v['rotulo']}** | {v['explicacao']} | "
                 f"{'obrigatório' if v['periodo_obrigatorio'] else 'opcional'} |")
    L += ["", "O favorecido é sempre quem presta contas, na chave Pix do cadastro "
          "dela. Não há credor a escolher.", ""]
    L += _tabela(CAMPOS_FUNDO_FIXO)
    L += ["", "## 4. Cadastro de contrato de empreita", "",
          "| Como é pago | O que é |", "|---|---|"]
    for v in SUBTIPOS_CONTRATO.values():
        L.append(f"| **{v['rotulo']}** | {v['explicacao']} |")
    L += [""] + _tabela(CAMPOS_CONTRATO)
    L += ["", f"Documentos: {_docs_txt(DOCUMENTOS_CONTRATO)}.", "",
          "O contrato nasce **aguardando aval** — quem tem alçada pelo valor aprova "
          "no ERP, e só então ele pode ser medido.", ""]
    from app.apps.erp.core.titulos import contrato_assinatura as ca
    L += ["### O empreiteiro, pelo documento", "",
          "No alto do bloco, joga-se **um documento da pessoa (RG, CNH, CPF) e o comprovante** (pessoa física) ou o "
          "**Cartão CNPJ** (empresa): a leitura preenche os campos, a pessoa confere, e o "
          "documento fica arquivado no cadastro do empreiteiro. Quem já está no "
          "cadastro é procurado pelo nome ou CPF/CNPJ e o bloco abre preenchido.", "",
          "| Campo | Aparece | Obrigatório |", "|---|---|---|",
          "| Pessoa física / Empresa | sempre | sim |",
          "| CPF ou CNPJ, nome | sempre | sim |",
          "| Endereço (CEP, rua, número, complemento, bairro, cidade, UF) | sempre | não (entra no contrato) |",
          "| WhatsApp, e-mail | sempre | ver abaixo |", ""]
    L += ["### Contrato gerado pelo sistema e assinado à distância", "",
          "No cadastro, escolhe-se: **gerar o contrato pelo sistema e colher as "
          "assinaturas**, ou **já tenho o contrato assinado** (aí o portal exige o "
          "documento anexado).", "",
          "| Campo | Aparece | Obrigatório |", "|---|---|---|",
          "| Quem assina pela empresa (um colaborador) | só se gerar | sim — com telefone ou e-mail no cadastro de colaboradores |",
          "| WhatsApp e e-mail do empreiteiro | sempre, no bloco do empreiteiro | um dos dois, se gerar (é por onde chega o link) |"]
    L += [f"| {p['rotulo']} | só se gerar | não (padrão {p['padrao']}) |"
          for p in ca.PARAMETROS.values()]
    L += ["| Condição a mais, do seu jeito (a IA revisa, você aceita ou não) | só se gerar | não |",
          "", "Cláusulas para marcar (as marcadas por padrão já vêm ligadas):", "",
          "| Cláusula | Para que serve | Vem marcada | Pede |", "|---|---|---|---|"]
    L += [f"| {o['rotulo']} | {o['explicacao']} | {'sim' if o['padrao'] else 'não'} | "
          f"{', '.join(c['rotulo'] for c in (o.get('campos') or {}).values()) or '—'} |"
          for o in ca.OPCIONAIS.values()]
    L += ["", "**O caminho:** cadastro → aval → os dois recebem o link por WhatsApp e "
          "e-mail → cada um abre no celular, pede um código de 6 dígitos (vale 15 "
          "minutos), confirma o CPF/CNPJ e assina com o dedo → com as duas "
          "assinaturas o PDF final (com o registro de cada assinatura e o lacre do "
          "texto) vai para o arquivo do contrato, e ele fica **vigente**. Até lá, "
          "não se mede.", "",
          "## 5. Medição de empreita", ""]
    L += _tabela(CAMPOS_MEDICAO)
    L += ["", f"Documentos: {_docs_txt(DOCUMENTOS_MEDICAO)}. A foto é cobrada na "
          "autorização (quem mediu não autoriza a própria medição).", "",
          "## 6. Cancelar um pedido", ""]
    L += _tabela(CAMPOS_CANCELAR)
    L += ["", "Quem lançou cancela o que é seu, enquanto ninguém pagou nem "
          "conciliou. Depois disso, só estorno, pelo financeiro.", ""]
    return "\n".join(L)


if __name__ == "__main__":             # gera o mapa do repositório
    import pathlib
    alvo = pathlib.Path(__file__).resolve().parents[2] / "PLEITOS_FINANCEIRO.md"
    alvo.write_text(mapa_em_markdown() + "\n", encoding="utf-8")
    print(f"mapa gravado em {alvo}")
