# ============================================================================
# ERP — core/auth/secoes.py
# As SEÇÕES do sistema, e o que "só olhar" e "olhar e mexer" querem dizer em
# cada uma.
#
# O PEDIDO, do dono, em 13/09/2026, e com uma reclamação junta — *"não era pra
# gente estar discutindo tanto isso repetidamente"*:
#
#   *"Eu crio um perfil de usuário. O perfil eu digo: esse perfil tem acesso a
#   isso, aquilo e aquilo outro. E o usuário está dentro daquele perfil. (…) O
#   operador mais, ele não vai ter acesso a nada. Aí eu vou agregando ao
#   cadastro dele possibilidades: somente leitura de alguma área, leitura de
#   todas as áreas, leitura e edição das áreas. Isso pra uma obra, pra várias
#   obras, pra todas as obras. (…) Só que tem uma diferença do banco, porque
#   tem a questão da obra."*
#
# ─────────────────────────────────────────────────────────────────────────────
# O QUE ESTAVA ERRADO NO MODELO ANTIGO, e por que a discussão voltava sempre
#
# Três coisas diferentes estavam embaraçadas numa só:
#
#   1. **o que a pessoa pode fazer** — vinha colada ao NOME DO CARGO, escrita
#      em código, e só um programador mudava;
#   2. **em quais obras ela pode fazer** — dependia do cargo TAMBÉM: uns
#      cargos enxergavam tudo, outros só as obras designadas, e isso estava
#      numa lista fixa;
#   3. **quem é a pessoa** — o cadastro.
#
# Resultado: toda vez que o dono dizia "fulano tem de ver só a obra dele", a
# resposta era mexer no código e discutir cargo por cargo. É o que ele cansou
# de repetir, com razão.
#
# ─────────────────────────────────────────────────────────────────────────────
# COMO FICA
#
#   · **Perfil é CADASTRO**, não código: criado e editado na tela.
#   · Para cada SEÇÃO, o perfil escolhe um nível: **NADA**, **LER** ou
#     **EDITAR**. Nada mais — é o vocabulário que ele pediu.
#   · **As OBRAS são do cadastro da pessoa**, não do perfil: as marcadas, ou
#     "todas as obras". Duas pessoas do mesmo perfil podem alcançar obras
#     diferentes, que é exatamente o caso da BWS.
#   · **O padrão é NADA.** Perfil recém-criado não abre porta nenhuma.
#
# ─────────────────────────────────────────────────────────────────────────────
# POR QUE ISTO NÃO REESCREVE A GUARDA, E É DE PROPÓSITO
#
# As AÇÕES (`lancar`, `pagar`, `ver_arquivo`…) continuam existindo exatamente
# como estão, e a guarda de cada rota continua a mesma. O que muda é de ONDE
# sai o conjunto de ações de uma pessoa: antes, de uma tabela em código
# indexada pelo cargo; agora, das seções marcadas no perfil dela.
#
# Reescrever a guarda seria trocar, de uma vez, a peça mais perigosa do
# sistema — a que decide quem entra onde — com 4.700 testes apoiados nela.
# Aqui a peça perigosa fica intacta e ganha uma camada por cima, que é o que o
# dono opera. Se esta camada tiver defeito, ela concede DE MENOS (padrão
# NADA), nunca de mais.
# ============================================================================
from __future__ import annotations

from typing import Any

# Os três níveis, e não mais que três. "Só olhar" e "olhar e mexer" foram as
# palavras do dono; NADA é o padrão.
NADA = "NADA"
LER = "LER"
EDITAR = "EDITAR"
NIVEIS = (NADA, LER, EDITAR)

NIVEL_ROTULOS = {
    NADA: "Não acessa",
    LER: "Só olhar",
    EDITAR: "Olhar e mexer",
}


# ---------------------------------------------------------------------------
# O CATÁLOGO
#
# Cada seção corresponde a uma TELA (ou a um assunto que atravessa telas), e
# diz quais ações o nível LER concede e quais o nível EDITAR acrescenta.
#
# REGRA DE OURO: EDITAR sempre inclui o que LER concede. Perfil que mexe e não
# enxerga seria armadilha — a pessoa abriria a tela vazia e não entenderia.
#
# A ação `ver_erp` NÃO está em seção nenhuma: ela é a porta de entrada e é
# concedida a quem tiver QUALQUER seção acima de NADA. Sem isso, um perfil com
# uma seção marcada não conseguiria abrir o ERP para chegar nela.
# ---------------------------------------------------------------------------
SECOES: list[dict[str, Any]] = [
    # ---------------------------------------------------------- Financeiro
    {"chave": "fin_lancar", "area": "Financeiro", "nome": "Lançar e Fundo fixo",
     "explicacao": "criar solicitação de pagamento e prestação de fundo fixo",
     "ler": ["ver_fundo_fixo"], "editar": ["lancar"]},
    {"chave": "fin_titulos", "area": "Financeiro", "nome": "Solicitações",
     "explicacao": ("a lista dos lançamentos e a fila de confirmação; editar "
                    "cancela o de outra pessoa e troca a conta do plano"),
     "ler": ["ver_titulos"], "editar": ["cancelar_titulo", "reclassificar"]},
    {"chave": "fin_empreitas", "area": "Financeiro", "nome": "Empreitas",
     "explicacao": "contratos de empreita e as medições que consomem o saldo",
     "ler": ["ver_empreitas"], "editar": []},
    # Confirmar e aprovar são DUAS seções, e não uma, porque na BWS são dois
    # passos de duas pessoas diferentes: quem confirma na obra não é quem
    # libera para pagamento. Juntar as duas num nível só daria aprovação a
    # quem hoje só confirma — foi o que a conferência contra a tabela antiga
    # apontou.
    {"chave": "fin_avalizar", "area": "Financeiro", "nome": "Confirmar (aval)",
     "explicacao": "confirmar o lançamento — o primeiro dos dois passos",
     "ler": [], "editar": ["avalizar"]},
    {"chave": "fin_aprovar", "area": "Financeiro", "nome": "Aprovar para pagamento",
     "explicacao": "liberar o lançamento confirmado para ser pago",
     "ler": [], "editar": ["aprovar"]},
    {"chave": "fin_pagar", "area": "Financeiro", "nome": "Pagamentos",
     "explicacao": "dar baixa, desfazer baixa e ver dados bancários do credor",
     "ler": ["ver_dados_pagamento"], "editar": ["pagar", "desfazer"]},
    {"chave": "fin_conciliar", "area": "Financeiro", "nome": "Conciliação e importação",
     "explicacao": "extrato do banco, conciliação e importação de planilha",
     "ler": [], "editar": ["conciliar", "importar"]},
    {"chave": "fin_receber", "area": "Financeiro", "nome": "Receber",
     "explicacao": "recebimento de medição e movimentação entre contas",
     "ler": [], "editar": ["receber"]},
    {"chave": "fin_notas", "area": "Financeiro", "nome": "Notas fiscais recebidas",
     "explicacao": "notas emitidas contra a empresa, e o cruzamento com o lançamento",
     "ler": ["ver_notas"], "editar": ["cruzar_notas"]},
    {"chave": "fin_relatorios", "area": "Financeiro", "nome": "Relatórios",
     "explicacao": "totais, DRE gerencial, fluxo de caixa e curva ABC",
     "ler": ["ver_relatorios"], "editar": []},
    # --------------------------------------------------------------- Obras
    {"chave": "obr_obras", "area": "Obras", "nome": "Painel de obras",
     "explicacao": "a lista das obras, com contrato, vigência e alertas",
     "ler": ["ver_obras"], "editar": []},
    {"chave": "obr_contratos", "area": "Obras", "nome": "Contratos e medições",
     "explicacao": "o quadro financeiro do contrato, medições e faturamento",
     "ler": ["ver_contratos"], "editar": []},
    {"chave": "obr_notas_emitidas", "area": "Obras", "nome": "Notas emitidas",
     "explicacao": "as notas que a empresa emite contra o cliente",
     "ler": ["ver_notas_emitidas"], "editar": ["emitir_nota"]},
    {"chave": "obr_agenda", "area": "Obras", "nome": "Agenda de obrigações",
     "explicacao": "vencimentos de certidão, seguro, ART e contrato",
     "ler": ["ver_agenda"], "editar": ["tratar_agenda"]},
    # ------------------------------------------------------------- Pessoal
    {"chave": "pes_despesas", "area": "Pessoal", "nome": "Despesas com colaborador",
     "explicacao": "diárias, produção e verbas",
     "ler": ["ver_pessoal"], "editar": ["lancar_dc"]},
    {"chave": "pes_colaboradores", "area": "Pessoal", "nome": "Colaboradores",
     "explicacao": "o cadastro das pessoas da obra",
     "ler": ["ver_pessoal"], "editar": ["editar_colaboradores"]},
    # --------------------------------------------------------- Suprimentos
    {"chave": "sup_solicitar", "area": "Suprimentos", "nome": "Solicitações de material",
     "explicacao": "pedir material para a obra",
     "ler": ["ver_suprimentos"], "editar": ["solicitar_suprimento"]},
    {"chave": "sup_comprar", "area": "Suprimentos", "nome": "Cotações e pedidos",
     "explicacao": "cotar, fechar pedido e acompanhar a fila",
     "ler": ["ver_suprimentos", "ver_pedidos_compra"], "editar": ["comprar"]},
    {"chave": "sup_autorizar", "area": "Suprimentos", "nome": "Autorização de pedido",
     "explicacao": "liberar o pedido de compra fechado",
     "ler": ["ver_pedidos_compra"], "editar": ["autorizar_pedido"]},
    {"chave": "sup_locacoes", "area": "Suprimentos", "nome": "Locações",
     "explicacao": "equipamentos locados, parcelas e a conferência mensal",
     "ler": ["ver_locacoes"], "editar": []},
    {"chave": "sup_cadastros", "area": "Suprimentos", "nome": "Insumos e fornecedores",
     "explicacao": "o catálogo de materiais e o cadastro de fornecedores",
     "ler": ["ver_suprimentos"],
     "editar": ["administrar_insumos", "administrar_fornecedores"]},
    # -------------------------------------------------------------- Geral
    {"chave": "ger_arquivo", "area": "Geral", "nome": "Arquivo de documentos",
     "explicacao": "o acervo da empresa — contrato, certidão, ART",
     "ler": ["ver_arquivo"], "editar": ["arquivar"]},
    {"chave": "ger_encaminhar", "area": "Geral", "nome": "Encaminhar informação",
     "explicacao": "mandar lançamento ou documento por WhatsApp",
     "ler": [], "editar": ["encaminhar"]},
    {"chave": "ger_equipe", "area": "Geral", "nome": "Trabalho da equipe",
     "explicacao": "o que cada pessoa fez no sistema",
     "ler": ["ver_uso_da_equipe"], "editar": []},
    {"chave": "adm_configurar", "area": "Administração", "nome": "Configurações",
     "explicacao": ("plano de contas, cadastro de obras, contas bancárias, "
                    "empresas e manutenção do banco"),
     "ler": [], "editar": ["configurar"]},
    {"chave": "adm_operadores", "area": "Administração", "nome": "Operadores e perfis",
     "explicacao": "quem entra no sistema e o que cada um pode",
     "ler": [], "editar": ["gerir_usuarios"]},
]

POR_CHAVE = {s["chave"]: s for s in SECOES}

# A porta de entrada. Quem tem QUALQUER seção acima de NADA recebe esta ação —
# do contrário o perfil abriria uma seção e a pessoa não conseguiria nem entrar
# no ERP para chegar nela.
ACAO_DE_ENTRADA = "ver_erp"


def areas() -> list[str]:
    """As áreas, na ordem em que a tela mostra."""
    vistas: list[str] = []
    for s in SECOES:
        if s["area"] not in vistas:
            vistas.append(s["area"])
    return vistas


def acoes_do_nivel(chave: str, nivel: str) -> set[str]:
    """As ações que esta seção concede NESTE nível.

    EDITAR sempre inclui o que LER concede: perfil que mexe e não enxerga
    abriria a tela vazia, e a pessoa não teria como entender o porquê.
    """
    s = POR_CHAVE.get(chave)
    if s is None or nivel not in NIVEIS or nivel == NADA:
        return set()
    acoes = set(s["ler"])
    if nivel == EDITAR:
        acoes |= set(s["editar"])
    return acoes


def acoes_do_perfil(marcacoes: dict[str, str]) -> set[str]:
    """O conjunto de ações de um perfil, a partir das seções marcadas nele.

    `marcacoes` é {chave da seção: nível}. O que não estiver lá é NADA — o
    padrão, e a razão de um perfil novo não abrir porta nenhuma.
    """
    acoes: set[str] = set()
    for chave, nivel in (marcacoes or {}).items():
        acoes |= acoes_do_nivel(chave, nivel)
    if acoes or any(n in (LER, EDITAR) for n in (marcacoes or {}).values()):
        acoes.add(ACAO_DE_ENTRADA)
    return acoes


def para_a_tela() -> list[dict[str, Any]]:
    """O catálogo agrupado por área, para a tela de cadastro do perfil."""
    saida = []
    for area in areas():
        saida.append({
            "area": area,
            "secoes": [{"chave": s["chave"], "nome": s["nome"],
                        "explicacao": s["explicacao"],
                        # A tela mostra o que cada nível libera, em português.
                        # Sem isso, marcar "olhar e mexer" é um chute.
                        "ler": sorted(s["ler"]), "editar": sorted(s["editar"])}
                       for s in SECOES if s["area"] == area],
        })
    return saida
