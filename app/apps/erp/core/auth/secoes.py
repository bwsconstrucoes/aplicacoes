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
     "dentro_de": "Financeiro › Confirmar",
     "ler": [], "editar": ["avalizar"]},
    {"chave": "fin_aprovar", "area": "Financeiro", "nome": "Aprovar para pagamento",
     "explicacao": "liberar o lançamento confirmado para ser pago",
     "dentro_de": "Financeiro › Confirmar",
     "ler": [], "editar": ["aprovar"]},
    {"chave": "fin_pagar", "area": "Financeiro", "nome": "Pagamentos",
     "explicacao": "dar baixa, desfazer baixa e ver dados bancários do credor",
     "ler": ["ver_dados_pagamento"], "editar": ["pagar", "desfazer"]},
    # SEÇÃO PRÓPRIA, e não um item de "Pagamentos" (22/09/2026): homologar a
    # conta do credor é conferir PARA ONDE o dinheiro vai, e quem lança pode
    # cadastrar essa conta. Se viesse junto de pagar, o dono não teria como
    # dar uma coisa sem a outra — e é exatamente essa separação que segura o
    # golpe da troca de conta.
    {"chave": "fin_homologar_conta", "area": "Financeiro",
     "nome": "Homologar conta do credor",
     "explicacao": "conferir o titular e liberar a conta bancária do credor "
                   "para receber pagamento",
     "dentro_de": "Financeiro › Pagamentos e Solicitações",
     "ler": [], "editar": ["homologar_conta_credor"]},
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
    # ⚑ O DIÁRIO É LIDO POR MAIS GENTE DO QUE ESCRITO, e muito mais: a
    # FISCALIZAÇÃO do contratante chega nele pelo perfil PARCEIRO. Por isso o
    # "ler" é largo e o "editar" é estreito — quem lança é o administrativo da
    # obra, e só ele. A observação da fiscalização NÃO é uma ação aqui: ela é
    # uma regra sobre QUAL CAMPO a pessoa escreve, e mora em
    # `core/obras/diario.py`.
    {"chave": "obr_diario", "area": "Obras", "nome": "Diário de obra",
     "explicacao": ("o relato de cada dia da obra: tempo, efetivo, serviços, "
                    "ocorrências e a observação da fiscalização"),
     "ler": ["ver_diario"], "editar": ["lancar_diario"]},
    {"chave": "obr_acompanhamento", "area": "Obras", "nome": "Acompanhamento",
     "explicacao": ("processos que correm fora da BWS — aditivo, apostilamento, "
                    "licença, protocolo — e o andamento de cada um"),
     "ler": ["ver_acompanhamento"], "editar": ["tocar_processo"]},
    # ------------------------------------------------------------- Pessoal
    {"chave": "pes_despesas", "area": "Pessoal", "nome": "Despesas com colaborador",
     "explicacao": "diárias, produção e verbas",
     "ler": ["ver_pessoal"], "editar": ["lancar_dc"]},
    {"chave": "pes_colaboradores", "area": "Pessoal", "nome": "Colaboradores",
     "explicacao": "o cadastro das pessoas da obra",
     # 26/11/2026: abrir a pessoa no modal-ponte (também vem de ver o ponto).
     "ler": ["ver_pessoal", "abrir_pessoa"], "editar": ["editar_colaboradores"]},
    # ⚑ SALÁRIO É SEÇÃO PRÓPRIA (17/11/2026), e não um pedaço de
    # "Colaboradores": quem precisa ver quem está na obra e em que etapa do
    # cadastro ela está não precisa ver quanto cada um ganha. Era o pedido
    # dele para o administrativo de obra — *"visão restrita é os dados básicos
    # mesmo"* — e, posta aqui, a separação passa a valer para QUALQUER perfil
    # que ele monte pela tela, não só para aquele.
    #
    # Sem "editar": quem muda salário muda o cadastro (`editar_colaboradores`).
    # Esta seção é só sobre ENXERGAR o número.
    {"chave": "pes_remuneracao", "area": "Pessoal", "nome": "Salário e pagamento",
     "explicacao": ("salário, diária, auxílios, Pix, banco e os documentos de "
                    "pagamento — quem não tem isto vê a pessoa, não o quanto "
                    "ela ganha"),
     "dentro_de": "Pessoal › Colaboradores",
     "ler": ["ver_remuneracao"], "editar": []},
    # LER = abrir pedido e acompanhar o que abriu; EDITAR = atender e recusar.
    # É a divisão que o pipe fazia na mão, com um checklist chamado "você não
    # possui autorização para esta solicitação".
    {"chave": "pes_solicitacoes", "area": "Pessoal", "nome": "Solicitações",
     # ⚑ A FRASE MUDOU EM 15/11/2026, junto com os tipos: readmissão agora
     # acontece DENTRO do cadastro (o CPF a detecta) e o vale-transporte
     # dentro da ajuda de custo. Prometer na explicação um tipo que não está
     # mais na lista é o defeito que esta classe de frase já causou uma vez,
     # com férias no portal.
     "explicacao": "cadastro, desligamento, cargo, dependente, férias e ajuda de custo",
     # ⚑ `conferir_cpf_colaborador` MORA AQUI (15/11/2026): quem abre pedido
     # precisa saber se o CPF já tem cadastro — é o que faz o pedido de
     # cadastro se converter em readmissão em vez de criar a segunda ficha da
     # pessoa. Quem cadastra pela tela de Colaboradores ganha a mesma ação por
     # implicação (`ACOES_IMPLICADAS`), e é por isso que ela é uma ação
     # própria: duas portas diferentes precisam da mesma resposta.
     "ler": ["solicitar_dp", "conferir_cpf_colaborador"],
     "editar": ["atender_dp"]},
    # LER = acompanhar o pedido que nasceu da requisição da obra — é o que
    # acaba com o "e aí, saiu?" por WhatsApp. EDITAR = pedir, cobrar, conferir o
    # retorno e ligar as rotinas do agente.
    {"chave": "pes_contabilidade", "area": "Pessoal",
     "nome": "Contabilidade e agente",
     "explicacao": "pedidos formais à contabilidade, cobrança de prazo e as "
                   "rotinas do agente de DP",
     "ler": ["ver_pessoal"], "editar": ["pedir_contabilidade"]},
    # ⚑ ACESSO AO PORTAL (18/11/2026). Ele: *"libera o DP também."*
    #
    # Seção PRÓPRIA, e não um pedaço de "Operadores e perfis": quem resolve
    # esta fila não ganha a tela de operadores — não cria operador com perfil
    # escolhido, não mexe na alçada de ninguém. O acesso que nasce daqui é
    # sempre administrativo de obra, preso à obra em que a pessoa bate ponto.
    {"chave": "pes_acesso_portal", "area": "Pessoal",
     "nome": "Acesso ao portal",
     "explicacao": "ver (ler) ou liberar e recusar (editar) quem pediu "
                   "acesso ao portal pelo aplicativo do ponto",
     "dentro_de": "Pessoal › Colaboradores e Configurações › Operadores",
     # 25/11/2026: ler = ver a fila (o DP); editar = liberar (o administrador).
     "ler": ["ver_pedidos_de_acesso"], "editar": ["liberar_acesso_portal"]},
    # --------------------------------------------------------- Suprimentos
    # ⚑ RECEBER MATERIAL ENTROU AQUI (20/11/2026), e não numa seção própria:
    # quem pede material para a obra é quem confere quando ele chega. Seção
    # nova exigiria mexer nos perfis já gravados de todo mundo; assim, quem já
    # "mexe" em Solicitações passa a receber, sem ninguém ganhar nem perder
    # mais nada — há teste comparando cada perfil pronto com o cargo.
    {"chave": "sup_solicitar", "area": "Suprimentos",
     "nome": "Solicitações e recebimento de material",
     "explicacao": "pedir material para a obra e registrar o que chegou",
     "ler": ["ver_suprimentos"],
     "editar": ["solicitar_suprimento", "receber_material"]},
    {"chave": "sup_liberar", "area": "Suprimentos", "nome": "Liberação do pedido da obra",
     "explicacao": "dar o ok da engenharia no material pedido pela obra e a ciência na compra feita por fora",
     "ler": ["ver_suprimentos"],
     "editar": ["liberar_solicitacao", "autorizar_compra_fora_do_fluxo"]},
    {"chave": "sup_comprar", "area": "Suprimentos", "nome": "Cotações e pedidos",
     "explicacao": "cotar, fechar pedido e acompanhar a fila",
     "ler": ["ver_suprimentos", "ver_pedidos_compra"], "editar": ["comprar"]},
    {"chave": "sup_autorizar", "area": "Suprimentos", "nome": "Autorização de pedido",
     "explicacao": "liberar o pedido de compra fechado",
     "ler": ["ver_pedidos_compra"],
     "editar": ["autorizar_pedido", "autorizar_compra_fora_do_fluxo"]},
    {"chave": "sup_locacoes", "area": "Suprimentos", "nome": "Locações",
     "explicacao": "equipamentos locados, parcelas e a conferência mensal",
     "ler": ["ver_locacoes"], "editar": []},
    {"chave": "sup_cadastros", "area": "Suprimentos", "nome": "Insumos e fornecedores",
     "explicacao": "o catálogo de materiais e o cadastro de fornecedores",
     "ler": ["ver_suprimentos"],
     "editar": ["administrar_insumos", "administrar_fornecedores"]},
    # --------------------------------------------------------------- Ponto
    # O módulo `ponto` (03/10/2026). Quatro seções, e não uma, porque são quatro
    # pessoas diferentes na BWS: o encarregado trata o ponto da obra dele; o DP
    # aprova afastamento e enxerga o atestado; quem fecha o mês; quem configura.
    {"chave": "pon_gestao", "area": "Ponto", "nome": "Gestão do ponto",
     "explicacao": ("quem bateu, o espelho, as pendências e os alertas das obras "
                    "da pessoa; mexer decide batida em análise e ajuste de batida"),
     "ler": ["ver_ponto"], "editar": ["tratar_ponto"]},
    {"chave": "pon_dp", "area": "Ponto", "nome": "Afastamentos e banco de horas (DP)",
     "explicacao": ("aprovar atestado, licença e férias, abrir o atestado e "
                    "lançar no banco de horas"),
     "ler": ["ver_ponto"], "editar": ["aprovar_afastamento"]},
    {"chave": "pon_competencia", "area": "Ponto", "nome": "Fechamento do mês",
     "explicacao": "fechar o mês do ponto para a folha, e reabrir com motivo",
     "ler": ["ver_ponto"], "editar": ["fechar_competencia"]},
    {"chave": "pon_config", "area": "Ponto", "nome": "Configuração do ponto",
     "explicacao": "escalas, feriados, aparelhos e as atualizações do banco do ponto",
     "ler": ["ver_ponto"], "editar": ["configurar_ponto"]},
    # -------------------------------------------------------------- Geral
    {"chave": "ger_arquivo", "area": "Geral", "nome": "Arquivo de documentos",
     "explicacao": "o acervo da empresa — contrato, certidão, ART",
     "ler": ["ver_arquivo"], "editar": ["arquivar"]},
    {"chave": "ger_encaminhar", "area": "Geral", "nome": "Encaminhar informação",
     "explicacao": "mandar lançamento ou documento por WhatsApp",
     "dentro_de": "em todo lançamento e documento",
     "ler": [], "editar": ["encaminhar"]},
    {"chave": "ger_equipe", "area": "Geral", "nome": "Trabalho da equipe",
     "explicacao": "o que cada pessoa fez no sistema",
     "dentro_de": "Administração › Trabalho no sistema",
     "ler": ["ver_uso_da_equipe"], "editar": []},
    {"chave": "adm_configurar", "area": "Administração", "nome": "Configurações",
     "explicacao": ("plano de contas, cadastro de obras, contas bancárias, "
                    "empresas e manutenção do banco"),
     "ler": [], "editar": ["configurar"]},
    {"chave": "adm_operadores", "area": "Administração", "nome": "Operadores e perfis",
     "explicacao": "quem entra no sistema e o que cada um pode",
     "dentro_de": "Administração › Configurações",
     "ler": [], "editar": ["gerir_usuarios"]},
    {"chave": "adm_mensagens", "area": "Administração", "nome": "Mensagens",
     "explicacao": ("por onde cada tipo de aviso sai (Telegram ou WhatsApp), o teto do "
                    "WhatsApp e o registro do que foi enviado"),
     "ler": ["ver_mensagens"], "editar": ["configurar_mensagens"]},
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


def para_a_tela_pelo_menu(modulos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """O catálogo agrupado PELO MENU DO ERP, e não por uma área escrita à mão.

    ⚑ O PEDIDO, de 27/11/2026, com as palavras dele: *"a divisão dessas seções
    tá totalmente solta, sem conexão entre cada área e cada tela"*, e logo
    depois *"essa tela precisa de uma repaginação completa. a experiência de
    uso dela é péssima."*

    Ele estava certo, e dava para medir: a área de cada seção era um texto
    digitado aqui ("Geral", "Administração"), que não correspondia aos módulos
    que ele vê em cima da tela. "Notas emitidas" aparecia em **Obras** no
    cadastro e em **Financeiro** no menu. "Mensagens" é módulo no menu e era
    uma linha de Administração aqui. E nada dizia QUAIS TELAS cada seção abre —
    então marcar era chute.

    Agora quem agrupa é o menu: a seção fica no módulo das telas que ela abre,
    e cada linha mostra os nomes dessas telas. Seção que **não abre tela** (o
    aval, a aprovação, o salário, o acesso ao portal…) sai da lista de telas e
    vai para um bloco próprio, "o que ela libera DENTRO da tela" — porque
    tratá-la como se fosse uma área é justamente o que embaralhava a tela.

    `modulos` chega de fora (`routes.MODULOS` + a ação de entrada de cada aba)
    para este arquivo não precisar importar as rotas — ele é carregado pela
    guarda que roda antes de TODA rota, e uma importação circular aí derrubaria
    o ERP inteiro.
    """
    # ação de entrada → (ordem do módulo, nome do módulo, nomes das telas)
    onde: dict[str, tuple[int, str, list[str]]] = {}
    for i, m in enumerate(modulos):
        for tela in m["telas"]:
            registro = onde.setdefault(tela["acao"], (i, m["nome"], []))
            registro[2].append(tela["nome"])

    quantas_no_modulo = {m["nome"]: len(m["telas"]) for m in modulos}
    grupos: dict[str, dict[str, Any]] = {}
    for s in SECOES:
        # A ação de entrada de uma tela pode estar no "só olhar" OU no "olhar e
        # mexer": Pagamentos, Conciliação, Receber e Configurações são telas
        # cujo ingresso é o próprio verbo.
        portas = [a for a in (*s["ler"], *s["editar"]) if a in onde]
        if portas:
            i, modulo, _ = onde[portas[0]]
            abre: list[str] = []
            for a in portas:
                for nome in onde[a][2]:
                    if nome not in abre:
                        abre.append(nome)
            destino = "telas"
            linha = {"abre": abre,
                     # Marcar UMA seção do Ponto abre as 15 telas do Ponto: a
                     # tela diz isso em uma linha em vez de listar quinze.
                     "abre_o_modulo": len(abre) == quantas_no_modulo.get(modulo)}
        else:
            # Seção que não abre tela nenhuma: o lugar dela é o que o catálogo
            # declara em `dentro_de`, e é por ele que ela se agrupa.
            dentro = s.get("dentro_de") or ""
            modulo = (dentro.split("›")[0].strip()
                      if "›" in dentro else "Em várias telas")
            i = next((j for j, m in enumerate(modulos) if m["nome"] == modulo),
                     len(modulos))
            destino, linha = "dentro", {"dentro_de": dentro}
        g = grupos.setdefault(modulo, {"modulo": modulo, "ordem": i,
                                       "telas": [], "dentro": []})
        g["ordem"] = min(g["ordem"], i)
        g[destino].append({"chave": s["chave"], "nome": s["nome"],
                           "explicacao": s["explicacao"],
                           "ler": sorted(s["ler"]), "editar": sorted(s["editar"]),
                           **linha})
    return sorted(grupos.values(), key=lambda g: g["ordem"])


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
