# ============================================================================
# ERP — core/cadastros/plano_padrao.py
# Plano financeiro da BWS em 3 níveis (GRUPO > SUBGRUPO > CATEGORIA), gravado
# direto no banco — sem CSV. Reexecutar é seguro: atualiza o que mudou e
# nunca duplica (chave = código da categoria).
#
# PRINCÍPIOS (correções ao plano herdado do Omie):
#   1. UM TRIBUTO, UMA CONTA. O INSS da obra é o mesmo INSS, tenha sido retido
#      pelo cliente na nota ou pago em guia. "Retenção" é FORMA DE LIQUIDAÇÃO
#      (campo do título), não conta contábil. Isso desfaz a divisão
#      "Despesas Tributárias" × "Retenções Tributárias", que impedia comparar
#      obras entre si.
#   2. DEDUTIBILIDADE NÃO É DA CONTA. É do documento; vive no título
#      (status PENDENTE/DEDUTIVEL/INDEDUTIVEL/PARCIAL). Aqui fica apenas uma
#      SUGESTÃO inicial para acelerar a triagem.
#   3. MEIO DE PAGAMENTO NÃO É CATEGORIA — mas FUNDO FIXO É. "Despesa com
#      cartão" e "BeeVale" saem do plano (são forma de pagamento). O fundo fixo
#      permanece como conta porque não é um meio de pagar: é um processo de
#      PRESTAÇÃO DE CONTAS entre o administrativo da obra e a empresa, com
#      comprovantes anexados e análise própria (tipo T10).
#   4. ATIVO NÃO É DESPESA. Aquisições vão para o grupo 8 (investimento,
#      natureza FLUXO), fora da DRE gerencial — corrige "Móveis e Utensílios"
#      dentro de Materiais Aplicados e "Conservação Predial" dentro de Ativos.
#   5. APORTE E DEVOLUÇÃO SÃO FLUXO, COM DIREÇÃO EXPLÍCITA. Contas separadas
#      para aporte recebido/concedido e devolução recebida/concedida, e o
#      parceiro/obra de destino é dito no rateio — resolve a confusão de
#      aportes entre obras próprias e obras em parceria.
#   6. REFORMA TRIBUTÁRIA. As contas de PIS e COFINS são mantidas (regime
#      atual) e já existem as de CBS e IBS para a transição 2026-2033, mais
#      a de Imposto Seletivo. Assim o período de coexistência não exige
#      remodelar o plano.
#
# Colunas do plano: código, descrição, natureza (RESULTADO/FLUXO), sugestão de
# dedutibilidade, tipos de documento aceitos e descrição de uso (o "quando usar
# esta conta", que evita a escolha errada na hora do lançamento).
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Categoria, TipoTitulo, Usuario

# Tipos de documento hábil (rótulos internos T*)
MAT = "T1_MATERIAL_NFE"
SRV = "T2_SERVICO_NFSE"
FRE = "T3_FRETE_CTE"
LOC = "T4_LOCACAO"
EMP = "T5_EMPREITEIRO"
RPA = "T6_SERVICO_PF_RPA"
FOL = "T7_FOLHA_ENCARGOS"
GUI = "T8_TRIBUTO_GUIA"
CON = "T9_CONCESSIONARIA"
FFX = "T10_FUNDO_FIXO"
ADI = "T11_ADIANTAMENTO"
REE = "T12_REEMBOLSO"
FIN = "T13_FINANCIAMENTO"
EXC = "T14_EXCECAO_SEM_NOTA"

# (grupo_codigo, grupo_nome, subgrupo_codigo, subgrupo_nome, [categorias])
# categoria = (codigo, descricao, natureza, sugestao_dedutivel, tipos, uso)
PLANO: list[tuple[str, str, str, str, list[tuple]]] = [

    # ---------------------------------------------------------------- 1 RECEITAS
    ("1", "Receitas", "1.1", "Receita operacional", [
        ("1.1.01", "Receita de obras (medições)", "RESULTADO", True, [],
         "Medição aprovada e faturada da obra. É a receita principal."),
        ("1.1.02", "Receita de aditivos e reajustes", "RESULTADO", True, [],
         "Faturamento decorrente de aditivo contratual ou reajuste de índice."),
    ]),
    ("1", "Receitas", "1.2", "Outras receitas operacionais", [
        ("1.2.01", "Devolução de material a fornecedor", "RESULTADO", True, [],
         "Crédito por material devolvido. Reduz o custo, não é venda."),
        ("1.2.02", "Estorno de despesas", "RESULTADO", True, [],
         "Devolução de valor pago a maior ou despesa cancelada."),
        ("1.2.03", "Reembolso de custas e despesas processuais", "RESULTADO", True, [],
         "Ressarcimento recebido de terceiros ou do judiciário."),
        ("1.2.04", "Multas e indenizações recebidas", "RESULTADO", True, [],
         "Multa contratual cobrada de fornecedor, indenização de seguro."),
    ]),
    ("1", "Receitas", "1.3", "Receitas financeiras", [
        ("1.3.01", "Rendimentos de aplicações financeiras", "RESULTADO", True, [],
         "Rendimento creditado pelo banco. O resgate do principal é 9.2."),
        ("1.3.02", "Juros e descontos obtidos", "RESULTADO", True, [],
         "Juros recebidos por atraso de cliente; desconto obtido em compra."),
    ]),
    ("1", "Receitas", "1.4", "Resultado na venda de ativos", [
        ("1.4.01", "Venda de imóveis", "RESULTADO", True, [],
         "Resultado da alienação. A entrada do dinheiro aparece no fluxo."),
        ("1.4.02", "Venda de veículos, máquinas e equipamentos", "RESULTADO", True, [], ""),
        ("1.4.03", "Venda de outros bens do patrimônio", "RESULTADO", True, [], ""),
    ]),

    # -------------------------------------------------- 2 TRIBUTOS (conta única)
    ("2", "Tributos", "2.1", "Tributos sobre receita e serviços", [
        ("2.1.01", "ISS sobre serviços", "RESULTADO", True, [GUI],
         "ISS da obra — retido pelo tomador na nota OU pago em guia. "
         "A forma de liquidação é informada no título, não muda a conta."),
        ("2.1.02", "INSS sobre serviços e obra (CEI/CNO)", "RESULTADO", True, [GUI],
         "INSS de serviço/obra, retido na nota ou recolhido em GPS. "
         "Conta única para permitir comparar obras."),
        ("2.1.03", "IRRF sobre serviços prestados", "RESULTADO", True, [GUI],
         "IR retido pelo tomador ou recolhido por nós. Não confundir com IRPJ."),
        ("2.1.04", "PIS sobre faturamento", "RESULTADO", True, [GUI],
         "Regime atual. Convive com a CBS durante a transição."),
        ("2.1.05", "COFINS sobre faturamento", "RESULTADO", True, [GUI], "Regime atual."),
        ("2.1.06", "CSRF/PCC retido (PIS-COFINS-CSLL sobre serviços)", "RESULTADO", True, [GUI],
         "Retenção conjunta de 4,65% sobre serviços prestados a PJ."),
        ("2.1.07", "CPRB — contribuição sobre a receita bruta", "RESULTADO", True, [GUI],
         "Desoneração da folha, quando aplicável à obra."),
        ("2.1.08", "RET — regime especial de tributação", "RESULTADO", True, [GUI],
         "Incorporação com patrimônio de afetação."),
        ("2.1.09", "CBS — contribuição sobre bens e serviços", "RESULTADO", True, [GUI],
         "Reforma tributária: substitui PIS/COFINS a partir da transição."),
        ("2.1.10", "IBS — imposto sobre bens e serviços", "RESULTADO", True, [GUI],
         "Reforma tributária: substitui ICMS/ISS a partir da transição."),
        ("2.1.11", "Imposto Seletivo", "RESULTADO", True, [GUI],
         "Reforma tributária: incidência específica, quando aplicável."),
        ("2.1.12", "ICMS — diferencial de alíquota (DIFAL)", "RESULTADO", True, [GUI],
         "DIFAL de material comprado em outro estado. Compõe o custo da obra."),
    ]),
    ("2", "Tributos", "2.2", "Tributos sobre o lucro", [
        ("2.2.01", "IRPJ", "RESULTADO", False, [GUI],
         "Imposto sobre o lucro — indedutível por natureza."),
        ("2.2.02", "CSLL", "RESULTADO", False, [GUI], "Indedutível por natureza."),
    ]),
    ("2", "Tributos", "2.3", "Penalidades tributárias", [
        ("2.3.01", "Multas e juros tributários", "RESULTADO", False, [GUI],
         "Multa e juros de mora de tributo. Multa punitiva é indedutível."),
        ("2.3.02", "Parcelamentos tributários — juros", "RESULTADO", True, [GUI, FIN],
         "Parcela de juros do parcelamento. O principal vai em 9.4."),
    ]),

    # ------------------------------------------------------- 3 CUSTOS DE OBRA
    ("3", "Custos de obra", "3.1", "Materiais aplicados", [
        ("3.1.01", "Cimento, concreto usinado e argamassas", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.02", "Agregados (areia, brita, arisco)", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.03", "Aço e armadura", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.04", "Elementos de vedação (tijolo, bloco, parede PVC)", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.05", "Pré-moldados e estrutura metálica", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.06", "Telhas e material de cobertura", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.07", "Madeiramento e fôrmas", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.08", "Material elétrico, cabeamento e CFTV", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.09", "Material hidráulico, sanitário e gás", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.10", "Material de combate a incêndio", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.11", "Material de climatização", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.12", "Pisos, cerâmicas e revestimentos", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.13", "Louças, metais e bancadas", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.14", "Esquadrias, vidros e serralheria", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.15", "Material de pintura", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.16", "Forro e divisórias", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.17", "Impermeabilizantes, aditivos e colas", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.18", "Parafusos, ferragens e acessórios", "RESULTADO", True, [MAT, EXC], ""),
        ("3.1.19", "Ferramentas e material de consumo", "RESULTADO", True, [MAT, FFX, EXC],
         "Ferramenta de baixo valor/consumo. Equipamento durável vai em 8.1."),
        ("3.1.20", "Paisagismo e jardinagem", "RESULTADO", True, [MAT, SRV, EXC], ""),
        ("3.1.99", "Outros materiais de obra", "RESULTADO", True, [MAT, EXC],
         "Use só quando nenhuma conta acima servir; revisar periodicamente."),
    ]),
    ("3", "Custos de obra", "3.2", "Serviços de terceiros", [
        ("3.2.01", "Empreiteiros e subcontratação (com medição)", "RESULTADO", True, [EMP],
         "Serviço de execução com contrato e medição. Exige CND e NFS-e."),
        ("3.2.02", "Serviços técnicos PJ aplicados à obra", "RESULTADO", True, [SRV],
         "Projeto, sondagem, topografia, laudo, instalação especializada."),
        ("3.2.03", "Serviços de pessoa física (RPA)", "RESULTADO", True, [RPA],
         "Autônomo com RPA: gera INSS (2.1.02) e IRRF (2.1.03)."),
        ("3.2.04", "Fretes, carretos e transporte de material", "RESULTADO", True, [FRE, EXC], ""),
        ("3.2.05", "Limpeza e conservação de obra", "RESULTADO", True, [SRV, MAT, EXC], ""),
        ("3.2.06", "Alimentação de equipe em obra", "RESULTADO", True, [SRV, FFX, REE],
         "Fornecimento terceirizado de refeições no canteiro."),
        ("3.2.07", "Segurança e vigilância de obra", "RESULTADO", True, [SRV], ""),
    ]),
    ("3", "Custos de obra", "3.3", "Locações", [
        ("3.3.01", "Locação de equipamentos", "RESULTADO", True, [LOC], "Exige contrato vigente."),
        ("3.3.02", "Locação de máquinas pesadas", "RESULTADO", True, [LOC], ""),
        ("3.3.03", "Locação de veículos", "RESULTADO", True, [LOC], ""),
        ("3.3.04", "Locação de imóveis para obra (canteiro/alojamento)", "RESULTADO", True, [LOC], ""),
        ("3.3.05", "Locação de andaimes, escoramentos e formas", "RESULTADO", True, [LOC], ""),
    ]),
    ("3", "Custos de obra", "3.4", "Despesas indiretas de obra", [
        ("3.4.01", "Água e energia da obra", "RESULTADO", True, [CON, EXC], ""),
        ("3.4.02", "Combustível de veículos e máquinas em obra", "RESULTADO", True, [MAT, FFX, REE], ""),
        ("3.4.03", "EPI e segurança do trabalho", "RESULTADO", True, [MAT, SRV], ""),
        ("3.4.04", "Exames ocupacionais e medicina do trabalho", "RESULTADO", True, [SRV], ""),
        ("3.4.05", "Mobilização, desmobilização e canteiro", "RESULTADO", True, [MAT, SRV, EXC], ""),
        ("3.4.06", "Taxas, licenças e ART/RRT da obra", "RESULTADO", True, [GUI, EXC], ""),
        ("3.4.07", "Seguros da obra (risco de engenharia, garantia)", "RESULTADO", True, [SRV, EXC], ""),
        ("3.4.08", "Fundo fixo da obra — prestação de contas", "RESULTADO", True, [FFX],
         "Reembolso ao administrativo da obra mediante prestação de contas com os "
         "comprovantes anexados. Item de valor relevante e com nota própria deve ser "
         "lançado na sua conta específica; o fundo fixo cobre a miudeza do canteiro."),
    ]),

    # ------------------------------------------------------------- 4 PESSOAL
    ("4", "Pessoal", "4.1", "Remuneração", [
        ("4.1.01", "Salários e ordenados", "RESULTADO", True, [FOL], ""),
        ("4.1.02", "Férias e 13º salário", "RESULTADO", True, [FOL], ""),
        ("4.1.03", "Horas extras, gratificações e produção", "RESULTADO", True, [FOL], ""),
        ("4.1.04", "Rescisões e verbas indenizatórias", "RESULTADO", True, [FOL], ""),
        ("4.1.05", "Participação nos lucros e resultados", "RESULTADO", True, [FOL], ""),
        ("4.1.06", "Pró-labore", "RESULTADO", True, [FOL], ""),
    ]),
    ("4", "Pessoal", "4.2", "Encargos", [
        ("4.2.01", "INSS patronal sobre folha", "RESULTADO", True, [FOL, GUI],
         "Encargo da folha. O INSS de obra/serviço fica em 2.1.02."),
        ("4.2.02", "FGTS", "RESULTADO", True, [FOL, GUI], ""),
        ("4.2.03", "IRRF sobre a folha", "RESULTADO", True, [FOL, GUI],
         "Retido do empregado e recolhido — não confundir com 2.1.03."),
        ("4.2.04", "Contribuição sindical e assistencial", "RESULTADO", True, [GUI, FOL], ""),
    ]),
    ("4", "Pessoal", "4.3", "Benefícios", [
        ("4.3.01", "Vale-transporte e deslocamento", "RESULTADO", True, [FOL, SRV], ""),
        ("4.3.02", "Vale-alimentação e cesta básica", "RESULTADO", True, [FOL, SRV], ""),
        ("4.3.03", "Plano de saúde e seguro de vida", "RESULTADO", True, [FOL, SRV], ""),
        ("4.3.04", "Outros benefícios", "RESULTADO", True, [FOL, SRV], ""),
    ]),
    ("4", "Pessoal", "4.4", "Desenvolvimento e contencioso", [
        ("4.4.01", "Cursos, treinamentos e certificações", "RESULTADO", True, [SRV], ""),
        ("4.4.02", "Acordos e condenações trabalhistas", "RESULTADO", True, [EXC, SRV],
         "Verba indenizatória de acordo; multa punitiva costuma ser indedutível."),
    ]),

    # ------------------------------------------------- 5 DESPESAS ADMINISTRATIVAS
    ("5", "Despesas administrativas", "5.1", "Estrutura", [
        ("5.1.01", "Aluguel e condomínio da sede", "RESULTADO", True, [LOC], ""),
        ("5.1.02", "Água e energia da sede", "RESULTADO", True, [CON], ""),
        ("5.1.03", "Internet, telefonia e software", "RESULTADO", True, [CON, SRV], ""),
        ("5.1.04", "Conservação e manutenção predial", "RESULTADO", True, [SRV, MAT],
         "Manutenção do imóvel. Reforma que valoriza o bem vai em 8.2."),
        ("5.1.05", "Limpeza e copa da sede", "RESULTADO", True, [SRV, MAT], ""),
        ("5.1.06", "Segurança e vigilância da sede", "RESULTADO", True, [SRV], ""),
    ]),
    ("5", "Despesas administrativas", "5.2", "Serviços profissionais", [
        ("5.2.01", "Contabilidade", "RESULTADO", True, [SRV], ""),
        ("5.2.02", "Assessoria jurídica e advogados", "RESULTADO", True, [SRV], ""),
        ("5.2.03", "Auditoria e consultorias", "RESULTADO", True, [SRV], ""),
        ("5.2.04", "Cartórios, CREA e taxas administrativas", "RESULTADO", True, [GUI, REE], ""),
    ]),
    ("5", "Despesas administrativas", "5.3", "Operação administrativa", [
        ("5.3.10", "Fundo fixo administrativo — prestação de contas", "RESULTADO", True, [FFX],
         "Mesmo rito do fundo fixo de obra, para despesas miúdas da sede."),
        ("5.3.01", "Material de escritório e impressões", "RESULTADO", True, [MAT, FFX], ""),
        ("5.3.02", "Combustível administrativo", "RESULTADO", True, [MAT, FFX, REE], ""),
        ("5.3.03", "Manutenção de veículos e máquinas", "RESULTADO", True, [SRV, MAT], ""),
        ("5.3.04", "Manutenção de ferramentas e equipamentos", "RESULTADO", True, [SRV, MAT], ""),
        ("5.3.05", "Viagens, hospedagem e deslocamento", "RESULTADO", True, [REE, SRV, FFX], ""),
        ("5.3.06", "Taxas, licenciamento e IPVA de veículos", "RESULTADO", True, [GUI], ""),
        ("5.3.07", "Multas de trânsito", "RESULTADO", False, [GUI, REE],
         "Indedutível por natureza (infração)."),
        ("5.3.08", "Marketing, brindes e relacionamento", "RESULTADO", True, [SRV, MAT],
         "Brinde sem vínculo com a atividade tende a ser indedutível — verificar."),
        ("5.3.99", "Outras despesas administrativas", "RESULTADO", True, [MAT, SRV, REE, EXC], ""),
    ]),

    # --------------------------------------------------------- 6 FINANCEIRAS
    ("6", "Despesas financeiras", "6.1", "Custo do dinheiro", [
        ("6.1.01", "Juros sobre empréstimos e financiamentos", "RESULTADO", True, [FIN],
         "Só os juros. O principal é fluxo (9.4)."),
        ("6.1.02", "Juros e multas por atraso a fornecedores", "RESULTADO", False, [EXC],
         "Apurar quem deu causa antes de aceitar."),
        ("6.1.03", "Descontos concedidos e antecipação de recebíveis", "RESULTADO", True, [EXC], ""),
    ]),
    ("6", "Despesas financeiras", "6.2", "Serviços bancários e garantias", [
        ("6.2.01", "Tarifas bancárias e IOF", "RESULTADO", True, [EXC], ""),
        ("6.2.02", "Seguro-garantia e fiança bancária", "RESULTADO", True, [SRV, EXC], ""),
        ("6.2.03", "Seguros corporativos (frota, patrimônio, RC)", "RESULTADO", True, [SRV], ""),
    ]),

    # --------------------------------------------------------- 8 INVESTIMENTOS
    ("8", "Investimentos (ativo)", "8.1", "Aquisição de bens", [
        ("8.1.01", "Veículos, máquinas e equipamentos", "FLUXO", True, [MAT, FIN],
         "Bem durável — não entra na DRE; deprecia."),
        ("8.1.02", "Equipamentos de informática", "FLUXO", True, [MAT], ""),
        ("8.1.03", "Móveis e utensílios", "FLUXO", True, [MAT], ""),
        ("8.1.04", "Ferramentas e equipamentos duráveis", "FLUXO", True, [MAT], ""),
    ]),
    ("8", "Investimentos (ativo)", "8.2", "Imóveis e benfeitorias", [
        ("8.2.01", "Aquisição de imóveis", "FLUXO", True, [EXC], ""),
        ("8.2.02", "Benfeitorias e reformas que valorizam o imóvel", "FLUXO", True, [SRV, MAT],
         "Aumenta a vida útil/valor do bem. Manutenção simples é 5.1.04."),
    ]),
    ("8", "Investimentos (ativo)", "8.3", "Intangíveis", [
        ("8.3.01", "Software, licenças e desenvolvimento", "FLUXO", True, [SRV, MAT], ""),
    ]),

    # ------------------------------------------- 9 MOVIMENTAÇÕES FINANCEIRAS
    ("9", "Movimentações financeiras", "9.1", "Transferências", [
        ("9.1.01", "Transferência entre contas da empresa", "FLUXO", True, [EXC],
         "Saída de uma conta e entrada em outra — não é despesa."),
    ]),
    ("9", "Movimentações financeiras", "9.2", "Aplicações", [
        ("9.2.01", "Aplicação financeira — saída", "FLUXO", True, [EXC], ""),
        ("9.2.02", "Resgate de aplicação — entrada", "FLUXO", True, [], ""),
    ]),
    ("9", "Movimentações financeiras", "9.3", "Aportes e parcerias", [
        ("9.3.01", "Aporte recebido de sócio", "FLUXO", True, [],
         "Dinheiro que entra do sócio na empresa."),
        ("9.3.02", "Aporte recebido de parceiro em obra", "FLUXO", True, [],
         "Parceiro aporta na obra em parceria. A obra é dita no rateio."),
        ("9.3.03", "Aporte concedido a obra própria", "FLUXO", True, [EXC],
         "Empresa injeta recurso em obra própria (rateio indica a obra)."),
        ("9.3.04", "Aporte concedido a obra em parceria", "FLUXO", True, [EXC],
         "Nossa parte no aporte da obra em parceria."),
        ("9.3.05", "Devolução de aporte recebida", "FLUXO", True, [],
         "Obra/parceiro devolve à empresa o que foi aportado."),
        ("9.3.06", "Devolução de aporte concedida", "FLUXO", True, [EXC],
         "Empresa devolve ao parceiro o aporte que ele fez."),
    ]),
    ("9", "Movimentações financeiras", "9.4", "Empréstimos e financiamentos", [
        ("9.4.01", "Captação de empréstimo — entrada", "FLUXO", True, [], ""),
        ("9.4.02", "Pagamento de principal de empréstimo", "FLUXO", True, [FIN],
         "Só o principal. Os juros vão em 6.1.01."),
        ("9.4.03", "Pagamento de principal de parcelamento tributário", "FLUXO", True, [FIN, GUI],
         "Só o principal. Os juros vão em 2.3.02."),
    ]),
    ("9", "Movimentações financeiras", "9.5", "Sócios", [
        ("9.5.01", "Distribuição de lucros e dividendos", "FLUXO", True, [EXC], ""),
        ("9.5.02", "Adiantamento a fornecedor (encontro de contas)", "FLUXO", True, [ADI],
         "Trânsito até a entrega; baixa contra a nota definitiva."),
    ]),
]


def _tipos(lista: list[str]) -> list[TipoTitulo]:
    return [TipoTitulo(t) for t in lista]


def aplicar_plano(s: Session, usuario: Optional[Usuario] = None,
                  sobrescrever_descricoes: bool = True) -> dict[str, Any]:
    """Grava o plano no banco. Idempotente: cria o que falta e atualiza o que
    mudou. Contas criadas pela BWS e contas RENOMEADAS pelo usuário
    (personalizada=True) têm o texto preservado."""
    criadas, atualizadas = [], []
    ordem = 0
    for grupo_cod, grupo_nome, sub_cod, sub_nome, categorias in PLANO:
        for cod, desc, natureza, ded, tipos, uso in categorias:
            ordem += 1
            cat = s.scalars(select(Categoria).where(Categoria.codigo == cod)).first()
            if cat is None:
                s.add(Categoria(
                    codigo=cod, descricao=desc, natureza=natureza,
                    grupo_codigo=grupo_cod, grupo_nome=grupo_nome,
                    subgrupo_codigo=sub_cod, subgrupo_nome=sub_nome,
                    descricao_uso=uso or None, dedutivel_padrao=ded,
                    tipos_permitidos=_tipos(tipos), ordem=ordem, ativo=True))
                criadas.append(cod)
            else:
                mudou = False
                for campo, valor in (("grupo_codigo", grupo_cod), ("grupo_nome", grupo_nome),
                                     ("subgrupo_codigo", sub_cod), ("subgrupo_nome", sub_nome),
                                     ("natureza", natureza), ("ordem", ordem)):
                    if getattr(cat, campo, None) != valor:
                        setattr(cat, campo, valor)
                        mudou = True
                if sobrescrever_descricoes and not cat.personalizada:
                    if cat.descricao != desc:
                        cat.descricao = desc
                        mudou = True
                    if (cat.descricao_uso or "") != (uso or ""):
                        cat.descricao_uso = uso or None
                        mudou = True
                if not cat.tipos_permitidos and tipos:
                    cat.tipos_permitidos = _tipos(tipos)
                    mudou = True
                if mudou:
                    atualizadas.append(cod)
    s.flush()
    registrar_evento(s, "categoria", 0, "PLANO_APLICADO",
                     {"criadas": len(criadas), "atualizadas": len(atualizadas)},
                     usuario.id if usuario else None)
    return {"criadas": criadas, "atualizadas": atualizadas,
            "total_no_plano": ordem}


def substituir_categoria(s: Session, origem_id: int, destino_id: int,
                         usuario: Usuario) -> dict[str, Any]:
    """Aposenta uma conta e manda TODOS os lançamentos dela para outra —
    sem editar título por título. A origem fica inativa e aponta para o
    destino, preservando o histórico da decisão."""
    from app.apps.erp.db.models.financeiro import Titulo

    origem = s.get(Categoria, origem_id)
    destino = s.get(Categoria, destino_id)
    if origem is None or destino is None:
        raise ErroValidacao("Categoria de origem ou destino inexistente.")
    if origem.id == destino.id:
        raise ErroValidacao("Origem e destino são a mesma categoria.")
    if not destino.ativo:
        raise ErroValidacao(f"A categoria de destino ({destino.codigo}) está inativa.")

    titulos = s.scalars(select(Titulo).where(Titulo.categoria_id == origem.id)).all()
    for t in titulos:
        t.categoria_id = destino.id
    origem.ativo = False
    origem.substituida_por_id = destino.id
    s.flush()
    registrar_evento(s, "categoria", origem.id, "SUBSTITUIDA", {
        "origem": f"{origem.codigo} · {origem.descricao}",
        "destino": f"{destino.codigo} · {destino.descricao}",
        "titulos_remanejados": len(titulos)}, usuario.id)
    return {"titulos_remanejados": len(titulos),
            "origem": origem.codigo, "destino": destino.codigo}
