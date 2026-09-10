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
#      obras entre si. Em 10/09/2026 o princípio foi aplicado até o fim: a
#      retenção conjunta CSRF/PCC (que juntava PIS, COFINS e CSLL numa conta
#      só) foi desfeita, e cada parcela da guia vai para a conta do seu
#      próprio tributo, por rateio do DARF 5952.
#   2. DEDUTIBILIDADE NÃO É DA CONTA. É do documento; vive no título
#      (status PENDENTE/DEDUTIVEL/INDEDUTIVEL/PARCIAL). Aqui fica apenas uma
#      SUGESTÃO inicial para acelerar a triagem.
#   3. MEIO DE PAGAMENTO NÃO É CATEGORIA — mas FUNDO FIXO É. "Despesa com
#      cartão" e "BeeVale" saem do plano (são forma de pagamento). O fundo fixo
#      permanece como conta porque não é um meio de pagar: é um processo de
#      PRESTAÇÃO DE CONTAS entre o administrativo da obra e a empresa, com
#      comprovantes anexados e análise própria (tipo T10).
#   4. O QUE SERVE A OBRA APARECE NO CUSTO DA OBRA. O grupo 8 (aquisição de
#      bens) era FLUXO, fora da DRE. Virou RESULTADO em 10/09/2026, por razão
#      operacional que prevalece sobre a contábil: uma betoneira de R$ 4.000
#      comprada para uma obra em parceria PRECISA aparecer no custo daquela
#      obra — senão não há como cobrar a parte do parceiro. Como o rateio é
#      obrigatório em todo título, a distinção se resolve sozinha: bem de alto
#      valor comprado para a empresa fica rateado na matriz; bem que serve a
#      obra vai rateado para a obra. A depreciação fica com a contabilidade
#      externa, no balanço.
#   5. APORTE E DEVOLUÇÃO SÃO FLUXO, COM DIREÇÃO EXPLÍCITA. Contas separadas
#      para aporte recebido/concedido e devolução recebida/concedida, e o
#      parceiro/obra de destino é dito no rateio — resolve a confusão de
#      aportes entre obras próprias e obras em parceria.
#   6. REFORMA TRIBUTÁRIA. As contas de PIS e COFINS são mantidas (regime
#      atual) e já existem as de CBS e IBS para a transição 2026-2033, mais
#      a de Imposto Seletivo. Assim o período de coexistência não exige
#      remodelar o plano.
#   7. DEVOLUÇÃO NÃO É RECEITA — É CUSTO NEGATIVO. Devolução de material,
#      estorno de despesa e reembolso de custas estavam em 1.2 (receitas).
#      Com isso a obra aparecia com receita a mais e custo inalterado, e a
#      margem saía errada dos dois lados. Foram para o grupo 3 marcadas como
#      REDUTORAS: entram no relatório com sinal negativo. R$ 10.000 de compra
#      e R$ 500 de devolução mostram R$ 9.500 de custo, com as duas linhas
#      visíveis. O lançamento original NÃO é estornado.
#
# Colunas do plano: código, descrição, natureza (RESULTADO/FLUXO), sugestão de
# dedutibilidade, tipos de documento aceitos, descrição de uso (o "quando usar
# esta conta", que evita a escolha errada na hora do lançamento) e, opcional,
# `True` no sétimo lugar quando a conta é REDUTORA de custo.
#
# AS DESCRIÇÕES DE USO NÃO SÃO ENFEITE. Elas aparecem na tela na hora de
# lançar, e são o que impede o plano de apodrecer: quem lança escolhe a conta
# sob pressão, dezenas de vezes por dia, e a dúvida entre duas contas parecidas
# é o que produz relatório torto. Por isso toda conta em que há risco de
# confusão diz "quando usar" e "com o que não confundir".
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select, text
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

# Onde fica a fronteira entre FERRAMENTA (3.1.19, custo da obra) e EQUIPAMENTO
# DURÁVEL (8.1.04). Sem um número escrito nas duas descrições, a escolha vira
# loteria e o relatório fica torto — foi o pedido explícito do dono.
# O valor segue o critério fiscal de bem de pequeno valor; mudá-lo aqui muda os
# dois textos de uma vez.
LIMITE_FERRAMENTA = "R$ 1.200,00"

# (grupo_codigo, grupo_nome, subgrupo_codigo, subgrupo_nome, [categorias])
# categoria = (codigo, descricao, natureza, sugestao_dedutivel, tipos, uso)
#             e um sétimo item opcional: True quando a conta é REDUTORA.
PLANO: list[tuple[str, str, str, str, list[tuple]]] = [

    # ---------------------------------------------------------------- 1 RECEITAS
    ("1", "Receitas", "1.1", "Receita operacional", [
        ("1.1.01", "Receita de obras (medições)", "RESULTADO", True, [],
         "Medição aprovada e faturada da obra. É a receita principal."),
        ("1.1.02", "Receita de aditivos e reajustes", "RESULTADO", True, [],
         "Faturamento decorrente de aditivo contratual ou reajuste de índice."),
    ]),
    # 1.2.01 a 1.2.03 SAÍRAM daqui em 10/09/2026: devolução e estorno não são
    # receita, são custo negativo, e foram para 3.5 como contas redutoras.
    # Sobrou 1.2.04, que é receita de verdade — não houve gasto nosso.
    ("1", "Receitas", "1.2", "Outras receitas operacionais", [
        ("1.2.04", "Multas e indenizações recebidas", "RESULTADO", True, [],
         "Multa contratual cobrada de fornecedor ou indenização de seguro. "
         "É receita porque não houve gasto nosso: é dinheiro que entra pelo "
         "descumprimento de um terceiro. Devolução de material que compramos "
         "é outra coisa e vai em 3.5.01."),
    ]),
    ("1", "Receitas", "1.3", "Receitas financeiras", [
        ("1.3.01", "Rendimentos de aplicações financeiras", "RESULTADO", True, [],
         "Rendimento creditado pelo banco. O resgate do principal é 9.2."),
        ("1.3.02", "Juros e descontos obtidos", "RESULTADO", True, [],
         "Juros recebidos por atraso de cliente; desconto obtido em compra."),
    ]),
    ("1", "Receitas", "1.4", "Resultado na venda de ativos", [
        ("1.4.01", "Venda de imóveis", "RESULTADO", True, [],
         "O que entra aqui é o RESULTADO da venda (o ganho ou a perda), não o "
         "preço recebido. O dinheiro entrando aparece no fluxo, no grupo 9."),
        ("1.4.02", "Venda de veículos, máquinas e equipamentos", "RESULTADO", True, [],
         "Resultado da venda do bem, não o valor recebido. O dinheiro entrando "
         "aparece no fluxo, no grupo 9."),
        ("1.4.03", "Venda de outros bens do patrimônio", "RESULTADO", True, [],
         "Resultado da venda de móvel, ferramenta ou equipamento baixado. "
         "O dinheiro entrando aparece no fluxo, no grupo 9."),
    ]),

    # -------------------------------------------------- 2 TRIBUTOS (conta única)
    ("2", "Tributos", "2.1", "Tributos sobre receita e serviços", [
        ("2.1.01", "ISS sobre serviços", "RESULTADO", True, [GUI],
         "ISS da obra — retido pelo tomador na nota OU pago em guia. "
         "A forma de liquidação é informada no título, não muda a conta."),
        ("2.1.02", "INSS sobre serviços e obra (CEI/CNO)", "RESULTADO", True, [GUI],
         "INSS que incide sobre a OBRA ou sobre o SERVIÇO (matrícula CEI/CNO), "
         "retido na nota ou recolhido em GPS. Não confundir com 4.2.01, que é o "
         "INSS patronal da folha de pagamento da empresa."),
        ("2.1.03", "IRRF sobre serviços prestados", "RESULTADO", True, [GUI],
         "IR retido sobre o que NOS pagam pelo serviço prestado, ou recolhido "
         "por nós sobre serviço de terceiro. Não confundir com 4.2.03, que é o "
         "IRRF descontado do salário do empregado, nem com o IRPJ (2.2.01)."),
        ("2.1.04", "PIS sobre faturamento", "RESULTADO", True, [GUI],
         "PIS do regime atual, apurado sobre o faturamento OU retido na nota "
         "pelo tomador. A retenção conjunta (DARF 5952) é rateada entre esta "
         "conta, a 2.1.05 e a 2.2.02. Convive com a CBS durante a transição."),
        ("2.1.05", "COFINS sobre faturamento", "RESULTADO", True, [GUI],
         "COFINS do regime atual, apurada sobre o faturamento OU retida na nota. "
         "A retenção conjunta (DARF 5952) é rateada entre esta conta, a 2.1.04 "
         "e a 2.2.02."),
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
         "Imposto sobre o lucro da empresa, apurado no período OU antecipado "
         "por retenção na nota. Conta única: a forma é informação do título. "
         "Indedutível por natureza. Não confundir com 2.1.03 (IRRF do serviço)."),
        ("2.2.02", "CSLL", "RESULTADO", False, [GUI],
         "Contribuição sobre o lucro, apurada no período OU antecipada por "
         "retenção na nota — é o MESMO tributo em dois momentos, e por isso uma "
         "conta só. A parcela de CSLL da retenção conjunta (DARF 5952) é rateada "
         "para cá. Indedutível por natureza."),
    ]),
    ("2", "Tributos", "2.3", "Penalidades tributárias", [
        ("2.3.01", "Multas e juros tributários", "RESULTADO", False, [GUI],
         "Multa e juros de mora de tributo. Multa punitiva é indedutível."),
        ("2.3.02", "Parcelamentos tributários — juros", "RESULTADO", True, [GUI, FIN],
         "SÓ a parcela de juros do parcelamento. O PRINCIPAL vai para a conta "
         "do próprio tributo, no grupo 2 — INSS parcelado em 2.1.02, ISS em "
         "2.1.01, e assim por diante. Não existe conta de fluxo para "
         "parcelamento: dívida de tributo é custo, não movimentação."),
    ]),

    # ------------------------------------------------------- 3 CUSTOS DE OBRA
    # OS NOMES SÃO OS DA PLANILHA DA BWS, e isso é uma decisão, não um acaso.
    #
    # A primeira versão deste plano juntou categorias que a planilha separava
    # (argamassa com cimento, vidro com esquadria, gás com hidráulica) e
    # renomeou outras "para ficar melhor". O dono recusou em 07/09/2026, com
    # razão: "as nomenclaturas estão de acordo com a nossa realidade, as
    # pessoas que lançam já estão acostumadas com elas".
    #
    # Quem lança escolhe a conta numa lista, sob pressão, dezenas de vezes por
    # dia. Se o nome não é o que ela tem na cabeça, ela erra — e erro de conta
    # não aparece na tela, aparece na contabilidade meses depois. Elegância de
    # plano de contas não vale esse preço.
    #
    # AO MEXER AQUI: nome que existe na planilha da BWS não se junta com outro
    # e não se "melhora". Conta nova ganha código novo no fim da sequência —
    # renumerar quebraria o vínculo dos títulos já lançados.
    ("3", "Custos de obra", "3.1", "Materiais aplicados", [
        ("3.1.01", "Cimento e Concreto Usinado", "RESULTADO", True, [MAT, EXC],
         "Cimento em saco e concreto entregue pela usina. Argamassa pronta de "
         "saco (colante, reboco, graute) vai em 3.1.21."),
        ("3.1.02", "Agregados (Areia, Brita, Arisco)", "RESULTADO", True, [MAT, EXC],
         "Material a granel: areia, brita, arisco, pó de pedra, rachão, "
         "pedrisco e brita graduada."),
        ("3.1.03", "Armadura", "RESULTADO", True, [MAT, EXC],
         "Aço CA-50 e CA-60, tela soldada, arame recozido e espaçador — o que "
         "forma a ferragem do concreto."),
        ("3.1.04", "Elementos de Vedação (Tijolo, Blocos e Paredes PVC)",
         "RESULTADO", True, [MAT, EXC],
         "O que FECHA a parede: tijolo, bloco cerâmico ou de concreto, painel "
         "de PVC, divisória. Peça de concreto que chega pronta (poste, "
         "meio-fio, verga, tubo) vai em 3.1.05."),
        ("3.1.05", "Pré-Moldados de Concreto", "RESULTADO", True, [MAT, EXC],
         "Peça de concreto que chega PRONTA da fábrica: laje, viga, poste, "
         "meio-fio, tubo, aduela, bloco de intertravado. Bloco e tijolo de "
         "vedação vão em 3.1.04."),
        ("3.1.06", "Telhas e Material p/ Coberturas", "RESULTADO", True, [MAT, EXC],
         "Telha, cumeeira, rufo, calha, manta subcobertura e acessório de "
         "fixação. O perfil metálico que sustenta o telhado vai em 3.1.22."),
        ("3.1.07", "Madeiramento", "RESULTADO", True, [MAT, EXC],
         "Madeira COMPRADA para forma, escoramento e madeiramento de telhado: "
         "compensado, sarrafo, caibro, ripa, pontalete. Forma e escoramento "
         "ALUGADOS vão em 3.3.06."),
        ("3.1.08", "Material Elétrico", "RESULTADO", True, [MAT, EXC],
         "Tudo de energia: cabo, fio, eletroduto, disjuntor, quadro, tomada, "
         "interruptor, luminária e SPDA. Cabo de rede, câmera e material de "
         "dados vão em 3.1.23."),
        ("3.1.09", "Material Hidráulico e Sanitário", "RESULTADO", True, [MAT, EXC],
         "A tubulação escondida: tubo, conexão, registro, caixa d'água, caixa "
         "de gordura e material de esgoto. O acabamento aparente (vaso, "
         "torneira) vai em 3.1.13; a rede de gás vai em 3.1.24."),
        ("3.1.10", "Material p/ Combate à Incêndio", "RESULTADO", True, [MAT, EXC],
         "Hidrante, extintor, sprinkler, mangueira, abrigo, sinalização de "
         "emergência e a tubulação exclusiva da rede de incêndio."),
        ("3.1.11", "Material p/ Climatização", "RESULTADO", True, [MAT, EXC],
         "Aparelho de ar-condicionado, duto, isolamento, tubulação de cobre, "
         "dreno e suporte de condensadora."),
        ("3.1.12", "Pisos, Cerâmicas e Revestimentos", "RESULTADO", True, [MAT, EXC],
         "Porcelanato, cerâmica, rodapé, revestimento de parede, rejunte e "
         "argamassa de assentamento pronta. Bancada, soleira e peitoril de "
         "pedra vão em 3.1.25."),
        ("3.1.13", "Louças e Metais", "RESULTADO", True, [MAT, EXC],
         "O acabamento sanitário aparente: vaso, cuba, tanque, torneira, "
         "chuveiro, sifão, válvula e acessório. Tubo e conexão vão em 3.1.09."),
        ("3.1.14", "Esquadrias de Alumínio, Metal e Madeira",
         "RESULTADO", True, [MAT, EXC],
         "Porta, janela, portão, batente e guichê COMPRADOS prontos, em "
         "alumínio, ferro, madeira ou vidro. Tubo e perfil comprados para "
         "FABRICAR na obra vão em 3.1.26."),
        ("3.1.15", "Material para Pintura", "RESULTADO", True, [MAT, EXC],
         "Tinta, massa corrida, selador, verniz, solvente, lixa, rolo, pincel "
         "e fita crepe."),
        ("3.1.16", "Material p/ Fôrro", "RESULTADO", True, [MAT, EXC],
         "Forro de PVC, gesso e drywall, com perfil, montante, guia, tabica e "
         "acessório de fixação."),
        ("3.1.17", "Impermeabilizantes, Aditivos e Colas", "RESULTADO", True, [MAT, EXC],
         "Produto químico que se aplica LÍQUIDO ou em manta: manta asfáltica, "
         "emulsão, aditivo de concreto, silicone, espuma expansiva, adesivo e "
         "selante. Argamassa colante de saco vai em 3.1.21."),
        ("3.1.18", "Parafusos, Ferragens e Acessórios", "RESULTADO", True, [MAT, EXC],
         "A miudeza que fixa e movimenta: parafuso, bucha, prego, arruela, "
         "abraçadeira, dobradiça, fechadura, trinco e eletrodo de solda. "
         "Tubo, metalon e perfil para fabricar peça vão em 3.1.26."),
        ("3.1.19", "Ferramentas", "RESULTADO", True, [MAT, FFX, EXC],
         "Ferramenta de CONSUMO — a que se gasta, quebra ou se perde na obra: "
         f"colher, martelo, disco de corte, broca, serrote. Critério: até "
         f"{LIMITE_FERRAMENTA} por unidade OU vida útil menor que um ano. "
         "Acima disso é equipamento durável e vai em 8.1.04. Conserto de "
         "ferramenta vai em 5.3.04; ferramenta alugada, em 3.3.06."),
        ("3.1.20", "Jardinagem", "RESULTADO", True, [MAT, SRV, EXC],
         "Grama, muda, terra vegetal, adubo, substrato e material de "
         "paisagismo, e o serviço de plantio."),
        # Estas seis nasceram em 07/09/2026, ao desfazer as junções. Códigos no
        # fim da sequência de propósito: os títulos já lançados apontam para os
        # números antigos, e renumerar quebraria o vínculo deles.
        ("3.1.21", "Argamassas", "RESULTADO", True, [MAT, EXC],
         "Argamassa que vem PRONTA em saco: colante AC-I/AC-II/AC-III, de "
         "reboco, de assentamento, graute e rejunte em pó. Cimento puro e "
         "concreto de usina vão em 3.1.01; aditivo e cola líquida, em 3.1.17."),
        ("3.1.22", "Estrutura Metálica", "RESULTADO", True, [MAT, EXC],
         "O metal que SUSTENTA a obra: perfil estrutural, treliça, terça, "
         "tesoura, chapa e pilar metálico. Tubo e metalon de serralheria "
         "(grade, guarda-corpo, portão) vão em 3.1.26."),
        ("3.1.23", "Material p/ Cabeamento Estruturado e CFTV",
         "RESULTADO", True, [MAT, EXC],
         "Dados e imagem: cabo de rede, patch panel, rack, conector, câmera, "
         "DVR, switch e cabo coaxial. Cabo de energia vai em 3.1.08."),
        ("3.1.24", "Material p/ Gás", "RESULTADO", True, [MAT, EXC],
         "A rede de gás: tubo de cobre, registro, regulador, central de GLP, "
         "detector e ventilação exigida. Tubulação de água vai em 3.1.09."),
        ("3.1.25", "Bancadas de Granito", "RESULTADO", True, [MAT, EXC],
         "Granito, mármore e pedra natural em bancada, soleira, peitoril e "
         "rodapé de pedra, com o acabamento. Piso e revestimento cerâmico vão "
         "em 3.1.12."),
        ("3.1.26", "Materais p/ Serralheria (Tubos, Metalon, Perfis, etc)",
         "RESULTADO", True, [MAT, EXC],
         "Tubo, metalon, cantoneira, barra chata e chapa comprados para "
         "FABRICAR peça na obra: grade, guarda-corpo, corrimão, portão. Peça "
         "pronta comprada vai em 3.1.14; estrutura que sustenta a obra, em "
         "3.1.22; parafuso e ferragem, em 3.1.18."),
        ("3.1.27", "Vidros e Espelhos", "RESULTADO", True, [MAT, EXC],
         "Vidro, espelho, box e ferragem de vidro temperado comprados avulsos. "
         "Janela ou porta que já vem com o vidro vai em 3.1.14."),
        ("3.1.99", "Outros Materiais", "RESULTADO", True, [MAT, EXC],
         "ÚLTIMO RECURSO: use só quando nenhuma conta acima servir. Esta conta "
         "deve ser revisada de tempos em tempos — se um item aparece repetido "
         "aqui, ele merece conta própria. Deixada solta, ela vira depósito de "
         "tudo e o relatório de materiais perde o sentido."),
    ]),
    ("3", "Custos de obra", "3.2", "Serviços de terceiros", [
        ("3.2.01", "Empreiteiros e subcontratação (com medição)", "RESULTADO", True, [EMP],
         "EXECUÇÃO de etapa da obra por empresa, com contrato e MEDIÇÃO: "
         "alvenaria, estrutura, instalações, acabamento. Exige CND e NFS-e. "
         "Projeto, laudo, sondagem e topografia — que entregam documento, não "
         "obra feita — vão em 3.2.02."),
        ("3.2.02", "Serviços técnicos PJ aplicados à obra", "RESULTADO", True, [SRV],
         "Serviço técnico que entrega DOCUMENTO ou parecer: projeto, sondagem, "
         "topografia, laudo, ensaio, consultoria de engenharia, instalação "
         "especializada pontual. Execução com medição vai em 3.2.01."),
        ("3.2.03", "Serviços de pessoa física (RPA)", "RESULTADO", True, [RPA],
         "Autônomo sem empresa. EXIGE RPA — recibo de pagamento a autônomo — e "
         "GERA dois tributos que precisam ser lançados junto: INSS (2.1.02) e "
         "IRRF (2.1.03). Sem RPA o pagamento não deve ser feito por aqui."),
        ("3.2.04", "Fretes, carretos e transporte de material", "RESULTADO", True, [FRE, EXC],
         "Frete, carreto e transporte de material até a obra, com CT-e ou nota "
         "do transportador."),
        ("3.2.05", "Material p/ Limpeza", "RESULTADO", True, [SRV, MAT, EXC],
         "Material e serviço de limpeza DA OBRA e do canteiro, inclusive a "
         "limpeza final de entrega. A limpeza da sede vai em 5.1.05."),
        ("3.2.06", "Alimentação de equipe em obra", "RESULTADO", True, [SRV, FFX, REE],
         "Fornecimento terceirizado de refeições no canteiro."),
        ("3.2.07", "Segurança e vigilância de obra", "RESULTADO", True, [SRV],
         "Vigilância e portaria do canteiro. A da sede vai em 5.1.06."),
    ]),
    ("3", "Custos de obra", "3.3", "Locações", [
        ("3.3.01", "Locação de Máquinas, Veículos e Equipamentos", "RESULTADO", True, [LOC],
         "Conta GENÉRICA de locação. Use quando o mesmo contrato junta máquina, "
         "veículo e equipamento num aluguel só, ou quando o item não couber nas "
         "específicas: máquina pesada em 3.3.02, veículo em 3.3.03, imóvel de "
         "apoio em 3.3.04, equipamento de obra em 3.3.06. Exige contrato "
         "vigente."),
        ("3.3.02", "Locação de Máquinas Pesadas", "RESULTADO", True, [LOC],
         "Escavadeira, retroescavadeira, pá-carregadeira, rolo compactador, "
         "guindaste e motoniveladora — com operador ou por hora-máquina."),
        ("3.3.03", "Locação de Veículos", "RESULTADO", True, [LOC],
         "Caminhão, caminhonete, van e carro alugados por mês ou por diária."),
        ("3.3.04", "Locação de imóveis para obra (canteiro/alojamento)",
         "RESULTADO", True, [LOC],
         "É AQUI que entra a CASA DE APOIO À OBRA: alojamento, barracão, "
         "galpão e terreno de canteiro. Não em 5.1.01, que é o aluguel da "
         "sede da empresa."),
        # 3.3.06 substituiu a antiga 3.3.05 ("Locação de andaimes, escoramentos
        # e formas") em 10/09/2026: é o nome que a planilha de insumos da BWS
        # usa, e ele cobre também betoneira, gerador e compactador, que na
        # conta antiga não cabiam.
        ("3.3.06", "Locação de Equipamentos", "RESULTADO", True, [LOC],
         "Equipamento de obra alugado: andaime, escoramento, fôrma, betoneira, "
         "compactador, gerador, martelete, bomba, container. Máquina pesada "
         "vai em 3.3.02; veículo, em 3.3.03."),
    ]),
    ("3", "Custos de obra", "3.4", "Despesas indiretas de obra", [
        ("3.4.01", "Água e energia da obra", "RESULTADO", True, [CON, EXC],
         "Conta de concessionária do canteiro. A da sede vai em 5.1.02."),
        ("3.4.02", "Combustível de veículos e máquinas em obra",
         "RESULTADO", True, [MAT, FFX, REE],
         "Diesel, gasolina e arla de veículo e máquina A SERVIÇO DA OBRA. "
         "Combustível do administrativo vai em 5.3.02."),
        ("3.4.03", "EPI (Equipamento de Proteção Individual)", "RESULTADO", True, [MAT, SRV],
         "Capacete, bota, luva, óculos, protetor, cinto e o fardamento da "
         "equipe de obra."),
        ("3.4.04", "Exames ocupacionais e medicina do trabalho",
         "RESULTADO", True, [SRV],
         "ASO, exame admissional e demissional, PCMSO e PGR."),
        ("3.4.05", "Mobilização, desmobilização e canteiro",
         "RESULTADO", True, [MAT, SRV, EXC],
         "Montagem e desmontagem do canteiro, tapume, placa de obra, ligação "
         "provisória e transporte de equipe para a mobilização."),
        ("3.4.06", "Taxas, licenças e ART/RRT da obra", "RESULTADO", True, [GUI, EXC],
         "Taxa de alvará, licença ambiental, habite-se e a ART/RRT do "
         "responsável técnico DESTA obra — que é custo da obra. A ANUIDADE do "
         "CREA/CAU da empresa e do profissional é administrativa e vai em "
         "5.2.04."),
        ("3.4.07", "Seguros da obra (risco de engenharia, garantia)",
         "RESULTADO", True, [SRV, EXC],
         "Seguro ligado a ESTA obra: risco de engenharia, garantia de execução, "
         "responsabilidade civil da obra. Seguro de frota e patrimônio da "
         "empresa vai em 6.2.03."),
        ("3.4.08", "Fundo fixo da obra — prestação de contas", "RESULTADO", True, [FFX],
         "Reembolso ao administrativo da obra mediante prestação de contas com os "
         "comprovantes anexados. Item de valor relevante e com nota própria deve ser "
         "lançado na sua conta específica; o fundo fixo cobre a miudeza do canteiro."),
    ]),
    # ------------------------------------------------- 3.5 REDUÇÕES DE CUSTO
    # Nasceu em 10/09/2026. Estas três contas estavam em 1.2 como "outras
    # receitas operacionais", e não são receita: são dinheiro nosso voltando.
    # Enquanto ficaram no grupo 1, a obra aparecia com receita a mais e custo
    # inalterado — a margem saía errada dos dois lados.
    # Elas são REDUTORAS: entram no relatório com sinal negativo. O lançamento
    # original NÃO é estornado; as duas linhas continuam visíveis no histórico.
    ("3", "Custos de obra", "3.5", "Reduções de custo", [
        ("3.5.01", "Devolução de material a fornecedor", "RESULTADO", True, [MAT, EXC],
         "Crédito por material devolvido ao fornecedor. ABATE o custo: se houve "
         "R$ 10.000 de compra e R$ 500 de devolução, o relatório mostra R$ 9.500. "
         "Não estorne o lançamento da compra — as duas linhas ficam no histórico.",
         True),
        ("3.5.02", "Estorno de despesas", "RESULTADO", True, [EXC, REE],
         "Devolução de valor pago a maior, serviço cancelado com dinheiro de "
         "volta, cobrança indevida ressarcida. ABATE o custo do período.",
         True),
        ("3.5.03", "Reembolso de custas e despesas processuais",
         "RESULTADO", True, [EXC, REE],
         "Custas processuais e despesas de processo que nos foram ressarcidas "
         "por terceiro ou pelo judiciário. ABATE o custo. Multa ou indenização "
         "recebida por descumprimento de terceiro é receita e vai em 1.2.04.",
         True),
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
         "Encargo da EMPRESA sobre a folha de pagamento. Não confundir com "
         "2.1.02, que é o INSS da obra ou do serviço (matrícula CEI/CNO), "
         "retido na nota ou pago em GPS da obra."),
        ("4.2.02", "FGTS", "RESULTADO", True, [FOL, GUI], ""),
        ("4.2.03", "IRRF sobre a folha", "RESULTADO", True, [FOL, GUI],
         "IR descontado DO EMPREGADO no contracheque e recolhido pela empresa. "
         "Não confundir com 2.1.03, que é o IRRF do serviço prestado."),
        ("4.2.04", "Contribuição sindical e assistencial", "RESULTADO", True, [GUI, FOL], ""),
    ]),
    ("4", "Pessoal", "4.3", "Benefícios", [
        ("4.3.01", "Vale-transporte e deslocamento", "RESULTADO", True, [FOL, SRV], ""),
        ("4.3.02", "Vale-alimentação e cesta básica", "RESULTADO", True, [FOL, SRV],
         "Benefício em cartão ou cesta entregue ao empregado. Refeição servida "
         "no canteiro por empresa terceirizada vai em 3.2.06."),
        ("4.3.03", "Plano de saúde e seguro de vida", "RESULTADO", True, [FOL, SRV], ""),
        ("4.3.04", "Outros benefícios", "RESULTADO", True, [FOL, SRV], ""),
    ]),
    ("4", "Pessoal", "4.4", "Desenvolvimento e contencioso", [
        ("4.4.01", "Cursos, treinamentos e certificações", "RESULTADO", True, [SRV],
         "NR-35, NR-10, NR-18 e demais treinamentos obrigatórios, cursos e "
         "certificações da equipe."),
        ("4.4.02", "Acordos e condenações trabalhistas", "RESULTADO", True, [EXC, SRV],
         "Verba indenizatória de acordo; multa punitiva costuma ser indedutível."),
    ]),

    # ------------------------------------------------- 5 DESPESAS ADMINISTRATIVAS
    ("5", "Despesas administrativas", "5.1", "Estrutura", [
        ("5.1.01", "Aluguel e condomínio da sede", "RESULTADO", True, [LOC],
         "Aluguel, condomínio e IPTU do ESCRITÓRIO. Casa de apoio à obra, "
         "alojamento, barracão ou terreno de canteiro NÃO entram aqui: vão em "
         "3.3.04."),
        ("5.1.02", "Água e energia da sede", "RESULTADO", True, [CON],
         "Concessionária do escritório. A da obra vai em 3.4.01."),
        ("5.1.03", "Internet, telefonia e sistemas", "RESULTADO", True, [CON, SRV],
         "ASSINATURA MENSAL: link de internet, telefonia, plano de celular e "
         "sistema pago por mensalidade (nuvem, licença mensal). Compra de "
         "licença perpétua ou desenvolvimento sob medida vai em 8.3.01."),
        ("5.1.04", "Conservação e manutenção predial", "RESULTADO", True, [SRV, MAT],
         "Reparo que MANTÉM o imóvel como está: conserto, troca de peça, "
         "pintura de manutenção, desentupimento. Reforma que VALORIZA o bem e "
         "aumenta a vida útil vai em 8.2.02."),
        ("5.1.05", "Limpeza da sede", "RESULTADO", True, [SRV, MAT],
         "Serviço e material de limpeza do escritório. A limpeza da obra e do "
         "canteiro vai em 3.2.05."),
        ("5.1.06", "Segurança e vigilância da sede", "RESULTADO", True, [SRV],
         "Alarme, monitoramento e portaria do escritório. A do canteiro vai em "
         "3.2.07."),
    ]),
    ("5", "Despesas administrativas", "5.2", "Serviços profissionais", [
        ("5.2.01", "Contabilidade", "RESULTADO", True, [SRV], ""),
        ("5.2.02", "Assessoria jurídica e advogados", "RESULTADO", True, [SRV],
         "Honorários de advogado e assessoria. Custas do processo ressarcidas "
         "depois aparecem em 3.5.03."),
        ("5.2.03", "Auditoria e consultorias", "RESULTADO", True, [SRV], ""),
        ("5.2.04", "Cartórios, CREA e taxas administrativas", "RESULTADO", True, [GUI, REE],
         "Cartório, certidão, junta comercial e a ANUIDADE do CREA/CAU da "
         "empresa e dos profissionais — que é despesa administrativa, não de "
         "obra. A ART/RRT de uma obra específica vai em 3.4.06."),
    ]),
    ("5", "Despesas administrativas", "5.3", "Operação administrativa", [
        ("5.3.10", "Fundo fixo administrativo — prestação de contas",
         "RESULTADO", True, [FFX],
         "Mesmo rito do fundo fixo de obra, para despesas miúdas da sede."),
        ("5.3.01", "Material de Escritório", "RESULTADO", True, [MAT, FFX],
         "Papel, caneta, cartucho, impressão, pasta e miudeza do escritório."),
        ("5.3.02", "Combustível administrativo", "RESULTADO", True, [MAT, FFX, REE],
         "Combustível de veículo do administrativo e da diretoria. O de "
         "veículo e máquina a serviço da obra vai em 3.4.02."),
        ("5.3.03", "Manutenção (Veículos e Máquinas)", "RESULTADO", True, [SRV, MAT],
         "Peça, óleo, pneu, revisão e serviço de oficina da frota e das "
         "máquinas. A COMPRA do veículo ou da máquina vai em 8.1.01."),
        ("5.3.04", "Manutenção (Ferramentas e Equipamentos)", "RESULTADO", True, [SRV, MAT],
         "Conserto e peça de reposição de ferramenta e equipamento. A COMPRA "
         "da ferramenta vai em 3.1.19 (consumo) ou 8.1.04 (durável)."),
        ("5.3.05", "Viagens, hospedagem e deslocamento",
         "RESULTADO", True, [REE, SRV, FFX], ""),
        ("5.3.06", "Taxas, licenciamento e IPVA de veículos", "RESULTADO", True, [GUI], ""),
        ("5.3.07", "Multas de trânsito", "RESULTADO", False, [GUI, REE],
         "Indedutível por natureza (infração)."),
        ("5.3.08", "Marketing, brindes e relacionamento", "RESULTADO", True, [SRV, MAT],
         "Brinde sem vínculo com a atividade tende a ser indedutível — verificar."),
        ("5.3.99", "Outras despesas administrativas",
         "RESULTADO", True, [MAT, SRV, REE, EXC],
         "ÚLTIMO RECURSO: use só quando nenhuma conta administrativa servir. "
         "Deve ser revisada de tempos em tempos — item que se repete aqui "
         "merece conta própria. Deixada solta, vira depósito de tudo e o "
         "relatório de despesas perde o sentido."),
    ]),

    # --------------------------------------------------------- 6 FINANCEIRAS
    ("6", "Despesas financeiras", "6.1", "Custo do dinheiro", [
        ("6.1.01", "Juros sobre empréstimos e financiamentos", "RESULTADO", True, [FIN],
         "SÓ os juros e encargos da parcela. O PRINCIPAL não é despesa — é "
         "devolução de dinheiro emprestado — e vai em 9.4.02. Juros de "
         "parcelamento de tributo vão em 2.3.02."),
        ("6.1.02", "Juros e multas por atraso a fornecedores", "RESULTADO", False, [EXC],
         "Apurar quem deu causa antes de aceitar."),
        ("6.1.03", "Descontos concedidos e antecipação de recebíveis",
         "RESULTADO", True, [EXC], ""),
    ]),
    ("6", "Despesas financeiras", "6.2", "Serviços bancários e garantias", [
        ("6.2.01", "Tarifas bancárias e IOF", "RESULTADO", True, [EXC], ""),
        ("6.2.02", "Seguro-garantia e fiança bancária", "RESULTADO", True, [SRV, EXC],
         "Garantia contratual exigida do contrato. Seguro de risco de "
         "engenharia da obra vai em 3.4.07."),
        ("6.2.03", "Seguros corporativos (frota, patrimônio, RC)",
         "RESULTADO", True, [SRV],
         "Seguro da empresa como um todo. Seguro ligado a uma obra específica "
         "vai em 3.4.07."),
    ]),

    # --------------------------------------------------------- 8 INVESTIMENTOS
    # NATUREZA RESULTADO desde 10/09/2026 — ver princípio 4 no topo. Estas
    # contas eram FLUXO e ficavam fora da DRE; com isso um bem comprado para
    # uma obra em parceria não aparecia no custo dela, e não havia como cobrar
    # a parte do parceiro. O rateio obrigatório resolve: o bem vai para a obra
    # que ele serve, ou para a matriz. A depreciação fica com a contabilidade
    # externa, no balanço.
    ("8", "Investimentos (ativo)", "8.1", "Aquisição de bens", [
        ("8.1.01", "Aquisição de Veículos, Máquinas e Equipamentos",
         "RESULTADO", True, [MAT, FIN],
         "COMPRA de veículo, máquina ou equipamento de valor relevante. Rateie "
         "para a obra que o bem vai servir — é isso que permite cobrar a parte "
         "do parceiro. Aluguel vai em 3.3; conserto, em 5.3.03."),
        ("8.1.02", "Equipamentos de Informática", "RESULTADO", True, [MAT],
         "Computador, notebook, impressora, servidor e periférico. O programa "
         "que roda neles vai em 8.3.01 (licença) ou 5.1.03 (mensalidade)."),
        ("8.1.03", "Móveis e Utensílios", "RESULTADO", True, [MAT],
         "Mesa, cadeira, armário, geladeira e utensílio de escritório ou de "
         "alojamento."),
        ("8.1.04", "Ferramentas e equipamentos duráveis", "RESULTADO", True, [MAT],
         "Equipamento que DURA ANOS e fica no patrimônio: furadeira "
         "profissional, serra, betoneira, andaime próprio, compressor. "
         f"Critério: acima de {LIMITE_FERRAMENTA} por unidade E vida útil maior "
         "que um ano. Abaixo disso é ferramenta de consumo e vai em 3.1.19."),
    ]),
    ("8", "Investimentos (ativo)", "8.2", "Imóveis e benfeitorias", [
        ("8.2.01", "Aquisição de imóveis", "RESULTADO", True, [EXC],
         "Compra de terreno ou imóvel, com ITBI e escritura."),
        ("8.2.02", "Benfeitorias e reformas que valorizam o imóvel",
         "RESULTADO", True, [SRV, MAT],
         "Obra que VALORIZA o imóvel e aumenta a vida útil: ampliação, troca "
         "de cobertura, reforma estrutural, nova instalação. Reparo que apenas "
         "mantém o bem como está vai em 5.1.04."),
    ]),
    ("8", "Investimentos (ativo)", "8.3", "Intangíveis", [
        ("8.3.01", "Software, licenças e desenvolvimento", "RESULTADO", True, [SRV, MAT],
         "COMPRA de licença perpétua e desenvolvimento de sistema sob medida. "
         "Assinatura mensal de sistema ou nuvem vai em 5.1.03."),
    ]),

    # ------------------------------------------- 9 MOVIMENTAÇÕES FINANCEIRAS
    ("9", "Movimentações financeiras", "9.1", "Transferências", [
        ("9.1.01", "Transferência entre contas da empresa", "FLUXO", True, [EXC],
         "Saída de uma conta e entrada em outra — não é despesa."),
        ("9.1.02", "Valores de terceiros recebidos e devolvidos", "FLUXO", True, [EXC],
         "Dinheiro que caiu na NOSSA conta por engano e foi devolvido. A MESMA "
         "conta registra as duas pontas: a entrada e a devolução. Saldo "
         "diferente de zero significa que alguma devolução ficou pendente."),
        ("9.1.03", "Pagamentos por conta errada e ressarcimentos", "FLUXO", True, [EXC],
         "Conta NOSSA paga pela conta bancária errada (a da outra empresa, a do "
         "sócio) e o ressarcimento feito depois. As duas pontas na mesma conta; "
         "saldo zero é o caso encerrado, saldo diferente de zero é pendência."),
    ]),
    ("9", "Movimentações financeiras", "9.2", "Aplicações", [
        ("9.2.01", "Aplicação financeira — saída", "FLUXO", True, [EXC],
         "Dinheiro que sai da conta corrente para a aplicação. O rendimento é "
         "receita e vai em 1.3.01."),
        ("9.2.02", "Resgate de aplicação — entrada", "FLUXO", True, [],
         "Dinheiro que volta da aplicação para a conta corrente."),
    ]),
    # A DIREÇÃO é o que confunde no grupo 9.3. Leia o nome inteiro antes de
    # escolher: RECEBIDO é dinheiro ENTRANDO na empresa; CONCEDIDO é dinheiro
    # SAINDO da empresa. Aporte e devolução são as duas metades de um mesmo
    # ciclo — o aporte vai, a devolução volta.
    ("9", "Movimentações financeiras", "9.3", "Aportes e parcerias", [
        ("9.3.01", "Aporte recebido de sócio", "FLUXO", True, [],
         "ENTRA dinheiro: o sócio põe recurso na empresa."),
        ("9.3.02", "Aporte recebido de parceiro em obra", "FLUXO", True, [],
         "ENTRA dinheiro: o parceiro aporta na obra em parceria. A obra é dita "
         "no rateio."),
        ("9.3.03", "Aporte concedido a obra própria", "FLUXO", True, [EXC],
         "SAI dinheiro: a empresa injeta recurso em obra própria (o rateio "
         "indica a obra)."),
        ("9.3.04", "Aporte concedido a obra em parceria", "FLUXO", True, [EXC],
         "SAI dinheiro: a nossa parte no aporte da obra em parceria."),
        ("9.3.05", "Devolução de aporte recebida", "FLUXO", True, [],
         "ENTRA dinheiro: a obra ou o parceiro devolve à empresa o que ela "
         "havia aportado. É a volta de 9.3.03 ou 9.3.04."),
        ("9.3.06", "Devolução de aporte concedida", "FLUXO", True, [EXC],
         "SAI dinheiro: a empresa devolve ao sócio ou ao parceiro o aporte que "
         "ele fez. É a volta de 9.3.01 ou 9.3.02."),
    ]),
    # 9.4.03 (principal de parcelamento tributário) foi eliminada em
    # 10/09/2026: gerava confusão sem necessidade. O principal do parcelamento
    # vai para a conta do próprio tributo, no grupo 2; os juros, em 2.3.02.
    ("9", "Movimentações financeiras", "9.4", "Empréstimos e financiamentos", [
        ("9.4.01", "Captação de empréstimo — entrada", "FLUXO", True, [],
         "ENTRA dinheiro: o empréstimo caiu na conta."),
        ("9.4.02", "Pagamento de principal de empréstimo", "FLUXO", True, [FIN],
         "SÓ o principal — a devolução do dinheiro emprestado, que não é "
         "despesa. Os juros da parcela vão em 6.1.01."),
    ]),
    ("9", "Movimentações financeiras", "9.5", "Sócios", [
        ("9.5.01", "Distribuição de lucros e dividendos", "FLUXO", True, [EXC], ""),
        ("9.5.02", "Adiantamento a fornecedor (encontro de contas)", "FLUXO", True, [ADI],
         "Trânsito até a entrega; baixa contra a nota definitiva."),
    ]),
]


# ---------------------------------------------------------------------------
# APELIDOS — nomes antigos e variações que apontam para a mesma conta
#
# Os importadores (insumos, obras) e o de-para do Omie casam a conta pelo NOME
# escrito na planilha. A planilha da BWS escreve "Manutenção (Veículos e
# Máquinas)"; o plano escrevia "Manutenção de veículos e máquinas". Sem esta
# tabela, a mesma conta deixaria de ser encontrada por causa de um parêntese —
# e o insumo entraria sem conta do plano, silenciosamente.
#
# Quando uma conta for RENOMEADA, o nome antigo entra aqui. Não se apaga
# apelido: planilha velha continua sendo importada anos depois.
# ---------------------------------------------------------------------------
APELIDOS: dict[str, str] = {
    # renomeadas em 10/09/2026 para os nomes da planilha de insumos da BWS
    "Veículos, máquinas e equipamentos": "8.1.01",
    "Veículos, Máquinas e Equipamentos": "8.1.01",
    "Manutenção de veículos e máquinas": "5.3.03",
    "Manutenção de ferramentas e equipamentos": "5.3.04",
    "Material de escritório e impressões": "5.3.01",
    "Locação de máquinas pesadas": "3.3.02",
    "Locação de veículos": "3.3.03",
    # renomeadas em 10/09/2026 por decisão de nomenclatura
    "Internet, telefonia e software": "5.1.03",
    "Limpeza e copa da sede": "5.1.05",
    # contas que mudaram de código ao mudar de grupo
    "Locação de andaimes, escoramentos e formas": "3.3.06",
    "Devolução de material a fornecedor": "3.5.01",
    "Estorno de despesas": "3.5.02",
    "Reembolso de custas e despesas processuais": "3.5.03",
}


# ---------------------------------------------------------------------------
# APOSENTADORIAS — contas que saíram do plano
#
# Conta com movimento NÃO some. Some o que nunca foi usado; o que tem
# lançamento é RELATADO para o dono decidir, e continua funcionando até ele
# decidir. É por isso que esta tabela guarda o motivo: quem lê o relatório
# precisa entender o que aconteceu sem abrir o código.
#
# destino = None quer dizer que não existe um destino único: o valor da conta
# antiga se reparte entre várias, e só uma pessoa pode dizer como. Nesse caso a
# conta nunca é aposentada sozinha.
# ---------------------------------------------------------------------------
APOSENTADORIAS: dict[str, tuple[Optional[str], str]] = {
    "1.2.01": ("3.5.01", "Devolução de material não é receita: virou conta "
                         "redutora de custo, no grupo 3."),
    "1.2.02": ("3.5.02", "Estorno de despesa não é receita: virou conta "
                         "redutora de custo, no grupo 3."),
    "1.2.03": ("3.5.03", "Reembolso de custas não é receita: virou conta "
                         "redutora de custo, no grupo 3."),
    "2.1.06": (None, "A retenção conjunta CSRF/PCC juntava três tributos com "
                     "alíquotas diferentes num só lugar. Agora a guia (DARF "
                     "5952) é RATEADA entre PIS (2.1.04), COFINS (2.1.05) e "
                     "CSLL (2.2.02). Como o valor se reparte, não há destino "
                     "único: o remanejamento é caso a caso."),
    "9.4.03": (None, "Parcelamento de tributo deixou de ser movimentação "
                     "financeira. O PRINCIPAL vai para a conta do próprio "
                     "tributo, no grupo 2 (INSS em 2.1.02, ISS em 2.1.01, e "
                     "assim por diante), e os JUROS em 2.3.02. O destino "
                     "depende de qual tributo foi parcelado."),
    "3.3.05": ("3.3.06", "Substituída por 'Locação de Equipamentos', que é o "
                         "nome da planilha da BWS e cobre também betoneira, "
                         "gerador e compactador."),
}


def _tipos(lista: list[str]) -> list[TipoTitulo]:
    return [TipoTitulo(t) for t in lista]


def _lancamentos_na_conta(s: Session, categoria_id: int) -> int:
    """Quantos títulos usam esta conta — direto ou pelo rateio.

    Contado por SQL agregado de propósito: carregar os títulos em memória para
    contar derrubaria a instância, que tem 2 GB e divide o processo com os
    outros módulos do monorepo.
    """
    linha = s.execute(text("""
        SELECT COUNT(*) FROM titulos t
         WHERE t.categoria_id = :c
            OR EXISTS (SELECT 1 FROM rateios r
                        WHERE r.titulo_id = t.id AND r.categoria_id = :c)
    """), {"c": categoria_id}).scalar()
    return int(linha or 0)


def aplicar_plano(s: Session, usuario: Optional[Usuario] = None,
                  sobrescrever_descricoes: bool = True) -> dict[str, Any]:
    """Grava o plano no banco. Idempotente: cria o que falta e atualiza o que
    mudou. Contas criadas pela BWS e contas RENOMEADAS pelo usuário
    (personalizada=True) têm o texto preservado.

    Também APOSENTA as contas que saíram do plano — mas só as que nunca foram
    usadas. Conta com lançamento continua ativa e sai no relatório em
    `pendentes_de_migracao`, para o dono remanejar pela tela de substituir
    conta, que leva o histórico junto. Histórico não se apaga por instalação
    de plano padrão.
    """
    criadas, atualizadas = [], []
    ordem = 0
    for grupo_cod, grupo_nome, sub_cod, sub_nome, categorias in PLANO:
        for linha in categorias:
            cod, desc, natureza, ded, tipos, uso = linha[:6]
            redutora = bool(linha[6]) if len(linha) > 6 else False
            ordem += 1
            cat = s.scalars(select(Categoria).where(Categoria.codigo == cod)).first()
            if cat is None:
                s.add(Categoria(
                    codigo=cod, descricao=desc, natureza=natureza,
                    grupo_codigo=grupo_cod, grupo_nome=grupo_nome,
                    subgrupo_codigo=sub_cod, subgrupo_nome=sub_nome,
                    descricao_uso=uso or None, dedutivel_padrao=ded,
                    redutora=redutora,
                    tipos_permitidos=_tipos(tipos), ordem=ordem, ativo=True))
                criadas.append(cod)
            else:
                mudou = False
                for campo, valor in (("grupo_codigo", grupo_cod), ("grupo_nome", grupo_nome),
                                     ("subgrupo_codigo", sub_cod), ("subgrupo_nome", sub_nome),
                                     ("natureza", natureza), ("ordem", ordem),
                                     ("redutora", redutora)):
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

    aposentadas, pendentes = _aposentar(s)
    s.flush()
    registrar_evento(s, "categoria", 0, "PLANO_APLICADO",
                     {"criadas": len(criadas), "atualizadas": len(atualizadas),
                      "aposentadas": len(aposentadas),
                      "pendentes_de_migracao": len(pendentes)},
                     usuario.id if usuario else None)
    return {"criadas": criadas, "atualizadas": atualizadas,
            "aposentadas": aposentadas, "pendentes_de_migracao": pendentes,
            "total_no_plano": ordem}


def _aposentar(s: Session) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Desativa as contas que saíram do plano — só as sem lançamento nenhum.

    A que tem lançamento volta no segundo retorno, com a contagem e o motivo,
    e SEGUE ATIVA. É de propósito: desativar uma conta em uso esconderia
    lançamento do relatório sem ninguém pedir.
    """
    aposentadas: list[dict[str, Any]] = []
    pendentes: list[dict[str, Any]] = []
    for cod, (destino_cod, motivo) in APOSENTADORIAS.items():
        cat = s.scalars(select(Categoria).where(Categoria.codigo == cod)).first()
        if cat is None or cat.codigo != cod or not cat.ativo:
            continue
        destino = None
        if destino_cod:
            achada = s.scalars(
                select(Categoria).where(Categoria.codigo == destino_cod)).first()
            destino = achada if achada is not None and achada.codigo == destino_cod else None
        quantos = _lancamentos_na_conta(s, cat.id)
        if quantos == 0 and destino is not None:
            cat.ativo = False
            cat.substituida_por_id = destino.id
            aposentadas.append({"codigo": cod, "descricao": cat.descricao,
                                "destino": destino_cod, "motivo": motivo})
        else:
            pendentes.append({"codigo": cod, "descricao": cat.descricao,
                              "destino": destino_cod, "lancamentos": quantos,
                              "motivo": motivo})
    return aposentadas, pendentes


def conta_por_nome(s: Session, nome: str) -> Optional[Categoria]:
    """Acha a conta pelo nome escrito na planilha, aceitando os apelidos.

    Existe para os importadores não dependerem de a planilha usar exatamente a
    palavra que o plano usa hoje.
    """
    import re
    import unicodedata

    def chave(texto: Optional[str]) -> str:
        bruto = unicodedata.normalize("NFKD", (texto or "").strip().lower())
        sem = "".join(c for c in bruto if not unicodedata.combining(c))
        return re.sub(r"\s+", " ", sem)

    alvo = chave(nome)
    if not alvo:
        return None
    contas = s.scalars(select(Categoria)).all()
    por_descricao = {chave(c.descricao): c for c in contas}
    achada = por_descricao.get(alvo)
    if achada is not None:
        return achada
    codigo = {chave(k): v for k, v in APELIDOS.items()}.get(alvo)
    if not codigo:
        return None
    por_codigo = {c.codigo: c for c in contas}
    return por_codigo.get(codigo)


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
