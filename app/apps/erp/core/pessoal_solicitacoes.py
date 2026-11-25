# -*- coding: utf-8 -*-
"""
A FILA DE SOLICITAÇÕES DO DP — o segundo pipe do Pipefy, dentro do ERP.

O pipe "SOLICITAÇÕES DP" tem 80.992 cards desde 2021 e 28 tipos de pedido. O
ponto do ERP já absorveu os de PONTO (atestado, declaração, licença, férias,
ajuste de batida, compensação, folga do banco); o que sobra é o ADMINISTRATIVO,
e é o que vive aqui.

DECISÃO DO DONO, 05/11/2026: *"todos os pedidos vão para o DP, e quem tiver
perfil do DP que vai aprovar."* **Uma etapa só** — diferente do ponto, onde
algumas coisas passam pelo encarregado. Não há cadeia para configurar, e isso é
simplificação de verdade: no pipe a autorização era um checklist chamado "você
não possui autorização para esta solicitação".

ABRIR é de quem está na obra (`solicitar_dp`); ATENDER é do DP (`atender_dp`).
A divisão vem do próprio pipe, que perguntava *"requisição solicitada por um
terceiro?"* porque o encarregado abre pedido para a equipe dele.

O QUE CADA TIPO FAZ AO SER ATENDIDO, e esta é a diferença que importa: no Pipefy
atender era mover o card e alguém ia mexer no cadastro à mão (ou num cenário do
Make). Aqui o atendimento **executa**: readmissão readmite, desligamento
desliga, alteração de cargo altera. O pedido e o efeito deixam de poder divergir.
"""
from __future__ import annotations

import re

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.apps.erp.core import admissao_processo as svc_admissao
from app.apps.erp.core import contabil_processo as svc_contabil
from app.apps.erp.core import ferias as svc_ferias
from app.apps.erp.core import dp_analise as svc_analise
from app.apps.erp.core import desligamento_processo as svc_processo
from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Colaborador, Usuario

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OS TIPOS — os do pipe que sobraram para o administrativo
#
# `executa` diz o que o atendimento faz no cadastro. `campos` é o que o
# formulário pede, e vira o JSONB do pedido.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# OS TIPOS — e, desde 10/11/2026, com os RÓTULOS DO PIPE, letra por letra
#
# Ele, olhando a tela: *"Prestador que passa a ter carteira. Qual foi o termo
# que eu usei? Por que não está usando o termo que está no Pipe? Por que está
# querendo inventar a roda de novo? Uma coisa que já está certa, já está
# funcionando, já está consolidada. Para que inventar?"*
#
# ⚑ A FONTE É `DE_PARA_PIPEFY_DP.md`, na pasta do ERP: os dois pipes
# exportados pelo GraphQL, com rótulo, tipo, obrigatoriedade e opções. Campo
# novo aqui se confere LÁ primeiro. Se o Pipe não tem, o ERP não inventa.
#
# ⚑ E A SEGUNDA LEITURA, que é a que eu tinha quebrado: no Pipe, o que está no
# FORMULÁRIO INICIAL é da obra; o que está numa FASE é do DP. Matrícula,
# convenção e os dois auxílios estão em FASE. Ele: *"matrícula, não é o
# funcionário que decide. Como é que a pessoa vai readmitir uma pessoa, botar
# uma matrícula? Quem solicita define uma matrícula?"*
#
# Por isso cada tipo tem DUAS listas:
#   · `campos`        — o que a OBRA preenche no pedido;
#   · `campos_do_dp`  — o que o DP preenche ao atender, na tela dele.
# Campo de DP que aparecesse no formulário da obra é defeito, e há teste.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# ⚑ OS DADOS DA PESSOA QUE A ADMISSÃO EXIGE (19/11/2026)
#
# Ele, com a lista na mão: *"E-Social precisa de nome, nome da mãe, data de
# nascimento, CPF. SomaPay, a gente precisa agregar RG, data de emissão, o
# órgão emissor, o estado de emissão e a data de início e o telefone e o tipo
# de contrato. (…) Além disso, a gente precisa internamente do endereço da
# pessoa."*
#
# Os nomes são os das COLUNAS do cadastro: o pedido grava, o atendimento
# copia para a ficha sem tradução, e o "já está no cadastro" da readmissão
# lê a mesma coluna. Nome e CPF ficam fora porque já estavam no formulário.
# ---------------------------------------------------------------------------
CAMPOS_DA_PESSOA = ("nome_mae", "data_nascimento",
                    "rg", "rg_emissao", "rg_orgao", "rg_uf",
                    "cep", "logradouro", "numero_endereco", "complemento",
                    "bairro", "municipio", "uf")
# O complemento é o único opcional: muita casa não tem.
OBRIGATORIOS_DA_PESSOA = tuple(c for c in CAMPOS_DA_PESSOA if c != "complemento")
# De qual papel cada um sai. A leitura preenche; o que ela não achar vira
# pergunta, e a pessoa digita.
DA_IDENTIDADE = ("nome", "cpf", "nome_mae", "data_nascimento",
                 "rg", "rg_emissao", "rg_orgao", "rg_uf")
DO_COMPROVANTE = ("cep", "logradouro", "numero_endereco", "complemento",
                  "bairro", "municipio", "uf")


TIPOS: dict[str, dict[str, Any]] = {
    "DESLIGAMENTO": {
        # Pipe: "Desligamento de Colaborador".
        "nome": "Desligamento de Colaborador",
        "explicacao": "fecha o vínculo e tira o acesso ao ponto",
        "exige_pessoa": True, "executa": "desligar",
        # ⚑ SAÍRAM, e foi ele quem apontou: "último dia trabalhado" e "faltas
        # a descontar". *"Não é a pessoa que está pedindo que vai dizer. Ele
        # vai colocar só a data do aviso prévio. Faltas a descontar também
        # não. Quem vai olhar isso aqui é o DP."* No Pipe, de fato, nenhum dos
        # dois existe neste formulário.
        # ⚑ A CHAVE PIX SAIU em 10/11/2026, a pedido dele: *"desligamento de
        # colaborador, não precisa de chave Pix mais."* Ela existe no Pipe
        # (para as verbas rescisórias), mas o ERP já tem a chave no cadastro
        # da pessoa — pedir de novo no desligamento é pedir o que o sistema
        # sabe, e abrir uma porta para digitarem uma chave diferente da
        # cadastrada bem na hora de pagar a rescisão.
        # ⚑ UM TIPO SÓ, E OS CAMPOS SAEM DO VÍNCULO DA PESSOA (15/11/2026).
        #
        # Ele: *"vamos ter um tipo só de desligamento, aí ele vai exibir os
        # dados da pessoa (…) forma de desligamento só para CTPS, e para
        # prestador só a data de saída."*
        #
        # Antes eram DOIS tipos na caixinha ("Desligamento de Colaborador" e
        # "Desligamento de Prestador de Serviço"), e quem abria tinha de saber
        # de antemão qual era o vínculo da pessoa para escolher o tipo certo.
        # Escolher o errado preenchia campos que não se aplicam: aviso prévio
        # de quem não tem aviso, data de saída de quem a tem calculada.
        #
        # O sistema SABE o vínculo — está no cadastro. Perguntar o que já se
        # sabe, e deixar a resposta errada passar, é o defeito. `por_vinculo`
        # diz o que cada vínculo pede; a tela lê isto depois de escolhida a
        # pessoa, e o servidor confere pela MESMA lista.
        #
        # `campos` continua sendo a UNIÃO dos dois, porque é dela que saem os
        # rótulos e as caixinhas — e porque pedido já gravado tem de continuar
        # legível.
        "campos": ("forma_desligamento", "aviso_previo", "data_saida",
                   "continua_prestador", "observacao"),
        # ⚑ O PADRÃO, QUANDO O VÍNCULO NÃO SE SABE, É O DA CARTEIRA — o mais
        # exigente dos dois. Padrão vazio seria trava frouxa: um pedido que
        # chegasse sem pessoa ligada passaria sem forma de desligamento
        # nenhuma, e o DP receberia "desligue o fulano" e nada mais.
        "obrigatorios": ("forma_desligamento", "aviso_previo"),
        # ⚑ ENCERRAMENTO DE CONTRATO DE EXPERIÊNCIA NÃO TEM AVISO PRÉVIO, e
        # agora o campo SOME da tela, não só deixa de ser exigido (15/11/2026).
        # Ele: *"quando é encerramento de contrato de experiência, eu não
        # preciso dar aviso. Então essa data de aviso já se torna
        # desnecessária, concorda?"*
        #
        # Deixar o campo à mostra, mesmo opcional, convida a preencher — e
        # data de aviso prévio onde não há aviso prévio vira conta errada na
        # rescisão.
        "ocultos_se": {"forma_desligamento": {
            "FIM_EXPERIENCIA": ("aviso_previo",)}},
        # O "vai trabalhar o aviso?" só existe no pedido de demissão.
        "visiveis_se": {"cumpre_aviso": {"forma_desligamento": ("PEDIDO_COLABORADOR",)}},
        "obrigatorios_se": {"forma_desligamento": {
            "PEDIDO_COLABORADOR": ("cumpre_aviso",)}},
        "por_vinculo": {
            "CTPS": {
                # ⚑ "ELA CONTINUA TRABALHANDO?" — A PERGUNTA QUE FECHA O BURACO
                # (15/11/2026).
                #
                # Ele: *"como é que a gente faz com o colaborador que é
                # desligado da carteira e continua como prestador? Ou apenas
                # demite e readmite? (…) isso aí provoca de vez em quando um
                # bug na parte do ponto."*
                #
                # Decisão dele: continua desligando e readmitindo — dois
                # pedidos, como hoje. O que entra é a TRAVA: o desligamento
                # pergunta se a pessoa segue trabalhando, e quando a resposta é
                # sim o sistema passa a COBRAR a readmissão.
                #
                # O bug do ponto é consequência disso: desligar corta o acesso
                # ao aparelho da obra na hora. Se a readmissão é esquecida, a
                # pessoa aparece para trabalhar e não consegue bater. Ninguém
                # avisava; a obra descobria às 7 da manhã.
                #
                # Agora: dois passos a mais no checklist do DP, e a lista de
                # "readmissões pendentes" na tela de Solicitações
                # (`readmissoes_esquecidas`). O pedido não pode ser fechado
                # deixando isso em branco.
                # ⚑ 24/11/2026: "cumpre_aviso" passou a ser da OBRA, e só no
                # pedido de demissão (`visiveis_se` abaixo). Ele: *"o mais
                # comum é que o colaborador peça para sair de imediato (…)
                # vamos ajustar para poder a gente trabalhar de uma forma ou
                # de outra. Se ele está pedindo desligamento imediato ou se ele
                # vai trabalhar o aviso."* Quem sabe isso é a obra, que recebeu
                # o pedido; o DP corrige se precisar.
                "campos": ("forma_desligamento", "aviso_previo", "cumpre_aviso",
                           "continua_prestador", "observacao"),
                "obrigatorios": ("forma_desligamento", "aviso_previo",
                                 "continua_prestador"),
                # A data de saída de quem tem carteira é CONTA do DP (último
                # dia + aviso), não informação da obra.
                # ⚑ 20/11/2026: o que o AVISO PRÉVIO escreve. A redução da
                # jornada (CLT art. 488) e, no pedido de demissão, se a
                # pessoa cumpre o aviso — os dois vão no texto que ela assina.
                "campos_do_dp": ("ultimo_dia", "data_saida",
                                 "faltas_a_descontar", "reducao_jornada",),
            },
            "PRESTADOR": {
                # Prestador não tem aviso prévio — não há o que avisar. Tem a
                # data em que a prestação acaba.
                "campos": ("data_saida", "observacao"),
                "obrigatorios": ("data_saida",),
                # Prestador não tem aviso prévio nem faltas a descontar em
                # folha: o DP não tem nada a acrescentar aqui.
                "campos_do_dp": (),
                "checklist": ("Conferir o período prestado",),
            },
        },
        # ⚑ `data_saida` NÃO ENTRA AQUI, e isso é a trava funcionando: para o
        # prestador ela é campo DA OBRA (é o fim combinado da prestação), e o
        # mesmo campo nos dois lados é exatamente o que a separação proíbe.
        # Ela é campo do DP só no ramo CTPS, acima, onde é conta dele.
        "campos_do_dp": ("ultimo_dia", "faltas_a_descontar"),
        # ⚑ O CHECKLIST FICA SÓ COM O QUE O SISTEMA NÃO ACOMPANHA (23/11/2026).
        # Eram os ONZE itens do Pipefy. O aviso, a contabilidade, a
        # documentação, as assinaturas e os pagamentos (TRCT e GRRF) são
        # passos do processo (`desligamento_processo`), lidos da prova; o
        # ponto corta sozinho; impressão e malote viraram o portal. E o
        # "sistema de pagamento" não se desativa — ele, revisando: *"quando
        # você coloca rescisão [no SomaPay], automaticamente ele desativa."*
        # As faltas vão no e-mail: *"basta informar a quantidade de faltas."*
        "checklist": ("Conferir as faltas no espelho do ponto",),
    },
    # ⚑ O PIPE TEM DOIS DESLIGAMENTOS, e eles pedem coisas diferentes: o do
    # prestador não tem aviso prévio (não há), tem DATA DE SAÍDA. Eu tinha
    # juntado os dois num só e pedido os campos dos dois ao mesmo tempo.
    # ⚑ SAIU DA CAIXINHA EM 15/11/2026 (`oculto`), e a entrada FICA: há
    # pedidos gravados com este tipo, e tirar a linha daqui deixaria o
    # histórico deles sem rótulo e o atendimento sem checklist. Quem abre não
    # vê mais; quem já abriu continua enxergando o que pediu.
    "DESLIGAMENTO_PRESTADOR": {
        "nome": "Desligamento de Prestador de Serviço",
        "explicacao": "encerra a prestação de serviço",
        "oculto": True,
        "exige_pessoa": True, "executa": "desligar",
        "campos": ("data_saida", "observacao"),
        "obrigatorios": ("data_saida",),
        "campos_do_dp": (),
        "checklist": ("Conferir o período prestado",),
    },
    "CANCELAR_DESLIGAMENTO": {
        "nome": "Cancelar Solicitação de Desligamento",
        "explicacao": ("a pessoa vai continuar trabalhando: o DP desfaz a "
                       "saída e avisa a contabilidade"),
        # ⚑ 20/11/2026: ATENDER NÃO FAZIA NADA — a saída continuava marcada e
        # a pessoa era desligada no dia mesmo assim. Agora desfaz de verdade
        # (`cancelar_desligamento`).
        "exige_pessoa": True, "executa": "cancelar_desligamento",
        "campos": ("observacao",),
        "obrigatorios": (),
        "campos_do_dp": (),
        "checklist": ("Confirmar com a obra que a pessoa continua trabalhando",),
    },
    # ⚑ SAIU DA CAIXINHA EM 15/11/2026, e quem a substitui é o CADASTRO.
    #
    # Ele: *"na admissão, se o CPF já existir, o sistema detecta e oferece a
    # readmissão — aí readmissão deixa de ser um tipo."*
    #
    # O motivo é que ninguém sabe, ao abrir o pedido, se a pessoa já passou
    # por aqui — e era justamente quem NÃO sabia que escolhia "Cadastro" e
    # criava a segunda ficha da mesma pessoa. Agora o CPF responde: digitado
    # no cadastro, o sistema procura, e se achar alguém desligado o pedido
    # vira readmissão sozinho (`é_readmissao`, em `abrir`). A entrada FICA
    # aqui pelos pedidos já gravados.
    "READMISSAO": {
        "nome": "Readmissão de Colaborador",
        "explicacao": "traz a pessoa de volta sem refazer o cadastro",
        "oculto": True,
        "exige_pessoa": True, "executa": "readmitir",
        # ⚑ "Tipo de Readmissão" é o nome no Pipe — eu tinha escrito "tipo de
        # contrato", que não existe em lugar nenhum. E saíram daqui convenção,
        # salário e matrícula: ele, com todas as letras, *"convenção coletiva,
        # é DP quem decide"* e *"matrícula, não é o funcionário que decide"*.
        "campos": ("data_inicio", "tipo_readmissao", "cargo_pleiteado_id",
                   "empresa_id", "observacao"),
        "obrigatorios": ("data_inicio", "tipo_readmissao", "cargo_pleiteado_id",
                         "empresa_id"),
        "campos_do_dp": ("cargo_id", "convencao_id", "salario", "matricula"),
        # 23/11/2026: data, cargo, convenção, obra e matrícula o atendimento
        # grava; não há sistema de pagamento a reativar.
        "checklist": (),
    },
    "ADMISSAO_PRESTADOR": {
        # Pipe: "Admissão de Prestador de Serviço". Eu tinha chamado de
        # "Prestador que passa a ter carteira" — inventado.
        "nome": "Admissão de Prestador de Serviço",
        "explicacao": "o prestador passa a ter registro em carteira",
        "exige_pessoa": True, "executa": "trocar_vinculo",
        # ⚑ A BUSCA SÓ OFERECE PRESTADOR (17/11/2026). Ele: *"na admissão de
        # prestador, na busca aparecer somente os que são Prestador de
        # Serviço."*
        #
        # É o tipo de pedido que não tem como dar certo com a pessoa errada:
        # admitir em carteira quem JÁ está em carteira não é operação que
        # exista. Antes a caixinha oferecia o cadastro inteiro da obra, e o
        # erro só aparecia no atendimento do DP — depois de alguém anexar as
        # fotos da carteira.
        #
        # `so_vinculo` é genérico de propósito: tipo novo que valha só para um
        # vínculo declara o dele e a busca passa a filtrar sozinha.
        "so_vinculo": "PRESTADOR",
        # ⚑ SÓ SE PEDE O QUE O CADASTRO NÃO TEM. Ele, em 10/11/2026:
        # *"admissão de prestador de serviço, vamos requerer os dados. Se não
        # tivermos, né? Acho que é interessante uma consulta, ver se tem
        # CTPS, se tem PIS."*
        #
        # A pessoa JÁ EXISTE neste pedido (é um prestador virando CTPS), e o
        # cadastro dela pode já ter os dois números. `ja_no_cadastro` diz de
        # qual coluna cada campo vem; quando ela está preenchida, o campo sai
        # da exigência e aparece só para conferir.
        # ⚑ 19/11/2026: virar CTPS é ENTRAR no eSocial e no SomaPay — então
        # os mesmos dados da admissão (`OBRIGATORIOS_DA_PESSOA`). O prestador
        # já tem ficha: o que ela tiver não se pergunta; só o que faltar.
        "campos": ("num_ctps", "num_pis") + CAMPOS_DA_PESSOA + (
                   "salario_familia", "observacao"),
        # ⚑ O PIS NÃO É OBRIGATÓRIO (19/11/2026). Ele: *"pode fazer a leitura,
        # se encontrar, mas não tornar item obrigatório, porque o cadastro
        # social é feito com o CPF."* O campo fica; a leitura preenche quando
        # acha.
        "obrigatorios": ("num_ctps",) + OBRIGATORIOS_DA_PESSOA + (
                         "salario_familia",),
        "rotulos_proprios": {"data_nascimento": "Data de Nascimento"},
        "ja_no_cadastro": {"num_ctps": "ctps_numero", "num_pis": "pis",
                           **{c: c for c in CAMPOS_DA_PESSOA}},
        "campos_do_dp": ("cargo_id", "salario", "matricula", "convencao_id"),
        "documentos": ("CTPS-FOTO", "CTPS-INFORMACOES"),
        # Mesma regra do cadastro: o número sai da foto da carteira que este
        # pedido JÁ exige. Ninguém transcreve onze dígitos à mão.
        "vem_do_documento": {"num_ctps": "CTPS-INFORMACOES",
                             "num_pis": "CTPS-INFORMACOES"},
        # Os dados da pessoa NÃO têm papel exigido aqui: a carteira não traz o
        # RG nem o endereço. Faltando no cadastro, a obra digita.
        "checklist": ("Conferir a documentação de admissão",),
    },
    "ALTERACAO_CARGO": {
        "nome": "Solicitação de Alteração de Cargo",
        "explicacao": "com o cargo pleiteado e a justificativa",
        "exige_pessoa": True, "executa": "alteracao",
        # No Pipe quem pede informa o CARGO PLEITEADO e a justificativa; o
        # "Cargo Deferido" é a resposta, e salário é consequência dele.
        "campos": ("data_inicio", "cargo_pleiteado_id", "justificativa"),
        "obrigatorios": ("data_inicio", "cargo_pleiteado_id", "justificativa"),
        "campos_do_dp": ("cargo_deferido_id", "salario"),
        "checklist": ("Conferir o piso da convenção",),
    },
    "ATUALIZACAO_DADOS": {
        "nome": "Atualização de Dados Cadastrais",
        "explicacao": "telefone, chave Pix, endereço, nome",
        "exige_pessoa": True, "executa": "atualizar_cadastro",
        # No Pipe é UM campo de texto ("Descreva as Informações a serem
        # atualizadas"), e não onze caixinhas. Quem pede escreve o que mudou;
        # o DP altera o cadastro com o documento na mão.
        # ⚑ O ENDEREÇO FALTAVA, e a explicação do tipo já o prometia
        # ("telefone, chave Pix, endereço, nome") — a mesma classe de defeito
        # da porta do portal que anunciava férias. Ele viu: *"dados
        # cadastrais pode ser telefone, chave Pix, endereço, né? Acho que
        # faltou endereço aí."*
        # ⚑ A EXPLICAÇÃO DESTE TIPO diz "telefone, chave Pix, endereço,
        # nome" — então o nome também tem campo. Prometer na vitrine o que o
        # formulário não tem é o mesmo defeito do portal que anunciava férias
        # e não tinha; em miniatura, mas é.
        "campos": ("descricao", "telefone", "pix_tipo", "pix_chave",
                   "cep", "logradouro", "numero_endereco", "bairro",
                   "municipio", "uf", "nome_novo"),
        "obrigatorios": ("descricao",),
        "campos_do_dp": (),
        "checklist": ("Conferir o documento que comprova",),
    },
    "DEPENDENTE": {
        "nome": "Solicitação de Cadastro de Dependente",
        "explicacao": "para salário-família",
        "exige_pessoa": True, "executa": "dependente",
        # ⚑ SAIU "conta no imposto de renda". Ele: *"você acha que a pessoa lá
        # da obra vai entender isso aqui, vai saber responder isso aqui? Conta
        # no imposto de renda ou não conta? Não vai saber."* E, de fato, o
        # Pipe não pergunta isso. Parentesco no Pipe é só Filho ou Cônjuge.
        # ⚑ AQUI "nome" e "cpf" são DO DEPENDENTE, não do colaborador — e o
        # rótulo precisa dizer isso, senão a tela pede duas vezes o nome de
        # quem já foi escolhido na busca. `rotulos_proprios` manda sobre o
        # `ROTULOS` geral.
        "campos": ("nome", "parentesco", "data_nascimento", "dependentes",
                   "observacao"),
        "rotulos_proprios": {"nome": "Nome do Dependente",
                             "cpf": "CPF do Dependente"},
        "obrigatorios": ("nome", "parentesco", "data_nascimento"),
        "campos_do_dp": ("cpf", "dependente_ir", "salario_familia"),
        "documentos": ("DOC-DEPENDENTE",),
        # ⚑ VÁRIOS DE UMA VEZ. Ele: *"dependente tem que ser um a um, não
        # posso adicionar outro, não, para pedir mais de um."*
        "repete": True,
        "checklist": ("Conferir a certidão ou o RG do dependente",),
    },
    "AJUDA_DE_CUSTO": {
        "nome": "Solicitação de Ajuda de Custo",
        "explicacao": "auxílio alimentação e auxílio transporte",
        "exige_pessoa": True, "executa": "beneficios",
        # ⚑ OS RÓTULOS E AS OPÇÕES SÃO OS DO PIPE. Ele: *"pagar alimentação,
        # aí segunda, quinta, esse termo está errado. Como é que estava no
        # Pipe? Para usar igual."* São "Categoria Auxílio Alimentação" e
        # "Categoria Auxílio Transporte", e as opções estão em CATEGORIAS.
        # ⚑ O VALE-TRANSPORTE ENTROU AQUI em 15/11/2026, e deixou de ser tipo
        # próprio: ele já era uma das opções de Categoria Auxílio Transporte.
        # `descontar_vt` é a pergunta que faltava — o VT desconta em folha, e
        # quem concede precisa dizer se vai descontar ou se a empresa absorve.
        "campos": ("solicitacao",
                   "categoria_alimentacao", "aux_alimentacao",
                   "categoria_transporte", "aux_transporte",
                   "descontar_vt", "observacao"),
        "obrigatorios": ("solicitacao",),
        # "Não recebe" esconde o valor — a mesma regra da admissão (19/11/2026).
        "ocultos_se": {
            "categoria_alimentacao": {"NAO_RECEBE": ("aux_alimentacao",)},
            "categoria_transporte": {"NAO_RECEBE": ("aux_transporte",
                                                    "descontar_vt")}},
        # Só se pergunta o desconto quando há vale-transporte para descontar,
        # e o VALOR só depois de escolhida a categoria — antes disso, "valor
        # da alimentação" não quer dizer nada.
        "visiveis_se": {
            "aux_alimentacao": {"categoria_alimentacao": ("*",)},
            "aux_transporte": {"categoria_transporte": ("*",)},
            "descontar_vt": {
                "categoria_transporte": ("VALE_TRANSPORTE", "DIARIO_E_VT")}},
        # ⚑ O VALOR SAIU DOS CAMPOS DO DP em 15/11/2026, a pedido dele: *"não
        # foi colocado campo para colocar valor de transporte e
        # alimentação."* Quem combina o valor com a pessoa é a OBRA — e o
        # próprio checklist dizia "conferir o valor com a obra", o que é
        # confissão de que o número nasce lá.
        "campos_do_dp": (),
        "checklist": ("Conferir o valor que a obra informou",
                      "Cadastro no sistema de transporte (se houver VT)",
                      "Encaminhamento do cartão (se houver VT)"),
    },
    # ⚑ SAIU DA CAIXINHA EM 15/11/2026: virou OPÇÃO da ajuda de custo.
    #
    # Ele: *"vale transporte não precisa ser um tipo de solicitação à parte,
    # ele é uma opção dentro de ajuda de custo."*
    #
    # E é verdade na própria lista: "Vale Transporte" já era uma das opções de
    # Categoria Auxílio Transporte. Ter as duas coisas fazia o mesmo pedido
    # caber em dois lugares — e quem concedesse VT pela ajuda de custo e
    # alguém negasse pelo tipo próprio não teria como saber qual valia.
    "VALE_TRANSPORTE": {
        "nome": "Solicitação de Vale Transporte",
        "explicacao": "conceder ou cancelar o vale-transporte",
        "oculto": True,
        "exige_pessoa": True, "executa": "beneficios",
        "campos": ("conceder", "observacao"),
        "obrigatorios": ("conceder",),
        "campos_do_dp": ("categoria_transporte", "aux_transporte"),
        "checklist": ("Cadastro no Sistema de Transporte",
                      "Encaminhamento do Cartão"),
    },
    "DESCONTO_EM_FOLHA": {
        "nome": "Lançar Desconto em Folha",
        "explicacao": "valor total e número de parcelas",
        "exige_pessoa": True, "executa": None,
        "campos": ("valor_total", "parcelas", "valor_parcela", "data",
                   "observacao"),
        "obrigatorios": ("valor_total",),
        "campos_do_dp": ("primeira_competencia",),
        "checklist": ("Conferir valor e parcelas",),
    },
    # ⚑ NÃO EXISTE "PRÉ-CADASTRO", e isso foi ELE quem corrigiu em 09/11/2026:
    # *"você colocou lá pré-cadastro colaborador, não existe esse pré-cadastro,
    # existe o cadastro, ponto final. O cadastro já tem que vir com
    # documentação e tudo mais."*
    #
    # ⚑ E OS DOIS PIPES VIRAM UM. Ele, em 10/11/2026: *"a gente está meio que
    # unificando. Vai ser tudo DP, porque tudo é solicitação DP. O cadastro e
    # as outras solicitações."* Por isso o cadastro é um TIPO desta fila, e
    # não uma tela à parte.
    "CADASTRO": {
        # ⚑ "ADMISSÃO DE COLABORADOR" desde 19/11/2026. Ele: *"eu quero trocar
        # por admissão de colaborador. Porque dessa forma, eu tanto atendo quem
        # não tem cadastro conosco, quem já teve cadastro conosco."* O CÓDIGO
        # continua CADASTRO: há pedidos gravados com ele, e o que muda é a
        # palavra que a pessoa lê.
        "nome": "Admissão de Colaborador",
        "explicacao": ("quem nunca trabalhou conosco e quem já trabalhou: a "
                       "obra manda os documentos, a leitura preenche, o DP "
                       "confere"),
        "exige_pessoa": False, "executa": "criar_cadastro",
        # Os rótulos vêm do pipe CADASTRO COLABORADORES. "Tipo de Documentação
        # para Cadastro" é o nome certo do que eu chamava de "tipo de
        # contrato"; "Função" é o nome certo de cargo neste formulário.
        # ⚑ O CNPJ DA ADMISSÃO ENTROU EM 11/11/2026, quando ele reparou:
        # *"na readmissão ou no cadastro de colaborador pergunta qual
        # empresa?"* A readmissão perguntava; o CADASTRO NÃO — e é justamente
        # ele que cria a pessoa. Sem isso o DP recebia um colaborador novo sem
        # saber por qual das três empresas registrar, e ia perguntar por fora
        # do sistema. No Pipe o campo está no formulário de ABERTURA, ou seja,
        # é de quem pede — não é decisão do DP, como convenção e matrícula.
        #
        # UM campo só, e não dois: no Pipe há "Empresa para Registro" E "CNPJ
        # para Admissão", mas as três empresas SÃO os três CNPJs. Perguntar as
        # duas coisas é a duplicidade que ele já derrubou no diário ("ou é um
        # ou é o outro").
        # ⚑ A ORDEM É A DA CONVERSA, e isso foi pedido dele em 15/11/2026:
        # *"esse formulário aqui está muito desorganizado."* Agora é: quem é a
        # pessoa (nome, CPF, telefone, Pix) → que tipo de contratação →
        # quando e para fazer o quê → quanto → os benefícios.
        #
        # ⚑ TELEFONE E CHAVE PIX SÃO OBRIGATÓRIOS. Ele: *"telefone é
        # obrigatoríssimo (…) é vital"* e *"chave Pix obrigatória"*. O motivo
        # é o mesmo dos documentos: sem telefone ninguém avisa a pessoa de
        # nada, e sem chave Pix ela não recebe. Campo que o pagamento exige
        # não é campo opcional — é pendência que alguém vai correr atrás
        # depois, por WhatsApp.
        # ⚑ "MEIO DE TRANSPORTE PARA CHEGAR À OBRA?" SAIU EM 17/11/2026, e foi
        # ele quem mandou: *"eliminar campo: Meio de Transporte para chegar à
        # Obra?"*
        #
        # O campo vinha do Pipefy e perguntava de ônibus/moto/bicicleta. Quem
        # responde o que a folha precisa é a AJUDA COM TRANSPORTE (a categoria),
        # que diz se é diário, vale-transporte, veículo, moto… Perguntar as
        # duas coisas dava duas respostas sobre transporte, e só uma delas
        # chegava ao pagamento.
        #
        # ⚑ RAZÃO SOCIAL E CNPJ ENTRARAM, só para PJ (`visiveis_se` abaixo).
        # Ele: *"quando for pessoa PJ, abrir campos Razão social (PJ) e CNPJ
        # (PJ)."* Sem eles o DP recebia um PJ e ia pedir a nota fiscal sem
        # saber em nome de quem ela sai.
        "campos": ("nome", "cpf") + CAMPOS_DA_PESSOA + (
                   "telefone", "pix_tipo", "pix_chave",
                   "tipo_documentacao", "tipo_contrato",
                   "razao_social_pj", "cnpj_pj", "empresa_id",
                   "num_ctps", "num_pis",
                   "data_inicio", "cargo_pleiteado_id",
                   "pagamento", "valor_diaria", "valor_mes",
                   "categoria_alimentacao", "aux_alimentacao",
                   "categoria_transporte", "aux_transporte", "descontar_vt",
                   "observacao"),
        # ⚑ AS DUAS AJUDAS VIRARAM OBRIGATÓRIAS (17/11/2026). Ele:
        # *"renomear campos e torná-los obrigatórios."*
        #
        # Só dá para exigir porque a lista ganhou "Não recebe": antes, obrigar
        # forçaria a inventar uma categoria para quem não recebe nada. Com a
        # opção explícita, a resposta sempre existe — e o DP deixa de receber
        # cadastro em que "não sei" e "não recebe" são a mesma casa vazia.
        #
        # VALOR DA DIÁRIA saiu dos obrigatórios fixos: ele só aparece para CLT
        # e autônomo (RPA), e campo que não aparece não pode ser exigido — o
        # envio travaria sem a pessoa ter como resolver. Virou
        # `obrigatorios_se`, abaixo.
        # ⚑ 19/11/2026: os dados do eSocial e do SomaPay (`OBRIGATORIOS_DA_
        # PESSOA`) e o TIPO DE CONTRATO, que o SomaPay exige de todos.
        "obrigatorios": ("nome", "cpf") + OBRIGATORIOS_DA_PESSOA + (
                         "telefone", "pix_tipo", "pix_chave",
                         "tipo_documentacao", "tipo_contrato", "empresa_id",
                         "data_inicio", "cargo_pleiteado_id",
                         "categoria_alimentacao", "categoria_transporte"),
        "rotulos_proprios": {"data_nascimento": "Data de Nascimento"},
        # ⚑ NA READMISSÃO, o que o cadastro antigo já tem não se pede de novo
        # (19/11/2026). Ele: *"se for um colaborador desligado, nós já temos os
        # dados dele"* — e, sobre o que faltar: a obra digita na hora.
        # ⚠️ CADASTRO não exige pessoa, então este mapa NÃO entra na busca de
        # pessoa do portal (`COLUNAS_JA_NO_CADASTRO`): ninguém lê o RG ou a
        # mãe de outra pessoa procurando pelo nome.
        "ja_no_cadastro": {c: c for c in CAMPOS_DA_PESSOA},
        # ⚑ SE FOR CTPS, A CARTEIRA E O PIS SÃO OBRIGATÓRIOS TAMBÉM. Ele, em
        # 10/11/2026: *"se for CTPS, os dados da carteira são obrigatórios,
        # PIS também."*
        #
        # ⚑ MAS NINGUÉM DIGITA: eles saem da LEITURA das páginas da carteira,
        # que já são anexadas no mesmo pedido. Ele, logo depois: *"eu vi que
        # você está pedindo para que seja escrito PIS, CTPS, mas isso aí a
        # gente já vai ler os documentos. O ideal era (…) se não encontrasse
        # nos documentos, aí exigisse de outra forma."*
        #
        # Pedir para transcrever um número de onze dígitos de uma foto que
        # acabou de ser tirada é trabalho dobrado — e é exatamente onde se
        # troca um algarismo. `vem_do_documento` diz à tela que o campo nasce
        # preenchido pela leitura e só vira pergunta quando ela não achar.
        # ⚑ E O QUE CADA CONTRATAÇÃO PEDE A MAIS.
        #
        # CTPS: a carteira e o PIS (que saem da LEITURA, abaixo).
        # PRESTADOR: o tipo de contrato (PJ, RPA, aprendiz…) e se é diarista
        # ou mensalista — duas coisas que, em carteira, não se escolhem.
        # ⚑ E A DIÁRIA É EXIGIDA ONDE ELA APARECE (17/11/2026). Ele: *"Valor da
        # Diária → exibir campo para CLT e se for Prestador de Serviço
        # (Autônomo (RPA)); demais, não exibir."* Então ela é obrigatória
        # nesses três casos, e nos outros não existe.
        "obrigatorios_se": {"tipo_documentacao": {
            # ⚑ PIS saiu em 19/11/2026: o eSocial é feito com o CPF.
            "CTPS": ("num_ctps", "tipo_contrato"),
            "PRESTADOR": ("tipo_contrato", "pagamento")},
            "tipo_contrato": {
                "CLT_INDETERMINADO": ("valor_diaria",),
                "CLT_DETERMINADO": ("valor_diaria",),
                "AUTONOMO_RPA": ("valor_diaria",),
                "PJ": ("razao_social_pj", "cnpj_pj")},
            "pagamento": {"MENSALISTA": ("valor_mes",)}},
        # ⚑ O QUE NÃO SE APLICA NEM APARECE (15/11/2026). Ele: *"quando é
        # CTPS, o valor é o da convenção, então valor do mês e pagamento
        # diarista ou mensalista não se aplicam. Tipo de contrato também não:
        # CTPS é sempre CLT."*
        #
        # Campo que não se aplica e fica na tela não é inofensivo: alguém
        # preenche, o DP recebe um número que contraria a convenção, e aí há
        # duas respostas para "quanto esta pessoa ganha". Esconder é a única
        # forma de não ter a segunda.
        #
        # VALOR DO MÊS também só aparece para mensalista — diarista não tem
        # valor de mês, tem diária.
        # ⚑ "NÃO RECEBE" ESCONDE O VALOR (19/11/2026). Ele: *"quando coloca
        # ajuda com alimentação, ajuda com transporte, não recebe, oculta o
        # campo valor."* Valor de ajuda que não existe é campo que alguém
        # preenche por engano.
        "ocultos_se": {
            "categoria_alimentacao": {"NAO_RECEBE": ("aux_alimentacao",)},
            "categoria_transporte": {"NAO_RECEBE": ("aux_transporte",
                                                    "descontar_vt")},
            "tipo_documentacao": {
            "CTPS": ("pagamento", "valor_mes"),
            # ⚑ 19/11/2026, ele: *"quando a gente colocar o vínculo prestador
            # de serviço, oculta o número da CTPS e oculta o número do PIS,
            # porque essa informação não precisa."* A leitura ainda os grava,
            # se achar — oculto não é apagado.
            "PRESTADOR": ("num_ctps", "num_pis")},
            "pagamento": {"DIARISTA": ("valor_mes",)}},
        # ⚑ O TIPO DE CONTRATO NASCE FECHADO e só abre DEPOIS de escolhida a
        # documentação — e aí abre para as DUAS, com listas diferentes.
        #
        # Em 15/11/2026 ele mandou fechá-lo para CTPS: *"tem que ocultar o
        # campo. Abre o campo só se for prestador de serviço."* O motivo era
        # "CTPS é sempre CLT". Em 17/11/2026 ele corrigiu a premissa, não a
        # regra: *"Se Jovem Aprendiz é CTPS, precisamos poder selecionar o
        # tipo de contrato entre: CLT (tempo indeterminado), CLT (tempo
        # determinado), Jovem Aprendiz."* Jovem aprendiz TEM carteira — então
        # CTPS não é sempre CLT, e a escolha existe dos dois lados.
        #
        # O que não mudou é o essencial: o campo não aparece ENQUANTO ninguém
        # escolheu a documentação. Senão alguém responde "PJ" antes de dizer
        # que é CTPS, e a resposta errada fica no formulário. Por isso "*" (já
        # respondeu, qualquer coisa) e não as duas opções soltas.
        "visiveis_se": {
            "tipo_contrato": {"tipo_documentacao": ("*",)},
            # ⚑ A DIÁRIA SÓ ONDE ELA EXISTE (17/11/2026). Ele: *"Valor da
            # Diária → exibir campo para CLT e se for Prestador de Serviço
            # (Autônomo (RPA)); demais, não exibir."*
            #
            # Antes ela aparecia sempre, inclusive para PJ e pró-labore, que
            # não têm diária — e campo que não se aplica e fica na tela é
            # campo que alguém preenche. Jovem aprendiz fica DE FORA: ele
            # disse "CLT", e aprendiz é mensalista pela própria lei.
            # ⚑ EM CÓDIGO, e não em rótulo: é o código que a coluna guarda e
            # que `traduzir_do_pipe` deixa no pedido. Escrevi em rótulo na
            # primeira versão e o campo não apareceu em caso NENHUM — o mesmo
            # defeito de alfabeto que deixou "Tipo de contrato" sem opções.
            "valor_diaria": {"tipo_contrato": (
                "CLT_INDETERMINADO", "CLT_DETERMINADO", "AUTONOMO_RPA")},
            # ⚑ OS DADOS DA EMPRESA DO PJ nascem com o PJ, e só com ele.
            "razao_social_pj": {"tipo_contrato": ("PJ",)},
            "cnpj_pj": {"tipo_contrato": ("PJ",)},
            # O valor só é perguntado depois de escolhida a categoria: antes
            # disso, "valor da alimentação" não quer dizer nada.
            "aux_alimentacao": {"categoria_alimentacao": ("*",)},
            "aux_transporte": {"categoria_transporte": ("*",)},
            "descontar_vt": {
                "categoria_transporte": ("VALE_TRANSPORTE", "DIARIO_E_VT")}},
        # ⚑ A LISTA DO CAMPO MUDA CONFORME OUTRA RESPOSTA (17/11/2026).
        #
        # Tipo de contrato é um campo só, com duas listas: quem é CTPS escolhe
        # entre as duas CLTs e jovem aprendiz; quem é prestador escolhe entre
        # PJ, autônomo (RPA), autônomo mensalista, estágio e pró-labore.
        #
        # Dois CAMPOS seria o caminho errado: a coluna que o banco grava é uma
        # só (`tipo_contrato`), e dois campos para uma coluna é o jeito de
        # gravar os dois e não saber qual vale. A fonte das duas listas é o
        # cadastro de pessoas — cópia aqui diverge no primeiro mês.
        "opcoes_se": {"tipo_contrato": {"tipo_documentacao": {
            "CTPS": tuple(svc_pessoas.TIPO_CONTRATO_ROTULOS[c]
                          for c in svc_pessoas.TIPOS_CONTRATO_CTPS),
            "PRESTADOR": tuple(svc_pessoas.TIPO_CONTRATO_ROTULOS[c]
                               for c in svc_pessoas.TIPOS_CONTRATO_PRESTADOR)}}},
        "vem_do_documento": {
            "num_ctps": "CTPS-INFORMACOES",
            "num_pis": "CTPS-INFORMACOES",
            **{c: "DOC-IDENTIDADE" for c in DA_IDENTIDADE},
            **{c: "COMPROVANTE-ENDERECO-PESSOA" for c in DO_COMPROVANTE},
        },
        # ⚑ Matrícula e convenção são do DP — estão em FASE no Pipe, não no
        # formulário. Os VALORES dos auxílios também; a CATEGORIA deles subiu
        # para o formulário em 15/11/2026, porque é a obra que sabe se a
        # pessoa come na obra todo dia ou volta para casa na sexta. Ele:
        # *"ajuda de custo e vale transporte têm que estar no cadastro
        # inicial, não como pedido separado depois."*
        # ⚑ O VALOR DOS AUXÍLIOS SAIU DAQUI em 15/11/2026, a pedido dele:
        # *"não foi colocado campo para colocar valor de transporte e
        # alimentação."*
        #
        # Eu havia deixado o VALOR com o DP e trazido só a CATEGORIA para o
        # formulário. A divisão estava errada: quem combina o valor com a
        # pessoa é a OBRA — e o próprio checklist do DP dizia "conferir o
        # valor com a obra", o que é confissão de que o número nasce lá.
        # Perguntar ao DP um número que ele vai ter de ligar para a obra para
        # saber é pôr uma ligação telefônica dentro do sistema.
        #
        # O DP continua podendo corrigir na ficha da pessoa.
        "campos_do_dp": ("matricula", "convencao_id", "cargo_id", "salario"),
        # ⚑ OS PAPÉIS SÃO OBRIGATÓRIOS, E O PEDIDO NÃO SAI SEM ELES.
        #
        # Eu tinha proposto deixar entrar incompleto, e ele derrubou — com o
        # argumento que eu não tinha: *"se você permitir que entre funcionário
        # sem os documentos, esse documento para ele voltar e ser concluído é
        # um Deus nos acuda. (…) Não se consegue pagar o funcionário no
        # SomaPay se não tiver CPF, não se consegue fazer o cadastro se não
        # tiver comprovante de endereço e CPF."*
        #
        # Isso muda a conta inteira: deixar passar NÃO evita o mal que eu
        # queria evitar — a pessoa continua sem receber —, e ainda cria um
        # cadastro pela metade que ninguém fecha. Pendência de documento não é
        # dívida que se paga depois; é dívida que ninguém paga.
        "documentos": ("DOC-IDENTIDADE", "CPF-DOC",
                       "COMPROVANTE-ENDERECO-PESSOA"),
        "documentos_se": {"tipo_documentacao": {
            "CTPS": ("CTPS-FOTO", "CTPS-INFORMACOES")}},
        "checklist": ("Conferir se a documentação veio completa",
                      "Conferir os dados contra os documentos",
                      "Conferir o que falta para admitir"),
        # ⚑ QUANDO O CPF JÁ EXISTE, O PEDIDO É READMISSÃO — e o trabalho do
        # DP é outro: não há cadastro para criar, há vínculo para reabrir.
        # Este é o checklist que o tipo READMISSAO tinha, e que vem para cá
        # junto com ele (15/11/2026).
        # ⚑ ENXUGADO EM 19/11/2026, item por item, por ele: a saída anterior
        # vai sozinha para o histórico; a admissão, a obra e o cargo vêm do
        # pedido; a matrícula é o CPF (ou a que já existia); prestador não tem
        # convenção; o ponto reativa sozinho; o sistema de pagamento não se
        # mexe. O que sobra é conferir o que veio.
        "checklist_readmissao": (
            "Conferir os dados e os documentos",),
    },
    # ⚑ FÉRIAS ENTRARAM COMO PEDIDO EM 20/11/2026. Ele: *"Se as férias não
    # estão em solicitações, tem que entrar também, porque o administrativo de
    # obra pode pedir as férias. E o DP tem que fazer a mesma coisa, solicitar
    # a contabilidade, aguardar retorno e etc. Para então finalizar."* Só para
    # quem tem carteira — prestador não tem férias. O pedido passa pela
    # contabilidade (modelo FERIAS) e fecha quando ela devolve o aviso
    # (`contabil_processo`).
    "FERIAS": {
        "nome": "Solicitação de Férias",
        "explicacao": "o período de férias; o DP pede o aviso à contabilidade",
        # ⚑ 20/11/2026 (tarde): atender PROGRAMA as férias (`core/ferias.py`)
        # — elas aparecem na tela Férias como "aguardando a contabilidade" e
        # viram "confirmadas" quando ela devolve.
        "exige_pessoa": True, "executa": "programar_ferias", "so_vinculo": "CTPS",
        "campos": ("ferias_inicio", "ferias_dias", "abono_pecuniario",
                   "adiantamento_13", "observacao"),
        "obrigatorios": ("ferias_inicio", "ferias_dias"),
        "campos_do_dp": (),
        "checklist": ("Conferir o período aquisitivo e o saldo de dias",),
    },
    # ⚑ ALTERAR E CANCELAR FÉRIAS (20/11/2026). Ele: *"tem que pensar na
    # possibilidade de alteração e cancelamento. E aí tem que estar DP no
    # meio, contabilidade no meio, o agente de IA no meio."* Dois pedidos
    # próprios, com o mesmo caminho das férias: a obra (ou o DP) pede, o DP
    # atende, a contabilidade devolve — e só então a data muda.
    "ALTERACAO_FERIAS": {
        "nome": "Alteração de Férias",
        "explicacao": "mudar a data ou os dias de férias já marcadas",
        "exige_pessoa": True, "executa": "alterar_ferias", "so_vinculo": "CTPS",
        "campos": ("ferias_inicio", "ferias_dias", "abono_pecuniario",
                   "adiantamento_13", "observacao"),
        "obrigatorios": ("ferias_inicio", "ferias_dias", "observacao"),
        "campos_do_dp": (),
        "checklist": ("Conferir o período aquisitivo e o saldo de dias",),
    },
    "CANCELAMENTO_FERIAS": {
        "nome": "Cancelamento de Férias",
        "explicacao": "desmarcar férias já marcadas",
        "exige_pessoa": True, "executa": "cancelar_ferias", "so_vinculo": "CTPS",
        "campos": ("observacao",),
        "obrigatorios": ("observacao",),
        "campos_do_dp": (),
        "checklist": (),
    },
    # ⚑ O AFASTAMENTO PASSA PELO DP E PELA CONTABILIDADE (20/11/2026). Ele:
    # *"se o que lançar no ponto demandar ações do DP junto à contabilidade
    # (…) automaticamente gera uma solicitação do DP para ser dado o devido
    # tratamento junto à contabilidade (…) acidente de trabalho também (…)
    # Mesma coisa para maternidade."* Nasce sozinho do ponto
    # (`ponte_do_ponto.py`) ou é aberto pela obra/DP. A natureza (INSS,
    # acidente, maternidade) quem confirma é o DP ao atender — o ponto não a
    # registra. O controle do benefício e das férias é da contabilidade
    # (Fortes), decisão dele.
    "AFASTAMENTO": {
        "nome": "Comunicação de Afastamento",
        "explicacao": ("atestado acima de 15 dias, INSS, acidente de trabalho, "
                       "maternidade — o DP comunica à contabilidade"),
        "exige_pessoa": True, "executa": "registrar_afastamento",
        "so_vinculo": "CTPS",
        "campos": ("afastamento_natureza", "afast_inicio", "afast_previsao",
                   "observacao"),
        "obrigatorios": ("afast_inicio",),
        "campos_do_dp": (),
        "checklist": ("Conferir o atestado, o laudo ou a CAT",),
    },
    "RETORNO_AFASTAMENTO": {
        "nome": "Retorno de Afastamento",
        "explicacao": "a pessoa voltou — o DP registra e comunica à contabilidade",
        "exige_pessoa": True, "executa": "registrar_retorno",
        "so_vinculo": "CTPS",
        "campos": ("retorno_data", "observacao"),
        "obrigatorios": ("retorno_data",),
        "campos_do_dp": (),
        "checklist": ("Conferir o exame de retorno (ASO)",),
    },
    # ⚑ A TRANSFERÊNCIA DE OBRA (20/11/2026). Ele: *"A transferência será
    # informada também à contabilidade, pois impacta diretamente na geração
    # das guias de FGTS e tributos."* A obra do pedido (`obra_id`) é a obra
    # de DESTINO. Nasce sozinha quando o ponto mostra a pessoa uma semana
    # noutra obra (`obra_atual.abrir_transferencias`), ou a obra/DP abre.
    "TRANSFERENCIA_OBRA": {
        # ⚑ SAIU DA CAIXINHA NO MESMO DIA (20/11/2026): a obra muda sozinha,
        # pelo mês trabalhado (`obra_do_mes`). Fica aqui só para os pedidos
        # já gravados continuarem legíveis.
        "oculto": True,
        "nome": "Transferência de Obra",
        "explicacao": "a pessoa passa a trabalhar em outra obra (de carteira, "
                      "o DP comunica à contabilidade)",
        "exige_pessoa": True, "executa": "transferir_obra",
        "campos": ("obra_id", "transferencia_data", "observacao"),
        "obrigatorios": ("obra_id", "transferencia_data"),
        "campos_do_dp": (),
        "checklist": (),
    },
    "DIVERSOS": {
        "nome": "Solicitações Diversas",
        "explicacao": "o que não cabe nos tipos acima",
        "exige_pessoa": False, "executa": None,
        "campos": ("descricao",),
        "obrigatorios": ("descricao",),
        "campos_do_dp": (),
        "checklist": (),
    },
}

# ---------------------------------------------------------------------------
# AS CAIXINHAS, com as opções do Pipe — palavra por palavra
# ---------------------------------------------------------------------------
CATEGORIAS: dict[str, tuple[str, ...]] = {
    "forma_desligamento": (
        "À Pedido do Colaborador", "Encerramento de Contrato de Experiência",
        "Demissão com Aviso Trabalhado", "Demissão com Aviso Indenizado"),
    # ⚑ UMA FONTE SÓ PARA O VÍNCULO (17/11/2026), e é isto que ele cobrou:
    #
    #   *"Quando eu falo em equalizar falo em relação aos dois campos abaixo:
    #    Vínculo — Carteira Assinada (CTPS) / Prestador de Serviço;
    #    Tipo de Documentação para Cadastro — Prestador de Serviço / CTPS."*
    #
    # São o MESMO dado com dois nomes e duas listas: o "Tipo de Documentação"
    # do pedido VIRA o `vinculo` da pessoa quando o DP atende. Enquanto as duas
    # listas eram escritas à mão, uma dizia "Carteira Assinada (CTPS)" e a
    # outra "CTPS", e a ordem era invertida.
    #
    # ⚑ A CORREÇÃO NÃO É ACERTAR AS DUAS: é ter UMA. Agora as opções saem do
    # cadastro de pessoas (`VINCULOS` + `VINCULO_ROTULOS`), que é onde o valor
    # fica gravado — então nunca mais divergem, nem na palavra nem na ordem.
    # É o mesmo caminho que o `tipo_contrato` já seguia, pelo mesmo motivo:
    # cópia de lista diverge no primeiro mês.
    #
    # "Tipo de Readmissão" é a MESMA pergunta ("volta como o quê?") e por isso
    # bebe da mesma fonte.
    "tipo_readmissao": tuple(svc_pessoas.VINCULO_ROTULOS[v]
                             for v in svc_pessoas.VINCULOS),
    "tipo_documentacao": tuple(svc_pessoas.VINCULO_ROTULOS[v]
                               for v in svc_pessoas.VINCULOS),
    # ⚑ AS DUAS LISTAS JUNTAS, e quem escolhe de verdade é `opcoes_se`: a
    # caixinha só abre depois do tipo de documentação, e aí mostra a lista
    # daquele lado (CTPS ou prestador). Esta aqui é o que sobra quando a tela
    # pergunta o campo fora da admissão — e cobre os dois, para nunca mostrar
    # uma lista menor do que a resposta certa.
    #
    # A fonte é o cadastro de pessoas. Duas listas para a mesma escolha
    # divergem no primeiro mês — foi o que aconteceu com as categorias de
    # auxílio, em 15/11/2026.
    "tipo_contrato": tuple(
        svc_pessoas.TIPO_CONTRATO_ROTULOS[c]
        for c in (svc_pessoas.TIPOS_CONTRATO_CTPS
                  + svc_pessoas.TIPOS_CONTRATO_PRESTADOR)),
    "parentesco": ("Filho", "Cônjuge"),
    "pagamento": ("Diarista", "Mensalista"),
    "meio_transporte": ("Nenhum", "Transporte Coletivo", "Moto", "Bicicleta"),
    # ⚑ AS DUAS CATEGORIAS DE AUXÍLIO VÊM DO CADASTRO, não de uma cópia aqui
    # (15/11/2026). Achado ao escrever a explicação de cada opção: havia DUAS
    # listas para o mesmo campo — esta, com os rótulos do Pipe ("Segunda à
    # Quinta", "Vale Transporte"), e a do cadastro de pessoas, com os códigos
    # que a coluna guarda ("SEG_QUI", "VALE_TRANSPORTE").
    #
    # A tela já usava a do cadastro; o pedido gravava a desta lista. Ou seja: o
    # pedido de ajuda de custo guardava um texto que o cadastro não reconhece,
    # e o atendimento não tinha como aplicá-lo. Duas palavras para o mesmo
    # dado, e a diferença só aparecia na hora de valer.
    #
    # A fonte é o cadastro, porque é lá que o valor fica gravado e é de lá que
    # a folha lê. "CARTAO" fica fora: ele existe para explicar a ficha de quem
    # já tem, e saiu de uso em cadastro novo (decisão dele em 05/11/2026).
    "categoria_alimentacao": tuple(svc_pessoas.CATEGORIAS_ALIMENTACAO),
    # ⚑ SEM "Cartão": ele existe na FASE do cadastro (o DP pode marcar), mas
    # não no formulário de quem pede — e a lista do pedido é a do formulário.
    # Oferecer no pedido uma opção que o Pipe só dá ao DP é recriar aqui uma
    # escolha que lá já foi tirada de quem abre.
    "categoria_transporte": tuple(
        c for c in svc_pessoas.CATEGORIAS_TRANSPORTE
        if c not in svc_pessoas.CATEGORIAS_TRANSPORTE_FORA_DE_USO),
    "descontar_vt": ("Sim, descontar em folha", "Não, a empresa absorve"),
    "pix_tipo": ("E-mail", "Telefone", "CPF", "Aleatória"),
    "solicitacao": ("Solicitar ou Atualizar", "Cancelar"),
    "conceder": ("Sim", "Não"),
    # A pergunta é feita de forma que o "não" seja a resposta neutra: quem só
    # desliga marca "não continua" e segue; quem sabe que a pessoa fica tem de
    # dizer, e aí o sistema cobra a readmissão.
    "continua_prestador": ("Não, está saindo da empresa",
                           "Sim, continua como prestador de serviço"),
    "salario_familia": ("Sim", "Não"),
    "abono_pecuniario": ("Sim", "Não"),
    "adiantamento_13": ("Sim", "Não"),
    "reducao_jornada": ("2 horas a menos por dia", "7 dias corridos de folga no fim",
                        "A pessoa escolhe na hora"),
    "afastamento_natureza": ("Auxílio-doença do INSS (atestado acima de 15 dias)",
                             "Acidente de trabalho ou doença ocupacional",
                             "Licença-maternidade",
                             "Atestado médico (até 15 dias)"),
    # 24/11/2026: a obra responde, e a resposta mais comum vem primeiro.
    "cumpre_aviso": ("Não — sai de imediato (pede dispensa do aviso)",
                     "Sim — vai trabalhar os 30 dias do aviso"),
}

# ---------------------------------------------------------------------------
# O QUE CADA OPÇÃO SIGNIFICA — a conta, escrita embaixo do campo
#
# ⚑ PEDIDO DELE EM 15/11/2026: *"explica cada modo de cálculo. Diário é dias
# úteis, mês é o valor cheio, mensal é 30 ou 31 vezes o valor, segunda e sexta
# é para quem fica alojado."*
#
# O problema que isto resolve é concreto: "Mensal" e "Mês" estão lado a lado na
# mesma caixinha e dão valores DIFERENTES — um multiplica pelos dias do mês, o
# outro é fixo. Duas palavras parecidas para duas contas diferentes, sem nada
# escrito, é erro garantido; e o erro só aparece na folha.
#
# A explicação vai para a TELA (dica embaixo do campo), não para um manual.
# ---------------------------------------------------------------------------
EXPLICACAO_DA_OPCAO: dict[str, dict[str, str]] = {
    # ⚑ A CHAVE É O CÓDIGO, e não o rótulo: é o código que a caixinha manda e
    # que a coluna guarda. Explicação escrita com o rótulo não casaria com
    # nada, e a dica apareceria vazia — foi exatamente o que aconteceu na
    # primeira tentativa, e só o navegador mostrou.
    "categoria_alimentacao": {
        "SEG_QUI": "paga quatro dias por semana — quem volta para casa na sexta",
        "SEG_SEX": "paga os cinco dias úteis da semana",
        "MENSAL": "o valor vezes os dias do mês (30 ou 31), trabalhados ou não",
        "MES": "o valor cheio do mês, sem contar dias",
        # ⚑ "NÃO RECEBE" (17/11/2026, pedido dele). A explicação importa porque
        # essa é a resposta que ANTES era o campo em branco — e em branco
        # queria dizer duas coisas ao mesmo tempo.
        "NAO_RECEBE": "não há ajuda com alimentação para esta pessoa",
    },
    "categoria_transporte": {
        "DIARIO": "o valor vezes os dias úteis em que a pessoa trabalhou no mês",
        "MENSAL": "o valor vezes os dias do mês (30 ou 31), trabalhados ou não",
        "MES": "o valor cheio do mês, sem contar dias",
        "SEG_E_SEX": "só a ida e a volta da semana — quem fica alojado na obra",
        "VALE_TRANSPORTE": "cartão de transporte coletivo, com desconto em folha",
        "DIARIO_E_VT": "os dois: o valor por dia útil mais o cartão",
        "VEICULO": "a pessoa usa carro próprio",
        "MOTOCICLETA": "a pessoa usa moto própria",
        "BICICLETA": "a pessoa usa bicicleta própria",
        "CARTAO": "saiu de uso — fica só para explicar ficha antiga",
        "NAO_RECEBE": "não há ajuda com transporte para esta pessoa",
    },
    "descontar_vt": {
        "Sim, descontar em folha": "o desconto legal do vale-transporte sai do salário",
        "Não, a empresa absorve": "a empresa paga o transporte sem descontar nada",
    },
    "continua_prestador": {
        "Não, está saindo da empresa":
            "o acesso ao ponto cai na data de saída e não volta",
        "Sim, continua como prestador de serviço":
            "a pessoa continua ativa e batendo ponto; depois do último dia de "
            "carteira o DP confirma, e o cadastro passa a prestador",
    },
    # ⚑ AS CHAVES SEGUEM O RÓTULO, que agora vem do cadastro de pessoas: se
    # alguém trocar a palavra lá e esquecer aqui, a dica some da tela. Tem
    # teste cobrando as duas pontas (`test_dp_pedidos_condicionais`).
    "tipo_documentacao": {
        svc_pessoas.VINCULO_ROTULOS["CTPS"]:
            "registro em carteira; o salário vem da convenção coletiva",
        svc_pessoas.VINCULO_ROTULOS["PRESTADOR"]:
            "sem carteira; o valor é o combinado com a obra",
    },
}


# ---------------------------------------------------------------------------
# O RÓTULO DO PIPE ↔ O CÓDIGO QUE O ERP GRAVA
#
# ⚑ O BANCO NÃO MUDA. "À Pedido do Colaborador" é o que a pessoa LÊ; o que
# fica gravado continua sendo `PEDIDO_COLABORADOR`, que é o que a regra de
# desligamento e os relatórios já entendem. Trocar o conteúdo da coluna para
# casar com o rótulo seria reescrever o passado de quem já foi desligado.
#
# A tradução é feita na BORDA (ao abrir e ao atender o pedido), num lugar só.
# ---------------------------------------------------------------------------
DO_PIPE_PARA_O_CODIGO: dict[str, dict[str, str]] = {
    "forma_desligamento": {
        "À Pedido do Colaborador": "PEDIDO_COLABORADOR",
        "Encerramento de Contrato de Experiência": "FIM_EXPERIENCIA",
        "Demissão com Aviso Trabalhado": "AVISO_TRABALHADO",
        "Demissão com Aviso Indenizado": "AVISO_INDENIZADO",
    },
    # Da mesma fonte das opções, logo acima: o rótulo que a pessoa escolhe
    # volta a ser o código que a coluna `vinculo` guarda.
    "tipo_readmissao": {svc_pessoas.VINCULO_ROTULOS[v]: v
                        for v in svc_pessoas.VINCULOS},
    "tipo_documentacao": {svc_pessoas.VINCULO_ROTULOS[v]: v
                          for v in svc_pessoas.VINCULOS},
    # Os códigos são os que `core/cadastros/pessoas.py` já grava na coluna
    # `tipo_contrato` — a tradução existe para a tela mostrar português e o
    # banco continuar com o que os relatórios entendem.
    "tipo_contrato": {svc_pessoas.TIPO_CONTRATO_ROTULOS[c]: c
                      for c in (svc_pessoas.TIPOS_CONTRATO_CTPS
                                + svc_pessoas.TIPOS_CONTRATO_PRESTADOR)},
    "descontar_vt": {"Sim, descontar em folha": "1", "Não, a empresa absorve": "0"},
    "continua_prestador": {"Não, está saindo da empresa": "0",
                           "Sim, continua como prestador de serviço": "1"},
    "afastamento_natureza": {
        "Auxílio-doença do INSS (atestado acima de 15 dias)": "INSS",
        "Acidente de trabalho ou doença ocupacional": "ACIDENTE",
        "Licença-maternidade": "MATERNIDADE",
        "Atestado médico (até 15 dias)": "ATESTADO"},
    "reducao_jornada": {"2 horas a menos por dia": "DUAS_HORAS",
                        "7 dias corridos de folga no fim": "SETE_DIAS",
                        "A pessoa escolhe na hora": "ESCOLHE"},
    "cumpre_aviso": {"Sim — vai trabalhar os 30 dias do aviso": "1",
                     "Não — sai de imediato (pede dispensa do aviso)": "0",
                     # os rótulos de antes de 24/11/2026, para pedido já gravado
                     "Sim, trabalha os 30 dias": "1",
                     "Não, pede dispensa do aviso": "0"},
    "parentesco": {"Filho": "FILHO", "Cônjuge": "CONJUGE"},
    "pagamento": {"Diarista": "DIARISTA", "Mensalista": "MENSALISTA"},
    "salario_familia": {"Sim": "1", "Não": "0"},
    "abono_pecuniario": {"Sim": "1", "Não": "0"},
    "adiantamento_13": {"Sim": "1", "Não": "0"},
    "conceder": {"Sim": "1", "Não": "0"},
    "pix_tipo": {"E-mail": "EMAIL", "Telefone": "TELEFONE", "CPF": "CPF",
                 "Aleatória": "ALEATORIA"},
}


# ---------------------------------------------------------------------------
# O QUE ESTE PEDIDO EXIGE — campos e papéis, inclusive os que dependem de
# outra resposta
# ---------------------------------------------------------------------------
ROTULOS_DE_DOCUMENTO = {
    "DOC-IDENTIDADE": "documento de identificação (RG, CNH ou CTPS)",
    "CPF-DOC": "CPF",
    "COMPROVANTE-ENDERECO-PESSOA": "comprovante de endereço",
    "CTPS-FOTO": "CTPS (página com a foto)",
    "CTPS-INFORMACOES": "CTPS (página com as informações)",
    "DOC-DEPENDENTE": "documento do dependente (certidão ou RG)",
    "ASO": "ASO (exame médico)",
}


# ---------------------------------------------------------------------------
# O VÍNCULO DA PESSOA DECIDE OS CAMPOS — um tipo, duas caras
#
# ⚑ ENTROU EM 15/11/2026, com o desligamento único. Ele: *"vamos ter um tipo
# só de desligamento (…) forma de desligamento só para CTPS, e para prestador
# só a data de saída."*
#
# A regra mora AQUI, num lugar só, e é lida pelos três que precisam dela: a
# tela (para mostrar), o `abrir` (para recusar o que falta) e o checklist (para
# o DP ter os passos do caso certo). Tela que esconde campo sem o servidor
# saber é meia trava — some da vista e continua exigido.
# ---------------------------------------------------------------------------
def _ramo(tipo: str, vinculo: Optional[str]) -> dict[str, Any]:
    """O pedaço do tipo que vale para este vínculo (vazio quando não há)."""
    regra = TIPOS.get(tipo) or {}
    chave = (vinculo or "").strip().upper()
    return (regra.get("por_vinculo") or {}).get(chave) or {}


def _nao_cabe_agora(regra: dict[str, Any], campo: str,
                    pedido: dict[str, Any]) -> bool:
    """Este campo ainda não faz sentido, pelo que já foi respondido?

    `visiveis_se` é o contrário de `ocultos_se`: o campo NÃO aparece até que
    outro campo tenha um dos valores listados. É o caso do desconto do
    vale-transporte, que só existe se houver vale-transporte.

    O padrão aqui é ESCONDER: campo que depende de uma escolha ainda não feita
    não tem o que perguntar. Mostrar por padrão convidaria a responder antes de
    a pergunta existir.
    """
    regras = (regra.get("visiveis_se") or {}).get(campo)
    if not regras:
        return False
    for gatilho, aceitos in regras.items():
        valor = str(pedido.get(gatilho) or "")
        # ⚑ OS DOIS ALFABETOS (17/11/2026). O certo é escrever a regra em
        # CÓDIGO, e é assim que ela está. Mas aceitar o rótulo também custa uma
        # linha e evita a classe de defeito que já apareceu duas vezes num dia:
        # regra escrita no alfabeto errado não dá erro — o campo só não
        # aparece, e quem descobre é quem abre a tela.
        rotulos = DO_PIPE_PARA_O_CODIGO.get(gatilho, {})
        aceitos = tuple(aceitos) + tuple(
            rotulos.get(a, a) for a in tuple(aceitos))
        # ⚑ "*" É O CURINGA: o campo nasce quando o gatilho tem QUALQUER
        # resposta. É o caso do valor do auxílio — ele faz sentido assim que a
        # categoria é escolhida, qualquer que seja ela, e listar as nove
        # opções uma por uma só criaria o jeito de esquecer a décima.
        if "*" in tuple(aceitos) and valor:
            return False
        if valor in tuple(aceitos):
            return False
    return True


def campos_do_tipo(tipo: str, vinculo: Optional[str] = None) -> tuple[str, ...]:
    """Os campos que o formulário pede. Sem vínculo conhecido, a união deles.

    A união é o certo quando não se sabe o vínculo: esconder campo por falta de
    informação seria recusar o pedido por um dado que o sistema não tem.
    """
    regra = TIPOS[tipo]
    return tuple(_ramo(tipo, vinculo).get("campos") or regra["campos"])


def obrigatorios_do_tipo(tipo: str,
                         vinculo: Optional[str] = None) -> tuple[str, ...]:
    """O que é obrigatório neste tipo, para este vínculo."""
    regra = TIPOS[tipo]
    ramo = _ramo(tipo, vinculo)
    if "obrigatorios" in ramo:
        return tuple(ramo["obrigatorios"])
    return tuple(regra.get("obrigatorios", ()))


def campos_do_dp_do_tipo(tipo: str,
                         vinculo: Optional[str] = None) -> tuple[str, ...]:
    """O que o DP preenche ao atender, para este vínculo.

    No desligamento a diferença é real: a data de saída de quem tem carteira é
    CONTA do DP (último dia + aviso prévio), e a do prestador é informação da
    obra (o fim combinado da prestação). Mesmo nome, donos diferentes.
    """
    regra = TIPOS[tipo]
    ramo = _ramo(tipo, vinculo)
    if "campos_do_dp" in ramo:
        return tuple(ramo["campos_do_dp"])
    return tuple(regra.get("campos_do_dp", ()))


# ⚑ OS PASSOS QUE A RESPOSTA ACRESCENTA ao checklist.
#
# ⚠️ ATÉ 19/11/2026 havia aqui "Abrir a readmissão como prestador": quem
# continuava era desligado e readmitido. Desde então a pessoa NÃO é desligada —
# o DP confirma a continuidade depois do último dia e o cadastro passa a
# prestador (`desligamento_processo.confirmar_continuidade`). A confirmação é
# um PASSO do processo, que trava o encerramento; repeti-la no checklist seria
# a mesma tarefa em dois lugares, um que trava e outro que não.
PASSOS_A_MAIS: dict[str, dict[str, dict[str, tuple[str, ...]]]] = {
    "DESLIGAMENTO": {"continua_prestador": {"1": (
        "Conferir, no dia seguinte ao último dia, que a pessoa continua "
        "batendo o ponto como prestador",)}},
}


# ===========================================================================
# ⚑ OS CHECKLISTS PASSARAM A SER CONFIGURÁVEIS (18/11/2026)
#
# Ele: *"vi que você importou checklist do Pipefy, só que eles estão todos
# desatualizados. Precisa de uma tela pra configurar eles de forma fácil."*
#
# Antes, mudar um passo era mexer no código e publicar — o DP dependia de uma
# publicação para corrigir a própria rotina de trabalho.
#
# ⚑ O PADRÃO DO CÓDIGO CONTINUA SENDO A BASE, e é o que torna isto seguro: a
# tabela `dp_checklists` guarda EXCEÇÃO, não a lista inteira. Tipo que ninguém
# configurou usa a lista daqui, que é a do pipe. Voltar ao padrão é apagar as
# linhas de lá, não redigitar onze itens.
# ===========================================================================
def checklist_configurado(s: Session, tipo: str,
                          vinculo: Optional[str] = None) -> tuple[str, ...]:
    """O que foi configurado na tela para este tipo, ou vazio.

    Procura primeiro o do VÍNCULO (mais específico) e, não achando, o que vale
    para todos. Mesma ordem de especificidade do código.
    """
    if not _tem_tabela(s):
        return ()
    v = (vinculo or "").strip().upper() or None
    for alvo in ([v] if v else []) + [None]:
        linhas = s.execute(text("""
            SELECT descricao FROM dp_checklists
             WHERE tipo = :t AND vinculo IS NOT DISTINCT FROM :v
             ORDER BY ordem"""), {"t": tipo, "v": alvo}).scalars().all()
        if linhas:
            return tuple(linhas)
    return ()


_TEM_TABELA: Optional[bool] = None


def _tem_tabela(s: Session) -> bool:
    """⚑ A RESPOSTA FICA GUARDADA. Esta função é chamada na abertura de TODA
    solicitação e na leitura de cada uma; sem memória seria uma consulta ao
    catálogo do banco toda vez, para perguntar algo que só muda quando alguém
    aperta o botão da migração.

    O "não" não fica guardado: assim o recurso liga sozinho logo depois do
    botão, sem reiniciar o serviço.
    """
    global _TEM_TABELA
    if _TEM_TABELA:
        return True
    achou = s.execute(text(
        "SELECT 1 FROM information_schema.tables "
        " WHERE table_schema = 'public' AND table_name = 'dp_checklists'"
    )).first() is not None
    if achou:
        _TEM_TABELA = True
    return achou


def checklist_do_tipo(tipo: str, vinculo: Optional[str] = None, *,
                      readmissao: bool = False,
                      pedido: Optional[dict[str, Any]] = None,
                      s: Optional[Session] = None) -> tuple[str, ...]:
    """Os passos que o DP faz — do vínculo certo, e não de um caso médio.

    Com a sessão, o que estiver configurado na tela manda sobre o padrão.
    Sem ela, vale o padrão — é o caso de quem só quer saber o que o código traz
    (a tela de configuração, para mostrar o original ao lado do atual).
    """
    regra = TIPOS[tipo]
    configurado = (checklist_configurado(s, tipo, vinculo) if s is not None
                   else ())
    if configurado:
        base = configurado
    elif readmissao and regra.get("checklist_readmissao"):
        base = tuple(regra["checklist_readmissao"])
    else:
        ramo = _ramo(tipo, vinculo)
        base = tuple(ramo["checklist"] if "checklist" in ramo
                     else regra.get("checklist", ()))
    for campo, mapa in (PASSOS_A_MAIS.get(tipo) or {}).items():
        valor = str((pedido or {}).get(campo) or "")
        base += tuple(mapa.get(valor, ()))
    return base


def checklists_para_a_tela(s: Session) -> list[dict[str, Any]]:
    """Todos os tipos, com o checklist de hoje e o original do pipe ao lado.

    ⚑ O ORIGINAL VAI JUNTO de propósito: sem ele, quem mexeu não tem como
    saber o que mudou nem como voltar. "Voltar ao padrão" só é uma promessa
    confiável se o padrão estiver à vista.
    """
    saida: list[dict[str, Any]] = []
    for tipo, regra in TIPOS.items():
        if regra.get("oculto"):
            # Tipo fora de uso não entra na tela de configurar: configurar o
            # que ninguém pode mais abrir é trabalho jogado fora.
            continue
        ramos: list[tuple[Optional[str], str]] = [(None, "todos os vínculos")]
        for v in (regra.get("por_vinculo") or {}):
            ramos.append((v, "carteira assinada" if v == "CTPS" else "prestador"))
        for vinculo, rotulo_ramo in ramos:
            padrao = checklist_do_tipo(tipo, vinculo)
            atual = checklist_configurado(s, tipo, vinculo)
            saida.append({
                "tipo": tipo,
                "tipo_rotulo": regra.get("nome") or tipo,
                "vinculo": vinculo, "vinculo_rotulo": rotulo_ramo,
                "itens": list(atual or padrao),
                "padrao": list(padrao),
                "configurado": bool(atual),
                "mudou": bool(atual) and list(atual) != list(padrao),
            })
    return saida


def gravar_checklist(s: Session, tipo: str, itens: list[str], *,
                     vinculo: Optional[str] = None,
                     usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Grava os passos deste tipo. Lista vazia = volta ao padrão do código.

    ⚠️ NÃO MEXE NO QUE JÁ ESTÁ ABERTO: os itens são copiados para a
    solicitação na hora em que ela é aberta. Trocar os passos de um pedido em
    andamento faria o DP perder o que já marcou e o histórico mentir sobre o
    que foi conferido.
    """
    if tipo not in TIPOS:
        raise ErroValidacao(f"Tipo de solicitação desconhecido: {tipo}.")
    v = (vinculo or "").strip().upper() or None
    if v and v not in ("CTPS", "PRESTADOR"):
        raise ErroValidacao("O vínculo tem de ser CTPS ou PRESTADOR.")

    limpos: list[str] = []
    for bruto in itens or []:
        texto_ = " ".join(str(bruto or "").split())[:300]
        if not texto_:
            continue
        if texto_ in limpos:
            # Item repetido vira duas caixinhas iguais, e aí ninguém sabe qual
            # já marcou. Recusar seria atrito; tirar a repetição, não.
            continue
        limpos.append(texto_)
    if len(limpos) > 40:
        raise ErroValidacao(
            "São mais de 40 passos. Um checklist que não cabe numa tela "
            "deixa de ser conferido — quebre em etapas ou tire o que virou "
            "rotina.")

    s.execute(text("DELETE FROM dp_checklists WHERE tipo = :t "
                   "  AND vinculo IS NOT DISTINCT FROM :v"),
              {"t": tipo, "v": v})
    for i, item in enumerate(limpos):
        s.execute(text("""
            INSERT INTO dp_checklists (tipo, vinculo, ordem, descricao, criado_por)
            VALUES (:t, :v, :o, :d, :u)"""),
            {"t": tipo, "v": v, "o": i, "d": item,
             "u": usuario.id if usuario else None})
    registrar_evento(s, "dp_checklist", 0,
                     "CHECKLIST_VOLTOU_AO_PADRAO" if not limpos
                     else "CHECKLIST_ALTERADO",
                     {"tipo": tipo, "vinculo": v, "itens": limpos},
                     usuario.id if usuario else None)
    s.flush()
    logger.info("ERP/DP: checklist de %s (%s) gravado com %s item(ns) por %s",
                tipo, v or "todos", len(limpos), usuario.nome if usuario else "?")
    return {"tipo": tipo, "vinculo": v, "itens": limpos,
            "voltou_ao_padrao": not limpos,
            "padrao": list(checklist_do_tipo(tipo, v))}


def vinculo_da_pessoa(s: Session, colaborador_id: Optional[int]) -> Optional[str]:
    """CTPS ou PRESTADOR, direto do cadastro — sem carregar o ORM inteiro."""
    if not colaborador_id:
        return None
    v = s.execute(text("SELECT vinculo FROM colaboradores WHERE id = :i"),
                  {"i": colaborador_id}).scalar()
    return (str(v).strip().upper() or None) if v else None


def _condicionais(regra: dict[str, Any], chave: str,
                  pedido: dict[str, Any]) -> tuple[str, ...]:
    """O que `obrigatorios_se` / `documentos_se` acrescentam, conforme a
    resposta já dada. Ex.: escolher CTPS passa a exigir a carteira e o PIS."""
    extra: list[str] = []
    for campo, mapa in (regra.get(chave) or {}).items():
        valor = pedido.get(campo)
        if isinstance(valor, str):
            extra += list(mapa.get(valor, ()))
    return tuple(extra)


# As colunas de `colaboradores` que qualquer tipo consulta. A busca de pessoa
# do portal traz TODAS elas de uma vez, para a tela já nascer preenchida — sem
# uma segunda ida ao servidor depois de escolher a pessoa.
#
# ⚑ Nome de coluna entra em SQL por interpolação (não dá para parametrizar
# nome de coluna), então o formato é conferido aqui: só letra minúscula,
# dígito e sublinhado. Tipo novo que escreva outra coisa quebra na importação
# do módulo, e não em produção.
COLUNAS_JA_NO_CADASTRO: tuple[str, ...] = tuple(sorted({
    coluna
    for regra in TIPOS.values() if regra.get("exige_pessoa")
    for coluna in (regra.get("ja_no_cadastro") or {}).values()
}))
# ⚑ DADO PESSOAL VIAJA COMO "TEM", NUNCA COMO VALOR (19/11/2026). A busca de
# pessoa do portal traz o que o cadastro já sabe, para não se perguntar de
# novo — mas o RG, a mãe e o endereço de alguém não podem descer para o
# celular de quem só procurou um nome. A tela recebe a marca e esconde o
# campo dizendo "já está no cadastro"; o valor fica no servidor, que o junta
# ao pedido (`o_que_o_cadastro_ja_tem`).
SO_A_MARCA = frozenset(CAMPOS_DA_PESSOA)
MARCA_TEM = "__tem__"
for _c in COLUNAS_JA_NO_CADASTRO:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", _c):
        raise ValueError(
            f"ja_no_cadastro: {_c!r} não é nome de coluna aceitável.")


def o_que_o_cadastro_ja_tem(s: Session, tipo: str,
                            colaborador_id: Optional[int]) -> dict[str, Any]:
    """O que esta pessoa já tem no cadastro, dos campos que o pedido pediria.

    ⚑ PEDIR O QUE O SISTEMA JÁ SABE É O DEFEITO MAIS CARO de um formulário:
    cansa quem preenche, e abre a porta para alguém digitar um número
    diferente do cadastrado. Ele, em 10/11/2026: *"admissão de prestador de
    serviço, vamos requerer os dados. Se não tivermos, né? Acho que é
    interessante uma consulta, ver se tem CTPS, se tem PIS."*

    Devolve `{campo: valor}` só do que ESTÁ preenchido. A tela mostra o valor
    para conferência e o campo sai da exigência.
    """
    de_onde = (TIPOS.get(tipo) or {}).get("ja_no_cadastro") or {}
    if not de_onde or not colaborador_id:
        return {}
    colunas = ", ".join(sorted(set(de_onde.values())))
    linha = s.execute(
        text(f"SELECT {colunas} FROM colaboradores WHERE id = :i"),
        {"i": colaborador_id}).mappings().first()
    if linha is None:
        return {}
    achados = {}
    for campo, coluna in de_onde.items():
        valor = linha.get(coluna)
        if valor is not None and str(valor).strip():
            achados[campo] = str(valor).strip()
    return achados


def exigencias(tipo: str, pedido: Optional[dict[str, Any]] = None,
               ja_tem: Optional[dict[str, Any]] = None,
               vinculo: Optional[str] = None) -> dict[str, Any]:
    """O que falta para este pedido poder sair — campos E papéis.

    A tela chama isto a cada mudança, para a lista ir se marcando sozinha
    enquanto a pessoa preenche. Assim o bloqueio não é uma parede no fim: é
    uma lista que a pessoa vê desde o começo.
    """
    from app.apps.erp.core import pessoal_experiencia as svc_exp

    regra = TIPOS[tipo]
    p = traduzir_do_pipe(pedido or {})
    campos = obrigatorios_do_tipo(tipo, vinculo) + _condicionais(
        regra, "obrigatorios_se", p)
    # ⚑ CAMPO ESCONDIDO NÃO É CAMPO EXIGIDO, e esta linha é a que impede o
    # defeito mais chato de formulário condicional: a tela esconde "valor do
    # mês" porque é CTPS, a pessoa confirma, e o servidor recusa pedindo um
    # campo que ela não tem como ver. Pedido impossível de corrigir.
    escondidos = set(_condicionais(regra, "ocultos_se", p))
    escondidos |= {c for c in campos if _nao_cabe_agora(regra, c, p)}
    if escondidos:
        campos = tuple(c for c in campos if c not in escondidos)
    # O mesmo para o vínculo: campo que não é deste vínculo não se exige.
    do_vinculo = set(campos_do_tipo(tipo, vinculo))
    campos = tuple(c for c in campos if c in do_vinculo or c == "obra_id")
    # ⚑ ENCERRAMENTO DE CONTRATO DE EXPERIÊNCIA NÃO TEM AVISO PRÉVIO, e por
    # isso a data deixa de ser exigida (15/11/2026). Ele: *"quando é
    # encerramento de contrato de experiência, eu não preciso dar aviso.
    # Então essa data de aviso já se torna desnecessária, concorda?"*
    #
    # É direito, não preferência: o contrato acaba porque o prazo acabou.
    # Aviso prévio existe para quem ROMPE antes do fim. Exigir a data aqui
    # obrigava a pessoa a inventar uma — e data inventada vira conta errada
    # na rescisão.
    #
    # A trava mora AQUI, e não só na tela: tela esconde campo, servidor é que
    # recusa pedido. As duas precisam concordar, e concordam porque leem a
    # mesma função.
    if "aviso_previo" in campos and not svc_exp.exige_aviso_previo(
            p.get("forma_desligamento")):
        campos = tuple(c for c in campos if c != "aviso_previo")
    # O que o cadastro já tem sai da exigência: não se pergunta de novo.
    if ja_tem:
        campos = tuple(c for c in campos if not str(ja_tem.get(c) or "").strip())
    papeis = tuple(regra.get("documentos", ())) + _condicionais(
        regra, "documentos_se", p)
    return {
        "campos": list(dict.fromkeys(campos)),
        "documentos": [{"codigo": c, "rotulo": ROTULOS_DE_DOCUMENTO.get(c, c)}
                       for c in dict.fromkeys(papeis)],
    }


def traduzir_do_pipe(dados: dict[str, Any]) -> dict[str, Any]:
    """Troca o rótulo que a pessoa escolheu pelo código que o ERP grava.

    Aceita os DOIS: quem mandar o código direto (um teste, uma importação)
    passa reto. É isso que permite trocar o rótulo na tela sem mexer em nada
    mais embaixo.
    """
    saida = dict(dados)
    for campo, mapa in DO_PIPE_PARA_O_CODIGO.items():
        v = saida.get(campo)
        if isinstance(v, str) and v in mapa:
            saida[campo] = mapa[v]
    return saida


# O rótulo de cada campo, como está no Pipe. A tela lê daqui — não escreve
# nome de campo à mão.
ROTULOS: dict[str, str] = {
    "obra_id": "Código da Obra",
    "ferias_inicio": "Início das férias",
    "ferias_dias": "Quantos dias",
    "abono_pecuniario": "Vender 1/3 (abono pecuniário)?",
    "adiantamento_13": "Adiantar a 1ª parcela do 13º?",
    "nome_mae": "Nome da Mãe",
    "rg": "Nº do RG",
    "rg_emissao": "Data de Emissão do RG",
    "rg_orgao": "Órgão Emissor do RG",
    "rg_uf": "Estado de Emissão do RG",
    "complemento": "Complemento",
    "forma_desligamento": "Forma de Desligamento",
    # 24/11/2026: no pedido de demissão é a data em que a pessoa pediu.
    "aviso_previo": "Data do aviso prévio (no pedido de demissão, a data do pedido)",
    "cumpre_aviso": "O colaborador vai trabalhar o aviso?",
    "data_saida": "Data de Saída",
    "data_inicio": "Data de Início",
    "tipo_readmissao": "Tipo de Readmissão",
    # ⚑ "VÍNCULO" desde 17/11/2026 — ele: *"renomear Tipo de Documentação para
    # Cadastro = Vínculo."* O CÓDIGO da coluna não muda (`tipo_documentacao`),
    # e os valores gravados continuam CTPS/PRESTADOR: o que muda é a palavra
    # que a pessoa lê, e ela passa a ser a MESMA que a ficha da pessoa usa.
    "tipo_documentacao": "Vínculo",
    "cargo_pleiteado_id": "Cargo Pleiteado",
    "cargo_deferido_id": "Cargo Deferido",
    # ⚑ "EMPRESA PARA REGISTRO", e não "CNPJ para Admissão" (15/11/2026).
    #
    # Ele, pela terceira vez no assunto: *"em relação à empresa, CNPJ da
    # empresa, você continua errando (…) tem que pegar do cadastro das
    # empresas do ERP. Lá já estão cadastradas 4 empresas."*
    #
    # Tinha razão, e a raiz era o próprio campo: ele pedia um CNPJ, então a
    # lista só podia oferecer empresa QUE TEM CNPJ — e a quarta, que está
    # cadastrada sem CNPJ, desaparecia. O campo certo é a EMPRESA; o CNPJ é
    # um dado dela, que o cadastro dela guarda.
    #
    # Agora o pedido guarda o NÚMERO da empresa, igual à ficha da pessoa faz
    # (`empresa_id`), e as quatro aparecem. Ele mesmo já havia dito em
    # 11/11/2026: *"empresa para registro e CNPJ para admissão é campo
    # duplicado. É a mesma informação."*
    "empresa_id": "Empresa para registro",
    "cnpj_admissao": "CNPJ para Admissão",
    "justificativa": "Justificativa de Alteração do Cargo",
    "num_ctps": "Nº CTPS",
    "num_pis": "Nº PIS",
    "salario_familia": "Solicitar Salário Família?",
    "nome": "Nome do Colaborador",
    "cpf": "CPF do Colaborador",
    "parentesco": "Parentesco",
    "data_nascimento": "Data de Nascimento do Dependente",
    "telefone": "Telefone",
    "pix_tipo": "Chave Pix",
    "pix_chave": "Chave Pix (o dado)",
    "pagamento": "Pagamento",
    "valor_diaria": "Valor da Diária",
    "valor_mes": "Valor do Mês",
    "meio_transporte": "Meio de Transporte para chegar à Obra?",
    "categoria_alimentacao": "Ajuda com Alimentação",
    "categoria_transporte": "Ajuda com Transporte",
    "aux_alimentacao": "Valor (Auxílio Alimentação)",
    "aux_transporte": "Valor (Auxílio Transporte)",
    "solicitacao": "Solicitação",
    "conceder": "Conceder Vale Transporte?",
    "descontar_vt": "Descontar o vale-transporte em folha?",
    "continua_prestador": "A pessoa continua trabalhando depois de sair da carteira?",
    "tipo_contrato": "Tipo de Contrato",
    # ⚑ OS DADOS DA EMPRESA DO PJ (17/11/2026). Os mesmos nomes que a ficha da
    # pessoa já usa (`razao_social_pj`, `cnpj_pj`), para o pedido cair direto
    # no cadastro sem tradução no meio.
    "razao_social_pj": "Razão social (PJ)",
    "cnpj_pj": "CNPJ (PJ)",
    "valor_total": "Valor Total do Desconto",
    "parcelas": "Quantidade de Parcelas",
    "valor_parcela": "Valor da Parcela",
    "data": "Data",
    # O Pipe escreve "Endereço (Logradouro)", "Município" e "UF (Endereço)".
    # Aqui vale a palavra que o resto do ERP já usa para o mesmo campo (Rua,
    # Cidade, Estado): duas palavras para o mesmo campo em duas telas é o tipo
    # de diferença que faz alguém achar que são coisas diferentes.
    "cep": "CEP",
    "logradouro": "Rua",
    "numero_endereco": "Número",
    "bairro": "Bairro",
    "municipio": "Cidade",
    "uf": "Estado",
    # Casamento, divórcio, correção de grafia na certidão. Fica por último no
    # formulário porque é o menos frequente dos quatro.
    "nome_novo": "Nome completo (só se mudou)",
    "descricao": "Descreva sua Solicitação:",
    "observacao": "Observações:",
}

# Os campos que SÓ o DP preenche não estão no Pipe com rótulo próprio (eles
# vivem nas fases, onde o nome da fase já diz o contexto). Estes são os nomes
# que a tela do DP mostra.
ROTULOS_DO_DP: dict[str, str] = {
    "ultimo_dia": "Último dia trabalhado",
    "faltas_a_descontar": "Faltas a descontar",
    "reducao_jornada": "Redução da jornada no aviso trabalhado (CLT art. 488)",
    "afastamento_natureza": "Natureza do afastamento",
    "afast_inicio": "Início do afastamento",
    "afast_previsao": "Previsão de retorno",
    "retorno_data": "Data em que voltou ao trabalho",
    "transferencia_data": "A partir de quando (na obra nova)",
    "cumpre_aviso": "No pedido de demissão, a pessoa cumpre o aviso?",
    "matricula": "Matrícula",
    "convencao_id": "Convenção",
    "cargo_id": "Cargo",
    "salario": "Salário",
    "admissao": "Data de admissão efetiva",
    "vigencia": "Vigência da alteração",
    "motivo": "Motivo da alteração",
    "dependente_ir": "Conta como dependente no IR",
    "primeira_competencia": "Primeira competência do desconto",
    "cpf": "CPF do dependente",
}

# OS DOIS TIPOS QUE SAÍRAM, em 05/11/2026, e por quê — para ninguém recriá-los
# achando que foi esquecimento. O dono: *"esse negócio de pedido manual, segunda
# via do cartão, documentação diversa, aqui acho que não tem necessidade."*
#
#   · **2ª via de cartão** — era recado manual com aviso de custo. Sai.
#   · **Documentação (admissão ou rescisão)** — a cobrança de documento passou a
#     ter dono de verdade: a ficha da pessoa diz quais papéis faltam, e o pedido
#     à contabilidade cobra o retorno dela. Pedido solto não era rastreio, era
#     lembrete.

SITUACOES = ("ABERTA", "EM_ANALISE", "EM_ANDAMENTO", "ATENDIDA", "RECUSADA",
             "CANCELADA")
# ⚑ "EM ANDAMENTO" (19/11/2026) é do desligamento de carteira: o DP já
# registrou a saída, e o processo segue até a rescisão e a multa pagas — ver
# `core/desligamento_processo.py`. O pedido continua ABERTO na fila (é
# trabalho de alguém), mas já NÃO se atende, recusa nem cancela: a saída foi
# gravada, e desfazê-la é outro pedido ("Cancelar desligamento").
ABERTAS = ("ABERTA", "EM_ANALISE", "EM_ANDAMENTO")
A_ATENDER = ("ABERTA", "EM_ANALISE")


def _opcoes(s: Optional[Session], campo: str) -> list[str]:
    """As opções de um campo de caixinha fechada."""
    return list(CATEGORIAS.get(campo, ()))


def empresas_para_escolher(s: Optional[Session] = None) -> list[dict[str, Any]]:
    """AS EMPRESAS DO GRUPO, direto do cadastro de Empresas do ERP.

    ⚑ ELE COBROU ISTO TRÊS VEZES, e nas duas primeiras eu consertei o lugar
    errado. 15/11/2026: *"em relação à empresa, CNPJ da empresa, você continua
    errando. Você está pegando do cadastro do colaborador, eu acho. Tem que
    pegar do cadastro das empresas do ERP. Lá já estão cadastradas 4 empresas.
    E continua aparecendo errado, ou seja, não está sendo usado o cadastro
    correto."*

    **A lista VINHA do cadastro de Empresas — o erro era o CAMPO.** Ele pedia
    um CNPJ ("CNPJ para Admissão"), então a lista só podia oferecer empresa que
    TEM CNPJ preenchido; a quarta, cadastrada sem CNPJ, desaparecia. Eu tinha
    "consertado" escrevendo embaixo do campo quem havia ficado de fora — o que
    explica o sintoma e mantém o defeito.

    O campo certo é a EMPRESA. O CNPJ é um dado dela, que o cadastro dela
    guarda, e o sistema busca quando precisa. Assim as quatro aparecem, e
    cadastrar o CNPJ que falta volta a ser o que é: arrumar o cadastro da
    empresa, não condição para abrir um pedido.

    ⚑ E NÃO HÁ MAIS LISTA DE SOCORRO. Antes, se a consulta não achasse nada, a
    caixinha mostrava TRÊS empresas escritas dentro do código. Três — o mesmo
    número que ele reclamou de ver. Lista inventada que parece certa é pior que
    lista vazia: ninguém desconfia dela. Agora, sem cadastro, a lista vem
    vazia e a tela diz o que fazer.
    """
    if s is None:
        return []
    linhas = s.execute(text("""
        SELECT id, razao_social,
               COALESCE(NULLIF(btrim(nome_fantasia), ''), razao_social) AS nome,
               COALESCE(btrim(cnpj), '') AS cnpj
          FROM empresas
         WHERE ativo IS NOT FALSE
         ORDER BY razao_social""")).mappings().all()
    return [{"id": l["id"], "nome": l["nome"],
             "razao_social": l["razao_social"], "cnpj": l["cnpj"],
             # O rótulo que a caixinha mostra. Sem CNPJ, DIZ que está sem — a
             # pessoa escolhe a empresa certa e sabe que há cadastro a arrumar.
             "rotulo": (f"{l['nome']} — {l['cnpj']}" if l["cnpj"]
                        else f"{l['nome']} — sem CNPJ no cadastro")}
            for l in linhas]


def empresa_do_pedido(s: Session, empresa_id: Any) -> dict[str, Any]:
    """A empresa escolhida, ou erro dizendo o que fazer."""
    linha = s.execute(text("""
        SELECT id, razao_social, COALESCE(btrim(cnpj), '') AS cnpj, ativo
          FROM empresas WHERE id = :i"""),
        {"i": int(empresa_id)}).mappings().first()
    if linha is None:
        raise ErroValidacao("Empresa não encontrada no cadastro.")
    if linha["ativo"] is False:
        raise ErroValidacao(
            f"{linha['razao_social']} está marcada como inativa no cadastro de "
            f"Empresas — não dá para registrar ninguém por ela.")
    return dict(linha)


def readmissoes_esquecidas(s: Session) -> list[dict[str, Any]]:
    """Quem foi desligado dizendo que CONTINUA, e não foi readmitido.

    ⚑ É A TRAVA DO BUG DO PONTO (15/11/2026). Ele: *"o colaborador que é
    desligado da carteira e continua como prestador (…) isso aí provoca de vez
    em quando um bug na parte do ponto."*

    O mecanismo do bug: desligar corta o acesso ao aparelho da obra na data de
    saída. Quando a readmissão como prestador é esquecida, a pessoa aparece
    para trabalhar no dia seguinte e não consegue bater — e quem descobre é a
    obra, às sete da manhã, sem saber o motivo.

    A conta tem três condições, e todas têm de valer: o pedido disse que
    continua, a pessoa está desligada, e não há vínculo aberto. Feita a
    readmissão, a pendência sai sozinha — ninguém precisa dar baixa nela, que é
    o tipo de lista que ninguém mantém.
    """
    linhas = s.execute(text("""
        SELECT d.numero, d.colaborador_id, d.atendido_em, d.criado_em,
               c.nome, c.demissao, o.codigo AS obra, d.obra_id, d.solicitante_id
          FROM dp_solicitacoes d
          JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
         WHERE d.tipo = 'DESLIGAMENTO'
           AND d.situacao <> 'CANCELADA'
           AND d.dados ->> 'continua_prestador' = '1'
           AND upper(COALESCE(c.situacao, '')) = 'DESLIGADO'
           AND NOT EXISTS (SELECT 1 FROM colaborador_vinculos v
                            WHERE v.colaborador_id = c.id
                              AND v.demissao IS NULL)
         ORDER BY c.demissao NULLS LAST, d.criado_em""")).mappings().all()
    return [{"numero": l["numero"], "colaborador_id": l["colaborador_id"],
             "pessoa": l["nome"], "obra": l["obra"] or "",
             "saiu_em": l["demissao"].isoformat() if l["demissao"] else None,
             "obra_id": l["obra_id"], "solicitante_id": l["solicitante_id"]}
            for l in linhas]


def tipos_para_tela(s: Optional[Session] = None) -> list[dict[str, Any]]:
    """Os tipos, com os campos que cada um pede.

    ⚑ A OBRA ENTRA EM TODOS, mesmo nos tipos que não a listavam (10/11/2026):
    ela passou a ser obrigatória em todo pedido. Acrescentada AQUI, e não
    copiada em cada tipo lá em cima, porque esquecer de copiá-la no tipo
    número doze é o defeito garantido — e ele só apareceria quando alguém
    tentasse abrir justamente aquele pedido.

    Ela vai na FRENTE da lista: "de qual obra é isto?" é a primeira pergunta
    de quem abre um pedido, não a última.
    """
    saida = []
    for c, d in TIPOS.items():
        # ⚑ TIPO OCULTO NÃO APARECE NA CAIXINHA (15/11/2026): desligamento de
        # prestador, readmissão e vale-transporte viraram, respectivamente,
        # ramo do desligamento, detecção de CPF no cadastro e opção da ajuda
        # de custo. A entrada fica no dicionário pelos pedidos já gravados.
        if d.get("oculto"):
            continue
        campos = list(d["campos"])
        # ⚑ A OBRA SÓ SE PERGUNTA QUANDO NÃO HÁ DE ONDE TIRÁ-LA (15/11/2026).
        #
        # Ele: *"a obra está no cadastro. Ignora isso aí, tira isso aí"* — e,
        # perguntado se isso valia para o cadastro de gente nova: *"admissão
        # nova ou de colaborador demitido tem que ter a obra. Demais não
        # precisa."*
        #
        # É exatamente a divisão `exige_pessoa`: pedido sobre quem já tem
        # ficha tira a obra da ficha; pedido que CRIA a ficha (cadastro,
        # readmissão) não tem de onde tirar, e pergunta. Perguntar nos outros
        # era pedir onze vezes um dado que o sistema já tinha — e abrir a
        # porta para alguém responder diferente do cadastro.
        if not d["exige_pessoa"] and "obra_id" not in campos:
            campos.insert(0, "obra_id")
        # ⚑ O ASTERISCO VEM DAQUI, e agora ele é de verdade. Ele, em
        # 10/11/2026: *"Pipe, todo campo obrigatório, ele tem um asterisco,
        # facilita. Aqui não, ninguém sabe o que é obrigatório, o que é que
        # não é."* A lista é a do Pipe, tipo por tipo.
        # A transferência é a exceção: a obra que ela pergunta é a de DESTINO,
        # que a ficha não tem (20/11/2026).
        obrigatorios = ([x for x in d.get("obrigatorios", ()) if x != "obra_id"]
                        if d["exige_pessoa"] and c != "TRANSFERENCIA_OBRA"
                        else ["obra_id"] + [x for x in d.get("obrigatorios", ())
                                            if x != "obra_id"])
        proprios = d.get("rotulos_proprios") or {}
        saida.append({"valor": c, "rotulo": d["nome"],
                      "explicacao": d["explicacao"],
                      "exige_pessoa": d["exige_pessoa"], "campos": campos,
                      "obrigatorios": obrigatorios,
                      # ⚑ O QUE MUDA COM O VÍNCULO DA PESSOA (desligamento).
                      # A tela redesenha o formulário ao escolher a pessoa; o
                      # servidor confere pela mesma lista.
                      "por_vinculo": {
                          v: {"campos": list(r.get("campos") or campos),
                              "obrigatorios": list(
                                  r.get("obrigatorios")
                                  or d.get("obrigatorios", ())),
                              "campos_do_dp": list(
                                  r.get("campos_do_dp")
                                  if "campos_do_dp" in r
                                  else d.get("campos_do_dp", ()))}
                          for v, r in (d.get("por_vinculo") or {}).items()},
                      # Campo que depende de outra resposta: o que passa a ser
                      # exigido, e o que deixa de aparecer.
                      "obrigatorios_se": {
                          k: {kk: list(vv) for kk, vv in mapa.items()}
                          for k, mapa in (d.get("obrigatorios_se") or {}).items()},
                      "ocultos_se": {
                          k: {kk: list(vv) for kk, vv in mapa.items()}
                          for k, mapa in (d.get("ocultos_se") or {}).items()},
                      "visiveis_se": {
                          k: {kk: list(vv) for kk, vv in mapa.items()}
                          for k, mapa in (d.get("visiveis_se") or {}).items()},
                      # Campo cuja LISTA depende de outra resposta (tipo de
                      # contrato, que muda com o tipo de documentação).
                      "opcoes_se": {
                          k: {kk: {kkk: list(vvv) for kkk, vvv in mm.items()}
                              for kk, mm in mapa.items()}
                          for k, mapa in (d.get("opcoes_se") or {}).items()},
                      # ⚑ O RÓTULO→CÓDIGO VIAJA JUNTO, e sem isto as condições
                      # da tela não funcionariam: as listas acima são escritas
                      # com o CÓDIGO que o banco guarda ("CTPS", "DIARISTA"),
                      # enquanto a caixinha mostra o rótulo do Pipe ("CTPS",
                      # "Diarista"). Parecem iguais em um caso e são
                      # diferentes no outro — que é justamente o jeito de o
                      # defeito passar na conferência e aparecer na tela.
                      "traducoes": {x: dict(DO_PIPE_PARA_O_CODIGO[x])
                                    for x in campos
                                    if x in DO_PIPE_PARA_O_CODIGO},
                      # O que o DP preenche ao atender — NÃO vai no formulário
                      # de quem pede. A tela de atendimento lê daqui.
                      "campos_do_dp": list(d.get("campos_do_dp", ())),
                      # Os papéis que este pedido exige, quando exige.
                      "documentos": list(d.get("documentos", ())),
                      # Pedido que aceita vários de uma vez (dependente).
                      "repete": bool(d.get("repete")),
                      # Qual papel preenche qual campo — a tela só pergunta
                      # quando a leitura não achou.
                      "vem_do_documento": dict(d.get("vem_do_documento") or {}),
                      # O rótulo e as opções de cada campo, como no Pipe.
                      "rotulos": {x: proprios.get(x, ROTULOS.get(x, x))
                                  for x in campos},
                      "opcoes": {x: _opcoes(s, x) for x in campos
                                 if x in CATEGORIAS},
                      # O que cada opção SIGNIFICA — a conta por extenso, para
                      # ninguém ter de adivinhar a diferença entre "Mensal" e
                      # "Mês" (15/11/2026).
                      "explicacoes": {x: EXPLICACAO_DA_OPCAO[x]
                                      for x in campos
                                      if x in EXPLICACAO_DA_OPCAO},
                      # O mesmo, para os campos que o DP preenche ao atender:
                      # a tela dele lê daqui, e não de uma segunda lista.
                      # ⚑ 23/11/2026: os campos do DP de TODOS os ramos do
                      # vínculo, não só os do tipo. A redução de jornada e o
                      # "cumpre aviso" só existem no ramo CTPS do desligamento
                      # e chegavam à tela SEM as opções — viravam texto livre.
                      "rotulos_do_dp": {
                          x: proprios.get(x, ROTULOS_DO_DP.get(x, ROTULOS.get(x, x)))
                          for x in _campos_do_dp_todos(d)},
                      "opcoes_do_dp": {x: _opcoes(s, x)
                                       for x in _campos_do_dp_todos(d)
                                       if x in CATEGORIAS}})
    return saida


def _campos_do_dp_todos(regra: dict[str, Any]) -> list[str]:
    """Os campos do DP do tipo e de cada ramo do vínculo, sem repetir."""
    saida = list(regra.get("campos_do_dp", ()))
    for ramo in (regra.get("por_vinculo") or {}).values():
        saida += [x for x in ramo.get("campos_do_dp", ()) if x not in saida]
    return saida


def _dec(v: Any, campo: str) -> Optional[Decimal]:
    texto = str(v or "").strip().replace("R$", "").strip()
    if not texto:
        return None
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        raise ErroValidacao(f"Valor inválido em {campo}: {v!r}")


def _data(v: Any, campo: str) -> Optional[date]:
    from datetime import datetime
    texto = str(v or "").strip()
    if not texto:
        return None
    for formato in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    raise ErroValidacao(f"Data inválida em {campo}: {v!r}")


def proximo_numero(s: Session) -> str:
    """SDP-000001, como o "Nº SDP" do pipe. Numeração humana, não o id."""
    n = s.execute(text("SELECT nextval('dp_solicitacao_numero_seq')")).scalar()
    return f"SDP-{int(n):06d}"


# ---------------------------------------------------------------------------
# Abrir
# ---------------------------------------------------------------------------
def abrir(s: Session, dados: dict[str, Any], usuario: Usuario) -> dict[str, Any]:
    tipo = str(dados.get("tipo") or "").strip().upper()
    if tipo not in TIPOS:
        raise ErroValidacao(f"Tipo de solicitação desconhecido: {tipo or '(vazio)'}")
    regra = TIPOS[tipo]
    colaborador_id = int(dados["colaborador_id"]) if dados.get("colaborador_id") else None
    if regra["exige_pessoa"] and not colaborador_id:
        raise ErroValidacao(f"{regra['nome']}: escolha a pessoa.")
    pessoa = s.get(Colaborador, colaborador_id) if colaborador_id else None
    if colaborador_id and pessoa is None:
        raise ErroValidacao("Colaborador não encontrado.")

    # -----------------------------------------------------------------------
    # ⚑ O CPF DECIDE SE É CADASTRO OU READMISSÃO (15/11/2026)
    #
    # Ele: *"na admissão, quando digitar o CPF, o sistema tem que detectar que
    # a pessoa já tem cadastro e oferecer a readmissão. Aí readmissão deixa de
    # ser um tipo de solicitação."*
    #
    # Antes, CPF repetido era RECUSA: "já tem cadastro, é uma readmissão" — e
    # quem pediu tinha de fechar a janela, descobrir que existe um tipo
    # chamado Readmissão, e redigitar tudo. Quem não soubesse disso tentava
    # outra vez com o nome escrito diferente, e nascia a segunda ficha da
    # mesma pessoa.
    #
    # Agora o pedido se CONVERTE: o mesmo formulário, a mesma pessoa, e o
    # atendimento reabre o vínculo em vez de criar um cadastro. Pessoa ATIVA
    # continua sendo recusa — aí não há nada para readmitir, há uma ficha para
    # corrigir.
    readmissao = False
    # ⚑ A EMPRESA ESCOLHIDA É CONFERIDA AQUI, e o CNPJ dela entra no pedido
    # (15/11/2026). O pedido guarda o NÚMERO da empresa — é o que o cadastro
    # da pessoa usa (`empresa_id`) — e o CNPJ vai junto só para o DP ler sem
    # abrir outra tela, e para o que já lia `cnpj_admissao` continuar lendo.
    #
    # Conferir aqui, e não só na tela, é o que impede pedido com empresa
    # inativa ou inexistente: tela esconde opção, servidor é que recusa.
    if dados.get("empresa_id"):
        emp = empresa_do_pedido(s, dados["empresa_id"])
        dados = {**dados, "cnpj_admissao": emp["cnpj"] or None}

    if tipo == "CADASTRO":
        from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
        # ⚑ O DESLIGADO ESCOLHIDO PELA BUSCA (19/11/2026). Ele: *"se não tiver
        # documento, eu queria poder pesquisar o nome da pessoa (…) e só
        # selecionar sem ter a necessidade de incluir de novos documentos."*
        # A busca devolve a pessoa, não o CPF inteiro (o portal não expõe
        # CPF); o CPF sai do cadastro, e a regra abaixo decide como sempre.
        if pessoa is not None and not somente_digitos(dados.get("cpf") or ""):
            dados = {**dados, "cpf": pessoa.cpf}
        if pessoa is not None and not str(dados.get("nome") or "").strip():
            dados = {**dados, "nome": pessoa.nome}
        cpf = somente_digitos(dados.get("cpf") or "")
        if not cpf_valido(cpf):
            raise ErroValidacao("CPF inválido.")
        ja = svc_pessoas.conferir_cpf(s, cpf)
        if ja["situacao"] == "ativo":
            raise ErroValidacao(ja["aviso"])
        if ja["situacao"] == "desligado":
            readmissao = True
            colaborador_id = int(ja["colaborador_id"])
            pessoa = s.get(Colaborador, colaborador_id)
        dados = {**dados, "cpf": cpf}

    # O PEDIDO SÓ GUARDA OS CAMPOS DO TIPO. Sem esta peneira, o formulário de um
    # tipo deixaria lixo do outro no JSONB — e foi assim que o pipe terminou com
    # 106 campos e metade vazia em cada card.
    # O VÍNCULO DA PESSOA é o que diz quais campos este pedido tem. Vem do
    # cadastro, nunca do formulário: quem abre não tem de saber se a pessoa é
    # de carteira ou prestadora, e se tivesse de saber erraria.
    # No CADASTRO o vínculo que vale é o ESCOLHIDO, não o que a pessoa tinha:
    # numa readmissão ela pode estar voltando de prestador para carteira, e é a
    # contratação nova que decide os campos.
    escolhido = (traduzir_do_pipe(dados).get("tipo_documentacao")
                 if tipo == "CADASTRO" else None)
    vinculo = escolhido or (pessoa.vinculo if pessoa else None)
    pedido = traduzir_do_pipe(
        {c: dados.get(c) for c in campos_do_tipo(tipo, vinculo)
         if dados.get(c) not in (None, "")})
    # Qual programação de férias se altera ou cancela, quando o pedido sai da
    # tela de Férias (o botão na linha). Pelo portal, sem ela, vale a próxima
    # férias da pessoa (`ferias.vigente`).
    if tipo in ("ALTERACAO_FERIAS", "CANCELAMENTO_FERIAS") \
            and str(dados.get("ferias_id") or "").isdigit():
        pedido["ferias_id"] = str(dados["ferias_id"])
    if tipo in ("CADASTRO", "ADMISSAO_PRESTADOR"):
        # ⚑ CAIXA ALTA E SEM ACENTO (19/11/2026), digitado ou lido — ver
        # `documentos_de_pessoa.caixa_alta`.
        from app.apps.erp.core.documentos_de_pessoa import (
            CAMPOS_DE_TEXTO, EXTRAS_DA_LEITURA, caixa_alta, normalizar_campos)
        # O que a leitura achou além do formulário (pai, sexo, estado civil,
        # naturalidade, série da CTPS…) — passado de novo pela conferência de
        # forma, porque vem do navegador.
        # O telefone chega formatado da tela ("(85) 98888-7777"); guarda-se
        # só o número, como o resto do sistema (WhatsApp, ponto) espera.
        if pedido.get("telefone"):
            pedido["telefone"] = "".join(c for c in str(pedido["telefone"])
                                         if c.isdigit())
        lidos = normalizar_campos(dados.get("lidos_do_documento") or {})
        for c in EXTRAS_DA_LEITURA:
            if lidos.get(c) and not pedido.get(c):
                pedido[c] = lidos[c]
        for c in CAMPOS_DE_TEXTO:
            if str(pedido.get(c) or "").strip():
                pedido[c] = caixa_alta(pedido[c])
    if readmissao:
        # Fica GRAVADO no pedido, e não só deduzido depois: é o que faz a
        # tela do DP e o histórico dizerem "readmissão" em vez de "cadastro".
        pedido["readmissao"] = "1"
    # O CNPJ da empresa escolhida viaja no pedido para o DP LER — a escolha
    # que vale é `empresa_id`. Dois campos para a mesma coisa só não divergem
    # porque um deles é derivado do outro, aqui, num lugar só.
    if pedido.get("empresa_id") and dados.get("cnpj_admissao"):
        pedido["cnpj_admissao"] = dados["cnpj_admissao"]

    # Duas conferências que evitam fila com pedido impossível. O resto é do
    # atendimento: conferir tudo na abertura empurraria o trabalho do DP para a
    # obra, que é o contrário do que a fila existe para fazer.
    # ⚑ O QUE FALTA É O QUE O PIPE MARCA COMO OBRIGATÓRIO, e nada além disso.
    # Antes eu exigia a DATA DE SAÍDA no desligamento — campo que, no Pipe,
    # não está neste formulário. Ele, em 10/11/2026: *"de saída, o último dia
    # trabalhado, não é a pessoa que está pedindo que vai dizer. Ele vai
    # colocar só a data do aviso prévio."* A data de saída é do DP, e entra
    # no atendimento.
    ja_tem = o_que_o_cadastro_ja_tem(s, tipo, colaborador_id)
    # O que já estava no cadastro entra no pedido: ele fica completo, e o DP
    # vê o número sem ter de abrir a ficha ao lado.
    for campo, valor in ja_tem.items():
        pedido.setdefault(campo, valor)
    exige = exigencias(tipo, pedido, ja_tem, vinculo)
    # ⚑ PEDIDO QUE ACEITA VÁRIOS (dependente) confere CADA UM, e não o corpo.
    # Ele: *"dependente tem que ser um a um, não posso adicionar outro, não,
    # para pedir mais de um."* Com a lista preenchida, exigir os campos no
    # topo seria exigir um quarto dependente solto.
    varios = pedido.get("dependentes")
    if regra.get("repete") and isinstance(varios, list) and varios:
        de_cada = [c for c in exige["campos"]
                   if c not in ("obra_id", "dependentes")]
        for n, um in enumerate(varios, 1):
            falta = [ROTULOS.get(c, c) for c in de_cada
                     if not (um or {}).get(c)]
            if falta:
                raise ErroValidacao(
                    f"No {n}º dependente, falta preencher: "
                    + ", ".join(falta) + ".")
        faltando = []
    else:
        proprios = regra.get("rotulos_proprios") or {}
        faltando = [proprios.get(c) or ROTULOS.get(c, c) for c in exige["campos"]
                    if c != "dependentes" and not pedido.get(c)]
    # ⚑ A OBRA SAI DESTA CONTA, e tem conferência PRÓPRIA logo abaixo: ela
    # pode vir do pedido OU do cadastro da pessoa, e exigi-la aqui recusaria
    # o caso comum (pedido da pessoa, que já tem obra) antes de olhar o
    # cadastro.
    faltando = [x for x in faltando if x != ROTULOS["obra_id"]]
    if faltando:
        raise ErroValidacao("Falta preencher: " + ", ".join(faltando) + ".")

    # ⚑ E OS PAPÉIS. `documentos` são os tipos dos arquivos que vieram JUNTO
    # com o pedido (a rota do portal os guarda na mesma transação — ver
    # `guardar_papeis`).
    #
    # ⚑ NA READMISSÃO NÃO SE EXIGE PAPEL (19/11/2026). Ele: *"já que é uma
    # pessoa que a gente já tem cadastro, né? Então a gente não precisaria."*
    # Os papéis da primeira passagem estão no cadastro dela; o que faltar de
    # DADO a obra digita (acima, pela mesma conta de sempre).
    anexados = {str(x).strip().upper() for x in (dados.get("documentos") or [])}
    sem_papel = [] if readmissao else [
        d["rotulo"] for d in exige["documentos"] if d["codigo"] not in anexados]
    if sem_papel:
        porque = (" Sem esses papéis o cadastro não se faz e a pessoa não "
                  "entra no sistema de pagamento — por isso o pedido não sai "
                  "incompleto." if tipo == "CADASTRO" else
                  " O pedido não sai sem eles.")
        raise ErroValidacao("Falta anexar: " + ", ".join(sem_papel) + "." + porque)
    # ⚑ A OBRA É OBRIGATÓRIA EM TODO PEDIDO (10/11/2026). O dono: *"eu quero
    # que seja obrigatória, porque a gente não sabe de onde é que está vindo
    # aquela pessoa, para onde está indo trabalhar."*
    #
    # Ela pode vir de dois lugares, e a ordem importa: o que o pedido DISSE
    # vence, e só na falta dele vale a obra do cadastro da pessoa. É o caso da
    # transferência — a obra que pede é a de DESTINO, e sobrescrevê-la pela do
    # cadastro mandaria o pedido para a obra de onde a pessoa está saindo.
    #
    # Quando não há nem uma nem outra, o pedido é RECUSADO em vez de entrar
    # sem obra: pedido sem obra é pedido que ninguém sabe de onde veio, e
    # quem for atendê-lo vai perguntar por WhatsApp — que é o trabalho que
    # esta fila existe para acabar.
    if tipo == "TRANSFERENCIA_OBRA" and not str(dados.get("obra_id") or "").isdigit():
        raise ErroValidacao("Escolha a obra para onde a pessoa vai.")
    obra_do_pedido = (int(dados["obra_id"]) if dados.get("obra_id")
                      else (pessoa.obra_id if pessoa else None))
    if not obra_do_pedido:
        raise ErroValidacao(
            "Informe a obra. Sem ela não dá para saber de onde vem a pessoa "
            "nem para onde ela vai trabalhar.")
    numero = proximo_numero(s)
    linha = s.execute(text("""
        INSERT INTO dp_solicitacoes
               (numero, tipo, situacao, colaborador_id, nome_informado, cpf_informado,
                obra_id, solicitante_id, requerente, dados, data_referencia, valor)
        VALUES (:numero, :tipo, 'ABERTA', :colaborador_id, :nome, :cpf, :obra_id,
                :solicitante, :requerente, CAST(:dados AS jsonb), :ref, :valor)
        RETURNING id"""), {
        "numero": numero, "tipo": tipo, "colaborador_id": colaborador_id,
        "nome": (pessoa.nome if pessoa else (pedido.get("nome") or None)),
        "cpf": (pessoa.cpf if pessoa else (pedido.get("cpf") or None)),
        "obra_id": obra_do_pedido,
        "solicitante": usuario.id if usuario else None,
        # ⚑ "REQUERENTE" SAIU DE USO EM 10/11/2026, por decisão dele:
        # *"requerente era assim: eu solicito por alguém; aí esse alguém é o
        # requerente. Mas vamos deixar de fora por enquanto."*
        #
        # A COLUNA FICA, e isso é escolha: há pedidos gravados com ela, e
        # apagá-los tiraria a resposta de "quem pediu isto?" de registros que
        # já existem. O que saiu foi PERGUNTAR. "Por enquanto" é a palavra
        # dele — voltar é reabrir o campo, não refazer a tabela.
        "requerente": None,
        "dados": __import__("json").dumps(pedido, ensure_ascii=False, default=str),
        "ref": _data(pedido.get("data_saida") or pedido.get("admissao")
                     or pedido.get("vigencia") or pedido.get("data_nascimento"), "data"),
        "valor": _dec(pedido.get("valor_total") or pedido.get("salario")
                      or pedido.get("aux_transporte"), "valor"),
    })
    solicitacao_id = int(linha.scalar())
    # -----------------------------------------------------------------------
    # ⚑ A ADMISSÃO NASCE COM O PRÉ-CADASTRO (19/11/2026)
    #
    # Ele: *"eu fiz uma admissão de colaborador e eu pensei que a pessoa já
    # apareceria na fase inicial (…) Se eu atender (…) ele gera o cadastro
    # inicial, é isso? Não tem sentido. (…) não existe um cadastro ou
    # pré-cadastro dessa pessoa já."*
    #
    # Agora a pessoa nova nasce no cadastro na hora do pedido, na etapa
    # INICIAL, com os dados e os papéis — é lá que o DP confere. Atender o
    # pedido passa a ser completar o que é do DP (matrícula, convenção,
    # cargo, salário). Recusado ou cancelado, o pré-cadastro vai para
    # "Cancelado". A readmissão continua como era: a pessoa já existe, e só
    # volta à ativa quando o DP atende.
    # -----------------------------------------------------------------------
    if tipo == "CADASTRO" and not readmissao and not colaborador_id:
        novo = svc_pessoas.salvar(s, {
            **para_o_cadastro(pedido), "obra_id": obra_do_pedido,
            "etapa": "CADASTRO_INICIAL"}, usuario)
        colaborador_id = novo.id
        pedido["pre_cadastro"] = "1"
        s.execute(text("""UPDATE dp_solicitacoes
                             SET colaborador_id = :c, dados = CAST(:d AS jsonb)
                           WHERE id = :s"""),
                  {"c": novo.id, "s": solicitacao_id,
                   "d": __import__("json").dumps(pedido, ensure_ascii=False,
                                                 default=str)})
    for i, item in enumerate(checklist_do_tipo(tipo, vinculo,
                                               readmissao=readmissao,
                                               pedido=pedido, s=s)):
        s.execute(text("""INSERT INTO dp_solicitacao_itens (solicitacao_id, ordem, descricao)
                          VALUES (:s, :o, :d)"""),
                  {"s": solicitacao_id, "o": i, "d": item})
    _evento(s, solicitacao_id, "ABERTA", {"tipo": tipo, "pedido": pedido}, usuario)
    if colaborador_id:
        registrar_evento(s, "colaborador", colaborador_id, "SOLICITACAO_DP_ABERTA",
                         {"numero": numero, "tipo": tipo},
                         usuario.id if usuario else None)
    logger.info("ERP/DP: solicitação %s aberta (%s) por %s", numero, tipo,
                usuario.nome if usuario else "?")
    return ler(s, solicitacao_id)


def _evento(s: Session, solicitacao_id: int, acao: str, detalhe: dict,
            usuario: Optional[Usuario]) -> None:
    s.execute(text("""INSERT INTO dp_solicitacao_eventos
                             (solicitacao_id, acao, detalhe, usuario_id)
                      VALUES (:s, :a, CAST(:d AS jsonb), :u)"""),
              {"s": solicitacao_id, "a": acao,
               "d": __import__("json").dumps(detalhe, ensure_ascii=False, default=str),
               "u": usuario.id if usuario else None})


# ---------------------------------------------------------------------------
# ⚑ OS PAPÉIS DO PEDIDO (19/11/2026)
#
# ⚠️ DEFEITO ACHADO NESTE TRABALHO, e grave: o portal LIA os documentos
# anexados e mandava ao servidor só os TIPOS ("anexei o RG") — o arquivo nunca
# subia. O comentário daqui dizia "o arquivo já subiu e está esperando", e a
# coluna `documentos.dp_solicitacao_id` (migração 100) existia sem ninguém
# escrever nela. Ou seja: o DP recebia o pedido de admissão sem papel nenhum.
#
# Agora os arquivos vêm NA MESMA requisição do pedido e são guardados na mesma
# transação: pessoa que já existe (readmissão) recebe o papel direto no
# cadastro; pessoa nova recebe quando o DP atende (`transferir_papeis`).
# ---------------------------------------------------------------------------
def cancelar_desligamento(s: Session, colaborador_id: int, p: dict[str, Any],
                          usuario: Optional[Usuario]) -> dict[str, Any]:
    """Desfaz o desligamento que ainda não aconteceu (20/11/2026).

    ⚠️ ANTES, atender este pedido não fazia NADA: a data de saída continuava
    marcada, a virada do dia desligava a pessoa no dia, e a contabilidade
    seguia preparando a rescisão.

    O que faz:
      · recusa se a pessoa JÁ saiu — aí não é cancelar, é readmitir;
      · apaga a saída agendada (data de saída, aviso, último dia) do cadastro
        e do vínculo aberto;
      · encerra o pedido de desligamento em andamento como CANCELADO;
      · pedido de rescisão à contabilidade ainda em rascunho: cancela; já
        enviado: o cancelamento PASSA pela contabilidade (modelo
        "Cancelamento de desligamento"), e este pedido fica Em andamento até
        ela devolver.
    """
    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    # ⚑ O CANCELAMENTO PEDIDO A TEMPO VALE MESMO DEPOIS DA DATA (20/11/2026).
    # A virada do dia SEGURA quem tem cancelamento aberto
    # (`desligamentos_do_dia`): se a obra pediu antes da saída e o DP atendeu
    # um dia depois, a pessoa não foi desligada — e recusar aqui a jogaria na
    # readmissão por demora nossa. Ele: *"Imagina desligar uma pessoa que vai
    # continuar trabalhando. É uma falha grave."*
    criado = p.get("criado_em")
    try:
        criado_dia = (criado.date() if hasattr(criado, "date")
                      else date.fromisoformat(str(criado or "")[:10]))
    except ValueError:
        criado_dia = None
    a_tempo = (c.demissao is not None and criado_dia is not None
               and criado_dia <= c.demissao
               and (c.situacao or "ATIVO").upper() != "DESLIGADO")
    if svc_pessoas.ja_saiu(c.demissao) and not a_tempo:
        raise ErroValidacao(
            f"{c.nome} já saiu em {c.demissao:%d/%m/%Y}. Depois da saída não se "
            "cancela o desligamento — o caminho é a readmissão.")
    efeito: dict[str, Any] = {}
    desl = s.execute(text("""
        SELECT id, numero, situacao FROM dp_solicitacoes
         WHERE colaborador_id = :c AND tipo = 'DESLIGAMENTO'
           AND situacao IN ('ABERTA', 'EM_ANALISE', 'EM_ANDAMENTO')
         ORDER BY id DESC LIMIT 1"""), {"c": c.id}).mappings().first()
    antes = {"data_saida": str(c.demissao) if c.demissao else None,
             "aviso_previo": str(c.aviso_previo) if c.aviso_previo else None,
             "ultimo_dia": str(c.ultimo_dia) if c.ultimo_dia else None}
    c.demissao = c.aviso_previo = c.ultimo_dia = None
    v = svc_pessoas.vinculo_aberto(s, c.id)
    if v is not None:
        v.demissao = v.aviso_previo = v.ultimo_dia = None
        v.forma_desligamento = None
    s.flush()
    registrar_evento(s, "colaborador", c.id, "DESLIGAMENTO_CANCELADO",
                     {"pedido": p.get("numero"), "antes": antes},
                     usuario.id if usuario else None)
    if desl is not None:
        # A RESPOSTA TAMBÉM: é ela que quem pediu o desligamento lê no portal
        # ("O que você já pediu") — e ele pode não ser quem pediu o
        # cancelamento. Todo mundo envolvido fica sabendo.
        s.execute(text("""UPDATE dp_solicitacoes
                             SET situacao = 'CANCELADA', cancelado_por = :u,
                                 cancelado_em = now(), atualizado_em = now(),
                                 motivo_cancelamento = :m, resposta = :m
                           WHERE id = :s"""),
                  {"u": usuario.id if usuario else None, "s": desl["id"],
                   "m": f"desligamento cancelado pelo pedido {p.get('numero')} "
                        "— a pessoa continua trabalhando"})
        _evento(s, desl["id"], "CANCELADA",
                {"motivo": f"cancelado pelo pedido {p.get('numero')}"}, usuario)
        efeito["desligamento_cancelado"] = desl["numero"]
    rescisao = s.execute(text("""
        SELECT id, numero, situacao, criado_em FROM dp_pedidos
         WHERE colaborador_id = :c AND template = 'DESLIGAMENTO'
           AND situacao NOT IN ('CANCELADO', 'CONCLUIDO')
         ORDER BY id DESC LIMIT 1"""), {"c": c.id}).mappings().first()
    if rescisao is not None:
        if rescisao["situacao"] == "RASCUNHO":
            from app.apps.erp.core import contabilidade as svc_ct
            svc_ct.cancelar(s, rescisao["id"], usuario,
                            motivo=f"desligamento cancelado ({p.get('numero')})")
        else:
            # A contabilidade já recebeu a rescisão: o pedido de cancelamento
            # sai montado com a data em que ela foi comunicada.
            s.execute(text("""UPDATE dp_solicitacoes
                                 SET dados = dados || CAST(:j AS jsonb)
                               WHERE id = :s"""),
                      {"s": p["id"], "j": __import__("json").dumps(
                          {"desligamento_comunicado_em":
                           rescisao["criado_em"].date().isoformat()})})
    # ⚑ DE CARTEIRA, O CANCELAMENTO SEMPRE PASSA PELA CONTABILIDADE
    # (20/11/2026). Antes, só quando a rescisão já tinha sido enviada. Ele:
    # *"esse cancelamento tem que ser avisado à contabilidade, ela tem que
    # estar ciente, tem que ser dado o retorno."* A contabilidade pode saber
    # do desligamento por outro caminho (telefone, e-mail, o eSocial já
    # agendado) — avisar quando ela não sabia custa um e-mail; não avisar
    # quando ela sabia é rescisão de quem continua trabalhando.
    # O prestador não tem registro na contabilidade: fecha no atendimento.
    if (vinculo_da_pessoa(s, c.id) or "CTPS").upper() == "CTPS":
        efeito["precisa_contabilidade"] = True
    return efeito


def _transferir_obra(s: Session, colaborador_id: int, p: dict[str, Any],
                     pedido: dict[str, Any], usuario: Optional[Usuario]) -> dict[str, Any]:
    """A obra de CADASTRO passa a ser a de destino. A de antes fica na trilha
    e no pedido — é o "de" que a contabilidade precisa para as guias."""
    destino = pedido.get("obra_id") or p.get("obra_id")
    if not str(destino or "").isdigit():
        raise ErroValidacao("Escolha a obra de destino.")
    c = s.get(Colaborador, colaborador_id)
    de = c.obra_id
    if de == int(destino):
        return {"transferencia": "a obra de cadastro já era esta"}
    c.obra_id = int(destino)
    s.flush()
    s.execute(text("UPDATE dp_solicitacoes SET dados = dados || CAST(:j AS jsonb) "
                   "WHERE id = :i"),
              {"i": p["id"], "j": __import__("json").dumps({"obra_origem_id": str(de or "")})})
    registrar_evento(s, "colaborador", c.id, "TRANSFERENCIA_DE_OBRA",
                     {"de": de, "para": int(destino), "pedido": p.get("numero"),
                      "a_partir_de": pedido.get("transferencia_data")},
                     usuario.id if usuario else None)
    return {"transferencia": f"obra {de} → {destino}"}


def _registrar_afastamento(s: Session, colaborador_id: int, pedido: dict[str, Any],
                           p: dict[str, Any], usuario: Optional[Usuario]) -> dict[str, Any]:
    """Atender a comunicação de afastamento: o afastamento entra na ficha (se
    ainda não estiver) e o pedido segue para a contabilidade."""
    from app.apps.erp.core.cadastros import afastamentos
    natureza = str(pedido.get("afastamento_natureza") or "").upper()
    if natureza not in ("INSS", "ACIDENTE", "MATERNIDADE", "ATESTADO"):
        raise ErroValidacao("Escolha a natureza do afastamento (INSS, acidente, "
                            "maternidade) — é o que a contabilidade informa ao "
                            "eSocial.")
    if afastamentos.aberto(s, colaborador_id) is not None:
        return {"afastamento": "já estava na ficha"}
    afastamentos.abrir(s, colaborador_id, {
        "tipo": natureza, "data_inicio": pedido.get("afast_inicio"),
        "previsao_retorno": pedido.get("afast_previsao") or "",
        "motivo": f"{p.get('numero')}: " + str(pedido.get("observacao") or "")[:400]},
        usuario)
    return {"afastamento": "registrado na ficha"}


def _registrar_retorno(s: Session, colaborador_id: int, pedido: dict[str, Any],
                       p: dict[str, Any], usuario: Optional[Usuario]) -> dict[str, Any]:
    """Atender o retorno: a volta é registrada na ficha (com as travas dela —
    o exame de retorno), salvo se o DP já registrou por lá."""
    from app.apps.erp.core.cadastros import afastamentos
    if afastamentos.aberto(s, colaborador_id) is None:
        return {"retorno": "já registrado na ficha"}
    afastamentos.retornar(s, colaborador_id, {
        "data_retorno": pedido.get("retorno_data"),
        "observacao": f"{p.get('numero')}: " + str(pedido.get("observacao") or "")[:400]},
        usuario)
    return {"retorno": "registrado na ficha"}


def comunicar_pela_ficha(s: Session, colaborador_id: int, tipo: str,
                         dados: dict[str, Any], usuario: Optional[Usuario]) -> Optional[str]:
    """O DP afastou (ou registrou a volta) direto na ficha: nasce a
    solicitação, para a contabilidade ficar sabendo pelo mesmo caminho de
    todo o resto (20/11/2026). Só de carteira, e só se não houver uma aberta
    para a mesma pessoa. Devolve o número, ou None."""
    if (vinculo_da_pessoa(s, colaborador_id) or "CTPS").upper() != "CTPS":
        return None
    ja = s.execute(text("""SELECT numero FROM dp_solicitacoes
                            WHERE colaborador_id = :c AND tipo = :t
                              AND situacao IN ('ABERTA', 'EM_ANALISE')"""),
                   {"c": colaborador_id, "t": tipo}).scalar()
    if ja:
        return None
    if tipo == "AFASTAMENTO":
        corpo = {"afastamento_natureza": str(dados.get("tipo") or "").upper(),
                 "afast_inicio": dados.get("data_inicio"),
                 "afast_previsao": dados.get("previsao_retorno") or "",
                 "observacao": "registrado pelo DP na ficha. "
                               + str(dados.get("motivo") or "")}
    else:
        corpo = {"retorno_data": dados.get("data_retorno"),
                 "observacao": "registrado pelo DP na ficha. "
                               + str(dados.get("observacao") or "")}
    p = abrir(s, {"tipo": tipo, "colaborador_id": colaborador_id,
                  **{k: v for k, v in corpo.items() if v}}, usuario)
    return p["numero"]


def para_o_cadastro(pedido: dict[str, Any]) -> dict[str, Any]:
    """O pedido de admissão no vocabulário da FICHA da pessoa.

    ⚠️ ACHADO EM 19/11/2026: o atendimento passava o pedido direto para o
    cadastro, e quatro campos tinham NOME DIFERENTE lá — o vínculo
    (`tipo_documentacao` × `vinculo`), o cargo (`cargo_pleiteado_id` ×
    `cargo_id`), a CTPS e o PIS. Os quatro se perdiam calados: o prestador
    nascia como CTPS, sem cargo e sem os números da carteira.
    """
    from app.apps.erp.core.cadastros.validadores import somente_digitos
    d = dict(pedido)
    if d.get("tipo_documentacao"):
        d["vinculo"] = d["tipo_documentacao"]
    if d.get("cargo_pleiteado_id"):
        d["cargo_id"] = d["cargo_pleiteado_id"]
    if d.get("num_ctps"):
        d["ctps_numero"] = d["num_ctps"]
    # O PIS não é obrigatório (19/11/2026); um PIS que não passa na conta dos
    # dígitos (leitura torta, digitação) não pode travar a admissão — ele
    # fica no pedido para o DP ver, e não entra no cadastro.
    if d.get("num_pis") and svc_pessoas.pis_valido(d["num_pis"]):
        d["pis"] = d["num_pis"]
    if d.get("data_inicio"):
        d.setdefault("admissao", d["data_inicio"])
    if str(d.get("pagamento") or "").upper() in ("MENSALISTA", "DIARISTA"):
        d["mensalista"] = str(d["pagamento"]).upper() == "MENSALISTA"
    # A MATRÍCULA É O CPF (19/11/2026), desde o nascimento do cadastro.
    cpf = somente_digitos(d.get("cpf") or "")
    if cpf and not str(d.get("matricula") or "").strip():
        d["matricula"] = cpf
    return d


def guardar_papeis(s: Session, solicitacao_id: int,
                   colaborador_id: Optional[int],
                   arquivos: list[dict[str, Any]],
                   usuario: Optional[Usuario] = None) -> int:
    """Guarda os arquivos do pedido. `arquivos`: [{conteudo, nome, tipo}]."""
    from app.apps.erp.core.arquivo import service as svc_arq
    from app.apps.erp.core.documentos.armazenamento import salvar

    numero = s.execute(text("SELECT numero FROM dp_solicitacoes WHERE id = :i"),
                       {"i": solicitacao_id}).scalar() or ""
    feitos = 0
    for a in arquivos:
        tipo = str(a.get("tipo") or "").strip().upper() or "DOC-IDENTIDADE"
        nome = str(a.get("nome") or "documento")
        conteudo = a.get("conteudo") or b""
        if not conteudo:
            continue
        if colaborador_id:
            svc_arq.arquivar(s, conteudo, nome, tipo_codigo=tipo,
                             colaborador_id=colaborador_id,
                             referencia=f"{numero} — enviado pela obra",
                             origem="PORTAL", usuario=usuario)
        else:
            anexo = salvar(s, conteudo, f"{tipo} {numero} {nome}",
                           entidade_tipo="dp_solicitacao",
                           entidade_id=solicitacao_id, categoria="OUTRO",
                           descricao=tipo, usuario=usuario)
            texto_ = ""
            try:
                from app.apps.erp.core.arquivo import texto as svc_texto
                texto_ = svc_texto.extrair(conteudo, nome) or ""
            except Exception:                            # noqa: BLE001
                texto_ = ""
            s.execute(text("""
                INSERT INTO documentos (tipo_codigo, anexo_id, nome_padronizado,
                                        nome_original, dp_solicitacao_id,
                                        referencia, origem, texto)
                VALUES (:t, :a, :n, :o, :s, :r, 'PORTAL', NULLIF(:x, ''))"""),
                {"t": tipo, "a": anexo.id, "n": f"{tipo} — {numero} — {nome}"[:200],
                 "o": nome[:200], "s": solicitacao_id,
                 "r": f"{numero} — enviado pela obra", "x": texto_})
        feitos += 1
    s.flush()
    return feitos


# ---------------------------------------------------------------------------
# ⚑ O PEDIDO EM LINGUAGEM DE GENTE (19/11/2026)
#
# Ele, olhando a tela do atendimento: *"a nomenclatura ela não está amigável
# (…) cargo hífen pleiteado hífen ID, aí está a numeração de sistema do cargo
# (…) não recebe todo maiúsculo com hífen (…) CEP não está no padrão do CEP,
# CNPJ (…) sem pontuação. Data (…) no padrão americano (…) Empresa hífen ID,
# aí está o número da empresa e não o nome."*
#
# A tela mostrava o JSON do pedido cru. Agora o servidor devolve cada campo
# com o rótulo em português e o valor como se lê: nome do cargo, da empresa e
# da obra; o rótulo da opção; data dd/mm/aaaa; CPF, CNPJ, CEP e telefone com
# a pontuação. As marcas internas (pré-cadastro, processo…) não aparecem.
# ---------------------------------------------------------------------------
_MARCAS_INTERNAS = {"pre_cadastro", "readmissao", "processo_desligamento",
                    "processo_admissao", "processo_contabil",
                    "continuidade_revista", "documentos", "lidos_do_documento",
                    "ferias_id"}


def _formatar(campo: str, valor: Any, s: Session) -> str:
    import re as _re
    from datetime import date as _date
    if valor in (None, ""):
        return ""
    if isinstance(valor, list):
        return f"{len(valor)} item(ns)"
    texto_ = str(valor).strip()
    so = "".join(c for c in texto_ if c.isdigit())
    # nomes de cadastro no lugar do número
    tabelas = {"cargo_pleiteado_id": ("cargos", "nome"),
               "cargo_id": ("cargos", "nome"),
               "convencao_id": ("convencoes", "nome"),
               "empresa_id": ("empresas", "COALESCE(nome_fantasia, razao_social)"),
               "obra_id": ("obras", "codigo || ' — ' || nome")}
    if campo in tabelas and so:
        tabela, coluna = tabelas[campo]
        nome = s.execute(text(f"SELECT {coluna} FROM {tabela} WHERE id = :i"),
                         {"i": int(so)}).scalar()
        return str(nome or texto_)
    # ⚑ DINHEIRO NO PADRÃO DO BRASIL (19/11/2026): R$ 1.234,56.
    if campo in CAMPOS_DE_DINHEIRO:
        try:
            v = _dec(texto_, campo)
        except ErroValidacao:
            v = None
        if v is not None:
            inteiro, cent = f"{v:,.2f}".split(".")
            return "R$ " + inteiro.replace(",", ".") + "," + cent
    if campo in ("cpf", "pix_chave") and len(so) == 11 and len(texto_) <= 14:
        return f"{so[:3]}.{so[3:6]}.{so[6:9]}-{so[9:]}"
    if campo in ("cnpj_admissao", "cnpj_pj") and len(so) == 14:
        return f"{so[:2]}.{so[2:5]}.{so[5:8]}/{so[8:12]}-{so[12:]}"
    if campo == "cep" and len(so) == 8:
        return f"{so[:5]}-{so[5:]}"
    if campo == "telefone" and len(so) in (10, 11):
        meio = 7 if len(so) == 11 else 6
        return f"({so[:2]}) {so[2:meio]}-{so[meio:]}"
    if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", texto_):
        a, m, d = texto_.split("-")
        return f"{d}/{m}/{a}"
    # o rótulo da opção, no lugar do código
    rotulos = {
        "categoria_alimentacao": svc_pessoas.CATEGORIA_ALIMENTACAO_ROTULOS,
        "categoria_transporte": svc_pessoas.CATEGORIA_TRANSPORTE_ROTULOS,
        "tipo_contrato": svc_pessoas.TIPO_CONTRATO_ROTULOS,
        "tipo_documentacao": svc_pessoas.VINCULO_ROTULOS,
        "vinculo": svc_pessoas.VINCULO_ROTULOS,
        "forma_desligamento": svc_pessoas.FORMA_DESLIGAMENTO_ROTULOS,
        "sexo": svc_pessoas.SEXO_ROTULOS,
        "estado_civil": svc_pessoas.ESTADO_CIVIL_ROTULOS,
        "parentesco": svc_pessoas.PARENTESCO_ROTULOS,
    }.get(campo)
    if rotulos and texto_ in rotulos:
        r = rotulos[texto_]
        return r if isinstance(r, str) else str(r)
    inverso = {v: k for k, v in (DO_PIPE_PARA_O_CODIGO.get(campo) or {}).items()}
    if texto_ in inverso:
        return inverso[texto_]
    if texto_ in ("1", "0") and campo in SIM_NAO_DOS_CAMPOS:
        return "Sim" if texto_ == "1" else "Não"
    return texto_


CAMPOS_DE_DINHEIRO = {"valor_diaria", "valor_mes", "aux_alimentacao",
                      "aux_transporte", "salario", "valor_total", "valor",
                      "valor_parcela", "valor_gratificacao"}
SIM_NAO_DOS_CAMPOS = {"abono_pecuniario", "adiantamento_13",
                      "continua_prestador", "salario_familia", "conceder",
                      "descontar_vt", "dependente_ir"}


def dados_legiveis(s: Session, tipo: str, dados: dict[str, Any]
                   ) -> list[dict[str, str]]:
    """O pedido como se lê: [{campo, rotulo, valor}] na ordem dos campos do
    tipo (e o que sobrar, depois)."""
    regra = TIPOS.get(tipo) or {}
    proprios = regra.get("rotulos_proprios") or {}
    ordem = list(regra.get("campos") or ()) + list(regra.get("campos_do_dp") or ())
    ordem += [c for c in dados if c not in ordem]
    saida = []
    for c in dict.fromkeys(ordem):
        if c in _MARCAS_INTERNAS or c not in dados:
            continue
        try:
            valor = _formatar(c, dados.get(c), s)
        except Exception:                                # noqa: BLE001
            valor = str(dados.get(c))
        if not valor:
            continue
        rotulo = (proprios.get(c) or ROTULOS.get(c)
                  or svc_pessoas.CAMPO_ROTULOS.get(c) or c.replace("_", " "))
        saida.append({"campo": c, "rotulo": rotulo, "valor": valor})
    return saida


def corrigir(s: Session, solicitacao_id: int, dados: dict[str, Any],
             usuario: Optional[Usuario]) -> dict[str, Any]:
    """O DP corrige o pedido na tela do atendimento — e, havendo cadastro
    ligado (pré-cadastro), o cadastro junto.

    ⚑ Ele: *"se eu precisar editar, esses campos deveriam ser todos
    editáveis (…) se eu entender que a obra não era para ser essa, se eu
    entender que o cargo não era para ser esse, então aqui é o local e momento
    de a gente corrigir."*

    Só campos do tipo; vazio APAGA o campo do pedido (é correção, a pessoa viu
    o campo e o limpou). Fica rastro de/para.
    """
    import json
    p = ler(s, solicitacao_id)
    # ⚑ DEPOIS DE ATENDIDO TAMBÉM SE CORRIGE (20/11/2026, noite) — e o que já
    # foi à contabilidade vira um pedido de CORREÇÃO a ela
    # (`contabil_processo.comunicar_correcao`).
    if p["situacao"] not in A_ATENDER + ("EM_ANDAMENTO", "ATENDIDA"):
        raise ErroValidacao(f"{p['numero']} já está {p['situacao_rotulo'].lower()} "
                            "— não dá mais para corrigir.")
    tipo = p["tipo"]
    permitidos = set(campos_do_tipo(tipo, p.get("pessoa_vinculo") or None)) \
        | set(campos_do_dp_do_tipo(tipo, p.get("pessoa_vinculo") or None)) \
        | {"obra_id"}
    novos = traduzir_do_pipe({k: v for k, v in (dados.get("dados") or {}).items()
                              if k in permitidos})
    if tipo in ("CADASTRO", "ADMISSAO_PRESTADOR"):
        from app.apps.erp.core.documentos_de_pessoa import CAMPOS_DE_TEXTO, caixa_alta
        for c in CAMPOS_DE_TEXTO:
            if str(novos.get(c) or "").strip():
                novos[c] = caixa_alta(novos[c])
        if novos.get("telefone"):
            novos["telefone"] = "".join(x for x in str(novos["telefone"]) if x.isdigit())
    antes = dict(p["dados"] or {})
    depois = dict(antes)
    mudou = []
    for k, v in novos.items():
        v = "" if v is None else v
        if str(antes.get(k) or "") != str(v):
            mudou.append({"campo": k, "de": antes.get(k), "para": v})
            if str(v).strip():
                depois[k] = v
            else:
                depois.pop(k, None)
    if not mudou:
        return p
    nova_obra = depois.get("obra_id")
    s.execute(text("""UPDATE dp_solicitacoes
                         SET dados = CAST(:d AS jsonb), atualizado_em = now(),
                             obra_id = COALESCE(:o, obra_id)
                       WHERE id = :s"""),
              {"d": json.dumps(depois, ensure_ascii=False, default=str),
               "o": int(nova_obra) if str(nova_obra or "").isdigit() else None,
               "s": solicitacao_id})
    _evento(s, solicitacao_id, "CORRIGIDO_PELO_DP", {"mudou": mudou}, usuario)
    # O pré-cadastro acompanha: é a mesma pessoa, e o DP corrigiu olhando o
    # documento. A readmissão aplica as correções no atendimento.
    if (antes.get("pre_cadastro") and p.get("colaborador_id")):
        c = s.get(Colaborador, int(p["colaborador_id"]))
        alterados = {m["campo"]: depois.get(m["campo"], "") for m in mudou}
        svc_pessoas.salvar(s, {**para_o_cadastro(alterados),
                               "cpf": c.cpf, "nome": depois.get("nome") or c.nome},
                           usuario)
    if p["situacao"] not in A_ATENDER:
        _corrigir_depois_de_atendido(s, {**p, "dados": depois}, mudou, usuario)
    s.flush()
    return ler(s, solicitacao_id)


def _corrigir_depois_de_atendido(s: Session, p: dict[str, Any],
                                 mudou: list[dict[str, Any]],
                                 usuario: Optional[Usuario]) -> None:
    """O efeito do atendimento acompanha a correção, onde dá para refazer sem
    risco; e a contabilidade fica sabendo do que já tinha recebido."""
    import json
    tipo = p["tipo"]
    alvo = p.get("colaborador_id")
    campos = {m["campo"] for m in mudou}
    d = p["dados"]
    if tipo in ("ATUALIZACAO_DADOS", "AJUDA_DE_CUSTO", "VALE_TRANSPORTE") and alvo:
        c = s.get(Colaborador, int(alvo))
        svc_pessoas.salvar(s, {**{k: d.get(k, "") for k in campos},
                               "cpf": c.cpf, "nome": c.nome}, usuario)
    elif tipo == "FERIAS" and svc_ferias.tem_tabela(s) and \
            campos & {"ferias_inicio", "ferias_dias"}:
        ini = svc_ferias._data(d.get("ferias_inicio"))
        dias = svc_ferias._dias(d.get("ferias_dias"))
        if ini and dias:
            s.execute(text("""UPDATE ferias_programadas
                                 SET inicio = :i, dias = :d, fim = :f,
                                     atualizado_em = now()
                               WHERE solicitacao_id = :s AND situacao = 'PEDIDA'"""),
                      {"i": ini, "d": dias, "f": svc_ferias.periodo(ini, dias),
                       "s": p["id"]})
    elif tipo == "DESLIGAMENTO" and campos & {"aviso_previo", "ultimo_dia",
                                              "data_saida", "forma_desligamento",
                                              "reducao_jornada", "cumpre_aviso"}:
        from app.apps.erp.core import termos
        if alvo and campos & {"aviso_previo", "ultimo_dia", "data_saida"}:
            c = s.get(Colaborador, int(alvo))
            for campo, coluna in (("aviso_previo", "aviso_previo"),
                                  ("ultimo_dia", "ultimo_dia"),
                                  ("data_saida", "demissao")):
                if campo in campos and d.get(campo) and \
                        (d.get("continua_prestador") != "1" or coluna != "demissao"):
                    setattr(c, coluna, svc_pessoas._data(d[campo], campo))
        # O aviso tem as datas antigas: vai um novo para a pessoa assinar.
        t_ = termos.do_pedido(s, int(p["id"]))
        if t_ is not None:
            if t_["situacao"] == "ABERTO":
                termos.cancelar(s, t_["id"], "as datas foram corrigidas", usuario)
            else:
                s.execute(text("UPDATE dp_termos SET situacao = 'CANCELADO', "
                               "motivo_recusa = 'substituído: as datas foram "
                               "corrigidas' WHERE id = :i"), {"i": t_["id"]})
        svc_processo.gerar_termo(s, p, usuario)
    else:
        _evento(s, p["id"], "AVISO", {"texto": "corrigido depois de atendido: "
                                      "confira se o cadastro precisa do mesmo "
                                      "acerto"}, usuario)
    svc_contabil.comunicar_correcao(s, p, mudou, usuario)
    # O pedido de mudança da obra, se havia, foi atendido.
    if (p.get("dados") or {}).get("mudanca_pedida"):
        s.execute(text("UPDATE dp_solicitacoes SET dados = dados - 'mudanca_pedida' "
                       "WHERE id = :i"), {"i": p["id"]})
    _ = json


def papeis_do_pedido(s: Session, solicitacao_id: int, numero: str,
                     colaborador_id: Optional[int]) -> list[dict[str, Any]]:
    """Os arquivos que vieram com este pedido — onde quer que estejam agora
    (ainda no pedido, ou já no cadastro da pessoa, marcados com o número)."""
    linhas = s.execute(text("""
        SELECT d.id, d.anexo_id, d.tipo_codigo, d.nome_original,
               d.nome_padronizado, t.nome AS tipo_nome
          FROM documentos d
          LEFT JOIN documento_tipos t ON t.codigo = d.tipo_codigo
         WHERE d.dp_solicitacao_id = :s
            OR (:c IS NOT NULL AND d.colaborador_id = :c
                AND d.referencia LIKE :ref)
         ORDER BY d.id"""),
        {"s": solicitacao_id, "c": colaborador_id,
         "ref": f"{numero} %"}).mappings().all()
    return [{"id": l["id"], "anexo_id": l["anexo_id"],
             "tipo": l["tipo_nome"] or l["tipo_codigo"],
             "nome": l["nome_original"] or l["nome_padronizado"]}
            for l in linhas]


def transferir_papeis(s: Session, solicitacao_id: int, colaborador_id: int) -> int:
    """Os papéis do pedido passam a ser da pessoa (dono único: `ck_documento_um_dono`)."""
    r = s.execute(text("""
        UPDATE documentos SET colaborador_id = :c, dp_solicitacao_id = NULL
         WHERE dp_solicitacao_id = :s"""), {"c": colaborador_id, "s": solicitacao_id})
    return r.rowcount or 0


# ---------------------------------------------------------------------------
# Atender — e EXECUTAR
# ---------------------------------------------------------------------------
def atender(s: Session, solicitacao_id: int, dados: dict[str, Any],
            usuario: Usuario) -> dict[str, Any]:
    """Atende o pedido E faz o que ele pede, na mesma transação.

    É a diferença que importa em relação ao Pipefy: lá, atender era mover o card
    — e alguém, depois, ia mexer no cadastro à mão ou por um cenário do Make. O
    pedido dizia uma coisa e o cadastro ficava outra, e ninguém sabia qual valia.

    Aqui, ou as duas coisas acontecem, ou nenhuma: o efeito no cadastro usa as
    MESMAS funções da tela (`core/cadastros/pessoas.py`), com as mesmas
    conferências. Pedido que não pode ser executado é RECUSADO com o motivo, não
    atendido pela metade.
    """
    p = ler(s, solicitacao_id)
    if p["situacao"] not in A_ATENDER:
        raise ErroValidacao(
            f"{p['numero']} já está {p['situacao_rotulo'].lower()} — não dá para atender de novo.")
    regra = TIPOS[p["tipo"]]
    # O que vale é o pedido, com o que o DP corrigiu em cima. Corrigir é normal:
    # a obra pede "salário 2200" e o DP acerta no piso da convenção.
    pedido = traduzir_do_pipe(
        {**p["dados"], **{k: v for k, v in (dados.get("dados") or {}).items()
                          if v not in (None, "")}})
    efeito: dict[str, Any] = {}
    executa = regra["executa"]
    # A obra do pedido entra no efeito: é ela que o cadastro grava, e sem isso
    # a pessoa nasceria (ou voltaria) sem obra — e quem é preso a uma obra não
    # a enxergaria.
    if p.get("obra_id"):
        pedido.setdefault("obra_id", p["obra_id"])
    # ⚑ A MATRÍCULA É O CPF (19/11/2026). Ele: *"toda vida que a gente
    # cadastrar um funcionário, a gente vai utilizar a matrícula e a matrícula
    # dele vai ser o CPF dele."* O SomaPay exige matrícula. Vale para quem
    # ENTRA daqui em diante — admissão e readmissão —; ele decidiu não mexer
    # nas matrículas de quem já está cadastrado. O DP ainda pode digitar outra
    # no atendimento: o que ele digitar vence.
    # ⚑ E NA READMISSÃO (19/11/2026, ele): *"se for uma readmissão e o campo
    # estiver em branco, bota o CPF. Se (…) o campo já existia, deixa o que já
    # estava."*
    if executa == "criar_cadastro":
        from app.apps.erp.core.cadastros.validadores import somente_digitos
        cpf_do_pedido = somente_digitos(pedido.get("cpf") or p.get("pessoa_cpf") or "")
        ja_tinha = ""
        if p.get("colaborador_id"):
            ja_tinha = str(s.execute(text(
                "SELECT matricula FROM colaboradores WHERE id = :i"),
                {"i": p["colaborador_id"]}).scalar() or "").strip()
        if cpf_do_pedido and not ja_tinha \
                and not str(pedido.get("matricula") or "").strip():
            pedido["matricula"] = cpf_do_pedido
    if executa and p["colaborador_id"]:
        alvo = p["colaborador_id"]
        # ⚑ CADASTRO COM PESSOA LIGADA É READMISSÃO, e precisa estar ANTES dos
        # outros ramos: sem este, `criar_cadastro` com `colaborador_id`
        # preenchido entrava aqui, não casava com nenhum ramo, e o pedido era
        # marcado como ATENDIDO sem nada ter acontecido. Atendimento que não
        # faz nada e diz que fez é pior que erro — ninguém vai conferir.
        if executa == "criar_cadastro" and pedido.get("pre_cadastro"):
            # O pré-cadastro já existe desde o pedido: atender é completar o
            # que é do DP. Só o que veio preenchido entra — vazio não apaga.
            c_atual = s.get(Colaborador, alvo)
            do_dp = {k: pedido[k] for k in campos_do_dp_do_tipo("CADASTRO")
                     if str(pedido.get(k) or "").strip()}
            if do_dp:
                svc_pessoas.salvar(s, {**do_dp, "cpf": c_atual.cpf,
                                       "nome": c_atual.nome}, usuario)
            efeito["completou_pre_cadastro"] = True
            # Papel que tenha ficado no pedido (pedido aberto antes do
            # pré-cadastro existir) passa para a pessoa agora.
            efeito["papeis"] = transferir_papeis(s, solicitacao_id, alvo)
        elif executa == "criar_cadastro":
            svc_pessoas.readmitir(s, alvo, {
                **pedido, "admissao": pedido.get("data_inicio"),
                "vinculo": pedido.get("tipo_documentacao") or None}, usuario)
            # ⚑ OS DADOS NOVOS ATUALIZAM O CADASTRO ANTIGO (19/11/2026). Ele:
            # *"os dados novos vão estar atualizados, a gente vai estar
            # atualizando os dados, se tiver colocado algum documento."* Só o
            # que o pedido TROUXE — campo vazio no pedido não apaga o que o
            # cadastro tinha. A trilha campo a campo registra o de→para.
            novos = {c: pedido[c] for c in CAMPOS_DA_PESSOA + ("telefone",)
                     if str(pedido.get(c) or "").strip()}
            if novos:
                c_atual = s.get(Colaborador, alvo)
                svc_pessoas.salvar(s, {**novos, "cpf": c_atual.cpf,
                                       "nome": c_atual.nome}, usuario)
            efeito["readmitiu"] = True
        elif executa == "desligar":
            # ⚑ O DESLIGAMENTO DE CARTEIRA VIRA PROCESSO (19/11/2026) — ver
            # `core/desligamento_processo.py`. E quem CONTINUA como prestador
            # não é desligado: a saída da carteira fica agendada, e o DP
            # confirma depois do último dia.
            vinculo_alvo = vinculo_da_pessoa(s, alvo)
            if svc_processo.e_processo(p["tipo"], vinculo_alvo):
                efeito["processo"] = True
                pedido["processo_desligamento"] = "1"
                if svc_processo.continua(pedido):
                    svc_pessoas.agendar_saida_da_carteira(s, alvo, pedido, usuario)
                    efeito["continua_a_confirmar"] = True
                else:
                    svc_pessoas.desligar(s, alvo, pedido, usuario)
            else:
                svc_pessoas.desligar(s, alvo, pedido, usuario)
        elif executa == "readmitir":
            svc_pessoas.readmitir(s, alvo, pedido, usuario)
        elif executa == "trocar_vinculo":
            svc_pessoas.trocar_vinculo(s, alvo, {
                **pedido, "vinculo": pedido.get("vinculo") or "CTPS",
                "ctps_numero": pedido.get("num_ctps"),
                "admissao": pedido.get("admissao") or pedido.get("data_inicio")
                or str(date.today())}, usuario)
            # ⚑ 20/11/2026: virou CTPS, então o cadastro NÃO está concluído
            # até a contabilidade registrar a carteira. Antes ficava em
            # "Concluído" sem registro — o que a trava de concluir existe para
            # impedir. Volta para a Inicial e o pedido segue Em andamento.
            c_alvo = s.get(Colaborador, alvo)
            # Os números da carteira e os dados do eSocial/SomaPay que a obra
            # informou entram na ficha (só o que veio — vazio não apaga).
            novos = para_o_cadastro({k: pedido[k] for k in
                                     ("num_ctps", "num_pis") + CAMPOS_DA_PESSOA
                                     if str(pedido.get(k) or "").strip()})
            novos.pop("matricula", None)
            if novos:
                svc_pessoas.salvar(s, {**novos, "cpf": c_alvo.cpf,
                                       "nome": c_alvo.nome}, usuario)
            if (c_alvo.etapa or "").upper() not in ("CADASTRO_INICIAL",
                                                     "NA_CONTABILIDADE"):
                svc_pessoas.mover_etapa(s, alvo, "CADASTRO_INICIAL", usuario,
                                        automatico=True,
                                        motivo="passou a CTPS: falta o registro "
                                               "da carteira")
        elif executa == "alteracao":
            # ⚠️ 20/11/2026: o pedido fala em "cargo pleiteado/deferido" e
            # "data de início"; o cadastro, em "cargo" e "vigência". Sem a
            # tradução o cargo nunca mudava — e a obra do pedido (que é a de
            # hoje) virava uma "alteração de obra" de mentira.
            svc_pessoas.registrar_alteracao(s, alvo, {
                "cargo_id": pedido.get("cargo_deferido_id")
                or pedido.get("cargo_pleiteado_id"),
                "salario": pedido.get("salario"),
                "convencao_id": pedido.get("convencao_id"),
                "vigencia": pedido.get("vigencia") or pedido.get("data_inicio"),
                "motivo": pedido.get("motivo"),
                "justificativa": pedido.get("justificativa")}, usuario)
        elif executa == "cancelar_desligamento":
            efeito.update(cancelar_desligamento(s, alvo, p, usuario))
        elif executa == "transferir_obra":
            efeito.update(_transferir_obra(s, alvo, p, pedido, usuario))
        elif executa == "registrar_afastamento":
            efeito.update(_registrar_afastamento(s, alvo, pedido, p, usuario))
        elif executa == "registrar_retorno":
            efeito.update(_registrar_retorno(s, alvo, pedido, p, usuario))
        elif executa == "programar_ferias":
            efeito.update(svc_ferias.programar(s, p, pedido, usuario))
        elif executa in ("alterar_ferias", "cancelar_ferias"):
            efeito.update(svc_ferias.pedir_mudanca(
                s, p, pedido, "ALTERACAO" if executa == "alterar_ferias"
                else "CANCELAMENTO", usuario))
        elif executa == "dependente":
            # ⚑ VÁRIOS DE UMA VEZ. Ele, em 10/11/2026: *"dependente tem que
            # ser um a um, não posso adicionar outro, não, para pedir mais de
            # um."* Um pedido com três filhos cadastra os três; o formato
            # antigo (um dependente solto no corpo) continua valendo, porque
            # é o que os pedidos já abertos têm gravado.
            varios = pedido.get("dependentes")
            if isinstance(varios, list) and varios:
                for um in varios:
                    svc_pessoas.salvar_dependente(
                        s, alvo, traduzir_do_pipe(um), usuario)
                efeito["dependentes"] = len(varios)
            else:
                svc_pessoas.salvar_dependente(s, alvo, pedido, usuario)
        elif executa in ("atualizar_cadastro", "beneficios"):
            c = s.get(Colaborador, alvo)
            # ⚑ O CPF NÃO SE TROCA — é a identidade da pessoa no sistema. O
            # NOME troca (casamento, divórcio, correção na certidão), mas só
            # quando o pedido traz um: `nome` sozinho no corpo do pedido seria
            # o nome ANTIGO vindo de volta, por isso o campo tem nome próprio.
            novo_nome = " ".join(str(pedido.get("nome_novo") or "").split())
            svc_pessoas.salvar(s, {**pedido, "cpf": c.cpf,
                                   "nome": novo_nome or c.nome}, usuario)
        efeito["executou"] = executa
    elif executa == "criar_cadastro":
        c = svc_pessoas.salvar(s, para_o_cadastro(pedido), usuario)
        s.execute(text("UPDATE dp_solicitacoes SET colaborador_id = :c WHERE id = :s"),
                  {"c": c.id, "s": solicitacao_id})
        efeito["colaborador_id"] = c.id
        # ⚑ OS PAPÉIS DO PEDIDO PASSAM PARA A PESSOA que acabou de nascer, na
        # mesma transação (migração 100: "o papel nunca fica órfão").
        efeito["papeis"] = transferir_papeis(s, solicitacao_id, c.id)
    # ⚑ A ADMISSÃO SÓ FECHA COM O CADASTRO CONCLUÍDO (19/11/2026) — ver
    # `admissao_processo`. O prestador que já pode, conclui agora; o resto
    # fica Em andamento e fecha sozinho quando o cadastro chegar lá.
    # ⚑ OS QUE PASSAM PELA CONTABILIDADE (20/11/2026) — alteração de cargo,
    # dependente, férias, cancelamento de desligamento já comunicado.
    if svc_contabil.e_processo(p["tipo"]) and (
            efeito.get("precisa_contabilidade")
            or svc_contabil.aplica(s, p["tipo"], p.get("colaborador_id"), pedido)):
        efeito["processo"] = True
        pedido["processo_contabil"] = "1"
    if svc_admissao.e_processo(p["tipo"]):
        pessoa_id = efeito.get("colaborador_id") or p.get("colaborador_id")
        pedido["processo_admissao"] = "1"
        if pessoa_id and not svc_admissao.avancar_no_atendimento(
                s, int(pessoa_id), usuario):
            efeito["processo"] = True
            efeito["falta"] = "o cadastro ainda não está concluído"
    # O desligamento de carteira não fecha aqui: abre o processo.
    nova = svc_processo.EM_ANDAMENTO if efeito.get("processo") else "ATENDIDA"
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = :nova, atendido_por = :u,
                             atendido_em = now(), resposta = :r, atualizado_em = now(),
                             dados = CAST(:d AS jsonb)
                       WHERE id = :s"""),
              {"nova": nova, "u": usuario.id if usuario else None,
               "s": solicitacao_id,
               "r": (str(dados.get("resposta") or "").strip() or None),
               "d": __import__("json").dumps(pedido, ensure_ascii=False, default=str)})
    _evento(s, solicitacao_id,
            ("DESLIGAMENTO_REGISTRADO" if p["tipo"] == "DESLIGAMENTO"
             else "ATENDIDA_EM_ANDAMENTO") if efeito.get("processo") else "ATENDIDA",
            {"resposta": dados.get("resposta"), **efeito}, usuario)
    if efeito.get("processo"):
        depois_de_atender(s, {**p, "dados": pedido, "situacao": nova}, usuario)
    logger.info("ERP/DP: solicitação %s atendida por %s (%s)", p["numero"],
                usuario.nome if usuario else "?", efeito.get("executou") or "sem efeito")
    return ler(s, solicitacao_id)


def depois_de_atender(s: Session, sol: dict[str, Any],
                      usuario: Optional[Usuario]) -> None:
    """O que segue sozinho depois que o DP atende (20/11/2026, noite).

    1. O DOCUMENTO PARA A PESSOA ASSINAR: o aviso prévio (ou o pedido de
       demissão) e a autorização de desconto. Nasce AQUI, depois da análise do
       DP — ele: *"eu só não posso liberar de imediato para a obra assinar (…)
       o DP só confere e libera para a obra"*.
    2. O DESPACHO: com a rotina ligada, o pedido à contabilidade já sai
       MONTADO (rascunho) — o DP confere o texto e envia. É o *"o DP apenas
       olhe e dê o ok"*. O que impede de montar (dado faltando no cadastro)
       vira recado na trilha, não erro: atender não pode falhar por isso.
    """
    from app.apps.erp.core import agente_dp
    tipo = sol["tipo"]
    try:
        with s.begin_nested():
            if tipo == "DESLIGAMENTO":
                svc_processo.gerar_termo(s, sol, usuario)
            elif tipo == "DESCONTO_EM_FOLHA":
                gerar_autorizacao_de_desconto(s, sol, usuario)
    except ErroValidacao as e:
        _evento(s, sol["id"], "AVISO",
                {"texto": f"o documento para assinar não foi gerado: {e}"}, usuario)
    if agente_dp.modo(s, "despacho") == "DESLIGADA":
        return
    if tipo in ("DESLIGAMENTO", "DESCONTO_EM_FOLHA"):
        return          # esperam a assinatura; montam quando ela voltar
    if not (svc_contabil.e_processo(tipo) or svc_admissao.e_processo(tipo)):
        return
    if (tipo in ("CADASTRO", "ADMISSAO_PRESTADOR")
            and (vinculo_da_pessoa(s, sol.get("colaborador_id")) or "CTPS") != "CTPS"):
        return
    despachar(s, sol["id"], usuario)


def despachar(s: Session, solicitacao_id: int, usuario: Optional[Usuario]) -> Optional[str]:
    """Monta o pedido à contabilidade desta solicitação, se ainda não houver.
    Devolve o número, ou None (com o motivo na trilha)."""
    sol = ler(s, solicitacao_id)
    try:
        with s.begin_nested():
            if sol["tipo"] == "DESLIGAMENTO":
                r = svc_processo.montar_rescisao(s, sol, usuario)
            else:
                r = svc_contabil.montar_pedido(s, sol, usuario)
    except ErroValidacao as e:
        _evento(s, solicitacao_id, "AVISO",
                {"texto": f"o agente não montou o pedido à contabilidade: {e}"},
                usuario)
        return None
    if r:
        _evento(s, solicitacao_id, "DESPACHADO_PELO_AGENTE",
                {"texto": f"{r['numero']} montado — confira e envie"}, usuario)
        return r["numero"]
    return None


def termo_voltou(s: Session, termo_id: int, usuario: Optional[Usuario]) -> None:
    """O documento voltou assinado (e valendo): fica na trilha do pedido, e o
    despacho monta o pedido à contabilidade que esperava por ele."""
    from app.apps.erp.core import agente_dp, termos
    t_ = termos.ler(s, termo_id)
    if not t_.get("solicitacao_id"):
        return
    _evento(s, t_["solicitacao_id"], "DOCUMENTO_ASSINADO",
            {"numero": t_["numero"],
             "texto": termos.FORMA_ROTULOS.get(t_.get("forma") or "", "")}, usuario)
    sol = s.execute(text("SELECT id, tipo, situacao FROM dp_solicitacoes "
                         "WHERE id = :i"), {"i": t_["solicitacao_id"]}).mappings().first()
    # O aviso de férias assinado é o último passo: o pedido de férias fecha.
    if termos.concluido(t_) and sol and sol["tipo"] in svc_ferias.TIPOS_COM_AVISO:
        if sol["situacao"] == "EM_ANDAMENTO" and \
                not svc_contabil.extras_abertos(s, int(sol["id"])):
            s.execute(text("UPDATE dp_solicitacoes SET situacao = 'ATENDIDA', "
                           "atualizado_em = now() WHERE id = :i"), {"i": sol["id"]})
            _evento(s, sol["id"], "ATENDIDA",
                    {"texto": "a pessoa assinou o aviso de férias"}, usuario)
        return
    if termos.concluido(t_) and agente_dp.modo(s, "despacho") != "DESLIGADA":
        despachar(s, int(t_["solicitacao_id"]), usuario)


def papel_no_escritorio(s: Session, solicitacao_id: int, qual: str, alvo_id: int,
                        conteudo: bytes, nome_arquivo: str,
                        usuario: Usuario) -> None:
    """⚑ O PLANO B (23/11/2026): o papel assinado chegou ao ESCRITÓRIO.

    Ele: *"e se de repente aconteceu uma circunstância que a pessoa assinou
    num papel e encaminhou para cá para a gente anexar? (…) pode ser que uma
    obra (…) não tem internet (…) a gente tem que ter uma alternativa."*

    - `termo_papel`: o documento que a pessoa assina (aviso prévio, pedido de
      demissão, aviso de férias, autorização de desconto). Entra como
      "assinado no papel" e JÁ VALIDADO — quem anexa é o DP, que está olhando
      o papel — e o processo segue como se tivesse sido assinado na tela.
    - `devolucao_papel`: o que a obra devia devolver pelo portal (documentação
      rescisória, ASO). A pendência fecha e o arquivo vai para a validação do
      DP, como se tivesse chegado pelo portal.
    """
    from app.apps.erp.core import termos
    from app.apps.erp.core import portal as svc_portal
    if not conteudo:
        raise ErroValidacao("Escolha a foto ou o PDF do papel assinado.")
    if qual == "termo_papel":
        if termos.ler(s, alvo_id).get("solicitacao_id") != solicitacao_id:
            raise ErroNaoEncontrado("Documento não encontrado.")
        termos.enviar_papel(
            s, alvo_id, None, conteudo=conteudo, nome_arquivo=nome_arquivo,
            conduzido_por={"colaborador_id": None,
                           "nome": f"{usuario.nome} (DP, no escritório)"})
        termos.validar(s, alvo_id, usuario, aceitar=True)
        termo_voltou(s, alvo_id, usuario)
        return
    if qual != "devolucao_papel":
        raise ErroValidacao("Não sei o que anexar.")
    dono = s.execute(text("SELECT colaborador_id FROM dp_devolucoes WHERE id = :i"),
                     {"i": alvo_id}).scalar()
    pessoa = s.execute(text("SELECT colaborador_id FROM dp_solicitacoes WHERE id = :i"),
                       {"i": solicitacao_id}).scalar()
    if not dono or dono != pessoa:
        raise ErroNaoEncontrado("Pendência não encontrada.")
    svc_portal.devolver(s, alvo_id, conteudo, usuario, nome_arquivo,
                        pelo_escritorio=True)
    _evento(s, solicitacao_id, "AVISO",
            {"texto": "documento assinado recebido no escritório e anexado pelo DP"},
            usuario)


def gerar_autorizacao_de_desconto(s: Session, sol: dict[str, Any],
                                  usuario: Optional[Usuario]) -> Optional[dict[str, Any]]:
    """A autorização de desconto que a pessoa assina (CLT art. 462)."""
    from app.apps.erp.core import termos
    if (vinculo_da_pessoa(s, sol.get("colaborador_id")) or "CTPS") != "CTPS":
        return None
    if not termos.tem_tabela(s):
        return None
    vivo = termos.do_pedido(s, int(sol["id"]))
    if vivo is not None and vivo["situacao"] == "ABERTO":
        termos.cancelar(s, vivo["id"], "gerado de novo", usuario)
    elif vivo is not None:
        raise ErroValidacao(f"{vivo['numero']} já voltou assinado.")
    d = sol.get("dados") or {}
    c = s.get(Colaborador, int(sol["colaborador_id"]))
    parcelas = str(d.get("parcelas") or "1")
    texto_ = (f"Eu, {c.nome}, CPF {c.cpf}, AUTORIZO a empresa a descontar do meu "
              f"salário o valor total de R$ {d.get('valor_total') or '?'}, em "
              f"{parcelas} parcela(s)"
              + (f" de R$ {d.get('valor_parcela')}" if d.get("valor_parcela") else "")
              + (f", a partir de {d.get('primeira_competencia')}"
                 if d.get("primeira_competencia") else "")
              + f".\n\nReferente a: {d.get('observacao') or '(não informado)'}."
              + "\n\nSe o contrato terminar antes do fim das parcelas, o saldo "
                "pode ser descontado na rescisão, até o limite da lei "
                "(CLT art. 477 §5º).")
    r = termos.abrir(s, colaborador_id=c.id, tipo="AUTORIZACAO-DESCONTO",
                     titulo="Autorização de desconto em folha", texto_=texto_,
                     solicitacao_id=int(sol["id"]), obra_id=sol.get("obra_id"),
                     dias=3, usuario=usuario)
    _evento(s, sol["id"], "AUTORIZACAO_PARA_ASSINAR", {"numero": r["numero"]}, usuario)
    return r


def pedir_mudanca(s: Session, solicitacao_id: int, dados: dict[str, Any],
                  usuario: Optional[Usuario]) -> dict[str, Any]:
    """Quem pediu quer CANCELAR ou MUDAR um pedido que o DP já está tocando.

    Ele: *"praticamente tudo que se pede pode vir a ser necessário ser
    cancelado (…) ou alterado alguma coisa."* A obra não mexe no que já foi à
    contabilidade: ela AVISA, o DP decide e faz (corrigir ou cancelar), e o
    agente cobra até ele decidir.
    """
    import json
    p = ler(s, solicitacao_id)
    if p["situacao"] in A_ATENDER:
        raise ErroValidacao("O DP ainda não atendeu: cancele o pedido ou "
                            "abra outro com o certo.")
    if p["situacao"] not in ("EM_ANDAMENTO", "ATENDIDA"):
        raise ErroValidacao(f"{p['numero']} já está {p['situacao_rotulo'].lower()}.")
    oque = str(dados.get("o_que") or "").strip().lower()
    if oque not in ("cancelar", "corrigir"):
        raise ErroValidacao("Diga se é para cancelar ou para corrigir.")
    texto_ = " ".join(str(dados.get("texto") or "").split())
    if len(texto_) < 5:
        raise ErroValidacao("Conte o que mudou — o DP precisa saber.")
    s.execute(text("UPDATE dp_solicitacoes SET dados = dados || CAST(:j AS jsonb), "
                   "atualizado_em = now() WHERE id = :i"),
              {"i": solicitacao_id, "j": json.dumps({"mudanca_pedida": {
                  "o_que": oque, "texto": texto_,
                  "por": usuario.nome if usuario else "",
                  "em": date.today().isoformat()}}, ensure_ascii=False)})
    _evento(s, solicitacao_id, "MUDANCA_PEDIDA",
            {"o_que": oque, "texto": texto_}, usuario)
    return ler(s, solicitacao_id)


def recusar(s: Session, solicitacao_id: int, dados: dict[str, Any],
            usuario: Usuario) -> dict[str, Any]:
    p = ler(s, solicitacao_id)
    if p["situacao"] not in A_ATENDER:
        raise ErroValidacao(f"{p['numero']} já está {p['situacao_rotulo'].lower()}.")
    motivo = str(dados.get("resposta") or dados.get("motivo") or "").strip()
    if len(motivo) < 5:
        # O pipe tinha o campo "Por quê?" obrigatório na recusa, e com razão:
        # quem pediu precisa saber o que corrigir para pedir de novo.
        raise ErroValidacao("Diga por que o pedido foi recusado — quem pediu precisa saber.")
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'RECUSADA', atendido_por = :u, atendido_em = now(),
                             resposta = :r, atualizado_em = now()
                       WHERE id = :s"""),
              {"u": usuario.id if usuario else None, "s": solicitacao_id, "r": motivo})
    _evento(s, solicitacao_id, "RECUSADA", {"motivo": motivo}, usuario)
    _cancelar_pre_cadastro(s, p, f"pedido {p['numero']} recusado: {motivo}", usuario)
    return ler(s, solicitacao_id)


def _cancelar_pre_cadastro(s: Session, p: dict[str, Any], motivo: str,
                           usuario: Optional[Usuario]) -> None:
    """O pré-cadastro de um pedido que não vai adiante vai para "Cancelado" —
    o cadastro que nunca virou gente na empresa (`pessoas.ETAPAS`)."""
    if not (p.get("dados") or {}).get("pre_cadastro") or not p.get("colaborador_id"):
        return
    try:
        svc_pessoas.mover_etapa(s, int(p["colaborador_id"]), "CANCELADO",
                                usuario, automatico=True, motivo=motivo[:300])
    except ErroValidacao as e:
        _evento(s, p["id"], "AVISO",
                {"texto": f"o pré-cadastro não foi para Cancelado: {e}"}, usuario)


def cancelar(s: Session, solicitacao_id: int, dados: dict[str, Any],
             usuario: Usuario) -> dict[str, Any]:
    """Quem abriu cancela o próprio pedido.

    No pipe, cancelar era ABRIR OUTRO CARD ("Cancelar Solicitação de
    Desligamento"). Aqui é ação sobre o próprio pedido — mas ele não desaparece:
    pedido apagado deixa "e o que aconteceu com aquele desligamento?" sem
    resposta.
    """
    p = ler(s, solicitacao_id)
    if p["situacao"] not in A_ATENDER:
        raise ErroValidacao(
            f"{p['numero']} já está {p['situacao_rotulo'].lower()} — não dá para cancelar."
            + (" A saída já foi registrada: para desfazê-la, abra um pedido de "
               "cancelamento de desligamento." if p["situacao"] == "EM_ANDAMENTO"
               else ""))
    motivo = str(dados.get("motivo") or "").strip()
    if len(motivo) < 5:
        raise ErroValidacao("Diga por que está cancelando.")
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'CANCELADA', cancelado_por = :u,
                             cancelado_em = now(), motivo_cancelamento = :m,
                             atualizado_em = now()
                       WHERE id = :s"""),
              {"u": usuario.id if usuario else None, "s": solicitacao_id, "m": motivo})
    _evento(s, solicitacao_id, "CANCELADA", {"motivo": motivo}, usuario)
    _cancelar_pre_cadastro(s, p, f"pedido {p['numero']} cancelado: {motivo}", usuario)
    return ler(s, solicitacao_id)


def marcar_item(s: Session, solicitacao_id: int, item_id: int, feito: bool,
                usuario: Usuario) -> dict[str, Any]:
    linha = s.execute(text("""SELECT descricao FROM dp_solicitacao_itens
                               WHERE id = :i AND solicitacao_id = :s"""),
                      {"i": item_id, "s": solicitacao_id}).first()
    if linha is None:
        raise ErroValidacao("Item do checklist não encontrado.")
    s.execute(text("""UPDATE dp_solicitacao_itens
                         SET feito = :f, feito_por = :u,
                             feito_em = CASE WHEN :f THEN now() ELSE NULL END
                       WHERE id = :i"""),
              {"f": bool(feito), "u": usuario.id if usuario else None, "i": item_id})
    s.execute(text("""UPDATE dp_solicitacoes SET situacao = 'EM_ANALISE',
                             atualizado_em = now()
                       WHERE id = :s AND situacao = 'ABERTA'"""), {"s": solicitacao_id})
    _evento(s, solicitacao_id, "ITEM_FEITO" if feito else "ITEM_DESMARCADO",
            {"item": linha[0]}, usuario)
    return ler(s, solicitacao_id)


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
SITUACAO_ROTULOS = {"ABERTA": "Aberta", "EM_ANALISE": "Em análise",
                    "EM_ANDAMENTO": "Em andamento",
                    "ATENDIDA": "Atendida", "RECUSADA": "Recusada",
                    "CANCELADA": "Cancelada"}


def ler(s: Session, solicitacao_id: int) -> dict[str, Any]:
    """O pedido inteiro: o que foi pedido, o checklist e a trilha.

    ⚠️ A TRILHA ORDENA POR `criado_em, id`, e o `id` não é enfeite: em Postgres
    `now()` é o **início da transação**, então dois eventos gravados no mesmo
    pedido (abrir e atender, num teste; marcar dois itens, na vida real) têm
    timestamp IDÊNTICO — e aí a ordem fica indefinida. O histórico mostrava
    "atendida" antes de "aberta". Pego por teste.
    """
    linha = s.execute(text("""
        SELECT d.*, c.nome AS pessoa, c.cpf AS pessoa_cpf, c.vinculo AS pessoa_vinculo,
               o.codigo AS obra,
               COALESCE(u.nome, sc.nome, d.requerente) AS solicitante,
               a.nome AS atendente
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN colaboradores sc ON sc.id = d.solicitante_colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN usuarios u ON u.id = d.solicitante_id
          LEFT JOIN usuarios a ON a.id = d.atendido_por
         WHERE d.id = :s"""), {"s": solicitacao_id}).mappings().first()
    if linha is None:
        raise ErroValidacao("Solicitação não encontrada.")
    d = dict(linha)
    regra = TIPOS.get(d["tipo"], {})
    # ⚑ O PEDIDO CONVERTIDO DIZ O QUE ELE É. Um CADASTRO cujo CPF já existia é
    # uma readmissão, e chamá-lo de "Cadastro de Colaborador" na fila faria o
    # DP abrir esperando criar ficha nova.
    rotulo_tipo = regra.get("nome", d["tipo"])
    if d["tipo"] == "CADASTRO" and (d["dados"] or {}).get("readmissao"):
        rotulo_tipo = "Readmissão de Colaborador"
    itens = s.execute(text("""SELECT i.id, i.ordem, i.descricao, i.feito, i.feito_em,
                                     u.nome AS por
                                FROM dp_solicitacao_itens i
                                LEFT JOIN usuarios u ON u.id = i.feito_por
                               WHERE i.solicitacao_id = :s ORDER BY i.ordem"""),
                      {"s": solicitacao_id}).mappings().all()
    eventos = s.execute(text("""SELECT e.acao, e.detalhe, e.criado_em, u.nome AS por
                                  FROM dp_solicitacao_eventos e
                                  LEFT JOIN usuarios u ON u.id = e.usuario_id
                                 WHERE e.solicitacao_id = :s
                                 ORDER BY e.criado_em, e.id"""),
                        {"s": solicitacao_id}).mappings().all()
    saida = {
        "id": d["id"], "numero": d["numero"], "tipo": d["tipo"],
        "tipo_rotulo": rotulo_tipo,
        "readmissao": bool((d["dados"] or {}).get("readmissao")),
        "situacao": d["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(d["situacao"], d["situacao"]),
        "colaborador_id": d["colaborador_id"],
        "pessoa": d["pessoa"] or d["nome_informado"] or "",
        # ⚑ O VÍNCULO VIAJA COM O PEDIDO. A tela do DP usa para saber quais
        # campos são dele: no desligamento, a data de saída de quem tem
        # carteira é conta do DP; a do prestador veio da obra.
        "pessoa_vinculo": (d["pessoa_vinculo"] or "") if d["pessoa_vinculo"] else "",
        "campos_do_dp": list(campos_do_dp_do_tipo(
            d["tipo"], d["pessoa_vinculo"])) if d["tipo"] in TIPOS else [],
        "pessoa_cpf": d["pessoa_cpf"] or d["cpf_informado"] or "",
        "obra": d["obra"] or "", "obra_id": d["obra_id"],
        "solicitante": d["solicitante"] or "", "requerente": d["requerente"] or "",
        "dados": d["dados"] or {},
        "data_referencia": d["data_referencia"].isoformat() if d["data_referencia"] else None,
        "valor": float(d["valor"]) if d["valor"] is not None else None,
        "atendente": d["atendente"] or "", "resposta": d["resposta"] or "",
        "atendido_em": d["atendido_em"].isoformat() if d["atendido_em"] else None,
        "motivo_cancelamento": d["motivo_cancelamento"] or "",
        "criado_em": d["criado_em"].isoformat(),
        "aberta": d["situacao"] in ABERTAS,
        # ⚑ OS PAPÉIS QUE VIERAM COM O PEDIDO (19/11/2026). Ele: *"conferir se
        # a documentação veio completa, mas cadê a documentação? A gente não
        # consegue visualizar ela."*
        "papeis": papeis_do_pedido(s, d["id"], d["numero"], d["colaborador_id"]),
        "pre_cadastro": bool((d["dados"] or {}).get("pre_cadastro")),
        "dados_legiveis": dados_legiveis(s, d["tipo"], d["dados"] or {}),
        # Atender/recusar/cancelar: só antes de a saída ser registrada.
        "a_atender": d["situacao"] in A_ATENDER,
        "processo": (svc_processo.passos(s, {**d, "dados": d["dados"] or {}})
                     if d["tipo"] == "DESLIGAMENTO"
                     and (d["situacao"] == svc_processo.EM_ANDAMENTO
                          or (d["dados"] or {}).get("processo_desligamento"))
                     else svc_admissao.passos(s, {**d, "dados": d["dados"] or {}})
                     if svc_admissao.e_processo(d["tipo"])
                     and (d["situacao"] == svc_admissao.EM_ANDAMENTO
                          or (d["dados"] or {}).get("processo_admissao"))
                     else svc_contabil.passos(s, {**d, "dados": d["dados"] or {}})
                     if svc_contabil.e_processo(d["tipo"])
                     and (d["situacao"] == svc_contabil.EM_ANDAMENTO
                          or (d["dados"] or {}).get("processo_contabil"))
                     else None),
        # ⚑ A ANÁLISE DO AGENTE (20/11/2026, noite) — `core/dp_analise.py`.
        "analise": svc_analise.analisar(s, {**d, "dados": d["dados"] or {}}),
        "mudanca_pedida": (d["dados"] or {}).get("mudanca_pedida"),
        "cancelando": (d["dados"] or {}).get("cancelando"),
        "itens": [{"id": i["id"], "descricao": i["descricao"], "feito": i["feito"],
                   "por": i["por"] or "",
                   "em": i["feito_em"].isoformat() if i["feito_em"] else None}
                  for i in itens],
        "feitos": sum(1 for i in itens if i["feito"]), "total_itens": len(itens),
        "eventos": [{"acao": e["acao"], "detalhe": e["detalhe"] or {},
                     "por": e["por"] or "", "em": e["criado_em"].isoformat()}
                    for e in eventos],
    }
    # ⚑ A CORREÇÃO E O CANCELAMENTO COMUNICADOS À CONTABILIDADE entram como
    # passos de qualquer processo — e sem processo, viram um (20/11/2026).
    extras = svc_contabil.passos_extras(s, d["id"])
    if extras:
        pr = saida["processo"] or {"passos": [], "cumpridos": 0, "total": 0,
                                   "pode_encerrar": False, "fecha_sozinho": True,
                                   "proximo": "", "marcacoes_disponiveis": True}
        pr["passos"] = list(pr["passos"]) + extras
        pr["total"] = len(pr["passos"])
        pr["cumpridos"] = sum(1 for x in pr["passos"]
                              if x["estado"] in ("FEITO", "NAO_SE_APLICA"))
        abertos = [x for x in pr["passos"]
                   if x["estado"] not in ("FEITO", "NAO_SE_APLICA")]
        pr["proximo"] = abertos[0]["rotulo"] if abertos else ""
        pr["pode_encerrar"] = pr["pode_encerrar"] and not any(
            x in abertos for x in extras)
        saida["processo"] = pr
    return saida


def listar(s: Session, *, situacao: str = "", tipo: str = "",
           colaborador_id: Optional[int] = None, obra_ids: Optional[list[int]] = None,
           usuario_id: Optional[int] = None, limite: int = 300) -> dict[str, Any]:
    """A fila. `obra_ids` é o recorte de quem só alcança algumas obras.

    `usuario_id` é indispensável quando `obra_ids` vem preenchido: o filtro
    inclui "ou foi você quem abriu", e sem o id a consulta estoura por parâmetro
    não preenchido. (Defeito pego pelos testes antes de chegar à tela.)
    """
    onde, params = ["1 = 1"], {"limite": limite, "eu": usuario_id}
    if situacao == "ABERTAS":
        onde.append("d.situacao IN ('ABERTA', 'EM_ANALISE', 'EM_ANDAMENTO')")
    elif situacao:
        onde.append("d.situacao = :situacao")
        params["situacao"] = situacao
    if tipo:
        onde.append("d.tipo = :tipo")
        params["tipo"] = tipo
    if colaborador_id:
        onde.append("d.colaborador_id = :cid")
        params["cid"] = colaborador_id
    # O ESCOPO POR OBRA: quem é preso a obra vê o que é da obra dele, mais o que
    # ele mesmo abriu (pedido de cadastro ainda não tem obra ligada).
    if obra_ids is not None:
        if obra_ids:
            onde.append("(d.obra_id = ANY(:obras) OR d.solicitante_id = :eu)")
            params["obras"] = obra_ids
        else:
            # Quem não alcança obra nenhuma vê só o que abriu. Sem `usuario_id`
            # não veria NADA, que é o certo: recorte sem dono não libera nada.
            onde.append("d.solicitante_id IS NOT DISTINCT FROM :eu" if usuario_id
                        else "FALSE")
    linhas = s.execute(text(f"""
        SELECT d.id, d.numero, d.tipo, d.situacao, d.colaborador_id, d.obra_id,
               d.data_referencia, d.valor, d.criado_em, d.atendido_em,
               COALESCE(c.nome, d.nome_informado) AS pessoa,
               COALESCE(c.cpf, d.cpf_informado) AS pessoa_cpf,
               COALESCE(gc.nome, gp.nome) AS cargo,
               o.codigo AS obra,
               -- ⚑ QUEM PEDIU (19/11/2026): pelo portal o autor é o
               -- COLABORADOR (o usuário ficava vazio), e a coluna saía em
               -- branco para quem olhava — inclusive o DP.
               COALESCE(u.nome, sc.nome, d.requerente) AS solicitante,
               (SELECT count(*) FROM dp_solicitacao_itens i
                 WHERE i.solicitacao_id = d.id) AS total_itens,
               (SELECT count(*) FROM dp_solicitacao_itens i
                 WHERE i.solicitacao_id = d.id AND i.feito) AS feitos
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN colaboradores sc ON sc.id = d.solicitante_colaborador_id
          LEFT JOIN cargos gc ON gc.id = c.cargo_id
          LEFT JOIN cargos gp ON gp.id::text = (d.dados ->> 'cargo_pleiteado_id')
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN usuarios u ON u.id = d.solicitante_id
         WHERE {' AND '.join(onde)}
         ORDER BY (d.situacao IN ('ABERTA', 'EM_ANALISE', 'EM_ANDAMENTO')) DESC,
                  d.criado_em DESC
         LIMIT :limite"""), params).mappings().all()
    fila = [{
        "id": l["id"], "numero": l["numero"], "tipo": l["tipo"],
        "tipo_rotulo": TIPOS.get(l["tipo"], {}).get("nome", l["tipo"]),
        "situacao": l["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(l["situacao"], l["situacao"]),
        "pessoa": l["pessoa"] or "", "obra": l["obra"] or "",
        "pessoa_cpf": _formatar("cpf", l["pessoa_cpf"], s) if l["pessoa_cpf"] else "",
        "cargo": l["cargo"] or "",
        "solicitante": l["solicitante"] or "",
        "data_referencia": l["data_referencia"].isoformat() if l["data_referencia"] else None,
        "valor": float(l["valor"]) if l["valor"] is not None else None,
        "criado_em": l["criado_em"].isoformat(),
        "aberta": l["situacao"] in ABERTAS,
        "feitos": int(l["feitos"]), "total_itens": int(l["total_itens"]),
    } for l in linhas]
    return {
        "solicitacoes": fila,
        "indicadores": {
            "abertas": sum(1 for x in fila if x["aberta"]),
            "atendidas": sum(1 for x in fila if x["situacao"] == "ATENDIDA"),
            "recusadas": sum(1 for x in fila if x["situacao"] == "RECUSADA"),
            # O que já tem checklist começado e não terminou: é o que trava o DP.
            "em_andamento": sum(1 for x in fila
                                if x["aberta"] and 0 < x["feitos"] < x["total_itens"]),
        },
        "tipos": tipos_para_tela(s),
        # ⚑ AS EMPRESAS DO GRUPO, direto do cadastro de Empresas — TODAS as
        # ativas, inclusive a que está sem CNPJ (ela aparece dizendo que
        # está). Ver `empresas_para_escolher`: o erro antigo era o CAMPO pedir
        # um CNPJ, e não a lista vir do lugar errado.
        "empresas": empresas_para_escolher(s),
        # Quem foi desligado dizendo que continua, e não foi readmitido: o
        # ponto não aceita essa pessoa amanhã (15/11/2026). Só pega os pedidos
        # atendidos ANTES de 19/11/2026 — os de agora não desligam quem
        # continua (ver `desligamento_processo`).
        # ⚑ Recortado pela obra desde 24/11/2026: antes ia a lista da empresa
        # inteira para qualquer um que abrisse a tela.
        "readmissoes_pendentes": [
            x for x in readmissoes_esquecidas(s)
            if obra_ids is None or x.get("obra_id") in set(obra_ids)
            or (usuario_id and x.get("solicitante_id") == usuario_id)],
        # ⚑ Os desligamentos em andamento, com o próximo passo de cada um.
        # Recortado pelo que esta pessoa enxerga na fila: quem é preso a obra
        # não vê o desligamento da obra dos outros por aqui.
        "desligamentos_em_andamento": [
            x for x in svc_processo.em_andamento(s)
            if obra_ids is None or x["id"] in {f["id"] for f in fila}],
        "situacoes": [{"valor": c, "rotulo": r} for c, r in SITUACAO_ROTULOS.items()],
    }
