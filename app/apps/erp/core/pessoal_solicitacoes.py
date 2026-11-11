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

from app.apps.erp.core.cadastros import pessoas as svc_pessoas
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
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
        "campos": ("forma_desligamento", "aviso_previo", "observacao"),
        "obrigatorios": ("forma_desligamento", "aviso_previo"),
        "campos_do_dp": ("ultimo_dia", "data_saida", "faltas_a_descontar"),
        # Os ONZE itens do checklist do pipe, na ordem em que o DP faz.
        "checklist": (
            "Solicitação de aviso prévio ou encerramento de contrato",
            "Lançamento de faltas",
            "Recebimento da documentação rescisória",
            "Lançar a despesa do TRCT",
            "Lançar a SP da GRRF",
            "Impressão do TRCT",
            "Impressão da folha de ponto",
            "Malote físico ou digital",
            "Pagamento do TRCT",
            "Desativação no sistema de pagamento (após o pagamento)",
            "Desativação no sistema de ponto",
        ),
    },
    # ⚑ O PIPE TEM DOIS DESLIGAMENTOS, e eles pedem coisas diferentes: o do
    # prestador não tem aviso prévio (não há), tem DATA DE SAÍDA. Eu tinha
    # juntado os dois num só e pedido os campos dos dois ao mesmo tempo.
    "DESLIGAMENTO_PRESTADOR": {
        "nome": "Desligamento de Prestador de Serviço",
        "explicacao": "encerra a prestação de serviço",
        "exige_pessoa": True, "executa": "desligar",
        "campos": ("data_saida", "observacao"),
        "obrigatorios": ("data_saida",),
        "campos_do_dp": (),
        "checklist": ("Conferir o período prestado",
                      "Encerrar no sistema de pagamento",
                      "Desativação no sistema de ponto"),
    },
    "CANCELAR_DESLIGAMENTO": {
        "nome": "Cancelar Solicitação de Desligamento",
        "explicacao": "desfaz o pedido de desligamento antes de ele ser feito",
        "exige_pessoa": True, "executa": None,
        "campos": ("observacao",),
        "obrigatorios": (),
        "campos_do_dp": (),
        "checklist": ("Conferir se o desligamento ainda não foi processado",
                      "Cancelar o pedido"),
    },
    "READMISSAO": {
        "nome": "Readmissão de Colaborador",
        "explicacao": "traz a pessoa de volta sem refazer o cadastro",
        "exige_pessoa": True, "executa": "readmitir",
        # ⚑ "Tipo de Readmissão" é o nome no Pipe — eu tinha escrito "tipo de
        # contrato", que não existe em lugar nenhum. E saíram daqui convenção,
        # salário e matrícula: ele, com todas as letras, *"convenção coletiva,
        # é DP quem decide"* e *"matrícula, não é o funcionário que decide"*.
        "campos": ("data_inicio", "tipo_readmissao", "cargo_pleiteado_id",
                   "cnpj_admissao", "observacao"),
        "obrigatorios": ("data_inicio", "tipo_readmissao", "cargo_pleiteado_id",
                         "cnpj_admissao"),
        "campos_do_dp": ("cargo_id", "convencao_id", "salario", "matricula"),
        "checklist": ("Atualizar a data de admissão", "Conferir cargo e convenção",
                      "Conferir a obra", "Atualizar a matrícula",
                      "Reativar no sistema de pagamento"),
    },
    "ADMISSAO_PRESTADOR": {
        # Pipe: "Admissão de Prestador de Serviço". Eu tinha chamado de
        # "Prestador que passa a ter carteira" — inventado.
        "nome": "Admissão de Prestador de Serviço",
        "explicacao": "o prestador passa a ter registro em carteira",
        "exige_pessoa": True, "executa": "trocar_vinculo",
        # ⚑ SÓ SE PEDE O QUE O CADASTRO NÃO TEM. Ele, em 10/11/2026:
        # *"admissão de prestador de serviço, vamos requerer os dados. Se não
        # tivermos, né? Acho que é interessante uma consulta, ver se tem
        # CTPS, se tem PIS."*
        #
        # A pessoa JÁ EXISTE neste pedido (é um prestador virando CTPS), e o
        # cadastro dela pode já ter os dois números. `ja_no_cadastro` diz de
        # qual coluna cada campo vem; quando ela está preenchida, o campo sai
        # da exigência e aparece só para conferir.
        "campos": ("num_ctps", "num_pis", "salario_familia", "observacao"),
        "obrigatorios": ("num_ctps", "num_pis", "salario_familia"),
        "ja_no_cadastro": {"num_ctps": "ctps_numero", "num_pis": "pis"},
        "campos_do_dp": ("cargo_id", "salario", "matricula", "convencao_id"),
        "documentos": ("CTPS-FOTO", "CTPS-INFORMACOES"),
        # Mesma regra do cadastro: o número sai da foto da carteira que este
        # pedido JÁ exige. Ninguém transcreve onze dígitos à mão.
        "vem_do_documento": {"num_ctps": "CTPS-INFORMACOES",
                             "num_pis": "CTPS-INFORMACOES"},
        "checklist": ("Conferir a documentação de admissão",
                      "Registrar na carteira", "Cadastro no sistema de pagamento"),
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
        "checklist": ("Conferir o piso da convenção", "Registrar a alteração",
                      "Atualizar no sistema de pagamento"),
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
        "checklist": ("Conferir o documento que comprova", "Atualizar o cadastro",
                      "Atualizar no sistema de pagamento"),
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
        "checklist": ("Receber a certidão", "Cadastrar o dependente",
                      "Solicitar o salário-família"),
    },
    "AJUDA_DE_CUSTO": {
        "nome": "Solicitação de Ajuda de Custo",
        "explicacao": "auxílio alimentação e auxílio transporte",
        "exige_pessoa": True, "executa": "beneficios",
        # ⚑ OS RÓTULOS E AS OPÇÕES SÃO OS DO PIPE. Ele: *"pagar alimentação,
        # aí segunda, quinta, esse termo está errado. Como é que estava no
        # Pipe? Para usar igual."* São "Categoria Auxílio Alimentação" e
        # "Categoria Auxílio Transporte", e as opções estão em CATEGORIAS.
        "campos": ("solicitacao", "categoria_alimentacao", "categoria_transporte",
                   "observacao"),
        "obrigatorios": ("solicitacao",),
        "campos_do_dp": ("aux_alimentacao", "aux_transporte"),
        "checklist": ("Conferir o valor com a obra", "Atualizar o cadastro"),
    },
    "VALE_TRANSPORTE": {
        "nome": "Solicitação de Vale Transporte",
        "explicacao": "conceder ou cancelar o vale-transporte",
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
        "checklist": ("Conferir a autorização do colaborador",
                      "Lançar no sistema de pagamento"),
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
        "nome": "Cadastro de Colaborador",
        "explicacao": "a obra abre com os dados E os documentos; o DP confere",
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
        "campos": ("nome", "cpf", "tipo_documentacao", "cnpj_admissao",
                   "num_ctps", "num_pis",
                   "data_inicio", "cargo_pleiteado_id", "pagamento",
                   "valor_diaria", "valor_mes", "telefone", "meio_transporte",
                   "observacao"),
        "obrigatorios": ("nome", "cpf", "tipo_documentacao", "cnpj_admissao",
                         "data_inicio", "cargo_pleiteado_id", "pagamento"),
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
        "obrigatorios_se": {"tipo_documentacao": {
            "CTPS": ("num_ctps", "num_pis")}},
        "vem_do_documento": {
            "num_ctps": "CTPS-INFORMACOES",
            "num_pis": "CTPS-INFORMACOES",
            "nome": "DOC-IDENTIDADE",
            "cpf": "DOC-IDENTIDADE",
        },
        # ⚑ Matrícula e convenção são do DP — estão em FASE no Pipe, não no
        # formulário. Os auxílios também.
        "campos_do_dp": ("matricula", "convencao_id", "cargo_id", "salario",
                         "categoria_alimentacao", "aux_alimentacao",
                         "categoria_transporte", "aux_transporte"),
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
    "tipo_readmissao": ("Prestador de Serviço", "CTPS"),
    "tipo_documentacao": ("Prestador de Serviço", "CTPS"),
    "cnpj_admissao": ("00.079.526/0001-09 - MATRIZ/CE",
                      "50.422.792/0001-09 - INFRADENDE",
                      "20.198.694/0001-20 - C3"),
    "parentesco": ("Filho", "Cônjuge"),
    "pagamento": ("Diarista", "Mensalista"),
    "meio_transporte": ("Nenhum", "Transporte Coletivo", "Moto", "Bicicleta"),
    "categoria_alimentacao": ("Segunda à Quinta", "Segunda à Sexta",
                              "Mensal", "Mês"),
    # ⚑ SEM "Cartão": ele existe na FASE do cadastro (o DP pode marcar), mas
    # não no formulário de quem pede — e a lista do pedido é a do formulário.
    # Oferecer no pedido uma opção que o Pipe só dá ao DP é recriar aqui uma
    # escolha que lá já foi tirada de quem abre.
    "categoria_transporte": ("Diário", "Mensal", "Segunda e Sexta",
                             "Vale Transporte", "Diário e Vale Transporte",
                             "Veículo", "Motocicleta", "Bicicleta"),
    "pix_tipo": ("E-mail", "Telefone", "CPF", "Aleatória"),
    "solicitacao": ("Solicitar ou Atualizar", "Cancelar"),
    "conceder": ("Sim", "Não"),
    "salario_familia": ("Sim", "Não"),
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
    "tipo_readmissao": {"Prestador de Serviço": "PRESTADOR", "CTPS": "CTPS"},
    "tipo_documentacao": {"Prestador de Serviço": "PRESTADOR", "CTPS": "CTPS"},
    "parentesco": {"Filho": "FILHO", "Cônjuge": "CONJUGE"},
    "pagamento": {"Diarista": "DIARISTA", "Mensalista": "MENSALISTA"},
    "salario_familia": {"Sim": "1", "Não": "0"},
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
}


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
    for regra in TIPOS.values()
    for coluna in (regra.get("ja_no_cadastro") or {}).values()
}))
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
               ja_tem: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """O que falta para este pedido poder sair — campos E papéis.

    A tela chama isto a cada mudança, para a lista ir se marcando sozinha
    enquanto a pessoa preenche. Assim o bloqueio não é uma parede no fim: é
    uma lista que a pessoa vê desde o começo.
    """
    regra = TIPOS[tipo]
    p = traduzir_do_pipe(pedido or {})
    campos = tuple(regra.get("obrigatorios", ())) + _condicionais(
        regra, "obrigatorios_se", p)
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
    "forma_desligamento": "Forma de Desligamento",
    "aviso_previo": "Data do Aviso Prévio",
    "data_saida": "Data de Saída",
    "data_inicio": "Data de Início",
    "tipo_readmissao": "Tipo de Readmissão",
    "tipo_documentacao": "Tipo de Documentação para Cadastro",
    "cargo_pleiteado_id": "Cargo Pleiteado",
    "cargo_deferido_id": "Cargo Deferido",
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
    "categoria_alimentacao": "Categoria Auxílio Alimentação",
    "categoria_transporte": "Categoria Auxílio Transporte",
    "aux_alimentacao": "Valor (Auxílio Alimentação)",
    "aux_transporte": "Valor (Auxílio Transporte)",
    "solicitacao": "Solicitação",
    "conceder": "Conceder Vale Transporte?",
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

SITUACOES = ("ABERTA", "EM_ANALISE", "ATENDIDA", "RECUSADA", "CANCELADA")
ABERTAS = ("ABERTA", "EM_ANALISE")


def tipos_para_tela() -> list[dict[str, Any]]:
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
        campos = list(d["campos"])
        if "obra_id" not in campos:
            campos.insert(0, "obra_id")
        # ⚑ O ASTERISCO VEM DAQUI, e agora ele é de verdade. Ele, em
        # 10/11/2026: *"Pipe, todo campo obrigatório, ele tem um asterisco,
        # facilita. Aqui não, ninguém sabe o que é obrigatório, o que é que
        # não é."* A lista é a do Pipe, tipo por tipo.
        obrigatorios = ["obra_id"] + [x for x in d.get("obrigatorios", ())
                                      if x != "obra_id"]
        proprios = d.get("rotulos_proprios") or {}
        saida.append({"valor": c, "rotulo": d["nome"],
                      "explicacao": d["explicacao"],
                      "exige_pessoa": d["exige_pessoa"], "campos": campos,
                      "obrigatorios": obrigatorios,
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
                      "opcoes": {x: list(CATEGORIAS[x]) for x in campos
                                 if x in CATEGORIAS},
                      # O mesmo, para os campos que o DP preenche ao atender:
                      # a tela dele lê daqui, e não de uma segunda lista.
                      "rotulos_do_dp": {
                          x: proprios.get(x, ROTULOS_DO_DP.get(x, ROTULOS.get(x, x)))
                          for x in d.get("campos_do_dp", ())},
                      "opcoes_do_dp": {x: list(CATEGORIAS[x])
                                       for x in d.get("campos_do_dp", ())
                                       if x in CATEGORIAS}})
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

    # O PEDIDO SÓ GUARDA OS CAMPOS DO TIPO. Sem esta peneira, o formulário de um
    # tipo deixaria lixo do outro no JSONB — e foi assim que o pipe terminou com
    # 106 campos e metade vazia em cada card.
    pedido = traduzir_do_pipe(
        {c: dados.get(c) for c in regra["campos"] if dados.get(c) not in (None, "")})

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
    exige = exigencias(tipo, pedido, ja_tem)
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
        faltando = [ROTULOS.get(c, c) for c in exige["campos"]
                    if c != "dependentes" and not pedido.get(c)]
    # ⚑ A OBRA SAI DESTA CONTA, e tem conferência PRÓPRIA logo abaixo: ela
    # pode vir do pedido OU do cadastro da pessoa, e exigi-la aqui recusaria
    # o caso comum (pedido da pessoa, que já tem obra) antes de olhar o
    # cadastro.
    faltando = [x for x in faltando if x != ROTULOS["obra_id"]]
    if faltando:
        raise ErroValidacao("Falta preencher: " + ", ".join(faltando) + ".")

    # ⚑ E OS PAPÉIS. `documentos` vem da tela: os códigos dos tipos que já
    # foram anexados ao pedido (o arquivo em si já subiu e está esperando).
    anexados = {str(x).strip().upper() for x in (dados.get("documentos") or [])}
    sem_papel = [d["rotulo"] for d in exige["documentos"]
                 if d["codigo"] not in anexados]
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
    obra_do_pedido = (int(dados["obra_id"]) if dados.get("obra_id")
                      else (pessoa.obra_id if pessoa else None))
    if not obra_do_pedido:
        raise ErroValidacao(
            "Informe a obra. Sem ela não dá para saber de onde vem a pessoa "
            "nem para onde ela vai trabalhar.")
    if tipo == "CADASTRO":
        from app.apps.erp.core.cadastros.validadores import cpf_valido, somente_digitos
        cpf = somente_digitos(pedido.get("cpf") or "")
        if not cpf_valido(cpf):
            raise ErroValidacao("CPF inválido.")
        ja = svc_pessoas.conferir_cpf(s, cpf)
        if ja["situacao"] in ("ativo", "desligado"):
            raise ErroValidacao(ja["aviso"])
        pedido["cpf"] = cpf

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
    for i, item in enumerate(regra["checklist"]):
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
    if p["situacao"] not in ABERTAS:
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
    if executa and p["colaborador_id"]:
        alvo = p["colaborador_id"]
        if executa == "desligar":
            svc_pessoas.desligar(s, alvo, pedido, usuario)
        elif executa == "readmitir":
            svc_pessoas.readmitir(s, alvo, pedido, usuario)
        elif executa == "trocar_vinculo":
            svc_pessoas.trocar_vinculo(s, alvo, {**pedido, "vinculo": pedido.get("vinculo") or "CTPS"}, usuario)
        elif executa == "alteracao":
            svc_pessoas.registrar_alteracao(s, alvo, pedido, usuario)
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
        c = svc_pessoas.salvar(s, {**pedido, "admissao": pedido.get("data_inicio")}, usuario)
        s.execute(text("UPDATE dp_solicitacoes SET colaborador_id = :c WHERE id = :s"),
                  {"c": c.id, "s": solicitacao_id})
        efeito["colaborador_id"] = c.id
    s.execute(text("""UPDATE dp_solicitacoes
                         SET situacao = 'ATENDIDA', atendido_por = :u,
                             atendido_em = now(), resposta = :r, atualizado_em = now(),
                             dados = CAST(:d AS jsonb)
                       WHERE id = :s"""),
              {"u": usuario.id if usuario else None, "s": solicitacao_id,
               "r": (str(dados.get("resposta") or "").strip() or None),
               "d": __import__("json").dumps(pedido, ensure_ascii=False, default=str)})
    _evento(s, solicitacao_id, "ATENDIDA",
            {"resposta": dados.get("resposta"), **efeito}, usuario)
    logger.info("ERP/DP: solicitação %s atendida por %s (%s)", p["numero"],
                usuario.nome if usuario else "?", efeito.get("executou") or "sem efeito")
    return ler(s, solicitacao_id)


def recusar(s: Session, solicitacao_id: int, dados: dict[str, Any],
            usuario: Usuario) -> dict[str, Any]:
    p = ler(s, solicitacao_id)
    if p["situacao"] not in ABERTAS:
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
    return ler(s, solicitacao_id)


def cancelar(s: Session, solicitacao_id: int, dados: dict[str, Any],
             usuario: Usuario) -> dict[str, Any]:
    """Quem abriu cancela o próprio pedido.

    No pipe, cancelar era ABRIR OUTRO CARD ("Cancelar Solicitação de
    Desligamento"). Aqui é ação sobre o próprio pedido — mas ele não desaparece:
    pedido apagado deixa "e o que aconteceu com aquele desligamento?" sem
    resposta.
    """
    p = ler(s, solicitacao_id)
    if p["situacao"] not in ABERTAS:
        raise ErroValidacao(
            f"{p['numero']} já está {p['situacao_rotulo'].lower()} — não dá para cancelar.")
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
        SELECT d.*, c.nome AS pessoa, c.cpf AS pessoa_cpf, o.codigo AS obra,
               u.nome AS solicitante, a.nome AS atendente
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN usuarios u ON u.id = d.solicitante_id
          LEFT JOIN usuarios a ON a.id = d.atendido_por
         WHERE d.id = :s"""), {"s": solicitacao_id}).mappings().first()
    if linha is None:
        raise ErroValidacao("Solicitação não encontrada.")
    d = dict(linha)
    regra = TIPOS.get(d["tipo"], {})
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
    return {
        "id": d["id"], "numero": d["numero"], "tipo": d["tipo"],
        "tipo_rotulo": regra.get("nome", d["tipo"]),
        "situacao": d["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(d["situacao"], d["situacao"]),
        "colaborador_id": d["colaborador_id"],
        "pessoa": d["pessoa"] or d["nome_informado"] or "",
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
        "itens": [{"id": i["id"], "descricao": i["descricao"], "feito": i["feito"],
                   "por": i["por"] or "",
                   "em": i["feito_em"].isoformat() if i["feito_em"] else None}
                  for i in itens],
        "feitos": sum(1 for i in itens if i["feito"]), "total_itens": len(itens),
        "eventos": [{"acao": e["acao"], "detalhe": e["detalhe"] or {},
                     "por": e["por"] or "", "em": e["criado_em"].isoformat()}
                    for e in eventos],
    }


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
        onde.append("d.situacao IN ('ABERTA', 'EM_ANALISE')")
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
               o.codigo AS obra, u.nome AS solicitante,
               (SELECT count(*) FROM dp_solicitacao_itens i
                 WHERE i.solicitacao_id = d.id) AS total_itens,
               (SELECT count(*) FROM dp_solicitacao_itens i
                 WHERE i.solicitacao_id = d.id AND i.feito) AS feitos
          FROM dp_solicitacoes d
          LEFT JOIN colaboradores c ON c.id = d.colaborador_id
          LEFT JOIN obras o ON o.id = d.obra_id
          LEFT JOIN usuarios u ON u.id = d.solicitante_id
         WHERE {' AND '.join(onde)}
         ORDER BY (d.situacao IN ('ABERTA', 'EM_ANALISE')) DESC, d.criado_em DESC
         LIMIT :limite"""), params).mappings().all()
    fila = [{
        "id": l["id"], "numero": l["numero"], "tipo": l["tipo"],
        "tipo_rotulo": TIPOS.get(l["tipo"], {}).get("nome", l["tipo"]),
        "situacao": l["situacao"],
        "situacao_rotulo": SITUACAO_ROTULOS.get(l["situacao"], l["situacao"]),
        "pessoa": l["pessoa"] or "", "obra": l["obra"] or "",
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
        "tipos": tipos_para_tela(),
        "situacoes": [{"valor": c, "rotulo": r} for c, r in SITUACAO_ROTULOS.items()],
    }
