# -*- coding: utf-8 -*-
"""
As perguntas que as telas fazem ao banco.

AQUI ESTÁ A DIFERENÇA para a versão em Streamlit. Lá, as 59 mil SPs eram
abertas na memória (162 MB por pessoa conectada, medidos) e o filtro rodava em
cima delas. Aqui, quem filtra e quem soma é o Postgres: a tela recebe a página
que vai mostrar e os poucos números do rodapé.

As regras de negócio são as MESMAS — status de agendamento, risco de
duplicidade, cadastro incompleto, boleto inválido e boleto duplicado foram
traduzidas uma a uma do `dados.py` original, sem mudar significado. Onde a
tradução muda alguma coisa, está escrito no comentário.
"""
from __future__ import annotations

import logging

from . import colunas

logger = logging.getLogger("analisesps.consultas")

# Quantas linhas por página. O Streamlit mandava até 2.000 de uma vez para a
# tabela e avisava quando cortava; uma página de 200 abre instantaneamente e
# não tem teto — a navegação alcança qualquer linha.
POR_PAGINA = 200

# Colunas cuja célula pode trazer MAIS DE UM valor. Hoje só o centro de custo
# (as obras): a planilha aceita duas na mesma célula quando a despesa é
# rateada entre elas. Tanto a lista do filtro quanto o casamento abrem a
# célula antes de comparar.
MULTIPLAS_NA_CELULA = {"centro_custo"}

# COMO A CÉLULA É ABERTA. A vírgula é o separador que o dono citou
# ("CONS, CRECHE SWAP"), mas a base real também traz BARRA
# ("OBRA-12 / OBRA-13") — está num teste escrito na época da conversão, a
# partir do dado de verdade. Aceitar os dois, e o ponto e vírgula de quebra,
# custa nada e evita descobrir o terceiro em produção.
SEPARADOR_DE_OBRAS = "[,;/]"

# Os campos varridos pela busca livre. Iguais aos do Streamlit.
CAMPOS_BUSCA = ["id", "credor", "documento", "descricao", "tipo_despesa",
                "centro_custo", "responsavel", "nf", "pedido", "analise_ia"]

# ---------------------------------------------------------------------------
# Pedaços de SQL que repetem a regra de negócio original
# ---------------------------------------------------------------------------

# Status de agendamento normalizado. Só aparece quando o status de pagamento é
# "Pagar"; nos demais (Pago, Cancelado) fica em branco — igual ao original.
SQL_STATUS_AGEND = """
CASE WHEN lower(trim(coalesce(status_pgt,''))) = 'pagar' THEN
  CASE
    WHEN lower(trim(coalesce(agendado,''))) LIKE '%falha%'  THEN 'Falha Agendar'
    WHEN lower(trim(coalesce(agendado,''))) = 'verificar'   THEN 'Verificar'
    WHEN lower(trim(coalesce(agendado,''))) = 'agendado'    THEN 'Agendado'
    WHEN lower(trim(coalesce(agendado,''))) = 'agendar'     THEN 'Agendar'
    ELSE '' END
ELSE '' END
"""

# Só os dígitos do código de barras — usado por "inválido" e "duplicado".
SQL_BARRAS_DIGITOS = r"regexp_replace(coalesce(codigo_barras,''), '\D', '', 'g')"

# Risco de duplicidade: é o que a análise da IA escreveu na coluna AL.
SQL_RISCO = "upper(coalesce(analise_ia,'')) LIKE '%COM RISCO%'"

# Cadastro incompleto — o "alerta laranja" do original. Três causas, e basta
# uma: (a) Pix/BeeVale sem a chave, (b) sem centro de custo, (c) sem código de
# integração do Omie num título ainda ativo.
SQL_CADASTRO_INCOMPLETO = r"""(
    (   (lower(coalesce(forma_pagamento,'')) LIKE '%pix%'
      OR lower(coalesce(forma_pagamento,'')) LIKE '%beevale%')
     AND trim(regexp_replace(coalesce(info_pgt,''),
                             'chave[[:space:]]*pix[[:space:]]*:?[[:space:]]*',
                             '', 'gi')) = '' )
 OR trim(coalesce(centro_custo,'')) = ''
 OR (    trim(coalesce(codigo_integracao,'')) = ''
     AND lower(trim(coalesce(status_pgt,''))) NOT IN ('cancelado','pago') )
)"""

# Boleto inválido: só entre os que estão a Pagar, como no original.
SQL_BOLETO_INVALIDO = (
    "( lower(trim(coalesce(forma_pagamento,''))) = 'boleto'"
    " AND lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    " AND ( upper(translate(coalesce(codigo_barras,''),'Á','A')) LIKE '%INVALIDO%'"
    f"      OR {SQL_BARRAS_DIGITOS} = ''"
    f"      OR {SQL_BARRAS_DIGITOS} ~ '^0+$' ) )")

# Boleto duplicado. A contagem de repetições é feita no universo COMPLETO e
# considera Pagar UNIÃO Pago (é o conjunto em que existe risco de pagar duas
# vezes); Cancelado não conta. Mostra só os que estão a Pagar — assim, um par
# "1 Pago + 1 Pagar" exibe o Pagar, que é o que ainda pode ser pago em
# duplicidade. Idêntico ao original.
SQL_BOLETO_DUPLICADO = (
    "( lower(trim(coalesce(forma_pagamento,''))) = 'boleto'"
    " AND lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    f" AND {SQL_BARRAS_DIGITOS} IN ("
    f"      SELECT {SQL_BARRAS_DIGITOS} FROM analisesps.sps"
    "        WHERE lower(trim(coalesce(forma_pagamento,''))) = 'boleto'"
    "          AND lower(trim(coalesce(status_pgt,''))) IN ('pagar','pago')"
    f"          AND {SQL_BARRAS_DIGITOS} <> ''"
    f"          AND {SQL_BARRAS_DIGITOS} !~ '^0+$'"
    "        GROUP BY 1 HAVING count(*) > 1 ) )")

# Hoje, em Brasília — não no UTC em que o servidor roda.
#
# A diferença é de três horas, e ela muda a resposta: entre 21h e meia-noite
# de Brasília o servidor já virou o dia. Sem a conversão, uma SP que vence
# amanhã apareceria em vermelho como atrasada para quem confere à noite. A
# conversão é feita AQUI, na fonte, e não em cada tela — é a mesma regra do
# `horario.py`.
SQL_HOJE = "(now() AT TIME ZONE 'America/Sao_Paulo')::date"

ORDENS = {
    "vencimento": "vencimento_d ASC NULLS LAST, id",
    "vencimento_desc": "vencimento_d DESC NULLS LAST, id",
    "valor": "valor_num ASC NULLS LAST, id",
    "valor_desc": "valor_num DESC NULLS LAST, id",
    "credor": "credor ASC, id",
    "id": "id",
}

SITUACOES = {
    "pendencias": "lower(trim(coalesce(status_pgt,''))) = 'pagar'",
    "risco": SQL_RISCO,
    "cadastro_incompleto": SQL_CADASTRO_INCOMPLETO,
    "boleto_invalido": SQL_BOLETO_INVALIDO,
    "boleto_duplicado": SQL_BOLETO_DUPLICADO,
}


# ---------------------------------------------------------------------------
# O RECORTE DA DOCUMENTAÇÃO FISCAL
#
# Correção do dono em 13/09/2026, depois de usar a tela: *"você replicou os
# filtros de solicitações, mas não é o que a gente trabalha aqui. Porque aqui o
# objetivo é categorizar a nota, o lançamento."*
#
# Ele tem razão, e o erro não era cosmético. O filtro de Solicitações responde
# "o que tem para pagar"; aqui a pergunta é outra — **o que falta documentar, o
# que está documentado, e o que provavelmente está errado**. Sem isso a tela
# vira uma lista para rolar, e ele disse a palavra: ingerível.
#
# POR QUE ISTO É SQL, E NÃO CONTA EM PYTHON. A conciliação roda sobre a página
# que está na tela (200 linhas). Filtrar e contar em cima dela responderia "o
# que falta NESTA PÁGINA", que é uma resposta inútil para decidir onde focar —
# e pior que inútil, porque parece certa. Aqui o recorte é do banco, sobre a
# base inteira.
#
# ONDE MORA CADA COISA. O card do Pipefy chega por duas portas e as duas valem:
# `sp_fiscal.doc_fiscal` é o que a planilha de apoio traz do card, e
# `sp_fiscal_analise` é o diário deste módulo (o que o card já trazia quando a
# tela nasceu, mais toda decisão tomada aqui). O diário manda quando existe.
# ---------------------------------------------------------------------------

# A documentação que vale para esta SP: a decidida aqui, ou a do card.
SQL_DOC_FISCAL = """
coalesce(nullif(trim(coalesce(
    (SELECT a.documentacao FROM analisesps.sp_fiscal_analise a
      WHERE a.sp_id = sps.id), '')), ''),
         nullif(trim(coalesce(
    (SELECT x.doc_fiscal FROM analisesps.sp_fiscal x
      WHERE x.sp_id = sps.id), '')), ''),
         '')
"""

# A chave de acesso conhecida, só dígitos.
SQL_CHAVE_FISCAL = r"""
regexp_replace(coalesce(
    (SELECT a.chave FROM analisesps.sp_fiscal_analise a
      WHERE a.sp_id = sps.id), ''), '\D', '', 'g')
"""

# Em que pé está o trabalho desta SP no diário.
SQL_SITUACAO_FISCAL = """
coalesce((SELECT a.situacao FROM analisesps.sp_fiscal_analise a
           WHERE a.sp_id = sps.id), '')
"""

# O CNPJ do credor, só dígitos — é com ele que o emitente de dentro da chave é
# comparado.
SQL_DOC_CREDOR = r"regexp_replace(coalesce(documento,''), '\D', '', 'g')"

# O CNPJ de quem emitiu, lido de DENTRO da chave: posições 7 a 20, definição da
# Receita. Serve para acusar nota trocada entre dois lançamentos sem depender
# de achar a nota no relatório.
SQL_EMITENTE_DA_CHAVE = f"substring({SQL_CHAVE_FISCAL} from 7 for 14)"

# As categorias que AFIRMAM existir nota eletrônica. Iguais às de `fiscal.py`,
# e o teste `test_as_categorias_que_exigem_nota_sao_as_mesmas` trava isso: duas
# listas divergentes fariam a tela contar uma coisa e a conciliação outra.
CATEGORIAS_QUE_EXIGEM_NOTA = ("NF-e (Mercadoria)", "NFS-e (Serviço)",
                              "CT-e (Frete)", "NFC-e (Cupom Fiscal eletrônico)")

_LISTA_EXIGEM_NOTA = ",".join(
    "'" + c.replace("'", "''") + "'" for c in CATEGORIAS_QUE_EXIGEM_NOTA)

# PROVAVELMENTE ERRADO. Três sintomas, e basta um:
#   (a) a nota que está no card foi CANCELADA — despesa contra documento que
#       não existe mais, o mais grave da lista;
#   (b) o card afirma que há nota eletrônica e não há chave nenhuma — alguém
#       classificou sem documento;
#   (c) a chave do card foi emitida por um CNPJ que não é o do credor — o
#       sintoma clássico de anexo trocado entre dois lançamentos.
SQL_FISCAL_PROVAVEL_ERRO = f"""(
    EXISTS (SELECT 1 FROM analisesps.notas_fiscais n
             WHERE n.chave = {SQL_CHAVE_FISCAL}
               AND upper(coalesce(n.status,'')) = 'CANCELADA')
 OR (trim({SQL_DOC_FISCAL}) IN ({_LISTA_EXIGEM_NOTA})
     AND {SQL_CHAVE_FISCAL} = '')
 OR (length({SQL_CHAVE_FISCAL}) = 44
     AND length({SQL_DOC_CREDOR}) = 14
     AND {SQL_EMITENTE_DA_CHAVE} <> {SQL_DOC_CREDOR})
)"""

# O RECORTE PRINCIPAL DESTA TELA. Somam-se como os outros: marcar dois exige
# os dois ao mesmo tempo.
SITUACOES_FISCAIS = {
    "sem_marcacao": f"trim({SQL_DOC_FISCAL}) = ''",
    "ja_marcado": f"trim({SQL_DOC_FISCAL}) <> ''",
    "provavel_erro": SQL_FISCAL_PROVAVEL_ERRO,
    "com_chave": f"length({SQL_CHAVE_FISCAL}) = 44",
    "sem_chave": f"length({SQL_CHAVE_FISCAL}) <> 44",
    "na_fila_ia": f"{SQL_SITUACAO_FISCAL} = 'NA_FILA_IA'",
    "lida_ia": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id AND a.origem = 'IA')"),
    "confirmada": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id AND a.situacao = 'CONFIRMADA' "
        "           AND a.escrita_em IS NULL)"),
    "escrita": f"{SQL_SITUACAO_FISCAL} = 'ESCRITA'",
    "decidida_por_pessoa": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id AND a.origem = 'PESSOA')"),
    # O OUTRO LADO DA MESMA PERGUNTA, e faltava: o que foi marcado SEM gente.
    # `CONCILIACAO` é proposta do sistema aprovada, `IA` é leitura de anexo.
    # `PIPEFY` fica de fora de propósito: aquilo não foi o sistema que marcou,
    # é o que já estava no card antes desta tela existir.
    "decidida_pelo_sistema": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id "
        "           AND a.origem IN ('CONCILIACAO', 'IA'))"),
    # E o que veio pronto do card, que não é decisão de ninguém aqui.
    "veio_do_card": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id AND a.origem = 'PIPEFY')"),
    "com_anexo": "trim(coalesce(anexo_link,'')) <> ''",
    "sem_anexo": "trim(coalesce(anexo_link,'')) = ''",
    # -----------------------------------------------------------------------
    # A CONFIANÇA DO QUE ESTÁ GRAVADO — pedido do dono em 13/09/2026:
    # *"deveria poder filtrar por confiança"*.
    #
    # ⚠️ E É PRECISO DIZER DE QUAL CONFIANÇA SE ESTÁ FALANDO, porque existem
    # DUAS na tela e confundi-las daria um filtro que mente:
    #
    #   1. A que está GRAVADA no diário — a da IA, e a da proposta que alguém
    #      já aprovou. Vive no banco, então dá para filtrar a base inteira.
    #      É esta.
    #   2. A do par que o sistema calcula AO ABRIR a tela, para a linha que
    #      ainda não foi decidida. Ela não existe no banco: nasce e morre a
    #      cada abertura, e só para as 200 linhas da página. Filtrar por ela
    #      responderia "nesta página", que parece certo e não é.
    #
    # Os nomes na tela dizem "do que está gravado" justamente para não haver
    # engano. Passar a segunda para o banco exige a varredura da base inteira
    # em processo separado — está oferecido ao dono e ainda sem resposta.
    # -----------------------------------------------------------------------
    "confianca_alta": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id AND a.confianca >= 80)"),
    "confianca_media": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id "
        "           AND a.confianca >= 60 AND a.confianca < 80)"),
    "confianca_baixa": (
        "EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "         WHERE a.sp_id = sps.id "
        "           AND a.confianca > 0 AND a.confianca < 60)"),
    "sem_confianca": (
        "NOT EXISTS (SELECT 1 FROM analisesps.sp_fiscal_analise a "
        "             WHERE a.sp_id = sps.id AND a.confianca > 0)"),
}

# ---------------------------------------------------------------------------
# COMO OS RECORTES APARECEM NA TELA — em grupos, e com o nome do DADO
#
# Refeito em 13/09/2026 depois de ele usar: *"a nomenclatura dos filtros tá
# estranha, a compreensão tá ruim, muito ruim mesmo. Eu não consigo filtrar
# como eu faria numa planilha facilmente. Não dá nem pra entender o que estamos
# filtrando, quais dados."*
#
# Ele está certo, e o erro era de nome, não de função: "Sem documentação",
# "Já categorizado", "Com chave de acesso" e "Confirmado" estavam numa lista
# corrida, sem dizer de QUE COLUNA cada um fala. Numa planilha ele filtra
# clicando no cabeçalho da coluna — sabe exatamente o que está recortando.
#
# ENTÃO OS RECORTES PASSAM A VIR EM GRUPOS, e o título do grupo é o nome do
# dado. "A categoria (coluna 'Está como')" diz, sozinho, o que as duas opções
# abaixo dele fazem. Cada opção ainda traz uma linha explicando, para o caso de
# o título não bastar.
# ---------------------------------------------------------------------------
GRUPOS_DE_RECORTE = [
    ("A categoria — é a coluna \"Está como\"", [
        ("sem_marcacao", "Está vazia",
         "nenhuma categoria, nem daqui nem do card do Pipefy"),
        ("ja_marcado", "Está preenchida",
         "tem categoria, seja qual for"),
    ]),
    ("A nota fiscal", [
        ("com_chave", "Tem chave de acesso",
         "os 44 números que identificam a nota"),
        ("sem_chave", "Não tem chave de acesso",
         "pode ter categoria e mesmo assim não ter nota apontada"),
        ("provavel_erro", "Parece errada",
         "nota cancelada, ou categoria que afirma nota sem haver chave, "
         "ou chave emitida por outro CNPJ"),
    ]),
    ("Quem preencheu", [
        ("decidida_por_pessoa", "Uma pessoa, aqui",
         "alguém digitou ou confirmou nesta tela"),
        ("decidida_pelo_sistema", "O sistema",
         "proposta aprovada ou leitura por IA"),
        ("veio_do_card", "Já veio do card",
         "estava no Pipefy antes desta tela existir"),
    ]),
    ("Em que pé está o trabalho", [
        ("na_fila_ia", "Esperando a IA ler o anexo", ""),
        ("lida_ia", "A IA já leu", ""),
        ("confirmada", "Confirmado aqui, falta ir para o card", ""),
        ("escrita", "Já gravado no card do Pipefy", ""),
    ]),
    ("A confiança do que está gravado", [
        ("confianca_alta", "Alta — 80% ou mais",
         "a IA ou a conciliação gravaram com pouca dúvida"),
        ("confianca_media", "Média — de 60% a 79%",
         "acima do corte para propor, mas vale conferir"),
        ("confianca_baixa", "Baixa — abaixo de 60%",
         "gravado, mas com dúvida; é onde olhar primeiro"),
        ("sem_confianca", "Sem confiança gravada",
         "nunca foi decidido, ou veio pronto do card"),
    ]),
    ("O anexo da SP", [
        ("com_anexo", "Tem anexo", "dá para mandar para a IA ler"),
        ("sem_anexo", "Não tem anexo",
         "sem anexo, a IA não tem o que ler"),
    ]),
]

# A lista corrida, que é o que a barra antiga usava e o painel ainda usa.
ROTULOS_FISCAIS = [(chave, rotulo)
                   for _, itens in GRUPOS_DE_RECORTE
                   for chave, rotulo, _ in itens]

# O nome de cada recorte numa frase só, para a tela poder dizer em português o
# que está filtrando agora. Sem isto, quem chega numa tela filtrada por outra
# pessoa (ou por si mesmo ontem) não tem como saber o que está vendo.
FRASE_DO_RECORTE = {
    "sem_marcacao": "a categoria está vazia",
    "ja_marcado": "a categoria está preenchida",
    "com_chave": "tem chave de acesso",
    "sem_chave": "não tem chave de acesso",
    "provavel_erro": "a nota parece errada",
    "decidida_por_pessoa": "quem preencheu foi uma pessoa",
    "decidida_pelo_sistema": "quem preencheu foi o sistema",
    "veio_do_card": "já veio preenchido do card",
    "na_fila_ia": "está esperando a IA ler o anexo",
    "lida_ia": "a IA já leu",
    "confirmada": "está confirmado aqui e falta ir para o card",
    "escrita": "já foi gravado no card",
    "com_anexo": "tem anexo",
    "sem_anexo": "não tem anexo",
    "confianca_alta": "a confiança gravada é 80% ou mais",
    "confianca_media": "a confiança gravada é de 60% a 79%",
    "confianca_baixa": "a confiança gravada é menor que 60%",
    "sem_confianca": "não há confiança gravada",
}


# ===========================================================================
# O QUE A DOCUMENTAÇÃO FISCAL NEM DEVE OLHAR — pedido do dono em 13/09/2026
#
# Três cortes que valem para a tela inteira, e não são "mais um filtro": são o
# tamanho do universo. Fora deles, o painel conta trabalho que ninguém vai
# fazer, e "faltam 4.000" vira um número que ninguém acredita.
#
# 1. *"Os registros da documentação fiscal não devem retornar apenas os dados
#    do que venceu em 2026 ou do que foi pago em 2026, o restante ignorar."*
#
#    ⚠️ ESCOLHI **2026 EM DIANTE**, e não "só 2026", e a diferença importa: com
#    "= 2026" a tela esvaziaria sozinha na virada do ano, sem ninguém mexer em
#    nada e sem aviso nenhum. Com ">= 2026" o atraso velho fica de fora — que é
#    o que ele pediu — e a tela continua funcionando em 2027. Se ele quiser
#    mesmo só o ano corrente, é trocar uma linha.
#
# 2. *"A princípio tudo que está com o Status Pgt = Cancelado não deveria ser
#    exibido, somente se colocássemos para exibir."* — some por padrão, com
#    uma caixa para trazer de volta.
#
# 3. *"Não deve ser exibido registros que contenham no Tipo de Despesa a
#    informação '(TRF)'."* — transferência não gera documento fiscal, e este
#    não tem caixa nenhuma: é sempre fora.
#
# POR QUE AQUI DENTRO, e não no `listar`: o painel, a lista e a paginação
# passam todos por `_condicoes`. Um corte aplicado em dois dos três daria de
# novo o defeito de 13/09 — o painel dizendo "3 já categorizados" e a linha
# mostrando "—".
# ===========================================================================
ANO_FISCAL_MINIMO = 2026

# O ano é lido do VENCIMENTO **ou** do PAGAMENTO: a SP vencida em dezembro de
# 2025 e paga em janeiro de 2026 é trabalho de 2026 e tem de aparecer.
#
# ⚠️ A SP SEM DATA NENHUMA FICA. Ela não é "velha" — ela é *sem data*, e são
# coisas diferentes. O pedido foi deixar de fora o atraso antigo; uma SP que
# não diz quando vence não prova ser antiga, e sumir com ela seria tirar da
# conta um trabalho que ninguém mais veria. Some em silêncio é o defeito que
# esta tela já teve duas vezes.
#
# Se em produção aparecer muita SP sem data, isto vira decisão do dono — e aí
# a linha muda para excluir. Hoje ela aparece.
SQL_ANO_FISCAL = (
    "(extract(year from vencimento_d) >= ? "
    " OR extract(year from data_pagamento_d) >= ? "
    " OR (vencimento_d IS NULL AND data_pagamento_d IS NULL))")

SQL_NAO_CANCELADA = "lower(btrim(coalesce(status_pgt,''))) <> 'cancelado'"

# `(TRF)` marca transferência entre contas da empresa. Casa sem diferenciar
# maiúscula, e o `%` dos dois lados porque a marca vem no meio do texto
# ("Mat. Construção (TRF)").
SQL_SEM_TRF = "coalesce(tipo_despesa,'') NOT ILIKE ?"
MARCA_TRF = "%(TRF)%"


def condicoes_do_escopo_fiscal(mostrar_canceladas: bool = False):
    """Os três cortes da tela fiscal, em SQL e parâmetros."""
    onde = [SQL_ANO_FISCAL, SQL_SEM_TRF]
    params: list = [ANO_FISCAL_MINIMO, ANO_FISCAL_MINIMO, MARCA_TRF]
    if not mostrar_canceladas:
        onde.append(SQL_NAO_CANCELADA)
    return onde, params


# ---------------------------------------------------------------------------
# Montagem do filtro
# ---------------------------------------------------------------------------
def _como_texto_literal(termo: str) -> str:
    """Prepara um termo digitado para entrar num LIKE, sem virar curinga.

    No LIKE, `%` quer dizer "qualquer coisa" e `_` quer dizer "um caractere
    qualquer". Quem procura por "100%" quer o texto "100%", não "100 seguido de
    qualquer coisa" — que casaria com "1000". E quem procura "nota_1" quer o
    sublinhado, não um caractere qualquer no lugar dele.

    O Streamlit fazia busca literal (`contains` sem expressão regular), e a
    tradução tem de manter isso: a barra invertida escapa os três caracteres
    especiais, que é como o Postgres entende por padrão.
    """
    return (termo.replace("\\", "\\\\")
                 .replace("%", "\\%")
                 .replace("_", "\\_"))


def _condicoes(f: dict) -> tuple[list[str], list]:
    """Traduz o dicionário de filtros da tela em pedaços de SQL e parâmetros.

    Tudo entra como PARÂMETRO, nunca costurado dentro do texto do SQL — é o que
    impede que um credor com aspas no nome, ou um texto de busca mal
    intencionado, vire comando."""
    onde: list[str] = []
    params: list = []

    # Busca livre: termos separados por vírgula, TODOS têm de aparecer.
    busca = str(f.get("busca") or "").strip()
    if busca:
        alvo = " || ' ' || ".join(f"lower(coalesce({c},''))" for c in CAMPOS_BUSCA)
        for termo in [t.strip().lower() for t in busca.split(",") if t.strip()]:
            onde.append(f"({alvo}) LIKE ?")
            params.append(f"%{_como_texto_literal(termo)}%")

    # Listas de seleção simples.
    for campo, coluna in (("status_pgt", "status_pgt"),
                          ("conta", "conta"),
                          ("forma", "forma_pagamento"),
                          ("tipo_despesa", "tipo_despesa"),
                          ("projeto", "projeto"),
                          ("responsavel", "responsavel")):
        escolhidos = [v for v in (f.get(campo) or []) if str(v).strip()]
        if escolhidos:
            marcadores = ",".join(["?"] * len(escolhidos))
            onde.append(f"trim(coalesce({coluna},'')) IN ({marcadores})")
            params.extend(escolhidos)

    # Status de agendamento: "Sem Agendamento" quer dizer o valor vazio.
    agend = [v for v in (f.get("status_agend") or []) if str(v).strip()]
    if agend:
        alvos = [v for v in agend if v != "Sem Agendamento"]
        pedacos = []
        if alvos:
            pedacos.append(f"({SQL_STATUS_AGEND}) IN ({','.join(['?'] * len(alvos))})")
            params.extend(alvos)
        if "Sem Agendamento" in agend:
            pedacos.append(f"({SQL_STATUS_AGEND}) = ''")
        onde.append("(" + " OR ".join(pedacos) + ")")

    # Centro de custo (as OBRAS). A célula às vezes traz mais de uma, separadas
    # por vírgula: "CONS, CRECHE SWAP".
    #
    # Antes o casamento era por "contém", copiado do Streamlit. Funcionava na
    # maioria dos casos e errava num que aparece: procurar a obra "CONS"
    # trazia também "CONSTRUÇÃO DO GALPÃO", porque uma é pedaço da outra.
    # Agora a célula é ABERTA na vírgula e a comparação é com a obra INTEIRA —
    # que é o que a pessoa escolheu na lista.
    centros = [str(v).strip() for v in (f.get("centro_custo") or [])
               if str(v).strip()]
    if centros:
        pedacos = []
        for c in centros:
            pedacos.append(
                "EXISTS (SELECT 1 FROM unnest(regexp_split_to_array("
                f"          coalesce(centro_custo,''), '{SEPARADOR_DE_OBRAS}')"
                "        ) AS o WHERE lower(btrim(o)) = ?)")
            params.append(c.lower())
        onde.append("(" + " OR ".join(pedacos) + ")")

    # Situações (as caixas de marcar). Somam-se: marcar duas exige as duas.
    for chave in (f.get("situacoes") or []):
        if chave in SITUACOES:
            onde.append(SITUACOES[chave])

    # O recorte da Documentação Fiscal. Mesma regra de soma, e de propósito na
    # MESMA função: se o filtro da tela e a conta do painel fossem montados em
    # dois lugares, o dia em que um ganhasse um recorte a mais o outro passaria
    # a mentir sem ninguém notar.
    for chave in (f.get("fiscais") or []):
        if chave in SITUACOES_FISCAIS:
            onde.append(SITUACOES_FISCAIS[chave])

    # FILTRAR POR UMA CATEGORIA, que é o clique no quadro por categoria.
    # *"Era interessante esse KPI direcionar pra uma tela com as informações:
    # eu clicar e mostrar 'olha, essas aqui são as de fundo fixo', aí a
    # lista."* O nome vem do endereço, então entra como PARÂMETRO — nunca
    # costurado no texto do SQL.
    categorias = [str(c) for c in (f.get("categoria") or []) if str(c).strip()]
    if categorias:
        marcas = ",".join(["?"] * len(categorias))
        onde.append(f"trim({SQL_DOC_FISCAL}) IN ({marcas})")
        params.extend(categorias)
    # E o clique na pilha do "(sem informação)", que é a ausência de categoria.
    if f.get("sem_categoria"):
        onde.append(f"trim({SQL_DOC_FISCAL}) = ''")

    # O ESCOPO DA DOCUMENTAÇÃO FISCAL. Só entra quando a tela pede — em
    # Solicitações ele veria menos SPs do que a planilha tem, e aí a conta dele
    # não fecharia com a SPsBD.
    if f.get("escopo_fiscal"):
        cortes, valores = condicoes_do_escopo_fiscal(
            bool(f.get("mostrar_canceladas")))
        onde.extend(cortes)
        params.extend(valores)

    # Períodos e faixa de valor.
    for campo, coluna, operador in (
            ("periodo_ini", "vencimento_d", ">="),
            ("periodo_fim", "vencimento_d", "<="),
            ("pgt_ini", "data_pagamento_d", ">="),
            ("pgt_fim", "data_pagamento_d", "<="),
            ("valor_ini", "valor_num", ">="),
            ("valor_fim", "valor_num", "<=")):
        valor = f.get(campo)
        if valor not in (None, ""):
            onde.append(f"{coluna} {operador} ?")
            params.append(valor)

    return onde, params


def _where(f: dict) -> tuple[str, list]:
    onde, params = _condicoes(f)
    return (" WHERE " + " AND ".join(onde)) if onde else "", params


# ---------------------------------------------------------------------------
# As perguntas
# ---------------------------------------------------------------------------
def resumo(f: dict) -> dict:
    """Quantas SPs o filtro alcança e quanto somam.

    Uma consulta só, sem trazer linha nenhuma. É o que substitui abrir a base
    inteira na memória para somar uma coluna."""
    from .db import consultar_um
    where, params = _where(f)
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(valor_num), 0), "
        "       count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) = 'pagar'), "
        "       coalesce(sum(valor_num) FILTER "
        "                (WHERE lower(trim(coalesce(status_pgt,''))) = 'pagar'), 0) "
        f"  FROM analisesps.sps{where}", tuple(params))
    if not linha:
        return {"quantidade": 0, "total": 0, "quantidade_pagar": 0, "total_pagar": 0}
    return {"quantidade": linha[0], "total": linha[1],
            "quantidade_pagar": linha[2], "total_pagar": linha[3]}


# As mesmas regras dos recortes, escritas sobre uma LINHA JÁ MONTADA em vez de
# subconsulta por linha. Ver `painel_fiscal` para o porquê — e para o teste que
# impede as duas de divergirem.
_PAINEL_COLUNAS = f"""
    SELECT sps.id, sps.valor_num, sps.anexo_link, sps.documento,
           coalesce(nullif(btrim(coalesce(a.documentacao, '')), ''),
                    btrim(coalesce(x.doc_fiscal, ''))) AS doc,
           regexp_replace(coalesce(a.chave, ''), '\\D', '', 'g') AS chave_lim,
           coalesce(a.situacao, '') AS situacao,
           coalesce(a.origem, '') AS origem,
           a.escrita_em
      FROM analisesps.sps
      LEFT JOIN analisesps.sp_fiscal_analise a ON a.sp_id = sps.id
      LEFT JOIN analisesps.sp_fiscal x ON x.sp_id = sps.id
"""

_PAINEL_REGRAS = {
    "sem_marcacao": "btrim(doc) = ''",
    "ja_marcado": "btrim(doc) <> ''",
    "com_chave": "length(chave_lim) = 44",
    "sem_chave": "length(chave_lim) <> 44",
    "na_fila_ia": "situacao = 'NA_FILA_IA'",
    "confirmada": "situacao = 'CONFIRMADA' AND escrita_em IS NULL",
    "escrita": "situacao = 'ESCRITA'",
    "sem_anexo": "btrim(coalesce(anexo_link, '')) = ''",
    "provavel_erro": f"""(
        EXISTS (SELECT 1 FROM analisesps.notas_fiscais n
                 WHERE n.chave = chave_lim
                   AND upper(coalesce(n.status, '')) = 'CANCELADA')
     OR (btrim(doc) IN ({_LISTA_EXIGEM_NOTA}) AND chave_lim = '')
     OR (length(chave_lim) = 44
         AND length(regexp_replace(coalesce(documento, ''), '\\D', '', 'g')) = 14
         AND substring(chave_lim from 7 for 14)
             <> regexp_replace(coalesce(documento, ''), '\\D', '', 'g'))
    )""",
}


def painel_fiscal(f: dict) -> dict:
    """Os totalizadores da Documentação Fiscal, sobre TUDO que o filtro alcança.

    Pedido do dono em 13/09/2026: *"onde é que eu vejo aqui como é que está a
    situação, uma espécie de totalizadores, pra saber o que que está faltando,
    o que que não está faltando, onde é que eu tenho que focar"*. Sem eles a
    tela é uma lista para rolar — a palavra dele foi ingerível.

    UMA CONSULTA SÓ, com `FILTER`: nove contagens em nove consultas seriam nove
    varreduras da base num banco que tem um décimo de um núcleo.

    ⚠️ E AS REGRAS SÃO ESCRITAS DE OUTRO JEITO AQUI, DE PROPÓSITO. Os recortes
    do filtro são subconsultas correlacionadas — certas, e o índice as resolve
    linha a linha. Repetir nove delas na MESMA varredura custava caro: medido
    em 13/09/2026 com 59.000 SPs, **1,18 segundo** nesta máquina, que é bem
    mais rápida que o banco do Render. Aqui as duas tabelas entram por JUNÇÃO,
    uma vez, e as contagens leem colunas já prontas.

    O RISCO DISSO É ÓBVIO — duas escritas da mesma regra divergindo — e é por
    isso que `test_o_painel_e_o_filtro_CONCORDAM_sempre` existe, com banco de
    verdade, comparando cada contagem com o filtro correspondente. Sem esse
    teste, esta otimização não valeria o preço."""
    from .db import consultar_um
    where, params = _where(f)

    def conta(chave):
        return f"count(*) FILTER (WHERE {_PAINEL_REGRAS[chave]})"

    linha = consultar_um(
        "SELECT count(*), "
        f"       {conta('sem_marcacao')}, "
        f"       {conta('ja_marcado')}, "
        f"       {conta('provavel_erro')}, "
        f"       {conta('com_chave')}, "
        f"       {conta('na_fila_ia')}, "
        f"       {conta('confirmada')}, "
        f"       {conta('escrita')}, "
        f"       {conta('sem_anexo')}, "
        "       coalesce(sum(valor_num) FILTER "
        f"               (WHERE {_PAINEL_REGRAS['sem_marcacao']}), 0) "
        f"  FROM ({_PAINEL_COLUNAS}{where}) t", tuple(params))

    nomes = ["total", "sem_marcacao", "ja_marcado", "provavel_erro",
             "com_chave", "na_fila_ia", "confirmada", "escrita", "sem_anexo",
             "valor_sem_marcacao"]
    if not linha:
        return {n: 0 for n in nomes}
    return dict(zip(nomes, linha))


def contagem_agendamento(f: dict) -> dict:
    """Agendar / Agendado / Pago / Falha — a mesma divisão do Streamlit.

    Os quatro grupos são EXCLUDENTES e nesta ordem de prioridade: pago ganha
    de tudo (não interessa como foi agendado, já saiu); depois falha; depois
    agendado; o resto é "a agendar". Sem essa ordem, uma SP paga que tinha
    ficado com "Falha Agendar" apareceria nas duas contas, e a soma dos
    quatro passaria do total."""
    from .db import consultar_um
    where, params = _where(f)
    pago = "lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    linha = consultar_um(
        "SELECT "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) = 'pago'), "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) <> 'pago' "
        "                     AND lower(coalesce(agendado,'')) LIKE '%falha%'), "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) <> 'pago' "
        "                     AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                     AND lower(trim(coalesce(agendado,''))) = 'agendado'), "
        "  count(*) FILTER (WHERE lower(trim(coalesce(status_pgt,''))) <> 'pago' "
        "                     AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                     AND lower(trim(coalesce(agendado,''))) <> 'agendado') "
        f"  FROM analisesps.sps{where}", tuple(params))
    if not linha:
        return {"Pago": 0, "Falha Agendar": 0, "Agendado": 0, "Agendar": 0}
    return {"Pago": linha[0], "Falha Agendar": linha[1],
            "Agendado": linha[2], "Agendar": linha[3]}


def resumo_e_agendamento(f: dict) -> tuple[dict, dict]:
    """Os dois de cima NUMA IDA SÓ ao banco.

    Separados, cada um varria a tabela filtrada por conta própria: medidos aqui
    com as 59 mil SPs, 44 ms + 48 ms. Juntos, 59 ms — porque a varredura é uma
    só e as contagens vão de carona. As duas funções acima continuam existindo
    para quem precisa de um dos dois sozinho (a exportação, por exemplo).

    O SQL é montado a partir das MESMAS peças das duas funções, de propósito:
    duas cópias do texto divergiriam no dia em que a regra de "pago ganha de
    tudo" mudasse em uma delas."""
    from .db import consultar_um
    where, params = _where(f)
    # O `lower(...)` vai escrito em cada linha, e não numa variável costurada
    # depois: há um teste que lê este arquivo linha a linha procurando LIKE
    # contra texto sem `lower()` — no Postgres o LIKE distingue maiúscula, e
    # esse já foi um defeito de verdade aqui. Esconder a normalização atrás de
    # uma variável cega o teste sem consertar nada.
    pago = "lower(trim(coalesce(status_pgt,'')))"
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(valor_num), 0), "
        f"       count(*) FILTER (WHERE {pago} = 'pagar'), "
        "       coalesce(sum(valor_num) FILTER "
        f"                (WHERE {pago} = 'pagar'), 0), "
        f"       count(*) FILTER (WHERE {pago} = 'pago'), "
        f"       count(*) FILTER (WHERE {pago} <> 'pago' "
        "                          AND lower(coalesce(agendado,'')) LIKE '%falha%'), "
        f"       count(*) FILTER (WHERE {pago} <> 'pago' "
        "                          AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                          AND lower(trim(coalesce(agendado,''))) = 'agendado'), "
        f"       count(*) FILTER (WHERE {pago} <> 'pago' "
        "                          AND lower(coalesce(agendado,'')) NOT LIKE '%falha%' "
        "                          AND lower(trim(coalesce(agendado,''))) <> 'agendado') "
        f"  FROM analisesps.sps{where}", tuple(params))
    if not linha:
        return ({"quantidade": 0, "total": 0, "quantidade_pagar": 0,
                 "total_pagar": 0},
                {"Pago": 0, "Falha Agendar": 0, "Agendado": 0, "Agendar": 0})
    return ({"quantidade": linha[0], "total": linha[1],
             "quantidade_pagar": linha[2], "total_pagar": linha[3]},
            {"Pago": linha[4], "Falha Agendar": linha[5],
             "Agendado": linha[6], "Agendar": linha[7]})


def soma_por(f: dict, coluna: str, limite: int = 12) -> list[dict]:
    """Σ do valor por conta ou por forma de pagamento, como no Streamlit.

    A coluna é escolhida por NÓS, de uma lista fechada — nunca vem de quem
    chama. Nome de coluna não entra como parâmetro do banco, e concatenar o
    que veio de fora aqui seria a porta aberta clássica."""
    permitidas = {"conta", "forma_pagamento"}
    if coluna not in permitidas:
        raise ValueError(f"Não somo por '{coluna}'.")
    from .db import consultar
    where, params = _where(f)
    linhas = consultar(
        f"SELECT coalesce(nullif(trim({coluna}), ''), '(sem informação)'), "
        "       count(*), coalesce(sum(valor_num), 0) "
        f"  FROM analisesps.sps{where} "
        f" GROUP BY 1 ORDER BY 3 DESC LIMIT ?", tuple(params) + (limite,))
    return [{"nome": l[0], "quantidade": l[1], "total": l[2]} for l in linhas]


CAMPOS_LISTA = [
    "id", "solicitacao_d", "vencimento_d", "credor", "documento",
    "tipo_despesa", "centro_custo", "projeto", "valor_num", "responsavel",
    "status_pgt", "status_aut", "forma_pagamento", "conta", "info_pgt",
    "nf", "pedido", "data_pagamento_d", "anuente", "validacao",
    "comprovante", "codigo_barras", "analise_ia", "descricao",
    "anexo_link", "card_link",
]


def listar(f: dict, ordem: str = "vencimento", pagina: int = 1) -> list[dict]:
    """Uma página de SPs, já ordenada. Só as colunas que a tela mostra."""
    from .db import consultar
    where, params = _where(f)
    ordenacao = ORDENS.get(ordem, ORDENS["vencimento"])
    pagina = max(1, int(pagina or 1))

    campos = ", ".join(CAMPOS_LISTA)
    linhas = consultar(
        f"SELECT {campos}, ({SQL_STATUS_AGEND}) AS status_agend, "
        f"       ({SQL_RISCO}) AS risco, "
        f"       {SQL_CADASTRO_INCOMPLETO} AS cadastro_incompleto, "
        # O atraso é calculado aqui, uma vez, e não linha a linha na tela.
        f"       (vencimento_d IS NOT NULL AND vencimento_d < {SQL_HOJE} "
        "         AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vencido, "
        f"       (vencimento_d = {SQL_HOJE} "
        "         AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vence_hoje "
        f"  FROM analisesps.sps{where} "
        f" ORDER BY {ordenacao} LIMIT ? OFFSET ?",
        tuple(params) + (POR_PAGINA, (pagina - 1) * POR_PAGINA))

    nomes = CAMPOS_LISTA + ["status_agend", "risco", "cadastro_incompleto",
                            "vencido", "vence_hoje"]
    return [dict(zip(nomes, linha)) for linha in linhas]


def uma(sp_id: str) -> dict | None:
    """A ficha completa de uma SP."""
    from .db import consultar
    campos = ", ".join(f'"{c}"' for c in colunas.CHAVES)
    linhas = consultar(
        f"SELECT {campos}, valor_num, solicitacao_d, vencimento_d, "
        f"       data_pagamento_d, dt_autorizacao_d, ({SQL_STATUS_AGEND}) "
        "  FROM analisesps.sps WHERE id = ?", (str(sp_id),))
    if not linhas:
        return None
    nomes = list(colunas.CHAVES) + [
        "valor_num", "solicitacao_d", "vencimento_d", "data_pagamento_d",
        "dt_autorizacao_d", "status_agend"]
    return dict(zip(nomes, linhas[0]))


def painel_por_agendamento(rotulos: list, quantos: int = 20) -> list[dict]:
    """As listas do painel do Lote, TODAS numa varredura só.

    Antes eram OITO consultas — uma lista e um resumo para cada um dos quatro
    status —, e cada uma percorria as 59 mil SPs inteiras. Medido: 185 dos
    200 ms da tela do Lote eram isto. Agora são duas: uma traz as primeiras
    linhas de cada status, outra traz quantidade e total de cada um.

    A primeira usa `row_number`, que numera as linhas DENTRO de cada status já
    ordenadas por vencimento — assim o banco separa os quatro grupos numa
    passada e devolve só as vinte de cada, em vez de mandar oitocentas para
    serem jogadas fora aqui."""
    from .db import consultar

    if not rotulos:
        return []
    marcadores = ",".join(["?"] * len(rotulos))
    campos = ", ".join(CAMPOS_LISTA)

    linhas = consultar(
        f"WITH classificadas AS ("
        f"  SELECT {campos}, ({SQL_STATUS_AGEND}) AS status_agend, "
        f"         ({SQL_RISCO}) AS risco, "
        f"         {SQL_CADASTRO_INCOMPLETO} AS cadastro_incompleto, "
        f"         (vencimento_d IS NOT NULL AND vencimento_d < {SQL_HOJE} "
        "           AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vencido, "
        f"         (vencimento_d = {SQL_HOJE} "
        "           AND lower(trim(coalesce(status_pgt,''))) = 'pagar') AS vence_hoje "
        "    FROM analisesps.sps), "
        "numeradas AS ("
        "  SELECT *, row_number() OVER (PARTITION BY status_agend "
        "                               ORDER BY vencimento_d ASC NULLS LAST, id) AS posicao "
        f"    FROM classificadas WHERE status_agend IN ({marcadores})) "
        f"SELECT * FROM numeradas WHERE posicao <= ? "
        " ORDER BY status_agend, posicao",
        tuple(rotulos) + (quantos,))

    totais = consultar(
        f"SELECT ({SQL_STATUS_AGEND}) AS status_agend, count(*), "
        "       coalesce(sum(valor_num), 0) "
        "  FROM analisesps.sps GROUP BY 1",
        ())
    por_status = {t[0]: (t[1], t[2]) for t in totais}

    nomes = CAMPOS_LISTA + ["status_agend", "risco", "cadastro_incompleto",
                            "vencido", "vence_hoje", "posicao"]
    agrupadas: dict = {r: [] for r in rotulos}
    for linha in linhas:
        registro = dict(zip(nomes, linha))
        agrupadas.setdefault(registro["status_agend"], []).append(registro)

    saida = []
    for rotulo in rotulos:
        quantidade, total = por_status.get(rotulo, (0, 0))
        minhas = agrupadas.get(rotulo, [])
        saida.append({"rotulo": rotulo, "linhas": minhas,
                      "quantidade": quantidade, "total": total,
                      "tem_mais": quantidade > len(minhas)})
    return saida


def opcoes(coluna: str, limite: int = 400) -> list[str]:
    """Os valores distintos de uma coluna, para montar as listas de filtro.

    Limitado de propósito: uma coluna com milhares de valores diferentes
    (credor, por exemplo) não cabe numa lista de seleção — para essas, a busca
    livre é o caminho certo."""
    permitidas = {"status_pgt", "conta", "forma_pagamento", "tipo_despesa",
                  "projeto", "responsavel", "centro_custo", "status_aut"}
    if coluna not in permitidas:
        raise ValueError(f"Coluna não permitida em filtro: {coluna}")
    from .db import consultar

    if coluna in MULTIPLAS_NA_CELULA:
        # A célula pode trazer MAIS DE UMA obra, separadas por vírgula
        # ("CONS, CRECHE SWAP"). Sem separar, a lista do filtro oferecia a
        # combinação inteira como se fosse uma obra — e a obra sozinha, que é
        # o que se procura, não aparecia em lugar nenhum.
        #
        # `unnest(string_to_array(...))` abre a célula em uma linha por obra;
        # o resto é o mesmo agrupamento de sempre.
        linhas = consultar(
            "SELECT obra, count(*) FROM ("
            "  SELECT btrim(unnest(regexp_split_to_array("
            f"           {coluna}, '{SEPARADOR_DE_OBRAS}'))) AS obra "
            f"    FROM analisesps.sps WHERE trim(coalesce({coluna},'')) <> ''"
            ") AS abertas WHERE obra <> '' "
            " GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ?", (limite,))
        return [linha[0] for linha in linhas]

    linhas = consultar(
        f"SELECT trim({coluna}), count(*) FROM analisesps.sps "
        f" WHERE trim(coalesce({coluna},'')) <> '' "
        f" GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT ?", (limite,))
    return [linha[0] for linha in linhas]


# ---------------------------------------------------------------------------
# AS LISTAS DE FILTRO, GUARDADAS ATÉ A PRÓXIMA CARGA
#
# Medido nesta máquina, com as 59.055 SPs de verdade: montar as sete listas
# custa 194 ms, e era isso a CADA clique no filtro. Cada uma varre a tabela
# inteira para descobrir quais valores existem naquela coluna, e o índice não
# ajuda — a consulta limpa o texto antes de agrupar, e aí o banco lê tudo.
# Índice de expressão foi tentado e o Postgres continuou preferindo a varredura;
# não é caminho.
#
# O desperdício é que essas listas quase nunca mudam: os projetos, as contas e
# os tipos de despesa da empresa são os mesmos hoje e amanhã. Só mudam quando
# entra SP nova — ou seja, quando a carga da planilha roda.
#
# Então a chave do que fica guardado é O CARIMBO DA ÚLTIMA SINCRONIZAÇÃO. Ele
# muda, as listas são refeitas; não muda, valem as de antes. Funciona ENTRE
# PROCESSOS sem combinação nenhuma: a carga roda num processo separado e não
# tem como avisar este, mas o carimbo que ela grava no banco é o próprio aviso.
#
# O CUSTO, dito na cara: um projeto novo cadastrado na planilha só aparece na
# listinha depois da próxima sincronização (a tela dispara uma a cada 5 min).
# A SP nova aparece na LISTA normalmente — é só o menu de filtro que demora a
# saber do valor novo.
# ---------------------------------------------------------------------------
COLUNAS_DE_FILTRO = {
    "status_pgt": ("status_pgt", 400),
    "conta": ("conta", 400),
    "forma": ("forma_pagamento", 400),
    "tipo_despesa": ("tipo_despesa", 400),
    "projeto": ("projeto", 400),
    "responsavel": ("responsavel", 400),
    "centro_custo": ("centro_custo", 200),
}

# Trocado inteiro a cada recálculo, nunca alterado no lugar: com 4 threads no
# mesmo processo, duas podem recalcular ao mesmo tempo — e trocar a referência
# de uma vez faz com que a pior consequência disso seja trabalho repetido, e
# nunca uma lista pela metade na tela.
_LISTAS_GUARDADAS: dict = {"carimbo": object(), "valores": {}}


def opcoes_de_filtro(carimbo=None) -> dict:
    """As sete listas da barra lateral, de uma vez.

    `carimbo` é o valor de `ultima_sincronizacao` — quem chama normalmente já
    o tem em mãos (veio do `base_carregada()`), e passá-lo evita uma consulta
    a mais só para descobrir se o que está guardado ainda serve."""
    if carimbo is None:
        from .db import consultar_um
        try:
            linha = consultar_um("SELECT valor FROM analisesps.meta "
                                 "WHERE chave = 'ultima_sincronizacao'")
            carimbo = linha[0] if linha else ""
        except Exception:  # noqa: BLE001 — sem carimbo, recalcula; não quebra
            carimbo = None

    guardado = _LISTAS_GUARDADAS
    if guardado["carimbo"] == carimbo and guardado["valores"]:
        return dict(guardado["valores"], status_agend=opcoes_agendamento())

    valores = {apelido: opcoes(coluna, limite=limite)
               for apelido, (coluna, limite) in COLUNAS_DE_FILTRO.items()}
    _substituir_listas(carimbo, valores)
    return dict(valores, status_agend=opcoes_agendamento())


def _substituir_listas(carimbo, valores) -> None:
    global _LISTAS_GUARDADAS
    _LISTAS_GUARDADAS = {"carimbo": carimbo, "valores": valores}


def esquecer_opcoes_de_filtro() -> None:
    """Joga fora o que está guardado. Para os testes e para quem mexer na
    estrutura sem passar por uma sincronização."""
    _substituir_listas(object(), {})


def opcoes_agendamento() -> list[str]:
    """Os valores possíveis do status de agendamento — lista fixa, curta, e na
    ordem em que o operador pensa neles."""
    return ["Agendar", "Agendado", "Verificar", "Falha Agendar", "Sem Agendamento"]


def base_carregada() -> dict:
    """Quantas SPs existem e quando foi a última sincronização.

    Serve para a tela dizer "a base ainda não foi carregada" em vez de mostrar
    uma lista vazia como se não houvesse nada a pagar.

    `desconhecida` separa dois estados que parecem iguais e não são: a base
    VAZIA (a pergunta foi feita, e a resposta é zero) e a base que NÃO DEU
    PARA CONSULTAR (banco fora do ar, ou estrutura ainda não criada). Dizer
    "vazia" no segundo caso é afirmar o que não se sabe — e foi assim que a
    tela de Configurações chegou a informar "o banco está em dia" justamente
    quando não conseguia falar com ele."""
    from .db import consultar, conexao

    # CONTAR AS SPs UMA VEZ POR SINCRONIZAÇÃO, E NÃO UMA VEZ POR TELA.
    #
    # `count(*)` no Postgres percorre a tabela inteira — e esta função é
    # chamada em TODA tela, só para saber se a base foi carregada e para
    # escrever "de 59.055 na base" embaixo do total.
    #
    # Na produção isso apareceu medido pelo dono em 09/09/2026: a rotina que
    # só pergunta a hora da base levou 1,4 segundo, e ela não fazia nada além
    # desta contagem. Aqui, com a mesma quantidade de SPs, custa 5 ms — a
    # diferença é o banco de lá, que recebe a base inteira reescrita a cada
    # carga e acumula linhas mortas até o faxineiro do Postgres passar.
    #
    # O número só muda quando a base é carregada ou sincronizada, e as duas
    # coisas deixam a HORA registrada. Então guardamos a contagem junto da
    # hora a que ela se refere: enquanto a hora for a mesma, o número vale, e
    # nenhuma tela precisa percorrer a tabela. Quando a hora muda, conta-se de
    # novo, uma vez, e guarda-se outra vez.
    #
    # O LIMITE, e é honesto dizê-lo: se alguém acrescentar ou apagar linhas
    # POR FORA da carga e da sincronização, o número fica velho até a próxima.
    # Hoje ninguém faz isso — a fila de volta altera SPs que já existem, não
    # cria nem remove.
    try:
        guardado = {c: v for c, v in consultar(
            "SELECT chave, valor FROM analisesps.meta "
            " WHERE chave IN ('ultima_sincronizacao', 'quantidade', "
            "                 'quantidade_em')")}
    except Exception:  # noqa: BLE001 — estrutura ainda não criada
        return {"pronta": False, "quantidade": 0, "ultima": None,
                "desconhecida": True}

    ultima = guardado.get("ultima_sincronizacao") or None

    if ultima and guardado.get("quantidade_em") == ultima:
        try:
            quantas = int(guardado.get("quantidade") or 0)
        except (TypeError, ValueError):
            quantas = -1
        if quantas >= 0:
            return {"pronta": quantas > 0, "quantidade": quantas,
                    "ultima": ultima, "desconhecida": False}

    try:
        linha = consultar("SELECT count(*) FROM analisesps.sps")
        quantas = linha[0][0] if linha else 0
    except Exception:  # noqa: BLE001 — tabela ainda não criada
        return {"pronta": False, "quantidade": 0, "ultima": ultima,
                "desconhecida": True}

    if ultima:
        try:
            with conexao() as conn:
                for chave, valor in (("quantidade", str(quantas)),
                                     ("quantidade_em", ultima)):
                    conn.execute(
                        "INSERT INTO analisesps.meta (chave, valor) "
                        "VALUES (?, ?) ON CONFLICT (chave) DO UPDATE "
                        "SET valor = EXCLUDED.valor", (chave, valor))
                conn.commit()
        except Exception:  # noqa: BLE001 — não conseguir guardar só custa lentidão
            logger.exception("Análise de SPs: falhou guardar a contagem da base")

    return {"pronta": quantas > 0, "quantidade": quantas, "ultima": ultima,
            "desconhecida": False}


# ---------------------------------------------------------------------------
# RELATÓRIO
#
# As mesmas contas do `relatorio.py` do Streamlit, feitas pelo banco. Lá elas
# rodavam sobre o DataFrame inteiro na memória; aqui cada uma é uma consulta
# que devolve dezenas de linhas, não dezenas de milhares.
#
# Uma regra vale para o relatório todo e vem do original: CANCELADAS FICAM DE
# FORA. Uma SP cancelada não é despesa, e somá-la inflaria todo total.
# ---------------------------------------------------------------------------
SEM_CANCELADAS = "lower(trim(coalesce(status_pgt,''))) <> 'cancelado'"

# As dimensões que o relatório sabe quebrar. É uma lista fechada de propósito:
# o nome da coluna entra no texto do SQL, então ele não pode vir de fora.
DIMENSOES = {
    "projeto": "Projeto",
    "centro_custo": "Centro de Custo",
    "tipo_despesa": "Tipo de Despesa",
    "conta": "Conta",
    "responsavel": "Responsável",
    "status_pgt": "Status de Pagamento",
    "forma_pagamento": "Forma de Pagamento",
}

VAZIO = "(vazio)"

# Os três recortes do relatório, e a data que manda em cada um.
TIPOS = {
    "geral": "Visão geral",
    "pagar": "Contas a pagar",
    "pagas": "Contas pagas",
}

PERIODOS = {"tudo": "Todo o período", "semana": "Esta semana", "mes": "Este mês"}


def _where_relatorio(f: dict, tipo: str) -> tuple[str, list]:
    """O filtro da barra lateral, mais o recorte do relatório.

    `tipo` escolhe o universo e, com ele, a data que importa: contas a pagar se
    olham pelo VENCIMENTO; contas pagas, pela DATA DO PAGAMENTO. Misturar as
    duas dá um total que não fecha com nada."""
    onde, params = _condicoes(f)
    onde.append(SEM_CANCELADAS)

    if tipo == "pagar":
        onde.append("lower(trim(coalesce(status_pgt,''))) = 'pagar'")
    elif tipo == "pagas":
        onde.append("lower(trim(coalesce(status_pgt,''))) = 'pago'")

    return " WHERE " + " AND ".join(onde), params


def coluna_de_data(tipo: str) -> str:
    """Qual data manda em cada recorte. Ver `_where_relatorio`."""
    return "data_pagamento_d" if tipo == "pagas" else "vencimento_d"


def _periodo(tipo: str, periodo: str) -> str:
    """O atalho de período: esta semana, este mês, ou tudo.

    Calculado com a data de Brasília, não com a do servidor — senão, entre 21h
    e meia-noite, "este mês" viraria o mês seguinte.

    `date_trunc('week')` do Postgres começa na SEGUNDA, que é como a semana é
    contada no original (`hoje.weekday()`)."""
    coluna = coluna_de_data(tipo)
    if periodo == "semana":
        return (f" AND {coluna} >= date_trunc('week', {SQL_HOJE})::date"
                f" AND {coluna} <= (date_trunc('week', {SQL_HOJE})"
                " + interval '6 days')::date")
    if periodo == "mes":
        return (f" AND {coluna} >= date_trunc('month', {SQL_HOJE})::date"
                f" AND {coluna} < (date_trunc('month', {SQL_HOJE})"
                " + interval '1 month')::date")
    return ""


def numeros_do_relatorio(f: dict, tipo: str = "geral",
                         periodo: str = "tudo") -> dict:
    """Total, quantidade, ticket médio e vencidos — numa consulta só."""
    from .db import consultar_um
    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    linha = consultar_um(
        "SELECT count(*), coalesce(sum(valor_num),0), "
        f"       count(*) FILTER (WHERE vencimento_d < {SQL_HOJE}), "
        "       coalesce(sum(valor_num) FILTER "
        f"               (WHERE vencimento_d < {SQL_HOJE}), 0) "
        f"  FROM analisesps.sps{where}{recorte}", tuple(params))
    if not linha:
        return {"quantidade": 0, "total": 0, "ticket": 0,
                "vencidos_qtd": 0, "vencidos_total": 0}
    quantidade, total = linha[0], linha[1]
    return {
        "quantidade": quantidade,
        "total": total,
        "ticket": (total / quantidade) if quantidade else 0,
        "vencidos_qtd": linha[2],
        "vencidos_total": linha[3],
    }


def agregar(f: dict, dimensao: str, tipo: str = "geral", periodo: str = "tudo",
            limite: int = 30) -> list[dict]:
    """Soma por uma dimensão, da maior para a menor.

    Valor em branco vira "(vazio)" em vez de sumir — é assim no original, e é o
    certo: uma despesa sem centro de custo continua sendo despesa, e escondê-la
    faria a soma das partes não bater com o total."""
    if dimensao not in DIMENSOES:
        raise ValueError(f"Dimensão não permitida no relatório: {dimensao}")
    from .db import consultar
    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    linhas = consultar(
        f"SELECT CASE WHEN trim(coalesce({dimensao},'')) = '' THEN ? "
        f"            ELSE trim({dimensao}) END AS rotulo, "
        "        count(*), coalesce(sum(valor_num),0) "
        f"  FROM analisesps.sps{where}{recorte} "
        "  GROUP BY 1 ORDER BY 3 DESC, 1 LIMIT ?",
        (VAZIO,) + tuple(params) + (limite,))
    return [{"rotulo": r[0], "quantidade": r[1], "total": r[2]} for r in linhas]


def agregar_varias(f: dict, dimensoes: list, tipo: str = "geral",
                   periodo: str = "tudo", limite: int = 100) -> dict:
    """Várias dimensões de uma vez, NUMA VARREDURA SÓ do banco.

    O Relatório soma por projeto, por obra, por tipo de despesa e por conta —
    quatro perguntas sobre EXATAMENTE as mesmas linhas. Separadas, eram quatro
    varreduras das 59 mil SPs, ~41 ms cada; medido, elas eram a maior parte dos
    331 ms da tela.

    `GROUPING SETS` é a resposta que o Postgres já tem para isto: ele percorre
    a tabela uma vez e devolve os quatro agrupamentos juntos, marcando a qual
    deles cada linha pertence. A ordenação e o corte de cada lista continuam
    sendo feitos aqui, sobre poucas dezenas de linhas.

    Devolve {dimensao: [{rotulo, quantidade, total}, ...]}, cada lista já
    ordenada do maior total para o menor — igual ao que `agregar` devolvia."""
    from .db import consultar

    pedidas = [d for d in dict.fromkeys(dimensoes) if d in DIMENSOES]
    if not pedidas:
        return {}
    if len(pedidas) == 1:
        # Uma só não tem o que agrupar junto; o caminho simples é mais barato.
        return {pedidas[0]: agregar(f, pedidas[0], tipo, periodo, limite)}

    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)

    # O rótulo de cada dimensão, na ordem pedida. O "(vazio)" entra como
    # parâmetro, como em `agregar`.
    rotulos = [f"CASE WHEN trim(coalesce({d},'')) = '' THEN ? "
               f"     ELSE trim({d}) END" for d in pedidas]
    # Os conjuntos de agrupamento: um por dimensão, pela posição no SELECT.
    conjuntos = ", ".join(f"({i + 1})" for i in range(len(pedidas)))
    # `GROUPING` diz, em cada linha do resultado, quais dimensões estão
    # agregadas — é como se sabe de qual das listas aquela linha é.
    marcas = ", ".join(f"GROUPING({r})" for r in rotulos)

    # A ORDEM DOS PARÂMETROS SEGUE A ORDEM DO TEXTO DO SQL, e não a ordem em
    # que a gente pensa nas partes. No texto vêm primeiro os CASE do SELECT,
    # LOGO EM SEGUIDA os mesmos CASE dentro de GROUPING(...), e só então o
    # WHERE. Trocar as duas últimas foi o defeito de 09/09: com filtro sem
    # valor nenhum as duas ordens coincidiam e a tela abria; bastava filtrar
    # por qualquer coisa para os CASE do GROUPING receberem o valor do filtro,
    # deixarem de ser idênticos aos do SELECT, e o banco recusar a consulta.
    linhas = consultar(
        "SELECT " + ", ".join(rotulos) + ", " + marcas
        + ", count(*), coalesce(sum(valor_num),0) "
        f"  FROM analisesps.sps{where}{recorte} "
        f" GROUP BY GROUPING SETS ({conjuntos})",
        tuple([VAZIO] * len(pedidas))          # os CASE do SELECT
        + tuple([VAZIO] * len(pedidas))        # os mesmos CASE no GROUPING
        + tuple(params))                       # o WHERE, que vem depois

    quantas = len(pedidas)
    saida: dict = {d: [] for d in pedidas}
    for linha in linhas:
        valores = linha[:quantas]
        agregadas = linha[quantas:quantas * 2]
        quantidade, total = linha[-2], linha[-1]
        # A dimensão desta linha é a única que NÃO está agregada (marca 0).
        for i, marca in enumerate(agregadas):
            if marca == 0:
                saida[pedidas[i]].append({"rotulo": valores[i],
                                          "quantidade": quantidade,
                                          "total": total})
                break

    for dimensao, lista in saida.items():
        lista.sort(key=lambda x: (-x["total"], x["rotulo"]))
        saida[dimensao] = lista[:limite]
    return saida


def top_credores(f: dict, tipo: str = "geral", periodo: str = "tudo",
                 limite: int = 30) -> list[dict]:
    """Os maiores credores, agrupados por CPF/CNPJ — não pelo nome.

    O agrupamento por documento é o do original, e existe porque o mesmo credor
    aparece escrito de vários jeitos ("ACME LTDA", "Acme Ltda ME"). Somar por
    nome partiria o mesmo fornecedor em três."""
    from .db import consultar
    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    linhas = consultar(
        "SELECT CASE WHEN trim(coalesce(documento,'')) = '' THEN ? "
        "            ELSE trim(documento) END AS doc, "
        # O nome exibido é o mais frequente naquele documento — mesma escolha
        # do original, que usava a moda.
        "       mode() WITHIN GROUP (ORDER BY trim(coalesce(credor,''))), "
        "       count(*), coalesce(sum(valor_num),0) "
        f"  FROM analisesps.sps{where}{recorte} "
        "  GROUP BY 1 ORDER BY 4 DESC, 2 LIMIT ?",
        ("(sem CPF/CNPJ)",) + tuple(params) + (limite,))
    return [{"documento": r[0], "credor": r[1], "quantidade": r[2], "total": r[3]}
            for r in linhas]


# Quantas linhas do analítico cabem num relatório. Ver `analitico_do_relatorio`.
ANALITICO_MAXIMO = 2000


def analitico_do_relatorio(f: dict, tipo: str = "geral", periodo: str = "tudo",
                           limite: int = 0) -> list[dict]:
    """Lançamento a lançamento: o que está POR TRÁS dos totais do relatório.

    Pedido do dono em 18/09/2026: *"queria que no relatório em PDF saísse mais
    abaixo o analítico. Está bom do jeito que está, mas falta a parte
    analítica: o lançamento, credor e a descrição com detalhe do que é."*

    ⚠️ O RECORTE É O MESMO DOS TOTAIS — `_where_relatorio` e `_periodo`, as
    mesmas funções que a contagem e as quebras usam. Isto não é zelo: se o
    detalhe filtrasse por um critério e o total por outro, a soma das linhas
    não fecharia com o número do topo, **na mesma folha**. Quem conferisse não
    teria como saber qual dos dois está certo, e o relatório inteiro perderia a
    credibilidade por causa da parte que deveria prová-lo.

    ⚠️ TEM TETO, e ele é honesto. A base tem 59 mil SPs; um relatório sem
    filtro viraria um PDF de centenas de páginas que ninguém abre e que come a
    memória do serviço ao ser montado. São 2.000 linhas, das maiores para as
    menores — e quem chama AVISA na folha quando o teto cortou, porque um
    analítico truncado em silêncio é pior do que analítico nenhum: quem soma as
    linhas não encontra o total e conclui que a conta está errada.

    A ordem é por valor, da maior para a menor: cortando em 2.000, o que fica
    de fora é o miúdo, não a despesa que interessa."""
    from .db import consultar

    where, params = _where_relatorio(f, tipo)
    recorte = _periodo(tipo, periodo)
    limite = int(limite or ANALITICO_MAXIMO)
    linhas = consultar(
        "SELECT id, "
        # A data que importa muda com o tipo de relatório, e é a MESMA que o
        # período recorta: no relatório de pagas conta o pagamento; nos outros,
        # o vencimento. Mostrar vencimento num relatório de pagas faria a
        # coluna não explicar por que aquela linha entrou.
        + ("data_pagamento_d" if tipo == "pagas" else "vencimento_d") + ", "
        "       trim(coalesce(credor,'')), trim(coalesce(documento,'')), "
        "       trim(coalesce(centro_custo,'')), "
        "       trim(coalesce(tipo_despesa,'')), "
        "       trim(coalesce(descricao,'')), coalesce(valor_num, 0) "
        f"  FROM analisesps.sps{where}{recorte} "
        "  ORDER BY coalesce(valor_num,0) DESC, id LIMIT ?",
        tuple(params) + (limite,))
    return [{"id": r[0], "data": r[1], "credor": r[2], "documento": r[3],
             "centro_custo": r[4], "tipo_despesa": r[5], "descricao": r[6],
             "valor": r[7]} for r in linhas]


# As faixas de atraso do original, na mesma ordem e com os mesmos limites.
FAIXAS_ATRASO = [(1, 7, "1 a 7 dias"), (8, 15, "8 a 15 dias"),
                 (16, 30, "16 a 30 dias"), (31, 60, "31 a 60 dias"),
                 (61, 90, "61 a 90 dias"), (91, None, "mais de 90 dias")]


def aging_vencidos(f: dict, periodo: str = "tudo") -> list[dict]:
    """Quanto está atrasado, e há quanto tempo. Só o que está a pagar.

    A conta do atraso usa o dia de Brasília. Uma SP que vence HOJE não está
    atrasada — o original exige atraso maior que zero, e a tradução mantém.

    As faixas entram no texto do SQL, e não como parâmetro, porque são números
    fixos escritos aqui — nunca chegam de fora."""
    from .db import consultar
    where, params = _where_relatorio(f, "pagar")
    recorte = _periodo("pagar", periodo)

    casos = []
    for inicio, fim, nome in FAIXAS_ATRASO:
        ate = f" AND atraso <= {int(fim)}" if fim else ""
        casos.append(f"WHEN atraso >= {int(inicio)}{ate} THEN '{nome}'")

    linhas = consultar(
        "SELECT faixa, count(*), coalesce(sum(valor_num),0) FROM ("
        f"  SELECT valor_num, CASE {' '.join(casos)} END AS faixa FROM ("
        f"    SELECT valor_num, ({SQL_HOJE} - vencimento_d) AS atraso "
        f"      FROM analisesps.sps{where}{recorte}"
        "  ) AS com_atraso WHERE atraso > 0"
        ") AS por_faixa WHERE faixa IS NOT NULL GROUP BY 1",
        tuple(params))

    achados = {r[0]: {"faixa": r[0], "quantidade": r[1], "total": r[2]}
               for r in linhas}
    return [achados[nome] for _, _, nome in FAIXAS_ATRASO if nome in achados]


# ===========================================================================
# A TELA DE VER — "similar ao que eu visualizo na planilha"
#
# Cobrança do dono em 13/09/2026, e ela é antiga: *"desde o começo eu pedi uma
# tela simples pra poder visualizar similar ao que eu visualizo na planilha.
# Uma tela das notas e outra tela dos registros com os dados que estamos
# trabalhando. Similar à planilha. Mas até agora não foi entregue."*
#
# ELE ESTÁ CERTO, E O QUE FALTAVA NÃO ERA DADO — era a TELA. Tudo o que este
# módulo tem são telas de TRABALHO: cada uma mostra um recorte, com painel,
# proposta, botão de agir. Nenhuma responde à pergunta mais simples que
# existe, que é *"deixa eu ver os dados"*.
#
# O QUE FAZ ESTA TELA SER "A PLANILHA" e não mais uma tela de trabalho:
#
#   1. TODAS AS COLUNAS, na ORDEM DA PLANILHA (A, B, C…), e não na ordem de
#      uso. Ele lê a SPsBD por posição — a coluna "O" é o Status Pgt, e ele
#      sabe disso de cor.
#   2. A LETRA DA COLUNA no cabeçalho. É o detalhe que faz reconhecer.
#   3. NADA DE AÇÃO. Sem propor, sem confirmar, sem marcar. Olhar não é mexer.
#   4. Uma busca só, e ordenar clicando no cabeçalho — como numa planilha.
#
# NÃO REUSA `listar`: aquela traz um punhado de colunas escolhidas e ainda
# calcula risco, atraso e agendamento por linha. Aqui é o contrário — tudo, e
# sem conta nenhuma por cima.
# ===========================================================================
# As colunas da planilha, na ordem dela, tirando as fórmulas que não guardamos.
COLUNAS_DA_PLANILHA = [
    (c.chave, c.letra, c.rotulo, c.tipo) for c in colunas.GUARDADAS]

# Quantas linhas por página. O mesmo teto das outras telas: 200 linhas é o que
# o navegador desenha sem engasgar, e a base tem 59 mil.
POR_PAGINA_PLANILHA = 200


def planilha_sps(busca: str = "", ordem: str = "id", desc: bool = False,
                 pagina: int = 1, tudo: bool = False) -> tuple[list, int]:
    """As SPs como a planilha mostra: todas as colunas, na ordem dela.

    ⚠️ SÓ DE 2026 EM DIANTE, por padrão — e isto mudou de ideia por decisão
    DELE, em 14/09/2026:

    *"Lembra que eu fiz um filtro pra exibir lá na parte do confronto só o que
    é vencimento em 2026 ou pago em 2026? Então eu quero que você aplique esse
    mesmo filtro lá. Porque só me interessa 2026, porque é o lucro real; antes
    era lucro presumido, então não preciso dessa informação."*

    Quando esta tela nasceu eu deixei o escopo de fora de propósito, com teste
    e tudo: o argumento era que "a planilha mostra a planilha", e esconder
    linhas faria a conta dele não fechar com a SPsBD. O argumento estava certo
    no geral e ERRADO no caso dele — o que ele confere é 2026, porque é o que
    o regime tributário torna relevante. Quem decide isso é ele.

    `tudo=True` traz o resto de volta, e a tela tem a caixa: esconder sem volta
    seria trocar um problema por outro."""
    from .db import consultar, consultar_um

    onde, params = [], []
    if not tudo:
        # O MESMO CORTE DE ANO da Documentação Fiscal, e sai do MESMO lugar —
        # duas cópias divergiriam no dia em que o ano mudasse.
        onde.append(SQL_ANO_FISCAL)
        params.extend([ANO_FISCAL_MINIMO, ANO_FISCAL_MINIMO])
    termo = str(busca or "").strip()
    if termo:
        # A MESMA BUSCA LIVRE DAS OUTRAS TELAS, para não haver duas ideias de
        # "procurar" no mesmo módulo.
        alvo = " || ' ' || ".join(f"lower(coalesce({c},''))" for c in CAMPOS_BUSCA)
        for pedaco in [t.strip().lower() for t in termo.split(",") if t.strip()]:
            onde.append(f"({alvo}) LIKE ?")
            params.append(f"%{_como_texto_literal(pedaco)}%")
    where = (" WHERE " + " AND ".join(onde)) if onde else ""

    # ⚠️ A ORDENAÇÃO SAI DE UMA LISTA FECHADA, e nunca do que vem no endereço:
    # costurar o nome da coluna dentro do SQL é o caminho conhecido para
    # alguém mandar comando pela barra do navegador.
    permitidas = {c for c, _, _, _ in COLUNAS_DA_PLANILHA}
    coluna = ordem if ordem in permitidas else "id"
    # Data e valor ordenam pela versão CONVERTIDA: ordenar "10/01/2026" como
    # texto põe outubro antes de fevereiro, e aí a tela mente.
    real = colunas.DERIVADAS_DATA.get(coluna, coluna)
    if coluna == colunas.DERIVADA_VALOR[0]:
        real = colunas.DERIVADA_VALOR[1]
    sentido = "DESC NULLS LAST" if desc else "ASC NULLS LAST"

    pagina = max(1, int(pagina or 1))
    campos = ", ".join(f'"{c}"' for c, _, _, _ in COLUNAS_DA_PLANILHA)
    linhas = consultar(
        f'SELECT {campos} FROM analisesps.sps{where} '
        f' ORDER BY "{real}" {sentido}, id '
        " LIMIT ? OFFSET ?",
        tuple(params) + (POR_PAGINA_PLANILHA,
                         (pagina - 1) * POR_PAGINA_PLANILHA))
    total = consultar_um(
        f"SELECT count(*) FROM analisesps.sps{where}", tuple(params))[0]

    nomes = [c for c, _, _, _ in COLUNAS_DA_PLANILHA]
    return [dict(zip(nomes, linha)) for linha in linhas], int(total or 0)


# ===========================================================================
# O QUADRO POR CATEGORIA — quantas SPs e QUANTO DINHEIRO em cada uma
#
# Pedido do dono em 14/09/2026: *"a parte de KPI, pra eu saber quanto tem
# analisado, quanto não tem, quanto tem nota, quanto tem de contrato, quanto é
# fundo fixo, quanto está sem informação nenhuma — quanto isso em VALORES, né?
# Quanto está pra ser resolvido. E era interessante esse KPI direcionar pra
# uma tela com as informações: eu clicar e mostrar 'olha, essas aqui são as de
# fundo fixo', aí a lista."*
#
# ⚠️ O QUE ISTO TEM QUE OS TOTALIZADORES DE CIMA NÃO TÊM: **valor**. Os que já
# existiam contam SPs — "faltam 4.000" —, e 4.000 SPs de R$ 50,00 e 4.000 de
# R$ 50.000,00 são problemas de tamanhos completamente diferentes. Sem o
# dinheiro ao lado, o número não diz por onde começar, que é justamente a
# pergunta dele.
#
# UMA CONSULTA SÓ, agrupando pela categoria. Uma consulta por categoria seriam
# 22 varreduras da base num banco de um décimo de núcleo.
#
# E CADA LINHA É UM ATALHO: clicar filtra a lista por aquela categoria. Ver um
# número e ter de ir procurá-lo no filtro seria meio caminho — é a mesma regra
# dos totalizadores de cima.
# ===========================================================================
# O rótulo do "vazio". Não é uma categoria do Pipefy: é a ausência de uma, e é
# a pilha que interessa mais.
SEM_CATEGORIA = "(sem informação)"


def quadro_por_categoria(f: dict) -> list[dict]:
    """Por categoria de documentação: quantas SPs e quanto em dinheiro."""
    from .db import consultar

    where, params = _where(f)
    linhas = consultar(
        f"SELECT trim({SQL_DOC_FISCAL}) AS categoria, "
        "        count(*), coalesce(sum(valor_num), 0) "
        f"  FROM analisesps.sps{where} "
        " GROUP BY 1 ORDER BY 3 DESC", tuple(params))

    saida = []
    for categoria, quantas, valor in linhas:
        nome = (categoria or "").strip()
        saida.append({
            "categoria": nome,
            "rotulo": nome or SEM_CATEGORIA,
            "vazia": not nome,
            "quantas": int(quantas or 0),
            "valor": valor or 0,
        })
    return saida


# ===========================================================================
# O CALENDÁRIO — o mesmo filtro das Solicitações, desenhado dia a dia
#
# Pedido do dono em 23/09/2026: *"é bem interessante a gente conseguir
# visualizar no formato de calendário o que é que tem (…) e a gente pode
# atrelar esse calendário aos filtros que já estão atrelados ao próprio
# relatório e à tela de solicitações."*
#
# ⚠️ UMA CONSULTA SÓ, E SÓ O MÊS PEDIDO. A tentação aqui é trazer as SPs do
# mês e agrupar em Python — são 59 mil linhas na tabela, e o dia em que
# alguém abrir o calendário sem filtro nenhum isso vira a tela mais cara do
# módulo. Quem agrupa é o banco, e volta uma linha por DIA: no máximo 31.
#
# ⚠️ A DATA QUE MANDA DEPENDE DO RECORTE, e é a mesma regra do Relatório
# (`coluna_de_data`): contas a pagar se olham pelo VENCIMENTO, contas pagas
# pela DATA DO PAGAMENTO. Um calendário que misturasse as duas mostraria a
# mesma SP em dois dias e não fecharia com o Relatório do mesmo filtro — que
# é exatamente a conferência que o dono vai fazer.
# ===========================================================================
def calendario_do_mes(f: dict, primeiro, ultimo, tipo: str = "geral") -> dict:
    """Quantas SPs e quanto em dinheiro caem em cada dia do intervalo.

    Devolve `{"dias": {data: {...}}, "total": ..., "quantidade": ...}`. O
    intervalo vem pronto de fora porque a grade do mês inclui os dias
    vizinhos que completam a primeira e a última semana — e o dono espera ver
    o que cai neles também, senão o fim do mês anterior aparece vazio sem
    estar.

    `sem_data` conta o que entrou no filtro mas não tem a data que manda: uma
    SP sem vencimento não cai em dia nenhum, e sumir do calendário sem aviso
    faria a soma do mês não bater com a do Relatório."""
    from .db import consultar, consultar_um

    coluna = coluna_de_data(tipo)
    where, params = _where_relatorio(f, tipo)

    # As TRÊS SITUAÇÕES QUE GANHAM COR, pedido do dono em 23/09/2026: *"coloca
    # azul para pago, vermelho para vencido e laranja a vencer."*
    #
    # ⚠️ ELAS SAEM NA MESMA VARREDURA, com `FILTER`. Três consultas seriam três
    # passagens pela mesma tabela filtrada para responder à mesma pergunta — e
    # o custo desta tela é justamente o que impede que ela fique cara com 59
    # mil SPs.
    #
    # ⚠️ "VENCIDO" É SEMPRE PELO VENCIMENTO, mesmo no recorte das pagas, onde o
    # DIA do calendário conta pelo pagamento. São coisas diferentes: o dia diz
    # QUANDO aquilo aconteceu; a cor diz O QUE aconteceu.
    pago = "lower(trim(coalesce(status_pgt,''))) = 'pago'"
    a_pagar = "lower(trim(coalesce(status_pgt,''))) = 'pagar'"
    vencido = f"{a_pagar} AND vencimento_d < {SQL_HOJE}"
    a_vencer = f"{a_pagar} AND (vencimento_d >= {SQL_HOJE} OR vencimento_d IS NULL)"

    def soma(condicao):
        return (f"count(*) FILTER (WHERE {condicao}), "
                f"coalesce(sum(valor_num) FILTER (WHERE {condicao}), 0)")

    linhas = consultar(
        f"SELECT {coluna} AS dia, count(*), coalesce(sum(valor_num), 0), "
        f"       {soma(pago)}, {soma(vencido)}, {soma(a_vencer)} "
        f"  FROM analisesps.sps{where} "
        f"   AND {coluna} >= ? AND {coluna} <= ? "
        " GROUP BY 1 ORDER BY 1", tuple(params) + (primeiro, ultimo))

    dias = {}
    total = quantidade = 0
    mes_por_situacao = {s: {"quantidade": 0, "total": 0}
                        for s in ("pago", "vencido", "a_vencer", "outros")}
    for linha in linhas:
        (dia, quantas, valor, pago_q, pago_v, venc_q, venc_v,
         av_q, av_v) = linha
        quantas = int(quantas or 0)
        valor = valor or 0
        situacoes = {
            "pago": {"quantidade": int(pago_q or 0), "total": pago_v or 0},
            "vencido": {"quantidade": int(venc_q or 0), "total": venc_v or 0},
            "a_vencer": {"quantidade": int(av_q or 0), "total": av_v or 0},
        }
        # ⚠️ O RESTO É CALCULADO, NÃO CONSULTADO. Status fora das três (uma SP
        # com a coluna em branco, um valor novo que a planilha ganhe amanhã)
        # continuaria no total do dia e não apareceria em cor nenhuma — as
        # partes não somariam o todo, e ninguém veria isso na tela. Assim o
        # que sobra ganha um balde neutro e a soma sempre fecha.
        situacoes["outros"] = {
            "quantidade": quantas - sum(s["quantidade"] for s in situacoes.values()),
            "total": valor - sum(s["total"] for s in situacoes.values()),
        }
        dias[dia] = {"quantidade": quantas, "total": valor,
                     "vencidas": situacoes["vencido"]["quantidade"],
                     "situacoes": situacoes}
        total += valor
        quantidade += quantas
        for nome, s in situacoes.items():
            mes_por_situacao[nome]["quantidade"] += s["quantidade"]
            mes_por_situacao[nome]["total"] += s["total"]

    # O que o filtro alcança mas não tem a data que manda. Consulta própria e
    # barata (uma contagem), e a tela só a mostra quando não é zero.
    sem_data = consultar_um(
        f"SELECT count(*), coalesce(sum(valor_num), 0) "
        f"  FROM analisesps.sps{where} AND {coluna} IS NULL", tuple(params))

    return {
        "dias": dias,
        "total": total,
        "quantidade": quantidade,
        "situacoes": mes_por_situacao,
        "sem_data_qtd": int((sem_data or (0, 0))[0] or 0),
        "sem_data_total": (sem_data or (0, 0))[1] or 0,
        "coluna": coluna,
    }
