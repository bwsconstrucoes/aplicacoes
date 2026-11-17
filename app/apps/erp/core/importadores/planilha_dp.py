# -*- coding: utf-8 -*-
"""
A ABA "Dados Documentos" DA PLANILHA "Registro de Colaboradores" → CADASTRO.

⚑ POR QUE ESTE ARQUIVO EXISTE, e vale ler antes de mexer.

Em 16/11/2026 o dono cobrou, depois de três tentativas frustradas de a
importação preencher o cadastro:

    "Meu Deus do céu, socorro! É só casar informação da planilha vs campo do
     ERP. Tu não tá conseguindo. Já foram várias tentativas. Tu precisa fazer
     algo diferente. O que tu precisa de mim?"

E, logo depois, a pergunta que acabou com o problema:

    "Por que você não consulta a planilha? Você tem acesso."

**Ele estava certo nas duas.** Eu vinha escrevendo o de-para de cabeça: lia os
NOMES das colunas no código (verificáveis) e INVENTAVA os VALORES que elas
guardam. Valor inventado que não bate não dá erro — o campo fica vazio e cala.
Três rodadas dele para descobrir algo que uma leitura da planilha resolvia.

⚑ ESTE ARQUIVO FOI ESCRITO COM A PLANILHA ABERTA. Cada nome de coluna e cada
valor das listas abaixo foi lido da aba de verdade, com a contagem de quantas
pessoas usam cada um. Nada aqui é palpite.

⚑ E A PLANILHA TEM TUDO. Era a outra coisa que eu tinha errado: eu afirmara
que RG, nome do pai e da mãe, sexo, escolaridade, estado civil e endereço só
existiam no card do Pipefy. Estão todos aqui, nas 79 colunas da aba. **Do
Pipefy sobra só o que a planilha não tem: os DOCUMENTOS (anexos).** Foi
exatamente o que ele disse desde o começo.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

logger = logging.getLogger(__name__)

ABA = "Dados Documentos"


def _cru(texto: Any) -> str:
    """Sem acento, sem caixa, sem o que não for letra ou número.

    É o que faz "Segunda à Sexta" e "SEGUNDA A SEXTA" serem a mesma coisa —
    diferença de digitação não pode custar um campo.
    """
    # ⚑ "Nº" E "N°" SÃO A MESMA COISA PARA QUEM DIGITA, E ERAM DUAS AQUI —
    # e isto custou campos vazios em 17/11/2026 (nº da CTPS, série, UF e
    # emissão). O "º" é o ordinal masculino e o "°" é o sinal de grau: na tela
    # são indistinguíveis, mas o NFKD decompõe o primeiro em "o" e deixa o
    # segundo intacto, que depois cai fora por não ser letra. Resultado:
    # "Nº CTPS" virava NOCTPS e "N° CTPS" virava NCTPS — duas colunas
    # diferentes para os olhos do código, idênticas para os olhos de quem
    # preenche a planilha.
    #
    # Ninguém vai descobrir isso olhando a planilha, e é o tipo de defeito que
    # volta sempre que alguém redigitar um cabeçalho. Os dois passam a valer
    # "O", que é como as duas formas são lidas em voz alta.
    bruto = str(texto or "").replace("°", "º")
    sem = unicodedata.normalize("NFKD", bruto)
    sem = "".join(c for c in sem if not unicodedata.combining(c))
    return "".join(c for c in sem.upper() if c.isalnum())


def _texto(v: Any) -> str:
    return " ".join(str(v or "").split())


def _digitos(v: Any) -> str:
    return re.sub(r"\D", "", str(v or ""))


def _data(v: Any) -> Optional[str]:
    """A planilha escreve dia/mês/ano. Devolve ISO, ou None."""
    bruto = _texto(v)
    if not bruto:
        return None
    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(bruto[:10], formato).date().isoformat()
        except ValueError:
            continue
    return None


def _dinheiro(v: Any) -> Optional[Decimal]:
    """"1.995,20" → 1995.20. Zero vira None: a planilha usa 0,00 para "nada"."""
    bruto = _texto(v).replace("R$", "").strip()
    if not bruto:
        return None
    try:
        valor = Decimal(bruto.replace(".", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return None
    return valor if valor != 0 else None


def _sim(v: Any) -> Optional[bool]:
    c = _cru(v)
    if c == "SIM":
        return True
    if c in ("NAO", "N"):
        return False
    return None


# ===========================================================================
# AS COLUNAS, COM O NOME QUE ELAS TÊM NA PLANILHA DE VERDADE
#
# Lidas da aba em 16/11/2026. Onde há mais de um nome possível, o primeiro é o
# que está lá hoje — os outros são tolerância para quando alguém renomear.
# ===========================================================================
TEXTO_DIRETO: dict[str, tuple[str, ...]] = {
    "nome":             ("Nome Completo",),
    "matricula":        ("Matrícula",),
    "nome_mae":         ("Nome da Mãe",),
    "nome_pai":         ("Nome do Pai",),
    "naturalidade":     ("Naturalidade",),
    "naturalidade_uf":  ("UF (Naturalidade)",),
    "rg":               ("Documento de Identificação (RG, CNH ou CTPS)",),
    "rg_orgao":         ("Órgão Emissor (Documento de e Identificação)",
                         "Órgão Emissor (Documento de Identificação)"),
    "rg_uf":            ("UF (Documento de Identificação)",),
    "logradouro":       ("Endereço (Logradouro)",),
    "numero_endereco":  ("Número",),
    "bairro":           ("Bairro",),
    "uf":               ("UF (Endereço)",),
    "municipio":        ("Município",),
    # Os nomes alternativos não são enfeite: é uma planilha que gente digita,
    # e cabeçalho redigitado muda de forma. O primeiro é o que está lá hoje.
    "ctps_numero":      ("Nº CTPS", "Nº da CTPS", "Número da CTPS",
                         "Numero CTPS", "CTPS Nº"),
    "ctps_serie":       ("Série (CTPS)", "Série da CTPS", "Serie CTPS"),
    "ctps_uf":          ("UF (CTPS)", "UF da CTPS"),
    "observacao_auxilio":   ("Observação Ajuda de Custo",),
    "id_fortes":        ("ID Fortes",),
    "requerente":       ("Responsável pela Solicitação",),
    "pix_chave":        ("Chave Pix",),
    # ⚑ OS DOIS CAMPOS DO PJ, apontados por ele em 17/11/2026 — e os nomes das
    # colunas não têm NADA a ver com o que elas guardam, por isso ficam
    # anotados: a razão social do prestador PJ está em "Nome Credor", e o CNPJ
    # dele em "Pix CNPJ" (é o CNPJ que recebe o Pix, que é o mesmo da empresa
    # dele). Adivinhar pelo nome da coluna levaria aos campos errados.
    "razao_social_pj":  ("Nome Credor", "Razão Social"),
}
# Campos que são número com pontuação na planilha e só dígitos no cadastro.
SO_DIGITOS: dict[str, tuple[str, ...]] = {
    "cpf":      ("CPF (Cadastro de Pessoa Física)", "CPF Números"),
    "telefone": ("Telefone",),
    "cep":      ("CEP",),
    "pis":      ("Nº PIS", "Nº do PIS", "Numero PIS"),
    "cnpj_pj":  ("Pix CNPJ", "CNPJ (PJ)"),
}
DATAS: dict[str, tuple[str, ...]] = {
    "data_nascimento": ("Data de Nascimento",),
    "rg_emissao":      ("Data de Emissão (Documento de Identificação)",),
    "ctps_emissao":    ("Data de Emissão (CTPS)", "Emissão da CTPS",
                        "Data de Emissao (CTPS)"),
    "data_inicio":     ("Data de Início",),
    "admissao":        ("Data de Admissão",),
    "aviso_previo":    ("Data do Aviso Prévio",),
    "ultimo_dia":      ("Último dia Trabalhado",),
    "demissao":        ("Data de Saída",),
}
DINHEIRO: dict[str, tuple[str, ...]] = {
    "salario":            ("Salário",),
    "valor_diaria":       ("Valor Diária",),
    "valor_mes":          ("Valor do Mês",),
    "aux_alimentacao":    ("Valor Auxílio Alimentação",),
    "aux_transporte":     ("Valor Auxílio Transporte",),
    "valor_gratificacao": ("Valor da Gratificação",),
}
SIM_NAO: dict[str, tuple[str, ...]] = {
    "solicitar_salario_familia": ("Solicitar Salário Família?",),
    "recebe_gratificacao":       ("Recebe Gratificação Mensal",),
    "parcela_unica":             ("Recebe Parcela Única",),
    "recebe_via_sp":             ("Recebe via SP",),
    "cesta_basica_extra":        ("Cesta Básica Extra",),
}

# ---------------------------------------------------------------------------
# AS CAIXINHAS FECHADAS — com os valores REAIS, e quantas pessoas usam cada um
# (contagem da planilha em 16/11/2026, 3.668 linhas).
#
# ⚠️ A chave é o texto passado por `_cru`. Valor fora desta tabela NÃO entra e
# é RELATADO: o cadastro recusaria de qualquer jeito, e preencher errado numa
# caixinha é pior que deixar vazio.
# ---------------------------------------------------------------------------
LISTAS: dict[str, tuple[tuple[str, ...], dict[str, str]]] = {
    "vinculo": (("Tipo de Cadastro Radio horizontal", "Tipo"), {
        "CTPS": "CTPS",                           # 2.667 pessoas
        "PRESTADORDESERVICO": "PRESTADOR",        # 1.000 pessoas
    }),
    "tipo_contrato": (("Tipo de Contrato",), {
        "CLTTEMPODETERMINADO": "CLT_DETERMINADO",     # 1.859
        "CLTTEMPOINDETERMINADO": "CLT_INDETERMINADO",  # 817
        "AUTONOMORPA": "AUTONOMO_RPA",                # 922
        "AUTONOMOMENSALISTA": "AUTONOMO_MENSALISTA",  # 17
        "PJ": "PJ",                                   # 31
        "JOVEMAPRENDIZ": "JOVEM_APRENDIZ",            # 12
        "ESTAGIO": "ESTAGIO",                         # 5
        "PROLABORE": "PRO_LABORE",                    # 4
        "TEMPORARIO": "TEMPORARIO",
    }),
    "sexo": (("Sexo",), {"MASCULINO": "M", "FEMININO": "F"}),
    "estado_civil": (("Estado Civil",), {
        "SOLTEIROA": "SOLTEIRO",          # 2.723 — a planilha escreve "Solteiro (a)"
        "CASADOA": "CASADO",              # 569
        "UNIAOESTAVEL": "UNIAO_ESTAVEL",  # 307
        "DIVORCIADOA": "DIVORCIADO",      # 61
        "VIUVOA": "VIUVO",                # 7
    }),
    "escolaridade": (("Escolaridade",), {
        "ENSINOFUNDAMENTAL": "FUNDAMENTAL",   # 1.906
        "ENSINOMEDIO": "MEDIO",               # 1.493
        "EDUCACAOINFANTIL": "INFANTIL",       # 112
        "ENSINOSUPERIOR": "SUPERIOR",         # 81
        "NAOALFABETIZADO": "NAO_ALFABETIZADO",  # 73
        "POSGRADUACAO": "POS",                # 2
        "MESTRADO": "MESTRADO", "DOUTORADO": "DOUTORADO",
    }),
    "meio_transporte": (("Meio de Transporte para chegar à Obra?",), {
        "NENHUM": "NENHUM",                   # 2.577
        "TRANSPORTECOLETIVO": "COLETIVO",     # 436
        "MOTO": "MOTO",                       # 378
        "BICICLETA": "BICICLETA",             # 80
    }),
    # ⚠️ "Segunda à Sexta" vem com CRASE na planilha. `_cru` tira o acento, e é
    # por isso que a comparação é feita assim e não por igualdade de texto.
    "categoria_alimentacao": (("Categoria Auxílio Alimentação",), {
        "MES": "MES",                      # 121
        "MENSAL": "MENSAL",                # 34
        "SEGUNDAASEXTA": "SEG_SEX",        # 21
        "SEGUNDAAQUINTA": "SEG_QUI",       # 15
    }),
    "categoria_transporte": (("Categoria Auxílio Transporte",), {
        "MENSAL": "MENSAL",                           # 262
        "DIARIO": "DIARIO",                           # 65
        "MOTOCICLETA": "MOTOCICLETA",                 # 9
        "SEGUNDAESEXTA": "SEG_E_SEX",                 # 9
        "VALETRANSPORTE": "VALE_TRANSPORTE",          # 8
        "DIARIOEVALETRANSPORTE": "DIARIO_E_VT",       # 1
        "CARTAO": "CARTAO",                           # 1
        "MES": "MES", "VEICULO": "VEICULO", "BICICLETA": "BICICLETA",
    }),
    "pix_tipo": (("Chave Pix Tipo",), {
        "CPF": "CPF", "TELEFONE": "TELEFONE", "EMAIL": "EMAIL",
        "ALEATORIA": "ALEATORIA", "CNPJ": "CNPJ",
    }),
    # ⚑ A PLANILHA ESCREVE A FRASE INTEIRA, O ERP GUARDA A ABREVIAÇÃO
    # (17/11/2026). Ele: *"alguns campos as opções de seleção sequer existem no
    # ERP."* Era este: a planilha diz "45 dias, prorrogável por mais 45 dias" e
    # a caixinha do cadastro só tem "45+45 dias". O texto entrava cru, não
    # casava com opção nenhuma, e a caixinha aparecia VAZIA na ficha — com o
    # dado gravado por baixo. Pior que não importar: parece que não veio.
    "contrato_experiencia": (("Contrato de Experiência",), {
        "45DIASPRORROGAVELPORMAIS45DIAS": "45+45 dias",
        "30DIASPRORROGAVELPORMAIS60DIAS": "30+60 dias",
        "60DIASPRORROGAVELPORMAIS30DIAS": "60+30 dias",
        "30DIASPRORROGAVELPORMAIS30DIAS": "30+30 dias",
        "SEMCONTRATODEEXPERIENCIA": "Sem contrato de experiência",
        # As abreviações também passam, para o dia em que a planilha mudar.
        "4545DIAS": "45+45 dias", "3060DIAS": "30+60 dias",
        "6030DIAS": "60+30 dias", "3030DIAS": "30+30 dias",
    }),
}

# As colunas que viram LIGAÇÃO (cargo, convenção, obra, empresa): aqui sai o
# NOME, e quem casa com o cadastro é quem chama.
NOMES: dict[str, tuple[str, ...]] = {
    "cargo":    ("Cargo [ ]", "Cargo"),
    "convencao": ("Convenção",),
    "obra":     ("Código da Obra",),
    "empresa":  ("CNPJ Admissão",),
    "fase":     ("Fase Atual",),
    "centro_custo_contabil": ("Centro de Custo Contabilidade",),
}


def colunas_que_eu_uso() -> set[str]:
    """Todo nome de coluna que este de-para sabe ler, na forma crua.

    Serve para o relatório dizer o contrário: QUAIS colunas a planilha tem e
    eu não uso. Campo vazio tem duas causas que de fora são idênticas — a
    coluna está vazia, ou a coluna existe e eu não a reconheço. Sem esta
    lista, descobrir qual das duas custa uma rodada do dono, e já custou
    quatro.
    """
    usadas: set[str] = set()
    for tabela in (TEXTO_DIRETO, SO_DIGITOS, DATAS, DINHEIRO, SIM_NAO, NOMES):
        for nomes in tabela.values():
            usadas.update(_cru(n) for n in nomes)
    for nomes, _mapa in LISTAS.values():
        usadas.update(_cru(n) for n in nomes)
    return usadas


def _coluna(cabecalho: dict[str, int], nomes: tuple[str, ...]) -> Optional[int]:
    """O índice da primeira coluna encontrada, comparando sem acento e caixa."""
    for nome in nomes:
        i = cabecalho.get(_cru(nome))
        if i is not None:
            return i
    return None


def cabecalho_de(linha: list[str]) -> dict[str, int]:
    return {_cru(c): i for i, c in enumerate(linha) if _cru(c)}


def ler_linha(cabecalho: dict[str, int], linha: list[str]) -> dict[str, Any]:
    """Uma linha da planilha virando os campos do cadastro do ERP.

    Devolve também `_nomes` (cargo, convenção, obra, empresa, fase, centro de
    custo — para quem chama casar com os cadastros) e `_nao_entendi` (o que a
    planilha escreve e esta tabela não conhece).

    ⚑ VALOR QUE NÃO BATE É RELATADO, NUNCA CHUTADO. É a regra que nasceu do
    erro de 16/11/2026: de-para é palpite até o dado real confirmar, e palpite
    errado sem relatório é invisível.
    """
    def crua(nomes: tuple[str, ...]) -> str:
        i = _coluna(cabecalho, nomes)
        return _texto(linha[i]) if i is not None and i < len(linha) else ""

    saida: dict[str, Any] = {}
    nao_entendi: list[dict[str, str]] = []

    for campo, nomes in TEXTO_DIRETO.items():
        v = crua(nomes)
        if v:
            saida[campo] = v
    for campo, nomes in SO_DIGITOS.items():
        v = _digitos(crua(nomes))
        if v:
            saida[campo] = v
    for campo, nomes in DATAS.items():
        bruto = crua(nomes)
        if not bruto:
            continue
        v = _data(bruto)
        if v:
            saida[campo] = v
        else:
            nao_entendi.append({"coluna": nomes[0], "valor": bruto,
                                "campo": campo, "motivo": "não parece uma data"})
    for campo, nomes in DINHEIRO.items():
        v = _dinheiro(crua(nomes))
        if v is not None:
            saida[campo] = str(v)
    for campo, nomes in SIM_NAO.items():
        v = _sim(crua(nomes))
        if v is not None:
            saida[campo] = v
    for campo, (nomes, tabela) in LISTAS.items():
        bruto = crua(nomes)
        if not bruto:
            continue
        achado = tabela.get(_cru(bruto))
        if achado:
            saida[campo] = achado
        else:
            nao_entendi.append({"coluna": nomes[0], "valor": bruto,
                                "campo": campo,
                                "motivo": "não é uma das opções do cadastro"})

    # ===================================================================
    # PAGAMENTO: QUEM TEM "VALOR DO MÊS" É MENSALISTA, E ELE MANDA
    #
    # ⚑ Corrigido em 17/11/2026 com ele olhando o caso da Amanda, que tem as
    # duas coisas na planilha: *"no caso da Amanda, vai se sobrepor a
    # informação Valor do Mês (…) ela não tem diária mais, é dado antigo."*
    #
    # Minha regra anterior era "quem tem diária é diarista", e ela errava
    # exatamente nesse caso. Contado na planilha (3.667 linhas, 17/11/2026):
    #
    #   3.166  só diária          → diarista
    #      58  só valor do mês    → mensalista
    #      53  os DOIS            → mensalista; a diária é resto antigo
    #     390  nenhum dos dois    → mensalista (não há diária para pagar)
    #
    # Os 53 são o caso dela: a diária ficou na planilha de quando a pessoa era
    # diarista e ninguém apagou. Trazê-la poria um valor morto na ficha de
    # quem é pago por mês — e diária na ficha é o que a folha usa para pagar
    # dia de fim de semana.
    # ===================================================================
    if saida.get("vinculo"):
        por_mes = "valor_mes" in saida
        saida["mensalista"] = por_mes or "valor_diaria" not in saida
        if por_mes and "valor_diaria" in saida:
            nao_entendi.append({
                "coluna": "Valor Diária", "campo": "valor_diaria",
                "valor": saida.pop("valor_diaria"),
                "motivo": ("não entrou: esta pessoa tem Valor do Mês, então é "
                           "mensalista — a diária na planilha é dado antigo")})

    # ===================================================================
    # DUAS INCOERÊNCIAS QUE A PLANILHA TEM, E O QUE ELE DECIDIU (17/11/2026)
    #
    # Varri as 3.667 linhas atrás de situações discrepantes — a pedido dele:
    # *"você devia analisar outras situações discrepantes e me questionar"* —
    # e levei a lista. Estas duas mudam o que entra no cadastro.
    #
    # 1) "Tipo" × "Tipo de Contrato" SE CONTRADIZEM em 57 linhas (prestador
    #    com contrato CLT, ou CTPS com RPA/aprendiz). Ele: *"todos que são
    #    prestador de serviço, que não estão desligados, estão com o contrato
    #    correto"* — e a conta confirma: 48 das 57 são de gente DESLIGADA,
    #    dado velho. As 9 ativas são erro de digitação na planilha.
    #    Então o "Tipo" manda (é a coluna que a folha já usa), e a
    #    contradição é RELATADA em vez de sumir.
    #
    # 2) DATA DE SAÍDA PREENCHIDA quer dizer que a pessoa SAIU, qualquer que
    #    seja a fase. Ele: *"quem está em Solicitações DP pode estar com data
    #    de saída — é gente que já saiu e o cadastro não foi finalizado,
    #    aguardando documentação. E quem está com o cadastro concluído NÃO
    #    PODE ter data de saída."*
    #    Então: data de saída ⇒ desligado; e "Cadastro Concluído" com data de
    #    saída é incoerência da planilha e vai para a lista (são 6 hoje).
    # ===================================================================
    # ⚑ JOVEM APRENDIZ É CTPS, e eu não tinha previsto. Ele, em 17/11/2026,
    # olhando os 9 ativos que eu havia acusado de erro: *"CTPS e jovem
    # aprendiz na verdade estão corretos, esse é o certo. Não tinha previsto
    # esse tipo de contrato formal. Não é CLT mas é Jovem aprendiz."*
    # O contrato de aprendizagem é formal, com carteira assinada — por isso
    # casa com "Tipo = CTPS". Os outros quatro (CTPS com Autônomo RPA) ele
    # confirmou que estão errados mesmo e vai corrigir na planilha.
    # A lista vem do CADASTRO (`TIPOS_CONTRATO_CTPS`), não de uma cópia
    # escrita aqui: duas listas para a mesma regra divergem no primeiro mês.
    # "TEMPORARIO" entra à parte porque só existe em ficha antiga — ele não é
    # oferecido em cadastro novo, mas a planilha ainda tem gente com ele.
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    # =======================================================================
    # ⚑ A FASE DA PLANILHA VIRA A ETAPA DO ERP — e isto é CONSERTO (17/11/2026)
    #
    # Ele, com razão e pela segunda vez: *"Veja as coisas que cansam muito. Eu
    # já havia falado sobre isso: não foi feito correlação entre fase e etapa."*
    #
    # A correlação existia desde 16/11 e estava CERTA — mas ligada só na
    # importação pelo Pipefy. A importação pela planilha, que nasceu um dia
    # depois e virou o caminho principal, lia a coluna "Fase Atual" apenas
    # para a regra da data de saída e jogava o resto fora. Ou seja: o dado
    # estava na planilha, a tradução estava escrita, e ninguém as ligava.
    #
    # Uma fonte só (`etapa_da_fase`, no cadastro) para as duas importações —
    # duas cópias da mesma correlação divergiriam na primeira mudança.
    # =======================================================================
    fase_escrita = crua(NOMES["fase"])
    if fase_escrita:
        etapa, situacao_da_fase = svc_pessoas.etapa_da_fase(fase_escrita)
        if etapa:
            saida["etapa"] = etapa
            saida["situacao"] = situacao_da_fase
        else:
            nao_entendi.append({
                "coluna": "Fase Atual", "campo": "Etapa", "valor": fase_escrita,
                "motivo": "não sei para que etapa do ERP esta fase vai"})

    CONTRATO_COM_CARTEIRA = svc_pessoas.TIPOS_CONTRATO_CTPS + ("TEMPORARIO",)
    contrato_e_de_prestador = (saida.get("tipo_contrato")
                               not in CONTRATO_COM_CARTEIRA)
    if saida.get("vinculo") and saida.get("tipo_contrato"):
        bate = ((saida["vinculo"] == "PRESTADOR") == contrato_e_de_prestador)
        if not bate:
            nao_entendi.append({
                "coluna": "Tipo × Tipo de Contrato", "campo": "vinculo",
                "valor": f'{crua(LISTAS["vinculo"][0])} com '
                         f'{crua(LISTAS["tipo_contrato"][0])}',
                "motivo": ("as duas colunas se contradizem — vale o “Tipo”, "
                           "que é o que a folha usa; confira na planilha")})

    # ⚑ RAZÃO SOCIAL E CNPJ DO PJ SÓ PARA QUEM É PJ (17/11/2026).
    #
    # As colunas são "Nome Credor" e "Pix CNPJ" — nomes que não dizem que o
    # dado é do PJ. Se a planilha preencher "Nome Credor" para todo mundo (e
    # "credor" é quem recebe, o que vale para qualquer pessoa), gravar isso em
    # "Razão social (PJ)" daria empresa a quem não tem empresa — e aí o
    # cadastro de um CLT afirmaria duas coisas que se contradizem.
    #
    # Decisão: quem não é PJ não recebe esses dois campos. Se a planilha só os
    # preenche para PJ, esta guarda não custa nada; se preenche para todos,
    # ela evita 2.600 fichas com razão social indevida.
    if saida.get("tipo_contrato") != "PJ":
        saida.pop("razao_social_pj", None)
        saida.pop("cnpj_pj", None)

    if saida.get("demissao"):
        # ⚑ "CADASTRO CONCLUÍDO" COM DATA DE SAÍDA: a data é que está errada.
        # Ele decidiu em 17/11/2026: *"Cadastro Concluído com data de saída é
        # apagar a data de saída e manter ativo."* São 6 pessoas hoje. A data
        # NÃO entra (ela desligaria alguém que está trabalhando) e o caso vai
        # para a lista, para ser arrumado na planilha.
        if _cru(crua(NOMES["fase"])) == _cru("Cadastro Concluído"):
            nao_entendi.append({
                "coluna": "Data de Saída", "campo": "demissao",
                "valor": saida.pop("demissao"),
                "motivo": ("não entrou: a fase é “Cadastro Concluído”, que é "
                           "gente trabalhando — a data de saída está errada e "
                           "precisa ser apagada na planilha")})
        else:
            # Nas outras fases a data manda: a pessoa saiu, o cadastro é que
            # não foi finalizado (o caso de "Solicitações DP").
            saida["situacao"] = "DESLIGADO"

    nomes_saida = {chave: crua(nomes) for chave, nomes in NOMES.items()}
    # "00.079.526/0001-09 - BWS" → só o CNPJ.
    # ⚠️ Cortar no primeiro "-" NÃO serve: o próprio CNPJ tem um ("0001-09"),
    # e o corte devolvia 12 dígitos. São os 14 primeiros dígitos da célula.
    if nomes_saida.get("empresa"):
        nomes_saida["empresa"] = _digitos(nomes_saida["empresa"])[:14]
    # "164.01 - OBRA ESCVARZEA …/SETOR PRODU" → só o código.
    if nomes_saida.get("centro_custo_contabil"):
        nomes_saida["centro_custo_contabil"] = \
            nomes_saida["centro_custo_contabil"].split(" ")[0].strip()

    saida["_nomes"] = nomes_saida
    saida["_nao_entendi"] = nao_entendi
    return saida


# ===========================================================================
# LER A PLANILHA AO VIVO
#
# ⚑ AO VIVO, E NÃO DA CÓPIA NO BANCO — conserto de 16/11/2026. A Análise de
# SPs mantém uma cópia (`analisesps.colaborador`), e eu tentei usá-la: ela tem
# só 30 das 79 colunas, e fica velha entre uma atualização e outra. O dono
# atualizou a planilha, importou, e nada mudou — porque a cópia era de antes.
#
# Aqui a leitura é da planilha mesmo, com a credencial que o monorepo já usa
# (`GOOGLE_CREDENTIALS_BASE64`). Uma leitura por importação, não por pessoa.
# ===========================================================================
import os

PLANILHA = os.getenv("ANALISESPS_SHEET_DADOS_DOCUMENTOS",
                     "1fqi4QUOVGUd1_4Gg4vK5qP_IMOSgFaw8DD9MDgmM3vo")


def carregar(planilha_id: str = "") -> dict[str, Any]:
    """A aba inteira, indexada pelo CPF, MAIS o que aconteceu ao ler.

    ⚑ O "MAIS O QUE ACONTECEU" É O CONSERTO DE 17/11/2026, e é a terceira vez
    que o mesmo tipo de silêncio custa uma rodada dele:

        *"Meu, não tá funcionando. Você precisa me mostrar o que tá
         acontecendo, como tá fazendo. A planilha tá sendo atualizada e os
         dados não alimentam o ERP."*

    A primeira versão engolia o erro num `logger.warning` e devolvia `{}`. Log
    é para quem abre o Render; ele abre a TELA. Falha de credencial, planilha
    renomeada, aba trocada de nome — tudo isso virava "não aconteceu nada",
    sem uma linha na tela dizendo por quê.

    Agora volta sempre: quantas pessoas foram lidas, qual planilha e qual aba,
    e o ERRO por extenso quando houver. Quem usa não precisa adivinhar, e eu
    não preciso pedir print.
    """
    chave = planilha_id or PLANILHA
    try:
        from app.apps.analisesps.credenciais import cliente, com_retry
        aba = com_retry(lambda: cliente().open_by_key(chave).worksheet(ABA))
        linhas = com_retry(lambda: aba.get_all_values())
    except Exception as e:                       # noqa: BLE001
        logger.warning("ERP/planilha-dp: não deu para ler a planilha (%s)", e)
        return {"pessoas": {}, "erro": str(e)[:500], "planilha": chave, "aba": ABA,
                "linhas": 0}
    if len(linhas) < 2:
        return {"pessoas": {}, "planilha": chave, "aba": ABA, "linhas": len(linhas),
                "erro": f'A aba "{ABA}" veio vazia.'}

    cabecalho = cabecalho_de(linhas[0])
    i_cpf = _coluna(cabecalho, SO_DIGITOS["cpf"])
    saida: dict[str, dict[str, Any]] = {}
    for linha in linhas[1:]:
        if i_cpf is None or i_cpf >= len(linha):
            continue
        cpf = _digitos(linha[i_cpf])
        # ⚠️ A SEGUNDA LINHA DA ABA É UMA RÉGUA DE ÍNDICES ("1", "2", "3"…),
        # não gente. CPF com menos de 11 dígitos cai fora sozinho.
        if len(cpf) != 11:
            continue
        saida[cpf] = ler_linha(cabecalho, linha)
    logger.info("ERP/planilha-dp: %d pessoa(s) lidas da aba %s", len(saida), ABA)
    return {"pessoas": saida, "erro": "", "planilha": chave, "aba": ABA,
            "linhas": len(linhas) - 1, "colunas": len(cabecalho),
            # ⚑ O CABEÇALHO CRU VOLTA JUNTO (17/11/2026), e é para acabar com
            # uma pergunta que já se repetiu quatro vezes: *"outros campos que
            # estão ficando vazios"*.
            #
            # Campo vazio tem duas causas possíveis, e de fora elas são
            # idênticas: a coluna está vazia na planilha, ou a coluna existe e
            # eu não a reconheço (foi renomeada, ou tem um caractere
            # parecido). O relatório passa a dizer qual das duas é, listando
            # as colunas que a planilha tem e eu não uso.
            "cabecalho": [c for c in linhas[0] if str(c or "").strip()]}


def carregar_por_cpf(planilha_id: str = "") -> dict[str, dict[str, Any]]:
    """Só as pessoas — para quem não se importa com o diagnóstico."""
    return carregar(planilha_id).get("pessoas") or {}


# ===========================================================================
# IMPORTAR DIRETO DA PLANILHA — sem card nenhum (17/11/2026)
#
# Ele, olhando a tela que pedia o número do card: *"mas por que rodar com o
# card dela se quero que puxe da planilha?"*
#
# ⚑ TEM RAZÃO, E A PERGUNTA EXPÕE UM RESTO DE DESENHO ANTIGO. O card servia
# para duas coisas: achar a pessoa e baixar os DOCUMENTOS. A planilha tem o
# CPF — ela acha a pessoa sozinha. Exigir o número do card para trazer dados
# que estão na planilha era obrigá-lo a colar 575 números para nada.
#
# Ficam duas portas, cada uma com o seu trabalho:
#   · DADOS      → daqui, pela planilha, sem card;
#   · DOCUMENTOS → pelo Pipefy, com os cards (é o que a planilha não tem).
# ===========================================================================
def importar(s, usuario=None, *, gravar: bool = False,
             cpfs: Optional[list[str]] = None,
             criar: bool = True,
             ler=None) -> dict[str, Any]:
    """Enche o cadastro de quem está na planilha, e cadastra quem falta.

    `cpfs` limita a algumas pessoas — é como se começa, com duas ou três, em
    vez de soltar 3.600 de uma vez. Sem ele, vai a planilha inteira.

    ⚑ PASSOU A CADASTRAR EM 17/11/2026. Ele, olhando o relatório que listava
    uma pessoa como "não existe no ERP": *"Pq não cadastrou a partir da
    planilha?"*

    A recusa anterior tinha um motivo — a planilha tem 3.667 linhas e cerca de
    2.960 são de gente desligada, e criar tudo encheria o cadastro de base
    morta, que depois aparece em lista de obra, em folha e em relatório. Mas a
    resposta certa não era "não cadastro": era **cadastrar só quem está
    ativo**, que é a regra que ele mesmo deu em 04/11/2026 (*"eu acho que a
    gente deve colocar só os que estão ativos"*).

    Quem a planilha mostra como desligado continua fora, e é dito no relatório
    com esse motivo — não com "não existe no ERP", que não explicava nada.
    """
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.db.models.cadastros import Cargo, Colaborador, Convencao, Obra
    from sqlalchemy import select, text

    lido = (ler or carregar)()
    pessoas = lido.get("pessoas") or {}
    rel: dict[str, Any] = {
        "gravou": bool(gravar),
        "planilha": {k: lido.get(k) for k in
                     ("erro", "planilha", "aba", "linhas", "colunas")},
        "pessoas_na_planilha": len(pessoas),
        "atualizadas": [], "criadas": [], "sem_cadastro": [], "recusadas": [],
        "nao_entendi": [], "cadastros_criados": [],
        "criou": bool(criar),
        # As colunas que a planilha tem e este de-para não lê. É a resposta
        # para "por que este campo está vazio?" sem precisar perguntar.
        "colunas_que_eu_nao_uso": sorted(
            c for c in (lido.get("cabecalho") or [])
            if _cru(c) and _cru(c) not in colunas_que_eu_uso()),
    }
    if not pessoas:
        return rel

    alvo = {re.sub(r"\D", "", c) for c in (cpfs or []) if c}
    if alvo:
        pessoas = {k: v for k, v in pessoas.items() if k in alvo}
        rel["pedidos"] = sorted(alvo)
        for c in sorted(alvo - set(pessoas)):
            rel["sem_cadastro"].append({"cpf": c, "nome": "",
                                        "motivo": "não está na planilha"})

    # Os cadastros do ERP, uma consulta só — 3.600 pessoas × 3 consultas cada
    # seria a diferença entre segundos e minutos.
    por_cpf = {re.sub(r"\D", "", c.cpf): c
               for c in s.scalars(select(Colaborador)).all()}
    cargos = {_cru(c.nome): c.id for c in s.scalars(select(Cargo)).all()}
    convencoes = {_cru(c.nome): c.id for c in s.scalars(select(Convencao)).all()}
    obras = {_cru(o.codigo): o.id for o in s.scalars(select(Obra)).all()}
    # ⚑ A EMPRESA CASA PELO CNPJ, NUNCA PELO NOME (17/11/2026). Ele mostrou o
    # motivo: a planilha escreve "00.079.526/0001-09 - BWS" e o cadastro do
    # ERP tem "BWS CONSTRUÇÕES - 00.079.526/0001-09". É a MESMA empresa, com o
    # nome e o número em ordem trocada e abreviações diferentes — comparar
    # texto nunca ia bater, e era por isso que "Empresa de registro" ficava
    # vazia em todo mundo.
    #
    # Os 14 dígitos são iguais nos dois lados, e são a identidade da empresa.
    empresas = {}
    for linha_e in s.execute(text(
            "SELECT id, cnpj FROM empresas WHERE cnpj IS NOT NULL")).mappings():
        digitos = re.sub(r"\D", "", str(linha_e["cnpj"] or ""))[:14]
        if len(digitos) == 14:
            empresas[digitos] = linha_e["id"]

    for cpf, dados in pessoas.items():
        dados = dict(dados)
        nomes = dados.pop("_nomes", {})
        for x in dados.pop("_nao_entendi", []):
            rel["nao_entendi"].append({**x, "cpf": cpf,
                                       "nome": dados.get("nome", "")})
        pessoa = por_cpf.get(cpf)
        nasceu_agora = False
        if pessoa is None:
            desligado = str(dados.get("situacao") or "").upper() == "DESLIGADO"
            if not criar:
                rel["sem_cadastro"].append({
                    "cpf": cpf, "nome": dados.get("nome", ""),
                    "motivo": "não existe no ERP"})
                continue
            if desligado:
                rel["sem_cadastro"].append({
                    "cpf": cpf, "nome": dados.get("nome", ""),
                    "motivo": "desligado na planilha — base morta não entra"})
                continue
            nome_novo = " ".join(str(dados.get("nome") or "").split())
            if len(nome_novo) < 4:
                rel["recusadas"].append({
                    "cpf": cpf, "nome": nome_novo,
                    "motivo": "a planilha não traz o nome completo"})
                continue
            nasceu_agora = True

        # ⚑ A EMPRESA DE REGISTRO, pelos 14 dígitos do CNPJ. Ver a montagem de
        # `empresas`, acima: é o único jeito de "00.079.526/0001-09 - BWS"
        # encontrar "BWS CONSTRUÇÕES - 00.079.526/0001-09".
        escrito_empresa = nomes.get("empresa") or ""
        if escrito_empresa:
            digitos = re.sub(r"\D", "", escrito_empresa)[:14]
            if len(digitos) == 14 and digitos in empresas:
                dados["empresa_id"] = empresas[digitos]
            else:
                rel["nao_entendi"].append({
                    "coluna": "CNPJ Admissão", "campo": "Empresa de registro",
                    "cpf": cpf, "nome": dados.get("nome", ""),
                    "valor": escrito_empresa,
                    "motivo": ("CNPJ incompleto na planilha"
                               if len(digitos) != 14
                               else "não existe no cadastro de Empresas")})

        # ⚑ CARGO E CONVENÇÃO SÃO CRIADOS quando faltam; OBRA não.
        #
        # A diferença não é capricho: cargo e convenção são, no ERP, um NOME —
        # o resto (piso, descrição) se preenche na tela deles depois. Obra é
        # cadastro pesado, com contrato, CNO, cliente e endereço; criar obra a
        # partir de um código de planilha produziria obra fantasma, e o código
        # que não casa já aparece no relatório para ele apontar qual é.
        for chave, campo, tabela, rotulo, modelo in (
                ("cargo", "cargo_id", cargos, "Cargo", Cargo),
                ("convencao", "convencao_id", convencoes, "Convenção", Convencao),
                ("obra", "obra_id", obras, "Obra", None)):
            escrito = nomes.get(chave) or ""
            if not escrito:
                continue
            # "Sem Convenção" na planilha quer dizer SEM convenção, e não uma
            # convenção chamada "Sem Convenção" — criá-la seria inventar um
            # cadastro para representar a ausência dele.
            if chave == "convencao" and _cru(escrito).startswith("SEMCONVENCAO"):
                dados[campo] = None
                continue
            achado = tabela.get(_cru(escrito))
            if achado:
                dados[campo] = achado
                continue
            if modelo is None:
                rel["nao_entendi"].append({
                    "coluna": chave, "campo": rotulo, "cpf": cpf,
                    "nome": dados.get("nome", ""), "valor": escrito,
                    "motivo": "não existe no cadastro do ERP"})
                continue
            if gravar:
                with s.begin_nested():
                    obj = modelo(nome=escrito)
                    s.add(obj)
                    s.flush()
                tabela[_cru(escrito)] = obj.id
                dados[campo] = obj.id
            else:
                # Na simulação não há id, mas o relatório já diz que vai nascer.
                tabela[_cru(escrito)] = None
            rel["cadastros_criados"].append({"tipo": rotulo, "nome": escrito})

        # ⚑ PIS QUE NÃO FECHA NÃO DERRUBA A PESSOA — ele vira PENDÊNCIA.
        #
        # Ele, em 17/11/2026, vendo 38 pessoas recusadas por isso: *"Recusou só
        # pq o dado não bateu? Importa sem o PIS e deixa como pendência."*
        #
        # Tinha razão, e o estrago era grande: o cadastro inteiro daquela
        # pessoa — endereço, vínculo, cargo, salário, 50 campos — ficava de
        # fora por causa de UM número de onze dígitos. O PIS errado não entra
        # (número de INSS errado é pior que vazio, porque vai para a guia),
        # mas o resto entra, e a pendência aparece na lista "o que falta para
        # admitir", que já existe no ERP.
        if "pis" in dados and not svc.pis_valido(dados["pis"]):
            rel["nao_entendi"].append({
                "coluna": "Nº PIS", "campo": "PIS/NIS", "cpf": cpf,
                "nome": dados.get("nome", ""), "valor": str(dados["pis"]),
                "motivo": ("não fecha os 11 dígitos — o resto do cadastro "
                           "entrou, o PIS ficou pendente")})
            dados.pop("pis")

        # ⚠️ O CPF é a identidade e o NOME só muda por decisão de alguém.
        dados.pop("cpf", None)

        if nasceu_agora:
            if gravar:
                try:
                    with s.begin_nested():
                        pessoa = svc.salvar(
                            s, {**dados, "cpf": cpf, "nome": nome_novo}, usuario)
                    por_cpf[cpf] = pessoa
                except ErroValidacao as e:
                    rel["recusadas"].append({"cpf": cpf, "nome": nome_novo,
                                             "motivo": str(e)[:300]})
                    continue
            rel["criadas"].append({"cpf": cpf, "nome": nome_novo})
            continue

        mudou = {k: v for k, v in dados.items()
                 if k != "nome" and str(getattr(pessoa, k, "") or "") != str(v)}
        if not mudou:
            continue
        if gravar:
            try:
                with s.begin_nested():
                    svc.salvar(s, {**dados, "cpf": pessoa.cpf,
                                   "nome": pessoa.nome}, usuario)
            except ErroValidacao as e:
                rel["recusadas"].append({"cpf": cpf, "nome": pessoa.nome,
                                         "motivo": str(e)[:300]})
                continue
        rel["atualizadas"].append(
            {"cpf": cpf, "nome": pessoa.nome,
             "campos": {k: str(v) for k, v in sorted(mudou.items())}})
    return rel
