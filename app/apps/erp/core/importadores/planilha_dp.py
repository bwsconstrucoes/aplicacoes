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
    sem = unicodedata.normalize("NFKD", str(texto or ""))
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
    "ctps_numero":      ("Nº CTPS",),
    "ctps_serie":       ("Série (CTPS)",),
    "ctps_uf":          ("UF (CTPS)",),
    "contrato_experiencia": ("Contrato de Experiência",),
    "observacao_auxilio":   ("Observação Ajuda de Custo",),
    "id_fortes":        ("ID Fortes",),
    "requerente":       ("Responsável pela Solicitação",),
    "pix_chave":        ("Chave Pix",),
}
# Campos que são número com pontuação na planilha e só dígitos no cadastro.
SO_DIGITOS: dict[str, tuple[str, ...]] = {
    "cpf":      ("CPF (Cadastro de Pessoa Física)", "CPF Números"),
    "telefone": ("Telefone",),
    "cep":      ("CEP",),
    "pis":      ("Nº PIS",),
}
DATAS: dict[str, tuple[str, ...]] = {
    "data_nascimento": ("Data de Nascimento",),
    "rg_emissao":      ("Data de Emissão (Documento de Identificação)",),
    "ctps_emissao":    ("Data de Emissão (CTPS)",),
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

    # ⚑ PAGAMENTO: a planilha não tem coluna dizendo "é mensalista". Quem tem
    # VALOR DIÁRIA é diarista; quem não tem, mensalista. É a leitura que a
    # folha já faz — e fica escrito porque é dedução, não dado.
    if saida.get("vinculo"):
        saida["mensalista"] = "valor_diaria" not in saida

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


def carregar_por_cpf(planilha_id: str = "") -> dict[str, dict[str, Any]]:
    """A aba inteira, indexada pelo CPF (só dígitos).

    Devolve {} quando a planilha não pôde ser lida — e aí a importação segue
    com o que tiver, dizendo o motivo. Derrubar a importação inteira porque o
    Google demorou seria trocar um cadastro incompleto por nenhum.
    """
    from app.apps.analisesps.credenciais import cliente, com_retry

    try:
        aba = com_retry(lambda: cliente()
                        .open_by_key(planilha_id or PLANILHA).worksheet(ABA))
        linhas = com_retry(lambda: aba.get_all_values())
    except Exception as e:                       # noqa: BLE001
        logger.warning("ERP/planilha-dp: não deu para ler a planilha (%s)", e)
        return {}
    if len(linhas) < 2:
        return {}

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
    return saida
