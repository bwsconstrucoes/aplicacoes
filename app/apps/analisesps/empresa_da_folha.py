# -*- coding: utf-8 -*-
"""
A EMPRESA DE ADMISSÃO — o recorte por CNPJ nas folhas (10/11/2026).

O dono: *"na C. Diários (…) Dados Documentos (…) existe uma coluna, é a coluna BT,
CNPJ Admissão. Eu preciso diferenciar as empresas. (…) gerar a folha de diaristas,
mas eu quero exibir somente os que estão cadastrados para uma empresa e não para
outra. (…) Antes de gerar qualquer informação. Isso vai se aplicar na parte de
alimentação e transporte, na parte de diaristas, despesa com colaboradores."*

Como funciona:
- A coluna BT ("CNPJ Admissão") vem na carga do cadastro, guardada no JSON de
  documentos da ficha (`colaboradores.COLUNAS_DO_PAGAMENTO`, sem migração).
- A empresa escolhida na barra do mês recorta a lista ANTES de tudo: totais, obras,
  seleção para pagamento, relatório e geração saem só com as pessoas daquela empresa.
- Quem está sem CNPJ no cadastro aparece como "sem CNPJ de admissão" — também é uma
  opção, para ninguém ficar de fora sem ser visto.
- O nome da empresa vem do cadastro de Empresas do ERP (pelo CNPJ); sem ele, o CNPJ.
"""
from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger("analisesps.empresa_da_folha")

SEM_CNPJ = "sem_cnpj"
ROTULO_SEM_CNPJ = "Sem CNPJ de admissão"


def _digitos(valor) -> str:
    return "".join(c for c in str(valor or "") if c.isdigit())


def cnpj_bonito(cnpj: str) -> str:
    d = _digitos(cnpj)
    if len(d) != 14:
        return str(cnpj or "")
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


# ⚠️ A COLUNA BT VEM ESCRITA "00.079.526/0001-09 - BWS" (dono, 10/11/2026: *"a coluna
# BT tá preenchida assim: 00.079.526/0001-09 - BWS, 50.422.792/0001-09 - INFRADENDE,
# 38.240.209/0001-03 - LC, 04.895.340/0001-89 - RVV, mas não é o que podemos filtrar,
# tá diferente"*). O filtro mostrava o nome do cadastro de Empresas do ERP (ou só o
# CNPJ); agora mostra o que está escrito na própria coluna. O CNPJ é o que separa; o
# nome escrito depois do traço é o que se lê.
_CNPJ_NO_TEXTO = re.compile(r"\d{2}\.?\d{3}\.?\d{3}\s*/?\s*\d{4}\s*-?\s*\d{2}")
_nomes_da_coluna: dict = {}


def ler_coluna(texto) -> tuple:
    """`(cnpj só dígitos, nome)` do texto da coluna BT. Sem CNPJ válido, ("", "")."""
    texto = " ".join(str(texto or "").split())
    achado = _CNPJ_NO_TEXTO.search(texto)
    cnpj = _digitos(achado.group(0)) if achado else _digitos(texto)[:14]
    if len(cnpj) != 14:
        return "", ""
    resto = texto[achado.end():] if achado else ""
    nome = resto.strip(" -–—:|").strip()
    return cnpj, nome


def cnpj_por_cpf() -> dict:
    """`{cpf: cnpj (só dígitos)}` de todo o cadastro, numa consulta. Quem está sem
    CNPJ fica de fora do dicionário."""
    from . import colaboradores
    from .db import consultar
    try:
        if not colaboradores.tem_documentos():
            return {}
        linhas = consultar("SELECT cpf, documentos FROM analisesps.colaborador "
                           " WHERE documentos IS NOT NULL AND documentos <> ''")
    except Exception:  # noqa: BLE001 — sem o cadastro, ninguém tem empresa: a tela segue
        logger.exception("Folha: não consegui ler o CNPJ de admissão do cadastro")
        return {}
    saida, nomes = {}, {}
    for cpf, docs in linhas:
        try:
            dado = json.loads(docs) if docs else {}
        except ValueError:
            continue
        if not isinstance(dado, dict):
            continue
        cnpj, nome = ler_coluna(dado.get("cnpj_admissao"))
        if cnpj:
            saida[cpf] = cnpj
            if nome:
                nomes.setdefault(cnpj, nome)
    # Trocado a cada leitura: um nome corrigido na planilha aparece na próxima.
    _nomes_da_coluna.clear()
    _nomes_da_coluna.update(nomes)
    return saida


def nomes_das_empresas() -> dict:
    """`{cnpj: nome}`: o nome escrito na coluna BT ("… - BWS") e, para o CNPJ que
    não tem nome lá, o do cadastro de Empresas do ERP."""
    saida = {}
    try:
        from .db import consultar
        saida = {_digitos(c): (f or r or "").strip()
                 for c, f, r in consultar(
                     "SELECT cnpj, nome_fantasia, razao_social FROM public.empresas")}
    except Exception:  # noqa: BLE001 — o nome é conforto; o CNPJ basta
        logger.info("Folha: não consegui ler os nomes das empresas do ERP.")
    if not _nomes_da_coluna:
        cnpj_por_cpf()                     # o nome da coluna vem na leitura do cadastro
    saida.update(_nomes_da_coluna)         # o da planilha vence o do ERP
    return saida


def rotulo(cnpj: str, nomes: dict | None = None) -> str:
    if not cnpj or cnpj == SEM_CNPJ:
        return ROTULO_SEM_CNPJ
    nome = (nomes or {}).get(_digitos(cnpj), "")
    # Como está na planilha: "00.079.526/0001-09 - BWS".
    return f"{cnpj_bonito(cnpj)} - {nome}" if nome else cnpj_bonito(cnpj)


def normalizar(empresa) -> str:
    """O valor do filtro: 14 dígitos, "sem_cnpj" ou vazio (todas)."""
    texto = str(empresa or "").strip()
    if texto == SEM_CNPJ:
        return SEM_CNPJ
    d, _ = ler_coluna(texto)
    return d


def marcar(pessoas: list, por_cpf: dict | None = None) -> list:
    """Põe em cada pessoa a chave `cnpj_admissao` ("" quando não tem)."""
    por_cpf = cnpj_por_cpf() if por_cpf is None else por_cpf
    for p in pessoas or []:
        p["cnpj_admissao"] = por_cpf.get(p.get("cpf") or "", "")
    return pessoas


def recortar(pessoas: list, empresa: str) -> list:
    """Só as pessoas da empresa pedida. Sem empresa, todas."""
    empresa = normalizar(empresa)
    if not empresa:
        return list(pessoas or [])
    alvo = "" if empresa == SEM_CNPJ else empresa
    return [p for p in pessoas or [] if (p.get("cnpj_admissao") or "") == alvo]


def opcoes(pessoas: list) -> list:
    """`[(valor, rótulo com quantas pessoas)]` das empresas presentes na lista."""
    nomes = nomes_das_empresas()
    contagem: dict = {}
    for p in pessoas or []:
        chave = p.get("cnpj_admissao") or SEM_CNPJ
        contagem[chave] = contagem.get(chave, 0) + 1
    ordem = sorted((c for c in contagem if c != SEM_CNPJ), key=lambda c: rotulo(c, nomes))
    if SEM_CNPJ in contagem:
        ordem.append(SEM_CNPJ)
    return [(c, f"{rotulo(c, nomes)} · {contagem[c]} pessoa{'s' if contagem[c] != 1 else ''}")
            for c in ordem]


def preparar(pessoas: list, empresa: str) -> tuple:
    """Marca o CNPJ em todos, guarda as opções (de TODOS) e devolve o recorte:
    `(recortadas, opcoes)`. Uma leitura do cadastro só."""
    marcar(pessoas)
    return recortar(pessoas, empresa), opcoes(pessoas)


# ---------------------------------------------------------------------------
# O FECHAMENTO POR EMPRESA (10/11/2026). O dono: *"acho que devemos fechar por
# empresa"* e, logo depois, *"quero poder fazer junto ou por empresa"*.
#
# A empresa entra no NOME DA VERBA do fechamento e da geração: "diaria" é o
# pagamento de todas juntas; "diaria.11222333000181" é o daquela empresa (e
# "diaria.sem_cnpj", o de quem está sem CNPJ). Assim cada empresa tem o seu
# fechamento, o seu arquivo e a sua SP — e o que já existia ("diaria") não muda.
# Rótulo, natureza, carteira e grupo do Pipefy olham a verba BASE (`separar`).
#
# ⚠️ JUNTO E POR EMPRESA NÃO SE MISTURAM NO MESMO PERÍODO (`conflita`): depois de
# gerar todas juntas, gerar uma empresa pagaria de novo quem já recebeu; depois de
# gerar uma empresa, gerar todas juntas também. Isso é recusado sempre, mesmo com
# "gerar mesmo assim". Repetir a MESMA geração continua com a regra de sempre.
# ---------------------------------------------------------------------------
SEPARADOR = "."


def verba_da_empresa(verba: str, empresa: str) -> str:
    e = normalizar(empresa)
    return f"{verba}{SEPARADOR}{e}" if e else str(verba)


def separar(verba: str) -> tuple:
    """`(verba base, empresa)` — a empresa vazia quando é o pagamento de todas."""
    texto = str(verba or "").strip().lower()
    if SEPARADOR in texto:
        base, empresa = texto.split(SEPARADOR, 1)
        return base, empresa
    return texto, ""


def rotulo_curto(empresa: str) -> str:
    """O nome da empresa (do ERP) ou o CNPJ formatado — para rótulo e título."""
    if not empresa:
        return ""
    if empresa == SEM_CNPJ:
        return "sem CNPJ de admissão"
    return nomes_das_empresas().get(_digitos(empresa)) or cnpj_bonito(empresa)


def conflita(verba_pedida: str, verba_gerada: str) -> str:
    """Como uma geração já feita (`verba_gerada`) esbarra na pedida: "" (não
    esbarra), "mesma" (é a mesma geração) ou "mistura" (junto × por empresa)."""
    base_p, emp_p = separar(verba_pedida)
    base_g, emp_g = separar(verba_gerada)
    if base_p != base_g:
        return ""
    if emp_p == emp_g:
        return "mesma"
    if not emp_p or not emp_g:
        return "mistura"
    return ""
