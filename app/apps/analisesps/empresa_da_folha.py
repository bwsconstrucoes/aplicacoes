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
    saida = {}
    for cpf, docs in linhas:
        try:
            dado = json.loads(docs) if docs else {}
        except ValueError:
            continue
        cnpj = _digitos((dado or {}).get("cnpj_admissao")) if isinstance(dado, dict) else ""
        if cnpj:
            saida[cpf] = cnpj
    return saida


def nomes_das_empresas() -> dict:
    """`{cnpj: nome}` pelo cadastro de Empresas do ERP. Sem ele, vazio — a tela
    mostra o CNPJ."""
    try:
        from .db import consultar
        return {_digitos(c): (f or r or "").strip()
                for c, f, r in consultar(
                    "SELECT cnpj, nome_fantasia, razao_social FROM public.empresas")}
    except Exception:  # noqa: BLE001 — o nome é conforto; o CNPJ basta
        logger.info("Folha: não consegui ler os nomes das empresas do ERP.")
        return {}


def rotulo(cnpj: str, nomes: dict | None = None) -> str:
    if not cnpj or cnpj == SEM_CNPJ:
        return ROTULO_SEM_CNPJ
    nome = (nomes or {}).get(_digitos(cnpj), "")
    return f"{nome} ({cnpj_bonito(cnpj)})" if nome else cnpj_bonito(cnpj)


def normalizar(empresa) -> str:
    """O valor do filtro: 14 dígitos, "sem_cnpj" ou vazio (todas)."""
    texto = str(empresa or "").strip()
    if texto == SEM_CNPJ:
        return SEM_CNPJ
    d = _digitos(texto)
    return d if len(d) == 14 else ""


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
