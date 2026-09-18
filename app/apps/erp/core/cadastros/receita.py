# ============================================================================
# ERP — core/cadastros/receita.py
# O cadastro de CNPJ como a Receita Federal o publica.
#
# PEDIDO DO DONO, 18/09/2026, olhando a planilha de fornecedores: *"eu queria
# que você fizesse uma equalização dessa razão social através de pesquisa via
# API (…) você faz a pesquisa desses CNPJs todos e já readequa com a
# nomenclatura certa"*.
#
# O PROBLEMA QUE ISSO RESOLVE é real e aparece na própria planilha: o mesmo
# CNPJ aparece como "STOCK COMERCIO EP LTDA" e "STOCK EPI E EQUIPAMENTOS
# INDUSTRIAIS"; outro como "GERDAU AÇO LONGOS SA" e "GERDAU AÇO LONGOS SA PE".
# Cada comprador digitou o nome como lembrava. Nota fiscal, contrato e certidão
# saem com a razão social OFICIAL — e é ela que tem de estar no cadastro.
#
# DE ONDE VEM O DADO: BrasilAPI (`brasilapi.com.br`), que republica a base
# pública da Receita. Sem chave, sem cadastro, sem conta a pagar — o mesmo
# critério que levou o ERP a usar o Banco Central para o INCC em vez da FGV.
# Se um dia ela sair do ar, o cadastro continua como está: a consulta é pelo
# BOTÃO, nunca no caminho de quem está usando a tela.
#
# O QUE ELA NUNCA FAZ: apagar o que a pessoa escreveu à mão sem dizer. A
# consulta preenche o que está VAZIO e, quando discorda do que existe, RELATA
# a divergência em vez de sobrescrever — quem decide trocar o nome de um
# fornecedor é gente, olhando os dois lados.
# ============================================================================
from __future__ import annotations

import logging
import re
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
TEMPO_LIMITE = 20

# A BrasilAPI limita a 3 consultas por minuto por IP no plano aberto. 21
# segundos entre uma e outra é o que cabe sem levar 429 — e é por isso que a
# equalização de 1.700 fornecedores é TAREFA DE FUNDO, em lotes, e não um
# botão que trava a tela por dez horas.
ESPERA_ENTRE_CONSULTAS = 21


class ReceitaIndisponivel(Exception):
    """A consulta não respondeu. Não é erro de dado — é falta de resposta."""


def _digitos(valor: Any) -> str:
    return re.sub(r"\D", "", str(valor or ""))


def consultar(cnpj: str) -> Optional[dict[str, Any]]:
    """O que a Receita publica sobre este CNPJ, ou None se ele não existe lá.

    Levanta `ReceitaIndisponivel` quando o problema é de rede — a diferença
    importa: CNPJ inexistente é achado sobre o dado, rede fora é "tente
    depois", e tratar os dois igual marcaria fornecedor bom como inválido.
    """
    import requests

    limpo = _digitos(cnpj)
    if len(limpo) != 14:
        return None            # CPF ou documento torto: não há o que consultar
    try:
        r = requests.get(URL.format(cnpj=limpo), timeout=TEMPO_LIMITE)
    except Exception as erro:
        raise ReceitaIndisponivel(str(erro)) from erro
    if r.status_code == 404:
        return None
    if r.status_code == 429:
        raise ReceitaIndisponivel("limite de consultas por minuto atingido")
    if r.status_code >= 400:
        raise ReceitaIndisponivel(f"a consulta respondeu {r.status_code}")
    try:
        d = r.json()
    except ValueError as erro:
        raise ReceitaIndisponivel("resposta ilegível") from erro
    return _traduzir(d)


def _traduzir(d: dict[str, Any]) -> dict[str, Any]:
    """Os nomes da BrasilAPI viram os nomes do ERP, e só o que interessa."""
    def t(chave: str) -> str:
        return str(d.get(chave) or "").strip()

    return {
        "cnpj": _digitos(d.get("cnpj")),
        "razao_social": t("razao_social"),
        # `nome_fantasia` vem vazio para a maioria das empresas — é campo
        # opcional na Receita. Vazio NÃO apaga o que a BWS já tem.
        "nome_fantasia": t("nome_fantasia"),
        "situacao": t("descricao_situacao_cadastral").upper(),
        "data_abertura": t("data_inicio_atividade"),
        "cnae_principal": t("cnae_fiscal") or str(d.get("cnae_fiscal") or ""),
        "cnae_descricao": t("cnae_fiscal_descricao"),
        "logradouro": " ".join(x for x in (t("descricao_tipo_de_logradouro"),
                                           t("logradouro")) if x).strip(),
        "numero": t("numero"),
        "complemento": t("complemento"),
        "bairro": t("bairro"),
        "cep": _digitos(d.get("cep")),
        "municipio": t("municipio"),
        "uf": t("uf"),
        "telefone": _digitos(d.get("ddd_telefone_1")),
        "email": t("email").lower(),
        "porte": t("porte"),
    }


# ---------------------------------------------------------------------------
# Equalizar o cadastro
# ---------------------------------------------------------------------------
# Os campos que a consulta pode PREENCHER quando estão vazios no cadastro.
# Razão social NÃO está aqui de propósito: ela quase nunca está vazia, e
# trocá-la é justamente a decisão que precisa de gente.
PREENCHIVEIS = ("cnae_principal", "logradouro", "numero", "complemento",
                "bairro", "cep", "municipio", "uf")


def comparar(forn, dados: dict[str, Any]) -> dict[str, Any]:
    """O que mudaria neste fornecedor, separado em PREENCHER e DIVERGE.

    Não toca no objeto: existe assim para a prévia mostrar antes de gravar, e
    para ser testável sem banco.
    """
    preencher: dict[str, Any] = {}
    diverge: dict[str, Any] = {}

    for campo in PREENCHIVEIS:
        novo = (dados.get(campo) or "").strip()
        if not novo:
            continue
        atual = (getattr(forn, campo, None) or "").strip()
        if not atual:
            preencher[campo] = novo
        elif _chave(atual) != _chave(novo):
            diverge[campo] = {"no_erp": atual, "na_receita": novo}

    oficial = (dados.get("razao_social") or "").strip()
    atual = (getattr(forn, "razao_social", None) or "").strip()
    if oficial and _chave(atual) != _chave(oficial):
        diverge["razao_social"] = {"no_erp": atual, "na_receita": oficial}

    fantasia = (dados.get("nome_fantasia") or "").strip()
    if fantasia and not (getattr(forn, "nome_fantasia", None) or "").strip():
        preencher["nome_fantasia"] = fantasia

    return {"preencher": preencher, "diverge": diverge,
            "situacao": dados.get("situacao", ""),
            "baixada": dados.get("situacao", "") not in ("ATIVA", "")}


def _chave(texto: str) -> str:
    import unicodedata
    bruto = unicodedata.normalize("NFKD", (texto or "").strip().lower())
    sem_acento = "".join(c for c in bruto if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", sem_acento).strip()
