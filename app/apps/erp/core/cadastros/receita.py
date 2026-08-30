# ============================================================================
# BWS ERP — core/cadastros/receita.py
# Consulta pública de CNPJ para cadastro limpo de fornecedores.
#
# Fontes (sem chave, gratuitas):
#   1ª BrasilAPI  — https://brasilapi.com.br/api/cnpj/v1/{cnpj}
#   2ª ReceitaWS  — https://receitaws.com.br/v1/cnpj/{cnpj}  (fallback; ~3/min)
#
# Robustez: valida DV antes de gastar rede, timeout, retry, verificação de
# HTTP 200, normalização (maiúsculas, espaços) e situação cadastral para as
# travas do cadastro (C4 da especificação: fornecedor BAIXADO/INAPTO não
# entra sem tratamento).
# ============================================================================
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

from app.apps.erp.core.cadastros.validadores import cnpj_valido, somente_digitos

_TIMEOUT = 20
_TENTATIVAS = 2

SITUACOES_BLOQUEANTES = {"BAIXADA", "INAPTA", "NULA"}
SITUACOES_ALERTA = {"SUSPENSA"}


class ErroConsultaCNPJ(Exception):
    """Falha de rede/serviço na consulta (não confundir com CNPJ inválido)."""


@dataclass
class DadosCNPJ:
    cnpj: str
    razao_social: str
    nome_fantasia: Optional[str]
    situacao: str                      # ATIVA / BAIXADA / SUSPENSA / INAPTA / NULA
    cnae_principal: Optional[str]
    cnae_descricao: Optional[str]
    data_abertura: Optional[str]       # AAAA-MM-DD
    municipio: Optional[str]
    uf: Optional[str]
    email: Optional[str]
    telefone: Optional[str]
    fonte: str = "BRASILAPI"
    bruto: dict[str, Any] = field(default_factory=dict)

    @property
    def bloqueante(self) -> bool:
        return self.situacao.upper() in SITUACOES_BLOQUEANTES

    @property
    def alerta(self) -> bool:
        return self.situacao.upper() in SITUACOES_ALERTA


def _normalizar_texto(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    v = re.sub(r"\s{2,}", " ", str(v)).strip().upper()
    return v or None


def _http_json(url: str) -> dict[str, Any]:
    ultimo: Optional[Exception] = None
    for tentativa in range(1, _TENTATIVAS + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ERP-BWS/1.0"})
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                if resp.status != 200:
                    raise ErroConsultaCNPJ(f"HTTP {resp.status} em {url}")
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise ErroConsultaCNPJ("CNPJ não encontrado na base da Receita.")
            if e.code == 429:
                raise ErroConsultaCNPJ("Limite de consultas atingido — aguarde 1 minuto e tente de novo.")
            ultimo = e
        except Exception as e:
            ultimo = e
        if tentativa < _TENTATIVAS:
            time.sleep(1.5 * tentativa)
    raise ErroConsultaCNPJ(f"Serviço de consulta indisponível: {ultimo}")


def _da_brasilapi(d: dict[str, Any], cnpj: str) -> DadosCNPJ:
    tel = somente_digitos(str(d.get("ddd_telefone_1") or ""))
    return DadosCNPJ(
        cnpj=cnpj,
        razao_social=_normalizar_texto(d.get("razao_social")) or "",
        nome_fantasia=_normalizar_texto(d.get("nome_fantasia")),
        situacao=(_normalizar_texto(d.get("descricao_situacao_cadastral")) or "DESCONHECIDA"),
        cnae_principal=str(d.get("cnae_fiscal") or "") or None,
        cnae_descricao=_normalizar_texto(d.get("cnae_fiscal_descricao")),
        data_abertura=(d.get("data_inicio_atividade") or None),
        municipio=_normalizar_texto(d.get("municipio")),
        uf=_normalizar_texto(d.get("uf")),
        email=(str(d.get("email") or "").strip().lower() or None),
        telefone=tel or None,
        fonte="BRASILAPI", bruto=d,
    )


def _da_receitaws(d: dict[str, Any], cnpj: str) -> DadosCNPJ:
    if str(d.get("status", "")).upper() == "ERROR":
        raise ErroConsultaCNPJ(d.get("message") or "CNPJ rejeitado pela ReceitaWS.")
    atv = (d.get("atividade_principal") or [{}])[0]
    abertura = None
    if d.get("abertura"):                                   # vem DD/MM/AAAA
        p = str(d["abertura"]).split("/")
        if len(p) == 3:
            abertura = f"{p[2]}-{p[1]}-{p[0]}"
    return DadosCNPJ(
        cnpj=cnpj,
        razao_social=_normalizar_texto(d.get("nome")) or "",
        nome_fantasia=_normalizar_texto(d.get("fantasia")),
        situacao=(_normalizar_texto(d.get("situacao")) or "DESCONHECIDA"),
        cnae_principal=somente_digitos(str(atv.get("code") or "")) or None,
        cnae_descricao=_normalizar_texto(atv.get("text")),
        data_abertura=abertura,
        municipio=_normalizar_texto(d.get("municipio")),
        uf=_normalizar_texto(d.get("uf")),
        email=(str(d.get("email") or "").strip().lower() or None),
        telefone=somente_digitos(str(d.get("telefone") or "").split("/")[0]) or None,
        fonte="RECEITAWS", bruto=d,
    )


def consultar_cnpj(cnpj: str) -> DadosCNPJ:
    """Consulta o CNPJ nas fontes públicas. Levanta ValueError para CNPJ
    estruturalmente inválido e ErroConsultaCNPJ para falhas de serviço."""
    dig = somente_digitos(cnpj)
    if not cnpj_valido(dig):
        raise ValueError(f"CNPJ inválido (dígito verificador não confere): {cnpj!r}")
    try:
        return _da_brasilapi(_http_json(f"https://brasilapi.com.br/api/cnpj/v1/{dig}"), dig)
    except ErroConsultaCNPJ as e_primaria:
        try:
            return _da_receitaws(_http_json(f"https://receitaws.com.br/v1/cnpj/{dig}"), dig)
        except ErroConsultaCNPJ:
            raise e_primaria
