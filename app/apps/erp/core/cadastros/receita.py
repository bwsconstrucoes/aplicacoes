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
    # O ENDEREÇO (migração 056). A declaração que vai para a prefeitura exige o
    # endereço de quem recebe o serviço; sem isto alguém teria de digitar à mão
    # o que a Receita já entrega de graça na mesma consulta.
    cep: Optional[str] = None
    logradouro: Optional[str] = None
    numero: Optional[str] = None
    complemento: Optional[str] = None
    bairro: Optional[str] = None
    codigo_ibge: Optional[str] = None
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


def _ibge_de(municipio: Optional[str], uf: Optional[str]) -> Optional[str]:
    """O código IBGE do município, pela tabela offline que já existe no repo.

    Só é usado quando a fonte não devolve o código — a ReceitaWS não devolve.
    A tabela é a do `emissaonf`, baixada uma vez do IBGE: reusar é melhor do
    que ter duas listas de 5.570 municípios que podem divergir.
    """
    if not municipio or not uf:
        return None
    try:
        import os

        from app.apps.emissaonf import municipios_ibge as mapa
        # Só a tabela que já está no repositório: `carregar_cache` sabe baixar
        # do IBGE se o arquivo faltar, e uma ida à rede no meio de um cadastro
        # é justamente o que não se quer aqui.
        if not os.path.exists(mapa.CACHE_PADRAO):
            return None
        return str(mapa.resolver(municipio, mapa.carregar_cache(), uf)) or None
    except Exception:
        # Município ambíguo, nome fora do padrão, tabela indisponível: fica sem
        # o código e a pessoa preenche. Chutar aqui poria a nota no município
        # errado, que é problema de fisco, não de cadastro.
        return None


def _da_brasilapi(d: dict[str, Any], cnpj: str) -> DadosCNPJ:
    tel = somente_digitos(str(d.get("ddd_telefone_1") or ""))
    municipio = _normalizar_texto(d.get("municipio"))
    uf = _normalizar_texto(d.get("uf"))
    return DadosCNPJ(
        cnpj=cnpj,
        razao_social=_normalizar_texto(d.get("razao_social")) or "",
        nome_fantasia=_normalizar_texto(d.get("nome_fantasia")),
        situacao=(_normalizar_texto(d.get("descricao_situacao_cadastral")) or "DESCONHECIDA"),
        cnae_principal=str(d.get("cnae_fiscal") or "") or None,
        cnae_descricao=_normalizar_texto(d.get("cnae_fiscal_descricao")),
        data_abertura=(d.get("data_inicio_atividade") or None),
        municipio=municipio,
        uf=uf,
        email=(str(d.get("email") or "").strip().lower() or None),
        telefone=tel or None,
        cep=somente_digitos(str(d.get("cep") or "")) or None,
        logradouro=_normalizar_texto(
            " ".join(x for x in (d.get("descricao_tipo_de_logradouro"),
                                 d.get("logradouro")) if x)),
        numero=(str(d.get("numero") or "").strip() or None),
        complemento=_normalizar_texto(d.get("complemento")),
        bairro=_normalizar_texto(d.get("bairro")),
        codigo_ibge=(str(d.get("codigo_municipio_ibge") or "").strip()
                     or _ibge_de(municipio, uf)),
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
        cep=somente_digitos(str(d.get("cep") or "")) or None,
        logradouro=_normalizar_texto(d.get("logradouro")),
        numero=(str(d.get("numero") or "").strip() or None),
        complemento=_normalizar_texto(d.get("complemento")),
        bairro=_normalizar_texto(d.get("bairro")),
        # A ReceitaWS não devolve o código do município: sai da tabela offline.
        codigo_ibge=_ibge_de(_normalizar_texto(d.get("municipio")),
                             _normalizar_texto(d.get("uf"))),
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
