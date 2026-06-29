# -*- coding: utf-8 -*-
"""
Cliente da API do ADN (Ambiente de Dados Nacional) do NFS-e nacional.
Distribuição de documentos para o contribuinte — autenticação TLS mútuo (mTLS)
com o MESMO certificado A1 (.p12) que usamos para assinar.

Endpoints (Manual ADN v1.0 / Swagger "API NFS-e - ADN Contribuinte"):
  GET /DFe/{NSU}?lote=true[&cnpjConsulta=...]   -> lote de DF-e a partir do NSU
  GET /NFSe/{ChaveAcesso}/Eventos               -> eventos vinculados à chave

O conteúdo de cada documento vem em ArquivoXml = base64( gzip( xml ) ).

Uso típico (polling):
    docs, novo_nsu, status = baixar_lote(cert_pem, chave_pem, ultimo_nsu)
    # filtra TipoDocumento == 'NFSE', extrai chave + xml, salva, atualiza ultimo_nsu
"""
from __future__ import annotations
import base64
import gzip
import tempfile
import time

import requests

BASE_PROD = "https://adn.nfse.gov.br/contribuintes"                    # produção (confirmar)
BASE_RESTRITA = "https://adn.producaorestrita.nfse.gov.br/contribuintes"  # produção restrita (testes)

# O endpoint de distribuição do ADN tem limite de frequência (HTTP 429). Estes
# parâmetros controlam o backoff e o ritmo da varredura.
PACING_PAGINAS = 4        # segundos de pausa entre páginas da varredura
BACKOFF_INICIAL = 20      # 1ª espera no 429 (cresce ao dobro até o teto)
BACKOFF_TETO = 120
BACKOFF_TENTATIVAS = 5


def _get_backoff(url, *, params=None, cert, timeout):
    """GET que respeita o rate limit do ADN: em 429 espera (Retry-After ou
    backoff exponencial) e tenta de novo. Demais status voltam pro chamador."""
    espera = BACKOFF_INICIAL
    for i in range(BACKOFF_TENTATIVAS):
        r = requests.get(url, params=params, headers={"Accept": "application/json"},
                         cert=cert, timeout=timeout)
        if r.status_code != 429:
            return r
        ra = r.headers.get("Retry-After")
        wait = int(ra) if (ra and str(ra).strip().isdigit()) else espera
        if i == BACKOFF_TENTATIVAS - 1:
            raise RuntimeError(
                f"ADN HTTP 429 (limite de requisições) após {BACKOFF_TENTATIVAS} tentativas. "
                f"A janela de limite ainda não liberou — espere alguns minutos e rode o job de novo.")
        print(f"  [ADN] 429 (limite de requisições) — aguardando {wait}s e repetindo "
              f"({i + 1}/{BACKOFF_TENTATIVAS})...")
        time.sleep(wait)
        espera = min(espera * 2, BACKOFF_TETO)
    return r


def _cert_temp(cert_pem: bytes, chave_pem: bytes):
    cf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); cf.write(cert_pem); cf.close()
    kf = tempfile.NamedTemporaryFile("wb", suffix=".pem", delete=False); kf.write(chave_pem); kf.close()
    return cf.name, kf.name


def _descompactar(arquivo_xml_b64: str) -> str:
    """ArquivoXml -> XML. base64 -> gzip -> texto UTF-8."""
    if not arquivo_xml_b64:
        return ""
    bruto = base64.b64decode(arquivo_xml_b64)
    try:
        return gzip.decompress(bruto).decode("utf-8")
    except (OSError, EOFError):
        return bruto.decode("utf-8", "replace")   # caso não esteja compactado


def _parse_lote(data: dict, ultimo_nsu: int) -> dict:
    docs = []
    maior = int(ultimo_nsu)
    for d in (data.get("LoteDFe") or []):
        nsu = d.get("NSU")
        if nsu is not None:
            maior = max(maior, int(nsu))
        docs.append({
            "nsu": nsu,
            "chave": d.get("ChaveAcesso"),
            "tipo": d.get("TipoDocumento"),          # DPS / NFSE / EVENTO / ...
            "tipo_evento": d.get("TipoEvento"),
            "xml": _descompactar(d.get("ArquivoXml") or ""),
            "gerado_em": d.get("DataHoraGeracao"),
        })
    return {
        "status": data.get("StatusProcessamento"),   # DOCUMENTOS_LOCALIZADOS / NENHUM... / REJEICAO
        "docs": docs,
        "ultimo_nsu": maior,
        "erros": data.get("Erros") or [],
        "alertas": data.get("Alertas") or [],
        "ambiente": data.get("TipoAmbiente"),
        "raw": data,
    }


def baixar_lote(cert_pem: bytes, chave_pem: bytes, ultimo_nsu: int = 0,
                cnpj_consulta: str | None = None, base: str = BASE_PROD,
                timeout: int = 60) -> dict:
    """Chama GET /DFe/{NSU} e devolve o lote já com os XMLs descompactados."""
    cert_path, key_path = _cert_temp(cert_pem, chave_pem)
    url = f"{base}/DFe/{int(ultimo_nsu)}"
    params = {"lote": "true"}
    if cnpj_consulta:
        params["cnpjConsulta"] = "".join(c for c in str(cnpj_consulta) if c.isdigit())
    r = _get_backoff(url, params=params, cert=(cert_path, key_path), timeout=timeout)
    if r.status_code not in (200, 400, 404):
        raise RuntimeError(f"ADN /DFe HTTP {r.status_code}: {r.text[:300]}")
    return _parse_lote(r.json(), ultimo_nsu)


def consultar_eventos(cert_pem: bytes, chave_pem: bytes, chave_acesso: str,
                      base: str = BASE_PROD, timeout: int = 60) -> dict:
    """Chama GET /NFSe/{ChaveAcesso}/Eventos — eventos (cancelamento, substituição...)."""
    cert_path, key_path = _cert_temp(cert_pem, chave_pem)
    url = f"{base}/NFSe/{chave_acesso}/Eventos"
    r = _get_backoff(url, cert=(cert_path, key_path), timeout=timeout)
    if r.status_code not in (200, 404):
        raise RuntimeError(f"ADN /Eventos HTTP {r.status_code}: {r.text[:300]}")
    return _parse_lote(r.json(), 0)


def varrer_tudo(cert_pem: bytes, chave_pem: bytes, ultimo_nsu: int = 0,
                cnpj_consulta: str | None = None, base: str = BASE_PROD,
                max_paginas: int = 50) -> dict:
    """Pagina a distribuição até acabar (NENHUM_DOCUMENTO_LOCALIZADO) ou bater o limite.
    Retorna todos os docs novos e o NSU final para você persistir."""
    todos, nsu = [], int(ultimo_nsu)
    for i in range(max_paginas):
        if i > 0:
            time.sleep(PACING_PAGINAS)   # ritmo entre páginas (evita 429)
        lote = baixar_lote(cert_pem, chave_pem, nsu, cnpj_consulta, base)
        todos.extend(lote["docs"])
        if lote["status"] != "DOCUMENTOS_LOCALIZADOS" or lote["ultimo_nsu"] <= nsu:
            nsu = lote["ultimo_nsu"]
            break
        nsu = lote["ultimo_nsu"]
    return {"docs": todos, "ultimo_nsu": nsu}