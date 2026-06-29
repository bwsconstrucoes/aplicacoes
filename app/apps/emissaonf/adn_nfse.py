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


SEFIN_PROD = "https://sefin.nfse.gov.br/sefinnacional"


def consultar_nfse_por_chave(cert_pem: bytes, chave_pem: bytes, chave_acesso: str,
                             base_sefin: str = SEFIN_PROD, timeout: int = 40) -> str:
    """GET SEFIN /nfse/{chave} (autenticado por certificado) -> XML nacional já
    descompactado. É a fonte rápida do nacional pela chave: sem captcha, sem NSU."""
    cert_path, key_path = _cert_temp(cert_pem, chave_pem)
    url = f"{base_sefin}/nfse/{chave_acesso}"
    r = requests.get(url, headers={"Accept": "application/json"},
                     cert=(cert_path, key_path), timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"SEFIN /nfse/{{chave}} HTTP {r.status_code}: {r.text[:300]}")
    data = r.json()
    b64 = data.get("nfseXmlGZipB64") or data.get("NfseXmlGZipB64") or ""
    if not b64:
        raise RuntimeError(f"SEFIN devolveu 200 mas sem nfseXmlGZipB64: {str(data)[:300]}")
    return _descompactar(b64)


def diag_dps_por_chave(cert_pem: bytes, chave_pem: bytes, chave_acesso: str,
                       base_sefin: str = SEFIN_PROD, timeout: int = 40) -> str:
    """Diagnóstico do caminho 'só certificado': pega o XML nacional pela chave,
    extrai o ID da DPS de dentro dele, e testa se a SEFIN devolve o nacional por
    esse ID. Se sim, o Cron pode montar o ID da DPS pelo número da nota e fechar
    sozinho, sem chave, sem município."""
    import re
    linhas = []
    try:
        xml = consultar_nfse_por_chave(cert_pem, chave_pem, chave_acesso, base_sefin, timeout)
    except Exception as e:
        return f">>> Falha ao obter XML pela chave: {type(e).__name__}: {e}"
    ids = re.findall(r'Id="([^"]+)"', xml)
    dps_ids = [i for i in ids if i.upper().startswith("DPS")]
    linhas.append(f"XML nacional obtido ({len(xml)} chars).")
    linhas.append(f"Todos os Id= no XML: {ids[:12]}")
    linhas.append(f"IDs de DPS encontrados: {dps_ids or '(nenhum começando com DPS)'}")
    if not dps_ids:
        # mostra um trecho pra inspecionar o formato do idDPS manualmente
        m = re.search(r'(DPS[0-9A-Za-z]{20,60})', xml)
        linhas.append(f"Trecho com 'DPS...': {m.group(1) if m else '(não achei)'}")
    cert_path, key_path = _cert_temp(cert_pem, chave_pem)
    cert = (cert_path, key_path)
    testados = set()
    for idd in dps_ids:
        for nome, url in [
            ("GET /dps/{id}",       f"{base_sefin}/dps/{idd}"),
            ("GET /nfse/dps/{id}",  f"{base_sefin}/nfse/dps/{idd}"),
        ]:
            if url in testados:
                continue
            testados.add(url)
            try:
                r = requests.get(url, headers={"Accept": "application/json"}, cert=cert, timeout=timeout)
                ct = r.headers.get("content-type", "")
                corpo = r.text[:240] if any(t in ct for t in ("json", "xml", "text")) else f"<binário {len(r.content)}b>"
                linhas.append(f"[{r.status_code}] {nome}\n      {url}\n      {corpo}")
            except Exception as e:
                linhas.append(f"[ERRO] {nome}\n      {url}\n      {type(e).__name__}: {e}")
    return "\n".join(linhas)


def diag_por_chave(cert_pem: bytes, chave_pem: bytes, chave_acesso: str,
                   base: str = BASE_PROD, timeout: int = 40) -> str:
    """Diagnóstico: tenta vários endpoints de busca POR CHAVE na API de
    contribuinte federal (autenticada por certificado) e relata o que cada um
    responde. Objetivo: descobrir se dá pra pegar a NFS-e / DANFSe nacional pela
    chave SEM captcha e SEM esperar a distribuição por NSU. Tudo GET (sem efeito)."""
    cert_path, key_path = _cert_temp(cert_pem, chave_pem)
    cert = (cert_path, key_path)
    sefin = "https://sefin.nfse.gov.br/sefinnacional"
    candidatos = [
        ("ADN  GET /NFSe/{chave}",        f"{base}/NFSe/{chave_acesso}"),
        ("ADN  GET /NFSe/{chave}/Xml",    f"{base}/NFSe/{chave_acesso}/Xml"),
        ("ADN  GET /NFSe/{chave}/DANFSE", f"{base}/NFSe/{chave_acesso}/DANFSE"),
        ("ADN  GET /DANFSE/{chave}",      f"{base}/DANFSE/{chave_acesso}"),
        ("SEFIN GET /nfse/{chave}",       f"{sefin}/nfse/{chave_acesso}"),
        ("SEFIN GET /danfse/{chave}",     f"{sefin}/danfse/{chave_acesso}"),
    ]
    linhas = []
    for nome, url in candidatos:
        try:
            r = requests.get(url, headers={"Accept": "*/*"}, cert=cert, timeout=timeout)
            ct = r.headers.get("content-type", "")
            if any(t in ct for t in ("json", "xml", "text")):
                corpo = r.text[:280]
            else:
                corpo = f"<binário {len(r.content)} bytes> (provável PDF/zip)"
            linhas.append(f"[{r.status_code}] {nome}\n      {url}\n      content-type: {ct}\n      {corpo}")
        except Exception as e:
            linhas.append(f"[ERRO] {nome}\n      {url}\n      {type(e).__name__}: {e}")
    return "\n\n".join(linhas)


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
