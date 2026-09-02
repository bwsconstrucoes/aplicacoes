# -*- coding: utf-8 -*-
"""
omie_client.py — Cliente HTTP para a API do Omie (financas).

Responsabilidades:
  - Autenticacao via app_key/app_secret lidos de variavel de ambiente / .env
    (NUNCA em texto no codigo).
  - Paginacao automatica (registros_por_pagina ate 500, recomendado pela Omie).
  - Retry com backoff exponencial em erros de rede, HTTP 5xx/429/425 e nos
    erros de "consumo indevido" (rate limit) que a Omie devolve via faultstring.
  - Pausa configuravel entre chamadas para respeitar o rate limit.
  - Metodos para os endpoints usados pelo sync: ContasPagar, ContasReceber,
    Movimentos, Categorias, Clientes, ContasCorrentes.
  - Leitura das amostras .txt (offline) para teste headless sem bater na API.

Variaveis de ambiente esperadas (as mesmas do modulo baixabradesco):
  OMIE_BWS_APP_KEY     = sua app_key
  OMIE_BWS_APP_SECRET  = seu app_secret

Uso rapido:
  from omie_client import OmieClient
  cli = OmieClient.de_ambiente()
  for pag, tot_pag, tot_reg, registros in cli.listar_contas_pagar():
      ...
"""
import os
import re
import time
import json
import logging

import requests

VERSAO = "2026-08-12.3"   # carimbo de versao — confira com o diagnostico.py

log = logging.getLogger("omie")

# ----------------------------------------------------------------------------- 
# .env (carregamento minimo, sem depender de python-dotenv)
# ----------------------------------------------------------------------------- 
def _limpar_credencial(s):
    """
    Remove de uma credencial QUALQUER caractere de espaco/invisivel (inclusive
    nao-quebravel \\xa0, zero-width \\u200b-\\u200d/\\u2060 e BOM \\ufeff) que costuma
    vir grudado no copia-e-cola do portal. app_key/app_secret nao tem espacos,
    entao remover todos e seguro.
    """
    s = s or ""
    invisiveis = {0x200b, 0x200c, 0x200d, 0x2060, 0xfeff}
    return "".join(c for c in s if not (c.isspace() or ord(c) in invisiveis))


def carregar_dotenv(caminho=".env"):
    """
    Carrega CHAVE=VALOR de um .env para os.environ.
    Robusto a: BOM (utf-8-sig), aspas, espacos, prefixo 'export'. O valor do .env
    preenche a variavel quando ela ainda nao existe OU esta vazia.
    """
    if not os.path.exists(caminho):
        return
    with open(caminho, "r", encoding="utf-8-sig") as f:  # utf-8-sig descarta o BOM
        for linha in f:
            linha = linha.strip().lstrip("\ufeff")
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, _, valor = linha.partition("=")
            chave = chave.strip()
            if chave.lower().startswith("export "):
                chave = chave[7:].strip()
            valor = valor.strip().strip('"').strip("'").strip()
            os.environ[chave] = valor  # .env tem prioridade (sobrescreve var existente)


# ----------------------------------------------------------------------------- 
# Endpoints
# ----------------------------------------------------------------------------- 
URL_CONTAPAGAR        = "https://app.omie.com.br/api/v1/financas/contapagar/"
URL_CONTARECEBER      = "https://app.omie.com.br/api/v1/financas/contareceber/"
URL_MOVIMENTOS        = "https://app.omie.com.br/api/v1/financas/mf/"
URL_CATEGORIAS        = "https://app.omie.com.br/api/v1/geral/categorias/"
URL_CLIENTES          = "https://app.omie.com.br/api/v1/geral/clientes/"
URL_CONTAS_CORRENTES  = "https://app.omie.com.br/api/v1/geral/contacorrente/"

# Trechos de faultstring da Omie que indicam rate limit / consumo indevido -> vale retry.
_RATE_LIMIT_HINTS = (
    "consumo indevido",
    "consumo redundante",
    "bloqueado por excesso",
    "numero de requisicoes",
    "número de requisições",
    "excedeu",
    "try again",
    "tente novamente",
)


# Trechos de faultstring que indicam erro DEFINITIVO: o recurso nao existe, ponto.
# Retentar so queima tempo e ainda faz a Omie acusar "consumo redundante" (a mesma
# requisicao repetida), entao esses abortam na primeira tentativa.
_ERROS_DEFINITIVOS = (
    "nao cadastrado",
    "não cadastrado",
    "nao encontrado",
    "não encontrado",
    "nao existe",
    "não existe",
    "not found",
    "inexistente",
)


class OmieAPIError(Exception):
    """Erro de negocio retornado pela Omie (faultstring nao relacionado a rate limit)."""
    def __init__(self, faultcode, faultstring):
        self.faultcode = faultcode
        self.faultstring = faultstring
        super().__init__(f"[{faultcode}] {faultstring}")

    @property
    def definitivo(self):
        """True quando nao adianta retentar (registro inexistente)."""
        return _texto_e_definitivo(self.faultstring)


def _texto_e_definitivo(texto):
    t = (texto or "").lower()
    return any(h in t for h in _ERROS_DEFINITIVOS)


def erro_definitivo(exc):
    """True se a excecao for um erro que nao vale retentar nem reprocessar depois."""
    return isinstance(exc, OmieAPIError) and exc.definitivo


class OmieClient:
    def __init__(self, app_key, app_secret, *,
                 pausa_entre_chamadas=0.3, max_tentativas=8,
                 backoff_base=1.6, timeout=120, registros_por_pagina=500):
        if not app_key or not app_secret:
            raise ValueError("app_key/app_secret ausentes. Configure OMIE_APP_KEY e OMIE_APP_SECRET.")
        self.app_key = app_key
        self.app_secret = app_secret
        self.pausa = float(pausa_entre_chamadas)
        self.max_tentativas = int(max_tentativas)
        self.backoff_base = float(backoff_base)
        self.timeout = int(timeout)
        self.registros_por_pagina = int(registros_por_pagina)
        self.sessao = requests.Session()
        self.sessao.headers.update({"Content-Type": "application/json"})

    @classmethod
    def de_ambiente(cls, caminho_env=".env", **kwargs):
        """Le a credencial do ambiente. No Render vem das variaveis do servico;
        localmente, de um .env. Os nomes oficiais sao os do resto do repositorio
        (`baixabradesco/omie.py`); os antigos do painel seguem aceitos para nao
        quebrar quem ja tem .env local."""
        carregar_dotenv(caminho_env)
        key = _limpar_credencial(os.environ.get("OMIE_BWS_APP_KEY")
                                 or os.environ.get("OMIE_APP_KEY"))
        secret = _limpar_credencial(os.environ.get("OMIE_BWS_APP_SECRET")
                                    or os.environ.get("OMIE_APP_SECRET"))
        if not key or not secret:
            faltando = ", ".join(n for n, v in (("OMIE_BWS_APP_KEY", key),
                                                ("OMIE_BWS_APP_SECRET", secret)) if not v)
            raise ValueError(
                f"Credenciais do OMIE ausentes ({faltando}). No Render, cadastre as "
                "variaveis de ambiente OMIE_BWS_APP_KEY e OMIE_BWS_APP_SECRET nas "
                "Settings do servico. Nunca escreva a chave no codigo.")
        return cls(key, secret, **kwargs)

    # ------------------------------------------------------------------ 
    # Chamada bruta com retry/backoff
    # ------------------------------------------------------------------ 
    def _call(self, url, call, param):
        corpo = {
            "call": call,
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            "param": [param],
        }
        ultimo_erro = None
        for tentativa in range(1, self.max_tentativas + 1):
            try:
                resp = self.sessao.post(url, data=json.dumps(corpo), timeout=self.timeout)
            except requests.RequestException as e:
                ultimo_erro = e
                espera = self._espera(tentativa, None)
                log.warning("Rede falhou em %s (tent. %d/%d): %s. Aguardando %.1fs.",
                            call, tentativa, self.max_tentativas, e, espera)
                time.sleep(espera)
                continue

            # HTTP que merece retry. Os 500 da Omie costumam ser TRANSITORIOS (inclusive
            # com faultstring enganosa tipo "chave de acesso") -> sempre retenta com backoff.
            if resp.status_code in (425, 429, 500, 502, 503, 504):
                try:
                    fs = (resp.json() or {}).get("faultstring", "") or ""
                except Exception:
                    fs = (resp.text or "")[:200]
                # A Omie devolve 500 tambem para "registro nao existe". Isso NAO e
                # transitorio: retentar so gasta tempo e ainda gera "consumo
                # redundante" (requisicao identica repetida).
                if _texto_e_definitivo(fs):
                    raise OmieAPIError(resp.status_code, fs)
                espera = self._espera(tentativa, resp.headers.get("Retry-After"))
                # Omie "Consumo redundante" pede um tempo explicito: "Aguarde N segundos".
                m = re.search(r"[Aa]guarde\s+(\d+)\s+segundos", fs)
                if m:
                    espera = max(espera, float(m.group(1)) + 2.0)
                log.warning("HTTP %s em %s (tent. %d/%d): %s. Aguardando %.1fs.",
                            resp.status_code, call, tentativa, self.max_tentativas,
                            fs[:120], espera)
                ultimo_erro = OmieAPIError(resp.status_code, fs or resp.text[:200])
                time.sleep(espera)
                continue

            if resp.status_code != 200:
                # erro definitivo (4xx que nao seja 425/429): credencial/permissao -> nao retenta
                if self._tem_faultstring(resp):
                    self._levantar_fault(resp)
                resp.raise_for_status()

            # 200 — pode ainda carregar faultstring de negocio em JSON
            try:
                dados = resp.json()
            except ValueError:
                ultimo_erro = OmieAPIError(200, "resposta nao-JSON")
                time.sleep(self._espera(tentativa, None))
                continue

            if isinstance(dados, dict) and dados.get("faultstring"):
                fs = dados.get("faultstring", "")
                if not _texto_e_definitivo(fs) and self._texto_e_rate_limit(fs):
                    espera = self._espera(tentativa, None)
                    log.warning("Rate limit (faultstring) em %s. Aguardando %.1fs.", call, espera)
                    time.sleep(espera)
                    continue
                raise OmieAPIError(dados.get("faultcode"), fs)

            return dados

        raise OmieAPIError("MAX_TENTATIVAS", f"Falha apos {self.max_tentativas} tentativas em {call}: {ultimo_erro}")

    def _espera(self, tentativa, retry_after):
        if retry_after:
            try:
                return max(1.0, float(retry_after))
            except (TypeError, ValueError):
                pass
        return self.backoff_base ** tentativa

    @staticmethod
    def _tem_faultstring(resp):
        try:
            return bool(resp.json().get("faultstring"))
        except Exception:
            return False

    @classmethod
    def _faultstring_e_rate_limit(cls, resp):
        try:
            return cls._texto_e_rate_limit(resp.json().get("faultstring", ""))
        except Exception:
            return False

    @staticmethod
    def _texto_e_rate_limit(texto):
        t = (texto or "").lower()
        return any(h in t for h in _RATE_LIMIT_HINTS)

    @staticmethod
    def _levantar_fault(resp):
        try:
            d = resp.json()
            raise OmieAPIError(d.get("faultcode"), d.get("faultstring"))
        except OmieAPIError:
            raise
        except Exception:
            resp.raise_for_status()

    # ------------------------------------------------------------------ 
    # Paginacao generica
    # ------------------------------------------------------------------ 
    def _listar(self, url, call, chave_registros, *, param_extra=None,
                campo_pagina="pagina", campo_regpp="registros_por_pagina",
                campo_totpag="total_de_paginas", campo_totreg="total_de_registros",
                pagina_inicial=1, max_paginas=None):
        """
        Generator que percorre todas as paginas.
        Devolve tuplas: (pagina_atual, total_paginas, total_registros, [registros]).
        """
        pagina = int(pagina_inicial)
        total_paginas = None
        total_registros = None
        while True:
            param = {campo_pagina: pagina, campo_regpp: self.registros_por_pagina}
            if param_extra:
                param.update(param_extra)
            dados = self._call(url, call, param)
            if total_paginas is None:
                total_paginas = int(dados.get(campo_totpag, 1) or 1)
                total_registros = int(dados.get(campo_totreg, 0) or 0)
                if max_paginas:
                    total_paginas = min(total_paginas, int(max_paginas))
            registros = dados.get(chave_registros, []) or []
            yield pagina, total_paginas, total_registros, registros
            if pagina >= total_paginas:
                break
            pagina += 1
            time.sleep(self.pausa)

    # ------------------------------------------------------------------ 
    # Endpoints especificos
    # ------------------------------------------------------------------ 
    def listar_contas_pagar(self, *, param_extra=None, max_paginas=None, pagina_inicial=1):
        return self._listar(URL_CONTAPAGAR, "ListarContasPagar", "conta_pagar_cadastro",
                            param_extra=param_extra, max_paginas=max_paginas, pagina_inicial=pagina_inicial)

    def listar_contas_receber(self, *, param_extra=None, max_paginas=None, pagina_inicial=1):
        return self._listar(URL_CONTARECEBER, "ListarContasReceber", "conta_receber_cadastro",
                            param_extra=param_extra, max_paginas=max_paginas, pagina_inicial=pagina_inicial)

    def listar_movimentos(self, *, param_extra=None, max_paginas=None):
        # Movimentos usa campos com prefixo "n" e registros em "movimentos".
        return self._listar(URL_MOVIMENTOS, "ListarMovimentos", "movimentos",
                            param_extra=param_extra,
                            campo_pagina="nPagina", campo_regpp="nRegPorPagina",
                            campo_totpag="nTotPaginas", campo_totreg="nTotRegistros",
                            max_paginas=max_paginas)

    def listar_categorias(self, *, max_paginas=None):
        return self._listar(URL_CATEGORIAS, "ListarCategorias", "categoria_cadastro",
                            max_paginas=max_paginas)

    def listar_clientes(self, *, param_extra=None, max_paginas=None):
        extra = {"clientesFiltro": {}}  # vazio = todos (clientes E fornecedores)
        if param_extra:
            extra.update(param_extra)
        return self._listar(URL_CLIENTES, "ListarClientes", "clientes_cadastro",
                            param_extra=extra, max_paginas=max_paginas)

    def listar_contas_correntes(self, *, max_paginas=None):
        return self._listar(URL_CONTAS_CORRENTES, "ListarContasCorrentes", "ListarContasCorrentes",
                            max_paginas=max_paginas)

    # ------------------------------------------------------------------
    # Consulta de UM titulo. Necessario porque a listagem (ListarContas*) devolve
    # um subconjunto dos campos — a observacao, por exemplo, so vem aqui.
    # Custo: 1 chamada por titulo. Use com parcimonia (ver backfill no sync_omie).
    # ------------------------------------------------------------------
    def consultar_conta_pagar(self, codigo_lancamento_omie):
        return self._call(URL_CONTAPAGAR, "ConsultarContaPagar",
                          {"codigo_lancamento_omie": int(codigo_lancamento_omie)})

    def consultar_conta_receber(self, codigo_lancamento_omie):
        return self._call(URL_CONTARECEBER, "ConsultarContaReceber",
                          {"codigo_lancamento_omie": int(codigo_lancamento_omie)})

    def consultar_titulo(self, codigo_lancamento_omie, natureza):
        """natureza 'P' (pagar) ou 'R' (receber)."""
        if (natureza or "").upper().startswith("R"):
            return self.consultar_conta_receber(codigo_lancamento_omie)
        return self.consultar_conta_pagar(codigo_lancamento_omie)


# ----------------------------------------------------------------------------- 
# Leitura de amostras (offline) — para teste headless sem bater na API
# ----------------------------------------------------------------------------- 
def carregar_amostra(caminho):
    """
    Le um arquivo de amostra .txt (resposta JSON da Omie) de forma resiliente.
    Trata o caso da amostra vir SEM a chave '{' inicial (truncada).
    Retorna o dict completo (com 'registros'/'total_*' e a lista de cadastro).
    """
    with open(caminho, "r", encoding="utf-8-sig") as f:
        texto = f.read().strip()
    if not texto.startswith("{"):
        texto = "{" + texto
    return json.loads(texto)


def registros_da_amostra(dados):
    """Extrai a lista de registros de um payload Omie, qualquer que seja a chave de cadastro."""
    for chave in ("conta_pagar_cadastro", "conta_receber_cadastro", "movimentos",
                  "categoria_cadastro", "clientes_cadastro"):
        if chave in dados:
            return chave, dados.get(chave) or []
    return None, []