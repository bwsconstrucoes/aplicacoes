# -*- coding: utf-8 -*-
"""
Atualização do título a receber no Omie após a emissão.
Consulta o título (por codigo_lancamento_integracao) e o ALTERA gravando as
retenções (IR, ISS, PIS, COFINS, CSLL, INSS) e o nº do documento fiscal.

Acúmulo de notas: se o título já tiver nota emitida, o novo número é acrescentado
ao campo, virando ex.: '3001/3072'. Em cancelamento, o número é removido do campo.

Credenciais (aba Credenciais): OMIE_KEY, OMIE_SECRET.
"""
from __future__ import annotations
import time
import requests

URL = "https://app.omie.com.br/api/v1/financas/contareceber/"


# mensagens do Omie quando o registro está travado por uma escrita recente
_OMIE_TRAVA = ("já foi processada", "ja foi processada", "está sendo processada",
               "sendo processada", "tente novamente")


def _post(call, param, creds, timeout=40, _tentativa=1):
    body = {"call": call, "param": [param],
            "app_key": creds["OMIE_KEY"], "app_secret": creds["OMIE_SECRET"]}
    r = requests.post(URL, json=body, timeout=timeout)
    corpo = r.text or ""
    # O Omie trava o registro por alguns segundos após uma escrita (ex.: AlterarContaReceber
    # do passo anterior). Espera e tenta de novo — as operações daqui são idempotentes.
    if _tentativa <= 5 and any(m in corpo for m in _OMIE_TRAVA):
        time.sleep(min(2 ** _tentativa, 10))   # 2, 4, 8, 10, 10
        return _post(call, param, creds, timeout=timeout, _tentativa=_tentativa + 1)
    if r.status_code != 200:
        raise RuntimeError(f"Omie {call} HTTP {r.status_code}: {corpo[:200]}")
    data = r.json()
    if isinstance(data, dict) and data.get("faultstring"):
        raise RuntimeError(f"Omie {call}: {data['faultstring']}")
    return data


def consultar(creds, codigo_integracao):
    return _post("ConsultarContaReceber",
                 {"codigo_lancamento_integracao": codigo_integracao}, creds)


def _f(v):
    return float(v)


def _split_docs(s):
    """'3001/3072' -> ['3001','3072'] (ignora vazios/espaços)."""
    return [x for x in str(s or "").replace(" ", "").split("/") if x]


def _merge_doc(atual, novo):
    """Acrescenta 'novo' ao campo, mantendo os que já existem. Não duplica."""
    docs = _split_docs(atual)
    novo = str(novo).strip()
    if novo and novo not in docs:
        docs.append(novo)
    return "/".join(docs)


def _remove_doc(atual, alvo):
    """Remove 'alvo' do campo (usado quando uma nota é cancelada)."""
    alvo = str(alvo).strip()
    return "/".join(d for d in _split_docs(atual) if d != alvo)


def _ler_num_doc(consulta):
    """Acha 'numero_documento_fiscal' na resposta do ConsultarContaReceber."""
    if isinstance(consulta, dict):
        if "numero_documento_fiscal" in consulta:
            return consulta["numero_documento_fiscal"]
        for v in consulta.values():
            achou = _ler_num_doc(v)
            if achou:
                return achou
    elif isinstance(consulta, list):
        for v in consulta:
            achou = _ler_num_doc(v)
            if achou:
                return achou
    return ""


def montar_param_retencoes(codigo_integracao, r, doc_final) -> dict:
    """Monta o param do AlterarContaReceber (puro, sem rede). r = ResultadoCalculo.
    doc_final = string pronta do numero_documento_fiscal (ex.: '3001/3072')."""
    fed = r.federais_retidos
    return {
        "codigo_lancamento_integracao": codigo_integracao,
        "numero_documento_fiscal": str(doc_final),
        "retem_inss": "S", "valor_inss": _f(r.inss),
        "retem_iss": "S" if r.iss_retido else "N", "valor_iss": _f(r.iss) if r.iss_retido else 0.0,
        "retem_ir": "S" if "IR" in fed else "N", "valor_ir": _f(r.ir) if "IR" in fed else 0.0,
        "retem_pis": "S" if "PIS" in fed else "N", "valor_pis": _f(r.pis) if "PIS" in fed else 0.0,
        "retem_cofins": "S" if "COFINS" in fed else "N", "valor_cofins": _f(r.cofins) if "COFINS" in fed else 0.0,
        "retem_csll": "S" if "CSLL" in fed else "N", "valor_csll": _f(r.csll) if "CSLL" in fed else 0.0,
    }


def alterar_retencoes(creds, codigo_integracao, r, numero_nota):
    """Lê o título, acumula o nº da nota no documento fiscal (ex.: 3001/3072) e grava
    as retenções calculadas. Retorna (resposta, doc_final).
    Use na PRIMEIRA nota (com r da medição INTEGRAL)."""
    atual = ""
    try:
        atual = _ler_num_doc(consultar(creds, codigo_integracao))
    except Exception:
        atual = ""                      # se a consulta falhar, grava só o novo número
    doc = _merge_doc(atual, numero_nota)
    param = montar_param_retencoes(codigo_integracao, r, doc)
    return _post("AlterarContaReceber", param, creds), doc


def adicionar_numero(creds, codigo_integracao, numero_nota):
    """Apenas ACUMULA o nº da nota no documento fiscal, SEM tocar nas retenções
    (use da 2ª nota parcial em diante — as retenções já são as da medição integral).
    Envia só a chave + o documento, então o Omie preserva os demais campos."""
    atual = ""
    try:
        atual = _ler_num_doc(consultar(creds, codigo_integracao))
    except Exception:
        atual = ""
    doc = _merge_doc(atual, numero_nota)
    param = {
        "codigo_lancamento_integracao": codigo_integracao,
        "numero_documento_fiscal": str(doc),
    }
    return _post("AlterarContaReceber", param, creds), doc


def remover_documento(creds, codigo_integracao, numero_cancelado):
    """Remove o nº de uma nota cancelada do campo numero_documento_fiscal do título.
    Retorna (resposta, doc_final)."""
    atual = _ler_num_doc(consultar(creds, codigo_integracao))
    novo = _remove_doc(atual, numero_cancelado)
    param = {"codigo_lancamento_integracao": codigo_integracao,
             "numero_documento_fiscal": novo}
    return _post("AlterarContaReceber", param, creds), novo


def substituir_numero(creds, codigo_integracao, numero_antigo, numero_novo):
    """Substituição: remove o nº antigo e adiciona o novo no numero_documento_fiscal
    em UMA ÚNICA chamada AlterarContaReceber. Evita duas escritas seguidas no mesmo
    título (que o Omie bloqueia por trava de registro / consumo redundante).
    Retorna (resposta, doc_final)."""
    atual = _ler_num_doc(consultar(creds, codigo_integracao))
    doc = _merge_doc(_remove_doc(atual, numero_antigo), numero_novo)
    param = {"codigo_lancamento_integracao": codigo_integracao,
             "numero_documento_fiscal": doc}
    return _post("AlterarContaReceber", param, creds), doc


# --------------------------------------------------------------------------- #
# Consulta, equalização e atualização dos TRIBUTOS — as três únicas operações
# que o dono quer manter no Omie (09/10/2026, com estas palavras):
#
#   "Os scripts, eles apenas para consulta, equalização e atualização da parte
#    de tributos no Omie. Se as emissões estiverem todas corretas e gerando
#    títulos corretos, a operação se limitará ao que eu disse e não mais a uma
#    série de outras funções que foram criadas."
#
# Estavam num Apps Script da planilha, com a credencial do Omie em texto claro
# (ver CONTEXTO.md §9). Aqui a credencial vem da aba Credenciais, como todo o
# resto do repositório.
# --------------------------------------------------------------------------- #
_CAMPOS_TRIBUTO = ("pis", "cofins", "csll", "ir", "iss", "inss")


def _achar(no, chave):
    """Procura uma chave em qualquer profundidade da resposta do Omie.

    A resposta aninha de formas diferentes conforme a chamada, e procurar pelo
    caminho exato quebrava a cada variação — foi o motivo de `_ler_num_doc` já
    fazer busca em profundidade."""
    if isinstance(no, dict):
        if chave in no:
            return no[chave]
        for v in no.values():
            achou = _achar(v, chave)
            if achou not in (None, ""):
                return achou
    elif isinstance(no, list):
        for v in no:
            achou = _achar(v, chave)
            if achou not in (None, ""):
                return achou
    return None


def ler_titulo(consulta) -> dict:
    """Tira da resposta do Omie o que a base de faturamento guarda.

    Função PURA: recebe a resposta já obtida. É o que permite testar a leitura
    sem falar com o Omie — e a leitura é a parte que erra em silêncio, porque
    todos os campos são números plausíveis."""
    from decimal import Decimal

    def num(chave):
        v = _achar(consulta, chave)
        try:
            return Decimal(str(v)) if v not in (None, "") else Decimal("0")
        except Exception:
            return Decimal("0")

    def txt(chave):
        v = _achar(consulta, chave)
        return "" if v is None else str(v).strip()

    d = {
        "codigo_lancamento": txt("codigo_lancamento_omie"),
        "codigo_integracao": txt("codigo_lancamento_integracao"),
        "numero_documento": _ler_num_doc(consulta) or "",
        "valor_titulo": num("valor_documento"),
    }
    for t in _CAMPOS_TRIBUTO:
        d[t] = num(f"valor_{t}")
        d[f"retem_{t}"] = (txt(f"retem_{t}") or "N").upper()[:1]
    return d


def ratear(valor_total, pesos) -> list:
    """Divide um valor entre vários itens, proporcional ao peso, FECHANDO AO CENTAVO.

    Por que isto existe e por que fecha ao centavo: **um título do Omie cobre
    várias notas** (a medição é faturada em partes). Para comparar o tributo do
    título com o de cada nota, o valor do título tem de ser dividido — e a soma
    das partes tem de dar exatamente o total, senão a conferência acusa
    divergência de um centavo em toda nota e o dono para de ler o alarme.

    O residual do arredondamento vai para a(s) nota(s) de MAIOR peso. É a regra
    que o Apps Script já usava (`ratearProporcional_`), e é a única parte dele
    que é regra de negócio de verdade.
    """
    from decimal import Decimal
    total_centavos = int((Decimal(str(valor_total or 0)) * 100).to_integral_value())
    pesos = [Decimal(str(p or 0)) for p in pesos]
    soma = sum(pesos)
    if soma <= 0:
        raise ValueError("Rateio impossível: a soma dos pesos é zero.")

    partes = [int((total_centavos * p) / soma) for p in pesos]
    residual = total_centavos - sum(partes)
    # maior peso primeiro; empate decidido pela ordem, para o resultado ser o
    # mesmo em duas rodadas com os mesmos dados
    ordem = sorted(range(len(pesos)), key=lambda i: (-pesos[i], i))
    i = 0
    while residual > 0 and ordem:
        partes[ordem[i % len(ordem)]] += 1
        residual -= 1
        i += 1
    while residual < 0 and ordem:
        partes[ordem[i % len(ordem)]] -= 1
        residual += 1
        i += 1
    return [Decimal(c) / 100 for c in partes]


def montar_param_tributos(codigo_integracao, tributos: dict) -> dict:
    """Param do AlterarContaReceber mexendo SÓ nos tributos.

    Não leva `numero_documento_fiscal` de propósito: o número da nota no título
    é assunto da emissão, e mandá-lo aqui faria a equalização de tributos
    sobrescrever, por tabela, o acúmulo que a emissão monta (ex.: '3001/3072').
    """
    param = {"codigo_lancamento_integracao": codigo_integracao}
    for t in _CAMPOS_TRIBUTO:
        valor = tributos.get(t)
        if valor is None:
            continue
        valor = _f(valor)
        param[f"valor_{t}"] = valor
        param[f"retem_{t}"] = "S" if valor > 0 else "N"
    return param


def alterar_tributos(creds, codigo_integracao, tributos: dict):
    """Grava os tributos equalizados no título. Só os tributos."""
    return _post("AlterarContaReceber",
                 montar_param_tributos(codigo_integracao, tributos), creds)
