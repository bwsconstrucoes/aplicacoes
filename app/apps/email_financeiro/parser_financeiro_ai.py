# -*- coding: utf-8 -*-
"""
Fase 2: OCR + IA (com fallback para parser_v2).
Requer ENV (opcional p/ IA):
  ENABLE_FINANCIAL_AI=true
  AI_PROVIDER=openai
  AI_MODEL=gpt-4o-mini
  OPENAI_API_KEY=sk-...
"""

import os
from typing import Dict, Any

# Fallback rule-based + OCR util
from .parser_financeiro_v2 import extract_financial_data_v2
from .ocr_utils import extract_text_with_ocr

# -------- IA (OpenAI) ----------
def _call_external_ai(text: str, filename: str) -> Dict[str, Any]:
    provider = os.getenv("AI_PROVIDER", "openai").lower()
    if provider != "openai":
        # Sem provedor implementado: devolve estrutura mínima
        return {
            "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "",
            "ValorNum": 0.0, "Vencimento": "", "Código de Barras": "", "Tipo": "AI",
            "Status": "INCOMPLETO", "Chave de Acesso": "", "Data Emissão": "",
            "Banco": "", "Linha Digitável": "", "Confidence": 0.60, "Fonte": "LLM-SIMULATED"
        }

    api_key = os.getenv("OPENAI_API_KEY", "")
    model = os.getenv("AI_MODEL", "gpt-4o-mini")
    if not api_key:
        return {
            "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "",
            "ValorNum": 0.0, "Vencimento": "", "Código de Barras": "", "Tipo": "AI",
            "Status": "INCOMPLETO", "Chave de Acesso": "", "Data Emissão": "",
            "Banco": "", "Linha Digitável": "", "Confidence": 0.0, "Fonte": "AI-NO-KEY"
        }

    try:
        # Import local para não quebrar se a lib não existir
        from openai import OpenAI
        import json, re

        client = OpenAI(api_key=api_key)

        def _truncate(txt: str, max_chars: int = 12000) -> str:
            return txt if not txt or len(txt) <= max_chars else txt[:max_chars]

        prompt_user = f"""
Você é um extrator de documentos financeiros do Brasil. Analise o conteúdo abaixo (texto extraído de PDF/XML/OCR) e retorne APENAS um JSON válido com os campos pedidos.

TEXTO({filename}):
{text}

Regras:
- Valores BR (1.234,56) -> ValorNum (float, ponto).
- Datas -> "YYYY-MM-DD".
- CNPJ/CPF -> apenas dígitos.
- Se houver linha digitável (47/48) ou código de barras (44), preencha.
- Em NF-e: fornecedor = emitente; extraia Série, Número e Chave de Acesso.

Campos (exatamente estas chaves):
{{
 "TipoDoc": "boleto|nfe|duplicata|outro",
 "Fornecedor": "",
 "CNPJ": "",
 "NumeroNF": "",
 "Serie": "",
 "ChaveAcesso": "",
 "DataEmissao": "",
 "Vencimento": "",
 "ValorNum": 0.0,
 "LinhaDigitavel": "",
 "CodigoBarras": "",
 "Banco": "",
 "Observacoes": "",
 "Confidence": 0.0
}}
"""

        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": "Você extrai dados financeiros e retorna somente JSON válido."},
                {"role": "user", "content": _truncate(prompt_user, 16000)}
            ],
            temperature=0.1,
            max_output_tokens=800,
            response_format={"type": "json_object"}
        )
        raw = resp.output_text
        if not raw.strip().startswith("{"):
            m = re.search(r"\{.*\}", raw, re.S)
            raw = m.group(0) if m else "{}"
        data = json.loads(raw)
    except Exception as e:
        return {
            "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "",
            "ValorNum": 0.0, "Vencimento": "", "Código de Barras": "", "Tipo": "AI",
            "Status": "INCOMPLETO", "Chave de Acesso": "", "Data Emissão": "",
            "Banco": "", "Linha Digitável": "", "Confidence": 0.0, "Fonte": f"AI-ERR:{type(e).__name__}"
        }

    out: Dict[str, Any] = {
        "Fornecedor": data.get("Fornecedor", "") or "",
        "CNPJ": (data.get("CNPJ", "") or "").replace(".", "").replace("-", "").replace("/", ""),
        "Nº NF": data.get("NumeroNF", "") or "",
        "Série": data.get("Serie", "") or "",
        "Valor (R$)": "",
        "ValorNum": float(data.get("ValorNum", 0.0) or 0.0),
        "Vencimento": data.get("Vencimento", "") or "",
        "Código de Barras": data.get("CodigoBarras", "") or "",
        "Tipo": "AI",
        "Status": "OK_AI" if (data.get("ValorNum", 0) or data.get("CodigoBarras")) else "INCOMPLETO",
        "Chave de Acesso": data.get("ChaveAcesso", "") or "",
        "Data Emissão": data.get("DataEmissao", "") or "",
        "Banco": data.get("Banco", "") or "",
        "Linha Digitável": data.get("LinhaDigitavel", "") or "",
        "Confidence": float(data.get("Confidence", 0.7) or 0.7),
        "Fonte": "OPENAI"
    }
    return out


# -------- Entrada principal que o coletor importa --------
def extract_financial_data_ai(filename: str, b: bytes) -> Dict[str, Any]:
    """
    Estratégia:
    - Usa parser_v2 (rápido). Se OK, retorna.
    - Se incompleto, executa OCR e tenta IA (se ENABLE_FINANCIAL_AI=true).
    - Sem IA: retorna RULES+OCR (não quebra).
    """
    use_ai = os.getenv("ENABLE_FINANCIAL_AI", "false").lower() in ("1", "true", "yes")

    base = extract_financial_data_v2(filename, b)
    ok_base = base.get("Status") == "OK" and (base.get("ValorNum", 0.0) > 0 or base.get("Código de Barras"))

    if ok_base and not use_ai:
        base["Confidence"] = 0.85
        base["Fonte"] = "RULES"
        return base

    text_ocr = extract_text_with_ocr(b, filename)

    if not use_ai:
        if base.get("Status") != "OK" and text_ocr:
            base["Status"] = "INCOMPLETO_OCR"
        base["Confidence"] = 0.65 if text_ocr else 0.50
        base["Fonte"] = "RULES+OCR" if text_ocr else "RULES"
        return base

    ai = _call_external_ai(text_ocr or "", filename)

    merged = base.copy()
    for k, v in ai.items():
        if k not in merged or not merged[k]:
            merged[k] = v

    if merged.get("ValorNum", 0.0) > 0 or merged.get("Código de Barras"):
        merged["Status"] = "OK_AI"
    merged["Fonte"] = ai.get("Fonte", "AI")
    merged["Confidence"] = ai.get("Confidence", 0.70)

    return merged
