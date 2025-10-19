import os, json, re
from typing import Dict, Any

def _truncate(txt: str, max_chars: int = 12000) -> str:
    if not txt:
        return ""
    if len(txt) <= max_chars:
        return txt
    return txt[:max_chars]

def _call_external_ai(text: str, filename: str) -> Dict[str, Any]:
    """
    Integração real com OpenAI (gpt-4o-mini) para extrair campos financeiros.
    Requer variáveis:
      - OPENAI_API_KEY
      - AI_MODEL (ex: gpt-4o-mini)
    """
    provider = os.getenv("AI_PROVIDER", "openai").lower()
    if provider != "openai":
        # Mantém compatível: se quiser plugar outro provedor depois
        return {
            "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "",
            "ValorNum": 0.0, "Vencimento": "", "Código de Barras": "", "Tipo": "AI",
            "Status": "INCOMPLETO", "Chave de Acesso": "", "Data Emissão": "",
            "Banco": "", "Linha Digitável": "", "Confidence": 0.60, "Fonte": "LLM-SIMULATED"
        }

    from openai import OpenAI
    api_key = os.getenv("OPENAI_API_KEY", "")
    model = os.getenv("AI_MODEL", "gpt-4o-mini")
    if not api_key:
        return {
            "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "",
            "ValorNum": 0.0, "Vencimento": "", "Código de Barras": "", "Tipo": "AI",
            "Status": "INCOMPLETO", "Chave de Acesso": "", "Data Emissão": "",
            "Banco": "", "Linha Digitável": "", "Confidence": 0.0, "Fonte": "AI-NO-KEY"
        }

    client = OpenAI(api_key=api_key)

    prompt_user = f"""
Você é um extrator de documentos financeiros do Brasil. Analise o conteúdo abaixo (texto extraído de PDF/XML/OCR) e retorne APENAS um JSON válido com os campos pedidos.

TEXTO({filename}):
{text}

Regras:
- Valores BR (1.234,56) -> ValorNum (float, ponto).
- Datas -> "YYYY-MM-DD" quando conseguir inferir.
- CNPJ/CPF -> apenas dígitos (11 ou 14).
- Se houver linha digitável (47/48 dígitos) e/ou código de barras (44), retorne nos campos.
- Em NF-e: fornecedor = emitente; extrair também Série, Número e Chave de Acesso se possível.
- Em boleto: fornecedor (favorecido/beneficiário/cedente), banco, vencimento e valor.

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

    try:
        # Responses API com saída JSON compacta
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": "Você extrai dados financeiros com precisão e retorna somente JSON válido."},
                {"role": "user", "content": _truncate(prompt_user, 16000)}
            ],
            temperature=0.1,
            max_output_tokens=800,
            response_format={"type": "json_object"}
        )
        raw = resp.output_text
        # fallback para tentar achar primeiro objeto JSON
        if not raw.strip().startswith("{"):
            m = re.search(r"\{.*\}", raw, re.S)
            raw = m.group(0) if m else "{}"
        data = json.loads(raw)
    except Exception as e:
        # Em caso de erro da IA, devolve estrutura mínima
        return {
            "Fornecedor": "", "CNPJ": "", "Nº NF": "", "Série": "", "Valor (R$)": "",
            "ValorNum": 0.0, "Vencimento": "", "Código de Barras": "", "Tipo": "AI",
            "Status": "INCOMPLETO", "Chave de Acesso": "", "Data Emissão": "",
            "Banco": "", "Linha Digitável": "", "Confidence": 0.0, "Fonte": f"AI-ERR:{type(e).__name__}"
        }

    # Normaliza chaves de saída para bater com o schema do Sheets
    out: Dict[str, Any] = {
        "Fornecedor": data.get("Fornecedor", "") or "",
        "CNPJ": (data.get("CNPJ", "") or "").replace(".", "").replace("-", "").replace("/", ""),
        "Nº NF": data.get("NumeroNF", "") or "",
        "Série": data.get("Serie", "") or "",
        "Valor (R$)": "",  # mantemos vazio; usamos ValorNum para somatório
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
