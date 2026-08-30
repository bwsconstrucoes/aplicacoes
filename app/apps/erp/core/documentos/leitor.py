# ============================================================================
# ERP — core/documentos/leitor.py
# Lê o documento anexado e devolve os campos para pré-preencher o lançamento.
#
# Estratégia por tipo, do mais confiável para o menos:
#   XML de NFe  → parser determinístico (chave, emitente, valor, duplicatas).
#                 Zero IA, zero chance de alucinação.
#   PDF         → texto extraído com pdfplumber e interpretado pela IA. Se o
#                 PDF for imagem (sem texto), cai para a leitura visual.
#   Imagem      → leitura visual pela IA.
#
# A IA NUNCA decide sozinha: devolve os campos como SUGESTÃO, com o que
# encontrou de literal no documento, e o financeiro confirma na tela. Também
# sugere a dedutibilidade (decisão que é do documento, não da categoria) —
# marcada com origem "IA" para ser confirmada.
#
# Reusa OPENAI_API_KEY, já configurada no serviço.
# ============================================================================
from __future__ import annotations

import base64
import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

_MODELO = os.getenv("ERP_MODELO_IA", "gpt-4o-mini")
_MAX_TEXTO = 14000          # caracteres enviados à IA
MAX_ARQUIVO_BYTES = 15 * 1024 * 1024


class ErroLeitura(Exception):
    """Documento ilegível ou serviço de leitura indisponível."""


_INSTRUCAO = """Você extrai dados de documentos fiscais brasileiros para um ERP de construção civil.
Responda SOMENTE com um objeto JSON, sem markdown, sem explicação, no formato:

{
 "tipo_documento": "NFE|NFSE|CTE|RECIBO|RPA|FATURA|GUIA|CONTRATO|BOLETO|COMPROVANTE|OUTRO",
 "emitente_nome": "razão social de QUEM EMITIU (o credor)",
 "emitente_documento": "CNPJ ou CPF do emitente, só dígitos",
 "destinatario_documento": "CNPJ do tomador, só dígitos",
 "numero_documento": "número da nota/recibo",
 "data_emissao": "AAAA-MM-DD",
 "valor_total": "0000.00",
 "valor_liquido": "0000.00 se o documento trouxer líquido diferente do total",
 "descricao": "descrição curta do que foi comprado ou do serviço prestado",
 "parcelas": [{"vencimento": "AAAA-MM-DD", "valor": "0000.00", "linha_digitavel": "só dígitos ou vazio"}],
 "retencoes": [{"tipo": "INSS|ISS|IRRF|PCC", "base_calculo": "0000.00", "aliquota": "0.00", "valor": "0000.00"}],
 "obra_mencionada": "nome/código da obra citado no documento, se houver",
 "dedutibilidade_sugerida": "DEDUTIVEL|INDEDUTIVEL|PARCIAL|PENDENTE",
 "dedutibilidade_motivo": "por que, em uma frase",
 "confianca": "ALTA|MEDIA|BAIXA",
 "observacoes": "o que ficou ilegível ou ambíguo"
}

Regras:
- Use SOMENTE o que está escrito no documento. Campo ausente = string vazia, nunca invente.
- Valores em ponto decimal, sem R$ e sem separador de milhar.
- O emitente é o credor (quem vai receber), não a BWS Construções.
- Multa de trânsito, multa punitiva e brinde sem vínculo com a atividade: INDEDUTIVEL.
- Nota fiscal de material/serviço aplicado à obra: DEDUTIVEL.
- Se não houver base para decidir: PENDENTE."""


def _cliente():
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        raise ErroLeitura("OPENAI_API_KEY não configurada — a leitura automática está indisponível. "
                          "Preencha os campos manualmente.")
    try:
        from openai import OpenAI
    except ImportError:
        raise ErroLeitura("Biblioteca da OpenAI indisponível no serviço.")
    return OpenAI(api_key=chave)


def _extrair_json(texto: str) -> dict[str, Any]:
    limpo = re.sub(r"^```(?:json)?|```$", "", (texto or "").strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(limpo)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", limpo, re.DOTALL)
        if not m:
            raise ErroLeitura("A leitura automática não devolveu dados estruturados.")
        return json.loads(m.group(0))


def _texto_do_pdf(conteudo: bytes) -> str:
    """Texto do PDF. Devolve vazio quando o PDF é imagem escaneada."""
    import gc
    import io
    try:
        import pdfplumber
    except ImportError:
        return ""
    partes: list[str] = []
    pdf = None
    try:
        pdf = pdfplumber.open(io.BytesIO(conteudo))
        for pagina in pdf.pages[:8]:          # nota fiscal não passa disso
            partes.append(pagina.extract_text() or "")
            pagina.flush_cache()
    except Exception as e:
        logger.warning("ERP: falha ao extrair texto do PDF: %s", e)
        return ""
    finally:
        if pdf is not None:
            pdf.close()
        gc.collect()
    return "\n".join(partes).strip()


def _ler_com_ia(texto: str = "", imagem_b64: str = "", media_type: str = "") -> dict[str, Any]:
    cliente = _cliente()
    if imagem_b64:
        conteudo: Any = [
            {"type": "text", "text": "Extraia os dados deste documento."},
            {"type": "image_url",
             "image_url": {"url": f"data:{media_type};base64,{imagem_b64}"}},
        ]
    else:
        conteudo = f"Documento (texto extraído):\n\n{texto[:_MAX_TEXTO]}"
    try:
        resp = cliente.chat.completions.create(
            model=_MODELO, temperature=0,
            messages=[{"role": "system", "content": _INSTRUCAO},
                      {"role": "user", "content": conteudo}],
        )
    except Exception as e:
        logger.exception("ERP: falha na chamada da IA")
        raise ErroLeitura(f"Serviço de leitura indisponível: {e}")
    return _extrair_json(resp.choices[0].message.content or "")


def ler_documento(conteudo: bytes, nome_arquivo: str) -> dict[str, Any]:
    """Lê o anexo e devolve os campos sugeridos + a origem da leitura."""
    if not conteudo:
        raise ErroLeitura("Arquivo vazio.")
    if len(conteudo) > MAX_ARQUIVO_BYTES:
        raise ErroLeitura(f"Arquivo acima de {MAX_ARQUIVO_BYTES // (1024*1024)} MB.")
    ext = os.path.splitext(nome_arquivo or "")[1].lower()

    # ---- XML de NFe: leitura exata, sem IA
    if ext == ".xml" or conteudo[:200].lstrip().startswith(b"<?xml"):
        from app.apps.erp.core.documentos.nfe import ErroNFe, parsear_nfe
        try:
            d = parsear_nfe(conteudo)
            return {
                "origem_leitura": "XML_NFE",
                "confianca": "ALTA",
                "tipo_documento": "NFE",
                "chave_acesso": d.chave,
                "emitente_nome": d.emitente_nome,
                "emitente_documento": d.emitente_cnpj,
                "destinatario_documento": d.destinatario_doc or "",
                "numero_documento": d.numero,
                "data_emissao": d.emissao.isoformat() if d.emissao else "",
                "valor_total": str(d.valor_total),
                "descricao": d.natureza_operacao or "",
                "parcelas": [{"vencimento": p.vencimento.isoformat() if p.vencimento else "",
                              "valor": str(p.valor), "linha_digitavel": ""}
                             for p in d.duplicatas],
                "retencoes": [],
                "dedutibilidade_sugerida": "PENDENTE",
                "dedutibilidade_motivo": "Confirmar se o material foi aplicado em obra da empresa.",
                "observacoes": "",
            }
        except ErroNFe as e:
            raise ErroLeitura(str(e))

    # ---- PDF: texto e, se for escaneado, leitura visual
    if ext == ".pdf" or conteudo[:5] == b"%PDF-":
        texto = _texto_do_pdf(conteudo)
        if len(texto) >= 80:
            d = _ler_com_ia(texto=texto)
            d["origem_leitura"] = "PDF_TEXTO_IA"
            return d
        d = _ler_com_ia(imagem_b64=base64.b64encode(conteudo).decode(),
                        media_type="application/pdf")
        d["origem_leitura"] = "PDF_IMAGEM_IA"
        return d

    # ---- imagem
    if ext in (".png", ".jpg", ".jpeg", ".webp"):
        media = {"png": "image/png", "webp": "image/webp"}.get(ext.strip("."), "image/jpeg")
        d = _ler_com_ia(imagem_b64=base64.b64encode(conteudo).decode(), media_type=media)
        d["origem_leitura"] = "IMAGEM_IA"
        return d

    raise ErroLeitura(f"Formato não suportado: {ext or 'desconhecido'}. "
                      f"Envie XML da NFe, PDF ou imagem.")
