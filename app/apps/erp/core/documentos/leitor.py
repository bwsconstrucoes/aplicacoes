# ============================================================================
# ERP — core/documentos/leitor.py
# Leitura de QUALQUER documento que chega ao financeiro, incluindo o caso mais
# comum e mais difícil: foto de nota tirada com o celular, torta e amassada.
#
# Cobertura de tipos: NFe (XML ou DANFE), NFS-e de qualquer município, CT-e,
# recibo, RPA, guia (DARF/GPS/FGTS/DAS/DAM/ISS), fatura de concessionária,
# boleto, contrato, termo de rescisão, comprovante bancário, prestação de
# contas de fundo fixo (vários comprovantes numa folha só) e nota de
# devolução. O que não se encaixar volta como OUTRO, com o que foi possível
# extrair — nunca em branco.
#
# Cascata de leitura, do mais confiável ao mais tolerante:
#   1. XML de NFe                → parser determinístico, exato, sem IA
#   2. PDF com camada de texto   → texto (pdfplumber) + IA
#   3. PDF escaneado / foto      → PÁGINAS RASTERIZADAS (PyMuPDF) + IA visual
#      Foto passa por endireitamento de tamanho e recompressão antes de subir.
#   4. Sempre que houver texto E imagem, os dois vão juntos: a IA confere um
#      contra o outro, o que derruba muito erro de OCR.
#
# Todo campo volta com a marca de onde saiu e um nível de confiança. Documento
# com múltiplos itens (prestação de contas) volta com a lista de itens, cada um
# virando uma linha para o financeiro classificar.
# ============================================================================
from __future__ import annotations

import time as _time

import base64
import gc
import io
import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

MODELO_TEXTO = os.getenv("ERP_MODELO_IA", "gpt-4o-mini")
MODELO_VISAO = os.getenv("ERP_MODELO_IA_VISAO", "gpt-4o")
MAX_ARQUIVO_BYTES = 20 * 1024 * 1024
MAX_PAGINAS = 6
MAX_TEXTO = 16000
_DPI_RASTER = 190          # legível para nota fiscal sem estourar o payload
_LADO_MAX = 1800           # px no maior lado da imagem enviada


class ErroLeitura(Exception):
    """Documento ilegível, formato não suportado ou serviço indisponível."""


TIPOS = ("NFE", "NFSE", "CTE", "NFCE", "RECIBO", "RPA", "GUIA", "BOLETO",
         "FATURA_CONCESSIONARIA", "CONTRATO", "TERMO_RESCISAO", "COMPROVANTE",
         "PRESTACAO_CONTAS", "NOTA_DEVOLUCAO", "ORCAMENTO", "OUTRO")

_INSTRUCAO = f"""Você lê documentos financeiros brasileiros para o ERP de uma construtora (BWS Construções).
Os documentos chegam como foto de celular, digitalização torta, PDF gerado por sistema ou impressão.
Leia com paciência: o que estiver ilegível você declara em "observacoes", nunca inventa.

Responda SOMENTE com JSON válido, sem markdown e sem comentários:

{{
 "tipo_documento": "um de: {' | '.join(TIPOS)}",
 "emitente_nome": "razão social/nome de QUEM EMITIU o documento (o credor)",
 "emitente_documento": "CNPJ/CPF do emitente, só dígitos",
 "destinatario_nome": "para quem foi emitido",
 "destinatario_documento": "CNPJ/CPF do destinatário, só dígitos",
 "numero_documento": "número da nota, recibo, guia ou fatura",
 "serie": "série, quando houver",
 "chave_acesso": "44 dígitos, se for NFe/NFCe/CTe e aparecer no documento",
 "codigo_verificacao": "código de verificação da NFSe, quando houver",
 "municipio_emissao": "município da prestação/emissão",
 "data_emissao": "AAAA-MM-DD",
 "competencia": "AAAA-MM quando o documento indicar mês de referência",
 "valor_total": "0000.00",
 "valor_liquido": "0000.00 quando diferente do total (após retenções/descontos)",
 "descricao": "o que foi comprado/contratado, em uma linha",
 "obra_mencionada": "obra/centro de custo citado (código ou nome), se houver",
 "placa_veiculo": "placa, se for despesa de veículo",
 "competencia_servico": "período do serviço, se citado",
 "parcelas": [{{"vencimento":"AAAA-MM-DD","valor":"0000.00","linha_digitavel":"só dígitos ou vazio"}}],
 "retencoes": [{{"tipo":"INSS|ISS|IRRF|PCC|FGTS","base_calculo":"0000.00","aliquota":"0.00","valor":"0000.00"}}],
 "itens": [{{"descricao":"item","valor":"0000.00","documento":"nº do cupom/nota do item"}}],
 "dados_bancarios_no_documento": "chave Pix ou conta que aparece no documento (só para conferência)",
 "dedutibilidade_sugerida": "DEDUTIVEL|INDEDUTIVEL|PARCIAL|PENDENTE",
 "dedutibilidade_motivo": "uma frase",
 "confianca": "ALTA|MEDIA|BAIXA",
 "campos_ilegiveis": ["nome dos campos que você não conseguiu ler"],
 "observacoes": "ambiguidades, rasuras, o que precisa de conferência humana"
}}

Regras:
- Campo ausente = string vazia. Lista ausente = [].
- Valores com ponto decimal, sem "R$" e sem separador de milhar (1.234,56 → 1234.56).
- Datas sempre AAAA-MM-DD. Se vier 15/03/26, entenda 2026-03-15.
- O EMITENTE é o credor (quem recebe). A BWS Construções costuma ser a destinatária;
  se a BWS aparecer como emitente, é documento de RECEITA — diga isso em observacoes.
- GUIA (DARF/GPS/FGTS/DAS/DAM): o "emitente" é o órgão; traga o código de barras em
  parcelas[].linha_digitavel e o período de apuração em competencia.
- PRESTACAO_CONTAS (folha com vários comprovantes, típica de fundo fixo): preencha
  "itens" com cada comprovante e some tudo em valor_total.
- RPA: traga as retenções de INSS e IRRF separadamente.
- TERMO_RESCISAO: valor_total é o líquido a pagar ao empregado.
- COMPROVANTE bancário: emitente = favorecido do pagamento; traga data e valor pagos.
- Multa de trânsito, multa punitiva, brinde sem vínculo: INDEDUTIVEL.
- Material/serviço aplicado à obra, com nota: DEDUTIVEL.
- Sem base para decidir: PENDENTE.
- Se a imagem estiver ruim demais para um campo, coloque-o em campos_ilegiveis e siga."""


# ---------------------------------------------------------------------------
# Infraestrutura
# ---------------------------------------------------------------------------
def _cliente():
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        raise ErroLeitura(
            "OPENAI_API_KEY não configurada — leitura automática indisponível. "
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
            raise ErroLeitura("A leitura não devolveu dados estruturados. Tente outra foto.")
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            raise ErroLeitura("A leitura devolveu dados incompletos. Tente outra foto.")


def _texto_do_pdf(conteudo: bytes) -> str:
    try:
        import pdfplumber
    except ImportError:
        return ""
    partes: list[str] = []
    pdf = None
    try:
        pdf = pdfplumber.open(io.BytesIO(conteudo))
        for pagina in pdf.pages[:MAX_PAGINAS]:
            partes.append(pagina.extract_text() or "")
            pagina.flush_cache()
    except Exception as e:
        logger.warning("ERP/leitor: PDF sem texto extraível (%s)", e)
        return ""
    finally:
        if pdf is not None:
            try:
                pdf.close()
            except Exception:
                pass
        gc.collect()
    return "\n".join(p for p in partes if p).strip()


def _paginas_como_png(conteudo: bytes) -> list[str]:
    """Rasteriza o PDF em PNG (base64). É o que permite ler DANFE escaneado,
    NFS-e em imagem e comprovante fotografado — a OpenAI não lê PDF cru."""
    try:
        import fitz  # PyMuPDF, já usado pelo pdf_processor
    except ImportError:
        raise ErroLeitura("PyMuPDF indisponível — não é possível ler PDF digitalizado.")
    imagens: list[str] = []
    doc = None
    try:
        doc = fitz.open(stream=conteudo, filetype="pdf")
        escala = _DPI_RASTER / 72.0
        for pagina in doc[:MAX_PAGINAS]:
            pix = pagina.get_pixmap(matrix=fitz.Matrix(escala, escala))
            imagens.append(base64.b64encode(pix.tobytes("png")).decode())
            pix = None
    except Exception as e:
        raise ErroLeitura(f"Não foi possível abrir o PDF: {e}")
    finally:
        if doc is not None:
            doc.close()
        gc.collect()
    return imagens


def _preparar_imagem(conteudo: bytes) -> tuple[str, str]:
    """Reduz e recomprime a foto antes de enviar. Foto de celular moderna tem
    12 MP e não cabe bem no payload; 1800 px no maior lado basta para OCR."""
    try:
        from PIL import Image
    except ImportError:
        return base64.b64encode(conteudo).decode(), "image/jpeg"
    try:
        img = Image.open(io.BytesIO(conteudo))
        img = img.convert("RGB")
        maior = max(img.size)
        if maior > _LADO_MAX:
            fator = _LADO_MAX / maior
            img = img.resize((int(img.width * fator), int(img.height * fator)),
                             Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=88, optimize=True)
        dados = buf.getvalue()
        img.close()
        gc.collect()
        return base64.b64encode(dados).decode(), "image/jpeg"
    except Exception as e:
        logger.warning("ERP/leitor: falha ao preparar imagem (%s); enviando original", e)
        return base64.b64encode(conteudo).decode(), "image/jpeg"


def _chamar_ia(*, texto: str = "", imagens: Optional[list[tuple[str, str]]] = None,
               dica: str = "") -> dict[str, Any]:
    cliente = _cliente()
    imagens = imagens or []
    partes: list[dict[str, Any]] = []
    if dica:
        partes.append({"type": "text", "text": dica})
    if texto:
        partes.append({"type": "text",
                       "text": f"Texto extraído do documento:\n\n{texto[:MAX_TEXTO]}"})
    for b64, media in imagens:
        partes.append({"type": "image_url",
                       "image_url": {"url": f"data:{media};base64,{b64}", "detail": "high"}})
    if not partes:
        raise ErroLeitura("Nada a ler no arquivo.")
    modelo = MODELO_VISAO if imagens else MODELO_TEXTO
    # Este é o ÚNICO ponto por onde toda leitura passa — por isso o consumo é
    # registrado aqui, e não em cada tela. A operação (fatura, contrato,
    # comprovante…) vem do contexto que quem chamou declarou.
    inicio = _time.perf_counter()
    try:
        resp = cliente.chat.completions.create(
            model=modelo, temperature=0, max_tokens=2000,
            messages=[{"role": "system", "content": _INSTRUCAO},
                      {"role": "user", "content": partes}])
    except Exception as e:
        logger.exception("ERP/leitor: falha na IA (%s)", modelo)
        # chamada que falhou também conta: a OpenAI pode ter cobrado, e o
        # painel precisa mostrar que a leitura está quebrando
        _registrar_consumo(None, modelo, None, _ms(inicio), sucesso=False, erro=str(e))
        raise ErroLeitura(f"Serviço de leitura indisponível: {e}")
    _registrar_consumo(resp, modelo, None, _ms(inicio))
    d = _extrair_json(resp.choices[0].message.content or "")
    d["modelo"] = modelo
    return d


def _ms(inicio: float) -> int:
    return int((_time.perf_counter() - inicio) * 1000)


def _registrar_consumo(resposta: Any, modelo: str, operacao: Optional[str],
                       duracao_ms: Optional[int], *, sucesso: bool = True,
                       erro: str = "") -> None:
    """Registra o consumo de uma chamada de IA em sessão própria.

    `operacao` None usa a declarada no contexto (ver ia_custo.contexto). Nunca
    levanta exceção: o registro é secundário à leitura.
    """
    from app.apps.erp.core.comum.ia_custo import registrar_autonomo
    try:
        registrar_autonomo(modelo=modelo, resposta=resposta, duracao_ms=duracao_ms,
                           operacao=operacao, sucesso=sucesso, erro=erro)
    except Exception as e:                   # pragma: no cover - defesa extra
        logger.warning("ERP/leitor: consumo não registrado (%s)", e)


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------
def _norm_valor(v: Any) -> str:
    s = str(v or "").strip().replace("R$", "").replace(" ", "")
    if not s:
        return ""
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return m.group(0) if m else ""


def _norm_data(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)
    m = re.match(r"(\d{2})/(\d{2})/(\d{2,4})", s)
    if m:
        d, mes, a = m.groups()
        a = a if len(a) == 4 else ("20" + a)
        return f"{a}-{mes}-{d}"
    return ""


def _normalizar(d: dict[str, Any]) -> dict[str, Any]:
    d["emitente_documento"] = re.sub(r"\D", "", str(d.get("emitente_documento") or ""))
    d["destinatario_documento"] = re.sub(r"\D", "", str(d.get("destinatario_documento") or ""))
    d["emitente_nome"] = (d.get("emitente_nome") or "").strip().upper()
    d["valor_total"] = _norm_valor(d.get("valor_total"))
    d["valor_liquido"] = _norm_valor(d.get("valor_liquido"))
    d["data_emissao"] = _norm_data(d.get("data_emissao"))
    d["chave_acesso"] = re.sub(r"\D", "", str(d.get("chave_acesso") or ""))[:44]
    tipo = (d.get("tipo_documento") or "OUTRO").strip().upper()
    d["tipo_documento"] = tipo if tipo in TIPOS else "OUTRO"

    parcelas = []
    for p in (d.get("parcelas") or []):
        venc, val = _norm_data(p.get("vencimento")), _norm_valor(p.get("valor"))
        if venc or val:
            parcelas.append({"vencimento": venc, "valor": val,
                             "linha_digitavel": re.sub(r"\D", "", str(p.get("linha_digitavel") or ""))})
    d["parcelas"] = parcelas

    ret = []
    for r in (d.get("retencoes") or []):
        val = _norm_valor(r.get("valor"))
        if val and float(val or 0) > 0:
            ret.append({"tipo": (r.get("tipo") or "").strip().upper()[:5],
                        "base_calculo": _norm_valor(r.get("base_calculo")) or d["valor_total"],
                        "aliquota": _norm_valor(r.get("aliquota")), "valor": val})
    d["retencoes"] = ret

    itens = []
    for i in (d.get("itens") or []):
        val = _norm_valor(i.get("valor"))
        if val:
            itens.append({"descricao": (i.get("descricao") or "").strip(),
                          "valor": val, "documento": (i.get("documento") or "").strip()})
    d["itens"] = itens

    # coerência: total ausente mas parcelas presentes
    if not d["valor_total"] and parcelas:
        soma = sum(float(p["valor"]) for p in parcelas if p["valor"])
        if soma:
            d["valor_total"] = f"{soma:.2f}"
    if not d["valor_total"] and itens:
        d["valor_total"] = f"{sum(float(i['valor']) for i in itens):.2f}"

    conf = (d.get("confianca") or "").upper()
    d["confianca"] = conf if conf in ("ALTA", "MEDIA", "BAIXA") else "MEDIA"
    if d.get("campos_ilegiveis"):
        d["confianca"] = "BAIXA" if len(d["campos_ilegiveis"]) > 2 else d["confianca"]
    return d


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------
def ler_documento(conteudo: bytes, nome_arquivo: str,
                  dica_usuario: str = "") -> dict[str, Any]:
    """Lê o documento e devolve os campos sugeridos. `dica_usuario` permite
    orientar a leitura ('é uma guia de INSS da obra X')."""
    if not conteudo:
        raise ErroLeitura("Arquivo vazio.")
    if len(conteudo) > MAX_ARQUIVO_BYTES:
        raise ErroLeitura(f"Arquivo acima de {MAX_ARQUIVO_BYTES // (1024*1024)} MB. "
                          f"Envie uma foto menor ou o PDF.")
    ext = os.path.splitext(nome_arquivo or "")[1].lower()
    cabeca = conteudo[:512].lstrip()
    dica = f"Contexto informado pelo usuário: {dica_usuario}" if dica_usuario.strip() else ""

    # ---------- 1. XML de NFe: exato, sem IA
    if ext == ".xml" or cabeca.startswith(b"<?xml") or b"<nfeProc" in cabeca:
        from app.apps.erp.core.documentos.nfe import ErroNFe, parsear_nfe
        try:
            n = parsear_nfe(conteudo)
            return _normalizar({
                "origem_leitura": "XML_NFE", "confianca": "ALTA", "modelo": "-",
                "tipo_documento": "NFE", "chave_acesso": n.chave,
                "emitente_nome": n.emitente_nome, "emitente_documento": n.emitente_cnpj,
                "destinatario_documento": n.destinatario_doc or "",
                "numero_documento": n.numero, "serie": n.serie,
                "data_emissao": n.emissao.isoformat() if n.emissao else "",
                "valor_total": str(n.valor_total), "descricao": n.natureza_operacao or "",
                "parcelas": [{"vencimento": p.vencimento.isoformat() if p.vencimento else "",
                              "valor": str(p.valor), "linha_digitavel": ""} for p in n.duplicatas],
                "retencoes": [], "itens": [],
                "dedutibilidade_sugerida": "PENDENTE",
                "dedutibilidade_motivo": "Confirmar aplicação em obra da empresa.",
                "campos_ilegiveis": [], "observacoes": "",
            })
        except ErroNFe as e:
            raise ErroLeitura(f"{e} (se for o XML de outro tipo de documento, "
                              f"envie o PDF ou a foto)")

    # ---------- 2/3. PDF: texto e/ou páginas rasterizadas
    if ext == ".pdf" or conteudo[:5] == b"%PDF-":
        texto = _texto_do_pdf(conteudo)
        tem_texto = len(texto) >= 120
        try:
            imagens_b64 = _paginas_como_png(conteudo)
        except ErroLeitura:
            imagens_b64 = []
        if tem_texto and imagens_b64:
            # texto + imagem juntos: a IA confere um contra o outro
            d = _chamar_ia(texto=texto,
                           imagens=[(b, "image/png") for b in imagens_b64[:3]], dica=dica)
            d["origem_leitura"] = "PDF_TEXTO_E_IMAGEM"
        elif tem_texto:
            d = _chamar_ia(texto=texto, dica=dica)
            d["origem_leitura"] = "PDF_TEXTO"
        elif imagens_b64:
            d = _chamar_ia(imagens=[(b, "image/png") for b in imagens_b64], dica=dica)
            d["origem_leitura"] = "PDF_DIGITALIZADO"
        else:
            raise ErroLeitura("PDF sem texto e sem páginas legíveis.")
        return _normalizar(d)

    # ---------- 4. Foto / imagem
    if ext in (".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif", ".bmp", ".tif", ".tiff") \
            or cabeca[:3] == b"\xff\xd8\xff" or cabeca[:8] == b"\x89PNG\r\n\x1a\n":
        b64, media = _preparar_imagem(conteudo)
        d = _chamar_ia(imagens=[(b64, media)], dica=dica)
        d["origem_leitura"] = "FOTO"
        return _normalizar(d)

    raise ErroLeitura(f"Formato não suportado: {ext or 'desconhecido'}. "
                      f"Envie PDF, foto (JPG/PNG/HEIC) ou o XML da NFe.")
