# -*- coding: utf-8 -*-
"""
pagamentos.py — interpreta a forma de pagamento (col J) e a info de pagamento
(col Y), e gera as imagens para leitura em tela:

  - Pix (forma Pix/BeeVale): QR a partir da chave (monta payload) ou direto do
    "copia e cola" quando a Y já vem nesse formato.
  - Boleto (forma Boleto): código de barras (Interleaved 2of5) a partir da col AI.

Também concentra a regra do ALERTA LARANJA (cadastro que impede pagar).
"""

import io
import re
import pix_brcode

_CHAVE_RE = re.compile(r"chave\s*pix\s*:?\s*", re.IGNORECASE)


def extrair_chave(y: str):
    """Texto após 'Chave Pix:'. Retorna None se não houver chave de fato."""
    if not y:
        return None
    s = str(y).strip()
    s = _CHAVE_RE.sub("", s, count=1).strip()
    return s or None


def eh_copia_cola(chave: str) -> bool:
    c = re.sub(r"\s", "", chave or "")
    return c.startswith("000201") or "br.gov.bcb.pix" in c.lower()


def classificar(forma: str, y: str) -> dict:
    """{tipo: pix|boleto|outro, subtipo: chave|copia_cola|None, chave, falta_chave}"""
    f = (forma or "").strip().lower()
    pix_like = ("pix" in f) or ("beevale" in f)
    chave = extrair_chave(y)
    if pix_like:
        if not chave:
            return {"tipo": "pix", "subtipo": None, "chave": None, "falta_chave": True}
        sub = "copia_cola" if eh_copia_cola(chave) else "chave"
        return {"tipo": "pix", "subtipo": sub, "chave": chave, "falta_chave": False}
    if "boleto" in f:
        return {"tipo": "boleto", "subtipo": None, "chave": None, "falta_chave": False}
    return {"tipo": "outro", "subtipo": None, "chave": None, "falta_chave": False}


# ---------------------------------------------------------------------------
# PIX
# ---------------------------------------------------------------------------
def _limpar_copia_cola(chave: str) -> str:
    """Prepara o payload 'copia e cola' SEM destruir os espaços internos legítimos
    (nome/cidade contam no tamanho declarado do TLV e no CRC — removê-los quebra
    a leitura do QR). Estratégia:
      1) corta qualquer texto antes do '000201';
      2) remove SÓ quebras de linha/tabs (colagem quebrada em linhas);
      3) valida o CRC: se não fechar, tenta a variante sem espaço algum (payloads
         que ganharam espaços por quebra de célula) e usa a que validar."""
    s = str(chave or "").strip()
    i = s.find("000201")
    if i > 0:
        s = s[i:]
    cand1 = re.sub(r"[\r\n\t]+", "", s).strip()

    def _crc_ok(p: str) -> bool:
        return (len(p) > 8 and "6304" in p[-8:]
                and pix_brcode.crc16(p[:-4]) == p[-4:].upper())

    if _crc_ok(cand1):
        return cand1
    cand2 = re.sub(r"\s+", "", s)
    if _crc_ok(cand2):
        return cand2
    return cand1        # nenhum validou: preserva os espaços (menos destrutivo)


def gerar_pix(chave: str, valor, nome: str, copia_cola: bool = False):
    """Retorna (png_bytes, payload). Levanta exceção se não der pra montar."""
    if copia_cola:
        payload = _limpar_copia_cola(chave)
    else:
        payload = pix_brcode.montar_payload(
            pix_brcode.normalizar_chave(chave),
            pix_brcode.formatar_valor(valor),
            nome=nome or "RECEBEDOR")
    return pix_brcode.qr_png_bytes(payload), payload


# ---------------------------------------------------------------------------
# BOLETO (código de barras)
# ---------------------------------------------------------------------------
def linha_para_barcode(linha47: str):
    """Converte linha digitável de boleto bancário (47 díg.) no código de barras (44)."""
    d = re.sub(r"\D", "", linha47 or "")
    if len(d) != 47:
        return None
    c1, c2, c3 = d[0:9], d[10:20], d[21:31]
    dv, c5 = d[32], d[33:47]
    return d[0:4] + dv + c5 + c1[4:9] + c2 + c3  # 44 dígitos


def codigo_boleto(ai: str):
    """
    Normaliza a coluna AI -> (codigo_digitos, status).
    status: 'ok' | 'invalido' | 'fora_padrao'
    """
    raw = str(ai or "").strip()
    if not raw:
        return None, "fora_padrao"
    if "INVALIDO" in raw.upper():
        return None, "invalido"
    d = re.sub(r"\D", "", raw)
    if len(d) == 47:                      # linha digitável bancária -> barcode 44
        conv = linha_para_barcode(d)
        if conv:
            d = conv
    if len(d) in (44, 48):                # ITF exige nº par de dígitos (44/48 ok)
        return d, "ok"
    return d, "fora_padrao"


def barcode_png_bytes(ai: str):
    """Retorna (png_bytes|None, status). Mantido por compatibilidade."""
    d, status = codigo_boleto(ai)
    if status != "ok":
        return None, status
    import barcode
    from barcode.writer import ImageWriter
    itf = barcode.get("itf", d, writer=ImageWriter())
    buf = io.BytesIO()
    # 1 pixel inteiro por módulo (dpi de tela) => barras nítidas quando exibidas
    # no tamanho nativo, sem o navegador "esticar" (que é o que borrava).
    itf.write(buf, options={"module_width": 0.2646, "module_height": 12.0, "dpi": 96,
                            "font_size": 9, "text_distance": 2.0, "quiet_zone": 3.0})
    return buf.getvalue(), "ok"


def barcode_svg(ai: str):
    """Retorna (svg_str|None, status). SVG vetorial: nítido em qualquer largura."""
    d, status = codigo_boleto(ai)
    if status != "ok":
        return None, status
    import barcode
    from barcode.writer import SVGWriter
    itf = barcode.get("itf", d, writer=SVGWriter())
    buf = io.BytesIO()
    itf.write(buf, options={"module_width": 0.3, "module_height": 20.0,
                            "font_size": 9, "text_distance": 3.0, "quiet_zone": 5.0})
    svg = buf.getvalue().decode("utf-8")
    # O SVGWriter emite as barras em milímetros (ex.: x="5mm") e o <svg> com
    # width/height em mm, SEM viewBox. Para a largura responsiva (width:100%) o
    # navegador converte "mm" em px (96/25.4 ≈ 3.78). Logo o viewBox precisa estar
    # NA MESMA escala (px), senão ele mostra só uma fatia e "corta" o código.
    mm2px = 96.0 / 25.4
    m = re.search(r'<svg([^>]*)>', svg)
    if m:
        wm = re.search(r'width="([\d.]+)mm"', m.group(1))
        hm = re.search(r'height="([\d.]+)mm"', m.group(1))
        if wm and hm:
            vbw = float(wm.group(1)) * mm2px
            vbh = float(hm.group(1)) * mm2px
            novo = (f'<svg version="1.1" xmlns="http://www.w3.org/2000/svg" '
                    f'viewBox="0 0 {vbw:.2f} {vbh:.2f}" width="100%" '
                    f'preserveAspectRatio="xMidYMid meet">')
            svg = svg[:m.start()] + novo + svg[m.end():]
    return svg, "ok"


# ---------------------------------------------------------------------------
# ALERTA LARANJA (cadastro que impede o pagamento)
# ---------------------------------------------------------------------------
def precisa_atualizar(forma: str, y: str, centro_custo: str) -> bool:
    info = classificar(forma, y)
    falta_chave = info["tipo"] == "pix" and info["falta_chave"]
    sem_cc = str(centro_custo or "").strip() == ""
    return bool(falta_chave or sem_cc)


def pendencias(forma, info_pgt, centro_custo, codigo_integracao, status_pgt) -> list:
    """
    Lista de pendências de cadastro de um lançamento (para o alerta laranja e
    o destaque no detalhe). Ordem fixa para exibição consistente.
      - 'Centro de Custo'  : coluna H em branco
      - 'Chave Pix'        : Pix/BeeVale com a coluna Y sem a chave
      - 'Integração Omie'  : coluna P sem código E título ainda ativo
                             (não Cancelado e não Pago)
    """
    out = []
    if str(centro_custo or "").strip() == "":
        out.append("Centro de Custo")
    info = classificar(forma, info_pgt)
    if info["tipo"] == "pix" and info["falta_chave"]:
        out.append("Chave Pix")
    status = str(status_pgt or "").strip().lower()
    ativo = status not in ("cancelado", "pago")
    if ativo and str(codigo_integracao or "").strip() == "":
        out.append("Integração Omie")
    return out
