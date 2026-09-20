# -*- coding: utf-8 -*-
"""
pix_brcode.py — gera payload Pix (BR Code / EMV do BACEN) a partir de VALOR + CHAVE.
Sem dependência de plataformas externas.
"""

import re
import unicodedata

try:
    import qrcode
except ImportError:
    qrcode = None


def _sem_acentos(texto: str) -> str:
    nf = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nf if not unicodedata.combining(c))


def _tlv(idv: str, valor: str) -> str:
    return f"{idv}{len(valor):02d}{valor}"


def _cpf_valido(dig: str) -> bool:
    """Valida os dígitos verificadores de um CPF (11 dígitos)."""
    if len(dig) != 11 or not dig.isdigit() or dig == dig[0] * 11:
        return False
    for n in (9, 10):
        soma = sum(int(dig[i]) * ((n + 1) - i) for i in range(n))
        dv = (soma * 10) % 11 % 10
        if dv != int(dig[n]):
            return False
    return True


# ---------------------------------------------------------------------------
# O RÓTULO COLADO NA CHAVE — 20/09/2026
#
# ⚠️ ESTE É O DEFEITO QUE GEROU QR QUEBRADO, e a nota fica para ninguém
# reintroduzir o caminho curto. A coluna de informação de pagamento vem
# digitada por gente, e quase sempre vem assim:
#
#     "Chave Pix: carlosgaldino884@gmail.com"
#
# O rótulo ENTRAVA no payload ("0137Chave Pix: carlos...@gmail.com"), e o
# banco não resolve uma chave dessas: o QR abria, o aplicativo lia, e
# recusava. Não era feio — era código de pagamento inútil.
#
# A limpeza mora AQUI, e não em quem chama, porque `normalizar_chave` é o
# funil por onde todo payload passa. Ficando no chamador, a próxima tela que
# montar um Pix nasceria com o mesmo defeito, e ninguém veria até alguém
# tentar pagar.
# ---------------------------------------------------------------------------

# "Chave Pix:", "chave pix -", "CHAVE PIX", "Chave:", "Pix:" ... Só o "pix"
# sozinho exige o separador: sem ele, uma chave que por acaso comece com
# essas três letras seria mutilada.
_ROTULO_RE = re.compile(
    r"^\s*(?:chave\s*(?:de\s*)?(?:pix)?\s*[:\-\u2013\u2014=]*"
    r"|pix\s*[:\-\u2013\u2014=]+)\s*",
    re.IGNORECASE)


def eh_copia_cola(chave: str) -> bool:
    """O texto já É um payload pronto (o "copia e cola"), não uma chave."""
    c = re.sub(r"\s", "", chave or "")
    return c.startswith("000201") or "br.gov.bcb.pix" in c.lower()


def limpar_rotulo(chave: str) -> str:
    """Tira o rótulo que veio grudado na chave. Devolve "" se não sobrar nada."""
    s = (chave or "").strip()
    s = _ROTULO_RE.sub("", s, count=1).strip()
    # Payload pronto para aqui: ele tem espaços legítimos (nome e cidade do
    # recebedor) e um deles pode conter "@". Mexer abaixo o destruiria.
    if eh_copia_cola(s):
        return s
    # E-mail NUNCA tem espaço. Se sobrou texto em volta ("carlos@x.com
    # (Nubank)", "Favor pagar carlos@x.com"), fica só o endereço.
    if "@" in s and re.search(r"\s", s):
        for pedaco in s.split():
            if "@" in pedaco:
                return pedaco.strip(" ,;.")
    return s


def chave_de_dentro(payload: str) -> str | None:
    """A chave que está DENTRO de um "copia e cola" pronto (campo 01 do 26).

    Serve para uma coisa só: descobrir se um payload que chegou pronto nasceu
    com o rótulo grudado na chave — o que o torna um código de pagamento
    inútil, por mais que o CRC dele feche. Não entendendo o payload, devolve
    None, e quem chamou deixa o texto como está.
    """
    s = payload or ""
    m = re.search(r"26(\d{2})", s)
    if not m:
        return None
    conta = s[m.end():m.end() + int(m.group(1))]
    if "br.gov.bcb.pix" not in conta.lower():
        return None
    m2 = re.search(r"01(\d{2})", conta)
    if not m2:
        return None
    return conta[m2.end():m2.end() + int(m2.group(1))] or None


def normalizar_chave(chave: str, tipo: str | None = None) -> str:
    bruta = limpar_rotulo(chave)
    t = (tipo or "").lower()
    if t == "telefone":
        dig = re.sub(r"\D", "", bruta)
        if len(dig) in (10, 11):
            dig = "55" + dig
        return "+" + dig
    if t in ("cpf", "cnpj"):
        return re.sub(r"\D", "", bruta)
    if t == "email":
        return bruta
    if t == "aleatoria":
        return bruta.lower()
    if bruta.startswith("+"):
        return "+" + re.sub(r"\D", "", bruta)
    if "@" in bruta:
        return bruta
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", bruta):
        return bruta.lower()
    dig = re.sub(r"\D", "", bruta)
    # --- Desempate por FORMATAÇÃO original (antes de olhar só os dígitos) ---
    # "(81) 98391-5233" / "81 98391-5233" -> telefone;  "068.663.434-96" -> CPF.
    if re.search(r"\(\s*\d{2}\s*\)", bruta):                      # tem (DDD)
        if len(dig) in (10, 11):
            return "+55" + dig
        if len(dig) in (12, 13) and dig.startswith("55"):
            return "+" + dig
    if re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}", bruta):         # CPF formatado
        return dig
    if re.fullmatch(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", bruta):   # CNPJ formatado
        return dig
    # celular já com país (5581983915233) sem o '+'
    if len(dig) in (12, 13) and dig.startswith("55"):
        return "+" + dig
    if len(dig) == 11:
        # 11 dígitos "pelados": só é CPF se os dígitos verificadores fecharem;
        # senão, tratamos como celular (DDD + 9XXXXXXXX) em E.164.
        return dig if _cpf_valido(dig) else "+55" + dig
    if len(dig) == 10:                                            # fixo com DDD
        return "+55" + dig
    if len(dig) == 14:                                            # CNPJ
        return dig
    return bruta


def formatar_valor(valor) -> str:
    if isinstance(valor, (int, float)):
        return f"{float(valor):.2f}"
    v = str(valor).strip().replace("R$", "").strip()
    v = v.replace(".", "").replace(",", ".")
    return f"{float(v):.2f}"


def crc16(payload: str) -> str:
    crc = 0xFFFF
    for byte in payload.encode("utf-8"):
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if (crc & 0x8000) else (crc << 1)
            crc &= 0xFFFF
    return f"{crc:04X}"


def montar_payload(chave: str, valor: str, nome: str = "RECEBEDOR",
                   cidade: str = "FORTALEZA", txid: str = "***") -> str:
    nome_fmt = _sem_acentos(nome).upper()[:25]
    cidade_fmt = _sem_acentos(cidade).upper()[:15]
    conta = _tlv("00", "br.gov.bcb.pix") + _tlv("01", chave)
    payload = (
        _tlv("00", "01") + _tlv("26", conta) + _tlv("52", "0000")
        + _tlv("53", "986") + _tlv("54", valor) + _tlv("58", "BR")
        + _tlv("59", nome_fmt) + _tlv("60", cidade_fmt)
        + _tlv("62", _tlv("05", txid)) + "6304")
    return payload + crc16(payload)


def validar_payload(payload: str) -> dict:
    corpo, crc_informado = payload[:-4], payload[-4:]
    return {
        "crc_ok": crc16(corpo) == crc_informado.upper(),
        "comeca_correto": payload.startswith("000201"),
        "tem_moeda_986": "5303986" in payload,
        "tem_pais_br": "5802BR" in payload,
    }


def gerar_pix(chave: str, valor, nome: str = "RECEBEDOR", cidade: str = "FORTALEZA",
              txid: str = "***", tipo_chave: str | None = None) -> dict:
    chave_norm = normalizar_chave(chave, tipo_chave)
    valor_fmt = formatar_valor(valor)
    payload = montar_payload(chave_norm, valor_fmt, nome, cidade, txid)
    val = validar_payload(payload)
    return {"payload": payload, "chave": chave_norm, "valor": valor_fmt,
            "valido": val["crc_ok"]}


def qr_png_bytes(payload: str, box_size: int = 8, border: int = 2) -> bytes:
    """Gera a imagem PNG (bytes) do QR a partir de um payload (copia-e-cola)."""
    import io
    if qrcode is None:
        raise RuntimeError('Instale: pip install "qrcode[pil]"')
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M,
                       box_size=box_size, border=border)
    qr.add_data(payload)
    qr.make(fit=True)
    buf = io.BytesIO()
    qr.make_image(fill_color="black", back_color="white").save(buf, format="PNG")
    return buf.getvalue()