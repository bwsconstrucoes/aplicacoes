"""Foto de iPhone (HEIC) vira JPG na chegada.

⚑ 25/11/2026 — pedido do dono: *"foto de iPhone no portal, sim, tem que
aceitar também. É comum o pessoal ter iPhone."*

O iPhone fotografa em HEIC. O navegador do próprio iPhone costuma converter
para JPG ao anexar pela câmera ou pela galeria, mas quem escolhe o arquivo
pelo app "Arquivos" (ou recebe a foto de outra pessoa) manda o HEIC cru. Sem
conversão, duas coisas falhavam: a leitura por IA (que não lê HEIC) e quem
abrisse o anexo depois no computador (o Chrome e o Windows não mostram HEIC).

Por isso a conversão é feita UMA VEZ, na chegada: o que se guarda e o que se lê
é JPG. Usa a biblioteca `pillow-heif` (dependência acrescentada com o ok do
dono nesta data). Sem ela instalada, ou com o arquivo corrompido, o arquivo
segue como veio — guardar o papel do jeito que chegou é melhor que perdê-lo.
"""
from __future__ import annotations

import io
import logging

logger = logging.getLogger(__name__)

# As "marcas" que o formato HEIF/HEIC grava nos bytes 8 a 12 do arquivo.
_MARCAS_HEIF = (b"heic", b"heix", b"hevc", b"hevx", b"heim", b"heis",
                b"mif1", b"msf1")
_REGISTRADO = False


def registrar_heic() -> bool:
    """Ensina o Pillow a abrir HEIC. Pode ser chamada quantas vezes quiser."""
    global _REGISTRADO
    if _REGISTRADO:
        return True
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
        _REGISTRADO = True
    except Exception:                                   # noqa: BLE001
        logger.warning("Fotos: pillow-heif indisponível; HEIC não será convertido")
    return _REGISTRADO


def eh_heic(conteudo: bytes, nome: str = "") -> bool:
    """Pelo conteúdo (o nome do arquivo pode mentir), ou pela extensão."""
    if conteudo[4:8] == b"ftyp" and conteudo[8:12] in _MARCAS_HEIF:
        return True
    return (nome or "").lower().endswith((".heic", ".heif"))


def heic_para_jpeg(conteudo: bytes, nome: str) -> tuple[bytes, str]:
    """(conteúdo, nome) prontos para guardar. Não sendo HEIC, devolve igual."""
    if not conteudo or not eh_heic(conteudo, nome) or not registrar_heic():
        return conteudo, nome
    try:
        from PIL import Image, ImageOps
        with Image.open(io.BytesIO(conteudo)) as img:
            img = ImageOps.exif_transpose(img).convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=88, optimize=True)
        base = (nome or "foto").rsplit(".", 1)[0] if "." in (nome or "") else (nome or "foto")
        return buf.getvalue(), f"{base}.jpg"
    except Exception:                                   # noqa: BLE001
        logger.warning("Fotos: não converti o HEIC %r; guardo como veio", nome,
                       exc_info=True)
        return conteudo, nome
