# -*- coding: utf-8 -*-
"""
ERP — AS FOTOS DO DIÁRIO DE OBRA (14/11/2026).

Pedido dele: *"era muito interessante também a gente poder colocar serviços
executados e fotos, e integrar isso no diário impresso também. (…) A página,
sempre quando tiver foto, mostra na página seguinte. (…) é como se fosse um
anexo ao diário, essa página das fotos, para não interferir no layout que a
gente já trabalhou e já tem. (…) Lembrando que fotos sempre no Google Drive."*

⚑ POR QUE NÃO TEM TABELA NOVA, e isso é decisão de projeto, não preguiça:

O ERP já tem a máquina inteira de anexo (`core/documentos/armazenamento.py`):
comprime a imagem, manda para o **Google Drive**, cai para o banco quando o
Drive não responde, confere permissão e escopo na hora de entregar o arquivo,
guarda o hash para não duplicar e tem a rotina que leva para o Drive o que
ficou no banco. Criar `diario_fotos` com upload próprio seria ter DUAS
verdades sobre onde o arquivo da empresa mora — e a segunda nasceria sem
nenhuma dessas proteções.

Então a foto do diário é um **anexo** com `entidade_tipo = "diario_foto"` e
`entidade_id = diarios_obra.id`. A legenda é a `descricao` do anexo, que já
existe e já tem tela para corrigir.

AS REGRAS DAQUI, que o armazenamento genérico não conhece:

1. **TETO DE 6 FOTOS POR DIA**, decidido por ele. Não é capricho de tela: o
   relatório de três meses tem 90 folhas, e seis fotos por dia já são 540
   imagens num arquivo que alguém vai mandar por e-mail.
2. **SÓ IMAGEM.** PDF e planilha têm outro lugar (o Arquivo da obra). Aqui é
   o registro visual do serviço executado.
3. **O DIA PRECISA EXISTIR.** Foto é do diário, não da data: sem o dia
   gravado não há a que prender.
4. **DEPOIS DE FECHADO, NÃO SE APAGA FOTO.** Acrescentar pode — foto só
   soma prova. Tirar, não: o papel já saiu com ela, e sumir com evidência de
   um documento fechado é outra conversa.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao

logger = logging.getLogger(__name__)

TIPO = "diario_foto"

# Decidido por ele em 14/11/2026, quando perguntei: *"sim, podemos limitar a
# qtd a 6."*
TETO_POR_DIA = 6

TIPOS_DE_IMAGEM = ("image/jpeg", "image/png", "image/webp", "image/heic",
                   "image/heif")

# O maior lado da foto DENTRO DO PAPEL. O armazenamento já guarda a 1800px; no
# relatório de 90 dias, 540 imagens nesse tamanho fazem um PDF que não passa
# em e-mail nenhum. 900px é mais do que uma folha A4 imprime a 150 dpi.
LADO_NO_PAPEL = 900
QUALIDADE_NO_PAPEL = 72


# ===========================================================================
# ONDE E QUANDO A FOTO FOI TIRADA (24/11/2026)
#
# Ele: *"se eu precisar que as minhas fotos tenham geolocalização e que na
# legenda dessa foto apareça essa geolocalização e ainda que fique um carimbo
# da foto com a data e geolocalização que ela foi retirada."*
#
# ⚠️ O DEFEITO DE BASE, e ele vinha de 14/11: para a foto caber no PDF o
# sistema a reduzia e REGRAVAVA — e regravar apaga o EXIF, que é onde o
# celular grava a hora e o GPS. A informação chegava e era destruída no
# envio. Por isso a leitura acontece ANTES de qualquer redução.
#
# ⚑ A FONTE HONESTA É A PRÓPRIA FOTO, não o aparelho de quem envia. Foto da
# galeria mandada três dias depois tem o local CERTO dentro dela; a posição
# do celular naquele instante seria a do escritório. O navegador só entra
# como reforço quando a foto não traz nada — e aí fica marcado que foi ele.
# ===========================================================================
def _graus(valor, referencia: str):
    """O EXIF guarda "3° 43' 54,84\" S"; o cadastro guarda -3,7319.

    Devolve `None` ao primeiro sinal de coisa estranha: coordenada inventada
    põe a obra no meio do oceano no mapa, e num documento que a fiscalização
    assina isso é pior que não ter coordenada.
    """
    try:
        g, m, s = (float(x) for x in valor)
    except (TypeError, ValueError):
        return None
    graus = g + m / 60 + s / 3600
    if str(referencia or "").upper() in ("S", "W"):
        graus = -graus
    if not (-180 <= graus <= 180):
        return None
    return round(graus, 7)


def ler_da_imagem(conteudo: bytes) -> dict[str, Any]:
    """Hora e GPS gravados pelo celular dentro da foto.

    Nunca levanta erro: foto sem EXIF, EXIF torto ou formato que o Pillow não
    entende viram simplesmente "sem local" — e uma foto sem local continua
    valendo como foto.
    """
    vazio = {"tirada_em": None, "latitude": None, "longitude": None,
             "precisao_m": None}
    try:
        import io as _io
        from PIL import Image, ExifTags

        im = Image.open(_io.BytesIO(conteudo))
        bruto = im.getexif()
        if not bruto:
            return vazio
        saida = dict(vazio)

        # A HORA: "DateTimeOriginal" é quando o obturador disparou. O
        # "DateTime" solto é quando o arquivo foi gravado, que uma edição
        # qualquer atualiza — por isso o original vem primeiro.
        nomes = {v: k for k, v in ExifTags.TAGS.items()}
        detalhe = bruto.get_ifd(0x8769) or {}
        texto = (detalhe.get(nomes.get("DateTimeOriginal"))
                 or bruto.get(nomes.get("DateTime")))
        if texto:
            from datetime import datetime as _dt
            for forma in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    saida["tirada_em"] = _dt.strptime(str(texto).strip()[:19],
                                                      forma)
                    break
                except ValueError:
                    continue

        gps = bruto.get_ifd(0x8825) or {}
        if gps:
            # Os números das etiquetas de GPS: 1/2 latitude, 3/4 longitude,
            # 11 a precisão em metros (nem todo aparelho grava).
            lat = _graus(gps.get(2), gps.get(1))
            lon = _graus(gps.get(4), gps.get(3))
            if lat is not None and lon is not None and -90 <= lat <= 90:
                saida["latitude"], saida["longitude"] = lat, lon
                try:
                    precisao = gps.get(11)
                    if precisao is not None:
                        saida["precisao_m"] = int(round(float(precisao)))
                except (TypeError, ValueError):
                    pass
        return saida
    except Exception as e:                              # noqa: BLE001
        logger.info("ERP/diário: foto sem dados legíveis de EXIF (%s)", e)
        return vazio


def _local_da_foto(conteudo: bytes, do_aparelho: Optional[dict[str, Any]]
                   ) -> dict[str, Any]:
    """Junta o que a foto traz com o que o navegador mandou. A foto ganha."""
    local = ler_da_imagem(conteudo)
    if local["latitude"] is not None:
        local["origem_local"] = "EXIF"
        return local
    pelo = do_aparelho or {}
    try:
        lat = float(pelo.get("latitude")) if pelo.get("latitude") is not None else None
        lon = float(pelo.get("longitude")) if pelo.get("longitude") is not None else None
    except (TypeError, ValueError):
        lat = lon = None
    if lat is None or lon is None or not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        local["origem_local"] = None
        return local
    local["latitude"], local["longitude"] = round(lat, 7), round(lon, 7)
    local["origem_local"] = "APARELHO"
    try:
        if pelo.get("precisao_m") is not None:
            local["precisao_m"] = int(round(float(pelo["precisao_m"])))
    except (TypeError, ValueError):
        pass
    return local


def _diario(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    linha = s.execute(text("""
        SELECT id, situacao FROM diarios_obra
         WHERE obra_id = :o AND data = :d"""),
        {"o": obra_id, "d": dia}).mappings().first()
    if linha is None:
        raise ErroNaoEncontrado(
            "Grave o diário deste dia antes de anexar foto — a foto fica "
            "presa ao dia, não à data.")
    return dict(linha)


def listar(s: Session, diario_id: int) -> list[dict[str, Any]]:
    """As fotos do dia, na ordem em que entraram."""
    linhas = s.execute(text("""
        SELECT id, nome_arquivo, descricao, mime_type, tamanho_bytes,
               guardado_em, criado_em,
               tirada_em, latitude, longitude, precisao_m, origem_local
          FROM anexos
         WHERE entidade_tipo = :t AND entidade_id = :i
         ORDER BY id"""), {"t": TIPO, "i": diario_id}).mappings().all()
    return [{
        "id": l["id"], "nome": l["nome_arquivo"],
        "legenda": l["descricao"] or "",
        "mime": l["mime_type"] or "image/jpeg",
        "kb": round((l["tamanho_bytes"] or 0) / 1024, 1),
        # Onde o arquivo está é informação de SAÚDE do sistema, não enfeite:
        # foto que ficou no banco é foto que o Drive recusou naquele momento.
        "no_drive": l["guardado_em"] == "DRIVE",
        # ⚑ O LOCAL VAI PRONTO PARA LER (24/11/2026). A tela não monta
        # coordenada: "-3,731900, -38,526700" se escreve num lugar só, e o
        # papel e a tela dizem a mesma coisa.
        "tirada_em": (l["tirada_em"].isoformat() if l["tirada_em"] else None),
        "latitude": (float(l["latitude"]) if l["latitude"] is not None else None),
        "longitude": (float(l["longitude"]) if l["longitude"] is not None else None),
        "precisao_m": l["precisao_m"],
        "origem_local": l["origem_local"],
        "local": coordenada_por_extenso(l["latitude"], l["longitude"]),
        # Em português, para a tela não ter de traduzir sigla de banco.
        "local_rotulo": {
            "EXIF": "local gravado pela câmera",
            "APARELHO": "local informado pelo aparelho no envio",
        }.get(l["origem_local"] or "", "sem localização"),
    } for l in linhas]


def quantas(s: Session, diario_id: int) -> int:
    return int(s.execute(text(
        "SELECT count(*) FROM anexos WHERE entidade_tipo = :t AND entidade_id = :i"),
        {"t": TIPO, "i": diario_id}).scalar() or 0)


def cabecalho_da_obra(s: Session, obra_id: int) -> str:
    """"094 · CRECHE VILA NOVA" — o que vai no canto do carimbo."""
    linha = s.execute(text("SELECT codigo, nome FROM obras WHERE id = :o"),
                      {"o": obra_id}).first()
    if not linha:
        return ""
    from app.apps.erp.core.comum.rotulos import rotulo_da_obra
    return rotulo_da_obra(linha[0], linha[1])[:48].upper()


def anexar(s: Session, obra_id: int, dia: date, conteudo: bytes,
           nome_arquivo: str, *, legenda: str = "",
           usuario: Optional[Any] = None,
           local_do_aparelho: Optional[dict[str, Any]] = None,
           carimbar_foto: bool = True) -> dict[str, Any]:
    """Guarda uma foto do dia — no Drive, com queda para o banco.

    ⚑ A ORDEM AQUI É A COISA MAIS IMPORTANTE DESTA FUNÇÃO (24/11/2026):

        1. LER o que a foto traz dentro (hora e GPS);
        2. só então CARIMBAR;
        3. só então guardar, que é quando a imagem é recomprimida.

    Invertida, a leitura acontece depois da recompressão e não acha nada —
    que foi exatamente o defeito de 14/11 a 24/11: o dado chegava e era
    destruído no envio.
    """
    from app.apps.erp.core.documentos import armazenamento

    d = _diario(s, obra_id, dia)
    if not conteudo:
        raise ErroValidacao("A foto chegou vazia.")

    # O tipo é decidido pelo NOME do arquivo, como no resto do ERP — o
    # navegador do celular manda `application/octet-stream` com frequência, e
    # recusar por isso seria recusar justamente quem está na obra.
    mime = armazenamento._mime(nome_arquivo)
    if mime not in TIPOS_DE_IMAGEM:
        raise ErroValidacao(
            "Aqui entra só foto (JPG, PNG ou WEBP). Documento da obra tem "
            "lugar próprio, no Arquivo.")

    ja = quantas(s, d["id"])
    if ja >= TETO_POR_DIA:
        raise ErroValidacao(
            f"Este dia já tem {ja} fotos, que é o limite. Apague uma antes de "
            f"acrescentar outra — o relatório de três meses com seis fotos "
            f"por dia já passa de 500 imagens.")

    # 1. LER (antes de qualquer recompressão)
    local = _local_da_foto(conteudo, local_do_aparelho)
    # A hora do carimbo: a da foto quando ela traz; senão a do dia do diário,
    # que é o que se pode afirmar. Nunca "agora" — foto de sábado enviada na
    # segunda sairia carimbada com segunda-feira.
    from datetime import datetime as _dt
    quando = local["tirada_em"] or _dt.combine(dia, _dt.min.time())

    # 2. CARIMBAR — só com local comprovado. Ver `carimbar`.
    if carimbar_foto and local["latitude"] is not None:
        cabeca = cabecalho_da_obra(s, obra_id)
        conteudo = carimbar(
            conteudo, quando=quando,
            latitude=local["latitude"], longitude=local["longitude"],
            precisao_m=local["precisao_m"], obra=cabeca,
            confirmado=local["origem_local"] == "EXIF")

    # 3. GUARDAR
    anexo = armazenamento.salvar(
        s, conteudo, nome_arquivo, entidade_tipo=TIPO, entidade_id=d["id"],
        categoria="FOTO", descricao=legenda, usuario=usuario)
    s.execute(text("""
        UPDATE anexos SET tirada_em = :t, latitude = :la, longitude = :lo,
               precisao_m = :p, origem_local = :og
         WHERE id = :i"""),
        {"t": local["tirada_em"], "la": local["latitude"],
         "lo": local["longitude"], "p": local["precisao_m"],
         "og": local["origem_local"], "i": anexo.id})
    logger.info("ERP/diário: foto %s no dia %s da obra %s (%s, local %s)",
                anexo.id, dia, obra_id, anexo.guardado_em,
                local["origem_local"] or "sem")
    return {"id": anexo.id, "quantas": ja + 1, "teto": TETO_POR_DIA,
            "local": local["origem_local"],
            "latitude": (float(local["latitude"])
                         if local["latitude"] is not None else None),
            "longitude": (float(local["longitude"])
                          if local["longitude"] is not None else None),
            "no_drive": anexo.guardado_em == "DRIVE"}


def legendar(s: Session, obra_id: int, dia: date, anexo_id: int,
             legenda: str, usuario: Optional[Any] = None) -> None:
    """A legenda é o que sai embaixo da foto no papel."""
    d = _diario(s, obra_id, dia)
    achou = s.execute(text("""
        UPDATE anexos SET descricao = :l
         WHERE id = :a AND entidade_tipo = :t AND entidade_id = :i
        RETURNING id"""),
        {"l": (legenda or "").strip()[:300] or None, "a": anexo_id,
         "t": TIPO, "i": d["id"]}).scalar()
    if achou is None:
        raise ErroNaoEncontrado("Foto não encontrada neste dia.")
    s.flush()


def remover(s: Session, obra_id: int, dia: date, anexo_id: int,
            usuario: Optional[Any] = None) -> None:
    """Tira a foto do dia — e só enquanto o diário está ABERTO."""
    from app.apps.erp.core.documentos import armazenamento

    d = _diario(s, obra_id, dia)
    if d["situacao"] == "FECHADO":
        raise ErroValidacao(
            "Este diário já foi fechado. Acrescentar foto pode; tirar, não — "
            "o papel saiu com ela, e sumir com prova de documento fechado é "
            "outra conversa.")
    dono = s.execute(text("""
        SELECT entidade_id FROM anexos
         WHERE id = :a AND entidade_tipo = :t"""),
        {"a": anexo_id, "t": TIPO}).scalar()
    if dono != d["id"]:
        raise ErroNaoEncontrado("Foto não encontrada neste dia.")
    armazenamento.excluir(s, anexo_id, usuario)


# ---------------------------------------------------------------------------
# AS FOTOS NO PAPEL
# ---------------------------------------------------------------------------
# ===========================================================================
# O CARIMBO — sobre a foto, sem tapar a foto (24/11/2026)
#
# Ele, corrigindo a primeira maquete, que era uma tarja preta embaixo:
#
#     *"Da forma que está, o que acontece? Você fez uma espécie de tarja. O
#      carimbo ele fica sobre a foto, mas ele não apaga os dados da foto. Ele
#      fica como se fosse uma… não é uma marca d'água, porque a marca d'água
#      normalmente ela é meio transparente, mas é como se fosse uma marca
#      d'água viva sobre a imagem. Pelo menos é assim que as pessoas têm feito
#      aqui quando eles tiram uma foto."*
#
# Então: véu escuro de pouca opacidade e texto cheio por cima. O que estava
# na foto continua visível por baixo — nada é apagado. É o que os aplicativos
# de câmera com carimbo fazem, e é o que a equipe dele já reconhece.
#
# ⚠️ SÓ SE CARIMBA FOTO COM LOCAL COMPROVADO. Carimbo em foto sem GPS seria o
# sistema AFIRMANDO onde ela foi tirada sem saber — num papel que a
# fiscalização assina. Sem local, a foto entra sem carimbo e a tela diz por
# quê.
# ===========================================================================
ALTURA_DO_CARIMBO = 0.13          # do lado maior da imagem
OPACIDADE_DO_VEU = 110            # de 0 a 255 — véu, não tarja


def _fonte(tamanho: int, negrito: bool = False):
    import os
    from PIL import ImageFont
    caminhos = (["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
                if negrito else
                ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"])
    for c in caminhos:
        if os.path.exists(c):
            try:
                return ImageFont.truetype(c, tamanho)
            except Exception:                           # noqa: BLE001
                break
    return ImageFont.load_default()


def coordenada_por_extenso(lat, lon) -> str:
    """-3.7319 vira "-3,731900" — vírgula decimal, como se lê no Brasil."""
    if lat is None or lon is None:
        return ""
    return f"{float(lat):.6f}".replace(".", ",") + ", " \
           + f"{float(lon):.6f}".replace(".", ",")


def carimbar(conteudo: bytes, *, quando, latitude, longitude,
             precisao_m=None, obra: str = "", confirmado: bool = True) -> bytes:
    """Escreve data, hora e coordenada SOBRE a foto, sem tapá-la.

    Devolve a imagem original se qualquer coisa der errado: foto sem carimbo
    é melhor que foto que não abre.
    """
    if latitude is None or longitude is None:
        return conteudo
    try:
        import io as _io
        from PIL import Image, ImageDraw

        base = Image.open(_io.BytesIO(conteudo))
        base.load()
        if base.mode != "RGB":
            base = base.convert("RGB")
        L, A = base.size
        alt = max(64, int(min(L, A) * ALTURA_DO_CARIMBO))

        # O VÉU em camada própria, com canal de transparência: desenhar
        # direto na foto apagaria os pixels, que é justamente o que ele não
        # quer.
        veu = Image.new("RGBA", (L, alt), (0, 0, 0, OPACIDADE_DO_VEU))
        base.paste(Image.alpha_composite(
            base.crop((0, A - alt, L, A)).convert("RGBA"), veu).convert("RGB"),
            (0, A - alt))

        d = ImageDraw.Draw(base)
        grande = _fonte(max(18, int(alt * 0.34)), True)
        miudo = _fonte(max(14, int(alt * 0.24)))
        margem = max(10, int(alt * 0.16))

        def escrever(xy, texto, fonte, cor=(255, 255, 255)):
            # Contorno escuro: sobre céu claro, texto branco puro some.
            x, y = xy
            for dx, dy in ((-2, 0), (2, 0), (0, -2), (0, 2)):
                d.text((x + dx, y + dy), texto, font=fonte, fill=(0, 0, 0))
            d.text(xy, texto, font=fonte, fill=cor)

        quando_txt = quando.strftime("%d/%m/%Y  %H:%M") if quando else ""
        escrever((margem, A - alt + margem), quando_txt, grande)
        linha2 = coordenada_por_extenso(latitude, longitude)
        if precisao_m:
            linha2 += f"   ±{int(precisao_m)} m"
        if not confirmado:
            linha2 += "   (local informado pelo aparelho)"
        escrever((margem, A - alt + margem + int(alt * 0.40)), linha2, miudo)
        if obra:
            largura = d.textlength(obra, font=miudo)
            escrever((L - largura - margem, A - alt + margem), obra, miudo,
                     (255, 206, 84))

        saida = _io.BytesIO()
        base.save(saida, format="JPEG", quality=86, optimize=True)
        return saida.getvalue()
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/diário: não consegui carimbar a foto (%s)", e)
        return conteudo


def _menor(conteudo: bytes, mime: str) -> tuple[bytes, str]:
    """Encolhe a foto para caber num relatório de 90 dias.

    Falhando a redução, devolve a original: foto grande num PDF grande é pior
    que nada? Não — é melhor que folha em branco. Quem decide isso aqui é o
    bom senso de quem vai ter de explicar um anexo fotográfico vazio.
    """
    try:
        import io as _io

        from PIL import Image
        im = Image.open(_io.BytesIO(conteudo))
        im.load()
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        if max(im.size) > LADO_NO_PAPEL:
            escala = LADO_NO_PAPEL / max(im.size)
            im = im.resize((max(1, int(im.width * escala)),
                            max(1, int(im.height * escala))),
                           Image.LANCZOS)
        saida = _io.BytesIO()
        im.save(saida, format="JPEG", quality=QUALIDADE_NO_PAPEL, optimize=True)
        menor = saida.getvalue()
        if menor and len(menor) < len(conteudo):
            return menor, "image/jpeg"
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/diário: não consegui encolher a foto (%s)", e)
    return conteudo, mime


def para_o_papel(s: Session, diario_id: int) -> list[dict[str, Any]]:
    """As fotos embutidas na folha, prontas para imprimir.

    ⚑ EMBUTIDAS, e não por link: o papel é gerado pelo navegador de quem
    imprime, e um `<img src="/erp/anexo/123">` dependeria de a sessão ainda
    valer na hora — além de fazer 540 idas ao servidor num relatório de três
    meses. O endereço do Drive nunca sai do servidor (regra do `drive.py`).

    Foto que não abrir NÃO derruba o relatório: ela some da folha e fica o
    registro no log. Diário que não imprime porque uma imagem está corrompida
    é pior que diário com cinco fotos em vez de seis.
    """
    import base64

    from app.apps.erp.core.documentos import armazenamento
    from app.apps.erp.db.models.financeiro import Anexo

    saida = []
    for linha in listar(s, diario_id):
        try:
            anexo = s.get(Anexo, linha["id"])
            dados = armazenamento.conteudo_de(s, anexo)
            if not dados:
                continue
            menor, mime = _menor(dados, linha["mime"])
            saida.append({
                "legenda": linha["legenda"],
                "nome": linha["nome"],
                # ⚑ O LOCAL VAI PARA O PAPEL (24/11/2026), embaixo da
                # legenda. O carimbo já está dentro da imagem; isto é o
                # mesmo dado em texto, que se lê e se copia de um PDF —
                # carimbo em foto impressa pequena não se transcreve.
                "local": linha["local"],
                "local_rotulo": linha["local_rotulo"],
                "precisao_m": linha["precisao_m"],
                "tem_local": linha["latitude"] is not None,
                "dados": f"data:{mime};base64," + base64.b64encode(menor).decode(),
            })
        except Exception as e:                          # noqa: BLE001
            logger.warning("ERP/diário: foto %s ficou de fora do papel (%s)",
                           linha["id"], e)
    return saida
