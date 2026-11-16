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
               guardado_em, criado_em
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
    } for l in linhas]


def quantas(s: Session, diario_id: int) -> int:
    return int(s.execute(text(
        "SELECT count(*) FROM anexos WHERE entidade_tipo = :t AND entidade_id = :i"),
        {"t": TIPO, "i": diario_id}).scalar() or 0)


def anexar(s: Session, obra_id: int, dia: date, conteudo: bytes,
           nome_arquivo: str, *, legenda: str = "",
           usuario: Optional[Any] = None) -> dict[str, Any]:
    """Guarda uma foto do dia — no Drive, com queda para o banco."""
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

    anexo = armazenamento.salvar(
        s, conteudo, nome_arquivo, entidade_tipo=TIPO, entidade_id=d["id"],
        categoria="FOTO", descricao=legenda, usuario=usuario)
    logger.info("ERP/diário: foto %s no dia %s da obra %s (%s)",
                anexo.id, dia, obra_id, anexo.guardado_em)
    return {"id": anexo.id, "quantas": ja + 1, "teto": TETO_POR_DIA,
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
                "dados": f"data:{mime};base64," + base64.b64encode(menor).decode(),
            })
        except Exception as e:                          # noqa: BLE001
            logger.warning("ERP/diário: foto %s ficou de fora do papel (%s)",
                           linha["id"], e)
    return saida
