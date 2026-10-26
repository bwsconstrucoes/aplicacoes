# -*- coding: utf-8 -*-
"""
Leitura do atestado por IA — o DP confere, nunca é gravado sem alguém olhar.

Usa o MESMO leitor de documentos do ERP (`erp/core/documentos/leitor.py`), com
a chave e o registro de consumo de IA que ele já tem — nada de chave nova, nada
de segundo caminho para a IA. A leitura só preenche: dias, médico, CRM e CID
voltam para a tela do DP, que corrige e aprova (regra da casa: "o que a
leitura preenche fica editável e vai para conferência").
"""
from __future__ import annotations

import json
import logging

from sqlalchemy.engine import Connection

from .. import db
from ..erros import ErroDeValidacao
from . import documentos

logger = logging.getLogger("ponto.leitura_atestado")

INSTRUCAO = """Você lê ATESTADOS e DECLARAÇÕES médicas brasileiras.
Devolva SÓ um JSON, sem texto em volta, com estas chaves:
{"tipo": "ATESTADO" | "DECLARACAO_DE_COMPARECIMENTO" | "OUTRO",
 "data_emissao": "AAAA-MM-DD" ou null,
 "data_inicio": "AAAA-MM-DD" ou null,
 "dias_afastamento": número inteiro ou null,
 "horario_inicio": "HH:MM" ou null, "horario_fim": "HH:MM" ou null,
 "medico": texto ou null, "crm": texto ou null, "cid": texto ou null,
 "paciente": texto ou null, "legivel": true | false,
 "observacao": texto curto com o que ficou duvidoso}
Regras: não invente. Campo que não está escrito é null. Datas no formato
brasileiro (dd/mm/aaaa) viram AAAA-MM-DD. "dias_afastamento" é o número de dias
de repouso escrito no documento (por extenso ou em número)."""


def ler(conn: Connection, ocorrencia_id: int) -> dict:
    o = db.um(conn, "SELECT id, tipo, documento_id FROM ponto.ocorrencias WHERE id = :id",
              id=ocorrencia_id)
    if not o or not o["documento_id"]:
        raise ErroDeValidacao("este pedido não tem documento para ler", campo="documento")
    dados, mime, _ = documentos.baixar(conn, o["documento_id"])
    from app.apps.erp.core.comum.ia_custo import contexto
    from app.apps.erp.core.documentos import leitor
    try:
        with contexto(operacao="ponto_atestado", referencia=f"ponto.ocorrencia:{ocorrencia_id}"):
            if mime == "application/pdf":
                texto = leitor._texto_do_pdf(dados)
                imagens = [] if texto else [(b, "image/png") for b in leitor._paginas_como_png(dados)]
                resultado = leitor._chamar_ia(texto=texto, imagens=imagens, instrucao=INSTRUCAO)
            else:
                b64, media = leitor._preparar_imagem(dados)
                resultado = leitor._chamar_ia(imagens=[(b64, media)], instrucao=INSTRUCAO)
    except leitor.ErroLeitura as e:
        raise ErroDeValidacao(str(e), campo="documento") from e
    db.executar(conn, "UPDATE ponto.ocorrencias SET leitura_ia = CAST(:j AS jsonb), "
                      "atualizado_em = now() WHERE id = :id",
                j=json.dumps(resultado, ensure_ascii=False, default=str), id=ocorrencia_id)
    logger.info("Ponto: atestado do pedido %d lido pela IA", ocorrencia_id)
    return resultado


# ---------------------------------------------------------------------------
# O CONFRONTO (26/10/2026, o dono: "é bom já cruzar o que foi lido pela IA com o que
# foi lançado — as datas, a quantidade de dias, o nome da pessoa — ver o que bate e
# já dar um confronto"). Campo a campo: BATE, NAO_BATE, SO_NO_DOCUMENTO (o pedido não
# tem; a leitura sugere) ou NAO_LIDO (o documento não diz). A decisão continua do DP.
# ---------------------------------------------------------------------------
import datetime as _dt
import re as _re
import unicodedata as _ud

_CONECTIVOS = {"DA", "DE", "DO", "DAS", "DOS", "E"}


def _palavras(nome) -> list[str]:
    t = _ud.normalize("NFKD", str(nome or "")).encode("ascii", "ignore").decode().upper()
    return [p for p in _re.findall(r"[A-Z]+", t) if p not in _CONECTIVOS]


def nomes_batem(lido, cadastro) -> bool:
    """PURA. O nome do documento é o da pessoa? Todas as palavras lidas estão no nome do
    cadastro (o médico costuma abreviar ou cortar o sobrenome), e o primeiro nome é o mesmo."""
    a, b = _palavras(lido), _palavras(cadastro)
    if not a or not b:
        return False
    return a[0] == b[0] and all(p in b or any(x.startswith(p) for x in b) for p in a)


def _data(v):
    try:
        return _dt.date.fromisoformat(str(v)[:10]) if v else None
    except ValueError:
        return None


def _texto(v) -> str:
    return _re.sub(r"[^0-9A-Z]", "", _ud.normalize("NFKD", str(v or "")).encode("ascii", "ignore").decode().upper())


def confronto(leitura: dict, pedido: dict) -> dict:
    """PURA. O que a leitura achou × o que foi lançado. `pedido` traz colaborador,
    data_inicio, data_fim (AAAA-MM-DD), cid, medico, crm."""
    if not leitura:
        return {"itens": [], "nao_batem": 0, "resumo": "sem leitura"}
    itens = []

    def item(campo, lido, lancado, situacao, nota=""):
        itens.append({"campo": campo, "lido": lido, "lancado": lancado, "situacao": situacao, "nota": nota})

    # a pessoa
    if leitura.get("paciente"):
        ok = nomes_batem(leitura["paciente"], pedido.get("colaborador"))
        item("Nome da pessoa", leitura["paciente"], pedido.get("colaborador"), "BATE" if ok else "NAO_BATE",
             "" if ok else "o nome no documento não é o desta pessoa — confira se o papel é dela")
    else:
        item("Nome da pessoa", None, pedido.get("colaborador"), "NAO_LIDO", "o documento não diz o nome (ou não deu para ler)")
    # as datas e os dias
    ini_p, fim_p = _data(pedido.get("data_inicio")), _data(pedido.get("data_fim"))
    ini_l = _data(leitura.get("data_inicio")) or _data(leitura.get("data_emissao"))
    dias_l = leitura.get("dias_afastamento")
    try:
        dias_l = int(dias_l) if dias_l not in (None, "") else None
    except (TypeError, ValueError):
        dias_l = None
    dias_p = (fim_p - ini_p).days + 1 if ini_p and fim_p else None
    if ini_l and ini_p:
        item("Primeiro dia", ini_l.isoformat(), ini_p.isoformat(), "BATE" if ini_l == ini_p else "NAO_BATE",
             "" if ini_l == ini_p else f"o documento começa {abs((ini_l - ini_p).days)} dia(s) "
                                       f"{'depois' if ini_l > ini_p else 'antes'} do lançado")
    else:
        item("Primeiro dia", ini_l.isoformat() if ini_l else None, ini_p.isoformat() if ini_p else None, "NAO_LIDO")
    if dias_l is not None and dias_p is not None:
        item("Dias de afastamento", dias_l, dias_p, "BATE" if dias_l == dias_p else "NAO_BATE",
             "" if dias_l == dias_p else f"o documento dá {dias_l} dia(s); o lançado tem {dias_p}")
    else:
        item("Dias de afastamento", dias_l, dias_p, "NAO_LIDO",
             "declaração de comparecimento costuma não ter dias — vale só o horário" if leitura.get("tipo") == "DECLARACAO_DE_COMPARECIMENTO" else "")
    if ini_l and dias_l and fim_p:
        fim_l = ini_l + _dt.timedelta(days=dias_l - 1)
        item("Último dia", fim_l.isoformat(), fim_p.isoformat(), "BATE" if fim_l == fim_p else "NAO_BATE")
    # médico, CRM e CID
    for campo, chave in (("CID", "cid"), ("CRM", "crm"), ("Médico", "medico")):
        l, p = leitura.get(chave), pedido.get(chave)
        if l and p:
            ok = _texto(l) == _texto(p) or (chave == "medico" and nomes_batem(l, p))
            item(campo, l, p, "BATE" if ok else "NAO_BATE")
        elif l:
            item(campo, l, None, "SO_NO_DOCUMENTO", "o documento tem; o lançado não — dá para preencher com a leitura")
    # os avisos do documento
    avisos = []
    if leitura.get("legivel") is False:
        avisos.append("documento pouco legível — a leitura pode ter errado")
    if leitura.get("tipo") == "DECLARACAO_DE_COMPARECIMENTO" and dias_p and dias_p >= 1 and not leitura.get("dias_afastamento"):
        avisos.append("é DECLARAÇÃO DE COMPARECIMENTO, não atestado de repouso: abona só o horário escrito"
                      + (f" ({leitura.get('horario_inicio')} às {leitura.get('horario_fim') or '?'})" if leitura.get("horario_inicio") else ""))
    emissao = _data(leitura.get("data_emissao"))
    if emissao and ini_p and (emissao - ini_p).days > 2:
        avisos.append(f"emitido em {emissao:%d/%m/%Y}, {(emissao - ini_p).days} dias depois do primeiro dia lançado — atestado com data para trás")
    if leitura.get("tipo") == "OUTRO":
        avisos.append("a leitura não reconheceu como atestado nem declaração")
    nao = sum(1 for i in itens if i["situacao"] == "NAO_BATE")
    return {"itens": itens, "avisos": avisos, "nao_batem": nao,
            "resumo": ("tudo o que foi lido bate com o lançado" if not nao and not avisos
                       else f"{nao} ponto(s) não bate(m)" + (f" e {len(avisos)} aviso(s)" if avisos else "") if nao
                       else f"{len(avisos)} aviso(s)")}
