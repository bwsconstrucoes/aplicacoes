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
