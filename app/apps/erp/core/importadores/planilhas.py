# ============================================================================
# BWS ERP — core/importadores/planilhas.py
# Importação de OBRAS e CATEGORIAS (plano financeiro) a partir de CSV
# exportado das planilhas Google. O de-para coluna→campo é explícito e
# apresentado ao usuário ANTES da importação (governança combinada).
#
# Formato esperado (cabeçalhos, maiúsc./minúsc. indiferente):
#   obras.csv:      codigo,nome,cno,municipio,uf,codigo_omie_depto
#   categorias.csv: codigo,descricao,codigo_omie,dedutivel_padrao,tipos_permitidos
#     - tipos_permitidos: lista separada por ; (ex.: T1_MATERIAL_NFE;T2_SERVICO_NFSE)
#     - vazio em tipos_permitidos = aceita todos os tipos
# ============================================================================
from __future__ import annotations

import csv
import io
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import categorias as svc_cat
from app.apps.erp.core.cadastros import obras as svc_obra
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Usuario


def _ler_csv(conteudo: bytes) -> list[dict[str, str]]:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            texto = conteudo.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ErroValidacao("CSV com codificação não reconhecida.")
    amostra = texto[:2048]
    delim = ";" if amostra.count(";") > amostra.count(",") else ","
    leitor = csv.DictReader(io.StringIO(texto), delimiter=delim)
    return [{(k or "").strip().lower(): (v or "").strip() for k, v in linha.items()}
            for linha in leitor]


def importar_obras_csv(s: Session, conteudo: bytes,
                       usuario: Optional[Usuario]) -> dict[str, Any]:
    linhas = _ler_csv(conteudo)
    criadas, rejeitadas = 0, []
    for i, ln in enumerate(linhas, start=2):     # linha 1 = cabeçalho
        try:
            svc_obra.criar(s, {
                "codigo": ln.get("codigo"), "nome": ln.get("nome"),
                "cno": ln.get("cno"), "municipio": ln.get("municipio"),
                "uf": ln.get("uf"), "codigo_omie_depto": ln.get("codigo_omie_depto"),
                "objeto": ln.get("objeto"), "cliente": ln.get("cliente"),
                "cnpj_cliente": ln.get("cnpj_cliente"), "contrato": ln.get("contrato"),
                "valor_contrato": ln.get("valor_contrato"),
                "aliquota_iss": ln.get("aliquota_iss"), "tributacao": ln.get("tributacao"),
                "data_inicio": ln.get("data_inicio"), "data_termino": ln.get("data_termino"),
                "orgao_resumido": ln.get("orgao_resumido"), "ref_pipefy": ln.get("ref_pipefy"),
                "origem": "IMPORTACAO_CSV",
            }, usuario)
            criadas += 1
        except ErroValidacao as e:
            rejeitadas.append({"linha": i, "codigo": ln.get("codigo"), "motivo": str(e)})
    return {"no_arquivo": len(linhas), "criadas": criadas, "rejeitadas": rejeitadas}


def importar_categorias_csv(s: Session, conteudo: bytes,
                            usuario: Optional[Usuario]) -> dict[str, Any]:
    linhas = _ler_csv(conteudo)
    criadas, rejeitadas = 0, []
    for i, ln in enumerate(linhas, start=2):
        try:
            tipos = [t.strip() for t in (ln.get("tipos_permitidos") or "").split(";") if t.strip()]
            dedutivel = (ln.get("dedutivel_padrao") or "sim").strip().lower() not in ("nao", "não", "n", "0", "false")
            svc_cat.criar(s, {
                "codigo": ln.get("codigo"), "descricao": ln.get("descricao"),
                "codigo_omie": ln.get("codigo_omie"),
                "natureza": ln.get("natureza") or "RESULTADO",
                "tipos_permitidos": tipos, "dedutivel_padrao": dedutivel,
                "origem": "IMPORTACAO_CSV",
            }, usuario)
            criadas += 1
        except ErroValidacao as e:
            rejeitadas.append({"linha": i, "codigo": ln.get("codigo"), "motivo": str(e)})
    return {"no_arquivo": len(linhas), "criadas": criadas, "rejeitadas": rejeitadas}
