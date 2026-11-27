"""A CONFERÊNCIA DO RETORNO DA CONTABILIDADE — o agente lê o documento que
voltou e, se bate com o cadastro, conclui sozinho (26/11/2026).

O dono: *"Um simples cadastro no dependente. O DP já disparou para a
contabilidade. Cara, contabilidade respondeu, a gente leu, foi atendido,
finalizou."* E: *"A conclusão (…) pode. Mas é bom dar destaque tudo que for
feito pelo agente."*

Como funciona:
  1. a caixa de entrada casa a resposta com o pedido e arquiva o anexo
     (`email_entrada._entregar_anexos`);
  2. aqui, UMA leitura por IA tira do documento os campos que o modelo do
     pedido manda conferir (nome, CPF, cargo, salário, datas);
  3. `contabilidade.receber(lido=…)` compara com o cadastro: diferença vira
     DIVERGENTE e fica com o DP, com o de/para à vista;
  4. sem diferença, e com a rotina "conferencia" em FAZ, o pedido é
     CONCLUÍDO pelo agente — o que fecha a solicitação ligada e avisa quem
     pediu (`pessoal_solicitacoes.avisar_quem_pediu`);
  5. cada passo vai para o diário do agente (`agente_diario`), para revisão.

O que NUNCA conclui sozinho, mesmo batendo: documento que não deu para ler (sem
nome e sem CPF lidos), o desligamento (pagamento e encerramento são de gente) e
a solicitação diversa (texto livre — o DP interpreta).
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

NAO_CONCLUI_SOZINHO = ("DESLIGAMENTO", "DIVERSOS")

INSTRUCAO = (
    "Você lê um documento que o escritório de contabilidade devolveu à empresa "
    "(ficha de registro, alteração contratual, aviso de férias, comprovante de "
    "inclusão de dependente, termo, etc.). Responda SÓ com um JSON com estas "
    "chaves, deixando vazio o que o documento não mostrar com clareza — nunca "
    "invente: "
    '{"nome": "nome completo do EMPREGADO (não do dependente)", '
    '"cpf": "CPF do empregado, só números", "cargo": "cargo/função", '
    '"salario": "salário em reais, só números com ponto decimal", '
    '"admissao": "data de admissão AAAA-MM-DD", '
    '"data_saida": "data de desligamento AAAA-MM-DD", '
    '"o_que_e": "uma frase: que documento é este"}'
)
CAMPOS = ("nome", "cpf", "cargo", "salario", "admissao", "data_saida")


def ler_retorno(conteudo: bytes, nome_arquivo: str) -> dict[str, Any]:
    """Os campos que o documento mostra. Vazio quando não deu para ler."""
    from app.apps.erp.core.comum.ia_custo import contexto
    from app.apps.erp.core.documentos.leitor import ler_com_instrucao
    with contexto(operacao="conferencia_retorno_contabilidade"):
        bruto = ler_com_instrucao(conteudo, nome_arquivo, INSTRUCAO) or {}
    lido = {k: str(bruto.get(k) or "").strip() for k in CAMPOS}
    lido = {k: v for k, v in lido.items() if v}
    if bruto.get("o_que_e"):
        lido["_o_que_e"] = str(bruto["o_que_e"])[:200]
    return lido


def conferir_e_talvez_concluir(s: Session, pedido_id: int, documento_id: int,
                               conteudo: bytes, nome_arquivo: str,
                               usuario: Optional[Usuario] = None) -> dict[str, Any]:
    from app.apps.erp.core import agente_diario, agente_dp
    from app.apps.erp.core import contabilidade as svc_ct

    ligada = agente_dp.modo(s, "conferencia") != "DESLIGADA"
    lido: dict[str, Any] = {}
    if ligada:
        try:
            lido = ler_retorno(conteudo, nome_arquivo)
        except Exception:                                # noqa: BLE001
            logger.warning("ERP/conferência: não consegui ler o retorno do pedido %s",
                           pedido_id, exc_info=True)
            lido = {}
    para_comparar = {k: v for k, v in lido.items() if not k.startswith("_")}
    r = svc_ct.receber(s, pedido_id, usuario, documento_id=documento_id,
                       lido=para_comparar, pelo_agente=True)
    if not ligada:
        return {**r, "concluiu": False}
    p = svc_ct.ler(s, pedido_id)
    if not para_comparar:
        agente_diario.registrar(
            s, "CONFERIU", f"{p['numero']}: o documento voltou, mas não consegui ler "
            "nome nem CPF — fica para o DP conferir.", pedido_id=pedido_id,
            detalhe={"lido": lido})
        return {**r, "concluiu": False}
    if r["divergencias"]:
        diffs = "; ".join(f"{d['campo']}: no ERP {d['no_erp']}, no documento {d['no_documento']}"
                          for d in r["divergencias"])
        agente_diario.registrar(
            s, "DIVERGIU", f"{p['numero']}: o documento voltou com diferença — {diffs}. "
            "Deixei para o DP.", pedido_id=pedido_id, detalhe={"lido": lido})
        return {**r, "concluiu": False}
    conferidos = ", ".join(k for k in para_comparar)
    if p["template"] in NAO_CONCLUI_SOZINHO or agente_dp.modo(s, "conferencia") != "FAZ":
        agente_diario.registrar(
            s, "CONFERIU", f"{p['numero']}: o documento voltou e bate com o cadastro "
            f"({conferidos}). Este tipo o DP conclui.", pedido_id=pedido_id,
            detalhe={"lido": lido})
        return {**r, "concluiu": False}
    svc_ct.concluir(s, pedido_id, usuario, motivo="o agente conferiu o documento: "
                    f"bate com o cadastro ({conferidos})", pelo_agente=True)
    agente_diario.registrar(
        s, "CONCLUIU", f"{p['numero']}: o documento voltou ({lido.get('_o_que_e') or 'documento'}) "
        f"e bate com o cadastro ({conferidos}) — concluí o pedido e fechei a solicitação.",
        pedido_id=pedido_id, detalhe={"lido": lido})
    return {**r, "concluiu": True}
