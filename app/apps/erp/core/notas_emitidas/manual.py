# ============================================================================
# ERP — core/notas_emitidas/manual.py
# A emissão da nota no modo MANUAL: o ERP prepara, a pessoa emite no portal da
# prefeitura, e o ERP registra de volta.
#
# POR QUE O MANUAL VEM ANTES DO AUTOMÁTICO
#
# Está escrito no roteiro e vale repetir: o caminho manual **funciona no dia
# seguinte**, sem credenciamento, sem certificado e sem token — e continua
# servindo de rede quando a API falhar ou quando a prefeitura estiver fora do
# ar. Construir o automático primeiro deixaria a segunda empresa (a de
# Petrolina, que ainda nem tem inscrição municipal) sem emitir nada.
#
# O QUE ESTA PARTE FAZ, E O QUE ELA NÃO FAZ
#
#   FAZ    junta num bloco só tudo que o portal pergunta — prestador, tomador,
#          discriminação do serviço, valor, e as retenções JÁ CALCULADAS pelo
#          cadastro da obra. Copiar campo por campo de três telas diferentes é
#          onde se erra um dígito, e nota com dígito errado se conserta com
#          cancelamento e carta ao cliente.
#   NÃO FAZ   emitir. Quem emite é a pessoa, no site da prefeitura. O ERP não
#          reserva número antes: no manual quem numera a nota é o portal, e
#          reservar aqui criaria uma sequência paralela que não existe lá.
#
# DEPOIS DE EMITIR, a nota volta para cá — e volta com o PDF. A IA lê o
# documento e sugere número, data e valores; a pessoa confere. É o mesmo
# leitor que já lê comprovante e nota de fornecedor, então não há caminho novo
# de leitura para manter.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import emissao as svc_emissao
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.titulos import tributacao
from app.apps.erp.db.models.cadastros import Empresa, Fornecedor, Obra
from app.apps.erp.db.models.financeiro import Titulo

logger = logging.getLogger(__name__)

# Como as retenções que o cálculo produz viram as colunas da tela de notas.
# O PCC é o caso especial: quando PIS, COFINS e CSLL são retidos juntos, a
# guia traz uma linha só de 4,65% — e a contabilidade precisa dos três
# separados de novo.
PCC = {"pis": Decimal("0.0065"), "cofins": Decimal("0.03"), "csll": Decimal("0.01")}
DE_PARA = {"ISS": "iss", "INSS": "inss", "IRRF": "ir", "IR": "ir",
           "PIS": "pis", "COFINS": "cofins", "CSLL": "csll"}


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal(0)


def _brl(v: Any) -> str:
    """Dinheiro em português. `f"R$ {v:.2f}"` é formato americano, e o bloco
    aqui é copiado direto para dentro do portal."""
    from app.apps.erp.core.comum.formato import _dinheiro_br
    return _dinheiro_br(v)


def retencoes_em_colunas(calculo) -> dict[str, float]:
    """O cálculo da obra traduzido para as colunas da tela de notas emitidas."""
    saida = {c: Decimal(0) for c in ("iss", "ir", "inss", "pis", "cofins", "csll")}
    for r in calculo.retencoes:
        if r.tipo == "PCC":
            # Reparte pela alíquota de cada um, que é como a guia se desfaz.
            total = sum(PCC.values())
            for chave, aliq in PCC.items():
                saida[chave] += (_dec(r.valor) * aliq / total).quantize(Decimal("0.01"))
            continue
        chave = DE_PARA.get(r.tipo)
        if chave:
            saida[chave] += _dec(r.valor)
    return {k: float(v) for k, v in saida.items()}


def _obra_do_titulo(s: Session, titulo: Titulo) -> Optional[Obra]:
    from app.apps.erp.db.models.financeiro import Rateio
    obras = sorted({r.obra_id for r in s.scalars(
        select(Rateio).where(Rateio.titulo_id == titulo.id)).all() if r.obra_id})
    return s.get(Obra, obras[0]) if len(obras) == 1 else None


def _discriminacao(titulo: Titulo, obra: Optional[Obra]) -> str:
    """O texto do serviço, como o órgão espera ler.

    Vai montado e não em branco porque é o campo que mais volta corrigido: sem
    o número da medição e o período, o setor de empenho do órgão não sabe a
    que competência a nota se refere e devolve a nota.
    """
    partes = []
    if titulo.numero_medicao:
        partes.append(f"Medição nº {titulo.numero_medicao}")
    if titulo.periodo_inicio and titulo.periodo_fim:
        partes.append(f"período de {titulo.periodo_inicio.strftime('%d/%m/%Y')} "
                      f"a {titulo.periodo_fim.strftime('%d/%m/%Y')}")
    if obra is not None:
        if obra.contrato:
            partes.append(f"contrato {obra.contrato}")
        if obra.objeto:
            partes.append(obra.objeto)
        elif obra.nome:
            partes.append(obra.nome)
        if obra.cno:
            partes.append(f"CNO {obra.cno}")
    return " — ".join(partes) or (titulo.descricao or "")


def preparar(s: Session, titulo_id: int) -> dict[str, Any]:
    """Tudo que o portal da prefeitura pergunta, num bloco só.

    Não reserva número e não emite: só junta. A tela mostra, a pessoa copia.
    """
    titulo = s.get(Titulo, titulo_id)
    if titulo is None:
        raise ErroValidacao("Título não encontrado.")
    if not titulo.numero_medicao:
        raise ErroValidacao(
            "Este título não é uma medição — não tem número de medição. "
            "A nota de serviço sai da medição.")

    # Por onde a nota sai NÃO se escolhe: desce a cadeia medição → obra →
    # empresa. Decisão do dono em 09/09/2026.
    destino = svc_emissao.resolver(s, titulo_id=titulo.id)
    obra = _obra_do_titulo(s, titulo)
    empresa = (s.get(Empresa, destino["empresa_id"])
               if destino.get("empresa_id") else None)
    cliente = (s.get(Fornecedor, titulo.fornecedor_id)
               if titulo.fornecedor_id else None)

    bruto = _dec(titulo.valor_liquido)
    calculo = (tributacao.calcular(obra, bruto) if obra is not None else None)
    colunas = retencoes_em_colunas(calculo) if calculo else {}

    tomador_nome = (getattr(obra, "cliente", "")
                    or getattr(cliente, "razao_social", "") or "")
    tomador_doc = (getattr(obra, "cnpj_cliente", "")
                   or getattr(cliente, "cnpj_cpf", "") or "")

    linhas = [
        f"PRESTADOR: {getattr(empresa, 'razao_social', '(empresa não definida)')}",
        f"CNPJ: {getattr(empresa, 'cnpj', '')}"
        + (f"   Inscrição municipal: {empresa.inscricao_municipal}"
           if getattr(empresa, "inscricao_municipal", "") else ""),
        f"MUNICÍPIO DA NOTA: {destino.get('municipio') or getattr(obra, 'municipio', '') or '(não definido)'}",
        "",
        f"TOMADOR: {tomador_nome or '(cliente não cadastrado na obra)'}",
        f"CNPJ do tomador: {tomador_doc or '(faltando)'}",
        "",
        f"DISCRIMINAÇÃO: {_discriminacao(titulo, obra)}",
        f"VALOR DO SERVIÇO: {_brl(bruto)}",
    ]
    if calculo is not None:
        for r in calculo.retencoes:
            linhas.append(f"  {r.tipo} retido: {_brl(r.valor)}   ({r.explicacao})")
        linhas.append(f"LÍQUIDO A RECEBER: {_brl(calculo.valor_liquido)}")

    return {
        "titulo_id": titulo.id, "numero_sp": titulo.numero_sp,
        "medicao": titulo.numero_medicao,
        "destino": destino,
        "obra": getattr(obra, "codigo", ""),
        "obra_id": getattr(obra, "id", None),
        "tomador": {"nome": tomador_nome, "documento": tomador_doc},
        "discriminacao": _discriminacao(titulo, obra),
        "valor_bruto": float(bruto),
        "retencoes": colunas,
        "retido": float(calculo.total_retencoes) if calculo else 0.0,
        "valor_liquido": float(calculo.valor_liquido) if calculo else float(bruto),
        "avisos": (calculo.avisos if calculo else
                   ["Sem obra definida: não dá para calcular as retenções. "
                    "Confira o rateio deste título."]),
        "detalhe_retencoes": ([{"tipo": r.tipo, "base": str(r.base_calculo),
                                "aliquota": str(r.aliquota), "valor": str(r.valor),
                                "explicacao": r.explicacao}
                               for r in calculo.retencoes] if calculo else []),
        # O que já foi emitido para ESTA medição. Uma medição pode virar duas
        # notas de propósito — mas a pessoa tem de ver a primeira antes de
        # emitir a segunda, senão a duplicidade acontece sem ninguém notar.
        "ja_emitidas": _ja_emitidas(s, titulo.id),
        "texto_para_copiar": "\n".join(linhas),
    }


def _ja_emitidas(s: Session, titulo_id: int) -> list[dict[str, Any]]:
    from app.apps.erp.db.models.financeiro import NotaEmitida
    return [{"id": n.id, "numero_nota": n.numero_nota, "situacao": n.situacao,
             "valor": float(n.valor_bruto or 0),
             "em": n.data_emissao.isoformat() if n.data_emissao else None}
            for n in s.scalars(select(NotaEmitida).where(
                NotaEmitida.titulo_id == titulo_id)
                .order_by(NotaEmitida.id)).all()]


def registrar(s: Session, titulo_id: int, *, numero_nota: str,
              emissao: Optional[date] = None,
              valor_bruto: Optional[Any] = None,
              retencoes: Optional[dict[str, Any]] = None,
              codigo_verificacao: str = "", chave_acesso: str = "",
              observacao: str = "", usuario=None):
    """A nota voltou do portal: registra, com as retenções do cálculo quando a
    pessoa não informou outras.

    Deixar as retenções em branco e o sistema gravar zero seria pior que não
    ter a coluna: o relatório da contabilidade sairia dizendo que nada foi
    retido, o que é uma afirmação, não uma ausência.
    """
    from app.apps.erp.core.notas_emitidas import listagem as svc_listagem

    dados = preparar(s, titulo_id)
    if retencoes is None or not any(_dec(v) for v in retencoes.values()):
        retencoes = dados["retencoes"]

    return svc_listagem.registrar_manual(
        s, empresa_id=dados["destino"].get("empresa_id") or 0,
        titulo_id=titulo_id, numero_nota=numero_nota, emissao=emissao,
        valor_bruto=(_dec(valor_bruto) if valor_bruto is not None
                     else _dec(dados["valor_bruto"])),
        retencoes=retencoes, observacao=observacao, usuario=usuario,
        codigo_verificacao=codigo_verificacao, chave_acesso=chave_acesso)


def ler_nota_emitida(conteudo: bytes, nome_arquivo: str) -> dict[str, Any]:
    """A IA lê o PDF da nota que o portal devolveu e sugere os campos.

    É o MESMO leitor do comprovante e da nota do fornecedor — de propósito:
    caminho de leitura novo é caminho novo para manter, e este já sabe lidar
    com PDF de texto, PDF digitalizado e foto de tela.
    """
    from app.apps.erp.core.documentos import leitor

    d = leitor.ler_documento(conteudo, nome_arquivo,
                             dica_usuario="É uma NOTA FISCAL DE SERVIÇO (NFS-e) "
                                          "emitida PELA nossa empresa contra o "
                                          "cliente. Quero o número da nota, a data "
                                          "de emissão, o valor do serviço e as "
                                          "retenções (ISS, IRRF, INSS, PIS, COFINS, "
                                          "CSLL).")
    sugerido = {c: 0.0 for c in ("iss", "ir", "inss", "pis", "cofins", "csll")}
    for r in (d.get("retencoes") or []):
        chave = DE_PARA.get(str(r.get("tipo", "")).upper())
        if chave:
            sugerido[chave] += float(_dec(r.get("valor")))
    return {
        "numero_nota": d.get("numero_documento", ""),
        "emissao": d.get("data_emissao", ""),
        "valor_bruto": d.get("valor_total", ""),
        "chave_acesso": d.get("chave_acesso", ""),
        "retencoes": sugerido,
        "confianca": d.get("confianca", ""),
        "campos_ilegiveis": d.get("campos_ilegiveis", []),
        "observacoes": d.get("observacoes", ""),
    }
