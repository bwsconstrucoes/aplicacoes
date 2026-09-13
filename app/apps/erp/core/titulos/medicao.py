# ============================================================================
# ERP — core/titulos/medicao.py
# O tipo da medição, a correlação entre elas, e o protocolo.
#
# O PONTO MAIS FÁCIL DE ERRAR DESTE MÓDULO INTEIRO, e o dono avisou antes:
#
#   *"Às vezes o nosso sistema não se adequa a cem por cento, porque teve uma
#    medição 1 alguma coisa e outra medição 1 alguma coisa, por conta de fontes
#    diferentes, e o órgão trata dessa forma. A gente precisa ter um pouco mais
#    de flexibilidade nisso."*
#
# Daí as duas decisões que atravessam este arquivo:
#
#   1. O TIPO É CATÁLOGO EDITÁVEL, não lista no código. Órgão diferente numera
#      diferente, e nenhuma lista fechada sobrevive ao segundo contrato.
#   2. O NÚMERO É TEXTO LIVRE. "1", "1R", "3", "1-FONTE-A" — quem manda na
#      nomenclatura é o órgão.
#
# E a correlação (a medição 1R aponta para a 1) é OPCIONAL de propósito: no
# órgão que numera o reajuste em sequência, o reajuste é a medição 3 e mesmo
# assim aponta para a 1. É essa ligação que permite dizer, no quadro do
# contrato, "a medição 1 rendeu X, mais Y de reajuste" — sem ela os dois
# valores ficam soltos e ninguém soma.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import (MedicaoTipo, Pagamento, Parcela,
                                               Titulo)

logger = logging.getLogger(__name__)

# (código, nome, é reajuste)
#
# Confirmados pelo dono em 09/09/2026 como suficientes — e mesmo assim
# editáveis, porque a próxima prefeitura pode inventar outro.
PADRAO: tuple[tuple[str, str, bool], ...] = (
    ("NORMAL", "Medição normal", False),
    ("REAJUSTE", "Medição de reajuste", True),
    ("ADITIVO", "Medição de aditivo", False),
    ("SUBSIDIARIA", "Medição subsidiária (outra fonte)", False),
    ("COMPLEMENTAR", "Medição complementar", False),
)


def aplicar_tipos(s: Session) -> dict[str, int]:
    """Cria os tipos que faltam. NUNCA apaga — tipo apagado deixaria medição
    órfã, e medição é documento com valor."""
    existentes = {t.codigo for t in s.scalars(select(MedicaoTipo)).all()}
    criados = 0
    for ordem, (codigo, nome, reajuste) in enumerate(PADRAO, 1):
        if codigo in existentes:
            continue
        s.add(MedicaoTipo(codigo=codigo, nome=nome, e_reajuste=reajuste, ordem=ordem))
        criados += 1
    s.flush()
    return {"criados": criados, "total": len(PADRAO)}


def listar_tipos(s: Session) -> list[dict[str, Any]]:
    return [{"codigo": t.codigo, "nome": t.nome, "e_reajuste": t.e_reajuste}
            for t in s.scalars(select(MedicaoTipo).where(MedicaoTipo.ativo.is_(True))
                               .order_by(MedicaoTipo.ordem)).all()]


# ---------------------------------------------------------------------------
# Classificar e correlacionar
# ---------------------------------------------------------------------------
def classificar(s: Session, titulo_id: int, *, tipo: str,
                medicao_de_id: Optional[int] = None,
                usuario: Optional[Usuario] = None) -> Titulo:
    """Diz que tipo de medição é esta, e — se for reajuste — de qual medição."""
    titulo = s.get(Titulo, titulo_id)
    if titulo is None:
        raise ErroValidacao("Título não encontrado.")
    if not titulo.numero_medicao:
        raise ErroValidacao(
            "Este título não é uma medição — não tem número de medição.")

    tipo = (tipo or "").strip().upper()
    t = s.get(MedicaoTipo, tipo)
    if t is None:
        raise ErroValidacao(f"Tipo de medição desconhecido: {tipo}.")

    if medicao_de_id is not None:
        if medicao_de_id == titulo.id:
            raise ErroValidacao("Uma medição não pode ser reajuste dela mesma.")
        origem = s.get(Titulo, medicao_de_id)
        if origem is None:
            raise ErroValidacao("A medição de origem não foi encontrada.")
        if origem.contrato_id != titulo.contrato_id:
            # Reajustar medição de outro contrato somaria valor no quadro
            # errado — e o erro só apareceria na conferência do contrato.
            raise ErroValidacao(
                f"A medição {origem.numero_medicao} é de outro contrato. "
                f"Reajuste só se liga a medição do MESMO contrato.")
        if getattr(origem, "medicao_de_id", None):
            raise ErroValidacao(
                f"A medição {origem.numero_medicao} já é reajuste de outra. "
                f"Reajuste de reajuste não existe — aponte para a medição "
                f"original.")

    titulo.medicao_tipo = tipo
    titulo.medicao_de_id = medicao_de_id
    s.flush()
    registrar_evento(s, "titulo", titulo.id, "MEDICAO_CLASSIFICADA",
                     {"tipo": tipo, "medicao_de_id": medicao_de_id},
                     usuario.id if usuario else None)
    return titulo


def protocolar(s: Session, titulo_id: int, *, numero: str,
               em: Optional[date] = None,
               usuario: Optional[Usuario] = None) -> Titulo:
    """Registra o protocolo no órgão.

    É o que destrava o indicador de tempo de recebimento, que hoje não existe
    em lugar nenhum: quantos dias entre protocolar e o dinheiro entrar.
    """
    titulo = s.get(Titulo, titulo_id)
    if titulo is None:
        raise ErroValidacao("Título não encontrado.")
    numero = (numero or "").strip()
    if not numero:
        raise ErroValidacao("Informe o número do protocolo.")
    quando = em or date.today()
    if quando > date.today():
        raise ErroValidacao("A data do protocolo não pode ser no futuro.")

    titulo.protocolo_numero = numero
    titulo.protocolo_em = quando
    s.flush()
    registrar_evento(s, "titulo", titulo.id, "PROTOCOLADA",
                     {"numero": numero, "em": quando.isoformat()},
                     usuario.id if usuario else None)
    logger.info("ERP/medição: %s protocolada sob nº %s em %s",
                titulo.numero_sp, numero, quando)
    return titulo


# ---------------------------------------------------------------------------
# O indicador que o protocolo destrava
# ---------------------------------------------------------------------------
def _recebimento_de(s: Session, titulo: Titulo) -> Optional[date]:
    """A data do ÚLTIMO recebimento — a medição só está paga quando fecha."""
    datas = [p.data_pagamento for p in s.scalars(
        select(Pagamento).join(Parcela, Pagamento.parcela_id == Parcela.id)
        .where(Parcela.titulo_id == titulo.id)).all() if p.data_pagamento]
    return max(datas) if datas else None


def tempo_de_recebimento(s: Session, *, contrato_id: Optional[int] = None,
                         obra_id: Optional[int] = None) -> dict[str, Any]:
    """Quantos dias entre protocolar e receber. Pedido do dono.

    Só entra no cálculo o que foi protocolado E recebido — medição protocolada
    e ainda não paga não tem prazo, tem espera, e misturar as duas coisas daria
    uma média mentirosa que melhora sozinha quando o cliente atrasa.
    """
    stmt = select(Titulo).where(Titulo.protocolo_em.is_not(None))
    if contrato_id:
        stmt = stmt.where(Titulo.contrato_id == contrato_id)
    titulos = list(s.scalars(stmt).all())
    if obra_id:
        from app.apps.erp.db.models.financeiro import Rateio
        de_obra = {r.titulo_id for r in s.scalars(
            select(Rateio).where(Rateio.obra_id == obra_id)).all()}
        titulos = [t for t in titulos if t.id in de_obra]

    fechados, esperando = [], []
    for t in titulos:
        recebido = _recebimento_de(s, t)
        if recebido is None:
            esperando.append({
                "numero_sp": t.numero_sp, "medicao": t.numero_medicao,
                "protocolo": t.protocolo_numero,
                "protocolado_em": t.protocolo_em.isoformat(),
                "dias_esperando": (date.today() - t.protocolo_em).days,
            })
            continue
        fechados.append({
            "numero_sp": t.numero_sp, "medicao": t.numero_medicao,
            "protocolo": t.protocolo_numero,
            "protocolado_em": t.protocolo_em.isoformat(),
            "recebido_em": recebido.isoformat(),
            "dias": (recebido - t.protocolo_em).days,
        })

    dias = [f["dias"] for f in fechados]
    esperando.sort(key=lambda e: e["dias_esperando"], reverse=True)
    return {
        "recebidas": len(fechados),
        "media_dias": round(sum(dias) / len(dias), 1) if dias else None,
        "menor": min(dias) if dias else None,
        "maior": max(dias) if dias else None,
        "aguardando": len(esperando),
        "mais_antiga": esperando[0] if esperando else None,
        "fechados": sorted(fechados, key=lambda f: f["protocolado_em"], reverse=True),
        "esperando": esperando,
    }


# ---------------------------------------------------------------------------
# A leitura que o quadro do contrato usa
# ---------------------------------------------------------------------------
def ler(s: Session, titulo: Titulo) -> dict[str, Any]:
    tipo = s.get(MedicaoTipo, titulo.medicao_tipo) if titulo.medicao_tipo else None
    origem = s.get(Titulo, titulo.medicao_de_id) if titulo.medicao_de_id else None
    recebido = _recebimento_de(s, titulo)
    return {
        "titulo_id": titulo.id, "numero_sp": titulo.numero_sp,
        "medicao": titulo.numero_medicao,
        "tipo": titulo.medicao_tipo,
        "tipo_nome": tipo.nome if tipo else "",
        "e_reajuste": bool(tipo and tipo.e_reajuste),
        "reajuste_de": ({"titulo_id": origem.id, "medicao": origem.numero_medicao}
                        if origem else None),
        "periodo": {
            "inicio": titulo.periodo_inicio.isoformat() if titulo.periodo_inicio else None,
            "fim": titulo.periodo_fim.isoformat() if titulo.periodo_fim else None,
        },
        "valor": float(titulo.valor_liquido or 0),
        "protocolo": titulo.protocolo_numero,
        "protocolado_em": titulo.protocolo_em.isoformat() if titulo.protocolo_em else None,
        "recebido_em": recebido.isoformat() if recebido else None,
        "dias_ate_receber": ((recebido - titulo.protocolo_em).days
                             if recebido and titulo.protocolo_em else None),
        "status": (titulo.status.value if hasattr(titulo.status, "value")
                   else str(titulo.status)),
    }
