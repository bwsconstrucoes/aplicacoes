# ============================================================================
# ERP — core/notas_emitidas/numeracao.py
# O controle da numeração das notas emitidas.
#
# A PERGUNTA DO DONO (09/09/2026): *"você vai conseguir enxergar qual o número
# da nota que tem que ser emitida, para registrar em sistema, e ser tranquilo?
# Eu digo para a gente manter o controle da numeração corretamente."*
#
# SIM — e o motivo é técnico, não otimismo: no padrão nacional e no ABRASF,
# **quem numera a declaração é QUEM EMITE**. A prefeitura recebe a DPS já
# numerada e devolve o número da NOTA. São dois números, e o ERP é dono do
# primeiro.
#
# COMO A RESERVA É FEITA, e por que assim:
#
#   O número é tomado ANTES de emitir, gravado, e só então a emissão acontece.
#   Se falhar, o número NÃO volta para a fila: fica registrado como falhado,
#   com motivo escrito. Número de nota fiscal não se apaga — se explica, porque
#   é isso que o fisco pergunta.
#
#   Reciclar número que falhou seria pior: a prefeitura pode ter recebido a
#   declaração e a resposta é que se perdeu. Emitir de novo com o mesmo número
#   dá duplicidade do lado dela.
#
# A TRAVA DE VERDADE ESTÁ NO BANCO (migração 048): índice único por empresa,
# ambiente, série e número. Duas pessoas emitindo ao mesmo tempo não conseguem
# tomar o mesmo número — o segundo recebe erro e tenta o seguinte.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Empresa, Usuario
from app.apps.erp.db.models.financeiro import NotaEmitida

logger = logging.getLogger(__name__)

SITUACOES = ("RESERVADA", "EMITIDA", "FALHADA", "CANCELADA", "SUBSTITUIDA")
TENTATIVAS = 5           # colisão de número é rara; cinco voltas bastam


def _serie_da(empresa: Empresa) -> str:
    return (empresa.emissao_serie or "1").strip() or "1"


def proximo_numero(s: Session, empresa: Empresa, *, serie: Optional[str] = None,
                   ambiente: Optional[str] = None) -> int:
    """Qual seria o próximo número — SEM tomar.

    É o que a tela mostra antes de emitir, respondendo à pergunta do dono
    ("qual o número da nota que tem que ser emitida?"). Como não reserva, duas
    telas abertas veem o mesmo número; quem toma é `reservar`.
    """
    maior = s.scalar(select(func.max(NotaEmitida.numero_dps)).where(
        NotaEmitida.empresa_id == empresa.id,
        NotaEmitida.ambiente == (ambiente or empresa.emissao_ambiente),
        NotaEmitida.serie == (serie or _serie_da(empresa)))) or 0
    return int(maior) + 1


def reservar(s: Session, empresa: Empresa, *, titulo_id: Optional[int] = None,
             obra_id: Optional[int] = None, competencia: Optional[date] = None,
             modo: Optional[str] = None, valor_bruto=None, valor_liquido=None,
             observacao: str = "", usuario: Optional[Usuario] = None) -> NotaEmitida:
    """Toma o próximo número e grava a linha como RESERVADA.

    Repete quando o banco recusa por colisão: outra pessoa tomou o número entre
    o cálculo e a gravação. Não é erro — é concorrência, e o certo é pegar o
    seguinte.
    """
    serie = _serie_da(empresa)
    ambiente = empresa.emissao_ambiente
    ultimo_erro: Optional[Exception] = None

    for _ in range(TENTATIVAS):
        numero = proximo_numero(s, empresa, serie=serie, ambiente=ambiente)
        ponto = s.begin_nested()
        nota = NotaEmitida(
            empresa_id=empresa.id, ambiente=ambiente, serie=serie,
            numero_dps=numero, titulo_id=titulo_id, obra_id=obra_id,
            competencia=competencia.replace(day=1) if competencia else None,
            modo=(modo or empresa.emissao_modo), situacao="RESERVADA",
            valor_bruto=valor_bruto, valor_liquido=valor_liquido,
            observacao=(observacao or "").strip() or None,
            criado_por=usuario.id if usuario else None)
        s.add(nota)
        try:
            s.flush()
        except IntegrityError as e:
            ponto.rollback()
            ultimo_erro = e
            continue
        registrar_evento(s, "nota_emitida", nota.id, "NUMERO_RESERVADO", {
            "empresa_id": empresa.id, "serie": serie, "numero_dps": numero,
            "ambiente": ambiente, "modo": nota.modo},
            usuario.id if usuario else None)
        logger.info("ERP/nota: reservado nº %s série %s (%s) para a empresa %s",
                    numero, serie, ambiente, empresa.cnpj)
        return nota

    raise ErroValidacao(
        "Não consegui reservar um número de nota — várias emissões ao mesmo "
        f"tempo. Tente de novo em instantes. ({ultimo_erro})")


def confirmar(s: Session, nota_id: int, *, numero_nota: str,
              data_emissao: Optional[date] = None,
              codigo_verificacao: str = "", chave_acesso: str = "",
              retencoes: Optional[dict[str, Any]] = None,
              usuario: Optional[Usuario] = None) -> NotaEmitida:
    """A nota saiu: guarda o número que a PREFEITURA devolveu.

    Vale para os dois modos — na API o número vem na resposta; no manual, a
    pessoa digita o que está no documento. O caminho é o mesmo de propósito:
    assim a tela de controle e os relatórios não precisam saber a diferença.
    """
    nota = s.get(NotaEmitida, nota_id)
    if nota is None:
        raise ErroValidacao("Nota não encontrada.")
    if nota.situacao == "EMITIDA":
        raise ErroValidacao(
            f"Esta nota já está registrada como emitida, com o número "
            f"{nota.numero_nota}. Se a prefeitura devolveu outro número, "
            f"cancele esta e registre a nova.")
    numero = (numero_nota or "").strip()
    if not numero:
        raise ErroValidacao("Informe o número que a prefeitura devolveu.")

    nota.numero_nota = numero
    nota.codigo_verificacao = (codigo_verificacao or "").strip() or None
    nota.chave_acesso = (chave_acesso or "").strip() or None
    nota.data_emissao = data_emissao or date.today()
    nota.retencoes = retencoes or {}
    nota.situacao = "EMITIDA"
    nota.atualizado_em = datetime.now()
    try:
        s.flush()
    except IntegrityError:
        # O índice único pegou: este número já foi registrado antes. É
        # exatamente o erro do modo MANUAL — digitar duas vezes a mesma nota.
        s.rollback()
        raise ErroValidacao(
            f"A nota {numero} já está registrada nesta empresa e série. "
            f"Confira se ela não foi lançada duas vezes.")
    registrar_evento(s, "nota_emitida", nota.id, "EMITIDA", {
        "numero_dps": nota.numero_dps, "numero_nota": numero,
        "modo": nota.modo}, usuario.id if usuario else None)
    return nota


def falhar(s: Session, nota_id: int, *, motivo: str,
           usuario: Optional[Usuario] = None) -> NotaEmitida:
    """A emissão não foi. O número fica queimado, COM motivo.

    Não recicla de propósito: a prefeitura pode ter recebido a declaração e só
    a resposta ter se perdido. Reemitir com o mesmo número daria duplicidade do
    lado dela — e aí o problema deixa de ser nosso e vira dela.
    """
    nota = s.get(NotaEmitida, nota_id)
    if nota is None:
        raise ErroValidacao("Nota não encontrada.")
    motivo = (motivo or "").strip()
    if not motivo:
        raise ErroValidacao(
            "Escreva o que aconteceu. Número de nota queimado sem explicação é "
            "o que o fisco pergunta e ninguém sabe responder.")
    nota.situacao = "FALHADA"
    nota.motivo = motivo
    nota.atualizado_em = datetime.now()
    s.flush()
    registrar_evento(s, "nota_emitida", nota.id, "FALHOU",
                     {"numero_dps": nota.numero_dps, "motivo": motivo},
                     usuario.id if usuario else None)
    return nota


def cancelar(s: Session, nota_id: int, *, motivo: str,
             usuario: Optional[Usuario] = None) -> NotaEmitida:
    nota = s.get(NotaEmitida, nota_id)
    if nota is None:
        raise ErroValidacao("Nota não encontrada.")
    motivo = (motivo or "").strip()
    if not motivo:
        raise ErroValidacao("Cancelar nota exige motivo escrito.")
    nota.situacao = "CANCELADA"
    nota.motivo = motivo
    nota.atualizado_em = datetime.now()
    s.flush()
    registrar_evento(s, "nota_emitida", nota.id, "CANCELADA",
                     {"numero_nota": nota.numero_nota, "motivo": motivo},
                     usuario.id if usuario else None)
    return nota


# ---------------------------------------------------------------------------
# A conferência que o fisco pergunta
# ---------------------------------------------------------------------------
def conferir(s: Session, empresa: Empresa, *, serie: Optional[str] = None,
             ambiente: Optional[str] = None) -> dict[str, Any]:
    """A sequência está inteira? O que falta, e por quê.

    Buraco na numeração é a pergunta clássica da fiscalização. Aqui ele é
    respondido em duas categorias que são MUITO diferentes entre si:

      buracos    números que NUNCA foram reservados. É o sinal de que alguém
                 emitiu fora do ERP — pelo portal da prefeitura, por exemplo.
                 Este é o preocupante.
      queimados  números reservados que não viraram nota, com o motivo
                 registrado. Estes têm resposta pronta.
    """
    serie = serie or _serie_da(empresa)
    ambiente = ambiente or empresa.emissao_ambiente
    linhas = s.scalars(select(NotaEmitida).where(
        NotaEmitida.empresa_id == empresa.id,
        NotaEmitida.ambiente == ambiente,
        NotaEmitida.serie == serie).order_by(NotaEmitida.numero_dps)).all()

    usados = {n.numero_dps for n in linhas}
    maior = max(usados) if usados else 0
    buracos = [n for n in range(1, maior + 1) if n not in usados]
    queimados = [{"numero_dps": n.numero_dps, "situacao": n.situacao,
                  "motivo": n.motivo}
                 for n in linhas if n.situacao in ("FALHADA", "CANCELADA")]

    return {
        "empresa": empresa.nome_fantasia or empresa.razao_social,
        "serie": serie, "ambiente": ambiente,
        "emitidas": sum(1 for n in linhas if n.situacao == "EMITIDA"),
        "reservadas": sum(1 for n in linhas if n.situacao == "RESERVADA"),
        "maior_numero": maior,
        "proximo": maior + 1,
        "buracos": buracos,
        "queimados": queimados,
        "integra": not buracos,
        "aviso": ("" if not buracos else
                  f"Faltam os números {', '.join(str(b) for b in buracos[:20])}"
                  f"{'…' if len(buracos) > 20 else ''} na sequência. Isso "
                  f"costuma significar que alguém emitiu por fora do ERP — "
                  f"pelo portal da prefeitura, por exemplo. Confira lá e "
                  f"registre aqui, senão a numeração não fecha."),
    }
