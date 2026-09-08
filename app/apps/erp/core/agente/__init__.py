# ============================================================================
# ERP — core/agente/
# O agente que vai atrás de quem tem pendência.
#
# O PEDIDO, nas palavras do dono: "vamos pensar numa forma de como a gente
# teria uma espécie de agente fazendo esse acompanhamento, cobrando, mandando
# mensagens… esse agente ele vai servir pro sistema como um todo".
#
# ELE SÓ AVISA. Decisão do dono em 07/09/2026: a resposta é dada NO SISTEMA,
# completa, nunca por mensagem. Este módulo manda texto e um link — não escuta,
# não interpreta resposta, não decide nada por ninguém. As três razões estão
# no HISTORICO: a conferência inteira não cabe num diálogo de WhatsApp; uma
# leitura errada de "acho que dá pra devolver" mexeria no contrato de verdade;
# e número de telefone não prova quem respondeu.
#
# A ESCADA, aprovada pelo dono:
#
#     dia  5  LEMBRETE  — ainda é cortesia, vai só para quem deve responder
#     dia 10  COBRANCA  — passou do prazo que a obra tem para fechar o mês
#     dia 15  ESCALADA  — a lista de quem não respondeu sobe para o dono e
#                         para o financeiro
#
# Os dias contam a partir do VENCIMENTO da pendência, não da criação — quem
# define o vencimento é cada assunto.
#
# POR QUE UM REGISTRO DE ASSUNTOS, e não um código que só sabe de locação: o
# agente nasce cobrando a conferência dos equipamentos, mas foi pedido para
# servir ao sistema todo. Assunto novo é uma função que devolve pendências no
# formato de `Pendencia` e entra em `ASSUNTOS` — sem mexer em nada daqui.
# ============================================================================
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
from app.apps.erp.db.models.financeiro import AgenteMensagem

logger = logging.getLogger(__name__)

# A escada, em dias depois do vencimento da pendência.
DEGRAUS = (("LEMBRETE", 5), ("COBRANCA", 10), ("ESCALADA", 15))

# Quem recebe a escalada do dia 15: quem manda e quem paga.
PERFIS_DA_ESCALADA = (PerfilUsuario.ADMIN, PerfilUsuario.DIRETOR_FINANCEIRO,
                      PerfilUsuario.FINANCEIRO)


@dataclass
class Pendencia:
    """Uma coisa que espera resposta de alguém.

    Todo assunto que o agente cobra devolve isto — e nada além disto. É o
    contrato que permite acrescentar assunto novo sem tocar no agente.
    """
    assunto: str                     # 'locacao_conferencia'
    referencia_id: int               # o número do registro pendente
    responsavel_id: Optional[int]    # quem tem de responder
    resumo: str                      # uma linha, em português, do que falta
    link: str                        # onde se resolve, caminho a partir da raiz
    dias_de_atraso: int              # desde o vencimento; negativo = no prazo
    detalhes: dict[str, Any] = field(default_factory=dict)


# assunto → função que lista o que está pendente
ASSUNTOS: dict[str, Callable[[Session], list[Pendencia]]] = {}


def registrar_assunto(nome: str, listar: Callable[[Session], list[Pendencia]]) -> None:
    ASSUNTOS[nome] = listar


def degrau_de(dias_de_atraso: int) -> Optional[str]:
    """Em que degrau da escada esta pendência está hoje.

    Devolve o degrau MAIS ALTO já alcançado — e não um por dia. Quem está com
    20 dias de atraso está em ESCALADA, não em LEMBRETE de novo: cada degrau
    só é mandado uma vez, e a trava disso é a chave única do banco.
    """
    alcancado = None
    for nome, dias in DEGRAUS:
        if dias_de_atraso >= dias:
            alcancado = nome
    return alcancado


def _ja_mandou(s: Session, p: Pendencia, destinatario_id: Optional[int],
               degrau: str) -> bool:
    achado = s.scalars(select(AgenteMensagem).where(
        AgenteMensagem.assunto == p.assunto,
        AgenteMensagem.referencia_id == p.referencia_id,
        AgenteMensagem.destinatario_id == destinatario_id,
        AgenteMensagem.degrau == degrau)).first()
    return achado is not None


def _endereco_base() -> str:
    import os
    return os.getenv("ERP_URL_PUBLICA", "").rstrip("/")


def montar_texto(p: Pendencia, degrau: str, para_quem: str) -> str:
    """A mensagem que a pessoa recebe. Curta, com o que falta e o link.

    Três cuidados de propósito:
      - o link vem por último, porque é o que ela vai tocar;
      - o degrau muda o tom, não o conteúdo — quem já foi cobrado não precisa
        reler a explicação inteira;
      - nunca vai valor de contrato nem dado bancário: mensagem de WhatsApp é
        lida em ônibus, em obra, e por quem pega o telefone emprestado.
    """
    base = _endereco_base()
    link = f"{base}{p.link}" if base else p.link
    if degrau == "LEMBRETE":
        cabeca = f"Oi, {para_quem.split()[0] if para_quem else 'tudo bem'}!"
        corpo = f"{p.resumo}\n\nQuando puder, responde aqui:"
    elif degrau == "COBRANCA":
        cabeca = f"{para_quem.split()[0] if para_quem else 'Olá'}, ainda falta você."
        corpo = (f"{p.resumo}\n\nJá são {p.dias_de_atraso} dia(s) depois do "
                 f"prazo. Leva menos de cinco minutos:")
    else:
        cabeca = "Pendência sem resposta."
        corpo = (f"{p.resumo}\n\nCom {p.dias_de_atraso} dia(s) de atraso e sem "
                 f"resposta de {para_quem or 'ninguém designado'}.")
    return f"{cabeca}\n\n{corpo}\n{link}\n\n— ERP BWS"


def _quem_recebe_a_escalada(s: Session) -> list[Usuario]:
    return [u for u in s.scalars(select(Usuario)).all()
            if u.ativo and u.perfil in PERFIS_DA_ESCALADA and (u.telefone or "").strip()]


def _enviar(texto: str, telefone: str) -> dict[str, Any]:
    """Manda pelos canais que o monorepo já tem. Falha NUNCA derruba a varredura:
    uma pessoa sem telefone não pode impedir as outras de serem avisadas."""
    try:
        from app.apps.notificador import notificar
        ack = notificar(telefone=telefone, mensagem=texto)
        return {"ok": any(bool(r.get("ok")) for r in ack.values()), "canais": ack}
    except Exception as e:                                  # pragma: no cover
        logger.warning("ERP/agente: envio falhou para %s: %s", telefone, e)
        return {"ok": False, "canais": {}, "erro": str(e)}


def varrer(s: Session, *, simular: bool = False,
           hoje: Optional[date] = None) -> dict[str, Any]:
    """Percorre todos os assuntos e manda o que tiver de ser mandado.

    `simular=True` monta tudo e NÃO envia nem grava — é como se olha o que o
    agente faria hoje antes de deixar ele solto. Foi assim que isto foi
    conferido antes de ir para produção.

    Roda todo dia. Rodar duas vezes no mesmo dia não manda nada duas vezes: a
    chave única do banco recusa, e o código pergunta antes.
    """
    enviadas, puladas, falhas = [], [], []

    for assunto, listar in ASSUNTOS.items():
        try:
            pendencias = listar(s)
        except Exception as e:                              # pragma: no cover
            logger.exception("ERP/agente: assunto %s falhou ao listar", assunto)
            falhas.append({"assunto": assunto, "erro": str(e)})
            continue

        for p in pendencias:
            degrau = degrau_de(p.dias_de_atraso)
            if degrau is None:
                puladas.append({"referencia": p.referencia_id,
                                "motivo": "ainda no prazo"})
                continue

            if degrau == "ESCALADA":
                destinos = [(u.id, u.nome, u.telefone)
                            for u in _quem_recebe_a_escalada(s)]
                nome_do_responsavel = _nome(s, p.responsavel_id)
            else:
                pessoa = s.get(Usuario, p.responsavel_id) if p.responsavel_id else None
                if pessoa is None or not (pessoa.telefone or "").strip():
                    puladas.append({"referencia": p.referencia_id,
                                    "motivo": "responsável sem telefone no cadastro"})
                    continue
                destinos = [(pessoa.id, pessoa.nome, pessoa.telefone)]
                nome_do_responsavel = pessoa.nome

            for destinatario_id, nome, telefone in destinos:
                if _ja_mandou(s, p, destinatario_id, degrau):
                    puladas.append({"referencia": p.referencia_id,
                                    "motivo": f"{degrau} já enviado a {nome}"})
                    continue
                texto = montar_texto(
                    p, degrau, nome if degrau != "ESCALADA" else nome_do_responsavel)
                if simular:
                    enviadas.append({"assunto": p.assunto, "referencia": p.referencia_id,
                                     "degrau": degrau, "para": nome,
                                     "telefone": telefone, "texto": texto,
                                     "simulado": True})
                    continue
                r = _enviar(texto, telefone)
                s.add(AgenteMensagem(
                    assunto=p.assunto, referencia_id=p.referencia_id,
                    destinatario_id=destinatario_id, telefone=telefone,
                    degrau=degrau, texto=texto, canais=r.get("canais") or {},
                    entregue=bool(r.get("ok")), erro=r.get("erro")))
                s.flush()
                (enviadas if r.get("ok") else falhas).append(
                    {"assunto": p.assunto, "referencia": p.referencia_id,
                     "degrau": degrau, "para": nome, "erro": r.get("erro")})

    logger.info("ERP/agente: %d enviada(s), %d pulada(s), %d falha(s)%s",
                len(enviadas), len(puladas), len(falhas),
                " (SIMULAÇÃO)" if simular else "")
    return {"enviadas": enviadas, "puladas": puladas, "falhas": falhas,
            "simulado": simular}


def _nome(s: Session, usuario_id: Optional[int]) -> str:
    if not usuario_id:
        return ""
    u = s.get(Usuario, usuario_id)
    return u.nome if u else ""


def historico(s: Session, *, assunto: Optional[str] = None,
              limite: int = 200) -> list[dict[str, Any]]:
    """O que o agente já falou. É a resposta para 'o Ruan foi cobrado?'."""
    stmt = select(AgenteMensagem).order_by(AgenteMensagem.criado_em.desc())
    if assunto:
        stmt = stmt.where(AgenteMensagem.assunto == assunto)
    saida = []
    for m in s.scalars(stmt).all()[:limite]:
        saida.append({
            "id": m.id, "assunto": m.assunto, "referencia": m.referencia_id,
            "para": _nome(s, m.destinatario_id), "telefone": m.telefone,
            "degrau": m.degrau, "entregue": m.entregue, "erro": m.erro,
            "quando": m.criado_em.isoformat() if m.criado_em else None,
            "texto": m.texto,
        })
    return saida


# Os assuntos que o agente conhece. Importado no fim de propósito: o registro
# acontece ao carregar o pacote, e assim quem importa `agente` já tem tudo.
from app.apps.erp.core.agente import locacao          # noqa: E402,F401
