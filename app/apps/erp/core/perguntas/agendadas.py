# ============================================================================
# ERP — core/perguntas/agendadas.py
# A pergunta que chega sozinha.
#
# O PEDIDO, do dono, com as palavras dele: *"toda segunda-feira me manda
# determinado tipo de informação. Aí a própria [IA] agendar essa necessidade
# minha e fazer aquela ação executar e me mandar. Isso é muito poderoso."*
#
# SEIS REGRAS, e cada uma existe porque o contrário estraga o relatório:
#
#  1. GUARDA A CONSULTA, NÃO A FRASE. Reinterpretar o texto toda segunda faria
#     o critério mudar sozinho, e comparar uma segunda com a outra — que é
#     para o que ele serve — deixaria de fazer sentido.
#  2. RODA COM A PERMISSÃO DE QUEM RECEBE. Não com a de quem criou. Senão
#     agendar algo para o gestor de uma obra manda a ele o número da empresa
#     inteira, sem ninguém querer.
#  3. COMPARA COM A RODADA ANTERIOR. "R$ 340 mil a pagar (era R$ 280 mil)" é
#     gestão; "R$ 340 mil" sozinho é ruído.
#  4. SÓ MANDA QUANDO HÁ O QUE MANDAR, se assim for pedido. Relatório que
#     chega igual todo mês vira spam e para de ser lido — e aí o dia em que
#     ele traz algo importante também não é lido.
#  5. RELATÓRIO QUEBRADO RECLAMA. Se a obra acabou ou a conta foi aposentada,
#     ele não pode mandar zero em silêncio: zero calado é pior que erro,
#     porque parece resposta.
#  6. SÓ LÊ. Nada aqui lança, aprova ou paga. Agendado que AGE sem ninguém
#     olhando é o que faz empresa desligar assistente — para agir, o caminho
#     continua sendo preparar e esperar a pessoa apertar.
#
# O RELÓGIO NÃO É DAQUI. Quem chama é a rotina diária que já existe (o agente,
# em `/erp/api/agente/rodar`). Um segundo relógio seria uma segunda coisa para
# quebrar, e duas coisas para lembrar de configurar.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.core.comum.formato import _dinheiro_br
from app.apps.erp.db.models.cadastros import Usuario
from app.apps.erp.db.models.financeiro import PerguntaAgendada

logger = logging.getLogger(__name__)

FREQUENCIAS = {
    "DIARIA": "todo dia",
    "SEMANAL": "toda semana",
    "MENSAL": "todo mês",
}
DIAS_DA_SEMANA = ("segunda", "terça", "quarta", "quinta", "sexta",
                  "sábado", "domingo")
# O banco aceita os dois desde a migração 060, mas só o Telegram está LIGADO.
#
# Por quê: o e-mail do ERP sai pela conta de uma EMPRESA (as credenciais de
# envio são por CNPJ), e a BWS tem mais de uma. Escolher uma por conta própria
# faria o relatório do dono chegar pelo remetente errado — e isso é decisão
# dele, não minha. Fica pronto para ligar no dia em que ele disser qual.
CANAIS = ("TELEGRAM", "EMAIL")
CANAIS_LIGADOS = ("TELEGRAM",)

# Um teto por pessoa. Não é economia: é que quinze relatórios chegando toda
# segunda não são lidos, e aí nenhum é.
TETO_POR_PESSOA = 12


# ---------------------------------------------------------------------------
# Combinar
# ---------------------------------------------------------------------------
def quando_por_extenso(frequencia: str, dia: Optional[int]) -> str:
    """"toda segunda-feira", "todo dia 5" — como a pessoa diria."""
    if frequencia == "SEMANAL":
        nome = DIAS_DA_SEMANA[dia % 7] if dia is not None else "segunda"
        return f"toda {nome}-feira" if dia is None or dia < 5 else f"todo {nome}"
    if frequencia == "MENSAL":
        return f"todo dia {dia or 1}"
    return "todo dia"


def agendar(s: Session, usuario: Usuario, *, chave: str, titulo: str,
            parametros: Optional[dict[str, Any]] = None,
            frequencia: str = "SEMANAL", dia: Optional[int] = 0,
            canal: str = "TELEGRAM", so_se_houver: bool = False,
            para_usuario_id: Optional[int] = None) -> PerguntaAgendada:
    """Combina o envio. Devolve o agendamento criado."""
    from app.apps.erp.core.perguntas import catalogo

    frequencia = (frequencia or "SEMANAL").upper()
    if frequencia not in FREQUENCIAS:
        raise ErroValidacao(f"Não sei mandar {frequencia.lower()}.")
    canal = (canal or "TELEGRAM").upper()
    if canal not in CANAIS:
        raise ErroValidacao(f"Não sei mandar por {canal.lower()}.")
    if canal not in CANAIS_LIGADOS:
        raise ErroValidacao(
            "Por enquanto só mando por Telegram. O e-mail do ERP sai pela "
            "conta de uma empresa, e falta decidir por qual delas estes "
            "relatórios devem sair.")
    if chave not in catalogo.POR_CHAVE:
        raise ErroValidacao("Essa pergunta não existe.")

    if frequencia == "MENSAL":
        dia = max(1, min(int(dia or 1), 28))   # 28 para caber em fevereiro
    elif frequencia == "SEMANAL":
        dia = max(0, min(int(dia or 0), 6))
    else:
        dia = None

    # MANDAR PARA OUTRA PESSOA É OUTRA COISA, e exige poder mexer em gente.
    # Sem isso, qualquer um agendaria um relatório no nome do diretor — que
    # rodaria com a permissão DELE.
    destino_id = usuario.id
    if para_usuario_id and int(para_usuario_id) != usuario.id:
        from app.apps.erp.core.auth.permissoes import pode
        if not pode(usuario, "gerir_usuarios"):
            raise ErroValidacao(
                "Você pode agendar relatórios para você. Para mandar a outra "
                "pessoa, fale com quem cuida dos acessos — o relatório roda "
                "com a permissão de quem recebe.")
        destino_id = int(para_usuario_id)

    quantas = len(s.scalars(select(PerguntaAgendada).where(
        PerguntaAgendada.usuario_id == destino_id,
        PerguntaAgendada.ativa.is_(True))).all())
    if quantas >= TETO_POR_PESSOA:
        raise ErroValidacao(
            f"Já são {quantas} relatórios automáticos para essa pessoa. "
            f"Desligue algum antes — mais que isso ninguém lê.")

    # JÁ COMBINADO É UMA RESPOSTA, NÃO UM ERRO.
    #
    # Dois cliques seguidos no botão chegam juntos, e a pessoa que clica de
    # novo quer saber que já está combinado — não ler "duplicate key value
    # violates unique constraint". A conferência aqui resolve o caso comum; a
    # trava do banco resolve a corrida, e a mensagem é a mesma nos dois.
    repetida = ErroValidacao(
        f"Esse relatório já está combinado — {quando_por_extenso(frequencia, dia)}. "
        f"Para trocar o dia, desligue o antigo primeiro.")
    ja = s.scalars(select(PerguntaAgendada).where(
        PerguntaAgendada.usuario_id == destino_id,
        PerguntaAgendada.chave == chave,
        PerguntaAgendada.ativa.is_(True))).all()
    if any((a.parametros or {}) == dict(parametros or {}) for a in ja):
        raise repetida

    agendada = PerguntaAgendada(
        usuario_id=destino_id, criado_por=usuario.id, chave=chave,
        parametros=dict(parametros or {}), titulo=(titulo or chave)[:200],
        frequencia=frequencia, dia=dia, canal=canal,
        so_se_houver=bool(so_se_houver))
    s.add(agendada)
    try:
        s.flush()
    except IntegrityError:
        # A corrida: o outro clique chegou primeiro. Para quem está olhando,
        # é a mesma coisa — já está combinado.
        s.rollback()
        raise repetida
    registrar_evento(s, "pergunta_agendada", agendada.id, "CRIADA",
                     {"chave": chave, "quando": quando_por_extenso(frequencia, dia),
                      "para": destino_id, "canal": canal}, usuario.id)
    return agendada


def listar(s: Session, usuario: Usuario) -> list[dict[str, Any]]:
    """Os relatórios automáticos desta pessoa."""
    linhas = s.scalars(select(PerguntaAgendada).where(
        PerguntaAgendada.usuario_id == usuario.id
    ).order_by(PerguntaAgendada.id)).all()
    return [{
        "id": a.id, "titulo": a.titulo, "chave": a.chave,
        "parametros": a.parametros or {},
        "quando": quando_por_extenso(a.frequencia, a.dia),
        "canal": a.canal, "ativa": a.ativa,
        "so_se_houver": a.so_se_houver,
        "ultima_rodada": a.ultima_rodada.isoformat() if a.ultima_rodada else None,
        "ultimo_erro": a.ultimo_erro or "",
    } for a in linhas]


def desligar(s: Session, usuario: Usuario, agendada_id: int) -> None:
    # A MESMA trava que a rota chama. Escrever a regra de novo aqui faria duas
    # versões do mesmo recorte, que divergem no dia em que alguém corrige uma.
    from app.apps.erp.core.auth.permissoes import exigir_agendada_no_escopo

    exigir_agendada_no_escopo(s, usuario, agendada_id)
    a = s.get(PerguntaAgendada, agendada_id)
    a.ativa = False
    # O `flush` não é enfeite: a sessão do ERP não descarrega sozinha
    # (`autoflush=False`), e sem isto quem desligasse um relatório e tentasse
    # combiná-lo de novo na mesma tela ouviria "já está combinado" — porque a
    # consulta ainda enxergaria a linha ligada.
    s.flush()
    registrar_evento(s, "pergunta_agendada", a.id, "DESLIGADA",
                     {"titulo": a.titulo}, usuario.id)


# ---------------------------------------------------------------------------
# Rodar
# ---------------------------------------------------------------------------
def vence_hoje(a: PerguntaAgendada, hoje: date) -> bool:
    """Hoje é dia deste relatório?

    Roda uma vez por dia: se já rodou hoje, não roda de novo. É o que faz a
    rotina diária poder ser chamada duas vezes sem duplicar o envio.
    """
    if not a.ativa:
        return False
    if a.ultima_rodada == hoje:
        return False
    if a.frequencia == "DIARIA":
        return True
    if a.frequencia == "SEMANAL":
        return hoje.weekday() == (a.dia if a.dia is not None else 0)
    return hoje.day == (a.dia or 1)


def _texto(a: PerguntaAgendada, resposta: dict[str, Any]) -> str:
    """A mensagem que chega. Curta: ela é lida no celular, de relance."""
    linhas = [f"📊 {a.titulo}", "", resposta.get("frase") or ""]

    # O "(era …)" — a comparação que transforma número em gestão.
    total = resposta.get("total")
    if total is not None and a.ultimo_total is not None:
        anterior = Decimal(str(a.ultimo_total))
        agora = Decimal(str(total))
        if anterior != agora:
            diferenca = agora - anterior
            seta = "↑" if diferenca > 0 else "↓"
            linhas.append(
                f"{seta} era R$ {_dinheiro_br(anterior)} na última vez "
                f"({'+' if diferenca > 0 else ''}R$ {_dinheiro_br(diferenca)}).")
        else:
            linhas.append("→ igual à última vez.")
    elif resposta.get("quantas") is not None and a.ultimas_linhas is not None:
        if resposta["quantas"] != a.ultimas_linhas:
            linhas.append(f"(eram {a.ultimas_linhas} na última vez)")

    de_onde = resposta.get("de_onde_veio") or {}
    if de_onde.get("explicacao"):
        linhas += ["", f"Confira em: {de_onde['explicacao']}"]
    linhas += ["", f"— ERP BWS, {quando_por_extenso(a.frequencia, a.dia)}. "
                   f"Para desligar, fale comigo em Perguntar."]
    return "\n".join(x for x in linhas if x is not None)


def _mandar(s: Session, a: PerguntaAgendada, dono: Usuario, texto: str) -> bool:
    """Entrega pelo canal combinado. Devolve se saiu."""
    if a.canal == "TELEGRAM":
        try:
            from app.apps.notificador import enviar_telegram
        except Exception as e:                    # pragma: no cover - ambiente
            logger.warning("ERP/agendadas: notificador indisponível (%s)", e)
            return False
        if not (dono.telefone or dono.cpf):
            return False
        r = enviar_telegram(telefone=dono.telefone, cpf=dono.cpf, mensagem=texto)
        return bool(r and r.get("ok"))

    # E-mail: ver CANAIS_LIGADOS. Chegar aqui quer dizer que alguém ligou o
    # canal sem escolher a empresa remetente — melhor não mandar do que mandar
    # pelo CNPJ errado.
    logger.warning("ERP/agendadas: canal %s ainda não está ligado", a.canal)
    return False


def rodar_do_dia(s: Session, *, hoje: Optional[date] = None,
                 simular: bool = False) -> dict[str, Any]:
    """Responde e manda tudo o que vence hoje.

    `simular=True` monta tudo e não envia nem grava — é como se confere o que
    vai sair antes de deixar solto.
    """
    from app.apps.erp.core.perguntas import catalogo

    hoje = hoje or date.today()
    enviados, pulados, falhas = [], [], []

    for a in s.scalars(select(PerguntaAgendada).where(
            PerguntaAgendada.ativa.is_(True))).all():
        if not vence_hoje(a, hoje):
            continue

        dono = s.get(Usuario, a.usuario_id)
        if dono is None or not dono.ativo:
            pulados.append({"id": a.id, "motivo": "quem recebia saiu do ERP"})
            continue

        try:
            # A PERMISSÃO É A DE QUEM RECEBE. É esta linha que impede o
            # relatório de virar um furo no escopo.
            resposta = catalogo.responder(a.chave, s, dono, a.parametros or {})
        except Exception as e:
            # RELATÓRIO QUEBRADO RECLAMA. Zero calado parece resposta.
            logger.exception("ERP/agendadas: %s falhou", a.chave)
            falhas.append({"id": a.id, "titulo": a.titulo, "erro": str(e)})
            if not simular:
                a.ultimo_erro = str(e)[:400]
                a.ultima_rodada = hoje
                _mandar(s, a, dono,
                        f"⚠️ {a.titulo}\n\nNão consegui montar este relatório "
                        f"hoje. Ele continua agendado, e eu tento de novo na "
                        f"próxima.\n\nMotivo: {str(e)[:200]}")
            continue

        vazio = not (resposta.get("linhas") or [])
        if a.so_se_houver and vazio:
            pulados.append({"id": a.id, "motivo": "nada a relatar hoje"})
            if not simular:
                a.ultima_rodada = hoje
                a.ultimo_erro = None
            continue

        texto = _texto(a, resposta)
        if simular:
            enviados.append({"id": a.id, "titulo": a.titulo, "texto": texto})
            continue

        try:
            saiu = _mandar(s, a, dono, texto)
        except Exception as e:
            logger.exception("ERP/agendadas: envio de %s falhou", a.id)
            falhas.append({"id": a.id, "titulo": a.titulo, "erro": str(e)})
            continue

        a.ultima_rodada = hoje
        a.ultimo_erro = None if saiu else "não há por onde mandar (sem telefone/e-mail)"
        if resposta.get("total") is not None:
            a.ultimo_total = Decimal(str(resposta["total"]))
        a.ultimas_linhas = resposta.get("quantas")
        registrar_evento(s, "pergunta_agendada", a.id,
                         "ENVIADA" if saiu else "SEM_DESTINO",
                         {"titulo": a.titulo, "canal": a.canal}, None)
        (enviados if saiu else pulados).append(
            {"id": a.id, "titulo": a.titulo,
             **({} if saiu else {"motivo": "sem telefone nem e-mail no cadastro"})})

    return {"enviados": enviados, "pulados": pulados, "falhas": falhas,
            "dia": hoje.isoformat()}
