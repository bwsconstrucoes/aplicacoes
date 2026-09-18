# ============================================================================
# ERP — core/suprimentos/cobranca.py
# O QUE PRECISA SER COBRADO: o sistema aponta, a pessoa cobra.
#
# PEDIDO DO DONO, 18/09/2026:
#
#   "A cobrança também acho que não deve ser feita sozinha, mas eu quero que o
#   sistema SUGIRA o que deve ser cobrado. Ó, isso aqui tem que ser cobrado, a
#   gente já disparou cotação, não recebeu resposta desses fornecedores. Por
#   quê? De repente o fornecedor não respondeu pelo e-mail, respondeu pelo
#   WhatsApp. Aí o comprador ainda não alimentou o sistema, e na verdade o
#   fornecedor já respondeu. Então eu acho que não deve ser disparado dessa
#   forma."
#
# A NUANCE ESTÁ NA PRÓPRIA FRASE DELE, e é ela que decide o desenho deste
# módulo: o sistema NÃO SABE se o fornecedor respondeu. Ele sabe uma coisa bem
# menor — que nenhum preço foi lançado naquela coluna. A distância entre as
# duas é o WhatsApp do comprador, a ligação, o vendedor que passou na obra.
#
# Por isso aqui não há "cobrança automática": há uma LISTA DE SUSPEITOS, com o
# que o sistema realmente sabe escrito em cada linha ("disparada há 6 dias,
# nenhum preço lançado") e dois botões que só gente aperta — "cobrar" e "já
# respondeu, foi por fora". O segundo é tão importante quanto o primeiro: é ele
# que impede o sistema de cobrar quem já respondeu, e é ele que alimenta a
# memória que um dia deixa este sistema inteligente (ver `desempenho.py`).
#
# QUANDO UM FORNECEDOR ENTRA NA LISTA
#
#   entra  → a cotação está ABERTA, o e-mail saiu, passou da carência, e não há
#            preço lançado nem marca de resposta;
#   sai    → alguém lançou preço, alguém marcou "respondeu por fora", ou o
#            fornecedor disse que não vai cotar;
#   espera → foi cobrado há menos de um dia. Cobrar duas vezes no mesmo dia não
#            acelera nada e queima o fornecedor bom.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoFornecedor, CotacaoPreco, EnvioEmail, Fornecedor,
    StatusCotacao, Usuario,
)

logger = logging.getLogger(__name__)

# Antes disso não é atraso, é o fornecedor trabalhando. Cobrar no dia seguinte
# ao envio ensina o vendedor a ignorar a nossa cobrança.
CARENCIA_DIAS = 2
# Depois de cobrar, o silêncio volta a valer só no dia seguinte.
ESPERA_ENTRE_COBRANCAS_DIAS = 1

CANAIS = {
    "EMAIL": "por e-mail",
    "WHATSAPP": "por WhatsApp",
    "TELEFONE": "por telefone",
    "PRESENCIAL": "pessoalmente",
}

URGENCIAS = (
    (10, "ATRASADO"),      # dez dias sem responder é proposta perdida
    (5, "ALTA"),
    (3, "MEDIA"),
    (0, "NORMAL"),
)


def _hoje(hoje: Optional[date] = None) -> date:
    return hoje or date.today()


def _urgencia(dias: int) -> str:
    for minimo, nome in URGENCIAS:
        if dias >= minimo:
            return nome
    return "NORMAL"


def _envios_por_coluna(s: Session) -> dict[int, datetime]:
    """Quando saiu o ÚLTIMO e-mail de cotação para cada coluna do mapa.

    Só conta o que o servidor de saída aceitou: um envio que falhou não é
    silêncio do fornecedor, é problema nosso — e cobrar alguém por um e-mail
    que nunca saiu é o tipo de coisa que faz o comprador perder a confiança na
    tela inteira.
    """
    saida: dict[int, datetime] = {}
    for e in s.scalars(select(EnvioEmail)).all():
        if e.destinatario_tipo != "cotacao_fornecedor" or not e.destinatario_id:
            continue
        if (e.situacao or "") != "ENVIADO":
            continue
        quando = e.criado_em
        if quando is None:
            continue
        atual = saida.get(e.destinatario_id)
        if atual is None or quando > atual:
            saida[e.destinatario_id] = quando
    return saida


def _colunas_com_preco(s: Session) -> set[int]:
    return {p.cotacao_fornecedor_id for p in s.scalars(select(CotacaoPreco)).all()}


def _dias(quando: datetime, hoje: date) -> int:
    d = quando.date() if isinstance(quando, datetime) else quando
    return max(0, (hoje - d).days)


def sugerir(s: Session, *, hoje: Optional[date] = None,
            cotacoes_permitidas: Optional[set[int]] = None) -> dict[str, Any]:
    """O que o sistema ACHA que precisa ser cobrado hoje.

    Devolve agrupado por cotação, porque é assim que se cobra: um e-mail por
    fornecedor, mas a cabeça do comprador está numa cotação de cada vez.
    """
    hoje = _hoje(hoje)
    enviados = _envios_por_coluna(s)
    com_preco = _colunas_com_preco(s)

    abertas = {c.id: c for c in s.scalars(select(Cotacao)).all()
               if c.status is StatusCotacao.ABERTA
               and (cotacoes_permitidas is None or c.id in cotacoes_permitidas)}
    fornecedores = {f.id: f for f in s.scalars(select(Fornecedor)).all()}

    blocos: dict[int, dict[str, Any]] = {}
    esperando, ja_responderam, nao_disparadas = 0, 0, 0

    for coluna in s.scalars(select(CotacaoFornecedor)).all():
        cot = abertas.get(coluna.cotacao_id)
        if cot is None:
            continue
        # Já respondeu, de qualquer forma que seja: preço lançado, marca de
        # resposta por fora, ou recusa declarada.
        if (coluna.id in com_preco or coluna.respondido_em is not None
                or getattr(coluna, "sem_interesse", False)):
            ja_responderam += 1
            continue
        saiu = enviados.get(coluna.id)
        if saiu is None:
            # A cotação existe mas o e-mail não saiu — isso não é cobrança, é
            # disparo, e vive na outra aba.
            nao_disparadas += 1
            continue
        dias = _dias(saiu, hoje)
        if dias < CARENCIA_DIAS:
            esperando += 1
            continue
        cobrado = getattr(coluna, "cobrado_em", None)
        if cobrado is not None and _dias(cobrado, hoje) < ESPERA_ENTRE_COBRANCAS_DIAS:
            esperando += 1
            continue

        forn = fornecedores.get(coluna.fornecedor_id)
        vezes = int(getattr(coluna, "cobrancas", 0) or 0)
        bloco = blocos.setdefault(cot.id, {
            "cotacao_id": cot.id, "numero": cot.numero, "titulo": cot.titulo,
            "aberta_ha": _dias(cot.criado_em, hoje) if cot.criado_em else 0,
            "fornecedores": [],
        })
        bloco["fornecedores"].append({
            "coluna_id": coluna.id,
            "fornecedor_id": coluna.fornecedor_id,
            "nome": (getattr(forn, "nome_fantasia", "") or
                     getattr(forn, "razao_social", "") or
                     f"fornecedor {coluna.fornecedor_id}"),
            "dias": dias,
            "urgencia": _urgencia(dias),
            "cobrancas": vezes,
            "porque": _porque(dias, vezes),
        })

    lista = sorted(blocos.values(),
                   key=lambda b: -max(f["dias"] for f in b["fornecedores"]))
    for b in lista:
        b["fornecedores"].sort(key=lambda f: -f["dias"])
        b["urgencia"] = _urgencia(max(f["dias"] for f in b["fornecedores"]))

    return {
        "blocos": lista,
        "a_cobrar": sum(len(b["fornecedores"]) for b in lista),
        "esperando": esperando,
        "ja_responderam": ja_responderam,
        "nao_disparadas": nao_disparadas,
        "observacao": (
            "O sistema sabe que o e-mail saiu e que nenhum preço foi lançado. "
            "Ele NÃO sabe se o fornecedor respondeu — pode ter respondido no "
            "WhatsApp. Antes de cobrar, se foi isso, marque 'já respondeu'."),
    }


def _porque(dias: int, cobrancas: int) -> str:
    base = f"disparada há {dias} dia(s), nenhum preço lançado"
    if cobrancas == 1:
        return base + " · já cobrado 1 vez"
    if cobrancas > 1:
        return base + f" · já cobrado {cobrancas} vezes"
    return base


# ---------------------------------------------------------------------------
# As duas saídas: cobrar, ou dizer que já respondeu
# ---------------------------------------------------------------------------
def marcar_resposta(s: Session, coluna_id: int, *, canal: str,
                    quem: str = "", usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """"Ele já respondeu, foi por fora." Tira da cobrança na hora.

    Não lança preço nenhum: o comprador digita depois, quando tiver tempo. O
    que este registro impede é o sistema cobrar de novo quem já respondeu — e o
    que ele guarda é o tempo de resposta daquele fornecedor, que é o dado de
    que a inteligência do amanhã vai precisar.
    """
    coluna = s.get(CotacaoFornecedor, coluna_id)
    if coluna is None:
        raise ErroNaoEncontrado("Fornecedor não está neste mapa.")
    canal = (canal or "").strip().upper()
    if canal not in CANAIS:
        raise ErroValidacao(
            "Diga por onde a resposta chegou: " +
            ", ".join(sorted(CANAIS)) + ".")
    coluna.respondido_canal = canal
    if coluna.respondido_em is None:
        coluna.respondido_em = datetime.now(timezone.utc)
    if (quem or "").strip():
        coluna.respondido_por = quem.strip()
    coluna.sem_interesse = False
    registrar_evento(s, "cotacao_fornecedor", coluna.id, "RESPOSTA_MARCADA",
                     {"canal": canal, "quem": quem or None},
                     usuario.id if usuario else None)
    return {"recado": f"Marcado como respondido {CANAIS[canal]}. "
                      "Lance os preços quando puder — ele já saiu da cobrança."}


def marcar_sem_interesse(s: Session, coluna_id: int, *, motivo: str = "",
                         usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """"Esse não vai cotar." Sai da lista, e não conta como quem não responde.

    A diferença importa para a memória: um fornecedor que avisa que está sem
    estoque respondeu; contá-lo junto de quem some diria, daqui a um ano, que o
    atencioso é relapso.
    """
    coluna = s.get(CotacaoFornecedor, coluna_id)
    if coluna is None:
        raise ErroNaoEncontrado("Fornecedor não está neste mapa.")
    coluna.sem_interesse = True
    coluna.motivo_sem_interesse = (motivo or "").strip() or None
    if coluna.respondido_em is None:
        coluna.respondido_em = datetime.now(timezone.utc)
    registrar_evento(s, "cotacao_fornecedor", coluna.id, "SEM_INTERESSE",
                     {"motivo": motivo or None},
                     usuario.id if usuario else None)
    return {"recado": "Anotado: este fornecedor não vai cotar desta vez."}


def cobrar(s: Session, colunas_ids: list[int], usuario: Usuario, *,
           observacao: str = "") -> dict[str, Any]:
    """Manda o lembrete para os fornecedores MARCADOS na tela.

    Quem aperta é gente — o módulo só monta. O texto é curto de propósito:
    lembrete longo é lido como cobrança formal, e o vendedor que estava só
    atrasado passa a se defender em vez de responder.
    """
    from app.apps.erp.core.comum import email as correio
    from app.apps.erp.core.suprimentos import envio as svc_envio

    ids = [int(x) for x in (colunas_ids or [])]
    if not ids:
        raise ErroValidacao("Marque ao menos um fornecedor para cobrar.")

    colunas = [c for c in s.scalars(select(CotacaoFornecedor)).all() if c.id in ids]
    if not colunas:
        raise ErroNaoEncontrado("Nenhum destes fornecedores está num mapa.")

    # Uma cobrança pode alcançar fornecedores de cotações diferentes; a empresa
    # remetente é a de cada cotação, não uma só escolhida na tela.
    por_cotacao: dict[int, list[CotacaoFornecedor]] = {}
    for c in colunas:
        por_cotacao.setdefault(c.cotacao_id, []).append(c)

    enviados, falhas, sem_endereco = [], [], []
    for cotacao_id, grupo in por_cotacao.items():
        cot = s.get(Cotacao, cotacao_id)
        if cot is None or cot.status is not StatusCotacao.ABERTA:
            for c in grupo:
                falhas.append({"coluna_id": c.id,
                               "motivo": "a cotação não está mais aberta"})
            continue
        empresa = svc_envio._empresa_do_disparo(s, cotacao_id, None)
        pode, falta = correio.conta_configurada(empresa, s)
        if not pode:
            for c in grupo:
                falhas.append({"coluna_id": c.id, "motivo": falta})
            continue
        for coluna in grupo:
            forn = s.get(Fornecedor, coluna.fornecedor_id)
            nome = (getattr(forn, "nome_fantasia", "") or
                    getattr(forn, "razao_social", "") or "fornecedor")
            destinos = svc_envio.destinos_do_fornecedor(
                s, coluna.fornecedor_id,
                list(getattr(coluna, "contatos_ids", None) or []))
            if not destinos:
                sem_endereco.append(nome)
                continue
            mensagem = _lembrete(cot, nome, observacao)
            registro = correio.enviar(
                s, empresa=empresa, para=destinos,
                assunto=mensagem["assunto"], corpo=mensagem["corpo"],
                entidade_tipo="cotacao", entidade_id=cotacao_id,
                destinatario_tipo="cotacao_fornecedor", destinatario_id=coluna.id,
                usuario=usuario)
            if registro.situacao == "ENVIADO":
                coluna.cobrado_em = datetime.now(timezone.utc)
                coluna.cobrancas = int(getattr(coluna, "cobrancas", 0) or 0) + 1
                enviados.append({"fornecedor": nome, "para": destinos})
            else:
                falhas.append({"coluna_id": coluna.id, "fornecedor": nome,
                               "motivo": registro.erro})
        registrar_evento(s, "cotacao", cotacao_id, "COTACAO_COBRADA",
                         {"fornecedores": len(grupo)},
                         usuario.id if usuario else None)

    partes = [f"{len(enviados)} lembrete(s) enviado(s)"]
    if falhas:
        partes.append(f"{len(falhas)} falharam")
    if sem_endereco:
        partes.append(f"{len(sem_endereco)} sem e-mail cadastrado")
    logger.info("ERP/suprimentos: cobrança de cotação — %s", "; ".join(partes))
    return {"enviados": enviados, "falhas": falhas, "sem_endereco": sem_endereco,
            "recado": ". ".join(partes) + "."}


def _lembrete(cot: Cotacao, nome: str, observacao: str) -> dict[str, str]:
    corpo = [
        f"Prezados {nome},", "",
        f"Retomamos a nossa solicitação de cotação {cot.numero} — {cot.titulo}.",
        "Ainda não recebemos a proposta de vocês e seguimos com o interesse.",
        "",
        "Se já enviaram por outro canal, por favor nos avisem para não "
        "cobrarmos de novo.",
    ]
    if (observacao or "").strip():
        corpo += ["", observacao.strip()]
    corpo += ["", "Obrigado.", "BWS Construções"]
    return {"assunto": f"Lembrete — cotação {cot.numero} ({cot.titulo})",
            "corpo": "\n".join(corpo)}
