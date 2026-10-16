# -*- coding: utf-8 -*-
"""
A BATIDA SEM INTERNET NO PONTO DA OBRA.

Pedido do dono, 16/10/2026: *"imagina que a gente está num local que não tem
internet. E aí, como é que a gente pode resolver?"* Decisões dele, no mesmo dia:
reconhecer por CPF e por QR pela LISTA que o aparelho baixa antes ("pode
reconhecer por lista"), e só no ponto da obra ("pode ser em quem bate ponto da
obra").

COMO FUNCIONA:
  · Com internet, o ponto da obra baixa a LISTA das pessoas que costumam bater
    nele: o nome, a impressão digital do CPF e a do QR do WhatsApp (o banco já
    guarda o QR assim — a lista não permite montar QR nenhum).
  · Sem internet, o aparelho reconhece a pessoa pela lista, tira a foto e
    GUARDA a batida no próprio navegador, com a hora, a localização e um número.
    Quem não está na lista bate do mesmo jeito (o ponto não pode impedir a
    marcação, Portaria 671) e a batida vai para conferência.
  · A internet volta e o aparelho manda cada batida para `registrar`. A hora da
    batida é a de quando a pessoa bateu; a de chegada fica em `recebida_em`; o
    NSR é dado na chegada (a ordem do registro).

A HORA: o relógio do aparelho se muda na mão, então ele não decide sozinho. A
cada resposta do servidor, o aparelho anota a hora certa e um contador interno
que ninguém ajusta; sem internet, a hora é a do servidor mais o tempo contado.
Se o contador e o relógio discordam (o relógio foi mexido, ou o aparelho dormiu
e o contador parou), ou se a página recomeçou e o contador se perdeu, a hora não
está COMPROVADA — e a batida vai para conferência, com as duas horas escritas.

O RISCO ACEITO PELO DONO: a impressão digital do CPF protege de olhar curioso,
não de um técnico com o aparelho na mão (CPF tem poucas combinações). Por isso a
lista leva só as pessoas DAQUELE aparelho, não a empresa inteira.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoConfigurado
from . import cadastros, dispositivos, marcacoes, qr

logger = logging.getLogger("ponto.sem_internet")

PREFIXO_HASH_CPF = "bwsp-cpf|"      # o aparelho calcula igual (app.html, `impressaoDoCpf`)
DIAS_DE_QUEM_BATEU = 60             # quem bateu no aparelho nesses dias entra na lista
FOLGA_FUTURO_S = 120                # hora "do futuro" além disso não é comprovada
CHEGADA_TARDIA_H = 24               # chegou depois disso: conferência, mesmo comprovada
FOLGA_RECONEXAO_MIN = 3             # a fila sobe em até 30 s da volta da rede; folga para isso
IDENT_NAO_RECONHECIDO = "CPF_NAO_RECONHECIDO"


def pronto(conn: Connection) -> bool:
    """A migração 012 já rodou? Sem ela não há como guardar sem duplicar."""
    return db.tem_coluna(conn, "marcacoes", "sem_internet")


def hash_cpf(cpf: str) -> str:
    return hashlib.sha256((PREFIXO_HASH_CPF + cadastros.normalizar_cpf(cpf)).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# A lista que o aparelho guarda
# ---------------------------------------------------------------------------
def _ids_do_aparelho(conn: Connection, aparelho: dict) -> set[int]:
    """Quem costuma bater neste aparelho: no de equipe, a lista dele; no da obra,
    quem tem a obra dele no cadastro, mais quem bateu nele nos últimos dias (a
    pessoa de outra obra que vem sempre). Aparelho sem obra não lista a empresa
    inteira — só quem já bateu nele."""
    ids: set[int] = set()
    if aparelho.get("perfil") == "LISTA":
        ids |= set(dispositivos.autorizados_de(conn, aparelho["id"]))
    else:
        for obra_id in dispositivos.obras_de(conn, aparelho["id"]):
            ids |= {int(c["id"]) for c in cadastros.listar_colaboradores(conn, obra_id=int(obra_id))}
    ids |= {int(l["colaborador_id"]) for l in db.todos(conn, """
        SELECT DISTINCT colaborador_id FROM ponto.marcacoes
         WHERE dispositivo_id = :d AND timestamp_servidor > now() - make_interval(days => :n)
    """, d=aparelho["id"], n=DIAS_DE_QUEM_BATEU)}
    return ids


def lista_do_aparelho(conn: Connection, aparelho: dict) -> dict:
    hoje = horario.hoje()
    pessoas = []
    for cid in sorted(_ids_do_aparelho(conn, aparelho)):
        p = cadastros.colaborador_por_id(conn, cid)
        # Só quem bateria agora com internet: desligado, afastado, inativo no ponto
        # ou fora do período de contrato não entra (se digitar o CPF, vai para
        # conferência e o servidor decide na chegada).
        if (not p or p["situacao"] in ("DESLIGADO", "AFASTADO") or not p["ativo_no_ponto"]
                or marcacoes.fora_do_contrato(p, hoje)):
            continue
        partes = p["nome"].split()
        pessoas.append({"id": int(p["id"]), "nome": p["nome"],
                        "primeiro_nome": partes[0].title() if partes else "",
                        "funcao": p.get("funcao"), "c": hash_cpf(p["cpf"]), "q": []})
    if pessoas and db.tem_003(conn):
        por_id = {p["id"]: p for p in pessoas}
        for l in db.todos(conn, """
            SELECT colaborador_id, token_hash FROM ponto.qr_codigos
             WHERE colaborador_id = ANY(:ids) AND revogado_em IS NULL
               AND (valido_ate IS NULL OR valido_ate > now())
        """, ids=list(por_id)):
            por_id[int(l["colaborador_id"])]["q"].append(l["token_hash"])
    return {"agora": horario.agora().astimezone(horario.UTC).isoformat(timespec="milliseconds"),
            "pessoas": pessoas}


# ---------------------------------------------------------------------------
# A batida que chega depois
# ---------------------------------------------------------------------------
def _hora(momento: dt.datetime) -> str:
    return f"{horario.para_local(momento):%d/%m às %H:%M}"


def _ler_momento(valor, campo: str) -> Optional[dt.datetime]:
    try:
        return horario.ler_iso(valor)
    except ValueError as e:
        raise ErroDeValidacao(f"{campo} ilegível (use ISO 8601)", campo=campo) from e


def decidir_hora(*, momento: Optional[dt.datetime], comprovada: bool, sincronizado_em: Optional[dt.datetime],
                 recebida_em: dt.datetime) -> tuple[dt.datetime, Optional[str]]:
    """PURA. A hora que vale e, se ela não está comprovada, o porquê (vira motivo
    de conferência). A hora nunca fica no futuro de quando chegou."""
    if momento is None:
        return recebida_em, "o aparelho não informou a hora da batida — ficou a hora de chegada"
    if momento > recebida_em + dt.timedelta(seconds=FOLGA_FUTURO_S):
        return recebida_em, (f"a hora informada pelo aparelho ({_hora(momento)}) estava no futuro "
                             "— ficou a hora de chegada")
    momento = min(momento, recebida_em)
    if sincronizado_em is not None and momento < sincronizado_em - dt.timedelta(seconds=FOLGA_FUTURO_S):
        return momento, "a hora informada é anterior à última conexão do aparelho — o relógio dele foi mexido?"
    if not comprovada:
        return momento, ("a hora não pôde ser comprovada (o aparelho reiniciou, ficou desligado ou o relógio "
                         "dele foi mexido) — confira")
    return momento, None


def _chegada_suspeita(conn: Connection, dispositivo_id: int, momento: dt.datetime,
                      recebida: dt.datetime) -> Optional[str]:
    """A hora comprovada pelo aparelho ainda pode ser forjada por quem mexe nele
    (a revisão de 16/10/2026: "ontem às 7h" mandado hoje). Duas travas: se o
    aparelho bateu COM internet depois da hora desta batida, ela devia ter subido
    ali; e a que chega um dia depois vai para conferência de qualquer jeito."""
    depois = db.um(conn, """
        SELECT min(timestamp_servidor) AS t FROM ponto.marcacoes
         WHERE dispositivo_id = :d AND NOT sem_internet AND timestamp_servidor > :de AND timestamp_servidor < :ate
    """, d=dispositivo_id, de=momento + dt.timedelta(minutes=2),
        ate=recebida - dt.timedelta(minutes=FOLGA_RECONEXAO_MIN))
    if depois and depois["t"]:
        return (f"o aparelho bateu com internet em {_hora(depois['t'])}, depois da hora desta batida — "
                "ela devia ter subido ali; confira")
    horas = (recebida - momento).total_seconds() / 3600
    if horas > CHEGADA_TARDIA_H:
        return f"chegou {int(horas // 24)} dia(s) e {int(horas % 24)} h depois da batida — confira"
    return None


def registrar(conn: Connection, aparelho: dict, d: dict, *, device_uuid: str, device_token: str,
              ip: Optional[str] = None) -> tuple[dict, bool]:
    """Registra uma batida guardada sem internet. Devolve (marcação, já_existia)."""
    if not pronto(conn):
        raise NaoConfigurado("o ponto ainda não aceita batida guardada sem internet — "
                             "aplique as atualizações do ponto (migração 012)")
    id_local = str(d.get("id") or "").strip()[:80]
    if len(id_local) < 8:
        raise ErroDeValidacao("a batida guardada veio sem número", campo="id")
    ja = db.um(conn, "SELECT id FROM ponto.marcacoes WHERE dispositivo_id = :d AND id_no_aparelho = :i",
               d=aparelho["id"], i=id_local)
    if ja:
        return marcacoes.por_id(conn, int(ja["id"])), True

    identificacao = str(d.get("identificacao") or "").strip().upper()
    if d.get("colaborador_id"):
        pessoa = cadastros.colaborador_por_id(conn, int(d["colaborador_id"]))
        if not pessoa:
            raise ErroDeValidacao("pessoa não cadastrada", campo="colaborador_id")
        cpf = pessoa["cpf"]
    else:
        cpf = cadastros.normalizar_cpf(d.get("cpf"))
        identificacao = IDENT_NAO_RECONHECIDO
    # QR_APP: a pessoa foi identificada COM internet e a conexão caiu antes do envio.
    if identificacao not in (qr.IDENT_CPF, qr.IDENT_WHATSAPP, qr.IDENT_APP, IDENT_NAO_RECONHECIDO):
        raise ErroDeValidacao("identificação desconhecida", campo="identificacao")

    recebida = horario.agora()
    momento, motivo_hora = decidir_hora(
        momento=_ler_momento(d.get("momento"), "momento"), comprovada=bool(d.get("hora_comprovada")),
        sincronizado_em=_ler_momento(d.get("sincronizado_em"), "sincronizado_em"), recebida_em=recebida)
    if motivo_hora is None:
        motivo_hora = _chegada_suspeita(conn, aparelho["id"], momento, recebida)
    motivos = []
    if motivo_hora:
        # Relógio e contador discordaram: o aparelho manda as duas, e quem confere
        # escolhe (o relógio mexido na mão e o aparelho que dormiu dão o mesmo sinal).
        contador = _ler_momento(d.get("hora_pelo_contador"), "hora_pelo_contador")
        if contador is not None:
            motivo_hora += f" (pela contagem interna do aparelho, {_hora(contador)})"
        motivos.append(f"sem internet: {motivo_hora}")
    if identificacao == IDENT_NAO_RECONHECIDO:
        motivos.append("sem internet: o aparelho não reconheceu o CPF digitado")
    return marcacoes.registrar(
        conn, cpf=cpf, obra=d.get("obra"), origem="PWA", device_uuid=device_uuid, device_token=device_token,
        latitude=d.get("latitude"), longitude=d.get("longitude"), precisao=d.get("precisao"),
        timestamp_dispositivo=d.get("relogio_aparelho"), foto_base64=d.get("foto_base64"), ip=ip,
        agora=momento, identificacao=(qr.IDENT_CPF if identificacao == IDENT_NAO_RECONHECIDO else identificacao),
        sem_internet={"recebida_em": recebida, "id_no_aparelho": id_local, "motivos": motivos})
