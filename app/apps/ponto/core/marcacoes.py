# -*- coding: utf-8 -*-
"""
A batida.

DUAS FAMÍLIAS DE PROBLEMA, DOIS DESTINOS — e a diferença é jurídica, não técnica:

  - **IDENTIDADE** (aparelho não aprovado ou não autorizado para a pessoa ou
    para a obra, pessoa desligada, obra encerrada, token errado): a batida é
    RECUSADA, com linha em `ponto.recusas` e resposta 403. Sem a identidade
    não há o que registrar.

  - **LUGAR** — mudou em 04/10/2026, por decisão do dono: "não queremos
    permitir que a pessoa bata ponto fora das áreas de obra. E quero ainda que
    a obra seja detectada automaticamente." A OBRA É A DA CERCA em que o
    aparelho está (`geo.localizar_obra`), não a escolhida na lista. FORA da
    área de qualquer obra, a batida é RECUSADA (com a distância na mensagem e
    linha em `recusas`) — salvo obra marcada ANALISAR. Na BORDA (fora do raio,
    mas dentro da precisão do GPS, até 150 m), entra para análise. Obra sem
    coordenada não bloqueia ninguém. Celular sem localização é recusado; o
    tablet da obra sem localização, não (o aparelho já é da obra). A regra
    pura é `decidir_lugar`.
    Sobre a lei, dito sem rodeio porque a primeira versão deste texto exagerou:
    a Portaria 671/2021 proíbe restringir o HORÁRIO da marcação, marcar
    sozinho e exigir autorização para hora extra; restringir o LUGAR não está
    nessa lista. O risco que fica é outro: quem trabalhou fora da obra e foi
    barrado tem as horas reclamáveis — por isso a recusa vira alerta para
    alguém lançar o ajuste.

  - **RELÓGIO E CADASTRO** (relógio do aparelho muito diferente do servidor,
    pessoa afastada no cadastro, obra fora da lista da pessoa, sem foto no
    tablet): a batida é ACEITA e marcada EM_ANALISE, com o motivo. Quem decide
    é gente, na tela de pendências.

NSR E HASH ENCADEADO: cada marcação recebe o próximo número da sequência (sem
furo, sob trava do Postgres para dois pedidos simultâneos não pegarem o mesmo)
e um hash que inclui o hash da marcação anterior. Apagar uma do meio quebra a
corrente — e é isso que a fiscalização quer poder conferir.

REPETIÇÃO: a mesma pessoa, menos de 60 segundos depois da última batida, não
gera batida nova — responde a que já existe, marcada `repetida`. Toque duplo no
celular não vira ponto duplo.

`decidir` é função PURA: recebe o que se sabe e devolve (status, motivos). É
ela que o teste sem banco percorre.

COMO A PESSOA FOI IDENTIFICADA fica na batida (`identificacao`, migração 003):
no celular dela (SESSAO), CPF digitado no tablet (CPF), QR do WhatsApp
(QR_WHATSAPP), QR do "Meu ponto" (QR_APP) ou sistema (CHAVE). E no APARELHO DA
OBRA a foto é parte da identificação: sem ela, a batida é aceita, mas vai para
análise — com a câmera sempre ligada, foto que falta é exceção a ser olhada.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, Recusada
from . import cadastros, dispositivos, fotos, geo, recusas

logger = logging.getLogger("ponto.marcacoes")

ORIGENS = ("PWA", "IDFACE", "MANUAL")
IDENTIFICACOES = ("SESSAO", "CPF", "QR_WHATSAPP", "QR_APP", "CHAVE")
MOTIVO_SEM_FOTO_NO_TABLET = "sem foto no aparelho da obra"
JANELA_REPETICAO_SEGUNDOS = 60
TOLERANCIA_RELOGIO_SEGUNDOS = 300

# Trava única para a sequência do NSR (qualquer inteiro fixo serve).
_TRAVA_NSR = 7_671_2021


# ---------------------------------------------------------------------------
# A decisão, pura
# ---------------------------------------------------------------------------
def decidir(*, origem: str, dentro_da_cerca: Optional[bool], motivo_cerca: Optional[str],
            diferenca_relogio_s: Optional[float], situacao_pessoa: str,
            obra_na_lista_da_pessoa: bool,
            sem_foto_no_tablet: bool = False) -> tuple[str, list[str]]:
    """Devolve (status, motivos). Só VALIDA quando não há motivo nenhum."""
    motivos: list[str] = []
    if sem_foto_no_tablet:
        motivos.append(MOTIVO_SEM_FOTO_NO_TABLET)
    if dentro_da_cerca is False and motivo_cerca:
        motivos.append(motivo_cerca)
    elif dentro_da_cerca is None and motivo_cerca:
        # Sem coordenada não dá para saber. Para o celular isso é problema;
        # o iDFace é fixo na obra e o lançamento manual é conferido por gente.
        if origem == "PWA":
            motivos.append(motivo_cerca)
    if diferenca_relogio_s is not None and abs(diferenca_relogio_s) > TOLERANCIA_RELOGIO_SEGUNDOS:
        motivos.append(f"relógio do aparelho difere do servidor em {abs(diferenca_relogio_s):.0f} s")
    if situacao_pessoa and situacao_pessoa != "ATIVO":
        motivos.append({"FORA_DO_REGISTRO": "pessoa fora do Registro de Colaboradores",
                        "AFASTADO": "pessoa afastada no cadastro"}.get(
            situacao_pessoa, f"pessoa {situacao_pessoa.lower()} no cadastro"))
    if not obra_na_lista_da_pessoa:
        motivos.append("obra fora da lista da pessoa")
    return ("VALIDA" if not motivos else "EM_ANALISE"), motivos


MODOS_FORA_DA_CERCA = ("BLOQUEAR", "ANALISAR")


def modo_fora_da_cerca(conn: Connection, obra_id: int) -> str:
    """BLOQUEAR (o padrão desde 04/10/2026) ou ANALISAR. Antes da migração 003
    ser aplicada, o jeito antigo (ANALISAR): o código chega ao servidor antes
    do botão, e nada pode mudar de comportamento pela metade."""
    if not db.tem_coluna(conn, "obra_config", "fora_da_cerca"):
        return "ANALISAR"
    linha = db.um(conn, "SELECT fora_da_cerca FROM ponto.obra_config WHERE obra_id = :o", o=obra_id)
    return (linha or {}).get("fora_da_cerca") or "BLOQUEAR"


def decidir_lugar(*, situacao: str, detectada: Optional[dict], distancia: Optional[float],
                  enviada: Optional[dict], no_tablet: bool, precisao, modo,
                  latitude=None, longitude=None) -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """PURA (o `modo` é uma função da obra). Devolve (obra, motivo da RECUSA,
    motivo de ANÁLISE). Decisão do dono, 04/10/2026: fora da área da obra não
    se bate ponto; e a obra é a da cerca, detectada sozinha.

      DENTRO     a obra da cerca, sem motivo nenhum
      BORDA      a obra da cerca, para conferência (o GPS impreciso não recusa)
      SEM_LOCAL  no tablet da obra: aceita, para conferência (o aparelho já é
                 da obra); no celular: recusa se a obra bloqueia
      FORA       recusa se a obra bloqueia; se a obra escolhida NÃO TEM
                 coordenada, não há como saber, e vai para conferência
    """
    if situacao == geo.DENTRO:
        return detectada, None, None
    if situacao == geo.BORDA:
        return detectada, None, (f"na borda da cerca: {geo.distancia_legivel(distancia)} da obra "
                                 f"(raio {detectada['raio_metros']} m; GPS com precisão de "
                                 f"{geo.distancia_legivel(float(precisao or 0))})")
    if situacao == geo.SEM_LOCAL:
        alvo = enviada
        if not alvo:
            return None, "sem localização não dá para saber a obra — ligue a localização", None
        if no_tablet or modo(alvo) == "ANALISAR":
            return alvo, None, None          # o motivo "sem localização" sai da avaliação da cerca
        return None, "localização desligada — ligue a localização do celular para bater o ponto", None
    # FORA
    if enviada and not geo.coordenada_valida(enviada.get("latitude"), enviada.get("longitude")):
        return enviada, None, None           # "obra sem coordenada cadastrada" vai para análise
    alvo = enviada or detectada
    if not alvo:
        return None, "nenhuma obra com coordenada cadastrada perto daqui", None
    d = (geo.distancia_metros(latitude, longitude, alvo["latitude"], alvo["longitude"])
         if alvo is not detectada else distancia)
    if modo(alvo) == "BLOQUEAR":
        return None, (f"fora da área da obra: {geo.distancia_legivel(d)} da obra {alvo['codigo']} "
                      f"(raio {alvo['raio_metros']} m)"), None
    return alvo, None, None                  # ANALISAR: o motivo "fora da cerca" sai da avaliação


def _proximo_nsr(conn: Connection, colaborador_id: int, obra_id: int,
                 momento: dt.datetime, origem: str) -> tuple[int, str]:
    """O próximo NSR e o hash encadeado, sob a trava do Postgres: dois pedidos
    ao mesmo tempo não pegam o mesmo número. A trava é da TRANSAÇÃO, e solta
    sozinha no fim dela."""
    conn.exec_driver_sql("SELECT pg_advisory_xact_lock(%s)", (_TRAVA_NSR,))
    anterior = db.um(conn, "SELECT nsr, hash_encadeado FROM ponto.marcacoes "
                           "ORDER BY nsr DESC LIMIT 1")
    nsr = int(anterior["nsr"]) + 1 if anterior else 1
    hash_anterior = anterior["hash_encadeado"] if anterior else "0" * 64
    return nsr, hash_da_marcacao(hash_anterior, nsr, colaborador_id, obra_id, momento, origem)


def hash_da_marcacao(hash_anterior: str, nsr: int, colaborador_id: int, obra_id: int,
                     momento: dt.datetime, origem: str) -> str:
    base = f"{hash_anterior}|{nsr}|{colaborador_id}|{obra_id}|" \
           f"{momento.astimezone(horario.UTC).isoformat(timespec='seconds')}|{origem}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def validar_origem(origem) -> str:
    valor = str(origem or "PWA").strip().upper()
    if valor not in ORIGENS:
        raise ErroDeValidacao(f"origem desconhecida: {origem!r} (use {', '.join(ORIGENS)})",
                              campo="origem")
    return valor


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def por_id(conn: Connection, marcacao_id: int) -> Optional[dict]:
    return db.um(conn, _SQL_MARCACAO + " WHERE m.id = :id", id=marcacao_id)


_SQL_MARCACAO = """
    SELECT m.*, c.cpf, c.nome AS colaborador_nome, o.codigo AS obra_codigo, o.nome AS obra_nome
      FROM ponto.marcacoes m
      JOIN public.colaboradores c ON c.id = m.colaborador_id
      JOIN public.obras o ON o.id = m.obra_id
"""


def para_json(m: dict) -> dict:
    return {
        "id": m["id"], "nsr": m["nsr"],
        "cpf": m["cpf"], "nome": m["colaborador_nome"], "colaborador_id": m["colaborador_id"],
        "obra": {"id": m["obra_id"], "codigo": m["obra_codigo"], "nome": m["obra_nome"]},
        "data_referencia": m["data_referencia"].isoformat(),
        "horario": horario.texto(m["timestamp_servidor"]),
        "timestamp_servidor_utc": m["timestamp_servidor"].astimezone(horario.UTC)
                                   .isoformat(timespec="seconds"),
        "timestamp_dispositivo": horario.texto(m.get("timestamp_dispositivo")),
        "latitude": float(m["latitude"]) if m.get("latitude") is not None else None,
        "longitude": float(m["longitude"]) if m.get("longitude") is not None else None,
        "dentro_da_cerca": m.get("dentro_da_cerca"),
        "distancia_metros": (float(m["distancia_metros"])
                             if m.get("distancia_metros") is not None else None),
        "dispositivo_id": m.get("dispositivo_id"),
        "foto_hash": m.get("foto_hash"), "tem_foto": m.get("foto_id") is not None,
        "origem": m["origem"], "status": m["status"], "motivo_analise": m.get("motivo_analise"),
        "marcacao_origem_id": m.get("marcacao_origem_id"),
        "registrado_por": m.get("registrado_por"),
        "identificacao": m.get("identificacao"),
        "hash": m["hash_encadeado"],
    }


# ---------------------------------------------------------------------------
# O registro
# ---------------------------------------------------------------------------
def _recusar(conn: Connection, motivo: str, **contexto) -> None:
    """Grava a recusa em TRANSAÇÃO PRÓPRIA e levanta `Recusada`. Própria porque a
    da batida é desfeita pelo erro — e a recusa precisa sobreviver a ele, senão
    ninguém investiga nada."""
    with db.conexao() as separada:
        recusas.registrar(separada, motivo=motivo, **contexto)
    raise Recusada(motivo)


def registrar(conn: Connection, *, cpf, obra, origem: str = "PWA",
              device_uuid: str | None = None, device_token: str | None = None,
              via_chave: bool = False, latitude=None, longitude=None,
              timestamp_dispositivo: str | None = None, foto_base64: str | None = None,
              registrado_por: str | None = None, ip: str | None = None,
              agora: dt.datetime | None = None,
              identificacao: str | None = None, precisao=None,
              justificativa: str | None = None) -> tuple[dict, bool]:
    """Registra a batida. Devolve (marcação, repetida).

    Levanta ErroDeValidacao (400) para entrada ruim, Recusada (403) para
    problema de identidade — já registrado em `ponto.recusas`."""
    momento = agora or horario.agora()
    origem_ok = validar_origem(origem)
    contexto = {"device_uuid": device_uuid, "cpf": str(cpf or ""), "obra": obra,
                "origem": origem_ok, "ip": ip}

    # --- quem chama ---------------------------------------------------------
    if origem_ok == "PWA" and not via_chave:
        if not device_uuid or not device_token:
            raise ErroDeValidacao("batida do celular exige device_uuid e X-Device-Token",
                                  campo="device_uuid")
    if origem_ok != "PWA" and not via_chave:
        _recusar(conn, f"origem {origem_ok} só com chave de API", **contexto)
    if origem_ok == "MANUAL" and not (registrado_por or "").strip():
        raise ErroDeValidacao("lançamento manual exige quem registrou (registrado_por)",
                              campo="registrado_por")

    # --- entrada ------------------------------------------------------------
    cpf_ok = cadastros.normalizar_cpf(cpf)
    contexto["cpf"] = cpf_ok
    if latitude is not None or longitude is not None:
        if not geo.coordenada_valida(latitude, longitude):
            raise ErroDeValidacao("latitude/longitude inválidas", campo="latitude")
    try:
        momento_aparelho = horario.ler_iso(timestamp_dispositivo)
    except ValueError as e:
        raise ErroDeValidacao("timestamp_dispositivo ilegível (use ISO 8601)",
                              campo="timestamp_dispositivo") from e

    # --- pessoa -------------------------------------------------------------
    pessoa = cadastros.colaborador_por_cpf(conn, cpf_ok)
    if not pessoa:
        _recusar(conn, "pessoa não cadastrada", **contexto)
    if pessoa["situacao"] == "DESLIGADO" or not pessoa["ativo_no_ponto"]:
        _recusar(conn, "pessoa desligada ou inativa no ponto", **contexto)

    # --- aparelho (quem é; a obra ele confere depois) ------------------------
    aparelho, obras_do_aparelho = None, set()
    if device_uuid and not via_chave:
        try:
            aparelho = dispositivos.autenticar(conn, device_uuid, device_token)
        except Exception:  # noqa: BLE001 — uuid desconhecido ou token errado: mesma recusa
            _recusar(conn, "aparelho desconhecido ou token inválido", **contexto)
        obras_do_aparelho = dispositivos.obras_de(conn, aparelho["id"])
        # Identidade ANTES do lugar: aparelho bloqueado, celular de outra
        # pessoa ou pessoa fora da lista é recusado pelo que é — não por estar
        # sem localização. (A obra do aparelho é conferida depois da cerca.)
        motivo = dispositivos.autorizado_para(
            aparelho, int(pessoa["id"]), -1, dispositivos.autorizados_de(conn, aparelho["id"]), set())
        if motivo:
            _recusar(conn, motivo, **contexto)
    elif device_uuid and via_chave:
        # Sistema informando por qual aparelho veio (iDFace): só registra a referência.
        aparelho = dispositivos.por_uuid(conn, device_uuid)
    no_tablet = bool(aparelho) and not via_chave and aparelho.get("perfil") in ("COMPARTILHADO", "LISTA")

    # --- obra: a da cerca em que o celular está -------------------------------
    enviada = cadastros.resolver_obra(conn, obra) if obra not in (None, "") else None
    if obra not in (None, "") and not enviada:
        _recusar(conn, "obra não cadastrada", **contexto)
    motivo_borda = None
    if origem_ok == "PWA" and not via_chave:
        candidatas = [o for o in cadastros.listar_obras(conn, so_ativas=True)
                      if not obras_do_aparelho or int(o["id"]) in obras_do_aparelho]
        situacao, detectada, distancia_detectada = geo.localizar_obra(
            latitude, longitude, precisao, candidatas)
        obra_ok, recusa, motivo_borda = decidir_lugar(
            situacao=situacao, detectada=detectada, distancia=distancia_detectada,
            enviada=enviada, no_tablet=no_tablet, precisao=precisao,
            modo=lambda o: modo_fora_da_cerca(conn, int(o["id"])),
            latitude=latitude, longitude=longitude)
        if recusa:
            _recusar(conn, recusa, situacao=situacao, distancia_metros=distancia_detectada,
                     precisao=precisao, **contexto)
        if enviada and obra_ok and int(obra_ok["id"]) != int(enviada["id"]):
            logger.info("Ponto: obra detectada pela cerca (%s) no lugar da escolhida (%s)",
                        obra_ok["codigo"], enviada["codigo"])
    else:
        obra_ok = enviada
        if not obra_ok:
            _recusar(conn, "obra não cadastrada", **contexto)
    contexto["obra"] = obra_ok["codigo"]
    if obra_ok["status"] != "ATIVA" or not obra_ok["ativo_no_ponto"]:
        _recusar(conn, "obra encerrada ou inativa no ponto", **contexto)
    if aparelho and not via_chave:
        motivo = dispositivos.autorizado_para(
            aparelho, int(pessoa["id"]), int(obra_ok["id"]),
            dispositivos.autorizados_de(conn, aparelho["id"]), obras_do_aparelho)
        if motivo:
            _recusar(conn, motivo, **contexto)

    # --- repetição ----------------------------------------------------------
    ultima = db.um(conn, _SQL_MARCACAO + """
        WHERE m.colaborador_id = :c AND m.timestamp_servidor > :desde
        ORDER BY m.timestamp_servidor DESC LIMIT 1
    """, c=pessoa["id"], desde=momento - dt.timedelta(seconds=JANELA_REPETICAO_SEGUNDOS))
    if ultima:
        logger.info("Ponto: batida repetida em %ds de %s — devolvendo NSR %s",
                    JANELA_REPETICAO_SEGUNDOS, recusas.mascarar_cpf(cpf_ok), ultima["nsr"])
        return ultima, True

    # --- cerca, relógio, decisão -------------------------------------------
    dentro, distancia, motivo_cerca = geo.avaliar_cerca(
        latitude, longitude, obra_ok["latitude"], obra_ok["longitude"], obra_ok["raio_metros"])
    if motivo_borda:
        motivo_cerca = motivo_borda
    diferenca = ((momento_aparelho - momento).total_seconds()
                 if momento_aparelho is not None else None)
    status, motivos = decidir(
        origem=origem_ok, dentro_da_cerca=dentro, motivo_cerca=motivo_cerca,
        diferenca_relogio_s=diferenca, situacao_pessoa=pessoa["situacao"],
        obra_na_lista_da_pessoa=int(obra_ok["id"]) in cadastros.obras_da_pessoa(conn, pessoa["id"]),
        sem_foto_no_tablet=(no_tablet and origem_ok == "PWA" and not foto_base64))
    # A obra escolhida na lista (não detectada) vem com a explicação da pessoa:
    # a batida não é a regular, e quem confere precisa saber por quê.
    justificativa_ok = " ".join(str(justificativa or "").split())[:300]
    if status == "EM_ANALISE" and justificativa_ok:
        motivos.append(f"justificativa da pessoa: {justificativa_ok}")
    if identificacao is None:
        identificacao = "CHAVE" if via_chave else ("CPF" if no_tablet else "SESSAO")
    if identificacao not in IDENTIFICACOES:
        raise ErroDeValidacao("identificação desconhecida", campo="identificacao")
    data_ref = horario.data_referencia(momento, pessoa["tipo_jornada"])

    # --- foto: valida e reduz ANTES de gravar (foto ilegível é 400 limpo) ----
    foto = fotos.preparar(foto_base64) if foto_base64 else None

    # --- NSR e corrente -----------------------------------------------------
    nsr, corrente = _proximo_nsr(conn, int(pessoa["id"]), int(obra_ok["id"]), momento, origem_ok)

    linha = db.um(conn, """
        INSERT INTO ponto.marcacoes (nsr, colaborador_id, obra_id, timestamp_servidor,
            timestamp_dispositivo, data_referencia, latitude, longitude, dentro_da_cerca,
            distancia_metros, dispositivo_id, foto_id, foto_hash, origem, status,
            motivo_analise, hash_encadeado, registrado_por)
        VALUES (:nsr, :c, :o, :ts, :tsd, :dref, :lat, :lon, :dentro, :dist, :disp, :foto,
                :fhash, :origem, :status, :motivo, :hash, :por)
        RETURNING id
    """, nsr=nsr, c=pessoa["id"], o=obra_ok["id"], ts=momento, tsd=momento_aparelho,
         dref=data_ref, lat=geo.decimal_ou_none(latitude), lon=geo.decimal_ou_none(longitude),
         dentro=dentro, dist=distancia, disp=(aparelho["id"] if aparelho else None),
         foto=None, fhash=(foto.hash if foto else None), origem=origem_ok, status=status,
         motivo=("; ".join(motivos) if motivos else None), hash=corrente,
         por=((registrado_por or "").strip()[:120] or None))
    if db.tem_003(conn):
        db.executar(conn, "UPDATE ponto.marcacoes SET identificacao = :i WHERE id = :id",
                    i=identificacao, id=linha["id"])
    if foto:
        # Depois do NSR, para o nome do arquivo carregar o número da batida.
        # Sem subir agora: a foto entra na sala de espera e a rota chama
        # `fotos.disparar_envio()` depois de a batida estar confirmada — quem
        # está na fila do tablet não espera o Drive.
        foto_id = fotos.guardar(conn, foto, nome=fotos.nome_do_arquivo(
            momento, int(pessoa["id"]), nsr), momento=momento, subir=False)
        db.executar(conn, "UPDATE ponto.marcacoes SET foto_id = :f WHERE id = :id",
                    f=foto_id, id=linha["id"])
    if aparelho:
        dispositivos.marcar_uso(conn, aparelho["id"])

    logger.info("Ponto: batida NSR %d — cpf %s, obra %s, %s em %s (%s)%s",
                nsr, recusas.mascarar_cpf(cpf_ok), obra_ok["codigo"], origem_ok,
                horario.texto(momento), status,
                f" — {'; '.join(motivos)}" if motivos else "")
    return por_id(conn, int(linha["id"])), False


# ---------------------------------------------------------------------------
# Pedido de ajuste (a decisão é fase 2)
# ---------------------------------------------------------------------------
TIPOS_AJUSTE = ("INCLUSAO", "EXCLUSAO", "ALTERACAO_HORARIO", "ALTERACAO_OBRA", "ABONO")


def solicitar_ajuste(conn: Connection, *, cpf, data_referencia: str, tipo: str,
                     justificativa: str, solicitado_por: str, marcacao_id: int | None = None,
                     horario_proposto: str | None = None, obra_proposta=None) -> dict:
    tipo_ok = str(tipo or "").strip().upper()
    if tipo_ok not in TIPOS_AJUSTE:
        raise ErroDeValidacao(f"tipo de ajuste desconhecido: {tipo!r}", campo="tipo")
    if len((justificativa or "").strip()) < 10:
        raise ErroDeValidacao("justificativa precisa de ao menos 10 caracteres",
                              campo="justificativa")
    if not (solicitado_por or "").strip():
        raise ErroDeValidacao("diga quem solicita (solicitado_por)", campo="solicitado_por")
    try:
        data = dt.date.fromisoformat(str(data_referencia))
    except ValueError as e:
        raise ErroDeValidacao("data_referencia ilegível (AAAA-MM-DD)", campo="data_referencia") from e
    pessoa = cadastros.colaborador_por_cpf(conn, cadastros.normalizar_cpf(cpf))
    if not pessoa:
        raise ErroDeValidacao("pessoa não cadastrada", campo="cpf")
    if marcacao_id is not None and not por_id(conn, int(marcacao_id)):
        raise ErroDeValidacao("marcação não encontrada", campo="marcacao_id")
    obra = cadastros.resolver_obra(conn, obra_proposta) if obra_proposta else None
    if obra_proposta and not obra:
        raise ErroDeValidacao("obra proposta não cadastrada", campo="obra_proposta")
    try:
        proposto = horario.ler_iso(horario_proposto)
    except ValueError as e:
        raise ErroDeValidacao("horario_proposto ilegível (ISO 8601)", campo="horario_proposto") from e
    linha = db.um(conn, """
        INSERT INTO ponto.ajustes (marcacao_id, colaborador_id, data_referencia, tipo,
            horario_proposto, obra_proposta_id, justificativa, solicitado_por)
        VALUES (:m, :c, :d, :t, :h, :o, :j, :por) RETURNING *
    """, m=marcacao_id, c=pessoa["id"], d=data, t=tipo_ok, h=proposto,
         o=(obra["id"] if obra else None), j=justificativa.strip()[:2000],
         por=solicitado_por.strip()[:120])
    logger.info("Ponto: ajuste %s pedido para cpf %s em %s (%s)", tipo_ok,
                recusas.mascarar_cpf(pessoa["cpf"]), data, solicitado_por)
    return dict(linha)


# ---------------------------------------------------------------------------
# Tratamento (fase 2): incluir a batida de um ajuste aprovado e decidir a
# batida em análise. Nenhum dos dois altera hora, lugar ou corrente de uma
# batida que já existe.
# ---------------------------------------------------------------------------
STATUS_DECIDIVEIS = ("VALIDA", "REJEITADA")


def registrar_ajustada(conn: Connection, *, colaborador_id: int, obra_id: int,
                       momento: dt.datetime, ocorrencia_id: int, aprovado_por: str) -> dict:
    """Inclui a batida que a pessoa esqueceu, como AJUSTADA. Ganha NSR próprio
    (o NSR é a ordem do REGISTRO, não do relógio) e a hora é a do pedido."""
    pessoa = cadastros.colaborador_por_id(conn, colaborador_id)
    if not pessoa:
        raise ErroDeValidacao("pessoa não cadastrada", campo="colaborador_id")
    data_ref = horario.data_referencia(momento, pessoa["tipo_jornada"])
    nsr, corrente = _proximo_nsr(conn, colaborador_id, obra_id, momento, "MANUAL")
    linha = db.um(conn, """
        INSERT INTO ponto.marcacoes (nsr, colaborador_id, obra_id, timestamp_servidor,
            data_referencia, origem, status, motivo_analise, hash_encadeado, registrado_por)
        VALUES (:nsr, :c, :o, :ts, :dref, 'MANUAL', 'AJUSTADA', :motivo, :hash, :por)
        RETURNING id
    """, nsr=nsr, c=colaborador_id, o=obra_id, ts=momento, dref=data_ref,
         motivo=f"inclusão pelo pedido de ajuste nº {ocorrencia_id}", hash=corrente,
         por=f"ajuste aprovado por {aprovado_por}"[:120])
    logger.info("Ponto: batida AJUSTADA NSR %d incluída (pedido %d, por %s)",
                nsr, ocorrencia_id, aprovado_por)
    return por_id(conn, int(linha["id"]))


def decidir_em_analise(conn: Connection, marcacao_id: int, *, para: str, motivo: str,
                       usuario_id: int | None, usuario_nome: str) -> dict:
    """Valida ou rejeita uma batida EM_ANALISE. Rejeitar exige motivo."""
    from . import competencias
    para_ok = str(para or "").strip().upper()
    if para_ok not in STATUS_DECIDIVEIS:
        raise ErroDeValidacao("decida VALIDA ou REJEITADA", campo="para")
    m = por_id(conn, marcacao_id)
    if not m:
        from ..erros import NaoEncontrado
        raise NaoEncontrado("batida não encontrada")
    if m["status"] != "EM_ANALISE":
        raise ErroDeValidacao(f"esta batida não está em análise (está {m['status'].lower()})",
                              campo="status")
    if para_ok == "REJEITADA" and len((motivo or "").strip()) < 5:
        raise ErroDeValidacao("diga por que a batida foi rejeitada", campo="motivo")
    competencias.exigir_aberta(conn, m["data_referencia"])
    db.executar(conn, "UPDATE ponto.marcacoes SET status = :s WHERE id = :id",
                s=para_ok, id=marcacao_id)
    db.executar(conn, """
        INSERT INTO ponto.marcacao_decisoes (marcacao_id, de_status, para_status, motivo,
                                             usuario_id, usuario_nome)
        VALUES (:m, 'EM_ANALISE', :p, :mot, :u, :n)
    """, m=marcacao_id, p=para_ok, mot=(motivo or "").strip()[:500], u=usuario_id,
         n=usuario_nome[:120])
    logger.info("Ponto: batida NSR %s %s por %s", m["nsr"], para_ok, usuario_nome)
    return por_id(conn, marcacao_id)
