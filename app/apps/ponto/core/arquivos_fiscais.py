# -*- coding: utf-8 -*-
"""
OS ARQUIVOS DA FISCALIZAÇÃO — AFD e AEJ (Portaria MTP 671/2021, Anexos V e VI).

06/11/2026, a revisão da Portaria com o dono: os dois arquivos que o auditor fiscal pede eram o item que faltava.
O leiaute OFICIAL está copiado em `docs/leiaute_afd_portaria_671.txt` e `docs/leiaute_aej_portaria_671.txt`
(baixados do gov.br nesse dia) — qualquer mudança aqui confere lá primeiro.

AFD (Arquivo Fonte de Dados) — as marcações ORIGINAIS do REP-P, como foram feitas:
  · registro "1" (cabeçalho, com CRC-16/KERMIT), um "7" por marcação original, o "9" (trailer) e a linha da
    assinatura digital ("ASSINATURA_DIGITAL_EM_ARQUIVO_P7S");
  · originais = toda marcação que NÃO foi incluída por ajuste (status AJUSTADA fica de fora: é tratamento, vai
    para o AEJ como "incluída"). A rejeitada e a em análise ENTRAM: o AFD é o que o relógio recebeu;
  · o código hash do "7" é o SHA-256 dos campos 1 a 7 + o hash do "7" anterior do mesmo empregador, desde a
    primeira marcação — por isso a corrente é refeita do começo a cada arquivo (e o mesmo período dá o mesmo arquivo).

AEJ (Arquivo Eletrônico de Jornada) — o tratamento: vínculos, horários contratuais, as marcações como contaram
(entrada/saída, a incluída por ajuste com o motivo, a desconsiderada com o motivo, o intervalo pré-assinalado),
as faltas não justificadas e o programa (PTRP).

O EMPREGADOR de cada marcação é a empresa da OBRA em que ela foi feita (`obras.empresa_id`). ⚠️ Escolha dita ao
dono: quem é contratado por um CNPJ e bate em obra de outro aparece no arquivo do CNPJ da obra.

O QUE FICA DE FORA (dito ao dono):
  · o arquivo .p7s da assinatura digital — precisa do certificado ICP-Brasil da empresa; a linha de assinatura sai;
  · registros "2" (identificação da empresa), "5" (empregado) e "6" (eventos sensíveis) do AFD: o NSR do sistema é
    o das marcações, e esses registros precisariam de números próprios na mesma sequência;
  · no AEJ, o DSR, a folga compensatória de feriado e o banco de horas (tipos 1, 4 e 3 do registro "07").
  · ⚠️ o NSR é UMA sequência para tudo (também a batida incluída por ajuste e as das outras empresas): o AFD de um
    CNPJ tem saltos nesses números. A numeração continua sendo a do comprovante que a pessoa recebeu.
"""
from __future__ import annotations

import datetime as dt
import hashlib
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao, NaoEncontrado
from . import parametros

ASSINATURA = "ASSINATURA_DIGITAL_EM_ARQUIVO_P7S".ljust(100)
FIM = "\r\n"
VERSAO_AFD = "004"
VERSAO_AEJ = "002"
NOME_DO_PROGRAMA = "BWS Ponto"
VERSAO_DO_PROGRAMA = "1.0"

PARAMETROS = {"inpi": "rep.inpi", "desenvolvedor_cnpj": "rep.desenvolvedor_cnpj",
              "desenvolvedor_nome": "rep.desenvolvedor_nome", "desenvolvedor_email": "rep.desenvolvedor_email"}

# O coletor da marcação (AFD, campo 6 do "7"): o aplicativo do ponto roda no navegador do celular ou do tablet.
COLETOR = {"PWA": "02", "IDFACE": "04", "MANUAL": "05"}


# ---------------------------------------------------------------------------
# Puras
# ---------------------------------------------------------------------------
def crc16_kermit(texto: str) -> str:
    """PURA. CRC-16/KERMIT (o "CCITT-TRUE" da Portaria), em 4 hexadecimais maiúsculos. "123456789" → "2189"."""
    crc = 0
    for byte in texto.encode("latin-1", errors="replace"):
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if crc & 1 else crc >> 1
    return f"{crc & 0xFFFF:04X}"


def dh(momento: dt.datetime) -> str:
    """PURA. "AAAA-MM-ddThh:mm:00-0300" (24 caracteres), na hora de Fortaleza."""
    local = horario.para_local(momento)
    off = local.utcoffset() or dt.timedelta(0)
    sinal = "-" if off < dt.timedelta(0) else "+"
    minutos = abs(int(off.total_seconds() // 60))
    return f"{local:%Y-%m-%dT%H:%M}:00{sinal}{minutos // 60:02d}{minutos % 60:02d}"


def _so_digitos(v) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def _campo(valor, tamanho: int, numerico: bool = False) -> str:
    """PURA. O campo de tamanho fixo: começa pela esquerda e completa com espaço (regra 7 do AFD); o numérico com
    valor completa com zeros à esquerda."""
    v = str(valor or "")
    if numerico and v:
        v = v.zfill(tamanho)
    if len(v) > tamanho:
        v = v[:tamanho]
    return v.ljust(tamanho)


def linha_tipo1(*, cnpj: str, razao: str, inpi: str, de: dt.date, ate: dt.date, gerado_em: dt.datetime,
                desenvolvedor_cnpj: str) -> str:
    corpo = ("000000000" + "1" + "1" + _campo(_so_digitos(cnpj), 14) + _campo("", 14)
             + _campo(razao, 150) + _campo(_so_digitos(inpi), 17, numerico=True)
             + de.isoformat() + ate.isoformat() + dh(gerado_em) + VERSAO_AFD
             + "1" + _campo(_so_digitos(desenvolvedor_cnpj), 14) + _campo("", 30))
    assert len(corpo) == 298, len(corpo)
    return corpo + crc16_kermit(corpo)


def linha_tipo7(*, nsr: int, marcada_em: dt.datetime, cpf: str, gravada_em: dt.datetime, coletor: str,
                off_line: bool, hash_anterior: str) -> tuple[str, str]:
    """PURA. (a linha, o hash dela). O hash é o SHA-256 dos campos 1 a 7 e do hash do registro anterior."""
    campos = (_campo(nsr, 9, numerico=True) + "7" + dh(marcada_em) + _campo(_so_digitos(cpf), 12, numerico=True)
              + dh(gravada_em) + coletor + ("1" if off_line else "0"))
    codigo = hashlib.sha256((campos + (hash_anterior or "")).encode("latin-1")).hexdigest()
    linha = campos + codigo
    assert len(linha) == 137, len(linha)
    return linha, codigo


def linha_tipo9(qtd_tipo7: int) -> str:
    return "999999999" + "0" * 9 * 5 + _campo(qtd_tipo7, 9, numerico=True) + "9"


def nome_do_afd(inpi: str, cnpj: str) -> str:
    return f"AFD{_so_digitos(inpi)}{_so_digitos(cnpj)}REP_P.txt"


def aej_linha(*campos) -> str:
    """PURA. Os campos separados por "|" (sem "|" no fim). O "|" dentro de um texto vira "/"."""
    return "|".join(str(c if c is not None else "").replace("|", "/").replace("\r", " ").replace("\n", " ")
                    for c in campos)


def hhmm(minuto: int) -> str:
    return f"{(int(minuto) // 60) % 24:02d}{int(minuto) % 60:02d}"


# ---------------------------------------------------------------------------
# Configuração (o registro do programa) e as empresas
# ---------------------------------------------------------------------------
def configuracao(conn: Connection) -> dict:
    return {k: parametros.ler(conn, p, "") or "" for k, p in PARAMETROS.items()}


def gravar_configuracao(conn: Connection, dados: dict, por: str) -> dict:
    novos = {k: str(dados.get(k) or "").strip() for k in PARAMETROS if k in dados}
    if "inpi" in novos and novos["inpi"] and not (_so_digitos(novos["inpi"]) and len(_so_digitos(novos["inpi"])) <= 17):
        raise ErroDeValidacao("o número de registro no INPI tem até 17 números", campo="inpi")
    if novos.get("desenvolvedor_cnpj") and len(_so_digitos(novos["desenvolvedor_cnpj"])) not in (11, 14):
        raise ErroDeValidacao("CNPJ (ou CPF) do desenvolvedor ilegível", campo="desenvolvedor_cnpj")
    for k, v in novos.items():
        parametros.gravar(conn, PARAMETROS[k], v, por)
    return configuracao(conn)


def empresas(conn: Connection) -> list[dict]:
    """As empresas (CNPJ) que executam obras em que alguém já bateu ponto."""
    if not db.um(conn, """SELECT 1 AS x FROM information_schema.columns
                           WHERE table_schema = 'public' AND table_name = 'obras' AND column_name = 'empresa_id'"""):
        return []
    return [dict(l) for l in db.todos(conn, """
        SELECT e.id, e.cnpj, e.razao_social, count(DISTINCT o.id) AS obras
          FROM public.empresas e JOIN public.obras o ON o.empresa_id = e.id
         WHERE EXISTS (SELECT 1 FROM ponto.marcacoes m WHERE m.obra_id = o.id)
         GROUP BY e.id, e.cnpj, e.razao_social ORDER BY e.razao_social""")]


def _empresa(conn: Connection, empresa_id: int) -> dict:
    e = db.um(conn, "SELECT id, cnpj, razao_social FROM public.empresas WHERE id = :i", i=int(empresa_id))
    if not e:
        raise NaoEncontrado("empresa não encontrada")
    if len(_so_digitos(e["cnpj"])) not in (11, 14):
        raise ErroDeValidacao(f"o CNPJ da empresa {e['razao_social']} está ilegível no cadastro", campo="empresa_id")
    return e


def _periodo(de, ate) -> tuple[dt.date, dt.date]:
    try:
        inicio, fim = dt.date.fromisoformat(str(de)), dt.date.fromisoformat(str(ate))
    except ValueError as e:
        raise ErroDeValidacao("período ilegível — escolha o primeiro e o último dia") from e
    if fim < inicio:
        raise ErroDeValidacao("o último dia vem antes do primeiro")
    if (fim - inicio).days > 400:
        raise ErroDeValidacao("o período vai até 400 dias por arquivo")
    return inicio, fim


def _exigir_configuracao(cfg: dict) -> None:
    falta = [n for k, n in (("inpi", "o número de registro no INPI"),
                            ("desenvolvedor_cnpj", "o CNPJ do desenvolvedor do programa")) if not cfg.get(k)]
    if falta:
        raise ErroDeValidacao("preencha antes " + " e ".join(falta) + " (Configuração › Arquivos da fiscalização)",
                              campo="inpi")


# ---------------------------------------------------------------------------
# AFD
# ---------------------------------------------------------------------------
def afd(conn: Connection, empresa_id: int, de, ate, *, agora: Optional[dt.datetime] = None) -> tuple[str, bytes]:
    """(nome do arquivo, conteúdo em ISO-8859-1) do AFD da empresa no período (pela data da marcação)."""
    inicio, fim = _periodo(de, ate)
    cfg = configuracao(conn)
    _exigir_configuracao(cfg)
    e = _empresa(conn, empresa_id)
    tem = db.tem_coluna(conn, "marcacoes", "sem_internet")
    extra = ", m.sem_internet, m.recebida_em" if tem else ", FALSE AS sem_internet, NULL::timestamptz AS recebida_em"
    # a corrente do hash começa na primeira marcação original da empresa: todas, até o fim do período
    limite = dt.datetime.combine(fim + dt.timedelta(days=1), dt.time(0), tzinfo=horario.FUSO)
    marcas = db.todos(conn, f"""
        SELECT m.nsr, m.timestamp_servidor, m.criado_em, m.origem, c.cpf {extra}
          FROM ponto.marcacoes m
          JOIN public.colaboradores c ON c.id = m.colaborador_id
          JOIN public.obras o ON o.id = m.obra_id
         WHERE o.empresa_id = :e AND m.status <> 'AJUSTADA' AND m.timestamp_servidor < :lim
         ORDER BY m.nsr""", e=int(e["id"]), lim=limite)
    linhas, anterior, n7 = [], "", 0
    ini_local = dt.datetime.combine(inicio, dt.time(0), tzinfo=horario.FUSO)
    for m in marcas:
        gravada = m["recebida_em"] or m["criado_em"] or m["timestamp_servidor"]
        linha, anterior = linha_tipo7(nsr=int(m["nsr"]), marcada_em=m["timestamp_servidor"], cpf=m["cpf"],
                                      gravada_em=max(gravada, m["timestamp_servidor"]),
                                      coletor=COLETOR.get(m["origem"], "05"), off_line=bool(m["sem_internet"]),
                                      hash_anterior=anterior)
        if m["timestamp_servidor"] >= ini_local:
            linhas.append(linha)
            n7 += 1
    cab = linha_tipo1(cnpj=e["cnpj"], razao=e["razao_social"], inpi=cfg["inpi"], de=inicio, ate=fim,
                      gerado_em=agora or horario.agora(), desenvolvedor_cnpj=cfg["desenvolvedor_cnpj"])
    texto = FIM.join([cab, *linhas, linha_tipo9(n7), ASSINATURA]) + FIM
    return nome_do_afd(cfg["inpi"], e["cnpj"]), texto.encode("latin-1", errors="replace")


# ---------------------------------------------------------------------------
# AEJ
# ---------------------------------------------------------------------------
def aej(conn: Connection, empresa_id: int, de, ate, *, agora: Optional[dt.datetime] = None) -> tuple[str, bytes]:
    """(nome do arquivo, conteúdo em ISO-8859-1) do AEJ: o tratamento das marcações da empresa no período."""
    from . import cadastros, espelho
    inicio, fim = _periodo(de, ate)
    cfg = configuracao(conn)
    _exigir_configuracao(cfg)
    e = _empresa(conn, empresa_id)
    obras = {int(l["id"]) for l in db.todos(conn, "SELECT id FROM public.obras WHERE empresa_id = :e", e=int(e["id"]))}
    ids = [int(l["colaborador_id"]) for l in db.todos(conn, """
        SELECT DISTINCT m.colaborador_id FROM ponto.marcacoes m
         WHERE m.obra_id = ANY(:o) AND m.data_referencia BETWEEN :i AND :f""", o=sorted(obras) or [0], i=inicio, f=fim)]
    pessoas = sorted((p for p in (cadastros.colaborador_por_id(conn, i) for i in ids) if p),
                     key=lambda p: (p.get("nome") or "").upper())
    # as marcações do período, TODAS (a rejeitada sai como desconsiderada, com o motivo da decisão)
    por_dia: dict[tuple, list] = {}
    for r in db.todos(conn, """
            SELECT m.id, m.colaborador_id, m.data_referencia, m.timestamp_servidor, m.status, m.motivo_analise,
                   (SELECT d.motivo FROM ponto.marcacao_decisoes d WHERE d.marcacao_id = m.id
                     ORDER BY d.id DESC LIMIT 1) AS motivo_decisao
              FROM ponto.marcacoes m
             WHERE m.colaborador_id = ANY(:ids) AND m.data_referencia BETWEEN :i AND :f
             ORDER BY m.timestamp_servidor, m.nsr""", ids=ids or [0], i=inicio, f=fim):
        por_dia.setdefault((int(r["colaborador_id"]), r["data_referencia"]), []).append(r)
    from .apuracao import BATIDA_REPETIDA_MIN

    gerado = agora or horario.agora()
    linhas = [aej_linha("01", "1", _so_digitos(e["cnpj"]), "", "", (e["razao_social"] or "")[:150],
                        inicio.isoformat(), fim.isoformat(), dh(gerado), VERSAO_AEJ),
              aej_linha("02", "1", "3", _so_digitos(cfg["inpi"]).zfill(17))]
    vinculos, horarios, marcacoes, ausencias = [], {}, [], []
    lote = espelho.Lote(conn, pessoas, inicio, fim) if pessoas else None
    for n, p in enumerate(pessoas, start=1):
        vinculos.append(aej_linha("03", n, _so_digitos(p["cpf"]).zfill(11), (p["nome"] or "")[:150]))
        esp = espelho.montar(conn, int(p["id"]), inicio, fim, lote=lote)
        for d in esp["dias"]:
            dia = dt.date.fromisoformat(d["data"])
            cod = None
            if d.get("previstas"):
                pares = [x["hora"].replace(":", "") for x in d["previstas"]]
                cod = "H" + "-".join(pares)
                horarios.setdefault(cod, (d.get("previsto") or 0, pares))
            if d.get("situacao") == "FALTA" and not d.get("ocorrencia"):
                ausencias.append(aej_linha("07", n, "2", d["data"], "", ""))
            # entrada, saída, entrada…: as que contam; a repetida (a menos de 10 min) e a rejeitada, desconsideradas;
            # o intervalo pré-assinalado entra na ordem
            eventos, ultima = [], None
            for b in por_dia.get((int(p["id"]), dia), []):
                if b["status"] == "REJEITADA":
                    marcacoes.append(aej_linha("05", n, dh(b["timestamp_servidor"]), "1", "D", "000", "O", "",
                                               str(b["motivo_decisao"] or b["motivo_analise"] or "desconsiderada")[:150]))
                    continue
                if ultima is not None and b["timestamp_servidor"] - ultima < dt.timedelta(minutes=BATIDA_REPETIDA_MIN):
                    marcacoes.append(aej_linha("05", n, dh(b["timestamp_servidor"]), "1", "D", "000",
                                               "I" if b["status"] == "AJUSTADA" else "O", "",
                                               "batida repetida (contada uma vez)"))
                    continue
                ultima = b["timestamp_servidor"]
                eventos.append((b["timestamp_servidor"], b))
            for hora in d.get("pre_assinalado") or []:
                hh, mm = hora.split(":")
                eventos.append((dt.datetime.combine(dia, dt.time(int(hh), int(mm)), tzinfo=horario.FUSO), None))
            eventos.sort(key=lambda x: x[0])
            for k, (momento, b) in enumerate(eventos):
                tp, seq = ("E" if k % 2 == 0 else "S"), k // 2 + 1
                if b is None:
                    fonte, motivo = "P", ""
                elif b["status"] == "AJUSTADA":
                    fonte, motivo = "I", str(b["motivo_analise"] or "incluída por ajuste aprovado")[:150]
                else:
                    fonte, motivo = "O", ""
                marcacoes.append(aej_linha("05", n, dh(momento), "1", tp, str(seq).zfill(3), fonte,
                                           cod if (tp == "E" and seq == 1 and cod) else "", motivo))
    horarios_linhas = []
    for cod, (dur, pares) in sorted(horarios.items()):
        horarios_linhas.append(aej_linha("04", cod, int(dur or 0), *pares))
    linhas += vinculos + horarios_linhas + marcacoes + ausencias
    linhas.append(aej_linha("08", NOME_DO_PROGRAMA, VERSAO_DO_PROGRAMA,
                            "1" if len(_so_digitos(cfg["desenvolvedor_cnpj"])) == 14 else "2",
                            _so_digitos(cfg["desenvolvedor_cnpj"]), (cfg.get("desenvolvedor_nome") or e["razao_social"])[:150],
                            (cfg.get("desenvolvedor_email") or "")[:50]))
    linhas.append(aej_linha("99", 1, 1, len(vinculos), len(horarios_linhas), len(marcacoes), 0, len(ausencias), 1))
    linhas.append(ASSINATURA)
    nome = f"AEJ{_so_digitos(e['cnpj'])}_{inicio:%Y%m%d}_{fim:%Y%m%d}.txt"
    return nome, (FIM.join(linhas) + FIM).encode("latin-1", errors="replace")
