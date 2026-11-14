# -*- coding: utf-8 -*-
"""
A BUSCA AUTOMÁTICA DE FERIADOS — nacional, estadual e municipal, pelas obras em andamento.

Pedido do dono, 05/11/2026 (no chat do Análise de SPs, que desde 04/11 lê os feriados
DAQUI): *"checar quais feriados temos a nível nacional, quais a nível estadual e quais a
nível municipal. O nacional é nacional. Estadual, baseado nas obras que estão em
andamento. E municipal também (…) cadastrou-se uma obra nova, talvez esse seria o
momento do start, além de ser por ano (…) obra concluída, não precisaria buscar mais."*

O QUE ELA FAZ, por ano (o corrente e, a partir de 1º de dezembro, o seguinte):
  · NACIONAL: calculado AQUI, sem internet — a mesma lista fixa que a migração 002
    semeou (leis 662/1949, 6.802/1980 e 14.759/2023). Carnaval, Paixão de Cristo e
    Corpus Christi continuam de fora, pelo mesmo motivo de lá: são ponto facultativo
    ou feriado municipal, e quem decide é o DP.
  · ESTADUAL: para cada UF com obra ATIVA no ponto.
  · MUNICIPAL: para cada município (UF + nome) com obra ATIVA no ponto.
  Estadual e municipal vêm da base pública "feriados-brasil" (GitHub joaopbini,
  licença MIT, arquivos JSON por ano, municípios pelo código do IBGE).

QUANDO BUSCA (`buscar_se_preciso`): SÓ DE MADRUGADA, das 2h às 5h (13/11/2026, o dono: *"o ideal seria rodar
apenas na madrugada"*). Quem chama é o relógio das rondas (`rondas._volta_do_relogio`), que roda sozinho no
serviço o tempo todo — de madrugada ninguém abre tela para acordar rotina nenhuma. As outras chamadas automáticas
(as planilhas de apoio do Análise de SPs e a leitura da C. Diários) continuam no código e, fora da janela, não
fazem nada: assim a regra mora num lugar só, sem mexer na área do Análise de SPs. O botão "Buscar agora" (Análise
de SPs › Folha › Calendário) chama `buscar` direto e vale a qualquer hora. A conferência é barata (uma consulta e
um parâmetro), no máximo uma por hora. Só vai à internet quando há o que buscar: ANO NOVO ainda não buscado, ou
UF/município de obra ativa que ainda não foi buscado naquele ano — é a obra nova em lugar novo, que agora ganha
os feriados na madrugada seguinte (antes, em até uma hora). Obra concluída sai da lista de obras ativas e o lugar
dela deixa de ser buscado; o que já foi gravado fica (só vale para obra daquele lugar, e não atrapalha).

⚠️ O QUE FOI EXCLUÍDO À MÃO NÃO VOLTA. Um lugar buscado num ano fica anotado como
buscado (parâmetro `feriados.busca`), e não é buscado de novo naquele ano. Se o DP
apagar um feriado municipal que a base trouxe errado, ele não reaparece.

⚠️ NADA SOBRESCREVE: a gravação é `ON CONFLICT DO NOTHING` pela chave do ponto (data,
abrangência, UF, município, obra). Feriado cadastrado à mão no mesmo dia e lugar fica
como está.

⚠️ O PASSADO NÃO MUDA: só se grava feriado a partir do 1º dia do MÊS ANTERIOR (a folha
de um mês se fecha no começo do seguinte). Um feriado de março gravado em novembro
mudaria o auxílio de março recalculado e viraria em feriado a falta de um mês já
fechado no ponto. Feriado passado que faltou, o DP cadastra à mão.

⚠️ A BASE É COMUNITÁRIA. Ela não diz a fonte de cada feriado, e o município que não
tem feriado lá simplesmente não ganha nenhum — a tela mostra quais municípios ficaram
sem feriado e quais não foram reconhecidos (nome diferente do IBGE), para o DP
conferir. Ano que a base ainda não publicou fica pendente e é tentado de novo depois.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db
from . import parametros
from .feriados import _sem_acento

logger = logging.getLogger("ponto.feriados_busca")

PARAMETRO = "feriados.busca"
FONTE = "https://raw.githubusercontent.com/joaopbini/feriados-brasil/master/dados"
URL_ESTADUAL = FONTE + "/feriados/estadual/json/{ano}.json"
URL_MUNICIPAL = FONTE + "/feriados/municipal/json/{ano}.json"
URL_MUNICIPIOS = FONTE + "/localizacao/municipios/municipios.json"
# Teto de cada arquivo (o municipal de um ano tem ~1,7 MB). A instância divide 2 GB com
# o monorepo inteiro e já morreu de falta de memória (CONTEXTO §3.7): download sem teto,
# nunca.
TETO_BYTES = 15 * 1024 * 1024
ESPERA_S = 60
# Ano que a base ainda não publicou: tenta de novo depois deste intervalo.
RETENTAR_ANO_S = 7 * 24 * 3600
# Conferência sem nada novo: no máximo uma vez a cada hora.
CONFERIR_A_CADA_S = 3600
JANELA_DA_MADRUGADA = (2, 5)     # das 2h às 5h, hora de Fortaleza (13/11/2026)


def na_madrugada(momento: Optional[dt.datetime] = None) -> bool:
    """PURA (com o relógio). O instante cai na janela da busca automática?"""
    from .. import horario
    local = horario.para_local(momento or horario.agora())
    return JANELA_DA_MADRUGADA[0] <= local.hour < JANELA_DA_MADRUGADA[1]

NACIONAIS = (
    (1, 1, "Confraternização Universal"), (4, 21, "Tiradentes"), (5, 1, "Dia do Trabalho"),
    (9, 7, "Independência do Brasil"), (10, 12, "Nossa Senhora Aparecida"),
    (11, 2, "Finados"), (11, 15, "Proclamação da República"),
    (11, 20, "Dia Nacional de Zumbi e da Consciência Negra"), (12, 25, "Natal"),
)

# Os dois primeiros dígitos do código IBGE do município são o código da UF.
UF_DO_CODIGO = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}


class AnoNaoPublicado(RuntimeError):
    """A base ainda não tem o arquivo deste ano (404)."""


# ---------------------------------------------------------------------------
# Internet (isolada: os testes trocam esta função)
# ---------------------------------------------------------------------------
def baixar_json(url: str):
    """Baixa um JSON com teto de tamanho. 404 vira `AnoNaoPublicado`."""
    import requests
    with requests.get(url, timeout=ESPERA_S, stream=True) as r:
        if r.status_code == 404:
            raise AnoNaoPublicado(url)
        r.raise_for_status()
        partes, total = [], 0
        for pedaco in r.iter_content(64 * 1024):
            total += len(pedaco)
            if total > TETO_BYTES:
                raise RuntimeError(f"arquivo maior que o teto de {TETO_BYTES // 1024 // 1024} MB: {url}")
            partes.append(pedaco)
    return json.loads(b"".join(partes).decode("utf-8"))


# ---------------------------------------------------------------------------
# PURAS
# ---------------------------------------------------------------------------
def nacionais(ano: int) -> list[dict]:
    return [{"data": dt.date(ano, m, d), "nome": n} for m, d, n in NACIONAIS]


def _data(texto) -> Optional[dt.date]:
    try:
        d, m, a = str(texto).strip().split("/")
        return dt.date(int(a), int(m), int(d))
    except (ValueError, AttributeError):
        return None


def lugares(obras: list[dict]) -> dict:
    """{'ufs': {UF}, 'municipios': {(UF, MUNICIPIO_NORMAL): nome como está}} das obras."""
    ufs, municipios = set(), {}
    for o in obras:
        uf = _sem_acento(o.get("uf"))
        if len(uf) != 2:
            continue
        ufs.add(uf)
        mun = _sem_acento(o.get("municipio"))
        if mun:
            municipios.setdefault((uf, mun), str(o.get("municipio")).strip())
    return {"ufs": ufs, "municipios": municipios}


def codigos_ibge(municipios_json: list[dict]) -> dict:
    """{(UF, MUNICIPIO_NORMAL): codigo_ibge} da lista de municípios da base."""
    saida = {}
    for m in municipios_json or []:
        codigo = str(m.get("codigo_ibge") or "")
        uf = UF_DO_CODIGO.get(codigo[:2])
        if uf and m.get("nome"):
            saida[(uf, _sem_acento(m["nome"]))] = codigo
    return saida


# ---------------------------------------------------------------------------
# Estado guardado
# ---------------------------------------------------------------------------
def estado(conn: Connection) -> dict:
    try:
        bruto = json.loads(parametros.ler(conn, PARAMETRO, "") or "{}")
    except ValueError:
        bruto = {}
    bruto.setdefault("anos", {})
    return bruto


def _gravar_estado(conn: Connection, est: dict, por: str) -> None:
    parametros.gravar(conn, PARAMETRO, json.dumps(est, ensure_ascii=False, default=str), por)


def _anos_alvo(hoje: dt.date) -> list[int]:
    return [hoje.year, hoje.year + 1] if hoje.month == 12 else [hoje.year]


def _obras_ativas(conn: Connection) -> list[dict]:
    from . import cadastros
    return cadastros.listar_obras(conn, so_ativas=True)


def o_que_falta(conn: Connection, hoje: dt.date) -> dict:
    """{ano: {'nacional': bool, 'ufs': [...], 'municipios': [(uf, mun)]}} — só o que falta."""
    est, onde = estado(conn), lugares(_obras_ativas(conn))
    agora = time.time()
    falta = {}
    for ano in _anos_alvo(hoje):
        feito = est["anos"].get(str(ano), {})
        if feito.get("nao_publicado_em") and agora - float(feito["nao_publicado_em"]) < RETENTAR_ANO_S:
            continue
        ufs = sorted(onde["ufs"] - set(feito.get("ufs", [])))
        ja = {tuple(x) for x in feito.get("municipios", [])}
        muns = sorted(k for k in onde["municipios"] if k not in ja)
        if not feito.get("nacional") or ufs or muns:
            falta[ano] = {"nacional": not feito.get("nacional"), "ufs": ufs, "municipios": muns,
                          "nomes": {f"{u}|{m}": onde["municipios"][(u, m)] for u, m in muns}}
    return falta


# ---------------------------------------------------------------------------
# A busca
# ---------------------------------------------------------------------------
def corte(hoje: dt.date) -> dt.date:
    """O primeiro dia do mês anterior: antes dele, a busca não grava nada."""
    return (hoje.replace(day=1) - dt.timedelta(days=1)).replace(day=1)


def _gravar(conn: Connection, data, nome, abrangencia, uf=None, municipio=None,
            desde: Optional[dt.date] = None) -> bool:
    if desde and data < desde:
        return False
    linha = db.um(conn, """
        INSERT INTO ponto.feriados (data, nome, abrangencia, uf, municipio)
        VALUES (:d, :n, :a, :uf, :m) ON CONFLICT DO NOTHING RETURNING id
    """, d=data, n=str(nome or "Feriado")[:120], a=abrangencia, uf=uf, m=municipio)
    return bool(linha)


def buscar(conn: Connection, hoje: dt.date, por: str = "", forcar: bool = False) -> dict:
    """Busca o que falta (ou tudo, com `forcar`) e grava. Devolve o resumo da rodada."""
    est = estado(conn)
    if forcar:
        for ano in _anos_alvo(hoje):
            est["anos"].pop(str(ano), None)
        _gravar_estado(conn, est, por)
    falta = o_que_falta(conn, hoje)
    resumo = {"anos": {}, "gravados": 0, "avisos": []}
    municipios_json = None
    desde = corte(hoje)
    for ano, item in falta.items():
        feito = est["anos"].setdefault(str(ano), {})
        r = {"nacional": 0, "estadual": 0, "municipal": 0,
             "sem_feriado": [], "nao_reconhecidos": []}
        if item["nacional"]:
            r["nacional"] = sum(_gravar(conn, f["data"], f["nome"], "NACIONAL", desde=desde)
                                for f in nacionais(ano))
            feito["nacional"] = True
        try:
            if item["ufs"]:
                for f in baixar_json(URL_ESTADUAL.format(ano=ano)):
                    uf = _sem_acento(f.get("uf"))
                    data = _data(f.get("data"))
                    if uf in item["ufs"] and data and data.year == ano:
                        r["estadual"] += _gravar(conn, data, f.get("nome"), "ESTADUAL", uf=uf, desde=desde)
                feito["ufs"] = sorted(set(feito.get("ufs", [])) | set(item["ufs"]))
            if item["municipios"]:
                if municipios_json is None:
                    municipios_json = codigos_ibge(baixar_json(URL_MUNICIPIOS))
                por_codigo = {}
                for uf, mun in item["municipios"]:
                    codigo = municipios_json.get((uf, mun))
                    if codigo:
                        por_codigo[codigo] = (uf, item["nomes"][f"{uf}|{mun}"])
                    else:
                        r["nao_reconhecidos"].append(f"{item['nomes'][f'{uf}|{mun}']}/{uf}")
                com_feriado = set()
                for f in baixar_json(URL_MUNICIPAL.format(ano=ano)):
                    codigo = str(f.get("codigo_ibge") or "")
                    data = _data(f.get("data"))
                    if codigo in por_codigo and data and data.year == ano:
                        uf, nome_mun = por_codigo[codigo]
                        com_feriado.add(codigo)
                        r["municipal"] += _gravar(conn, data, f.get("nome"), "MUNICIPAL",
                                                  uf=uf, municipio=nome_mun, desde=desde)
                r["sem_feriado"] = sorted(f"{n}/{u}" for c, (u, n) in por_codigo.items()
                                          if c not in com_feriado)
                feito["municipios"] = sorted({tuple(x) for x in feito.get("municipios", [])}
                                             | set(item["municipios"]))
            feito.pop("nao_publicado_em", None)
        except AnoNaoPublicado:
            feito["nao_publicado_em"] = time.time()
            resumo["avisos"].append(f"a base pública ainda não publicou os feriados de {ano}; "
                                    "nova tentativa em uma semana.")
        feito["ultima"] = dt.datetime.now().isoformat(timespec="seconds")
        feito.setdefault("sem_feriado", [])
        feito["sem_feriado"] = sorted(set(feito["sem_feriado"]) | set(r["sem_feriado"]))
        feito["nao_reconhecidos"] = sorted(set(feito.get("nao_reconhecidos", [])) | set(r["nao_reconhecidos"]))
        resumo["anos"][ano] = r
        resumo["gravados"] += r["nacional"] + r["estadual"] + r["municipal"]
    est["conferido_em"] = time.time()
    _gravar_estado(conn, est, por)
    if falta:
        logger.info("Ponto: busca de feriados (%s) — %s", por or "?", json.dumps(resumo, default=str))
    return resumo


def buscar_se_preciso(por: str = "automático", sempre: bool = False) -> Optional[dict]:
    """A conferência barata das rodadas automáticas. None quando não havia nada a fazer.
    Nunca levanta: feriado que não veio agora vem na próxima rodada.

    `sempre`: confere mesmo dentro da hora — é o caso da base de obras acabada de ler,
    quando pode ter entrado obra em lugar novo."""
    from .. import horario
    if not na_madrugada():
        return None                      # só de madrugada (13/11/2026); o botão vale a qualquer hora
    try:
        with db.conexao() as conn:
            if not db.schema_existe(conn):
                return None
            est = estado(conn)
            if not sempre and time.time() - float(est.get("conferido_em") or 0) < CONFERIR_A_CADA_S:
                return None
            hoje = horario.agora().date()
            if not o_que_falta(conn, hoje):
                est["conferido_em"] = time.time()
                _gravar_estado(conn, est, por)
                return None
            return buscar(conn, hoje, por)
    except Exception:  # noqa: BLE001
        logger.exception("Ponto: a busca automática de feriados falhou; tenta de novo depois")
        return None


def situacao(conn: Connection, hoje: dt.date) -> dict:
    """Para a tela: o que já foi buscado em cada ano e o que falta."""
    est = estado(conn)
    anos = {}
    for ano in sorted({*(int(a) for a in est["anos"]), *_anos_alvo(hoje)}):
        feito = est["anos"].get(str(ano), {})
        anos[ano] = {"ufs": feito.get("ufs", []),
                     "municipios": [f"{m}/{u}" for u, m in feito.get("municipios", [])],
                     "sem_feriado": feito.get("sem_feriado", []),
                     "nao_reconhecidos": feito.get("nao_reconhecidos", []),
                     "nao_publicado": bool(feito.get("nao_publicado_em")),
                     "ultima": feito.get("ultima")}
    return {"anos": anos, "falta": o_que_falta(conn, hoje)}
