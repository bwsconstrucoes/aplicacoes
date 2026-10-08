# -*- coding: utf-8 -*-
"""
QUEM PODE O QUÊ NO APLICATIVO DO PONTO (/ponto/app), e QUEM CADA UM ENXERGA.

Pedido do dono, 08/10/2026: uma tela inicial e, dali, três caminhos — bater o
ponto, consultar o ponto das pessoas, e incluir atestado, afastamento, ajuste,
compensação. *"O ideal é que eu só possa consultar colaboradores que têm algum
ponto batido naquela obra (...) é permitido visualizar aquele celular que está
dentro daquela cerca."*

OS PAPÉIS
  PONTO DA OBRA (aparelho COMPARTILHADO)  bate para todos, sem login. Consultar e
      pedir POR OUTROS pede o CPF e o PIN do responsável pelo aparelho (ou de um
      administrativo de obra): o aparelho fica na obra, à mão de qualquer um, e
      a consulta mostra o mês das pessoas. Vale para a obra cuja CERCA contém o
      aparelho naquele momento.
  ADMINISTRATIVO DE OBRA (marcação na pessoa, `colaborador_config.
      administrativo_obra`)  em qualquer aparelho — o celular dele, o ponto da
      obra, o computador — consulta e pede pelas pessoas da obra em que ESTÁ:
        · pela localização: a obra cuja cerca contém o aparelho agora;
        · MAIS as obras em que ele bateu ponto nos últimos DIAS_RECENTES dias —
          é o que vale no computador, que não tem localização, e a qualquer
          hora. Decisão do dono, 08/10/2026: "acho que pode afrouxar mais (...)
          se precisar lançar algo fora do horário, deixa". (A primeira versão,
          do mesmo dia, só valia com o ponto dele aberto naquele momento.)
      Mudou de obra? Vê a nova quando bater nela (ou estiver dentro dela); a
      antiga sai sozinha DIAS_RECENTES dias depois da última batida lá. O
      próprio ponto ele vê sempre, e pede por si mesmo.
  PONTO DE EQUIPE (aparelho LISTA)  o responsável vê a equipe da lista e pede
      por ela só se tiver a marcação "faz pedidos pelo celular".
  CELULAR PESSOAL  cada um vê o próprio ponto. Pedido pelo próprio celular só
      com a marcação "faz pedidos pelo celular" (`pede_no_celular`); sem ela, o
      atestado é entregue no ponto da obra ou pelo administrativo.

QUEM APARECE NA CONSULTA de uma obra, num mês: quem bateu ponto nela naquele mês
(mesmo que tenha batido outros dias em outra obra), MAIS quem é da obra no
cadastro. O segundo grupo cobre o furo de quem passou o mês inteiro de
atestado, ou chegou hoje: sem batida nenhuma, não haveria como lançar nada
para ele. A pessoa aparece com o mês inteiro — é a folha dela.

O QUE NÃO SE VÊ: CID, médico e o documento do atestado continuam só do DP
(`ocorrencias._para_json`); a consulta mostra "Atestado" e os dias.

Antes da migração 008, ninguém é administrativo, e pedido pelo celular segue a
regra de 06/10 (quem bate no próprio celular pede por ele).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from . import cadastros, competencias, dispositivos, forma_de_bater, geo

DE_ONDE_CERCA = "CERCA"
DE_ONDE_BATIDAS = "BATIDAS"
# Por quantos dias a obra em que o administrativo bateu ponto continua ao alcance
# dele sem localização (o computador). Uma semana cobre o fim de semana e a
# folga; mais que isso, a obra antiga demoraria a sair.
DIAS_RECENTES = 7
DE_ONDE_EQUIPE = "EQUIPE"


def disponivel(conn: Connection) -> bool:
    return db.tem_coluna(conn, "colaborador_config", "administrativo_obra")


def e_administrativo(conn: Connection, pessoa: Optional[dict]) -> bool:
    return bool(pessoa) and disponivel(conn) and bool(pessoa.get("administrativo_obra"))


def pede_pelo_proprio_celular(conn: Connection, pessoa: dict) -> bool:
    """A pessoa faz pedido para SI pelo celular dela?"""
    if not disponivel(conn):
        return forma_de_bater.pode_no_celular(pessoa, forma_de_bater.em_vigor(conn))
    return bool(pessoa.get("pede_no_celular") or pessoa.get("administrativo_obra"))


def definir(conn: Connection, colaborador_id: int, *, pede_no_celular: Optional[bool] = None,
            administrativo_obra: Optional[bool] = None) -> None:
    from ..erros import ErroDeValidacao
    if not disponivel(conn):
        raise ErroDeValidacao("aplique as atualizações do ponto (migração 008) antes de marcar isto")
    db.executar(conn, """
        INSERT INTO ponto.colaborador_config (colaborador_id, pede_no_celular, administrativo_obra)
        VALUES (:c, COALESCE(:p, FALSE), COALESCE(:a, FALSE))
        ON CONFLICT (colaborador_id) DO UPDATE SET
            pede_no_celular = COALESCE(:p, ponto.colaborador_config.pede_no_celular),
            administrativo_obra = COALESCE(:a, ponto.colaborador_config.administrativo_obra),
            atualizado_em = now()
    """, c=colaborador_id, p=pede_no_celular, a=administrativo_obra)


# ---------------------------------------------------------------------------
# O alcance: de quem a pessoa logada pode ver o ponto e por quem pode pedir
# ---------------------------------------------------------------------------
@dataclass
class Alcance:
    obras: dict = field(default_factory=dict)      # obra_id → {"id", "codigo", "nome"}
    equipe: set = field(default_factory=set)       # pessoas fixas (ponto de equipe)
    de_onde: list = field(default_factory=list)    # frases para a tela
    pode_pedir: bool = False                       # pede POR OUTROS
    papel: Optional[str] = None                    # para a tela: quem a pessoa é aqui
    sem_alcance: Optional[str] = None              # por que não vê ninguém

    @property
    def vazio(self) -> bool:
        return not self.obras and not self.equipe


def aparelho_valendo(a: Optional[dict]) -> bool:
    if not a or a.get("status") != "APROVADO":
        return False
    v = dispositivos.vencimento(a)
    return not (v and v["vencido"])


def obras_recentes(conn: Connection, colaborador_id: int, hoje: Optional[dt.date] = None) -> list[int]:
    """As obras em que a pessoa bateu ponto nos últimos DIAS_RECENTES dias, da
    batida mais recente para a mais antiga."""
    hoje = hoje or horario.hoje()
    return [int(l["obra_id"]) for l in db.todos(conn, """
        SELECT obra_id, max(timestamp_servidor) AS ultima FROM ponto.marcacoes
         WHERE colaborador_id = :c AND data_referencia BETWEEN :i AND :f AND status <> 'REJEITADA'
         GROUP BY obra_id ORDER BY ultima DESC""", c=colaborador_id,
        i=hoje - dt.timedelta(days=DIAS_RECENTES - 1), f=hoje)]


def _obra_da_cerca(conn: Connection, local: Optional[dict], so_estas: Optional[set] = None) -> Optional[dict]:
    if not local or not geo.coordenada_valida(local.get("latitude"), local.get("longitude")):
        return None
    candidatas = [o for o in cadastros.listar_obras(conn, so_ativas=True)
                  if not so_estas or int(o["id"]) in so_estas]
    situacao, obra, _ = geo.localizar_obra(local["latitude"], local["longitude"], local.get("precisao"),
                                           candidatas)
    return obra if situacao in (geo.DENTRO, geo.BORDA) else None


def _curta(o: dict) -> dict:
    return {"id": int(o["id"]), "codigo": o["codigo"], "nome": o["nome"]}


def alcance(conn: Connection, pessoa: Optional[dict], aparelho: Optional[dict],
            local: Optional[dict], *, hoje: Optional[dt.date] = None) -> Alcance:
    """O que a pessoa logada (`pessoa`, ou None no ponto da obra sem login)
    alcança, NESTE aparelho e NESTE lugar."""
    r = Alcance()
    if not pessoa:
        r.sem_alcance = "entre com o CPF e o PIN do responsável por este aparelho"
        return r
    admin = e_administrativo(conn, pessoa)
    meu = (aparelho_valendo(aparelho) and aparelho.get("perfil") != "INDIVIDUAL"
           and aparelho.get("colaborador_id") == pessoa["id"])
    if admin:
        r.papel = "ADMINISTRATIVO"
    elif meu:
        r.papel = "RESPONSAVEL_OBRA" if aparelho["perfil"] == "COMPARTILHADO" else "RESPONSAVEL_EQUIPE"

    # 1. A cerca: o administrativo em qualquer aparelho; o responsável no ponto
    #    da obra dele (nas obras do aparelho, se ele tiver lista).
    if admin or (meu and aparelho["perfil"] == "COMPARTILHADO"):
        so_estas = None if admin else (dispositivos.obras_de(conn, aparelho["id"]) or None)
        o = _obra_da_cerca(conn, local, so_estas)
        if o:
            r.obras[int(o["id"])] = _curta(o)
            r.de_onde.append(f"pela localização: dentro da obra {o['codigo']}")
    # 2. As obras em que ele bateu ponto nos últimos dias (o computador; e
    #    também fora do horário — decisão do dono, 08/10/2026).
    if admin:
        for oid in obras_recentes(conn, pessoa["id"], hoje):
            if oid in r.obras:
                continue
            o = cadastros.obra_por_id(conn, oid)
            if o:
                r.obras[oid] = _curta(o)
                r.de_onde.append(f"pelas suas batidas dos últimos {DIAS_RECENTES} dias na obra {o['codigo']}")
    # 3. A equipe do ponto de equipe.
    if meu and aparelho["perfil"] == "LISTA":
        r.equipe = set(dispositivos.autorizados_de(conn, aparelho["id"])) - {pessoa["id"]}
        if r.equipe:
            r.de_onde.append(f"a equipe deste aparelho ({len(r.equipe)} pessoa(s))")

    r.pode_pedir = bool(admin or (meu and aparelho["perfil"] == "COMPARTILHADO")
                        or (meu and aparelho["perfil"] == "LISTA" and pede_pelo_proprio_celular(conn, pessoa)))
    if r.vazio:
        if admin:
            r.sem_alcance = (f"você não está dentro da área de nenhuma obra nem bateu ponto em obra nos últimos "
                             f"{DIAS_RECENTES} dias — a consulta vale nas obras em que você trabalha")
        elif meu and aparelho["perfil"] == "COMPARTILHADO":
            r.sem_alcance = "o aparelho não está dentro da área de nenhuma obra dele — confira a localização"
        else:
            r.sem_alcance = "a consulta do ponto de outras pessoas é do administrativo e do ponto da obra"
    return r


def pessoas_do_alcance(conn: Connection, a: Alcance, competencia=None, *, exceto: Optional[int] = None) -> list[dict]:
    """Quem aparece na consulta, no mês: batidas na obra no mês + cadastrados na
    obra (ativos) + a equipe fixa. Com quantas batidas cada um tem ali no mês.
    `exceto`: quem consulta — o próprio ponto dele fica no "Meu ponto"."""
    if a.vazio:
        return []
    inicio = competencias.primeiro_dia(competencia or horario.hoje())
    fim = competencias.ultimo_dia(inicio)
    obras = sorted(a.obras)
    batidas: dict[int, int] = {}
    if obras:
        for l in db.todos(conn, """
            SELECT colaborador_id, count(*) AS n FROM ponto.marcacoes
             WHERE obra_id = ANY(:o) AND data_referencia BETWEEN :i AND :f AND status <> 'REJEITADA'
             GROUP BY colaborador_id""", o=obras, i=inicio, f=fim):
            batidas[int(l["colaborador_id"])] = int(l["n"])
    todas = cadastros.listar_colaboradores(conn, so_ativos=False)
    saida = []
    for p in todas:
        pid = int(p["id"])
        cadastrado = p["situacao"] != "DESLIGADO" and bool(p.get("ativo_no_ponto", True)) and (
            (p.get("obra_id") in a.obras) or any(o["id"] in a.obras for o in p.get("obras_adicionais", [])))
        if pid == exceto:
            continue
        if pid in batidas or cadastrado or pid in a.equipe:
            saida.append({"id": pid, "nome": p["nome"], "funcao": p.get("funcao"),
                          "obra": p.get("obra_codigo"), "cpf_final": (p.get("cpf") or "")[-3:],
                          "batidas_na_obra": batidas.get(pid, 0),
                          "situacao": p["situacao"]})
    saida.sort(key=lambda x: x["nome"])
    return saida


def no_alcance(conn: Connection, a: Alcance, colaborador_id: int, *datas) -> bool:
    """A pessoa está na consulta — no mês de hoje ou no mês de alguma das datas?"""
    meses = {competencias.primeiro_dia(horario.hoje())} | {competencias.primeiro_dia(d) for d in datas if d}
    return any(int(colaborador_id) in {p["id"] for p in pessoas_do_alcance(conn, a, m)} for m in meses)
