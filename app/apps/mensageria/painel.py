# -*- coding: utf-8 -*-
"""
O PAINEL DAS MENSAGENS — o que saiu, para quem, por onde, quando.

Pedido do dono em 14/11/2026: *"uma espécie de dashboard com monitoramento,
com o tipo de mensagem, para quem e o que, com filtragens na sidebar (…) com
gráficos, para a gente ver como é que está se comportando o envio das
mensagens por dia, por período, por horário (…) tanto via Telegram como
WhatsApp."*

Tudo sai do registro `mensageria.envios` (90 dias). A PESSOA de cada linha é
achada pelo CPF, pelo telefone (com ou sem 55 e nono dígito) ou pelo id do
chat do Telegram, contra o cadastro do ERP (colaboradores e operadores) e o
cadastro do Telegram. Linha que não casa com ninguém aparece pelo telefone.

Hora e dia são os de Fortaleza.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Optional

from . import core, db, telegram_cadastro as tc

FUSO_SQL = "America/Fortaleza"
ENTREGUE = "ENVIADO"
NAO_CHEGOU = ("SEM_DESTINO", "FALHOU")
BARRADO = ("DESLIGADO", "LIMITE", "BARRADO")
MODULOS = {
    "ponto": "Ponto", "erp": "ERP", "analisesps": "Análise de SPs", "telegram": "Telegram",
    "mensageria": "Mensagens", "externo": "Make e emissão de nota", "sem_tipo": "Módulos antigos",
}


def _data(valor) -> Optional[dt.date]:
    try:
        return dt.date.fromisoformat(str(valor)[:10]) if valor else None
    except ValueError:
        return None


def _hoje() -> dt.date:
    return dt.datetime.now(core.FUSO).date()


def periodo(de=None, ate=None, dias=None) -> tuple[dt.date, dt.date]:
    """De/até em dias de Fortaleza, no máximo os 90 dias do registro."""
    hoje = _hoje()
    ate_d = _data(ate) or hoje
    de_d = _data(de) or (ate_d - dt.timedelta(days=max(1, int(dias or 7)) - 1))
    de_d = max(de_d, hoje - dt.timedelta(days=core.RETENCAO_DIAS))
    if de_d > ate_d:
        de_d, ate_d = ate_d, de_d
    return de_d, ate_d


class Pessoas:
    """Acha a pessoa de uma linha do registro."""

    def __init__(self, conn):
        self.por_cpf, self.por_tel, self.por_chat, self.nomes = {}, {}, {}, {}
        for p in tc.pessoas_elegiveis(conn):
            self._guardar(p["chave"], p["nome"], p["cpf"], p["telefone"])
        # Para dar NOME a quem recebeu, vale todo colaborador (sem telefone,
        # desligado) e todo operador — não só quem pode se cadastrar hoje.
        for c in db.todos(conn, "SELECT id, nome, cpf, telefone FROM public.colaboradores"):
            cpf = tc.normalizar_cpf(c["cpf"])
            if cpf and cpf not in self.por_cpf:
                self._guardar(f"C{c['id']}", (c["nome"] or "").strip(), cpf, "")
        for u in db.todos(conn, "SELECT id, nome, cpf FROM public.usuarios"):
            cpf = tc.normalizar_cpf(u["cpf"])
            if cpf and cpf not in self.por_cpf:
                self._guardar(f"U{u['id']}", (u["nome"] or "").strip(), cpf, "")
        for c in db.todos(conn, "SELECT chat_id, cpf, nome, telefone FROM mensageria.telegram_cadastros"):
            chave = self.por_cpf.get(tc.normalizar_cpf(c["cpf"])) if c["cpf"] else None
            if chave:
                self.por_chat[str(c["chat_id"])] = chave
            elif c["nome"]:
                self.nomes.setdefault(f"T{c['chat_id']}", c["nome"])
                self.por_chat[str(c["chat_id"])] = f"T{c['chat_id']}"

    def _guardar(self, chave, nome, cpf, telefone):
        self.nomes[chave] = nome
        if cpf:
            self.por_cpf[cpf] = chave
        for v in tc.variantes_telefone(telefone):
            self.por_tel[v] = chave

    def chave(self, cpf: str, destinatario: str) -> str:
        cpf_n = tc.normalizar_cpf(cpf) if cpf else ""
        if cpf_n and cpf_n in self.por_cpf:
            return self.por_cpf[cpf_n]
        d = str(destinatario or "")
        if d in self.por_chat:
            return self.por_chat[d]
        for v in tc.variantes_telefone(d):
            if v in self.por_tel:
                return self.por_tel[v]
        return "?" + (cpf_n or tc.digitos(d) or "sem destinatário")

    def nome(self, chave: str) -> str:
        if chave.startswith("?"):
            resto = chave[1:]
            return f"Fora do cadastro ({resto})" if resto != "sem destinatário" else "Sem destinatário"
        return self.nomes.get(chave, chave)

    def destinos_da_busca(self, busca: str) -> tuple[list[str], list[str]]:
        """CPFs e destinos (telefones, chats) de quem casa com a busca."""
        b = (busca or "").strip().lower()
        bd = tc.digitos(busca)
        chaves = {c for c, n in self.nomes.items() if b and b in (n or "").lower()}
        if bd:
            chaves |= {c for cpf, c in self.por_cpf.items() if bd in cpf}
            chaves |= {c for v, c in self.por_tel.items() if bd in v}
        cpfs = [cpf for cpf, c in self.por_cpf.items() if c in chaves]
        destinos = [chat for chat, c in self.por_chat.items() if c in chaves]
        for v, c in self.por_tel.items():
            if c in chaves:
                destinos += [v, "55" + v]
        if bd and len(bd) >= 8:
            destinos.append(bd)
        return cpfs or ["~"], destinos or ["~"]


def _filtro(f: dict, pessoas: Pessoas) -> tuple[str, dict]:
    de, ate = periodo(f.get("de"), f.get("ate"), f.get("dias"))
    sql = [f"(criado_em AT TIME ZONE '{FUSO_SQL}')::date BETWEEN :de AND :ate"]
    p: dict = {"de": de, "ate": ate}
    if f.get("canal") in ("telegram", "whatsapp"):
        sql.append("canal = :canal"); p["canal"] = f["canal"]
    if f.get("tipo"):
        sql.append("tipo = :tipo"); p["tipo"] = core.normalizar_chave(f["tipo"]) or f["tipo"]
    if f.get("modulo") in MODULOS:
        sql.append("split_part(tipo, '.', 1) = :modulo"); p["modulo"] = f["modulo"]
    situacao = f.get("situacao") or ""
    if situacao == "entregue":
        sql.append("status = 'ENVIADO'")
    elif situacao == "nao_chegou":
        sql.append("status IN ('SEM_DESTINO', 'FALHOU')")
    elif situacao == "barrado":
        sql.append("status IN ('DESLIGADO', 'LIMITE', 'BARRADO')")
    elif situacao:
        sql.append("status = :status"); p["status"] = situacao.upper()[:20]
    if (f.get("pessoa") or "").strip():
        cpfs, destinos = pessoas.destinos_da_busca(f["pessoa"])
        sql.append("(cpf = ANY(:cpfs) OR destinatario = ANY(:destinos))")
        p["cpfs"], p["destinos"] = cpfs, destinos
    return " AND ".join(sql), {**p, "_de": de, "_ate": ate}


def dados(conn, f: dict) -> dict:
    pessoas = Pessoas(conn)
    onde, p = _filtro(f, pessoas)
    de, ate = p.pop("_de"), p.pop("_ate")
    local = f"(criado_em AT TIME ZONE '{FUSO_SQL}')"

    por_status = db.todos(conn, f"SELECT canal, status, count(*) AS n FROM mensageria.envios WHERE {onde} "
                                "GROUP BY canal, status", **p)
    def soma(cond):
        return sum(int(r["n"]) for r in por_status if cond(r))
    total = soma(lambda r: True)
    kpis = {"total": total,
            "entregues": soma(lambda r: r["status"] == ENTREGUE),
            "nao_chegaram": soma(lambda r: r["status"] in NAO_CHEGOU),
            "barradas": soma(lambda r: r["status"] in BARRADO),
            "telegram": soma(lambda r: r["canal"] == "telegram" and r["status"] == ENTREGUE),
            "whatsapp": soma(lambda r: r["canal"] == "whatsapp" and r["status"] == ENTREGUE)}
    kpis["taxa_entrega"] = round(100 * kpis["entregues"] / total) if total else None

    dias_rows = db.todos(conn, f"""SELECT {local}::date AS dia, canal, status, count(*) AS n
                                     FROM mensageria.envios WHERE {onde} GROUP BY 1, 2, 3""", **p)
    serie_dias = []
    d = de
    while d <= ate:
        linhas = [r for r in dias_rows if r["dia"] == d]
        serie_dias.append({"dia": d.isoformat(),
                           "telegram": sum(int(r["n"]) for r in linhas if r["canal"] == "telegram" and r["status"] == ENTREGUE),
                           "whatsapp": sum(int(r["n"]) for r in linhas if r["canal"] == "whatsapp" and r["status"] == ENTREGUE),
                           "nao_chegaram": sum(int(r["n"]) for r in linhas if r["status"] in NAO_CHEGOU),
                           "barradas": sum(int(r["n"]) for r in linhas if r["status"] in BARRADO)})
        d += dt.timedelta(days=1)

    horas_rows = db.todos(conn, f"""SELECT extract(hour FROM {local})::int AS hora, canal, count(*) AS n
                                      FROM mensageria.envios WHERE {onde} AND status = 'ENVIADO'
                                     GROUP BY 1, 2""", **p)
    serie_horas = [{"hora": h,
                    "telegram": sum(int(r["n"]) for r in horas_rows if r["hora"] == h and r["canal"] == "telegram"),
                    "whatsapp": sum(int(r["n"]) for r in horas_rows if r["hora"] == h and r["canal"] == "whatsapp")}
                   for h in range(24)]

    semana_rows = db.todos(conn, f"""SELECT extract(isodow FROM {local})::int AS dow, count(*) AS n
                                       FROM mensageria.envios WHERE {onde} AND status = 'ENVIADO' GROUP BY 1""", **p)
    nomes_dia = ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"]
    serie_semana = [{"dia": nomes_dia[i - 1], "n": sum(int(r["n"]) for r in semana_rows if r["dow"] == i)}
                    for i in range(1, 8)]

    tipos_rows = db.todos(conn, f"""SELECT tipo, canal, status, count(*) AS n FROM mensageria.envios
                                     WHERE {onde} GROUP BY 1, 2, 3""", **p)
    tipos = defaultdict(lambda: {"total": 0, "entregues": 0, "nao_chegaram": 0, "barradas": 0,
                                 "telegram": 0, "whatsapp": 0})
    for r in tipos_rows:
        t, n = tipos[r["tipo"] or "sem_tipo"], int(r["n"])
        t["total"] += n
        t["entregues"] += n if r["status"] == ENTREGUE else 0
        t["nao_chegaram"] += n if r["status"] in NAO_CHEGOU else 0
        t["barradas"] += n if r["status"] in BARRADO else 0
        if r["status"] == ENTREGUE and r["canal"] in ("telegram", "whatsapp"):
            t[r["canal"]] += n
    por_tipo = sorted(({"tipo": k, "nome": core.nome_do_tipo(k),
                        "modulo": MODULOS.get(k.split(".")[0], k.split(".")[0]), **v}
                       for k, v in tipos.items()), key=lambda x: -x["total"])

    grupo = db.todos(conn, f"""SELECT cpf, destinatario, tipo, canal, status, count(*) AS n, max(criado_em) AS ultima
                                 FROM mensageria.envios WHERE {onde} GROUP BY 1, 2, 3, 4, 5""", **p)
    gente = {}
    for r in grupo:
        chave = pessoas.chave(r["cpf"], r["destinatario"])
        g = gente.setdefault(chave, {"chave": chave, "nome": pessoas.nome(chave), "total": 0, "entregues": 0,
                                     "telegram": 0, "whatsapp": 0, "nao_chegaram": 0, "barradas": 0,
                                     "tipos": defaultdict(int), "ultima": None})
        n = int(r["n"])
        g["total"] += n
        if r["status"] == ENTREGUE:
            g["entregues"] += n
            if r["canal"] in ("telegram", "whatsapp"):
                g[r["canal"]] += n
        elif r["status"] in NAO_CHEGOU:
            g["nao_chegaram"] += n
        else:
            g["barradas"] += n
        g["tipos"][core.nome_do_tipo(r["tipo"])] += n
        g["ultima"] = max(filter(None, [g["ultima"], r["ultima"]]))
    por_pessoa = sorted(gente.values(), key=lambda g: (-g["total"], g["nome"]))
    for g in por_pessoa:
        g["tipos"] = sorted(({"nome": k, "n": v} for k, v in g["tipos"].items()), key=lambda x: -x["n"])
        g["ultima"] = g["ultima"].isoformat() if g["ultima"] else ""

    ultimas = db.todos(conn, f"""SELECT id, criado_em, tipo, canal, cpf, destinatario, status, detalhe, texto,
                                        nome_arquivo FROM mensageria.envios WHERE {onde}
                                  ORDER BY id DESC LIMIT 200""", **p)
    for u in ultimas:
        u["criado_em"] = u["criado_em"].isoformat()
        u["tipo_nome"] = core.nome_do_tipo(u["tipo"])
        u["pessoa"] = pessoas.nome(pessoas.chave(u["cpf"], u["destinatario"]))
        u["texto"] = (u["texto"] or "")[:300]

    opcoes_tipos = [{"chave": t["chave"], "nome": t["nome"], "modulo": t["modulo"]} for t in core.CATALOGO]
    for k, v in core.NOMES_FORA_DO_CATALOGO.items():
        opcoes_tipos.append({"chave": k, "nome": v, "modulo": "Outros"})
    return {"de": de.isoformat(), "ate": ate.isoformat(), "kpis": kpis, "por_status": por_status,
            "por_dia": serie_dias, "por_hora": serie_horas, "por_semana": serie_semana,
            "por_tipo": por_tipo, "por_pessoa": por_pessoa[:100], "pessoas_total": len(por_pessoa),
            "ultimas": ultimas, "opcoes": {"tipos": opcoes_tipos, "modulos": MODULOS}}
