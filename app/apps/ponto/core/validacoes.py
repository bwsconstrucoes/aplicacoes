# -*- coding: utf-8 -*-
"""
A lista de VALIDAÇÕES: tudo o que espera uma decisão de alguém, numa tela só.

Pedido do dono, 04/10/2026: *"uma tela onde liste tudo que está pendente para
validação (…) fulano tem ajuste, ciclano tem atestado (…) poder filtrar por
obra, período (…) poder ver os mosaicos também."*

Entram: os pedidos (ajuste, atestado, licença, compensação, folga, férias…),
as batidas em conferência (fora do normal, borda da cerca, sem foto, foto
suspeita…), os mosaicos obrigatórios sem conferência e — para quem configura o
ponto — os aparelhos esperando aprovação.

Cada linha diz QUEM valida (a regra de `validacao.py`) e se quem está olhando
pode decidir, e traz o endereço da decisão. O recorte por obra é o de sempre:
cada um vê só as obras dele; o atestado não mostra dado de saúde a quem não é
do DP.
"""
from __future__ import annotations

import datetime as dt
from collections import Counter
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db, horario
from ..erros import ErroDeValidacao
from . import dispositivos, leitura_atestado, mosaico, ocorrencias, validacao
from .ocorrencias import Quem

ROTULO = {"BATIDA_EM_ANALISE": "Batida em conferência", "MOSAICO": "Mosaico sem conferência",
          "APARELHO": "Aparelho esperando aprovação", "RENOVAR": "Aparelho a renovar ou desativar"}
ETAPA_ROTULO = {"SUPERVISOR": "Administração da obra", "DP": "DP"}


def _data(valor, campo) -> Optional[dt.date]:
    if not valor:
        return None
    try:
        return dt.date.fromisoformat(str(valor))
    except ValueError as e:
        raise ErroDeValidacao("data ilegível (AAAA-MM-DD)", campo=campo) from e


def _esperando(desde: Optional[dt.datetime]) -> Optional[int]:
    if not desde:
        return None
    return max(0, (horario.agora() - desde).days)


def listar(conn: Connection, quem: Quem, *, obra_id: Optional[int] = None, de=None, ate=None,
           tipos: Optional[list[str]] = None, busca: str = "", so_minhas: bool = False,
           ver_aparelhos: bool = False) -> dict:
    de, ate = _data(de, "de"), _data(ate, "ate")
    # O ponto vale a partir de 16/10/2026 (vigencia.py): o que é de antes não espera decisão.
    from . import vigencia
    de = max(de, vigencia.inicio(conn)) if de else vigencia.inicio(conn)
    tipos = set(t.upper() for t in (tipos or []))
    busca = busca.strip().lower()
    digitos = "".join(ch for ch in busca if ch.isdigit())
    itens: list[dict] = []

    def quer(tipo: str) -> bool:
        return not tipos or tipo in tipos

    def casa_busca(nome: str, cpf: str = "", *outros: str) -> bool:
        """A pessoa (nome ou CPF) — e, nos pedidos, quem pediu (21/10/2026)."""
        return (not busca or busca in (nome or "").lower() or bool(digitos and digitos in (cpf or ""))
                or any(busca in (t or "").lower() for t in outros))

    # --- pedidos ---------------------------------------------------------
    obra_codigo = None
    if obra_id is not None:
        obra_codigo = (db.um(conn, "SELECT codigo FROM public.obras WHERE id = :o", o=obra_id) or {}).get("codigo")
    for p in ocorrencias.listar(conn, quem, status="PENDENTES", limite=1000):
        por = ocorrencias.pedido_por(p["origem"], p.get("solicitado_por"), p["colaborador"])
        if not quer(p["tipo"]) or not casa_busca(p["colaborador"], p["cpf"], por["texto"]):
            continue
        if obra_codigo and p["obra"] != obra_codigo:
            continue
        ini, fim = dt.date.fromisoformat(p["data_inicio"]), dt.date.fromisoformat(p["data_fim"])
        if (de and fim < de) or (ate and ini > ate):
            continue
        etapa = p["etapa_atual"]
        if p["tipo"] == "AJUSTE_BATIDA":
            quando = f"{ini:%d/%m/%Y} às {(p['horario'] or '')[11:16]}"
        elif p["tipo"] == "COMPENSACAO":
            quando = f"trabalha {dt.date.fromisoformat(p['dia_trabalhado']):%d/%m}, folga {ini:%d/%m}"
        else:
            quando = f"{ini:%d/%m/%Y}" + (f" a {fim:%d/%m/%Y} ({p['dias']} dias)" if fim != ini else "")
        rota = (f"/erp/api/ponto/afastamentos/{p['id']}/etapa-dp" if etapa == "DP"
                else f"/erp/api/ponto/ocorrencias/{p['id']}/etapa-supervisor")
        criado = horario.ler_iso(p["criado_em"])
        itens.append({
            "categoria": "PEDIDO", "tipo": p["tipo"], "rotulo": p["rotulo"], "id": p["id"],
            "colaborador_id": p["colaborador_id"], "pessoa": p["colaborador"], "obra": p["obra"],
            "data": p["data_inicio"], "quando": quando, "detalhe": p["descricao"],
            "sigiloso": p["sigiloso"], "tem_documento": p["tem_documento"], "documento_id": p["documento_id"],
            "leitura_ia": p.get("leitura_ia"), "cid": p.get("cid"),
            # o que a IA leu × o que foi lançado (26/10/2026); só para quem vê a leitura
            "confronto": (leitura_atestado.confronto(p["leitura_ia"], p) if p.get("leitura_ia") and quem.dp else None),
            "etapa": ETAPA_ROTULO.get(etapa, etapa), "etapas": [ETAPA_ROTULO[e] for e in p["etapas"]],
            "pode_decidir": p["pode_decidir"], "decidir_em": rota,
            "desde": p["criado_em"], "dias_esperando": _esperando(criado), "origem": p["origem"],
            "pedido_por": por,
        })

    # --- batidas em conferência ------------------------------------------
    if quer("BATIDA_EM_ANALISE"):
        regra = validacao.quem_valida_batida(conn)
        sql = """SELECT m.id, m.nsr, m.colaborador_id, m.obra_id, m.timestamp_servidor, m.data_referencia,
                        m.motivo_analise, m.foto_id, m.criado_em, c.nome, c.cpf, o.codigo AS obra,
                        m.latitude, m.longitude, m.distancia_metros, m.dentro_da_cerca
                   FROM ponto.marcacoes m JOIN public.colaboradores c ON c.id = m.colaborador_id
                   JOIN public.obras o ON o.id = m.obra_id WHERE m.status = 'EM_ANALISE'"""
        params: dict = {}
        if quem.obras is not None:
            sql += " AND m.obra_id = ANY(:obras)"
            params["obras"] = list(quem.obras) or [0]
        if obra_id is not None:
            sql += " AND m.obra_id = :o"
            params["o"] = obra_id
        if de:
            sql += " AND m.data_referencia >= :de"
            params["de"] = de
        if ate:
            sql += " AND m.data_referencia <= :ate"
            params["ate"] = ate
        for m in db.todos(conn, sql + " ORDER BY m.timestamp_servidor LIMIT 1000", **params):
            if not casa_busca(m["nome"], m["cpf"]):
                continue
            pode = (quem.dp if regra == validacao.DP else quem.supervisor) and quem.alcanca_obra(m["obra_id"])
            itens.append({
                "categoria": "BATIDA", "tipo": "BATIDA_EM_ANALISE", "rotulo": ROTULO["BATIDA_EM_ANALISE"],
                "id": m["id"], "nsr": m["nsr"], "colaborador_id": m["colaborador_id"], "pessoa": m["nome"],
                "obra": m["obra"], "obra_id": m["obra_id"], "data": m["data_referencia"].isoformat(),
                "quando": f"{horario.para_local(m['timestamp_servidor']):%d/%m/%Y às %H:%M}",
                "detalhe": m["motivo_analise"], "tem_foto": m["foto_id"] is not None,
                "latitude": float(m["latitude"]) if m.get("latitude") is not None else None,
                "longitude": float(m["longitude"]) if m.get("longitude") is not None else None,
                "distancia_metros": float(m["distancia_metros"]) if m.get("distancia_metros") is not None else None,
                "etapa": validacao.ROTULO_OPCAO[regra], "pode_decidir": bool(pode),
                "decidir_em": (f"/erp/api/ponto/marcacoes/{m['id']}/decidir-dp" if regra == validacao.DP
                               else f"/erp/api/ponto/marcacoes/{m['id']}/decidir"),
                "desde": horario.texto(m["criado_em"]), "dias_esperando": _esperando(m["criado_em"]),
            })

    # --- mosaicos obrigatórios sem conferência ----------------------------
    if quer("MOSAICO") and db.tem_003(conn) and not busca:
        for mo in mosaico.pendentes(conn, quem.obras):
            d = dt.date.fromisoformat(mo["data"])
            if (obra_id is not None and mo["obra_id"] != obra_id) or (de and d < de) or (ate and d > ate):
                continue
            itens.append({
                "categoria": "MOSAICO", "tipo": "MOSAICO", "rotulo": ROTULO["MOSAICO"], "id": mo["obra_id"],
                "obra_id": mo["obra_id"], "pessoa": None, "obra": mo["codigo"], "data": mo["data"],
                "quando": f"{d:%d/%m/%Y}", "detalhe": f"{mo['batidas']} batidas para conferir",
                "etapa": "Responsável da obra", "pode_decidir": bool(quem.supervisor and quem.alcanca_obra(mo["obra_id"])),
                "abrir_em": f"/erp/ponto/mosaico?obra={mo['obra_id']}&data={mo['data']}",
                "desde": mo["avisado_em"], "dias_esperando": max(0, (horario.hoje() - d).days - 1),
            })

    # --- aparelhos ----------------------------------------------------------
    if ver_aparelhos and quer("APARELHO") and obra_id is None:
        # O celular de quem NÃO é exceção não entra na fila: o padrão é só o
        # aparelho da obra bater, e 400 celulares esperando aprovação esconderiam
        # o tablet que precisa dela. Continua em Configuração › Aparelhos.
        from . import forma_de_bater
        excecoes = None
        if forma_de_bater.em_vigor(conn):
            excecoes = {int(l["colaborador_id"]) for l in db.todos(
                conn, "SELECT colaborador_id FROM ponto.colaborador_config WHERE bate_no_celular")}
        # Quem já tem celular pessoal aprovado (09/10/2026: "uma pessoa que já tinha
        # o celular cadastrado (…) está pedindo novamente o cadastro do aparelho"):
        # o pedido novo diz isso, para quem aprova saber que é troca.
        aprovados = {int(l["colaborador_id"]): l for l in db.todos(conn, """
            SELECT DISTINCT ON (colaborador_id) colaborador_id, aprovado_em, descricao
              FROM ponto.dispositivos
             WHERE perfil = 'INDIVIDUAL' AND status = 'APROVADO' AND colaborador_id IS NOT NULL
             ORDER BY colaborador_id, aprovado_em DESC NULLS LAST""")}
        # o pedido mais novo em cima (16/10/2026), pela hora do pedido, não do nome
        def _pedido(a):
            return max((x for x in (a.get("criado_em"), a.get("ultimo_uso_em")) if x), default=a.get("criado_em"))
        for a in sorted(dispositivos.listar(conn, "PENDENTE"), key=_pedido, reverse=True):
            if (excecoes is not None and a.get("colaborador_id") and int(a["colaborador_id"]) not in excecoes
                    and not dispositivos.foi_pedido(a)):      # o pedido explícito entra sempre
                continue
            j = dispositivos.para_json(a)
            if not casa_busca((j.get("dono") or {}).get("nome"), "", j.get("descricao"), j.get("codigo")):
                continue
            antigo = aprovados.get(int(a["colaborador_id"])) if a.get("colaborador_id") else None
            troca = (f" · já tem um celular aprovado"
                     + (f" desde {horario.para_local(antigo['aprovado_em']):%d/%m/%Y}" if antigo["aprovado_em"] else "")
                     + ": provavelmente trocou de celular ou apagou os dados do navegador — aprovar este "
                       "bloqueia o anterior") if antigo else ""
            itens.append({
                "categoria": "APARELHO", "tipo": "APARELHO", "rotulo": ROTULO["APARELHO"], "id": j["id"],
                "pessoa": (j.get("dono") or {}).get("nome"), "obra": None, "data": None, "quando": j.get("pedido_em"),
                "detalhe": (f"{j.get('descricao') or 'aparelho sem nome (ninguém entrou nele com CPF e PIN)'}"
                            f" — código {j.get('codigo')}{troca}"),
                "troca_de_celular": bool(antigo),
                "codigo": j.get("codigo"), "descricao": j.get("descricao"), "dono": j.get("dono"),
                "etapa": "Quem configura o ponto",
                "pode_decidir": True, "abrir_em": "/erp/ponto/configuracao",
                "desde": j.get("pedido_em"), "dias_esperando": None,
                "pedido_por": ({"nome": j["dono"]["nome"], "como": "entrou com CPF e PIN e pediu", "propria": True,
                                "texto": j["dono"]["nome"]} if (j.get("dono") or {}).get("nome") else None),
            })

    # --- aparelhos a renovar ou desativar (todos vencem; o de grupo é passageiro) ---
    if ver_aparelhos and quer("RENOVAR") and obra_id is None:
        for g in dispositivos.aparelhos_a_rever(conn):
            if not casa_busca(g["dono"], "", g["descricao"], g["codigo"]):
                continue
            quem_tem = (f"de {g['dono']}" if g["dono"] else
                        f"{g['pessoas']} pessoa(s) no grupo" if g["perfil"] == "LISTA" else "da obra")
            itens.append({
                "categoria": "RENOVAR", "tipo": "RENOVAR", "rotulo": ROTULO["RENOVAR"], "id": g["id"],
                "pessoa": g["dono"], "obra": None, "data": None,
                "quando": f"vale até {dt.date.fromisoformat(g['valido_ate']):%d/%m/%Y}" if g["valido_ate"] else "—",
                "detalhe": f"{g['descricao']} ({quem_tem}) — código {g['codigo']}: " + "; ".join(g["motivos"]),
                "sugestao": g["sugestao"], "perfil": g["perfil"], "etapa": "Quem configura o ponto",
                "pode_decidir": True, "desde": None, "dias_esperando": None,
            })

    if so_minhas:
        itens = [i for i in itens if i["pode_decidir"]]
    itens.sort(key=lambda i: (i["dias_esperando"] is None, -(i["dias_esperando"] or 0), i.get("data") or ""))
    contagem = Counter(i["tipo"] for i in itens)
    return {"itens": itens, "quantidade": len(itens), "por_tipo": dict(contagem),
            "minhas": sum(1 for i in itens if i["pode_decidir"]),
            "regra": validacao.ler(conn)}
