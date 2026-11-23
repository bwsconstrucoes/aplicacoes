# -*- coding: utf-8 -*-
"""
O TERMO DE ACORDO DE COMPENSAÇÃO, que nasce sozinho (23/11/2026).

A decisão do dono, com as palavras dele: *"o sistema permite ser lançado, o DP toma ciência, (…) o próprio sistema
aguarda e cobra que a compensação é lançada e o sistema em seguida cobra a assinatura do termo. Aí se houver algum
problema, alguma discordância, o DP vai fazer contato (…) vai poder cancelar aquele lançamento."*

Ou seja: a COMPENSAÇÃO VALE ao ser aprovada; o termo vem depois, como pendência de assinatura — da própria pessoa
no "Meu ponto" ou conduzida pelo administrativo da obra. Não trava nada.

POR QUE EXISTE: compensar fora do mês é banco de horas e pede acordo individual ESCRITO (CLT, art. 59, §5º). A
observação assinada na folha de ponto mostra o combinado, mas não é o acordo (21/11/2026).

COMO: `sincronizar` olha as compensações de DIA aprovadas (não as de horas, nem as trocas de plantão) de algumas
pessoas e abre UM termo por pessoa para as que ainda não têm — o recesso de quatro folgas vira um termo só, com os
quatro pares no anexo. Se as compensações de um termo ainda aberto forem todas canceladas, o termo é cancelado
junto. O termo mora em `dp_termos` (ERP, migração 117/126): é o mesmo documento que o DP já manda assinar.

SÓ AS NOVAS: valem as compensações lançadas a partir de 23/11/2026 (`INICIO`) — as de antes não ganham termo de
surpresa.

CUSTO: por pessoa, uma leitura das compensações dela no ponto, uma dos termos dela no ERP e, quando há o que fazer,
uma gravação. Nada vai para fora.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from sqlalchemy.engine import Connection

from .. import db

logger = logging.getLogger("ponto.termo_compensacao")

ORIGEM = "PONTO_COMPENSACAO"
TIPO = "TERMO-COMPENSACAO"
TITULO = "Termo de acordo individual de compensação de jornada"
INICIO = dt.date(2026, 11, 23)
PRAZO_DIAS = 7

_DIAS = ("segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo")


def _br(d: dt.date) -> str:
    return f"{d:%d/%m/%Y} ({_DIAS[d.weekday()]})"


def _hm(m: int) -> str:
    return f"{int(m) // 60}h{int(m) % 60:02d}"


def texto(pessoa: dict, empresa: Optional[dict], pares: list[dict]) -> str:
    """PURA. O texto do termo: o modelo de 22/11/2026, preenchido. `pares`: [{folga, pagar, minutos}]."""
    emp = (f"{empresa['razao_social']}, CNPJ {_cnpj(empresa.get('cnpj'))}" if empresa
           else "a empresa empregadora")
    total = sum(int(p.get("minutos") or 0) for p in pares)
    linhas = "\n".join(f"- Folga em {_br(p['folga'])}, de {_hm(p['minutos'])}: compensada com o trabalho em "
                       f"{_br(p['pagar'])}." for p in pares)
    return f"""EMPREGADORA: {emp}.
EMPREGADO(A): {pessoa['nome']}, CPF {_cpf(pessoa.get('cpf'))}{(', função ' + pessoa['funcao']) if pessoa.get('funcao') else ''}.

As partes, de comum acordo e com base no art. 59, §§ 5º e 6º, da Consolidação das Leis do Trabalho, ajustam o seguinte:

Cláusula 1ª - Objeto. A EMPRESA concede ao EMPREGADO folga nos dias listados no Anexo, sem desconto no salário, e o EMPREGADO compensa essas horas trabalhando nos dias de compensação também listados no Anexo.

Cláusula 2ª - Hora por hora. Cada dia de folga é compensado pelas horas que o EMPREGADO teria de trabalhar nele, conforme a sua escala, uma hora trabalhada para cada hora de folga. As horas trabalhadas nos dias de compensação, até esse limite, não são horas extras e não têm adicional.

Cláusula 3ª - Compensação antes ou depois. Os dias de compensação podem ser anteriores ou posteriores à folga, desde que todos fiquem dentro de 6 (seis) meses contados da primeira folga.

Cláusula 4ª - Registro no ponto. A folga e o dia de compensação ficam registrados no ponto eletrônico da EMPRESA e aparecem como observação na folha de ponto dos meses envolvidos, que o EMPREGADO confere e assina.

Cláusula 5ª - Compensação não cumprida. Se o EMPREGADO não trabalhar, sem justificativa legal, num dia de compensação, as horas que faltarem são lançadas como falta naquele dia, com o desconto correspondente no salário do mês, inclusive o reflexo no descanso semanal remunerado previsto em lei. Ausência justificada (atestado médico, licença legal) não gera falta; nesse caso as partes combinam outro dia de compensação dentro do prazo da Cláusula 3ª.

Cláusula 6ª - Rescisão. Se o contrato terminar antes de toda a compensação, as horas de folga ainda não compensadas são descontadas nas verbas rescisórias, no limite permitido em lei; e as horas trabalhadas a mais que ainda não viraram folga são pagas como horas extras, com o adicional legal (CLT, art. 59, §3º).

Cláusula 7ª - Limites da jornada. Os dias de compensação respeitam a jornada máxima de 10 (dez) horas por dia, o intervalo para refeição e o descanso de 11 (onze) horas entre jornadas.

Cláusula 8ª - Mudança de data. A EMPRESA pode trocar um dia de compensação por outro, avisando o EMPREGADO com pelo menos 7 (sete) dias de antecedência, mantido o prazo da Cláusula 3ª.

Cláusula 9ª - Vigência. Este acordo vale da data da assinatura até o último dia de compensação do Anexo, sem passar dos 6 (seis) meses da Cláusula 3ª.

ANEXO - os dias de folga e os de compensação
{linhas}
Total: {_hm(total)} de folga, compensadas em {len(pares)} dia(s)."""


def _cpf(c) -> str:
    c = "".join(ch for ch in str(c or "") if ch.isdigit())
    return f"{c[0:3]}.{c[3:6]}.{c[6:9]}-{c[9:11]}" if len(c) == 11 else c


def _cnpj(c) -> str:
    c = "".join(ch for ch in str(c or "") if ch.isdigit())
    return f"{c[0:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:14]}" if len(c) == 14 else c


def _empresa(conn: Connection, obra_id: Optional[int]) -> Optional[dict]:
    if not obra_id or not db.um(conn, """SELECT 1 AS x FROM information_schema.columns WHERE table_schema = 'public'
                                         AND table_name = 'obras' AND column_name = 'empresa_id'"""):
        return None
    return db.um(conn, """SELECT e.razao_social, e.cnpj FROM public.obras o JOIN public.empresas e ON e.id = o.empresa_id
                           WHERE o.id = :o""", o=int(obra_id))


def _compensacoes(conn: Connection, ids: list[int]) -> list[dict]:
    """As compensações de DIA que contam (aprovadas, desde `INICIO`), de todas as situações — as canceladas também,
    para saber quando um termo aberto ficou sem nada."""
    from .espelho import compensacao_de_horas, e_troca
    linhas = db.todos(conn, """
        SELECT id, colaborador_id, tipo, subtipo, status, data_inicio, data_fim, dia_trabalhado, descricao, obra_id
          FROM ponto.ocorrencias
         WHERE colaborador_id = ANY(:ids) AND tipo = 'COMPENSACAO' AND dia_trabalhado IS NOT NULL
           AND criado_em >= :inicio
         ORDER BY data_inicio, id""", ids=[int(x) for x in ids], inicio=INICIO)
    return [dict(l) for l in linhas if not compensacao_de_horas(l) and not e_troca(l)]


def sincronizar(colaborador_ids: list[int]) -> dict:
    """Abre o termo de quem tem compensação aprovada sem termo; cancela o termo aberto cujas compensações caíram
    todas. Nunca derruba quem chamou: a compensação já vale; o termo é a cobrança que vem depois."""
    ids = sorted({int(x) for x in colaborador_ids or []})
    feito = {"abertos": 0, "cancelados": 0}
    if not ids:
        return feito
    try:
        from app.apps.erp.core import termos
        from app.apps.erp.db.database import get_session
        from . import cadastros, escalas
        with db.conexao() as conn:
            comps = _compensacoes(conn, ids)
            if not comps:
                return feito
            com_comp = sorted({int(c["colaborador_id"]) for c in comps})
            pessoas = {cid: cadastros.colaborador_por_id(conn, cid) for cid in com_comp}
            previstos = {}
            for c in comps:
                if c["status"] == "APROVADA":
                    vig = escalas.EscalasDaPessoa(conn, int(c["colaborador_id"]))
                    escala, _ = vig.no_dia(c["data_inicio"])
                    previstos[c["id"]] = (sum(max(0, s - e) for e, s in escala.periodos(c["data_inicio"]))
                                          if escala is not None else 0)
            empresas = {cid: _empresa(conn, (pessoas[cid] or {}).get("obra_id")) for cid in com_comp}
        with get_session() as s:
            if not termos.tem_tabela(s) or not termos.tem_origem(s):
                logger.info("Ponto: termo de compensação esperando a atualização do banco do ERP (126)")
                return feito
            existentes = termos.da_origem(s, ORIGEM, com_comp)
            cobertos = {int(i) for t in existentes for i in (t["origem_ids"] or [])}
            status = {int(c["id"]): c["status"] for c in comps}
            # o termo aberto cujas compensações caíram todas: cancelado junto
            for t in existentes:
                if t["situacao"] == "ABERTO" and t["origem_ids"] and all(
                        status.get(int(i)) in ("CANCELADA", "NEGADA") for i in t["origem_ids"]):
                    termos.cancelar(s, int(t["id"]), "a compensação foi cancelada no ponto", None)
                    feito["cancelados"] += 1
            for cid in com_comp:
                novas = [c for c in comps if int(c["colaborador_id"]) == cid and c["status"] == "APROVADA"
                         and int(c["id"]) not in cobertos]
                if not novas or not pessoas.get(cid):
                    continue
                pares = [{"folga": c["data_inicio"], "pagar": c["dia_trabalhado"],
                          "minutos": previstos.get(c["id"], 0)} for c in novas]
                primeira = min(c["data_inicio"] for c in novas)
                dias = max(1, min(PRAZO_DIAS, (primeira - dt.date.today()).days)) if primeira > dt.date.today() \
                    else PRAZO_DIAS
                termos.abrir(s, colaborador_id=cid, tipo=TIPO, titulo=TITULO,
                             texto_=texto(pessoas[cid], empresas.get(cid), pares), dias=dias,
                             origem=ORIGEM, origem_ids=[int(c["id"]) for c in novas])
                feito["abertos"] += 1
            s.commit()
    except Exception:  # noqa: BLE001 — a compensação já está gravada; o termo se refaz na próxima vez
        logger.exception("Ponto: não consegui abrir o termo de compensação de %s", ids)
    if feito["abertos"] or feito["cancelados"]:
        logger.info("Ponto: termos de compensação — %d aberto(s), %d cancelado(s)", feito["abertos"],
                    feito["cancelados"])
    return feito
