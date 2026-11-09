# -*- coding: utf-8 -*-
"""
O DIÁRIO DE OBRA.

Ditado pelo dono em 08/11/2026. **O PRINCÍPIO manda em tudo o que está aqui:**

> o diário pergunta SÓ o que ninguém sabe além de quem esteve lá.

Tudo o que o sistema já tem, o sistema preenche. Diário que obriga a redigitar
o que o ERP sabe é diário preenchido de qualquer jeito na sexta à tarde, com
todos os dias da semana iguais — e aí ele não serve nem para a obra nem para o
pleito de prazo.

**O QUE VEM DE GRAÇA, e de onde:**

| | De onde | Por quê |
|---|---|---|
| cabeçalho da obra e a conta de prazo | `obras` | é cadastro, e muda quando há aditivo |
| **o efetivo** | **o PONTO** | *"a gente não precisaria colocar quantidade de pessoas e função (…) já traria do ponto que é quem trabalhou"* |
| material recebido | Suprimentos | diário registra material recebido, e o dado já existe |
| o relato de ontem | o próprio diário | *"é muito comum o relato do dia anterior ser similar ao do dia seguinte"* |
| sábado, domingo e feriado | o calendário e o ponto | nascem "sem expediente" sem ninguém escrever |

**O QUE A PESSOA ESCREVE:** o tempo, o relato do dia, as ocorrências. E o
terceirizado, que é o único efetivo que o ponto não conhece.

---

**DUAS REGRAS QUE DÃO VALOR JURÍDICO AO DOCUMENTO, e não são detalhe de tela:**

1. **O bloco da fiscalização é DELA.** *"a gente poder conceder acesso a, por
   exemplo, a fiscalização, para a fiscalização fazer observações."* Quem lança
   o diário NÃO escreve nesse campo, e quem é fiscalização não reescreve o
   relato da obra. Um diário que a obra pode reescrever por inteiro não prova
   nada: é justamente a parte que ela não controla que dá valor ao resto.
2. **Depois de FECHADO, corrige-se com registro.** Não é proibido (erro de
   digitação acontece) nem silencioso (reescrever o passado sem rastro tira o
   valor do documento). Fica guardado o que estava, o que passou a estar, quem
   e por quê.

---

**O PONTO PODE NÃO ESTAR LÁ, e o diário não cai por isso.** O schema do ponto
nasce das migrações dele, que são de outra área: num banco onde o ponto nunca
rodou, a consulta do efetivo falha. Então ela é protegida e o diário abre com o
efetivo vazio e um recado — nunca com erro na cara de quem está na obra.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

SITUACOES = ("ABERTO", "FECHADO")

# O tempo, por período. Lista aberta de propósito: o modelo que cada órgão
# exige tem a sua, e migração para acrescentar "garoa" seria absurdo.
TEMPOS = ("BOM", "NUBLADO", "CHUVOSO", "IMPRATICAVEL")
TEMPO_ROTULOS = {
    "BOM": "Bom", "NUBLADO": "Nublado", "CHUVOSO": "Chuvoso",
    "IMPRATICAVEL": "Impraticável",
}

# Os campos que a OBRA escreve. A observação da fiscalização NÃO está aqui, e
# essa ausência é a regra 1 lá de cima — ela não é esquecimento.
CAMPOS_DA_OBRA = (
    "tempo_manha", "tempo_tarde", "relato", "ocorrencias",
    "fiscalizacao_esteve", "fiscalizacao_quem", "sem_expediente",
)


# ---------------------------------------------------------------------------
# A CONSULTA QUE PODE NÃO EXISTIR — e o savepoint que salva a transação
#
# ⚑ ISTO É A PARTE MAIS IMPORTANTE DO ARQUIVO, e custou um defeito para
# aparecer. Em Postgres, UMA consulta que falha ABORTA A TRANSAÇÃO INTEIRA:
# tudo o que vier depois responde "current transaction is aborted", mesmo
# tendo sido pego por `except`. Então `try/except` em volta de um SELECT NÃO
# BASTA — ele engole o erro e deixa a sessão quebrada para o resto do pedido.
#
# Foi exatamente o que aconteceu aqui: num banco sem o schema do ponto, a
# consulta do efetivo falhava, o `except` devolvia lista vazia "com
# elegância", e a busca do material logo abaixo morria com um erro que não
# tinha nada a ver. O diário inteiro caía por causa de uma consulta opcional.
#
# `begin_nested()` abre um SAVEPOINT: o que falhar dentro dele volta sozinho e
# a transação de quem chamou segue viva. É o mesmo conserto que o importador de
# pessoas precisou quando um PIS repetido derrubava todas as linhas seguintes.
# ---------------------------------------------------------------------------
def _consulta_opcional(s: Session, sql: str, **params):
    """Roda uma consulta que PODE não existir, sem levar a transação junto."""
    try:
        with s.begin_nested():
            return list(s.execute(text(sql), params).mappings().all())
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/diário: consulta opcional falhou — %s", e)
        return None


# ---------------------------------------------------------------------------
# 1. O CABEÇALHO DA OBRA — e a conta de prazo
# ---------------------------------------------------------------------------
def cabecalho(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    """O que vem do cadastro da obra. NADA DISSO É DIGITADO NO DIÁRIO.

    E nada disso é COPIADO para a tabela do diário, tampouco: obra que ganha
    aditivo de prazo passaria a ter diários antigos afirmando o prazo velho. A
    conta é feita na hora, sobre as datas que valem hoje.
    """
    o = s.execute(text("""
        SELECT id, codigo, nome, objeto, contrato,
               data_inicio, data_termino, ordem_servico, data_ordem_servico,
               prazo_execucao_dias, responsavel_tecnico, crea_obra,
               municipio, uf
          FROM obras WHERE id = :o"""), {"o": obra_id}).mappings().first()
    if o is None:
        raise ErroNaoEncontrado("Obra não encontrada.")

    inicio = o["data_inicio"] or o["data_ordem_servico"]
    fim = o["data_termino"]
    prazo: dict[str, Any] = {"decorridos": None, "total": None, "faltam": None,
                             "pct": None}
    if inicio and dia >= inicio:
        prazo["decorridos"] = (dia - inicio).days + 1
    if inicio and fim and fim >= inicio:
        prazo["total"] = (fim - inicio).days + 1
    if fim:
        # Pode dar NEGATIVO, e tem de dar: obra que passou do prazo precisa
        # dizer isso em voz alta, não mostrar zero.
        prazo["faltam"] = (fim - dia).days
    if prazo["decorridos"] and prazo["total"]:
        prazo["pct"] = round(100 * prazo["decorridos"] / prazo["total"])

    return {
        "obra_id": o["id"], "codigo": o["codigo"], "nome": o["nome"],
        "objeto": o["objeto"] or "", "contrato": o["contrato"] or "",
        "data_inicio": str(inicio) if inicio else None,
        "data_termino": str(fim) if fim else None,
        "ordem_servico": o["ordem_servico"] or "",
        "data_ordem_servico": (str(o["data_ordem_servico"])
                               if o["data_ordem_servico"] else None),
        "responsavel_tecnico": o["responsavel_tecnico"] or "",
        "crea": o["crea_obra"] or "",
        "municipio": o["municipio"] or "", "uf": o["uf"] or "",
        "prazo": prazo,
    }


# ---------------------------------------------------------------------------
# 2. O EFETIVO — vem do PONTO, e é a melhor parte do desenho
# ---------------------------------------------------------------------------
def efetivo_do_ponto(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    """Quem bateu ponto NESTA obra NESTE dia, com função e horário.

    Exigência dele, e é o que faz o diário deixar de ser trabalho: *"hoje a
    gente tem um ponto. Então a gente não precisaria colocar quantidade de
    pessoas e função. Na hora que fosse abrir o dia, já está ali listado."*

    **PROTEGIDA DE PROPÓSITO.** O schema `ponto` nasce das migrações da área do
    ponto. Num banco onde elas não rodaram — ou se o ponto mudar a tabela —
    isto falha, e o diário tem de abrir do mesmo jeito, com o efetivo vazio e
    um recado. Diário que não abre porque o ponto está fora é pior que diário
    sem efetivo automático.
    """
    vazio = {"pessoas": [], "total": 0, "por_funcao": [], "indisponivel": None}
    linhas = _consulta_opcional(s, """
            SELECT c.id, c.nome,
                   COALESCE(cg.nome, f.nome, '') AS funcao,
                   MIN(m.timestamp_servidor) AS entrada,
                   MAX(m.timestamp_servidor) AS saida,
                   count(*) AS batidas
              FROM ponto.marcacoes m
              JOIN colaboradores c ON c.id = m.colaborador_id
              LEFT JOIN cargos cg ON cg.id = c.cargo_id
              LEFT JOIN funcoes f ON f.id = c.funcao_id
             WHERE m.obra_id = :o
               AND m.data_referencia = :d
               AND m.status <> 'CANCELADA'
             GROUP BY c.id, c.nome, cg.nome, f.nome
             ORDER BY c.nome""", o=obra_id, d=dia)
    if linhas is None:
        return {**vazio,
                "indisponivel": "O ponto não respondeu. O efetivo pode ser "
                                "acrescentado à mão."}

    pessoas = [{
        "colaborador_id": l["id"], "nome": l["nome"],
        "funcao": l["funcao"] or "—",
        "entrada": l["entrada"].strftime("%H:%M") if l["entrada"] else None,
        "saida": (l["saida"].strftime("%H:%M")
                  if l["saida"] and l["batidas"] > 1 else None),
    } for l in linhas]

    contagem: dict[str, int] = {}
    for p in pessoas:
        contagem[p["funcao"]] = contagem.get(p["funcao"], 0) + 1
    por_funcao = [{"funcao": f, "quantidade": q}
                  for f, q in sorted(contagem.items(), key=lambda x: -x[1])]
    return {"pessoas": pessoas, "total": len(pessoas),
            "por_funcao": por_funcao, "indisponivel": None}


def lotados_que_bateram_em_outra(s: Session, obra_id: int,
                                 dia: date) -> list[dict[str, Any]]:
    """Quem é desta obra e bateu ponto em OUTRA hoje.

    Não é acusação: é a transferência que ninguém avisou, e que faria o
    efetivo do diário não bater com a obra que pagou o dia. Hoje isso não é
    percebido por ninguém.
    """
    linhas = _consulta_opcional(s, """
            SELECT DISTINCT c.id, c.nome, o.codigo AS onde
              FROM ponto.marcacoes m
              JOIN colaboradores c ON c.id = m.colaborador_id
              JOIN obras o ON o.id = m.obra_id
             WHERE c.obra_id = :o AND m.obra_id <> :o
               AND m.data_referencia = :d
               AND m.status <> 'CANCELADA'
             ORDER BY c.nome LIMIT 50""", o=obra_id, d=dia) or []
    return [{"colaborador_id": l["id"], "nome": l["nome"], "onde": l["onde"]}
            for l in linhas]


# ---------------------------------------------------------------------------
# 3. O MATERIAL QUE CHEGOU — vem de Suprimentos
# ---------------------------------------------------------------------------
def material_recebido(s: Session, obra_id: int, dia: date) -> list[dict[str, Any]]:
    """O que foi recebido nesta obra neste dia, das entregas já lançadas.

    Diário de obra registra material recebido, e esse dado já existe no ERP —
    digitá-lo de novo seria criar uma segunda verdade sobre a mesma entrega.
    Protegida pelo mesmo motivo do efetivo: Suprimentos pode não ter sido
    usado ainda neste banco.
    """
    linhas = _consulta_opcional(s, """
            SELECT COALESCE(i.nome, si.especificacao, '—') AS item,
                   ri.quantidade, si.unidade,
                   r.nota_numero, f.razao_social AS fornecedor
              FROM recebimentos r
              JOIN recebimento_itens ri ON ri.recebimento_id = r.id
              JOIN pedido_itens pi ON pi.id = ri.pedido_item_id
              LEFT JOIN suprimento_itens si ON si.id = pi.suprimento_item_id
              LEFT JOIN insumos i ON i.id = si.insumo_id
              LEFT JOIN pedidos_compra pc ON pc.id = r.pedido_id
              LEFT JOIN fornecedores f ON f.id = pc.fornecedor_id
             WHERE r.obra_id = :o AND r.data = :d
             ORDER BY item LIMIT 100""", o=obra_id, d=dia) or []
    return [{
        "item": l["item"],
        "quantidade": (f"{l['quantidade']:g}" if l["quantidade"] is not None else ""),
        "unidade": l["unidade"] or "",
        "nota": l["nota_numero"] or "",
        "fornecedor": l["fornecedor"] or "",
    } for l in linhas]


# ---------------------------------------------------------------------------
# 4. O DIA QUE NASCE SEM EXPEDIENTE
# ---------------------------------------------------------------------------
def _e_feriado(s: Session, obra_id: int, dia: date) -> Optional[str]:
    """O feriado é o do PONTO — ele já tem cadastro de feriado por obra.

    Não invento um segundo calendário: dois calendários de feriado divergem no
    primeiro município que decreta ponto facultativo, e aí o diário cobra um
    dia que o ponto considera folga.
    """
    linhas = _consulta_opcional(s, """
            SELECT nome FROM ponto.feriados
             WHERE data = :d
               AND (obra_id IS NULL OR obra_id = :o)
             ORDER BY (obra_id IS NULL) LIMIT 1""", d=dia, o=obra_id)
    return linhas[0]["nome"] if linhas else None


def nasce_sem_expediente(s: Session, obra_id: int, dia: date) -> tuple[bool, str]:
    """Sábado, domingo e feriado nascem "não houve expediente".

    Pedido dele: *"finais de semana, a princípio, se não houver relato, e
    possivelmente vai ter só a presença de vigias, aí já pode colocar que não
    houve expediente. Fim de semana e feriados, a menos que seja digitado
    alguma coisa."*

    **MAS SE ALGUÉM BATEU PONTO, NÃO NASCE ASSIM.** Obra que trabalhou no
    sábado tem diário de sábado — e deixar nascer "sem expediente" com doze
    pessoas no canteiro seria o sistema mentindo por comodidade. O vigia
    sozinho não conta como expediente; dois ou mais, conta.
    """
    feriado = _e_feriado(s, obra_id, dia)
    fim_de_semana = dia.weekday() >= 5
    if not (feriado or fim_de_semana):
        return False, ""
    quantos = efetivo_do_ponto(s, obra_id, dia)["total"]
    if quantos > 1:
        return False, ""
    motivo = f"Feriado: {feriado}" if feriado else (
        "Sábado" if dia.weekday() == 5 else "Domingo")
    if quantos == 1:
        motivo += " — só a vigilância"
    return True, motivo


# ---------------------------------------------------------------------------
# 5. ABRIR O DIA — é aqui que tudo se junta
# ---------------------------------------------------------------------------
def abrir_dia(s: Session, obra_id: int, dia: date,
              usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """O dia, já preenchido com tudo o que o sistema sabe. NÃO GRAVA NADA.

    Abrir não cria linha: quem cria é `salvar`. Assim dá para folhear os dias
    de uma obra sem encher a tabela de diários vazios — e o calendário de
    pendências continua dizendo a verdade sobre o que foi escrito.
    """
    cab = cabecalho(s, obra_id, dia)
    linha = _linha(s, obra_id, dia)
    efetivo = efetivo_do_ponto(s, obra_id, dia)
    sugere_sem, motivo_sem = (False, "")
    if linha is None:
        sugere_sem, motivo_sem = nasce_sem_expediente(s, obra_id, dia)

    return {
        "obra": cab,
        "data": str(dia),
        "existe": linha is not None,
        "diario": _para_tela(s, linha) if linha is not None else {
            "situacao": "ABERTO",
            "tempo_manha": "", "tempo_tarde": "", "relato": "",
            "ocorrencias": "", "fiscalizacao_esteve": False,
            "fiscalizacao_quem": "",
            "sem_expediente": sugere_sem,
            "obs_fiscalizacao": "", "obs_fiscalizacao_por": "",
            "extras": [],
        },
        "sugere_sem_expediente": sugere_sem,
        "motivo_sem_expediente": motivo_sem,
        "efetivo": efetivo,
        "avisos_do_ponto": lotados_que_bateram_em_outra(s, obra_id, dia),
        "material": material_recebido(s, obra_id, dia),
        # ⚑ O RELATO DE ONTEM, para reaproveitar. Pedido dele: *"é muito comum
        # o relato do dia anterior ser similar ao do dia seguinte, porque às
        # vezes você começa uma coisa hoje e dá continuidade no dia seguinte.
        # Havia uma forma de facilitar esse resgate."* Vem junto, e a tela
        # oferece um botão — nunca preenche sozinho, senão o diário vira
        # cópia-cola e deixa de ser relato.
        "relato_anterior": _relato_anterior(s, obra_id, dia),
        "tempos": [{"valor": t, "rotulo": TEMPO_ROTULOS[t]} for t in TEMPOS],
    }


def _linha(s: Session, obra_id: int, dia: date):
    return s.execute(text("SELECT * FROM diarios_obra WHERE obra_id = :o AND data = :d"),
                     {"o": obra_id, "d": dia}).mappings().first()


def _relato_anterior(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    """O último dia COM RELATO antes deste. Pula o fim de semana sozinho."""
    linha = s.execute(text("""
        SELECT data, relato FROM diarios_obra
         WHERE obra_id = :o AND data < :d
           AND relato IS NOT NULL AND btrim(relato) <> ''
         ORDER BY data DESC LIMIT 1"""),
        {"o": obra_id, "d": dia}).mappings().first()
    if linha is None:
        return {"data": None, "relato": ""}
    return {"data": str(linha["data"]), "relato": linha["relato"]}


def _para_tela(s: Session, l) -> dict[str, Any]:
    extras = s.execute(text("""
        SELECT id, descricao, funcao, quantidade, empresa
          FROM diario_efetivo_extra WHERE diario_id = :i ORDER BY id"""),
        {"i": l["id"]}).mappings().all()
    return {
        "id": l["id"], "situacao": l["situacao"],
        "tempo_manha": l["tempo_manha"] or "", "tempo_tarde": l["tempo_tarde"] or "",
        "relato": l["relato"] or "", "ocorrencias": l["ocorrencias"] or "",
        "fiscalizacao_esteve": bool(l["fiscalizacao_esteve"]),
        "fiscalizacao_quem": l["fiscalizacao_quem"] or "",
        "sem_expediente": bool(l["sem_expediente"]),
        "obs_fiscalizacao": l["obs_fiscalizacao"] or "",
        "obs_fiscalizacao_por": l["obs_fiscalizacao_por"] or "",
        "obs_fiscalizacao_em": (str(l["obs_fiscalizacao_em"])
                                if l["obs_fiscalizacao_em"] else None),
        "fechado_em": str(l["fechado_em"]) if l["fechado_em"] else None,
        "assinado_em": str(l["assinado_em"]) if l["assinado_em"] else None,
        "assinado_nome": l["assinado_nome"] or "",
        "clima_medido": l["clima_medido"],
        "extras": [{"id": e["id"], "descricao": e["descricao"],
                    "funcao": e["funcao"] or "", "quantidade": e["quantidade"],
                    "empresa": e["empresa"] or ""} for e in extras],
    }


# ---------------------------------------------------------------------------
# 6. GRAVAR — e a regra de quem escreve o quê
# ---------------------------------------------------------------------------
def salvar(s: Session, obra_id: int, dia: date, dados: dict[str, Any],
           usuario: Optional[Usuario], *, motivo: str = "") -> dict[str, Any]:
    """Grava o que a OBRA escreve. Nunca a observação da fiscalização.

    `CAMPOS_DA_OBRA` não inclui `obs_fiscalizacao` de propósito: mesmo que ela
    venha no corpo do pedido, não entra. É a regra 1 do topo deste arquivo, e
    ela precisa morar aqui — na tela, ela seria só um campo escondido, e campo
    escondido se mostra com o navegador.

    **DEPOIS DE FECHADO, exige motivo** e guarda o que mudou. Diário é
    documento: corrigir pode, apagar o rastro não.
    """
    cabecalho(s, obra_id, dia)                 # 404 se a obra não existe
    linha = _linha(s, obra_id, dia)
    fechado = linha is not None and linha["situacao"] == "FECHADO"
    if fechado and not (motivo or "").strip():
        raise ErroValidacao(
            "Este diário já foi fechado. Para corrigir, diga o motivo — ele "
            "fica registrado junto com o que mudou.")

    limpo: dict[str, Any] = {}
    for campo in CAMPOS_DA_OBRA:
        if campo not in dados:
            continue
        valor = dados[campo]
        if campo in ("fiscalizacao_esteve", "sem_expediente"):
            limpo[campo] = bool(valor)
        elif campo in ("tempo_manha", "tempo_tarde"):
            v = str(valor or "").strip().upper()
            if v and v not in TEMPOS:
                raise ErroValidacao(f"Tempo desconhecido: {valor}.")
            limpo[campo] = v or None
        else:
            limpo[campo] = str(valor or "").strip() or None

    # DIA COM EXPEDIENTE PRECISA DE RELATO. Sem isso o diário vira linha vazia
    # que conta como "preenchido" no calendário — e aí a pendência some sem o
    # trabalho ter sido feito.
    if not limpo.get("sem_expediente", linha["sem_expediente"] if linha else False):
        relato = limpo.get("relato", linha["relato"] if linha else None)
        if not (relato or "").strip():
            raise ErroValidacao(
                "Escreva o relato do dia. Se não houve expediente, marque "
                "“não houve expediente” — aí o relato não é exigido.")

    if linha is None:
        novo = s.execute(text("""
            INSERT INTO diarios_obra
                (obra_id, data, tempo_manha, tempo_tarde, relato, ocorrencias,
                 fiscalizacao_esteve, fiscalizacao_quem, sem_expediente,
                 criado_por)
            VALUES (:o, :d, :tm, :tt, :rel, :oc, :fe, :fq, :se, :u)
            RETURNING id"""),
            {"o": obra_id, "d": dia,
             "tm": limpo.get("tempo_manha"), "tt": limpo.get("tempo_tarde"),
             "rel": limpo.get("relato"), "oc": limpo.get("ocorrencias"),
             "fe": limpo.get("fiscalizacao_esteve", False),
             "fq": limpo.get("fiscalizacao_quem"),
             "se": limpo.get("sem_expediente", False),
             "u": usuario.id if usuario else None}).scalar()
        registrar_evento(s, "diario_obra", novo, "ABERTO",
                         {"obra_id": obra_id, "data": str(dia)},
                         usuario.id if usuario else None)
        s.flush()
        return {"id": novo, "criado": True}

    if fechado:
        for campo, valor in limpo.items():
            antes = linha[campo]
            if _mesmo(antes, valor):
                continue
            s.execute(text("""
                INSERT INTO diario_correcoes
                    (diario_id, campo, antes, depois, motivo, usuario_id)
                VALUES (:i, :c, :a, :d, :m, :u)"""),
                {"i": linha["id"], "c": campo,
                 "a": _texto(antes), "d": _texto(valor), "m": motivo.strip(),
                 "u": usuario.id if usuario else None})

    if limpo:
        sets = ", ".join(f"{c} = :{c}" for c in limpo)
        s.execute(text(f"UPDATE diarios_obra SET {sets}, atualizado_em = now() "
                       f"WHERE id = :id"), {**limpo, "id": linha["id"]})
    s.flush()
    return {"id": linha["id"], "criado": False, "corrigido": fechado}


def _mesmo(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    return (a or "") == (b or "")


def _texto(v: Any) -> str:
    if isinstance(v, bool):
        return "sim" if v else "não"
    return "" if v is None else str(v)


def fechar(s: Session, obra_id: int, dia: date,
           usuario: Optional[Usuario]) -> dict[str, Any]:
    """Fecha o dia. Daqui em diante, corrigir exige motivo."""
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Não há diário neste dia para fechar.")
    if linha["situacao"] == "FECHADO":
        raise ErroValidacao("Este diário já está fechado.")
    if not linha["sem_expediente"] and not (linha["relato"] or "").strip():
        raise ErroValidacao("Não dá para fechar um dia sem relato.")
    s.execute(text("""
        UPDATE diarios_obra
           SET situacao = 'FECHADO', fechado_em = now(), fechado_por = :u,
               atualizado_em = now()
         WHERE id = :i"""), {"i": linha["id"], "u": usuario.id if usuario else None})
    registrar_evento(s, "diario_obra", linha["id"], "FECHADO",
                     {"obra_id": obra_id, "data": str(dia)},
                     usuario.id if usuario else None)
    s.flush()
    return {"id": linha["id"], "situacao": "FECHADO"}


# ---------------------------------------------------------------------------
# 7. O BLOCO DA FISCALIZAÇÃO — e é só ele que a fiscalização escreve
# ---------------------------------------------------------------------------
def observar_como_fiscalizacao(s: Session, obra_id: int, dia: date, texto_obs: str,
                               usuario: Optional[Usuario]) -> dict[str, Any]:
    """A fiscalização escreve a observação dela. E NADA MAIS.

    Esta função é a outra metade da regra: `salvar` não deixa a obra escrever
    aqui, e esta não deixa a fiscalização escrever lá. Duas portas estreitas,
    cada uma com a sua chave — é isso que faz o registro valer alguma coisa.

    **O diário pode estar FECHADO e ela ainda escreve.** O fechamento é da
    obra sobre o relato da obra; a fiscalização costuma visitar depois, e
    travá-la pelo fechamento seria impedir justamente a parte independente.
    """
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Não há diário neste dia.")
    obs = (texto_obs or "").strip()
    if not obs:
        raise ErroValidacao("Escreva a observação.")
    quem = (usuario.nome if usuario else "") or "fiscalização"
    s.execute(text("""
        UPDATE diarios_obra
           SET obs_fiscalizacao = :t, obs_fiscalizacao_por = :q,
               obs_fiscalizacao_em = now(), atualizado_em = now()
         WHERE id = :i"""), {"i": linha["id"], "t": obs, "q": quem})
    registrar_evento(s, "diario_obra", linha["id"], "OBSERVADO_FISCALIZACAO",
                     {"obra_id": obra_id, "data": str(dia), "por": quem},
                     usuario.id if usuario else None)
    s.flush()
    return {"id": linha["id"], "por": quem}


# ---------------------------------------------------------------------------
# 8. O EFETIVO QUE O PONTO NÃO CONHECE
# ---------------------------------------------------------------------------
def acrescentar_extra(s: Session, obra_id: int, dia: date, dados: dict[str, Any],
                      usuario: Optional[Usuario]) -> dict[str, Any]:
    """O terceirizado que trabalhou e não bate ponto aqui."""
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado(
            "Abra e grave o diário do dia antes de acrescentar gente.")
    descricao = str(dados.get("descricao") or "").strip()
    if not descricao:
        raise ErroValidacao("Diga quem trabalhou (a pessoa ou a equipe).")
    try:
        quantidade = int(dados.get("quantidade") or 1)
    except (TypeError, ValueError):
        raise ErroValidacao("Quantidade inválida.")
    if quantidade < 1:
        raise ErroValidacao("A quantidade tem de ser pelo menos 1.")
    novo = s.execute(text("""
        INSERT INTO diario_efetivo_extra
            (diario_id, descricao, funcao, quantidade, empresa)
        VALUES (:i, :d, :f, :q, :e) RETURNING id"""),
        {"i": linha["id"], "d": descricao,
         "f": str(dados.get("funcao") or "").strip() or None,
         "q": quantidade,
         "e": str(dados.get("empresa") or "").strip() or None}).scalar()
    s.flush()
    return {"id": novo}


def remover_extra(s: Session, obra_id: int, dia: date, extra_id: int) -> None:
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Diário não encontrado.")
    apagou = s.execute(text(
        "DELETE FROM diario_efetivo_extra WHERE id = :e AND diario_id = :i"),
        {"e": extra_id, "i": linha["id"]}).rowcount
    if not apagou:
        raise ErroNaoEncontrado("Registro não encontrado.")
    s.flush()


# ---------------------------------------------------------------------------
# 9. O CALENDÁRIO DE PENDÊNCIAS — "o que está faltando"
#
# Pedido dele em 08/11/2026: *"a gente precisa visualizar por obra facilmente o
# que é que tem diário, o que é que não tem, para entender o que é que está
# pendente. Até lembrando um pouco o ponto (…) uma espécie de calendário e a
# pessoa apontando o que é que tem e o que é que não tem."*
#
# ⚑ FIM DE SEMANA E FERIADO NÃO SÃO PENDÊNCIA. Se fossem, o calendário nasceria
# com 104 faltas por ano e ninguém olharia para ele de novo — um painel que
# grita sempre não avisa nunca. Mas um sábado em que GENTE BATEU PONTO vira
# pendência, porque ali houve obra.
# ---------------------------------------------------------------------------
def calendario(s: Session, obra_id: int, ano: int, mes: int) -> dict[str, Any]:
    """Dia a dia do mês: o que tem diário, o que falta, o que não era para ter."""
    import calendar as _cal

    cab = cabecalho(s, obra_id, date(ano, mes, 1))
    _, ultimo = _cal.monthrange(ano, mes)
    primeiro_dia, ultimo_dia = date(ano, mes, 1), date(ano, mes, ultimo)

    escritos = {l["data"]: l for l in s.execute(text("""
        SELECT data, situacao, sem_expediente, relato,
               (obs_fiscalizacao IS NOT NULL) AS tem_fiscal
          FROM diarios_obra
         WHERE obra_id = :o AND data BETWEEN :i AND :f"""),
        {"o": obra_id, "i": primeiro_dia, "f": ultimo_dia}).mappings().all()}

    # UMA consulta para o mês inteiro, não uma por dia: trinta idas ao banco
    # para desenhar um calendário é o tipo de coisa que deixa a tela lenta sem
    # ninguém entender por quê.
    com_ponto = {l["data_referencia"] for l in (_consulta_opcional(s, """
            SELECT DISTINCT data_referencia FROM ponto.marcacoes
             WHERE obra_id = :o AND data_referencia BETWEEN :i AND :f
               AND status <> 'CANCELADA'""",
            o=obra_id, i=primeiro_dia, f=ultimo_dia) or [])}
    feriados = {l["data"]: l["nome"] for l in (_consulta_opcional(s, """
            SELECT data, nome FROM ponto.feriados
             WHERE data BETWEEN :i AND :f AND (obra_id IS NULL OR obra_id = :o)""",
            o=obra_id, i=primeiro_dia, f=ultimo_dia) or [])}

    inicio_obra = cab["data_inicio"]
    fim_obra = cab["data_termino"]
    hoje = date.today()
    dias, faltam = [], 0
    for n in range(1, ultimo + 1):
        d = date(ano, mes, n)
        fora = bool(
            (inicio_obra and str(d) < inicio_obra)
            or (fim_obra and str(d) > fim_obra)
            or d > hoje)
        linha = escritos.get(d)
        feriado = feriados.get(d)
        descanso = (d.weekday() >= 5 or feriado is not None) and d not in com_ponto

        if linha is not None:
            estado = ("sem_expediente" if linha["sem_expediente"]
                      else ("fechado" if linha["situacao"] == "FECHADO" else "aberto"))
        elif fora:
            estado = "fora"
        elif descanso:
            estado = "descanso"
        else:
            estado = "falta"
            faltam += 1

        dias.append({
            "dia": n, "data": str(d), "semana": d.weekday(), "estado": estado,
            "feriado": feriado or "",
            "teve_ponto": d in com_ponto,
            "tem_fiscalizacao": bool(linha["tem_fiscal"]) if linha else False,
        })

    return {"obra": cab, "ano": ano, "mes": mes, "dias": dias,
            "faltam": faltam,
            "primeiro_dia_da_semana": primeiro_dia.weekday()}


def pendencias_por_obra(s: Session, ate: Optional[date] = None, *,
                        obra_ids: Optional[list[int]] = None,
                        dias: int = 30) -> list[dict[str, Any]]:
    """Quantos dias cada obra deve de diário nos últimos N dias.

    É a visão de quem olha TODAS as obras — o painel que diz onde correr. O
    calendário mostra uma obra por vez; este diz por onde começar.
    """
    ate = ate or date.today()
    de = ate - timedelta(days=dias)
    linhas = s.execute(text("""
        SELECT o.id, o.codigo, o.nome,
               max(d.data) AS ultimo
          FROM obras o
          LEFT JOIN diarios_obra d ON d.obra_id = o.id AND d.data BETWEEN :de AND :ate
         WHERE o.status <> 'ENCERRADA'
           AND (:todas OR o.id = ANY(:obras))
         GROUP BY o.id, o.codigo, o.nome
         ORDER BY o.codigo"""),
        {"de": de, "ate": ate,
         "obras": obra_ids or [], "todas": obra_ids is None}).mappings().all()

    saida = []
    for l in linhas:
        cal_faltas = 0
        for n in range((ate - de).days + 1):
            d = de + timedelta(days=n)
            if d.weekday() >= 5:
                continue
            cal_faltas += 1
        escritos = s.execute(text("""
            SELECT count(*) FROM diarios_obra
             WHERE obra_id = :o AND data BETWEEN :de AND :ate
               AND EXTRACT(ISODOW FROM data) < 6"""),
            {"o": l["id"], "de": de, "ate": ate}).scalar() or 0
        saida.append({
            "obra_id": l["id"], "codigo": l["codigo"], "nome": l["nome"],
            "esperados": cal_faltas, "escritos": int(escritos),
            "faltam": max(0, cal_faltas - int(escritos)),
            "ultimo": str(l["ultimo"]) if l["ultimo"] else None,
        })
    return sorted(saida, key=lambda x: -x["faltam"])


# ---------------------------------------------------------------------------
# 10. O RELATÓRIO — o papel que vai para a fiscalização
#
# Pedido dele em 09/11/2026: *"antes de eu lhe mandar os modelos de diário, eu
# queria que você gerasse um seu (…) gere aí um modelo para eu avaliar."*
#
# ⚑ A DIFERENÇA ENTRE A TELA E O PAPEL, e ela é dele: *"provavelmente a gente
# vai visualizar em tela o nome das pessoas, mas eu acredito que talvez a gente
# não vá relacionar nome a nome no diário, vai colocar só quantidade e
# função."* Por isso `efetivo_do_ponto` já devolve as DUAS coisas, e é aqui que
# a escolha acontece: o papel leva a CONTAGEM POR FUNÇÃO.
#
# Faz sentido além da preferência: o RDO é documento contratual, e listar nome
# e horário de cada pessoa num papel que circula fora da empresa é expor dado
# de gente sem necessidade — a fiscalização precisa saber QUANTOS pedreiros
# havia, não quem eram.
#
# O NÚMERO DO RDO é o dia de obra (dia 234 de 361). Não inventei sequência
# própria: ela divergiria do prazo no primeiro diário esquecido, e aí o papel
# passaria a contar uma história diferente do contrato.
# ---------------------------------------------------------------------------
def relatorio(s: Session, obra_id: int, de: date, ate: Optional[date] = None,
              *, so_escritos: bool = True) -> dict[str, Any]:
    """Os dias de uma obra, prontos para imprimir. Um dia por página."""
    ate = ate or de
    if ate < de:
        raise ErroValidacao("A data final é anterior à inicial.")
    if (ate - de).days > 93:
        raise ErroValidacao(
            "O período é longo demais para um relatório só (máximo três "
            "meses). Gere por mês.")

    cab = cabecalho(s, obra_id, ate)
    escritos = {l["data"]: l for l in s.execute(text("""
        SELECT * FROM diarios_obra
         WHERE obra_id = :o AND data BETWEEN :de AND :ate
         ORDER BY data"""), {"o": obra_id, "de": de, "ate": ate}).mappings().all()}

    paginas = []
    n = 0
    while True:
        dia = de + timedelta(days=n)
        n += 1
        if dia > ate:
            break
        linha = escritos.get(dia)
        if linha is None and so_escritos:
            continue
        efetivo = efetivo_do_ponto(s, obra_id, dia)
        extras = []
        if linha is not None:
            extras = [{"descricao": e["descricao"], "funcao": e["funcao"] or "",
                       "quantidade": e["quantidade"], "empresa": e["empresa"] or ""}
                      for e in s.execute(text("""
                SELECT descricao, funcao, quantidade, empresa
                  FROM diario_efetivo_extra WHERE diario_id = :i ORDER BY id"""),
                {"i": linha["id"]}).mappings().all()]
        paginas.append({
            "data": str(dia),
            "dia_da_semana": _DIA_DA_SEMANA[dia.weekday()],
            # O cabeçalho é recalculado POR DIA: o "dia 234 de 361" do dia 10
            # não é o do dia 11, e um relatório com o mesmo número em todas as
            # páginas seria um relatório errado.
            "prazo": cabecalho(s, obra_id, dia)["prazo"],
            "existe": linha is not None,
            "sem_expediente": bool(linha["sem_expediente"]) if linha else False,
            "tempo_manha": TEMPO_ROTULOS.get(linha["tempo_manha"] or "", "—") if linha else "—",
            "tempo_tarde": TEMPO_ROTULOS.get(linha["tempo_tarde"] or "", "—") if linha else "—",
            "relato": (linha["relato"] or "") if linha else "",
            "ocorrencias": (linha["ocorrencias"] or "") if linha else "",
            "fiscalizacao_esteve": bool(linha["fiscalizacao_esteve"]) if linha else False,
            "fiscalizacao_quem": (linha["fiscalizacao_quem"] or "") if linha else "",
            "obs_fiscalizacao": (linha["obs_fiscalizacao"] or "") if linha else "",
            "obs_fiscalizacao_por": (linha["obs_fiscalizacao_por"] or "") if linha else "",
            "situacao": linha["situacao"] if linha else "ABERTO",
            # ⚑ A CONTAGEM, NÃO OS NOMES. Decisão dele, e boa prática: o RDO
            # circula fora da empresa.
            "efetivo": _efetivo_para_o_papel(efetivo, extras),
            "material": material_recebido(s, obra_id, dia),
            "correcoes": _correcoes(s, linha["id"]) if linha else [],
        })

    return {"obra": cab, "de": str(de), "ate": str(ate), "paginas": paginas,
            "gerado_em": date.today().isoformat()}


_DIA_DA_SEMANA = ("segunda-feira", "terça-feira", "quarta-feira",
                  "quinta-feira", "sexta-feira", "sábado", "domingo")


def _efetivo_para_o_papel(efetivo: dict[str, Any],
                          extras: list[dict[str, Any]]) -> dict[str, Any]:
    """Função e quantidade. O próprio e o terceirizado em linhas separadas —
    quem lê o RDO precisa distinguir mão de obra própria de contratada."""
    proprio = [{"funcao": f["funcao"], "quantidade": f["quantidade"],
                "tipo": "próprio"} for f in efetivo.get("por_funcao", [])]
    de_fora = [{"funcao": (e["funcao"] or e["descricao"]),
                "quantidade": e["quantidade"], "tipo": "terceirizado",
                "empresa": e.get("empresa") or e["descricao"]} for e in extras]
    return {
        "linhas": proprio + de_fora,
        "total_proprio": sum(f["quantidade"] for f in proprio),
        "total_terceirizado": sum(f["quantidade"] for f in de_fora),
        "total": (sum(f["quantidade"] for f in proprio)
                  + sum(f["quantidade"] for f in de_fora)),
        "indisponivel": efetivo.get("indisponivel"),
    }


def _correcoes(s: Session, diario_id: int) -> list[dict[str, Any]]:
    """As correções feitas depois do fechamento SAEM NO PAPEL.

    Esconder a correção seria o mesmo que não registrá-la: o valor do diário
    como documento está em o leitor poder ver que algo mudou, quando e por quê.
    """
    return [{"campo": l["campo"], "motivo": l["motivo"],
             "quando": str(l["criado_em"])[:16]}
            for l in s.execute(text("""
        SELECT campo, motivo, criado_em FROM diario_correcoes
         WHERE diario_id = :i ORDER BY criado_em"""),
        {"i": diario_id}).mappings().all()]
