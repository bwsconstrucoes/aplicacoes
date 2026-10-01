# -*- coding: utf-8 -*-
"""
OS DIARISTAS: quem tem dia de DIÁRIA no mês, e quantos.

*"E cadê os diaristas? Não entrou diaristas."* (dono, 28/09/2026)

⚠️ A REGRA JÁ EXISTIA — o que faltava era ligá-la. `folha_vinculo.py` é tradução
fiel de uma fórmula que roda em produção (coluna AH da aba Mobponto), e a
descoberta que ela carrega é esta: **o vínculo é decidido POR DIA, não por
pessoa.** A mesma pessoa tem dias de diária e dias de CTPS no mês em que foi
registrada — os dias entre começar a trabalhar e ser admitida são diária; do
registro em diante, CTPS.

Classificar por pessoa jogaria o mês inteiro de quem foi admitido no meio do
período para um lado só, e metade do dinheiro iria pelo caminho errado.

⚠️ O QUE ISTO JÁ CONSEGUE, E O QUE AINDA NÃO:

  **consegue** — dizer QUEM tem dia de diária e QUANTOS dias, porque para isso
  basta a DATA de cada dia de ponto (que está guardada) mais as duas datas do
  cadastro (Data de Início e Data de Admissão).

  **não consegue** — dizer QUANTO pagar. O valor da diária não está em nenhuma
  coluna que eu tenha confirmado, e os acréscimos (+20 feriado, +10 sábado, +20
  domingo, VIGIA fora) dependem de saber a OBRA e a situação de cada dia — que
  vêm dos campos do ponto cujo nome ainda não é conhecido.

  E a diferença entre as duas coisas fica DITA na tela. Mostrar "R$ 0,00" onde
  falta o valor da diária seria pior do que mostrar "falta o valor": zero tem cara
  de resposta.
"""
from __future__ import annotations

import calendar
import datetime as dt
import logging
import unicodedata
from decimal import Decimal

from . import colaboradores, folha_vinculo, ponto

logger = logging.getLogger("analisesps.folha")


def _cadastro_para_a_regra(ficha: dict) -> dict:
    """A ficha no formato que `folha_vinculo.classificar_dia` espera."""
    return {"inicio": ficha.get("data_inicio"),
            "admissao": ficha.get("data_admissao"),
            "tipo": ficha.get("tipo"),
            "contrato": ficha.get("tipo_contrato")}


def levantar(ano: int, mes: int) -> dict:
    """Quem trabalhou como diarista no mês, e quantos dias cada um.

    Sai da carga do ponto do mês cruzada com o cadastro. Sem carga do ponto não há
    o que dizer — e a tela diz isso, em vez de mostrar lista vazia."""
    from .db import consultar

    carga = None
    try:
        carga = ponto.carga_do_mes(ano, mes)
    except Exception:  # noqa: BLE001 — a tela tem de dizer o que houve
        logger.exception("Diaristas: não consegui ler a carga do ponto")
    if not carga:
        return {"tem_ponto": False, "pessoas": [], "carga": None,
                "dias_de_diaria": 0, "quantos": 0, "sem_cadastro": []}

    # Os dias por pessoa, de uma consulta só. A data é o que a regra precisa.
    linhas = consultar(
        "SELECT cpf, nome, data FROM analisesps.ponto_dia "
        " WHERE carga_id = ? AND data IS NOT NULL ORDER BY cpf, data",
        (carga["id"],))
    dias_por_cpf: dict = {}
    nome_do_ponto: dict = {}
    for cpf, nome, data in linhas:
        dias_por_cpf.setdefault(cpf, []).append(data)
        nome_do_ponto.setdefault(cpf, nome or "")

    fichas = colaboradores.muitos_por_cpf(list(dias_por_cpf))
    obras_por_nome = colaboradores.codigos_das_obras()

    pessoas, sem_cadastro = [], []
    for cpf, dias in dias_por_cpf.items():
        ficha = fichas.get(cpf)
        if not ficha:
            # ⚠️ NÃO DESAPARECE DA LISTA. Quem bate ponto e não está no cadastro é
            # justamente o caso que dá confusão — e esconder faria alguém
            # trabalhar e não receber, sem nada na tela.
            sem_cadastro.append({"cpf": cpf, "nome": nome_do_ponto.get(cpf, ""),
                                 "dias": len(dias)})
            continue
        contagem = folha_vinculo.classificar_dias(
            dias, _cadastro_para_a_regra(ficha))
        de_diaria = contagem.get(folha_vinculo.DIARIA, 0)
        if not de_diaria:
            continue
        pessoas.append({
            "cpf": cpf, "cpf_bonito": ficha.get("cpf_bonito", ""),
            "nome": ficha.get("nome") or nome_do_ponto.get(cpf, ""),
            "cargo": ficha.get("cargo", ""),
            "obra": colaboradores.resolver_obra(ficha, obras_por_nome),
            "link_pipefy": ficha.get("link_pipefy", ""),
            "dias_de_diaria": de_diaria,
            "dias_de_ctps": contagem.get(folha_vinculo.CTPS, 0),
            "dias_sem_decidir": (contagem.get(folha_vinculo.FALTA_DATA, 0)
                                 + contagem.get(folha_vinculo.NAO_ENCONTRADO, 0)),
            "data_inicio": ficha.get("data_inicio"),
            "data_admissao": ficha.get("data_admissao"),
            "tipo_contrato": ficha.get("tipo_contrato", ""),
            "contagem": contagem,
        })

    # Quem tem dia sem decidir vem primeiro: é o cadastro pela metade, e é o que
    # trava o pagamento.
    pessoas.sort(key=lambda p: (p["dias_sem_decidir"] == 0,
                                (p["nome"] or "").lower()))
    return {
        "tem_ponto": True, "carga": carga, "pessoas": pessoas,
        "quantos": len(pessoas),
        "dias_de_diaria": sum(p["dias_de_diaria"] for p in pessoas),
        "com_pendencia": [p for p in pessoas if p["dias_sem_decidir"]],
        "sem_cadastro": sem_cadastro,
        # ⚠️ O QUE FALTA PARA VIRAR DINHEIRO. Fica no resultado para a tela dizer,
        # em vez de mostrar zero com cara de resposta.
        "falta_para_pagar": [
            "o VALOR da diária de cada pessoa (não achei a coluna no cadastro)",
            "o nome dos campos de cada dia do ponto, que é o que diz a OBRA do dia "
            "e se foi sábado, domingo ou feriado (os acréscimos de +10 e +20)",
        ],
    }


# ===========================================================================
# O PAGAMENTO DOS DIARISTAS — 01/10/2026
#
# O dono: *"a prioridade agora é a de diaristas, que é o próximo que eu vou
# gerar"*, e que as outras folhas herdem o que a da contabilidade ganhou:
# desligados fora, filtros, seleção de quem paga, fechar, gerar e card.
#
# A REGRA É A DA ABA "Diaristas" DA PLANILHA (§7.14.12 e §7.10.4 do
# `docs/FOLHA_DE_PAGAMENTO.md`), lida das fórmulas:
#
#   - só os dias que a regra de vínculo diz DIÁRIA (antes da admissão, ou
#     prestador RPA) — `folha_vinculo`;
#   - VIGIA fica fora (a planilha exclui pela função);
#   - quem está sem valor de diária no cadastro não é pago ("CORRIGIR VALOR
#     DIÁRIA") — aqui fica na lista, marcado, em vez de sumir;
#   - a quantidade do dia: 1 com presença; 0,5 em PRESENÇA PARCIAL acima de 7h;
#     0,5 em falta justificada de sábado ou domingo acima de 6h30;
#   - o valor do dia: quantidade × valor da diária, mais 20 no feriado, 10 no
#     sábado e 20 no domingo;
#   - o diarista NÃO tem desconto de dia compensado (só a diária extra de quem é
#     CTPS tem).
#
# ⚠️ A REGRA DA MEIA DIÁRIA FOI ESCRITA A PARTIR DA LEITURA DA FÓRMULA, que não
# está mais à mão. Por isso cada dia mostra quanto contou e por quê — quem
# confere vê a regra funcionando e acha o caso que não bate.
# ===========================================================================
CENTAVO = Decimal("0.01")

PERIODOS = {"mes": "Mês inteiro", "quinzena": "1 a 15", "fim_de_mes": "16 ao fim"}
# Sob qual pagamento o fechamento fica guardado (a tela de gerar trabalha com
# quinzena e fim de mês). O mês inteiro é pago junto do fim de mês.
TIPO_DO_FECHAMENTO = {"mes": "fim_de_mes", "quinzena": "quinzena",
                      "fim_de_mes": "fim_de_mes"}
VERBA = "diaria"

ADICIONAL_FERIADO = Decimal("20.00")
ADICIONAL_SABADO = Decimal("10.00")
ADICIONAL_DOMINGO = Decimal("20.00")
MINUTOS_MEIA_PARCIAL = 7 * 60          # PRESENÇA PARCIAL acima de 7h → 0,5
MINUTOS_MEIA_FIM_DE_SEMANA = 6 * 60 + 30   # falta justificada sáb/dom > 6h30


def periodo(ano: int, mes: int, qual: str) -> tuple:
    ultimo = calendar.monthrange(int(ano), int(mes))[1]
    if qual == "quinzena":
        return dt.date(ano, mes, 1), dt.date(ano, mes, 15)
    if qual == "fim_de_mes":
        return dt.date(ano, mes, 16), dt.date(ano, mes, ultimo)
    return dt.date(ano, mes, 1), dt.date(ano, mes, ultimo)


def _sem_acento(texto) -> str:
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    return " ".join("".join(c for c in cru if not unicodedata.combining(c))
                    .lower().split())


def _minutos(texto) -> int:
    """"07:30" → 450. Aceita "7:30", "07:30:00"; o que não ler vira 0."""
    partes = str(texto or "").strip().split(":")
    try:
        return int(partes[0]) * 60 + (int(partes[1]) if len(partes) > 1 else 0)
    except (ValueError, IndexError):
        return 0


def e_vigia(ficha: dict) -> bool:
    return "vigia" in _sem_acento(ficha.get("cargo"))


def quantidade_do_dia(lido: dict, tem_obra: bool) -> tuple:
    """`(quantidade, motivo)` — 1, 0,5 ou 0 diária neste dia."""
    presenca = _sem_acento(lido.get("presenca"))
    falta = _sem_acento(lido.get("falta"))
    minutos = _minutos(lido.get("total_de_horas"))
    data = lido.get("data")
    fim_de_semana = isinstance(data, dt.date) and data.weekday() >= 5
    if "parcial" in presenca:
        if minutos > MINUTOS_MEIA_PARCIAL:
            return Decimal("0.5"), "presença parcial acima de 7h: meia diária"
        return Decimal("0"), "presença parcial de até 7h: não conta"
    if fim_de_semana and "justific" in (presenca + " " + falta) \
            and minutos > MINUTOS_MEIA_FIM_DE_SEMANA:
        return Decimal("0.5"), "falta justificada no fim de semana acima de 6h30"
    if tem_obra and (not presenca or presenca.startswith("presenca")):
        return Decimal("1"), ""
    if falta:
        return Decimal("0"), "falta"
    if presenca:
        return Decimal("0"), lido.get("presenca") or ""
    return Decimal("0"), "sem marcação de obra"


def _feriados(inicio, fim) -> list:
    from . import folha_calendario
    from .db import consultar
    if not folha_calendario._pronto():
        return []
    return [{"data": l[0], "abrangencia": l[1],
             "obra": " ".join(str(l[2] or "").split()).upper()}
            for l in consultar(
                "SELECT data, abrangencia, obra FROM analisesps.feriado "
                " WHERE data >= ? AND data <= ?", (inicio, fim))]


def _e_feriado(data, obra: str, feriados: list, codigo_por_nome: dict) -> bool:
    """O nacional vale sempre; o da obra só na obra — que no ponto é o CÓDIGO e
    no cadastro de feriados pode ser o NOME."""
    from . import folha_calendario
    for f in feriados:
        if f["data"] != data:
            continue
        if f["abrangencia"] == folha_calendario.NACIONAL:
            return True
        if obra and (f["obra"] == obra or codigo_por_nome.get(f["obra"]) == obra):
            return True
    return False


def adicional_do_dia(data, e_feriado: bool) -> tuple:
    if e_feriado:
        return ADICIONAL_FERIADO, "feriado"
    if data.weekday() == 5:
        return ADICIONAL_SABADO, "sábado"
    if data.weekday() == 6:
        return ADICIONAL_DOMINGO, "domingo"
    return Decimal("0.00"), ""


def calcular_pessoa(ficha: dict, dias_lidos: list, inicio, fim,
                    valor_diaria, feriados: list, codigo_por_nome: dict,
                    obra_do_cadastro: str = "", ajuste: dict | None = None) -> dict:
    """A diária de uma pessoa no período, dia a dia. Devolve o caminho inteiro."""
    from . import folha_apropriacao
    from .folha_auxilio import _decidir

    ajuste = ajuste or {}
    cadastro = _cadastro_para_a_regra(ficha)
    valor_diaria = (None if valor_diaria is None
                    else Decimal(str(valor_diaria)).quantize(CENTAVO))
    saida = {
        "cpf": ficha.get("cpf", ""), "cpf_bonito": ficha.get("cpf_bonito", ""),
        "nome": ficha.get("nome", ""), "cargo": ficha.get("cargo", ""),
        "fase": ficha.get("fase") or "", "situacao": ficha.get("situacao") or "",
        "link_pipefy": ficha.get("link_pipefy", ""),
        "tipo_contrato": ficha.get("tipo_contrato", ""),
        "data_inicio": ficha.get("data_inicio"),
        "data_admissao": ficha.get("data_admissao"),
        "obra_cadastro": obra_do_cadastro,
        "valor_diaria": valor_diaria, "dias": [],
        "quantidade": Decimal("0"), "adicionais": Decimal("0.00"),
        "valor": Decimal("0.00"), "por_obra": [], "obra": "",
        "dias_de_ctps": 0, "dias_sem_decidir": 0,
        "motivos": [], "pagar": True, "impossivel": False,
        "desligado": False, "vigia": e_vigia(ficha),
        "ajuste_pagar": ajuste.get("pagar"),
    }
    por_obra: dict = {}
    for lido in sorted(dias_lidos or [], key=lambda d: d.get("data") or dt.date.min):
        data = lido.get("data")
        if not isinstance(data, dt.date) or not (inicio <= data <= fim):
            continue
        vinculo = folha_vinculo.classificar_dia(data, cadastro)
        if vinculo == folha_vinculo.CTPS:
            saida["dias_de_ctps"] += 1
            continue
        if vinculo != folha_vinculo.DIARIA:
            saida["dias_sem_decidir"] += 1
            continue
        decidido = folha_apropriacao.obra_do_dia(
            lido.get("marcacoes"), lido.get("presenca", ""), lido.get("falta", ""))
        obra = decidido.get("obra") or ""
        qtd, motivo = quantidade_do_dia(lido, bool(obra))
        if qtd and not obra:
            obra = obra_do_cadastro
        feriado = _e_feriado(data, obra, feriados, codigo_por_nome)
        adicional, porque = adicional_do_dia(data, feriado) if qtd else (Decimal("0.00"), "")
        valor = ((qtd * valor_diaria + adicional).quantize(CENTAVO)
                 if valor_diaria is not None and qtd else Decimal("0.00"))
        saida["dias"].append({
            "data": data, "obra": obra, "quantidade": qtd, "motivo": motivo,
            "adicional": adicional, "porque_adicional": porque, "valor": valor,
            "presenca": lido.get("presenca", ""),
            "horas": lido.get("total_de_horas", ""),
            "batidas": list(zip(lido.get("horas") or [], lido.get("marcacoes") or []))})
        if qtd:
            saida["quantidade"] += qtd
            saida["adicionais"] += adicional
            saida["valor"] += valor
            alvo = por_obra.setdefault(obra or "(SEM OBRA)", {
                "obra": obra or "(SEM OBRA)", "dias": Decimal("0"),
                "valor": Decimal("0.00")})
            alvo["dias"] += qtd
            alvo["valor"] += valor
    saida["por_obra"] = sorted(por_obra.values(), key=lambda o: -o["valor"])
    saida["obra"] = saida["por_obra"][0]["obra"] if saida["por_obra"] else ""
    saida["obras"] = [o["obra"] for o in saida["por_obra"]]

    # POLÍTICA — dá para calcular; o que se decidiu é não pagar.
    if saida["situacao"] == colaboradores.SITUACAO_SAIU:
        saida["desligado"] = True
        saida["pagar"] = False
        saida["motivos"].append(ficha.get("motivo") or "já saiu da empresa.")
    elif saida["situacao"] == colaboradores.SITUACAO_SAINDO:
        saida["motivos"].append(ficha.get("motivo") or "está saindo.")
    if saida["vigia"]:
        saida["pagar"] = False
        saida["motivos"].append(
            "vigia não recebe diária por aqui (regra da planilha de diaristas).")
    if saida["dias_sem_decidir"]:
        saida["motivos"].append(
            f"{saida['dias_sem_decidir']} dia(s) sem data de início ou admissão "
            "no cadastro — não dá para saber se são diária.")
    # IMPOSSÍVEL — não há valor para pagar, e marcar pagaria zero.
    if valor_diaria is None:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            "o cadastro não tem o valor da diária. Corrija no card do Pipefy e "
            "atualize o cadastro.")
    elif not saida["quantidade"]:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append("nenhum dia de diária com presença no período.")
    return _decidir(saida, ajuste)


def _tipo_do_ajuste(qual: str) -> str:
    return f"{VERBA}_{qual}"


def calcular(ano: int, mes: int, qual: str = "mes") -> dict:
    """Os diaristas do período, com o valor de cada um e os totais."""
    from . import folha_auxilio
    if qual not in PERIODOS:
        qual = "mes"
    ano, mes = int(ano), int(mes)
    inicio, fim = periodo(ano, mes, qual)
    base = {"ano": ano, "mes": mes, "qual": qual, "rotulo_periodo": PERIODOS[qual],
            "competencia": f"{mes:02d}/{ano}", "inicio": inicio, "fim": fim,
            "tem_ponto": False, "tem_coluna_da_diaria": colaboradores.tem_valor_diaria(),
            "pessoas": [], "sem_cadastro": [], "total": Decimal("0.00"),
            "quantos_a_pagar": 0, "por_obra": [], "fechamento": None}
    carga = None
    try:
        carga = ponto.carga_do_mes(ano, mes)
    except Exception:  # noqa: BLE001
        logger.exception("Diaristas: não consegui ler a carga do ponto")
    if not carga:
        return base
    base["tem_ponto"] = True
    base["carga"] = carga

    dias_por_cpf = ponto.dias_por_cpf(ano, mes)
    fichas = colaboradores.muitos_por_cpf(list(dias_por_cpf), ate=fim)
    valores = colaboradores.valores_de_diaria(list(fichas))
    codigo_por_nome = colaboradores.codigos_das_obras()
    feriados = _feriados(inicio, fim)
    ajustes = folha_auxilio.ajustes_do_mes(_tipo_do_ajuste(qual), ano, mes)

    pessoas, sem_cadastro = [], []
    for cpf, dias in dias_por_cpf.items():
        no_periodo = [d for d in dias if isinstance(d.get("data"), dt.date)
                      and inicio <= d["data"] <= fim]
        if not no_periodo:
            continue
        ficha = fichas.get(cpf)
        if not ficha:
            sem_cadastro.append({"cpf": cpf, "dias": len(no_periodo)})
            continue
        p = calcular_pessoa(
            ficha, no_periodo, inicio, fim, valores.get(cpf), feriados,
            codigo_por_nome, colaboradores.resolver_obra(ficha, codigo_por_nome),
            ajustes.get(cpf))
        if not p["dias"]:
            continue          # nenhum dia de diária: não é diarista neste período
        pessoas.append(p)

    if sem_cadastro:
        # O nome como veio do ponto: é o que permite achar a pessoa no Mobponto.
        from .db import consultar
        nomes = dict(consultar(
            "SELECT cpf, max(nome) FROM analisesps.ponto_dia "
            " WHERE carga_id = ? GROUP BY cpf", (carga["id"],)))
        for s in sem_cadastro:
            s["nome"] = nomes.get(s["cpf"], "")

    pessoas.sort(key=lambda p: (p["pagar"], (p["nome"] or "").lower()))
    a_pagar = [p for p in pessoas if p["pagar"] and p["valor"] > 0]
    por_obra: dict = {}
    for p in a_pagar:
        for o in p["por_obra"]:
            alvo = por_obra.setdefault(o["obra"], {"obra": o["obra"], "pessoas": 0,
                                                   "dias": Decimal("0"),
                                                   "total": Decimal("0.00")})
            alvo["pessoas"] += 1
            alvo["dias"] += o["dias"]
            alvo["total"] += o["valor"]
    base.update({
        "pessoas": pessoas, "sem_cadastro": sem_cadastro,
        "quantos": len(pessoas), "quantos_a_pagar": len(a_pagar),
        "total": sum((p["valor"] for p in a_pagar), Decimal("0.00")),
        "por_obra": sorted(por_obra.values(), key=lambda o: -o["total"]),
        "fechamento": _fechamento(ano, mes, qual),
    })
    return base


def _fechamento(ano: int, mes: int, qual: str):
    from . import folha_apropriacao_guardada as guardada
    try:
        return guardada.fechamento(ano, mes, TIPO_DO_FECHAMENTO[qual], VERBA)
    except Exception:  # noqa: BLE001
        logger.exception("Diaristas: não consegui ler o fechamento")
        return None


def salvar_selecao(ano: int, mes: int, qual: str, decisoes, quem: str = "") -> dict:
    """Guarda de uma vez quem vai e quem não vai ser pago — só as exceções, como
    no auxílio (`folha_auxilio.salvar_selecao`)."""
    from . import folha_auxilio
    from .db import conexao
    from .folha_rateio import so_digitos
    if not folha_auxilio._pronto():
        raise folha_auxilio.ErroDoAuxilio(
            'a tabela dos ajustes ainda não existe. Aperte "Aplicar '
            'atualizações do banco" em Configurações.')
    calculado = calcular(ano, mes, qual)
    por_cpf = {p["cpf"]: p for p in calculado["pessoas"]}
    tipo = _tipo_do_ajuste(calculado["qual"])
    gravados = limpos = 0
    ignorados = []
    with conexao() as conn:
        for item in decisoes or []:
            cpf = so_digitos((item or {}).get("cpf"))
            pessoa = por_cpf.get(cpf)
            if not pessoa:
                ignorados.append(cpf)
                continue
            querido = bool((item or {}).get("pagar"))
            if querido == bool(pessoa.get("pagar_calculado")):
                cur = conn.execute(
                    "DELETE FROM analisesps.auxilio_ajuste "
                    " WHERE tipo = ? AND ano = ? AND mes = ? AND cpf = ?",
                    (tipo, int(ano), int(mes), cpf))
                limpos += 1 if (cur.rowcount or 0) > 0 else 0
                cur.close()
                continue
            if querido and pessoa.get("impossivel"):
                ignorados.append(cpf)
                continue
            conn.execute(
                "INSERT INTO analisesps.auxilio_ajuste "
                "  (tipo, ano, mes, cpf, pagar, alterado_por) VALUES (?,?,?,?,?,?) "
                " ON CONFLICT (tipo, ano, mes, cpf) DO UPDATE SET "
                "   pagar = EXCLUDED.pagar, alterado_em = now(), "
                "   alterado_por = EXCLUDED.alterado_por",
                (tipo, int(ano), int(mes), cpf, querido, str(quem or "")[:120]))
            gravados += 1
        conn.commit()
    logger.info("Diaristas: seleção de %02d/%d (%s) salva por %s — %d exceção(ões).",
                int(mes), int(ano), qual, quem or "(sem nome)", gravados)
    return {"gravados": gravados, "limpos": limpos, "ignorados": ignorados}


def apropriado(calculado: dict) -> dict:
    """O resultado no formato que `folha_apropriacao_guardada.fechar` guarda:
    pessoa por pessoa, a diária de cada obra."""
    pessoas = []
    for p in calculado["pessoas"]:
        pessoas.append({
            "cpf": p["cpf"], "nome": p["nome"], "nome_cadastro": p["nome"],
            "fora": not (p["pagar"] and p["valor"] > 0),
            "por_obra": [{"obra": o["obra"],
                          # A tabela guarda dias inteiros; a meia diária fica no
                          # valor, que é o que paga.
                          "dias": int(o["dias"].to_integral_value()),
                          "valor": o["valor"], "origem": "ponto"}
                         for o in p["por_obra"]]})
    total = calculado["total"]
    return {"pessoas": pessoas, "total_da_folha": total,
            "total_apropriado": total, "fecha": True}


def fechar(ano: int, mes: int, qual: str, quem: str = "") -> dict:
    """Congela a diária do período, para o arquivo poder sair."""
    from . import folha_apropriacao_guardada as guardada
    calculado = calcular(ano, mes, qual)
    if not calculado["tem_ponto"]:
        raise guardada.ErroDaApropriacao(
            "o ponto deste mês ainda não foi trazido — não há diária para fechar.")
    if not calculado["quantos_a_pagar"]:
        raise guardada.ErroDaApropriacao("ninguém marcado para receber diária.")
    sem_obra = [p["nome"] for p in calculado["pessoas"] if p["pagar"]
                and any(o["obra"] == "(SEM OBRA)" for o in p["por_obra"])]
    if sem_obra:
        raise guardada.ErroDaApropriacao(
            "há diária sem obra (sem marcação no ponto e sem obra no cadastro): "
            + ", ".join(sem_obra[:5]) + ". Sem obra não há conta para pagar.")
    novo = guardada.fechar(ano, mes, TIPO_DO_FECHAMENTO[calculado["qual"]],
                           apropriado(calculado), verba=VERBA, quem=quem)
    return {"id": novo, "pessoas": calculado["quantos_a_pagar"],
            "total": calculado["total"],
            "tipo": TIPO_DO_FECHAMENTO[calculado["qual"]]}
