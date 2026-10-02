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
            "o VALOR da diária de cada colaborador (coluna não encontrada no cadastro)",
            "o nome dos campos de cada dia do ponto, que identificam a OBRA do dia "
            "e se foi sábado, domingo ou feriado (acréscimos de +10 e +20)",
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
#   - a quantidade do dia: a coluna AJ, traduzida termo a termo mais abaixo
#     (fórmula enviada pelo dono em 02/10/2026);
#   - o valor do dia: quantidade × valor da diária, mais 20 no feriado, 10 no
#     sábado e 20 no domingo;
#   - o diarista NÃO tem desconto de dia compensado (só a diária extra de quem é
#     CTPS tem).
#
# Cada dia mostra quanto contou e por quê — quem confere vê a regra funcionando e
# acha o caso que não bate.
# ===========================================================================
CENTAVO = Decimal("0.01")

# ⚠️ DIARISTA É PAGO POR QUINZENA (dono, 02/10/2026). A primeira versão tinha
# também "mês inteiro" como padrão — suposição minha, retirada.
PERIODOS = {"quinzena": "1ª quinzena (1 a 15)", "fim_de_mes": "2ª quinzena (16 ao fim)"}
# Sob qual pagamento o fechamento fica guardado (a tela de gerar trabalha com
# quinzena e fim de mês).
TIPO_DO_FECHAMENTO = {"quinzena": "quinzena", "fim_de_mes": "fim_de_mes"}


def periodo_sugerido(hoje) -> tuple:
    """`(ano, mes, periodo)` que a tela abre. Até o dia 10, a 2ª quinzena do mês
    anterior (paga no início do mês); depois, a 1ª quinzena do mês corrente."""
    if hoje.day <= 10:
        anterior = hoje.replace(day=1) - dt.timedelta(days=1)
        return anterior.year, anterior.month, "fim_de_mes"
    return hoje.year, hoje.month, "quinzena"
VERBA = "diaria"

ADICIONAL_FERIADO = Decimal("20.00")
ADICIONAL_SABADO = Decimal("10.00")
ADICIONAL_DOMINGO = Decimal("20.00")


def periodo(ano: int, mes: int, qual: str) -> tuple:
    ultimo = calendar.monthrange(int(ano), int(mes))[1]
    if qual == "quinzena":
        return dt.date(ano, mes, 1), dt.date(ano, mes, 15)
    return dt.date(ano, mes, 16), dt.date(ano, mes, ultimo)


def _sem_acento(texto) -> str:
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    return " ".join("".join(c for c in cru if not unicodedata.combining(c))
                    .lower().split())


def e_vigia(ficha: dict) -> bool:
    """A função é VIGIA? Igualdade, como a planilha (`W != 'VIGIA'`) — "VIGIA
    NOTURNO" é outra função e recebe diária."""
    return _sem_acento(ficha.get("cargo")) == "vigia"


# ===========================================================================
# A QUANTIDADE DO DIA — tradução da coluna AJ ("QTD DIÁRIAS") da aba MobPonto.
#
# O dono mandou a fórmula em 02/10/2026, junto com a da AG (horas trabalhadas).
# A primeira versão fora escrita de memória da leitura de 27/09 e estava ERRADA
# no essencial: presença parcial com 7h ou mais vale UMA diária (não meia), e
# abaixo de 7h vale MEIA (não zero).
#
#   I, J, K, L = hr_entrada, hr_almoco, hr_retorno, hr_saida
#   AG = SE(K="";J-I;SE(L="";K-I;L-I))           (fração do dia)
#   X = Tipo de Cadastro   AA = Tipo de Contrato   AH = vínculo do dia
#   Q = presença           M = descrição da falta  AQ = PAGAR DIÁRIA / EXTRA
#   D = dia da semana
#
#   1   — Prestador RPA, PRESENÇA, M ≠ "Falta não justificada"
#       — CTPS em dia de DIÁRIA, PRESENÇA
#       — CTPS em dia de CTPS, PRESENÇA, PAGAR EXTRA, M ≠ "Falta Justificada"
#   1   — Prestador RPA, PRESENÇA PARCIAL, AG > 0,2916 (7h)
#       — CTPS em dia de DIÁRIA, PRESENÇA PARCIAL, AG > 0,2916
#       — CTPS em dia de CTPS, PRESENÇA, PAGAR EXTRA, "Falta Justificada",
#         sábado ou domingo, AG > 0,2708 (6h30)
#       — CTPS em dia de CTPS, PRESENÇA PARCIAL, PAGAR EXTRA, AG > 0,2916
#   0,5 — Prestador RPA, PRESENÇA PARCIAL, AG < 0,2916
#       — Prestador em dia de DIÁRIA, PRESENÇA, "Falta não justificada"
#       — CTPS em dia de DIÁRIA, PRESENÇA PARCIAL, AG < 0,2916
#       — CTPS em dia de CTPS, PRESENÇA, PAGAR EXTRA, "Falta Justificada"
#       — CTPS em dia de CTPS, PRESENÇA PARCIAL, PAGAR EXTRA, AG < 0,2916
#   vazio no resto (e a aba Diaristas só pega AJ > 0).
#
# ⚠️ OS LIMITES SÃO OS NÚMEROS DA FÓRMULA, em fração do dia, e não "7h" redondo:
# 0,2916 dia = 6h59min56s. Comparar em minutos inteiros com 420 daria o mesmo na
# prática, mas o número da planilha é o que vale — e fica aqui para conferência.
# A ordem dos blocos também é a da fórmula: o primeiro que casa decide.
#
# ⚠️ A FÓRMULA NÃO OLHA A OBRA. Dia com presença e sem obra marcada conta; a
# obra cai na do cadastro, e o dia fica marcado para conferência.
# ===========================================================================
LIMITE_PARCIAL = Decimal("0.2916")
LIMITE_FIM_DE_SEMANA = Decimal("0.2708")
SEGUNDOS_DO_DIA = 86400

_PRESTADOR = "prestador de servico"
_RPA = "autonomo (rpa)"
_CTPS = "ctps"
_PRESENCA = "presenca"
_PRESENCA_PARCIAL = "presenca parcial"
_FALTA_JUSTIFICADA = "falta justificada"
_FALTA_NAO_JUSTIFICADA = "falta nao justificada"
PAGAR_DIARIA = "PAGAR DIÁRIA"
PAGAR_EXTRA = "PAGAR EXTRA"


def _segundos(texto) -> int:
    """"07:30" → 27000. Célula vazia vale ZERO, como na conta da planilha."""
    partes = str(texto or "").strip().split(":")
    try:
        return (int(partes[0]) * 3600
                + (int(partes[1]) * 60 if len(partes) > 1 else 0)
                + (int(partes[2]) if len(partes) > 2 else 0))
    except (ValueError, IndexError):
        return 0


def horas_trabalhadas(horas) -> Decimal:
    """Coluna AG, em fração do dia: saída − entrada; sem saída, retorno −
    entrada; sem retorno, almoço − entrada. Tradução literal — inclusive o
    intervalo do almoço, que a fórmula não desconta."""
    i, j, k, l = (list(horas or []) + ["", "", "", ""])[:4]
    if not str(k or "").strip():
        segundos = _segundos(j) - _segundos(i)
    elif not str(l or "").strip():
        segundos = _segundos(k) - _segundos(i)
    else:
        segundos = _segundos(l) - _segundos(i)
    return Decimal(segundos) / Decimal(SEGUNDOS_DO_DIA)


def _hhmm(fracao: Decimal) -> str:
    segundos = int(fracao * SEGUNDOS_DO_DIA)
    if segundos <= 0:
        return ""
    return f"{segundos // 3600:02d}:{segundos % 3600 // 60:02d}"


def quantidade_da_planilha(tipo, contrato, vinculo, aq, presenca, falta,
                           ag: Decimal, fim_de_semana: bool) -> Decimal:
    """A coluna AJ, termo a termo. Devolve 1, 0,5 ou 0 (o "vazio")."""
    x, aa = _sem_acento(tipo), _sem_acento(contrato)
    q, m = _sem_acento(presenca), _sem_acento(falta)
    ah = vinculo
    extra = _sem_acento(aq) == _sem_acento(PAGAR_EXTRA)
    rpa = x == _PRESTADOR and aa == _RPA
    ctps_diaria = x == _CTPS and ah == folha_vinculo.DIARIA
    ctps_ctps = x == _CTPS and ah == folha_vinculo.CTPS
    presenca_cheia, parcial = q == _PRESENCA, q == _PRESENCA_PARCIAL
    fj, fnj = m == _FALTA_JUSTIFICADA, m == _FALTA_NAO_JUSTIFICADA

    if ((rpa and presenca_cheia and not fnj)
            or (ctps_diaria and presenca_cheia)
            or (ctps_ctps and presenca_cheia and extra and not fj)):
        return Decimal("1")
    if ((rpa and parcial and ag > LIMITE_PARCIAL)
            or (ctps_diaria and parcial and ag > LIMITE_PARCIAL)
            or (ctps_ctps and presenca_cheia and extra and fj and fim_de_semana
                and ag > LIMITE_FIM_DE_SEMANA)
            or (ctps_ctps and parcial and extra and ag > LIMITE_PARCIAL)):
        return Decimal("1")
    if ((rpa and parcial and ag < LIMITE_PARCIAL)
            or (x == _PRESTADOR and ah == folha_vinculo.DIARIA and presenca_cheia
                and fnj)
            or (ctps_diaria and parcial and ag < LIMITE_PARCIAL)
            or (ctps_ctps and presenca_cheia and extra and fj)
            or (ctps_ctps and parcial and extra and ag < LIMITE_PARCIAL)):
        return Decimal("0.5")
    return Decimal("0")


def e_rpa(cadastro: dict) -> bool:
    return (_sem_acento(cadastro.get("tipo")) == _PRESTADOR
            and _sem_acento(cadastro.get("contrato")) == _RPA)


def quantidade_do_dia(lido: dict, cadastro: dict, vinculo: str,
                      aq: str = PAGAR_DIARIA) -> tuple:
    """`(quantidade, motivo)` de um dia — a AJ com o motivo escrito para a tela."""
    data = lido.get("data")
    fim_de_semana = isinstance(data, dt.date) and data.weekday() >= 5
    ag = horas_trabalhadas(lido.get("horas"))
    qtd = quantidade_da_planilha(
        cadastro.get("tipo"), cadastro.get("contrato"), vinculo, aq,
        lido.get("presenca"), lido.get("falta"), ag, fim_de_semana)
    q, m = _sem_acento(lido.get("presenca")), _sem_acento(lido.get("falta"))
    horas = _hhmm(ag)
    if qtd == 1:
        if q == _PRESENCA_PARCIAL:
            return qtd, f"presença parcial com {horas or '0h'} (acima de 7h): diária integral"
        return qtd, ""
    if qtd:
        if q == _PRESENCA_PARCIAL:
            return qtd, f"presença parcial com {horas or '0h'} (abaixo de 7h): meia diária"
        if m == _FALTA_NAO_JUSTIFICADA:
            return qtd, "presença com falta não justificada: meia diária"
        return qtd, "meia diária"
    if m:
        return qtd, lido.get("falta") or "falta"
    if q:
        return qtd, f"{lido.get('presenca')}: não computado"
    return qtd, "sem presença registrada"


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
    rpa = e_rpa(cadastro)
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
        # A coluna AQ: o prestador RPA é sempre PAGAR DIÁRIA; os outros, só no
        # dia que a AH diz DIÁRIA (§7.10.3 do docs/FOLHA_DE_PAGAMENTO.md).
        if not rpa:
            if vinculo == folha_vinculo.CTPS:
                saida["dias_de_ctps"] += 1
                continue
            if vinculo != folha_vinculo.DIARIA:
                saida["dias_sem_decidir"] += 1
                continue
        decidido = folha_apropriacao.obra_do_dia(
            lido.get("marcacoes"), lido.get("presenca", ""), lido.get("falta", ""))
        obra = decidido.get("obra") or ""
        qtd, motivo = quantidade_do_dia(lido, cadastro, vinculo)
        if qtd and not obra:
            obra = obra_do_cadastro
            motivo = "; ".join(x for x in (
                motivo, "obra não informada no ponto: atribuída à obra do cadastro") if x)
        feriado = _e_feriado(data, obra, feriados, codigo_por_nome)
        adicional, porque = adicional_do_dia(data, feriado) if qtd else (Decimal("0.00"), "")
        valor = ((qtd * valor_diaria + adicional).quantize(CENTAVO)
                 if valor_diaria is not None and qtd else Decimal("0.00"))
        saida["dias"].append({
            "data": data, "obra": obra, "quantidade": qtd, "motivo": motivo,
            "adicional": adicional, "porque_adicional": porque, "valor": valor,
            "presenca": lido.get("presenca", ""),
            "horas": _hhmm(horas_trabalhadas(lido.get("horas")))
                     or lido.get("total_de_horas", ""),
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
        saida["motivos"].append(ficha.get("motivo") or "colaborador desligado.")
    elif saida["situacao"] == colaboradores.SITUACAO_SAINDO:
        saida["motivos"].append(ficha.get("motivo") or "em processo de desligamento.")
    if saida["vigia"]:
        saida["pagar"] = False
        saida["motivos"].append(
            "vigia não recebe diária por este módulo (regra da planilha de diaristas).")
    if saida["dias_sem_decidir"]:
        saida["motivos"].append(
            f"{saida['dias_sem_decidir']} dia(s) sem data de início ou admissão "
            "no cadastro — não é possível determinar se são diária.")
    # IMPOSSÍVEL — não há valor para pagar, e marcar pagaria zero.
    if valor_diaria is None:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append(
            "o cadastro não informa o valor da diária. Corrija no card do Pipefy e "
            "atualize o cadastro.")
    elif not saida["quantidade"]:
        saida["pagar"] = False
        saida["impossivel"] = True
        saida["motivos"].append("nenhum dia de diária com presença no período.")
    return _decidir(saida, ajuste)


def _tipo_do_ajuste(qual: str) -> str:
    return f"{VERBA}_{qual}"


def calcular(ano: int, mes: int, qual: str = "quinzena") -> dict:
    """Os diaristas do período, com o valor de cada um e os totais."""
    from . import folha_auxilio
    if qual not in PERIODOS:
        qual = "quinzena"
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
            'tabela de ajustes não encontrada no banco. Clique em "Aplicar '
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
            "o ponto desta competência ainda não foi importado — não há diária para fechar.")
    if not calculado["quantos_a_pagar"]:
        raise guardada.ErroDaApropriacao("nenhum colaborador marcado para receber diária.")
    sem_obra = [p["nome"] for p in calculado["pessoas"] if p["pagar"]
                and any(o["obra"] == "(SEM OBRA)" for o in p["por_obra"])]
    if sem_obra:
        raise guardada.ErroDaApropriacao(
            "há diária sem obra (sem marcação no ponto e sem obra no cadastro): "
            + ", ".join(sem_obra[:5]) + ". Sem obra não há conta de pagamento.")
    novo = guardada.fechar(ano, mes, TIPO_DO_FECHAMENTO[calculado["qual"]],
                           apropriado(calculado), verba=VERBA, quem=quem)
    return {"id": novo, "pessoas": calculado["quantos_a_pagar"],
            "total": calculado["total"],
            "tipo": TIPO_DO_FECHAMENTO[calculado["qual"]]}
