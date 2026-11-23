# -*- coding: utf-8 -*-
"""
A ANÁLISE DO AGENTE EM CADA PEDIDO — as regras trabalhistas, antes do DP decidir.

⚑ PEDIDO DELE, 20/11/2026: *"O DP (…) precisa fazer uma analisada no que está
pedindo, na forma que se está pedindo, se não tem nada errado (…) as análises
da IA tem que observar também, baseado nas regras trabalhistas, baseado no
contrato de trabalho da pessoa, no contrato de experiência, nas datas. Quando
falamos em rescisão, precisamos verificar se as datas — data de aviso prévio,
último dia de trabalho, opção por aviso indenizado ou saída antecipada (…) a
ideia de futuro é que o DP apenas olhe e dê o ok."*

E os exemplos que ele deu, que viraram regra aqui:
  · *"ela pediu uma demissão, mas é um encerramento de contrato de
    experiência (…) aí não precisa de aviso"*;
  · *"é uma demissão (…) por conta da convenção coletiva, que se você demitir
    a pessoa naquele mês, aí paga em dobro (…) aí segura uns dias"*;
  · *"pedir uma demissão (…) com aviso indenizado, que sai mais caro"*;
  · *"por causa de um dia acaba tendo que fazer a rescisão contratual"*.

⚑ É REGRA ESCRITA, NÃO IA — de propósito. A conta do aviso proporcional, o fim
da experiência e o mês da data-base têm resposta exata; IA aqui erraria em
silêncio, e o DP não tem como conferir. A IA entra onde a regra não alcança
(ler o documento que volta). Custo: zero idas à internet — só o banco.

⚑ O LIMITE: o agente APONTA, nunca decide. Cada item diz a regra e o número;
quem escolhe segurar a demissão, indenizar ou mudar a forma é gente.

Cada item: `{"ok": True|False|None, "texto": "...", "regra": "..."}` —
True = conferido e bate; False = atenção; None = lembrete do que o sistema não
sabe (gestante, CIPA…).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

TETO_AVISO = 90


# ---------------------------------------------------------------------------
# AS CONTAS, puras — o que dá para testar sem banco
# ---------------------------------------------------------------------------
def anos_completos(inicio: date, fim: date) -> int:
    anos = fim.year - inicio.year
    if (fim.month, fim.day) < (inicio.month, inicio.day):
        anos -= 1
    return max(0, anos)


def dias_de_aviso(admissao: Optional[date], data_aviso: date,
                  forma: str) -> int:
    """Lei 12.506/2011: 30 dias + 3 por ano completo, até 90 — quando a
    empresa dispensa. No pedido de demissão, quem deve o aviso é a pessoa, e
    ela deve 30 (o proporcional é só a favor do empregado)."""
    if forma == "PEDIDO_COLABORADOR" or not admissao:
        return 30
    return min(TETO_AVISO, 30 + 3 * anos_completos(admissao, data_aviso))


def datas_do_aviso(admissao: Optional[date], data_aviso: date, forma: str,
                   cumpre: bool = True) -> dict[str, Any]:
    """O que se espera das datas, pela forma. Súmula 380 do TST: o dia do
    aviso não conta, conta o do vencimento.

    - Trabalhado: trabalha 30 dias; o que passar de 30 (o proporcional) é
      indenizado e projeta a data de saída.
    - Indenizado: o último dia trabalhado é o do aviso; a data de saída é a
      PROJEÇÃO do aviso (OJ 82 da SDI-1).
    - Pedido de demissão: 30 dias, trabalhados se a pessoa cumprir.
    """
    dias = dias_de_aviso(admissao, data_aviso, forma)
    if forma == "AVISO_INDENIZADO":
        ultimo = data_aviso
    elif forma == "PEDIDO_COLABORADOR" and not cumpre:
        ultimo = data_aviso
    else:
        ultimo = data_aviso + timedelta(days=30)
    saida = data_aviso + timedelta(days=dias)
    if forma == "PEDIDO_COLABORADOR":
        saida = ultimo
    return {"dias": dias, "ultimo_dia": ultimo, "data_saida": saida,
            "indenizados": max(0, dias - 30) if forma == "AVISO_TRABALHADO"
            else (dias if forma == "AVISO_INDENIZADO" else 0)}


def risco_da_data_base(mes_data_base: Optional[int],
                       saida_projetada: date) -> Optional[dict[str, Any]]:
    """Lei 7.238/84, art. 9º: dispensa sem justa causa nos 30 dias ANTES da
    data-base dá à pessoa uma indenização adicional de um salário. O aviso
    indenizado conta (Súmula 182). Se a projeção passa da data-base, não há a
    indenização, mas o reajuste entra na rescisão (Súmula 314)."""
    if not mes_data_base:
        return None
    for ano in (saida_projetada.year, saida_projetada.year + 1):
        base = date(ano, int(mes_data_base), 1)
        if base - timedelta(days=30) <= saida_projetada < base:
            return {"data_base": base,
                    "segurar_ate": base,
                    "antes_de": base - timedelta(days=30)}
    return None


def periodos_de_ferias(admissao: date, hoje: date) -> list[dict[str, date]]:
    """Os períodos aquisitivos desde a admissão, com o fim do concessivo."""
    saida = []
    k = 0
    while True:
        ini = _mais_anos(admissao, k)
        if ini > hoje:
            break
        fim = _mais_anos(admissao, k + 1) - timedelta(days=1)
        saida.append({"inicio": ini, "fim": fim,
                      "limite": _mais_anos(admissao, k + 2) - timedelta(days=1)})
        k += 1
    return saida


def _mais_anos(d: date, n: int) -> date:
    try:
        return d.replace(year=d.year + n)
    except ValueError:                       # 29/02
        return d.replace(year=d.year + n, day=28)


def _data(v: Any) -> Optional[date]:
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v or "")[:10])
    except ValueError:
        return None


def _br(d: Optional[date]) -> str:
    return d.strftime("%d/%m/%Y") if d else "?"


def _dinheiro(v: Any) -> Optional[float]:
    if v in (None, ""):
        return None
    t = str(v).strip().replace("R$", "").strip()
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def ok(texto_: str, regra: str = "") -> dict[str, Any]:
    return {"ok": True, "texto": texto_, "regra": regra}


def atencao(texto_: str, regra: str = "") -> dict[str, Any]:
    return {"ok": False, "texto": texto_, "regra": regra}


def lembrete(texto_: str, regra: str = "") -> dict[str, Any]:
    return {"ok": None, "texto": texto_, "regra": regra}


# ---------------------------------------------------------------------------
# O QUE SE LÊ DO BANCO
# ---------------------------------------------------------------------------
def _pessoa(s: Session, colaborador_id: Optional[int]) -> Optional[dict[str, Any]]:
    if not colaborador_id:
        return None
    linha = s.execute(text("""
        SELECT c.id, c.nome, c.vinculo, c.admissao, c.salario, c.contrato_experiencia,
               c.data_nascimento, c.etapa, c.situacao, c.convencao_id,
               cv.mes_data_base, cv.nome AS convencao
          FROM colaboradores c
          LEFT JOIN convencoes cv ON cv.id = c.convencao_id
         WHERE c.id = :i"""), {"i": colaborador_id}).mappings().first()
    return dict(linha) if linha else None


def _estabilidade(s: Session, colaborador_id: int, quando: date) -> Optional[dict]:
    linha = s.execute(text("""
        SELECT estabilidade_ate, estabilidade_motivo FROM colaborador_afastamentos
         WHERE colaborador_id = :c AND estabilidade_ate IS NOT NULL
           AND estabilidade_ate >= :d
         ORDER BY estabilidade_ate DESC LIMIT 1"""),
        {"c": colaborador_id, "d": quando}).mappings().first()
    return dict(linha) if linha else None


def _afastado(s: Session, colaborador_id: int) -> Optional[dict]:
    linha = s.execute(text("""
        SELECT tipo, data_inicio FROM colaborador_afastamentos
         WHERE colaborador_id = :c AND data_retorno IS NULL
         ORDER BY data_inicio DESC LIMIT 1"""), {"c": colaborador_id}).mappings().first()
    return dict(linha) if linha else None


# ---------------------------------------------------------------------------
# POR TIPO DE PEDIDO
# ---------------------------------------------------------------------------
def analisar(s: Session, sol: dict[str, Any],
             hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """A análise do pedido como ele está agora (com o que o DP já corrigiu).
    Nunca levanta: análise que quebra não pode travar o atendimento."""
    hoje = hoje or date.today()
    try:
        tipo = sol.get("tipo")
        dados = sol.get("dados") or {}
        p = _pessoa(s, sol.get("colaborador_id"))
        fn = {"DESLIGAMENTO": _desligamento, "FERIAS": _ferias,
              "ALTERACAO_FERIAS": _ferias, "ALTERACAO_CARGO": _alteracao,
              "CADASTRO": _admissao, "ADMISSAO_PRESTADOR": _admissao,
              "DESCONTO_EM_FOLHA": _desconto,
              "CANCELAR_DESLIGAMENTO": _cancelar_desligamento,
              }.get(tipo)
        return fn(s, sol, dados, p, hoje) if fn else []
    except Exception:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).exception("ERP/DP: a análise do pedido falhou")
        return [lembrete("a análise automática falhou neste pedido — confira à mão")]


def _desligamento(s, sol, dados, p, hoje) -> list[dict[str, Any]]:
    from app.apps.erp.core import pessoal_experiencia as svc_exp
    if p is None or (p["vinculo"] or "CTPS").upper() != "CTPS":
        return []
    forma = str(dados.get("forma_desligamento") or "").upper()
    itens: list[dict[str, Any]] = []
    exp = svc_exp.situacao(p["admissao"], p["contrato_experiencia"], hoje)
    aviso = _data(dados.get("aviso_previo"))

    # 1. O CONTRATO DE EXPERIÊNCIA — o engano mais caro do formulário.
    if exp["tem"] and not exp["ja_acabou"]:
        fim = _data(exp["fim_primeira"]) if exp["fase"] == "primeira" else _data(exp["fim_total"])
        if forma == "FIM_EXPERIENCIA":
            saida = _data(dados.get("data_saida"))
            alvos = {_data(exp["fim_primeira"]), _data(exp["fim_total"])}
            if saida and saida not in alvos:
                itens.append(atencao(
                    f"a saída {_br(saida)} não é o fim do contrato de experiência "
                    f"({_br(_data(exp['fim_primeira']))}"
                    + (f" ou {_br(_data(exp['fim_total']))}" if exp["prorrogavel"] else "")
                    + "). Antes do fim é rescisão ANTECIPADA: a empresa paga "
                      "metade dos dias que faltam (art. 479). Depois do fim o "
                      "contrato virou indeterminado: aí é demissão com aviso.",
                    "CLT art. 479 e 451"))
            else:
                itens.append(ok(f"encerramento da experiência no prazo ({_br(fim)}): "
                                "sem aviso prévio e sem multa de 40%", "CLT art. 443 e 481"))
        elif forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO"):
            itens.append(atencao(
                f"{p['nome']} está em contrato de experiência até {_br(fim)} "
                f"(faltam {exp['dias_para_o_fim']} dia(s)). Encerrar no fim do "
                "prazo NÃO tem aviso prévio nem multa de 40%; romper antes custa "
                "metade dos dias restantes (art. 479). Confira se a forma certa "
                "não é o encerramento da experiência.", "CLT art. 479"))
        elif forma == "PEDIDO_COLABORADOR":
            itens.append(lembrete(
                f"pedido de demissão durante a experiência (até {_br(fim)}): não "
                "há aviso prévio, salvo cláusula de rescisão antecipada no "
                "contrato; a pessoa pode ter de indenizar o prejuízo (art. 480)",
                "CLT art. 480 e 481"))
    elif exp["tem"] and exp["ja_acabou"] and forma == "FIM_EXPERIENCIA":
        itens.append(atencao(
            f"o contrato de experiência JÁ VENCEU em {_br(_data(exp['fim_total']))} "
            "e virou prazo indeterminado — o encerramento de experiência não cabe "
            "mais; a saída é com aviso prévio", "CLT art. 451"))

    # 2. AS DATAS DO AVISO.
    if forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO", "PEDIDO_COLABORADOR") and aviso:
        cumpre = str(dados.get("cumpre_aviso") or "1") not in ("0", "nao", "não", "NAO")
        esp = datas_do_aviso(p["admissao"], aviso, forma, cumpre)
        if forma != "PEDIDO_COLABORADOR":
            anos = anos_completos(p["admissao"], aviso) if p["admissao"] else 0
            itens.append(ok(
                f"aviso de {esp['dias']} dias: 30 + 3 por ano completo ({anos} "
                f"ano(s) de casa)" + (f", dos quais {esp['indenizados']} "
                                      "indenizados" if esp["indenizados"] else ""),
                "Lei 12.506/2011"))
        ultimo = _data(dados.get("ultimo_dia"))
        saida = _data(dados.get("data_saida"))
        if ultimo and ultimo != esp["ultimo_dia"]:
            itens.append(atencao(
                f"último dia trabalhado informado {_br(ultimo)}; pela forma e "
                f"pela data do aviso ({_br(aviso)}) o esperado é "
                f"{_br(esp['ultimo_dia'])}", "Súmula 380 do TST"))
        elif not ultimo:
            itens.append(lembrete(f"último dia trabalhado esperado: "
                                  f"{_br(esp['ultimo_dia'])}", "Súmula 380 do TST"))
        if saida and saida != esp["data_saida"]:
            itens.append(atencao(
                f"data de saída informada {_br(saida)}; com a projeção do aviso o "
                f"esperado é {_br(esp['data_saida'])}",
                "OJ 82 da SDI-1 do TST"))
        elif not saida:
            itens.append(lembrete(f"data de saída esperada (com a projeção do "
                                  f"aviso): {_br(esp['data_saida'])}",
                                  "OJ 82 da SDI-1 do TST"))
        if aviso < hoje - timedelta(days=2):
            itens.append(atencao(f"a data do aviso ({_br(aviso)}) já passou — o "
                                 "aviso vale do dia em que a pessoa assina",
                                 "CLT art. 487"))
        if forma == "AVISO_INDENIZADO":
            itens.append(lembrete(
                f"aviso indenizado: os {esp['dias']} dias são pagos sem trabalho "
                "— é a forma mais cara. A rescisão vence em 10 dias do aviso.",
                "CLT art. 477 §6º"))
        if forma == "AVISO_TRABALHADO":
            itens.append(lembrete("no aviso trabalhado a pessoa escolhe: 2 horas "
                                  "a menos por dia ou 7 dias corridos de folga "
                                  "no fim", "CLT art. 488"))
        # 3. A DATA-BASE DA CONVENÇÃO.
        if forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO"):
            risco = risco_da_data_base(p["mes_data_base"], aviso + timedelta(days=esp["dias"]))
            if risco:
                itens.append(atencao(
                    f"a projeção do aviso termina nos 30 dias antes da data-base "
                    f"da convenção ({_br(risco['data_base'])}): a pessoa ganha "
                    "uma indenização adicional de um salário. Dar o aviso alguns "
                    "dias depois (projeção a partir da data-base) evita isso.",
                    "Lei 7.238/84, art. 9º; Súmulas 182 e 314 do TST"))
            elif p["mes_data_base"]:
                itens.append(ok("fora dos 30 dias antes da data-base da convenção",
                                "Lei 7.238/84, art. 9º"))
            else:
                itens.append(lembrete("a convenção da pessoa está sem o mês da "
                                      "data-base — o sistema não consegue conferir "
                                      "a indenização adicional"))
    elif forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO", "PEDIDO_COLABORADOR"):
        itens.append(atencao("falta a data do aviso prévio", "CLT art. 487"))

    # 4. ESTABILIDADE E AFASTAMENTO.
    afast = _afastado(s, p["id"])
    if afast:
        itens.append(atencao(
            f"{p['nome']} está AFASTADO ({afast['tipo']}) desde "
            f"{_br(afast['data_inicio'])}: com o contrato suspenso não se demite",
            "CLT art. 471 e 476"))
    est = _estabilidade(s, p["id"], hoje)
    if est and forma != "PEDIDO_COLABORADOR":
        itens.append(atencao(
            f"estabilidade até {_br(est['estabilidade_ate'])} "
            f"({est['estabilidade_motivo'] or 'retorno de afastamento'}): demitir "
            "sem justa causa antes disso gera reintegração ou indenização",
            "Lei 8.213/91, art. 118"))
    itens.append(lembrete("o sistema não sabe de gravidez, CIPA nem da "
                          "estabilidade pré-aposentadoria da convenção — confirme "
                          "antes de liberar"))
    return itens


def _ferias(s, sol, dados, p, hoje) -> list[dict[str, Any]]:
    if p is None:
        return []
    itens = []
    inicio = _data(dados.get("ferias_inicio"))
    try:
        dias = int(str(dados.get("ferias_dias") or "0"))
    except ValueError:
        dias = 0
    if not inicio or not dias:
        return [atencao("faltam o início e os dias das férias")]
    adm = p["admissao"]
    if adm and _mais_anos(adm, 1) > inicio:
        itens.append(atencao(f"o primeiro período aquisitivo só fecha em "
                             f"{_br(_mais_anos(adm, 1) - timedelta(days=1))} — "
                             "antes disso não há férias individuais", "CLT art. 130"))
    if inicio.weekday() in (4, 5):
        itens.append(atencao("as férias não podem começar nos dois dias antes do "
                             "descanso semanal (sexta ou sábado, com folga no "
                             "domingo) nem antes de feriado", "CLT art. 134 §3º"))
    if dias < 5:
        itens.append(atencao("nenhum período de férias pode ter menos de 5 dias",
                             "CLT art. 134 §1º"))
    elif dias < 14:
        itens.append(lembrete("férias divididas: um dos períodos do ano tem de "
                              "ter pelo menos 14 dias", "CLT art. 134 §1º"))
    antecedencia = (inicio - hoje).days
    if antecedencia < 30:
        itens.append(atencao(f"faltam {antecedencia} dia(s) para o início: o aviso "
                             "de férias é com 30 dias, e o pagamento até 2 dias "
                             "antes", "CLT art. 135 e 145"))
    else:
        itens.append(ok(f"{antecedencia} dias de antecedência (o aviso pede 30)",
                        "CLT art. 135"))
    if adm:
        from app.apps.erp.core import ferias as svc_ferias
        aberto = svc_ferias.periodo_em_aberto(s, p["id"], adm, hoje)
        if aberto:
            fim = inicio + timedelta(days=dias - 1)
            if fim > aberto["limite"]:
                itens.append(atencao(
                    f"o período {_br(aberto['inicio'])}–{_br(aberto['fim'])} tem de "
                    f"ser gozado até {_br(aberto['limite'])}; estas férias passam "
                    "disso — o que passar é pago em dobro", "CLT art. 137"))
            else:
                itens.append(ok(f"dentro do prazo do período "
                                f"{_br(aberto['inicio'])}–{_br(aberto['fim'])} "
                                f"(gozar até {_br(aberto['limite'])})", "CLT art. 134"))
    if str(dados.get("abono_pecuniario")) == "1":
        itens.append(lembrete("o abono (vender 1/3) tem de ser pedido até 15 dias "
                              "antes do fim do período aquisitivo; depois disso "
                              "depende da empresa aceitar", "CLT art. 143 §1º"))
    return itens


def _alteracao(s, sol, dados, p, hoje) -> list[dict[str, Any]]:
    if p is None:
        return []
    itens = []
    novo = _dinheiro(dados.get("salario"))
    atual = _dinheiro(p["salario"])
    if novo is not None and atual is not None and novo < atual:
        itens.append(atencao(f"o salário novo (R$ {novo:,.2f}) é MENOR que o atual "
                             f"(R$ {atual:,.2f}): reduzir salário é proibido, "
                             "salvo acordo ou convenção coletiva",
                             "CF art. 7º VI; CLT art. 468"))
    vig = _data(dados.get("vigencia") or dados.get("data_inicio"))
    if vig and vig < hoje - timedelta(days=5):
        itens.append(atencao(f"vigência retroativa ({_br(vig)}): a contabilidade "
                             "terá de refazer folha já fechada"))
    if (p["vinculo"] or "").upper() == "CTPS":
        from app.apps.erp.core.agente_dp import _abaixo_do_piso
        from app.apps.erp.db.models.cadastros import Colaborador
        c = s.get(Colaborador, p["id"])
        if novo is not None and c is not None:
            abaixo = _abaixo_do_piso(s, c, dados.get("salario"))
            itens.append(atencao(abaixo, "convenção coletiva") if abaixo
                         else ok("o salário novo respeita o piso da convenção"))
    return itens


def _admissao(s, sol, dados, p, hoje) -> list[dict[str, Any]]:
    itens = []
    vinc = str(dados.get("tipo_documentacao") or (p or {}).get("vinculo") or "").upper()
    if sol.get("tipo") == "ADMISSAO_PRESTADOR":
        vinc = "CTPS"
    if vinc != "CTPS":
        return []
    ini = _data(dados.get("data_inicio") or dados.get("admissao"))
    if ini and ini <= hoje:
        itens.append(atencao(f"o início ({_br(ini)}) é hoje ou já passou: o "
                             "registro no eSocial tem de sair ANTES do primeiro dia",
                             "eSocial, S-2200"))
    elif ini:
        itens.append(ok(f"início em {_br(ini)}: o registro tem de sair até a véspera",
                        "eSocial, S-2200"))
    nasc = _data(dados.get("data_nascimento") or (p or {}).get("data_nascimento"))
    if nasc:
        idade = anos_completos(nasc, ini or hoje)
        if idade < 16:
            itens.append(atencao(f"{idade} anos: abaixo de 16 só como aprendiz "
                                 "(a partir de 14)", "CF art. 7º XXXIII"))
        elif idade < 18:
            itens.append(atencao(f"{idade} anos: menor de 18 não pode trabalho "
                                 "insalubre, perigoso nem noturno — na obra, "
                                 "confira a função", "CLT art. 405; CF art. 7º XXXIII"))
    # ⚑ READMISSÃO EM ATÉ 1 ANO: SEM EXPERIÊNCIA (regra dele, 20/11/2026; a
    # lei, CLT art. 452, fala em 6 meses — a empresa usa 1 ano).
    if p and p.get("id"):
        from app.apps.erp.core.cadastros import pessoas as svc_p
        saida = svc_p.readmissao_sem_experiencia(s, p["id"], ini or hoje)
        if saida:
            itens.append(lembrete(
                f"readmissão: saiu em {_br(saida)}, volta em menos de 1 ano — o "
                "contrato entra por prazo INDETERMINADO, sem experiência (o "
                "sistema grava assim sozinho)", "CLT art. 452; regra da empresa"))
            return itens
    # ⚑ O ASO ADMISSIONAL (21/11/2026): opcional por enquanto, decisão dele;
    # o agente lembra o que a norma pede.
    from app.apps.erp.core import pessoal_solicitacoes as svc_sp
    papeis = svc_sp.papeis_do_pedido(s, sol["id"], sol.get("numero") or "",
                                     sol.get("colaborador_id")) if sol.get("id") else []
    if any("saúde ocupacional" in str(x.get("tipo") or "").lower()
           or str(x.get("tipo") or "").upper() == "ASO" for x in papeis):
        itens.append(ok("o ASO admissional veio com os documentos", "NR-7"))
    else:
        # ⚑ 21/11/2026, ele: *"vamos alertar, só não vai ser obrigatório por
        # enquanto."* Atenção (⚠️), não trava.
        itens.append(atencao("o ASO admissional não veio — não trava (por enquanto), "
                             "mas a norma pede o exame antes do primeiro dia", "NR-7"))
    if not str(dados.get("contrato_experiencia") or (p or {}).get("contrato_experiencia") or "").strip():
        itens.append(lembrete("sem contrato de experiência escolhido: a pessoa "
                              "entra direto por prazo indeterminado",
                              "CLT art. 443 e 445"))
    return itens


def _desconto(s, sol, dados, p, hoje) -> list[dict[str, Any]]:
    if p is None or (p["vinculo"] or "").upper() != "CTPS":
        return []
    itens = [lembrete("desconto em folha de quem tem carteira precisa de "
                      "autorização por escrito da pessoa (fora adiantamento, "
                      "lei, convenção ou dano com dolo)", "CLT art. 462")]
    total = _dinheiro(dados.get("valor_total") or dados.get("valor"))
    try:
        parcelas = int(str(dados.get("parcelas") or dados.get("numero_parcelas") or "1"))
    except ValueError:
        parcelas = 1
    salario = _dinheiro(p["salario"])
    if total and salario and parcelas:
        parcela = total / max(1, parcelas)
        if parcela > 0.3 * salario:
            itens.append(atencao(f"a parcela (R$ {parcela:,.2f}) passa de 30% do "
                                 f"salário (R$ {salario:,.2f}) — confira se a "
                                 "pessoa consegue arcar e o que a convenção diz"))
    return itens


def _cancelar_desligamento(s, sol, dados, p, hoje) -> list[dict[str, Any]]:
    if p is None:
        return []
    saida = s.execute(text("SELECT demissao FROM colaboradores WHERE id = :i"),
                      {"i": p["id"]}).scalar()
    if saida and saida <= hoje + timedelta(days=1):
        return [atencao(f"a saída está marcada para {_br(saida)}: atenda HOJE — o "
                        "aplicativo do ponto corta o acesso na data")]
    if saida:
        return [ok(f"a saída marcada ({_br(saida)}) ainda não chegou")]
    return [lembrete("não há saída marcada no cadastro")]


def resumo(itens: list[dict[str, Any]]) -> dict[str, int]:
    return {"atencao": sum(1 for i in itens if i["ok"] is False),
            "ok": sum(1 for i in itens if i["ok"] is True),
            "lembretes": sum(1 for i in itens if i["ok"] is None)}


# ---------------------------------------------------------------------------
# O QUE O DP PREENCHE AO ATENDER O DESLIGAMENTO — já sugerido
#
# ⚑ PEDIDO DELE, 23/11/2026, ao atender um pedido de demissão: *"As faltas a
# descontar, eu gostaria que já fosse coletada do ponto a título de sugestão.
# Do dia 26 do mês anterior em diante."* E sobre as datas: *"quando é a empresa
# que está demitindo (…) se você souber, você já calcula a partir da data do
# aviso (…) como tem a data de admissão no cadastro, talvez já dê para
# calcular."*
#
# É SUGESTÃO: a janela de atendimento chega preenchida e o DP muda o que
# quiser. A análise acima continua apontando se o que ficou gravado diverge.
#
# Custo: duas consultas no banco do ERP e UM espelho do ponto (o mesmo que a
# tela do ponto monta para uma pessoa) — só quando o DP aperta "Atender".
# ---------------------------------------------------------------------------
def inicio_das_faltas(ultimo: date, corte: int) -> date:
    """PURA. O primeiro dia da folha em que o último dia cai: o dia seguinte ao
    corte do ponto. Com corte 25, último dia 10/12 → 26/11; 28/12 → 26/12."""
    if ultimo.day > corte:
        return ultimo.replace(day=corte + 1)
    mes_anterior = ultimo.replace(day=1) - timedelta(days=1)
    return mes_anterior.replace(day=corte + 1)


def faltas_no_ponto(colaborador_id: int, ultimo: date,
                    hoje: Optional[date] = None) -> dict[str, Any]:
    """As faltas da pessoa no ponto, do dia seguinte ao corte até o último dia
    (ou até ontem, se o último dia ainda não chegou). Usa o espelho do próprio
    ponto — a mesma conta do fechamento —, nunca a tabela por fora.

    Nunca levanta: sem ponto, devolve `disponivel: False` e o motivo."""
    hoje = hoje or date.today()
    try:
        from app.apps.ponto import db as ponto_db
        from app.apps.ponto.core import espelho, fechamento
    except Exception as e:  # noqa: BLE001
        return {"disponivel": False, "motivo": f"o módulo do ponto não carregou: {e}"}
    try:
        with ponto_db.conexao() as conn:
            corte = fechamento.dia_de_corte(conn)
            inicio = inicio_das_faltas(ultimo, corte)
            fim = min(ultimo, hoje - timedelta(days=1))
            base = {"disponivel": True, "inicio": inicio.isoformat(),
                    "fim": fim.isoformat(), "corte": corte,
                    "parcial_ate_hoje": fim < ultimo}
            if fim < inicio:
                return {**base, "integrais": 0, "dias": [], "parciais": []}
            esp = espelho.montar(conn, int(colaborador_id), inicio, fim)
            integrais, parciais = [], []
            for d in esp.get("dias") or []:
                f = fechamento.falta_do_dia(d)
                if not f:
                    continue
                if f["tipo"] == "INTEGRAL":
                    integrais.append(d["data"])
                else:
                    parciais.append({"data": d["data"], "minutos": f["minutos"],
                                     "rotulo": f["rotulo"]})
            return {**base, "integrais": len(integrais), "dias": integrais,
                    "parciais": parciais}
    except Exception as e:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).exception("ERP/DP: faltas do ponto para o desligamento")
        return {"disponivel": False, "motivo": f"o ponto não respondeu ({e})"}


def sugestoes_do_desligamento(s: Session, sol: dict[str, Any],
                              hoje: Optional[date] = None) -> dict[str, Any]:
    """Os valores sugeridos para a janela de atendimento do desligamento de
    quem tem carteira: último dia, data de saída e faltas a descontar — cada
    um com a explicação da conta. Nunca levanta."""
    hoje = hoje or date.today()
    saida: dict[str, Any] = {"valores": {}, "explicacoes": {}}
    try:
        from app.apps.erp.core import pessoal_experiencia as svc_exp
        dados = sol.get("dados") or {}
        p = _pessoa(s, sol.get("colaborador_id"))
        if p is None or (p["vinculo"] or "CTPS").upper() != "CTPS":
            return saida
        forma = str(dados.get("forma_desligamento") or "").upper()
        aviso = _data(dados.get("aviso_previo"))
        cumpre = str(dados.get("cumpre_aviso") or "1") not in ("0", "nao", "não", "NAO")
        exp = svc_exp.situacao(p["admissao"], p["contrato_experiencia"], aviso or hoje)
        ultimo = data_saida = None
        texto = ""
        if forma == "FIM_EXPERIENCIA" and exp["tem"]:
            fim = _data(exp["fim_primeira"]) if exp["fase"] == "primeira" \
                else _data(exp["fim_total"])
            ultimo = data_saida = fim
            texto = (f"fim do contrato de experiência ({_br(fim)}): sem aviso "
                     "prévio, o último dia e a saída são o mesmo dia")
        elif forma == "PEDIDO_COLABORADOR" and aviso and exp["tem"] \
                and not exp["ja_acabou"]:
            ultimo = data_saida = aviso
            texto = ("pedido de demissão durante a experiência: não há aviso "
                     "prévio (salvo cláusula no contrato) — sai no dia do pedido")
        elif forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO",
                       "PEDIDO_COLABORADOR") and aviso:
            esp = datas_do_aviso(p["admissao"], aviso, forma, cumpre)
            ultimo, data_saida = esp["ultimo_dia"], esp["data_saida"]
            if forma == "PEDIDO_COLABORADOR":
                texto = (f"pedido de demissão com aviso em {_br(aviso)}: "
                         + ("trabalha os 30 dias — último dia e saída em "
                            f"{_br(ultimo)} (se for dispensada do aviso, "
                            f"os dois passam a ser {_br(aviso)})" if cumpre else
                            "dispensado do aviso — sai no dia do pedido")
                         + ". O aviso de quem pede é sempre 30 dias (o "
                           "proporcional só vale a favor da pessoa). Se ela "
                           "combinou outro último dia, troque aqui.")
            else:
                anos = anos_completos(p["admissao"], aviso) if p["admissao"] else 0
                texto = (f"aviso de {esp['dias']} dias (30 + 3 por ano completo; "
                         f"{anos} ano(s) de casa"
                         + ("" if p["admissao"] else " — SEM admissão no "
                            "cadastro, contei só 30") + "). "
                         + (f"Trabalha 30 dias até {_br(ultimo)}; "
                            if forma == "AVISO_TRABALHADO" else
                            f"Indenizado: o último dia é o do aviso ({_br(ultimo)}); ")
                         + f"a saída é a projeção do aviso, {_br(data_saida)}.")
        if ultimo:
            saida["valores"]["ultimo_dia"] = ultimo.isoformat()
            saida["explicacoes"]["ultimo_dia"] = texto
        if data_saida:
            saida["valores"]["data_saida"] = data_saida.isoformat()
            saida["explicacoes"]["data_saida"] = (
                "na carteira, a data de saída é a projeção do aviso (OJ 82 da "
                "SDI-1 do TST)" if forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO")
                else "sem aviso a projetar, a saída é o último dia")

        if forma == "PEDIDO_COLABORADOR":
            saida["explicacoes"]["reducao_jornada"] = (
                "a redução de jornada (2 horas por dia ou 7 dias) é direito de "
                "quem foi DISPENSADO (CLT art. 488) — no pedido de demissão, "
                "deixe em branco")
        elif forma in ("AVISO_TRABALHADO", "AVISO_INDENIZADO", "FIM_EXPERIENCIA"):
            saida["explicacoes"]["cumpre_aviso"] = (
                "só vale no pedido de demissão — aqui, deixe em branco")
            if forma != "AVISO_TRABALHADO":
                saida["explicacoes"]["reducao_jornada"] = (
                    "só vale no aviso trabalhado — aqui, deixe em branco")

        # As faltas: do dia seguinte ao corte do ponto até o último dia.
        if ultimo and sol.get("colaborador_id"):
            f = faltas_no_ponto(int(sol["colaborador_id"]), ultimo, hoje)
            if f.get("disponivel"):
                saida["valores"]["faltas_a_descontar"] = str(f["integrais"])
                partes = [f"pelo ponto, de {_br(_data(f['inicio']))} a "
                          f"{_br(_data(f['fim']))}: {f['integrais']} falta(s) de "
                          "dia inteiro sem justificativa"]
                if f["dias"]:
                    partes.append("(" + ", ".join(_br(_data(x))[:5] for x in f["dias"]) + ")")
                if f["parciais"]:
                    minutos = sum(x["minutos"] for x in f["parciais"])
                    partes.append(f"; mais {len(f['parciais'])} dia(s) com horas a "
                                  f"menos ({minutos // 60}h{minutos % 60:02d} no "
                                  "total), que não entram neste número")
                if f["parcial_ate_hoje"]:
                    partes.append(". O último dia ainda não chegou: o número é até "
                                  "ontem — confira de novo perto da saída")
                saida["explicacoes"]["faltas_a_descontar"] = " ".join(partes).replace(" ;", ";").replace(" .", ".")
            else:
                saida["explicacoes"]["faltas_a_descontar"] = (
                    f"não deu para ler o ponto: {f.get('motivo')}")
        saida["forma"] = forma
        return saida
    except Exception:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).exception("ERP/DP: sugestões do desligamento")
        return saida
