# -*- coding: utf-8 -*-
"""
Agenda de compromissos que se repetem — contas, impostos, empréstimos, FGTS.

A MATEMÁTICA DE CALENDÁRIO VEIO INTEIRA do Streamlit, sem uma linha alterada:
o cálculo da Páscoa (que decide Carnaval, Sexta-feira Santa e Corpus Christi),
o ajuste para dia útil, o tratamento de fim de mês e a expansão das
recorrências. É a parte difícil e está certa; reescrever seria só arriscar.

O que mudou foi ONDE os compromissos moram. No Streamlit eles ficavam num
campo de texto do banco local, lidos inteiros a cada uso. Aqui têm tabela
própria (migração 002), continuam vindo da aba "Agenda" da planilha, e a
sincronização os traz para cá.

Duas regras que não são óbvias e vale ter escritas:

  - `dia_mes` igual a 31 quer dizer "o ÚLTIMO dia do mês", não o dia 31. Sem
    isso, um compromisso de fim de mês sumiria em fevereiro.
  - O ajuste para dia útil ANTECIPA em impostos, FGTS e parcelamentos, e
    POSTERGA no resto. Não é preferência: imposto pago depois do vencimento tem
    multa, então quando cai em feriado ele anda para trás.
"""
from __future__ import annotations

import calendar as _cal
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger("analisesps.agenda")

ABA_AGENDA = "Agenda"
ABA_FERIADOS = "Feriados"

COLUNAS = ["id", "titulo", "descricao", "categoria", "data_base", "recorrencia",
           "dia_mes", "ajuste_dia_util", "alerta_dias_antes", "status",
           "concluido_em", "responsavel", "criado_por", "criado_em"]

CATEGORIAS = ["Conta", "Empréstimo", "Imposto", "FGTS", "Parcelamento",
              "Transferência", "Outro"]
RECORRENCIAS = ["nenhuma", "mensal", "anual", "semanal"]
AJUSTES = ["posterga", "antecipa", "nenhum"]

# Categorias cujo padrão é ANTECIPAR quando cai em dia não útil. Ver o topo.
_ANTECIPA_POR_PADRAO = {"Imposto", "FGTS", "Parcelamento"}


def ajuste_sugerido(categoria: str) -> str:
    return "antecipa" if str(categoria).strip() in _ANTECIPA_POR_PADRAO else "posterga"


# ---------------------------------------------------------------------------
# Datas
# ---------------------------------------------------------------------------

def parse_date(s):
    """Aceita date/datetime, 'YYYY-MM-DD' ou 'DD/MM/YYYY'. Retorna date ou None."""
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    txt = str(s or "").strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    return None


def fmt_date(d) -> str:
    d = parse_date(d)
    return d.strftime("%Y-%m-%d") if d else ""


def fmt_br(d) -> str:
    d = parse_date(d)
    return d.strftime("%d/%m/%Y") if d else ""


def _ultimo_dia(ano: int, mes: int) -> int:
    return _cal.monthrange(ano, mes)[1]


# ---------------------------------------------------------------------------
# Feriados (nacionais calculados + extras da planilha)
# ---------------------------------------------------------------------------

def _pascoa(ano: int) -> date:
    """Domingo de Páscoa (algoritmo de Gauss/Anonymous Gregorian)."""
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    L = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * L) // 451
    mes = (h + L - 7 * m + 114) // 31
    dia = ((h + L - 7 * m + 114) % 31) + 1
    return date(ano, mes, dia)


def feriados_nacionais(ano: int) -> set:
    """Feriados nacionais fixos + móveis (Carnaval, Sexta Santa, Corpus Christi)."""
    pascoa = _pascoa(ano)
    fixos = [date(ano, 1, 1), date(ano, 4, 21), date(ano, 5, 1),
             date(ano, 9, 7), date(ano, 10, 12), date(ano, 11, 2),
             date(ano, 11, 15), date(ano, 11, 20), date(ano, 12, 25)]
    moveis = [pascoa - timedelta(days=48),   # Segunda de Carnaval
              pascoa - timedelta(days=47),   # Terça de Carnaval
              pascoa - timedelta(days=2),    # Sexta-feira Santa
              pascoa + timedelta(days=60)]   # Corpus Christi
    return set(fixos + moveis)


def feriados_extra() -> set:
    """Feriados estaduais e municipais, lidos da tabela.

    Falha aqui NUNCA derruba a tela: sem os feriados locais a agenda continua
    certa nos nacionais, e um ajuste de dia útil a menos é muito melhor do que
    a tela não abrir."""
    try:
        from .db import consultar
        return {linha[0] for linha in
                consultar("SELECT dia FROM analisesps.feriados") if linha[0]}
    except Exception:  # noqa: BLE001
        logger.exception("Análise de SPs: não consegui ler os feriados locais")
        return set()


def feriados_do_ano(ano: int) -> set:
    return feriados_nacionais(ano) | {d for d in feriados_extra() if d.year == ano}


def _todos_feriados(anos) -> set:
    out = set()
    for a in anos:
        out |= feriados_do_ano(a)
    return out


def eh_dia_util(d: date, fer: set) -> bool:
    return d.weekday() < 5 and d not in fer


def ajustar_dia_util(d: date, modo: str, fer: set) -> date:
    """Move 'd' p/ um dia útil conforme o modo ('posterga' p/ frente, 'antecipa'
    p/ trás, 'nenhum' = sem mexer)."""
    modo = (modo or "nenhum").strip().lower()
    if modo not in ("posterga", "antecipa"):
        return d
    passo = 1 if modo == "posterga" else -1
    cur = d
    for _ in range(15):                       # limite de segurança
        if eh_dia_util(cur, fer):
            return cur
        cur = cur + timedelta(days=passo)
    return d


# ---------------------------------------------------------------------------
# Ocorrências
# ---------------------------------------------------------------------------

def _data_mensal(ano: int, mes: int, dia_mes: int) -> date:
    """Constrói a data do mês tratando fim de mês: dia_mes>=31 (ou maior que o
    último dia) cai no ÚLTIMO dia do mês."""
    ult = _ultimo_dia(ano, mes)
    dia = ult if (dia_mes >= 31 or dia_mes > ult) else max(1, dia_mes)
    return date(ano, mes, dia)


def _iter_meses(ini: date, fim: date):
    a, m = ini.year, ini.month
    while (a, m) <= (fim.year, fim.month):
        yield a, m
        m += 1
        if m > 12:
            m = 1
            a += 1


def ocorrencias(c: dict, ini: date, fim: date, fer: set) -> list:
    """Datas (já ajustadas a dia útil) em que o compromisso 'c' ocorre em
    [ini, fim]. Não filtra concluídas — quem chama decide."""
    base = parse_date(c.get("data_base"))
    if not base:
        return []
    rec = str(c.get("recorrencia", "nenhuma")).strip().lower()
    modo = str(c.get("ajuste_dia_util", "nenhum")).strip().lower()
    try:
        dia_mes = int(float(c.get("dia_mes") or base.day))
    except (TypeError, ValueError):
        dia_mes = base.day

    brutas = []
    if rec == "nenhuma" or rec == "":
        if ini <= base <= fim:
            brutas.append(base)
    elif rec == "mensal":
        for a, m in _iter_meses(max(ini, base.replace(day=1)), fim):
            brutas.append(_data_mensal(a, m, dia_mes))
    elif rec == "anual":
        for ano in range(max(ini.year, base.year), fim.year + 1):
            try:
                d = date(ano, base.month, base.day)
            except ValueError:                 # 29/02 em ano não bissexto
                d = date(ano, base.month, 28)
            if d >= base:
                brutas.append(d)
    elif rec == "semanal":
        # primeira ocorrência >= ini, mesmo dia da semana de base
        passo = timedelta(days=7)
        d = base
        if d < ini:
            faltam = (ini - d).days
            d = d + timedelta(days=((faltam + 6) // 7) * 7)
        while d <= fim:
            brutas.append(d)
            d += passo

    # ajusta cada ocorrência ao dia útil e mantém só as que caem na janela
    out = []
    for d in brutas:
        aj = ajustar_dia_util(d, modo, fer)
        if ini <= aj <= fim:
            out.append(aj)
    return sorted(set(out))


def proxima_ocorrencia(c: dict, hoje: date, fer: set, horizonte_dias: int = 420):
    fim = hoje + timedelta(days=horizonte_dias)
    occ = ocorrencias(c, hoje, fim, fer)
    return occ[0] if occ else None


def _concluidas(c: dict) -> set:
    return {d for d in (parse_date(x) for x in
                        str(c.get("concluido_em", "")).replace(";", ",").split(","))
            if d}


def lembretes(lista: list, hoje: date) -> list:
    """Compromissos ATIVOS cuja próxima ocorrência (não concluída) está dentro da
    janela de alerta (alerta_dias_antes) ou é hoje. Retorna lista de dicts:
    {compromisso, data, dias} ordenada por data."""
    anos = {hoje.year, hoje.year + 1}
    fer = _todos_feriados(anos)
    out = []
    for c in lista:
        if str(c.get("status", "ativo")).strip().lower() not in ("", "ativo"):
            continue
        try:
            alerta = int(float(c.get("alerta_dias_antes") or 0))
        except (TypeError, ValueError):
            alerta = 0
        feitas = _concluidas(c)
        occ = ocorrencias(c, hoje, hoje + timedelta(days=max(alerta, 0) + 1), fer)
        occ = [d for d in occ if d not in feitas]
        if not occ:
            continue
        d = occ[0]
        dias = (d - hoje).days
        if dias <= alerta:                      # inclui dias==0 (hoje)
            out.append({"compromisso": c, "data": d, "dias": dias})
    out.sort(key=lambda x: (x["data"], x["compromisso"].get("titulo", "")))
    return out


# ---------------------------------------------------------------------------
# Leitura/escrita na planilha (aba 'Agenda' da planilha de Credenciais)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Onde os compromissos moram
#
# A origem continua sendo a aba "Agenda" da planilha. Esta tabela é a cópia
# local, refeita pela sincronização — o mesmo arranjo das SPs.
# ---------------------------------------------------------------------------
def listar() -> list[dict]:
    """Todos os compromissos, como dicionários iguais aos da planilha.

    A lista é curta (dezenas), então vem inteira. Não é o caso das SPs, e a
    diferença é essa: aqui não há o que paginar."""
    from .db import consultar
    campos = ", ".join(f'"{c}"' for c in COLUNAS)
    linhas = consultar(
        f"SELECT {campos} FROM analisesps.agenda ORDER BY titulo")
    return [dict(zip(COLUNAS, linha)) for linha in linhas]


def um(compromisso_id: str) -> dict | None:
    from .db import consultar
    campos = ", ".join(f'"{c}"' for c in COLUNAS)
    linhas = consultar(
        f"SELECT {campos} FROM analisesps.agenda WHERE id = ?",
        (str(compromisso_id),))
    return dict(zip(COLUNAS, linhas[0])) if linhas else None


def gravar(conn, registros: list[dict]) -> int:
    """Grava os compromissos vindos da planilha."""
    from .formatos import para_data
    if not registros:
        return 0
    campos = list(COLUNAS) + ["data_base_d"]
    marcadores = ", ".join(["?"] * len(campos))
    nomes = ", ".join(f'"{c}"' for c in campos)
    atualiza = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in campos if c != "id")
    valores = []
    for r in registros:
        linha = [str(r.get(c, "") or "") for c in COLUNAS]
        linha.append(para_data(r.get("data_base")))
        valores.append(tuple(linha))
    conn.executemany(
        f"INSERT INTO analisesps.agenda ({nomes}) VALUES ({marcadores}) "
        f"ON CONFLICT (id) DO UPDATE SET {atualiza}, atualizado_em = now()",
        valores)
    conn.commit()
    return len(valores)


def gravar_feriados(conn, dias: list) -> int:
    """Substitui a lista de feriados locais.

    Apaga e regrava em vez de acumular: um feriado tirado da planilha tem de
    sumir daqui, senão o ajuste de dia útil continuaria desviando de uma data
    que já não é feriado."""
    conn.execute("DELETE FROM analisesps.feriados")
    if dias:
        conn.executemany(
            "INSERT INTO analisesps.feriados (dia, nome) VALUES (?, ?) "
            "ON CONFLICT (dia) DO NOTHING",
            [(d, nome) for d, nome in dias])
    conn.commit()
    return len(dias)


# ---------------------------------------------------------------------------
# O que a tela mostra
# ---------------------------------------------------------------------------
def proximos(dias_a_frente: int = 90) -> list[dict]:
    """Os compromissos que vencem daqui até `dias_a_frente`, em ordem de data.

    Cada linha traz a data AJUSTADA para dia útil e, ao lado, a original quando
    as duas diferem — assim quem lê entende por que a data mudou, em vez de
    achar que o sistema errou.

    Como `ocorrencias()` devolve só as datas já ajustadas, e a original se perde
    ali dentro, este método refaz os dois passos que ela faz: pede as datas SEM
    ajuste e ajusta cada uma aqui. Nada do calendário foi alterado — as mesmas
    duas funções são usadas, só que separadas, para as duas datas sobreviverem.
    """
    from .horario import agora

    hoje = agora().date()
    fim = hoje + timedelta(days=dias_a_frente)
    lista = listar()
    if not lista:
        return []

    feriados = _todos_feriados(range(hoje.year, fim.year + 2))
    saida = []
    for compromisso in lista:
        if str(compromisso.get("status", "")).strip().lower() == "cancelado":
            continue
        modo = str(compromisso.get("ajuste_dia_util", "nenhum")).strip().lower()

        # Janela alargada para os dois lados: uma data que cai fora por um dia
        # pode entrar depois do ajuste, e vice-versa. Quinze dias é o mesmo
        # limite de segurança que `ajustar_dia_util` usa.
        sem_ajuste = dict(compromisso, ajuste_dia_util="nenhum")
        for bruta in ocorrencias(sem_ajuste, hoje - timedelta(days=15),
                                 fim + timedelta(days=15), feriados):
            ajustada = ajustar_dia_util(bruta, modo, feriados)
            if not (hoje <= ajustada <= fim):
                continue
            saida.append({
                "compromisso": compromisso,
                "data": ajustada,
                "data_original": bruta,
                "dias": (ajustada - hoje).days,
            })

    saida.sort(key=lambda x: (x["data"],
                              str(x["compromisso"].get("titulo", ""))))
    return saida


def a_vencer(dias_de_alerta: int = 7) -> list[dict]:
    """Só o que já entrou na janela de alerta de cada compromisso.

    Cada um tem a sua janela (`alerta_dias_antes`), porque avisar de um imposto
    com um dia de antecedência é inútil e avisar de uma conta de luz com trinta
    é ruído."""
    from .horario import agora
    return lembretes(listar(), agora().date())


# ---------------------------------------------------------------------------
# O CALENDÁRIO DO MÊS
#
# O Streamlit mostrava a agenda numa grade de mês, com ◀ ▶ para navegar e o
# dia clicável abrindo o que cai nele. A conversão deixou só listas, e lista
# não responde "como está a semana que vem" — que é a pergunta que se faz
# olhando um calendário.
# ---------------------------------------------------------------------------
def calendario(ano: int, mes: int) -> dict:
    """A grade do mês, semana a semana, com o que cai em cada dia.

    A grade inclui os dias vizinhos que completam a primeira e a última
    semana — é o que faz o calendário parecer um calendário. Eles vêm
    marcados como `do_mes: False` para a tela desenhar apagados.

    Começa no DOMINGO, como o do Streamlit e como todo calendário de parede
    no Brasil."""
    import calendar as _cal
    from .horario import agora

    hoje = agora().date()
    semanas_datas = _cal.Calendar(firstweekday=6).monthdatescalendar(ano, mes)
    inicio, fim = semanas_datas[0][0], semanas_datas[-1][-1]

    feriados = _todos_feriados({inicio.year, fim.year})
    por_dia: dict = {}
    for c in listar():
        if str(c.get("status") or "ativo").strip().lower() not in ("", "ativo"):
            continue
        for d in ocorrencias(c, inicio, fim, feriados):
            por_dia.setdefault(d, []).append(c)

    semanas = []
    for semana in semanas_datas:
        linha = []
        for d in semana:
            linha.append({
                "data": d,
                "dia": d.day,
                "do_mes": d.month == mes,
                "hoje": d == hoje,
                "feriado": d in feriados,
                "fim_de_semana": d.weekday() >= 5,
                "compromissos": por_dia.get(d, []),
            })
        semanas.append(linha)

    return {"ano": ano, "mes": mes, "semanas": semanas,
            "quantos": sum(len(v) for v in por_dia.values())}


MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro"]


def mes_vizinho(ano: int, mes: int, passo: int) -> tuple:
    """O mês anterior ou o seguinte, virando o ano quando precisa."""
    indice = (ano * 12 + (mes - 1)) + passo
    return indice // 12, indice % 12 + 1
