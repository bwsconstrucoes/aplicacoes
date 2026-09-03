# -*- coding: utf-8 -*-
"""
agenda.py — compromissos do financeiro (base COMPARTILHADA na aba 'Agenda' da
planilha de Credenciais). Lógica de recorrência, feriados e ajuste de dia útil,
mais leitura/escrita na planilha (via a mesma Service Account).

Regras de dia útil (campo ajuste_dia_util):
  - 'posterga' : se cair em fim de semana/feriado, joga para o PRÓXIMO dia útil
                 (contas em geral).
  - 'antecipa' : joga para o dia útil ANTERIOR (FGTS, impostos, parcelamentos).
  - 'nenhum'   : usa a data como está.

Recorrência: 'nenhuma' / 'mensal' / 'anual' / 'semanal'.
  - mensal usa 'dia_mes' (1..31; 31 = sempre o ÚLTIMO dia do mês).
"""
from __future__ import annotations

import json
import time
import calendar as _cal
from datetime import date, datetime, timedelta

import cache
import config

ABA_AGENDA = "Agenda"
ABA_FERIADOS = "Feriados"           # opcional; feriados extras (estaduais/municipais)
_META = "agenda_cache"
_META_FER = "agenda_feriados_extra"

COLUNAS = ["id", "titulo", "descricao", "categoria", "data_base", "recorrencia",
           "dia_mes", "ajuste_dia_util", "alerta_dias_antes", "status",
           "concluido_em", "responsavel", "criado_por", "criado_em"]

CATEGORIAS = ["Conta", "Empréstimo", "Imposto", "FGTS", "Parcelamento",
              "Transferência", "Outro"]
RECORRENCIAS = ["nenhuma", "mensal", "anual", "semanal"]
AJUSTES = ["posterga", "antecipa", "nenhum"]

# Categorias cujo padrão é ANTECIPAR quando cai em dia não útil.
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
    """Feriados extras (estaduais/municipais) cadastrados na aba 'Feriados'.
    Lidos do cache local; atualizados por sincronizar()."""
    try:
        raw = cache.get_meta(_META_FER, "") or "[]"
        return {d for d in (parse_date(x) for x in json.loads(raw)) if d}
    except Exception:
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

def _ws():
    import gsheets
    return gsheets._abrir_aba(ABA_AGENDA, planilha_id=config.PLANILHA_CONFIG)


def _ws_feriados():
    import gsheets
    return gsheets._abrir_aba(ABA_FERIADOS, planilha_id=config.PLANILHA_CONFIG)


def _com_retry(fn):
    import gsheets
    return gsheets._com_retry(fn)


def _api(fn, tentativas: int = 5):
    """Retry consciente de cota: em 429 (Quota exceeded) espera mais (a cota do
    Sheets é POR MINUTO), então backoff longo costuma resolver. Em outros erros,
    backoff curto."""
    erro = None
    for i in range(tentativas):
        try:
            return fn()
        except Exception as e:
            erro = e
            msg = str(e)
            quota = ("429" in msg or "Quota exceeded" in msg
                     or "RESOURCE_EXHAUSTED" in msg or "rateLimitExceeded" in msg)
            if i >= tentativas - 1:
                break
            time.sleep(min(30, 10 * (i + 1)) if quota else (1 + i))
    raise erro


def sincronizar() -> list:
    """Lê a aba Agenda (e Feriados, se existir) e guarda no cache local. Online."""
    ws = _ws()
    valores = _com_retry(lambda: ws.get_all_values())
    linhas = []
    if valores:
        cab = [str(x).strip() for x in valores[0]]
        idx = {c: cab.index(c) for c in COLUNAS if c in cab}
        for r in valores[1:]:
            if not any(str(x).strip() for x in r):
                continue
            d = {c: (str(r[idx[c]]).strip() if c in idx and idx[c] < len(r) else "")
                 for c in COLUNAS}
            if d.get("id"):
                linhas.append(d)
    cache.set_meta(_META, json.dumps(linhas, ensure_ascii=False))

    # Feriados extras (aba opcional)
    extras = []
    try:
        fv = _com_retry(lambda: _ws_feriados().get_all_values())
        for r in (fv[1:] if fv else []):
            d = parse_date(r[0]) if r else None
            if d:
                extras.append(d.strftime("%Y-%m-%d"))
    except Exception:
        pass
    cache.set_meta(_META_FER, json.dumps(extras, ensure_ascii=False))
    return linhas


_BOOT_TS = 0.0


def bootstrap() -> None:
    """Sincroniza UMA vez (best-effort) quando o cache está vazio. Não trava a cada
    rerun: se falhar, só tenta de novo após 2 min. Reads usam retry curto."""
    global _BOOT_TS
    if _cache_lista():
        return
    if (time.time() - _BOOT_TS) < 120:
        return
    _BOOT_TS = time.time()
    try:
        sincronizar()
    except Exception:
        pass


def carregar(forcar: bool = False) -> list:
    """Lista de compromissos. Por padrão lê SÓ o cache local (rápido, sem rede).
    Sincroniza apenas se forcar=True (ex.: botão 'Sincronizar agenda')."""
    if forcar:
        try:
            return sincronizar()
        except Exception:
            pass
    return _cache_lista()


def _novo_id() -> str:
    return f"AG{int(time.time() * 1000)}"


def _cache_lista() -> list:
    try:
        return json.loads(cache.get_meta(_META, "") or "[]")
    except Exception:
        return []


def _cache_salvar(lista: list):
    cache.set_meta(_META, json.dumps(lista, ensure_ascii=False))


def adicionar(dados: dict, criado_por: str = "") -> str:
    """Acrescenta um compromisso (append na planilha) e atualiza SÓ o cache local
    (sem reler a aba inteira — economiza cota de leitura)."""
    reg = {c: "" for c in COLUNAS}
    reg.update({k: v for k, v in dados.items() if k in COLUNAS})
    reg["id"] = reg.get("id") or _novo_id()
    reg["status"] = reg.get("status") or "ativo"
    reg["criado_por"] = reg.get("criado_por") or criado_por
    reg["criado_em"] = reg.get("criado_em") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reg["data_base"] = fmt_date(reg.get("data_base"))
    ws = _ws()
    _api(lambda: ws.append_row([reg[c] for c in COLUNAS],
                               value_input_option="USER_ENTERED"))
    lista = _cache_lista()
    lista.append({c: reg.get(c, "") for c in COLUNAS})
    _cache_salvar(lista)
    return reg["id"]


def _achar_linha(ws, sp_id: str) -> int:
    """Índice (1-based) da linha cujo 'id' == sp_id, ou 0 se não achar."""
    col = _api(lambda: ws.col_values(COLUNAS.index("id") + 1))
    for i, v in enumerate(col):
        if str(v).strip() == str(sp_id).strip():
            return i + 1
    return 0


def atualizar(sp_id: str, dados: dict) -> bool:
    ws = _ws()
    ln = _achar_linha(ws, sp_id)
    if not ln:
        return False
    lista = _cache_lista()
    atual = next((c for c in lista if c.get("id") == sp_id), None) or {}
    novo = {c: atual.get(c, "") for c in COLUNAS}
    novo.update({k: v for k, v in dados.items() if k in COLUNAS})
    novo["id"] = sp_id
    if "data_base" in dados:
        novo["data_base"] = fmt_date(novo.get("data_base"))
    valores = [novo.get(c, "") for c in COLUNAS]
    fim_col = _cal_col_letra(len(COLUNAS))
    _api(lambda: ws.update(f"A{ln}:{fim_col}{ln}", [valores],
                           value_input_option="USER_ENTERED"))
    # atualiza só o item no cache local
    achou = False
    for i, c in enumerate(lista):
        if c.get("id") == sp_id:
            lista[i] = novo; achou = True; break
    if not achou:
        lista.append(novo)
    _cache_salvar(lista)
    return True


def concluir(sp_id: str, data_ocorrencia) -> bool:
    """Marca uma OCORRÊNCIA como concluída (acrescenta a data em concluido_em).
    Em compromisso sem recorrência, também marca status='concluido'."""
    c = next((x for x in _cache_lista() if x.get("id") == sp_id), None)
    if not c:
        return False
    feitas = _concluidas(c)
    d = parse_date(data_ocorrencia)
    if d:
        feitas.add(d)
    conc = ",".join(sorted(x.strftime("%Y-%m-%d") for x in feitas))
    upd = {"concluido_em": conc}
    if str(c.get("recorrencia", "nenhuma")).strip().lower() in ("", "nenhuma"):
        upd["status"] = "concluido"
    return atualizar(sp_id, upd)


def remover(sp_id: str) -> bool:
    ws = _ws()
    ln = _achar_linha(ws, sp_id)
    if not ln:
        return False
    _api(lambda: ws.delete_rows(ln))
    _cache_salvar([c for c in _cache_lista() if c.get("id") != sp_id])
    return True


def _cal_col_letra(n: int) -> str:
    """Número de coluna (1-based) -> letra(s) A, B, ... AA."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s