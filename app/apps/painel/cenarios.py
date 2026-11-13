# -*- coding: utf-8 -*-
"""
Cenários da prestação de contas — guardar um jeito de dividir, com nome.

O pedido do dono, em 22/09/2026, depois de olhar o que existia espalhado em
quatro telas:

    "Imagina você ter todo um trabalho de fazer um rateio, pá, pá, pá, de um
     jeito. Salvei, aí eu quero que seja salvo. Mas, e se de repente eu quiser
     um outro cenário? (…) Esse cenário eu vou utilizar 50% da mão de obra.
     Esse cenário eu vou utilizar o faturamento."

Um cenário guarda TUDO o que muda o resultado: a régua do rateio (mão de obra
ou faturamento), quanto de cada conta da matriz entra no bolo, a régua dos
juros, a taxa de administração e quem divide o quê. Trocar de cenário troca o
resultado inteiro — e o anterior continua lá, intacto, para comparar.

**Por que os percentuais são uma HIERARQUIA e não uma lista de regras:** a tela
antiga pedia um cadastro por regra, com nome, escopo e vigência. Dava trabalho,
e o dono disse isso com todas as letras. Aqui a conta da matriz herda o
percentual padrão do cenário; marcar um GRUPO sobrepõe; marcar uma CATEGORIA
sobrepõe o grupo; marcar um LANÇAMENTO sobrepõe a categoria. Configurar é
tocar em poucas linhas, não em todas.

Nada aqui calcula: isto lê e grava. A conta mora em `prestacao.py`.
"""
from __future__ import annotations

import re

from .db import conexao, consultar, consultar_um

CRITERIOS = {
    "pessoal": "Mão de obra — o custo de pessoal de cada obra",
    "faturamento": "Faturamento — o que cada obra recebeu",
}

# Quantos meses somar para medir a participação de cada obra. Janela curta
# reage rápido e balança; janela longa é estável e demora a reagir.
JANELAS = {"1": "o próprio mês", "3": "3 meses", "12": "12 meses",
           "acumulado": "tudo desde o início"}

NIVEIS = ("grupo", "categoria", "lancamento")

_CAMPOS = ("id", "nome", "criterio", "janela", "pct_padrao", "juros_por_deficit",
           "juros_sem_deficit", "taxa_adm_pct", "medida", "observacao")

_SELECT = ("SELECT id, nome, criterio, janela, pct_padrao, juros_por_deficit, "
           "       juros_sem_deficit, taxa_adm_pct, medida, observacao "
           "  FROM cenario")


def _linha(t) -> dict:
    cenario = dict(zip(_CAMPOS, t))
    for campo in ("pct_padrao", "taxa_adm_pct"):
        cenario[campo] = float(cenario[campo] or 0)
    cenario["juros_por_deficit"] = int(cenario["juros_por_deficit"] or 0)
    return cenario


# ----------------------------------------------------------------------- ler
def listar() -> list[dict]:
    return [_linha(t) for t in consultar(_SELECT + " ORDER BY nome")]


def buscar(cenario_id) -> dict | None:
    t = consultar_um(_SELECT + " WHERE id = ?", [int(cenario_id)])
    return _linha(t) if t else None


def pesos_fixos(cenario_id) -> dict:
    """{nível: {chave: percentual}} — o percentual fixo de cada conta, no
    formato antigo: QUANTO SE RATEIA entre todas (o resto fica no principal).
    Hoje só o lançamento marcado um a um nasce aqui; grupo e categoria passaram
    para as regras por período, e o que restar deles aqui é herança."""
    saida = {nivel: {} for nivel in NIVEIS}
    for nivel, chave, pct in consultar(
            "SELECT nivel, chave, pct FROM cenario_peso WHERE cenario_id = ?",
            [int(cenario_id)]):
        if nivel in saida:
            saida[nivel][chave] = float(pct or 0)
    return saida


def pesos(cenario_id) -> dict:
    """{nível: {chave: percentual fixo}} — ver `pesos_fixos` — e, em
    `periodos`, {nível: {chave: [(desde, fica no principal, é da filial)]}}:
    as três partes de cada conta (11/11/2026), em ordem de data."""
    saida = pesos_fixos(cenario_id)
    saida["periodos"] = periodos(cenario_id)
    return saida


def _colunas_dos_periodos() -> set:
    """As colunas de `cenario_peso_periodo` — vazio se a migração 024 ainda não
    foi aplicada. Diz também se a 025 (as três partes) já chegou."""
    try:
        return {c for (c,) in consultar(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = 'painel' AND table_name = 'cenario_peso_periodo'")}
    except Exception:  # noqa: BLE001 — banco de teste dublado
        return set()


def _tem_periodos() -> bool:
    """A migração 024 já foi aplicada? Sem ela, nada de período."""
    return bool(_colunas_dos_periodos())


def _tem_tres_partes() -> bool:
    """A migração 025 já foi aplicada? Sem ela não há onde guardar a parte da
    filial — a parte que fica no principal ainda cabe nas colunas antigas."""
    return "pct_fica" in _colunas_dos_periodos()


def partes_do_formato_antigo(pct, principal=None) -> tuple:
    """(fica no principal, é da filial) de uma linha gravada antes das três
    partes: `pct` era "quanto entra" e `principal` "quanto do que entra vai
    para o principal". O que não entrava FICA no principal; o resto se rateia
    entre todas. É a mesma conversão da migração 025."""
    entra = float(pct or 0)
    return (100.0 - entra) + entra * float(principal or 0) / 100.0, 0.0


def periodos(cenario_id) -> dict:
    """{nível: {chave: [(desde, fica no principal, é da filial, até, valor
    fixo), ...]}}, em ordem de começo. As duas partes em por cento; o que
    falta para 100 se rateia entre todas as obras.

    `desde` "1900-01" é a regra que vale sempre (a linha da conta); "0001-01"
    é o período que só tem fim ("até 12/2020"). `até` vazio = sem fim. `valor
    fixo` (R$) sai do total do período ANTES dos percentuais e fica inteiro no
    projeto principal."""
    saida: dict = {nivel: {} for nivel in NIVEIS}
    colunas = _colunas_dos_periodos()
    if not colunas:
        return saida
    novas = "pct_fica, pct_filial" if "pct_fica" in colunas else "NULL, NULL"
    fim = "ate" if "ate" in colunas else "''"
    fixo = "valor_fica" if "valor_fica" in colunas else "0"
    for nivel, chave, desde, pct, principal, fica, filial, ate, valor in consultar(
            f"SELECT nivel, chave, desde, pct, pct_principal, {novas}, {fim}, {fixo}"
            "  FROM cenario_peso_periodo"
            " WHERE cenario_id = ? ORDER BY nivel, chave, desde", [int(cenario_id)]):
        if nivel not in saida:
            continue
        if fica is None:
            fica, filial = partes_do_formato_antigo(pct, principal)
        saida[nivel].setdefault(chave, []).append(
            (desde, float(fica), float(filial or 0), ate or "", float(valor or 0)))
    return saida


def _tem_fim_de_periodo() -> bool:
    """A migração 026 já foi aplicada? Sem ela não há onde guardar o "até"."""
    return "ate" in _colunas_dos_periodos()


_MES = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


# o "mês" da regra que vale sempre — a linha da conta
DESDE_O_INICIO = "1900-01"
# o "começo" do período que só tem fim ("até 12/2020"): antes de tudo, e
# diferente do de cima para as duas linhas caberem na mesma conta
SEM_COMECO = "0001-01"


def _preenchido(valor) -> bool:
    return str(valor if valor is not None else "").strip() != ""


def mes_seguinte(mes: str) -> str:
    ano, m = int(mes[:4]), int(mes[5:7])
    return f"{ano + (m == 12):04d}-{(m % 12) + 1:02d}"


def mes_anterior(mes: str) -> str:
    ano, m = int(mes[:4]), int(mes[5:7])
    return f"{ano - (m == 1):04d}-{12 if m == 1 else m - 1:02d}"


def _mes_br(mes: str) -> str:
    return f"{mes[5:7]}/{mes[:4]}"


def dinheiro(valor) -> float:
    """Valor em reais digitado por gente: "500.000,00", "500000", "1.250,5".
    Em branco é zero; negativo vira positivo (é um valor a destacar)."""
    if isinstance(valor, (int, float)):
        return abs(float(valor))
    texto = str(valor or "").replace("R$", "").strip().replace(" ", "")
    if not texto:
        return 0.0
    if "," in texto:                       # padrão brasileiro: ponto é milhar
        texto = texto.replace(".", "").replace(",", ".")
    elif texto.count(".") > 1 or (texto.count(".") == 1 and len(texto.split(".")[1]) == 3):
        texto = texto.replace(".", "")     # "500.000" é quinhentos mil
    try:
        return abs(float(texto))
    except ValueError:
        raise ValueError(f"“{valor}” não é um valor em reais") from None


def _periodos_limpos(lista) -> list[tuple]:
    """[(desde, fica no principal, é da filial, até, valor fixo)] válidos e
    em ordem.

    Linha com as duas partes em branco e sem valor fixo é ignorada (é assim que se apaga);
    começo em branco quer dizer "sempre" — ou, se a linha tem fim, "até ele";
    começo repetido, vale o último. As duas partes não podem passar de 100 (o
    que falta é o que se rateia), o fim não pode vir antes do começo, e dois
    períodos com fim não podem se sobrepor."""
    limpos = {}
    for item in lista or []:
        desde, fica, filial, ate, valor = (list(item) + [None] * 5)[:5]
        valor = dinheiro(valor)
        if not _preenchido(fica) and not _preenchido(filial) and valor <= 0.005:
            continue
        desde, ate = (desde or "").strip(), (ate or "").strip()
        if ate and not _MES.match(ate):
            continue
        desde = desde or (SEM_COMECO if ate else DESDE_O_INICIO)
        if not _MES.match(desde):
            continue
        if ate and ate < desde:
            raise ValueError(f"o período termina em {_mes_br(ate)}, antes de começar "
                             f"({_mes_br(desde)})")
        f = _limpar(fica) if _preenchido(fica) else 0.0
        n = _limpar(filial) if _preenchido(filial) else 0.0
        if f + n > 100.005:
            raise ValueError(
                f"as partes somam {f + n:.0f}% — o que fica no projeto principal "
                "mais o que é da filial não pode passar de 100%")
        limpos[desde] = (f, n, ate, round(valor, 2))
    saida = [(d, f, n, ate, v) for d, (f, n, ate, v) in sorted(limpos.items())]
    # dois períodos fechados que se cruzam: não dá para saber qual vale
    com_data = [p for p in saida if p[0] != DESDE_O_INICIO]
    for atual, proximo in zip(com_data, com_data[1:]):
        if atual[3] and proximo[0] <= atual[3]:
            raise ValueError(
                f"dois períodos se sobrepõem: um vai até {_mes_br(atual[3])} e o "
                f"outro começa em {_mes_br(proximo[0])}")
    return saida


def lacunas_dos_periodos(lista) -> list[tuple]:
    """[(de, até)] dos meses que ficam ENTRE dois períodos sem regra de
    período nenhuma (12/11/2026, o dono: "e se ficar uma lacuna? O sistema
    acusa?"). Nesses meses vale a linha da conta — ou, sem ela, o nível de
    cima. Não é erro; é para a tela avisar."""
    com_data = sorted(p for p in lista or [] if p[0] != DESDE_O_INICIO)
    fim_de = lambda p: p[3] if len(p) > 3 else ""  # noqa: E731
    buracos = []
    for atual, proximo in zip(com_data, com_data[1:]):
        if not fim_de(atual) or mes_seguinte(fim_de(atual)) >= proximo[0]:
            continue
        de, ate = mes_seguinte(fim_de(atual)), mes_anterior(proximo[0])
        # um período mais antigo, sem fim (ou com fim depois), ainda cobre o vão
        coberto = any(p[0] <= de and (not fim_de(p) or fim_de(p) >= ate)
                      for p in com_data if p is not atual)
        if not coberto:
            buracos.append((de, ate))
    return buracos


def gravar_periodos(cenario_id, nivel: str, chave: str, lista) -> None:
    """Troca as regras de UMA conta pelas da lista
    [(desde, fica no principal, é da filial[, até[, valor fixo]])]."""
    if nivel not in NIVEIS or not (chave or "").strip() or not _tem_periodos():
        return
    limpos = _periodos_limpos(lista)
    colunas = _colunas_dos_periodos()
    tres, com_fim, com_valor = ("pct_fica" in colunas, "ate" in colunas,
                                "valor_fica" in colunas)
    aplique = ("aplique antes as atualizações do banco (Configurações › Aplicar "
               "atualizações do banco).")
    if not tres and any(p[2] > 0.005 for p in limpos):
        raise ValueError("Para dizer que parte de uma conta é da filial, " + aplique)
    if not com_fim and any(p[3] for p in limpos):
        raise ValueError("Para dar fim (\"até\") a um período, " + aplique)
    if not com_valor and any(p[4] > 0.005 for p in limpos):
        raise ValueError("Para destacar um valor fixo num período, " + aplique)
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_peso_periodo"
                     " WHERE cenario_id = ? AND nivel = ? AND chave = ?",
                     (int(cenario_id), nivel, chave.strip()))
        for desde, fica, filial, ate, valor in limpos:
            campos = {"cenario_id": int(cenario_id), "nivel": nivel,
                      "chave": chave.strip(), "desde": desde,
                      # sem a 025, a parte que fica cabe no formato antigo
                      "pct": 100.0 - fica - filial if tres else 100.0 - fica}
            if tres:
                campos.update(pct_fica=fica, pct_filial=filial)
            if com_fim:
                campos["ate"] = ate
            if com_valor:
                campos["valor_fica"] = valor
            conn.execute(
                f"INSERT INTO cenario_peso_periodo ({', '.join(campos)})"
                f" VALUES ({', '.join('?' for _ in campos)})", tuple(campos.values()))
        conn.commit()


# ---------------------------------------------------------------------------
# Quem RECEBE o custo da matriz, por projeto (09/11/2026)
# ---------------------------------------------------------------------------
PAPEIS = {"": "recebe sempre", "nao": "não recebe",
          "periodo": "recebe só no período"}


def _tem_projetos() -> bool:
    try:
        return bool(consultar(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'painel'"
            "   AND table_name = 'cenario_projeto'"))
    except Exception:  # noqa: BLE001
        return False


def projetos(cenario_id) -> dict:
    """{"principal": nome ou "", "regras": {projeto: (papel, desde, ate)}}.
    Projeto sem regra recebe sempre."""
    saida = {"principal": "", "regras": {}}
    if not _tem_projetos():
        return saida
    for projeto, papel, desde, ate in consultar(
            "SELECT projeto, papel, desde, ate FROM cenario_projeto"
            " WHERE cenario_id = ? ORDER BY projeto", [int(cenario_id)]):
        if papel == "principal":
            saida["principal"] = projeto
        elif papel in ("nao", "periodo"):
            saida["regras"][projeto] = (papel, desde or "", ate or "")
    return saida


def gravar_projetos(cenario_id, principal: str, regras: dict) -> None:
    """Troca tudo de uma vez: o principal e as regras {projeto: (papel, desde,
    ate)}. "Recebe sempre" não precisa de linha; período sem as duas datas
    vira "recebe sempre"."""
    if not _tem_projetos():
        return
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_projeto WHERE cenario_id = ?", [int(cenario_id)])
        if (principal or "").strip():
            conn.execute("INSERT INTO cenario_projeto (cenario_id, projeto, papel)"
                         " VALUES (?,?,'principal')", (int(cenario_id), principal.strip()))
        for projeto, (papel, desde, ate) in (regras or {}).items():
            projeto = (projeto or "").strip()
            if not projeto or projeto == (principal or "").strip():
                continue
            desde = desde.strip() if _MES.match((desde or "").strip()) else ""
            ate = ate.strip() if _MES.match((ate or "").strip()) else ""
            if papel == "periodo" and not (desde or ate):
                continue
            if papel not in ("nao", "periodo"):
                continue
            conn.execute("INSERT INTO cenario_projeto (cenario_id, projeto, papel, desde, ate)"
                         " VALUES (?,?,?,?,?)", (int(cenario_id), projeto, papel, desde, ate))
        conn.commit()


def recebe_no_mes(regra, mes: str) -> bool:
    """A regra de um projeto deixa ele receber neste mês (AAAA-MM)?"""
    if not regra:
        return True
    papel, desde, ate = regra
    if papel == "nao":
        return False
    if papel == "periodo":
        if desde and mes < desde:
            return False
        if ate and mes > ate:
            return False
    return True


def participacoes(cenario_id) -> list[dict]:
    """Quem divide, já com o nome do sócio — a tela nunca mostra número de
    cadastro para quem está lendo."""
    return [{"id": i, "obra": obra, "socio_id": s, "socio": nome,
             "tipo": tipo, "pct": float(pct or 0)}
            for i, obra, s, nome, tipo, pct in consultar(
                "SELECT p.id, p.obra, p.socio_id, s.nome, s.tipo, p.pct "
                "  FROM cenario_participacao p "
                "  JOIN socios s ON s.id = p.socio_id "
                " WHERE p.cenario_id = ? AND s.ativo = 1 "
                " ORDER BY p.obra, s.nome", [int(cenario_id)])]


def excluidas(cenario_id) -> list[str]:
    """O que fica FORA da análise: "obra:NOME" ou "projeto:NOME"."""
    return [i for (i,) in consultar(
        "SELECT item FROM cenario_excluida WHERE cenario_id = ? ORDER BY item",
        [int(cenario_id)])]


def completo(cenario_id) -> dict | None:
    """O cenário com tudo o que ele guarda — uma leitura só para a tela."""
    cenario = buscar(cenario_id)
    if not cenario:
        return None
    cenario["pesos"] = pesos(cenario_id)
    cenario["participacoes"] = participacoes(cenario_id)
    cenario["excluidas"] = excluidas(cenario_id)
    cenario["projetos"] = projetos(cenario_id)
    return cenario


# --------------------------------------------------------------------- gravar
def _limpar(valor, padrao: float = 0.0, teto: float = 100.0) -> float:
    """Percentual digitado por gente: vírgula vale, e fora da faixa é preso no
    limite em vez de virar erro na cara de quem está configurando."""
    try:
        v = float(str(valor).replace(",", ".").strip())
    except (TypeError, ValueError, AttributeError):
        return padrao
    return min(max(v, 0.0), teto)


def criar(nome: str, **campos) -> int:
    """Cria um cenário. Nome repetido é recusado pelo banco, de propósito:
    dois cenários com o mesmo nome tornam a comparação inútil."""
    nome = (nome or "").strip() or "Sem nome"
    with conexao() as conn:
        cur = conn.execute(
            "INSERT INTO cenario (nome, criterio, janela, pct_padrao, "
            "                     juros_por_deficit, juros_sem_deficit, "
            "                     taxa_adm_pct, medida, observacao) "
            "VALUES (?,?,?,?,?,?,?,?,?) RETURNING id",
            (nome,
             campos.get("criterio", "pessoal"),
             str(campos.get("janela", "1")),
             _limpar(campos.get("pct_padrao", 100), 100.0),
             1 if str(campos.get("juros_por_deficit", 1)) in ("1", "True", "on") else 0,
             campos.get("juros_sem_deficit", "sobra"),
             _limpar(campos.get("taxa_adm_pct", 1.5), 1.5),
             # cenário novo nasce pelo EXECUTADO (12/11/2026, o dono:
             # "gostaria de trabalhar com o executado, e não com o comprometido")
             campos.get("medida", "executado"),
             (campos.get("observacao") or "").strip()))
        novo = cur.fetchone()[0]
        conn.commit()
    return int(novo)


def atualizar(cenario_id, **campos) -> None:
    """Grava só os campos informados — a tela salva um bloco de cada vez."""
    permitidos = {
        "nome": lambda v: (str(v).strip() or "Sem nome"),
        "criterio": lambda v: (v if v in CRITERIOS else "pessoal"),
        "janela": lambda v: (str(v) if str(v) in JANELAS else "1"),
        "pct_padrao": lambda v: _limpar(v, 100.0),
        "juros_por_deficit": lambda v: 1 if str(v) in ("1", "True", "on") else 0,
        "juros_sem_deficit": lambda v: (v if v in ("sobra", "estrutura") else "sobra"),
        "taxa_adm_pct": lambda v: _limpar(v, 1.5),
        "medida": lambda v: ("executado" if v == "executado" else "comprometido"),
        "observacao": lambda v: (str(v) or "").strip(),
    }
    pares = [(c, f(campos[c])) for c, f in permitidos.items() if c in campos]
    if not pares:
        return
    sql = ("UPDATE cenario SET " + ", ".join(f"{c} = ?" for c, _ in pares)
           + ", atualizado_em = now() WHERE id = ?")
    with conexao() as conn:
        conn.execute(sql, [v for _c, v in pares] + [int(cenario_id)])
        conn.commit()


def marcar_peso(cenario_id, nivel: str, chave: str, pct) -> None:
    """Marca (ou desmarca) quanto de uma conta entra no bolo.

    Vazio APAGA a marcação em vez de gravar zero: são coisas diferentes — zero
    é "esta conta não se divide", vazio é "esta conta segue o nível de cima"."""
    if nivel not in NIVEIS or not (chave or "").strip():
        return
    chave = chave.strip()
    vazio = pct is None or str(pct).strip() == ""
    with conexao() as conn:
        if vazio:
            conn.execute("DELETE FROM cenario_peso "
                         " WHERE cenario_id = ? AND nivel = ? AND chave = ?",
                         (int(cenario_id), nivel, chave))
        else:
            conn.execute(
                "INSERT INTO cenario_peso (cenario_id, nivel, chave, pct) "
                "VALUES (?,?,?,?) ON CONFLICT (cenario_id, nivel, chave) "
                "DO UPDATE SET pct = excluded.pct",
                (int(cenario_id), nivel, chave, _limpar(pct)))
        conn.commit()


def marcar_pesos(cenario_id, nivel: str, chaves, pct) -> int:
    """O mesmo, para vários de uma vez — é assim que a tela marca em lote.

    O dono foi explícito: "não adianta uma coisa que eu tenho que digitar coisa
    por coisa". Marcar trinta lançamentos com 0% tem de ser um gesto só."""
    quantos = 0
    for chave in chaves:
        if (chave or "").strip():
            marcar_peso(cenario_id, nivel, chave, pct)
            quantos += 1
    return quantos


def conferir_periodos(entradas) -> dict:
    """{(nível, chave): [(desde, fica, filial, até)]} já limpo — ou ValueError
    com o nome da conta. Roda ANTES de gravar qualquer coisa: o "Salvar tudo"
    não pode gravar metade da página e recusar a outra.

    `entradas`: [(nível, chave, desde, fica, filial[, até])], com `desde`
    "inicio" na linha principal da conta."""
    brutos: dict = {}
    for entrada in entradas or ():
        nivel, chave, desde, fica, filial, ate, valor = (list(entrada) + [None] * 7)[:7]
        chave = (chave or "").strip()
        desde, ate = (desde or "").strip(), (ate or "").strip()
        try:
            tem_valor = dinheiro(valor) > 0.005
        except ValueError as e:
            raise ValueError(f"{chave}: {e}.") from None
        if desde == "inicio":                       # a linha principal da conta
            desde, ate, valor = DESDE_O_INICIO, "", None
        elif (_preenchido(fica) or _preenchido(filial) or tem_valor) and not desde and not ate:
            raise ValueError(f"Falta o mês em {chave}: a linha de período tem "
                             "percentual ou valor, mas não diz de quando até quando.")
        brutos.setdefault((nivel, chave), []).append((desde, fica, filial, ate, valor))
    limpos = {}
    for (nivel, chave), lista in brutos.items():
        try:
            limpos[(nivel, chave)] = _periodos_limpos(lista)
        except ValueError as e:
            raise ValueError(f"{chave}: {e}.") from None
    return limpos


def gravar_periodos_da_arvore(cenario_id, presentes, entradas) -> int:
    """As regras da árvore inteira, de uma vez. `presentes`: as contas cuja
    linha estava na tela ("nível|chave") — só elas são trocadas, para uma
    conta que a tela não mostrou não perder a regra dela. `entradas`:
    [(nível, chave, desde, fica no principal, é da filial, até)], com `desde`
    "inicio" na linha principal da conta. Devolve quantas contas mudaram.

    A linha principal é a regra "desde o início": o percentual fixo antigo
    (`cenario_peso`) da conta sai junto, senão ele voltaria a valer quando a
    pessoa apagasse a linha."""
    novos = conferir_periodos(entradas)
    atuais = periodos(cenario_id)
    fixos = pesos_fixos(cenario_id)
    mudou = 0
    for item in presentes:
        nivel, _, chave = (item or "").partition("|")
        chave = chave.strip()
        if nivel not in NIVEIS or not chave:
            continue
        lista = novos.get((nivel, chave), [])
        tem_fixo = chave in (fixos.get(nivel) or {})
        if lista == list((atuais.get(nivel) or {}).get(chave, [])) and not tem_fixo:
            continue
        gravar_periodos(cenario_id, nivel, chave, lista)
        if tem_fixo:
            with conexao() as conn:
                conn.execute("DELETE FROM cenario_peso"
                             " WHERE cenario_id = ? AND nivel = ? AND chave = ?",
                             (int(cenario_id), nivel, chave))
                conn.commit()
        mudou += 1
    return mudou


def marcar_pesos_da_arvore(cenario_id, itens) -> int:
    """A árvore inteira de uma vez: [(nível, chave, percentual digitado)].

    05/11/2026, o dono, nos Parâmetros: *"da forma que tá, um pouquinho
    complicado de lançar um a um"*. A montagem salvava linha por linha (um
    "ok" em cada). Agora a pessoa digita o grupo, abre, ajusta as categorias
    que quiser e salva tudo num clique. Só grava o que MUDOU: reescrever as
    linhas intocadas não muda nada, mas apagaria a diferença entre "deixei
    em branco" e "não mexi" se um dia ela importar."""
    atuais = pesos(cenario_id)
    mudou = 0
    for nivel, chave, pct in itens:
        if nivel not in NIVEIS or not (chave or "").strip():
            continue
        novo = "" if pct is None else str(pct).strip()
        atual = (atuais.get(nivel) or {}).get(chave.strip())
        if novo == "" and atual is None:
            continue
        if novo != "" and atual is not None:
            try:
                if abs(_limpar(novo) - float(atual)) < 0.0001:
                    continue
            except (TypeError, ValueError):
                pass
        marcar_peso(cenario_id, nivel, chave, novo)
        mudou += 1
    return mudou


def salvar_participacao(cenario_id, socio_id, pct, obra: str = "") -> None:
    with conexao() as conn:
        conn.execute(
            "INSERT INTO cenario_participacao (cenario_id, obra, socio_id, pct) "
            "VALUES (?,?,?,?) ON CONFLICT (cenario_id, obra, socio_id) "
            "DO UPDATE SET pct = excluded.pct",
            (int(cenario_id), (obra or "").strip(), int(socio_id), _limpar(pct)))
        conn.commit()


def conferir_grade(celulas: dict) -> dict:
    """A grade conferida, sem gravar: `{(obra, sócio): percentual | None}`.
    Separada para o "Salvar tudo" da montagem conferir ANTES de gravar
    qualquer seção — um percentual errado não deixa a página salva pela metade."""
    limpas = {}
    for (obra, socio_id), valor in celulas.items():
        texto = str(valor or "").strip()
        if not texto:
            limpas[((obra or "").strip(), int(socio_id))] = None
            continue
        try:
            v = float(texto.replace(",", "."))
        except ValueError:
            v = -1
        if not 0 <= v <= 100:
            raise ValueError(f"Percentual inválido em {obra or 'Todas as obras'}: "
                             f"\"{texto}\" (use um número de 0 a 100).")
        limpas[((obra or "").strip(), int(socio_id))] = v
    return limpas


def gravar_participacoes_em_grade(cenario_id, celulas: dict) -> int:
    """A grade "quem divide" inteira de uma vez: `{(obra, socio_id): "50"}`,
    obra "" = todas as obras. Célula vazia apaga. Confere tudo antes de
    gravar (11/11/2026 — ver `prestacao_dados.gravar_participacoes_em_grade`)."""
    limpas = conferir_grade(celulas)
    with conexao() as conn:
        for (obra, socio_id), pct in limpas.items():
            if pct is None:
                conn.execute("DELETE FROM cenario_participacao WHERE cenario_id = ?"
                             " AND obra = ? AND socio_id = ?",
                             (int(cenario_id), obra, socio_id))
            else:
                conn.execute(
                    "INSERT INTO cenario_participacao (cenario_id, obra, socio_id, pct) "
                    "VALUES (?,?,?,?) ON CONFLICT (cenario_id, obra, socio_id) "
                    "DO UPDATE SET pct = excluded.pct",
                    (int(cenario_id), obra, socio_id, pct))
        conn.commit()
    return sum(1 for v in limpas.values() if v is not None)


def apagar_participacao(participacao_id) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_participacao WHERE id = ?",
                     [int(participacao_id)])
        conn.commit()


def excluir(cenario_id, item: str) -> None:
    """Tira uma obra ("obra:NOME") ou um projeto inteiro ("projeto:NOME") da
    análise deste cenário. Repetir não dá erro."""
    item = (item or "").strip()
    if not (item.startswith("obra:") or item.startswith("projeto:")) or len(item) < 6:
        return
    with conexao() as conn:
        conn.execute("INSERT INTO cenario_excluida (cenario_id, item) VALUES (?,?) "
                     "ON CONFLICT DO NOTHING", (int(cenario_id), item))
        conn.commit()


def gravar_fora(cenario_id, itens) -> None:
    """A lista "fora da análise" deste cenário inteira, de uma vez (11/11/2026):
    fora totalmente ("obra:X") ou só depois de um mês ("obra:X@2024-06")."""
    limpos = []
    for item in itens or ():
        item = (item or "").strip()
        if (item.startswith(("obra:", "projeto:")) and len(item) >= 6
                and item not in limpos):
            limpos.append(item)
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_excluida WHERE cenario_id = ?", (int(cenario_id),))
        for item in limpos:
            conn.execute("INSERT INTO cenario_excluida (cenario_id, item) VALUES (?,?)",
                         (int(cenario_id), item))
        conn.commit()


def reincluir(cenario_id, item: str) -> None:
    with conexao() as conn:
        conn.execute("DELETE FROM cenario_excluida WHERE cenario_id = ? AND item = ?",
                     (int(cenario_id), (item or "").strip()))
        conn.commit()


def duplicar(cenario_id, nome: str) -> int:
    """Copia um cenário inteiro com outro nome.

    É o que torna barato perguntar "e se fosse faturamento?": parte-se do que
    já está pronto e troca-se uma coisa só, em vez de configurar tudo de novo.
    """
    origem = buscar(cenario_id)
    if not origem:
        raise ValueError("Cenário não encontrado.")
    novo = criar(nome, **{c: origem[c] for c in _CAMPOS if c not in ("id", "nome")})
    with conexao() as conn:
        conn.execute(
            "INSERT INTO cenario_peso (cenario_id, nivel, chave, pct) "
            "SELECT ?, nivel, chave, pct FROM cenario_peso WHERE cenario_id = ?",
            (novo, int(cenario_id)))
        conn.execute(
            "INSERT INTO cenario_participacao (cenario_id, obra, socio_id, pct) "
            "SELECT ?, obra, socio_id, pct FROM cenario_participacao "
            " WHERE cenario_id = ?",
            (novo, int(cenario_id)))
        conn.execute(
            "INSERT INTO cenario_excluida (cenario_id, item) "
            "SELECT ?, item FROM cenario_excluida WHERE cenario_id = ?",
            (novo, int(cenario_id)))
        if _tem_periodos():
            colunas = "nivel, chave, desde, pct, pct_principal" + (
                ", pct_fica, pct_filial" if _tem_tres_partes() else "") + (
                ", ate" if _tem_fim_de_periodo() else "") + (
                ", valor_fica" if "valor_fica" in _colunas_dos_periodos() else "")
            conn.execute(
                f"INSERT INTO cenario_peso_periodo (cenario_id, {colunas})"
                f" SELECT ?, {colunas}"
                " FROM cenario_peso_periodo WHERE cenario_id = ?", (novo, int(cenario_id)))
        if _tem_projetos():
            conn.execute(
                "INSERT INTO cenario_projeto (cenario_id, projeto, papel, desde, ate)"
                " SELECT ?, projeto, papel, desde, ate FROM cenario_projeto"
                " WHERE cenario_id = ?", (novo, int(cenario_id)))
        conn.commit()
    return novo


def apagar(cenario_id) -> None:
    """Apaga o cenário e tudo o que pende dele (o banco cuida do resto)."""
    with conexao() as conn:
        conn.execute("DELETE FROM cenario WHERE id = ?", [int(cenario_id)])
        conn.commit()
