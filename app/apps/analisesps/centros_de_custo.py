# -*- coding: utf-8 -*-
"""
OS CENTROS DE CUSTO DA CONTABILIDADE × O PONTO (11/11/2026).

O dono: *"a folha da contabilidade tem determinado centro de custo, só que na
nomenclatura diferente da nossa (…) na aba C. Diários eu adicionei a última
coluna, a coluna AN, o centro de custo da contabilidade (…) a gente precisa gerar
um relatório onde o funcionário está trabalhando no ponto. Consequentemente,
baseado no ponto, eu vou saber qual é o centro de custo da contabilidade que ele
deverá estar cadastrado (…) comparar a folha da contabilidade e entender se
aqueles cadastros estão ok ou se precisa haver remanejo (…) esse relatório eu vou
passar para a contabilidade."*

DE ONDE VEM CADA COISA:
- O centro de custo NA FOLHA: o setor de cada pessoa na Folha Sintética importada
  ("001.07 - CONSTRUTORA/ESCRITORIO NORDESTE"; `folha_linha.setor_codigo/nome`).
- ONDE ELA TRABALHOU: o ponto do mês inteiro da competência
  (`ponto.dias_por_cpf`), a obra de cada dia (`folha_apropriacao.obra_do_dia`). A
  obra PRINCIPAL é a de mais dias.
- O CENTRO DE CUSTO ESPERADO: a coluna AN ("centro de custo da contabilidade") da
  aba C. Diários da planilha "Bases de Dados Pipefy" — a mesma que o ponto lê
  (`ponto.core.base_obras.ler_planilha`), procurada pelo nome e, sem o nome, na AN.
- A pessoa: pelo ID Fortes (o CPF vem do de/para do cadastro).

QUAL FOLHA: a complementar SUBSTITUI a folha do mês no sistema (a importação de
novo troca a da mesma competência e tipo), então a folha guardada já é a versão
mais nova. Entre quinzena e fim de mês, vale qualquer uma; a tela indica a de fim
de mês quando ela existe (é a completa).

AS SITUAÇÕES:
- `ok`         — o centro de custo da folha é o da obra principal do ponto.
- `coerente`   — está em AFASTADO INSS ou DESATIVAR e NÃO bateu ponto (o dono: *"a
                 gente subentende que está correto e coerente"*).
- `remanejar`  — o da folha é outro: sugere de qual para qual.
- `conferir`   — trabalhou em mais de uma obra e a folha está numa delas que não é
                 a principal; ou está em afastado/desativar e BATEU ponto; ou não
                 tem ponto no mês; ou a obra não tem centro de custo na coluna AN;
                 ou o ID Fortes não casa com o cadastro.
"""
from __future__ import annotations

import logging
import re
import time
import unicodedata

logger = logging.getLogger("analisesps.centros_de_custo")

OK, COERENTE, REMANEJAR, CONFERIR = "ok", "coerente", "remanejar", "conferir"
ORDEM = (REMANEJAR, CONFERIR, OK, COERENTE)
ROTULO = {OK: "Ok", COERENTE: "Coerente (afastado/desativar sem ponto)",
          REMANEJAR: "Remanejar", CONFERIR: "Conferir"}

# A coluna AN (índice 39 contando do zero) — pelo nome primeiro.
NOMES_DA_COLUNA = ("Centro de Custo Contabilidade", "Centro de Custo da Contabilidade",
                   "Centro Custo Contabilidade", "CC Contabilidade", "C. Custo Contabilidade",
                   "Centro de Custo Contábil", "Centro de Custo Contabil")
POSICAO_DA_COLUNA = 39          # AN
PALAVRAS_SEM_TRABALHO = ("AFASTAD", "DESATIV")
VALIDADE_DA_LEITURA_S = 300
_lido: dict = {}


def _sem_acento(texto) -> str:
    base = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode()
    return " ".join(base.upper().split())


def codigo_do_centro(texto) -> str:
    """PURA. "001.07 - CONSTRUTORA/ESCRITORIO NORDESTE" → "001.07"; sem código, ""."""
    achado = re.match(r"\s*(\d+(?:\.\d+)*)", str(texto or ""))
    return achado.group(1) if achado else ""


def mesmo_centro(a, b) -> bool:
    """PURA. Os dois textos apontam o mesmo centro de custo? Pelo código ("140.01"),
    aceitando o nível de cima ("140" casa com "140.01"); sem código, pelo texto."""
    ca, cb = codigo_do_centro(a), codigo_do_centro(b)
    if ca and cb:
        return ca == cb or ca.startswith(cb + ".") or cb.startswith(ca + ".")
    return bool(_sem_acento(a)) and _sem_acento(a) == _sem_acento(b)


def sem_trabalho(setor_nome) -> bool:
    """O centro de custo de quem não trabalha: AFASTADO INSS ou DESATIVAR."""
    nome = _sem_acento(setor_nome)
    return any(p in nome for p in PALAVRAS_SEM_TRABALHO)


# ---------------------------------------------------------------------------
# A coluna AN da C. Diários
# ---------------------------------------------------------------------------
def mapa_da_c_diarios(valores: list) -> dict:
    """PURA. `{"centros": {obra (sem acento, maiúscula): centro}, "coluna": letra,
    "titulo": cabeçalho, "achou_pelo_nome": bool, "obras_sem_centro": [códigos]}`.
    A obra entra pelo código primário, pelo código da coluna A e pelo nome."""
    from app.apps.ponto.core import base_obras
    if not valores:
        return {"centros": {}, "coluna": "", "titulo": "", "achou_pelo_nome": False,
                "obras_sem_centro": []}
    cabecalho = [str(c or "").strip() for c in valores[0]]
    # O cabeçalho no formato do leitor do ponto (sem acento, minúsculo) — é o que o
    # `_achar` dele compara.
    norm = [base_obras._sem_acento(c) for c in cabecalho]
    i_cc = base_obras._achar(norm, NOMES_DA_COLUNA)
    achou = i_cc is not None
    if i_cc is None:
        i_cc = POSICAO_DA_COLUNA
    i_codigo = base_obras._achar(norm, base_obras.NOMES_CODIGO)
    i_nome = base_obras._achar(norm, base_obras.NOMES_NOME)

    def cel(linha, i):
        return str(linha[i]).strip() if i is not None and i < len(linha) and linha[i] is not None else ""

    centros, sem = {}, []
    for linha in valores[1:]:
        codigo = cel(linha, i_codigo) or cel(linha, 0)
        if not codigo:
            continue
        centro = cel(linha, i_cc)
        if not centro:
            sem.append(codigo)
            continue
        for chave in (codigo, cel(linha, 0), cel(linha, i_nome)):
            if _sem_acento(chave):
                centros.setdefault(_sem_acento(chave), centro)
    return {"centros": centros, "coluna": base_obras._letra(i_cc),
            "titulo": cel(cabecalho, i_cc), "achou_pelo_nome": achou,
            "obras_sem_centro": sem}


def ler_mapa(recarregar: bool = False) -> dict:
    """A coluna AN da C. Diários, lida da planilha (guardada por 5 minutos)."""
    from app.apps.ponto.core import base_obras
    guardado = _lido.get("mapa")
    if guardado and not recarregar and time.monotonic() - guardado[0] < VALIDADE_DA_LEITURA_S:
        return guardado[1]
    mapa = mapa_da_c_diarios(base_obras.ler_planilha())
    _lido["mapa"] = (time.monotonic(), mapa)
    return mapa


# ---------------------------------------------------------------------------
# O ponto do mês, por pessoa
# ---------------------------------------------------------------------------
def obras_do_ponto(dias: list) -> list:
    """PURA. `[{"obra", "dias"}]` de quem bateu ponto, da obra com mais dias para a
    de menos. Dia sem obra (falta, folga, sem marcação) não conta."""
    from . import folha_apropriacao
    contagem: dict = {}
    for d in dias or []:
        obra = (folha_apropriacao.obra_do_dia(d.get("marcacoes"), d.get("presenca", ""),
                                              d.get("falta", "")) or {}).get("obra")
        if obra:
            contagem[obra] = contagem.get(obra, 0) + 1
    return [{"obra": o, "dias": n} for o, n in sorted(contagem.items(), key=lambda x: (-x[1], x[0]))]


# ---------------------------------------------------------------------------
# A comparação (PURA) e a montagem
# ---------------------------------------------------------------------------
def comparar_pessoa(linha: dict, obras: list, centros: dict, tem_cpf: bool) -> dict:
    """PURA. A situação de UMA pessoa da folha. `obras`: `obras_do_ponto`;
    `centros`: `{obra normalizada: centro da contabilidade}`."""
    setor = " - ".join(x for x in (linha.get("setor_codigo") or "", linha.get("setor_nome") or "") if x)
    for o in obras:
        o["centro"] = centros.get(_sem_acento(o["obra"]), "")
    saida = {"id_fortes": linha.get("id_fortes") or "", "nome": linha.get("nome") or "",
             "cpf": linha.get("cpf") or "", "centro_folha": setor, "obras": obras,
             "dias": sum(o["dias"] for o in obras), "centro_esperado": "", "situacao": OK,
             "critica": ""}
    principal = obras[0] if obras else None
    if principal:
        saida["centro_esperado"] = principal["centro"]
    if sem_trabalho(linha.get("setor_nome")):
        if not obras:
            saida.update(situacao=COERENTE, critica="sem ponto no mês — coerente com o centro de custo")
        else:
            saida.update(situacao=CONFERIR, critica=(
                f"está em {setor}, mas bateu ponto {saida['dias']} dia(s) — "
                f"{principal['obra']}" + (f" ({principal['centro']})" if principal["centro"] else "")))
        return saida
    if not tem_cpf:
        saida.update(situacao=CONFERIR,
                     critica="o ID Fortes não casa com o cadastro — não dá para ver o ponto")
        return saida
    if not obras:
        saida.update(situacao=CONFERIR, critica="sem ponto no mês (férias? afastamento?)")
        return saida
    if not principal["centro"]:
        saida.update(situacao=CONFERIR, critica=(
            f"a obra {principal['obra']} não tem centro de custo da contabilidade na "
            "C. Diários (coluna AN)"))
        return saida
    if mesmo_centro(setor, principal["centro"]):
        return saida
    outra = next((o for o in obras[1:] if o["centro"] and mesmo_centro(setor, o["centro"])), None)
    if outra:
        saida.update(situacao=CONFERIR, critica=(
            f"trabalhou em mais de uma obra: a folha está em {outra['obra']} ({outra['dias']} "
            f"dia(s)), mas a maior parte foi em {principal['obra']} ({principal['dias']} dia(s))"))
        return saida
    saida.update(situacao=REMANEJAR, critica=f"remanejar de {setor or '(sem centro)'} para "
                                             f"{principal['centro']}")
    return saida


def montar(folha_id: int, recarregar: bool = False) -> dict:
    """Tudo o que a tela e a planilha precisam."""
    from . import colaboradores, folha_arquivo
    from . import ponto as mod_ponto
    from .colaboradores import normalizar_id_fortes
    folha = folha_arquivo.abrir(folha_id)
    if folha is None:
        raise ValueError("folha não encontrada.")
    ano, mes = int(folha["ano"]), int(folha["mes"])
    base = {"folha": folha, "ano": ano, "mes": mes, "pessoas": [], "avisos": [],
            "contagem": {s: 0 for s in ORDEM}, "mapa": None,
            "tem_setor": bool(folha.get("tem_setor")), "outras_folhas": []}
    base["outras_folhas"] = [f for f in folha_arquivo.listar(teto=24)
                             if f["ano"] == ano and f["mes"] == mes and f["id"] != folha["id"]]
    if not base["tem_setor"]:
        base["avisos"].append("esta folha foi importada antes de o sistema guardar o centro de "
                              "custo de cada pessoa — importe o arquivo de novo.")
        return base
    try:
        mapa = ler_mapa(recarregar)
    except Exception as e:  # noqa: BLE001 — sem a planilha, a tela diz por quê
        logger.exception("Centros de custo: não consegui ler a C. Diários")
        mapa = {"centros": {}, "coluna": "", "titulo": "", "achou_pelo_nome": False,
                "obras_sem_centro": []}
        base["avisos"].append(f"não consegui ler a C. Diários: {e}")
    base["mapa"] = mapa
    if mapa["coluna"] and not mapa["achou_pelo_nome"]:
        base["avisos"].append(
            f'a coluna do centro de custo da contabilidade foi lida pela posição ({mapa["coluna"]}, '
            f'cabeçalho "{mapa["titulo"] or "vazio"}") — confira se é a certa.')
    de_para = colaboradores.de_para_do_fortes()
    dias = mod_ponto.dias_por_cpf(ano, mes)
    if not dias:
        base["avisos"].append(f"não há ponto carregado de {mes:02d}/{ano}: todos ficam sem ponto.")
    for l in folha["linhas"]:
        cpf = l.get("cpf") or (de_para.get(normalizar_id_fortes(l.get("id_fortes"))) or {}).get("cpf") or ""
        l = dict(l, cpf=cpf)
        base["pessoas"].append(comparar_pessoa(l, obras_do_ponto(dias.get(cpf) or []),
                                               mapa["centros"], bool(cpf)))
    base["pessoas"].sort(key=lambda p: (ORDEM.index(p["situacao"]), p["centro_folha"], p["nome"]))
    for p in base["pessoas"]:
        base["contagem"][p["situacao"]] += 1
    return base


# ---------------------------------------------------------------------------
# A planilha para a contabilidade
# ---------------------------------------------------------------------------
def planilha(montado: dict) -> bytes:
    """O Excel: "Remanejos" (o que a contabilidade precisa fazer) e "Conferência"
    (todo mundo, com o ponto)."""
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    wb = Workbook()
    competencia = f"{montado['mes']:02d}/{montado['ano']}"

    def aba(ws, titulo, cabecalho, linhas, larguras):
        ws.append([titulo])
        ws["A1"].font = Font(bold=True, size=13)
        ws.append([])
        ws.append(cabecalho)
        for c in ws[3]:
            c.font = Font(bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", fgColor="1F4E79")
        for linha in linhas:
            ws.append(linha)
        for i, w in enumerate(larguras, start=1):
            ws.column_dimensions[ws.cell(row=3, column=i).column_letter].width = w
        for row in ws.iter_rows(min_row=4):
            for c in row:
                c.alignment = Alignment(vertical="top", wrap_text=True)
        ws.freeze_panes = "A4"

    def obras_txt(p):
        return "; ".join(f"{o['obra']} ({o['dias']} dia(s))" + (f" → {o['centro']}" if o["centro"] else "")
                         for o in p["obras"])

    ws = wb.active
    ws.title = "Remanejos"
    remanejos = [p for p in montado["pessoas"] if p["situacao"] == REMANEJAR]
    aba(ws, f"Remanejos de centro de custo — folha {competencia} (pelo ponto do mês)",
        ["ID Fortes", "Colaborador", "Centro de custo na folha", "Centro de custo pelo ponto",
         "Obra(s) no ponto"],
        [[p["id_fortes"], p["nome"], p["centro_folha"], p["centro_esperado"], obras_txt(p)]
         for p in remanejos], [11, 38, 46, 46, 60])
    ws2 = wb.create_sheet("Conferência")
    aba(ws2, f"Conferência de centro de custo — folha {competencia}",
        ["Situação", "ID Fortes", "Colaborador", "Centro de custo na folha",
         "Centro de custo pelo ponto", "Dias no ponto", "Obra(s) no ponto", "Observação"],
        [[ROTULO[p["situacao"]], p["id_fortes"], p["nome"], p["centro_folha"],
          p["centro_esperado"], p["dias"], obras_txt(p), p["critica"]]
         for p in montado["pessoas"]], [16, 11, 38, 46, 46, 10, 60, 60])
    saida = io.BytesIO()
    wb.save(saida)
    return saida.getvalue()
