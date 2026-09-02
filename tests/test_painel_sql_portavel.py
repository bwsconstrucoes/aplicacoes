# -*- coding: utf-8 -*-
"""
O SQL do painel é Postgres, não SQLite.

O código do espelho nasceu falando com um arquivo SQLite. Ao trocar para
Postgres, sobraram construções que existem só lá — e cada uma só apareceu
quando quebrou em produção, sempre depois de horas de download:

  - `GROUP BY <apelido>` quando a tabela tem coluna com o mesmo nome;
  - `conn.cursor()` sem tradução dos marcadores;
  - `MAX(a, b)`: no SQLite é "o maior de dois valores"; no Postgres, `MAX` é
    agregação de UM argumento. A carga morria com
    "function max(text, text) does not exist" **depois** de já ter baixado
    todas as contas a pagar.

Achar isso uma por vez, cada uma custando uma carga inteira, não é aceitável.
Este arquivo varre o código atrás da classe inteira de problema — inclusive das
que ainda não aconteceram.

Não abre banco: lê o próprio código. É de propósito, porque o defeito mora no
texto do SQL, e assim o teste roda também no PC de quem edita, sem Postgres.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import re

import pytest

PAINEL = pathlib.Path(__file__).resolve().parents[1] / "app" / "apps" / "painel"

# `referencia_streamlit/` guarda as telas antigas, que continuam falando SQLite
# de propósito — elas não rodam. Tudo o mais aqui conversa com o Postgres.
IGNORADAS = {"referencia_streamlit"}

# Construções que só existem no SQLite, ou que lá querem dizer outra coisa.
PROIBIDAS = {
    r"\bMAX\s*\([^();]*,": "MAX de dois valores é do SQLite — no Postgres use GREATEST",
    r"\bMIN\s*\([^();]*,": "MIN de dois valores é do SQLite — no Postgres use LEAST",
    r"\bIFNULL\s*\(": "IFNULL é do SQLite — use COALESCE",
    r"\bINSTR\s*\(": "INSTR é do SQLite — use POSITION ou STRPOS",
    r"\bGROUP_CONCAT\s*\(": "GROUP_CONCAT é do SQLite — use STRING_AGG",
    r"\bJULIANDAY\s*\(": "JULIANDAY é do SQLite",
    r"\bTYPEOF\s*\(": "TYPEOF é do SQLite",
    r"\bAUTOINCREMENT\b": "AUTOINCREMENT é do SQLite — use BIGSERIAL",
    r"\bPRAGMA\b": "PRAGMA é do SQLite",
    r"INSERT\s+OR\s+(REPLACE|IGNORE)": "é do SQLite — use ON CONFLICT",
    # `.strftime(` é o método do Python e é legítimo; o proibido é a função SQL
    # de mesmo nome, que apareceria solta dentro de uma consulta.
    r"(?<![.\w])strftime\s*\(\s*['\"]": "strftime de SQL é do SQLite — use to_char",
    r"(?<![.\w])datetime\s*\(\s*['\"]now": "datetime('now') é do SQLite — use now()",
}


def sem_prosa(texto: str, sufixo: str = ".py") -> str:
    """Remove comentários e DOCSTRINGS, deixando só o código.

    Necessário porque as explicações deste projeto citam justamente as
    construções proibidas: a docstring que ensina "não use MAX(a, b)" contém
    `MAX(a, b)`. Um teste que tropeça na própria explicação não serve.

    Mas não dá para apagar tudo que está entre aspas triplas — boa parte do SQL
    mora ali. Então as docstrings são localizadas pela árvore sintática (só as
    de verdade, as que abrem módulo, classe ou função) e só as linhas delas
    somem.
    """
    if sufixo == ".sql":
        return re.sub(r"--[^\n]*", "", texto)

    linhas = texto.splitlines()
    apagar: set[int] = set()
    try:
        arvore = ast.parse(texto)
    except SyntaxError:
        arvore = None
    if arvore is not None:
        alvos = [arvore] + [
            no for no in ast.walk(arvore)
            if isinstance(no, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
        for no in alvos:
            corpo = getattr(no, "body", None)
            if not corpo:
                continue
            primeiro = corpo[0]
            if (isinstance(primeiro, ast.Expr)
                    and isinstance(primeiro.value, ast.Constant)
                    and isinstance(primeiro.value.value, str)):
                fim = primeiro.end_lineno or primeiro.lineno
                apagar.update(range(primeiro.lineno, fim + 1))

    return "\n".join(
        "" if (numero in apagar) or linha.lstrip().startswith("#") else linha
        for numero, linha in enumerate(linhas, start=1))


def _arquivos():
    for caminho in sorted(PAINEL.rglob("*")):
        if caminho.suffix not in (".py", ".sql"):
            continue
        if any(parte in IGNORADAS for parte in caminho.parts):
            continue
        yield caminho


@pytest.mark.parametrize("padrao,motivo", list(PROIBIDAS.items()))
def test_o_sql_do_painel_nao_usa_construcao_de_sqlite(padrao, motivo):
    achados = []
    for caminho in _arquivos():
        codigo = sem_prosa(caminho.read_text(encoding="utf-8"), caminho.suffix)
        for numero, linha in enumerate(codigo.splitlines(), start=1):
            if re.search(padrao, linha):
                achados.append(f"{caminho.name}:{numero}  {linha.strip()[:90]}")
    assert not achados, f"{motivo}\n" + "\n".join(achados)


def test_rowid_nao_e_usado_como_chave():
    """O SQLite dá um `rowid` a toda tabela; o Postgres não. O código apagava
    movimentos por rowid — hoje a tabela tem uma coluna `id` de verdade."""
    achados = []
    for caminho in _arquivos():
        codigo = sem_prosa(caminho.read_text(encoding="utf-8"), caminho.suffix)
        if re.search(r"\browid\b", codigo, re.IGNORECASE):
            achados.append(caminho.name)
    assert not achados, f"rowid não existe no Postgres: {achados}"


def test_busca_por_texto_usa_ilike():
    """No SQLite `LIKE` ignora maiúsculas; no Postgres, não. Um `LIKE '%Retido%'`
    portado sem pensar deixaria de achar "RETIDO" — e a receita líquida sairia
    errada sem aparecer erro nenhum.

    `LIKE` continua valendo para casar prefixo técnico (as marcas de etapa da
    carga, por exemplo). O que se exige é que a busca por TEXTO use ILIKE."""
    suspeitos = []
    for caminho in _arquivos():
        codigo = sem_prosa(caminho.read_text(encoding="utf-8"), caminho.suffix)
        for numero, linha in enumerate(codigo.splitlines(), start=1):
            for trecho in re.findall(r"(?<!I)LIKE\s+'([^']*)'", linha):
                # '%Retido%' busca texto; 'carga:%' casa prefixo técnico
                if trecho.startswith("%") and any(c.isupper() for c in trecho):
                    suspeitos.append(f"{caminho.name}:{numero}  LIKE '{trecho}'")
    assert not suspeitos, (
        "busca por texto com LIKE distingue maiúsculas no Postgres; use ILIKE:\n"
        + "\n".join(suspeitos))


def test_a_maior_data_e_comparada_como_data_e_nao_como_texto():
    """A data de referência do incremental é guardada como texto dd/mm/aaaa.
    Comparar como texto poria "31/12/2024" acima de "01/01/2025" — e a
    atualização diária passaria a rebaixar mais do que precisa, ou, num arranjo
    diferente, a pular o que deveria trazer."""
    from app.apps.painel.sync import espelho

    codigo = sem_prosa(inspect.getsource(espelho._atualizar_sync_state))
    assert "_dalt_para_data" in codigo, \
        "a comparação tem de passar pelo interpretador de data, não por texto"
    assert "GREATEST" not in codigo and "MAX(" not in codigo, \
        "a escolha da maior data é feita em Python, não no SQL"


def test_o_proprio_filtro_de_prosa_funciona():
    """Se `sem_prosa` falhasse, os testes acima passariam sempre — e a rede de
    proteção seria só aparência."""
    exemplo = (
        'def f():\n'
        '    """Não use MAX(a, b) aqui."""\n'
        '    # nem PRAGMA em comentário\n'
        '    sql = """SELECT GREATEST(a, b) FROM t"""\n'
    )
    limpo = sem_prosa(exemplo)
    assert "MAX(a, b)" not in limpo        # a docstring saiu
    assert "PRAGMA" not in limpo           # o comentário saiu
    assert "GREATEST(a, b)" in limpo       # o SQL entre aspas triplas ficou
