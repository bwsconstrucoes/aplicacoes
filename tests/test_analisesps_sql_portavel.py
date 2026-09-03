# -*- coding: utf-8 -*-
"""
O SQL da Análise de SPs é Postgres — e passa por um tradutor antes de chegar lá.

Duas classes de defeito que este arquivo pega SEM abrir banco, e que só
apareceriam na tela do operador:

1. CONSTRUÇÃO DE SQLITE. O módulo nasceu falando com um arquivo SQLite. A mesma
   varredura que o painel faz vale aqui, pela mesma razão: cada uma dessas
   sobras custou uma carga inteira lá.

2. O SINAL DE PORCENTAGEM. Este módulo usa `LIKE` muito mais do que o painel —
   busca livre, "contém" no centro de custo, palavra dentro do código de
   barras. O `db.py` traduz `?` para `%s` e protege o `%` literal virando `%%`,
   porque o psycopg2 consome um nível de `%` ao interpolar os parâmetros. Se
   essa proteção falhar, o banco recebe `LIKE '%falha%'` com o `%f` comido no
   caminho — e a consulta volta vazia sem erro nenhum. Silenciosa, que é o pior
   tipo.

Não abre banco: lê o próprio código e simula o que o psycopg2 faz. É de
propósito — assim roda também no PC de quem edita, onde não há Postgres.
"""
from __future__ import annotations

import ast
import pathlib
import re

import pytest

MODULO = (pathlib.Path(__file__).resolve().parents[1]
          / "app" / "apps" / "analisesps")

# Construções que só existem no SQLite, ou que lá querem dizer outra coisa.
#
# A lista é a mesma do teste equivalente do painel, e está repetida aqui de
# propósito em vez de importada: os dois módulos são independentes, e o teste
# de um não pode quebrar porque o arquivo do outro mudou de nome ou saiu.
PROIBIDAS = {
    r"\bMAX\s*\([^();]*,": "MAX de dois valores é do SQLite — no Postgres use GREATEST",
    r"\bMIN\s*\([^();]*,": "MIN de dois valores é do SQLite — no Postgres use LEAST",
    r"\bIFNULL\s*\(": "IFNULL é do SQLite — use COALESCE",
    r"\bINSTR\s*\(": "INSTR é do SQLite — use POSITION ou STRPOS",
    r"\bGROUP_CONCAT\s*\(": "GROUP_CONCAT é do SQLite — use STRING_AGG",
    r"\bJULIANDAY\s*\(": "JULIANDAY é do SQLite",
    r"\bTYPEOF\s*\(": "TYPEOF é do SQLite",
    r"\bAUTOINCREMENT\b": "AUTOINCREMENT é do SQLite — use GENERATED AS IDENTITY",
    r"\bPRAGMA\b": "PRAGMA é do SQLite",
    r"INSERT\s+OR\s+(REPLACE|IGNORE)": "é do SQLite — use ON CONFLICT",
    r"\browid\b": "rowid é do SQLite — o Postgres não dá coluna implícita",
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


def test_o_proprio_filtro_de_prosa_funciona():
    """Se `sem_prosa` falhasse, a varredura passaria sempre — e a rede de
    proteção seria só aparência."""
    exemplo = ('def f():\n'
               '    """Não use MAX(a, b) aqui."""\n'
               '    # nem PRAGMA em comentário\n'
               '    sql = """SELECT GREATEST(a, b) FROM t"""\n')
    limpo = sem_prosa(exemplo)
    assert "MAX(a, b)" not in limpo        # a docstring saiu
    assert "PRAGMA" not in limpo           # o comentário saiu
    assert "GREATEST(a, b)" in limpo       # o SQL entre aspas triplas ficou

# A pasta `app/` guarda o Streamlit original, que continua falando SQLite de
# propósito — ele roda no computador do dono e não sobe para o serviço.
#
# ATENÇÃO ao comparar: o caminho ABSOLUTO deste módulo contém `app/` duas
# vezes (é `.../aplicacoes/app/apps/analisesps/...`). Comparar contra as partes
# do caminho absoluto excluía TODOS os arquivos, e a varredura passava sobre
# nada — verde, e sem valer nada. Por isso o filtro é sobre o caminho RELATIVO
# ao módulo, e por isso o teste-sentinela logo abaixo existe.
IGNORADAS = {"app", ".venv", "runtime", "data", "secrets", "__pycache__"}


def _arquivos():
    for caminho in sorted(MODULO.rglob("*")):
        if caminho.suffix not in (".py", ".sql"):
            continue
        relativo = caminho.relative_to(MODULO)
        if any(parte in IGNORADAS for parte in relativo.parts):
            continue
        yield caminho


def test_a_varredura_enxerga_os_arquivos():
    """Se o filtro de pastas ficasse largo demais, os testes abaixo passariam
    sempre — sobre nenhum arquivo."""
    nomes = {c.name for c in _arquivos()}
    assert {"consultas.py", "sincronizacao.py", "db.py",
            "001_estrutura.sql"} <= nomes


@pytest.mark.parametrize("padrao,motivo", list(PROIBIDAS.items()))
def test_nao_usa_construcao_de_sqlite(padrao, motivo):
    achados = []
    for caminho in _arquivos():
        codigo = sem_prosa(caminho.read_text(encoding="utf-8"), caminho.suffix)
        for numero, linha in enumerate(codigo.splitlines(), start=1):
            if re.search(padrao, linha):
                achados.append(f"{caminho.name}:{numero}  {linha.strip()[:90]}")
    assert not achados, f"{motivo}\n" + "\n".join(achados)


def test_todo_like_com_texto_ignora_maiuscula_de_proposito():
    """No SQLite `LIKE` ignora maiúsculas; no Postgres, não.

    Aqui a solução não é `ILIKE`: é aplicar `lower()` ou `upper()` na coluna
    antes de comparar, porque as mesmas colunas são comparadas com valores
    vindos da tela, já rebaixados em Python. O que este teste exige é que uma
    das duas coisas esteja presente — nunca um `LIKE` cru contra texto."""
    suspeitos = []
    for caminho in _arquivos():
        codigo = sem_prosa(caminho.read_text(encoding="utf-8"), caminho.suffix)
        for numero, linha in enumerate(codigo.splitlines(), start=1):
            for trecho in re.findall(r"(?<!I)LIKE\s+'([^']*)'", linha):
                if not trecho.startswith("%"):
                    continue              # casa prefixo técnico, não texto
                if "lower(" in linha or "upper(" in linha or "ILIKE" in linha:
                    continue              # a coluna já foi rebaixada
                suspeitos.append(f"{caminho.name}:{numero}  LIKE '{trecho}'")
    assert not suspeitos, (
        "LIKE contra texto distingue maiúsculas no Postgres. Use lower()/upper() "
        "na coluna, ou ILIKE:\n" + "\n".join(suspeitos))


# ---------------------------------------------------------------------------
# O sinal de porcentagem sobrevive à tradução
# ---------------------------------------------------------------------------
def como_o_psycopg2_veria(sql: str, quantos_parametros: int) -> str:
    """Simula o que o psycopg2 faz: interpola os parâmetros e, ao fazê-lo,
    consome um nível de `%`. É por isso que o `%` literal precisa ir dobrado."""
    return sql % tuple([f"'p{i}'" for i in range(quantos_parametros)])


@pytest.mark.parametrize("sql_original,esperado_no_banco", [
    ("SELECT 1 WHERE a LIKE '%falha%'", "SELECT 1 WHERE a LIKE '%falha%'"),
    ("SELECT 1 WHERE a LIKE ?", "SELECT 1 WHERE a LIKE 'p0'"),
    ("SELECT 1 WHERE a LIKE '%COM RISCO%' AND b = ?",
     "SELECT 1 WHERE a LIKE '%COM RISCO%' AND b = 'p0'"),
    ("SELECT 1 WHERE a ~ '^0+$'", "SELECT 1 WHERE a ~ '^0+$'"),
])
def test_o_por_cento_chega_inteiro_ao_banco(sql_original, esperado_no_banco):
    """O caso que dói: `LIKE '%falha%'` sem a proteção viraria `LIKE 'alha'`
    depois da interpolação — o `%f` seria lido como marcador de formato. A
    consulta voltaria vazia, sem erro, e o filtro de agendamento simplesmente
    deixaria de achar as SPs com falha."""
    from app.apps.analisesps.db import traduzir_placeholders
    traduzido = traduzir_placeholders(sql_original)
    quantos = traduzido.count("%s")
    assert como_o_psycopg2_veria(traduzido, quantos) == esperado_no_banco


def test_o_sql_de_verdade_deste_modulo_sobrevive_a_traducao():
    """Não são exemplos: são as consultas que o módulo realmente manda ao
    banco, com todos os `%` e todas as expressões regulares que elas têm."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.db import traduzir_placeholders

    trechos = {
        "status_agend": consultas.SQL_STATUS_AGEND,
        "risco": consultas.SQL_RISCO,
        "cadastro_incompleto": consultas.SQL_CADASTRO_INCOMPLETO,
        "boleto_invalido": consultas.SQL_BOLETO_INVALIDO,
        "boleto_duplicado": consultas.SQL_BOLETO_DUPLICADO,
        "hoje_brasilia": consultas.SQL_HOJE,
    }
    for nome, sql in trechos.items():
        traduzido = traduzir_placeholders(sql)
        recomposto = como_o_psycopg2_veria(traduzido, traduzido.count("%s"))
        assert recomposto == sql, (
            f"o trecho '{nome}' chega diferente ao banco depois da tradução:\n"
            f"  saiu daqui: {sql}\n  chega assim: {recomposto}")


def test_a_consulta_montada_inteira_sobrevive():
    """O caminho completo: filtros da tela -> condições -> SQL final.

    O SQL de saída tem `?` onde entram os parâmetros, e o recomposto tem o
    valor no lugar deles — então a comparação é contra o original com os `?`
    já trocados pelos mesmos rótulos. O que se verifica é o resto: cada `%`
    do corpo do comando chega inteiro."""
    from app.apps.analisesps import consultas
    from app.apps.analisesps.db import traduzir_placeholders

    onde, params = consultas._condicoes({
        "busca": "cimento, 100%",           # o próprio termo tem um %
        "status_agend": ["Agendar", "Sem Agendamento"],
        "centro_custo": ["OBRA-12"],
        "situacoes": ["boleto_invalido", "boleto_duplicado",
                      "cadastro_incompleto", "risco"],
        "status_pgt": ["Pagar"],
    })
    sql = "SELECT * FROM analisesps.sps WHERE " + " AND ".join(onde)
    traduzido = traduzir_placeholders(sql)
    assert traduzido.count("%s") == len(params), (
        "a quantidade de marcadores não bate com a de parâmetros — o banco "
        "recusaria a consulta")

    esperado = sql
    for i in range(len(params)):
        esperado = esperado.replace("?", f"'p{i}'", 1)
    assert como_o_psycopg2_veria(traduzido, len(params)) == esperado


def test_o_termo_de_busca_com_por_cento_e_texto_e_nao_curinga():
    """Quem procura "100%" quer o texto "100%".

    Sem escapar, o `%` do operador viraria curinga do LIKE e a busca acharia
    "1000", "100 sacos", qualquer coisa começada em 100 — silenciosamente
    diferente do Streamlit, que faz busca literal. O mesmo vale para o `_`."""
    from app.apps.analisesps import consultas

    onde, params = consultas._condicoes({"busca": "100%"})
    assert params == ["%100\\%%"]
    assert "100" not in " ".join(onde)      # o valor não entra no comando

    _, params = consultas._condicoes({"busca": "nota_1"})
    assert params == ["%nota\\_1%"]

    _, params = consultas._condicoes({"centro_custo": ["OBRA_1"]})
    assert params == ["%obra\\_1%"]


def test_a_barra_invertida_digitada_tambem_e_escapada():
    """Se ela não fosse escapada primeiro, um termo terminado em barra comeria
    o escape do caractere seguinte e o Postgres recusaria o padrão."""
    from app.apps.analisesps import consultas
    _, params = consultas._condicoes({"busca": "c:\\pasta"})
    assert params == ["%c:\\\\pasta%"]


# ---------------------------------------------------------------------------
# A migração
# ---------------------------------------------------------------------------
def test_a_migracao_cria_tudo_dentro_do_schema_proprio():
    """O ERP tem tabelas de nome genérico (`titulos`, `categorias`, `rateios`).
    Uma tabela criada sem o prefixo cairia no schema `public` e poderia colidir
    com o ERP — o pior defeito possível, porque a escrita de um módulo
    alcançaria dado do outro."""
    sql = (MODULO / "migracoes" / "001_estrutura.sql").read_text(encoding="utf-8")
    sql = sem_prosa(sql, ".sql")
    criacoes = re.findall(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
                          sql, re.IGNORECASE)
    assert criacoes, "a migração não cria tabela nenhuma — algo está errado"
    fora = [nome for nome in criacoes if not nome.lower().startswith("analisesps.")]
    assert not fora, f"tabelas criadas fora do schema próprio: {fora}"

    indices = re.findall(r"CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+ON\s+(\S+)",
                         sql, re.IGNORECASE)
    fora = [nome for nome in indices if not nome.lower().startswith("analisesps.")]
    assert not fora, f"índices criados fora do schema próprio: {fora}"


def test_a_migracao_e_repetivel():
    """O botão pode ser apertado duas vezes, e o Render pode reiniciar no meio.
    Tudo que a migração cria usa IF NOT EXISTS."""
    sql = sem_prosa(
        (MODULO / "migracoes" / "001_estrutura.sql").read_text(encoding="utf-8"),
        ".sql")
    for comando in re.findall(r"CREATE\s+(TABLE|INDEX|SCHEMA)\s+(.{0,30})",
                              sql, re.IGNORECASE):
        assert "IF NOT EXISTS" in comando[1].upper(), (
            f"CREATE {comando[0]} sem IF NOT EXISTS: {comando[1].strip()}")


def test_as_colunas_da_migracao_batem_com_o_mapeamento_da_planilha():
    """Se as duas listas divergirem, a gravação estoura com "column does not
    exist" — no meio de uma carga, depois de já ter lido a planilha inteira."""
    from app.apps.analisesps import colunas
    sql = (MODULO / "migracoes" / "001_estrutura.sql").read_text(encoding="utf-8")
    trecho = sql[sql.index("CREATE TABLE IF NOT EXISTS analisesps.sps"):]
    trecho = trecho[:trecho.index(");")]
    for chave in colunas.CHAVES:
        assert re.search(rf"^\s+{re.escape(chave)}\s+", trecho, re.MULTILINE), (
            f"a coluna '{chave}' está no mapeamento da planilha mas não existe "
            "na tabela")
    for derivada in ("valor_num", "solicitacao_d", "vencimento_d",
                     "data_pagamento_d", "dt_autorizacao_d"):
        assert re.search(rf"^\s+{derivada}\s+", trecho, re.MULTILINE)


# ---------------------------------------------------------------------------
# A varredura completa: TODO literal de SQL do módulo
# ---------------------------------------------------------------------------
PALAVRAS_DE_SQL = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|GROUP BY|ORDER BY|VALUES|"
    r"CASE|COALESCE)\b", re.I)


def _literais_de_texto(caminho):
    """Todo literal de string do arquivo, pela árvore sintática.

    Pela árvore, e não por expressão regular, porque boa parte do SQL deste
    módulo é montada por pedaços dentro de f-strings — e uma regex sobre o
    texto do arquivo pegaria os pedaços pela metade."""
    arvore = ast.parse(caminho.read_text(encoding="utf-8"))
    for no in ast.walk(arvore):
        if isinstance(no, ast.Constant) and isinstance(no.value, str):
            yield no.value, no.lineno
        elif isinstance(no, ast.JoinedStr):
            for parte in no.values:
                if isinstance(parte, ast.Constant) and isinstance(parte.value, str):
                    yield parte.value, no.lineno


def test_todo_sql_do_modulo_sobrevive_a_traducao():
    """A varredura que não depende de eu lembrar de listar um trecho novo.

    Cada `LIKE '%pix%'`, cada expressão regular do Postgres, cada `%` que
    aparecer em qualquer consulta deste módulo passa por aqui. Um deles mal
    traduzido devolve resultado VAZIO em vez de erro — silencioso, que é o
    pior tipo de defeito: o filtro simplesmente deixa de achar."""
    from app.apps.analisesps.db import traduzir_placeholders

    suspeitos = []
    conferidos = 0
    for arq in _arquivos():
        if arq.suffix != ".py":
            continue
        for texto, linha in _literais_de_texto(arq):
            if not PALAVRAS_DE_SQL.search(texto):
                continue
            if "%" not in texto and "?" not in texto:
                continue
            conferidos += 1
            traduzido = traduzir_placeholders(texto)
            quantos = traduzido.count("%s")
            voltou = como_o_psycopg2_veria(traduzido, quantos)
            esperado = texto
            for i in range(quantos):
                esperado = esperado.replace("?", f"'p{i}'", 1)
            if voltou != esperado:
                suspeitos.append(f"{arq.name}:{linha}  {texto[:70]!r}")

    assert conferidos > 30, (
        f"a varredura só achou {conferidos} pedaços de SQL — o filtro de "
        "arquivos ou de palavras deve ter ficado estreito demais")
    assert not suspeitos, (
        "estes pedaços de SQL chegam DIFERENTES ao banco depois da tradução "
        "dos marcadores:\n" + "\n".join(suspeitos))


# ---------------------------------------------------------------------------
# Bradesco: a normalização do código de barras
# ---------------------------------------------------------------------------
LINHA_44 = "34191790010104351004791020150008291070026000"
DIGITAVEL_47 = "34191.79001 01043.510047 91020.150008 2 91070026000"


def test_a_linha_digitavel_e_o_codigo_de_barras_viram_o_mesmo():
    """O boleto aparece de duas formas: o código de barras de 44 dígitos e a
    linha digitável de 47, que a pessoa digita. São o mesmo boleto, e a
    conferência do extrato depende de os dois casarem.

    Este teste nasceu de um defeito silencioso: o import que faz essa conversão
    era achatado (`import pagamentos`), o que dentro de um pacote não resolve —
    e como ele mora num `try/except`, a falha não aparecia. O código caía no
    caminho simples e a conferência deixava de casar QUALQUER boleto na forma
    de 47 dígitos, sem erro nenhum na tela."""
    from app.apps.analisesps import bradesco
    assert bradesco._norm_barcode(LINHA_44) == bradesco._norm_barcode(DIGITAVEL_47)
    assert len(bradesco._norm_barcode(DIGITAVEL_47)) == 44


def test_codigo_estranho_nao_estoura():
    """Dado ruim colado do banco não pode derrubar a conferência."""
    from app.apps.analisesps import bradesco
    for entrada in ("", "abc", None, "123"):
        bradesco._norm_barcode(entrada)          # não pode levantar


# ---------------------------------------------------------------------------
# Nenhum import achatado sobrou
# ---------------------------------------------------------------------------
def test_nenhum_modulo_do_pacote_e_importado_de_forma_achatada():
    """Os arquivos vindos do Streamlit se importavam pelo nome curto, porque lá
    a pasta estava no caminho de busca. Dentro de um pacote isso não resolve.

    O perigo é que boa parte desses imports mora dentro de `try/except`: a
    falha não aparece, e a função cai num caminho de reserva — em silêncio."""
    import ast

    nossos = {c.stem for c in MODULO.glob("*.py")} - {"__init__"}
    achados = []
    for caminho in _arquivos():
        if caminho.suffix != ".py":
            continue
        arvore = ast.parse(caminho.read_text(encoding="utf-8"))
        for no in ast.walk(arvore):
            if isinstance(no, ast.Import):
                for nome in no.names:
                    if nome.name.split(".")[0] in nossos:
                        achados.append(f"{caminho.name}:{no.lineno}  import {nome.name}")
            elif isinstance(no, ast.ImportFrom):
                if no.level == 0 and no.module and no.module.split(".")[0] in nossos:
                    achados.append(f"{caminho.name}:{no.lineno}  from {no.module} import ...")
    assert not achados, (
        "estes imports usam o nome curto de um módulo do próprio pacote; dentro "
        "de um pacote eles não resolvem. Use `from . import x`:\n"
        + "\n".join(achados))


# ---------------------------------------------------------------------------
# Nada que o serviço não tenha
# ---------------------------------------------------------------------------
def test_o_modulo_nao_depende_de_pandas_nem_de_streamlit():
    """As duas bibliotecas que o Streamlit usava e que o serviço NÃO tem.

    `pandas` é o ponto: era ele que abria as 59 mil SPs na memória. Se voltar a
    entrar por descuido, volta junto o problema que motivou a conversão."""
    import ast

    proibidas = {"pandas", "streamlit", "numpy", "altair", "st_aggrid",
                 "reportlab", "openpyxl"}
    achados = []
    for caminho in _arquivos():
        if caminho.suffix != ".py":
            continue
        arvore = ast.parse(caminho.read_text(encoding="utf-8"))
        for no in ast.walk(arvore):
            nomes = []
            if isinstance(no, ast.Import):
                nomes = [a.name.split(".")[0] for a in no.names]
            elif isinstance(no, ast.ImportFrom) and no.module:
                nomes = [no.module.split(".")[0]]
            for nome in nomes:
                if nome in proibidas:
                    achados.append(f"{caminho.name}:{no.lineno}  {nome}")
    assert not achados, (
        "o serviço não tem estas bibliotecas — e o pandas, em particular, é o "
        "que abria as 59 mil SPs na memória:\n" + "\n".join(achados))


def test_tudo_que_o_modulo_importa_esta_no_requirements():
    """Uma biblioteca que falta no `requirements.txt` só aparece no Render,
    depois da publicação, e derruba o módulo inteiro no start."""
    import ast
    import sys

    # .../aplicacoes/app/apps/analisesps -> parents[2] é a raiz do repositório.
    raiz = MODULO.parents[2]
    requisitos = (raiz / "requirements.txt").read_text(encoding="utf-8").lower()

    # O nome que se importa nem sempre é o que se instala.
    INSTALADO_COMO = {
        "flask": "flask", "gspread": "gspread", "google": "google-auth",
        "qrcode": "qrcode", "barcode": "python-barcode",
        "psycopg2": "psycopg2-binary", "sqlalchemy": "sqlalchemy",
        "PIL": "pillow", "requests": "requests",
    }

    faltando = []
    for caminho in _arquivos():
        if caminho.suffix != ".py":
            continue
        arvore = ast.parse(caminho.read_text(encoding="utf-8"))
        for no in ast.walk(arvore):
            nomes = []
            if isinstance(no, ast.Import):
                nomes = [a.name.split(".")[0] for a in no.names]
            elif isinstance(no, ast.ImportFrom) and no.level == 0 and no.module:
                nomes = [no.module.split(".")[0]]
            for nome in nomes:
                if nome in sys.stdlib_module_names:
                    continue
                if nome in {c.stem for c in MODULO.glob("*.py")}:
                    continue
                pacote = INSTALADO_COMO.get(nome)
                if pacote is None:
                    faltando.append(f"{caminho.name}:{no.lineno}  '{nome}' "
                                    "não está na lista de nomes conhecidos")
                elif pacote not in requisitos:
                    faltando.append(f"{caminho.name}:{no.lineno}  '{nome}' precisa "
                                    f"de '{pacote}' no requirements.txt")
    assert not faltando, "\n".join(faltando)
