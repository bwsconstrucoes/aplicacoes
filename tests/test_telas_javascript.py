"""As telas do ERP, olhadas como JavaScript.

Existe por um defeito que passou por toda a suíte e ficou em produção: três
telas de Suprimentos (Cotações, Pedidos e Banco de preços) declaravam
`function moeda(...)` enquanto a base já declarava `const moeda`. Em
JavaScript isso não é "a última vence": é **erro de sintaxe**, e o bloco
inteiro de script da tela deixa de rodar. As três telas abriam com o cabeçalho
e o corpo vazio, e nenhum teste via nada — porque o servidor respondia 200,
com o HTML certo, e o erro só acontece no navegador.

O que este arquivo confere, tela por tela:

  1. nenhuma tela redeclara um nome que a base já declarou no topo;
  2. nenhuma tela chama uma função de ajuda que não existe em lugar nenhum
     (foi assim que a coluna de data do banco de preços chamava `data()`,
     que nunca existiu);
  3. o JavaScript de cada tela é sintaticamente válido.

O item 3 só roda se houver `node` na máquina; os outros dois rodam sempre.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

TEMPLATES = Path(__file__).resolve().parents[1] / "app" / "apps" / "erp" / "templates"
BASE = TEMPLATES / "erp_base.html"

# Nomes que a tela pode usar sem declarar: vêm do navegador ou da base.
DO_NAVEGADOR = {
    "window", "document", "location", "console", "fetch", "Promise", "Number",
    "String", "Object", "Array", "Set", "Map", "Math", "JSON", "Date", "URL",
    "URLSearchParams", "FormData", "Blob", "Intl", "setTimeout", "clearTimeout",
    "setInterval", "clearInterval", "isNaN", "parseInt", "parseFloat", "alert",
    "confirm", "encodeURIComponent", "decodeURIComponent", "btoa", "atob",
    "MutationObserver", "IntersectionObserver", "requestAnimationFrame",
    "structuredClone", "AbortController", "Error", "RegExp", "Boolean",
}


def _blocos(caminho: Path) -> list[str]:
    return re.findall(r"<script>(.*?)</script>", caminho.read_text(encoding="utf-8"), re.S)


def _sem_jinja(texto: str) -> str:
    texto = re.sub(r"\{\{.*?\}\}", "null", texto, flags=re.S)
    texto = re.sub(r"\{%.*?%\}", "", texto, flags=re.S)
    return re.sub(r"\{#.*?#\}", "", texto, flags=re.S)


def _so_codigo(js: str) -> str:
    """Tira comentário e texto de string, deixando só o que roda.

    Sem isto, uma palavra em português dentro de um comentário — "situação
    (ver abaixo)" — parece uma chamada de função. Dentro de crase, o que roda
    é só o que está em ${...}, e é isso que fica.
    """
    js = re.sub(r"/\*.*?\*/", " ", js, flags=re.S)
    js = re.sub(r"(?m)//.*$", " ", js)

    saida, i, n = [], 0, len(js)
    while i < n:
        c = js[i]
        if c in "'\"":
            i += 1
            while i < n and js[i] != c:
                i += 2 if js[i] == "\\" else 1
            i += 1
            saida.append(' "" ')
        elif c == "`":
            i += 1
            while i < n and js[i] != "`":
                if js[i] == "\\":
                    i += 2
                elif js[i] == "$" and i + 1 < n and js[i + 1] == "{":
                    i += 2
                    nivel, ini = 1, i
                    while i < n and nivel:
                        if js[i] == "{":
                            nivel += 1
                        elif js[i] == "}":
                            nivel -= 1
                        if nivel:
                            i += 1
                    saida.append(" " + _so_codigo(js[ini:i]) + " ")
                    i += 1
                else:
                    i += 1
            i += 1
            saida.append(' "" ')
        else:
            saida.append(c)
            i += 1
    return "".join(saida)


def _declarados(codigo: str) -> set[str]:
    """Nomes declarados no NÍVEL DE CIMA do script — que é onde o choque
    acontece. Declaração dentro de função tem escopo próprio e não colide."""
    nomes = set()
    for linha in codigo.split("\n"):
        if linha[:1] not in ("c", "l", "v", "f", "a"):     # não começa no topo
            continue
        m = re.match(r"(?:const|let|var)\s+([A-Za-z_$][\w$]*)", linha)
        if m:
            nomes.add(m.group(1))
        m = re.match(r"(?:async\s+)?function\s+([A-Za-z_$][\w$]*)", linha)
        if m:
            nomes.add(m.group(1))
    return nomes


NOMES_DA_BASE = set().union(*(_declarados(_sem_jinja(b)) for b in _blocos(BASE)))


def _estende_a_base(p: Path) -> bool:
    """A tela herda do `erp_base.html`?

    QUASE TODAS herdam, e para elas as travas deste arquivo valem. A exceção é
    a PÁGINA SOLTA: o portal da obra (06/11/2026) é HTML inteiro, sem o menu e
    sem a sessão do ERP, porque quem o abre é gente de obra sem login do ERP.
    Nela não há base nenhuma para chocar — e exigir os ajudantes da base seria
    exigir um arquivo que ela não carrega.
    """
    return "{% extends" in p.read_text(encoding="utf-8")[:400]


TELAS = sorted(p for p in TEMPLATES.glob("erp_*.html")
               if p != BASE and _estende_a_base(p))
SOLTAS = sorted(p for p in TEMPLATES.glob("erp_*.html")
                if p != BASE and not _estende_a_base(p))


@pytest.mark.parametrize("tela", SOLTAS, ids=lambda p: p.name)
def test_a_pagina_solta_traz_os_proprios_ajudantes(tela: Path):
    """Página que não herda a base tem de declarar o que usa.

    É o outro lado da moeda: a tela que herda não pode REDECLARAR `els`; a que
    não herda é obrigada a DECLARAR. Sem isto, a página solta abriria com
    "els is not defined" no console e em branco na tela — e quem a abre é gente
    de obra no celular, que não vai abrir console nenhum.
    """
    codigo = "\n".join(_sem_jinja(b) for b in _blocos(tela))
    declarados = _declarados(codigo)
    for usado in ("els", "esc"):
        if usado + "(" in codigo:
            assert usado in declarados, (
                f"{tela.name} usa `{usado}(` e não declara — a página abre em "
                f"branco, porque ela não carrega os ajudantes da base.")


def test_a_base_declara_os_ajudantes_que_as_telas_usam():
    """Se este teste falhar, a base mudou de nome e as telas vão junto."""
    assert {"moeda", "numero", "els", "api", "aviso", "dataBR", "comBusca"} <= NOMES_DA_BASE


@pytest.mark.parametrize("tela", TELAS, ids=lambda p: p.name)
def test_a_tela_nao_redeclara_nome_da_base(tela: Path):
    """Redeclarar `const` da base é erro de sintaxe: a tela inteira morre."""
    for bloco in _blocos(tela):
        chocam = _declarados(_sem_jinja(bloco)) & NOMES_DA_BASE
        assert not chocam, (
            f"{tela.name} redeclara {sorted(chocam)}, que a base já declara. "
            f"Isso derruba TODO o script desta tela no navegador — ela abre "
            f"vazia. Use o ajudante da base, ou dê outro nome ao seu.")


@pytest.mark.parametrize("tela", TELAS, ids=lambda p: p.name)
def test_a_tela_nao_chama_ajudante_que_nao_existe(tela: Path):
    """Pega o caso da coluna de data que chamava `data()`: a função nunca
    existiu, e o erro só aparecia quando a linha era desenhada."""
    codigo = _so_codigo("\n".join(_sem_jinja(b) for b in _blocos(tela)))
    conhecidos = NOMES_DA_BASE | DO_NAVEGADOR | _declarados(codigo)
    # declarado em qualquer profundidade, não só no topo
    conhecidos |= set(re.findall(r"(?:const|let|var)\s+([A-Za-z_$][\w$]*)", codigo))
    conhecidos |= set(re.findall(r"function\s+([A-Za-z_$][\w$]*)", codigo))
    # parâmetro de função também é um nome válido dentro dela
    conhecidos |= set(re.findall(r"([A-Za-z_$][\w$]*)\s*=>", codigo))
    for lista in re.findall(r"\(([^()]*)\)\s*(?:=>|\{)", codigo):
        for parte in lista.split(","):
            nome = parte.split("=")[0].strip().lstrip(".")
            if re.fullmatch(r"[A-Za-z_$][\w$]*", nome):
                conhecidos.add(nome)

    chamadas = set(re.findall(r"(?<![.\w$])([a-z_$][\w$]*)\s*\(", codigo))
    palavras = {"if", "for", "while", "switch", "catch", "return", "typeof",
                "function", "await", "new", "else", "do", "in", "of", "case",
                "delete", "void", "throw", "yield", "async", "var", "let",
                "const", "instanceof"}
    faltando = sorted(chamadas - conhecidos - palavras)
    assert not faltando, (
        f"{tela.name} chama {faltando}, que não está declarado nem na tela nem "
        f"na base. No navegador isso é 'is not defined' na hora de desenhar.")


@pytest.mark.parametrize("tela", TELAS + [BASE], ids=lambda p: p.name)
def test_o_javascript_da_tela_e_valido(tela: Path):
    if not shutil.which("node"):
        pytest.skip("node não instalado — a checagem de sintaxe fica de fora")
    for i, bloco in enumerate(_blocos(tela)):
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False,
                                         encoding="utf-8") as tmp:
            tmp.write(_sem_jinja(bloco))
            caminho = tmp.name
        r = subprocess.run(["node", "--check", caminho], capture_output=True, text=True)
        Path(caminho).unlink()
        assert r.returncode == 0, f"{tela.name} bloco {i}:\n{r.stderr}"


# ---------------------------------------------------------------------------
# O elemento existe na página?
#
# O defeito que motivou este teste: a tela de Solicitações do financeiro
# chamava `els("opcoes-conta")` para um painel de filtro que NUNCA foi
# escrito no HTML. `els(...)` devolveu nulo, o `innerHTML` estourou dentro do
# `try`, e a tela ficou meses mostrando "Não foi possível carregar" — sem erro
# nenhum no console, porque o `catch` engoliu.
#
# A regra: todo id pedido por `els("x")` tem de aparecer como `id="x"` em
# algum lugar do arquivo — no HTML da página ou no HTML que o próprio
# JavaScript monta. Ids que a tela cria por `createElement` ficam na lista de
# exceções abaixo, com o motivo.
# ---------------------------------------------------------------------------

# Ids criados em tempo de execução (createElement) e não escritos como id="".
IDS_CRIADOS_EM_CODIGO = {
    "dlg-pergunta",     # erp_base.html: a caixa de diálogo genérica
    # erp_base.html: o cadastro de empresa, que precisa de diálogo PRÓPRIO —
    # quando é chamado de dentro da criação de obra, o diálogo da obra já está
    # aberto e não pode ser substituído.
    "dlg-empresa",
}


def _ids_pedidos(codigo: str) -> set[str]:
    return set(re.findall(r'els\(\s*"([\w-]+)"\s*\)', codigo)) | \
           set(re.findall(r'getElementById\(\s*"([\w-]+)"\s*\)', codigo))


@pytest.mark.parametrize("tela", TELAS + [BASE], ids=lambda p: p.name)
def test_a_tela_nao_procura_elemento_que_nao_existe(tela: Path):
    texto = tela.read_text(encoding="utf-8")
    # AQUI o código vai CRU, sem passar por `_so_codigo`: o id que interessa
    # mora dentro das aspas de `els("...")`, e é justamente o conteúdo das
    # aspas que aquela função joga fora.
    codigo = "\n".join(_blocos(tela))
    # ids escritos como id="x" — no HTML da página e dentro das strings de JS
    escritos = set(re.findall(r'id="([\w-]+)"', texto))
    escritos |= set(re.findall(r"id='([\w-]+)'", texto))
    # a base é comum a todas as telas: o que ela declara vale para todas
    escritos |= set(re.findall(r'id="([\w-]+)"', BASE.read_text(encoding="utf-8")))
    # e ids montados com interpolação (id="pg-${i}") não se resolvem aqui
    prefixos = tuple(p for p in re.findall(r'id="([\w-]*?)\$\{', texto))

    faltando = sorted(
        i for i in _ids_pedidos(codigo)
        if i not in escritos
        and i not in IDS_CRIADOS_EM_CODIGO
        and not any(i.startswith(p) for p in prefixos if p))
    assert not faltando, (
        f"{tela.name} procura o(s) elemento(s) {faltando}, que não existe(m) "
        f"em lugar nenhum da página. No navegador `els(...)` devolve nulo e a "
        f"tela quebra — normalmente dentro de um try, sem aviso no console.")


# ---------------------------------------------------------------------------
# BOTÃO QUE NÃO FAZ NADA
#
# Esta trava nasceu de um defeito de verdade: o "+ Colaborador" montava o
# formulário inteiro e NUNCA ABRIA A JANELA. Nenhum erro no console, nenhuma
# mensagem — o botão simplesmente não respondia. A única coisa que pega isso é
# olhar a tela, e olhar a tela não roda na suíte. Então a suíte confere o que
# dá para conferir: toda tela que tem uma janela `<dialog>` precisa abri-la em
# algum lugar, e a função que a preenche tem de ser a mesma que a abre.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tela", TELAS + SOLTAS, ids=lambda p: p.name)
def test_a_tela_que_tem_janela_sabe_abrir_a_janela(tela: Path):
    texto = tela.read_text(encoding="utf-8")
    codigo = "\n".join(_blocos(tela))
    if "<dialog" not in texto:
        return
    # ids de <dialog> declarados nesta tela
    janelas = set(re.findall(r'<dialog[^>]*\bid="([\w-]+)"', texto))
    if not janelas:
        return
    abertas = set(re.findall(r'els\("([\w-]+)"\)\.showModal\(\)', codigo))
    abertas |= set(re.findall(r'getElementById\("([\w-]+)"\)\.showModal\(\)',
                              codigo))
    nunca_abertas = sorted(janelas - abertas)
    assert not nunca_abertas, (
        f"{tela.name} declara a(s) janela(s) {nunca_abertas} e nunca chama "
        f"showModal() nela(s). No navegador o botão que deveria abri-la não "
        f"faz nada, e não aparece erro nenhum no console.")


@pytest.mark.parametrize("tela", TELAS + SOLTAS, ids=lambda p: p.name)
def test_quem_preenche_a_janela_tambem_a_abre(tela: Path):
    """Preencher sem abrir é o defeito exato do "+ Colaborador"."""
    codigo = "\n".join(_blocos(tela))
    # as funções que escrevem no corpo de uma janela
    for m in re.finditer(r"(?:async\s+)?function\s+(\w+)\s*\([^)]*\)\s*\{",
                         codigo):
        nome = m.group(1)
        # o corpo da função, por contagem de chaves
        i = codigo.index("{", m.start())
        nivel, j = 0, i
        while j < len(codigo):
            if codigo[j] == "{":
                nivel += 1
            elif codigo[j] == "}":
                nivel -= 1
                if nivel == 0:
                    break
            j += 1
        corpo = codigo[i:j]
        if 'els("dlg-corpo").innerHTML' not in corpo:
            continue
        assert "showModal()" in corpo, (
            f"{tela.name}: a função {nome}() escreve no corpo da janela mas "
            f"não a abre. No navegador o botão que chama {nome}() não faz "
            f"nada, sem erro nenhum no console.")


# ---------------------------------------------------------------------------
# A TELA NÃO PODE PROMETER O QUE NÃO TEM
#
# Regra do dono, 17/09/2026: o documento entra DENTRO do formulário, como um
# bloco no topo. A tela de colaborador chegou a escrever "jogue o documento no
# bloco abaixo" e NÃO TINHA BLOCO NENHUM — o caminho só existia para quem já
# estava cadastrado. Promessa sem botão é pior que não prometer: a pessoa
# procura, não acha, e conclui que o sistema está quebrado.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tela", TELAS + SOLTAS, ids=lambda p: p.name)
def test_a_tela_que_fala_em_jogar_documento_tem_onde_jogar(tela: Path):
    texto = tela.read_text(encoding="utf-8")
    if not re.search(r"[Jj]ogue o documento|[Jj]ogue aqui|arraste o documento",
                     texto):
        return
    # um campo de arquivo, escrito no HTML, montado em JavaScript, ou pedido
    # ao `perguntar()` da base com `tipo: "arquivo"`
    tem_onde = ('type="file"' in texto or "type='file'" in texto
                or 'type=\\"file\\"' in texto
                or re.search(r'tipo:\s*"arquivo"', texto) is not None)
    assert tem_onde, (
        f"{tela.name} convida a jogar o documento e não tem campo de arquivo "
        f"nenhum. A pessoa procura o bloco, não acha, e conclui que o sistema "
        f"está quebrado.")


# ---------------------------------------------------------------------------
# A FOLHA DE ESTILO QUE NÃO CARREGA
#
# Defeito de verdade, achado em 07/11/2026: a tela do portal pedia
# `url_for('static', ...)` — o `static` DA APLICAÇÃO — mas o `erp.css` mora no
# blueprint do ERP, que é `erp.static`. A página subiu em produção SEM FOLHA DE
# ESTILO NENHUMA, e nada acusou: o servidor responde 200 na página, o 404 é só
# do arquivo, e não aparece em teste que olha HTML.
#
# Pior: a maquete que eu usei para fotografar a tela carregava uma CÓPIA LOCAL
# do erp.css, então a foto saía bonita enquanto a página de verdade estava
# pelada. Maquete com arquivo local esconde exatamente este erro.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("tela", TELAS + SOLTAS + [BASE], ids=lambda p: p.name)
def test_a_folha_de_estilo_do_erp_vem_do_blueprint_do_erp(tela: Path):
    texto = tela.read_text(encoding="utf-8")
    errados = re.findall(r"url_for\(\s*'static'\s*,\s*filename\s*=\s*'([^']+)'",
                         texto)
    errados += re.findall(r'url_for\(\s*"static"\s*,\s*filename\s*=\s*"([^"]+)"',
                          texto)
    assert not errados, (
        f"{tela.name} pede {errados} de `static` (o da aplicação), mas os "
        f"arquivos do ERP moram em `erp.static`. A página sobe 200 e o arquivo "
        f"dá 404 — a tela aparece sem estilo e nada acusa.")


# ---------------------------------------------------------------------------
# DATA COM HORA PASSADA PARA O AJUDANTE DE DATA
#
# Defeito de verdade, achado ao olhar a tela do diário em 08/11/2026: ela
# mostrava **"fechado em 08 17:50:45.327771+00:00/11/2026"**.
#
# `dataBR` recebe "2026-11-08" e devolve "08/11/2026" — ele parte no hífen e
# inverte. Dado um TIMESTAMP ("2026-11-08 17:50:45.327771+00:00"), a última
# parte vira "08 17:50:45.327771+00:00" e o resultado é aquele embrulho. O
# `erp_base.html` já tem `momentoBR` para isto, e o comentário dele avisa com
# todas as letras — eu usei o errado mesmo assim.
#
# O teste é textual porque o defeito é textual: nada quebra, nada vai para o
# console, e só aparece para quem abre a tela e lê.
# ---------------------------------------------------------------------------
# As colunas que o banco guarda COM HORA, lidas do schema e das migrações —
# não de uma lista escrita à mão, que envelheceria. A primeira versão desta
# trava chutava pelo NOME (qualquer `*_em`) e acusou `protocolado_em` e
# `retencao_liberada_em`, que são DATE: alarme falso em metade dos achados.
# O que decide é o TIPO da coluna, e ele está escrito no SQL.
def _colunas_com_hora() -> set[str]:
    sql = [Path("app/apps/erp/schema.sql")]
    sql += sorted(Path("app/apps/erp/scripts/migracoes").glob("*.sql"))
    com_hora: set[str] = set()
    so_data: set[str] = set()
    for arq in sql:
        if not arq.exists():
            continue
        for linha in arq.read_text(encoding="utf-8").splitlines():
            m = re.match(r"\s*(?:ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?)?"
                         r"([a-z_]+)\s+(TIMESTAMPTZ|TIMESTAMP|DATE)\b",
                         linha.strip(), re.I)
            if not m:
                continue
            nome, tipo = m.group(1).lower(), m.group(2).upper()
            (com_hora if tipo.startswith("TIMESTAMP") else so_data).add(nome)
    # Nome que aparece como DATE em alguma tabela é ambíguo: a tela pode estar
    # mostrando a coluna de data. Fora da trava, para não gerar alarme falso.
    #
    # ⚑ E NOME CURTO TAMBÉM SAI. Existe uma coluna chamada só `em`
    # (migração 072). Com ela na lista, a trava acusava `n.recebimento.em` da
    # tela de notas emitidas, que é um campo de DATA montado em Python e não
    # tem relação nenhuma com aquela coluna. Nome de duas letras não identifica
    # coluna; identifica coincidência.
    return {c for c in (com_hora - so_data) if len(c) > 4}


@pytest.mark.parametrize("tela", TELAS + SOLTAS, ids=lambda p: p.name)
def test_campo_de_momento_nao_vai_para_o_ajudante_de_data(tela: Path):
    codigo = "\n".join(_blocos(tela))
    com_hora = _colunas_com_hora()
    assert com_hora, "não li coluna de TIMESTAMP nenhuma — a trava perdeu a "\
                     "referência e precisa ser consertada, não ignorada"
    errados = sorted({
        achado for achado in re.findall(r"dataBR\(\s*([\w.]+)\s*\)", codigo)
        if achado.split(".")[-1].lower() in com_hora})
    assert not errados, (
        f"{tela.name} passa {errados} para `dataBR`, e o nome diz que é "
        f"TIMESTAMP. `dataBR` parte no hífen e inverte: com hora junto, sai "
        f"\"08 17:50:45+00:00/11/2026\" na tela. Use `momentoBR`.")
