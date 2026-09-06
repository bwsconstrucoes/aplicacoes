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
TELAS = sorted(p for p in TEMPLATES.glob("erp_*.html") if p != BASE)


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
