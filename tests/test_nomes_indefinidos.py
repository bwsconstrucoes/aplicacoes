# ============================================================================
# ERP — a varredura de NOME QUE NÃO EXISTE
#
# POR QUE ESTE ARQUIVO EXISTE
#
# Já aconteceu quatro vezes: uma tela ou uma rota foi para produção MORTA,
# porque o código usava um nome que não existe ali — uma variável com o nome
# errado, um `Obra` sem o import, um `contrato_id` dentro de um laço cuja
# variável se chama `c`. Python só reclama disso na HORA em que a linha roda.
# Se a linha estiver dentro de uma rota que a suíte não exercita, o erro
# atravessa o commit, a suíte, a publicação — e aparece para quem estava
# tentando trabalhar.
#
# Este teste lê TODO o código Python das aplicações e pergunta uma coisa só:
# cada nome usado dentro de uma função existe em algum lugar que Python vá
# encontrar? (no próprio módulo, num import, num escopo de fora, ou nos nomes
# embutidos da linguagem). Não julga lógica, não roda nada — é a varredura
# barata que o compilador não faz sozinho.
#
# COMO FUNCIONA, para quem for mexer: `symtable` é a mesma tabela de nomes que
# o Python monta para compilar o arquivo. Ela já sabe distinguir variável
# local, parâmetro, nome de fora e nome global — inclusive dentro de laços,
# compreensões e funções aninhadas. Só perguntamos a ela quais nomes ficaram
# como "global" e conferimos se o módulo tem esse nome no topo.
# ============================================================================
from __future__ import annotations

import builtins
import symtable
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
PASTAS = [RAIZ / "app"]

# Nomes que aparecem sem estar no topo do arquivo por motivo legítimo.
CONHECIDOS = {
    "__file__", "__name__", "__doc__", "__package__", "__builtins__",
    "__spec__", "__loader__", "__path__", "__class__",
    # o `_` das traduções e do descarte, quando usado como global
    "_",
}
EMBUTIDOS = set(dir(builtins))


def _arquivos() -> list[Path]:
    saida: list[Path] = []
    for pasta in PASTAS:
        for caminho in sorted(pasta.rglob("*.py")):
            partes = set(caminho.parts)
            if "__pycache__" in partes or ".venv" in partes:
                continue
            saida.append(caminho)
    return saida


def _globais_do_modulo(tabela: symtable.SymbolTable) -> set[str]:
    """Os nomes que o módulo define no topo: imports, funções, classes, constantes."""
    return {s.get_name() for s in tabela.get_symbols()
            if s.is_assigned() or s.is_imported() or s.is_namespace()}


def _cobrar(tabela: symtable.SymbolTable, disponiveis: set[str],
            arquivo: Path, achados: list[str]) -> None:
    """Percorre função por função, cobrando cada nome usado como global."""
    for filha in tabela.get_children():
        proprios = set(disponiveis)
        # o que esta função define fica visível para as de dentro dela
        proprios |= {s.get_name() for s in filha.get_symbols()
                     if s.is_assigned() or s.is_imported() or s.is_parameter()}
        for simbolo in filha.get_symbols():
            nome = simbolo.get_name()
            if not simbolo.is_referenced() or not simbolo.is_global():
                continue
            if nome in disponiveis or nome in EMBUTIDOS or nome in CONHECIDOS:
                continue
            achados.append(
                f"{arquivo.relative_to(RAIZ)}: dentro de "
                f"{filha.get_name()}() o nome '{nome}' não existe "
                f"— nem no arquivo, nem nos imports, nem no Python")
        _cobrar(filha, proprios, arquivo, achados)


@pytest.mark.parametrize("arquivo", _arquivos(), ids=lambda p: str(p.name))
def test_nenhuma_funcao_usa_nome_que_nao_existe(arquivo: Path) -> None:
    """Nenhuma função pode citar um nome que o Python não vai achar na hora.

    Falhou? A mensagem diz o arquivo, a função e o nome. Quase sempre é uma
    das três: faltou o `import`, o nome está escrito diferente de onde foi
    criado, ou a variável do laço tem outro nome.
    """
    texto = arquivo.read_text(encoding="utf-8")
    try:
        tabela = symtable.symtable(texto, str(arquivo), "exec")
    except SyntaxError as e:                       # pragma: no cover
        pytest.fail(f"{arquivo.relative_to(RAIZ)} não compila: {e}")

    disponiveis = _globais_do_modulo(tabela) | EMBUTIDOS | CONHECIDOS
    achados: list[str] = []
    _cobrar(tabela, disponiveis, arquivo, achados)
    assert not achados, "\n".join(achados)
