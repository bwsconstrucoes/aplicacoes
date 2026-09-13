"""O quadro "o que está ligado" — e por que ele existe.

Em 11/09/2026 o dono disse que a chave da OpenAI "já existe, talvez com um
nome um pouquinho diferente". Aí está o problema inteiro: **credencial
cadastrada com o nome errado não dá erro nenhum**. A função simplesmente não
acontece, recusa com uma frase educada, e todo mundo acha que é assim mesmo.
O Arquivo pode passar semanas sem ler documento nenhum por causa de um
sublinhado a mais, e ninguém descobre.

Este quadro responde, de dentro do ERP: o que está ligado, **sob qual nome** o
sistema procura, e — quando falta — se existe no ambiente alguma variável de
nome parecido, que é exatamente o caso de "está lá, com outro nome".

O QUE ESTE ARQUIVO GUARDA: que o VALOR nunca vaze para a tela. Nome de
variável não é segredo; o conteúdo é, e esta tela é vista por ADMIN, mas
também vai para captura de tela, para conversa de suporte e para o PDF de
exportação.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.apps.erp.core.comum import saude

TELA = Path("app/apps/erp/templates/erp_config.html").read_text(encoding="utf-8")

CHAVE_DE_MENTIRA = "sk-proj-EXEMPLO-que-nao-pode-aparecer-em-lugar-nenhum"


# ---------------------------------------------------------------------------
# O segredo não vaza
# ---------------------------------------------------------------------------
def test_o_valor_da_credencial_nunca_vai_para_a_tela(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", CHAVE_DE_MENTIRA)
    inteiro = repr(saude.integracoes())
    assert CHAVE_DE_MENTIRA not in inteiro
    # nem em pedaços grandes o suficiente para servir de pista
    assert "sk-proj-EXEMPLO" not in inteiro


def test_so_os_quatro_ultimos_aparecem_e_so_em_chave_longa(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", CHAVE_DE_MENTIRA)
    linha = _por_nome("OPENAI_API_KEY", monkeypatch)
    assert linha["final"] == "…" + CHAVE_DE_MENTIRA[-4:]
    assert len(linha["final"]) == 5


def test_credencial_curta_nao_mostra_final_nenhum(monkeypatch):
    """Quatro de oito caracteres já é meio segredo."""
    monkeypatch.setenv("ERP_AGENTE_SECRET", "curto123")
    assert _por_nome("ERP_AGENTE_SECRET", monkeypatch)["final"] == ""


def test_o_endereco_do_banco_nao_mostra_final(monkeypatch):
    """Em `DATABASE_URL` o final são as últimas letras do nome do banco: não
    ajuda a conferir nada e mostra um pedaço do endereço à toa. O final só faz
    sentido em credencial — você tem a chave na mão e compara."""
    monkeypatch.setenv("DATABASE_URL",
                       "postgresql://alguem:senha@servidor/banco_de_producao")
    assert _por_nome("DATABASE_URL", monkeypatch)["final"] == ""


# ---------------------------------------------------------------------------
# O achado que importa: está lá, com o nome trocado
# ---------------------------------------------------------------------------
def _por_nome(nome, monkeypatch):
    return {i["nome"]: i for i in saude.integracoes()}[nome]


def test_nome_parecido_no_ambiente_e_apontado(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_KEY", CHAVE_DE_MENTIRA)
    linha = _por_nome("OPENAI_API_KEY", monkeypatch)
    assert linha["configurada"] is False
    assert "OPENAI_KEY" in linha["parecidas"]


def test_o_parecido_vira_AVISO_e_nao_so_uma_linha_de_tabela(monkeypatch):
    """Numa tabela de oito linhas ninguém repara. No topo, em amarelo, sim."""
    import inspect
    fonte = inspect.getsource(saude.panorama)
    assert "nome trocado" in fonte
    assert "parecidas" in fonte


def test_so_o_NOME_da_parecida_e_mostrado_nunca_o_valor(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("CHATGPT_TOKEN", CHAVE_DE_MENTIRA)
    linha = _por_nome("OPENAI_API_KEY", monkeypatch)
    assert linha["parecidas"] == ["CHATGPT_TOKEN"]
    assert CHAVE_DE_MENTIRA not in repr(linha)


def test_credencial_configurada_nao_procura_parecida(monkeypatch):
    """Achado só interessa quando está faltando — senão vira barulho."""
    monkeypatch.setenv("OPENAI_API_KEY", CHAVE_DE_MENTIRA)
    monkeypatch.setenv("OPENAI_KEY", "outra")
    assert _por_nome("OPENAI_API_KEY", monkeypatch)["parecidas"] == []


# ---------------------------------------------------------------------------
# O quadro cobre o que o código realmente lê
# ---------------------------------------------------------------------------
def test_a_chave_da_ia_esta_no_quadro():
    """É a que motivou o quadro; sair dele seria perder o próprio motivo."""
    assert "OPENAI_API_KEY" in {i["nome"] for i in saude.integracoes()}


def test_todo_nome_do_quadro_e_lido_de_verdade_pelo_codigo():
    """Listar uma variável que ninguém lê faria o dono configurar à toa."""
    import subprocess
    # As duas formas que o repositório usa: `os.getenv("X")` e
    # `os.environ.get("X")`. Procurar só uma delas deu falso positivo na
    # primeira versão deste teste — o TELEGRAM_BOT_TOKEN é lido pela segunda.
    lidas = subprocess.run(
        ["grep", "-rhno", r'\(getenv\|environ\.get\|environ\[\)("\?[A-Z0-9_]*',
         "app/"], capture_output=True, text=True).stdout
    for nome, _, _ in saude.INTEGRACOES:
        if nome == "DATABASE_URL":
            continue          # lida por outro caminho (db/database.py)
        assert nome in lidas, (
            f"{nome} está no quadro mas nenhum código a lê — o dono ia "
            f"configurar uma variável que não serve para nada")


def test_cada_linha_diz_o_que_para_de_funcionar_sem_ela():
    """"OPENAI_API_KEY: falta" não ajuda ninguém a decidir. "O Arquivo não lê
    nota nem PDF" ajuda."""
    for i in saude.integracoes():
        assert len(i["sem_ela"]) > 15, f"{i['nome']} sem explicação real"
        assert len(i["para_que"]) > 10


def test_o_quadro_nao_precisa_de_banco(monkeypatch):
    """Ele só lê o ambiente. Se algum dia consultar o banco, vai quebrar
    justamente quando o banco for o problema — que é quando mais importa."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert saude.integracoes()


# ---------------------------------------------------------------------------
# A tela
# ---------------------------------------------------------------------------
def test_a_tela_mostra_o_quadro():
    assert 'id="sa-integracoes"' in TELA
    assert "O que está ligado" in TELA


def test_a_tela_avisa_que_nome_diferente_e_o_mesmo_que_nao_existir():
    assert "Nome diferente é o mesmo que não existir" in TELA


def test_a_tela_diz_que_o_valor_nao_aparece():
    assert "O valor nunca aparece aqui" in TELA
