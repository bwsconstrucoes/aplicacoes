"""O assistente no canto de TODA tela — e o que ele não pode virar.

O dono corrigiu o rumo em 11/09/2026: *"o perguntar que está na barra lá em
cima é ser acessado de forma geral, e não por exemplo dentro do financeiro. O
ideal é que abra um modal que fique sobre a tela no cantinho, como uma
assistente virtual mesmo."*

É também o padrão que se usa lá fora — "ambient copilot": presente em toda
tela, sempre opcional, sem tirar ninguém do que estava fazendo.

AS TRÊS COISAS QUE ESTE ARQUIVO GUARDA:

1. **Ele não cria caminho novo até o número.** Fala com as MESMAS rotas da
   tela Perguntar, que carregam a permissão e o escopo por obra de cada grupo.
   Uma rota própria "que responde tudo" teria de conferir permissão por
   dentro, e a ação declarada nela mentiria.
2. **Nenhum nome dele colide com as telas.** Ele vive em `erp_base.html`,
   carregado junto com as 20 telas do ERP: um nome repetido ou apaga a função
   da tela em silêncio, ou mata a tela inteira com erro de sintaxe.
3. **Ninguém vê SQL.** O recado técnico do servidor apareceu no cantinho, com
   o nome de todas as colunas de uma tabela. Para quem usa, isso é susto, não
   informação.
"""
from __future__ import annotations

import re
from pathlib import Path

BASE = Path("app/apps/erp/templates/erp_base.html").read_text(encoding="utf-8")
TELAS = Path("app/apps/erp/templates")


# ---------------------------------------------------------------------------
# Está em toda tela, e não dentro de um módulo
# ---------------------------------------------------------------------------
def test_o_assistente_mora_na_base_e_nao_numa_tela():
    """Na base, ele aparece nas 20 telas de uma vez. Numa tela só, ele volta
    a ser 'coisa do Financeiro' — que é exatamente o que o dono recusou."""
    assert 'id="ia-botao"' in BASE
    assert 'id="ia-painel"' in BASE


def test_perguntar_saiu_da_barra_do_financeiro():
    from app.apps.erp import routes
    rotas_do_financeiro = [
        chave for m in routes.MODULOS if m["chave"] == "financeiro"
        for chave, _, _ in m["abas"]]
    assert "perguntar" not in rotas_do_financeiro, (
        "a aba Perguntar voltou para dentro do Financeiro — o assistente "
        "alcança obras, contratos e suprimentos, e ficar numa aba de lá dá a "
        "entender que é coisa do Financeiro")


def test_a_tela_cheia_continua_existindo():
    """A tabela grande não cabe no cantinho; o ⤢ leva para a tela inteira."""
    from app.apps.erp import routes
    assert any(str(r) == "/erp/perguntar" for r in _rotas(routes))
    assert 'href="/erp/perguntar"' in BASE


def _rotas(routes):
    from flask import Flask
    a = Flask(__name__)
    a.register_blueprint(routes.bp)
    return list(a.url_map.iter_rules())


# ---------------------------------------------------------------------------
# Mesmo caminho, mesmas permissões
# ---------------------------------------------------------------------------
def test_usa_as_mesmas_rotas_dos_grupos():
    for grupo in ("financeiro", "contratos", "suprimentos", "obras"):
        assert f'"/erp/api/perguntar/{grupo}"' in BASE, (
            f"o assistente não fala com a rota do grupo {grupo}")


def test_nao_inventou_rota_propria_que_responde_tudo():
    """Uma rota só, respondendo perguntas de pesos diferentes, teria de
    conferir permissão por dentro — e a ação declarada nela mentiria."""
    # Só dentro do bloco do assistente: a base tem outros endereços próprios
    # (a exportação, por exemplo), que não são dele.
    dentro = BASE.split("function assistenteDoErp()")[1]
    endereco = re.findall(r'"(/erp/api/[a-z/\-]+)"', dentro)
    permitidos = {
        "/erp/api/perguntar/entender", "/erp/api/perguntar/ouvir",
        "/erp/api/perguntar/documento", "/erp/api/perguntar/financeiro",
        "/erp/api/perguntar/contratos", "/erp/api/perguntar/suprimentos",
        "/erp/api/perguntar/obras",
    }
    assert set(endereco) <= permitidos, (
        f"endereço novo no assistente: {set(endereco) - permitidos}")


def test_o_audio_vai_para_o_campo_e_nao_direto_para_a_resposta():
    """Transcrição erra. Pergunta mal ouvida respondida em silêncio é o pior
    defeito possível — a frase tem de passar pelos olhos de quem perguntou."""
    trecho = BASE.split("async function mandarAudio")[1].split("// ---")[0]
    assert 'document.getElementById("ia-texto").value = d.texto' in trecho
    assert "responder(" not in trecho


def test_o_anexo_avisa_que_foi_LIDO_e_nao_calculado():
    assert "li do arquivo</b>, não calculei" in BASE
    assert "Nada foi gravado" in BASE


# ---------------------------------------------------------------------------
# Não colide com nenhuma das telas
# ---------------------------------------------------------------------------
def _declarados(texto: str) -> set[str]:
    return set(re.findall(r"^(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(",
                          texto, re.M)) | set(
           re.findall(r"^(?:const|let)\s+([A-Za-z_$][\w$]*)\s*=", texto, re.M))


def test_tudo_do_assistente_comeca_com_ia():
    """A convenção é a defesa: prefixo próprio, colisão impossível de nascer."""
    dentro = BASE.split("function assistenteDoErp()")[1]
    for nome in _declarados(dentro):
        assert nome.startswith("ia") or nome[0].isupper() or nome in {
            "painel", "botao", "conversa", "ultima", "gravador", "pedacos",
            "comecou", "tela", "esc", "curto", "lembrar", "guardar",
            "desenhar", "dizer", "euDisse", "tabela", "enviar", "responder",
            "gravar", "mandarAudio", "lerDocumento", "abrir", "onde",
            "podeGravarIa", "extensaoIa",
        }, f"{nome} não tem prefixo e não está na lista de nomes internos"


def test_o_assistente_vive_dentro_de_uma_funcao_fechada():
    """Tudo dele fica dentro de uma função que se chama sozinha. Assim nada
    dele escapa para o escopo das telas — é o que torna a colisão impossível,
    e não apenas improvável."""
    assert "(function assistenteDoErp(){" in BASE
    assert "})();" in BASE.split("function assistenteDoErp")[1]


# ---------------------------------------------------------------------------
# Ninguém vê SQL
# ---------------------------------------------------------------------------
def test_o_erro_mostrado_e_curto_e_sem_sql():
    assert "function curto(mensagem)" in BASE
    assert '.split(" [SQL:")[0]' in BASE, (
        "o recado do servidor voltou a ser mostrado inteiro — já apareceu no "
        "cantinho com o nome de todas as colunas de uma tabela")


def test_toda_falha_passa_pelo_encurtador():
    assert "esc(e.message)" not in BASE, (
        "sobrou um lugar mostrando o recado técnico cru")
    assert BASE.count("curto(e.message)") >= 4


# ---------------------------------------------------------------------------
# A conversa é do navegador, não do banco
# ---------------------------------------------------------------------------
def test_a_conversa_nao_e_gravada_no_servidor():
    """Guardar pergunta e resposta seria guardar número calculado — que
    envelhece — e dado que pode ser de obra que a próxima pessoa não enxerga.
    O que o sistema registra é só que a pergunta foi feita."""
    assert "sessionStorage" in BASE
    assert "localStorage" not in BASE.split("assistenteDoErp")[1], (
        "localStorage sobrevive ao fechar o navegador: a conversa de um "
        "operador ficaria no aparelho para o próximo que sentar ali")


def test_memoria_bloqueada_nao_derruba_o_assistente():
    """Aba anônima e política de aparelho bloqueiam o armazenamento."""
    trecho = BASE.split("function guardar()")[1].split("}")[0]
    assert "try" in trecho
