# -*- coding: utf-8 -*-
"""A consulta do CNPJ, e o CNPJ digitado errado — 13/09/2026.

*"Quando você dá sugestão aqui, esse CNPJ é o quê? Eu quero que você faça a
consulta via API do credor desse CNPJ."*

E o caso difícil, na mesma mensagem: *"pode ser que a pessoa digitou errado o
CNPJ. Digamos que ela foi digitar o CNPJ de uma empresa e confundiu: olhou na
nota e olhou o CNPJ da BWS, da empresa que ela trabalha, aí digitou o nome da
empresa ao invés do CNPJ ao qual a nota fazia referência."*

**Nenhuma comparação de nomes resolve isso**, e é o que torna o caso diferente
de tudo o que a tela fazia: o erro não está no nome, está no NÚMERO.

NENHUM TESTE AQUI FALA COM A INTERNET. A conversa está isolada numa função, e
tudo o mais é exercitado com a resposta dublada.
"""
from __future__ import annotations

import pytest

BWS = "10656452007869"
FORNECEDOR = "29066773000152"


# ---------------------------------------------------------------------------
# O CNPJ DIGITADO ERRADO
# ---------------------------------------------------------------------------
def test_o_CNPJ_da_PROPRIA_BWS_e_certeza_de_erro():
    """A empresa não é fornecedora de si mesma. É exatamente o engano que ele
    descreveu: copiar o CNPJ do destinatário da nota em vez do emitente."""
    from app.apps.analisesps import credores

    achado = credores.suspeita_de_cnpj_errado(
        "10.656.452/0078-69", ["ACME MATERIAIS"], nossos={BWS})
    assert achado["grau"] == "certeza"
    assert "PRÓPRIA BWS" in achado["motivo"]
    assert "corrigir o CNPJ" in achado["o_que_fazer"]


def test_CNPJ_que_a_Receita_NAO_CONHECE_e_certeza_de_erro():
    """Número que não existe foi digitado errado, ponto."""
    from app.apps.analisesps import credores

    achado = credores.suspeita_de_cnpj_errado(
        "29.066.773/0001-52", ["ACME"],
        {"erro": "CNPJ não encontrado na Receita"})
    assert achado["grau"] == "certeza"


def test_razao_social_que_NAO_PARECE_com_nome_nenhum_e_SUSPEITA():
    """Suspeita, e não veredito: nome de fantasia legítimo também não se parece
    com a razão social. Serve para olhar, não para concluir."""
    from app.apps.analisesps import credores

    achado = credores.suspeita_de_cnpj_errado(
        "29.066.773/0001-52", ["MAGNA MOTORS"],
        {"razao_social": "SERTAO CASA E CONSTRUCAO LTDA"})
    assert achado["grau"] == "suspeita"
    assert "SERTAO CASA" in achado["motivo"]
    assert "pode ser só nome de fantasia" in achado["o_que_fazer"]


def test_nome_PARECIDO_com_a_razao_social_nao_acusa_nada():
    """"SERTAO CASA E CONSTRUCAO" contra "SERTAO CASA E CONSTRUCAO LTDA" é a
    mesma empresa. Exigir igualdade acusaria metade da base — e um aviso que
    aparece sempre é um aviso que ninguém lê."""
    from app.apps.analisesps import credores

    assert credores.suspeita_de_cnpj_errado(
        "29.066.773/0001-52", ["SERTAO CASA E CONSTRUCAO"],
        {"razao_social": "SERTAO CASA E CONSTRUCAO LTDA"}) == {}


def test_o_nome_de_FANTASIA_tambem_vale_como_parecido():
    """A equipe lança pelo nome de fantasia o tempo todo. Acusar isso seria
    encher a tela de falso alarme."""
    from app.apps.analisesps import credores

    assert credores.suspeita_de_cnpj_errado(
        "29.066.773/0001-52", ["CASA FORTE"],
        {"razao_social": "COMERCIAL XYZ LTDA", "fantasia": "CASA FORTE"}) == {}


def test_sem_consulta_e_sem_ser_nosso_nao_ha_o_que_acusar():
    """A consulta é opcional. Sem ela, a tela não inventa suspeita."""
    from app.apps.analisesps import credores

    assert credores.suspeita_de_cnpj_errado(
        "29.066.773/0001-52", ["QUALQUER NOME"]) == {}


def test_CPF_nao_entra_nessa_conferencia():
    """Pessoa física não tem razão social pública para comparar."""
    from app.apps.analisesps import credores

    assert credores.suspeita_de_cnpj_errado(
        "123.456.789-09", ["JOSE"], {"razao_social": "OUTRA COISA"}) == {}


# ---------------------------------------------------------------------------
# A CONSULTA EM SI
# ---------------------------------------------------------------------------
def test_CPF_e_recusado_com_a_razao_escrita():
    from app.apps.analisesps import receita

    with pytest.raises(receita.ErroDeConsulta) as e:
        receita.consultar("123.456.789-09")
    assert "CNPJ" in str(e.value)


def test_a_consulta_NAO_acontece_ao_abrir_a_tela():
    """Varrer novecentos fornecedores ao abrir a tela é o jeito certo de ser
    bloqueado por uso excessivo — e aí a consulta para de funcionar inclusive no
    caso em que importa. Ela é sob demanda, por botão, um CNPJ por vez."""
    import inspect

    from app.apps.analisesps import credores

    fonte = inspect.getsource(credores.divergencias_da_base)
    assert "receita.guardadas" in fonte, "a tela lê o que JÁ foi perguntado"
    assert "receita.consultar" not in fonte, (
        "a tela não pode disparar consulta externa ao abrir")


def test_a_suspeita_vem_na_FRENTE_da_lista():
    """Escolher o nome certo para o CNPJ errado é trabalho jogado fora — e
    pior, é trabalho que dá ar de resolvido."""
    import inspect

    from app.apps.analisesps import credores

    fonte = inspect.getsource(credores.divergencias_da_base)
    assert 'not d.get("suspeita")' in fonte


def test_a_resposta_da_Receita_fica_GUARDADA(monkeypatch):
    """CNPJ não muda de dono. Perguntar duas vezes a mesma coisa é desperdício,
    e é o que leva ao bloqueio por uso excessivo do serviço público."""
    import inspect

    from app.apps.analisesps import receita

    fonte = inspect.getsource(receita.consultar)
    assert "if antes and not antes.get(\"erro\") and not forcar" in fonte, (
        "consulta já feita com sucesso não pode ir à rede de novo")


def test_o_404_vira_RESPOSTA_e_nao_falha():
    """"CNPJ não encontrado" é justamente o achado mais útil quando o número
    foi digitado errado. Tratar como falha faria a tela reperguntar para sempre
    e nunca mostrar a resposta que resolve o caso."""
    import inspect

    from app.apps.analisesps import receita

    fonte = inspect.getsource(receita.consultar)
    assert "e.code == 404" in fonte
    assert "CNPJ não encontrado na Receita" in fonte


def test_o_tempo_de_espera_e_CURTO():
    """É uma pessoa esperando na frente da tela. Melhor dizer "não consegui" em
    alguns segundos do que deixá-la olhando para o nada."""
    from app.apps.analisesps import receita

    assert receita.SEGUNDOS <= 10


def test_a_consulta_nao_derruba_a_tela_quando_o_servico_cai(monkeypatch):
    """*"Se ele sair do ar, esta tela continua funcionando"* — a consulta é um
    extra; nada aqui depende dela."""
    from app.apps.analisesps import receita

    monkeypatch.setattr(receita, "guardada", lambda cnpj: {})
    monkeypatch.setattr(receita, "_guardar", lambda *a, **k: None)

    def cai(*a, **k):
        raise OSError("sem rede")

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", cai)

    with pytest.raises(receita.ErroDeConsulta) as e:
        receita.consultar("29.066.773/0001-52")
    assert "não consegui falar" in str(e.value)


def test_CNPJ_comprovadamente_errado_SAI_da_pilha_do_automatico():
    """A pilha do "resolve sozinho" é aplicada em bloco, sem ninguém olhar
    linha a linha. Deixar ali um caso em que o NÚMERO está errado faria o
    sistema equalizar bonitinho o nome de um fornecedor que não é aquele —
    trabalho jogado fora, e pior, com ar de resolvido."""
    import inspect

    from app.apps.analisesps import credores

    fonte = inspect.getsource(credores.divergencias_da_base)
    assert 'd["suspeita"].get("grau") == "certeza"' in fonte
    assert 'd["automatico"] = False' in fonte


def test_a_SUSPEITA_nao_tira_da_pilha_do_automatico():
    """Ela pode ser só nome de fantasia. Barrar por suspeita encheria a pilha
    da decisão de coisa que não precisa de decisão — e aí ninguém decide nada."""
    import inspect

    from app.apps.analisesps import credores

    fonte = inspect.getsource(credores.divergencias_da_base)
    # A trava é pelo grau "certeza", e não por haver suspeita.
    assert 'if d["suspeita"]:' not in fonte
