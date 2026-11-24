"""A CHAVE QUE CIFRA AS SENHAS — diagnóstico, recado e geração.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026).

Ele foi gravar a conta de e-mail de uma empresa e levou:

> *"A chave em ERP_CHAVE_SEGREDOS não tem o formato esperado. Ela é gerada uma
> única vez com Fernet.generate_key() — ver o cabeçalho de
> core/comum/segredos.py."*

Duas coisas estavam erradas, e nenhuma era a chave dele:

  1. **A tela não avisava antes.** `ha_chave()` perguntava só "a variável
     existe?". Com uma chave presente e torta, ele preenchia a conta inteira,
     digitava a senha e só então levava o recado.
  2. **O recado mandava rodar um comando de programador.** Quem opera o sistema
     não roda comando nenhum — na prática a mensagem dizia "chame alguém".

O que estes casos prendem:

  · a chave torta é reconhecida como torta, e não como ausente;
  · o diagnóstico diz O QUE há de errado (tamanho, aspas) e **nunca a chave**;
  · o recado manda para a tela que resolve, não para um terminal;
  · a chave gerada pelo sistema serve de verdade;
  · guardar e ler de volta um segredo continua funcionando.
"""
from __future__ import annotations

import pytest

from app.apps.erp.core.comum import segredos
from app.apps.erp.core.comum.auditoria import ErroValidacao

VALIDA = segredos.gerar_chave()

# ⚑ OS NOMES DOS CASOS SÃO FIXOS, e isto não é estética: a chave é SORTEADA a
# cada execução, e com `--dist loadfile` cada trabalhador do xdist sorteia a
# sua. Sem nome fixo, os casos nasciam com identificadores diferentes em cada
# processo e o pytest abortava a sessão inteira com "Different tests were
# collected between gw2 and gw0". Custou uma rodada da suíte para descobrir.
# (os identificadores vão em `ids=` em cada caso, logo abaixo)


@pytest.fixture(autouse=True)
def _esquecer():
    yield


def _com(monkeypatch, valor):
    if valor is None:
        monkeypatch.delenv(segredos.VARIAVEL, raising=False)
    else:
        monkeypatch.setenv(segredos.VARIAVEL, valor)


# ---------------------------------------------------------------------------
# 1. O diagnóstico separa "não tem" de "não serve"
# ---------------------------------------------------------------------------
def test_sem_variavel_e_ausencia_nao_e_defeito(monkeypatch):
    _com(monkeypatch, None)

    e = segredos.situacao()

    assert e["tem"] is False and e["serve"] is False
    assert "não existe" in e["problema"]
    assert segredos.ha_chave() is False


def test_chave_valida_serve(monkeypatch):
    _com(monkeypatch, VALIDA)

    e = segredos.situacao()

    assert e["tem"] is True and e["serve"] is True and e["problema"] == ""
    assert segredos.ha_chave() is True


def test_espaco_em_volta_nao_estraga_a_chave(monkeypatch):
    """Copiar e colar no Render traz espaço e quebra de linha junto. Recusar
    por isso seria recusar uma chave que está certa."""
    _com(monkeypatch, f"  {VALIDA}\n")

    assert segredos.ha_chave() is True


@pytest.mark.parametrize("quase", ['"{}"', "'{}'", "{}lixo"],
                         ids=["aspas-duplas", "aspas-simples", "lixo-no-fim"])
def test_aspas_e_lixo_no_fim_nao_derrubam_a_chave(monkeypatch, quase):
    """⚑ DESCOBERTO AO ESCREVER ESTE ARQUIVO: aspas em volta e texto colado no
    fim o cofre ENGOLE — a decodificação ignora o que não é do alfabeto e para
    na "=" do fim. A chave continua sendo a mesma.

    Fica escrito porque o diagnóstico quase mentiu: a primeira versão dizia
    "tire as aspas que resolve" para uma chave que já funcionava, o que manda
    arrumar o que não é o problema."""
    _com(monkeypatch, quase.format(VALIDA))

    assert segredos.ha_chave() is True


@pytest.mark.parametrize("torta, pedaco", [
    (VALIDA[:-4],            "faltam"),       # cortada ao copiar
    (VALIDA[:20],            "faltam"),       # metade
    ("x" * 44,               "não é uma chave gerada pelo sistema"),
    ("senha-super-secreta",  "faltam"),       # alguém inventou uma
    ("12345678",             "faltam"),
], ids=["cortada-no-fim", "metade", "tamanho-certo-mas-inventada",
        "uma-senha-qualquer", "oito-digitos"])
def test_chave_torta_e_reconhecida_como_torta_e_o_motivo_e_dito(
        monkeypatch, torta, pedaco):
    _com(monkeypatch, torta)

    e = segredos.situacao()

    assert e["tem"] is True, "a variável EXISTE — dizer que falta manda procurar o que já está lá"
    assert e["serve"] is False
    assert pedaco in e["problema"]
    assert segredos.ha_chave() is False


# ---------------------------------------------------------------------------
# 2. ⚠️ O DIAGNÓSTICO NUNCA MOSTRA A CHAVE
#
# É a regra que não pode ser quebrada por conveniência: o problema vai para a
# tela e para o log, e segredo que aparece em log deixa de ser segredo.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("valor", [VALIDA, VALIDA[:-4], VALIDA[:20], "x" * 44],
                         ids=["valida", "cortada", "metade", "inventada"])
def test_o_problema_nunca_contem_a_chave(monkeypatch, valor):
    _com(monkeypatch, valor)

    texto = (segredos.situacao()["problema"]
             + " " + segredos.recado_do_problema("A senha não foi guardada"))

    for pedaco in (valor, valor.strip('"\''), VALIDA, VALIDA[:12], VALIDA[-12:]):
        if len(pedaco) < 8:
            continue
        assert pedaco not in texto, "o diagnóstico vazou a chave"


# ---------------------------------------------------------------------------
# 3. O recado manda para a tela que resolve, não para um terminal
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("valor", [None, "x" * 44, VALIDA[:-4]],
                         ids=["sem-chave", "inventada", "cortada"])
def test_o_recado_diz_onde_resolver_e_nao_manda_rodar_comando(monkeypatch, valor):
    _com(monkeypatch, valor)

    recado = segredos.recado_do_problema("A senha não foi guardada")

    assert "Configurações" in recado and "Chave de segredos" in recado
    assert "A senha não foi guardada:" in recado
    assert "O resto do cadastro foi salvo" in recado
    for jargao in ("Fernet", "python", "generate_key", "core/comum"):
        assert jargao not in recado, f"recado para o dono não fala em {jargao!r}"


def test_gravar_com_chave_torta_explica_em_vez_de_mandar_rodar_comando(monkeypatch):
    _com(monkeypatch, "x" * 44)

    with pytest.raises(ErroValidacao) as e:
        segredos.cifrar("senha-da-conta")

    assert "não serve" in str(e.value)
    assert "Configurações" in str(e.value)
    assert "Fernet" not in str(e.value)


# ---------------------------------------------------------------------------
# 4. A chave que o sistema gera serve, e o segredo volta inteiro
# ---------------------------------------------------------------------------
def test_a_chave_gerada_pelo_sistema_serve(monkeypatch):
    nova = segredos.gerar_chave()
    _com(monkeypatch, nova)

    assert len(nova) == segredos.TAMANHO_DA_CHAVE
    assert segredos.ha_chave() is True


def test_guarda_e_le_de_volta(monkeypatch):
    _com(monkeypatch, segredos.gerar_chave())

    cifrado = segredos.cifrar("minha-senha-de-aplicativo")

    assert "minha-senha" not in cifrado, "o que vai ao banco não pode ser legível"
    assert segredos.decifrar(cifrado) == "minha-senha-de-aplicativo"


def test_trocar_a_chave_invalida_o_que_ja_estava_guardado(monkeypatch):
    """O que justifica o aviso da tela antes de gerar uma chave nova."""
    _com(monkeypatch, segredos.gerar_chave())
    cifrado = segredos.cifrar("senha-antiga")

    _com(monkeypatch, segredos.gerar_chave())

    with pytest.raises(ErroValidacao) as e:
        segredos.decifrar(cifrado)
    assert "trocada" in str(e.value)
    assert "digite a senha de novo" in str(e.value).lower()
