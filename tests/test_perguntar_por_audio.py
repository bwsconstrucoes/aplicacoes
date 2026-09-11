"""Perguntar FALANDO — e as três coisas que o áudio não pode fazer.

O dono pediu com estas palavras: *"eu quero poder trabalhar com áudio"*. Faz
sentido onde o ERP mais vai ser usado: no celular, dentro da obra.

O QUE ESTE ARQUIVO PROTEGE:

1. **O áudio não responde nada.** Ele vira texto, e o texto segue o mesmo
   caminho de sempre — `entender()` e as funções de código já testadas. Se
   algum dia alguém ligar o áudio direto na resposta, terá criado um segundo
   caminho até o número, sem as travas do primeiro.
2. **A pessoa lê antes.** "A pagar" e "apagar" soam igual. A frase transcrita
   aparece na caixa de escrita para ser conferida; pergunta mal ouvida e
   respondida em silêncio é o pior defeito possível aqui.
3. **O gasto aparece.** Transcrição se cobra por MINUTO. Na conta de tokens
   ela custaria zero, e o teto mensal que o dono definiu deixaria de valer
   justamente na função nova.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from app.apps.erp.core.comum import ia_custo
from app.apps.erp.core.perguntas import audio

TELA = Path("app/apps/erp/templates/erp_perguntar.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# O custo por minuto
# ---------------------------------------------------------------------------
def test_um_minuto_de_audio_custa_o_preco_do_minuto():
    assert ia_custo.custo_de_audio("gpt-4o-mini-transcribe", 60) == Decimal("0.003")


def test_meio_minuto_custa_metade():
    assert ia_custo.custo_de_audio("gpt-4o-mini-transcribe", 30) == Decimal("0.0015")


def test_modelo_desconhecido_custa_MAIS_e_nao_zero():
    """Na dúvida o gasto aparece maior do que é. Subestimar só se descobre na
    fatura; superestimar, no painel — e aí dá para corrigir."""
    desconhecido = ia_custo.custo_de_audio("modelo-que-nao-existe", 60)
    assert desconhecido > max(ia_custo.PRECOS_POR_MINUTO.values())


def test_audio_sem_duracao_nao_vira_custo_negativo_nem_estranho():
    for valor in (None, 0, "", "abacaxi", -5):
        assert ia_custo.custo_de_audio("whisper-1", valor) == Decimal("0")


def test_o_custo_do_audio_nao_passa_pela_conta_de_tokens():
    """Se alguém apagar `custo_usd` do registro, a pergunta falada volta a
    custar zero — e ninguém percebe, porque zero não dá erro."""
    import inspect
    fonte = inspect.getsource(audio._registrar)
    assert "custo_de_audio" in fonte
    assert "custo_usd=" in fonte


# ---------------------------------------------------------------------------
# Os tetos — o dedo preso no botão, e o arquivo mandado de fora da tela
# ---------------------------------------------------------------------------
def test_audio_vazio_e_recusado():
    with pytest.raises(audio.ErroAudio):
        audio.transcrever(b"", "pergunta.webm")


def test_audio_grande_demais_e_recusado_antes_de_gastar():
    with pytest.raises(audio.ErroAudio) as e:
        audio.transcrever(b"x" * (audio.MAX_BYTES + 1), "pergunta.webm")
    assert "MB" in str(e.value)


def test_formato_estranho_e_recusado():
    with pytest.raises(audio.ErroAudio) as e:
        audio.transcrever(b"x" * 100, "pergunta.exe")
    assert "não suportado" in str(e.value)


def test_gravacao_longa_demais_e_recusada(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "")     # nem chega a tentar
    with pytest.raises(audio.ErroAudio) as e:
        audio.transcrever(b"x" * 1000, "pergunta.webm",
                          segundos=audio.MAX_SEGUNDOS + 10)
    assert "uma coisa de cada vez" in str(e.value)


def test_os_formatos_do_celular_sao_aceitos():
    """Android grava em webm; iPhone, em mp4/m4a. Faltar um deles deixaria
    metade das pessoas sem o botão funcionando."""
    for formato in ("webm", "mp4", "m4a"):
        assert formato in audio.FORMATOS


# ---------------------------------------------------------------------------
# Sem chave configurada: recusa honesta, não erro feio
# ---------------------------------------------------------------------------
def test_sem_chave_a_recusa_explica_e_oferece_o_outro_caminho(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(audio.ErroAudio) as e:
        audio.transcrever(b"x" * 100, "pergunta.webm", segundos=3)
    frase = str(e.value)
    assert "não está ligada" in frase
    assert "Escreva a pergunta" in frase, (
        "A recusa tem de dizer o que fazer em vez disso — senão a pessoa "
        "acha que o sistema quebrou.")


# ---------------------------------------------------------------------------
# A transcrição não inventa
# ---------------------------------------------------------------------------
class _RespostaVazia:
    text = "   "


class _Cliente:
    def __init__(self, resposta):
        self.audio = self
        self.transcriptions = self
        self._resposta = resposta

    def create(self, **kwargs):
        self.recebido = kwargs
        return self._resposta


def test_audio_mudo_diz_que_nao_entendeu_em_vez_de_inventar(monkeypatch):
    monkeypatch.setattr(audio, "_cliente", lambda: _Cliente(_RespostaVazia()))
    monkeypatch.setattr(audio, "_registrar", lambda *a, **k: None)
    with pytest.raises(audio.ErroAudio) as e:
        audio.transcrever(b"x" * 100, "pergunta.webm", segundos=3)
    assert "Não consegui entender" in str(e.value)


def test_a_transcricao_e_em_portugues_e_recebe_o_vocabulario_da_obra(monkeypatch):
    """Sem a dica, "medição" vira "medicão" e "CNO" vira "seno". A dica não
    responde nada — só ajuda a grafia."""
    class Resp:
        text = "  o que tem a pagar hoje  "
    cliente = _Cliente(Resp())
    monkeypatch.setattr(audio, "_cliente", lambda: cliente)
    monkeypatch.setattr(audio, "_registrar", lambda *a, **k: None)
    lido = audio.transcrever(b"x" * 100, "pergunta.webm", segundos=4)
    assert lido["texto"] == "o que tem a pagar hoje"
    assert cliente.recebido["language"] == "pt"
    for termo in ("medição", "CNO", "empreita"):
        assert termo in cliente.recebido["prompt"]


def test_a_falha_do_servico_tambem_e_registrada(monkeypatch):
    """A chamada que falhou pode ter sido cobrada, e o painel precisa mostrar
    que a transcrição está quebrando."""
    class Explode(_Cliente):
        def create(self, **kwargs):
            raise RuntimeError("serviço fora")

    registros = []
    monkeypatch.setattr(audio, "_cliente", lambda: Explode(None))
    monkeypatch.setattr(audio, "_registrar",
                        lambda *a, **k: registros.append(k))
    with pytest.raises(audio.ErroAudio):
        audio.transcrever(b"x" * 100, "pergunta.webm", segundos=3)
    assert registros and registros[0].get("sucesso") is False


# ---------------------------------------------------------------------------
# A tela: o áudio entra pela caixa de escrita, não pela resposta
# ---------------------------------------------------------------------------
def test_a_tela_poe_a_transcricao_na_caixa_para_a_pessoa_conferir():
    assert 'els("texto-livre").value = d.texto' in TELA
    assert "Confira a frase antes de responder" in TELA


def test_a_tela_nao_liga_o_audio_direto_na_resposta():
    """O áudio chama `perguntarEscrevendo`, que só CLASSIFICA a frase. Ligar
    em `responder` puxaria o número direto do áudio, sem ninguém ler."""
    trecho = TELA.split("async function enviarAudio")[1].split("}")[0:40]
    corpo = "}".join(trecho)
    assert "perguntarEscrevendo()" in corpo
    assert "responder()" not in corpo


def test_o_botao_de_falar_so_aparece_onde_da_para_gravar():
    """Botão que não funciona é pior que botão que não existe."""
    assert 'id="btn-gravar"' in TELA and "hidden" in TELA.split('id="btn-gravar"')[1][:120]
    assert "if (podeGravar())" in TELA


def test_microfone_recusado_nao_quebra_a_tela():
    assert "Não consegui usar o microfone" in TELA


def test_o_parar_nao_le_o_gravador_ja_zerado():
    """Defeito real, achado no navegador em 11/09/2026.

    `onstop` roda DEPOIS de `stop()` retornar. A primeira versão lia a
    variável global lá dentro — e quando o trecho executava, `pararDeGravar`
    já a tinha zerado: a gravação morria com "Cannot read mimeType of null",
    calada, com a tela presa em "Gravando…" para sempre.

    A trava: dentro do `onstop` só pode aparecer a referência LOCAL.
    """
    corpo = TELA.split("gravador.onstop = () => {")[1].split("};")[0]
    assert "GRAVADOR." not in corpo, (
        "o onstop voltou a ler a variável global — ela já está zerada "
        "quando ele roda")
    assert "gravador.mimeType" in corpo


# ---------------------------------------------------------------------------
# Quando o serviço recusa, a frase diz O QUE FAZER
#
# Transcrever usa um modelo DIFERENTE dos que o resto do ERP usa para ler
# documento. A mesma chave pode alcançar o gpt-4o e não alcançar o de áudio —
# e "model not found" não diz a ninguém que a saída é trocar uma variável.
# ---------------------------------------------------------------------------
def test_modelo_nao_reconhecido_ensina_a_trocar_a_variavel():
    recado = audio._recado_da_falha(
        RuntimeError("Error code: 404 - The model `x` does not exist"))
    assert "ERP_MODELO_IA_AUDIO" in recado
    assert "whisper-1" in recado


def test_limite_da_conta_nao_manda_mexer_em_configuracao():
    """Mandar trocar o modelo quando o problema é saldo faz a pessoa perder
    tempo mexendo no que estava certo."""
    recado = audio._recado_da_falha(
        RuntimeError("insufficient_quota: You exceeded your current quota"))
    assert "ERP_MODELO_IA_AUDIO" not in recado
    assert "limite da conta" in recado


def test_chave_recusada_aponta_a_tela_que_mostra_isso():
    recado = audio._recado_da_falha(RuntimeError("401 Invalid API key"))
    assert "O que está ligado" in recado


def test_o_recado_do_servico_vem_junto_sempre():
    """Resumir é bom; esconder o original não — sem ele ninguém investiga."""
    for erro in ("404 model not found", "insufficient_quota", "401 api key",
                 "qualquer outra coisa"):
        assert erro.split()[0] in audio._recado_da_falha(RuntimeError(erro))
