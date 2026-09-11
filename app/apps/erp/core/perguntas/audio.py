# ============================================================================
# ERP — core/perguntas/audio.py
# Perguntar FALANDO: o áudio vira texto, e o texto segue o caminho de sempre.
#
# POR QUE ISTO EXISTE. O dono disse com todas as letras: *"eu quero poder
# trabalhar com áudio"*. E faz sentido onde o ERP mais vai ser usado — no
# celular, dentro da obra, com a mão suja e sem teclado à mão.
#
# O QUE ESTE ARQUIVO NÃO FAZ, E É O MAIS IMPORTANTE:
#
#   · ele NÃO responde nada. Devolve TEXTO. O texto cai na mesma caixa de
#     escrita da tela, passa pelo mesmo `entender()` e é respondido pelas
#     mesmas funções de código já testadas. Áudio é uma porta de entrada nova,
#     não um segundo caminho até o número.
#   · ele NÃO decide sozinho. A transcrição erra — troca "Triunfo" por
#     "triunfo", "a pagar" por "apagar". Por isso o texto aparece na tela para
#     a pessoa LER e corrigir antes de qualquer número ser calculado. Pergunta
#     mal ouvida e respondida em silêncio é exatamente o defeito que este
#     sistema inteiro tenta evitar.
#   · ele NÃO adivinha quando não entendeu. Áudio mudo ou ruído volta como
#     "não consegui entender", nunca como frase inventada.
#
# CUSTO. Transcrição se cobra por MINUTO, não por token — ver
# `ia_custo.custo_de_audio`. Sem isso a pergunta falada apareceria custando
# zero no painel de consumo, e o teto mensal deixaria de valer justamente na
# função nova.
# ============================================================================
from __future__ import annotations

import io
import logging
import os
import time as _time
from typing import Any, Optional

logger = logging.getLogger(__name__)

MODELO = os.getenv("ERP_MODELO_IA_AUDIO", "gpt-4o-mini-transcribe")

# Uma pergunta falada é curta. Estes tetos existem para o caso do dedo que
# fica preso no botão, e para o arquivo mandado de fora da tela.
MAX_BYTES = 8 * 1024 * 1024
MAX_SEGUNDOS = 120

# Formatos que o serviço de transcrição aceita. O navegador do celular grava
# em webm (Android) ou mp4/m4a (iPhone) — os dois estão aqui.
FORMATOS = ("webm", "mp4", "m4a", "mp3", "mpeg", "mpga", "wav", "ogg", "oga", "flac")

OPERACAO = "pergunta_por_audio"


class ErroAudio(Exception):
    """Áudio ilegível, longo demais, ou serviço indisponível."""


def _extensao(nome: str) -> str:
    return (nome or "").rsplit(".", 1)[-1].strip().lower() if "." in (nome or "") else ""


def _segundos(bruto: Any, tamanho: int) -> float:
    """Quanto tempo durou a gravação.

    Quem mede é o NAVEGADOR, que é quem gravou — e por isso o número é uma
    estimativa. Quando ele não vem, sobra estimar pelo tamanho do arquivo: o
    celular grava em Opus a algo perto de 32 kbps. A conta erra, e erra para
    mais no arquivo pequeno, o que é o lado seguro: o custo aparece maior do
    que foi, nunca menor.
    """
    try:
        s = float(bruto)
        if 0 < s <= MAX_SEGUNDOS * 2:
            return round(s, 2)
    except (TypeError, ValueError):
        pass
    return round(max(1.0, tamanho / 4000.0), 2)


def _cliente():
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        raise ErroAudio(
            "A pergunta por voz não está ligada neste sistema (falta a chave "
            "do serviço de transcrição). Escreva a pergunta que eu respondo "
            "igual.")
    try:
        from openai import OpenAI
    except ImportError:                      # pragma: no cover - ambiente
        raise ErroAudio("Biblioteca da OpenAI indisponível no serviço.")
    return OpenAI(api_key=chave)


def transcrever(conteudo: bytes, nome_arquivo: str, *,
                segundos: Any = None,
                usuario_id: Optional[int] = None) -> dict[str, Any]:
    """Devolve {"texto", "modelo", "segundos"}. Nunca devolve texto inventado."""
    if not conteudo:
        raise ErroAudio("Não chegou áudio nenhum.")
    if len(conteudo) > MAX_BYTES:
        raise ErroAudio(
            f"O áudio tem {len(conteudo) // (1024 * 1024)} MB e o limite é "
            f"{MAX_BYTES // (1024 * 1024)} MB. Uma pergunta cabe em poucos "
            f"segundos de fala.")

    extensao = _extensao(nome_arquivo) or "webm"
    if extensao not in FORMATOS:
        raise ErroAudio(f"Formato de áudio não suportado (.{extensao}).")

    duracao = _segundos(segundos, len(conteudo))
    if duracao > MAX_SEGUNDOS:
        raise ErroAudio(
            f"A gravação tem {int(duracao)} segundos e o limite é "
            f"{MAX_SEGUNDOS}. Pergunte uma coisa de cada vez.")

    cliente = _cliente()
    arquivo = io.BytesIO(conteudo)
    arquivo.name = f"pergunta.{extensao}"    # o serviço decide o formato pelo nome

    inicio = _time.perf_counter()
    try:
        resp = cliente.audio.transcriptions.create(
            model=MODELO, file=arquivo, language="pt",
            # A dica não conta a resposta: ela só empurra a grafia dos nomes
            # que aparecem o tempo todo aqui e que qualquer transcritor erra.
            prompt=("Pergunta ao sistema de gestão de uma construtora. "
                    "Termos comuns: obra, medição, insumo, título, aditivo, "
                    "empreita, locação, CNO, ISS, NFS-e, rateio, fundo fixo."))
    except Exception as e:
        logger.exception("ERP/áudio: falha ao transcrever")
        _registrar(duracao, _ms(inicio), usuario_id, sucesso=False, erro=str(e))
        raise ErroAudio(_recado_da_falha(e))

    _registrar(duracao, _ms(inicio), usuario_id)

    texto = (getattr(resp, "text", "") or "").strip()
    if not texto:
        raise ErroAudio(
            "Não consegui entender o áudio. Fale um pouco mais perto do "
            "aparelho, ou escreva a pergunta.")
    return {"texto": texto, "modelo": MODELO, "segundos": duracao}


def _recado_da_falha(e: Exception) -> str:
    """Erro de serviço vira frase que diz O QUE FAZER.

    Existe por um motivo concreto: **transcrever usa um modelo DIFERENTE** dos
    que o resto do ERP usa para ler documento. A mesma chave pode alcançar o
    `gpt-4o` e não alcançar o de áudio — e aí o recado cru ("model not found")
    não diz a ninguém que a saída é trocar uma variável de ambiente.
    """
    bruto = str(e)
    seco = bruto.lower()
    if any(p in seco for p in ("model", "modelo", "not found", "does not exist",
                               "404", "unsupported")):
        return (f"O serviço não reconheceu o modelo de transcrição "
                f"“{MODELO}”. A mesma chave que lê documento pode não alcançar "
                f"o modelo de áudio. Troque a variável ERP_MODELO_IA_AUDIO no "
                f"Render para “whisper-1”, que é o mais antigo e o mais aceito. "
                f"(Recado do serviço: {bruto})")
    if any(p in seco for p in ("quota", "insufficient", "billing", "429")):
        return (f"O serviço recusou por limite da conta (saldo ou cota). "
                f"Escreva a pergunta que eu respondo igual. "
                f"(Recado do serviço: {bruto})")
    if any(p in seco for p in ("api key", "unauthorized", "401", "invalid_api")):
        return (f"A chave do serviço foi recusada. Confira em Configurações › "
                f"Saúde do sistema › “O que está ligado”. "
                f"(Recado do serviço: {bruto})")
    return f"Não consegui transcrever o áudio agora: {bruto}"


def _ms(inicio: float) -> int:
    return int((_time.perf_counter() - inicio) * 1000)


def _registrar(segundos: float, duracao_ms: int, usuario_id: Optional[int],
               *, sucesso: bool = True, erro: str = "") -> None:
    """O gasto entra no MESMO painel de consumo de IA das outras funções.

    Chamada que falhou também conta: o fornecedor pode ter cobrado, e o painel
    precisa mostrar que a transcrição está quebrando.
    """
    from app.apps.erp.core.comum.ia_custo import custo_de_audio, registrar_autonomo
    try:
        registrar_autonomo(modelo=MODELO, operacao=OPERACAO,
                           duracao_ms=duracao_ms, usuario_id=usuario_id,
                           sucesso=sucesso, erro=erro,
                           custo_usd=custo_de_audio(MODELO, segundos),
                           referencia=f"{segundos:.0f}s de áudio")
    except Exception as e:                   # pragma: no cover - defesa extra
        logger.warning("ERP/áudio: consumo não registrado (%s)", e)
