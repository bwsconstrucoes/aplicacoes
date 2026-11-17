# -*- coding: utf-8 -*-
"""
ERP — SUGESTÃO DE TEXTO PARA O DIÁRIO DE OBRA.

Pedido do dono em 10/11/2026:

    *"Seria interessante se no relato a gente pudesse… eu gerei um diário e eu
    gostaria que a IA fizesse a leitura do relato, tanto dos serviços como
    não da parte da fiscalização, que a gente não pode alterar, mas outros
    dados, a parte de ocorrência, a parte dos serviços, poder sugerir um
    melhor texto. (…) correção ortográfica, um melhor português, numa
    linguagem inclusive um pouco mais de obra, mais técnica, caso esteja uma
    linguagem ruim. (…) E quando ela sugerisse, eu pudesse editar a sugestão e
    gravar."*

⚑ AS QUATRO REGRAS, e elas são o que separa isto de um gerador de texto:

1. **A OBSERVAÇÃO DA FISCALIZAÇÃO NÃO ENTRA, NUNCA.** Ele disse isso sozinho,
   e é a regra 1 do diário inteiro: o que dá valor jurídico ao documento é a
   parte que a obra NÃO controla. Uma IA que "melhora" o texto da fiscalização
   destrói exatamente isso. A lista de campos é FECHADA (`CAMPOS`), e o
   pedido de qualquer outro campo é recusado — não é configuração, é trava.

2. **NÃO INVENTA SERVIÇO.** O modelo reescreve o que está lá: corrige
   português, troca gíria por termo de obra, organiza em itens. Não acrescenta
   serviço, quantidade, medida, equipamento nem data que não estejam no texto
   original. Um diário com um serviço a mais é prova falsa.

3. **NADA É GRAVADO POR AQUI.** A função devolve a sugestão; quem grava é a
   pessoa, pelo botão de sempre, depois de editar. É o mesmo princípio do
   relato de ontem e do equipamento repetido: vem OFERECIDO.

4. **O ORIGINAL VOLTA JUNTO.** A tela mostra os dois lado a lado. Sugestão sem
   o original ao lado é troca às cegas.
"""
from __future__ import annotations

import logging
import os
import re
import time as _time
from typing import Any, Optional

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

MODELO = os.getenv("ERP_MODELO_IA", "gpt-4o-mini")

# Texto curto não precisa de IA, e cobrar por isso é desperdício.
MINIMO = 15
# Teto do que viaja. Relato de diário não passa disso; acima é colagem.
TETO = 6000

# ---------------------------------------------------------------------------
# ⚑ A LISTA FECHADA. `obs_fiscalizacao` NÃO está aqui, e a ausência é a regra.
# ---------------------------------------------------------------------------
CAMPOS = {
    "relato": {
        "rotulo": "relato diário",
        "o_que_e": "o que foi EXECUTADO na obra naquele dia",
        "jeito": ("Organize em itens numerados quando houver mais de um "
                  "serviço, um por linha. Comece cada item pelo serviço."),
    },
    "ocorrencias": {
        "rotulo": "ocorrências",
        "o_que_e": ("o que saiu do normal: visita, reunião, acidente, chuva "
                    "que parou o serviço, falta de material"),
        "jeito": ("Texto corrido e curto. Diga o fato, a hora se houver, e o "
                  "efeito no serviço."),
    },
    "historico": {
        "rotulo": "histórico dos serviços",
        "o_que_e": "como o serviço vem caminhando, em poucas linhas",
        "jeito": "Texto corrido, no máximo três frases.",
    },
    "equipamentos": {
        "rotulo": "equipamentos",
        "o_que_e": "o que estava em obra",
        "jeito": "Uma lista curta, separada por vírgula.",
    },
}

_INSTRUCAO = """Você revisa DIÁRIO DE OBRA de uma construtora brasileira.

Sua tarefa é reescrever o texto abaixo, que é {o_que_e}.

O QUE FAZER:
- Corrigir ortografia, acentuação, concordância e pontuação.
- Trocar gíria e abreviação de canteiro pelo termo técnico de construção civil
  correspondente, quando o termo certo for evidente ("pizo" → "piso",
  "intetravado" → "intertravado", "calhar" → "calha", "armação" → "armação").
- Deixar a linguagem objetiva e técnica, como se escreve em diário de obra.
- {jeito}

O QUE NUNCA FAZER:
- NÃO acrescente nenhum serviço, material, equipamento, quantidade, medida,
  local, pessoa, hora ou data que não esteja no texto original. Se o original
  diz "concretagem da laje", não vire "concretagem da laje do 2º pavimento".
- NÃO remova nenhum serviço que esteja no original.
- NÃO invente justificativa, causa ou resultado.
- NÃO escreva nada além do texto revisado: sem título, sem comentário, sem
  aspas em volta, sem "segue o texto".
- Se o texto for apenas uma palavra de situação (FOLGA, DOMINGO, FERIADO,
  RECESSO, AGUARDANDO LICENÇA e parecidos), devolva-o como está.

Texto original:
---
{texto}
---

Devolva SOMENTE o texto revisado."""


def _cliente():
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        raise ErroValidacao(
            "A sugestão de texto por IA não está ligada neste serviço "
            "(falta a chave). O texto continua podendo ser escrito à mão.")
    try:
        from openai import OpenAI
    except ImportError:                                   # pragma: no cover
        raise ErroValidacao("Biblioteca da OpenAI indisponível no serviço.")
    return OpenAI(api_key=chave)


def _limpo(texto: str) -> str:
    """Tira o que o modelo às vezes põe em volta mesmo mandado não pôr."""
    t = (texto or "").strip()
    t = re.sub(r"^```[a-z]*\s*|\s*```$", "", t, flags=re.IGNORECASE).strip()
    # Aspas em volta do texto inteiro — e só nesse caso.
    if len(t) > 1 and t[0] in "\"'“" and t[-1] in "\"'”":
        t = t[1:-1].strip()
    return t


def sugerir(campo: str, texto: str, *,
            usuario_id: Optional[int] = None) -> dict[str, Any]:
    """A sugestão de texto para UM campo do diário.

    Devolve `{"campo", "original", "sugestao", "mudou"}`. Nada é gravado.
    """
    chave = str(campo or "").strip().lower()
    if chave not in CAMPOS:
        # ⚑ Recusa por lista fechada. É aqui que a observação da fiscalização
        # fica fora do alcance da IA, e não por um `if` espalhado na tela.
        raise ErroValidacao(
            "Este campo não aceita sugestão de texto. A observação da "
            "fiscalização, em especial, nunca é alterada pelo sistema.")
    original = str(texto or "").strip()
    if len(original) < MINIMO:
        raise ErroValidacao(
            f"Escreva pelo menos {MINIMO} caracteres antes de pedir a "
            f"sugestão — com menos que isso não há o que melhorar.")
    if len(original) > TETO:
        raise ErroValidacao(
            f"O texto passa de {TETO} caracteres. Peça a sugestão por partes.")

    regra = CAMPOS[chave]
    comeco = _time.monotonic()
    resposta = None
    # ⚑ A CRIAÇÃO DO CLIENTE FICA DENTRO DO `try`. Ela também falha — chave
    # malformada, proxy fora do ar, biblioteca meio instalada — e uma falha
    # ali vazava crua, virando "erro inesperado" na tela de quem acabou de
    # escrever cinco linhas de relato. O recado tem de ser o mesmo: o texto
    # não se perdeu.
    try:
        cliente = _cliente()
        resposta = cliente.chat.completions.create(
            model=MODELO, temperature=0.2, max_tokens=1500,
            messages=[{"role": "user", "content": _INSTRUCAO.format(
                o_que_e=regra["o_que_e"], jeito=regra["jeito"],
                texto=original)}])
        sugestao = _limpo(resposta.choices[0].message.content or "")
    except ErroValidacao:
        raise
    except Exception as e:                                # noqa: BLE001
        _consumo(resposta, int((_time.monotonic() - comeco) * 1000),
                 usuario_id, sucesso=False, erro=str(e)[:200])
        logger.exception("ERP/diário: sugestão de texto falhou")
        raise ErroValidacao(
            "Não consegui pedir a sugestão agora. Tente de novo em instantes "
            "— o texto que você escreveu não se perdeu.")
    _consumo(resposta, int((_time.monotonic() - comeco) * 1000), usuario_id)

    if not sugestao:
        raise ErroValidacao("A sugestão voltou vazia. O texto segue como está.")
    return {"campo": chave, "rotulo": regra["rotulo"], "original": original,
            "sugestao": sugestao, "mudou": sugestao.strip() != original.strip()}


def _consumo(resposta: Any, duracao_ms: int, usuario_id: Optional[int],
             *, sucesso: bool = True, erro: str = "") -> None:
    """O custo fica registrado como o de qualquer outra chamada de IA.

    Nunca levanta: o registro é secundário à sugestão.
    """
    from app.apps.erp.core.comum.ia_custo import registrar_autonomo
    try:
        registrar_autonomo(modelo=MODELO, resposta=resposta,
                           duracao_ms=duracao_ms, operacao="diario_texto",
                           usuario_id=usuario_id, sucesso=sucesso, erro=erro)
    except Exception as e:                     # pragma: no cover - defesa
        logger.warning("ERP/diário: consumo da sugestão não registrado (%s)", e)
