# ============================================================================
# ERP — core/perguntas/entender.py
# Da frase escrita para a pergunta que o sistema sabe responder.
#
# POR QUE ESTE ARQUIVO EXISTE, E O QUE ELE NÃO FAZ.
# O dono lembrou em 11/09/2026 que o assistente **não pode ficar preso ao
# catálogo** — *"isso é só um norte"*. Ele quer escrever a pergunta, não
# escolher de uma lista. Este módulo é a primeira metade disso: recebe o texto
# e diz QUAL pergunta do catálogo ele é.
#
# ELE NÃO TOCA NO BANCO E NÃO RESPONDE NADA. É de propósito, e resolve um
# problema de permissão que seria feio de outro jeito: cada grupo de perguntas
# tem a sua ação (financeiro é aberto a todo operador, contratos não). Se uma
# rota só recebesse a frase e já devolvesse a resposta, ela teria de conferir
# permissão por dentro, pergunta a pergunta — e a ação declarada nela
# mentiria. Aqui o entendimento é operação de TEXTO: devolve a chave e o
# grupo, e quem responde continua sendo a rota daquele grupo, com a ação dela.
#
# SEM IA, POR ENQUANTO — e isso é escada, não teto. O casamento é por palavras
# (sem acento, sem plural bobo, ignorando as palavras de ligação). Quando a
# chave da OpenAI existir em produção, a IA entra EXATAMENTE aqui, escolhendo a
# mesma chave com mais jeito; o resto do sistema não muda uma linha. E o que
# nem a IA entender continua caindo no mesmo lugar honesto: "isto eu não sei".
# ============================================================================
from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional

# Palavras que não ajudam a distinguir uma pergunta da outra. Tirar isto é o
# que impede "o que tem a pagar" e "o que está vencido" de empatarem por causa
# do "o que" e do "está".
VAZIAS = {
    "a", "as", "o", "os", "um", "uma", "de", "do", "da", "dos", "das", "em",
    "no", "na", "nos", "nas", "por", "para", "pra", "pro", "com", "sem", "que",
    "qual", "quais", "quanto", "quanta", "quantos", "quantas", "como", "onde",
    "quem", "e", "ou", "me", "meu", "minha", "eu", "se", "ja", "nao", "sim",
    "esta", "estao", "ser", "sao", "tem", "ter", "vai", "foi", "esse", "essa",
    "isso", "aquele", "tal", "ai", "la", "manda", "mandar", "diz", "dizer",
    "mostra", "mostrar", "ver", "saber", "gostaria", "queria", "quero",
    "favor", "faz", "fazer", "the", "tenho", "temo", "ter", "haver", "existe",
    "relatorio", "lista", "informacao", "dado", "sistema",
}

# Abaixo disto, o sistema prefere dizer que não entendeu. Chutar a pergunta
# errada é pior que não responder: a pessoa recebe um número certo de uma
# pergunta que ela não fez, e não tem como perceber.
CONFIANCA_MINIMA = 0.34

# Quanto a melhor precisa ganhar da segunda para ser escolhida SOZINHA.
#
# Esta é a regra que o próprio dono deu, sobre outra coisa: *"talvez valesse a
# pena questionar, se não tivesse sido bem específica"*. Sem ela, "o que está
# sem nota" — que empata entre TRÊS perguntas, porque "nota" quer dizer duas
# coisas diferentes no ERP (a nota que recebemos, anexada ao título, e a nota
# que emitimos ao cliente) — seria respondida com uma delas, com ar de certeza.
# E "quantos títulos não estão conciliados", que o sistema ainda não sabe
# responder, casava pela metade com duas perguntas erradas.
#
# Empate no topo não é falha: é a hora de perguntar de volta.
MARGEM_MINIMA = 0.15


def _limpo(texto: Any) -> str:
    bruto = unicodedata.normalize("NFKD", str(texto or "").strip().lower())
    return "".join(c for c in bruto if not unicodedata.combining(c))


def _raiz(palavra: str) -> str:
    """Corta plural e alguns finais, para "insumos" casar com "insumo".

    Deliberadamente burro: não é análise de português, é o suficiente para a
    palavra do dia a dia casar. Errar para o lado de casar demais aqui é
    barato — quem decide é a confiança mínima, mais abaixo.
    """
    for fim in ("coes", "oes", "aes", "ns", "es", "s"):
        if len(palavra) > 4 and palavra.endswith(fim):
            return palavra[: -len(fim)]
    return palavra


def palavras(texto: Any) -> set[str]:
    achadas = re.findall(r"[a-z0-9]+", _limpo(texto))
    return {_raiz(p) for p in achadas if len(p) > 1 and p not in VAZIAS}


def _parecenca(da_pessoa: set[str], da_pergunta: set[str]) -> float:
    """Quanto da frase da pessoa aparece na pergunta do catálogo.

    Mede sobre a frase DELA, não sobre a do catálogo: quem escreve três
    palavras certas acertou a pergunta, mesmo que a do catálogo tenha dez.
    """
    if not da_pessoa or not da_pergunta:
        return 0.0
    return len(da_pessoa & da_pergunta) / len(da_pessoa)


def _vocabulario(pergunta: dict[str, Any]) -> set[str]:
    """Tudo que identifica uma pergunta: o enunciado, os exemplos e a chave.

    Os EXEMPLOS carregam o peso: são eles que trazem as palavras do dia a dia
    ("está atrasado", "quanto pago de aluguel") que o enunciado, mais formal,
    não tem.
    """
    junto = set()
    junto |= palavras(pergunta.get("pergunta"))
    for exemplo in pergunta.get("exemplos") or []:
        junto |= palavras(exemplo)
    junto |= palavras((pergunta.get("chave") or "").replace("_", " "))
    return junto


# Palavras que ANUNCIAM um parâmetro dentro da frase. O nome do parâmetro é a
# própria pista: quem escreve "da categoria hidráulico" ou "na obra Creche"
# está dizendo qual filtro quer, e ignorar isso obrigaria a pessoa a repetir
# na mão o que ela já escreveu.
ANUNCIAM = {
    "obra": ("obra", "canteiro"),
    "categoria": ("categoria",),
    "insumo": ("insumo", "material", "preco de", "preço de"),
}


def extrair_parametros(texto: str, pergunta: dict[str, Any]) -> dict[str, str]:
    """O que a frase já disse sobre os filtros da pergunta.

    Procura a palavra que anuncia o parâmetro e pega o que vem depois dela.
    "me manda a lista dos insumos da categoria hidráulico" tem a categoria
    escrita ali — respondê-la com o catálogo inteiro seria ignorar metade do
    que a pessoa falou.

    Deliberadamente simples e sem tocar no banco: o que ele não pegar, a
    pessoa ajusta no campo ao lado, que continua visível.
    """
    achados: dict[str, str] = {}
    cru = _limpo(texto)
    for parametro in pergunta.get("parametros") or []:
        nome = parametro.get("nome") or ""
        if parametro.get("tipo") != "texto":
            continue
        for anuncio in ANUNCIAM.get(nome, (nome,)):
            marca = _limpo(anuncio)
            pos = cru.find(marca + " ")
            if pos < 0:
                continue
            resto = cru[pos + len(marca):].strip()
            # Só o que vem logo depois, até a próxima palavra de ligação que
            # comece outra ideia ("na obra X e o que falta receber").
            palavras_depois = []
            for palavra in re.findall(r"[a-z0-9\-]+", resto):
                if palavra in {"e", "ou", "que", "de", "do", "da", "no", "na"}:
                    if palavras_depois:
                        break
                    continue
                palavras_depois.append(palavra)
                if len(palavras_depois) >= 3:
                    break
            valor = " ".join(palavras_depois).strip()
            if valor and valor not in {"tal", "x"}:
                achados[nome] = valor
            break
    return achados


def entender(texto: str, catalogo: list[dict[str, Any]]) -> dict[str, Any]:
    """Qual pergunta do catálogo é esta frase — ou a admissão de que não sei.

    Devolve sempre o mesmo formato, e `entendi=False` é uma resposta legítima,
    não um erro: é ela que evita responder com segurança a pergunta errada.
    """
    da_pessoa = palavras(texto)
    notas = []
    for pergunta in catalogo:
        nota = _parecenca(da_pessoa, _vocabulario(pergunta))
        if nota > 0:
            notas.append((nota, pergunta))
    notas.sort(key=lambda x: (-x[0], x[1]["chave"]))

    melhor = notas[0] if notas else None
    outras = [{"chave": p["chave"], "grupo": p["grupo"],
               "pergunta": p["pergunta"], "confianca": round(n, 2)}
              for n, p in notas[1:4]]

    if melhor is None or melhor[0] < CONFIANCA_MINIMA:
        return {
            "entendi": False, "ambigua": False,
            "texto": texto,
            "motivo": ("Não achei, entre as perguntas que sei responder, "
                       "nenhuma que case com isso."),
            "parecidas": [{"chave": p["chave"], "grupo": p["grupo"],
                           "pergunta": p["pergunta"], "confianca": round(n, 2)}
                          for n, p in notas[:3]],
        }

    # EMPATE NO TOPO: não se escolhe por ela. Responder uma das empatadas com
    # ar de certeza é o erro que não tem como ser percebido — o número sai
    # certo, só que de outra pergunta.
    empatadas = [(n, p) for n, p in notas if melhor[0] - n <= MARGEM_MINIMA]
    if len(empatadas) > 1:
        return {
            "entendi": False, "ambigua": True,
            "texto": texto,
            "motivo": ("Isso pode ser mais de uma coisa. Qual delas você quis "
                       "dizer?"),
            "parecidas": [{"chave": p["chave"], "grupo": p["grupo"],
                           "pergunta": p["pergunta"], "confianca": round(n, 2)}
                          for n, p in empatadas[:4]],
        }

    nota, escolhida = melhor
    return {
        "entendi": True, "ambigua": False,
        "texto": texto,
        "chave": escolhida["chave"],
        "grupo": escolhida["grupo"],
        "pergunta": escolhida["pergunta"],
        "confianca": round(nota, 2),
        # O que a frase já disse sobre os filtros — para a tela não obrigar a
        # pessoa a repetir na mão o que ela acabou de escrever.
        "parametros": extrair_parametros(texto, escolhida),
        # A segunda opção viaja junto para a tela poder oferecer "era esta?"
        # quando a diferença for pequena — sem obrigar a pessoa a reescrever.
        "parecidas": outras,
    }
