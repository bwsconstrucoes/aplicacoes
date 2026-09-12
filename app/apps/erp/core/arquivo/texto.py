# ============================================================================
# ERP — core/arquivo/texto.py
# O texto de dentro do documento — para PROCURAR e para PERGUNTAR depois.
#
# O PEDIDO, do dono, em 12/09/2026: *"é interessante a gente colocar essa
# possibilidade de pergunta sobre arquivos que já estão anexados (…) tem um
# contrato de uma obra e eu quero perguntar alguma coisa sobre ele"*.
#
# O BURACO QUE ISTO TAPA. O texto do documento só era guardado quando a
# LEITURA POR IA tinha rodado no arquivamento — e mesmo aí, só das SEIS
# primeiras páginas, que é o teto da leitura por IA (`core/documentos/
# leitor.py`, onde página custa dinheiro e memória). Resultado prático: um
# contrato de quarenta páginas estava 85% invisível, e documento arrastado
# para a tela sem passar pela IA não tinha texto NENHUM — para a busca, ele
# não existia.
#
# AQUI NÃO ENTRA IA, E É ESSE O PONTO. Extrair a camada de texto de um PDF é
# trabalho de biblioteca, não de modelo: não custa um centavo, não erra e não
# precisa de chave. Por isso pode rodar em TODO documento, sempre, sem pedir
# licença a ninguém e sem entrar na conta de consumo.
#
# PyMuPDF E NÃO pdfplumber, de propósito. O `leitor.py` usa pdfplumber porque
# lá o que importa é tabela e posição — e ele lê seis páginas. Aqui são
# CENTENAS de páginas, e pdfplumber leva dezenas de milissegundos por página:
# um contrato grande seguraria uma das quatro linhas de atendimento do serviço
# por vários segundos. PyMuPDF faz o mesmo em uma fração disso, e já é
# dependência do monorepo.
#
# O QUE CONTINUA DE FORA, por decisão do dono em 12/09/2026: *"não ler
# escaneados por hora"*. Documento que é FOTO ou digitalização não tem camada
# de texto; lê-lo exigiria IA olhando página por página, e isso custa por
# documento. Aqui ele devolve vazio, e quem pergunta ouve "este documento é
# uma imagem, não consigo ler o texto dele" — nunca uma resposta inventada
# sobre um documento que o sistema não leu.
# ============================================================================
from __future__ import annotations

import gc
import io
import logging
import re
from typing import Any, Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Tetos. Existem para o serviço de 2 GB continuar de pé com QUALQUER arquivo
# que alguém arraste para a tela — inclusive o projeto executivo de 600
# páginas, que ninguém previu mas vai acontecer.
MAX_PAGINAS = 300
MAX_CARACTERES = 400_000

# Extensões cujo conteúdo já É texto. XML de nota fica de fora de propósito:
# ele é dado para o sistema ler, não prosa para alguém perguntar sobre.
EXTENSOES_DE_TEXTO = (".txt", ".md", ".csv")


def _limpar(t: str) -> str:
    """Tira o excesso de espaço e de linha em branco que o PDF traz.

    Não é estética: espaço à toa infla o que é enviado à IA e atrapalha a
    conferência do trecho citado, que compara o que a IA devolveu com o que
    está escrito no documento.
    """
    t = t.replace("\x00", " ")
    t = re.sub(r"[ \t ]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def extrair(conteudo: bytes, nome_arquivo: str = "") -> str:
    """O texto de dentro do arquivo. Devolve "" quando não há o que extrair.

    Nunca levanta erro: documento ilegível é documento sem texto, e isso não
    pode derrubar o arquivamento de ninguém.
    """
    if not conteudo:
        return ""

    nome = (nome_arquivo or "").lower()
    if nome.endswith(EXTENSOES_DE_TEXTO):
        try:
            return _limpar(conteudo.decode("utf-8", errors="ignore"))[:MAX_CARACTERES]
        except Exception as e:                     # pragma: no cover - defensivo
            logger.warning("ERP/arquivo-texto: %s não decodificou (%s)", nome, e)
            return ""

    # Tudo o mais só tem texto se for PDF. Imagem (foto de contrato, JPEG de
    # comprovante) cai aqui e sai vazia — que é o combinado.
    if not conteudo[:5].startswith(b"%PDF") and not nome.endswith(".pdf"):
        return ""

    try:
        import fitz
    except ImportError:                            # pragma: no cover - ambiente
        logger.warning("ERP/arquivo-texto: PyMuPDF indisponível")
        return ""

    doc = None
    partes: list[str] = []
    tamanho = 0
    try:
        doc = fitz.open(stream=conteudo, filetype="pdf")
        for pagina in doc[:MAX_PAGINAS]:
            pedaco = pagina.get_text() or ""
            if not pedaco.strip():
                continue                            # página só de imagem
            partes.append(pedaco)
            tamanho += len(pedaco)
            if tamanho >= MAX_CARACTERES:
                break
    except Exception as e:
        logger.warning("ERP/arquivo-texto: PDF sem texto extraível (%s)", e)
        return ""
    finally:
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass
        gc.collect()

    return _limpar("\n".join(partes))[:MAX_CARACTERES]


def garantir_texto(s: Session, documento) -> str:
    """O texto do documento, extraindo AGORA se ele ainda não tiver.

    Quem chega aqui é quem vai perguntar sobre o documento. Extrair na hora
    (uns poucos décimos de segundo) é melhor que responder "ainda não
    processei, volte depois" — e o resultado fica guardado, então acontece uma
    vez só por documento.
    """
    from app.apps.erp.core.documentos.armazenamento import conteudo_de, obter

    if documento is None:
        return ""
    if (documento.texto or "").strip():
        return documento.texto

    try:
        anexo = obter(s, documento.anexo_id)
        conteudo = conteudo_de(s, anexo)
    except Exception as e:
        logger.warning("ERP/arquivo-texto: documento %s sem arquivo (%s)",
                       documento.id, e)
        return ""

    novo = extrair(conteudo, anexo.nome_arquivo or documento.nome_padronizado)
    del conteudo
    gc.collect()
    if novo:
        documento.texto = novo
        s.flush()
        logger.info("ERP/arquivo-texto: documento %s ganhou %s caracteres",
                    documento.id, len(novo))
    return novo


# ---------------------------------------------------------------------------
# OS PEDAÇOS QUE INTERESSAM À PERGUNTA
#
# Um contrato longo não cabe inteiro numa pergunta à IA — e mandar tudo custa
# proporcionalmente ao tamanho, em todo pergunta. Então o documento é partido
# em pedaços e só os que falam do assunto sobem, EM ORDEM DE DOCUMENTO.
#
# Por que isto é busca por palavra, e não por significado: é de graça, é
# previsível, e o teto de trinta mil caracteres já cobre o documento inteiro na
# esmagadora maioria dos casos — quando cobre, a escolha nem chega a acontecer.
# O índice por significado custa por documento indexado, e a hora de comprá-lo
# é quando a lista de perguntas sem resposta mostrar que faz falta.
# ---------------------------------------------------------------------------
TAMANHO_DO_PEDACO = 2_500
MAX_PARA_A_IA = 30_000

_SEM_VALOR = {
    "a", "o", "as", "os", "um", "uma", "de", "do", "da", "dos", "das", "em",
    "no", "na", "nos", "nas", "por", "para", "pra", "com", "sem", "que", "qual",
    "quais", "quanto", "quanta", "quando", "onde", "como", "e", "ou", "se",
    "ao", "aos", "à", "às", "pelo", "pela", "meu", "minha", "esse", "essa",
    "este", "esta", "isso", "isto", "ele", "ela", "eu", "me", "diz", "fala",
    "sobre", "qualquer", "tem", "há", "ser", "é", "são", "foi", "está",
}


def _palavras(t: str) -> list[str]:
    return [p for p in re.findall(r"[0-9a-zA-ZÀ-ÿ]{3,}", (t or "").lower())
            if p not in _SEM_VALOR]


def pedacos_para_a_pergunta(texto: str, pergunta: str,
                            teto: int = MAX_PARA_A_IA) -> str:
    """O documento inteiro, se couber; senão, os trechos que falam do assunto."""
    texto = (texto or "").strip()
    if not texto:
        return ""
    if len(texto) <= teto:
        return texto

    alvo = set(_palavras(pergunta))
    pedacos = [texto[i:i + TAMANHO_DO_PEDACO]
               for i in range(0, len(texto), TAMANHO_DO_PEDACO)]
    if not alvo:
        # Sem palavra aproveitável na pergunta, o começo do documento é o
        # melhor palpite — é onde moram objeto, partes, prazo e valor.
        return texto[:teto]

    notas = []
    for i, p in enumerate(pedacos):
        tem = set(_palavras(p))
        notas.append((len(alvo & tem), -i, i))
    notas.sort(reverse=True)

    escolhidos: list[int] = []
    total = 0
    for nota, _, i in notas:
        if nota == 0 and escolhidos:
            break
        if total + len(pedacos[i]) > teto:
            continue
        escolhidos.append(i)
        total += len(pedacos[i])
    if not escolhidos:
        return texto[:teto]

    escolhidos.sort()
    partes = []
    anterior: Optional[int] = None
    for i in escolhidos:
        if anterior is not None and i != anterior + 1:
            partes.append("\n[…]\n")
        partes.append(pedacos[i])
        anterior = i
    return "".join(partes)


# ---------------------------------------------------------------------------
# A CONFERÊNCIA DO TRECHO CITADO
#
# A IA devolve a resposta E os trechos de onde tirou. Antes de mostrar, o
# sistema PROCURA cada trecho dentro do documento de verdade. Trecho que não
# está lá não vai para a tela.
#
# Isto não é zelo: é a única coisa que separa "o contrato diz" de "a IA acha
# que o contrato diz". Um número inventado com cara de citação é exatamente o
# que o dono não tem como conferir — e foi o que ele pediu que nunca
# acontecesse.
# ---------------------------------------------------------------------------
def _achatar(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip().lower()


def conferir_trechos(trechos: list[str], texto: str) -> list[str]:
    """Só os trechos que estão MESMO escritos no documento."""
    plano = _achatar(texto)
    if not plano:
        return []
    bons = []
    for t in trechos or []:
        alvo = _achatar(t)
        # Trecho curto demais não prova nada: "o prazo" aparece em qualquer
        # contrato e passaria na conferência sem significar coisa alguma.
        if len(alvo) >= 25 and alvo in plano:
            bons.append(re.sub(r"\s+", " ", str(t)).strip())
    return bons
