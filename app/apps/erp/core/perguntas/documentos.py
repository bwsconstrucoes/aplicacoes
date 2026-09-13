# ============================================================================
# ERP — core/perguntas/documentos.py
# Perguntar sobre o que está ESCRITO nos documentos da empresa.
#
# O PEDIDO, do dono, em 11/09/2026: *"atrelar depois documentação da empresa
# para orientar o assistente/agente… temos que pensar grande"*.
#
# A DIFERENÇA PARA TUDO O QUE VEIO ANTES. As outras respostas do assistente são
# CONTAS sobre o banco: somam, filtram, comparam — e o sistema pode garantir o
# número. Aqui não há conta nenhuma. A resposta é um pedaço de texto que já
# estava escrito num contrato, num edital, numa norma. O que o sistema garante
# é outra coisa, e é o suficiente: **de qual documento saiu, e em que trecho**.
#
# É por isso que a resposta NUNCA vem sem o trecho ao lado. Decisão do dono na
# mesma conversa, e a mesma regra que o mercado aprendeu: sem a citação, é a
# IA falando — e só se pode confiar no que dá para conferir na fonte.
#
# COMO A BUSCA FUNCIONA, E POR QUE ASSIM. Busca de texto do próprio Postgres,
# com dicionário de PORTUGUÊS: ele entende que "reajustar", "reajuste" e
# "reajustado" são a mesma palavra, e ignora palavra de ligação. Decisão do
# dono: *"vamos começar do simples, depois a gente decide se parte pro caro"*.
#
#   O que isso resolve: a pergunta feita com as palavras que ESTÃO no
#   documento — que é a maioria.
#   O que isso NÃO resolve: a pergunta feita com outras palavras ("quando o
#   preço sobe?" para achar a cláusula de reajuste). Para isso existe o índice
#   por significado, que custa por documento indexado. A hora de comprar isso é
#   quando a lista de perguntas sem resposta mostrar que faz falta — não antes.
#
# ESCOPO. Passa pelo MESMO recorte da tela do Arquivo (`aplicar_escopo`), que
# é faixa de sigilo mais obra designada. Pedido do dono com todas as letras:
# *"quem vê o quê tem que estar associado às suas permissões"*. Sem isso o
# assistente viraria a porta dos fundos do controle de acesso que já existe.
# ============================================================================
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from sqlalchemy import func, literal_column, select
from sqlalchemy.orm import Session

from app.apps.erp.db.models.cadastros import Obra, Usuario
from app.apps.erp.db.models.financeiro import Documento, DocumentoTipo

logger = logging.getLogger(__name__)

# Quantos documentos entram na resposta. Poucos de propósito: o valor está em
# ler o trecho certo, não em receber trinta para garimpar.
TETO_DE_DOCUMENTOS = 5

# Tamanho do trecho mostrado, em palavras. Curto demais não dá para entender a
# frase; longo demais vira a página inteira e ninguém lê.
PALAVRAS_ANTES = 12
PALAVRAS_DEPOIS = 28

IDIOMA = "portuguese"


class SemBusca(Exception):
    """A pergunta não tem nenhuma palavra que dê para procurar."""


def _consulta(pergunta: str):
    """A pergunta vira uma busca que o Postgres entende.

    `websearch_to_tsquery` é a forma tolerante: aceita a frase escrita como
    gente escreve, aceita aspas para expressão exata, e **não explode** quando
    a pessoa digita pontuação solta — o que as outras formas fazem.
    """
    return func.websearch_to_tsquery(IDIOMA, pergunta)


def procurar(s: Session, usuario: Usuario, *, pergunta: str,
             obra: str = "", limite: int = TETO_DE_DOCUMENTOS
             ) -> list[dict[str, Any]]:
    """Os trechos dos documentos que falam disso — já dentro do escopo."""
    from app.apps.erp.core.arquivo.service import aplicar_escopo

    pergunta = (pergunta or "").strip()
    if not pergunta:
        raise SemBusca("Diga o que você quer procurar nos documentos.")

    consulta = _consulta(pergunta)
    # O trecho vem do banco, com as palavras da pergunta marcadas. Recortar
    # aqui, e não no Python, evita trazer o documento inteiro (um contrato tem
    # dezenas de páginas) só para mostrar três linhas dele.
    trecho = func.ts_headline(
        IDIOMA, func.coalesce(Documento.texto, Documento.resumo, ""), consulta,
        f"StartSel=«, StopSel=», MaxWords={PALAVRAS_ANTES + PALAVRAS_DEPOIS}, "
        f"MinWords={PALAVRAS_ANTES}, MaxFragments=2, FragmentDelimiter= … ")
    # A coluna `busca` é do BANCO (`GENERATED ALWAYS`, migração 059) e não está
    # no modelo de propósito: mapeá-la faria o SQLAlchemy tentar escrever nela
    # em todo arquivamento, e o Postgres recusaria. Aqui ela é citada direto.
    coluna_busca = literal_column("documentos.busca")
    nota = func.ts_rank_cd(coluna_busca, consulta)

    stmt = (select(Documento, DocumentoTipo, trecho, nota)
            .join(DocumentoTipo, Documento.tipo_codigo == DocumentoTipo.codigo)
            .where(coluna_busca.op("@@")(consulta))
            .order_by(nota.desc(), Documento.id.desc())
            .limit(max(1, min(int(limite or TETO_DE_DOCUMENTOS), 20))))
    stmt = aplicar_escopo(stmt, s, usuario)

    if obra:
        from app.apps.erp.core.perguntas.respostas import _casa
        ids = [o.id for o in s.scalars(select(Obra)).all()
               if _casa(obra, o.codigo) or _casa(obra, o.nome)]
        stmt = stmt.where(Documento.obra_id.in_(ids or [-1]))

    achados = []
    for documento, tipo, pedaco, pontos in s.execute(stmt):
        achados.append({
            "documento_id": documento.id,
            "nome": documento.nome_padronizado,
            "tipo": tipo.nome,
            "de_quem": _de_quem(s, documento),
            "trecho": (pedaco or "").strip(),
            "relevancia": round(float(pontos or 0), 4),
        })
    return achados


def _de_quem(s: Session, documento: Documento) -> str:
    """A obra, a empresa, a pessoa ou o fornecedor dono do documento.

    Aparece na citação porque "o contrato" não quer dizer nada: quem lê precisa
    saber de QUAL obra é o contrato que respondeu.
    """
    from app.apps.erp.db.models.cadastros import Colaborador, Empresa, Fornecedor

    if documento.obra_id:
        o = s.get(Obra, documento.obra_id)
        return f"obra {o.codigo}" if o else "obra"
    if documento.empresa_id:
        e = s.get(Empresa, documento.empresa_id)
        return (e.nome_fantasia or e.razao_social) if e else "empresa"
    if documento.colaborador_id:
        c = s.get(Colaborador, documento.colaborador_id)
        return c.nome if c else "colaborador"
    if documento.fornecedor_id:
        f = s.get(Fornecedor, documento.fornecedor_id)
        return f.razao_social if f else "fornecedor"
    return "—"


# ---------------------------------------------------------------------------
# A RESPOSTA EM UMA FRASE — e por que ela é opcional
#
# Com os trechos na mão, a IA consegue juntá-los numa frase: "o contrato prevê
# reajuste anual pelo INCC, a partir da data-base do orçamento". Isso é útil, e
# é seguro DESDE QUE duas coisas valham:
#
#   1. ela lê SÓ os trechos achados, nunca o banco inteiro;
#   2. os trechos continuam na tela, embaixo da frase.
#
# Sem as duas, vira a IA opinando sobre a empresa. Com as duas, é alguém lendo
# o contrato em voz alta — e você conferindo por cima do ombro.
#
# Sem chave de IA configurada, a resposta é só a lista de trechos. Continua
# valendo: era assim que ela nascia, e já respondia.
# ---------------------------------------------------------------------------
MODELO = os.getenv("ERP_MODELO_IA_DOCUMENTOS", "gpt-4o-mini")
OPERACAO = "pergunta_sobre_documento"

_INSTRUCAO = """Você responde perguntas sobre documentos de uma construtora brasileira (BWS Construções).

REGRAS, e elas não têm exceção:
- Responda SOMENTE com o que estiver nos trechos abaixo. Não complete com conhecimento geral, não suponha o que seria usual no mercado.
- Se os trechos não responderem, diga exatamente: "Os documentos que eu achei não respondem isso."
- Cite o documento de onde tirou cada afirmação, pelo nome, entre parênteses.
- Responda em português do Brasil, em no máximo 4 linhas, direto ao ponto.
- Não repita a pergunta e não escreva introdução."""


def resumir(trechos: list[dict[str, Any]], pergunta: str,
            usuario_id: Optional[int] = None) -> str:
    """Uma frase juntando os trechos. Devolve "" quando não dá para fazer."""
    if not trechos:
        return ""
    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        return ""
    try:
        from openai import OpenAI
    except ImportError:                        # pragma: no cover - ambiente
        return ""

    material = "\n\n".join(
        f"[{t['nome']} — {t['de_quem']}]\n{t['trecho']}" for t in trechos)
    from app.apps.erp.core.comum import ia_custo
    try:
        with ia_custo.contexto(operacao=OPERACAO, usuario_id=usuario_id,
                               referencia=pergunta[:120]):
            resp = OpenAI(api_key=chave).chat.completions.create(
                model=MODELO, temperature=0, max_tokens=400,
                messages=[{"role": "system", "content": _INSTRUCAO},
                          {"role": "user",
                           "content": f"Pergunta: {pergunta}\n\nTrechos:\n{material}"}])
            ia_custo.registrar_autonomo(modelo=MODELO, resposta=resp,
                                        operacao=OPERACAO, usuario_id=usuario_id)
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        # Não responder em uma frase é um deslize; os trechos continuam ali e
        # respondem. Derrubar a pergunta inteira por causa disso, não.
        logger.warning("ERP/documentos: resumo não saiu (%s)", e)
        return ""


# ===========================================================================
# PERGUNTAR SOBRE **UM** DOCUMENTO
#
# O PEDIDO, do dono, em 12/09/2026: *"tem um contrato de uma obra e eu quero
# perguntar alguma coisa sobre ele"*.
#
# A DIFERENÇA PARA A BUSCA ACIMA, e ela é grande. Lá a pessoa procura uma
# palavra no acervo inteiro e a IA lê só os pedacinhos que casaram — umas
# quarenta palavras. Serve para achar ONDE está escrito; não serve para
# perguntar "qual o prazo de garantia deste contrato", porque a resposta
# depende de ler o documento, não de encontrar a palavra.
#
# Aqui a pessoa APONTA o documento e a IA lê o texto dele. Nada de acervo,
# nada de banco: um documento, uma pergunta.
#
# AS TRÊS TRAVAS, e nenhuma delas é enfeite:
#
#   1. ESCOPO. Passa por `exigir_documento_no_escopo` antes de qualquer coisa.
#      Documento fora do recorte responde "não encontrado" — nunca "sem
#      permissão", que confirmaria a existência dele.
#
#   2. SÓ O QUE ESTÁ NO DOCUMENTO. A instrução proíbe completar com
#      conhecimento geral, e manda dizer que não está escrito quando não
#      estiver. O que o mercado costuma praticar não é o que ESTE contrato diz.
#
#   3. A CITAÇÃO É CONFERIDA PELO SISTEMA. A IA devolve os trechos de onde
#      tirou, e o código PROCURA cada um dentro do documento antes de mostrar.
#      Trecho que não está lá não vai para a tela, e a resposta sai marcada
#      como não conferida. Essa é a única defesa real contra número inventado
#      com cara de citação — o resto é confiança, e confiança não dá para
#      auditar.
#
# O QUE FICA DE FORA, por decisão do dono: documento ESCANEADO (foto, PDF sem
# camada de texto). Ele responde "este documento é uma imagem, não consigo ler
# o texto dele" — e é isso mesmo que tem de acontecer. Fingir que leu uma
# imagem seria o pior desfecho possível.
# ===========================================================================
MODELO_UM = os.getenv("ERP_MODELO_IA_DOCUMENTO", "gpt-4o-mini")
OPERACAO_UM = "pergunta_sobre_um_documento"

_INSTRUCAO_UM = """Você lê UM documento de uma construtora brasileira (BWS Construções) e responde uma pergunta sobre ele.

REGRAS, e elas não têm exceção:
- Responda SOMENTE com o que está escrito no documento abaixo. Nunca complete com conhecimento geral, com o que é usual no mercado ou com o que seria razoável supor.
- Se o documento não responder, responda com "achou": false e explique em uma linha o que falta.
- Todo trecho que você citar em "trechos" deve ser COPIADO LETRA POR LETRA do documento, sem reescrever, sem resumir e sem corrigir. O sistema procura cada trecho dentro do documento e descarta o que não encontrar.
- Português do Brasil, direto ao ponto, no máximo 5 linhas.
- Valores e prazos: repita exatamente como estão escritos.

Devolva SOMENTE um JSON assim:
{"achou": true, "resposta": "...", "trechos": ["...", "..."]}"""


def perguntar_sobre(s: Session, usuario: Usuario, *, documento_id: int,
                    pergunta: str) -> dict[str, Any]:
    """A resposta desta pergunta sobre ESTE documento, com a citação conferida."""
    from app.apps.erp.core.arquivo import texto as svc_texto
    from app.apps.erp.core.arquivo.service import exigir_documento_no_escopo
    from app.apps.erp.core.comum.auditoria import ErroValidacao

    pergunta = (pergunta or "").strip()
    if not pergunta:
        raise ErroValidacao("Diga o que você quer saber sobre o documento.")

    # PRIMEIRA COISA, antes de tocar no documento: quem não alcança este
    # documento na tela também não pergunta sobre ele por aqui.
    exigir_documento_no_escopo(s, usuario, documento_id)
    documento = s.get(Documento, documento_id)
    if documento is None:
        from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado
        raise ErroNaoEncontrado("Documento não encontrado.")

    nome = documento.nome_padronizado
    conteudo = svc_texto.garantir_texto(s, documento)
    if not conteudo:
        return {
            "documento_id": documento_id, "documento": nome,
            "achou": False, "conferido": False, "trechos": [],
            "resposta": (
                "Não consigo ler o texto deste documento — ele é uma imagem "
                "(foto ou digitalização), e não tem texto por dentro. Dá para "
                "abri-lo no Arquivo e ler na tela."),
        }

    chave = os.getenv("OPENAI_API_KEY", "").strip()
    if not chave:
        return {
            "documento_id": documento_id, "documento": nome,
            "achou": False, "conferido": False, "trechos": [],
            "resposta": ("A leitura por IA não está ligada neste ambiente, "
                         "então não consigo responder sobre o documento."),
        }
    try:
        from openai import OpenAI
    except ImportError:                            # pragma: no cover - ambiente
        return {
            "documento_id": documento_id, "documento": nome,
            "achou": False, "conferido": False, "trechos": [],
            "resposta": "A leitura por IA não está disponível neste ambiente.",
        }

    material = svc_texto.pedacos_para_a_pergunta(conteudo, pergunta)
    from app.apps.erp.core.comum import ia_custo
    with ia_custo.contexto(operacao=OPERACAO_UM,
                           usuario_id=getattr(usuario, "id", None),
                           referencia=f"{nome}: {pergunta}"[:120]):
        resp = OpenAI(api_key=chave).chat.completions.create(
            model=MODELO_UM, temperature=0, max_tokens=700,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": _INSTRUCAO_UM},
                      {"role": "user", "content": (
                          f"Documento: {nome}\n\n"
                          f"Pergunta: {pergunta}\n\n"
                          f"Texto do documento:\n{material}")}])
        ia_custo.registrar_autonomo(modelo=MODELO_UM, resposta=resp,
                                    operacao=OPERACAO_UM,
                                    usuario_id=getattr(usuario, "id", None))

    bruto = (resp.choices[0].message.content or "").strip()
    try:
        dados = json.loads(bruto)
    except json.JSONDecodeError:
        logger.warning("ERP/documentos: resposta fora do formato para %s", nome)
        dados = {"achou": False, "resposta": bruto[:800], "trechos": []}

    achou = bool(dados.get("achou"))
    frase = str(dados.get("resposta") or "").strip()
    pedidos = [str(t) for t in (dados.get("trechos") or []) if str(t).strip()]
    # AQUI ESTÁ A GARANTIA: só sobe para a tela o trecho que o sistema achou
    # dentro do documento. O resto some, e a resposta avisa que não foi
    # conferida.
    conferidos = svc_texto.conferir_trechos(pedidos, conteudo)

    if achou and not conferidos:
        logger.warning("ERP/documentos: %s respondeu sem trecho conferível", nome)

    return {
        "documento_id": documento_id, "documento": nome,
        "achou": achou and bool(conferidos),
        "conferido": bool(conferidos),
        "trechos": conferidos,
        "resposta": frase or "Não consegui responder com este documento.",
        "parcial": len(material) < len(conteudo),
    }
