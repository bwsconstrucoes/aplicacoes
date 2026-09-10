# ============================================================================
# ERP — core/arquivo/leitura.py
# A IA lê o documento da empresa e sugere onde ele vai.
#
# O PEDIDO, nas palavras do dono: "um ambiente onde eu pudesse simplesmente
# jogar esse documento, ele fosse interpretado, lido, e a partir dali
# categorizado, renomeado e salvo".
#
# O QUE ESTE MÓDULO FAZ E O QUE NÃO FAZ
#
#   * Faz: pergunta à IA que documento é aquele, de quem é, quando foi emitido
#     e até quando vale — e traduz a resposta para os números que o cadastro
#     usa (id da empresa, da obra, da pessoa, do fornecedor).
#   * NÃO faz: guardar. Quem guarda é `service.arquivar`, e só depois de a
#     pessoa confirmar. Leitura automática é SUGESTÃO, não decisão: um
#     documento arquivado no tipo errado some do bloco que o cliente pede, e
#     ninguém descobre até o dia da entrega.
#
# A pergunta que a IA responde é montada a partir do CATÁLOGO QUE ESTÁ NO
# BANCO, não de uma lista fixa aqui. O catálogo é editável pela empresa: tipo
# novo criado hoje já entra na leitura de amanhã, sem mexer em código.
# ============================================================================
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.arquivo import nomes
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Colaborador, Empresa, Fornecedor, Obra
from app.apps.erp.db.models.financeiro import DocumentoTipo

logger = logging.getLogger(__name__)

# Quantos tipos cabem na pergunta. O catálogo tem 59; se a empresa criar
# centenas, mandar todos encareceria cada leitura sem melhorar o acerto.
MAX_TIPOS_NA_PERGUNTA = 120


def _texto_simples(t: str) -> str:
    """Sem acento, sem pontuação e em minúsculas — para comparar nomes."""
    t = unicodedata.normalize("NFKD", t or "")
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", t.lower()).strip()


def _so_digitos(t: Any) -> str:
    return re.sub(r"\D", "", str(t or ""))


def _data(v: Any) -> Optional[date]:
    """Aceita AAAA-MM-DD e DD/MM/AAAA; qualquer outra coisa vira None.

    Data mal lida é pior do que data ausente: o documento passaria a vencer
    numa data que não existe, e o aviso sairia na hora errada.
    """
    t = str(v or "").strip()
    if not t:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{2,4})$", t)
    if m:
        d, mes, a = m.groups()
        a = a if len(a) == 4 else "20" + a
        try:
            return date(int(a), int(mes), int(d))
        except ValueError:
            return None
    return None


def _competencia(v: Any) -> Optional[date]:
    """AAAA-MM vira o primeiro dia do mês, que é como a competência é guardada."""
    t = str(v or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})$", t)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            return None
    d = _data(t)
    return d.replace(day=1) if d else None


# ---------------------------------------------------------------------------
# A pergunta
# ---------------------------------------------------------------------------
def montar_instrucao(s: Session, *, extracao: str = "") -> str:
    tipos = s.scalars(select(DocumentoTipo)
                      .where(DocumentoTipo.ativo.is_(True))
                      .order_by(DocumentoTipo.grupo, DocumentoTipo.codigo)).all()
    if not tipos:
        raise ErroValidacao(
            "O catálogo de tipos de documento está vazio — instale o catálogo "
            "antes de usar a leitura automática.")
    linhas = []
    for t in tipos[:MAX_TIPOS_NA_PERGUNTA]:
        marcas = []
        if t.vence:
            marcas.append("vence")
        if t.por_competencia:
            marcas.append("é de um mês")
        linhas.append(f"- {t.codigo}: {t.nome} (pertence a {t.dono}"
                      + (f"; {', '.join(marcas)}" if marcas else "") + ")")
    catalogo_texto = "\n".join(linhas)
    return f"""Você organiza o arquivo de documentos de uma construtora brasileira (BWS Construções).
Chega um documento — pode ser digitalização torta, foto de celular ou PDF gerado por sistema.
Sua tarefa é dizer QUE documento é este, DE QUEM é e ATÉ QUANDO vale. Nada além disso.

Os tipos possíveis, com o código que você deve devolver:

{catalogo_texto}

Responda SOMENTE com JSON válido, sem markdown e sem comentários:

{{
 "tipo_codigo": "um dos códigos da lista acima, ou vazio se nenhum servir",
 "tipo_motivo": "em uma linha, o que no documento levou a esse tipo",
 "dono_especie": "EMPRESA | OBRA | PESSOA | PARCEIRO | LANCAMENTO",
 "dono_nome": "nome de quem o documento é (razão social, nome da pessoa, nome/código da obra)",
 "dono_documento": "CNPJ ou CPF do dono, só dígitos",
 "emissao": "AAAA-MM-DD, a data em que o documento foi emitido",
 "validade": "AAAA-MM-DD, até quando vale — vazio se o documento não vence",
 "competencia": "AAAA-MM quando o documento se refere a um mês (folha, guia, medição)",
 "referencia": "número que identifica este documento entre os do mesmo tipo (nº da ART, do aditivo, da medição, da apólice)",
 "resumo": "duas linhas sobre o que o documento diz",
 "confianca": "ALTA|MEDIA|BAIXA",
 "campos_ilegiveis": ["nome dos campos que você não conseguiu ler"],
 "observacoes": "o que precisa de conferência humana"
}}
{extracao}

Regras:
- Campo ausente = string vazia. Lista ausente = [].
- Datas sempre AAAA-MM-DD. Se vier 15/03/26, entenda 2026-03-15.
- CERTIDÃO: "validade" é a data de VALIDADE impressa, não a de emissão. Certidão sem
  validade impressa costuma valer 180 dias da emissão — diga isso em observacoes e
  deixe "validade" vazio, não invente.
- O DONO é de quem o documento TRATA, não quem o emitiu. Uma certidão federal da BWS
  é da EMPRESA BWS, ainda que quem emita seja a Receita Federal. Um holerite é da
  PESSOA. Um seguro de obra é da OBRA.
- A construtora (BWS e as empresas do grupo) é quase sempre o dono quando o documento
  é cadastral, fiscal ou de certidão.
- Se o documento não couber em nenhum tipo da lista, devolva "tipo_codigo" vazio e
  explique em observacoes — não force o tipo mais parecido.
- Confiança BAIXA quando o documento estiver ilegível ou o tipo for ambíguo."""


# ---------------------------------------------------------------------------
# Traduzir o que a IA disse para os números do cadastro
# ---------------------------------------------------------------------------
def _achar_empresa(s: Session, nome: str, documento: str) -> Optional[Empresa]:
    lista = s.scalars(select(Empresa)).all()
    if documento:
        for e in lista:
            if _so_digitos(e.cnpj) == documento:
                return e
    alvo = _texto_simples(nome)
    if not alvo:
        return None
    for e in lista:
        for candidato in (e.nome_fantasia, e.razao_social):
            c = _texto_simples(candidato or "")
            if c and (c == alvo or c in alvo or alvo in c):
                return e
    return None


def _achar_obra(s: Session, nome: str) -> Optional[Obra]:
    alvo = _texto_simples(nome)
    if not alvo:
        return None
    lista = s.scalars(select(Obra)).all()
    for o in lista:                       # código bate primeiro: é o que a equipe usa
        c = _texto_simples(o.codigo or "")
        if c and (c == alvo or c in alvo.split()):
            return o
    for o in lista:
        c = _texto_simples(o.nome or "")
        if c and (c == alvo or c in alvo or alvo in c):
            return o
    return None


def _achar_pessoa(s: Session, nome: str, documento: str) -> Optional[Colaborador]:
    lista = s.scalars(select(Colaborador)).all()
    if documento:
        for c in lista:
            if _so_digitos(c.cpf) == documento:
                return c
    alvo = _texto_simples(nome)
    if not alvo:
        return None
    for c in lista:
        n = _texto_simples(c.nome or "")
        if n and (n == alvo or n in alvo or alvo in n):
            return c
    return None


def _achar_parceiro(s: Session, nome: str, documento: str) -> Optional[Fornecedor]:
    lista = s.scalars(select(Fornecedor)).all()
    if documento:
        for f in lista:
            if _so_digitos(f.cnpj_cpf) == documento:
                return f
    alvo = _texto_simples(nome)
    if not alvo:
        return None
    for f in lista:
        for candidato in (f.nome_fantasia, f.razao_social):
            c = _texto_simples(candidato or "")
            if c and (c == alvo or c in alvo or alvo in c):
                return f
    return None


def _resolver_dono(s: Session, especie: str, nome: str, documento: str) -> dict[str, Any]:
    """Do nome lido para o registro do cadastro. Não achou é resposta válida.

    Achar o dono errado é pior do que não achar: o documento fica pendurado em
    quem não é dele e some da busca de quem procura. Por isso a comparação
    exige CNPJ/CPF igual ou nome que se contenha — nunca "o mais parecido".
    """
    especie = (especie or "").strip().upper()
    documento = _so_digitos(documento)
    achado: dict[str, Any] = {"empresa_id": None, "obra_id": None,
                              "colaborador_id": None, "fornecedor_id": None,
                              "dono_encontrado": "", "dono_especie": especie}
    if especie == "OBRA":
        o = _achar_obra(s, nome)
        if o:
            achado.update(obra_id=o.id, dono_encontrado=f"{o.codigo} · {o.nome}")
    elif especie == "PESSOA":
        c = _achar_pessoa(s, nome, documento)
        if c:
            achado.update(colaborador_id=c.id, dono_encontrado=c.nome)
    elif especie == "PARCEIRO":
        f = _achar_parceiro(s, nome, documento)
        if f:
            achado.update(fornecedor_id=f.id, dono_encontrado=f.razao_social)
    elif especie == "EMPRESA":
        e = _achar_empresa(s, nome, documento)
        if e:
            achado.update(empresa_id=e.id,
                          dono_encontrado=e.nome_fantasia or e.razao_social)
    return achado


# ---------------------------------------------------------------------------
# O que ainda falta a pessoa preencher
# ---------------------------------------------------------------------------
def _pendencias(tipo: Optional[DocumentoTipo], dono: dict[str, Any],
                validade: Optional[date], competencia: Optional[date]) -> list[str]:
    faltando: list[str] = []
    if tipo is None:
        faltando.append("o tipo do documento")
    if not any(dono.get(c) for c in ("empresa_id", "obra_id",
                                     "colaborador_id", "fornecedor_id")):
        faltando.append("de quem é o documento")
    if tipo is not None and tipo.vence and validade is None:
        faltando.append("até quando vale")
    if tipo is not None and tipo.por_competencia and competencia is None:
        faltando.append("a competência (o mês)")
    return faltando


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------
def sugerir(s: Session, conteudo: bytes, nome_arquivo: str, *,
            dica: str = "", extracao: str = "") -> dict[str, Any]:
    """Lê o documento e devolve o formulário preenchido — para conferir.

    Nada é gravado aqui. O texto extraído volta junto para ser guardado com o
    documento na hora do arquivamento: extrair na entrada é barato, e
    reprocessar depois, para poder buscar dentro do documento, seria caro.

    `extracao` acrescenta uma segunda pergunta à MESMA leitura — os campos do
    cadastro que aquele documento preenche. Vai junto e não numa chamada à
    parte porque o modelo já está com o documento na mão: perguntar duas vezes
    custaria duas leituras para responder o que cabe numa.
    """
    from app.apps.erp.core.documentos.leitor import ler_com_instrucao

    bruto = ler_com_instrucao(conteudo, nome_arquivo,
                              montar_instrucao(s, extracao=extracao), dica)

    codigo = (bruto.get("tipo_codigo") or "").strip().upper()
    tipo = s.scalars(select(DocumentoTipo)
                     .where(DocumentoTipo.codigo == codigo)).first() if codigo else None
    if tipo is not None and not tipo.ativo:
        tipo = None

    dono = _resolver_dono(s, bruto.get("dono_especie") or (tipo.dono if tipo else ""),
                          bruto.get("dono_nome") or "", bruto.get("dono_documento") or "")

    emissao = _data(bruto.get("emissao"))
    validade = _data(bruto.get("validade"))
    competencia = _competencia(bruto.get("competencia"))
    referencia = (bruto.get("referencia") or "").strip()[:60]

    # Validade que caiu ANTES da emissão é leitura trocada, não documento
    # vencido — descarta em vez de guardar um aviso que nasceria errado.
    if emissao and validade and validade < emissao:
        logger.warning("ERP/arquivo: validade (%s) antes da emissão (%s) — descartada",
                       validade, emissao)
        validade = None
        bruto["confianca"] = "BAIXA"

    nome_sugerido = ""
    if tipo is not None and dono.get("dono_encontrado"):
        nome_sugerido = nomes.montar(
            tipo_codigo=tipo.codigo, dono=dono["dono_encontrado"],
            referencia=referencia, competencia=competencia, emissao=emissao,
            validade=validade, nome_original=nome_arquivo,
            por_competencia=bool(tipo.por_competencia), vence=bool(tipo.vence))

    confianca = (bruto.get("confianca") or "").strip().upper()
    if confianca not in ("ALTA", "MEDIA", "BAIXA"):
        confianca = "MEDIA"
    ilegiveis = [str(x) for x in (bruto.get("campos_ilegiveis") or [])][:8]
    if len(ilegiveis) > 2:
        confianca = "BAIXA"

    faltando = _pendencias(tipo, dono, validade, competencia)
    if faltando:
        confianca = "BAIXA" if confianca == "ALTA" else confianca

    return {
        "tipo_codigo": tipo.codigo if tipo is not None else "",
        "tipo_nome": tipo.nome if tipo is not None else "",
        "tipo_motivo": (bruto.get("tipo_motivo") or "").strip()[:200],
        "empresa_id": dono["empresa_id"], "obra_id": dono["obra_id"],
        "colaborador_id": dono["colaborador_id"], "fornecedor_id": dono["fornecedor_id"],
        "dono_especie": dono["dono_especie"],
        "dono_lido": (bruto.get("dono_nome") or "").strip()[:120],
        "dono_encontrado": dono["dono_encontrado"],
        "emissao": emissao.isoformat() if emissao else "",
        "validade": validade.isoformat() if validade else "",
        "competencia": competencia.strftime("%Y-%m") if competencia else "",
        "referencia": referencia,
        "resumo": (bruto.get("resumo") or "").strip()[:600],
        "nome_sugerido": nome_sugerido,
        "confianca": confianca,
        "campos_ilegiveis": ilegiveis,
        "observacoes": (bruto.get("observacoes") or "").strip()[:600],
        "faltando": faltando,
        "origem_leitura": bruto.get("origem_leitura") or "",
        # O que a segunda pergunta trouxe, cru. Quem traduz para campo de
        # cadastro é o `preenchimento.py`, que sabe o que cada tipo pode
        # preencher — a leitura não decide isso.
        "dados_extraidos": bruto.get("dados_extraidos") or {},
        "aditivo": bruto.get("aditivo") or {},
        "texto": (bruto.get("texto_extraido") or "")[:200000],
        "lido_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
    }
