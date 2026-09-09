# ============================================================================
# ERP — core/arquivo/nomes.py
# Como cada documento é batizado.
#
# POR QUE ISTO É UM ARQUIVO SÓ, E NÃO CÓDIGO ESPALHADO
#
# O nome do arquivo SAI do sistema: vai por e-mail, sobe em portal de
# licitação, entra em pasta compactada que o cliente abre. Se duas partes do
# ERP nomearem de jeitos diferentes, a padronização morre no primeiro mês — e
# padronização meio feita é pior que nenhuma, porque dá a impressão de ordem.
#
# O FORMATO
#
#     TIPO_DONO[_REFERENCIA]_DATA.ext
#
# REGRAS, e o motivo de cada uma:
#
#   1. Só A-Z, 0-9, hífen e sublinhado. Nada de acento, cedilha ou espaço — não
#      é preciosismo: portal de licitação e sistema de prefeitura ainda
#      engasgam com acento, e o arquivo volta corrompido ou é recusado.
#   2. Sublinhado separa CAMPOS; hífen separa palavras DENTRO de um campo.
#      Assim dá para ler de olho e dá para a máquina separar.
#   3. Tipo primeiro, porque numa pasta baixada o que agrupa é o tipo.
#   4. Data em AAAA-MM-DD (ou AAAA-MM na competência): é o único formato que
#      ordena sozinho.
#   5. Documento que vence leva a validade prefixada por `val`. Bater o olho no
#      nome e saber até quando vale é metade do problema resolvido.
#   6. Campo que não se aplica some — nunca sobra sublinhado duplo.
# ============================================================================
from __future__ import annotations

import re
import unicodedata
from datetime import date
from typing import Optional

MAX_CAMPO = 40
MAX_NOME = 150


def limpar(texto: str, *, maiusculo: bool = True) -> str:
    """Tira acento, troca o que não serve por hífen e encurta.

    "Construções Planalto" vira "CONSTRUCOES-PLANALTO"; "João da Silva" vira
    "JOAO-DA-SILVA".
    """
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[^A-Za-z0-9]+", "-", texto).strip("-")
    texto = re.sub(r"-{2,}", "-", texto)
    if maiusculo:
        texto = texto.upper()
    return texto[:MAX_CAMPO].strip("-")


def _data(d: Optional[date], *, mes: bool = False) -> str:
    if d is None:
        return ""
    return d.strftime("%Y-%m" if mes else "%Y-%m-%d")


def extensao(nome_original: str) -> str:
    """A extensão do arquivo, em minúsculas e sem surpresa.

    Vem do nome original porque é o único lugar onde ela existe de verdade —
    e limitada a letras/números para um nome malicioso não virar parte do nome
    final.
    """
    pedaco = (nome_original or "").rsplit(".", 1)
    if len(pedaco) != 2:
        return ""
    ext = re.sub(r"[^A-Za-z0-9]", "", pedaco[1]).lower()
    return ext[:8]


def montar(*, tipo_codigo: str, dono: str, referencia: str = "",
           competencia: Optional[date] = None, emissao: Optional[date] = None,
           validade: Optional[date] = None, nome_original: str = "",
           por_competencia: bool = False, vence: bool = False) -> str:
    """O nome padronizado do documento.

    A DATA que entra no nome segue uma ordem de prioridade, e ela não é
    arbitrária — é a data que a PESSOA procura:
      * documento que vence  → a validade (`val-AAAA-MM-DD`);
      * documento de competência → o mês (`AAAA-MM`);
      * o resto → a emissão.
    """
    campos = [limpar(tipo_codigo), limpar(dono)]
    if referencia:
        campos.append(limpar(referencia))

    if vence and validade is not None:
        campos.append("val-" + _data(validade))
    elif por_competencia and competencia is not None:
        campos.append(_data(competencia, mes=True))
    elif emissao is not None:
        campos.append(_data(emissao))
    elif competencia is not None:
        campos.append(_data(competencia, mes=True))
    elif validade is not None:
        campos.append("val-" + _data(validade))

    nome = "_".join(c for c in campos if c)[:MAX_NOME]
    ext = extensao(nome_original)
    return f"{nome}.{ext}" if ext else nome


def apelido_da_empresa(empresa) -> str:
    """O nome curto da empresa, para caber no nome do arquivo.

    Prefere o nome fantasia — é o que a pessoa reconhece. "BWS", não
    "BWS CONSTRUCOES E EMPREENDIMENTOS LTDA".
    """
    bruto = (getattr(empresa, "nome_fantasia", None)
             or getattr(empresa, "razao_social", "") or "")
    return limpar(bruto)


def apelido_da_obra(obra) -> str:
    """O código da obra, que já é curto e já é o que todo mundo usa."""
    return limpar(getattr(obra, "codigo", "") or getattr(obra, "nome", ""))


def apelido_da_pessoa(pessoa) -> str:
    return limpar(getattr(pessoa, "nome", ""))


def apelido_do_parceiro(parceiro) -> str:
    bruto = (getattr(parceiro, "nome_fantasia", None)
             or getattr(parceiro, "razao_social", "") or "")
    return limpar(bruto)
