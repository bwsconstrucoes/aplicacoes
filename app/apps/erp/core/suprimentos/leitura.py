# ============================================================================
# ERP — core/suprimentos/leitura.py
# Colar a planilha do cronograma e virar linhas da solicitação.
#
# Ao montar o cronograma, a quantidade de insumos já está tabulada em algum
# lugar. Redigitar 40 linhas é onde se perde tempo e onde entram erros de
# digitação — então a pessoa cola o texto e a IA monta as linhas.
#
# A regra que vale para toda leitura por IA neste sistema: ela SUGERE e
# CRITICA, nunca decide sozinha. Item que não casou com o cadastro vem marcado
# como não reconhecido, e quem está pedindo resolve na tela.
# ============================================================================
from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Insumo, Obra, UnidadeCompra

logger = logging.getLogger(__name__)

DICA = (
    "Isto é uma LISTA DE MATERIAIS colada de uma planilha ou de um texto. "
    "Devolva em 'itens' uma entrada por material, com: descricao (o nome do "
    "material como está escrito), quantidade (só o número), unidade (a sigla, "
    "se houver: UN, M, M2, M3, KG, SC, L…), obra (o nome ou código da obra, se "
    "aparecer) e especificacao (medida, cor, bitola ou detalhe que distinga o "
    "material — ex.: '6mm', 'cinza', 'CA50 12.5mm'). "
    "Não invente material que não esteja no texto. Se a quantidade não estiver "
    "clara, deixe vazia em vez de chutar."
)

MINIMO_PARA_SUGERIR = 0.60      # abaixo disso, é chute — melhor dizer que não achou
CONFIANCA_ALTA = 0.85


def _semelhanca(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").upper(), (b or "").upper()).ratio()


def _numero(valor: Any) -> str:
    """Aceita '1.200,50', '1200.5' ou '12 un' e devolve só o número."""
    texto = str(valor or "").strip()
    if not texto:
        return ""
    achado = re.search(r"-?\d[\d.,]*", texto)
    if not achado:
        return ""
    bruto = achado.group(0)
    if "," in bruto:                       # padrão brasileiro: 1.200,50
        bruto = bruto.replace(".", "").replace(",", ".")
    return bruto.rstrip(".")


def ler_lista(s: Session, texto: str, dica_extra: str = "") -> dict[str, Any]:
    """Transforma o texto colado em linhas prontas para conferir na tela.

    Devolve cada linha com o insumo sugerido (ou nada, quando não reconheceu),
    a obra sugerida, a unidade e a confiança — para a tela poder destacar o que
    precisa de olho humano.
    """
    texto = (texto or "").strip()
    if len(texto) < 10:
        raise ErroValidacao("Cole a lista de materiais para eu ler.")
    if len(texto) > 20000:
        raise ErroValidacao(
            "A lista é grande demais para uma vez só. Cole em partes.")

    # Passa pelo MESMO ponto de chamada das outras leituras — é lá que o
    # consumo de IA é registrado. Ler por fora sairia do painel de custo.
    from app.apps.erp.core.comum.ia_custo import contexto
    from app.apps.erp.core.documentos.leitor import ErroLeitura, _chamar_ia

    try:
        with contexto(operacao="lista_suprimentos"):
            lido = _chamar_ia(texto=texto, dica=DICA + (" " + dica_extra if dica_extra else ""))
    except ErroLeitura as e:
        raise ErroValidacao(f"Não consegui ler a lista: {e}")
    except Exception as e:                       # pragma: no cover - rede/serviço
        logger.exception("ERP/suprimentos: falha na leitura da lista")
        raise ErroValidacao(f"Não consegui ler a lista: {e}")

    # `is not False` e não `if i.ativo`: objeto recém-construído tem `ativo`
    # em branco (o padrão só existe no banco), e "em branco" não é inativo.
    insumos = [i for i in s.scalars(select(Insumo)).all()
               if getattr(i, "ativo", None) is not False]
    obras = list(s.scalars(select(Obra)).all())
    unidades = {u.codigo for u in s.scalars(select(UnidadeCompra)).all()}

    linhas, nao_reconhecidos = [], []
    for bruto in (lido.get("itens") or []):
        descricao = (bruto.get("descricao") or "").strip()
        if not descricao:
            continue
        insumo, escore = _mais_parecido(descricao, insumos,
                                        lambda i: getattr(i, "descricao", ""))
        obra, escore_obra = _mais_parecido(
            str(bruto.get("obra") or ""), obras,
            lambda o: f"{getattr(o, 'codigo', '')} {getattr(o, 'nome', '')}")

        unidade = (bruto.get("unidade") or "").strip().upper()
        if unidade not in unidades:
            unidade = (getattr(insumo, "unidade", "") or "").upper() if insumo else ""
        if unidade not in unidades:
            unidade = ""

        casou = insumo is not None and escore >= MINIMO_PARA_SUGERIR
        if not casou:
            nao_reconhecidos.append(descricao)
        linhas.append({
            "descricao_lida": descricao,
            "insumo_id": insumo.id if casou else None,
            "insumo": f"{insumo.codigo} · {insumo.descricao}" if casou else None,
            "confianca": ("ALTA" if casou and escore >= CONFIANCA_ALTA
                          else "MEDIA" if casou else "NAO_RECONHECIDO"),
            "quantidade": _numero(bruto.get("quantidade")),
            "unidade": unidade,
            "especificacao": (bruto.get("especificacao") or "").strip() or None,
            "obra_id": (obra.id if obra is not None
                        and escore_obra >= MINIMO_PARA_SUGERIR else None),
        })

    return {
        "itens": linhas,
        "nao_reconhecidos": nao_reconhecidos,
        "resumo": (f"{len(linhas)} linha(s) lidas, "
                   f"{len(nao_reconhecidos)} sem correspondência no cadastro."),
    }


def _mais_parecido(alvo: str, candidatos: list, texto_de) -> tuple[Any, float]:
    alvo = (alvo or "").strip()
    if not alvo or not candidatos:
        return None, 0.0
    melhor, escore = None, 0.0
    for c in candidatos:
        r = _semelhanca(alvo, texto_de(c))
        if r > escore:
            melhor, escore = c, r
    return melhor, escore
