# ============================================================================
# BWS ERP — core/cadastros/projetos.py
# O PROJETO: um conjunto de obras que se olha junto (migração 066).
#
# Pedido do dono em 13/09/2026: *"no cadastro das obras eu precisaria criar
# projetos, porque com projetos eu faço uma associação de algumas obras e
# coloco todas dentro do projeto (…) tudo que eu for visualizar em relação a
# elas — relatórios, resultados, custos — eu poder visualizar o projeto, ou
# seja, o somatório daquelas obras"*.
#
# A hierarquia, e ela vale para o sistema inteiro: **empresa › projeto › obra**.
# Obra sem projeto continua sendo o caso comum.
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Empresa, Obra, Projeto, Usuario


def _como_dicionario(s: Session, p: Projeto, *, com_obras: bool = True) -> dict[str, Any]:
    obras = list(s.scalars(select(Obra).where(Obra.projeto_id == p.id)
                           .order_by(Obra.codigo)).all())
    empresa = s.get(Empresa, p.empresa_id) if p.empresa_id else None
    saida = {
        "id": p.id, "codigo": p.codigo, "nome": p.nome,
        "descricao": p.descricao or "",
        "empresa_id": p.empresa_id,
        "empresa_nome": (empresa.razao_social if empresa else ""),
        "ativo": bool(p.ativo),
        "quantas_obras": len(obras),
    }
    if com_obras:
        # A lista NOMINAL das obras só sai no detalhe, que exige configurar. A
        # listagem é aberta a quem entra no ERP (o projeto é filtro de tela), e
        # quem é preso a uma obra não tem por que saber o nome das outras.
        saida["obras"] = [{"id": o.id, "codigo": o.codigo, "nome": o.nome}
                          for o in obras]
    return saida


def listar(s: Session, *, incluir_arquivados: bool = False) -> list[dict]:
    projetos = list(s.scalars(select(Projeto)).all())
    projetos = [p for p in projetos if incluir_arquivados or p.ativo]
    projetos.sort(key=lambda p: (p.codigo or "").lower())
    return [_como_dicionario(s, p, com_obras=False) for p in projetos]


def obter(s: Session, projeto_id: int) -> dict:
    p = s.get(Projeto, projeto_id)
    if p is None:
        raise ErroNaoEncontrado("Projeto não encontrado.")
    return _como_dicionario(s, p)


def _codigo_valido(s: Session, codigo: str, projeto_id: Optional[int] = None) -> str:
    codigo = (codigo or "").strip().upper()
    if not codigo:
        raise ErroValidacao("Dê um código ao projeto (ex.: CRECHES-2026).")
    if len(codigo) > 40:
        raise ErroValidacao("O código do projeto é longo demais (máximo 40).")
    for outro in s.scalars(select(Projeto)).all():
        if outro.id != projeto_id and (outro.codigo or "").upper() == codigo:
            raise ErroValidacao(f"Já existe um projeto com o código {codigo}.")
    return codigo


def criar(s: Session, dados: dict, autor: Optional[Usuario]) -> dict:
    codigo = _codigo_valido(s, dados.get("codigo"))
    nome = (dados.get("nome") or "").strip()
    if not nome:
        raise ErroValidacao("Dê um nome ao projeto.")
    p = Projeto(codigo=codigo, nome=nome,
                descricao=(dados.get("descricao") or "").strip() or None,
                empresa_id=int(dados["empresa_id"]) if dados.get("empresa_id") else None,
                ativo=True, criado_por=autor.id if autor else None)
    s.add(p)
    s.flush()
    if "obras" in dados:
        definir_obras(s, p.id, dados.get("obras") or [], autor)
    registrar_evento(s, "projeto", p.id, "PROJETO_CRIADO",
                     {"codigo": codigo, "nome": nome},
                     autor.id if autor else None)
    return _como_dicionario(s, p)


def editar(s: Session, projeto_id: int, dados: dict, autor: Optional[Usuario]) -> dict:
    p = s.get(Projeto, projeto_id)
    if p is None:
        raise ErroNaoEncontrado("Projeto não encontrado.")
    if "codigo" in dados:
        p.codigo = _codigo_valido(s, dados.get("codigo"), projeto_id)
    if "nome" in dados:
        nome = (dados.get("nome") or "").strip()
        if not nome:
            raise ErroValidacao("Dê um nome ao projeto.")
        p.nome = nome
    if "descricao" in dados:
        p.descricao = (dados.get("descricao") or "").strip() or None
    if "empresa_id" in dados:
        p.empresa_id = int(dados["empresa_id"]) if dados.get("empresa_id") else None
    if "obras" in dados:
        definir_obras(s, projeto_id, dados.get("obras") or [], autor)
    s.flush()
    registrar_evento(s, "projeto", projeto_id, "PROJETO_EDITADO",
                     {"codigo": p.codigo}, autor.id if autor else None)
    return _como_dicionario(s, p)


def definir_obras(s: Session, projeto_id: int, obras_ids: list,
                  autor: Optional[Usuario]) -> None:
    """Quais obras pertencem a este projeto. Um caminho só, usado pelas duas
    telas (a do projeto e a da obra), para as duas nunca divergirem.

    Obra pertence a NO MÁXIMO um projeto: pendurar aqui a tira do projeto
    anterior. Mais de um faria o somatório contar a mesma obra duas vezes.
    """
    querem = {int(x) for x in (obras_ids or [])}
    atuais = {o.id for o in s.scalars(
        select(Obra).where(Obra.projeto_id == projeto_id)).all()}
    for obra_id in atuais - querem:
        obra = s.get(Obra, obra_id)
        if obra is not None:
            obra.projeto_id = None
    for obra_id in querem - atuais:
        obra = s.get(Obra, obra_id)
        if obra is None:
            raise ErroValidacao(f"Obra {obra_id} não existe.")
        obra.projeto_id = projeto_id
    s.flush()


def arquivar(s: Session, projeto_id: int, autor: Optional[Usuario]) -> dict:
    """Tira o projeto de circulação — e recusa enquanto houver obra dentro.

    Arquivar com obra dentro mudaria, sem avisar, o alcance de quem tem o
    projeto marcado no cadastro: as obras sairiam da vista dessa pessoa. Tire
    as obras primeiro, e aí a consequência fica à vista de quem faz.
    """
    p = s.get(Projeto, projeto_id)
    if p is None:
        raise ErroNaoEncontrado("Projeto não encontrado.")
    quantas = int(s.scalar(select(func.count()).select_from(Obra)
                           .where(Obra.projeto_id == projeto_id)) or 0)
    if quantas:
        raise ErroValidacao(
            f"Este projeto não pode ser arquivado: {quantas} "
            f"{'obra está' if quantas == 1 else 'obras estão'} dentro dele. "
            "Tire as obras do projeto primeiro.")
    p.ativo = False
    s.flush()
    registrar_evento(s, "projeto", projeto_id, "PROJETO_ARQUIVADO",
                     {"codigo": p.codigo}, autor.id if autor else None)
    return _como_dicionario(s, p)
