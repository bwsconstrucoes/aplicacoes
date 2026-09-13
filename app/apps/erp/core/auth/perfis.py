# ============================================================================
# ERP — core/auth/perfis.py
# O cadastro de PERFIS de acesso: criar, editar, arquivar.
#
# Pedido do dono em 13/09/2026, no modelo do banco dele: *"eu cadastro usuários
# e cadastro perfil. O perfil eu digo: esse perfil tem acesso a isso, aquilo e
# aquilo outro. E o usuário está dentro daquele perfil"*.
#
# Aqui mora só a REGRA. A decisão de quem entra onde continua em
# `permissoes.py`, e o vocabulário de seções e níveis em `secoes.py`.
# ============================================================================
from __future__ import annotations

from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.core.auth import secoes as cat
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Perfil, PerfilSecao, Usuario


def _quantas_pessoas(s: Session, perfil_id: int) -> int:
    return int(s.scalar(select(func.count()).select_from(Usuario)
                        .where(Usuario.perfil_id == perfil_id)) or 0)


def _como_dicionario(s: Session, p: Perfil, com_pessoas: bool = True) -> dict:
    marcacoes = {sec.secao: sec.nivel for sec in p.secoes}
    saida = {
        "id": p.id,
        "nome": p.nome,
        "descricao": p.descricao or "",
        "de_sistema": bool(p.de_sistema),
        "ativo": bool(p.ativo),
        "secoes": marcacoes,
        # O que este perfil abre, em número — para a tela dizer "12 seções, 4
        # com edição" sem ter de reproduzir a regra no navegador.
        "quantas_secoes": len(marcacoes),
        "quantas_editam": sum(1 for n in marcacoes.values() if n == cat.EDITAR),
    }
    if com_pessoas:
        saida["pessoas"] = _quantas_pessoas(s, p.id)
    return saida


def listar(s: Session, incluir_arquivados: bool = False) -> list[dict]:
    """Os perfis cadastrados, em ordem alfabética."""
    q = select(Perfil)
    if not incluir_arquivados:
        q = q.where(Perfil.ativo.is_(True))
    perfis = list(s.scalars(q).all())
    perfis = [p for p in perfis if incluir_arquivados or p.ativo]
    perfis.sort(key=lambda p: (p.nome or "").lower())
    return [_como_dicionario(s, p) for p in perfis]


def obter(s: Session, perfil_id: int) -> dict:
    p = s.get(Perfil, perfil_id)
    if p is None:
        raise ErroNaoEncontrado("Perfil não encontrado.")
    return _como_dicionario(s, p)


def _limpar_secoes(pedido: Optional[dict]) -> dict[str, str]:
    """Valida as seções pedidas e devolve só o que vale.

    Nível fora do vocabulário ou seção que não existe é ERRO, não silêncio: um
    nome de seção digitado errado viraria um perfil que não abre a tela que a
    pessoa acha que abriu — e ninguém descobriria antes de alguém reclamar.
    """
    limpo: dict[str, str] = {}
    for chave, nivel in (pedido or {}).items():
        nivel = str(nivel or "").upper().strip()
        if nivel in ("", cat.NADA):
            continue          # NADA é o padrão: não vira linha no banco
        if chave not in cat.POR_CHAVE:
            raise ErroValidacao(f"Seção desconhecida: {chave}.")
        if nivel not in (cat.LER, cat.EDITAR):
            raise ErroValidacao(
                f"Nível inválido em {chave}: use 'só olhar' ou 'olhar e mexer'.")
        limpo[chave] = nivel
    return limpo


def _nome_valido(s: Session, nome: str, perfil_id: Optional[int] = None) -> str:
    nome = (nome or "").strip()
    if not nome:
        raise ErroValidacao("Dê um nome ao perfil.")
    if len(nome) > 80:
        raise ErroValidacao("O nome do perfil é longo demais (máximo 80 letras).")
    for outro in s.scalars(select(Perfil)).all():
        if outro.id != perfil_id and (outro.nome or "").strip().lower() == nome.lower():
            raise ErroValidacao(f"Já existe um perfil chamado “{outro.nome}”.")
    return nome


def criar(s: Session, dados: dict, autor: Usuario) -> dict:
    """Cria o perfil. Sem seções marcadas, ele não abre porta nenhuma."""
    nome = _nome_valido(s, dados.get("nome"))
    marcacoes = _limpar_secoes(dados.get("secoes"))
    p = Perfil(nome=nome, descricao=(dados.get("descricao") or "").strip() or None,
               de_sistema=False, ativo=True,
               criado_por=autor.id if autor else None)
    s.add(p)
    s.flush()
    for chave, nivel in marcacoes.items():
        s.add(PerfilSecao(perfil_id=p.id, secao=chave, nivel=nivel))
    s.flush()
    registrar_evento(s, "perfil", p.id, "PERFIL_CRIADO",
                     {"nome": nome, "secoes": marcacoes},
                     autor.id if autor else None)
    return _como_dicionario(s, p)


def editar(s: Session, perfil_id: int, dados: dict, autor: Usuario) -> dict:
    """Renomeia e regrava as seções.

    Perfil de sistema TAMBÉM é editável — ele nasceu espelhando o cargo antigo,
    e mexer nele é justamente o que o dono pediu. O que não se faz com ele é
    apagar (ver `arquivar`).
    """
    p = s.get(Perfil, perfil_id)
    if p is None:
        raise ErroNaoEncontrado("Perfil não encontrado.")

    antes = {sec.secao: sec.nivel for sec in p.secoes}
    if "nome" in dados:
        p.nome = _nome_valido(s, dados.get("nome"), perfil_id)
    if "descricao" in dados:
        p.descricao = (dados.get("descricao") or "").strip() or None

    if "secoes" in dados:
        marcacoes = _limpar_secoes(dados.get("secoes"))
        for sec in list(s.scalars(select(PerfilSecao)
                                  .where(PerfilSecao.perfil_id == perfil_id)).all()):
            if sec.perfil_id == perfil_id:
                s.delete(sec)
        s.flush()
        for chave, nivel in marcacoes.items():
            s.add(PerfilSecao(perfil_id=perfil_id, secao=chave, nivel=nivel))
        s.flush()
        s.refresh(p)
    else:
        marcacoes = antes

    registrar_evento(s, "perfil", perfil_id, "PERFIL_EDITADO",
                     {"antes": antes, "depois": marcacoes},
                     autor.id if autor else None)
    return _como_dicionario(s, p)


def arquivar(s: Session, perfil_id: int, autor: Usuario) -> dict:
    """Tira o perfil de circulação — sem apagar, e nunca com gente dentro.

    Apagar deixaria operador sem perfil nenhum, e quem não tem perfil cai na
    regra antiga do cargo: a pessoa ganharia de volta um acesso que o dono
    tinha acabado de tirar. Por isso: primeiro mude as pessoas de perfil.
    """
    p = s.get(Perfil, perfil_id)
    if p is None:
        raise ErroNaoEncontrado("Perfil não encontrado.")
    quantas = _quantas_pessoas(s, perfil_id)
    if quantas:
        raise ErroValidacao(
            f"Este perfil não pode ser arquivado: {quantas} "
            f"{'pessoa está' if quantas == 1 else 'pessoas estão'} nele. "
            "Mude essas pessoas de perfil primeiro.")
    p.ativo = False
    s.flush()
    registrar_evento(s, "perfil", perfil_id, "PERFIL_ARQUIVADO",
                     {"nome": p.nome}, autor.id if autor else None)
    return _como_dicionario(s, p)


def acoes_do_perfil(s: Session, perfil_id: int) -> set[str]:
    """As ações que este perfil concede — a mesma conta da guarda."""
    p = s.get(Perfil, perfil_id)
    if p is None:
        raise ErroNaoEncontrado("Perfil não encontrado.")
    return cat.acoes_do_perfil({sec.secao: sec.nivel for sec in p.secoes})
