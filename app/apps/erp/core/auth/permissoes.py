# ============================================================================
# ERP — core/auth/permissoes.py
# Quem pode o quê, e principalmente QUEM VÊ O QUÊ.
#
# ADMIN                 tudo, inclusive configurações e plano de contas
# FINANCEIRO            opera o sistema inteiro (lança, aprova, paga, concilia),
#                       mas não mexe em configuração
# GESTOR_OBRA           lança e acompanha TODAS as obras; não paga nem configura
# SUPERVISOR_OBRA       lança e acompanha as obras designadas a ele
# ADMINISTRATIVO_OBRA   lança e acompanha o que ELE MESMO lançou
# APROVADOR / LANCADOR / CONSULTA   perfis herdados, mantidos
#
# O escopo não é enfeite de tela: ele entra na consulta, então o que está fora
# do alcance do usuário nem chega ao navegador.
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroPermissao
from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario, UsuarioObra
from app.apps.erp.db.models.financeiro import Rateio, Titulo

P = PerfilUsuario

# ação → perfis autorizados
PERMISSOES: dict[str, set[PerfilUsuario]] = {
    # Leitura geral das telas do ERP. Não é "qualquer um": é a declaração
    # consciente de que a rota é aberta a todo operador, e o que ela devolve
    # é limitado por ESCOPO DE OBJETO, não por alçada. Toda rota tem de
    # declarar alguma ação — esta existe para que "aberto a todos" seja uma
    # escolha escrita, e não o silêncio de quem esqueceu.
    "ver_erp":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL,
                        P.APROVADOR, P.LANCADOR, P.CONSULTA},
    "lancar":          {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.LANCADOR,
                        P.DEPARTAMENTO_PESSOAL},
    "avalizar":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.GESTOR_OBRA, P.SUPERVISOR_OBRA},
    "aprovar":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.APROVADOR},
    "pagar":           {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "conciliar":       {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "receber":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "reclassificar":   {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "desfazer":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "importar":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "ver_dados_pagamento": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO,
                            P.GESTOR_OBRA, P.SUPERVISOR_OBRA, P.APROVADOR},
    "configurar":      {P.ADMIN},
    "gerir_usuarios":  {P.ADMIN},
    "ver_relatorios":  {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA},
    # Pessoal: o DP revisa a despesa com colaborador depois do supervisor,
    # porque só ele conhece o cadastro e sabe se a verba é devida
    "ver_pessoal":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL},
    "lancar_dc":       {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL},
    "editar_colaboradores": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.DEPARTAMENTO_PESSOAL},
}

ROTULOS = {
    P.ADMIN: "Administrador",
    P.DIRETOR_FINANCEIRO: "Diretor financeiro",
    P.FINANCEIRO: "Administrativo financeiro",
    P.GESTOR_OBRA: "Gestor de obras (todas)",
    P.SUPERVISOR_OBRA: "Supervisor de obras (designadas)",
    P.ADMINISTRATIVO_OBRA: "Administrativo de obra",
    P.DEPARTAMENTO_PESSOAL: "Departamento pessoal",
    P.APROVADOR: "Aprovador",
    P.LANCADOR: "Lançador",
    P.CONSULTA: "Consulta",
}


def pode(usuario: Usuario, acao: str) -> bool:
    return usuario is not None and usuario.perfil in PERMISSOES.get(acao, set())


def exigir(usuario: Usuario, acao: str) -> None:
    if not pode(usuario, acao):
        raise ErroPermissao(
            f"Seu perfil ({ROTULOS.get(usuario.perfil, usuario.perfil.value)}) "
            f"não tem permissão para esta operação.")


def obras_do_usuario(s: Session, usuario: Usuario) -> Optional[list[int]]:
    """IDs das obras que o usuário enxerga. None = todas."""
    if usuario.perfil in (P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                          P.APROVADOR, P.CONSULTA):
        return None
    if usuario.perfil == P.SUPERVISOR_OBRA:
        return [o.obra_id for o in s.scalars(
            select(UsuarioObra).where(UsuarioObra.usuario_id == usuario.id)).all()]
    return None          # administrativo de obra filtra por autoria, não por obra


def aplicar_escopo(stmt: Select, s: Session, usuario: Usuario) -> Select:
    """Restringe a consulta de títulos ao que o usuário pode ver."""
    if usuario.perfil in (P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                          P.APROVADOR, P.CONSULTA):
        return stmt
    if usuario.perfil in (P.ADMINISTRATIVO_OBRA, P.LANCADOR):
        return stmt.where(Titulo.solicitante_id == usuario.id)
    if usuario.perfil == P.SUPERVISOR_OBRA:
        obras = obras_do_usuario(s, usuario) or []
        if not obras:
            # supervisor sem obra designada só enxerga o que lançou
            return stmt.where(Titulo.solicitante_id == usuario.id)
        return stmt.where(or_(
            Titulo.solicitante_id == usuario.id,
            Titulo.id.in_(select(Rateio.titulo_id).where(Rateio.obra_id.in_(obras)))))
    return stmt.where(Titulo.solicitante_id == usuario.id)


# ---------------------------------------------------------------------------
# Escopo de OBJETO
#
# Alçada responde "este perfil pode executar esta ação?". Não responde "pode
# executá-la NESTE registro?". Um supervisor tem a ação de lançar; isso não o
# autoriza a abrir o título da obra de outro. As funções abaixo respondem a
# segunda pergunta, e o fazem passando pelo MESMO `aplicar_escopo` que a
# listagem usa — de modo que detalhe e lista não têm como divergir sem que
# alguém altere os dois.
# ---------------------------------------------------------------------------
def pode_ver_titulo(s: Session, usuario: Usuario, titulo_id: int) -> bool:
    """O título existe E está dentro do escopo deste usuário?"""
    stmt = aplicar_escopo(select(Titulo.id).where(Titulo.id == titulo_id), s, usuario)
    return s.scalar(stmt) is not None


def exigir_titulo_no_escopo(s: Session, usuario: Usuario, titulo_id: int) -> None:
    """Fora do escopo responde igual a inexistente — ver ErroNaoEncontrado."""
    if not pode_ver_titulo(s, usuario, titulo_id):
        raise ErroNaoEncontrado("Título não encontrado.")


def pode_ver_obra(s: Session, usuario: Usuario, obra_id: int) -> bool:
    obras = obras_do_usuario(s, usuario)
    if obras is None:
        return True                      # perfil que enxerga todas as obras
    return int(obra_id) in set(obras)


def exigir_obra_no_escopo(s: Session, usuario: Usuario, obra_id: int) -> None:
    if not pode_ver_obra(s, usuario, obra_id):
        raise ErroNaoEncontrado("Obra não encontrada.")


def contexto_permissoes(s: Session, usuario: Usuario) -> dict[str, Any]:
    """O que a tela precisa saber para esconder o que o usuário não pode."""
    obras = obras_do_usuario(s, usuario)
    return {
        "perfil": usuario.perfil.value,
        "perfil_rotulo": ROTULOS.get(usuario.perfil, usuario.perfil.value),
        "pode": {acao: pode(usuario, acao) for acao in PERMISSOES},
        "escopo_obras": obras,
        "escopo_descricao": (
            "todas as obras" if obras is None and usuario.perfil != P.ADMINISTRATIVO_OBRA
            else f"{len(obras)} obra(s) designada(s)" if obras
            else "apenas os lançamentos que você fez"),
    }
