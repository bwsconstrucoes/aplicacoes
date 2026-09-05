# ============================================================================
# ERP — core/suprimentos/fornecedores.py
# O fornecedor visto por Suprimentos: o que ele vende, de onde atende, por
# onde recebe cotação e com quem se fala.
#
# O cadastro básico (documento, razão social, contas bancárias) continua sendo
# de `core/cadastros/fornecedores.py` — é o mesmo fornecedor que o financeiro
# paga, e ter duas bases seria o começo do fim. Aqui só entra o que Suprimentos
# acrescentou na migração 033, mais a visão de gestão da tela.
#
# Por que "quem não tem categoria não recebe cotação": disparar a cotação de
# cimento para quem vende cabo elétrico faz o fornecedor parar de responder —
# e aí o que falta não é preço, é resposta.
# ============================================================================
from __future__ import annotations

import logging
import unicodedata
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import fornecedores as base
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorCategoria, FornecedorContato, FornecedorPorte,
    InsumoCategoria, Usuario,
)

logger = logging.getLogger(__name__)

PORTE_ROTULOS = {
    "FABRICA": "Fábrica",
    "REP_FABRICA": "Representante de fábrica",
    "DISTRIBUIDOR": "Distribuidor",
    "LOCAL": "Comércio local",
    "HOMECENTER": "Home center",
}
CANAIS = ("EMAIL", "WHATSAPP", "TELEFONE", "PORTAL", "PRESENCIAL")
CANAL_ROTULOS = {
    "EMAIL": "E-mail", "WHATSAPP": "WhatsApp", "TELEFONE": "Telefone",
    "PORTAL": "Portal do fornecedor", "PRESENCIAL": "Presencial",
}


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def _chave(texto: str) -> str:
    forma = unicodedata.normalize("NFKD", _texto(texto))
    return "".join(c for c in forma if not unicodedata.combining(c)).casefold()


def _obter(s: Session, fornecedor_id: int) -> Fornecedor:
    forn = s.get(Fornecedor, fornecedor_id, with_for_update=True,
                 populate_existing=True)
    if forn is None:
        raise ErroNaoEncontrado("Fornecedor não encontrado.")
    return forn


# ---------------------------------------------------------------------------
# Os campos que Suprimentos acrescentou
# ---------------------------------------------------------------------------
def _aplicar_campos_de_suprimentos(s: Session, forn: Fornecedor,
                                   dados: dict[str, Any]) -> dict[str, Any]:
    mudou: dict[str, Any] = {}

    if "porte" in dados:
        bruto = _texto(dados.get("porte")).upper().replace(" ", "_")
        if not bruto:
            forn.porte = None
        elif bruto in PORTE_ROTULOS:
            forn.porte = FornecedorPorte(bruto)
        else:
            raise ErroValidacao(f"Porte desconhecido: {dados.get('porte')!r}")
        mudou["porte"] = bruto or None

    if "regioes_atuacao" in dados:
        regioes = sorted({_texto(r).upper() for r in (dados.get("regioes_atuacao") or [])
                          if _texto(r)})
        forn.regioes_atuacao = regioes
        mudou["regioes_atuacao"] = regioes

    if "canais_cotacao" in dados:
        canais = sorted({_texto(c).upper() for c in (dados.get("canais_cotacao") or [])
                         if _texto(c)})
        desconhecidos = [c for c in canais if c not in CANAIS]
        if desconhecidos:
            raise ErroValidacao(
                f"Canal de cotação desconhecido: {', '.join(desconhecidos)}.")
        forn.canais_cotacao = canais
        mudou["canais_cotacao"] = canais

    if "categorias" in dados:
        mudou["categorias"] = definir_categorias(s, forn, dados.get("categorias") or [])
    return mudou


def definir_categorias(s: Session, forn: Fornecedor,
                       categoria_ids: list[Any]) -> list[int]:
    """O que este fornecedor vende. Substitui a lista inteira — é assim que a
    tela funciona: marcar e desmarcar caixas."""
    desejadas = set()
    for bruto in categoria_ids:
        try:
            desejadas.add(int(bruto))
        except (TypeError, ValueError):
            continue
    conhecidas = {c.id for c in s.scalars(select(InsumoCategoria)).all()}
    invalidas = desejadas - conhecidas
    if invalidas:
        raise ErroValidacao(
            f"Categoria de insumo inexistente: {sorted(invalidas)}.")

    atuais = {v.categoria_insumo_id: v for v in s.scalars(
        select(FornecedorCategoria)).all() if v.fornecedor_id == forn.id}
    for categoria_id in desejadas - set(atuais):
        s.add(FornecedorCategoria(fornecedor_id=forn.id,
                                  categoria_insumo_id=categoria_id))
    for categoria_id in set(atuais) - desejadas:
        s.delete(atuais[categoria_id])
    s.flush()
    return sorted(desejadas)


# ---------------------------------------------------------------------------
# Criar e editar
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Fornecedor:
    """Cadastro novo pela tela de Suprimentos, já com o que a cotação precisa."""
    forn = base.criar(s, dados, usuario)
    _aplicar_campos_de_suprimentos(s, forn, dados)
    # O contato só nasce junto se houver e-mail ou telefone: sem isso o banco
    # recusaria a linha e derrubaria o cadastro inteiro do fornecedor.
    if _texto(dados.get("contato_nome")) and (_texto(dados.get("email"))
                                              or _texto(dados.get("telefone"))):
        acrescentar_contato(s, forn.id, {
            "nome": dados.get("contato_nome"), "funcao": dados.get("contato_funcao"),
            "email": dados.get("email"), "telefone": dados.get("telefone")}, usuario)
    s.flush()
    return forn


_CAMPOS_BASE = {"razao_social", "nome_fantasia", "email", "telefone",
                "municipio", "uf", "observacoes", "ativo"}


def editar(s: Session, fornecedor_id: int, dados: dict[str, Any],
           usuario: Usuario) -> Fornecedor:
    """Correção em linha. Documento e tipo de pessoa não mudam nunca — isso é
    regra do cadastro base, não escolha desta tela."""
    forn = _obter(s, fornecedor_id)
    do_base = {k: v for k, v in dados.items() if k in _CAMPOS_BASE}
    if do_base:
        base.atualizar(s, fornecedor_id, do_base, usuario)
    mudou = _aplicar_campos_de_suprimentos(s, forn, dados)
    if mudou:
        registrar_evento(s, "fornecedor", forn.id, "EDITADO_SUPRIMENTOS", mudou,
                         usuario.id if usuario else None)
    return forn


def acrescentar_contato(s: Session, fornecedor_id: int, dados: dict[str, Any],
                        usuario: Usuario) -> FornecedorContato:
    """O cotador: a pessoa que responde. Sem nome de gente, cotação vira
    e-mail para caixa geral — e caixa geral não responde."""
    forn = _obter(s, fornecedor_id)
    nome = _texto(dados.get("nome"))
    if len(nome) < 3:
        raise ErroValidacao("Diga o nome de quem responde por este fornecedor.")
    # O banco exige um dos dois (ck_contato_tem_canal), e com razão: contato
    # sem e-mail nem telefone não serve para disparar cotação, que é a única
    # razão de ele existir. Recusar aqui dá o recado em português — deixar
    # chegar no banco daria "violates check constraint" na cara de quem digita.
    if not _texto(dados.get("email")) and not _texto(dados.get("telefone")):
        raise ErroValidacao(
            f"Informe o e-mail ou o telefone de {nome} — sem um dos dois não "
            f"há como mandar cotação para essa pessoa.")
    contato = FornecedorContato(
        fornecedor_id=forn.id, nome=nome,
        funcao=_texto(dados.get("funcao")) or None,
        email=_texto(dados.get("email")) or None,
        telefone=_texto(dados.get("telefone")) or None)
    s.add(contato)
    s.flush()
    registrar_evento(s, "fornecedor", forn.id, "CONTATO_ACRESCENTADO",
                     {"nome": nome}, usuario.id if usuario else None)
    return contato


def remover_contato(s: Session, contato_id: int, usuario: Usuario) -> None:
    contato = s.get(FornecedorContato, contato_id)
    if contato is None:
        raise ErroNaoEncontrado("Contato não encontrado.")
    fornecedor_id = contato.fornecedor_id
    s.delete(contato)
    s.flush()
    registrar_evento(s, "fornecedor", fornecedor_id, "CONTATO_REMOVIDO",
                     {"contato_id": contato_id}, usuario.id if usuario else None)


# ---------------------------------------------------------------------------
# A tela de gestão
# ---------------------------------------------------------------------------
def gerenciar(s: Session) -> dict[str, Any]:
    """Todos os fornecedores com o que Suprimentos precisa ver, mais os
    números do topo e as listas para os filtros."""
    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}
    por_fornecedor: dict[int, list[int]] = {}
    for v in s.scalars(select(FornecedorCategoria)).all():
        por_fornecedor.setdefault(v.fornecedor_id, []).append(v.categoria_insumo_id)

    contatos: dict[int, list[dict[str, Any]]] = {}
    for c in s.scalars(select(FornecedorContato)).all():
        contatos.setdefault(c.fornecedor_id, []).append(
            {"id": c.id, "nome": c.nome, "funcao": c.funcao or "",
             "email": c.email or "", "telefone": c.telefone or ""})

    linhas = []
    for f in s.scalars(select(Fornecedor)).all():
        if getattr(f, "e_fornecedor", True) is False:
            continue
        ids = sorted(set(por_fornecedor.get(f.id, [])))
        porte = f.porte.value if getattr(f, "porte", None) else ""
        linhas.append({
            "id": f.id, "razao_social": f.razao_social,
            "nome_fantasia": f.nome_fantasia or "",
            "cnpj_cpf": f.cnpj_cpf, "email": f.email or "",
            "telefone": f.telefone or "",
            "municipio": f.municipio or "", "uf": f.uf or "",
            "porte": porte, "porte_rotulo": PORTE_ROTULOS.get(porte, ""),
            "regioes": list(f.regioes_atuacao or []),
            "canais": list(f.canais_cotacao or []),
            "categorias_ids": ids,
            "categorias": [categorias.get(i, "") for i in ids],
            "contatos": contatos.get(f.id, []),
            "ativo": getattr(f, "ativo", True) is not False,
        })
    linhas.sort(key=lambda x: _chave(x["razao_social"]))

    ativos = [l for l in linhas if l["ativo"]]
    contagem_porte: dict[str, int] = {}
    for l in ativos:
        chave = l["porte"] or "SEM_PORTE"
        contagem_porte[chave] = contagem_porte.get(chave, 0) + 1

    regioes = sorted({r for l in linhas for r in l["regioes"]})
    return {
        "fornecedores": linhas,
        "categorias": sorted(
            [{"id": i, "nome": n} for i, n in categorias.items()],
            key=lambda x: _chave(x["nome"])),
        "regioes": regioes,
        "portes": [{"chave": k, "rotulo": v} for k, v in PORTE_ROTULOS.items()],
        "canais": [{"chave": k, "rotulo": CANAL_ROTULOS[k]} for k in CANAIS],
        "indicadores": {
            "total": len(linhas),
            "ativos": len(ativos),
            "inativos": len(linhas) - len(ativos),
            # Estes três são a razão de a tela existir: cada um é um fornecedor
            # que NÃO vai receber a próxima cotação, e ninguém percebe até a
            # cotação voltar com três preços em vez de seis.
            "sem_categoria": sum(1 for l in ativos if not l["categorias_ids"]),
            "sem_contato": sum(1 for l in ativos if not l["contatos"]),
            "sem_email": sum(1 for l in ativos
                             if "EMAIL" in l["canais"] and not l["email"]),
            "por_porte": contagem_porte,
        },
    }


def para_cotar(s: Session, categoria_ids: list[int],
               regiao: Optional[str] = None) -> list[dict[str, Any]]:
    """Quem vende estas categorias — a lista que a tela de cotação oferece.

    Fornecedor sem categoria nenhuma NÃO aparece: é o efeito prático de
    manter o cadastro em dia, e o motivo de a tela de gestão avisar quantos
    estão assim.
    """
    alvo = {int(c) for c in categoria_ids if str(c).strip()}
    saida = []
    for f in gerenciar(s)["fornecedores"]:
        if not f["ativo"] or not f["categorias_ids"]:
            continue
        if alvo and not (alvo & set(f["categorias_ids"])):
            continue
        if regiao and f["regioes"] and regiao.upper() not in f["regioes"]:
            continue
        saida.append(f)
    return saida
