# ============================================================================
# ERP — core/arquivo/service.py
# Arquivar, encontrar e entregar documento.
#
# O QUE ESTE MÓDULO NÃO FAZ, DE PROPÓSITO:
#
#   * não guarda bytes — quem guarda é `core/documentos/armazenamento.py`, que
#     desde a migração 043 sabe morar no banco ou no Google Drive;
#   * não lê documento com IA — quem lê é `core/documentos/leitor.py`;
#   * não decide permissão sozinho — o escopo por obra vem de `aplicar_escopo`,
#     porque regra de escopo nova nunca se escreve à mão.
#
# Ele faz uma coisa só: transforma "arquivo guardado" em "documento
# encontrável" — tipo, dono, validade, competência e nome padronizado.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import func, or_, select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.arquivo import catalogo, nomes
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento)
from app.apps.erp.db.models.cadastros import (Colaborador, Empresa, Fornecedor,
                                              Obra, PerfilUsuario as P, Usuario)
from app.apps.erp.db.models.financeiro import Anexo, Documento, DocumentoTipo

logger = logging.getLogger(__name__)

# Quem enxerga cada faixa de sigilo. A mais restrita ganha, e o escopo por obra
# continua valendo por cima disto.
VE_RESTRITO = (P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
               P.DEPARTAMENTO_PESSOAL)
VE_PESSOAL = (P.ADMIN, P.DIRETOR_FINANCEIRO, P.DEPARTAMENTO_PESSOAL)


# Todas as faixas. Serve só à conferência que o SISTEMA faz sozinho (o aviso
# da agenda sobre pasta incompleta): sem ela, a conferência sem usuário
# enxergaria só a faixa aberta e diria que a pasta fiscal está completa quando
# está vazia — quase tudo nela é restrito.
SIGILOS_TODOS = ("ABERTO", "RESTRITO", "PESSOAL")


def aplicar_escopo(stmt, s: Session, usuario: Optional[Usuario]):
    """O recorte do acervo para esta pessoa — em UM lugar só.

    Duas travas, e a mais restrita ganha:

      · a FAIXA DE SIGILO do tipo de documento (aberto, restrito, pessoal);
      · a OBRA, para quem é preso a obras designadas.

    Existe como função desde 11/09/2026 porque a busca do assistente precisou
    do mesmo recorte da tela. Duas cópias da regra divergem no dia em que
    alguém corrige uma — e aqui divergir quer dizer alguém ver documento que
    não devia. Decisão do dono, com todas as letras: *"quem vê o quê tem que
    estar associado às suas permissões"*.

    A consulta precisa já ter feito o `join` com `DocumentoTipo`.
    """
    stmt = stmt.where(DocumentoTipo.sigilo.in_(sigilos_visiveis(usuario)))
    if usuario is None:
        return stmt

    from app.apps.erp.core.auth.permissoes import obras_do_usuario

    minhas = obras_do_usuario(s, usuario)
    if minhas is None:
        return stmt                       # enxerga todas as obras

    if usuario.perfil is P.PARCEIRO:
        # O parceiro é de FORA da BWS: documento que não é de obra nenhuma é
        # papelada da empresa (contrato social, certidão, seguro) e não lhe diz
        # respeito. Sem obra designada, ele não alcança documento nenhum.
        return stmt.where(Documento.obra_id.in_(minhas or [-1]))

    # Quem é de dentro e responde por obra continua alcançando o que é da
    # empresa como um todo — é a papelada que ele precisa para tocar a obra.
    return stmt.where(or_(Documento.obra_id.is_(None),
                          Documento.obra_id.in_(minhas or [-1])))


def sigilos_visiveis(usuario: Optional[Usuario]) -> tuple[str, ...]:
    if usuario is None:
        return ("ABERTO",)
    faixas = ["ABERTO"]
    if usuario.perfil in VE_RESTRITO:
        faixas.append("RESTRITO")
    if usuario.perfil in VE_PESSOAL:
        faixas.append("PESSOAL")
    return tuple(faixas)


# ---------------------------------------------------------------------------
# Arquivar
# ---------------------------------------------------------------------------
def _apelido_do_dono(s: Session, tipo: DocumentoTipo, dono: dict[str, Any]) -> str:
    if dono.get("empresa_id"):
        e = s.get(Empresa, dono["empresa_id"])
        if e is None:
            raise ErroValidacao("Empresa não encontrada.")
        return nomes.apelido_da_empresa(e)
    if dono.get("obra_id"):
        o = s.get(Obra, dono["obra_id"])
        if o is None:
            raise ErroValidacao("Obra não encontrada.")
        return nomes.apelido_da_obra(o)
    if dono.get("colaborador_id"):
        c = s.get(Colaborador, dono["colaborador_id"])
        if c is None:
            raise ErroValidacao("Colaborador não encontrado.")
        return nomes.apelido_da_pessoa(c)
    if dono.get("fornecedor_id"):
        f = s.get(Fornecedor, dono["fornecedor_id"])
        if f is None:
            raise ErroValidacao("Fornecedor não encontrado.")
        return nomes.apelido_do_parceiro(f)
    if dono.get("lancamento_id"):
        return nomes.limpar(f"{dono.get('lancamento_tipo') or 'LANC'}-{dono['lancamento_id']}")
    raise ErroValidacao(
        "Diga a quem este documento pertence: uma empresa, uma obra, uma "
        "pessoa, um fornecedor ou um lançamento.")


def _primeiro_do_mes(d: Optional[date]) -> Optional[date]:
    return d.replace(day=1) if d else None


def arquivar(s: Session, conteudo: bytes, nome_arquivo: str, *,
             tipo_codigo: str, empresa_id: Optional[int] = None,
             obra_id: Optional[int] = None, colaborador_id: Optional[int] = None,
             fornecedor_id: Optional[int] = None,
             lancamento_tipo: str = "", lancamento_id: Optional[int] = None,
             competencia: Optional[date] = None, referencia: str = "",
             emissao: Optional[date] = None, validade: Optional[date] = None,
             texto: str = "", resumo: str = "", observacao: str = "",
             origem: str = "TELA",
             usuario: Optional[Usuario] = None) -> Documento:
    """Guarda o arquivo e o cataloga, com nome padronizado."""
    from app.apps.erp.core.documentos.armazenamento import salvar

    tipo = catalogo.obter(s, tipo_codigo)
    dono = {"empresa_id": empresa_id, "obra_id": obra_id,
            "colaborador_id": colaborador_id, "fornecedor_id": fornecedor_id,
            "lancamento_tipo": lancamento_tipo or None,
            "lancamento_id": lancamento_id}
    quantos = sum(1 for c in ("empresa_id", "obra_id", "colaborador_id",
                              "fornecedor_id", "lancamento_id") if dono.get(c))
    if quantos != 1:
        raise ErroValidacao(
            "O documento pertence a UM dono: uma empresa, uma obra, uma "
            "pessoa, um fornecedor ou um lançamento — nem nenhum, nem dois. "
            "Documento pendurado em dois lugares não é achado em nenhum.")
    if tipo.vence and validade is None:
        raise ErroValidacao(
            f"{tipo.nome} vence — informe até quando vale. Sem isso o sistema "
            f"não tem como avisar antes, que é metade do valor de guardá-lo.")
    if tipo.por_competencia and competencia is None:
        raise ErroValidacao(
            f"{tipo.nome} é documento de um mês — informe a competência.")

    competencia = _primeiro_do_mes(competencia)
    apelido = _apelido_do_dono(s, tipo, dono)
    nome_padrao = nomes.montar(
        tipo_codigo=tipo.codigo, dono=apelido, referencia=referencia,
        competencia=competencia, emissao=emissao, validade=validade,
        nome_original=nome_arquivo, por_competencia=tipo.por_competencia,
        vence=tipo.vence)

    # A entidade do anexo espelha o dono, para o escopo por obra continuar
    # valendo sem regra nova.
    entidade_tipo = ("obra" if obra_id else "empresa" if empresa_id
                     else "colaborador" if colaborador_id
                     else "fornecedor" if fornecedor_id
                     else (lancamento_tipo or "documento"))
    entidade_id = (obra_id or empresa_id or colaborador_id or fornecedor_id
                   or lancamento_id or 0)
    anexo = salvar(s, conteudo, nome_padrao, entidade_tipo=entidade_tipo,
                   entidade_id=entidade_id, categoria="OUTRO",
                   descricao=tipo.nome, usuario=usuario)

    ja = s.scalars(select(Documento).where(Documento.anexo_id == anexo.id)).first()
    if ja is not None:
        return ja        # o mesmo arquivo já está catalogado

    d = Documento(
        tipo_codigo=tipo.codigo, anexo_id=anexo.id,
        nome_padronizado=nome_padrao, nome_original=nome_arquivo or None,
        empresa_id=empresa_id, obra_id=obra_id, colaborador_id=colaborador_id,
        fornecedor_id=fornecedor_id,
        lancamento_tipo=(lancamento_tipo or None), lancamento_id=lancamento_id,
        competencia=competencia, referencia=(referencia or None),
        emissao=emissao, validade=validade,
        texto=(texto or None), resumo=(resumo or None),
        observacao=(observacao or None), origem=origem,
        criado_por=usuario.id if usuario else None,
        confirmado_por=usuario.id if usuario else None,
        confirmado_em=datetime.now())
    s.add(d)
    s.flush()
    registrar_evento(s, "documento", d.id, "ARQUIVADO", {
        "tipo": tipo.codigo, "nome": nome_padrao, "origem": origem},
        usuario.id if usuario else None)
    logger.info("ERP/arquivo: %s guardado", nome_padrao)
    return d


# ---------------------------------------------------------------------------
# Encontrar
# ---------------------------------------------------------------------------
def _situacao_validade(d: Documento, hoje: date) -> tuple[str, Optional[int]]:
    if d.validade is None:
        return "SEM_VALIDADE", None
    dias = (d.validade - hoje).days
    if dias < 0:
        return "VENCIDO", dias
    avisar = d.tipo.avisar_dias if d.tipo is not None else None
    if avisar is not None and dias <= avisar:
        return "VENCENDO", dias
    return "VALIDO", dias


def ler(s: Session, d: Documento, hoje: Optional[date] = None) -> dict[str, Any]:
    hoje = hoje or date.today()
    situacao, dias = _situacao_validade(d, hoje)
    dono_nome = ""
    if d.empresa_id:
        e = s.get(Empresa, d.empresa_id)
        dono_nome = getattr(e, "nome_fantasia", None) or getattr(e, "razao_social", "")
    elif d.obra_id:
        o = s.get(Obra, d.obra_id)
        dono_nome = getattr(o, "codigo", "")
    elif d.colaborador_id:
        c = s.get(Colaborador, d.colaborador_id)
        dono_nome = getattr(c, "nome", "")
    elif d.fornecedor_id:
        f = s.get(Fornecedor, d.fornecedor_id)
        dono_nome = getattr(f, "razao_social", "")
    elif d.lancamento_id:
        dono_nome = f"{d.lancamento_tipo} {d.lancamento_id}"
    return {
        "id": d.id, "tipo": d.tipo_codigo,
        "tipo_nome": d.tipo.nome if d.tipo else d.tipo_codigo,
        "grupo": d.tipo.grupo if d.tipo else "",
        "sigilo": d.tipo.sigilo if d.tipo else "ABERTO",
        "nome": d.nome_padronizado, "nome_original": d.nome_original,
        "dono": dono_nome,
        "empresa_id": d.empresa_id, "obra_id": d.obra_id,
        "colaborador_id": d.colaborador_id, "fornecedor_id": d.fornecedor_id,
        "competencia": d.competencia.strftime("%Y-%m") if d.competencia else None,
        "referencia": d.referencia,
        "emissao": d.emissao.isoformat() if d.emissao else None,
        "validade": d.validade.isoformat() if d.validade else None,
        "situacao": situacao, "dias_para_vencer": dias,
        "resumo": d.resumo,
        "anexo_id": d.anexo_id,
        "em": d.criado_em.strftime("%d/%m/%Y") if d.criado_em else None,
    }


def listar(s: Session, *, usuario: Optional[Usuario] = None,
           tipo: str = "", grupo: str = "", empresa_id: Optional[int] = None,
           obra_id: Optional[int] = None, colaborador_id: Optional[int] = None,
           fornecedor_id: Optional[int] = None, competencia: Optional[date] = None,
           situacao: str = "", busca: str = "",
           limite: int = 500) -> dict[str, Any]:
    hoje = date.today()
    stmt = (select(Documento).join(DocumentoTipo,
                                   Documento.tipo_codigo == DocumentoTipo.codigo)
            .order_by(Documento.id.desc()).limit(limite))
    stmt = aplicar_escopo(stmt, s, usuario)
    if tipo:
        stmt = stmt.where(Documento.tipo_codigo == tipo)
    if grupo:
        stmt = stmt.where(DocumentoTipo.grupo == grupo)
    if empresa_id:
        stmt = stmt.where(Documento.empresa_id == empresa_id)
    if obra_id:
        stmt = stmt.where(Documento.obra_id == obra_id)
    if colaborador_id:
        stmt = stmt.where(Documento.colaborador_id == colaborador_id)
    if fornecedor_id:
        stmt = stmt.where(Documento.fornecedor_id == fornecedor_id)
    if competencia:
        stmt = stmt.where(Documento.competencia == _primeiro_do_mes(competencia))

    busca = (busca or "").strip()
    if busca:
        # A busca olha o nome, o resumo E o texto de dentro do documento. É por
        # isso que o texto é extraído na entrada.
        alvo = f"%{busca}%"
        stmt = stmt.where(or_(Documento.nome_padronizado.ilike(alvo),
                              Documento.nome_original.ilike(alvo),
                              Documento.referencia.ilike(alvo),
                              Documento.resumo.ilike(alvo),
                              Documento.texto.ilike(alvo)))

    linhas = [ler(s, d, hoje) for d in s.scalars(stmt).all()]
    if situacao:
        linhas = [l for l in linhas if l["situacao"] == situacao]

    resumo = {
        "quantidade": len(linhas),
        "vencidos": sum(1 for l in linhas if l["situacao"] == "VENCIDO"),
        "vencendo": sum(1 for l in linhas if l["situacao"] == "VENCENDO"),
        "validos": sum(1 for l in linhas if l["situacao"] == "VALIDO"),
    }
    return {"documentos": linhas, "resumo": resumo,
            "limite_atingido": len(linhas) >= limite}


def vencendo(s: Session, *, dias: int = 30,
             usuario: Optional[Usuario] = None) -> list[dict[str, Any]]:
    """O que vence nos próximos `dias`, e o que já venceu.

    É a pergunta que dá valor à gestão inteira: certidão que vence sem ninguém
    ver é a diferença entre ganhar e perder uma licitação.
    """
    hoje = date.today()
    stmt = (select(Documento).join(DocumentoTipo,
                                   Documento.tipo_codigo == DocumentoTipo.codigo)
            .where(Documento.validade.is_not(None),
                   Documento.validade <= hoje + timedelta(days=dias),
                   DocumentoTipo.sigilo.in_(sigilos_visiveis(usuario)))
            .order_by(Documento.validade))
    return [ler(s, d, hoje) for d in s.scalars(stmt).all()]


def pode_ver_documento(s: Session, usuario: Optional[Usuario],
                       documento_id: int) -> bool:
    """O documento existe E está dentro do recorte desta pessoa?

    Passa pelo MESMO `aplicar_escopo` da listagem, de propósito: detalhe e
    lista não têm como divergir sem que alguém altere os dois.
    """
    stmt = select(Documento.id).join(
        DocumentoTipo, Documento.tipo_codigo == DocumentoTipo.codigo
    ).where(Documento.id == documento_id)
    return s.scalar(aplicar_escopo(stmt, s, usuario)) is not None


def exigir_documento_no_escopo(s: Session, usuario: Optional[Usuario],
                               documento_id: int) -> None:
    """Fora do recorte responde igual a inexistente.

    Dizer "sem permissão" para um número que existe confirmaria que ele
    existe, e varrer os números mapearia o acervo inteiro sem abrir nada.
    """
    if not pode_ver_documento(s, usuario, documento_id):
        raise ErroNaoEncontrado("Documento não encontrado.")


def excluir(s: Session, documento_id: int, usuario: Usuario) -> None:
    """Apaga o documento e o arquivo guardado com ele.

    APAGAR É DESTRUTIVO, e até 11/09/2026 não conferia nada: quem tinha a ação
    "arquivar" apagava qualquer documento pelo número, inclusive de faixa de
    sigilo que não enxerga na tela (o FINANCEIRO e o gestor de obra não veem
    documento PESSOAL) e de obra que não é dele. Agora passa pelo mesmo
    recorte da listagem.
    """
    from app.apps.erp.core.documentos.armazenamento import excluir as excluir_anexo
    exigir_documento_no_escopo(s, usuario, documento_id)
    d = s.get(Documento, documento_id)
    if d is None:
        raise ErroNaoEncontrado("Documento não encontrado.")
    registrar_evento(s, "documento", d.id, "EXCLUIDO",
                     {"tipo": d.tipo_codigo, "nome": d.nome_padronizado},
                     usuario.id)
    anexo_id = d.anexo_id
    s.delete(d)
    s.flush()
    excluir_anexo(s, anexo_id, usuario)
