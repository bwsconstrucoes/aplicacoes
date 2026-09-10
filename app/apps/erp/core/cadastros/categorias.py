# ============================================================================
# BWS ERP — core/cadastros/categorias.py
# Service simples do plano financeiro (categorias).
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Categoria, TipoTitulo, Usuario


def listar(s: Session, *, apenas_ativas: bool = True, busca: str = "") -> list[Categoria]:
    stmt = select(Categoria).order_by(Categoria.codigo)
    if apenas_ativas:
        stmt = stmt.where(Categoria.ativo.is_(True))
    busca = (busca or "").strip()
    if busca:
        stmt = stmt.where(Categoria.descricao.ilike(f"%{busca}%") | Categoria.codigo.ilike(f"%{busca}%"))
    return list(s.scalars(stmt).all())


def _lugar_no_plano(s: Session, codigo: str,
                    dados: dict[str, Any]) -> tuple[tuple, tuple]:
    """Onde a conta nova entra no plano, deduzido do CÓDIGO dela.

    O plano da BWS tem três níveis: "3.1.01" é a conta 01 do subgrupo 3.1
    ("Material"), dentro do grupo 3 ("Custos de obra"). Até 10/09/2026 a conta
    criada pela tela nascia SEM grupo — e o dono viu o efeito na hora: *"eu
    cadastrei a categoria dentro de custo de obra e ela não aparece"*. Aparecia,
    mas num grupo "Sem grupo" no alto da lista, longe de onde ele foi procurar.
    E nos relatórios ela ficava fora dos totais por grupo.

    Os NOMES do grupo e do subgrupo saem de uma conta irmã, que é o único jeito
    de a conta nova cair com o mesmo rótulo das outras. Sem irmã (grupo novo em
    folha), fica o código como nome — feio, mas visível e corrigível, e melhor
    do que sumir.
    """
    # Só deduz de código com a forma do plano ("3.1.01"). Código sem ponto não
    # diz a que grupo pertence, e chutar o código inteiro como grupo criaria um
    # grupo de uma conta só — pior que deixar em branco, porque parece certo.
    partes = codigo.split(".")
    tem_forma = len(partes) >= 2
    g_cod = (dados.get("grupo_codigo") or (partes[0] if tem_forma else "")).strip()
    sg_cod = (dados.get("subgrupo_codigo")
              or (".".join(partes[:2]) if tem_forma else "")).strip()

    irma_g = irma_sg = None
    if g_cod:
        for c in s.scalars(select(Categoria).where(
                Categoria.grupo_codigo == g_cod)).all():
            if c.grupo_codigo != g_cod:
                continue                     # a sessão dublada ignora o WHERE
            irma_g = irma_g or c
            if sg_cod and c.subgrupo_codigo == sg_cod:
                irma_sg = irma_sg or c

    g_nome = (dados.get("grupo_nome")
              or (irma_g.grupo_nome if irma_g else "") or g_cod)
    sg_nome = (dados.get("subgrupo_nome")
               or (irma_sg.subgrupo_nome if irma_sg else "") or sg_cod)
    return (g_cod or None, g_nome or None), (sg_cod or None, sg_nome or None)


def _proxima_ordem(s: Session, subgrupo: Optional[str]) -> int:
    """Depois da última conta do mesmo subgrupo — para a conta nova aparecer no
    fim do bloco a que pertence, e não no começo da lista inteira."""
    if not subgrupo:
        return 0
    ordens = [c.ordem or 0 for c in s.scalars(select(Categoria).where(
        Categoria.subgrupo_codigo == subgrupo)).all()
        if c.subgrupo_codigo == subgrupo]
    return (max(ordens) + 1) if ordens else 0


def ajeitar_sem_grupo(s: Session,
                      usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Põe no lugar as contas que nasceram sem grupo, deduzindo pelo código.

    Existe por causa das contas criadas ANTES de 10/09/2026 pela tela de
    Configurações: elas ficaram sem grupo e sem subgrupo, apareciam num "Sem
    grupo" no alto da lista e ficavam fora dos totais por grupo no relatório.
    Corrigir na frente resolve as próximas; estas precisavam de um empurrão.

    Não inventa nada que não esteja no código da conta, e não toca em conta que
    já tem grupo.
    """
    ajeitadas = []
    for cat in s.scalars(select(Categoria)).all():
        if (cat.grupo_codigo or "").strip():
            continue
        grupo, subgrupo = _lugar_no_plano(s, cat.codigo or "", {})
        if not grupo[0]:
            continue                     # código sem ponto: não dá para deduzir
        cat.grupo_codigo, cat.grupo_nome = grupo
        cat.subgrupo_codigo, cat.subgrupo_nome = subgrupo
        ajeitadas.append({"codigo": cat.codigo, "descricao": cat.descricao,
                          "grupo": f"{grupo[0]} · {grupo[1]}"})
    if ajeitadas:
        s.flush()
        registrar_evento(s, "categoria", 0, "GRUPO_AJEITADO",
                         {"contas": ajeitadas},
                         usuario.id if usuario else None)
    return {"ajeitadas": ajeitadas, "quantidade": len(ajeitadas)}


def criar(s: Session, dados: dict[str, Any], usuario: Optional[Usuario]) -> Categoria:
    codigo = (dados.get("codigo") or "").strip()
    descricao = (dados.get("descricao") or "").strip()
    if not codigo or not descricao:
        raise ErroValidacao("Código e descrição da categoria são obrigatórios.")
    if s.scalars(select(Categoria).where(Categoria.codigo == codigo)).first():
        raise ErroValidacao(f"Já existe categoria com o código {codigo}.")
    tipos = []
    for t in (dados.get("tipos_permitidos") or []):
        try:
            tipos.append(TipoTitulo(t if isinstance(t, str) else t.value))
        except ValueError:
            raise ErroValidacao(f"Tipo de título inválido em tipos_permitidos: {t!r}")
    natureza = (dados.get("natureza") or "RESULTADO").strip().upper()
    if natureza not in ("RESULTADO", "FLUXO"):
        raise ErroValidacao("Natureza da categoria deve ser RESULTADO ou FLUXO.")
    grupo, subgrupo = _lugar_no_plano(s, codigo, dados)
    cat = Categoria(codigo=codigo, descricao=descricao, natureza=natureza,
                    codigo_omie=(str(dados.get("codigo_omie") or "").strip() or None),
                    tipos_permitidos=tipos,
                    grupo_codigo=grupo[0], grupo_nome=grupo[1],
                    subgrupo_codigo=subgrupo[0], subgrupo_nome=subgrupo[1],
                    # Conta feita à mão é PERSONALIZADA: "Instalar plano padrão"
                    # não pode reescrever a descrição que o dono escolheu.
                    personalizada=True,
                    ordem=_proxima_ordem(s, subgrupo[0]),
                    dedutivel_padrao=bool(dados.get("dedutivel_padrao", True)),
                    credito_pis_cofins=bool(dados.get("credito_pis_cofins", False)),
                    conta_contabil=(dados.get("conta_contabil") or "").strip() or None)
    s.add(cat)
    s.flush()
    registrar_evento(s, "categoria", cat.id, "CRIADA",
                     {"codigo": codigo, "descricao": descricao,
                      "origem": dados.get("origem", "SISTEMA")},
                     usuario.id if usuario else None)
    return cat
