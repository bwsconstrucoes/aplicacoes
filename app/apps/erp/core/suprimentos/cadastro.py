# ============================================================================
# ERP — core/suprimentos/cadastro.py
# A base de Suprimentos: categoria de insumo, unidade de compra e o insumo.
#
# Existe porque a primeira versão do módulo só sabia RECEBER pedido de cadastro
# de insumo. Não havia como criar uma categoria de insumo pela tela — e sem
# categoria não se cadastra insumo, então nada em Suprimentos podia ser
# testado. Aqui o cadastro é de mão dupla: quem administra cria e corrige
# direto; quem está na obra continua pedindo por `insumos.py`.
#
# Duas regras que valem para tudo neste arquivo:
#
#   1. NADA SE APAGA, DESATIVA-SE. Insumo apagado leva junto o histórico de
#      preço e as solicitações que o citam. `ativo = false` some das listas de
#      escolha e preserva o passado.
#   2. A CONTA DO PLANO DE UM INSUMO É DE DESPESA OU MATERIAL. Oferecer o
#      plano inteiro põe "Receita de obras" ao lado de "Cimento" na mesma
#      caixa — e alguém vai escolher errado.
# ============================================================================
from __future__ import annotations

import logging
import unicodedata
from typing import Any, Iterable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Categoria, Insumo, InsumoCategoria, UnidadeCompra, Usuario,
)

logger = logging.getLogger(__name__)

# Tipos de documento que representam COMPRA. Uma conta do plano que não aceita
# nenhum deles não é conta de insumo: receita, tributo, folha e movimentação
# financeira ficam de fora sozinhas, sem lista escrita à mão.
TIPOS_DE_COMPRA = frozenset({
    "T1_MATERIAL_NFE", "T2_SERVICO_NFSE", "T3_FRETE_CTE", "T4_LOCACAO",
})
GRUPO_RECEITA = "1"


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def _sem_acento(texto: str) -> str:
    forma = unicodedata.normalize("NFKD", texto or "")
    return "".join(c for c in forma if not unicodedata.combining(c))


def _chave(texto: str) -> str:
    """Para comparar nomes: sem acento, sem caixa, sem espaço repetido."""
    return _sem_acento(_texto(texto)).casefold()


# ---------------------------------------------------------------------------
# Conta do plano financeiro — só o que se compra
# ---------------------------------------------------------------------------
def _tipos_de(categoria: Categoria) -> set[str]:
    saida = set()
    for t in (getattr(categoria, "tipos_permitidos", None) or []):
        saida.add(t.value if hasattr(t, "value") else str(t))
    return saida


def e_conta_de_compra(categoria: Categoria) -> bool:
    """Conta que pode receber um insumo: aceita nota de material, serviço,
    frete ou locação, e não é conta de receita."""
    if getattr(categoria, "ativo", True) is False:
        return False
    if (getattr(categoria, "grupo_codigo", "") or "") == GRUPO_RECEITA:
        return False
    return bool(_tipos_de(categoria) & TIPOS_DE_COMPRA)


def contas_de_compra(s: Session) -> list[dict[str, Any]]:
    """O plano financeiro reduzido ao que faz sentido para um insumo.

    Vem agrupado porque a tela monta um <optgroup> por subgrupo: escolher
    entre 12 contas de "Materiais aplicados" é diferente de escolher entre 140
    contas soltas.
    """
    contas = s.scalars(select(Categoria).order_by(Categoria.codigo)).all()
    saida = []
    for c in contas:
        if not e_conta_de_compra(c):
            continue
        saida.append({
            "id": c.id, "codigo": c.codigo, "descricao": c.descricao,
            "grupo": c.grupo_nome or "Sem grupo",
            "subgrupo": c.subgrupo_nome or (c.grupo_nome or "Sem grupo"),
            "uso": c.descricao_uso or "",
            "tipos": sorted(_tipos_de(c) & TIPOS_DE_COMPRA),
        })
    return sorted(saida, key=lambda x: x["codigo"])


# ---------------------------------------------------------------------------
# Categorias de insumo
# ---------------------------------------------------------------------------
def _proximo_codigo_categoria(s: Session) -> str:
    numeros = []
    for c in s.scalars(select(InsumoCategoria)).all():
        codigo = getattr(c, "codigo", "") or ""
        sufixo = codigo.split("-", 1)[1] if codigo.upper().startswith("CAT-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return f"CAT-{(max(numeros) + 1) if numeros else 1:04d}"


def listar_categorias(s: Session) -> list[dict[str, Any]]:
    """As categorias com o que cada uma carrega — é o que permite decidir se
    uma categoria vazia deve ser desativada ou preenchida."""
    insumos = s.scalars(select(Insumo)).all()
    por_categoria: dict[Optional[int], int] = {}
    for i in insumos:
        if getattr(i, "ativo", True) is not False:
            por_categoria[i.categoria_insumo_id] = por_categoria.get(i.categoria_insumo_id, 0) + 1

    saida = []
    for c in s.scalars(select(InsumoCategoria)).all():
        saida.append({"id": c.id, "codigo": c.codigo, "nome": c.nome,
                      "ativo": getattr(c, "ativo", True) is not False,
                      "insumos": por_categoria.get(c.id, 0)})
    return sorted(saida, key=lambda x: _chave(x["nome"]))


def criar_categoria(s: Session, dados: dict[str, Any],
                    usuario: Usuario) -> InsumoCategoria:
    nome = _texto(dados.get("nome"))
    if len(nome) < 2:
        raise ErroValidacao("Dê um nome à categoria.")
    for existente in s.scalars(select(InsumoCategoria)).all():
        if _chave(existente.nome) == _chave(nome):
            raise ErroValidacao(f"Já existe a categoria {existente.nome!r}.")

    codigo = _texto(dados.get("codigo")).upper() or _proximo_codigo_categoria(s)
    categoria = InsumoCategoria(codigo=codigo, nome=nome, ativo=True)
    s.add(categoria)
    s.flush()
    registrar_evento(s, "insumo_categoria", categoria.id, "CRIADA",
                     {"codigo": codigo, "nome": nome},
                     usuario.id if usuario else None)
    return categoria


def editar_categoria(s: Session, categoria_id: int, dados: dict[str, Any],
                     usuario: Usuario) -> InsumoCategoria:
    categoria = s.get(InsumoCategoria, categoria_id, with_for_update=True,
                      populate_existing=True)
    if categoria is None:
        raise ErroNaoEncontrado("Categoria de insumo não encontrada.")

    antes = {"nome": categoria.nome, "ativo": categoria.ativo}
    if "nome" in dados:
        nome = _texto(dados.get("nome"))
        if len(nome) < 2:
            raise ErroValidacao("Dê um nome à categoria.")
        for outra in s.scalars(select(InsumoCategoria)).all():
            if outra.id != categoria_id and _chave(outra.nome) == _chave(nome):
                raise ErroValidacao(f"Já existe a categoria {outra.nome!r}.")
        categoria.nome = nome
    if "ativo" in dados:
        categoria.ativo = bool(dados.get("ativo"))

    registrar_evento(s, "insumo_categoria", categoria.id, "EDITADA",
                     {"antes": antes, "depois": {"nome": categoria.nome,
                                                 "ativo": categoria.ativo}},
                     usuario.id if usuario else None)
    return categoria


# ---------------------------------------------------------------------------
# Unidades de compra
# ---------------------------------------------------------------------------
def listar_unidades(s: Session) -> list[dict[str, Any]]:
    insumos = s.scalars(select(Insumo)).all()
    uso: dict[str, int] = {}
    for i in insumos:
        chave = (getattr(i, "unidade", "") or "").upper()
        if chave:
            uso[chave] = uso.get(chave, 0) + 1
    unidades = s.scalars(select(UnidadeCompra)).all()
    saida = [{"codigo": u.codigo, "descricao": u.descricao,
              "ordem": u.ordem or 0, "ativo": getattr(u, "ativo", True) is not False,
              "insumos": uso.get((u.codigo or "").upper(), 0)}
             for u in unidades]
    return sorted(saida, key=lambda x: (x["ordem"], x["codigo"]))


def criar_unidade(s: Session, dados: dict[str, Any],
                  usuario: Usuario) -> UnidadeCompra:
    codigo = _texto(dados.get("codigo")).upper()
    descricao = _texto(dados.get("descricao"))
    if not codigo:
        raise ErroValidacao("Informe a sigla da unidade (ex.: SC, M², VB).")
    if len(codigo) > 10:
        raise ErroValidacao("A sigla da unidade é curta — até 10 caracteres.")
    if not descricao:
        raise ErroValidacao("Escreva por extenso o que a sigla significa.")
    if s.get(UnidadeCompra, codigo) is not None:
        raise ErroValidacao(f"A unidade {codigo} já existe.")

    unidade = UnidadeCompra(codigo=codigo, descricao=descricao,
                            ordem=int(dados.get("ordem") or 99), ativo=True)
    s.add(unidade)
    s.flush()
    registrar_evento(s, "unidade_compra", 0, "CRIADA",
                     {"codigo": codigo, "descricao": descricao},
                     usuario.id if usuario else None)
    return unidade


def editar_unidade(s: Session, codigo: str, dados: dict[str, Any],
                   usuario: Usuario) -> UnidadeCompra:
    unidade = s.get(UnidadeCompra, _texto(codigo).upper(), with_for_update=True,
                    populate_existing=True)
    if unidade is None:
        raise ErroNaoEncontrado("Unidade de compra não encontrada.")
    antes = {"descricao": unidade.descricao, "ativo": unidade.ativo}
    if "descricao" in dados:
        descricao = _texto(dados.get("descricao"))
        if not descricao:
            raise ErroValidacao("Escreva por extenso o que a sigla significa.")
        unidade.descricao = descricao
    if "ativo" in dados:
        unidade.ativo = bool(dados.get("ativo"))
    if "ordem" in dados:
        unidade.ordem = int(dados.get("ordem") or 0)
    registrar_evento(s, "unidade_compra", 0, "EDITADA",
                     {"codigo": unidade.codigo, "antes": antes},
                     usuario.id if usuario else None)
    return unidade


# ---------------------------------------------------------------------------
# Insumos
# ---------------------------------------------------------------------------
def _validar_insumo(s: Session, dados: dict[str, Any],
                    ignorar_id: Optional[int] = None) -> dict[str, Any]:
    descricao = _texto(dados.get("descricao"))
    if len(descricao) < 3:
        raise ErroValidacao("Dê um nome ao insumo (mínimo 3 letras).")
    for outro in s.scalars(select(Insumo)).all():
        if outro.id != ignorar_id and _chave(outro.descricao) == _chave(descricao):
            raise ErroValidacao(
                f"Já existe o insumo {outro.codigo} · {outro.descricao}.")

    if not dados.get("categoria_insumo_id"):
        raise ErroValidacao(
            "Escolha a categoria de insumo — é ela que decide quem recebe a "
            "cotação deste material.")
    categoria = s.get(InsumoCategoria, int(dados["categoria_insumo_id"]))
    if categoria is None:
        raise ErroValidacao("Categoria de insumo não encontrada.")

    if not dados.get("categoria_id"):
        raise ErroValidacao(
            "Escolha a conta do plano financeiro. Sem ela, o pedido de compra "
            "não vira previsão de pagamento apropriada.")
    conta = s.get(Categoria, int(dados["categoria_id"]))
    if conta is None:
        raise ErroValidacao("Conta do plano financeiro não encontrada.")
    if not e_conta_de_compra(conta):
        raise ErroValidacao(
            f"{conta.codigo} · {conta.descricao} não é conta de despesa ou "
            f"material. Insumo se aponta para conta de custo, não de receita.")

    unidade = _texto(dados.get("unidade")).upper() or None
    if unidade and s.get(UnidadeCompra, unidade) is None:
        raise ErroValidacao(
            f"A unidade {unidade} não está cadastrada. Cadastre-a antes em "
            f"Configurações de Suprimentos.")
    return {"descricao": descricao,
            "categoria_insumo_id": int(dados["categoria_insumo_id"]),
            "categoria_id": int(dados["categoria_id"]),
            "unidade": unidade}


def criar_insumo(s: Session, dados: dict[str, Any], usuario: Usuario) -> Insumo:
    """Cadastro direto, por quem administra Suprimentos. O caminho do pedido
    (`insumos.solicitar`) continua existindo para quem está na obra."""
    from app.apps.erp.core.suprimentos.insumos import proximo_codigo

    campos = _validar_insumo(s, dados)
    insumo = Insumo(codigo=proximo_codigo(s), ativo=True,
                    locavel=bool(dados.get("locavel")), **campos)
    s.add(insumo)
    s.flush()
    registrar_evento(s, "insumo", insumo.id, "CRIADO",
                     {"codigo": insumo.codigo, "descricao": insumo.descricao},
                     usuario.id if usuario else None)
    return insumo


def editar_insumo(s: Session, insumo_id: int, dados: dict[str, Any],
                  usuario: Usuario) -> Insumo:
    """Correção em linha, como se corrige uma célula de planilha."""
    insumo = s.get(Insumo, insumo_id, with_for_update=True, populate_existing=True)
    if insumo is None:
        raise ErroNaoEncontrado("Insumo não encontrado.")

    antes = {"descricao": insumo.descricao, "unidade": insumo.unidade,
             "categoria_insumo_id": insumo.categoria_insumo_id,
             "categoria_id": insumo.categoria_id, "ativo": insumo.ativo,
             "locavel": insumo.locavel}

    # Só o que veio muda; o resto continua como está. Assim a tela pode mandar
    # uma célula sozinha sem apagar as outras por omissão.
    completo = dict(antes)
    completo.update({k: v for k, v in dados.items() if k in
                     ("descricao", "unidade", "categoria_insumo_id", "categoria_id")})
    campos = _validar_insumo(s, completo, ignorar_id=insumo_id)
    for chave, valor in campos.items():
        setattr(insumo, chave, valor)
    if "ativo" in dados:
        insumo.ativo = bool(dados.get("ativo"))
    if "locavel" in dados:
        insumo.locavel = bool(dados.get("locavel"))

    registrar_evento(s, "insumo", insumo.id, "EDITADO",
                     {"antes": antes,
                      "depois": {"descricao": insumo.descricao,
                                 "unidade": insumo.unidade,
                                 "categoria_insumo_id": insumo.categoria_insumo_id,
                                 "categoria_id": insumo.categoria_id,
                                 "ativo": insumo.ativo, "locavel": insumo.locavel}},
                     usuario.id if usuario else None)
    return insumo


def gerenciar_insumos(s: Session) -> dict[str, Any]:
    """A tela de gestão: a lista completa mais o que ela precisa para filtrar,
    e os números do topo.

    Vem tudo de uma vez de propósito — a filtragem acontece no navegador,
    como numa planilha, sem uma volta ao servidor por tecla digitada.
    """
    categorias = {c.id: c for c in s.scalars(select(InsumoCategoria)).all()}
    contas = {c.id: c for c in s.scalars(select(Categoria)).all()}
    ultimo = _ultimo_preco_por_insumo(s)

    linhas = []
    for i in s.scalars(select(Insumo)).all():
        categoria = categorias.get(i.categoria_insumo_id)
        conta = contas.get(i.categoria_id)
        preco = ultimo.get(i.id) or {}
        linhas.append({
            "id": i.id, "codigo": i.codigo, "descricao": i.descricao,
            "unidade": i.unidade or "",
            "categoria_insumo_id": i.categoria_insumo_id,
            "categoria_insumo": getattr(categoria, "nome", "") or "",
            "categoria_id": i.categoria_id,
            "conta_codigo": getattr(conta, "codigo", "") or "",
            "conta": (f"{conta.codigo} · {conta.descricao}" if conta else ""),
            "locavel": bool(i.locavel),
            "ativo": getattr(i, "ativo", True) is not False,
            "ultimo_preco": preco.get("valor"),
            "ultimo_preco_em": preco.get("data"),
            "ultimo_preco_origem": preco.get("origem"),
        })
    linhas.sort(key=lambda x: _chave(x["descricao"]))

    ativos = [l for l in linhas if l["ativo"]]
    return {
        "insumos": linhas,
        "categorias": listar_categorias(s),
        "unidades": listar_unidades(s),
        "contas": contas_de_compra(s),
        "indicadores": {
            "total": len(linhas),
            "ativos": len(ativos),
            "inativos": len(linhas) - len(ativos),
            "sem_categoria": sum(1 for l in ativos if not l["categoria_insumo_id"]),
            "sem_conta": sum(1 for l in ativos if not l["categoria_id"]),
            "sem_unidade": sum(1 for l in ativos if not l["unidade"]),
            "com_preco": sum(1 for l in ativos if l["ultimo_preco"] is not None),
            "locaveis": sum(1 for l in ativos if l["locavel"]),
        },
    }


def _ultimo_preco_por_insumo(s: Session) -> dict[int, dict[str, Any]]:
    """O preço mais recente de cada insumo, do banco de preços.

    Silencioso se a tabela ainda não existir: a tela de insumos não pode
    depender de uma migração de Suprimentos ter sido aplicada para abrir.
    """
    from app.apps.erp.db.models.cadastros import PrecoHistorico
    try:
        registros = s.scalars(select(PrecoHistorico)).all()
    except Exception:                                  # pragma: no cover
        return {}

    melhores: dict[int, Any] = {}
    for r in registros:
        insumo_id = getattr(r, "insumo_id", None)
        if not insumo_id:
            continue
        atual = melhores.get(insumo_id)
        if atual is None or _data_do_preco(r) >= _data_do_preco(atual):
            melhores[insumo_id] = r

    saida = {}
    for insumo_id, r in melhores.items():
        tipo = getattr(r, "tipo", None)
        data = getattr(r, "data", None)
        saida[insumo_id] = {
            "valor": float(getattr(r, "preco_unitario", 0) or 0),
            "data": data.isoformat() if data else None,
            "origem": tipo.value if hasattr(tipo, "value") else tipo,
        }
    return saida


def _data_do_preco(registro: Any) -> tuple:
    """Ordena por data e, no empate do mesmo dia, pelo id — o último lançado
    é o mais recente."""
    data = getattr(registro, "data", None)
    return ((data.toordinal() if data else 0), (getattr(registro, "id", 0) or 0))


def catalogo_para_escolha(s: Session) -> dict[str, Any]:
    """O mínimo que as telas de operação precisam: insumos ativos, categorias
    ativas e unidades ativas — sem arrastar o cadastro inteiro."""
    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}
    insumos = []
    for i in s.scalars(select(Insumo)).all():
        if getattr(i, "ativo", True) is False:
            continue
        insumos.append({"id": i.id, "codigo": i.codigo, "descricao": i.descricao,
                        "unidade": i.unidade or "",
                        "categoria_insumo_id": i.categoria_insumo_id,
                        "categoria_insumo": categorias.get(i.categoria_insumo_id, "")})
    insumos.sort(key=lambda x: _chave(x["descricao"]))
    return {"insumos": insumos,
            "categorias": [c for c in listar_categorias(s) if c["ativo"]],
            "unidades": [u for u in listar_unidades(s) if u["ativo"]]}
