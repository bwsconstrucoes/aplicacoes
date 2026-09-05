# ============================================================================
# ERP — core/importadores/suprimentos.py
# Carga inicial de FORNECEDORES e INSUMOS a partir das planilhas em uso.
#
# Por que CSV e não leitura direta do Google: é o padrão que o ERP já usa para
# obras e plano de contas (`planilhas.py`) — o de-para coluna→campo fica
# explícito, o dono confere a prévia antes de gravar, e nenhum dado de
# fornecedor (CNPJ, e-mail, telefone de pessoa) precisa morar no repositório.
#
# A planilha é exportada aba a aba: Registro de Fornecedores › aba "Registro",
# Cadastro de Insumos › aba "Cadastrar". Os cabeçalhos são aceitos como estão
# lá, com acento e maiúscula — quem exporta não deveria ter de editar arquivo.
#
# Rodar duas vezes NÃO duplica: fornecedor casa por CNPJ/CPF, insumo casa pela
# descrição. O que já existe é atualizado, e o relatório diz quantos foram.
# ============================================================================
from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import fornecedores as svc_forn
from app.apps.erp.core.cadastros.validadores import somente_digitos
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.importadores.planilhas import _ler_csv
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, FornecedorCategoria, FornecedorContato,
    FornecedorPorte, Insumo, InsumoCategoria, Usuario,
)

# Como a planilha escreve o porte → como o sistema guarda
PORTES = {
    "fabrica": FornecedorPorte.FABRICA,
    "rep. de fabrica": FornecedorPorte.REP_FABRICA,
    "rep de fabrica": FornecedorPorte.REP_FABRICA,
    "representante de fabrica": FornecedorPorte.REP_FABRICA,
    "distribuidor": FornecedorPorte.DISTRIBUIDOR,
    "fornecedor local": FornecedorPorte.LOCAL,
    "local": FornecedorPorte.LOCAL,
    "homecenter": FornecedorPorte.HOMECENTER,
    "home center": FornecedorPorte.HOMECENTER,
}

CANAIS = {"email": "EMAIL", "e-mail": "EMAIL",
          "whatsapp": "WHATSAPP", "whats app": "WHATSAPP", "zap": "WHATSAPP"}


def _chave(texto: Optional[str]) -> str:
    """Texto comparável: sem acento, sem caixa, sem espaço sobrando.

    É o que faz "Rep. de Fábrica" e "rep de fabrica" caírem no mesmo lugar, e
    o que evita cadastrar o mesmo insumo duas vezes por causa de um acento.
    """
    bruto = unicodedata.normalize("NFKD", (texto or "").strip().lower())
    sem_acento = "".join(c for c in bruto if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", sem_acento)


def _campo(linha: dict[str, str], *nomes: str) -> str:
    """O primeiro cabeçalho que existir na linha. A planilha muda de nome com
    o tempo; o importador não deveria quebrar por causa disso."""
    for nome in nomes:
        for chave, valor in linha.items():
            if _chave(chave) == _chave(nome):
                return (valor or "").strip()
    return ""


def _lista(texto: str) -> list[str]:
    return [p.strip() for p in re.split(r"[;,/]", texto or "") if p.strip()]


# ---------------------------------------------------------------------------
# Fornecedores
# ---------------------------------------------------------------------------
def importar_fornecedores_csv(s: Session, conteudo: bytes, usuario: Optional[Usuario],
                              simular: bool = False) -> dict[str, Any]:
    """Fornecedores da planilha, com região, porte, canal, categorias e cotador.

    `simular=True` só relata o que aconteceria — é a prévia que o dono confere
    antes de deixar gravar.
    """
    linhas = _ler_csv(conteudo)
    categorias = {_chave(c.nome): c for c in s.scalars(select(InsumoCategoria)).all()}

    criados, atualizados, rejeitados, sem_categoria = 0, 0, [], set()
    for i, ln in enumerate(linhas, start=2):
        razao = _campo(ln, "razão social", "razao social", "fornecedor")
        doc = somente_digitos(_campo(ln, "cnpj/cpf", "cnpj", "cpf", "documento"))
        if not razao and not doc:
            continue                     # linha em branco no fim da planilha
        try:
            if not doc:
                raise ErroValidacao("CNPJ/CPF em branco.")
            tipo = "PJ" if len(doc) == 14 else "PF"
            dados = {
                "tipo_pessoa": tipo, "cnpj_cpf": doc, "razao_social": razao,
                "nome_fantasia": _campo(ln, "nome do fornecedor", "nome fantasia"),
                "email": _campo(ln, "email", "e-mail"),
                "telefone": _campo(ln, "telefone"),
                "municipio": _campo(ln, "cidade", "município", "municipio"),
            }
            forn = svc_forn.obter_por_documento(s, doc)
            if forn is None:
                if simular:
                    criados += 1
                    continue
                forn = svc_forn.criar(s, dados, usuario)
                criados += 1
            else:
                if not simular:
                    for campo in ("nome_fantasia", "email", "telefone", "municipio"):
                        if dados[campo]:
                            setattr(forn, campo, dados[campo])
                atualizados += 1
            if simular:
                continue

            porte = PORTES.get(_chave(_campo(ln, "porte do fornecedor", "porte")))
            if porte:
                forn.porte = porte
            regioes = [r.upper() for r in _lista(_campo(ln, "região de atuação",
                                                        "regiao de atuacao", "região"))]
            if regioes:
                forn.regioes_atuacao = regioes
            canais = [CANAIS[_chave(c)] for c in _lista(_campo(ln, "envio de cotações",
                                                               "envio de cotacoes", "canal"))
                      if _chave(c) in CANAIS]
            if canais:
                forn.canais_cotacao = sorted(set(canais))
            s.flush()

            _ligar_categorias(s, forn, _lista(_campo(ln, "categoria de insumo",
                                                     "categoria")), categorias,
                              sem_categoria)
            _garantir_contato(s, forn,
                              nome=_campo(ln, "contato", "nome do contato"),
                              email=dados["email"], telefone=dados["telefone"])
        except ErroValidacao as e:
            rejeitados.append({"linha": i, "fornecedor": razao or doc, "motivo": str(e)})

    return {"no_arquivo": len(linhas), "criados": criados, "atualizados": atualizados,
            "rejeitados": rejeitados,
            "categorias_nao_encontradas": sorted(sem_categoria),
            "simulacao": simular}


def _ligar_categorias(s: Session, forn: Fornecedor, nomes: list[str],
                      categorias: dict[str, InsumoCategoria], nao_achadas: set) -> None:
    """O que o fornecedor vende. Categoria que não existe é RELATADA, não
    criada: inventar categoria na importação é como a base começa a apodrecer."""
    atuais = {c.categoria_insumo_id for c in s.scalars(
        select(FornecedorCategoria).where(
            FornecedorCategoria.fornecedor_id == forn.id)).all()
        if c.fornecedor_id == forn.id}
    for nome in nomes:
        cat = categorias.get(_chave(nome))
        if cat is None:
            nao_achadas.add(nome)
            continue
        if cat.id not in atuais:
            s.add(FornecedorCategoria(fornecedor_id=forn.id, categoria_insumo_id=cat.id))
            atuais.add(cat.id)


def _garantir_contato(s: Session, forn: Fornecedor, nome: str,
                      email: str, telefone: str) -> None:
    """O cotador da planilha. Sem e-mail e sem telefone não entra: contato que
    não recebe cotação não serve para nada, e o banco recusa."""
    nome = (nome or "").strip()
    if not nome or not (email or telefone):
        return
    ja_tem = [c for c in s.scalars(select(FornecedorContato).where(
        FornecedorContato.fornecedor_id == forn.id)).all()
        if c.fornecedor_id == forn.id and _chave(c.nome) == _chave(nome)]
    if ja_tem:
        return
    s.add(FornecedorContato(fornecedor_id=forn.id, nome=nome,
                            email=email or None, telefone=telefone or None))


# ---------------------------------------------------------------------------
# Insumos
# ---------------------------------------------------------------------------
def importar_insumos_csv(s: Session, conteudo: bytes, usuario: Optional[Usuario],
                         simular: bool = False) -> dict[str, Any]:
    """Insumos da planilha, com a categoria de suprimento e a conta do plano.

    A conta do plano é o que permite o pedido virar previsão de pagamento já
    apropriada — por isso insumo sem conta é aceito, mas contado e relatado.
    """
    linhas = _ler_csv(conteudo)
    categorias = {_chave(c.nome): c for c in s.scalars(select(InsumoCategoria)).all()}
    contas = {_chave(c.descricao): c for c in s.scalars(select(Categoria)).all()}
    existentes = {_chave(i.descricao): i for i in s.scalars(select(Insumo)).all()}

    proximo = _proximo_codigo(s)
    criados, atualizados, rejeitados = 0, 0, []
    sem_categoria, sem_conta = set(), 0
    for i, ln in enumerate(linhas, start=2):
        descricao = _campo(ln, "insumos", "descrição do insumo", "descricao do insumo",
                           "insumo", "descrição", "descricao")
        if not descricao:
            continue
        nome_cat = _campo(ln, "categoria do insumo", "sub-categoria", "categoria")
        nome_conta = _campo(ln, "plano financeiro", "categoria (plano financeiro)",
                            "conta do plano")
        cat = categorias.get(_chave(nome_cat)) if nome_cat else None
        if nome_cat and cat is None:
            sem_categoria.add(nome_cat)
        conta = contas.get(_chave(nome_conta)) if nome_conta else None
        if conta is None:
            sem_conta += 1

        insumo = existentes.get(_chave(descricao))
        if insumo is None:
            criados += 1
            if simular:
                continue
            insumo = Insumo(codigo=f"INS-{proximo:04d}", descricao=descricao.strip())
            proximo += 1
            s.add(insumo)
            existentes[_chave(descricao)] = insumo
        else:
            atualizados += 1
            if simular:
                continue
        if cat is not None:
            insumo.categoria_insumo_id = cat.id
        if conta is not None:
            insumo.categoria_id = conta.id
        unidade = _campo(ln, "und", "unidade")
        if unidade:
            insumo.unidade = unidade.upper()
        s.flush()

    return {"no_arquivo": len(linhas), "criados": criados, "atualizados": atualizados,
            "rejeitados": rejeitados, "sem_conta_do_plano": sem_conta,
            "categorias_nao_encontradas": sorted(sem_categoria), "simulacao": simular}


def _proximo_codigo(s: Session) -> int:
    """Continua a numeração INS-0001 de onde parou, para a carga poder ser
    feita em partes sem colidir."""
    numeros = []
    for insumo in s.scalars(select(Insumo)).all():
        codigo = (getattr(insumo, "codigo", "") or "")
        sufixo = codigo.split("-", 1)[1] if codigo.startswith("INS-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return (max(numeros) + 1) if numeros else 1
