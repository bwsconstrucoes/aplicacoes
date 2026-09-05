# ============================================================================
# ERP — core/suprimentos/exemplo.py
# Um punhado de dados fictícios para simular o fluxo de Suprimentos.
#
# Existe porque testar o módulo exigia cadastrar categoria, insumo, fornecedor
# e solicitação à mão antes de conseguir chegar na primeira cotação — e quem
# vai testar não tem tempo para isso.
#
# Três cuidados fazem a diferença entre "dado de exemplo" e "sujeira na base":
#
#   1. TUDO FICA MARCADO. Os ids do que foi criado ficam guardados em
#      `parametros`. Remover apaga exatamente esses, e mais nada — não há
#      heurística por nome, que erraria no dia em que alguém cadastrar
#      "Cimento CP-II" de verdade.
#   2. NUNCA APAGA O QUE FOI USADO. Se um insumo de exemplo já entrou num
#      pedido de verdade, a remoção é RECUSADA inteira, e nada sai.
#   3. NÃO INVENTA OBRA NEM PESSOA. As solicitações só nascem se já houver
#      obra cadastrada; a obra é da empresa, não do exemplo.
# ============================================================================
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Categoria, CondicaoPagamento, Fornecedor, FornecedorCategoria,
    FornecedorContato, Insumo, InsumoCategoria, Obra,
    Parametro, SuprimentoItem, SuprimentoSolicitacao, UnidadeCompra, Usuario,
)

logger = logging.getLogger(__name__)

CHAVE = "suprimentos_dados_de_exemplo"

# (nome da categoria, [(insumo, unidade, conta do plano)])
CATALOGO: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("Cimento, concreto e argamassa", [
        ("Cimento CP-II-Z 32 saco 50kg", "SC", "3.1.01"),
        ("Argamassa colante AC-III saco 20kg", "SC", "3.1.01"),
        ("Concreto usinado FCK 25 MPa", "M3", "3.1.01"),
    ]),
    ("Agregados", [
        ("Areia média lavada", "M3", "3.1.02"),
        ("Brita 1", "M3", "3.1.02"),
    ]),
    ("Aço e ferragem", [
        ("Aço CA-50 10mm barra 12m", "VR", "3.1.03"),
        ("Arame recozido 18 BWG", "KG", "3.1.03"),
    ]),
    ("Material elétrico", [
        ("Cabo flexível 2,5mm² 750V rolo 100m", "UN", "3.1.08"),
        ("Eletroduto flexível 3/4\" rolo 50m", "UN", "3.1.08"),
    ]),
    ("Material hidráulico", [
        ("Tubo PVC soldável 25mm barra 6m", "VR", "3.1.09"),
        ("Joelho PVC soldável 25mm 90°", "UN", "3.1.09"),
    ]),
    ("Impermeabilizantes e colas", [
        ("Cola/selante PU sachê 800ml", "UN", "3.1.17"),
        ("Manta asfáltica 3mm rolo 10m²", "UN", "3.1.17"),
    ]),
]

# (razão social, fantasia, base do CNPJ, cidade, UF, porte, regiões,
#  categorias que atende, contato)
FORNECEDORES: list[tuple] = [
    ("CIMENTOS DO NORDESTE EXEMPLO LTDA", "CimeNorte", "710000010001",
     "Fortaleza", "CE", "FABRICA", ["CE", "RMF"],
     ["Cimento, concreto e argamassa"], "Ricardo Alves"),
    ("AGREGADOS MARACANAU EXEMPLO LTDA", "Agrega Maracanaú", "710000020001",
     "Maracanaú", "CE", "DISTRIBUIDOR", ["RMF"],
     ["Agregados", "Cimento, concreto e argamassa"], "Sandra Bezerra"),
    ("ACOS E FERRAGENS EXEMPLO LTDA", "Aços Exemplo", "710000030001",
     "Fortaleza", "CE", "REP_FABRICA", ["CE"],
     ["Aço e ferragem"], "Paulo Menezes"),
    ("ELETRICA E HIDRAULICA EXEMPLO LTDA", "EletroHidro", "710000040001",
     "Fortaleza", "CE", "DISTRIBUIDOR", ["CE", "RMF"],
     ["Material elétrico", "Material hidráulico"], "Camila Rocha"),
    ("CONSTRUTUDO HOMECENTER EXEMPLO SA", "ConstruTudo", "710000050001",
     "Fortaleza", "CE", "HOMECENTER", ["RMF"],
     ["Impermeabilizantes e colas", "Material elétrico", "Material hidráulico",
      "Aço e ferragem"], "Atendimento Obras"),
]

# (título da solicitação, prioridade, [(insumo, quantidade, especificação)])
SOLICITACOES: list[tuple[str, str, list[tuple[str, str, Optional[str]]]]] = [
    ("Concretagem da fundação — bloco A", "ALTA", [
        ("Cimento CP-II-Z 32 saco 50kg", "120", None),
        ("Areia média lavada", "18", None),
        ("Brita 1", "22", None),
        ("Aço CA-50 10mm barra 12m", "80", "dobrado conforme projeto"),
    ]),
    ("Alvenaria e contrapiso — 2º pavimento", "NORMAL", [
        ("Argamassa colante AC-III saco 20kg", "60", None),
        ("Cimento CP-II-Z 32 saco 50kg", "40", None),
    ]),
    ("Infra elétrica e hidráulica — pavimento térreo", "NORMAL", [
        ("Cabo flexível 2,5mm² 750V rolo 100m", "12", "cor azul e preto"),
        ("Eletroduto flexível 3/4\" rolo 50m", "8", None),
        ("Tubo PVC soldável 25mm barra 6m", "30", None),
        ("Joelho PVC soldável 25mm 90°", "150", None),
    ]),
    ("Impermeabilização das lajes técnicas", "MEDIA", [
        ("Manta asfáltica 3mm rolo 10m²", "14", None),
        ("Cola/selante PU sachê 800ml", "24", None),
    ]),
]

CONDICOES = [("À vista (exemplo)", 100, []), ("28/56 dias (exemplo)", 0, [28, 56])]

ROTULOS = {
    "insumo_categorias": "categoria(s) de insumo",
    "insumos": "insumo(s)",
    "fornecedores": "fornecedor(es)",
    "condicoes_pagamento": "condição(ões) de pagamento",
    "suprimento_solicitacoes": "solicitação(ões)",
}


# ---------------------------------------------------------------------------
# A marca do que é de exemplo
# ---------------------------------------------------------------------------
def _marcas(s: Session) -> dict[str, list[int]]:
    linha = s.get(Parametro, CHAVE)
    if linha is None or not (linha.valor or "").strip():
        return {}
    try:
        return json.loads(linha.valor)
    except ValueError:                                  # pragma: no cover
        logger.warning("ERP/suprimentos: marca de dados de exemplo ilegível")
        return {}


def _gravar_marcas(s: Session, marcas: dict[str, list], usuario: Usuario) -> None:
    linha = s.get(Parametro, CHAVE)
    if linha is None:
        linha = Parametro(chave=CHAVE, valor="")
        s.add(linha)
    linha.valor = json.dumps(marcas, ensure_ascii=False)
    linha.atualizado_por = usuario.id if usuario else None
    s.flush()


def situacao(s: Session) -> dict[str, Any]:
    """O que a tela mostra antes de o dono decidir apertar qualquer botão."""
    marcas = _marcas(s)
    resumo = [{"chave": k, "rotulo": ROTULOS.get(k, k), "quantos": len(v)}
              for k, v in marcas.items() if v]
    return {"presente": bool(resumo), "resumo": resumo}


# ---------------------------------------------------------------------------
# Criar
# ---------------------------------------------------------------------------
def _digito_cnpj(base: str) -> str:
    """Fecha o CNPJ com os dois dígitos verificadores.

    Sem isso o cadastro recusaria os fornecedores de exemplo — e com razão: o
    validador do ERP é o mesmo para dado de teste e para dado de verdade.
    """
    def calcular(numeros: str, pesos: list[int]) -> str:
        soma = sum(int(d) * p for d, p in zip(numeros, pesos))
        resto = soma % 11
        return "0" if resto < 2 else str(11 - resto)

    d1 = calcular(base, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = calcular(base + d1, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return base + d1 + d2


def _conta(s: Session, codigo: str,
           reserva: Optional[Categoria] = None) -> Optional[Categoria]:
    """A conta do plano com este código, ou a conta de reserva.

    A reserva existe porque o plano da empresa pode ter sido personalizado: se
    "3.1.01" não existir mais, o insumo de exemplo aponta para outra conta de
    compra em vez de ficar de fora — dado de exemplo pela metade não serve
    para simular nada.
    """
    for c in s.scalars(select(Categoria)).all():
        if (getattr(c, "codigo", "") or "") == codigo:
            return c
    return reserva


def criar(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Traz os dados de exemplo. Recusa se já houver — dois conjuntos de
    exemplo na base seria pior do que nenhum."""
    from app.apps.erp.core.suprimentos import cadastro as svc_cad
    from app.apps.erp.core.suprimentos import fornecedores as svc_forn

    if _marcas(s):
        raise ErroValidacao(
            "Os dados de exemplo já estão no sistema. Remova-os antes de "
            "trazer outra vez.")

    # Sem plano financeiro carregado não há conta para os insumos apontarem, e
    # insumo sem conta não vira previsão de pagamento. Melhor recusar inteiro
    # e dizer o que falta do que criar meia dúzia de insumos capengas.
    contas_possiveis = svc_cad.contas_de_compra(s)
    if not contas_possiveis:
        raise ErroValidacao(
            "O plano financeiro ainda não foi carregado. Carregue o plano em "
            "Administração › Configurações antes de trazer os dados de exemplo.")
    reserva = s.get(Categoria, contas_possiveis[0]["id"])

    marcas: dict[str, list] = {k: [] for k in ROTULOS}
    avisos: list[str] = []

    # ---- categorias e insumos
    unidades = {u.codigo for u in s.scalars(select(UnidadeCompra)).all()}
    por_nome: dict[str, InsumoCategoria] = {}
    for nome, itens in CATALOGO:
        categoria = svc_cad.criar_categoria(s, {"nome": nome}, usuario)
        marcas["insumo_categorias"].append(categoria.id)
        por_nome[nome] = categoria
        for descricao, unidade, conta_codigo in itens:
            conta = _conta(s, conta_codigo, reserva)
            if conta.codigo != conta_codigo:
                avisos.append(f"a conta {conta_codigo} não existe neste plano — "
                              f"{descricao} ficou em {conta.codigo}")
            if unidade not in unidades:
                avisos.append(f"unidade {unidade} não cadastrada — "
                              f"{descricao} ficou sem unidade")
            insumo = svc_cad.criar_insumo(s, {
                "descricao": descricao,
                "categoria_insumo_id": categoria.id,
                "categoria_id": conta.id,
                "unidade": unidade if unidade in unidades else ""}, usuario)
            marcas["insumos"].append(insumo.id)

    # ---- fornecedores
    for razao, fantasia, base, cidade, uf, porte, regioes, categorias, contato in FORNECEDORES:
        documento = _digito_cnpj(base)
        if any(f.cnpj_cpf == documento for f in s.scalars(select(Fornecedor)).all()):
            avisos.append(f"{razao} já existia — não foi recriado")
            continue
        forn = svc_forn.criar(s, {
            "tipo_pessoa": "PJ", "cnpj_cpf": documento, "razao_social": razao,
            "nome_fantasia": fantasia, "municipio": cidade, "uf": uf,
            "email": f"cotacao@{fantasia.lower().replace(' ', '')}.exemplo",
            "telefone": "(85) 0000-0000",
            "observacoes": "Fornecedor de exemplo, criado para simulação.",
            "porte": porte, "regioes_atuacao": regioes,
            "canais_cotacao": ["EMAIL", "WHATSAPP"],
            "categorias": [por_nome[c].id for c in categorias if c in por_nome],
            "contato_nome": contato, "contato_funcao": "vendas"}, usuario)
        marcas["fornecedores"].append(forn.id)

    # ---- condições de pagamento
    existentes = {c.nome for c in s.scalars(select(CondicaoPagamento)).all()}
    for nome, entrada, dias in CONDICOES:
        if nome in existentes:
            continue
        c = CondicaoPagamento(nome=nome, entrada_percentual=Decimal(entrada),
                              dias=dias, ordem=90)
        s.add(c)
        s.flush()
        marcas["condicoes_pagamento"].append(c.id)

    # ---- solicitações (só se houver obra: obra é da empresa, não do exemplo)
    obras = [o for o in s.scalars(select(Obra)).all()
             if getattr(o, "status", None) != "ENCERRADA"]
    if not obras:
        avisos.append("nenhuma obra cadastrada — as solicitações de exemplo "
                      "não foram criadas")
    else:
        marcas["suprimento_solicitacoes"] = _criar_solicitacoes(s, obras, usuario)

    _gravar_marcas(s, marcas, usuario)
    registrar_evento(s, "suprimentos", 0, "DADOS_DE_EXEMPLO_CRIADOS",
                     {k: len(v) for k, v in marcas.items()},
                     usuario.id if usuario else None)
    return {"marcas": {k: len(v) for k, v in marcas.items()}, "avisos": avisos}


def _criar_solicitacoes(s: Session, obras: list[Obra], usuario: Usuario) -> list[int]:
    from app.apps.erp.core.suprimentos import solicitacao as svc

    por_descricao = {i.descricao: i for i in s.scalars(select(Insumo)).all()}
    criadas = []
    hoje = date.today()
    for posicao, (titulo, prioridade, itens) in enumerate(SOLICITACOES):
        obra = obras[posicao % len(obras)]
        linhas = []
        for descricao, quantidade, especificacao in itens:
            insumo = por_descricao.get(descricao)
            if insumo is None:
                continue
            linhas.append({"insumo_id": insumo.id, "quantidade": quantidade,
                           "unidade": insumo.unidade, "obra_id": obra.id,
                           "especificacao": especificacao})
        if not linhas:
            continue
        sol = svc.criar(s, {
            "titulo": titulo, "prioridade": prioridade,
            "previsao_entrega": (hoje + timedelta(days=7 + posicao * 5)).isoformat(),
            "observacoes": "Solicitação de exemplo, criada para simulação.",
            "itens": linhas}, usuario)
        criadas.append(sol.id)
    return criadas


# ---------------------------------------------------------------------------
# Remover
# ---------------------------------------------------------------------------
def remover(s: Session, usuario: Usuario) -> dict[str, Any]:
    """Apaga exatamente o que foi criado — e recusa inteiro se algo já foi
    usado de verdade. Meia remoção é pior do que nenhuma."""
    marcas = _marcas(s)
    if not marcas:
        raise ErroValidacao("Não há dados de exemplo para remover.")

    presos = _o_que_ja_foi_usado(s, marcas)
    if presos:
        raise ErroValidacao(
            "Não dá para remover: " + "; ".join(presos) +
            ". Zere o movimento (Configurações › Zerar dados de teste) antes.")

    saiu: dict[str, int] = {}

    for sol_id in marcas.get("suprimento_solicitacoes", []):
        sol = s.get(SuprimentoSolicitacao, sol_id)
        if sol is not None:
            s.delete(sol)                        # os itens saem em cascata
            saiu["suprimento_solicitacoes"] = saiu.get("suprimento_solicitacoes", 0) + 1
    s.flush()

    for forn_id in marcas.get("fornecedores", []):
        forn = s.get(Fornecedor, forn_id)
        if forn is None:
            continue
        for v in [x for x in s.scalars(select(FornecedorCategoria)).all()
                  if x.fornecedor_id == forn_id]:
            s.delete(v)
        for c in [x for x in s.scalars(select(FornecedorContato)).all()
                  if x.fornecedor_id == forn_id]:
            s.delete(c)
        s.flush()
        s.delete(forn)
        saiu["fornecedores"] = saiu.get("fornecedores", 0) + 1
    s.flush()

    for insumo_id in marcas.get("insumos", []):
        insumo = s.get(Insumo, insumo_id)
        if insumo is not None:
            s.delete(insumo)
            saiu["insumos"] = saiu.get("insumos", 0) + 1
    s.flush()

    for categoria_id in marcas.get("insumo_categorias", []):
        categoria = s.get(InsumoCategoria, categoria_id)
        if categoria is not None:
            s.delete(categoria)
            saiu["insumo_categorias"] = saiu.get("insumo_categorias", 0) + 1

    for condicao_id in marcas.get("condicoes_pagamento", []):
        condicao = s.get(CondicaoPagamento, condicao_id)
        if condicao is not None:
            s.delete(condicao)
            saiu["condicoes_pagamento"] = saiu.get("condicoes_pagamento", 0) + 1
    s.flush()

    linha = s.get(Parametro, CHAVE)
    if linha is not None:
        s.delete(linha)
    registrar_evento(s, "suprimentos", 0, "DADOS_DE_EXEMPLO_REMOVIDOS", saiu,
                     usuario.id if usuario else None)
    return {"removidos": saiu}


def _o_que_ja_foi_usado(s: Session, marcas: dict[str, list[int]]) -> list[str]:
    """Insumo de exemplo citado numa solicitação de VERDADE, ou fornecedor de
    exemplo com título/cotação em cima. Nesses casos a remoção não acontece."""
    presos: list[str] = []
    insumos = set(marcas.get("insumos", []))
    nossas = set(marcas.get("suprimento_solicitacoes", []))

    if insumos:
        de_fora = [i for i in s.scalars(select(SuprimentoItem)).all()
                   if i.insumo_id in insumos and i.solicitacao_id not in nossas]
        if de_fora:
            presos.append(f"{len(de_fora)} item(ns) de solicitação de verdade "
                          f"usam insumos de exemplo")

    fornecedores = set(marcas.get("fornecedores", []))
    if fornecedores:
        for modelo, rotulo in _onde_fornecedor_aparece():
            try:
                usados = [x for x in s.scalars(select(modelo)).all()
                          if getattr(x, "fornecedor_id", None) in fornecedores]
            except Exception:                           # pragma: no cover
                continue
            if usados:
                presos.append(f"{len(usados)} {rotulo} de fornecedor de exemplo")
    return presos


def _onde_fornecedor_aparece():
    """Onde um fornecedor deixa rastro. Importado aqui dentro porque nem toda
    tabela existe antes das migrações de Suprimentos."""
    from app.apps.erp.db.models.cadastros import CotacaoFornecedor, PedidoCompra
    from app.apps.erp.db.models.financeiro import Titulo
    return [(Titulo, "título(s)"), (PedidoCompra, "pedido(s) de compra"),
            (CotacaoFornecedor, "coluna(s) de mapa de cotação")]
