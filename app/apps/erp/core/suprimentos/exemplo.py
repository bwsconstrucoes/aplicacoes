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
import re
import unicodedata
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

# ---------------------------------------------------------------------------
# TUDO ABAIXO SAIU DAS PLANILHAS DA EMPRESA, não foi inventado.
#
#   - insumos, categorias, unidades e contas: "Cadastro de Insumos", aba
#     Cadastrar, e a coluna Plano Financeiro da "Solicitação de Suprimentos";
#   - as cinco solicitações: "Solicitação de Suprimentos", aba Pedidos, com o
#     material, a especificação, a quantidade e a obra como foram pedidos de
#     verdade;
#   - só os FORNECEDORES são fictícios, e de propósito. Usar os fornecedores
#     de verdade aqui seria perigoso: no dia em que a carga da planilha
#     atualizasse um deles, "remover os dados de exemplo" apagaria um
#     cadastro que virou real. Os 111 de verdade entram pela importação.
# ---------------------------------------------------------------------------

# O plano financeiro da planilha é o ANTIGO, com nome em vez de código. Aqui
# está a tradução para o plano do ERP. É a única parte deste arquivo que é
# opinião minha, e vale o dono conferir: errar aqui joga a compra na conta de
# custo errada, e o erro só aparece no relatório do mês.
PLANO_DA_PLANILHA = {
    # Um para um: cada nome da planilha da BWS tem a SUA conta.
    #
    # Até 07/09/2026 este mapa juntava categorias — argamassa caía em cimento,
    # vidro caía em esquadria, gás caía em hidráulica. O dono recusou: "as
    # nomenclaturas estão de acordo com a nossa realidade, as pessoas que
    # lançam já estão acostumadas com elas". As contas que faltavam foram
    # criadas (3.1.21 a 3.1.27) e este mapa deixou de misturar.
    "Agregados (Areia, Brita, Arisco)": "3.1.02",
    "Argamassas": "3.1.21",
    "Armadura": "3.1.03",
    "Bancadas de Granito": "3.1.25",
    "Cimento e Concreto Usinado": "3.1.01",
    "EPI (Equipamento de Proteção Individual)": "3.4.03",
    "Elementos de Vedação (Tijolo, Blocos e Paredes PVC)": "3.1.04",
    "Esquadrias de Alumínio, Metal e Madeira": "3.1.14",
    "Estrutura Metálica": "3.1.22",
    "Ferramentas": "3.1.19",
    "Impermeabilizantes, Aditivos e Colas": "3.1.17",
    "Jardinagem": "3.1.20",
    "Locação de Máquinas, Veículos e Equipamentos": "3.3.01",
    "Louças e Metais": "3.1.13",
    "Madeiramento": "3.1.07",
    "Materais p/ Serralheria (Tubos, Metalon, Perfis, etc)": "3.1.26",
    "Material Elétrico": "3.1.08",
    "Material Hidráulico e Sanitário": "3.1.09",
    "Material p/ Cabeamento Estruturado e CFTV": "3.1.23",
    "Material p/ Climatização": "3.1.11",
    "Material p/ Combate à Incêndio": "3.1.10",
    "Material p/ Fôrro": "3.1.16",
    "Material p/ Gás": "3.1.24",
    "Material p/ Limpeza": "3.2.05",
    "Material para Pintura": "3.1.15",
    "Móveis e Utensílios": "8.1.03",
    "Outros Materiais": "3.1.99",
    "Parafusos, Ferragens e Acessórios": "3.1.18",
    "Pisos, Cerâmicas e Revestimentos": "3.1.12",
    "Pré-Moldados de Concreto": "3.1.05",
    "Telhas e Material p/ Coberturas": "3.1.06",
    "Vidros e Espelhos": "3.1.27",
}

# (categoria de insumo, [(insumo, unidade, conta do plano da planilha)])
# São as categorias e os insumos que as cinco solicitações abaixo citam.
CATALOGO: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("Ferramentas Manuais", [
        ("Torquês 12 pol.", "UN", "Ferramentas"),
        ("Facão", "UN", "Ferramentas"),
        ("Espátula 8cm", "UN", "Ferramentas"),
        ("Aplicador p/ Silicone", "UN", "Ferramentas"),
        ("Pá de Bico c/ Cabo", "UN", "Ferramentas"),
        ("Enxada c/ Cabo", "UN", "Ferramentas"),
        ("Régua de Alumínio", "UN", "Parafusos, Ferragens e Acessórios"),
    ]),
    ("Armadura e Serralheria", [
        ("Vergalhão CA50 8.0mm", "KG", "Armadura"),
        ("Vergalhão CA50 12.5mm", "UN", "Armadura"),
    ]),
    ("Esquadrias de Madeira", [
        ("Alisar p/ Porta", "UN", "Esquadrias de Alumínio, Metal e Madeira"),
    ]),
    ("Pintura", [
        ("Trincha 3 pol.", "UN", "Material para Pintura"),
        ("Trincha 4 pol.", "UN", "Material para Pintura"),
    ]),
    ("Impermeabilização", [
        ("Tela de Fibra Sintética p/ Impermeabilização", "UN",
         "Impermeabilizantes, Aditivos e Colas"),
    ]),
    ("Suplementos", [
        ("Tarucel p/ Junta de Dilatação", "M", "Pisos, Cerâmicas e Revestimentos"),
        ("Cola/Selante PU Sache 800ml", "UN", "Impermeabilizantes, Aditivos e Colas"),
        ("Câmara de Ar Carrinho de Mão 3,25 x 8 pol.", "UN", "Ferramentas"),
        ("Pneu p/ Carrinho de Mão", "UN", "Ferramentas"),
    ]),
    ("Madeiramento e Fôrma", [
        ("Barrote em Pinus 5x5cm", "UN", "Madeiramento"),
        ("Madeirite Resinado (2,20 x 1,10m) E=10mm", "UN", "Madeiramento"),
        ("Tábua de Pinus L=30cm E=2.5cm", "UN", "Madeiramento"),
    ]),
    ("Parafusos", [
        ("Prego com Cabeça 2.1/2 x 10 (18x27mm)", "KG",
         "Parafusos, Ferragens e Acessórios"),
    ]),
    ("Cobertura em Estrutura Metálica", [
        ("Telha Metálica Termoacústica", "UN", "Telhas e Material p/ Coberturas"),
    ]),
    ("Esgoto", [
        ("Tubo PVC de Esgoto 75mm", "UN", "Material Hidráulico e Sanitário"),
        ("Tubo PVC de Esgoto 50mm", "UN", "Material Hidráulico e Sanitário"),
        ("Tubo PVC de Esgoto 40mm", "UN", "Material Hidráulico e Sanitário"),
        ("Caixa Sifonada c/ Três Entradas (c/ Tampa Quadrada) 100x150x50mm", "UN",
         "Material Hidráulico e Sanitário"),
        ("Joelho PVC de Esgoto Simples 90° 100mm", "UN", "Material Hidráulico e Sanitário"),
        ("Junção PVC de Esgoto c/ redução 100 x 75mm", "UN",
         "Material Hidráulico e Sanitário"),
    ]),
]

# (razão social, fantasia, base do CNPJ, cidade, UF, porte, regiões,
#  categorias que atende, contato)
# Fictícios de propósito — ver a nota no topo. O nome traz "EXEMPLO" para
# ninguém confundir com fornecedor de verdade na tela.
FORNECEDORES: list[tuple] = [
    ("FERRAGENS E FERRAMENTAS EXEMPLO LTDA", "FerraExemplo", "710000010001",
     "Fortaleza", "CE", "DISTRIBUIDOR", ["CE", "RMF"],
     ["Ferramentas Manuais", "Parafusos", "Suplementos"], "Ricardo Alves"),
    ("ACOS E ARMADURAS EXEMPLO LTDA", "Aços Exemplo", "710000020001",
     "Fortaleza", "CE", "REP_FABRICA", ["CE"],
     ["Armadura e Serralheria"], "Paulo Menezes"),
    ("MADEIREIRA E COBERTURAS EXEMPLO LTDA", "Madeira Exemplo", "710000030001",
     "Caucaia", "CE", "DISTRIBUIDOR", ["RMF"],
     ["Madeiramento e Fôrma", "Cobertura em Estrutura Metálica",
      "Esquadrias de Madeira"], "Sandra Bezerra"),
    ("HIDRAULICA E SANEAMENTO EXEMPLO LTDA", "HidroExemplo", "710000040001",
     "Recife", "PE", "DISTRIBUIDOR", ["PE"],
     ["Esgoto"], "Camila Rocha"),
    ("CONSTRUTUDO HOMECENTER EXEMPLO SA", "ConstruTudo Exemplo", "710000050001",
     "Fortaleza", "CE", "HOMECENTER", ["CE", "RMF", "PE"],
     ["Ferramentas Manuais", "Pintura", "Impermeabilização", "Suplementos",
      "Parafusos", "Esgoto"], "Atendimento Obras"),
]

# (título, prioridade, código da obra na planilha,
#  [(insumo, quantidade, especificação)])
# Cinco pedidos de verdade da aba Pedidos, com o material, a quantidade e a
# obra como foram pedidos. O título é meu — a planilha não tem esse campo, e
# é justamente ele que torna o pedido localizável no ERP depois.
SOLICITACOES: list[tuple[str, str, str, list[tuple[str, str, Optional[str]]]]] = [
    ("Ferramental e acabamento — reposição da obra", "NORMAL", "CREPETRIUNFO", [
        ("Torquês 12 pol.", "1", "torquês 10, armador"),
        ("Vergalhão CA50 12.5mm", "14", None),
        ("Alisar p/ Porta", "4", "kit alisagem"),
        ("Facão", "1", None),
        ("Trincha 3 pol.", "2", None),
        ("Trincha 4 pol.", "2", None),
        ("Espátula 8cm", "2", None),
    ]),
    ("Ferramental e tela para o reboco", "NORMAL", "MERCADOBARBALHA", [
        ("Régua de Alumínio", "10", "6 metros"),
        ("Pá de Bico c/ Cabo", "5", "pá redonda"),
        ("Enxada c/ Cabo", "2", None),
        ("Tela de Fibra Sintética p/ Impermeabilização", "3", "tela PVC para reboco, rolo"),
        ("Câmara de Ar Carrinho de Mão 3,25 x 8 pol.", "15", None),
        ("Pneu p/ Carrinho de Mão", "10", None),
    ]),
    ("Fôrmas, madeiramento e tapume", "ALTA", "ESCPLANALTO", [
        ("Barrote em Pinus 5x5cm", "210", None),
        ("Madeirite Resinado (2,20 x 1,10m) E=10mm", "50", None),
        ("Tábua de Pinus L=30cm E=2.5cm", "50", None),
        ("Prego com Cabeça 2.1/2 x 10 (18x27mm)", "20", None),
        ("Telha Metálica Termoacústica", "120", "para tapume com 6,00 m"),
    ]),
    ("Armadura e rede de esgoto", "ALTA", "IFPESANTACRUZ", [
        ("Vergalhão CA50 8.0mm", "430", None),
        ("Tubo PVC de Esgoto 75mm", "2", None),
        ("Tubo PVC de Esgoto 50mm", "4", None),
        ("Tubo PVC de Esgoto 40mm", "2", "45mm"),
        ("Caixa Sifonada c/ Três Entradas (c/ Tampa Quadrada) 100x150x50mm", "2", None),
        ("Joelho PVC de Esgoto Simples 90° 100mm", "9", None),
        ("Junção PVC de Esgoto c/ redução 100 x 75mm", "8", "junção 100mm x 100mm"),
    ]),
    ("Junta de dilatação do piso", "MEDIA", "CREPETERRA", [
        ("Tarucel p/ Junta de Dilatação", "180", "6mm"),
        ("Cola/Selante PU Sache 800ml", "12", "cinza"),
        ("Aplicador p/ Silicone", "1", "800ml"),
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


def _apelido(nome: str) -> str:
    """Nome de fantasia → pedaço de endereço de e-mail.

    Sem acento e sem cedilha: "Aços Exemplo" virava
    "cotacao@açosexemplo.exemplo", que não é endereço de e-mail nenhum.
    """
    sem_acento = unicodedata.normalize("NFKD", nome or "")
    limpo = "".join(c for c in sem_acento if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", limpo.lower()) or "fornecedor"


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
        for descricao, unidade, plano_da_planilha in itens:
            conta_codigo = PLANO_DA_PLANILHA.get(plano_da_planilha, "3.1.99")
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
            "email": f"cotacao@{_apelido(fantasia)}.exemplo",
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
    # A planilha diz para qual obra cada pedido foi. Se essa obra existir no
    # ERP, o pedido cai nela — e a simulação fica igual ao que aconteceu. Se
    # não existir, cai em qualquer obra ativa, só para o exemplo não morrer.
    por_codigo = {(o.codigo or "").strip().upper(): o for o in obras}
    criadas = []
    hoje = date.today()
    for posicao, (titulo, prioridade, codigo_obra, itens) in enumerate(SOLICITACOES):
        obra = por_codigo.get(codigo_obra.upper()) or obras[posicao % len(obras)]
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
