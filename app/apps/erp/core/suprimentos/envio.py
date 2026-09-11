# ============================================================================
# ERP — core/suprimentos/envio.py
# Disparar a cotação para os fornecedores do mapa, e provar que disparou.
#
# Era o buraco que sobrava do módulo: o mapa ficava pronto e alguém copiava o
# texto na mão para o e-mail. 109 dos 111 fornecedores da BWS só recebem
# cotação por e-mail.
#
# TRÊS DECISÕES QUE VALEM SER ENTENDIDAS
#
# 1. QUEM MANDA É A EMPRESA DA OBRA. A BWS opera com mais de um CNPJ, e o
#    fornecedor responde para quem mandou. Se a cotação tem itens de obras de
#    empresas diferentes, o comprador escolhe — e a tela diz por quê, em vez
#    de o sistema sortear.
#
# 2. CADA FORNECEDOR RECEBE SÓ A LISTA, NUNCA O MAPA. Mandar o mapa seria
#    entregar ao fornecedor A o preço do fornecedor B. O corpo do e-mail é a
#    lista de itens com quantidade e unidade, e mais nada.
#
# 3. UM FORA DO AR NÃO DERRUBA OS OUTROS. Cada fornecedor é um envio e um
#    registro: quem saiu, saiu; quem falhou fica com o motivo escrito e o
#    botão de reenviar. Meia cotação enviada é melhor do que nenhuma, desde
#    que a tela diga qual metade.
#
# O QUE O REGISTRO PROVA, E O QUE NÃO PROVA
#
# Prova que o servidor de saída aceitou a mensagem, com data, hora, para quem
# e o texto exato. NÃO prova entrega, e muito menos leitura. Quem quiser essa
# certeza precisa de um serviço de entrega com retorno — é outra decisão, e
# outro custo. A tela repete isso com estas palavras.
# ============================================================================
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import empresas as svc_empresas
from app.apps.erp.core.comum import email as correio
from app.apps.erp.core.suprimentos.entrega import endereco_da_obra, rotulo_da_obra
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Cotacao, CotacaoFornecedor, CotacaoItem, Empresa, Fornecedor,
    FornecedorContato, Insumo, Obra, StatusCotacao, SuprimentoItem, Usuario,
)

logger = logging.getLogger(__name__)


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def _quantidade(valor: Any) -> str:
    """"14.000" no banco é catorze, não catorze mil. Some a casa decimal que
    não existe — o fornecedor lê este número e faz proposta em cima dele."""
    try:
        numero = float(str(valor))
    except (TypeError, ValueError):
        return str(valor or "")
    inteiro = int(numero)
    texto = f"{numero:.3f}".rstrip("0").rstrip(".")
    return str(inteiro) if numero == inteiro else texto.replace(".", ",")


# ---------------------------------------------------------------------------
# De qual empresa esta cotação sai
# ---------------------------------------------------------------------------
def empresas_possiveis(s: Session, cotacao_id: int) -> dict[str, Any]:
    """As empresas das obras que aparecem na cotação.

    Quando é uma só, o disparo nem pergunta. Quando são várias, o comprador
    escolhe — porque a decisão "por qual CNPJ esta compra corre" é dele, não
    do sistema.
    """
    itens = [x for x in s.scalars(select(CotacaoItem)).all()
             if x.cotacao_id == cotacao_id]
    obras_ids = set()
    for linha in itens:
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is not None and item.obra_id:
            obras_ids.add(item.obra_id)

    achadas: dict[int, dict[str, Any]] = {}
    sem_empresa = []
    for obra_id in sorted(obras_ids):
        obra = s.get(Obra, obra_id)
        if obra is None:
            continue
        if not getattr(obra, "empresa_id", None):
            sem_empresa.append(getattr(obra, "codigo", str(obra_id)))
            continue
        empresa = s.get(Empresa, obra.empresa_id)
        if empresa is None:
            continue
        registro = achadas.setdefault(empresa.id, {
            "id": empresa.id, "razao_social": empresa.razao_social,
            "cnpj": empresa.cnpj, "obras": []})
        registro["obras"].append(getattr(obra, "codigo", str(obra_id)))

    padrao = svc_empresas.padrao(s)
    lista = sorted(achadas.values(), key=lambda e: e["razao_social"])
    return {
        "empresas": lista,
        "obras_sem_empresa": sorted(sem_empresa),
        "sugerida": (lista[0]["id"] if len(lista) == 1
                     else (padrao.id if padrao else None)),
        "precisa_escolher": len(lista) > 1,
    }


def _empresa_do_disparo(s: Session, cotacao_id: int,
                        empresa_id: Optional[int]) -> Empresa:
    if empresa_id:
        return svc_empresas.obter(s, int(empresa_id))
    possiveis = empresas_possiveis(s, cotacao_id)
    if possiveis["precisa_escolher"]:
        nomes = ", ".join(e["razao_social"] for e in possiveis["empresas"])
        raise ErroValidacao(
            f"Esta cotação tem itens de obras de empresas diferentes ({nomes}). "
            f"Escolha por qual empresa ela sai — o fornecedor responde para "
            f"quem mandou.")
    if not possiveis["sugerida"]:
        raise ErroValidacao(
            "Não há empresa definida para esta cotação. Ligue a obra a uma "
            "empresa em Administração › Empresas, ou marque uma empresa como "
            "padrão.")
    return svc_empresas.obter(s, possiveis["sugerida"])


# ---------------------------------------------------------------------------
# O texto que o fornecedor recebe
# ---------------------------------------------------------------------------
def _blocos_de_entrega(s: Session, linhas: list) -> dict[str, dict[str, Any]]:
    """Os itens agrupados pelo endereço em que descem, na ordem do documento."""
    blocos: dict[str, dict[str, Any]] = {}
    for linha in linhas:
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is None:
            continue
        insumo = s.get(Insumo, item.insumo_id)
        obra = s.get(Obra, item.obra_id) if item.obra_id else None
        endereco = endereco_da_obra(obra)
        bloco = blocos.setdefault(endereco, {"endereco": endereco,
                                             "obras": [], "itens": []})
        rotulo = rotulo_da_obra(obra)
        if rotulo and rotulo not in bloco["obras"]:
            bloco["obras"].append(rotulo)
        bloco["itens"].append((item, insumo))
    return blocos


def _por_endereco(s: Session, linhas: list) -> list[dict[str, Any]]:
    return list(_blocos_de_entrega(s, linhas).values())


def montar_mensagem(s: Session, cotacao_id: int, empresa: Empresa,
                    *, prazo: Optional[str] = None,
                    observacao: str = "") -> dict[str, str]:
    """Assunto e corpo. Sem preço de ninguém — só o que se quer comprar."""
    cot = s.get(Cotacao, cotacao_id)
    if cot is None:
        raise ErroNaoEncontrado("Cotação não encontrada.")

    linhas = sorted([x for x in s.scalars(select(CotacaoItem)).all()
                     if x.cotacao_id == cotacao_id],
                    key=lambda x: x.numero or 0)
    if not linhas:
        raise ErroValidacao("A cotação não tem item nenhum para cotar.")

    corpo = [f"Prezados,", "",
             f"Solicitamos proposta para os itens abaixo.", ""]
    # Agrupado por ENDEREÇO DE ENTREGA: o frete depende da distância, e pedir
    # preço sem dizer onde entregar é receber um preço que muda depois.
    # A ESPECIFICAÇÃO vai junto de cada item — é ela que diz qual variação do
    # material se quer, e sem ela o fornecedor cota o que quiser.
    numero_do_item = 0
    for bloco in _por_endereco(s, linhas):
        corpo.append(f"ENTREGAR EM: {bloco['endereco']}")
        if bloco["obras"]:
            corpo.append(f"Obra(s): {' · '.join(bloco['obras'])}")
        for item, insumo in bloco["itens"]:
            numero_do_item += 1
            descricao = _texto(getattr(insumo, "descricao", "")) or "item"
            especificacao = _texto(getattr(item, "especificacao", ""))
            corpo.append(
                f"{numero_do_item:>3}. {descricao}"
                f"{f' — {especificacao}' if especificacao else ''}"
                f"   [{_quantidade(getattr(item, 'quantidade', ''))} "
                f"{_texto(getattr(item, 'unidade', ''))}]")
        corpo.append("")

    corpo += ["Pedimos que a proposta informe, para cada item:",
              "  · preço unitário", "  · prazo de entrega",
              "  · condição de pagamento", "  · valor do frete, se houver", ""]
    if prazo:
        corpo.append(f"Pedimos retorno até {prazo}.")
        corpo.append("")
    if _texto(observacao):
        corpo += [_texto(observacao), ""]

    corpo.append(f"Referência desta cotação: {cot.numero}. "
                 f"Favor mantê-la ao responder.")
    corpo += ["", "Atenciosamente,"] + _assinatura(empresa)

    return {"assunto": f"Cotação {cot.numero} — {cot.titulo}",
            "corpo": "\n".join(corpo)}


def _cnpj_por_extenso(cnpj: Any) -> str:
    """71000001000184 → 71.000.001/0001-84. O fornecedor confere este número
    contra o cadastro dele; sem pontuação, ninguém lê catorze dígitos seguidos.

    A conta em si mora em `core/comum/formato.py` desde 11/09/2026 — a leitura
    de documento anexado precisou da mesma, e duas cópias divergem.
    """
    from app.apps.erp.core.comum.formato import documento_por_extenso
    formatado = documento_por_extenso(cnpj)
    return formatado if formatado else _texto(cnpj)


def _assinatura(empresa: Empresa) -> list[str]:
    """Quem está pedindo, com CNPJ e endereço. O fornecedor precisa disso para
    faturar — e, no pedido, é o que identifica por qual CNPJ a compra corre."""
    linhas = [_texto(empresa.nome_fantasia) or _texto(empresa.razao_social)]
    if _texto(empresa.nome_fantasia) and _texto(empresa.razao_social) and \
            _texto(empresa.nome_fantasia) != _texto(empresa.razao_social):
        linhas.append(_texto(empresa.razao_social))
    if _texto(empresa.cnpj):
        linhas.append(f"CNPJ {_cnpj_por_extenso(empresa.cnpj)}")
    endereco = ", ".join(x for x in [_texto(empresa.logradouro),
                                     _texto(empresa.numero),
                                     _texto(empresa.bairro),
                                     _texto(empresa.municipio),
                                     _texto(empresa.uf)] if x)
    if endereco:
        linhas.append(endereco)
    if _texto(empresa.telefone):
        linhas.append(_texto(empresa.telefone))
    return linhas


def destinos_do_fornecedor(s: Session, fornecedor_id: int) -> list[str]:
    """Para onde a cotação vai: os contatos marcados para receber cotação e,
    na falta deles, o e-mail do próprio fornecedor."""
    enderecos = []
    for c in s.scalars(select(FornecedorContato)).all():
        if c.fornecedor_id != fornecedor_id:
            continue
        if getattr(c, "recebe_cotacao", True) is False:
            continue
        if getattr(c, "ativo", True) is False:
            continue
        if _texto(c.email):
            enderecos.append(_texto(c.email))
    if not enderecos:
        forn = s.get(Fornecedor, fornecedor_id)
        if forn is not None and _texto(forn.email):
            enderecos.append(_texto(forn.email))
    # sem repetir, mantendo a ordem
    return list(dict.fromkeys(enderecos))


# ---------------------------------------------------------------------------
# O disparo
# ---------------------------------------------------------------------------
def preparar(s: Session, cotacao_id: int) -> dict[str, Any]:
    """O que a tela mostra ANTES de o comprador apertar o botão: quem recebe,
    quem não tem endereço, de qual empresa sai e o texto que vai."""
    possiveis = empresas_possiveis(s, cotacao_id)
    empresa = None
    if possiveis["sugerida"]:
        empresa = s.get(Empresa, possiveis["sugerida"])

    colunas = sorted([x for x in s.scalars(select(CotacaoFornecedor)).all()
                      if x.cotacao_id == cotacao_id],
                     key=lambda x: (x.ordem or 0, x.id or 0))
    # O histórico vem do mais novo para o mais velho, então o primeiro que
    # aparece de cada fornecedor é o último envio para ele.
    ultimo_por_coluna: dict[int, dict[str, Any]] = {}
    for e in correio.historico(s, "cotacao", cotacao_id):
        if e["destinatario_tipo"] != "cotacao_fornecedor":
            continue
        ultimo_por_coluna.setdefault(e["destinatario_id"], e)

    destinatarios = []
    for coluna in colunas:
        forn = s.get(Fornecedor, coluna.fornecedor_id)
        enderecos = destinos_do_fornecedor(s, coluna.fornecedor_id)
        ultimo = ultimo_por_coluna.get(coluna.id)
        destinatarios.append({
            "coluna_id": coluna.id, "fornecedor_id": coluna.fornecedor_id,
            "fornecedor": getattr(forn, "razao_social", ""),
            "para": enderecos,
            "pode": bool(enderecos),
            "motivo": ("" if enderecos else
                       "sem e-mail no cadastro — corrija em Cadastros › "
                       "Fornecedores, ou mande por outro canal"),
            "ultimo_envio": ultimo,
        })

    pode_empresa, falta = correio.conta_configurada(empresa)
    mensagem = (montar_mensagem(s, cotacao_id, empresa) if empresa else
                {"assunto": "", "corpo": ""})
    return {
        "empresa": ({"id": empresa.id, "razao_social": empresa.razao_social,
                     "cnpj": empresa.cnpj,
                     "remetente": correio.remetente_de(empresa)}
                    if empresa else None),
        "empresas_possiveis": possiveis["empresas"],
        "precisa_escolher_empresa": possiveis["precisa_escolher"],
        "obras_sem_empresa": possiveis["obras_sem_empresa"],
        "conta_pronta": pode_empresa, "o_que_falta": falta,
        "destinatarios": destinatarios,
        "assunto": mensagem["assunto"], "corpo": mensagem["corpo"],
        "aviso": ("O sistema registra que a mensagem foi ACEITA pelo servidor "
                  "de saída, com data, hora e o texto exato. Isso não é o "
                  "mesmo que entregue, e muito menos lido."),
    }


def disparar(s: Session, cotacao_id: int, dados: dict[str, Any],
             usuario: Usuario) -> dict[str, Any]:
    """Manda a cotação. Um envio e um registro por fornecedor."""
    cot = s.get(Cotacao, cotacao_id)
    if cot is None:
        raise ErroNaoEncontrado("Cotação não encontrada.")
    if cot.status is not StatusCotacao.ABERTA:
        raise ErroValidacao(
            f"Esta cotação está {cot.status.value.lower()} e não recebe mais "
            f"proposta. Abra uma cotação nova.")

    empresa = _empresa_do_disparo(s, cotacao_id, dados.get("empresa_id"))
    pode, falta = correio.conta_configurada(empresa)
    if not pode:
        raise ErroValidacao(falta)

    escolhidas = [int(x) for x in (dados.get("colunas") or [])]
    colunas = [c for c in s.scalars(select(CotacaoFornecedor)).all()
               if c.cotacao_id == cotacao_id
               and (not escolhidas or c.id in escolhidas)]
    if not colunas:
        raise ErroValidacao(
            "Nenhum fornecedor escolhido. Acrescente fornecedores ao mapa antes "
            "de disparar.")

    mensagem = montar_mensagem(s, cotacao_id, empresa,
                               prazo=_texto(dados.get("prazo")) or None,
                               observacao=dados.get("observacao") or "")

    enviados, falhas, sem_endereco = [], [], []
    for coluna in sorted(colunas, key=lambda c: (c.ordem or 0, c.id or 0)):
        forn = s.get(Fornecedor, coluna.fornecedor_id)
        nome = getattr(forn, "razao_social", f"fornecedor {coluna.fornecedor_id}")
        destinos = destinos_do_fornecedor(s, coluna.fornecedor_id)
        if not destinos:
            sem_endereco.append(nome)
            continue
        registro = correio.enviar(
            s, empresa=empresa, para=destinos,
            assunto=mensagem["assunto"], corpo=mensagem["corpo"],
            entidade_tipo="cotacao", entidade_id=cotacao_id,
            destinatario_tipo="cotacao_fornecedor", destinatario_id=coluna.id,
            usuario=usuario)
        if registro.situacao == "ENVIADO":
            enviados.append({"fornecedor": nome, "para": destinos})
        else:
            falhas.append({"fornecedor": nome, "para": destinos,
                           "motivo": registro.erro})

    registrar_evento(s, "cotacao", cotacao_id, "COTACAO_DISPARADA",
                     {"empresa_id": empresa.id, "enviados": len(enviados),
                      "falhas": len(falhas), "sem_endereco": len(sem_endereco)},
                     usuario.id if usuario else None)
    logger.info("ERP/suprimentos: cotação %s disparada — %d enviada(s), "
                "%d falha(s), %d sem endereço",
                cot.numero, len(enviados), len(falhas), len(sem_endereco))

    return {
        "empresa": empresa.razao_social,
        "remetente": correio.remetente_de(empresa),
        "enviados": enviados, "falhas": falhas, "sem_endereco": sem_endereco,
        "resumo": _resumo(enviados, falhas, sem_endereco),
    }


def _resumo(enviados: list, falhas: list, sem_endereco: list) -> str:
    partes = [f"{len(enviados)} fornecedor(es) receberam a cotação"]
    if falhas:
        partes.append(f"{len(falhas)} falharam")
    if sem_endereco:
        partes.append(f"{len(sem_endereco)} estão sem e-mail no cadastro")
    return " · ".join(partes) + "."


# ============================================================================
# O PEDIDO DE COMPRA — o documento que FIRMA a compra
#
# A cotação pergunta preço; o pedido fecha. São documentos diferentes e o
# conteúdo é diferente:
#
#   cotação  →  o que se quer, quanto, e ONDE entregar. Sem preço nenhum.
#   pedido   →  o que se comprou, por quanto, COMO se paga, onde entregar e
#               até quando. É por este papel que o fornecedor fatura e que a
#               obra confere o que chegou.
#
# Duas travas que valem ser entendidas:
#
# 1. SÓ SAI PEDIDO AUTORIZADO. Mandar ao fornecedor um pedido que ainda está
#    na fila de autorização é comprar sem alçada — o fornecedor entrega e a
#    conta chega. A recusa é explícita, com o motivo.
#
# 2. O PREÇO VAI NO DOCUMENTO. É o que impede a discussão de nota com valor
#    diferente do combinado. Frete, desconto e total fecham a conta.
# ============================================================================
def empresa_do_pedido(s: Session, pedido_id: int) -> dict[str, Any]:
    """As empresas das obras que aparecem no pedido — mesma regra da cotação:
    quando é uma só, não se pergunta; quando são várias, quem compra decide."""
    from app.apps.erp.db.models.cadastros import PedidoCompra, PedidoItem

    if s.get(PedidoCompra, pedido_id) is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    obras_ids = set()
    for linha in s.scalars(select(PedidoItem)).all():
        if linha.pedido_id != pedido_id:
            continue
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        if item is not None and item.obra_id:
            obras_ids.add(item.obra_id)
    return _empresas_das_obras(s, obras_ids)


def _empresas_das_obras(s: Session, obras_ids) -> dict[str, Any]:
    achadas: dict[int, dict[str, Any]] = {}
    sem_empresa = []
    for obra_id in sorted(obras_ids):
        obra = s.get(Obra, obra_id)
        if obra is None:
            continue
        if not getattr(obra, "empresa_id", None):
            sem_empresa.append(getattr(obra, "codigo", str(obra_id)))
            continue
        empresa = s.get(Empresa, obra.empresa_id)
        if empresa is None:
            continue
        registro = achadas.setdefault(empresa.id, {
            "id": empresa.id, "razao_social": empresa.razao_social,
            "cnpj": empresa.cnpj, "obras": []})
        registro["obras"].append(getattr(obra, "codigo", str(obra_id)))

    padrao = svc_empresas.padrao(s)
    lista = sorted(achadas.values(), key=lambda e: e["razao_social"])
    return {
        "empresas": lista,
        "obras_sem_empresa": sorted(sem_empresa),
        "sugerida": (lista[0]["id"] if len(lista) == 1
                     else (padrao.id if padrao else None)),
        "precisa_escolher": len(lista) > 1,
    }


def _dinheiro(valor: Any) -> str:
    """1234.5 → "1.234,50". O fornecedor lê este número; ponto e vírgula
    trocados de lugar é a diferença entre mil reais e um real."""
    from decimal import Decimal, InvalidOperation
    try:
        d = Decimal(str(valor or 0)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return str(valor or "")
    inteiro, _, centavos = f"{d:.2f}".partition(".")
    negativo = inteiro.startswith("-")
    inteiro = inteiro.lstrip("-")
    grupos = []
    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]
    grupos.insert(0, inteiro)
    return ("-" if negativo else "") + ".".join(grupos) + "," + centavos


def montar_pedido(s: Session, pedido_id: int, empresa: Empresa) -> dict[str, str]:
    """O texto do pedido de compra, como o fornecedor precisa lê-lo."""
    from decimal import Decimal
    from app.apps.erp.core.suprimentos import pedido as svc_pedido
    from app.apps.erp.core.suprimentos.pagamento import descrever
    from app.apps.erp.db.models.cadastros import (
        CondicaoPagamento, ModoEntrega, PedidoCompra, PedidoItem,
    )

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    forn = s.get(Fornecedor, pedido.fornecedor_id)
    cond = (s.get(CondicaoPagamento, pedido.condicao_pagamento_id)
            if pedido.condicao_pagamento_id else None)

    linhas = sorted([x for x in s.scalars(select(PedidoItem)).all()
                     if x.pedido_id == pedido_id], key=lambda x: x.numero or 0)
    if not linhas:
        raise ErroValidacao("Este pedido não tem item nenhum.")

    corpo = [f"PEDIDO DE COMPRA {pedido.numero}", ""]
    corpo.append(f"Fornecedor: {_texto(getattr(forn, 'razao_social', ''))}")
    corpo.append("")
    corpo.append("Prezados,")
    corpo.append("")
    corpo.append("Confirmamos a compra dos itens abaixo, nas condições "
                 "acertadas. Favor confirmar o recebimento deste pedido.")
    corpo.append("")

    # Agrupado por endereço: é o motorista que lê, e um mesmo pedido leva
    # material para obras diferentes.
    blocos: dict[str, dict[str, Any]] = {}
    for linha in linhas:
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        obra = s.get(Obra, item.obra_id) if item is not None and item.obra_id else None
        endereco = endereco_da_obra(obra)
        bloco = blocos.setdefault(endereco, {"obras": [], "itens": []})
        rotulo = rotulo_da_obra(obra)
        if rotulo and rotulo not in bloco["obras"]:
            bloco["obras"].append(rotulo)
        bloco["itens"].append((linha, item, insumo))

    numero_do_item = 0
    soma = Decimal("0")
    for endereco, bloco in blocos.items():
        corpo.append(f"ENTREGAR EM: {endereco}")
        if bloco["obras"]:
            corpo.append(f"Obra(s): {' · '.join(bloco['obras'])}")
        for linha, item, insumo in bloco["itens"]:
            numero_do_item += 1
            quantidade = Decimal(str(linha.quantidade or 0))
            unitario = Decimal(str(linha.preco_unitario or 0))
            total_linha = quantidade * unitario
            soma += total_linha
            descricao = _texto(getattr(insumo, "descricao", "")) or "item"
            # A ESPECIFICAÇÃO é o que diz QUAL variação do material foi
            # comprada. Sem ela, o mesmo nome de insumo cobre coisas
            # diferentes e chega a errada.
            especificacao = _texto(getattr(item, "especificacao", ""))
            corpo.append(
                f"{numero_do_item:>3}. {descricao}"
                f"{f' — {especificacao}' if especificacao else ''}")
            corpo.append(
                f"     {_quantidade(linha.quantidade)} "
                f"{_texto(getattr(item, 'unidade', ''))} x "
                f"R$ {_dinheiro(unitario)}  =  R$ {_dinheiro(total_linha)}")
        corpo.append("")

    corpo.append(f"Subtotal dos itens: R$ {_dinheiro(soma)}")
    if Decimal(str(pedido.frete or 0)) > 0:
        corpo.append(f"Frete: R$ {_dinheiro(pedido.frete)}")
    if Decimal(str(pedido.desconto or 0)) > 0:
        corpo.append(f"Desconto: R$ {_dinheiro(pedido.desconto)}")
    corpo.append(f"TOTAL DO PEDIDO: R$ {_dinheiro(svc_pedido.total(s, pedido))}")
    corpo.append("")

    if cond is not None:
        # "28/56 dias (28/56 dias)" é ruído: a explicação só entra quando
        # acrescenta alguma coisa ao nome cadastrado.
        nome = _texto(cond.nome)
        em_palavras = descrever(cond.entrada_percentual, cond.dias)
        repete = em_palavras.lower().replace(" ", "") == nome.lower().replace(" ", "")
        corpo.append(f"Condição de pagamento: {nome}"
                     f"{f' ({em_palavras})' if em_palavras and not repete else ''}")
    else:
        corpo.append("Condição de pagamento: a combinar")
    if pedido.entrega is not None:
        corpo.append("Entrega: " + ("coleta por nossa conta"
                                    if pedido.entrega is ModoEntrega.COLETA
                                    else "por conta do fornecedor"))
    if pedido.previsao_entrega:
        corpo.append("Precisamos do material em obra até "
                     f"{pedido.previsao_entrega.strftime('%d/%m/%Y')}.")
    if _texto(pedido.observacoes):
        corpo += ["", _texto(pedido.observacoes)]

    corpo += ["",
              f"Referência deste pedido: {pedido.numero}. Favor citá-la na "
              f"nota fiscal e em qualquer contato sobre esta compra.",
              "", "Atenciosamente,"] + _assinatura(empresa)

    return {"assunto": f"Pedido de compra {pedido.numero} — "
                       f"{_texto(getattr(forn, 'razao_social', ''))}",
            "corpo": "\n".join(corpo)}


def preparar_pedido(s: Session, pedido_id: int) -> dict[str, Any]:
    """O que a tela mostra ANTES de mandar o pedido: para quem vai, de qual
    empresa sai, o texto exato e o que já foi enviado antes."""
    from app.apps.erp.db.models.cadastros import PedidoCompra, StatusPedidoCompra

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    possiveis = empresa_do_pedido(s, pedido_id)
    empresa = s.get(Empresa, possiveis["sugerida"]) if possiveis["sugerida"] else None

    forn = s.get(Fornecedor, pedido.fornecedor_id)
    destinos = destinos_do_fornecedor(s, pedido.fornecedor_id)
    envios = correio.historico(s, "pedido_compra", pedido_id)
    autorizado = pedido.status is StatusPedidoCompra.AUTORIZADO

    pode_empresa, falta = correio.conta_configurada(empresa)
    mensagem = ({"assunto": "", "corpo": ""} if empresa is None
                else montar_pedido(s, pedido_id, empresa))
    return {
        "pedido": pedido.numero,
        "autorizado": autorizado,
        "situacao": pedido.status.value,
        "fornecedor": getattr(forn, "razao_social", ""),
        "para": destinos,
        "sem_endereco": not destinos,
        "empresa": ({"id": empresa.id, "razao_social": empresa.razao_social,
                     "cnpj": empresa.cnpj,
                     "remetente": correio.remetente_de(empresa)}
                    if empresa else None),
        "empresas_possiveis": possiveis["empresas"],
        "precisa_escolher_empresa": possiveis["precisa_escolher"],
        "obras_sem_empresa": possiveis["obras_sem_empresa"],
        "conta_pronta": pode_empresa, "o_que_falta": falta,
        "assunto": mensagem["assunto"], "corpo": mensagem["corpo"],
        "envios": envios,
        "aviso": ("O sistema registra que a mensagem foi ACEITA pelo servidor "
                  "de saída, com data, hora e o texto exato. Isso não é o "
                  "mesmo que entregue, e muito menos lido."),
    }


def disparar_pedido(s: Session, pedido_id: int, dados: dict[str, Any],
                    usuario: Usuario) -> dict[str, Any]:
    """Manda o pedido de compra ao fornecedor, e registra o que foi mandado."""
    from app.apps.erp.db.models.cadastros import PedidoCompra, StatusPedidoCompra

    pedido = s.get(PedidoCompra, pedido_id)
    if pedido is None:
        raise ErroNaoEncontrado("Pedido não encontrado.")
    if pedido.status is not StatusPedidoCompra.AUTORIZADO:
        raise ErroValidacao(
            f"O pedido {pedido.numero} está "
            f"{pedido.status.value.replace('_', ' ').lower()} e não pode ser "
            f"mandado ao fornecedor. Pedido sai depois de autorizado — mandar "
            f"antes é comprar sem alçada.")

    possiveis = empresa_do_pedido(s, pedido_id)
    empresa_id = dados.get("empresa_id") or possiveis["sugerida"]
    if possiveis["precisa_escolher"] and not dados.get("empresa_id"):
        nomes = ", ".join(e["razao_social"] for e in possiveis["empresas"])
        raise ErroValidacao(
            f"Este pedido tem itens de obras de empresas diferentes ({nomes}). "
            f"Escolha por qual empresa ele sai — é o CNPJ que vai ser faturado.")
    if not empresa_id:
        raise ErroValidacao(
            "Não há empresa definida para este pedido. Ligue a obra a uma "
            "empresa em Administração › Empresas, ou marque uma como padrão.")
    empresa = svc_empresas.obter(s, int(empresa_id))

    pode, falta = correio.conta_configurada(empresa)
    if not pode:
        raise ErroValidacao(falta)

    destinos = destinos_do_fornecedor(s, pedido.fornecedor_id)
    if not destinos:
        forn = s.get(Fornecedor, pedido.fornecedor_id)
        raise ErroValidacao(
            f"{getattr(forn, 'razao_social', 'O fornecedor')} está sem e-mail "
            f"no cadastro. Corrija em Cadastros › Fornecedores, ou mande o "
            f"pedido por outro canal.")

    mensagem = montar_pedido(s, pedido_id, empresa)
    registro = correio.enviar(
        s, empresa=empresa, para=destinos,
        assunto=mensagem["assunto"], corpo=mensagem["corpo"],
        entidade_tipo="pedido_compra", entidade_id=pedido_id,
        destinatario_tipo="fornecedor", destinatario_id=pedido.fornecedor_id,
        usuario=usuario)

    registrar_evento(s, "pedido_compra", pedido_id, "PEDIDO_ENVIADO",
                     {"empresa_id": empresa.id, "para": destinos,
                      "situacao": registro.situacao, "erro": registro.erro},
                     usuario.id if usuario else None)
    logger.info("ERP/suprimentos: pedido %s mandado ao fornecedor — %s",
                pedido.numero, registro.situacao)

    if registro.situacao != "ENVIADO":
        return {"ok": False, "erro": registro.erro, "para": destinos,
                "resumo": f"Não saiu: {registro.erro}"}
    return {"ok": True, "para": destinos,
            "empresa": empresa.razao_social,
            "remetente": correio.remetente_de(empresa),
            "resumo": f"Pedido {pedido.numero} enviado para "
                      f"{', '.join(destinos)}."}
