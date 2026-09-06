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
    for i, linha in enumerate(linhas, start=1):
        item = s.get(SuprimentoItem, linha.suprimento_item_id)
        insumo = s.get(Insumo, item.insumo_id) if item is not None else None
        descricao = _texto(getattr(insumo, "descricao", "")) or "item"
        especificacao = _texto(getattr(item, "especificacao", ""))
        corpo.append(
            f"{i:>3}. {descricao}"
            f"{f' — {especificacao}' if especificacao else ''}"
            f"   [{_quantidade(getattr(item, 'quantidade', ''))} "
            f"{_texto(getattr(item, 'unidade', ''))}]")

    corpo += ["", "Pedimos que a proposta informe, para cada item:",
              "  · preço unitário", "  · prazo de entrega",
              "  · condição de pagamento", "  · valor do frete, se houver", ""]
    if prazo:
        corpo.append(f"Pedimos retorno até {prazo}.")
        corpo.append("")
    if _texto(observacao):
        corpo += [_texto(observacao), ""]

    corpo.append(f"Referência desta cotação: {cot.numero}. "
                 f"Favor mantê-la ao responder.")
    corpo += ["", "Atenciosamente,",
              _texto(empresa.nome_fantasia) or _texto(empresa.razao_social)]
    if _texto(empresa.cnpj):
        corpo.append(f"CNPJ {empresa.cnpj}")
    endereco = ", ".join(x for x in [_texto(empresa.logradouro),
                                     _texto(empresa.numero),
                                     _texto(empresa.bairro),
                                     _texto(empresa.municipio),
                                     _texto(empresa.uf)] if x)
    if endereco:
        corpo.append(endereco)
    if _texto(empresa.telefone):
        corpo.append(_texto(empresa.telefone))

    return {"assunto": f"Cotação {cot.numero} — {cot.titulo}",
            "corpo": "\n".join(corpo)}


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
