# ============================================================================
# BWS ERP — core/comum/atalho.py
# O NÚMERO VIRA ENDEREÇO: /erp/ir/<numero> abre o registro daquele número.
#
# Pedido do dono em 22/09/2026:
#
#   *"Cada lançamento, cadastro, uma obra, um título a pagar, uma medição, um
#   pedido de compra — a gente tem uma numeração, e essa numeração a gente tem
#   um link que a gente possa acessar (…) da mesma forma que eu consigo
#   acessar um card do Pipefy. Porque de repente eu encaminho via celular o
#   número de um registro, e a pessoa só clica e puf, abre o sistema."*
#
# O DESENHO, e o que ele resolve: as telas já abriam direto num registro, mas
# só pelo NÚMERO INTERNO do banco (`/erp/titulos?titulo=57`) — que ninguém vê,
# ninguém decora e não está em papel nenhum. O que a pessoa tem na mão é o
# número que o sistema imprime: 000123, PC-0001, CRECHE01. Este módulo faz a
# ponte entre os dois, num lugar só.
#
# Por que num módulo separado e não na rota: quem responde "que registro é
# este número?" também serve para o botão de copiar link, para a mensagem de
# WhatsApp e para o assistente. Repetir a tabela em três lugares é garantir
# que um dia eles discordem.
# ============================================================================
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class Destino:
    """Um tipo de registro que tem número próprio e tela onde abrir."""
    chave: str                # nome curto, usado em teste e em log
    rotulo: str               # como a pessoa chama isso
    tabela: str               # nome da tabela, para a consulta
    campo: str                # a coluna do número que a pessoa vê
    endpoint: str             # a tela do ERP
    parametro: str            # o nome do parâmetro que a tela já entende
    acao: str                 # a ação que o perfil precisa ter


# A ORDEM IMPORTA quando dois números se parecem: o primeiro que casar ganha.
# Obra vem por último de propósito — o código dela é texto livre ("CRECHE01"),
# e é o único que pode colidir com qualquer coisa.
DESTINOS: tuple[Destino, ...] = (
    Destino("titulo", "Lançamento (SP)", "titulos", "numero_sp",
            "erp.pagina_titulos", "titulo", "ver_titulos"),
    Destino("solicitacao", "Pedido de material", "suprimento_solicitacoes", "numero",
            "erp.pagina_suprimentos", "solicitacao", "ver_suprimentos"),
    Destino("cotacao", "Cotação", "cotacoes", "numero",
            "erp.pagina_suprimentos_cotacoes", "cotacao", "comprar"),
    Destino("pedido_compra", "Pedido de compra", "pedidos_compra", "numero",
            "erp.pagina_suprimentos_pedidos", "pedido", "ver_pedidos_compra"),
    Destino("empreita", "Empreita", "contratos_servico", "numero",
            "erp.pagina_empreitas", "empreita", "ver_empreitas"),
    Destino("locacao", "Locação", "contratos_locacao", "numero",
            "erp.pagina_locacoes", "contrato", "ver_locacoes"),
    Destino("despesa_colaborador", "Despesa de colaborador", "despesas_colaborador",
            "numero", "erp.pagina_dc", "despesa", "ver_pessoal"),
    Destino("processo", "Processo do acompanhamento", "processos", "numero",
            "erp.pagina_acompanhamento", "processo", "ver_acompanhamento"),
    Destino("insumo", "Insumo", "insumos", "codigo",
            "erp.pagina_suprimentos_insumos", "insumo", "ver_suprimentos"),
    Destino("obra", "Obra", "obras", "codigo",
            "erp.pagina_obras", "obra", "ver_obras"),
)

POR_CHAVE = {d.chave: d for d in DESTINOS}


# O que as pessoas digitam junto do número e não faz parte dele: "SP 000123",
# "#000123", "sp-000123". Tudo isso é o mesmo lançamento.
_ENFEITE = re.compile(r"^(?:sp|n[ºo°]?|num(?:ero)?|#)[\s.:\-]*", re.IGNORECASE)
_FORMATO = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-/]{0,39}$")


def limpar(bruto: Any) -> str:
    """O número do jeito que o banco guarda, a partir do que a pessoa colou."""
    texto = " ".join(str(bruto or "").split())
    texto = _ENFEITE.sub("", texto).strip()
    return texto.upper()


def _variacoes(numero: str) -> list[str]:
    """O mesmo número escrito de outros jeitos que valem a pena tentar.

    O caso real é o lançamento: ele é gravado com seis dígitos ("000123"),
    mas quem fala dele por telefone diz "cento e vinte e três" e digita 123.
    """
    formas = [numero]
    if numero.isdigit():
        formas.append(numero.zfill(6))
        sem_zeros = numero.lstrip("0")
        if sem_zeros and sem_zeros != numero:
            formas.append(sem_zeros)
    return list(dict.fromkeys(formas))


def achar(s: Session, bruto: Any) -> list[dict[str, Any]]:
    """Todos os registros cujo número bate. Normalmente zero ou um.

    Devolve lista, e não um só, porque o código da obra é texto livre: nada
    impede alguém de cadastrar a obra "INS-0001". Melhor mostrar as duas e
    deixar a pessoa escolher do que abrir a errada calada.
    """
    from sqlalchemy import text as _text

    numero = limpar(bruto)
    if not numero or not _FORMATO.match(numero):
        return []

    formas = _variacoes(numero)
    achados: list[dict[str, Any]] = []
    for d in DESTINOS:
        # consulta direta e simples: nome de tabela e coluna vêm da tabela
        # acima (constante do código), nunca do que a pessoa digitou — o valor
        # procurado vai por parâmetro.
        linhas = s.execute(_text(
            f"SELECT id, {d.campo} AS numero FROM {d.tabela} "  # noqa: S608
            f"WHERE UPPER({d.campo}) = ANY(:formas) LIMIT 5"
        ), {"formas": formas}).fetchall()
        for linha in linhas:
            achados.append({"destino": d, "id": linha.id, "numero": linha.numero})
    return achados


# O PEDIDO DE MATERIAL é o único que viaja pelo NÚMERO, e não pelo número
# interno: a tela dele lista ITENS, não pedidos, então o atalho deixa a busca
# preenchida com "SS-0001" em vez de abrir uma ficha que não existe.
PELO_NUMERO = {"solicitacao"}


def endereco(destino: Destino, registro_id: int, numero: str | None = None) -> str:
    """O endereço da tela já aberta (ou já filtrada) naquele registro."""
    from flask import url_for
    valor = numero if (destino.chave in PELO_NUMERO and numero) else registro_id
    return url_for(destino.endpoint, **{destino.parametro: valor})


def link_do_registro(chave: str, registro_id: int, *, absoluto: bool = False) -> str:
    """O link para copiar e mandar. `chave` é o nome curto do destino."""
    from flask import url_for
    d = POR_CHAVE.get(chave)
    if d is None:
        raise KeyError(f"Não sei montar link de {chave!r}.")
    return url_for(d.endpoint, _external=absoluto, **{d.parametro: registro_id})


def link_pelo_numero(numero: Any, *, absoluto: bool = True) -> str:
    """O link CURTO, o que se manda por WhatsApp: /erp/ir/<numero>.

    É este que sobrevive a tudo: não depende do número interno do banco, não
    quebra se a tela mudar de endereço, e a pessoa entende o que está clicando
    porque o número que ela conhece está ali no meio.
    """
    from flask import url_for
    return url_for("erp.ir_para_o_numero", numero=limpar(numero), _external=absoluto)
