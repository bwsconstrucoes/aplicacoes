# -*- coding: utf-8 -*-
"""
CARGO, CONVENÇÃO COLETIVA e o PISO — a regra (migração 085).

Pedido do dono, 05/11/2026, sobre a planilha de cargos que ele exportou:

> *"veja que ela tá mal construída. Mistura Cargo e convenção. Isso é pra ser
> distinto. Seleciona Cargo e Selecionar Convenção Coletiva. Ao mesmo tempo tem
> a questão salarial e as alterações salariais pra serem gravadas."*

AS TRÊS COISAS, e por que separá-las resolve o problema dele:

  · **cargo** é o que a pessoa faz (PEDREIRO). Tem CBO, e é um só.
  · **convenção** é o acordo que manda no piso (CE-Fortaleza, BA-Sobradinho).
  · o **cruzamento** é onde vive o piso e a categoria — PEDREIRO em
    CE-Fortaleza paga R$ 2.230; em MA-Imperatriz, R$ 1.608,23.

Na planilha os três estão num texto: `PEDREIRO (CE-Fortaleza)`. São 135 linhas
para 63 cargos — PEDREIRO repetido 11 vezes. Mudar o piso de uma convenção
obrigava a achar e editar linha por linha, e foi assim que apareceram os dois
casos de MESMO cargo e convenção com salários diferentes.

O QUE A IMPORTAÇÃO CONSERTA SOZINHA, e o que ela NÃO conserta:

| defeito na planilha | aqui |
|---|---|
| 16 convenções que são 13 ("MT-Mato Grosso" e "MT- Mato Grosso") | colapsam: a comparação ignora caixa, acento e espaço repetido |
| 19 categorias que são ~13 ("não qualificados" × "NÃO QUALIFICADO") | idem, e o sufixo da convenção ("PROFISSIONAL (MA)") é removido |
| CBO em 5 formatos (`7152-10`, `715505.0`, `72411-0`, vazio) | guardado só com dígitos; a tela exibe com máscara |
| salário por extenso digitado, com erro ("mil quinhetoss") | não é importado: o ERP gera do valor |
| `ALMOXARIFADO` onde era ALMOXARIFE, `PEDEEIRO` onde era PEDREIRO | **NÃO conserta** — vira aviso na tela, para o dono decidir |
| mesmo cargo+convenção com dois salários | **NÃO escolhe** — vira aviso com os dois valores |

Erro de digitação em nome não se adivinha: "ALMOXARIFADO" pode ser cargo de
verdade em outra empresa. Corrigir calado criaria um cargo que ninguém pediu.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Cargo, CargoConvencao, CargoConvencaoSalario, Convencao, Usuario,
)

logger = logging.getLogger(__name__)

# As categorias que a planilha usa, já sem o sufixo de convenção e sem variação
# de caixa. Ficam aqui para a tela oferecer lista em vez de texto livre — é o
# texto livre que produziu "não qualificados", "Não qualificados" e
# "NÃO QUALIFICADO" como três categorias.
CATEGORIAS = (
    "PROFISSIONAL", "MEIO-PROFISSIONAL", "QUALIFICADO", "NAO QUALIFICADO",
    "OPERARIO QUALIFICADO", "SERVENTE", "PRATICO", "COMUM",
    "ENCARREGADO DE SETOR", "MESTRE DE OBRAS", "PESSOAL DE APOIO ADM.",
    "SEM CATEGORIA",
)


def _sem_acento(texto: Any) -> str:
    t = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(c for c in t if not unicodedata.combining(c))


def normalizar_nome(texto: Any) -> str:
    """PURA. Para comparar: sem acento, maiúsculo, um espaço só — e **sem espaço
    ao redor do hífen**.

    O hífen é o detalhe que decide, e eu errei nele na primeira versão: a
    planilha tem "MT-Mato Grosso" e "MT- Mato Grosso", "SP-Feticom" e
    "SP - Feticom". Colapsar só espaço repetido deixa os dois pares de pé, e aí
    a mesma convenção entra duas vezes — com dois pisos para o mesmo cargo, que
    é exatamente o defeito que esta importação existe para consertar.
    """
    limpo = " ".join(_sem_acento(texto).upper().split())
    return re.sub(r"\s*-\s*", "-", limpo)


def normalizar_cbo(bruto: Any) -> Optional[str]:
    """PURA. O CBO só com dígitos.

    A planilha traz cinco formatos, e o pior é o `715505.0`: o Google Sheets
    entendeu `7155-05` como número, dividiu e devolveu decimal. Sem esta
    função, `7152-10`, `715210` e `715210.0` seriam três cargos.

    Mantém até 7 dígitos (o CBO brasileiro tem 6 ou 7); o que sobra é lixo de
    conversão e é descartado com aviso de quem chamou.
    """
    texto = str(bruto or "").strip()
    if not texto:
        return None
    if texto.endswith(".0"):             # 715505.0 -> 715505
        texto = texto[:-2]
    digitos = re.sub(r"\D", "", texto)
    return digitos[:7] or None


def cbo_com_mascara(cbo: Any) -> str:
    """PURA. 715210 -> 7152-10. É só exibição."""
    d = re.sub(r"\D", "", str(cbo or ""))
    if len(d) == 6:
        return f"{d[:4]}-{d[4:]}"
    if len(d) == 7:
        return f"{d[:5]}-{d[5:]}"
    return d


def normalizar_categoria(bruto: Any, convencao: Any = None) -> Optional[str]:
    """PURA. Tira a variação de caixa e o sufixo da convenção.

    "PROFISSIONAL (MA)" é PROFISSIONAL — o "(MA)" repete a convenção, que já é
    a coluna ao lado. Com o sufixo, a mesma categoria viraria uma por estado, e
    nenhuma tela conseguiria somar "quantos profissionais a empresa tem".
    """
    texto = normalizar_nome(bruto)
    if not texto:
        return None
    texto = re.sub(r"\s*\([^)]*\)\s*$", "", texto).strip()
    # "NAO QUALIFICADOS" (plural) -> singular, como as outras
    if texto in ("NAO QUALIFICADOS", "NAO QUALIFICADO"):
        return "NAO QUALIFICADO"
    return texto or None


def _dec(bruto: Any) -> Optional[Decimal]:
    texto = str(bruto or "").strip()
    if not texto:
        return None
    texto = texto.replace("R$", "").strip()
    # "1.910,65" (planilha brasileira) e "1910.65" (exportação) convivem
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


# ---------------------------------------------------------------------------
# Achar ou criar
# ---------------------------------------------------------------------------
def achar_ou_criar_cargo(s: Session, nome: Any, *, cbo: Any = None) -> Cargo:
    limpo = " ".join(str(nome or "").split())
    if len(limpo) < 2:
        raise ErroValidacao("Informe o nome do cargo.")
    alvo = normalizar_nome(limpo)
    for c in s.scalars(select(Cargo)).all():
        if normalizar_nome(c.nome) == alvo:
            if cbo and not c.cbo:             # o CBO que faltava chegou agora
                c.cbo = normalizar_cbo(cbo)
            return c
    c = Cargo(nome=limpo.upper(), cbo=normalizar_cbo(cbo))
    s.add(c)
    s.flush()
    return c


def achar_ou_criar_convencao(s: Session, nome: Any, *, uf: Any = None) -> Convencao:
    limpo = " ".join(str(nome or "").split())
    if not limpo:
        limpo = "Sem Convenção"
    alvo = normalizar_nome(limpo)
    for c in s.scalars(select(Convencao)).all():
        if normalizar_nome(c.nome) == alvo:
            return c
    # A UF sai do próprio nome quando ele começa por ela ("CE-Fortaleza").
    sigla = str(uf or "").strip().upper()[:2] or None
    if not sigla:
        m = re.match(r"^([A-Z]{2})\s*-", limpo.upper())
        sigla = m.group(1) if m else None
    c = Convencao(nome=limpo, uf=sigla)
    s.add(c)
    s.flush()
    return c


def definir_piso(s: Session, cargo_convencao_id: int, *, salario: Any,
                 vigencia: Optional[date] = None, valor_diaria: Any = None,
                 motivo: str = "", usuario: Optional[Usuario] = None) -> CargoConvencao:
    """Troca o piso e FECHA o anterior, na mesma transação.

    O valor vigente fica repetido em `cargo_convencao.salario_piso` porque é o
    que toda tela lê; a história fica na tabela de salários. Manter os dois de
    acordo é trabalho desta função, e de nenhuma outra — se alguém escrever o
    piso direto na coluna, a história passa a mentir.
    """
    cc = s.get(CargoConvencao, cargo_convencao_id)
    if cc is None:
        raise ErroValidacao("Cruzamento de cargo e convenção não encontrado.")
    valor = _dec(salario)
    if valor is None or valor <= 0:
        raise ErroValidacao("Informe o piso salarial.")
    quando = vigencia or date.today()
    aberto = s.scalars(select(CargoConvencaoSalario).where(
        CargoConvencaoSalario.cargo_convencao_id == cc.id,
        CargoConvencaoSalario.vigencia_fim.is_(None))).first()
    if aberto is not None:
        if aberto.salario_piso == valor and _dec(valor_diaria) == aberto.valor_diaria:
            return cc                     # nada mudou: não suja a história
        if quando <= aberto.vigencia_inicio:
            raise ErroValidacao(
                f"O piso vigente começou em {aberto.vigencia_inicio:%d/%m/%Y}. "
                "A vigência do novo precisa ser depois disso.")
        aberto.vigencia_fim = quando
    s.add(CargoConvencaoSalario(
        cargo_convencao_id=cc.id, salario_piso=valor, valor_diaria=_dec(valor_diaria),
        vigencia_inicio=quando, motivo=(motivo or "").strip() or None,
        usuario_id=usuario.id if usuario else None))
    cc.salario_piso = valor
    if _dec(valor_diaria) is not None:
        cc.valor_diaria = _dec(valor_diaria)
    s.flush()
    registrar_evento(s, "cargo_convencao", cc.id, "PISO_DEFINIDO",
                     {"de": str(aberto.salario_piso) if aberto else None,
                      "para": str(valor), "vigencia": str(quando), "motivo": motivo},
                     usuario.id if usuario else None)
    return cc


def piso_em(s: Session, cargo_convencao_id: int, quando: date) -> Optional[Decimal]:
    """O piso que valia NAQUELA data — é o que a conferência da folha usa."""
    linha = s.scalars(select(CargoConvencaoSalario).where(
        CargoConvencaoSalario.cargo_convencao_id == cargo_convencao_id,
        CargoConvencaoSalario.vigencia_inicio <= quando).order_by(
        CargoConvencaoSalario.vigencia_inicio.desc())).first()
    if linha is None:
        return None
    if linha.vigencia_fim is not None and linha.vigencia_fim <= quando:
        return None
    return linha.salario_piso


def cruzamento(s: Session, cargo_id: int, convencao_id: int, *,
               criar: bool = False) -> Optional[CargoConvencao]:
    cc = s.scalars(select(CargoConvencao).where(
        CargoConvencao.cargo_id == cargo_id,
        CargoConvencao.convencao_id == convencao_id)).first()
    if cc is None and criar:
        cc = CargoConvencao(cargo_id=cargo_id, convencao_id=convencao_id)
        s.add(cc)
        s.flush()
    return cc


# ---------------------------------------------------------------------------
# Ler para a tela
# ---------------------------------------------------------------------------
def listar(s: Session, *, so_ativos: bool = True,
           com_salario: bool = True) -> dict[str, Any]:
    """A lista para a tela.

    ⚠️ `com_salario=False` existe por uma brecha que eu mesmo abri e o teste
    `test_permissoes_rotas` pegou: a tela de cadastro precisa da LISTA de cargos
    para montar a caixinha, e ela é aberta a quem tem `ver_pessoal` — o que
    inclui supervisor de obra e PARCEIRO. Mandar o piso junto entregaria a
    tabela salarial da empresa a quem só precisa escolher um cargo numa lista.
    """
    cargos = {c.id: c for c in s.scalars(select(Cargo).order_by(Cargo.nome)).all()}
    convs = {c.id: c for c in s.scalars(select(Convencao).order_by(Convencao.nome)).all()}
    stmt = select(CargoConvencao)
    if so_ativos:
        stmt = stmt.where(CargoConvencao.ativo.is_(True))
    cruzamentos = []
    for cc in s.scalars(stmt).all():
        cargo, conv = cargos.get(cc.cargo_id), convs.get(cc.convencao_id)
        cruzamentos.append({
            "id": cc.id, "cargo_id": cc.cargo_id, "convencao_id": cc.convencao_id,
            "cargo": cargo.nome if cargo else "", "convencao": conv.nome if conv else "",
            "cbo": cbo_com_mascara(cargo.cbo) if cargo else "",
            "categoria": cc.categoria or "",
            "ativo": cc.ativo,
        })
        if com_salario:
            cruzamentos[-1].update({
                "salario_piso": float(cc.salario_piso) if cc.salario_piso else None,
                "valor_diaria": float(cc.valor_diaria) if cc.valor_diaria else None,
            })
    cruzamentos.sort(key=lambda x: (x["cargo"], x["convencao"]))
    return {
        "cargos": [{"id": c.id, "nome": c.nome, "cbo": cbo_com_mascara(c.cbo),
                    "ativo": c.ativo} for c in cargos.values()
                   if c.ativo or not so_ativos],
        "convencoes": [{"id": c.id, "nome": c.nome, "uf": c.uf or "",
                        "mes_data_base": c.mes_data_base, "ativo": c.ativo}
                       for c in convs.values() if c.ativo or not so_ativos],
        "cruzamentos": cruzamentos,
        "categorias": list(CATEGORIAS),
    }


def historico_do_piso(s: Session, cargo_convencao_id: int) -> list[dict[str, Any]]:
    linhas = s.scalars(select(CargoConvencaoSalario).where(
        CargoConvencaoSalario.cargo_convencao_id == cargo_convencao_id).order_by(
        CargoConvencaoSalario.vigencia_inicio.desc())).all()
    return [{
        "salario_piso": float(l.salario_piso),
        "valor_diaria": float(l.valor_diaria) if l.valor_diaria else None,
        "vigencia_inicio": l.vigencia_inicio.isoformat(),
        "vigencia_fim": l.vigencia_fim.isoformat() if l.vigencia_fim else None,
        "vigente": l.vigencia_fim is None,
        "motivo": l.motivo or "",
    } for l in linhas]
