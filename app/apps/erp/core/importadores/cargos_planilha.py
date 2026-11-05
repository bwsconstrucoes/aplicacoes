# -*- coding: utf-8 -*-
"""
IMPORTAR A PLANILHA DE CARGOS DO PIPEFY — e separar o que ela juntava.

O dono exportou a tabela "Cargo e Convenção" do Pipefy em 05/11/2026 e disse:

> *"To colocando a planilha de cargos, mas veja que ela tá mal construída.
> Mistura Cargo e convenção. Isso é pra ser distinto."*

O que esta importação faz com as 135 linhas dela:

  1. **separa** cargo, convenção e o cruzamento (63 cargos, ~13 convenções,
     135 cruzamentos);
  2. **colapsa** a duplicação de grafia: "MT-Mato Grosso" e "MT- Mato Grosso"
     viram uma; "não qualificados" e "NÃO QUALIFICADO", uma categoria;
  3. **conserta o CBO**, que vem em cinco formatos — inclusive 24 linhas em que
     o Sheets converteu `7155-05` no número `715505.0`;
  4. **abre o histórico do piso** com a vigência que o dono informar (ou hoje),
     para a próxima mudança de dissídio ser uma linha nova e não um valor
     sobrescrito;
  5. **NÃO conserta nome errado nem escolhe entre dois salários** — os dois
     viram aviso, porque adivinhar aqui é pior que perguntar.

SIMULA ANTES DE GRAVAR. `gravar=False` devolve o relatório inteiro sem tocar no
banco — é o botão "conferir" da tela. Importação de cadastro base roda uma vez e
é vista por todo mundo depois.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import cargos as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.importadores.planilhas import ler_tabela
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

# Os nomes como a exportação do Pipefy os escreve, com alternativas — a mesma
# técnica do resto do repositório: a planilha de gente muda de cabeçalho.
COLUNAS = {
    "cargo": ("cargo:", "cargo", "cargo e convenção:", "cargo e convencao:"),
    "convencao": ("convenção:", "convencao:", "convenção", "convencao"),
    "categoria": ("categoria:", "categoria"),
    "salario": ("salário:", "salario:", "salário", "salario"),
    "cbo": ("cbo", "código cbo", "codigo cbo"),
    "diaria": ("valor diária", "valor diaria", "diária", "diaria"),
    "codigo": ("código", "codigo"),
    "titulo": ("título", "titulo"),
    "status": ("status",),
    "riscos": ("riscos associados",),
    "epis": ("epi's necessários", "epis necessários", "epi's necessarios"),
}
# A coluna que identifica a tabela: sem ela não é a planilha de cargos.
ESPERADAS = ("Cargo:", "Convenção:")


def _valor(linha: dict[str, str], chave: str) -> str:
    for nome in COLUNAS[chave]:
        if nome in linha:
            v = (linha[nome] or "").strip()
            if v:
                return v
    return ""


def importar(s: Session, conteudo: bytes, *, gravar: bool,
             vigencia: Optional[date] = None,
             usuario: Optional[Usuario] = None) -> dict[str, Any]:
    linhas = ler_tabela(conteudo, colunas_esperadas=ESPERADAS)
    if not linhas:
        raise ErroValidacao("A planilha veio vazia.")
    quando = vigencia or date.today()

    rel: dict[str, Any] = {
        "gravou": gravar, "linhas": len(linhas), "vigencia": quando.isoformat(),
        "cargos_novos": [], "convencoes_novas": [], "cruzamentos_novos": 0,
        "cruzamentos_atualizados": 0, "pisos_abertos": 0,
        "avisos": [], "erros": [],
    }
    # Para a simulação não criar nada: a conta é feita em memória.
    vistos_cargo: dict[str, str] = {}
    vistos_conv: dict[str, str] = {}
    vistos_cruz: dict[tuple[str, str], str] = {}

    for n, linha in enumerate(linhas, start=2):
        nome_cargo = _valor(linha, "cargo")
        # A coluna "Cargo e Convenção:" ("PEDREIRO (CE-Fortaleza)") é a primeira
        # da planilha e seria pega por engano quando "Cargo:" está vazia — o
        # parêntese denuncia, e aí fica só o que vem antes dele.
        if "(" in nome_cargo and _valor(linha, "convencao"):
            nome_cargo = nome_cargo.split("(")[0].strip()
        nome_conv = _valor(linha, "convencao") or "Sem Convenção"
        if not nome_cargo:
            rel["erros"].append(f"linha {n}: sem o nome do cargo")
            continue
        status = _valor(linha, "status")
        if status and svc.normalizar_nome(status) not in ("ATIVO", ""):
            # "Concluído" no card = cargo aposentado. Entra inativo, não fora:
            # há gente cadastrada nele, e cargo que desaparece deixa a ficha
            # dela sem cargo nenhum.
            rel["avisos"].append(
                f"linha {n}: {nome_cargo} ({nome_conv}) está “{status}” no Pipefy — "
                "entra como INATIVO")
        chave_cargo = svc.normalizar_nome(nome_cargo)
        chave_conv = svc.normalizar_nome(nome_conv)
        cbo = svc.normalizar_cbo(_valor(linha, "cbo"))
        if _valor(linha, "cbo") and not cbo:
            rel["avisos"].append(
                f"linha {n}: CBO “{_valor(linha, 'cbo')}” não tem dígito nenhum — ficou vazio")
        salario = _valor(linha, "salario")
        if not salario:
            rel["avisos"].append(f"linha {n}: {nome_cargo} ({nome_conv}) sem salário na planilha")

        # O MESMO cargo+convenção duas vezes com salários diferentes: a planilha
        # tem dois casos (MESTRE DE OBRAS/Sem Convenção e ELETRICISTA/
        # CE-Fortaleza, este último porque o campo "Cargo" de "AUXILIAR DE
        # ELETRICISTA" diz só "ELETRICISTA"). Não se escolhe: vale a primeira e
        # o aviso diz os dois valores, com a linha.
        par = (chave_cargo, chave_conv)
        if par in vistos_cruz:
            if vistos_cruz[par] != salario:
                rel["avisos"].append(
                    f"linha {n}: {nome_cargo} ({nome_conv}) aparece de novo com salário "
                    f"{salario} — a primeira linha trouxe {vistos_cruz[par]}. "
                    "Valeu a primeira; confira qual é o certo.")
            continue
        vistos_cruz[par] = salario

        if chave_cargo not in vistos_cargo:
            vistos_cargo[chave_cargo] = nome_cargo
        if chave_conv not in vistos_conv:
            vistos_conv[chave_conv] = nome_conv

        if not gravar:
            continue

        cargo = svc.achar_ou_criar_cargo(s, nome_cargo, cbo=cbo)
        if cargo.criado_em is None:
            rel["cargos_novos"].append(cargo.nome)
        conv = svc.achar_ou_criar_convencao(s, nome_conv)
        cc = svc.cruzamento(s, cargo.id, conv.id, criar=True)
        novo = cc.salario_piso is None and cc.categoria is None
        cc.categoria = svc.normalizar_categoria(_valor(linha, "categoria"), nome_conv)
        cc.ref_pipefy = _valor(linha, "codigo") or cc.ref_pipefy
        if status and svc.normalizar_nome(status) not in ("ATIVO", ""):
            cc.ativo = False
        rel["cruzamentos_novos" if novo else "cruzamentos_atualizados"] += 1
        if salario:
            antes = cc.salario_piso
            svc.definir_piso(s, cc.id, salario=salario, vigencia=quando,
                             valor_diaria=_valor(linha, "diaria"),
                             motivo="importado da planilha do Pipefy", usuario=usuario)
            if cc.salario_piso != antes:
                rel["pisos_abertos"] += 1

    rel["cargos_distintos"] = len(vistos_cargo)
    rel["convencoes_distintas"] = len(vistos_conv)
    rel["cruzamentos"] = len(vistos_cruz)
    # O que a planilha chama de cargo mas parece erro de digitação: nome que não
    # casa com o título do card. Vale como aviso, nunca como correção.
    for n, linha in enumerate(linhas, start=2):
        titulo, cargo_col = _valor(linha, "titulo"), _valor(linha, "cargo")
        if titulo and cargo_col and "(" in titulo:
            do_titulo = svc.normalizar_nome(titulo.split("(")[0])
            if do_titulo and do_titulo != svc.normalizar_nome(cargo_col):
                rel["avisos"].append(
                    f"linha {n}: o card se chama “{titulo.split('(')[0].strip()}” e a coluna "
                    f"Cargo diz “{cargo_col}” — confira qual é o certo")
    rel["avisos"] = rel["avisos"][:120]
    logger.info("ERP: cargos da planilha — %d linhas, %d cargos, %d convenções, "
                "%d cruzamentos, %d avisos (%s)", rel["linhas"], rel["cargos_distintos"],
                rel["convencoes_distintas"], rel["cruzamentos"], len(rel["avisos"]),
                "gravado" if gravar else "simulação")
    return rel
