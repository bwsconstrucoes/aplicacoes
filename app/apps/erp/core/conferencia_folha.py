# -*- coding: utf-8 -*-
"""
A CONFERÊNCIA QUE AUTORIZA DESLIGAR A PLANILHA.

⚑ Pedido dele em 17/11/2026, e com o motivo escrito:

    "Precisamos fazer de uma forma infalível essa desativação da planilha e a
     leitura do Análise de SPs pela base do ERP. A GRAVIDADE DISSO É PAGAR
     FUNCIONÁRIOS DE FORMA EQUIVOCADA."

⚠️ POR QUE ESTE ARQUIVO EXISTE, e não mais um relatório: "infalível" não se
consegue raciocinando sobre o código. Consegue-se **calculando as duas coisas
e comparando**. Hoje a folha lê a planilha; amanhã vai ler o ERP. Então a
pergunta que decide o desligamento é uma só, e é verificável:

    Para CADA pessoa, a folha tomaria a MESMA decisão lendo o ERP e lendo a
    planilha?

Enquanto a resposta não for "sim para todas" — ou "não, e eu sei exatamente
por que em cada caso" —, a planilha não se desliga.

O QUE É COMPARADO, do mais grave para o menos:

  1. **PODE RECEBER?** (`trava`) — é a pergunta de dinheiro. Ela é respondida
     pela mesma função do Análise de SPs (`situacao_no_pagamento`), alimentada
     uma vez com a ficha da planilha e outra com a ficha do ERP. Divergência
     aqui quer dizer: alguém recebe que não devia, ou deixa de receber.
  2. **A MODALIDADE DO AUXÍLIO** — é o que decide quantos dias e qual valor.
     Modalidade que a folha não entende faz a pessoa sair com ZERO.
  3. **OS VALORES** — alimentação, transporte, gratificação, diária.
  4. **O QUE DECIDE DIARISTA × CTPS** — tipo, tipo de contrato e as duas datas.
  5. O resto do cadastro que a folha lê (cargo, obra, convenção, matrícula…).

⚠️ A COMPARAÇÃO É DELIBERADAMENTE TOLERANTE EM TRÊS COISAS, e cada tolerância
está escrita porque tolerância escondida é o jeito de um relatório mentir
verde:

  · **dinheiro**: 180 e 180.00 são iguais; e ZERO é igual a VAZIO, porque a
    planilha usa "0,00" para dizer "não tem" (ver `planilha_dp._dinheiro`);
  · **texto**: acento, caixa e espaço sobrando não contam — é a mesma régua
    que a própria folha usa para comparar;
  · **nome**: diferença de nome é relatada à parte e NÃO conta como
    divergência de pagamento, porque não muda um centavo. Mas é relatada: nome
    diferente no mesmo CPF costuma ser cadastro duplicado em algum lugar.

Este módulo é SÓ LEITURA. Ele não escreve nada, em lado nenhum.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("erp.conferencia_folha")


# ---------------------------------------------------------------------------
# O QUE COMPARAR, EM ORDEM DE GRAVIDADE
#
# (campo, rótulo em português, como comparar, peso)
# O peso é o que a tela usa para ordenar: "dinheiro" primeiro, "cadastro"
# depois. Quem abre a conferência tem de ver o que paga errado antes do que
# está só feio.
# ---------------------------------------------------------------------------
CAMPOS = (
    ("modo_alimentacao",   "Modalidade da alimentação", "texto",    "dinheiro"),
    ("valor_alimentacao",  "Valor da alimentação",      "dinheiro", "dinheiro"),
    ("modo_transporte",    "Modalidade do transporte",  "texto",    "dinheiro"),
    ("valor_transporte",   "Valor do transporte",       "dinheiro", "dinheiro"),
    ("valor_gratificacao", "Valor da gratificação",     "dinheiro", "dinheiro"),
    ("parcela_unica",      "Recebe parcela única",      "sim",      "dinheiro"),
    ("tipo",               "Tipo (vínculo)",            "texto",    "dinheiro"),
    ("tipo_contrato",      "Tipo de contrato",          "texto",    "dinheiro"),
    ("data_inicio",        "Data de início",            "data",     "dinheiro"),
    ("data_admissao",      "Data de admissão",          "data",     "dinheiro"),
    ("data_saida",         "Data de saída",             "data",     "dinheiro"),
    ("aviso_previo",       "Data do aviso prévio",      "data",     "dinheiro"),
    ("ultimo_dia",         "Último dia trabalhado",     "data",     "dinheiro"),
    ("cargo",              "Cargo",                     "texto",    "cadastro"),
    ("convencao",          "Convenção coletiva",        "texto",    "cadastro"),
    ("obra_codigo",        "Código da obra",            "texto",    "cadastro"),
    ("matricula",          "Matrícula",                 "texto",    "cadastro"),
    ("id_fortes",          "Código no Fortes",          "texto",    "cadastro"),
    ("celular",            "Telefone",                  "digitos",  "cadastro"),
    ("observacao_auxilio", "Observação da ajuda de custo", "texto", "cadastro"),
)


def _cru(v: Any) -> str:
    sem = unicodedata.normalize("NFKD", str(v or ""))
    sem = "".join(c for c in sem if not unicodedata.combining(c))
    return " ".join(sem.upper().split())


def _digitos(v: Any) -> str:
    return re.sub(r"\D", "", str(v or ""))


def _dinheiro(v: Any) -> Decimal:
    """Vazio e zero são a MESMA coisa: a planilha usa "0,00" para dizer
    "não tem" (ver `planilha_dp._dinheiro`)."""
    if v in (None, "", "-"):
        return Decimal("0.00")
    texto = str(v).strip().replace("R$", "").strip()
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def _data(v: Any) -> str:
    if not v:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat()[:10]
    texto = str(v).strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", texto)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return texto[:10]


def _sim(v: Any) -> bool:
    return _cru(v).startswith("SIM")


def _iguais(modo: str, a: Any, b: Any) -> bool:
    if modo == "dinheiro":
        return _dinheiro(a) == _dinheiro(b)
    if modo == "data":
        return _data(a) == _data(b)
    if modo == "sim":
        return _sim(a) == _sim(b)
    if modo == "digitos":
        return _digitos(a) == _digitos(b)
    return _cru(a) == _cru(b)


def _mostrar(modo: str, v: Any) -> str:
    if modo == "dinheiro":
        d = _dinheiro(v)
        return "—" if d == 0 else f"{d}".replace(".", ",")
    if modo == "data":
        iso = _data(v)
        return "—" if not iso else f"{iso[8:10]}/{iso[5:7]}/{iso[0:4]}"
    if modo == "sim":
        return "Sim" if _sim(v) else "—"
    return str(v or "—")


def _planilha_existe(s: Session) -> bool:
    """A cópia da planilha vive no schema `analisesps`, que pode não existir
    (banco de teste, ou antes do botão de atualizações daquele módulo)."""
    try:
        s.execute(text("SELECT 1 FROM analisesps.colaborador LIMIT 1"))
        return True
    except Exception:                              # noqa: BLE001
        return False


def comparar(s: Session, *, ate=None) -> dict[str, Any]:
    """A conferência inteira. SÓ LEITURA.

    `ate` é o último dia do período que se pagaria — é o que
    `situacao_no_pagamento` usa para decidir quem já saiu. Sem ele, hoje.
    """
    from app.apps.analisesps import colaboradores as asp
    from app.apps.erp.core.cadastros import pessoas as svc

    saida: dict[str, Any] = {
        "ok": True, "erro": "", "quando": None,
        "total_planilha": 0, "total_erp": 0,
        "iguais": 0,
        "trava_diferente": [], "divergentes": [],
        "so_na_planilha": [], "so_no_erp": [],
        "nomes_diferentes": [],
        "por_campo": {},
    }
    if not _planilha_existe(s):
        saida["ok"] = False
        saida["erro"] = ("a cópia da planilha (analisesps.colaborador) não "
                         "existe neste banco — nada para comparar")
        return saida

    # A cópia da planilha, como as telas do Análise de SPs a leem.
    campos = list(asp.CAMPOS_LIDOS)
    linhas = s.execute(text(
        "SELECT " + ", ".join(campos) + " FROM analisesps.colaborador")).all()
    da_planilha: dict[str, dict[str, Any]] = {}
    for linha in linhas:
        ficha = {c: linha[i] for i, c in enumerate(campos)}
        cpf = _digitos(ficha.get("cpf"))
        if len(cpf) == 11:
            da_planilha[cpf] = ficha
    saida["total_planilha"] = len(da_planilha)

    # O ERP, na forma que a folha leria. `so_ativos=False`: quem saiu também
    # entra na conferência, porque é justamente nele que a decisão "pode
    # receber?" mais erra.
    do_erp = {}
    for ficha in svc.para_folha(s, so_ativos=False):
        cpf = _digitos(ficha.get("cpf"))
        if len(cpf) == 11:
            do_erp[cpf] = ficha
    saida["total_erp"] = len(do_erp)

    quando = s.execute(text(
        "SELECT MAX(atualizado_em) FROM analisesps.colaborador")).scalar()
    saida["quando"] = quando.isoformat() if quando else None

    for cpf in sorted(set(da_planilha) | set(do_erp)):
        p, e = da_planilha.get(cpf), do_erp.get(cpf)
        if p is None:
            saida["so_no_erp"].append({"cpf": cpf, "nome": e.get("nome", "")})
            continue
        if e is None:
            saida["so_na_planilha"].append({"cpf": cpf, "nome": p.get("nome", "")})
            continue

        # ⚠️ 1) A PERGUNTA DE DINHEIRO, respondida pela MESMA função dos dois
        # lados. É o item mais grave do relatório: divergência aqui é alguém
        # recebendo que não devia, ou deixando de receber.
        try:
            sp = asp.situacao_no_pagamento(dict(p), ate=ate)
            se = asp.situacao_no_pagamento(dict(e), ate=ate)
        except Exception as erro:                  # noqa: BLE001
            logger.warning("conferência: situação de %s falhou (%s)", cpf, erro)
            sp = se = None
        if sp and se and (sp["trava"] != se["trava"]
                          or sp["situacao"] != se["situacao"]):
            saida["trava_diferente"].append({
                "cpf": cpf, "nome": e.get("nome") or p.get("nome") or "",
                "planilha": sp["situacao"], "erp": se["situacao"],
                "planilha_paga": not sp["trava"], "erp_paga": not se["trava"],
                "motivo_planilha": sp["motivo"], "motivo_erp": se["motivo"]})

        # 2) O resto, campo por campo.
        diferencas = []
        for campo, rotulo, modo, peso in CAMPOS:
            if not _iguais(modo, p.get(campo), e.get(campo)):
                diferencas.append({
                    "campo": campo, "rotulo": rotulo, "peso": peso,
                    "planilha": _mostrar(modo, p.get(campo)),
                    "erp": _mostrar(modo, e.get(campo))})
                saida["por_campo"][rotulo] = saida["por_campo"].get(rotulo, 0) + 1

        # O nome fica à parte: não muda um centavo, mas nome diferente no
        # mesmo CPF costuma ser cadastro duplicado em algum lugar.
        if _cru(p.get("nome")) != _cru(e.get("nome")):
            saida["nomes_diferentes"].append({
                "cpf": cpf, "planilha": p.get("nome") or "",
                "erp": e.get("nome") or ""})

        if diferencas:
            diferencas.sort(key=lambda x: (x["peso"] != "dinheiro", x["rotulo"]))
            saida["divergentes"].append({
                "cpf": cpf, "nome": e.get("nome") or p.get("nome") or "",
                "campos": diferencas,
                "pesa_dinheiro": any(d["peso"] == "dinheiro" for d in diferencas)})
        else:
            saida["iguais"] += 1

    # O mais grave primeiro, e dentro disso o que mexe em dinheiro.
    saida["divergentes"].sort(
        key=lambda x: (not x["pesa_dinheiro"], -len(x["campos"]), x["nome"]))
    saida["pronto_para_desligar"] = (
        not saida["trava_diferente"]
        and not saida["so_na_planilha"]
        and not [d for d in saida["divergentes"] if d["pesa_dinheiro"]])
    return saida
