# -*- coding: utf-8 -*-
"""
A COMPLEMENTAR DAS DIÁRIAS — pagar só a DIFERENÇA de uma quinzena de diaristas já
paga, depois de o ponto ser corrigido (11/11/2026).

O dono: *"fizemos o pagamento, geramos os arquivos (…) da última quinzena. Só que
aí o pessoal precisou corrigir ponto (…) a gente precisaria pagar a diferença
desse pessoal. (…) peguei o relatório anterior, gerei um novo relatório (…) joguei
na IA para fazer o confronto (…) queria que a gente pudesse fazer isso dentro do
sistema (…) como na folha da contabilidade a gente tem aquele complemento (…) ver
o que foi pago, ver o que tem de diferente e poder gerar um segundo arquivo só da
diferença do que a gente selecionar."*

É a mesma conta da Complementar da folha (`folha_complementar`, desenho aprovado
pelo dono em 10/11/2026), com duas trocas:

- O PAGO é a soma, por CPF, do FECHAMENTO de cada geração de diárias do período no
  log: a `diaria` (todas as empresas), a `diaria.<cnpj>` (por empresa) e as
  complementares já feitas (`diaria_c1`, `diaria_c2`…, com ou sem empresa).
- O NOVO é a conta de hoje da tela dos Diaristas (`folha_diaristas.calcular`), com o
  ponto já corrigido — de quem tem diária a receber, marcado ou não na tela (quem
  estava desmarcado vem em "não pago antes", desmarcado, com o motivo).

Os grupos, a seleção (vem marcado só "a receber"), o arquivo da diferença (verba
própria `diaria_cN`, com a obra da conta nova), a lista "a descontar" e o
relatório são os da folha. Com uma EMPRESA escolhida (`empresa_da_folha`), entram
só as pessoas dela, e a verba leva a empresa (`diaria_c1.<cnpj>`).
"""
from __future__ import annotations

import logging
import re
from decimal import Decimal

from . import folha_complementar as fcomp

logger = logging.getLogger("analisesps.diaristas_complementar")

PREFIXO = "diaria_c"
_COMPLEMENTAR = re.compile(r"diaria_c(\d+)")
CHAVE_DA_SELECAO = "diaristas.complementar.{ano}-{mes:02d}-{tipo}-{empresa}"


def numero(verba) -> int:
    from . import empresa_da_folha
    achado = _COMPLEMENTAR.fullmatch(empresa_da_folha.separar(verba)[0])
    return int(achado.group(1)) if achado else 0


def e_da_diaria(verba) -> bool:
    """A verba é um pagamento de diárias: a normal ou uma complementar, de todas as
    empresas ou de uma."""
    from . import empresa_da_folha
    base = empresa_da_folha.separar(verba)[0]
    return base == "diaria" or bool(_COMPLEMENTAR.fullmatch(base))


def pagamentos_feitos(ano: int, mes: int, tipo: str) -> list:
    """As gerações de diárias do período (`tipo`: quinzena ou fim_de_mes), mais
    antiga primeiro. Uma verba gerada duas vezes conta UMA (o fechamento é um só)."""
    from . import folha_geracao as geracao, folha_pagamento as fp
    vistas, saida = set(), []
    for g in reversed(fp.log(teto=400, ano=int(ano), mes=int(mes))):
        if g["destino"] != fp.ANALISE or g["tipo"] != tipo:
            continue
        for verba in (g["verbas"] or "").split("+"):
            verba = verba.strip().lower()
            if verba and e_da_diaria(verba) and verba not in vistas:
                vistas.add(verba)
                saida.append({"verba": verba, "rotulo": geracao.rotulo_da_verba(verba),
                              "criado_em": g["criado_em"],
                              "criado_por": g.get("criado_por") or "",
                              "total": g["total"], "pessoas": g.get("pessoas") or 0})
    return saida


def proxima_verba(pagamentos: list, empresa: str = "") -> str:
    """A próxima complementar: o número segue o maior já usado no período, de
    qualquer empresa — cada uma tem o seu fechamento e nada se sobrepõe."""
    from . import empresa_da_folha
    n = max([numero(p["verba"]) for p in pagamentos] or [0])
    return empresa_da_folha.verba_da_empresa(f"{PREFIXO}{n + 1}", empresa)


def _chave_da_selecao(ano, mes, tipo, empresa) -> str:
    return CHAVE_DA_SELECAO.format(ano=int(ano), mes=int(mes), tipo=tipo,
                                   empresa=empresa or "todas")


def selecao(ano, mes, tipo, empresa="") -> dict:
    dado = fcomp._ler_meta(_chave_da_selecao(ano, mes, tipo, empresa))
    return dado if isinstance(dado, dict) else {}


def salvar_selecao(ano, mes, qual, decisoes, empresa: str = "", quem: str = "") -> dict:
    from . import empresa_da_folha, folha_diaristas as fd
    tipo = fd.TIPO_DO_FECHAMENTO.get(qual) or fd.TIPO_DO_FECHAMENTO["quinzena"]
    empresa = empresa_da_folha.normalizar(empresa)
    atual = selecao(ano, mes, tipo, empresa)
    for d in decisoes or []:
        cpf = "".join(c for c in str((d or {}).get("cpf") or "") if c.isdigit())
        if len(cpf) == 11:
            atual[cpf] = bool(d.get("pagar"))
    fcomp._gravar_meta(_chave_da_selecao(ano, mes, tipo, empresa), atual)
    return {"salvas": len(decisoes or [])}


def _novas(calculado: dict) -> tuple:
    """(pessoas no formato de `folha_complementar.comparar`, ajustes com o motivo de
    quem está desmarcado na tela dos Diaristas)."""
    novas, ajustes = [], {}
    for p in calculado.get("pessoas") or []:
        if p.get("vigia") or p.get("impossivel") or not p.get("valor") or p["valor"] <= 0:
            continue
        novas.append({"cpf": p["cpf"], "nome": p.get("nome") or "", "valor": p["valor"],
                      "por_obra": [{"obra": o["obra"], "dias": o.get("dias") or 0,
                                    "valor": o["valor"], "origem": "ponto"}
                                   for o in p.get("por_obra") or []
                                   if o.get("obra") and o["obra"] != "(SEM OBRA)"]})
        if not p.get("pagar"):
            ajustes[p["cpf"]] = {"fora": True,
                                 "motivo": "; ".join(p.get("motivos") or []) or
                                 "desmarcado na tela dos Diaristas"}
    return novas, ajustes


def montar(ano: int, mes: int, qual: str = "quinzena", empresa: str = "") -> dict:
    """Tudo o que a tela, o relatório e o arquivo precisam."""
    from . import empresa_da_folha, folha_diaristas as fd
    if qual not in fd.PERIODOS:
        qual = "quinzena"
    ano, mes = int(ano), int(mes)
    empresa = empresa_da_folha.normalizar(empresa)
    tipo = fd.TIPO_DO_FECHAMENTO[qual]
    pagamentos = pagamentos_feitos(ano, mes, tipo)
    verba = proxima_verba(pagamentos, empresa)
    rotulo_periodo = fd.PERIODOS[qual]
    base = {"ano": ano, "mes": mes, "qual": qual, "tipo": tipo, "empresa": empresa,
            "rotulo_periodo": rotulo_periodo, "competencia": f"{mes:02d}/{ano}",
            "pagamentos": pagamentos, "ja_paga": bool(pagamentos), "verba": verba,
            "pessoas": [], "grupos": {}, "totais": {}, "a_pagar": [],
            "total_a_pagar": Decimal("0.00"), "empresas": [],
            # O que a parte comum (`folha_complementar`) usa no lugar do da folha:
            "chave_da_selecao": _chave_da_selecao(ano, mes, tipo, empresa),
            "rotulo_a_descontar": f"Diárias {mes:02d}/{ano} {rotulo_periodo}",
            "chave_a_descontar": f"diaria-{ano}-{mes:02d}-{tipo}",
            "rotulo_do_relatorio": f"Diárias complementar {numero(verba)} · {rotulo_periodo}",
            "prefixo_do_relatorio": "Diárias complementar"}
    calculado = fd.calcular(ano, mes, qual, empresa)
    base["empresas"] = calculado.get("empresas") or []
    if not base["ja_paga"]:
        return base
    pagos = fcomp.pago_por_cpf(ano, mes, tipo, pagamentos)
    if empresa:
        # Só quem é da empresa escolhida: sem isso, quem é de outra apareceria como
        # "saiu da conta nova".
        alvo = "" if empresa == empresa_da_folha.SEM_CNPJ else empresa
        cnpjs = empresa_da_folha.cnpj_por_cpf()
        pagos = {c: v for c, v in pagos.items() if cnpjs.get(c, "") == alvo}
    novas, ajustes = _novas(calculado)
    linhas = fcomp.comparar(novas, pagos, ajustes)
    for l in linhas:
        if l["saiu"]:
            l["motivos"] = ["sem diária a receber na conta de hoje (ponto corrigido?)"]
    return fcomp.finalizar(base, linhas, selecao(ano, mes, tipo, empresa))


def depois_de_gerar(montado: dict, quem: str = "") -> None:
    fcomp.depois_de_gerar(montado, quem=quem)
